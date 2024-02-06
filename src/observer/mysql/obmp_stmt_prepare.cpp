/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/mysql/obmp_stmt_prepare.h"

#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/omt/ob_tenant.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql.h"
#include "observer/ob_req_time_service.h"
#include "observer/mysql/obmp_utils.h"

namespace oceanbase
{

using namespace rpc;
using namespace common;
using namespace share;
using namespace obmysql;
using namespace sql;

namespace observer
{

ObMPStmtPrepare::ObMPStmtPrepare(const ObGlobalContext &gctx)
    : ObMPBase(gctx),
      retry_ctrl_(/*ctx_.retry_info_*/),
      sql_(),
      sql_len_(),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0)
{
  ctx_.exec_type_ = MpQuery;
}

int ObMPStmtPrepare::deserialize()
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(req_)) || (req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K(req_));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    sql_.assign_ptr(const_cast<char *>(pkt.get_cdata()), pkt.get_clen()-1);
  }

  return ret;
}

int ObMPStmtPrepare::before_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMPBase::before_process())) {
    LOG_WARN("failed to pre processing packet", K(ret));
  } else if (0 == sql_.case_compare("call dbms_output.get_line(?, ?)")) {
    // do nothing
  } else if (!GCONF._ob_enable_prepared_statement) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
    "while parameter _ob_enable_prepared_statement is disabled, prepared statement");
    send_error_packet(ret, NULL);
  }

  return ret;
}

int ObMPStmtPrepare::multiple_query_check(ObSQLSessionInfo &session,
                                          ObString &sql,
                                          bool &force_sync_resp,
                                          bool &need_response_error)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 == session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS)) {
    ObSEArray<ObString, 1> queries;
    ObParser parser(THIS_WORKER.get_allocator(),
                    session.get_sql_mode(), session.get_charsets4parser());
    bool parse_fail = false;
    ObMPParseStat parse_stat;
    force_sync_resp = true;
    /* MySQL处理Multi-Stmt出错时候的行为：
      * 遇到首次运行失败（包括解析或执行）的SQL后，停止读取后继数据
      *  例如：
      *  (1) select 1; selct 2; select 3;
      *  select 1执行成功，selct 2报语法错误，select 3不被执行
      *  (2) select 1; drop table not_exists_table; select 3;
      *  select 1执行成功，drop table not_exists_table报表不存在错误，select 3不被执行
      *
      * 特别注意：
      * split_multiple_stmt是根据分号来分割语句，但有可能遇到“语法错误”，
      * 这里说的“语法错误”不是说select写成了selct，而是“token”级别的语法错误，例如语句
      * select 1;`select 2; select 3;
      * 上面`和'都没有形成闭合的字符串token，token parser会报告语法错误
      * 上面的例子中，得到的queries.count() 等于 2，分别为select 1和 `select 2; select 3;
      */
    ret = parser.split_multiple_stmt(sql, queries, parse_stat, false, true);
    if (OB_SUCC(ret)) { // ret=SUCC，并不意味着parse就一定成功，可能最后一个query是parse失败的
      if (OB_UNLIKELY(queries.count() <= 0)) {
        LOG_ERROR("emtpy query count. client would have suspended. never be here!",
                  K(sql), K(parse_fail));
      } else if (queries.count() > 1) {
        ret = OB_NOT_SUPPORTED;
        need_response_error = true;
        LOG_WARN("can't not prepare multi stmt", K(ret), K(queries.count()));
      } else {
        if (OB_UNLIKELY(parse_stat.parse_fail_ && (0 == parse_stat.fail_query_idx_)
                        && ObSQLUtils::check_need_disconnect_parser_err(parse_stat.fail_ret_))) {
          // 进入本分支，说明在multi_query中的某条query parse失败，如果不是语法错，则进入该分支
          // 如果当前query_count 为1， 则不断连接;如果大于1，
          // 则需要在发错误包之后断连接，防止客户端一直在等接下来的回包
          // 这个改动是为了解决
          ret = parse_stat.fail_ret_;
          need_response_error = true;
        }
      }
    } else {
      // 进入本分支，说明push_back出错，OOM，委托外层代码返回错误码
      // 且进入改分支之后，要断连接
      need_response_error = true;
      LOG_WARN("need response error", K(ret));
    }
  }
  return ret;
}

int ObMPStmtPrepare::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = NULL;
  bool need_response_error = true;
  bool async_resp_used = false; // 由事务提交线程异步回复客户端
  int64_t query_timeout = 0;
  ObSMConnection *conn = get_conn();
  bool need_disconnect = true;

  if (OB_ISNULL(req_) || OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("req or conn is null", K_(req), K(conn), K(ret));
  } else if (OB_UNLIKELY(!conn->is_in_authed_phase())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("receive sql without session", K_(sql), K(ret));
  } else if (OB_ISNULL(conn->tenant_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", K_(sql), K(conn->tenant_), K(ret));
  } else if (OB_FAIL(get_session(sess))) {
    LOG_WARN("get session fail", K_(sql), K(ret));
  } else if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K_(sql), K(sess), K(ret));
  } else if (OB_FAIL(update_transmission_checksum_flag(*sess))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObSQLSessionInfo &session = *sess;
    THIS_WORKER.set_session(sess);
    lib::CompatModeGuard g(sess->get_compatibility_mode() == ORACLE_MODE ?
                             lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    session.get_raw_audit_record().request_memory_used_ = 0;
    observer::ObProcessMallocCallback pmcb(0,
          session.get_raw_audit_record().request_memory_used_);
    lib::ObMallocCallbackGuard guard(pmcb);
    session.set_proxy_version(get_proxy_version());
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    int64_t packet_len = pkt.get_clen();
    if (OB_UNLIKELY(!session.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid session", K_(sql), K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(session.get_session_state()), K_(sql),
               K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K_(sql), K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                session.get_effective_tenant_id(), tenant_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (pkt.exist_trace_info()
               && OB_FAIL(session.update_sys_variable(SYS_VAR_OB_TRACE_INFO,
                                                      pkt.get_trace_info()))) {
      LOG_WARN("fail to update trace info", K(ret));
    } else if (FALSE_IT(session.set_txn_free_route(pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session.post_sync_session_info())) {
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      need_disconnect = false;
      LOG_WARN("packet too large than allowd for the session", K_(sql), K(ret));
    } else if (OB_FAIL(sql::ObFLTUtils::init_flt_info(pkt.get_extra_info(), session,
                            conn->proxy_cap_flags_.is_full_link_trace_support()))) {
      LOG_WARN("failed to init flt extra info", K(ret));
    } else {
      FLTSpanGuard(ps_prepare);
      FLT_SET_TAG(log_trace_id, ObCurTraceId::get_trace_id_str(),
                    receive_ts, get_receive_timestamp(),
                    client_info, session.get_client_info(),
                    module_name, session.get_module_name(),
                    action_name, session.get_action_name(),
                    sess_id, session.get_sessid());
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      retry_ctrl_.set_tenant_global_schema_version(tenant_version);
      retry_ctrl_.set_sys_global_schema_version(sys_version);
      session.partition_hit().reset();
      session.set_pl_can_retry(true);

      bool has_more = false;
      bool force_sync_resp = false;
      need_disconnect = false;
      need_response_error = false;
      if (OB_FAIL(multiple_query_check(session, sql_, force_sync_resp, need_response_error))) {
        need_disconnect = OB_NOT_SUPPORTED == ret ? false : true; 
        LOG_WARN("check multiple query fail.", K(ret));
      } else {
        ret = process_prepare_stmt(ObMultiStmtItem(false, 0, sql_), session, has_more, force_sync_resp, async_resp_used);
      }

      if (OB_FAIL(ret)) {
        //if (OB_EAGAIN == ret) {
          //large query, do nothing
        //} else
        if (is_conn_valid()) {//The memory of sql sting is invalid if conn_valid_ has ben set false.
          LOG_WARN("execute sql failed", "sql_id", ctx_.sql_id_, K_(sql), K(ret));
        } else {
          LOG_WARN("execute sql failed", K(ret));
        }
      }
    }

    if (!session.get_in_transaction()) {
        // transcation ends, end trace
        FLT_END_TRACE();
    }

    if (OB_FAIL(ret) && is_conn_valid()) {
      if (need_response_error) {
        send_error_packet(ret, NULL);
      }
      if (need_disconnect) {
        force_disconnect();
        LOG_WARN("disconnect connection when process query", K(ret));
      }
    }

    session.set_last_trace_id(ObCurTraceId::get_trace_id());
    THIS_WORKER.set_session(NULL);
    revert_session(sess); //current ignore revert session ret
  }
  return ret;
}

int ObMPStmtPrepare::process_prepare_stmt(const ObMultiStmtItem &multi_stmt_item,
                                          ObSQLSessionInfo &session,
                                          bool has_more_result,
                                          bool force_sync_resp,
                                          bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  bool need_response_error = true;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  setup_wb(session);

  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  if (OB_FAIL(init_process_var(ctx_, multi_stmt_item, session))) {
    LOG_WARN("init process var faield.", K(ret), K(multi_stmt_item));
  } else {
    const bool enable_trace_log = lib::is_trace_log_enabled();
    if (enable_trace_log) {
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(),
                                         session.get_effective_tenant_id()))) {
      LOG_WARN("failed to check_and_refresh_schema", K(ret));
    } else if (OB_FAIL(session.update_timezone_info())) {
      LOG_WARN("fail to update time zone info", K(ret));
    } else {
      ctx_.self_add_plan_ = false;
      ctx_.is_prepare_protocol_ = true; //set to prepare protocol
      ctx_.is_prepare_stage_ = true;
      need_response_error = false;
      do {
        share::schema::ObSchemaGetterGuard schema_guard;
        retry_ctrl_.clear_state_before_each_retry(session.get_retry_info_for_update());
        if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
                    session.get_effective_tenant_id(), schema_guard))) {
          LOG_WARN("get schema guard failed", K(ret));
        } else if (OB_FAIL(schema_guard.get_schema_version(
                    session.get_effective_tenant_id(), tenant_version))) {
          LOG_WARN("fail get schema version", K(ret));
        } else if (OB_FAIL(schema_guard.get_schema_version(
                    OB_SYS_TENANT_ID, sys_version))) {
          LOG_WARN("fail get sys schema version", K(ret));
        } else {
          ctx_.schema_guard_ = &schema_guard;
          retry_ctrl_.set_tenant_local_schema_version(tenant_version);
          retry_ctrl_.set_sys_local_schema_version(sys_version);
        }
        if (OB_SUCC(ret)) {
          ret = do_process(session,
                           has_more_result,
                           force_sync_resp,
                           async_resp_used);
          session.set_session_in_retry(retry_ctrl_.need_retry());
        }
      } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());
      if (OB_SUCC(ret) && retry_ctrl_.get_retry_times() > 0) {
        LOG_TRACE("sql retry succeed", K(ret),
                  "retry_times", retry_ctrl_.get_retry_times(), K(multi_stmt_item));
      }
    }
    if (enable_trace_log) {
      ObThreadLogLevelUtils::clear();
    }
  }

  //对于tracelog的处理，不影响正常逻辑，错误码无须赋值给ret
  int tmp_ret = OB_SUCCESS;
  //清空WARNING BUFFER
  tmp_ret = do_after_process(session, ctx_, async_resp_used);
  tmp_ret = record_flt_trace(session);
  // need_response_error这个变量保证仅在
  // do { do_process } while(retry) 之前出错才会
  // 走到send_error_packet逻辑
  // 所以无需考虑当前为sync还是async模式
  if (!OB_SUCC(ret) && need_response_error && is_conn_valid()) {
    send_error_packet(ret, NULL);
  }
  UNUSED(tmp_ret);
  return ret;
}

int ObMPStmtPrepare::check_and_refresh_schema(uint64_t login_tenant_id,
                                              uint64_t effective_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t local_version = 0;
  int64_t last_version = 0;

  if (login_tenant_id != effective_tenant_id) {
    // do nothing
    //
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null schema service", K(ret), K(gctx_));
  } else {
    if (OB_ISNULL(ctx_.session_info_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid session info", K(ret), K(ctx_.session_info_));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(effective_tenant_id, local_version))) {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret));
    } else if (OB_FAIL(ctx_.session_info_->get_ob_last_schema_version(last_version))) {
      LOG_WARN("failed to get_sys_variable", K(OB_SV_LAST_SCHEMA_VERSION));
    } else if (local_version >= last_version) {
      // skip
    } else if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(effective_tenant_id, last_version))) {
      LOG_WARN("failed to refresh schema", K(ret), K(effective_tenant_id), K(last_version));
    }
  }
  return ret;
}

int ObMPStmtPrepare::do_process(ObSQLSessionInfo &session,
                                const bool has_more_result,
                                const bool force_sync_resp,
                                bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  audit_record.try_cnt_++;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit
                                && session.get_local_ob_enable_sql_audit();
  single_process_timestamp_ = ObTimeUtility::current_time();
  bool is_diagnostics_stmt = false;
  bool need_response_error = true;
  const ObString &sql = ctx_.multi_stmt_item_.get_sql();
  ObPsStmtId inner_stmt_id = OB_INVALID_ID;

  /* !!!
   * 注意req_timeinfo_guard一定要放在result前面
   * !!!
   */
  ObReqTimeGuard req_timeinfo_guard;
  SMART_VAR(ObMySQLResultSet, result, session, THIS_WORKER.get_allocator()) {
    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_perf_event) {
        audit_record.exec_record_.record_start(di);
      }
      result.set_has_more_result(has_more_result);
      ObTaskExecutorCtx *task_ctx = result.get_exec_context().get_task_executor_ctx();
      int64_t execution_id = 0;
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task executor ctx can not be NULL", K(task_ctx), K(ret));
      } else {
        task_ctx->set_query_tenant_begin_schema_version(retry_ctrl_.get_tenant_global_schema_version());
        task_ctx->set_query_sys_begin_schema_version(retry_ctrl_.get_sys_global_schema_version());
        task_ctx->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
        ctx_.retry_times_ = retry_ctrl_.get_retry_times();
        if (OB_ISNULL(ctx_.schema_guard_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("newest schema is NULL", K(ret));
        } else if (OB_FAIL(result.init())) {
          LOG_WARN("result set init failed", K(ret));
        } else if (OB_ISNULL(gctx_.sql_engine_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid sql engine", K(ret), K(gctx_));
        } else if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
          //nothing to do
        } else if (OB_FAIL(set_session_active(sql, session, ObTimeUtil::current_time(), obmysql::ObMySQLCmd::COM_STMT_PREPARE))) {
          LOG_WARN("fail to set session active", K(ret));
        } else if (OB_FAIL(gctx_.sql_engine_->stmt_prepare(sql, ctx_, result, false/*is_inner_sql*/))) {
          exec_start_timestamp_ = ObTimeUtility::current_time();
          int cli_ret = OB_SUCCESS;
          retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
          LOG_WARN("run stmt_query failed, check if need retry",
                   K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K(sql));
          ret = cli_ret;
        } else if (common::OB_INVALID_ID != result.get_statement_id()
                   && OB_FAIL(session.get_inner_ps_stmt_id(result.get_statement_id(), inner_stmt_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ps : get inner stmt id fail.", K(ret), K(result.get_statement_id()));
        } else {
          //监控项统计开始
          exec_start_timestamp_ = ObTimeUtility::current_time();

          // 本分支内如果出错，全部会在response_result内部处理妥当
          // 无需再额外处理回复错误包
          need_response_error = false;
          is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result.get_literal_stmt_type());
          ctx_.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result.get_literal_stmt_type());
          session.set_current_execution_id(execution_id);

          //response_result
          if (OB_SUCC(ret) && OB_FAIL(response_result(result,
                                                      session,
                                                      force_sync_resp,
                                                      async_resp_used))) {
            ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
            if (OB_ISNULL(plan_ctx)) {
              LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
            } else {
              LOG_WARN("execute query fail", K(ret), "timeout_timestamp",
                      plan_ctx->get_timeout_timestamp());
            }
          }
          //监控项统计结束
          exec_end_timestamp_ = ObTimeUtility::current_time();

          // some statistics must be recorded for plan stat, even though sql audit disabled
          bool first_record = (1 == audit_record.try_cnt_);
          ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
          audit_record.exec_timestamp_.update_stage_time();

          if (enable_perf_event) {
            audit_record.exec_record_.record_end(di);
            audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
            audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
            audit_record.update_event_stage_state();
            if (!THIS_THWORKER.need_retry()) {
              const int64_t time_cost = exec_end_timestamp_ - get_receive_timestamp();
              EVENT_INC(SQL_PS_PREPARE_COUNT);
              EVENT_ADD(SQL_PS_PREPARE_TIME, time_cost);
            }
          }
        }
      }
    } // diagnose end

    // 重试需要满足一下条件：
    // 1. rs.open 执行失败
    // 2. 没有给客户端返回结果，本次执行没有副作用
    // 3. need_retry(result, ret)：schema 或 location cache 失效
    // 4. 小于重试次数限制
    if (OB_UNLIKELY(retry_ctrl_.need_retry())) {
      LOG_WARN("try to execute again",
              K(ret),
              N_TYPE, result.get_stmt_type(),
              "retry_type", retry_ctrl_.get_retry_type(),
              "timeout_remain", THIS_WORKER.get_timeout_remain());
    } else {
      // 首个plan执行完成后立即freeze partition hit
      // partition_hit一旦freeze后，后继的try_set_bool操作都不生效
      if (OB_LIKELY(NULL != result.get_physical_plan())) {
        session.partition_hit().freeze();
      }

      // store the warning message from the most recent statement in the current session
      if (OB_SUCC(ret) && is_diagnostics_stmt) {
        // if diagnostic stmt execute successfully, it dosen't clear the warning message
        session.update_show_warnings_buf();
      } else {
        session.set_show_warnings_buf(ret); // TODO: 挪个地方性能会更好，减少部分wb拷贝
      }

      if (!OB_SUCC(ret) && !async_resp_used && need_response_error && is_conn_valid() && !THIS_WORKER.need_retry()) {
        LOG_WARN("query failed", K(ret), K(retry_ctrl_.need_retry()), K_(sql));
        // 当need_retry=false时，可能给客户端回过包了，可能还没有回过任何包。
        // 不过，可以确定：这个请求出错了，还没处理完。如果不是已经交给异步EndTrans收尾，
        // 则需要在下面回复一个error_packet作为收尾。否则后面没人帮忙发错误包给客户端了，
        // 可能会导致客户端挂起等回包。
        bool is_partition_hit = session.get_err_final_partition_hit(ret);
        int err = send_error_packet(ret, NULL, is_partition_hit);
        if (OB_SUCCESS != err) {  // 发送error包
          LOG_WARN("send error packet failed", K(ret), K(err));
        }
      }
    }
    if (enable_sql_audit) {
      audit_record.status_ = ret;
      audit_record.client_addr_ = session.get_peer_addr();
      audit_record.user_client_addr_ = session.get_user_client_addr();
      audit_record.user_group_ = THIS_WORKER.get_group_id();
      audit_record.ps_stmt_id_ = result.get_statement_id();
      audit_record.ps_inner_stmt_id_ = inner_stmt_id;
      audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
    }
    bool need_retry = (THIS_THWORKER.need_retry()
                       || RETRY_TYPE_NONE != retry_ctrl_.get_retry_type());
    ObSQLUtils::handle_audit_record(need_retry, EXECUTE_PS_PREPARE, session, ctx_.is_sensitive_);
  }

  // reset thread waring buffer in sync mode
  if (!async_resp_used) {
    clear_wb_content(session);
  }
  return ret;
}

// return false only if send packet fail.
int ObMPStmtPrepare::response_result(
    ObMySQLResultSet &result,
    ObSQLSessionInfo &session,
    bool force_sync_resp,
    bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  UNUSED(force_sync_resp);
  UNUSED(async_resp_used);
//  const ObMySQLRawPacket &packet = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  if (OB_FAIL(send_prepare_packet(result))) {
    LOG_WARN("send prepare packet failed", K(ret));
  } else if (OB_FAIL(send_param_packet(session, result))) {
    LOG_WARN("send param packet failed", K(ret));
  } else if (OB_FAIL(send_column_packet(session, result))) {
    LOG_WARN("send column packet failed", K(ret));
  }
  return ret;
}

int ObMPStmtPrepare::send_prepare_packet(const ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  OMPKPrepare prepare_packet;
  const ParamsFieldIArray *params = result.get_param_fields();
  const ColumnsFieldIArray *columns = result.get_field_columns();
  if (OB_ISNULL(params) || OB_ISNULL(columns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(columns), K(params));
  } else {
    prepare_packet.set_statement_id(static_cast<uint32_t>(result.get_statement_id()));
    prepare_packet.set_column_num(static_cast<uint16_t>(result.get_field_cnt()));
    prepare_packet.set_warning_count(static_cast<uint16_t>(result.get_warning_count()));
    if (OB_ISNULL(result.get_param_fields())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(result.get_param_fields()));
    } else {
      prepare_packet.set_param_num(
        static_cast<uint16_t>(result.get_param_fields()->count()));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(response_packet(prepare_packet, const_cast<ObSQLSessionInfo *>(&result.get_session())))) {
    LOG_WARN("response packet failed", K(ret));
  }

  if (OB_SUCC(ret) && need_send_extra_ok_packet() && columns->count() == 0 && params->count() == 0) {
    ObOKPParam ok_param;
    if (OB_FAIL(send_ok_packet(*(const_cast<ObSQLSessionInfo *>(&result.get_session())), ok_param))) {
      LOG_WARN("fail to send ok packet", K(ret));
    }
  }
  return ret;
}

int ObMPStmtPrepare::send_column_packet(const ObSQLSessionInfo &session,
                                        ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  const ColumnsFieldIArray *columns = result.get_field_columns();
  if (OB_ISNULL(columns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(columns));
  } else if (columns->count() > 0) {
    ObMySQLField field;
    ret = result.next_field(field);
    while (OB_SUCC(ret)) {
      OMPKField fp(field);
      if (OB_FAIL(response_packet(fp, const_cast<ObSQLSessionInfo *>(&session)))) {
        LOG_WARN("response packet fail", K(ret));
      } else {
        LOG_DEBUG("response field succ", K(field));
        ret = result.next_field(field);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(packet_sender_.update_last_pkt_pos())) {
        LOG_WARN("failed to update last packet pos", K(ret));
      } else {
        if (need_send_extra_ok_packet()) {
          ObOKPParam ok_param;
          if (OB_FAIL(send_eof_packet(session, result, &ok_param))) {
            LOG_WARN("send eof field failed", K(ret));
          }
        } else {
          if (OB_FAIL(send_eof_packet(session, result))) {
            LOG_WARN("send eof field failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMPStmtPrepare::send_param_packet(const ObSQLSessionInfo &session,
                                       ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  const ParamsFieldIArray *params = result.get_param_fields();
  const ColumnsFieldIArray *columns = result.get_field_columns();
  if (OB_ISNULL(params) || OB_ISNULL(columns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(columns), K(params));
  } else if (params->count() > 0) {
    ObMySQLField field;
    ret = result.next_param(field);
    while (OB_SUCC(ret)) {
      OMPKField fp(field);
      if (OB_FAIL(response_packet(fp, const_cast<ObSQLSessionInfo *>(&session)))) {
        LOG_DEBUG("response packet fail", K(ret));
      } else {
//        LOG_INFO("response field succ", K(field));
        ret = result.next_param(field);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (need_send_extra_ok_packet() && columns->count() == 0) {
        ObOKPParam ok_param;
        if (OB_FAIL(send_eof_packet(session, result, &ok_param))) {
          LOG_WARN("send eof field failed", K(ret));
        }
      } else {
        if (OB_FAIL(send_eof_packet(session, result))) {
          LOG_WARN("send eof field failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} //end of namespace observer
} //end of namespace oceanbase

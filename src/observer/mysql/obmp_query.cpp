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

#include "observer/mysql/obmp_query.h"

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/worker.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/profile/ob_perf_event.h"
#include "share/ob_debug_sync.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
#include "share/ob_resource_limit.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/engine/px/ob_px_admission.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_async_cmd_driver.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "observer/ob_req_time_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/ob_server.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/monitor/ob_security_audit.h"
#include "lib/rc/context.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "observer/mysql/obmp_utils.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/trace/ob_trace.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::trace;
using namespace oceanbase::sql;
ObMPQuery::ObMPQuery(const ObGlobalContext &gctx)
    : ObMPBase(gctx),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      is_com_filed_list_(false),
      params_value_len_(0),
      params_value_(NULL)
{
  ctx_.exec_type_ = MpQuery;
}


ObMPQuery::~ObMPQuery()
{
}


int ObMPQuery::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = NULL;
  uint32_t sessid = 0;
  bool need_response_error = true;
  bool need_disconnect = true;
  bool async_resp_used = false; // 由事务提交线程异步回复客户端
  int64_t query_timeout = 0;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  ObSMConnection *conn = get_conn();
  static int64_t concurrent_count = 0;
  bool need_dec = false;
  bool do_ins_batch_opt = false;
  if (RL_IS_ENABLED) {
    if (ATOMIC_FAA(&concurrent_count, 1) > RL_CONF.get_max_concurrent_query_count()) {
      ret = OB_RESOURCE_OUT;
      LOG_WARN("reach max concurrent limit", K(ret), K(concurrent_count),
          K(RL_CONF.get_max_concurrent_query_count()));
    }
    need_dec = true;
  }
  DEFER(if (need_dec) (void)ATOMIC_FAA(&concurrent_count, -1));
  if (OB_FAIL(ret)) {
    // do-nothing
  } else if (OB_ISNULL(req_) || OB_ISNULL(conn) || OB_ISNULL(cur_trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null conn ptr", K_(sql), K_(req), K(conn), K(cur_trace_id), K(ret));
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
  } else {
    lib::CompatModeGuard g(sess->get_compatibility_mode() == ORACLE_MODE ?
                             lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    THIS_WORKER.set_session(sess);
    ObSQLSessionInfo &session = *sess;
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    int64_t val = 0;
    const bool check_throttle = !is_root_user(sess->get_user_id());

    if (check_throttle &&
        !sess->is_inner() &&
        sess->get_raw_audit_record().try_cnt_ == 0 &&
        lib::Worker::WS_OUT_OF_THROTTLE == THIS_THWORKER.check_rate_limiter()) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_WARN("query is throttled", K(ret), K(sess->get_user_id()));
      need_disconnect = false;
    } else if (OB_SUCC(sess->get_sql_throttle_current_priority(val))) {
      THIS_WORKER.set_sql_throttle_current_priority(check_throttle ? val : -1);
      if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_THWORKER.check_qtime_throttle()) {
        ret = OB_KILLED_BY_THROTTLING;
        LOG_WARN("query is throttled", K(ret));
        need_disconnect = false;
      }
    } else {
      LOG_WARN("get system variable sql_throttle_current_priority fail", K(ret));
      // reset ret for compatibility.
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      sessid = conn->sessid_;
      int64_t tenant_version = 0;
      int64_t sys_version = 0;
      session.set_thread_id(GETTID());
      const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
      int64_t packet_len = pkt.get_clen();
      req_->set_trace_point(ObRequest::OB_EASY_REQUEST_MPQUERY_PROCESS);
      if (OB_UNLIKELY(!session.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid session", K_(sql), K(ret));
      } else if (OB_UNLIKELY(session.is_zombie())) {
        //session has been killed some moment ago
        ret = OB_ERR_SESSION_INTERRUPTED;
        LOG_WARN("session has been killed", K(session.get_session_state()), K_(sql),
                 K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
      } else if (OB_FAIL(session.check_and_init_retry_info(*cur_trace_id, sql_))) {
        // 注意，retry info和last query trace id的逻辑要写在query lock内，否则会有并发问题
        LOG_WARN("fail to check and init retry info", K(ret), K(*cur_trace_id), K_(sql));
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
        //packet size check with session variable max_allowd_packet or net_buffer_length
        need_disconnect = false;
        ret = OB_ERR_NET_PACKET_TOO_LARGE;
        LOG_WARN("packet too large than allowed for the session", K_(sql), K(ret));
      } else if (OB_FAIL(sql::ObFLTUtils::init_flt_info(pkt.get_extra_info(), session,
                              conn->proxy_cap_flags_.is_full_link_trace_support()))) {
        LOG_WARN("failed to update flt extra info", K(ret));
      } else if (OB_FAIL(session.gen_configs_in_pc_str())) {
        LOG_WARN("fail to generate configuration strings that can influence execution plan", K(ret));
      } else {
        FLTSpanGuard(com_query_process);
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
        ObLockWaitNode &lock_wait_node  = req_->get_lock_wait_node();
        lock_wait_node.set_session_info(session.get_sessid());

        bool has_more = false;
        bool force_sync_resp = false;
        need_response_error = false;
        ObParser parser(THIS_WORKER.get_sql_arena_allocator(),
                        session.get_sql_mode(), session.get_charsets4parser());
        //为了性能优化考虑，减少数组长度，降低无用元素的构造和析构开销
        ObSEArray<ObString, 1> queries;
        ObSEArray<ObString, 1> ins_queries;
        ObMPParseStat parse_stat;
        if (GCONF.enable_record_trace_id) {
          PreParseResult pre_parse_result;
          if (OB_FAIL(ObParser::pre_parse(sql_, pre_parse_result))) {
            LOG_WARN("fail to pre parse", K(ret));
          } else {
            session.set_app_trace_id(pre_parse_result.trace_id_);
            LOG_DEBUG("app trace id", "app_trace_id", pre_parse_result.trace_id_,
                                      "sessid", session.get_sessid(), K_(sql));
          }
        }

        if (OB_FAIL(ret)) {
          //do nothing
        } else if (OB_FAIL(parser.split_multiple_stmt(sql_, queries, parse_stat))) {
          // 进入本分支，说明push_back出错，OOM，委托外层代码返回错误码
          // 且进入此分支之后，要断连接
          need_response_error = true;
        } else if (OB_UNLIKELY(queries.count() <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          need_response_error = true;//进入此分支之后，要断连接，极其严重错误
          LOG_ERROR("emtpy query count. client would have suspended. never be here!", K_(sql), K(ret));
        } else if (OB_UNLIKELY(1 == session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS)) {
          // 处理Multiple Statement
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
          bool optimization_done = false;
          const char *p_normal_start = nullptr;
          if (queries.count() > 1 && session.is_txn_free_route_temp()) {
            need_disconnect = false;
            need_response_error = true;
            ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;
            LOG_WARN("multi stmt is not supported to be executed on txn temporary node", KR(ret),
                     "tx_free_route_ctx", session.get_txn_free_route_ctx(),
                     "trans_id", session.get_tx_id(), K(session));
          } else if (queries.count() > 1
            && OB_FAIL(try_batched_multi_stmt_optimization(session,
                                                          queries,
                                                          parse_stat,
                                                          optimization_done,
                                                          async_resp_used,
                                                          need_disconnect,
                                                          false))) {
            LOG_WARN("failed to try multi-stmt-optimization", K(ret));
          } else if (!optimization_done
                     && session.is_enable_batched_multi_statement()
                     && ObSQLUtils::is_enable_explain_batched_multi_statement()
                     && ObParser::is_explain_stmt(queries.at(0), p_normal_start)) {
            ret = OB_SUCC(ret) ? OB_NOT_SUPPORTED : ret;
            need_disconnect = false;
            need_response_error = true;
            LOG_WARN("explain batch statement failed", K(ret));
          } else if (!optimization_done) {
            ARRAY_FOREACH(queries, i) {
              // in multistmt sql, audit_record will record multistmt_start_ts_ when count over 1
              // queries.count()>1 -> batch,(m)sql1,(m)sql2,...    |    queries.count()=1 -> sql1
              if (i > 0) {
                session.get_raw_audit_record().exec_timestamp_.multistmt_start_ts_
                                                              = ObTimeUtility::current_time();
                // before handle multi-stmt's followers, re-calc the txn_free_route's baseline
                // in order to capture accurate state changed by current stmt
                session.prep_txn_free_route_baseline();
              }
              need_disconnect = true;
              //FIXME qianfu NG_TRACE_EXT(set_disconnect, OB_ID(disconnect), true, OB_ID(pos), "multi stmt begin");
              if (OB_UNLIKELY(parse_stat.parse_fail_
                  && (i == parse_stat.fail_query_idx_)
                  && ObSQLUtils::check_need_disconnect_parser_err(parse_stat.fail_ret_))) {
                // 进入本分支，说明在multi_query中的某条query parse失败，如果不是语法错，则进入该分支
                // 如果当前query_count 为1， 则不断连接;如果大于1，
                // 则需要在发错误包之后断连接，防止客户端一直在等接下来的回包
                // 这个改动是为了解决
                ret = parse_stat.fail_ret_;
                need_response_error = true;
                break;
              } else {
                has_more = (queries.count() > i + 1);
                // 本来可以做成不管queries.count()是多少，最后一个query都可以异步回包的，
                // 但是目前的代码实现难以在不同的线程处理同一个请求的回包，
                // 因此这里只允许只有一个query的multi query请求异步回包。
                force_sync_resp = queries.count() <= 1? false : true;
                // is_part_of_multi 表示当前sql是 multi stmt 中的一条，
                // 原来的值默认为true，会影响单条sql的二次路由，现在改为用 queries.count() 判断。
                bool is_part_of_multi = queries.count() > 1 ? true : false;
                ret = process_single_stmt(ObMultiStmtItem(is_part_of_multi, i, queries.at(i)),
                                          session,
                                          has_more,
                                          force_sync_resp,
                                          async_resp_used,
                                          need_disconnect);
              }
            }
          }
          // 以multiple query协议发过来的语句总数
          EVENT_INC(SQL_MULTI_QUERY_COUNT);
          // 以multiple query协议发过来，但实际只包含一条SQL的语句的个数
          if (queries.count() <= 1) {
            EVENT_INC(SQL_MULTI_ONE_QUERY_COUNT);
          }
        } else { // OB_CLIENT_MULTI_STATEMENTS not enabled
          if (OB_UNLIKELY(queries.count() != 1)) {
            ret = OB_ERR_PARSER_SYNTAX;
            need_disconnect = false;
            need_response_error = true;
            LOG_WARN("unexpected error. multi stmts sql while OB_CLIENT_MULTI_STATEMENTS not enabled.", K(ret), K(sql_));
          } else {
            EVENT_INC(SQL_SINGLE_QUERY_COUNT);
            // 处理普通的Single Statement
            ret = process_single_stmt(ObMultiStmtItem(false, 0, sql_),
                                      session,
                                      has_more,
                                      force_sync_resp,
                                      async_resp_used,
                                      need_disconnect);
          }
        }
        if (OB_FAIL(ret)) {
          FLT_SET_TAG(err_code, ret);
        }
      }
    }
    // THIS_WORKER.need_retry()是指是否扔回队列重试，包括大查询被扔回队列的情况。
    session.check_and_reset_retry_info(*cur_trace_id, THIS_WORKER.need_retry());
    session.set_last_trace_id(ObCurTraceId::get_trace_id());
    IGNORE_RETURN record_flt_trace(session);
  }

  if (OB_UNLIKELY(NULL != GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid() && is_conn_valid()) {
    int tmp_ret = OB_SUCCESS;
    // Call setup_user_resource_group no matter OB_SUCC or OB_FAIL
    // because we have to reset conn.group_id_ according to user_name.
    // Otherwise, suppose we execute a query with a mapping rule on the column in the query at first,
    // we switch to the defined consumer group, batch_group for example,
    // and after that, the next query will also be executed with batch_group.
    if (OB_UNLIKELY(OB_SUCCESS !=
            (tmp_ret = setup_user_resource_group(*conn, sess->get_effective_tenant_id(), sess)))) {
      LOG_WARN("fail setup user resource group", K(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  if (OB_FAIL(ret) && need_response_error && is_conn_valid()) {
    send_error_packet(ret, NULL);
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(need_disconnect) && is_conn_valid()) {
    force_disconnect();
    LOG_WARN("disconnect connection", KR(ret));
  }

  // 如果已经异步回包，则这部分逻辑在cb中执行，这里跳过flush_buffer()
  if (!THIS_WORKER.need_retry()) {
    if (async_resp_used) {
      async_resp_used_ = true;
      packet_sender_.disable_response();
    } else if (OB_UNLIKELY(!is_conn_valid())) {
      tmp_ret = OB_CONNECT_ERROR;
      LOG_WARN("connection in error, maybe has disconnected", K(tmp_ret));
    } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = flush_buffer(true)))) {
      LOG_WARN("failed to flush_buffer", K(tmp_ret));
    }
  } else {
    need_retry_ = true;
  }

  // bugfix:
  // 必须总是将 THIS_WORKER 里的指针设置为 null
  THIS_WORKER.set_session(NULL); // clear session

  if (sess != NULL) {
    revert_session(sess); //current ignore revert session ret
  }

  return (OB_SUCCESS != ret) ? ret : tmp_ret;
}

/*
 * Try to evaluate multiple update queries as a single query to optimize rpc cost
 * for details, please ref to
 */
int ObMPQuery::try_batched_multi_stmt_optimization(sql::ObSQLSessionInfo &session,
                                                   common::ObIArray<ObString> &queries,
                                                   const ObMPParseStat &parse_stat,
                                                   bool &optimization_done,
                                                   bool &async_resp_used,
                                                   bool &need_disconnect,
                                                   bool is_ins_multi_val_opt)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  bool force_sync_resp = true;
  bool enable_batch_opt = session.is_enable_batched_multi_statement();
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  optimization_done = false;
  if (queries.count() <= 1 || parse_stat.parse_fail_) {
    /*do nothing*/
  } else if (!enable_batch_opt) {
    // 未打开batch开关
  } else if (!use_plan_cache) {
    // 不打开plan_cache开关，则优化不支持
  } else if (OB_FAIL(process_single_stmt(ObMultiStmtItem(false, 0, sql_, &queries, is_ins_multi_val_opt),
                                         session,
                                         has_more,
                                         force_sync_resp,
                                         async_resp_used,
                                         need_disconnect))) {
    int tmp_ret = ret;
    if (THIS_WORKER.need_retry()) {
      // fail optimize, is a large query, just go back to large query queue and retry
    } else {
      ret = OB_SUCCESS;
    }
    LOG_WARN("failed to process batch stmt, cover the error code and reset retry flag",
        K(tmp_ret), K(ret), K(THIS_WORKER.need_retry()));
  } else {
    optimization_done = true;
  }

  LOG_TRACE("after to try batched multi-stmt optimization", K(optimization_done),
      K(queries), K(enable_batch_opt), K(ret), K(THIS_WORKER.need_retry()), K(retry_ctrl_.need_retry()), K(retry_ctrl_.get_retry_type()));
  return ret;
}

int ObMPQuery::process_single_stmt(const ObMultiStmtItem &multi_stmt_item,
                                   ObSQLSessionInfo &session,
                                   bool has_more_result,
                                   bool force_sync_resp,
                                   bool &async_resp_used,
                                   bool &need_disconnect)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(mpquery_single_stmt);
  ctx_.spm_ctx_.reset();
  bool need_response_error = true;
  const bool enable_trace_log = lib::is_trace_log_enabled();
  session.get_raw_audit_record().request_memory_used_ = 0;
  observer::ObProcessMallocCallback pmcb(0,
        session.get_raw_audit_record().request_memory_used_);
  lib::ObMallocCallbackGuard guard(pmcb);
  // 执行setup_wb后，所有WARNING都会写入到当前session的WARNING BUFFER中
  setup_wb(session);
  // 当新语句开始的时候，将该值归0，因为curr_trans_last_stmt_end_time是用于
  // 实现事务内部的语句执行间隔过长超时功能的。
  session.set_curr_trans_last_stmt_end_time(0);

  //============================ 注意这些变量的生命周期 ================================
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  if (OB_FAIL(init_process_var(ctx_, multi_stmt_item, session))) {
    LOG_WARN("init process var failed.", K(ret), K(multi_stmt_item));
  } else {
    if (enable_trace_log) {
      //set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    // obproxy may use 'SET @@last_schema_version = xxxx' to set newest schema,
    // observer will force refresh schema if local_schema_version < last_schema_version;
    if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(),
                                         session.get_effective_tenant_id(),
                                         &session))) {
      LOG_WARN("failed to check_and_refresh_schema", K(ret));
    } else if (OB_FAIL(session.update_timezone_info())) {
      LOG_WARN("fail to update time zone info", K(ret));
    } else {
      need_response_error = false;
      //每次执行不同sql都需要更新
      ctx_.self_add_plan_ = false;
      retry_ctrl_.reset_retry_times();//每个statement单独记录retry times
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
      do {
        ret = OB_SUCCESS; //当发生本地重试的时候，需要重置错误码，不然无法推进重试
        need_disconnect = true;
        // do the real work
        //create a new temporary memory context for executing sql can
        //avoid the problem the memory cannot be released in time due to too many sql items
        //but it will drop the sysbench performance about 1~4%
        //so we execute the first sql with the default memory context and
        //execute the rest sqls with a temporary memory context to avoid memory dynamic leaks
        retry_ctrl_.clear_state_before_each_retry(session.get_retry_info_for_update());
        bool first_exec_sql = session.get_is_in_retry() ? false :
            (multi_stmt_item.is_part_of_multi_stmt() ? multi_stmt_item.get_seq_num() <= 1 : true);
        if (OB_LIKELY(first_exec_sql)) {
          ret = do_process(session,
                           has_more_result,
                           force_sync_resp,
                           async_resp_used,
                           need_disconnect);
          ctx_.clear();
        } else {
          ret = process_with_tmp_context(session,
                                         has_more_result,
                                         force_sync_resp,
                                         async_resp_used,
                                         need_disconnect);
        }
        //set session retry state
        session.set_session_in_retry(retry_ctrl_.need_retry());
      } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());
      //@notice: after the async packet is responsed,
      //the easy_buf_ hold by the sql string may have been released.
      //from here on, we can no longer access multi_stmt_item.sql_,
      //otherwise there is a risk of coredump
      //@TODO: need to determine a mechanism to ensure the safety of memory access here
    }
    ObThreadLogLevelUtils::clear();
    const int64_t debug_sync_timeout = GCONF.debug_sync_timeout;
    if (debug_sync_timeout > 0) {
      // ignore thread local debug sync actions to session actions failed
      int tmp_ret = OB_SUCCESS;
      tmp_ret = GDS.collect_result_actions(session.get_debug_sync_actions());
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("set thread local debug sync actions to session actions failed", K(tmp_ret));
      }
    }
  }

  //对于tracelog的处理，不影响正常逻辑，错误码无须赋值给ret
  int tmp_ret = OB_SUCCESS;
  //清空WARNING BUFFER
  tmp_ret = do_after_process(session, ctx_, async_resp_used);

  // 设置上一条语句的结束时间，由于这里只用于实现事务内部的语句之间的执行超时，
  // 因此，首先，需要判断是否处于事务执行的过程中。然后对于事务提交的时候的异步回包,
  // 也不需要在这里设置结束时间，因为这已经相当于事务的最后一条语句了。
  // 最后，需要判断ret错误码，只有成功执行的sql才记录结束时间
  if (session.get_in_transaction() && !async_resp_used && OB_SUCC(ret)) {
    session.set_curr_trans_last_stmt_end_time(common::ObTimeUtility::current_time());
  }

  // need_response_error这个变量保证仅在
  // do { do_process } while(retry) 之前出错才会
  // 走到send_error_packet逻辑
  // 所以无需考虑当前为sync还是async模式
  if (!OB_SUCC(ret) && need_response_error && is_conn_valid()) {
    send_error_packet(ret, NULL);
  }
  ctx_.reset();
  return ret;
}

OB_NOINLINE int ObMPQuery::process_with_tmp_context(ObSQLSessionInfo &session,
                                                    bool has_more_result,
                                                    bool force_sync_resp,
                                                    bool &async_resp_used,
                                                    bool &need_disconnect)
{
  int ret = OB_SUCCESS;
  //create a temporary memory context to process retry or the rest sql of multi-query,
  //avoid memory dynamic leaks caused by query retry or too many multi-query items
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(),
      ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(!lib::is_mini_mode() ? OB_MALLOC_BIG_BLOCK_SIZE
        : OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = do_process(session,
                     has_more_result,
                     force_sync_resp,
                     async_resp_used,
                     need_disconnect);
    ctx_.first_plan_hash_ = 0;
    ctx_.first_outline_data_.reset();
    ctx_.clear();
  }
  return ret;
}

OB_INLINE int ObMPQuery::get_tenant_schema_info_(const uint64_t tenant_id,
                                                ObTenantCachedSchemaGuardInfo *cache_info,
                                                ObSchemaGetterGuard *&schema_guard,
                                                int64_t &tenant_version,
                                                int64_t &sys_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &cached_guard = cache_info->get_schema_guard();
  bool need_refresh = false;

  if (!cached_guard.is_inited()) {
    // 第一次获取schema guard
    need_refresh = true;
  } else if (tenant_id != cached_guard.get_tenant_id()) {
    // change tenant
    need_refresh = true;
  } else {
    int64_t tmp_tenant_version = 0;
    int64_t tmp_sys_version = 0;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(tenant_id, tmp_tenant_version))) {
      LOG_WARN("get tenant refreshed schema version error", K(ret), K(tenant_id));
    } else if (OB_FAIL(cached_guard.get_schema_version(tenant_id, tenant_version))) {
      LOG_WARN("fail get schema version", K(ret), K(tenant_id));
    } else if (tmp_tenant_version != tenant_version) {
      //需要获取schema guard
      need_refresh = true;
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, tmp_sys_version))) {
      LOG_WARN("get sys tenant refreshed schema version error", K(ret), "sys_tenant_id", OB_SYS_TENANT_ID);
    } else if (OB_FAIL(cached_guard.get_schema_version(OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get sys schema version", K(ret));
    } else if (tmp_sys_version != sys_version) {
      //需要获取schema guard
      need_refresh = true;
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (!need_refresh) {
      //获取session上缓存的最新schema guard
      schema_guard = &(cache_info->get_schema_guard());
    } else if (OB_FAIL(cache_info->refresh_tenant_schema_guard(tenant_id))) {
      LOG_WARN("refresh tenant schema guard failed", K(ret), K(tenant_id));
    } else {
      //获取session上缓存的最新schema guard
      schema_guard = &(cache_info->get_schema_guard());
      if (OB_FAIL(schema_guard->get_schema_version(tenant_id, tenant_version))) {
        LOG_WARN("fail get schema version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard->get_schema_version(OB_SYS_TENANT_ID, sys_version))) {
        LOG_WARN("fail get sys schema version", K(ret));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

OB_INLINE int ObMPQuery::do_process(ObSQLSessionInfo &session,
                                    bool has_more_result,
                                    bool force_sync_resp,
                                    bool &async_resp_used,
                                    bool &need_disconnect)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  audit_record.try_cnt_++;
  bool is_diagnostics_stmt = false;
  bool need_response_error = true;
  const ObString &sql = ctx_.multi_stmt_item_.get_sql();
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit =
    GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();
  single_process_timestamp_ = ObTimeUtility::current_time();
  /* !!!
   * 注意req_timeinfo_guard一定要放在result前面
   * !!!
   */
  ObReqTimeGuard req_timeinfo_guard;
  ObPhysicalPlan *plan = nullptr;
  ObSchemaGetterGuard* schema_guard = nullptr;
  ObTenantCachedSchemaGuardInfo &cached_schema_info = session.get_cached_schema_guard_info();
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  common::ObSqlInfoGuard si_guard(sql);
  ObSqlFatalErrExtraInfoGuard extra_info_guard;
  extra_info_guard.set_cur_sql(sql);
  extra_info_guard.set_tenant_id(session.get_effective_tenant_id());
  ObIAllocator &allocator = CURRENT_CONTEXT->get_arena_allocator();
  SMART_VAR(ObMySQLResultSet, result, session, allocator) {
    if (OB_FAIL(get_tenant_schema_info_(session.get_effective_tenant_id(),
                                        &cached_schema_info,
                                        schema_guard,
                                        tenant_version,
                                        sys_version))) {
      LOG_WARN("get tenant schema info error", K(ret), K(session));
    } else if (OB_FAIL(session.update_query_sensitive_system_variable(*schema_guard))) {
      LOG_WARN("update query sensitive system vairable in session failed", K(ret));
    } else if (OB_FAIL(update_transmission_checksum_flag(session))) {
      LOG_WARN("update transmisson checksum flag failed", K(ret));
    } else if (OB_ISNULL(gctx_.sql_engine_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid sql engine", K(ret), K(gctx_));
    } else {
      session.set_current_execution_id(GCTX.sql_engine_->get_execution_id());
      result.get_exec_context().set_need_disconnect(true);
      ctx_.schema_guard_ = schema_guard;
      retry_ctrl_.set_tenant_local_schema_version(tenant_version);
      retry_ctrl_.set_sys_local_schema_version(sys_version);
      extra_info_guard.set_exec_context(&(result.get_exec_context()));
    }

    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo *di = NULL;
    if (OB_SUCC(ret)) {
      if (enable_perf_event) {
        di = ObDiagnoseSessionInfo::get_local_diagnose_info();
      }
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_perf_event) {
        audit_record.exec_record_.record_start(di);
      }
      result.set_has_more_result(has_more_result);
      ObTaskExecutorCtx &task_ctx = result.get_exec_context().get_task_exec_ctx();
      task_ctx.schema_service_ = gctx_.schema_service_;
      task_ctx.set_query_tenant_begin_schema_version(retry_ctrl_.get_tenant_local_schema_version());
      task_ctx.set_query_sys_begin_schema_version(retry_ctrl_.get_sys_local_schema_version());
      task_ctx.set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
      ctx_.retry_times_ = retry_ctrl_.get_retry_times();
      ctx_.enable_sql_resource_manage_ = true;
      //storage::ObPartitionService* ps = static_cast<storage::ObPartitionService *> (GCTX.par_ser_);
      //bool is_read_only = false;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(ctx_.schema_guard_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("newest schema is NULL", K(ret));
      } else if (OB_FAIL(set_session_active(sql, session, single_process_timestamp_))) {
        LOG_WARN("fail to set session active", K(ret));
      } else if (OB_FAIL(gctx_.sql_engine_->stmt_query(sql, ctx_, result))) {
        exec_start_timestamp_ = ObTimeUtility::current_time();
        if (!THIS_WORKER.need_retry()) {
          int cli_ret = OB_SUCCESS;
          retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
          if (OB_ERR_PROXY_REROUTE == ret) {
            LOG_DEBUG("run stmt_query failed, check if need retry",
                      K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K(sql));
          } else {
            LOG_WARN("run stmt_query failed, check if need retry",
                     K(ret), K(cli_ret), K(retry_ctrl_.need_retry()),
                     "sql", ctx_.is_sensitive_ ? ObString(OB_MASKED_STR) : sql);
          }
          ret = cli_ret;
          if (OB_ERR_PROXY_REROUTE == ret) {
            // 该错误码在下层是由编译阶段被设置，async_resp_used标志一定为false
            // 所以此时可以同步回包，设置need_response_error
            // 向客户端返回一个error包，表示需要二次路由
            need_response_error = true;
          } else if (ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
            // batch execute with error,should not response error packet
            need_response_error = false;
          } else if (OB_BATCHED_MULTI_STMT_ROLLBACK == ret) {
            need_response_error = false;
          }
        } else {
          retry_ctrl_.set_packet_retry(ret);
          session.get_retry_info_for_update().set_last_query_retry_err(ret);
          session.get_retry_info_for_update().inc_retry_cnt();
        }
      } else {
        //监控项统计开始
        exec_start_timestamp_ = ObTimeUtility::current_time();
        result.get_exec_context().set_plan_start_time(exec_start_timestamp_);
        // 本分支内如果出错，全部会在response_result内部处理妥当
        // 无需再额外处理回复错误包
        need_response_error = false;
        is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result.get_literal_stmt_type());
        ctx_.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result.get_literal_stmt_type());
        plan = result.get_physical_plan();
        extra_info_guard.set_cur_plan(plan);

        if (get_is_com_filed_list()) {
          result.set_is_com_filed_list();
          result.set_wildcard_string(wild_str_);
        }

        //response_result
        if (OB_FAIL(ret)) {
        //TODO shengle, confirm whether 4.0 is required
        //} else if (OB_FAIL(fill_feedback_session_info(*result, session))) {
          //need_response_error = true;
          //LOG_WARN("failed to fill session info", K(ret));
        } else if (OB_FAIL(response_result(result,
                                           force_sync_resp,
                                           async_resp_used))) {
          ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
          if (OB_ISNULL(plan_ctx)) {
            LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
          } else {
            if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
              LOG_WARN("execute query fail", K(ret), "timeout_timestamp",
                       plan_ctx->get_timeout_timestamp());
            }
          }
        }
      }

      int tmp_ret = OB_SUCCESS;
      tmp_ret = OB_E(EventTable::EN_PRINT_QUERY_SQL) OB_SUCCESS;
      if (OB_SUCCESS != tmp_ret) {
        LOG_INFO("query info:", K(sql_),
                 "sess_id", result.get_session().get_sessid(),
                 "trans_id", result.get_session().get_tx_id());
      }

      //监控项统计结束
      exec_end_timestamp_ = ObTimeUtility::current_time();

      // some statistics must be recorded for plan stat, even though sql audit disabled
      bool first_record = (1 == audit_record.try_cnt_);
      ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
      audit_record.exec_timestamp_.update_stage_time();

      if (enable_perf_event) {
        audit_record.exec_record_.record_end(di);
        record_stat(result.get_stmt_type(), exec_end_timestamp_);
        audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
        audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
        audit_record.update_event_stage_state();
      }

      if (enable_perf_event && !THIS_THWORKER.need_retry()
        && OB_NOT_NULL(result.get_physical_plan())) {
        const int64_t time_cost = exec_end_timestamp_ - get_receive_timestamp();
        ObSQLUtils::record_execute_time(result.get_physical_plan()->get_plan_type(), time_cost);
      }
      // 重试需要满足一下条件：
      // 1. rs.open 执行失败
      // 2. 没有给客户端返回结果，本次执行没有副作用
      // 3. need_retry(result, ret)：schema 或 location cache 失效
      // 4. 小于重试次数限制
      if (OB_UNLIKELY(retry_ctrl_.need_retry())) {
        if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
          //锁冲突重试不打印日志, 避免刷屏
          LOG_WARN("try to execute again",
                   K(ret),
                   N_TYPE, result.get_stmt_type(),
                   "retry_type", retry_ctrl_.get_retry_type(),
                   "timeout_remain", THIS_WORKER.get_timeout_remain());
        }
      } else {

        // 首个plan执行完成后立即freeze partition hit
        // partition_hit一旦freeze后，后继的try_set_bool操作都不生效
        if (OB_LIKELY(NULL != result.get_physical_plan())) {
          session.partition_hit().freeze();
        }

        // store the warning message from the most recent statement in the current session
        if ((OB_SUCC(ret) && is_diagnostics_stmt) || async_resp_used) {
          // If diagnostic stmt execute successfully, it dosen't clear the warning message.
          // Or if it response to client asynchronously, it doesn't clear the warning message here,
          // but will do it in the callback thread.
          session.update_show_warnings_buf();
        } else {
          session.set_show_warnings_buf(ret); // TODO: 挪个地方性能会更好，减少部分wb拷贝
        }

        if (OB_FAIL(ret) && !async_resp_used && need_response_error && is_conn_valid() && !THIS_WORKER.need_retry()) {
          if (OB_ERR_PROXY_REROUTE == ret) {
            LOG_DEBUG("query should be rerouted", K(ret), K(async_resp_used));
          } else {
            LOG_WARN("query failed", K(ret), K(session),
                     "sql", ctx_.is_sensitive_ ? ObString(OB_MASKED_STR) : sql,
                     K(retry_ctrl_.need_retry()));
          }
          // 当need_retry=false时，可能给客户端回过包了，可能还没有回过任何包。
          // 不过，可以确定：这个请求出错了，还没处理完。如果不是已经交给异步EndTrans收尾，
          // 则需要在下面回复一个error_packet作为收尾。否则后面没人帮忙发错误包给客户端了，
          // 可能会导致客户端挂起等回包。
          bool is_partition_hit = session.get_err_final_partition_hit(ret);
          int err = send_error_packet(ret, NULL, is_partition_hit, (void *)ctx_.get_reroute_info());
          if (OB_SUCCESS != err) {  // 发送error包
            LOG_WARN("send error packet failed", K(ret), K(err));
          }
        }
      }
    }

    audit_record.status_ = (0 == ret || OB_ITER_END == ret)
        ? REQUEST_SUCC : (ret);
    if (enable_sql_audit) {
      audit_record.seq_ = 0;  //don't use now
      audit_record.execution_id_ = session.get_current_execution_id();
      audit_record.client_addr_ = session.get_peer_addr();
      audit_record.user_client_addr_ = session.get_user_client_addr();
      audit_record.user_group_ = THIS_WORKER.get_group_id();
      MEMCPY(audit_record.sql_id_, ctx_.sql_id_, (int32_t)sizeof(audit_record.sql_id_));
      if (NULL != plan) {
        audit_record.plan_type_ = plan->get_plan_type();
        audit_record.table_scan_ = plan->contain_table_scan();
        audit_record.plan_id_ = plan->get_plan_id();
        audit_record.plan_hash_ = plan->get_plan_hash_value();
        audit_record.rule_name_ = const_cast<char *>(plan->get_rule_name().ptr());
        audit_record.rule_name_len_ = plan->get_rule_name().length();
        audit_record.partition_hit_ = session.partition_hit().get_bool();
      }
      if (OB_FAIL(ret) && audit_record.trans_id_ == 0) {
        // normally trans_id is set in the `start-stmt` phase,
        // if `start-stmt` hasn't run, set trans_id from session if an active txn exist
        audit_record.trans_id_ = session.get_tx_id();
      }
      audit_record.affected_rows_ = result.get_affected_rows();
      audit_record.return_rows_ = result.get_return_rows();
      audit_record.partition_cnt_ = result.get_exec_context()
                                          .get_das_ctx()
                                          .get_related_tablet_cnt();
      audit_record.expected_worker_cnt_ = result.get_exec_context()
                                                .get_task_exec_ctx()
                                                .get_expected_worker_cnt();
      audit_record.used_worker_cnt_ = result.get_exec_context()
                                            .get_task_exec_ctx()
                                            .get_admited_worker_cnt();

      audit_record.is_executor_rpc_ = false;
      audit_record.is_inner_sql_ = false;
      audit_record.is_hit_plan_cache_ = result.get_is_from_plan_cache();
      audit_record.is_multi_stmt_ = session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS;
      audit_record.is_batched_multi_stmt_ = ctx_.multi_stmt_item_.is_batched_multi_stmt();

      OZ (store_params_value_to_str(allocator, session, result.get_ps_params()));
      audit_record.params_value_ = params_value_;
      audit_record.params_value_len_ = params_value_len_;
      audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();

      ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_ISNULL(plan_ctx)) {
        //do nothing
      } else {
        audit_record.consistency_level_ = plan_ctx->get_consistency_level();
      }
    }
      //update v$sql statistics
    if ((OB_SUCC(ret) || audit_record.is_timeout())
        && session.get_local_ob_enable_plan_cache()
        && !retry_ctrl_.need_retry()) {
      ObIArray<ObTableRowCount> *table_row_count_list = NULL;
      ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_ISNULL(plan_ctx)) {
        // do nothing
      } else {
        table_row_count_list = &(plan_ctx->get_table_row_count_list());
        audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
      }
      if (NULL != plan) {
        if (!(ctx_.self_add_plan_) && ctx_.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
                                 false, // false mean not first update plan stat
                                 result.get_exec_context().get_is_evolution(),
                                 table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        } else if (ctx_.self_add_plan_ && !ctx_.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
                                 true,
                                 result.get_exec_context().get_is_evolution(),
                                 table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        } else if (ctx_.self_add_plan_ && ctx_.plan_cache_hit_) {
          // spm evolution plan first execute
          plan->update_plan_stat(audit_record,
                                 true,
                                 result.get_exec_context().get_is_evolution(),
                                 table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        }
      }
    }
    // reset thread waring buffer in sync mode
    if (!async_resp_used) {
      clear_wb_content(session);
    }

    need_disconnect = (result.get_exec_context().need_disconnect()
                       && !is_query_killed_return(ret));//明确是kill query时，不应该断连接
    if (need_disconnect) {
      LOG_WARN("need disconnect", K(ret), K(need_disconnect));
    }
    bool is_need_retry = THIS_THWORKER.need_retry() ||
        RETRY_TYPE_NONE != retry_ctrl_.get_retry_type();
#ifdef OB_BUILD_SPM
    if (!is_need_retry) {
      (void)ObSQLUtils::handle_plan_baseline(audit_record, plan, ret, ctx_);
    }
#endif
    (void)ObSQLUtils::handle_audit_record(is_need_retry, EXECUTE_LOCAL, session,
        ctx_.is_sensitive_);
#ifdef OB_BUILD_AUDIT_SECURITY
    // 对于触发重试的语句不需要进行审计，以免一条语句被审计多次
    if (!retry_ctrl_.need_retry() && !async_resp_used) {
      (void)ObSecurityAuditUtils::handle_security_audit(result,
                                                        ctx_.schema_guard_,
                                                        ctx_.cur_stmt_,
                                                        ObString::make_empty_string(),
                                                        ret);
    }
#endif
  }
  return ret;
}

int ObMPQuery::store_params_value_to_str(ObIAllocator &allocator,
                                         sql::ObSQLSessionInfo &session,
                                         common::ParamStore &params)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t length = OB_MAX_SQL_LENGTH;
  CK (OB_NOT_NULL(params_value_ = static_cast<char *>(allocator.alloc(OB_MAX_SQL_LENGTH))));
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    const common::ObObjParam &param = params.at(i);
    if (param.is_ext()) {
      pos = 0;
      params_value_ = NULL;
      params_value_len_ = 0;
      break;
    } else {
      OZ (param.print_sql_literal(params_value_, length, pos, allocator, TZ_INFO(&session)));
      if (i != params.count() - 1) {
        OZ (databuff_printf(params_value_, length, pos, allocator, ","));
      }
    }
  }
  if (OB_FAIL(ret)) {
    params_value_ = NULL;
    params_value_len_ = 0;
    ret = OB_SUCCESS;
  } else {
    params_value_len_ = pos;
  }
  return ret;
}

//int ObMPQuery::fill_feedback_session_info(ObMySQLResultSet &result,
//                                          ObSQLSessionInfo &session)
//{
//  int ret = OB_SUCCESS;
//  ObPhysicalPlan *temp_plan = NULL;
//  ObTaskExecutorCtx *temp_task_ctx = NULL;
//  ObSchemaGetterGuard *schema_guard = NULL;
//  if (session.is_abundant_feedback_support() &&
//      NULL != (temp_plan = result.get_physical_plan()) &&
//      NULL != (temp_task_ctx = result.get_exec_context().get_task_executor_ctx()) &&
//      NULL != (schema_guard = ctx_.schema_guard_) &&
//      temp_plan->get_plan_type() == ObPhyPlanType::OB_PHY_PLAN_REMOTE &&
//      temp_plan->get_location_type() != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN &&
//      temp_task_ctx->get_table_locations().count() == 1 &&
//      temp_task_ctx->get_table_locations().at(0).get_partition_location_list().count() == 1) {
//    bool is_cache_hit = false;
//    ObFBPartitionParam param;
//    //FIXME: should remove ObPartitionKey
//    ObPartitionKey partition_key;
//    ObPartitionLocation partition_loc;
//    const ObTableSchema *table_schema = NULL;
//    ObPartitionReplicaLocationIArray &pl_array =
//        temp_task_ctx->get_table_locations().at(0).get_partition_location_list();
//    if (OB_FAIL(pl_array.at(0).get_partition_key(partition_key))) {
//      LOG_WARN("failed to get partition key", K(ret));
//    } else if (OB_FAIL(temp_cache->get(partition_key,
//                                       partition_loc,
//                                       0,
//                                       is_cache_hit))) {
//      LOG_WARN("failed to get partition location", K(ret));
//    } else if (OB_FAIL(schema_guard->get_table_schema(partition_key.get_tenant_id(),
//                                                      partition_key.get_table_id(),
//                                                      table_schema))) {
//      LOG_WARN("failed to get table schema", K(ret), K(partition_key));
//    } else if (OB_ISNULL(table_schema)) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("null table schema", K(ret));
//    } else if (OB_FAIL(build_fb_partition_param(*table_schema, partition_loc, param))) {
//      LOG_WARN("failed to build fb partition pararm", K(ret));
//    } else if (OB_FAIL(session.set_partition_location_feedback(param))) {
//      LOG_WARN("failed to set partition location feedback", K(param), K(ret));
//    } else { /*do nothing*/ }
//  } else { /*do nothing*/}
//  return ret;
//}

//int ObMPQuery::build_fb_partition_param(
//    const ObTableSchema &table_schema,
//    const ObPartitionLocation &partition_loc,
//    ObFBPartitionParam &param) {
//  INIT_SUCC(ret);
//  param.schema_version_ = table_schema.get_schema_version();
//  int64_t origin_partition_idx = OB_INVALID_ID;
//  if (OB_FAIL(param.pl_.assign(partition_loc))) {
//    LOG_WARN("fail to assign pl", K(partition_loc), K(ret));
//  }
//  // when table partition_id to client, we need convert it to
//  // real partition idx(e.g. hash partition split)
//  else if (OB_FAIL(table_schema.convert_partition_id_to_idx(
//          partition_loc.get_partition_id(), origin_partition_idx))) {
//    LOG_WARN("fail to convert partition id", K(partition_loc), K(ret));
//  } else {
//    param.original_partition_id_ = origin_partition_idx;
//  }
//
//  return ret;
//}

int ObMPQuery::check_readonly_stmt(ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  bool is_readonly = false;
  //在该阶段，show语句会转换为select语句，
  //literal_stmt_type若不为stmt::T_NONE，则表示原有的类型show
  const stmt::StmtType type = stmt::T_NONE == result.get_literal_stmt_type() ?
                              result.get_stmt_type() :
                              result.get_literal_stmt_type();
  ObConsistencyLevel consistency = INVALID_CONSISTENCY;
  if (OB_FAIL(is_readonly_stmt(result, is_readonly))) {
    LOG_WARN("check stmt is readonly fail", K(ret), K(result));
  } else if (!is_readonly) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("stmt is not readonly", K(ret), K(result));
  } else if (stmt::T_SELECT == type) {
    //对select语句，需要禁强一致读
    //通过设置/*+read_consistency()*/ hint
    //or 指定session级别ob_read_consistency = 2
    //来设置弱一致性读
    const int64_t table_count = DAS_CTX(result.get_exec_context()).get_table_loc_list().size();
    if (0 == table_count) {
      //此处比较特殊，jdbc在发送查询语句时会带上特殊的语句select @@session.tx_read_only;
      //为方便obtest测试，需要放开对无table_locatitons的select语句的限制
    } else if (OB_FAIL(result.get_read_consistency(consistency))) {
      LOG_WARN("get read consistency fail", K(ret));
    } else if (WEAK != consistency) {
      ret = OB_ERR_READ_ONLY;
      LOG_WARN("strong consistency read is not allowed", K(ret), K(type), K(consistency));
    }
  }
  return ret;
}

int ObMPQuery::is_readonly_stmt(ObMySQLResultSet &result, bool &is_readonly)
{
  int ret = OB_SUCCESS;
  is_readonly = false;
  const stmt::StmtType type = stmt::T_NONE == result.get_literal_stmt_type() ?
                              result.get_stmt_type() :
                              result.get_literal_stmt_type();
  switch (type) {
    case stmt::T_SELECT: {
      //对select...for update语句，也需要禁止
      ObPhysicalPlan *physical_plan = result.get_physical_plan();
      if (NULL == physical_plan) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical_plan should not be null", K(ret));
      } else if (physical_plan->has_for_update()) {
        is_readonly = false;
      } else {
        is_readonly = true;
      }
      break;
    }
    case stmt::T_VARIABLE_SET: {
      //禁止set @@global.variable语句
      if (result.has_global_variable()) {
        is_readonly = false;
      } else {
        is_readonly = true;
      }
      break;
    }
    case stmt::T_EXPLAIN:
    case stmt::T_SHOW_TABLES:
    case stmt::T_SHOW_DATABASES:
    case stmt::T_SHOW_COLUMNS:
    case stmt::T_SHOW_VARIABLES:
    case stmt::T_SHOW_TABLE_STATUS:
    case stmt::T_SHOW_SCHEMA:
    case stmt::T_SHOW_CREATE_DATABASE:
    case stmt::T_SHOW_CREATE_TABLE:
    case stmt::T_SHOW_CREATE_VIEW:
    case stmt::T_SHOW_PARAMETERS:
    case stmt::T_SHOW_SERVER_STATUS:
    case stmt::T_SHOW_INDEXES:
    case stmt::T_SHOW_WARNINGS:
    case stmt::T_SHOW_ERRORS:
    case stmt::T_SHOW_PROCESSLIST:
    case stmt::T_SHOW_CHARSET:
    case stmt::T_SHOW_COLLATION:
    case stmt::T_SHOW_TABLEGROUPS:
    case stmt::T_SHOW_STATUS:
    case stmt::T_SHOW_TENANT:
    case stmt::T_SHOW_CREATE_TENANT:
    case stmt::T_SHOW_TRACE:
    case stmt::T_SHOW_TRIGGERS:
    case stmt::T_SHOW_ENGINES:
    case stmt::T_SHOW_PRIVILEGES:
    case stmt::T_SHOW_RESTORE_PREVIEW:
    case stmt::T_SHOW_GRANTS:
    case stmt::T_SHOW_QUERY_RESPONSE_TIME:
    case stmt::T_SHOW_RECYCLEBIN:
    case stmt::T_SHOW_SEQUENCES:
    case stmt::T_HELP:
    case stmt::T_USE_DATABASE:
    case stmt::T_SET_NAMES: //read only not restrict it
    case stmt::T_START_TRANS:
    case stmt::T_END_TRANS: {
      is_readonly = true;
      break;
    }
    default: {
      is_readonly = false;
      break;
    }
  }
  return ret;
}
int ObMPQuery::deserialize()
{
  int ret = OB_SUCCESS;

  //OB_ASSERT(req_);
  //OB_ASSERT(req_->get_type() == ObRequest::OB_MYSQL);
  if ( (OB_ISNULL(req_)) || (req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K(req_));
  } else if (get_is_com_filed_list()) {
    if (OB_FAIL(deserialize_com_field_list())) {
      LOG_WARN("failed to deserialize com field list", K(ret));
    }
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    sql_.assign_ptr(const_cast<char *>(pkt.get_cdata()), pkt.get_clen()-1);
  }

  return ret;
}

// return false only if send packet fail.
OB_INLINE int ObMPQuery::response_result(ObMySQLResultSet &result,
                                         bool force_sync_resp,
                                         bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(sql_execute);
  //ac = 1时线程新启事务进行oracle临时表数据清理会和clog回调形成死锁, 这里改成同步方式
  ObSQLSessionInfo &session = result.get_session();
  CHECK_COMPATIBILITY_MODE(&session);

#ifndef OB_BUILD_SPM
  bool need_trans_cb  = result.need_end_trans_callback() && (!force_sync_resp);
#else
  bool need_trans_cb  = result.need_end_trans_callback() &&
                        (!force_sync_resp) &&
                        (!ctx_.spm_ctx_.check_execute_status_);
#endif

  // 通过判断 plan 是否为 null 来确定是 plan 还是 cmd
  // 针对 plan 和 cmd 分开处理，逻辑会较为清晰。
  if (OB_LIKELY(NULL != result.get_physical_plan())) {
    if (need_trans_cb) {
      ObAsyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      // NOTE: sql_end_cb必须在drv.response_result()之前初始化好
      ObSqlEndTransCb &sql_end_cb = session.get_mysql_end_trans_cb();
      if (OB_FAIL(sql_end_cb.init(packet_sender_, &session))) {
        LOG_WARN("failed to init sql end callback", K(ret));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      // 试点ObQuerySyncDriver
      ObSyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      ret = drv.response_result(result);
    }
  } else {
    if (need_trans_cb) {
      ObSqlEndTransCb &sql_end_cb = session.get_mysql_end_trans_cb();
      ObAsyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      if (OB_FAIL(sql_end_cb.init(packet_sender_, &session))) {
        LOG_WARN("failed to init sql end callback", K(ret));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      session.set_pl_query_sender(&drv);
      session.set_ps_protocol(result.is_ps_protocol());
      ret = drv.response_result(result);
      session.set_pl_query_sender(NULL);
    }
  }

  return ret;
}

inline void ObMPQuery::record_stat(const stmt::StmtType type, const int64_t end_time) const
{
#define ADD_STMT_STAT(type)                     \
  case stmt::T_##type:                          \
    EVENT_INC(SQL_##type##_COUNT);              \
    EVENT_ADD(SQL_##type##_TIME, time_cost);    \
    break
  const int64_t time_cost = end_time - get_receive_timestamp();
  if (!THIS_THWORKER.need_retry())
  {
    switch (type)
    {
      ADD_STMT_STAT(SELECT);
      ADD_STMT_STAT(INSERT);
      ADD_STMT_STAT(REPLACE);
      ADD_STMT_STAT(UPDATE);
      ADD_STMT_STAT(DELETE);
    default:
    {
      EVENT_INC(SQL_OTHER_COUNT);
      EVENT_ADD(SQL_OTHER_TIME, time_cost);
    }
    }
  }
#undef ADD_STMT_STAT
}

int ObMPQuery::deserialize_com_field_list()
{
  int ret = OB_SUCCESS;
  //如果设置了只需要返回列定义，说明client传来的是COM_FIELD_LIST命令
  /* mysql中的COM_FIELD_LIST命令用于获取table中的列定义，其从client to server的packet为:
  *  1              [04] COM_FIELD_LIST
  *  string[NUL]    table
  *  string[EOF]    field wildcard
  *  首先是CMD类型，然后是表名，最后是匹配条件
  *
  * server to client的packet为下面中的其中一个:
  * 1. a ERR_Packet（返回一个错误包）
  * 2. one or more Column Definition packets and a closing EOF_Packet（返回n个列定义+EOF）
  *
  * 由于普通的select的查询结果已经包含了column定义，同时最大限度复用当前的代码逻辑，因此可以将COM_FIELD_LIST的命令
  * 等价于:
  * select * from table limit 0 ==> 获取filed define ==> 根据field wildcard 按需反回 Column Definition
  *
  * 参考：https://dev.mysql.com/doc/internals/en/com-field-list.html
   */
  ObIAllocator *alloc = &THIS_WORKER.get_sql_arena_allocator();
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(alloc), K(ret));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char *str = pkt.get_cdata();
    uint32_t length = pkt.get_clen();
    const char *str1 = "select * from ";
    const char *str2 = " where 0";
    int64_t i = 0;
    const int64_t str1_len = strlen(str1);
    const int64_t str2_len = strlen(str2);
    const int pre_size = str1_len + str2_len;
    //寻找client传过来的table_name和filed wildcard之间的分隔符（table_name [NULL] filed wildcard）
    for (; static_cast<int>(str[i]) != 0 && i < length; ++i) {}
    char *dest_str = static_cast<char *>(alloc->alloc(length + pre_size));
    if (OB_ISNULL(dest_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc", K(dest_str));
    } else {
      char *buf = dest_str;
      uint32_t real_len = 0;
      MEMSET(buf, 0, length + pre_size);
      MEMCPY(buf, str1, str1_len);
      buf = buf + str1_len;
      real_len = real_len + str1_len;
      MEMCPY(buf, str, i);
      buf = buf + i;
      real_len = real_len + i;
      MEMCPY(buf, str2, str2_len);
      real_len = real_len + str2_len;
      sql_.assign_ptr(dest_str, real_len);
      //extract wildcard
      if (i + 1 < length - 1) {
        wild_str_.assign_ptr(str + i + 1, (length - 1) - (i +1));
      }
    }
  }
  return ret;
}

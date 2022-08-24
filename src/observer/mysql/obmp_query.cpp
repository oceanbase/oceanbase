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
#include "share/ob_worker.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/profile/ob_perf_event.h"
#include "share/ob_debug_sync.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
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
#include "sql/ob_sql_partition_location_cache.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/ob_query_exec_ctx_mgr.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_async_cmd_driver.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "observer/ob_req_time_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/ob_server.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "storage/ob_partition_service.h"
#include "lib/rc/context.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

using namespace oceanbase::sql;
ObMPQuery::ObMPQuery(const ObGlobalContext& gctx)
    : ObMPBase(gctx),
      ctx_(),
      retry_ctrl_(/*ctx_.retry_info_*/),
      sql_(),
      sql_len_(0),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      is_com_filed_list_(false),
      wild_str_()
{
  ctx_.exec_type_ = MpQuery;
}

ObMPQuery::~ObMPQuery()
{}

int ObMPQuery::process()
{
#ifdef PERF_MODE
  static OMPKOK okp;
  if (sql_.case_compare("ping") == 0) {
    okp.set_capability(get_conn()->cap_flags_);
    response_packet(okp);
    return flush_buffer(true);
  }
#endif

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo* sess = NULL;
  uint32_t sessid = 0;
  bool need_response_error = true;
  bool need_disconnect = true;
  bool async_resp_used = false;  // response client in trans thread
  int64_t query_timeout = 0;
  ObCurTraceId::TraceId* cur_trace_id = ObCurTraceId::get_trace_id();
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(req_) || OB_ISNULL(conn) || OB_ISNULL(cur_trace_id)) {
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
  } else if (OB_FAIL(setup_user_resource_group(*conn, sess->get_effective_tenant_id(), sess->get_user_id()))) {
    LOG_WARN("fail setup user resource group", K(ret));
  } else {
    share::CompatModeGuard g(sess->get_compatibility_mode() == ORACLE_MODE ? share::ObWorker::CompatMode::ORACLE
                                                                           : share::ObWorker::CompatMode::MYSQL);
    THIS_WORKER.set_session(sess);
    ObSQLSessionInfo& session = *sess;
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_use_static_typing_engine(GCONF.enable_static_engine_for_query());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    int64_t val = 0;
    const bool check_throttle = extract_pure_id(sess->get_user_id()) != OB_SYS_USER_ID;
    if (check_throttle && !sess->is_inner() && sess->get_raw_audit_record().try_cnt_ == 0 &&
        ObWorker::WS_OUT_OF_THROTTLE == THIS_THWORKER.check_rate_limiter()) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_WARN("query is throttled", K(ret), K(sess->get_user_id()));
      need_disconnect = false;
    } else if (OB_SUCC(sess->get_sql_throttle_current_priority(val))) {
      THIS_WORKER.set_sql_throttle_current_priority(check_throttle ? val : -1);
      if (ObWorker::WS_OUT_OF_THROTTLE == THIS_THWORKER.check_qtime_throttle()) {
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
      const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
      int64_t packet_len = pkt.get_clen();
      if (OB_UNLIKELY(!session.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid session", K_(sql), K(ret));
      } else if (OB_UNLIKELY(session.is_zombie())) {
        // session has been killed some moment ago
        ret = OB_ERR_SESSION_INTERRUPTED;
        LOG_WARN("session has been killed",
            K(session.get_session_state()),
            K_(sql),
            K(session.get_sessid()),
            "proxy_sessid",
            session.get_proxy_sessid(),
            K(ret));
      } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
        // packet size check with session variable max_allowd_packet or net_buffer_length
        ret = OB_ERR_NET_PACKET_TOO_LARGE;
        LOG_WARN("packet too large than allowed for the session", K_(sql), K(ret));
      } else if (OB_FAIL(session.check_and_init_retry_info(*cur_trace_id, sql_))) {
        // retry info and last query trace id should under query lock protection
        LOG_WARN("fail to check and init retry info", K(ret), K(*cur_trace_id), K_(sql));
      } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
        LOG_WARN("fail to get query timeout", K_(sql), K(ret));
      } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                     session.get_effective_tenant_id(), tenant_version))) {
        LOG_WARN("fail get tenant broadcast version", K(ret));
      } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(OB_SYS_TENANT_ID, sys_version))) {
        LOG_WARN("fail get tenant broadcast version", K(ret));
      } else if (pkt.exist_trace_info() &&
                 OB_FAIL(session.update_sys_variable(SYS_VAR_OB_TRACE_INFO, pkt.get_trace_info()))) {
        LOG_WARN("fail to update trace info", K(ret));
      } else {
        THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
        retry_ctrl_.set_tenant_global_schema_version(tenant_version);
        retry_ctrl_.set_sys_global_schema_version(sys_version);
        session.partition_hit().reset();
        session.set_pl_can_retry(true);
        ObLockWaitNode& lock_wait_node = req_->get_lock_wait_node();
        lock_wait_node.set_session_info(session.get_sessid(), session.get_version());

        bool has_more = false;
        bool force_sync_resp = false;
        need_response_error = false;
        ObParser parser(
            THIS_WORKER.get_sql_arena_allocator(), session.get_sql_mode(), session.get_local_collation_connection());
        ObSEArray<ObString, 1> queries;
        ObMPParseStat parse_stat;
        if (GCONF.enable_record_trace_id) {
          PreParseResult pre_parse_result;
          if (OB_FAIL(ObParser::pre_parse(sql_, pre_parse_result))) {
            LOG_WARN("fail to pre parse", K(ret));
          } else {
            session.set_app_trace_id(pre_parse_result.trace_id_);
            LOG_DEBUG(
                "app trace id", "app_trace_id", pre_parse_result.trace_id_, "sessid", session.get_sessid(), K_(sql));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(parser.split_multiple_stmt(sql_, queries, parse_stat))) {
          // need close connection when reach here
          need_response_error = true;
        } else if (OB_UNLIKELY(queries.count() <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          need_response_error = true;  // critical error, close connection
          LOG_ERROR("emtpy query count. client would have suspended. never be here!", K_(sql), K(ret));
        } else if (OB_UNLIKELY(1 == session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS)) {
          // For Multiple Statement
          /* MySQL Multi-Stmt error handling:
           * stop read next sql when reach error:
           *  e.g.:
           *  (1) select 1; select 2; select 3;
           *  select 1 success, select 2 syntax error, select 3 will not be executed
           *  (2) select 1; drop table not_exists_table; select 3;
           *  select 1 success, drop table not_exists_table fail on table not exist, select 3 will not be executed
           */
          bool optimization_done = false;
          if (queries.count() > 1 &&
              OB_FAIL(try_batched_multi_stmt_optimization(
                  session, queries, parse_stat, optimization_done, async_resp_used, need_disconnect))) {
            LOG_WARN("failed to try multi-stmt-optimization", K(ret));
          } else if (!optimization_done) {
            ARRAY_FOREACH(queries, i)
            {
              need_disconnect = true;
              // FIXME  NG_TRACE_EXT(set_disconnect, OB_ID(disconnect), true, OB_ID(pos), "multi stmt begin");
              if (OB_UNLIKELY(parse_stat.parse_fail_ && (i == parse_stat.fail_query_idx_) &&
                              (OB_ERR_PARSE_SQL != parse_stat.fail_ret_))) {
                ret = parse_stat.fail_ret_;
                need_response_error = true;
                break;
              } else {
                has_more = (queries.count() > i + 1);
                force_sync_resp = queries.count() <= 1 ? false : true;
                ret = process_single_stmt(ObMultiStmtItem(true, i, queries.at(i)),
                    session,
                    has_more,
                    force_sync_resp,
                    async_resp_used,
                    need_disconnect);
              }
            }
          }
          EVENT_INC(SQL_MULTI_QUERY_COUNT);
          if (queries.count() <= 1) {
            EVENT_INC(SQL_MULTI_ONE_QUERY_COUNT);
          }
        } else {  // OB_CLIENT_MULTI_STATEMENTS not enabled
          if (OB_UNLIKELY(queries.count() != 1)) {
            ret = OB_ERR_PARSER_SYNTAX;
            need_disconnect = false;
            need_response_error = true;
            LOG_WARN(
                "unexpected error. multi stmts sql while OB_CLIENT_MULTI_STATEMENTS not enabled.", K(ret), K(sql_));
          } else {
            EVENT_INC(SQL_SINGLE_QUERY_COUNT);
            // for single Statement
            ret = process_single_stmt(
                ObMultiStmtItem(false, 0, sql_), session, has_more, force_sync_resp, async_resp_used, need_disconnect);
          }
        }

        // logging SQL stmt for diagnosis
        if (OB_FAIL(ret)) {
          if (OB_EAGAIN == ret) {
            // large query, do nothing
          } else if (conn_valid_) {  // The memory of sql string is invalid if conn_valid_ has been set false.
            if (OB_UNLIKELY((retry_ctrl_.need_retry() && OB_TRY_LOCK_ROW_CONFLICT == ret))) {
              // lock conflict will retry frequently, do not logging here
            } else {
              LOG_WARN("fail execute sql",
                  "sql_id",
                  ctx_.sql_id_,
                  "sql",
                  ObString(sql_len_, sql_buf_),
                  K(sessid),
                  KR(ret),
                  K(need_disconnect));
            }
          } else {
            LOG_WARN("fail execute sql", K_(conn_valid), KR(ret));
          }
        }
      }
    }
    session.check_and_reset_retry_info(*cur_trace_id, THIS_WORKER.need_retry());
  }

  if (OB_FAIL(ret) && need_response_error && conn_valid_) {
    send_error_packet(ret, NULL);
    if (need_disconnect) {
      disconnect();
    }
    need_disconnect = false;
    LOG_WARN("disconnect connection when process query", KR(ret));
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(need_disconnect) && conn_valid_) {
    disconnect();
    LOG_WARN("disconnect connection", KR(ret));
  }

  // not flush_buffer() for async response
  if (!THIS_WORKER.need_retry()) {
    if (async_resp_used) {
      req_has_wokenup_ = true;
    } else if (OB_UNLIKELY(!conn_valid_)) {
      tmp_ret = OB_CONNECT_ERROR;
      LOG_WARN("connection in error, maybe has disconnected", K(tmp_ret));
    } else if (req_has_wokenup_) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("req_has_wokenup, resource maybe has destroy", K(tmp_ret));
    } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = flush_buffer(true)))) {
      LOG_WARN("failed to flush_buffer", K(tmp_ret));
    }
  }

  THIS_WORKER.set_session(NULL);  // clear session

  if (sess != NULL) {
    revert_session(sess);  // current ignore revert session ret
  }

  return (OB_SUCCESS != ret) ? ret : tmp_ret;
}

/*
 * Try to evaluate multiple update queries as a single query to optimize rpc cost
 */
int ObMPQuery::try_batched_multi_stmt_optimization(sql::ObSQLSessionInfo& session, common::ObIArray<ObString>& queries,
    const ObMPParseStat& parse_stat, bool& optimization_done, bool& async_resp_used, bool& need_disconnect)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  bool force_sync_resp = true;
  bool enable_batch_opt = false;
  optimization_done = false;
  if (queries.count() <= 1 || parse_stat.parse_fail_ || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2230) {
    /*do nothing*/
  } else {
    // avoid holding the read latch when generating query plan
    enable_batch_opt = session.is_enable_batched_multi_statement();
  }
  if (!enable_batch_opt) {
    // do nothing
  } else if (OB_FAIL(process_single_stmt(ObMultiStmtItem(false, 0, sql_, &queries),
                 session,
                 has_more,
                 force_sync_resp,
                 async_resp_used,
                 need_disconnect))) {
    if (OB_BATCHED_MULTI_STMT_ROLLBACK == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
    } else {
      LOG_WARN("failed to process single stmt", K(ret));
    }
  } else {
    optimization_done = true;
  }
  LOG_TRACE(
      "succeed to try batched multi-stmt optimization", K(optimization_done), K(queries.count()), K(enable_batch_opt));
  return ret;
}

int ObMPQuery::after_process()
{
  int ret = OB_SUCCESS;
  ret = ObMPBase::after_process();
  return ret;
}

int ObMPQuery::process_single_stmt(const ObMultiStmtItem& multi_stmt_item, ObSQLSessionInfo& session,
    bool has_more_result, bool force_sync_resp, bool& async_resp_used, bool& need_disconnect)
{
  int ret = OB_SUCCESS;
  // LOG_DEBUG("new query", K(sql));
  bool need_response_error = true;
  bool use_sess_trace = false;
  const bool enable_trace_log = lib::is_trace_log_enabled();

  // after setup_wb, all WARNING will write to session's WARNING BUFFER
  setup_wb(session);
  session.set_curr_trans_last_stmt_end_time(0);

  ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  if (OB_FAIL(init_process_var(ctx_, multi_stmt_item, session, vt_iter_factory, use_sess_trace))) {
    LOG_WARN("init process var failed.", K(ret), K(multi_stmt_item));
  } else {
    if (enable_trace_log) {
      // set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    // obproxy may use 'SET @@last_schema_version = xxxx' to set newest schema,
    // observer will force refresh schema if local_schema_version < last_schema_version;
    if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(), session.get_effective_tenant_id(), &session))) {
      LOG_WARN("failed to check_and_refresh_schema", K(ret));
    } else if (OB_FAIL(session.update_timezone_info())) {
      LOG_WARN("fail to update time zone info", K(ret));
    } else {
      need_response_error = false;
      // need reset for different sql execution
      ctx_.self_add_plan_ = false;
      retry_ctrl_.reset_retry_times();
      do {
        ret = OB_SUCCESS;  // reset error code for local retry
        need_disconnect = true;
        ObSQLMockSchemaGuard mock_schema_guard;
        // do the real work
        retry_ctrl_.clear_state_before_each_retry(session.get_retry_info_for_update());
        lib::ContextParam param;
        param.set_mem_attr(lib::current_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
            .set_properties(lib::USE_TL_PAGE_OPTIONAL)
            .set_page_size(!lib::is_mini_mode() ? OB_MALLOC_BIG_BLOCK_SIZE : OB_MALLOC_MIDDLE_BLOCK_SIZE)
            .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
        CREATE_WITH_TEMP_CONTEXT(param)
        {
          ret = do_process(session, has_more_result, force_sync_resp, async_resp_used, need_disconnect);
          ctx_.clear();
        }
        // set session retry state
        session.set_session_in_retry(retry_ctrl_.need_retry());
        if (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type()) {
          THIS_WORKER.reset_retry_flag();
        }
      } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());
      if (OB_SUCC(ret) && retry_ctrl_.get_retry_times() > 0) {
        // logging stmt which retry success
        LOG_TRACE("sql retry succeed",
            K(ret),
            "retry_times",
            retry_ctrl_.get_retry_times(),
            "sql",
            ObString(sql_len_, sql_buf_));
      }
    }
    if (enable_trace_log) {
      ObThreadLogLevelUtils::clear();
    }
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

  int tmp_ret = OB_SUCCESS;
  // reset WARNING BUFFER
  tmp_ret = do_after_process(session, use_sess_trace, ctx_, async_resp_used);
  if (session.get_in_transaction() && !async_resp_used && OB_SUCC(ret)) {
    session.set_curr_trans_last_stmt_end_time(ObTimeUtility::current_time());
  }
  if (!OB_SUCC(ret) && need_response_error && conn_valid_) {
    send_error_packet(ret, NULL);
  }
  ctx_.reset();
  UNUSED(tmp_ret);
  return ret;
}

OB_INLINE int ObMPQuery::get_tenant_schema_info_(const uint64_t tenant_id, ObTenantCachedSchemaGuardInfo* cache_info,
    ObSchemaGetterGuard*& schema_guard, int64_t& tenant_version, int64_t& sys_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard& cached_guard = cache_info->get_schema_guard();
  bool need_refresh = false;

  if (!cached_guard.is_inited()) {
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
      need_refresh = true;
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, tmp_sys_version))) {
      LOG_WARN("get sys tenant refreshed schema version error", K(ret), "sys_tenant_id", OB_SYS_TENANT_ID);
    } else if (OB_FAIL(cached_guard.get_schema_version(OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get sys schema version", K(ret));
    } else if (tmp_sys_version != sys_version) {
      need_refresh = true;
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (!need_refresh) {
      // get schema guard cached in session
      schema_guard = &(cache_info->get_schema_guard());
    } else if (OB_FAIL(cache_info->refresh_tenant_schema_guard(tenant_id))) {
      LOG_WARN("refresh tenant schema guard failed", K(ret), K(tenant_id));
    } else {
      // get schema guard cached in session
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

OB_INLINE int ObMPQuery::do_process(
    ObSQLSessionInfo& session, bool has_more_result, bool force_sync_resp, bool& async_resp_used, bool& need_disconnect)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData& audit_record = session.get_audit_record();
  bool is_diagnostics_stmt = false;
  bool need_response_error = true;
  const ObString& sql = ctx_.multi_stmt_item_.get_sql();
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();

  single_process_timestamp_ = ObTimeUtility::current_time();
  int64_t stmt_len = min(sql.length(), OB_MAX_SQL_LENGTH);
  if (stmt_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql length", K(ret), K(stmt_len));
  } else if (NULL != sql.ptr()) {
    MEMCPY(sql_buf_, sql.ptr(), stmt_len);
    sql_len_ = stmt_len;
  }
  if (OB_SUCC(ret)) {
    sql_buf_[stmt_len] = '\0';  // make string(char *) will be used
  }

  /* !!!
   * req_timeinfo_guardi must before result
   * !!!
   */
  ObReqTimeGuard req_timeinfo_guard;
  ObMySQLResultSet* result = nullptr;
  ObPhysicalPlan* plan = nullptr;
  ObQueryExecCtx* query_ctx = nullptr;
  ObSchemaGetterGuard* schema_guard = nullptr;
  ObTenantCachedSchemaGuardInfo* cached_schema_info = nullptr;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  ObSqlFatalErrExtraInfoGuard extra_info_guard;
  extra_info_guard.set_cur_sql(sql);
  extra_info_guard.set_tenant_id(session.get_effective_tenant_id());

  if (OB_ISNULL(query_ctx = ObQueryExecCtx::alloc(session))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(cached_schema_info = query_ctx->get_cached_schema_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get cached schema info error", K(ret), KP(cached_schema_info), K(session));
  } else if (OB_FAIL(get_tenant_schema_info_(
                 session.get_effective_tenant_id(), cached_schema_info, schema_guard, tenant_version, sys_version))) {
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
    result = &query_ctx->get_result_set();
    result->get_exec_context().set_need_disconnect(true);
    result->get_exec_context().set_need_change_timeout_ret(true);
    ctx_.schema_guard_ = schema_guard;
    retry_ctrl_.set_tenant_local_schema_version(tenant_version);
    retry_ctrl_.set_sys_local_schema_version(sys_version);
    extra_info_guard.set_exec_context(&(query_ctx->get_result_set().get_exec_context()));
  }

  ObWaitEventStat total_wait_desc;
  ObDiagnoseSessionInfo* di = ObDiagnoseSessionInfo::get_local_diagnose_info();
  if (OB_SUCC(ret)) {
    ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
    if (enable_sql_audit) {
      audit_record.exec_record_.record_start(di);
    }
    // sql::ObSqlPartitionLocationCache sql_location_cache;
    // sql_location_cache.init(gctx_.location_cache_, gctx_.self_addr_);
    result->init_partition_location_cache(gctx_.location_cache_, gctx_.self_addr_, ctx_.schema_guard_);
    result->set_has_more_result(has_more_result);
    ObTaskExecutorCtx& task_ctx = result->get_exec_context().get_task_exec_ctx();
    task_ctx.schema_service_ = gctx_.schema_service_;
    task_ctx.set_query_tenant_begin_schema_version(retry_ctrl_.get_tenant_local_schema_version());
    task_ctx.set_query_sys_begin_schema_version(retry_ctrl_.get_sys_local_schema_version());
    task_ctx.set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
    ctx_.retry_times_ = retry_ctrl_.get_retry_times();
    ctx_.partition_location_cache_ = &(result->get_partition_location_cache());
    // storage::ObPartitionService* ps = static_cast<storage::ObPartitionService *> (GCTX.par_ser_);
    // bool is_read_only = false;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(ctx_.schema_guard_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("newest schema is NULL", K(ret));
    } else if (OB_FAIL(result->init())) {
      LOG_WARN("result set init failed", K(ret));
    } else if (OB_FAIL(set_session_active(sql, session, single_process_timestamp_))) {
      LOG_WARN("fail to set session active", K(ret));
    } else if (OB_FAIL(gctx_.sql_engine_->stmt_query(sql, ctx_, *result))) {
      exec_start_timestamp_ = ObTimeUtility::current_time();
      if (!THIS_WORKER.need_retry()) {
        int cli_ret = OB_SUCCESS;
        retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, *result, ret, cli_ret);
        if (OB_ERR_PROXY_REROUTE == ret) {
          LOG_DEBUG(
              "run stmt_query failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K(sql));
        } else {
          LOG_WARN(
              "run stmt_query failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K(sql));
        }
        ret = cli_ret;
        if (OB_ERR_PROXY_REROUTE == ret) {
          need_response_error = true;
        } else if (OB_BATCHED_MULTI_STMT_ROLLBACK == ret) {
          need_response_error = false;
        }
      }
    } else {
      if (enable_perf_event) {
        exec_start_timestamp_ = ObTimeUtility::current_time();
      }
      need_response_error = false;
      is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result->get_literal_stmt_type());
      ctx_.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result->get_literal_stmt_type());
      session.set_last_trace_id(ObCurTraceId::get_trace_id());
      plan = result->get_physical_plan();
      extra_info_guard.set_cur_plan(plan);

      if (get_is_com_filed_list()) {
        result->set_is_com_filed_list();
        result->set_wildcard_string(wild_str_);
      }

      // response_result
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fill_feedback_session_info(*result, session))) {
        need_response_error = true;
        LOG_WARN("failed to fill session info", K(ret));
      } else if (OB_FAIL(response_result(*query_ctx, force_sync_resp, async_resp_used))) {
        ObPhysicalPlanCtx* plan_ctx = result->get_exec_context().get_physical_plan_ctx();
        if (OB_ISNULL(plan_ctx)) {
          LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
        } else {
          if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
            LOG_WARN("execute query fail", K(ret), "timeout_timestamp", plan_ctx->get_timeout_timestamp());
          }
        }
      } else {
        // can no overwrite errno code after end trans.
        int refresh_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (refresh_ret = result->check_and_nonblock_refresh_location_cache()))) {
          LOG_WARN("fail to check and nonblock refresh location cache", K(ret), K(refresh_ret));
        }
      }
    }

    if (OB_NOT_NULL(result)) {
      int tmp_ret = OB_SUCCESS;
      tmp_ret = E(EventTable::EN_PRINT_QUERY_SQL) OB_SUCCESS;
      if (OB_SUCCESS != tmp_ret) {
        LOG_INFO("query info:",
            K(sql_),
            "sess_id",
            result->get_session().get_sessid(),
            "trans_id",
            result->get_session().get_trans_desc().get_trans_id());
      }
    }
    exec_end_timestamp_ = ObTimeUtility::current_time();
    if (enable_perf_event) {
      record_stat(result->get_stmt_type(), exec_end_timestamp_);
    }
    if (enable_sql_audit) {
      audit_record.exec_record_.record_end(di);
    }
    // some statistics must be recorded for plan stat
    // even though sql audit disabled
    audit_record.tenant_id_=session.get_effective_tenant_id();
    update_audit_info(total_wait_desc, audit_record);
    // Tetry:
    // 1. rs.open fail
    // 2. no data send to client
    // 3. need_retry(result, ret): schema or location cache expired
    // 4. under retry limit
    if (OB_UNLIKELY(retry_ctrl_.need_retry())) {
      if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
        // no log for lock conflict
        LOG_WARN("try to execute again",
            K(ret),
            N_TYPE,
            result->get_stmt_type(),
            "retry_type",
            retry_ctrl_.get_retry_type(),
            "timeout_remain",
            THIS_WORKER.get_timeout_remain());
      }
    } else {

      if (OB_LIKELY(NULL != result->get_physical_plan())) {
        session.partition_hit().freeze();
      }

      // store the warning message from the most recent statement in the current session
      if ((OB_SUCC(ret) && is_diagnostics_stmt) || async_resp_used) {
        // If diagnostic stmt execute successfully, it dosen't clear the warning message.
        // Or if it response to client asynchronously, it doesn't clear the warning message here,
        // but will do it in the callback thread.
      } else {
        session.set_show_warnings_buf(ret);
      }

      if (!OB_SUCC(ret) && !async_resp_used && need_response_error && conn_valid_ && !THIS_WORKER.need_retry()) {
        if (OB_ERR_PROXY_REROUTE == ret) {
          LOG_DEBUG("query should be rerouted", K(ret), K(async_resp_used));
        } else {
          LOG_WARN("query failed", K(ret), K(retry_ctrl_.need_retry()));
        }
        bool is_partition_hit = session.get_err_final_partition_hit(ret);
        int err = send_error_packet(ret, NULL, is_partition_hit, (void *)&ctx_.reroute_info_);
        if (OB_SUCCESS != err) {  // send error packet
          LOG_WARN("send error packet failed", K(ret), K(err));
        }
      }
    }
  }
  // set read_only
  if (OB_SUCC(ret)) {
    if (session.get_in_transaction()) {
      if (ObStmt::is_write_stmt(result->get_stmt_type(), result->has_global_variable())) {
        session.set_has_exec_write_stmt(true);
      }
    } else {
      session.set_has_exec_write_stmt(false);
    }
  }

  audit_record.status_ = (0 == ret || OB_ITER_END == ret) ? REQUEST_SUCC : (ret);
  const int return_code = audit_record.status_;
  if (enable_sql_audit && result != nullptr) {
    audit_record.seq_ = 0;  // don't use now
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
    }
    audit_record.affected_rows_ = result->get_affected_rows();
    audit_record.return_rows_ = result->get_return_rows();
    audit_record.partition_cnt_ = result->get_exec_context().get_task_exec_ctx().get_related_part_cnt();
    audit_record.expected_worker_cnt_ = result->get_exec_context().get_task_exec_ctx().get_expected_worker_cnt();
    audit_record.used_worker_cnt_ = result->get_exec_context().get_task_exec_ctx().get_allocated_worker_cnt();

    audit_record.is_executor_rpc_ = false;
    audit_record.is_inner_sql_ = false;
    audit_record.is_hit_plan_cache_ = result->get_is_from_plan_cache();
    audit_record.is_multi_stmt_ = session.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS;
    audit_record.trans_hash_ = session.get_trans_desc().get_trans_id().hash();
    audit_record.is_batched_multi_stmt_ = ctx_.multi_stmt_item_.is_batched_multi_stmt();

    ObPhysicalPlanCtx* plan_ctx = result->get_exec_context().get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      // do nothing
    } else {
      audit_record.consistency_level_ = plan_ctx->get_consistency_level();
    }
  }
  // update v$sql statistics
  if ((OB_SUCC(ret) || audit_record.is_timeout()) && session.get_local_ob_enable_plan_cache() &&
      !retry_ctrl_.need_retry() && result != nullptr) {
    ObIArray<ObTableRowCount>* table_row_count_list = NULL;
    ObPhysicalPlanCtx* plan_ctx = result->get_exec_context().get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      // do nothing
    } else {
      table_row_count_list = &(plan_ctx->get_table_row_count_list());
      audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
    }
    if (NULL != plan) {
      if (!(ctx_.self_add_plan_) && ctx_.plan_cache_hit_) {
        plan->update_plan_stat(audit_record,
            false,  // false mean not first update plan stat
            result->get_exec_context().get_is_evolution(),
            table_row_count_list);
        plan->update_cache_access_stat(audit_record.table_scan_stat_);
      } else if (ctx_.self_add_plan_ && !ctx_.plan_cache_hit_) {
        plan->update_plan_stat(audit_record, true, result->get_exec_context().get_is_evolution(), table_row_count_list);
        plan->update_cache_access_stat(audit_record.table_scan_stat_);
      }
    }
  }
  // reset thread waring buffer in sync mode
  if (!async_resp_used) {
    clear_wb_content(session);
  }

  if (result != nullptr) {
    need_disconnect = (result->get_exec_context().need_disconnect() &&
                       ret != OB_ERR_QUERY_INTERRUPTED);  // do not close connection for kill query
    if (need_disconnect) {
      LOG_WARN("need disconnect", K(ret), K(need_disconnect));
    }
    bool is_need_retry = THIS_THWORKER.need_retry() || RETRY_TYPE_NONE != retry_ctrl_.get_retry_type();
    (void)ObSQLUtils::handle_audit_record(is_need_retry, EXECUTE_LOCAL, session, result->get_exec_context());
  }
  ObQueryExecCtx::free(query_ctx);
  query_ctx = nullptr;
  return ret;
}

int ObMPQuery::fill_feedback_session_info(ObMySQLResultSet& result, ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* temp_plan = NULL;
  ObTaskExecutorCtx* temp_task_ctx = NULL;
  ObIPartitionLocationCache* temp_cache = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  if (session.is_abundant_feedback_support() && NULL != (temp_plan = result.get_physical_plan()) &&
      NULL != (temp_task_ctx = result.get_exec_context().get_task_executor_ctx()) &&
      NULL != (temp_cache = temp_task_ctx->get_partition_location_cache()) &&
      NULL != (schema_guard = ctx_.schema_guard_) && temp_plan->get_plan_type() == ObPhyPlanType::OB_PHY_PLAN_REMOTE &&
      temp_plan->get_location_type() != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN &&
      temp_task_ctx->get_table_locations().count() == 1 &&
      temp_task_ctx->get_table_locations().at(0).get_partition_location_list().count() == 1) {
    bool is_cache_hit = false;
    ObFBPartitionParam param;
    ObPartitionKey partition_key;
    ObPartitionLocation partition_loc;
    const ObTableSchema* table_schema = NULL;
    ObPartitionReplicaLocationIArray& pl_array =
        temp_task_ctx->get_table_locations().at(0).get_partition_location_list();
    if (OB_FAIL(pl_array.at(0).get_partition_key(partition_key))) {
      LOG_WARN("failed to get partition key", K(ret));
    } else if (OB_FAIL(temp_cache->get(partition_key, partition_loc, 0, is_cache_hit))) {
      LOG_WARN("failed to get partition location", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(partition_key.get_table_id(), table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table schema", K(ret));
    } else if (OB_FAIL(build_fb_partition_param(*table_schema, partition_loc, param))) {
      LOG_WARN("failed to build fb partition pararm", K(ret));
    } else if (OB_FAIL(session.set_partition_location_feedback(param))) {
      LOG_WARN("failed to set partition location feedback", K(param), K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObMPQuery::build_fb_partition_param(
    const ObTableSchema& table_schema, const ObPartitionLocation& partition_loc, ObFBPartitionParam& param)
{
  INIT_SUCC(ret);
  param.schema_version_ = table_schema.get_schema_version();
  int64_t origin_partition_idx = OB_INVALID_ID;
  if (OB_FAIL(param.pl_.assign(partition_loc))) {
    LOG_WARN("fail to assign pl", K(partition_loc), K(ret));
  }
  // when table partition_id to client, we need convert it to
  // real partition idx(e.g. hash partition split)
  else if (OB_FAIL(table_schema.convert_partition_id_to_idx(partition_loc.get_partition_id(), origin_partition_idx))) {
    LOG_WARN("fail to convert partition id", K(partition_loc), K(ret));
  } else {
    param.original_partition_id_ = origin_partition_idx;
  }

  return ret;
}

int ObMPQuery::check_readonly_stmt(ObMySQLResultSet& result)
{
  int ret = OB_SUCCESS;
  bool is_readonly = false;
  const stmt::StmtType type =
      stmt::T_NONE == result.get_literal_stmt_type() ? result.get_stmt_type() : result.get_literal_stmt_type();
  ObConsistencyLevel consistency = INVALID_CONSISTENCY;
  if (OB_FAIL(is_readonly_stmt(result, is_readonly))) {
    LOG_WARN("check stmt is readonly fail", K(ret), K(result));
  } else if (!is_readonly) {
    ret = OB_ERR_READ_ONLY;
    LOG_WARN("stmt is not readonly", K(ret), K(result));
  } else if (stmt::T_SELECT == type) {
    const int64_t table_count = result.get_exec_context().get_task_exec_ctx().get_table_locations().count();
    if (0 == table_count) {
    } else if (OB_FAIL(result.get_read_consistency(consistency))) {
      LOG_WARN("get read consistency fail", K(ret));
    } else if (WEAK != consistency) {
      ret = OB_ERR_READ_ONLY;
      LOG_WARN("strong consistency read is not allowed", K(ret), K(type), K(consistency));
    }
  }
  return ret;
}

int ObMPQuery::is_readonly_stmt(ObMySQLResultSet& result, bool& is_readonly)
{
  int ret = OB_SUCCESS;
  is_readonly = false;
  const stmt::StmtType type =
      stmt::T_NONE == result.get_literal_stmt_type() ? result.get_stmt_type() : result.get_literal_stmt_type();
  switch (type) {
    case stmt::T_SELECT: {
      // disable for select...for update
      ObPhysicalPlan* physical_plan = result.get_physical_plan();
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
      // disable set @@global.variable stmt
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
    case stmt::T_SHOW_ENGINES:
    case stmt::T_SHOW_PRIVILEGES:
    case stmt::T_SHOW_RESTORE_PREVIEW:
    case stmt::T_SHOW_GRANTS:
    case stmt::T_SHOW_QUERY_RESPONSE_TIME:
    case stmt::T_SHOW_RECYCLEBIN:
    case stmt::T_USE_DATABASE:
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

  // OB_ASSERT(req_);
  // OB_ASSERT(req_->get_type() == ObRequest::OB_MYSQL);
  if ((OB_ISNULL(req_)) || (req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K(req_));
  } else if (get_is_com_filed_list()) {
    if (OB_FAIL(deserialize_com_field_list())) {
      LOG_WARN("failed to deserialize com field list", K(ret));
    }
  } else {
    const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    sql_.assign_ptr(const_cast<char*>(pkt.get_cdata()), pkt.get_clen() - 1);
  }

  return ret;
}

int ObMPQuery::register_callback_with_async(ObQueryExecCtx& query_ctx)
{
  int ret = OB_SUCCESS;
  ObMySQLResultSet& result = query_ctx.get_result_set();
  ObSQLSessionInfo& session = result.get_session();
  ObRemoteTaskCtx* task_ctx = nullptr;
  ObExecutionID execution_id(GCTX.self_addr_, session.get_current_execution_id());
  if (OB_ISNULL(GCTX.query_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global context is invalid", K(ret), K(GCTX.query_ctx_mgr_));
  } else if (OB_FAIL(GCTX.query_ctx_mgr_->create_task_exec_ctx(execution_id, query_ctx, task_ctx))) {
    LOG_WARN("get query exec ctx failed", K(ret), K(execution_id));
  } else if (OB_FAIL(task_ctx->get_mppacket_sender().init(
                 req_, &session, seq_, conn_valid_, req_has_wokenup_, get_receive_timestamp(), io_thread_mark_))) {
    LOG_WARN("init mppacket sender failed", K(ret));
  } else {
    query_ctx.set_cur_task_ctx(task_ctx);
    task_ctx->get_sql_ctx().multi_stmt_item_ = ctx_.multi_stmt_item_;
    task_ctx->get_sql_ctx().schema_guard_ = ctx_.schema_guard_;
    task_ctx->get_retry_ctrl().set_sys_global_schema_version(retry_ctrl_.get_sys_global_schema_version());
    task_ctx->get_retry_ctrl().set_sys_local_schema_version(retry_ctrl_.get_sys_local_schema_version());
    task_ctx->get_retry_ctrl().set_tenant_global_schema_version(retry_ctrl_.get_tenant_global_schema_version());
    task_ctx->get_retry_ctrl().set_tenant_local_schema_version(retry_ctrl_.get_tenant_local_schema_version());
    task_ctx->get_retry_ctrl().set_in_async_execute(true);
    task_ctx->set_mysql_request(req_);
    if (OB_FAIL(result.execute())) {
      LOG_WARN("execute result failed", K(ret));
    } else {
      task_ctx->set_is_running(true);
    }
  }
  result.get_exec_context().clean_resolve_ctx();
  // Expression evaluation context contain thread local page manager memory entity,
  // need be destroyed here to avoid cross thread problem.
  // Evaluation context is no longer used, since we get data from remote, no expression
  // need be evaluated.
  result.get_exec_context().destroy_eval_ctx();
  if (OB_NOT_NULL(GCTX.query_ctx_mgr_) && OB_NOT_NULL(task_ctx)) {
    query_ctx.set_cur_task_ctx(nullptr);
    GCTX.query_ctx_mgr_->revert_task_exec_ctx(task_ctx);
    task_ctx = nullptr;
  }
  if (OB_NOT_NULL(GCTX.query_ctx_mgr_) && OB_FAIL(ret)) {
    GCTX.query_ctx_mgr_->drop_task_exec_ctx(execution_id);
  }
  return ret;
}

// return false only if send packet fail.
OB_INLINE int ObMPQuery::response_result(ObQueryExecCtx& query_ctx, bool force_sync_resp, bool& async_resp_used)
{
  int ret = OB_SUCCESS;
  ObMySQLResultSet& result = query_ctx.get_result_set();
  ObSQLSessionInfo& session = result.get_session();
  CHECK_COMPATIBILITY_MODE(&session);

  bool need_trans_cb = result.need_end_trans_callback() && (!force_sync_resp);
  bool need_execute_async = false;
  NG_TRACE_EXT(exec_begin, OB_ID(arg1), force_sync_resp, OB_ID(end_trans_cb), need_trans_cb);
  // plan is NULL for cmd
  if (OB_LIKELY(NULL != result.get_physical_plan())) {
    if (need_execute_async && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260) {
      ctx_.is_execute_async_ = true;
      WITH_CONTEXT(query_ctx.get_mem_context())
      {
        if (OB_FAIL(register_callback_with_async(query_ctx))) {
          LOG_WARN("response result with async failed", K(ret));
        } else {
          async_resp_used = true;
        }
      }
      if (OB_FAIL(ret)) {
        result.set_errcode(ret);
        // send error to client if remote execute fail
        ObRemotePlanDriver remote_plan_driver(gctx_, ctx_, session, retry_ctrl_, *this);
        int sret = remote_plan_driver.response_result(result);
        if (OB_SUCCESS != sret) {
          LOG_WARN("send remote plan error code to client failed", K(ret), K(sret));
        }
        ctx_.is_execute_async_ = false;
      }
    } else if (need_trans_cb) {
      ObAsyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      // NOTE: sql_end_cb must initialized before drv.response_result()
      ObSqlEndTransCb& sql_end_cb = session.get_mysql_end_trans_cb();
      if (OB_FAIL(sql_end_cb.init(const_cast<rpc::ObRequest*>(req_), &session, seq_, conn_valid_))) {
        LOG_WARN("failed to init sql end callback", K(ret), K(conn_valid_));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      ret = drv.response_result(result);
    }
  } else {
    if (need_trans_cb) {
      ObSqlEndTransCb& sql_end_cb = session.get_mysql_end_trans_cb();
      ObAsyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      if (OB_FAIL(sql_end_cb.init(const_cast<rpc::ObRequest*>(req_), &session, seq_, conn_valid_))) {
        LOG_WARN("failed to init sql end callback", K(ret), K(conn_valid_));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      ret = drv.response_result(result);
    }
  }
  NG_TRACE(exec_end);
  return ret;
}

inline void ObMPQuery::record_stat(const stmt::StmtType type, const int64_t end_time) const
{
#define ADD_STMT_STAT(type)                  \
  case stmt::T_##type:                       \
    EVENT_INC(SQL_##type##_COUNT);           \
    EVENT_ADD(SQL_##type##_TIME, time_cost); \
    break
  const int64_t time_cost = end_time - get_receive_timestamp();
  if (!THIS_THWORKER.need_retry()) {
    switch (type) {
      ADD_STMT_STAT(SELECT);
      ADD_STMT_STAT(INSERT);
      ADD_STMT_STAT(REPLACE);
      ADD_STMT_STAT(UPDATE);
      ADD_STMT_STAT(DELETE);
      default: {
        EVENT_INC(SQL_OTHER_COUNT);
        EVENT_ADD(SQL_OTHER_TIME, time_cost);
      }
    }
  }
#undef ADD_STMT_STAT
}

void ObMPQuery::disconnect()
{
  ObMPBase::disconnect();
}

void ObMPQuery::update_audit_info(const ObWaitEventStat& total_wait_desc, ObAuditRecordData& audit_record)
{
  bool first_record = (0 == audit_record.try_cnt_);
  ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
  audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
  audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
  audit_record.update_stage_stat();
}

int ObMPQuery::deserialize_com_field_list()
{
  int ret = OB_SUCCESS;
  ObIAllocator* alloc = &THIS_WORKER.get_sql_arena_allocator();
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(alloc), K(ret));
  } else {
    const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* str = pkt.get_cdata();
    uint32_t length = pkt.get_clen();
    const char* str1 = "select * from ";
    const char* str2 = " where 0";
    int64_t i = 0;
    const int64_t str1_len = strlen(str1);
    const int64_t str2_len = strlen(str2);
    const int pre_size = str1_len + str2_len;
    // find separator between able_name and filed wildcard. (table_name [NULL] filed wildcard)
    for (; static_cast<int>(str[i]) != 0 && i < length; ++i) {}
    char* dest_str = static_cast<char*>(alloc->alloc(length + pre_size));
    if (OB_ISNULL(dest_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc", K(dest_str));
    } else {
      char* buf = dest_str;
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
      // extract wildcard
      if (i + 1 < length - 1) {
        wild_str_.assign_ptr(str + i + 1, (length - 1) - (i + 1));
      }
    }
  }
  return ret;
}

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

#include "obmp_base.h"

#include "lib/worker.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_change_user.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "observer/mysql/obsm_row.h"
#include "observer/mysql/obsm_utils.h"            // ObSMUtils
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/ob_query_driver.h"
#include "share/config/ob_server_config.h"
#include "storage/tx/ob_trans_define.h"
#include "share/ob_lob_access_utils.h"
#include "sql/monitor/flt/ob_flt_utils.h"
#include "sql/session/ob_sess_info_verify.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
namespace oceanbase
{
using namespace share;
using namespace rpc;
using namespace obmysql;
using namespace common;
using namespace transaction;
using namespace share::schema;
namespace sql
{
  class ObPiece;
}

namespace observer
{

ObMPBase::ObMPBase(const ObGlobalContext &gctx)
    : gctx_(gctx), process_timestamp_(0), proxy_version_(0)
{
}

ObMPBase::~ObMPBase()
{
  // wakeup_request内部会判断has_req_wakeup_标，
  // 这里调一次兜底异常路径忘记flush_buffer的场景
  if (!THIS_WORKER.need_retry()) {
    packet_sender_.finish_sql_request();
  }
}

int ObMPBase::response(const int retcode)
{
  UNUSED(retcode);
  int ret = OB_SUCCESS;
  if (!THIS_WORKER.need_retry()) {
    if (OB_FAIL(flush_buffer(true))) {
      LOG_WARN("failed to flush_buffer", K(ret));
    }
  }
  return ret;
}

int ObMPBase::setup_packet_sender()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(packet_sender_.init(req_))) {
    LOG_ERROR("packet sender init fail", KP(req_), K(ret));
    send_error_packet(ret, NULL);
  }
  return ret;
}

int ObMPBase::before_process()
{
  int ret = OB_SUCCESS;
  process_timestamp_ = common::ObTimeUtility::current_time();
  return ret;
}

int ObMPBase::update_transmission_checksum_flag(const ObSQLSessionInfo &session)
{
  return packet_sender_.update_transmission_checksum_flag(session);
}

int ObMPBase::update_proxy_sys_vars(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  if (OB_UNLIKELY(NULL == conn)) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection in error, maybe has disconnected", K(ret));
  } else if (OB_FAIL(session.set_proxy_user_privilege(session.get_user_priv_set()))) {
    LOG_WARN("fail to set proxy user privilege system variables", K(ret));
  } else if (OB_FAIL(session.set_proxy_capability(conn->proxy_cap_flags_.capability_))) {
    LOG_WARN("fail to set proxy capability", K(ret));
  }
  return ret;
}

int ObMPBase::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  if (!lib::is_diagnose_info_enabled()) {
  } else {
    NG_TRACE_EXT(process_end, OB_ID(run_ts), get_run_timestamp());
    const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
    bool is_slow = (elapsed_time > GCONF.trace_log_slow_query_watermark)
      && !THIS_WORKER.need_retry();
    if (is_slow) {
      if (THIS_WORKER.need_retry() && OB_TRY_LOCK_ROW_CONFLICT == error_code) {
        //如果是锁冲突，且接下来会重试，则不需要打印这条日志了
      } else {
        FORCE_PRINT_TRACE(THE_TRACE, "[slow query]");

        // slow query will flush cache
        FLUSH_TRACE();
      }
    } else if (can_force_print(error_code)) {
      // 需要打印TRACE日志的错误码添加在这里
      int process_ret = error_code;
      NG_TRACE_EXT(process_ret, OB_Y(process_ret));
      FORCE_PRINT_TRACE(THE_TRACE, "[err query]");
    } else if (THIS_WORKER.need_retry()) {
      if (OB_TRY_LOCK_ROW_CONFLICT != error_code) {
        FORCE_PRINT_TRACE(THE_TRACE, "[packet retry query]");
      }
    } else {
      PRINT_TRACE(THE_TRACE);
    }

    if (common::OB_SUCCESS != error_code) {
      FLUSH_TRACE();
    }
  }
  ObFLTUtils::clean_flt_env();
  return ret;
}

void ObMPBase::cleanup()
{
}

void ObMPBase::disconnect()
{
  return packet_sender_.disconnect();
}

void ObMPBase::force_disconnect()
{
  return packet_sender_.force_disconnect();
}

int ObMPBase::clean_buffer()
{
  return packet_sender_.clean_buffer();
}

int ObMPBase::flush_buffer(const bool is_last)
{
  return packet_sender_.is_disable_response()? OB_SUCCESS: packet_sender_.flush_buffer(is_last);
}

ObSMConnection* ObMPBase::get_conn() const
{
  return packet_sender_.get_conn();
}

int ObMPBase::get_conn_id(uint32_t &sessid) const
{
  return packet_sender_.get_conn_id(sessid);
}

int ObMPBase::read_packet(obmysql::ObICSMemPool& mem_pool, obmysql::ObMySQLPacket *&pkt)
{
  return packet_sender_.read_packet(mem_pool, pkt);
}

int ObMPBase::release_packet(obmysql::ObMySQLPacket* pkt)
{
  return packet_sender_.release_packet(pkt);
 }
int ObMPBase::send_error_packet(int err,
                                const char* errmsg,
                                bool is_partition_hit /* = true */,
                                void *extra_err_info /* = NULL */)
{
  return packet_sender_.send_error_packet(err, errmsg, is_partition_hit, extra_err_info);
}

int ObMPBase::send_switch_packet(ObString &auth_name, ObString& auth_data)
{
  int ret = OB_SUCCESS;
  OMPKChangeUser packet;
  packet.set_auth_plugin_name(auth_name);
  packet.set_auth_response(auth_data);
  if (OB_FAIL(response_packet(packet, NULL))) {
    LOG_WARN("failed to send switch packet", K(packet), K(ret));
  }
  return ret;
}

int ObMPBase::load_system_variables(const ObSysVariableSchema &sys_variable_schema, ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema.get_sysvar_count(); ++i) {
    const ObSysVarSchema *sysvar = NULL;
    sysvar = sys_variable_schema.get_sysvar_schema(i);
    if (sysvar != NULL) {
      LOG_DEBUG("load system variable", K(*sysvar));
      if (OB_FAIL(session.load_sys_variable(calc_buf, sysvar->get_name(), sysvar->get_data_type(),
                                            sysvar->get_value(), sysvar->get_min_val(),
                                            sysvar->get_max_val(), sysvar->get_flags(), true))) {
        LOG_WARN("load sys variable failed", K(ret), K(*sysvar));
      }
    }
  }
  if (OB_SUCC(ret)) {
    //设置系统变量的最大版本号
    session.set_global_vars_version(sys_variable_schema.get_schema_version());
    //将影响plan的系统变量序列化并缓存
    if (OB_FAIL(session.gen_sys_var_in_pc_str())) {
      LOG_WARN("fail to gen sys var in pc str", K(ret));
    } else if (OB_FAIL(session.gen_configs_in_pc_str())) {
      LOG_WARN("fail to gen configs in pc string", K(ret));
    } else {
      session.set_enable_mysql_compatible_dates(
        session.get_enable_mysql_compatible_dates_from_config());
    }
  }
  return ret;
}

int ObMPBase::send_ok_packet(ObSQLSessionInfo &session, ObOKPParam &ok_param, obmysql::ObMySQLPacket* pkt)
{
  return packet_sender_.send_ok_packet(session, ok_param, pkt);
}

int ObMPBase::send_eof_packet(const ObSQLSessionInfo &session, const ObMySQLResultSet &result, ObOKPParam *ok_param)
{
  return packet_sender_.send_eof_packet(session, result, ok_param);
}

int ObMPBase::create_session(ObSMConnection *conn, ObSQLSessionInfo *&sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get connection fail", K(ret));
  } else if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session manager is null", K(ret));
  } else {
    if (OB_FAIL(gctx_.session_mgr_->create_session(conn, sess_info))) {
      LOG_WARN("create session fail", "sessid", conn->sessid_,
                "proxy_sessid", conn->proxy_sessid_, K(ret));
    } else {
      LOG_DEBUG("create session successfully", "sessid", conn->sessid_,
               "proxy_sessid", conn->proxy_sessid_);
      conn->is_sess_alloc_ = true;
      sess_info->set_user_session();
      sess_info->set_shadow(false);
      if (SQL_REQ_OP.get_sql_ssl_st(req_) != NULL) {
        sess_info->set_ssl_cipher(SSL_get_cipher_name((SSL*)SQL_REQ_OP.get_sql_ssl_st(req_)));
      } else {
        sess_info->set_ssl_cipher("");
      }
      sess_info->set_client_sessid(conn->client_sessid_);
      sess_info->gen_gtt_session_scope_unique_id();
      sess_info->gen_gtt_trans_scope_unique_id();
    }
  }
  return ret;
}

int ObMPBase::free_session()
{
  int ret = OB_SUCCESS;
  ObSMConnection* conn = NULL;
  if (NULL == (conn = packet_sender_.get_conn())) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session manager is null", K(ret));
  } else {
    bool is_need_clear = false;
    ObFreeSessionCtx ctx;
    ctx.tenant_id_ = conn->tenant_id_;
    ctx.sessid_ = conn->sessid_;
    ctx.proxy_sessid_ = conn->proxy_sessid_;
    ctx.has_inc_active_num_ = conn->has_inc_active_num_;
    if (OB_FAIL(gctx_.session_mgr_->free_session(ctx))) {
      LOG_WARN("fail to free session", K(ctx), K(ret));
    } else {
      LOG_INFO("free session successfully", K(ctx));
      conn->is_sess_free_ = true;
      if (OB_UNLIKELY(OB_FAIL(sql::ObSQLSessionMgr::is_need_clear_sessid(conn, is_need_clear)))) {
        LOG_ERROR("fail to judge need clear", K(ret), "sessid", conn->sessid_);
      } else if (is_need_clear) {
        if (OB_FAIL(GCTX.session_mgr_->mark_sessid_unused(conn->sessid_))) {
          LOG_WARN("mark session id unused failed", K(ret), "sessid", conn->sessid_);
        } else {
          LOG_INFO("mark session id unused", "sessid", conn->sessid_);
        }
      }
    }
  }
  return ret;
}

int ObMPBase::get_session(ObSQLSessionInfo *&sess_info)
{
  return packet_sender_.get_session(sess_info);
}

int ObMPBase::revert_session(ObSQLSessionInfo *sess_info)
{
  return packet_sender_.revert_session(sess_info);
}

int ObMPBase::init_process_var(sql::ObSqlCtx &ctx,
                               const ObMultiStmtItem &multi_stmt_item,
                               sql::ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (!packet_sender_.is_conn_valid()) {
    ret = OB_CONNECT_ERROR;
    LOG_WARN("connection already disconnected", K(ret));
  } else {
    const int64_t debug_sync_timeout = GCONF.debug_sync_timeout;
    // ignore session debug sync action actions to thread local actions error
    if (debug_sync_timeout > 0) {
      int tmp_ret = GDS.set_thread_local_actions(session.get_debug_sync_actions());
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("set session debug sync actions to thread local actions failed", K(tmp_ret));
      }
    }
    // construct sql context
    ctx.multi_stmt_item_ = multi_stmt_item;
    ctx.session_info_ = &session;
    session.set_rpc_tenant_id(THIS_WORKER.get_rpc_tenant());
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());

    if (0 == multi_stmt_item.get_seq_num()) {
      // 第一条sql
      ctx.can_reroute_sql_ = (pkt.can_reroute_pkt() && get_conn()->is_support_proxy_reroute());
    }
    ctx.is_protocol_weak_read_ = pkt.is_weak_read();
    ctx.set_enable_strict_defensive_check(GCONF.enable_strict_defensive_check());
    ctx.set_enable_user_defined_rewrite(session.enable_udr());
    LOG_DEBUG("protocol flag info", K(ctx.can_reroute_sql_), K(ctx.is_protocol_weak_read_),
        K(ctx.get_enable_strict_defensive_check()), "enable_udr", session.enable_udr());
  }
  return ret;
}

//外层调用会忽略do_after_process的错误码，因此这里将set_session_state的错误码返回也没有意义。
//因此这里忽略set_session_state错误码，warning buffer的reset和trace log 记录的流程不收影响。
int ObMPBase::do_after_process(sql::ObSQLSessionInfo &session,
                               sql::ObSqlCtx &ctx,
                               bool async_resp_used) const
{
  int ret = OB_SUCCESS;
  if (session.get_is_in_retry()) {
    // do nothing.
  } else {
    session.set_is_request_end(true);
    session.set_retry_active_time(0);
  }
  // reset warning buffers
  // 注意，此处req_has_wokenup_可能为true，不能再访问req对象
  // @todo 重构wb逻辑
  if (!async_resp_used) { // 异步回包不重置warning buffer，重置操作在callback中做
    session.reset_warnings_buf();
    if (!session.get_is_in_retry()) {
      session.set_session_sleep();
      session.reset_cur_sql_id();
      session.reset_current_plan_id();
      session.reset_current_plan_hash();
    }
  }
  // clear tsi warning buffer
  ob_setup_tsi_warning_buffer(NULL);
  session.reset_plsql_exec_time();
  session.reset_plsql_compile_time();
  return ret;
}

int ObMPBase::record_flt_trace(sql::ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  //trace end
  if (lib::is_diagnose_info_enabled()) {
    NG_TRACE(query_end);

    if (session.is_use_trace_log()) {
      //不影响正常逻辑
      // show trace will always show last request info
      if (OB_FAIL(ObFLTUtils::clean_flt_show_trace_env(session))) {
        LOG_WARN("failed to clean flt show trace env", K(ret));
      }
    } else {
      // not need to record
      ObString trace_id;
      trace_id.reset();
      if (OB_FAIL(session.set_last_flt_trace_id(trace_id))) {
        LOG_WARN("failed to reset last flt trace id", K(ret));
      }
    }
  }
  return ret;
}

void ObMPBase::set_request_expect_group_id(sql::ObSQLSessionInfo *session)
{
  if (OB_INVALID_ID != session->get_expect_group_id()) {
    // Session->expected_group_id_ is set when hit plan cache or resolve a query, and find that
    // expcted group is consistent with current group.
    // Set group_id of req_ so that the req_ will be put in the corresponding queue when do packet retry.
    if (NULL != req_) {
      req_->set_group_id(session->get_expect_group_id());
    }
    // also set conn.group_id_. It means use current consumer group when execute next query for first time.
    // conn.group_id_ = session->get_expect_group_id();
    // reset to invalid because session.expected_group_id is single_use.
    session->set_expect_group_id(OB_INVALID_ID);
  }
}

int ObMPBase::setup_user_resource_group(
    ObSMConnection &conn,
    const uint64_t tenant_id,
    sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  uint64_t group_id = 0;
  uint64_t user_id = session->get_user_id();
  if (OB_INVALID_ID != session->get_expect_group_id()) {
    set_request_expect_group_id(session);
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (conn.group_id_ == OBCG_DIAG_TENANT) {
    // OBCG_DIAG_TENANT was set in check_update_tenant_id, DO NOT overlap it.
  } else if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_user(
              tenant_id, user_id, group_id))) {
    LOG_WARN("fail get group id by user", K(user_id), K(tenant_id), K(ret));
  } else {
    // 将 group id 设置到调度层，之后这个 session 上的所有请求都是用这个 cgroup 的资源
    conn.group_id_ = group_id;
  }
  LOG_TRACE("setup user resource group", K(user_id), K(tenant_id), K(ret));
  return ret;
}

// force refresh schema if local schema version < last schema version
int ObMPBase::check_and_refresh_schema(uint64_t login_tenant_id,
                                       uint64_t effective_tenant_id,
                                       ObSQLSessionInfo *session_info)
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
    bool need_revert_session = false;
    if (NULL == session_info) {
      if (OB_FAIL(get_session(session_info))) {
        LOG_WARN("get session failed");
      } else if (OB_ISNULL(session_info)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid session info", K(ret), K(session_info));
      } else {
        need_revert_session = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(effective_tenant_id, local_version))) {
        LOG_WARN("fail to get tenant refreshed schema version", K(ret));
      } else if (OB_FAIL(session_info->get_ob_last_schema_version(last_version))) {
        LOG_WARN("failed to get_sys_variable", K(OB_SV_LAST_SCHEMA_VERSION));
      } else if (local_version >= last_version) {
        // skip
      } else if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(effective_tenant_id, last_version))) {
        LOG_WARN("failed to refresh schema", K(ret), K(effective_tenant_id), K(last_version));
      }
      if (need_revert_session && OB_LIKELY(NULL != session_info)) {
        revert_session(session_info);
      }
    }
  }
  return ret;
}

int ObMPBase::response_row(ObSQLSessionInfo &session,
                           common::ObNewRow &row,
                           const ColumnsFieldIArray *fields,
                           bool is_packed,
                           ObExecContext *exec_ctx,
                           bool is_ps_protocol,
                           ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObNewRow tmp_row;
  bool has_charset_convert = false;
  if (OB_ISNULL(fields) || row.get_count() != fields->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fields is null", K(ret), KP(fields));
  } else if (OB_FAIL(ob_write_row(allocator, row, tmp_row))) {
    LOG_WARN("deep copy row fail.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_row.get_count(); ++i) {
      ObObj &value = tmp_row.get_cell(i);
      ObCharsetType charset_type = CHARSET_INVALID;
      ObCharsetType ncharset_type = CHARSET_INVALID;
      // need at ps mode
      if (!is_packed && value.get_type() != fields->at(i).type_.get_type()
          && !(value.is_geometry() && lib::is_oracle_mode())) {// oracle gis will do cast in process_sql_udt_results
        ObCastCtx cast_ctx(&allocator, NULL, CM_WARN_ON_FAIL, fields->at(i).type_.get_collation_type());
        if (ObDecimalIntType == fields->at(i).type_.get_type()) {
          cast_ctx.res_accuracy_ = const_cast<ObAccuracy*>(&fields->at(i).accuracy_);
        }
        if (OB_FAIL(common::ObObjCaster::to_type(fields->at(i).type_.get_type(),
                                          cast_ctx,
                                          value,
                                          value))) {
          LOG_WARN("failed to cast object", K(ret), K(value), K(i),
                    K(value.get_type()), K(fields->at(i).type_.get_type()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_packed) {
        // do nothing
      } else if (OB_FAIL(session.get_character_set_results(charset_type))) {
        LOG_WARN("fail to get result charset", K(ret));
      } else if (OB_FAIL(session.get_ncharacter_set_connection(ncharset_type))) {
        LOG_WARN("fail to get result charset", K(ret));
      } else {
        if (lib::is_oracle_mode()
            && (value.is_nchar() || value.is_nvarchar2())
            && ncharset_type != CHARSET_INVALID
            && ncharset_type != CHARSET_BINARY) {
          charset_type = ncharset_type;
        }
        if (ob_is_string_tc(value.get_type())
            && CS_TYPE_INVALID != value.get_collation_type()
            && OB_FAIL(value.convert_string_value_charset(charset_type, allocator))) {
          LOG_WARN("convert string value charset failed", K(ret), K(value));
        } else if (value.is_clob_locator()
                    && OB_FAIL(ObQueryDriver::convert_lob_value_charset(value, charset_type, allocator))) {
          LOG_WARN("convert lob value charset failed", K(ret));
        } else if (ob_is_text_tc(value.get_type())
                    && OB_FAIL(ObQueryDriver::convert_text_value_charset(value, charset_type, allocator, &session, exec_ctx))) {
          LOG_WARN("convert text value charset failed", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if(OB_FAIL(ObQueryDriver::process_lob_locator_results(value,
                                    session.is_client_use_lob_locator(),
                                    session.is_client_support_lob_locatorv2(),
                                    &allocator,
                                    &session,
                                    exec_ctx))) {
          LOG_WARN("convert lob locator to longtext failed", K(ret));
        } else if ((value.is_user_defined_sql_type() || value.is_collection_sql_type() || value.is_geometry())
                   && OB_FAIL(ObXMLExprHelper::process_sql_udt_results(value,
                                    &allocator,
                                    &session,
                                    exec_ctx,
                                    is_ps_protocol,
                                    fields,
                                    schema_guard))) {
          LOG_WARN("convert udt to client format failed", K(ret), K(value.get_udt_subschema_id()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session);
      ObSMRow sm_row(obmysql::BINARY, tmp_row, dtc_params, fields, schema_guard, session.get_effective_tenant_id());
      sm_row.set_packed(is_packed);
      obmysql::OMPKRow rp(sm_row);
      rp.set_is_packed(is_packed);
      if (OB_FAIL(response_packet(rp, &session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("response packet fail", K(ret));
      }
    }
  }
  return ret;
}

int ObMPBase::process_extra_info(sql::ObSQLSessionInfo &session,
              const obmysql::ObMySQLRawPacket &pkt, bool &need_response_error)
{
  int ret = OB_SUCCESS;
  SessionInfoVerifacation sess_info_verification;
  LOG_DEBUG("process extra info", K(ret),K(pkt.get_extra_info().exist_sess_info_veri()));
  if (FALSE_IT(session.set_has_query_executed(true))) {
  } else if (pkt.get_extra_info().exist_sync_sess_info()
              && OB_FAIL(ObMPUtils::sync_session_info(session,
                          pkt.get_extra_info().get_sync_sess_info()))) {
    // won't response error, disconnect will let proxy sens failure
    need_response_error = false;
    LOG_WARN("fail to update sess info", K(ret));
  } else if (pkt.get_extra_info().exist_sess_info_veri()
              && OB_FAIL(ObSessInfoVerify::sync_sess_info_veri(session,
                        pkt.get_extra_info().get_sess_info_veri(),
                        sess_info_verification))) {
    LOG_WARN("fail to get verify info requied", K(ret));
  } else if (pkt.get_extra_info().exist_sess_info_veri() &&
              pkt.is_proxy_switch_route() &&
              OB_FAIL(ObSessInfoVerify::verify_session_info(session,
              sess_info_verification))) {
    LOG_WARN("fail to verify sess info", K(ret));
  }
  return ret;
}

// The obmp layer handles the kill client session logic.
int ObMPBase::process_kill_client_session(sql::ObSQLSessionInfo &session, bool is_connect)
{
  int ret = OB_SUCCESS;
  uint64_t create_time = 0;
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid session mgr", K(ret), K(gctx_));
  } else if (OB_UNLIKELY(session.is_mark_killed())) {
    ret = OB_ERR_KILL_CLIENT_SESSION;
    LOG_WARN("client session need be killed", K(session.get_session_state()),
            K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(),
            K(session.get_client_sessid()), K(ret));
  } else if (is_connect) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != (gctx_.session_mgr_->get_kill_client_sess_map().
              get_refactored(session.get_client_sessid(), create_time)))) {
      if (session.get_client_create_time() == create_time) {
        ret = OB_ERR_KILL_CLIENT_SESSION;
        LOG_WARN("client session need be killed", K(session.get_session_state()),
                K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(),
                K(session.get_client_sessid()), K(ret),K(create_time));
      } else {
        LOG_DEBUG("client session is created later", K(create_time),
                K(session.get_client_create_time()),
                K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(),
                K(session.get_client_sessid()));
      }
    }
  } else {
  }
  return ret;
}

int ObMPBase::update_charset_sys_vars(ObSMConnection &conn, ObSQLSessionInfo &sess_info)
{
  int ret = OB_SUCCESS;
  int64_t cs_type = conn.client_cs_type_;
  if (ObCharset::is_valid_collation(cs_type)) {
    if (OB_FAIL(sess_info.update_sys_variable(SYS_VAR_CHARACTER_SET_CLIENT, cs_type))) {
      SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
    } else if (OB_FAIL(sess_info.update_sys_variable(SYS_VAR_CHARACTER_SET_RESULTS, cs_type))) {
      SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
    } else if (OB_FAIL(sess_info.update_sys_variable(SYS_VAR_CHARACTER_SET_CONNECTION, cs_type))) {
      SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
    } else if (OB_FAIL(sess_info.update_sys_variable(SYS_VAR_COLLATION_CONNECTION, cs_type))) {
      SQL_ENG_LOG(WARN, "failed to update sys var", K(ret));
    }
  }
  return ret;
}

int ObMPBase::load_privilege_info_for_change_user(sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  ObSMConnection *conn = NULL;
  if (OB_ISNULL(session) || OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN,"invalid argument", K(session), K(gctx_.schema_service_));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null conn", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
                                  session->get_effective_tenant_id(), schema_guard))) {
    OB_LOG(WARN,"fail get schema guard", K(ret));
  } else {
    SSL *ssl_st = SQL_REQ_OP.get_sql_ssl_st(req_);
    share::schema::ObUserLoginInfo login_info = session->get_login_info();
    share::schema::ObSessionPrivInfo session_priv;
    // disconnect previous user connection first.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(session->on_user_disconnect())) {
      LOG_WARN("user disconnect failed", K(ret));
    }
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_guard.check_user_access(login_info, session_priv,
                ssl_st, user_info))) {
      OB_LOG(WARN, "User access denied", K(login_info), K(ret));
    } else if (OB_FAIL(session->on_user_connect(session_priv, user_info))) {
      OB_LOG(WARN, "user connect failed", K(ret), K(session_priv));
    } else {
      uint64_t db_id = OB_INVALID_ID;
      const ObSysVariableSchema *sys_variable_schema = NULL;
      session->set_user(session_priv.user_name_, session_priv.host_name_, session_priv.user_id_);
      session->set_user_priv_set(session_priv.user_priv_set_);
      session->set_db_priv_set(session_priv.db_priv_set_);
      session->set_enable_role_array(session_priv.enable_role_id_array_);
      if (OB_FAIL(session->set_tenant(login_info.tenant_name_, session_priv.tenant_id_))) {
        OB_LOG(WARN, "fail to set tenant", "tenant name", login_info.tenant_name_, K(ret));
      } else if (OB_FAIL(session->set_real_client_ip_and_port(login_info.client_ip_, session->get_client_addr_port()))) {
          LOG_WARN("failed to set_real_client_ip", K(ret));
      } else if (OB_FAIL(schema_guard.get_sys_variable_schema(session_priv.tenant_id_, sys_variable_schema))) {
        LOG_WARN("get sys variable schema failed", K(ret));
      } else if (OB_ISNULL(sys_variable_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys variable schema is null", K(ret));
      } else if (OB_FAIL(session->load_all_sys_vars(*sys_variable_schema, true))) {
        LOG_WARN("load system variables failed", K(ret));
      } else if (OB_FAIL(session->update_database_variables(&schema_guard))) {
        OB_LOG(WARN, "failed to update database variables", K(ret));
      } else if (!session->get_database_name().empty() &&
                  OB_FAIL(schema_guard.get_database_id(session->get_effective_tenant_id(),
                                                      session->get_database_name(),
                                                      db_id))) {
        OB_LOG(WARN, "failed to get database id", K(ret));
      } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
        LOG_WARN("update transmisson checksum flag failed", K(ret));
      } else if (OB_FAIL(update_proxy_sys_vars(*session))) {
        LOG_WARN("update_proxy_sys_vars failed", K(ret));
      } else if (OB_FAIL(update_charset_sys_vars(*conn, *session))) {
        LOG_WARN("fail to update charset sys vars", K(ret));
      } else {
        session->set_database_id(db_id);
        session->reset_user_var();
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

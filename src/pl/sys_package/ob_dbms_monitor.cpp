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

#define USING_LOG_PREFIX PL
#include "pl/sys_package/ob_dbms_application.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
#include "pl/sys_package/ob_dbms_monitor.h"
#include "lib/number/ob_number_v2.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace pl
{
int ObDBMSMonitor::resolve_control_info(FLTControlInfo &coninfo, ObNumber level, ObNumber sample_pct, ObString rp_val)
{
  int ret = OB_SUCCESS;
  int64_t level_int = 0;
  // resolve level, smaple_pct_
  if (OB_FAIL(level.cast_to_int64(level_int))) {
    LOG_WARN("failed to cast int", K(ret));
  } else {
    coninfo.level_ = (int8_t)(level_int);
    coninfo.sample_pct_ = atof(sample_pct.format());
  }

  // resolve record_policy
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (rp_val.case_compare(RP_ALL) == 0) {
    coninfo.rp_ = FLTControlInfo::RecordPolicy::RP_ALL;
  } else if (rp_val.case_compare(RP_ONLY_SLOW_QUERY) == 0) {
    coninfo.rp_ = FLTControlInfo::RecordPolicy::RP_ONLY_SLOW_QUERY;
  } else if (rp_val.case_compare(RP_SAMPLE_AND_SLOW_QUERY) == 0) {
    coninfo.rp_ = FLTControlInfo::RecordPolicy::RP_SAMPLE_AND_SLOW_QUERY;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid record policy", K(ret));
  }
  if (!coninfo.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "control info");
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::session_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObNumber sess_id;
  uint32_t sid = 0;
  ObNumber level;
  ObNumber sample_pct;
  ObString record_policy;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(4 == params.count()));
  OV (params.at(0).is_number() || params.at(0).is_null(), OB_INVALID_ARGUMENT);
  if (params.at(0).is_number()) {
    OZ (params.at(0).get_number(sess_id));
    if (sess_id.is_zero()) {
      sid = ctx.get_my_session()->get_sessid();
    } else {
      sid = atoi(sess_id.format());
    }
  } else {
    sid = ctx.get_my_session()->get_sessid();
  }
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(1).get_number(level));
  OV (params.at(2).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(2).get_number(sample_pct));
  OV (params.at(3).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(3).get_string(record_policy));
  // add record polciy
  sql::ObSQLSessionInfo *sess = NULL;
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  FLTControlInfo c_info;
  ObString sess_id_str(sess_id.format());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(session_mgr)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "sessionMgr is NULL", K(ret));
  } else {
    sql::ObSessionGetterGuard guard(*session_mgr, sid);
    if (OB_FAIL(guard.get_session(sess))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "User session ID does not exist.");
      } else {
        LOG_WARN("failed to get session", K(sid), K(sess));
      }
    } else if (OB_FAIL(resolve_control_info(c_info, level, sample_pct, record_policy))) {
      LOG_WARN("failed to resolve control info", K(ret), K(sample_pct), K(record_policy));
    } else {
      sess->set_send_control_info(false);
      sess->set_coninfo_set_by_sess(true);
      sess->set_flt_control_info(c_info);
    }
  }

  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::session_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObNumber sess_id;
  uint32_t sid = 0;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(1 == params.count()));
  OV (params.at(0).is_number() || params.at(0).is_null(), OB_INVALID_ARGUMENT);
  if (params.at(0).is_number()) {
    OZ (params.at(0).get_number(sess_id));
    if (sess_id.is_zero()) {
      sid = ctx.get_my_session()->get_sessid();
    } else {
      sid = atoi(sess_id.format());
    }
  } else {
    sid = ctx.get_my_session()->get_sessid();
  }
  // check policy, if policy not exist, report err.
  // mark session
  sql::ObSQLSessionInfo *sess = NULL;
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  ObString sess_id_str(sess_id.format());
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  FLTControlInfo con;

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (OB_ISNULL(session_mgr)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "sessionMgr is NULL", K(ret));
  } else {
    sql::ObSessionGetterGuard guard(*session_mgr, sid);
    if (OB_FAIL(guard.get_session(sess))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "User session ID does not exist.");
      } else {
        LOG_WARN("failed to get session", K(sid), K(sess));
      }
    } else if (!sess->is_coninfo_set_by_sess()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for sess is not enabled");
    } else if (FALSE_IT(sess->set_coninfo_set_by_sess(false))) {
      // do nothing
    } else if (OB_FAIL(mgr.find_appropriate_con_info(*sess))) {
      LOG_WARN("failed to find control info", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::client_id_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString client_id;
  ObNumber level;
  ObNumber sample_pct;
  ObString record_policy;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(4 == params.count()));
  OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_string(client_id));
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(1).get_number(level));
  OV (params.at(2).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(2).get_number(sample_pct));
  OV (params.at(3).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(3).get_string(record_policy));
  ObIdentifierConInfo i_co;
  i_co.identifier_name_ = client_id;
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  int64_t idx = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (OB_FAIL(mgr.find_identifier_con_info(client_id, idx))) {
    LOG_WARN("failed to find identifier control info", K(ret));
  } else if (idx > -1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for client identifier is already enabled");
  } else if (OB_FAIL(resolve_control_info(i_co.control_info_, level, sample_pct, record_policy))) {
    LOG_WARN("failed to resolve control info", K(ret), K(sample_pct), K(record_policy));
  } else if (OB_FAIL(mgr.add_identifier_con_info(ctx, i_co))) {
    LOG_WARN("failed to add identifier con", K(ret));
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::client_id_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString client_id;
  UNUSED(result);
  CK (OB_LIKELY(1 == params.count()));
  OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_string(client_id));
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  int64_t idx = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (OB_FAIL(mgr.find_identifier_con_info(client_id, idx))) {
    LOG_WARN("failed to find identifier control info", K(ret));
  } else if (idx == -1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for client identifier is not enabled");
  } else if (OB_FAIL(mgr.remove_identifier_con_info(ctx, client_id))) {
    LOG_WARN("failed to remove tenant con info", K(ret));
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::mod_act_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString mod_name;
  ObString act_name;
  ObNumber level;
  ObNumber sample_pct;
  ObString record_policy;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(5 == params.count()));
  OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_string(mod_name));
  OV (params.at(1).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(1).get_string(act_name));
  OV (params.at(2).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(2).get_number(level));
  OV (params.at(3).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(3).get_number(sample_pct));
  OV (params.at(4).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(4).get_string(record_policy));
  ObModActConInfo m_co;
  m_co.mod_name_ = mod_name;
  m_co.act_name_ = act_name;
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  int64_t idx = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (OB_FAIL(mgr.find_mod_act_con_info(mod_name, act_name, idx))) {
    LOG_WARN("failed to find identifier control info", K(ret));
  } else if (idx > -1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for module/action is already enabled");
  } else if (OB_FAIL(resolve_control_info(m_co.control_info_, level, sample_pct, record_policy))) {
    LOG_WARN("failed to resolve control info", K(ret), K(sample_pct), K(record_policy));
  } else if (OB_FAIL(mgr.add_mod_act_con_info(ctx, m_co))) {
    LOG_WARN("failed to add identifier con", K(ret));
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::mod_act_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString mod_name;
  ObString act_name;
  UNUSED(result);
  CK (OB_LIKELY(2 == params.count()));
  OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_string(mod_name));
  OV (params.at(1).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(1).get_string(act_name));
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  int64_t idx = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (OB_FAIL(mgr.find_mod_act_con_info(mod_name, act_name, idx))) {
    LOG_WARN("failed to find identifier control info", K(ret));
  } else if (idx == -1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for module/action is not enabled");
  } else if (OB_FAIL(mgr.remove_mod_act_con_info(ctx, mod_name, act_name))) {
    LOG_WARN("failed to remove tenant con info", K(ret));
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::tenant_trace_enable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObNumber level;
  ObNumber sample_pct;
  ObString record_policy;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(3 == params.count()));
  OV (params.at(0).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(0).get_number(level));
  OV (params.at(1).is_number(), OB_INVALID_ARGUMENT);
  OZ (params.at(1).get_number(sample_pct));
  OV (params.at(2).is_varchar(), OB_INVALID_ARGUMENT);
  OZ (params.at(2).get_string(record_policy));
  FLTControlInfo con;
  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (mgr.is_valid_tenant_config()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for tenant is already enabled");
  } else if (OB_FAIL(resolve_control_info(con, level, sample_pct, record_policy))) {
    LOG_WARN("failed to resolve control info", K(ret), K(sample_pct), K(record_policy));
  } else if (OB_FAIL(mgr.add_tenant_con_info(ctx, con))) {
    LOG_WARN("failed to add identifier con", K(ret));
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSMonitor::tenant_trace_disable(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString tenant_name;
  UNUSED(result);
  if (1 == params.count()) {
    if (params.at(0).is_varchar()) {
      OZ (params.at(0).get_string(tenant_name));
      if (tenant_name.case_compare(GET_MY_SESSION(ctx)->get_tenant_name()) == 0) {
        // do nothing
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name");
      }
    } else if (params.at(0).is_null()) {
      // do nothing
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name");
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name");
  }

  ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init full link trace info manager", K(ret));
  } else if (!mgr.is_valid_tenant_config()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tracing for tenant is not enabled");
  } else if (OB_FAIL(mgr.remove_tenant_con_info(ctx))) {
    LOG_WARN("failed to remove tenant con info", K(ret));
  }
  return ret;
}
} // end of pl
} // end oceanbase


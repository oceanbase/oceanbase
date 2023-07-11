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
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
namespace oceanbase
{

using namespace sql;
namespace pl
{
// this is a procedure, and not need to return result
int ObDBMSAppInfo::read_client_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString client_info;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(1 == params.count()));
  OV (params.at(0).get_param_meta().is_varchar(), OB_INVALID_ARGUMENT);
  client_info = ctx.get_my_session()->get_client_info();
  params.at(0).set_varchar(client_info);
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSAppInfo::read_module(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString module_name;
  ObString action_name;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_LIKELY(2 == params.count()));
  OV (params.at(0).get_param_meta().is_varchar(), OB_INVALID_ARGUMENT);
  OV (params.at(1).get_param_meta().is_varchar(), OB_INVALID_ARGUMENT);
  module_name = ctx.get_my_session()->get_module_name();
  action_name = ctx.get_my_session()->get_action_name();
  params.at(0).set_varchar(module_name);
  params.at(1).set_varchar(action_name);
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSAppInfo::set_action(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString action_name;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  ObSQLSessionInfo* sess = const_cast<ObSQLSessionInfo*>(ctx.get_my_session());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sess->is_obproxy_mode() && !sess->is_ob20_protocol()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "oceanbase 2.0 protocol is not ready, and dbms_application_info not support");
  } else {
    CK (OB_LIKELY(1 == params.count()));
    OV (params.at(0).is_varchar(), OB_INVALID_ARGUMENT);
    OZ (params.at(0).get_string(action_name));
    OZ (sess->get_app_info_encoder().set_action_name(sess, action_name));

    FLTControlInfo con;
    ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(mgr.init())) {
      LOG_WARN("failed to init full link trace info manager", K(ret));
    } else if (OB_FAIL(mgr.find_appropriate_con_info(*sess))) {
      LOG_WARN("failed to get control info for client info", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSAppInfo::set_client_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString client_info;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  ObSQLSessionInfo* sess = const_cast<ObSQLSessionInfo*>(ctx.get_my_session());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sess->is_obproxy_mode() && !sess->is_ob20_protocol()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "oceanbase 2.0 protocol is not ready, and dbms_application_info not support");
  } else {
    CK (OB_LIKELY(1 == params.count()));
    OV (params.at(0).is_varchar() || params.at(0).is_null_oracle(), OB_INVALID_ARGUMENT);
    if (params.at(0).is_null_oracle()) {
      client_info.reset();
    } else {
      OZ (params.at(0).get_string(client_info));
    }
    OZ (sess->get_app_info_encoder().set_client_info(sess, client_info));

    FLTControlInfo con;
    ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(mgr.init())) {
      LOG_WARN("failed to init full link trace info manager", K(ret));
    } else if (OB_FAIL(mgr.find_appropriate_con_info(*sess))) {
      LOG_WARN("failed to get control info for client info", K(ret), K(client_info));
    } else {
      // do nothing
    }
  }
  return ret;
}
// this is a procedure, and not need to return result
int ObDBMSAppInfo::set_module(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString module_name;
  ObString action_name;
  UNUSED(result);
  CK (OB_NOT_NULL(ctx.get_my_session()));
  ObSQLSessionInfo* sess = const_cast<ObSQLSessionInfo*>(ctx.get_my_session());
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sess->is_obproxy_mode() && !sess->is_ob20_protocol()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "oceanbase 2.0 protocol is not ready, and dbms_application_info not support");
  } else {
    CK (OB_LIKELY(2 == params.count()));
    OV (params.at(0).is_varchar() || params.at(0).is_null_oracle(), OB_INVALID_ARGUMENT);
    if (params.at(0).is_null_oracle()) {
      module_name.reset();
    } else {
      OZ (params.at(0).get_string(module_name));
    }
    OV (params.at(1).is_varchar() || params.at(1).is_null_oracle(), OB_INVALID_ARGUMENT);
    if (params.at(1).is_null_oracle()) {
      action_name.reset();
    } else {   
      OZ (params.at(1).get_string(action_name));
    }
    OZ (sess->get_app_info_encoder().set_module_name(sess, module_name));
    OZ (sess->get_app_info_encoder().set_action_name(sess, action_name));

    FLTControlInfo con;
    ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(mgr.init())) {
      LOG_WARN("failed to init full link trace info manager", K(ret));
    } else if (OB_FAIL(mgr.find_appropriate_con_info(*sess))) {
      LOG_WARN("failed to get control info for client info", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}
} // end of pl
} // end oceanbase


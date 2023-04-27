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
#include "pl/sys_package/ob_dbms_session.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/ob_pl.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_context_operator.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"

namespace oceanbase
{
namespace pl
{

int ObDBMSSession::clear_all_context(sql::ObExecContext &ctx,
                                     sql::ParamStore &params,
                                     common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObString context_name;
  ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  const ObContextSchema *ctx_schema = nullptr;
  ObPLContext *pl_ctx = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (OB_ISNULL(pl_ctx = session->get_pl_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get pl context", K(ret));
  } else if (OB_UNLIKELY(1 != params.count())) {
    ObString func_name("CLEAR_ALL_CONTEXT");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (OB_FAIL(check_argument(params.at(0), false, true, 0,
                                    OB_MAX_CONTEXT_STRING_LENGTH, context_name))) {
    LOG_WARN("failed to check param 0", K(ret));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_context_schema_with_name(session->get_effective_tenant_id(),
                                                        context_name, ctx_schema))) {
    LOG_WARN("failed to get context schema", K(ret));
  } else if (OB_ISNULL(ctx_schema)) {
    ret = OB_ERR_NO_SYS_PRIVILEGE;
    LOG_WARN("schema not exists, can not update context value", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_SYS_PRIVILEGE);
  } else if (OB_FAIL(check_privileges(pl_ctx, ctx_schema->get_trusted_package(),
                                      ctx_schema->get_schema_name()))) {
    LOG_WARN("failed to check privileges", K(ret));
  } else {
    if (ACCESSED_GLOBALLY == ctx_schema->get_context_type()) {
      share::ObGlobalContextOperator ctx_operator;
      if (OB_FAIL(ctx_operator.delete_global_contexts_by_id(ctx_schema->get_tenant_id(),
                                                            ctx_schema->get_context_id(),
                                                            *ctx.get_sql_proxy()))) {
        LOG_WARN("execute delete failed", K(ctx_schema->get_tenant_id()), K(ret));
      }
    } 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session->clear_all_context(context_name))) {
        LOG_WARN("failed to set context value", K(ret));
      }
    }
  }
  return ret;
}

int ObDBMSSession::clear_context(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObString context_name;
  ObString client_id;
  ObString attribute;
  ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  ObPLContext *pl_ctx = nullptr;
  const ObContextSchema *ctx_schema = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (OB_ISNULL(pl_ctx = session->get_pl_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get pl context", K(ret));
  } else if (OB_UNLIKELY(3 != params.count())) {
    ObString func_name("CLEAR_CONTEXT");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (OB_FAIL(check_argument(params.at(0), false, true, 0,
                                    OB_MAX_CONTEXT_STRING_LENGTH, context_name))) {
    LOG_WARN("failed to check param 0", K(ret));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_context_schema_with_name(session->get_effective_tenant_id(),
                                                        context_name, ctx_schema))) {
    LOG_WARN("failed to get context schema", K(ret));
  } else if (OB_ISNULL(ctx_schema)) {
    ret = OB_ERR_NO_SYS_PRIVILEGE;
    LOG_WARN("schema not exists, can not update context value", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_SYS_PRIVILEGE);
  } else if (OB_FAIL(check_privileges(pl_ctx, ctx_schema->get_trusted_package(),
                                      ctx_schema->get_schema_name()))) {
    LOG_WARN("failed to check privileges", K(ret));
  } else if (OB_FAIL(check_client_id(params.at(1),
                      OB_MAX_CONTEXT_CLIENT_IDENTIFIER_LENGTH_IN_SESSION, client_id))) {
    LOG_WARN("failed to check param 1", K(ret));
  } else if (OB_FAIL(check_argument(params.at(2), true, true, 2,
                      OB_MAX_CONTEXT_STRING_LENGTH, attribute))) {
    LOG_WARN("failed to check param 2", K(ret));
  } else {
    if (ACCESSED_GLOBALLY == ctx_schema->get_context_type()) {
      if (client_id.empty()) {
        client_id = ObString("");
      }
      share::ObGlobalContextOperator ctx_operator;
      if (OB_FAIL(ctx_operator.delete_global_context(ctx_schema->get_tenant_id(),
                                                     ctx_schema->get_context_id(),
                                                     attribute,
                                                     client_id,
                                                     *ctx.get_sql_proxy()))) {
        LOG_WARN("execute delete failed", K(ctx_schema->get_tenant_id()), K(ret));
      }
    } 
    if (OB_SUCC(ret)) {
      if (attribute.empty()) {
        if (OB_FAIL(session->clear_all_context(context_name))) {
          LOG_WARN("failed to clear context value", K(ret));
        }
      } else if (OB_FAIL(session->clear_context(context_name, attribute))) {
        LOG_WARN("failed to clear context value", K(ret));
      }
    }
  }
  return ret;
}

int ObDBMSSession::clear_identifier(sql::ObExecContext &ctx,
                                    sql::ParamStore &params,
                                    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObString client_id = "";
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (OB_UNLIKELY(0 != params.count())) {
    ObString func_name("CLEAR_IDENTIFIER");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (OB_FAIL(session->set_client_id(client_id))) {
    LOG_WARN("failed to set client id", K(ret));
  }
  return ret;
}


int ObDBMSSession::set_context(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObString context_name;
  ObString attribute;
  ObString value;
  ObString username;
  ObString client_id;
  ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  const ObContextSchema *ctx_schema = nullptr;
  ObPLContext *pl_ctx = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (session->is_obproxy_mode() && !session->is_ob20_protocol()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "oceanbase 2.0 protocol is not ready, and dbms_session not support");
  } else if (OB_ISNULL(pl_ctx = session->get_pl_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get pl context", K(ret));
  } else if (OB_UNLIKELY(5 != params.count())) {
    ObString func_name("SET_CONTEXT");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (OB_FAIL(check_argument(params.at(0), false, true, 0,
                                    OB_MAX_CONTEXT_STRING_LENGTH, context_name))) {
    LOG_WARN("failed to check param 0", K(ret));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_context_schema_with_name(session->get_effective_tenant_id(),
                                                        context_name, ctx_schema))) {
    LOG_WARN("failed to get context schema", K(ret));
  } else if (OB_ISNULL(ctx_schema)) {
    ret = OB_ERR_NO_SYS_PRIVILEGE;
    LOG_WARN("schema not exists, can not update context value", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_SYS_PRIVILEGE);
  } else if (OB_FAIL(check_privileges(pl_ctx, ctx_schema->get_trusted_package(),
                                      ctx_schema->get_schema_name()))) {
    LOG_WARN("failed to check privileges", K(ret));
  } else if (OB_FAIL(check_argument(params.at(1), false, true, 1,
                                    OB_MAX_CONTEXT_STRING_LENGTH, attribute))) {
    LOG_WARN("failed to check param 1", K(ret));
  } else if (OB_FAIL(check_argument(params.at(2), true, false, 2,
                                    OB_MAX_CONTEXT_VALUE_LENGTH, value))) {
    LOG_WARN("failed to check param 2", K(ret));
  } else {
    if (ACCESSED_GLOBALLY == ctx_schema->get_context_type()) {
      if (OB_FAIL(check_argument(params.at(3), true, true, 3,
                                 OB_MAX_CONTEXT_STRING_LENGTH, username))) {
        LOG_WARN("failed to check param 3", K(ret));
      } else if (OB_FAIL(check_client_id(params.at(4),
                         OB_MAX_CONTEXT_CLIENT_IDENTIFIER_LENGTH_IN_SESSION, client_id))) {
        LOG_WARN("failed to check param 4", K(ret));
      } else {
        if (client_id.empty()) {
          client_id = ObString("");
        }
        if (username.empty()) {
          username = ObString("");
        }
        if (value.empty()) {
          value = ObString("");
        }
        share::ObGlobalContextOperator ctx_operator;
        if (OB_FAIL(ctx_operator.insert_update_context(ctx_schema->get_tenant_id(),
                                                       ctx_schema->get_context_id(),
                                                       context_name,
                                                       attribute,
                                                       client_id,
                                                       username,
                                                       value,
                                                       *ctx.get_sql_proxy()))) {
          LOG_WARN("failed to insert update context", K(ret), K(ctx_schema->get_tenant_id()));
        }
      }
    } else {
      if (OB_FAIL(session->set_context_values(context_name, attribute, value))) {
        LOG_WARN("failed to set context value", K(ret));
      }
    }
  }
  return ret;
}

int ObDBMSSession::set_identifier(sql::ObExecContext &ctx,
                                  sql::ParamStore &params,
                                  common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObString client_id;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (OB_UNLIKELY(1 != params.count())) {
    ObString func_name("SET_IDENTIFIER");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (params.at(0).is_null()) {
    client_id = ObString("");
  } else if (!params.at(0).is_varchar()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get wrong param in set identifier", K(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(0).get_varchar(client_id))) {
    LOG_WARN("failed to get param", K(ret), K(params.at(0)));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session->set_client_id(client_id))) {
    LOG_WARN("failed to set client id", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObFLTControlInfoManager mgr(GET_MY_SESSION(ctx)->get_effective_tenant_id());
    if (OB_FAIL(mgr.init())) {
      LOG_WARN("failed to init full link trace info manager", K(ret));
    } else if (OB_FAIL(mgr.find_appropriate_con_info(*session))) {
      LOG_WARN("failed to get control info for client info", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObDBMSSession::reset_package(sql::ObExecContext &ctx,
                                  sql::ParamStore &params,
                                  common::ObObj &result)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = ctx.get_my_session();
  ObPLContext *pl_ctx = nullptr;
  ObString client_id;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is nullptr", K(ret));
  } else if (OB_UNLIKELY(0 != params.count())) {
    ObString func_name("RESET_PACKAGE");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else {
    session->set_need_reset_package(true);
  }
  return ret;
}

int ObDBMSSession::check_argument(const ObObj &input_param, bool allow_null,
                                  bool need_case_up, int32_t param_idx,
                                  int64_t max_len, ObString &output_param)
{
  int ret = OB_SUCCESS;
  if (input_param.is_null()) {
    if (allow_null) {
      output_param.reset();
    } else {
      ret = OB_ERR_INVALID_INPUT_ARGUMENT;
      LOG_USER_ERROR(OB_ERR_INVALID_INPUT_ARGUMENT, param_idx + 1);
    }
  } else if (!input_param.is_varchar()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(input_param), K(param_idx));
  } else if (OB_FAIL(input_param.get_varchar(output_param))) {
    LOG_WARN("failed to get varchar", K(ret), K(input_param), K(param_idx));
  } else if (output_param.length() > max_len) {
    ret = OB_ERR_INVALID_INPUT_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_INPUT_ARGUMENT, param_idx + 1);
  } else if (need_case_up) {
    try_caseup(input_param.get_collation_type(), output_param);
  }
  return ret;
}

int ObDBMSSession::check_client_id(const ObObj &input_param,
                                   int64_t max_len,
                                   ObString &output_param)
{
  int ret = OB_SUCCESS;
  if (input_param.is_null()) {
    output_param.reset();
  } else if (!input_param.is_varchar()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(input_param));
  } else if (OB_FAIL(input_param.get_varchar(output_param))) {
    LOG_WARN("failed to get varchar", K(ret), K(input_param));
  } else if (output_param.length() > max_len) {
    ret = OB_ERR_CLIENT_IDENTIFIER_TOO_LONG;
    LOG_USER_ERROR(OB_ERR_CLIENT_IDENTIFIER_TOO_LONG);
  } else {
    try_caseup(input_param.get_collation_type(), output_param);
  }
  return ret;
}

void ObDBMSSession::try_caseup(ObCollationType cs_type, ObString &str_val)
{
  if (!str_val.empty()) {
    if (str_val.ptr()[0] == '\"' && str_val.ptr()[str_val.length() - 1] == '\"') {
      str_val.assign(str_val.ptr() + 1, str_val.length() - 2);
    } else {
      ObCharset::caseup(cs_type, str_val);
    }
  }
}

int ObDBMSSession::check_privileges(pl::ObPLContext *pl_ctx,
                                    const ObString &package_name,
                                    const ObString &schema_name)
{
  int ret = OB_SUCCESS;
  ObPLExecState *frame = NULL;
  bool trusted = false;
  // ob store sys package in oceanbase schema, to compat with oracle
  // we rewrite SYS to OCEANBASE
  ObString real_schema_name = (0 == schema_name.case_compare("SYS") 
                               && 0 == package_name.case_compare("DBMS_SESSION"))
                               ? "OCEANBASE" : schema_name;
  CK (OB_NOT_NULL(pl_ctx));
  if (OB_SUCC(ret)) {
    uint64_t stack_cnt = pl_ctx->get_exec_stack().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < stack_cnt && !trusted; ++i) {
      frame = pl_ctx->get_exec_stack().at(i);
      if (OB_ISNULL(frame)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get frame", K(i), K(stack_cnt), K(ret));
      } else {
        ObPLFunction &func = frame->get_function();
        if (0 == package_name.case_compare(func.get_package_name())
            || (func.get_package_name().empty()
                && 0 == package_name.case_compare(func.get_function_name()))) {
          if (0 == real_schema_name.case_compare(func.get_database_name())) {
            trusted = true;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !trusted) {
    ret = OB_ERR_NO_SYS_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_SYS_PRIVILEGE);
  }
  return ret;
}

} // end of pl
} // end oceanbase

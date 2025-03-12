/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_audit_log_func.h"
#ifdef OB_BUILD_AUDIT_SECURITY
#include "sql/audit/ob_audit_log_info.h"
#include "sql/audit/ob_audit_log_table_operator.h"
#include "sql/audit/ob_audit_log_utils.h"
#endif
#include "sql/engine/ob_exec_context.h"
#include "sql/privilege_check/ob_privilege_check.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObExprAuditLogFunc::ObExprAuditLogFunc(ObIAllocator &alloc,
                                       ObExprOperatorType type,
                                       const char *name,
                                       int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL)
{
}

int ObExprAuditLogFunc::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_data_version(type_ctx))) {
    LOG_WARN("failed to check data version");
  } else if (OB_FAIL(check_param_type(type1))) {
    LOG_WARN("failed to check param type");
  } else {
    type.set_varchar();
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);

    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}

int ObExprAuditLogFunc::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_data_version(type_ctx))) {
    LOG_WARN("failed to check data version");
  } else if (OB_FAIL(check_param_type(type1)) || OB_FAIL(check_param_type(type2))) {
    LOG_WARN("failed to check param type");
  } else {
    type.set_varchar();
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(common::CS_LEVEL_COERCIBLE);
    type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);

    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(ObCharset::get_system_collation());
    type2.set_calc_type(ObVarcharType);
    type2.set_calc_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}

int ObExprAuditLogFunc::check_data_version(ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  uint64_t data_version = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < MOCK_DATA_VERSION_4_2_4_0 ||
             (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_3_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, get_name());
  }
  return ret;
}

int ObExprAuditLogFunc::check_param_type(const ObExprResType &type) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!type.is_varchar_or_char())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, get_name());
    LOG_WARN("invalid param type", K(ret), K(type));
  }
  return ret;
}

int ObExprAuditLogFunc::check_privilege(ObEvalCtx &ctx,
                                        bool &is_valid,
                                        ObString &error_info)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = ctx.exec_ctx_.get_sql_ctx();
  ObArenaAllocator alloc;
  ObStmtNeedPrivs stmt_need_privs(alloc);
  ObNeedPriv need_priv("", "", OB_PRIV_USER_LEVEL, OB_PRIV_SUPER, false);
  if (OB_ISNULL(sql_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(ObPrivilegeCheck::check_privilege(*sql_ctx, stmt_need_privs))) {
    if(OB_ERR_NO_PRIVILEGE == ret) {
      ret = OB_SUCCESS;
      is_valid = false;
      error_info = "Request ignored. SUPER needed to perform operation";
    } else {
      LOG_WARN("failed to check privilege", K(ret));
    }
  }
  return ret;
}

int ObExprAuditLogFunc::parse_user_name(const ObString &str,
                                        ObString &user_name,
                                        ObString &host,
                                        bool &is_valid,
                                        ObString &error_info)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (NULL == str.find('@')) {
    user_name = str;
    user_name = user_name.trim();
    host.reset();
  } else {
    host = str;
    user_name = host.split_on('@');
    user_name = user_name.trim();
    host = host.trim();
  }
  if (OB_SUCC(ret) && is_valid) {
    int64_t name_len = ObCharset::strlen_char(ObCharset::get_system_collation(),
                                              user_name.ptr(), user_name.length());
    if (OB_UNLIKELY(user_name.empty())) {
      is_valid = false;
      error_info = "User cannot be empty";
    } else if (OB_UNLIKELY(name_len > OB_MAX_USER_NAME_LENGTH)) {
      is_valid = false;
      error_info = "User name is too long";
    } else if (OB_UNLIKELY(0 == user_name.compare("%"))) {
      // do nothing
    } else if (OB_NOT_NULL(user_name.find('_')) || OB_NOT_NULL(user_name.find('%'))) {
      is_valid = false;
      error_info = "Invalid character in the user name";
      LOG_WARN("invalid user name", K(user_name));
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (host.empty()) {
      host = "%";
    } else if (OB_UNLIKELY(0 != host.compare("%"))) {
      is_valid = false;
      error_info = "Invalid host name";
      LOG_WARN("invalid host name", K(host));
    }
  }
  return ret;
}

int ObExprAuditLogFunc::parse_filter_name(const ObString &str,
                                          ObString &filter_name,
                                          bool &is_valid,
                                          ObString &error_info)
{
  int ret = OB_SUCCESS;
  int64_t name_len = 0;
  is_valid = true;
  filter_name = str;
  filter_name.trim();
  name_len = ObCharset::strlen_char(ObCharset::get_system_collation(),
                                    filter_name.ptr(), filter_name.length());
  if (OB_UNLIKELY(filter_name.empty())) {
    is_valid = false;
    error_info = "Filter name cannot be empty";
  } else if (OB_UNLIKELY(name_len > MAX_AUDIT_FILTER_NAME_LENGTH)) {
    is_valid = false;
    error_info = "Could not store field of the audit_log_filter table";
  }
  return ret;
}

int ObExprAuditLogFunc::parse_definition(const ObString &str,
                                         ObString &definition,
                                         bool &is_valid,
                                         ObString &error_info)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_AUDIT_SECURITY
  definition = str;
  definition = definition.trim();
  uint64_t audit_class = 0;
  if (OB_UNLIKELY(definition.empty())) {
    is_valid = false;
    error_info = "JSON parsing error";
  } else if (OB_FAIL(ObAuditLogUtils::parse_filter_definition(definition, is_valid, audit_class))) {
    LOG_WARN("failed to parse filter definition", K(ret));
  } else if (!is_valid) {
    error_info = "JSON parsing error";
  }
#endif
  return ret;
}

int ObExprAuditLogFunc::fill_res_datum(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const bool is_valid,
                                       const common::ObString &error_info,
                                       ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (is_valid) {
    expr_datum.set_string("OK");
  } else {
    int64_t buf_len = error_info.length() + strlen("ERROR: .") + 1;
    int64_t res_len = 0;
    char *buf = expr.get_str_res_mem(ctx, buf_len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, res_len, "ERROR: %.*s.",
                                       error_info.length(), error_info.ptr()))) {
      LOG_WARN("failed to print", K(ret), K(error_info));
    } else {
      expr_datum.set_string(buf, res_len);
    }
  }
  return ret;
}

ObExprAuditLogSetFilter::ObExprAuditLogSetFilter(ObIAllocator &alloc)
  : ObExprAuditLogFunc(alloc, T_FUN_SYS_AUDIT_LOG_SET_FILTER, N_AUDIT_LOG_FILTER_SET_FILTER, 2)
{
}

int ObExprAuditLogSetFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                     ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_set_filter;
  return ret;
}

int ObExprAuditLogSetFilter::eval_set_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_AUDIT_SECURITY
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObString filter_name;
    ObString definition;
    ObString error_info;
    ObSEArray<ObString, 2> params;
    bool is_valid = true;
    if (OB_FAIL(check_privilege(ctx, is_valid, error_info))) {
      LOG_WARN("failed to check privilege", K(ret));
    } else if (is_valid && OB_FAIL(parse_filter_name(arg0->get_string(), filter_name,
                                                     is_valid, error_info))) {
    } else if (is_valid && OB_FAIL(parse_definition(arg1->get_string(), definition,
                                                     is_valid, error_info))) {
      LOG_WARN("failed to parse definition", KPC(arg1));
    } else if (is_valid) {
      uint64_t tenant_id = session->get_effective_tenant_id();
      ObAuditLogFilterInfo audit_filter;
      ObAuditLogTableOperator table_operator;
      ObISQLClient *sql_client = ctx.exec_ctx_.get_sql_proxy();
      audit_filter.set_tenant_id(tenant_id);
      audit_filter.set_filter_name(filter_name);
      audit_filter.set_definition(definition);
      if (OB_ISNULL(sql_client)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sql client", K(ret));
      } else if (OB_FAIL(table_operator.init(tenant_id, sql_client))) {
        LOG_WARN("failed to init audit log table operator", K(ret), K(tenant_id));
      } else if (OB_FAIL(table_operator.set_audit_filter(audit_filter))) {
        LOG_WARN("failed to set audit log filter", K(ret), K(audit_filter));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_res_datum(expr, ctx, is_valid, error_info, expr_datum))) {
      LOG_WARN("failed to fill expr_datum", K(ret), K(is_valid), K(error_info));
    }
  }
#endif
  return ret;
}

ObExprAuditLogRemoveFilter::ObExprAuditLogRemoveFilter(ObIAllocator &alloc)
  : ObExprAuditLogFunc(alloc, T_FUN_SYS_AUDIT_LOG_REMOVE_FILTER, N_AUDIT_LOG_FILTER_REMOVE_FILTER, 1)
{
}

int ObExprAuditLogRemoveFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                     ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_remove_filter;
  return ret;
}

int ObExprAuditLogRemoveFilter::eval_remove_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_AUDIT_SECURITY
  ObDatum *arg0 = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObString filter_name;
    ObString error_info;
    ObSEArray<ObString, 1> params;
    bool is_valid = true;
    if (OB_FAIL(check_privilege(ctx, is_valid, error_info))) {
      LOG_WARN("failed to check privilege", K(ret));
    } else if (is_valid && OB_FAIL(parse_filter_name(arg0->get_string(), filter_name,
                                                     is_valid, error_info))) {
      LOG_WARN("failed to parse filter name", KPC(arg0));
    } else if (is_valid) {
      uint64_t tenant_id = session->get_effective_tenant_id();
      ObAuditLogFilterInfo audit_filter;
      ObAuditLogTableOperator table_operator;
      ObMySQLTransaction trans;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_allocator = alloc_guard.get_allocator();
      bool is_exist = false;
      audit_filter.set_tenant_id(tenant_id);
      audit_filter.set_filter_name(filter_name);
      if (OB_FAIL(trans.start(ctx.exec_ctx_.get_sql_proxy(), tenant_id))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(table_operator.init(tenant_id, &trans))) {
        LOG_WARN("failed to init audit log table operator", K(ret), K(tenant_id));
      } else if (OB_FAIL(table_operator.get_audit_filter(audit_filter, tmp_allocator,
                                                         true /*for_update*/, is_exist))) {
        LOG_WARN("failed to set audit log filter", K(ret), K(audit_filter));
      } else if (!is_exist) {
        // do nothing
      } else if (OB_FAIL(table_operator.mark_audit_user_delete_by_filter(audit_filter))) {
        LOG_WARN("failed to delete audit log user by filter", K(ret), K(audit_filter));
      } else if (OB_FAIL(table_operator.mark_audit_filter_delete(audit_filter))) {
        LOG_WARN("failed to delete audit log filter", K(ret), K(audit_filter));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(ret), K(temp_ret));
          ret = OB_SUCC(ret) ? temp_ret : ret;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_res_datum(expr, ctx, is_valid, error_info, expr_datum))) {
      LOG_WARN("failed to fill expr_datum", K(ret), K(is_valid), K(error_info));
    }
  }
#endif
  return ret;
}

ObExprAuditLogSetUser::ObExprAuditLogSetUser(ObIAllocator &alloc)
  : ObExprAuditLogFunc(alloc, T_FUN_SYS_AUDIT_LOG_SET_USER, N_AUDIT_LOG_FILTER_SET_USER, 2)
{
}

int ObExprAuditLogSetUser::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                     ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_set_user;
  return ret;
}

int ObExprAuditLogSetUser::eval_set_user(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_AUDIT_SECURITY
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObString user_name;
    ObString host;
    ObString filter_name;
    ObString error_info;
    ObSEArray<ObString, 2> params;
    bool is_valid = true;
    if (OB_FAIL(check_privilege(ctx, is_valid, error_info))) {
      LOG_WARN("failed to check privilege", K(ret));
    } else if (is_valid && OB_FAIL(parse_user_name(arg0->get_string(), user_name, host,
                                                   is_valid, error_info))) {
      LOG_WARN("failed to parse user name", KPC(arg0));
    } else if (is_valid && OB_FAIL(parse_filter_name(arg1->get_string(), filter_name,
                                                     is_valid, error_info))) {
      LOG_WARN("failed to parse filter name", KPC(arg1));
    } else if (is_valid) {
      uint64_t tenant_id = session->get_effective_tenant_id();
      ObAuditLogFilterInfo audit_filter;
      ObAuditLogUserInfo audit_user;
      ObAuditLogTableOperator table_operator;
      ObMySQLTransaction trans;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_allocator = alloc_guard.get_allocator();
      bool is_exist = false;
      audit_filter.set_tenant_id(tenant_id);
      audit_user.set_tenant_id(tenant_id);
      audit_filter.set_filter_name(filter_name);
      audit_user.set_user_name(user_name);
      audit_user.set_host(host);
      audit_user.set_filter_name(filter_name);
      if (OB_FAIL(trans.start(ctx.exec_ctx_.get_sql_proxy(), tenant_id))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(table_operator.init(tenant_id, &trans))) {
        LOG_WARN("failed to init audit log table operator", K(ret), K(tenant_id));
      } else if (OB_FAIL(table_operator.get_audit_filter(audit_filter, tmp_allocator,
                                                         true /*for_update*/, is_exist))) {
        LOG_WARN("failed to set audit log filter", K(ret), K(audit_filter));
      } else if (!is_exist) {
        // do nothing
      } else if (OB_FAIL(table_operator.set_audit_user(audit_user))) {
        LOG_WARN("failed to set audit log user", K(ret), K(audit_user));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(ret), K(temp_ret));
          ret = OB_SUCC(ret) ? temp_ret : ret;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_res_datum(expr, ctx, is_valid, error_info, expr_datum))) {
      LOG_WARN("failed to fill expr_datum", K(ret), K(is_valid), K(error_info));
    }
  }
#endif
  return ret;
}
ObExprAuditLogRemoveUser::ObExprAuditLogRemoveUser(ObIAllocator &alloc)
  : ObExprAuditLogFunc(alloc, T_FUN_SYS_AUDIT_LOG_REMOVE_USER, N_AUDIT_LOG_FILTER_REMOVE_USER, 1)
{
}

int ObExprAuditLogRemoveUser::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                     ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_remove_user;
  return ret;
}

int ObExprAuditLogRemoveUser::eval_remove_user(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_AUDIT_SECURITY
  ObDatum *arg0 = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObString user_name;
    ObString host;
    ObString error_info;
    ObSEArray<ObString, 1> params;
    bool is_valid = true;
    if (OB_FAIL(check_privilege(ctx, is_valid, error_info))) {
      LOG_WARN("failed to check privilege", K(ret));
    } else if (is_valid && OB_FAIL(parse_user_name(arg0->get_string(), user_name, host,
                                                   is_valid, error_info))) {
    } else if (is_valid) {
      uint64_t tenant_id = session->get_effective_tenant_id();
      ObAuditLogUserInfo audit_user;
      ObAuditLogTableOperator table_operator;
      ObISQLClient *sql_client = ctx.exec_ctx_.get_sql_proxy();
      audit_user.set_tenant_id(tenant_id);
      audit_user.set_user_name(user_name);
      audit_user.set_host(host);
      if (OB_ISNULL(sql_client)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sql client", K(ret));
      } else if (OB_FAIL(table_operator.init(tenant_id, sql_client))) {
        LOG_WARN("failed to init audit log table operator", K(ret), K(tenant_id));
      } else if (OB_FAIL(table_operator.mark_audit_user_delete(audit_user))) {
        LOG_WARN("failed to mark audit log user delete", K(ret), K(audit_user));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_res_datum(expr, ctx, is_valid, error_info, expr_datum))) {
      LOG_WARN("failed to fill expr_datum", K(ret), K(is_valid), K(error_info));
    }
  }
#endif
  return ret;
}

}
}

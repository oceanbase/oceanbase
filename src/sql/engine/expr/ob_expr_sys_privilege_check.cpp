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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_sys_privilege_check.h"

#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprSysPrivilegeCheck::ObExprSysPrivilegeCheck(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_SYS_PRIVILEGE_CHECK, N_SYS_PRIVILEGE_CHECK, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSysPrivilegeCheck::~ObExprSysPrivilegeCheck()
{
}

int ObExprSysPrivilegeCheck::calc_result_typeN(
    ObExprResType &type,
    ObExprResType *types,
    int64_t param_num,
    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK(NULL != types);
  CK(2 <= param_num && param_num <= 5);
  CK(NOT_ROW_DIMENSION == row_dimension_);
  if (OB_SUCC(ret)) {
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);

    types[0].set_type(ObVarcharType);
    types[0].set_calc_collation_type(ObCharset::get_system_collation());
    types[1].set_calc_type(ObIntType);
    // if param_num == 2, it means only check user-level privilege
    if (param_num >= 3) { // db_name
      types[2].set_type(ObVarcharType);
      types[2].set_calc_collation_type(ObCharset::get_system_collation());
    }
    if (param_num >= 4) { // obj_name
      types[3].set_type(ObVarcharType);
      types[3].set_calc_collation_type(ObCharset::get_system_collation());
    }
    if (param_num >= 5) { // routine_type
      types[4].set_type(ObIntType);
      types[4].set_calc_collation_type(ObCharset::get_system_collation());
    }
  }
  return ret;
}

int ObExprSysPrivilegeCheck::check_show_priv(bool &allow_show,
                                             ObExecContext &exec_ctx,
                                             const common::ObString &level_str,
                                             const uint64_t tenant_id,
                                             const common::ObString &db_name,
                                             const common::ObString &obj_name,
                                             const int64_t routine_type /* 0 */)
{
  int ret = OB_SUCCESS;
  share::schema::ObSessionPrivInfo session_priv;
  const common::ObIArray<uint64_t> &enable_role_id_array = exec_ctx.get_my_session()->get_enable_role_array();
  const share::schema::ObSchemaGetterGuard *schema_guard =
      exec_ctx.get_virtual_table_ctx().schema_guard_;
  if (OB_UNLIKELY(NULL == schema_guard)) {
    ret = OB_SCHEMA_ERROR;
  }
  allow_show = true;
  if (OB_SUCC(ret)) {
    //tenant_id in table is static casted to int64_t,
    //and use statis_cast<uint64_t> for retrieving(same with schema_service)
    // schema拆分后，普通租户schema表的tenant_id为0，此时鉴权取session_priv.tenant_id_
    if (OB_FAIL(exec_ctx.get_my_session()->get_session_priv_info(session_priv))) {
      LOG_WARN("fail to get session priv info", K(ret));
    } else if (session_priv.tenant_id_ != static_cast<uint64_t>(tenant_id)
        && OB_INVALID_TENANT_ID != tenant_id) {
      //not current tenant's row
    } else if (0 == level_str.case_compare("db_acc")) {
      if (OB_FAIL(const_cast<share::schema::ObSchemaGetterGuard *>(schema_guard)->check_db_show(
                  session_priv, enable_role_id_array, db_name, allow_show))) {
        LOG_WARN("Check db show failed", K(ret));
      }
    } else if (0 == level_str.case_compare("table_acc")) {
      //if (OB_FAIL(priv_mgr.check_table_show(session_priv,
      if (OB_FAIL(const_cast<share::schema::ObSchemaGetterGuard *>(schema_guard)->check_table_show(
                  session_priv, enable_role_id_array, db_name, obj_name, allow_show))) {
        LOG_WARN("Check table show failed", K(ret));
      }
    } else if (0 == level_str.case_compare("routine_acc")) {
      if (OB_FAIL(const_cast<share::schema::ObSchemaGetterGuard *>(schema_guard)->check_routine_show(
                  session_priv, enable_role_id_array, db_name, obj_name, allow_show, routine_type))) {
        LOG_WARN("Check routine show failed", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Check priv level error", K(ret));
    }
  }
  return ret;
}

int ObExprSysPrivilegeCheck::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  CK(2 <= expr.arg_cnt_ && expr.arg_cnt_ <= 5);
  expr.eval_func_ = eval_sys_privilege_check;
  return ret;
}

int ObExprSysPrivilegeCheck::eval_sys_privilege_check(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *level = NULL;
  ObDatum *tenant = NULL;
  ObDatum *db = NULL;
  ObDatum *obj = NULL;
  ObDatum *routine_type = NULL;
  bool allow_show = true;
  if (OB_FAIL(expr.eval_param_value(ctx, level, tenant, db, obj, routine_type))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (tenant->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is null", K(ret));
  } else if (OB_FAIL(check_show_priv(allow_show, ctx.exec_ctx_,
                                     level->is_null() ? ObString() : level->get_string(),
                                     tenant->get_int(),
                                     (NULL == db || db->is_null()) ? ObString() : db->get_string(),
                                     (NULL == obj || obj->is_null()) ? ObString() : obj->get_string(),
                                     (NULL == routine_type || routine_type->is_null()) ? 0 : routine_type->get_int()))) {
    LOG_WARN("check show privilege failed", K(ret));
  } else {
    expr_datum.set_int(allow_show ? 0 : -1);
  }

  return ret;
}

}// ns sql
}// ns oceanbase

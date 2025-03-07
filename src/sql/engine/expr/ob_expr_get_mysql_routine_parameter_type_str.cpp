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

#include "sql/engine/expr/ob_expr_get_mysql_routine_parameter_type_str.h"
#include "pl/ob_pl_stmt.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObExprGetMySQLRoutineParameterTypeStr::ObExprGetMySQLRoutineParameterTypeStr(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_GET_MYSQL_ROUTINE_PARAMETER_TYPE_STR, N_GET_MYSQL_ROUTINE_PARAMETER_TYPE_STR, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprGetMySQLRoutineParameterTypeStr::~ObExprGetMySQLRoutineParameterTypeStr()
{
}


int ObExprGetMySQLRoutineParameterTypeStr::calc_result_type2(ObExprResType &type,
                                                          ObExprResType &type1,
                                                          ObExprResType &type2,
                                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type.set_type(ObVarcharType);
  type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_length(static_cast<common::ObLength>(OB_MAX_SYS_PARAM_NAME_LENGTH));
  return ret;
}

int ObExprGetMySQLRoutineParameterTypeStr::get_mysql_routine_parameter_type_str(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    const share::schema::ObRoutineInfo *routine_info = NULL;
    ObDatum &first_param = expr.locate_param_datum(ctx, 0);
    ObDatum &second_param = expr.locate_param_datum(ctx, 1);
    int64_t routine_id = first_param.get_int();
    int64_t param_pos = second_param.get_int() - 1;
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()));
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()->schema_guard_));
    OZ (ctx.exec_ctx_.get_sql_ctx()->schema_guard_->get_routine_info(pl::get_tenant_id_by_object_id(routine_id), routine_id, routine_info), routine_id);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(routine_info)) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("routine info is NULL", K(ret), K(routine_id));
    } else {
      share::schema::ObRoutineParam *param = NULL;
      OZ (routine_info->get_routine_param(param_pos, param));
      CK (OB_NOT_NULL(param));
      if (OB_SUCC(ret)) {
        int64_t type_pos = 0;
        char * type_str = NULL;
        type_str = static_cast<char *>(ctx.get_expr_res_alloc().alloc(OB_MAX_SYS_PARAM_NAME_LENGTH));
        if (OB_ISNULL(type_str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for type str failed", K(ret));
        } else {
          bzero(type_str, OB_MAX_SYS_PARAM_NAME_LENGTH);
          OZ (ob_sql_type_str(param->get_param_type().get_meta_type(),
                              param->get_param_type().get_accuracy(),
                              param->get_extended_type_info(), ctx.exec_ctx_.get_my_session()->get_local_nls_length_semantics(),
                              type_str, OB_MAX_SYS_PARAM_NAME_LENGTH, type_pos));
          OX (res_datum.set_string(type_str, type_pos));
        }
      }
    }
  }
  return ret;
}

int ObExprGetMySQLRoutineParameterTypeStr::cg_expr(ObExprCGCtx &op_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprGetMySQLRoutineParameterTypeStr::get_mysql_routine_parameter_type_str;
  return OB_SUCCESS;
}

}
}
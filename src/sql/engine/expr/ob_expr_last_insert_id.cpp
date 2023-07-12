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
#include "sql/engine/expr/ob_expr_last_insert_id.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprLastInsertID::ObExprLastInsertID(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_LAST_INSERT_ID, N_LAST_INSERT_ID, ZERO_OR_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLastInsertID::~ObExprLastInsertID()
{
}

int ObExprLastInsertID::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_array,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_array);
  UNUSED(param_num);
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_result_flag(NOT_NULL_FLAG);
  if (1 == param_num) {
    const ObObjType arg_type = types_array[0].get_type();
    if (ob_is_int_tc(arg_type) || ob_is_uint_tc(arg_type)) {
      // get the uint value directly without extra cast
    } else if (ob_is_number_tc(arg_type)) {
      types_array[0].set_calc_type(ObIntType);
    } else {
      types_array[0].set_calc_type(ObUInt64Type);
    }
  }
  return OB_SUCCESS;
}

int ObExprLastInsertID::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &eval_last_insert_id;
  return ret;
}

int ObExprLastInsertID::eval_last_insert_id(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObDatum *arg = NULL;
  if (NULL == plan_ctx || NULL == session) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan context or session is NULL", K(ret), K(plan_ctx), K(session));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    if (0 == expr.arg_cnt_) {
      expr_datum.set_uint(session->get_local_last_insert_id());
    } else if (1 == expr.arg_cnt_) {
      plan_ctx->set_last_insert_id_with_expr(true);
      plan_ctx->set_last_insert_id_changed(true);
      if (arg->is_null()) {
        expr_datum.set_null();
        plan_ctx->set_last_insert_id_session(0);
      } else {
        expr_datum.set_uint(arg->get_uint());
        plan_ctx->set_last_insert_id_session(arg->get_uint());
      }
    }
  }
  return ret;
}

}
}

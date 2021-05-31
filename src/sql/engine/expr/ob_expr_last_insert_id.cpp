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

namespace oceanbase {
namespace sql {

ObExprLastInsertID::ObExprLastInsertID(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LAST_INSERT_ID, N_LAST_INSERT_ID, ZERO_OR_ONE, NOT_ROW_DIMENSION)
{}

ObExprLastInsertID::~ObExprLastInsertID()
{}

int ObExprLastInsertID::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_array);
  UNUSED(param_num);
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  if (1 == param_num) {
    types_array[0].set_calc_type(ObUInt64Type);
  }
  return OB_SUCCESS;
}

int ObExprLastInsertID::calc_resultN(
    common::ObObj& result, const common::ObObj* objs_array, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_array) || (OB_UNLIKELY(0 != param_num) && OB_UNLIKELY(1 != param_num))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(param_num));
  } else {
    // param_num is 0 or 1; checked in ObSysFunRawExpr::check_param_num()
    ObPhysicalPlanCtx* plan_ctx = expr_ctx.phy_plan_ctx_;
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to get plan_ctx", K(ret));
    } else {
      if (0 == param_num) {
        // last_insert_id should be updated int last insert or last_insert_id(#) in this insert
        result.set_uint64(plan_ctx->get_last_insert_id_session());
      } else if (1 == param_num) {
        uint64_t param_value = 0;
        if (objs_array[0].is_null()) {
          result.set_null();
        } else {
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_RANGE_CHECK);
          expr_ctx.cast_mode_ &= ~(CM_WARN_ON_FAIL);
          const ObObj& obj_tmp = objs_array[0];
          EXPR_GET_UINT64_V2(obj_tmp, param_value);
          result.set_uint64(param_value);
        }
        if (OB_SUCC(ret)) {
          plan_ctx->set_last_insert_id_with_expr(true);
          plan_ctx->set_last_insert_id_changed(true);
          plan_ctx->set_last_insert_id_session(param_value);
        }
      }
    }
  }
  return ret;
}

int ObExprLastInsertID::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &eval_last_insert_id;
  return ret;
}

int ObExprLastInsertID::eval_last_insert_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  ObDatum* arg = NULL;
  if (NULL == plan_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan context is NULL", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    if (0 == expr.arg_cnt_) {
      expr_datum.set_uint(plan_ctx->get_last_insert_id_session());
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

}  // namespace sql
}  // namespace oceanbase

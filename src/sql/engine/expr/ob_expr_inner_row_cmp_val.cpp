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
#include "sql/engine/expr/ob_expr_inner_row_cmp_val.h"
#include "lib/oblog/ob_log.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprInnerRowCmpVal::ObExprInnerRowCmpVal(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_INNER_ROW_CMP_VALUE, N_INNER_ROW_CMP_VALUE, 3,
      NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprInnerRowCmpVal::calc_result_type3(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprResType &type2,
                                            ObExprResType &type3,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (!ob_is_decimal_int(type1.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the 1th param type is unexpected", K(ret), K(type1));
  } else {
    type2.set_calc_meta(type1.get_obj_meta());
    type2.set_calc_accuracy(type1.get_accuracy());
    const uint64_t cast_mode = type1.get_cast_mode();
    if (0 != (cast_mode & CM_CONST_TO_DECIMAL_INT_UP)) {
      type2.add_cast_mode(CM_CONST_TO_DECIMAL_INT_DOWN);
    } else if (0 != (cast_mode & CM_CONST_TO_DECIMAL_INT_DOWN)) {
      type2.add_cast_mode(CM_CONST_TO_DECIMAL_INT_UP);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cast mode", K(ret), K(cast_mode));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_meta(type3.get_obj_meta());
    type.set_accuracy(type3.get_accuracy());
  }
  return ret;
}

int ObExprInnerRowCmpVal::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (rt_expr.arg_cnt_ != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg_cnt of eval_inner_row_cmp_val error.", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) ||
             OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg of eval_inner_row_cmp_val is null.", K(ret), K(rt_expr));
  } else if (!ob_is_decimal_int_tc(rt_expr.args_[0]->datum_meta_.type_) ||
             !ob_is_decimal_int_tc(rt_expr.args_[1]->datum_meta_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the 1th arg of eval_inner_row_cmp_val is invalid.", K(ret));
  } else if (rt_expr.args_[0]->datum_meta_.precision_ != rt_expr.args_[1]->datum_meta_.precision_
             || rt_expr.args_[0]->datum_meta_.scale_ != rt_expr.args_[1]->datum_meta_.scale_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum meta mismatch", K(ret), K(rt_expr.args_[0]->datum_meta_),
                                            K(rt_expr.args_[1]->datum_meta_));
  } else {
    rt_expr.eval_func_ = eval_inner_row_cmp_val;
    rt_expr.extra_ = raw_expr.get_extra();
  }
  return ret;
}

int ObExprInnerRowCmpVal::eval_inner_row_cmp_val(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum* left_datum = NULL;
  ObDatum* right_datum = NULL;
  ObDatum* real_val_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, left_datum))) {
    LOG_WARN("fail to eval eq expr", K(ret), K(expr));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, right_datum))) {
    LOG_WARN("fail to eval eq expr", K(ret), K(expr));
  } else if ((left_datum->is_null() != right_datum->is_null()) ||
      left_datum->len_ != right_datum->len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum result mismatch", K(ret), K(*left_datum), K(*right_datum));
  } else if (!left_datum->is_null() &&
      (0 != MEMCMP(left_datum->ptr_, right_datum->ptr_, left_datum->len_))) {
    ret = -static_cast<int>(expr.extra_);
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, real_val_datum))) {
    LOG_WARN("fail to eval real res datum fail.", K(ret), K(expr));
  } else {
    res.set_datum(*real_val_datum);
  }
  return ret;
}

}
}
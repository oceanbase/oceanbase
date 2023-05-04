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
#include "sql/engine/expr/ob_expr_elt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_equal.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}

ObExprElt::ObExprElt(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_ELT, N_ELT, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

int ObExprElt::calc_result_typeN(
  ObExprResType &type,
  ObExprResType *types_stack,
  int64_t param_num,
  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, input arguments should > 2", K(ret));
  } else if (OB_ISNULL(types_stack)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null stack", K(types_stack), K(ret));
  } else {
    type.set_varchar();
    ret = aggregate_charsets_for_string_result(
      type, types_stack + 1, param_num - 1, type_ctx.get_coll_type());
    if (OB_SUCC(ret)) {
      int32_t length = 0;
      for (int64_t i = 1; i < param_num; ++i) {
        if (!types_stack[i].is_null() && length < types_stack[i].get_length()) {
          length = types_stack[i].get_length();
        }
      }
      type.set_length(length);
    }
    if (OB_SUCC(ret)) {
      types_stack[0].set_calc_type(ObIntType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
      for (int64_t i = 1; i < param_num; ++i) {
        types_stack[i].set_calc_meta(type);
      }
    }
  }

  return ret;

}

int ObExprElt::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  CK(expr.arg_cnt_ > 1);
  expr.eval_func_ = eval_elt;
  return ret;
}

int ObExprElt::eval_elt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *first = NULL;
  ObDatum *d = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, first))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else if (first->is_null() || first->get_int() <= 0 || first->get_int() >= expr.arg_cnt_) {
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[first->get_int()]->eval(ctx, d))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    expr_datum.set_datum(*d);
  }
  return ret;
}

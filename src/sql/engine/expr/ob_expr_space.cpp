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

#include "sql/engine/expr/ob_expr_space.h"

#include "lib/utility/ob_macro_utils.h"

#include "sql/engine/expr/ob_expr_repeat.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
ObExprSpace::ObExprSpace(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_SPACE, N_SPACE, 1, NOT_ROW_DIMENSION) {}

inline int ObExprSpace::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // space is mysql only expr
  CK(lib::is_mysql_mode());

  type.set_type(ObVarcharType);
  type.set_collation_level(type1.get_collation_level());
  type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
  type.set_length(OB_MAX_VARCHAR_LENGTH);
  type1.set_calc_type(ObIntType);
  // Set cast mode for parameter casting, truncate string to integer.
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  return ret;
}

int ObExprSpace::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_space;
  return ret;
}

int ObExprSpace::eval_space(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *count = NULL;
  int64_t max_size = 0;
  bool is_null = false;
  ObString output;
  ObString space(1, " ");
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  bool has_lob_header = expr.obj_meta_.has_lob_header();
  if (OB_FAIL(expr.args_[0]->eval(ctx, count))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (count->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_size))) {
    LOG_WARN("get max length failed", K(ret));
  } else if (count->get_int() > max_size) {
    LOG_WARN("Result of space was larger than max_allow_packet_size",
             K(count->get_int()), K(max_size));
    LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "space", static_cast<int>(max_size));
    expr_datum.set_null();
  } else {
    if (!ob_is_text_tc(expr.datum_meta_.type_)) {
      ret = ObExprRepeat::repeat(output, is_null, space, count->get_int(), expr_res_alloc, max_size);
    } else { // text tc
      ret = ObExprRepeat::repeat_text(expr.datum_meta_.type_, has_lob_header, output, is_null,
                                      space, count->get_int(), expr_res_alloc, max_size);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("do repeat failed", K(ret));
    } else {
      if (is_null) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(output);
      }
    }
  }
  return ret;
}

} // sql
} // oceanbase

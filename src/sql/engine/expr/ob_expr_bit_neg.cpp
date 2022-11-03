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
#include "ob_expr_bit_neg.h"
#include "lib/oblog/ob_log.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprBitNeg::ObExprBitNeg(ObIAllocator &alloc)
    : ObBitwiseExprOperator(alloc, T_OP_BIT_NEG, N_BIT_NEG, 1, NOT_ROW_DIMENSION) {}

int ObExprBitNeg::calc_bitneg_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                   ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  uint64_t uint_val = 0;
  ObDatum *child_res = NULL;
  ObCastMode cast_mode = CM_NONE;
  void *get_uint_func = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (child_res->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(choose_get_int_func(expr.args_[0]->datum_meta_.type_, get_uint_func))) {
    LOG_WARN("choose_get_int_func failed", K(ret), K(expr.args_[0]->datum_meta_));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0,
                                    ctx.exec_ctx_.get_my_session(), cast_mode))) {
    LOG_WARN("get_default_cast_mode failed", K(ret));
  } else if (OB_FAIL((reinterpret_cast<GetUIntFunc>(get_uint_func)(*child_res, true,
                                                                   uint_val, cast_mode)))) {
    LOG_WARN("get uint from datum failed", K(ret), K(*child_res), K(cast_mode));
  } else {
    res_datum.set_uint(~uint_val);
  }
  return ret;
}

int ObExprBitNeg::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_NEG;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr for bitneg failed", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_bitneg_expr;
  }

  return ret;
}
}//end of ns sql
}//end of ns oceanbase

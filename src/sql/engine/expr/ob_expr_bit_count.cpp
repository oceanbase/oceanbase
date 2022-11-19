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
#include "sql/engine/expr/ob_expr_bit_count.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const uint8_t ObExprBitCount::char_to_num_bits[256] =
{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

ObExprBitCount::ObExprBitCount(ObIAllocator &alloc)
: ObBitwiseExprOperator(alloc, T_FUN_SYS_BIT_COUNT, "bit_count", 1, NOT_ROW_DIMENSION)
{
}

ObExprBitCount::~ObExprBitCount()
{
}

int ObExprBitCount::calc_bitcount_expr(const ObExpr &expr, ObEvalCtx &ctx,
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
    LOG_WARN("get uint64 failed", K(ret), K(*child_res));
  } else {
    uint64_t temp_result = 0;
    temp_result = static_cast<uint64_t>(
                  char_to_num_bits[static_cast<uint8_t>(uint_val)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 8)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 16)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 24)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 32)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 40)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 48)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 56)]);
    res_datum.set_uint(temp_result);
  }
  return ret;
}

int ObExprBitCount::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_COUNT;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr for bit count failed", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprBitCount::calc_bitcount_expr;
  }

  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

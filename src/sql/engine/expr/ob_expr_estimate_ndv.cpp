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
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
ObExprEstimateNdv::ObExprEstimateNdv(ObIAllocator &alloc)
:  ObFuncExprOperator(alloc, T_FUN_SYS_ESTIMATE_NDV, "estimate_ndv", 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                      INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprEstimateNdv::~ObExprEstimateNdv()
{
}

int ObExprEstimateNdv::calc_result_type1(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObCollationType coll_type = CS_TYPE_INVALID;
  type1.set_calc_type(ObVarcharType);
  OC((type_ctx.get_session()->get_collation_connection)(coll_type));
  type1.set_calc_collation_type(coll_type);
  type1.set_calc_collation_level(CS_LEVEL_IMPLICIT);
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    if (lib::is_oracle_mode()) {
      type.set_number();
      type.set_scale(0);
      type.set_precision(OB_MAX_NUMBER_PRECISION);
    } else {
      type.set_type(ObIntType);
      type.set_scale(0);
      type.set_precision(MAX_BIGINT_WIDTH);
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

void ObExprEstimateNdv::llc_estimate_ndv(int64_t &result, const ObString &bitmap_str)
{
  int ret = OB_SUCCESS;
  double res_double = 0.0;
  result = OB_INVALID_COUNT;
  if (OB_FAIL(llc_estimate_ndv(res_double, bitmap_str))) {
    LOG_WARN("calculate estimate ndv failed.");
  } else if (OB_UNLIKELY(res_double > UINT64_MAX)) {
    // 基本不会走到这里
    LOG_WARN("estimate ndv value overflows", K(res_double));
  } else {
    result = static_cast<int64_t>(res_double);
  }
}
uint64_t ObExprEstimateNdv::llc_leading_zeros(uint64_t value, uint64_t bit_width)
{
  OB_ASSERT(0ULL != value);
  return std::min(bit_width, static_cast<uint64_t>(__builtin_clzll(value)));
}

inline double ObExprEstimateNdv::llc_alpha_times_m_square(const uint64_t m)
{
  double alpha = 0.0;
  switch (m) {
  case 16:
    alpha = 0.673;
    break;
  case 32:
    alpha = 0.697;
    break;
  case 64:
    alpha = 0.709;
    break;
  default:
    alpha = 0.7213 / (1 + 1.079 / static_cast<double>(m));
    break;
  }
  return  alpha * static_cast<double>(m) * static_cast<double>(m);
}

int ObExprEstimateNdv::llc_estimate_ndv(double &estimate_ndv,
                                        const ObString &bitmap_buf)
{
  int ret = OB_SUCCESS;
  double sum_of_pmax = 0.0;
  uint64_t num_empty_buckets = 0;
  ObString::obstr_size_t llc_num_buckets = bitmap_buf.length();
  if (OB_UNLIKELY(!llc_is_num_buckets_valid(llc_num_buckets))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number of buckets is invalid", K(llc_num_buckets));
  } else {
    for (int64_t i = 0; i < llc_num_buckets; ++i) {
      sum_of_pmax += 1.0 / static_cast<double>((1ULL << (bitmap_buf[i])));
      if (bitmap_buf[i] == 0) {
        ++num_empty_buckets;
      }
    }
    estimate_ndv = llc_alpha_times_m_square(llc_num_buckets) / sum_of_pmax;
    if (OB_UNLIKELY(estimate_ndv > UINT64_MAX)) {
      LOG_WARN("estimate ndv value overflows", K(estimate_ndv));
    } else {
      if (estimate_ndv <= 2.5 * static_cast<double>(llc_num_buckets)) {
        if (0 != num_empty_buckets) {
          // use linear count
          estimate_ndv = static_cast<double>(llc_num_buckets)
              * log(static_cast<double>(llc_num_buckets) / static_cast<double>(num_empty_buckets));
        }
      }
    }
  }
  return ret;
}

bool ObExprEstimateNdv::llc_is_num_buckets_valid(int64_t num_buckets)
{
  // 要求 LLC_NUM_BUCKETS_MIN <= 桶数 <= LLC_NUM_BUCKETS_MAX 且是2的次幂
  return (num_buckets >= LLC_NUM_BUCKETS_MIN)
      && (num_buckets <= LLC_NUM_BUCKETS_MAX)
      && !(num_buckets & (num_buckets - 1));
}

int ObExprEstimateNdv::calc_estimate_ndv_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  double res_double = 0.0;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (!arg->is_null() &&
             OB_FAIL(ObExprEstimateNdv::llc_estimate_ndv(res_double, arg->get_string()))) {
    LOG_WARN("calculate estimate ndv failed.");
  } else if (OB_UNLIKELY(res_double > INT64_MAX)) {
    // 基本不会走到这里
    LOG_WARN("estimate ndv value overflows", K(res_double));
    res_datum.set_null();
  } else if (lib::is_oracle_mode()) {
    number::ObNumber result_num;
    char local_buff[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
    if (OB_FAIL(result_num.from(static_cast<int64_t>(res_double), local_alloc))) {
      LOG_WARN("fail to call from", K(ret));
    } else {
      res_datum.set_number(result_num);
    }
  } else {
    res_datum.set_int(static_cast<int64_t>(res_double));
  }
  return ret;
}

int ObExprEstimateNdv::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_estimate_ndv_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

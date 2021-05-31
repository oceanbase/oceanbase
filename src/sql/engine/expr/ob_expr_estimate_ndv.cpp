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

constexpr double DOUBLE_OVERFLOW_UINT64 = static_cast<double>(UINT64_MAX);

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
ObExprEstimateNdv::ObExprEstimateNdv(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ESTIMATE_NDV, "estimate_ndv", 1, NOT_ROW_DIMENSION)
{}

ObExprEstimateNdv::~ObExprEstimateNdv()
{}

int ObExprEstimateNdv::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_uint64();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int ObExprEstimateNdv::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  return llc_estimate_ndv(result, obj, expr_ctx);
}

int ObExprEstimateNdv::llc_estimate_ndv(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  TYPE_CHECK(obj, ObVarcharType);
  const ObString& bitmap_str = obj.get_string();
  double res_double = 0.0;
  if (OB_FAIL(llc_estimate_ndv(res_double, bitmap_str))) {
    LOG_WARN("calculate estimate ndv failed.");
  } else if (OB_UNLIKELY(res_double >= DOUBLE_OVERFLOW_UINT64)) {
    LOG_WARN("estimate ndv value overflows", K(res_double));
    result.set_null();
  } else {
    result.set_uint64(static_cast<uint64_t>(res_double));
  }
  return ret;
}

void ObExprEstimateNdv::llc_estimate_ndv(int64_t& result, const ObString& bitmap_str)
{
  int ret = OB_SUCCESS;
  double res_double = 0.0;
  result = OB_INVALID_COUNT;
  if (OB_FAIL(llc_estimate_ndv(res_double, bitmap_str))) {
    LOG_WARN("calculate estimate ndv failed.");
  } else if (OB_UNLIKELY(res_double >= DOUBLE_OVERFLOW_UINT64)) {
    LOG_WARN("estimate ndv value overflows", K(res_double));
  } else {
    result = static_cast<int64_t>(res_double);
  }
}
uint64_t ObExprEstimateNdv::llc_leading_zeros(uint64_t value, uint64_t bit_width)
{
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
  return alpha * static_cast<double>(m) * static_cast<double>(m);
}

int ObExprEstimateNdv::llc_estimate_ndv(double& estimate_ndv, const ObString& bitmap_buf)
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
    if (OB_UNLIKELY(estimate_ndv >= DOUBLE_OVERFLOW_UINT64)) {
      LOG_WARN("estimate ndv value overflows", K(estimate_ndv));
    } else {
      if (estimate_ndv <= 2.5 * static_cast<double>(llc_num_buckets)) {
        if (0 != num_empty_buckets) {
          // use linear count
          estimate_ndv = static_cast<double>(llc_num_buckets) *
                         log(static_cast<double>(llc_num_buckets) / static_cast<double>(num_empty_buckets));
        }
      }
    }
  }
  return ret;
}

bool ObExprEstimateNdv::llc_is_num_buckets_valid(int64_t num_buckets)
{
  return (num_buckets >= LLC_NUM_BUCKETS_MIN) && (num_buckets <= LLC_NUM_BUCKETS_MAX) &&
         !(num_buckets & (num_buckets - 1));
}

int ObExprEstimateNdv::calc_estimate_ndv_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg cannot be null", K(ret));
  } else {
    const ObString& bitmap_str = arg->get_string();
    double res_double = 0.0;
    if (OB_FAIL(ObExprEstimateNdv::llc_estimate_ndv(res_double, bitmap_str))) {
      LOG_WARN("calculate estimate ndv failed.");
    } else if (OB_UNLIKELY(res_double >= DOUBLE_OVERFLOW_UINT64)) {
      LOG_WARN("estimate ndv value overflows", K(res_double));
      res_datum.set_null();
    } else {
      res_datum.set_uint(static_cast<uint64_t>(res_double));
    }
  }
  return ret;
}

int ObExprEstimateNdv::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_estimate_ndv_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

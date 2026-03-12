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

#ifndef DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_
#define DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_

#include "sql/das/search/ob_das_search_define.h"

namespace oceanbase
{
namespace sql
{

class ObDASSearchUtils
{
public:
  struct FixedFormatAccessor
  {
    FixedFormatAccessor(const char *payload, int64_t len) : payload_(payload), len_(len) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(payload_ + idx * len_, len_, is_null);
    }
    const char *payload_;
    int64_t len_;
  };

  struct ContinuousFormatAccessor
  {
    ContinuousFormatAccessor(const char *payload, const uint32_t *offsets)
        : payload_(payload), offsets_(offsets) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(payload_ + offsets_[idx], offsets_[idx + 1] - offsets_[idx], is_null);
    }
    const char *payload_;
    const uint32_t *offsets_;
  };

  struct DiscreteFormatAccessor
  {
    DiscreteFormatAccessor(char **ptrs, int32_t *lens) : ptrs_(ptrs), lens_(lens) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(ptrs_[idx], lens_[idx], is_null);
    }
    char **ptrs_;
    int32_t *lens_;
  };

  struct UniformFormatAccessor
  {
    UniformFormatAccessor(const ObDatum *datums) : datums_(datums) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      UNUSED(is_null);
      return datums_[idx];
    }
    const ObDatum *datums_;
  };

  struct UniformConstFormatAccessor
  {
    UniformConstFormatAccessor(const ObDatum *datums) : datums_(datums) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      UNUSED(idx);
      UNUSED(is_null);
      return datums_[0];
    }
    const ObDatum *datums_;
  };

  static int binary_search_lower_bound(ObEvalCtx &eval_ctx,
                                       int64_t count,
                                       const ObExpr &expr,
                                       const ObDatum &target_datum,
                                       int64_t &idx)
  {
    int ret = OB_SUCCESS;
    int64_t left = idx;
    int64_t right = count;
    while (OB_SUCC(ret) && left < right) {
      int64_t mid = (left + right) >> 1;
      int cmp = 0;
      ObDatum datum;
      if (OB_FAIL(get_datum(expr, eval_ctx, mid, datum))) {
        SQL_DAS_LOG(WARN, "failed to get datum", K(ret));
      } else if (OB_FAIL(expr.basic_funcs_->null_first_cmp_(datum, target_datum, cmp))) {
        SQL_DAS_LOG(WARN,"failed to compare datums", K(ret));
      } else if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    if (OB_SUCC(ret)) {
      idx = left;
    }
    return ret;
  }

  static int get_datum(const ObExpr &expr,
                       ObEvalCtx &eval_ctx,
                       int64_t idx,
                       ObDatum &datum)
  {
    int ret = OB_SUCCESS;
    ObIVector *vec = expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "unexpected nullptr vector", K(ret));
    } else {
      const VectorFormat format = vec->get_format();
      switch (format) {
        case VEC_FIXED: {
          if (OB_ISNULL(expr.get_rev_buf(eval_ctx)) || OB_UNLIKELY(expr.res_buf_len_ <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr rev buf or res buf len is 0", K(ret), K(expr.res_buf_len_));
          } else {
            FixedFormatAccessor accessor(expr.get_rev_buf(eval_ctx), expr.res_buf_len_);
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_CONTINUOUS: {
          if (OB_ISNULL(expr.get_continuous_vector_data(eval_ctx)) ||
              OB_ISNULL(expr.get_continuous_vector_offsets(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr continuous vector data or continuous vector offsets", K(ret));
          } else {
            ContinuousFormatAccessor accessor(expr.get_continuous_vector_data(eval_ctx),
                                              expr.get_continuous_vector_offsets(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_DISCRETE: {
          if (OB_ISNULL(expr.get_discrete_vector_ptrs(eval_ctx)) || OB_ISNULL(expr.get_discrete_vector_lens(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr discrete vector ptrs or discrete vector lens", K(ret));
          } else {
            DiscreteFormatAccessor accessor(expr.get_discrete_vector_ptrs(eval_ctx),
                                            expr.get_discrete_vector_lens(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_UNIFORM: {
          if (OB_ISNULL(expr.locate_batch_datums(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr batch datums", K(ret));
          } else {
            UniformFormatAccessor accessor(expr.locate_batch_datums(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_UNIFORM_CONST: {
          if (OB_ISNULL(expr.locate_batch_datums(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr batch datums", K(ret));
          } else {
            UniformConstFormatAccessor accessor(expr.locate_batch_datums(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_DAS_LOG(WARN, "unexpected vector format", KR(ret), K(format));
          break;
      }
    }
    return ret;
  }
};

} // namespace sql
} // namespace oceanbase

#endif // DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_

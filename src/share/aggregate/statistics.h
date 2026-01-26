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

#ifndef OCEANBASE_SHARE_AGGREGATE_STATISTICS_H_
#define OCEANBASE_SHARE_AGGREGATE_STATISTICS_H_

#include "share/aggregate/iaggregate.h"
#include "share/aggregate/util.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

enum StatisticsType {
  VAR_SAMP,
  STDDEV_SAMP,
  INVALID_STATISTICS_TYPE,
};

constexpr __int128 pow10_table[]
= {static_cast<__int128>(1LL),
    static_cast<__int128>(10LL),
    static_cast<__int128>(100LL),
    static_cast<__int128>(1000LL),
    static_cast<__int128>(10000LL),
    static_cast<__int128>(100000LL),
    static_cast<__int128>(1000000LL),
    static_cast<__int128>(10000000LL),
    static_cast<__int128>(100000000LL),
    static_cast<__int128>(1000000000LL),
    static_cast<__int128>(10000000000LL),
    static_cast<__int128>(100000000000LL),
    static_cast<__int128>(1000000000000LL),
    static_cast<__int128>(10000000000000LL),
    static_cast<__int128>(100000000000000LL),
    static_cast<__int128>(1000000000000000LL),
    static_cast<__int128>(10000000000000000LL),
    static_cast<__int128>(100000000000000000LL),
    static_cast<__int128>(1000000000000000000LL),
    static_cast<__int128>(1000000000000000000LL) * 10LL,
    static_cast<__int128>(1000000000000000000LL) * 100LL,
    static_cast<__int128>(1000000000000000000LL) * 1000LL,
    static_cast<__int128>(1000000000000000000LL) * 10000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000LL,
    static_cast<__int128>(1000000000000000000LL) * 1000000LL,
    static_cast<__int128>(1000000000000000000LL) * 10000000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000LL,
    static_cast<__int128>(1000000000000000000LL) * 1000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 10000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 1000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 10000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 1000000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 10000000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
    static_cast<__int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL};

template<VecValueTypeClass in_tc, VecValueTypeClass out_tc, StatisticsType type>
class StatisticsAggregate final: public BatchAggregateWrapper<StatisticsAggregate<in_tc, out_tc, type>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
  using ResultType = AggCalcType<out_tc>;
  using ParamType = AggCalcType<in_tc>;

public:
  // Variance calculation state using Welford's online algorithm.
  // This structure maintains running statistics for numerically stable variance computation.
  //
  // Fields:
  //   mean_:  running mean of the dataset
  //   m2_:    sum of squared differences from the mean (M2 = Σ(xi - mean)²)
  //   count_: number of samples processed
  struct StatisticsCell {
    OB_INLINE void add(const double &value) {
      count_++;
      double delta = value - mean_;
      mean_ += delta / count_;
      m2_ += delta * (value - mean_);
    }
    OB_INLINE void merge(const StatisticsCell &rhs)
    {
      uint64_t total_count = count_ + rhs.count_;
      if (total_count != 0) {
        double factor = static_cast<double>(count_ * rhs.count_) / total_count;
        double delta = mean_ - rhs.mean_;

        // Choose merge formula based on numerical stability considerations:
        // - If both sources are large and similar in size: use weighted average formula
        //   to avoid catastrophic cancellation from subtracting two large, close numbers
        // - If sources differ significantly in size: use incremental update formula
        //   to prevent smaller values from being "swallowed" by larger ones
        if (are_large_and_similar(count_, rhs.count_)) {
          mean_ = (rhs.count_ * rhs.mean_ + count_ * mean_) / total_count;
        } else {
          mean_ = rhs.mean_ + delta * (static_cast<double>(count_) / total_count);
        }
        m2_ += rhs.m2_ + delta * delta * factor;
        count_ = total_count;
      }
    }
    OB_INLINE double get_result() const
    {
      double ret = 0.0;
      switch (type) {
        case VAR_SAMP: {
          ret = get_var_samp();
          break;
        }
        case STDDEV_SAMP: {
          ret = get_stddev_samp();
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected statistics type", K(type));
          break;
        }
      }
      return ret;
    }
    uint64_t get_count() const
    {
      return count_;
    }
    TO_STRING_KV(K_(mean), K_(m2), K_(count));
  private:
    // Returns true if BOTH conditions are met:
    // 1. Both counts are sufficiently large (> 10,000)
    // 2. Counts are similar in magnitude (relative difference < 0.1%)
    OB_INLINE bool are_large_and_similar(const uint64_t &a, const uint64_t &b) const
    {
      const double sensitivity = 0.001;
      const uint64_t threshold = 10000;
      bool ret = false;

      if (a != 0 && b != 0) {
        uint64_t min_val = std::min(a, b);
        uint64_t max_val = std::max(a, b);
        ret = (((1 - static_cast<double>(min_val) / max_val) < sensitivity) && (min_val > threshold));
      }
      return ret;
    }
    OB_INLINE double get_var_samp() const
    {
      return m2_ / (double)(count_ - 1);
    }
    OB_INLINE double get_stddev_samp() const
    {
      return std::sqrt(m2_ / (double)(count_ - 1));
    }
    double mean_ = 0.0;
    double m2_ = 0.0;
    uint64_t count_ = 0;
  };
  StatisticsAggregate() {}

  virtual int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    OB_ASSERT(aggr_info.expr_ != NULL);
    OB_ASSERT(aggr_info.expr_->args_ != NULL && aggr_info.expr_->arg_cnt_ > 0);

    ObDatumMeta &in_meta = aggr_info.expr_->args_[0]->datum_meta_;
    in_scale_ = in_meta.scale_;
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    UNUSED(calc_info);
    int ret = OB_SUCCESS;

    const char* param_payload = nullptr;
    int32_t param_len = 0;
    columns.get_payload(row_num, param_payload, param_len);

    StatisticsCell *cell = reinterpret_cast<StatisticsCell *>(agg_cell);
    cell->add(get_double_value(param_payload));
    return ret;
  }

  // 添加可空行
  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns,
                                 const int32_t row_num, const int32_t agg_col_id,
                                 char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      SQL_LOG(DEBUG, "skip null row", K(row_num));
    } else if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id,
                               agg_cell, tmp_res, calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                          const int32_t agg_col_id, const char *agg_cell,
                          const int32_t agg_cell_len)
  {
    UNUSED(agg_cell_len);
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));

    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      const StatisticsCell *cell = reinterpret_cast<const StatisticsCell *>(agg_cell);

      if (OB_UNLIKELY(cell->get_count() <= 1)) {
        res_vec->set_null(output_idx);
      } else {
        double result_double = cell->get_result();
        res_vec->set_double(output_idx, result_double);
      }
    } else {
      res_vec->set_null(output_idx);
    }

    return ret;
  }

  // 添加单行（用于多组聚合）
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                        const bool is_null, const char *data, const int32_t data_len,
                        int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(!is_null)) {
      StatisticsCell *cell = reinterpret_cast<StatisticsCell *>(agg_cell);
      cell->add(get_double_value(data));
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
      not_nulls.set(agg_col_idx);
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                AggrRowPtr group_row, AggrRowPtr rollup_row,
                                int64_t cur_rollup_group_idx,
                                int64_t max_group_cnt = INT64_MIN) override
  {
    UNUSED(cur_rollup_group_idx);
    UNUSED(max_group_cnt);
    int ret = OB_SUCCESS;
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    const NotNullBitVector &curr_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    const StatisticsCell *curr_cell = reinterpret_cast<const StatisticsCell *>(curr_agg_cell);
    StatisticsCell *rollup_cell = reinterpret_cast<StatisticsCell *>(rollup_agg_cell);
    if (curr_not_nulls.at(agg_col_idx) && rollup_not_nulls.at(agg_col_idx)) {
      rollup_cell->merge(*curr_cell);
    } else if (curr_not_nulls.at(agg_col_idx)) {
      rollup_cell->merge(*curr_cell);
      rollup_not_nulls.set(agg_col_idx);
    } else {
      // do nothing
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "var_samp", K(in_tc), K(out_tc));

private:
  OB_INLINE double get_double_value(const char *data) const
  {
    double ret = 0.0;
    switch (in_tc) {
      case VEC_TC_INTEGER: {
        ret = static_cast<double>(*reinterpret_cast<const int64_t *>(data));
        break;
      }
      case VEC_TC_UINTEGER: {
        ret = static_cast<double>(*reinterpret_cast<const uint64_t *>(data));
        break;
      }
      case VEC_TC_FLOAT: {
        ret = static_cast<double>(*reinterpret_cast<const float *>(data));
        break;
      }
      case VEC_TC_DOUBLE:
      case VEC_TC_FIXED_DOUBLE: {
        ret = static_cast<double>(*reinterpret_cast<const double *>(data));
        break;
      }
      case VEC_TC_DEC_INT32: {
        ret =
          static_cast<double>(*reinterpret_cast<const int32_t *>(data)) / static_cast<double>(pow10_table[in_scale_]);
        break;
      }
      case VEC_TC_DEC_INT64: {
        ret =
          static_cast<double>(*reinterpret_cast<const int64_t *>(data)) / static_cast<double>(pow10_table[in_scale_]);
        break;
      }
      case VEC_TC_DEC_INT128: {
        const int128_t *value = reinterpret_cast<const int128_t *>(data);
        __int128 dec_val = *reinterpret_cast<const __int128 *>(value->items_);
        ret = static_cast<double>(dec_val) / static_cast<double>(pow10_table[in_scale_]);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected in type", K(in_tc));
        break;
      }
    }
    return ret;
  }
  int in_scale_ = 0;
};

namespace helper
{
template<StatisticsType type>
int init_statistics_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
  ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  OB_ASSERT(aggr_info.expr_ != NULL);
  OB_ASSERT(aggr_info.expr_->args_ != NULL && aggr_info.expr_->arg_cnt_ > 0);

  ObDatumMeta &in_meta = aggr_info.expr_->args_[0]->datum_meta_;
  ObDatumMeta &out_meta = aggr_info.expr_->datum_meta_;
  VecValueTypeClass in_tc = get_vec_value_tc(in_meta.type_, in_meta.scale_, in_meta.precision_);
  VecValueTypeClass out_tc = get_vec_value_tc(out_meta.type_, out_meta.scale_, out_meta.precision_);
#define INIT_VAR_SAMP_AGGREGATE(in_tc, out_tc) \
  if (out_tc == VEC_TC_DOUBLE) { \
    ret = init_agg_func<StatisticsAggregate<in_tc, VEC_TC_DOUBLE, type>>( \
    agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg); \
  } else { \
    ret = init_agg_func<StatisticsAggregate<in_tc, VEC_TC_FIXED_DOUBLE, type>>( \
    agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg); \
  } \
  break;

  switch (in_tc) {
  case VEC_TC_INTEGER: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_INTEGER, out_tc);
    break;
  }
  case VEC_TC_UINTEGER: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_UINTEGER, out_tc);
    break;
  }
  case VEC_TC_FLOAT: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_FLOAT, out_tc);
    break;
  }
  case VEC_TC_DOUBLE: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_DOUBLE, out_tc);
    break;
  }
  case VEC_TC_FIXED_DOUBLE: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_FIXED_DOUBLE, out_tc);
    break;
  }
  case VEC_TC_DEC_INT32: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_DEC_INT32, out_tc);
    break;
  }
  case VEC_TC_DEC_INT64: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_DEC_INT64, out_tc);
    break;
  }
  case VEC_TC_DEC_INT128: {
    INIT_VAR_SAMP_AGGREGATE(VEC_TC_DEC_INT128, out_tc);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected in & out type class", K(in_tc), K(out_tc));
  }
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(WARN, "init var_samp aggregate failed", K(ret), K(in_tc), K(out_tc));
  }
  return ret;
#undef INIT_VAR_SAMP_AGGREGATE
}
} // end helper

} // end aggregate
} // end share
} // end oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_VAR_SAMP_H_

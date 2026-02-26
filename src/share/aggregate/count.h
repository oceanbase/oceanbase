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

#ifndef OCEANBASE_SHARE_COUNT_AGGREGATE_H_
#define OCEANBASE_SHARE_COUNT_AGGREGATE_H_

#include "share/aggregate/iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
template <VecValueTypeClass out_tc>
class CountAggregate final : public BatchAggregateWrapper<CountAggregate<out_tc>>
{
  static_assert(out_tc == VEC_TC_INTEGER || out_tc == VEC_TC_NUMBER, "must be integer or number");
  using BaseClass = BatchAggregateWrapper<CountAggregate<out_tc>>;

public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
  CountAggregate()
  {}
  int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx, AggrRowPtr group_row,
                         AggrRowPtr rollup_row, int64_t cur_rollup_group_idx,
                         int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    const char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    const int64_t &curr_data = *reinterpret_cast<const int64_t *>(curr_agg_cell);
    int64_t &rollup_data = *reinterpret_cast<int64_t *>(rollup_agg_cell);
    rollup_data += curr_data;
    return ret;
  }

  template <typename T>
  inline void set_skip_with_row_sel(RowSelector &row_sel, ObBitVector *tmp_skip, T &cur_vec)
  {
    for (int i = 0; i < row_sel.size(); i++) {
      int batch_idx = row_sel.index(i);
      if (!tmp_skip->at(batch_idx) && cur_vec.is_null(batch_idx)) {
        tmp_skip->set(batch_idx);
      }
    }
  }

  template <typename T>
  inline void set_skip_with_bound(const sql::EvalBound &bound, ObBitVector *tmp_skip, T &cur_vec)
  {
    for (int i = bound.start(); i < bound.end(); i++) {
      if (!tmp_skip->at(i) && cur_vec.is_null(i)) {
        tmp_skip->set(i);
      }
    }
  }

  template <typename T>
  inline void minus_diff(const sql::EvalBound &bound, int &diff, const sql::ObBitVector &skip,
                         T &cur_vec)
  {
    for (int i = bound.start(); (0 < diff) && i < bound.end(); i++) {
      if (skip.at(i) || cur_vec.is_null(i)) {
        diff -= 1;
      }
    }
  }

  template <typename T>
  inline void count_not_null(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                             RowSelector &row_sel, AggrRowPtr *agg_rows, T &cur_vec)
  {
    for (int i = 0; i < row_sel.size(); i++) {
      int batch_idx = row_sel.index(i);
      if (!cur_vec.is_null(batch_idx)) {
        char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
        int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
        count += 1;
      }
    }
  }

  template <typename T>
  inline void count_diff_not_null(int &diff, const RowSelector &row_sel, T &cur_vec)
  {
    for (int i = 0; i < row_sel.size(); i++) {
      if (!cur_vec.is_null(row_sel.index(i))) {
        diff += 1;
      }
    }
  }

  inline int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                        RowSelector &row_sel, const int64_t batch_size,
                                        const int32_t agg_col_id) override final
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;

    if (aggr_info.param_exprs_.count() == 1) {
      ObExpr *param_expr = aggr_info.param_exprs_.at(0);
      VectorFormat fmt = param_expr->get_format(eval_ctx);
      ObIVector *param_vec = param_expr->get_vector(eval_ctx);

      if (fmt == VEC_DISCRETE || fmt == VEC_CONTINUOUS || fmt == VEC_FIXED) {
        count_not_null<ObBitmapNullVectorBase>(agg_ctx, agg_col_id, row_sel, agg_rows,
                                               *static_cast<ObBitmapNullVectorBase *>(param_vec));
      } else if (fmt == VEC_UNIFORM) {
        count_not_null<ObUniformFormat<false>>(agg_ctx, agg_col_id, row_sel, agg_rows,
                                               *static_cast<ObUniformFormat<false> *>(param_vec));
      } else if (fmt == VEC_UNIFORM_CONST) {
        count_not_null<ObUniformFormat<true>>(agg_ctx, agg_col_id, row_sel, agg_rows,
                                              *static_cast<ObUniformFormat<true> *>(param_vec));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid param format", K(ret), K(fmt));
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);

      int64_t skip_size = ObBitVector::memory_size(eval_ctx.max_batch_size_);

      char *skip_buf = nullptr;
      if (OB_ISNULL(skip_buf = (char *)alloc_guard.get_allocator().alloc(skip_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMSET(skip_buf, 0, skip_size);
        ObBitVector *tmp_skip = to_bit_vector(skip_buf);

        ObIArray<ObExpr *> &param_exprs = aggr_info.param_exprs_;

        for (int param_id = 0; param_id < param_exprs.count(); param_id += 1) {
          ObIVector *param_vec = param_exprs.at(param_id)->get_vector(eval_ctx);
          VectorFormat param_fmt = param_exprs.at(param_id)->get_format(eval_ctx);

          if (param_fmt == VEC_DISCRETE || param_fmt == VEC_CONTINUOUS || param_fmt == VEC_FIXED) {
            ObBitmapNullVectorBase &cur_vec = *static_cast<ObBitmapNullVectorBase *>(param_vec);
            if (cur_vec.has_null()) {
              set_skip_with_row_sel<ObBitmapNullVectorBase>(row_sel, tmp_skip, cur_vec);
            }
          } else if (param_fmt == VEC_UNIFORM) {
            set_skip_with_row_sel<ObUniformFormat<false>>(
              row_sel, tmp_skip, *static_cast<ObUniformFormat<false> *>(param_vec));
          } else if (param_fmt == VEC_UNIFORM_CONST) {
            set_skip_with_row_sel<ObUniformFormat<true>>(
              row_sel, tmp_skip, *static_cast<ObUniformFormat<true> *>(param_vec));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "invalid param format", K(ret), K(param_fmt));
          }
        }

        for (int i = 0; i < row_sel.size() && OB_SUCC(ret); i += 1) {
          int batch_idx = row_sel.index(i);
          char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
          if (!tmp_skip->at(batch_idx)) {
            int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
            count += 1;
          }
        }
      }
    }
    return ret;
    // #undef INNER_ADD
  }

  template <typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *data, const int32_t data_len)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(data_len);
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t res_data = *reinterpret_cast<const int64_t *>(data);
    int64_t output_idx = ctx.get_batch_idx();
    ColumnFormat &columns = *static_cast<ColumnFormat *>(agg_expr.get_vector(ctx));
    SQL_LOG(DEBUG, "count collect result", K(agg_col_id), K(res_data));
    if (lib::is_oracle_mode()) {
      char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
      ObDataBuffer local_alloc(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
      number::ObNumber tmp_nmb;
      if (OB_FAIL(tmp_nmb.from(res_data, local_alloc))) {
        SQL_LOG(WARN, "number::from failed", K(ret));
      } else {
        columns.set_number(output_idx, tmp_nmb);
      }
    } else {
      columns.set_int(output_idx, res_data);
    }
    return ret;
  }

  int add_params_batch_row(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                           const sql::ObBitVector &skip, const sql::EvalBound &bound,
                           const RowSelector &row_sel, char *aggr_cell)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx::TempAllocGuard alloc_guard(agg_ctx.eval_ctx_);
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;

    int64_t skip_size = ObBitVector::memory_size(agg_ctx.eval_ctx_.max_batch_size_);

    char *skip_buf = nullptr;
    if (OB_ISNULL(skip_buf = (char *)alloc_guard.get_allocator().alloc(skip_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      MEMSET(skip_buf, 0, skip_size);
      ObBitVector *tmp_skip = to_bit_vector(skip_buf);
      // if row_sel is empty, use `skip` && `bound` to iterate data
      // else use just use `row_sel`, `skip` may contain incorrect data!!!
      if (row_sel.is_empty()) {
        tmp_skip->deep_copy(skip, bound.start(), bound.end());
      }
      for (int param_id = 0; OB_SUCC(ret) && param_id < param_exprs.count(); param_id++) {
        ObIVector *param_vec = param_exprs.at(param_id)->get_vector(agg_ctx.eval_ctx_);
        VectorFormat param_fmt = param_exprs.at(param_id)->get_format(agg_ctx.eval_ctx_);
        VecValueTypeClass param_tc = get_vec_value_tc(
          param_exprs.at(param_id)->datum_meta_.type_, param_exprs.at(param_id)->datum_meta_.scale_,
          param_exprs.at(param_id)->datum_meta_.precision_);

        if (param_fmt == VEC_DISCRETE || param_fmt == VEC_CONTINUOUS || param_fmt == VEC_FIXED) {
          ObBitmapNullVectorBase &cur_vec = *static_cast<ObBitmapNullVectorBase *>(param_vec);
          if (cur_vec.has_null()) {
            if (OB_LIKELY(row_sel.is_empty())) {
              tmp_skip->bit_or(*cur_vec.get_nulls(), bound);
            } else {
              set_skip_with_row_sel<ObBitmapNullVectorBase>(const_cast<RowSelector &>(row_sel),
                                                            tmp_skip, cur_vec);
            }
          }
        } else if (param_fmt == VEC_UNIFORM) {
          ObUniformFormat<false> &cur_vec = *static_cast<ObUniformFormat<false> *>(param_vec);
          if (OB_LIKELY(row_sel.is_empty())) {
            set_skip_with_bound<ObUniformFormat<false>>(bound, tmp_skip, cur_vec);
          } else {
            set_skip_with_row_sel<ObUniformFormat<false>>(const_cast<RowSelector &>(row_sel),
                                                          tmp_skip, cur_vec);
          }
        } else if (param_fmt == VEC_UNIFORM_CONST) {
          ObUniformFormat<true> &cur_vec = *static_cast<ObUniformFormat<true> *>(param_vec);
          if (OB_LIKELY(row_sel.is_empty())) {
            set_skip_with_bound<ObUniformFormat<true>>(bound, tmp_skip, cur_vec);
          } else {
            set_skip_with_row_sel<ObUniformFormat<true>>(const_cast<RowSelector &>(row_sel),
                                                         tmp_skip, cur_vec);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid param format", K(ret), K(param_fmt), K(param_tc));
        }

        if (OB_SUCC(ret) && param_id == param_exprs.count() - 1) {
          int64_t &count = *reinterpret_cast<int64_t *>(aggr_cell);
          int diff = 0;
          if (OB_LIKELY(row_sel.is_empty())) {
            diff = bound.range_size();
            diff -= tmp_skip->accumulate_bit_cnt(bound);
          } else {
            diff = row_sel.size();
            diff -= tmp_skip->accumulate_bit_cnt(agg_ctx.eval_ctx_.max_batch_size_);
          }

          if (OB_UNLIKELY(agg_ctx.removal_info_.enable_removal_opt_)) {
            if (agg_ctx.removal_info_.is_inverse_agg_) {
              diff = -diff;
            }
          }
          count += diff;
        }
      }
    }
    return ret;
#undef ADD_COLUMN_ROWS
  }

  class SkipCounter
  {
  public:
    int &skip_cnt;

    explicit SkipCounter(int &skip_count) : skip_cnt(skip_count)
    {}

    OB_INLINE int operator()(const uint64_t l, const uint64_t r)
    {
      skip_cnt += popcount64(l | r);
      return 0;
    }
  };

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override final
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;

    if (param_exprs.count() == 1) {
      VectorFormat fmt = VEC_INVALID;
      ObExpr *param_expr = nullptr;

      fmt = param_exprs.at(0)->get_format(ctx);
      param_expr = param_exprs.at(0);

      if (fmt == VEC_UNIFORM || fmt == VEC_UNIFORM_CONST || fmt == VEC_DISCRETE
          || fmt == VEC_CONTINUOUS || fmt == VEC_FIXED) {
        ObVectorBase &columns = *static_cast<ObVectorBase *>(param_expr->get_vector(ctx));
        bool all_not_null = !columns.has_null();
        int diff = 0;

        if (row_sel.is_empty()) {
          diff = bound.range_size();
          if (all_not_null && bound.get_all_rows_active()) {
          } else {
            if (fmt == VEC_DISCRETE || fmt == VEC_CONTINUOUS || fmt == VEC_FIXED) {
              ObBitmapNullVectorBase &columns =
                *static_cast<ObBitmapNullVectorBase *>(param_expr->get_vector(ctx));
              const sql::ObBitVector *null_vec = columns.get_nulls();

              if (bound.get_all_rows_active()) {
                diff -= null_vec->accumulate_bit_cnt(bound);
              } else if (all_not_null) {
                diff -= skip.accumulate_bit_cnt(bound);
              } else {
                int skip_cnt = 0;
                SkipCounter counter(skip_cnt);
                ObBitVector::bit_op_zero(skip, *null_vec, bound, counter);
                diff -= skip_cnt;
              }
            } else if (fmt == VEC_UNIFORM) {
              minus_diff<ObUniformFormat<false>>(
                bound, diff, skip,
                *static_cast<ObUniformFormat<false> *>(param_expr->get_vector(ctx)));
            } else {
              minus_diff<ObUniformFormat<true>>(
                bound, diff, skip,
                *static_cast<ObUniformFormat<true> *>(param_expr->get_vector(ctx)));
            }
          }
        } else {
          if (fmt == VEC_DISCRETE || fmt == VEC_CONTINUOUS || fmt == VEC_FIXED) {
            count_diff_not_null<ObBitmapNullVectorBase>(
              diff, row_sel, *static_cast<ObBitmapNullVectorBase *>(param_expr->get_vector(ctx)));
          } else if (fmt == VEC_UNIFORM) {
            count_diff_not_null<ObUniformFormat<false>>(
              diff, row_sel, *static_cast<ObUniformFormat<false> *>(param_expr->get_vector(ctx)));
          } else {
            count_diff_not_null<ObUniformFormat<true>>(
              diff, row_sel, *static_cast<ObUniformFormat<true> *>(param_expr->get_vector(ctx)));
          }
        }

        if (OB_UNLIKELY(agg_ctx.removal_info_.enable_removal_opt_)) {
          if (agg_ctx.removal_info_.is_inverse_agg_) {
            diff = -diff;
          }
        }
        int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
        data += diff;
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected fmt", K(fmt), K(*param_exprs.at(0)));
      }

      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "add batch rows failed", K(ret), K(fmt), K(param_exprs.at(0)->datum_meta_));
      }

      // count *
    } else if (param_exprs.empty()) {
      ObItemType agg_fun_type = agg_ctx.aggr_infos_.at(agg_col_id).get_expr_type();
      if (T_FUN_COUNT == agg_fun_type) {
        if (OB_FAIL(quick_add_batch_rows_for_count(this, agg_ctx, false, skip, bound, row_sel,
                                                   agg_col_id, agg_cell))) {
          SQL_LOG(WARN, "quick add batch rows failed", K(ret));
        }
      } else if (T_FUN_GROUP_ID == agg_fun_type) {
        // TODO:
        ret = OB_NOT_IMPLEMENT;
        SQL_LOG(ERROR, "T_FUN_GROUP_ID is not implemented");
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected aggregate function", K(ret), K(agg_fun_type),
                K(*agg_ctx.aggr_infos_.at(agg_col_id).expr_));
      }
      // count distinct
    } else {
      if (OB_FAIL(add_params_batch_row(agg_ctx, agg_col_id, skip, bound, row_sel, agg_cell))) {
        SQL_LOG(WARN, "add param batch rows failed", K(ret));
      }
    }

    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    UNUSEDx(agg_ctx, row_num, batch_size, data_len, agg_col_idx);
    int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
    if (OB_LIKELY(agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.count() == 1)) {
      if (!is_null) {
        count++;
      }
    } else {
      // multiply params
      ObAggrInfo &info = agg_ctx.aggr_infos_.at(agg_col_idx);
      bool has_null = false;
      for (int i = 0; !has_null && i < info.param_exprs_.count(); i++) {
        has_null = info.param_exprs_.at(i)->get_vector(agg_ctx.eval_ctx_)->is_null(row_num);
      }
      if (!has_null) {
        count++;
      }
    }
    return OB_SUCCESS;
  }

  TO_STRING_KV("aggregate", "count");
};

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_COUNT_AGGREGATE_H_

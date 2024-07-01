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
#include "count.h"
#include "share/aggregate/single_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
int init_count_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                         ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_COUNT_CASE(vec_tc)                                                                    \
  case (vec_tc): {                                                                                 \
    if (lib::is_oracle_mode()) {                                                                   \
      ret = init_agg_func<CountAggregate<vec_tc, VEC_TC_NUMBER>>(agg_ctx, agg_col_id,              \
                                                                 has_distinct, allocator, agg);    \
    } else {                                                                                       \
      ret = init_agg_func<CountAggregate<vec_tc, VEC_TC_INTEGER>>(agg_ctx, agg_col_id,             \
                                                                  has_distinct, allocator, agg);   \
    }                                                                                              \
  } break

  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  agg = nullptr;
  bool has_distinct = aggr_info.has_distinct_;
  // count(*) param_exprs is empty
  VecValueTypeClass vec_tc = VEC_TC_INTEGER;
  if (aggr_info.param_exprs_.count() > 0) {
    vec_tc =
      get_vec_value_tc(aggr_info.get_first_child_type(), aggr_info.get_first_child_datum_scale(),
                       aggr_info.get_first_child_datum_precision());
  }
  switch (vec_tc) {
    LST_DO_CODE(INIT_COUNT_CASE, AGG_VEC_TC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param format", K(ret), K(vec_tc));
    }
  }
  return ret;
#undef INIT_COUNT_CASE
}

} // end namespace helper

int quick_add_batch_rows_for_count(IAggregate *agg, RuntimeContext &agg_ctx,
                                   const bool is_single_row_agg, const sql::ObBitVector &skip,
                                   const sql::EvalBound &bound, const RowSelector &row_sel,
                                   const int32_t agg_col_id, char *agg_cell)
{
  int ret = OB_SUCCESS;
  ObUniformVector<true, VectorBasicOp<VEC_TC_NULL>> mock_cols(nullptr, nullptr);
  int64_t fake_calc_info = 0;
  if (OB_ISNULL(agg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null aggregate", K(ret));
  } else if (!is_single_row_agg) {
    auto &count_agg = *static_cast<CountAggregate<VEC_TC_INTEGER, VEC_TC_INTEGER> *>(agg);
    if (OB_UNLIKELY(agg_ctx.removal_info_.enable_removal_opt_)) {
      int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
      if (!agg_ctx.removal_info_.is_inverse_agg_) {
        if (row_sel.is_empty()) {
          count += bound.range_size() - skip.accumulate_bit_cnt(bound);
        } else {
          count += row_sel.size();
        }
      } else {
        if (row_sel.is_empty()) {
          count -= bound.range_size() - skip.accumulate_bit_cnt(bound);
        } else {
          count -= row_sel.size();
        }
      }
    } else if (OB_LIKELY(row_sel.is_empty() && bound.get_all_rows_active())) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret =
          count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
      }
    } else if (!row_sel.is_empty()) {
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        ret = count_agg.add_row(agg_ctx, mock_cols, row_sel.index(i), agg_col_id, agg_cell, nullptr,
                                fake_calc_info);
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i)) {
        } else {
          ret =
            count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
        }
      }
    }
  } else if (lib::is_mysql_mode()) {
    auto &count_agg = *static_cast<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_INTEGER> *>(agg);
    if (OB_LIKELY(row_sel.is_empty() && bound.get_all_rows_active())) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret =
          count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
      }
    } else if (!row_sel.is_empty()) {
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        ret = count_agg.add_row(agg_ctx, mock_cols, row_sel.index(i), agg_col_id, agg_cell, nullptr,
                                fake_calc_info);
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i)) {
        } else {
          ret =
            count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
        }
      }
    }
  } else {
    auto &count_agg = *static_cast<SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_NUMBER> *>(agg);
    if (OB_LIKELY(row_sel.is_empty() && bound.get_all_rows_active())) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret =
          count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
      }
    } else if (!row_sel.is_empty()) {
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        ret = count_agg.add_row(agg_ctx, mock_cols, row_sel.index(i), agg_col_id, agg_cell, nullptr,
                                fake_calc_info);
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i)) {
        } else {
          ret =
            count_agg.add_row(agg_ctx, mock_cols, i, agg_col_id, agg_cell, nullptr, fake_calc_info);
        }
      }
    }
  }
  SQL_LOG(DEBUG, "count: quick add batch rows", K(ret), K(*reinterpret_cast<int64_t *>(agg_cell)),
          K(agg_col_id), K(is_single_row_agg));
  if (OB_FAIL(ret)) { SQL_LOG(WARN, "count: quick add batch rows failed", K(ret)); }
  return ret;
}

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
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

#ifndef  OCEANBASE_SHARE_COUNT_AGGREGATE_H_
#define  OCEANBASE_SHARE_COUNT_AGGREGATE_H_

#include "share/aggregate/iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

template<VecValueTypeClass in_tc, VecValueTypeClass out_tc>
class CountAggregate final: public BatchAggregateWrapper<CountAggregate<in_tc, out_tc>>
{
static_assert(out_tc == VEC_TC_INTEGER || out_tc == VEC_TC_NUMBER, "must be integer or number");
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  CountAggregate() {}

  template <typename ColumnFmt>
  int add_param_batch(RuntimeContext &agg_ctx, const ObBitVector &skip, ObBitVector &pvt_skip,
                      const EvalBound &bound, const RowSelector &row_sel, const int32_t agg_col_id,
                      const int32_t param_id, ColumnFmt &param_vec, char *aggr_cell)
  {
    UNUSED(skip);
    int ret = OB_SUCCESS;
    if (row_sel.is_empty()) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (pvt_skip.at(i)) {
        } else if (param_vec.is_null(i)) {
          pvt_skip.set(i);
        }
      }
    } else {
      for (int i = 0; i < row_sel.size(); i++) {
        if (param_vec.is_null(row_sel.index(i))) { pvt_skip.set(row_sel.index(i)); }
      }
    }
    if (param_id == agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() - 1) {
      // last param
      int64_t &count = *reinterpret_cast<int64_t *>(aggr_cell);
      if (OB_LIKELY(!agg_ctx.removal_info_.enable_removal_opt_)
          || !agg_ctx.removal_info_.is_inverse_agg_) {
        if (row_sel.is_empty()) {
          for (int i = bound.start(); i < bound.end(); i++) { count += !pvt_skip.at(i); }
        } else {
          for (int i = 0; i < row_sel.size(); i++) { count += !pvt_skip.at(row_sel.index(i)); }
        }
      } else {
        if (row_sel.is_empty()) {
          for (int i = bound.start(); i < bound.end(); i++) { count -= !pvt_skip.at(i); }
        } else {
          for (int i = 0; i < row_sel.size(); i++) { count -= !pvt_skip.at(row_sel.index(i)); }
        }
      }
    }
    return ret;
  }
  template <typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *data, const int32_t data_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(data_len);
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

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    UNUSEDx(agg_ctx, row_num, batch_size, data_len, agg_col_idx);
    int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
    if (OB_LIKELY(agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.count() == 1)) {
      if (!is_null) { count++; }
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
  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, tmp_res, calc_info, agg_col_id);
    int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
    data++;
    return OB_SUCCESS;
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, tmp_res, calc_info, agg_col_id);
    if (!columns.is_null(row_num)) {
      int64_t &data = *reinterpret_cast<int64_t *>(agg_cell);
      data++;
    }
    return OB_SUCCESS;
  }

  template <typename ColumnFmt>
  int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      int64_t &count = *reinterpret_cast<int64_t *>(agg_cell);
      if (is_trans) {
        count++;
      } else {
        count--;
      }
    }
    return ret;
  }
  TO_STRING_KV("aggregate", "count");
};

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_COUNT_AGGREGATE_H_

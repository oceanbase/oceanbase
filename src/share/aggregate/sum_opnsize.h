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

#ifndef OCEANBASE_SHARE_AGGREGATE_SUM_OPNSIZE_H
#define OCEANBASE_SHARE_AGGREGATE_SUM_OPNSIZE_H

#include "share/aggregate/iaggregate.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
template <VecValueTypeClass out_tc, bool has_lob_header>
class SumOpNSize final
  : public BatchAggregateWrapper<SumOpNSize<out_tc, has_lob_header>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  SumOpNSize() {}

  OB_INLINE int sum_lob_length(const char *payload, int32_t len, int64_t &diff)
  {
    int ret = OB_SUCCESS;
    ObString lob_data(len, payload);
    ObLobLocatorV2 locator(lob_data, true);
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      SQL_LOG(WARN, "get lob data byte length failed", K(ret));
    } else {
      diff += sizeof(ObDatum) + lob_data_byte_len;
    }
    return ret;
  }

  template <typename T, bool has_null, bool has_skip>
  OB_INLINE void sum_diff_base(const sql::EvalBound &bound, int64_t &diff, const sql::ObBitVector &skip,
                               T &cur_vec)
  {
    if (!has_null && !has_skip) {
      for (int i = bound.start(); i < bound.end(); i++) {
        diff += sizeof(ObDatum) + cur_vec.get_length(i);
      }
    } else if (!has_null) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (skip.at(i)) {
        } else {
          diff += sizeof(ObDatum) + cur_vec.get_length(i);
        }
      }
    } else if (!has_skip) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (cur_vec.is_null(i)) {
          diff += sizeof(ObDatum);
        } else {
          diff += sizeof(ObDatum) + cur_vec.get_length(i);
        }
      }
    } else {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (skip.at(i)) {
        } else if (cur_vec.is_null(i)) {
          diff += sizeof(ObDatum);
        } else {
          diff += sizeof(ObDatum) + cur_vec.get_length(i);
        }
      }
    }
  }

  template <typename T>
  OB_INLINE int sum_diff(const sql::EvalBound &bound, int64_t &diff, const sql::ObBitVector &skip,
                         T &cur_vec, bool not_has_skip, bool not_has_null)
  {
    int ret = OB_SUCCESS;
    if (!has_lob_header) {
      if (not_has_skip && not_has_null) {
        sum_diff_base<T, false, false>(bound, diff, skip, cur_vec);
      } else if (not_has_skip) {
        sum_diff_base<T, true, false>(bound, diff, skip, cur_vec);
      } else if (not_has_null) {
        sum_diff_base<T, false, true>(bound, diff, skip, cur_vec);
      } else {
        sum_diff_base<T, true, true>(bound, diff, skip, cur_vec);
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (skip.at(i)) {
        } else if (cur_vec.is_null(i)) {
          diff += sizeof(ObDatum);
        } else {
          const char *payload = nullptr;
          int32_t len = 0;
          cur_vec.get_payload(i, payload, len);
          ret = sum_lob_length(payload, len, diff);
        }
      }
    }
    return ret;
  }

  template <>
  OB_INLINE int sum_diff<ObFixedLengthBase>(const sql::EvalBound &bound, int64_t &diff, const sql::ObBitVector &skip,
                                            ObFixedLengthBase &cur_vec, bool not_has_skip, bool not_has_null)
  {
    int ret = OB_SUCCESS;
    const sql::ObBitVector *null_vec = cur_vec.get_nulls();
    if (not_has_skip && not_has_null) {
      diff += bound.range_size() * (sizeof(ObDatum) +  cur_vec.get_length());
    } else if (not_has_skip) {
      diff += (sizeof(ObDatum) * bound.range_size()) + (bound.range_size()
                - null_vec->accumulate_bit_cnt(bound)) * cur_vec.get_length();
    } else if (not_has_null) {
      diff += (bound.range_size() - skip.accumulate_bit_cnt(bound))
                * (sizeof(ObDatum) + cur_vec.get_length());
    } else {
      int cnt = bound.range_size() - skip.accumulate_bit_cnt(bound);
      diff += (sizeof(ObDatum) * cnt) + (cnt
                - null_vec->accumulate_bit_cnt(bound)) * cur_vec.get_length();
    }
    return ret;
  }

  template <typename T>
  OB_INLINE int sum_diff_row_sel(int64_t &diff, const RowSelector &row_sel, T &cur_vec)
  {
    int ret = OB_SUCCESS;
    if (!has_lob_header) {
      for (int i = 0; i < row_sel.size(); i++) {
        if (cur_vec.is_null(row_sel.index(i))) {
          diff += sizeof(ObDatum);
        } else {
          diff += sizeof(ObDatum) + cur_vec.get_length(row_sel.index(i));
        }
      }
    } else {
      for (int rs = 0; OB_SUCC(ret) && rs < row_sel.size(); rs++) {
        int index = row_sel.index(rs);
        if (cur_vec.is_null(index)) {
          diff += sizeof(ObDatum);
        } else {
          const char *payload = nullptr;
          int32_t len = 0;
          cur_vec.get_payload(index, payload, len);
          ret = sum_lob_length(payload, len, diff);
        }
      }
    }
    return ret;
  }

  template <>
  OB_INLINE int sum_diff_row_sel<ObFixedLengthBase>(int64_t &diff, const RowSelector &row_sel, ObFixedLengthBase &cur_vec)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; i < row_sel.size(); i++) {
      if (cur_vec.is_null(row_sel.index(i))) {
        diff += sizeof(ObDatum);
      } else {
        diff += sizeof(ObDatum) + cur_vec.get_length();
      }
    }
    return ret;
  }

  template <typename T>
  OB_INLINE int sum_row_sel(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                          RowSelector &row_sel, AggrRowPtr *agg_rows, T &cur_vec)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
      int batch_idx = row_sel.index(i);
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
      int64_t &sum = *reinterpret_cast<int64_t *>(agg_cell);
      if (cur_vec.is_null(batch_idx)) {
        sum += sizeof(ObDatum);
      } else if (!has_lob_header) {
        sum += sizeof(ObDatum) + cur_vec.get_length(batch_idx);
      } else {
        const char *payload = nullptr;
        int32_t len = 0;
        cur_vec.get_payload(batch_idx, payload, len);
        ret = sum_lob_length(payload, len, sum);
      }
    }
    return ret;
  }

  template <>
  OB_INLINE int sum_row_sel<ObFixedLengthBase>(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                             RowSelector &row_sel, AggrRowPtr *agg_rows,
                                             ObFixedLengthBase &cur_vec)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
      int batch_idx = row_sel.index(i);
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
      int64_t &sum = *reinterpret_cast<int64_t *>(agg_cell);
      if (cur_vec.is_null(batch_idx)) {
        sum += sizeof(ObDatum);
      } else if (!has_lob_header) {
        sum += sizeof(ObDatum) + cur_vec.get_length();
      } else {
        int32_t len = cur_vec.get_length();
        const char *payload = reinterpret_cast<const char *>(cur_vec.get_data() + len * batch_idx);
        ret = sum_lob_length(payload, len, sum);
      }
    }
    return ret;
  }

  OB_INLINE bool is_valid_format(VectorFormat fmt)
  {
    return (fmt == VEC_UNIFORM || fmt == VEC_UNIFORM_CONST || fmt == VEC_DISCRETE
              || fmt == VEC_CONTINUOUS || fmt == VEC_FIXED);
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override final
  {
    int ret = OB_SUCCESS;
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
    VectorFormat fmt = param_exprs.at(0)->get_format(ctx);
    ObExpr *param_expr = param_exprs.at(0);
    if (is_valid_format(fmt)) {
      ObVectorBase &columns = *static_cast<ObVectorBase *>(param_expr->get_vector(ctx));
      bool all_not_null = !columns.has_null();
      int64_t &sum = *reinterpret_cast<int64_t *>(agg_cell);
      int64_t diff = 0;
      if (row_sel.is_empty()) {

        if (fmt == VEC_FIXED) {
          ret = sum_diff<ObFixedLengthBase>(bound, diff, skip, static_cast<ObFixedLengthBase&>(columns),
                                      bound.get_all_rows_active(), all_not_null);
        } else if (fmt == VEC_CONTINUOUS) {
          ret = sum_diff<ObContinuousFormat>(bound, diff, skip, static_cast<ObContinuousFormat&>(columns),
                                       bound.get_all_rows_active(), all_not_null);
        } else if (fmt == VEC_DISCRETE) {
          ret = sum_diff<ObDiscreteFormat>(bound, diff, skip, static_cast<ObDiscreteFormat&>(columns),
                                     bound.get_all_rows_active(), all_not_null);
        } else if (fmt == VEC_UNIFORM) {
          ret = sum_diff<ObUniformFormat<false>>(bound, diff, skip, static_cast<ObUniformFormat<false>&>(columns),
                                           bound.get_all_rows_active(), all_not_null);
        } else if (fmt == VEC_UNIFORM_CONST) {
          ret = sum_diff<ObUniformFormat<true>>(bound, diff, skip, static_cast<ObUniformFormat<true>&>(columns),
                                          bound.get_all_rows_active(), all_not_null);
        }
      } else {
        if (fmt == VEC_FIXED) {
          ret = sum_diff_row_sel<ObFixedLengthBase>(diff, row_sel, static_cast<ObFixedLengthBase&>(columns));
        } else if (fmt == VEC_CONTINUOUS) {
          ret = sum_diff_row_sel<ObContinuousFormat>(diff, row_sel, static_cast<ObContinuousFormat&>(columns));
        } else if (fmt == VEC_DISCRETE) {
          ret = sum_diff_row_sel<ObDiscreteFormat>(diff, row_sel, static_cast<ObDiscreteFormat&>(columns));
        } else if (fmt == VEC_UNIFORM) {
          ret = sum_diff_row_sel<ObUniformFormat<false>>(diff, row_sel, static_cast<ObUniformFormat<false>&>(columns));
        } else if (fmt == VEC_UNIFORM_CONST) {
          ret = sum_diff_row_sel<ObUniformFormat<true>>(diff, row_sel, static_cast<ObUniformFormat<true>&>(columns));
        }
      }
      if (OB_SUCC(ret)) {
        sum += diff;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid param format", K(ret), K(fmt));
    }
    return ret;
  }

  inline int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                        RowSelector &row_sel, const int64_t batch_size,
                                        const int32_t agg_col_id) override final
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
    ObExpr *param_expr = aggr_info.param_exprs_.at(0);
    VectorFormat fmt = param_expr->get_format(eval_ctx);
    ObIVector *param_vec = param_expr->get_vector(eval_ctx);
    if (is_valid_format(fmt)) {
      if (fmt == VEC_FIXED) {
        ret = sum_row_sel<ObFixedLengthBase>(agg_ctx, agg_col_id, row_sel, agg_rows, static_cast<ObFixedLengthBase&>(*param_vec));
      } else if (fmt == VEC_CONTINUOUS) {
        ret = sum_row_sel<ObContinuousFormat>(agg_ctx, agg_col_id, row_sel, agg_rows, static_cast<ObContinuousFormat&>(*param_vec));
      } else if (fmt == VEC_DISCRETE) {
        ret = sum_row_sel<ObDiscreteFormat>(agg_ctx, agg_col_id, row_sel, agg_rows, static_cast<ObDiscreteFormat&>(*param_vec));
      } else if (fmt == VEC_UNIFORM) {
        ret = sum_row_sel<ObUniformFormat<false>>(agg_ctx, agg_col_id, row_sel, agg_rows, static_cast<ObUniformFormat<false>&>(*param_vec));
      } else if (fmt == VEC_UNIFORM_CONST) {
        ret = sum_row_sel<ObUniformFormat<true>>(agg_ctx, agg_col_id, row_sel, agg_rows, static_cast<ObUniformFormat<true>&>(*param_vec));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid param format", K(ret), K(fmt));
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
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

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    int64_t res_data = *reinterpret_cast<const int64_t *>(agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    if (lib::is_mysql_mode()) {
      res_vec->set_int(output_idx, res_data);
    } else {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber res_nmb;
      if (OB_FAIL(res_nmb.from(res_data, tmp_alloc))) {
        SQL_LOG(WARN, "convert int to number failed", K(ret));
      } else {
        res_vec->set_number(output_idx, res_nmb);
      }
    }
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                  const bool is_null, const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    int64_t &sum = *reinterpret_cast<int64_t *>(agg_cell);
    if (is_null) {
      sum += sizeof(ObDatum);
    } else if (!has_lob_header) {
      data += sizeof(ObDatum) + data_len;
    } else {
      ObString lob_data(data_len, data);
      ObLobLocatorV2 locator(lob_data, true);
      int64_t lob_data_byte_len = 0;
      if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
        SQL_LOG(WARN, "get lob data byte length failed", K(ret));
      } else {
        data += sizeof(ObDatum) + static_cast<int64_t>(lob_data_byte_len);
      }
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "sum_opnsize", K(out_tc));
private:
};

} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_SUM_OPNSIZE_H
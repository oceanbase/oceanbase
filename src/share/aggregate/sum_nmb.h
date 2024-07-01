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

#ifndef OCEANBASE_SHARE_AGGREGATE_SUM_NMB_H_
#define OCEANBASE_SHARE_AGGREGATE_SUM_NMB_H_

#include "share/aggregate/iaggregate.h"
#include "sql/engine/aggregate/ob_aggregate_util.h"

#define ACCUMULATE_NMB(fmt)                                                                        \
  if (!input_vec->has_null()) {                                                                    \
    if (row_sel.is_empty()) {                                                                      \
      ret = sql::number_accumulator(input_wrapper<fmt, false>(input_vec), allocator1, allocator2,  \
                                    result, sum_digits, all_skip, bound_wrapper(bound, skip));     \
    } else {                                                                                       \
      ret = sql::number_accumulator(input_wrapper<fmt, false>(input_vec), allocator1, allocator2,  \
                                    result, sum_digits, all_skip, rowsel_wrapper(row_sel));        \
    }                                                                                              \
  } else {                                                                                         \
    if (row_sel.is_empty()) {                                                                      \
      ret = sql::number_accumulator(input_wrapper<fmt, true>(input_vec), allocator1, allocator2,   \
                                    result, sum_digits, all_skip, bound_wrapper(bound, skip));     \
    } else {                                                                                       \
      ret = sql::number_accumulator(input_wrapper<fmt, true>(input_vec), allocator1, allocator2,   \
                                    result, sum_digits, all_skip, rowsel_wrapper(row_sel));        \
    }                                                                                              \
  }

#define ADD_OR_SUB_NMB(fmt)                                                                        \
  do {                                                                                             \
    fmt *columns = static_cast<fmt *>(input_vec);                                                  \
    if (row_sel.is_empty()) {                                                                      \
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {                          \
        if (skip.at(i)) {                                                                          \
        } else {                                                                                   \
          ret = add_or_sub_row<fmt>(agg_ctx, *columns, i, agg_col_id, agg_cell, nullptr,           \
                                    mock_calc_info);                                               \
        }                                                                                          \
      }                                                                                            \
    } else {                                                                                       \
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {                                   \
        ret = add_or_sub_row<fmt>(agg_ctx, *columns, row_sel.index(i), agg_col_id, agg_cell,       \
                                  nullptr, mock_calc_info);                                        \
      }                                                                                            \
    }                                                                                              \
  } while (false)

namespace oceanbase
{
namespace share
{
namespace aggregate
{
// fast number sum aggregation, wrapper of `sql::number_accumulator`
class SumNumberAggregate final: public BatchAggregateWrapper<SumNumberAggregate>
{
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NUMBER;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_NUMBER;

public:
  SumNumberAggregate(): is_ora_count_sum_(false) {}
  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    is_ora_count_sum_ = agg_ctx.aggr_infos_.at(agg_col_id).get_expr_type() == T_FUN_COUNT_SUM;
    return ret;
  }
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(!is_null)) {
      ObNumber l_num(*reinterpret_cast<const ObCompactNumber *>(data));
      ObNumber r_num(*reinterpret_cast<const ObCompactNumber *>(agg_cell));
      char res_buf[ObNumber::MAX_CALC_BYTE_LEN] = {0};
      ObCompactNumber *res_cnum = reinterpret_cast<ObCompactNumber *>(res_buf);
      ObNumber::Desc &res_desc = res_cnum->desc_;
      uint32_t *res_digits = res_cnum->digits_;
      if (ObNumber::try_fast_add(l_num, r_num, res_digits, res_desc)) {
        int32_t cp_len = sizeof(ObNumberDesc) + res_desc.len_ * sizeof(uint32_t);
        MEMCPY(agg_cell, res_buf, cp_len);
      } else {
        const ObCompactNumber *l_cnum = reinterpret_cast<const ObCompactNumber *>(data);
        const ObCompactNumber *r_cnum = reinterpret_cast<const ObCompactNumber *>(agg_cell);
        ret = add_values(*l_cnum, *r_cnum, agg_cell, ObNumber::MAX_CALC_BYTE_LEN);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "adder number failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
        not_nulls.set(agg_col_idx);
      }
    }
    return ret;
  }
  inline int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                            const sql::ObBitVector &skip, const sql::EvalBound &bound,
                            char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
#ifndef NDEBUG
    helper::print_input_rows(row_sel, skip, bound, agg_ctx.aggr_infos_.at(agg_col_id), false,
                             agg_ctx.eval_ctx_, this, agg_col_id);
#endif // NDEBUG
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() == 1);
    ObExpr *param_expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0);
    OB_ASSERT(param_expr != NULL);
    ObIVector *input_vec = param_expr->get_vector(agg_ctx.eval_ctx_);
    VectorFormat in_fmt = param_expr->get_format(agg_ctx.eval_ctx_);
    if (OB_LIKELY(!agg_ctx.removal_info_.enable_removal_opt_)) {
      uint32_t sum_digits[number::ObNumber::OB_CALC_BUFFER_SIZE] = {0};
      char buf_alloc1[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
      char buf_alloc2[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
      ObDataBuffer allocator1(buf_alloc1, number::ObNumber::MAX_CALC_BYTE_LEN);
      ObDataBuffer allocator2(buf_alloc2, number::ObNumber::MAX_CALC_BYTE_LEN);
      number::ObNumber result(*reinterpret_cast<number::ObCompactNumber *>(agg_cell));
      bool all_skip = true;
      switch (in_fmt) {
      case common::VEC_UNIFORM: {
        ACCUMULATE_NMB(ObUniformFormat<false>);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ACCUMULATE_NMB(ObUniformFormat<true>);
        break;
      }
      case common::VEC_DISCRETE: {
        ACCUMULATE_NMB(ObDiscreteFormat);
        break;
      }
      case common::VEC_CONTINUOUS: {
        ACCUMULATE_NMB(ObContinuousFormat);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected format for sum aggregate", K(ret), K(*this), K(in_fmt));
      }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "accumulate number failed", K(ret));
      } else if (OB_LIKELY(!all_skip)) {
        NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
        not_nulls.set(agg_col_id);
        number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(agg_cell);
        cnum->desc_ = result.d_;
        MEMCPY(&(cnum->digits_[0]), result.get_digits(), result.d_.len_ * sizeof(uint32_t));
      }
      SQL_LOG(DEBUG, "number result", K(result), K(all_skip), K(ret));
    } else {
      int64_t mock_calc_info = 0;
      switch(in_fmt) {
      case common::VEC_UNIFORM: {
        ADD_OR_SUB_NMB(ObUniformFormat<false>);
        break;
      }
      case common::VEC_DISCRETE: {
        ADD_OR_SUB_NMB(ObDiscreteFormat);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ADD_OR_SUB_NMB(ObUniformFormat<true>);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid input format", K(ret), K(in_fmt));
      }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "add or sub rows failed", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *aggr_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, agg_col_id, aggr_cell, tmp_res, calc_info);
    SQL_LOG(DEBUG, "add_row do nothing");
    return OB_SUCCESS;
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
    SQL_LOG(DEBUG, "add_nullable_row do nothing");
    return OB_SUCCESS;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ObIVector *output_vec = agg_expr.get_vector(agg_ctx.eval_ctx_);
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      static_cast<ColumnFmt *>(output_vec)->set_number(output_idx, *reinterpret_cast<const number::ObCompactNumber *>(agg_cell));
    } else if (is_ora_count_sum_){
      number::ObNumber zero;
      zero.set_zero();
      static_cast<ColumnFmt *>(output_vec)->set_payload(output_idx, &zero, sizeof(ObNumberDesc));
    } else {
      static_cast<ColumnFmt *>(output_vec)->set_null(output_idx);
    }
    return ret;
  }

  template <typename ColumnFmt>
  int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(tmp_res, calc_info);
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      const number::ObCompactNumber *param_cnum =
        reinterpret_cast<const number::ObCompactNumber *>(columns.get_payload(row_num));
      number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(agg_cell);
      number::ObNumber param1(*param_cnum);
      number::ObNumber param2(*reinterpret_cast<number::ObCompactNumber *>(agg_cell));
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (is_trans) {
        if (OB_FAIL(param2.add_v3(param1, res_nmb, tmp_alloc))) {
          SQL_LOG(WARN, "add number failed", K(ret));
        }
      } else if (OB_FAIL(param2.sub_v3(param1, res_nmb, tmp_alloc))){
        SQL_LOG(WARN, "sub number failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        res_cnum->desc_ = res_nmb.d_;
        MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
      }
    } else if (is_trans){
      agg_ctx.removal_info_.null_cnt_++;
    } else {
      agg_ctx.removal_info_.null_cnt_--;
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "sum_nmb", K_(is_ora_count_sum));
private:
template<typename ColumnFmt, bool has_null>
struct input_wrapper
{
  static const constexpr bool _param_maybe_null = has_null;
  input_wrapper(): input_vec_(nullptr), d_() {}
  input_wrapper(ObIVector *input_vec): input_vec_(input_vec), d_() {}
  OB_INLINE ObDatum *at(const int64_t idx) const
  {
    const char *payload = nullptr;
    d_.reset();
    if (has_null && static_cast<ColumnFmt *>(input_vec_)->is_null(idx)) {
      d_.set_null();
    } else {
      payload = static_cast<ColumnFmt *>(input_vec_)->get_payload(idx);
      d_.set_number_shallow(*reinterpret_cast<const number::ObCompactNumber *>(payload));
    }
    return &d_;
  }
  ObIVector *input_vec_;
  mutable ObDatum d_;
};

struct rowsel_wrapper
{
  rowsel_wrapper(const RowSelector &row_sel): row_sel_(row_sel) {}
  OB_INLINE uint16_t begin() const
  {
    return 0;
  }
  OB_INLINE uint16_t end() const
  {
    return row_sel_.size();
  }

  OB_INLINE uint16_t get_batch_index(const uint16_t &idx) const
  {
    return row_sel_.index(idx);
  }
  OB_INLINE void next(uint16_t &idx) const
  {
    idx += 1;
  }
  const RowSelector &row_sel_;
};

struct bound_wrapper
{
  bound_wrapper(const sql::EvalBound &bound, const ObBitVector &skip) : bound_(bound), skip_(skip)
  {}

  OB_INLINE uint16_t begin() const
  {
    uint16_t idx = bound_.start();
    if (OB_LIKELY(bound_.get_all_rows_active())) {
      return idx;
    }
    while (idx < bound_.end() && skip_.at(idx)) {
      idx++;
    }
    return idx;
  }
  OB_INLINE uint16_t end() const
  {
    return bound_.end();
  }

  OB_INLINE uint16_t get_batch_index(const uint16_t &idx) const
  {
    return idx;
  }
  OB_INLINE void next(uint16_t &idx) const
  {
    if (OB_LIKELY(bound_.get_all_rows_active())) {
      idx += 1;
    } else {
      do {
        idx++;
      } while(idx < bound_.end() && skip_.at(idx));
    }
  }
  const sql::EvalBound &bound_;
  const ObBitVector &skip_;
};

private:
  bool is_ora_count_sum_;
};
} // end aggregate
} // end share
} // end oceanbase
#undef ACCUMULATE_NMB
#undef ADD_OR_SUB_NMB
#endif // OCEANBASE_SHARE_AGGREGATE_SUM_NMB_H_
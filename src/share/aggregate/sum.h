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

#ifndef OCEANBASE_SHARE_AGGREGATE_SUM_H_
#define OCEANBASE_SHARE_AGGREGATE_SUM_H_

#include "share/aggregate/iaggregate.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

#include <type_traits>
namespace oceanbase
{
namespace share
{
namespace aggregate
{

struct SumCalcInfo
{
  SumCalcInfo(): flags_(0) {}
  SumCalcInfo(int32_t agg_cell_len, int16_t scale) :
    agg_cell_len_(agg_cell_len), scale_(scale), reserved_(0)
  {}
  SumCalcInfo(int64_t flags): flags_(flags) {}
  operator int64_t() const { return flags_; }
  union {
    struct {
      int32_t agg_cell_len_;
      int16_t scale_;
      int16_t reserved_;
    };
    int64_t flags_;
  };
};

static_assert(sizeof(SumCalcInfo) <= sizeof(int64_t), "");
// sum aggregate rules:
// int/uint -> number
// decint->decint'
// number->number
// float->float
// double->double

// param & result are same types
template<VecValueTypeClass in_tc, VecValueTypeClass out_tc>
class SumAggregate final: public BatchAggregateWrapper<SumAggregate<in_tc, out_tc>>
{
  using ResultType = AggCalcType<out_tc>;
  using ParamType = AggCalcType<in_tc>;
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  SumAggregate() {}

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
              const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    int ret = OB_SUCCESS;

    const char* param_payload = nullptr;
    int32_t param_len = 0;
    columns.get_payload(row_num, param_payload, param_len);
    const ParamType *lparam = reinterpret_cast<const ParamType *>(param_payload);
    const ResultType &rparam = *reinterpret_cast<const ResultType *>(agg_cell);
    SQL_LOG(DEBUG, "sum add row", K(agg_col_id));
    if ((is_decint_vec(in_tc) && is_decint_vec(out_tc)) // sum(int64/int32) -> int128
        || (in_tc == VEC_TC_INTEGER && out_tc == VEC_TC_INTEGER)) { // count_sum
      ret = add_values(*lparam, rparam, agg_cell, sizeof(ResultType));
    } else {
      SumCalcInfo &sum_calc_info = reinterpret_cast<SumCalcInfo &>(calc_info);
      if (OB_FAIL(add_to_result(*lparam, rparam, sum_calc_info.scale_, agg_cell,
                                sum_calc_info.agg_cell_len_))) {
        SQL_LOG(WARN, "add_to_result failed", K(ret));
      }
    }
    return ret;
  }

  template<typename ColumnFmt>
  OB_INLINE int sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
              const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    const char* param_payload = nullptr;
    int32_t param_len = 0;
    columns.get_payload(row_num, param_payload, param_len);
    const ParamType *lparam = reinterpret_cast<const ParamType *>(param_payload);
    const ResultType &rparam = *reinterpret_cast<const ResultType *>(agg_cell);
    SQL_LOG(DEBUG, "sum sub row", K(agg_col_id));
    ret = sub_values(rparam, *lparam, agg_cell, sizeof(ResultType));
    if (OB_FAIL(ret)) {
      SQL_LOG(WARN, "sub value failed", K(ret));
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      SQL_LOG(DEBUG, "add null row", K(ret), K(row_num));
    } else if (OB_FAIL(
                 add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      if (is_trans) {
        if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
          SQL_LOG(WARN, "add row failed", K(ret));
        }
      } else if (OB_FAIL(
                   sub_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
        SQL_LOG(WARN, "sub row failed", K(ret));
      }
    } else {
      if (is_trans) {
        agg_ctx.removal_info_.null_cnt_++;
      } else {
        agg_ctx.removal_info_.null_cnt_--;
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      CellWriter<ResultType>::set(agg_cell, agg_cell_len, res_vec, output_idx, nullptr);
    } else if (agg_ctx.locate_aggr_info(agg_col_id).get_expr_type() == T_FUN_COUNT_SUM) {
      if (VEC_TC_INTEGER == out_tc) {
        int64_t res = 0;
        res_vec->set_payload(output_idx, &res, sizeof(int64_t));
      } else if (VEC_TC_NUMBER == out_tc) {
        number::ObNumber zero;
        zero.set_zero();
        res_vec->set_payload(output_idx, &zero, sizeof(ObNumberDesc));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected output result type", K(ret), K(out_tc));
      }
    } else {
      res_vec->set_null(output_idx);
    }
    if (out_tc == VEC_TC_DEC_INT512 && OB_SUCC(ret) && lib::is_mysql_mode()) {
      // check mysql decimal int overflow
      const int512_t &res = *reinterpret_cast<const int512_t *>(agg_cell);
      if (res <= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MIN
          || res >= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX) {
        int ret = OB_ERR_TRUNCATED_WRONG_VALUE;
        ObString decimal_type_str("DECIMAL");
        char buf[MAX_PRECISION_DECIMAL_INT_512];
        int64_t pos = 0;
        wide::to_string(wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX_AVAILABLE, buf,
                        MAX_PRECISION_DECIMAL_INT_512, pos);
        LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, decimal_type_str.length(),
                      decimal_type_str.ptr(), static_cast<int32_t>(pos), buf);
        SQL_LOG(WARN, "decimal int out of range", K(ret));
        // overflow, set datum to max available decimal int
        const ObDecimalInt *max_available_val = reinterpret_cast<const ObDecimalInt *>(
          &(wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX_AVAILABLE));
        res_vec->set_payload_shallow(output_idx, max_available_val, sizeof(int512_t));
        ret = OB_SUCCESS; // reset ret to SUCCESS, just log user warnings
      }
    }
    return ret;
  }
  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                     char *agg_cell) override
  {
    UNUSED(agg_cell);
    ObScale scale = agg_ctx.get_first_param_scale(agg_col_idx);
    int32_t agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, nullptr /*not used*/);
    return static_cast<int64_t>(SumCalcInfo(agg_cell_len, scale));
  }

  OB_INLINE int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                  const bool is_null, const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    UNUSEDx(data_len, batch_idx, batch_size);
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_null)) {
      const ParamType *lparam = reinterpret_cast<const ParamType *>(data);
      const ResultType &rparam = *reinterpret_cast<const ResultType *>(agg_cell);
      if ((is_decint_vec(in_tc) && is_decint_vec(out_tc))             // sum(int64/int32) -> int128
          || (in_tc == VEC_TC_INTEGER && out_tc == VEC_TC_INTEGER)) { // count_sum
        ret = add_values(*lparam, rparam, agg_cell, sizeof(ResultType));
      } else {
        SumCalcInfo calc_info = get_batch_calc_info(agg_ctx, agg_col_idx, agg_cell);

        if (OB_FAIL(add_to_result(*lparam, rparam, calc_info.scale_, agg_cell,
                                  calc_info.agg_cell_len_))) {
          SQL_LOG(WARN, "add_to_result failed", K(ret), K(*this), K(*lparam), K(rparam), K(batch_idx), KP(data));
        }
      }
      if (OB_SUCC(ret)) {
        NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
        not_nulls.set(agg_col_idx);
      }
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
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    const NotNullBitVector &curr_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    if (curr_not_nulls.at(agg_col_idx) && rollup_not_nulls.at(agg_col_idx)) {
      const ResultType *curr_param = reinterpret_cast<const ResultType *>(curr_agg_cell);
      const ResultType &rollup_param = *reinterpret_cast<const ResultType *>(rollup_agg_cell);
      if ((is_decint_vec(in_tc) && is_decint_vec(out_tc))             // sum(int64/int32) -> int128
          || (in_tc == VEC_TC_INTEGER && out_tc == VEC_TC_INTEGER)) { // count_sum
        ret = add_values(*curr_param, rollup_param, rollup_agg_cell, sizeof(ResultType));
      } else {
        SumCalcInfo calc_info = get_batch_calc_info(agg_ctx, agg_col_idx, rollup_agg_cell);

        if (OB_FAIL(add_to_result(*curr_param, rollup_param, calc_info.scale_, rollup_agg_cell,
                                  calc_info.agg_cell_len_))) {
          SQL_LOG(WARN, "add_to_result failed", K(ret), K(*this), K(*curr_param), K(rollup_param));
        }
      }
    } else if (curr_not_nulls.at(agg_col_idx)) {
      int32_t curr_agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, group_row);
      agg_ctx.set_agg_cell(curr_agg_cell, curr_agg_cell_len, agg_col_idx, rollup_agg_cell);
      rollup_not_nulls.set(agg_col_idx);

      const ResultType &curr_param = *reinterpret_cast<const ResultType *>(curr_agg_cell);
      const ResultType &rollup_param = *reinterpret_cast<const ResultType *>(rollup_agg_cell);
    } else {
      // do nothing
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "sum", K(in_tc), K(out_tc));
private:
  int add_to_result(const ParamType &lparam, const ResultType &rparam, const ObScale scale,
                    char *res_buf, const int32_t res_len)
  {
    int ret = OB_SUCCESS;
    ret = add_overflow(lparam, rparam, res_buf, res_len);
    if (OB_FAIL(ret)) {
      if (ret == OB_OPERATE_OVERFLOW && out_tc == VEC_TC_FLOAT) {
        if (!lib::is_oracle_mode()) {
          char buf[OB_MAX_TWO_OPERATOR_EXPR_LENGTH] = {0};
          int64_t buf_len = OB_MAX_TWO_OPERATOR_EXPR_LENGTH;
          int64_t pos = 0;
          BUF_PRINTF("'(");
          BUF_PRINTO(lparam);
          BUF_PRINTF(" + ");
          BUF_PRINTO(rparam);
          BUF_PRINTF(")'");
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, (in_tc == VEC_TC_FLOAT ? "BINARY_FLOAT" : "DOUBLE"),
                         buf);
          SQL_LOG(WARN, "do_overflow failed", K(lparam), K(rparam), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    return ret;
  }
};

template <VecValueTypeClass in_tc, VecValueTypeClass out_tc, typename TmpStore>
class SumAggregateWithTempStore final
  : public BatchAggregateWrapper<SumAggregateWithTempStore<in_tc, out_tc, TmpStore>>
{
  using ParamType = AggCalcType<in_tc>;
  using ResultType = AggCalcType<out_tc>;
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  SumAggregateWithTempStore() {}

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  int collect_tmp_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    TmpStore &tmp_res = *reinterpret_cast<TmpStore *>(get_tmp_res(agg_ctx, agg_col_id, agg_cell));
    if (OB_LIKELY(tmp_res != 0)) {
      if (OB_LIKELY(!agg_ctx.removal_info_.enable_removal_opt_)
          || !agg_ctx.removal_info_.is_inverse_agg_) {
        ResultType &res = *reinterpret_cast<ResultType *>(agg_cell);
        const int32_t agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_cell);
        ObScale scale = agg_ctx.get_first_param_scale(agg_col_id);
        if (out_tc == VEC_TC_NUMBER) {
          if (OB_FAIL(add_value_to_nmb(tmp_res, scale, agg_cell))) {
            SQL_LOG(WARN, "add value to nmb failed", K(ret));
          }
        } else {
          ResultType *res_val = reinterpret_cast<ResultType *>(agg_cell);
          ret = add_values(tmp_res, *res_val, agg_cell, agg_ctx.get_cell_len(agg_col_id, agg_cell));
        }
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "do op failed", K(ret));
        } else {
          tmp_res = 0;
        }
      } else {
        if (out_tc == VEC_TC_NUMBER) {
          ObScale scale = agg_ctx.get_first_param_scale(agg_col_id);
          number::ObNumber param_nmb, res_nmb;
          number::ObNumber cur_nmb(*reinterpret_cast<number::ObCompactNumber *>(agg_cell));
          number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(agg_cell);
          ObNumStackAllocator<3> tmp_alloc;
          if (OB_FAIL(to_nmb(tmp_res, scale, tmp_alloc, param_nmb))) {
            SQL_LOG(WARN, "to number failed", K(ret));
          } else if (OB_FAIL(cur_nmb.sub(param_nmb, res_nmb, tmp_alloc))) {
            SQL_LOG(WARN, "number::sub failed", K(ret));
          } else {
            res_cnum->desc_ = res_nmb.d_;
            MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), sizeof(uint32_t) * res_nmb.d_.len_);
            tmp_res = 0;
          }
        } else {
          constexpr unsigned res_bits =
            (out_tc == VEC_TC_NUMBER ? 128 : sizeof(ResultType) * CHAR_BIT);
          using res_int_type = wide::ObWideInteger<res_bits>;
          res_int_type &res_val = *reinterpret_cast<res_int_type *>(agg_cell);
          // overflow is impossible
          sub_values(res_val, tmp_res, agg_cell, sizeof(res_int_type));
          tmp_res = 0;
        }
      }
    }
    return ret;
  }

  OB_INLINE int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                  const bool is_null, const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    if (!is_null) {
      const ParamType *row_param = reinterpret_cast<const ParamType *>(data);
      TmpStore *tmp_res = reinterpret_cast<TmpStore *>(get_tmp_res(agg_ctx, agg_col_idx, agg_cell));
      TmpStore copied_tmp_res = *tmp_res;
      SQL_LOG(DEBUG, "sum with tmp::add_one_row ", K(*row_param), K(*tmp_res), K(batch_idx));
      ret = add_overflow(*row_param, copied_tmp_res, reinterpret_cast<char *>(tmp_res),
                         sizeof(TmpStore));
      if (OB_FAIL(ret)) {
        if (OB_LIKELY(OB_OPERATE_OVERFLOW == ret)) {
          ret = OB_SUCCESS;
          if (out_tc == VEC_TC_NUMBER) {
            ObScale scale = agg_ctx.get_first_param_scale(agg_col_idx);
            if (OB_FAIL(add_value_to_nmb(copied_tmp_res, scale, agg_cell))) {
              SQL_LOG(WARN, "add value to nmb failed", K(ret));
            } else if (OB_FAIL(add_value_to_nmb(*row_param, scale, agg_cell))) {
              SQL_LOG(WARN, "add value to nmb failed", K(ret));
            } else {
              *tmp_res = 0;
            }
          } else {
            ResultType &res_val = *reinterpret_cast<ResultType *>(agg_cell);
            if(OB_FAIL(add_values(*row_param, res_val, agg_cell, sizeof(ResultType)))) {
            } else if (OB_FAIL(add_values(*tmp_res, res_val, agg_cell, sizeof(ResultType)))) {
            } else {
              *tmp_res = 0;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "do addition failed", K(ret));
      } else {
        NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
        not_nulls.set(agg_col_idx);
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int64_t row_num,
              const int32_t agg_col_id, char *agg_cell, void *tmp_res_ptr, int64_t &calc_info)
  {
    OB_ASSERT(tmp_res_ptr != NULL);
    int ret = OB_SUCCESS;
    const char *param_payload = nullptr;
    int32_t param_len = 0;
    columns.get_payload(row_num, param_payload, param_len);
    const ParamType *row_param = reinterpret_cast<const ParamType *>(param_payload);
    // add_overflow may overwrite `tmp_res`, deep copy is needed
    TmpStore &tmp_res = *reinterpret_cast<TmpStore *>(tmp_res_ptr);
    TmpStore copied_tmp_res = tmp_res;

    SQL_LOG(DEBUG, "sum add row", K(row_param), K(row_num), K(tmp_res));
    ret = add_overflow(*row_param, copied_tmp_res, reinterpret_cast<char *>(&tmp_res),
                       sizeof(TmpStore));
    if (OB_FAIL(ret)) {
      if (OB_LIKELY(ret == OB_OPERATE_OVERFLOW)) {
        ret = OB_SUCCESS;
        if (out_tc == VEC_TC_NUMBER) {
          ObScale in_scale = agg_ctx.get_first_param_scale(agg_col_id);
          if (OB_FAIL(add_value_to_nmb(copied_tmp_res, in_scale, agg_cell))) {
            SQL_LOG(WARN, "add_value_to_nmb failed", K(ret));
          } else if (OB_FAIL(add_value_to_nmb(*row_param, in_scale, agg_cell))) {
            SQL_LOG(WARN, "add_value_to_nmb failed", K(ret));
          } else {
            tmp_res = 0;
          }
        } else {
          ResultType &res_val = *reinterpret_cast<ResultType *>(agg_cell);
          if (OB_FAIL(add_values(copied_tmp_res, res_val, agg_cell, sizeof(ResultType)))) {
            SQL_LOG(WARN, "add values failed", K(ret));
          } else if (OB_FAIL(add_values(*row_param, res_val, agg_cell, sizeof(ResultType)))) {
            SQL_LOG(WARN, "add values failed", K(ret));
          } else {
            tmp_res = 0;
          }
        }
      } else {
        SQL_LOG(WARN, "add overflow failed", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int64_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res_ptr,
                       int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      SQL_LOG(DEBUG, "sum add null", K(ret));
      // do nothing
    } else if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res_ptr,
                               calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  template<typename ColumnFmt>
  OB_INLINE int sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    // for substraction with tmp result, we added all substractions in tmp results
    // and do substraction when overflow or tmp result collecting happended.
    const char *payload = columns.get_payload(row_num);
    const ParamType &param = *reinterpret_cast<const ParamType *>(payload);
    TmpStore &tmp_val = *reinterpret_cast<TmpStore *>(tmp_res);
    TmpStore copied_tmp_val = tmp_val;
    ret = add_overflow(param, copied_tmp_val, (char *)tmp_res, sizeof(TmpStore));
    if (OB_FAIL(ret)) {
      if (OB_LIKELY(ret == OB_OPERATE_OVERFLOW)) {
        ret = OB_SUCCESS;
        if (out_tc == VEC_TC_NUMBER) {
          ObNumStackAllocator<4> tmp_alloc;
          number::ObNumber sub1, sub2, tmp_sum, res_nmb;
          number::ObNumber cur_nmb(*reinterpret_cast<number::ObCompactNumber *>(agg_cell));
          ObScale scale = agg_ctx.get_first_param_scale(agg_col_id);
          if (OB_FAIL(to_nmb(tmp_val, scale, tmp_alloc, sub1))) {
            SQL_LOG(WARN, "to number failed", K(ret));
          } else if (OB_FAIL(to_nmb(param, scale, tmp_alloc, sub2))) {
            SQL_LOG(WARN, "to number failed", K(ret));
          } else if (OB_FAIL(sub1.add(sub2, tmp_sum, tmp_alloc))) {
            SQL_LOG(WARN, "number::add failed", K(ret));
          } else if (OB_FAIL(cur_nmb.sub(tmp_sum, res_nmb, tmp_alloc))) {
            SQL_LOG(WARN, "number::sub failed", K(ret));
          } else {
            number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(agg_cell);
            res_cnum->desc_ = res_nmb.d_;
            MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
            tmp_val = 0;
          }
        } else {
          constexpr unsigned res_bits =
            (out_tc == VEC_TC_NUMBER ? 128 : sizeof(ResultType) * CHAR_BIT);
          using res_int_type = wide::ObWideInteger<res_bits>;
          res_int_type &res_val = *reinterpret_cast<res_int_type *>(agg_cell);
          if (OB_FAIL(sub_values(res_val, tmp_val, agg_cell, sizeof(ResultType)))) {
            SQL_LOG(WARN, "sub values failed", K(ret));
          } else if (OB_FAIL(sub_values(res_val, param, agg_cell, sizeof(ResultType)))) {
            SQL_LOG(WARN, "sub values failed", K(ret));
          } else {
            tmp_val = 0;
          }
        }
      } else {
        SQL_LOG(WARN, "add overflow", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      if (is_trans) {
        ret = add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "add row failed", K(ret));
        }
      } else {
        if (OB_FAIL(sub_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
          SQL_LOG(WARN, "sub row failed", K(ret));
        }
      }
    } else {
      if (is_trans) {
        agg_ctx.removal_info_.null_cnt_++;
      } else {
        agg_ctx.removal_info_.null_cnt_--;
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell, const int32_t agg_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    int64_t output_idx = ctx.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      TmpStore *tmp_res = reinterpret_cast<TmpStore *>(
          get_tmp_res(agg_ctx, agg_col_id, const_cast<char *>(agg_cell)));
      if (VEC_TC_NUMBER == out_tc) {
        ObScale scale = agg_ctx.get_first_param_scale(agg_col_id);
        if (OB_FAIL(add_value_to_nmb(*tmp_res, scale, const_cast<char *>(agg_cell)))) {
          SQL_LOG(WARN, "add value to nmb failed", K(ret));
        } else {
          CellWriter<ResultType>::set(agg_cell, agg_len, res_vec, output_idx, nullptr);
        }
      } else {
        ResultType *res_val = reinterpret_cast<ResultType *>(const_cast<char *>(agg_cell));
        ret = add_values(*tmp_res, *res_val, const_cast<char *>(agg_cell), agg_len);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "add values failed", K(ret));
        } else {
          CellWriter<ResultType>::set(agg_cell, agg_len, res_vec, output_idx, nullptr);
        }
      }
    } else if (agg_ctx.locate_aggr_info(agg_col_id).get_expr_type() == T_FUN_COUNT_SUM) {
      if (lib::is_oracle_mode() || out_tc == VEC_TC_NUMBER) {
        number::ObNumber res_nmb;
        res_nmb.set_zero();
        res_vec->set_number(output_idx, res_nmb);
      } else if (wide::IsIntegral<ResultType>::value) { // decimal int is used
        set_decint_zero<ColumnFmt, sizeof(ResultType)>(res_vec, output_idx);
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid result type of count sum", K(ret), K(out_tc));
      }
    } else {
      static_cast<ColumnFmt *>(agg_expr.get_vector(ctx))->set_null(output_idx);
    }
    if (out_tc == VEC_TC_DEC_INT512 && OB_SUCC(ret) && lib::is_mysql_mode()) {
      // check mysql decimal int overflow
      const int512_t &res = *reinterpret_cast<const int512_t *>(agg_cell);
      if (res <= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MIN
          || res >= wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX) {
        int ret = OB_ERR_TRUNCATED_WRONG_VALUE;
        ObString decimal_type_str("DECIMAL");
        char buf[MAX_PRECISION_DECIMAL_INT_512];
        int64_t pos = 0;
        wide::to_string(wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX_AVAILABLE, buf,
                        MAX_PRECISION_DECIMAL_INT_512, pos);
        LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, decimal_type_str.length(),
                      decimal_type_str.ptr(), static_cast<int32_t>(pos), buf);
        SQL_LOG(WARN, "decimal int out of range", K(ret));
        // overflow, set datum to max available decimal int
        const ObDecimalInt *max_available_val = reinterpret_cast<const ObDecimalInt *>(
          &(wide::ObDecimalIntConstValue::MYSQL_DEC_INT_MAX_AVAILABLE));
        res_vec->set_payload_shallow(output_idx, max_available_val, sizeof(int512_t));
        ret = OB_SUCCESS; // reset ret to SUCCESS, just log user warnings
      }
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
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    const NotNullBitVector &curr_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls =
      agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    if (curr_not_nulls.at(agg_col_idx)) {
      int32_t rollup_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, rollup_row);
      ResultType *cur_cell = reinterpret_cast<ResultType *>(curr_agg_cell);
      ResultType *rollup_cell = reinterpret_cast<ResultType *>(rollup_agg_cell);
      ret =
        add_values(*cur_cell, *rollup_cell, reinterpret_cast<char *>(rollup_cell), rollup_cell_len);
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "do addition failed", K(ret));
      } else {
        rollup_not_nulls.set(agg_col_idx);
      }
    }
    return ret;
  }

  inline void *get_tmp_res(RuntimeContext &agg_ctx, int32_t agg_col_id, char *agg_cell) override
  {
    int32_t cell_len = agg_ctx.get_cell_len(agg_col_id, agg_cell);
    return (void *)(agg_cell + cell_len);
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_id,
                                     char *agg_cell) override
  {
    UNUSED(agg_cell);
    ObScale scale = agg_ctx.get_first_param_scale(agg_col_id);
    int32_t agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, nullptr /*not used*/);
    return static_cast<int64_t>(SumCalcInfo(agg_cell_len, scale));
  }
  TO_STRING_KV("aggregate", "sum_with_tmp_store", K(in_tc), K(out_tc));
private:
  template<typename ColumnFmt, int int_len>
  void set_decint_zero(ColumnFmt *res_vec, const int32_t output_idx)
  {
    switch (int_len) {
    case sizeof(int32_t): {
      int32_t v = 0;
      res_vec->set_payload(output_idx, &v, sizeof(int32_t));
      break;
    }
    case sizeof(int64_t): {
      int64_t v = 0;
      res_vec->set_payload(output_idx, &v, sizeof(int64_t));
      break;
    }
    case sizeof(int128_t): {
      int128_t v = 0;
      res_vec->set_payload(output_idx, &v, sizeof(int128_t));
      break;
    }
    case sizeof(int256_t): {
      int256_t v = 0;
      res_vec->set_payload(output_idx, &v, sizeof(int256_t));
      break;
    }
    case sizeof(int512_t): {
      int512_t v = 0;
      res_vec->set_payload(output_idx, &v, sizeof(int512_t));
      break;
    }
    default: {
      ob_assert(false);
    }
    }
  }

  template<typename T>
  int to_nmb(const T &v, ObScale in_scale, ObIAllocator &alloc, number::ObNumber &res_nmb)
  {
    int ret = OB_SUCCESS;
    if (sizeof(T) > sizeof(int64_t)) {
      if (OB_FAIL(wide::to_number(v, in_scale, alloc, res_nmb))) {
        SQL_LOG(WARN, "wide::to_number failed", K(ret));
      }
    } else if (sizeof(T) <= sizeof(int32_t) || std::is_same<T, int64_t>::value) {
      int64_t tmp_v = v;
      if (OB_FAIL(wide::to_number(tmp_v, in_scale, alloc, res_nmb))) {
        SQL_LOG(WARN, "wide::to_number failed", K(ret));
      }
    } else if (std::is_same<T, uint64_t>::value) {
      if (OB_FAIL(wide::to_number(static_cast<uint64_t>(v), in_scale, alloc, res_nmb))) {
        SQL_LOG(WARN, "wide::to_number failed", K(ret));
      }
    }
    return ret;
  }
  template<typename T>
  int add_value_to_nmb(const T &v, const ObScale in_scale, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    ObNumStackAllocator<2> tmp_alloc;
    number::ObNumber res_nmb, param_nmb;
    number::ObNumber tmp(*reinterpret_cast<number::ObCompactNumber *>(agg_cell));
    if (OB_FAIL(to_nmb(v, in_scale, tmp_alloc, param_nmb))) {
      SQL_LOG(WARN, "to_nmb failed", K(ret));
    } else if (OB_FAIL(tmp.add_v3(param_nmb, res_nmb, tmp_alloc))) {
      SQL_LOG(WARN, "ObNumber::add_v3 failed", K(ret));
    } else {
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(agg_cell);
      cnum->desc_ = res_nmb.d_;
      MEMCPY(&cnum->digits_[0], res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
    }
    return ret;
  }
};

class SumVectorAggregate final: public BatchAggregateWrapper<SumVectorAggregate>
{
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_COLLECTION;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_COLLECTION;

public:
  SumVectorAggregate() {}
  
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_idx);
    ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
    VectorFormat fmt = aggr_info.param_exprs_.at(0)->get_format(eval_ctx);
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    common::ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "SumVector", common::ObCtxIds::WORK_AREA));
    if (OB_LIKELY(!is_null)) {
      // add single row is full lob no matter what VectorFormat is
      ObString array_data(data_len, data);
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                            ObLongTextType,
                                                            CS_TYPE_BINARY,
                                                            true,
                                                            array_data))) {
        SQL_LOG(WARN, "fail to get real data.", K(ret), K(array_data));
      } else if (not_nulls.at(agg_col_idx)) {
        int32_t agg_cell_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
        const char *agg_cell_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(agg_cell));
        ObString agg_array_data(agg_cell_len, agg_cell_data);
        if (array_data.length() != agg_array_data.length()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpect length", K(ret), K(agg_array_data), K(array_data));
        } else {
          // update in-place
          int64_t length = array_data.length() / sizeof(float);
          float *float_data = reinterpret_cast<float *>(array_data.ptr());
          float *float_res = reinterpret_cast<float *>(agg_array_data.ptr());
          for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
            float_res[i] += float_data[i];
            if (isinff(float_res[i]) != 0) {
              ret = OB_OPERATE_OVERFLOW;
              SQL_LOG(WARN, "value overflow", K(ret), K(i), K(float_data[i]), K(float_res[i]));
            }
          }
        }
      } else {
        char *res_ptr = nullptr;
        if (OB_ISNULL(res_ptr = (char*)agg_ctx.allocator_.alloc(array_data.length()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "failed to allocator memory", K(ret));
        } else {
          MEMCPY(res_ptr, array_data.ptr(), array_data.length());
          *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(res_ptr);
          *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = array_data.length();
          not_nulls.set(agg_col_idx);

        }
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int inner_add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                            const int32_t agg_col_id, char *aggr_cell, bool is_add)
  {
    int ret = OB_SUCCESS;
    VectorFormat fmt = columns.get_format();
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, aggr_cell);
    const char* param_payload = nullptr;
    int32_t param_len = 0;
    columns.get_payload(row_num, param_payload, param_len);
    common::ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "SumVector", common::ObCtxIds::WORK_AREA));
    ObString array_data(param_len, param_payload);
    if (!ObCollectionExprUtil::is_compact_fmt_cell(param_payload)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected data format", K(ret));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                                 ObLongTextType,
                                                                 CS_TYPE_BINARY,
                                                                 true,
                                                                 array_data))) {
      SQL_LOG(WARN, "fail to get real data.", K(ret), K(array_data));
    } else if (not_nulls.at(agg_col_id)) {
      int32_t agg_cell_len = *reinterpret_cast<int32_t *>(aggr_cell + sizeof(char *));
      const char *agg_cell_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(aggr_cell));
      ObString agg_array_data(agg_cell_len, agg_cell_data);
      if (array_data.length() != agg_array_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpect length", K(ret), K(agg_array_data), K(array_data));
      } else {
        // update in-place
        int64_t length = array_data.length() / sizeof(float);
        float *float_data = reinterpret_cast<float *>(array_data.ptr());
        float *float_res = reinterpret_cast<float *>(agg_array_data.ptr());
        for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
          is_add ? float_res[i] += float_data[i] : float_res[i] -= float_data[i];
          if (isinff(float_res[i]) != 0) {
            ret = OB_OPERATE_OVERFLOW;
            SQL_LOG(WARN, "value overflow", K(ret), K(i), K(float_data[i]), K(float_res[i]));
          }
        }
      }
    } else if (is_add) {
      char *res_ptr = nullptr;
      if (OB_ISNULL(res_ptr = (char*)agg_ctx.allocator_.alloc(array_data.length()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "failed to allocator memory", K(ret));
      } else {
        MEMCPY(res_ptr, array_data.ptr(), array_data.length());
        *reinterpret_cast<int64_t *>(aggr_cell) = reinterpret_cast<int64_t>(res_ptr);
        *reinterpret_cast<int32_t *>(aggr_cell + sizeof(char *)) = array_data.length();
        not_nulls.set(agg_col_id);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null agg_ecll", K(ret), K(is_add));
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *aggr_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    return inner_add_or_sub_row(agg_ctx, columns, row_num, agg_col_id, aggr_cell, true/*is_add*/);
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      SQL_LOG(DEBUG, "add null row", K(ret), K(row_num));
    } else if (OB_FAIL(
                 add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *aggr_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    return inner_add_or_sub_row(agg_ctx, columns, row_num, agg_col_id, aggr_cell, false/*is_add*/);
  }

  template <typename ColumnFmt>
  OB_INLINE int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      if (is_trans) {
        if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
          SQL_LOG(WARN, "add row failed", K(ret));
        }
      } else if (OB_FAIL(
                   sub_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
        SQL_LOG(WARN, "sub row failed", K(ret));
      }
    } else {
      if (is_trans) {
        agg_ctx.removal_info_.null_cnt_++;
      } else {
        agg_ctx.removal_info_.null_cnt_--;
      }
    }
    return ret;
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
    const char *agg_data = reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(agg_cell));
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      ObString res;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr, agg_cell_len, agg_ctx.allocator_, res, agg_data))) {
        SQL_LOG(WARN, "failed to set array res", K(ret));
      } else {
        static_cast<ColumnFmt *>(output_vec)->set_string(output_idx, res);
      }
    } else {
      static_cast<ColumnFmt *>(output_vec)->set_null(output_idx);
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_NOT_SUPPORTED;
    return ret;
  }

  TO_STRING_KV("aggregate", "sum_vector");
};

} // end aggregate
} // end share
} // end oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_SUM_H_

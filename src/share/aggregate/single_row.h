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
#ifndef OCEANBSE_SHARE_AGGREGATE_SINGLE_ROW_H_
#define OCEANBSE_SHARE_AGGREGATE_SINGLE_ROW_H_
#include "iaggregate.h"
#include "src/sql/engine/expr/ob_expr_estimate_ndv.h"

#include <type_traits>

namespace oceanbase
{
namespace share
{
namespace aggregate
{
int llc_add_value(const uint64_t value, char *llc_bitmap_buf, int64_t size);

template <ObExprOperatorType agg_func, VecValueTypeClass in_tc, VecValueTypeClass out_tc>
class SingleRowAggregate final : public BatchAggregateWrapper<SingleRowAggregate<agg_func, in_tc, out_tc>>
{
using ParamType = typename std::conditional<agg_func == T_FUN_COUNT, int64_t, AggCalcType<in_tc>>::type;
using ResultType = AggCalcType<out_tc>;
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  SingleRowAggregate(): is_first_row_(false) {}
  template <typename ResultFmt>
  inline int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                                  const int32_t agg_col_id, const char *agg_cell,
                                  const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    const NotNullBitVector &notnulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ResultFmt *res_vec = static_cast<ResultFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    if (notnulls.at(agg_col_id) && T_FUN_COUNT == agg_func) {
      if (lib::is_oracle_mode()) {
        static const uint32_t constexpr one_val[2] = {3221225473, 1};
        res_vec->set_number(output_idx, *reinterpret_cast<const number::ObCompactNumber *>(one_val));
      } else {
        res_vec->set_int(output_idx, 1);
      }
    } else if (notnulls.at(agg_col_id)) {
      if (agg_func != T_FUN_SUM || in_tc == out_tc) {
        const char *agg_data = agg_cell;
        if (helper::is_var_len_agg_cell(in_tc)) {
          agg_data = reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(agg_cell));
          char *res_buf = nullptr;
          if (OB_ISNULL(res_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_, agg_cell_len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_LOG(WARN, "allocate memory failed", K(ret));
          } else {
            CellWriter<ResultType>::set(agg_data, agg_cell_len, res_vec, output_idx, res_buf);
          }
        } else {
          CellWriter<ResultType>::set(agg_data, agg_cell_len, res_vec, output_idx, nullptr);
        }
      } else if (out_tc == VEC_TC_NUMBER) {
        ResultType *out_val = nullptr;
        int32_t out_len = 0;
        ObScale in_scale = agg_ctx.get_first_param_scale(agg_col_id);
        char local_buf[64] = {0};
        ObDataBuffer tmp_alloc(local_buf, 64);
        ret = Caster<ParamType, ResultType>::to_type(agg_cell, agg_cell_len, in_scale,
                                                                  tmp_alloc, out_val, out_len);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "cast value failed", K(ret));
        } else {
          CellWriter<ResultType>::set(reinterpret_cast<const char *>(out_val), out_len, res_vec,
                                      output_idx, nullptr);
        }
      } else {
        CellWriter<ResultType>::cp_and_set(*reinterpret_cast<const ParamType *>(agg_cell), res_vec,
                                           output_idx, nullptr);
      }
    } else if (agg_func == T_FUN_COUNT) {
      static const uint32_t constexpr zero_val[1] = {2147483648};
      if (lib::is_oracle_mode()) {
        // number::ObNumber tmp_nmb;
        // tmp_nmb.set_zero();
        res_vec->set_number(output_idx, *reinterpret_cast<const number::ObCompactNumber *>(zero_val));
      } else {
        res_vec->set_int(output_idx, 0);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    return ret;
  }
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    AggrRowPtr agg_row = agg_ctx.agg_rows_.at(batch_idx);
    NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    if (agg_func != T_FUN_COUNT) {
      if (OB_LIKELY(!is_null)) {
        char *cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, agg_row);
        if (helper::is_var_len_agg_cell(in_tc)) {
          *reinterpret_cast<int64_t *>(cell) = reinterpret_cast<int64_t>(data);
          *reinterpret_cast<int32_t *>(cell + sizeof(char *)) = data_len;
        } else {
          MEMCPY(cell, data, data_len);
        }
        notnulls.set(agg_col_idx);
      }
    } else if (!is_null) { // COUNT function, only need to set not null
      notnulls.set(agg_col_idx);
    }
    return ret;
  }
  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSEDx(agg_cell, tmp_res, calc_info);
    AggrRowPtr agg_row = agg_ctx.agg_rows_.at(row_num);
    NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    if (agg_func != T_FUN_COUNT) {
      char *cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
      const char *payload = nullptr;
      int32_t len = 0;
      columns.get_payload(row_num, payload, len);
      if (helper::is_var_len_agg_cell(in_tc)) {
        *reinterpret_cast<int64_t *>(cell) = reinterpret_cast<int64_t>(payload);
        *reinterpret_cast<int32_t *>(cell + sizeof(char *)) = len;
      } else {
        MEMCPY(cell, payload, len);
      }
    }
    notnulls.set(agg_col_id);
    return OB_SUCCESS;
  }
  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      // do nothing
    } else {
      ret = add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
    }
    return ret;
  }

  template <typename ColumnFmt>
  int add_param_batch(RuntimeContext &agg_ctx, const ObBitVector &skip, ObBitVector &pvt_skip,
                      const EvalBound &bound, const RowSelector &row_sel, const int32_t agg_col_id,
                      const int32_t param_id, ColumnFmt &param_vec, char *aggr_cell)
  {
    int ret = OB_SUCCESS;
    AggrRowPtr agg_row = nullptr;
    if (row_sel.is_empty()) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (pvt_skip.at(i)) { continue; }
        if (param_vec.is_null(i)) { pvt_skip.set(i); }
      }
    } else {
      for (int i = 0; i < row_sel.size(); i++) {
        if (param_vec.is_null(row_sel.index(i))) { pvt_skip.set(row_sel.index(i)); }
      }
    }
    if (param_id == agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() - 1) {
      // last param expr
      if (row_sel.is_empty()) {
        for (int i = bound.start(); i < bound.end(); i++) {
          if (skip.at(i)) { continue; }
          agg_row = agg_ctx.agg_rows_.at(i);
          NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
          if (!pvt_skip.at(i)) {
            notnulls.set(agg_col_id);
          }
        }
      } else {
        for (int i = 0; i < row_sel.size(); i++) {
          agg_row = agg_ctx.agg_rows_.at(row_sel.index(i));
          NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
          if (!pvt_skip.at(row_sel.index(i))) {
            notnulls.set(agg_col_id);
          }
        }
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
    const char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    int32_t curr_agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, group_row);
    const NotNullBitVector &curr_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    if (OB_LIKELY(curr_not_nulls.at(agg_col_idx))) {
      rollup_not_nulls.set(agg_col_idx);
      if (helper::is_var_len_agg_cell(in_tc)) {
        *reinterpret_cast<int64_t *>(rollup_agg_cell) =
          *reinterpret_cast<const int64_t *>(curr_agg_cell);
        *reinterpret_cast<int32_t *>(rollup_agg_cell + sizeof(char *)) = curr_agg_cell_len;
      } else {
        MEMCPY(rollup_agg_cell, curr_agg_cell, curr_agg_cell_len);
      }
    }
    return ret;
  }

  void set_first_row(bool is_first_row) { is_first_row_ = is_first_row; }
  TO_STRING_KV("aggregate", "single_row", K(in_tc), K(out_tc), K(is_first_row_), K(agg_func));
private:
  bool is_first_row_;
};

template<VecValueTypeClass in_tc>
class SingleRowApproxCntSynopsis final: public BatchAggregateWrapper<SingleRowApproxCntSynopsis<in_tc>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_STRING;
public:
  SingleRowApproxCntSynopsis() {}
  virtual int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    llc_num_buckets_ = ObAggrInfo::get_approx_cnt_llc_buck_num(aggr_info.expr_->extra_);
    if (OB_UNLIKELY(!ObExprEstimateNdv::llc_is_num_buckets_valid(llc_num_buckets_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid llc buckets number", K(ret), K(llc_num_buckets_), KP(aggr_info.expr_));
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    AggrRowPtr agg_row = agg_ctx.agg_rows_.at(row_num);
    NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    const char *payload = nullptr;
    int32_t len = 0;
    columns.get_payload(row_num, payload, len);
    char *cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
    uint64_t hash_val = 0;
    ObExprHashFuncType hash_func = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->basic_funcs_->murmur_hash_;
    if (OB_FAIL(inner_calc_hash(hash_func, payload, len, hash_val))) {
      SQL_LOG(WARN, "calculate hash value failed", K(ret));
    } else {
      *reinterpret_cast<uint64_t *>(cell) = hash_val;
      notnulls.set(agg_col_id);
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      // do nothing
    } else {
      ret = add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
    }
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    AggrRowPtr agg_row = agg_ctx.agg_rows_.at(batch_idx);
    NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
    if (OB_LIKELY(!is_null)) {
      char *cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, agg_row);
      ObExprHashFuncType hash_func = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0)->basic_funcs_->murmur_hash_;
      uint64_t hash_val = 0;
      if (OB_FAIL(inner_calc_hash(hash_func, data, data_len, hash_val))) {
        SQL_LOG(WARN, "calculate hash value failed", K(ret));
      } else {
        *reinterpret_cast<uint64_t *>(cell) = hash_val;
        notnulls.set(agg_col_idx);
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_param_batch(RuntimeContext &agg_ctx, const ObBitVector &skip, ObBitVector &pvt_skip,
                             const EvalBound &bound, const RowSelector &row_sel, const int32_t agg_col_id,
                             const int32_t param_id, ColumnFmt &param_vec, char *aggr_cell)
  {
    int ret = OB_SUCCESS;
    AggrRowPtr agg_row = nullptr;
    if (row_sel.is_empty()) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (pvt_skip.at(i)) { continue; }
        if (param_vec.is_null(i)) { pvt_skip.set(i); }
      }
    } else {
      for (int i = 0; i < row_sel.size(); i++) {
        if (param_vec.is_null(row_sel.index(i))) { pvt_skip.set(row_sel.index(i)); }
      }
    }
    if (param_id == agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() - 1) {
      // last param expr
      if (row_sel.is_empty()) {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i)) { continue; }
          agg_row = agg_ctx.agg_rows_.at(i);
          NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
          char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
          if (!pvt_skip.at(i)) {
            notnulls.set(agg_col_id);
            uint64_t hash_val = 0;
            if (OB_FAIL(inner_calc_params_hash(agg_ctx.aggr_infos_.at(agg_col_id),
                                               agg_ctx.eval_ctx_, i, hash_val))) {
              SQL_LOG(WARN, "calculate hash value failed", K(ret));
            } else {
              *reinterpret_cast<uint64_t *>(agg_cell) = hash_val;
            }
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          agg_row = agg_ctx.agg_rows_.at(row_sel.index(i));
          NotNullBitVector &notnulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
          char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
          if (!pvt_skip.at(row_sel.index(i))) {
            notnulls.set(agg_col_id);
            uint64_t hash_val = 0;
            if (OB_FAIL(inner_calc_params_hash(agg_ctx.aggr_infos_.at(agg_col_id),
                                               agg_ctx.eval_ctx_, row_sel.index(i), hash_val))) {
              SQL_LOG(WARN, "calculate hash value failed", K(ret));
            } else {
              *reinterpret_cast<uint64_t *>(agg_cell) = hash_val;
            }
          }
        }
      }
    }
    return ret;
  }

  template<typename ResultFmt>
  inline int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                                  const int32_t agg_col_id, const char *agg_cell,
                                  const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    const NotNullBitVector &notnulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ResultFmt *res_vec = static_cast<ResultFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    uint64_t hash_val = *reinterpret_cast<const uint64_t *>(agg_cell);
    char *hash_val_buf = nullptr;
    if (OB_ISNULL(hash_val_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_, sizeof(uint64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(*reinterpret_cast<uint64_t *>(hash_val_buf) = hash_val)) {
    } else if (FALSE_IT(res_vec->set_payload_shallow(output_idx, hash_val_buf, sizeof(uint64_t)))) {
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    return OB_NOT_IMPLEMENT;
  }

  TO_STRING_KV("aggregate", "single_row_approx_cnt_syn", K(in_tc), K(in_tc));

private:
  int inner_calc_hash(ObExprHashFuncType hash_func, const char *data, const int32_t data_len,
                      uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    ObDatum tmp_datum;
    tmp_datum.ptr_ = data;
    tmp_datum.pack_ = data_len;
    if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
      SQL_LOG(WARN, "calculate hash value failed", K(ret));
    }
    return ret;
  }

  int inner_calc_params_hash(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, const int32_t row_num, uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    const char *param_data = nullptr;
    int32_t param_len = 0;
    for (int j = 0; OB_SUCC(ret) && j < aggr_info.param_exprs_.count(); j++) {
      ObExpr *param_expr = aggr_info.param_exprs_.at(j);
      ObExprHashFuncType hash_func = param_expr->basic_funcs_->murmur_hash_;
      param_expr->get_vector(eval_ctx)->get_payload(row_num, param_data, param_len);
      if (OB_FAIL(inner_calc_hash(hash_func, param_data, param_len, hash_val))) {
        SQL_LOG(WARN, "calculate hash value failed", K(ret));
      }
    }
    return ret;
  }
private:
  int64_t llc_num_buckets_;
};

namespace helper
{
int init_single_row_aggregates(RuntimeContext &agg_ctx, ObIAllocator &allocator,
                               ObIArray<IAggregate *> &aggregates);
} // end helper
} // end aggregate
} // end share
} // end oceanbase

#endif // OCEANBSE_SHARE_AGGREGATE_SINGLE_ROW_H_
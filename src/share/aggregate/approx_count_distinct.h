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

#ifndef OCEANBASE_SHARE_AGGREGATE_APPROX_COUNT_DISTINCT_H
#define OCEANBASE_SHARE_AGGREGATE_APPROX_COUNT_DISTINCT_H

#include "share/aggregate/iaggregate.h"
#include "src/sql/engine/expr/ob_expr_estimate_ndv.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
template <ObExprOperatorType agg_func, VecValueTypeClass in_tc, VecValueTypeClass out_tc>
class ApproxCountDistinct final
  : public BatchAggregateWrapper<ApproxCountDistinct<agg_func, in_tc, out_tc>>
{
  static const constexpr int8_t LLC_BUCKET_BITS = 10;
  static const constexpr int64_t LLC_NUM_BUCKETS = (1 << LLC_BUCKET_BITS);
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  ApproxCountDistinct() {}
  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE) {
      char *llc_bitmap_buf = EXTRACT_MEM_ADDR(agg_cell);
      const char *payload = nullptr;
      int32_t len = 0;
      columns.get_payload(row_num, payload, len);
      if (OB_UNLIKELY(len != LLC_NUM_BUCKETS)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected length of input", K(ret), K(len));
      } else if (OB_ISNULL(llc_bitmap_buf)) {
        // not calculated before, copy from payload
        llc_bitmap_buf = (char *)agg_ctx.allocator_.alloc(LLC_NUM_BUCKETS);
        if (OB_ISNULL(llc_bitmap_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          MEMSET(llc_bitmap_buf, 0, LLC_NUM_BUCKETS);
          MEMCPY(llc_bitmap_buf, payload, LLC_NUM_BUCKETS);
          STORE_MEM_ADDR(llc_bitmap_buf, agg_cell);
          *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = LLC_NUM_BUCKETS;
        }
      } else {
        for (int i = 0; i < LLC_NUM_BUCKETS; i++) {
          llc_bitmap_buf[i] =
            std::max(static_cast<uint8_t>(llc_bitmap_buf[i]), static_cast<uint8_t>(payload[i]));
        }
      }
    } else {
      const char *payload = nullptr;
      int32_t len = 0;
      ObExprHashFuncType hash_func = reinterpret_cast<ObExprHashFuncType>(calc_info);
      ObDatum tmp_datum;
      uint64_t hash_val = 0;
      if (OB_ISNULL(tmp_res)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null tmp result", K(ret));
      } else {
        columns.get_payload(row_num, payload, len);
        tmp_datum.ptr_ = payload;
        tmp_datum.pack_ = len;
        if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
          SQL_LOG(WARN, "hash func failed", K(ret));
        } else if (OB_FAIL(
                     llc_add_value(hash_val, reinterpret_cast<char *>(tmp_res), LLC_NUM_BUCKETS))) {
          SQL_LOG(WARN, "llc add value failed", K(ret));
        }
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(columns.is_null(row_num))) {
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
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    const char *llc_bitmap_buf = nullptr;
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    if (agg_func != T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE) {
      llc_bitmap_buf = (const char *)get_tmp_res(agg_ctx, agg_col_id, const_cast<char *>(agg_cell));
    } else {
      llc_bitmap_buf = reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(agg_cell));
    }
    if (agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE) {
      if (OB_LIKELY(not_nulls.at(agg_col_id))) {
        char *res_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_, LLC_NUM_BUCKETS);
        if (OB_ISNULL(res_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          MEMCPY(res_buf, llc_bitmap_buf, LLC_NUM_BUCKETS);
          res_vec->set_payload_shallow(output_idx, res_buf, LLC_NUM_BUCKETS);
        }
      } else {
        res_vec->set_null(output_idx);
      }
    } else if (OB_ISNULL(llc_bitmap_buf)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN , "invalid null llc bitmap", K(ret));
    } else if (agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS) {
      char *res_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_, LLC_NUM_BUCKETS);
      if (OB_ISNULL(res_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMCPY(res_buf, llc_bitmap_buf, LLC_NUM_BUCKETS);
        res_vec->set_payload_shallow(output_idx, res_buf, LLC_NUM_BUCKETS);
      }
    } else if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      ObString llc_str(LLC_NUM_BUCKETS, llc_bitmap_buf);
      int64_t tmp_result = OB_INVALID_COUNT;
      ObExprEstimateNdv::llc_estimate_ndv(tmp_result, llc_str);
      if (tmp_result >= 0) {
        if (lib::is_mysql_mode()) {
          res_vec->set_int(output_idx, tmp_result);
        } else {
          ObNumStackOnceAlloc tmp_alloc;
          number::ObNumber res_nmb;
          if (OB_FAIL(res_nmb.from(tmp_result, tmp_alloc))) {
            SQL_LOG(WARN, "convert int to number failed", K(ret));
          } else {
            res_vec->set_number(output_idx, res_nmb);
          }
        }
      }
    } else {
      if (lib::is_oracle_mode()) {
        // set zero number
        number::ObNumber zero_nmb;
        zero_nmb.set_zero();
        res_vec->set_payload(output_idx, &zero_nmb, sizeof(ObNumberDesc));
      } else {
        res_vec->set_int(output_idx, 0);
      }
    }
    return ret;
  }

  template<typename ColumnFmt>
  int add_param_batch(RuntimeContext &agg_ctx, const ObBitVector &skip, ObBitVector &pvt_skip,
                      const EvalBound &bound, const RowSelector &row_sel, const int32_t agg_col_id,
                      const int32_t param_id, ColumnFmt &param_vec, char *aggr_cell)
  {
    int ret = OB_SUCCESS;
    ob_assert(agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS);
    char *llc_bitmap_buf = EXTRACT_MEM_ADDR(aggr_cell);
    if (OB_ISNULL(llc_bitmap_buf)) {
      llc_bitmap_buf = (char *)agg_ctx.allocator_.alloc(LLC_NUM_BUCKETS);
      if (OB_ISNULL(llc_bitmap_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMSET(llc_bitmap_buf, 0, LLC_NUM_BUCKETS);
        STORE_MEM_ADDR(llc_bitmap_buf, aggr_cell);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (row_sel.is_empty()) {
      for (int i = bound.start(); i < bound.end(); i++) {
        if (pvt_skip.at(i)) { continue; }
        if (param_vec.is_null(i)) {
          pvt_skip.set(i);
        }
      }
    } else {
      for (int i = 0; i < row_sel.size(); i++) {
        if (param_vec.is_null(row_sel.index(i))) { pvt_skip.set(row_sel.index(i)); }
      }
    }
    if (param_id == agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() - 1) {
      // last param, calculate hash values if possible
      ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
      const char *payload = nullptr;
      int32_t len = 0;
      ObDatum tmp_datum;
      if (row_sel.is_empty()) {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          uint64_t hash_val = 0;
          if (pvt_skip.at(i)) {
            continue;
          }
          for (int j = 0; OB_SUCC(ret) && j < param_exprs.count(); j++) {
            ObExpr *expr = param_exprs.at(j);
            ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_;
            expr->get_vector(agg_ctx.eval_ctx_)->get_payload(i, payload, len);
            tmp_datum.ptr_ = payload;
            tmp_datum.pack_ = len;
            if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
              SQL_LOG(WARN, "hash calculation failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(llc_add_value(hash_val, llc_bitmap_buf, LLC_NUM_BUCKETS))) {
              SQL_LOG(WARN, "add llc value failed");
            }
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          if (pvt_skip.at(row_sel.index(i))) {
            continue;
          }
          int64_t row_num = row_sel.index(i);
          uint64_t hash_val = 0;
          for (int j = 0; OB_SUCC(ret) && j < param_exprs.count(); j++) {
            ObExpr *expr = param_exprs.at(j);
            ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_;
            expr->get_vector(agg_ctx.eval_ctx_)->get_payload(row_num, payload, len);
            tmp_datum.ptr_ = payload;
            tmp_datum.pack_ = len;
            if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
              SQL_LOG(WARN, "hash calculation failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(llc_add_value(hash_val, llc_bitmap_buf, LLC_NUM_BUCKETS))) {
              SQL_LOG(WARN, "add llc value failed");
            }
          }
        }
      }
    }
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                  const bool is_null, const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    void *llc_bitmap_buf = get_tmp_res(agg_ctx, agg_col_idx, agg_cell);

    if (agg_func != T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE && OB_ISNULL(llc_bitmap_buf)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null llc bitmap buf", K(ret), K(*this));
    } else if (OB_LIKELY(agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.count() == 1)) {
      if (agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE) {
        if (OB_UNLIKELY(data_len != LLC_NUM_BUCKETS)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected input length", K(ret));
        } else if (!is_null) {
          char *llc_bitmap_buf = EXTRACT_MEM_ADDR(agg_cell);
          if (OB_ISNULL(llc_bitmap_buf)) {
            // not calculated before
            llc_bitmap_buf = (char *)agg_ctx.allocator_.alloc(LLC_NUM_BUCKETS);
            if (OB_ISNULL(llc_bitmap_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SQL_LOG(WARN, "allocate memory failed", K(ret));
            } else {
              MEMSET(llc_bitmap_buf, 0, LLC_NUM_BUCKETS);
              MEMCPY(llc_bitmap_buf, data, LLC_NUM_BUCKETS);
              STORE_MEM_ADDR(llc_bitmap_buf, agg_cell);
              *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = LLC_NUM_BUCKETS;
            }
          } else {
            for (int i = 0; i < LLC_NUM_BUCKETS; i++) {
              llc_bitmap_buf[i] =
                std::max(static_cast<uint8_t>(llc_bitmap_buf[i]), static_cast<uint8_t>(data[i]));
            }
          }
          agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell).set(agg_col_idx);
        }
      } else {
        const ObExpr *param_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0);
        ObExprHashFuncType hash_func = param_expr->basic_funcs_->murmur_hash_;
        if (OB_LIKELY(!is_null)) {
          ObDatum tmp_datum;
          tmp_datum.ptr_ = data;
          tmp_datum.pack_ = data_len;
          uint64_t hash_val = 0;
          if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
            SQL_LOG(WARN, "hash calculation failed", K(ret));
          } else if (OB_FAIL(llc_add_value(hash_val, reinterpret_cast<char *>(llc_bitmap_buf),
                                           LLC_NUM_BUCKETS))) {
            SQL_LOG(WARN, "llc add value failed", K(ret));
          } else {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
            not_nulls.set(agg_col_idx);
          }
        }
      }
    } else if (agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.count() > 1) {
      ob_assert(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == agg_func);
      ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_;
      bool has_null = false;
      for (int i = 0; !has_null && i < param_exprs.count(); i++) {
        has_null = param_exprs.at(i)->get_vector(agg_ctx.eval_ctx_)->is_null(batch_idx);
      }
      if (!has_null) {
        uint64_t hash_val = 0;
        ObDatum tmp_datum;
        const char *payload = nullptr;
        int32_t len = 0;
        for (int i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
          ObExpr *expr = param_exprs.at(i);
          ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_;
          expr->get_vector(agg_ctx.eval_ctx_)->get_payload(batch_idx, payload, len);
          tmp_datum.ptr_ = payload;
          tmp_datum.pack_ = len;
          if (OB_FAIL(hash_func(tmp_datum, hash_val, hash_val))) {
            SQL_LOG(WARN, "hash calculation failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(llc_add_value(hash_val, (char *)llc_bitmap_buf, LLC_NUM_BUCKETS))) {
            SQL_LOG(WARN, "llc add value failed", K(ret));
          } else {
            agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell).set(agg_col_idx);
          }
        }
      }
    }
    return ret;
  }

  inline void *get_tmp_res(RuntimeContext &agg_ctx, int32_t agg_col_id, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    void *ret_ptr = nullptr;
    if (agg_func == T_FUN_APPROX_COUNT_DISTINCT) {
      int32_t cell_len = agg_ctx.get_cell_len(agg_col_id, agg_cell);
      char *llc_bitmap_buf = EXTRACT_MEM_ADDR((agg_cell + cell_len));
      if (OB_ISNULL(llc_bitmap_buf)) {
        // allocate tmp res memory
        llc_bitmap_buf = (char *)agg_ctx.allocator_.alloc(LLC_NUM_BUCKETS);
        if (OB_ISNULL(llc_bitmap_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(ERROR, "allocate memory failed", K(ret));
        } else {
          MEMSET(llc_bitmap_buf, 0, LLC_NUM_BUCKETS);
          STORE_MEM_ADDR(llc_bitmap_buf, (agg_cell + cell_len));
          ret_ptr = llc_bitmap_buf;
        }
      } else {
        ret_ptr = llc_bitmap_buf;
      }
    } else if (agg_func == T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS) {
      char *llc_bitmap_buf = EXTRACT_MEM_ADDR(agg_cell);
      if (OB_ISNULL(llc_bitmap_buf)) {
        llc_bitmap_buf = (char *)agg_ctx.allocator_.alloc(LLC_NUM_BUCKETS);
        if (OB_ISNULL(llc_bitmap_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(ERROR, "allocate memory failed", K(ret));
        } else {
          MEMSET(llc_bitmap_buf, 0, LLC_NUM_BUCKETS);
          STORE_MEM_ADDR(llc_bitmap_buf, agg_cell);
          // set result length of approx_count_distinct_synopsis
          *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = LLC_NUM_BUCKETS;
        }
      }
      if (OB_SUCC(ret)) {
        ret_ptr = llc_bitmap_buf;
      }
    } else {
    }
    return ret_ptr;
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_id,
                                     char *agg_cell) override
  {
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.count() == 1);
    const ObExpr *expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0);
    OB_ASSERT(expr != NULL);
    OB_ASSERT(expr->basic_funcs_ != NULL);
    ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_;
    return reinterpret_cast<int64_t>(hash_func);
  }
  TO_STRING_KV("aggregate", "approx_count_distinct", K(in_tc), K(out_tc), K(agg_func));
private:
  int llc_add_value(const uint64_t value, char *llc_bitmap_buf, int64_t size)
  {
    // copy from `ObAggregateProcessor::llv_add_value`
    int ret = OB_SUCCESS;
    uint64_t bucket_index = value >> (64 - LLC_BUCKET_BITS);
    uint64_t pmax = 0;
    if (0 == value << LLC_BUCKET_BITS) {
      // do nothing
    } else {
      pmax = ObExprEstimateNdv::llc_leading_zeros(value << LLC_BUCKET_BITS, 64 - LLC_BUCKET_BITS) + 1;
    }
    ObString::obstr_size_t llc_num_buckets = size;
    OB_ASSERT(size == LLC_NUM_BUCKETS);
    OB_ASSERT(ObExprEstimateNdv::llc_is_num_buckets_valid(llc_num_buckets));
    OB_ASSERT(llc_num_buckets > bucket_index);
    if (pmax > static_cast<uint8_t>(llc_bitmap_buf[bucket_index])) {
      // 理论上pmax不会超过65.
      llc_bitmap_buf[bucket_index] = static_cast<uint8_t>(pmax);
    }
    return ret;
  }
};

} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_APPROX_COUNT_DISTINCT_H
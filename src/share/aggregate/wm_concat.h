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

#ifndef OCEANBASE_SHARE_WM_CONCAT_AGGREGATE_H_
#define OCEANBASE_SHARE_WM_CONCAT_AGGREGATE_H_

#include "lib/utility/utility.h"
#include "share/aggregate/iaggregate.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/expr/ob_expr.h"
#include "storage/lob/ob_lob_locator.h"
#include <algorithm>

namespace oceanbase {
namespace share {
namespace aggregate {

/**
 * Oracle mode wm_concat aggregate function implementation
 *
 * features:
 * 1. no ORDER BY: stream processing, directly concatenate strings
 * 2. with ORDER BY: use GroupStoreWrapper to store data and then sort
 * 3. support DISTINCT: use DistinctWrapper to remove duplicates
 * 4. support T_FUN_KEEP_WM_CONCAT: use GroupStoreWrapper to store data and use
 * topn sort
 */
template <VecValueTypeClass in_tc, bool has_lob_header>
class WmConcatAggregate final
    : public BatchAggregateWrapper<WmConcatAggregate<in_tc, has_lob_header>> {
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_LOB;
  using BaseClass =
      BatchAggregateWrapper<WmConcatAggregate<in_tc, has_lob_header>>;

private:
struct WMConcatCell {
  char *string_ptr_;
  int32_t buf_len_;
  int32_t str_len_;
};

private:
class WMConcatCellHelper {
public:
  static WMConcatCell *from_agg_cell(char *agg_cell) {
    return reinterpret_cast<WMConcatCell *>(agg_cell);
  }

  static const WMConcatCell *from_agg_cell(const char *agg_cell) {
    return reinterpret_cast<const WMConcatCell *>(agg_cell);
  }

  static void get_string_from_cell(const char *agg_cell,
                                   ObString &base_string) {
    const WMConcatCell *cell = from_agg_cell(agg_cell);
    base_string = ObString(cell->buf_len_, cell->str_len_, cell->string_ptr_);
  }

  static void set_string(char *agg_cell, const ObString &base_string) {
    WMConcatCell *cell = from_agg_cell(agg_cell);
    cell->string_ptr_ = const_cast<char *>(base_string.ptr());
    cell->buf_len_ = base_string.size();
    cell->str_len_ = base_string.length();
  }

};

public:
  WmConcatAggregate() {}
  template <typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *data,
                           const int32_t data_len) {
    int ret = OB_SUCCESS;
    UNUSEDx(data_len);
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObString res_data = nullptr;
    if (FALSE_IT(WMConcatCellHelper::get_string_from_cell(data, res_data))) {
      SQL_LOG(WARN, "get string from agg cell failed", K(ret));
    } else {
      int64_t output_idx = ctx.get_batch_idx();
      ColumnFormat &columns =
          *static_cast<ColumnFormat *>(agg_expr.get_vector(ctx));
      const NotNullBitVector &not_nulls =
          agg_ctx.locate_notnulls_bitmap(agg_col_id, data);
      if (OB_LIKELY(not_nulls.at(agg_col_id))) {
        ObLobLocator *result = nullptr;
        char *total_buf = NULL;
        const int64_t total_buf_len =
            sizeof(ObLobLocator) + res_data.length(); // Notice: using lob locator v1
        if (OB_ISNULL(total_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_,
                                                           total_buf_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "Failed to allocate memory for lob locator", K(ret),
                  K(total_buf_len));
        } else if (FALSE_IT(result =
                                reinterpret_cast<ObLobLocator *>(total_buf))) {
        } else if (OB_FAIL(result->init(res_data))) {
          SQL_LOG(WARN, "Failed to init lob locator", K(ret), K(res_data),
                   K(result));
        } else {
          ObDatum res_datum;
          res_datum.set_lob_locator(*result);
          CellWriter<AggCalcType<VEC_TC_LOB>>::set(
              res_datum.ptr_, res_datum.get_int_bytes(), &columns, output_idx,
              total_buf);
        }

      } else {
        columns.set_null(output_idx);
      }
    }
    return ret;
  }

  inline int concat_one_row(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                            int64_t row_num, ObString &base_string,
                            int64_t &concated_rows,
                            const uint64_t concat_str_max_len,
                            const ObString &sep_str) {
    int ret = OB_SUCCESS;
    ObAggrInfo &info = agg_ctx.aggr_infos_.at(agg_col_idx);
    ObIArray<ObExpr *> &param_exprs = info.param_exprs_;
    int32_t append_length = 0;

    // append all candidate strings
    ObString candidate_string;
    const ObCollationType cs_type = info.expr_->datum_meta_.cs_type_;
    if (has_lob_header) {
      ObString lob_data = param_exprs.at(0)
                              ->get_vector(agg_ctx.eval_ctx_)
                              ->get_string(row_num);
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              agg_ctx.allocator_,
              param_exprs.at(0)->get_vector(agg_ctx.eval_ctx_),
              param_exprs.at(0)->datum_meta_,
              param_exprs.at(0)->obj_meta_.has_lob_header(), lob_data,
              row_num))) {
        SQL_LOG(WARN, "failed to read text", K(ret), K(lob_data));
      } else {
        candidate_string = lob_data;
      }
    } else {
      candidate_string = param_exprs.at(0)
                             ->get_vector(agg_ctx.eval_ctx_)
                             ->get_string(row_num);
    }

    if (OB_FAIL(ret)) {
    } else if (base_string.length() > 0) {
      if (OB_FAIL(append_str(cs_type, base_string, sep_str, concat_str_max_len))) {
        SQL_LOG(WARN, "append string failed", K(ret), K(base_string));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append_str(cs_type, base_string, candidate_string, concat_str_max_len))) {
      SQL_LOG(WARN, "append candidate string failed", K(ret),
              K(candidate_string.length()));
    } else {
      concated_rows += 1;
    }

    return ret;
  }

  /**
   * single row processing interface (for compatibility)
   */
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num,
                         int64_t batch_size, const bool is_null,
                         const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override {
    int ret = OB_NOT_IMPLEMENT;
    return OB_SUCCESS;
  }

  /**
   * rollup aggregation (not supported)
   */
  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "vectorization 2.0 not support wm_concat with rollup", K(ret));
    return ret;
  }

  TO_STRING_KV("aggregate", "wm_concat");

protected:
  /**
   * append string to result
   */
  int append_str(const ObCollationType cs_type, ObString &base_string,
                 ObString add_string, const uint64_t concat_str_max_len) {
    int ret = OB_SUCCESS;
    int32_t append_len = add_string.length();


    if (base_string.size() < append_len + base_string.length()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "concat buf len is error", K(ret), K(base_string.size()),
              K(base_string.length()), K(append_len));
    } else if (0 < append_len) {
      int32_t written = base_string.write(add_string.ptr(), append_len);
    }

    return ret;
  }

  /**
   * extend string buffer
   */
  inline int extend_string(ObString &base_string, const int32_t length,
                           common::ObArenaAllocator &allocator) {
    int ret = OB_SUCCESS;
    int original_length = base_string.length();
    bool use_tmp_allocator = false;
    if (OB_LIKELY(length > original_length)) {
      use_tmp_allocator = true;
      char *tmp = static_cast<char *>(allocator.alloc((const int64_t)length));
      if (OB_ISNULL(tmp)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret), K(original_length),
                K(length), K(allocator.get_label()), K(allocator.used()),
                K(allocator.total()));
      } else {
        if (original_length > 0) {
          MEMCPY(tmp, base_string.ptr(), original_length);
          allocator.free(base_string.ptr()); // actually not work
        }
        base_string = ObString(length, original_length, tmp);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "Unexpected condition", K(length),
              K(original_length));
    }
    return ret;
  }

public:
  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound,
                     char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());

    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    ObExpr *param_expr = aggr_info.param_exprs_.at(0);
    ObIVector *vec = param_expr->get_vector(agg_ctx.eval_ctx_);

    uint64_t concat_str_max_len = OB_MAX_PACKET_LENGTH;

    if (!row_sel.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "row_sel is null", K(ret));
    } else if (skip.is_all_true(bound.start(), bound.end())) {
      SQL_LOG(DEBUG, "add_batch_rows - early return",
              K(skip.is_all_true(bound.start(), bound.end())));
    } else {
      ObString base_string = nullptr;
      int64_t concated_rows = 0;
      if (FALSE_IT(WMConcatCellHelper::get_string_from_cell(agg_cell,
                                                            base_string))) {
        SQL_LOG(WARN, "get string failed", K(ret));
      } else {
        ObString sep_str = ObCharsetUtils::get_const_str(
            aggr_info.expr_->datum_meta_.cs_type_, ',');

        // 1st loop: calculate the total length of this batch
        int64_t total_append_length = 0;
        int64_t valid_rows_count = 0;
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i) || vec->is_null(i)) {
            continue;
          }
          int64_t param_len = 0;
          if (has_lob_header) {
            ObString lob_header = vec->get_string(i);
            ObLobLocatorV2 locator(lob_header,
                                   param_expr->obj_meta_.has_lob_header());
            if (OB_FAIL(locator.get_lob_data_byte_len(param_len))) {
              SQL_LOG(WARN, "get lob data byte length failed", K(ret),
                      K(locator));
            }
          } else {
            param_len = vec->get_length(i);
          }

          if ((base_string.length() > 0 && valid_rows_count == 0) ||
              valid_rows_count > 0) {
            total_append_length += 1; // separator length
          }
          total_append_length += param_len;
          valid_rows_count++;
        }

        // if there are valid data, do the second loop: actual append operation
        if (OB_SUCC(ret) && valid_rows_count > 0) {
          int64_t needed_length = base_string.length() + total_append_length;
          if (OB_UNLIKELY(needed_length > concat_str_max_len)) {
            ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
            SQL_LOG(WARN, "result of string concatenation is too long", K(ret), K(needed_length),
                                                                     K(OB_MAX_PACKET_LENGTH));
          } else if (needed_length > base_string.size() &&
              OB_FAIL(extend_string(base_string, needed_length,
                                    agg_ctx.allocator_))) {
            SQL_LOG(WARN, "ensure string space failed", K(ret));
          }
        }

        // 2nd loop: actual append operation
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i) || vec->is_null(i)) {
            continue;
          }
          if (OB_FAIL(concat_one_row(agg_ctx, agg_col_id, i, base_string,
                                     concated_rows, concat_str_max_len, sep_str))) {
            SQL_LOG(WARN, "concat one row failed", K(ret));
          }
        }

        if (OB_SUCC(ret) && concated_rows > 0) {
          NotNullBitVector &not_nulls =
              agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
          not_nulls.set(agg_col_id);
          WMConcatCellHelper::set_string(agg_cell, base_string);
        }
      }
    }
    return ret;
  }
};

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_WM_CONCAT_AGGREGATE_H_

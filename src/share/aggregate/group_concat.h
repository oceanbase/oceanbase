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
template <VecValueTypeClass in_tc>
class GroupConcatAggregate final : public BatchAggregateWrapper<GroupConcatAggregate<in_tc>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_STRING;
  using BaseClass = BatchAggregateWrapper<GroupConcatAggregate<in_tc>>;

public:
  GroupConcatAggregate()
  {}

  template <typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *data, const int32_t data_len)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(data_len);
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;

    ObString res_data = nullptr;
    if (OB_FALSE_IT(get_string_from_cell(data, res_data))) {
      SQL_LOG(WARN, "get string from agg cell failed", K(ret));
    } else {
      int64_t output_idx = ctx.get_batch_idx();
      ColumnFormat &columns = *static_cast<ColumnFormat *>(agg_expr.get_vector(ctx));
      const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, data);
      if (OB_LIKELY(not_nulls.at(agg_col_id))) {
        char *res_buf = nullptr;
        if (OB_ISNULL(res_buf = agg_expr.get_str_res_mem(agg_ctx.eval_ctx_, res_data.length()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          CellWriter<AggCalcType<VEC_TC_STRING>>::set(res_data.ptr(), res_data.length(), &columns,
                                                      output_idx, res_buf);
        }
      } else {
        columns.set_null(output_idx);
      }
    }
    return ret;
  }

  inline int concat_one_row(RuntimeContext &agg_ctx, int32_t agg_col_idx, int64_t row_num,
                            ObString &base_string, int64_t &concated_rows, bool &buf_is_full,
                            const uint64_t concat_str_max_len, const ObString &sep_str)
  {
    int ret = OB_SUCCESS;

    ObAggrInfo &info = agg_ctx.aggr_infos_.at(agg_col_idx);
    ObIArray<ObExpr *> &param_exprs = info.param_exprs_;

    bool has_null = false;

    int32_t append_length = 0;
    if (concated_rows > 0 && !buf_is_full) {
      append_length += sep_str.length();
    }
    for (int j = 0; j < info.group_concat_param_count_ && !buf_is_full && OB_SUCC(ret); j++) {
      if (param_exprs.at(j)->get_vector(agg_ctx.eval_ctx_)->is_null(row_num)) {
        has_null = true;
        break;
      }
      append_length += param_exprs.at(j)->get_vector(agg_ctx.eval_ctx_)->get_length(row_num);
    }

    if (has_null) {
    } else if (OB_FAIL(ensure_string_space(base_string, append_length, agg_ctx.allocator_,
                                           concat_str_max_len))) {
    } else {
      if (concated_rows > 0 && !buf_is_full) {
        if (OB_FAIL(append_str(info.expr_->datum_meta_.cs_type_, base_string, sep_str,
                               concated_rows, buf_is_full, concat_str_max_len))) {
          SQL_LOG(WARN, "append string failed", K(ret), K(base_string));
        }
      }
      for (int j = 0; j < info.group_concat_param_count_ && !buf_is_full && OB_SUCC(ret); j++) {
        ObString candidate_string =
          param_exprs.at(j)->get_vector(agg_ctx.eval_ctx_)->get_string(row_num);
        ret = append_str(info.expr_->datum_meta_.cs_type_, base_string, candidate_string,
                         concated_rows, buf_is_full, concat_str_max_len);
      }
      if (OB_SUCC(ret)) {
        concated_rows += 1;
      }
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_NOT_IMPLEMENT;
    // Scalar Group by and Merge Group by should not reach this.
    return OB_SUCCESS;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_NOT_IMPLEMENT;
    if (lib::is_oracle_mode()) {
      SQL_LOG(WARN, "vectorization 2.0 not support LISTAGG with rollup", K(ret));
    } else {
      SQL_LOG(WARN, "vectorization 2.0 not support group_concat with rollup", K(ret));
    }

    return ret;
  }

  TO_STRING_KV("aggregate", "count");

protected:
  /*
              ------------------------------------------------------------------
    Agg_cell: |pointer of string| buffer size | string length | concated rows  |
              ------------------------------------------------------------------
              |     char **     |   int32_t   |    int32_t    |     int64_t    |
              ------------------------------------------------------------------

    The GroupConcat function uses an ObString type along with an int32_t to process and record
    concatenated rows for aggregation. As shown in the table, an ObString is stored using a string
    pointer and two int32_t values, while the final int64_t tracks the number of concatenated rows.
    This design allows for the output of a warning message when the string length exceed
    concat_str_max_len in MySQL mode. A buffer resizing strategy is employed, where the buffer size
    doubles whenever the current capacity is insufficient.

    Initially, the string buffer is set to 1024 bytes by default. When more space is required, the
    buffer size doubles. However, if the concat_str_max_len value is smaller than the required
    buffer size, the string length is capped at concat_str_max_len.

    Whenever a new string (add_string) needs to be appended to the base string (base_string), we
    first check if the combined length of base_string and add_string fits within the current buffer.
    If the combined length is less than or equal to the buffer size, we append add_string.
    Otherwise, we attempt to expand the buffer to the next power of two that can accommodate the
    combined length. If the expansion is successful, add_string is appended. If the buffer extension
    fails due to the concat_str_max_len limit, the behavior varies depending on the mode (MySQL or
    Oracle).

    In Oracle mode, the function sets the error code OB_ERR_TOO_LONG_STRING_IN_CONCAT and returns
    the error.

    In MySQL mode, the handling is a bit more complex. If the buffer is full, add_string will be
    trimmed to fill the remaining space up to str_max_len. Additionally, the current count of
    concatenated rows is outputed. To improve efficiency, the buf_is_full flag is computed before
    processing each batch, allowing the system to skip unnecessary concatenation once the buffer is
    full.
  */
  int append_str(const ObCollationType cs_type, ObString &base_string, ObString add_string,
                 int64_t concated_rows, bool &buf_is_full, const uint64_t concat_str_max_len)
  {
    int ret = OB_SUCCESS;

    int32_t append_len = add_string.length();
    if (concat_str_max_len < base_string.length() + add_string.length()) {
      append_len = concat_str_max_len - base_string.length();
      int64_t well_formed_len = 0;
      int32_t well_formed_error = 0;

      if (OB_FAIL(ObCharset::well_formed_len(cs_type, add_string.ptr(), append_len, well_formed_len,
                                             well_formed_error))) {
        SQL_LOG(WARN, "invalid string for charset", K(ret), K(cs_type), K(add_string));
      } else {
        append_len = well_formed_len;
        buf_is_full = true;
        LOG_USER_WARN(OB_ERR_CUT_VALUE_GROUP_CONCAT, static_cast<long>(concated_rows + 1));
      }
    }

    if (OB_SUCC(ret)) {
      if (base_string.size() < append_len + base_string.length()) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "concat buf len is error", K(ret), K(base_string.size()),
                K(base_string.length()), K(append_len));
      } else if (0 < append_len) {
        base_string.write(add_string.ptr(), append_len);
      }
    }
    return ret;
  }

  inline int get_concat_str_max_len(RuntimeContext &agg_ctx, uint64_t &concat_str_max_len)
  {
    int ret = OB_SUCCESS;
    concat_str_max_len = (lib::is_oracle_mode() ? OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE :
                                                  OB_DEFAULT_GROUP_CONCAT_MAX_LEN);
    if (!lib::is_oracle_mode()
        && OB_FAIL(agg_ctx.eval_ctx_.exec_ctx_.get_my_session()->get_group_concat_max_len(
             concat_str_max_len))) {
      SQL_LOG(WARN, "fail to get group concat max len", K(ret));
    }
    return ret;
  }

  int get_sep_str(RuntimeContext &agg_ctx, const ObAggrInfo &aggr_info, ObString &sep_str,
                  const sql::ObBitVector &skip, const sql::EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    if (aggr_info.separator_expr_ == NULL) {
      // Default sperator for not specific case.
      if (lib::is_oracle_mode()) {
        sep_str = ObString::make_empty_string();
      } else {
        // default comma
        sep_str = ObCharsetUtils::get_const_str(aggr_info.expr_->datum_meta_.cs_type_, ',');
      }
    } else if (aggr_info.separator_expr_->is_const_expr()) {
      // If user specific a seperator, and it is a const, use it directly.
      ObDatum *separator_result = NULL;
      sql::ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
      if (OB_UNLIKELY(!aggr_info.separator_expr_->obj_meta_.is_string_type())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "expr node is null", K(ret), KPC(aggr_info.separator_expr_));
      } else if (OB_FAIL(aggr_info.separator_expr_->eval_vector(eval_ctx, skip, bound))) {
        SQL_LOG(WARN, "eval failed", K(ret));
      } else {
        sep_str =
          aggr_info.separator_expr_->get_vector(eval_ctx)->get_string(eval_ctx.get_batch_idx());
      }
    } else {
      // If the seperator is a column, use the first one of its group.
      if (OB_UNLIKELY(!aggr_info.separator_expr_->obj_meta_.is_string_type())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "expr node is null", K(ret), KPC(aggr_info.separator_expr_));
      } else {
        sql::ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
        if (OB_FAIL(aggr_info.separator_expr_->eval_vector(eval_ctx, skip, bound))) {
          SQL_LOG(WARN, "eval failed", K(ret));
        } else {
          int first_idx = bound.start();
          while (first_idx < bound.end()) {
            if (skip.at(first_idx)) {
              first_idx += 1;
            } else {
              break;
            }
          }
          if (first_idx >= bound.end()
              || aggr_info.separator_expr_->get_vector(eval_ctx)->is_null(first_idx)) {
            sep_str = ObString::make_empty_string();
          } else {
            sep_str = aggr_info.separator_expr_->get_vector(eval_ctx)->get_string(first_idx);
          }
        }
        SQL_LOG(DEBUG, "get sep str", K(sep_str));
      }
    }

    return ret;
  }

  inline int get_buf_is_full_sign(char *agg_cell, bool &buf_is_full,
                                  const uint64_t &concat_str_max_len)
  {
    int ret = OB_SUCCESS;
    buf_is_full = false;
    if (oceanbase::lib::is_mysql_mode()) {
      int32_t buffer_size = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **));
      int32_t string_len =
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **) + sizeof(int32_t));
      if (buffer_size == string_len && buffer_size == concat_str_max_len) {
        buf_is_full = true;
      }
    }
    return ret;
  }

  // Get the string from agg_cell and assign an appropriate buffer size
  inline void get_string_from_cell(char *agg_cell, ObString &cur_string, int64_t &concated_rows)
  {
    int32_t buffer_size = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **));
    int32_t string_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **) + sizeof(int32_t));
    concated_rows = *reinterpret_cast<int64_t *>(agg_cell + sizeof(char **) + 2 * sizeof(int32_t));
    char *string_ptr = *reinterpret_cast<char **>(agg_cell);

    cur_string = ObString(buffer_size, string_len, string_ptr);
  }

  // Get the string without any modification, designed for collect.
  inline void get_string_from_cell(const char *agg_cell, ObString &cur_string)
  {
    int32_t buffer_size = *reinterpret_cast<const int32_t *>(agg_cell + sizeof(char **));
    int32_t string_len =
      *reinterpret_cast<const int32_t *>(agg_cell + sizeof(char **) + sizeof(int32_t));
    const char *string_ptr = *reinterpret_cast<const char *const *>(agg_cell);

    cur_string = ObString(buffer_size, string_len, string_ptr);
  }

  inline void set_string(char *agg_cell, ObString &cur_string, int64_t concated_rows)
  {
    *reinterpret_cast<char **>(agg_cell) = cur_string.ptr();
    *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **)) = cur_string.size();
    *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **) + sizeof(int32_t)) = cur_string.length();
    *reinterpret_cast<int64_t *>(agg_cell + sizeof(char **) + 2 * sizeof(int32_t)) = concated_rows;
  }

  inline int extend_string(ObString &base_string, const int32_t length,
                           common::ObArenaAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    int original_length = base_string.length();
    if (OB_LIKELY(length > original_length)) {
      char *tmp = static_cast<char *>(allocator.alloc((const int64_t)length));
      if (OB_ISNULL(tmp)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        if (original_length > 0) {
          MEMCPY(tmp, base_string.ptr(), original_length);
          allocator.free(base_string.ptr());
        }
        base_string = ObString(length, original_length, tmp);
      }
    }
    return ret;
  }

  // If the space not enough, double the space.
  inline int ensure_string_space(ObString &base_string, const int32_t append_length,
                                 common::ObArenaAllocator &allocator,
                                 const uint64_t max_length = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    int after_length = base_string.length() + append_length;
    int buffer_size = base_string.size();

    if (after_length <= buffer_size) {
      // do nothing
    } else {
      if (after_length <= max_length || oceanbase::lib::is_mysql_mode()) {
        if (after_length > buffer_size && buffer_size < max_length) {
          if (OB_UNLIKELY(buffer_size <= 0)) {
            // Init the buffer size to 1024.
            buffer_size = max_length > 1024 ? 1024 : max_length;
          }
          if (buffer_size * 2 < after_length) {
            buffer_size = next_pow2(after_length);
          } else if (after_length > buffer_size) {
            buffer_size = buffer_size * 2;
          }
          if (buffer_size > max_length) {
            buffer_size = max_length;
          }
          ret = extend_string(base_string, buffer_size, allocator);
        }
      } else {
        ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
        SQL_LOG(WARN, "result of string concatenation is too long", K(ret), K(append_length),
                K(base_string.length()), K(max_length));
      }
    }
    return ret;
  }

public:
  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(row_sel.is_empty());
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;

    bool buf_is_full = false;
    uint64_t concat_str_max_len = 0;

    if (skip.is_all_true(bound.start(), bound.end())) {
    } else if (OB_FAIL(get_concat_str_max_len(agg_ctx, concat_str_max_len))) {
      SQL_LOG(WARN, "fail to get group concat max len", K(ret));
    } else if (OB_FAIL(get_buf_is_full_sign(agg_cell, buf_is_full, concat_str_max_len))) {
      SQL_LOG(WARN, "get buf is full failed", K(ret));
    } else if (buf_is_full) {
    } else {
      ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);

      // If all exprs are visited, then append those not skipped rows to agg_cell.
      ObString base_string = nullptr;
      int64_t concated_rows = 0;
      if (FALSE_IT(get_string_from_cell(agg_cell, base_string, concated_rows))) {
        SQL_LOG(WARN, "get string failed", K(ret));
      } else {
        int64_t before_concat_rows = concated_rows;
        ObString sep_str = nullptr;
        if (OB_FAIL(get_sep_str(agg_ctx, aggr_info, sep_str, skip, bound))) {
          SQL_LOG(WARN, "get sep str failed", K(ret));
        }
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end() && !buf_is_full; i++) {
          if (skip.at(i)) {
            continue;
          }
          ret = concat_one_row(agg_ctx, agg_col_id, i, base_string, concated_rows, buf_is_full,
                               concat_str_max_len, sep_str);
        }
        if (OB_SUCC(ret) && concated_rows > before_concat_rows) {
          NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
          not_nulls.set(agg_col_id);
          set_string(agg_cell, base_string, concated_rows);
        }
      }
    }
    return ret;
  }
};

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_COUNT_AGGREGATE_H_

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

#ifndef OCEANBASE_SHARE_AGGREGATE_ANY_H_
#define OCEANBASE_SHARE_AGGREGATE_ANY_H_

#include "share/aggregate/iaggregate.h"
#include "min_max.ipp"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
using namespace sql;

template<VecValueTypeClass vec_tc>
class AnyAggregate final : public BatchAggregateWrapper<AnyAggregate<vec_tc>>
{
  using buf_node = std::pair<char *, int32_t>;
  static const int32_t BUF_BLOCK_SIZE = 512;
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = vec_tc;
public:

public:
  AnyAggregate() {}

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    UNUSEDx(agg_col_id, allocator);
    int ret = OB_SUCCESS;
    return ret;
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                    const sql::ObBitVector &skip, const sql::EvalBound &bound,
                    char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    const char *payload = nullptr;
    bool null_cell = false;
    int32_t data_len = 0;
    bool has_data = false;
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      // already copied
    } else if (OB_ISNULL(aggr_info.param_exprs_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null expr", K(ret));
    } else if (row_sel.is_empty()) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (!bound.get_all_rows_active() && skip.at(i)) { // do nothing
        } else {
          ObIVector *data_vec = aggr_info.param_exprs_.at(0)->get_vector(ctx);
          if (data_vec->is_null(i)) {
            continue;
          } else if (aggr_info.param_exprs_.at(0)->is_nested_expr()) {
            if(OB_FAIL(ObArrayExprUtils::get_collection_payload(agg_ctx.allocator_, ctx, *aggr_info.param_exprs_.at(0), i, payload, data_len))) {
              SQL_LOG(WARN, "get collection payload failed", K(ret));
            }
          } else {
            data_vec->get_payload(i, payload, data_len);
          }
          has_data = true;
          break;
        }
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        ObIVector *data_vec = aggr_info.param_exprs_.at(0)->get_vector(ctx);
        if (data_vec->is_null(row_sel.index(i))) {
          continue;
        } else if (aggr_info.param_exprs_.at(0)->is_nested_expr()) {
          if(OB_FAIL(ObArrayExprUtils::get_collection_payload(agg_ctx.allocator_, ctx, *aggr_info.param_exprs_.at(0), row_sel.index(i), payload, data_len))) {
            SQL_LOG(WARN, "get collection payload failed", K(ret));
          }
        } else {
          aggr_info.param_exprs_.at(0)->get_vector(ctx)->get_payload(row_sel.index(i), payload, data_len);
        }
        has_data = true;
        break;
      }
    }
    if (OB_SUCC(ret) && has_data) {
      if (aggr_info.param_exprs_.at(0)->is_nested_expr() && data_len > 0) {
        // do memcpy in get_collection_payload
        agg_ctx.set_agg_cell(payload, data_len, agg_col_id, agg_cell);
      } else if (data_len > 0) {
        void *tmp_buf = agg_ctx.allocator_.alloc(data_len);
        if (OB_ISNULL(tmp_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          MEMCPY(tmp_buf, payload, data_len);
          // store data ptr and len
          agg_ctx.set_agg_cell((char *)tmp_buf, data_len, agg_col_id, agg_cell);
        }
      } else {
        agg_ctx.set_agg_cell(nullptr, data_len, agg_col_id, agg_cell);
      }
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_idx);
    if (OB_LIKELY(not_nulls.at(agg_col_idx))) {
      // already copied
    } else if (!is_null) {
      if (aggr_info.param_exprs_.at(0)->is_nested_expr() && data_len > 0) {
        const char *compact_ptr = nullptr;
        int32_t compact_data_len = 0;
        if (OB_FAIL(ObArrayExprUtils::get_collection_payload(agg_ctx.allocator_, agg_ctx.eval_ctx_,
                                                             *aggr_info.param_exprs_.at(0), batch_idx,
                                                             compact_ptr, compact_data_len))) {
          SQL_LOG(WARN, "get collection payload failed", K(ret));
        } else {
          agg_ctx.set_agg_cell(compact_ptr, compact_data_len, agg_col_idx, agg_cell);
        }
      } else if (data_len > 0) {
        if (OB_ISNULL(data)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_LOG(WARN, "invalid null payload", K(ret));
        } else {
          void *tmp_buf = agg_ctx.allocator_.alloc(data_len);
          if (OB_ISNULL(tmp_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_LOG(WARN, "allocate memory failed", K(ret));
          } else {
            MEMCPY(tmp_buf, data, data_len);
            // store data ptr and len
            agg_ctx.set_agg_cell((char *)tmp_buf, data_len, agg_col_idx, agg_cell);
          }
        }
      }
      not_nulls.set(agg_col_idx);
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
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      if (helper::is_var_len_agg_cell(vec_tc)) {
        char *res_buf = agg_expr.get_str_res_mem(ctx, agg_cell_len);
        if (OB_ISNULL(res_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          const char *data =
            reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(agg_cell));
          CellWriter<AggCalcType<vec_tc>>::set(data, agg_cell_len, res_vec, output_idx, res_buf);
        }
      } else {
        CellWriter<AggCalcType<vec_tc>>::set(agg_cell, agg_cell_len, res_vec, output_idx, nullptr);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    return ret;
  }

  TO_STRING_KV("aggregate", "any", K(vec_tc));
  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    const NotNullBitVector &curr_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    if (!rollup_not_nulls.at(agg_col_idx)) {
      if (curr_not_nulls.at(agg_col_idx)) {
        rollup_not_nulls.set(agg_col_idx);
      }
      int32_t curr_agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, group_row);
      if (OB_LIKELY(curr_not_nulls.at(agg_col_idx) && curr_agg_cell_len != INT32_MAX)) {
        const char *curr_payload = (const char *)(curr_agg_cell);
          agg_ctx.set_agg_cell(curr_payload, curr_agg_cell_len, agg_col_idx, rollup_agg_cell);
      }
    }
    return ret;
  }
};

} // end aggregate
} // end share
} // end oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_MIN_H_
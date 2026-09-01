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

#ifndef OCEANBASE_SHARE_AGGREGATE_RETENTION_H_
#define OCEANBASE_SHARE_AGGREGATE_RETENTION_H_

#include "share/aggregate/iaggregate.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

// RETENTION(event_1, ..., event_n) aggregate — two-phase design
//
// Stage is detected at runtime from the first param's SQL type:
//   param[0] is integer    → stage-1 (partial / normal)
//   param[0] is collection → stage-2 (merge)
//
// Stage-1 input : N integer/bool params
//         output: ARRAY<TINYINT> raw bits  [bit0, bit1, ..., bit_{N-1}]
// Stage-2 input : param[0] = partial ARRAY<TINYINT> from stage-1
//         output: ARRAY<TINYINT> with RETENTION semantics
//                 result[0]   = bit0
//                 result[i>0] = bit0 AND bit_i
//
// agg_cell layout (5 bytes):
//   ┌──────────────────┬──────────┐
//   │ uint32_t bitmap  │ int8_t N │
//   │ (offset 0)       │ (offset 4)│
//   └──────────────────┴──────────┘
//   bitmap: bit i set iff event_{i+1} seen in this group
//   N     : event count (max 32), written lazily on first contributing row
//           stage-1 → param_exprs_.count()
//           stage-2 → first partial array's size()
//
// reserved_agg_col_size has a special case for T_FUN_RETENTION → sizeof(int32_t) + sizeof(int8_t).
// use_var_len_ excludes T_FUN_RETENTION so the framework does not misread the cell.

class RetentionAggregate final : public BatchAggregateWrapper<RetentionAggregate>
{
public:
  // RETENTION is multi-param + custom layout, so the framework's single-param IN_TC
  // dispatch path is not taken. IN_TC is therefore only a compile-time placeholder.
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_COLLECTION;

  // Maximum number of event conditions accepted by RETENTION(event_1, ..., event_n).
  // Bound by the 32-bit agg_cell bitmap; also enforced at resolve time.
  static const constexpr int64_t MAX_RETENTION_EVENTS = 32;

  static const constexpr int64_t CELL_SIZE = sizeof(int32_t) + sizeof(int8_t);

  static inline uint32_t &cell_bitmap(char *cell)
  { return *reinterpret_cast<uint32_t *>(cell); }
  static inline uint32_t cell_bitmap(const char *cell)
  { return *reinterpret_cast<const uint32_t *>(cell); }
  static inline int8_t &cell_event_count(char *cell)
  { return *reinterpret_cast<int8_t *>(cell + sizeof(int32_t)); }
  static inline int8_t cell_event_count(const char *cell)
  { return *reinterpret_cast<const int8_t *>(cell + sizeof(int32_t)); }

  // Returns true when all bits [0, event_count) are set — i.e. every event seen.
  static inline bool bitmap_saturated(uint32_t bitmap, int8_t event_count)
  {
    if (event_count == 0) { return false; }
    const uint32_t full_mask =
        (event_count == MAX_RETENTION_EVENTS) ? ~0u : ((1u << event_count) - 1u);
    return (bitmap & full_mask) == full_mask;
  }

  // Runtime stage detection: stage-2 has a collection-typed first param.
  static inline bool is_merge_stage(const sql::ObAggrInfo &aggr_info)
  {
    return aggr_info.param_exprs_.count() == 1 && ob_is_collection_sql_type(aggr_info.param_exprs_.at(0)->datum_meta_.get_type());
  }

public:
  RetentionAggregate() {}

  // Single-row path not used (multi-param aggregate). Return error to catch misuse.
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    UNUSEDx(agg_ctx, row_num, batch_size, is_null, data, data_len, agg_col_idx, agg_cell);
    return OB_ERR_UNEXPECTED;
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound,
                     char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    sql::ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;

    if (is_merge_stage(aggr_info)) {
      ObIVector *vec = aggr_info.param_exprs_.at(0)->get_vector(eval_ctx);
      const uint16_t meta_id = aggr_info.param_exprs_.at(0)->obj_meta_.get_subschema_id();
      // Init cell once per batch (same agg_cell for all rows in non-hash path).
      setup_initial_value(agg_ctx, agg_cell, agg_col_id);
      if (row_sel.is_empty()) {
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); idx++) {
          if (skip.at(idx) || vec->is_null(idx)) { continue; }
          if (bitmap_saturated(cell_bitmap(agg_cell), cell_event_count(agg_cell))) { continue; }
          const char *payload = nullptr;
          int32_t len = 0;
          vec->get_payload(idx, payload, len);
          if (OB_FAIL(merge_partial_array(eval_ctx, meta_id, payload, len, agg_cell))) {
            SQL_LOG(WARN, "merge partial array failed", K(ret));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          const int64_t idx = row_sel.index(i);
          if (vec->is_null(idx)) { continue; }
          if (bitmap_saturated(cell_bitmap(agg_cell), cell_event_count(agg_cell))) { continue; }
          const char *payload = nullptr;
          int32_t len = 0;
          vec->get_payload(idx, payload, len);
          if (OB_FAIL(merge_partial_array(eval_ctx, meta_id, payload, len, agg_cell))) {
            SQL_LOG(WARN, "merge partial array failed", K(ret));
          }
        }
      }
    } else {
      // Stage-1: pre-fetch vectors and null metadata once per batch.
      const int64_t param_count = MIN(aggr_info.param_exprs_.count(), MAX_RETENTION_EVENTS);
      ObIVector *vecs[MAX_RETENTION_EVENTS];
      bool any_param_has_null = false;
      for (int64_t i = 0; i < param_count; i++) {
        vecs[i] = aggr_info.param_exprs_.at(i)->get_vector(eval_ctx);
        any_param_has_null = any_param_has_null || vecs[i]->has_null();
      }
      // Init cell once per batch (same agg_cell for all rows in non-hash path).
      // NOTE: event_count is set lazily on the first contributing row, so a group
      // whose every row is skipped (NULL in some arg) keeps event_count == 0 and
      // is emitted as NULL by collect_group_result.
      setup_initial_value(agg_ctx, agg_cell, agg_col_id);
      uint32_t &bitmap = cell_bitmap(agg_cell);
      const int8_t param_count8 = static_cast<int8_t>(param_count);
      if (row_sel.is_empty()) {
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); idx++) {
          if (skip.at(idx)) { continue; }
          if (bitmap_saturated(bitmap, param_count8)) { break; }
          if (any_param_has_null) {
            bool row_has_null = false;
            for (int64_t i = 0; i < param_count; i++) {
              if (vecs[i]->is_null(idx)) {
                row_has_null = true;
                break;
              }
            }
            if (row_has_null) { continue; }
          }
          set_event_count_once(agg_cell, static_cast<int32_t>(param_count));
          for (int64_t i = 0; i < param_count; i++) {
            const char *payload = nullptr;
            int32_t len = 0;
            vecs[i]->get_payload(idx, payload, len);
            for (int32_t b = 0; b < len; b++) {
              if (payload[b] != 0) {
                bitmap |= (1U << i);
                break;
              }
            }
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          const int64_t idx = row_sel.index(i);
          if (bitmap_saturated(bitmap, param_count8)) { break; }
          if (any_param_has_null) {
            bool row_has_null = false;
            for (int64_t p = 0; p < param_count; p++) {
              if (vecs[p]->is_null(idx)) {
                row_has_null = true;
                break;
              }
            }
            if (row_has_null) { continue; }
          }
          set_event_count_once(agg_cell, static_cast<int32_t>(param_count));
          for (int64_t p = 0; p < param_count; p++) {
            const char *payload = nullptr;
            int32_t len = 0;
            vecs[p]->get_payload(idx, payload, len);
            for (int32_t b = 0; b < len; b++) {
              if (payload[b] != 0) {
                bitmap |= (1U << p);
                break;
              }
            }
          }
        }
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
    UNUSED(agg_cell_len);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));

    int32_t param_count = cell_event_count(agg_cell);
    if (param_count == 0) {
      res_vec->set_null(output_idx);
    } else {
      uint32_t bitmap = cell_bitmap(agg_cell);
      bool event_0 = (bitmap & 1) != 0;

      ObIArrayType *arr_obj = nullptr;
      const uint16_t meta_id = agg_expr.obj_meta_.get_subschema_id();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(agg_ctx.eval_ctx_);
      if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_alloc_g.get_allocator(),
                                                         agg_ctx.eval_ctx_, meta_id,
                                                         arr_obj, false))) {
        SQL_LOG(WARN, "construct array obj failed", K(ret));
      } else {
        ObArrayFixedSize<int8_t> *fixed_arr = static_cast<ObArrayFixedSize<int8_t> *>(arr_obj);
        if (agg_ctx.aggr_infos_.at(agg_col_id).retention_is_final_ && !event_0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < param_count; i++) {
            if (OB_FAIL(fixed_arr->push_back(int8_t(0)))) {
              SQL_LOG(WARN, "append to array failed", K(ret));
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < param_count; i++) {
            int8_t val = ((bitmap >> i) & 1) ? int8_t(1) : int8_t(0);
            if (OB_FAIL(fixed_arr->push_back(val))) {
              SQL_LOG(WARN, "append to array failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ColumnFmt>(
                  arr_obj, agg_expr, agg_ctx.eval_ctx_, res_vec, output_idx))) {
            SQL_LOG(WARN, "set array result failed", K(ret));
          }
        }
      }
    }
    return ret;
  }

  // Hash group by path: each row in row_sel may belong to a different group.
  virtual int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                         RowSelector &row_sel, const int64_t batch_size,
                                         const int32_t agg_col_id) override
  {
    int ret = OB_SUCCESS;
    UNUSED(batch_size);
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    sql::ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;

    if (is_merge_stage(aggr_info)) {
      ObIVector *vec = aggr_info.param_exprs_.at(0)->get_vector(eval_ctx);
      const uint16_t meta_id = aggr_info.param_exprs_.at(0)->obj_meta_.get_subschema_id();
      for (int64_t i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        const int64_t row_idx = row_sel.index(i);
        if (vec->is_null(row_idx)) { continue; }
        char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[row_idx]);
        if (bitmap_saturated(cell_bitmap(agg_cell), cell_event_count(agg_cell))) { continue; }
        setup_initial_value(agg_ctx, agg_cell, agg_col_id);
        const char *payload = nullptr;
        int32_t len = 0;
        vec->get_payload(row_idx, payload, len);
        if (OB_FAIL(merge_partial_array(eval_ctx, meta_id, payload, len, agg_cell))) {
          SQL_LOG(WARN, "merge partial array failed", K(ret));
        }
      }
    } else {
      // Stage-1: pre-fetch vectors and null metadata once per batch.
      int64_t param_count = MIN(aggr_info.param_exprs_.count(), MAX_RETENTION_EVENTS);
      ObIVector *vecs[MAX_RETENTION_EVENTS];
      bool any_param_has_null = false;
      for (int64_t p = 0; p < param_count; p++) {
        vecs[p] = aggr_info.param_exprs_.at(p)->get_vector(eval_ctx);
        any_param_has_null = any_param_has_null || vecs[p]->has_null();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        const int64_t row_idx = row_sel.index(i);
        char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[row_idx]);
        setup_initial_value(agg_ctx, agg_cell, agg_col_id);
        if (bitmap_saturated(cell_bitmap(agg_cell), static_cast<int8_t>(param_count))) { continue; }
        if (any_param_has_null) {
          bool row_has_null = false;
          for (int64_t p = 0; p < param_count; p++) {
            if (vecs[p]->is_null(row_idx)) {
              row_has_null = true;
              break;
            }
          }
          if (row_has_null) { continue; }
        }
        set_event_count_once(agg_cell, static_cast<int32_t>(param_count));
        uint32_t &bitmap = cell_bitmap(agg_cell);
        for (int64_t p = 0; p < param_count; p++) {
          const char *payload = nullptr;
          int32_t len = 0;
          vecs[p]->get_payload(row_idx, payload, len);
          for (int32_t b = 0; b < len; b++) {
            if (payload[b] != 0) {
              bitmap |= (1U << p);
              break;
            }
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
    int ret = OB_ERR_UNEXPECTED;
    UNUSEDx(agg_ctx, agg_col_idx, group_row, rollup_row, cur_rollup_group_idx, max_group_cnt);
    SQL_LOG(WARN, "retention rollup must use hash rollup", K(ret));
    return ret;
  }

  TO_STRING_KV("aggregate", "retention");

private:
  void setup_initial_value(RuntimeContext &agg_ctx, char *agg_cell, const int32_t agg_col_id)
  {
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    if (OB_UNLIKELY(!not_nulls.at(agg_col_id))) {
      cell_bitmap(agg_cell) = 0;
      cell_event_count(agg_cell) = 0;
      not_nulls.set(agg_col_id);
    }
  }

  // N is written once per cell (first contributing row sets it).
  static inline void set_event_count_once(char *agg_cell, int32_t n)
  {
    if (cell_event_count(agg_cell) == 0) {
      cell_event_count(agg_cell) = static_cast<int8_t>(n);
    }
  }

  static int merge_partial_array(sql::ObEvalCtx &eval_ctx, const uint16_t meta_id,
                                 const char *payload, const int32_t len, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
    ObIArrayType *arr_obj = nullptr;
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_alloc_g.get_allocator(), eval_ctx,
                                                 meta_id, ObString(len, payload),
                                                 arr_obj))) {
      SQL_LOG(WARN, "get array obj failed", K(ret));
    } else {
      ObArrayFixedSize<int8_t> *fixed_arr =
          static_cast<ObArrayFixedSize<int8_t> *>(arr_obj);
      int64_t elem_count = MIN(static_cast<int64_t>(fixed_arr->size()), MAX_RETENTION_EVENTS);
      set_event_count_once(agg_cell, static_cast<int32_t>(elem_count));
      uint32_t &bitmap = cell_bitmap(agg_cell);
      for (int64_t i = 0; i < elem_count; i++) {
        if ((*fixed_arr)[i] != 0) {
          bitmap |= (1U << i);
        }
      }
    }
    return ret;
  }
};

} // namespace aggregate
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_RETENTION_H_
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

#ifndef OCEANBASE_SHARE_AGGREGATE_WINDOW_FUNNEL_H_
#define OCEANBASE_SHARE_AGGREGATE_WINDOW_FUNNEL_H_

#include "share/aggregate/iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

enum WindowFunnelMode {
  DEFAULT = 0,
  STRICT_DEDUPLICATION = 1,
  MAX_MODE = 2,
};

// example of window_funnel cell in aggr_row
// ---------------------------------------------
// ...| int64_t * ,  bitvector * |...
// ---------------------------------------------

/*
Window Funnel (single pass, events are pre-sorted by time)

Algorithm
- Input: time-ordered events; each event matches exactly one funnel condition idx in [0..N-1].
- State:
  - time_records[i] : start timestamp of a valid path that reaches condition i, or -1 if unreachable.
  - has_conditions[i]: whether condition i has ever been reached (optional flag for output/reporting).
- Init:
  - time_records[*] = -1
  - has_conditions[*] = 0
- For each event (idx, cur_time):
  - If idx == 0:
      time_records[0] = cur_time                      // always keep the latest start
      has_conditions[0] = 1
    Else:
      // can advance only if previous stage exists and the whole chain is within window
      if time_records[idx - 1] != -1 &&
         cur_time - time_records[idx - 1] <= W:
          time_records[idx] = time_records[idx - 1]   // inherit the same chain start time
          has_conditions[idx] = 1
- Result:
  - Scan time_records from back to front, find the largest i with time_records[i] != -1.
    max_level = i + 1 (or 0 if none).

Example (3-step funnel: login->browse->pay, W=1000)
Events: (t, idx)
  (100,0) login
  (200,1) browse
  (500,0) login
  (600,1) browse
  (1200,2) pay

State transitions (time_records):
  init            [-1,  -1,  -1]
  login(100)      [100, -1,  -1]   // start A
  browse(200)     [100, 100, -1]   // A continues (200-100 <= 1000)
  login(500)      [500, 100, -1]   // start B overwrites start A
  browse(600)     [500, 500, -1]   // B continues
  pay(1200)       [500, 500, 500]  // B completes (1200-500 <= 1000)
max_level = 3
Path A fails (1200-100 > 1000), path B succeeds (1200-500 <= 1000).

Correctness sketch
Key properties
1) Stage 0 always stores the latest condition-0 timestamp.
   The algorithm always tries to build paths from the most recent start.
2) Advancing to stage i copies the same start time from stage i-1:
     time_records[i] = time_records[i-1]
   Therefore the window check always measures the total span from the chain start.
3) Reached stages never "degrade":
   once time_records[i] becomes non -1, it is never reset to -1 (only updated to another valid start).

Proof idea (by considering a longest valid path)
Assume there exists a longest valid path P reaching stage k:
  e0 at t0 -> e1 at t1 -> ... -> ek at tk, with tk - t0 <= W.
We show that after processing all events, time_records[k] != -1.

Case 1: e0(t0) is the last condition-0 event.
- When processing e0: time_records[0] = t0.
- For each subsequent ei(ti): since ti - t0 <= tk - t0 <= W, the check passes and
  time_records[i] becomes t0.
- In particular at ek(tk): time_records[k] becomes t0, hence != -1.

Case 2: there exists a later condition-0 event at t0' > t0, so time_records[0] is updated to t0'.
- If starting from t0' can also reach stage k within W, then time_records[k] will be set by that newer path.
- Otherwise, the update to t0' cannot invalidate an already-reached stage k:
  updates happen only when the window check passes; if the newer start cannot reach some later stage,
  those later time_records entries are not overwritten by an invalid start, so any previously stored
  valid value for time_records[k] is preserved.

Therefore, whenever a valid path to stage k exists, the algorithm finishes with time_records[k] != -1.
*/
template <VecValueTypeClass vec_tc>
struct WindowFunnelAggCell {
  int64_t calc_count_;
  RTCType<vec_tc> *time_records_;
  ObBitVector *has_conditions_;
  WindowFunnelAggCell() : calc_count_(0), time_records_(nullptr), has_conditions_(nullptr) {}

  OB_INLINE int init_cell(RuntimeContext &agg_ctx, ObAggrInfo &aggr_info, int32_t condition_count)
  {
    int ret = OB_SUCCESS;
    int64_t buf_size = 0;
    void *buf = nullptr;
    buf_size += sizeof(RTCType<vec_tc>) * condition_count; // time_records_
    buf_size += ObBitVector::memory_size(condition_count); // has_conditions_
    if (OB_ISNULL(buf = agg_ctx.allocator_.alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      time_records_ = static_cast<RTCType<vec_tc> *>(buf);
      MEMSET(time_records_, -1, sizeof(RTCType<vec_tc>) * condition_count);
      has_conditions_ = to_bit_vector((char *)(buf) + sizeof(RTCType<vec_tc>) * condition_count);
      has_conditions_->reset(condition_count);
      calc_count_ = 0;
    }
    return ret;
  }

  OB_INLINE int update_time_records(int32_t cond_idx, int32_t condition_count,
                                     RTCType<vec_tc> cur_time, int64_t time_window)
  {
    int ret = OB_SUCCESS;
    has_conditions_->set(cond_idx);
    bool check_in_timewindow = false;
    if (cond_idx == 0) {
      time_records_[cond_idx] = cur_time;
      calc_count_ = max(calc_count_, cond_idx + 1);
    } else {
      RTCType<vec_tc> first_time = time_records_[cond_idx - 1];
      if (first_time == -1) {
        // do nothing
      } else if (OB_FAIL(is_in_timewindow(cur_time, first_time, time_window, check_in_timewindow))) {
        SQL_LOG(WARN, "is_in_timewindow failed", K(ret));
      } else if (check_in_timewindow) {
        time_records_[cond_idx] = first_time;
        calc_count_ = max(calc_count_, cond_idx + 1);
      }
    }
    has_conditions_->set(cond_idx);
    return ret;
  }

  OB_INLINE int is_in_timewindow(RTCType<vec_tc> cur_time, RTCType<vec_tc> first_time, int64_t time_window, bool &check_in_timewindow)
  {
    int ret = OB_SUCCESS;
    check_in_timewindow = false;
    switch (vec_tc) {
      case VEC_TC_INTEGER:
      case VEC_TC_UINTEGER:
      case VEC_TC_DATE: {
        check_in_timewindow = (cur_time - first_time <= time_window);
        break;
      }
      case VEC_TC_DATETIME:
      case VEC_TC_TIME: {
        // time_window is in seconds, need to convert to microseconds
        check_in_timewindow = (cur_time - first_time <= time_window * 1000000);
        break;
      }
      case VEC_TC_MYSQL_DATETIME: {
        int64_t cur_datetime = 0;
        int64_t first_datetime = 0;
        ObDateSqlMode date_sql_mode;
        if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime(cur_time, cur_datetime, date_sql_mode))) {
          SQL_LOG(WARN, "mdatetime_to_datetime failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime(first_time, first_datetime, date_sql_mode))) {
          SQL_LOG(WARN, "mdatetime_to_datetime failed", K(ret));
        } else {
          check_in_timewindow = (cur_datetime - first_datetime <= time_window * 1000000);
        }
        break;
      }
      case VEC_TC_MYSQL_DATE: {
        int32_t cur_date = 0;
        int32_t first_date = 0;
        ObDateSqlMode date_sql_mode;
        if (OB_FAIL(ObTimeConverter::mdate_to_date(cur_time, cur_date, date_sql_mode))) {
          SQL_LOG(WARN, "mdate_to_date failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::mdate_to_date(first_time, first_date, date_sql_mode))) {
          SQL_LOG(WARN, "mdate_to_date failed", K(ret));
        } else {
          check_in_timewindow = (cur_date - first_date <= time_window);
        }
        break;
      }
    }
    return ret;
  }

  OB_INLINE int get_current_max_stage(int32_t condition_count, int32_t &max_level)
  {
    int ret = OB_SUCCESS;
    max_level = 0;
    for (int32_t i = condition_count - 1; i >= 0; i--) {
      if (time_records_[i] != -1) {
        max_level = i + 1;
        break;
      }
    }
    return ret;
  }
};

template<VecValueTypeClass vec_tc, WindowFunnelMode mode = DEFAULT>
class WindowFunnelAggregate final
    : public BatchAggregateWrapper<WindowFunnelAggregate<vec_tc, mode>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = vec_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_INTEGER;

  using AggCellType = WindowFunnelAggCell<vec_tc>;
  using BaseClass = BatchAggregateWrapper<WindowFunnelAggregate<vec_tc>>;

public:
  WindowFunnelAggregate()
      : time_window_(-1),
        condition_count_(0) {}

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id,
          ObIAllocator &allocator) override;

  OB_INLINE int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
    const bool is_null, const char *data, const int32_t data_len,
    int32_t agg_col_idx, char *agg_cell) override
  {
    UNUSEDx(agg_ctx, row_num, batch_size, is_null, data, data_len, agg_col_idx, agg_cell);
    return OB_ERR_UNEXPECTED;
  }

  // Called by WindowFunnelWrapper to evaluate events list from extra result
  OB_INLINE int eval_events_list(RuntimeContext &agg_ctx, int32_t agg_col_idx, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_idx);
    WindowFunnelVecExtraResult *extra = reinterpret_cast<WindowFunnelVecExtraResult *>(
      agg_ctx.get_extra_window_funnel_store(agg_col_idx, (const char *)agg_cell));
    WindowFunnelAggCell<vec_tc> *cell =
        reinterpret_cast<WindowFunnelAggCell<vec_tc> *>(const_cast<char *>(agg_cell));
    if (OB_ISNULL(cell->time_records_) && OB_FAIL(cell->init_cell(agg_ctx, aggr_info, condition_count_))) {
      SQL_LOG(WARN, "init cell failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < extra->event_list_size_; i++) {
        int32_t event_idx = extra->event_list_[i].event_idx_;
        RTCType<vec_tc> cur_time = *reinterpret_cast<const RTCType<vec_tc> *>(&(extra->event_list_[i].time_));
        if (mode == STRICT_DEDUPLICATION) {
          if (cell->has_conditions_->at(event_idx)) {
            break;
          }
        }
        if (OB_FAIL(cell->update_time_records(event_idx, condition_count_, cur_time, time_window_))) {
          SQL_LOG(WARN, "add event failed", K(ret));
        }
      }
    }
    return ret;
  }

  template <typename ColumnFmt, typename ConditionFmt>
  OB_INLINE int add_batch_rows(RuntimeContext &agg_ctx, const sql::ObBitVector &skip,
                     const sql::EvalBound &bound, const ObExpr &time_expr, const ObExpr &cond_expr,
                     const int32_t agg_col_id, char *agg_cell, const RowSelector row_sel)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ColumnFmt *time_columns = static_cast<ColumnFmt *>(time_expr.get_vector(ctx));
    ConditionFmt *cond_columns = static_cast<ConditionFmt *>(cond_expr.get_vector(ctx));
    OB_ASSERT(!time_columns->has_null());
    WindowFunnelAggCell<vec_tc> *cell = reinterpret_cast<WindowFunnelAggCell<vec_tc> *>(agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    for (int64_t idx = bound.start(); idx < bound.end(); idx++) {
      RTCType<vec_tc> cur_time = *reinterpret_cast<const RTCType<vec_tc> *>(time_columns->get_payload(idx));
      int32_t cond_idx = cond_columns->get_int(idx);
      if (mode == STRICT_DEDUPLICATION) {
        if (cell->has_conditions_->at(cond_idx)) {
          break;
        }
      }
      if (OB_FAIL(cell->update_time_records(cond_idx, condition_count_, cur_time, time_window_))) {
        SQL_LOG(WARN, "add event failed", K(ret));
      }
    }
    return ret;
  }

#define DISPATCH_VECTOR_IN_ARG_FORMAT(                                   \
        func_name, left_vec, r_format)                                   \
switch (r_format) {                                                      \
  case VEC_FIXED: {                                                      \
    ret = func_name<left_vec, fixlen_fmt<VEC_TC_INTEGER>>(               \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
    break;                                                               \
  }                                                                      \
  case VEC_DISCRETE: {                                                   \
    ret = func_name<left_vec, discrete_fmt<VEC_TC_INTEGER>>(             \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
    break;                                                               \
  }                                                                      \
  case VEC_CONTINUOUS: {                                                 \
    ret = func_name<left_vec, continuous_fmt<VEC_TC_INTEGER>>(           \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
    break;                                                               \
  }                                                                      \
  case VEC_UNIFORM: {                                                    \
    ret = func_name<left_vec, uniform_fmt<VEC_TC_INTEGER, false>>(       \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
    break;                                                               \
  }                                                                      \
  case VEC_UNIFORM_CONST: {                                              \
    ret = func_name<left_vec, uniform_fmt<VEC_TC_INTEGER, true>>(        \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
    break;                                                               \
  }                                                                      \
  default: {                                                             \
    ret = func_name<left_vec, ObVectorBase>(                             \
      agg_ctx, skip, bound, *time_expr, *cond_expr, agg_col_id, agg_cell, row_sel);  \
  }                                                                      \
}
  OB_INLINE int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
    const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
    const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;

    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(row_sel.is_empty());
    OB_ASSERT(bound.get_all_rows_active());
    WindowFunnelAggCell<vec_tc> *cell = reinterpret_cast<WindowFunnelAggCell<vec_tc> *>(agg_cell);
    if (OB_ISNULL(cell->time_records_) && OB_FAIL(cell->init_cell(agg_ctx, agg_ctx.aggr_infos_.at(agg_col_id), condition_count_))) {
      SQL_LOG(WARN, "init cell failed", K(ret));
    } else {
      sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
      ObExpr *time_expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(WINDOW_FUNNEL_PSEUDO_TIME_EXPR_IDX);
      ObExpr *cond_expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(WINDOW_FUNNEL_PSEUDO_EVENT_IDX_EXPR_IDX);
      VectorFormat time_fmt = time_expr->get_format(ctx);
      VectorFormat condition_fmt = cond_expr->get_format(ctx);
      switch (time_fmt) {
      case common::VEC_UNIFORM: {
        DISPATCH_VECTOR_IN_ARG_FORMAT(add_batch_rows, ObUniformFormat<false>, condition_fmt);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        DISPATCH_VECTOR_IN_ARG_FORMAT(add_batch_rows, ObUniformFormat<true>, condition_fmt);
        break;
      }
      case common::VEC_FIXED: {
        DISPATCH_VECTOR_IN_ARG_FORMAT(add_batch_rows, fixlen_fmt<vec_tc>, condition_fmt);
        break;
      }
      case common::VEC_DISCRETE: {
        DISPATCH_VECTOR_IN_ARG_FORMAT(add_batch_rows, discrete_fmt<vec_tc>, condition_fmt);
        break;
      }
      case common::VEC_CONTINUOUS: {
        DISPATCH_VECTOR_IN_ARG_FORMAT(add_batch_rows, continuous_fmt<vec_tc>, condition_fmt);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected time fmt", K(time_fmt), K(*time_expr));
      }
      }
    }
    return ret;
  }
#undef DISPATCH_VECTOR_IN_ARG_FORMAT
  template<typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                          const int32_t agg_col_id, const char *data,
                          const int32_t data_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(data_len);

    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();

    WindowFunnelAggCell<vec_tc> *cell =
        reinterpret_cast<WindowFunnelAggCell<vec_tc> *>(const_cast<char *>(data));
    if (OB_SUCC(ret)) {
      ColumnFormat &columns = *static_cast<ColumnFormat *>(agg_expr.get_vector(ctx));
      columns.set_int(output_idx, static_cast<int32_t>(cell->calc_count_));
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx, AggrRowPtr group_row,
    AggrRowPtr rollup_row, int64_t cur_rollup_group_idx, int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_ERR_UNEXPECTED;
    UNUSEDx(agg_ctx, agg_col_idx, group_row, rollup_row, cur_rollup_group_idx, max_group_cnt);
    SQL_LOG(WARN, "window_funnel rollup must use hash rollup", K(ret));
    return ret;
  }

  TO_STRING_KV("aggregate", "window_funnel");

private:
  int64_t time_window_;
  int32_t condition_count_;
};

} // namespace aggregate
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_WINDOW_FUNNEL_H_

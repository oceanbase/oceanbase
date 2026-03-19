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

#define USING_LOG_PREFIX SQL_ENG

#include "share/aggregate/window_funnel.h"
#include "share/aggregate/processor.h"
#include "share/datum/ob_datum_util.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

template<VecValueTypeClass vec_tc, WindowFunnelMode mode>
int WindowFunnelAggregate<vec_tc, mode>::init(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                         ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
  // param_exprs: (timestamp, time_window, mode, condition1, condition2, ...)
  if (OB_UNLIKELY(aggr_info.param_exprs_.count() < 4)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "window_funnel requires at least 4 parameters", K(ret), K(aggr_info.param_exprs_.count()));
  } else {
    int64_t mock_skip_data = 0;
    ObBitVector *skip = to_bit_vector(&mock_skip_data);
    EvalBound bound(1, true);

    ObExpr *time_window_expr = aggr_info.param_exprs_.at(WINDOW_FUNNEL_WINDOW_EXPR_IDX);
    if (OB_ISNULL(time_window_expr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "time_window_expr is null", K(ret));
    } else if (OB_UNLIKELY(!time_window_expr->is_const_expr() || !time_window_expr->obj_meta_.is_integer_type())) {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "time_window_expr must be constant", K(ret));
    } else if (OB_FAIL(time_window_expr->eval_vector(agg_ctx.eval_ctx_, *skip, bound))) {
      SQL_LOG(WARN, "eval time_window_expr failed", K(ret));
    } else {
      time_window_ = time_window_expr->get_vector(agg_ctx.eval_ctx_)->get_int(0);
      condition_count_ = aggr_info.param_exprs_.count() - WINDOW_FUNNEL_CONDITION_START_IDX;
    }
  }


  return ret;
}

namespace helper {

// Specialized init function for WindowFunnelAggregate that creates WindowFunnelWrapper
template <typename AggType>
int init_window_funnel_agg(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                           ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  void *agg_buf = nullptr, *wrapper_buf = nullptr;
  AggType *inner_agg = nullptr;
  WindowFunnelWrapper<AggType> *wf_wrapper = nullptr;

  if (OB_ISNULL(agg_buf = allocator.alloc(sizeof(AggType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FALSE_IT(inner_agg = new (agg_buf) AggType())) {
  } else if (OB_ISNULL(wrapper_buf = allocator.alloc(sizeof(WindowFunnelWrapper<AggType>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FALSE_IT(wf_wrapper = new (wrapper_buf) WindowFunnelWrapper<AggType>())) {
  } else {
    wf_wrapper->set_inner_aggregate(inner_agg);
    agg = wf_wrapper;
    if (OB_FAIL(agg->init(agg_ctx, agg_col_id, allocator))) {
      SQL_LOG(WARN, "init aggregate failed", K(ret));
    }
  }
  return ret;
}

int init_window_funnel_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                 ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_WINDOW_FUNNEL_AGGREGATE_CASE(vec_tc, mode)                                             \
  case (vec_tc): {                                                                                  \
    if (mode == WindowFunnelMode::DEFAULT) {                                                        \
      ret = init_window_funnel_agg<WindowFunnelAggregate<vec_tc, WindowFunnelMode::DEFAULT>>(       \
          agg_ctx, agg_col_id, allocator, agg);                                                     \
    } else {                                                                                        \
      ret = init_window_funnel_agg<WindowFunnelAggregate<vec_tc, WindowFunnelMode::STRICT_DEDUPLICATION>>( \
          agg_ctx, agg_col_id, allocator, agg);                                                     \
    }                                                                                               \
  } break

  int ret = OB_SUCCESS;
  agg = nullptr;

  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  if (OB_ISNULL(aggr_info.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid null expr", K(ret));
  } else if (OB_UNLIKELY(aggr_info.param_exprs_.count() < 4)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "window_funnel requires at least 4 parameters (timestamp + window + mode + conditions)",
            K(ret), K(aggr_info.param_exprs_.count()));
  } else {
    // 获取第一个参数（timestamp列）的类型
    const ObExpr *timestamp_expr = aggr_info.param_exprs_.at(WINDOW_FUNNEL_TIMESTAMP_EXPR_IDX);
    VecValueTypeClass timestamp_vec_tc =
        get_vec_value_tc(timestamp_expr->datum_meta_.type_,
                        timestamp_expr->datum_meta_.scale_,
                        timestamp_expr->datum_meta_.precision_);

    const ObExpr *mode_expr = aggr_info.param_exprs_.at(WINDOW_FUNNEL_MODE_EXPR_IDX);
    WindowFunnelMode mode = WindowFunnelMode::DEFAULT;
    if (!mode_expr->is_const_expr() || !mode_expr->obj_meta_.is_string_type()) {
      ret = OB_INVALID_ARGUMENT;
      SQL_LOG(WARN, "mode must be constant string", K(ret));
    } else {
      int64_t mock_skip_data = 0;
      ObBitVector *skip = to_bit_vector(&mock_skip_data);
      EvalBound bound(1, true);
      if (OB_FAIL(mode_expr->eval_vector(agg_ctx.eval_ctx_, *skip, bound))) {
        SQL_LOG(WARN, "eval mode failed", K(ret));
      } else {
        ObString mode_str = mode_expr->get_vector(agg_ctx.eval_ctx_)->get_string(0);
        if (mode_str.case_compare("strict_deduplication") == 0) {
          mode = WindowFunnelMode::STRICT_DEDUPLICATION;
        } else if (mode_str.case_compare("default") == 0) {
          mode = WindowFunnelMode::DEFAULT;
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unknown mode", K(ret), K(mode_str));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      switch (timestamp_vec_tc) {
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_INTEGER, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_UINTEGER, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_DATETIME, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_DATE, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_TIME, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_MYSQL_DATETIME, mode);
        INIT_WINDOW_FUNNEL_AGGREGATE_CASE(VEC_TC_MYSQL_DATE, mode);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected timestamp type for window_funnel",
                K(ret), K(timestamp_vec_tc), K(timestamp_expr->datum_meta_.type_));
      }
      }
    }
  }

  return ret;

#undef INIT_WINDOW_FUNNEL_AGGREGATE_CASE
}

} // namespace helper

} // namespace aggregate
} // namespace share
} // namespace oceanbase

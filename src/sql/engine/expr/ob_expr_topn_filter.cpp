/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_EXE
#include "sql/engine/expr/ob_expr_topn_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"

namespace oceanbase
{
namespace sql
{
template <typename ResVec>
static inline int proc_by_pass(ResVec *res_vec, const ObBitVector &skip, const EvalBound &bound,
                               int64_t &valid_cnt, bool calc_valid_cnt = false);

template <>
inline int proc_by_pass<IntegerUniVec>(IntegerUniVec *res_vec, const ObBitVector &skip,
                                       const EvalBound &bound, int64_t &valid_cnt,
                                       bool calc_valid_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(calc_valid_cnt)) {
    valid_cnt = 0;
    if (OB_FAIL(ObBitVector::flip_foreach(
            skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
              ++valid_cnt;
              res_vec->set_int(idx, 1);
              return OB_SUCCESS;
            }))) {
      LOG_WARN("fail to do for each operation", K(ret));
    }
  } else {
    if (OB_FAIL(ObBitVector::flip_foreach(
            skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
              res_vec->set_int(idx, 1);
              return OB_SUCCESS;
            }))) {
      LOG_WARN("fail to do for each operation", K(ret));
    }
  }
  return ret;
}

template <>
inline int proc_by_pass<IntegerFixedVec>(IntegerFixedVec *res_vec, const ObBitVector &skip,
                                         const EvalBound &bound, int64_t &valid_cnt,
                                         bool calc_valid_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data + bound.start(), 1, (bound.range_size() * res_vec->get_length(0)));
  if (OB_LIKELY(calc_valid_cnt)) {
    valid_cnt = bound.range_size() - skip.accumulate_bit_cnt(bound);
  }
  return ret;
}

const int64_t ObExprTopNFilterContext::ROW_COUNT_CHECK_INTERVAL = 255;
const int64_t ObExprTopNFilterContext::EVAL_TIME_CHECK_INTERVAL = 63;

ObExprTopNFilterContext::~ObExprTopNFilterContext()
{
  if (OB_NOT_NULL(topn_filter_msg_)) {
    // topn_filter_msg_ is got from PX_P2P_DH map
    // do not destroy it, because other worker threads may not start yet
    topn_filter_msg_->dec_ref_count();
    topn_filter_msg_ = nullptr;
  }
  cmp_funcs_.reset();
  int ret = OB_SUCCESS;
  double filter_rate = filter_count_ / double(check_count_ + 1);
  LOG_TRACE("[TopN Filter] print the filter rate", K(total_count_), K(check_count_),
            K(filter_count_), K(filter_rate));
}

void ObExprTopNFilterContext::reset_for_rescan()
{
  state_ = FilterState::NOT_READY;
  if (OB_NOT_NULL(topn_filter_msg_)) {
    // topn_filter_msg_ is got from PX_P2P_DH map
    // do not destroy it, because other worker threads may not start yet
    topn_filter_msg_->dec_ref_count();
    topn_filter_msg_ = nullptr;
  }
  n_times_ = 0;
  n_rows_ = 0;
  flag_ = 0;
  is_first_ = true;
  slide_window_.reset_for_rescan();
}

// with perfect forwarding, we can write one state machine rather than
// write three state machines for eval/eval_batch/eval_vector interface
template <typename... Args>
int ObExprTopNFilterContext::state_machine(const ObExpr &expr, ObEvalCtx &ctx, Args &&... args)
{
  // state transformation of topn filter:
  //           each n epoch              check pass        filter rate < 0.5
  // NOT_READY ------------> CHECK_READY ------------> ENABLE ------------> DYNAMIC_DISABLE
  //           <------------                                  <------------
  //           check not pass                                after k windows

  int ret = OB_SUCCESS;
  bool result_filled = false;
  do {
    switch (state_) {
      case FilterState::NOT_READY: {
        ret = bypass(expr, ctx, std::forward<Args>(args)...);
        result_filled = true;
        break;
      }
      case FilterState::CHECK_READY: {
        if (OB_FAIL(check_filter_ready())) {
          LOG_WARN("fail to check filter ready", K(ret));
        }
        // result not filled, so while loop continues
        break;
      }
      case FilterState::ENABLE: {
        ret = do_process(expr, ctx, std::forward<Args>(args)...);
        result_filled = true;
        break;
      }
      case FilterState::DYNAMIC_DISABLE: {
        ret = bypass(expr, ctx, std::forward<Args>(args)...);
        result_filled = true;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state_");
        break;
      }
    }
  } while (!result_filled && OB_SUCC(ret));
  return ret;
}

// bypass interface for eval one row, for storege black filter
inline int ObExprTopNFilterContext::bypass(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  res.set_int(1);
  if (!dynamic_disable()) {
    // if msg not ready, add n_times_ and check ready every ROW_COUNT_CHECK_INTERVAL
    if ((++n_times_ > EVAL_TIME_CHECK_INTERVAL) || (++n_rows_ > ROW_COUNT_CHECK_INTERVAL)) {
      state_ = FilterState::CHECK_READY;
    }
  } else {
    // if msg is ready but dynamic disable, collect_sample_info so it can be enabled later
    (void)collect_sample_info(0, 1);
    if (!dynamic_disable()) {
      state_ = FilterState::ENABLE;
    }
  }
  return ret;
}

// bypass interface for eval_batch rows, for storege black filter
inline int ObExprTopNFilterContext::bypass(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, batch_size, [&](int64_t idx) __attribute__((always_inline)) {
            eval_flags.set(idx);
            results[idx].set_int(1);
            ++total_count;
            return OB_SUCCESS;
          }))) {
    LOG_WARN("failed to flip_foreach");
  } else if (FALSE_IT(total_count_ += total_count)) {
  } else if (!dynamic_disable()) {
    // if msg not ready, add n_times_ and check ready every ROW_COUNT_CHECK_INTERVAL
    n_rows_ += total_count;
    if ((++n_times_ > EVAL_TIME_CHECK_INTERVAL) || (n_rows_ > ROW_COUNT_CHECK_INTERVAL)) {
      state_ = FilterState::CHECK_READY;
    }
  } else {
    // if msg is ready but dynamic disable, collect_sample_info so it can be enabled later
    (void)collect_sample_info(0, total_count);
    if (!dynamic_disable()) {
      state_ = FilterState::ENABLE;
    }
  }
  return ret;
}

// bypass interface for eval_vector rows, for storege black filter
inline int ObExprTopNFilterContext::bypass(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  int64_t valid_cnt = 0;
  VectorFormat res_format = expr.get_format(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (VEC_UNIFORM == res_format) {
    IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
    ret = proc_by_pass(res_vec, skip, bound, valid_cnt, true /* calc_valid_cnt */);
  } else if (VEC_FIXED == res_format) {
    IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
    ret = proc_by_pass(res_vec, skip, bound, valid_cnt, true /* calc_valid_cnt */);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to proc_by_pass", K(res_format), K(ret));
  } else {
    total_count_ += valid_cnt;
    eval_flags.set_all(true);
    // if msg not ready, add n_times_ and check ready every ROW_COUNT_CHECK_INTERVAL
    if (!dynamic_disable()) {
      n_rows_ += valid_cnt;
      if ((++n_times_ > EVAL_TIME_CHECK_INTERVAL) || (n_rows_ > ROW_COUNT_CHECK_INTERVAL)) {
        state_ = FilterState::CHECK_READY;
      }
    } else {
      // if msg is ready but dynamic disable, collect_sample_info so it can be enabled later
      (void)collect_sample_info(0, valid_cnt);
      if (!dynamic_disable()) {
        state_ = FilterState::ENABLE;
      }
    }
  }
  return ret;
}

// bypass interface for storege white filter
inline int ObExprTopNFilterContext::bypass(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDynamicFilterExecutor &dynamic_filter,
                                           ObRuntimeFilterParams &params, bool &is_data_prepared)
{
  // for prepare storege white data stage
  dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
  return OB_SUCCESS;
}

// filter interface for eval one row, for storege black filter
inline int ObExprTopNFilterContext::do_process(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = topn_filter_msg_->filter_out_data(expr, ctx, *this, res);
  if (dynamic_disable()) {
    state_ = FilterState::DYNAMIC_DISABLE;
  }
  return ret;
}

// filter interface for eval_batch rows, for storege black filter
inline int ObExprTopNFilterContext::do_process(const ObExpr &expr, ObEvalCtx &ctx,
                                               const ObBitVector &skip, const int64_t batch_size)
{
  int ret = topn_filter_msg_->filter_out_data_batch(expr, ctx, skip, batch_size, *this);
  if (dynamic_disable()) {
    state_ = FilterState::DYNAMIC_DISABLE;
  }
  return ret;
}

// filter interface for eval_vector rows, for storege black filter
inline int ObExprTopNFilterContext::do_process(const ObExpr &expr, ObEvalCtx &ctx,
                                               const ObBitVector &skip, const EvalBound &bound)
{
  int ret = topn_filter_msg_->filter_out_data_vector(expr, ctx, skip, bound, *this);
  if (dynamic_disable()) {
    state_ = FilterState::DYNAMIC_DISABLE;
  }
  return ret;
}

// get filter date from topn filter msg to enable storege white filter
inline int ObExprTopNFilterContext::do_process(const ObExpr &expr, ObEvalCtx &ctx,
                                               ObDynamicFilterExecutor &dynamic_filter,
                                               ObRuntimeFilterParams &params,
                                               bool &is_data_prepared)
{
  // for prepare data stage
  int ret = OB_SUCCESS;
  if (OB_FAIL(topn_filter_msg_->prepare_storage_white_filter_data(dynamic_filter, ctx, params,
                                                                  is_data_prepared))) {
    LOG_WARN("fail to prepare_storage_white_filter_data", K(ret));
  } else {
    if (topn_filter_msg_->is_null_first(dynamic_filter.get_col_idx())) {
      dynamic_filter.cmp_func_ =
          expr.args_[dynamic_filter.get_col_idx()]->basic_funcs_->null_first_cmp_;
    } else {
      dynamic_filter.cmp_func_ =
          expr.args_[dynamic_filter.get_col_idx()]->basic_funcs_->null_last_cmp_;
    }
    LOG_TRACE("[TopN Filter] prepare white filter data succ", K(total_count_), K(filter_count_),
              K(check_count_), K(topn_filter_msg_->is_null_first(dynamic_filter.get_col_idx())));
  }
  return ret;
}

int ObExprTopNFilterContext::check_filter_ready()
{
  int ret = OB_SUCCESS;
  if (is_first_) {
    start_time_ = ObTimeUtility::current_time();
    is_first_ = false;
  }
  // try get msg
  ObP2PDatahubMsgBase *basic_msg = topn_filter_msg_;
  if (OB_ISNULL(basic_msg)) {
    if (OB_FAIL(PX_P2P_DH.atomic_get_msg(topn_filter_key_, basic_msg))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get msg", K(ret));
      }
    } else {
      topn_filter_msg_ = static_cast<ObPushDownTopNFilterMsg *>(basic_msg);
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(basic_msg)) {
    LOG_TRACE("[TopN Filter] succ get msg from P2PDH", K(topn_filter_key_), K(basic_msg),
              K(total_count_));
  }
  // try check msg ready
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(topn_filter_msg_) && topn_filter_msg_->check_ready()) {
      ready_time_ = ObTimeUtility::current_time();
      state_ = FilterState::ENABLE;
      slide_window_.start_to_work();
    } else {
      state_ = FilterState::NOT_READY;
    }
    n_times_ = 0;
    n_rows_ = 0;
  }
  return ret;
}

int ObExprTopNFilter::calc_result_typeN(ObExprResType &type, ObExprResType *types_stack,
                                        int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

int ObExprTopNFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_topn_filter;
  rt_expr.eval_batch_func_ = eval_topn_filter_batch;
  rt_expr.eval_vector_func_ = eval_topn_filter_vector;
  return ret;
}

int ObExprTopNFilter::eval_topn_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  uint64_t expr_ctx_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprTopNFilterContext *topn_filter_ctx = nullptr;
  if (OB_ISNULL(topn_filter_ctx = static_cast<ObExprTopNFilterContext *>(
                    exec_ctx.get_expr_op_ctx(expr_ctx_id)))) {
    // topn filter ctx may be null in das.
    res.set_int(1);
  } else {
    ret = topn_filter_ctx->state_machine(expr, ctx, res);
  }
  return ret;
}

int ObExprTopNFilter::eval_topn_filter_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  uint64_t expr_ctx_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprTopNFilterContext *topn_filter_ctx = nullptr;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  if (OB_ISNULL(topn_filter_ctx = static_cast<ObExprTopNFilterContext *>(
                    exec_ctx.get_expr_op_ctx(expr_ctx_id)))) {
    // topn filter ctx may be null in das.
    if (OB_FAIL(ObBitVector::flip_foreach(
            skip, batch_size, [&](int64_t idx) __attribute__((always_inline)) {
              eval_flags.set(idx);
              results[idx].set_int(1);
              return OB_SUCCESS;
            }))) {}
  } else {
    ret = topn_filter_ctx->state_machine(expr, ctx, skip, batch_size);
  }
  return ret;
}

int ObExprTopNFilter::eval_topn_filter_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                              const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObExprTopNFilterContext *topn_filter_ctx = nullptr;
  if (OB_ISNULL(topn_filter_ctx = static_cast<ObExprTopNFilterContext *>(
                    ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
    // topn filter ctx may be null in das.
    int64_t valid_cnt = 0;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_UNIFORM == res_format) {
      IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
      ret = proc_by_pass(res_vec, skip, bound, valid_cnt, false /* calc_valid_cnt */);
    } else if (VEC_FIXED == res_format) {
      IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
      ret = proc_by_pass(res_vec, skip, bound, valid_cnt, false /* calc_valid_cnt */);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to proc_by_pass for das", K(res_format), K(ret));
    } else {
      eval_flags.set_all(true);
    }
  } else {
    ret = topn_filter_ctx->state_machine(expr, ctx, skip, bound);
  }
  return ret;
}

int ObExprTopNFilter::prepare_storage_white_filter_data(const ObExpr &expr,
                                                        ObDynamicFilterExecutor &dynamic_filter,
                                                        ObEvalCtx &eval_ctx,
                                                        ObRuntimeFilterParams &params,
                                                        bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  is_data_prepared = false;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = eval_ctx.exec_ctx_;
  ObExprTopNFilterContext *topn_filter_ctx = NULL;
  // get expr ctx from exec ctx
  if (OB_ISNULL(topn_filter_ctx = static_cast<ObExprTopNFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
    // topn filter ctx may be null in das.
    is_data_prepared = true;
    dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
  } else {
    ret = topn_filter_ctx->state_machine(expr, eval_ctx, dynamic_filter, params, is_data_prepared);
  }
  return ret;
}

int ObExprTopNFilter::update_storage_white_filter_data(const ObExpr &expr,
                                                       ObDynamicFilterExecutor &dynamic_filter,
                                                       ObEvalCtx &eval_ctx,
                                                       ObRuntimeFilterParams &params,
                                                       bool &is_update)
{
  int ret = OB_SUCCESS;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = eval_ctx.exec_ctx_;
  ObExprTopNFilterContext *topn_filter_ctx =
      static_cast<ObExprTopNFilterContext *>(exec_ctx.get_expr_op_ctx(op_id));
  if (OB_ISNULL(topn_filter_ctx)) {
    // in update stage, means prepare succ, topn_filter_ctx must not null
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("topn_filter_ctx must not null during update stage");
  } else if (OB_FAIL(topn_filter_ctx->topn_filter_msg_->update_storage_white_filter_data(
                 dynamic_filter, params, is_update))) {
    LOG_WARN("Failed to update_storage_white_filter_data");
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

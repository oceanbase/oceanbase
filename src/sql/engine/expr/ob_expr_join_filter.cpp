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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_join_filter.h"
#include "ob_expr_extract.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"


using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

#define FILL_BATCH_RESULT() \
      if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,\
        [&](int64_t idx) __attribute__((always_inline)) {\
          ++join_filter_ctx->n_times_;\
          int ret = OB_SUCCESS;\
          eval_flags.set(idx);\
          results[idx].set_int(is_match);\
          collect_sample_info(join_filter_ctx, is_match);\
          ++join_filter_ctx->total_count_;\
          return ret;\
        }))) {}
#define CHECK_MAX_WAIT_TIME() \
          int64_t cur_time = ObTimeUtility::current_time();\
          if (cur_time - join_filter_ctx->start_time_ >\
              join_filter_ctx->max_wait_time_ms_ * 1000) {\
            join_filter_ctx->need_wait_rf_ = false;\
            break;\
          } else {\
            ob_usleep(1000);\
          }

template <typename ResVec>
static int proc_if_das(ResVec *res_vec, const ObBitVector &skip, int64_t batch_size);

template <>
int proc_if_das<IntegerUniVec>(IntegerUniVec *res_vec, const ObBitVector &skip, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, batch_size, [&](int64_t idx) __attribute__((always_inline)) {
            res_vec->set_int(idx, 1);
            return OB_SUCCESS;
          }))) {
    LOG_WARN("fail to do for each operation", K(ret));
  }
  return ret;
}

template <>
int proc_if_das<IntegerFixedVec>(IntegerFixedVec *res_vec, const ObBitVector &skip,
                                 int64_t batch_size)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data, 1, (batch_size * res_vec->get_length(0)));
  return ret;
}

template <typename ResVec>
static int proc_by_pass(ResVec *res_vec, const ObBitVector &skip, int64_t batch_size,
                        ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx);

template <>
int proc_by_pass<IntegerUniVec>(IntegerUniVec *res_vec, const ObBitVector &skip, int64_t batch_size,
                                ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  int ret = OB_SUCCESS;
  int valid_cnt = 0;
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, batch_size, [&](int64_t idx) __attribute__((always_inline)) {
            ++valid_cnt;
            res_vec->set_int(idx, 1);
            return OB_SUCCESS;
          }))) {}
  join_filter_ctx->n_times_ += valid_cnt;
  join_filter_ctx->total_count_ += valid_cnt;
  ObExprJoinFilter::collect_sample_info_batch(*join_filter_ctx, 0, valid_cnt);
  return ret;
}

template <>
int proc_by_pass<IntegerFixedVec>(IntegerFixedVec *res_vec, const ObBitVector &skip,
                                  int64_t batch_size,
                                  ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data, 1, (batch_size * res_vec->get_length(0)));

  int64_t valid_cnt = batch_size - skip.accumulate_bit_cnt(batch_size);
  join_filter_ctx->n_times_ += valid_cnt;
  join_filter_ctx->total_count_ += valid_cnt;
  ObExprJoinFilter::collect_sample_info_batch(*join_filter_ctx, 0, valid_cnt);
  return ret;
}

ObExprJoinFilter::ObExprJoinFilterContext::~ObExprJoinFilterContext()
{
  if (OB_NOT_NULL(rf_msg_)) {
    // rf_msg_ is got from PX_P2P_DH map
    // do not destroy it, because other worker threads may not start yet
    rf_msg_->dec_ref_count();
  }
  hash_funcs_.reset();
  cmp_funcs_.reset();
  cur_row_.reset();
}

void ObExprJoinFilter::ObExprJoinFilterContext::reset_monitor_info()
{
  filter_count_ = 0;
  total_count_ = 0;
  check_count_ = 0;
  n_times_ = 0;
  ready_ts_ = 0;
  dynamic_disable_ = false;
  is_ready_ = false;
}

void ObExprJoinFilter::ObExprJoinFilterContext::collect_monitor_info(
    const int64_t filtered_rows_count,
    const int64_t check_rows_count,
    const int64_t total_rows_count)
{
  filter_count_ += filtered_rows_count;
  check_count_ += check_rows_count;
  total_count_ += total_rows_count;
}

ObExprJoinFilter::ObExprJoinFilter(ObIAllocator& alloc)
    : ObExprOperator(alloc,
                     T_OP_RUNTIME_FILTER,
                     "JOIN_BLOOM_FILTER",
                     MORE_THAN_ZERO,
                     VALID_FOR_GENERATED_COL,
                     NOT_ROW_DIMENSION,
                     INTERNAL_IN_MYSQL_MODE)
{
}
ObExprJoinFilter::~ObExprJoinFilter() {}

int ObExprJoinFilter::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
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

int ObExprJoinFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  switch(raw_expr.get_runtime_filter_type()) {
    case RuntimeFilterType::BLOOM_FILTER: {
      rt_expr.eval_func_ = eval_bloom_filter;
      rt_expr.eval_batch_func_ = eval_bloom_filter_batch;
      rt_expr.eval_vector_func_ = eval_bloom_filter_vector;
      break;
    }
    case RuntimeFilterType::RANGE: {
      rt_expr.eval_func_ = eval_range_filter;
      rt_expr.eval_batch_func_ = eval_range_filter_batch;
      rt_expr.eval_vector_func_ = eval_range_filter_vector;
      break;
    }
    case RuntimeFilterType::IN: {
      rt_expr.eval_func_ = eval_in_filter;
      rt_expr.eval_batch_func_ = eval_in_filter_batch;
      rt_expr.eval_vector_func_ = eval_in_filter_vector;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected raw expr type", K(ret));
    }
  }

  return ret;
}

int ObExprJoinFilter::check_rf_ready(
    ObExecContext &exec_ctx,
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *&rf_msg = join_filter_ctx->rf_msg_;
  if (join_filter_ctx->is_ready()) {
  } else if (OB_ISNULL(rf_msg)) {
    if (join_filter_ctx->need_wait_ready()) {
      while (!join_filter_ctx->is_ready() && OB_SUCC(exec_ctx.fast_check_status())) {
        if (OB_NOT_NULL(rf_msg)) {
#ifdef ERRSIM
          if (OB_FAIL(OB_E(EventTable::EN_PX_JOIN_FILTER_HOLD_MSG) OB_SUCCESS)) {
            LOG_WARN("join filter hold msg by design", K(ret));
            ob_usleep(80000000);
          }
#endif
          if (rf_msg->check_ready()) {
            break;
          }
          CHECK_MAX_WAIT_TIME();
        } else if (OB_FAIL(PX_P2P_DH.atomic_get_msg(join_filter_ctx->rf_key_, rf_msg))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            CHECK_MAX_WAIT_TIME();
          } else {
            LOG_WARN("fail to get msg", K(ret));
          }
        }
      }
    } else if ((join_filter_ctx->n_times_ & CHECK_TIMES) == 0) {
      if (OB_FAIL(PX_P2P_DH.atomic_get_msg(join_filter_ctx->rf_key_, rf_msg))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get msg", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(rf_msg) && rf_msg->check_ready()) {
        join_filter_ctx->is_ready_ = true;
        join_filter_ctx->ready_ts_ = ObTimeUtility::current_time();
      }
    }
  } else if ((join_filter_ctx->n_times_ & CHECK_TIMES) == 0 &&
             rf_msg->check_ready()) {
    join_filter_ctx->ready_ts_ = ObTimeUtility::current_time();
    join_filter_ctx->is_ready_ = true;
  }
  return ret;
}

void ObExprJoinFilter::collect_sample_info(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
    bool is_match)
{
  if (OB_NOT_NULL(join_filter_ctx)) {
    check_need_dynamic_diable_bf(join_filter_ctx);
    if (!join_filter_ctx->dynamic_disable()) {
      if (!is_match) {
        join_filter_ctx->partial_filter_count_++;
      }
      join_filter_ctx->partial_total_count_++;
    }
  }
}

int ObExprJoinFilter::prepare_storage_white_filter_data(const ObExpr &expr,
                                ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
                                bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  is_data_prepared = false;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = eval_ctx.exec_ctx_;
  ObExprJoinFilterContext *join_filter_ctx = NULL;
  // get expr ctx from exec ctx
  if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
    // join filter ctx may be null in das.
    dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
  } else {
    if (join_filter_ctx->is_first_) {
      join_filter_ctx->start_time_ = ObTimeUtility::current_time();
      join_filter_ctx->is_first_ = false;
    }
    if (OB_FAIL(check_rf_ready(exec_ctx, join_filter_ctx))) {
       LOG_WARN("fail to check bf ready", K(ret));
    } else if (OB_ISNULL(join_filter_ctx->rf_msg_)) {
    } else if (!join_filter_ctx->is_ready() || join_filter_ctx->dynamic_disable()) {
    } else if (OB_FAIL(join_filter_ctx->rf_msg_->prepare_storage_white_filter_data(
        dynamic_filter, eval_ctx, params, is_data_prepared))) {
      LOG_WARN("fail to prepare_storage_white_filter_data", K(ret));
    } else {
      dynamic_filter.cmp_func_ =
          join_filter_ctx->cmp_funcs_.at(dynamic_filter.get_col_idx()).cmp_func_;
    }
  }
  return ret;
}

void ObExprJoinFilter::check_need_dynamic_diable_bf(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  if (OB_ISNULL(join_filter_ctx)) {
  } else if (join_filter_ctx->cur_pos_ == join_filter_ctx->next_check_start_pos_) {
    join_filter_ctx->partial_total_count_ = 0;
    join_filter_ctx->partial_filter_count_ = 0;
    if (join_filter_ctx->dynamic_disable()) {
      join_filter_ctx->dynamic_disable_ = false;
    }
  } else if (join_filter_ctx->cur_pos_ >=
             join_filter_ctx->next_check_start_pos_ + join_filter_ctx->window_size_) {
    if (join_filter_ctx->partial_total_count_ -
          join_filter_ctx->partial_filter_count_ <
          join_filter_ctx->partial_filter_count_) {
       // partial_filter_count_ / partial_total_count_ > 0.5
       // The optimizer choose the bloom filter when the filter threshold is larger than 0.6
       // 0.5 is a acceptable value
      join_filter_ctx->partial_total_count_ = 0;
      join_filter_ctx->partial_filter_count_ = 0;
      join_filter_ctx->window_cnt_ = 0;
      join_filter_ctx->next_check_start_pos_ = join_filter_ctx->cur_pos_;
    } else {
      join_filter_ctx->partial_total_count_ = 0;
      join_filter_ctx->partial_filter_count_ = 0;
      join_filter_ctx->window_cnt_++;
      join_filter_ctx->next_check_start_pos_ = join_filter_ctx->cur_pos_ +
          (join_filter_ctx->window_size_ * join_filter_ctx->window_cnt_);
      join_filter_ctx->dynamic_disable_ = true;
    }
  }
}

void ObExprJoinFilter::collect_sample_info_batch(
    ObExprJoinFilter::ObExprJoinFilterContext &join_filter_ctx,
    int64_t filter_count, int64_t total_count)
{
  if (!join_filter_ctx.dynamic_disable()) {
    join_filter_ctx.partial_filter_count_ += filter_count;
    join_filter_ctx.partial_total_count_ += total_count;
  }
  check_need_dynamic_diable_bf_batch(join_filter_ctx);
}

void ObExprJoinFilter::check_need_dynamic_diable_bf_batch(
    ObExprJoinFilter::ObExprJoinFilterContext &join_filter_ctx)
{
  if (join_filter_ctx.cur_pos_ >= join_filter_ctx.next_check_start_pos_
      && join_filter_ctx.need_reset_sample_info_) {
    join_filter_ctx.partial_total_count_ = 0;
    join_filter_ctx.partial_filter_count_ = 0;
    join_filter_ctx.need_reset_sample_info_ = false;
    if (join_filter_ctx.dynamic_disable()) {
      join_filter_ctx.dynamic_disable_ = false;
    }
  } else if (join_filter_ctx.cur_pos_ >=
            join_filter_ctx.next_check_start_pos_ + join_filter_ctx.window_size_) {
    if (join_filter_ctx.partial_total_count_ -
          join_filter_ctx.partial_filter_count_ <
          join_filter_ctx.partial_filter_count_) {
      // partial_filter_count_ / partial_total_count_ > 0.5
      // The optimizer choose the bloom filter when the filter threshold is larger than 0.6
      // 0.5 is a acceptable value
      // if enabled, the slide window not needs to expand
      join_filter_ctx.window_cnt_ = 0;
      join_filter_ctx.next_check_start_pos_ = join_filter_ctx.cur_pos_;
    } else {
      join_filter_ctx.window_cnt_++;
      join_filter_ctx.next_check_start_pos_ = join_filter_ctx.cur_pos_ +
          (join_filter_ctx.window_size_ * join_filter_ctx.window_cnt_);
      join_filter_ctx.dynamic_disable_ = true;
    }
    join_filter_ctx.partial_total_count_ = 0;
    join_filter_ctx.partial_filter_count_ = 0;
    join_filter_ctx.need_reset_sample_info_ = true;
  }
}

int ObExprJoinFilter::eval_bloom_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_filter_internal(expr, ctx, res);
}

int ObExprJoinFilter::eval_range_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_filter_internal(expr, ctx, res);
}

int ObExprJoinFilter::eval_in_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_filter_internal(expr, ctx, res);
}

int ObExprJoinFilter::eval_filter_internal(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprJoinFilterContext *join_filter_ctx = NULL;
  // get expr ctx from exec ctx
  if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
    // join filter ctx may be null in das.
    res.set_int(1);
  } else {
    if (join_filter_ctx->is_first_) {
      join_filter_ctx->start_time_ = ObTimeUtility::current_time();
      join_filter_ctx->is_first_ = false;
    }
    if (OB_FAIL(check_rf_ready(exec_ctx, join_filter_ctx))) {
       LOG_WARN("fail to check bf ready", K(ret));
    } else if (OB_ISNULL(join_filter_ctx->rf_msg_)) {
      res.set_int(1);
    } else if (!join_filter_ctx->is_ready() || join_filter_ctx->dynamic_disable()) {
      res.set_int(1);
    } else if (OB_FAIL(join_filter_ctx->rf_msg_->might_contain(expr, ctx, *join_filter_ctx, res))) {
      LOG_WARN("fail to check contain row", K(ret));
    }
    if (OB_SUCC(ret)) {
      join_filter_ctx->n_times_++;
      join_filter_ctx->total_count_++;
    }

  }
  return ret;
}

int ObExprJoinFilter::eval_bloom_filter_batch(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size)
{
  return eval_filter_batch_internal(expr, ctx, skip, batch_size);
}

int ObExprJoinFilter::eval_range_filter_batch(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size)
{
  return eval_filter_batch_internal(expr, ctx, skip, batch_size);
}

int ObExprJoinFilter::eval_in_filter_batch(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size)
{
  return eval_filter_batch_internal(expr, ctx, skip, batch_size);
}

int ObExprJoinFilter::eval_filter_batch_internal(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprJoinFilterContext *join_filter_ctx = NULL;
  ObDatum *results = expr.locate_batch_datums(ctx); // for batch
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx); // for batch
  if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) { // get expr_ctx from exec_ctx
    // join filter ctx may be null in das.
    if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
      [&](int64_t idx) __attribute__((always_inline)) {
        eval_flags.set(idx);
        results[idx].set_int(is_match); // all results are true when join_filter_ctx is not ready.
        return OB_SUCCESS;
      }))) { /* do nothing*/ }
  } else {
    if (join_filter_ctx->is_first_) {
      join_filter_ctx->start_time_ = ObTimeUtility::current_time();
      join_filter_ctx->is_first_ = false;
    }
    if (OB_FAIL(check_rf_ready(exec_ctx, join_filter_ctx))) {
       LOG_WARN("fail to check bf ready", K(ret));
    } else if (OB_ISNULL(join_filter_ctx->rf_msg_)) {
      FILL_BATCH_RESULT();
    } else if (!join_filter_ctx->is_ready() || join_filter_ctx->dynamic_disable()) {
      FILL_BATCH_RESULT();
    } else if (OB_FAIL(join_filter_ctx->rf_msg_->might_contain_batch(
          expr, ctx, skip, batch_size, *join_filter_ctx))) {
      LOG_WARN("fail to might contain batch");
    }
  }
  return ret;
}


int ObExprJoinFilter::eval_bloom_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  return eval_filter_vector_internal(expr, ctx, skip, bound);
}

int ObExprJoinFilter::eval_range_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  return eval_filter_vector_internal(expr, ctx, skip, bound);
}

int ObExprJoinFilter::eval_in_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  return eval_filter_vector_internal(expr, ctx, skip, bound);
}

int ObExprJoinFilter::eval_filter_vector_internal(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = bound.batch_size();
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprJoinFilterContext *join_filter_ctx = NULL;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx); // for batch
  VectorFormat res_format = expr.get_format(ctx);
  if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
    // join filter ctx may be null in das.
    if (VEC_UNIFORM == res_format) {
      IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
      ret = proc_if_das(res_vec, skip, batch_size);
    } else if (VEC_FIXED == res_format) {
      IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
      ret = proc_if_das(res_vec, skip, batch_size);
    }
    eval_flags.set_all(true);
  } else {
    if (join_filter_ctx->is_first_) {
      join_filter_ctx->start_time_ = ObTimeUtility::current_time();
      join_filter_ctx->is_first_ = false;
    }
    if (OB_FAIL(check_rf_ready(exec_ctx, join_filter_ctx))) {
      LOG_WARN("fail to check bf ready", K(ret));
    } else if (OB_ISNULL(join_filter_ctx->rf_msg_) || !join_filter_ctx->is_ready()
               || join_filter_ctx->dynamic_disable()) {
      // rf_msg_ is null: no msg arrived yet
      // rf_msg_ not ready: not all msgs arrived
      // rf_msg_ dynamic_disable: disable filter when filter rate < 0.5
      if (VEC_UNIFORM == res_format) {
        IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
        ret = proc_by_pass(res_vec, skip, batch_size, join_filter_ctx);
      } else if (VEC_FIXED == res_format) {
        IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
        ret = proc_by_pass(res_vec, skip, batch_size, join_filter_ctx);
      }
      eval_flags.set_all(true);
    } else if (OB_FAIL(join_filter_ctx->rf_msg_->might_contain_vector(expr, ctx, skip, bound,
                                                                      *join_filter_ctx))) {
      LOG_WARN("fail to might contain batch");
    }
  }
  return ret;
}

}
}

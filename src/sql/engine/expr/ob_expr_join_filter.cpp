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
          if (OB_FAIL(collect_sample_info(join_filter_ctx, is_match))) {\
            LOG_WARN("fail to collect sample info", K(ret));\
          } else {\
            ++join_filter_ctx->total_count_;\
          }\
          return ret;\
        }))) {}

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

ObExprJoinFilter::ObExprJoinFilter(ObIAllocator& alloc)
    : ObExprOperator(alloc,
                     T_OP_JOIN_BLOOM_FILTER,
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

int ObExprJoinFilter::eval_bloom_filter(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprJoinFilterContext *join_filter_ctx = NULL;

  // 在exec_ctx中获取expr_ctx
  if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilterContext *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
    // join filter ctx may be null in das.
    res.set_int(1);
  } else {
    // 获取join bloom filter
    ObPxBloomFilter *&bloom_filter_ptr_ = join_filter_ctx->bloom_filter_ptr_;
    if (OB_ISNULL(bloom_filter_ptr_) && (join_filter_ctx->n_times_ & CHECK_TIMES) == 0) {
     if (OB_FAIL(ObPxBloomFilterManager::instance().get_px_bloom_filter(join_filter_ctx->bf_key_,
           bloom_filter_ptr_))) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(bloom_filter_ptr_)) {
      if (OB_FAIL(check_bf_ready(exec_ctx, join_filter_ctx))) {
        LOG_WARN("fail to check bf ready", K(ret));
      } else if (!join_filter_ctx->is_ready() || join_filter_ctx->dynamic_disable()) {
      } else if (expr.arg_cnt_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the expr of arg cnt is invalid", K(ret));
      } else {
        uint64_t hash_val = JOIN_FILTER_SEED;
        ObDatum *datum = nullptr;
        ObHashFunc hash_func;
        for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
          if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
            LOG_WARN("failed to eval datum", K(ret));
          } else {
            if (OB_ISNULL(expr.inner_functions_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr.inner_functions_ is null", K(ret));
            } else {
              hash_func.hash_func_ = reinterpret_cast<ObDatumHashFuncType>(expr.inner_functions_[i * 2]);
            }
            hash_val = hash_func.hash_func_(*datum, hash_val);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(bloom_filter_ptr_->might_contain(hash_val, is_match))) {
            LOG_WARN("fail to check filter might contain value", K(ret), K(hash_val));
          } else {
            join_filter_ctx->check_count_++;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++join_filter_ctx->n_times_;
      if (!is_match) {
        join_filter_ctx->filter_count_++;
      }
      res.set_int(is_match ? 1 : 0);
      if (OB_FAIL(collect_sample_info(join_filter_ctx, is_match))) {
        LOG_WARN("fail to collect sample info", K(ret));
      } else {
        join_filter_ctx->total_count_++;
      }
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
  LOG_DEBUG("eval expr bloom filter in batch mode", K(batch_size));
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
    ObPxBloomFilter *&bloom_filter_ptr_ = join_filter_ctx->bloom_filter_ptr_; // get join bloom filter
    if (OB_ISNULL(bloom_filter_ptr_) && (join_filter_ctx->n_times_ & CHECK_TIMES) == 0) {
     if (OB_FAIL(ObPxBloomFilterManager::instance().get_px_bloom_filter(join_filter_ctx->bf_key_,
           bloom_filter_ptr_))) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(bloom_filter_ptr_)) {
      if (OB_FAIL(check_bf_ready(exec_ctx, join_filter_ctx))) {
        LOG_WARN("fail to check bf ready", K(ret));
      } else if (!join_filter_ctx->is_ready() || join_filter_ctx->dynamic_disable()) {
        FILL_BATCH_RESULT();
      } else if (expr.arg_cnt_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the expr of arg cnt is invalid", K(ret));
      } else {
        uint64_t seed = JOIN_FILTER_SEED;
        uint64_t *hash_values = reinterpret_cast<uint64_t *>(
                                ctx.frames_[expr.frame_idx_] + expr.res_buf_off_);
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
          ObExpr *e = expr.args_[i];
          if (OB_FAIL(e->eval_batch(ctx, skip, batch_size))) {
            LOG_WARN("evaluate batch failed", K(ret), K(*e));
          } else {
            const bool is_batch_seed = (i > 0);
            if (OB_ISNULL(expr.inner_functions_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("the inner_functions_ of expr is null", K(ret));
            } else {
              ObBatchDatumHashFunc hash_func_batch = reinterpret_cast<ObBatchDatumHashFunc>(expr.inner_functions_[i * 2 + 1]);
              hash_func_batch(hash_values,
                              e->locate_batch_datums(ctx), e->is_batch_result(),
                              skip, batch_size,
                              is_batch_seed ? hash_values : &seed,
                              is_batch_seed);
            }
          }
        }
        if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
              [&](int64_t idx) __attribute__((always_inline)) {
                bloom_filter_ptr_->prefetch_bits_block(hash_values[idx]); return OB_SUCCESS;
              }))) {
        } else if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
            [&](int64_t idx) __attribute__((always_inline)) {
              ret = bloom_filter_ptr_->might_contain(hash_values[idx], is_match);
              if (OB_SUCC(ret)) {
                join_filter_ctx->filter_count_ += !is_match;
                eval_flags.set(idx);
                results[idx].set_int(is_match);
                if (OB_FAIL(collect_sample_info(join_filter_ctx, is_match))) {
                  LOG_WARN("fail to collect sample info", K(ret));
                } else {
                  ++join_filter_ctx->check_count_;
                  ++join_filter_ctx->total_count_;
                }
              }
              return ret;
            }))) {
          LOG_WARN("failed to process prefetch block", K(ret));
        }
      }
    } else { // bloom_filter_ptr_ is null
      LOG_DEBUG("the bloom_filter_ptr_ is null in batch mode", K(ret));
      FILL_BATCH_RESULT();
    }
  }

  return ret;
}

int ObExprJoinFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_bloom_filter;
  rt_expr.eval_batch_func_ = eval_bloom_filter_batch;
  rt_expr.inner_func_cnt_ = rt_expr.arg_cnt_ * 2;

  if (0 == rt_expr.inner_func_cnt_) {
    // do nothing
  } else if (OB_ISNULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(expr_cg_ctx.allocator_->
                       alloc(sizeof(ObExpr::EvalFunc) * rt_expr.arg_cnt_ * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for inner_functions_ failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_; ++i) {
      bool is_murmur_hash_v2_ = expr_cg_ctx.cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0;
      rt_expr.inner_functions_[i * 2] = is_murmur_hash_v2_ ?
          reinterpret_cast<void*>(rt_expr.args_[i]->basic_funcs_->murmur_hash_v2_)
          : reinterpret_cast<void*>(rt_expr.args_[i]->basic_funcs_->murmur_hash_);
      rt_expr.inner_functions_[i * 2 + 1] = is_murmur_hash_v2_ ?
          reinterpret_cast<void*>(rt_expr.args_[i]->basic_funcs_->murmur_hash_v2_batch_)
          : reinterpret_cast<void*>(rt_expr.args_[i]->basic_funcs_->murmur_hash_batch_);
    }
  }

  return ret;
}

int ObExprJoinFilter::check_bf_ready(
    ObExecContext &exec_ctx,
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_filter_ctx)) {
  } else if (!join_filter_ctx->is_ready()) {
    ObPxBloomFilter *&bloom_filter_ptr_ = join_filter_ctx->bloom_filter_ptr_;
    if (join_filter_ctx->need_wait_ready()) {
      while (!join_filter_ctx->is_ready() && OB_SUCC(exec_ctx.fast_check_status())) {
        if (bloom_filter_ptr_->check_ready()) {
          join_filter_ctx->ready_ts_ = ObTimeUtility::current_time();
          join_filter_ctx->is_ready_ = true;
        } else {
          ob_usleep(100);
        }
      }
    } else {
      if ((join_filter_ctx->n_times_ & CHECK_TIMES) == 0 && bloom_filter_ptr_->check_ready()) {
        join_filter_ctx->ready_ts_ = ObTimeUtility::current_time();
        join_filter_ctx->is_ready_ = true;
      }
    }
  }
  return ret;
}

int ObExprJoinFilter::collect_sample_info(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
    bool is_match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_filter_ctx)) {
  } else if (OB_FAIL(check_need_dynamic_diable_bf(join_filter_ctx))) {
    LOG_WARN("fail to check need dynamic disable bf", K(ret));
  } else if (!join_filter_ctx->dynamic_disable()) {
    if (!is_match) {
      join_filter_ctx->partial_filter_count_++;
    }
    join_filter_ctx->partial_total_count_++;
  }
  return ret;
}

int ObExprJoinFilter::check_need_dynamic_diable_bf(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx)
{
  int ret = OB_SUCCESS;
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
  return ret;
}


}
}

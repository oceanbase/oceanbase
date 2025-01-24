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
#define USING_LOG_PREFIX SQL

#include "processor.h"
#include "share/aggregate/single_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

int Processor::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    LOG_DEBUG("already inited, do nothing");
  } else {
    if (OB_ISNULL(row_selector_ = (uint16_t *)allocator_.alloc(
                    sizeof(uint16_t) * agg_ctx_.eval_ctx_.max_batch_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMSET(row_selector_, 0, sizeof(uint16_t) * agg_ctx_.eval_ctx_.max_batch_size_);
    }

    if (OB_FAIL(ret)) {
    } else if (agg_ctx_.aggr_infos_.count() <= 0) {
      // do nothing
    } else if (OB_UNLIKELY(agg_ctx_.aggr_infos_.count() >= MAX_SUPPORTED_AGG_CNT)) {
      ret = OB_NOT_SUPPORTED;
      SQL_LOG(WARN, "too many aggregations, not supported", K(ret));
    } else if (OB_FAIL(aggregates_.reserve(agg_ctx_.aggr_infos_.count()))) {
      SQL_LOG(WARN, "reserved allocator failed", K(ret));
    } else if (OB_FAIL(helper::init_aggregates(agg_ctx_, allocator_, aggregates_))) {
      SQL_LOG(WARN, "init aggregates failed", K(ret));
    } else if (OB_FAIL(add_one_row_fns_.prepare_allocate(agg_ctx_.aggr_infos_.count()))) {
      SQL_LOG(WARN, "prepare allocate elements failed", K(ret));
    } else {
      clear_add_one_row_fns();
    }
    if (OB_SUCC(ret)) { inited_ = true; }
  }

  return ret;
}

int Processor::init_fast_single_row_aggs()
{
  int ret = OB_SUCCESS;
  if (support_fast_single_row_agg_) {
    if (fast_single_row_aggregates_.count() > 0) {
      // do nothing
    } else if (OB_FAIL(fast_single_row_aggregates_.reserve(agg_ctx_.aggr_infos_.count()))) {
      SQL_LOG(WARN, "reserve elements failed", K(ret));
    } else if (OB_FAIL(helper::init_single_row_aggregates(agg_ctx_, allocator_,
                                                          fast_single_row_aggregates_))) {
      SQL_LOG(WARN, "init single row aggregate failed", K(ret));
    }
  }
  return ret;
}

void Processor::destroy()
{
  agg_ctx_.destroy();
  for (int i = 0; i < aggregates_.count(); i++) {
    if (OB_NOT_NULL(aggregates_.at(i))) {
      aggregates_.at(i)->destroy();
    }
  }
  aggregates_.reset();
  for (int i = 0; i < fast_single_row_aggregates_.count(); i++) {
    if (OB_NOT_NULL(fast_single_row_aggregates_.at(i))) {
      fast_single_row_aggregates_.at(i)->destroy();
    }
  }
  fast_single_row_aggregates_.reset();
  allocator_.reset();
  row_selector_ = nullptr;
  cur_batch_group_idx_ = 0;
  cur_batch_group_buf_ = nullptr;
  inited_ = false;
}

void Processor::reuse()
{
  agg_ctx_.reuse();
  for (int i = 0; i < aggregates_.count(); i++) {
    if (OB_NOT_NULL(aggregates_.at(i))) {
      aggregates_.at(i)->reuse();
    }
  }
  for (int i = 0; i < fast_single_row_aggregates_.count(); i++) {
    if (OB_NOT_NULL(fast_single_row_aggregates_.at(i))) {
      fast_single_row_aggregates_.at(i)->reuse();
    }
  }
  cur_batch_group_idx_ = 0;
  cur_batch_group_buf_ = nullptr;
}

int Processor::add_batch_rows(const int32_t start_agg_id, const int32_t end_agg_id,
                              AggrRowPtr row, const ObBatchRows &brs, uint16_t begin,
                              uint16_t end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_LIKELY(brs.size_ > 0)) {
    OB_ASSERT(aggregates_.count() >= end_agg_id && start_agg_id >= 0);
    sql::EvalBound bound(brs.size_, begin, end, brs.all_rows_active_);
    CK(OB_NOT_NULL(brs.skip_));
    for (int col_id = start_agg_id; OB_SUCC(ret) && col_id < end_agg_id;
         col_id++) {
      char *aggr_cell = agg_ctx_.row_meta().locate_cell_payload(col_id, row);
      if (OB_FAIL(aggregates_.at(col_id)->add_batch_rows(agg_ctx_, col_id, *brs.skip_, bound,
                                                         aggr_cell))) {
        SQL_LOG(WARN, "add batch rows failed", K(ret));
      }
    }
  }
  return ret;
}

int Processor::add_batch_rows(const int32_t start_agg_id, const int32_t end_agg_id,
                              AggrRowPtr row, const ObBatchRows &brs,
                              const uint16_t *selector_array, uint16_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_LIKELY(brs.size_ > 0)) {
    CK(OB_NOT_NULL(brs.skip_));
    OB_ASSERT(aggregates_.count() >= end_agg_id && start_agg_id >= 0);
    sql::EvalBound bound(brs.size_, 0, brs.size_, brs.all_rows_active_);
    for (int32_t col_id = start_agg_id; OB_SUCC(ret) && col_id < end_agg_id;
         col_id++) {
      char *aggr_cell = agg_ctx_.row_meta().locate_cell_payload(col_id, row);
      if (OB_FAIL(aggregates_.at(col_id)->add_batch_rows(
            agg_ctx_, col_id, *brs.skip_, bound, aggr_cell, RowSelector(selector_array, count)))) {
        SQL_LOG(WARN, "add batch rows failed", K(ret));
      }
    }
  }
  return ret;
}

int Processor::advance_collect_result(const int64_t cur_group_id, const RowMeta &row_meta,
                                      aggregate::AggrRowPtr group_row)
{
  int ret = OB_SUCCESS;
  int32_t batch_size =
    min(1, static_cast<int32_t>(agg_ctx_.agg_rows_.count() - cur_group_id));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(batch_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch size", K(ret), K(batch_size));
  } else {
    int32_t output_size = batch_size;
    bool got_result = false;
    ObEvalCtx::BatchInfoScopeGuard guard(agg_ctx_.eval_ctx_);
    guard.set_batch_size(1);
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx_.aggr_infos_.count(); i++) {
      int32_t agg_col_idx = i;
      ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(agg_col_idx);
      if (helper::has_extra_info(aggr_info)) {
        int32_t cur_batch_size = 0;
        char *curr_agg_cell = agg_ctx_.row_meta().locate_cell_payload(agg_col_idx, group_row);
        VecExtraResult *&extra = agg_ctx_.get_extra(agg_col_idx, curr_agg_cell);
        if (OB_ISNULL(extra)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "is null", K(extra), K(agg_col_idx));
        } else if (OB_UNLIKELY(agg_col_idx >= aggregates_.count())) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected agg_col_idx", K(agg_col_idx), K(aggregates_));
        } else if (OB_ISNULL(aggr_info.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
        } else {
          // do nothing
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(aggregates_.at(agg_col_idx)
                             ->eval_group_extra_result(agg_ctx_, agg_col_idx,
                                                       static_cast<int32_t>(cur_group_id)))) {
          SQL_LOG(WARN, "collect group results failed", K(ret), K(batch_size));
        } else {
          if (!got_result) {
            output_size = cur_batch_size;
            got_result = true;
          } else if (OB_UNLIKELY(output_size != cur_batch_size)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "unexepcted output batch", K(output_size), K(cur_batch_size), K(ret));
          }
          extra->set_is_evaluated();
          extra->reuse();
        }
      } else if (T_FUN_SYS_RB_BUILD_AGG == aggr_info.get_expr_type()) {
        // rb_build_agg does not have extra, but need advance collect to save memory
        int32_t cur_batch_size = 0;
        if (OB_UNLIKELY(agg_col_idx >= aggregates_.count())) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected agg_col_idx", K(agg_col_idx), K(aggregates_));
        } else if (OB_ISNULL(aggr_info.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
        } else if (OB_FAIL(aggregates_.at(agg_col_idx)
                             ->eval_group_extra_result(agg_ctx_, agg_col_idx,
                                                       static_cast<int32_t>(cur_group_id)))) {
          SQL_LOG(WARN, "collect group results failed", K(ret), K(batch_size));
        } else {
          // real need ?
          if (!got_result) {
            output_size = cur_batch_size;
            got_result = true;
          } else if (OB_UNLIKELY(output_size != cur_batch_size)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "unexepcted output batch", K(output_size), K(cur_batch_size), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int Processor::collect_group_results(const RowMeta &row_meta, int32_t &output_batch_size,
                                     ObBatchRows &output_brs, int64_t &cur_group_id)
{
  int ret = OB_SUCCESS;
  int32_t batch_size =
    min(output_batch_size, static_cast<int32_t>(agg_ctx_.agg_rows_.count() - cur_group_id));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(batch_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch size", K(ret), K(batch_size));
  } else {
    int32_t aggr_cnt = static_cast<int32_t>(agg_ctx_.aggr_infos_.count());
    // default to batch_size
    // if no aggregate functions, there is no need to collect group results, e.g.
    // `select /*+parallel(2)*/ count(distinct b), a from t group by a`, stage 1.
    // However, HashGroupBy operator still adds aggregate rows into processor and
    // calls `collect_group_results` to get output row size to stop iteration.
    int32_t output_size = batch_size;
    bool got_result = false;
    ObEvalCtx::BatchInfoScopeGuard guard(agg_ctx_.eval_ctx_);
    guard.set_batch_size(batch_size);
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx_.aggr_infos_.count(); i++) {
      int32_t agg_col_idx = i;
      int32_t output_start_idx = static_cast<int32_t>(output_brs.size_);
      int32_t cur_batch_size = 0;
      int32_t start_gid = static_cast<int32_t>(cur_group_id);
      ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(agg_col_idx);
      if (OB_UNLIKELY(agg_col_idx >= aggregates_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected agg_col_idx", K(agg_col_idx), K(aggregates_));
      } else if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(aggregates_.at(agg_col_idx)
                           ->collect_batch_group_results(agg_ctx_, agg_col_idx, start_gid,
                                                         output_start_idx, batch_size,
                                                         cur_batch_size))) {
        SQL_LOG(WARN, "collect group results failed", K(ret), K(batch_size));
      } else if (!got_result) {
        output_size = cur_batch_size;
        got_result = true;
      } else if (OB_UNLIKELY(output_size != cur_batch_size)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexepcted output batch", K(output_size), K(cur_batch_size), K(ret));
      }
      OX (agg_ctx_.aggr_infos_.at(i).expr_->get_eval_info(agg_ctx_.eval_ctx_).projected_ = true);
    }
    LOG_DEBUG("collect group results", K(ret), K(output_size), K(cur_group_id), K(output_brs),
              K(output_batch_size));
    if (OB_SUCC(ret)) {
      output_batch_size = output_size;
      output_brs.size_ += output_size;
      cur_group_id += output_size;
    }
  }
  return ret;
}

int Processor::collect_group_results(const RowMeta &row_meta,
                                     const ObIArray<ObExpr *> &groupby_exprs,
                                     const int32_t output_batch_size, ObBatchRows &output_brs,
                                     int64_t &cur_group_id)
{
  int ret = OB_SUCCESS;
  int32_t batch_size =
    min(output_batch_size, static_cast<int32_t>(agg_ctx_.agg_rows_.count() - cur_group_id));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(batch_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch size", K(ret), K(batch_size));
  } else {
    int32_t aggr_cnt = static_cast<int32_t>(agg_ctx_.aggr_infos_.count());
    // default to batch_size
    // if no aggregate functions, there is no need to collect group results, e.g.
    // `select /*+parallel(2)*/ count(distinct b), a from t group by a`, stage 1.
    // However, HashGroupBy operator still adds aggregate rows into processor and
    // calls `collect_group_results` to get output row size to stop iteration.
    int32_t output_size = batch_size;
    bool got_result = false;
    ObEvalCtx::BatchInfoScopeGuard guard(agg_ctx_.eval_ctx_);
    guard.set_batch_size(batch_size);
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx_.aggr_infos_.count(); i++) {
      int32_t agg_col_idx = i;
      int32_t output_start_idx = static_cast<int32_t>(output_brs.size_);
      int32_t cur_batch_size = 0;
      int32_t start_gid = static_cast<int32_t>(cur_group_id);
      ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(agg_col_idx);
      if (OB_UNLIKELY(agg_col_idx >= aggregates_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected agg_col_idx", K(agg_col_idx), K(aggregates_));
      } else if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(aggregates_.at(agg_col_idx)
                           ->collect_batch_group_results(agg_ctx_, agg_col_idx, start_gid,
                                                         output_start_idx, batch_size,
                                                         cur_batch_size))) {
        SQL_LOG(WARN, "collect group results failed", K(ret), K(batch_size));
      } else if (!got_result) {
        output_size = cur_batch_size;
        got_result = true;
      } else if (OB_UNLIKELY(output_size != cur_batch_size)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexepcted output batch", K(output_size), K(cur_batch_size), K(ret));
      }
      OX (agg_ctx_.aggr_infos_.at(i).expr_->get_eval_info(agg_ctx_.eval_ctx_).projected_ = true);
    }

    if (OB_SUCC(ret)) { clear_op_evaluated_flag(); }
    if (OB_SUCC(ret)) {
      ObEvalCtx::TempAllocGuard alloc_guard(agg_ctx_.eval_ctx_);
      ObFixedArray<const ObCompactRow *, ObIAllocator> stored_rows(alloc_guard.get_allocator(),
                                                                   output_size);
      for (int i = 0; OB_SUCC(ret) && i < output_size; i++) {
        int64_t group_id = cur_group_id + i;
        AggrRowPtr agg_row = agg_ctx_.agg_rows_.at(group_id);
        const ObCompactRow &row = get_groupby_stored_row(row_meta, agg_row);
        if (OB_FAIL(stored_rows.push_back(&row))) {
          SQL_LOG(WARN, "push back element failed", K(ret));
        }
      }
      for (int col_id = 0; OB_SUCC(ret) && col_id < groupby_exprs.count(); col_id++) {
        ObExpr *out_expr = groupby_exprs.at(col_id);
        if (OB_ISNULL(out_expr)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid null expr", K(ret));
        } else if (OB_UNLIKELY(out_expr->is_const_expr())) {
          // do not project const exprs
          // do nothing
        } else if (OB_FAIL(out_expr->init_vector_default(agg_ctx_.eval_ctx_, output_batch_size))) {
          SQL_LOG(WARN, "init vector failed", K(ret));
        } else if (OB_FAIL(out_expr->get_vector(agg_ctx_.eval_ctx_)
                             ->from_rows(row_meta, stored_rows.get_data(), output_size, col_id))) {
          SQL_LOG(WARN, "from rows failed", K(ret));
        }
      }
      for (int i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); i++) {
        if (!groupby_exprs.at(i)->is_const_expr()) {
          groupby_exprs.at(i)->set_evaluated_projected(agg_ctx_.eval_ctx_);
        }
      }
    }
    LOG_DEBUG("collect group results", K(ret), K(output_size), K(cur_group_id), K(output_brs),
             K(output_batch_size));
    if (OB_SUCC(ret)) {
      output_brs.size_ += output_size;
      cur_group_id += output_size;
    }
  }
  return ret;
}

int Processor::collect_group_results(const RowMeta &row_meta,
                                     const ObIArray<ObExpr *> &groupby_exprs,
                                     const int32_t batch_size, const ObCompactRow **rows,
                                     ObBatchRows &output_brs)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(inited_ && batch_size > 0)) {
    ObEvalCtx::BatchInfoScopeGuard batch_guard(agg_ctx_.eval_ctx_);
    batch_guard.set_batch_size(batch_size);
    for (int col_id = 0; OB_SUCC(ret) && col_id < agg_ctx_.aggr_infos_.count(); col_id++) {
      ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(col_id);
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected null aggregate expr", K(ret));
      } else if (OB_FAIL(aggregates_.at(col_id)->collect_batch_group_results(
                   agg_ctx_, col_id, 0, batch_size, rows, row_meta))) {
        SQL_LOG(WARN, "collect batch group results", K(ret));
      } else {
        agg_ctx_.aggr_infos_.at(col_id).expr_->get_eval_info(agg_ctx_.eval_ctx_).projected_ = true;
      }
    }
    if (OB_SUCC(ret)) { clear_op_evaluated_flag(); }

    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      LOG_DEBUG("stored group rows", "input", CompactRow2STR(row_meta, *rows[i], &groupby_exprs));
    }

    if (OB_SUCC(ret)) {
      ObEvalCtx::TempAllocGuard alloc_guard(agg_ctx_.eval_ctx_);
      for (int col_id = 0; OB_SUCC(ret) && col_id < groupby_exprs.count(); col_id++) {
        ObExpr *out_expr = groupby_exprs.at(col_id);
        if (OB_ISNULL(out_expr)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid null output expr", K(ret));
        } else if (OB_UNLIKELY(out_expr->is_const_expr())) {
          // do not project const exprs
          // do nothing
        } else if (OB_FAIL(out_expr->init_vector_default(agg_ctx_.eval_ctx_, batch_size))) {
          SQL_LOG(WARN, "init vector failed", K(ret));
        } else if (OB_FAIL(out_expr->get_vector(agg_ctx_.eval_ctx_)
                             ->from_rows(row_meta, rows, batch_size, col_id))) {
          SQL_LOG(WARN, "from rows failed", K(ret));
        }
      }
      for (int col_id = 0; OB_SUCC(ret) && col_id < groupby_exprs.count(); col_id++) {
        if (!groupby_exprs.at(col_id)->is_const_expr()) {
          groupby_exprs.at(col_id)->set_evaluated_projected(agg_ctx_.eval_ctx_);
        }
      }
    }
    LOG_DEBUG("collect group results", K(ret), K(batch_size), K(output_brs));
    if (OB_SUCC(ret)) {
      output_brs.size_ += batch_size;
    }
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (batch_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch size", K(ret), K(batch_size));
  }
  return ret;
}

int Processor::setup_rt_info(AggrRowPtr row,
                             RuntimeContext &agg_ctx,
                             ObIAllocator *extra_allocator /*=nullptr*/,
                             const int64_t group_id /*=0*/)
{
  static const int constexpr extra_arr_buf_size = 16;
  int ret = OB_SUCCESS;
  int32_t row_size = agg_ctx.row_meta().row_size_;
  char *extra_array_buf = nullptr;
  MEMSET(row, 0, row_size);
  for (int col_id = 0; col_id < agg_ctx.aggr_infos_.count(); col_id++) {
    ObDatumMeta &res_meta = agg_ctx.aggr_infos_.at(col_id).expr_->datum_meta_;
    VecValueTypeClass res_tc = get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
    char *cell = nullptr;
    int32_t cell_len = 0;
    agg_ctx.row_meta().locate_cell_payload(col_id, row, cell, cell_len);
    // oracle mode use ObNumber as result type for count aggregation
    // we use int64_t as result type for count aggregation in aggregate row
    // and cast int64_t to ObNumber during `collect_group_result`
    if (res_tc == VEC_TC_NUMBER && agg_ctx.aggr_infos_.at(col_id).get_expr_type() != T_FUN_COUNT) {
      ObNumberDesc &d = *reinterpret_cast<ObNumberDesc *>(cell);
      // set zero number
      d.len_ = 0;
      d.sign_ = number::ObNumber::POSITIVE;
      d.exp_ = 0;
    } else if (res_tc == VEC_TC_FLOAT) {
      *reinterpret_cast<float *>(cell) = float();
    } else if (res_tc == VEC_TC_DOUBLE || res_tc == VEC_TC_FIXED_DOUBLE) {
      *reinterpret_cast<double *>(cell) = double();
    }

    if (T_FUN_SYS_RB_BUILD_AGG == agg_ctx.aggr_infos_.at(col_id).get_expr_type()) {
      // rb_build_agg does not have extra, but need advance collect to save memory
      agg_ctx.need_advance_collect_ = true;
    }
  }
  int extra_size = agg_ctx.row_meta().extra_cnt_ * sizeof(char *);
  if (agg_ctx.row_meta().extra_cnt_ > 0) {
    void *extra_array_buf = agg_ctx.allocator_.alloc(extra_size);
    if (OB_ISNULL(extra_array_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMSET(extra_array_buf, 0, extra_size);
      if (OB_FAIL(init_aggr_row_extra_info(
            agg_ctx, (char *)extra_array_buf,
            nullptr == extra_allocator ? agg_ctx.allocator_ : *extra_allocator, group_id))) {
        SQL_LOG(WARN, "failed to init aggr row extra info", K(ret), K(group_id));
      } else if (OB_FAIL(agg_ctx.agg_extras_.push_back((AggregateExtras)extra_array_buf))) {
        SQL_LOG(WARN, "push back element failed", K(ret));
      } else {
        *reinterpret_cast<int32_t *>(row + agg_ctx.row_meta().extra_idx_offset_) =
          static_cast<int32_t>(agg_ctx.agg_extras_.count()) - 1;
      }
    }
  }
  return ret;
}

VecExtraResult *&Processor::get_extra(const int64_t agg_col_id, RuntimeContext &agg_ctx,
                                      char *extra_array_buf)
{
  OB_ASSERT(agg_col_id < agg_ctx.agg_row_meta_.col_cnt_);
  OB_ASSERT(agg_ctx.agg_row_meta_.extra_idxes_ != nullptr);
  OB_ASSERT(agg_ctx.agg_row_meta_.extra_idxes_[agg_col_id] >= 0);
  int32_t agg_extra_id = agg_ctx.agg_row_meta_.extra_idxes_[agg_col_id];
  OB_ASSERT(agg_ctx.agg_row_meta_.extra_cnt_ > agg_extra_id);
  return ((AggregateExtras)extra_array_buf)[agg_extra_id];
}

int Processor::init_aggr_row_extra_info(RuntimeContext &agg_ctx, char *extra_array_buf,
                                        ObIAllocator &extra_allocator, const int64_t group_id /*=0*/)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(i);
    switch (aggr_info.get_expr_type()) {
      case T_FUN_GROUP_CONCAT:
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_WM_CONCAT:
      case T_FUN_WM_CONCAT:
      case T_FUN_PL_AGG_UDF:
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_ORA_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG:
      case T_FUN_ORA_JSON_OBJECTAGG:
      case T_FUN_ORA_XMLAGG:
      case T_FUN_HYBRID_HIST:
      case T_FUN_TOP_FRE_HIST:
      case T_FUN_AGG_UDF:
      case T_FUNC_SYS_ARRAY_AGG: {
        agg_ctx.need_advance_collect_ = true;
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported aggregate type", K(ret), K(aggr_info.get_expr_type()));
        break;
      }
      default: break;
    }
    if (OB_SUCC(ret) && aggr_info.has_distinct_) {
      agg_ctx.need_advance_collect_ = true;
      VecExtraResult *&extra = get_extra(i, agg_ctx, extra_array_buf);
      if (nullptr == extra) {
        void *tmp_buf = NULL;
        if (OB_ISNULL(tmp_buf =
                        agg_ctx.allocator_.alloc(sizeof(HashBasedDistinctVecExtraResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          extra = new (tmp_buf)
            HashBasedDistinctVecExtraResult(extra_allocator, *agg_ctx.op_monitor_info_);
        }
      }

      if (OB_SUCC(ret)) {
        // In window function, get result will be called more than once, rewind is needed.
        //
        // The distinct set will iterate twice with rollup, for rollup processing and aggregation,
        // rewind is needed for the second iteration.
        //
        // Rollup is supported and only supported in sort based group by with multi-groups,
        // only groups with group id greater than zero need to rewind.
        // The groupid of hash groupby also is greater then 0, then need rewind ???
        const bool need_rewind = (agg_ctx.in_window_func_ || group_id > 0);
        if (OB_FAIL(static_cast<HashBasedDistinctVecExtraResult *>(extra)->init_distinct_set(
              aggr_info, need_rewind, *agg_ctx.hp_infras_mgr_, agg_ctx.eval_ctx_))) {
          LOG_WARN("init_distinct_set failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int Processor::single_row_agg_batch(AggrRowPtr *agg_rows, const int64_t batch_size,
                                    ObEvalCtx &eval_ctx, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  batch_info_guard.set_batch_size(batch_size);
  LOG_DEBUG("by pass single row aggregate", K(batch_size), K(support_fast_single_row_agg_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(agg_rows)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected null aggregate rows", K(ret));
  } else if (FALSE_IT(MEMSET(agg_rows[0], 0, get_aggregate_row_size() * batch_size))) {
  } else if (!support_fast_single_row_agg_) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip.at(i)) {
      } else if (OB_FAIL(setup_rt_info(agg_rows[i], agg_ctx_))) {
        SQL_LOG(WARN, "setup runtime info failed", K(ret));
      } else {
        sql::EvalBound bound(batch_size, i, i + 1, true);
        AggrRowPtr row = agg_rows[i];
        int32_t aggr_cell_len = 0;
        int32_t output_size = 0;
        for (int agg_col_id = 0; OB_SUCC(ret) && agg_col_id < aggregates_.count(); agg_col_id++) {
          char *aggr_cell = agg_ctx_.row_meta().locate_cell_payload(agg_col_id, row);
          int32_t aggr_cell_len = agg_ctx_.row_meta().get_cell_len(agg_col_id, row);
          if (OB_FAIL(aggregates_.at(agg_col_id)->add_batch_rows(agg_ctx_,
                                                                 agg_col_id,
                                                                 skip,
                                                                 bound,
                                                                 aggr_cell))) {
            SQL_LOG(WARN, "add batch rows failed", K(ret));
          }
        } // end for
      }
    } // end for

    // must do init vector here, otherwise value stored in agg_expr is reset unexpectedly.
    for (int col_id = 0; OB_SUCC(ret) && col_id < aggregates_.count(); col_id++) {
      ObExpr *agg_expr = agg_ctx_.aggr_infos_.at(col_id).expr_;
      if (OB_FAIL(agg_expr->init_vector_for_write(
            agg_ctx_.eval_ctx_, agg_expr->get_default_res_format(), batch_size))) {
        LOG_WARN("init vector for write failed", K(ret));
      }
    } // end for
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip.at(i)) {
        continue;
      }
      int32_t output_size = 0;
      for (int agg_col_id = 0; OB_SUCC(ret) && agg_col_id < aggregates_.count(); agg_col_id++) {
        if (OB_FAIL(aggregates_.at(agg_col_id)->collect_batch_group_results(
                                                  agg_ctx_, agg_col_id, i, i, 1, output_size, nullptr, false))) {
          SQL_LOG(WARN, "collect result batch faile", K(ret));
        } else if (OB_UNLIKELY(output_size != 1)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid output size", K(output_size));
        }
      } // end for
    }
  } else {
    EvalBound bound(batch_size, skip.accumulate_bit_cnt(batch_size) == 0);
    int32_t output_size = 0;
    char *aggr_cell = nullptr; // fake aggr_cell
    int32_t aggr_cell_len = 0;
    for (int i = 0; OB_SUCC(ret) && i < fast_single_row_aggregates_.count(); i++) {
      ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(i);
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null expr", K(ret));
      } else if (aggr_info.is_implicit_first_aggr()) {
        // do nothing
      } else {
        int32_t output_size = 0;
        if (OB_FAIL(fast_single_row_aggregates_.at(i)->add_batch_rows(agg_ctx_, i, skip, bound,
                                                                      aggr_cell))) {
          SQL_LOG(WARN, "add batch rows faile", K(ret));
        } else if (OB_FAIL(fast_single_row_aggregates_.at(i)->collect_batch_group_results(
                     agg_ctx_, i, bound.start(), bound.start(),
                     static_cast<int32_t>(bound.batch_size()), output_size,
                     (bound.get_all_rows_active() ? nullptr : &skip)))) {
          SQL_LOG(WARN, "collect batch group results faile", K(ret));
        }
      }
    }
  }
  return ret;
}

int Processor::eval_aggr_param_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  bool need_calc_param = (brs.size_ != 0);
  for (int i = 0; OB_SUCC(ret) && need_calc_param && i < agg_ctx_.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(i);
    for (int j = 0; OB_SUCC(ret) && j < aggr_info.param_exprs_.count(); j++) {
      if (OB_FAIL(aggr_info.param_exprs_.at(j)->eval_vector(agg_ctx_.eval_ctx_, brs))) {
        SQL_LOG(WARN, "eval params batch failed", K(ret));
      }
    }
  }
  return ret;
}

int Processor::collect_scalar_results(const RowMeta &row_meta, const ObCompactRow **rows,
                                      const int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null aggregate row", K(ret));
  } else if (OB_LIKELY(batch_size > 0)) {
    for (int col_id = 0; OB_SUCC(ret) && col_id < agg_ctx_.aggr_infos_.count(); col_id++) {
      const ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(col_id);
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null aggregate expr", K(ret));
      } else if (OB_FAIL(aggregates_.at(col_id)->collect_batch_group_results(
                   agg_ctx_, col_id, 0, batch_size, rows, row_meta))) {
        LOG_WARN("collect batch group results failed", K(ret));
      } else {
        aggr_info.expr_->set_evaluated_projected(agg_ctx_.eval_ctx_);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch_size", K(ret), K(batch_size));
  }
  return ret;
}

int Processor::rollup_batch_process(const AggrRowPtr group_row, AggrRowPtr rollup_row,
                                    int64_t diff_group_idx /* -1 */,
                                    const int64_t max_group_cnt /* INT64_MIN */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_row) || OB_ISNULL(rollup_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null aggregate row", K(ret));
  } else {
    for (int col_id = 0; OB_SUCC(ret) && col_id < agg_ctx_.aggr_infos_.count(); col_id++) {
      const ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(col_id);
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null aggregate expr", K(ret));
      } else if (T_FUN_TOP_FRE_HIST == aggr_info.get_expr_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("topk fre hist not support in group by rollup", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "topk fre hist in group by rollup");
      } else if (OB_FAIL(aggregates_.at(col_id)->rollup_aggregation(
                   agg_ctx_, col_id, group_row, rollup_row, diff_group_idx, max_group_cnt))) {
        LOG_WARN("collect batch group results failed", K(ret));
      }
    }
  }
  return ret;
}

int Processor::collect_empty_set(bool collect_for_third_stage) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < agg_ctx_.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(i);
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null aggregate info", K(ret));
    } else {
      // init vector before collect group results
      VectorFormat vec_fmt = aggr_info.expr_->get_default_res_format();
      if (OB_FAIL(aggr_info.expr_->init_vector_for_write(agg_ctx_.eval_ctx_, vec_fmt,
                                                         agg_ctx_.eval_ctx_.get_batch_size()))) {
        LOG_WARN("init vector failed", K(ret));
      }
    }
    ObIVector *res_vec = nullptr;
    int64_t output_idx = agg_ctx_.eval_ctx_.get_batch_idx();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(res_vec = aggr_info.expr_->get_vector(agg_ctx_.eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null vector", K(ret));
    } else if (aggr_info.is_implicit_first_aggr() && !collect_for_third_stage) {
      res_vec->set_null(output_idx);
    } else {
      switch(aggr_info.get_expr_type()) {
      case T_FUN_COUNT:
      case T_FUN_COUNT_SUM:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_KEEP_COUNT:
      case T_FUN_GROUP_PERCENT_RANK: {
        if (lib::is_oracle_mode()) {
          number::ObNumber zero_nmb;
          zero_nmb.set_zero();
          res_vec->set_number(output_idx, zero_nmb);
        } else {
          res_vec->set_int(output_idx, 0);
        }
        break;
      }
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_CUME_DIST: {
        number::ObNumber result_num;
        int64_t num = 1;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(result_num.from(num, tmp_alloc))) {
          LOG_WARN("failed to create number", K(ret));
        } else {
          res_vec->set_number(output_idx, result_num);
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        ret = llc_init_empty(*aggr_info.expr_, agg_ctx_.eval_ctx_);
        break;
      }
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR: {
        uint64_t init_val =
          (aggr_info.get_expr_type() == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0);
        res_vec->set_uint(output_idx, init_val);
        break;
      }
      default: {
        res_vec->set_null(output_idx);
        break;
      }
      }
    }
    if (OB_SUCC(ret)) {
      aggr_info.expr_->set_evaluated_projected(agg_ctx_.eval_ctx_);
    }
  }
  return ret;
}

int Processor::init_aggr_row_meta(RowMeta &row_meta)
{
  int ret = OB_SUCCESS;
  row_meta.reset();
  ObSEArray<ObExpr *, 1> mock_exprs;
  if (OB_FAIL(row_meta.init(mock_exprs, agg_ctx_.row_meta().row_size_))) {
    LOG_WARN("init row meta failed", K(ret));
  }
  return ret;
}

int Processor::init_one_aggr_row(const RowMeta &row_meta, ObCompactRow *&row,
                                 ObIAllocator &extra_allocator, const int64_t group_id /*=0*/)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = row_meta.get_row_fixed_size() + agg_ctx_.row_meta().row_size_;
  if (0 == cur_batch_group_idx_ % BATCH_GROUP_SIZE) {
    if (OB_ISNULL(cur_batch_group_buf_ =
                    (char *)agg_ctx_.allocator_.alloc(alloc_size * BATCH_GROUP_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(alloc_size), K(group_id));
    } else {
      MEMSET(cur_batch_group_buf_, 0, alloc_size * BATCH_GROUP_SIZE);
    }
  }
  if (OB_SUCC(ret)) {
    row = new (cur_batch_group_buf_) ObCompactRow();
    row->init(row_meta);
    AggrRowPtr agg_row = (char *)row->get_extra_payload(row_meta);
    cur_batch_group_buf_ += alloc_size;
    ++cur_batch_group_idx_;
    cur_batch_group_idx_ %= BATCH_GROUP_SIZE;
    if (OB_FAIL(setup_rt_info(agg_row, agg_ctx_, &extra_allocator, group_id))) {
      LOG_WARN("setup rt info failed", K(ret));
    } else if (OB_FAIL(agg_ctx_.agg_rows_.push_back(agg_row))) {
      SQL_LOG(WARN, "push back element failed", K(ret));
    }
  }
  return ret;
}

int Processor::generate_group_rows(AggrRowPtr *row_arr, const int32_t batch_size)
{
  int ret = OB_SUCCESS;
  void *rows_buf = nullptr;
  if (agg_ctx_.aggr_infos_.count() == 0) {
    // first stage with no non-distinc-agg
    // do nothing
  } else if (OB_ISNULL(row_arr)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected null row array", K(ret));
  } else if (OB_ISNULL(rows_buf =
                         agg_ctx_.allocator_.alloc(agg_ctx_.row_meta().row_size_ * batch_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret), K(batch_size));
  } else {
    MEMSET(rows_buf, 0, agg_ctx_.row_meta().row_size_ * batch_size);
    int32_t offset = 0;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      row_arr[i] = static_cast<char *>(rows_buf) + offset;
      offset += agg_ctx_.row_meta().row_size_;
      if (OB_FAIL(add_one_aggregate_row(row_arr[i], agg_ctx_.row_meta().row_size_, true))) {
        SQL_LOG(WARN, "setup rt info failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret) && rows_buf != nullptr) { agg_ctx_.allocator_.free(rows_buf); }
  return ret;
}

int Processor::add_one_aggregate_row(AggrRowPtr data, const int32_t row_size,
                                     bool push_agg_row /*true*/)
{
  int ret = OB_SUCCESS;
  UNUSED(row_size);
  if (OB_FAIL(setup_rt_info(data, agg_ctx_))) {
    SQL_LOG(WARN, "setup runtime info failed", K(ret));
  } else if (push_agg_row && OB_FAIL(agg_ctx_.agg_rows_.push_back(data))) {
    SQL_LOG(WARN, "push back element failed", K(ret));
  }
  return ret;
}

int Processor::reuse_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  AggrRowPtr agg_row = agg_ctx_.agg_rows_.at(group_id);
  int32_t extra_idx = 0;
  // backup extra_idx
  if (agg_ctx_.has_extra_) {
    extra_idx = *reinterpret_cast<const int32_t *>(agg_row + agg_ctx_.row_meta().extra_idx_offset_);
  }
  MEMSET(agg_row, 0, agg_ctx_.row_meta().row_size_);
  // restore extra_idx
  if (agg_ctx_.has_extra_) {
    *reinterpret_cast<int32_t *>(agg_row + agg_ctx_.row_meta().extra_idx_offset_) = extra_idx;
  }
  for (int col_id = 0; OB_SUCC(ret) && col_id < agg_ctx_.aggr_infos_.count(); col_id++) {
    ObAggrInfo &aggr_info = agg_ctx_.aggr_infos_.at(col_id);
    ObDatumMeta &res_meta = aggr_info.expr_->datum_meta_;
    VecValueTypeClass res_tc = get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
    char *cell = nullptr;
    int32_t cell_len = 0;
    if (res_tc == VEC_TC_NUMBER && aggr_info.get_expr_type() != T_FUN_COUNT) {
      agg_ctx_.row_meta().locate_cell_payload(col_id, agg_row, cell, cell_len);
      ObNumberDesc &d = *reinterpret_cast<ObNumberDesc *>(cell);
      // set zero number
      d.len_ = 0;
      d.sign_ = number::ObNumber::POSITIVE;
      d.exp_ = 0;
    } else if (res_tc == VEC_TC_FLOAT) {
      agg_ctx_.row_meta().locate_cell_payload(col_id, agg_row, cell, cell_len);
      *reinterpret_cast<float *>(cell) = float();
    } else if (res_tc == VEC_TC_DOUBLE || res_tc == VEC_TC_FIXED_DOUBLE) {
      agg_ctx_.row_meta().locate_cell_payload(col_id, agg_row, cell, cell_len);
      *reinterpret_cast<double *>(cell) = double();
    }
    if (agg_ctx_.has_extra_ && helper::has_extra_info(aggr_info)) {
      char *curr_agg_cell = agg_ctx_.row_meta().locate_cell_payload(col_id, agg_row);
      VecExtraResult *&extra = agg_ctx_.get_extra(col_id, curr_agg_cell);
      if (OB_ISNULL(extra)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "is null", K(extra), K(col_id));
      } else {
        extra->reuse();
      }
    }
  }
  return ret;
}

template <typename ColumnFmt>
inline static int add_one_row(IAggregate *aggr, RuntimeContext &agg_ctx, const int32_t agg_col_id,
                       AggrRowPtr row, ObIVector *data_vec, const int64_t batch_idx,
                       const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const char *data = nullptr;
  int32_t data_len = 0;
  ColumnFmt *columns = reinterpret_cast<ColumnFmt *>(data_vec);
  char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, row);
  bool is_null = false;
  if (!std::is_same<ObVectorBase, ColumnFmt>::value) {
    reinterpret_cast<ColumnFmt *>(data_vec)->get_payload(batch_idx, data, data_len);
    is_null = reinterpret_cast<ColumnFmt *>(data_vec)->is_null(batch_idx);
  }
  if (OB_FAIL(aggr->add_one_row(agg_ctx, batch_idx, batch_size, is_null, data, data_len, agg_col_id,
                                agg_cell))) {
    SQL_LOG(WARN, "add one row failed", K(ret));
  }
  return ret;
}
int Processor::prepare_adding_one_row()
{
#define GET_FIXED_FN(tc)                                                                           \
  case (tc): {                                                                                     \
    fn_ptr = aggregate::add_one_row<ObFixedLengthFormat<RTCType<tc>>>;                             \
  } break
  int ret = OB_SUCCESS;
  if (FALSE_IT(clear_add_one_row_fns())) {
     SQL_LOG(WARN, "reserve failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < agg_ctx_.aggr_infos_.count(); i++) {
      ObAggrInfo &info = agg_ctx_.aggr_infos_.at(i);
      add_one_row_fn fn_ptr = nullptr;
      if ((info.param_exprs_.count() <= 0 && !info.is_implicit_first_aggr())
          || info.param_exprs_.count() > 1) {
        fn_ptr = aggregate::add_one_row<ObVectorBase>;
      } else {
        ObDatumMeta meta;
        VectorFormat fmt;
        if (info.is_implicit_first_aggr()) {
          meta = info.expr_->datum_meta_;
          fmt = info.expr_->get_format(agg_ctx_.eval_ctx_);
        } else {
          meta = info.param_exprs_.at(0)->datum_meta_;
          fmt = info.param_exprs_.at(0)->get_format(agg_ctx_.eval_ctx_);
        }
        VecValueTypeClass vec_tc = get_vec_value_tc(meta.type_, meta.scale_, meta.precision_);
        switch(fmt) {
          case common::VEC_UNIFORM: {
            fn_ptr = aggregate::add_one_row<ObUniformFormat<false>>;
            break;
          }
          case common::VEC_UNIFORM_CONST: {
            fn_ptr = aggregate::add_one_row<ObUniformFormat<true>>;
            break;
          }
          case common::VEC_DISCRETE: {
            fn_ptr = aggregate::add_one_row<ObDiscreteFormat>;
            break;
          }
          case common::VEC_CONTINUOUS: {
            fn_ptr = aggregate::add_one_row<ObContinuousFormat>;
            break;
          }
          case common::VEC_FIXED: {
            switch(vec_tc) {
              LST_DO_CODE(GET_FIXED_FN, AGG_FIXED_TC_LIST);
              default: {
                ret = OB_ERR_UNEXPECTED;
              }
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      if (OB_SUCC(ret)) {
        add_one_row_fns_.at(i) = fn_ptr;
      }
    }
  }
  return ret;
}

int Processor::finish_adding_one_row()
{
  int ret = OB_SUCCESS;
  clear_add_one_row_fns();
  return ret;
}

int Processor::llc_init_empty(ObExpr &expr, ObEvalCtx &eval_ctx) const
{
  int ret = OB_SUCCESS;
  char *llc_bitmap_buf = nullptr;
  const int64_t llc_bitmap_size = ObAggregateProcessor::get_llc_size();
  if (OB_ISNULL(llc_bitmap_buf = expr.get_str_res_mem(eval_ctx, llc_bitmap_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    MEMSET(llc_bitmap_buf, 0, llc_bitmap_size);
    expr.get_vector(eval_ctx)->set_payload_shallow(eval_ctx.get_batch_idx(), llc_bitmap_buf,
                                                   llc_bitmap_size);
  }
  return ret;
}

int Processor::add_batch_aggregate_rows(AggrRowPtr *ptrs, uint16_t *selector,
                                        int64_t selector_cnt, bool push_agg_row)
{
  int ret = OB_SUCCESS;
  if (!push_agg_row) {
    for (int64_t i = 0; OB_SUCC(ret) && i < selector_cnt; ++i) {
      if (OB_FAIL(setup_rt_info(ptrs[selector[i]], agg_ctx_))) {
        SQL_LOG(WARN, "setup runtime info failed", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < selector_cnt; ++i) {
      if (OB_FAIL(setup_rt_info(ptrs[selector[i]], agg_ctx_))) {
        SQL_LOG(WARN, "setup runtime info failed", K(ret));
      } else if (push_agg_row && OB_FAIL(agg_ctx_.agg_rows_.push_back(ptrs[selector[i]]))) {
        SQL_LOG(WARN, "push back element failed", K(ret));
      }
    }
  }
  return ret;
}

} // end aggregate
} // end share
} // end oceanbase

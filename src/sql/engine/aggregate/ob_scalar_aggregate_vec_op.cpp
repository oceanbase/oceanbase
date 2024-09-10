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

#include "ob_scalar_aggregate_vec_op.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObScalarAggregateVecSpec, ObScalarAggregateSpec), can_return_empty_set_);

int ObScalarAggregateVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByVecOp::inner_open())) {
    LOG_WARN("groupby inner open failed", K(ret));
  } else if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(dir_id_))) {
    LOG_WARN("failed to allocate dir id", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory context failed", K(ret));
  } else if (OB_FAIL(init_hp_infras_group_mgr())) {
    LOG_WARN("failed to init hp infras group manager", K(ret));
  } else {
    aggr_processor_.set_dir_id(dir_id_);
    aggr_processor_.set_io_event_observer(&io_event_observer_);
    aggr_processor_.set_op_monitor_info(&op_monitor_info_);
    if (OB_FAIL(init_one_aggregate_row())) {
      LOG_WARN("failed to init aggregate row", K(ret));
    } else {
      started_ = false;
    }
  }
  return ret;
}

int ObScalarAggregateVecOp::init_one_aggregate_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggr_processor_.init_aggr_row_meta(row_meta_))) {
    LOG_WARN("failed to init aggregate row meta", K(ret));
  } else if (OB_FAIL(aggr_processor_.init_one_aggr_row(row_meta_, row_,
                                                       mem_context_->get_arena_allocator()))) {
    LOG_WARN("init scalar aggregate row failed", K(ret));
  }
  return ret;
}

int ObScalarAggregateVecOp::inner_close()
{
  started_ = false;
  sql_mem_processor_.unregister_profile();
  return ObGroupByVecOp::inner_close();
}

void ObScalarAggregateVecOp::destroy()
{
  started_ = false;
  sql_mem_processor_.unregister_profile_if_necessary();
  if (OB_NOT_NULL(mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  row_meta_.reset();
  hp_infras_mgr_.destroy();
  ObGroupByVecOp::destroy();
}

int ObScalarAggregateVecOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByVecOp::inner_switch_iterator())) {
    LOG_WARN("failed to switch iterator", K(ret));
  } else if (OB_FAIL(init_one_aggregate_row())) {
    LOG_WARN("failed to init scalar aggregate row", K(ret));
  } else {
    started_ = false;
  }
  return ret;
}

int ObScalarAggregateVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else if (OB_FAIL(init_one_aggregate_row())) {
    LOG_WARN("failed to init scalar aggregate row", K(ret));
  } else {
    started_ = false;
  }
  return ret;
}

int ObScalarAggregateVecOp::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObScalarAggregateVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;

  LOG_DEBUG("before inner get_next_batch", "aggr_hold_size", aggr_processor_.get_aggr_hold_size(),
            "aggr_used_size", aggr_processor_.get_aggr_used_size(), K(batch_cnt));
  if (OB_ISNULL(row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate group row is null", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx_);
    aggregate::AggrRowPtr agg_row = static_cast<char *>(row_->get_extra_payload(row_meta_));
    int32_t start_agg_id = 0, end_agg_id = aggr_processor_.aggregates_cnt();
    bool ret_empty_set = false;
    while (OB_SUCC(ret) && OB_SUCC(child_->get_next_batch(batch_cnt, child_brs))) {
      clear_evaluated_flag();
      if (!started_ && child_brs->end_ && child_brs->size_ == 0) {
        if (MY_SPEC.can_return_empty_set()) {
          ret_empty_set = true;
        } else {
          // no result from first iteration, just return empty
          batch_guard.set_batch_size(1);
          batch_guard.set_batch_idx(0);
          const ObCompactRow *tmp_row = row_;
          if (OB_FAIL(aggr_processor_.collect_empty_set(ObThreeStageAggrStage::THIRD_STAGE
                                                        == MY_SPEC.aggr_stage_))) {
            LOG_WARN("collect scalard results failed", K(ret));
          }
        }
        break;
      } else if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(*child_brs))) {
        LOG_WARN("failed to eval aggregate params batch", K(ret), K(*child_brs));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_LIKELY(ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_)
                 && OB_FAIL(aggr_processor_.add_batch_rows(
                   start_agg_id, end_agg_id, agg_row, *child_brs, static_cast<uint16_t>(0),
                   static_cast<uint16_t>(child_brs->size_)))) {
        LOG_WARN("add batch rows failed", K(ret));
      } else if (ObThreeStageAggrStage::THIRD_STAGE == MY_SPEC.aggr_stage_
                 && OB_FAIL(add_batch_rows_for_3stage(*child_brs, agg_row))) {
        LOG_WARN("add third stage rows failed", K(ret));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        started_ = true;
        if (child_brs->end_) { break; }
      }
    }
    if (OB_SUCC(ret) && child_brs->end_ && started_) {
      batch_guard.set_batch_size(1);
      batch_guard.set_batch_idx(0);
      const ObCompactRow *tmp_row = row_;
      if (OB_FAIL(aggr_processor_.collect_scalar_results(row_meta_, &tmp_row,
                                                         eval_ctx_.get_batch_size()))) {
        LOG_WARN("collect scalar results failed", K(ret));
      }
    }
    if (ret_empty_set) {
      brs_.size_ = 0;
      brs_.end_ = true;
      brs_.all_rows_active_ = false;
    } else {
      brs_.size_ = 1; // make sure return one line for scalar groupby
      brs_.end_ = true;
      brs_.all_rows_active_ = true;
    }
  }
  LOG_DEBUG("after inner_get_next_batch", "hold_mem_size", aggr_processor_.get_aggr_hold_size(),
            "used_mem_size", aggr_processor_.get_aggr_used_size(), K(batch_cnt));
  return ret;
}

int ObScalarAggregateVecOp::add_batch_rows_for_3stage(const ObBatchRows &brs, aggregate::AggrRowPtr agg_row)
{
  int ret = OB_SUCCESS;
  int32_t start_agg_id = -1, end_agg_id = -1;
  for (int i = 0; OB_SUCC(ret) && i < brs.size_; i++) {
    if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(*row_, row_meta_, i, start_agg_id,
                                                          end_agg_id))) {
      LOG_WARN("calculate stage info failed", K(ret));
    } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid start/end aggregate id", K(ret), K(start_agg_id), K(end_agg_id));
    } else if (OB_FAIL(aggr_processor_.add_batch_rows(start_agg_id, end_agg_id, agg_row, brs, i,
                                                      i + 1))) {
      LOG_WARN("add batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObScalarAggregateVecOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (mem_context_ == nullptr) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
        ObModIds::OB_SQL_AGGR_FUNC,
        ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    }
  }
  return ret;
}

int ObScalarAggregateVecOp::init_hp_infras_group_mgr()
{
  int ret = OB_SUCCESS;
  int64_t distinct_cnt = 0;
  uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (aggr_processor_.has_distinct()) {
    int64_t est_rows = MY_SPEC.rows_;
    aggr_processor_.set_io_event_observer(&io_event_observer_);
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows,
                                                  est_rows))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(), tenant_id,
                                               est_rows * MY_SPEC.width_, MY_SPEC.type_,
                                               MY_SPEC.id_, &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (OB_FAIL(hp_infras_mgr_.init(tenant_id, GCONF.is_sql_operator_dump_enabled(),
                                           est_rows, MY_SPEC.width_, true /*unique*/, 1 /*ways*/,
                                           &eval_ctx_, &sql_mem_processor_, &io_event_observer_,
                                           MY_SPEC.compress_type_))) {
      LOG_WARN("failed to init hash infras group", K(ret));
    } else if (FALSE_IT(distinct_cnt = aggr_processor_.get_distinct_count())) {
    } else if (aggr_processor_.has_distinct() && distinct_cnt > 0
        && OB_FAIL(hp_infras_mgr_.reserve_hp_infras(distinct_cnt))) {
      LOG_WARN("failed to reserve", K(ret), K(distinct_cnt));
    } else {
      aggr_processor_.set_hp_infras_mgr(&hp_infras_mgr_);
    }
  }
  return ret;
}

} // end sql
} // end oceanbase
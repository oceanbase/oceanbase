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

#include "sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "sql/engine/px/ob_px_util.h"


namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObScalarAggregateSpec, ObGroupBySpec), enable_hash_base_distinct_);

int ObScalarAggregateOp::inner_open()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_FAIL(ObGroupByOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(tenant_id, dir_id_))) {
    LOG_WARN("failed to alloc dir id", K(ret));
  } else if (FALSE_IT(aggr_processor_.set_dir_id(dir_id_))) {
  } else if (FALSE_IT(aggr_processor_.set_io_event_observer(&io_event_observer_))) {
  } else if (MY_SPEC.enable_hash_base_distinct_
    && OB_FAIL(init_hp_infras_group_mgr())) {
    LOG_WARN("failed to init hp infras group manager", K(ret));
  } else if (OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("failed to init one group",  K(ret));
  } else {
    bool need_dir_id = aggr_processor_.processor_need_alloc_dir_id();
    if (need_dir_id && OB_FAIL(ObChunkStoreUtil::alloc_dir_id(tenant_id, dir_id_))) {
      LOG_WARN("failed to alloc dir id", K(ret));
    } else if (need_dir_id && FALSE_IT(aggr_processor_.set_dir_id(dir_id_))) {
    } else if (FALSE_IT(aggr_processor_.set_io_event_observer(&io_event_observer_))) {
    } else if (OB_FAIL(aggr_processor_.init_one_group())) {
      LOG_WARN("failed to init one group",  K(ret));
    } else {
      started_ = false;
    }
  }
  return ret;
}

int ObScalarAggregateOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByOp::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  } else {
    started_ = false;
  }
  sql_mem_processor_.unregister_profile();
  return ret;
}

void ObScalarAggregateOp::destroy()
{
  started_ = false;
  sql_mem_processor_.unregister_profile_if_necessary();
  hp_infras_mgr_.destroy();
  ObGroupByOp::destroy();
}

int ObScalarAggregateOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByOp::inner_switch_iterator())) {
    LOG_WARN("failed to switch_iterator", K(ret));
  } else if (OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("failed to init one group",  K(ret));
  } else {
    started_ = false;
  }
  return ret;
}

int ObScalarAggregateOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else if (OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("failed to init one group",  K(ret));
  } else {
    started_ = false;
  }
  return ret;
}

int ObScalarAggregateOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (started_) {
    ret = OB_ITER_END;
  } else {
    LOG_DEBUG("before inner_get_next_row",
              "aggr_hold_size", aggr_processor_.get_aggr_hold_size(),
              "aggr_used_size", aggr_processor_.get_aggr_used_size());
    started_ = true;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));

      //聚集的整个集合为空集，对外返回空集分组生成的行
      } else if (OB_FAIL(aggr_processor_.collect_for_empty_set())) {
        LOG_WARN("fail to prepare the aggr func", K(ret));
      }
    } else {
      ObAggregateProcessor::GroupRow *group_row = NULL;
      if (OB_FAIL(aggr_processor_.get_group_row(0, group_row))) {
        LOG_WARN("failed to get_group_row", K(ret));
      } else if (OB_ISNULL(group_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group_row is null", K(ret));
      } else if (OB_FAIL(aggr_processor_.prepare(*group_row))) {
        LOG_WARN("fail to prepare the aggr func", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          clear_evaluated_flag();
          if (OB_FAIL((child_->get_next_row()))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get row from child", K(ret));
            }
          } else if (OB_FAIL(try_check_status())) {
            LOG_WARN("check status failed", K(ret));
          } else if (OB_FAIL(aggr_processor_.process(*group_row))) {
            LOG_WARN("fail to process the aggr func", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          if (OB_FAIL(aggr_processor_.collect())) {
            LOG_WARN("fail to collect result", K(ret));
          }
        }
      }
    }
    LOG_DEBUG("after inner_get_next_row",
              "hold_mem_size", aggr_processor_.get_aggr_hold_size(),
              "used_mem_size", aggr_processor_.get_aggr_used_size());
  }
  return ret;
}

int ObScalarAggregateOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  ObAggregateProcessor::GroupRow *group_row = nullptr;
  const ObBatchRows * child_brs = nullptr;

  LOG_DEBUG("before inner_get_next_batch",
            "aggr_hold_size", aggr_processor_.get_aggr_hold_size(),
            "aggr_used_size", aggr_processor_.get_aggr_used_size(), K(batch_cnt));
  if (OB_FAIL(aggr_processor_.get_group_row(0, group_row))) {
    LOG_WARN("failed to get_group_row", K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    while (OB_SUCC(ret)
           && OB_SUCC(child_->get_next_batch(batch_cnt, child_brs))) {
      clear_evaluated_flag();
      if (!started_ && child_brs->end_ && child_brs->size_ == 0) {
        // no result from 1st iteration, just return empty
        guard.set_batch_size(1);
        guard.set_batch_idx(0);
        if (OB_FAIL(aggr_processor_.collect_for_empty_set())) {
          LOG_WARN("fail to prepare the aggr func", K(ret));
        }
        break;
      } else if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(*child_brs))) {
        LOG_WARN("fail to eval aggr param batch", K(ret), K(*child_brs));
      } else if (OB_FAIL(aggr_processor_.process_batch(*group_row, *child_brs, 0, child_brs->size_))) {
        LOG_WARN("fail to process the aggr func", K(ret));
      }
      started_ = true;
      if (child_brs->end_) {
        break;
      }
    }

    if (OB_SUCC(ret) && child_brs->end_ && started_) {
      guard.set_batch_size(1);
      guard.set_batch_idx(0);
      if (OB_FAIL(aggr_processor_.collect_scalar_batch(*child_brs, 0, nullptr, batch_cnt))) {
        LOG_WARN("fail to collect result", K(ret));
      }
    }
    brs_.size_ = 1; // make sure return one line for scalar groupby
    brs_.end_ = true;
  }
  LOG_DEBUG("after inner_get_next_batch",
            "hold_mem_size", aggr_processor_.get_aggr_hold_size(),
            "used_mem_size", aggr_processor_.get_aggr_used_size(), K(batch_cnt));
  return ret;
}

int ObScalarAggregateOp::init_hp_infras_group_mgr()
{
  int ret = OB_SUCCESS;
  int64_t distinct_cnt = 0;
  uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (aggr_processor_.has_distinct()) {
    int64_t est_rows = MY_SPEC.rows_;
    aggr_processor_.set_io_event_observer(&io_event_observer_);
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
        &ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
                    &ctx_.get_allocator(),
                    tenant_id,
                    est_rows * MY_SPEC.width_,
                    MY_SPEC.type_,
                    MY_SPEC.id_,
                    &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (OB_FAIL(hp_infras_mgr_.init(tenant_id,
      GCONF.is_sql_operator_dump_enabled(), est_rows, MY_SPEC.width_, true/*unique*/, 1/*ways*/,
      &eval_ctx_, &sql_mem_processor_, &io_event_observer_))) {
      LOG_WARN("failed to init hash infras group", K(ret));
    } else if (FALSE_IT(distinct_cnt = aggr_processor_.get_distinct_count())) {
    } else if (aggr_processor_.has_distinct() && distinct_cnt > 0
        && OB_FAIL(hp_infras_mgr_.reserve_hp_infras(distinct_cnt))) {
      LOG_WARN("failed to reserve", K(ret), K(distinct_cnt));
    } else {
      aggr_processor_.set_hp_infras_mgr(&hp_infras_mgr_);
      aggr_processor_.set_enable_hash_distinct();
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

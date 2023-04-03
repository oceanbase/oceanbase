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
#include "lib/number/ob_number_v2.h"


namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObScalarAggregateSpec, ObGroupBySpec));

int ObScalarAggregateOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupByOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(dir_id_))) {
    LOG_WARN("failed to alloc dir id", K(ret));
  } else if (FALSE_IT(aggr_processor_.set_dir_id(dir_id_))) {
  } else if (FALSE_IT(aggr_processor_.set_io_event_observer(&io_event_observer_))) {
  } else if (OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("failed to init one group",  K(ret));
  } else {
    started_ = false;
  }
  return ret;
}

int ObScalarAggregateOp::inner_close()
{
  started_ = false;
  return ObGroupByOp::inner_close();
}

void ObScalarAggregateOp::destroy()
{
  started_ = false;
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
  } else if (OB_FAIL(aggr_processor_.prepare_in_batch_mode(group_row))) {
    LOG_WARN("fail to prepare the aggr func", K(ret));
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

} // end namespace sql
} // end namespace oceanbase

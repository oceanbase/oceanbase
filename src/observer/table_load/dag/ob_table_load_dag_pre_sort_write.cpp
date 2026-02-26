/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_dag_pre_sort_write.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_mem_compact.h"
#include "observer/table_load/dag/ob_table_load_dag_pre_sort_write_channel.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;
using namespace table;

class ObTableLoadPreSortTaskBase : public ObTableLoadMemCompactTaskBase
{
public:
  ObTableLoadPreSortTaskBase(ObTableLoadDag *dag, ObTableLoadDagPreSortWriteChannel *write_channel)
    : ObTableLoadMemCompactTaskBase(dag, &write_channel->mem_compact_ctx_),
      write_channel_(write_channel)
  {
  }
  ObTableLoadPreSortTaskBase(ObTableLoadPreSortTaskBase *parent)
    : ObTableLoadMemCompactTaskBase(parent), write_channel_(parent->write_channel_)
  {
  }
  virtual ~ObTableLoadPreSortTaskBase() = default;

protected:
  ObTableLoadDagPreSortWriteChannel *write_channel_;
};

/**
 * ObTableLoadPreSortWriteSortTask
 */

class ObTableLoadPreSortWriteSortTask final
  : public ObTableLoadDagPreSortWriteChannel::SortChunkTask,
    public ObTableLoadPreSortTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadPreSortWriteSortTask(ObTableLoadPreSortTaskBase *parent, const int64_t chunk_idx)
    : ObTableLoadDagPreSortWriteChannel::SortChunkTask(TASK_TYPE_DIRECT_LOAD_PRE_SORT_WRITE_SORT),
      ObTableLoadPreSortTaskBase(parent),
      chunk_idx_(chunk_idx)
  {
  }
  virtual ~ObTableLoadPreSortWriteSortTask()
  {
    if (nullptr != chunk_) {
      ctx_->release_chunk(chunk_);
      chunk_ = nullptr;
    }
  }
  int process() override;

private:
  const int64_t chunk_idx_;
};

int ObTableLoadPreSortWriteSortTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(chunk_)) {
    CompareType compare;
    if (OB_FAIL(compare.init(*ctx_->datum_utils_, store_ctx_->ctx_->param_.dup_action_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(chunk_->sort(compare, ctx_->enc_params_))) {
      LOG_WARN("fail to sort chunk", KR(ret));
    } else if (OB_FAIL(round_ctx_->add_chunk(chunk_))) {
      LOG_WARN("fail to add chunk", KR(ret));
    } else {
      chunk_ = nullptr;
    }
  }
  return ret;
}

/**
 * ObTableLoadPreSortWriteTask
 */

class ObTableLoadPreSortWriteTask final : public ObITask, public ObTableLoadPreSortTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadPreSortWriteTask(ObTableLoadDag *dag, ObTableLoadDagPreSortWriteChannel *write_channel)
    : ObITask(TASK_TYPE_DIRECT_LOAD_PRE_SORT_WRITE), ObTableLoadPreSortTaskBase(dag, write_channel)
  {
  }
  ObTableLoadPreSortWriteTask(ObTableLoadPreSortWriteTask *parent)
    : ObITask(TASK_TYPE_DIRECT_LOAD_PRE_SORT_WRITE),
      ObTableLoadPreSortTaskBase(parent->dag_, parent->write_channel_)
  {
  }
  ObITaskPriority get_priority() override
  {
    return ObTableLoadDagTaskBase::get_priority(ctx_->can_make_round_ctx());
  }
  int process() override;
};

int ObTableLoadPreSortWriteTask::process()
{
  int ret = OB_SUCCESS;
  if (write_channel_->no_more_closed_chunk()) {
  } else if (OB_FAIL(ctx_->make_round_ctx(round_ctx_))) {
    LOG_WARN("fail to make round ctx", KR(ret));
  } else {
    ObTableLoadPreSortWriteSortTask *sort_task = nullptr;
    ObTableLoadMemCompactSampleTask *sample_task = nullptr;
    ObTableLoadMemCompactDumpTask *dump_task = nullptr;
    ObTableLoadMemCompactCompactTask *compact_task = nullptr;
    ObTableLoadPreSortWriteTask *next_task = nullptr;
    // alloc task
    if (OB_FAIL(dag_->alloc_task(sample_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(dump_task, this, 0 /*range_idx*/))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(compact_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(next_task, this))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->compact_chunk_cnt_; ++i) {
      if (OB_FAIL(dag_->alloc_task(sort_task, this, i))) {
        LOG_WARN("fail to alloc task", KR(ret));
      } else if (OB_FAIL(sort_task->add_child(*sample_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (OB_FAIL(sort_task->add_child(*next_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (OB_FAIL(write_channel_->add_sort_chunk_task(sort_task))) {
        LOG_WARN("fail to add sort task", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    }
    // 建立依赖关系: sort_task -> sample_task -> dump_task -> compact_task -> [next_op_task]
    //                 |
    //                 +-> next_task -> [next_op_task]
    else if (OB_FAIL(sample_task->add_child(*dump_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(dump_task->add_child(*compact_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(compact_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    } else if (OB_FAIL(next_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 添加task
    else if (OB_FAIL(dag_->add_task(*next_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*compact_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*dump_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*sample_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

/*
 * ObTableLoadPreSortWriteOpTask
 */

ObTableLoadPreSortWriteOpTask::ObTableLoadPreSortWriteOpTask(ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PRE_SORT_WRITE_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadPreSortWriteOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadPreSortWriteOp *op = static_cast<ObTableLoadPreSortWriteOp *>(op_);
  FLOG_INFO("[DIRECT_LOAD_OP] pre sort write op start", KP(op));
  op->start_time_ = ObTimeUtil::current_time();

  // init write_channel_
  if (OB_ISNULL(op->write_channel_ =
                  OB_NEWx(ObTableLoadDagPreSortWriteChannel, &op->op_ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagPreSortWriteChannel", KR(ret));
  } else if (OB_FAIL(op->write_channel_->init(dag_, op))) {
    LOG_WARN("fail to init write channel", KR(ret));
  }
  // init write_ctx_
  else {
    store_ctx_->write_ctx_.table_data_desc_ = op->op_ctx_->table_store_.get_table_data_desc();
    store_ctx_->write_ctx_.write_channel_ = op->write_channel_;
    store_ctx_->write_ctx_.is_inited_ = true;
  }

  if (OB_SUCC(ret)) {
    ObTableLoadPreSortWriteTask *first_task = nullptr;
    ObTableLoadDagWriteChannel::FinishTask *write_finish_task = nullptr;
    ObTableLoadPreSortWriteOpFinishTask *op_finish_task = nullptr;
    ObTableLoadDagStartMergeTask *start_merge_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(first_task, dag_, op->write_channel_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(write_finish_task, op->write_channel_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(op_finish_task, dag_, op_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(start_merge_task, dag_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 建立依赖关系:
    // first_task -> write_finish_task -> op_finish_task -> start_merge_task -> [next_op_task]
    else if (OB_FAIL(first_task->add_child(*write_finish_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(write_finish_task->add_child(*op_finish_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(op_finish_task->add_child(*start_merge_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(start_merge_task->deep_copy_children(get_child_nodes()))) {
      LOG_WARN("fail to deep copy children", KR(ret));
    }
    // 添加task
    else if (OB_FAIL(dag_->add_task(*start_merge_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*op_finish_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*write_finish_task))) {
      LOG_WARN("fail to add task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*first_task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadPreSortWriteOpFinishTask
 */

ObTableLoadPreSortWriteOpFinishTask::ObTableLoadPreSortWriteOpFinishTask(ObTableLoadDag *dag,
                                                                         ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_PRE_SORT_WRITE_OP_FINISH), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadPreSortWriteOpFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadPreSortWriteOp *op = static_cast<ObTableLoadPreSortWriteOp *>(op_);
  if (OB_FAIL(op->write_channel_->close())) {
    LOG_WARN("fail to close write channel", KR(ret));
  }

  op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] pre sort write op finish", KP(op), "time_cost",
            op->end_time_ - op->start_time_);

  reset_op(op);
  return ret;
}

void ObTableLoadPreSortWriteOpFinishTask::reset_op(ObTableLoadPreSortWriteOp *op)
{
  if (OB_NOT_NULL(op)) {
    OB_DELETEx(ObTableLoadDagPreSortWriteChannel, &op->op_ctx_->allocator_, op->write_channel_);
  }
}

} // namespace observer
} // namespace oceanbase

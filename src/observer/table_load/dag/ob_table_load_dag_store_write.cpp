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

#include "observer/table_load/dag/ob_table_load_dag_store_write.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_store_write_channel.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/plan/ob_table_load_write_op.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;

/*
 * ObTableLoadStoreWriteOpTask
 */

ObTableLoadStoreWriteOpTask::ObTableLoadStoreWriteOpTask(ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_STORE_WRITE_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadStoreWriteOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadStoreWriteOp *op = static_cast<ObTableLoadStoreWriteOp *>(op_);
  FLOG_INFO("[DIRECT_LOAD_OP] store write op start", KP(op));
  op->start_time_ = ObTimeUtil::current_time();

  // init write_channel_
  if (OB_ISNULL(op->write_channel_ =
                  OB_NEWx(ObTableLoadDagStoreWriteChannel, &op->op_ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadDagStoreWriteChannel", KR(ret));
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
    ObTableLoadDagWriteChannel::FinishTask *write_finish_task = nullptr;
    ObTableLoadStoreWriteOpFinishTask *op_finish_task = nullptr;
    ObTableLoadDagStartMergeTask *start_merge_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(write_finish_task, op->write_channel_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(op_finish_task, dag_, op_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(dag_->alloc_task(start_merge_task, dag_))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 建立依赖关系: write_finish_task -> op_finish_task -> start_merge_task -> [next_op_task]
    else if (OB_FAIL(write_finish_task->add_child(*op_finish_task))) {
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
    }
  }
  return ret;
}

/**
 * ObTableLoadStoreWriteOpFinishTask
 */

ObTableLoadStoreWriteOpFinishTask::ObTableLoadStoreWriteOpFinishTask(ObTableLoadDag *dag,
                                                                     ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_STORE_WRITE_OP_FINISH), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadStoreWriteOpFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadStoreWriteOp *op = static_cast<ObTableLoadStoreWriteOp *>(op_);
  if (OB_FAIL(op->write_channel_->close())) {
    LOG_WARN("fail to close write channel", KR(ret));
  }

  op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] store write op finish", KP(op), "time_cost",
            op->end_time_ - op->start_time_);

  reset_op(op);
  return ret;
}

void ObTableLoadStoreWriteOpFinishTask::reset_op(ObTableLoadStoreWriteOp *op)
{
  if (OB_NOT_NULL(op)) {
    OB_DELETEx(ObTableLoadDagStoreWriteChannel, &op->op_ctx_->allocator_, op->write_channel_);
  }
}

} // namespace observer
} // namespace oceanbase

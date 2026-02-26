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

#include "observer/table_load/dag/ob_table_load_dag_task.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_compact_table_task.h"
#include "observer/table_load/dag/ob_table_load_dag_direct_write.h"
#include "observer/table_load/dag/ob_table_load_dag_insert_sstable_task.h"
#include "observer/table_load/dag/ob_table_load_dag_mem_sort.h"
#include "observer/table_load/dag/ob_table_load_dag_pre_sort_write.h"
#include "observer/table_load/dag/ob_table_load_dag_store_write.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;

/**
 * ObTableLoadDagTaskBase
 */

ObTableLoadDagTaskBase::ObTableLoadDagTaskBase(ObTableLoadDag *dag)
  : store_ctx_(dag->store_ctx_), dag_(dag)
{
}

ObTableLoadDagTaskBase::~ObTableLoadDagTaskBase() {}

/**
 * ObTableLoadDagOpTaskBase
 */

ObTableLoadDagOpTaskBase::ObTableLoadDagOpTaskBase(ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObTableLoadDagTaskBase(dag), op_(op)
{
}

ObTableLoadDagOpTaskBase::~ObTableLoadDagOpTaskBase() {}

int ObTableLoadDagOpTaskBase::create_op_task(ObTableLoadDag *dag, ObTableLoadOp *op,
                                             ObITask *&op_task)
{
  int ret = OB_SUCCESS;
  switch (op->get_op_type()) {
#define OP_TASK_CREATE_SWITCH(op_type, OpType, OpTaskType) \
  case ObTableLoadOpType::op_type: {                       \
    OpTaskType *task = nullptr;                            \
    if (OB_FAIL(dag->alloc_task(task, dag, op))) {         \
      LOG_WARN("fail to alloc task", KR(ret));             \
    } else {                                               \
      op_task = task;                                      \
    }                                                      \
    break;                                                 \
  }

    OB_TABLE_LOAD_DAG_OP_TASK(OP_TASK_CREATE_SWITCH)

#undef OP_TASK_CREATE_SWITCH
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op type", KR(ret), KPC(op));
      break;
  }
  return ret;
}

// start_merge
ObTableLoadDagStartMergeTask::ObTableLoadDagStartMergeTask(ObTableLoadDag *dag)
  : ObITask(TASK_TYPE_DIRECT_LOAD_START_MERGE), ObTableLoadDagTaskBase(dag)
{
}

ObITask::ObITaskPriority ObTableLoadDagStartMergeTask::get_priority()
{
  return ObTableLoadDagTaskBase::get_priority(store_ctx_->is_status_merging());
}

int ObTableLoadDagStartMergeTask::process()
{
  FLOG_INFO("[DIRECT_LOAD_OP] start merge");
  return OB_SUCCESS;
}

// open
ObTableLoadDagTableOpOpenOpTask::ObTableLoadDagTableOpOpenOpTask(ObTableLoadDag *dag,
                                                                 ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_TABLE_OP_OPEN_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagTableOpOpenOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableOpOpenOp *op = static_cast<ObTableLoadTableOpOpenOp *>(op_);
  ObTableLoadTableOp *table_op = op->table_op_;
  FLOG_INFO("[DIRECT_LOAD_OP] table op open", KP(op), KP(table_op), "op_type",
            ObTableLoadOpType::get_type_string(table_op->get_op_type()), "table_id",
            table_op->op_ctx_->store_table_ctx_->table_id_);
  table_op->start_time_ = ObTimeUtil::current_time();

  if (OB_FAIL(table_op->open())) {
    LOG_WARN("fail to open table op", KR(ret), KPC(table_op));
  } else {
    ObSEArray<ObITask *, 2> ddl_start_tasks;
    if (OB_FAIL(dag_->generate_start_tasks(ddl_start_tasks, this))) {
      LOG_WARN("fail to generate start tasks", KR(ret));
    } else if (OB_FAIL(dag_->batch_add_task(ddl_start_tasks))) {
      LOG_WARN("fail to batch add task", KR(ret));
    }
  }
  return ret;
}

// close
ObTableLoadDagTableOpCloseOpTask::ObTableLoadDagTableOpCloseOpTask(ObTableLoadDag *dag,
                                                                   ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_TABLE_OP_CLOSE_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagTableOpCloseOpTask::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableOpCloseOp *op = static_cast<ObTableLoadTableOpCloseOp *>(op_);
  ObTableLoadTableOp *table_op = op->table_op_;
  if (OB_FAIL(table_op->close())) {
    LOG_WARN("fail to close table op", KR(ret), KPC(table_op));
  }

  table_op->end_time_ = ObTimeUtil::current_time();
  FLOG_INFO("[DIRECT_LOAD_OP] table op close", KP(op), KP(table_op), "op_type",
            ObTableLoadOpType::get_type_string(table_op->get_op_type()), "table_id",
            table_op->op_ctx_->store_table_ctx_->table_id_, "time_cost",
            table_op->end_time_ - table_op->start_time_);
  return ret;
}

// finish
ObTableLoadDagFinishOpTask::ObTableLoadDagFinishOpTask(ObTableLoadDag *dag, ObTableLoadOp *op)
  : ObITask(TASK_TYPE_DIRECT_LOAD_FINISH_OP), ObTableLoadDagOpTaskBase(dag, op)
{
}

int ObTableLoadDagFinishOpTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_ctx_->set_status_merged())) {
    LOG_WARN("fail to set status merged", KR(ret));
  }
  FLOG_INFO("[DIRECT_LOAD_OP] finish");
  return ret;
}

} // namespace observer
} // namespace oceanbase

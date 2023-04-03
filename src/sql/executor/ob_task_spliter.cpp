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

#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_info.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "sql/engine/px/exchange/ob_receive_op.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>
ObTaskSpliter::ObTaskSpliter()
    : server_(),
      plan_ctx_(NULL),
      exec_ctx_(NULL),
      allocator_(NULL),
      job_(NULL),
      task_store_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObTaskSpliter::~ObTaskSpliter()
{
  FOREACH_CNT(t, task_store_) {
    if (NULL != *t) {
      (*t)->~ObTaskInfo();
      *t = NULL;
    }
  }
  task_store_.reset();
}

int ObTaskSpliter::init(ObPhysicalPlanCtx *plan_ctx,
                        ObExecContext *exec_ctx,
                        ObJob &job,
                        common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObOpSpec *op_spec_ = job.get_root_spec(); // for static engine
  plan_ctx_ = plan_ctx;
  exec_ctx_ = exec_ctx;
  job_ = &job;
  allocator_ = &allocator;
  if (OB_I(t1) (
        OB_ISNULL(plan_ctx)
        || OB_ISNULL(exec_ctx)
        || OB_ISNULL(op_spec_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid NULL ptr", K(plan_ctx), K(exec_ctx), K(op_spec_));
  } else {
    server_ = exec_ctx->get_task_exec_ctx().get_self_addr();
  }
  return ret;
}

int ObTaskSpliter::find_scan_ops(ObIArray<const ObTableScanSpec*> &scan_ops, const ObOpSpec &op)
{
  return find_scan_ops_inner<true>(scan_ops, op);
}

template <bool NEW_ENG>
int ObTaskSpliter::find_scan_ops_inner(ObIArray<const ENG_OP::TSC *> &scan_ops, const ENG_OP::Root &op)
{
  // 后序遍历，保证scan_ops.at(0)为最左下的叶子节点
  int ret = OB_SUCCESS;
  if (!IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root *child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *child_op))) {
        LOG_WARN("fail to find child scan ops",
                 K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (op.is_table_scan() && op.get_type() != PHY_FAKE_CTE_TABLE) {
    if (static_cast<const ENG_OP::TSC &>(op).use_dist_das()) {
      //do nothing,使用DAS执行TSC，DAS会处理DAS相关信息，不需要调度器感知TSC
    } else if (OB_FAIL(scan_ops.push_back(static_cast<const ENG_OP::TSC *>(&op)))) {
      LOG_WARN("fail to push back table scan op", K(ret));
    }
  }
  return ret;
}

int ObTaskSpliter::find_insert_ops(ObIArray<const ObTableModifySpec *> &insert_ops, const ObOpSpec &op)
{
  return find_insert_ops_inner<true>(insert_ops, op);
}

template <bool NEW_ENG>
int ObTaskSpliter::find_insert_ops_inner(ObIArray<const ENG_OP::TableModify *> &insert_ops, const ENG_OP::Root &op)
{
  int ret = OB_SUCCESS;
  if (IS_TABLE_INSERT(op.get_type())) { // INSERT, REPLACE, INSERT UPDATE, INSERT RETURNING
    if (OB_FAIL(insert_ops.push_back(static_cast<const ENG_OP::TableModify *>(&op)))) {
      LOG_WARN("fail to push back table insert op", K(ret));
    }
  }
  if (OB_SUCC(ret) && !IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root *child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObTaskSpliter::find_insert_ops(insert_ops, *child_op))) {
        LOG_WARN("fail to find child insert ops",
                 K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  return ret;
}

int ObTaskSpliter::create_task_info(ObTaskInfo *&task)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  task = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObTaskInfo))) ||
             OB_ISNULL(task = new (buf) ObTaskInfo(*allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buf or create task info", K(ret));
    if (NULL != buf) {
      allocator_->free(buf);
      buf = NULL;
    }
  } else if (OB_FAIL(task_store_.push_back(task))) {
    LOG_WARN("array push back failed", K(ret));
    task->~ObTaskInfo();
    task = NULL;
    allocator_->free(buf);
    buf = NULL;
  } else {
    task->set_state(OB_TASK_STATE_NOT_INIT);
    task->set_task_split_type(get_type());
    task->set_force_save_interm_result(false);
  }
  return ret;
}

}/* ns ns*/
}/* ns oceanbase */

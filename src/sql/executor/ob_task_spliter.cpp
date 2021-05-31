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
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_job_conf.h"
#include "sql/executor/ob_task_info.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/executor/ob_receive.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>
ObTaskSpliter::ObTaskSpliter()
    : server_(),
      plan_ctx_(NULL),
      exec_ctx_(NULL),
      allocator_(NULL),
      job_(NULL),
      job_conf_(NULL),
      task_store_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

ObTaskSpliter::~ObTaskSpliter()
{
  FOREACH_CNT(t, task_store_)
  {
    if (NULL != *t) {
      (*t)->~ObTaskInfo();
      *t = NULL;
    }
  }
  task_store_.reset();
}

int ObTaskSpliter::init(
    ObPhysicalPlanCtx* plan_ctx, ObExecContext* exec_ctx, ObJob& job, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* op = job.get_root_op();
  plan_ctx_ = plan_ctx;
  exec_ctx_ = exec_ctx;
  job_ = &job;
  allocator_ = &allocator;
  if (OB_I(t1)(OB_ISNULL(plan_ctx) || OB_ISNULL(exec_ctx) || OB_ISNULL(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job root op", K(plan_ctx), K(exec_ctx), K(op));
  } else {
    server_ = exec_ctx->get_task_exec_ctx().get_self_addr();
    if (IS_TRANSMIT(op->get_type())) {
      ObTransmit* transmit = static_cast<ObTransmit*>(op);
      job_conf_ = &transmit->get_job_conf();
    }
  }
  return ret;
}

int ObTaskSpliter::find_scan_ops(ObIArray<const ObTableScanSpec*>& scan_ops, const ObOpSpec& op)
{
  return find_scan_ops_inner<true>(scan_ops, op);
}

int ObTaskSpliter::find_scan_ops(ObIArray<const ObTableScan*>& scan_ops, const ObPhyOperator& op)
{
  return find_scan_ops_inner<false>(scan_ops, op);
}

template <bool NEW_ENG>
int ObTaskSpliter::find_scan_ops_inner(ObIArray<const ENG_OP::TSC*>& scan_ops, const ENG_OP::Root& op)
{
  // Post-order traversal, make sure that scan_ops.at(0) is the lower left leaf node.
  int ret = OB_SUCCESS;
  if (!IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root* child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *child_op))) {
        LOG_WARN("fail to find child scan ops", K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (op.is_table_scan() && op.get_type() != PHY_FAKE_CTE_TABLE) {
    if (OB_FAIL(scan_ops.push_back(static_cast<const ENG_OP::TSC*>(&op)))) {
      LOG_WARN("fail to push back table scan op", K(ret));
    }
  }
  return ret;
}

int ObTaskSpliter::find_insert_ops(ObIArray<const ObTableModifySpec*>& insert_ops, const ObOpSpec& op)
{
  return find_insert_ops_inner<true>(insert_ops, op);
}

int ObTaskSpliter::find_insert_ops(ObIArray<const ObTableModify*>& insert_ops, const ObPhyOperator& op)
{
  return find_insert_ops_inner<false>(insert_ops, op);
}

template <bool NEW_ENG>
int ObTaskSpliter::find_insert_ops_inner(ObIArray<const ENG_OP::TableModify*>& insert_ops, const ENG_OP::Root& op)
{
  int ret = OB_SUCCESS;
  if (IS_TABLE_INSERT(op.get_type())) {  // INSERT, REPLACE, INSERT UPDATE, INSERT RETURNING
    if (OB_FAIL(insert_ops.push_back(static_cast<const ENG_OP::TableModify*>(&op)))) {
      LOG_WARN("fail to push back table insert op", K(ret));
    }
  }
  if (OB_SUCC(ret) && !IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root* child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObTaskSpliter::find_insert_ops(insert_ops, *child_op))) {
        LOG_WARN("fail to find child insert ops", K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  return ret;
}

int ObTaskSpliter::find_all_table_location_keys(ObIArray<TableLocationKey>& table_location_keys, const ObOpSpec& op)
{
  return find_all_table_location_keys_inner<true>(table_location_keys, op);
}

int ObTaskSpliter::find_all_table_location_keys(
    ObIArray<TableLocationKey>& table_location_keys, const ObPhyOperator& op)
{
  return find_all_table_location_keys_inner<false>(table_location_keys, op);
}

template <bool NEW_ENG>
int ObTaskSpliter::find_all_table_location_keys_inner(
    ObIArray<TableLocationKey>& table_location_keys, const ENG_OP::Root& op)
{
  int ret = OB_SUCCESS;
  TableLocationKey table_location_key;
  if (!IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root* child_op = op.get_child(i);
      CK(OB_NOT_NULL(child_op));
      OZ(find_all_table_location_keys(table_location_keys, *child_op), op.get_id(), child_op->get_id());
    }
  }
  if (OB_SUCC(ret)) {
    if (op.is_table_scan() && op.get_type() != PHY_FAKE_CTE_TABLE) {
      table_location_key.table_id_ = static_cast<const ENG_OP::TSC&>(op).get_table_location_key();
      table_location_key.ref_table_id_ = static_cast<const ENG_OP::TSC&>(op).get_location_table_id();
      OZ(table_location_keys.push_back(table_location_key), table_location_key);
    }
    if (OB_SUCC(ret) && PHY_MV_TABLE_SCAN == op.get_type()) {
      const ENG_OP::MV_TSC& mv_scan_op = static_cast<const ENG_OP::MV_TSC&>(op);
      table_location_key.table_id_ = mv_scan_op.get_right_table_location_key();
      table_location_key.ref_table_id_ = mv_scan_op.get_right_table_location_key();
      OZ(table_location_keys.push_back(table_location_key), table_location_key);
    }

    if (OB_SUCC(ret) && op.is_dml_operator()) {
      const ENG_OP::TableModify& table_modify = static_cast<const ENG_OP::TableModify&>(op);
      if (!table_modify.is_multi_dml()) {
        table_location_key.table_id_ = table_modify.get_table_id();
        table_location_key.ref_table_id_ = table_modify.get_index_tid();
        OZ(table_location_keys.push_back(table_location_key), table_location_key);
      }
    }
  }
  return ret;
}

int ObTaskSpliter::check_dml_exist(bool& exist, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (op.is_dml_operator()) {
    exist = true;
  }
  if (OB_SUCC(ret) && !IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && !exist && i < op.get_child_num(); i++) {
      const ObPhyOperator* child = op.get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL operator", K(ret));
      } else if (OB_FAIL(check_dml_exist(exist, *child))) {
        LOG_WARN("check dml operator exist failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTaskSpliter::create_task_info(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  task = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
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
    task->set_root_op(job_->get_root_op());
    task->set_state(OB_TASK_STATE_NOT_INIT);
    task->set_task_split_type(get_type());
    task->set_force_save_interm_result(false);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

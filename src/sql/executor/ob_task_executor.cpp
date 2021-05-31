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

#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_task_executor.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTaskExecutor::ObTaskExecutor()
{}

ObTaskExecutor::~ObTaskExecutor()
{}

int ObTaskExecutor::execute(ObExecContext& query_ctx, ObJob* job, ObTaskInfo* task_info)
{
  UNUSED(query_ctx);
  UNUSED(job);
  UNUSED(task_info);
  return OB_NOT_IMPLEMENT;
}

// Recursively construct the Input of each Opeator belonging to this task
// Note: There can be multiple tasks of the same structure under a job,
//    but their parameters are different (task_info)
// For each task, you need to call build_task_op_input
int ObTaskExecutor::build_task_op_input(ObExecContext& query_ctx, ObTaskInfo& task_info, const ObPhyOperator& root_op)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* child_op = NULL;
  ObIPhyOperatorInput* op_input = GET_PHY_OP_INPUT(ObIPhyOperatorInput, query_ctx, root_op.get_id());
  // Some ops have no input and continue to recurse when NULL == op_input
  if (NULL != op_input) {
    op_input->reset();
    OZ(op_input->init(query_ctx, task_info, root_op));
  }
  if (OB_SUCC(ret)) {
    if (!IS_RECEIVE(root_op.get_type())) {
      for (int32_t i = 0; OB_SUCC(ret) && i < root_op.get_child_num(); ++i) {
        if (OB_ISNULL(child_op = root_op.get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(OB_I(t2) build_task_op_input(query_ctx, task_info, *child_op))) {
          LOG_WARN("fail to build child op input", K(ret), K(i), K(child_op->get_id()));
        }
      }
    } else {
      // Encounter ObReceive, stop recursion
    }
  }
  return ret;
}

int ObTaskExecutor::should_skip_failed_tasks(ObTaskInfo& task_info, bool& skip_failed_tasks) const
{
  int ret = OB_SUCCESS;
  // when the task only involves virtual tables, the failed tasks are skipped
  skip_failed_tasks = false;
  ObPhyOperator* root_op = NULL;
  ObSEArray<const ObTableScan*, 1> scan_ops;
  if (OB_ISNULL(root_op = task_info.get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("root op is NULL", K(ret), K(task_info));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("fail to find scan ops", K(ret), K(*root_op), K(task_info));
  } else if (scan_ops.count() > 0) {
    skip_failed_tasks = true;
    for (int64_t i = 0; OB_SUCC(ret) && true == skip_failed_tasks && i < scan_ops.count(); ++i) {
      const ObTableScan* scan_op = scan_ops.at(i);
      if (OB_ISNULL(scan_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("scan op is NULL", K(ret), K(i), K(*root_op), K(task_info));
      } else if (!is_virtual_table(scan_op->get_ref_table_id())) {
        skip_failed_tasks = false;
      }
    }
  }
  return ret;
}

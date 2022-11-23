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
#include "sql/engine/px/exchange/ob_receive_op.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTaskExecutor::ObTaskExecutor()
{
}

ObTaskExecutor::~ObTaskExecutor()
{
}

int ObTaskExecutor::execute(ObExecContext &query_ctx, ObJob *job, ObTaskInfo *task_info)
{
  UNUSED(query_ctx);
  UNUSED(job);
  UNUSED(task_info);
  return OB_NOT_IMPLEMENT;
}

// for static engine
int ObTaskExecutor::build_task_op_input(ObExecContext &query_ctx,
                                        ObTaskInfo &task_info,
                                        const ObOpSpec &root_spec)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *child_op = NULL;
  ObOpInput *op_input = query_ctx.get_operator_kit(root_spec.get_id())->input_;

  if (NULL != op_input) {
    op_input->reset();
    OZ(op_input->init(task_info));
  }
  if (OB_SUCC(ret)) {
    if (!IS_RECEIVE(root_spec.get_type())) {
      for (int32_t i = 0; OB_SUCC(ret) && i < root_spec.get_child_num(); ++i) {
        if (OB_ISNULL(child_op = root_spec.get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(OB_I(t2) build_task_op_input(query_ctx, task_info, *child_op))) {
          LOG_WARN("fail to build child op input", K(ret), K(i), K(child_op->get_id()));
        }
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTaskExecutor::should_skip_failed_tasks(ObTaskInfo &task_info, bool &skip_failed_tasks) const
{
  int ret = OB_SUCCESS;
  // 目前的情况下，当该task只涉及到虚拟表的时候，则跳过失败的那些task
  skip_failed_tasks = false;
  ObOpSpec *root_spec = task_info.get_root_spec();

  ObSEArray<const ObTableScanSpec*, 1> scan_specs;
  if (OB_ISNULL(root_spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("root_spec is NULL", K(ret));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_specs, *root_spec))) {
    LOG_WARN("fail to find scan specs", K(ret), K(*root_spec), K(task_info));
  } else if (scan_specs.count() > 0) {
    skip_failed_tasks = true;
    for (int64_t i = 0;
         OB_SUCC(ret) && true == skip_failed_tasks && i < scan_specs.count();
         ++i) {
      const ObTableScanSpec *scan_spec = scan_specs.at(i);
      if (OB_ISNULL(scan_spec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("scan op is NULL", K(ret), K(i), K(task_info));
      } else if (!is_virtual_table(scan_spec->ref_table_id_)) {
        skip_failed_tasks = false;
      }
    }
  }


  return ret;
}




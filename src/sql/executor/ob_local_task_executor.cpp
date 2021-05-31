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

#include "sql/executor/ob_local_task_executor.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObLocalTaskExecutor::ObLocalTaskExecutor()
{}

ObLocalTaskExecutor::~ObLocalTaskExecutor()
{}

int ObLocalTaskExecutor::execute(ObExecContext& ctx, ObJob* job, ObTaskInfo* task_info)
{
  UNUSED(job);

  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObExecuteResult& exec_result = task_exec_ctx.get_execute_result();

  if (OB_ISNULL(task_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("job or taskinfo not set", K(task_info));
  } else {
    if (OB_UNLIKELY(NULL == (root_op = task_info->get_root_op()))) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail execute task. no query found.", K(root_op), K(ret));
    } else if (OB_FAIL(build_task_op_input(ctx, *task_info, *root_op))) {
      LOG_WARN("fail to build op input", K(ret));
    } else {
      // set root op into executor result
      exec_result.set_root_op(root_op);
      task_info->set_state(OB_TASK_STATE_RUNNING);
    }

    if (OB_FAIL(ret)) {
      task_info->set_state(OB_TASK_STATE_FAILED);
    }
  }
  NG_TRACE_EXT(local_task_completed, OB_ID(ret), ret);
  return ret;
}

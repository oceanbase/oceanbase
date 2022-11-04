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
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/utility/utility.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObLocalTaskExecutor::ObLocalTaskExecutor()
{
}

ObLocalTaskExecutor::~ObLocalTaskExecutor()
{
}

// 执行Task，负责控制Task的执行流程，通过ObTransmit结果汇报/输出结果到客户端.
// execute内部负责重试逻辑管理，不对外暴露retry()接口
int ObLocalTaskExecutor::execute(ObExecContext &ctx, ObJob *job, ObTaskInfo *task_info)
{
  UNUSED(job);
  int ret = OB_SUCCESS;
  ObOpSpec *root_spec_ = NULL; // for static engine
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObExecuteResult &exec_result = task_exec_ctx.get_execute_result();

  if (OB_ISNULL(task_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("job or taskinfo not set", K(task_info));
  } else {
    if (OB_ISNULL(root_spec_ = task_info->get_root_spec())) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail execute task. no query found.", K(ret));
    } else if (OB_FAIL(build_task_op_input(ctx, *task_info, *root_spec_))) {
      LOG_WARN("fail to build op input", K(ret));
    } else {
      LOG_DEBUG("static engine remote execute");
      ObOperator *op = NULL;
      if (OB_FAIL(root_spec_->create_operator(ctx, op))) {
        LOG_WARN("create operator from spec failed", K(ret));
      } else if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("created operator is NULL", K(ret));
      } else {
        exec_result.set_static_engine_root(op);
      }

      // 将task_info设成OB_TASK_STATE_RUNNING状态，
      // 后面如果需要重试则会根据这个状态将该task_info中的partition信息
      // 加入到需要重试的partition里面
      task_info->set_state(OB_TASK_STATE_RUNNING);
    }

    if (OB_FAIL(ret)) {
      // 如果失败了，则将task_info设成OB_TASK_STATE_FAILED状态，
      // 这样后面如果需要重试则会根据这个状态将该task_info中的partition信息
      // 加入到需要重试的partition里面
      task_info->set_state(OB_TASK_STATE_FAILED);
    }
  }
  NG_TRACE_EXT(local_task_completed, OB_ID(ret), ret);
  return ret;
}

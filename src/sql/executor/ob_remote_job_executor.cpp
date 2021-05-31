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

#include "lib/container/ob_array.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_remote_job_executor.h"

namespace oceanbase {
namespace sql {

using namespace oceanbase::common;

ObRemoteJobExecutor::ObRemoteJobExecutor() : job_(NULL), executor_(NULL)
{}

ObRemoteJobExecutor::~ObRemoteJobExecutor()
{}

int ObRemoteJobExecutor::execute(ObExecContext& query_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task_info = NULL;

  if (OB_ISNULL(job_) || OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_ or executor_ is NULL", K(ret), K(job_), K(executor_));
  } else if (OB_FAIL(get_executable_task(query_ctx, task_info))) {  // get task info
    LOG_WARN("fail get a executable task", K(ret));
  } else if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info is NULL", K(ret));
  } else if (OB_FAIL(executor_->execute(query_ctx, job_,
                 task_info))) {  // job_ + task_info as task's frame and param
    LOG_WARN("fail execute task", K(ret), K(*task_info));
  } else {
  }
  return ret;
}

int ObRemoteJobExecutor::get_executable_task(ObExecContext& ctx, ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  ObTaskControl* tq = NULL;
  ObArray<ObTaskInfo*> ready_tasks;

  if (OB_ISNULL(job_) || OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_ or executor_ is NULL", K(ret), K(job_), K(executor_));
  } else if (OB_FAIL(job_->get_task_control(ctx, tq))) {
    LOG_WARN("fail get task control", K(ret));
  } else if (OB_ISNULL(tq)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to get task control, but task control is NULL", K(ret));
  } else if (OB_FAIL(tq->get_ready_tasks(ready_tasks))) {
    LOG_WARN("fail get ready task", K(ret));
  } else if (OB_UNLIKELY(1 != ready_tasks.count())) {
    LOG_WARN("unexpected ready task count", "count", ready_tasks.count());
  } else if (OB_FAIL(ready_tasks.at(0, task_info))) {
    LOG_WARN("fail get task from array", K(ret));
  } else {
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

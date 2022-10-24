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

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;

ObRemoteJobExecutor::ObRemoteJobExecutor()
  : job_(NULL),
    executor_(NULL)
{
}

ObRemoteJobExecutor::~ObRemoteJobExecutor()
{
}

int ObRemoteJobExecutor::execute(ObExecContext &query_ctx)
{
  int ret = OB_SUCCESS;
  // ObTask只作为序列化用，故而只需要是一个栈变量即可
  ObTaskInfo *task_info = NULL;

  if (OB_ISNULL(job_) || OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_ or executor_ is NULL", K(ret), K(job_), K(executor_));
  } else if (OB_FAIL(get_executable_task(query_ctx, task_info))) { // 获得一个task info
    LOG_WARN("fail get a executable task", K(ret));
  } else if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info is NULL", K(ret));
  } else if (OB_FAIL(executor_->execute(query_ctx,
                                        job_,
                                        task_info))) { // job_ + task_info 作为 task 的骨架和参数
    LOG_WARN("fail execute task", K(ret), K(*task_info));
  } else {}
  return ret;
}

/**
 * Task均用于读取单表物理数据, 因此Task的划分规则与LocationCache有关
 * 获得的Location均保存到Task结构中
 *
 * 同时，RemoteJobExecutor所执行的Job只会读取单分区的数据, 所以这里TaskControl还是只会输出一个Task
 * 如何划分Task，是根据TaskControl中的task_spliter决定
 */
int ObRemoteJobExecutor::get_executable_task(ObExecContext &ctx, ObTaskInfo *&task_info)
{
  int ret = OB_SUCCESS;
  ObTaskControl *tq = NULL;
  ObArray<ObTaskInfo *> ready_tasks;

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
  } else {}
  return ret;
}

} /* ns sql */
} /* ns oceanbase */

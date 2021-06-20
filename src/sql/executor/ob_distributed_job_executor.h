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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_JOB_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_JOB_EXECUTOR_

#include "sql/executor/ob_distributed_task_executor.h"
#include "sql/executor/ob_task_event.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace common {
class ObLightyQueue;
}
namespace sql {
class ObJob;
class ObExecContext;
class ObDistributedJobExecutor {
public:
  ObDistributedJobExecutor();
  virtual ~ObDistributedJobExecutor();

  int execute_step(ObExecContext& ctx);
  int kill_job(ObExecContext& ctx);
  int close_all_results(ObExecContext& ctx);

  inline ObJob* get_job()
  {
    return job_;
  }
  inline void set_job(ObJob& job)
  {
    job_ = &job;
  }
  inline void set_task_executor(ObDistributedTaskExecutor& executor)
  {
    executor_ = &executor;
  }
  inline void reset()
  {
    job_ = NULL;
    executor_ = NULL;
  }

private:
  int get_executable_tasks(const ObExecContext& ctx, common::ObArray<ObTaskInfo*>& ready_tasks);

  ObJob* job_;
  ObDistributedTaskExecutor* executor_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedJobExecutor);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_JOB_EXECUTOR_ */

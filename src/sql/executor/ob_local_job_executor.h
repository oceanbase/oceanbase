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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_LOCAL_JOB_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_OB_LOCAL_JOB_EXECUTOR_
namespace oceanbase
{
namespace sql
{
class ObTaskInfo;
class ObJob;
class ObTaskExecutor;
class ObExecContext;
class ObLocalJobExecutor
{
public:
  ObLocalJobExecutor();
  virtual ~ObLocalJobExecutor();
  // 设置待调度Job
  void set_job(ObJob &job) { job_ = &job; }
  void set_task_executor(ObTaskExecutor &executor) { executor_ = &executor; }
  // 调度Job，将Job中的Task分发出去执行
  // Job暴露出足够的状态接口，Execute就可以获得正确的待执行Task
  int execute(ObExecContext &ctx);
  inline void reset () { job_ = NULL; executor_ = NULL; }
private:
  // disallow copy
  ObLocalJobExecutor(const ObLocalJobExecutor &other);
  ObLocalJobExecutor &operator=(const ObLocalJobExecutor &ohter);

  int get_executable_task(ObExecContext &ctx, ObTaskInfo *&task);
private:
  ObJob *job_;
  ObTaskExecutor *executor_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_LOCAL_JOB_EXECUTOR_ */
//// end of header file


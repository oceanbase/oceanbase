/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_LOCAL_TASK_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_LOCAL_TASK_EXECUTOR_

#include "sql/executor/ob_task_executor.h"
namespace oceanbase
{
namespace sql
{
class ObLocalTaskExecutor : public ObTaskExecutor
{
public:
  ObLocalTaskExecutor();
  virtual ~ObLocalTaskExecutor();
  virtual int execute(ObExecContext &ctx, ObJob *job, ObTaskInfo *task_info);
  inline virtual void reset() { ObTaskExecutor::reset(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLocalTaskExecutor);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_LOCAL_TASK_EXECUTOR_ */
//// end of header file


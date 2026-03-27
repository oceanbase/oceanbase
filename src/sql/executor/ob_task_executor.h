/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_TASK_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_TASK_EXECUTOR_
#include "ob_task_info.h"

namespace oceanbase
{
namespace sql
{
class ObJob;
class ObTaskInfo;
class ObExecContext;
class ObTaskExecutor
{
public:
  ObTaskExecutor();
  virtual ~ObTaskExecutor();
  virtual int execute(ObExecContext &query_ctx, ObJob *job, ObTaskInfo *task_info) = 0;
  inline virtual void reset() {}
protected:
  int build_task_op_input(ObExecContext &query_ctx,
                          ObTaskInfo &task_info,
                          const ObOpSpec &root_spec); // for static engine
  int should_skip_failed_tasks(ObTaskInfo &task_info, bool &skip_failed_tasks) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskExecutor);
};
} /* ns sql */
} /* ns oceanbase */
#endif /* OCEANBASE_SQL_EXECUTOR_TASK_EXECUTOR_ */
//// end of header file

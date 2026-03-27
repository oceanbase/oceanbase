/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_REMOTE_IDENTITY_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_OB_REMOTE_IDENTITY_TASK_SPLITER_

#include "sql/executor/ob_task_spliter.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;
class ObTaskInfo;
class ObRemoteIdentityTaskSpliter : public ObTaskSpliter
{
public:
  ObRemoteIdentityTaskSpliter();
  virtual ~ObRemoteIdentityTaskSpliter();
  virtual int get_next_task(ObTaskInfo *&task);
  inline virtual TaskSplitType get_type() const { return ObTaskSpliter::REMOTE_IDENTITY_SPLIT; }
private:
  ObTaskInfo *task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteIdentityTaskSpliter);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_REMOTE_IDENTITY_TASK_SPLITER_ */


/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_LOCAL_IDENTITY_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_OB_LOCAL_IDENTITY_TASK_SPLITER_

#include "sql/executor/ob_task_spliter.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;
class ObTaskInfo;

// 该类仅用于只生成一个本地task的情况，而在executor阶段如果判断到
// 切分类型为ObTaskSpliter::LOCAL_IDENTITY_SPLIT则会直接优化掉，不走切分job的流程，
// 相当于这个类的大部分函数都不会被调用到
class ObLocalIdentityTaskSpliter : public ObTaskSpliter
{
public:
  ObLocalIdentityTaskSpliter();
  virtual ~ObLocalIdentityTaskSpliter();
  virtual int get_next_task(ObTaskInfo *&task);
  inline virtual TaskSplitType get_type() const { return ObTaskSpliter::LOCAL_IDENTITY_SPLIT; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLocalIdentityTaskSpliter);
private:
  ObTaskInfo *task_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_LOCAL_IDENTITY_TASK_SPLITER_ */
//// end of header file


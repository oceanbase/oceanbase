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


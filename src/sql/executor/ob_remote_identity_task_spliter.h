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


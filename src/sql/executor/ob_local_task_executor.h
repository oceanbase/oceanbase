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


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

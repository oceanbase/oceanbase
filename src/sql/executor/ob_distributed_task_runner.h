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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_RUNNER_
#define OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_RUNNER_
#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "sql/executor/ob_task_event.h"
namespace oceanbase {
namespace sql {
class ObExecContext;
class ObPhysicalPlan;
class ObDistributedTaskRunner {
public:
  ObDistributedTaskRunner();
  virtual ~ObDistributedTaskRunner();
  int execute(ObExecContext& ctx, ObPhysicalPlan& phy_plan, common::ObIArray<ObSliceEvent>& slice_events);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedTaskRunner);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_RUNNER_ */

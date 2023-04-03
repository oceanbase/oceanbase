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

#ifndef OCEANBASE_SQL_EXECUTOR_REMOTE_SCHEDULER_
#define OCEANBASE_SQL_EXECUTOR_REMOTE_SCHEDULER_

#include "share/ob_define.h"
#include "sql/plan_cache/ob_cache_object.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;
class ObExecContext;
class ObRemoteTask;
class ObRemoteScheduler
{
public:
  ObRemoteScheduler();
  virtual ~ObRemoteScheduler();
  int schedule(ObExecContext &ctx, ObPhysicalPlan *phy_plan);
private:
  int execute_with_plan(ObExecContext &ctx, ObPhysicalPlan *phy_plan);
  int execute_with_sql(ObExecContext &ctx, ObPhysicalPlan *phy_plan);
  int build_remote_task(ObExecContext &ctx,
      ObRemoteTask &remote_task,
      const DependenyTableStore &dependency_tables);
  // variable
  // functions
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRemoteScheduler);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_REMOTE_SCHEDULER_ */
//// end of header file

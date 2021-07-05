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

#ifndef OCEANBASE_SQL_EXECUTOR_LOCAL_SCHEDULER_
#define OCEANBASE_SQL_EXECUTOR_LOCAL_SCHEDULER_

#include "sql/executor/ob_sql_scheduler.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/executor/ob_addrs_provider_factory.h"
#include "sql/executor/ob_local_job_control.h"

namespace oceanbase {
namespace sql {

class ObLocalScheduler : public ObSqlScheduler {
public:
  ObLocalScheduler();
  virtual ~ObLocalScheduler();
  virtual int schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan);

private:
  int direct_generate_task_and_execute(
      ObExecContext& ctx, const ObExecutionID& ob_execution_id, ObPhyOperator* root_op);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLocalScheduler);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __SQL_EXECUTOR_LOCAL_SCHEDULER_ */
//// end of header file

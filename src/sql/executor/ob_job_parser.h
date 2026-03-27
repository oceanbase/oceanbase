/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_EXECUTOR_OB_JOB_PARSER_
#define OCEANBASE_SQL_EXECUTOR_OB_JOB_PARSER_

#include "sql/engine/ob_physical_plan.h"
#include "sql/executor/ob_job_control.h"
#include "sql/executor/ob_task_spliter_factory.h"

namespace oceanbase
{
namespace sql
{
class ObJobParser
{
public:
  ObJobParser();
  virtual ~ObJobParser();

  void reset() {}

  int parse_job(ObExecContext &ctx,
                ObPhysicalPlan *plan,
                const ObExecutionID &ob_execution_id,
                ObTaskSpliterFactory &spliter_factory,
                ObJobControl &jc) const;

private:
  int split_jobs(ObExecContext &ctx,
                 ObPhysicalPlan *phy_plan,
                 ObOpSpec *op_spec,
                 const ObExecutionID &ob_execution_id,
                 ObJobControl &jc,
                 ObTaskSpliterFactory &spliter_factory,
                 ObJob &cur_job) const;

  int create_job(
      ObExecContext &ctx,
      ObPhysicalPlan *phy_plan,
      ObOpSpec *op_spec,
      const ObExecutionID &ob_execution_id,
      ObJobControl &jc,
      const int task_split_type,
      ObTaskSpliterFactory &spfactory,
      ObJob *&job) const;

  bool is_outer_join_child(const ObOpSpec &op_spec) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObJobParser);
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_JOB_PARSER_ */


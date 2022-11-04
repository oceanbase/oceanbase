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


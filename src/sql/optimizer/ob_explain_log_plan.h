/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPLAIN_LOG_PLAN_H
#define _OB_EXPLAIN_LOG_PLAN_H
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{
  class ObExplainLogPlan : public ObLogPlan
  {
  public:
    ObExplainLogPlan(ObOptimizerContext &ctx, const ObDMLStmt *explain_stmt)
      : ObLogPlan(ctx, explain_stmt)
    {}
    virtual ~ObExplainLogPlan() {}
  protected:
    virtual int generate_normal_raw_plan() override;
  private:
    int check_explain_generate_plan_with_outline(ObLogPlan *real_plan);
    DISALLOW_COPY_AND_ASSIGN(ObExplainLogPlan);
  };
}
}
#endif

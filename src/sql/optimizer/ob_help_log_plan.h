/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OPTIMITZER_OB_HELP_LOG_PLAN_H
#define OCEANBASE_SQL_OPTIMITZER_OB_HELP_LOG_PLAN_H
#include "sql/optimizer/ob_log_plan.h"
#include "sql/resolver/cmd/ob_help_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObHelpLogPlan : public ObLogPlan
{
public:
 ObHelpLogPlan(ObOptimizerContext &ctx, const ObDMLStmt *help_stmt)
     : ObLogPlan(ctx, help_stmt)
     {}
  virtual ~ObHelpLogPlan() {}
protected:
  int generate_normal_raw_plan();
private:
  DISALLOW_COPY_AND_ASSIGN(ObHelpLogPlan);
};
}// sql
}// oceanbase
#endif

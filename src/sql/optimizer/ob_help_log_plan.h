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

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

#ifndef _OB_EXPLAIN_LOG_PLAN_H
#define _OB_EXPLAIN_LOG_PLAN_H
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase {
namespace sql {
class ObExplainLogPlan : public ObLogPlan {
public:
  ObExplainLogPlan(ObOptimizerContext& ctx, const ObDMLStmt* explain_stmt) : ObLogPlan(ctx, explain_stmt)
  {}
  virtual ~ObExplainLogPlan()
  {}

  int generate_raw_plan();

  virtual int generate_plan();

private:
  DISALLOW_COPY_AND_ASSIGN(ObExplainLogPlan);
};
}  // namespace sql
}  // namespace oceanbase
#endif

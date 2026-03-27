/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_H
#define OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_H

#include "sql/optimizer/optimizer_plan_rewriter/ob_optimize_rule.h"

namespace oceanbase
{
namespace sql
{
class ObLogPlan;

class JoinFilterPushdown : public ObOptimizeRule
{
public:
  JoinFilterPushdown();
  virtual ~JoinFilterPushdown();
  int apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) override;
  bool is_enabled(const ObOptimizerContext& ctx) override;
  ObString get_name() const override {
    return ObString::make_string("JoinFilterPushdown");
  }
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_JOIN_FILTER_PUSHDOWN_H

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_H
 #define OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_H

 #include "sql/optimizer/optimizer_plan_rewriter/ob_optimize_rule.h"
 #include "sql/optimizer/ob_optimizer_context.h"

 namespace oceanbase
 {
 namespace sql
 {
 class ObLogPlan;

 class PartialLimitPushdown : public ObOptimizeRule
 {
 public:
 PartialLimitPushdown();
   virtual ~PartialLimitPushdown();
   int apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) override;
   bool is_enabled(const ObOptimizerContext& ctx) override
   {
     // not inner session
     return ctx.enable_partial_limit_pushdown();
   }
   ObString get_name() const override {
    return ObString::make_string("PartialLimitPushdown");
   }
 };

 } // namespace sql
 } // namespace oceanbase

 #endif // OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_H


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

 #ifndef OCEANBASE_SQL_OB_PREPROCESS_PLAN_TREE_H
 #define OCEANBASE_SQL_OB_PREPROCESS_PLAN_TREE_H

 #include "sql/optimizer/optimizer_plan_rewriter/ob_optimize_rule.h"
 #include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"

 namespace oceanbase
 {
 namespace sql
 {
 class ObLogPlan;

 class PlanNodeIdAllocator : public SimplePlanVisitor<RewriterResult, RewriterContext> {
    public:
    PlanNodeIdAllocator(ObOptimizerContext* ctx) : ctx_(ctx) {}
    virtual ~PlanNodeIdAllocator() {}
    int visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result) override;
    private:
    ObOptimizerContext* ctx_;
 };

 class PreprocessPlanTreeRule : public ObOptimizeRule
 {
 public:
 PreprocessPlanTreeRule();
   virtual ~PreprocessPlanTreeRule();
   int apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) override;
   bool is_enabled(const ObOptimizerContext& ctx) override
   {
     return true;
   }
   ObString get_name() const override {
    return ObString::make_string("PreprocessPlanTreeRule");
   }
 };

 } // namespace sql
 } // namespace oceanbase

 #endif // OCEANBASE_SQL_OB_PREPROCESS_PLAN_TREE_H


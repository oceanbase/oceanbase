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

 #define USING_LOG_PREFIX SQL_OPT
 #include "sql/optimizer/optimizer_plan_rewriter/ob_preprocess_plan_tree_rule.h"
 #include "sql/optimizer/ob_log_plan.h"

 namespace oceanbase
 {
 namespace sql
 {

int PlanNodeIdAllocator::visit_node(ObLogicalOperator * plannode, RewriterContext* context, RewriterResult*& result)
{
 int ret = OB_SUCCESS;
 if (OB_ISNULL(plannode)) {
   ret = OB_ERR_UNEXPECTED;
   LOG_WARN("get unexpected null", K(ret));
 } else if (OB_FALSE_IT(plannode->set_op_id(ctx_->get_next_op_id()))) {
   LOG_WARN("failed to set plan id", K(ret));
 } else if (OB_FAIL(visit_children_with(plannode, context, result))) {
   LOG_WARN("failed to rewrite children", K(ret));
 }
 return ret;
}

 PreprocessPlanTreeRule::PreprocessPlanTreeRule()
 {
 }

 PreprocessPlanTreeRule::~PreprocessPlanTreeRule()
 {
 }

 // rewrite log plan
 int PreprocessPlanTreeRule::apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) {
   // all related contexed are not null
   int ret = OB_SUCCESS;
   ObLogicalOperator *root = NULL;
   if (OB_ISNULL(root = root_plan->get_plan_root()) || OB_ISNULL(ctx.get_query_ctx())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("get unexpected null", K(ret));
   } else if (!is_enabled(ctx)) {
     OPT_TRACE("PreprocessPlanTreeRule is disabled");
   } else {
    PlanNodeIdAllocator rewriter(&ctx);
    RewriterResult *result = NULL;
    if (OB_FAIL((rewriter.visit(root, NULL, result)))) {
      LOG_WARN("failed to do plannode id allocate", K(ret));
    } else {
      OPT_TRACE("after apply preprocess plan tree:", root);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

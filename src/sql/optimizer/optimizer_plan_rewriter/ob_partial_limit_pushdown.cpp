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
 #include "sql/optimizer/optimizer_plan_rewriter/ob_partial_limit_pushdown.h"
 #include "sql/optimizer/ob_log_plan.h"
 #include "sql/optimizer/optimizer_plan_rewriter/ob_partial_limit_pushdown_rewriter.h"
 #include "lib/allocator/page_arena.h"

 namespace oceanbase
 {
 namespace sql
 {

 PartialLimitPushdown::PartialLimitPushdown()
 {
 }

 PartialLimitPushdown::~PartialLimitPushdown()
 {
 }

 // rewrite log plan
 int PartialLimitPushdown::apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) {
   // all related contexed are not null
   int ret = OB_SUCCESS;
   ObLogicalOperator *root = NULL;
   if (OB_ISNULL(root = root_plan->get_plan_root()) || OB_ISNULL(ctx.get_query_ctx())
     || OB_ISNULL(ctx.get_session_info())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("get unexpected null", K(ret));
   } else if (!is_enabled(ctx)) {
     OPT_TRACE("partial limit pushdown is disabled");
   } else {
    // todo use its own allocator
    uint64_t tenant_id = ctx.get_session_info()->get_effective_tenant_id();
    common::ObArenaAllocator arena_alloc("RuleAllocator", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    LimitPushdownRewriter rewriter(arena_alloc, ctx.get_expr_factory(), ctx.get_session_info(), &ctx);
    LimitPushdownResult *result = NULL;
    if (OB_FAIL((rewriter.visit(root, NULL, result)))) {
      LOG_WARN("failed to do partial limit pushdown rewrite", K(ret));
    } else {
      // replace root with result
      // result should be not null
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        root_plan->set_plan_root(result->rewrited_plan_);
      }
    }
    LOG_DEBUG("memory usage for ", K(get_name()), "used mem", arena_alloc.used(), "total mem", arena_alloc.total());
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

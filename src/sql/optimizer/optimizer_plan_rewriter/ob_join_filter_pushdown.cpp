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
#include "sql/optimizer/optimizer_plan_rewriter/ob_join_filter_pushdown.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/optimizer_plan_rewriter/ob_join_filter_pushdown_rewriter.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

JoinFilterPushdown::JoinFilterPushdown()
{
}

JoinFilterPushdown::~JoinFilterPushdown()
{
}
bool JoinFilterPushdown::is_enabled(const ObOptimizerContext& ctx)
{
  return ctx.enable_runtime_filter() && ctx.get_query_ctx() != NULL &&
         ctx.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_6_0);
}

int JoinFilterPushdown::apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx) {
  // all related contexed are not null
  int ret = OB_SUCCESS;
  OPT_TRACE_TITLE("begin to apply rule: runtime filter pushdown ");
  ObLogicalOperator *root = NULL;
  if (OB_ISNULL(root = root_plan->get_plan_root()) || OB_ISNULL(ctx.get_query_ctx())
     || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!ctx.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_6_0)) {
  } else if (!is_enabled(ctx)) {
    OPT_TRACE("runtime filter pushdown is disabled");
  } else {
    // check whether has join or is parallel plan
    JoinFilterPushdownChecker checker;
    RewriterResult *dummy_result1 = NULL;
    if (OB_FAIL((checker.visit(root, NULL, dummy_result1)))) {
      LOG_WARN("failed to do join filter pushdown rewrite", K(ret));
    } else if (checker.has_join_ && checker.is_parallel_plan_) {
      // do the rewrite
      // use arenaallocator with tenant id
      uint64_t tenant_id = ctx.get_session_info()->get_effective_tenant_id();
      common::ObArenaAllocator arena_alloc("RuleAllocator", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
      JoinFilterPushdownRewriter rewriter(arena_alloc, ctx.get_expr_factory(), ctx.get_session_info());
      JoinFilterPushdownResult *dummy_result2 = NULL;
      if (OB_FAIL((rewriter.visit(root, NULL, dummy_result2)))) {
        LOG_WARN("failed to do join filter pushdown rewrite", K(ret));
      } else {
        if (rewriter.valid_rtfs_.count() > 0) {
          OPT_TRACE("start to purne rtfs, current rtf count: ", rewriter.valid_rtfs_.count());
          JoinFilterPruneAndUpdateRewriter pruner(arena_alloc,
                                                  ctx.get_expr_factory(),
                                                  ctx.get_session_info(),
                                                  ctx.get_query_ctx(),
                                                  ctx.get_rtf_creator_max_row_count(),
                                                  ctx.get_rtf_user_min_partition_count());
          if (OB_FAIL(pruner.init(rewriter.valid_rtfs_, rewriter.consumer_table_infos_))) {
            LOG_WARN("failed to init join filter prune and update rewriter", K(ret));
          } else {
            RewriterResult *dummy_result3 = NULL;
            if (OB_FAIL((pruner.visit(root, NULL, dummy_result3)))) {
              LOG_WARN("failed to do join filter prune and update", K(ret));
            }
          }
        } else {
          OPT_TRACE("no valid rtfs found");
        }
      }
      LOG_DEBUG("memory usage for ", K(get_name()), "used mem", arena_alloc.used(), "total mem", arena_alloc.total());
    } else {
      OPT_TRACE("skip runtime filter pushdown, no join or not parallel plan: ",checker.has_join_, checker.is_parallel_plan_);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

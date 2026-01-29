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
#include "sql/optimizer/optimizer_plan_rewriter/ob_optimize_rule.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{

ObOptimizeRule::ObOptimizeRule()
{
}

ObOptimizeRule::~ObOptimizeRule()
{
}
int ObOptimizeRule::apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx)
{
  // Default implementation does nothing
  UNUSED(root_plan);
  UNUSED(ctx);
  return OB_SUCCESS;
}
int ObOptimizeRule::apply_rule(ObLogPlan* root_plan, ObOptimizerContext& ctx, ObRuleResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_plan) || OB_ISNULL(ctx.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ret = apply_rule(root_plan, ctx))) {
    LOG_WARN("failed to apply rule", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

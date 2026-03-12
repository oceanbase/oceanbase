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

#include "sql/optimizer/ob_log_hybrid_fusion.h"
#include "sql/hybrid_search/ob_hybrid_search_node.h"

namespace oceanbase
{
namespace sql
{

void ObLogHybridFusion::set_has_hybrid_fusion_op()
{
  fusion_node_->has_hybrid_fusion_op_ = true;
}

ObFusionMethod ObLogHybridFusion::get_fusion_algo() const
{
  return fusion_node_->method_;
}

ObRawExpr* ObLogHybridFusion::get_fusion_score_expr() const
{
  return fusion_node_->get_fusion_score_expr();
}

ObRawExpr* ObLogHybridFusion::get_rank_window_size_expr() const
{
  return fusion_node_->window_size_;
}

ObRawExpr* ObLogHybridFusion::get_rank_constant_expr() const
{
  return fusion_node_->rank_const_;
}

ObRawExpr* ObLogHybridFusion::get_size_expr() const
{
  return fusion_node_->size_;
}

ObRawExpr* ObLogHybridFusion::get_from_expr() const
{
  return fusion_node_->from_;
}

ObRawExpr* ObLogHybridFusion::get_min_score_expr() const
{
  return fusion_node_->min_score_;
}

const ObIArray<ObRawExpr*>& ObLogHybridFusion::get_weights_exprs() const
{
  return fusion_node_->weight_cols_;
}

const ObIArray<ObRawExpr*>& ObLogHybridFusion::get_score_exprs() const
{
  return fusion_node_->score_cols_;
}

const ObIArray<ObRawExpr*>& ObLogHybridFusion::path_top_k_limit_exprs() const
{
  return fusion_node_->path_top_k_limit_;
}

bool ObLogHybridFusion::has_search_subquery() const
{
  return fusion_node_->has_search_subquery_;
}

ObLogHybridFusion::ObLogHybridFusion(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    fusion_node_(nullptr) {}

int ObLogHybridFusion::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  access_exprs_.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < get_score_exprs().count(); ++i) {
    ObRawExpr *score_expr = get_score_exprs().at(i);
    if (OB_ISNULL(score_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null score expr, should have been set in extract_fusion_params_from_child", K(ret), K(i));
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, score_expr))) {
      LOG_WARN("failed to add score expr", K(ret), K(i));
    }
  }
  return ret;
}

int ObLogHybridFusion::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  // Generate access_exprs_ if not already done
  if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs from parent", K(ret));
  } else if (access_exprs_.empty() && OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append access exprs", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

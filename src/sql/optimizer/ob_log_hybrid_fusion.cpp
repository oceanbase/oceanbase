/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  // hybrid fusion op needs path_scores computed by the fusion iter,
  // so SKIP_FUSION_ITER (single-query shortcut) is not valid for partitioned tables
  if (fusion_node_->fusion_iter_exec_mode_ == ObFusionIterExecMode::SKIP_FUSION_ITER) {
    fusion_node_->fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL;
  }
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

bool ObLogHybridFusion::has_rerank() const
{
  return fusion_node_->rerank_info_.has_rerank();
}

ObRawExpr* ObLogHybridFusion::get_rerank_model_key_expr() const
{
  return fusion_node_->rerank_info_.model_;
}

ObRawExpr* ObLogHybridFusion::get_rerank_query_expr() const
{
  return fusion_node_->rerank_info_.query_;
}

ObRawExpr* ObLogHybridFusion::get_rerank_field_expr() const
{
  return static_cast<ObRawExpr*>(fusion_node_->rerank_info_.field_);
}

ObRawExpr* ObLogHybridFusion::get_rerank_window_size_expr() const
{
  return fusion_node_->rerank_info_.rank_window_size_;
}

bool ObLogHybridFusion::has_search_subquery() const
{
  return fusion_node_->has_search_subquery_;
}

int64_t ObLogHybridFusion::get_search_index() const
{
  return fusion_node_->search_index_;
}

ObLogHybridFusion::ObLogHybridFusion(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    fusion_node_(nullptr) {}

bool ObLogHybridFusion::get_is_single_partition() const
{
  return fusion_node_->get_is_single_partition();
}

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
  // AI rerank: only the text column (field) is read from child rows; model_key/query/rank_window_size are constants.
  if (OB_SUCC(ret) && has_rerank()) {
    ObRawExpr *field_expr = get_rerank_field_expr();
    if (OB_NOT_NULL(field_expr) && OB_FAIL(add_var_to_array_no_dup(access_exprs_, field_expr))) {
      LOG_WARN("failed to add ai rerank field expr", K(ret));
    }
  }
  return ret;
}

int ObLogHybridFusion::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs from parent", K(ret));
  } else if (access_exprs_.empty() && OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append access exprs", K(ret));
  } else if (has_rerank()) {
    // So that CG can construct rt_expr_ for rerank params (model_key, query, window_size)
    ObRawExpr *model_key = get_rerank_model_key_expr();
    ObRawExpr *query = get_rerank_query_expr();
    ObRawExpr *window_size = get_rerank_window_size_expr();
    if (OB_NOT_NULL(model_key) && OB_FAIL(add_var_to_array_no_dup(all_exprs, model_key))) {
      LOG_WARN("failed to add rerank model_key to op exprs", K(ret));
    } else if (OB_NOT_NULL(query) && OB_FAIL(add_var_to_array_no_dup(all_exprs, query))) {
      LOG_WARN("failed to add rerank query to op exprs", K(ret));
    } else if (OB_NOT_NULL(window_size) && OB_FAIL(add_var_to_array_no_dup(all_exprs, window_size))) {
      LOG_WARN("failed to add rerank window_size to op exprs", K(ret));
    }
  }
  return ret;
}

int ObLogHybridFusion::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  if (has_rerank()) {
    ObRawExpr *field_expr = get_rerank_field_expr();
    ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
    if (OB_NOT_NULL(field_expr) && OB_NOT_NULL(child)) {
      if (!ObOptimizerUtil::find_item(child->get_output_exprs(), field_expr)
          && OB_FAIL(child->get_output_exprs().push_back(field_expr))) {
        LOG_WARN("failed to add ai rerank field to child output", K(ret));
      }
      if (OB_SUCC(ret)) {
        uint64_t child_branch_id = branch_id_;
        for (int64_t i = 0; i < ctx.expr_producers_.count(); ++i) {
          if (ctx.expr_producers_.at(i).producer_id_ == child->get_operator_id()
              && OB_INVALID_ID != ctx.expr_producers_.at(i).producer_branch_) {
            child_branch_id = ctx.expr_producers_.at(i).producer_branch_;
            break;
          }
        }
        if (OB_FAIL(mark_expr_produced(field_expr, child_branch_id, child->get_operator_id(), ctx))) {
          LOG_WARN("failed to mark ai rerank field as produced by child", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

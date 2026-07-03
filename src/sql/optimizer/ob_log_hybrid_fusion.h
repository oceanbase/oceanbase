/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_LOG_HYBRID_FUSION_H_
#define OCEANBASE_SQL_OPTIMIZER_OB_LOG_HYBRID_FUSION_H_

#include "sql/optimizer/ob_logical_operator.h"
#include "share/hybrid_search/ob_query_parse.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
class ObFusionNode;
class ObLogHybridFusion : public ObLogicalOperator
{
public:
  ObLogHybridFusion(ObLogPlan &plan);
  virtual ~ObLogHybridFusion() {}

  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  int generate_access_exprs();
  virtual bool is_block_op() const override { return true; }
  void set_fusion_node(ObFusionNode *fusion_node) { fusion_node_ = fusion_node; }
  void set_has_hybrid_fusion_op();
  bool get_is_single_partition() const;
  bool has_search_subquery() const;
  int64_t get_search_index() const;
  ObFusionMethod get_fusion_algo() const;
  ObRawExpr* get_fusion_score_expr() const;
  ObRawExpr* get_rank_window_size_expr() const;
  ObRawExpr* get_rank_constant_expr() const;
  ObRawExpr* get_size_expr() const;
  ObRawExpr* get_from_expr() const;
  ObRawExpr* get_min_score_expr() const;
  ObFusionNode* get_fusion_node() const { return fusion_node_; }
  const ObIArray<ObRawExpr*>& get_weights_exprs() const;
  const ObIArray<ObRawExpr*>& get_score_exprs() const;
  const ObIArray<ObRawExpr*>& path_top_k_limit_exprs() const;

  // AI rerank (optional)
  bool has_rerank() const;
  ObRawExpr* get_rerank_model_key_expr() const;
  ObRawExpr* get_rerank_query_expr() const;
  ObRawExpr* get_rerank_field_expr() const;
  ObRawExpr* get_rerank_window_size_expr() const;

private:
  ObFusionNode *fusion_node_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogHybridFusion);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OPTIMIZER_OB_LOG_HYBRID_FUSION_H_
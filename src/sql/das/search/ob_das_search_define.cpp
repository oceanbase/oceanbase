/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_search_define.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/optimizer/ob_storage_estimator.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObIDASSearchCtDef, ObDASAttachCtDef), is_scoring_, is_top_level_scoring_);

OB_SERIALIZE_MEMBER((ObIDASSearchRtDef, ObDASAttachRtDef));

int ObIDASSearchRtDef::get_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  if (cost_.is_valid()) {
    cost = cost_;
  } else if (OB_FAIL(compute_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute cost", K(ret));
  } else if (!cost.is_valid()) {
    cost = search_ctx.get_row_count();
    cost_ = cost;
  } else {
    cost_ = cost;
  }
  return ret;
}

int ObIDASSearchRtDef::can_pushdown_filter_to_bmm(bool &can_pushdown)
{
  can_pushdown = false;
  return OB_SUCCESS;
}

void ObIDASSearchRtDef::set_pushdown_filter(const bool query_optional, ObIDASSearchOp *filter_op)
{
  UNUSED(filter_op);
  UNUSED(query_optional);
}

OB_SERIALIZE_MEMBER((ObDASFusionCtDef, ObDASAttachCtDef), search_index_, rowid_exprs_, score_exprs_, rank_exprs_, weight_exprs_, path_top_k_limit_exprs_, size_expr_, offset_expr_, rank_window_size_expr_, rank_constant_expr_, min_score_expr_, has_search_subquery_, has_vector_subquery_, is_top_k_query_, fusion_method_, has_hybrid_fusion_op_);

OB_SERIALIZE_MEMBER((ObDASFusionRtDef, ObDASAttachRtDef));

int ObDASFusionCtDef::init(
    int64_t search_index,
    bool has_search_subquery,
    bool has_vector_subquery,
    bool is_top_k_query,
    ObFusionMethod fusion_method,
    bool has_hybrid_fusion_op,
    ObExpr *size_expr,
    ObExpr *offset_expr,
    ObExpr *rank_window_size_expr,
    ObExpr *rank_constant_expr,
    ObExpr *min_score_expr,
    const common::ObIArray<ObExpr *> &rowid_exprs,
    const common::ObIArray<ObExpr *> &score_exprs,
    const common::ObIArray<ObExpr *> &result_output_exprs,
    const common::ObIArray<ObExpr *> &weight_exprs,
    const common::ObIArray<ObExpr *> &path_top_k_limit_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowid_exprs_.assign(rowid_exprs))) {
    LOG_WARN("failed to assign rowid_exprs", K(ret));
  } else if (OB_FAIL(score_exprs_.assign(score_exprs))) {
    LOG_WARN("failed to assign score_exprs", K(ret));
  } else if (OB_FAIL(result_output_.assign(result_output_exprs))) {
    LOG_WARN("failed to assign result_output", K(ret));
  } else if (OB_FAIL(weight_exprs_.assign(weight_exprs))) {
    LOG_WARN("failed to assign weight_exprs", K(ret));
  } else if (OB_FAIL(path_top_k_limit_exprs_.assign(path_top_k_limit_exprs))) {
    LOG_WARN("failed to assign path_top_k_limit_exprs", K(ret));
  } else {
    search_index_ = search_index;
    has_search_subquery_ = has_search_subquery;
    has_vector_subquery_ = has_vector_subquery;
    is_top_k_query_ = is_top_k_query;
    fusion_method_ = fusion_method;
    has_hybrid_fusion_op_ = has_hybrid_fusion_op;
    size_expr_ = size_expr;
    offset_expr_ = offset_expr;
    rank_window_size_expr_ = rank_window_size_expr;
    rank_constant_expr_ = rank_constant_expr;
    min_score_expr_ = min_score_expr;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

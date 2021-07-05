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

#ifndef OB_TRANSFORM_SUBQUERY_COALESCE_H
#define OB_TRANSFORM_SUBQUERY_COALESCE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_stmt_comparer.h"

namespace oceanbase {
namespace sql {

class ObTransformSubqueryCoalesce : public ObTransformRule {
public:
  ObTransformSubqueryCoalesce(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObTransformSubqueryCoalesce()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

protected:
  int adjust_transform_types(uint64_t& transform_types);

private:
  enum TransformFlag { DEFAULT, EXISTS_NOT_EXISTS, ANY_ALL };
  struct TransformParam {
    ObRawExpr* exists_expr_;
    ObRawExpr* not_exists_expr_;
    ObRawExpr* any_expr_;
    ObRawExpr* all_expr_;
    ObStmtMapInfo map_info_;
    TransformFlag trans_flag_ = DEFAULT;  // defalut value;

    TO_STRING_KV(K(exists_expr_), K(not_exists_expr_), K(any_expr_), K(all_expr_), K(map_info_), K(trans_flag_));
  };

  int transform_same_exprs(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& conds, bool& is_happened);

  int transform_diff_exprs(ObDMLStmt* stmt, ObDMLStmt*& trans_stmt,
      ObIArray<ObPCParamEqualInfo>& rule_based_equal_infos, ObIArray<ObPCParamEqualInfo>& cost_based_equal_infos,
      bool& rule_based_trans_happened);

  int coalesce_same_exists_exprs(
      ObDMLStmt* stmt, const ObItemType type, common::ObIArray<ObRawExpr*>& filters, bool& is_happened);

  int coalesce_same_any_all_exprs(
      ObDMLStmt* stmt, const ObItemType type, common::ObIArray<ObRawExpr*>& filters, bool& is_happened);

  int coalesce_diff_exists_exprs(
      ObDMLStmt* stmt, common::ObIArray<ObRawExpr*>& cond_exprs, common::ObIArray<TransformParam>& trans_params);

  int coalesce_diff_any_all_exprs(
      ObDMLStmt* stmt, common::ObIArray<ObRawExpr*>& cond_exprs, common::ObIArray<TransformParam>& trans_params);

  int merge_exists_subqueries(TransformParam& trans_param, ObRawExpr*& new_exist_expr);

  int merge_any_all_subqueries(ObQueryRefRawExpr* any_ref_expr, ObQueryRefRawExpr* all_ref_expr,
      TransformParam& trans_param, ObRawExpr*& new_any_all_query);

  int classify_conditions(ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& validity_exprs);

  int get_remove_exprs(
      ObIArray<ObRawExpr*>& ori_exprs, ObIArray<ObRawExpr*>& remain_exprs, ObIArray<ObRawExpr*>& remove_exprs);

  int check_conditions_validity(
      common::ObIArray<ObRawExpr*>& conds, common::ObIArray<TransformParam>& trans_params, bool& has_false_conds);

  int check_query_ref_validity(ObRawExpr* expr, bool& is_valid);

  int compare_any_all_subqueries(TransformParam& param, ObIArray<TransformParam>& trans_params, bool& has_false_conds,
      bool& is_used, bool can_coalesce);
  int get_same_classify_exprs(ObIArray<ObRawExpr*>& validity_exprs, ObIArray<ObRawExpr*>& same_classify_exprs,
      ObItemType ctype, ObExprInfoFlag flag);

private:
  ObQueryRefRawExpr* get_exists_query_expr(ObRawExpr* expr);

  ObQueryRefRawExpr* get_any_all_query_expr(ObRawExpr* expr);

  ObRawExpr* get_any_all_left_hand_expr(ObRawExpr* expr);

  int create_and_expr(const ObIArray<ObRawExpr*>& params, ObRawExpr*& ret_expr);

  int make_false(ObIArray<ObRawExpr*>& conds);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_TRANSFORM_SUBQUERY_COALESCE_H

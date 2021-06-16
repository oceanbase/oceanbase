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

#ifndef _OB_TRANSFORM_AGGR_SUBQUERY_H_
#define _OB_TRANSFORM_AGGR_SUBQUERY_H_ 1

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {
class ObUpdateStmt;

class ObTransformAggrSubquery : public ObTransformRule {
public:
  ObTransformAggrSubquery(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObTransformAggrSubquery()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  enum PullupFlag { USE_OUTER_JOIN = 1, ADD_CASE_WHEN_EXPR = 2, JOIN_FIRST = 4, AGGR_FIRST = 8 };

  /**
   * @brief The TransformParam struct
   * a candidate ja query for transformation
   */
  struct TransformParam {
    TransformParam() : ja_query_ref_(NULL), nested_conditions_(), pullup_flag_(0), not_null_expr_(NULL)
    {}

    TransformParam(int64_t trans_strategy)
        : ja_query_ref_(NULL), nested_conditions_(), pullup_flag_(trans_strategy), not_null_expr_(NULL)
    {}

    ObQueryRefRawExpr* ja_query_ref_;  // the ja query ref expr
    ObSEArray<ObRawExpr*, 4> query_refs_;
    ObSEArray<ObRawExpr*, 4> nested_conditions_;  // nested conditions in the ja query
    ObSEArray<bool, 4> is_null_prop_;             // need case when for a expr
    int64_t pullup_flag_;                         // the methods uesd when pulling up the ja query
    ObRawExpr* not_null_expr_;                    //  not null expr of subquery if corelated join happened

    TO_STRING_KV(
        K_(ja_query_ref), K_(query_refs), K_(nested_conditions), K_(is_null_prop), K_(pullup_flag), K_(not_null_expr));
  };

  int transform_with_aggregation_first(ObDMLStmt*& stmt, bool& trans_happened);

  int do_aggr_first_transform(ObDMLStmt*& stmt, ObRawExpr* expr, bool& trans_happened);

  int gather_transform_params(
      ObRawExpr* root_expr, ObRawExpr* child_expr, int64_t pullup_strategy, ObIArray<TransformParam>& params);

  int check_subquery_validity(
      ObSelectStmt* subquery, const bool is_vector_assign, ObIArray<ObRawExpr*>& nested_conditions, bool& is_valid);

  int choose_pullup_method(ObIArray<ObRawExpr*>& conditions, TransformParam& param);

  int fill_query_refs(ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& param);

  int transform_child_stmt(ObDMLStmt* stmt, ObSelectStmt& subquery, TransformParam& param);

  int deduce_query_values(ObDMLStmt& stmt, TransformParam& param, ObIArray<ObRawExpr*>& select_exprs,
      ObIArray<ObRawExpr*>& view_columns, ObIArray<ObRawExpr*>& real_values);

  int transform_upper_stmt(ObDMLStmt& stmt, TransformParam& param);

  int transform_from_list(
      ObDMLStmt& stmt, TableItem* view_table_item, const ObIArray<ObRawExpr*>& joined_conds, const int64_t pullup_flag);

  int extract_nullable_exprs(const ObRawExpr* expr, ObIArray<const ObRawExpr*>& vars);

  inline bool use_outer_join(int64_t flag)
  {
    return 0 != (flag & USE_OUTER_JOIN);
  }

  inline bool add_case_when_expr(int64_t flag)
  {
    return 0 != (flag & ADD_CASE_WHEN_EXPR);
  }

  inline bool join_first(int64_t flag)
  {
    return 0 != (flag & JOIN_FIRST);
  }

  inline bool aggr_first(int64_t flag)
  {
    return 0 != (flag & AGGR_FIRST);
  }

  int transform_with_join_first(ObDMLStmt*& stmt, bool& trans_happened);

  int check_stmt_valid(ObDMLStmt& stmt, bool& is_valid);

  int check_subquery_validity(
      ObSelectStmt* subquery, const bool is_vector_assign, const bool in_exists, bool& is_valid);

  int choose_pullup_method_for_exists(
      ObQueryRefRawExpr* query_ref, int64_t& pullup_flag, const bool with_not, bool& is_valid);

  int get_trans_param(ObDMLStmt& stmt, TransformParam& trans_param, ObRawExpr*& root, bool& post_group_by);

  int get_trans_view(ObDMLStmt& stmt, ObSelectStmt*& view_stmt, ObRawExpr* root_expr, bool post_group_by);

  int do_join_first_transform(ObSelectStmt& select_stmt, TransformParam& trans_param, ObRawExpr* root_expr);

  int get_unique_keys(ObDMLStmt& stmt, ObIArray<ObRawExpr*>& pkeys);

  int transform_from_list(ObDMLStmt& stmt, ObSelectStmt& subquery, const int64_t pullup_flag);

  int rebuild_conditon(ObSelectStmt& stmt);

  int check_subquery_aggr_item(const ObSelectStmt& subquery, bool& is_valid);

  int check_count_const_validity(const ObSelectStmt& subquery, bool& is_valid);

  int is_count_const_expr(const ObRawExpr* expr, bool& is_count_const);

  int is_const_null_value(const ObDMLStmt& stmt, const ObRawExpr* param_expr, bool& is_const_null);

  int replace_count_const(ObAggFunRawExpr* agg_expr, ObRawExpr* not_null_expr);

  int check_subquery_table_item(const ObSelectStmt& subquery, bool& is_valid);

  int check_can_use_outer_join(TransformParam& param, bool& is_valid);

  int check_subquery_select(ObSelectStmt& subquery, bool& is_valid);

  int check_subquery_having(const ObSelectStmt& subquery, bool& is_valid);

  int check_subquery_on_conditions(ObSelectStmt& subquery, bool& is_valid);

  int check_subquery_conditions(ObSelectStmt& subquery, ObIArray<ObRawExpr*>& nested_conds, bool& is_valid);

  int replace_columns_and_aggrs(ObRawExpr*& expr, ObTransformerCtx* ctx);

  inline bool is_exists_op(const ObItemType type)
  {
    return type == T_OP_EXISTS || type == T_OP_NOT_EXISTS;
  }

  bool check_is_filter(const ObDMLStmt& stmt, const ObRawExpr* expr);

  int get_filters(ObDMLStmt& stmt, ObRawExpr* expr, ObIArray<ObRawExpr*>& filters);

  int is_valid_group_by(const ObSelectStmt& subquery, bool& is_valid);
  int extract_no_rewrite_select_exprs(ObDMLStmt*& stmt);
  int extract_no_rewrite_expr(ObRawExpr* expr);

private:
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> no_rewrite_exprs_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_TRANSFORM_AGGREGATION_SUBQUERY_H_ */

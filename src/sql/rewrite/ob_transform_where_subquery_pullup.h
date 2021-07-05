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

#ifndef _OB_WHERE_SUBQUERY_PULLUP_H_
#define _OB_WHERE_SUBQUERY_PULLUP_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {

class ObWhereSubQueryPullup : public ObTransformRule {
public:
  ObWhereSubQueryPullup(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObWhereSubQueryPullup()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;
  common::ObIAllocator* get_allocator()
  {
    common::ObIAllocator* allocator = NULL;
    if (ctx_ != NULL) {
      allocator = ctx_->allocator_;
    } else { /*do nothing*/
    }
    return allocator;
  }

private:
  struct TransformParam {
    TransformParam()
        : op_(NULL),
          subquery_expr_(NULL),
          subquery_(NULL),
          left_hand_(NULL),
          can_unnest_pullup_(false),
          can_be_transform_(true),
          direct_correlated_(false),
          need_create_spj_(false),
          need_add_limit_constraint_(false)
    {}

    ObOpRawExpr* op_;                   // the all/any/exists contain subquery expr;
    ObQueryRefRawExpr* subquery_expr_;  // the query expr
    ObSelectStmt* subquery_;            // subquery select stmt
    ObRawExpr* left_hand_;              // the all/any op left expr
    bool can_unnest_pullup_;
    bool can_be_transform_;
    bool direct_correlated_;
    bool need_create_spj_;
    // Just in case different parameters hit same plan, firstly we need add const param constraint
    bool need_add_limit_constraint_;

    TO_STRING_KV(K_(op), K_(subquery_expr), K_(subquery), K_(left_hand), K_(can_unnest_pullup), K_(can_be_transform),
        K_(direct_correlated), K_(need_create_spj), K_(need_add_limit_constraint));
  };

  int transform_anyall_query(ObDMLStmt* stmt, bool& trans_happened);

  int transform_single_set_query(ObDMLStmt* stmt, bool& trans_happened);

  int transform_one_expr(ObDMLStmt* stmt, ObRawExpr* expr, bool& trans_happened);
  int gather_transform_params(ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& trans_param);
  int can_be_unnested(
      const ObItemType op_type, const ObSelectStmt* subquery, bool& can_be, bool& need_add_limit_constraint);

  int check_limit(
      const ObItemType op_type, const ObSelectStmt* subquery, bool& has_limit, bool& need_add_limit_constraint) const;

  int can_unnest_with_spj(ObSelectStmt& subquery, bool& can_create);

  int check_can_pullup_conds(ObSelectStmt& view, bool& has_special_expr);

  int check_correlated_expr_can_pullup(ObRawExpr* expr, int level, bool& can_pullup);

  int check_correlated_select_item_can_pullup(ObSelectStmt& subquery, bool& can_pullup);

  int check_correlated_having_expr_can_pullup(ObSelectStmt& subquery, bool has_special_expr, bool& can_pullup);

  int check_correlated_where_expr_can_pullup(ObSelectStmt& subquery, bool has_special_expr, bool& can_pullup);

  int create_spj(TransformParam& trans_param);

  int pullup_correlated_expr(ObRawExpr* expr, int level, ObIArray<ObRawExpr*>& new_select_list);

  int extract_current_level_column_exprs(ObRawExpr* expr, int level, ObIArray<ObRawExpr*>& column_exprs);

  int pullup_correlated_select_expr(ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list);

  int pullup_correlated_having_expr(ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list);

  int pullup_correlated_where_expr(ObSelectStmt& stmt, ObSelectStmt& view, ObIArray<ObRawExpr*>& new_select_list);

  int has_upper_level_column(const ObSelectStmt* subquery, bool& has_upper_column);

  int recursive_find_subquery_column(
      const ObRawExpr* expr, const int64_t level, common::ObIArray<const ObColumnRefRawExpr*>& subquery_columns);
  int create_select_items(
      ObSelectStmt* subquery, const ObColumnRefRawExpr* expr, common::ObIArray<SelectItem>& select_items);
  int find_select_item(const common::ObIArray<SelectItem>& new_select_items, const ObRawExpr* expr, int64_t& index);

  int recursive_replace_subquery_column(ObRawExpr*& condition, const TableItem& table_item,
      common::ObIArray<SelectItem>& new_select_items, ObDMLStmt* stmt, ObSelectStmt* subquery);

  int do_transform_pullup_subquery(ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& trans_param, bool& trans_happened);

  int pullup_subquery_as_view(ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr,
      ObQueryRefRawExpr* subquery_expr, const bool is_direct_correlated);

  int pull_exprs_relation_id_and_levels(ObIArray<ObRawExpr*>& exprs, const int32_t cur_stmt_level);

  int get_correlated_conditions(ObSelectStmt& subquery, ObIArray<ObRawExpr*>& correlated_conds);

  int get_single_set_subquery(const int32_t current_level, ObRawExpr* expr, ObIArray<ObQueryRefRawExpr*>& queries);

  int check_subquery_validity(ObSelectStmt* subquery, bool& is_valid);

  bool is_vector_query(ObQueryRefRawExpr* query);

  int unnest_single_set_subquery(ObDMLStmt* stmt, ObQueryRefRawExpr* query_expr, const bool use_outer_join,
      const bool is_vector_assign, const bool in_select_expr = false);

  int pull_up_semi_info(ObDMLStmt* stmt, ObSelectStmt* subquery);

  int trans_from_list(ObDMLStmt* stmt, ObSelectStmt* subquery, const bool use_outer_join);

  int pullup_non_correlated_subquery_as_view(
      ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr, ObQueryRefRawExpr* subquery_expr);

  int pullup_correlated_subquery_as_view(
      ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr, ObQueryRefRawExpr* subquery_expr);

  int check_transform_validity(ObDMLStmt* stmt, const ObRawExpr* expr, TransformParam& trans_param);

  int check_basic_validity(ObDMLStmt* stmt, const ObRawExpr* expr, TransformParam& trans_param);

  int pull_up_tables_and_columns(ObDMLStmt* stmt, ObSelectStmt* subquery);

  int pull_up_stmt_structure(ObDMLStmt* stmt, ObRawExpr* expr, ObSelectStmt* subquery);

  int generate_semi_conditions(
      ObDMLStmt* stmt, ObSelectStmt* subquery, ObRawExpr* expr, common::ObIArray<ObRawExpr*>& new_conditions);
  int generate_conditions(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& subq_exprs, ObSelectStmt* subquery, ObRawExpr* expr,
      ObIArray<ObRawExpr*>& new_conditions);

  int get_semi_oper_type(ObRawExpr* op, ObItemType& oper_type);

  int generate_anti_condition(ObDMLStmt* stmt, const ObSelectStmt* subquery, ObRawExpr* expr, ObRawExpr* left_arg,
      ObRawExpr* right_arg, ObRawExpr* generated_column_, ObRawExpr*& anti_expr);

  int make_null_test(ObDMLStmt* stmt, ObRawExpr* in_expr, ObRawExpr*& out_expr);

  int generate_semi_info(ObDMLStmt* stmt, ObRawExpr* expr, TableItem* right_table,
      ObIArray<ObRawExpr*>& semi_conditions, SemiInfo*& semi_info);
  int fill_semi_left_table_ids(ObDMLStmt* stmt, SemiInfo* info);

  int is_where_subquery_correlated(const ObSelectStmt* subquery, bool& is_correlated);

  int is_subquery_direct_correlated(const ObSelectStmt* subquery, bool& is_correlated);

  int is_join_conditions_correlated(const ObSelectStmt* subquery, bool& is_correlated);

  int check_joined_conditions_correlated(const JoinedTable* joined_table, int32_t stmt_level, bool& is_correlated);

  int check_semi_conditions_correlated(const SemiInfo* semi_info, int32_t stmt_level, bool& is_correlated);

  int is_correlated_join_expr(const ObSelectStmt* subquery, const ObRawExpr* expr, bool& is_correlated);

  int is_select_item_contain_subquery(const ObSelectStmt* subquery, bool& contain);

  /**
   * Simplify subuqery in exists, any, all (subq)
   * 1. Eliminate subquery if possible
   * 2. Eliminate select list to const 1
   * 3. Eliminate group by
   */
  int recursive_eliminate_subquery(
      ObDMLStmt* stmt, ObRawExpr* expr, bool& can_be_eliminated, bool& need_add_limit_constraint, bool& trans_happened);
  int eliminate_subquery(
      ObDMLStmt* stmt, ObRawExpr* expr, bool& can_be_eliminated, bool& need_add_limit_constraint, bool& trans_happened);

  int eliminate_subquey_limit_clause(ObSelectStmt* subquery, ObQueryCtx* query_ctx, bool& trans_happened);

  int add_const_param_constraints(ObIArray<ObRawExpr*>& params, ObQueryCtx* query_ctx, ObPhysicalPlanCtx* plan_ctx);

  int subquery_can_be_eliminated_in_exists(const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated,
      bool& need_add_limit_constraint) const;
  int select_list_can_be_eliminated(const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated,
      bool& need_add_limit_constraint) const;

  int groupby_can_be_eliminated_in_exists(const ObItemType op_type, const ObSelectStmt* stmt, bool& can_be_eliminated,
      bool& need_add_limit_constraint) const;

  int groupby_can_be_eliminated_in_any_all(const ObSelectStmt* stmt, bool& can_be_eliminated) const;

  int eliminate_subquery_in_exists(
      ObDMLStmt* stmt, ObRawExpr*& expr, bool need_add_limit_constraint, bool& trans_happened);

  int refine_subquery_aggrs_in_exists(ObSelectStmt* subquery, bool& trans_happened);
  int eliminate_select_list_in_exists(
      ObDMLStmt* stmt, const ObItemType op_type, ObSelectStmt* subquery, bool& trans_happened);
  int eliminate_groupby_in_exists(
      ObDMLStmt* stmt, const ObItemType op_type, ObSelectStmt*& subquery, bool& trans_happened);
  int eliminate_groupby_in_any_all(ObSelectStmt*& stmt, bool& trans_happened);
  int eliminate_distinct_in_any_all(ObSelectStmt* subquery, bool& trans_happened);
  int check_need_add_limit(ObSelectStmt* subquery, bool& need_add_limit);
  int check_if_subquery_contains_correlated_table_item(const ObSelectStmt& stmt, bool& contains);
  int add_limit_for_exists_subquery(ObDMLStmt* stmt, bool& trans_happened);
  int recursive_add_limit_for_exists_expr(ObRawExpr* expr, bool& trans_happened);

private:
  DISALLOW_COPY_AND_ASSIGN(ObWhereSubQueryPullup);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_WHERE_SUBQUERY_PULLUP_H_ */

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

#ifndef OB_TRANSFORM_SIMPILFY_SUBQUERY_H
#define OB_TRANSFORM_SIMPILFY_SUBQUERY_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifySubquery : public ObTransformRule
{
public:
  ObTransformSimplifySubquery(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_SIMPLIFY_SUBQUERY)
  {}

  virtual ~ObTransformSimplifySubquery() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int transform_subquery_as_expr(ObDMLStmt *stmt, bool &trans_happened);
  int try_trans_subquery_in_expr(ObDMLStmt *stmt,
                                 ObRawExpr *&expr,
                                 bool &trans_happened);
  int do_trans_subquery_as_expr(ObDMLStmt *stmt,
                                ObRawExpr *&expr,
                                bool &trans_happened);
  int is_subquery_to_expr_valid(const ObSelectStmt *stmt,
                                bool &is_valid);
  int transform_not_expr(ObDMLStmt *stmt, bool &trans_happened);
  int do_transform_not_expr(ObRawExpr *&expr, bool &trans_happened);
  int remove_redundant_select(ObDMLStmt *&stmt, bool &trans_happened);
  int try_remove_redundant_select(ObSelectStmt &stmt, ObSelectStmt *&new_stmt);
  int check_subquery_valid(ObSelectStmt &stmt, bool &is_valid);
  int push_down_outer_join_condition(ObDMLStmt *stmt, bool &trans_happened);
  int push_down_outer_join_condition(ObDMLStmt *stmt,
                                     TableItem *join_table,
                                     bool &trans_happened);
  int try_push_down_outer_join_conds(ObDMLStmt *stmt,
                                     JoinedTable *join_table,
                                     bool &trans_happened);

  int add_limit_for_exists_subquery(ObDMLStmt *stmt,bool &trans_happened);

  int recursive_add_limit_for_exists_expr(ObRawExpr *expr, bool &trans_happened);

  /**
   * @brief transform_any_all
   * 遍历stmt中不同部分中的表达式，并尝试对其改写
   */
  int transform_any_all(ObDMLStmt *stmt, bool &trans_happened);

  /**
   * @brief try_trans_any_all
   * 尝试对表达式 expr 本身或者它的参数表达式进行改写
   */
  int try_transform_any_all(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened);

  /**
   * @brief do_trans_any_all
   * 判断一个表达式是否可以进行改写，如果可以那么进行改写
   */
  int do_transform_any_all(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened);

  int check_any_all_as_min_max(ObRawExpr *expr, bool &is_valid);


  int transform_any_all_as_min_max(ObDMLStmt *stmt, ObRawExpr *expr, bool &trans_happened);

  int do_transform_any_all_as_min_max(ObSelectStmt *sel_stmt, const ObItemType aggr_type,
                                      bool is_with_all, bool &trans_happened);

  /**
   * @brief ObTransformAnyAll::eliminate_any_all_for_scalar_query
   * 如果 subquery 返回的记录数量为0或者1，那么 any 可以消除
   * 如果 subquery 返回记录数量为1，那么 all 可以消除
   *   rel.c > all 空集  与 rel.c > 空集 语义不相同
   */
  int eliminate_any_all_before_subquery(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened);

  /**
   * @brief is_any_all_removeable
   * 判断一个subquery compare 表达式的 any/all flag是否可以移除
   */
  int check_any_all_removeable(ObRawExpr *expr, bool &can_be_removed);

  /**
   * @brief clear_any_all_flag
   * 将一个 subquery compare 表达式 expr 替换为相应的 common compare 表达式
   */
  int clear_any_all_flag(ObDMLStmt *stmt, ObRawExpr *&expr, ObQueryRefRawExpr *query_ref);

  ObItemType get_aggr_type(ObItemType op_type, bool is_with_all);

  ObItemType query_cmp_to_value_cmp(const ObItemType cmp_type);

  int transform_exists_query(ObDMLStmt *stmt, bool &trans_happened);

  int try_eliminate_subquery(ObDMLStmt *stmt, ObRawExpr *&expr, bool &trans_happened);
  
    /**
   * Simplify subuqery in exists, any, all (subq)
   * 1. Eliminate subquery if possible
   * 2. Eliminate select list to const 1
   * 3. Eliminate group by
   */
  int recursive_eliminate_subquery(ObDMLStmt *stmt, ObRawExpr *&expr,
                                   bool &trans_happened);
  int eliminate_subquery(ObDMLStmt *stmt,
                         ObRawExpr *&expr,
                         bool &trans_happened);


  /**
   * 当[not] exists(subq)满足以下所有条件时，subquery可以被消除
   * 1. 当前stmt不是set stmt
   * 2. 当前stmt不包含having或group by
   * 3. stmt包含聚集函数
   */
  static bool is_subquery_not_empty(const ObSelectStmt &stmt);

  int subquery_can_be_eliminated_in_exists(const ObItemType op_type,
                                           const ObSelectStmt *stmt,
                                           bool &can_be_eliminated) const;
  /**
   * 当[not] exists(subq)满足以下所有条件时，select list可替换为1
   * 1. 当前stmt不是set stmt
   * 2. distinct和limit不同时存在
   */
  int select_items_can_be_simplified(const ObItemType op_type,
                                    const ObSelectStmt *stmt,
                                    bool &can_be_simplified) const;

  /**
   * 当[not] exists(subq)满足以下所有条件时，可消除group by子句:
   * 1. 当前stmt不是set stmt
   * 2. 没有having子句
   * 3. 没有limit子句
   */
  int groupby_can_be_eliminated_in_exists(const ObItemType op_type,
                                          const ObSelectStmt *stmt,
                                          bool &can_be_eliminated) const;

  /**
   * 当Any/all/in(subq)满足以下所有条件时，可消除group by子句:
   * 1. 当前stmt不是set stmt
   * 2. 没有having子句
   * 3. 没有limit子句
   * 4. 无聚集函数（select item中）
   * 5. 非常量select item列，全部包含在group exprs中
   */
  int groupby_can_be_eliminated_in_any_all(const ObSelectStmt *stmt, bool &can_be_eliminated) const;

  int eliminate_subquery_in_exists(ObDMLStmt *stmt,
                                   ObRawExpr *&expr,
                                   bool &trans_happened);

  int simplify_select_items(ObDMLStmt *stmt,
                            const ObItemType op_type,
                            ObSelectStmt *subquery,
                            bool parent_is_set_query,
                            bool &trans_happened);
  int eliminate_groupby_in_exists(ObDMLStmt *stmt,
                                  const ObItemType op_type,
                                  ObSelectStmt *&subquery,
                                  bool &trans_happened);
  int eliminate_groupby_distinct_in_any_all(ObRawExpr *expr, bool &trans_happened);
  int eliminate_groupby_in_any_all(ObSelectStmt *&stmt, bool &trans_happened);
  int eliminate_distinct_in_any_all(ObSelectStmt *subquery,bool &trans_happened);
  int check_need_add_limit(ObSelectStmt *subquery, bool &need_add_limit);
  int check_limit(const ObItemType op_type,
                  const ObSelectStmt *subquery,
                  bool &has_limit) const;
  int need_add_limit_constraint(const ObItemType op_type,
                const ObSelectStmt *subquery,
                bool &add_limit_constraint) const;
  int check_const_select(const ObSelectStmt &stmt, bool &is_const_select) const;
  int get_push_down_conditions(ObDMLStmt *stmt,
                               JoinedTable *join_table,
                               ObIArray<ObRawExpr *> &join_conds,
                               ObIArray<ObRawExpr *> &push_down_conds);
  int try_trans_any_all_as_exists(ObDMLStmt *stmt,
                                  ObRawExpr *&expr,
                                  ObNotNullContext *not_null_ctx,
                                  bool is_bool_expr,
                                  bool &trans_happened);
  int add_limit_for_any_all_subquery(ObRawExpr *stmt,bool &trans_happened);
  int transform_any_all_as_exists(ObDMLStmt *stmt, bool &trans_happened);
  int transform_any_all_as_exists_joined_table(ObDMLStmt* stmt,
                                               TableItem *table,
                                               bool &trans_happened);
  int try_trans_any_all_as_exists(ObDMLStmt *stmt,
                                  ObIArray<ObRawExpr* > &exprs,
                                  ObNotNullContext *not_null_cxt,
                                  bool is_bool_expr,
                                  bool &trans_happened);
  int empty_table_subquery_can_be_eliminated_in_exists(ObRawExpr *expr,
                                                       bool &is_valid);
  int do_trans_empty_table_subquery_as_expr(ObRawExpr *&expr,
                                            bool &trans_happened);
};

}
}
#endif // OB_TRANSFORM_SIMPILFY_SUBQUERY_H

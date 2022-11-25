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

#ifndef OB_TRANSFORM_WIN_MAGIC_H
#define OB_TRANSFORM_WIN_MAGIC_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
namespace sql
{
struct ObStmtMapInfo;
class ObTransformWinMagic : public ObTransformRule
{
public:
  ObTransformWinMagic(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_WIN_MAGIC)
  {}

  virtual ~ObTransformWinMagic() {}
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
protected:
  virtual int adjust_transform_types(uint64_t &transform_types) override;
private:

  int check_hint_valid(ObDMLStmt &stmt, TableItem &subquery, bool &is_valid);
  int check_aggr_expr_validity(ObSelectStmt &subquery,
                               bool &is_valid);

  int check_lossess_join(ObSelectStmt &subquery,
                         ObIArray<FromItem> &from_items,
                         ObIArray<ObRawExpr *> &conditions,
                         bool &is_valid);

  int create_window_function(ObAggFunRawExpr *agg_expr,
                             ObIArray<ObRawExpr *> &partition_exprs,
                             ObWinFunRawExpr *&win_expr);

  int do_transform(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                   ObDMLStmt *&stmt,
                   int64_t drill_down_idx,
                   int64_t roll_up_idx,
                   ObStmtMapInfo &map_info,
                   ObStmtCompareContext &context,
                   bool &trans_happened,
                   ObIArray<TableItem *> &trans_tables);

  int do_transform_from_type(ObDMLStmt *&stmt, 
                            const int64_t drill_down_idx,
                            const int64_t roll_up_idx, 
                            ObStmtCompareContext &context,
                            ObStmtMapInfo &map_info);

  int check_select_expr_validity(ObSelectStmt &subquery, 
                                 bool &is_valid);

  int check_view_table_basic(ObSelectStmt *stmt, 
                             bool &is_valid);

  int get_view_to_trans(ObDMLStmt *&stmt, 
                        int64_t &drill_down_idx,
                        int64_t &roll_up_idx, 
                        ObStmtCompareContext &context,
                        ObStmtMapInfo &map_info,
                        ObIArray<TableItem *> &trans_tables);

  int sanity_check_and_init(ObDMLStmt *stmt, 
                            ObDMLStmt *view, 
                            ObStmtMapInfo &map_info, 
                            ObStmtCompareContext &context);

  int check_stmt_and_view(ObDMLStmt *main_stmt, 
                          TableItem *rewrite_table, 
                          ObStmtMapInfo &map_info,
                          bool &is_valid,
                          ObIArray<TableItem *> &trans_tables);

  int check_view_and_view(ObDMLStmt *main_stmt,
                          TableItem *drill_down_table, 
                          TableItem *roll_up_table, 
                          ObStmtMapInfo &map_info, 
                          bool &is_valid,
                          ObIArray<TableItem *> &trans_tables);
  
  int check_view_valid_to_trans(ObSelectStmt *view, 
                                ObStmtMapInfo &map_info, 
                                bool &is_valid);

  int check_outer_stmt_conditions(ObDMLStmt *stmt, 
                                  ObIArray<ObRawExpr *> &roll_group_exprs,
                                  ObIArray<ObRawExpr *> &drill_group_exprs, 
                                  ObIArray<int64_t> &map,
                                  bool &is_valid);

  int create_aggr_expr(ObItemType type,
                       ObAggFunRawExpr *&agg_expr,
                       ObRawExpr *child_expr);

  int adjust_agg_to_win(ObSelectStmt *view_stmt);

  int adjust_view_for_trans(ObDMLStmt *main_stmt, 
                            TableItem *&drill_down_table, 
                            TableItem *roll_up_table,
                            TableItem *&adjust_view_for_trans,
                            ObIArray<ObRawExpr *> &new_transed_output,
                            ObStmtCompareContext &context,
                            ObStmtMapInfo &map_info);

  int adjust_win_after_group_by(ObDMLStmt *main_stmt,
                      TableItem *view_table, 
                      TableItem *matched_table,
                      TableItem *transed_view_table,
                      ObIArray<ObRawExpr *> &new_transed_output,
                      ObStmtCompareContext &context,
                      ObStmtMapInfo &map_info);

  int remove_dup_condition(ObDMLStmt *stmt);

  int adjust_column_and_table(ObDMLStmt *main_stmt,
                              TableItem *view_table,
                              ObStmtMapInfo &map_info);

  int change_agg_to_win_func(ObDMLStmt *main_stmt, 
                             ObSelectStmt *matched_stmt, 
                             ObSelectStmt *view_stmt, 
                             TableItem *transed_view_table, 
                             ObStmtMapInfo &map_info, 
                             ObStmtCompareContext &context, 
                             ObIArray<ObRawExpr *> &old_agg,
                             ObIArray<ObRawExpr *> &new_win);

  int simple_check_group_by(ObIArray<ObRawExpr *> &sel_exprs,
                            ObIArray<ObRawExpr *> &group_exprs, 
                            bool &is_valid);

  int check_expr_in_group(ObRawExpr *expr, ObIArray<ObRawExpr *> &group_exprs, bool &is_valid);

  int try_to_push_down_join(ObDMLStmt *&main_stmt);

  int push_down_join(ObDMLStmt *main_stmt, 
                     TableItem *view_table, 
                     TableItem *push_down_table, 
                     ObIArray<ObRawExpr *> &cond_to_push_down);

  int check_join_push_down(ObDMLStmt *main_stmt, 
                           TableItem *view_table, 
                           TableItem *push_down_table, 
                           ObIArray<ObRawExpr *> &cond_to_push_down, 
                           bool &is_valid);

  int get_reverse_map(ObIArray<int64_t> &map, ObIArray<int64_t> &reverse_map, int64_t size);

  int check_mode_and_agg_type(ObSelectStmt *stmt, bool &is_valid);

  int construct_trans_table(const ObDMLStmt *stmt,
                            const TableItem *table,
                            ObIArray<ObSEArray<TableItem *, 4>> &trans_basic_tables);

  int construct_trans_tables(const ObDMLStmt *stmt,
                             const ObDMLStmt *trans_stmt,
                             ObIArray<ObSEArray<TableItem *, 4>> &trans_basic_tables,
                             ObIArray<TableItem *> &trans_tables);
};

}
}

#endif // OB_TRANSFORM_WIN_MAGIC_H

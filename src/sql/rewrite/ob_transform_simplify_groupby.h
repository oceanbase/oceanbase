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

#ifndef OB_TRANSFORM_SIMPLIFY_GROUPBY_H
#define OB_TRANSFORM_SIMPLIFY_GROUPBY_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql
{

class ObTransformSimplifyGroupby : public ObTransformRule
{
public:
  ObTransformSimplifyGroupby(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER,
                      T_SIMPLIFY_GROUP_BY)
  {}

  virtual ~ObTransformSimplifyGroupby() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int remove_redundant_group_by(ObDMLStmt *stmt, bool &trans_happened);
  int check_upper_stmt_validity(ObSelectStmt *upper_stmt, bool &is_valid);
  int get_valid_child_stmts(ObSelectStmt *upper_stmt,
                            ObSelectStmt *stmt,
                            ObArray<ObSelectStmt*> &valid_child_stmts);
  int remove_child_stmts_group_by(ObArray<ObSelectStmt*> &child_stmts);
  int get_upper_column_exprs(ObSelectStmt &upper_stmt,
                             ObSelectStmt &stmt,
                             ObIArray<ObRawExpr*> &aggr_column_exprs,
                             ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                             ObIArray<ObRawExpr*> &child_aggr_exprs,
                             bool &is_valid);
  int check_upper_group_by(ObSelectStmt &upper_stmt,
                           ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                           bool &is_valid);
  int check_aggrs_matched(ObIArray<ObAggFunRawExpr*> &upper_aggrs,
                          ObIArray<ObRawExpr*> &aggr_column_exprs,
                          ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                          ObIArray<ObRawExpr*> &child_aggr_exprs,
                          bool &is_valid);
  int check_upper_condition(ObIArray<ObRawExpr*> &cond_exprs,
                            ObIArray<ObRawExpr*> &aggr_column_exprs,
                            bool &is_valid);
  int exist_exprs_in_expr(const ObRawExpr *src_expr,
                          ObIArray<ObRawExpr*> &dst_exprs,
                          bool &is_exist);
  int remove_stmt_group_by(ObDMLStmt *&stmt, bool &trans_happened);
  int check_stmt_group_by_can_be_removed(ObSelectStmt *select_stmt, bool &can_be);
  int inner_remove_stmt_group_by(ObSelectStmt *select_stmt, bool &trans_happened);
  int remove_group_by_duplicates(ObDMLStmt *&stmt, bool &trans_happened);
  int remove_aggr_distinct(ObDMLStmt *stmt, bool &trans_happened);
  int remove_aggr_duplicates(ObSelectStmt *select_stmt);
  int remove_win_func_duplicates(ObSelectStmt *select_stmt);
  int convert_count_aggr_contain_const(ObDMLStmt *stmt,
                                       bool &trans_happened);

  int convert_valid_count_aggr(ObSelectStmt *select_stmt,
                               ObIArray<ObAggFunRawExpr*> &count_null,
                               ObIArray<ObAggFunRawExpr*> &count_const);
  int get_valid_count_aggr(ObSelectStmt *select_stmt,
                           ObIArray<ObAggFunRawExpr*> &count_null,
                           ObIArray<ObAggFunRawExpr*> &count_const);
  int check_aggr_win_can_be_removed(const ObDMLStmt *stmt,
                                    ObRawExpr *expr,
                                    bool &can_remove);
  int transform_aggr_win_to_common_expr(ObSelectStmt *select_stmt,
                                        ObRawExpr *expr,
                                        ObRawExpr *&new_expr);
  int get_param_value(const ObDMLStmt *stmt,
                      ObRawExpr *param,
                      bool &is_valid,
                      int64_t &value);
  int transform_const_aggr(ObDMLStmt *stmt, bool &trans_happened);
  int is_valid_const_aggregate(ObSelectStmt *stmt, bool &is_valid);
  int is_const_aggr(ObSelectStmt *stmt, ObRawExpr *expr, bool &is_const);
  int prune_group_by_rollup(ObIArray<ObParentDMLStmt> &parent_stmts,
                            ObDMLStmt *stmt,
                            bool &trans_happened);
  int check_can_prune_rollup(ObIArray<ObParentDMLStmt> &parent_stmts,
                             ObSelectStmt *stmt,
                             int64_t &pruned_expr_idx);
  int check_rollup_pruned_by_self(ObSelectStmt *stmt,
                                  int64_t &pruned_expr_idx);
  int check_rollup_pruned_by_parent(ObIArray<ObParentDMLStmt> &parent_stmts,
                                    ObSelectStmt *stmt,
                                    int64_t &pruned_expr_idx);
  int find_null_propagate_select_exprs(ObSelectStmt *stmt,
                                       const ObRawExpr *expr,
                                       ObIArray<ObRawExpr *> &select_exprs);
  int is_first_rollup_with_duplicates(ObSelectStmt *stmt,
                                      const int64_t rollup_expr_idx,
                                      bool &is_first);
  int do_prune_rollup(ObSelectStmt *stmt, const int64_t pruned_expr_idx);
  int get_valid_having_exprs_contain_aggr(const ObIArray<ObRawExpr *> &having_exprs,
                                          ObIArray<ObRawExpr *> &vaild_having_exprs);
  int convert_group_by_to_distinct(ObDMLStmt *stmt, bool &trans_happened);
  int check_can_convert_to_distinct(ObSelectStmt *stmt, bool &can_convert);
};

}
}
#endif // OB_TRANSFORM_SIMPLIFY_GROUPBY_H

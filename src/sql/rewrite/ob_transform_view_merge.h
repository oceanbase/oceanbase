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

#ifndef _OB_TRANSFORM_VIEW_MERGE_H
#define _OB_TRANSFORM_VIEW_MERGE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"


namespace oceanbase
{

namespace common
{
class ObIAllocator;
}

namespace sql
{

class ObDelUpdStmt;

class ObTransformViewMerge : public ObTransformRule
{
public:
  ObTransformViewMerge(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_MERGE_HINT)
  {}
  virtual ~ObTransformViewMerge() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
private:
  struct ViewMergeHelper {
    ViewMergeHelper():
      parent_table(NULL),
      trans_table(NULL),
      not_null_column(NULL),
      need_check_null_propagate(false),
      can_push_where(true)
      {}
    virtual ~ViewMergeHelper(){}

    JoinedTable *parent_table;
    TableItem *trans_table;
    ObRawExpr *not_null_column;
    bool need_check_null_propagate;
    bool can_push_where;
  };
  int check_can_be_merged(ObDMLStmt *parent_stmt,
                          ObSelectStmt* child_stmt,
                          ViewMergeHelper &helper,
                          bool need_check_subquery,
                          bool is_left_join_right_table,
                          bool &can_be);

  int check_left_join_right_view_need_merge(ObDMLStmt *parent_stmt,
                                            ObSelectStmt* child_stmt,
                                            TableItem *view_table,
                                            JoinedTable *joined_table,
                                            bool &need_merge);

  int check_semi_right_table_can_be_merged(ObDMLStmt *stmt,
                                           ObSelectStmt *ref_query,
                                           bool &can_be);

  int check_basic_validity(ObDMLStmt *parent_stmt,
                           ObSelectStmt *child_stmt,
                           bool &can_be);

  int check_contain_inner_table(const ObSelectStmt &stmt,
                                bool &contain);

  int find_not_null_column(ObDMLStmt &parent_stmt,
                          ObSelectStmt &child_stmt,
                          ViewMergeHelper &helper,
                          ObIArray<ObRawExpr *> &column_exprs,
                          bool &can_be);

  int find_not_null_column_with_condition(ObDMLStmt &parent_stmt,
                                          ObSelectStmt &child_stmt,
                                          ViewMergeHelper &helper,
                                          ObIArray<ObRawExpr *> &column_exprs);

  int find_null_propagate_column(ObRawExpr *condition,
                                ObIArray<ObRawExpr*> &columns,
                                ObRawExpr *&null_propagate_column,
                                bool &is_valid);

  int do_view_merge(ObDMLStmt *parent_stmt,
                                   ObSelectStmt *child_stmt,
                                   TableItem *table_item,
                                   ViewMergeHelper &helper);

  int do_view_merge_for_semi_right_table(ObDMLStmt *parent_stmt,
                                         ObSelectStmt *child_stmt,
                                         SemiInfo *semi_info);

  int replace_stmt_exprs(ObDMLStmt *parent_stmt,
                         ObSelectStmt *child_stmt,
                         TableItem *table,
                         ViewMergeHelper &helper,
                         bool need_wrap_case_when);

  int wrap_case_when_if_necessary(ObSelectStmt &child_stmt,
                                  ViewMergeHelper &helper,
                                  ObIArray<ObRawExpr *> &exprs);

  /**
   * @brief wrap_case_when
   * 如果当前视图是left outer join的右表或者right outer join的左表
   * 需要对null rejuect的new column expr包裹一层case when
   * 需要寻找视图的非空列，如果null_reject_columns不为空，
   * 直接拿第一个使用，否则需要在stmt中查找非空列，
   * 也可以是试图内基表的pk，但不能是outer join的补null侧
   */
  int wrap_case_when(ObSelectStmt &child_stmt,
                    ObRawExpr *not_null_column,
                    ObRawExpr *&expr);
  int adjust_stmt_semi_infos(ObDMLStmt *parent_stmt,
                             ObSelectStmt *child_stmt,
                             uint64_t table_id);

  int adjust_updatable_view(ObDMLStmt *parent_stmt, TableItem *table_item);

  int transform_joined_table(ObDMLStmt *stmt,
                             JoinedTable *parent_table,
                             bool can_push_where,
                             ObIArray<ObSelectStmt*> &merged_stmts,
                             bool &trans_happened);
  int create_joined_table_for_view(ObSelectStmt *child_stmt,
                                   TableItem *&new_table);
  int transform_generated_table(ObDMLStmt *parent_stmt,
                                JoinedTable *parent_table,
                                TableItem *table_item,
                                bool need_check_where_condi,
                                bool can_push_where,
                                ObIArray<ObSelectStmt*> &merged_stmts,
                                bool &trans_happened);
  int transform_generated_table(ObDMLStmt *parent_stmt,
                                TableItem *table_item,
                                ObIArray<ObSelectStmt*> &merged_stmts,
                                bool &trans_happened);
  int transform_in_from_item(ObDMLStmt *stmt,
                             ObIArray<ObSelectStmt*> &merged_stmts,
                             bool &trans_happened);
  int transform_in_semi_info(ObDMLStmt *stmt, 
                             ObIArray<ObSelectStmt*> &merged_stmts,
                             bool &trans_happened);
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
  int check_hint_allowed_merge(ObDMLStmt &stmt, 
                               ObSelectStmt &ref_query, 
                               bool &force_merge,
                               bool &force_no_merge);
  DISALLOW_COPY_AND_ASSIGN(ObTransformViewMerge);
};

} //namespace sql
} //namespace oceanbase

#endif /* _OB_TRANSFORM_VIEW_MERGE_H */

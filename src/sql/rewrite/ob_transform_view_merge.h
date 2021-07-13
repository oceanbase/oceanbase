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

namespace oceanbase {

namespace common {
class ObIAllocator;
}

namespace sql {

class ObDelUpdStmt;

class ObTransformViewMerge : public ObTransformRule {
public:
  ObTransformViewMerge(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER), for_post_process_(false)
  {}
  virtual ~ObTransformViewMerge()
  {}

  void set_for_post_process(bool for_post_process)
  {
    for_post_process_ = for_post_process;
  }

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  struct ViewMergeHelper {
    ViewMergeHelper()
        : parent_table(NULL),
          trans_table(NULL),
          not_null_column(NULL),
          need_check_null_propagate(false),
          can_push_where(true)
    {}
    virtual ~ViewMergeHelper()
    {}

    JoinedTable* parent_table;
    TableItem* trans_table;
    ObRawExpr* not_null_column;
    bool need_check_null_propagate;
    bool can_push_where;
  };
  int check_can_be_unnested(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, ViewMergeHelper& helper,
      bool need_check_subquery, bool& can_be);

  int check_semi_right_table_can_be_merged(ObDMLStmt* stmt, SemiInfo* semi_info, bool& can_be);

  int check_basic_validity(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, bool& can_be);

  int find_not_null_column(ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt, ViewMergeHelper& helper,
      ObIArray<ObRawExpr*>& column_exprs, bool& can_be);

  int find_not_null_column_with_condition(
      ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt, ViewMergeHelper& helper, ObIArray<ObRawExpr*>& column_exprs);

  int find_null_propagate_column(
      ObRawExpr* condition, ObIArray<ObRawExpr*>& columns, ObRawExpr*& null_propagate_column, bool& is_valid);

  int do_view_merge_transformation(
      ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, TableItem* table_item, ViewMergeHelper& helper);

  int do_view_merge_for_semi_right_table(ObDMLStmt* parent_stmt, SemiInfo* semi_info);

  int replace_stmt_exprs(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, uint64_t table_id, ViewMergeHelper& helper,
      bool need_wrap_case_when);

  int wrap_case_when_if_necessary(ObSelectStmt& child_stmt, ViewMergeHelper& helper, ObIArray<ObRawExpr*>& exprs);

  int wrap_case_when(ObSelectStmt& child_stmt, ObRawExpr* not_null_column, ObRawExpr*& expr);

  int adjust_stmt_hints(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, TableItem* table_item);
  int adjust_stmt_semi_infos(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, uint64_t table_id);

  int adjust_updatable_view(ObDMLStmt* parent_stmt, TableItem* table_item);

  int transform_joined_table(ObDMLStmt* stmt, JoinedTable* parent_table, bool can_push_where, bool& trans_happened);
  int create_joined_table_for_view(ObSelectStmt* child_stmt, TableItem*& new_table);
  int transform_generated_table(ObDMLStmt* parent_stmt, JoinedTable* parent_table, TableItem* table_item,
      bool need_check_where_condi, bool can_push_where, bool& trans_happened);
  int transform_generated_table(ObDMLStmt* parent_stmt, TableItem* table_item, bool& trans_happened);
  int transform_in_from_item(ObDMLStmt* stmt, bool& trans_happened);
  int transform_in_semi_info(ObDMLStmt* stmt, bool& trans_happened);
  bool for_post_process_;
  DISALLOW_COPY_AND_ASSIGN(ObTransformViewMerge);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_TRANSFORM_VIEW_MERGE_H */

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

#ifndef _OB_TRANSFORM_QUERY_PUSH_DOWN_H
#define _OB_TRANSFORM_QUERY_PUSH_DOWN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"


namespace oceanbase
{

namespace common
{
class ObIAllocator;
}

namespace sql
{

class ObRawExpr;

class ObTransformQueryPushDown : public ObTransformRule
{
public:
  explicit ObTransformQueryPushDown(ObTransformerCtx *ctx)
        : ObTransformRule(ctx, TransMethod::POST_ORDER, T_MERGE_HINT)
  {
  }
  virtual ~ObTransformQueryPushDown()
  {
  }

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
private:
  int check_transform_validity(ObSelectStmt *select_stmt,
                               ObSelectStmt *view_stmt,
                               bool &can_transform,
                               bool &need_distinct,
                               bool &transform_having,
                               common::ObIArray<int64_t> &select_offset,
                               common::ObIArray<SelectItem> &const_select_items);
  int check_rownum_push_down(ObSelectStmt *select_stmt,
                             ObSelectStmt *view_stmt,
                             bool &can_be);
  int check_where_condition_push_down(ObSelectStmt *select_stmt,
                                      ObSelectStmt *view_stmt,
                                      bool &transform_having,
                                      bool &can_be);
  int check_window_function_push_down(ObSelectStmt *view_stmt,
                                      bool &can_be);
  int check_distinct_push_down(ObSelectStmt *view_stmt,
                               bool &need_distinct,
                               bool &can_be);
  int is_select_item_same(ObSelectStmt *select_stmt,
                          ObSelectStmt *view_stmt,
                          bool &is_same,
                          common::ObIArray<int64_t> &select_offset,
                          common::ObIArray<SelectItem> &const_select_items);
  int check_set_op_expr_reference(ObSelectStmt *select_stmt,
                                  ObSelectStmt *view_stmt,
                                  common::ObIArray<int64_t> &select_offset,
                                  bool &is_contain);
  int do_transform(ObSelectStmt *select_stmt,
                   ObSelectStmt *view_stmt,
                   bool need_distinct,
                   bool transform_having,
                   const TableItem *view_table_item,
                   common::ObIArray<int64_t> &select_offset,
                   common::ObIArray<SelectItem> &const_select_items);
  int push_down_stmt_exprs(ObSelectStmt *select_stmt,
                           ObSelectStmt *view_stmt,
                           bool need_distinct,
                           bool transform_having,
                           common::ObIArray<int64_t> &select_offset,
                           common::ObIArray<SelectItem> &const_select_items);
  bool can_limit_merge(ObSelectStmt &upper_stmt, ObSelectStmt &view_stmt);
  int do_limit_merge(ObSelectStmt &upper_stmt, ObSelectStmt &view_stmt);
  int check_select_item_push_down(ObSelectStmt *select_stmt,
                                  ObSelectStmt *view_stmt,
                                  common::ObIArray<int64_t> &select_offset,
                                  common::ObIArray<SelectItem> &const_select_items,
                                  bool &can_be);

  int check_select_item_subquery(ObSelectStmt &select_stmt,
                                 ObSelectStmt &view,
                                 bool &can_be);

  int replace_stmt_exprs(ObDMLStmt *parent_stmt,
                         ObSelectStmt *child_stmt,
                         uint64_t table_id);
  int recursive_adjust_select_item(ObSelectStmt *view_stmt,
                                   common::ObIArray<int64_t> &select_offset,
                                   common::ObIArray<SelectItem> &const_select_items);
  int reset_set_stmt_select_list(ObSelectStmt *select_stmt,
                                 ObIArray<int64_t> &select_offset);
  int check_set_op_is_const_expr(ObSelectStmt *select_stmt, ObRawExpr *expr, bool &is_const);
  int check_hint_allowed_query_push_down(const ObDMLStmt &stmt,
                                         const ObSelectStmt &ref_query,
                                         bool &allowed);
  DISALLOW_COPY_AND_ASSIGN(ObTransformQueryPushDown);
};
}
}

#endif /* _OB_TRANSFORM_QUERY_PUSH_DOWN_H */

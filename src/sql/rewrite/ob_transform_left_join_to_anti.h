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

#ifndef _OB_TRANSFORM_LEFT_JOIN_TO_ANTI_H
#define _OB_TRANSFORM_LEFT_JOIN_TO_ANTI_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase
{
namespace sql
{
class ObTransformLeftJoinToAnti: public ObTransformRule 
{
public:
  explicit ObTransformLeftJoinToAnti(ObTransformerCtx *ctx) :
    ObTransformRule(ctx, TransMethod::POST_ORDER, T_LEFT_TO_ANTI) {}
  virtual ~ObTransformLeftJoinToAnti() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
private:
  int check_can_be_trans(ObDMLStmt *stmt,
                         const ObIArray<ObRawExpr *> &cond_exprs,
                         const JoinedTable *joined_table,
                         ObIArray<ObRawExpr *> &target_exprs,
                         ObIArray<ObRawExpr *> &constraints,
                         bool &is_valid);
  int check_hint_valid(const ObDMLStmt &stmt,
                       const TableItem &table,
                       bool &is_valid);
  int transform_left_join_to_anti_join_rec(ObDMLStmt *stmt,
                                           TableItem *table,
                                           ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                           bool is_root_table,
                                           bool &trans_happened);
  int transform_left_join_to_anti_join(ObDMLStmt *&stmt,
                                       TableItem *table,
                                       ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                       bool is_root_table,
                                       bool &trans_happened);
  int trans_stmt_to_anti(ObDMLStmt *stmt,
                         JoinedTable *joined_table);
  int construct_trans_table_list(const ObDMLStmt *stmt,
                                 const TableItem *table,
                                 ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  
  int get_column_ref_in_is_null_condition(const ObRawExpr *expr, 
                                          ObIArray<const ObRawExpr*> &target);
  int check_condition_expr_validity(const ObRawExpr *expr,
                                    ObDMLStmt *stmt,
                                    const JoinedTable *joined_table,
                                    ObIArray<ObRawExpr *> &constraints,
                                    bool &is_valid);
  int clear_for_update(TableItem *table);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OB_TRANSFORM_LEFT_JOIN_TO_ANTI_H */

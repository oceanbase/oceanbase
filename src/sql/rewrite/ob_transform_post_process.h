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

#ifndef OB_TRANSFORM_POST_PROCESS_H_
#define OB_TRANSFORM_POST_PROCESS_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {

class ObTransformPostProcess : public ObTransformRule {
public:
  ObTransformPostProcess(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformPostProcess()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int extract_calculable_expr(ObDMLStmt*& stmt, bool& trans_happened);

  int transform_for_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened);

  int pullup_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened);

  int transform_prior_exprs(ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table);
  int get_prior_exprs(ObIArray<ObRawExpr*>& expr, ObIArray<ObRawExpr*>& prior_exprs);
  int get_prior_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& prior_exprs);

  int modify_prior_exprs(ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table,
      ObIArray<ObRawExpr*>& prior_exprs, ObIArray<ObRawExpr*>& convert_exprs);

  int make_connect_by_joined_table(ObSelectStmt& stmt, TableItem& left_table, TableItem& right_table);

  int merge_mock_view(ObDMLStmt* stmt, TableItem& table);

  int check_select_item(ObSelectStmt& stmt, bool& has_dup_expr, bool& is_all_column_expr);

  DISALLOW_COPY_AND_ASSIGN(ObTransformPostProcess);
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OB_TRANSFORM_POST_PROCESS_H_ */

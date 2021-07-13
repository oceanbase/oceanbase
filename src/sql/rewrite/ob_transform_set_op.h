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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SET_OP_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SET_OP_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {
namespace common {
class ObExprCtx;
};

namespace sql {
class ObTransformSetOp : public ObTransformRule {
public:
  explicit ObTransformSetOp(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::PRE_ORDER)
  {}
  virtual ~ObTransformSetOp()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int add_limit_order_distinct_for_union(
      const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened);
  int remove_order_by_for_set(ObDMLStmt*& stmt, bool& trans_happened);

  int recursive_adjust_order_by(ObSelectStmt* stmt, ObRawExpr* cur_expr, ObRawExpr*& find_expr);
  int is_calc_found_rows_for_union(
      const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& is_calc);

  int add_distinct(ObSelectStmt* stmt, ObSelectStmt* select_stmt);
  int add_limit(ObSelectStmt* stmt, ObSelectStmt* select_stmt);
  int add_order_by(ObSelectStmt* stmt, ObSelectStmt* select_stmt);
  int check_can_push(ObSelectStmt* stmt, ObSelectStmt* upper_stmt, bool& can_push);

  int add_limit_order_for_union(const common::ObIArray<ObParentDMLStmt>& parent_stmts, ObSelectStmt*& stmt);
  int check_can_pre_push(ObSelectStmt* stmt, ObSelectStmt* upper_stmt, bool& can_push);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformSetOp);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SET_OP_

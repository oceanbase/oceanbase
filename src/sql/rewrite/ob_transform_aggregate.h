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

#ifndef _OB_TRANSFORM_AGGREGATE_H
#define _OB_TRANSFORM_AGGREGATE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/parser/ob_item_type.h"
namespace oceanbase {
namespace common {
class ObIAllocator;
template <typename T>
class ObIArray;
}  // namespace common
namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
}  // namespace oceanbase

namespace oceanbase {
namespace sql {
class ObSelectStmt;
class ObDMLStmt;
class ObStmt;
class ObRawExpr;
class ObOpRawExpr;
class ObColumnRefRawExpr;
class ObAggFunRawExpr;
class SelectItem;

class ObTransformAggregate : public ObTransformRule {
public:
  explicit ObTransformAggregate(ObTransformerCtx* ctx);
  virtual ~ObTransformAggregate();
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  const static char* SUBQUERY_COL_ALIAS;

  int is_valid_column_aggregate(ObSelectStmt* stmt, ObAggFunRawExpr*& aggr_expr, bool& is_valid);

  int is_valid_const_aggregate(ObSelectStmt* stmt, bool& is_valid);

  int transform_column_aggregate(ObSelectStmt* stmt, bool& trans_happened);

  int transform_const_aggregate(ObSelectStmt* stmt, bool& trans_happened);

  int transform_aggregate(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr, bool& trans_happened);

  int is_valid_index_column(const ObSelectStmt* stmt, const ObRawExpr* expr, bool& is_expected_index);

  int is_valid_having(const ObSelectStmt* stmt, const ObAggFunRawExpr* column_aggr_expr, bool& is_expected);

  int is_valid_aggr_expr(
      const ObSelectStmt* stmt, const ObRawExpr* expr, ObAggFunRawExpr*& column_aggr_expr, bool& is_valid);

  int recursive_find_unexpected_having_expr(
      const ObAggFunRawExpr* aggr_expr, const ObRawExpr* cur_expr, bool& is_unexpected);

  int create_stmt(ObSelectStmt* stmt, ObSelectStmt*& child_stmt);

  int transform_upper_stmt(ObSelectStmt* upper_stmt, ObSelectStmt* child_stmt, ObAggFunRawExpr* aggr_expr);

  int set_upper_from_item(ObSelectStmt* upper_stmt, ObSelectStmt* child_stmt);

  int set_upper_select_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr);

  int clear_unused_attribute(ObSelectStmt* stmt) const;

  int transform_child_stmt(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr);

  int set_child_select_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr);

  int set_child_condition(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr);

  int set_child_order_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr);

  int set_child_limit_item(ObSelectStmt* stmt);

  int is_min_max_const(ObSelectStmt* stmt, ObRawExpr* expr, bool& is_with_const);
  int is_not_const(ObSelectStmt* stmt, ObRawExpr* expr, bool& is_current_level);

private:
  int is_valid_select_list(const ObSelectStmt& stmt, const ObRawExpr*& aggr_expr, bool& is_valid);
  int64_t idx_aggr_column_;
  bool is_column_aggregate_;
  DISALLOW_COPY_AND_ASSIGN(ObTransformAggregate);
};

}  // namespace sql
}  // namespace oceanbase
#endif

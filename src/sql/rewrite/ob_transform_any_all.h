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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_ANY_ALL_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_ANY_ALL_
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/parser/ob_item_type.h"
namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObColumnRefRawExpr;
class ObQueryRefRawExpr;
class ObSelectStmt;

class ObTransformAnyAll : public ObTransformRule {
public:
  explicit ObTransformAnyAll(ObTransformerCtx* ctx);
  virtual ~ObTransformAnyAll();
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int transform_any_all(ObDMLStmt* stmt, bool& trans_happened);

  int try_transform_any_all(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);

  int do_transform_any_all(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);

  int check_any_all_as_min_max(ObRawExpr* expr, bool& is_valid);

  int transform_any_all_as_min_max(ObDMLStmt* stmt, ObRawExpr* expr, bool& trans_happened);

  int do_transform_any_all_as_min_max(
      ObSelectStmt* sel_stmt, const ObItemType aggr_type, bool is_with_all, bool& trans_happened);

  int eliminate_any_all_before_subquery(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);

  int check_any_all_removeable(ObRawExpr* expr, bool& can_be_removed);

  int clear_any_all_flag(ObDMLStmt* stmt, ObRawExpr*& expr, ObQueryRefRawExpr* query_ref);

  ObItemType get_aggr_type(ObItemType op_type, bool is_with_all);

  ObItemType query_cmp_to_value_cmp(const ObItemType cmp_type);

  DISALLOW_COPY_AND_ASSIGN(ObTransformAnyAll);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_REWRITE_OB_TRANSFORM_ANY_ALL_

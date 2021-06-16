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

#ifndef _OB_TRANSFORM_OUTERJOIN_LIMIT_PUSHDOWN_H
#define _OB_TRANSFORM_OUTERJOIN_LIMIT_PUSHDOWN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase {

namespace common {
class ObIAllocator;
}

namespace sql {
class ObRawExpr;
class ObTransformOuterJoinLimitPushDown : public ObTransformRule {
private:
  struct OjLimitPushDownHelper {
    OjLimitPushDownHelper()
        : select_stmt_(NULL),
          target_table_(NULL),
          view_table_(NULL),
          extracted_conditions_(),
          saved_order_items_(),
          is_limit_only_(false),
          need_create_view_(true)
    {}
    virtual ~OjLimitPushDownHelper(){};

    ObSelectStmt* select_stmt_;
    TableItem* target_table_;
    TableItem* view_table_;
    ObSEArray<ObRawExpr*, 8> extracted_conditions_;
    ObSEArray<OrderItem, 8> saved_order_items_;
    bool is_limit_only_;
    bool need_create_view_;

    TO_STRING_KV(K_(select_stmt), K_(target_table), K_(view_table), K_(extracted_conditions), K_(saved_order_items),
        K_(is_limit_only), K_(need_create_view));
  };

public:
  explicit ObTransformOuterJoinLimitPushDown(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::PRE_ORDER)
  {}

  virtual ~ObTransformOuterJoinLimitPushDown()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int check_stmt_validity(ObDMLStmt* stmt, OjLimitPushDownHelper& helper, bool& is_valid);

  int do_transform(OjLimitPushDownHelper& helper);

  int check_basic(ObDMLStmt* stmt, OjLimitPushDownHelper& helper, bool& is_valid);

  int check_orderby_and_condition(OjLimitPushDownHelper& helper, bool& is_valid);

  int collect_condition_exprs_table_ids(ObSelectStmt* select_stmt, ObSqlBitSet<>& table_ids, bool& is_valid_condition,
      ObIArray<ObRawExpr*>& extracted_conditions);

  int collect_orderby_table_ids(ObSelectStmt* select_stmt, ObSqlBitSet<>& table_ids, bool& is_valid_orderby);

  int remove_and_copy_condition_orderby(
      ObSelectStmt* stmt, ObIArray<ObRawExpr*>& extracted_conditions, ObIArray<OrderItem>& saved_order_items);

  int check_offset_limit_expr(ObRawExpr* offset_limit_expr, bool& is_valid);

  int check_limit(ObSelectStmt* select_stmt, bool& is_valid);

  int check_join_type(TableItem* table_item, ObJoinType joined_type, bool& is_type_deep);

  int find_target_table(ObSelectStmt* select_stmt, ObSqlBitSet<> table_ids, TableItem*& target_table);

  int check_validity_for_target_table(OjLimitPushDownHelper& helper, bool& is_valid);

  int prepare_view_table(ObSelectStmt* stmt, TableItem* target_table, ObIArray<ObRawExpr*>& extracted_conditions,
      ObIArray<OrderItem>& saved_order_items, TableItem*& view_table);

  int pushdown_view_table(ObSelectStmt* stmt, TableItem* target_table, ObIArray<ObRawExpr*>& extracted_conditions,
      ObIArray<OrderItem>& saved_order_items, bool need_rename, bool is_limit_only);

  int add_condition_expr_for_viewtable(
      ObSelectStmt* generated_view, ObIArray<ObRawExpr*>& extracted_conditions, bool need_rename);

  int add_orderby_for_viewtable(
      ObSelectStmt* generated_view, ObSelectStmt* upper_stmt, ObIArray<OrderItem>& saved_order_items, bool need_rename);

  int add_limit_for_viewtable(ObSelectStmt* generated_view, ObSelectStmt* upper_stmt);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_TRANSFORM_OUTERJOIN_LIMIT_PUSHDOWN_H */

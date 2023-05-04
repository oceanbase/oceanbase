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

#ifndef OB_TRANSFORM_SIMPLIFY_ORDER_BY_H
#define OB_TRANSFORM_SIMPLIFY_ORDER_BY_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifyOrderby : public ObTransformRule
{
public:
  ObTransformSimplifyOrderby(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER,
                      T_SIMPLIFY_ORDER_BY)
  {}

  virtual ~ObTransformSimplifyOrderby() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

  int remove_order_by_for_subquery(ObDMLStmt *stmt, bool &trans_happened);

  int remove_order_by_for_view_stmt(ObDMLStmt *stmt, bool &trans_happened, bool &force_serial_set_order);

  int do_remove_stmt_order_by(ObSelectStmt *select_stmt, bool &trans_happened);

  int remove_order_by_duplicates(ObDMLStmt *stmt,
                                 bool &trans_happened);

  int exist_item_by_expr(ObRawExpr *expr,
                         ObIArray<OrderItem> &order_items,
                         bool &is_exist);

  int remove_order_by_for_set_stmt(ObDMLStmt *&stmt, bool &trans_happened, bool& force_serial_set_order);
};

}
}

#endif // OB_TRANSFORM_SIMPLIFY_ORDER_BY_H

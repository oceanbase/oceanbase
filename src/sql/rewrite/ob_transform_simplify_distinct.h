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

#ifndef OB_TRANSFORM_SIMPLIFY_DISTINCT_H
#define OB_TRANSFORM_SIMPLIFY_DISTINCT_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifyDistinct : public ObTransformRule
{
public:
  ObTransformSimplifyDistinct(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER,
                      T_SIMPLIFY_DISTINCT)
  {}

  virtual ~ObTransformSimplifyDistinct() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

  int remove_distinct_on_const_exprs(ObSelectStmt *stmt, bool &trans_happened);

  int distinct_can_be_eliminated(ObSelectStmt *stmt, bool &is_valid);

  int remove_distinct_on_unique_exprs(ObSelectStmt *stmt, bool &trans_happened);

  int remove_child_stmt_distinct(ObSelectStmt *set_stmt, bool &trans_happened);

  int try_remove_child_stmt_distinct(ObSelectStmt *stmt, bool &trans_happened);

};

}
}
#endif // OB_TRANSFORM_SIMPLIFY_DISTINCT_H

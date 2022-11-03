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

#ifndef OB_TRANSFORM_SIMPLIFY_LIMIT_H
#define OB_TRANSFORM_SIMPLIFY_LIMIT_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifyLimit : public ObTransformRule
{
public:
  ObTransformSimplifyLimit(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER,
                      T_SIMPLIFY_LIMIT)
  {}

  virtual ~ObTransformSimplifyLimit() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                    bool &trans_happened);

  int check_need_add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                               SemiInfo *semi_info,
                                               bool &need_add);

  int pushdown_limit_offset(ObDMLStmt *stmt,
                            bool &trans_happened);

  int check_pushdown_limit_offset_validity(ObSelectStmt *upper_stmt,
                                           ObSelectStmt *&view_stmt,
                                           bool &is_valid);

  int do_pushdown_limit_offset(ObSelectStmt *upper_stmt,
                               ObSelectStmt *view_stmt);

  int pushdown_limit_order_for_union(ObDMLStmt *stmt, bool& trans_happened);

  int check_can_pushdown_limit_order(ObSelectStmt& upper_stmt,
                                     ObSelectStmt*& view_stmt,
                                     bool& can_push);

  int do_pushdown_limit_order_for_union(ObSelectStmt& upper_stmt, ObSelectStmt* view_stmt);
};

}
}
#endif // OB_TRANSFORM_SIMPLIFY_LIMIT_H

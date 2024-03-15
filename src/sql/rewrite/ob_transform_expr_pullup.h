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

#ifndef OB_TRANSFORM_EXPR_PULLUP_H
#define OB_TRANSFORM_EXPR_PULLUP_H


#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {


class ObExprNodeMap
{
public:
  struct ExprCounter {
    ExprCounter() : expr_(nullptr), count_(0) {}
    ObRawExpr *expr_;
    int64_t count_;
  };
  ObExprNodeMap() {}
  int init();
  int add_expr_map(ObRawExpr *expr);
  int is_exist(ObRawExpr *expr, bool &is_exist);
  int get_ref_count(ObRawExpr *expr, int64_t &ref_count);
private:
  uint64_t get_hash_value(ObRawExpr *expr) {
    return (uint64_t)(expr);
  }
private:
  hash::ObHashMap<uint64_t, ExprCounter, hash::NoPthreadDefendMode> expr_map_;
};

class ObTransformExprPullup : public ObTransformRule
{
public:
  ObTransformExprPullup(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_PULLUP_EXPR)
  {}
  virtual ~ObTransformExprPullup() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened) override;
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt, bool &need_trans) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
protected:

private:
  int build_parent_reject_exprs_map(ObSelectStmt &parent,
                                    ObExprNodeMap &expr_reject_map,
                                    ObExprNodeMap &subquery_reject_map);
  int check_stmt_validity(const ObDMLStmt *stmt, bool &is_valid);
  int rewrite_decision_by_hint(ObSelectStmt &parent, ObSelectStmt &ref_query,
                               bool stmt_may_reduce_row_count, bool &go_rewrite);
  int transform_view_recursively(TableItem *table_item, ObSelectStmt &stmt,
                                 ObExprNodeMap &parent_reject_expr_map,
                                 ObExprNodeMap &parent_reject_subquery_map,
                                 common::ObIArray<ObSelectStmt *> &transformed_views,
                                 bool stmt_may_reduce_row_count, bool &trans_happened);
  int pullup_expr_from_view(TableItem *view, ObSelectStmt &select_stmt,
                            ObExprNodeMap &parent_reject_expr_map,
                            ObExprNodeMap &parent_reject_subquery_map,
                            common::ObIArray<ObSelectStmt *> &transformed_stmts,
                            bool stmt_may_reduce_row_count, bool &trans_happened);
  int extract_params(ObRawExpr *expr, ObExprNodeMap &child_reject_map,
                     common::ObIArray<ObRawExpr*> &param_exprs);
  int search_expr_cannot_pullup(ObRawExpr *expr,
                                ObExprNodeMap &expr_map,
                                common::ObIArray<ObRawExpr *> &expr_cannot_pullup);
  int is_stmt_may_reduce_row_count(const ObSelectStmt &stmt, bool &is_true);

  static bool is_view_acceptable_for_rewrite(TableItem &view);
  static bool expr_can_pullup(ObRawExpr *expr);
  static bool expr_need_pullup(ObRawExpr *expr);

};
}
}

#endif // OB_TRANSFORM_EXPR_PULLUP_H

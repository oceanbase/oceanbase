/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_TRANSFORM_SIMPLIFY_WINFUNC_H
#define OB_TRANSFORM_SIMPLIFY_WINFUNC_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifyWinfunc : public ObTransformRule
{
public:
  ObTransformSimplifyWinfunc(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER,
                      T_SIMPLIFY_WINFUNC)
  {}

  virtual ~ObTransformSimplifyWinfunc() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

  int remove_stmt_win(ObSelectStmt *select_stmt, bool &trans_happened);

  int check_aggr_win_can_be_removed(const ObDMLStmt *stmt,
                                    ObRawExpr *expr,
                                    bool &can_remove);

  int transform_aggr_win_to_common_expr(ObSelectStmt *select_stmt,
                                        ObRawExpr *expr,
                                        ObRawExpr *&new_expr);
  int get_param_value(const ObDMLStmt *stmt,
                      ObRawExpr *param,
                      bool &is_valid,
                      int64_t &value);

  int check_stmt_win_can_be_removed(ObSelectStmt *select_stmt,
                                    ObWinFunRawExpr *win_expr,
                                    bool &can_be);
  int check_window_contain_aggr(ObWinFunRawExpr *win_expr,
                                bool &contain);

  int do_remove_stmt_win(ObSelectStmt *select_stmt,
                         ObIArray<ObRawExpr*> &exprs);

  int simplify_win_exprs(ObSelectStmt *stmt,
                         bool &trans_happened);

  int simplify_win_expr(ObWinFunRawExpr &win_expr,
                        bool &trans_happened);

  int rebuild_win_compare_range_expr(ObWinFunRawExpr &win_expr,
                                     ObRawExpr* order_expr);
};

}
}

#endif // OB_TRANSFORM_SIMPLIFY_WINFUNC_H

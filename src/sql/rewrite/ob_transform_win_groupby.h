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

#ifndef OB_TRANSFORM_WIN_GROUPBY_H_
#define OB_TRANSFORM_WIN_GROUPBY_H_

#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformWinGroupBy : public ObTransformRule {
private:
  struct WinGroupByHelper {
    WinGroupByHelper()
        : transformed_stmt_(NULL),
          winfun_stmt_(NULL),
          outer_stmt_(NULL),
          inner_stmt_(NULL),
          ref_groupby_exprs_(),
          ref_column_exprs_(),
          inner_pos_(),
          outer_pos_(),
          outer_table_item_(),
          outer_old_exprs_(),
          outer_new_exprs_(),
          dummy_expr_(NULL)
    {}
    virtual ~WinGroupByHelper(){};
    // final output statement
    ObSelectStmt* transformed_stmt_;
    // intermediate statement that should produces window function
    ObSelectStmt* winfun_stmt_;
    // original outer statement that should accept group by exprs
    ObSelectStmt* outer_stmt_;
    // original inner statement having window functions to be moved out
    ObSelectStmt* inner_stmt_;
    // inner select exprs used in outer group by exprs
    ObSEArray<ObRawExpr*, 16> ref_groupby_exprs_;
    // inner select exprs used in outer group by and agg exprs
    ObSEArray<ObRawExpr*, 16> ref_column_exprs_;
    // maintain the inner select items position that need be pulled up
    ObSEArray<int64_t, 16> inner_pos_;
    // maintain the inner window exprs that need be pulled up
    ObSEArray<ObWinFunRawExpr*, 16> inner_winexprs_;
    // maintain the outer select items position that need be pulled up
    ObSEArray<int64_t, 16> outer_pos_;
    TableItem* outer_table_item_;
    // old window exprs from inner stmt
    ObSEArray<ObRawExpr*, 16> outer_old_exprs_;
    // new window exprs from winfun stmt
    ObSEArray<ObRawExpr*, 16> outer_new_exprs_;
    // dummy expr
    ObConstRawExpr* dummy_expr_;
    TO_STRING_KV(K_(ref_groupby_exprs), K_(ref_column_exprs), K_(inner_pos), K_(inner_winexprs), K_(outer_pos),
        K_(outer_old_exprs), K_(outer_new_exprs));
  };

public:
  explicit ObTransformWinGroupBy(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformWinGroupBy()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int do_win_groupby_transform(WinGroupByHelper& helper);
  int adjust_inner_stmt(WinGroupByHelper& helper);
  int adjust_outer_stmt(WinGroupByHelper& helper);
  int adjust_stmt_expr(ObSelectStmt* outer_stmt, ObSelectStmt* middle_stmt, ObSelectStmt* inner_stmt,
      const common::ObIArray<ObRawExpr*>& bypass_exprs, const common::ObIArray<ObRawExpr*>* replace_exprs,
      ObRawExpr*& inner_expr);
  int adjust_select_item(ObSelectStmt* outer_stmt, ObSelectStmt* inner_stmt, ObRawExpr*& inner_expr);
  int adjust_win_func_expr(WinGroupByHelper& helper, ObWinFunRawExpr* win_expr);
  int check_transform_validity(ObDMLStmt* stmt, WinGroupByHelper& helper, bool& is_valid);
  int check_outer_stmt_validity(WinGroupByHelper& helper, bool& is_valid);
  int check_inner_stmt_validity(WinGroupByHelper& helper, bool& is_valid);
  int check_win_expr_valid(ObRawExpr* expr, const common::ObIArray<ObRawExpr*>& stmt_exprs,
      const common::ObIArray<ObRawExpr*>& groupby_exprs, common::ObIArray<ObWinFunRawExpr*>& win_exprs,
      common::ObIArray<ObRawExpr*>& remaining_column_exprs);

  int mark_select_expr(ObSelectStmt& stmt, const bool add_flag = true);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_TRANSFORM_WIN_GROUPBY_H_ */

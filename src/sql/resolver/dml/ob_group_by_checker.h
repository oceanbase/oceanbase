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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_GROUP_BY_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_GROUP_BY_CHECKER_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {

class ObGroupByChecker : public ObRawExprVisitor {
public:
  ObGroupByChecker(ObIArray<ObRawExpr*>* group_by_exprs, ObIArray<ObRawExpr*>* rollup_exprs = nullptr,
      ObIArray<ObGroupbyExpr>* groupby_exprs = nullptr)
      : ObRawExprVisitor(),
        level_(-1),
        group_by_exprs_(group_by_exprs),
        rollup_exprs_(rollup_exprs),
        grouping_sets_exprs_(groupby_exprs),
        cur_stmts_(),
        skip_expr_(nullptr),
        top_stmt_(nullptr),
        query_ctx_(nullptr),
        has_nested_aggr_(false)
  {}
  virtual ~ObGroupByChecker()
  {}

  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr& expr);
  virtual int visit(ObVarRawExpr& expr);
  virtual int visit(ObQueryRefRawExpr& expr);
  virtual int visit(ObColumnRefRawExpr& expr);
  virtual int visit(ObOpRawExpr& expr);
  virtual int visit(ObCaseOpRawExpr& expr);
  virtual int visit(ObAggFunRawExpr& expr);
  virtual int visit(ObSysFunRawExpr& expr);
  virtual int visit(ObSetOpRawExpr& expr);
  virtual int visit(ObAliasRefRawExpr& expr);
  virtual int visit(ObFunMatchAgainst& expr);
  virtual int visit(ObWinFunRawExpr& expr);
  virtual int visit(ObPseudoColumnRawExpr& expr);

  // set expr skip
  virtual bool skip_child(ObRawExpr& expr)
  {
    return skip_expr_ == &expr;
  }

private:
  int64_t level_;
  ObIArray<ObRawExpr*>* group_by_exprs_;
  ObIArray<ObRawExpr*>* rollup_exprs_;
  ObIArray<ObGroupbyExpr>* grouping_sets_exprs_;
  ObArray<const ObSelectStmt*> cur_stmts_;
  ObRawExpr* skip_expr_;
  const ObSelectStmt* top_stmt_;
  ObQueryCtx* query_ctx_;
  bool has_nested_aggr_;

private:
  bool is_top_select_stmt()
  {
    return 0 == level_;
  }
  void set_skip_expr(ObRawExpr* expr)
  {
    skip_expr_ = expr;
  }
  void set_query_ctx(ObQueryCtx* query_ctx)
  {
    query_ctx_ = query_ctx;
  }
  bool find_in_group_by(ObRawExpr& expr);
  bool find_in_rollup(ObRawExpr& expr);
  bool find_in_grouping_sets(ObRawExpr& expr);
  int belongs_to_check_stmt(ObRawExpr& expr, bool& belongs_to);
  int colref_belongs_to_check_stmt(ObRawExpr& expr, bool& belongs_to);
  int check_select_stmt(const ObSelectStmt* ref_stmt);
  int add_pc_const_param_info(ObExprEqualCheckContext& check_context);
  void set_nested_aggr(bool has_nested_aggr)
  {
    has_nested_aggr_ = has_nested_aggr;
  }
  bool has_nested_aggr()
  {
    return has_nested_aggr_;
  }
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGroupByChecker);

public:
  void set_level(int64_t level)
  {
    level_ = level;
  }
  static int check_group_by(ObSelectStmt* ref_stmt);
  static int check_groupby_valid(ObRawExpr* expr);
  static int check_by_expr(const ObSelectStmt* ref_stmt, common::ObIArray<ObRawExpr*>& group_by_exprs,
      common::ObIArray<ObRawExpr*>& checked_exprs, int err_code);
  static int check_analytic_function(ObSelectStmt* ref_stmt, common::ObIArray<ObRawExpr*>& arg_exp_arr,
      common::ObIArray<ObRawExpr*>& partition_exp_arr);
  static int check_scalar_groupby_valid(ObRawExpr* expr);
};

}  // namespace sql
}  // namespace oceanbase
#endif

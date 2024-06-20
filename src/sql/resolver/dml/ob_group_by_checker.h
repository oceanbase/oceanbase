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

namespace oceanbase
{
namespace sql
{

class ObGroupByChecker: public ObRawExprVisitor
{
public:
// if param_store is not null, group by checker will extract plan cache const constraint
  ObGroupByChecker(const ParamStore *param_store,
                   ObIArray<ObRawExpr*> *group_by_exprs,
                   ObIArray<ObRawExpr*> *rollup_exprs = nullptr,
                   ObIArray<ObRawExpr*> *cube_exprs = nullptr,
                   ObIArray<ObGroupbyExpr> *groupby_exprs = nullptr)
    : ObRawExprVisitor(),
      level_(-1),
      group_by_exprs_(group_by_exprs),
      rollup_exprs_(rollup_exprs),
      cube_exprs_(cube_exprs),
      grouping_sets_exprs_(groupby_exprs),
      cur_stmts_(),
      skip_expr_(nullptr),
      top_stmt_(nullptr),
      query_ctx_(nullptr),
      has_nested_aggr_(false),
      is_check_order_by_(false),
      dblink_groupby_expr_(NULL),
      only_need_contraints_(false),
      param_store_(param_store)

  {}
  virtual ~ObGroupByChecker()
  {}

  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);

  // set expr skip
  virtual bool skip_child(ObRawExpr &expr)
  { return skip_expr_ == &expr || expr.is_query_ref_expr(); }
private:
  int64_t level_;
  ObIArray<ObRawExpr*> *group_by_exprs_;
  ObIArray<ObRawExpr*> *rollup_exprs_;
  ObIArray<ObRawExpr*> *cube_exprs_;
  ObIArray<ObGroupbyExpr> *grouping_sets_exprs_;
  ObArray<const ObSelectStmt*> cur_stmts_;
  ObRawExpr *skip_expr_;
  const ObSelectStmt *top_stmt_;
  ObQueryCtx *query_ctx_;
  bool has_nested_aggr_;
  bool is_check_order_by_;
  common::ObIArray<ObRawExpr*> *dblink_groupby_expr_;
  // if true, only add constraints for shared exprs which will be replaced in replace_stmt_expr_with_groupby_exprs
  bool only_need_contraints_;
  const ParamStore *param_store_;
private:
  // Top select stmt 是指当前调用group by checker的select_stmt，不是select_stmt的level
  // 其他select_stmt会每进入一层，递增一层，同时检查结束退出，会递减一层
  bool is_top_select_stmt() { return 0 == level_; }
  void set_skip_expr(ObRawExpr *expr) { skip_expr_ = expr; }
  void set_query_ctx(ObQueryCtx *query_ctx) { query_ctx_ = query_ctx; }
  bool find_in_group_by(ObRawExpr &expr);
  bool find_in_rollup(ObRawExpr &expr);
  bool find_in_cube(ObRawExpr &expr);
  bool find_in_grouping_sets(ObRawExpr &expr);
  int add_abs_equal_constraint_in_grouping_sets(ObConstRawExpr &expr);
  bool check_obj_abs_equal(const ObObj &obj1, const ObObj &obj2);
  int belongs_to_check_stmt(ObRawExpr &expr, bool &belongs_to);
  int colref_belongs_to_check_stmt(ObColumnRefRawExpr &expr, bool &belongs_to);
  int check_select_stmt(const ObSelectStmt *ref_stmt);
  int add_pc_const_param_info(ObExprEqualCheckContext &check_context);
  void set_nested_aggr(bool has_nested_aggr) { has_nested_aggr_ = has_nested_aggr; }
  bool has_nested_aggr() { return has_nested_aggr_; }
  void set_check_order_by(bool check_order_by) { is_check_order_by_ = check_order_by; }
  void set_dblink_groupby_expr(common::ObIArray<ObRawExpr*> *dblink_groupby_array);
  static int check_groupby_valid(ObRawExpr *expr);
  static int check_scalar_groupby_valid(ObRawExpr *expr);
// disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGroupByChecker);
public:
  void set_level(int64_t level) { level_ = level; }
  // all functions below should only called in resolver
  static int check_group_by(const ParamStore *param_store,
                            ObSelectStmt *ref_stmt,
                            bool has_having_self_column,
                            bool has_group_by_clause,
                            bool only_need_constraints);
  static int check_by_expr(const ParamStore *param_store,
                           const ObSelectStmt *ref_stmt,
                           common::ObIArray<ObRawExpr*> &group_by_exprs,
                           common::ObIArray<ObRawExpr*> &checked_exprs,
                           int err_code,
                           bool is_check_order_by = false);
  static int dblink_check_groupby(
    const ObSelectStmt *ref_stmt,
    common::ObIArray<ObRawExpr*> &group_by_exprs,
    common::ObIArray<ObRawExpr*> &rollup_exprs,
    const common::ObIArray<ObRawExpr*> &checked_exprs);
  static int check_analytic_function(const ParamStore *param_store,
                                     ObSelectStmt *ref_stmt,
                                     common::ObIArray<ObRawExpr *> &arg_exp_arr,        //等价于查询项中表达式
                                     common::ObIArray<ObRawExpr *> &partition_exp_arr); //等价于group by项
};

}
}
#endif

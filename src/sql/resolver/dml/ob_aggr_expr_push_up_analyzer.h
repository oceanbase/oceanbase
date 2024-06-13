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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_
#include "lib/container/ob_array.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_stmt_expr_visitor.h"
namespace oceanbase
{
namespace sql
{
class ObDMLResolver;
class ObAggFunRawExpr;
class ObSelectResolver;
class ObSelectStmt;

class ObStmtExecParamReplacer : public ObStmtExprVisitor
{
public:
  ObStmtExecParamReplacer() {}
  virtual int do_visit(ObRawExpr *&expr) override;
  int add_replace_exprs(const ObIArray<ObExecParamRawExpr *> &from_exprs,
                        const ObIArray<ObExecParamRawExpr *> &to_exprs);
  int check_need_replace(const ObRawExpr *old_expr, ObRawExpr *&new_expr, bool &need_replace);
private:
  hash::ObHashSet<uint64_t> to_exprs_;
  hash::ObHashMap<uint64_t, uint64_t> expr_replace_map_;
};

class ObAggrExprPushUpAnalyzer
{
public:
  ObAggrExprPushUpAnalyzer(ObSelectResolver &cur_resolver)
    : has_cur_layer_column_(false),
      cur_resolver_(cur_resolver) {}
  ~ObAggrExprPushUpAnalyzer() {}

  int analyze_and_push_up_aggr_expr(ObRawExprFactory &expr_factory,
                                    ObAggFunRawExpr *aggr_expr,
                                    ObRawExpr *&final_aggr);
private:
  int analyze_aggr_param_expr(ObRawExpr *&param_expr,
                              bool is_in_aggr_expr,
                              bool is_root = false,
                              bool is_child_stmt = false);

  int analyze_child_stmt(ObSelectStmt* child_stmt);

  int get_min_level_resolver(ObSelectResolver *&resolver);

  ObSelectResolver *fetch_final_aggr_resolver(ObDMLResolver *cur_resolver,
                                              ObSelectResolver *min_level_resolver);

  int get_exec_params(ObDMLResolver *resolver,
                      ObIArray<ObExecParamRawExpr *> &all_exec_params,
                      ObIArray<ObExecParamRawExpr *> &my_exec_params);

  int push_up_aggr_column(ObSelectResolver *resolver);

  int push_up_subquery_in_aggr(ObSelectResolver &final_resolver,
                               const ObIArray<ObQueryRefRawExpr *> &query_refs);

  int check_param_aggr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                       bool &has_aggr);

  int has_aggr_expr(const ObRawExpr *expr, bool &has);

  int remove_alias_exprs();

  int remove_alias_exprs(ObRawExpr* &expr);
  int replace_final_exec_param_in_aggr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                       ObIArray<ObQueryRefRawExpr *> &param_query_refs,
                                       ObRawExprFactory &expr_factory);
private:
  // contain current layer column (a real column or a alias select item)
  bool has_cur_layer_column_;

  // 1. ref an upper layer column
  // 2. ref an upper layer select item
  common::ObArray<ObExecParamRawExpr *, common::ModulePageAllocator, true> exec_columns_;

  // aggregate function in this resolver
  ObSelectResolver &cur_resolver_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_ */

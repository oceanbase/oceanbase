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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_SET_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_SET_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_context.h"
#include <utility>

namespace oceanbase
{
namespace sql
{
class ObTransformSimplifySet : public ObTransformRule
{
private:
  struct SimplifySetHelper {
    SimplifySetHelper() :
      const_constraint_exprs_(),
      precalc_constraint_exprs_() {};
    virtual ~SimplifySetHelper() {};

    /* we need to store the const false exprs  and limit 0 exprs in each branches,
     */
    ObSEArray<std::pair<ObRawExpr*, int64_t>, 1> const_constraint_exprs_;
    ObSEArray<std::pair<ObRawExpr*, int64_t>, 1> precalc_constraint_exprs_;

    void reset() {
      const_constraint_exprs_.reset();
      precalc_constraint_exprs_.reset();
    }
    int assign(const SimplifySetHelper &other); 
    TO_STRING_KV(K_(const_constraint_exprs),
                 K_(precalc_constraint_exprs));
  };
public:
  explicit ObTransformSimplifySet(ObTransformerCtx *ctx)
      : ObTransformRule(ctx, TransMethod::PRE_ORDER, T_SIMPLIFY_SET) {}
  virtual ~ObTransformSimplifySet() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int add_limit_order_distinct_for_union(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         ObDMLStmt *&stmt,
                                         bool &trans_happened);

  int is_calc_found_rows_for_union(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                   ObDMLStmt *&stmt, bool &is_calc);

  int add_distinct(ObSelectStmt *stmt, ObSelectStmt *select_stmt);
  int add_limit(ObSelectStmt *stmt, ObSelectStmt *select_stmt);
  int add_order_by(ObSelectStmt *stmt, ObSelectStmt *select_stmt);
  int check_can_push(ObSelectStmt *stmt, ObSelectStmt *upper_stmt, bool &need_push_distinct,
                     bool &need_push_orderby, bool &can_push);

  int pruning_set_query(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                        ObSelectStmt *&stmt,
                        bool &trans_happened);
  int check_exprs_constant_false(common::ObIArray<ObRawExpr*> &exprs,
                                 bool &constant_false,
                                 int64_t stmt_idx,
                                 SimplifySetHelper &helper);
  int remove_set_query_in_stmt(ObSelectStmt *&stmt,
                               ObIArray<int64_t> &remove_list,
                               common::ObIArray<int64_t> &constraints_idxs,
                               bool &trans_happened);
  int check_limit_zero_in_stmt(ObRawExpr *limit_expr,
                               ObRawExpr *offset_expr,
                               ObRawExpr *percent_expr,
                               bool &need_remove,
                               int64_t stmt_idx,
                               SimplifySetHelper &helper);
  int check_set_stmt_removable(ObSelectStmt *stmt,
                               bool &need_remove,
                               int64_t stmt_idx,
                               SimplifySetHelper &helper);
  int check_first_stmt_removable(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObSelectStmt *&stmt,
                                 ObIArray<int64_t> &remove_list,
                                 bool &can_remove);
  int replace_set_stmt_with_child_stmt(ObSelectStmt *&parent_stmt,
                                      ObSelectStmt *child_stmt);
  int add_constraints_by_idx(common::ObIArray<int64_t> &constraints_idx,
                             SimplifySetHelper &helper);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformSimplifySet);
};

} //namespace sql
} //namespace oceanbase
#endif //OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_SET_

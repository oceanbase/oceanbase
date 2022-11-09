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

#ifndef OB_TRANSFORM_SIMPLIFY_EXPR_H
#define OB_TRANSFORM_SIMPLIFY_EXPR_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformSimplifyExpr : public ObTransformRule
{
public:
  ObTransformSimplifyExpr(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_SIMPLIFY_EXPR)
  {}

  virtual ~ObTransformSimplifyExpr() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int replace_is_null_condition(ObDMLStmt *stmt, bool &trans_happened);
  int inner_replace_is_null_condition(ObDMLStmt *stmt,
                                      ObRawExpr *&expr,
                                      bool &trans_happened);
  int do_replace_is_null_condition(ObDMLStmt *stmt,
                                   ObRawExpr *&expr,
                                   bool &trans_happened);
  int replace_op_null_condition(ObDMLStmt *stmt, bool &trans_happened);
  int replace_cmp_null_condition(ObRawExpr *&expr,
                                 const ObDMLStmt &stmt,
                                 const ParamStore &param_store,
                                 bool &trans_happened);

  int extract_null_expr(ObRawExpr *expr,
                        const ObDMLStmt &stmt,
                        const ParamStore &param_store,
                        ObIArray<const ObRawExpr *> &null_expr_lists);
  int is_expected_table_for_replace(ObDMLStmt *stmt, uint64_t table_id, bool &is_expected);
  int convert_preds_vector_to_scalar(ObDMLStmt *stmt, bool &trans_happened);
  int convert_join_preds_vector_to_scalar(TableItem *table_item, bool &trans_happened);
  int inner_convert_preds_vector_to_scalar(ObRawExpr *expr, ObIArray<ObRawExpr*> &exprs, bool &trans_happened);
  int remove_dummy_exprs(ObDMLStmt *stmt, bool &trans_happened);
  int remove_dummy_filter_exprs(common::ObIArray<ObRawExpr*> &exprs,
                                ObIArray<ObExprConstraint> &constraints);
  int remove_dummy_join_condition_exprs(TableItem *table,
                                        ObIArray<ObExprConstraint> &constraints);
  int inner_remove_dummy_expr(ObRawExpr *&expr,
                              ObIArray<ObExprConstraint> &constraints);
  int inner_remove_dummy_expr(common::ObIArray<ObRawExpr*> &exprs,
                              ObIArray<ObExprConstraint> &constraints);
                              
  int is_valid_transform_type(ObRawExpr *expr,
                              bool &is_valid);

  int adjust_dummy_expr(const ObIArray<int64_t> &true_exprs,
                        const ObIArray<int64_t> &false_exprs,
                        const bool is_and_op,
                        ObIArray<ObRawExpr *> &adjust_exprs,
                        ObRawExpr *&transed_expr,
                        ObIArray<ObExprConstraint> &constraints);

  int remove_dummy_case_when(ObDMLStmt *stmt, bool &trans_happened);
  int remove_dummy_case_when(ObQueryCtx* query_ctx,
                             ObRawExpr *expr,
                             bool &trans_happened);
  int inner_remove_dummy_case_when(ObQueryCtx* query_ctx,
                                   ObCaseOpRawExpr *case_expr,
                                   bool &trans_happened);

  int flatten_stmt_exprs(ObDMLStmt *stmt, bool &trans_happened);
  int flatten_join_condition_exprs(TableItem *table, bool &trans_happened);
  int flatten_exprs(common::ObIArray<ObRawExpr*> &exprs, bool &trans_happened);
};

}
}

#endif // OB_TRANSFORM_SIMPLIFY_EXPR_H

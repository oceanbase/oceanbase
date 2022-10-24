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

#ifndef _OB_TRANSFORM_SEMI_TO_INNER_H_
#define _OB_TRANSFORM_SEMI_TO_INNER_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

struct ObCostBasedRewriteCtx {
  uint64_t table_id_;
  uint64_t view_table_id_;
  bool is_multi_join_cond_;
  bool hint_force_;
  ObCostBasedRewriteCtx()
  :table_id_(OB_INVALID_ID),
  view_table_id_(OB_INVALID_ID),
  is_multi_join_cond_(false),
  hint_force_(false)
  {}
};

class ObTransformSemiToInner : public ObTransformRule
{
public:
  ObTransformSemiToInner(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_SEMI_TO_INNER)
  {}
  virtual ~ObTransformSemiToInner() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
protected:
  int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool &is_valid) override;

private:
  int transform_semi_to_inner(ObDMLStmt *root_stmt,
                              ObDMLStmt *stmt,
                              const SemiInfo *pre_semi_info,
                              ObDMLStmt *&trans_stmt,
                              ObCostBasedRewriteCtx &ctx,
                              bool &need_check_cost,
                              bool &trans_happened);

  int check_basic_validity(ObDMLStmt *root_stmt,
                           ObDMLStmt &stmt,
                           SemiInfo &semi_info,
                           ObCostBasedRewriteCtx &ctx,
                           bool &is_valid,
                           bool &need_add_distinct,
                           bool &need_check_cost,
                           bool &need_add_limit_constraint,
                           bool &right_table_need_add_limit);

  int check_right_exprs_unique(ObDMLStmt &stmt,
                               TableItem *right_table,
                               ObIArray<ObRawExpr*> &right_exprs,
                               bool &is_unique);

  int check_semi_join_condition(ObDMLStmt &stmt,
                                SemiInfo &semi_info,
                                ObIArray<ObRawExpr*> &left_exprs,
                                ObIArray<ObRawExpr*> &right_exprs,
                                bool &is_all_equal_cond,
                                bool &is_all_left_filter,
                                bool &is_multi_join_cond);

  int check_right_table_output_one_row(TableItem &right_table,
                                       bool &is_one_row);

  int check_can_add_distinct(const ObIArray<ObRawExpr*> &left_exprs,
                             const ObIArray<ObRawExpr*> &right_exprs,
                             bool &is_valid);

  int check_need_add_cast(const ObRawExpr *left_arg,
                          const ObRawExpr *right_arg,
                          bool &need_add_cast,
                          bool &is_valid);

  int check_join_condition_match_index(ObDMLStmt *root_stmt,
                                       ObDMLStmt &stmt,
                                       SemiInfo &semi_info,
                                       const common::ObIArray<ObRawExpr*> &semi_conditions,
                                       bool &is_match_index);

  int do_transform(ObDMLStmt &stmt,
                   SemiInfo *semi_info,
                   ObCostBasedRewriteCtx &ctx,
                   bool need_add_distinct,
                   bool right_table_need_add_limit);

  int find_basic_table(ObSelectStmt* stmt, uint64_t &table_id);

  int add_distinct(ObSelectStmt &view,
                   const ObIArray<ObRawExpr*> &left_exprs,
                   const ObIArray<ObRawExpr*> &right_exprs);

  int add_ignore_semi_info(const uint64_t semi_id);

  int is_ignore_semi_info(const uint64_t semi_id, bool &ignore);

  int find_operator(ObLogicalOperator* root,
                    ObIArray<ObLogicalOperator*> &parents,
                    uint64_t table_id,
                    ObLogicalOperator *&table_op);

  int check_is_semi_condition(ObIArray<ObExecParamRawExpr *> &nl_params,
                              uint64_t table_id,
                              bool &is_valid);
    int check_hint_valid(const ObDMLStmt &stmt, 
                        const TableItem& table,
                        bool &force_trans,
                        bool &force_no_trans) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformSemiToInner);
};

}
}

#endif /* _OB_TRANSFORM_SEMI_TO_INNER_H_ */

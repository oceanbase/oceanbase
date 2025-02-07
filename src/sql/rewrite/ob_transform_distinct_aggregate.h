/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TRANSFORM_DISTINCT_AGGREGATE_H
#define _OB_TRANSFORM_DISTINCT_AGGREGATE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObTransformDistinctAggregate : public ObTransformRule
{
public:
  explicit ObTransformDistinctAggregate(ObTransformerCtx *ctx)
  : ObTransformRule(ctx, TransMethod::POST_ORDER, T_TRANSFORM_DISTINCT_AGG) {}

  virtual ~ObTransformDistinctAggregate() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int check_transform_validity(const ObDMLStmt *stmt, bool &is_valid);
  int do_transform(ObSelectStmt *stmt, bool &trans_happened);
  int classify_aggr_exprs(const ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                          ObIArray<ObAggFunRawExpr*> &non_distinct_aggr,
                          ObIArray<ObAggFunRawExpr*> &distinct_aggr);
  int construct_view_select_exprs(const ObIArray<ObAggFunRawExpr*> &non_distinct_aggr,
                                  ObIArray<ObRawExpr*> &view_select_exprs);
  int construct_view_group_exprs(const ObIArray<ObRawExpr*> &ori_group_expr,
                                 const ObIArray<ObAggFunRawExpr*> &distinct_aggr,
                                 ObIArray<ObRawExpr*> &view_group_exprs);
  int replace_aggr_func(ObSelectStmt *stmt,
                        TableItem *view_table,
                        const ObIArray<ObAggFunRawExpr*> &distinct_aggr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformDistinctAggregate);
};
} // namespace sql
} // namespace oceanbase

#endif
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

#ifndef _OB_TRANSFORM_MIX_MAX_H
#define _OB_TRANSFORM_MIX_MAX_H

#include "sql/rewrite/ob_transform_rule.h"
#include "objit/common/ob_item_type.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T>
class ObIArray;
}//common
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
}//oceanbase

namespace oceanbase
{
namespace sql
{
class ObSelectStmt;
class ObDMLStmt;
class ObStmt;
class ObRawExpr;
class ObOpRawExpr;
class ObColumnRefRawExpr;
class ObAggFunRawExpr;

/* rewrite min or max aggr on index as a subquery which can table scan just one line.
 * eg:
 * select min(pk) from t1
 * -->
 * select min(v.c1) from (select pk from t1 where pk is not null order by pk limit 1)
 *
 * rewrite requests:
 * 1. max/min aggragate on a column of table, and this column is a index or the first nonconst column of index.
 * 2. select stmt is scalar group by and hasn't limit.
 * 3. just deal single table yet.
 */
class ObTransformMinMax : public ObTransformRule
{
public:
  explicit ObTransformMinMax(ObTransformerCtx *ctx);
  virtual ~ObTransformMinMax();
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  static int check_transform_validity(ObTransformerCtx &ctx,
                                      ObSelectStmt *stmt,
                                      bool &is_valid);

private:
  int do_transform(ObSelectStmt *select_stmt);

  int do_single_minmax_transform(ObSelectStmt *select_stmt);

  int do_multi_minmax_transform(ObSelectStmt *select_stmt);

  int deep_copy_subquery_for_aggr(const ObSelectStmt &copied_stmt,
                                  ObRawExpr *aggr_param,
                                  ObItemType aggr_type,
                                  ObSelectStmt *&child_stmt);

  static int is_valid_index_column(ObTransformerCtx &ctx,
                                   const ObSelectStmt *stmt,
                                   const ObRawExpr *expr,
                                   EqualSets *equal_sets,
                                   ObIArray<ObRawExpr*> *const_exprs,
                                   bool &is_valid);

  int set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_param);

  int set_child_order_item(ObSelectStmt *stmt, ObRawExpr *aggr_param, ObItemType aggr_type);

  static int is_valid_aggr_items(ObTransformerCtx &ctx, const ObSelectStmt &stmt, bool &is_valid);

  static int is_valid_select_list(const ObSelectStmt &stmt, bool &is_valid);

  static int is_valid_select_expr(const ObRawExpr *expr, bool &is_valid);

  static int is_valid_having_list(const ObSelectStmt &stmt, bool &is_valid);

  static int is_valid_having_expr(const ObRawExpr *expr, bool &is_valid);

  static int is_valid_order_list(const ObSelectStmt &stmt, bool &is_valid);

  static int is_valid_order_expr(const ObRawExpr *expr, bool &is_valid);

  DISALLOW_COPY_AND_ASSIGN(ObTransformMinMax);
};

} //namespace sql
} //namespace oceanbase
#endif

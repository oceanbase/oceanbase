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
  struct MinMaxAggrHelper {
    MinMaxAggrHelper()
      : aggr_expr_ids_(),
        raw_expr_id_(-1),
        raw_expr_ptr_(NULL) {}
    virtual ~MinMaxAggrHelper() {};
    void reset() {
      aggr_expr_ids_.reuse();
      raw_expr_id_ = -1;
      raw_expr_ptr_ = NULL;
    }
    int assign(const MinMaxAggrHelper &other);
    static int alloc_helper(ObIAllocator &allocator, MinMaxAggrHelper* &helper);

    TO_STRING_KV(K(aggr_expr_ids_),
                 K(raw_expr_id_),
                 K(raw_expr_ptr_));

    ObSEArray<int64_t, 4> aggr_expr_ids_;
    int64_t raw_expr_id_;
    ObRawExpr *raw_expr_ptr_;
  };

  // check every expr is valid
  static int check_expr_validity(ObTransformerCtx &ctx,
                                 ObSelectStmt *select_stmt,
                                 const ObRawExpr *expr,
                                 const int64_t expr_id,
                                 bool &is_valid,
                                 MinMaxAggrHelper *&helper);

  static int check_transform_validity(ObTransformerCtx &ctx,
                                      ObSelectStmt *select_stmt,
                                      bool &is_valid,
                                      ObIArray<MinMaxAggrHelper*> *selecthelpers  = NULL,
                                      ObIArray<MinMaxAggrHelper*> *havinghelpers = NULL);

private:
  int create_new_ref_expr(ObQueryRefRawExpr *&ref_expr, 
                          ObSelectStmt *aggr_ref_stmt, 
                          ObRawExpr* temp_aggr_expr);

  int do_transform_one_stmt(ObSelectStmt *select_stmt, ObAggFunRawExpr *aggr_expr, ObSelectStmt *&ref_stmt);

  int do_transform(ObDMLStmt *&select_stmt, ObIArray<MinMaxAggrHelper*> &selecthelpers, ObIArray<MinMaxAggrHelper*> &havinghelpers);

  static int is_valid_index_column(ObTransformerCtx &ctx,
                                   const ObSelectStmt *stmt,
                                   const ObRawExpr *expr,
                                   EqualSets &equal_sets,
                                   ObIArray<ObRawExpr*> &const_exprs,
                                   bool &is_expected_index);

  static int check_valid_aggr_expr(const ObRawExpr *expr,
                                   ObIArray<ObAggFunRawExpr *> &aggr_expr_array,
                                   ObIArray<int64_t> &aggr_expr_ids,
                                   bool &is_valid);

  static int inner_check_valid_aggr_expr(const ObRawExpr *expr,
                                         ObIArray<ObAggFunRawExpr *> &aggr_expr_array,
                                         ObIArray<int64_t> &aggr_expr_ids,
                                         bool &is_valid);
                                
  static int replace_aggr_expr_by_subquery(ObRawExpr *&expr,
                                           const ObAggFunRawExpr *aggr_expr,
                                           ObRawExpr *ref_expr);

  static int replace_aggr_expr_by_subquery(ObRawExpr *&expr,
                                           const ObAggFunRawExpr *aggr_expr,
                                           ObRawExpr *ref_expr,
                                           bool &is_valid);                                         

  int set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_expr);

  int set_child_order_item(ObSelectStmt *stmt, ObRawExpr *aggr_expr);

  DISALLOW_COPY_AND_ASSIGN(ObTransformMinMax);
};

} //namespace sql
} //namespace oceanbase
#endif

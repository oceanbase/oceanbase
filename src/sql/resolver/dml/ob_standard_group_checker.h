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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_STANDARD_GROUP_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_STANDARD_GROUP_CHECKER_H_
#include "lib/hash/ob_hashset.h"
#include "lib/container/ob_array.h"
namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObColumnRefRawExpr;
class ObStandardGroupChecker
{
  struct ObUnsettledExprItem
  {
    TO_STRING_KV(N_EXPR, PC(expr_),
                 K_(start_idx),
                 K_(dependent_column_cnt));

    const ObRawExpr *expr_;
    //the column start index of expr_ in dependent_columns_
    int64_t start_idx_;
    //the dependent column count of expr_ in dependent_columns_
    int64_t dependent_column_cnt_;
  };
public:
  ObStandardGroupChecker()
    : has_group_(false) {}
  ~ObStandardGroupChecker() {}

  void set_has_group(bool has_group) { has_group_ = has_group; }
  int init() { return settled_columns_.create(SETTLED_COLUMN_BUCKETS); }
  int add_group_by_expr(const ObRawExpr *expr);
  int add_unsettled_column(const ObRawExpr *column_ref);
  int add_unsettled_expr(const ObRawExpr *expr);
  int check_only_full_group_by();
private:
  int check_unsettled_expr(const ObRawExpr *unsettled_expr, const ObColumnRefRawExpr &undefined_column);
  int check_unsettled_column(const ObRawExpr *unsettled_column, const ObColumnRefRawExpr *&undefined_column);
private:
  static const int64_t SETTLED_COLUMN_BUCKETS = 64;
  bool has_group_;
  //all unsettled exprs that needed be check whether meet the only full group by semantic constraints in current stmt
  common::ObArray<ObUnsettledExprItem> unsettled_exprs_;
  //all columns in unsettled_exprs_
  common::ObArray<const ObRawExpr*> dependent_columns_;
  //all settled columns that meet the only full group by semantic constraints in current stmt
  common::hash::ObHashSet<int64_t> settled_columns_;
  //all settled exprs that meet the only full group by semantic constraints in current stmt
  common::ObArray<const ObRawExpr*> settled_exprs_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_STANDARD_GROUP_CHECKER_H_ */

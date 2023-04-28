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

#ifndef _OB_EQUAL_ANALYSIS_H
#define _OB_EQUAL_ANALYSIS_H

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_equal_set.h"
namespace oceanbase
{
namespace sql
{
typedef ObEqualSet<const ObRawExpr *, const ObRawExpr *> ObExprEqualSet;
// An algorithm class to generate all equal condition expression
struct EqualSetKey
{
  virtual bool operator ==(const EqualSetKey &other) const
  {
    if (NULL == expr_ || NULL == other.expr_) {
      return expr_ == other.expr_;
    } else {
      return (expr_ == other.expr_) || (expr_->same_as(*other.expr_));
    }
  }
  virtual uint64_t hash() const
  {
    uint64_t result = 0;
    if (NULL != expr_) {
      result = expr_->hash(result);
    }
    return result;
  }
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  const ObRawExpr *expr_;
  TO_STRING_KV(K_(expr));
};
class ObEqualAnalysis
{
public:
  explicit ObEqualAnalysis();

  virtual ~ObEqualAnalysis();
  void reset();
  int init();
  // input
  int feed_equal_sets(const EqualSets &equal_sets);
  int feed_where_expr(const ObRawExpr *expr);
  int finish_feed();
  // output
  int get_equal_sets(ObIAllocator *allocator, EqualSets &equal_sets) const;
  TO_STRING_KV(K_(equal_sets));

  static int check_type_equivalent(const ObRawExpr &cur_expr, const ObRawExpr &same_expr,
                                   const ObRawExpr &new_expr, bool &can_be);

  static int compute_equal_set(ObIAllocator *allocator,
                               const ObIArray<ObRawExpr *> &eset_conditions,
                               EqualSets &output_equal_set);

  static int compute_equal_set(ObIAllocator *allocator,
                               const ObIArray<ObRawExpr *> &eset_conditions,
                               const EqualSets &input_equal_sets,
                               EqualSets &output_equal_sets);

  static int compute_equal_set(ObIAllocator *allocator,
                               ObRawExpr *eset_condition,
                               const EqualSets &input_equal_sets,
                               EqualSets &output_equal_sets);

  static int merge_equal_set(ObIAllocator *allocator,
                               const EqualSets &left_equal_sets,
                               const EqualSets &right_equal_sets,
                               EqualSets &output_equal_sets);
protected:
  // types and constants
  typedef common::hash::ObHashMap<EqualSetKey, int64_t,
                                  common::hash::NoPthreadDefendMode> ColumnSet;
protected:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObEqualAnalysis);
  // function members
  ObExprEqualSet *new_equal_set();
  int find_equal_set(int64_t expr_idx, const ObRawExpr *new_expr, ObExprEqualSet *&equal_set_ret);
  int add_equal_cond(const ObOpRawExpr &expr);
  int get_or_add_expr_idx(const ObRawExpr *expr, int64_t &expr_idx);
  int expr_can_be_add_to_equal_set(const ObExprEqualSet &equal_set,
                                    const ObRawExpr *same_expr,
                                    const ObRawExpr *new_expr,
                                    bool &can_be) const;
  int check_whether_can_be_merged(const ObExprEqualSet &equal_set,
                                  const ObExprEqualSet &another_set,
                                  bool &can_be) const;
protected:
  // data members
  common::ObArenaAllocator equal_set_alloc_;
  ColumnSet column_set_;
  common::ObDList<ObExprEqualSet> equal_sets_;
};
} // end namespace sql
} // end namespace oceanbase
#endif /* _OB_EQUAL_ANALYSIS_H */

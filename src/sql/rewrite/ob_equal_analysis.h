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
namespace oceanbase
{
namespace sql
{
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
      result = expr_->get_expr_hash();
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
  void destroy();
  int init();
  // input
  int feed_equal_sets(const EqualSets &equal_sets);
  int feed_where_expr(ObRawExpr *expr);
  int finish_feed();
  // output
  int get_equal_sets(ObIAllocator *allocator, EqualSets &equal_sets) const;
  TO_STRING_KV(K_(parent_idx), K_(exprs));

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
private:
  // types and constants
  typedef common::hash::ObHashMap<EqualSetKey, int64_t,
                                  common::hash::NoPthreadDefendMode> ExprIdxMap;
  // function members
  int add_equal_cond(ObOpRawExpr &expr);
  int get_expr_idx(ObRawExpr *expr, int64_t &expr_idx);
  int find_root_idx(const int64_t expr_idx, int64_t &root_idx);
  int union_expr(const int64_t l_idx, const int64_t r_idx);
private:
  // data members
  ExprIdxMap expr_idx_map_;
  common::ObSEArray<int64_t, 8> parent_idx_;
  common::ObSEArray<ObRawExpr*, 8> exprs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObEqualAnalysis);
};
} // end namespace sql
} // end namespace oceanbase
#endif /* _OB_EQUAL_ANALYSIS_H */

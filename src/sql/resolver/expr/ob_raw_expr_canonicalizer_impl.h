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

#ifndef _OB_RAW_EXPR_CANONICALIZER_IMPL_H
#define _OB_RAW_EXPR_CANONICALIZER_IMPL_H
#include "sql/resolver/expr/ob_raw_expr_canonicalizer.h"
namespace oceanbase
{
namespace sql
{
class ObRawExprCanonicalizerImpl : public ObRawExprCanonicalizer
{
public:
  explicit ObRawExprCanonicalizerImpl(ObExprResolveContext &ctx);
  virtual ~ObRawExprCanonicalizerImpl() {}

  virtual int canonicalize(ObRawExpr *&expr);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprCanonicalizerImpl);
  // function members
  int push_not(ObRawExpr *&expr);
  int do_push_not(ObRawExpr *&expr);
  int remove_duplicate_conds(ObRawExpr *&expr);
  int pull_parallel_expr(ObRawExpr *&expr);
  int pull_and_factor(ObRawExpr *&expr);
  int pull_similar_expr(ObRawExpr *&expr);
  int cluster_and_or(ObRawExpr *&expr);
private:
  struct ObOppositeOpPair
  {
    ObItemType  original_;
    ObItemType  opposite_;
  };
  static ObItemType get_opposite_op(ObItemType type);
  static ObOppositeOpPair OPPOSITE_PAIRS[];
private:
  // data members
  ObExprResolveContext &ctx_;
//  ObIAllocator &allocator_;

};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_CANONICALIZER_IMPL_H */

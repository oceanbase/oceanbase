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

#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int RowDesc::init()
{
  static const int64_t BUCKET_SIZE = 256;
  return expr_idx_map_.create(BUCKET_SIZE, ObModIds::OB_SQL_CG, ObModIds::OB_SQL_CG);
}

void RowDesc::reset()
{
  expr_idx_map_.clear();
  exprs_.reset();
}

int RowDesc::assign(const RowDesc &other)
{
  reset();
  return append(other);
}

int RowDesc::append(const RowDesc &other)
{
  int ret = OB_SUCCESS;
  int64_t N = other.get_column_num();
  ObRawExpr *raw_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(other.get_column(i, raw_expr))) {
      SQL_CG_LOG(WARN, "failed to get column", K(ret), K(i), K(other.get_column_num()));
    } else if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_CG_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_FAIL(add_column(raw_expr))) {
      SQL_CG_LOG(WARN, "failed to add column", K(ret), K(*raw_expr));
    }
  } // end for
  return ret;
}

int RowDesc::add_column(ObRawExpr *raw_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_CG_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(exprs_.push_back(raw_expr))) {
    SQL_CG_LOG(WARN, "failed to add raw_expr", K(ret), K(*raw_expr));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    ret = expr_idx_map_.get_refactored(reinterpret_cast<int64_t>(
                                       static_cast<jit::expr::ObIRawExpr *>(raw_expr)), idx);
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(expr_idx_map_.set_refactored(reinterpret_cast<int64_t>(
                                                      static_cast<jit::expr::ObIRawExpr *>(raw_expr)),
                                                                 exprs_.count() - 1))) {
        SQL_CG_LOG(WARN, "failed to set", K(ret), K(*raw_expr));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(ret)) {
      SQL_CG_LOG(WARN, "failed to get hashmap", K(ret));
    }
  }
  if (OB_SUCC(ret) && !raw_expr->has_flag(IS_COLUMNLIZED)
      && OB_FAIL(raw_expr->add_flag(IS_COLUMNLIZED))) {
    SQL_CG_LOG(WARN, "failed to add flag", K(ret));
  }
  return ret;
}

int RowDesc::replace_column(ObRawExpr *old_expr, ObRawExpr *new_expr)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_ISNULL(new_expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_CG_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(get_idx(old_expr, idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_HASH_NOT_EXIST;
    }
    SQL_CG_LOG(WARN, "get expr index failed", K(ret), KPC(old_expr), K_(exprs));
  } else if (OB_FAIL(expr_idx_map_.erase_refactored(reinterpret_cast<int64_t>(static_cast<jit::expr::ObIRawExpr *>(old_expr))))) {
    SQL_CG_LOG(WARN, "erase old expr failed", K(ret), KPC(old_expr), K(idx));
  } else if (OB_FAIL(expr_idx_map_.set_refactored(reinterpret_cast<int64_t>(static_cast<jit::expr::ObIRawExpr *>(new_expr)), idx))) {
    SQL_CG_LOG(WARN, "set expr idx failed", K(ret), KPC(new_expr), K(idx));
  } else {
    exprs_.at(idx) = new_expr;
  }
  if (OB_SUCC(ret) && !new_expr->has_flag(IS_COLUMNLIZED)
      && OB_FAIL(new_expr->add_flag(IS_COLUMNLIZED))) {
    SQL_CG_LOG(WARN, "failed to add flag", K(ret));
  }
  return ret;
}

int RowDesc::swap_position(const ObRawExpr *expr1, const ObRawExpr *expr2)
{
  int ret = OB_SUCCESS;
  int64_t idx1 = OB_INVALID_INDEX;
  int64_t idx2 = OB_INVALID_INDEX;
  if (OB_ISNULL(expr1) || OB_ISNULL(expr2)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_CG_LOG(WARN, "expr is invalid", K(expr1), K(expr2), K(ret));
  } else if (OB_FAIL(get_idx(expr1, idx1))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_HASH_NOT_EXIST;
    }
    SQL_CG_LOG(WARN, "get expr index failed", K(ret), KPC(expr1), K_(exprs));
  } else if (OB_FAIL(get_idx(expr2, idx2))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_HASH_NOT_EXIST;
    }
    SQL_CG_LOG(WARN, "get expr index failed", K(ret), KPC(expr2), K_(exprs));
  } else if (OB_FAIL(expr_idx_map_.set_refactored(reinterpret_cast<int64_t>(
      static_cast<const jit::expr::ObIRawExpr *>(expr1)), idx2, 1/*overwrite value*/))) {
    SQL_CG_LOG(WARN, "overwrite expr1 index failed", K(ret), KPC(expr1), K(idx2));
  } else if (OB_FAIL(expr_idx_map_.set_refactored(reinterpret_cast<int64_t>(
      static_cast<const jit::expr::ObIRawExpr *>(expr2)), idx1, 1/*overwrite value*/))) {
    SQL_CG_LOG(WARN, "overwrite expr2 index failed", K(ret), KPC(expr2), K(idx1));
  } else {
    exprs_.at(idx1) = const_cast<ObRawExpr*>(expr2);
    exprs_.at(idx2) = const_cast<ObRawExpr*>(expr1);
  }
  return ret;
}

int64_t RowDesc::get_column_num() const
{
  return exprs_.count();
}

ObRawExpr* RowDesc::get_column(int64_t idx) const
{
  ObRawExpr *ret = NULL;
  if (0 <= idx && idx < exprs_.count()) {
    ret = reinterpret_cast<ObRawExpr*>(exprs_.at(idx));
  }
  return ret;
}

int RowDesc::get_column(int64_t idx, ObRawExpr *&raw_expr) const
{
  int ret = OB_SUCCESS;
  if (0 <= idx && idx < exprs_.count()) {
    raw_expr = reinterpret_cast<ObRawExpr*>(exprs_.at(idx));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int RowDesc::get_idx(const jit::expr::ObIRawExpr *raw_expr, int64_t &idx) const
{
  idx = OB_INVALID_INDEX;
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_idx_map_.get_refactored(reinterpret_cast<int64_t>(
      static_cast<const jit::expr::ObIRawExpr *>(raw_expr)), idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

const ObIArray<ObRawExpr*> &RowDesc::get_columns() const
{
  return exprs_;
}

}//end of namespace sql
}//end of namespace oceanbase

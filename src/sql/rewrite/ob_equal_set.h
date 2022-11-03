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

#ifndef _OB_EQUAL_SET_H
#define _OB_EQUAL_SET_H
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "lib/hash/ob_iteratable_hashmap.h"
namespace oceanbase
{
namespace sql
{
// Set <{<c1>, <c2>, <c3>}, {1, 2}> means c1 = c2 and c2 = c3 and c3 in (1, 2)
template <typename COLUMN_INFO = const void *, typename CONST_INFO = const void *>
class ObEqualSet: public common::ObDLinkBase<ObEqualSet<COLUMN_INFO, CONST_INFO> >
{
  typedef typename common::hash::ObIteratableHashMap<int64_t, COLUMN_INFO>::const_iterator_t ColIter;
public:
  struct ColumnIterator
  {
  public:
    ColumnIterator();
    ColumnIterator(const ObEqualSet *equal_set, const ColIter &col_iter);
    ~ColumnIterator();
    ColumnIterator(const ColumnIterator &other);
    ColumnIterator &operator=(const ColumnIterator &other);

    bool operator==(const ColumnIterator &other) const;
    bool operator!=(const ColumnIterator &other) const;

    ColumnIterator &operator++();
    bool get_flag() const;
    int64_t get_expr_idx() const;
    const COLUMN_INFO &get_expr_info() const;
  private:
    const ObEqualSet *equal_set_;
    ColIter col_iter_;
  };
public:
  ObEqualSet();
  virtual ~ObEqualSet();

  int add_expr(int64_t expr_idx, bool flag, const COLUMN_INFO &payload);
  int set_flag(int64_t expr_idx, bool flag);
  bool has_expr(int64_t expr_idx) const;
  const COLUMN_INFO *get_expr(int64_t expr_idx) const;

  int add_const(const CONST_INFO &obj);
  bool has_const() const;

//  void reset();

  int64_t get_column_num() const;
  ColumnIterator column_begin() const
  {
    return ColumnIterator(this, exprs_.begin());
  }
  ColumnIterator column_end() const
  {
    return ColumnIterator(this, exprs_.end());
  }

  int64_t get_const_num() const;
  int get_const(int64_t idx, CONST_INFO &val) const;

  bool includes_equal_set(const ObEqualSet &other) const
  {
    return other.expr_bitmap_.is_subset(expr_bitmap_);
  }
  bool intersect_equal_set(const ObEqualSet &other) const;
  common::ObIArray<CONST_INFO> &get_constants()
  {
    return constants_;
  }
  TO_STRING_KV(N_COLUMN, exprs_,
               N_CONST, constants_);
private:
  // types and constants
  static const int64_t COMMON_CONST_NUM = 64;
  typedef common::ObSEArray<CONST_INFO, COMMON_CONST_NUM, common::ModulePageAllocator, true> Constants;
  typedef common::hash::ObIteratableHashMap<int64_t, COLUMN_INFO> Expressions;
private:
  // disallow copy
  ObEqualSet(const ObEqualSet &other);
  ObEqualSet &operator=(const ObEqualSet &other);
  // function members
private:
  // data members
  Expressions exprs_;
  common::ObBitSet<common::OB_ROW_MAX_COLUMNS_COUNT> flags_; // column flag
  common::ObBitSet<common::OB_ROW_MAX_COLUMNS_COUNT> expr_bitmap_;
  Constants constants_;   // const values information
};
} // end namespace sql
} // end namespace oceanbase

#include "ob_equal_set.ipp"
#endif /* _OB_EQUAL_SET_H */

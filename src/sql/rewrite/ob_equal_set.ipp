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

namespace oceanbase
{
namespace sql
{
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::ObEqualSet()
{
}
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::~ObEqualSet()
{
}

template<typename COLUMN_INFO, typename CONST_INFO>
int ObEqualSet<COLUMN_INFO, CONST_INFO>::add_expr(int64_t expr_idx, bool flag, const COLUMN_INFO &payload)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(expr_idx < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "expr_idx is invalid", K(expr_idx));
  } else if (OB_FAIL(exprs_.set_refactored(expr_idx, payload))) {
    SQL_REWRITE_LOG(WARN, "set exprs failed", K(ret));
  } else if (OB_FAIL(expr_bitmap_.add_member(static_cast<int32_t>(expr_idx)))) {
    SQL_REWRITE_LOG(WARN, "add member failed", K(ret));
  } else {
    if (flag) {
      if (OB_FAIL(flags_.add_member(static_cast<int32_t>(expr_idx)))) {
        SQL_REWRITE_LOG(WARN, "add member failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::intersect_equal_set(const ObEqualSet &other) const
{
  return expr_bitmap_.overlap(other.expr_bitmap_);
}

template <typename COLUMN_INFO, typename CONST_INFO>
int ObEqualSet<COLUMN_INFO, CONST_INFO>::set_flag(int64_t expr_idx, bool flag)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(expr_idx < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "expr_idx is invalid", K(expr_idx));
  } else {
    if (flag) {
      if (OB_FAIL(flags_.add_member(static_cast<int32_t>(expr_idx)))) {
        SQL_REWRITE_LOG(WARN, "add member failed", K(ret));
      }
    } else {
      if (OB_FAIL(flags_.del_member(static_cast<int32_t>(expr_idx)))) {
        SQL_REWRITE_LOG(WARN, "del member failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::has_expr(int64_t expr_idx) const
{
  return NULL != exprs_.get(expr_idx);
}

template <typename COLUMN_INFO, typename CONST_INFO>
const COLUMN_INFO *ObEqualSet<COLUMN_INFO, CONST_INFO>::get_expr(int64_t expr_idx) const
{
  return exprs_.get(expr_idx);
}

template <typename COLUMN_INFO, typename CONST_INFO>
int ObEqualSet<COLUMN_INFO, CONST_INFO>::add_const(const CONST_INFO &obj)
{
  return constants_.push_back(obj);
}
template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::has_const() const
{
  return constants_.count() > 0;
}

//template <typename COLUMN_INFO, typename CONST_INFO>
//void ObEqualSet<COLUMN_INFO, CONST_INFO>::reset()
//{
//  exprs_.reuse();
//  flags_.reset();
//  constants_.reset();
//  expr_bitmap_.reset();
//}
template <typename COLUMN_INFO, typename CONST_INFO>
int64_t ObEqualSet<COLUMN_INFO, CONST_INFO>::get_column_num() const
{
  return exprs_.count();
}
template <typename COLUMN_INFO, typename CONST_INFO>
int64_t ObEqualSet<COLUMN_INFO, CONST_INFO>::get_const_num() const
{
  return constants_.count();
}
template <typename COLUMN_INFO, typename CONST_INFO>
int ObEqualSet<COLUMN_INFO, CONST_INFO>::get_const(int64_t idx, CONST_INFO &val) const
{
  return constants_.at(idx, val);
}
////////////////////////////////////////////////////////////////
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::ColumnIterator()
    : equal_set_(NULL),
      col_iter_()
{
}
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::ColumnIterator(const ObEqualSet *equal_set,
                                                                    const ObEqualSet<COLUMN_INFO, CONST_INFO>::ColIter &col_iter)
    : equal_set_(equal_set),
      col_iter_(col_iter)
{
}
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::~ColumnIterator()
{
}
template <typename COLUMN_INFO, typename CONST_INFO>
ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::ColumnIterator(const ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator &other)
    : equal_set_(other.equal_set_),
      col_iter_(other.col_iter_)
{
}
template <typename COLUMN_INFO, typename CONST_INFO>
typename ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator
&ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::operator=(const ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator &other)
{
  if (this != &other) {
    equal_set_ = other.equal_set_;
    col_iter_ = other.col_iter_;
  }
  return *this;
}
template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::operator==(const ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator &other) const
{
  return equal_set_ == other.equal_set_ && col_iter_ == other.col_iter_;
}
template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::operator!=(const ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator &other) const
{
  return equal_set_ != other.equal_set_ || col_iter_ != other.col_iter_;
}
template <typename COLUMN_INFO, typename CONST_INFO>
typename ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator &ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::operator++()
{
  ++col_iter_;
  return *this;
}
template <typename COLUMN_INFO, typename CONST_INFO>
bool ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::get_flag() const
{
  return equal_set_->flags_.has_member(static_cast<int32_t>((*col_iter_).first));
}
template <typename COLUMN_INFO, typename CONST_INFO>
int64_t ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::get_expr_idx() const
{
  return (*col_iter_).first;
}
template <typename COLUMN_INFO, typename CONST_INFO>
const COLUMN_INFO &ObEqualSet<COLUMN_INFO, CONST_INFO>::ColumnIterator::get_expr_info() const
{
  return (*col_iter_).second;
}
} // end namespace sql
} // end namespace oceanbase

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
#pragma once

#include "storage/direct_load/ob_direct_load_data_block.h"

namespace oceanbase
{
namespace storage
{

template <typename SSTable>
class ObDirectLoadSSTableIndexBlockIterator
{
  typedef ObDirectLoadSSTableIndexBlockIterator<SSTable> self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef int64_t difference_type;
  typedef self_t value_type;
  typedef self_t *value_ptr_t;
  typedef self_t *pointer;
  typedef self_t &reference;
public:
  ObDirectLoadSSTableIndexBlockIterator() : sstable_(nullptr), fragment_idx_(0), index_block_idx_(0)
  {
  }
  ObDirectLoadSSTableIndexBlockIterator(const self_t &other) { *this = other; }
  ~ObDirectLoadSSTableIndexBlockIterator() {}
  self_t &operator=(const self_t &other)
  {
    sstable_ = other.sstable_;
    fragment_idx_ = other.fragment_idx_;
    index_block_idx_ = other.index_block_idx_;
    return *this;
  }
public:
  reference operator*() { return *this; }
  value_ptr_t operator->() { return this; }
  operator value_ptr_t() { return this; }
  bool operator==(const self_t &other) const
  {
    abort_unless(sstable_ == other.sstable_);
    return fragment_idx_ == other.fragment_idx_ && index_block_idx_ == other.index_block_idx_;
  }
  bool operator!=(const self_t &other) const { return !(*this == other); }
  bool operator<(const self_t &other) const
  {
    abort_unless(sstable_ == other.sstable_);
    return (fragment_idx_ != other.fragment_idx_ ? fragment_idx_ < other.fragment_idx_
                                                 : index_block_idx_ < other.index_block_idx_);
  }
  bool operator>(const self_t &other) const
  {
    abort_unless(sstable_ == other.sstable_);
    return (fragment_idx_ != other.fragment_idx_ ? fragment_idx_ > other.fragment_idx_
                                                 : index_block_idx_ > other.index_block_idx_);
  }
  bool operator>=(const self_t &other) const { return !(*this < other); }
  bool operator<=(const self_t &other) const { return !(*this > other); }
  difference_type operator-(const self_t &rhs) const
  {
    abort_unless(sstable_ == rhs.sstable_);
    difference_type value = 0;
    if (fragment_idx_ == rhs.fragment_idx_) {
      value = index_block_idx_ - rhs.index_block_idx_;
    } else if (fragment_idx_ > rhs.fragment_idx_) { // lhs > rhs
      int64_t fragment_idx = fragment_idx_ - 1;
      value += index_block_idx_;
      abort_unless(fragment_idx < sstable_->get_fragments().count());
      while (fragment_idx > rhs.fragment_idx_) {
        abort_unless(fragment_idx >= 0);
        const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx);
        value += fragment.index_block_count_;
        --fragment_idx;
      }
      abort_unless(fragment_idx >= 0);
      const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx);
      value += (fragment.index_block_count_ - rhs.index_block_idx_);
    } else { // lhs < rhs
      abort_unless(fragment_idx_ >= 0 && fragment_idx_ < sstable_->get_fragments().count());
      const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx_);
      value -= (fragment.index_block_count_ - index_block_idx_);
      int64_t fragment_idx = fragment_idx_ + 1;
      while (fragment_idx < rhs.fragment_idx_) {
        abort_unless(fragment_idx < sstable_->get_fragments().count());
        const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx);
        value -= fragment.index_block_count_;
        ++fragment_idx;
      }
      value -= rhs.index_block_idx_;
    }
    return value;
  }
  self_t &operator+=(difference_type step)
  {
    while (step > 0) {
      abort_unless(fragment_idx_ < sstable_->get_fragments().count());
      const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx_);
      if (index_block_idx_ + step < fragment.index_block_count_) {
        index_block_idx_ += step;
        step = 0;
      } else {
        step -= (fragment.index_block_count_ - index_block_idx_);
        ++fragment_idx_;
        index_block_idx_ = 0;
      }
    }
    return *this;
  }
  self_t operator+(difference_type step)
  {
    self_t value(*this);
    return (value += step);
  }
  self_t &operator-=(difference_type step)
  {
    while (step > 0) {
      if (index_block_idx_ == 0) {
        // prev fragment
        fragment_idx_ -= 1;
        abort_unless(fragment_idx_ >= 0);
        const typename SSTable::Fragment &fragment = sstable_->get_fragments().at(fragment_idx_);
        index_block_idx_ = fragment.index_block_count_;
      } else if (index_block_idx_ >= step) {
        index_block_idx_ -= step;
        step = 0;
      } else {
        step -= index_block_idx_;
        index_block_idx_ = 0;
      }
    }
    return *this;
  }
  self_t operator-(difference_type step)
  {
    self_t value(*this);
    return (value -= step);
  }
  self_t &operator++()
  {
    *this += 1;
    return *this;
  }
  self_t operator++(int)
  {
    self_t tmp = *this;
    *this += 1;
    return tmp;
  }
  self_t &operator--()
  {
    *this -= 1;
    return *this;
  }
  self_t operator--(int)
  {
    self_t tmp = *this;
    *this -= 1;
    return tmp;
  }
  TO_STRING_KV(KP_(sstable), K_(fragment_idx), K_(index_block_idx));
public:
  SSTable *sstable_;
  int64_t fragment_idx_;
  int64_t index_block_idx_;
};

} // namespace storage
} // namespace oceanbase

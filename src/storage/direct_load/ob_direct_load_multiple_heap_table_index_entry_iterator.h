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

#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"

namespace oceanbase
{
namespace storage
{

template <typename Table>
class ObDirectLoadMultipleHeapTableIndexEntryIterator
{
  typedef ObDirectLoadMultipleHeapTableIndexEntryIterator<Table> self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef int64_t difference_type;
  typedef self_t value_type;
  typedef self_t *value_ptr_t;
  typedef self_t *pointer;
  typedef self_t &reference;
public:
  ObDirectLoadMultipleHeapTableIndexEntryIterator() : table_(nullptr), index_entry_idx_(0) {}
  ObDirectLoadMultipleHeapTableIndexEntryIterator(const self_t &other) { *this = other; }
  ~ObDirectLoadMultipleHeapTableIndexEntryIterator() {}
  self_t &operator=(const self_t &other)
  {
    table_ = other.table_;
    index_entry_idx_ = other.index_entry_idx_;
    return *this;
  }
public:
  reference operator*() { return *this; }
  value_ptr_t operator->() { return this; }
  operator value_ptr_t() { return this; }
  bool operator==(const self_t &other) const
  {
    abort_unless(table_ == other.table_);
    return index_entry_idx_ == other.index_entry_idx_;
  }
  bool operator!=(const self_t &other) const { return !(*this == other); }
  bool operator<(const self_t &other) const
  {
    abort_unless(table_ == other.table_);
    return index_entry_idx_ < other.index_entry_idx_;
  }
  bool operator>=(const self_t &other) const { return !(*this < other); }
  bool operator>(const self_t &other) const
  {
    abort_unless(table_ == other.table_);
    return index_entry_idx_ > other.index_entry_idx_;
  }
  bool operator<=(const self_t &other) const { return !(*this > other); }
  difference_type operator-(const self_t &rhs) const
  {
    abort_unless(table_ == rhs.table_);
    return index_entry_idx_ - rhs.index_entry_idx_;
  }
  self_t &operator+=(difference_type step)
  {
    abort_unless(index_entry_idx_ + step <= table_->get_meta().index_entry_count_);
    index_entry_idx_ += step;
    return *this;
  }
  self_t operator+(difference_type step) const
  {
    self_t value(*this);
    return (value += step);
  }
  self_t &operator-=(difference_type step)
  {
    abort_unless(index_entry_idx_ - step >= 0);
    index_entry_idx_ -= step;
    return *this;
  }
  self_t operator-(difference_type step) const
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
  TO_STRING_KV(KP_(table), K_(index_entry_idx));
public:
  Table *table_;
  int64_t index_entry_idx_;
};

} // namespace storage
} // namespace oceanbase

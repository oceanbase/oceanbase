/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Abstract SortedList
 */

#ifndef OCEANBSE_OBCDC_SORTED_LIST_
#define OCEANBSE_OBCDC_SORTED_LIST_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace libobcdc
{

template<typename T>
struct DefaultValComparator
{
  static int compare(const T &a, const T &b) { return a.compare(b); }
};

template<typename T, typename CompareFunc>
class ListNode
{
public:
  typedef ListNode<T, CompareFunc> ListNodeType;
  ListNode() : val_(nullptr), next_(nullptr) {}
  ListNode(T &val) : val_(&val), next_(nullptr) {}
  virtual ~ListNode()
  {
    val_ = nullptr;
    next_ = nullptr;
  }

public:
  virtual int compare(const ListNodeType &other) const
  { OB_ASSERT(nullptr != val_); return CompareFunc::compare(*val_, *other.val_); }
  bool operator<(const ListNodeType &other) { return compare(other) < 0; }
  bool operator==(const ListNodeType &other) { return compare(other) == 0; }
  bool operator>(const ListNodeType &other) { return compare(other) > 0; }
  bool operator!=(const ListNodeType &other) { return compare(other) != 0; }
  int compare(const T &other) const
  { OB_ASSERT(nullptr != val_); return CompareFunc::compare(*val_, other); }
  bool operator<(const T &other) { return compare(other) < 0; }
  bool operator==(const T &other) { return compare(other) == 0; }
  bool operator>(const T &other) { return compare(other) > 0; }
public:
  OB_INLINE T* get_val() { return val_; }

  OB_INLINE void set_next(ListNodeType *next_node) { next_ = next_node; }
  OB_INLINE ListNodeType *get_next() const {return next_;}
  VIRTUAL_TO_STRING_KV(KPC_(val));
  DISABLE_COPY_ASSIGN(ListNode);
protected:
  T *val_;
  ListNodeType *next_;
};

template <typename T, typename CompareFunc = DefaultValComparator<T>>
class SortedListIterator;

// interface for sorted list
template <typename T, typename CompareFunc = DefaultValComparator<T>>
class SortedList
{
  friend class SortedListIterator<T, CompareFunc>;
  typedef ListNode<T, CompareFunc> ListNodeType;
public:
  SortedList() : count_(0) {}
  virtual ~SortedList() { reset(); }
  virtual void reset() { ATOMIC_SET(&count_, 0); };
  virtual int push(T &val) = 0;
  virtual int pop(T *&val) = 0;
  virtual bool contains(const T &val) = 0;
public:
  typedef SortedListIterator<T, CompareFunc> Iterator;

public:
  Iterator begin() const;
  Iterator end() const;
  T *top() const
  {
    T *val = nullptr;
    ListNodeType *first_node = get_first_node();
    if (OB_NOT_NULL(first_node)) {
      val = first_node->get_val();
    }
    return val;
  }
  int count() const { return ATOMIC_LOAD(&count_); }
  bool empty() const { return 0 == count(); }
  VIRTUAL_TO_STRING_KV(K_(count));
protected:
  virtual ListNodeType *get_first_node() const = 0;
  virtual int get_next_node(const ListNodeType &node, ListNodeType *&next_node) const = 0;
  void inc_node_num_() { ATOMIC_INC(&count_); }
  void dec_node_num_() { ATOMIC_DEC(&count_); }
private:
  int64_t count_;
};

} // end of namespace libobcdc
} // end of namespace oceanbase

#endif
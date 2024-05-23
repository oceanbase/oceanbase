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
 * Iterator of SortedList
 */

#ifndef OCEANBSE_OBCDC_SORTED_LIST_ITERATOR_
#define OCEANBSE_OBCDC_SORTED_LIST_ITERATOR_

#include "ob_cdc_sorted_list.h"

namespace oceanbase
{
namespace libobcdc
{
template <typename T, typename CompareFunc>
typename SortedList<T, CompareFunc>::Iterator SortedList<T, CompareFunc>::begin() const
{
  return Iterator(this, get_first_node());
}

template <typename T, typename CompareFunc>
typename SortedList<T, CompareFunc>::Iterator SortedList<T, CompareFunc>::end() const
{
  return Iterator(this, 0);
}

template <typename T, typename CompareFunc>
class SortedListIterator
{
  typedef ListNode<T, CompareFunc> ListNodeType;
public:
  // using standard iterator feature
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = T;
  using pointer = T*;
  using reference = T&;
  SortedListIterator(const SortedList<T, CompareFunc> *list, ListNodeType *node_ptr) : list_(list), node_ptr_(node_ptr) {}
  virtual ~SortedListIterator() { reset(); }
  bool is_valid() const { return nullptr != list_ && nullptr != node_ptr_; }
  void reset() { list_ = nullptr; node_ptr_ = nullptr; }

  // dereference operation
  reference operator*() const {
    ob_assert(nullptr != list_);
    ob_assert(nullptr != node_ptr_);
    ob_assert(nullptr != node_ptr_->get_val());
    return *(node_ptr_->get_val());
  }

  // member pointer access
  pointer operator->() const {
    ob_assert(nullptr != list_);
    ob_assert(nullptr != node_ptr_);
    return node_ptr_->get_val(); // return pointer to data pointer
  }

  // ++iter
  SortedListIterator& operator++() {
    ob_assert(nullptr != list_);
    if (node_ptr_) {
      int ret = OB_SUCCESS;
      ListNodeType *next = nullptr;
      if (OB_FAIL(list_->get_next_node(*node_ptr_, next))) {
        OBLOG_LOG(ERROR, "get_next_node failed", KR(ret), KPC_(list));
      } else {
        node_ptr_ = next;
      }
    }
    return *this;
  }

  // iter++
  SortedListIterator operator++(int) {
    ob_assert(nullptr != list_);
    SortedListIterator tmp = *this;
    ++(*this);
    return tmp;
  }
  // NOTICE: not support operator--

  bool operator==(const SortedListIterator& other) const {
    return node_ptr_ == other.node_ptr_;
  }

  bool operator!=(const SortedListIterator& other) const {
    return node_ptr_ != other.node_ptr_;
  }
  TO_STRING_KV(KPC_(list), KPC_(node_ptr));

private:
  const SortedList<T, CompareFunc>* list_;
  ListNode<T, CompareFunc>* node_ptr_;
};

} // end namespace libobcdc
} // end namespace oceanbase


#endif
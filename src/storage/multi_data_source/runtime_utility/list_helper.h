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
 */

#ifndef SRC_STORAGE_MULTI_DATA_SOURCE_UTILITY_LIST_HELPER_H
#define SRC_STORAGE_MULTI_DATA_SOURCE_UTILITY_LIST_HELPER_H

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "common_define.h"
#include "lib/utility/utility.h"
#include "share/ob_errno.h"
#include "src/share/ob_delegate.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"
#include "common_define.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename T>
class List;

struct ListNodeBase// erase ListNode type
{
  ListNodeBase() : prev_(nullptr), next_(nullptr) {}
  virtual ~ListNodeBase() { reset(); }
  ListNodeBase(const ListNodeBase &) = delete;
  ListNodeBase(ListNodeBase &&) = delete;
  ListNodeBase &operator=(const ListNodeBase &) = delete;
  ListNodeBase &operator=(ListNodeBase &&) = delete;
  void reset() { prev_= nullptr; next_ = nullptr; }
  virtual int64_t to_string(char *, const int64_t) const = 0;
  ListNodeBase *prev_;
  ListNodeBase *next_;
};

template <typename T>
struct ListNode : public ListNodeBase
{
  ListNode() : ListNodeBase() {}
  virtual ~ListNode() { reset(); }
  ListNode<T> *prev() { return static_cast<ListNode<T> *>(prev_); }
  ListNode<T> *next() { return static_cast<ListNode<T> *>(next_); }
};

template <typename T>
struct CompareOperationWrapper
{
  CompareOperationWrapper(T &data) : data_(data) {}
  bool operator<(const CompareOperationWrapper<T> &rhs) const { return data_ < rhs.data_; }
  bool operator>(const CompareOperationWrapper<T> &rhs) const { return !(data_ < rhs.data_) && !(data_ == rhs.data_); }
  bool operator==(const CompareOperationWrapper<T> &rhs) const { return data_ == rhs.data_; }
  bool operator>=(const CompareOperationWrapper<T> &rhs) const { return data_ > rhs.data_ || data_ == rhs.data_;}
  bool operator<=(const CompareOperationWrapper<T> &rhs) const { return data_ < rhs.data_ || data_ == rhs.data_;}
  T &data_;
};

struct ListBase// erase List Type
{
  ListBase() : list_head_(nullptr), list_tail_(nullptr) {}
  virtual ~ListBase() { clear(); }
  void check_invariance_() const {
#ifdef UNITTEST_DEBUG
    if (OB_NOT_NULL(list_head_)) {
      MDS_ASSERT(OB_NOT_NULL(list_tail_));
      MDS_ASSERT(OB_ISNULL(list_head_->prev_));
      MDS_ASSERT(OB_ISNULL(list_tail_->next_));
      // can reach tail from head
      const ListNodeBase *iter = list_head_;
      while (iter != list_tail_) {
        iter = iter->next_;
      }
      MDS_ASSERT(iter == list_tail_);
    }
    if (OB_NOT_NULL(list_tail_)) {
      MDS_ASSERT(OB_NOT_NULL(list_tail_));
      MDS_ASSERT(OB_ISNULL(list_head_->prev_));
      MDS_ASSERT(OB_ISNULL(list_tail_->next_));
      // can reach head from tail
      const ListNodeBase *iter = list_tail_;
      while (iter != list_head_) {
        iter = iter->prev_;
      }
      MDS_ASSERT(iter == list_head_);
    }
#endif
  }
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    ListNodeBase *iter = list_head_;
    common::databuff_printf(buf, buf_len, pos, "list_head:0x%llx", (unsigned long long)list_head_);
    common::databuff_printf(buf, buf_len, pos, ", list_tail:0x%llx, list:", (unsigned long long)list_tail_);
    int64_t list_len = 0;
    while (OB_NOT_NULL(iter)) {
      if (list_len < MAX_PRINT_COUNT) {
        common::databuff_printf(buf, buf_len, pos, "[%s]<->", to_cstring(*iter));
      } else if(list_len == MAX_PRINT_COUNT) {
        common::databuff_printf(buf, buf_len, pos, "...");
      }
      ++list_len;
      iter = iter->next_;
    }
    common::databuff_printf(buf, buf_len, pos, "NULL, total_size:%ld", list_len);
    return pos;
  }
  ListBase(const ListBase &) = default;
  ListBase(ListBase &&) = default;
  ListBase &operator=(const ListBase &) = default;
  ListBase &operator=(ListBase &&) = default;
  void clear() { list_head_ = nullptr; list_tail_ = nullptr; }
  bool empty() const { return nullptr == list_head_; }
  void append(ListNodeBase *new_node) {
    new_node->reset();
    if (OB_ISNULL(list_head_)) {// insert into head
      MDS_ASSERT(OB_ISNULL(list_tail_));
      list_head_ = new_node;
      list_tail_ = new_node;
    } else {// insert after tail
      MDS_ASSERT(OB_ISNULL(list_tail_->next_));
      MDS_ASSERT(OB_NOT_NULL(new_node));
      MDS_ASSERT(OB_ISNULL(new_node->prev_));
      MDS_ASSERT(OB_ISNULL(new_node->next_));
      list_tail_->next_ = new_node;
      new_node->prev_ = list_tail_;
      new_node->next_ = nullptr;
      list_tail_ = new_node;
    }
    check_invariance_();
  }
  void del(ListNodeBase *list_node) {
    MDS_ASSERT(OB_NOT_NULL(list_node));
    ListNodeBase *before = list_node->prev_;
    ListNodeBase *after = list_node->next_;
    if (OB_ISNULL(before)) {// the first node
      MDS_ASSERT(list_head_ == list_node);
      list_head_ = after;
      if (OB_NOT_NULL(list_head_)) {
        list_head_->prev_ = nullptr;
      }
    } else {
      before->next_ = list_node->next_;
    }
    if (OB_ISNULL(after)) {// the last node
      MDS_ASSERT(list_tail_ == list_node);
      list_tail_ = before;
      if (OB_NOT_NULL(list_tail_)) {
        list_tail_->next_ = nullptr;
      }
    } else {
      after->prev_ = list_node->prev_;
    }
    check_invariance_();
    list_node->reset();
  }
  bool check_node_exist(ListNodeBase *list_node) {
    const ListNodeBase *iter = list_head_;
    while (OB_NOT_NULL(iter) &&
           iter != list_node &&
           iter != list_tail_) {
      iter = iter->next_;
    }
    return iter == list_node;
  }
public:
  ListNodeBase *list_head_;
  ListNodeBase *list_tail_;
  static const int64_t MAX_PRINT_COUNT = 16;
};

template <typename T>
class List : public ListBase// the List DO NOT OWN those nodes
{
public:
  List() : ListBase() {}
  virtual ~List() override { clear(); }
  template <typename FUNC>
  void for_each_node_from_head_to_tail_until_true(FUNC &&func) const {
    int ret = OB_SUCCESS;
    const ListNode<T> *iter = static_cast<const ListNode<T> *>(list_head_);
    while (OB_NOT_NULL(iter)) {
      const T *data = static_cast<const T *>(iter);
      if (OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "downcast failed", KP(iter), KP(data), KP(this), KP(list_head_), KP(list_tail_));
      } else {
        const ListNode<T> *temp_iter_next = static_cast<const ListNode<T> *>(iter->next_);
        if (func(*data)) {
          break;
        } else {
          iter = temp_iter_next;
        }
      }
    }
    check_invariance_();
  }
  template <typename FUNC>
  void for_each_node_from_tail_to_head_until_true(FUNC &&func) const {
    int ret = OB_SUCCESS;
    const ListNode<T> *iter = static_cast<const ListNode<T> *>(list_tail_);
    while (OB_NOT_NULL(iter)) {
      const T *data = static_cast<const T *>(iter);
      if (OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "downcast failed", KP(iter), KP(data), KP(this), KP(list_head_), KP(list_tail_));
      } else {
        const ListNode<T> *temp_iter_prev = static_cast<const ListNode<T> *>(iter->prev_);
        if (func(*data)) {
          break;
        } else {
          iter = temp_iter_prev;
        }
      }
    }
    check_invariance_();
  }
  template <typename FUNC>
  int for_each_node(FUNC &&func) const {
    int ret = OB_SUCCESS;
    const ListNode<T> *iter = static_cast<const ListNode<T> *>(list_head_);
    while (OB_SUCC(ret) && OB_NOT_NULL(iter)) {
      const T *data = static_cast<const T *>(iter);
      if (OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "downcast failed", KP(iter), KP(data), KP(this), KP(list_head_), KP(list_tail_));
      } else {
        const ListNode<T> *temp_iter_next = static_cast<const ListNode<T> *>(iter->next_);
        if (OB_FAIL(func(*data))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        } else {
          iter = temp_iter_next;
        }
      }
    }
    check_invariance_();
    return ret;
  }
  template <typename FUNC>
  int reverse_for_each_node(FUNC &&func) const {
    int ret = OB_SUCCESS;
    const ListNode<T> *iter = static_cast<const ListNode<T> *>(list_tail_);
    while (OB_SUCC(ret) && OB_NOT_NULL(iter)) {
      const T *data = static_cast<const T *>(iter);
      if (OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(ERROR, "downcast failed", KP(iter), KP(data), KP(this), KP(list_head_), KP(list_tail_));
      } else {
        const ListNode<T> *temp_iter_prev = static_cast<const ListNode<T> *>(iter->prev_);
        if (OB_FAIL(func(*data))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        } else {
          iter = temp_iter_prev;
        }
      }
    }
    check_invariance_();
    return ret;
  }
  ListNode<T> *fetch_from_head() {
    ListNode<T> *head = static_cast<ListNode<T> *>(list_head_);
    if (OB_NOT_NULL(head)) {
      ListBase::del(head);
    }
    check_invariance_();
    return head;
  }
  void insert_into_head(ListNode<T> *new_node) {
    MDS_ASSERT(OB_NOT_NULL(new_node));
    new_node->reset();
    if (OB_ISNULL(list_head_)) {// insert into head
      MDS_ASSERT(OB_ISNULL(list_tail_));
      list_head_ = new_node;
      list_tail_ = new_node;
    } else {// insert before head
      MDS_ASSERT(OB_ISNULL(list_head_->prev_));
      MDS_ASSERT(OB_ISNULL(new_node->prev_));
      MDS_ASSERT(OB_ISNULL(new_node->next_));
      list_head_->prev_ = new_node;
      new_node->next_ = list_head_;
      new_node->prev_ = nullptr;
      list_head_ = new_node;
    }
    check_invariance_();
  }
};

enum SORT_TYPE
{
  DESC = 0,
  ASC = 1,
};

// if b is false, get reverse of a
static inline bool XNOR(bool a, bool b) {
  return ((a & b) | (!a & !b));
}

// sorted from small to big(from head to tail) by default
// if REVERSE == true, sorted from big to small(from head to tail)
// two elements equal is allowed
template <typename T, int SORT_TYPE>
class SortedList : public List<T>
{
  struct BinaryCompareOp {
    BinaryCompareOp(ListNode<T> *new_node, ListNode<T> *&next_node)
    : ret_(OB_SUCCESS),
    new_node_(new_node),
    next_node_(next_node) {}
    template <typename T2 = T, ENABLE_IF_MDS_SERIALIZEABLE(T2)>// binary comparable
    bool operator()(const T &node) {
      bool ret = false;
      const ListNode<T> &list_node = dynamic_cast<const ListNode<T>&>(node);
      T &rhs_data = dynamic_cast<T&>(*new_node_);
      int compare_result = 0;
      if (OB_SUCCESS != (ret_ = compare_binary_key<T>(node, rhs_data, compare_result))) {
        MDS_LOG(WARN, "fail to compare binary key buffer", K(node), K(rhs_data));
      } else if (XNOR(compare_result > 0, SORT_TYPE)) {
        next_node_ = &const_cast<ListNode<T> &>(list_node);
        ret = true;
      } else {
        ret = false;
      }
      return ret;
    }
    template <typename T2 = T, ENABLE_IF_NOT_MDS_SERIALIZEABLE(T2)>// origin comparable
    bool operator()(const T &node) {
      bool ret = false;
      const ListNode<T> &list_node = dynamic_cast<const ListNode<T>&>(node);
      T &rhs_data = dynamic_cast<T&>(*new_node_);
      if (XNOR(CompareOperationWrapper<const T>(node) > CompareOperationWrapper<const T>(rhs_data), SORT_TYPE)) {
        next_node_ = &const_cast<ListNode<T> &>(list_node);
        ret = true;
      } else {
        ret = false;
      }
      return ret;
    }
    int ret_;
    ListNode<T> *new_node_;
    ListNode<T> *&next_node_;
  };
public:
  SortedList() = default;
  virtual ~SortedList() override { ListBase::clear(); }
  SortedList(const SortedList<T, SORT_TYPE> &) = delete;
  SortedList(SortedList<T, SORT_TYPE> &&) = delete;
  SortedList<T, SORT_TYPE> &operator=(const SortedList<T, SORT_TYPE> &) = delete;
  SortedList<T, SORT_TYPE> &operator=(SortedList<T, SORT_TYPE> &&) = delete;
  int insert(ListNode<T> *new_node) {
    int ret = OB_SUCCESS;
    MDS_ASSERT(OB_NOT_NULL(new_node));
    MDS_ASSERT(OB_ISNULL(new_node->next_));
    MDS_ASSERT(OB_ISNULL(new_node->prev_));
    if (ListBase::empty()) {
      ListBase::list_head_ = new_node;
      ListBase::list_tail_ = new_node;
    } else {
      ListNode<T> *next_node = nullptr;
      BinaryCompareOp op(new_node, next_node);
      List<T>::for_each_node_from_head_to_tail_until_true(op);
      if (OB_FAIL(op.ret_)) {
      } else if (OB_ISNULL(next_node)) {// insert to tail
        ListBase::list_tail_->next_ = new_node;
        new_node->prev_ = ListBase::list_tail_;
        ListBase::list_tail_ = new_node;
      } else if (OB_ISNULL(next_node->prev_)) {// insert to head
        MDS_ASSERT(ListBase::list_head_ == next_node);
        ListBase::list_head_->prev_ = new_node;
        new_node->next_ = ListBase::list_head_;
        ListBase::list_head_ = new_node;
      } else {// insert to middle
        new_node->prev_ = next_node->prev_;
        new_node->prev_->next_ = new_node;
        next_node->prev_ = new_node;
        new_node->next_ = next_node;
      }
    }
    List<T>::check_invariance_();
    return ret;
  }
  T &get_head() {
    T *data = nullptr;
    MDS_ASSERT(OB_NOT_NULL(data = (T*)(ListNode<T> *)(ListBase::list_head_)));
    return *data;
  }
  T &get_tail() {
    T *data = nullptr;
    MDS_ASSERT(OB_NOT_NULL(data = (T*)(ListNode<T> *)(ListBase::list_tail_)));
    return *data;
  }
};

}
}
}
#endif
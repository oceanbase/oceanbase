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
 * SortedLinkList, NOT THREAD SAFE!
 */

#ifndef OCEANBSE_OBCDC_SORTED_LINKED_LIST_
#define OCEANBSE_OBCDC_SORTED_LINKED_LIST_

#include "ob_cdc_sorted_list.h"
#include "ob_cdc_mem_mgr.h"

namespace oceanbase
{
namespace libobcdc
{

// Obj T should:
// 0. not final
// 1. set_next() and get_next() to set next obj to build the list;
// 2. operator== to check if already has same obj in list;
// 3. operator< and operator> to find pos to insert obj into the list;
// 4. should already imply TO_STRING_KV;
template <typename T, typename CompareFunc = DefaultValComparator<T>>
class SortedLinkedList : public SortedList<T, CompareFunc>
{
public:
  typedef ListNode<T, CompareFunc> ListNodeType;
  typedef SortedList<T, CompareFunc> BaseList;
  SortedLinkedList(ObIAllocator &allocator, const bool is_unique = true)
    : BaseList(),
      allocator_(allocator),
      l_head_(),
      l_tail_(nullptr),
      is_unique_(is_unique) {}
  virtual ~SortedLinkedList() { reset(); }

  void reset() override
  {
    reset_data_();
    is_unique_ = false;
  }

public:
  int push(T &val) override
  {
    int ret = OB_SUCCESS;
    ListNodeType *node_for_val = nullptr;

    if (OB_FAIL(alloc_node_(val, node_for_val))) {
      OBLOG_LOG(ERROR, "alloc_node for val failed", KR(ret), K(val));
    } else if (OB_FAIL(push_node(node_for_val))) {
      if (OB_ENTRY_EXIST != ret) {
        OBLOG_LOG(ERROR, "push node into SortedList failed", KR(ret), K(val));
      } else {
        OBLOG_LOG(INFO, "duplicated node with same value in SortedList, push failed", KR(ret), K(val));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(node_for_val)) {
      node_for_val->~ListNodeType();
      allocator_.free(node_for_val);
    }

    return ret;
  }

  int pop(T *&val) override
  {
    int ret = OB_SUCCESS;
    ListNodeType *first_node = pop_front_node();
    if (OB_NOT_NULL(first_node)) {
      val = first_node->get_val();
      first_node->~ListNodeType();
      allocator_.free(first_node);
      first_node = nullptr;
    }
    return ret;
  }

  // check if queue contains element that has the same value with val
  bool contains(const T &val) override
  {
    bool b_ret = false;
    ListNodeType *tmp = nullptr;

    if (!is_list_empty()) {
      if ((*l_tail_) < val) {
        // quick-path: val is lager than last ListNode: list doesn't contains val
        b_ret = false;
      } else if ((*l_tail_) == val || (*get_first_node()) == val) {
        // quick-path: val is equals to first/last ListNode: list contains val
        b_ret = true;
      } else {
        find_pos_(val, tmp, b_ret);
      }
    }

    return b_ret;
  }

  ListNodeType *get_first_node() const override { return l_head_.get_next(); }

  int get_next_node(const ListNodeType &node, ListNodeType *&next_node) const override
  {
    next_node = node.get_next();
    return OB_SUCCESS;
  }

public:
  bool is_list_empty() const { return nullptr == l_head_.get_next(); }

  // push ListNode(type of T*) into queue
  // can't push the same data into list multi times !!!
  // retval OB_SUCCESS:               push success
  // retval OB_INVALID_ARGUMENT       ListNode is nullptr
  // retval OB_ENTRY_EXIST            elements if queue is required unique(is_unique = true) and ListNode is already in queue
  int push_node(ListNodeType *node_for_val)
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(node_for_val)) {
      ret = OB_INVALID_ARGUMENT;
      OBLOG_LOG(ERROR, "invalid node to push into SortedList", KR(ret));
    } else if (OB_ISNULL(l_tail_)) {
      // empty list
      ob_assert(is_list_empty());
      l_head_.set_next(node_for_val);
      l_tail_ = node_for_val;
      BaseList::inc_node_num_();
    } else if (*l_tail_ < *node_for_val) {
      // insert at tail
      l_tail_->set_next(node_for_val);
      l_tail_ = node_for_val;
      BaseList::inc_node_num_();
    } else {
      // insert between dummy and l_tail_
      ListNodeType *prev_node = nullptr;
      bool has_duplicate_node = false;
      find_pos_(*(node_for_val->get_val()), prev_node, has_duplicate_node);
      if (has_duplicate_node && is_unique_) {
        ret = OB_ENTRY_EXIST;
        OBLOG_LOG(INFO, "found duplicatee ListNode in list", KPC(node_for_val), KP(prev_node), KPC(prev_node), "next_node", prev_node->get_next(), KPC(this));
      } else {
        node_for_val->set_next(prev_node->get_next());
        prev_node->set_next(node_for_val);
        BaseList::inc_node_num_();
      }
    }

    OBLOG_LOG(DEBUG, "push_node finish", KR(ret), KPC(this), KPC(node_for_val));

    return ret;
  }

  ListNodeType* pop_front_node()
  {
    ListNodeType *node = get_first_node();
    if (OB_NOT_NULL(node)) {
      ListNodeType *next = node->get_next();
      l_head_.set_next(next);
      node->set_next(nullptr);
      BaseList::dec_node_num_();
      if (OB_ISNULL(next)) {
        ob_assert(is_list_empty());
        l_tail_ = nullptr;
      }
    }

    OBLOG_LOG(DEBUG, "pop_front_node", KPC(this), KPC(node));

    return node;
  }

  INHERIT_TO_STRING_KV(
      "sorted_linked_list", BaseList,
      "is_list_empty", is_list_empty(),
      "l_head", l_head_.get_next(),
      K_(l_tail),
      KPC_(l_tail),
      K_(is_unique));
private:
  void reset_data_()
  {
    T *val = nullptr;
    while (! is_list_empty()) {
      pop(val);
    }
    l_tail_ = nullptr;
    l_head_.set_next(l_tail_);
  }
  int alloc_node_(T &val, ListNodeType *&node)
  {
    int ret = OB_SUCCESS;
    int64_t alloc_size = sizeof(ListNodeType);
    if (OB_ISNULL(OBCDC_ALLOC_MEM_CHECK_NULL_WITH_CAST("sorted_linked_list_node", ListNodeType, node, allocator_, alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OBLOG_LOG(ERROR, "alloc memory for list node failed", KR(ret), K(val), K(alloc_size));
    } else {
      new(node) ListNodeType(val);
    }

    return ret;
  }
  // find pos to insert new obj
  // prev_node is the last ListNode in list that value less than the input_ListNode
  // O(N)
  // @param  [in]   ListNode          ListNode to search
  // @param  [out]  prev_node     the last ListNode already in list that has value less than ListNode
  // @param  [out]  has_duplicate has duplicate ListNode(means has the same value with ListNode)
  void find_pos_(const T &val, ListNodeType *&prev_node, bool &has_duplicate)
  {
    prev_node = &l_head_;
    ListNodeType *next = l_head_.get_next();
    bool found = false;
    has_duplicate = false;

    // conditions to quit search prev_node
    // 1. found duplicate ListNode, ret = OB_ENTRY_EXIST
    // 2. not found data equals or greater than ListNode, the last prev_node is returned(expected l_tail_)

    while (OB_NOT_NULL(next = prev_node->get_next()) && !found) {
      if (*next < val) {
        // next ListNode is less than ListNode, push forword prev_node and go on search
        prev_node = next;
      } else {
        found = true;
        if (*next == val) {
          has_duplicate = true;
        }
      }
    }
  }

private:
  ObIAllocator &allocator_;
  ListNodeType l_head_;   // as head of list，always points to dummy_
  ListNodeType *l_tail_;  // tail of list, change if ListNode appends to last ListNode of lis
  bool is_unique_;        // if ListNode in list is unique(value but not address)
private:
  DISALLOW_COPY_AND_ASSIGN(SortedLinkedList);
};

}
}

#endif

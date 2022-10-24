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
 *
 * SortedLinkList, NOT THREAD SAFE!
 * only support push node, remove node is not supported currently
 */

#ifndef OCEANBSE_OBCDC_SORTED_LIGHTY_LIST_
#define OCEANBSE_OBCDC_SORTED_LIGHTY_LIST_

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"

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
template <typename T>
class SortedLightyList
{
public:
  SortedLightyList(const bool is_unique) :dummy_(), l_head_(&dummy_), l_tail_(NULL), num_(0), is_unique_(is_unique) {}
  ~SortedLightyList() { reset(); }

  void reset()
  {
    reset_data();
    is_unique_ = false;
  }

  void reset_data()
  {
    l_tail_ = NULL;
    T *node = dummy_.get_next();
    while (OB_NOT_NULL(node)) {
      T *tmp = node->get_next();
      node->set_next(NULL);
      node = tmp;
    }
    dummy_.set_next(NULL);
    num_ = 0;
  }

  bool operator==(const SortedLightyList<T> &other)
  {
    bool b_ret = true;

    if (num_ != other.num_) {
      b_ret = false;
    } else {
      T *next = get_first_node();
      T *other_next = other.get_first_node();
      while(OB_NOT_NULL(next) && b_ret) {
        if (OB_ISNULL(other_next)) {
          b_ret = false;
        } else if ((*next) == (*other_next)) {
          next = next->get_next();
          other_next = other_next->get_next();
        } else {
          b_ret = false;
        }
      }
    }

    return b_ret;
  }

public:
  // push node(type of T*) into queue
  // can't push the same data into list multi times !!!
  // retval OB_SUCCESS:               push success
  // retval OB_INVALID_ARGUMENT       node is NULL
  // retval OB_ENTRY_EXIST            elements if queue is required unique(is_unique = true) and node is already in queue
  int push(T *node)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(node) || OB_UNLIKELY(node->get_next())) {
      ret = OB_INVALID_ARGUMENT;
      OBLOG_LOG(ERROR, "invalid node or next_ptr of node is not NULL", KPC(node), KP(node), KPC(this));
    } else {
      T *prev_node = NULL;

      if (OB_ISNULL(l_tail_)) {
        // empty list
        dummy_.set_next(node);
        l_tail_ = node;
        inc_num_();
      } else if (*l_tail_ < *node) {
        // insert at tail
        l_tail_->set_next(node);
        l_tail_ = node;
        inc_num_();
      } else {
        // insert between dummy and l_tail_
        bool has_duplicate_node = false;
        find_pos_(node, prev_node, has_duplicate_node);
        if (has_duplicate_node && is_unique_) {
          ret = OB_ENTRY_EXIST;
          OBLOG_LOG(INFO, "found duplicatee node in list", KPC(node), KP(prev_node), KPC(prev_node), "next_node", prev_node->get_next(), KPC(this));
        } else if (node == prev_node->get_next()) {
          // do nothing cause same data in list
        } else {
          node->set_next(prev_node->get_next());
          prev_node->set_next(node);
          inc_num_();
        }
      }
    }

    return ret;
  }

  // check if queue contains element that has the same value with node
  bool contains(const T *node) const
  {
    bool b_ret = false;
    int ret = OB_SUCCESS;
    T *tmp = NULL;

    if (OB_NOT_NULL(node)) {
      if (!is_empty()) {
        if ((*l_tail_) < (*node)) {
          // quick-path: node is lager than last node: list doesn't contains node
          b_ret = false;
        } else if ((*l_tail_) == (*node) || (*(l_head_->get_next())) == (*node)) {
          // quick-path: node is equals to first/last node: list contains node
          b_ret = true;
        } else {
          find_pos_(node, tmp, b_ret);
        }
      }
    } else {
      // return false if node is NULL
    }

    return b_ret;
  }

  int64_t count() const { return num_; }
  bool is_empty() const { return 0 == num_; }
  T *get_first_node() const { return l_head_->get_next(); }
  T *get_last_node() const { return l_tail_; }
  TO_STRING_KV(K_(dummy), "dummy_next", dummy_.get_next(), K_(l_tail), KPC_(l_tail), K_(num), K_(is_unique));
private:
  // find pos to insert new obj
  // prev_node is the last node in list that value less than the input_node
  // O(N)
  // @param  [in]   node          node to search
  // @param  [out]  prev_node     the last node already in list that has value less than node
  // @param  [out]  has_duplicate has duplicate node(means has the same value with node)
  void find_pos_(const T *node, T *&prev_node, bool &has_duplicate) const
  {
    prev_node = l_head_;
    T *next = NULL;
    bool found = false;
    has_duplicate = false;

    // conditions to quit search prev_node
    // 1. found duplicate node, ret = OB_ENTRY_EXIST
    // 2. not found data equals or greater than node, the last prev_node is returned(expected l_tail_)
    while (OB_NOT_NULL(next = prev_node->get_next()) && !found) {
      if (*next < *node) {
        // next node is less than node, push forword prev_node and go on search
        prev_node = next;
      } else {
        found = true;
        if (*next == *node) {
          has_duplicate = true;
        }
      }
    }
  }

  void inc_num_() { num_++; }
  void dec_num_() { num_--; }

private:
  class DummyNode: public T
  {
  public:
    DummyNode() {}
    virtual ~DummyNode() {}
    bool operator<(const T &other) { return true; }
    bool operator==(const T &other) { return false; }
  };

private:
  DummyNode dummy_; // as head of list
  T *l_head_;       // always points to dummy_
  T *l_tail_;       // tail of list, change if node appends to last node of list
  int64_t num_;     // total node count in list(except dummy_)
  bool is_unique_;  // if node in list is unique(value but not address)
private:
  DISALLOW_COPY_AND_ASSIGN(SortedLightyList);
};

}
}

#endif

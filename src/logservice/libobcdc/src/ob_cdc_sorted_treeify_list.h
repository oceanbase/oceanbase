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
 * SortedTreeifyList, NOT THREAD SAFE!
 */

#ifndef OCEANBSE_OBCDC_SORTED_TREEIFY_LIST_
#define OCEANBSE_OBCDC_SORTED_TREEIFY_LIST_

#include "lib/container/ob_rbtree.h"
#include "ob_cdc_sorted_linked_list.h"
#include "ob_log_config.h"
#include "ob_cdc_mem_mgr.h"

namespace oceanbase
{
using namespace container;
namespace libobcdc
{

template <typename T, typename CompareFunc>
class TreeNode : public ListNode<T, CompareFunc>
{
  public:
    typedef TreeNode<T, CompareFunc> NodeType;
    explicit TreeNode(T &val) : ListNode<T, CompareFunc>(val) {}
    virtual ~TreeNode() {}
    int compare(const NodeType *left, const NodeType *right) const
    {
      int ret = 0;
      if (OB_ISNULL(left) || OB_ISNULL(left->val_)) {
        ret = -1;
      } else if (OB_ISNULL(right) || OB_ISNULL(right->val_)) {
        ret = 1;
      } else {
        ret = CompareFunc::compare(*(left->val_), *(right->val_));
      }
      return ret;
    }
    int compare(const NodeType *other) const { return compare(this, other); }
    RBNODE(NodeType, rblink);
};

template<typename T, typename CompareFunc = DefaultValComparator<T>>
class SortedTreeifyList : public SortedList<T, CompareFunc>
{
  friend class SortedLinkedList<T, CompareFunc>;
public:
  typedef TreeNode<T, CompareFunc> NodeType;
  typedef ListNode<T, CompareFunc> BaseNodeType;
  typedef SortedList<T, CompareFunc> BaseList;

  SortedTreeifyList(ObIAllocator &allocator, bool auto_treeify_mode = false)
    : BaseList(),
      allocator_(allocator),
      auto_treeify_mode_(auto_treeify_mode),
      is_tree_mode_(false),
      list_(allocator, true/*is_unique*/), // treeify list must guaratee val in list is unique
      tree_(),
      auto_treeify_threshold_(0),
      auto_untreeify_threshold_(0)
  {
    if (auto_treeify_mode_) {
      auto_treeify_threshold_ = TCONF.sorted_list_auto_treeify_threshold;
      auto_untreeify_threshold_ = TCONF.sorted_list_auto_untreeify_threshold;
      _OBLOG_LOG(TRACE, "[TREEIFY_LIST][AUTO_TREEIFY_THRESHOLD:%ld][AUTO_UNTREEIFY_THRESHOLD:%ld]",
          auto_treeify_threshold_, auto_untreeify_threshold_);
    }
  }
  virtual ~SortedTreeifyList() {}
  void reset() override
  {
    untreeify();
    NodeType *node = nullptr;
    while(OB_NOT_NULL(node = pop_from_list_())) {
      if (OB_ISNULL(node)) {
        OBLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected null node");
      } else {
        node->~NodeType();
        allocator_.free(node);
        node = nullptr;
      }
    }
    ob_assert(list_.is_list_empty());
    ob_assert(BaseList::empty());
    tree_.init_tree();
  }

public:
  int push(T &val) override
  {
    int ret = OB_SUCCESS;
    int64_t alloc_size = sizeof(NodeType);
    NodeType *node = nullptr;

    if (OB_ISNULL(OBCDC_ALLOC_MEM_CHECK_NULL_WITH_CAST("SortedTreeifyList::push", NodeType, node, allocator_, alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OBLOG_LOG(ERROR, "alloc memory for node failed", KR(ret), K(alloc_size));
    } else {
      new(node) NodeType(val);

      if (OB_UNLIKELY(is_tree_mode_)) {
        if (OB_FAIL(push_to_tree_(*node))) {
          OBLOG_LOG(ERROR, "insert node into tree failed", KR(ret), KPC(this));
        }
      } else if (OB_FAIL(push_to_list_(*node))) {
        OBLOG_LOG(ERROR, "insert node into list failed", KR(ret), KPC(this));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(node)) {
        node->~NodeType();
        allocator_.free(node);
        node = nullptr;
      }
    }

    if (OB_SUCC(ret)) {
      if (auto_treeify_mode_ && ! is_tree_mode_ && BaseList::count() >= auto_treeify_threshold_) {
        if (OB_FAIL(treeify())) {
          OBLOG_LOG(WARN, "treeify failed", KR(ret), KPC(this));
        }
      }
    }

    return ret;
  }

  int pop(T *&val) override
  {
    int ret = OB_SUCCESS;
    NodeType *node = nullptr;
    val = nullptr;

    if (OB_UNLIKELY(is_tree_mode_)) {
      node = pop_from_tree_();
    } else {
      node = pop_from_list_();
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(node)) {
      if (auto_treeify_mode_ && is_tree_mode_ && BaseList::count() < auto_untreeify_threshold_) {
        if (OB_FAIL(untreeify())) {
          OBLOG_LOG(WARN, "untreeify failed", KR(ret), KPC(this));
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(node)) {
      val = node->get_val();
      node->~NodeType();
      allocator_.free(node);
      node = nullptr;
    }

    return ret;
  }

  bool contains(const T &val) override
  {
    int ret = OB_SUCCESS;
    bool found = false;

    if (is_tree_mode_) {
      T &non_const_val = const_cast<T&>(val);
      NodeType node(non_const_val);
      NodeType *found_node = nullptr;
      if (OB_FAIL(tree_.search(&node, found_node))) {
        OBLOG_LOG(WARN, "search node from tree failed", KR(ret), K(node));
      } else {
        found = (nullptr != found_node);
      }
    } else {
      found = list_.contains(val);
    }

    return found;
  }

  // manage node by rbtree
  int treeify()
  {
    int ret = OB_SUCCESS;
    const int64_t node_cnt = BaseList::count();
    OBLOG_LOG(DEBUG, "treeify begin", KPC(this));

    if (OB_UNLIKELY(is_tree_mode_)) {
      // ignore cause already in tree mode
    } else if (OB_UNLIKELY(!tree_.is_empty())) {
      ret = OB_STATE_NOT_MATCH;
      OBLOG_LOG(ERROR, "expect empty tree before treeify", KR(ret), KPC(this));
    } else {
      int64_t removed_linked_node_cnt = 0;

      while (OB_SUCC(ret) && removed_linked_node_cnt++ < node_cnt) {
        NodeType *node = nullptr;
        if (OB_ISNULL(node = pop_from_list_())) {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "invalid node poped from linked_list", KR(ret), KP(node));
        } else if (OB_FAIL(push_to_tree_(*node))) {
          OBLOG_LOG(ERROR, "insert node into tree failed", KR(ret), KPC(this));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ob_assert(list_.is_list_empty());
      ob_assert(node_cnt == BaseList::count());
      is_tree_mode_ = true;
    }
    OBLOG_LOG(DEBUG, "treeify end", KPC(this));

    return ret;
  }

  // manage node by linked list
  int untreeify()
  {
    int ret = OB_SUCCESS;
    const int64_t node_cnt = BaseList::count();
    OBLOG_LOG(DEBUG, "untreeify begin", KPC(this));

    if (OB_UNLIKELY(!is_tree_mode_)) {
    } else if (OB_UNLIKELY(! list_.is_list_empty())) {
      ret = OB_STATE_NOT_MATCH;
      OBLOG_LOG(ERROR, "expect empty list before untreeify", KR(ret), KPC(this));
    } else {
      int64_t removed_tree_node_cnt = 0;

      while (OB_SUCC(ret) && removed_tree_node_cnt++ < node_cnt) {
        NodeType *node = nullptr;

        if (OB_ISNULL(node = pop_from_tree_())) {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "pop_from_tree_ failed", KR(ret));
        } else if (OB_FAIL(push_to_list_(*node))) {
          OBLOG_LOG(ERROR, "insert node into list failed", KR(ret), KPC(this));
        }
      }

    }

    if (OB_SUCC(ret)) {
      ob_assert(tree_.is_empty());
      ob_assert(node_cnt == BaseList::count());
      is_tree_mode_ = false;
    }
    OBLOG_LOG(DEBUG, "untreeify end", KPC(this));

    return ret;
  }

  virtual BaseNodeType* get_first_node() const override
  {
    BaseNodeType *node = nullptr;
    if (OB_UNLIKELY(is_tree_mode_)) {
      node = static_cast<BaseNodeType*>(tree_.get_first());
    } else {
      node = list_.get_first_node();
    }
    return node;
  }

  int get_next_node(const BaseNodeType &node, BaseNodeType *&next_node) const override
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(is_tree_mode_)) {
      const NodeType *cur_node = static_cast<const NodeType *>(&node);
      NodeType *next = nullptr;

      if (OB_ISNULL(cur_node)) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "invalid TreeNode converted from ListNodeType", KR(ret), K(node));
      } else if (OB_FAIL(tree_.get_next(cur_node, next))) {
        OBLOG_LOG(WARN, "fail to get next node from tree", KR(ret), KPC(cur_node));
      } else {
        next_node = static_cast<BaseNodeType*>(next);
      }
    } else {
      next_node = node.get_next();
    }

    return ret;
  }

  INHERIT_TO_STRING_KV(
      "sorted_treeify_list", BaseList,
      K_(is_tree_mode),
      K_(auto_treeify_mode),
      "is_tree_empty", tree_.is_empty(),
      K_(list),
      K_(auto_treeify_threshold),
      K_(auto_untreeify_threshold));

private:
  int push_to_tree_(NodeType &node)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tree_.insert(&node))) {
      OBLOG_LOG(ERROR, "insert node into tree failed", KR(ret), KPC(this));
    } else {
      BaseList::inc_node_num_();
      OBLOG_LOG(DEBUG, "push_to_tree_ succ", KPC(this));
    }
    return ret;
  }

  NodeType *pop_from_tree_()
  {
    int ret = OB_SUCCESS;
    NodeType *node = tree_.get_first();
    if (OB_NOT_NULL(node)) {
      if (OB_FAIL(tree_.remove(node))) {
        OBLOG_LOG(ERROR, "remove node from tree failed", KR(ret), KPC(node));
      } else {
        BaseList::dec_node_num_();
        OBLOG_LOG(DEBUG, "pop_from_tree_ succ", KPC(this));
      }
    }
    return node;
  }

  int push_to_list_(NodeType &node)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(list_.push_node(&node))) {
      OBLOG_LOG(ERROR, "insert node into list failed", KR(ret), KPC(this));
    } else {
      BaseList::inc_node_num_(); // should operate the node count cause list is another container which is seperate with treeify_list
      OBLOG_LOG(DEBUG, "push_to_list_ succ", KPC(this));
    }
    return ret;
  }

  NodeType *pop_from_list_()
  {
    NodeType *node = static_cast<NodeType*>(list_.pop_front_node());
    if (OB_NOT_NULL(node)) {
      BaseList::dec_node_num_();
      OBLOG_LOG(DEBUG, "pop_from_list_ succ", KPC(this));
    }
    return node;
  }

private:
  typedef ObRbTree<NodeType, ObDummyCompHelper<NodeType>> tree_t;
  // allocator to alloc memory for NODE
  ObIAllocator &allocator_;
  bool auto_treeify_mode_;
  bool is_tree_mode_;
  SortedLinkedList<T, CompareFunc> list_;
  tree_t tree_;
  // treefiy the list if auto_treeify_mode_ = true and node_num is large than auto_treeify_threshold_
  int64_t auto_treeify_threshold_;
  // untreeify the list if auto_treeify_mode_ = true and node_num is small than auto_untreeify_threshold_
  int64_t auto_untreeify_threshold_;
}; // end of SortedTreeifyList

} // end of namespace libobcdc
} // end of namespace oceanbase
#endif

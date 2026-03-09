/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_FAST_LIST_H_
#define _OCEANBASE_STORAGE_FTS_OB_FAST_LIST_H_

#include "storage/fts/ik/ob_fast_segment_array.h"

namespace oceanbase
{
namespace storage
{
/**
 * @brief A fast list implementation using a segment array.
 *
 * @tparam T The type of the elements in the list.
 * @tparam block_capacity The capacity of each block in the segment array.
 *
 * NOTE: This container is only safe for trivially-destructible / POD-like types.
 * Element destructors are never called individually; memory is reclaimed in bulk
 * by the underlying allocator. Do NOT use it with types that manage external resources.
 */
template <typename T, int64_t block_capacity = 256>
class ObFastList
{
  struct Node
  {
  public:
    Node() : next(nullptr), prev(nullptr), value() { }
    Node(const T &value) : next(nullptr), prev(nullptr), value(value) { }

  public:
    Node *next;
    Node *prev;
    T value;
  };

  struct NodeHolder
  {
  public:
    operator Node *() { return reinterpret_cast<Node *>(this); }
    operator const Node *() const { return reinterpret_cast<const Node *>(this); }

  public:
    Node *next;
    Node *prev;
  };

public:
  class iterator
  {
  public:
    iterator() : node_(nullptr) {}
    explicit iterator(Node *node) : node_(node) {}

    T &operator*() const { return node_->value; }
    T *operator->() const { return &node_->value; }
    iterator &operator++()
    {
      node_ = node_ ? node_->next : nullptr;
      return *this;
    }
    iterator operator++(int)
    {
      iterator tmp(*this);
      node_ = node_ ? node_->next : nullptr;
      return tmp;
    }
    iterator &operator--()
    {
      node_ = node_ ? node_->prev : nullptr;
      return *this;
    }
    iterator operator--(int)
    {
      iterator tmp(*this);
      node_ = node_ ? node_->prev : nullptr;
      return tmp;
    }
    bool operator==(const iterator &other) const { return node_ == other.node_; }
    bool operator!=(const iterator &other) const { return node_ != other.node_; }

  private:
    friend class ObFastList;
    Node *node_;
  };

  class const_iterator
  {
  public:
    const_iterator() : node_(nullptr) { }
    explicit const_iterator(const Node *node) : node_(const_cast<Node *>(node)) { }
    const_iterator(const iterator &it) : node_(it.node_) { }

    const T &operator*() const { return node_->value; }
    const T *operator->() const { return &node_->value; }
    const_iterator &operator++()
    {
      node_ = node_ ? node_->next : nullptr;
      return *this;
    }
    const_iterator operator++(int)
    {
      const_iterator tmp(*this);
      node_ = node_ ? node_->next : nullptr;
      return tmp;
    }
    const_iterator &operator--()
    {
      node_ = node_ ? node_->prev : nullptr;
      return *this;
    }
    const_iterator operator--(int)
    {
      const_iterator tmp(*this);
      node_ = node_ ? node_->prev : nullptr;
      return tmp;
    }
    bool operator==(const const_iterator &other) const { return node_ == other.node_; }
    bool operator!=(const const_iterator &other) const { return node_ != other.node_; }

  private:
    friend class ObFastList;
    Node *node_;
  };

public:
  explicit ObFastList(ObIAllocator &allocator)
      : pool_(allocator),
        size_(0)
  {
    root_.next = root_;
    root_.prev = root_;
  }

  ~ObFastList() { reset(); }

  inline void reset()
  {
    clear();
    pool_.reset();
  }

  inline void reuse()
  {
    root_.next = root_;
    root_.prev = root_;
    size_ = 0;
    pool_.reuse();
  }

  inline bool empty() const { return 0 == size_; }
  inline int64_t size() const { return size_; }

  inline T &get_first() { return root_.next->value; }
  inline const T &get_first() const { return root_.next->value; }

  inline T &get_last() { return root_.prev->value; }
  inline const T &get_last() const { return root_.prev->value; }

  inline iterator begin() { return iterator(root_.next); }
  inline iterator end() { return iterator(root_); }
  inline const_iterator begin() const { return const_iterator(root_.next); }
  inline const_iterator end() const { return const_iterator(root_); }
  inline iterator last() { return iterator(root_.prev); }
  inline const_iterator last() const { return const_iterator(root_.prev); }
  inline int push_front(const T &value) { return insert_before_(root_.next, value); }
  inline int push_back(const T &value) { return insert_before_(root_, value); }
  inline int insert(iterator pos, const T &value) { return insert_before_(pos.node_, value); }
  inline int insert(const_iterator pos, const T &value) { return insert_before_(pos.node_, value); }

  inline int pop_front()
  {
    int ret = OB_SUCCESS;
    if (empty()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      remove_node_(root_.next);
    }
    return ret;
  }

  inline int pop_back()
  {
    int ret = OB_SUCCESS;
    if (empty()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      remove_node_(root_.prev);
    }
    return ret;
  }

private:
  void clear()
  {
    Node *cur = root_.next;
    while (cur != root_) {
      Node *next = cur->next;
      destroy_node_(cur);
      cur = next;
    }
    root_.next = root_;
    root_.prev = root_;
    size_ = 0;
  }

  inline int insert_before_(Node *pos, const T &value)
  {
    Node *node = nullptr;
    int ret = OB_SUCCESS;
    if (OB_FAIL(alloc_node_(node, value))) {
    } else {
      Node *anchor = nullptr == pos ? root_ : pos;
      node->next = anchor;
      node->prev = anchor->prev;
      anchor->prev->next = node;
      anchor->prev = node;
      ++size_;
    }
    return ret;
  }

  inline int alloc_node_(Node *&node, const T &value)
  {
    int ret = OB_SUCCESS;
    Node dummy(value);
    if (OB_FAIL(pool_.push_back(dummy))) {
    } else {
      node = &pool_.at(pool_.count() - 1);
    }
    return ret;
  }

  inline void remove_node_(Node *node)
  {
    if (nullptr == node || node == root_) {
    } else {
      node->prev->next = node->next;
      node->next->prev = node->prev;
      destroy_node_(node);
      --size_;
    }
  }

  inline void destroy_node_(Node *node)
  {
    node->next = node->prev = nullptr;
  }

private:
  ObFastSegmentArray<Node, block_capacity> pool_;
  NodeHolder root_;
  int64_t size_;
};

} // namespace storage
} // namespace oceanbase
#endif // _OCEANBASE_STORAGE_FTS_OB_FAST_LIST_H_
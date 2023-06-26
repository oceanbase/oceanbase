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

#ifndef OCEANBASE_LIB_LIST_OB_DLINK_NODE_H_
#define OCEANBASE_LIB_LIST_OB_DLINK_NODE_H_

#include <stdint.h>
#include "lib/hash_func/ob_hash_func.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
template <typename DLinkNode>
class ObDList;

// A template base class used as the node of double linked list
// The concrete node class B should inherit ObDLinkNode<B>
template <typename Derived>
class ObDLinkBase
{
public:
  ObDLinkBase();
  virtual ~ObDLinkBase() {}

  Derived *get_next() {return next_;}
  Derived *get_prev() {return prev_;}

  const Derived *get_next() const {return next_;}
  const Derived *get_prev() const {return prev_;}

  /// these utility functions do not check arguments on purpose,
  /// the caller should ensure the validity
  /// @assert NULL != e
  /// insert the node e before this node
  void add_before(Derived *e);
  /// insert the node e after this node
  void add_after(Derived *e);
  /// unlink this node from the list
  void unlink();
  /// add linked list after this node
  void add_range_after(Derived *first, Derived *last);
  /// replace
  void replace_by(Derived *e);

  virtual void reset() {prev_ = NULL; next_ = NULL;}
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  };
protected:
  void add(Derived *prev, Derived *e, Derived *next);
  template <typename DLinkNode>
  friend class ObDList;
protected:
  Derived *prev_;
  Derived *next_;
};

template <typename Derived>
ObDLinkBase<Derived>::ObDLinkBase()
    : prev_(NULL), next_(NULL)
{
}

// insert one node before this node
template <typename Derived>
void ObDLinkBase<Derived>::add_before(Derived *e)
{
  add(prev_, e, static_cast<Derived *>(this));
}

// insert one node after this node
template <typename Derived>
void ObDLinkBase<Derived>::add_after(Derived *e)
{
  add(static_cast<Derived *>(this), e, next_);
}

// remove node from list
template <typename Derived>
void ObDLinkBase<Derived>::unlink()
{
  Derived *next_node = next_;
  Derived *prev_node = prev_;
  if (next_node && prev_node) {
    next_node->prev_ = prev_node;
    prev_node->next_ = next_node;
    next_ = prev_ = nullptr;
  }
}

// remove node from list
template <typename Derived>
void ObDLinkBase<Derived>::replace_by(Derived *e)
{
  if (e != this) {
    e->next_ = next_;
    e->prev_ = prev_;
    prev_->next_ = e;
    next_->prev_ = e;
    prev_ = NULL;
    next_ = NULL;
  }
}

template <typename Derived>
void ObDLinkBase<Derived>::add(Derived *prev, Derived *e, Derived *next)
{
  e->prev_ = prev;
  e->next_ = next;
  prev->next_ = e;
  next->prev_ = e;
}

template <typename Derived>
void ObDLinkBase<Derived>::add_range_after(Derived *first, Derived *last)
{
  Derived *next = this->next_;
  this->next_ = first;
  first->prev_ = static_cast<Derived *>(this);
  next->prev_ = last;
  last->next_ = next;
}

template<typename T>
struct ObDLinkNode: public ObDLinkBase<ObDLinkNode<T> >
{
  ObDLinkNode():data_() {};
  ObDLinkNode(T data):data_(data) {};
  ~ObDLinkNode() {};

  T &get_data() {return data_;};
  const T &get_data() const {return data_;};
  bool operator== (const ObDLinkNode &other) const {return data_ == other.data_;};
  int64_t hash() const { return do_hash(data_);};
private:
  T data_;
};

template<typename T>
struct ObDLinkDerived: public ObDLinkBase<T>, T
{
  ObDLinkDerived() {};
  ~ObDLinkDerived() {};
};

} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_LIST_OB_DLINK_NODE_H_

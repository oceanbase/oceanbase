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

#ifndef OB_DI_LIST_H_
#define OB_DI_LIST_H_
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

namespace oceanbase
{
namespace common
{
template<typename Value>
struct ObDINode
{
  ObDINode() : prev_(NULL), next_(NULL) {}
  virtual ~ObDINode() {}
  Value *get_next() { return next_; }
  Value *get_prev() { return prev_; }
  void add_before(Value *e);
  void add_after(Value *e);
  void add(Value *prev, Value *e, Value *next);
  void unlink();
  Value *prev_;
  Value *next_;
};

template<typename DLinkNode>
struct ObDIList
{
  ObDIList();
  virtual ~ObDIList();
  DLinkNode *get_header() {return static_cast<DLinkNode *>(&header_);}
  DLinkNode *get_first() { return header_.next_; }
  bool is_empty() const { return header_.next_ == static_cast<const DLinkNode *>(&header_); }
  bool add_last(DLinkNode *e);
  bool add_first(DLinkNode *e);
  bool move_to_first(DLinkNode *e);
  bool move_to_last(DLinkNode *e);
  DLinkNode *remove(DLinkNode *e);
  DLinkNode *remove_last();
  DLinkNode *remove_first();
  ObDINode<DLinkNode> header_;
  int32_t  size_;
};

template <typename Value>
void ObDINode<Value>::add_before(Value *e)
{
  add(prev_, e, static_cast<Value *>(this));
}

template <typename Value>
void ObDINode<Value>::add_after(Value *e)
{
  add(static_cast<Value *>(this), e, next_);
}

template <typename Value>
void ObDINode<Value>::add(Value *prev, Value *e, Value *next)
{
  prev->next_ = e;
  e->prev_ = prev;
  next->prev_ = e;
  e->next_ = next;
}

template <typename Value>
void ObDINode<Value>::unlink()
{
 if (NULL != prev_ && NULL != next_) {
    prev_->next_ = next_;
    next_->prev_ = prev_;
    prev_ = NULL;
    next_ = NULL;
 }
}

template<typename DLinkNode>
ObDIList<DLinkNode>::ObDIList()
{
  header_.next_ = static_cast<DLinkNode *>(&header_);
  header_.prev_ = static_cast<DLinkNode *>(&header_);
  size_ = 0;
}

template<typename DLinkNode>
ObDIList<DLinkNode>::~ObDIList()
{
}

template <typename DLinkNode>
bool ObDIList<DLinkNode>::add_last(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
  } else if (OB_UNLIKELY(e->get_prev() != NULL
             || e->get_next() != NULL)) {
    ret = false;
  } else {
    header_.add_before(e);
    ++size_;
  }
  return ret;
}

template <typename DLinkNode>
bool ObDIList<DLinkNode>::add_first(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
  } else if (e->get_prev() != NULL
             || e->get_next() != NULL) {
    ret = false;
  } else {
    header_.add_after(e);
    ++size_;
  }
  return ret;
}

template <typename DLinkNode>
bool ObDIList<DLinkNode>::move_to_first(DLinkNode *e)
{
  bool ret = true;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = false;
  } else {
    e->unlink();
    --size_;
    ret = add_first(e);
  }
  return ret;
}

template <typename DLinkNode>
bool ObDIList<DLinkNode>::move_to_last(DLinkNode *e)
{
  bool ret = true;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = false;
  } else {
    e->unlink();
    size_--;
    ret = add_last(e);
  }
  return ret;
}

template <typename DLinkNode>
DLinkNode *ObDIList<DLinkNode>::remove(DLinkNode *e)
{
  DLinkNode *ret = e;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = NULL;
  } else {
    e->unlink();
    size_--;
  }
  return ret;
}

template <typename DLinkNode>
DLinkNode *ObDIList<DLinkNode>::remove_last()
{
  return remove(header_.prev_);
}

template <typename DLinkNode>
DLinkNode *ObDIList<DLinkNode>::remove_first()
{
  return remove(header_.next_);
}

}//common
}//oceanbase
#endif /* OB_DI_LIST_H_ */

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

#ifndef OCEANBASE_LIB_LIST_OB_DLIST_H_
#define OCEANBASE_LIB_LIST_OB_DLIST_H_

#include "lib/ob_define.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_unify_serialize.h"

#define DLIST_FOREACH_X(curr, dlist, extra_condition)                     \
  for (auto curr = (dlist).get_first();                                   \
       (extra_condition) && curr != (dlist).get_header() && curr != NULL;  \
       curr = curr->get_next())
#define DLIST_FOREACH_NORET(curr, dlist) DLIST_FOREACH_X(curr, dlist, true)
#define DLIST_FOREACH(curr, dlist) DLIST_FOREACH_X(curr, dlist, OB_SUCC(ret))

#define DLIST_FOREACH_BACKWARD_X(curr, dlist, extra_condition)           \
  for (auto curr = (dlist).get_last();                                   \
       (extra_condition) && curr != (dlist).get_header() && curr != NULL; \
       curr = curr->get_prev())

#define DLIST_FOREACH_BACKWARD_REMOVESAFE_X(curr, dlist, extra_condition) \
  for (auto curr = (dlist).get_last(), save = curr->get_prev();          \
       (extra_condition) && curr != (dlist).get_header();                  \
       curr = save, save = save->get_prev())

#define DLIST_FOREACH_REMOVESAFE_X(curr, dlist, extra_condition)          \
  for (auto curr = (dlist).get_first(), save = curr->get_next();          \
       (extra_condition) && curr != (dlist).get_header();                  \
       curr = save, save = save->get_next())
#define DLIST_FOREACH_REMOVESAFE_NORET(curr, dlist) DLIST_FOREACH_REMOVESAFE_X(curr, dlist, true)
#define DLIST_FOREACH_REMOVESAFE(curr, dlist) DLIST_FOREACH_REMOVESAFE_X(curr, dlist, OB_SUCC(ret))

#define DLIST_REMOVE_ALL_X(p, dlist, extra_condition)                     \
  for (auto p = (dlist).remove_first();                                   \
       (extra_condition) && NULL != p && p != (dlist).get_header();        \
       p = (dlist).remove_first())
#define DLIST_REMOVE_ALL_NORET(curr, dlist) DLIST_REMOVE_ALL_X(curr, dlist, true)
#define DLIST_REMOVE_ALL(curr, dlist) DLIST_REMOVE_ALL_X(curr, dlist, OB_SUCC(ret))

namespace oceanbase
{
namespace common
{
// DLinkNode should be a derived class of ObDLinkBase. See also ObDLinkNode and ObDLinkDerived
template <typename DLinkNode>
class ObDList
{
public:
  typedef DLinkNode node_t;

  ObDList();
  ~ObDList();

  // move nodes to another list.
  int move(ObDList &list)
  {
    list.clear();
    list.size_ = size_;
    if (size_ > 0) {
      list.header_ = header_;
      get_first()->prev_ = static_cast<DLinkNode *>(&list.header_);
      get_last()->next_ = static_cast<DLinkNode *>(&list.header_);
    }
    clear();
    return OB_SUCCESS;
  }

  // get the header
  DLinkNode *get_header() {return static_cast<DLinkNode *>(&header_);}
  const DLinkNode *get_header() const {return static_cast<const DLinkNode *>(&header_);}

  //get the last node
  DLinkNode *get_last() { return header_.prev_; }
  const DLinkNode *get_last() const { return header_.prev_; }

  //get the first node
  DLinkNode *get_first() { return header_.next_; }
  const DLinkNode *get_first() const { return header_.next_; }
  const DLinkNode *get_first_const() const { return header_.next_; }

  // insert the node to the tail
  bool add_last(DLinkNode *e);
  // insert the node to the head
  bool add_first(DLinkNode *e);
  bool add_before(const DLinkNode *pos, DLinkNode *e);
  bool increasing_add(DLinkNode *e);
  // move the node to the head
  bool move_to_first(DLinkNode *e);
  // move the node to the tail
  bool move_to_last(DLinkNode *e);

  // remove the node at tail
  DLinkNode *remove_last();
  // remove the node at head
  DLinkNode *remove_first();

  void push_range(ObDList<DLinkNode> &range);
  void pop_range(int32_t num, ObDList<DLinkNode> &range);

  //the list is empty or not
  bool is_empty() const { return header_.next_ == static_cast<const DLinkNode *>(&header_); }
  // get the list size
  int32_t get_size() const { return size_; }

  DLinkNode *remove(DLinkNode *e);

  void clear()
  {
    header_.next_ = static_cast<DLinkNode *>(&header_);
    header_.prev_ = static_cast<DLinkNode *>(&header_);
    size_ = 0;
  }
  void reset()
  { clear(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDList);
private:
  ObDLinkBase<DLinkNode> header_;
  int32_t  size_;
};

template <typename DLinkNode>
ObDList<DLinkNode>::ObDList()
{
  header_.next_ = static_cast<DLinkNode *>(&header_);
  header_.prev_ = static_cast<DLinkNode *>(&header_);
  size_ = 0;
}

template <typename DLinkNode>
ObDList<DLinkNode>::~ObDList()
{
}

template <typename DLinkNode>
bool ObDList<DLinkNode>::add_last(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
    LIB_LOG(ERROR, "the poniter is null",K(e));
  } else if (OB_UNLIKELY(e->get_prev() != NULL
             || e->get_next() != NULL)) {
    ret = false;
    LIB_LOG(ERROR, "link node is not alone",
              K(e->get_prev()), K(e->get_next()), K(e));
  } else {
    header_.add_before(e);
    ++size_;
  }
  return ret;
}

// insert the node to the head
template <typename DLinkNode>
bool ObDList<DLinkNode>::add_first(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
    LIB_LOG(ERROR, "the poniter is null",K(e));
  } else if (e->get_prev() != NULL
             || e->get_next() != NULL) {
    ret = false;
    LIB_LOG(ERROR, "link node is not alone",
              K(e->get_prev()), K(e->get_next()), K(e));
  } else {
    header_.add_after(e);
    ++size_;
  }
  return ret;
}

template <typename DLinkNode>
bool ObDList<DLinkNode>::add_before(const DLinkNode *pos, DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(pos) || OB_ISNULL(e)) {
    ret = false;
    LIB_LOG(ERROR, "the pointer is null",K(e));
  } else if (NULL == pos->get_prev() || NULL == pos->get_next()
      || NULL != e->get_prev() || NULL != e->get_next()) {
    ret = false;
    LIB_LOG(ERROR, "position node not linked or link node is not alone",
        K(pos->get_prev()), K(pos->get_next()), K(e->get_prev()), K(e->get_next()), K(e));
  } else {
    const_cast<DLinkNode *>(pos)->add_before(e);
    ++size_;
  }
  return ret;
}

template <typename DLinkNode>
bool ObDList<DLinkNode>::increasing_add(DLinkNode *e)
{
  bool bret = true;
  if (OB_ISNULL(e)) {
    bret = false;
  } else {
    DLinkNode *cur_node = header_.get_next();
    while (cur_node != &header_) {
      if (*e <= *cur_node) {
        cur_node = cur_node->get_next();
      } else {
        break;
      }
    }
    cur_node->add_before(e);
    ++size_;
  }
  return bret;
}

// move the node to the head
template <typename DLinkNode>
bool ObDList<DLinkNode>::move_to_first(DLinkNode *e)
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

// move the node to the tail
template <typename DLinkNode>
bool ObDList<DLinkNode>::move_to_last(DLinkNode *e)
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

// remove the node at tail
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove_last()
{
  return remove(header_.prev_);
}

// remove the node at head
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove_first()
{
  return remove(header_.next_);
}
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove(DLinkNode *e)
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
void ObDList<DLinkNode>::push_range(ObDList<DLinkNode> &range)
{
  if (!range.is_empty()) {
    DLinkNode *first = range.header_.next_;
    DLinkNode *last = range.header_.prev_;
    first->prev_ = NULL;
    last->next_ = NULL;
    this->header_.add_range_after(first, last);
    size_ += range.get_size();
    range.reset();
  }
}
template <typename DLinkNode>
void ObDList<DLinkNode>::pop_range(int32_t num, ObDList<DLinkNode> &range)
{
  DLinkNode *first = this->header_.next_;
  DLinkNode *last = first;
  int count = 0;
  if (count < num && last != &this->header_) {
    ++ count;
  }
  while (count < num && last->next_ != &this->header_) {
    ++ count;
    last = last->next_;
  }
  if (0 < count) {
    if (last->next_ == &this->header_) {
      reset();
    } else {
      header_.next_ = last->next_;
      last->next_->prev_ = static_cast<DLinkNode *>(&header_);
      size_ -= count;
    }
    first->prev_ = NULL;
    last->next_ = NULL;
    range.header_.add_range_after(first, last);
    range.size_ += count;
  }
}

int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt,
                    ...) __attribute__((format(printf, 4, 5)));

template <typename DLinkNode>
int64_t ObDList<DLinkNode>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) { // json style
  }
  for (DLinkNode *it = header_.next_;
       it != &header_ && it != header_.prev_ && OB_SUCCESS == ret;
       it = it->next_) {
    if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *it))) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
    }
  }
  if (OB_SUCCESS == ret) {
    if (!is_empty()) {
      if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *header_.prev_))) {
      }
    }
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
      }
    }
  }
  return pos;
}

template<typename T>
struct has_normal_deserialize {
  template<typename U, int (U::*)(const char *, const int64_t, int64_t &)> struct HELPS;
  template<typename U> static char Test(HELPS<U, &U::deserialize>*);
  template<typename U> static int Test(...);
  const static bool has_ = sizeof(Test<T>(0)) == sizeof(char);
};

template <bool>
class ObDlistDeserialzeHelper;

template<>
class ObDlistDeserialzeHelper<true>
{
public:
  template <typename DLinkNode, typename ALLOCATOR>
  int operator()(
      DLinkNode &node,
      ALLOCATOR &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
  {
    int ret = OB_SUCCESS;
    UNUSED(allocator);
    if (NULL == buf || data_len < 0 || pos < 0) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(node.deserialize(buf, data_len, pos))) {
      LIB_LOG(WARN, "failed to decode node", K(ret));
    }

    return ret;
  }
};
template<>
class ObDlistDeserialzeHelper<false>
{
public:
  template <typename DLinkNode, typename ALLOCATOR>
  int operator()(
      DLinkNode &node,
      ALLOCATOR &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
  {
    int ret = OB_SUCCESS;

    if (NULL == buf || data_len < 0 || pos < 0) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(node.deserialize(allocator, buf, data_len, pos))) {
      LIB_LOG(WARN, "failed to decode node", K(ret));
    }

    return ret;
  }
};

template <bool>
class ObDlistNodeDestoryHelper;

template<>
class ObDlistNodeDestoryHelper<true>
{
public:
  template <typename DLinkNode, typename ALLOCATOR>
  int operator()(
      DLinkNode &node,
      ALLOCATOR &allocator)
  {
    int ret = OB_SUCCESS;
    UNUSED(allocator);
    node.~DLinkNode();

    return ret;
  }
};

template<>
class ObDlistNodeDestoryHelper<false>
{
public:
  template <typename DLinkNode, typename ALLOCATOR>
  int operator()(
      DLinkNode &node,
      ALLOCATOR &allocator)
  {
    int ret = OB_SUCCESS;
     if (OB_FAIL(node.destroy(allocator))) {
      LIB_LOG(WARN, "failed to destroy node", K(ret));
    }
    node.~DLinkNode();

    return ret;
  }
};

template <typename DLinkNode, typename ALLOCATOR>
int deserialize_dlist(
    ObDList<DLinkNode> &list,
    ALLOCATOR &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t list_count = 0;
  void *tmp_buf = NULL;
  DLinkNode *tmp_node = NULL;

  if (list.get_size() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "cannot decode list when list is not empty", K(ret), K(list.get_size()));
  } else if (NULL == buf || data_len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid args", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(list_count);
    for (int64_t i = 0; OB_SUCC(ret) && i < list_count; ++i) {
      if (NULL == (tmp_buf = allocator.alloc(sizeof(DLinkNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "failed to alloc buf", K(ret));
      } else if (NULL == (tmp_node = new(tmp_buf) DLinkNode())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "failed to new node", K(ret));
      } else if (OB_SUCCESS != (ret = ObDlistDeserialzeHelper<has_normal_deserialize<DLinkNode>::has_ >() (
         *tmp_node,  allocator, buf, data_len, pos))) {
        LIB_LOG(WARN, "failed to decode node", K(ret));
      } else if (!list.add_last(tmp_node)) {
        ret = OB_ERR_SYS;
        LIB_LOG(WARN, "failed to add node", K(ret));
      }

      if (OB_FAIL(ret) && NULL != tmp_node) {
        int tmp_ret = ObDlistNodeDestoryHelper<has_normal_deserialize<DLinkNode>::has_>()(*tmp_node, allocator);
        if (OB_SUCCESS != tmp_ret) {
          LIB_LOG(WARN, "failed to destroy node", K(tmp_ret));
        }
        allocator.free(tmp_buf);
        tmp_buf = NULL;
        tmp_node = NULL;
      }
    }
  }

  if (OB_SUCC(ret) && list_count != list.get_size()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "list count not match", K(ret), K(list_count), K(list.get_size()));
  }
  return ret;

}

template <typename DLinkNode>
int serialize_dlist(const ObDList<DLinkNode> &list, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t list_count = list.get_size();
  OB_UNIS_ENCODE(list_count);
  DLIST_FOREACH_X(node, list, OB_SUCC(ret)) {
    if (OB_ISNULL(node)) {
      ret = OB_ERR_SYS;
      LIB_LOG(WARN, "node must not null", K(ret));
    } else if (OB_FAIL(node->serialize(buf, buf_len, pos))) {
      LIB_LOG(WARN, "failed to serialize node", K(ret));
    }
  }
  return ret;
}

template <typename DLinkNode>
int64_t get_dlist_serialize_size(const ObDList<DLinkNode> &list)
{
  int64_t len = 0;
  const int64_t list_count = list.get_size();

  OB_UNIS_ADD_LEN(list_count);
  DLIST_FOREACH_NORET(node, list) {
    if (NULL != node) {
      len += node->get_serialize_size();
    }
  }
  return len;
}

} // end namespace common
} // end namespace oceanbase


#endif  //OCEANBASE_LIB_LIST_OB_DLIST_H_

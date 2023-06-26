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

#ifndef  OCEANBASE_COMMON_LIST_H_
#define  OCEANBASE_COMMON_LIST_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "lib/hash/ob_hashutils.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace common
{
template <class T, class Allocator>
class ObList;

namespace list
{
template <class T>
struct Node
{
  typedef Node *ptr_t;
  typedef const Node *const_ptr_t;
  ptr_t next;
  ptr_t prev;
  T data;
  TO_STRING_KV(K(data));
};

template <class List>
class ConstIterator
{
  typedef ConstIterator<List> self_t;
public:
  typedef std::bidirectional_iterator_tag iterator_category;
  typedef typename List::value_type value_type;
  typedef typename List::const_pointer pointer;
  typedef typename List::const_reference reference;
private:
  typedef typename List::iterator iterator;
  typedef Node<value_type> node_t;
  typedef typename node_t::ptr_t node_ptr_t;
  typedef typename node_t::const_ptr_t node_const_ptr_t;
  friend class ObList<value_type, typename List::allocator_t>;
public:
  ConstIterator() : node_(NULL)
  {
  };
  ConstIterator(const self_t &other)
  {
    *this = other;
  };
  OB_INLINE self_t &operator =(const self_t &other)
  {
    node_ = other.node_;
    return *this;
  };
  ConstIterator(const iterator &other)
  {
    *this = other;
  };
  OB_INLINE self_t &operator =(const iterator &other)
  {
    node_ = other.node_;
    return *this;
  };
  ConstIterator(node_ptr_t node)
  {
    node_ = node;
  };
  ConstIterator(node_const_ptr_t node)
  {
    node_ = const_cast<node_ptr_t>(node);
  };
public:
  OB_INLINE reference operator *() const
  {
    // the access of end is undefined
    return node_->data;
  };
  OB_INLINE pointer operator ->() const
  {
    // the access of end is undefined
    return &(node_->data);
  };
  OB_INLINE bool operator ==(const self_t &other) const
  {
    return (node_ == other.node_);
  };
  OB_INLINE bool operator !=(const self_t &other) const
  {
    return (node_ != other.node_);
  };
  OB_INLINE self_t &operator ++()
  {
    // it will return to head if not judge end
    node_ = node_->next;
    return *this;
  };
  OB_INLINE self_t operator ++(int)
  {
    self_t tmp = *this;
    node_ = node_->next;
    return tmp;
  };
  OB_INLINE self_t &operator --()
  {
    node_ = node_->prev;
    return *this;
  };
  OB_INLINE self_t operator --(int)
  {
    self_t tmp = *this;
    node_ = node_->prev;
    return tmp;
  };
  TO_STRING_KV(K_(node));
private:
  node_ptr_t node_;
};

template <class List>
class Iterator
{
  typedef Iterator<List> self_t;
public:
  typedef std::bidirectional_iterator_tag iterator_category;
  typedef typename List::value_type value_type;
  typedef typename List::pointer pointer;
  typedef typename List::reference reference;
private:
  typedef typename List::const_iterator const_iterator;
  typedef Node<value_type> node_t;
  typedef typename node_t::ptr_t node_ptr_t;
  typedef typename node_t::const_ptr_t node_const_ptr_t;
  friend class ConstIterator<List>;
  friend class ObList<value_type, typename List::allocator_t>;
public:
  Iterator() : node_(NULL)
  {
  };
  Iterator(const self_t &other)
  {
    *this = other;
  };
  OB_INLINE self_t &operator =(const self_t &other)
  {
    node_ = other.node_;
    return *this;
  };
  explicit Iterator(node_ptr_t node)
  {
    node_ = node;
  };
  explicit Iterator(node_const_ptr_t node)
  {
    node_ = const_cast<node_ptr_t>(node);
  };
public:
  OB_INLINE reference operator *() const
  {
    // the access of end is undefined
    return node_->data;
  };
  OB_INLINE pointer operator ->() const
  {
    // the access of end is undefined
    return &(node_->data);
  };
  OB_INLINE bool operator ==(const self_t &other) const
  {
    return (node_ == other.node_);
  };
  OB_INLINE bool operator !=(const self_t &other) const
  {
    return (node_ != other.node_);
  };
  OB_INLINE self_t &operator ++()
  {
    // it will return to head if not judge end
    node_ = node_->next;
    return *this;
  };
  OB_INLINE self_t operator ++(int)
  {
    self_t tmp = *this;
    node_ = node_->next;
    return tmp;
  };
  OB_INLINE self_t &operator --()
  {
    node_ = node_->prev;
    return *this;
  };
  OB_INLINE self_t operator --(int)
  {
    self_t tmp = *this;
    node_ = node_->prev;
    return tmp;
  };
  TO_STRING_KV(K_(node));
private:
  node_ptr_t node_;
};
}

template <class T>
struct ListTypes
{
  typedef list::Node<T> AllocType;
};

template <class T, class Allocator = ObMalloc>
class ObList
{
  typedef ObList<T, Allocator> self_t;
public:
  typedef T value_type;
  typedef value_type *pointer;
  typedef value_type &reference;
  typedef const value_type *const_pointer;
  typedef const value_type &const_reference;
  typedef list::Iterator<self_t> iterator;
  typedef list::ConstIterator<self_t> const_iterator;
  typedef Allocator allocator_t;
private:
  typedef list::Node<value_type> node_t;
  typedef typename node_t::ptr_t node_ptr_t;
  typedef typename node_t::const_ptr_t node_const_ptr_t;
  typedef struct NodeHolder
  {
    node_ptr_t next;
    node_ptr_t prev;
    operator node_ptr_t()
    {
      return reinterpret_cast<node_ptr_t>(this);
    };
    operator node_const_ptr_t() const
    {
      return reinterpret_cast<node_const_ptr_t>(this);
    };
  } node_holder_t;

private:
  ObList(const self_t &other);
  self_t &operator =(const self_t &other);

public:
  ObList(Allocator &allocator) : size_(0), allocator_(allocator)
  {
    root_.next = root_;
    root_.prev = root_;
  };
  ~ObList()
  {
    reset();
  };

public:
  int push_back(const value_type &value)
  {
    int ret = OB_SUCCESS;
    node_ptr_t tmp = static_cast<node_t*>(allocator_.alloc(sizeof(node_t)));
    if (OB_ISNULL(tmp)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(construct_assign(tmp->data, value))) {
        _OB_LOG(ERROR, "copy assign failed, ret=%d", ret);
        allocator_.free(tmp);
        tmp = NULL;
      } else {
        tmp->prev = root_.prev;
        tmp->next = root_;
        root_.prev->next = tmp;
        root_.prev = tmp;
        size_++;
      }
    }
    return ret;
  };

  int push_front(const value_type &value)
  {
    int ret = OB_SUCCESS;
    node_ptr_t tmp = static_cast<node_t*>(allocator_.alloc(sizeof(node_t)));
    if (OB_ISNULL(tmp)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // use operator= temporarily, may change to construct soon.
      if (OB_FAIL(construct_assign(tmp->data, value))) {
        _OB_LOG(ERROR, "copy assign failed, ret=%d", ret);
        allocator_.free(tmp);
        tmp = NULL;
      } else {
        tmp->prev = root_;
        tmp->next = root_.next;
        root_.next->prev = tmp;
        root_.next = tmp;
        size_++;
      }
    }
    return ret;
  };

  int pop_back()
  {
    int ret = OB_SUCCESS;
    if (0 >= size_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      node_ptr_t tmp = root_.prev;
      root_.prev = tmp->prev;
      tmp->prev->next = root_;
      allocator_.free(tmp);
      size_--;
    }
    return ret;
  };

  int pop_front(value_type &value)
  {
    int ret = OB_SUCCESS;
    if (0 >= size_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      node_ptr_t tmp = root_.next;
      if (OB_FAIL(copy_assign(value, tmp->data))) {
        _OB_LOG(ERROR, "copy assign failed, ret=%d", ret);
      } else {
        root_.next = tmp->next;
        tmp->next->prev = root_;
        allocator_.free(tmp);
        tmp = NULL;
        size_--;
      }
    }
    return ret;
  }

  int pop_front()
  {
    int ret = OB_SUCCESS;
    if (0 >= size_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      node_ptr_t tmp = root_.next;
      root_.next = tmp->next;
      tmp->next->prev = root_;
      allocator_.free(tmp);
      size_--;
    }
    return ret;
  };

  int insert(iterator iter, const value_type &value)
  {
    int ret = OB_SUCCESS;
    node_ptr_t tmp = static_cast<node_t*>(allocator_.alloc(sizeof(node_t)));
    if (NULL == tmp) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(copy_assign(tmp->data, value))) {
        _OB_LOG(ERROR, "copy assign failed, ret=%d", ret);
        allocator_.free(tmp);
        tmp = NULL;
      } else {
        tmp->next = iter.node_;
        tmp->prev = iter.node_->prev;
        iter.node_->prev->next = tmp;
        iter.node_->prev = tmp;
        size_++;
      }
    }
    return ret;
  };

  int erase(iterator iter)
  {
    int ret = OB_SUCCESS;
    if (0 >= size_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (NULL == iter.node_
               || iter.node_ == (node_ptr_t)&root_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      node_ptr_t tmp = iter.node_;
      tmp->next->prev = iter.node_->prev;
      tmp->prev->next = iter.node_->next;
      allocator_.free(tmp);
      size_--;
    }
    return ret;
  };

  int erase(const value_type &value)
  {
    int ret = OB_SUCCESS;;
    iterator it = begin();
    for (; it != end(); ++it) {
      if (it.node_->data == value) {
        ret = erase(it);
        break;
      }
    }
    return ret;
  }

  T &get_first() { return root_.next->data; }
  const T &get_first() const { return root_.next->data; }
  T &get_last() { return root_.prev->data; }

  OB_INLINE iterator begin()
  {
    return iterator(root_.next);
  };
  OB_INLINE const_iterator begin() const
  {
    return const_iterator(root_.next);
  };
  OB_INLINE iterator end()
  {
    return iterator(root_);
  };
  OB_INLINE const_iterator end() const
  {
    return const_iterator(root_);
  };
  void clear()
  {
    node_ptr_t iter = root_.next;
    while (iter != root_) {
      node_ptr_t tmp = iter->next;
      allocator_.free(iter);
      iter = tmp;
    }
    root_.next = root_;
    root_.prev = root_;
    size_ = 0;
  };
  OB_INLINE void destroy()
  {
    clear();
  };
  OB_INLINE bool empty() const
  {
    return (0 == size_);
  };
  OB_INLINE int64_t size() const
  {
    return size_;
  };
  OB_INLINE void reset()
  {
    clear();
  };

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    int64_t tmp_pos = pos;
    int64_t sz = size();
    if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, tmp_pos, sz))) {
      _OB_LOG(WARN, "serialize size=%ld fail, ret=%d buf=%p buf_len=%ld pos=%ld",
                sz, ret, buf, buf_len, tmp_pos);
    } else {
      const_iterator iter;
      for (iter = begin(); iter != end(); iter++) {
        if (OB_SUCCESS != (ret = iter->serialize(buf, buf_len, tmp_pos))) {
          OB_LOG(WARN, "serialize fail", "item", *iter, K(ret), KP(buf), K(buf_len), K(tmp_pos));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        pos = tmp_pos;
      }
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t tmp_pos = pos;
    int64_t sz = 0;
    if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, tmp_pos, &sz))) {
      _OB_LOG(WARN, "deserialize size fail, ret=%d buf=%p date_len=%ld pos=%ld",
                ret, buf, data_len, tmp_pos);
    } else {
      clear();
      for (int64_t i = 0; i < sz; i++) {
        value_type item;
        if (OB_SUCCESS != (ret = item.deserialize(buf, data_len, tmp_pos))) {
          _OB_LOG(WARN, "deserialize item fail, idx=%ld ret=%d buf=%p date_len=%ld pos=%ld",
                    i, ret, buf, data_len, tmp_pos);
          break;
        }
        if (OB_SUCCESS != (ret = push_back(item))) {
          OB_LOG(WARN, "push back fail", K(item), K(i), K(ret), KP(buf), K(data_len), K(tmp_pos));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        pos = tmp_pos;
      }
    }
    return ret;
  }

  int64_t get_serialize_size(void) const
  {
    int64_t ret = 0;
    int64_t sz = size();
    ret += serialization::encoded_length_i64(sz);
    const_iterator iter;
    for (iter = begin(); iter != end(); iter++) {
      ret += iter->get_serialize_size();
    }
    return ret;
  }

  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    const_iterator iter;
    databuff_printf(buffer, length, pos, "[");
    for (iter = begin(); iter != end(); iter++) {
      databuff_print_obj(buffer, length, pos, *iter);
    }
    databuff_printf(buffer, length, pos, "]");
    return pos;
  }

  template <class C>
  int assign(const C &container)
  {
    int ret = OB_SUCCESS;
    typename C::const_iterator iter;
    clear();
    for (iter = container.begin(); iter != container.end(); iter++) {
      if (OB_SUCCESS != (ret = push_back(*iter))) {
        break;
      }
    }
    return ret;
  }

private:
  node_holder_t root_;
  int64_t size_;
  Allocator &allocator_;
};
}
}

#endif //OCEANBASE_COMMON_LIST_H_

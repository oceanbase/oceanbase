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

#ifndef OCEANBASE_CACHE_OB_CACHE_UTILS_H_
#define OCEANBASE_CACHE_OB_CACHE_UTILS_H_

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase
{
namespace common
{
template <typename Key, typename Value>
struct MapPair {
  Key first;
  Value second;
};

template <typename Key, typename Value>
struct MapNode {
  MapPair<Key, Value> pair_;
  MapNode *next_;
};

template <typename Key, typename Value, typename HashFunc = common::hash::hash_func<Key> >
class ObFixedHashMap;

template <typename Key, typename Value, typename HashFunc = common::hash::hash_func<Key> >
class ObFixedHashMapIterator
{
public:
  typedef ObFixedHashMap<Key, Value, HashFunc> HashMap;
  typedef ObFixedHashMapIterator<Key, Value, HashFunc> Iterator;

  ObFixedHashMapIterator() : hash_map_(NULL), bucket_pos_(0), node_(NULL) {}
  ObFixedHashMapIterator(const Iterator &other)
    : hash_map_(other.hash_map_), bucket_pos_(other.bucket_pos_), node_(other.node_) {}
  ObFixedHashMapIterator(const HashMap *hash_map, const int64_t bucket_pos, MapNode<Key, Value> *node)
    : hash_map_(hash_map), bucket_pos_(bucket_pos), node_(node) {}

  MapPair<Key, Value> &operator *() const
  {
    return node_->pair_;
  }

  MapPair<Key, Value> *operator ->() const
  {
    return &(node_->pair_);
  }

  bool operator ==(const Iterator &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const Iterator &iter) const
  {
    return node_ != iter.node_;
  }

  Iterator &operator ++()
  {
    if (OB_ISNULL(hash_map_)) {
      SHARE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "hash_map_ is null");
    } else if (NULL != node_ && NULL != (node_ = node_->next_)) {
      // do nothing
    } else {
      for (int64_t i = bucket_pos_ + 1; i < hash_map_->bucket_num_; i++) {
        if (NULL != (node_ = hash_map_->buckets_[i])) {
          bucket_pos_ = i;
          break;
        }
      }
      if (NULL == node_) {
        bucket_pos_ = hash_map_->bucket_num_;
      }
    }
    return *this;
  }

  Iterator operator ++(int)
  {
    Iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  const HashMap *hash_map_;
  int64_t bucket_pos_;
  MapNode<Key, Value> *node_;
};

template <typename Key, typename Value, typename HashFunc>
class ObFixedHashMap
{
public:
  typedef MapNode<Key, Value> Node;
  typedef ObFixedHashMapIterator<Key, Value, HashFunc> iterator;
  friend class ObFixedHashMapIterator<Key, Value, HashFunc>;
  ObFixedHashMap();
  virtual ~ObFixedHashMap();

  int init(const int64_t bucket_num, const int64_t node_num, const lib::ObLabel &label);
  void destroy();
  void reuse();

  int get(Key key, Value &value);
  int set(const Key &key, const Value &value);
  int erase(const Key &key);

  iterator begin();
  iterator end();
private:
  void push_free_node(Node *node);
  Node *pop_free_node();

  bool inited_;
  Node **buckets_;
  int64_t bucket_num_;
  int64_t size_;
  Node *free_nodes_;
  Node *nodes_;
  int64_t node_num_;
  HashFunc hash_func_;
};

template <typename Key, typename Value, typename HashFunc>
ObFixedHashMap<Key, Value, HashFunc>::ObFixedHashMap()
  : inited_(false), buckets_(NULL), bucket_num_(0),
    size_(0), free_nodes_(NULL), nodes_(NULL), node_num_(0)
{
}

template <typename Key, typename Value, typename HashFunc>
ObFixedHashMap<Key, Value, HashFunc>::~ObFixedHashMap()
{
  destroy();
}

template <typename Key, typename Value, typename HashFunc>
int ObFixedHashMap<Key, Value, HashFunc>::init(int64_t bucket_num,
                                               const int64_t node_num, const lib::ObLabel &label)
{
  int ret = common::OB_SUCCESS;
  ObMemAttr attr;
  attr.label_ = label;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else if (bucket_num <= 0 || node_num <= 0 || !label.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(bucket_num), K(node_num), K(label));
  } else if (NULL == (buckets_ = static_cast<Node **>(common::ob_malloc(sizeof(Node *) * bucket_num, attr)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "allocate memory failed", K(ret), K(bucket_num), K(attr));
  } else if (NULL == (nodes_ = static_cast<Node *>(common::ob_malloc(sizeof(Node) * node_num, attr)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "allocate memory failed", K(ret), K(node_num), K(attr));
  } else {
    memset(buckets_, 0, sizeof(Node *) * bucket_num);
    bucket_num_ = bucket_num;
    memset(nodes_, 0, sizeof(Node) * node_num);
    size_ = 0;
    for (int i = 0; i < node_num; ++i) {
      Node *node = static_cast<Node *>(nodes_) + i;
      node = new(node) Node();
      push_free_node(node);
    }
    node_num_ = node_num;
    inited_ = true;
  }

  if (!inited_) {
    destroy();
  }

  return ret;
}

template <typename Key, typename Value, typename HashFunc>
void ObFixedHashMap<Key, Value, HashFunc>::destroy()
{
  if (NULL != buckets_) {
    common::ob_free(buckets_);
    buckets_ = NULL;
  }

  if (NULL != nodes_) {
    common::ob_free(nodes_);
    nodes_ = NULL;
  }

  bucket_num_ = 0;
  size_ = 0;
  free_nodes_ = NULL;
  node_num_ = 0;
  inited_ = false;
}

template <typename Key, typename Value, typename HashFunc>
void ObFixedHashMap<Key, Value, HashFunc>::reuse()
{
  if (inited_) {
    for (int64_t i = 0; i < bucket_num_; ++i) {
      Node *node = buckets_[i];
      while (NULL != node) {
        Node *next = node->next_;
        push_free_node(node);
        node = next;
      }
      buckets_[i] = NULL;
    }
    size_ = 0;
  }
}

template <typename Key, typename Value, typename HashFunc>
int ObFixedHashMap<Key, Value, HashFunc>::get(Key key, Value &value)
{
  int ret = common::OB_ENTRY_NOT_EXIST;
  uint64_t hash_val = 0;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(hash_func_(key, hash_val))) {
    SHARE_LOG(WARN, "hash failed", K(ret));
  } else {
    ret = common::OB_ENTRY_NOT_EXIST;
    const int64_t pos = hash_val % bucket_num_;
    Node *node = buckets_[pos];
    while (NULL != node) {
      if (node->pair_.first == key) {
        value = node->pair_.second;
        ret = common::OB_SUCCESS;
        break;
      } else {
        node = node->next_;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename HashFunc>
typename ObFixedHashMap<Key, Value, HashFunc>::iterator ObFixedHashMap<Key, Value, HashFunc>::begin()
{
  Node *node = NULL;
  int64_t bucket_pos = 0;
  if (!inited_) {
    SHARE_LOG_RET(WARN, common::OB_NOT_INIT, "not init");
  } else {
    while (NULL == node && bucket_pos < bucket_num_) {
      node = buckets_[bucket_pos];
      if (NULL == node) {
        ++bucket_pos;
      }
    }
  }
  return iterator(this, bucket_pos, node);
}

template <typename Key, typename Value, typename HashFunc>
typename ObFixedHashMap<Key, Value, HashFunc>::iterator ObFixedHashMap<Key, Value, HashFunc>::end()
{
  return iterator(this, bucket_num_, NULL);
}

template <typename Key, typename Value, typename HashFunc>
int ObFixedHashMap<Key, Value, HashFunc>::set(const Key &key, const Value &value)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (size_ >= node_num_) {
    ret = common::OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "hashmap is full", K(ret), K_(size), K_(node_num));
  } else if (OB_FAIL(hash_func_(key, hash_val))) {
    SHARE_LOG(WARN, "hash failed", K(ret));
  } else {
    const int64_t pos = hash_val % bucket_num_;
    Node *node = buckets_[pos];
    while (NULL != node && OB_SUCC(ret)) {
      if (node->pair_.first == key) {
        ret = common::OB_ENTRY_EXIST;
        break;
      } else {
        node = node->next_;
      }
    }

    if (OB_SUCC(ret)) {
      Node *new_node = pop_free_node();
      if (NULL == new_node) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "pop_free_node return null", K(ret));
      } else {
        new_node->pair_.first = key;
        new_node->pair_.second = value;
        new_node->next_ = buckets_[pos];
        buckets_[pos] = new_node;
        ++size_;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename HashFunc>
int ObFixedHashMap<Key, Value, HashFunc>::erase(const Key &key)
{
  int ret = common::OB_ENTRY_NOT_EXIST;
  uint64_t hash_val = 0;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(hash_func_(key, hash_val))) {
    SHARE_LOG(WARN, "hash failed", K(ret));
  } else {
    ret = common::OB_ENTRY_NOT_EXIST;
    const int64_t pos = hash_val % bucket_num_;
    Node *node = buckets_[pos];
    Node *prev = NULL;
    while (NULL != node && OB_ENTRY_NOT_EXIST == ret) {
      if (node->pair_.first == key) {
        if (NULL == prev) {
          buckets_[pos] = node->next_;
        } else {
          prev->next_ = node->next_;
        }
        push_free_node(node);
        --size_;
        ret = common::OB_SUCCESS;
        break;
      } else {
        prev = node;
        node = node->next_;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename HashFunc>
void ObFixedHashMap<Key, Value, HashFunc>::push_free_node(Node *node)
{
  node->next_ = free_nodes_;
  free_nodes_ = node;
}

template <typename Key, typename Value, typename HashFunc>
typename ObFixedHashMap<Key, Value, HashFunc>::Node *ObFixedHashMap<Key, Value, HashFunc>::pop_free_node()
{
  Node *node = free_nodes_;
  if (NULL != node) {
    free_nodes_ = node->next_;
  }
  return node;
}

// sbrk many times, then reuse
template <typename T>
class ObFreeHeap
{
public:
  ObFreeHeap();
  virtual ~ObFreeHeap();
  int init(const int64_t count, const char *label);
  void destroy();

  int sbrk(T *&buf);
  int sbrk(const int64_t count, T *&buf);
  void reuse();
private:
  bool inited_;
  common::ObArenaAllocator allocator_;
  T *buf_;
  int64_t free_cnt_;
  int64_t total_cnt_;
};

template <typename T>
ObFreeHeap<T>::ObFreeHeap()
  : inited_(false), allocator_(),
    buf_(NULL), free_cnt_(0), total_cnt_(0)
{
}

template <typename T>
ObFreeHeap<T>::~ObFreeHeap()
{
  destroy();
}

template <typename T>
int ObFreeHeap<T>::init(const int64_t count, const char *label)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else if (count <= 0 || nullptr == label) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(count), K(label));
  } else {
    allocator_.set_label(label);
    buf_ = static_cast<T *>(allocator_.alloc(sizeof(T) * count));
    if (NULL == buf_) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "alloc memory failed", K(ret), K(count));
    } else {
      for (int64_t i = 0; i < count; ++i) {
        new (&buf_[i]) T();
      }
      free_cnt_ = count;
      total_cnt_ = count;
      inited_ = true;
    }
  }
  return ret;
}

template <typename T>
void ObFreeHeap<T>::destroy()
{
  if (inited_) {
    for (int64_t i = 0; i < total_cnt_; ++i) {
      buf_[i].~T();
    }
    free_cnt_ = 0;
    total_cnt_ = 0;
    allocator_.free(buf_);
    buf_ = NULL;
    inited_ = false;
  }
}

template <typename T>
int ObFreeHeap<T>::sbrk(T *&buf)
{
  const int64_t count = 1;
  return sbrk(count, buf);
}

template <typename T>
int ObFreeHeap<T>::sbrk(const int64_t count, T *&buf)
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (count <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(count));
  } else if (free_cnt_ < count) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SHARE_LOG(WARN, "buffer not enough", K(ret), K_(free_cnt), K(count));
  } else {
    free_cnt_ -= count;
    buf = buf_ + free_cnt_;
  }
  return ret;
}

template <typename T>
void ObFreeHeap<T>::reuse()
{
  if (inited_) {
    for (int64_t i = 0; i < total_cnt_; ++i) {
      buf_[i].reuse();
    }
    free_cnt_ = total_cnt_;
  }
}

template <typename T>
class ObSimpleFixedArray final : public common::ObFixedArrayImpl<T, common::ObArenaAllocator>
{
public:
  ObSimpleFixedArray();
  virtual ~ObSimpleFixedArray();

  int init(const int64_t capacity, const char *label);
  void reuse();
  void destroy();
protected:
  using ObIArray<T>::data_;
  using ObIArray<T>::count_;
  using ObFixedArrayImpl<T, common::ObArenaAllocator>::init_cnt_;
private:
  bool is_destructor_safe() const
  {
    //no need to call destructor
    return std::is_trivially_destructible<T>::value;
  }
  bool inited_;
  common::ObArenaAllocator local_allocator_;
};

template <typename T>
ObSimpleFixedArray<T>::ObSimpleFixedArray()
  : inited_(false), local_allocator_()
{
}

template <typename T>
ObSimpleFixedArray<T>::~ObSimpleFixedArray()
{
}

template <typename T>
int ObSimpleFixedArray<T>::init(const int64_t capacity, const char *label)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else if (capacity <= 0 || nullptr == label) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(capacity), K(label));
  } else {
    local_allocator_.set_label(label);
    this->set_allocator(&local_allocator_);
    ret = this->common::ObFixedArrayImpl<T, common::ObArenaAllocator>::init(capacity);
    if (OB_FAIL(ret)) {
      SHARE_LOG(WARN, "array init failed", K(ret), K(capacity));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

template <typename T>
void ObSimpleFixedArray<T>::reuse()
{
  if (NULL != data_) {
    if (is_destructor_safe()) {
    } else {
      for (int64_t i = 0; i < init_cnt_; ++i) {
        data_[i].~T();
      }
    }
  }
  count_ = 0;
}

template <typename T>
void ObSimpleFixedArray<T>::destroy()
{
  this->common::ObFixedArrayImpl<T, common::ObArenaAllocator>::destroy();
  local_allocator_.reset();
  inited_ = false;
}

class ObWashableSizeInfo
{
public:
  ObWashableSizeInfo()
    : buckets_(nullptr),
      bucket_num_(0),
      nodes_(nullptr),
      size_(0),
      count_(0),
      allocator_(nullptr),
      is_inited_(false) {}
  ~ObWashableSizeInfo()
  {
    destroy();
  }
  int init(const int64_t size, const int64_t bucket_num, ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "Init twice", K(ret), K(is_inited_));
    } else if (OB_UNLIKELY(size <= 0 || bucket_num <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument", K(ret), K(size), K(bucket_num));
    } else {
      char *buf = static_cast<char*>(allocator.alloc(sizeof(int32_t) * bucket_num + sizeof(Node) * size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Fail to alloc memory", K(ret));
      } else {
        allocator_ = &allocator;
        buckets_ = reinterpret_cast<int32_t *>(buf);
        bucket_num_ = bucket_num;
        MEMSET(buckets_, -1, sizeof(int32_t) * bucket_num);
        nodes_ = reinterpret_cast<Node *>(buf + sizeof(int32_t) * bucket_num);
        new (nodes_) Node[size];
        size_ = size;
        is_inited_ = true;
      }
    }
    return ret;
  }
  void destroy()
  {
    is_inited_= false;
    if (OB_NOT_NULL(buckets_) && OB_NOT_NULL(allocator_)) {
      allocator_->free(buckets_);
    }
    allocator_ = nullptr;
    buckets_ = nullptr;
    bucket_num_ = 0;
    nodes_ = nullptr;
    size_ = 0;
    count_ = 0;
  }
  void reuse()
  {
    MEMSET(buckets_, -1, sizeof(int32_t) * bucket_num_);
    for (int i = 0 ; i < size_ ; ++i) {
      nodes_[i].reuse();
    }
    count_ = 0;
  }
  int copy_from(const ObWashableSizeInfo &other)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "Not inited", K(ret));
    } else if (OB_UNLIKELY(!other.is_inited_ || bucket_num_ != other.bucket_num_ || size_ != other.size_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument", K(ret), K(other.is_inited_), K(bucket_num_),
                                           K(other.bucket_num_), K(size_), K(other.size_));
    } else {
      MEMCPY(buckets_, other.buckets_, sizeof(int32_t) * bucket_num_ + sizeof(Node) * size_);
      count_ = other.count_;
    }
    return ret;
  }
  int get_size(const uint64_t tenant_id, int64_t &washable_size)
  {
    int ret = OB_SUCCESS;
    washable_size = 0;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "Not inited", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
    } else {
      Node *tenant_node = find_tenant_node(tenant_id);
      if (tenant_node != nullptr) {
        washable_size = tenant_node->washable_size_;
      }
    }
    return ret;
  }
  int add_washable_size(const uint64_t tenant_id, const int64_t size)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "Not inited", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || size < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id), K(size));
    } else {
      Node *tenant_node = find_tenant_node(tenant_id, true /* add tenant */);
      if (tenant_node != nullptr) {
        tenant_node->washable_size_ += size;
      }
    }
    return ret;
  }

private:
  struct Node
  {
    Node() : tenant_id_(OB_INVALID_TENANT_ID), washable_size_(0), next_(-1) {}
    void reuse() {
      tenant_id_ = OB_INVALID_TENANT_ID;
      washable_size_ = 0;
      next_ = -1;
    }
    TO_STRING_KV(K_(tenant_id), K_(washable_size), K_(next));
    uint64_t tenant_id_;
    int64_t washable_size_;
    int32_t next_;
  };

  Node *find_tenant_node(const uint64_t tenant_id, const bool add_tenant_node = false)
  {
    Node *tenant_node = nullptr;
    int32_t idx = buckets_[tenant_id % bucket_num_];
    tenant_node = get_node(idx);
    while (tenant_node != nullptr) {
      if (tenant_id == tenant_node->tenant_id_) {
        break;
      } else {
        tenant_node = get_node(tenant_node->next_);
      }
    }
    if (nullptr == tenant_node && add_tenant_node && count_ < size_) {
      tenant_node = nodes_ + count_;
      tenant_node->tenant_id_ = tenant_id;
      tenant_node->next_ = idx;
      buckets_[tenant_id % bucket_num_] = count_++;
    }
    return tenant_node;
  }
  Node *get_node(const int32_t idx) const
  {
    Node *node = nullptr;
    if (idx >= 0 && idx < size_) {
      node = nodes_ + idx;
    }
    return node;
  }

  int32_t *buckets_;
  int64_t bucket_num_;
  Node *nodes_;
  int64_t size_;
  int32_t count_;
  ObIAllocator *allocator_;
  bool is_inited_;
};


}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_CACHE_UTILS_H_

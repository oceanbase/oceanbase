/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_HANDLE_CACHE_H_
#define OCEANBASE_STORAGE_OB_HANDLE_CACHE_H_

#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashtable.h"
#include "lib/utility/ob_macro_utils.h"


namespace oceanbase
{
namespace storage
{

template<typename Key>
struct ObHandleCacheHasher {
  static uint64_t hash(const Key &k) { return static_cast<uint64_t>(k.hash()); }
};
template<>
struct ObHandleCacheHasher<uint64_t> {
  static uint64_t hash(uint64_t k) { return k; }
};

template<typename Key, typename Handle>
class ObHandleCacheNode : public common::ObDLinkBase<ObHandleCacheNode<Key, Handle>>
{
public:
  ObHandleCacheNode() : bucket_idx_(-1) {}
  virtual ~ObHandleCacheNode() { reset(); }

  void reset()
  {
    ObDLinkBase<ObHandleCacheNode<Key, Handle>>::reset();
    handle_.reset();
    bucket_idx_ = -1;
  }

  Handle handle_;
  Key key_;
  int16_t bucket_idx_;
};

template<typename Key, typename Handle, int64_t N>
class ObHandleCache
{
  typedef ObHandleCacheNode<Key, Handle> CacheNode;
  typedef common::ObDList<CacheNode> LRUList;
public:
  ObHandleCache()
  {
    STATIC_ASSERT(N <= 8192, "number of bucket is larger than 8192");
    reset();
  }

  virtual ~ObHandleCache() {}

  void reset()
  {
    lru_list_.reset();
    for (int64_t i = 0; i < N; ++i) {
      nodes_[i].reset();
    }
    MEMSET(buckets_, -1, sizeof(buckets_));
    MEMSET(chain_, -1, sizeof(chain_));
    hold_size_ = 0;
  }

  int get_handle(const Key &key, Handle &handle)
  {
    int ret = common::OB_SUCCESS;
    const uint64_t h = ObHandleCacheHasher<Key>::hash(key);
    int16_t idx = buckets_[h & MASK];
    while (-1 < idx) {
      if (nodes_[idx].key_ == key) {
        handle = nodes_[idx].handle_;
        break;
      } else {
        idx = chain_[idx];
      }
    }
    if (-1 == idx) {
      STORAGE_LOG(DEBUG, "get handle from handle cache failed", K(key));
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      STORAGE_LOG(DEBUG, "get handle from handle cache succeed", K(key));
      lru_list_.remove(&nodes_[idx]);
      lru_list_.add_first(&nodes_[idx]);
    }
    return ret;
  }

  int put_handle(const Key &key, Handle &handle)
  {
    int ret = common::OB_SUCCESS;
    CacheNode *node = lru_list_.get_size() >= N ? lru_list_.remove_last() : nodes_ + lru_list_.get_size();
    const int16_t node_idx = static_cast<int16_t>(node - nodes_);
    int16_t *idx_ptr = NULL;
    int16_t idx = 0;
    if (-1 < node->bucket_idx_) {
      idx_ptr = &buckets_[node->bucket_idx_];
      idx = buckets_[node->bucket_idx_];
      while (OB_LIKELY(-1 < idx) && (node_idx != idx)) {
        idx_ptr = &chain_[idx];
        idx = chain_[idx];
      }
      if (OB_UNLIKELY(-1 == idx)) {
        ret = common::OB_ERR_UNEXPECTED;
        lru_list_.add_last(node);
        STORAGE_LOG(WARN, "idx can not be -1, the entry must exist", K(ret), K(key), K(node->bucket_idx_));
      } else {
        *idx_ptr = chain_[idx];
        chain_[idx] = -1;
      }
    }
    if (OB_SUCC(ret)) {
      hold_size_ += handle.get_handle_size() - node->handle_.get_handle_size();
      node->reset();
      node->key_ = key;
      node->handle_ = handle;
      node->bucket_idx_ = static_cast<int16_t>(ObHandleCacheHasher<Key>::hash(key) & MASK);
      idx_ptr = &buckets_[node->bucket_idx_];
      chain_[node_idx] = *idx_ptr;
      *idx_ptr = node_idx;
      lru_list_.add_first(node);
      STORAGE_LOG(DEBUG, "put handle succeed", K(key));
    }
    return ret;
  }

  template<typename Func>
  int for_each_key(Func &&func) const
  {
    int ret = common::OB_SUCCESS;
    const CacheNode *curr = lru_list_.get_first();
    while (OB_SUCC(ret) && nullptr != curr && curr != lru_list_.get_header()) {
      const CacheNode *next = static_cast<const CacheNode *>(curr->get_next());
      if (OB_FAIL(func(curr->key_))) {
      } else {
        curr = next;
      }
    }
    return ret;
  }

  OB_INLINE int64_t hold() const { return hold_size_; }
private:
  static const uint64_t BUCKET_SIZE = common::next_pow2(N * 2);
  static const uint64_t MASK = BUCKET_SIZE - 1;
  CacheNode nodes_[N];
  int16_t buckets_[BUCKET_SIZE];
  int16_t chain_[N];
  LRUList lru_list_;
  int64_t hold_size_;
};

}
}

#endif /* OCEANBASE_STORAGE_OB_HANDLE_CACHE_H_ */

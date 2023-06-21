
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
 */

#ifndef OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_IPP
#define OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_IPP

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "meta_programming/ob_meta_copy.h"
#include <cstdint>
#include <utility>
#ifndef OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H_IPP
#define OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H_IPP
#include "ob_recycle_multi_kvcache.h"
#endif

namespace oceanbase
{
namespace common
{
namespace cache
{

template <typename K, typename V>
ObRecycleMultiKVCache<K, V>::HashBkt::HashBkt()
: hash_bkt_(nullptr), hash_bkt_num_(0), rehash_seed_(0) {}

template <typename K, typename V>
ObRecycleMultiKVCache<K, V>::HashBkt::HashBkt(KVNode<K, V> **hash_bkt,
                                              const int64_t hash_bkt_num,
                                              const int64_t rehash_seed)
:hash_bkt_(hash_bkt),
hash_bkt_num_(hash_bkt_num),
rehash_seed_(rehash_seed) {
  for (int64_t idx = 0; idx < hash_bkt_num_; ++idx) {
    hash_bkt_[idx] = nullptr;
  }
}

template <typename K, typename V>
ObRecycleMultiKVCache<K, V>::HashBkt::~HashBkt() {
  new (this) HashBkt();
}

template <typename K, typename V>
void ObRecycleMultiKVCache<K, V>::HashBkt::insert(KVNode<K, V> *node) {
  KVNode<K, V> **hash_prev_node_next_ptr = nullptr;
  KVNode<K, V> *bkt_next = nullptr;
  KVNode<K, V> *list = find_list(node->k_, hash_prev_node_next_ptr, bkt_next);
  if (nullptr != list) {// find a list
    OB_ASSERT(OB_NOT_NULL(list->hash_bkt_prev_next_ptr_));
    list->key_node_prev_->key_node_next_ = node;
    node->key_node_prev_ = list->key_node_prev_;
    node->key_node_next_ = list;
    list->key_node_prev_ = node;
  } else {
    *hash_prev_node_next_ptr = node;
    node->hash_bkt_next_ = bkt_next;
    node->hash_bkt_prev_next_ptr_ = hash_prev_node_next_ptr;
    if (nullptr != bkt_next) {// maybe no next
      bkt_next->hash_bkt_prev_next_ptr_ = &(node->hash_bkt_next_);
    }
    node->key_node_prev_ = node;
    node->key_node_next_ = node;
  }
}

template <typename K, typename V>
void ObRecycleMultiKVCache<K, V>::HashBkt::remove(KVNode<K, V> *node) {
  if (node->key_node_prev_ == node) {// last node in list
    OB_ASSERT(node->key_node_next_ == node);
    OB_ASSERT(OB_NOT_NULL(node->hash_bkt_prev_next_ptr_));
    *node->hash_bkt_prev_next_ptr_ = node->hash_bkt_next_;// remove this list from bucket
    if (nullptr != node->hash_bkt_next_) {
      node->hash_bkt_next_->hash_bkt_prev_next_ptr_ = node->hash_bkt_prev_next_ptr_;
    }
  } else {// not last node, remove it and check if replay list head
    node->key_node_prev_->key_node_next_ = node->key_node_next_;
    node->key_node_next_->key_node_prev_ = node->key_node_prev_;
    if (OB_UNLIKELY(nullptr != node->hash_bkt_prev_next_ptr_)) {
      KVNode<K, V> *new_list_head = node->key_node_next_;
      OB_ASSERT(new_list_head->hash_bkt_prev_next_ptr_ == nullptr);
      OB_ASSERT(new_list_head->hash_bkt_next_ == nullptr);
      *node->hash_bkt_prev_next_ptr_ = new_list_head;
      new_list_head->hash_bkt_prev_next_ptr_ = node->hash_bkt_prev_next_ptr_;
      new_list_head->hash_bkt_next_ = node->hash_bkt_next_;
      if (nullptr != node->hash_bkt_next_) {// maybe no next
        node->hash_bkt_next_->hash_bkt_prev_next_ptr_ = &(new_list_head->hash_bkt_next_);
      }
    }
  }
  // clear node self info
  node->key_node_prev_ = nullptr;
  node->key_node_next_ = nullptr;
  node->hash_bkt_prev_next_ptr_ = nullptr;
  node->hash_bkt_next_ = nullptr;
}

template <typename K, typename V>
KVNode<K, V> *ObRecycleMultiKVCache<K, V>::HashBkt::find_list(const K &key,
                                                              KVNode<K, V> **&hash_bkt_prev_node_next_ptr,
                                                              KVNode<K, V> *&bkt_next) const {
  KVNode<K, V> *list = nullptr;
  int64_t bkt_idx = re_hash_idx_(key);
  hash_bkt_prev_node_next_ptr = &hash_bkt_[bkt_idx];
  KVNode<K, V> *iter = hash_bkt_[bkt_idx];
  while (nullptr != iter && nullptr == list) {
    if (iter->k_ < key) {// keep iter next to find proper position
      hash_bkt_prev_node_next_ptr = &iter->hash_bkt_next_;
    } else if (iter->k_ == key) {// node's key is same with this list, insert into tail of it
      list = iter;
    } else {// no same key, insert into hash bkt between two nodes
      break;
    }
    iter = iter->hash_bkt_next_;
  }
  OCCAM_LOG(DEBUG, "TO COMPILER: DO NOT OPTIMIZE IT", K(bkt_idx));
  bkt_next = iter;
  return list;
}

template <typename K, typename V>
uint64_t ObRecycleMultiKVCache<K, V>::HashBkt::re_hash_idx_(const K &key) const {
#ifdef UNITTEST_DEBUG
  return key.hash();
#else
  uint64_t hash = key.hash();
  hash = murmurhash(&rehash_seed_, sizeof(rehash_seed_), hash);
  return hash % hash_bkt_num_;
#endif
}

template <typename K, typename V>
int64_t ObRecycleMultiKVCache<K, V>::round_end_(int64_t offset) const
{
  return ((offset/buffer_len_) + 1) * buffer_len_;
}

template <typename K, typename V>
ObRecycleMultiKVCache<K, V>::~ObRecycleMultiKVCache()
{
  OCCAM_LOG(DEBUG, "destructed", K(*this));
  if (OB_NOT_NULL(allocator_)) {
    while (OB_BUF_NOT_ENOUGH != recycle_one_());// keep recycling until meet buf not enough
    allocator_->free(total_buffer_);
    new (this) ObRecycleMultiKVCache<K, V>();
  }
}

template <typename K, typename V>
template <int N>
int ObRecycleMultiKVCache<K, V>::init(const char (&mem_tag)[N],// used for alloc static memory
                                      ObIAllocator &alloc,
                                      const int64_t recycle_buffer_size,
                                      const int64_t hash_idx_bkt_num,
                                      const int64_t rehash_seed)
{
  #define PRINT_WRAPPER K(ret), K(mem_tag), K(recycle_buffer_size), K(hash_idx_bkt_num), K(rehash_seed), K(*this)
  int ret = OB_SUCCESS;
  void *allocated_ptr = nullptr;
  int64_t total_size = hash_idx_bkt_num * sizeof(KVNode<K, V> *) + recycle_buffer_size;
  char *p_buffer = nullptr;
  if (OB_ISNULL(mem_tag) || recycle_buffer_size < 1 || hash_idx_bkt_num < 1) {
    ret = OB_INVALID_ARGUMENT;
    OCCAM_LOG(WARN, "invalid argument", PRINT_WRAPPER);
  } else if (OB_NOT_NULL(buffer_)) {
    ret = OB_INIT_TWICE;
    OCCAM_LOG(WARN, "init twice", PRINT_WRAPPER);
  } else if (nullptr == (p_buffer = (char *)alloc.alloc(total_size, ObMemAttr(common::OB_SERVER_TENANT_ID, lib::ObLabel(mem_tag))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OCCAM_LOG(WARN, "fail to alloc memory", PRINT_WRAPPER);
  } else {
    total_buffer_ = p_buffer;// record this buffer pointer, for free it when destructed
    new (&hash_bkt_) HashBkt((KVNode<K, V> **)p_buffer, hash_idx_bkt_num, rehash_seed);
    buffer_ = p_buffer + hash_idx_bkt_num * sizeof(KVNode<K, V> *);
    buffer_len_ = recycle_buffer_size;
    offset_next_to_appended_ = 0;
    offset_can_write_end_ = buffer_len_;
    allocator_ = &alloc;
    OCCAM_LOG(DEBUG, "succ to init", PRINT_WRAPPER, KP(p_buffer));
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename Value>
int ObRecycleMultiKVCache<K, V>::construct_assign_(const K &key,
                                                   Value &&data,
                                                   InnerMockAllocator &alloc,
                                                   KVNode<K, V> *&p_new_node)
{
  #define PRINT_WRAPPER K(ret), K(key), K(data), K(alloc), KP(p_new_node), KP(p_new_node)
  int ret = OB_SUCCESS;
  p_new_node = (KVNode<K, V> *)alloc.alloc(sizeof(KVNode<K, V>));
  if (OB_ISNULL(p_new_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OCCAM_LOG(DEBUG, "fail to alloc memory when construct KVNode", PRINT_WRAPPER);
  } else {
    new (p_new_node) KVNode<K, V>();
    p_new_node->k_ = key;
    if (OB_FAIL(meta::move_or_copy_or_assign(std::forward<Value>(data), p_new_node->v_, alloc))) {
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        OCCAM_LOG(DEBUG, "fail to alloc memory when call value assign", PRINT_WRAPPER);
      } else {
        OCCAM_LOG(WARN, "fail to call value assign", PRINT_WRAPPER);
      }
      p_new_node->~KVNode<K, V>();
      p_new_node = nullptr;
    } else {
      OCCAM_LOG(DEBUG, "succ to alloc new node", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
int ObRecycleMultiKVCache<K, V>::recycle_one_()
{
  int ret = OB_SUCCESS;
  KVNode<K, V> *cur = (KVNode<K, V> *)&buffer_[offset_can_write_end_ % buffer_len_];
  if (offset_can_write_end_ - offset_next_to_appended_ == buffer_len_) {
    ret = OB_BUF_NOT_ENOUGH;
    OCCAM_LOG(DEBUG, "can not recycle", K(ret), K(*this));
  } else {
    offset_can_write_end_ += sizeof(KVNode<K, V>) + cur->extre_buffer_size_;
    if (offset_can_write_end_ == offset_reserved_) {
      offset_can_write_end_ = round_end_(offset_can_write_end_);
    }
    OB_ASSERT(offset_can_write_end_ - offset_next_to_appended_ <= buffer_len_);
    OCCAM_LOG(DEBUG, "recycle one", K(ret), K(*this), K(*cur));
    hash_bkt_.remove(cur);
    inner_mock_allocator_.set_can_free_area((char *)cur + sizeof(KVNode<K, V>), cur->extre_buffer_size_);
    cur->~KVNode<K, V>();
    ++recycle_number_;
  }
  return ret;
}

template <typename K, typename V>
template <typename Value>
int ObRecycleMultiKVCache<K, V>::append(const K &key, Value &&value)
{
  #define PRINT_WRAPPER K(ret), K(tmp_ret), K(key), K(value), K(*this)
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSpinLockGuard lg(lock_);
  if (OB_ISNULL(buffer_)) {
    ret = OB_NOT_INIT;
    OCCAM_LOG(WARN, "not init", PRINT_WRAPPER);
  } else {
    KVNode<K, V> *p_new_node = nullptr;
    do {
      int64_t round_end_offset = round_end_(offset_next_to_appended_);
      int64_t max_can_alloc_len_in_this_round = std::min(offset_can_write_end_, round_end_offset) - offset_next_to_appended_;
      inner_mock_allocator_.set_can_alloc_area(&buffer_[offset_next_to_appended_ % buffer_len_], max_can_alloc_len_in_this_round);
      if (OB_TMP_FAIL(construct_assign_(key, std::forward<Value>(value), inner_mock_allocator_, p_new_node))) {
        OCCAM_LOG(DEBUG, "fail to appended", PRINT_WRAPPER);
        if (OB_ALLOCATE_MEMORY_FAILED == tmp_ret) {
          if (offset_can_write_end_ - offset_next_to_appended_ == buffer_len_) {// no data lefted
            ret = OB_BUF_NOT_ENOUGH;
          } else if (offset_can_write_end_ >= round_end_offset) {// this round not has enough memory to append this new one
            offset_reserved_ = offset_next_to_appended_ + buffer_len_;
            offset_next_to_appended_ = round_end_offset;
          } else if (OB_FAIL(recycle_one_())) {
            OCCAM_LOG(WARN, "fail to do recycle", PRINT_WRAPPER);
          }
        } else {
          ret = tmp_ret;
          OCCAM_LOG(WARN, "user assign functon report error", PRINT_WRAPPER);
        }
      } else {
        offset_next_to_appended_ += inner_mock_allocator_.allocated_size_;
        p_new_node->extre_buffer_size_ = (uint32_t)(inner_mock_allocator_.allocated_size_ - sizeof(KVNode<K, V>));
        hash_bkt_.insert(p_new_node);// build index
        ++write_number_;
      }
    } while (OB_SUCC(ret) && OB_ALLOCATE_MEMORY_FAILED == tmp_ret);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
template <typename OP>// int OP(const V &);
int ObRecycleMultiKVCache<K, V>::for_each(const K &key, OP &&op) const// optimized for point select
{
  int ret = OB_SUCCESS;
  KVNode<K, V> **_1;
  KVNode<K, V> *_2;
  ObSpinLockGuard lg(lock_);
  KVNode<K, V> *list = hash_bkt_.find_list(key, _1, _2);
  auto iter = list;
  if (OB_ISNULL(list)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    do {
      if (OB_FAIL(op(iter->v_))) {
        OCCAM_LOG(DEBUG, "do user op faled on iter", K(key), K(*iter), K(typeid(op).name()), K(*this));
      }
      iter = iter->key_node_next_;
    } while (list != iter && OB_SUCC(ret));
  }
  return ret;
}

template <typename K, typename V>
template <typename OP>// int OP(const K &, const V &);
int ObRecycleMultiKVCache<K, V>::for_each(OP &&op) const// optimized for point select
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lg(lock_);
  int64_t offset = offset_can_write_end_;
  while (OB_SUCC(ret) && offset - offset_next_to_appended_ != buffer_len_) {
    KVNode<K, V> *cur = (KVNode<K, V> *)&buffer_[offset % buffer_len_];
    if (OB_FAIL(op(cur->k_, cur->v_))) {
      OCCAM_LOG(WARN, "fail to apply user op on KVNode", K(*cur), K(*this));
    } else {
      offset += sizeof(KVNode<K, V>) + cur->extre_buffer_size_;
      if (offset == offset_reserved_) {
        offset = round_end_(offset);
      }
    }
    OB_ASSERT(offset - offset_next_to_appended_ <= buffer_len_);
  }
  return ret;
}

}
}
}
#endif
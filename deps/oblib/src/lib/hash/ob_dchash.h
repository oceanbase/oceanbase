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

#ifndef OCEANBASE_HASH_OB_DCHASH_H_
#define OCEANBASE_HASH_OB_DCHASH_H_

#include "lib/hash/ob_darray.h"
#include "lib/queue/ob_link.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/metrics/ob_counter.h"
#include "lib/utility/utility.h"
#include <typeinfo>

namespace oceanbase
{
namespace common
{
struct HashNode: public ObLink
{
  HashNode(): ObLink(), hash_(0) {}
  explicit HashNode(uint64_t hash): ObLink(), hash_(hash) {}
  ~HashNode() {}
  uint64_t load_hash() { return ATOMIC_LOAD(&hash_); }
  void set_linked() { ATOMIC_STORE(&hash_, load_hash() | 1ULL); }
  bool is_linked() { return hash_ & 1ULL; }
  bool is_deleted() { return is_last_bit_set((uint64_t)next_); }
  bool is_dummy_node() { return (hash_ & 3ULL) != 2ULL; }
  int compare(HashNode* that) {
    int ret = 0;
    uint64_t h1 = this->load_hash();
    uint64_t h2 = that->load_hash();
    if (h1 > h2) {
      ret = 1;
    } else if (h1 < h2) {
      ret = -1;
    } else {
      ret = 0;
    }
    return ret;
  }
  uint64_t hash_;
};

class DCArray
{
public:
  DCArray(DCArray* parent, uint64_t capacity):
      parent_(parent), capacity_(capacity), shift_(calc_shift(capacity_)), fill_idx_(0) {
    for(uint64_t i = 0; i < capacity_; i++) {
      new(array_ + i)HashNode(i<<shift_);
    }
  }
  ~DCArray() {}
  uint64_t capacity() { return capacity_; }
  static uint64_t calc_nbytes(uint64_t capacity) { return capacity * sizeof(HashNode) + sizeof(DCArray); }
  bool is_parent_retired() { return NULL == ATOMIC_LOAD(&parent_); }
  DCArray* retire_parent() { return ATOMIC_TAS(&parent_, NULL); }
  HashNode* locate_pre(uint64_t hash) {
    HashNode* prev = NULL;
    if (NULL == (prev = this->locate_prev_(hash))) {
      DCArray* parent = NULL;
      if (NULL != (parent = ATOMIC_LOAD(&parent_))) {
        prev = parent->locate_prev_(hash);
      }
    }
    return prev;
  }

  void init(HashNode* root, HashNode* tail) {
    HashNode* prev = root;
    for(uint64_t i = 0; i < capacity_; i++) {
      prev->next_ = array_ + i;
      prev = array_ + i;
      array_[i].set_linked();
    }
    prev->next_ = tail;
    fill_idx_ = capacity_;
  }

  DCArray* copy_from_prev_array(HashNode* root, int64_t batch_size) {
    DCArray* retired_array = NULL;
    DCArray* parent = NULL;
    uint64_t copy_start = -1;
    if (NULL != (parent = ATOMIC_LOAD(&parent_))) {
      if(ATOMIC_LOAD(&fill_idx_) < parent->capacity_
         && (copy_start = ATOMIC_FAA(&fill_idx_, batch_size)) < parent->capacity_) {
        for(int64_t i = 0; i < batch_size; i++) {
          copy_range(parent, copy_start + i, root);
        }
        if (copy_start + batch_size == parent->capacity_) {
          retired_array = this;
        }
      }
    }
    return retired_array;
  }

private:
  static uint64_t calc_shift(uint64_t capacity)
  {
    OB_ASSERT(0ULL != capacity);
    return __builtin_clzll(capacity) + 1;
  }
  HashNode* locate(uint64_t hash) {
    return array_ + (hash >> shift_);
  }
  HashNode* locate_prev_(uint64_t hash) {
    HashNode* prev = locate(hash);
    return (prev && prev->is_linked())? prev: NULL;
  }
  HashNode* array_node_get_prev(uint64_t idx, HashNode* root) {
    return (0 == idx)? root: locate_pre(idx - 1);
  }

  void copy_range(DCArray* parent, uint64_t start_idx, HashNode* root) {
    uint64_t shift = parent->shift_;
    uint64_t start = start_idx << shift; // care int overflow
    while(0 != parent->delete_one(array_node_get_prev(start, root), start_idx))
      ;
    if (0 == (start & ((1ULL<<shift_) - 1))) {
      for(uint64_t i = 0; i < (1ULL<<shift); i += (1ULL<<shift_)) {
        uint64_t idx = (start + i)>>shift_;
        while (0 != this->insert_one(array_node_get_prev(start, root), idx))
          ;
        array_[idx].set_linked();
      }
    }
  }
  int delete_one(HashNode* prev, uint64_t idx) {
    int err = 0;
    if (NULL == prev) {
      err = -EAGAIN;
    } else {
      HashNode* deleted = NULL;
      err = _ol_del(prev, array_ + idx, deleted);
    }
    return err;
  }
  int insert_one(HashNode* prev, uint64_t idx) {
    int err = 0;
    if (NULL == prev) {
      err = -EAGAIN;
    } else {
      err = _ol_insert(prev, array_ + idx);
    }
    return err;
  }

private:
  DCArray* parent_;
  uint64_t capacity_;
  uint64_t shift_;
  uint64_t fill_idx_;
  HashNode array_[0];
};

template<typename key_t>
struct KeyHashNode: public HashNode
{
  KeyHashNode(): HashNode(), key_() {}
  explicit KeyHashNode(const key_t& key): HashNode(calc_hash(key)), key_(key) {}
  ~KeyHashNode() {}
  KeyHashNode* set(const key_t& key) {
    hash_ = calc_hash(key);
    key_ = key;
    return this;
  }
  int compare(KeyHashNode* that) {
    int ret = 0;
    if (this->hash_ > that->hash_) {
      ret = 1;
    } else if (this->hash_ < that->hash_) {
      ret = -1;
    } else {
      ret = this->key_.compare(that->key_);
    }
    return ret;
  }
  static uint64_t calc_hash(const key_t& key) { return (key.hash() | 2ULL) & (~1ULL); }
  key_t key_;
};

template<typename key_t, int64_t SHRINK_THRESHOLD = 8>
class DCHash
{
public:
  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  typedef ObQSync QSync;
  //typedef EstimateCounter NodeCounter;
  typedef ObSimpleCounter NodeCounter;
  typedef DCArray Array;
  typedef KeyHashNode<key_t> Node;
  enum { BATCH_SIZE = 64 };
  class Handle
  {
  public:
    Handle(DCHash& host, int& err, int node_change_count):
        host_(host), qsync_(host.get_qsync()), ref_(-1), array_(NULL),
        err_(err), node_change_count_(node_change_count) {}
    ~Handle() { retire(err_, node_change_count_); }
    int search_pre(uint64_t hash, HashNode*& pre) {
      int err = 0;
      acquire_ref();
      if (OB_UNLIKELY(NULL == (array_ = host_.search_pre(hash, pre)))) {
        err = -ENOMEM;
      } else if (OB_UNLIKELY(NULL == pre)) {
        err = -EAGAIN;
      }
      return err;
    }
  private:
    void retire(int err, int64_t x) {
      if (0 == err) {
        host_.change_node_count(x);
      }
      Array* filled_array = host_.do_pending_task(array_);
      release_ref();
      if (NULL != filled_array) {
        qsync_.sync(); // wait until all pending tasks were finished
        Array* retired_array = filled_array->retire_parent();
        qsync_.sync(); // wait until all access threads were quiescent.
        host_.destroy_array(retired_array);
      }
    }
    void acquire_ref() { ref_ = qsync_.acquire_ref(); }
    void release_ref() { qsync_.release_ref(ref_); }
  private:
    DCHash& host_;
    QSync& qsync_;
    int64_t ref_;
    Array* array_;
    int& err_;
    int node_change_count_;
  };
  friend class Handle;
public:
  DCHash(IArrayAlloc& alloc, int64_t min_size, int64_t max_size):
      lock_(common::ObLatchIds::HASH_MAP_LOCK), alloc_(alloc), root_(0),
      tail_(UINT64_MAX), cur_array_(NULL),
      min_size_(min_size), max_size_(max_size), target_size_(min_size)
  {
    if (OB_UNLIKELY(min_size_ < BATCH_SIZE)) {
      _OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "min_size(%ld) is smaller than BATCH_SIZE(%u)", min_size_, BATCH_SIZE);
      min_size_ = BATCH_SIZE;
    }
    if (OB_UNLIKELY(min_size_ > max_size_)) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "bad min/max size", K(min_size_), K(max_size_));
    }
  }
  ~DCHash() { destroy(); }

  void destroy() {
    if (NULL != cur_array_) {
      destroy_array(cur_array_);
      cur_array_ = NULL;
    }
  }
  int get(const key_t& key, Node*& node) {
    int err = 0;
    HashNode* pre = NULL;
    Node key_node(key);
    Handle handle(*this, err, 0);
    if (0 == (err = handle.search_pre(key_node.hash_, pre))) {
      err = ol_get((Node*)pre, &key_node, node);
    }
    return err;
  }

  int insert(const key_t& key, Node* node) {
#ifdef ENABLE_DEBUG_LOG
    common::ObTimeGuard tg("dc_hash::insert", 100 * 1000);
#endif
    int err = 0;
    HashNode* pre = NULL;
    Node key_node(key);
    {
      Handle handle(*this, err, 1);
      if (0 == (err = handle.search_pre(key_node.hash_, pre))) {
        err = _ol_insert((Node*)pre, node);
      }
    }
#ifdef ENABLE_DEBUG_LOG
    if (tg.get_diff() > 100000) {
      _OB_LOG(INFO, "ObDCHash insert cost too much time, click diff (%s)", to_cstring(tg));
    }
#endif

    return err;
  }

  int del(const key_t& key, Node*& node) {
    int err = 0;
    HashNode* pre = NULL;
    Node key_node(key);
    Handle handle(*this, err, -1);
    if (0 == (err = handle.search_pre(key_node.hash_, pre))) {
      err = _ol_del((Node*)pre, &key_node, node);
    }
    return err;
  }

  int64_t count() const { return node_count_.value(); }
  Node* next(Node* node) {
    get_qsync().acquire_ref();
    while(NULL != (node = next_node(node))
          && node->is_dummy_node()) {
      // add usleep for dummy node test
      // usleep(1 * 1000 * 1000);
    }
    get_qsync().release_ref();
    return node;
  }
private:
  static uint64_t next2n(const uint64_t x)
  {
    return x <= 2 ? x : (1UL << 63) >> (__builtin_clzll(x - 1) - 1);
  }

  Node* next_node(Node* node) {
    Node* next = NULL;
    if (NULL == node) {
      next = (Node*)&root_;
    } else if (is_last_bit_set((uint64_t)(next = (Node*)ATOMIC_LOAD(&node->next_)))) {
      Node* prev = NULL;
      while(NULL != search_pre(node->hash_, (HashNode*&)prev) && NULL == prev)
        ;
      if (NULL != prev) {
        next = ol_search(prev, node, prev);
      }
    }
    return next;

  }
  Array* search_pre(uint64_t hash, HashNode*& pre) {
    Array* array = NULL;
    if (NULL != (array = alloc_and_init_cur_array())) {
      pre = array->locate_pre(hash);
    }
    return array;
  }

  void change_node_count(int64_t x) {
    int64_t cur_size = 0;
    node_count_.inc(x);
    cur_size = node_count_.value();
    cur_size = std::max(cur_size, min_size_);
    cur_size = std::min(cur_size, max_size_);
    uint64_t target_size = ATOMIC_LOAD(&target_size_);
    const int64_t shrink_threshold = (SHRINK_THRESHOLD > 0 ? SHRINK_THRESHOLD : 1);
    if (cur_size > (int64_t)target_size ||
        cur_size < (int64_t)target_size/shrink_threshold) {
      ATOMIC_STORE(&target_size_, next2n(cur_size));
      _OB_LOG(INFO, "DCHash: change_size: %s this=%p node_count=%ld new_size=%ld",
           typeid(key_t).name(), this, cur_size, next2n(cur_size));
    }
  }

  Array* do_pending_task(Array* cur_array) {
    Array* filled_array = NULL;
    if (NULL != cur_array) {
      filled_array = cur_array->copy_from_prev_array(&root_, BATCH_SIZE);
      uint64_t target_size = 0;
      if (cur_array->capacity() != (target_size = ATOMIC_LOAD(&target_size_))
          && cur_array->is_parent_retired()) {
        if (OB_SUCCESS == lock_.trylock()) {
          if (ATOMIC_LOAD(&cur_array_) == cur_array) {
            Array* new_array = NULL;
            if(NULL != (new_array = alloc_array(cur_array, target_size))) {
              ATOMIC_STORE(&cur_array_, new_array);
            }
          }
          (void)lock_.unlock();
        }
      }
    }
    return filled_array;
  }

  Array* alloc_and_init_cur_array() {
    common::ObTimeGuard tg("dc_hash::insert", 100 * 1000);
    Array* array = NULL;
    if (NULL == (array = ATOMIC_LOAD(&cur_array_))) {
      tg.click();
      LockGuard lock_guard(lock_);
      tg.click();
      if (NULL == (array = ATOMIC_LOAD(&cur_array_))) {
        if (NULL != (array = alloc_array(NULL, min_size_))) {
          tg.click();
          array->init(&root_, &tail_);
          tg.click();
          ATOMIC_STORE(&cur_array_, array);
        }
      }
    }
    if (tg.get_diff() > 100000) {
      _OB_LOG(INFO, "ObDCHash alloc and init array cost too much time, click diff (%s)", to_cstring(tg));
    }
    return array;
  }

  Array* alloc_array(Array* prev, int64_t size) {
    Array* array = NULL;
    if (NULL != (array = (Array*)alloc_.alloc(Array::calc_nbytes(size)))) {
      _OB_LOG(INFO, "DCHash: alloc_array: %s this=%p array=%p array_size=%ld prev_array=%p",
           typeid(key_t).name(), this, array, size, prev);
      new(array)Array(prev, size);
    }
    return array;
  }

  void destroy_array(Array* array) {
    if (NULL != array) {
      _OB_LOG(INFO, "DCHash: destroy_array: %s this=%p array=%p array_size=%ld",
           typeid(key_t).name(), this, array, array->capacity());
      alloc_.free(array);
      array = NULL;
    }
  }

  static QSync& get_qsync() {
    static QSync qsync;
    return qsync;
  }
private:
  Lock lock_;
  IArrayAlloc& alloc_;
  HashNode root_ CACHE_ALIGNED;
  HashNode tail_ CACHE_ALIGNED;
  Array* cur_array_ CACHE_ALIGNED;
  int64_t min_size_;
  int64_t max_size_;
  int64_t target_size_;
  NodeCounter node_count_ CACHE_ALIGNED;
};
}; // end namespace common
}; // end namespace oceanbase


#endif /* OCEANBASE_HASH_OB_DCHASH_H_ */

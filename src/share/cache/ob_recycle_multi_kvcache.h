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

/**
 * ObRecycleMultiKVCache
 *
 * ObRecycleMultiKVCache is a recycle cache buffer contains a series of KV pair data
 * (the number of pairs depends on the memory size pre-allocated and the run-time memory of each node costs).
 * There are some core advantages ObRecycleMultiKVCache designed for:
 * 1. no dynamic memory actually allocated guaranteed, this is very important for runtime performance.
 * 2. KV pair data is appended only, so makes write cost O(1) time,
 *    and pre-allocated memory in init() will be recycle used, the oldest KV pair data is recycled for the new one.
 * 3. hash index makes point read cost O(1) time.
 * 4. if V has a pointer inside and has to be deep-copied, ObRecycleMultiKVCache will pass a mock inner allocator to it,
 *    and acutually use the recycle buffer to alloc memory, if there is no enough memory, willl recycle the oldest data
 *    until the buffer is enough.
 *    + if V's copy action need extra memory, the function signature should be like: int V::assign(ObIAllocator &, const V &) const.
 *    + if V's copy action do not need allocator, it's ok with meta programing's help.
 *    + if V's just support move action, it's ok if you passed a rvalue.
 * 5. one key can map to multi values.
 * 6. tamplate implementation of course.
 *
 * Memory usage:
 *   - the dynamic memory and allocator is specified in init() interface, and no extra memory allocated guaranteed,
 *     ObRecycleMultiKVCache is RAII designed, the only thing you should care about lifetime is the allocator you passed
 *     to ObRecycleMultiKVCache shou alive longer than ObRecycleMultiKVCache.
 *
 * Concurrency:
 *   - there is no way to write a same lock-free structure, but lock will never be the performance problem,
 *     if you find too much cpu wasted on the lock, maybe you should consider about create multi ObRecycleMultiKVCache
 *     instances, and seperate the write concurrency.
 *
 * Manual:
 *   - Interface
 *     1. construction & destruction & copy & move
 *     + default construction & default destruction:
 *         ObRecycleMultiKVCache()
 *         ~ObRecycleMultiKVCache()
 *     + copy & move:
 *         not allowed
 *     + init:
 *         init(const char (&mem_tag)[N],// tag used for alloc static memory
 *              ObIAllocator &alloc,// the real allocator to alloc memory
 *              const int64_t recycle_buffer_size,// to store kv pair
 *              const int64_t hash_idx_bkt_num,// to build read index
 *              const int64_t rehash_seed)// to recalculate hash value
 *     2. write
 *     + apped KV pair, support r-value:
 *         template <typename T>
 *         int append(const K &key, T &&event);
 *     3. read
 *     + read by key, OP must could used like int op(const V &):
 *         template <typename OP>
 *         int for_each(const K &key, OP &&op) const;
 *     + scan read all, OP must could used like int OP(const K &, const V &)
 *         template <typename OP>
 *         int for_each(OP &&op) const;
 *
 *  - Contact  for help.
 */

#ifndef OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H
#define OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "meta_programming/ob_type_traits.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "util/easy_time.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "common/meta_programming/ob_meta_compare.h"

namespace oceanbase
{
namespace common
{
namespace cache
{

template <typename K, typename V>
struct KVNode {
  KVNode()
  : hash_bkt_prev_next_ptr_(nullptr),
  hash_bkt_next_(nullptr),
  key_node_prev_(nullptr),
  key_node_next_(nullptr),
  extre_buffer_size_(0) {}
  KVNode(const K &key, const V &value) : KVNode() { k_ = key; v_ = value; }// for unittest
  ~KVNode() {
    hash_bkt_prev_next_ptr_ = nullptr;
    hash_bkt_next_ = nullptr;
    key_node_prev_ = nullptr;
    key_node_next_ = nullptr;
    extre_buffer_size_ = 0;
  }
  TO_STRING_KV(KP_(hash_bkt_prev_next_ptr), KP_(hash_bkt_next), KP_(key_node_prev), KP_(key_node_next),\
               K_(extre_buffer_size), K_(k), K_(v));
  K k_;
  V v_;
  KVNode **hash_bkt_prev_next_ptr_;// to record hash conflict next Node
  KVNode *hash_bkt_next_;// to record hash conflict next Node
  KVNode *key_node_prev_;// to record multi key value
  KVNode *key_node_next_;// to record multi key value
  uint32_t extre_buffer_size_;// dynamic alloc memory size
};

struct InnerMockAllocator final : public ObIAllocator
{
  InnerMockAllocator()
  : can_alloc_ptr_(nullptr),
  can_alloc_len_(0),
  allocated_size_(0),
  can_free_ptr_(nullptr),
  can_free_len_(0) {}
  void set_can_alloc_area(char *can_alloc_ptr, const int64_t len) {
    can_alloc_ptr_ = can_alloc_ptr;
    can_alloc_len_ = len;
    allocated_size_ = 0;
  }
  void set_can_free_area(char *can_free_ptr, const int64_t len) {
    can_free_ptr_ = can_free_ptr;
    can_free_len_ = len;
  }
  virtual void *alloc(const int64_t size) override {
    void *alloc_ptr = nullptr;
    if (size > UINT32_MAX) {
      OCCAM_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "alloc memory too large", K(size), K(*this), K(lbt()));
    } else if (size > can_alloc_len_ - allocated_size_) {
      OCCAM_LOG_RET(DEBUG, OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed", K(size), K(*this));
    } else {
      alloc_ptr = can_alloc_ptr_ + allocated_size_;
      allocated_size_ += size;
      set_can_free_area(can_alloc_ptr_, allocated_size_);
    }
    return alloc_ptr;
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSED(attr);
    return alloc(size);
  }
  virtual void free(void *ptr) override {
    OB_ASSERT(ptr >= can_free_ptr_ && ptr < can_free_ptr_ + can_free_len_);
  }
  TO_STRING_KV(KP_(can_alloc_ptr), K_(can_alloc_len), K_(allocated_size), KP_(can_free_ptr), K_(can_free_len));
  char *can_alloc_ptr_;
  int64_t can_alloc_len_;
  int64_t allocated_size_;
  char *can_free_ptr_;
  int64_t can_free_len_;
};

template <typename K, typename V>
class ObRecycleMultiKVCache {// FIFO write, so can be recycled, and no extra memory allocated guaranteed
private:
  struct HashBkt {
    HashBkt();
    HashBkt(KVNode<K, V> **hash_bkt, const int64_t hash_bkt_num, const int64_t rehash_seed);
    ~HashBkt();
    void insert(KVNode<K, V> *node);
    void remove(KVNode<K, V> *node);
    KVNode<K, V> *find_list(const K &key,
                            KVNode<K, V> **&hash_bkt_prev_node_next_ptr,
                            KVNode<K, V> *&hash_bkt_next) const;
    TO_STRING_KV(KP_(hash_bkt), K_(hash_bkt_num), K_(rehash_seed))
  private:
    uint64_t re_hash_idx_(const K &key) const;
  private:
    KVNode<K, V> **hash_bkt_;// to find KVNode by Key
    int64_t hash_bkt_num_;
    int64_t rehash_seed_;
  };
public:
  ObRecycleMultiKVCache()
  : allocator_(nullptr),
  offset_next_to_appended_(0),
  offset_can_write_end_(0),
  offset_reserved_(0),
  hash_bkt_(),
  buffer_(nullptr),
  buffer_len_(0),
  inner_mock_allocator_() {}
  ObRecycleMultiKVCache(const ObRecycleMultiKVCache &) = delete;
  ~ObRecycleMultiKVCache();
  template <int N>
  int init(const char (&mem_tag)[N],// tag used for alloc static memory
           ObIAllocator &alloc,// the real allocator to alloc memory
           const int64_t recycle_buffer_size,
           const int64_t hash_idx_bkt_num,
           const int64_t rehash_seed = 0);// the buffer to store to_string result by KVNode
  template <typename T>
  int append(const K &key, T &&event);
  template <typename OP>// int OP(const V &);
  int for_each(const K &key, OP &&op) const;// optimized for point select
  template <typename OP>// int OP(const K &, const V &);
  int for_each(OP &&op) const;// optimized for point select
  void get_statistics(int64_t &write_pos,
                      int64_t &recycle_pos,
                      int64_t &write_number,
                      int64_t &recycle_number,
                      int64_t &buf_size) const {
    ObSpinLockGuard lg(lock_);
    write_pos = offset_next_to_appended_;
    recycle_pos = offset_can_write_end_ - buffer_len_;
    write_number = write_number_;
    recycle_number = recycle_number_;
    buf_size = buffer_len_;
  }
  TO_STRING_KV(KP_(allocator), K_(offset_next_to_appended), K_(offset_can_write_end),\
               K_(offset_reserved), K_(hash_bkt), KP_(total_buffer), KP_(buffer), K_(buffer_len),\
               K_(inner_mock_allocator), K_(write_number), K_(recycle_number));
private:
  template <typename OP>// bool OP(KVNode<K, V> &node);
  void for_each_node_continuously_until_true_(OP &&op);
  template <typename Value>
  int construct_assign_(const K &key,
                        Value &&event,
                        InnerMockAllocator &alloc,
                        KVNode<K, V> *&p_new_node);
  int recycle_one_();
  int64_t round_end_(int64_t offset) const;
private:
  ObIAllocator *allocator_;// to alloc buffer in heap
  int64_t offset_next_to_appended_;// ptr to append next time
  int64_t offset_can_write_end_;// ptr to recycled next time, will alway not less than offset_next_to_appended_
  int64_t offset_reserved_;// if alloc memory failed at the end of some round, will try restart alloc in next round, and those end erea with no data called reserved
  HashBkt hash_bkt_;
  char *total_buffer_;// start pointer of heap area, including hash_idx + data_bufer
  char *buffer_;// the start pointer of recycle buffer on heap
  int64_t buffer_len_;
  InnerMockAllocator inner_mock_allocator_;
  int64_t write_number_;
  int64_t recycle_number_;
  mutable ObSpinLock lock_;
};

}
}
}
#ifndef OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H_IPP
#define OCEANBASE_LIB_CACHE_OB_RECYCLE_CACHE_H_IPP
#include "ob_recycle_multi_kvcache.ipp"
#endif

#endif
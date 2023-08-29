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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_STRUCT_H_
#define OCEANBASE_CACHE_OB_KVCACHE_STRUCT_H_

#include "share/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/atomic/ob_atomic_reference.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_link.h" // lock free double linked list
#include "lib/resource/ob_resource_mgr.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/metrics/ob_counter.h"

namespace oceanbase
{
namespace common
{

static const int64_t MAX_CACHE_NUM = 32;
static const int64_t INVALID_CACHE_ID = -1;  // cache id must be in [0,MAX_CACHE_NUM)
static const int64_t MAX_TENANT_NUM_PER_SERVER = 1024;
static const int32_t MAX_CACHE_NAME_LENGTH = 127;
static const double CACHE_SCORE_DECAY_FACTOR = 0.9;

class ObIKVCacheKey
{
public:
  ObIKVCacheKey() {}
  virtual ~ObIKVCacheKey() {}
  virtual bool operator ==(const ObIKVCacheKey &other) const { return false; }
  virtual uint64_t hash() const { return 0; }
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const { equal = *this == other; return OB_SUCCESS; }
  virtual int hash(uint64_t &hash_value) const  { hash_value = hash(); return OB_SUCCESS; }
  virtual uint64_t get_tenant_id() const = 0;
  virtual int64_t size() const = 0;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const = 0;
};

class ObIKVCacheValue
{
public:
  ObIKVCacheValue() {}
  virtual ~ObIKVCacheValue() {}
  virtual int64_t size() const = 0;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const = 0;
};

struct ObKVCachePair
{
  uint32_t magic_;
  int32_t size_;
  ObIKVCacheKey *key_;
  ObIKVCacheValue *value_;
  static const uint32_t KVPAIR_MAGIC_NUM = 0x4B564B56;  //"KVKV"
  ObKVCachePair()
      : magic_(KVPAIR_MAGIC_NUM), size_(0), key_(NULL), value_(NULL)
  {
  }
};

enum ObKVCachePolicy
{
  LRU = 0,
  LFU = 1,
  MAX_POLICY = 2
};

class ObKVStoreMemBlock
{
public:
  ObKVStoreMemBlock(char *buffer, const int64_t size);
  virtual ~ObKVStoreMemBlock();
  void reuse();
  static int64_t upper_align(int64_t input, int64_t align);
  static int64_t get_align_size(const ObIKVCacheKey &key, const ObIKVCacheValue &value);
  static int64_t get_align_size(const int64_t key_size, const int64_t value_size);
  int store(const ObIKVCacheKey &key, const ObIKVCacheValue &value, ObKVCachePair *&kvpair);
  int alloc(const int64_t key_size, const int64_t value_size, const int64_t align_kv_size, ObKVCachePair *&kvpair);
  inline int64_t get_payload_size() const
  {
    return payload_size_;
  }
  inline int64_t get_align_size() const
  {
    return lib::ObTenantMemoryMgr::align(payload_size_ + sizeof(ObKVStoreMemBlock));
  }
  inline int64_t get_size() const { return atomic_pos_.buffer; }
  inline int64_t get_kv_cnt() const { return atomic_pos_.pairs; }
private:
  static const int64_t ALIGN_SIZE = sizeof(size_t);
  AtomicInt64 atomic_pos_;
  int64_t payload_size_;
  char *buffer_;
};

enum ObKVMBHandleStatus
{
  FREE = 0,
  USING = 1,
  FULL = 2,
};

struct ObKVCacheInst;
class ObWorkingSet;
struct ObKVMemBlockHandle : public common::ObDLink
{
  ObKVStoreMemBlock * volatile mem_block_;
  volatile enum ObKVMBHandleStatus status_;
  ObKVCacheInst *inst_;
  enum ObKVCachePolicy policy_;
  int64_t get_cnt_;
  int64_t recent_get_cnt_;
  double score_;
  int64_t kv_cnt_;
  ObAtomicReference handle_ref_;
  common::ObLink retire_link_;
  ObWorkingSet *working_set_;

  ObKVMemBlockHandle();
  virtual ~ObKVMemBlockHandle();
  void reset();
  bool is_mark_delete() const { return is_last_bit_set((uint64_t)ATOMIC_LOAD(&next_)); }
  uint32_t get_seq_num() const { return handle_ref_.get_seq_num(); }
  uint32_t get_ref_cnt() const { return handle_ref_.get_ref_cnt(); }
  int store(const ObIKVCacheKey &key, const ObIKVCacheValue &value, ObKVCachePair *&kvpair);
  int alloc(const int64_t key_size, const int64_t value_size, const int64_t align_kv_size, ObKVCachePair *&kvpair);
  void set_full(const double base_mb_score);
  ObKVMemBlockHandle *get_mb_handle() { return this; }
  TO_STRING_KV(KP_(mem_block), K_(status), KP_(inst), K_(policy), K_(get_cnt),
      K_(recent_get_cnt), K_(score), K_(kv_cnt));
};

struct ObKVCacheInstKey
{
  ObKVCacheInstKey() : cache_id_(-1), tenant_id_(OB_INVALID_ID) {}
  ObKVCacheInstKey(const int64_t cache_id, const uint64_t tenant_id)
  : cache_id_(cache_id), tenant_id_(tenant_id) {}
  int64_t cache_id_;
  uint64_t tenant_id_;
  inline uint64_t hash() const { return cache_id_ + tenant_id_; }
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  inline bool operator==(const ObKVCacheInstKey &other) const
  {
    return cache_id_ == other.cache_id_ && tenant_id_ == other.tenant_id_;
  }
  inline bool operator!=(const ObKVCacheInstKey &other) const
  {
    return !(*this == other);
  }
  inline bool is_valid() const { return cache_id_ >= 0 && cache_id_ < MAX_CACHE_NUM
      && tenant_id_ < (uint64_t) OB_DEFAULT_TENANT_COUNT; }
  inline void reset()
  {
    tenant_id_ = OB_INVALID_ID;
    cache_id_ = -1;
  }
  TO_STRING_KV(K_(cache_id), K_(tenant_id));
};

struct ObKVCacheConfig
{
public:
  ObKVCacheConfig();
  void reset();
  bool is_valid_;
  int64_t priority_;
  char cache_name_[MAX_CACHE_NAME_LENGTH];
};

struct ObKVCacheStatus
{
public:
  ObKVCacheStatus();
  void refresh(const int64_t period_us);
  double get_hit_ratio() const;
  inline void set_hold_size(const int64_t hold_size) { ATOMIC_STORE(&hold_size_, hold_size); }
  inline int64_t get_hold_size() const { return ATOMIC_LOAD(&hold_size_); }
  void reset();
  TO_STRING_KV(KP_(config), K_(kv_cnt), K_(store_size), K_(map_size), K_(lru_mb_cnt),
      K_(lfu_mb_cnt), K_(base_mb_score), K_(hold_size));

  const ObKVCacheConfig *config_;
  ObPCNonAtomicCounter total_put_cnt_;
  ObPCNonAtomicCounter total_hit_cnt_;
  int64_t kv_cnt_;
  int64_t store_size_;
  int64_t lru_mb_cnt_;
  int64_t lfu_mb_cnt_;
  int64_t map_size_;
  int64_t last_hit_cnt_;
  int64_t total_miss_cnt_;
  double base_mb_score_;
  // guarantee at least hold_size_ memory left in cache after wash
  int64_t hold_size_;
};

struct ObKVCacheInfo
{
  ObKVCacheInstKey inst_key_;
  ObKVCacheStatus status_;
  ObKVCacheInfo() : inst_key_(), status_() {}
  TO_STRING_KV(K_(inst_key), K_(status));
};

struct ObKVCacheStoreMemblockInfo
{
public:
  ObKVCacheStoreMemblockInfo()
    : tenant_id_(OB_INVALID_TENANT_ID),
      cache_id_(-1),
      ref_count_(-1),
      using_status_(-1),
      policy_(-1),
      kv_cnt_(-1),
      get_cnt_(-1),
      recent_get_cnt_(-1),
      priority_(0),
      score_(0),
      align_size_(-1),
      cache_name_(),
      memblock_ptr_()
  {
    memset(cache_name_, 0, MAX_CACHE_NAME_LENGTH);
    memset(memblock_ptr_, 0, 32);
  }
  ~ObKVCacheStoreMemblockInfo() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(cache_id), K_(ref_count), K_(using_status), K_(policy), K_(kv_cnt), K_(get_cnt),
          K_(recent_get_cnt), K_(priority), K_(score), K_(align_size), KP_(cache_name), KP_(memblock_ptr));
public:
  uint64_t tenant_id_;
  int64_t cache_id_;
  int64_t ref_count_;
  int64_t using_status_;
  int64_t policy_;
  int64_t kv_cnt_;
  int64_t get_cnt_;
  int64_t recent_get_cnt_;
  int64_t priority_;
  double score_;
  int64_t align_size_;
  char cache_name_[MAX_CACHE_NAME_LENGTH];
  char memblock_ptr_[32];  // store memblock address by char[]
};

class ObIMBHandleAllocator
{
public:
  virtual int alloc_mbhandle(ObKVCacheInst &inst, const int64_t block_size,
                             ObKVMemBlockHandle *&mb_handle) = 0;
  virtual int alloc_mbhandle(ObKVCacheInst &inst, ObKVMemBlockHandle *&mb_handle) = 0;
  virtual int alloc_mbhandle(const ObKVCacheInstKey &inst_key, ObKVMemBlockHandle *&mb_handle) = 0;
  virtual int free_mbhandle(ObKVMemBlockHandle *mb_handle, const bool do_retire) = 0;
  virtual int mark_washable(ObKVMemBlockHandle *mb_handle) = 0;

  virtual bool add_handle_ref(ObKVMemBlockHandle *mb_handle, const uint32_t seq_num) = 0;
  virtual bool add_handle_ref(ObKVMemBlockHandle *mb_handle) = 0;
  virtual uint32_t de_handle_ref(ObKVMemBlockHandle *mb_handle, const bool do_retire = true) = 0;

  virtual int64_t get_block_size() const = 0;
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_KVCACHE_STRUCT_H_

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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_INST_MAP_H_
#define OCEANBASE_CACHE_OB_KVCACHE_INST_MAP_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/cache/ob_cache_utils.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/ob_i_tenant_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{
class ObKVCacheInstMap;
struct ObTenantMBList
{
  ObTenantMBList() { reset(); }
  ~ObTenantMBList() { reset(); }

  int init(const uint64_t tenant_id);
  void reset() {
    tenant_id_ = common::OB_INVALID_ID;
    head_.reset();
    head_.prev_ = &head_;
    head_.next_ = &head_;
    resource_mgr_.reset();
    ref_cnt_ = 0;
    inited_ = false;
  }
  inline bool is_valid() const { return inited_; }
  inline void inc_ref() { ATOMIC_AAF(&ref_cnt_, 1); }
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }

  uint64_t tenant_id_;
  ObKVMemBlockHandle head_;
  lib::ObTenantResourceMgrHandle resource_mgr_;
  int64_t ref_cnt_;
  bool inited_;
};

struct ObTenantMBListHandle
{
  ObTenantMBListHandle();
  ~ObTenantMBListHandle();
  int init(ObKVCacheInstMap *map, ObTenantMBList *list);
  inline bool is_valid() const { return NULL != map_ && NULL != list_; }
  void reset();

  ObKVMemBlockHandle *get_head();
  lib::ObTenantResourceMgrHandle *get_resource_handle();

  ObKVCacheInstMap *map_;
  ObTenantMBList *list_;
};

struct ObKVCacheInst
{
  int64_t cache_id_;
  uint64_t tenant_id_;
  ObKVMemBlockHandle *handles_[MAX_POLICY];
  ObLfFIFOAllocator node_allocator_;
  ObKVCacheStatus status_;
  bool is_delete_;
  int64_t ref_cnt_;
  ObTenantMBListHandle mb_list_handle_; // list of tenant mbs
  ObKVCacheInst()
    : cache_id_(0),
      tenant_id_(0),
      node_allocator_(),
      status_(),
      is_delete_(false),
      ref_cnt_(0),
      mb_list_handle_() { MEMSET(handles_, 0, sizeof(handles_)); }
  bool can_destroy() const ;
  void reset() {
    cache_id_ = 0;
    tenant_id_ = 0;
    node_allocator_.destroy();
    status_.reset();
    is_delete_ = false;
    ref_cnt_ = 0;
    mb_list_handle_.reset();
    MEMSET(handles_, 0, sizeof(handles_));
  }
  bool is_valid() const { return ref_cnt_ > 0; }
  bool is_mark_delete() const { return ATOMIC_LOAD(&is_delete_); }
  void try_mark_delete();

  // hold size related
  inline bool need_hold_cache() { return ATOMIC_LOAD(&status_.hold_size_) > 0; }
  inline int64_t get_memory_limit_pct() { return status_.get_memory_limit_pct(); }
  common::ObDLink *get_mb_list() { return mb_list_handle_.get_head(); }

  TO_STRING_KV(K_(cache_id), K_(tenant_id), K_(is_delete), K_(status), K_(ref_cnt));
};

class ObKVCacheInstHandle
{
public:
  ObKVCacheInstHandle();
  virtual ~ObKVCacheInstHandle();
  void reset();
  bool is_valid() const;
  inline ObKVCacheInst *get_inst() { return inst_; }
  ObKVCacheInstHandle(const ObKVCacheInstHandle &other);
  ObKVCacheInstHandle& operator = (const ObKVCacheInstHandle& other);
  VIRTUAL_TO_STRING_KV(K_(inst));
private:
  friend class ObKVCacheInstMap;
  ObKVCacheInstMap *map_;
  ObKVCacheInst *inst_;
};

class ObKVCacheInstMap
{
public:
  ObKVCacheInstMap();
  virtual ~ObKVCacheInstMap();
  int init(const int64_t max_entry_cnt, const ObKVCacheConfig *configs,
           const ObITenantMemLimitGetter &mem_limit_getter);
  void destroy();
  int get_cache_inst(
      const ObKVCacheInstKey &inst_key,
      ObKVCacheInstHandle &inst_handle);
  int mark_tenant_delete(const uint64_t tenant_id);
  int erase_tenant(const uint64_t tenant_id);
  int refresh_score();
  int set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority);
  int get_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle> &inst_handles);
  void print_all_cache_info();
  void print_tenant_cache_info(const uint64_t tenant_id);

  int set_hold_size(const uint64_t tenant_id, const char *cache_name, const int64_t hold_size);
  int get_hold_size(const uint64_t tenant_id, const char *cache_name, int64_t &hold_size);

  int get_mb_list(const uint64_t tenant_id, ObTenantMBListHandle &list_handle, const bool create_list = true);
  int dec_mb_list_ref(ObTenantMBList *list);
private:
  friend class ObKVCacheInstHandle;
  typedef hash::ObHashMap<ObKVCacheInstKey, ObKVCacheInst*, hash::NoPthreadDefendMode> KVCacheInstMap;
  typedef ObFixedHashMap<uint64_t, ObTenantMBList *> TenantMBListMap;
  void add_inst_ref(ObKVCacheInst *inst);
  void de_inst_ref(ObKVCacheInst *inst);
  int inner_push_inst_handle(const KVCacheInstMap::iterator &iter, ObIArray<ObKVCacheInstHandle> &inst_handles);
private:
  DRWLock lock_;
  KVCacheInstMap  inst_map_;
  DRWLock list_lock_;
  TenantMBListMap list_map_;
  ObFixedQueue<ObTenantMBList> list_pool_;
  const ObKVCacheConfig *configs_;
  ObArenaAllocator allocator_;
  hash::ObHashSet<uint64_t> tenant_set_;

  // used by clean garbage inst
  ObSimpleFixedArray<ObKVCacheInstKey> inst_keys_;
  const ObITenantMemLimitGetter *mem_limit_getter_;

  // used by erase tenant cache inst
  bool is_inited_;
};


}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_KVCACHE_INST_MAP_H_

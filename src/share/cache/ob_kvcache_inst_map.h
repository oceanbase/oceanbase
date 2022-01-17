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
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/cache/ob_cache_utils.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/ob_i_tenant_mgr.h"

namespace oceanbase {
namespace common {
class ObKVCacheInstMap;
struct ObTenantMBList {
  ObTenantMBList()
  {
    reset();
  }
  ~ObTenantMBList()
  {
    reset();
  }

  int init(const uint64_t tenant_id);
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    head_.reset();
    head_.prev_ = &head_;
    head_.next_ = &head_;
    resource_mgr_.reset();
    ref_cnt_ = 0;
    inited_ = false;
  }
  inline bool is_valid() const
  {
    return inited_;
  }
  inline void inc_ref()
  {
    ATOMIC_AAF(&ref_cnt_, 1);
  }
  int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1);
  }
  int64_t get_ref() const
  {
    return ATOMIC_LOAD(&ref_cnt_);
  }

  uint64_t tenant_id_;
  ObKVMemBlockHandle head_;
  lib::ObTenantResourceMgrHandle resource_mgr_;
  int64_t ref_cnt_;
  bool inited_;
};

struct ObTenantMBListHandle {
  ObTenantMBListHandle();
  ~ObTenantMBListHandle();
  int init(ObKVCacheInstMap* map, ObTenantMBList* list);
  inline bool is_valid() const
  {
    return NULL != map_ && NULL != list_;
  }
  void reset();

  ObKVMemBlockHandle* get_head();
  lib::ObTenantResourceMgrHandle* get_resource_handle();

  ObKVCacheInstMap* map_;
  ObTenantMBList* list_;
};

struct ObKVCacheInst {
  int64_t cache_id_;
  uint64_t tenant_id_;
  ObKVMemBlockHandle* handles_[MAX_POLICY];
  ObLfFIFOAllocator node_allocator_;
  ObKVCacheStatus status_;
  int64_t ref_cnt_;
  ObTenantMBListHandle mb_list_handle_;  // list of tenant mbs
  ObKVCacheInst() : cache_id_(0), tenant_id_(0), node_allocator_(), status_(), ref_cnt_(0), mb_list_handle_()
  {
    MEMSET(handles_, 0, sizeof(handles_));
  }
  bool can_destroy()
  {
    return 1 == ATOMIC_LOAD(&ref_cnt_) && 0 == ATOMIC_LOAD(&status_.kv_cnt_) &&
           0 == ATOMIC_LOAD(&status_.store_size_) && 0 == ATOMIC_LOAD(&status_.lru_mb_cnt_) &&
           0 == ATOMIC_LOAD(&status_.lfu_mb_cnt_);
  }
  void reset()
  {
    cache_id_ = 0;
    tenant_id_ = 0;
    node_allocator_.destroy();
    status_.reset();
    ref_cnt_ = 0;
    mb_list_handle_.reset();
    MEMSET(handles_, 0, sizeof(handles_));
  }
  bool is_valid() const
  {
    return ref_cnt_ > 0;
  }

  // hold size related
  inline bool need_hold_cache()
  {
    return ATOMIC_LOAD(&status_.hold_size_) > 0;
  }

  common::ObDLink* get_mb_list()
  {
    return mb_list_handle_.get_head();
  }
};

class ObKVCacheInstHandle {
public:
  ObKVCacheInstHandle();
  virtual ~ObKVCacheInstHandle();
  void reset();
  inline ObKVCacheInst* get_inst()
  {
    return inst_;
  }
  ObKVCacheInstHandle(const ObKVCacheInstHandle& other);
  ObKVCacheInstHandle& operator=(const ObKVCacheInstHandle& other);
  VIRTUAL_TO_STRING_KV(K_(inst_->status));

private:
  friend class ObKVCacheInstMap;
  ObKVCacheInstMap* map_;
  ObKVCacheInst* inst_;
};

class ObKVCacheInstMap {
public:
  ObKVCacheInstMap();
  virtual ~ObKVCacheInstMap();
  int init(const int64_t max_entry_cnt, const ObKVCacheConfig* configs, const ObITenantMgr& tenant_mgr);
  void destroy();
  int get_cache_inst(const ObKVCacheInstKey& inst_key, ObKVCacheInstHandle& inst_handle);
  int clean_garbage_inst();
  int refresh_score();
  int set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority);
  int get_tenant_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle>& inst_handles);
  int get_all_cache_info(ObIArray<ObKVCacheInstHandle>& inst_handles);

  int set_hold_size(const uint64_t tenant_id, const char* cache_name, const int64_t hold_size);
  int get_hold_size(const uint64_t tenant_id, const char* cache_name, int64_t& hold_size);

  int get_mb_list(const uint64_t tenant_id, ObTenantMBListHandle& list_handle);
  int dec_mb_list_ref(ObTenantMBList* list);

private:
  friend class ObKVCacheInstHandle;
  typedef hash::ObHashMap<ObKVCacheInstKey, ObKVCacheInst*, hash::NoPthreadDefendMode> KVCacheInstMap;
  typedef ObFixedHashMap<uint64_t, ObTenantMBList*> TenantMBListMap;
  void add_inst_ref(ObKVCacheInst* inst);
  void de_inst_ref(ObKVCacheInst* inst);
  DRWLock lock_;
  KVCacheInstMap inst_map_;
  ObFixedQueue<ObKVCacheInst> inst_pool_;
  DRWLock list_lock_;
  TenantMBListMap list_map_;
  ObFixedQueue<ObTenantMBList> list_pool_;
  const ObKVCacheConfig* configs_;
  ObArenaAllocator allocator_;

  // used by clean garbage inst
  ObSimpleFixedArray<ObKVCacheInstKey> inst_keys_;
  const ObITenantMgr* tenant_mgr_;
  bool is_inited_;
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_CACHE_OB_KVCACHE_INST_MAP_H_

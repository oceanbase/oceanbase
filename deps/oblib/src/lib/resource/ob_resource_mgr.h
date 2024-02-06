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

#ifndef OCEANBASE_RESOURCE_OB_RESOURCE_MGR_H_
#define OCEANBASE_RESOURCE_OB_RESOURCE_MGR_H_

#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/resource/ob_cache_washer.h"
#include "lib/resource/achunk_mgr.h"

namespace oceanbase
{
namespace lib
{
class ObTenantMemoryMgr
{
public:
  static const int64_t LARGE_REQUEST_EXTRA_MB_COUNT = 2;
  static const int64_t ALIGN_SIZE = static_cast<int64_t>(INTACT_ACHUNK_SIZE);
  static bool error_log_when_tenant_500_oversize;

  ObTenantMemoryMgr();
  ObTenantMemoryMgr(const uint64_t tenant_id);

  virtual ~ObTenantMemoryMgr() {}

  void set_cache_washer(ObICacheWasher &cache_washer);

  AChunk *alloc_chunk(const int64_t size, const ObMemAttr &attr);
  void free_chunk(AChunk *chunk, const ObMemAttr &attr);

  // used by cache module
  void *alloc_cache_mb(const int64_t size);
  void free_cache_mb(void *ptr);

  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_limit(const int64_t limit) { limit_ = limit; }
  int64_t get_limit() const { return limit_; }
  int64_t get_sum_hold() const { return sum_hold_; }
  int64_t get_cache_hold() const { return cache_hold_; }
  int64_t get_cache_item_count() const { return cache_item_count_; }
  int64_t get_rpc_hold() const { return rpc_hold_; }

  void update_rpc_hold(const int64_t size) { ATOMIC_AAF(&rpc_hold_, size); }
  const volatile int64_t *get_ctx_hold_bytes() const { return hold_bytes_; }
  inline static int64_t align(const int64_t size)
  {
    return static_cast<int64_t>(CHUNK_MGR.aligned(static_cast<uint64_t>(size)));
  }
  int set_ctx_limit(const uint64_t ctx_id, const int64_t limit);
  int get_ctx_limit(const uint64_t ctx_id, int64_t &limit) const;
  int get_ctx_hold(const uint64_t ctx_id, int64_t &hold) const;
  bool update_hold(const int64_t size, const uint64_t ctx_id, const lib::ObLabel &label,
      bool &reach_ctx_limit);
private:
  void update_cache_hold(const int64_t size);
  bool update_ctx_hold(const uint64_t ctx_id, const int64_t size);
  AChunk *ptr2chunk(void *ptr);
  AChunk *alloc_chunk_(const int64_t size, const ObMemAttr &attr);
  void free_chunk_(AChunk *chunk, const ObMemAttr &attr);
  ObICacheWasher *cache_washer_;
  uint64_t tenant_id_;
  int64_t limit_;
  int64_t sum_hold_;
  int64_t rpc_hold_;
  int64_t cache_hold_;
  int64_t cache_item_count_;
  volatile int64_t hold_bytes_[common::ObCtxIds::MAX_CTX_ID];
  volatile int64_t limit_bytes_[common::ObCtxIds::MAX_CTX_ID];
};

struct ObTenantResourceMgr : public common::ObLink
{
  ObTenantResourceMgr();
  explicit ObTenantResourceMgr(const uint64_t tenant_id);
  virtual ~ObTenantResourceMgr();

  uint64_t tenant_id_;
  ObTenantMemoryMgr memory_mgr_;
  // add other mgr here
  int64_t ref_cnt_;
};

class ObResourceMgr;
class ObTenantResourceMgrHandle
{
public:
  ObTenantResourceMgrHandle();
  virtual ~ObTenantResourceMgrHandle();

  int init(ObResourceMgr *resource_mgr, ObTenantResourceMgr *tenant_resource_mgr);
  bool is_valid() const;
  void reset();
  ObTenantMemoryMgr *get_memory_mgr();
  const ObTenantMemoryMgr *get_memory_mgr() const;
private:
  ObResourceMgr *resource_mgr_;
  ObTenantResourceMgr *tenant_resource_mgr_;
};

class ObResourceMgr
{
  friend class ObTenantResourceMgrHandle;
public:
  ObResourceMgr();
  virtual ~ObResourceMgr();

  int init();
  void destroy();
  static ObResourceMgr &get_instance();
  int set_cache_washer(ObICacheWasher &cache_washer);

  // will create resource mgr if not exist
  int get_tenant_resource_mgr(const uint64_t tenant_id, ObTenantResourceMgrHandle &handle);
private:
  static const int64_t MAX_TENANT_COUNT = 12289;  // prime number
  void inc_ref(ObTenantResourceMgr *tenant_resource_mgr);
  void dec_ref(ObTenantResourceMgr *tenant_resource_mgr);
  int get_tenant_resource_mgr_unsafe(const uint64_t tenant_id, ObTenantResourceMgr *&tenant_resource_mgr);
  int remove_tenant_resource_mgr_unsafe(const uint64_t tenant_id);
  int create_tenant_resource_mgr_unsafe(const uint64_t tenant_id, ObTenantResourceMgr *&tenant_resource_mgr);

  bool inited_;
  ObICacheWasher *cache_washer_;
  common::SpinRWLock locks_[MAX_TENANT_COUNT];
  ObTenantResourceMgr *tenant_resource_mgrs_[MAX_TENANT_COUNT];
};

}//end namespace lib
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_CACHE_MEMORY_MGR_H_

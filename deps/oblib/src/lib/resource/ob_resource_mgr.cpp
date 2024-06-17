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

#define USING_LOG_PREFIX COMMON

#include "ob_resource_mgr.h"
#include <new>
#include <stdlib.h>

#include "lib/alloc/memory_sanity.h"
#include "lib/alloc/ob_malloc_time_monitor.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/utility.h"
#include "lib/alloc/alloc_failed_reason.h"

namespace oceanbase
{
using namespace common;
namespace lib
{
ObTenantMemoryMgr::ObTenantMemoryMgr()
  : cache_washer_(NULL), tenant_id_(common::OB_INVALID_ID),
    limit_(INT64_MAX), sum_hold_(0), rpc_hold_(0), cache_hold_(0),
    cache_item_count_(0)
{
  for (uint64_t i = 0; i < common::ObCtxIds::MAX_CTX_ID; i++) {
    ATOMIC_STORE(&(hold_bytes_[i]), 0);
    ATOMIC_STORE(&(limit_bytes_[i]), INT64_MAX);
  }
}

ObTenantMemoryMgr::ObTenantMemoryMgr(const uint64_t tenant_id)
  : cache_washer_(NULL), tenant_id_(tenant_id),
    limit_(INT64_MAX), sum_hold_(0), rpc_hold_(0), cache_hold_(0),
    cache_item_count_(0)
{
  for (uint64_t i = 0; i < common::ObCtxIds::MAX_CTX_ID; i++) {
    ATOMIC_STORE(&(hold_bytes_[i]), 0);
    ATOMIC_STORE(&(limit_bytes_[i]), INT64_MAX);
  }
}
void ObTenantMemoryMgr::set_cache_washer(ObICacheWasher &cache_washer)
{
  cache_washer_ = &cache_washer;
}

AChunk *ObTenantMemoryMgr::alloc_chunk(const int64_t size, const ObMemAttr &attr)
{
  AChunk *chunk = NULL;
  int ret = OB_SUCCESS;
  if (tenant_id_ != attr.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tenant_id not match", K(ret), K_(tenant_id), K(attr));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid size", K(ret), K(size));
  } else {
    const int64_t hold_size = static_cast<int64_t>(CHUNK_MGR.hold(static_cast<uint64_t>(size)));
    bool reach_ctx_limit = false;
    if (update_hold(hold_size, attr.ctx_id_, attr.label_, reach_ctx_limit)) {
      chunk = alloc_chunk_(size, attr);
      if (NULL == chunk) {
        update_hold(-hold_size, attr.ctx_id_, attr.label_, reach_ctx_limit);
      } else if (attr.label_ == ObNewModIds::OB_KVSTORE_CACHE_MB) {
        update_cache_hold(hold_size);
      }
    }
    BASIC_TIME_GUARD_CLICK("ALLOC_CHUNK_END");
    if (!reach_ctx_limit && NULL != cache_washer_ && NULL == chunk && hold_size < cache_hold_
        && attr.label_ != ObNewModIds::OB_KVSTORE_CACHE_MB) {
      // try wash memory from cache
      ObICacheWasher::ObCacheMemBlock *washed_blocks = NULL;
      int64_t wash_size = hold_size + LARGE_REQUEST_EXTRA_MB_COUNT * INTACT_ACHUNK_SIZE;
      while (!reach_ctx_limit && OB_SUCC(ret) && NULL == chunk && wash_size < cache_hold_) {
        if (OB_FAIL(cache_washer_->sync_wash_mbs(tenant_id_, wash_size, washed_blocks))) {
          LOG_WARN("sync_wash_mbs failed", K(ret), K_(tenant_id), K(wash_size));
        } else {
          // should return back to os, then realloc again
          ObMemAttr cache_attr;
          cache_attr.tenant_id_ = tenant_id_;
          cache_attr.label_ = ObNewModIds::OB_KVSTORE_CACHE_MB;
          ObICacheWasher::ObCacheMemBlock *next = NULL;
          while (NULL != washed_blocks) {
            AChunk *chunk = ptr2chunk(washed_blocks);
            next = washed_blocks->next_;
            free_chunk(chunk, cache_attr);
            chunk = NULL;
            washed_blocks = next;
          }

          if (update_hold(static_cast<int64_t>(hold_size), attr.ctx_id_, attr.label_,
                          reach_ctx_limit)) {
            chunk = alloc_chunk_(size, attr);
            if (NULL == chunk) {
              update_hold(-hold_size, attr.ctx_id_, attr.label_, reach_ctx_limit);
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("after wash from cache, still can't alloc chunk from chunk_mgr, "
                "maybe alloc by other thread", K(size), K(wash_size), K(ret));
      }
      BASIC_TIME_GUARD_CLICK("WASH_KVCACHE_END");
    }
  }
  return chunk;
}

void ObTenantMemoryMgr::free_chunk(AChunk *chunk, const ObMemAttr &attr)
{
  if (tenant_id_ != attr.tenant_id_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant_id not match", K_(tenant_id), K(attr));
  } else if (NULL != chunk) {
    bool reach_ctx_limit = false;
    const int64_t hold_size = static_cast<int64_t>(chunk->hold());
    update_hold(-hold_size, attr.ctx_id_, attr.label_, reach_ctx_limit);
    if (attr.label_ == ObNewModIds::OB_KVSTORE_CACHE_MB) {
      update_cache_hold(-hold_size);
    }
    free_chunk_(chunk, attr);
  }
}

void *ObTenantMemoryMgr::alloc_cache_mb(const int64_t size)
{
  void *ptr = NULL;
  AChunk *chunk = NULL;
  ObMemAttr attr;
  attr.tenant_id_ = tenant_id_;
  attr.prio_ = OB_NORMAL_ALLOC;
  attr.label_ = ObNewModIds::OB_KVSTORE_CACHE_MB;
  if (NULL != (chunk = alloc_chunk(size, attr))) {
    const int64_t all_size = CHUNK_MGR.aligned(size);
    SANITY_UNPOISON(chunk, all_size);
    ptr = chunk->data_;
  }
  return ptr;
}

void ObTenantMemoryMgr::free_cache_mb(void *ptr)
{
  if (NULL != ptr) {
    ObMemAttr attr;
    attr.tenant_id_ = tenant_id_;
    attr.prio_ = OB_NORMAL_ALLOC;
    attr.label_ = ObNewModIds::OB_KVSTORE_CACHE_MB;
    AChunk *chunk = ptr2chunk(ptr);
    const int64_t all_size = chunk->aligned();
    SANITY_POISON(chunk, all_size);
    free_chunk(chunk, attr);
  }
}

int ObTenantMemoryMgr::set_ctx_limit(const uint64_t ctx_id, const int64_t limit)
{
  int ret = OB_SUCCESS;
  if (ctx_id >= ObCtxIds::MAX_CTX_ID || limit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(ctx_id), K(limit));
  } else {
    limit_bytes_[ctx_id] = limit;
  }
  return ret;
}

int ObTenantMemoryMgr::get_ctx_limit(const uint64_t ctx_id, int64_t &limit) const
{
  int ret = OB_SUCCESS;
  if (ctx_id >= ObCtxIds::MAX_CTX_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(ctx_id));
  } else {
    limit = limit_bytes_[ctx_id];
  }
  return ret;
}

int ObTenantMemoryMgr::get_ctx_hold(const uint64_t ctx_id, int64_t &hold) const
{
  int ret = OB_SUCCESS;
  if (ctx_id >= ObCtxIds::MAX_CTX_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(ctx_id));
  } else {
    hold = hold_bytes_[ctx_id];
    if (ObCtxIds::KVSTORE_CACHE_ID == ctx_id) {
      hold += get_cache_hold();
    }
  }
  return ret;
}

void ObTenantMemoryMgr::update_cache_hold(const int64_t size)
{
  if (0 != size) {
    ATOMIC_AAF(&cache_hold_, size);
    ATOMIC_AAF(&cache_item_count_, size > 0 ? 1 : -1);
  }
}

bool ObTenantMemoryMgr::update_hold(const int64_t size, const uint64_t ctx_id,
                                    const lib::ObLabel &label, bool &reach_ctx_limit)
{
  bool updated = false;
  reach_ctx_limit = false;
  if (size <= 0) {
    ATOMIC_AAF(&sum_hold_, size);
    updated = true;
  } else {
    if (sum_hold_ + size <= limit_) {
      const int64_t nvalue = ATOMIC_AAF(&sum_hold_, size);
      if (nvalue > limit_) {
        ATOMIC_AAF(&sum_hold_, -size);
      } else {
        updated = true;
      }
    }
  }
  if (!updated) {
    auto &afc = g_alloc_failed_ctx();
    afc.reason_ = TENANT_HOLD_REACH_LIMIT;
    afc.alloc_size_ = size;
    afc.tenant_id_ = tenant_id_;
    afc.tenant_hold_ = sum_hold_;
    afc.tenant_limit_ = limit_;
  } else if (label != ObNewModIds::OB_KVSTORE_CACHE_MB) {
    if (!update_ctx_hold(ctx_id, size)) {
      ATOMIC_AAF(&sum_hold_, -size);
      updated = false;
      reach_ctx_limit = true;
    }
  }
  return updated;
}

bool ObTenantMemoryMgr::update_ctx_hold(const uint64_t ctx_id, const int64_t size)
{
  bool updated = false;
  if (ctx_id < ObCtxIds::MAX_CTX_ID) {
    volatile int64_t &hold = hold_bytes_[ctx_id];
    volatile int64_t &limit = limit_bytes_[ctx_id];
    if (size <= 0) {
      ATOMIC_AAF(&hold, size);
      updated = true;
    } else {
      if (hold + size <= limit) {
        const int64_t nvalue = ATOMIC_AAF(&hold, size);
        if (nvalue > limit) {
          ATOMIC_AAF(&hold, -size);
        } else {
          updated = true;
        }
      }
    }
    if (!updated) {
      auto &afc = g_alloc_failed_ctx();
      afc.reason_ = CTX_HOLD_REACH_LIMIT;
      afc.alloc_size_ = size;
      afc.ctx_id_ = ctx_id;
      afc.ctx_hold_ = hold;
      afc.ctx_limit_ = limit;
    }
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid ctx_id", K(ctx_id));
  }
  return updated;
}

AChunk *ObTenantMemoryMgr::ptr2chunk(void *ptr)
{
  AChunk *chunk = NULL;
  if (NULL != ptr) {
    chunk = reinterpret_cast<AChunk *>(reinterpret_cast<char *>(ptr) - ACHUNK_PURE_HEADER_SIZE);
  }
  return chunk;
}

AChunk *ObTenantMemoryMgr::alloc_chunk_(const int64_t size, const ObMemAttr &attr)
{
  AChunk *chunk = nullptr;
  if (OB_UNLIKELY(attr.ctx_id_ == ObCtxIds::CO_STACK)) {
    chunk = CHUNK_MGR.alloc_co_chunk(static_cast<uint64_t>(size));
  } else {
    chunk = CHUNK_MGR.alloc_chunk(static_cast<uint64_t>(size), OB_HIGH_ALLOC == attr.prio_);
  }
  return chunk;
}

void ObTenantMemoryMgr::free_chunk_(AChunk *chunk, const ObMemAttr &attr)
{
  if (OB_UNLIKELY(attr.ctx_id_ == ObCtxIds::CO_STACK)) {
    CHUNK_MGR.free_co_chunk(chunk);
  } else {
    CHUNK_MGR.free_chunk(chunk);
  }
}

ObTenantResourceMgr::ObTenantResourceMgr()
  : tenant_id_(OB_INVALID_ID), memory_mgr_(), ref_cnt_(0)
{
}

ObTenantResourceMgr::ObTenantResourceMgr(const uint64_t tenant_id)
  : tenant_id_(tenant_id), memory_mgr_(tenant_id), ref_cnt_(0)
{
}

ObTenantResourceMgr::~ObTenantResourceMgr()
{
  tenant_id_ = OB_INVALID_ID;
  ref_cnt_ = 0;
}

ObTenantResourceMgrHandle::ObTenantResourceMgrHandle()
  : resource_mgr_(NULL),
    tenant_resource_mgr_(NULL)
{
}

ObTenantResourceMgrHandle::~ObTenantResourceMgrHandle()
{
  reset();
}

int ObTenantResourceMgrHandle::init(ObResourceMgr *resource_mgr, ObTenantResourceMgr *tenant_resource_mgr)
{
  // can't invoke reset here, because init is invoked with read lock acquired,
  // reset will invoke dec_ref which may try to acquire write lock, leading to
  // recursive lock
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == resource_mgr || NULL == tenant_resource_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(resource_mgr), KP(tenant_resource_mgr));
  } else {
    resource_mgr_ = resource_mgr;
    tenant_resource_mgr_ = tenant_resource_mgr;
    resource_mgr_->inc_ref(tenant_resource_mgr_);
  }
  return ret;
}

bool ObTenantResourceMgrHandle::is_valid() const
{
  return NULL != resource_mgr_ && NULL != tenant_resource_mgr_;
}

void ObTenantResourceMgrHandle::reset()
{
  if (is_valid()) {
    resource_mgr_->dec_ref(tenant_resource_mgr_);
    resource_mgr_ = NULL;
    tenant_resource_mgr_ = NULL;
  }
}

ObTenantMemoryMgr *ObTenantResourceMgrHandle::get_memory_mgr()
{
  ObTenantMemoryMgr *memory_mgr = NULL;
  if (NULL != tenant_resource_mgr_) {
    memory_mgr = &tenant_resource_mgr_->memory_mgr_;
  }
  return memory_mgr;
}

const ObTenantMemoryMgr *ObTenantResourceMgrHandle::get_memory_mgr() const
{
  const ObTenantMemoryMgr *memory_mgr = NULL;
  if (NULL != tenant_resource_mgr_) {
    memory_mgr = &tenant_resource_mgr_->memory_mgr_;
  }
  return memory_mgr;
}

ObResourceMgr::ObResourceMgr()
  : inited_(false), cache_washer_(NULL), locks_(), tenant_resource_mgrs_()
{
  for (int64_t i = 0; i < MAX_TENANT_COUNT; ++i) {
    locks_[i].enable_record_stat(false);
    locks_[i].set_latch_id(common::ObLatchIds::TENANT_RES_MGR_LIST_LOCK);
  }
}

ObResourceMgr::~ObResourceMgr()
{
}

int ObResourceMgr::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObResourceMgr::destroy()
{
  if (inited_) {
    cache_washer_ = NULL;
    memset(tenant_resource_mgrs_, 0, sizeof(ObTenantResourceMgr *) * MAX_TENANT_COUNT);
    inited_ = false;
  }
}

ObResourceMgr &ObResourceMgr::get_instance()
{
  static ObResourceMgr resource_mgr;
  if (!resource_mgr.inited_) {
    // use first lock to avoid concurrent init of resource mgr
    ObDisableDiagnoseGuard disable_diagnose_guard;
    SpinWLockGuard guard(resource_mgr.locks_[0]);
    if (!resource_mgr.inited_) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(resource_mgr.init())) {
        LOG_WARN("resource_mgr init failed", K(ret));
      }
    }
  }
  return resource_mgr;
}

int ObResourceMgr::set_cache_washer(ObICacheWasher &cache_washer)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    cache_washer_ = &cache_washer;
    for (int64_t pos = 0; pos < MAX_TENANT_COUNT; ++pos) {
      ObDisableDiagnoseGuard disable_diagnose_guard;
      SpinWLockGuard guard(locks_[pos]);
      ObTenantResourceMgr *tenant_resource_mgr = tenant_resource_mgrs_[pos];
      while (NULL != tenant_resource_mgr) {
        tenant_resource_mgr->memory_mgr_.set_cache_washer(cache_washer);
        tenant_resource_mgr = static_cast<ObTenantResourceMgr *>(tenant_resource_mgr->next_);
      }
    }
  }
  return ret;
}

int ObResourceMgr::get_tenant_resource_mgr(const uint64_t tenant_id,
                                           ObTenantResourceMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    const int64_t pos = tenant_id % MAX_TENANT_COUNT;
    ObDisableDiagnoseGuard disable_diagnose_guard;
    ObTenantResourceMgr *tenant_resource_mgr = NULL;
    {
      SpinRLockGuard guard(locks_[pos]);
      if (OB_FAIL(get_tenant_resource_mgr_unsafe(tenant_id, tenant_resource_mgr))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get_tenant_resource_mgr_unsafe failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(handle.init(this, tenant_resource_mgr))) {
        LOG_WARN("init handle failed", K(ret), KP(tenant_resource_mgr));
      }
    }

    if (OB_SUCC(ret) && !handle.is_valid()) {
      SpinWLockGuard guard(locks_[pos]);
      // maybe other thread create, so retry get here
      if (OB_FAIL(get_tenant_resource_mgr_unsafe(tenant_id, tenant_resource_mgr))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get_tenant_resource_mgr_unsafe failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(create_tenant_resource_mgr_unsafe(tenant_id, tenant_resource_mgr))) {
            LOG_WARN("create_tenant_resource_mgr_unsafe failed", K(ret), K(tenant_id));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(handle.init(this, tenant_resource_mgr))) {
          LOG_WARN("init handle failed", K(ret), KP(tenant_resource_mgr));
        }
      }
    }
  }
  return ret;
}

void ObResourceMgr::inc_ref(ObTenantResourceMgr *tenant_resource_mgr)
{
  if (NULL != tenant_resource_mgr) {
    ATOMIC_AAF(&tenant_resource_mgr->ref_cnt_, 1);
  }
}

void ObResourceMgr::dec_ref(ObTenantResourceMgr *tenant_resource_mgr)
{
  if (NULL != tenant_resource_mgr) {
    int64_t ref_cnt = 0;
    if (0 == (ref_cnt = ATOMIC_SAF(&tenant_resource_mgr->ref_cnt_, 1))) {
      const int64_t pos = tenant_resource_mgr->tenant_id_ % MAX_TENANT_COUNT;
      ObDisableDiagnoseGuard disable_diagnose_guard;
      SpinWLockGuard guard(locks_[pos]);
      if (0 == ATOMIC_LOAD(&tenant_resource_mgr->ref_cnt_)) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(remove_tenant_resource_mgr_unsafe(tenant_resource_mgr->tenant_id_))) {
          LOG_WARN("remove_tenant_resource_mgr_unsafe failed", K(ret),
              "tenant_id", tenant_resource_mgr->tenant_id_);
        }
      }
    } else if (ref_cnt < 0) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ref_cnt negative", K(ref_cnt));
    }
  }
}

int ObResourceMgr::get_tenant_resource_mgr_unsafe(const uint64_t tenant_id,
                                                  ObTenantResourceMgr *&tenant_resource_mgr)
{
  int ret = OB_SUCCESS;
  tenant_resource_mgr = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    const int64_t pos = tenant_id % MAX_TENANT_COUNT;
    ObTenantResourceMgr *iter = tenant_resource_mgrs_[pos];
    while (NULL != iter && NULL == tenant_resource_mgr) {
      if (iter->tenant_id_ == tenant_id) {
        tenant_resource_mgr = iter;
      } else {
        iter = static_cast<ObTenantResourceMgr *>(iter->next_);
      }
    }
    if (NULL == tenant_resource_mgr) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObResourceMgr::create_tenant_resource_mgr_unsafe(const uint64_t tenant_id,
                                                     ObTenantResourceMgr *&tenant_resource_mgr)
{
  int ret = OB_SUCCESS;

  tenant_resource_mgr = NULL;
  void *ptr = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_SERVER_TENANT_ID == tenant_id)) {
    static char buf[sizeof(ObTenantResourceMgr)] __attribute__((__aligned__(16)));
    ptr = buf;
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, "TntResourceMgr");
    SET_USE_500(attr);
    if (OB_ISNULL(ptr = ob_malloc(sizeof(ObTenantResourceMgr), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    tenant_resource_mgr = new (ptr) ObTenantResourceMgr(tenant_id);
    const int64_t pos = tenant_id % MAX_TENANT_COUNT;
    if (NULL != cache_washer_) {
      tenant_resource_mgr->memory_mgr_.set_cache_washer(*cache_washer_);
    }
    tenant_resource_mgr->next_ = tenant_resource_mgrs_[pos];
    tenant_resource_mgrs_[pos] = tenant_resource_mgr;
  }
  return ret;
}

int ObResourceMgr::remove_tenant_resource_mgr_unsafe(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    const int64_t pos = tenant_id % MAX_TENANT_COUNT;
    ObTenantResourceMgr *head = tenant_resource_mgrs_[pos];
    ObTenantResourceMgr *tenant_resource_mgr = NULL;
    if (NULL == head) {
    } else if (head->tenant_id_ == tenant_id) {
      tenant_resource_mgr = head;
      tenant_resource_mgrs_[pos] = static_cast<ObTenantResourceMgr *>(head->next_);
    } else {
      ObTenantResourceMgr *prev = head;
      ObTenantResourceMgr *cur = static_cast<ObTenantResourceMgr *>(head->next_);
      while (NULL != cur && NULL == tenant_resource_mgr) {
        if (cur->tenant_id_ == tenant_id) {
          prev->next_ = cur->next_;
          tenant_resource_mgr = cur;
        } else {
          prev = cur;
          cur = static_cast<ObTenantResourceMgr *>(cur->next_);
        }
      }
    }
    if (NULL == tenant_resource_mgr) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tenant memory mgr not exist", K(ret), K(tenant_id));
    } else {
      tenant_resource_mgr->~ObTenantResourceMgr();
      if (tenant_id != OB_SERVER_TENANT_ID) {
        ob_free(tenant_resource_mgr);
      }
      tenant_resource_mgr = NULL;
    }
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase

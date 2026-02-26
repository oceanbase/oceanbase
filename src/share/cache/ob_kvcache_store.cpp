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

#include "lib/ob_errno.h"
#include "share/cache/ob_kvcache_struct.h"
#define USING_LOG_PREFIX COMMON

#include "share/cache/ob_kvcache_store.h"
#include "share/cache/ob_kvcache_hazard_domain.h"
#include "share/config/ob_server_config.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_kvcache_store.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
using namespace lib;
namespace common
{

ObKVMemBlockHandle* mb_handles;

ObKVMemBlockHandle* mb_handle_at(uint32_t handle_index)
{
  return &mb_handles[handle_index];
}

uint32_t handle_index_of(ObKVMemBlockHandle* mb_handle)
{
  return mb_handle - mb_handles;
}

int ObIKVCacheStore::store(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    ObKVCachePair *&kvpair,
    HazptrHolder &hazptr_holder,
    const enum ObKVCachePolicy policy)
{
  int ret = common::OB_SUCCESS;
  const int64_t key_size = key.size();
  const int64_t value_size = value.size();
  if (OB_FAIL(alloc_kvpair(inst, key_size, value_size, kvpair, hazptr_holder, policy))) {
    COMMON_LOG(WARN, "failed to alloc", K(ret), K(key_size), K(value_size));
  } else {
    if (OB_FAIL(key.deep_copy(reinterpret_cast<char *>(kvpair->key_), key_size, kvpair->key_))) {
      COMMON_LOG(WARN, "failed to deep copy key", K(ret));
    } else if (OB_FAIL(value.deep_copy(reinterpret_cast<char *>(kvpair->value_), value_size, kvpair->value_))) {
      COMMON_LOG(WARN, "failed to deep copy value", K(ret));
    }
    if (OB_FAIL(ret)) {
      hazptr_holder.release();
      kvpair = nullptr;
    }
  }
  return ret;
}

int ObIKVCacheStore::alloc_kvpair(
    ObKVCacheInst &inst,
    const int64_t key_size,
    const int64_t value_size,
    ObKVCachePair *&kvpair,
    HazptrHolder &hazptr_holder,
    const enum ObKVCachePolicy policy)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = inst.tenant_id_;
  int64_t washed_size;
  if (OB_SUCC(alloc_kvpair_without_retry(inst, key_size, value_size, kvpair, hazptr_holder, policy))) {
  } else if (OB_ALLOCATE_MEMORY_FAILED != ret) {
    COMMON_LOG(WARN, "failed to allocate kvpair", K(inst), K(key_size), K(value_size), K(policy));
  } else if (0 >= (washed_size = ObMallocAllocator::get_instance()->sync_wash(tenant_id, 0, INT64_MAX))) {
    COMMON_LOG(WARN, "failed to sync wash", K(inst));
  } else if (OB_FAIL(alloc_kvpair_without_retry(inst, key_size, value_size, kvpair, hazptr_holder, policy))) {
    COMMON_LOG(WARN, "failed to allocate kvpair", K(inst), K(key_size), K(value_size), K(policy));
  }

  return ret;
}

int ObIKVCacheStore::alloc_kvpair_without_retry(
    ObKVCacheInst &inst,
    const int64_t key_size,
    const int64_t value_size,
    ObKVCachePair *&kvpair,
    HazptrHolder &hazptr_holder,
    const enum ObKVCachePolicy policy)
{
  int ret = common::OB_SUCCESS;
  bool protect_success;
  const int64_t block_size = get_block_size();
  const int64_t block_payload_size = block_size - sizeof(ObKVStoreMemBlock);
  int64_t align_kv_size = ObKVStoreMemBlock::get_align_size(key_size, value_size);
  kvpair = NULL;
  ObKVMemBlockHandle* mb_handle = nullptr;

  if (align_kv_size > block_payload_size) {
    //large kv
    const int64_t big_block_size = align_kv_size + sizeof(ObKVStoreMemBlock);
    if (OB_FAIL(alloc(inst, policy, big_block_size, mb_handle))) {
      COMMON_LOG(WARN, "alloc failed", K(ret), K(big_block_size));
    } else {
      if (OB_FAIL(hazptr_holder.protect(protect_success, mb_handle))) {
        COMMON_LOG(WARN, "protect failed", KP(mb_handle));
      } else if (protect_success) {
        if (OB_FAIL(mb_handle->alloc(key_size, value_size, align_kv_size, kvpair))) {
          hazptr_holder.release();
          COMMON_LOG(WARN, "alloc failed", K(ret));
        } else {
          // success to alloc kv
          mb_handle->set_full(inst.status_.base_mb_score_);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "protect failed", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(mb_handle)) {
      free(mb_handle);
    }
  } else {
    //small kv
    do {
      mb_handle = get_curr_mb(inst, policy);
      if (NULL != mb_handle) {
        if (OB_FAIL(hazptr_holder.protect(protect_success, mb_handle))) {
          COMMON_LOG(WARN, "failed to protect mb handle", KP(mb_handle));
        } else if (protect_success) {
          if (mb_status_match(inst, policy, mb_handle)) {
            if (OB_FAIL(mb_handle->alloc(key_size, value_size, align_kv_size, kvpair))) {
              if (OB_BUF_NOT_ENOUGH != ret) {
                COMMON_LOG(WARN, "alloc failed", K(ret));
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              break;
            }
          }
          hazptr_holder.release();
        }
      }

      if (OB_SUCC(ret)) {
        ObKVMemBlockHandle *new_mb_handle = NULL;
        if (OB_FAIL(alloc(inst, policy, block_size, new_mb_handle))) {
          COMMON_LOG(WARN, "alloc failed", K(ret), K(block_size));
        } else if (ATOMIC_BCAS((uint64_t*)(&get_curr_mb(inst, policy)), (uint64_t)mb_handle, (uint64_t)new_mb_handle)) {
          if (NULL != mb_handle) {
            mb_handle->set_full(inst.status_.base_mb_score_);
          }
        } else if (OB_FAIL(free(new_mb_handle))) {
          COMMON_LOG(ERROR, "free failed", K(ret));
        }
      }
    } while (OB_SUCC(ret));
  }

  if (OB_FAIL(ret)) {
    kvpair = NULL;
    hazptr_holder.reset();
  }
  return ret;
}

ObKVCacheStore::ObKVCacheStore()
    : inited_(false),
      insts_(NULL),
      cur_mb_num_(0),
      max_mb_num_(0),
      block_size_(0),
      block_payload_size_(0),
      mb_handles_(NULL),
      mb_handles_pool_(),
      wash_out_lock_(common::ObLatchIds::WASH_OUT_LOCK),
      tenant_ids_(),
      inst_handles_(),
      wash_info_free_heap_(),
      tenant_wash_map_(),
      mb_ptr_pool_(),
      washable_size_allocator_(),
      washbale_size_info_(),
      tmp_washbale_size_info_(),
      tenant_reserve_mem_ratio_(TENANT_RESERVE_MEM_RATIO),
      wash_itid_(-1),
      mem_limit_getter_(NULL)
{
}

ObKVCacheStore::~ObKVCacheStore()
{
  destroy();
}

int ObKVCacheStore::init(ObKVCacheInstMap &insts,
                         const int64_t max_cache_size,
                         const int64_t block_size,
                         const ObITenantMemLimitGetter &mem_limit_getter)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheStore has been inited, ", K(ret));
  } else if (OB_UNLIKELY(max_cache_size <= block_size * 3)
      || OB_UNLIKELY(block_size <= (int64_t)(sizeof(ObKVStoreMemBlock)))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments, ", K(max_cache_size),
      K(block_size), K(ret));
  } else {
    max_mb_num_ = compute_mb_handle_num(max_cache_size, block_size);
    if (NULL == (mb_handles_ = static_cast<ObKVMemBlockHandle*>(
                            buf = ob_malloc((sizeof(ObKVMemBlockHandle) + sizeof(ObKVMemBlockHandle*)) * max_mb_num_,
                                ObMemAttr(OB_SERVER_TENANT_ID, "CACHE_MB_HANDLE", ObCtxIds::UNEXPECTED_IN_500))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory for mb_handles_, ", K_(max_mb_num), K(ret));
    } else if (FALSE_IT(mb_handles = mb_handles_)) {
    } else if (OB_FAIL(mb_handles_pool_.init(max_mb_num_, (char*)(buf) + sizeof(ObKVMemBlockHandle) * max_mb_num_))) {
      COMMON_LOG(WARN, "Fail to init mb_handles_pool_, ", K(ret));
    } else {
      MEMSET(buf, 0, sizeof(ObKVMemBlockHandle) * max_mb_num_);
      block_size_ = block_size;
      block_payload_size_ = block_size - sizeof(ObKVStoreMemBlock);
      // prepare memory block handle
      (void)try_supply_mb(SUPPLY_MB_NUM_ONCE);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_wash_structs())) {
        COMMON_LOG(WARN, "preapre wash structs failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    insts_ = &insts;
    mem_limit_getter_ = &mem_limit_getter;
    inited_ = true;
    COMMON_LOG(INFO, "ObKVCacheStore init success", K(max_cache_size), K(block_size));
  }
  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObKVCacheStore::destroy()
{
  int ret = OB_SUCCESS;

  if (NULL != mb_handles_) {
    for (int64_t i = 0; i < max_mb_num_; ++i) {
      if (FREE != mb_handles_[i].status_) {
        free_mb(*mb_handles_[i].inst_->mb_list_handle_.get_resource_handle(),
            mb_handles_[i].inst_->tenant_id_,
            mb_handles_[i].mem_block_);
      }
    }
    // free all mb handles cached by threads
    purge_mb_handle_retire_station();

    ob_free(mb_handles_);
    mb_handles_ = NULL;
  }

  mb_handles_pool_.destroy();
  block_size_ = 0;
  block_payload_size_ = 0;
  insts_ = NULL;

  destroy_wash_structs();
  inited_ = false;
}

int ObKVCacheStore::set_priority(
    const int64_t cache_id,
    const int64_t old_priority,
    const int64_t new_priority)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0)
      || OB_UNLIKELY(cache_id >= MAX_CACHE_NUM)
      || OB_UNLIKELY(old_priority <=0)
      || OB_UNLIKELY(new_priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(old_priority), K(new_priority), K(ret));
  } else {
    lib::ObMutexGuard guard(wash_out_lock_);
    if (OB_FAIL(insts_->set_priority(cache_id, old_priority, new_priority))) {
      COMMON_LOG(WARN, "Fail to set inst priority, ", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_mb_num_; i++) {
        HazptrHolder hazptr_holder;
        bool protect_success;
        if (OB_FAIL(hazptr_holder.protect(protect_success, &mb_handles_[i]))) {
          COMMON_LOG(WARN, "failed to protect mb_handle");
        } else if (protect_success) {
          if (NULL != mb_handles_[i].inst_) {
            if (mb_handles_[i].inst_->cache_id_ == cache_id) {
              mb_handles_[i].score_ = (mb_handles_[i].score_ / double(old_priority) * double(new_priority));
            }
          }
          hazptr_holder.release();
        }
      } // hazptr_holder
    }
  }

  return ret;
}

// implement functions of ObIKVStore<ObKVMemBlockHandle>
int ObKVCacheStore::alloc(ObKVCacheInst &inst, const enum ObKVCachePolicy policy,
    const int64_t block_size, ObKVMemBlockHandle *&mb_handle)
{
  return alloc_mbhandle(inst, policy, block_size, mb_handle);
}

int ObKVCacheStore::free(ObKVMemBlockHandle *mb_handle)
{
  int ret = common::OB_SUCCESS;
  if (NULL == mb_handle) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", K(ret), KP(mb_handle));
  } else if (FALSE_IT(mb_handle->set_full(0))) {
  } else if (OB_LIKELY(GCONF._enable_kvcache_hazard_pointer)) {
    mb_handle->retire();
  } else {
    if (ATOMIC_BCAS(&mb_handle->status_, FULL, FREE)) {
      ATOMIC_STORE_RLX(&mb_handle->seq_num_, mb_handle->seq_num_ + 1);
      de_handle_ref(mb_handle);
    }
  }
  return ret;
}

ObKVMemBlockHandle *&ObKVCacheStore::get_curr_mb(
    ObKVCacheInst &inst, const enum ObKVCachePolicy policy)
{
  return inst.handles_[policy];
}

bool ObKVCacheStore::mb_status_match(ObKVCacheInst &inst,
    const enum ObKVCachePolicy policy, ObKVMemBlockHandle *mb_handle)
{
  return &inst == mb_handle->inst_ && policy == mb_handle->policy_;
}


int ObKVCacheStore::refresh_score()
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t priority = 1;
  double score = 0;

  if (OB_FAIL(insts_->refresh_score())) {
    COMMON_LOG(WARN, "Fail to refresh inst score, ", K(ret));
  } else {
    HazptrHolder hazptr_holder;
    bool protect_success;
    for (i = 0; OB_SUCC(ret) && i < cur_mb_num_; i++) {
      if (OB_FAIL(hazptr_holder.protect(protect_success, &mb_handles_[i]))) {
        COMMON_LOG(WARN, "failed to protect mb_handle");
      } else if (protect_success) {
        if (NULL != mb_handles_[i].inst_) {
          priority = mb_handles_[i].inst_->status_.config_->priority_;
          score = mb_handles_[i].score_;
          score = score * CACHE_SCORE_DECAY_FACTOR + (double)(mb_handles_[i].recent_get_cnt_ * priority);
          mb_handles_[i].score_ = score;
          ATOMIC_STORE(&mb_handles_[i].recent_get_cnt_, 0);
        }
        hazptr_holder.release();
      }
    }
  }
  return ret;
}

void ObKVCacheStore::WashCallBack::operator()(ObKVMemBlockHandle *mb_handle)
{
  freed_mem_size_ += mb_handle->mem_block_->get_hold_size();
  store_.free_mbhandle(mb_handle, true);
}

bool ObKVCacheStore::wash()
{
  bool is_wash_valid = true;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  WashMap::iterator wash_iter;
  TenantWashInfo *tenant_wash_info = NULL;

  // Record time cost of every step of wash 
  int64_t compute_wash_size_time = 0;
  int64_t refresh_score_time = 0;
  // int64_t wash_sort_time = 0;
  int64_t wash_time = 0;
  int64_t reclaim_time = 0;
  int64_t start_time = 0;
  int64_t current_time = 0;
  uint64_t reclaimed_size = 0;

  // Record how many times refresh is skipped
  static int64_t skip_refresh = 0;

  if (-1 == wash_itid_) {
    wash_itid_ = get_itid();
  }
  lib::ObMutexGuard guard(wash_out_lock_);
  reuse_wash_structs();

  //compute the wash size of each tenant
  start_time = ObTimeUtility::current_time();
  is_wash_valid = compute_tenant_wash_size();
  current_time = ObTimeUtility::current_time();
  compute_wash_size_time = current_time - start_time;
  start_time = current_time;
  
  if (is_wash_valid || skip_refresh++ >= MAX_SKIP_REFRESH_TIMES) {
    // refresh score of every mb_handle
    // ignore
    refresh_score();
    if (is_wash_valid) {
      current_time = ObTimeUtility::current_time();
      refresh_score_time = current_time - start_time;
      start_time = current_time;
      skip_refresh = 0;
      tmp_washbale_size_info_.reuse();
      //sort mb_handles to wash
      HazptrHolder hazptr_holder;
      bool protect_success = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_mb_num_; ++i) {
        do {
          ret = hazptr_holder.protect(protect_success, &mb_handles_[i]);
        } while (OB_UNLIKELY(OB_ALLOCATE_MEMORY_FAILED == ret));
        if (OB_FAIL(ret)) {
          COMMON_LOG(WARN, "failed to protect mb_handle");
        } else if (protect_success) {
          if (OB_ISNULL(mb_handles_[i].inst_)) {
            COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "mb_handle.inst_ is null!", K(mb_handles_[i]));
          } else {
            enum ObKVMBHandleStatus status = mb_handles_[i].get_status();
            uint64_t tenant_id = mb_handles_[i].inst_->tenant_id_;
            if (OB_SUCC(tenant_wash_map_.get(tenant_id, tenant_wash_info))) {
              if (FULL == status) {
                if (OB_TMP_FAIL(tmp_washbale_size_info_.add_washable_size(
                        tenant_id,
                        mb_handles_[i].mem_block_->get_hold_size()))) {
                  COMMON_LOG(WARN,
                             "Fail to add tenant washable size",
                             K(tmp_ret),
                             K(tenant_id));
                }
                if (OB_FAIL(tenant_wash_info->add(&mb_handles_[i]))) {
                  COMMON_LOG(WARN, "add failed", K(ret));
                }
              }
            } else if (OB_ENTRY_NOT_EXIST == ret) {
              COMMON_LOG(INFO,
                         "Wash memory of tenant not exist, ",
                         "tenant_id",
                         mb_handles_[i].inst_->tenant_id_,
                         "cache id",
                         mb_handles_[i].inst_->cache_id_,
                         "wash_size",
                         mb_handles_[i].mem_block_->get_hold_size());
              wash_mb(&mb_handles_[i]);
            } else {
              COMMON_LOG(ERROR, "Unexpected error, ", K(ret));
            }
          }
          //any error should not break washing, so reset ret to OB_SUCCESS
          ret = OB_SUCCESS;
          hazptr_holder.release();
        }
      }
      if (OB_LIKELY(OB_SUCCESS == tmp_ret)) {
        washbale_size_info_.copy_from(tmp_washbale_size_info_);
      }
      for (wash_iter = tenant_wash_map_.begin(); wash_iter != tenant_wash_map_.end(); ++wash_iter) {
        wash_iter->second->normalize();
      }

      //wash memory in tenant wash heap
      for (wash_iter = tenant_wash_map_.begin(); wash_iter != tenant_wash_map_.end(); ++wash_iter) {
        tenant_wash_info = wash_iter->second;
        if (NULL != tenant_wash_info && tenant_wash_info->wash_heap_.mb_cnt_ > 0) {
          wash_mbs(tenant_wash_info->wash_heap_);
          COMMON_LOG(INFO, "Wash memory, ",
              "tenant_id", wash_iter->first,
              "cache_size", tenant_wash_info->cache_size_,
              "lower_mem_limit", tenant_wash_info->lower_limit_,
              "upper_mem_limit", tenant_wash_info->upper_limit_,
              "min_wash_size", tenant_wash_info->min_wash_size_,
              "max_wash_size", tenant_wash_info->max_wash_size_,
              "mem_usage", tenant_wash_info->mem_usage_,
              "reserve_mem", tenant_wash_info->reserve_mem_,
              "wash_size", tenant_wash_info->wash_size_);
        }
      }
      wash_time = ObTimeUtility::current_time() - start_time;
      WashCallBack callback(*this, reclaimed_size);
      reclaim_time = ObTimeUtility::current_time();
      HazardDomain::get_instance().reclaim(callback);
      reclaim_time = ObTimeUtility::current_time() - reclaim_time;
      purge_mb_handle_retire_station();
      COMMON_LOG(INFO,
          "Wash time detail, ",
          K(compute_wash_size_time),
          K(refresh_score_time),
          K(wash_time),
          K(reclaim_time),
          K(reclaimed_size));
    }
  }

  return is_wash_valid;
}

int ObKVCacheStore::get_avg_cache_item_size(const uint64_t tenant_id, const int64_t cache_id, int64_t &avg_cache_item_size)
{
  int ret = OB_SUCCESS;
  int64_t total_cache_size = 0;
  int64_t total_kv_cnt = 0;
  if (OB_INVALID_ID == tenant_id || cache_id < 0 || cache_id >= MAX_CACHE_NUM) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(cache_id));
  } else {
    HazptrHolder hazptr_holder;
    bool protect_success;
    for (int64_t i = 0; i < cur_mb_num_; ++i) {
      if (OB_FAIL(hazptr_holder.protect(protect_success, &mb_handles_[i]))) {
        COMMON_LOG(WARN, "failed to protect mb_handle");
      } else if (protect_success) {
        if (mb_handles_[i].inst_->tenant_id_ == tenant_id && mb_handles_[i].inst_->cache_id_ == cache_id) {
          enum ObKVMBHandleStatus status = mb_handles_[i].get_status();
          if (FULL == status) {
            total_cache_size += mb_handles_[i].mem_block_->get_size();
            total_kv_cnt += mb_handles_[i].mem_block_->get_kv_cnt();
          }
        }
        hazptr_holder.release();
      }
    }

    if (total_kv_cnt <= 0) {
      avg_cache_item_size = 0;
    } else {
      avg_cache_item_size = total_cache_size / total_kv_cnt;
    }
    COMMON_LOG(DEBUG, "avg cache item size", K(tenant_id), K(cache_id), K(total_cache_size), K(total_kv_cnt));
  }
  return ret;
}

int ObKVCacheStore::get_washable_size(const uint64_t tenant_id, int64_t &washable_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(washbale_size_info_.get_size(tenant_id, washable_size))) {
    COMMON_LOG(WARN, "Fail to get tenant wash info", K(ret), K(tenant_id));
  }
  COMMON_LOG(DEBUG, "get washable size details", K(ret), K(tenant_id), K(washable_size));

  return ret;
}

int ObKVCacheStore::flush_washable_mbs()
{
  int ret = OB_SUCCESS, last_error_code = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore has not been inited", K(ret));
  } else {
    ObSEArray<uint64_t, FLUSH_PRESERVE_TENANT_NUM> tenant_ids;
    if (OB_FAIL(mem_limit_getter_->get_all_tenant_id(tenant_ids))) {
      COMMON_LOG(WARN, "Fail to get all tenant ids", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      // record error code in last_error_code
      for (int64_t i = 0 ; /* OB_SUCC(ret) && */ i < tenant_ids.count() ; ++i) {
        if (OB_FAIL(tenant_ids.at(i, tenant_id))) {
          COMMON_LOG(WARN, "Fail to get tenant id, continue to flush rest tenants", K(ret), K(i));
        } else if (OB_FAIL(flush_washable_mbs(tenant_id))) {
          COMMON_LOG(WARN, "Fail to flush tenant washable memblock");
        }
        last_error_code = ret;
      }
    }
  }

  return ret == OB_SUCCESS ? last_error_code : ret;
}

int ObKVCacheStore::flush_washable_mbs(const uint64_t tenant_id, const bool force_flush)
{
  int ret = OB_SUCCESS;

  ObICacheWasher::ObCacheMemBlock *flush_blocks = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore has not been inited", K(ret));
  } else if (OB_UNLIKELY(tenant_id <= OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else if (force_flush) {
    lib::ObMutexGuard guard(wash_out_lock_);
    if (OB_FAIL(try_flush_washable_mb(tenant_id, flush_blocks, -1, INT64_MAX, force_flush))) {
      COMMON_LOG(WARN, "Fail to try flush mb", K(ret), K(tenant_id), K(force_flush));
    }
  } else if (OB_FAIL(try_flush_washable_mb(tenant_id, flush_blocks, -1, INT64_MAX, force_flush))) {
    COMMON_LOG(WARN, "Fail to try flush mb", K(ret), K(tenant_id), K(force_flush));
  }

  return ret;
}

void ObKVCacheStore::flush_washable_mbs(const int64_t cache_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore has not been inited", K(ret));
  } else if (cache_id < 0 ) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arument", K(ret), K(cache_id));
  } else {
    ObSEArray<uint64_t, FLUSH_PRESERVE_TENANT_NUM> tenant_ids;
    if (OB_FAIL(mem_limit_getter_->get_all_tenant_id(tenant_ids))) {
      COMMON_LOG(WARN, "Fail to get tenant id, continue to flush rest tenants", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      for (int64_t i = 0 ; i < tenant_ids.count() ; ++i) {
        if (OB_FAIL(tenant_ids.at(i, tenant_id))) {
          COMMON_LOG(WARN, "Fail to get tenant id", K(ret), K(i));
        } else {
          flush_washable_mbs(tenant_id, cache_id);
        }
      } 
    }
  }

}

bool ObKVCacheStore::add_handle_ref(ObKVMemBlockHandle *mb_handle, const int64_t seq_num) const
{
  bool bret = false;
  if (NULL != mb_handle) {
    if (seq_num != mb_handle->get_seq_num()) {
      bret = false;
    } else {
      ATOMIC_FAA(&mb_handle->ref_cnt_, 1);
      if (seq_num != ATOMIC_LOAD_RLX(&mb_handle->seq_num_)) {
        ATOMIC_SAF(&mb_handle->ref_cnt_, 1);
        bret = false;
      } else {
        bret = true;
      }
    }
  }
  return bret;
}

bool ObKVCacheStore::add_handle_ref(ObKVMemBlockHandle *mb_handle) const
{
  bool bret = false;
  if (NULL != mb_handle) {
    if (FREE == mb_handle->get_status()) {
      bret = false;
    } else {
      ATOMIC_FAA(&mb_handle->ref_cnt_, 1);
      if (FREE == ATOMIC_LOAD_RLX(&mb_handle->status_)) {
        ATOMIC_SAF(&mb_handle->ref_cnt_, 1);
        bret = false;
      } else {
        bret = true;
      }
    }
  }
  return bret;
}


int64_t ObKVCacheStore::de_handle_ref(ObKVMemBlockHandle *mb_handle, const bool do_retire)
{
  int64_t ref_cnt = 0;
  if (0 == (ref_cnt = ATOMIC_SAF(&mb_handle->ref_cnt_, 1))) {
    int tmp_ret = 0;
    if (OB_TMP_FAIL(free_mbhandle(mb_handle, do_retire))) {
      COMMON_LOG_RET(WARN, tmp_ret, "free_mbhandle failed");
    }
  }
  return ref_cnt;
}

void ObKVCacheStore::flush_washable_mbs(const uint64_t tenant_id, const int64_t cache_id)
{
  int ret = OB_SUCCESS;

  ObICacheWasher::ObCacheMemBlock *flush_blocks = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCaheStore has not been inited", K(ret), K(tenant_id));
  } else if (tenant_id <= OB_INVALID_TENANT_ID || cache_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(tenant_id), K(cache_id));
  } else if (OB_FAIL(try_flush_washable_mb(tenant_id, flush_blocks, cache_id))) {
    COMMON_LOG(WARN, "Fail to try flush mb", K(ret), K(tenant_id));
  }
}

int ObKVCacheStore::sync_wash_mbs(const uint64_t tenant_id,
                                  const int64_t size_to_wash,
                                  ObICacheWasher::ObCacheMemBlock *&wash_blocks,
                                  const int64_t wash_cache_id)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || size_to_wash <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(size_to_wash));
  } else if (OB_FAIL(try_flush_washable_mb(tenant_id, wash_blocks, wash_cache_id, size_to_wash))) {
    if (ret != OB_CACHE_FREE_BLOCK_NOT_ENOUGH) {
      COMMON_LOG(WARN, "Fail to try flush mb", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(wash_blocks)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "wash_blocks is null!");
  }

  return ret;
}

void ObKVCacheStore::SyncWashCallBack::operator()(ObKVMemBlockHandle* handle)
{
  int ret = OB_SUCCESS;
  dl_del(handle);
  retire_list_.push(&handle->retire_link_);
  void* buf;
  int64_t hold_size;
  if (OB_FAIL(store_.do_wash_mb(handle, buf, hold_size))) {
    COMMON_LOG(WARN, "Fail to wash memblock", K(ret));
  } else if (size_washed_ >= size_to_wash_ || handle->inst_->tenant_id_ != tenant_id_) {
    store_.free_mb(*handle->inst_->mb_list_handle_.get_resource_handle(), handle->inst_->tenant_id_, buf);
  } else {
    ObICacheWasher::ObCacheMemBlock* wash_block = new (buf) ObICacheWasher::ObCacheMemBlock();
    size_washed_ += hold_size;
    wash_block->next_ = wash_blocks_;
    wash_blocks_ = wash_block;
  }
}

void ObKVCacheStore::free_mbs(lib::ObTenantResourceMgrHandle& resource_handle, int64_t tenant_id,
    lib::ObICacheWasher::ObCacheMemBlock* wash_blocks)
{
  ObICacheWasher::ObCacheMemBlock* wash_block = wash_blocks;
  ObICacheWasher::ObCacheMemBlock* next = NULL;
  while (NULL != wash_block) {
    next = wash_block->next_;
    free_mb(resource_handle, tenant_id, reinterpret_cast<void*>(wash_block));
    wash_block = next;
  }
}

int ObKVCacheStore::try_flush_washable_mb(const uint64_t tenant_id, ObICacheWasher::ObCacheMemBlock*& wash_blocks,
    const int64_t cache_id, const int64_t size_to_wash, const bool force_flush)
{
  int ret = OB_SUCCESS;

  ObTenantMBListHandle list_handle;
  ObDLink *head = nullptr;
  if (OB_FAIL(insts_->get_mb_list(tenant_id, list_handle, size_to_wash != INT64_MAX))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "Fail to get tenant memblock list", K(ret), K(tenant_id));
    }
  } else if (NULL == (head = list_handle.get_head())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Tenant memblock list is null", K(ret), K(tenant_id));
  } else {
    int64_t size_washed = 0;
    const int64_t start = ObTimeUtility::current_time();
    const bool is_flush = size_to_wash == INT64_MAX;
    if (OB_FAIL(inner_flush_washable_mb(cache_id, size_to_wash, size_washed, wash_blocks, list_handle, force_flush))) {
      COMMON_LOG(WARN,
          "failed to inner flush washable mb",
          K(ret),
          K(tenant_id),
          K(cache_id),
          K(size_to_wash),
          K(force_flush));
      free_mbs(*list_handle.get_resource_handle(), tenant_id, wash_blocks);
      wash_blocks = nullptr;
    } else if (is_flush) {
      // flush
      free_mbs(*list_handle.get_resource_handle(), tenant_id, wash_blocks);
      wash_blocks = nullptr;
    } else {
      // sync wash
      if (OB_SUCC(ret) && size_washed < size_to_wash) {
        ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
        INIT_SUCC(tmp_ret);
        if (REACH_TIME_INTERVAL(3 * 1000 * 1000 /* 3s */)) {
          if (OB_TMP_FAIL(print_tenant_memblock_info(head))) {
            COMMON_LOG(WARN, "Fail to print tenant memblock info", K(tmp_ret));
          }
        }
        COMMON_LOG(INFO, "can not find enough memory block to wash", K(ret), K(size_washed), K(size_to_wash));
      }
      EVENT_ADD(KVCACHE_SYNC_WASH_TIME, ObTimeUtility::current_time() - start);
      EVENT_INC(KVCACHE_SYNC_WASH_COUNT);
    }

    if (OB_FAIL(ret)) {
      // free memory of memory blocks washed if any error occur
      free_mbs(*list_handle.get_resource_handle(), tenant_id, wash_blocks);
      wash_blocks = nullptr;
    }

    if (!is_flush && (OB_CACHE_FREE_BLOCK_NOT_ENOUGH == ret || OB_SYNC_WASH_MB_TIMEOUT == ret)) {
      if (REACH_TIME_INTERVAL(500 * 1000 /* 500 ms */)) {
        insts_->print_tenant_cache_info(tenant_id);
      }
    }

    COMMON_LOG(INFO,
        "ObKVCache try flush washable memblock details",
        K(ret),
        K(force_flush),
        K(tenant_id),
        K(cache_id),
        K(size_washed),
        K(size_to_wash));
  }

  return ret;
}

int ObKVCacheStore::inner_flush_washable_mb(const int64_t cache_id, const int64_t size_to_wash, int64_t& size_washed,
    lib::ObICacheWasher::ObCacheMemBlock*& wash_blocks, ObTenantMBListHandle& list_handle, bool force_flush)
{
  int ret = OB_SUCCESS;
  constexpr static int64_t check_interval = 512;
  const static int64_t retire_wash_retry_ratio = 2;
  const int64_t start = ObTimeUtility::current_time();
  int64_t size_retired = 0;
  int64_t size_to_retire = size_to_wash;
  int64_t check_idx = 0;
  HazardList retire_list;
  ObDLink* head = list_handle.get_head();
  int64_t tenant_id = list_handle.list_->tenant_id_;
  int64_t timeout_us = INT_MAX == size_to_wash ? FLUSH_MB_TIMEOUT_US : SYNC_WASH_MB_TIMEOUT_US;
  if (OB_LIKELY(GCONF._enable_kvcache_hazard_pointer)) {
    // retire memblock and reclaim until
    // 1. wash out enough memory, or
    // 2. iterate over the whole tenant memblock list, or
    // 3. time out
    ObKVMemBlockHandle* handle = nullptr;
    HazptrHolder hazptr_holder;
    bool protect_success;
    while (OB_SUCC(ret) && size_washed < size_to_wash && head != handle) {
      {
        QClockGuard guard(get_qclock());
        handle = static_cast<ObKVMemBlockHandle*>(link_next(head));
        while (OB_SUCC(ret) && size_retired < size_to_retire && head != handle) {
          bool can_try_wash = false;
          int64_t status = -1;
          int64_t size;
          do {
            ret = hazptr_holder.protect(protect_success, handle);
          } while (OB_UNLIKELY(OB_ALLOCATE_MEMORY_FAILED == ret));
          if (OB_FAIL(ret)) {
            COMMON_LOG(WARN, "failed to protect mb_handle", KP(handle));
          }
          if (protect_success) {
            status = handle->get_status();
            if (FULL == status) {
              if (-1 == cache_id || cache_id == handle->inst_->cache_id_) {
                size = handle->mem_block_->get_size();
                can_try_wash = true;
              }
            }
            hazptr_holder.release();
          }
          if (can_try_wash) {
            if (handle->retire()) {
              size_retired += size;
            }
          } else {
            if (force_flush && protect_success) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN,
                  "Can not sync wash memblock of erased tenant",
                  K(ret),
                  K(tenant_id),
                  KPC(handle),
                  K(status),
                  KPC(handle->inst_));
            }
          }
          handle = static_cast<ObKVMemBlockHandle*>(link_next(handle));

          if (!force_flush && check_idx > 0 && 0 == check_idx % check_interval) {
            const int64_t cost = ObTimeUtility::current_time() - start;
            if (cost > timeout_us) {
              ret = OB_SYNC_WASH_MB_TIMEOUT;
              COMMON_LOG(WARN, "sync wash mb timeout", K(cost), K(timeout_us));
            }
          }
          ++check_idx;
        }
      }  // qclock guard

      if (OB_FAIL(ret) && OB_SYNC_WASH_MB_TIMEOUT != ret) {
      } else if (size_retired >= size_to_wash - size_washed || size_to_wash == INT64_MAX || HazardDomain::get_instance().get_retired_size() > size_to_wash - size_washed) {
        // do recliam if has retired enough memory
        int64_t start_time = ObTimeUtility::current_time();
        SyncWashCallBack callback(*this, retire_list, wash_blocks, size_washed, size_to_wash, tenant_id);
        // avoid reclaiming while holding qclock
        HazardDomain::get_instance().reclaim(callback);
        int64_t reclaim_time = ObTimeUtility::current_time() - start_time;
        COMMON_LOG(INFO, "KVCache sync wash / flush reclaim", K(size_washed), K(size_to_wash), K(reclaim_time));
        if (size_to_wash == INT64_MAX) {
          // flush
          break;
        } else if (size_washed < size_to_wash) {
          size_to_retire = (size_to_wash - size_washed) * retire_wash_retry_ratio;
          size_retired = 0;
        }
      }
    }
  } else {
    // try wash memblock that can be washed until
    // 1. wash out enough memory, or
    // 2. iterate over the whole tenant memblock list, or
    // 3. time out
    QClockGuard guard(get_qclock());
    ObKVMemBlockHandle* handle = static_cast<ObKVMemBlockHandle*>(link_next(head));
    while (OB_SUCC(ret) && size_washed < size_to_wash && head != handle) {
      bool can_try_wash = false, add_ref_success;
      int64_t ref_cnt = -1;
      int64_t status = -1;
      if ((add_ref_success = add_handle_ref(handle))) {
        status = handle->get_status();
        ref_cnt = handle->ref_cnt_;
        if (FULL == status && 2 == ref_cnt) {
          if (-1 == cache_id || cache_id == handle->inst_->cache_id_) {
            can_try_wash = true;
          }
        }
        if (0 == de_handle_ref(handle)) {
          can_try_wash = false;
        }
      }
      if (can_try_wash) {
        void* buf = nullptr;
        int64_t mb_size = 0;
        if (try_wash_mb(handle, tenant_id, buf, mb_size)) {
          if (nullptr == buf) {
            ret = OB_ERR_UNDEFINED;
            COMMON_LOG(ERROR, "Try wash memblock is null", K(ret), K(tenant_id));
          } else {
            size_washed += mb_size;
            ObICacheWasher::ObCacheMemBlock* mem_block = new (buf) ObICacheWasher::ObCacheMemBlock();
            mem_block->next_ = wash_blocks;
            wash_blocks = mem_block;
          }
          dl_del(handle);
          retire_list.push(&handle->retire_link_);
        } else if (force_flush) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN,
              "Fail to try wash memblock.",
              K(ret),
              K(tenant_id),
              KPC(handle),
              K(status),
              K(ref_cnt),
              KPC(handle->inst_));
        }
      } else if (force_flush && add_ref_success) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN,
            "Can not sync wash memblock of erased tenant",
            K(ret),
            K(tenant_id),
            KPC(handle),
            K(status),
            K(ref_cnt),
            KPC(handle->inst_));
      }
      handle = static_cast<ObKVMemBlockHandle*>(link_next(handle));

      if (!force_flush && check_idx > 0 && 0 == check_idx % check_interval) {
        const int64_t cost = ObTimeUtility::current_time() - start;
        if (cost > timeout_us) {
          ret = OB_SYNC_WASH_MB_TIMEOUT;
          COMMON_LOG(WARN, "sync wash mb timeout", K(cost), K(timeout_us));
        }
      }
      ++check_idx;
    }
  }
  retire_mb_handles(retire_list, true /* do retire */);
  return ret;
}

int ObKVCacheStore::inner_push_memblock_info(const ObKVMemBlockHandle &handle, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos, int64_t tenant_id)
{
  INIT_SUCC(ret);
  ObKVCacheInst* inst = handle.inst_;
  ObKVStoreMemBlock* memblock = handle.mem_block_;
  if (OB_SYS_TENANT_ID == tenant_id || inst->tenant_id_ == tenant_id) {
    ObKVCacheStoreMemblockInfo mb_info;
    STRNCPY(mb_info.cache_name_, inst->status_.config_->cache_name_, MAX_CACHE_NAME_LENGTH - 1);
    mb_info.tenant_id_ = inst->tenant_id_;
    mb_info.cache_id_ = inst->cache_id_;
    mb_info.ref_count_ = ATOMIC_LOAD_RLX(&handle.ref_cnt_);
    mb_info.using_status_ = handle.get_status();
    mb_info.policy_ = handle.policy_;
    mb_info.kv_cnt_ = handle.kv_cnt_;
    mb_info.get_cnt_ = handle.get_cnt_;
    mb_info.recent_get_cnt_ = handle.recent_get_cnt_;
    mb_info.priority_ = inst->status_.config_->priority_;
    mb_info.score_ = handle.score_;
    mb_info.align_size_ = memblock->get_hold_size();
    if (OB_UNLIKELY(0 > snprintf(mb_info.memblock_ptr_, 32, "%p", memblock))) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "Fail to snprintf memblock pointer", K(ret), K(errno), KERRNOMSG(errno));
    } else if (OB_FAIL(memblock_infos.push_back(mb_info))) {
      COMMON_LOG(WARN, "Fail to push memblock info", K(ret), K(mb_info));
    }
  }

  return ret;
}

void ObKVCacheStore::purge_mb_handle_retire_station()
{
  HazardList reclaim_list;
  get_retire_station().purge(reclaim_list);
  reuse_mb_handles(reclaim_list);
}

int ObKVCacheStore::get_memblock_info(const uint64_t tenant_id, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore is not inited", K(ret));
  } else {
    bool protect_success;
    HazptrHolder hazptr_holder;
    for (int i = 0; OB_SUCC(ret) && i < cur_mb_num_; ++i) {
      ObKVMemBlockHandle& handle = mb_handles_[i];
      if (OB_FAIL(hazptr_holder.protect(protect_success, &handle))) {
        COMMON_LOG(WARN, "Failed to protect memblock", K(ret));
      } else if (!protect_success) {
      } else if (OB_FAIL(inner_push_memblock_info(handle, memblock_infos, tenant_id))) {
        COMMON_LOG(WARN, "Failed to inner push memblock info", K(ret));
      }
      if (protect_success) {
        hazptr_holder.release();
      }
    }
  }

  return ret;
}

int ObKVCacheStore::print_tenant_memblock_info(ObDLink* head)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore is not inited", K(ret));
  } else if (nullptr == head) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unexpected nullptr", K(ret), KP(head));
  } else {
    ContextParam param;
    param.set_mem_attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_TEMP_VARIABLES);
    CREATE_WITH_TEMP_CONTEXT(param) {
      static const int64_t BUFLEN = 1 << 18;
      char *buf = (char *)ctxalp(BUFLEN);
      HazardList retire_list;
      int64_t ctx_pos = 0;
      if (nullptr == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Fail to allocate memory for print tenant memblock info", K(ret), KP(buf));
      } else {
        QClockGuard guard(get_qclock());
        ObKVMemBlockHandle *handle = static_cast<ObKVMemBlockHandle *>(link_next(head));
        HazptrHolder hazptr_holder;
        bool protect_success;
        while (OB_SUCC(ret) && head != handle) {
          if (OB_FAIL(hazptr_holder.protect(protect_success, handle))) {
            COMMON_LOG(WARN, "failed to protect mb_handle", KP(handle));
          } else if (protect_success) {
            if (OB_FAIL(databuff_printf(buf, BUFLEN, ctx_pos,
                "[CACHE-SYNC-WASH] tenant_id=%8ld | cache_id=%8ld | status=%8d | policy=%8d | kv_cnt=%8ld | get_cnt=%8ld | score=%8lf |\n",
                handle->inst_->tenant_id_,
                handle->inst_->cache_id_,
                handle->get_status(),
                handle->policy_,
                handle->kv_cnt_,
                handle->get_cnt_,
                handle->score_))) {
              COMMON_LOG(WARN, "Fail to print tenant memblock info", K(ret), K(ctx_pos));
            }
            hazptr_holder.release();
          }
          handle = static_cast<ObKVMemBlockHandle *>(link_next(handle));
        }
      } // qclock guard
      if (OB_SUCC(ret)) {
        HazardDomain::get_instance().print_info();
        _OB_LOG(WARN, "[CACHE-SYNC-WASH] len: %8ld tenant sync wash failed, cache memblock info: \n%s", ctx_pos, buf);
      }
    }
  }
  return ret;
}

int ObKVCacheStore::alloc_mbhandle(ObKVCacheInst &inst,
    const int64_t block_size, ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  mb_handle = NULL;
  const enum ObKVCachePolicy policy = LRU;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_mbhandle(inst, policy, block_size, mb_handle))) {
    LOG_WARN("alloc_mbhandle failed", K(ret), K(policy), K(block_size));
  }
  return ret;
}

int ObKVCacheStore::alloc_mbhandle(ObKVCacheInst &inst, ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  mb_handle = NULL;
  const enum ObKVCachePolicy policy = LRU;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_mbhandle(inst, policy, block_size_, mb_handle))) {
    LOG_WARN("alloc_mbhandle failed", K(ret), K(policy), K_(block_size));
  }
  return ret;
}

int ObKVCacheStore::alloc_mbhandle(const ObKVCacheInstKey &inst_key,
                                   ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  mb_handle = NULL;
  ObKVCacheInstHandle inst_handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(insts_->get_cache_inst(inst_key, inst_handle))) {
    LOG_WARN("get_cache_inst failed", K(ret), K(inst_key));
  } else if (OB_FAIL(alloc_mbhandle(*inst_handle.get_inst(), block_size_, mb_handle))) {
    LOG_WARN("alloc_mbhandle failed", K(ret), K_(block_size));
  }
  return ret;
}

int ObKVCacheStore::free_mbhandle(ObKVMemBlockHandle *mb_handle, const bool do_retire)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(mb_handle));
  } else {
    void *buf = NULL;
    int64_t mb_size = 0;
    const uint64_t tenant_id = mb_handle->inst_->tenant_id_;
    if (OB_FAIL(do_wash_mb(mb_handle, buf, mb_size))) {
      COMMON_LOG(ERROR, "do_wash_mb failed", K(ret));
    } else {
      free_mb(*mb_handle->inst_->mb_list_handle_.get_resource_handle(), tenant_id, buf);
      if (OB_FAIL(remove_mb_handle(mb_handle, do_retire))) {
        COMMON_LOG(WARN, "remove_mb failed", K(ret));
      }
    }
  }
  return ret;
}

ObKVMemBlockHandle *ObKVCacheStore::alloc_mbhandle(
  ObKVCacheInst &inst,
  const enum ObKVCachePolicy policy,
  const int64_t block_size)
{
  ObKVMemBlockHandle *mb_handle = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_mbhandle(inst, policy, block_size, mb_handle))) {
    COMMON_LOG(WARN, "alloc_mbhandle failed", K(ret), K(policy), K(block_size));
  }
  return mb_handle;
}

int ObKVCacheStore::alloc_mbhandle(
  ObKVCacheInst &inst,
  const enum ObKVCachePolicy policy,
  const int64_t block_size,
  ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  mb_handle = NULL;
  ObKVStoreMemBlock *mem_block = NULL;
  char *buf = NULL;
  const uint64_t tenant_id = inst.tenant_id_;
  const int64_t memory_limit_pct = inst.get_memory_limit_pct();
  const int64_t cache_store_size = ATOMIC_AAF(&inst.status_.store_size_, block_size);

  if (!inst.mb_list_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mb_list_handle is invalid", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (NULL == (buf = static_cast<char*>(alloc_mb(
            *inst.mb_list_handle_.get_resource_handle(), tenant_id, block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Fail to allocate memory, ", K(block_size), K(ret));
  } else {
    mem_block = new (buf) ObKVStoreMemBlock(buf + sizeof(ObKVStoreMemBlock),
        block_size - sizeof(ObKVStoreMemBlock));
    while (OB_FAIL(mb_handles_pool_.pop(mb_handle))) {
      if (OB_UNLIKELY(!try_supply_mb(SUPPLY_MB_NUM_ONCE))) {
        break;
      }
    }

    if (OB_FAIL(ret)) {
      mem_block->~ObKVStoreMemBlock();
      free_mb(*inst.mb_list_handle_.get_resource_handle(), tenant_id, mem_block);
      COMMON_LOG(WARN, "Fail to pop mb_handle, ", K(ret));
    } else {
      if (LRU == policy) {
        (void) ATOMIC_AAF(&inst.status_.lru_mb_cnt_, 1);
      } else {
        (void) ATOMIC_AAF(&inst.status_.lfu_mb_cnt_, 1);
      }
      mb_handle->inst_ = &inst;
      mb_handle->policy_ = policy;
      mb_handle->mem_block_ = mem_block;
      mb_handle->last_modified_time_us_ = ObTimeUtility::current_time_us();
      if (OB_UNLIKELY(!GCONF._enable_kvcache_hazard_pointer)) {
        ATOMIC_AAF(&mb_handle->ref_cnt_, 1);
      } else {
        WEAK_BARRIER();
      }
      ATOMIC_STORE_RLX(&mb_handle->status_, ObKVMBHandleStatus::USING);
    }
  }

  if (OB_SUCC(ret)) {
    ObDLink *head = NULL;
    if (NULL == (head = inst.get_mb_list())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "head is null", K(ret), "tenant_id", inst.tenant_id_);
    } else if (OB_FAIL(insert_mb_handle(head, mb_handle))) {
      COMMON_LOG(WARN, "insert_mb_handle failed", K(ret));
    }
  } else {
    ATOMIC_SAF(&inst.status_.store_size_, block_size);
  }

  return ret;
}

bool ObKVCacheStore::compute_tenant_wash_size()
{
  bool is_wash_valid = false;

  int64_t total_tenant_wash_block_count = 0;

  int ret = OB_SUCCESS;
  int64_t tenant_min_wash_size = 0;
  int64_t tenant_max_wash_size = 0;
  uint64_t tenant_id = 0;
  int64_t lower_limit = 0;
  int64_t upper_limit = 0;
  int64_t mem_usage = 0;
  int64_t reserve_mem = 0;
  ObMemAttr mem_attr;
  TenantWashInfo *tenant_wash_info = NULL;
  WashMap::iterator wash_iter;
  int64_t global_cache_size = 0;
  int64_t sys_total_wash_size = MIN(lib::get_memory_used() - lib::get_memory_limit(), 0) + lib::ob_get_reserved_urgent_memory();

  if (OB_FAIL(mem_limit_getter_->get_all_tenant_id(tenant_ids_))) {
    COMMON_LOG(WARN, "Fail to get all tenant ids, ", K(ret));
  } else if (OB_FAIL(insts_->get_cache_info(inst_handles_))) {
    COMMON_LOG(WARN, "Fail to get all cache infos, ", K(ret));
  }

  //get tenant memory lower limit and upper limit
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    if (OB_FAIL(tenant_ids_.at(i, tenant_id))) {
      COMMON_LOG(WARN, "Fail to get ith tenant id, ", K(ret), K(i));
    } else if (OB_FAIL(mem_limit_getter_->get_tenant_mem_limit(tenant_id,
        lower_limit, upper_limit))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        lower_limit = 0;
        upper_limit = 0;
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "Fail to get tenant memory limit, ", K(tenant_id), K(ret));
      }
    } else if (OB_FAIL(wash_info_free_heap_.sbrk(tenant_wash_info))) {
      COMMON_LOG(WARN, "wash_info_free_heap_ sbrk failed", K(ret), K(i));
    } else if (OB_FAIL(tenant_wash_map_.set(tenant_id, tenant_wash_info))) {
      COMMON_LOG(WARN, "Fail to set tenant_wash_info to tenant_wash_map, ", K(ret));
    } else {
      tenant_wash_info->lower_limit_ = lower_limit;
      tenant_wash_info->upper_limit_ = upper_limit;
    }
  }

  ObKVCacheInst * inst = NULL;
  //get tenant cache size
  for (int64_t i = 0; OB_SUCC(ret) && i < inst_handles_.count(); ++i) {
    inst = inst_handles_.at(i).get_inst();
    if (OB_ISNULL(inst)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ObKVCacheInst is NULL", K(ret));
    } else if (OB_FAIL(tenant_wash_map_.get(inst->tenant_id_, tenant_wash_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "Fail to get tenant wash info", K(inst->tenant_id_));
      }
    } else {
      tenant_wash_info->cache_size_ += inst->status_.store_size_;
      global_cache_size += inst->status_.store_size_;
    }
  }

  //identify tenant_min_wash_size and tenant_max_wash_size
  for (wash_iter = tenant_wash_map_.begin(); OB_SUCC(ret) && wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    tenant_id = wash_iter->first;
    tenant_wash_info = wash_iter->second;
    upper_limit = tenant_wash_info->upper_limit_;
    lower_limit = tenant_wash_info->lower_limit_;
    mem_usage = lib::get_tenant_memory_hold(tenant_id);
    tenant_wash_info->mem_usage_ = mem_usage;
    if (upper_limit <= 1024L * 1024L * 1024L) {
      reserve_mem = upper_limit / 10;
    } else {
      reserve_mem = log10(static_cast<double>(upper_limit)/(1024.0 * 1024.0 * 1024.0)) * upper_limit / 20
                    + 100L * 1024L * 1024L;
    }
    tenant_wash_info->reserve_mem_ = reserve_mem;

    //identify min_wash_size
    tenant_wash_info->min_wash_size_ = std::max(0L, mem_usage - upper_limit + reserve_mem);
    if (tenant_wash_info->min_wash_size_ > tenant_wash_info->cache_size_) {
      tenant_wash_info->min_wash_size_ = tenant_wash_info->cache_size_;
    }
    if (tenant_wash_info->min_wash_size_ > 0) {
      tenant_min_wash_size += tenant_wash_info->min_wash_size_;
    }
    //identify max_wash_size
    tenant_wash_info->max_wash_size_ = std::max(0L, mem_usage - lower_limit + reserve_mem);
    if (tenant_wash_info->max_wash_size_ > tenant_wash_info->cache_size_) {
      tenant_wash_info->max_wash_size_ = tenant_wash_info->cache_size_;
    }
    if (tenant_wash_info->max_wash_size_ > 0) {
      tenant_max_wash_size += tenant_wash_info->max_wash_size_;
    }
  }

  //add sys wash size to each tenant
  int64_t heap_size = 0;
  for (wash_iter = tenant_wash_map_.begin(); OB_SUCC(ret) && wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    tenant_id = wash_iter->first;
    tenant_wash_info = wash_iter->second;
    //determine the final wash size
    if (tenant_min_wash_size >= sys_total_wash_size) {
      if (tenant_wash_info->min_wash_size_ > 0) {
        tenant_wash_info->wash_size_ = tenant_wash_info->min_wash_size_;
      }
    } else {
      if (tenant_max_wash_size >= sys_total_wash_size) {
        tenant_wash_info->wash_size_ = tenant_wash_info->min_wash_size_ +
            static_cast<int64_t>(static_cast<double>((sys_total_wash_size - tenant_min_wash_size))
            * (static_cast<double>(tenant_wash_info->max_wash_size_ - tenant_wash_info->min_wash_size_)
            / static_cast<double>(tenant_max_wash_size - tenant_min_wash_size)));
      } else {
        if (tenant_wash_info->max_wash_size_ > 0) {
          tenant_wash_info->wash_size_ = tenant_wash_info->max_wash_size_;
        } else {
          tenant_wash_info->wash_size_ = 0;
        }
        if (global_cache_size > 0) {
          tenant_wash_info->wash_size_ += static_cast<int64_t>(static_cast<double>(sys_total_wash_size - tenant_max_wash_size)
                * (static_cast<double>(tenant_wash_info->cache_size_) / static_cast<double>(global_cache_size)));
        }
      }
    }

    //allocate memory for wash heap
    heap_size = tenant_wash_info->wash_size_ / block_size_;
    if (heap_size > 0) {
      if (OB_FAIL(init_wash_heap(tenant_wash_info->wash_heap_, heap_size))) {
        COMMON_LOG(WARN, "init_wash_heap failed", K(ret), K(heap_size));
      }
    }
    total_tenant_wash_block_count += heap_size;

    if (!is_wash_valid) {
      is_wash_valid = is_tenant_wash_valid(tenant_wash_info->wash_size_, tenant_wash_info->cache_size_);
    }

  }

  if (is_wash_valid || is_global_wash_valid(total_tenant_wash_block_count, global_cache_size)) {
    // If total wash size is large than global wash threshold, the current wash process is invalid.
    is_wash_valid = true;

    // allocate memory for cache wash heaps whose hold size is set, this step will not be executed if wash process is invalid
    for (int64_t i = 0; OB_SUCC(ret) && i < inst_handles_.count(); ++i) {
      inst = inst_handles_.at(i).get_inst();
      if (OB_ISNULL(inst)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "ObKVCacheInst is NULL", K(ret));
      } else if (OB_SUCC(tenant_wash_map_.get(inst->tenant_id_, tenant_wash_info))) {
        if (inst->status_.hold_size_ > 0) {
          const int64_t heap_size = std::min(tenant_wash_info->wash_size_,
                inst->status_.store_size_ - inst->status_.hold_size_) / block_size_;
          if (OB_FAIL(init_wash_heap(tenant_wash_info->cache_wash_heaps_[inst->cache_id_], heap_size))) {
            COMMON_LOG(WARN, "init_wash_heap failed", K(ret), K(heap_size));
          }
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  COMMON_LOG(INFO, "Wash compute wash size", K(is_wash_valid), K(sys_total_wash_size), K(global_cache_size),
      K(tenant_max_wash_size),K(tenant_min_wash_size), K(tenant_ids_));
  return is_wash_valid;
}

bool ObKVCacheStore::is_tenant_wash_valid(const int64_t tenant_wash_size, const int64_t tenant_cache_size)
{
  int64_t threshold = tenant_cache_size >> TENANT_WASH_THRESHOLD_RATIO;
  if (threshold > MAX_TENANT_WASH_THRESHOLD) {
    threshold = MAX_TENANT_WASH_THRESHOLD;
  } else if (threshold < MIN_TENANT_WASH_THRESHOLD) {
    threshold = MIN_TENANT_WASH_THRESHOLD;
  }
  return tenant_wash_size >= threshold;
}

bool ObKVCacheStore::is_global_wash_valid(const int64_t total_tenant_wash_block_count, const int64_t global_cache_size)
{
  int64_t threshold = global_cache_size / block_size_ >> GLOBAL_WASH_THRESHOLD_RATIO;
  if (threshold > MAX_GLOBAL_WASH_THRESHOLD) {
    threshold = MAX_GLOBAL_WASH_THRESHOLD;
  } else if (threshold < MIN_GLOBAL_WASH_THRESHOLD) {
    threshold = MIN_GLOBAL_WASH_THRESHOLD;
  }
  return total_tenant_wash_block_count >= threshold;
}

void ObKVCacheStore::wash_mbs(WashHeap &heap)
{
  if (OB_LIKELY(GCONF._enable_kvcache_hazard_pointer)) {
    ObKVCacheInst* insts[MAX_CACHE_NUM] = {nullptr};
    uint64_t retired_mb_sizes[MAX_CACHE_NUM] = {0};
    uint64_t total_retired_size = 0;
    if (OB_NOT_NULL(heap.heap_) && OB_LIKELY(heap.mb_cnt_ > 0)) {
      ObLink* head = nullptr;
      ObLink* tail = nullptr;
      uint32_t seq_num;
      for (int64_t i = 0; i < heap.mb_cnt_; ++i) {
        if (FULL == heap.heap_[i]->get_status() && ATOMIC_BCAS(&heap.heap_[i]->status_, FULL, FREE)) {
          ATOMIC_STORE_RLX(&heap.heap_[i]->seq_num_, heap.heap_[i]->seq_num_ + 1);
          if (OB_ISNULL(tail)) {
            head = tail = &heap.heap_[i]->retire_link_;
          } else {
            tail->next_ = &heap.heap_[i]->retire_link_;
            tail = tail->next_;
          }
          if (OB_UNLIKELY(0 == retired_mb_sizes[heap.heap_[i]->inst_->cache_id_])) {
            insts[heap.heap_[i]->inst_->cache_id_] = heap.heap_[i]->inst_;
          }
          retired_mb_sizes[heap.heap_[i]->inst_->cache_id_] += heap.heap_[i]->mem_block_->get_hold_size();
        }
      }
      if (OB_NOT_NULL(tail)) {
        tail->next_ = nullptr;
        for (int i = 0; i < MAX_CACHE_NUM; ++i) {
          if (0 != retired_mb_sizes[i]) {
            ATOMIC_FAA(&insts[i]->status_.retired_size_, retired_mb_sizes[i]);
            total_retired_size += retired_mb_sizes[i];
          }
        }
        HazardDomain::get_instance().retire(head, tail, total_retired_size);
      }
    }
  } else {
    for (int64_t i = 0; i < heap.mb_cnt_; ++i) {
      wash_mb(heap.heap_[i]);
    }
  }
}

void ObKVCacheStore::wash_mb(ObKVMemBlockHandle* mb_handle)
{
  if (OB_NOT_NULL(mb_handle)) {
    if (OB_LIKELY(GCONF._enable_kvcache_hazard_pointer)) {
      mb_handle->retire();
    } else {
      if (ATOMIC_BCAS(&mb_handle->status_, FULL, FREE)) {
        ATOMIC_STORE_RLX(&mb_handle->seq_num_, mb_handle->seq_num_ + 1);
        de_handle_ref(mb_handle);
      }
    }
  }
}

bool ObKVCacheStore::try_wash_mb(ObKVMemBlockHandle *mb_handle, const uint64_t tenant_id, void *&buf, int64_t &mb_size)
{
  int ret = OB_SUCCESS;
  bool block_washed = false;
  uint32_t seq_num;
  if (NULL == mb_handle || OB_INVALID_ID == tenant_id) {
    COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid arguments", KP(mb_handle), K(tenant_id));
  } else {
    if (FULL == mb_handle->get_status() && ATOMIC_BCAS(&mb_handle->status_, FULL, FREE)) {
      if (mb_handle->inst_->tenant_id_ == tenant_id) {
        ATOMIC_STORE_RLX(&mb_handle->seq_num_, mb_handle->seq_num_ + 1);
        if (0 != ATOMIC_SAF(&mb_handle->ref_cnt_, 1)) {
        } else if (OB_FAIL(do_wash_mb(mb_handle, buf, mb_size))) {
          COMMON_LOG(ERROR, "do_wash_mb failed", K(ret));
        } else {
          block_washed = true;
        }
      } else {
        if (!ATOMIC_BCAS(&mb_handle->status_, FREE, FULL)) {
          COMMON_LOG(ERROR, "change mb_handle status back to FULL failed");
        }
      }
    }
  }
  return block_washed;
}

int ObKVCacheStore::do_wash_mb(ObKVMemBlockHandle *mb_handle, void *&buf, int64_t &mb_size)
{
  int ret = OB_SUCCESS;
  if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "mb_handle is null", K(ret));
  } else if (NULL == mb_handle->mem_block_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mem_block_ is null", K(ret));
  } else {
    if (NULL != mb_handle->inst_) {
      (void) ATOMIC_SAF(&mb_handle->inst_->status_.store_size_,
                        mb_handle->mem_block_->get_payload_size() + sizeof(ObKVStoreMemBlock));
      if (mb_handle->policy_ == LRU) {
        (void) ATOMIC_SAF(&mb_handle->inst_->status_.lru_mb_cnt_, 1);
      } else {
        (void) ATOMIC_SAF(&mb_handle->inst_->status_.lfu_mb_cnt_, 1);
      }
    }
    buf = mb_handle->mem_block_;
    mb_size = mb_handle->mem_block_->get_hold_size();
    mb_handle->mem_block_->~ObKVStoreMemBlock();
    mb_handle->mem_block_ = NULL;
    mb_handle->last_modified_time_us_ = ObTimeUtility::current_time_us();
  }
  return ret;
}

int ObKVCacheStore::init_wash_heap(WashHeap &heap, const int64_t heap_size)
{
  int ret = OB_SUCCESS;
  heap.mb_cnt_ = 0;
  if (heap_size > 0) {
    heap.heap_size_ = heap_size;
    if (OB_FAIL(mb_ptr_pool_.alloc(heap_size, heap.heap_))) {
      COMMON_LOG(WARN, "mb_ptr_free_heap_ sbrk failed", K(ret), K(heap_size));
    }
  } else {
    heap.heap_size_ = 0;
    heap.heap_ = NULL;
  }
  return ret;
}

int ObKVCacheStore::prepare_wash_structs()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_num = MAX_TENANT_NUM_PER_SERVER;
  const int64_t cache_num = MAX_TENANT_NUM_PER_SERVER * MAX_CACHE_NUM;
  const int64_t bucket_num = DEFAULT_TENANT_BUCKET_NUM;
  const char *label = ObModIds::OB_KVSTORE_CACHE_WASH_STRUCT;
  washable_size_allocator_.set_label(label);
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(tenant_ids_.init(tenant_num, label))) {
    COMMON_LOG(WARN, "tenant_ids_ init failed", K(ret), K(tenant_num), K(label));
  } else if (OB_FAIL(inst_handles_.init(cache_num, label))) {
    COMMON_LOG(WARN, "cache_infos_ init failed", K(ret), K(cache_num), K(label));
  } else if (OB_FAIL(wash_info_free_heap_.init(tenant_num, label))) {
    COMMON_LOG(WARN, "wash_info_free_heap_ init failed", K(ret), K(tenant_num), K(label));
  } else if (OB_FAIL(tenant_wash_map_.init(tenant_num, cache_num, label))) {
    COMMON_LOG(WARN, "tenant_wash_map_ init failed", K(ret), K(tenant_num), K(cache_num), K(label));
  } else if (OB_FAIL(washbale_size_info_.init(tenant_num, bucket_num, washable_size_allocator_))) {
    COMMON_LOG(WARN, "Fail to init washable size info", K(ret));
  } else if (OB_FAIL(tmp_washbale_size_info_.init(tenant_num, bucket_num, washable_size_allocator_))) {
    COMMON_LOG(WARN, "Fail to init tmp washable size info", K(ret));
  } else if (OB_FAIL(mb_ptr_pool_.init(2 * max_mb_num_, label))) {
    COMMON_LOG(WARN, "mb_ptr_pool_ init failed", K(ret), K_(max_mb_num));
  }

  return ret;
}

void ObKVCacheStore::reuse_wash_structs()
{
  int ret = OB_SUCCESS;
  tenant_ids_.reuse();
  inst_handles_.reuse();
  wash_info_free_heap_.reuse();
  tenant_wash_map_.reuse();
  mb_ptr_pool_.reuse();
}

void ObKVCacheStore::destroy_wash_structs()
{
  int ret = OB_SUCCESS;
  tenant_ids_.destroy();
  inst_handles_.destroy();
  wash_info_free_heap_.destroy();
  tenant_wash_map_.destroy();
  mb_ptr_pool_.destroy();
  washbale_size_info_.destroy();
  tmp_washbale_size_info_.destroy();
  washable_size_allocator_.reset();
}

void *ObKVCacheStore::alloc_mb(ObTenantResourceMgrHandle &resource_handle,
    const uint64_t tenant_id,
    const int64_t block_size)
{
  void *ptr = NULL;
  int ret = OB_SUCCESS;
  if (!resource_handle.is_valid() || OB_INVALID_ID == tenant_id || block_size <= 0 || block_size < block_size_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), "handle valid", resource_handle.is_valid(),
      K(tenant_id), K(block_size), K_(block_size));
  } else if (NULL == (ptr = resource_handle.get_memory_mgr()->alloc_cache_mb(block_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to alloc cache mem block", K(tenant_id), K(block_size));
  }
  return ptr;
}

void ObKVCacheStore::free_mb(ObTenantResourceMgrHandle &resource_handle,
    const uint64_t tenant_id, void *ptr)
{
  if (NULL != ptr) {
    if (OB_INVALID_ID == tenant_id) {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid tenant_id", K(tenant_id));
    } else if (!resource_handle.is_valid()) {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid resource_handle");
    } else {
      resource_handle.get_memory_mgr()->free_cache_mb(ptr);
    }
  }
}

int ObKVCacheStore::insert_mb_handle(ObDLink *head, ObKVMemBlockHandle *mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (NULL == head || NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(head), KP(mb_handle));
  } else {
    QClockGuard guard(get_qclock());
    dl_insert_before(head, mb_handle);
  }
  return ret;
}

int ObKVCacheStore::remove_mb_handle(ObKVMemBlockHandle *mb_handle, const bool do_retire)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(mb_handle));
  } else {
    if (do_retire) {
      // default
      {
        QClockGuard guard(get_qclock());
        dl_del(mb_handle);
      }
      retire_mb_handle(mb_handle, do_retire);
    } else {
      // sync wash has already get qclock
      dl_del(mb_handle);
    }
  }
  return ret;
}

void ObKVCacheStore::retire_mb_handle(ObKVMemBlockHandle *mb_handle, const bool do_retire)
{
  if (NULL != mb_handle) {
    HazardList retire_list;
    retire_list.push(&mb_handle->retire_link_);
    retire_mb_handles(retire_list, do_retire);
  }
}

void ObKVCacheStore::retire_mb_handles(HazardList &retire_list, const bool do_retire)
{
  if (retire_list.size() > 0) {
    HazardList reclaim_list;
    int64_t retire_limit = do_retire ? RETIRE_LIMIT : INT64_MAX;
    if (wash_itid_ == get_itid()) {  // wash thread should not sync wash
      retire_limit = WASH_THREAD_RETIRE_LIMIT;
    }
    bool need_purge = retire_list.size() > retire_limit;
    get_retire_station().retire(reclaim_list, retire_list, retire_limit);
    if (need_purge) {
      get_retire_station().retire(reclaim_list, retire_list, -1);
    }
    reuse_mb_handles(reclaim_list);
  }
}

void ObKVCacheStore::reuse_mb_handles(HazardList &reclaim_list)
{
  int ret = OB_SUCCESS;
  ObLink *p = NULL;
  // should continue even error occur
  while (NULL != (p = reclaim_list.pop())) {
    ObKVMemBlockHandle *mb_handle = CONTAINER_OF(p, ObKVMemBlockHandle, retire_link_);
    mb_handle->reset();
    if (OB_FAIL(mb_handles_pool_.push(mb_handle))) {
      COMMON_LOG(ERROR, "push mb_handle to pool failed", K(ret));
    }
  }
}

bool ObKVCacheStore::try_supply_mb(const int64_t mb_count)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  const int64_t old_num = ATOMIC_LOAD(&cur_mb_num_);
  if (old_num >= max_mb_num_) {
    bool_ret = false;
  } else {
    const int64_t new_num = (old_num + mb_count <= max_mb_num_ ? old_num + mb_count : max_mb_num_);
    if (ATOMIC_BCAS(&cur_mb_num_, old_num, new_num)) {
      for (int64_t i = old_num; OB_SUCCESS == ret && i < new_num; i++) {
        if (OB_FAIL(mb_handles_pool_.push(&(mb_handles_[i])))) {
          COMMON_LOG(ERROR, "supply mb failed", K(ret));
        }
      }
    } else {
      // other thread may have produced some mem block handles
    }
  }
  return bool_ret;
}

bool ObKVCacheStore::StoreMBHandleCmp::operator ()(
  const ObKVMemBlockHandle *a,
  const ObKVMemBlockHandle *b) const
{
  bool bret = false;
  if (NULL != a && NULL != b) {
    bret = a->score_ < b->score_;
  }
  return bret;
}

ObKVCacheStore::WashHeap::WashHeap()
  : heap_(NULL), heap_size_(0), mb_cnt_(0)
{
}

ObKVCacheStore::WashHeap::~WashHeap()
{
}

ObKVMemBlockHandle *ObKVCacheStore::WashHeap::add(ObKVMemBlockHandle *mb_handle)
{
  StoreMBHandleCmp mb_cmp;
  ObKVMemBlockHandle *remove_handle = NULL;
  if (NULL != mb_handle && NULL != heap_ && (mb_cnt_ < heap_size_ || mb_cmp(mb_handle, heap_[0]))) {
    if (mb_cnt_ < heap_size_) {
      heap_[mb_cnt_++] = mb_handle;
    } else {
      std::pop_heap(&heap_[0], &heap_[mb_cnt_], mb_cmp);
      remove_handle = heap_[mb_cnt_ - 1];
      heap_[mb_cnt_ - 1] = mb_handle;
    }
    std::push_heap(&heap_[0], &heap_[mb_cnt_], mb_cmp);
  }
  return remove_handle;
}

void ObKVCacheStore::WashHeap::reset()
{
  heap_ = NULL;
  heap_size_ = 0;
  mb_cnt_ = 0;
}

ObKVCacheStore::TenantWashInfo::TenantWashInfo()
  : cache_size_(0),
    lower_limit_(0),
    upper_limit_(0),
    mem_usage_(0),
    reserve_mem_(0),
    max_wash_size_(0),
    min_wash_size_(0),
    wash_size_(0)
{
}

int ObKVCacheStore::TenantWashInfo::add(ObKVMemBlockHandle *mb_handle)
{
  int ret = OB_SUCCESS;
  if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", K(ret), KP(mb_handle));
  } else if (mb_handle->inst_->need_hold_cache()) {
    cache_wash_heaps_[mb_handle->inst_->cache_id_].add(mb_handle);
  } else {
    wash_heap_.add(mb_handle);
  }
  return ret;
}

void ObKVCacheStore::TenantWashInfo::normalize()
{
  for (int i = 0; i < MAX_CACHE_NUM; ++i) {
    for (int j = 0; j < cache_wash_heaps_[i].mb_cnt_; ++j) {
      wash_heap_.add(cache_wash_heaps_[i].heap_[j]);
    }
  }
}

void ObKVCacheStore::TenantWashInfo::reuse()
{
  cache_size_ = 0;
  lower_limit_ = 0;
  upper_limit_= 0;
  mem_usage_ = 0;
  reserve_mem_ = 0;
  max_wash_size_ = 0;
  min_wash_size_ = 0;
  wash_size_ = 0;
  wash_heap_.reset();
  for (int64_t i = 0 ; i < MAX_CACHE_NUM ; ++i) {
    cache_wash_heaps_[i].reset();
  }
}

ObKVCacheStore::MBHandlePointerWashPool::MBHandlePointerWashPool()
  : inited_(false), 
    total_count_(0),
    free_count_(0),
    buf_(nullptr),
    allocator_(ObModIds::OB_KVSTORE_CACHE_WASH_STRUCT)
{
}

int ObKVCacheStore::MBHandlePointerWashPool::init(const int64_t count, const char *label)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice!", K(ret));
  } else if (count <= 0 || nullptr == label) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret));
  } else {
    allocator_.set_label(label);
    buf_ = static_cast<ObKVMemBlockHandle **>(allocator_.alloc(sizeof(ObKVMemBlockHandle *) * count));
    if (nullptr == buf_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      MEMSET(buf_, 0, sizeof(ObKVMemBlockHandle *) * count);
      total_count_ = count;
      free_count_ = count;
      inited_ = true;
    }
  }

  return ret;
}

int ObKVCacheStore::MBHandlePointerWashPool::alloc(const int64_t count, ObKVMemBlockHandle **&heap)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret));
  } else if (count > free_count_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    free_count_ -= count;
    heap = buf_ + free_count_;
  }

  return ret;
}

void ObKVCacheStore::MBHandlePointerWashPool::reuse()
{
  free_count_ = total_count_;
}

int ObKVCacheStore::MBHandlePointerWashPool::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(inited_)) {
    free_count_ = 0;
    total_count_ = 0;
    allocator_.free(buf_);
    buf_ = nullptr;
    inited_ = false;
  }

  return ret;
}

}//end namespace common
}//end namespace oceanbase

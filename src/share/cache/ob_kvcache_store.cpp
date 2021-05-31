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

#include "ob_kvcache_store.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
using namespace lib;
namespace common {
ObKVCacheStore::ObKVCacheStore()
    : inited_(false),
      insts_(NULL),
      cur_mb_num_(0),
      max_mb_num_(0),
      block_size_(0),
      aligned_block_size_(0),
      block_payload_size_(0),
      mb_handles_(NULL),
      mb_handles_pool_(),
      wash_out_lock_(),
      tenant_ids_(),
      inst_handles_(),
      wash_info_free_heap_(),
      tenant_wash_map_(),
      mb_ptr_free_heap_(),
      tenant_reserve_mem_ratio_(TENANT_RESERVE_MEM_RATIO),
      wash_itid_(-1),
      tenant_mgr_(NULL)
{}

ObKVCacheStore::~ObKVCacheStore()
{
  destroy();
}

int ObKVCacheStore::init(
    ObKVCacheInstMap& insts, const int64_t max_cache_size, const int64_t block_size, const ObITenantMgr& tenant_mgr)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheStore has been inited, ", K(ret));
  } else if (OB_UNLIKELY(max_cache_size <= block_size * 3) ||
             OB_UNLIKELY(block_size <= (int64_t)(sizeof(ObKVStoreMemBlock)))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid arguments, ", K(max_cache_size), K(block_size), K(ret));
  } else {
    max_mb_num_ = max_cache_size / block_size + 2 * (WASH_THREAD_RETIRE_LIMIT + RETIRE_LIMIT * OB_MAX_THREAD_NUM);
    if (NULL == (mb_handles_ = static_cast<ObKVMemBlockHandle*>(
                     buf = ob_malloc((sizeof(ObKVMemBlockHandle) + sizeof(ObKVMemBlockHandle*)) * max_mb_num_,
                         ObNewModIds::OB_KVSTORE_CACHE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory for mb_handles_, ", K_(max_mb_num), K(ret));
    } else if (OB_FAIL(mb_handles_pool_.init(max_mb_num_, (char*)(buf) + sizeof(ObKVMemBlockHandle) * max_mb_num_))) {
      COMMON_LOG(WARN, "Fail to init mb_handles_pool_, ", K(ret));
    } else {
      MEMSET(mb_handles_, 0, sizeof(ObKVMemBlockHandle) * max_mb_num_);
      block_size_ = block_size;
      aligned_block_size_ = ObTenantMemoryMgr::align(block_size_);
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
    tenant_mgr_ = &tenant_mgr;
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
  if (NULL != mb_handles_) {
    for (int64_t i = 0; i < max_mb_num_; ++i) {
      if (add_handle_ref(&mb_handles_[i])) {
        de_handle_ref(&mb_handles_[i]);
        de_handle_ref(&mb_handles_[i]);
      }
    }

    // free all mb handles cached by threads
    HazardList reclaim_list;
    get_retire_station().purge(reclaim_list);
    reuse_mb_handles(reclaim_list);

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

int ObKVCacheStore::set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(cache_id >= MAX_CACHE_NUM) || OB_UNLIKELY(old_priority <= 0) ||
             OB_UNLIKELY(new_priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(old_priority), K(new_priority), K(ret));
  } else {
    lib::ObMutexGuard guard(wash_out_lock_);
    if (OB_FAIL(insts_->set_priority(cache_id, old_priority, new_priority))) {
      COMMON_LOG(WARN, "Fail to set inst priority, ", K(ret));
    } else {
      for (int64_t i = 0; i < cur_mb_num_; i++) {
        if (add_handle_ref(&mb_handles_[i])) {
          if (NULL != mb_handles_[i].inst_) {
            if (mb_handles_[i].inst_->cache_id_ == cache_id) {
              mb_handles_[i].score_ = (mb_handles_[i].score_ / double(old_priority) * double(new_priority));
            }
          }
          de_handle_ref(&mb_handles_[i]);
        }
      }
    }
  }

  return ret;
}

bool ObKVCacheStore::add_handle_ref(ObKVMemBlockHandle* mb_handle, const uint32_t seq_num)
{
  bool bret = false;
  if (NULL != mb_handle) {
    bret = (OB_SUCCESS == mb_handle->handle_ref_.check_seq_num_and_inc_ref_cnt(seq_num));
  }
  return bret;
}

bool ObKVCacheStore::add_handle_ref(ObKVMemBlockHandle* mb_handle)
{
  bool bret = false;
  if (NULL != mb_handle) {
    bret = (OB_SUCCESS == mb_handle->handle_ref_.check_and_inc_ref_cnt());
  }
  return bret;
}

int64_t ObKVCacheStore::get_handle_ref_cnt(ObKVMemBlockHandle* mb_handle)
{
  if (NULL != mb_handle) {
    return mb_handle->handle_ref_.get_ref_cnt();
  } else {
    return 0;
  }
}

// implement functions of ObIKVStore<ObKVMemBlockHandle>
int ObKVCacheStore::alloc(
    ObKVCacheInst& inst, const enum ObKVCachePolicy policy, const int64_t block_size, ObKVMemBlockHandle*& mb_handle)
{
  return alloc_mbhandle(inst, policy, block_size, mb_handle);
}

int ObKVCacheStore::free(ObKVMemBlockHandle* mb_handle)
{
  int ret = common::OB_SUCCESS;
  if (NULL == mb_handle) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "mb_handle is null", K(ret), KP(mb_handle));
  } else {
    de_handle_ref(mb_handle);
  }
  return ret;
}

ObKVMemBlockHandle*& ObKVCacheStore::get_curr_mb(ObKVCacheInst& inst, const enum ObKVCachePolicy policy)
{
  return inst.handles_[policy];
}

bool ObKVCacheStore::mb_status_match(
    ObKVCacheInst& inst, const enum ObKVCachePolicy policy, ObKVMemBlockHandle* mb_handle)
{
  return &inst == mb_handle->inst_ && policy == mb_handle->policy_;
}

void ObKVCacheStore::refresh_score()
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t priority = 1;
  double score = 0;

  if (OB_FAIL(insts_->refresh_score())) {
    COMMON_LOG(WARN, "Fail to refresh inst score, ", K(ret));
  } else {
    for (i = 0; i < cur_mb_num_; i++) {
      if (add_handle_ref(&mb_handles_[i])) {
        if (NULL != mb_handles_[i].inst_) {
          priority = mb_handles_[i].inst_->status_.config_->priority_;
          score = mb_handles_[i].score_;
          score = score * CACHE_SCORE_DECAY_FACTOR + (double)(mb_handles_[i].recent_get_cnt_ * priority);
          mb_handles_[i].score_ = score;
          ATOMIC_STORE(&mb_handles_[i].recent_get_cnt_, 0);
        }
        de_handle_ref(&mb_handles_[i]);
      }
    }
  }
  return;
}

void ObKVCacheStore::wash()
{
  int ret = OB_SUCCESS;
  WashMap::iterator wash_iter;
  TenantWashInfo* tenant_wash_info = NULL;
  int64_t cur_time = ObTimeUtility::current_time();
  int64_t start_time = cur_time;

  if (-1 == wash_itid_) {
    wash_itid_ = get_itid();
  }
  lib::ObMutexGuard guard(wash_out_lock_);

  refresh_score();
  cur_time = ObTimeUtility::current_time();
  int64_t refresh_score_time = cur_time - start_time;
  start_time = cur_time;

  // compute the wash size of each tenant
  compute_tenant_wash_size();
  cur_time = ObTimeUtility::current_time();
  int64_t compute_wash_size_time = cur_time - start_time;
  start_time = cur_time;

  // sort mb_handles to wash
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_mb_num_; ++i) {
    if (add_handle_ref(&mb_handles_[i])) {
      enum ObKVMBHandleStatus status = ATOMIC_LOAD(&mb_handles_[i].status_);
      if (OB_SUCC(tenant_wash_map_.get(mb_handles_[i].inst_->tenant_id_, tenant_wash_info))) {
        if (FULL == status) {
          if (OB_FAIL(tenant_wash_info->add(&mb_handles_[i]))) {
            COMMON_LOG(WARN, "add failed", K(ret));
          }
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        wash_mb(&mb_handles_[i]);
      } else {
        COMMON_LOG(ERROR, "Unexpected error, ", K(ret));
      }
      de_handle_ref(&mb_handles_[i]);
      // any error should not break washing, so reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
    }
  }
  for (wash_iter = tenant_wash_map_.begin(); wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    wash_iter->second->normalize();
  }
  cur_time = ObTimeUtility::current_time();
  int64_t wash_sort_time = cur_time - start_time;
  start_time = cur_time;

  // wash memory in tenant wash heap
  for (wash_iter = tenant_wash_map_.begin(); wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    tenant_wash_info = wash_iter->second;
    if (NULL != tenant_wash_info && tenant_wash_info->wash_heap_.mb_cnt_ > 0) {
      wash_mbs(tenant_wash_info->wash_heap_);
      COMMON_LOG(INFO,
          "Wash memory, ",
          "tenant_id",
          wash_iter->first,
          "cache_size",
          tenant_wash_info->cache_size_,
          "lower_mem_limit",
          tenant_wash_info->lower_limit_,
          "upper_mem_limit",
          tenant_wash_info->upper_limit_,
          "min_wash_size",
          tenant_wash_info->min_wash_size_,
          "max_wash_size",
          tenant_wash_info->max_wash_size_,
          "mem_usage",
          lib::get_tenant_memory_hold(wash_iter->first),
          "reserve_mem",
          static_cast<int64_t>((static_cast<double>(tenant_wash_info->upper_limit_)) * tenant_reserve_mem_ratio_),
          "wash_size",
          tenant_wash_info->wash_size_);
    }
  }
  cur_time = ObTimeUtility::current_time();
  int64_t wash_time = cur_time - start_time;
  start_time = cur_time;

  COMMON_LOG(
      INFO, "Wash time detail, ", K(refresh_score_time), K(compute_wash_size_time), K(wash_sort_time), K(wash_time));

  reuse_wash_structs();
}

int ObKVCacheStore::get_avg_cache_item_size(
    const uint64_t tenant_id, const int64_t cache_id, int64_t& avg_cache_item_size)
{
  int ret = OB_SUCCESS;
  int64_t total_cache_size = 0;
  int64_t total_kv_cnt = 0;
  if (OB_INVALID_ID == tenant_id || cache_id < 0 || cache_id >= MAX_CACHE_NUM) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(cache_id));
  } else {
    for (int64_t i = 0; i < cur_mb_num_; ++i) {
      if (add_handle_ref(&mb_handles_[i])) {
        if (mb_handles_[i].inst_->tenant_id_ == tenant_id && mb_handles_[i].inst_->cache_id_ == cache_id) {
          enum ObKVMBHandleStatus status = ATOMIC_LOAD(&mb_handles_[i].status_);
          if (FULL == status) {
            total_cache_size += mb_handles_[i].mem_block_->get_size();
            total_kv_cnt += mb_handles_[i].mem_block_->get_kv_cnt();
          }
        }
        de_handle_ref(&mb_handles_[i]);
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

int ObKVCacheStore::sync_wash_mbs(const uint64_t tenant_id, const int64_t size_need_washed, const bool wash_single_mb,
    ObICacheWasher::ObCacheMemBlock*& wash_blocks)
{
  int ret = OB_SUCCESS;
  ObDLink* head = NULL;
  ObTenantMBListHandle list_handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || size_need_washed <= 0 ||
             (wash_single_mb && size_need_washed != aligned_block_size_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(tenant_id),
        K(size_need_washed),
        K(wash_single_mb),
        K_(aligned_block_size));
  } else if (OB_FAIL(insts_->get_mb_list(tenant_id, list_handle))) {
    COMMON_LOG(WARN, "get_mb_list failed", K(ret), K(tenant_id));
  } else if (NULL == (head = list_handle.get_head())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "head in list_handle is null", K(ret));
  } else {
    const int64_t check_interval = 512;
    const int64_t start = ObTimeUtility::current_time();
    int64_t size_washed = 0;
    int64_t check_idx = 0;
    HazardList retire_list;
    {
      QClockGuard guard(get_qclock());
      ObKVMemBlockHandle* handle = static_cast<ObKVMemBlockHandle*>(link_next(head));
      while (OB_SUCC(ret) && size_washed < size_need_washed && head != handle) {
        bool can_try_wash = false;
        if (handle->is_mark_delete()) {
          // ignore handle already remove
        } else if (add_handle_ref(handle)) {
          if (handle->inst_->tenant_id_ == tenant_id) {
            enum ObKVMBHandleStatus status = ATOMIC_LOAD(&handle->status_);
            if (FULL == status && 2 == get_handle_ref_cnt(handle)) {
              if (!wash_single_mb || size_need_washed == handle->mem_block_->get_align_size()) {
                can_try_wash = true;
              }
            }
          }
          de_handle_ref(handle);
        }

        if (OB_SUCC(ret) && can_try_wash) {
          int64_t mb_size = 0;
          void* buf = NULL;
          if (try_wash_mb(handle, tenant_id, buf, mb_size)) {
            if (NULL == buf) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(ERROR, "buf is null", K(ret));
            } else {
              size_washed += mb_size;
              ObICacheWasher::ObCacheMemBlock* mem_block = new (buf) ObICacheWasher::ObCacheMemBlock();
              if (NULL == wash_blocks) {
                wash_blocks = mem_block;
              } else {
                mem_block->next_ = wash_blocks;
                wash_blocks = mem_block;
              }
            }
            dl_del(handle);
            retire_list.push(&handle->retire_link_);
          }
        }
        handle = static_cast<ObKVMemBlockHandle*>(link_next(handle));

        if (check_idx > 0 && 0 == check_idx % check_interval) {
          const int64_t cost = ObTimeUtility::current_time() - start;
          if (cost > SYNC_WASH_MB_TIMEOUT_US) {
            ret = OB_SYNC_WASH_MB_TIMEOUT;
            COMMON_LOG(WARN, "sync wash mb timeout", K(cost), LITERAL_K(SYNC_WASH_MB_TIMEOUT_US));
          }
        }
        ++check_idx;
      }
    }

    if (OB_SUCC(ret) && size_washed < size_need_washed) {
      ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
      COMMON_LOG(ERROR, "can not find enough memory block to wash", K(ret), K(size_washed), K(size_need_washed));
    }

    if (OB_FAIL(ret)) {
      // free memory of memory blocks washed if any error occur
      ObICacheWasher::ObCacheMemBlock* wash_block = wash_blocks;
      ObICacheWasher::ObCacheMemBlock* next = NULL;
      while (NULL != wash_block) {
        next = wash_block->next_;
        free_mb(*list_handle.get_resource_handle(), tenant_id, reinterpret_cast<void*>(wash_block));
        wash_block = next;
      }
      wash_blocks = NULL;
    }
    EVENT_ADD(KVCACHE_SYNC_WASH_TIME, ObTimeUtility::current_time() - start);
    EVENT_INC(KVCACHE_SYNC_WASH_COUNT);

    retire_mb_handles(retire_list);
  }
  return ret;
}

int ObKVCacheStore::alloc_mbhandle(ObKVCacheInst& inst, const int64_t block_size, ObKVMemBlockHandle*& mb_handle)
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

int ObKVCacheStore::alloc_mbhandle(ObKVCacheInst& inst, ObKVMemBlockHandle*& mb_handle)
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

int ObKVCacheStore::alloc_mbhandle(const ObKVCacheInstKey& inst_key, ObKVMemBlockHandle*& mb_handle)
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

int ObKVCacheStore::mark_washable(ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mb_handle));
  } else if (!add_handle_ref(mb_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add_handle_ref failed", K(ret));
  } else {
    ATOMIC_STORE((uint32_t*)(&mb_handle->status_), FULL);
    de_handle_ref(mb_handle);
  }
  return ret;
}

int ObKVCacheStore::free_mbhandle(ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(mb_handle));
  } else {
    void* buf = NULL;
    int64_t mb_size = 0;
    const uint64_t tenant_id = mb_handle->inst_->tenant_id_;
    if (OB_FAIL(do_wash_mb(mb_handle, buf, mb_size))) {
      COMMON_LOG(ERROR, "do_wash_mb failed", K(ret));
    } else {
      free_mb(*mb_handle->inst_->mb_list_handle_.get_resource_handle(), tenant_id, buf);
      if (OB_FAIL(remove_mb_handle(mb_handle))) {
        COMMON_LOG(WARN, "remove_mb failed", K(ret));
      }
    }
  }
  return ret;
}

ObKVMemBlockHandle* ObKVCacheStore::alloc_mbhandle(
    ObKVCacheInst& inst, const enum ObKVCachePolicy policy, const int64_t block_size)
{
  ObKVMemBlockHandle* mb_handle = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_mbhandle(inst, policy, block_size, mb_handle))) {
    COMMON_LOG(WARN, "alloc_mbhandle failed", K(ret), K(policy), K(block_size));
  }
  return mb_handle;
}

int ObKVCacheStore::alloc_mbhandle(
    ObKVCacheInst& inst, const enum ObKVCachePolicy policy, const int64_t block_size, ObKVMemBlockHandle*& mb_handle)
{
  int ret = OB_SUCCESS;
  mb_handle = NULL;
  ObKVStoreMemBlock* mem_block = NULL;
  char* buf = NULL;
  const uint64_t tenant_id = inst.tenant_id_;

  if (!inst.mb_list_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "mb_list_handle is invalid", K(ret));
  } else if (NULL ==
             (buf = static_cast<char*>(alloc_mb(*inst.mb_list_handle_.get_resource_handle(), tenant_id, block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "Fail to allocate memory, ", K(block_size), K(ret));
  }

  if (NULL != buf) {
    mem_block = new (buf) ObKVStoreMemBlock(buf + sizeof(ObKVStoreMemBlock), block_size - sizeof(ObKVStoreMemBlock));
    while (OB_FAIL(mb_handles_pool_.pop(mb_handle))) {
      if (OB_UNLIKELY(!try_supply_mb(SUPPLY_MB_NUM_ONCE))) {
        break;
      }
    }
    if (OB_FAIL(ret)) {
      mem_block->~ObKVStoreMemBlock();
      free_mb(*inst.mb_list_handle_.get_resource_handle(), tenant_id, mem_block);
      COMMON_LOG(WARN, "Fail to pop mb_handle, ", K(ret));
    } else if (!ATOMIC_BCAS((uint32_t*)(&mb_handle->status_), FREE, USING)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "Fail to set mb_handle status from FREE to USING, ", K(ret));
    } else {
      (void)ATOMIC_AAF(&inst.status_.store_size_, block_size);
      if (LRU == policy) {
        (void)ATOMIC_AAF(&inst.status_.lru_mb_cnt_, 1);
      } else {
        (void)ATOMIC_AAF(&inst.status_.lfu_mb_cnt_, 1);
      }
      mb_handle->inst_ = &inst;
      mb_handle->policy_ = policy;
      mb_handle->mem_block_ = mem_block;
      mb_handle->handle_ref_.inc_ref_cnt();
    }
  }

  if (OB_SUCC(ret)) {
    ObDLink* head = NULL;
    if (NULL == (head = inst.get_mb_list())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "head is null", K(ret), "tenant_id", inst.tenant_id_);
    } else if (OB_FAIL(insert_mb_handle(head, mb_handle))) {
      COMMON_LOG(WARN, "insert_mb_handle failed", K(ret));
    }
  }

  return ret;
}

void ObKVCacheStore::de_handle_ref(ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  uint32_t ref_cnt = 0;
  if (OB_ISNULL(mb_handle)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, the mb_handle is NULL, ", K(ret));
  } else if (OB_FAIL(mb_handle->handle_ref_.dec_ref_cnt_and_inc_seq_num(ref_cnt))) {
    COMMON_LOG(ERROR, "Fail to dec ref cnt, ", K(ret));
  } else if (0 == ref_cnt) {
    if (OB_FAIL(free_mbhandle(mb_handle))) {
      COMMON_LOG(WARN, "free_mbhandle failed", K(ret));
    }
  }
}

void ObKVCacheStore::compute_tenant_wash_size()
{
  int ret = OB_SUCCESS;
  int64_t tenant_min_wash_size = 0;
  int64_t tenant_max_wash_size = 0;
  uint64_t tenant_id = 0;
  int64_t lower_limit = 0;
  int64_t upper_limit = 0;
  int64_t mem_usage = 0;
  int64_t reserve_mem = 0;
  ObMemAttr mem_attr;
  TenantWashInfo* tenant_wash_info = NULL;
  WashMap::iterator wash_iter;
  int64_t global_cache_size = 0;
  int64_t sys_total_wash_size = lib::get_memory_used() - lib::get_memory_limit() + lib::ob_get_reserved_urgent_memory();

  if (OB_FAIL(tenant_mgr_->get_all_tenant_id(tenant_ids_))) {
    COMMON_LOG(WARN, "Fail to get all tenant ids, ", K(ret));
  } else if (OB_FAIL(insts_->get_all_cache_info(inst_handles_))) {
    COMMON_LOG(WARN, "Fail to get all cache infos, ", K(ret));
  }

  // get tenant memory lower limit and upper limit
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); ++i) {
    if (OB_FAIL(tenant_ids_.at(i, tenant_id))) {
      COMMON_LOG(WARN, "Fail to get ith tenant id, ", K(i), K(ret));
    } else if (OB_FAIL(tenant_mgr_->get_tenant_mem_limit(tenant_id, lower_limit, upper_limit))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        lower_limit = 0;
        upper_limit = 0;
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "Fail to get tenant memory limit, ", K(tenant_id), K(ret));
      }
    } else if (OB_FAIL(wash_info_free_heap_.sbrk(tenant_wash_info))) {
      COMMON_LOG(WARN, "wash_info_free_heap_ sbrk failed", K(ret));
    } else if (OB_FAIL(tenant_wash_map_.set(tenant_id, tenant_wash_info))) {
      COMMON_LOG(WARN, "Fail to set tenant_wash_info to tenant_wash_map, ", K(ret));
    } else {
      tenant_wash_info->lower_limit_ = lower_limit;
      tenant_wash_info->upper_limit_ = upper_limit;
    }
  }

  ObKVCacheInst* inst = NULL;
  // get tenant cache size
  for (int64_t i = 0; OB_SUCC(ret) && i < inst_handles_.count(); ++i) {
    inst = inst_handles_.at(i).get_inst();
    if (OB_ISNULL(inst)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ObKVCacheInst is NULL", K(ret));
    } else if (OB_SUCC(tenant_wash_map_.get(inst->tenant_id_, tenant_wash_info))) {
      tenant_wash_info->cache_size_ += (inst->status_.store_size_ + inst->status_.map_size_);
      global_cache_size += (inst->status_.store_size_ + inst->status_.map_size_);
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }

  // identify tenant_min_wash_size and tenant_max_wash_size
  for (wash_iter = tenant_wash_map_.begin(); OB_SUCC(ret) && wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    tenant_id = wash_iter->first;
    tenant_wash_info = wash_iter->second;
    upper_limit = tenant_wash_info->upper_limit_;
    lower_limit = tenant_wash_info->lower_limit_;
    mem_usage = lib::get_tenant_memory_hold(tenant_id);
    if (upper_limit <= 1024L * 1024L * 1024L) {
      reserve_mem = (OB_SYS_TENANT_ID == tenant_id) ? upper_limit / 3 : upper_limit / 2;
    } else {
      reserve_mem = static_cast<int64_t>(1.368 * log(static_cast<double>(upper_limit) / (1024.0 * 1024.0 * 1024.0)) *
                                         1024L * 1024L * 1024L) +
                    600L * 1024L * 1024L;
    }

    // identify min_wash_size
    tenant_wash_info->min_wash_size_ = std::max(0L, mem_usage - upper_limit + reserve_mem);
    if (tenant_wash_info->min_wash_size_ > tenant_wash_info->cache_size_) {
      tenant_wash_info->min_wash_size_ = tenant_wash_info->cache_size_;
    }
    if (tenant_wash_info->min_wash_size_ > 0) {
      tenant_min_wash_size += tenant_wash_info->min_wash_size_;
    }
    // identify max_wash_size
    tenant_wash_info->max_wash_size_ = std::max(0L, mem_usage - lower_limit + reserve_mem);
    if (tenant_wash_info->max_wash_size_ > tenant_wash_info->cache_size_) {
      tenant_wash_info->max_wash_size_ = tenant_wash_info->cache_size_;
    }
    if (tenant_wash_info->max_wash_size_ > 0) {
      tenant_max_wash_size += tenant_wash_info->max_wash_size_;
    }
  }

  // add sys wash size to each tenant
  int64_t heap_size = 0;
  for (wash_iter = tenant_wash_map_.begin(); OB_SUCC(ret) && wash_iter != tenant_wash_map_.end(); ++wash_iter) {
    tenant_id = wash_iter->first;
    tenant_wash_info = wash_iter->second;
    // determine the final wash size
    if (tenant_min_wash_size >= sys_total_wash_size) {
      if (tenant_wash_info->min_wash_size_ > 0) {
        tenant_wash_info->wash_size_ = tenant_wash_info->min_wash_size_;
      }
    } else {
      if (tenant_max_wash_size >= sys_total_wash_size) {
        tenant_wash_info->wash_size_ =
            tenant_wash_info->min_wash_size_ +
            static_cast<int64_t>(
                static_cast<double>((sys_total_wash_size - tenant_min_wash_size)) *
                (static_cast<double>(tenant_wash_info->max_wash_size_ - tenant_wash_info->min_wash_size_) /
                    static_cast<double>(tenant_max_wash_size - tenant_min_wash_size)));
      } else {
        if (tenant_wash_info->max_wash_size_ > 0) {
          tenant_wash_info->wash_size_ = tenant_wash_info->max_wash_size_;
        } else {
          tenant_wash_info->wash_size_ = 0;
        }
        if (global_cache_size > 0) {
          tenant_wash_info->wash_size_ += static_cast<int64_t>(
              static_cast<double>(sys_total_wash_size - tenant_max_wash_size) *
              (static_cast<double>(tenant_wash_info->cache_size_) / static_cast<double>(global_cache_size)));
        }
      }
    }

    // allocate memory for wash heap
    heap_size = tenant_wash_info->wash_size_ / block_size_;
    if (heap_size > 0) {
      if (OB_FAIL(init_wash_heap(tenant_wash_info->wash_heap_, heap_size))) {
        COMMON_LOG(WARN, "init_wash_heap failed", K(ret), K(heap_size));
      }
    }
  }

  // allocate memory for cache wash heaps whose hold size is set
  for (int64_t i = 0; OB_SUCC(ret) && i < inst_handles_.count(); ++i) {
    inst = inst_handles_.at(i).get_inst();
    if (OB_ISNULL(inst)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ObKVCacheInst is NULL", K(ret));
    } else if (OB_SUCC(tenant_wash_map_.get(inst->tenant_id_, tenant_wash_info))) {
      if (inst->status_.hold_size_ > 0) {
        const int64_t heap_size =
            std::min(tenant_wash_info->wash_size_, inst->status_.store_size_ - inst->status_.hold_size_) / block_size_;
        if (OB_FAIL(init_wash_heap(tenant_wash_info->cache_wash_heaps_[inst->cache_id_], heap_size))) {
          COMMON_LOG(WARN, "init_wash_heap failed", K(ret), K(heap_size));
        }
      }
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  COMMON_LOG(INFO,
      "Wash compute wash size",
      K(sys_total_wash_size),
      K(global_cache_size),
      K(tenant_max_wash_size),
      K(tenant_min_wash_size),
      K(tenant_ids_));
}

void ObKVCacheStore::wash_mb(ObKVMemBlockHandle* mb_handle)
{
  if (NULL != mb_handle) {
    if (add_handle_ref(mb_handle)) {
      if (ATOMIC_BCAS((uint32_t*)(&mb_handle->status_), FULL, FREE)) {
        de_handle_ref(mb_handle);
      }
      de_handle_ref(mb_handle);
    }
  }
}

void ObKVCacheStore::wash_mbs(WashHeap& heap)
{
  for (int64_t i = 0; i < heap.mb_cnt_; ++i) {
    if (NULL != heap.heap_) {
      wash_mb(heap.heap_[i]);
    }
  }
}

bool ObKVCacheStore::try_wash_mb(ObKVMemBlockHandle* mb_handle, const uint64_t tenant_id, void*& buf, int64_t& mb_size)
{
  bool block_washed = false;
  if (NULL == mb_handle || OB_INVALID_ID == tenant_id) {
    COMMON_LOG(ERROR, "invalid arguments", KP(mb_handle), K(tenant_id));
  } else {
    if (ATOMIC_BCAS((uint32_t*)(&mb_handle->status_), FULL, FREE)) {
      int ret = OB_SUCCESS;
      if (mb_handle->inst_->tenant_id_ == tenant_id && mb_handle->handle_ref_.try_inc_seq_num()) {
        if (OB_FAIL(do_wash_mb(mb_handle, buf, mb_size))) {
          COMMON_LOG(ERROR, "do_wash_mb failed", K(ret));
        } else {
          block_washed = true;
        }
      } else {
        if (!ATOMIC_BCAS((uint32_t*)(&mb_handle->status_), FREE, FULL)) {
          COMMON_LOG(ERROR, "change mb_handle status back to FULL failed");
        }
      }
    }
  }
  return block_washed;
}

int ObKVCacheStore::do_wash_mb(ObKVMemBlockHandle* mb_handle, void*& buf, int64_t& mb_size)
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
      (void)ATOMIC_SAF(&mb_handle->inst_->status_.store_size_,
          mb_handle->mem_block_->get_payload_size() + sizeof(ObKVStoreMemBlock));
      if (mb_handle->policy_ == LRU) {
        (void)ATOMIC_SAF(&mb_handle->inst_->status_.lru_mb_cnt_, 1);
      } else {
        (void)ATOMIC_SAF(&mb_handle->inst_->status_.lfu_mb_cnt_, 1);
      }
    }
    buf = mb_handle->mem_block_;
    mb_size = mb_handle->mem_block_->get_align_size();
    mb_handle->mem_block_->~ObKVStoreMemBlock();
    mb_handle->mem_block_ = NULL;
    /*
    mb_handle->reset();
    if (OB_FAIL(mb_handles_pool_.push(mb_handle))) {
      //should not happen
      COMMON_LOG(ERROR, "Fail to push mb_handle to mb_handles_pool_, ", K(ret));
    }
    */
  }
  return ret;
}

int ObKVCacheStore::init_wash_heap(WashHeap& heap, const int64_t heap_size)
{
  int ret = OB_SUCCESS;
  if (heap_size > 0) {
    heap.mb_cnt_ = 0;
    heap.heap_size_ = heap_size;
    if (OB_FAIL(mb_ptr_free_heap_.sbrk(heap_size, heap.heap_))) {
      COMMON_LOG(WARN, "mb_ptr_free_heap_ sbrk failed", K(ret), K(heap_size));
    }
  } else {
    heap.mb_cnt_ = 0;
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
  const char* label = ObModIds::OB_KVSTORE_CACHE_WASH_STRUCT;
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
  } else if (OB_FAIL(mb_ptr_free_heap_.init(2 * max_mb_num_, label))) {
    COMMON_LOG(WARN, "mb_ptr_free_heap_ init failed", K(ret), K_(max_mb_num));
  }
  return ret;
}

void ObKVCacheStore::reuse_wash_structs()
{
  tenant_ids_.reuse();
  inst_handles_.reuse();
  wash_info_free_heap_.reuse();
  tenant_wash_map_.reuse();
  mb_ptr_free_heap_.reuse();
}

void ObKVCacheStore::destroy_wash_structs()
{
  tenant_ids_.destroy();
  inst_handles_.destroy();
  wash_info_free_heap_.destroy();
  tenant_wash_map_.destroy();
  mb_ptr_free_heap_.destroy();
}

void* ObKVCacheStore::alloc_mb(
    ObTenantResourceMgrHandle& resource_handle, const uint64_t tenant_id, const int64_t block_size)
{
  void* ptr = NULL;
  int ret = OB_SUCCESS;
  if (!resource_handle.is_valid() || OB_INVALID_ID == tenant_id || block_size <= 0 || block_size < block_size_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "invalid arguments",
        K(ret),
        "handle valid",
        resource_handle.is_valid(),
        K(tenant_id),
        K(block_size),
        K_(block_size));
  } else if (NULL == (ptr = resource_handle.get_memory_mgr()->alloc_cache_mb(block_size))) {
    if (block_size == block_size_) {
      ObICacheWasher::ObCacheMemBlock* washed_blocks = NULL;
      const bool wash_single_mb = true;
      const int64_t wash_size = aligned_block_size_;
      if (OB_FAIL(sync_wash_mbs(tenant_id, wash_size, wash_single_mb, washed_blocks))) {
        COMMON_LOG(WARN, "sync_wash_mbs failed", K(ret), K(tenant_id), K(wash_size), K(wash_single_mb));
      } else {
        ptr = reinterpret_cast<void*>(washed_blocks);
      }
    } else {
      const int64_t max_retry_count = 3;
      int64_t retry_count = 0;
      const bool wash_single_mb = false;
      while (OB_SUCC(ret) && NULL == ptr && retry_count < max_retry_count) {
        ObICacheWasher::ObCacheMemBlock* washed_blocks = NULL;
        int64_t wash_size = ObTenantMemoryMgr::align(block_size);
        if (wash_size > aligned_block_size_) {
          wash_size += 2 * aligned_block_size_;
        }
        if (OB_FAIL(sync_wash_mbs(tenant_id, wash_size, wash_single_mb, washed_blocks))) {
          COMMON_LOG(WARN, "sync_wash_mbs failed", K(ret), K(tenant_id), K(wash_size), K(wash_single_mb));
        } else {
          ObICacheWasher::ObCacheMemBlock* wash_block = washed_blocks;
          ObICacheWasher::ObCacheMemBlock* next = NULL;
          while (NULL != wash_block) {
            next = wash_block->next_;
            free_mb(resource_handle, tenant_id, reinterpret_cast<void*>(wash_block));
            wash_block = next;
          }

          // retry again
          if (NULL == (ptr = resource_handle.get_memory_mgr()->alloc_cache_mb(block_size))) {
            ++retry_count;
            if (retry_count < max_retry_count) {
              COMMON_LOG(WARN,
                  "after wash from cache, alloc_cache_mb failed, need retry",
                  K(block_size),
                  K(retry_count),
                  K(max_retry_count));
            } else {
              COMMON_LOG(WARN,
                  "after wash from cache, alloc_cache_mb failed",
                  K(block_size),
                  K(retry_count),
                  K(max_retry_count));
            }
          }
        }
      }
    }
  }
  return ptr;
}

void ObKVCacheStore::free_mb(ObTenantResourceMgrHandle& resource_handle, const uint64_t tenant_id, void* ptr)
{
  if (NULL != ptr) {
    if (OB_INVALID_ID == tenant_id) {
      COMMON_LOG(ERROR, "invalid tenant_id", K(tenant_id));
    } else if (!resource_handle.is_valid()) {
      COMMON_LOG(ERROR, "invalid resource_handle");
    } else {
      resource_handle.get_memory_mgr()->free_cache_mb(ptr);
    }
  }
}

int ObKVCacheStore::insert_mb_handle(ObDLink* head, ObKVMemBlockHandle* mb_handle)
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

int ObKVCacheStore::remove_mb_handle(ObKVMemBlockHandle* mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(mb_handle));
  } else {
    {
      QClockGuard guard(get_qclock());
      dl_del(mb_handle);
    }
    retire_mb_handle(mb_handle);
  }
  return ret;
}

void ObKVCacheStore::retire_mb_handle(ObKVMemBlockHandle* mb_handle)
{
  if (NULL != mb_handle) {
    HazardList retire_list;
    retire_list.push(&mb_handle->retire_link_);
    retire_mb_handles(retire_list);
  }
}

void ObKVCacheStore::retire_mb_handles(HazardList& retire_list)
{
  if (retire_list.size() > 0) {
    HazardList reclaim_list;
    int64_t retire_limit = RETIRE_LIMIT;
    if (wash_itid_ == get_itid()) {
      retire_limit = WASH_THREAD_RETIRE_LIMIT;
    }
    get_retire_station().retire(reclaim_list, retire_list, retire_limit);
    reuse_mb_handles(reclaim_list);
  }
}

void ObKVCacheStore::reuse_mb_handles(HazardList& reclaim_list)
{
  int ret = OB_SUCCESS;
  ObLink* p = NULL;
  // should continue even error occur
  while (NULL != (p = reclaim_list.pop())) {
    ObKVMemBlockHandle* mb_handle = CONTAINER_OF(p, ObKVMemBlockHandle, retire_link_);
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

bool ObKVCacheStore::StoreMBHandleCmp::operator()(const ObKVMemBlockHandle* a, const ObKVMemBlockHandle* b) const
{
  bool bret = false;
  if (NULL != a && NULL != b) {
    bret = a->score_ < b->score_;
  }
  return bret;
}

ObKVCacheStore::WashHeap::WashHeap() : heap_(NULL), heap_size_(0), mb_cnt_(0)
{}

ObKVCacheStore::WashHeap::~WashHeap()
{}

ObKVMemBlockHandle* ObKVCacheStore::WashHeap::add(ObKVMemBlockHandle* mb_handle)
{
  StoreMBHandleCmp mb_cmp;
  ObKVMemBlockHandle* remove_handle = NULL;
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
    : cache_size_(0), lower_limit_(0), upper_limit_(0), max_wash_size_(0), min_wash_size_(0), wash_size_(0)
{}

int ObKVCacheStore::TenantWashInfo::add(ObKVMemBlockHandle* mb_handle)
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

}  // end namespace common
}  // end namespace oceanbase

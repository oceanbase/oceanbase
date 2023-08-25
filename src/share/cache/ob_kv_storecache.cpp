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

#include "share/cache/ob_kv_storecache.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_task_define.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/alloc/alloc_func.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_debug_sync.h"             // DEBUG_SYNC
#include "share/ob_debug_sync_point.h"
#include "share/ob_thread_mgr.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace lib;
namespace common
{
ObKVCacheHandle::ObKVCacheHandle()
  : mb_handle_(NULL)
{
}

ObKVCacheHandle::~ObKVCacheHandle()
{
  reset();
}

ObKVCacheHandle::ObKVCacheHandle(const ObKVCacheHandle &other)
  : mb_handle_(NULL)
{
  *this = other;
}

ObKVCacheHandle &ObKVCacheHandle::operator =(const ObKVCacheHandle &other)
{
  if (&other != this) {
    int ret = OB_SUCCESS;
    if (NULL != mb_handle_) {
#ifdef ENABLE_DEBUG_LOG
      ObKVCacheHandleRefChecker::get_instance().handle_ref_de(*this);
#endif
      ObKVGlobalCache::get_instance().revert(mb_handle_);
    }
    mb_handle_ = other.mb_handle_;
    if (NULL != mb_handle_) {
      if (OB_FAIL(mb_handle_->handle_ref_.check_and_inc_ref_cnt())) {
        //should not happen
        COMMON_LOG(ERROR, "Fail to add handle ref, ", K(ret));
      }
#ifdef ENABLE_DEBUG_LOG
      ObKVCacheHandleRefChecker::get_instance().handle_ref_inc(*this);
#endif
    }
  }
  return *this;
}

void ObKVCacheHandle::reset()
{
  if (NULL != mb_handle_) {
#ifdef ENABLE_DEBUG_LOG
    ObKVCacheHandleRefChecker::get_instance().handle_ref_de(*this);
#endif
    ObKVGlobalCache::get_instance().revert(mb_handle_);
    mb_handle_ = NULL;
  }
}


/*
 * ----------------------------------------ObKVCacheMapIterator---------------------------------------------------------
 */
ObKVCacheIterator::ObKVCacheIterator()
    : cache_id_(-1), map_(NULL), pos_(0), allocator_(ObModIds::OB_KVSTORE_CACHE_ITERATOR, OB_MALLOC_NORMAL_BLOCK_SIZE),
      handle_list_(allocator_), is_inited_(false)
{
}

ObKVCacheIterator::~ObKVCacheIterator()
{
  handle_list_.reset();
}

int ObKVCacheIterator::init(const int64_t cache_id, ObKVCacheMap * const map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheIterator has been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(NULL == map)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), KP(map), K(ret));
  } else {
    map_ = map;
    cache_id_ = cache_id;
    is_inited_ = true;
  }
  return ret;
}

void ObKVCacheIterator::reset()
{
  cache_id_ = -1;
  handle_list_.reset();
  map_ = NULL;
  pos_ = 0;
  is_inited_ = false;
}


/*
 * -------------------------------------------------------ObKVGlobalCache---------------------------------------------------------------
 */
const double ObKVGlobalCache::MAX_RESERVED_MEMORY_RATIO = 0.3;
//TODO bucket num level map should be system parameter
const int64_t ObKVGlobalCache::bucket_num_array_[MAX_BUCKET_NUM_LEVEL] =
    {
      786433l,      // more than 2G, 6M kvcache meta
      1572869l,     // more than 4G, 12M kvcache meta
      3145739l,     // more than 8G, 25M kvcache meta
      6291469l,     // more than 16G, 50M kvcache meta
      12582917l,    // more than 32G, 100M kvcache meta
      25165843l,    // more than 64G, 200M kvcache meta
      50331653l,   // more than 128G, 500M kvcache meta
      100663319l,   // more than 256G, 1G kvcache meta
      201326611l,   // more than 512G, 2G kvcache meta
      402653189ll   // more than 1024G, 4G kvcache meta
    };

ObKVGlobalCache::ObKVGlobalCache()
    : inited_(false),
      mem_limit_getter_(nullptr),
      cache_num_(0),
      mutex_(common::ObLatchIds::GLOBAL_KV_CACHE_CONFIG_LOCK),
      map_clean_pos_(0),
      map_once_clean_num_(0),
      map_replace_pos_(0),
      map_once_replace_num_(0),
      map_replace_skip_count_(0),
      stopped_(true),
      cache_wash_interval_(0)
{
}

ObKVGlobalCache::~ObKVGlobalCache()
{
  destroy();
}

ObKVGlobalCache &ObKVGlobalCache::get_instance()
{
  static ObKVGlobalCache instance_;
  return instance_;
}

int64_t ObKVGlobalCache::get_suitable_bucket_num()
{
  int64_t server_memory_factor = upper_align(GMEMCONF.get_server_memory_limit(), BASE_SERVER_MEMORY_FACTOR) / BASE_SERVER_MEMORY_FACTOR;
  int64_t reserved_memory = GMEMCONF.get_reserved_server_memory() * MAX_RESERVED_MEMORY_RATIO;
  int64_t bucket_num = DEFAULT_BUCKET_NUM;
  for (int64_t bucket_level = MAX_BUCKET_NUM_LEVEL -1; bucket_level > 0; bucket_level--) {
    if ((1 << bucket_level) > server_memory_factor) {
      // pass
    } else {
      if (bucket_num_array_[bucket_level] * static_cast<int64_t>(sizeof(void *)) <= reserved_memory) {
        bucket_num = bucket_num_array_[bucket_level];
        break;
      }
    }
  }

  share::ObTaskController::get().allow_next_syslog();
  COMMON_LOG(INFO, "The ObKVGlobalCache set suitable kvcache buckets", K(bucket_num), K(server_memory_factor), K(reserved_memory));

  return bucket_num;
}

int ObKVGlobalCache::init(
    ObITenantMemLimitGetter *mem_limit_getter,
    const int64_t bucket_num,
    const int64_t max_cache_size,
    const int64_t block_size,
    const int64_t cache_wash_interval)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVGlobalCache has been inited, ", K(ret));
  } else if (OB_ISNULL(mem_limit_getter) ||
             bucket_num <= 0 ||
             max_cache_size <= 0 ||
             block_size <= 0 ||
             cache_wash_interval < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(ret), K(mem_limit_getter),
               K(bucket_num), K(max_cache_size), K(block_size), K(cache_wash_interval));
  } else if (OB_FAIL(store_.init(insts_,
                                 max_cache_size,
                                 block_size,
                                 *mem_limit_getter))) {
    COMMON_LOG(WARN, "Fail to init store, ", K(ret));
  } else if (OB_FAIL(map_.init(hash::cal_next_prime(bucket_num), &store_))) {
    COMMON_LOG(WARN, "Fail to init map, ", K(ret), K(bucket_num));
  } else if (OB_FAIL(insts_.init(MAX_CACHE_NUM * MAX_TENANT_NUM_PER_SERVER,
                                 configs_,
                                 *mem_limit_getter))) {
    COMMON_LOG(WARN, "Fail to init insts, ", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::KVCacheWash))) {
    COMMON_LOG(WARN, "Fail to init wash timer, ", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::KVCacheRep))) {
    COMMON_LOG(WARN, "Fail to init replace timer", K(ret));
  } else if (FALSE_IT(cache_wash_interval_ = cache_wash_interval)) {
  } else if (OB_FAIL(reload_wash_interval())) {
    COMMON_LOG(WARN, "failed to reload wash interval", K(ret));
  } else {
    cache_num_ = 0;
    stopped_ = false;
    mem_limit_getter_ = mem_limit_getter;
    map_once_clean_num_ = min(MAX_MAP_ONCE_CLEAN_NUM, bucket_num / MAP_ONCE_CLEAN_RATIO);
    map_once_replace_num_ = min(MAX_MAP_ONCE_REPLACE_NUM, bucket_num / MAP_ONCE_REPLACE_RATIO);
    inited_ = true;
#ifdef ENABLE_DEBUG_LOG
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(set_checker_cache_name(GCONF.leak_mod_to_check.str()))) {
      COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Fail to set check cache name", K(tmp_ret));
    }
#endif
  }

  if (OB_UNLIKELY(!inited_)) {
    destroy();
    COMMON_LOG(ERROR, "Fail to create ObKVGlobalCache, ", K(ret));
  } else {
    COMMON_LOG(INFO, "ObKVGlobalCache has been inited!", K(bucket_num), K(max_cache_size), K(block_size));
  }

  return ret;
}

void ObKVGlobalCache::stop()
{
  if (inited_) {
    stopped_ = true;
    TG_STOP(lib::TGDefIDs::KVCacheWash);
    TG_STOP(lib::TGDefIDs::KVCacheRep);
  }
}

void ObKVGlobalCache::wait()
{
  if (inited_) {
    TG_WAIT(lib::TGDefIDs::KVCacheWash);
    TG_WAIT(lib::TGDefIDs::KVCacheRep);
  }
}

void ObKVGlobalCache::destroy()
{
  if (inited_) {
    COMMON_LOG(INFO, "Begin destroy the ObKVGlobalCache!");
    // should destroy store_ before timer threads exit, before some mb_handles may
    // cache in wash thread.
    stop();
    wait();
    ws_mgr_.destroy();
    map_.destroy();
    store_.destroy();
    insts_.destroy();
    for (int64_t i = 0; i < MAX_CACHE_NUM; ++i) {
      configs_[i].reset();
    }
    cache_num_ = 0;
    mem_limit_getter_ = nullptr;

    inited_ = false;
    COMMON_LOG(INFO, "The ObKVGlobalCache has been destroyed!");
  }
}

int ObKVGlobalCache::put(
  const int64_t cache_id,
  const ObIKVCacheKey &key,
  const ObIKVCacheValue &value,
  const ObIKVCacheValue *&pvalue,
  ObKVMemBlockHandle *&mb_handle,
  bool overwrite)
{
  return put(store_, cache_id, key, value, pvalue, mb_handle, overwrite);
}

int ObKVGlobalCache::put(
  ObWorkingSet *working_set,
  const ObIKVCacheKey &key,
  const ObIKVCacheValue &value,
  const ObIKVCacheValue *&pvalue,
  ObKVMemBlockHandle *&mb_handle,
  bool overwrite)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == working_set) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(working_set));
  } else {
    const int64_t cache_id = working_set->get_cache_id();
    if (OB_FAIL(put(*working_set, cache_id, key, value, pvalue, mb_handle, overwrite))) {
      LOG_WARN("put failed", K(ret), K(cache_id));
    }
  }
  return ret;
}

template<typename MBWrapper>
int ObKVGlobalCache::put(
    ObIKVCacheStore<MBWrapper> &store,
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    const ObIKVCacheValue *&pvalue,
    ObKVMemBlockHandle *&mb_handle,
    bool overwrite)
{
  int ret = OB_SUCCESS;
  ObKVCacheInstKey inst_key(cache_id, key.get_tenant_id());
  ObKVCacheInstHandle inst_handle;
  ObKVCachePair *kvpair = NULL;
  pvalue = NULL;
  mb_handle = NULL;
  MBWrapper *mb_wrapper = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!inst_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The tenant_id is too large, ", K(inst_key), K(ret));
  } else if (OB_FAIL(insts_.get_cache_inst(inst_key, inst_handle))) {
    COMMON_LOG(WARN, "Fail to get cache inst, ", K(ret));
  } else if (NULL == inst_handle.get_inst()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The inst is NULL, ", K(ret));
  } else if (!overwrite && (OB_SUCC(map_.get(cache_id, key, pvalue, mb_handle)))) {
    ret = OB_ENTRY_EXIST;
  } else if (OB_FAIL(store.store(*inst_handle.get_inst(), key, value, kvpair, mb_wrapper))) {
    COMMON_LOG(WARN, "Fail to store kvpair to store, ", K(ret));
  } else {
    mb_handle = mb_wrapper->get_mb_handle();
    pvalue = kvpair->value_;
    if (OB_FAIL(map_.put(*inst_handle.get_inst(), key, kvpair, mb_handle, overwrite))) {
      if (OB_ENTRY_EXIST != ret) {
        COMMON_LOG(WARN, "Fail to put kvpair to map, ", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ENTRY_EXIST != ret) {
      revert(mb_handle);
      mb_handle = NULL;
      pvalue = NULL;
    }
  }
  return ret;

}

int ObKVGlobalCache::alloc(
    const int64_t cache_id,
    const uint64_t tenant_id,
    const int64_t key_size,
    const int64_t value_size,
    ObKVCachePair *&kvpair,
    ObKVMemBlockHandle *&mb_handle,
    ObKVCacheInstHandle &inst_handle)
{
  return alloc(store_, cache_id, tenant_id, key_size, value_size, kvpair, mb_handle, inst_handle);
}

int ObKVGlobalCache::alloc(
    ObWorkingSet *working_set,
    const uint64_t tenant_id,
    const int64_t key_size,
    const int64_t value_size,
    ObKVCachePair *&kvpair,
    ObKVMemBlockHandle *&mb_handle,
    ObKVCacheInstHandle &inst_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(working_set)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguemnt", K(ret), KP(working_set));
  } else if (OB_FAIL(alloc(*working_set, working_set->get_cache_id(), tenant_id,
          key_size, value_size, kvpair, mb_handle, inst_handle))) {
    COMMON_LOG(WARN, "failed to alloc kvpair", K(ret));
  }
  return ret;
}

template <typename MBWrapper>
int ObKVGlobalCache::alloc(
    ObIKVCacheStore<MBWrapper> &store,
    const int64_t cache_id,
    const uint64_t tenant_id,
    const int64_t key_size,
    const int64_t value_size,
    ObKVCachePair *&kvpair,
    ObKVMemBlockHandle *&mb_handle,
    ObKVCacheInstHandle &inst_handle)
{
  int ret = OB_SUCCESS;
  ObKVCacheInstKey inst_key(cache_id, tenant_id);
  MBWrapper *mb_wrapper = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (nullptr != mb_handle) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Cannot overwrite valid mb_handle", K(ret), KP(mb_handle));
  } else if (OB_FAIL(insts_.get_cache_inst(inst_key, inst_handle))) {
    COMMON_LOG(WARN, "Fail to get cache inst, ", K(ret));
  } else if (OB_ISNULL(inst_handle.get_inst())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The inst is NULL, ", K(ret));
  } else if (OB_FAIL(store.alloc_kvpair(*inst_handle.get_inst(),
          key_size, value_size, kvpair, mb_wrapper))) {
    COMMON_LOG(WARN, "Fail to store kvpair, ", K(ret));
  } else {
    mb_handle = mb_wrapper->get_mb_handle();
  }
  return ret;
}

int ObKVGlobalCache::get(
  const int64_t cache_id,
  const ObIKVCacheKey &key,
  const ObIKVCacheValue *&pvalue,
  ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (FALSE_IT(revert(mb_handle))) {
  } else if (OB_FAIL(map_.get(cache_id, key, pvalue, mb_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to get value from map, ", K(ret));
    }
  }
  return ret;
}

int ObKVGlobalCache::erase(const int64_t cache_id, const ObIKVCacheKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (OB_FAIL(map_.erase(cache_id, key))) {
    COMMON_LOG(WARN, "Fail to erase key from cache, ", K(cache_id), K(ret));
  }
  return ret;
}

int ObKVGlobalCache::erase_cache()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    store_.flush_washable_mbs();
    if (OB_FAIL(map_.erase_all())) {
      COMMON_LOG(WARN, "fail to erase cache, ", K(ret));
    }
  }
  return ret;
}

int ObKVGlobalCache::erase_cache(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    store_.flush_washable_mbs(tenant_id);
    if (OB_FAIL(map_.erase_tenant(tenant_id))) {
      COMMON_LOG(WARN, "fail to erase cache, ", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObKVGlobalCache::sync_flush_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The global kvcache has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(mem_limit_getter_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unexpected null mem limit getter", K(ret), KP(mem_limit_getter_));
  } else if (mem_limit_getter_->has_tenant(tenant_id)) {  // check tenant
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The tenant is still existed", K(ret), K(tenant_id));
  } else if (OB_FAIL(insts_.mark_tenant_delete(tenant_id))) {
    COMMON_LOG(WARN, "Fail to mark tenant cache inst delete", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_.flush_washable_mbs(tenant_id, true /* force flush */))) {
    COMMON_LOG(WARN, "Fail to erase tenant from store", K(ret), K(tenant_id));
  } else if (OB_FAIL(map_.erase_tenant(tenant_id, true /* force_erase */))) {
    COMMON_LOG(WARN, "Fail to retire cache node from map", K(ret), K(tenant_id));
  } else if (OB_FAIL(insts_.erase_tenant(tenant_id))) {
    COMMON_LOG(WARN, "Fail to erase tenant from insts", K(ret), K(tenant_id));
  }

  COMMON_LOG(INFO, "erase tenant cache details", K(ret), K(tenant_id));

  return ret;
}

int ObKVGlobalCache::erase_cache(const uint64_t tenant_id, const char *cache_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    int64_t cache_id = -1;
    for (int16_t i = 0; i < MAX_CACHE_NUM; ++i) {
      if (configs_[i].is_valid_) {
        if (0 == STRNCMP(configs_[i].cache_name_, cache_name, MAX_CACHE_NAME_LENGTH)) {
          cache_id = i;
          break;
        }
      }
    }
    if (-1 != cache_id) {
      store_.flush_washable_mbs(tenant_id, cache_id);
      if (OB_FAIL(map_.erase_tenant_cache(tenant_id, cache_id))) {
        COMMON_LOG(WARN, "fail to erase cache, ", K(ret), K(tenant_id));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument, ", K(ret));
    }
  }
  return ret;
}

int ObKVGlobalCache::erase_cache(const char *cache_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheMap has not been inited, ", K(ret));
  } else {
    int64_t cache_id = -1;
    for (int16_t i = 0; i < MAX_CACHE_NUM; ++i) {
      if (configs_[i].is_valid_) {
        if (0 == STRNCMP(configs_[i].cache_name_, cache_name, MAX_CACHE_NAME_LENGTH)) {
          cache_id = i;
          break;
        }
      }
    }
    if (-1 != cache_id) {
      store_.flush_washable_mbs(cache_id);
      if (OB_FAIL(map_.erase_all(cache_id))) {
        COMMON_LOG(WARN, "fail to erase cache, ", K(ret), K(cache_id));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Invalid argument, ", K(ret));
    }
  }
  return ret;
}

int ObKVGlobalCache::register_cache(
  const char *cache_name,
  const int64_t priority,
  int64_t &cache_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (NULL == cache_name || priority <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(cache_name), K(priority), K(ret));
  } else {
    int64_t i = 0;
    lib::ObMutexGuard guard(mutex_);
    for (i = 0; OB_SUCC(ret) && i < cache_num_; ++i) {
      if (configs_[i].is_valid_) {
        if (0 == STRNCMP(cache_name, configs_[i].cache_name_, MAX_CACHE_NAME_LENGTH)) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "The cache name has been registered, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (cache_num_ >= MAX_CACHE_NUM) {
        ret = OB_SIZE_OVERFLOW;
        COMMON_LOG(WARN, "Can not register more cache, ", K(ret));
      } else {
        cache_id = cache_num_++;
        STRNCPY(configs_[cache_id].cache_name_, cache_name, MAX_CACHE_NAME_LENGTH - 1);
        configs_[cache_id].cache_name_[MAX_CACHE_NAME_LENGTH - 1] = '\0';
        configs_[cache_id].priority_ = priority;
        configs_[cache_id].is_valid_ = true;
      }
    }
  }

  return ret;
}

void ObKVGlobalCache::deregister_cache(const int64_t cache_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(cache_id >= MAX_CACHE_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    configs_[cache_id].is_valid_ = false;
  }

  if (OB_SUCC(ret)) {
    COMMON_LOG(INFO, "Success to deregister cache, ", K(cache_id));
  }
}

int ObKVGlobalCache::create_working_set(const ObKVCacheInstKey &inst_key, ObWorkingSet *&working_set)
{
  int ret = OB_SUCCESS;
  int64_t lower_limit = 0;
  int64_t upper_limit = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!inst_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(inst_key));
  } else if (OB_FAIL(mem_limit_getter_->get_tenant_mem_limit(
      inst_key.tenant_id_, lower_limit, upper_limit))) {
    LOG_WARN("get_tenant_mem_limit failed", K(ret), K(inst_key));
  } else {
    const int64_t limit = upper_limit * WORKING_SET_LIMIT_PERCENTAGE / 100;
    if (OB_FAIL(ws_mgr_.create_working_set(inst_key, limit, working_set))) {
      LOG_WARN("create_working_set failed", K(ret), K(inst_key), K(limit));
    }
  }
  return ret;
}

int ObKVGlobalCache::delete_working_set(ObWorkingSet *working_set)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == working_set) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(working_set));
  } else if (OB_FAIL(ws_mgr_.delete_working_set(working_set))) {
    LOG_WARN("delete_working_set failed", K(ret));
  }
  return ret;
}

int ObKVGlobalCache::set_priority(const int64_t cache_id, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(cache_id >= MAX_CACHE_NUM)
      || OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(priority), K(ret));
  } else if (configs_[cache_id].priority_ == priority) {
    //same priority, do nothing
  } else if (OB_FAIL(store_.set_priority(cache_id, configs_[cache_id].priority_, priority))) {
    COMMON_LOG(WARN, "Fail to set priority, ", K(cache_id), K(priority));
  } else {
    configs_[cache_id].priority_ = priority;
  }
  return ret;
}

void ObKVGlobalCache::wash()
{
  if (OB_LIKELY(inited_ && !stopped_)) {
    DEBUG_SYNC(BEFORE_BACKGROUND_WASH);
    static int64_t wash_count = 0;
    if (store_.wash() || (++wash_count >= MAP_WASH_CLEAN_INTERNAL)) {
      map_.clean_garbage_node(map_clean_pos_, map_once_clean_num_);
      wash_count = 0;
    }
  }
}

void ObKVGlobalCache::replace_map()
{
  if (inited_ && !stopped_) {
    int ret = OB_SUCCESS;
    int64_t replace_node_count = 0;
    if (map_replace_skip_count_ <= 0) {
      if (OB_FAIL(map_.replace_fragment_node(map_replace_pos_, replace_node_count, map_once_replace_num_))) {
      } else if (0 == replace_node_count) {
        map_replace_skip_count_ = MAP_REPLACE_ONCE_SKIP_COUNT;
      }
    } else {
      --map_replace_skip_count_;
    }
    COMMON_LOG(INFO, "replace map num details", K(ret), K(replace_node_count), K(map_once_replace_num_),
                                                K(map_replace_skip_count_));
  }
}

void ObKVGlobalCache::revert(ObKVMemBlockHandle *mb_handle)
{
  if (inited_ && NULL != mb_handle) {
    store_.de_handle_ref(mb_handle);
  }
}

void ObKVGlobalCache::reload_priority()
{
  int ret = OB_SUCCESS;
  int64_t priority = 0;
  for (int16_t i = 0; i < MAX_CACHE_NUM; ++i) {
    if (configs_[i].is_valid_) {
      if (0 == STRNCMP(configs_[i].cache_name_, "opt_table_stat_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().opt_tab_stat_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "opt_column_stat_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().opt_tab_stat_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "opt_ds_stat_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().opt_tab_stat_cache_priority;
      }else if (0 == STRNCMP(configs_[i].cache_name_, "tablet_ls_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().tablet_ls_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "index_block_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().index_block_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "user_block_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().user_block_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "user_row_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().user_row_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "fuse_row_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().fuse_row_cache_priority;
      } else if (0 == STRNCMP(configs_[i].cache_name_, "bf_cache", MAX_CACHE_NAME_LENGTH)) {
        priority = common::ObServerConfig::get_instance().bf_cache_priority;
      } else {
        priority = 0;
      }

      if (priority > 0) {
        if (OB_FAIL(set_priority(i, priority))) {
          COMMON_LOG(WARN, "Fail to set priority, ", K(i), K(priority));
        }
      }
    }
  }
}

int ObKVGlobalCache::reload_wash_interval()
{
  int ret = OB_SUCCESS;
  if (0 == cache_wash_interval_) {
    const int64_t wash_interval = GCONF._cache_wash_interval;
    bool is_exist = false;
    if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::KVCacheWash, wash_task_, is_exist))) {
      COMMON_LOG(WARN, "failed to check wash task exist", K(ret));
    } else if (is_exist && OB_FAIL(TG_CANCEL_R(lib::TGDefIDs::KVCacheWash, wash_task_))) {
      COMMON_LOG(WARN, "failed to cancel wash task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::KVCacheWash, wash_task_, wash_interval, true))) {
      COMMON_LOG(WARN, "failed to schedule wash task", K(ret));
    }

    is_exist = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::KVCacheRep, replace_task_, is_exist))) {
      COMMON_LOG(WARN, "failed to check replace task exist", K(ret));
    } else if (is_exist && OB_FAIL(TG_CANCEL_R(lib::TGDefIDs::KVCacheRep, replace_task_))) {
      COMMON_LOG(WARN, "failed to cancel replace task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::KVCacheRep, replace_task_, wash_interval, true))) {
      COMMON_LOG(WARN, "failed to schedule replace task", K(ret));
    }
    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "success to reload_wash_interval", K(wash_interval));
    }
  } else if (!inited_) {
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::KVCacheWash, wash_task_, cache_wash_interval_, true))) {
      COMMON_LOG(WARN, "failed to schedule wash task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::KVCacheRep, replace_task_,
                                   cache_wash_interval_, true))) {
      COMMON_LOG(WARN, "failed to schedule replace task", K(ret));
    }
  }
  return ret;
}

int ObKVGlobalCache::set_hold_size(const uint64_t tenant_id, const char *cache_name,
                                   const int64_t hold_size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == cache_name || hold_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(cache_name), K(hold_size));
  } else if (OB_FAIL(insts_.set_hold_size(tenant_id, cache_name, hold_size))) {
    COMMON_LOG(WARN, "set_hold_size failed", K(ret), K(tenant_id), KP(cache_name), K(hold_size));
  }
  return ret;
}

int ObKVGlobalCache::get_hold_size(const uint64_t tenant_id, const char *cache_name,
                                   int64_t &hold_size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(cache_name), K(hold_size));
  } else if (OB_FAIL(insts_.get_hold_size(tenant_id, cache_name, hold_size))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "get_hold_size failed", K(ret), K(tenant_id), KP(cache_name));
    }
  }
  return ret;
}

int ObKVGlobalCache::get_avg_cache_item_size(const uint64_t tenant_id, const char *cache_name,
                                             int64_t &avg_cache_item_size)
{
  int ret = OB_SUCCESS;
  int64_t cache_id = -1;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(cache_name));
  } else if (OB_FAIL(get_cache_id(cache_name, cache_id))) {
    COMMON_LOG(WARN, "get_cache_id failed", K(ret), K(cache_name));
  } else if (OB_FAIL(store_.get_avg_cache_item_size(tenant_id, cache_id, avg_cache_item_size))) {
    COMMON_LOG(WARN, "get_avg_cache_item_size failed", K(ret), K(tenant_id), K(cache_id));
  }
  return ret;
}

int ObKVGlobalCache::get_washable_size(const uint64_t tenant_id, int64_t &washable_size, const int64_t ratio)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ratio < 0 || ratio > ObKVCacheStore::MAX_RATIO)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_.get_washable_size(tenant_id, washable_size, ratio))) {
    COMMON_LOG(WARN, "get tenant washable size failed", K(ret), K(tenant_id), K(washable_size));
  }
  return ret;
}

int ObKVGlobalCache::sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                                   const bool wash_single_mb,
                                   ObICacheWasher::ObCacheMemBlock *&wash_blocks)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || wash_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), K(wash_size));
  } else if (OB_FAIL(store_.sync_wash_mbs(tenant_id, wash_size, wash_single_mb, wash_blocks))) {
    if (ret != OB_CACHE_FREE_BLOCK_NOT_ENOUGH) {
      COMMON_LOG(WARN, "sync_wash_mbs failed", K(ret), K(tenant_id), K(wash_size), K(wash_single_mb));
    }
  }
  return ret;
}

int ObKVGlobalCache::set_checker_cache_name(const char *cache_name)
{
  int ret = OB_SUCCESS;
  int64_t cache_id = INVALID_CACHE_ID;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] The ObKVGlobalCache has not been inited", K(ret));
  } else if (nullptr == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "[CACHE-HANDLE-CHECKER] Invalid argument", K(ret), KP(cache_name));
  } else {
    if (0 == STRNCMP(cache_name, ObKVCacheHandleRefChecker::ALL_CACHE_NAME, MAX_CACHE_NAME_LENGTH)) {
      cache_id = MAX_CACHE_NUM;
    } else {
      lib::ObMutexGuard guard(mutex_);
      for (int64_t i = 0 ; OB_SUCC(ret) && i < cache_num_ ; ++i) {
        if (configs_[i].is_valid_) {
          if (0 == STRNCMP(cache_name, configs_[i].cache_name_, MAX_CACHE_NAME_LENGTH)) {
            cache_id = i;
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObKVCacheHandleRefChecker::get_instance().set_cache_id(cache_id))) {
      COMMON_LOG(WARN, "Fail to set cache handle checker cache id", K(ret), K(cache_id));
    }
    COMMON_LOG(DEBUG, "[CACHE-HANDLE-CHECKER] set cache id details", K(ret), K(cache_id), K(cache_name));
  }
  return ret;
}

void ObKVGlobalCache::print_all_cache_info()
{
  if (OB_UNLIKELY(!inited_)) {
    COMMON_LOG_RET(WARN, common::OB_NOT_INIT, "The ObKVGlobalCache has not been inited, ");
  } else {
    insts_.print_all_cache_info();
    map_.print_hazard_version_info();
  }
}

int ObKVGlobalCache::get_cache_inst_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle> &inst_handles)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(insts_.get_cache_info(tenant_id, inst_handles))) {
    COMMON_LOG(WARN, "Fail to get all cache info", K(ret));
  }

  return ret;
}

int ObKVGlobalCache::get_memblock_info(const uint64_t tenant_id, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache has not been inited", K(ret));
  } else if (0 == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_.get_memblock_info(tenant_id, memblock_infos))) {
    COMMON_LOG(WARN, "Fail to get all memblock info", K(ret));
  }
  return ret;
}


int ObKVGlobalCache::get_cache_id(const char *cache_name, int64_t &cache_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache is not inited", K(ret));
  } else if (NULL == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cache_name", K(ret), KP(cache_name));
  } else {
    bool find = false;
    lib::ObMutexGuard guard(mutex_);
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < cache_num_; ++i) {
      if (configs_[i].is_valid_) {
        if (0 == STRNCMP(cache_name, configs_[i].cache_name_, MAX_CACHE_NAME_LENGTH)) {
          cache_id = i;
          find = true;
        }
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WARN, "cache not exist", K(ret), K(cache_name));
    }
  }
  return ret;
}

int ObKVGlobalCache::get_cache_name(const int64_t cache_id, char *cache_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVGlobalCache is not inited", K(ret));
  } else if (cache_id < 0 || cache_id > cache_num_ || nullptr == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(cache_id), KP(cache_name));
  } else {
    MEMCPY(cache_name, configs_[cache_id].cache_name_, MAX_CACHE_NAME_LENGTH);
  }
  return ret;
}


} // namespace common
} // namespace oceanbase

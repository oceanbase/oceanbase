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

#ifndef  OCEANBASE_COMMON_KV_STORE_CACHE_H_
#define  OCEANBASE_COMMON_KV_STORE_CACHE_H_

#include "storage/ob_storage_checked_object_base.h"
#include "storage/ob_storage_leak_checker.h"
#include "lib/lock/ob_mutex.h"
#include "lib/task/ob_timer.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/list/ob_list.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kvcache_inst_map.h"
#include "share/cache/ob_kvcache_map.h"
#include "share/cache/ob_kvcache_hazard_domain.h"
#include "sql/optimizer/ob_opt_default_stat.h"


namespace oceanbase
{
namespace observer
{
class ObServer;
}
namespace blocksstable
{
class ObMicroBlockBufferHandle;
}
namespace common
{
class ObKVCacheHandle;
class ObKVCacheIterator;

template <class Key, class Value>
class ObIKVCache
{
public:
  virtual int put(const Key &key, const Value &value, bool overwrite = true) = 0;
  virtual int put_and_fetch(const Key &key, const Value &value, const Value *&pvalue,
      ObKVCacheHandle &handle, bool overwrite = true) = 0;
  virtual int get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle) = 0;
  virtual int erase(const Key &key) = 0;
  virtual int alloc(const uint64_t tenant_id, const int64_t key_size, const int64_t value_size,
      ObKVCachePair *&kvpair, ObKVCacheHandle &handle, ObKVCacheInstHandle &inst_handle) = 0;
  virtual int put_kvpair(ObKVCacheInstHandle &inst_handle, ObKVCachePair *kvpair, ObKVCacheHandle &handle, bool overwrite = true);
};

template <class Key, class Value>
class ObKVCache : public ObIKVCache<Key, Value>
{
public:
  ObKVCache();
  virtual ~ObKVCache();
  int init(const char *cache_name, const int64_t priority = 1, const int64_t mem_limit_pct = 100);
  void destroy();
  int set_priority(const int64_t priority);
  int set_mem_limit_pct(const int64_t mem_limit_pct);
  virtual int put(const Key &key, const Value &value, bool overwrite = true);
  virtual int put_and_fetch(
    const Key &key,
    const Value &value,
    const Value *&pvalue,
    ObKVCacheHandle &handle,
    bool overwrite = true);
  virtual int get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle);
  int get_iterator(ObKVCacheIterator &iter);
  virtual int erase(const Key &key);
  virtual int alloc(
      const uint64_t tenant_id,
      const int64_t key_size,
      const int64_t value_size,
      ObKVCachePair *&kvpair,
      ObKVCacheHandle &handle,
      ObKVCacheInstHandle &inst_handle) override;
  int64_t size(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t count(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t get_hit_cnt(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t get_miss_cnt(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  double get_hit_rate(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t store_size(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t get_cache_id() const { return cache_id_; }
private:
  bool inited_;
  int64_t cache_id_;
};

class ObKVCacheHandle;
class ObKVGlobalCache : public lib::ObICacheWasher
{
  friend class observer::ObServer;
public:
  static const int64_t DEFAULT_ONCE_BATCH_GET_BUCKET_NUM = 10000;
  static ObKVGlobalCache &get_instance();
  int init(ObITenantMemLimitGetter *mem_limit_getter,
           const int64_t bucket_num = DEFAULT_BUCKET_NUM,
           const int64_t max_cache_size = DEFAULT_MAX_CACHE_SIZE,
           const int64_t block_size = lib::ACHUNK_SIZE,
           const int64_t cache_wash_interval = 0);
  void stop();
  void wait();
  void destroy();
  void reload_priority();
  int reload_wash_interval();
  int64_t get_suitable_bucket_num();
  int get_cache_inst_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle> &inst_handles);
  int get_memblock_info(const uint64_t tenant_id, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos);
  void print_all_cache_info();
  int erase_cache();
  virtual int erase_cache(const uint64_t tenant_id) override;
  int sync_flush_tenant(const uint64_t tenant_id);
  int erase_cache(const uint64_t tenant_id, const char *cache_name);
  int erase_cache(const char *cache_name);

  int set_hold_size(const uint64_t tenant_id, const char *cache_name, const int64_t hold_size);
  int get_hold_size(const uint64_t tenant_id, const char *cache_name, int64_t &hold_size);
  int get_avg_cache_item_size(const uint64_t tenant_id, const char *cache_name,
                              int64_t &avg_cache_item_size);

  int get_washable_size(const uint64_t tenant_id, int64_t &washable_size);

  // wash memblock from cache synchronously
  virtual int sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                            lib::ObICacheWasher::ObCacheMemBlock *&wash_blocks);
  int set_storage_leak_check_mod(const char *check_mod);
  int get_cache_name(const int64_t cache_id, char *cache_name);
  int get_batch_data_block_cache_key(ObIArray<blocksstable::ObMicroBlockCacheKey> &keys) {
    return map_.get_batch_data_block_cache_key(DEFAULT_ONCE_BATCH_GET_BUCKET_NUM, keys);
  }
  OB_INLINE int64_t get_bucket_num() const { return map_.get_bucket_num(); }
  int64_t get_block_size() const;
  HazardDomain& get_hazard_domain() { return hazard_domain_; }
private:
  template<class Key, class Value> friend class ObIKVCache;
  template<class Key, class Value> friend class ObKVCache;
  friend class ObKVCacheHandle;
  friend class HazptrHolder;
  ObKVGlobalCache();
  virtual ~ObKVGlobalCache();
  int register_cache(const char *cache_name, const int64_t priority, const int64_t mem_limit_pct, int64_t &cache_id);
  void deregister_cache(const int64_t cache_id);
  int set_priority(const int64_t cache_id, const int64_t priority);
  int set_mem_limit_pct(const int64_t cache_id, const int64_t mem_limit_pct);
  int put(
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    const ObIKVCacheValue *&pvalue,
    HazptrHolder &hazptr_holder,
    bool overwrite = true);
  int put(
    ObIKVCacheStore &store,
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    const ObIKVCacheValue *&pvalue,
    HazptrHolder &hazptr_holder,
    bool overwrite = true);
  int alloc(
      const int64_t cache_id,
      const uint64_t tenant_id,
      const int64_t key_size,
      const int64_t value_size,
      ObKVCachePair *&kvpair,
      HazptrHolder &hazptr_holder,
      ObKVCacheInstHandle &inst_handle);
  // int alloc(
  //     ObWorkingSet *working_set,
  //     const uint64_t tenant_id,
  //     const int64_t key_size,
  //     const int64_t value_size,
  //     ObKVCachePair *&kvpair,
  //     HazptrHolder &hazptr_holder,
  //     ObKVCacheInstHandle &inst_handle);
  int alloc(
      ObIKVCacheStore &store,
      const int64_t cache_id,
      const uint64_t tenant_id,
      const int64_t key_size,
      const int64_t value_size,
      ObKVCachePair *&kvpair,
      HazptrHolder &hazptr_holder,
      ObKVCacheInstHandle &inst_handle);
  int get(
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue *&pvalue,
    HazptrHolder &hazptr_holder);
  int erase(const int64_t cache_id, const ObIKVCacheKey &key);
  void revert(HazptrHolder& mb_handle);
  void wash();
  void replace_map();
  int get_cache_id(const char *cache_name, int64_t &cache_id);
private:
  static const int64_t DEFAULT_BUCKET_NUM = 10000000L;
  static const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;  //1T
  static const int64_t MAP_ONCE_CLEAN_RATIO = 50;  // 50 * 0.2 = 10s
  static const int64_t MAP_ONCE_REPLACE_RATIO = 100;  // 100 * 0.2 = 20s
  static const int64_t MAX_MAP_ONCE_CLEAN_NUM = 200000;  // 200K
  static const int64_t EXPAND_MAP_ONCE_CLEAN_RATIO = 10;
  static const int64_t MAX_MAP_ONCE_REPLACE_NUM = 100000;  // 100K
  static const int64_t TIMER_SCHEDULE_INTERVAL_US = 800 * 1000;
  static const int64_t WORKING_SET_LIMIT_PERCENTAGE = 5;
  static const int64_t BASE_SERVER_MEMORY_FACTOR = 1L << 31; // 2G is the start level
  static constexpr double MAX_RESERVED_MEMORY_RATIO = 0.3;
  static const int64_t MAX_BUCKET_NUM_LEVEL = 10;
  static const int64_t bucket_num_array_[MAX_BUCKET_NUM_LEVEL];
  static const int64_t PRINT_INTERVAL = 30 * 1000L * 1000L;
  static const int64_t MAP_WASH_CLEAN_INTERNAL = 10;
  static const int64_t MAP_REPLACE_ONCE_SKIP_COUNT = 10;
private:
  class KVStoreWashTask: public ObTimerTask
  {
  public:
    KVStoreWashTask()
    {
    }
    virtual ~KVStoreWashTask()
    {
    }
    void runTimerTask()
    {
      ObKVGlobalCache::get_instance().wash();
      HazardDomain::get_instance().wash();
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        ObKVGlobalCache::get_instance().print_all_cache_info();
      }
    }
  };
  class KVMapReplaceTask : public ObTimerTask
  {
  public:
    KVMapReplaceTask()
    {
    }
    virtual ~KVMapReplaceTask()
    {
    }
    void runTimerTask()
    {
      ObKVGlobalCache::get_instance().replace_map();
    }
  };
private:
  bool inited_;
  // mem limit getter
  ObITenantMemLimitGetter *mem_limit_getter_;
  // map
  ObKVCacheMap map_;
  // store
  ObKVCacheStore store_;
  // cache instances
  ObKVCacheInstMap insts_;
  // cache configs
  ObKVCacheConfig configs_[MAX_CACHE_NUM];
  HazardDomain hazard_domain_;
  int64_t cache_num_;
  lib::ObMutex mutex_;
  // timer and task
  int64_t map_clean_pos_;
  int64_t map_once_clean_num_;
  KVStoreWashTask wash_task_;
  int64_t map_replace_pos_;
  int64_t map_once_replace_num_;
  int64_t map_replace_skip_count_;
  KVMapReplaceTask replace_task_;
  bool stopped_;
  int64_t cache_wash_interval_;
};


class ObKVCacheHandle : storage::ObStorageCheckedObjectBase
{
public:
  ObKVCacheHandle();
  ~ObKVCacheHandle(); // release hazard pointer
  void reset(); // only release protection, hazard pointer is not released
  inline bool is_valid() const { return hazptr_holder_.is_valid(); }
  // simulate move obj, use must pay attention
  void move_from(ObKVCacheHandle &other);
  int assign(const ObKVCacheHandle& other);
  inline ObKVMemBlockHandle* get_mb_handle() const { return hazptr_holder_.get_mb_handle(); }
  inline void set_hazptr_holder(HazptrHolder& hazptr_holder) { this->hazptr_holder_.move_from(hazptr_holder); }
  bool need_trace() const;
  storage::ObStorageCheckID get_check_id() const { return static_cast<storage::ObStorageCheckID>(get_mb_handle()->inst_->cache_id_); }
  TO_STRING_KV(K_(hazptr_holder));
  
private:
  template<class Key, class Value> friend class ObIKVCache;
  template<class Key, class Value> friend class ObKVCache;
  friend class ObKVCacheIterator;
  friend class storage::ObStorageLeakChecker;
  friend class ObPointerSwizzleNode;

  HazptrHolder hazptr_holder_;
};

class ObKVCacheIterator
{
public:
  ObKVCacheIterator();
  virtual ~ObKVCacheIterator();
  int init(const int64_t cache_id, ObKVCacheMap *map);
  /**
   * get a kvpair from the kvcache, if return OB_SUCCESS, remember to call revert(handle)
   * to revert the handle.
   * @param key: out
   * @param value: out
   * @param handle: out
   * @return OB_SUCCESS or OB_ITER_END or other error code
   */
  template <class Key, class Value>
  int get_next_kvpair(const Key *&key, const Value *&value, ObKVCacheHandle &handle);
  void reset();
private:
  int64_t cache_id_;
  ObKVCacheMap *map_;
  int64_t pos_;
  common::ObArenaAllocator allocator_;
  common::ObList<ObKVCacheMap::Node, common::ObArenaAllocator> handle_list_;
  bool is_inited_;
};

//-------------------------------------------------------Template Methods----------------------------------------------------------

template <class Key, class Value>
int ObIKVCache<Key, Value>::put_kvpair(ObKVCacheInstHandle &inst_handle, ObKVCachePair *kvpair, ObKVCacheHandle &handle, bool overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == kvpair)
      || OB_UNLIKELY(NULL == kvpair->key_)
      || OB_UNLIKELY(NULL == kvpair->value_)
      || OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(kvpair), K(handle), K(ret));
  } else {
    if (OB_ISNULL(inst_handle.get_inst())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The inst is NULL, ", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().map_.put(*inst_handle.get_inst(),
        *kvpair->key_, kvpair, handle.hazptr_holder_, overwrite))) {
      if (OB_ENTRY_EXIST != ret) {
        COMMON_LOG(WARN, "Fail to put kvpair to map, ", K(ret));
      }
    } else {
      storage::ObStorageLeakChecker::get_instance().handle_hold(&handle);
    }
  }
  return ret;
}


/*
 * ------------------------------------------------------------ObKVCache-----------------------------------------------------------------
 */
template <class Key, class Value>
ObKVCache<Key, Value>::ObKVCache()
    : inited_(false), cache_id_(-1)
{
}

template <class Key, class Value>
ObKVCache<Key, Value>::~ObKVCache()
{
  destroy();
}

template <class Key, class Value>
int ObKVCache<Key, Value>::init(const char *cache_name, const int64_t priority, const int64_t mem_limit_pct)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCache has been inited, ", K(ret));
  } else if (OB_UNLIKELY(NULL == cache_name)
      || OB_UNLIKELY(priority <= 0 || mem_limit_pct <= 0 || mem_limit_pct > 100)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(cache_name), K(priority), K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().register_cache(cache_name, priority, mem_limit_pct, cache_id_))) {
    COMMON_LOG(WARN, "Fail to register cache, ", K(ret));
  } else {
    COMMON_LOG(INFO, "Succ to register cache", K(cache_name), K(priority), K_(cache_id));
    inited_ = true;
  }
  return ret;
}

template <class Key, class Value>
void ObKVCache<Key, Value>::destroy()
{
  if (OB_LIKELY(inited_)) {
    ObKVGlobalCache::get_instance().deregister_cache(cache_id_);
    inited_ = false;
  }
}

template <class Key, class Value>
int ObKVCache<Key, Value>::set_priority(const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(priority), K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().set_priority(cache_id_, priority))) {
    COMMON_LOG(WARN, "Fail to set priority, ", K(ret));
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::set_mem_limit_pct(const int64_t mem_limit_pct)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(mem_limit_pct <= 0 || mem_limit_pct > 100)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(mem_limit_pct), K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().set_mem_limit_pct(cache_id_, mem_limit_pct))) {
    COMMON_LOG(WARN, "Fail to set mem_limit_pct, ", K(ret));
  }

  return ret;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::size(const uint64_t tenant_id) const
{
  int64_t size = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        size += inst_handle.get_inst()->status_.store_size_;
        size += inst_handle.get_inst()->node_allocator_.allocated();
      }
    }
  }
  return size;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::count(const uint64_t tenant_id) const
{
  int64_t count = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        count = inst_handle.get_inst()->status_.kv_cnt_;
      }
    }
  }
  return count;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::get_hit_cnt(const uint64_t tenant_id) const
{
  int64_t hit_cnt = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        hit_cnt = inst_handle.get_inst()->status_.total_hit_cnt_.value();
      }
    }
  }
  return hit_cnt;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::get_miss_cnt(const uint64_t tenant_id) const
{
  int64_t miss_cnt = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        miss_cnt = inst_handle.get_inst()->status_.total_miss_cnt_;
      }
    }
  }
  return miss_cnt;
}

template <class Key, class Value>
double ObKVCache<Key, Value>::get_hit_rate(const uint64_t tenant_id) const
{
  UNUSED(tenant_id);
  return DEFAULT_CACHE_HIT_RATE;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::get_iterator(ObKVCacheIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(iter.init(cache_id_, &ObKVGlobalCache::get_instance().map_))) {
    COMMON_LOG(WARN, "Fail to init ObKVCacheIterator, ", K(ret));
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::put(const Key &key, const Value &value, bool overwrite)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObIKVCacheValue *pvalue = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().put(cache_id_, key, value, pvalue,
      handle.hazptr_holder_, overwrite))) {
    if (OB_ENTRY_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to put kv to ObKVGlobalCache, ", K_(cache_id), K(ret));
    }
  }
  return ret;
}


template <class Key, class Value>
int ObKVCache<Key, Value>::put_and_fetch(
    const Key &key,
    const Value &value,
    const Value *&pvalue,
    ObKVCacheHandle &handle,
    bool overwrite)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().put(cache_id_, key, value,
      reinterpret_cast<const ObIKVCacheValue *&>(pvalue), handle.hazptr_holder_, overwrite))) {
    if (OB_ENTRY_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to put kv to ObKVGlobalCache, ", K_(cache_id), K(ret));
    }
  } else {
    storage::ObStorageLeakChecker::get_instance().handle_hold(&handle);
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObIKVCacheValue *value = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else {
    handle.reset();
    if (OB_FAIL(ObKVGlobalCache::get_instance().get(cache_id_, key, value, handle.hazptr_holder_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "Fail to get value from ObKVGlobalCache, ", K(ret));
      }
    } else {
      pvalue = reinterpret_cast<const Value*> (value);
      storage::ObStorageLeakChecker::get_instance().handle_hold(&handle);
    }
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::erase(const Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().erase(cache_id_, key))) {
    COMMON_LOG(WARN, "Fail to erase key from ObKVGlobalCache, ", K_(cache_id), K(ret));
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::alloc(const uint64_t tenant_id, const int64_t key_size, const int64_t value_size,
    ObKVCachePair *&kvpair, ObKVCacheHandle &handle, ObKVCacheInstHandle &inst_handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().alloc(
          cache_id_,
          tenant_id,
          key_size,
          value_size,
          kvpair,
          handle.hazptr_holder_,
          inst_handle))) {
    COMMON_LOG(WARN, "failed to alloc", K(ret));
  } else {
    storage::ObStorageLeakChecker::get_instance().handle_hold(&handle);
  }

  return ret;
}


template <class Key, class Value>
int64_t ObKVCache<Key, Value>::store_size(const uint64_t tenant_id) const
{
  int64_t store_size = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        store_size += inst_handle.get_inst()->status_.store_size_;
      }
    }
  }
  return store_size;

}

/*
 * ----------------------------------------------------ObKVCacheIterator---------------------------------------------
 */
template <class Key, class Value>
int ObKVCacheIterator::get_next_kvpair(
    const Key *&key,
    const Value *&value,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  ObKVCacheMap::Node node;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheIterator has not been inited, ", K(ret));
  } else {
    handle.reset();
    while (OB_SUCC(ret)) {
      if (pos_ >= map_->bucket_num_ && handle_list_.empty()) {
        ret = OB_ITER_END;
      } else if (OB_SUCC(handle_list_.pop_front(node))) {
        bool protect_success;
        if (OB_FAIL(handle.hazptr_holder_.protect(protect_success, node.mb_handle_, node.seq_num_))) {
          COMMON_LOG(WARN, "protect failed", KP(node.mb_handle_));
        } else if (protect_success) {
          break;
        }
      } else {
        if (common::OB_ENTRY_NOT_EXIST == ret) {
          if (pos_ >= map_->bucket_num_) {
            ret = OB_ITER_END;
          } else if (OB_FAIL(map_->multi_get(cache_id_, pos_++, handle_list_))) {
            COMMON_LOG(WARN, "Fail to multi get from map, ", K(ret));
          }
        } else {
          COMMON_LOG(WARN, "Unexpected error, ", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    key = reinterpret_cast<const Key*>(node.key_);
    value = reinterpret_cast<const Value*>(node.value_);
    storage::ObStorageLeakChecker::get_instance().handle_hold(&handle);
  }
  return ret;
}

} // common
} // oceanbase

#endif //OCEANBASE_COMMON_KV_STORE_CACHE_H_

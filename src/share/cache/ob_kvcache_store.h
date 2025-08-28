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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_STORE_H_
#define OCEANBASE_CACHE_OB_KVCACHE_STORE_H_


#include "lib/allocator/ob_retire_station.h"
#include "lib/resource/ob_cache_washer.h"
#include "lib/resource/ob_resource_mgr.h"
#include "share/cache/ob_cache_utils.h"
#include "share/cache/ob_kvcache_hazard_pointer.h"
#include "share/cache/ob_kvcache_inst_map.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/ob_i_tenant_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{
class HazptrHolder;
class ObKVGlobalCache;
class ObIKVCacheStore
{
public:
  int store(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    ObKVCachePair *&kvpair,
    HazptrHolder& hazptr_holder,
    const enum ObKVCachePolicy policy = LRU);
  int alloc_kvpair(
      ObKVCacheInst &inst,
      const int64_t key_size,
      const int64_t value_size,
      ObKVCachePair *&kvpair,
      HazptrHolder& hazptr_holder,
      const enum ObKVCachePolicy policy = LRU);
protected:
  virtual int alloc(ObKVCacheInst &inst, const enum ObKVCachePolicy policy,
      const int64_t block_size, ObKVMemBlockHandle *&mb_handle) = 0;
  virtual int free(ObKVMemBlockHandle *mb_handle) = 0;
  virtual ObKVMemBlockHandle *&get_curr_mb(ObKVCacheInst &inst, const enum ObKVCachePolicy policy) = 0;
  virtual bool mb_status_match(ObKVCacheInst &inst,
      const enum ObKVCachePolicy policy, ObKVMemBlockHandle *mb_handle) = 0;
  virtual int64_t get_block_size() const = 0;
private:
  int alloc_kvpair_without_retry(
      ObKVCacheInst &inst,
      const int64_t key_size,
      const int64_t value_size,
      ObKVCachePair *&kvpair,
      HazptrHolder &hazptr_holder,
      const enum ObKVCachePolicy policy);
};

class ObKVCacheStore final : public ObIKVCacheStore,
    public ObIMBHandleAllocator
{
public:
  ObKVCacheStore();
  virtual ~ObKVCacheStore();
  int init(ObKVCacheInstMap &insts, const int64_t max_cache_size,
           const int64_t block_size, const ObITenantMemLimitGetter &mem_limit_getter);
  void destroy();
  int set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority);
  int refresh_score();
  bool wash();
  bool add_handle_ref(ObKVMemBlockHandle *mb_handle) const;
  bool add_handle_ref(ObKVMemBlockHandle *mb_handle, const int64_t seq_num) const;
  int64_t de_handle_ref(ObKVMemBlockHandle *mb_handle, const bool do_retire = true);
  int get_avg_cache_item_size(const uint64_t tenant_id, const int64_t cache_id,
                              int64_t &avg_cache_item_size);

  int get_washable_size(const uint64_t tenant_id, int64_t &washable_size);
  int flush_washable_mbs();
  int flush_washable_mbs(const uint64_t tenant_id, const bool force_flush = false);
  void flush_washable_mbs(const int64_t cache_id);
  void flush_washable_mbs(const uint64_t tenant_id, const int64_t cache_id);

  int sync_wash_mbs(const uint64_t tenant_id,
                    const int64_t wash_size,
                    lib::ObICacheWasher::ObCacheMemBlock *&wash_blocks,
                    const int64_t wash_cache_id = -1);

  virtual int alloc_mbhandle(ObKVCacheInst &inst, const int64_t block_size,
                             ObKVMemBlockHandle *&mb_handle);
  virtual int alloc_mbhandle(ObKVCacheInst &inst, ObKVMemBlockHandle *&mb_handle);
  virtual int alloc_mbhandle(const ObKVCacheInstKey &inst_key, ObKVMemBlockHandle *&mb_handle);
  virtual int free_mbhandle(ObKVMemBlockHandle *mb_handle, const bool do_retire);

  virtual int64_t get_block_size() const { return block_size_; }
  // implement functions of ObIObKVMemBlockHandleMgr
  virtual int alloc(ObKVCacheInst &inst, const enum ObKVCachePolicy policy,
      const int64_t block_size, ObKVMemBlockHandle *&mb_handle);
  virtual int free(ObKVMemBlockHandle *mb_handle);
  virtual ObKVMemBlockHandle *&get_curr_mb(ObKVCacheInst &inst, const enum ObKVCachePolicy policy);
  virtual bool mb_status_match(ObKVCacheInst &inst,
      const enum ObKVCachePolicy policy, ObKVMemBlockHandle *mb_handle);
  int get_memblock_info(const uint64_t tenant_id, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos);
  int print_tenant_memblock_info(ObDLink *link);
  static int64_t compute_mb_handle_num(const int64_t max_cache_size, const int64_t block_size)
  {
    return max_cache_size / block_size + 2 * (WASH_THREAD_RETIRE_LIMIT + RETIRE_LIMIT * OB_MAX_THREAD_NUM);
  }

private:
  int try_flush_washable_mb(const uint64_t tenant_id, lib::ObICacheWasher::ObCacheMemBlock*& wash_blocks,
            const int64_t cache_id = -1, const int64_t size_need_washed = INT64_MAX, const bool force_flush = false);
  int inner_flush_washable_mb(const int64_t cache_id, const int64_t size_to_wash, int64_t& size_washed,
    lib::ObICacheWasher::ObCacheMemBlock*& wash_blocks, ObTenantMBListHandle& list_handle, bool force_flush);
  void free_mbs(lib::ObTenantResourceMgrHandle& resource_handle, int64_t tenant_id, lib::ObICacheWasher::ObCacheMemBlock* wash_blocks);
  int inner_push_memblock_info(const ObKVMemBlockHandle &handle, ObIArray<ObKVCacheStoreMemblockInfo> &memblock_infos, int64_t tenant_id);
  void purge_mb_handle_retire_station();

  static const int64_t SYNC_WASH_MB_TIMEOUT_US = 100 * 1000; // 100ms
  static const int64_t RETIRE_LIMIT = 16;
  static const int64_t WASH_THREAD_RETIRE_LIMIT = 2048;
  static const int64_t SUPPLY_MB_NUM_ONCE = 128;
  static const int64_t SAFE_COUNT = 5;
  static const int64_t MAX_SKIP_REFRESH_TIMES = 100; // max skip refresh_score times during free time
  static const int64_t TENANT_WASH_THRESHOLD_RATIO = 8; // 1/256
  static const int64_t GLOBAL_WASH_THRESHOLD_RATIO = 9; // 1/512
  static const int64_t MAX_TENANT_WASH_THRESHOLD = 256L << 20;  // 256MB
  static const int64_t MIN_TENANT_WASH_THRESHOLD = 8L << 20;  // 8MB
  static const int64_t MAX_GLOBAL_WASH_THRESHOLD = 64L;  // 64 * 2M = 128M
  static const int64_t MIN_GLOBAL_WASH_THRESHOLD = 8L;  // 8 * 2M = 16M
  static const int64_t FLUSH_PRESERVE_TENANT_NUM = 10; // number preversed for flush
  static const int64_t DEFAULT_TENANT_BUCKET_NUM = 64;
  static const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;  //1T
  static const int64_t MAX_MB_NUM = DEFAULT_MAX_CACHE_SIZE / lib::ACHUNK_SIZE;

public:
  static const int64_t MAX_MB_HANDLE_NUM = 
        MAX_MB_NUM + 2 * (ObKVCacheStore::WASH_THREAD_RETIRE_LIMIT + ObKVCacheStore::RETIRE_LIMIT * OB_MAX_THREAD_NUM);

private:
struct WashCallBack {
  WashCallBack(ObKVCacheStore& store, uint64_t& freed_mem_size) : store_(store), freed_mem_size_(freed_mem_size) {}
  void operator()(ObKVMemBlockHandle *mb_handle);
  ObKVCacheStore& store_;
  uint64_t& freed_mem_size_;
};
struct SyncWashCallBack {
  SyncWashCallBack(ObKVCacheStore& store, HazardList& retire_list, lib::ObICacheWasher::ObCacheMemBlock*& wash_blocks,
      int64_t& size_washed, const int64_t size_to_wash, const int64_t tenant_id)
      : store_(store),
        retire_list_(retire_list),
        wash_blocks_(wash_blocks),
        size_washed_(size_washed),
        size_to_wash_(size_to_wash),
        tenant_id_(tenant_id)
  {}
  void operator()(ObKVMemBlockHandle *mb_handle);
  ObKVCacheStore& store_;
  HazardList& retire_list_;
  lib::ObICacheWasher::ObCacheMemBlock*& wash_blocks_;
  int64_t& size_washed_;
  const int64_t size_to_wash_;
  const int64_t tenant_id_;
};

struct StoreMBHandleCmp {
  bool operator()(const ObKVMemBlockHandle* a, const ObKVMemBlockHandle* b) const;
};
  struct WashHeap
  {
  public:
    WashHeap();
    virtual ~WashHeap();
    ObKVMemBlockHandle *add(ObKVMemBlockHandle *mb_handle);
    void reset();
    ObKVMemBlockHandle **heap_;
    int64_t heap_size_;
    int64_t mb_cnt_;
  };
  struct TenantWashInfo
  {
  public:
    TenantWashInfo();
    int add(ObKVMemBlockHandle *mb_handle);
    // put wash memblocks in cache_wash_heaps_ to wash_heap_
    void normalize();
    void reuse();
    int64_t cache_size_;
    int64_t lower_limit_;
    int64_t upper_limit_;
    int64_t mem_usage_;
    int64_t reserve_mem_;
    int64_t max_wash_size_;
    int64_t min_wash_size_;
    int64_t wash_size_;
    WashHeap wash_heap_;
    WashHeap cache_wash_heaps_[MAX_CACHE_NUM];
  };
  class MBHandlePointerWashPool {
  public:
    MBHandlePointerWashPool();
    int init(const int64_t count, const char *label);
    int alloc(const int64_t count, ObKVMemBlockHandle **&heap);
    void reuse();
    int destroy();
  private:
    bool inited_;
    int64_t total_count_;
    int64_t free_count_;
    ObKVMemBlockHandle **buf_;
    common::ObArenaAllocator allocator_;

  };
  typedef ObFixedHashMap<uint64_t, TenantWashInfo*> WashMap;
private:
  ObKVMemBlockHandle *alloc_mbhandle(
      ObKVCacheInst &inst,
      const enum ObKVCachePolicy policy,
      const int64_t block_size);
  int alloc_mbhandle(
    ObKVCacheInst &inst,
    const enum ObKVCachePolicy policy,
    const int64_t block_size,
    ObKVMemBlockHandle *&mb_handle);
  bool compute_tenant_wash_size();
  bool is_tenant_wash_valid(const int64_t tenant_wash_size, const int64_t tenant_cache_size);
  bool is_global_wash_valid(const int64_t total_tenant_wash_block_count, const int64_t global_cache_size);
  void wash_mb(ObKVMemBlockHandle *mb_handle);
  void wash_mbs(WashHeap &heap);
  bool try_wash_mb(ObKVMemBlockHandle *mb_handle, const uint64_t tenant_id, void *&buf, int64_t &mb_size);
  int do_wash_mb(ObKVMemBlockHandle *mb_handle, void *&buf, int64_t &mb_size);
  int init_wash_heap(WashHeap &heap, const int64_t heap_size);
  int prepare_wash_structs();
  void reuse_wash_structs();
  void destroy_wash_structs();

  void *alloc_mb(lib::ObTenantResourceMgrHandle &resource_handle,
        const uint64_t tenant_id,
        const int64_t block_size);
  void free_mb(lib::ObTenantResourceMgrHandle &resource_handle,
        const uint64_t tenant_id, void *ptr);

  static QClock &get_qclock()
  {
    static QClock qclock;
    return qclock;
  }
  static RetireStation &get_retire_station()
  {
    static RetireStation retire_station(get_qclock(), RETIRE_LIMIT);
    return retire_station;
  }

  int insert_mb_handle(common::ObDLink *head, ObKVMemBlockHandle *mb_handle);
  int remove_mb_handle(ObKVMemBlockHandle *mb_handle, const bool do_retire);
  void retire_mb_handle(ObKVMemBlockHandle *mb_handle, const bool do_retire);
  void retire_mb_handles(HazardList &retire_list, const bool do_retire);
  void reuse_mb_handles(HazardList &reclaim_list);
  bool try_supply_mb(const int64_t mb_count);
private:
  bool inited_;
  ObKVCacheInstMap *insts_;
  //data structures for store
  int64_t cur_mb_num_;
  int64_t max_mb_num_;
  int64_t block_size_;
  int64_t block_payload_size_;
  ObKVMemBlockHandle *mb_handles_;
  ObFixedQueue<ObKVMemBlockHandle> mb_handles_pool_;

  //data structures for wash
  lib::ObMutex wash_out_lock_;
  ObSimpleFixedArray<uint64_t> tenant_ids_;
  ObSimpleFixedArray<ObKVCacheInstHandle> inst_handles_;
  ObFreeHeap<TenantWashInfo> wash_info_free_heap_;
  WashMap tenant_wash_map_;
  MBHandlePointerWashPool mb_ptr_pool_;
  ObArenaAllocator washable_size_allocator_;
  ObWashableSizeInfo washbale_size_info_;
  ObWashableSizeInfo tmp_washbale_size_info_;
  double tenant_reserve_mem_ratio_;
  int64_t wash_itid_;
  const ObITenantMemLimitGetter *mem_limit_getter_;
};

ObKVMemBlockHandle* mb_handle_at(uint32_t handle_index);
uint32_t handle_index_of(ObKVMemBlockHandle* mb_handle);

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_KVCACHE_STORE_H_

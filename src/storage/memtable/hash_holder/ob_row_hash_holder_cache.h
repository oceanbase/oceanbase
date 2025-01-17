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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_H_

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "ob_row_hash_holder_info.h"
#include "ob_row_hash_holder_allocator.h"
#include "ob_row_hash_holder_monitor.h"

namespace oceanbase
{
namespace memtable
{

static constexpr bool DISABLE_DYNAMIC_ALLOC = false;

template <typename T>
struct RowHolderObjectCache {
  RowHolderList *&get_linked_pointer(RowHolderList &obj) { return obj.next_list_; }
  RowHolderNode *&get_linked_pointer(RowHolderNode &obj) { return obj.prev_; }
  RowHolderObjectCache(T *pool, int64_t pool_size);
  ~RowHolderObjectCache();
  T *fetch_cache_object();
  void revert_cache_object(T *node);
  T *pre_alloc_obj_pool_;
  int64_t pool_size_;
  T *next_cached_obj_;
  ObByteLock lock_;
  int64_t fetch_times_;
  int64_t revert_times_;
} CACHE_ALIGNED;

template <typename T>
struct RowHolderObjectCacheMultiThreadDispatcher {
  using cache_type = T;
  static constexpr int64_t BASE_CACHE_SIZE = 64;
  static constexpr int64_t ALLOC_SIZE_FOR_ONE_OBJ_CACHE = (sizeof(RowHolderObjectCache<T>) / BASE_CACHE_SIZE + 1) * BASE_CACHE_SIZE;
  RowHolderObjectCacheMultiThreadDispatcher()
  : thread_cache_(nullptr),
  cache_size_each_thread_(0),
  thread_size_(0),
  pool_begin_addr_(nullptr),
  alloc_size_for_pool_in_one_cache_(0) {}
  ~RowHolderObjectCacheMultiThreadDispatcher();
  int init(int64_t cache_size_each_thread, int64_t thread_cache_number);
  template <typename ...Args>
  T *fetch_cache_object_and_construct(uint64_t hash_val, Args &&...args);
  void revert_cache_object(T *node, int64_t cache_idx);
  RowHolderObjectCache<T> *thread_cache_;
  int64_t cache_size_each_thread_;
  int64_t thread_size_;
  char *pool_begin_addr_;
  int64_t alloc_size_for_pool_in_one_cache_;
};

struct HashHolderFactory {
  template <typename T>
  struct CacheTypeRouter;
  template <>
  struct CacheTypeRouter<RowHolderNode> {
    using type = RowHolderObjectCacheMultiThreadDispatcher<RowHolderNode>;
    static type &get_cache(HashHolderFactory &factory) { return factory.node_cache_; }
  };
  template <>
  struct CacheTypeRouter<RowHolderList> {
    using type = RowHolderObjectCacheMultiThreadDispatcher<RowHolderList>;
    static type &get_cache(HashHolderFactory &factory) { return factory.list_cache_; }
  };
  HashHolderFactory(HashHolderGlocalMonitor &monitor)
  : monitor_(monitor),
  node_cache_(),
  list_cache_() {}
  template <typename T>
  int64_t get_object_cached_thread_idx(T *obj);
  template <typename T, typename ...Args>
  int create(T *&obj, uint64_t hash, Args &&...args);
  template <typename T>
  void destroy(T *obj, uint64_t hash);
  int init(bool for_unit_test) {
    int ret = OB_SUCCESS;
    if (for_unit_test) {
      if (OB_FAIL(list_cache_.init(MIN_LIST_CACHE_CNT, 1))) {
        DETECT_LOG(WARN, "fail to init little list_cache", KR(ret));
      } else if (OB_FAIL(node_cache_.init(MIN_NODE_CACHE_CNT, 1))) {
        DETECT_LOG(WARN, "fail to init little node_cache", KR(ret));
      }
    } else {
      int64_t tenant_memory_limit = lib::get_tenant_memory_limit(MTL_ID());
      if (!is_user_tenant(MTL_ID()) || tenant_memory_limit < 1_GB) {// non-user tenant or small user tenant use little cache
        if (OB_FAIL(list_cache_.init(MIN_LIST_CACHE_CNT, 1))) {// 32KB
          DETECT_LOG(WARN, "fail to init little list_cache", KR(ret));
        } else if (OB_FAIL(node_cache_.init(MIN_NODE_CACHE_CNT, 1))) {// 32KB
          DETECT_LOG(WARN, "fail to init little node_cache", KR(ret));
        }
      } else {
        int64_t each_thread_cache_list_cnt = MIN(tenant_memory_limit * 0.01, 3.9_GB) / THREAD_CACHE_CNT / sizeof(RowHolderList); // 1%
        int64_t each_thread_cache_node_cnt = MIN(tenant_memory_limit * 0.01, 3.9_GB) / THREAD_CACHE_CNT / sizeof(RowHolderNode); // 1%
        if (OB_FAIL(list_cache_.init(each_thread_cache_list_cnt, THREAD_CACHE_CNT))) {
          DETECT_LOG(WARN, "fail to init little list_cache", KR(ret));
        } else if (OB_FAIL(node_cache_.init(each_thread_cache_node_cnt, THREAD_CACHE_CNT))) {
          DETECT_LOG(WARN, "fail to init little node_cache", KR(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      this->~HashHolderFactory();
    }
    return ret;
  }
public:
  static constexpr int64_t MIN_LIST_CACHE_CNT = 1LL << 10;// 1024
  static constexpr int64_t MIN_NODE_CACHE_CNT = 1LL << 10;// 1024
  static constexpr int64_t THREAD_CACHE_CNT = 1LL << 4;// 16
  HashHolderGlocalMonitor &monitor_;
  CacheTypeRouter<RowHolderNode>::type node_cache_;
  CacheTypeRouter<RowHolderList>::type list_cache_;
};
}
}

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_IPP_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_IPP_
#include "ob_row_hash_holder_cache.ipp"
#endif

#endif
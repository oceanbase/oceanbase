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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_OBJECT_MANAGER_
#define OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_OBJECT_MANAGER_

#include "lib/hash/ob_hashmap.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "observer/ob_req_time_service.h"

namespace oceanbase
{
namespace sql
{
class ObPlanCache;

class ObLCObjectManager
{
public:
  typedef common::hash::ObHashMap<ObCacheObjID, ObILibCacheObject*> IdCacheObjectMap;

  ObLCObjectManager() : object_id_(0) {}
  int init(int64_t hash_bucket, uint64_t tenant_id);
  int alloc(ObCacheObjGuard& guard,
            ObLibCacheNameSpace ns,
            uint64_t tenant_id,
            lib::MemoryContext &parent_context);
  int destroy_cache_obj(const bool is_leaked,
                        const uint64_t object_id);
  void free(ObILibCacheObject *&obj, const CacheRefHandleID ref_handle)
  {
    common_free(obj, ref_handle);
    obj = NULL;
  }
  template<class _callback>
  int foreach_cache_obj(_callback &callback) const;
  template<class _callback>
  int foreach_alloc_cache_obj(_callback &callback) const;
  template<class _callback>
  int atomic_get_cache_obj(ObCacheObjID id, _callback &callback);
  template<class _callback>
  int atomic_get_alloc_cache_obj(ObCacheObjID id, _callback &callback);
  int erase_cache_obj(ObCacheObjID id, const CacheRefHandleID ref_handle);
  int add_cache_obj(ObILibCacheObject *obj);
  int64_t get_cache_obj_size() const { return cache_obj_map_.size(); }
  IdCacheObjectMap &get_cache_obj_map() { return cache_obj_map_; }
  IdCacheObjectMap &get_alloc_cache_obj_map() { return alloc_cache_obj_map_; }
  uint64_t allocate_object_id() { return __sync_add_and_fetch(&object_id_, 1); }
  template<typename ClassT>
  static int alloc(lib::MemoryContext &mem_ctx,
                   ObILibCacheObject *&obj,
                   CacheRefHandleID ref_handle,
                   uint64_t tenant_id);

private:
  void inner_free(ObILibCacheObject *obj);
  void common_free(ObILibCacheObject *obj, const CacheRefHandleID ref_handle);

private:
  // used for generate cache obj ids
  volatile ObCacheObjID object_id_;
  /**
   *                                 library cache
   *                                       |key
   *                                   cache node
   *                            |          |          |
   *                        cache_obj  cache_obj  cache_obj
   *
   * In the library cache, each key corresponds to a cache node structure, which is very
   * inconvenient when traversing the output of the virtual table, so a map of key-cache_obj
   * is used to facilitate traversal.
   */
  IdCacheObjectMap cache_obj_map_;
  /**
   *
   * All cache_obj objects created by the alloc method will have their reference count incremented
   * and added to alloc_cache_obj_map_. when the free method is called, if the reference count of the
   * cache_obj object decreases to 0, it will be removed from alloc_cache_obj_map_. the role of
   * alloc_cache_obj_map_ is to traverse alloc_cache_obj_map_ to release the leaked memory when
   * memory leak occurs.
   */
  IdCacheObjectMap alloc_cache_obj_map_;
};

template<class _callback>
int ObLCObjectManager::foreach_cache_obj(_callback &callback) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_obj_map_.foreach_refactored(callback))) {
    OB_LOG(WARN, "traversal cache_obj_map_ failed", K(ret));
  }
  return ret;
}

template<class _callback>
int ObLCObjectManager::foreach_alloc_cache_obj(_callback &callback) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_cache_obj_map_.foreach_refactored(callback))) {
    OB_LOG(WARN, "traversal alloc_cache_obj_map_ failed", K(ret));
  }
  return ret;
}

template<class _callback>
int ObLCObjectManager::atomic_get_cache_obj(ObCacheObjID id, _callback &callback)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_obj_map_.atomic_refactored(id, callback))) {
    OB_LOG(WARN, "failed to atomic get cache obj", K(ret));
  }
  return ret;
}

template<class _callback>
int ObLCObjectManager::atomic_get_alloc_cache_obj(ObCacheObjID id, _callback &callback)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_cache_obj_map_.atomic_refactored(id, callback))) {
    OB_LOG(WARN, "failed to atomic get cache obj", K(ret));
  }
  return ret;
}

template<typename ClassT>
int ObLCObjectManager::alloc(lib::MemoryContext &mem_ctx,
                             ObILibCacheObject *&cache_obj,
                             CacheRefHandleID ref_handle,
                             uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  observer::ObGlobalReqTimeService::check_req_timeinfo();
  if (NULL == (ptr = (char *)mem_ctx->get_arena_allocator().alloc(sizeof(ClassT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate memory for lib cache node", K(ret));
  } else {
    cache_obj = new(ptr)ClassT(mem_ctx);
    cache_obj->set_tenant_id(tenant_id);
    cache_obj->inc_ref_count(ref_handle);
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_OBJECT_MANAGER_

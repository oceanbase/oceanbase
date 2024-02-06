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

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_lib_cache_object_manager.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "observer/ob_req_time_service.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{

int ObLCObjectManager::init(int64_t hash_bucket, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_obj_map_.create(hash::cal_next_prime(hash_bucket),
                                    ObModIds::OB_HASH_BUCKET_LC_STAT,
                                    ObModIds::OB_HASH_NODE_LC_STAT,
                                    tenant_id))) {
    LOG_WARN("failed to init cache obj map", K(ret));
  } else if (OB_FAIL(alloc_cache_obj_map_.create(hash::cal_next_prime(hash_bucket),
                                                 ObModIds::OB_HASH_BUCKET_LC_STAT,
                                                 ObModIds::OB_HASH_NODE_LC_STAT,
                                                 tenant_id))) {
    LOG_WARN("failed to init alloc cache obj map", K(ret));
  }
  return ret;
}

int ObLCObjectManager::alloc(ObCacheObjGuard& guard,
                             ObLibCacheNameSpace ns,
                             uint64_t tenant_id,
                             MemoryContext &parent_context)
{
  int ret = OB_SUCCESS;
  lib::MemoryContext entity = NULL;
  ObMemAttr mem_attr;
  ObILibCacheObject *cache_obj = NULL;
  mem_attr.tenant_id_ = tenant_id;
  mem_attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
  if (guard.ref_handle_ == MAX_HANDLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache object guard has not been initalized!", K(ret));
  } else if (ns <= NS_INVALID || ns >= NS_MAX || OB_ISNULL(LC_CO_ALLOC[ns])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("out of the max type", K(ret), K(ns));
  } else if (FALSE_IT(mem_attr.label_ = LC_NS_TYPE_LABELS[ns])) {
  } else if (OB_FAIL(parent_context->CREATE_CONTEXT(entity,
                     lib::ContextParam().set_mem_attr(mem_attr)))) {
    LOG_WARN("create entity failed", K(ret), K(mem_attr));
  } else if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL memory entity", K(ret));
  } else {
    WITH_CONTEXT(entity) {
      if (OB_FAIL(LC_CO_ALLOC[ns](entity, cache_obj, guard.ref_handle_, tenant_id))) {
        LOG_WARN("failed to create lib cache node", K(ret), K(ns));
      } else {
        uint64_t obj_id = allocate_object_id();
        cache_obj->object_id_ = obj_id;
        if (OB_FAIL(alloc_cache_obj_map_.set_refactored(obj_id, cache_obj))) {
          LOG_WARN("failed to add element to hashmap", K(ret));
          inner_free(cache_obj);
          entity = NULL;
          cache_obj = NULL;
        }
      }
    }
  }
  if (OB_FAIL(ret) && NULL != entity) {
    DESTROY_CONTEXT(entity);
    entity = NULL;
  }
  guard.cache_obj_ = cache_obj;
  return ret;
}

int ObLCObjectManager::add_cache_obj(ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cache_obj), K(ret));
  } else if (OB_FAIL(cache_obj_map_.set_refactored(cache_obj->get_object_id(), cache_obj))) {
    LOG_WARN("failed to set element", K(ret), K(cache_obj->get_object_id()));
  }
  return ret;
}

int ObLCObjectManager::erase_cache_obj(ObCacheObjID id, const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *cache_obj = NULL;
  if (OB_FAIL(cache_obj_map_.erase_refactored(id, &cache_obj))) {
    SQL_PC_LOG(WARN, "failed to erase cache obj", K(ret), K(id));
  } else {
    SQL_PC_LOG(DEBUG, "succeed to remove cache obj", K(id), K(ret));
    if (NULL != cache_obj) {
      // set logical deleted time
      cache_obj->set_logical_del_time(common::ObTimeUtility::current_monotonic_time());
      LOG_DEBUG("set logical del time", K(cache_obj->get_logical_del_time()),
                                        K(cache_obj->get_object_id()),
                                        K(cache_obj->added_lc()),
                                        K(cache_obj));
      common_free(cache_obj, ref_handle);
      cache_obj = NULL;
    }
  }
  return ret;
}

void ObLCObjectManager::common_free(ObILibCacheObject *cache_obj,
                                    const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    // do nothing
  } else {
    if (!cache_obj->added_lc()) {
        cache_obj->set_logical_del_time(ObTimeUtility::current_monotonic_time());
        LOG_WARN("set logical del time", K(cache_obj->get_logical_del_time()),
                                         K(cache_obj->added_lc()),
                                         K(cache_obj->get_object_id()),
                                         K(cache_obj->get_tenant_id()),
                                         K(lbt()));
    }
    int64_t ref_count = cache_obj->dec_ref_count(ref_handle);
    if (ref_count > 0) {
      // do nothing
    } else if (ref_count == 0) {
      uint64_t tenant_id = cache_obj->get_tenant_id();
      if (OB_FAIL(cache_obj->before_cache_evicted())) {
        LOG_WARN("failed to process before_cache_evicted");
      } else if (OB_FAIL(destroy_cache_obj(false, cache_obj->get_object_id()))) {
        LOG_WARN("failed to destroy cache obj", K(ret));
      }
    } else {
      LOG_ERROR("invalid cache obj ref count", K(ref_count), KP(cache_obj));
    }
  }
}

int ObLCObjectManager::destroy_cache_obj(const bool is_leaked,
                                         const uint64_t object_id)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *to_del_obj;
  if (OB_FAIL(alloc_cache_obj_map_.erase_refactored(object_id, &to_del_obj))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase element from alloc obj list", K(object_id), K(ret));
    }
  } else if (OB_ISNULL(to_del_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cache obj", K(to_del_obj));
  } else {
    to_del_obj->dump_deleted_log_info(!is_leaked);
    inner_free(to_del_obj);
  }
  return ret;
}

void ObLCObjectManager::inner_free(ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  lib::MemoryContext entity = cache_obj->get_mem_context();
  WITH_CONTEXT(entity) { cache_obj->~ObILibCacheObject(); }
  cache_obj = NULL;
  DESTROY_CONTEXT(entity);
}

} // namespace common
} // namespace oceanbase

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

#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_sql.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "lib/alloc/malloc_hook.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_req_time_service.h"

namespace oceanbase {
using namespace common;
using namespace lib;
namespace sql {

int ObCacheObjectFactory::alloc(ObPhysicalPlan*& plan, const CacheRefHandleID ref_handle, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCacheObject* cache_obj = NULL;
  if (OB_FAIL(alloc(cache_obj, ref_handle, T_CO_SQL_CRSR, tenant_id))) {
    LOG_WARN("alloc physical plan failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(cache_obj) || OB_UNLIKELY(!cache_obj->is_sql_crsr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache object is invalid", KPC(cache_obj));
  } else {
    plan = static_cast<ObPhysicalPlan*>(cache_obj);
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    ObCacheObjectFactory::free(cache_obj, ref_handle);
    cache_obj = NULL;
  }
  return ret;
}

int ObCacheObjectFactory::alloc(
    ObCacheObject*& cache_obj, const CacheRefHandleID ref_handle, ObCacheObjType co_type, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  observer::ObGlobalReqTimeService::check_req_timeinfo();
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id;
  ObPlanCache* plan_cache = get_plan_cache(tenant_id);
  if (T_CO_PRCR == co_type || T_CO_PKG == co_type || T_CO_ANON == co_type) {
    mem_attr.label_ = ObNewModIds::OB_SQL_PHY_PL_OBJ;
  } else {
    mem_attr.label_ = ObNewModIds::OB_SQL_PHY_PLAN;
  }
  mem_attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
  MemoryContext* entity = NULL;

  if (OB_UNLIKELY(co_type < T_CO_SQL_CRSR) || OB_UNLIKELY(co_type >= T_CO_MAX) || OB_ISNULL(plan_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("co_type is invalid", K(co_type), K(plan_cache), K(tenant_id));
  } else if (OB_FAIL(ROOT_CONTEXT.CREATE_CONTEXT(entity, lib::ContextParam().set_mem_attr(mem_attr)))) {
    LOG_WARN("create entity failed", K(ret), K(mem_attr));
  } else if (NULL == entity) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL memory entity", K(ret));
  }

  if (OB_SUCC(ret)) {
    WITH_CONTEXT(entity)
    {
      switch (co_type) {
        case T_CO_SQL_CRSR:
          if (NULL != (buf = entity->get_arena_allocator().alloc(sizeof(ObPhysicalPlan)))) {
            cache_obj = new (buf) ObPhysicalPlan(*entity);
          }
          break;
        case T_CO_ANON:
        case T_CO_PRCR:
        case T_CO_PKG:
        default:
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "fail to alloc cache obj", K(ret), KP(buf), K(co_type));
    } else {
      cache_obj->set_tenant_id(tenant_id);
      uint64_t cache_obj_id = plan_cache->allocate_plan_id();
      cache_obj->object_id_ = cache_obj_id;

      cache_obj->inc_ref_count(ref_handle);
      /*
       * to deconstruct the CacheObject object, you must first remove the corresponding kv pair from the deleted_map,
       * then the atomicity of hashmap can ensure that only one thread does the removal action, and
       * only this thread gets the CacheObject pointer to do subsequent destructuring
       */
      if (OB_FAIL(plan_cache->get_deleted_map().set_refactored(cache_obj_id, cache_obj))) {
        LOG_WARN("failed to add element to hashmap", K(ret));
        inner_free(cache_obj);
        cache_obj = NULL;
        entity = NULL;
      }
    }
  }

  if (OB_FAIL(ret) && NULL != entity) {
    DESTROY_CONTEXT(entity);
    entity = NULL;
  }
  if (NULL != plan_cache) {
    plan_cache->dec_ref_count();
    plan_cache = NULL;
  }
  return ret;
}

void ObCacheObjectFactory::common_free(ObCacheObject* cache_obj, const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    // nothing to do
  } else {
    int64_t ref_count = cache_obj->dec_ref_count(ref_handle);
    if (ref_count > 0) {
      if (!cache_obj->added_pc()) {
        cache_obj->set_logical_del_time(ObTimeUtility::current_monotonic_time());
        LOG_WARN("set logical del time",
            K(cache_obj->get_logical_del_time()),
            K(cache_obj->added_pc()),
            K(cache_obj->get_object_id()),
            K(cache_obj->get_tenant_id()),
            K(lbt()));
      }
    } else if (ref_count == 0) {
      uint64_t tenant_id = cache_obj->get_tenant_id();
      ObPlanCache* plan_cache = get_plan_cache(tenant_id);

      if (cache_obj->get_pre_calc_expr_handler() != NULL) {
        LOG_DEBUG("dec pre calculabe expression.",
            KP(cache_obj->get_pre_calc_expr_handler()),
            KP(cache_obj),
            K(cache_obj->get_pre_expr_ref_count()));
      }
      // decrease reference and remove it when reference count is ZERO.
      cache_obj->dec_pre_expr_ref_count();

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(plan_cache)) {
        // if plan cache is marked by destroyed_, this is not a ERROR.
        LOG_WARN("invalid null plan cache", K(ret), K(tenant_id));
      } else if (OB_FAIL(destroy_cache_obj(false, cache_obj->get_object_id(), plan_cache))) {
        LOG_WARN("failed to destroy cache obj", K(ret));
      }
      if (NULL != plan_cache) {
        plan_cache->dec_ref_count();
        plan_cache = NULL;
      }
    } else {
      LOG_ERROR("invalid plan ref count", K(ref_count), KP(cache_obj));
    }
  }
}

void ObCacheObjectFactory::inner_free(ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;

  MemoryContext& entity = cache_obj->get_mem_context();
  WITH_CONTEXT(&entity)
  {
    cache_obj->~ObCacheObject();
  }
  cache_obj = NULL;
  DESTROY_CONTEXT(&entity);
}

ObPlanCache* ObCacheObjectFactory::get_plan_cache(const uint64_t tenant_id)
{
  ObPlanCache* ret_pc = NULL;
  uint64_t used_tenant_id = tenant_id;
  ObPCMemPctConf default_conf;
  if (tenant_id > OB_SYS_TENANT_ID && tenant_id <= OB_MAX_RESERVED_TENANT_ID) {
    used_tenant_id = OB_SYS_TENANT_ID;
  }
  if (OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.sql_engine_->get_plan_cache_manager())) {
    LOG_WARN("invalid null sql engine", K(GCTX.sql_engine_));
  } else {
    ret_pc = GCTX.sql_engine_->get_plan_cache_manager()->get_or_create_plan_cache(used_tenant_id, default_conf);
  }
  return ret_pc;
}

int ObCacheObjectFactory::destroy_cache_obj(const bool is_leaked, const uint64_t obj_id, ObPlanCache* plan_cache)
{
  int ret = OB_SUCCESS;
  ObCacheObject* to_del_obj;
  if (OB_ISNULL(plan_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(plan_cache));
  } else if (OB_FAIL(plan_cache->get_deleted_map().erase_refactored(obj_id, &to_del_obj))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase element from deleted map", K(obj_id), K(ret));
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
}  // end namespace sql
}  // namespace oceanbase

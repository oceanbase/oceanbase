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
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "lib/alloc/malloc_hook.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_req_time_service.h"

namespace oceanbase
{
using namespace common;
using namespace pl;
using namespace lib;
namespace sql
{

int ObCacheObjectFactory::alloc(ObCacheObjGuard& guard, ObLibCacheNameSpace ns, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObPlanCache *lib_cache = MTL(ObPlanCache*);
    if (OB_ISNULL(lib_cache)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null plan cache", K(ret));
    } else if (OB_FAIL(lib_cache->alloc_cache_obj(guard, ns, tenant_id))) {
      LOG_WARN("failed to alloc cache obj", K(ret), K(ns));
    }
  }
  return ret;
}

void ObCacheObjectFactory::inner_free(ObILibCacheObject *&cache_obj,
                                      const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = cache_obj->get_tenant_id();
  MTL_SWITCH(tenant_id) {
    ObPlanCache *lib_cache = MTL(ObPlanCache*);
    if (OB_ISNULL(lib_cache)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid null plan cache");
    } else {
      lib_cache->free_cache_obj(cache_obj, ref_handle);
    }
  }
}

void ObCacheObjectFactory::inner_free(ObPlanCache *pc,
                                      ObILibCacheObject *&cache_obj,
                                      const CacheRefHandleID ref_handle)
{
  if (OB_ISNULL(pc)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid null plan cache");
  } else {
    pc->free_cache_obj(cache_obj, ref_handle);
  }
}

int ObCacheObjectFactory::destroy_cache_obj(const bool is_leaked,
                                            const uint64_t obj_id,
                                            ObPlanCache *lib_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lib_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null plan cache", K(ret));
  } else if (OB_FAIL(lib_cache->destroy_cache_obj(is_leaked, obj_id))) {
    LOG_WARN("failed to destory cache obj", K(ret), K(is_leaked), K(obj_id));
  }
  return ret;
}

int ObCacheObjGuard::force_early_release(ObPlanCache *plan_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj_)) {
    // do nothing
  } else if (OB_ISNULL(plan_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(ret));
  } else {
    ObCacheObjectFactory::free(plan_cache, cache_obj_, ref_handle_);
    cache_obj_ = NULL;
  }
  return ret;
}

} //end namespace sql
} //namespace oceanbase

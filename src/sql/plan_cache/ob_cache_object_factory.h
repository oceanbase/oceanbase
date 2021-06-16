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

#ifndef DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_FACTORY_H_
#define DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_FACTORY_H_
#include "sql/plan_cache/ob_cache_object.h"
#include "lib/allocator/page_arena.h"
#include "lib/objectpool/ob_global_factory.h"
#include "lib/objectpool/ob_tc_factory.h"

namespace oceanbase {
namespace sql {
class ObPhysicalPlan;
class ObPlanCache;
class ObCacheObjectFactory {
  friend class ObCacheObject;
  friend class ObPlanCacheManager;

public:
  static int alloc(ObCacheObject*& cache_obj, const CacheRefHandleID ref_handle, ObCacheObjType co_type,
      uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static int alloc(
      ObPhysicalPlan*& plan, const CacheRefHandleID ref_handle, uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  /* The reason for designing the interface like this:
   * The outer application will generally make a judgment when calling ObCacheObjectFactory::free:
   * if (NULL != cache_obj) {ObCacheObjectFactory::free(cache_obj);}
   * If cache_obj is not set to NULL after free, it may be freed again somewhere in the future
   */
#define DEF_FREE_CACHE_FUNC(CACHE_TYPE)                                              \
  inline static void free(CACHE_TYPE*& cache_obj, const CacheRefHandleID ref_handle) \
  {                                                                                  \
    ObCacheObject* tmp_obj = (ObCacheObject*)cache_obj;                              \
    common_free(tmp_obj, ref_handle);                                                \
    cache_obj = NULL;                                                                \
  }

  DEF_FREE_CACHE_FUNC(ObPhysicalPlan)
  DEF_FREE_CACHE_FUNC(ObCacheObject)
private:
  static void inner_free(ObCacheObject* cache_obj);
  static ObPlanCache* get_plan_cache(const uint64_t tenant_id);

  static void common_free(ObCacheObject* cache_obj, const CacheRefHandleID ref_handle);

  static int destroy_cache_obj(const bool is_leaked, const uint64_t obj_id, ObPlanCache* plan_cache);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_FACTORY_H_ */

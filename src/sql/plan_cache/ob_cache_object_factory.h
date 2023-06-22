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

namespace oceanbase
{
namespace pl

{
class ObPLFunction;
class ObPLPackage;
}  // namespace pl
namespace sql
{
class ObPhysicalPlan;
class ObPlanCache;
class ObCacheObjGuard;
class ObSql;
class ObCacheObjectFactory
{
friend class ObPlanCacheObject;
friend class ObPlanCache;
public:
  static int alloc(ObCacheObjGuard& guard,
                   ObLibCacheNameSpace ns,
                   uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static void inner_free(ObILibCacheObject *&cache_obj,
                         const CacheRefHandleID ref_handle);
  static void inner_free(ObPlanCache *pc,
                         ObILibCacheObject *&cache_obj,
                         const CacheRefHandleID ref_handle);
  template<typename ClassT>
  static void free(ClassT *&cache_obj, const CacheRefHandleID ref_handle)
  {
    ObILibCacheObject *tmp_obj = (ObILibCacheObject *)cache_obj;
    inner_free(tmp_obj, ref_handle);
    cache_obj = NULL;
  }
  template<typename ClassT>
  static void free(ObPlanCache *pc, ClassT *&cache_obj, const CacheRefHandleID ref_handle)
  {
    ObILibCacheObject *tmp_obj = (ObILibCacheObject *)cache_obj;
    inner_free(pc, tmp_obj, ref_handle);
    cache_obj = NULL;
  }

private:
  static int destroy_cache_obj(const bool is_leaked,
                               const uint64_t obj_id,
                               ObPlanCache *lib_cache);
};


class ObCacheObjGuard {
friend class ObPlanCache;
friend class ObLCObjectManager;
private:
  // access only
  ObILibCacheObject* cache_obj_;
  // readable and writable
  CacheRefHandleID ref_handle_;

public:
  ObCacheObjGuard()
    : cache_obj_(NULL),
    ref_handle_(MAX_HANDLE)
  {
  }
  ObCacheObjGuard(CacheRefHandleID ref_handle)
    : cache_obj_(NULL),
    ref_handle_(ref_handle)
  {
  }

  ~ObCacheObjGuard()
  {
    if (OB_ISNULL(cache_obj_)) {
      // do nothing
    } else {
      ObCacheObjectFactory::free(cache_obj_, ref_handle_);
      cache_obj_ = NULL;
    }
  }

  void init(CacheRefHandleID ref_handle)
  {
    ref_handle_ = ref_handle;
  }

  ObILibCacheObject* get_cache_obj() const
  {
    return cache_obj_;
  }

  CacheRefHandleID get_ref_handle() const
  {
    return ref_handle_;
  }

  int force_early_release(ObPlanCache *pc);

  // this function may be somewhat dangerous and may cause some memory leak.
  // Before use this function, PLEASE CONCAT @Shengle or @Juehui
  //
  // Why we provide swap, rather than assign?
  // We assume 'other' may be another stack variable, and it may be used by others
  // and therefore we cannot directly deconstruct it in this function. However, swap
  // need not destroy this variable in this function.
  //
  // Which scenario can use this function?
  // Change life cycle of current guard.
  void swap(ObCacheObjGuard& other)
  {
    ObCacheObjGuard tmp(MAX_HANDLE);

    tmp.cache_obj_ = this->cache_obj_;
    tmp.ref_handle_ = this->ref_handle_;

    this->cache_obj_ = other.cache_obj_;
    this->ref_handle_ = other.ref_handle_;

    other.cache_obj_ = tmp.cache_obj_;
    other.ref_handle_ = tmp.ref_handle_;

    // If not reset tmp in this line, the reference count of current cache_obj_
    //  will be mistakenly decrease.
    tmp.reset();
  }
  TO_STRING_KV(K_(cache_obj));
private:
  void reset(){
    cache_obj_ = NULL;
    ref_handle_ = MAX_HANDLE;
  }
};

  }  // namespace sql
} //namespace oceanbase
#endif /* DEV_SRC_SQL_PLAN_CACHE_OB_CACHE_OBJECT_FACTORY_H_ */

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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_CALLBACK_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_CALLBACK_

#include "ob_pcv_set.h"

namespace oceanbase {
namespace sql {

class ObPlanCacheAtomicOp {
protected:
  typedef common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet*> PlanCacheKV;

public:
  ObPlanCacheAtomicOp(const CacheRefHandleID ref_handle) : pcv_set_(NULL), ref_handle_(ref_handle)
  {}
  virtual ~ObPlanCacheAtomicOp()
  {}
  // get pcv_set and lock
  virtual int get_value(ObPCVSet*& pcv_set);
  // get pcv_set and increase reference count
  void operator()(PlanCacheKV& entry);

protected:
  // when get value, need lock
  virtual int lock(ObPCVSet& pcv_set) = 0;

protected:
  // According to the interface of ObHashTable, all returned values will be passed
  // back to the caller via the callback functor.
  // pcv_set_ - the plan cache value that is referenced.
  ObPCVSet* pcv_set_;
  CacheRefHandleID ref_handle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheAtomicOp);
};

class ObPlanCacheWlockAndRef : public ObPlanCacheAtomicOp {
public:
  ObPlanCacheWlockAndRef(const CacheRefHandleID ref_handle) : ObPlanCacheAtomicOp(ref_handle)
  {}
  virtual ~ObPlanCacheWlockAndRef()
  {}
  int lock(ObPCVSet& pcv_set)
  {
    return pcv_set.lock(false /*wlock*/);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheWlockAndRef);
};

class ObPlanCacheRlockAndRef : public ObPlanCacheAtomicOp {
public:
  ObPlanCacheRlockAndRef(const CacheRefHandleID ref_handle) : ObPlanCacheAtomicOp(ref_handle)
  {}
  virtual ~ObPlanCacheRlockAndRef()
  {}
  int lock(ObPCVSet& pcvs)
  {
    return pcvs.lock(true /*rlock*/);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheRlockAndRef);
};

class ObCacheObjAtomicOp {
protected:
  typedef common::hash::HashMapPair<ObCacheObjID, ObCacheObject*> ObjKV;

public:
  ObCacheObjAtomicOp(const CacheRefHandleID ref_handle) : cache_obj_(NULL), ref_handle_(ref_handle)
  {}
  virtual ~ObCacheObjAtomicOp()
  {}
  // get lock and increase reference count
  void operator()(ObjKV& entry);

  ObCacheObject* get_value() const
  {
    return cache_obj_;
  }

protected:
  ObCacheObject* cache_obj_;
  const CacheRefHandleID ref_handle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCacheObjAtomicOp);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // _OB_PLAN_CACHE_CALLBACK_H

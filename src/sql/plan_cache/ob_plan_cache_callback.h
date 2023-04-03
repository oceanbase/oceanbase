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

namespace oceanbase
{
namespace sql
{

class ObLibCacheAtomicOp
{
protected:
  typedef common::hash::HashMapPair<ObILibCacheKey*, ObILibCacheNode *> LibCacheKV;

public:
  ObLibCacheAtomicOp(const CacheRefHandleID ref_handle)
    : cache_node_(NULL), ref_handle_(ref_handle)
  {
  }
  virtual ~ObLibCacheAtomicOp() {}
  // get cache node and lock
  virtual int get_value(ObILibCacheNode *&cache_node);
  // get cache node and increase reference count
  void operator()(LibCacheKV &entry);

protected:
  // when get value, need lock
  virtual int lock(ObILibCacheNode &cache_node) = 0;
protected:
  // According to the interface of ObHashTable, all returned values will be passed
  // back to the caller via the callback functor.
  // cache_node_ - the plan cache value that is referenced.
  ObILibCacheNode *cache_node_;
  CacheRefHandleID ref_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLibCacheAtomicOp);
};

class ObLibCacheWlockAndRef : public ObLibCacheAtomicOp
{
public:
  ObLibCacheWlockAndRef(const CacheRefHandleID ref_handle)
    : ObLibCacheAtomicOp(ref_handle)
  {
  }
  virtual ~ObLibCacheWlockAndRef() {}
  int lock(ObILibCacheNode &cache_node)
  {
    return cache_node.lock(false/*wlock*/);
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObLibCacheWlockAndRef);
};

class ObLibCacheRlockAndRef : public ObLibCacheAtomicOp
{
public:
  ObLibCacheRlockAndRef(const CacheRefHandleID ref_handle)
    : ObLibCacheAtomicOp(ref_handle)
  {
  }
  virtual ~ObLibCacheRlockAndRef() {}
  int lock(ObILibCacheNode &cache_node)
  {
    return cache_node.lock(true/*rlock*/);
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObLibCacheRlockAndRef);
};

class ObCacheObjAtomicOp
{
protected:
  typedef common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> ObjKV;

public:
  ObCacheObjAtomicOp(const CacheRefHandleID ref_handle): cache_obj_(NULL), ref_handle_(ref_handle) {}
  virtual ~ObCacheObjAtomicOp() {}
  // get lock and increase reference count
  void operator()(ObjKV &entry);

  ObILibCacheObject *get_value() const { return cache_obj_; }

protected:
  ObILibCacheObject *cache_obj_;
  const CacheRefHandleID ref_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCacheObjAtomicOp);
};

}
}
#endif // _OB_PLAN_CACHE_CALLBACK_H

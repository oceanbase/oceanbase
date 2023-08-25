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
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_plan_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

ObILibCacheNode::~ObILibCacheNode()
{
  IGNORE_RETURN remove_all_plan_stat();
  free_cache_obj_array();
  co_list_.reset();
}

int ObILibCacheNode::init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj)
{
  UNUSED(ctx);
  UNUSED(cache_obj);
  int ret = OB_SUCCESS;
  return ret;
}

void ObILibCacheNode::free_cache_obj_array()
{
  if (OB_ISNULL(lib_cache_)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "lib cache is invalid");
  } else {
    ObLCObjectManager &mgr = lib_cache_->get_cache_obj_mgr();
    SpinWLockGuard lock_guard(co_list_lock_);
    ObILibCacheObject* obj = nullptr;
    while (!co_list_.empty()) {
      co_list_.pop_front(obj);
      if (OB_ISNULL(obj)) {
        //do nothing
      } else {
        CacheRefHandleID ref_handle = obj->get_dynamic_ref_handle();
        ref_handle = (ref_handle != MAX_HANDLE ? ref_handle : LC_REF_CACHE_NODE_HANDLE);
        mgr.free(obj, ref_handle);
        obj = NULL;
      }
    }
  }
}

int ObILibCacheNode::remove_all_plan_stat()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lib_cache_)) {
    LOG_WARN("lib cache is invalid");
  } else {
    SpinRLockGuard lock_guard(co_list_lock_);
    ObILibCacheObject* obj = nullptr;
    CacheObjList::const_iterator iter = co_list_.begin();
    for (; iter != co_list_.end(); iter++) {
      if (OB_ISNULL(obj = *iter)) {
        // do nothing
      } else if (obj->added_lc()
        && OB_FAIL(lib_cache_->remove_cache_obj_stat_entry(obj->get_object_id()))) {
        LOG_WARN("failed to remove plan stat", K(obj->get_object_id()), K(ret));
      }
    }
  }
  return ret;
}

int ObILibCacheNode::get_cache_obj(ObILibCacheCtx &ctx,
                                   ObILibCacheKey *key,
                                   ObILibCacheObject *&obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(inner_get_cache_obj(ctx, key, obj))) {
    LOG_DEBUG("failed to inner get cache obj", K(ret), K(key));
  } else {
    CacheRefHandleID ref_handle = obj->get_dynamic_ref_handle();
    ref_handle = (ref_handle != MAX_HANDLE ? ref_handle : LC_REF_CACHE_NODE_HANDLE);
    obj->inc_ref_count(ref_handle);
    LOG_DEBUG("succ to get cache obj", KPC(obj));
  }
  return ret;
}

int ObILibCacheNode::add_cache_obj(ObILibCacheCtx &ctx,
                                   ObILibCacheKey *key,
                                   ObILibCacheObject *obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lib_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is invalid", K(ret));
  } else if (OB_ISNULL(key) || OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(obj));
  } else if (OB_FAIL(inner_add_cache_obj(ctx, key, obj))) {
    LOG_WARN("failed to inner add cache obj", K(ret), K(key), K(obj));
  } else {
    {
      SpinWLockGuard lock_guard(co_list_lock_);
      if (OB_FAIL(co_list_.push_back(obj))) {
        LOG_WARN("failed to add cache obj to cache_obj_list", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      CacheRefHandleID ref_handle = obj->get_dynamic_ref_handle();
      ref_handle = (ref_handle != MAX_HANDLE ? ref_handle : LC_REF_CACHE_NODE_HANDLE);
      obj->inc_ref_count(ref_handle);
      obj->set_added_lc(true);
      LOG_DEBUG("succ to add cache obj", KPC(obj));
    }
  }
  return ret;
}

int ObILibCacheNode::lock(bool is_rdlock)
{
  int ret = OB_SUCCESS;
  // if the lock fails, keep retrying the lock until the lock_timeout_ts_ is exceeded
  if (is_rdlock) {
    if (!rwlock_.try_rdlock()) {
      const int64_t lock_timeout_ts = ObTimeUtility::current_time() + lock_timeout_ts_;
      if (OB_FAIL(rwlock_.rdlock(lock_timeout_ts))) {
        ret = OB_PC_LOCK_CONFLICT;
      }
    }
  } else {
    const int64_t lock_timeout_ts = ObTimeUtility::current_time() + lock_timeout_ts_;
    if (OB_FAIL(rwlock_.wrlock(lock_timeout_ts))) {
      ret = OB_PC_LOCK_CONFLICT;
    }
  }
  return ret;
}

int ObILibCacheNode::update_node_stat(ObILibCacheCtx &ctx)
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&(node_stat_.last_active_timestamp_), ObClockGenerator::getClock());
  ATOMIC_INC(&(node_stat_.execute_count_));
  return ret;
}

int64_t ObILibCacheNode::get_mem_size()
{
  int ret = OB_SUCCESS;
  int64_t total_mem_size = 0;
  SpinRLockGuard lock_guard(co_list_lock_);
  CacheObjList::iterator iter = co_list_.begin();
  for (; OB_SUCC(ret) && iter != co_list_.end(); iter++) {
    ObILibCacheObject *obj = *iter;
    if (OB_ISNULL(obj)) {
      BACKTRACE(ERROR, true, "invalid cache obj");
    } else {
      total_mem_size += obj->get_mem_size();
    }
  }
  total_mem_size += allocator_.total();
  return total_mem_size;
}

int64_t ObILibCacheNode::inc_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    if (OB_ISNULL(lib_cache_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid null lib cache", K(ret));
    } else {
      lib_cache_->get_ref_handle_mgr().record_ref_op(ref_handle);
    }
  }
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObILibCacheNode::dec_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    if (OB_ISNULL(lib_cache_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid null lib cache", K(ret));
    } else {
      lib_cache_->get_ref_handle_mgr().record_deref_op(ref_handle);
    }
  }
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    // do nothing
  } else if (0 == ref_count) {
    LOG_DEBUG("remove cache node", K(ref_count), K(this));
    if (OB_ISNULL(lib_cache_)) {
      LOG_ERROR("invalid null lib cache");
    } else {
      ObLCNodeFactory &ln_factory = lib_cache_->get_cache_node_factory();
      lib_cache_->dec_mem_used(get_mem_size());
      ln_factory.destroy_cache_node(this);
    }
  } else {
    LOG_ERROR("invalid pcv_set ref count", K(ref_count));
  }
  return ref_count;
}

int ObILibCacheNode::before_cache_evicted()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before_cache_evicted", K(this), KPC(this));
  return ret;
}

int ObILibCacheNode::remove_cache_obj_entry(const ObCacheObjID obj_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lib_cache_)) {
    LOG_WARN("lib cache is invalid");
  } else {
    ObLCObjectManager &mgr = lib_cache_->get_cache_obj_mgr();
    SpinWLockGuard lock_guard(co_list_lock_);
    CacheObjList::iterator iter = co_list_.begin();
    for (; OB_SUCC(ret) && iter != co_list_.end(); iter++) {
      ObILibCacheObject *obj = *iter;
      if (OB_ISNULL(obj)) {
        BACKTRACE(ERROR, true, "invalid cache obj");
      } else if (obj_id == obj->get_object_id()) {
        co_list_.erase(iter);
        CacheRefHandleID ref_handle = obj->get_dynamic_ref_handle();
        ref_handle = (ref_handle != MAX_HANDLE ? ref_handle : LC_REF_CACHE_NODE_HANDLE);
        mgr.free(obj, ref_handle);
        obj = NULL;
        break;
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

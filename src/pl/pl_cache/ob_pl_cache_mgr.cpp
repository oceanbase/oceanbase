/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_CACHE
#include "ob_pl_cache_mgr.h"
#include "observer/ob_req_time_service.h"
#include "sql/plan_cache/ob_plan_cache.h"

using namespace oceanbase::observer;

namespace oceanbase
{
namespace pl
{


int ObPLCacheMgr::get_pl_object(ObPlanCache *lib_cache, ObILibCacheCtx &ctx, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_get_pl_object);
  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  //guard.get_cache_obj() = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(lib_cache) || OB_ISNULL(pc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    pc_ctx.key_.sys_vars_str_ = pc_ctx.session_info_->get_sys_var_in_pc_str();
    if (OB_FAIL(lib_cache->get_cache_obj(ctx, &pc_ctx.key_, guard))) {
      PL_CACHE_LOG(DEBUG, "failed to get plan", K(ret));
      // if schema expired, update pl cache;
      if (OB_OLD_SCHEMA_VERSION == ret) {
        PL_CACHE_LOG(WARN, "start to remove pl object", K(ret), K(pc_ctx.key_));
        if (OB_FAIL(lib_cache->remove_cache_node(&pc_ctx.key_))) {
          PL_CACHE_LOG(WARN, "fail to remove pcv set when schema/plan expired", K(ret));
        } else {
          ret = OB_SQL_PC_NOT_EXIST;
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
        ObILibCacheObject *cache_obj = guard.get_cache_obj();
        ObCacheObjectFactory::free(lib_cache, cache_obj, guard.get_ref_handle());
        //guard.get_cache_obj() = NULL;
      }
    } else if (OB_ISNULL(guard.get_cache_obj()) ||
              (!guard.get_cache_obj()->is_prcr() &&
                !guard.get_cache_obj()->is_sfc() &&
                !guard.get_cache_obj()->is_pkg() &&
                !guard.get_cache_obj()->is_anon() &&
                guard.get_cache_obj()->get_ns() != ObLibCacheNameSpace::NS_CALLSTMT)) {
      ret = OB_ERR_UNEXPECTED;
      PL_CACHE_LOG(WARN, "cache obj is invalid", KPC(guard.get_cache_obj()));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
      // TODO PL pc_ctx
      ObILibCacheObject *cache_obj = guard.get_cache_obj();
      ObCacheObjectFactory::free(lib_cache, cache_obj, static_cast<ObPLCacheCtx &>(pc_ctx).handle_id_);
      //guard.get_cache_obj() = NULL;
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
      lib_cache->inc_hit_and_access_cnt();
    } else {
      lib_cache->inc_access_cnt();
    }
  }

  return ret;
}

int ObPLCacheMgr::get_pl_cache(ObPlanCache *lib_cache, ObCacheObjGuard& guard, ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  pc_ctx.handle_id_ = guard.get_ref_handle();
  if (OB_NOT_NULL(pc_ctx.session_info_) &&
      false == pc_ctx.session_info_->get_local_ob_enable_pl_cache()) {
    // do nothing
  } else if (OB_FAIL(get_pl_object(lib_cache, pc_ctx, guard))) {
    PL_CACHE_LOG(DEBUG, "fail to get plan", K(ret));
  } else if (OB_ISNULL(guard.get_cache_obj())) {
    ret = OB_ERR_UNEXPECTED;
    PL_CACHE_LOG(WARN, "cache obj is invalid", KPC(guard.get_cache_obj()));
  } else {
    // update pl func/package stat
    pl::PLCacheObjStat *stat = NULL;
    int64_t current_time = ObTimeUtility::current_time();
    pl::ObPLCacheObject* pl_object = static_cast<pl::ObPLFunction*>(guard.get_cache_obj());
    stat = &pl_object->get_stat_for_update();
    ATOMIC_INC(&(stat->hit_count_));
    ATOMIC_STORE(&(stat->last_active_time_), current_time);
  }
  return ret;
}

int ObPLCacheMgr::add_pl_object(ObPlanCache *lib_cache,
                                      ObILibCacheCtx &ctx,
                                      ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    PL_CACHE_LOG(WARN, "invalid cache obj", K(ret));
  } else if (OB_ISNULL(lib_cache) || OB_ISNULL(pc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    pc_ctx.key_.sys_vars_str_ = pc_ctx.session_info_->get_sys_var_in_pc_str();
    pl::PLCacheObjStat *stat = NULL;
    pl::ObPLCacheObject* pl_object = static_cast<pl::ObPLCacheObject*>(cache_obj);
    stat = &pl_object->get_stat_for_update();
    ATOMIC_STORE(&(stat->db_id_), pc_ctx.key_.db_id_);
    do {
      if (OB_FAIL(lib_cache->add_cache_obj(ctx, &pc_ctx.key_, cache_obj)) && OB_OLD_SCHEMA_VERSION == ret) {
        PL_CACHE_LOG(INFO, "schema in pl cache value is old, start to remove pl object", K(ret), K(pc_ctx.key_));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = lib_cache->remove_cache_node(&pc_ctx.key_))) {
          ret = tmp_ret;
          PL_CACHE_LOG(WARN, "fail to remove lib cache node", K(ret));
        }
      }
    } while (OB_OLD_SCHEMA_VERSION == ret);
  }
  return ret;
}

int ObPLCacheMgr::add_pl_cache(ObPlanCache *lib_cache, ObILibCacheObject *pl_object, ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_add_pl_object);
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(lib_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else if (OB_NOT_NULL(pc_ctx.session_info_) &&
              false == pc_ctx.session_info_->get_local_ob_enable_pl_cache()) {
    // do nothing
  } else if (OB_ISNULL(pl_object)) {
     ret = OB_INVALID_ARGUMENT;
     PL_CACHE_LOG(WARN, "invalid physical plan", K(ret));
  } else if (lib_cache->get_mem_hold() > lib_cache->get_mem_limit()) {
     ret = OB_REACH_MEMORY_LIMIT;
     PL_CACHE_LOG(DEBUG, "lib cache memory used reach the high water mark",
     K(lib_cache->get_mem_used()), K(lib_cache->get_mem_limit()), K(ret));
  } else if (pl_object->get_mem_size() >= lib_cache->get_mem_high()) {
    // do nothing
  } else {
    ObLibCacheNameSpace ns = NS_INVALID;
    switch (pl_object->get_ns()) {
      case NS_PRCR:
      case NS_SFC: {
        ns = NS_PRCR;
      }
        break;
      case NS_PKG:
      case NS_ANON:
      case NS_CALLSTMT: {
        ns = pl_object->get_ns();
      }
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        PL_CACHE_LOG(WARN, "pl object to cache is not valid", K(pl_object->get_ns()), K(ret));
      }
      break;
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(pc_ctx.key_.namespace_ = ns)) {
    } else if (OB_FAIL(add_pl_object(lib_cache, pc_ctx, pl_object))) {
      if (!is_not_supported_err(ret)
          && OB_SQL_PC_PLAN_DUPLICATE != ret) {
        PL_CACHE_LOG(WARN, "fail to add pl function", K(ret));
      }
    } else {
      (void)lib_cache->inc_mem_used(pl_object->get_mem_size());
    }
  }
  return ret;
}


// delete all pl cache obj
int ObPLCacheMgr::cache_evict_all_pl(ObPlanCache *lib_cache)
{
  int ret = OB_SUCCESS;
  PL_CACHE_LOG(TRACE, "cache evict all pl cache start");
  if (OB_ISNULL(lib_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    LCKeyValueArray to_evict_keys;
    ObGetPLKVEntryOp get_ids_op(&to_evict_keys, PCV_GET_PL_KEY_HANDLE);
    if (OB_FAIL(lib_cache->foreach_cache_evict(get_ids_op))) {
      PL_CACHE_LOG(WARN, "failed to foreach cache evict", K(ret));
    }
    PL_CACHE_LOG(TRACE, "cache evict all pl end");
  }

  return ret;
}

template<typename GETPLKVEntryOp, typename EvictAttr>
int ObPLCacheMgr::cache_evict_pl_cache_single(ObPlanCache *lib_cache, uint64_t db_id, EvictAttr &attr)
{
  int ret = OB_SUCCESS;
  PL_CACHE_LOG(TRACE, "cache evict single plan start");
  if (OB_ISNULL(lib_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    LCKeyValueArray to_evict_keys;
    GETPLKVEntryOp get_ids_op(db_id, attr, &to_evict_keys, PCV_GET_PL_KEY_HANDLE);
    if (OB_FAIL(lib_cache->foreach_cache_evict(get_ids_op))) {
      PL_CACHE_LOG(WARN, "failed to foreach cache evict", K(ret));
    }
  }
  PL_CACHE_LOG(TRACE, "cache evict single plan end");
  return ret;
}

template int ObPLCacheMgr::cache_evict_pl_cache_single<ObGetPLKVEntryBySchemaIdOp, uint64_t>(ObPlanCache *lib_cache, uint64_t db_id, uint64_t &schema_id);
template int ObPLCacheMgr::cache_evict_pl_cache_single<ObGetPLKVEntryBySQLIDOp, common::ObString>(ObPlanCache *lib_cache, uint64_t db_id, common::ObString &sql_id);

}
}
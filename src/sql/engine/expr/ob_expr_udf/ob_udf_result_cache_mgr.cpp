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

#define USING_LOG_PREFIX PL_UDF_RESULT_CACHE
#include "ob_udf_result_cache_mgr.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

using namespace oceanbase::observer;

namespace oceanbase
{
namespace pl
{

int ObPLUDFResultCacheMgr::get_udf_result_object(ObPlanCache *lib_cache, ObILibCacheCtx &ctx, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_get_pl_object);
  ObPLUDFResultCacheCtx &rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  //guard.get_cache_obj() = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(lib_cache) || OB_ISNULL(rc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    if (OB_FAIL(lib_cache->get_cache_obj(ctx, &rc_ctx.key_, guard))) {
      PL_UDF_RESULT_CACHE_LOG(DEBUG, "failed to get plan", K(ret));
      // if schema expired, update pl cache;
      if (OB_OLD_SCHEMA_VERSION == ret) {
        PL_UDF_RESULT_CACHE_LOG(WARN, "start to remove pl object", K(ret), K(rc_ctx.key_));
        if (OB_FAIL(lib_cache->remove_cache_node(&rc_ctx.key_))) {
          PL_UDF_RESULT_CACHE_LOG(WARN, "fail to remove pcv set when schema/plan expired", K(ret));
        } else {
          ret = OB_SQL_PC_NOT_EXIST;
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
        ObILibCacheObject *cache_obj = guard.get_cache_obj();
        ObCacheObjectFactory::free(lib_cache, cache_obj, guard.get_ref_handle());
      }
    } else if (OB_ISNULL(guard.get_cache_obj()) ||
              (guard.get_cache_obj()->get_ns() != ObLibCacheNameSpace::NS_UDF_RESULT_CACHE)) {
      ret = OB_ERR_UNEXPECTED;
      PL_UDF_RESULT_CACHE_LOG(WARN, "cache obj is invalid", KPC(guard.get_cache_obj()));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
      ObILibCacheObject *cache_obj = guard.get_cache_obj();
      ObCacheObjectFactory::free(lib_cache, cache_obj, static_cast<ObPLUDFResultCacheCtx &>(rc_ctx).handle_id_);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(guard.get_cache_obj())) {
      lib_cache->inc_hit_and_access_cnt();
    } else {
      lib_cache->inc_access_cnt();
    }
  }
  FLT_SET_TAG(pl_hit_pl_cache, (OB_NOT_NULL(guard.get_cache_obj()) && OB_SUCC(ret)));
  return ret;
}

int ObPLUDFResultCacheMgr::get_udf_result_cache(ObPlanCache *lib_cache, ObCacheObjGuard& guard, ObPLUDFResultCacheCtx &rc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  rc_ctx.handle_id_ = guard.get_ref_handle();

  if (OB_FAIL(get_udf_result_object(lib_cache, rc_ctx, guard))) {
    PL_UDF_RESULT_CACHE_LOG(DEBUG, "fail to get plan", K(ret));
  } else if (OB_ISNULL(guard.get_cache_obj())) {
    ret = OB_ERR_UNEXPECTED;
    PL_UDF_RESULT_CACHE_LOG(WARN, "cache obj is invalid", KPC(guard.get_cache_obj()));
  } else {
    // update stat
    pl::PLUDFResultCacheObjStat *stat = NULL;
    int64_t current_time = ObTimeUtility::current_time();
    pl::ObPLUDFResultCacheObject* result_object = static_cast<pl::ObPLUDFResultCacheObject*>(guard.get_cache_obj());
    stat = &result_object->get_stat_for_update();
    ATOMIC_INC(&(stat->hit_count_));
    ATOMIC_STORE(&(stat->last_active_time_), current_time);
  }
  return ret;
}

int ObPLUDFResultCacheMgr::add_udf_result_object(ObPlanCache *lib_cache,
                                      ObILibCacheCtx &ctx,
                                      ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObPLUDFResultCacheCtx &rc_ctx = static_cast<ObPLUDFResultCacheCtx&>(ctx);
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    PL_UDF_RESULT_CACHE_LOG(WARN, "invalid cache obj", K(ret));
  } else if (OB_ISNULL(lib_cache) || OB_ISNULL(rc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    pl::PLUDFResultCacheObjStat *stat = NULL;
    pl::ObPLUDFResultCacheObject* result_object = static_cast<pl::ObPLUDFResultCacheObject*>(cache_obj);
    stat = &result_object->get_stat_for_update();
    ATOMIC_STORE(&(stat->db_id_), rc_ctx.key_.db_id_);
    do {
      if (OB_FAIL(lib_cache->add_cache_obj(ctx, &rc_ctx.key_, cache_obj)) && OB_OLD_SCHEMA_VERSION == ret) {
        PL_UDF_RESULT_CACHE_LOG(INFO, "schema in pl cache value is old, start to remove pl object", K(ret), K(rc_ctx.key_));
      }
      if (ctx.need_destroy_node_) {
        PL_UDF_RESULT_CACHE_LOG(WARN, "fail to add cache obj, need destroy node", K(ret), K(rc_ctx.key_));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = lib_cache->remove_cache_node(&rc_ctx.key_))) {
          ret = tmp_ret;
          PL_UDF_RESULT_CACHE_LOG(WARN, "fail to remove lib cache node", K(ret));
        }
      }
    } while (OB_OLD_SCHEMA_VERSION == ret);
  }
  return ret;
}

int ObPLUDFResultCacheMgr::add_udf_result_cache(ObPlanCache *lib_cache, ObILibCacheObject *result_object, ObPLUDFResultCacheCtx &rc_ctx)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_add_pl_object);
  ObGlobalReqTimeService::check_req_timeinfo();
  int64_t max_result_size = 0;
  int64_t result_cache_max_size = 0;
  if (FALSE_IT(max_result_size = rc_ctx.result_cache_max_result_)) {
  } else if (FALSE_IT(result_cache_max_size = rc_ctx.result_cache_max_size_)) {
  } else if (OB_ISNULL(lib_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else if (OB_ISNULL(result_object)) {
    ret = OB_INVALID_ARGUMENT;
    PL_UDF_RESULT_CACHE_LOG(WARN, "invalid physical plan", K(ret));
  } else if (result_object->get_mem_size() >= max_result_size * result_cache_max_size / 100) {
    ret = OB_REACH_MEMORY_LIMIT;
    PL_UDF_RESULT_CACHE_LOG(WARN, "result object hold memory over max result size allowed",
                          K(result_object->get_mem_size()), K(max_result_size), K(ret));
  } else {
    ObLibCacheNameSpace ns = NS_UDF_RESULT_CACHE;
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(rc_ctx.key_.namespace_ = ns)) {
    } else if (OB_FAIL(add_udf_result_object(lib_cache, rc_ctx, result_object))) {
      if (!is_not_supported_err(ret)
          && OB_SQL_PC_PLAN_DUPLICATE != ret) {
        PL_UDF_RESULT_CACHE_LOG(WARN, "fail to add pl function", K(ret));
      }
    } else {
      (void)lib_cache->inc_mem_used(result_object->get_mem_size());
      FLT_SET_TAG(pl_add_cache_object_size, result_object->get_mem_size());
    }
  }
  FLT_SET_TAG(pl_add_cache_plan, OB_SUCCESS == ret);
  return ret;
}

int ObPLUDFResultCacheMgr::cache_evict_all_obj(ObPlanCache *lib_cache)
{
  int ret = OB_SUCCESS;
  PL_CACHE_LOG(TRACE, "cache evict all pl cache start");
  if (OB_ISNULL(lib_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else {
    LCKeyValueArray to_evict_keys;
    ObGetResultCacheKVEntryOp get_ids_op(&to_evict_keys, PC_REF_UDF_RESULT_HANDLE);
    if (OB_FAIL(lib_cache->foreach_cache_evict(get_ids_op))) {
      PL_UDF_RESULT_CACHE_LOG(WARN, "failed to foreach cache evict", K(ret));
    }
    PL_UDF_RESULT_CACHE_LOG(TRACE, "cache evict all pl end");
  }

  return ret;
}

int ObPLUDFResultCacheMgr::result_cache_evict(ObPlanCache *lib_cache)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  int64_t result_cache_max_size = 0;
  int64_t ob_result_cache_evict_percentage = 0;
  int64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  lib::ObLabel label("OB_RESULT_CACHE");
  int64_t result_cache_hold = lib_cache->get_label_hold(label);
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_config", K(ret));
  } else if (FALSE_IT(result_cache_max_size = tenant_config->result_cache_max_size)) {
  } else if (FALSE_IT(ob_result_cache_evict_percentage = tenant_config->ob_result_cache_evict_percentage)) {
  } else if (result_cache_hold >= result_cache_max_size * ob_result_cache_evict_percentage / 100) {
    LCKeyValueArray co_list;
    ObGetResultCacheKVEntryOp traverse_op(&co_list, PCV_EXPIRE_BY_MEM_HANDLE);
    if (OB_FAIL(lib_cache->get_cache_key_node_map().foreach_refactored(traverse_op))) {
      PL_UDF_RESULT_CACHE_LOG(WARN, "traversing cache_key_node_map failed", K(ret));
    } else {
      int64_t cache_evict_num = 0;
      int64_t N = co_list.count();
      int64_t mem_to_free = traverse_op.get_total_mem_used() / 2;
      LCKeyValueArray to_evict_list;

      std::make_heap(co_list.begin(), co_list.end(), [](const LCKeyValue &left, const LCKeyValue &right) -> bool {
        return left.node_->get_node_stat()->weight() > right.node_->get_node_stat()->weight();
      });
      while (OB_SUCC(ret) && mem_to_free > 0 && cache_evict_num < N) {
        LCKeyValue kv;
        std::pop_heap(co_list.begin(), co_list.end(), [](const LCKeyValue &left, const LCKeyValue &right) -> bool {
          return left.node_->get_node_stat()->weight() > right.node_->get_node_stat()->weight();
        });
        co_list.pop_back(kv);
        mem_to_free -= kv.node_->get_mem_size();
        ++cache_evict_num;
        if (OB_FAIL(to_evict_list.push_back(kv))) {
          PL_UDF_RESULT_CACHE_LOG(WARN, "failed to add to evict obj", K(ret));
        }
      }

      PL_UDF_RESULT_CACHE_LOG(INFO, "evict cache result start",
                                    K(tenant_id),
                                    "mem_hold", result_cache_hold,
                                    "max_size", result_cache_max_size,
                                    "mem_to_free", mem_to_free,
                                    "evict_cache_node_num", to_evict_list.count(),
                                    "tatal_cache_node_num", co_list.count());

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(lib_cache->batch_remove_cache_node(to_evict_list))) {
        PL_UDF_RESULT_CACHE_LOG(WARN, "failed to remove lib cache node", K(ret));
      }
      for (int64_t i = 0; i < co_list.count(); i++) {
        if (nullptr != co_list.at(i).node_) {
          co_list.at(i).node_->dec_ref_count(PCV_EXPIRE_BY_MEM_HANDLE);
        }
      }
      for (int64_t i = 0; i < to_evict_list.count(); i++) {
        if (nullptr != to_evict_list.at(i).node_) {
          to_evict_list.at(i).node_->dec_ref_count(PCV_EXPIRE_BY_MEM_HANDLE);
        }
      }
    }
  }

  return ret;
}

}
}

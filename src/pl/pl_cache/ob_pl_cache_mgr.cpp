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
#include "src/sql/plan_cache/ob_plan_cache_util.h"
using namespace oceanbase::observer;

namespace oceanbase
{
namespace pl
{
/*INFLUENCE_PL sys var:
 *div_precision_increment only mysql mode
 *nls_length_semantics
 *ob_compatibility_version
 *_enable_old_charset_aggregation
*/
static constexpr int64_t PL_CACHE_SYS_VAR_COUNT = 4;
static constexpr share::ObSysVarClassType InfluencePLMap[PL_CACHE_SYS_VAR_COUNT + 1] = {
  share::SYS_VAR_DIV_PRECISION_INCREMENT,
  share::SYS_VAR_NLS_LENGTH_SEMANTICS,
  share::SYS_VAR_OB_COMPATIBILITY_VERSION,
  share::SYS_VAR__ENABLE_OLD_CHARSET_AGGREGATION,
  share::SYS_VAR_INVALID
};

int ObPLCacheMgr::get_sys_var_in_pl_cache_str(ObBasicSessionInfo &session,
                                              ObIAllocator &allocator,
                                              ObString &sys_var_str)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_SYS_VARS_STR_SIZE = 256;
  ObObj val;
  ObSysVarInPC sys_vars;
  char *buf = nullptr;
  int64_t pos = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < PL_CACHE_SYS_VAR_COUNT; ++i) {
    val.reset();
    if (OB_FAIL(session.get_sys_variable(InfluencePLMap[i], val))) {
      LOG_WARN("failed to get sys_variable", K(InfluencePLMap[i]), K(ret));
    } else if (OB_FAIL(sys_vars.push_back(val))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t sys_var_encode_max_size = MAX_SYS_VARS_STR_SIZE;
    if (nullptr == (buf = (char *)allocator.alloc(MAX_SYS_VARS_STR_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocator memory", K(ret), K(MAX_SYS_VARS_STR_SIZE));
    } else if (OB_FAIL(sys_vars.serialize_sys_vars(buf, sys_var_encode_max_size, pos))) {
      if (OB_BUF_NOT_ENOUGH == ret || OB_SIZE_OVERFLOW ==ret) {
        ret = OB_SUCCESS;
        // expand MAX_SYS_VARS_STR_SIZE 3 times.
        for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
          sys_var_encode_max_size = 2 * sys_var_encode_max_size;
          if (NULL == (buf = (char *)allocator.alloc(sys_var_encode_max_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocator memory", K(ret), K(sys_var_encode_max_size));
          } else if (OB_FAIL(sys_vars.serialize_sys_vars(buf, sys_var_encode_max_size, pos))) {
            if (i != 2 && (OB_BUF_NOT_ENOUGH == ret || OB_SIZE_OVERFLOW ==ret)) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to serialize system vars", K(ret));
            }
          } else {
            break;
          }
        }
      } else {
        LOG_WARN("fail to serialize system vars", K(ret));
      }
      if (OB_SUCC(ret)) {
        (void)sys_var_str.assign(buf, int32_t(pos));
      }
    } else {
      (void)sys_var_str.assign(buf, int32_t(pos));
    }
  }

  return ret;
}

int ObPLCacheMgr::get_pl_object(ObPlanCache *lib_cache, ObILibCacheCtx &ctx, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_get_pl_object);
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  FLT_SET_TAG(pl_cache_key_id, pc_ctx.key_.key_id_);
  FLT_SET_TAG(pl_cache_key_name, pc_ctx.key_.name_);
  //guard.get_cache_obj() = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(lib_cache) || OB_ISNULL(pc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else if (OB_FAIL(get_sys_var_in_pl_cache_str(*pc_ctx.session_info_, tmp_alloc, pc_ctx.key_.sys_vars_str_))) {
    LOG_WARN("fail to gen sys var", K(ret));
  } else {
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
    pc_ctx.key_.sys_vars_str_.reset();
  }
  FLT_SET_TAG(pl_hit_pl_cache, (OB_NOT_NULL(guard.get_cache_obj()) && OB_SUCC(ret)));
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
  } else if (OB_FAIL(pc_ctx.adjust_definer_database_id())) {
    LOG_WARN("reset db_id failed!", K(ret));
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
  ObArenaAllocator tmp_alloc(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObPLCacheCtx &pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    PL_CACHE_LOG(WARN, "invalid cache obj", K(ret));
  } else if (OB_ISNULL(lib_cache) || OB_ISNULL(pc_ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null");
  } else if (OB_FAIL(get_sys_var_in_pl_cache_str(*pc_ctx.session_info_, tmp_alloc, pc_ctx.key_.sys_vars_str_))) {
    LOG_WARN("fail to gen sys var", K(ret));
  } else {
    pl::PLCacheObjStat *stat = NULL;
    pl::ObPLCacheObject* pl_object = static_cast<pl::ObPLCacheObject*>(cache_obj);
    stat = &pl_object->get_stat_for_update();
    ATOMIC_STORE(&(stat->db_id_), pc_ctx.key_.db_id_);
    do {
      if (OB_FAIL(lib_cache->add_cache_obj(ctx, &pc_ctx.key_, cache_obj)) && OB_OLD_SCHEMA_VERSION == ret) {
        PL_CACHE_LOG(INFO, "schema in pl cache value is old, start to remove pl object", K(ret), K(pc_ctx.key_));
      }
      if (ctx.need_destroy_node_) {
        PL_CACHE_LOG(WARN, "fail to add cache obj, need destroy node", K(ret), K(pc_ctx.key_));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = lib_cache->remove_cache_node(&pc_ctx.key_))) {
          ret = tmp_ret;
          PL_CACHE_LOG(WARN, "fail to remove lib cache node", K(ret));
        }
      }
    } while (OB_OLD_SCHEMA_VERSION == ret);
    pc_ctx.key_.sys_vars_str_.reset();
  }
  return ret;
}

int ObPLCacheMgr::add_pl_cache(ObPlanCache *lib_cache, ObILibCacheObject *pl_object, ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_add_pl_object);
  FLT_SET_TAG(pl_cache_key_id, pc_ctx.key_.key_id_);
  FLT_SET_TAG(pl_cache_key_name, pc_ctx.key_.name_);
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
     PL_CACHE_LOG(WARN, "lib cache memory used reach the high water mark",
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
    } else if (OB_FAIL(pc_ctx.adjust_definer_database_id())) {
      LOG_WARN("reset db_id failed!", K(ret));
    } else if (OB_FAIL(add_pl_object(lib_cache, pc_ctx, pl_object))) {
      if (!is_not_supported_err(ret)
          && OB_SQL_PC_PLAN_DUPLICATE != ret) {
        PL_CACHE_LOG(WARN, "fail to add pl function", K(ret));
      }
    } else {
      (void)lib_cache->inc_mem_used(pl_object->get_mem_size());
      FLT_SET_TAG(pl_add_cache_object_size, pl_object->get_mem_size());
    }
  }
  FLT_SET_TAG(pl_add_cache_plan, OB_SUCCESS == ret);
  return ret;
}

int ObPLCacheMgr::flush_pl_cache_by_sql(
                                  uint64_t key_id,
                                  uint64_t db_id,
                                  uint64_t tenant_id,
                                  share::schema::ObMultiVersionSchemaService & schema_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard tenant_schema_guard;
  const ObSimpleTenantSchema *tenant = NULL;
  
  ObString db_name;
  ObString tenant_name;
  //get tenant name
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, tenant_schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("failed get tenant info", K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is null", K(ret));
  } else {
    tenant_name = tenant->get_tenant_name_str();
  }

  if (OB_INVALID_ID != db_id) {
    //get db name
    const ObSimpleDatabaseSchema *database_schema = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(tenant_schema_guard.get_database_schema(tenant_id, db_id, database_schema))) {
      LOG_WARN("failed get db schema", K(ret));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant is null", K(ret));
    } else {
      db_name = database_schema->get_database_name();
    }
  }

  share::ObTenantRole tenant_role;
  ObMySQLProxy *sql_proxy = nullptr;
  // system tenant execute global flush
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(OB_SYS_TENANT_ID);
  ObSqlString sql;
  int64_t affected_rows = 0;
  CK (OB_NOT_NULL(sql_proxy = GCTX.sql_proxy_));
  CK (!tenant_name.empty());
  if (OB_SUCC(ret)) {
    if (!db_name.empty()) {
      OZ (sql.assign_fmt("alter system flush pl cache schema_id = %lu databases = \"%.*s\" TENANT = \"%.*s\" global", key_id, 
        db_name.length(), db_name.ptr(), tenant_name.length(), tenant_name.ptr()));
    } else {
      OZ (sql.assign_fmt("alter system flush pl cache schema_id = %lu TENANT = \"%.*s\" global", key_id, 
        tenant_name.length(), tenant_name.ptr()));
    }
  }
  OZ (sql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows));
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
template int ObPLCacheMgr::cache_evict_pl_cache_single<ObGetPLKVEntryByDbIdOp, uint64_t>(ObPlanCache *lib_cache, uint64_t db_id, uint64_t &schema_id);
template int ObPLCacheMgr::cache_evict_pl_cache_single<ObGetPLKVEntryBySQLIDOp, common::ObString>(ObPlanCache *lib_cache, uint64_t db_id, common::ObString &sql_id);

}
}
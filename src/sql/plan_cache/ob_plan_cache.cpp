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
#include "sql/plan_cache/ob_plan_cache.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/json/ob_json_print_utils.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/alloc_func.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_truncated_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"
#include "sql/plan_cache/ob_ps_cache_callback.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "observer/ob_req_time_service.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share::schema;
using namespace oceanbase::lib;
using namespace oceanbase::observer;
namespace oceanbase {
namespace sql {
struct ObGetPlanIdBySqlIdOp {
  explicit ObGetPlanIdBySqlIdOp(
      common::ObIArray<uint64_t>* key_array, const common::ObString& sql_id, const uint64_t& plan_hash_value)
      : key_array_(key_array), sql_id_(sql_id), plan_hash_value_(plan_hash_value)
  {}
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObCacheObject*>& entry)
  {
    int ret = common::OB_SUCCESS;
    ObPhysicalPlan* plan = NULL;
    if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
      ret = common::OB_NOT_INIT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (!entry.second->is_sql_crsr()) {
      // not sql plan
      // do nothing
    } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan*>(entry.second))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan", K(ret), K(plan));
    } else if (sql_id_ != plan->stat_.bl_info_.sql_id_) {
      // do nothing
    } else if (OB_INVALID_ID != plan_hash_value_ && plan->stat_.bl_info_.plan_hash_value_ != plan_hash_value_) {
      // do nothing
    } else if (OB_FAIL(key_array_->push_back(entry.first))) {
      SQL_PC_LOG(WARN, "fail to push back plan_id", K(ret));
    }

    return ret;
  }

  common::ObIArray<uint64_t>* key_array_;
  common::ObString sql_id_;
  uint64_t plan_hash_value_;
};

struct ObGetAllSqlIdOp {
  explicit ObGetAllSqlIdOp(common::ObIArray<PCKeyValue>* key_array, const CacheRefHandleID ref_handle)
      : key_array_(key_array), ref_handle_(ref_handle)
  {}
  int operator()(common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet*>& entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(key_array_), K(entry.second), K(ret));
    } else if (NS_CRSR == entry.first.namespace_) {
      if (OB_FAIL(key_array_->push_back(ObPCKeyValue(entry.first, entry.second)))) {
        SQL_PC_LOG(WARN, "fail to push back key", K(ret));
      } else {
        entry.second->inc_ref_count(ref_handle_);
      }
    } else {
      // do nothing
    }
    return ret;
  }

  common::ObIArray<PCKeyValue>* key_array_;
  const CacheRefHandleID ref_handle_;
};

struct ObGetAllPLIdOp {
  explicit ObGetAllPLIdOp(common::ObIArray<PCKeyValue>* key_array, const CacheRefHandleID ref_handle)
      : key_array_(key_array), ref_handle_(ref_handle)
  {}
  int operator()(common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet*>& entry)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(key_array_), K(entry.second));
    } else if (NS_PRCD == entry.first.namespace_ || NS_ANON == entry.first.namespace_ ||
               NS_PKG == entry.first.namespace_) {
      if (OB_FAIL(key_array_->push_back(ObPCKeyValue(entry.first, entry.second)))) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        entry.second->inc_ref_count(ref_handle_);
      }
    }
    return ret;
  }
  common::ObIArray<PCKeyValue>* key_array_;
  const CacheRefHandleID ref_handle_;
};

// operator get keys of all expired plan
struct ObGetExpiredKeyOp {
  explicit ObGetExpiredKeyOp(
      int64_t merged_version, common::ObIArray<PCKeyValue>* key_array, const CacheRefHandleID ref_handle)
      : merged_version_(merged_version), key_array_(key_array), ref_handle_(ref_handle)
  {}

  int operator()(common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet*>& entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
      ret = common::OB_NOT_INIT;
      SQL_PC_LOG(WARN, "invalid argument", K(key_array_), K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_VERSION == entry.second->get_min_merged_version()) ||
               0 == GCONF.merge_stat_sampling_ratio || NS_PRCD == entry.first.namespace_ ||
               NS_ANON == entry.first.namespace_ ||
               NS_PKG == entry.first.namespace_) {  // pl function, pl package do not need flush
      // do noting
    } else if (entry.second->get_min_merged_version() < merged_version_) {
      if (OB_FAIL(key_array_->push_back(ObPCKeyValue(entry.first, entry.second)))) {
        SQL_PC_LOG(WARN, "fail to push back key", K(ret));
      } else {
        entry.second->inc_ref_count(ref_handle_);
      }
    }

    return ret;
  }

  int64_t merged_version_;
  common::ObIArray<PCKeyValue>* key_array_;
  const CacheRefHandleID ref_handle_;
};

// true means entry_left is more active than entry_right
bool stat_compare(const PCKeyValue& left, const PCKeyValue& right)
{
  bool cmp_ret = false;
  if (OB_ISNULL(left.pcv_set_) || OB_ISNULL(right.pcv_set_)) {
    cmp_ret = false;
    SQL_PC_LOG(ERROR, "invalid argument", KP(left.pcv_set_), KP(right.pcv_set_), K(cmp_ret));
  } else if (OB_ISNULL(left.pcv_set_->get_stmt_stat()) || OB_ISNULL(right.pcv_set_->get_stmt_stat())) {
    cmp_ret = false;
    SQL_PC_LOG(
        ERROR, "invalid argument", K(left.pcv_set_->get_stmt_stat()), K(right.pcv_set_->get_stmt_stat()), K(cmp_ret));
  } else {
    cmp_ret = left.pcv_set_->get_stmt_stat()->weight() < right.pcv_set_->get_stmt_stat()->weight();
  }

  return cmp_ret;
}

// filter entries satisfy stat_condition
// get 'evict_num' number of sql_ids which weight is lower;
struct ObStatFilterOp {
  ObStatFilterOp(int64_t evict_num, PCKeyValueArray* evict_key_array, const CacheRefHandleID ref_handle)
      : evict_num_(evict_num), to_evict_keys_(evict_key_array), ref_handle_(ref_handle)
  {}

  int operator()(common::hash::HashMapPair<ObPlanCacheKey, ObPCVSet*>& entry)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(to_evict_keys_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (NULL == entry.second) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObPCKeyValue pc_kv(entry.first, entry.second);
      if (to_evict_keys_->count() < evict_num_) {
        if (OB_FAIL(to_evict_keys_->push_back(pc_kv))) {
          SQL_PC_LOG(WARN, "fail to push into evict_array", K(ret));
        } else {
          pc_kv.pcv_set_->inc_ref_count(ref_handle_);
        }
      } else {
        // replace one element
        if (stat_compare(pc_kv, to_evict_keys_->at(0))) {
          // pop_heap move the largest value to last
          std::pop_heap(to_evict_keys_->begin(), to_evict_keys_->end(), stat_compare);
          // remove and replace the largest value
          PCKeyValue kv;
          to_evict_keys_->pop_back(kv);
          if (NULL != kv.pcv_set_) {
            kv.pcv_set_->dec_ref_count(ref_handle_);
          }
          if (OB_FAIL(to_evict_keys_->push_back(pc_kv))) {
            SQL_PC_LOG(WARN, "fail to push into evict_array", K(ret));
          } else {
            pc_kv.pcv_set_->inc_ref_count(ref_handle_);
          }
        }
      }
    }
    // construct max-heap, the most active entry is on top
    // std::push_heap arrange the last element, make evict_array max-heap
    if (OB_SUCC(ret)) {
      std::push_heap(to_evict_keys_->begin(), to_evict_keys_->end(), stat_compare);
    }

    return ret;
  }

  int64_t evict_num_;
  PCKeyValueArray* to_evict_keys_;
  const CacheRefHandleID ref_handle_;
};

ObPlanCache::ObPlanCache()
    : inited_(false),
      valid_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      mem_limit_pct_(OB_PLAN_CACHE_PERCENTAGE),
      mem_high_pct_(OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE),
      mem_low_pct_(OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE),
      mem_used_(0),
      bucket_num_(0),
      inner_allocator_(),
      location_cache_(NULL),
      plan_id_(0),
      ref_count_(0),
      ref_handle_mgr_()
{}

ObPlanCache::~ObPlanCache()
{
  destroy();
}

void ObPlanCache::destroy()
{
  if (inited_) {
    if (OB_SUCCESS != (cache_evict_all_plan())) {
      SQL_PC_LOG(WARN, "fail to evict all plan cache cache");
    }
    if (OB_SUCCESS != (cache_evict_all_pl())) {
      SQL_PC_LOG(WARN, "fail to evict all pl cache");
    }
    inited_ = false;
  }
}

int ObPlanCache::init(
    int64_t hash_bucket, common::ObAddr addr, share::ObIPartitionLocationCache* location_cache, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (OB_FAIL(sql_pcvs_map_.create(hash::cal_next_prime(hash_bucket),
            ObModIds::OB_HASH_BUCKET_PLAN_CACHE,
            ObModIds::OB_HASH_NODE_PLAN_CACHE,
            tenant_id))) {
      SQL_PC_LOG(WARN, "failed to init PlanCache", K(ret));
    } else if (OB_FAIL(plan_stat_map_.create(hash::cal_next_prime(hash_bucket),
                   ObModIds::OB_HASH_BUCKET_PLAN_STAT,
                   ObModIds::OB_HASH_NODE_PLAN_STAT,
                   tenant_id))) {
      SQL_PC_LOG(WARN, "failed to init PlanStat", K(ret));
    } else if (OB_FAIL(deleted_map_.create(hash::cal_next_prime(hash_bucket),
                   ObModIds::OB_HASH_BUCKET_PLAN_STAT,
                   ObModIds::OB_HASH_NODE_PLAN_STAT,
                   tenant_id))) {
      SQL_PC_LOG(WARN, "failed to init Deleted Map", K(ret));
    } else {
      ObMemAttr attr = get_mem_attr();
      attr.tenant_id_ = tenant_id;
      inner_allocator_.set_attr(attr);
      set_location_cache(location_cache);
      set_host(addr);
      bucket_num_ = hash::cal_next_prime(hash_bucket);
      tenant_id_ = tenant_id;
      ref_handle_mgr_.set_tenant_id(tenant_id_);
      inited_ = true;
      valid_ = true;
    }
  }

  return ret;
}

int ObPlanCache::get_cache_obj(ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj)
{
  int ret = OB_SUCCESS;
  ObPCVSet* pcv_set = NULL;
  // get the read lock and increase reference count
  ObPlanCacheRlockAndRef r_ref_lock(PCV_RD_HANDLE);

  if (OB_FAIL(get_value(pc_ctx.fp_result_.pc_key_, pcv_set, r_ref_lock /* read locked */))) {
    SQL_PC_LOG(DEBUG, "failed to access plan cache", K(pc_ctx.fp_result_.pc_key_), K(ret));
  } else if (OB_UNLIKELY(NULL == pcv_set)) {
    ret = OB_SQL_PC_NOT_EXIST;
    SQL_PC_LOG(DEBUG, "physical plan does not exist!", K(pc_ctx.fp_result_.pc_key_));
  } else {
    LOG_DEBUG("inner_get_plan", K(pc_ctx.fp_result_.pc_key_), K(pcv_set));
    pcv_set->update_stmt_stat();
    if (OB_FAIL(pcv_set->get_plan(pc_ctx, cache_obj))) {
      if (OB_OLD_SCHEMA_VERSION != ret && OB_SQL_PC_NOT_EXIST != ret) {
        LOG_WARN("pcv_set fail to get plan", K(ret));
      }
    } else {
      LOG_DEBUG("succ to choose a physical plan", K(pc_ctx.raw_sql_));
    }

    ObPhysicalPlan* plan = NULL;
    if (cache_obj != NULL && cache_obj->is_sql_crsr()) {
      plan = static_cast<ObPhysicalPlan*>(cache_obj);
    }
    // if schema expired, update pcv set;
    if (OB_OLD_SCHEMA_VERSION == ret || (plan != NULL && plan->is_expired())) {
      if (plan != NULL && plan->is_expired()) {
        LOG_INFO("the statistics of table is stale and evict plan.", K(plan->stat_));
      }
      if (OB_FAIL(remove_pcv_set(pc_ctx.fp_result_.pc_key_))) {
        LOG_WARN("fail to remove pcv set when schema/plan expired", K(ret));
      } else {
        ret = OB_SQL_PC_NOT_EXIST;
      }
    }
    // release lock whatever
    (void)pcv_set->unlock();
    (void)pcv_set->dec_ref_count(PCV_RD_HANDLE);

    NG_TRACE(pc_choose_plan);
  }

  return ret;
}

// 1.fast parser gets param sql and raw params
// 2.get pcv set with param sql
// 3.check privilege
int ObPlanCache::get_plan(
    const CacheRefHandleID ref_handle, common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  ObCacheObject* cache_obj = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();

  pc_ctx.handle_id_ = ref_handle;
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid arguement", K(ret), K(pc_ctx.sql_ctx_.schema_guard_), K(pc_ctx.exec_ctx_.get_physical_plan_ctx()));
  } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
    if (OB_FAIL(construct_multi_stmt_fast_parser_result(allocator, pc_ctx))) {
      if (OB_BATCHED_MULTI_STMT_ROLLBACK != ret) {
        LOG_WARN("failed to construct multi stmt fast parser", K(ret));
      }
    } else {
      pc_ctx.fp_result_ = pc_ctx.multi_stmt_fp_results_.at(0);
    }
  } else {
    if (OB_FAIL(construct_fast_parser_result(allocator, pc_ctx, pc_ctx.raw_sql_, pc_ctx.fp_result_))) {
      LOG_WARN("failed to construct fast parser results", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_cache_obj(pc_ctx, cache_obj))) {
      SQL_PC_LOG(DEBUG, "fail to get plan", K(ret));
    } else if (OB_ISNULL(cache_obj) || !cache_obj->is_sql_crsr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache obj is invalid", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    plan = static_cast<ObPhysicalPlan*>(cache_obj);
    MEMCPY(pc_ctx.sql_ctx_.sql_id_, plan->stat_.bl_info_.sql_id_.ptr(), plan->stat_.bl_info_.sql_id_.length());
    uint64_t tenant_id = pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id();
    bool read_only = false;
    if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
      LOG_WARN("failed to check read_only privilege", K(ret));
    } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->check_read_only_privilege(read_only, pc_ctx.sql_traits_))) {
      LOG_WARN("failed to check read_only privilege", K(ret));
    }
  }

  if (OB_FAIL(ret) && cache_obj != NULL) {
    ObCacheObjectFactory::free(cache_obj, pc_ctx.handle_id_);
    cache_obj = NULL;
    plan = NULL;
  }
  return ret;
}

int ObPlanCache::construct_multi_stmt_fast_parser_result(common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObString>* queries = NULL;
  if (OB_ISNULL(queries = pc_ctx.sql_ctx_.multi_stmt_item_.get_queries())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(queries), K(ret));
  } else if (OB_FAIL(pc_ctx.multi_stmt_fp_results_.reserve(queries->count()))) {
    LOG_WARN("failed to reserve array space", K(ret));
  } else {
    ObFastParserResult parser_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < queries->count(); i++) {
      parser_result.reset();
      if (OB_FAIL(construct_fast_parser_result(allocator, pc_ctx, queries->at(i), parser_result))) {
        LOG_WARN("failed to construct fast parser result", K(ret));
      } else if (OB_FAIL(pc_ctx.multi_stmt_fp_results_.push_back(parser_result))) {
        LOG_WARN("failed to push back parser result", K(ret));
      } else if (i > 0 &&
                 ((parser_result.pc_key_.name_ != pc_ctx.multi_stmt_fp_results_.at(0).pc_key_.name_) ||
                     (parser_result.raw_params_.count() != pc_ctx.multi_stmt_fp_results_.at(0).raw_params_.count()))) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        if (REACH_TIME_INTERVAL(10000000)) {
          LOG_INFO("batched multi_stmt needs rollback",
              K(parser_result.pc_key_),
              K(pc_ctx.multi_stmt_fp_results_.at(0).pc_key_),
              K(ret));
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObPlanCache::construct_fast_parser_result(common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx,
    const common::ObString& raw_sql, ObFastParserResult& fp_result)

{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSQLMode sql_mode = pc_ctx.sql_ctx_.session_info_->get_sql_mode();
    ObCollationType conn_coll = pc_ctx.sql_ctx_.session_info_->get_local_collation_connection();
    fp_result.cache_params_ = &(pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update());
    if (OB_FAIL(construct_plan_cache_key(*pc_ctx.sql_ctx_.session_info_, NS_CRSR, fp_result.pc_key_))) {
      LOG_WARN("failed to construct plan cache key", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::fast_parser(
                   allocator, sql_mode, conn_coll, raw_sql, pc_ctx.sql_ctx_.handle_batched_multi_stmt(), fp_result))) {
      LOG_WARN("failed to fast parser", K(ret), K(sql_mode), K(pc_ctx.raw_sql_));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObPlanCache::get_pl_cache(ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj)
{
  int ret = OB_SUCCESS;
  cache_obj = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(pc_ctx.sql_ctx_.session_info_), K(pc_ctx.sql_ctx_.schema_guard_), K(ret));
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, pc_ctx.fp_result_.pc_key_.namespace_))) {
    LOG_WARN("construct plan cache key failed", K(ret));
  } else if (OB_FAIL(get_cache_obj(pc_ctx, cache_obj))) {
    SQL_PC_LOG(DEBUG, "fail to get plan", K(ret));
  } else if (OB_ISNULL(cache_obj) ||
             (!cache_obj->is_prcr() && !cache_obj->is_sfc() && !cache_obj->is_pkg() && !cache_obj->is_anon())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is invalid", KPC(cache_obj));
  } else { /*do nothing*/
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    // TODO PL PC_CTX
    ObCacheObjectFactory::free(cache_obj, pc_ctx.handle_id_);
    cache_obj = NULL;
  }
  if (OB_SUCC(ret) && cache_obj != NULL) {
    inc_hit_and_access_cnt();
  } else {
    inc_access_cnt();
  }
  return ret;
}

// 1. check memory limit
// 2. add plan
// 3. add plan stat
int ObPlanCache::add_plan(ObPhysicalPlan* plan, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else if (is_reach_memory_limit()) {
    ret = OB_REACH_MEMORY_LIMIT;
    if (REACH_TIME_INTERVAL(1000000)) {
      SQL_PC_LOG(
          ERROR, "plan cache memory used reach limit", K_(tenant_id), K(get_mem_hold()), K(get_mem_limit()), K(ret));
    }
  } else if (plan->get_mem_size() >= get_mem_high()) {
    // plan mem is too big, do not add plan
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, NS_CRSR))) {
    LOG_WARN("construct plan cache key failed", K(ret));
  } else {
    if (OB_FAIL(add_cache_obj(plan, pc_ctx))) {
      if (!is_not_supported_err(ret) && OB_SQL_PC_PLAN_DUPLICATE != ret && OB_PC_LOCK_CONFLICT != ret) {
        SQL_PC_LOG(WARN, "fail to add plan", K(ret));
      }
    } else {
      (void)inc_mem_used(plan->get_mem_size());
    }
  }
  return ret;
}

int ObPlanCache::add_exists_pcv_set_by_sql(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx)
{
  // 1. get pcv_set with id
  // 2. insert <sql, pcv_se get last time> into map
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  UNUSED(cache_obj);
  ObPlanCacheWlockAndRef w_ref_lock(PCV_WR_HANDLE);
  ObPCVSet* pcv_set = NULL;
  uint64_t old_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
  ObString sql = pc_ctx.fp_result_.pc_key_.name_;
  pc_ctx.fp_result_.pc_key_.name_.reset();
  if (OB_INVALID_ID == old_stmt_id) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid stmt_id", K(ret), K(old_stmt_id));
  } else if (OB_ISNULL(sql.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid sql", K(ret), K(sql));
  } else if (OB_FAIL(get_value(pc_ctx.fp_result_.pc_key_, pcv_set, w_ref_lock))) {
    SQL_PC_LOG(WARN, "get pcv_set failed", K(ret), K(pc_ctx.fp_result_.pc_key_));
  } else if (OB_ISNULL(pcv_set)) {
    ret = OB_SUCCESS;
    SQL_PC_LOG(DEBUG, "pcv_set is NULL, may be removed by another thread", K(ret), K(pc_ctx.fp_result_.pc_key_));
  } else if (OB_ISNULL(pcv_set->get_sql().ptr())) {
    ret = OB_SUCCESS;
    SQL_PC_LOG(DEBUG, "sql is NULL while adding sql key into plan cache, ignore this", K(ret));
  } else {
    ObPlanCacheKey pc_key(pcv_set->get_sql(),  // name_
        OB_INVALID_ID,                         // key_id_
        pcv_set->get_plan_cache_key().db_id_,
        pcv_set->get_plan_cache_key().sessid_,
        true, /* is ps mode*/
        pcv_set->get_plan_cache_key().sys_vars_str_,
        pcv_set->get_plan_cache_key().namespace_);
    pcv_set->inc_ref_count(PCV_SET_HANDLE);
    int hash_err = sql_pcvs_map_.set_refactored(pc_key, pcv_set);
    if (OB_HASH_EXIST == hash_err) {
      pcv_set->dec_ref_count(PCV_SET_HANDLE);
      SQL_PC_LOG(DEBUG, "kv pair may be set by another thread", K(ret), K(pc_key));
    } else if (OB_SUCCESS == hash_err) {
      SQL_PC_LOG(DEBUG, "sql in key is set succeed", K(ret), K(pc_key), K(sql_pcvs_map_.size()));
    } else {
      pcv_set->dec_ref_count(PCV_SET_HANDLE);
      SQL_PC_LOG(WARN, "set pcv_set using sql as key into plan cache failed", K(ret), K(pc_key));
    }
    pcv_set->unlock();
    pcv_set->dec_ref_count(PCV_WR_HANDLE);
  }

  // reset pc_key
  pc_ctx.fp_result_.pc_key_.name_ = sql;
  pc_ctx.fp_result_.pc_key_.key_id_ = old_stmt_id;
  return ret;
}

int ObPlanCache::add_exists_pcv_set_by_new_stmt_id(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  ObPlanCacheWlockAndRef w_ref_lock(PCV_WR_HANDLE);
  ObPCVSet* pcv_set = NULL;
  uint64_t new_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
  ObString sql = pc_ctx.fp_result_.pc_key_.name_;

  // get pcv_set with sql as key
  pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;

  if (OB_INVALID_ID == new_stmt_id) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid stmt_id", K(ret), K(new_stmt_id));
  } else if (OB_ISNULL(sql.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid sql", K(ret), K(sql));
  } else if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "cache_obj is NULL", K(ret));
  } else if (OB_FAIL(get_value(pc_ctx.fp_result_.pc_key_, pcv_set, w_ref_lock))) {
    SQL_PC_LOG(WARN, "get pcv_set failed", K(ret), K(pc_ctx.fp_result_.pc_key_));
  } else if (OB_ISNULL(pcv_set)) {
    ret = OB_SUCCESS;
    SQL_PC_LOG(DEBUG, "pcv_set is NULL, may be removed by another thread", K(ret), K(pc_ctx.fp_result_.pc_key_));
  } else {
    // insert pcv_set into map with new_stmt_id as key
    ObPlanCacheKey pc_key(ObString(),  // name_
        new_stmt_id,                   // key_id_
        pcv_set->get_plan_cache_key().db_id_,
        pcv_set->get_plan_cache_key().sessid_,
        true, /* is ps mode*/
        pcv_set->get_plan_cache_key().sys_vars_str_,
        pcv_set->get_plan_cache_key().namespace_);
    pcv_set->inc_ref_count(PCV_SET_HANDLE);
    int hash_err = sql_pcvs_map_.set_refactored(pc_key, pcv_set);
    if (OB_HASH_EXIST == hash_err) {
      pcv_set->dec_ref_count(PCV_SET_HANDLE);
      SQL_PC_LOG(DEBUG, "kv pair may be set by another thread", K(ret), K(pc_key));
    } else if (OB_SUCCESS == hash_err) {
      SQL_PC_LOG(DEBUG, "pcv_set uisng new_stmt_id as key is set into plan cache succeed", K(ret), K(pc_key));
      if (cache_obj->is_sql_crsr()) {
        ObPhysicalPlan* plan = dynamic_cast<ObPhysicalPlan*>(cache_obj);
        if (OB_ISNULL(plan)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_PC_LOG(WARN, "convert cache_obj to ObPhysicalPlan failed", K(ret));
        } else {
          SQL_PC_LOG(DEBUG, "ps_stmt_id changed", K(plan->stat_.ps_stmt_id_), K(new_stmt_id));
          plan->stat_.ps_stmt_id_ = new_stmt_id;
        }
      }
    } else {
      pcv_set->dec_ref_count(PCV_SET_HANDLE);
      SQL_PC_LOG(WARN,
          "pcv_set uisng new_stmt_id as key is set into plan cache failed",
          K(ret),
          K(pc_ctx.fp_result_.pc_key_),
          K(new_stmt_id));
    }

    pcv_set->unlock();
    pcv_set->dec_ref_count(PCV_WR_HANDLE);
  }
  // reset pc_key
  pc_ctx.fp_result_.pc_key_.name_ = sql;
  pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
  return ret;
}

/* need to lock add plan stat, otherwise there may be memory leak of plan object. A case:

   thread A                            thread B

   get write lock for pcv_set

   add_cache_obj

   release write lock for pcv_set

                                       get write lock for pcv_set

   add plan_stat starting

                                       remove pcv_set (plan_stat_map for plan id not exists, but pcv_set is removed)

   add plan_stat done

   Thread A execute sequencely an then insert a record into plan_stat_map. plan_stat_map records <plan_id, plan
   pointer>. Each plan pointer add ref count of the plan. record in plan_stat_map is deleted when remove_pcv_set. But in
   this case, pcv_set has been deleted by thread B, so there is a record in plan_cache_stat won't be deleted, and memory
   of according plan leak.
 */
int ObPlanCache::add_cache_obj(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else {
    ObPlanCacheWlockAndRef w_ref_lock(PCV_WR_HANDLE);
    ObPCVSet* pcv_set = NULL;
    if (OB_FAIL(get_value(pc_ctx.fp_result_.pc_key_, pcv_set, w_ref_lock /*  write locked */))) {
      SQL_PC_LOG(DEBUG, "failed to get pcv_set from plan cache by key", K(ret));
    } else if (NULL == pcv_set) { /* did not get pcv_set, create one */
      // create pcv_set, init pcvset, deep copy new_key, and add plan
      if (OB_FAIL(create_pcv_set_and_add_plan(cache_obj, pc_ctx, pcv_set))) {  // pcv_set ref count+1
        if (!is_not_supported_err(ret)) {
          SQL_PC_LOG(WARN, "fail to create pcv_set and add plan", K(ret));
        }
      } else {
        // do nothing
      }
      // set key value
      if (OB_SUCC(ret)) {
        /*
         * add ref_count before add pcv_set, otherwise pointer of pcv_set may become wild pointer.
         *
         *            Thread A                                      Background Thread
         *     set pcv_set  (ref_count = 1)
         *
         *                                                   get_pcv_set key (ref count = 2)
         *
         *                                              remove pcv set and dec ref (ref_count =1)
         *
         *                                                   evict done, dec_ref (ref_count = 0)
         *
         *                                                      destroy pcv set
         *          add_plan_stat
         *
         *      pcv_set->unlock  (core...)
         *
         */
        pcv_set->inc_ref_count(PCV_SET_HANDLE);  // inc ref count in block
        int hash_err = sql_pcvs_map_.set_refactored(pcv_set->get_plan_cache_key(), pcv_set);
        if (OB_HASH_EXIST == hash_err) {  // may be this pcv_set has been set by other thread.
          pcv_set->unlock();
          pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in block
          pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in alloc
          if (OB_FAIL(add_cache_obj(cache_obj, pc_ctx))) {
            SQL_PC_LOG(TRACE, "fail to add plan", K(ret), K(cache_obj));
          }
        } else if (OB_SUCCESS == hash_err) {
          SQL_PC_LOG(DEBUG, "succeed to set pcv_set to sql_pcvs_map");
          /* must add stat after set_refactored successfully, otherwise :
           *              Thread A                                            Thread B
           *     create_pcv_set_and_add_plan
           *
           *         add_cache_obj_stat
           *                                                          create_pcv_set_and_add_plan
           *
           *                                                               add_cache_obj_stat
           *
           *                                                        set_refactored(pcv_set) succeed
           *
           *    set_refactored(pcv_set) failed
           *
           *   pcv_set->dec_ref_count(), mark plan as deleted
           *
           *          add_cache_obj(plan)
           * Problem is that after plan is deleted logically and assigned log_del_time, thread x
           * will free this plan, even this plan is still stored in plan cache.
           *
           */
          if (OB_FAIL(add_stat_for_cache_obj(pc_ctx, cache_obj))) {
            LOG_WARN("failed to add stat", K(ret));
            ObPCVSet* del_pcvset = NULL;
            int tmp_ret = sql_pcvs_map_.erase_refactored(pcv_set->get_plan_cache_key(), &del_pcvset);
            if (OB_UNLIKELY(tmp_ret != OB_SUCCESS) || OB_UNLIKELY(del_pcvset != pcv_set)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret), K(tmp_ret), K(del_pcvset), K(pcv_set));
            } else {
              pcv_set->unlock();
              pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in block
              pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in alloc
            }
          } else {
            // unlock here is to make sure add cache obj and add cache obj stat is an atomic operation.
            pcv_set->unlock();
            pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in block
          }
        } else {
          SQL_PC_LOG(TRACE, "failed to add pcv_set to sql_pcvs_map", K(ret), KPC(cache_obj));
          pcv_set->unlock();
          pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in block
          pcv_set->dec_ref_count(PCV_SET_HANDLE);  // pcv set dec ref in alloc
        }
      }
    } else { /* value exist, add plan to it */
      LOG_TRACE("inner_add_plan", K(pc_ctx.fp_result_.pc_key_), K(pcv_set));
      if (OB_FAIL(pcv_set->add_cache_obj(cache_obj, pc_ctx))) {
        SQL_PC_LOG(DEBUG, "failed to add plan to plan cache value", K(ret));
      } else {
        pcv_set->update_stmt_stat();
      }
      bool is_cache_obj_stat_added = false;
      // If version of table or view is alreay invalid when add plan, delete that pcv_set.
      if (OB_OLD_SCHEMA_VERSION == ret) {
        SQL_PC_LOG(INFO, "table or view in plan cache value is old", K(ret));
        if (OB_FAIL(remove_pcv_set(pc_ctx.fp_result_.pc_key_))) {
          SQL_PC_LOG(WARN, "fail to remove plan cache value", K(ret));
        } else if (OB_FAIL(add_cache_obj(cache_obj, pc_ctx))) {
          SQL_PC_LOG(DEBUG, "fail to add plan", K(ret), K(cache_obj));
        } else {
          // pcv_set is newly created, and cache obj stat is already added
          is_cache_obj_stat_added = true;
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (!is_cache_obj_stat_added && OB_FAIL(add_stat_for_cache_obj(pc_ctx, cache_obj))) {
        LOG_WARN("failed to add stat", K(ret), K(pc_ctx));
      } else {
        // do nothing
      }
      // release wlock whatever
      pcv_set->unlock();
      pcv_set->dec_ref_count(PCV_WR_HANDLE);
    }
  }
  return ret;
}

// get pcv from key->pcv map with plan cache key
int ObPlanCache::get_value(const ObPlanCacheKey key, ObPCVSet*& pcv_set, ObPlanCacheAtomicOp& op)
{
  int ret = OB_SUCCESS;
  // get pcv and inc ref count
  int hash_err = sql_pcvs_map_.read_atomic(key, op);
  switch (hash_err) {
    case OB_SUCCESS: {
      // get pcv and lock
      if (OB_FAIL(op.get_value(pcv_set))) {
        SQL_PC_LOG(DEBUG, "failed to lock pcv set", K(ret), K(key));
      }
      break;
    }
    case OB_HASH_NOT_EXIST: {
      SQL_PC_LOG(DEBUG, "entry does not exist.", K(key));
      break;
    }
    default: {
      SQL_PC_LOG(WARN, "failed to get pcv set", K(ret), K(key));
      ret = hash_err;
      break;
    }
  }
  return ret;
}

int ObPlanCache::cache_evict_all_plan()
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  SQL_PC_LOG(DEBUG, "cache evict all plan start");
  PCKeyValueArray to_evict_keys;
  ObGetAllSqlIdOp get_ids_op(&to_evict_keys, PCV_GET_PLAN_KEY_HANDLE);
  if (OB_FAIL(sql_pcvs_map_.foreach_refactored(get_ids_op))) {
    SQL_PC_LOG(WARN, "fail to get all sql_ids in id2value_map", K(ret));
  } else if (OB_FAIL(remove_pcv_sets(to_evict_keys))) {
    SQL_PC_LOG(WARN, "fail to remove pcv set", K(ret));
  }
  int64_t N = to_evict_keys.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (NULL != to_evict_keys.at(i).pcv_set_) {
      to_evict_keys.at(i).pcv_set_->dec_ref_count(PCV_GET_PLAN_KEY_HANDLE);
    }
  }

  SQL_PC_LOG(DEBUG, "cache evict all plan end");
  return ret;
}

// delete all pl cache obj
int ObPlanCache::cache_evict_all_pl()
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  SQL_PC_LOG(DEBUG, "cache evict all pl cache start");
  PCKeyValueArray to_evict_keys;
  ObGetAllPLIdOp get_ids_op(&to_evict_keys, PCV_GET_PL_KEY_HANDLE);
  if (OB_FAIL(sql_pcvs_map_.foreach_refactored(get_ids_op))) {  // inc each pcv_set's ref_count
    LOG_WARN("failed to get all sql_ids in map", K(ret));
  } else if (OB_FAIL(remove_pcv_sets(to_evict_keys))) {
    LOG_WARN("failed to remove pcv sest", K(ret));
  }

  // dec each pcv_set's ref_count
  int64_t N = to_evict_keys.count();
  for (int64_t i = 0; i < N; i++) {
    if (NULL != to_evict_keys.at(i).pcv_set_) {
      to_evict_keys.at(i).pcv_set_->dec_ref_count(PCV_GET_PL_KEY_HANDLE);
    }
  }

  SQL_PC_LOG(DEBUG, "cache evict all pl end", K(to_evict_keys));
  return ret;
}

// 1. calc evict_num : (mem_used - mem_lwm) / (mem_used / cache_value_count)
// 2. get evict_sql_id from calc_cache_evict_keys
// 3. evict value
// 4. evict stmtkey
/* maybe thread_A erase value --> thread_B get id --> thread_A erase stmtkey-->thread_B alloc new value
 * now stmtkey->id is not exist, the new value while be death,
 * but when this id->value weigth is lower enough, the value will be drop;
 *
 *        thread_A                thread_B
 *
 *      erase id->value
 *           |
 *           |               get id from stmtkey->id
 *           |                         |
 *     erase stmtkey->id               |
 *                                     |
 *                           cann't get value, create new value
 * */

int ObPlanCache::cache_evict()
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  SQL_PC_LOG(INFO,
      "plan cache evict info",
      K_(tenant_id),
      "mem_hold",
      get_mem_hold(),
      "mem_limit",
      get_mem_limit(),
      "plan_num",
      get_plan_num(),
      "pcv_set_num",
      sql_pcvs_map_.size());
  (void)evict_expired_plan();
  if (get_mem_hold() > get_mem_high()) {
    int64_t plan_cache_evict_num = 0;
    if (calc_evict_num(plan_cache_evict_num)) {
      PCKeyValueArray to_evict;
      if (OB_FAIL(calc_evict_keys(plan_cache_evict_num, to_evict))) {
        SQL_PC_LOG(WARN, "failed to get evict array", K(ret), K(plan_cache_evict_num));
      } else if (OB_FAIL(remove_pcv_sets(to_evict))) {
        SQL_PC_LOG(WARN, "failed to remove pcv_set", K(ret));
      } else {
        SQL_PC_LOG(INFO, "EVICT INFO", K(to_evict.count()));
      }
      int64_t N = to_evict.count();
      for (int64_t i = 0; i < N; i++) {
        if (NULL != to_evict.at(i).pcv_set_) {
          to_evict.at(i).pcv_set_->dec_ref_count(PCV_EXPIRE_BY_MEM_HANDLE);
        }
      }
    }
  }
  return ret;
}

int ObPlanCache::evict_expired_plan()
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (GCTX.inited_) {
    int64_t merged_version = *(GCTX.merged_version_);
    PCKeyValueArray to_evict;
    ObGetExpiredKeyOp op(merged_version, &to_evict, PCV_EXPIRE_BY_USED_HANDLE);
    if (OB_FAIL(sql_pcvs_map_.foreach_refactored(op))) {
      SQL_PC_LOG(WARN, "fail to calculate cache evict sql id");
    } else if (OB_FAIL(remove_pcv_sets(to_evict))) {
      SQL_PC_LOG(WARN, "failed to remove cache value", K(to_evict), K(ret));
    } else {
      SQL_PC_LOG(INFO, "evict expired plan", K(to_evict.count()), K(merged_version));
    }
    int64_t N = to_evict.count();
    for (int64_t i = 0; i < N; i++) {
      if (NULL != to_evict.at(i).pcv_set_) {
        to_evict.at(i).pcv_set_->dec_ref_count(PCV_EXPIRE_BY_USED_HANDLE);
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

bool ObPlanCache::calc_evict_num(int64_t& plan_cache_evict_num)
{
  bool ret = true;
  int64_t pc_hold = get_mem_hold();
  int64_t mem_to_free = pc_hold - get_mem_low();
  if (mem_to_free <= 0) {
    ret = false;
  }

  if (ret) {
    if (pc_hold > 0) {
      plan_cache_evict_num = (int64_t)(((double)mem_to_free / (double)pc_hold) * (double)(sql_pcvs_map_.size()));
    } else {
      plan_cache_evict_num = 0;
    }
  }

  return ret;
}

int ObPlanCache::remove_pcv_sets(ObIArray<PCKeyValue>& to_evict)
{
  int ret = OB_SUCCESS;
  int64_t N = to_evict.count();
  SQL_PC_LOG(INFO, "actual evict number", "evict_value_num", to_evict.count());
  // evict value, continue when encounter error
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    if (OB_FAIL(remove_pcv_set(to_evict.at(i).key_))) {
      SQL_PC_LOG(WARN, "failed to remove plan from plan cache", K(ret));
    }
  }

  return ret;
}

/* revoe pcv set according to plan cache key
 *               k->v (ref_count = 2)
 *                   |
 *                 erase(k, e_v)
 *                   |
 *      ---------- exist?--------
 *      |no                       | yes
 *       |                         |
 * v.dec_ref_count()         e_v.dec_ref_count()
 *                           v.dec_ref_count()
 *
 *1.v and e_v are usually same obj.
 *2.v and e_v are different obj in this case: thread A and B do delete and thread C do get:
 *  thread A         thread B                     thread C
 *                 get k,v already
 *
 * erase successfully
 *                                            get with k failed, add(k,v') successfully
 *
 *                 erase(k,v') successfully
 *                 ref_count of v -1
 *                 ref_count of v' -1
 */
int ObPlanCache::remove_pcv_set(const ObPlanCacheKey& key)
{
  int ret = OB_SUCCESS;
  int hash_err = OB_SUCCESS;
  ObPCVSet* pcv_set = NULL;
  hash_err = sql_pcvs_map_.erase_refactored(key, &pcv_set);
  if (OB_SUCCESS == hash_err) {
    if (NULL != pcv_set) {
      // remove plan cache reference, even remove_plan_stat() failed
      pcv_set->dec_ref_count(PCV_SET_HANDLE);
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(ERROR, "pcv_set should not be null", K(key));
    }
  } else if (OB_HASH_NOT_EXIST == hash_err) {
    SQL_PC_LOG(INFO, "plan cache key is alreay be deleted", K(key));
  } else {
    ret = hash_err;
    SQL_PC_LOG(WARN, "failed to erase pcv_set from plan cache by key", K(key), K(hash_err));
  }

  return ret;
}

int ObPlanCache::calc_evict_keys(int64_t evict_num, PCKeyValueArray& to_evict_keys)
{
  int ret = OB_SUCCESS;
  if (evict_num > 0) {
    ObStatFilterOp filter(evict_num, &to_evict_keys, PCV_EXPIRE_BY_MEM_HANDLE);
    if (OB_FAIL(sql_pcvs_map_.foreach_refactored(filter))) {
      SQL_PC_LOG(WARN, "fail to calculate cache evict sql id", K(ret));
    }
  }

  return ret;
}

int64_t ObPlanCache::inc_ref_count()
{
  int64_t ret = 0;
  ret = ATOMIC_AAF((uint64_t*)&ref_count_, 1);
  return ret;
}

void ObPlanCache::dec_ref_count()
{
  int64_t ref_count = ATOMIC_SAF((uint64_t*)&ref_count_, 1);
  if (ref_count > 0) {
  } else if (0 == ref_count) {
    // delete
    this->~ObPlanCache();
  } else if (ref_count < 0) {
    BACKTRACE(ERROR, true, "Plan Cache %p ref count < 0, ref_count = %ld", this, ref_count);
  }
}

int ObPlanCache::ref_cache_obj(const ObCacheObjID obj_id, const CacheRefHandleID ref_handle, ObCacheObject*& cache_obj)
{
  int ret = OB_SUCCESS;
  ObCacheObjAtomicOp op(ref_handle);
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_FAIL(plan_stat_map_.atomic_refactored(obj_id, op))) {
    SQL_PC_LOG(WARN, "failed to get update plan statistic", K(obj_id), K(ret));
  } else {
    cache_obj = op.get_value();
  }
  return ret;
}

int ObPlanCache::ref_plan(const ObCacheObjID plan_id, const CacheRefHandleID ref_handle, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  ObCacheObject* cache_obj = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_FAIL(ref_cache_obj(plan_id, ref_handle, cache_obj))) {  // inc ref count by 1
    LOG_WARN("failed to ref cache obj", K(ret));
  } else if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cache object", K(ret));
  } else if (!cache_obj->is_sql_crsr()) {
    // dec ref count
    if (NULL != cache_obj) {
      ObCacheObjectFactory::free(cache_obj, ref_handle);
      cache_obj = NULL;
    }
  } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan*>(cache_obj))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret), K(plan), K(plan_id), K(cache_obj->get_type()));
  }

  if (OB_FAIL(ret)) {
    if (NULL != cache_obj) {
      ObCacheObjectFactory::free(cache_obj, ref_handle);
    }
  }

  return ret;
}

int ObPlanCache::add_cache_obj_stat(ObPlanCacheCtx& pc_ctx, ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* plan = NULL;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(cache_obj), K(ret));
  } else if (cache_obj->is_sql_crsr()) {
    plan = dynamic_cast<ObPhysicalPlan*>(cache_obj);
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan", K(ret), K(plan));
    } else {
      plan->stat_.plan_id_ = plan->get_plan_id();
      plan->stat_.bl_info_.plan_hash_value_ = plan->get_signature();
      plan->stat_.gen_time_ = ObTimeUtility::current_time();
      plan->stat_.schema_version_ = plan->get_tenant_schema_version();
      plan->stat_.merged_version_ = plan->get_merged_version();
      plan->stat_.last_active_time_ = plan->stat_.gen_time_;
      plan->stat_.hit_count_ = 0;
      plan->stat_.mem_used_ = plan->get_mem_size();
      plan->stat_.slow_count_ = 0;
      plan->stat_.slowest_exec_time_ = 0;
      plan->stat_.slowest_exec_usec_ = 0;
      if (pc_ctx.is_ps_mode_) {
        plan->stat_.stmt_len_ = pc_ctx.raw_sql_.length();
      } else {
        plan->stat_.stmt_len_ = pc_ctx.bl_key_.constructed_sql_.length();
      }
      if (plan->stat_.stmt_len_ >= ObPlanStat::STMT_MAX_LEN) {
        plan->stat_.stmt_len_ = ObPlanStat::STMT_MAX_LEN;
      }
      if (pc_ctx.is_ps_mode_) {
        MEMCPY(plan->stat_.stmt_, pc_ctx.raw_sql_.ptr(), plan->stat_.stmt_len_);
        plan->stat_.ps_stmt_id_ = pc_ctx.fp_result_.pc_key_.key_id_;
      } else {
        MEMCPY(plan->stat_.stmt_, pc_ctx.bl_key_.constructed_sql_.ptr(), plan->stat_.stmt_len_);
      }
      MEMCPY(plan->stat_.sql_id_, pc_ctx.sql_ctx_.sql_id_, (int32_t)sizeof(pc_ctx.sql_ctx_.sql_id_));
      plan->stat_.large_querys_ = 0;
      plan->stat_.delayed_large_querys_ = 0;
      plan->stat_.delayed_px_querys_ = 0;
      plan->stat_.outline_version_ = plan->get_outline_state().outline_version_.version_;
      plan->stat_.outline_id_ = plan->get_outline_state().outline_version_.object_id_;
      plan->inc_ref_count(PC_REF_PLAN_STAT_HANDLE);

      ObTruncatedString trunc_raw_sql(pc_ctx.raw_sql_, OB_MAX_SQL_LENGTH);
      if (OB_FAIL(pc_ctx.get_not_param_info_str(plan->get_allocator(), plan->stat_.sp_info_str_))) {
        SQL_PC_LOG(WARN, "fail to get special param info string", K(ret));
      } else if (OB_FAIL(ob_write_string(
                     plan->get_allocator(), pc_ctx.fp_result_.pc_key_.sys_vars_str_, plan->stat_.sys_vars_str_))) {
        SQL_PC_LOG(DEBUG, "succeed to add plan statistic", "plan_id", plan->get_plan_id(), K(ret));
      } else if (OB_FAIL(plan->init_params_info_str())) {
        SQL_PC_LOG(DEBUG, "fail to gen param info str", K(ret));
      } else if (OB_FAIL(ob_write_string(plan->get_allocator(), trunc_raw_sql.string(), plan->stat_.raw_sql_))) {
        SQL_PC_LOG(DEBUG, "fail to copy raw sql", "plan_id", plan->get_plan_id(), K(ret));
      } else if (OB_FAIL(plan_stat_map_.set_refactored(plan->get_plan_id(), plan))) {
        SQL_PC_LOG(WARN, "fail to set plan stat", K(ret), K(plan->stat_), K(plan));
      } else {
        // do nothing
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (plan->get_access_table_num() > 0) {
      if (OB_ISNULL(plan->get_table_row_count_first_exec() = static_cast<ObTableRowCount*>(
                        plan->get_allocator().alloc(plan->get_access_table_num() * sizeof(ObTableRowCount))))) {
        LOG_WARN("allocate memory for table row count list failed", K(plan->get_access_table_num()));
      } else {
        for (int64_t i = 0; i < plan->get_access_table_num(); ++i) {
          plan->get_table_row_count_first_exec()[i].op_id_ = OB_INVALID_ID;
          plan->get_table_row_count_first_exec()[i].row_count_ = -1;
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (pc_ctx.tmp_table_names_.count() > 0) {
      LOG_DEBUG("set tmp table name str", K(pc_ctx.tmp_table_names_));
      plan->stat_.sessid_ = pc_ctx.sql_ctx_.session_info_->get_sessid();

      int64_t pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < pc_ctx.tmp_table_names_.count(); i++) {
        if (OB_ISNULL(plan->stat_.plan_tmp_tbl_name_str_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_DEBUG("null plan tmp tbl id str", K(ret));
        } else if (OB_FAIL(databuff_printf(plan->stat_.plan_tmp_tbl_name_str_,
                       ObPlanStat::STMT_MAX_LEN,
                       pos,
                       "%.*s%s",
                       pc_ctx.tmp_table_names_.at(i).length(),
                       pc_ctx.tmp_table_names_.at(i).ptr(),
                       ((i == pc_ctx.tmp_table_names_.count() - 1) ? "" : ", ")))) {
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to write plan tmp tbl name info", K(pc_ctx.tmp_table_names_.at(i)), K(i), K(ret));
          }
        } else {
          // do nothing
        }
      }
      if (OB_SUCC(ret)) {
        plan->stat_.plan_tmp_tbl_name_str_[pos] = '\0';
        pos += 1;
        plan->stat_.plan_tmp_tbl_name_str_len_ = static_cast<int32_t>(pos);
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }

    if (OB_FAIL(ret) && plan != NULL) {
      SQL_PC_LOG(WARN, "failed to add plan statistic", "plan_id", plan->get_plan_id(), K(ret));
      ObCacheObject* del_obj = NULL;
      (void)plan_stat_map_.erase_refactored(plan->get_object_id(), &del_obj);
      UNUSED(del_obj);
      ObCacheObjectFactory::free(plan, PC_REF_PLAN_STAT_HANDLE);
      plan = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG(
        "succeeded to add cache object stat", K(cache_obj->get_object_id()), K(cache_obj->added_pc()), K(cache_obj));
  }

  return ret;
}

int ObPlanCache::remove_cache_obj_stat_entry(const ObCacheObjID cache_obj_id)
{
  int ret = OB_SUCCESS;
  ObCacheObject* cache_obj = NULL;
  if (OB_FAIL(plan_stat_map_.erase_refactored(cache_obj_id, &cache_obj))) {
    SQL_PC_LOG(WARN, "failed to erase plan statistic entry", K(ret), K(cache_obj_id));
  } else {
    SQL_PC_LOG(DEBUG, "succeed to remove plan statistic", K(cache_obj_id), K(ret));
    if (NULL != cache_obj) {
      // set logical deleted time
      cache_obj->set_logical_del_time(common::ObTimeUtility::current_monotonic_time());
      LOG_DEBUG("set logical del time",
          K(cache_obj->get_logical_del_time()),
          K(cache_obj->get_object_id()),
          K(cache_obj->added_pc()),
          K(cache_obj));
      ObCacheObjectFactory::free(cache_obj, cache_obj->is_sql_crsr() ? PC_REF_PLAN_STAT_HANDLE : PC_REF_PL_STAT_HANDLE);
      cache_obj = NULL;
    }
  }

  return ret;
}

int ObPlanCache::create_pcv_set_and_add_plan(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx, ObPCVSet*& pcv_set)
{
  int ret = OB_SUCCESS;
  pcv_set = NULL;
  char* ptr = NULL;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (NULL == (ptr = (char*)inner_allocator_.alloc(sizeof(ObPCVSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pcv set", K(ret));
  } else {
    pcv_set = new (ptr) ObPCVSet(this);
    pcv_set->inc_ref_count(PCV_SET_HANDLE);
    pcv_set->lock(true);
    if (OB_FAIL(pcv_set->init(pc_ctx, cache_obj))) {
      LOG_WARN("fail to init pcv set", K(ret));
    }
  }

  // add plan
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pcv_set->add_cache_obj(cache_obj, pc_ctx))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "failed to add plan to plan cache value", K(ret));
      }
    } else {
      pcv_set->update_stmt_stat();
    }
  }

  if (OB_FAIL(ret) && NULL != pcv_set) {
    // dec_ref_count to zero, will destroy itself
    pcv_set->dec_ref_count(PCV_SET_HANDLE);
    pcv_set = NULL;
  }

  return ret;
}

int ObPlanCache::set_mem_conf(const ObPCMemPctConf& conf)
{
  int ret = OB_SUCCESS;

  if (conf.limit_pct_ != get_mem_limit_pct()) {
    set_mem_limit_pct(conf.limit_pct_);
    LOG_INFO("update ob_plan_cache_percentage", "new value", conf.limit_pct_, "old value", get_mem_limit_pct());
  }
  if (conf.high_pct_ != get_mem_high_pct()) {
    set_mem_high_pct(conf.high_pct_);
    LOG_INFO(
        "update ob_plan_cache_evict_high_percentage", "new value", conf.high_pct_, "old value", get_mem_high_pct());
  }
  if (conf.low_pct_ != get_mem_low_pct()) {
    set_mem_low_pct(conf.low_pct_);
    LOG_INFO("update ob_plan_cache_evict_low_percentage", "new value", conf.low_pct_, "old value", get_mem_low_pct());
  }

  return ret;
}

int ObPlanCache::update_memory_conf()
{
  int ret = OB_SUCCESS;
  ObPCMemPctConf pc_mem_conf;
  const char* conf_names[3] = {
      "ob_plan_cache_percentage", "ob_plan_cache_evict_low_percentage", "ob_plan_cache_evict_high_percentage"};
  int64_t* conf_values[3] = {&pc_mem_conf.limit_pct_, &pc_mem_conf.low_pct_, &pc_mem_conf.high_pct_};
  ObArenaAllocator alloc;
  ObObj obj_val;

  if (tenant_id_ > OB_SYS_TENANT_ID && tenant_id_ <= OB_MAX_RESERVED_TENANT_ID) {
    // tenant id between (OB_SYS_TENANT_ID, OB_MAX_RESERVED_TENANT_ID) is a virtual tenant,
    // virtual tenants do not have schema
    // do nothing
  } else {
    for (int32_t i = 0; i < 3 && OB_SUCC(ret); ++i) {
      if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(
              tenant_id_, alloc, ObDataTypeCastParams(), ObString(conf_names[i]), obj_val))) {
      } else if (OB_FAIL(obj_val.get_int(*conf_values[i]))) {
        LOG_WARN("failed to get int", K(ret), K(obj_val));
      } else if (*conf_values[i] < 0 || *conf_values[i] > 100) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of plan cache conf", K(obj_val));
      }
    }  // end for

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_mem_conf(pc_mem_conf))) {
        SQL_PC_LOG(WARN, "fail to update plan cache mem conf", K(ret));
      }
    }
  }

  LOG_INFO("update plan cache memory config",
      "ob_plan_cache_percentage",
      pc_mem_conf.limit_pct_,
      "ob_plan_cache_evict_high_percentage",
      pc_mem_conf.high_pct_,
      "ob_plan_cache_evict_low_percentage",
      pc_mem_conf.low_pct_,
      "tenant_id",
      tenant_id_);

  return ret;
}

int64_t ObPlanCache::get_mem_hold() const
{
  return get_tenant_memory_hold(tenant_id_, ObCtxIds::PLAN_CACHE_CTX_ID);
}

int64_t ObPlanCache::get_mod_hold(int mod_id) const
{
  ObModItem item;
  get_tenant_mod_memory(tenant_id_, mod_id, item);
  return item.hold_;
}

// add plan in plan cache
// 1.check whether there is enough memory for this plan
// 2.get pcv with plan cache key:
//     if get pcv successfully: add plan in pcv
//     if get pcv failed: generate new pcv; add the plan in pcv; then add the pcv in key->pcv map.
template <class T>
int ObPlanCache::add_ps_plan(T* plan, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else if (is_reach_memory_limit()) {
    ret = OB_REACH_MEMORY_LIMIT;
    SQL_PC_LOG(DEBUG, "plan cache memory used reach the high water mark", K(mem_used_), K(get_mem_limit()), K(ret));
  } else if (plan->get_mem_size() >= get_mem_high()) {
    // plan mem is too big to reach memory highwater, do not add plan
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, NS_CRSR))) {
    LOG_WARN("fail to construnct plan cache key", K(ret));
  } else if (OB_FAIL(add_cache_obj(plan, pc_ctx))) {
    if (OB_FAIL(deal_add_ps_plan_result(ret, pc_ctx, *plan))) {
      LOG_WARN("fail to deal result code", K(ret));
    }
  } else {
    (void)inc_mem_used(plan->get_mem_size());
  }

  if (OB_SUCC(ret) && !pc_ctx.sql_ctx_.is_remote_sql_) {
    // remote sql use PS interface cache plan, but it differs from simple ps protocol.
    // remote sql does not use stmt_id, so we don't construct a map between stmt and sql here.
    if (OB_ISNULL(pc_ctx.raw_sql_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(WARN, "pc_ctx.raw_sql_.ptr() is NULL, cannot add plan to plan cache by sql", K(ret));
    } else {
      pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
      SQL_PC_LOG(DEBUG, "start to add ps plan by sql", K(pc_ctx.fp_result_.pc_key_));
      if (OB_FAIL(add_exists_pcv_set_by_sql(plan, pc_ctx))) {
        SQL_PC_LOG(WARN, "add_exists_pcv_set_by_sql failed", K(ret), K(pc_ctx.fp_result_.pc_key_));
      }
      // reset pc_ctx
      pc_ctx.fp_result_.pc_key_.name_.reset();
    }
  }
  return ret;
}

template int ObPlanCache::add_ps_plan<ObPhysicalPlan>(ObPhysicalPlan* plan, ObPlanCacheCtx& pc_ctx);

int ObPlanCache::deal_add_ps_plan_result(int add_plan_ret, ObPlanCacheCtx& pc_ctx, const ObCacheObject& cache_object)
{
  int ret = add_plan_ret;
  if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
    ret = OB_SUCCESS;
    LOG_DEBUG("this plan has been added by others, need not add again", K(cache_object));
  } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
    if (REACH_TIME_INTERVAL(1000000)) {
      ObTruncatedString trunc_sql(pc_ctx.raw_sql_);
      LOG_INFO("can't add plan to plan cache", K(ret), K(cache_object.get_mem_size()), K(trunc_sql), K(get_mem_used()));
    }
    ret = OB_SUCCESS;
  } else if (is_not_supported_err(ret)) {
    ret = OB_SUCCESS;
    LOG_DEBUG("plan cache don't support add this kind of plan now", K(cache_object));
  } else if (OB_FAIL(ret)) {
    if (OB_REACH_MAX_CONCURRENT_NUM != ret) {
      ret = OB_SUCCESS;
      LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
    }
  } else {
    pc_ctx.sql_ctx_.self_add_plan_ = true;
    LOG_DEBUG("Successed to add plan to ObPlanCache", K(cache_object));
  }

  return ret;
}

template <class T>
int ObPlanCache::get_ps_plan(
    const CacheRefHandleID ref_handle, const ObPsStmtId stmt_id, ObPlanCacheCtx& pc_ctx, T*& plan)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  UNUSED(stmt_id);
  ObSqlTraits sql_traits;
  ObCacheObject* cache_obj = NULL;
  pc_ctx.handle_id_ = ref_handle;
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_) ||
      OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN,
        "invalid argument",
        K(pc_ctx.sql_ctx_.session_info_),
        K(pc_ctx.sql_ctx_.schema_guard_),
        K(pc_ctx.exec_ctx_.get_physical_plan_ctx()),
        K(ret));
  } else if (FALSE_IT(pc_ctx.fp_result_.cache_params_ =
                          &(pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update()))) {
    // do nothing
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, NS_CRSR))) {
    LOG_WARN("fail to construnct plan cache key", K(ret));
  } else if (OB_FAIL(get_cache_obj(pc_ctx, cache_obj))) {
    SQL_PC_LOG(DEBUG, "fail to get plan", K(ret));
  } else if (OB_ISNULL(cache_obj) ||
             OB_UNLIKELY(!cache_obj->is_sql_crsr() && !cache_obj->is_prcr() && !cache_obj->is_sfc() &&
                         !cache_obj->is_pkg() && !cache_obj->is_anon())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is invalid", K(ret), KPC(cache_obj));
  } else {
    plan = static_cast<T*>(cache_obj);
  }

  if (OB_SQL_PC_NOT_EXIST == ret && !pc_ctx.sql_ctx_.is_remote_sql_) {
    // remote sql use PS interface cache plan, but it differs from simple ps protocol.
    // remote sql does not use stmt_id, so we don't construct a map between stmt and sql here.
    ObPsStmtId new_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;
    pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
    SQL_PC_LOG(
        DEBUG, "start to get plan by sql", K(new_stmt_id), K(pc_ctx.fp_result_.pc_key_), K(sql_pcvs_map_.size()));
    if (OB_FAIL(get_cache_obj(pc_ctx, cache_obj))) {
      if (OB_OLD_SCHEMA_VERSION == ret) {
        SQL_PC_LOG(DEBUG,
            "get cache obj by sql failed because of old_schema_version",
            K(ret),
            K(pc_ctx.raw_sql_),
            K(new_stmt_id),
            K(sql_pcvs_map_.size()));
      } else {
        SQL_PC_LOG(
            WARN, "get cache obj by sql failed", K(ret), K(pc_ctx.raw_sql_), K(new_stmt_id), K(sql_pcvs_map_.size()));
      }
    } else {
      pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
      pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
      SQL_PC_LOG(DEBUG,
          "get cache obj by sql succeed, will add kv pair by new stmt_id",
          K(new_stmt_id),
          K(pc_ctx.fp_result_.pc_key_));
      if (OB_FAIL(add_exists_pcv_set_by_new_stmt_id(cache_obj, pc_ctx))) {
        SQL_PC_LOG(WARN, "add cache obj by new stmt_id failed", K(ret), K(stmt_id));
      } else {
        SQL_PC_LOG(DEBUG, "add_exists_pcv_set succeed", K(pc_ctx.fp_result_.pc_key_));
        plan = static_cast<T*>(cache_obj);
      }
    }
    // reset pc_ctx
    pc_ctx.fp_result_.pc_key_.name_.reset();
    pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
  }

  if (OB_SUCC(ret)) {
    if (cache_obj->is_sql_crsr()) {
      ObPhysicalPlan* sql_plan = static_cast<ObPhysicalPlan*>(cache_obj);
      MEMCPY(
          pc_ctx.sql_ctx_.sql_id_, sql_plan->stat_.bl_info_.sql_id_.ptr(), sql_plan->stat_.bl_info_.sql_id_.length());
    }
  }

  // check read only privilege
  if (OB_SUCC(ret) && !GCONF.enable_perf_event) {
    uint64_t tenant_id = pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id();
    bool read_only = false;
    if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
      SQL_PC_LOG(WARN, "fail to get tenant read only attribute", K(tenant_id), K(ret));
    } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->check_read_only_privilege(read_only, sql_traits))) {
      SQL_PC_LOG(WARN, "failed to check read_only privilege", K(ret));
    }
  }
  if (OB_FAIL(ret) && cache_obj != NULL) {
    LOG_DEBUG("cache object got", K(cache_obj->get_object_id()));
    ObCacheObjectFactory::free(cache_obj, pc_ctx.handle_id_);
    cache_obj = NULL;
    plan = NULL;
  }
  return ret;
}

template int ObPlanCache::get_ps_plan<ObPhysicalPlan>(
    const CacheRefHandleID ref_handle, const ObPsStmtId stmt_id, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan);

int ObPlanCache::construct_plan_cache_key(ObPlanCacheCtx& plan_ctx, ObjNameSpace ns)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = plan_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (OB_FAIL(construct_plan_cache_key(*session, ns, plan_ctx.fp_result_.pc_key_))) {
    LOG_WARN("failed to construct plan cache key", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPlanCache::construct_plan_cache_key(ObSQLSessionInfo& session, ObjNameSpace ns, ObPlanCacheKey& pc_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.get_database_id(pc_key.db_id_))) {
    LOG_WARN("get database id failed", K(ret));
  } else {
    pc_key.namespace_ = ns;
    pc_key.sys_vars_str_ = session.get_sys_var_in_pc_str();
  }
  return ret;
}

int ObPlanCache::add_stat_for_cache_obj(ObPlanCacheCtx& pc_ctx, ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cache_obj));
  } else if (!pc_ctx.need_real_add_) {
    // do nothing
  } else if (OB_FAIL(add_cache_obj_stat(pc_ctx, cache_obj))) {
    LOG_WARN("failed to add cache obj stat", K(ret));
  }
  return ret;
}

int ObPlanCache::dump_all_objs() const
{
  int ret = OB_SUCCESS;
  ObArray<DeletedCacheObjInfo> deleted_objs;
  ObGetAllDeletedCacheObjIdsOp get_all_objs_op(&deleted_objs, true, INT64_MAX, DUMP_ALL);

  if (OB_FAIL(deleted_map_.foreach_refactored(get_all_objs_op))) {
    LOG_WARN("failed to traverse objs in deleted map", K(ret));
  } else {
    LOG_INFO("Dumping All Cache Objs", K(deleted_objs.count()), K(deleted_objs));
  }
  return ret;
}

template <DumpType dump_type>
int ObPlanCache::dump_deleted_objs(ObIArray<DeletedCacheObjInfo>& deleted_objs, const int64_t safe_timestamp) const
{
  int ret = OB_SUCCESS;
  ObGetAllDeletedCacheObjIdsOp get_deleted_objs_op(&deleted_objs, false, safe_timestamp, dump_type);
  if (OB_FAIL(deleted_map_.foreach_refactored(get_deleted_objs_op))) {
    LOG_WARN("failed to traverse objs in deleted map", K(ret));
  } else { /* do nothing */
  }
  return ret;
}

template int ObPlanCache::dump_deleted_objs<DUMP_PL>(ObIArray<DeletedCacheObjInfo>&, const int64_t) const;
template int ObPlanCache::dump_deleted_objs<DUMP_SQL>(ObIArray<DeletedCacheObjInfo>&, const int64_t) const;
template int ObPlanCache::dump_deleted_objs<DUMP_ALL>(ObIArray<DeletedCacheObjInfo>&, const int64_t) const;

int ObGetAllDeletedCacheObjIdsOp::operator()(common::hash::HashMapPair<uint64_t, ObCacheObject*>& entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_array_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "key array not inited", K(ret));
  } else if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "unexpected null entry.second", K(ret));
  } else if (dump_all_) {
    if (OB_FAIL(key_array_->push_back(DeletedCacheObjInfo(entry.second->get_object_id(),
            entry.second->get_tenant_id(),
            entry.second->get_logical_del_time(),
            safe_timestamp_,
            entry.second->get_ref_count(),
            entry.second->get_allocator().used(),
            entry.second->added_pc())))) {
      LOG_WARN("failed to push back element", K(ret));
    }
  } else if (entry.second->should_release(safe_timestamp_) && should_dump(entry.second->get_type()) &&
             OB_FAIL(key_array_->push_back(DeletedCacheObjInfo(entry.second->get_object_id(),
                 entry.second->get_tenant_id(),
                 entry.second->get_logical_del_time(),
                 safe_timestamp_,
                 entry.second->get_ref_count(),
                 entry.second->get_allocator().used(),
                 entry.second->added_pc())))) {
    SQL_PC_LOG(WARN, "failed to push back element", K(ret));
  } else { /* do nothing */
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase

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
#include "lib/rc/ob_rc.h"
#include "observer/ob_server_struct.h"
#include "sql/plan_cache/ob_ps_cache_callback.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/udr/ob_udr_mgr.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "observer/ob_req_time_service.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share::schema;
using namespace oceanbase::lib;
using namespace oceanbase::pl;
using namespace oceanbase::observer;
namespace oceanbase
{
namespace sql
{
struct ObGetPlanIdBySqlIdOp
{
  explicit ObGetPlanIdBySqlIdOp(common::ObIArray<uint64_t> *key_array,
                                const common::ObString &sql_id,
                                const bool with_plan_hash,
                                const uint64_t &plan_hash_value)
    : key_array_(key_array), sql_id_(sql_id), with_plan_hash_(with_plan_hash), plan_hash_value_(plan_hash_value)
  {
  }
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
  {
    int ret = common::OB_SUCCESS;
    ObPhysicalPlan *plan = NULL;
    if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
      ret = common::OB_NOT_INIT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (ObLibCacheNameSpace::NS_CRSR != entry.second->get_ns()) {
      // not sql plan
      // do nothing
    } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan *>(entry.second))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan", K(ret), K(plan));
    } else if (sql_id_ != plan->stat_.sql_id_) {
      // do nothing
    } else if (with_plan_hash_ && plan->stat_.plan_hash_value_ != plan_hash_value_) {
      // do nothing
    } else if (OB_FAIL(key_array_->push_back(entry.first))) {
      SQL_PC_LOG(WARN, "fail to push back plan_id", K(ret));
    }

    return ret;
  }

  common::ObIArray<uint64_t> *key_array_;
  common::ObString sql_id_;
  bool with_plan_hash_;
  uint64_t plan_hash_value_;
};

struct ObGetKVEntryByNsOp : public ObKVEntryTraverseOp
{
  explicit ObGetKVEntryByNsOp(const ObLibCacheNameSpace ns,
                              LCKeyValueArray *key_val_list,
                              const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      namespace_(ns)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (namespace_ == entry.first->namespace_) {
      is_match = true;
    }
    return ret;
  }

  ObLibCacheNameSpace namespace_;
};

struct ObGetKVEntryBySQLIDOp : public ObKVEntryTraverseOp
{
  explicit ObGetKVEntryBySQLIDOp(uint64_t db_id,
                                 common::ObString sql_id,
                                 LCKeyValueArray *key_val_list,
                                 const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      db_id_(db_id),
      sql_id_(sql_id)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = OB_SUCCESS;
    is_match = false;
    if (entry.first->namespace_ == ObLibCacheNameSpace::NS_CRSR) {
      ObPlanCacheKey *key = static_cast<ObPlanCacheKey*>(entry.first);
      ObPCVSet *node = static_cast<ObPCVSet*>(entry.second);
      if (db_id_ != common::OB_INVALID_ID && db_id_ != key->db_id_) {
        // skip entry that has non-matched db_id
      } else if (!contain_sql_id(node->get_sql_id())) {
        // skip entry which not contains same sql_id
      } else {
        is_match = true;
      }
    }
    return ret;
  }
  bool contain_sql_id(common::ObIArray<common::ObString> &sql_ids)
  {
    bool contains = false;
    for (int64_t i = 0; !contains && i < sql_ids.count(); i++) {
      if (sql_ids.at(i) == sql_id_) {
        contains = true;
      }
    }
    return contains;
  }

  uint64_t db_id_;
  common::ObString sql_id_;
};


struct ObGetPcvSetByTabNameOp : public ObKVEntryTraverseOp
{
  explicit ObGetPcvSetByTabNameOp(uint64_t db_id, common::ObString tab_name,
                                  LCKeyValueArray *key_val_list,
                                  const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      db_id_(db_id),
      tab_name_(tab_name)
  {
  }
  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    int ret = common::OB_SUCCESS;
    is_match = false;
    if (entry.first->namespace_ >= ObLibCacheNameSpace::NS_CRSR
        && entry.first->namespace_ <= ObLibCacheNameSpace::NS_PKG) {
      ObPlanCacheKey *key = static_cast<ObPlanCacheKey*>(entry.first);
      ObPCVSet *node = static_cast<ObPCVSet*>(entry.second);
      if (db_id_ == common::OB_INVALID_ID) {
        // do nothing
      } else if (db_id_ != key->db_id_) {
        // skip entry that has non-matched db_id
      } else if (OB_FAIL(node->check_contains_table(db_id_, tab_name_, is_match))) {
        LOG_WARN("fail to check table name", K(ret), K(db_id_), K(tab_name_));
      } else {
        // do nothing
      }
    }
    return ret;
  }
  uint64_t db_id_;
  common::ObString tab_name_;
};


struct ObGetTableIdOp
{
  explicit ObGetTableIdOp(uint64_t table_id)
    : table_id_(table_id)
  {}

  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
  {
    int ret = common::OB_SUCCESS;
    ObPhysicalPlan *plan = NULL;
    int64_t version = -1;
    if (OB_ISNULL(entry.second)) {
      ret = common::OB_NOT_INIT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (ObLibCacheNameSpace::NS_CRSR != entry.second->get_ns()) {
      // not sql plan
      // do nothing
    } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan *>(entry.second))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan", K(ret), K(plan));
    } else if (plan->get_base_table_version(table_id_, version)) {
      LOG_WARN("failed to get base table version", K(ret));
    } else if (version > 0) {
      plan->set_is_expired(true);
    }
    return ret;
  }
  const uint64_t table_id_;
};

// true means entry_left is more active than entry_right
bool stat_compare(const LCKeyValue &left, const LCKeyValue &right)
{
  bool cmp_ret = false;
  if (OB_ISNULL(left.node_) || OB_ISNULL(right.node_)) {
    cmp_ret = false;
    SQL_PC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", KP(left.node_), KP(right.node_), K(cmp_ret));
  } else if (OB_ISNULL(left.node_->get_node_stat())
             || OB_ISNULL(right.node_->get_node_stat())) {
    cmp_ret = false;
    SQL_PC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(left.node_->get_node_stat()),
                      K(right.node_->get_node_stat()), K(cmp_ret));
  } else {
    cmp_ret = left.node_->get_node_stat()->weight() < right.node_->get_node_stat()->weight();
  }
  return cmp_ret;
}

// filter entries satisfy stat_condition
// get 'evict_num' number of sql_ids which weight is lower;
struct ObNodeStatFilterOp : public ObKVEntryTraverseOp
{
  ObNodeStatFilterOp(int64_t evict_num,
                     LCKeyValueArray *key_val_list,
                     const CacheRefHandleID ref_handle)
    : ObKVEntryTraverseOp(key_val_list, ref_handle),
      evict_num_(evict_num)
  {
  }
  virtual int operator()(LibCacheKVEntry &entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(key_value_list_) || OB_ISNULL(entry.first) || OB_ISNULL(entry.second)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument",
      K(key_value_list_), K(entry.first), K(entry.second), K(ret));
    } else {
      ObLCKeyValue lc_kv(entry.first, entry.second);
      if (key_value_list_->count() < evict_num_) {
        if (OB_FAIL(key_value_list_->push_back(lc_kv))) {
           SQL_PC_LOG(WARN, "fail to push into evict_array", K(ret));
        } else {
          lc_kv.node_->inc_ref_count(ref_handle_);
        }
      } else {
        // replace one element
        if (stat_compare(lc_kv, key_value_list_->at(0))) {
          // pop_heap move the largest value to last
          std::pop_heap(key_value_list_->begin(), key_value_list_->end(), stat_compare);
          // remove and replace the largest value
          LCKeyValue kv;
          key_value_list_->pop_back(kv);
          if (NULL != kv.node_) {
            kv.node_->dec_ref_count(ref_handle_);
          }
          if (OB_FAIL(key_value_list_->push_back(lc_kv))) {
            SQL_PC_LOG(WARN, "fail to push into evict_array", K(ret));
          } else {
            lc_kv.node_->inc_ref_count(ref_handle_);
          }
        }
      }
      // construct max-heap, the most active entry is on top
      // std::push_heap arrange the last element, make evict_array max-heap
      if (OB_SUCC(ret)) {
        std::push_heap(key_value_list_->begin(), key_value_list_->end(), stat_compare);
      }
    }
    return ret;
  }

  int64_t evict_num_;
};

ObPlanCache::ObPlanCache()
  :inited_(false),
   tenant_id_(OB_INVALID_TENANT_ID),
   mem_limit_pct_(OB_PLAN_CACHE_PERCENTAGE),
   mem_high_pct_(OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE),
   mem_low_pct_(OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE),
   mem_used_(0),
   bucket_num_(0),
   inner_allocator_(),
   ref_handle_mgr_(),
   pcm_(NULL),
   destroy_(0),
   tg_id_(-1)
{
}

ObPlanCache::~ObPlanCache()
{
  destroy();
}

void ObPlanCache::destroy()
{
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (inited_) {
    TG_DESTROY(tg_id_);
    if (OB_SUCCESS != (cache_evict_all_obj())) {
      SQL_PC_LOG_RET(WARN, OB_ERROR, "fail to evict all lib cache cache");
    }
    inited_ = false;
  }
}

int ObPlanCache::init(int64_t hash_bucket, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ObPCMemPctConf default_conf;
    if (OB_FAIL(co_mgr_.init(hash_bucket, tenant_id))) {
      SQL_PC_LOG(WARN, "failed to init lib cache manager", K(ret));
    } else if (OB_FAIL(cache_key_node_map_.create(hash::cal_next_prime(hash_bucket),
                                                  ObModIds::OB_HASH_BUCKET_PLAN_CACHE,
                                                  ObModIds::OB_HASH_NODE_PLAN_CACHE,
                                                  tenant_id))) {
      SQL_PC_LOG(WARN, "failed to init PlanCache", K(ret));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::PlanCacheEvict, tg_id_))) {
      LOG_WARN("failed to create tg", K(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("failed to start tg", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, evict_task_, GCONF.plan_cache_evict_interval, true))) {
      LOG_WARN("failed to schedule refresh task", K(ret));
    } else if (OB_FAIL(set_mem_conf(default_conf))) {
      LOG_WARN("fail to set plan cache memory conf", K(ret));
    } else {
      evict_task_.plan_cache_ = this;
      cn_factory_.set_lib_cache(this);
      ObMemAttr attr = get_mem_attr();
      attr.tenant_id_ = tenant_id;
      inner_allocator_.set_attr(attr);
      set_host(const_cast<ObAddr &>(GCTX.self_addr()));
      bucket_num_ = hash::cal_next_prime(hash_bucket);
      tenant_id_ = tenant_id;
      ref_handle_mgr_.set_tenant_id(tenant_id_);
      inited_ = true;
    }
  }
  return ret;
}

int ObPlanCache::check_after_get_plan(int tmp_ret,
                                      ObILibCacheCtx &ctx,
                                      ObILibCacheObject *cache_obj)
{
  int ret = tmp_ret;
  ObPhysicalPlan *plan = NULL;
  bool enable_udr = false;
  bool need_late_compilation = false;
  ObJITEnableMode jit_mode = ObJITEnableMode::OFF;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  if (tenant_config.is_valid()) {
    enable_udr = tenant_config->enable_user_defined_rewrite_rules;
  }
  if (cache_obj != NULL && ObLibCacheNameSpace::NS_CRSR == cache_obj->get_ns()) {
    plan = static_cast<ObPhysicalPlan *>(cache_obj);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null session info", K(ret));
    } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->get_jit_enabled_mode(jit_mode))) {
      LOG_WARN("failed to get jit mode");
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret) && plan != NULL) {
    bool is_exists = false;
    sql::ObUDRMgr *rule_mgr = MTL(sql::ObUDRMgr*);
    // when the global rule version changes or enable_user_defined_rewrite_rules changes
    // it is necessary to check whether the physical plan are expired
    if ((plan->get_rule_version() != rule_mgr->get_rule_version()
      || plan->is_enable_udr() != enable_udr)) {
      if (OB_FAIL(rule_mgr->fuzzy_check_by_pattern_digest(pc_ctx.get_normalized_pattern_digest(), is_exists))) {
        LOG_WARN("failed to fuzzy check by pattern digest", K(ret));
      } else if (is_exists || plan->is_rewrite_sql()) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_TRACE("Obsolete user-defined rewrite rules require eviction plan", K(ret),
        K(is_exists), K(pc_ctx.raw_sql_), K(plan->is_enable_udr()), K(enable_udr),
        K(plan->is_rewrite_sql()), K(plan->get_rule_version()), K(rule_mgr->get_rule_version()));
      } else {
        plan->set_rule_version(rule_mgr->get_rule_version());
        plan->set_is_enable_udr(enable_udr);
      }
    }
    if (OB_SUCC(ret)) {
      if (ObJITEnableMode::AUTO == jit_mode && // only use late compilation when jit_mode is auto
        OB_FAIL(need_late_compile(plan, need_late_compilation))) {
        LOG_WARN("failed to check for late compilation", K(ret));
      } else {
        // set context's need_late_compile_ for upper layer to proceed
        pc_ctx.sql_ctx_.need_late_compile_ = need_late_compilation;
      }
    }
  }
  // if schema expired, update pcv set;
  if (OB_OLD_SCHEMA_VERSION == ret
    || (plan != NULL && plan->is_expired())
    || need_late_compilation) {
    if (plan != NULL && plan->is_expired()) {
      LOG_INFO("the statistics of table is stale and evict plan.", K(plan->stat_));
    }
    if (OB_FAIL(remove_cache_node(pc_ctx.key_))) {
      LOG_WARN("fail to remove pcv set when schema/plan expired", K(ret));
    } else {
      ret = OB_SQL_PC_NOT_EXIST;
    }
  }
  return ret;
}

//plan cache获取plan入口
//1.fast parser获取param sql及raw params
//2.根据param sql获得pcv set
//3.检查权限信息
int ObPlanCache::get_plan(common::ObIAllocator &allocator,
                          ObPlanCacheCtx &pc_ctx,
                          ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;

  FLTSpanGuard(pc_get_plan);
  ObGlobalReqTimeService::check_req_timeinfo();
  pc_ctx.handle_id_ = guard.ref_handle_;
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)
      || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement",K(ret),
             K(pc_ctx.sql_ctx_.schema_guard_), K(pc_ctx.exec_ctx_.get_physical_plan_ctx()));
  } else if (pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
    if (OB_FAIL(construct_multi_stmt_fast_parser_result(allocator,
                                                        pc_ctx))) {
      if (OB_BATCHED_MULTI_STMT_ROLLBACK != ret) {
        LOG_WARN("failed to construct multi stmt fast parser", K(ret));
      }
    } else {
      pc_ctx.fp_result_ = pc_ctx.multi_stmt_fp_results_.at(0);
    }
  } else if (OB_FAIL(construct_fast_parser_result(allocator,
                                                  pc_ctx,
                                                  pc_ctx.raw_sql_,
                                                  pc_ctx.fp_result_ ))) {
    LOG_WARN("failed to construct fast parser results", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_plan_cache(pc_ctx, guard))) {
      SQL_PC_LOG(DEBUG, "failed to get plan", K(ret));
    } else if (OB_ISNULL(guard.cache_obj_)
      || ObLibCacheNameSpace::NS_CRSR != guard.cache_obj_->get_ns()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache obj is invalid", K(ret));
    } else {
      ObPhysicalPlan* plan = NULL;
      if (OB_ISNULL(plan = static_cast<ObPhysicalPlan*>(guard.cache_obj_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast cache obj to physical plan.", K(ret));
      } else {
        MEMCPY(pc_ctx.sql_ctx_.sql_id_,
               plan->stat_.sql_id_.ptr(),
               plan->stat_.sql_id_.length());
        if (GCONF.enable_perf_event) {
          uint64_t tenant_id = pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id();
          bool read_only = false;
          if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
            LOG_WARN("failed to check read_only privilege", K(ret));
          } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->check_read_only_privilege(
                             read_only, pc_ctx.sql_traits_))) {
            LOG_WARN("failed to check read_only privilege", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(guard.cache_obj_)) {
    co_mgr_.free(guard.cache_obj_, guard.ref_handle_);
    guard.cache_obj_ = NULL;
  }
  return ret;
}

int ObPlanCache::construct_multi_stmt_fast_parser_result(common::ObIAllocator &allocator,
                                                         ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObString> *queries = NULL;
  bool enable_explain_batched_multi_statement = ObSQLUtils::is_enable_explain_batched_multi_statement();
  if (OB_ISNULL(queries = pc_ctx.sql_ctx_.multi_stmt_item_.get_queries())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(queries), K(ret));
  } else if (OB_FAIL(pc_ctx.multi_stmt_fp_results_.reserve(queries->count()))) {
    LOG_WARN("failed to reserve array space", K(ret));
  } else {
    ObFastParserResult parser_result;
    ObFastParserResult trimmed_parser_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < queries->count(); i++) {
      parser_result.reset();
      if (OB_FAIL(construct_fast_parser_result(allocator,
                                               pc_ctx,
                                               queries->at(i),
                                               parser_result))) {
        LOG_WARN("failed to construct fast parser result", K(ret));
      } else if (OB_FAIL(pc_ctx.multi_stmt_fp_results_.push_back(parser_result))) {
        LOG_WARN("failed to push back parser result", K(ret));
      } else if (i == 0 && enable_explain_batched_multi_statement) {
        const char *p_normal_start = nullptr;
        ObString trimmed_query;
        if (ObParser::is_explain_stmt(queries->at(0), p_normal_start)) {
          trimmed_query = queries->at(0).after(p_normal_start - 1);
          if (OB_FAIL(construct_fast_parser_result(allocator,
                                                   pc_ctx,
                                                   trimmed_query,
                                                   trimmed_parser_result))) {
            LOG_WARN("failed to construct fast parser result", K(ret));
          }
        }
      } else if (i > 0 ) {
        // Example        query: 'explain update t1 set b=0 where a=1;explain update t1 set b=0 where a=2;'
        // Original first query: 'explain update t1 set b=0 where a=1'
        // Trimmed  first query: 'update t1 set b=0 where a=1'
        //
        // If we compare the second query (e.g. 'explain update t1 set b=0 where a=2') with the original
        // first query, the check will pass, but we will batch run one 'update' statement with one
        // 'explain update' statement which makes no sense. Thus, we should use the trimmed first query instead.
        ObFastParserResult &first_parser_result = enable_explain_batched_multi_statement ?
                trimmed_parser_result : pc_ctx.multi_stmt_fp_results_.at(0);
        if (parser_result.pc_key_.name_ != first_parser_result.pc_key_.name_
            || parser_result.raw_params_.count() != first_parser_result.raw_params_.count()) {
          ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          if (REACH_TIME_INTERVAL(10000000)) {
            LOG_INFO("batched multi_stmt needs rollback",
                     K(parser_result.pc_key_),
                     K(parser_result.raw_params_.count()),
                     K(first_parser_result.pc_key_),
                     K(first_parser_result.raw_params_.count()),
                     K(ret), K(lbt()));
          }
        }
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPlanCache::construct_fast_parser_result(common::ObIAllocator &allocator,
                                              ObPlanCacheCtx &pc_ctx,
                                              const common::ObString &raw_sql,
                                              ObFastParserResult &fp_result)

{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) ||
      OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSQLMode sql_mode = pc_ctx.sql_ctx_.session_info_->get_sql_mode();
    ObCollationType conn_coll = pc_ctx.sql_ctx_.session_info_->get_local_collation_connection();
    // if exact mode is on, not needs to do fast parser
    bool enable_exact_mode = pc_ctx.sql_ctx_.session_info_->get_enable_exact_mode();
    fp_result.cache_params_ =
        &(pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update());
    if (OB_FAIL(construct_plan_cache_key(*pc_ctx.sql_ctx_.session_info_,
                                         ObLibCacheNameSpace::NS_CRSR,
                                         fp_result.pc_key_))) {
      LOG_WARN("failed to construct plan cache key", K(ret));
    } else if (enable_exact_mode) {
      (void)fp_result.pc_key_.name_.assign_ptr(raw_sql.ptr(), raw_sql.length());
    } else {
      FPContext fp_ctx(conn_coll);
      fp_ctx.enable_batched_multi_stmt_ = pc_ctx.sql_ctx_.handle_batched_multi_stmt();
      fp_ctx.sql_mode_ = sql_mode;
      if (OB_FAIL(ObSqlParameterization::fast_parser(allocator,
                                                    fp_ctx,
                                                    raw_sql,
                                                    fp_result))) {
        LOG_WARN("failed to fast parser", K(ret), K(sql_mode), K(pc_ctx.raw_sql_));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

//plan cache中add plan 入口
//1.检查内存限制
//2.添加plan
//3.添加plan stat
int ObPlanCache::add_plan(ObPhysicalPlan *plan, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(pc_add_plan);
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else if (is_reach_memory_limit()) {
    ret = OB_REACH_MEMORY_LIMIT;
    if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
      SQL_PC_LOG(WARN, "plan cache memory used reach limit",
                        K_(tenant_id), K(get_mem_hold()), K(get_mem_limit()), K(ret));
    }
  } else if (plan->get_mem_size() >= get_mem_high()) {
    // plan mem is too big, do not add plan
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, ObLibCacheNameSpace::NS_CRSR))) {
    LOG_WARN("construct plan cache key failed", K(ret));
  } else if (OB_FAIL(add_plan_cache(pc_ctx, plan))) {
    if (!is_not_supported_err(ret)
        && OB_SQL_PC_PLAN_DUPLICATE != ret
        && OB_PC_LOCK_CONFLICT != ret) {
      SQL_PC_LOG(WARN, "fail to add plan", K(ret));
    }
  } else {
    (void)inc_mem_used(plan->get_mem_size());
  }

  return ret;
}

int ObPlanCache::add_plan_cache(ObILibCacheCtx &ctx,
                                ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid cache obj", K(ret));
  } else {
    ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
    pc_ctx.key_ = &(pc_ctx.fp_result_.pc_key_);
    do {
      if (OB_FAIL(add_cache_obj(ctx, pc_ctx.key_, cache_obj)) && OB_OLD_SCHEMA_VERSION == ret) {
        SQL_PC_LOG(INFO, "table or view in plan cache value is old", K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = remove_cache_node(pc_ctx.key_))) {
          ret = tmp_ret;
          SQL_PC_LOG(WARN, "fail to remove lib cache node", K(ret));
        }
      }
    } while (OB_OLD_SCHEMA_VERSION == ret && pc_ctx.need_retry_add_plan());
  }
  return ret;
}

int ObPlanCache::get_plan_cache(ObILibCacheCtx &ctx,
                                ObCacheObjGuard &guard)
{
  int ret = OB_SUCCESS;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  pc_ctx.key_ = &(pc_ctx.fp_result_.pc_key_);
  if (OB_FAIL(get_cache_obj(ctx, pc_ctx.key_, guard))) {
    SQL_PC_LOG(DEBUG, "failed to get plan", K(ret));
  }
  // check the returned error code and whether the plan has expired
  if (OB_FAIL(check_after_get_plan(ret, ctx, guard.cache_obj_))) {
    SQL_PC_LOG(DEBUG, "failed to check after get plan", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(guard.cache_obj_)) {
    co_mgr_.free(guard.cache_obj_, guard.ref_handle_);
    guard.cache_obj_ = NULL;
  }
  return ret;
}

int ObPlanCache::add_cache_obj(ObILibCacheCtx &ctx,
                               ObILibCacheKey *key,
                               ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObLibCacheWlockAndRef w_ref_lock(LC_NODE_WR_HANDLE);
  ObILibCacheNode *cache_node = NULL;
  if (OB_ISNULL(key) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid null argument", K(ret), K(key), K(cache_obj));
  } else if (get_tenant_id() != cache_obj->get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "unmatched tenant_id", K(ret), K(get_tenant_id()), K(cache_obj->get_tenant_id()));
  } else if (OB_FAIL(get_value(key, cache_node, w_ref_lock /*write locked*/))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(DEBUG, "failed to get cache node from lib cache by key", K(ret));
  } else if (NULL == cache_node) {
    ObILibCacheKey *cache_key = NULL;
    //create node, init node, and add cache obj
    if (OB_FAIL(create_node_and_add_cache_obj(key, ctx, cache_obj, cache_node))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "fail to create node and add cache obj", K(ret));
      }
    } else if (OB_FAIL(OBLCKeyCreator::create_cache_key(cache_obj->get_ns(),
                                                        cache_node->get_allocator_ref(),
                                                        cache_key))) {
      cache_node->dec_ref_count(LC_NODE_HANDLE);//cache node dec ref in alloc
      SQL_PC_LOG(WARN, "failed to create lib cache key", K(ret));
    } else if (OB_FAIL(cache_key->deep_copy(cache_node->get_allocator_ref(),
                                            static_cast<ObILibCacheKey&>(*key)))) {
      cache_node->dec_ref_count(LC_NODE_HANDLE);//cache node dec ref in alloc
      SQL_PC_LOG(WARN, "failed to deep copy cache key", K(ret), KPC(key));
    }
    if (OB_SUCC(ret)) {
      cache_node->inc_ref_count(LC_NODE_HANDLE); //inc ref count in block
      int hash_err = cache_key_node_map_.set_refactored(cache_key, cache_node);
      if (OB_HASH_EXIST == hash_err) { //may be this node has been set by other thread。
        cache_node->unlock();
        // before add_cache_obj again, first need to release the cache key and cache node
        // that has not been added to cache_key_node_map_.
        cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in block
        cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in alloc
        if (OB_FAIL(add_cache_obj(ctx, key, cache_obj))) {
          SQL_PC_LOG(TRACE, "fail to add cache obj", K(ret), K(cache_obj));
        }
      } else if (OB_SUCCESS == hash_err) {
        SQL_PC_LOG(DEBUG, "succeed to set node to key_node_map");
        /* stat must be added after set_refactored is successful, otherwise the following may occur:
         *              Thread A                                            Thread B
         *    create_node_and_add_cache_obj
         *
         *       add_stat_for_cache_obj
         *                                                         create_node_and_add_cache_obj
         *
         *                                                             add_stat_for_cache_obj
         *
         *                                                        set_refactored(cache_node) succeed
         *
         *   set_refactored(cache_node) failed
         *
         *   destroy cache node and mark the cache obj logic deleted
         *
         *           add_cache_obj
         *
         * the problem is that after the cache obj is marked as logic deleted, the background thread
         * will actively release the cache obj after a period of time, even if the cache obj object
         * is still cached in the lib cache.
         *
         */
        if (OB_FAIL(add_stat_for_cache_obj(ctx, cache_obj))) {
          LOG_WARN("failed to add stat", K(ret));
          ObILibCacheNode *del_node = NULL;
          int tmp_ret = cache_key_node_map_.erase_refactored(cache_key, &del_node);
          if (OB_UNLIKELY(tmp_ret != OB_SUCCESS)
              || OB_UNLIKELY(del_node != cache_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret), K(tmp_ret), K(del_node), K(cache_node));
          } else {
            cache_node->unlock();
            cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in block
            cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in alloc
          }
        } else {
          cache_node->unlock();
          cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in block
        }
      } else {
        SQL_PC_LOG(TRACE, "failed to add node to key_node_map", K(ret), KPC(cache_obj));
        cache_node->unlock();
        cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in block
        cache_node->dec_ref_count(LC_NODE_HANDLE); //cache node dec ref in alloc
      }
    }
  } else {  /* node exist, add cache obj to it */
    LOG_TRACE("inner add cache obj", K(key), K(cache_node));
    if (OB_FAIL(cache_node->add_cache_obj(ctx, key, cache_obj))) {
      SQL_PC_LOG(DEBUG, "failed to add cache obj to lib cache node", K(ret));
    } else if (OB_FAIL(cache_node->update_node_stat(ctx))) {
      SQL_PC_LOG(WARN, "failed to update node stat", K(ret));
    } else if (OB_FAIL(add_stat_for_cache_obj(ctx, cache_obj))) {
      LOG_WARN("failed to add stat", K(ret), K(ctx));
    }
    // release wlock whatever
    cache_node->unlock();
    cache_node->dec_ref_count(LC_NODE_WR_HANDLE);
  }
  return ret;
}

int ObPlanCache::get_cache_obj(ObILibCacheCtx &ctx,
                               ObILibCacheKey *key,
                               ObCacheObjGuard &guard)
{
  int ret = OB_SUCCESS;
  ObILibCacheNode *cache_node = NULL;
  ObILibCacheObject *cache_obj = NULL;
  // get the read lock and increase reference count
  ObLibCacheRlockAndRef r_ref_lock(LC_NODE_RD_HANDLE);
  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid null argument", K(ret), K(key));
  } else if (OB_FAIL(get_value(key, cache_node, r_ref_lock /*read locked*/))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(DEBUG, "failed to get cache node from lib cache by key", K(ret));
  } else if (OB_UNLIKELY(NULL == cache_node)) {
    ret = OB_SQL_PC_NOT_EXIST;
    SQL_PC_LOG(DEBUG, "cache obj does not exist!", K(key));
  } else {
    LOG_DEBUG("inner_get_cache_obj", K(key), K(cache_node));
    if (OB_FAIL(cache_node->update_node_stat(ctx))) {
      SQL_PC_LOG(WARN, "failed to update node stat",  K(ret));
    } else if (OB_FAIL(cache_node->get_cache_obj(ctx, key, cache_obj))) {
      if (OB_SQL_PC_NOT_EXIST != ret) {
        LOG_DEBUG("cache_node fail to get cache obj", K(ret));
      }
    } else {
      guard.cache_obj_ = cache_obj;
      LOG_DEBUG("succ to get cache obj", KPC(key));
    }
    // release lock whatever
    (void)cache_node->unlock();
    (void)cache_node->dec_ref_count(LC_NODE_RD_HANDLE);
    NG_TRACE(pc_choose_plan);
  }

  return ret;
}

int ObPlanCache::cache_node_exists(ObILibCacheKey* key,
                                   bool& is_exists)
{
  int ret = OB_SUCCESS;
  ObILibCacheNode *cache_node = NULL;
  // get the read lock and increase reference count
  ObLibCacheRlockAndRef r_ref_lock(LC_NODE_RD_HANDLE);
  is_exists = false;
  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid null argument", K(ret), K(key));
  } else if (OB_FAIL(get_value(key, cache_node, r_ref_lock /*read locked*/))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(DEBUG, "failed to get cache node from lib cache by key", K(ret));
  } else if (OB_UNLIKELY(NULL == cache_node)) {
    is_exists = false;
  } else {
    // release lock whatever
    (void)cache_node->unlock();
    (void)cache_node->dec_ref_count(LC_NODE_RD_HANDLE);
    is_exists = true;
  }

  return ret;
}

int ObPlanCache::get_value(ObILibCacheKey *key,
                           ObILibCacheNode *&node,
                           ObLibCacheAtomicOp &op)
{
  int ret = OB_SUCCESS;
  //get lib cache node and inc ref count
  int hash_err = cache_key_node_map_.read_atomic(key, op);
  switch (hash_err) {
  case OB_SUCCESS: {
      //get node and lock
      if (OB_FAIL(op.get_value(node))) {
        SQL_PC_LOG(DEBUG, "failed to lock cache node", K(ret), KPC(key));
      }
      break;
    }
  case OB_HASH_NOT_EXIST: { //返回时 node = NULL; ret = OB_SUCCESS;
      SQL_PC_LOG(DEBUG, "entry does not exist.", KPC(key));
      break;
    }
  default: {
      SQL_PC_LOG(WARN, "failed to get cache node", K(ret), KPC(key));
      ret = hash_err;
      break;
    }
  }
  return ret;
}

int ObPlanCache::evict_plan(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObGetTableIdOp get_ids_op(table_id);
  if (OB_FAIL(co_mgr_.foreach_cache_obj(get_ids_op))) {
    SQL_PC_LOG(WARN, "fail to get all sql_ids in id2value_map", K(ret));
  }
  return ret;
}

template<typename CallBack>
int ObPlanCache::foreach_cache_evict(CallBack &cb)
{
  int ret = OB_SUCCESS;
  const LCKeyValueArray *to_evict_list = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_FAIL(cache_key_node_map_.foreach_refactored(cb))) {
    SQL_PC_LOG(WARN, "traversing cache_key_node_map failed", K(ret));
  } else if (OB_ISNULL(to_evict_list = cb.get_key_value_list())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "to_evict_list is null", K(ret));
  } else if (OB_FAIL(batch_remove_cache_node(*to_evict_list))) {
    SQL_PC_LOG(WARN, "failed to remove lib cache node", K(ret));
  }
  //decrement reference count
  int64_t N = to_evict_list->count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (NULL != to_evict_list->at(i).node_) {
      to_evict_list->at(i).node_->dec_ref_count(cb.get_ref_handle());
    }
  }
  return ret;
}

template int ObPlanCache::foreach_cache_evict<pl::ObGetPLKVEntryOp>(pl::ObGetPLKVEntryOp &);

// Remove all cache object in the lib cache
int ObPlanCache::cache_evict_all_obj()
{
  int ret = OB_SUCCESS;
  SQL_PC_LOG(DEBUG, "cache evict all plan start");
  LCKeyValueArray to_evict_keys;
  ObKVEntryTraverseOp get_ids_op(&to_evict_keys, PCV_GET_PLAN_KEY_HANDLE);
  if (OB_FAIL(foreach_cache_evict(get_ids_op))) {
    SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
  }
  SQL_PC_LOG(DEBUG, "cache evict all plan end");
  return ret;
}

int ObPlanCache::cache_evict_by_ns(ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  SQL_PC_LOG(DEBUG, "cache evict all obj by ns start");
  LCKeyValueArray to_evict_keys;
  ObGetKVEntryByNsOp get_ids_op(ns, &to_evict_keys, PCV_GET_PLAN_KEY_HANDLE);
  if (OB_FAIL(foreach_cache_evict(get_ids_op))) {
    SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
  }
  SQL_PC_LOG(DEBUG, "cache evict all obj by ns end");
  return ret;
}

// Remove all plans in the plan cache
int ObPlanCache::cache_evict_all_plan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cache_evict_by_ns(ObLibCacheNameSpace::NS_CRSR))) {
    SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
  }
  return ret;
}

// delete plan by sql_id
int ObPlanCache::cache_evict_plan_by_sql_id(uint64_t db_id, common::ObString sql_id)
{
  int ret = OB_SUCCESS;
  SQL_PC_LOG(DEBUG, "cache evict plan by sql id start");
  LCKeyValueArray to_evict_keys;
  ObGetKVEntryBySQLIDOp get_ids_op(db_id, sql_id, &to_evict_keys, PCV_GET_PLAN_KEY_HANDLE);
  if (OB_FAIL(foreach_cache_evict(get_ids_op))) {
    SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
  }
  SQL_PC_LOG(DEBUG, "cache evict plan by sql id end");
  return ret;
}


int ObPlanCache::evict_plan_by_table_name(uint64_t database_id, ObString tab_name)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObGlobalReqTimeService::check_req_timeinfo();
  SQL_PC_LOG(DEBUG, "cache evict plan by table name start");
  LCKeyValueArray to_evict_keys;
  ObGetPcvSetByTabNameOp get_ids_op(database_id, tab_name, &to_evict_keys, PCV_GET_PLAN_KEY_HANDLE);
  if (OB_FAIL(foreach_cache_evict(get_ids_op))) {
    SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
  }
  SQL_PC_LOG(DEBUG, "cache evict plan baseline by sql id end");

  return ret;
}

// Delete the cache according to the evict mechanism
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
  int64_t cache_evict_num = 0;
  ObGlobalReqTimeService::check_req_timeinfo();
  SQL_PC_LOG(INFO, "start lib cache evict",
             K_(tenant_id),
             "mem_hold", get_mem_hold(),
             "mem_limit", get_mem_limit(),
             "cache_obj_num", get_cache_obj_size(),
             "cache_node_num", cache_key_node_map_.size());
  //determine whether it is still necessary to evict
  if (get_mem_hold() > get_mem_high()) {
    if (calc_evict_num(cache_evict_num) && cache_evict_num > 0) {
      LCKeyValueArray to_evict_keys;
      ObNodeStatFilterOp filter(cache_evict_num, &to_evict_keys, PCV_EXPIRE_BY_MEM_HANDLE);
      if (OB_FAIL(foreach_cache_evict(filter))) {
        SQL_PC_LOG(WARN, "failed to foreach cache evict", K(ret));
      }
    }
  }
  SQL_PC_LOG(INFO, "end lib cache evict",
             K_(tenant_id),
             "cache_evict_num", cache_evict_num,
             "mem_hold", get_mem_hold(),
             "mem_limit", get_mem_limit(),
             "cache_obj_num", get_cache_obj_size(),
             "cache_node_num", cache_key_node_map_.size());
  return ret;
}

// int ObPlanCache::load_plan_baseline()
// {
//   int ret = OB_SUCCESS;
//   ObGlobalReqTimeService::check_req_timeinfo();
//   SMART_VAR(PlanIdArray, plan_ids) {
//     ObGetAllPlanIdOp plan_id_op(&plan_ids);
//     if (OB_FAIL(co_mgr_.foreach_cache_obj(plan_id_op))) {
//       LOG_WARN("fail to traverse id2stat_map", K(ret));
//     } else {
//       ObPhysicalPlan *plan = NULL;
//       for (int64_t i = 0; i < plan_ids.count(); i++) {
//         uint64_t plan_id= plan_ids.at(i);
//         ObCacheObjGuard guard(LOAD_BASELINE_HANDLE);
//         int tmp_ret = ref_plan(plan_id, guard); //plan引用计数加1
//         plan = static_cast<ObPhysicalPlan*>(guard.cache_obj_);
//         if (OB_HASH_NOT_EXIST == tmp_ret) {
//           //do nothing;
//         } else if (OB_SUCCESS != tmp_ret || NULL == plan) {
//           LOG_WARN("get plan failed", K(tmp_ret), KP(plan));
//         } else if (false == plan->stat_.is_evolution_) { //不在演进中
//           LOG_DEBUG("load plan baseline", "bl_info", plan->stat_.bl_info_, K(plan->should_add_baseline()));
//           share::schema::ObSchemaGetterGuard schema_guard;
//           const share::schema::ObPlanBaselineInfo *bl_info = NULL;
//           if (OB_FAIL(ObPlanBaseline::select_bl(schema_guard,
//                                                 plan->stat_.bl_info_.key_,
//                                                 bl_info))) {
//             LOG_WARN("fail to get outline data from baseline", K(ret));
//           } else if (!OB_ISNULL(bl_info)) { //plan baseline不为空
//             if (bl_info->outline_data_ == plan->stat_.bl_info_.outline_data_) {
//               //do nothing
//             } else { //outline data不同, 不同机器可能生成不同的计划, 不符合预期
//               LOG_WARN("diff plan in plan cache and plan baseline",
//                       "baseline info in plan cache", plan->stat_.bl_info_,
//                       "baseline info in plan baseline", *bl_info);
//             }
//           } else if (plan->should_add_baseline() &&
//                     OB_SUCCESS != (tmp_ret = ObPlanBaseline::insert_bl(
//                                               plan->stat_.bl_info_))) {
//             LOG_WARN("fail to replace plan baseline", K(tmp_ret), K(plan->stat_.bl_info_));
//           } else {
//             // do nothing
//           }
//         } else { //plan cache计划在演进中
//           // do nothing
//         }
//       }
//     }
//   }
//   return ret;
// }


// 计算plan_cache需要淘汰的pcv_set个数
// ret = true表示执行正常，否则失败
bool ObPlanCache::calc_evict_num(int64_t &plan_cache_evict_num)
{
  bool ret = true;
  //按照当前各自的内存比例，先计算各自需要淘汰多少内存
  int64_t pc_hold = get_mem_hold();
  int64_t mem_to_free = pc_hold - get_mem_low();
  if (mem_to_free <= 0) {
    ret = false;
  }

  //然后计算需要淘汰的条数
  if (ret) {
    if (pc_hold > 0) {
      plan_cache_evict_num = (int64_t)(((double)mem_to_free /
               (double)pc_hold) * (double)(cache_key_node_map_.size()));
    } else {
      plan_cache_evict_num = 0;
    }
  }

  return ret;
}

int ObPlanCache::batch_remove_cache_node(const LCKeyValueArray &to_evict)
{
  int ret = OB_SUCCESS;
  int64_t N = to_evict.count();
  SQL_PC_LOG(INFO, "actual evict number", "evict_value_num", to_evict.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(remove_cache_node(to_evict.at(i).key_))) {
      SQL_PC_LOG(WARN, "failed to remove cache node from lib cache", K(ret));
    }
  }
  return ret;
}

int ObPlanCache::remove_cache_node(ObILibCacheKey *key)
{
  int ret = OB_SUCCESS;
  int hash_err = OB_SUCCESS;
  ObILibCacheNode *del_node = NULL;
  hash_err = cache_key_node_map_.erase_refactored(key, &del_node);
  if (OB_SUCCESS == hash_err) {
    if (NULL != del_node) {
      del_node->dec_ref_count(LC_NODE_HANDLE);
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

int ObPlanCache::ref_cache_obj(const ObCacheObjID obj_id, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  ObCacheObjAtomicOp op(guard.ref_handle_);
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_FAIL(co_mgr_.atomic_get_alloc_cache_obj(obj_id, op))) {
    SQL_PC_LOG(WARN, "failed to get update plan statistic", K(obj_id), K(ret));
  } else if (NULL == op.get_value()) {
    ret = OB_HASH_NOT_EXIST;
  } else {
    guard.cache_obj_ = op.get_value();
  }
  return ret;
}

int ObPlanCache::ref_plan(const ObCacheObjID plan_id, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  ObILibCacheObject *cache_obj = NULL;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_FAIL(ref_cache_obj(plan_id, guard))) { // inc ref count by 1
    LOG_WARN("failed to ref cache obj", K(ret));
  } else if (FALSE_IT(cache_obj = guard.cache_obj_)) {
    // do nothing
  } else if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cache object", K(ret));
  } else if (ObLibCacheNameSpace::NS_CRSR != cache_obj->get_ns()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this cache object is not a plan.", K(ret), K(cache_obj->get_ns()));
  }
  return ret;
}

int ObPlanCache::add_cache_obj_stat(ObILibCacheCtx &ctx, ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(cache_obj), K(ret));
  } else if (OB_FAIL(cache_obj->update_cache_obj_stat(ctx))) {
    SQL_PC_LOG(WARN, "failed to update cache obj stat", K(cache_obj), K(ret));
  } else {
    cache_obj->inc_ref_count(LC_REF_CACHE_OBJ_STAT_HANDLE);
    if (OB_FAIL(co_mgr_.add_cache_obj(cache_obj))) {
      LOG_WARN("failed to set element", K(ret), K(cache_obj->get_object_id()));
      co_mgr_.free(cache_obj, LC_REF_CACHE_OBJ_STAT_HANDLE);
      cache_obj = NULL;
    } else {
      LOG_DEBUG("succeeded to add cache object stat", K(cache_obj->get_object_id()), K(cache_obj));
    }
  }
  return ret;
}

int ObPlanCache::remove_cache_obj_stat_entry(const ObCacheObjID cache_obj_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(co_mgr_.erase_cache_obj(cache_obj_id, LC_REF_CACHE_OBJ_STAT_HANDLE))) {
    SQL_PC_LOG(WARN, "failed to erase plan statistic entry", K(ret), K(cache_obj_id));
  }
  return ret;
}

int ObPlanCache::create_node_and_add_cache_obj(ObILibCacheKey *cache_key,
                                               ObILibCacheCtx &ctx,
                                               ObILibCacheObject *cache_obj,
                                               ObILibCacheNode *&cache_node)
{
  int ret = OB_SUCCESS;
  cache_node = NULL;
  if (OB_ISNULL(cache_key) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(cache_key), K(cache_obj));
  } else if (OB_FAIL(cn_factory_.create_cache_node(cache_key->namespace_,
                                                        cache_node,
                                                        tenant_id_))) {
    SQL_PC_LOG(WARN, "failed to create cache node", K(ret), K_(cache_key->namespace), K_(tenant_id));
  } else {
    // reference count after constructing cache node
    cache_node->inc_ref_count(LC_NODE_HANDLE);
    if (OB_FAIL(cache_node->init(ctx, cache_obj))) {
      LOG_WARN("fail to init lib cache node", K(ret));
    } else if (OB_FAIL(cache_node->add_cache_obj(ctx, cache_key, cache_obj))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "failed to add cache obj to lib cache node",  K(ret));
      }
    } else if (OB_FAIL(cache_node->update_node_stat(ctx))) {
      SQL_PC_LOG(WARN, "failed to update node stat",  K(ret));
    }
  }
  if (OB_FAIL(ret) && NULL != cache_node) {
    // dec_ref_count to zero, will destroy itself
    cache_node->dec_ref_count(LC_NODE_HANDLE);
    cache_node = NULL;
  }
  if (NULL != cache_node) {
    cache_node->lock(true); // add read lock
  }
  return ret;
}

int ObPlanCache::set_mem_conf(const ObPCMemPctConf &conf)
{
  int ret = OB_SUCCESS;

  if (conf.limit_pct_ != get_mem_limit_pct()) {
    set_mem_limit_pct(conf.limit_pct_);
    LOG_INFO("update ob_plan_cache_percentage",
             "new value", conf.limit_pct_,
             "old value", get_mem_limit_pct());
  }
  if (conf.high_pct_ != get_mem_high_pct()) {
    set_mem_high_pct(conf.high_pct_);
    LOG_INFO("update ob_plan_cache_evict_high_percentage",
             "new value", conf.high_pct_,
             "old value", get_mem_high_pct());
  }
  if (conf.low_pct_ != get_mem_low_pct()) {
    set_mem_low_pct(conf.low_pct_);
    LOG_INFO("update ob_plan_cache_evict_low_percentage",
             "new value", conf.low_pct_,
             "old value", get_mem_low_pct());
  }

  return ret;
}

int ObPlanCache::update_memory_conf()
{
  int ret = OB_SUCCESS;
  ObPCMemPctConf pc_mem_conf;
  const char* conf_names[3] =
      {
        "ob_plan_cache_percentage",
        "ob_plan_cache_evict_low_percentage",
        "ob_plan_cache_evict_high_percentage"
      };
  int64_t *conf_values[3] =
      {
        &pc_mem_conf.limit_pct_,
        &pc_mem_conf.low_pct_,
        &pc_mem_conf.high_pct_
      };
  ObArenaAllocator alloc;
  ObObj obj_val;

  if (tenant_id_ > OB_SYS_TENANT_ID && tenant_id_ <= OB_MAX_RESERVED_TENANT_ID) {
    // tenant id between (OB_SYS_TENANT_ID, OB_MAX_RESERVED_TENANT_ID) is a virtual tenant,
    // virtual tenants do not have schema
    // do nothing
  } else {
    for (int32_t i = 0; i < 3 && OB_SUCC(ret); ++i) {
    if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(tenant_id_,
                                                            alloc,
                                                            ObDataTypeCastParams(),
                                                            ObString(conf_names[i]), obj_val))) {
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
            "ob_plan_cache_percentage", pc_mem_conf.limit_pct_,
            "ob_plan_cache_evict_high_percentage", pc_mem_conf.high_pct_,
            "ob_plan_cache_evict_low_percentage", pc_mem_conf.low_pct_,
            "tenant_id", tenant_id_);

  return ret;
}

int64_t ObPlanCache::get_mem_hold() const
{
  return get_tenant_memory_hold(tenant_id_, ObCtxIds::PLAN_CACHE_CTX_ID);
}

int64_t ObPlanCache::get_label_hold(lib::ObLabel &label) const
{
  common::ObLabelItem item;
  get_tenant_label_memory(tenant_id_, label, item);
  return item.hold_;
}

//添加plan到plan cache
// 1.判断plan cache内存是否达到上限；
// 2.判断plan大小是否超出限制
// 3.通过plan cache key获取pcv：
//     如果获取pcv成功：则将plan 加入pcv
//     如果获取pcv失败：则新生成pcv; 将plan 加入该pcv; 最后将该pcv 加入到key->pcv map中，
// template<class T>
// int ObPlanCache::add_ps_plan(T *plan, ObPlanCacheCtx &pc_ctx)
// {
//   int ret = OB_SUCCESS;
//   ObGlobalReqTimeService::check_req_timeinfo();
//   if (OB_ISNULL(plan)) {
//     ret = OB_INVALID_ARGUMENT;
//     SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
//   } else if (is_reach_memory_limit()) {
//     ret = OB_REACH_MEMORY_LIMIT;
//     SQL_PC_LOG(DEBUG, "plan cache memory used reach the high water mark",
//     K(mem_used_), K(get_mem_limit()), K(ret));
//   } else if (plan->get_mem_size() >= get_mem_high()) {
//     // plan mem is too big to reach memory highwater, do not add plan
//   } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, ObLibCacheNameSpace::NS_CRSR))) {
//     LOG_WARN("fail to construnct plan cache key", K(ret));
//   } else if (OB_FAIL(add_plan_cache(pc_ctx, plan))) {
//     if (OB_FAIL(deal_add_ps_plan_result(ret, pc_ctx, *plan))) {
//       LOG_WARN("fail to deal result code", K(ret));
//     }
//   } else {
//     (void)inc_mem_used(plan->get_mem_size());
//   }
//   // Add plan cache with sql as key
//   if (OB_SUCC(ret) && !pc_ctx.sql_ctx_.is_remote_sql_) {
//     // Remote sql uses the ps to cache plans, but there are some differences from
//     // the normal ps protocol. remote sql does not use stmt_id to map plans
//     if (OB_ISNULL(pc_ctx.raw_sql_.ptr())) {
//       ret = OB_ERR_UNEXPECTED;
//       SQL_PC_LOG(WARN, "pc_ctx.raw_sql_.ptr() is NULL, cannot add plan to plan cache by sql", K(ret));
//     } else {
//       pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
//       if (OB_FAIL(add_exists_cache_obj_by_sql(pc_ctx, plan))) {
//         SQL_PC_LOG(WARN, "failed to add exists cache obj by sql", K(ret));
//       }
//     }
//   }
//   return ret;
// }

template<class T>
int ObPlanCache::add_ps_plan(T *plan, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else if (is_reach_memory_limit()) {
    ret = OB_REACH_MEMORY_LIMIT;
    SQL_PC_LOG(DEBUG, "plan cache memory used reach the high water mark",
    K(mem_used_), K(get_mem_limit()), K(ret));
  } else if (plan->get_mem_size() >= get_mem_high()) {
    // plan mem is too big to reach memory highwater, do not add plan
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, ObLibCacheNameSpace::NS_CRSR))) {
    LOG_WARN("fail to construnct plan cache key", K(ret));
  } else if (OB_ISNULL(pc_ctx.raw_sql_.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "pc_ctx.raw_sql_.ptr() is NULL, cannot add plan to plan cache by sql", K(ret));
  } else {
    pc_ctx.fp_result_.pc_key_.mode_ = pc_ctx.mode_;
    pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
    uint64_t old_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    // the remote plan uses key_id is 0 to distinguish, so if key_id is 0, it cannot be set to OB_INVALID_ID
    if (pc_ctx.fp_result_.pc_key_.key_id_ != 0) {
      pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;
    }
    if (OB_FAIL(add_plan_cache(pc_ctx, plan))) {
      if (OB_FAIL(deal_add_ps_plan_result(ret, pc_ctx, *plan))) {
        LOG_WARN("fail to deal result code", K(ret));
      }
    } else {
      (void)inc_mem_used(plan->get_mem_size());
    }
    // reset pc_ctx
    pc_ctx.fp_result_.pc_key_.name_.reset();
    pc_ctx.fp_result_.pc_key_.key_id_ = old_stmt_id;
  }
  return ret;
}

int ObPlanCache::add_exists_cache_obj_by_sql(ObILibCacheCtx &ctx,
                                             ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  // cache obj stat is already added, so set need_add_obj_stat_ to false to avoid adding it again
  pc_ctx.need_add_obj_stat_ = false;
  uint64_t old_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
  pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;
  SQL_PC_LOG(DEBUG, "start to add ps plan by sql", K(pc_ctx.fp_result_.pc_key_));
  if (OB_FAIL(add_plan_cache(pc_ctx, cache_obj))) {
    if (OB_FAIL(deal_add_ps_plan_result(ret, pc_ctx, *cache_obj))) {
      LOG_WARN("fail to deal result code", K(ret));
    }
  }
  // reset pc_ctx
  pc_ctx.fp_result_.pc_key_.name_.reset();
  pc_ctx.fp_result_.pc_key_.key_id_ = old_stmt_id;
  return ret;
}

template int ObPlanCache::add_ps_plan<ObPhysicalPlan>(ObPhysicalPlan *plan, ObPlanCacheCtx &pc_ctx);
template int ObPlanCache::add_ps_plan<ObPLFunction>(ObPLFunction *plan, ObPlanCacheCtx &pc_ctx);

int ObPlanCache::deal_add_ps_plan_result(int add_plan_ret,
                                         ObPlanCacheCtx &pc_ctx,
                                         const ObILibCacheObject &cache_object)
{
  int ret = add_plan_ret;
  if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
    ret = OB_SUCCESS;
    LOG_DEBUG("this plan has been added by others, need not add again", K(cache_object));
  } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
    if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
      ObTruncatedString trunc_sql(pc_ctx.raw_sql_);
      LOG_INFO("can't add plan to plan cache",
               K(ret), K(cache_object.get_mem_size()), K(trunc_sql),
               K(get_mem_used()));
    }
    ret = OB_SUCCESS;
  } else if (is_not_supported_err(ret)) {
    ret = OB_SUCCESS;
    LOG_DEBUG("plan cache don't support add this kind of plan now",  K(cache_object));
  } else if (OB_FAIL(ret)) {
    if (OB_REACH_MAX_CONCURRENT_NUM != ret) { //如果是达到限流上限, 则将错误码抛出去
      ret = OB_SUCCESS; //add plan出错, 覆盖错误码, 确保因plan cache失败不影响正常执行路径
      LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
    }
  } else {
    pc_ctx.sql_ctx_.self_add_plan_ = true;
    LOG_DEBUG("Successed to add plan to ObPlanCache", K(cache_object));
  }

  return ret;
}

int ObPlanCache::add_exists_cache_obj_by_stmt_id(ObILibCacheCtx &ctx,
                                                 ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  // cache obj stat is already added, so set need_add_obj_stat_ to false to avoid adding it again
  pc_ctx.need_add_obj_stat_ = false;
  if (OB_FAIL(add_plan_cache(ctx, cache_obj))) {
    if (OB_FAIL(deal_add_ps_plan_result(ret, pc_ctx, *cache_obj))) {
      LOG_WARN("fail to deal result code", K(ret));
    }
  } else {
    ObPsStmtId new_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    if (ObLibCacheNameSpace::NS_CRSR == cache_obj->get_ns()) {
      ObPhysicalPlan *plan = dynamic_cast<ObPhysicalPlan *>(cache_obj);
      if (OB_ISNULL(plan)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(WARN, "convert cache_obj to ObPhysicalPlan failed", K(ret));
      } else {
        SQL_PC_LOG(DEBUG, "ps_stmt_id changed", K(plan->stat_.ps_stmt_id_), K(new_stmt_id));
        plan->stat_.ps_stmt_id_ = new_stmt_id;
      }
    } else {
      ObPLFunction *pl_func = NULL;
      if (NS_ANON == cache_obj->get_ns()) {
        if (OB_ISNULL(pl_func = dynamic_cast<ObPLFunction *>(cache_obj))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected pl function", K(ret));
        } else {
          PLCacheObjStat &stat = pl_func->get_stat_for_update();
          stat.pl_schema_id_ = new_stmt_id;
        }
      }
    }
  }
  return ret;
}

// int ObPlanCache::get_ps_plan(ObCacheObjGuard& guard,
//                              const ObPsStmtId stmt_id,
//                              ObPlanCacheCtx &pc_ctx)
// {
//   int ret = OB_SUCCESS;
//   ObGlobalReqTimeService::check_req_timeinfo();
//   UNUSED(stmt_id);
//   ObSqlTraits sql_traits;
//   pc_ctx.handle_id_ = guard.ref_handle_;
//   int64_t original_param_cnt = 0;

//   if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)
//       || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)
//       || OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
//     ret = OB_INVALID_ARGUMENT;
//     SQL_PC_LOG(WARN, "invalid argument",
//                K(pc_ctx.sql_ctx_.session_info_),
//                K(pc_ctx.sql_ctx_.schema_guard_),
//                K(pc_ctx.exec_ctx_.get_physical_plan_ctx()),
//                K(ret));
//   } else if (FALSE_IT(pc_ctx.fp_result_.cache_params_ =
//     &(pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update()))) {
//     //do nothing
//   } else if (FALSE_IT(original_param_cnt = pc_ctx.fp_result_.cache_params_->count())) {
//     // do nothing
//   } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, ObLibCacheNameSpace::NS_CRSR))) {
//     LOG_WARN("fail to construnct plan cache key", K(ret));
//   } else if (OB_FAIL(get_plan_cache(pc_ctx, guard))) {
//     SQL_PC_LOG(DEBUG, "fail to get plan", K(ret));
//   } else if (OB_ISNULL(guard.cache_obj_) || OB_UNLIKELY(!guard.cache_obj_->is_valid_cache_obj())) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("cache obj is invalid", K(ret), KPC(guard.cache_obj_));
//   }

//   if (OB_SUCC(ret)) {
//     // successfully retrieve plan via ps params
//   } else if (OB_SQL_PC_NOT_EXIST == ret && !pc_ctx.sql_ctx_.is_remote_sql_) {
//     // use sql as the key to find the plan from the plan cache
//     ObPsStmtId new_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
//     pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;
//     pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
//     pc_ctx.exec_ctx_.get_physical_plan_ctx()->restore_param_store(original_param_cnt);
//     SQL_PC_LOG(DEBUG, "start to get plan by sql",
//                        K(new_stmt_id), K(pc_ctx.fp_result_.pc_key_), K(cache_key_node_map_.size()));
//     if (OB_FAIL(get_plan_cache(pc_ctx, guard))) {
//       if (OB_OLD_SCHEMA_VERSION == ret) {
//         SQL_PC_LOG(DEBUG, "get cache obj by sql failed because of old_schema_version",
//         K(ret), K(pc_ctx.raw_sql_), K(new_stmt_id), K(cache_key_node_map_.size()));
//       } else {
//         SQL_PC_LOG(WARN, "get cache obj by sql failed",
//         K(ret), K(pc_ctx.raw_sql_), K(new_stmt_id), K(cache_key_node_map_.size()));
//       }
//     } else {
//       // The plan is found with sql as the key, and now it is added to the plan cache with stmt_id as the key
//       pc_ctx.fp_result_.pc_key_.name_.reset();
//       pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
//       SQL_PC_LOG(DEBUG, "get cache obj by sql succeed, will add kv pair by new stmt_id",
//                          K(new_stmt_id), K(pc_ctx.fp_result_.pc_key_));
//       if (OB_FAIL(add_exists_cache_obj_by_stmt_id(pc_ctx, guard.cache_obj_))) {
//         SQL_PC_LOG(WARN, "add cache obj by new stmt_id failed", K(ret), K(stmt_id));
//       } else {
//         SQL_PC_LOG(DEBUG, "add_exists_pcv_set succeed", K(pc_ctx.fp_result_.pc_key_));
//       }
//     }
//     // reset pc_ctx
//     pc_ctx.fp_result_.pc_key_.name_.reset();
//     pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
//   } else {
//     // failed to retrieve plan via sql.
//   }
//   if (OB_SUCC(ret) && ObLibCacheNameSpace::NS_CRSR == guard.cache_obj_->get_ns()) {
//     ObPhysicalPlan *sql_plan = static_cast<ObPhysicalPlan *>(guard.cache_obj_);
//     MEMCPY(pc_ctx.sql_ctx_.sql_id_,
//            sql_plan->stat_.sql_id_.ptr(),
//            sql_plan->stat_.sql_id_.length());
//   }
//   //check read only privilege
//   if (OB_SUCC(ret) && GCONF.enable_perf_event) {
//     uint64_t tenant_id = pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id();
//     bool read_only = false;
//     if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
//       SQL_PC_LOG(WARN, "fail to get tenant read only attribute", K(tenant_id), K(ret));
//     } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->check_read_only_privilege(read_only,
//                                                                                 sql_traits))) {
//       SQL_PC_LOG(WARN, "failed to check read_only privilege", K(ret));
//     }
//   }
//   if (OB_FAIL(ret) && OB_NOT_NULL(guard.cache_obj_)) {
//     co_mgr_.free(guard.cache_obj_, guard.ref_handle_);
//     guard.cache_obj_ = NULL;
//   }
//   return ret;
// }

int ObPlanCache::get_ps_plan(ObCacheObjGuard& guard,
                             const ObPsStmtId stmt_id,
                             ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObGlobalReqTimeService::check_req_timeinfo();
  UNUSED(stmt_id);
  ObSqlTraits sql_traits;
  pc_ctx.handle_id_ = guard.ref_handle_;
  int64_t original_param_cnt = 0;

  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)
      || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)
      || OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument",
               K(pc_ctx.sql_ctx_.session_info_),
               K(pc_ctx.sql_ctx_.schema_guard_),
               K(pc_ctx.exec_ctx_.get_physical_plan_ctx()),
               K(ret));
  } else if (FALSE_IT(pc_ctx.fp_result_.cache_params_ =
    &(pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store_for_update()))) {
    //do nothing
  } else if (FALSE_IT(original_param_cnt = pc_ctx.fp_result_.cache_params_->count())) {
    // do nothing
  } else if (OB_FAIL(construct_plan_cache_key(pc_ctx, ObLibCacheNameSpace::NS_CRSR))) {
    LOG_WARN("fail to construnct plan cache key", K(ret));
  } else {
    ObPsStmtId new_stmt_id = pc_ctx.fp_result_.pc_key_.key_id_;
    // the remote plan uses key_id is 0 to distinguish, so if key_id is 0, it cannot be set to OB_INVALID_ID
    if (pc_ctx.fp_result_.pc_key_.key_id_ != 0) {
      pc_ctx.fp_result_.pc_key_.key_id_ = OB_INVALID_ID;
    }
    pc_ctx.fp_result_.pc_key_.name_ = pc_ctx.raw_sql_;
    if (OB_FAIL(get_plan_cache(pc_ctx, guard))) {
      if (OB_OLD_SCHEMA_VERSION == ret) {
        SQL_PC_LOG(DEBUG, "get cache obj by sql failed because of old_schema_version",
        K(ret), K(pc_ctx.raw_sql_), K(new_stmt_id), K(cache_key_node_map_.size()));
      } else {
        SQL_PC_LOG(DEBUG, "get cache obj by sql failed",
        K(ret), K(pc_ctx.raw_sql_), K(new_stmt_id), K(cache_key_node_map_.size()));
      }
      SQL_PC_LOG(DEBUG, "fail to get plan", K(ret));
    } else if (OB_ISNULL(guard.cache_obj_) || OB_UNLIKELY(!guard.cache_obj_->is_valid_cache_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache obj is invalid", K(ret), KPC(guard.cache_obj_));
    }
    pc_ctx.fp_result_.pc_key_.name_.reset();
    pc_ctx.fp_result_.pc_key_.key_id_ = new_stmt_id;
  }
  if (OB_SUCC(ret) && ObLibCacheNameSpace::NS_CRSR == guard.cache_obj_->get_ns()) {
    ObPhysicalPlan *sql_plan = static_cast<ObPhysicalPlan *>(guard.cache_obj_);
    MEMCPY(pc_ctx.sql_ctx_.sql_id_,
           sql_plan->stat_.sql_id_.ptr(),
           sql_plan->stat_.sql_id_.length());
  }
  //check read only privilege
  if (OB_SUCC(ret) && GCONF.enable_perf_event) {
    uint64_t tenant_id = pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id();
    bool read_only = false;
    if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_tenant_read_only(tenant_id, read_only))) {
      SQL_PC_LOG(WARN, "fail to get tenant read only attribute", K(tenant_id), K(ret));
    } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->check_read_only_privilege(read_only,
                                                                                sql_traits))) {
      SQL_PC_LOG(WARN, "failed to check read_only privilege", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(guard.cache_obj_)) {
    co_mgr_.free(guard.cache_obj_, guard.ref_handle_);
    guard.cache_obj_ = NULL;
  }
  return ret;
}

int ObPlanCache::construct_plan_cache_key(ObPlanCacheCtx &plan_ctx, ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = plan_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (OB_FAIL(construct_plan_cache_key(*session,
                                              ns,
                                              plan_ctx.fp_result_.pc_key_))) {
    LOG_WARN("failed to construct plan cache key", K(ret));
  } else {
    plan_ctx.key_ = &(plan_ctx.fp_result_.pc_key_);
  }
  return ret;
}

OB_INLINE int ObPlanCache::construct_plan_cache_key(ObSQLSessionInfo &session,
                                                    ObLibCacheNameSpace ns,
                                                    ObPlanCacheKey &pc_key)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  session.get_database_id(database_id);
  pc_key.db_id_ = (database_id == OB_INVALID_ID) ? OB_OUTLINE_DEFAULT_DATABASE_ID : database_id;
  pc_key.namespace_ = ns;
  pc_key.sys_vars_str_ = session.get_sys_var_in_pc_str();
  pc_key.config_str_ = session.get_config_in_pc_str();
  return ret;

}

int ObPlanCache::need_late_compile(ObPhysicalPlan *plan,
                                   bool &need_late_compilation)
{
  int ret = OB_SUCCESS;
  need_late_compilation = false;
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan));
  } else if (plan->is_use_jit()) {
    // do nothing
  } else if (plan->stat_.execute_times_ >= 10) { // after ten times of execution
    uint64_t avg_exec_duration = plan->stat_.cpu_time_ / plan->stat_.execute_times_;
    // for now, hard code the exec duration threshold to be 1s
    if (avg_exec_duration > 1000000UL) {
      need_late_compilation = true;
    } else {
      need_late_compilation = false;
    }
  }
  LOG_DEBUG("will use late compilation", K(need_late_compilation));
  return ret;
}

int ObPlanCache::add_stat_for_cache_obj(ObILibCacheCtx &ctx, ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  bool need_real_add = true;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cache_obj));
  } else if (OB_FAIL(cache_obj->check_need_add_cache_obj_stat(ctx, need_real_add))) {
    LOG_WARN("failed to check need add cache obj stat", K(ret));
  } else if (need_real_add && OB_FAIL(add_cache_obj_stat(ctx, cache_obj))) {
    LOG_WARN("failed to add cache obj stat", K(ret));
  }
  return ret;
}

int ObPlanCache::alloc_cache_obj(ObCacheObjGuard& guard, ObLibCacheNameSpace ns, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(co_mgr_.alloc(guard, ns, tenant_id))) {
    LOG_WARN("failed to alloc cache obj", K(ret));
  }
  return ret;
}

void ObPlanCache::free_cache_obj(ObILibCacheObject *&cache_obj, const CacheRefHandleID ref_handle)
{
  co_mgr_.free(cache_obj, ref_handle);
  cache_obj = NULL;
}

int ObPlanCache::destroy_cache_obj(const bool is_leaked, const uint64_t object_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(co_mgr_.destroy_cache_obj(is_leaked, object_id))) {
    LOG_WARN("failed to destroy cache obj", K(ret));
  }
  return ret;
}

int ObPlanCache::dump_all_objs() const
{
  int ret = OB_SUCCESS;
  ObArray<AllocCacheObjInfo> alloc_obj_list;
  ObDumpAllCacheObjOp get_all_objs_op(&alloc_obj_list, INT64_MAX);
  if (OB_FAIL(co_mgr_.foreach_alloc_cache_obj(get_all_objs_op))) {
    LOG_WARN("failed to traverse alloc cache obj map", K(ret));
  } else {
    LOG_INFO("Dumping All Cache Objs", K(alloc_obj_list.count()), K(alloc_obj_list));
  }
  return ret;
}

int ObPlanCache::dump_deleted_objs_by_ns(ObIArray<AllocCacheObjInfo> &deleted_objs,
                                         const int64_t safe_timestamp,
                                         const ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  ObDumpAllCacheObjByNsOp get_deleted_objs_op(&deleted_objs, safe_timestamp, ns);
  if (OB_FAIL(co_mgr_.foreach_alloc_cache_obj(get_deleted_objs_op))) {
    LOG_WARN("failed to traverse alloc cache obj map", K(ret));
  }
  return ret;
}

template<DumpType dump_type>
int ObPlanCache::dump_deleted_objs(ObIArray<AllocCacheObjInfo> &deleted_objs,
                                   const int64_t safe_timestamp) const
{
  int ret = OB_SUCCESS;
  ObDumpAllCacheObjByTypeOp get_deleted_objs_op(&deleted_objs, safe_timestamp, dump_type);
  if (OB_FAIL(co_mgr_.foreach_alloc_cache_obj(get_deleted_objs_op))) {
    LOG_WARN("failed to traverse alloc cache obj map", K(ret));
  }
  return ret;
}

template int ObPlanCache::dump_deleted_objs<DUMP_PL>(ObIArray<AllocCacheObjInfo> &,
                                                       const int64_t) const;
template int ObPlanCache::dump_deleted_objs<DUMP_SQL>(ObIArray<AllocCacheObjInfo> &,
                                                       const int64_t) const;
template int ObPlanCache::dump_deleted_objs<DUMP_ALL>(ObIArray<AllocCacheObjInfo> &,
                                                        const int64_t) const;
int ObPlanCache::mtl_init(ObPlanCache* &plan_cache)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = lib::current_resource_owner_id();
  int64_t mem_limit = lib::get_tenant_memory_limit(tenant_id);
  if (OB_FAIL(plan_cache->init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
    LOG_WARN("failed to init request manager", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObPlanCache::mtl_stop(ObPlanCache * &plan_cache)
{
  if (OB_LIKELY(nullptr != plan_cache)) {
    TG_CANCEL(plan_cache->tg_id_, plan_cache->evict_task_);
    TG_STOP(plan_cache->tg_id_);
  }
}

int ObPlanCache::flush_plan_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_FAIL(cache_evict_all_plan())) {
    SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
  }
  ObArray<AllocCacheObjInfo> deleted_objs;
  int64_t safe_timestamp = INT64_MAX;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance().get_global_safe_timestamp(safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(dump_deleted_objs<DUMP_SQL>(deleted_objs, safe_timestamp))) {
    SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
  } else {
    LOG_INFO("Deleted Cache Objs", K(deleted_objs));
    for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
      if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                          deleted_objs.at(i).obj_id_,
                                                          this))) {
          LOG_WARN("failed to destroy cache obj", K(ret));
      }
    }
  }
  return ret;
}

int ObPlanCache::flush_plan_cache_by_sql_id(uint64_t db_id, common::ObString sql_id)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_FAIL(cache_evict_plan_by_sql_id(db_id, sql_id))) {
    SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
  }
  ObArray<AllocCacheObjInfo> deleted_objs;
  int64_t safe_timestamp = INT64_MAX;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                        .get_global_safe_timestamp(safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(dump_deleted_objs<DUMP_SQL>(deleted_objs, safe_timestamp))) {
    SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
  } else {
    LOG_INFO("Deleted Cache Objs", K(deleted_objs));
    for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
      if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                          deleted_objs.at(i).obj_id_,
                                                          this))) {
          LOG_WARN("failed to destroy cache obj", K(ret));
      }
    }
  }
  return ret;
}

int ObPlanCache::flush_lib_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_FAIL(cache_evict_all_obj())) {
    SQL_PC_LOG(ERROR, "lib cache evict failed, please check", K(ret));
  }
  ObArray<AllocCacheObjInfo> deleted_objs;
  int64_t safe_timestamp = INT64_MAX;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance().get_global_safe_timestamp(safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(dump_deleted_objs<DUMP_ALL>(deleted_objs, safe_timestamp))) {
    SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
  } else {
    LOG_INFO("Deleted Cache Objs", K(deleted_objs));
    for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
      if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                          deleted_objs.at(i).obj_id_,
                                                          this))) {
          LOG_WARN("failed to destroy cache obj", K(ret));
      }
    }
  }
  return ret;
}

int ObPlanCache::flush_lib_cache_by_ns(const ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  int64_t safe_timestamp = INT64_MAX;
  ObArray<AllocCacheObjInfo> deleted_objs;
  if (OB_FAIL(cache_evict_by_ns(ns))) {
    SQL_PC_LOG(ERROR, "cache evict by ns failed, please check", K(ret));
  } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                         .get_global_safe_timestamp(safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(dump_deleted_objs_by_ns(deleted_objs, safe_timestamp, ns))) {
    SQL_PC_LOG(ERROR, "failed to dump deleted objs by ns", K(ret));
  } else {
    LOG_INFO("Deleted Cache Objs", K(deleted_objs));
    for (int i = 0; i < deleted_objs.count(); i++) {  // ignore error code and continue
      if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                          deleted_objs.at(i).obj_id_,
                                                          this))) {
        LOG_WARN("failed to destroy cache obj", K(ret));
      }
    }
  }
  return ret;
}


int ObPlanCache::flush_pl_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  int64_t safe_timestamp = INT64_MAX;
  ObArray<AllocCacheObjInfo> deleted_objs;
  if (OB_FAIL(ObPLCacheMgr::cache_evict_all_pl(this))) {
    SQL_PC_LOG(ERROR, "PL cache evict failed, please check", K(ret));
  } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                         .get_global_safe_timestamp(safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(dump_deleted_objs<DUMP_PL>(deleted_objs, safe_timestamp))) {
    SQL_PC_LOG(ERROR, "failed to dump deleted pl objs", K(ret));
  } else {
    LOG_INFO("Deleted Cache Objs", K(deleted_objs));
    for (int i = 0; i < deleted_objs.count(); i++) {  // ignore error code and continue
      if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                          deleted_objs.at(i).obj_id_,
                                                          this))) {
        LOG_WARN("failed to destroy cache obj", K(ret));
      }
    }
  }
  return ret;
}

const char *plan_cache_gc_confs[3] = { "OFF", "REPORT", "AUTO" };

int ObPlanCache::get_plan_cache_gc_strategy()
{
  PlanCacheGCStrategy strategy = INVALID;
  for (int i = 0; i < ARRAYSIZEOF(plan_cache_gc_confs) && strategy == INVALID; i++) {
    if (0 == ObString::make_string(plan_cache_gc_confs[i])
               .case_compare(GCONF._ob_plan_cache_gc_strategy)) {
      strategy = static_cast<PlanCacheGCStrategy>(i);
    }
  }
  return strategy;
}


void ObPlanCacheEliminationTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  ++run_task_counter_;
  const int64_t auto_flush_pc_interval = (int64_t)(GCONF._ob_plan_cache_auto_flush_interval) / (1000 * 1000L); // second
  {
    // 在调用plan cache接口前引用plan资源前必须定义guard
    observer::ObReqTimeGuard req_timeinfo_guard;

    run_plan_cache_task();
    if (0 != auto_flush_pc_interval
      && 0 == run_task_counter_ % auto_flush_pc_interval) {
      IGNORE_RETURN plan_cache_->flush_plan_cache();
    }
    SQL_PC_LOG(INFO, "schedule next cache evict task",
              "evict_interval", (int64_t)(GCONF.plan_cache_evict_interval));
  }
  // free cache obj in deleted map
  if (plan_cache_->get_plan_cache_gc_strategy() > 0) {
    observer::ObReqTimeGuard req_timeinfo_guard;
    run_free_cache_obj_task();
  }
  SQL_PC_LOG(INFO, "schedule next cache evict task",
             "evict_interval", (int64_t)(GCONF.plan_cache_evict_interval));
}

void ObPlanCacheEliminationTask::run_plan_cache_task()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(plan_cache_->update_memory_conf())) { //如果失败, 则不更新设置, 也不影响其他流程
    SQL_PC_LOG(WARN, "fail to update plan cache memory sys val", K(ret));
  }
  if (OB_FAIL(plan_cache_->cache_evict())) {
    SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
  }
}

void ObPlanCacheEliminationTask::run_free_cache_obj_task()
{
  int ret = OB_SUCCESS;
  ObArray<AllocCacheObjInfo> deleted_objs;
  int64_t safe_timestamp = INT64_MAX;
  if (observer::ObGlobalReqTimeService::get_instance()
                         .get_global_safe_timestamp(safe_timestamp)) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else if (OB_FAIL(plan_cache_->dump_deleted_objs<DUMP_ALL>(deleted_objs, safe_timestamp))) {
    SQL_PC_LOG(WARN, "failed to traverse hashmap", K(ret));
  } else {
    int64_t tot_mem_used = 0;
    for (int k = 0; k < deleted_objs.count(); k++) {
      tot_mem_used += deleted_objs.at(k).mem_used_;
    } // end for
    if (tot_mem_used >= ((plan_cache_->get_mem_limit() / 100) * 30)) {
      LOG_ERROR("Cache Object Memory Leaked Much!!!", K(tot_mem_used),
                K(plan_cache_->get_mem_limit()), K(deleted_objs), K(safe_timestamp));
    } else if (deleted_objs.count() > 0) {
      LOG_WARN("Cache Object Memory Leaked Much!!!", K(deleted_objs),
               K(safe_timestamp), K(plan_cache_->get_mem_limit()));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(plan_cache_->dump_all_objs())) {
    LOG_WARN("failed to dump deleted map", K(ret));
  }
}

} // end of namespace sql
} // end of namespace oceanbase

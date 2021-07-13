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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/alloc/alloc_func.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_id_manager_allocator.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"

namespace oceanbase {
namespace share {
class ObIPartitionLocationCache;
}
using common::ObPsStmtId;
namespace sql {
class ObPlanCacheValue;
class ObPlanCacheAtomicOp;
class ObPsPCVSetAtomicOp;
class ObTaskExecutorCtx;
class ObSqlCtx;
class ObExecContext;
class ObPCVSet;
class ObCacheObject;
class ObPhysicalPlan;

enum DumpType { DUMP_SQL, DUMP_PL, DUMP_ALL };
struct ObGetAllDeletedCacheObjIdsOp {
  explicit ObGetAllDeletedCacheObjIdsOp(
      common::ObIArray<DeletedCacheObjInfo>* key_array, const bool dump_all, int64_t safe_timestamp, DumpType dump_type)
      : key_array_(key_array), dump_all_(dump_all), safe_timestamp_(safe_timestamp), dump_type_(dump_type)
  {}
  int operator()(common::hash::HashMapPair<uint64_t, ObCacheObject*>& entry);

private:
  bool should_dump(ObCacheObjType obj_type) const
  {
    bool ret_bool = false;
    if (obj_type >= T_CO_MAX) {
      // invalid cache object should not be dumped.
      ret_bool = false;
      SQL_PC_LOG(WARN, "Invalid cache object should not be dumped");
    } else if (DUMP_SQL == dump_type_) {
      ret_bool = (T_CO_SQL_CRSR == obj_type);
    } else if (DUMP_PL == dump_type_) {
      ret_bool = (T_CO_PRCR == obj_type || T_CO_SFC == obj_type || T_CO_PKG == obj_type || T_CO_ANON == obj_type);
    } else if (DUMP_ALL == dump_type_) {
      ret_bool = true;
    } else {
      ret_bool = false;
    }
    return ret_bool;
  }

private:
  common::ObIArray<DeletedCacheObjInfo>* key_array_;
  const bool dump_all_;
  int64_t safe_timestamp_;
  DumpType dump_type_;
};

class ObPlanCache {
  friend class ObCacheObjectFactory;

public:
  static const int64_t MAX_PLAN_SIZE = 20 * 1024 * 1024;                 // 20M
  static const int64_t MAX_PLAN_CACHE_SIZE = 5 * 1024L * 1024L * 1024L;  // 5G
  static const int64_t EVICT_KEY_NUM = 8;
  static const int64_t MAX_TENANT_MEM = ((int64_t)(1) << 40);  // 1T
  typedef common::hash::ObHashMap<ObCacheObjID, ObCacheObject*> PlanStatMap;
  typedef common::hash::ObHashMap<ObPlanCacheKey, ObPCVSet*> SqlPCVSetMap;

  ObPlanCache();
  virtual ~ObPlanCache();
  int init(
      int64_t hash_bucket, common::ObAddr addr, share::ObIPartitionLocationCache* location_cache, uint64_t tenant_id);
  bool is_inited()
  {
    return inited_;
  }
  int get_plan(const CacheRefHandleID ref_handle, common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx,
      ObPhysicalPlan*& plan);

  /**
   * Add new plan to PlanCache
   */
  int add_plan(ObPhysicalPlan* plan, ObPlanCacheCtx& pc_ctx);
  /**
   * Add new ps plan to PlanCache
   */
  template <class T>
  int add_ps_plan(T* plan, ObPlanCacheCtx& pc_ctx);
  template <class T>
  int get_ps_plan(const CacheRefHandleID ref_handle, const ObPsStmtId stmt_id, ObPlanCacheCtx& pc_ctx, T*& plan);

  /**
   * memory related
   *    high water mark
   *    low water mark
   *    memory used
   */
  int set_mem_conf(const ObPCMemPctConf& conf);
  int update_memory_conf();
  int64_t get_mem_limit() const
  {
    int64_t tenant_mem = get_tenant_memory();
    int64_t mem_limit = -1;
    if (OB_UNLIKELY(0 >= tenant_mem || tenant_mem >= MAX_TENANT_MEM)) {
      mem_limit = MAX_TENANT_MEM * 0.05;
    } else {
      mem_limit = get_tenant_memory() / 100 * get_mem_limit_pct();
    }
    return mem_limit;
  }
  int64_t get_mem_high() const
  {
    return get_mem_limit() / 100 * get_mem_high_pct();
  }
  int64_t get_mem_low() const
  {
    return get_mem_limit() / 100 * get_mem_low_pct();
  }

  int64_t get_mem_limit_pct() const
  {
    return ATOMIC_LOAD(&mem_limit_pct_);
  }
  int64_t get_mem_high_pct() const
  {
    return ATOMIC_LOAD(&mem_high_pct_);
  }
  int64_t get_mem_low_pct() const
  {
    return ATOMIC_LOAD(&mem_low_pct_);
  }
  void set_mem_limit_pct(int64_t pct)
  {
    ATOMIC_STORE(&mem_limit_pct_, pct);
  }
  void set_mem_high_pct(int64_t pct)
  {
    ATOMIC_STORE(&mem_high_pct_, pct);
  }
  void set_mem_low_pct(int64_t pct)
  {
    ATOMIC_STORE(&mem_low_pct_, pct);
  }

  uint64_t inc_mem_used(uint64_t mem_delta)
  {
    SQL_PC_LOG(DEBUG, "before inc mem_used", K(mem_used_));
    return ATOMIC_FAA((uint64_t*)&mem_used_, mem_delta);
  };
  uint64_t dec_mem_used(uint64_t mem_delta)
  {
    SQL_PC_LOG(DEBUG, "before dec mem_used, mem_used", K(mem_used_));
    return ATOMIC_FAA((uint64_t*)&mem_used_, -mem_delta);
  };

  void inc_access_cnt()
  {
    ATOMIC_INC(&pc_stat_.access_count_);
  }
  void inc_hit_and_access_cnt()
  {
    ATOMIC_INC(&pc_stat_.hit_count_);
    ATOMIC_INC(&pc_stat_.access_count_);
  }
  int64_t get_mem_used() const
  {
    return mem_used_ + get_mod_hold(ObNewModIds::OB_SQL_PLAN_CACHE);
  }
  int64_t get_mem_hold() const;
  int64_t get_mod_hold(int mod_id) const;
  int64_t get_bucket_num() const
  {
    return bucket_num_;
  }
  /*
   * cache evict
   */
  int cache_evict_all_plan();
  int cache_evict_all_pl();
  // evict plan, adjust mem between hwm and lwm
  int cache_evict();
  // evict plan whose merged version is expired
  int evict_expired_plan();
  bool is_valid()
  {
    return valid_;
  }
  void set_valid(bool valid)
  {
    valid_ = valid;
  }
  void destroy();
  int64_t inc_ref_count();
  void dec_ref_count();
  common::ObAddr& get_host()
  {
    return host_;
  }
  void set_host(common::ObAddr& addr)
  {
    host_ = addr;
  }
  void set_location_cache(share::ObIPartitionLocationCache* location_cache)
  {
    location_cache_ = location_cache;
  }
  share::ObIPartitionLocationCache* get_location_cache()
  {
    return location_cache_;
  }
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_tenant_memory() const
  {
    return lib::get_tenant_memory_limit(tenant_id_);
  }
  void set_tenant_id(int64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  common::ObIAllocator* get_pc_allocator()
  {
    return &inner_allocator_;
  }
  common::ObIAllocator& get_pc_allocator_ref()
  {
    return inner_allocator_;
  }
  int64_t get_ref_count() const
  {
    return ref_count_;
  }

  int64_t get_sql_num() const
  {
    return sql_pcvs_map_.size();
  }
  int64_t count() const
  {
    return sql_pcvs_map_.size();
  }
  int64_t get_plan_num() const
  {
    return plan_stat_map_.size();
  }
  ObPlanCacheStat& get_plan_cache_stat()
  {
    return pc_stat_;
  }
  const ObPlanCacheStat& get_plan_cache_stat() const
  {
    return pc_stat_;
  }
  PlanStatMap& get_plan_stat_map()
  {
    return plan_stat_map_;
  }
  int ref_cache_obj(const ObCacheObjID obj_id, const CacheRefHandleID ref_handle, ObCacheObject*& cache_obj);
  int ref_plan(const ObCacheObjID obj_id, const CacheRefHandleID ref_handle, ObPhysicalPlan*& plan);
  int remove_cache_obj_stat_entry(const ObCacheObjID cache_obj_id);
  uint64_t allocate_plan_id()
  {
    return __sync_add_and_fetch(&plan_id_, 1);
  }
  int add_exists_pcv_set_by_new_stmt_id(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx);
  int add_exists_pcv_set_by_sql(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx);
  static int construct_fast_parser_result(common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx,
      const common::ObString& raw_sql, ObFastParserResult& fp_result);
  static int construct_multi_stmt_fast_parser_result(common::ObIAllocator& allocator, ObPlanCacheCtx& pc_ctx);
  PlanStatMap& get_deleted_map()
  {
    return deleted_map_;
  }
  int dump_all_objs() const;
  template <DumpType dump_type>
  int dump_deleted_objs(common::ObIArray<DeletedCacheObjInfo>& deleted_objs, const int64_t safe_timestamp) const;
  static common::ObMemAttr get_mem_attr()
  {
    common::ObMemAttr attr;
    attr.label_ = ObNewModIds::OB_SQL_PLAN_CACHE;
    attr.tenant_id_ = OB_INVALID_ID;
    attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
    return attr;
  }

  TO_STRING_KV(K_(tenant_id), K_(mem_limit_pct), K_(mem_high_pct), K_(mem_low_pct));

  ObCacheRefHandleMgr& get_ref_handle_mgr()
  {
    return ref_handle_mgr_;
  }
  const ObCacheRefHandleMgr& get_ref_handle_mgr() const
  {
    return ref_handle_mgr_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCache);
  int add_cache_obj(ObCacheObject* plan, ObPlanCacheCtx& pc_ctx);
  int get_cache_obj(ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj);
  int get_value(const ObPlanCacheKey key, ObPCVSet*& pcv_set, ObPlanCacheAtomicOp& op);
  int add_cache_obj_stat(ObPlanCacheCtx& pc_ctx, ObCacheObject* plan);
  bool calc_evict_num(int64_t& plan_cache_evict_num);
  int calc_evict_keys(int64_t evict_num, PCKeyValueArray& to_evict_keys);
  int remove_pcv_set(const ObPlanCacheKey& key);
  int remove_pcv_sets(common::ObIArray<PCKeyValue>& to_evict);
  bool is_reach_memory_limit()
  {
    return get_mem_hold() > get_mem_limit();
  }
  int construct_plan_cache_key(ObPlanCacheCtx& plan_ctx, ObjNameSpace ns);
  static int construct_plan_cache_key(ObSQLSessionInfo& session, ObjNameSpace ns, ObPlanCacheKey& pc_key);
  /**
   * @brief wether jit compilation is needed in this sql
   *
   */
  int add_stat_for_cache_obj(ObPlanCacheCtx& pc_ctx, ObCacheObject* cache_obj);
  int create_pcv_set_and_add_plan(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx, ObPCVSet*& pcv_set);
  int deal_add_ps_plan_result(int add_plan_ret, ObPlanCacheCtx& pc_ctx, const ObCacheObject& cache_object);

  int get_pl_cache(ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj);

private:
  const static int64_t SLICE_SIZE = 1024;  // 1k
private:
  bool inited_;
  bool valid_;
  int64_t tenant_id_;
  int64_t mem_limit_pct_;
  int64_t mem_high_pct_;  // high water mark percentage
  int64_t mem_low_pct_;   // low water mark percentage
  int64_t mem_used_;      // mem used now
  int64_t bucket_num_;
  // parameterized_sql --> pcv_set
  SqlPCVSetMap sql_pcvs_map_;
  common::ObMalloc inner_allocator_;  // used for stmtkey and pre_calc_expr deep copy
  common::ObAddr host_;
  share::ObIPartitionLocationCache* location_cache_;
  ObPlanCacheStat pc_stat_;
  PlanStatMap deleted_map_;
  // use map for iterating in virtual table.
  PlanStatMap plan_stat_map_;
  // used for gen cache obj ids
  volatile ObCacheObjID plan_id_;
  volatile int64_t ref_count_;
  // ObSqlParameterization sql_parameterization_;
  // ref handle infos
  ObCacheRefHandleMgr ref_handle_mgr_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_PLAN_CACHE_H */

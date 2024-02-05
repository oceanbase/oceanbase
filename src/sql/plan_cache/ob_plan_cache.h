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
#include "sql/plan_cache/ob_lib_cache_key_creator.h"
#include "sql/plan_cache/ob_lib_cache_node_factory.h"
#include "sql/plan_cache/ob_lib_cache_object_manager.h"
namespace oceanbase
{
namespace observer
{
  class ObGVSql;
  class ObAllVirtualSqlPlan;
}
namespace rpc
{
  class ObLoadBaselineArg;
}
namespace pl
{
class ObPLFunction;
class ObPLPackage;
class ObGetPLKVEntryOp;
}  // namespace pl
using common::ObPsStmtId;
namespace sql
{
class ObPlanCacheValue;
class ObPlanCacheAtomicOp;
class ObPsPCVSetAtomicOp;
class ObTaskExecutorCtx;
struct ObSqlCtx;
class ObExecContext;
class ObPCVSet;
class ObILibCacheObject;
class ObPhysicalPlan;
class ObLibCacheAtomicOp;
class ObEvolutionPlan;

typedef common::hash::ObHashMap<uint64_t, ObPlanCache *> PlanCacheMap;
#ifdef OB_BUILD_SPM
typedef common::ObSEArray<ObEvolutionPlan*, 16> EvolutionPlanList;
#endif

struct ObKVEntryTraverseOp
{
  typedef common::hash::HashMapPair<ObILibCacheKey *, ObILibCacheNode *> LibCacheKVEntry;
  explicit ObKVEntryTraverseOp(LCKeyValueArray *key_val_list,
                               const CacheRefHandleID ref_handle)
    : total_mem_used_(0),
      ref_handle_(ref_handle),
      key_value_list_(key_val_list)
  {
  }

  virtual int check_entry_match(LibCacheKVEntry &entry, bool &is_match)
  {
    UNUSED(entry);
    int ret = OB_SUCCESS;
    is_match = true;
    return ret;
  }
  virtual int operator()(LibCacheKVEntry &entry)
  {
    int ret = common::OB_SUCCESS;
    bool is_match = false;
    if (OB_ISNULL(key_value_list_) || OB_ISNULL(entry.first) || OB_ISNULL(entry.second)) {
      ret = common::OB_INVALID_ARGUMENT;
      PL_CACHE_LOG(WARN, "invalid argument",
      K(key_value_list_), K(entry.first), K(entry.second), K(ret));
    } else if (OB_FAIL(check_entry_match(entry, is_match))) {
      PL_CACHE_LOG(WARN, "failed to check entry match", K(ret));
    } else if (is_match) {
      if (OB_FAIL(key_value_list_->push_back(ObLCKeyValue(entry.first, entry.second)))) {
        PL_CACHE_LOG(WARN, "fail to push back key", K(ret));
      } else {
        entry.second->inc_ref_count(ref_handle_);
        total_mem_used_ += entry.second->get_mem_size();
      }
    }
    return ret;
  }
  int64_t get_total_mem_used() const { return total_mem_used_; }
  CacheRefHandleID get_ref_handle() { return ref_handle_; } const
  LCKeyValueArray *get_key_value_list() { return key_value_list_; }

  int64_t total_mem_used_;
  const CacheRefHandleID ref_handle_;
  LCKeyValueArray *key_value_list_;
};


struct ObDumpAllCacheObjOp
{
  explicit ObDumpAllCacheObjOp(common::ObIArray<AllocCacheObjInfo> *key_array,
                               int64_t safe_timestamp)
    : key_array_(key_array),
      safe_timestamp_(safe_timestamp)
  {
  }
  int operator()(common::hash::HashMapPair<uint64_t, ObILibCacheObject *> &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(key_array_)) {
      ret = OB_NOT_INIT;
      SQL_PC_LOG(WARN, "key array not inited", K(ret));
    } else if (OB_ISNULL(entry.second)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(WARN, "unexpected null entry.second", K(ret));
    } else if (should_dump(entry.second)
              && OB_FAIL(key_array_->push_back(AllocCacheObjInfo(
                  entry.second->get_object_id(),
                  entry.second->get_tenant_id(),
                  entry.second->get_logical_del_time(),
                  safe_timestamp_,
                  entry.second->get_ref_count(),
                  entry.second->get_allocator().used(),
                  entry.second->added_lc())))) {
      SQL_PC_LOG(WARN, "failed to push back element", K(ret));
    }
    return ret;
  }

protected:
  virtual bool should_dump(ObILibCacheObject *cache_obj) const
  {
    UNUSED(cache_obj);
    return true;
  }
protected:
  common::ObIArray<AllocCacheObjInfo> *key_array_;
  int64_t safe_timestamp_;
};

enum DumpType { DUMP_SQL, DUMP_PL, DUMP_ALL };
struct ObDumpAllCacheObjByTypeOp : ObDumpAllCacheObjOp
{
  explicit ObDumpAllCacheObjByTypeOp(common::ObIArray<AllocCacheObjInfo> *key_array,
                                     int64_t safe_timestamp,
                                     DumpType dump_type)
    : ObDumpAllCacheObjOp(key_array, safe_timestamp),
      dump_type_(dump_type)
  {
  }
  virtual bool should_dump(ObILibCacheObject *cache_obj) const
  {
    bool ret_bool = false;
    if (cache_obj->should_release(safe_timestamp_)) {
      ObLibCacheNameSpace ns = cache_obj->get_ns();
      if (DUMP_ALL == dump_type_) {
        ret_bool = true;
      } else if (DUMP_SQL == dump_type_) {
        ret_bool = (ObLibCacheNameSpace::NS_CRSR == ns);
      } else if (DUMP_PL == dump_type_) {
        ret_bool = (ObLibCacheNameSpace::NS_PRCR == ns
                  || ObLibCacheNameSpace::NS_SFC == ns
                  || ObLibCacheNameSpace::NS_PKG == ns
                  || ObLibCacheNameSpace::NS_ANON == ns);
      }
    }
    return ret_bool;
  }

  DumpType dump_type_;
};

struct ObDumpAllCacheObjByNsOp : ObDumpAllCacheObjOp
{
  explicit ObDumpAllCacheObjByNsOp(common::ObIArray<AllocCacheObjInfo> *key_array,
                                   int64_t safe_timestamp,
                                   ObLibCacheNameSpace ns)
    : ObDumpAllCacheObjOp(key_array, safe_timestamp),
      namespace_(ns)
  {
  }
  virtual bool should_dump(ObILibCacheObject *cache_obj) const
  {
    bool ret_bool = false;
    if (cache_obj->should_release(safe_timestamp_)) {
      ObLibCacheNameSpace ns = cache_obj->get_ns();
      ret_bool = (namespace_ == ns);
    }
    return ret_bool;
  }

  ObLibCacheNameSpace namespace_;
};

class ObPlanCacheEliminationTask : public common::ObTimerTask
{
public:
  ObPlanCacheEliminationTask() : plan_cache_(NULL),
                            run_task_counter_(0)
  {
  }
  void runTimerTask(void);
private:
  void run_plan_cache_task();
  //void run_ps_cache_task();
  void run_free_cache_obj_task();
public:
  ObPlanCache* plan_cache_;
  int64_t run_task_counter_;
};

class ObPlanCache
{
friend class ObCacheObjectFactory;
friend class ObPlanCacheEliminationTask;
friend class observer::ObAllVirtualSqlPlan;
friend class observer::ObGVSql;

public:
  static const int64_t MAX_PLAN_SIZE = 20*1024*1024; //20M
  static const int64_t MAX_PLAN_CACHE_SIZE = 5*1024L*1024L*1024L; // 5G
  static const int64_t EVICT_KEY_NUM = 8;
  static const int64_t MAX_TENANT_MEM = ((int64_t)(1) << 40); // 1T
  typedef common::hash::ObHashMap<ObILibCacheKey*, ObILibCacheNode*> CacheKeyNodeMap;
  typedef common::ObSEArray<uint64_t, 1024> PlanIdArray;

  ObPlanCache();
  virtual ~ObPlanCache();
  static int mtl_init(ObPlanCache* &plan_cache);
  static void mtl_stop(ObPlanCache * &plan_cache);
  int init(int64_t hash_bucket, uint64_t tenant_id);
  bool is_inited() { return inited_; }

  static int check_can_do_insert_opt(common::ObIAllocator &allocator,
                                     ObPlanCacheCtx &pc_ctx,
                                     ObFastParserResult &fp_result,
                                     bool &can_do_batch,
                                     int64_t &batch_count,
                                     ObString &first_truncated_sql,
                                     bool &is_insert_values);
  static int rebuild_raw_params(common::ObIAllocator &allocator,
                                ObPlanCacheCtx &pc_ctx,
                                ObFastParserResult &fp_result,
                                int64_t row_count);

  static int restore_param_to_truncated_sql(ObPlanCacheCtx &pc_ctx);

  static bool can_do_insert_batch_opt(ObPlanCacheCtx &pc_ctx);

  /**
   * Add new plan to PlanCache
   */
  int add_plan(ObPhysicalPlan *plan, ObPlanCacheCtx &pc_ctx);

  /**
   * Add new ps plan to PlanCache
   */
  template<class T>
  int add_ps_plan(T *plan,
                  ObPlanCacheCtx &pc_ctx);

  // cache object access functions
  /* 根据ObPlanCacheKey以及参数在plan cache中查询符合要求的执行计划 */
  int get_plan(common::ObIAllocator &allocator, ObPlanCacheCtx &pc_ctx, ObCacheObjGuard& guard);
  /* 根据ObPlanCacheKey以及参数在plan cache中查询符合要求的执行计划 */
  int get_ps_plan(ObCacheObjGuard& guard, const ObPsStmtId stmt_id, ObPlanCacheCtx &pc_ctx);
  int ref_cache_obj(const ObCacheObjID obj_id, ObCacheObjGuard& guard);
  int ref_plan(const ObCacheObjID obj_id, ObCacheObjGuard& guard);
  int add_cache_obj(ObILibCacheCtx &ctx, ObILibCacheKey *key, ObILibCacheObject *cache_obj);
  int get_cache_obj(ObILibCacheCtx &ctx, ObILibCacheKey *key, ObCacheObjGuard &guard);
  int cache_node_exists(ObILibCacheKey* key, bool& is_exists);
  int add_exists_cache_obj_by_stmt_id(ObILibCacheCtx &ctx,
                                      ObILibCacheObject *cache_obj);
  int add_exists_cache_obj_by_sql(ObILibCacheCtx &ctx,
                                  ObILibCacheObject *cache_obj);
  int evict_plan(uint64_t table_id);
  int evict_plan_by_table_name(uint64_t database_id, ObString tab_name);

  /**
   * memory related
   *    high water mark
   *    low water mark
   *    memory used
   */
  //后台线程会每隔30s检查内存相关设置是否更新，如果更新会变更，因此需要atomic操作
  int set_mem_conf(const ObPCMemPctConf &conf);
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
  int64_t get_mem_high() const { return get_mem_limit()/100 * get_mem_high_pct(); }
  int64_t get_mem_low() const { return get_mem_limit()/100 * get_mem_low_pct(); }

  int64_t get_mem_limit_pct() const { return ATOMIC_LOAD(&mem_limit_pct_); }
  int64_t get_mem_high_pct() const { return ATOMIC_LOAD(&mem_high_pct_); }
  int64_t get_mem_low_pct() const { return ATOMIC_LOAD(&mem_low_pct_); }
  void set_mem_limit_pct(int64_t pct) { ATOMIC_STORE(&mem_limit_pct_, pct); }
  void set_mem_high_pct(int64_t pct) { ATOMIC_STORE(&mem_high_pct_, pct); }
  void set_mem_low_pct(int64_t pct) { ATOMIC_STORE(&mem_low_pct_, pct); }

  uint64_t inc_mem_used(uint64_t mem_delta)
  {
    SQL_PC_LOG(DEBUG, "before inc mem_used", K(mem_used_));
    return ATOMIC_FAA((uint64_t*)&mem_used_, mem_delta);
  };
  uint64_t dec_mem_used(uint64_t mem_delta)
  {
    SQL_PC_LOG(DEBUG, "before dec mem_used, mem_used", K(mem_used_));
    return ATOMIC_FAA((uint64_t *)&mem_used_, -mem_delta);
  };

  int64_t get_mem_used() const
  {
    lib::ObLabel label;
    label = ObNewModIds::OB_SQL_PLAN_CACHE;
    return mem_used_ + get_label_hold(label);
  }
  int64_t get_mem_hold() const;
  int64_t get_label_hold(lib::ObLabel &label) const;
  int64_t get_bucket_num() const { return bucket_num_; }

  // access count related
  void inc_access_cnt() { ATOMIC_INC(&pc_stat_.access_count_);}
  void inc_hit_and_access_cnt()
  {
    ATOMIC_INC(&pc_stat_.hit_count_);
    ATOMIC_INC(&pc_stat_.access_count_);
  }

  /*
   * cache evict
   */
  int cache_evict_all_plan();
  int cache_evict_all_obj();
  //evict plan, adjust mem between hwm and lwm
  int cache_evict();
  int cache_evict_by_glitch_node();
  int cache_evict_plan_by_sql_id(uint64_t db_id, common::ObString sql_id);
  int cache_evict_by_ns(ObLibCacheNameSpace ns);
  template<typename CallBack = ObKVEntryTraverseOp>
  int foreach_cache_evict(CallBack &cb);
#ifdef OB_BUILD_SPM
  int cache_evict_baseline_by_sql_id(uint64_t db_id, common::ObString sql_id);
  // load plan baseline from plan cache
  // int load_plan_baseline();
  int load_plan_baseline(const obrpc::ObLoadPlanBaselineArg &arg, uint64_t &load_count);
  int check_baseline_finish();
#endif
  void destroy();
  common::ObAddr &get_host() { return host_; }
  void set_host(common::ObAddr &addr) { host_ = addr; }
  int64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_tenant_memory() const {
    return lib::get_tenant_memory_limit(tenant_id_);
  }
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
  common::ObIAllocator *get_pc_allocator() { return &inner_allocator_; }
  common::ObIAllocator &get_pc_allocator_ref() { return inner_allocator_; }
  int64_t get_cache_obj_size() const { return co_mgr_.get_cache_obj_size(); }
  ObPlanCacheStat &get_plan_cache_stat() { return pc_stat_; }
  const ObPlanCacheStat &get_plan_cache_stat() const { return pc_stat_; }
  int remove_cache_obj_stat_entry(const ObCacheObjID cache_obj_id);
  int remove_cache_node(ObILibCacheKey *key);
  ObLCObjectManager &get_cache_obj_mgr() { return co_mgr_; }
  ObLCNodeFactory &get_cache_node_factory() { return cn_factory_; }
  int alloc_cache_obj(ObCacheObjGuard& guard, ObLibCacheNameSpace ns, uint64_t tenant_id);
  void free_cache_obj(ObILibCacheObject *&cache_obj, const CacheRefHandleID ref_handle);
  int destroy_cache_obj(const bool is_leaked, const uint64_t object_id);
  static int construct_fast_parser_result(common::ObIAllocator &allocator,
                                          ObPlanCacheCtx &pc_ctx,
                                          const common::ObString &raw_sql,
                                          ObFastParserResult &fp_result);
  static int construct_multi_stmt_fast_parser_result(common::ObIAllocator &allocator,
                                                     ObPlanCacheCtx &pc_ctx);
  int dump_all_objs() const;
  int dump_deleted_objs_by_ns(ObIArray<AllocCacheObjInfo> &deleted_objs,
                              const int64_t safe_timestamp,
                              const ObLibCacheNameSpace ns);
  template<DumpType dump_type>
  int dump_deleted_objs(common::ObIArray<AllocCacheObjInfo> &deleted_objs,
                        const int64_t safe_timestamp) const;
  template<typename _callback>
  int foreach_cache_obj(_callback &callback) const;
  template<typename _callback>
  int foreach_alloc_cache_obj(_callback &callback) const;

  common::ObMemAttr get_mem_attr() {
    common::ObMemAttr attr;
    attr.label_ = ObNewModIds::OB_SQL_PLAN_CACHE;
    attr.tenant_id_ = tenant_id_;
    attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
    return attr;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(mem_limit_pct),
               K_(mem_high_pct),
               K_(mem_low_pct));

  ObCacheRefHandleMgr &get_ref_handle_mgr() { return ref_handle_mgr_; }
  const ObCacheRefHandleMgr &get_ref_handle_mgr() const { return ref_handle_mgr_; }

public:
  int flush_plan_cache();
  int flush_plan_cache_by_sql_id(uint64_t db_id, common::ObString sql_id);
  template<typename GETPLKVEntryOp, typename EvictAttr>
  int flush_pl_cache_single_cache_obj(uint64_t db_id, EvictAttr &attr);
  int flush_lib_cache();
  int flush_lib_cache_by_ns(const ObLibCacheNameSpace ns);
  int flush_pl_cache();

protected:
  int ref_alloc_obj(const ObCacheObjID obj_id, ObCacheObjGuard& guard);
  int ref_alloc_plan(const ObCacheObjID obj_id, ObCacheObjGuard& guard);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCache);
  int add_plan_cache(ObILibCacheCtx &ctx,
                     ObILibCacheObject *cache_obj);
  int get_plan_cache(ObILibCacheCtx &ctx,
                     ObCacheObjGuard &guard);
  int get_value(ObILibCacheKey *key,
                ObILibCacheNode *&node,
                ObLibCacheAtomicOp &op);
  int add_cache_obj_stat(ObILibCacheCtx &ctx,
                         ObILibCacheObject *cache_obj);
  bool calc_evict_num(int64_t &plan_cache_evict_num);

  int batch_remove_cache_node(const LCKeyValueArray &to_evict);
  bool is_reach_memory_limit() { return get_mem_hold() > get_mem_limit(); }
  int construct_plan_cache_key(ObPlanCacheCtx &plan_ctx, ObLibCacheNameSpace ns);
  static int construct_plan_cache_key(ObSQLSessionInfo &session,
                                      ObLibCacheNameSpace ns,
                                      ObPlanCacheKey &pc_key,
                                      bool is_weak);
  /**
   * @brief wether jit compilation is needed in this sql
   *
   */
  int need_late_compile(ObPhysicalPlan *plan, bool &need_late_compilation);
  int add_stat_for_cache_obj(ObILibCacheCtx &ctx, ObILibCacheObject *cache_obj);
  int create_node_and_add_cache_obj(ObILibCacheKey *key,
                                    ObILibCacheCtx &ctx,
                                    ObILibCacheObject *cache_obj,
                                    ObILibCacheNode *&node);
  int deal_add_ps_plan_result(int add_plan_ret,
                              ObPlanCacheCtx &pc_ctx,
                              const ObILibCacheObject &cache_object);
  int check_after_get_plan(int tmp_ret, ObILibCacheCtx &ctx, ObILibCacheObject *cache_obj);
  int get_normalized_pattern_digest(const ObPlanCacheCtx &pc_ctx, uint64_t &pattern_digest);
private:
  enum PlanCacheGCStrategy { INVALID = -1, OFF = 0, REPORT = 1, AUTO = 2};
  static int get_plan_cache_gc_strategy();
private:
  const static int64_t SLICE_SIZE = 1024; //1k
private:
  bool inited_;
  int64_t tenant_id_;
  int64_t mem_limit_pct_;
  int64_t mem_high_pct_;                     // high water mark percentage
  int64_t mem_low_pct_;                      // low water mark percentage
  int64_t mem_used_;                         // mem used now
  int64_t bucket_num_;
  lib::MemoryContext root_context_;
  common::ObMalloc inner_allocator_;
  common::ObAddr host_;
  ObPlanCacheStat pc_stat_;
  // ref handle infos
  ObCacheRefHandleMgr ref_handle_mgr_;
  PlanCacheMap* pcm_;
  // mark this Plan Cache whether is destroying.
  volatile int64_t destroy_;
  ObLCObjectManager co_mgr_;
  ObLCNodeFactory cn_factory_;
  CacheKeyNodeMap cache_key_node_map_;
  ObPlanCacheEliminationTask evict_task_;
  int tg_id_;
};

template<typename _callback>
int ObPlanCache::foreach_cache_obj(_callback &callback) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(co_mgr_.foreach_cache_obj(callback))) {
    _OB_LOG(WARN, "fail to traverse cache obj map");
  }
  return ret;
}

template<typename _callback>
int ObPlanCache::foreach_alloc_cache_obj(_callback &callback) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(co_mgr_.foreach_alloc_cache_obj(callback))) {
    _OB_LOG(WARN, "fail to traverse alloc cache obj map");
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PLAN_CACHE_H */

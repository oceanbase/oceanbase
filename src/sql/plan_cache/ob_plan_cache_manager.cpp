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

#include "ob_plan_cache_manager.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_req_time_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "pl/ob_pl.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"

namespace oceanbase
{
namespace sql
{
int ObPlanCacheManager::init(common::ObAddr addr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (OB_FAIL(pcm_.create(hash::cal_next_prime(512),
                            ObModIds::OB_HASH_BUCKET_PLAN_CACHE,
                            ObModIds::OB_HASH_NODE_PLAN_CACHE))) {
      SQL_PC_LOG(WARN, "Failed to init plan cache manager", K(ret));
    } else if (OB_FAIL(ps_pcm_.create(hash::cal_next_prime(512),
                                      ObModIds::OB_HASH_BUCKET_PLAN_CACHE,
                                      ObModIds::OB_HASH_NODE_PS_CACHE))) {
      SQL_PC_LOG(WARN, "Failed to init ps plan cache manager", K(ret));
    } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::PlanCacheEvict, tg_id_))) {
      SQL_PC_LOG(WARN, "tg create failed", K(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      //TODO shengle, maybe exist memory leak
      SQL_PC_LOG(WARN, "Failed to init timer", K(ret));
    } else {
      self_addr_ = addr;
      elimination_task_.plan_cache_manager_ = this;
      if (OB_FAIL(TG_SCHEDULE(tg_id_, elimination_task_,
                              GCONF.plan_cache_evict_interval, false))) {
        SQL_PC_LOG(WARN, "Failed to schedule cache elimination task", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
    destroyed_ = false;
  }
  return ret;
}

void ObPlanCacheManager::destroy()
{
  if (inited_ && !destroyed_) {
    destroyed_ = true;
    inited_ = false;
    observer::ObReqTimeGuard req_timeinfo_guard;
    TG_DESTROY(tg_id_);
    for (PlanCacheMap::iterator it = pcm_.begin();
         it != pcm_.end();
         it++) {
      if (OB_ISNULL(it->second)) {
        BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "plan_cache is null");
      } else {
        it->second->destroy();
      }
    }
  }
}

ObPlanCache *ObPlanCacheManager::get_plan_cache(uint64_t tenant_id)
{
  ObPlanCache *pc = NULL;
  if (!destroyed_) {
    ObPlanCacheManagerAtomic op;
    int ret = pcm_.atomic_refactored(tenant_id, op);
    if (OB_SUCCESS == ret) {
      pc = op.get_plan_cache();
      SQL_PC_LOG(DEBUG, "Get Plan Cache Success", K(tenant_id), K(pc));
    } else if (OB_HASH_NOT_EXIST != ret) {
      SQL_PC_LOG(WARN, "fait to do atomic", K(ret));
    }
  }
  return pc;
}

ObPsCache *ObPlanCacheManager::get_ps_cache(const uint64_t tenant_id)
{
  ObPsCache *ps_cache = NULL;
  if (!destroyed_) {
    ObPsCacheManagerAtomic op;
    int ret = ps_pcm_.atomic_refactored(tenant_id, op);
    if (OB_SUCC(ret)) {
      SQL_PC_LOG(DEBUG, "Get ps plan cache success", K(tenant_id), K(ps_cache));
      ps_cache = op.get_ps_cache();
    } else if (OB_HASH_NOT_EXIST != ret) {
      SQL_PC_LOG(WARN, "failed to do atomic", K(ret));
    }
  }
  return ps_cache;
}

//  maybe get plan_cache = NULL;
//    this thread                    other thread
//
//   get but not exist
//                            set tenand_id->plan_cache to map
//  alloc plan_cache
//  set to map but exist now
//  free plan_cache
//                            erase tenand_id->plan_cache
//  get but not exist
//   return NULL
ObPlanCache *ObPlanCacheManager::get_or_create_plan_cache(uint64_t tenant_id, const ObPCMemPctConf &pc_mem_conf)
{
  ObPlanCache *pc = NULL;
  if (!destroyed_) {
    ObPlanCacheManagerAtomic op;
    int ret = pcm_.read_atomic(tenant_id, op);
    if (OB_SUCCESS == ret) {
      pc = op.get_plan_cache();
      SQL_PC_LOG(DEBUG, "Get Plan Cache Success", K(tenant_id), K(pc));
    } else if (OB_HASH_NOT_EXIST == ret) {
      // create plan cache
      void *buff = ob_malloc(sizeof(ObPlanCache), ObNewModIds::OB_SQL_PLAN_CACHE);
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_PC_LOG(ERROR, "Alloc mem for plan cache failed");
      } else if (NULL == (pc = new(buff)ObPlanCache())) {
        ret = OB_NOT_INIT;
        SQL_PC_LOG(WARN, "fail to constructor plan cache");
      } else {
        if (OB_FAIL(pc->init(common::OB_PLAN_CACHE_BUCKET_NUMBER,
                             self_addr_,
                             tenant_id,
                             &pcm_))) {
          SQL_PC_LOG(WARN, "init plan cache fail", K(tenant_id));
        } else if (OB_FAIL(pc->set_mem_conf(pc_mem_conf))) {
          SQL_PC_LOG(WARN, "fail to set plan cache memory conf", K(ret));
        }

        if (OB_FAIL(ret)) {
          SQL_PC_LOG(WARN, "Failed to init plan cache", K(tenant_id), K(ret));
          pc->~ObPlanCache();
          ob_free(buff);
          buff = NULL;
          pc = NULL;
        } else if (OB_SUCCESS == ret) {// add new plan cache to sql engine
          pc->inc_ref_count(); //hash表引用计数
          pc->inc_ref_count(); //外部代码引用计数
          ret = pcm_.set_refactored(tenant_id, pc);
          if (OB_SUCC(ret)) {
            SQL_PC_LOG(DEBUG, "Add Plan Cache Success", K(tenant_id), K(pc));
          } else {
            // someone else add plan cache already
            // or error when add plan cache to hashmap
            pc->~ObPlanCache();
            pc = NULL;
            ob_free(buff);
            buff = NULL;
            if (OB_SUCCESS == pcm_.atomic_refactored(tenant_id, op)) {
              pc = op.get_plan_cache();
              SQL_PC_LOG(DEBUG, "Get Plan Cache Success", K(tenant_id), K(pc));
            } else {
              //this case is describle on function name;
              SQL_PC_LOG(DEBUG, "tenant erase by others",K(tenant_id), K(ret));
            }
          }
        }
      }
    } else {
      SQL_PC_LOG(WARN, "unexpectd error", K(ret));
    }
  }

  return pc;
}

ObPsCache *ObPlanCacheManager::get_or_create_ps_cache(
    const uint64_t tenant_id, const ObPCMemPctConf &pc_mem_conf)
{
  ObPsCache *ps_cache = NULL;
  if (!destroyed_) {
    ObPsCacheManagerAtomic op;
    int ret = ps_pcm_.atomic_refactored(tenant_id, op);
    if (OB_SUCCESS == ret) {
      ps_cache = op.get_ps_cache();
      SQL_PC_LOG(DEBUG, "Get ps Plan Cache Success", K(tenant_id), K(ps_cache));
    } else if (OB_HASH_NOT_EXIST == ret) {
      // create ps plan cache
      void *buff = ob_malloc(sizeof(ObPsCache), ObModIds::OB_SQL_PS_CACHE);
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_PC_LOG(ERROR, "Alloc mem for ps plan cache failed");
      } else if (NULL == (ps_cache = new(buff)ObPsCache())) {
        ret = OB_NOT_INIT;
        SQL_PC_LOG(WARN, "fail to constructor ps plan cache");
      } else {
        if (OB_SUCC(ps_cache->init(common::OB_PLAN_CACHE_BUCKET_NUMBER,
                                        self_addr_,
                                        tenant_id))) {
          SQL_PC_LOG(INFO, "Init Ps Plan Cache Success", K(tenant_id));
        } else if (OB_FAIL(ps_cache->set_mem_conf(pc_mem_conf))) {
          SQL_PC_LOG(WARN, "fail to set plan cache memory conf", K(ret));
        } else {
          SQL_PC_LOG(WARN, "Failed to init plan cache", K(tenant_id), K(ret));
          ps_cache->~ObPsCache();
          ob_free(buff);
          buff = NULL;
          ps_cache = NULL;
        }

        // add new plan cache to sql engine
        if (OB_SUCCESS == ret && NULL != ps_cache) {
          ps_cache->inc_ref_count(); //hash表引用计数
          ps_cache->inc_ref_count(); //外部代码引用计数
          ret = ps_pcm_.set_refactored(tenant_id, ps_cache);
          if (OB_SUCC(ret)) {
            SQL_PC_LOG(INFO, "Add ps Plan Cache Success", K(tenant_id), K(ps_cache),
                       "ref_count", ps_cache->get_ref_count());
          } else {
            // someone else add plan cache already
            // or error when add plan cache to hashmap
            ps_cache->~ObPsCache();
            ps_cache = NULL;
            ob_free(buff);
            buff = NULL;
            if (OB_SUCCESS == ps_pcm_.atomic_refactored(tenant_id, op)) {
              ps_cache = op.get_ps_cache();
              SQL_PC_LOG(DEBUG, "Get Ps Plan Cache Success", K(tenant_id), K(ps_cache));
            } else {
              //this case is describle on function name;
              SQL_PC_LOG(INFO, "tenant erase by others",K(tenant_id), K(ret));
            }
          }
        }
      }
    } else {
      SQL_PC_LOG(WARN, "unexpectd error", K(ret));
    }
  }
  return ps_cache;
}

int ObPlanCacheManager::revert_plan_cache(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObPlanCache *ppc = NULL;
  observer::ObReqTimeGuard req_timeinfo_guard;
  int tmp_ret = pcm_.get_refactored(tenant_id, ppc);
  if (OB_SUCCESS == tmp_ret && NULL != ppc) {
    SQL_PC_LOG(INFO, "plan_cache_manager revert plan cache",
               "pc ref_count", ppc->get_ref_count(),
               K(tenant_id));
    //cancel scheduled task
    ppc->dec_ref_count();
  } else if (OB_HASH_NOT_EXIST == tmp_ret) { // maybe erase by other thread
    SQL_PC_LOG(INFO, "Plan Cache not exist", K(tenant_id));
  } else {
    ret = tmp_ret;
    SQL_PC_LOG(ERROR, "unexpected error when erase plan cache",
              K(tenant_id), K(ppc), K(ret));
  }
  return ret;
}

void ObPlanCacheManager::ObPlanCacheEliminationTask::runTimerTask()
{
#define NEED_AUTO_FLUSH_PC(v) (0 == v ? false : true)
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_cache_manager_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "plan_cache_manager not inited", K(ret));
  } else {
    ++run_task_counter_;
    const int64_t auto_flush_pc_interval = (int64_t)(GCONF._ob_plan_cache_auto_flush_interval) / (1000 * 1000L); // second
    {
      // 在调用plan cache接口前引用plan资源前必须定义guard
      observer::ObReqTimeGuard req_timeinfo_guard;

      run_plan_cache_task();
      run_ps_cache_task();
      if (NEED_AUTO_FLUSH_PC(auto_flush_pc_interval)
        && 0 == run_task_counter_ % auto_flush_pc_interval) {
        run_auto_flush_plan_cache_task();
      }
      SQL_PC_LOG(INFO, "schedule next cache evict task",
                "evict_interval", (int64_t)(GCONF.plan_cache_evict_interval));
    }
    // free cache obj in deleted map
    if (get_plan_cache_gc_strategy() > 0) {
      observer::ObReqTimeGuard req_timeinfo_guard;
      run_free_cache_obj_task();
    }
    SQL_PC_LOG(INFO, "schedule next cache evict task",
               "evict_interval", (int64_t)(GCONF.plan_cache_evict_interval));
    if (OB_FAIL(TG_SCHEDULE(plan_cache_manager_->tg_id_, *this,
                            GCONF.plan_cache_evict_interval,
                            false))) {
      SQL_PC_LOG(WARN, "Schedule new elimination failed", K(ret));
    }
  }
}

void ObPlanCacheManager::ObPlanCacheEliminationTask::run_auto_flush_plan_cache_task()
{
  if (OB_ISNULL(plan_cache_manager_)) {
    // do nothing
  } else {
    IGNORE_RETURN plan_cache_manager_->flush_all_plan_cache();
  }
}

void ObPlanCacheManager::ObPlanCacheEliminationTask::run_plan_cache_task()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_ISNULL(plan_cache_manager_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "plan_cache_manager not inited", K(ret));
  } else if (OB_FAIL(plan_cache_manager_->pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_id_array.count(); i++) { //循环忽略错误码,保证所有plan cache启动淘汰
      uint64_t tenant_id = tenant_id_array.at(i);
      MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

      // 由于 tenat_id_array的tenant移除 与 drop tenant存在一定时延。因此，在这里需要检查一下tenant是否存在。
      // bug fix:
      if (!GCTX.omt_->is_available_tenant(tenant_id)) {
        // do nothing
      } else if (OB_FAIL(guard.switch_to(tenant_id))) {
        ret = OB_SUCCESS;
        LOG_DEBUG("switch tenant fail", K(tenant_id));
      } else {
        ObPlanCache *plan_cache = plan_cache_manager_->get_plan_cache(tenant_id);
        if (NULL != plan_cache) {
          if (OB_FAIL(plan_cache->update_memory_conf())) { //如果失败, 则不更新设置, 也不影响其他流程
            SQL_PC_LOG(WARN, "fail to update plan cache memory sys val", K(ret));
          }
          if (OB_FAIL(plan_cache->cache_evict())) {
            SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
          }
          plan_cache->dec_ref_count();
        }
      }
    }
  }
}

void ObPlanCacheManager::ObPlanCacheEliminationTask::run_ps_cache_task()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_ISNULL(plan_cache_manager_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "plan_cache_manager not inited", K(ret));
  } else if (OB_FAIL(plan_cache_manager_->ps_pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_id_array.count(); i++) { //循环忽略错误码,保证所有ps cache启动淘汰
      uint64_t tenant_id = tenant_id_array.at(i);
      ObPCMemPctConf conf;
      ObPsCache *ps_cache = plan_cache_manager_->get_ps_cache(tenant_id); // inc ps_cache ref
      ObPlanCache *plan_cache = plan_cache_manager_->get_plan_cache(tenant_id);
      if (NULL != plan_cache) {
        conf.limit_pct_ = plan_cache->get_mem_limit_pct();
        conf.high_pct_ = plan_cache->get_mem_high_pct();
        conf.low_pct_ = plan_cache->get_mem_low_pct();
      }
      if (NULL != ps_cache) {
        ps_cache->set_mem_conf(conf);
        ps_cache->cache_evict();
        ps_cache->dec_ref_count();
      }
    }
  }
}

void ObPlanCacheManager::ObPlanCacheEliminationTask::run_free_cache_obj_task()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;

  ObGetAllCacheKeyOp op(&tenant_id_array);
  int64_t safe_timestamp = INT64_MAX;
  if (OB_ISNULL(plan_cache_manager_)) {
    ret = OB_NOT_INIT;
    SQL_PC_LOG(WARN, "plan_cache_manager not inited", K(ret));
  } else if (OB_FAIL(plan_cache_manager_->pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "failed to traverse pcm", K(ret));
  } else if (observer::ObGlobalReqTimeService::get_instance()
                         .get_global_safe_timestamp(safe_timestamp)) {
    SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
  } else {
    for (int i = 0; i < tenant_id_array.count(); i++) {
      uint64_t tenant_id = tenant_id_array.at(i);
      // inc plan cache ref
      ObPlanCache *plan_cache = plan_cache_manager_->get_plan_cache(tenant_id);
      if (NULL != plan_cache) {
        ObArray<AllocCacheObjInfo> deleted_objs;
        MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

        if (!GCTX.omt_->is_available_tenant(tenant_id)) {
          // do nothing
        } else if (OB_FAIL(guard.switch_to(tenant_id))) {
          ret = OB_SUCCESS;
          LOG_DEBUG("switch tenant fail", K(tenant_id));
        } else if (OB_FAIL(plan_cache->dump_deleted_objs<DUMP_ALL>(deleted_objs, safe_timestamp))) {
          SQL_PC_LOG(WARN, "failed to traverse hashmap", K(ret));
        } else {
          int64_t tot_mem_used = 0;
          for (int k = 0; k < deleted_objs.count(); k++) {
            tot_mem_used += deleted_objs.at(k).mem_used_;
          } // end for
          if (tot_mem_used >= ((plan_cache->get_mem_limit() / 100) * 30)) {
            LOG_ERROR("Cache Object Memory Leaked Much!!!", K(tot_mem_used),
                      K(plan_cache->get_mem_limit()), K(deleted_objs),
                      K(tenant_id), K(safe_timestamp));
          } else if (deleted_objs.count() > 0) {
            LOG_WARN("Cache Object Memory Leaked Much!!!", K(deleted_objs), K(tenant_id), 
                     K(safe_timestamp), K(plan_cache->get_mem_limit()));
          }
#if defined(NDEBUG)
          LOG_DEBUG("AUTO Mode is not active for now");
#else
          if (AUTO == get_plan_cache_gc_strategy()) {
            for (int j = 0; j < deleted_objs.count(); j++) {
              if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                                  deleted_objs.at(j).obj_id_,
                                                                  plan_cache))) {
                LOG_WARN("failde to destroy cache obj", K(ret));
              }
            }  // end inner loop
          }
#endif
        }
#if !defined(NDEBUG)
        if (OB_SUCC(ret) && OB_FAIL(plan_cache->dump_all_objs())) {
          LOG_WARN("failed to dump deleted map", K(ret));
        }
#endif
        if(NULL != plan_cache) {
          plan_cache->dec_ref_count();
          plan_cache = NULL;
        }
      }
    } // end outter loop
  }
}

int ObPlanCacheManager::evict_plan_by_table_name(uint64_t tenant_id, uint64_t database_id, ObString tab_name)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  if (NULL != plan_cache) {
    if (OB_FAIL(plan_cache->evict_plan_by_table_name(tenant_id, database_id, tab_name))) {
      SQL_PC_LOG(WARN, "fail to evict plan by table name", K(ret));
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}

int ObPlanCacheManager::flush_plan_cache_by_sql_id(uint64_t tenant_id,
                                                   uint64_t db_id,
                                                   common::ObString sql_id) {
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  if (NULL != plan_cache) {
    if (OB_FAIL(plan_cache->cache_evict_plan_by_sql_id(db_id, sql_id))) {
      SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
    }
    ObArray<AllocCacheObjInfo> deleted_objs;
    int64_t safe_timestamp = INT64_MAX;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                          .get_global_safe_timestamp(safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
    } else if (OB_FAIL(plan_cache->dump_deleted_objs<DUMP_SQL>(deleted_objs, safe_timestamp))) {
      SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
    } else {
      LOG_INFO("Deleted Cache Objs", K(deleted_objs));
      for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
        if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                            deleted_objs.at(i).obj_id_,
                                                            plan_cache))) {
            LOG_WARN("failed to destroy cache obj", K(ret));
        }
      }
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}

int ObPlanCacheManager::flush_all_plan_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_FAIL(pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  }
  for (int64_t i = 0; i < tenant_id_array.count(); i ++) { //ignore ret
    uint64_t tenant_id = tenant_id_array.at(i);
    if (OB_FAIL(flush_plan_cache(tenant_id))) {
      SQL_PC_LOG(ERROR, "plan cache evict failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObPlanCacheManager::flush_all_lib_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_FAIL(pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  }
  for (int64_t i = 0; i < tenant_id_array.count(); i ++) { //ignore ret
    uint64_t tenant_id = tenant_id_array.at(i);
    if (OB_FAIL(flush_lib_cache(tenant_id))) {
      SQL_PC_LOG(ERROR, "lib cache evict failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObPlanCacheManager::flush_all_lib_cache_by_ns(ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (ns <= ObLibCacheNameSpace::NS_INVALID || ns >= ObLibCacheNameSpace::NS_MAX) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "invalid namespace type", K(ns));
  } else if (OB_FAIL(pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  }
  for (int64_t i = 0; i < tenant_id_array.count(); i ++) { //ignore ret
    uint64_t tenant_id = tenant_id_array.at(i);
    if (OB_FAIL(flush_lib_cache_by_ns(tenant_id, ns))) {
      SQL_PC_LOG(ERROR, "lib cache evict failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObPlanCacheManager::flush_all_pl_cache()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_FAIL(pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  }
  for (int64_t i = 0; i < tenant_id_array.count(); i++) {
    uint64_t tenant_id = tenant_id_array.at(i);
    if (OB_FAIL(flush_pl_cache(tenant_id))) {
      SQL_PC_LOG(ERROR, "pl cache evict failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObPlanCacheManager::flush_pl_cache(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  int64_t safe_timestamp = INT64_MAX;
  ObArray<AllocCacheObjInfo> deleted_objs;
  if (NULL != plan_cache) {
    if (OB_FAIL(pl::ObPLCacheMgr::cache_evict_all_pl(plan_cache))) {
      SQL_PC_LOG(ERROR, "PL cache evict failed, please check", K(ret));
    } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                           .get_global_safe_timestamp(safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
    } else if (OB_FAIL(plan_cache->dump_deleted_objs<DUMP_PL>(deleted_objs, safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to dump deleted pl objs", K(ret));
    } else {
      LOG_INFO("Deleted Cache Objs", K(deleted_objs));
      for (int i = 0; i < deleted_objs.count(); i++) {  // ignore error code and continue
        if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                            deleted_objs.at(i).obj_id_,
                                                            plan_cache))) {
          LOG_WARN("failed to destroy cache obj", K(ret));
        }
      }
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}

int ObPlanCacheManager::flush_all_ps_cache()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;
  ObGetAllCacheKeyOp op(&tenant_id_array);
  if (OB_FAIL(pcm_.foreach_refactored(op))) {
    SQL_PC_LOG(ERROR, "fail to traverse pcm", K(ret));
  }
  for (int64_t i = 0; i < tenant_id_array.count(); i++) {
    uint64_t tenant_id = tenant_id_array.at(i);
    if (OB_FAIL(flush_ps_cache(tenant_id))) {
      SQL_PC_LOG(ERROR, "ps cache evict failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

int ObPlanCacheManager::flush_ps_cache(const uint64_t tenant_id) 
{
  int ret = OB_SUCCESS;
  ObPsCache *ps_cache = get_ps_cache(tenant_id);

  if (NULL != ps_cache) {
    if (OB_FAIL(ps_cache->cache_evict_all_ps())) {
      SQL_PC_LOG(ERROR, "ps cache evict failed, please check", K(ret)); 
    }
    ps_cache->dec_ref_count(); 
  }
  return ret;
}

int ObPlanCacheManager::flush_lib_cache_by_ns(const uint64_t tenant_id, const ObLibCacheNameSpace ns)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  int64_t safe_timestamp = INT64_MAX;
  ObArray<AllocCacheObjInfo> deleted_objs;
  if (NULL != plan_cache) {
    if (OB_FAIL(plan_cache->cache_evict_by_ns(ns))) {
      SQL_PC_LOG(ERROR, "cache evict by ns failed, please check", K(ret));
    } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance()
                           .get_global_safe_timestamp(safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
    } else if (OB_FAIL(plan_cache->dump_deleted_objs_by_ns(deleted_objs, safe_timestamp, ns))) {
      SQL_PC_LOG(ERROR, "failed to dump deleted objs by ns", K(ret));
    } else {
      LOG_INFO("Deleted Cache Objs", K(deleted_objs));
      for (int i = 0; i < deleted_objs.count(); i++) {  // ignore error code and continue
        if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                            deleted_objs.at(i).obj_id_,
                                                            plan_cache))) {
          LOG_WARN("failed to destroy cache obj", K(ret));
        }
      }
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}

int ObPlanCacheManager::flush_lib_cache(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  if (NULL != plan_cache) {
    if (OB_FAIL(plan_cache->cache_evict_all_obj())) {
      SQL_PC_LOG(ERROR, "lib cache evict failed, please check", K(ret));
    }
    ObArray<AllocCacheObjInfo> deleted_objs;
    int64_t safe_timestamp = INT64_MAX;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance().get_global_safe_timestamp(safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
    } else if (OB_FAIL(plan_cache->dump_deleted_objs<DUMP_ALL>(deleted_objs, safe_timestamp))) {
      SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
    } else {
      LOG_INFO("Deleted Cache Objs", K(deleted_objs));
      for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
        if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                            deleted_objs.at(i).obj_id_,
                                                            plan_cache))) {
            LOG_WARN("failed to destroy cache obj", K(ret));
        }
      }
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}

int ObPlanCacheManager::flush_plan_cache(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPlanCache *plan_cache = get_plan_cache(tenant_id);
  if (NULL != plan_cache) {
    if (OB_FAIL(plan_cache->cache_evict_all_plan())) {
      SQL_PC_LOG(ERROR, "Plan cache evict failed, please check", K(ret));
    }
    ObArray<AllocCacheObjInfo> deleted_objs;
    int64_t safe_timestamp = INT64_MAX;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(observer::ObGlobalReqTimeService::get_instance().get_global_safe_timestamp(safe_timestamp))) {
      SQL_PC_LOG(ERROR, "failed to get global safe timestamp", K(ret));
    } else if (OB_FAIL(plan_cache->dump_deleted_objs<DUMP_SQL>(deleted_objs, safe_timestamp))) {
      SQL_PC_LOG(WARN, "failed to get deleted sql objs", K(ret));
    } else {
      LOG_INFO("Deleted Cache Objs", K(deleted_objs));
      for (int64_t i = 0; i < deleted_objs.count(); i++) { // ignore error code and continue
        if (OB_FAIL(ObCacheObjectFactory::destroy_cache_obj(true,
                                                            deleted_objs.at(i).obj_id_,
                                                            plan_cache))) {
            LOG_WARN("failed to destroy cache obj", K(ret));
        }
      }
    }
    plan_cache->dec_ref_count();
  }
  return ret;
}


int ObPlanCacheManager::revert_ps_cache(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObPsCache *ppc = NULL;
  observer::ObReqTimeGuard req_timeinfo_guard;
  int tmp_ret = ps_pcm_.erase_refactored(tenant_id, &ppc);
  if (OB_SUCCESS == tmp_ret && NULL != ppc) {
    SQL_PC_LOG(INFO, "plan_cache_manager revert ps plan cache",
               "pc ref_count", ppc->get_ref_count(),
               K(tenant_id));
    //cancel scheduled task
    ppc->dec_ref_count();
  } else if (OB_HASH_NOT_EXIST == tmp_ret) { // maybe erase by other thread
    SQL_PC_LOG(INFO, "PS Plan Cache not exist", K(tenant_id));
  } else {
    ret = tmp_ret;
    SQL_PC_LOG(ERROR, "unexpected error when erase plan cache",
              K(tenant_id), K(ppc), K(ret));
  }
  return ret;
}

int ObPlanCacheManager::get_plan_cache_gc_strategy()
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

} // end of namespace sql
} // end of namespace oceanbase

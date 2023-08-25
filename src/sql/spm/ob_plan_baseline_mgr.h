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

#ifndef OB_SQL_SPM_OB_PLAN_BASELINE_MGR_H_
#define OB_SQL_SPM_OB_PLAN_BASELINE_MGR_H_
#include "sql/spm/ob_spm_define.h"
#include "sql/spm/ob_plan_baseline_sql_service.h"
#include "lib/task/ob_timer.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "sql/monitor/ob_exec_stat.h"


namespace oceanbase
{
namespace sql
{
class ObPlanBaselineMgr;
class ObPlanCacheCtx;
class ObPhysicalPlan;

class SpmTmpAllocatorGuard
{
public:
  SpmTmpAllocatorGuard()
  : mem_context_(nullptr),
    inited_(false) {}
  ~SpmTmpAllocatorGuard();
  int init(const uint64_t tenant_id);
  ObIAllocator* get_allocator();
private:
  lib::MemoryContext mem_context_;
  bool inited_;
};
class ObPlanBaselineRefreshTask : public common::ObTimerTask
{
public:
  ObPlanBaselineRefreshTask() : baseline_mgr_(NULL) {}
  void runTimerTask(void);
private:
  // void run_plan_cache_task();
  // void run_free_cache_obj_task();
public:
  const static int64_t REFRESH_INTERVAL = 15L * 60L * 1000L * 1000L; // 15 min
  ObPlanBaselineMgr* baseline_mgr_;
};

typedef ObList<EvoResultUpdateTask*, ObIAllocator> task_result_list;

class ObPlanBaselineMgr
{
  friend class ObPlanBaselineRefreshTask;
public:
  ObPlanBaselineMgr()
  : inner_allocator_(NULL),
    inited_(false),
    destroyed_(false),
    tg_id_(-1),
    last_sync_time_(-1),
    trl_(nullptr) {}
  ~ObPlanBaselineMgr();
  static int mtl_init(ObPlanBaselineMgr* &node_list);
  static void mtl_stop(ObPlanBaselineMgr* &node_list);
  static void mtl_wait(ObPlanBaselineMgr* &node_list);
  void destroy();
  int create_list();
  void destroy_list(task_result_list*& list);
  int swap_list(task_result_list*& old_list);
  int add_list(EvolutionTaskResult& result);
  int check_baseline_exists(ObSpmCacheCtx& spm_ctx,
                            const uint64_t plan_hash,
                            bool& is_exists,
                            bool& need_add_baseline);
  int get_best_baseline(ObSpmCacheCtx& spm_ctx,
                        ObCacheObjGuard& obj_guard);
  int add_baseline(ObSpmCacheCtx& spm_ctx,
                   ObPlanCacheCtx& pc_ctx,
                   ObPhysicalPlan* plan);
  int update_plan_baseline_statistic(EvolutionTaskResult& result);
  int accept_new_plan_baseline(ObSpmCacheCtx& spm_ctx, const ObAuditRecordData &audit_record);
  int force_accept_new_plan_baseline(ObSpmCacheCtx& spm_ctx, uint64_t plan_hash, const bool with_plan_hash);
  int sync_baseline_from_inner_table();
  int sync_baseline_from_server();

  int alter_plan_baseline(const uint64_t tenant_id,
                          const uint64_t database_id,
                          AlterPlanBaselineArg& arg,
                          int64_t &baseline_affected);
  int spm_configure(const uint64_t tenant_id, const uint64_t database_id, const ObString& param_name, const int64_t& param_value);
  int drop_plan_baseline(const uint64_t tenant_id,
                         const uint64_t database_id,
                         const ObString &sql_id,
                         const uint64_t plan_hash,
                         const bool with_plan_hash,
                         int64_t &baseline_affected);
  int load_baseline(ObBaselineKey &key, ObPhysicalPlan* plan, const bool fixed, const bool enabled);
  int purge_baselines(const uint64_t tenant_id, int64_t baseline_affected);
  int evict_plan_baseline(ObSpmCacheCtx& spm_ctx);
  int check_evolution_task();
private:
  int init(uint64_t tenant_id);
  int init_mem_context(uint64_t tenant_id);
private:
  lib::MemoryContext mem_context_;
  common::ObIAllocator* inner_allocator_;
  ObPlanBaselineSqlService sql_service_;
  ObPlanBaselineRefreshTask refresh_task_;
  bool inited_;
  bool destroyed_;
  uint64_t tenant_id_;
  int tg_id_;
  int64_t last_sync_time_;
  task_result_list* trl_;
  common::SpinRWLock lock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanBaselineMgr);
};


} // namespace sql end
} // namespace oceanbase end
#endif

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
#include "share/ob_rpc_struct.h"

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

class ObSpmBaselineLoader
{
public:
  ObSpmBaselineLoader()
  : tenant_id_(0),
    alloc_guard_(),
    baseline_count_(0),
    origin_(0),
    flags_(0)
   {}
  virtual ~ObSpmBaselineLoader()  {}
  int init_baseline_loader(const obrpc::ObLoadPlanBaselineArg &arg);
  int add_one_plan_baseline(const ObPhysicalPlan &plan, bool &added);
  int get_baseline_item_dml(ObSqlString &item_dml);
  int get_baseline_info_dml(ObSqlString &info_dml);
  inline uint64_t get_baseline_count() { return baseline_count_; }
  ObIAllocator *get_allocator() { return alloc_guard_.get_allocator();  }
private:
  uint64_t tenant_id_;
  SpmTmpAllocatorGuard alloc_guard_;
  share::ObDMLSqlSplicer item_dml_splicer_;
  share::ObDMLSqlSplicer info_dml_splicer_;
  uint64_t baseline_count_;
  // info same as ObPlanBaselineItem
  int64_t origin_;  // baseline source, 1 for AUTO-CAPTURE, 2 for MANUAL-LOAD
  common::ObString db_version_;  // database version when generate baseline
  int64_t flags_;
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

typedef ObList<EvolutionTaskResult*, ObIAllocator> task_result_list;

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
                            bool& is_accepted,
                            bool& is_fixed);
  int get_best_baseline(ObSpmCacheCtx& spm_ctx,
                        ObCacheObjGuard& obj_guard);
  int add_unaccepted_baseline(ObSpmCacheCtx& spm_ctx,
                              ObPlanCacheCtx& pc_ctx,
                              ObPhysicalPlan* plan);
  int add_unaccepted_baseline(ObSpmCacheCtx &spm_ctx,
                              const ObPhysicalPlan *plan,
                              ObCacheObjGuard &guard);
  int check_and_update_plan_baseline(ObPlanCacheCtx &pc_ctx, ObPhysicalPlan &plan);
  int update_plan_baseline_statistic(const ObPhysicalPlan *evo_plan,
                                     EvolutionTaskResult& result);
  int update_statistic_for_baseline(const ObPhysicalPlan *evo_plan,
                                    EvolutionTaskResult& result);
  static int update_baseline_item(const ObEvolutionStat &stat,
                                  const bool is_verified,
                                  ObPlanBaselineItem *item);
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
                         const uint64_t parallel,
                         int64_t &baseline_affected);
  int load_baseline(ObSpmBaselineLoader &baseline_loader);
  int purge_baselines(const uint64_t tenant_id, int64_t &baseline_affected);
  int handle_spm_evo_record(const uint64_t tenant_id);
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

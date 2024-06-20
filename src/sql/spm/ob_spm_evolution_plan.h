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

#ifndef OCEANBASE_SQL_SPM_OB_SPM_EVOLUTION_PLAN_
#define OCEANBASE_SQL_SPM_OB_SPM_EVOLUTION_PLAN_

#include "lib/random/ob_random.h"
#include "sql/ob_sql_define.h"
#include "sql/plan_cache/ob_plan_cache.h"

namespace oceanbase
{
namespace sql
{
class ObSqlPlanSet;
class ObPhysicalPlan;
class ObPlanCacheCtx;

typedef int32_t EvolutionPercentage;

class ObRefLockGuard
{
public:
  ObRefLockGuard(common::SpinRWLock &lock, bool is_wrlock)
    : ref_lock_(lock)
  {
    if (is_wrlock) {
      (void)ref_lock_.wrlock();
    } else {
      (void)ref_lock_.rdlock();
    }
  }
  ~ObRefLockGuard()
  {
    (void)ref_lock_.unlock();
  }

private:
  common::SpinRWLock &ref_lock_;
};

class ObEvolutionPlan
{
public:
  enum EvolutionPlanType
  {
    INVALID_PLAN_TYPE,
    EVOLVING_PLAN_TYPE,
    BASELINE_PLAN_TYPE,
    ALL_PLAN_TYPE
  };

  ObEvolutionPlan()
    : plan_set_(NULL),
      ref_lock_(common::ObLatchIds::PLAN_SET_LOCK),
      is_evolving_(false),
      evolution_count_(0),
      start_evolution_time_(0),
      evolution_count_ts_(DEFAULT_EVOLUTION_COUNT_THRESHOLD),
      evolution_timeout_ts_(DEFAULT_EVOLUTION_TIMEOUT_THRESHOLD),
      evolving_plan_(NULL),
      history_exec_time_(0),
      is_inited_(false),
      current_stage_cnt_(0),
      lazy_finished_(false),
      lazy_better_(false)
  {
  }
  void reset();
  int remove_all_plan();
  int init(ObSqlPlanSet *plan_set);
  int init(ObSqlPlanSet *plan_set,
           const int64_t evolution_count_ts,
           const int64_t evolution_timeout_ts);
  void disable_is_evolving_flag() { ATOMIC_STORE(&is_evolving_, false); }
  void enable_is_evolving_flag() { ATOMIC_STORE(&is_evolving_, true); }
  bool get_is_evolving_flag() const { return ATOMIC_LOAD(&is_evolving_); }
  int64_t get_mem_size();
  int64_t get_evolution_count() const { return evolution_count_; }
  int64_t get_start_evolution_time() const { return start_evolution_time_; }
  int64_t get_evolution_count_ts() const { return evolution_count_ts_; }
  int64_t get_evolution_timeout_ts() const { return evolution_timeout_ts_; }
  // EvolutionPercentage get_evolution_percentage() const { return percentage_; }
  const ObPhysicalPlan *get_evolving_plan() const { return evolving_plan_; }
  const ObIArray<ObPhysicalPlan *> &get_baseline_plans() const { return baseline_plans_; }
  void set_evolution_count_ts(const int64_t threshold)
  {
    evolution_count_ts_ = threshold;
  }
  void set_evolution_timeout_ts(const int64_t threshold)
  {
    evolution_timeout_ts_ = threshold;
  }
  int get_plan(ObPlanCacheCtx &ctx, ObPhysicalPlan *&plan);
  int add_plan(ObPlanCacheCtx &ctx, ObPhysicalPlan *plan);
  void inc_evolution_finish_count();
  void check_task_need_finish();
  int compare_and_lazy_finalize_plan();
  TO_STRING_KV(K_(is_evolving),
               K_(evolution_count),
               K_(start_evolution_time),
               K_(evolution_count_ts),
               K_(evolution_timeout_ts),
               K_(baseline_plans),
               KPC_(evolving_plan));
  
private:
  // static const EvolutionPercentage DEFAULT_EVOLUTION_PERCENTAGE = 50;
  static const int64_t DEFAULT_EVOLUTION_COUNT_THRESHOLD = 150;
  static const int64_t DEFAULT_EVOLUTION_TIMEOUT_THRESHOLD = 3 * 60 * 60 * 1000L * 1000L; //3 hours
  static const int64_t DEFAULT_ERROR_COUNT_THRESHOLD = 3;

  int get_plan_with_guard(ObPlanCacheCtx &ctx,
                          ObPhysicalPlan *&plan);
  int add_baseline_plan(ObPlanCacheCtx &ctx,
                        ObPhysicalPlan &plan);
  int add_evolving_plan(ObPlanCacheCtx &ctx,
                        ObPhysicalPlan &plan);
  int get_baseline_plan(ObPlanCacheCtx &ctx,
                        ObPhysicalPlan *&plan);
  int get_evolving_plan(ObPlanCacheCtx &ctx,
                        ObPhysicalPlan *&plan);
  int compare_and_finalize_plan(ObPlanCacheCtx &ctx,
                                ObPhysicalPlan *&plan);
  // bool need_evolution_plan_this_time();
  bool need_evolution_plan_this_time(const int64_t evolution_count);
  // Use the hash value of the plan to determine whether it is the same plan
  int is_same_plan(const ObPhysicalPlan *l_plan,
                   const ObPhysicalPlan *r_plan,
                   bool &is_same) const;
  int is_evolving_plan_better(bool &is_better) const;
  int process_plan_evolution_result(const bool is_better);
  void discard_evolving_plan_add_baseline_plan_to_pc(ObPlanCacheCtx &ctx);
  void discard_baseline_plan_add_evolving_plan_to_pc(ObPlanCacheCtx &ctx);
  int discard_all_plan_by_type(EvolutionPlanType plan_type, bool evict_plan = false);
  int add_evolving_plan_to_pc(ObPlanCacheCtx &ctx);
  int add_all_baseline_plans_to_pc(ObPlanCacheCtx &ctx);
  void reset_evolution_plan_info();
  void get_evolution_count_and_calc_dynamic_exec_count(int64_t &evolution_count);
  int get_final_plan_and_discard_other_plan(const bool need_evolving_plan,
                                            ObPlanCacheCtx &ctx,
                                            ObPhysicalPlan *&plan);
  int64_t get_baseline_plan_error_cnt();
  int64_t get_plan_finish_cnt();

protected:
  ObSqlPlanSet *plan_set_;
  common::SpinRWLock ref_lock_;
  bool is_evolving_;
  int64_t evolution_count_;
  int64_t start_evolution_time_;
  int64_t evolution_count_ts_;
  int64_t evolution_timeout_ts_;
  // ObRandom random_;
  // EvolutionPercentage percentage_;
  ObPhysicalPlan *evolving_plan_;
  common::ObSEArray<ObPhysicalPlan *, 4> baseline_plans_; 
  common::ObSEArray<ObPhysicalPlan *, 4> garbage_list_;
  int64_t history_exec_time_;
  bool is_inited_;
  bool is_evo_best_plan_;
  int64_t best_plan_cnt_;
  int64_t current_stage_cnt_;
  bool lazy_finished_;
  bool lazy_better_;
};

} //namespace sql end
} //namespace oceanbase end

#endif

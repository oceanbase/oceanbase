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

#ifndef _OB_DIST_PLANS_H
#define _OB_DIST_PLANS_H

#include "lib/container/ob_se_array.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_pwj_comparer.h"

namespace oceanbase {
namespace sql {
struct ObPlanCacheCtx;
class ObPhysicalPlan;
class ObTableLocation;
class ObPhyTableLocation;
class ObCandiTableLoc;
class ObSqlPlanSet;

class ObDistPlans {
public:
  ObDistPlans()
    : plan_set_(NULL)
  { }

  /**
   * @brief init a ObDistPlans
   */
  int init(ObSqlPlanSet *ps);

  /**
   * @brief add a plan to dist plan list
   *
   * @param plan distribute physical plan to add
   * @param pc_ctx plan cache context
   */
  int add_plan(ObPhysicalPlan &plan,
               ObPlanCacheCtx &pc_ctx);

  int add_evolution_plan(ObPhysicalPlan &plan, ObPlanCacheCtx &pc_ctx);

  /**
   * @brief get plan from dist plan list
   *
   * @param pc_ctx plan cache contex
   * @retval plan physical plan returned if matched
   */
  int get_plan(ObPlanCacheCtx &pc_ctx,
               ObPhysicalPlan *&plan);

  /**
   * @brief remove all the plans the corresponding plan stats
   *
   */
  int remove_all_plan();

  /**
   * @brief return the memory used by dist plan list
   *
   * @return memory size
   */
  int64_t get_mem_size() const;

  /**
   * @brief remove all the plan stats
   *
   */
  int remove_plan_stat();

  /**
   * @brief get # of plans
   *
   */
  int64_t count() const { return dist_plans_.count(); }

  /**
   * @brief reset ob dist plan list
   *
   */
  void reset()
  {
    dist_plans_.reset();
    plan_set_ = nullptr;
  }

  // get first plan in the array if possible
  ObPhysicalPlan* get_first_plan();

private:
  int is_same_plan(const ObPhysicalPlan *l_plan, const ObPhysicalPlan *r_plan,
                   bool &is_same) const;

  /**
   * @brief 为pc_ctx.exec_ctx设置table location
   *
   */
  int set_phy_table_locations_for_ctx(ObPlanCacheCtx &pc_ctx,
                                      const ObIArray<ObPhyTableLocation> &table_locations);

private:
  common::ObSEArray<ObPhysicalPlan *, 4> dist_plans_;
  ObSqlPlanSet *plan_set_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistPlans);
};

} // namespace sql
} // namespace oceanbase

#endif /* _OB_DIST_PLANS_H */

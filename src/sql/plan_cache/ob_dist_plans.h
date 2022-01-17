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
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_pwj_comparer.h"

namespace oceanbase {
namespace sql {
class ObPlanCacheCtx;
class ObPhysicalPlan;
class ObTableLocation;
class ObPhyTableLocation;
class ObPhyTableLocationInfo;
class ObSqlPlanSet;

class ObDistPlans {
public:
  ObDistPlans() : plan_set_(NULL), should_get_plan_directly_(false)
  {}

  /**
   * @brief init a ObDistPlans
   */
  int init(ObSqlPlanSet* ps);

  /**
   * @brief add a plan to dist plan list
   *
   * @param is_single_table if there is only a single involved in dist plan,
   * only one plan is allowed in dist plan lists
   * @param plan distribute physical plan to add
   * @param pc_ctx plan cache context
   */
  int add_plan(const bool is_single_table, const uint64_t try_flags, ObPhysicalPlan& plan, ObPlanCacheCtx& pc_ctx);

  /**
   * @brief get plan from dist plan list
   *
   * @param pc_ctx plan cache contex
   * @retval plan physical plan returned if matched
   */
  int get_plan(ObPlanCacheCtx& pc_ctx, const uint64_t try_flags, share::ObIPartitionLocationCache& location_cache_used,
      ObPhysicalPlan*& plan);

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
  int64_t count() const
  {
    return dist_plans_.count();
  }

  /**
   * @brief reset ob dist plan list
   *
   */
  void reset()
  {
    dist_plans_.reset();
    should_get_plan_directly_ = false;
    plan_set_ = nullptr;
  }

  // get first plan in the array if possible
  ObPhysicalPlan* get_first_plan();

private:
  /**
   * @brief check if loc constraint can meet in current loc cache.
   * if yes, return the plan
   */
  int match(const ObPlanCacheCtx& pc_ctx, const ObPhysicalPlan* plan, share::ObIPartitionLocationCache& location_cache,
      ObIArray<ObPhyTableLocation>& out_phy_tbl_locs, bool& is_matched) const;

  int is_same_plan(const ObPhysicalPlan* l_plan, const ObPhysicalPlan* r_plan, bool& is_same) const;

  /**
   * @brief retrieve table locations according to ObLocationConstraint info,
   * from cached table location in Physical Plan
   *
   */
  int calc_table_locations(const ObIArray<LocationConstraint>& loc_cons,
      const common::ObIArray<ObTableLocation>& in_tbl_locations, share::ObIPartitionLocationCache& location_cache,
      const ObPlanCacheCtx& pc_ctx, common::ObIArray<ObTableLocation>& out_tbl_locations,
      common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos) const;

  int cmp_table_types(const ObIArray<LocationConstraint>& loc_cons, const common::ObAddr& server,
      const common::ObIArray<ObTableLocation>& tbl_locs, const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos,
      bool& is_same) const;

  /**
   * @brief check if location physically equal
   *
   */
  int check_inner_constraints_old(const common::ObIArray<ObPlanPwjConstraint>& strict_cons,
      const common::ObIArray<ObPlanPwjConstraint>& non_strict_cons,
      const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, bool& is_same) const;

  /**
   * @brief check if  location physically equal in pwj case and non-strict pwj case
   */
  int check_inner_constraints(const common::ObIArray<ObPlanPwjConstraint>& strict_cons,
      const common::ObIArray<ObPlanPwjConstraint>& non_strict_cons,
      const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, const ObPlanCacheCtx& pc_ctx,
      PWJPartitionIdMap& pwj_map, bool& is_same) const;

  /**
   * @brief Get table location with table location key
   *
   */
  int get_tbl_loc_with_key(const TableLocationKey key, const ObIArray<ObTableLocation>& table_locations,
      const ObTableLocation*& ret_loc_ptr) const;

  int match_tbl_partition_locs(
      const ObPhyTableLocationInfo& left, const ObPhyTableLocationInfo& right, bool& is_matched) const;

  /**
   * @brief set table location for pc_ctx.exec_ctx
   */
  int set_phy_table_locations_for_ctx(ObPlanCacheCtx& pc_ctx, const ObIArray<ObPhyTableLocation>& table_locations);

  int reselect_duplicate_table_best_replica(const ObIArray<LocationConstraint>& loc_cons, const common::ObAddr& server,
      const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos) const;

  int check_partition_constraint(const ObIArray<LocationConstraint>& loc_cons,
      const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, bool& is_match) const;

  /**
   * @brief check if meet pwj constraint
   */
  int64_t check_pwj_cons(const ObPlanCacheCtx& pc_ctx, const ObPlanPwjConstraint& pwj_cons,
      const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, ObIArray<PwjTable>& pwj_tables,
      ObPwjComparer& pwj_comparer, PWJPartitionIdMap& pwj_map, bool& is_same) const;

private:
  common::ObSEArray<ObPhysicalPlan*, 4> dist_plans_;
  ObSqlPlanSet* plan_set_;
  // optimize perf for single table distributed plan
  bool should_get_plan_directly_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistPlans);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_DIST_PLANS_H */

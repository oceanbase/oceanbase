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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_MATCH_HELPER_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_MATCH_HELPER_

#include "lib/container/ob_se_array.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_pwj_comparer.h"

namespace oceanbase {
namespace sql {
class ObPhysicalPlan;
class ObPlanCacheCtx;
class ObTableLocation;
class ObPhyTableLocation;
class ObPhyTableLocationInfo;
class ObSqlPlanSet;

class ObPlanMatchHelper
{
public:
  ObPlanMatchHelper(ObSqlPlanSet *plan_set)
    : plan_set_(plan_set)
  {
  }
  int match_plan(const ObPlanCacheCtx &pc_ctx,
                 const ObPhysicalPlan *plan,
                 bool &is_matched,
                 common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                 common::ObIArray<ObTableLocation> &out_tbl_locations) const;

  /**
   * @brief Get the table locations of all relational tables involved in LocationConstraint
   * from the table location cached by Physical Plan
   *
   */
  int calc_table_locations(
      const ObIArray<LocationConstraint> &loc_cons,
      const common::ObIArray<ObTableLocation> &in_tbl_locations,
      const ObPlanCacheCtx &pc_ctx,
      common::ObIArray<ObTableLocation> &out_tbl_locations,
      common::ObIArray<ObCandiTableLoc> &phy_tbl_infos) const;

private:
  /**
   * @brief Get table location with table location key
   *
   */
  int get_tbl_loc_with_key(
      const TableLocationKey key,
      const ObIArray<ObTableLocation> &table_locations,
      const ObTableLocation *&ret_loc_ptr) const;
  /**
   * @brief In the case of replicated tables, this is the only replica position adjustment
   * that occurs before matching begins. Otherwise, there is the possibility of repeated
   * adjustment of cmp_table_types and check_inner_constraints later. This can cause a piece
   * of information to become mismatched.
   *
   * All partitions prefer native. subsequent queries will benefit from
   * this even if the plan misses as a result
   *
   */
  int reselect_duplicate_table_best_replica(
      const ObIArray<LocationConstraint> &loc_cons,
      const common::ObAddr &server,
      const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
      const ObIArray<ObDupTabConstraint> &dup_table_replica_cons) const;
  /**
   * @brief Compare table location types
   *
   */
  int cmp_table_types(
      const ObIArray<LocationConstraint> &loc_cons,
      const common::ObAddr &server,
      const common::ObIArray<ObTableLocation> &tbl_locs,
      const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
      bool &is_same) const;
  /**
   * @brief Check only_one_first_part constraint and only_one_sub_part constraint
   * in base table constraints
   *
   */
  int check_partition_constraint(const ObPlanCacheCtx &pc_ctx,
                                 const ObIArray<LocationConstraint> &loc_cons,
                                 const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                                 bool &is_match) const;
  /**
   * @brief Check that tables in strict pwj constraints and non-strict pwj constraints
   * conform to physical partition consistent constraints. And save the mapping relationship
   * of partition_id of the base table with constraint relationship to pwj_map
   *
   */
  int check_inner_constraints(const common::ObIArray<ObPlanPwjConstraint>& strict_cons,
                              const common::ObIArray<ObPlanPwjConstraint>& non_strict_cons,
                              const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                              const ObPlanCacheCtx &pc_ctx,
                              PWJTabletIdMap &pwj_map,
                              bool &is_same) const;
  /**
   * @brief Check if strict pwj constraints are satisfied
   *
   */
  int check_strict_pwj_cons(const ObPlanCacheCtx &pc_ctx,
                            const ObPlanPwjConstraint &pwj_cons,
                            const ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                            ObStrictPwjComparer &pwj_comparer,
                            PWJTabletIdMap &pwj_map,
                            bool &is_same) const;
  /**
   * @brief Check if non-strict pwj constraints are satisfied
   *
   */
  int check_non_strict_pwj_cons(const ObPlanPwjConstraint &pwj_cons,
                                const ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                                ObNonStrictPwjComparer &pwj_comparer,
                                bool &is_same) const;
  /**
   * @brief Check table partition location
   *
   */
  int match_tbl_partition_locs(const ObCandiTableLoc &left,
                               const ObCandiTableLoc &right,
                               bool &is_matched) const;
private:
  ObSqlPlanSet *plan_set_;
};
} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_MATCH_HELPER_ */

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
#include "sql/plan_cache/ob_dist_plans.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {
int ObDistPlans::init(ObSqlPlanSet* ps)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    plan_set_ = ps;
  }
  return ret;
}

int ObDistPlans::get_plan(ObPlanCacheCtx& pc_ctx, const uint64_t try_flags,
    ObIPartitionLocationCache& location_cache_used, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  bool is_matched = false;

  LOG_DEBUG("Get Plan", K(dist_plans_.count()), K(try_flags), K(should_get_plan_directly_));
  if (OB_ISNULL(plan_set_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null plan_set_", K(ret));
  } else if (should_get_plan_directly_ && (0 == try_flags || plan_set_->is_multi_stmt_plan())) {
    // must be single table dist paln if it is multi stmt plan
    // single table should just return plan, do not match
    if (0 == dist_plans_.count()) {
      ret = OB_SQL_PC_NOT_EXIST;
      LOG_DEBUG("dist plan list is empty", K(ret), K(dist_plans_.count()));
    } else if (OB_ISNULL(dist_plans_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null plan", K(ret), K(dist_plans_.at(0)));
    } else {
      dist_plans_.at(0)->inc_ref_count(pc_ctx.handle_id_);
      plan = dist_plans_.at(0);
      is_matched = true;

      // fill table location for single plan using px
      // for single dist plan without px, we already fill the phy locations while calculating plan type
      // for multi table px plan, physical location is calculated in match step
      ObSEArray<ObPhyTableLocationInfo, 4> out_phy_tbl_loc_infos;
      ObSEArray<ObPhyTableLocation, 4> out_phy_tbl_locs;
      bool need_check_on_same_server = false;
      if (OB_ISNULL(plan_set_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null plan set", K(ret), K(plan_set_));
      } else if (!plan_set_->enable_inner_part_parallel()) {
        // do nothing
      } else if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(plan->get_table_locations(),
                     pc_ctx,
                     location_cache_used,
                     out_phy_tbl_loc_infos,
                     need_check_on_same_server))) {
        LOG_WARN("failed to get physical table locations", K(ret));
      } else if (OB_FAIL(out_phy_tbl_locs.prepare_allocate(out_phy_tbl_loc_infos.count()))) {
        LOG_WARN("failed to preparep allocate array", K(ret), K(out_phy_tbl_loc_infos.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < out_phy_tbl_loc_infos.count(); i++) {
          if (OB_FAIL(out_phy_tbl_locs.at(i).assign_from_phy_table_loc_info(out_phy_tbl_loc_infos.at(i)))) {
            LOG_WARN("failed to assign phy table location", K(ret));
          } else {
            // do nothing
          }
        }  // for end

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(set_phy_table_locations_for_ctx(pc_ctx, out_phy_tbl_locs))) {
          LOG_WARN("failed to set physical table locations for exec ctx", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }

  ObSEArray<ObPhyTableLocation, 4> out_phy_tbl_locs;

  for (int64_t i = 0; OB_SUCC(ret) && !is_matched && i < dist_plans_.count(); i++) {
    ObPhysicalPlan* tmp_plan = dist_plans_.at(i);
    if (OB_ISNULL(tmp_plan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(tmp_plan));
    } else if (OB_FAIL(match(pc_ctx, tmp_plan, location_cache_used, out_phy_tbl_locs, is_matched))) {
      LOG_WARN("fail to match dist plan", K(ret));
    } else if (is_matched) {
      tmp_plan->inc_ref_count(pc_ctx.handle_id_);
      if (OB_FAIL(set_phy_table_locations_for_ctx(pc_ctx, out_phy_tbl_locs))) {
        LOG_WARN("failed to set phy table locations for exec_ctx", K(ret));
      } else {
        plan = tmp_plan;
      }
    }
  }

  if (OB_SUCC(ret) && plan == NULL) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  return ret;
}

int ObDistPlans::add_plan(
    const bool is_single_table, const uint64_t try_flags, ObPhysicalPlan& plan, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  bool is_same = false;

  for (int64_t i = 0; OB_SUCC(ret) && !is_same && i < dist_plans_.count(); i++) {
    const ObPhysicalPlan* tmp_plan = dist_plans_.at(i);
    if (OB_FAIL(is_same_plan(tmp_plan, &plan, is_same))) {
      LOG_WARN("fail to check same plan", K(ret));
    } else if (false == is_same) {
      // do nothing
    } else {
      ret = OB_SQL_PC_PLAN_DUPLICATE;
    }
  }

  if (OB_SUCC(ret) && !is_same) {
    if (OB_FAIL(plan.set_location_constraints(pc_ctx.sql_ctx_.base_constraints_,
            pc_ctx.sql_ctx_.strict_constraints_,
            pc_ctx.sql_ctx_.non_strict_constraints_))) {
      LOG_WARN("failed to set location constraints",
          K(ret),
          K(plan),
          K(pc_ctx.sql_ctx_.base_constraints_),
          K(pc_ctx.sql_ctx_.strict_constraints_),
          K(pc_ctx.sql_ctx_.non_strict_constraints_));
    } else if (is_single_table && dist_plans_.count() >= 1) {
      // only one distributed plan is allowed for single table
      ret = OB_SQL_PC_PLAN_DUPLICATE;
      LOG_INFO("distributed plan already exists for single table query", K(ret), K(dist_plans_.count()));
    } else if (OB_FAIL(dist_plans_.push_back(&plan))) {
      LOG_WARN("fail to add plan", K(ret));
    } else {
      should_get_plan_directly_ = is_single_table && (0 == try_flags || plan.get_px_dop() > 1);
      LOG_DEBUG("should get plan without matching constraint",
          K(should_get_plan_directly_),
          K(is_single_table),
          K(try_flags),
          K(plan.get_px_dop()));
    }
  }

  return ret;
}

// check if same plan using plan's hash value
int ObDistPlans::is_same_plan(const ObPhysicalPlan* l_plan, const ObPhysicalPlan* r_plan, bool& is_same) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(l_plan) || OB_ISNULL(r_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_plan), K(r_plan));
  } else {
    is_same = (l_plan->get_signature() == r_plan->get_signature());
    LOG_DEBUG("compare plan", K(l_plan->get_signature()), K(r_plan->get_signature()), K(is_same));
  }

  return ret;
}

int ObDistPlans::reselect_duplicate_table_best_replica(const ObIArray<LocationConstraint>& loc_cons,
    const common::ObAddr& server, const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos) const
{
  int ret = OB_SUCCESS;
  if (loc_cons.count() == phy_tbl_infos.count()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_infos.count(); ++i) {
      const ObPhyTableLocationInfo& phy_tbl_info = phy_tbl_infos.at(i);
      if (phy_tbl_info.is_duplicate_table_not_in_dml()) {
        for (int64_t j = 0; OB_SUCC(ret) && j < phy_tbl_info.get_partition_cnt(); ++j) {
          const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_tbl_info.get_phy_part_loc_info_list().at(j);
          int64_t replica_idx = 0;
          if (phy_part_loc_info.is_server_in_replica(server, replica_idx)) {
            LOG_DEBUG("reselect replica index will happen", K(phy_tbl_info), K(replica_idx), K(server));
            if (OB_FAIL(
                    const_cast<ObPhyPartitionLocationInfo&>(phy_part_loc_info).set_selected_replica_idx(replica_idx))) {
              LOG_WARN("failed to set selected replica idx", K(ret), K(replica_idx));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDistPlans::match(const ObPlanCacheCtx& pc_ctx, const ObPhysicalPlan* plan,
    share::ObIPartitionLocationCache& location_cache, ObIArray<ObPhyTableLocation>& out_phy_tbl_locs,
    bool& is_matched) const
{
  int ret = OB_SUCCESS;
  bool has_duplicate_table = false;
  bool is_retrying = false;
  is_matched = true;
  const ObAddr& server = pc_ctx.exec_ctx_.get_addr();
  const ObIArray<LocationConstraint>& base_cons = plan->get_base_constraints();
  const ObIArray<ObPlanPwjConstraint>& strict_cons = plan->get_strict_constraints();
  const ObIArray<ObPlanPwjConstraint>& non_strict_cons = plan->get_non_strict_constraints();
  const ObIArray<ObTableLocation>& plan_tbl_locs = plan->get_table_locations();
  PWJPartitionIdMap pwj_map;
  bool use_pwj_map = false;
  out_phy_tbl_locs.reset();

  if (0 == base_cons.count()) {
    LOG_WARN("plan's location constraint should not be empty", K(plan), K(base_cons), K(pc_ctx));
  }
  if (OB_NOT_NULL(plan_set_) && PST_SQL_CRSR == plan_set_->get_type() &&
      static_cast<ObSqlPlanSet*>(plan_set_)->has_duplicate_table()) {
    if (OB_FAIL(pc_ctx.is_retry(is_retrying))) {
      LOG_WARN("failed to test if retrying", K(ret));
    } else if (is_retrying) {
      has_duplicate_table = false;
    } else {
      has_duplicate_table = true;
    }
    LOG_DEBUG("contain duplicate table", K(has_duplicate_table), K(is_retrying));
  }

  if (OB_SUCC(ret)) {
    // check base table constraints
    ObSEArray<ObPhyTableLocationInfo, 4> phy_tbl_infos;
    ObSEArray<ObTableLocation, 4> out_tbl_locations;

    if (OB_FAIL(
            calc_table_locations(base_cons, plan_tbl_locs, location_cache, pc_ctx, out_tbl_locations, phy_tbl_infos))) {
      LOG_WARN("failed to calculate table locations", K(ret), K(base_cons));
    } else if (has_duplicate_table &&
               OB_FAIL(reselect_duplicate_table_best_replica(base_cons, server, phy_tbl_infos))) {
      LOG_WARN("failed to reselect duplicate table replica", K(ret));
    } else if (should_get_plan_directly_) {
      // if should_get_plan_directly_ == true:
      //  it is single table dist plan
      //  no need to check if match the constraint
    } else if (OB_FAIL(cmp_table_types(base_cons, server, out_tbl_locations, phy_tbl_infos, is_matched))) {
      LOG_WARN("failed to compare table types", K(ret), K(base_cons));
    } else if (!is_matched) {
      LOG_DEBUG("table types not match", K(base_cons));
    } else if (OB_FAIL(check_partition_constraint(base_cons, phy_tbl_infos, is_matched))) {
      LOG_WARN("failed to check partition constraint", K(ret));
    } else if (!is_matched) {
      LOG_DEBUG("partition constraint not match", K(base_cons));
    } else if (strict_cons.count() <= 0 && non_strict_cons.count() <= 0) {
      // do nothing
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270 || !plan->is_use_px()) {
      if (OB_FAIL(check_inner_constraints_old(strict_cons, non_strict_cons, phy_tbl_infos, is_matched))) {
        LOG_WARN("failed to check inner constraints old", K(ret));
      }
    } else {
      use_pwj_map = true;
      if (OB_FAIL(pwj_map.create(8, ObModIds::OB_PLAN_EXECUTE))) {
        LOG_WARN("create pwj map failed", K(ret));
      } else if (OB_FAIL(check_inner_constraints(
                     strict_cons, non_strict_cons, phy_tbl_infos, pc_ctx, pwj_map, is_matched))) {
        LOG_WARN("failed to check inner constraints", K(ret));
      }
    }

    if (OB_SUCC(ret) && is_matched) {
      PWJPartitionIdMap* exec_pwj_map = NULL;
      if (use_pwj_map) {
        if (OB_FAIL(pc_ctx.exec_ctx_.get_pwj_map(exec_pwj_map))) {
          LOG_WARN("failed to get exec pwj map", K(ret));
        } else if (OB_FAIL(exec_pwj_map->reuse())) {
          LOG_WARN("failed to reuse pwj map", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < base_cons.count(); ++i) {
        // if type = multi part insert:
        //   only match locaiton constraint, no need to add its phy location to out_phy_tbl_locs
        if (!base_cons.at(i).is_multi_part_insert()) {
          ObPhyTableLocation phy_location;
          ObPhyTableLocationInfo& src_location = phy_tbl_infos.at(i);
          if (use_pwj_map) {
            PartitionIdArray partition_id_array;
            if (OB_FAIL(pwj_map.get_refactored(i, partition_id_array))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get refactored", K(ret));
              }
            } else if (OB_FAIL(exec_pwj_map->set_refactored(base_cons.at(i).key_.table_id_, partition_id_array))) {
              LOG_WARN("failed to set refactored", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(phy_location.assign_from_phy_table_loc_info(src_location))) {
              LOG_WARN("failed to construct physical table location from phy_table_loc_info",
                  K(ret),
                  K(phy_tbl_infos.at(i)));
            } else if (OB_FAIL(out_phy_tbl_locs.push_back(phy_location))) {
              LOG_WARN("failed to push back phy_location", K(ret));
            }
          }
        }
      }
    }
  }

  // log the unexpected situation
  if (OB_SUCC(ret) && (1 == base_cons.count())) {
    // single table with dist plan not matched, physical location type not matched, unexpected
    if (!is_matched) {
      LOG_WARN(
          "single table with dist plan not matched, physical location type changed", K(base_cons), K(plan_tbl_locs));
    }
  }

  if (OB_FAIL(ret)) {
    is_matched = false;
  }

  if (pwj_map.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pwj_map.destroy()))) {
      LOG_WARN("failed to destroy pwj map", K(tmp_ret));
    }
  }
  return ret;
}

// remove all plan and its plan stat
int ObDistPlans::remove_all_plan()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  for (int64_t i = 0; i < dist_plans_.count(); i++) {
    if (OB_ISNULL(dist_plans_.at(i))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get un expected null in dist_plans", K(tmp_ret), K(dist_plans_.at(i)));
    } else {
      ObCacheObjectFactory::free(dist_plans_.at(i), PC_REF_PLAN_DIST_HANDLE);
      dist_plans_.at(i) = NULL;
    }
  }

  dist_plans_.reset();
  ret = tmp_ret;

  return ret;
}

int64_t ObDistPlans::get_mem_size() const
{
  int64_t plan_set_mem = 0;
  for (int64_t i = 0; i < dist_plans_.count(); i++) {
    if (OB_ISNULL(dist_plans_.at(i))) {
      BACKTRACE(ERROR, true, "null physical plan");
    } else {
      plan_set_mem += dist_plans_.at(i)->get_mem_size();
    }
  }
  return plan_set_mem;
}

int ObDistPlans::remove_plan_stat()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPlanCache* pc = NULL;
  if (OB_ISNULL(plan_set_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_set_));
  } else if (OB_ISNULL(pc = plan_set_->get_plan_cache())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc));
  } else {
    for (int64_t i = 0; i < dist_plans_.count(); i++) {
      if (OB_ISNULL(dist_plans_.at(i))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get an unexpected null", K(tmp_ret), K(dist_plans_.at(i)));
      } else if (OB_SUCCESS != (tmp_ret = pc->remove_cache_obj_stat_entry(dist_plans_.at(i)->get_plan_id()))) {
        LOG_WARN(
            "failed to remove plan stat", K(tmp_ret), K(dist_plans_.at(i)->get_plan_id()), K(dist_plans_.at(i)), K(i));
      } else {
        /* do nothing */
      }
    }
  }
  ret = tmp_ret;
  return ret;
}

int ObDistPlans::get_tbl_loc_with_key(const TableLocationKey key, const ObIArray<ObTableLocation>& table_locations,
    const ObTableLocation*& ret_loc_ptr) const
{
  int ret = OB_SUCCESS;
  ret_loc_ptr = NULL;
  const ObTableLocation* tmp_loc_ptr;
  for (int i = 0; i < table_locations.count(); i++) {
    tmp_loc_ptr = &table_locations.at(i);
    if (tmp_loc_ptr->get_table_id() == key.table_id_ && tmp_loc_ptr->get_ref_table_id() == key.ref_table_id_) {
      ret_loc_ptr = tmp_loc_ptr;
      break;
    }
  }
  if (OB_ISNULL(ret_loc_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got an unexpected null", K(ret));
  }
  return ret;
}

int ObDistPlans::match_tbl_partition_locs(
    const ObPhyTableLocationInfo& left, const ObPhyTableLocationInfo& right, bool& is_matched) const
{
  int ret = OB_SUCCESS;
  is_matched = true;
  if (left.get_partition_cnt() != right.get_partition_cnt()) {
    is_matched = false;
  } else if (left.get_partition_cnt() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no partition_location in phy_location", K(ret), K(left), K(right));
  } else {
    ObReplicaLocation left_replica_loc;
    ObReplicaLocation right_replica_loc;
    for (int64_t i = 0; OB_SUCC(ret) && is_matched && i < left.get_partition_cnt(); i++) {
      left_replica_loc.reset();
      right_replica_loc.reset();
      const ObPhyPartitionLocationInfo& left_phy_part_loc_info = left.get_phy_part_loc_info_list().at(i);
      const ObPhyPartitionLocationInfo& right_phy_part_loc_info = right.get_phy_part_loc_info_list().at(i);

      if (OB_FAIL(left_phy_part_loc_info.get_selected_replica(left_replica_loc)) ||
          OB_FAIL(right_phy_part_loc_info.get_selected_replica(right_replica_loc))) {
        LOG_WARN("failed to get selected replica", K(ret), K(left_replica_loc), K(right_replica_loc));
      } else if (!left_replica_loc.is_valid() || !right_replica_loc.is_valid()) {
        LOG_WARN("replica_location is invalid", K(ret), K(left_replica_loc), K(right_replica_loc));
      } else if (left_replica_loc.server_ != right_replica_loc.server_) {
        is_matched = false;
        LOG_DEBUG("part location do not match", K(ret), K(i), K(left_replica_loc), K(right_replica_loc));
      } else {
        LOG_DEBUG("matched partition location", K(left_replica_loc), K(right_replica_loc), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_matched = false;
  } else {
    /* do nothing */
  }

  return ret;
}

int ObDistPlans::calc_table_locations(const ObIArray<LocationConstraint>& loc_cons,
    const ObIArray<ObTableLocation>& in_tbl_locations, share::ObIPartitionLocationCache& location_cache,
    const ObPlanCacheCtx& pc_ctx, common::ObIArray<ObTableLocation>& out_tbl_locations,
    common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos) const
{
  int ret = OB_SUCCESS;
  if (loc_cons.count() <= 0 || in_tbl_locations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(loc_cons.count()), K(in_tbl_locations.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < loc_cons.count(); i++) {
      const ObTableLocation* tmp_tbl_loc_ptr;
      if (OB_FAIL(get_tbl_loc_with_key(loc_cons.at(i).key_, in_tbl_locations, tmp_tbl_loc_ptr))) {
        LOG_WARN("failed to get table location with key", K(ret), K(loc_cons.at(i).key_), K(i));
      } else if (OB_ISNULL(tmp_tbl_loc_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null tbl_loc_ptr", K(ret), K(tmp_tbl_loc_ptr));
      } else {
        out_tbl_locations.push_back(*tmp_tbl_loc_ptr);
      }
    }
    if (OB_SUCC(ret)) {
      bool need_check_on_same_server = true;
      if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(
              out_tbl_locations, pc_ctx, location_cache, phy_tbl_infos, need_check_on_same_server))) {
        LOG_WARN("failed to get phy locations", K(ret));
      } else {
        LOG_DEBUG("calculated phy locations", K(loc_cons), K(phy_tbl_infos));
      }
    }
  }

  if (OB_FAIL(ret)) {
    out_tbl_locations.reset();
    phy_tbl_infos.reset();
  }
  return ret;
}

int ObDistPlans::cmp_table_types(const ObIArray<LocationConstraint>& loc_cons, const common::ObAddr& server,
    const common::ObIArray<ObTableLocation>& tbl_locs, const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos,
    bool& is_same) const
{
  int ret = OB_SUCCESS;

  is_same = true;
  if (loc_cons.count() != phy_tbl_infos.count() || tbl_locs.count() != phy_tbl_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(loc_cons.count()), K(phy_tbl_infos.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < loc_cons.count(); i++) {
      ObTableLocationType loc_type = OB_TBL_LOCATION_UNINITIALIZED;
      const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_infos.at(i).get_phy_part_loc_info_list();
      const ObTableLocation& tbl_loc = tbl_locs.at(i);
      if (OB_FAIL(tbl_loc.get_location_type(server, phy_part_loc_info_list, loc_type))) {
        LOG_WARN("failed to get table location type", K(ret), K(server), K(phy_part_loc_info_list));
      } else {
        is_same = (loc_type == loc_cons.at(i).phy_loc_type_);
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

int ObDistPlans::check_inner_constraints_old(const ObIArray<ObPlanPwjConstraint>& strict_cons,
    const ObIArray<ObPlanPwjConstraint>& non_strict_cons, const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos,
    bool& is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < strict_cons.count(); ++i) {
    const ObPlanPwjConstraint& pwj_cons = strict_cons.at(i);
    if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_same && j < pwj_cons.count() - 1; ++j) {
      const ObPhyTableLocationInfo& l_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(j));
      const ObPhyTableLocationInfo& r_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(j + 1));

      if (OB_FAIL(match_tbl_partition_locs(l_phy_tbl_info, r_phy_tbl_info, is_same))) {
        LOG_WARN("failed tp compare table partition locations", K(ret), K(l_phy_tbl_info), K(r_phy_tbl_info));
      } else {
        // do nothing
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < non_strict_cons.count(); ++i) {
    const ObPlanPwjConstraint& pwj_cons = non_strict_cons.at(i);
    if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_same && j < pwj_cons.count() - 1; ++j) {
      const ObPhyTableLocationInfo& l_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(j));
      const ObPhyTableLocationInfo& r_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(j + 1));

      if (OB_FAIL(match_tbl_partition_locs(l_phy_tbl_info, r_phy_tbl_info, is_same))) {
        LOG_WARN("failed tp compare table partition locations", K(ret), K(l_phy_tbl_info), K(r_phy_tbl_info));
      } else {
        // do nothing
      }
    }
  }

  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

int ObDistPlans::check_inner_constraints(const ObIArray<ObPlanPwjConstraint>& strict_cons,
    const ObIArray<ObPlanPwjConstraint>& non_strict_cons, const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos,
    const ObPlanCacheCtx& pc_ctx, PWJPartitionIdMap& pwj_map, bool& is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (strict_cons.count() > 0 || non_strict_cons.count() > 0) {
    const int64_t tbl_count = phy_tbl_infos.count();
    ObSEArray<PwjTable, 8> pwj_tables;
    ObPwjComparer strict_pwj_comparer(true);
    ObPwjComparer non_strict_pwj_comparer(false);
    if (OB_FAIL(pwj_tables.prepare_allocate(tbl_count))) {
      LOG_WARN("failed to prepare allocate pwj tables", K(ret));
    }

    /* Traverse strict pwj constraints and non-strict pwj constraints, and generate a partition id
     *  mapping that can do partition wise join.
     * Because there may be constraints in the following forms
     * strict_pwj_cons = [0,1], [2,3]
     * non_strict_pwj_cons = [0,2]
     * If you traverse strict_pwj_cons first, and then traverse non_strict_pwj_cons, there may be a situation:
     *   After traversing strict_pwj_cons, the mapping of [part_array0, part_array1, part_array2, part_array3] is set in
     * pwj_map, But when traversing non_strict_pwj_cons, it is found that partition_array2 needs to be adjusted, then
     * all the arrays related to it must be adjusted recursively, The adjustment is very complicated. Traverse in
     * strict_pwj_cons and non_strict_pwj_cons separately in the order of the base table to avoid this situation:
     *   Traverse all constraints starting with 0, and set the mapping of [part_array0, part_array1, part_array2] in
     * pwj_map; Traverse all the constraints starting with 1, and find that there are no related constraints; To
     * traverse all constraints starting with 2, you need to set part_array3 in pwj_map, because part_array2 has been
     * set, so The mapping of part_array3 generated by part_array2 does not need to be adjusted Traverse all the
     * constraints starting with 3 and find that there are no related constraints
     */
    for (int64_t i = 0; OB_SUCC(ret) && i < tbl_count; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < strict_cons.count(); ++j) {
        const ObPlanPwjConstraint& pwj_cons = strict_cons.at(j);
        if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (pwj_cons.at(0) == i) {
          if (OB_FAIL(
                  check_pwj_cons(pc_ctx, pwj_cons, phy_tbl_infos, pwj_tables, strict_pwj_comparer, pwj_map, is_same))) {
            LOG_WARN("failed to check pwj cons", K(ret));
          }
        }
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < non_strict_cons.count(); ++j) {
        const ObPlanPwjConstraint& pwj_cons = non_strict_cons.at(j);
        if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (pwj_cons.at(0) == i) {
          if (OB_FAIL(check_pwj_cons(
                  pc_ctx, pwj_cons, phy_tbl_infos, pwj_tables, non_strict_pwj_comparer, pwj_map, is_same))) {
            LOG_WARN("failed to check pwj cons", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

int ObDistPlans::set_phy_table_locations_for_ctx(ObPlanCacheCtx& pc_ctx, const ObIArray<ObPhyTableLocation>& tbl_locs)
{
  int ret = OB_SUCCESS;
  ObExecContext& exec_ctx = pc_ctx.exec_ctx_;
  ObPhyTableLocationIArray& ctx_phy_locations = exec_ctx.get_task_exec_ctx().get_table_locations();
  ctx_phy_locations.reset();
  if (OB_FAIL(ctx_phy_locations.prepare_allocate(tbl_locs.count()))) {
    LOG_WARN("failed to allocate memory for phy locations", K(ret));
  } else if (OB_FAIL(ctx_phy_locations.assign(tbl_locs))) {
    LOG_WARN("failed to assign phy locations", K(ret));
  } else { /* do nothing*/
  }

  if (OB_FAIL(ret)) {
    ctx_phy_locations.reset();
  }
  return ret;
}

ObPhysicalPlan* ObDistPlans::get_first_plan()
{
  ObPhysicalPlan* plan = nullptr;
  if (dist_plans_.count() >= 1) {
    plan = dist_plans_.at(0);
  }
  return plan;
}

int ObDistPlans::check_partition_constraint(const ObIArray<LocationConstraint>& loc_cons,
    const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, bool& is_match) const
{
  int ret = OB_SUCCESS;
  is_match = true;
  if (loc_cons.count() != phy_tbl_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(loc_cons.count()), K(phy_tbl_infos.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < loc_cons.count(); i++) {
      const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_infos.at(i).get_phy_part_loc_info_list();
      if (loc_cons.at(i).is_partition_single()) {
        // is_partition_single requires only one part in partition level one
        int64_t first_part_id = OB_INVALID_PARTITION_ID;
        for (int64_t j = 0; OB_SUCC(ret) && is_match && j < phy_part_loc_info_list.count(); ++j) {
          int64_t cur_partition_id = phy_part_loc_info_list.at(j).get_partition_location().get_partition_id();
          int64_t cur_part_id = extract_part_idx(cur_partition_id);
          if (OB_INVALID_PARTITION_ID == first_part_id) {
            first_part_id = cur_part_id;
          } else if (cur_part_id != first_part_id) {
            is_match = false;
          }
        }
      } else if (loc_cons.at(i).is_subpartition_single()) {
        // is_subpartition_single requires only one part in secondary partition level
        ObSqlBitSet<> part_ids;
        for (int64_t j = 0; OB_SUCC(ret) && is_match && j < phy_part_loc_info_list.count(); ++j) {
          int64_t cur_partition_id = phy_part_loc_info_list.at(j).get_partition_location().get_partition_id();
          int64_t cur_part_id = extract_part_idx(cur_partition_id);
          if (part_ids.has_member(cur_part_id)) {
            is_match = false;
          } else if (OB_FAIL(part_ids.add_member(cur_part_id))) {
            LOG_WARN("failed to add member", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_match = false;
  }
  return ret;
}

int64_t ObDistPlans::check_pwj_cons(const ObPlanCacheCtx& pc_ctx, const ObPlanPwjConstraint& pwj_cons,
    const common::ObIArray<ObPhyTableLocationInfo>& phy_tbl_infos, ObIArray<PwjTable>& pwj_tables,
    ObPwjComparer& pwj_comparer, PWJPartitionIdMap& pwj_map, bool& is_same) const
{
  int ret = OB_SUCCESS;
  // check all table in same pwj constraint have same partition count
  const int64_t part_count = phy_tbl_infos.at(pwj_cons.at(0)).get_partition_cnt();
  for (int64_t i = 1; is_same && i < pwj_cons.count(); ++i) {
    if (part_count != phy_tbl_infos.at(pwj_cons.at(i)).get_partition_cnt()) {
      is_same = false;
    }
  }

  if (1 == part_count) {
    // all tables in pwj constraint are local or remote
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count() - 1; ++i) {
      const ObPhyTableLocationInfo& l_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(i));
      const ObPhyTableLocationInfo& r_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(i + 1));
      if (OB_FAIL(match_tbl_partition_locs(l_phy_tbl_info, r_phy_tbl_info, is_same))) {
        LOG_WARN("failed tp compare table partition locations", K(ret), K(l_phy_tbl_info), K(r_phy_tbl_info));
      }
    }
  } else {
    // distribute partition wise join
    pwj_comparer.reset();
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count(); ++i) {
      const int64_t table_idx = pwj_cons.at(i);
      PwjTable& table = pwj_tables.at(table_idx);
      bool need_set_refactored = false;
      if (OB_INVALID_ID == table.ref_table_id_) {
        // pwj table no init
        need_set_refactored = true;
        const ObPhyTableLocationInfo& phy_tbl_info = phy_tbl_infos.at(table_idx);
        share::schema::ObSchemaGetterGuard* schema_guard = pc_ctx.sql_ctx_.schema_guard_;
        const share::schema::ObTableSchema* table_schema = NULL;
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(schema_guard->get_table_schema(phy_tbl_info.get_ref_table_id(), table_schema))) {
          LOG_WARN("failed to get table schema", K(ret), K(phy_tbl_info.get_ref_table_id()));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(table.init(*table_schema, phy_tbl_info))) {
          LOG_WARN("failed to init pwj table with table schema", K(ret));
        }
      } else {
        // pwj table already init, table's ordered partition ids should use
        // partition id array in pwj map
        PartitionIdArray partition_id_array;
        if (OB_FAIL(pwj_map.get_refactored(table_idx, partition_id_array))) {
          if (OB_HASH_NOT_EXIST == ret) {
            LOG_WARN("get refactored not find partition id array", K(ret));
          } else {
            LOG_WARN("failed to get refactored", K(ret));
          }
        } else if (OB_FAIL(table.ordered_partition_ids_.assign(partition_id_array))) {
          LOG_WARN("failed to assign partition id array", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(pwj_comparer.add_table(table, is_same))) {
          LOG_WARN("failed to add table", K(ret));
        } else if (is_same && need_set_refactored &&
                   OB_FAIL(pwj_map.set_refactored(table_idx, pwj_comparer.get_partition_id_group().at(i)))) {
          LOG_WARN("failed to set refactored", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

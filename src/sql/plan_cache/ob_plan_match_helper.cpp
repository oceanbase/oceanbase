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
#include "sql/plan_cache/ob_plan_match_helper.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace sql {

int ObPlanMatchHelper::match_plan(const ObPlanCacheCtx &pc_ctx,
                                  const ObPhysicalPlan *plan,
                                  bool &is_matched,
                                  ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                                  ObIArray<ObTableLocation> &out_tbl_locations) const
{
  int ret = OB_SUCCESS;
  bool has_duplicate_table = false;
  bool is_retrying = false;
  is_matched = true;
  const ObAddr &server = pc_ctx.exec_ctx_.get_addr();
  const ObIArray<LocationConstraint>& base_cons = plan->get_base_constraints();
  const ObIArray<ObPlanPwjConstraint>& strict_cons = plan->get_strict_constraints();
  const ObIArray<ObPlanPwjConstraint>& non_strict_cons = plan->get_non_strict_constraints();
  const ObIArray<ObDupTabConstraint>& dup_rep_cons = plan->get_dup_table_replica_constraints();
  const ObIArray<ObTableLocation> &plan_tbl_locs = plan->get_table_locations();
  PWJTabletIdMap pwj_map;
  bool use_pwj_map = false;
  if (0 == base_cons.count()) {
    // match all
    is_matched = true;
  } else {
    if (OB_NOT_NULL(plan_set_) && plan_set_->has_duplicate_table()) {
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
      if (OB_FAIL(calc_table_locations(base_cons, plan_tbl_locs, pc_ctx,
                                      out_tbl_locations, phy_tbl_infos))) {
        LOG_WARN("failed to calculate table locations", K(ret), K(base_cons));
      } else if (has_duplicate_table &&
                OB_FAIL(reselect_duplicate_table_best_replica(base_cons, server, phy_tbl_infos,
                                                                                dup_rep_cons))) {
        LOG_WARN("failed to reselect duplicate table replica", K(ret));
      } else if (OB_FAIL(cmp_table_types(base_cons, server, out_tbl_locations,
                                        phy_tbl_infos, is_matched))) {
        LOG_WARN("failed to compare table types", K(ret), K(base_cons));
      } else if (!is_matched) {
        LOG_DEBUG("table types not match", K(base_cons));
      } else if (OB_FAIL(check_partition_constraint(pc_ctx, base_cons, phy_tbl_infos, is_matched))) {
        LOG_WARN("failed to check partition constraint", K(ret));
      } else if (!is_matched) {
        LOG_DEBUG("partition constraint not match", K(base_cons));
      } else if (strict_cons.count() <= 0 && non_strict_cons.count() <= 0) {
        // do nothing
      } else if (OB_FAIL(pwj_map.create(8, ObModIds::OB_PLAN_EXECUTE))) {
        LOG_WARN("create pwj map failed", K(ret));
      } else if (OB_FAIL(check_inner_constraints(strict_cons, non_strict_cons, phy_tbl_infos,
                                                pc_ctx, pwj_map, is_matched))) {
        LOG_WARN("failed to check inner constraints", K(ret));
      } else {
        use_pwj_map = true;
      }

      if (OB_SUCC(ret) && is_matched) {
        PWJTabletIdMap *exec_pwj_map = NULL;
        ObDASCtx &das_ctx = DAS_CTX(pc_ctx.exec_ctx_);
        if (use_pwj_map) {
          if (OB_FAIL(pc_ctx.exec_ctx_.get_pwj_map(exec_pwj_map))) {
            LOG_WARN("failed to get exec pwj map", K(ret));
          } else if (OB_FAIL(exec_pwj_map->reuse())) {
            LOG_WARN("failed to reuse pwj map", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < base_cons.count(); ++i) {
          // in the case of multi part insert, only the location constraint is matched, but the
          // corresponding phy table location information does not need to be added to table_locs
          if (!base_cons.at(i).is_multi_part_insert()) {
            ObCandiTableLoc &src_location = phy_tbl_infos.at(i);
            if (use_pwj_map) {
              TabletIdArray tablet_id_array;
              if (OB_FAIL(pwj_map.get_refactored(i, tablet_id_array))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  // 没找到说明当前表不需要做partition wise join
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get refactored", K(ret));
                }
              } else if (OB_FAIL(exec_pwj_map->set_refactored(base_cons.at(i).key_.table_id_,
                                                              tablet_id_array))) {
                LOG_WARN("failed to set refactored", K(ret));
              }
            }
          }
        }
      }
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

int ObPlanMatchHelper::get_tbl_loc_with_key(const TableLocationKey key,
                                            const ObIArray<ObTableLocation> &table_locations,
                                            const ObTableLocation *&ret_loc_ptr) const
{
  int ret = OB_SUCCESS;
  ret_loc_ptr = NULL;
  const ObTableLocation *tmp_loc_ptr;
  for (int i = 0; i < table_locations.count(); i++) {
    tmp_loc_ptr = &table_locations.at(i);
    if (tmp_loc_ptr->get_table_id() == key.table_id_ &&
        tmp_loc_ptr->get_ref_table_id() == key.ref_table_id_) {
      ret_loc_ptr = tmp_loc_ptr;
      break;
    }
  }
  if (OB_ISNULL(ret_loc_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got an unexpected null", K(ret), K(key), K(table_locations));
  }
  return ret;
}

int ObPlanMatchHelper::calc_table_locations(
    const ObIArray<LocationConstraint> &loc_cons,
    const ObIArray<ObTableLocation> &in_tbl_locations,
    const ObPlanCacheCtx &pc_ctx,
    common::ObIArray<ObTableLocation> &out_tbl_locations,
    common::ObIArray<ObCandiTableLoc> &phy_tbl_infos) const
{
  int ret = OB_SUCCESS;
  if (loc_cons.count() <= 0 || in_tbl_locations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(loc_cons.count()), K(in_tbl_locations.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < loc_cons.count(); i++) {
      const ObTableLocation *tmp_tbl_loc_ptr;
      if (OB_FAIL(get_tbl_loc_with_key(loc_cons.at(i).key_,
                                       in_tbl_locations,
                                       tmp_tbl_loc_ptr))) {
        LOG_WARN("failed to get table location with key", K(ret), K(loc_cons.at(i).key_), K(i));
      } else if (OB_ISNULL(tmp_tbl_loc_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null tbl_loc_ptr", K(ret), K(tmp_tbl_loc_ptr));
      } else if (OB_FAIL(out_tbl_locations.push_back(*tmp_tbl_loc_ptr))) {
        LOG_WARN("failed to add table location", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool need_check_on_same_server = true;
      if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(out_tbl_locations,
                                                         pc_ctx,
                                                         phy_tbl_infos,
                                                         need_check_on_same_server))) {
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

int ObPlanMatchHelper::reselect_duplicate_table_best_replica(
    const ObIArray<LocationConstraint> &loc_cons,
    const common::ObAddr &server,
    const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
    const ObIArray<ObDupTabConstraint> &dup_table_replica_cons) const
{
  int ret = OB_SUCCESS;
  if (loc_cons.count() == phy_tbl_infos.count()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_infos.count(); ++i) {
      const ObCandiTableLoc &phy_tbl_info = phy_tbl_infos.at(i);
      if (phy_tbl_info.is_duplicate_table_not_in_dml()) {
        bool selected_replica = false;
        if (phy_tbl_info.get_partition_cnt() == 1) {
          const ObCandiTabletLoc &dup_phy_part_loc =
                                        phy_tbl_info.get_phy_part_loc_info_list().at(0);
          int64_t replica_idx = 0;
          int64_t left_tbl_pos = -1;
          // find first constraint
          for (int64_t j = 0; OB_SUCC(ret) && j < dup_table_replica_cons.count(); ++j) {
            ObDupTabConstraint con = dup_table_replica_cons.at(j);
            if (con.first_ == OB_INVALID) {
              // do nothing
            } else if (con.first_==i) {
              left_tbl_pos = con.second_;
            }
          }
          if (left_tbl_pos != -1) {
            const ObCandiTabletLoc& left_tbl_part_loc_info =
                            phy_tbl_infos.at(left_tbl_pos).get_phy_part_loc_info_list().at(0);
            share::ObLSReplicaLocation replica_loc;
            if (OB_FAIL(left_tbl_part_loc_info.get_selected_replica(replica_loc))) {
              LOG_WARN("failed to set selected replica idx", K(ret), K(left_tbl_part_loc_info));
            } else if (dup_phy_part_loc.is_server_in_replica(replica_loc.get_server(), replica_idx)) {
              LOG_DEBUG("reselect replica index according to pwj constraints will happen",
                        K(dup_phy_part_loc), K(replica_idx), K(replica_loc.get_server()), K(replica_loc));
              ObRoutePolicy::CandidateReplica replica;
              if (OB_FAIL(dup_phy_part_loc.get_priority_replica(replica_idx, replica))) {
                LOG_WARN("failed to get priority replica", K(ret));
              } else if (OB_FAIL(const_cast<ObCandiTabletLoc&>(dup_phy_part_loc).
                    set_selected_replica_idx(replica_idx))) {
                LOG_WARN("failed to set selected replica idx", K(ret), K(replica_idx));
              } else {
                selected_replica = true;
              }
            }
          }
        }
        // if not found, just select local
        if (!selected_replica) {
          for (int64_t j = 0; OB_SUCC(ret) && j < phy_tbl_info.get_partition_cnt(); ++j) {
            const ObCandiTabletLoc &phy_part_loc_info =
                phy_tbl_info.get_phy_part_loc_info_list().at(j);
            int64_t replica_idx = 0;
            if (phy_part_loc_info.is_server_in_replica(server, replica_idx)) {
              LOG_DEBUG("reselect replica index will happen",
                        K(phy_tbl_info), K(replica_idx), K(server));
              ObRoutePolicy::CandidateReplica replica;
              if (OB_FAIL(phy_part_loc_info.get_priority_replica(replica_idx, replica))) {
                LOG_WARN("failed to get priority replica", K(ret));
              } else if (OB_FAIL(const_cast<ObCandiTabletLoc&>(phy_part_loc_info).
                  set_selected_replica_idx(replica_idx))) {
                LOG_WARN("failed to set selected replica idx", K(ret), K(replica_idx));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPlanMatchHelper::cmp_table_types(
    const ObIArray<LocationConstraint> &loc_cons,
    const common::ObAddr &server,
    const common::ObIArray<ObTableLocation> &tbl_locs,
    const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
    bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (loc_cons.count() != phy_tbl_infos.count() ||
      tbl_locs.count() != phy_tbl_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(loc_cons.count()), K(phy_tbl_infos.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < loc_cons.count(); i++) {
      ObTableLocationType loc_type = OB_TBL_LOCATION_UNINITIALIZED;
      const ObCandiTabletLocIArray &phy_part_loc_info_list =
        phy_tbl_infos.at(i).get_phy_part_loc_info_list();
      const ObTableLocation &tbl_loc = tbl_locs.at(i);
      if (OB_FAIL(tbl_loc.get_location_type(server,
                                            phy_part_loc_info_list,
                                            loc_type))) {
        LOG_WARN("failed to get table location type",
                 K(ret),
                 K(server),
                 K(phy_part_loc_info_list));
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

int ObPlanMatchHelper::check_partition_constraint(
    const ObPlanCacheCtx &pc_ctx,
    const ObIArray<LocationConstraint> &loc_cons,
    const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
    bool &is_match) const
{
  int ret = OB_SUCCESS;
  is_match = true;
  share::schema::ObSchemaGetterGuard *schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (loc_cons.count() != phy_tbl_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(loc_cons.count()), K(phy_tbl_infos.count()));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(GET_MY_SESSION(pc_ctx.exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session", KR(ret));
  } else {
    const uint64_t tenant_id = GET_MY_SESSION(pc_ctx.exec_ctx_)->get_effective_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < loc_cons.count(); i++) {
      const ObCandiTabletLocIArray &phy_part_loc_info_list =
        phy_tbl_infos.at(i).get_phy_part_loc_info_list();
      if (!loc_cons.at(i).is_partition_single() && !loc_cons.at(i).is_subpartition_single()) {
        // do nothing
      } else if (OB_FAIL(schema_guard->get_table_schema(
                 tenant_id, phy_tbl_infos.at(i).get_ref_table_id(), table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(phy_tbl_infos.at(i).get_ref_table_id()));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table schema", K(ret), K(phy_tbl_infos.at(i).get_ref_table_id()));
      } else if (loc_cons.at(i).is_partition_single()) {
        // is_partition_single requires that the current secondary partition
        // table only involves one primary partition
        int64_t first_part_id = OB_INVALID_PARTITION_ID;
        for (int64_t j = 0; OB_SUCC(ret) && is_match && j < phy_part_loc_info_list.count(); ++j) {
          ObTabletID cur_tablet_id =
              phy_part_loc_info_list.at(j).get_partition_location().get_tablet_id();
          int64_t cur_part_id = OB_INVALID_ID;
          int64_t cur_subpart_id = OB_INVALID_ID;
          if (OB_FAIL(table_schema->get_part_id_by_tablet(cur_tablet_id, cur_part_id, cur_subpart_id))) {
            LOG_WARN("failed to get part id by tablet", K(ret));
          } else if (OB_INVALID_PARTITION_ID == first_part_id) {
            first_part_id = cur_part_id;
          } else if (cur_part_id != first_part_id) {
            is_match = false;
          }
        }
      } else if (loc_cons.at(i).is_subpartition_single()) {
        // is_subpartition_single requires that each primary partition of the current
        // secondary partition table involves only one secondary partition
        ObSEArray<int64_t, 4> part_ids;
        for (int64_t j = 0; OB_SUCC(ret) && is_match && j < phy_part_loc_info_list.count(); ++j) {
          ObTabletID cur_tablet_id =
              phy_part_loc_info_list.at(j).get_partition_location().get_tablet_id();
          int64_t cur_part_id = OB_INVALID_ID;
          int64_t cur_subpart_id = OB_INVALID_ID;
          if (OB_FAIL(table_schema->get_part_id_by_tablet(cur_tablet_id, cur_part_id, cur_subpart_id))) {
            LOG_WARN("failed to get part id by tablet", K(ret));
          } else {
            for (int64_t k = 0; OB_SUCC(ret) && is_match && k < part_ids.count(); ++k) {
              if (part_ids.at(k) == cur_part_id) {
                is_match = false;
              }
            }

            if (OB_FAIL(ret)) {
              // do nothing
            } else if (!is_match) {
              // do nothing
            } else if (OB_FAIL(part_ids.push_back(cur_part_id))) {
              LOG_WARN("failed to add member", K(ret));
            } else {
              // do nothing
            }
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

int ObPlanMatchHelper::check_inner_constraints(
    const ObIArray<ObPlanPwjConstraint> &strict_cons,
    const ObIArray<ObPlanPwjConstraint> &non_strict_cons,
    const common::ObIArray<ObCandiTableLoc> &phy_tbl_infos,
    const ObPlanCacheCtx &pc_ctx,
    PWJTabletIdMap &pwj_map,
    bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (strict_cons.count() >0 || non_strict_cons.count() > 0) {
    const int64_t tbl_count = phy_tbl_infos.count();
    ObSEArray<PwjTable, 4> pwj_tables;
    SMART_VARS_2((ObStrictPwjComparer, strict_pwj_comparer),
                 (ObNonStrictPwjComparer, non_strict_pwj_comparer)) {
      if (OB_FAIL(pwj_tables.prepare_allocate(tbl_count))) {
        LOG_WARN("failed to prepare allocate pwj tables", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && is_same && i < strict_cons.count(); ++i) {
        const ObPlanPwjConstraint &pwj_cons = strict_cons.at(i);
        if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (OB_FAIL(check_strict_pwj_cons(pc_ctx, pwj_cons, phy_tbl_infos,
                                                 strict_pwj_comparer, pwj_map, is_same))) {
          LOG_WARN("failed to check strict pwj cons", K(ret));
        } else {
          LOG_DEBUG("succ to check strict pwj cons", K(is_same));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && is_same && i < non_strict_cons.count(); ++i) {
        const ObPlanPwjConstraint &pwj_cons = non_strict_cons.at(i);
        if (OB_UNLIKELY(pwj_cons.count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (OB_FAIL(check_non_strict_pwj_cons(pwj_cons, phy_tbl_infos,
                                                     non_strict_pwj_comparer, is_same))) {
          LOG_WARN("failed to check non strict pwj cons", K(ret));
        } else {
          LOG_DEBUG("succ to check non strict pwj cons", K(is_same));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

int ObPlanMatchHelper::check_strict_pwj_cons(
        const ObPlanCacheCtx &pc_ctx,
        const ObPlanPwjConstraint &pwj_cons,
        const ObIArray<ObCandiTableLoc> &phy_tbl_infos,
        ObStrictPwjComparer &pwj_comparer,
        PWJTabletIdMap &pwj_map,
        bool &is_same) const
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
      const ObCandiTableLoc &l_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(i));
      const ObCandiTableLoc &r_phy_tbl_info = phy_tbl_infos.at(pwj_cons.at(i+1));
      if (OB_FAIL(match_tbl_partition_locs(l_phy_tbl_info, r_phy_tbl_info, is_same))) {
        LOG_WARN("failed tp compare table partition locations",
                K(ret), K(l_phy_tbl_info), K(r_phy_tbl_info));
      }
    }
  } else if (OB_ISNULL(GET_MY_SESSION(pc_ctx.exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session", KR(ret));
  } else {
    // distribute partition wise join
    pwj_comparer.reset();
    const uint64_t tenant_id = GET_MY_SESSION(pc_ctx.exec_ctx_)->get_effective_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count(); ++i) {
      const int64_t table_idx = pwj_cons.at(i);
      PwjTable pwj_table;
      const ObCandiTableLoc &phy_tbl_info = phy_tbl_infos.at(table_idx);
      share::schema::ObSchemaGetterGuard *schema_guard = pc_ctx.sql_ctx_.schema_guard_;
      const share::schema::ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(
                 tenant_id, phy_tbl_info.get_ref_table_id(), table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(phy_tbl_info.get_ref_table_id()));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(pwj_table.init(*table_schema, phy_tbl_info))) {
        LOG_WARN("failed to init pwj table with table schema", K(ret));
      } else if (OB_FAIL(pwj_comparer.add_table(pwj_table, is_same))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (is_same &&
                 OB_FAIL(pwj_map.set_refactored(table_idx, pwj_comparer.get_tablet_id_group().at(i)))) {
        LOG_WARN("failed to set refactored", K(ret));
      }
    }
  }
  return ret;
}

int ObPlanMatchHelper::check_non_strict_pwj_cons(const ObPlanPwjConstraint &pwj_cons,
                                                 const ObIArray<ObCandiTableLoc> &phy_tbl_infos,
                                                 ObNonStrictPwjComparer &pwj_comparer,
                                                 bool &is_same) const
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObAddr, 8> server_list;
  // non strict partition wise join constraint, request all tables in same group have at least
  // one partition on same server. Each table's partition count may be different.
  pwj_comparer.reset();
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count(); ++i) {
    const int64_t table_idx = pwj_cons.at(i);
    PwjTable pwj_table;
    server_list.reuse();
    const ObCandiTableLoc &table_loc = phy_tbl_infos.at(table_idx);
    if (OB_FAIL(table_loc.get_all_servers(server_list))) {
      LOG_WARN("failed to get all servers", K(ret));
    } else if (OB_FAIL(pwj_table.init(server_list))) {
      LOG_WARN("failed to init pwj table with table schema", K(ret));
    } else if (OB_FAIL(pwj_comparer.add_table(pwj_table, is_same))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int ObPlanMatchHelper::match_tbl_partition_locs(const ObCandiTableLoc &left,
                                                const ObCandiTableLoc &right,
                                                bool &is_matched) const
{
  int ret = OB_SUCCESS;
  is_matched = true;
  if (left.get_partition_cnt() != right.get_partition_cnt()) {
    is_matched = false;
  } else if (left.get_partition_cnt() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no partition_location in phy_location", K(ret), K(left),
             K(right));
  } else {
    ObLSReplicaLocation left_replica_loc;
    ObLSReplicaLocation right_replica_loc;
    for (int64_t i = 0;
         OB_SUCC(ret) && is_matched && i < left.get_partition_cnt(); i++) {
      left_replica_loc.reset();
      right_replica_loc.reset();
      const ObCandiTabletLoc &left_phy_part_loc_info =
          left.get_phy_part_loc_info_list().at(i);
      const ObCandiTabletLoc &right_phy_part_loc_info =
          right.get_phy_part_loc_info_list().at(i);

      if (OB_FAIL(left_phy_part_loc_info.get_selected_replica(left_replica_loc)) ||
          OB_FAIL(right_phy_part_loc_info.get_selected_replica(right_replica_loc))) {
        LOG_WARN("failed to get selected replica", K(ret), K(left_replica_loc),
                 K(right_replica_loc));
      } else if (!left_replica_loc.is_valid() ||
                 !right_replica_loc.is_valid()) {
        LOG_WARN("replica_location is invalid", K(ret), K(left_replica_loc),
                 K(right_replica_loc));
      } else if (left_replica_loc.get_server() != right_replica_loc.get_server()) {
        is_matched = false;
        LOG_DEBUG("part location do not match", K(ret), K(i),
                  K(left_replica_loc), K(right_replica_loc));
      } else {
        LOG_DEBUG("matched partition location", K(left_replica_loc),
                  K(right_replica_loc), K(i));
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
} // namespace sql
} // namespace oceanbase

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

#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_i_tablet_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor.h"
#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
int ObGetAllPlanIdOp::set_key_array(common::ObIArray<uint64_t> *key_array)
{
int ret = common::OB_SUCCESS;
if (NULL == key_array) {
  ret = common::OB_INVALID_ARGUMENT;
  SQL_PC_LOG(WARN, "invalid argument", K(ret));
} else {
  key_array_ = key_array;
}
return ret;
}

int ObGetAllPlanIdOp::operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (NULL == key_array_) {
    ret = common::OB_NOT_INIT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (ObLibCacheNameSpace::NS_CRSR == entry.second->get_ns() &&
      OB_FAIL(key_array_->push_back(entry.first))) {
    SQL_PC_LOG(WARN, "fail to push back plan_id", K(ret));
  }
  return ret;
}

int ObGetAllCacheIdOp::set_key_array(common::ObIArray<uint64_t> *key_array)
{
int ret = common::OB_SUCCESS;
if (NULL == key_array) {
  ret = common::OB_INVALID_ARGUMENT;
  SQL_PC_LOG(WARN, "invalid argument", K(ret));
} else {
  key_array_ = key_array;
}
return ret;
}

int ObGetAllCacheIdOp::operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (NULL == key_array_ || OB_ISNULL(entry.second)) {
    ret = common::OB_NOT_INIT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (entry.second->get_ns() >= ObLibCacheNameSpace::NS_CRSR
            && entry.second->get_ns() <= ObLibCacheNameSpace::NS_PKG) {
    if (OB_ISNULL(entry.second)) {
      // do nothing
    } else if (!entry.second->added_lc()) {
      // do nothing
    } else if (OB_FAIL(key_array_->push_back(entry.first))) {
      SQL_PC_LOG(WARN, "fail to push back plan_id", K(ret));
    }
  }
  return ret;
}

int ObPlanCacheCtx::is_retry(bool &v) const
{
  int ret = OB_SUCCESS;
  v = 0;
  if (OB_ISNULL(sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    v = sql_ctx_.session_info_->get_is_in_retry();
  }

  return ret;
}

int ObPlanCacheCtx::is_retry_for_dup_tbl(bool &v) const
{
  int ret = OB_SUCCESS;
  v = 0;
  if (OB_ISNULL(sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    v = sql_ctx_.session_info_->get_is_in_retry_for_dup_tbl();
  }

  return ret;
}

int ObPhyLocationGetter::get_phy_locations(const common::ObIArray<ObTablePartitionInfo *> &partition_infos,
                                           ObIArray<ObCandiTableLoc> &candi_table_locs)
{
  int ret = OB_SUCCESS;
  //ObDASTableLoc table_loc;
  int64_t N = partition_infos.count();
  if (OB_FAIL(candi_table_locs.reserve(N))) {
    LOG_WARN("fail reserve memory", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (OB_ISNULL(partition_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition info", K(ret));
    } else if (OB_FAIL(candi_table_locs.push_back(
                   partition_infos.at(i)->get_phy_tbl_location_info()))) {
      LOG_WARN("failed to push_back phy_location_info", K(ret),
               K(partition_infos.at(i)->get_phy_tbl_location_info()));
    } else { /* do nothing */ }
  }
  return ret;
}

//包含复制表的情况下, 选择完副本后调整复制表的副本选择使和非复制表的location一致(前提是非复制表都在同一server)
//好处是可以使得计划类型从 DIST --> REMOTE; 不能在ObSqlPlanSet::calc_phy_plan_type_by_proj计算
//的原因是get_phy_locations就会将物理位置赋值给task_exec_ctx; (实现参考了is_partition_in_same_server_by_proj)
int ObPhyLocationGetter::reselect_duplicate_table_best_replica(const ObIArray<ObCandiTableLoc> &phy_locations,
                                                               bool &on_same_server)
{
  int ret = OB_SUCCESS;
  bool has_duplicate_tbl = false;
  bool is_same = true;
  ObAddr normal_table_addr;
  ObAddr duplicate_table_addr;
  ObSEArray<ObAddr, 4> candi_addrs;
  ObSEArray<int64_t, 8> new_replic_idxs;
  int64_t proj_cnt = phy_locations.count();
  ObLSReplicaLocation replica_location;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < proj_cnt; ++i) {
    const ObCandiTableLoc &ptli = phy_locations.at(i);
    if (ptli.get_partition_cnt() > 1) {
      is_same = false;
    } else if (ptli.get_partition_cnt() > 0) {
      const ObCandiTabletLoc &part_info = ptli.get_phy_part_loc_info_list().at(0);
      if (OB_FAIL(part_info.get_selected_replica(replica_location))) {
        SQL_PC_LOG(WARN, "fail to get selected replica", K(ret), K(ptli));
      } else if (!replica_location.is_valid()) {
        SQL_PC_LOG(WARN, "replica_location is invalid", K(ret), K(replica_location));
      } else if (!ptli.is_duplicate_table_not_in_dml()) {
        // handle normal table
        if (!normal_table_addr.is_valid()) {
          normal_table_addr = replica_location.get_server();
          SQL_PC_LOG(DEBUG, "part_location first replica", K(ret), K(replica_location));
        } else if (normal_table_addr != replica_location.get_server()) {
          is_same = false;
          SQL_PC_LOG(DEBUG, "part_location replica", K(ret), K(i), K(replica_location));
        }
      } else {
        // handle duplicate table
        if (!has_duplicate_tbl) {
          const ObIArray<ObRoutePolicy::CandidateReplica> &replicas =
              part_info.get_partition_location().get_replica_locations();
          for (int64_t j = 0; OB_SUCC(ret) && j < replicas.count(); ++j) {
            if (OB_FAIL(candi_addrs.push_back(replicas.at(j).get_server()))) {
              LOG_WARN("failed to push back servers", K(ret));
            }
          }
          duplicate_table_addr = replica_location.get_server();
          has_duplicate_tbl = true;
          SQL_PC_LOG(DEBUG, "has duplicate table");
        } else if (duplicate_table_addr != replica_location.get_server()) {
          duplicate_table_addr.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_same) {
      // normal table does not use the same server, or
      // there is multi part table
      candi_addrs.reset();
      normal_table_addr.reset();
    } else if (!normal_table_addr.is_valid()) {
      // no normal table found
      if (duplicate_table_addr.is_valid()) {
        // duplicate tables use the same server
        candi_addrs.reset();
      }
    } else if (normal_table_addr == duplicate_table_addr) {
      // normal table and duplicate table already select the same server
      candi_addrs.reset();
    } else {
      // normal table uses the same server
      // duplicate table needs to reselect replica
      candi_addrs.reset();
      if (OB_FAIL(candi_addrs.push_back(normal_table_addr))) {
        LOG_WARN("failed to push back normal table addr", K(ret));
      }
    }
  }
  //如果无复制表或者非复制表就已经无法保证在同一server, 是分布式计划, 就没要必要在这里更改复制表的副本idx了
  if (OB_SUCC(ret) && !candi_addrs.empty()) {
    is_same = false;
    if (OB_FAIL(new_replic_idxs.prepare_allocate(proj_cnt))) {
      SQL_PC_LOG(WARN, "failed to pre-alloc array space", K(ret), K(proj_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_same && i < candi_addrs.count(); ++i) {
      bool is_valid = true;
      const ObAddr &addr = candi_addrs.at(i);
      //a, 是否在同一server上有副本
      for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < proj_cnt; ++j) {
        const ObCandiTableLoc &ptli = phy_locations.at(j);
        if (ptli.is_duplicate_table_not_in_dml()) {
          is_valid = ptli.get_phy_part_loc_info_list().at(0).is_server_in_replica(
                       addr, new_replic_idxs.at(j));
        }
      }
      //b, 所有复制表都有在 addr 上的副本, 一起更改之
      for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < proj_cnt; ++j) {
        ObCandiTableLoc &ptli = const_cast<ObCandiTableLoc&>(phy_locations.at(j));
        if (!ptli.is_duplicate_table_not_in_dml()) {
          // do nothing
        } else if (OB_FAIL(ptli.get_phy_part_loc_info_list_for_update().at(0).
                           set_selected_replica_idx(new_replic_idxs.at(j)))) {
          SQL_PC_LOG(WARN, "failed to set selected replica idx", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_valid) {
        is_same = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    on_same_server = is_same;
  }
  return ret;
}

//need_check_on_same_server: out, 是否需要检查分区在同一server, 如果这里检查过了就置为false
int ObPhyLocationGetter::get_phy_locations(const ObIArray<ObTableLocation> &table_locations,
                                           const ObPlanCacheCtx &pc_ctx,
                                           ObIArray<ObCandiTableLoc> &candi_table_locs,
                                           bool &need_check_on_same_server)
{
  int ret = OB_SUCCESS;
  bool has_duplicate_tbl_not_in_dml = false;
  ObExecContext &exec_ctx = pc_ctx.exec_ctx_;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(pc_ctx.sql_ctx_.session_info_);
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  int64_t N = table_locations.count();
  bool is_retrying = false;
  bool on_same_server = true;
  need_check_on_same_server = true;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid executor ctx!", K(ret), K(plan_ctx));
  } else {
    ObSEArray<const ObTableLocation *, 2> table_location_ptrs;
    ObSEArray<ObCandiTableLoc *, 2> phy_location_info_ptrs;
    const ParamStore &params = plan_ctx->get_param_store();
    if (OB_FAIL(candi_table_locs.prepare_allocate(N))) {
      LOG_WARN("phy_locations_info prepare allocate error", K(ret), K(N));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        const ObTableLocation &table_location = table_locations.at(i);
        ObCandiTableLoc &candi_table_loc = candi_table_locs.at(i);
        NG_TRACE(calc_partition_location_begin);
        // 这里认为materialized view的复制表是每个server都有副本的，
        // 因此这里不判断是否能生成materialized view了，一定都能生成
        if (OB_FAIL(table_location.calculate_candi_tablet_locations(exec_ctx,
                                                                    params,
                                                                    candi_table_loc.get_phy_part_loc_info_list_for_update(),
                                                                    dtc_params))) {
          LOG_WARN("failed to calculate partition location", K(ret));
        } else {
          NG_TRACE(calc_partition_location_end);
          if (table_location.is_duplicate_table_not_in_dml()) {
            has_duplicate_tbl_not_in_dml = true;
          }
          candi_table_loc.set_duplicate_type(table_location.get_duplicate_type());
          candi_table_loc.set_table_location_key(
              table_location.get_table_id(), table_location.get_ref_table_id());
          LOG_DEBUG("plan cache util", K(candi_table_loc));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(table_location_ptrs.push_back(&table_location))) {
            LOG_WARN("failed to push back table location ptrs", K(ret), K(i),
                     K(N), K(table_locations.at(i)));
          } else if (OB_FAIL(phy_location_info_ptrs.push_back(&candi_table_loc))) {
            LOG_WARN("failed to push back phy location info ptrs", K(ret), K(i),
                     K(N), K(candi_table_locs.at(i)));
          } else if (OB_FAIL(pc_ctx.is_retry_for_dup_tbl(is_retrying))) {
            LOG_WARN("failed to test if retrying", K(ret));
          } else if (is_retrying) {
            LOG_INFO("Physical Location from Location Cache", K(candi_table_loc));
          }
        }
      } // for end
    }

    //Only check the on_same_server when has table location in the phy_plan.
    if (OB_SUCC(ret) && N!=0 ) {
      if (OB_FAIL(ObLogPlan::select_replicas(exec_ctx, table_location_ptrs,
                                             exec_ctx.get_addr(),
                                             phy_location_info_ptrs))) {
        LOG_WARN("failed to select replicas", K(ret), K(table_locations),
                 K(exec_ctx.get_addr()), K(phy_location_info_ptrs));
      } else if (!has_duplicate_tbl_not_in_dml || is_retrying) {
        // do nothing
      } else if (OB_FAIL(reselect_duplicate_table_best_replica(candi_table_locs,
                                                               on_same_server))) {
        LOG_WARN("failed to reselect replicas", K(ret));
      } else if (!on_same_server) {
        need_check_on_same_server = false;
      }
      LOG_TRACE("after select_replicas", K(on_same_server), K(has_duplicate_tbl_not_in_dml),
                K(candi_table_locs), K(table_locations), K(ret));
    }
  }

  return ret;
}

int ObPhyLocationGetter::build_table_locs(ObDASCtx &das_ctx,
                                          const ObIArray<ObTableLocation> &table_locations,
                                          const ObIArray<ObCandiTableLoc> &candi_table_locs)
{
  int ret = OB_SUCCESS;
  CK(table_locations.count() == candi_table_locs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < table_locations.count(); i++) {
    if (OB_FAIL(das_ctx.add_candi_table_loc(table_locations.at(i).get_loc_meta(), candi_table_locs.at(i)))) {
      LOG_WARN("add candi table location failed", K(ret), K(table_locations.at(i).get_loc_meta()));
    }
  }
  if (OB_FAIL(ret)) {
    das_ctx.clear_all_location_info();
  }

  return ret;
}

//this function will rewrite the related tablet map info in DASCtx
int ObPhyLocationGetter::build_related_tablet_info(const ObTableLocation &table_location,
                                                   ObExecContext &exec_ctx,
                                                   DASRelatedTabletMap *&related_map)
{
  int ret = OB_SUCCESS;
  ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(exec_ctx.get_my_session());
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObArray<ObObjectID> partition_ids;
  ObArray<ObObjectID> first_level_part_ids;
  ObArray<ObTabletID> tablet_ids;

  if (OB_FAIL(table_location.calculate_tablet_ids(exec_ctx,
                                                  plan_ctx->get_param_store(),
                                                  tablet_ids,
                                                  partition_ids,
                                                  first_level_part_ids,
                                                  dtc_params))) {
    LOG_WARN("calculate tablet ids failed", K(ret));
  } else {
    related_map = &exec_ctx.get_das_ctx().get_related_tablet_map();
    LOG_DEBUG("build_related tablet info", K(tablet_ids), K(partition_ids),
             K(table_location.get_loc_meta()), KPC(related_map));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableRowCount, op_id_, row_count_);

int ObConfigInfoInPC::load_influence_plan_config()
{
  int ret = OB_SUCCESS;
  // Note: if you need to add a tenant config please
  //        uncomment next line to retrive tenant config.
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));

  // For Cluster configs
  // here to add value of configs that can influence execution plan.
  enable_px_ordered_coord_ = GCONF._enable_px_ordered_coord;
  enable_newsort_ = GCONF._enable_newsort;
  is_strict_defensive_check_ = GCONF.enable_strict_defensive_check();
  is_enable_px_fast_reclaim_ = GCONF._enable_px_fast_reclaim;

  // For Tenant configs
  // tenant config use tenant_config to get configs
  if (tenant_config.is_valid()) {
    pushdown_storage_level_ = tenant_config->_pushdown_storage_level;
    rowsets_enabled_ = tenant_config->_rowsets_enabled;
    enable_px_batch_rescan_ = tenant_config->_enable_px_batch_rescan;
    bloom_filter_enabled_ = tenant_config->_bloom_filter_enabled;
    px_join_skew_handling_ = tenant_config->_px_join_skew_handling;
    px_join_skew_minfreq_ = static_cast<int8_t>(tenant_config->_px_join_skew_minfreq);
    min_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    enable_var_assign_use_das_ = tenant_config->_enable_var_assign_use_das;
  }

  return ret;
}

// reading values and generate strings
int ObConfigInfoInPC::serialize_configs(char *buf, int buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;

  // gen config str
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", pushdown_storage_level_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(pushdown_storage_level_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", rowsets_enabled_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(rowsets_enabled_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", enable_px_batch_rescan_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(enable_px_batch_rescan_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", enable_px_ordered_coord_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(enable_px_ordered_coord_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", bloom_filter_enabled_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(bloom_filter_enabled_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", enable_newsort_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(enable_newsort_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", px_join_skew_handling_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(px_join_skew_handling_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", is_strict_defensive_check_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(is_strict_defensive_check_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%d,", px_join_skew_minfreq_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(px_join_skew_minfreq_));

  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                               "%lu,", min_cluster_version_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(min_cluster_version_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                               "%d", is_enable_px_fast_reclaim_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(is_enable_px_fast_reclaim_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                               "%d", enable_var_assign_use_das_))) {
    SQL_PC_LOG(WARN, "failed to databuff_printf", K(ret), K(enable_var_assign_use_das_));
  } else {
    // do nothing
  }
  // trim last comma
  pos--;
  return ret;
}

}
}

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
#include "share/ob_i_data_access_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_executor.h"
#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace sql {
int64_t ObPlanCacheKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(name), K_(key_id), K_(db_id), K_(sessid), K_(is_ps_mode), K_(sys_vars_str), K_(namespace));
  J_OBJ_END();
  return pos;
}
int64_t ObPCKeyValue::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(key), KP(pcv_set_));
  J_OBJ_END();
  return pos;
}
int64_t ObSysVarInPC::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(system_variables));
  J_OBJ_END();
  return pos;
}
int64_t NotParamInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(idx), K_(raw_text));
  J_OBJ_END();
  return pos;
}
int64_t PsNotParamInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(idx), K_(ps_param));
  J_OBJ_END();
  return pos;
}
int64_t ObPCParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(node), K_(flag));
  J_OBJ_END();
  return pos;
}
int64_t ObPCConstParamInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(const_idx), K_(const_params));
  J_OBJ_END();
  return pos;
}
int64_t ObPCParamEqualInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(first_param_idx), K_(second_param_idx));
  J_OBJ_END();
  return pos;
}
int64_t ObFastParserResult::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(pc_key_), K(raw_params_), K(ps_params_), K(cache_params_));
  J_OBJ_END();
  return pos;
}
int64_t SelectItemParamInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(questions_pos), K_(params_idx), K_(neg_params_idx), K_(name_len), K_(esc_str_flag),K(common::ObString(name_len_, paramed_field_name_)));
  J_OBJ_END();
  return pos;
}
int64_t ObPlanCacheCtx::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(is_ps_mode_), K(raw_sql_), K(need_real_add_), K(add_pre_acs_), K(not_param_info_), K(not_param_var_),K(not_param_index_), K(neg_param_index_), K(param_charset_type_), K(outlined_sql_len_), K(should_add_plan_));
  J_OBJ_END();
  return pos;
}
int64_t ObPlanCacheStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("access_count", access_count_, "hit_count", hit_count_);
  J_OBJ_END();
  return pos;
}
int64_t ObOperatorStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(plan_id), K_(operation_id), K_(execute_times), K_(input_rows), K_(rescan_times), K_(output_rows));
  J_OBJ_END();
  return pos;
}
int64_t ObAcsIdxSelRange::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(index_name), K_(low_bound_sel), K_(high_bound_sel));
  J_OBJ_END();
  return pos;
}
int64_t ObTableScanStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(query_range_row_count), K_(indexback_row_count), K_(output_row_count), K_(bf_filter_cnt),K_(bf_access_cnt), K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(fuse_row_cache_hit_cnt),K_(fuse_row_cache_miss_cnt));
  J_OBJ_END();
  return pos;
}
int64_t ObTableRowCount::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(op_id), K_(row_count));
  J_OBJ_END();
  return pos;
}
int64_t ObPlanStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(plan_id), "sql_text", ObString(stmt_len_, stmt_), K_(raw_sql), K_(gen_time), K_(schema_version),K_(last_active_time), K_(hit_count), K_(mem_used), K_(slow_count), K_(slowest_exec_time), K_(slowest_exec_usec),K_(execute_times), K_(disk_reads), K_(direct_writes), K_(buffer_gets), K_(application_wait_time),K_(concurrency_wait_time), K_(user_io_wait_time), K_(rows_processed), K_(elapsed_time), K_(cpu_time),K_(large_querys), K_(delayed_large_querys), K_(outline_version), K_(outline_id), K_(is_evolution),K_(is_last_open_succ), K_(is_bind_sensitive), K_(is_bind_aware), K_(is_last_open_succ), K_(timeout_count),K_(bl_info));
  J_OBJ_END();
  return pos;
}
int64_t SysVarNameVal::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(name), K_(value));
  J_OBJ_END();
  return pos;
}
int64_t StmtStat::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(memory_used), K_(last_active_timestamp), K_(execute_average_time), K_(execute_slowest_time),K_(execute_slowest_timestamp), K_(execute_count), K_(execute_slow_count), K_(ps_count), K_(to_delete));
  J_OBJ_END();
  return pos;
}

const char* plan_cache_gc_confs[3] = {"OFF", "REPORT", "AUTO"};

int ObGetAllPlanIdOp::set_key_array(common::ObIArray<uint64_t>* key_array)
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

int ObGetAllPlanIdOp::operator()(common::hash::HashMapPair<ObCacheObjID, ObCacheObject*>& entry)
{
  int ret = common::OB_SUCCESS;
  if (NULL == key_array_) {
    ret = common::OB_NOT_INIT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(key_array_->push_back(entry.first))) {
    SQL_PC_LOG(WARN, "fail to push back plan_id", K(ret));
  }
  return ret;
}

int ObPlanCacheCtx::is_retry(bool& v) const
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

int ObPlanCacheCtx::is_retry_for_dup_tbl(bool& v) const
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

int ObPhyLocationGetter::get_phy_locations(const common::ObIArray<ObTablePartitionInfo*>& partition_infos,
    ObIArray<ObPhyTableLocation>& phy_locations, ObIArray<ObPhyTableLocationInfo>& phy_location_infos)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocation phy_location;
  int64_t N = partition_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    phy_location.reset();
    if (OB_ISNULL(partition_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition info", K(ret));
    } else if (OB_FAIL(
                   phy_location.assign_from_phy_table_loc_info(partition_infos.at(i)->get_phy_tbl_location_info()))) {
      LOG_WARN("failed to assign_from_phy_table_loc_info", K(ret), K(i));
    } else if (OB_FAIL(phy_locations.push_back(phy_location))) {
      LOG_WARN("failed to push_back phy_location", K(ret), K(phy_location));
    } else if (OB_FAIL(phy_location_infos.push_back(partition_infos.at(i)->get_phy_tbl_location_info()))) {
      LOG_WARN("failed to push_back phy_location_info", K(ret), K(partition_infos.at(i)->get_phy_tbl_location_info()));
    } else { /* do nothing */
    }
  }
  return ret;
}

// In the case of a copy table, after selecting the copy,
// adjust the copy selection of the copy table to make it consistent with
// the location of the non-replicated table (provided that the non-replicated tables are on the same server)
// The advantage is that the plan type can be made from DIST --> REMOTE;
// can not be calculated in ObSqlPlanSet::calc_phy_plan_type_by_proj
// The reason is that get_phy_locations will assign the physical location to task_exec_ctx;
//(implementation refers to is_partition_in_same_server_by_proj)
int ObPhyLocationGetter::reselect_duplicate_table_best_replica(
    const ObIArray<ObPhyTableLocationInfo>& phy_locations, bool& on_same_server)
{
  int ret = OB_SUCCESS;
  bool has_duplicate_tbl = false;
  bool is_same = true;
  ObAddr normal_table_addr;
  ObAddr duplicate_table_addr;
  ObSEArray<ObAddr, 4> candi_addrs;
  ObSEArray<int64_t, 8> new_replic_idxs;
  int64_t proj_cnt = phy_locations.count();
  ObReplicaLocation replica_location;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < proj_cnt; ++i) {
    const ObPhyTableLocationInfo& ptli = phy_locations.at(i);
    if (ptli.get_partition_cnt() > 1) {
      is_same = false;
    } else if (ptli.get_partition_cnt() > 0) {
      const ObPhyPartitionLocationInfo& part_info = ptli.get_phy_part_loc_info_list().at(0);
      if (OB_FAIL(part_info.get_selected_replica(replica_location))) {
        SQL_PC_LOG(WARN, "fail to get selected replica", K(ret), K(ptli));
      } else if (!replica_location.is_valid()) {
        SQL_PC_LOG(WARN, "replica_location is invalid", K(ret), K(replica_location));
      } else if (!ptli.is_duplicate_table_not_in_dml()) {
        // handle normal table
        if (!normal_table_addr.is_valid()) {
          normal_table_addr = replica_location.server_;
          SQL_PC_LOG(DEBUG, "part_location first replica", K(ret), K(replica_location));
        } else if (normal_table_addr != replica_location.server_) {
          is_same = false;
          SQL_PC_LOG(DEBUG, "part_location replica", K(ret), K(i), K(replica_location));
        }
      } else {
        // handle duplicate table
        if (!has_duplicate_tbl) {
          const ObIArray<ObRoutePolicy::CandidateReplica>& replicas =
              part_info.get_partition_location().get_replica_locations();
          for (int64_t j = 0; OB_SUCC(ret) && j < replicas.count(); ++j) {
            if (OB_FAIL(candi_addrs.push_back(replicas.at(j).server_))) {
              LOG_WARN("failed to push back servers", K(ret));
            }
          }
          duplicate_table_addr = replica_location.server_;
          has_duplicate_tbl = true;
          SQL_PC_LOG(DEBUG, "has duplicate table");
        } else if (duplicate_table_addr != replica_location.server_) {
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
  if (OB_SUCC(ret) && !candi_addrs.empty()) {
    is_same = false;
    if (OB_FAIL(new_replic_idxs.prepare_allocate(proj_cnt))) {
      SQL_PC_LOG(WARN, "failed to pre-alloc array space", K(ret), K(proj_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_same && i < candi_addrs.count(); ++i) {
      bool is_valid = true;
      const ObAddr& addr = candi_addrs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < proj_cnt; ++j) {
        const ObPhyTableLocationInfo& ptli = phy_locations.at(j);
        if (ptli.is_duplicate_table_not_in_dml()) {
          is_valid = ptli.get_phy_part_loc_info_list().at(0).is_server_in_replica(addr, new_replic_idxs.at(j));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < proj_cnt; ++j) {
        ObPhyTableLocationInfo& ptli = const_cast<ObPhyTableLocationInfo&>(phy_locations.at(j));
        if (!ptli.is_duplicate_table_not_in_dml()) {
          // do nothing
        } else if (OB_FAIL(ptli.get_phy_part_loc_info_list_for_update().at(0).set_selected_replica_idx(
                       new_replic_idxs.at(j)))) {
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

int ObPhyLocationGetter::get_phy_locations(const ObIArray<ObTableLocation>& table_locations,
    const ObPlanCacheCtx& pc_ctx, share::ObIPartitionLocationCache& location_cache,
    ObIArray<ObPhyTableLocationInfo>& phy_location_infos, bool& need_check_on_same_server)
{
  int ret = OB_SUCCESS;
  bool has_duplicate_tbl_not_in_dml = false;
  ObExecContext& exec_ctx = pc_ctx.exec_ctx_;
  ObSchemaGetterGuard* schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(pc_ctx.sql_ctx_.session_info_);
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  ObTaskExecutorCtx* task_exec_ctx = exec_ctx.get_task_executor_ctx();
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  int64_t N = table_locations.count();
  bool is_retrying = false;
  bool on_same_server = true;
  need_check_on_same_server = true;
  if (OB_ISNULL(session) || OB_ISNULL(task_exec_ctx) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid executor ctx!", K(ret), K(session), K(task_exec_ctx), K(plan_ctx));
  } else {
    ObSEArray<const ObTableLocation*, 2> table_location_ptrs;
    ObSEArray<ObPhyTableLocationInfo*, 2> phy_location_info_ptrs;
    const ParamStore& params = plan_ctx->get_param_store();
    ObPhyTableLocationIArray& phy_locations = task_exec_ctx->get_table_locations();
    phy_locations.reset();
    if (OB_FAIL(phy_locations.prepare_allocate(N))) {
      LOG_WARN("phy_locations prepare allocate error", K(ret), K(N));
    } else if (OB_FAIL(phy_location_infos.prepare_allocate(N))) {
      LOG_WARN("phy_locations_info prepare allocate error", K(ret), K(N));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        const ObTableLocation& table_location = table_locations.at(i);
        ObPhyTableLocationInfo& phy_location_info = phy_location_infos.at(i);
        NG_TRACE(calc_partition_location_begin);
        // It is believed that the copy table of materialized view is a copy of each server,
        // Therefore, it is not judged whether the materialized view can be generated,
        // it must be generated
        if (OB_FAIL(table_location.calculate_partition_location_infos(exec_ctx,
                schema_guard,
                params,
                location_cache,
                phy_location_info.get_phy_part_loc_info_list_for_update(),
                dtc_params,
                true /* non-block */))) {
          LOG_WARN("failed to calculate partition location", K(ret));
        } else {
          NG_TRACE(calc_partition_location_end);
          if (table_location.is_duplicate_table_not_in_dml()) {
            has_duplicate_tbl_not_in_dml = true;
          }
          phy_location_info.set_duplicate_type(table_location.get_duplicate_type());
          phy_location_info.set_table_location_key(table_location.get_table_id(), table_location.get_ref_table_id());
          LOG_DEBUG("plan cache utitl", K(phy_location_info));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(table_location_ptrs.push_back(&table_location))) {
            LOG_WARN("failed to push back table location ptrs", K(ret), K(i), K(N), K(table_locations.at(i)));
          } else if (OB_FAIL(phy_location_info_ptrs.push_back(&phy_location_info))) {
            LOG_WARN("failed to push back phy location info ptrs", K(ret), K(i), K(N), K(phy_location_infos.at(i)));
          } else if (OB_FAIL(pc_ctx.is_retry_for_dup_tbl(is_retrying))) {
            LOG_WARN("failed to test if retrying", K(ret));
          } else if (is_retrying) {
            LOG_INFO("Physical Location from Location Cache", K(phy_location_info));
          }
        }
      }  // for end

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObLogPlan::select_replicas(
                exec_ctx, table_location_ptrs, exec_ctx.get_addr(), phy_location_info_ptrs))) {
          LOG_WARN("failed to select replicas",
              K(ret),
              K(table_locations),
              K(exec_ctx.get_addr()),
              K(phy_location_info_ptrs));
        } else if (!has_duplicate_tbl_not_in_dml || is_retrying) {
          // do nothing
        } else if (OB_FAIL(reselect_duplicate_table_best_replica(phy_location_infos, on_same_server))) {
          LOG_WARN("failed to reselect replicas", K(ret));
        } else if (!on_same_server) {
          need_check_on_same_server = false;
        }
        LOG_DEBUG(
            "after select_replicas", K(on_same_server), K(has_duplicate_tbl_not_in_dml), K(phy_location_infos), K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        const ObTableLocation& table_location = table_locations.at(i);
        ObPhyTableLocation& phy_location = phy_locations.at(i);
        ObPhyTableLocationInfo& phy_location_info = phy_location_infos.at(i);

        if (OB_FAIL(phy_location_info.set_direction(table_location.get_direction()))) {
          LOG_WARN("failed to set phy location info direction", K(ret), K(table_location));
        } else if (OB_FAIL(phy_location.assign_from_phy_table_loc_info(phy_location_info))) {
          LOG_WARN("failed to assign from phy table loc info", K(ret), K(phy_location_info));
        }
      }
    }
  }

  return ret;
}

int ObPlanBaselineHeler::init_baseline_params_info_str(
    const Ob2DArray<ObParamInfo, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false>& params_info,
    ObIAllocator& allocer, ObString& param_info_str)
{
  int ret = OB_SUCCESS;
  int64_t N = params_info.count();
  int64_t buf_len = N * ObParamInfo::MAX_STR_DES_LEN + 1;
  int64_t pos = 0;
  char* buf = (char*)allocer.alloc(buf_len);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for param info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (N - 1 != i) {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "{%d,%d,%d,%d,%d},",
                params_info.at(i).flag_.need_to_check_type_,
                params_info.at(i).flag_.need_to_check_bool_value_,
                params_info.at(i).flag_.expected_bool_value_,
                params_info.at(i).scale_,
                params_info.at(i).type_))) {
          SQL_PC_LOG(WARN, "fail to buff_print param info", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "{%d,%d,%d,%d,%d}",
                params_info.at(i).flag_.need_to_check_type_,
                params_info.at(i).flag_.need_to_check_bool_value_,
                params_info.at(i).flag_.expected_bool_value_,
                params_info.at(i).scale_,
                params_info.at(i).type_))) {
          SQL_PC_LOG(WARN, "fail to buff_print param info", K(ret));
        }
      }
    }  // for end
  }
  if (OB_SUCC(ret)) {
    param_info_str.assign_ptr(buf, pos);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableRowCount, op_id_, row_count_);

}  // namespace sql
}  // namespace oceanbase

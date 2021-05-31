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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace sql {

int ObTablePartitionInfo::assign(const ObTablePartitionInfo& other)
{
  int ret = OB_SUCCESS;
  table_location_ = other.table_location_;
  if (OB_FAIL(phy_tbl_location_info_.assign(other.phy_tbl_location_info_))) {
    LOG_WARN("fail to assign phy_tbl_location_info_", K(ret), K(phy_tbl_location_info_));
  }
  return ret;
}

int ObTablePartitionInfo::init_table_location_with_rowid(ObSqlSchemaGuard& schema_guard,
    const ObIArray<int64_t>& param_idx, const uint64_t table_id, const uint64_t ref_table_id,
    ObSQLSessionInfo& session_info, const bool is_dml_table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_location_.init_table_location_with_rowid(
          schema_guard, param_idx, table_id, ref_table_id, session_info, is_dml_table))) {
    LOG_WARN("failed to init table location with rowid", K(ret));
  } else {
    LOG_TRACE("init table location with rowid succeed", K(param_idx), K(table_id), K(ref_table_id));
  }
  return ret;
}

int ObTablePartitionInfo::init_table_location(ObSqlSchemaGuard& schema_guard, ObDMLStmt& stmt,
    ObSQLSessionInfo* session_info, const ObIArray<ObRawExpr*>& filter_exprs, const uint64_t table_id,
    const uint64_t ref_table_id,
    // const uint64_t index_table_id,
    const ObPartHint* part_hint_, const common::ObDataTypeCastParams& dtc_params, bool is_dml_table,
    ObIArray<ObRawExpr*>* sort_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_location_.init(schema_guard,
            stmt,
            session_info,
            filter_exprs,
            table_id,
            ref_table_id,
            part_hint_,
            dtc_params,
            is_dml_table,
            sort_exprs))) {
      LOG_WARN("fail to init table location", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ref_table_id), K(ret));
    } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope()) {
      phy_tbl_location_info_.set_duplicate_type(
          is_dml_table ? ObDuplicateType::DUPLICATE_IN_DML : ObDuplicateType::DUPLICATE);
    }
  }
  if (OB_SUCC(ret) && !stmt.is_insert_stmt()) {
    common::ObIArray<ObDMLStmt::PartExprArray>& related_part_expr_arrays = stmt.get_related_part_expr_arrays();
    for (int64_t i = 0; OB_SUCC(ret) && i < related_part_expr_arrays.count(); i++) {
      if (table_id == related_part_expr_arrays.at(i).table_id_ &&
          ref_table_id == related_part_expr_arrays.at(i).index_tid_) {
        table_location_array_size_ = related_part_expr_arrays.at(i).part_expr_array_.count();
        if (table_location_array_size_ > 0) {
          table_location_array_ =
              (ObTableLocation*)allocator_.alloc(sizeof(ObTableLocation) * table_location_array_size_);
          if (table_location_array_ == NULL) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate table location array");
          }
        }

        for (int64_t j = 0; OB_SUCC(ret) && j < table_location_array_size_; j++) {
          new (table_location_array_ + j) ObTableLocation(allocator_);
          table_location_array_[j].set_related_part_expr_idx(static_cast<int32_t>(j));
          if (OB_FAIL(table_location_array_[j].init(schema_guard,
                  stmt,
                  session_info,
                  filter_exprs,
                  table_id,
                  ref_table_id,
                  part_hint_,
                  dtc_params,
                  is_dml_table,
                  sort_exprs))) {
            LOG_WARN("fail to init table_location_array", K(ret));
          }
        }
        break;
      }
    }
  }
  return ret;
}

int ObTablePartitionInfo::get_not_insert_dml_part_sort_expr(ObDMLStmt& stmt, ObIArray<ObRawExpr*>* sort_exprs) const
{
  return table_location_.get_not_insert_dml_part_sort_expr(stmt, sort_exprs);
}

int ObTablePartitionInfo::calculate_phy_table_location_info(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const ParamStore& params, ObIPartitionLocationCache& location_cache, const common::ObDataTypeCastParams& dtc_params)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocationInfo phy_table_location_info;
  if (OB_FAIL(table_location_.calculate_partition_location_infos(exec_ctx,
          part_mgr,
          params,
          location_cache,
          phy_tbl_location_info_.get_phy_part_loc_info_list_for_update(),
          dtc_params))) {
    LOG_WARN("Failed to calculate table location", K(ret));
  } else {
    phy_tbl_location_info_.set_table_location_key(table_location_.get_table_id(), table_location_.get_ref_table_id());
  }

  if (best_location_idx_ == OB_INVALID_INDEX) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_location_array_size_; i++) {
      if (OB_FAIL(table_location_array_[i].calculate_partition_location_infos(exec_ctx,
              part_mgr,
              params,
              location_cache,
              phy_table_location_info.get_phy_part_loc_info_list_for_update(),
              dtc_params))) {
        LOG_WARN("Failed to calculate table location", K(ret));
      } else {
        phy_table_location_info.set_table_location_key(
            table_location_array_[i].get_table_id(), table_location_array_[i].get_ref_table_id());
        if (phy_table_location_info.get_phy_part_loc_info_list().count() <
            phy_tbl_location_info_.get_phy_part_loc_info_list().count()) {
          best_location_idx_ = i;
          if (OB_FAIL(phy_tbl_location_info_.assign(phy_table_location_info))) {
            LOG_WARN("fail to assign", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && best_location_idx_ != OB_INVALID_INDEX) {
      table_location_ = table_location_array_[best_location_idx_];
    }
  } else {
    LOG_WARN("invalid best_location_idx", K_(best_location_idx), K_(table_location_array_size));
  }
  return ret;
}

int ObTablePartitionInfo::calculate_phy_table_location_info(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const ParamStore& params, share::ObIPartitionLocationCache& location_cache,
    const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>& partition_ids)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocationInfo phy_table_location_info;
  if (OB_FAIL(table_location_.set_partition_locations(exec_ctx,
          location_cache,
          table_location_.get_ref_table_id(),
          partition_ids,
          phy_tbl_location_info_.get_phy_part_loc_info_list_for_update()))) {
    LOG_WARN("failed to set partition locations", K(ret));
  } else {
    phy_tbl_location_info_.set_table_location_key(table_location_.get_table_id(), table_location_.get_ref_table_id());
  }

  if (best_location_idx_ == OB_INVALID_INDEX) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_location_array_size_; i++) {
      if (OB_FAIL(table_location_array_[i].calculate_partition_location_infos(exec_ctx,
              part_mgr,
              params,
              location_cache,
              phy_table_location_info.get_phy_part_loc_info_list_for_update(),
              dtc_params))) {
        LOG_WARN("Failed to calculate table location", K(ret));
      } else {
        phy_table_location_info.set_table_location_key(
            table_location_array_[i].get_table_id(), table_location_array_[i].get_ref_table_id());
        if (phy_table_location_info.get_phy_part_loc_info_list().count() <
            phy_tbl_location_info_.get_phy_part_loc_info_list().count()) {
          best_location_idx_ = i;
          if (OB_FAIL(phy_tbl_location_info_.assign(phy_table_location_info))) {
            LOG_WARN("fail to assign", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && best_location_idx_ != OB_INVALID_INDEX) {
      table_location_ = table_location_array_[best_location_idx_];
    }
  } else {
    LOG_WARN("invalid best_location_idx", K_(best_location_idx), K_(table_location_array_size));
  }
  return ret;
}

int ObTablePartitionInfo::calculate_phy_table_location_info(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard* schema_guard, const ParamStore& params,
    share::ObIPartitionLocationCache& location_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null schema guard", K(ret));
  } else if (OB_FAIL(table_location_.calc_partition_location_infos_with_rowid(exec_ctx,
                 *schema_guard,
                 params,
                 location_cache,
                 phy_tbl_location_info_.get_phy_part_loc_info_list_for_update()))) {
    LOG_WARN("failed to calc partition infos", K(ret));
  } else {
    phy_tbl_location_info_.set_duplicate_type(table_location_.get_duplicate_type());
    phy_tbl_location_info_.set_table_location_key(table_location_.get_table_id(), table_location_.get_ref_table_id());
    LOG_TRACE("calculated partiton location infos", K(phy_tbl_location_info_));
  }
  return ret;
}

int ObTablePartitionInfo::calc_phy_table_loc_and_select_leader(ObExecContext& exec_ctx, ObPartMgr* part_mgr,
    const ParamStore& params, ObIPartitionLocationCache& location_cache, const common::ObDataTypeCastParams& dtc_params,
    const uint64_t index_table_id, const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  bool is_on_same_server = true;
  ObAddr same_server;
  if (OB_FAIL(calculate_phy_table_location_info(exec_ctx, part_mgr, params, location_cache, dtc_params))) {
    LOG_WARN("fail to calculate phy table location info", K(ret));
  } else if (OB_FAIL(phy_tbl_location_info_.all_select_leader(is_on_same_server, same_server))) {
    LOG_WARN("fail to all select leader", K(ret), K(phy_tbl_location_info_));
    ObPhyTableLocationInfo phy_tbl_location_info;
    ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(task_exec_ctx)) {
      // don't overwirte err code
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_exec_ctx not inited", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = phy_tbl_location_info.assign(phy_tbl_location_info_))) {
      LOG_WARN("fail to assign", K(tmp_ret), K(phy_tbl_location_info_));
    } else {
      phy_tbl_location_info.set_direction(UNORDERED);
      ObPhyPartitionLocationInfoIArray& info_array = phy_tbl_location_info.get_phy_part_loc_info_list_for_update();

      for (int64_t i = 0; i < info_array.count() && OB_SUCCESS == tmp_ret; i++) {
        ObPhyPartitionLocationInfo& info = info_array.at(i);
        if (info.get_partition_location().get_replica_locations().count() <= 0) {
          // nothing todo
        } else if (OB_SUCCESS != (tmp_ret = info.set_selected_replica_idx(0))) {
          LOG_WARN("fail to set selected replica index", KR(ret));
        }
      }

      if (OB_SUCCESS != tmp_ret) {
        // nothing todo
      } else if (OB_SUCCESS != (tmp_ret = task_exec_ctx->append_table_location(phy_tbl_location_info))) {
        LOG_WARN("fail append table locaion info", K(ret), K(tmp_ret));
      }
    }
  } else if (OB_FAIL(set_log_op_infos(index_table_id, direction))) {
    LOG_WARN("fail to set log op infos", K(ret), K(index_table_id), K(direction));
  }
  return ret;
}

int ObTablePartitionInfo::calc_phy_table_loc_and_select_fixed_location(ObExecContext& exec_ctx,
    const ObAddr& fixed_server, ObPartMgr* part_mgr, const ParamStore& params,
    ObIPartitionLocationCache& location_cache, const common::ObDataTypeCastParams& dtc_params,
    const uint64_t index_table_id, const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(session));
  } else if (OB_FAIL(calculate_phy_table_location_info(exec_ctx, part_mgr, params, location_cache, dtc_params))) {
    LOG_WARN("fail to calculate phy table location info", K(ret));
  } else if (OB_FAIL(phy_tbl_location_info_.all_select_fixed_server(fixed_server))) {
    LOG_WARN("fail to all select fixed server",
        K(ret),
        K(fixed_server),
        K(session->get_retry_info()),
        K(phy_tbl_location_info_));
  } else if (OB_FAIL(set_log_op_infos(index_table_id, direction))) {
    LOG_WARN("fail to set log op infos", K(ret), K(index_table_id), K(direction));
  }
  return ret;
}

int ObTablePartitionInfo::set_log_op_infos(const uint64_t index_table_id, const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_location_.set_log_op_infos(index_table_id, direction))) {
    LOG_WARN("fail to set log op infos", K(ret), K(index_table_id), K(direction));
  } else if (OB_FAIL(phy_tbl_location_info_.set_direction(table_location_.get_direction()))) {
    LOG_WARN("fail to set phy tbl loc info direction", K(ret), K(table_location_.get_direction()));
  }
  return ret;
}

int ObTablePartitionInfo::get_location_type(const common::ObAddr& server, ObTableLocationType& type) const
{
  const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_location_info_.get_phy_part_loc_info_list();
  return table_location_.get_location_type(server, phy_part_loc_info_list, type);
}

int ObTablePartitionInfo::get_all_servers(ObIArray<common::ObAddr>& servers) const
{
  int ret = OB_SUCCESS;
  const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_location_info_.get_phy_part_loc_info_list();
  FOREACH_CNT_X(it, phy_part_loc_info_list, OB_SUCC(ret))
  {
    share::ObReplicaLocation replica_location;
    if (OB_FAIL((*it).get_selected_replica(replica_location))) {
      LOG_WARN("fail to get selected replica", K(*it));
    } else if (!replica_location.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("replica location is invalid", K(ret), K(replica_location));
    } else if (OB_FAIL(add_var_to_array_no_dup(servers, replica_location.server_))) {
      LOG_WARN("failed to push back server", K(ret));
    }
  }
  return ret;
}

int ObTablePartitionInfo::get_all_local_partition_key(
    ObIArray<ObPartitionKey>& pkeys, const common::ObAddr& server) const
{
  int ret = OB_SUCCESS;
  const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_location_info_.get_phy_part_loc_info_list();
  FOREACH_CNT_X(it, phy_part_loc_info_list, OB_SUCC(ret))
  {
    share::ObReplicaLocation replica_location;
    ObPartitionKey pkey;
    if (OB_FAIL(it->get_selected_replica(replica_location))) {
      LOG_WARN("failed to get selected replica", K(*it));
    } else if (!replica_location.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("replica location is invalid", K(ret), K(replica_location));
    } else if (server != replica_location.server_) {
      // the partition is not here(this server)
    } else if (OB_FAIL(it->get_partition_location().get_partition_key(pkey))) {
      LOG_WARN("get partition key faile", K(ret), K(*it));
    } else if (OB_FAIL(pkeys.push_back(pkey))) {
      LOG_WARN("push back pkey failed", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

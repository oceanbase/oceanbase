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

#define USING_LOG_PREFIX SQL_JO
#include "sql/optimizer/ob_join_order.h"
#include "share/object/ob_obj_cast.h"
#include "share/stat/ob_table_stat.h"
#include "sql/ob_sql_utils.h"
#include "sql/rewrite/ob_equal_analysis.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_skyline_prunning.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/engine/join/ob_block_based_nested_loop_join.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "common/ob_smart_call.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using common::ObArray;
using oceanbase::share::schema::ObColDesc;
using oceanbase::share::schema::ObColumnSchemaV2;
using oceanbase::share::schema::ObTableSchema;
using share::schema::ObSchemaGetterGuard;

class OrderingInfo;
class QueryRangeInfo;

#define OPT_CTX (get_plan()->get_optimizer_context())

int ConflictDetector::build_confict(common::ObIAllocator& allocator, ConflictDetector*& detector)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(detector = static_cast<ConflictDetector*>(allocator.alloc(sizeof(ConflictDetector))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for conflict detector failed");
  } else {
    new (detector) ConflictDetector();
  }
  return ret;
}

ObJoinOrder::~ObJoinOrder()
{}

int ObJoinOrder::fill_query_range_info(const QueryRangeInfo& range_info, ObCostTableScanInfo& est_cost_info)
{
  int ret = OB_SUCCESS;
  const ObQueryRangeArray& ranges = range_info.get_ranges();
  oceanbase::storage::ObTableScanParam& table_scan_param = est_cost_info.table_scan_param_;
  est_cost_info.ranges_.reset();
  table_scan_param.key_ranges_.reset();
  // maintain query range info
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (OB_ISNULL(ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range is null", K(ret));
    } else if (OB_FAIL(est_cost_info.ranges_.push_back(*ranges.at(i)))) {
      LOG_WARN("failed to add range", K(ret));
    } else if ((!est_cost_info.is_virtual_table_ ||
                   is_oracle_mapping_real_virtual_table(est_cost_info.ref_table_id_)) &&
               OB_FAIL(table_scan_param.key_ranges_.push_back(*ranges.at(i)))) {
      LOG_WARN("failed to add range", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_location_for_paths(
    const bool is_inner_path, ObIArray<AccessPath*>& access_paths, ObIArray<ObTablePartitionInfo*>& tbl_part_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(allocator_), K(ret));
  } else if (!is_inner_path) {
    if (OB_FAIL(compute_table_location(
            table_id_, table_meta_info_.ref_table_id_, access_paths, false, table_partition_info_))) {
      LOG_WARN("failed to calc table location", K(ret));
    } else if (OB_FAIL(get_plan()->get_optimizer_context().get_table_partition_info_list().push_back(
                   &table_partition_info_))) {
      LOG_WARN("failed to push back table partition info", K(ret));
    } else if (OB_FAIL(tbl_part_infos.push_back(&table_partition_info_))) {
      LOG_WARN("failed to push back table partition info", K(ret));
    }
  }
  // compute table location for global index
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    AccessPath* path = NULL;
    if (OB_ISNULL(path = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(path), K(ret));
    } else if (!path->is_global_index_) {
      path->table_partition_info_ = &table_partition_info_;
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < available_access_paths_.count(); ++j) {
        AccessPath* cur_path = available_access_paths_.at(j);
        if (OB_ISNULL(cur_path)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (path->table_id_ == cur_path->table_id_ && path->ref_table_id_ == cur_path->ref_table_id_ &&
                   path->index_id_ == cur_path->index_id_) {
          path->table_partition_info_ = cur_path->table_partition_info_;
          break;
        }
      }
      if (OB_SUCC(ret) && NULL == path->table_partition_info_) {
        ObTablePartitionInfo* table_partition_info = NULL;
        if (OB_ISNULL(table_partition_info =
                          reinterpret_cast<ObTablePartitionInfo*>(allocator_->alloc(sizeof(ObTablePartitionInfo))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate memory", K(table_partition_info), K(ret));
        } else {
          table_partition_info = new (table_partition_info) ObTablePartitionInfo(*allocator_);
          if (OB_FAIL(compute_table_location(
                  path->table_id_, path->index_id_, access_paths, true, *table_partition_info))) {
            LOG_WARN("failed to calc table location", K(ret));
          } else if (OB_FAIL(get_plan()->get_optimizer_context().get_table_partition_info_list().push_back(
                         table_partition_info))) {
            LOG_WARN("failed to push back table partition info", K(ret));
          } else if (OB_FAIL(tbl_part_infos.push_back(table_partition_info))) {
            LOG_WARN("failed to push back table partition info", K(ret));
          } else {
            path->table_partition_info_ = table_partition_info;
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_location(const uint64_t table_id, const uint64_t ref_table_id,
    ObIArray<AccessPath*>& access_paths, const bool is_global_index, ObTablePartitionInfo& table_partition_info)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObSqlSchemaGuard* sql_schema_guard = NULL;
  ObDMLStmt* stmt = NULL;
  const ParamStore* params = NULL;
  ObIPartitionLocationCache* location_cache = NULL;
  ObExecContext* exec_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObSQLSessionInfo* session_info = NULL;

  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_schema_guard()) ||
      OB_ISNULL(sql_schema_guard = opt_ctx->get_sql_schema_guard()) ||
      OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) || OB_ISNULL(params = opt_ctx->get_params()) ||
      OB_ISNULL(location_cache = opt_ctx->get_location_cache()) || OB_ISNULL(exec_ctx = opt_ctx->get_exec_ctx()) ||
      OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx()) || OB_ISNULL(phy_plan_ctx = exec_ctx->get_physical_plan_ctx()) ||
      OB_ISNULL(session_info = sql_ctx->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("These Arguments should not be NULL",
        K(get_plan()),
        K(opt_ctx),
        K(schema_guard),
        KP(sql_schema_guard),
        K(stmt),
        K(params),
        K(location_cache),
        K(exec_ctx),
        K(sql_ctx),
        K(phy_plan_ctx),
        K(session_info),
        K(ret));
  } else {
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
    const ObPartHint* part_hint = NULL;  // get part hint
    // check whether the ref table will be modified by dml operator
    bool is_dml_table = false;
    ObDMLStmt* top_stmt = opt_ctx->get_root_stmt();
    if (!is_global_index && stmt->get_stmt_hint().part_hints_.count() > 0) {
      part_hint = stmt->get_stmt_hint().get_part_hint(table_id);
    }
    if (NULL != top_stmt) {
      if (top_stmt->is_explain_stmt()) {
        top_stmt = static_cast<ObExplainStmt*>(top_stmt)->get_explain_query_stmt();
      }
      if (OB_ISNULL(top_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("top stmt should not be null", K(ret));
      } else {
        is_dml_table = top_stmt->check_table_be_modified(ref_table_id);
      }
    }

    // this place restrict_info include sub_query.
    ObArray<ObRawExpr*> correlated_filters;
    ObArray<ObRawExpr*> uncorrelated_filters;
    bool rowid_part_id_calc_done = false;
    if (OB_FAIL(ObOptimizerUtil::extract_parameterized_correlated_filters(
            get_restrict_infos(), params->count(), correlated_filters, uncorrelated_filters))) {
      LOG_WARN("Failed to extract correlated filters", K(ret));
    } else if (OB_FAIL(try_calculate_partitions_with_rowid(table_id,
                   ref_table_id,
                   is_dml_table,
                   access_paths,
                   uncorrelated_filters,
                   *sql_schema_guard,
                   *params,
                   *exec_ctx,
                   *location_cache,
                   *session_info,
                   table_partition_info,
                   rowid_part_id_calc_done))) {
      LOG_WARN("failed trying calculate partitions withs rowid", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (rowid_part_id_calc_done) {
      LOG_TRACE("succeed to calculate base table sharding info",
          K(table_id),
          K(ref_table_id),
          K(is_global_index),
          K(table_partition_info_));
    } else if (OB_FAIL(table_partition_info.init_table_location(*sql_schema_guard,
                   *stmt,
                   exec_ctx->get_my_session(),
                   uncorrelated_filters,
                   table_id,
                   ref_table_id,
                   part_hint,
                   dtc_params,
                   is_dml_table))) {
      LOG_WARN("Failed to initialize table location", K(ret));
    } else if (has_array_binding_param(*params)) {
      ObSEArray<int64_t, 16> partition_ids;
      ObSEArray<int64_t, 16> no_dup_partition_ids;
      if (OB_FAIL(ObSqlPlanSet::get_table_array_binding_parition_ids(table_partition_info.get_table_location(),
              opt_ctx->get_session_info(),
              *params,
              schema_guard,
              *exec_ctx,
              partition_ids))) {
        LOG_WARN("failed to get array binding partition ids", K(ret));
      } else if (OB_FAIL(append_array_no_dup(no_dup_partition_ids, partition_ids))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else if (!opt_ctx->is_batched_multi_stmt() && 1 != no_dup_partition_ids.count()) {
        ret = OB_ARRAY_BINDING_ROLLBACK;
        LOG_TRACE("array binding needs rollback", K(ret));
      } else if (OB_FAIL(table_partition_info.calculate_phy_table_location_info(
                     *exec_ctx, schema_guard, *params, *location_cache, dtc_params, no_dup_partition_ids))) {
        LOG_WARN("Failed to calculate table location", K(ret));
      } else if (opt_ctx->is_batched_multi_stmt() && phy_plan_ctx->get_batched_stmt_param_idxs().empty()) {
        const ObIArray<ObString>* batched_queries = NULL;
        if (OB_ISNULL(batched_queries = sql_ctx->multi_stmt_item_.get_queries())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(batched_queries), K(ret));
        } else if (OB_UNLIKELY(partition_ids.count() != batched_queries->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not equal array count", K(partition_ids.count()), K(batched_queries->count()), K(ret));
        } else if (OB_FAIL(phy_plan_ctx->set_batched_stmt_partition_ids(partition_ids))) {
          LOG_WARN("failed to set partition ids", K(ret));
        } else {
          LOG_TRACE("batched multi-stmt partition ids", K(partition_ids));
        }
      }
    } else if (OB_FAIL(table_partition_info.calculate_phy_table_location_info(
                   *exec_ctx, schema_guard, *params, *location_cache, dtc_params))) {
      LOG_WARN("Failed to calculate table location", K(ret));
    } else {
      LOG_TRACE("succeed to calculate base table sharding info", K(table_id), K(ref_table_id), K(is_global_index));
    }
  }
  return ret;
}

bool ObJoinOrder::has_array_binding_param(const ParamStore& param_store)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < param_store.count(); i++) {
    bret = (ObExtendType == param_store.at(i).get_type());
  }
  return bret;
}

int ObJoinOrder::compute_sharding_info_for_paths(const bool is_inner_path, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  bool can_intra_parallel = false;
  ObDMLStmt* stmt = NULL;
  AccessPath* path = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(allocator_) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(allocator_), K(stmt), K(ret));
  } else if (FALSE_IT(can_intra_parallel = get_plan()->get_optimizer_context().use_intra_parallel())) {
    /*do nothing*/
  } else if (!is_inner_path) {
    if (OB_FAIL(compute_sharding_info(can_intra_parallel, table_partition_info_, sharding_info_))) {
      LOG_WARN("failed to calc sharding info", K(ret));
    }
  }
  // compute sharding info for global index
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    if (OB_ISNULL(path = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!path->is_global_index_) {
      path->sharding_info_ = &sharding_info_;
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < available_access_paths_.count(); ++j) {
        AccessPath* cur_path = available_access_paths_.at(j);
        if (OB_ISNULL(cur_path)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (path->table_id_ == cur_path->table_id_ && path->ref_table_id_ == cur_path->ref_table_id_ &&
                   path->index_id_ == cur_path->index_id_) {
          path->sharding_info_ = cur_path->sharding_info_;
          break;
        }
      }
      if (OB_SUCC(ret) && NULL == path->sharding_info_) {
        ObShardingInfo* sharding_info = NULL;
        if (OB_ISNULL(sharding_info = reinterpret_cast<ObShardingInfo*>(allocator_->alloc(sizeof(ObShardingInfo))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate memory", K(ret));
        } else {
          sharding_info = new (sharding_info) ObShardingInfo();
          if (OB_ISNULL(path->table_partition_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(compute_sharding_info(can_intra_parallel, *path->table_partition_info_, *sharding_info))) {
            LOG_WARN("failed to calc sharding info", K(ret));
          } else {
            path->sharding_info_ = sharding_info;
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_sharding_info(
    bool can_intra_parallel, ObTablePartitionInfo& table_partition_info, ObShardingInfo& sharding_info)
{
  int ret = OB_SUCCESS;
  ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (OB_FAIL(table_partition_info.get_location_type(
                 get_plan()->get_optimizer_context().get_local_server_addr(), location_type))) {
    LOG_WARN("get table location type error", K(ret));
  } else if (can_intra_parallel &&
             (OB_TBL_LOCATION_LOCAL == location_type || OB_TBL_LOCATION_REMOTE == location_type)) {
    sharding_info.set_ref_table_id(table_partition_info.get_ref_table_id());
    sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
    LOG_TRACE("base table sharding info, intra partition parallel", K(sharding_info));
  } else if (OB_FAIL(sharding_info.init_partition_info(get_plan()->get_optimizer_context(),
                 *get_plan()->get_stmt(),
                 table_partition_info.get_table_id(),
                 table_partition_info.get_ref_table_id(),
                 table_partition_info.get_phy_tbl_location_info_for_update()))) {
    LOG_WARN("failed to set partition key", K(ret));
  } else {
    sharding_info.set_location_type(location_type);
    LOG_TRACE("base table sharding info", K(sharding_info));
  }
  return ret;
}

int ObJoinOrder::get_hint_index_ids(const uint64_t ref_table_id, const ObIndexHint* index_hint,
    const ObPartHint* part_hint, ObIArray<uint64_t>& valid_index_ids)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  // If this table has a partition hint, we can't use global index(at this time).
  const bool with_global_index = (nullptr == part_hint);
  const bool with_mv = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument with NULL pointer", K(get_plan()), K(schema_guard), K(ret));
  } else if (NULL == index_hint) {
    /*do nothing*/
  } else {
    uint64_t tids[OB_MAX_INDEX_PER_TABLE + 1];
    int64_t table_index_count = OB_MAX_INDEX_PER_TABLE + 1;
    int64_t ignore_index_count = 0;
    uint64_t ignore_ids[OB_MAX_INDEX_PER_TABLE + 1];
    if (OB_FAIL(schema_guard->get_can_read_index_array(
            ref_table_id, tids, table_index_count, with_mv, with_global_index, false /*domain index*/))) {
      LOG_WARN("failed to get can read index", K(ret));
    } else if (table_index_count > OB_MAX_INDEX_PER_TABLE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table index count is bigger than OB_MAX_INDEX_PER_TABLE", K(ret), K(table_index_count));
    } else {
      LOG_TRACE("get readable index", K(table_index_count));
      const share::schema::ObTableSchema* index_schema = NULL;
      tids[table_index_count++] = ref_table_id;
      for (int64_t hint_i = 0; OB_SUCC(ret) && hint_i < index_hint->index_list_.count(); ++hint_i) {
        ObString hinted_index_name = index_hint->index_list_.at(hint_i);
        bool found = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found && i < table_index_count; ++i) {
          uint64_t index_id = tids[i];
          ObString index_name;
          if (index_id == ref_table_id) {
            found = (0 == hinted_index_name.case_compare(ObStmtHint::PRIMARY_KEY));
          } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema)) || OB_ISNULL(index_schema)) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("fail to get table schema", K(index_id), K(ret));
          } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
            LOG_WARN("fail to get index name", K(index_name), K(ret));
          } else if (0 != hinted_index_name.case_compare(index_name)) {
            // do nothing, just continue
            continue;
          } else {
            found = true;
          }
          if (OB_SUCC(ret)) {
            if (found) {
              if (OB_FAIL(add_var_to_array_no_dup(const_cast<ObIndexHint*>(index_hint)->valid_index_ids_, index_id))) {
                LOG_WARN("fail to add member to valid_index_ids", K(ret), K(index_id));
              } else if (ObIndexHint::IGNORE != index_hint->type_) {
                if (NULL != index_schema && index_schema->is_domain_index()) {
                } else if (OB_FAIL(add_var_to_array_no_dup(valid_index_ids, index_id))) {
                  LOG_WARN("Failed to add var to array", K(ret), K(ignore_index_count));
                } else { /*do nothing*/
                }
              } else {
                if (OB_FAIL(add_var_to_array_no_dup(
                        ignore_ids, OB_MAX_INDEX_PER_TABLE + 1, ignore_index_count, index_id))) {
                  LOG_WARN("Failed to add var to array", K(ret), K(ignore_index_count));
                }
              }
            } else {
              if (NULL != index_schema && index_schema->is_domain_index()) {
                if (OB_FAIL(add_var_to_array_no_dup(
                        ignore_ids, OB_MAX_INDEX_PER_TABLE + 1, ignore_index_count, index_id))) {
                  LOG_WARN("Failed to add var to array", K(ret), K(ignore_index_count));
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (ObIndexHint::IGNORE == index_hint->type_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < table_index_count; ++i) {
            bool found = false;
            for (int64_t j = 0; OB_SUCC(ret) && !found && j < ignore_index_count; ++j) {
              if (tids[i] == ignore_ids[j]) {
                found = true;
              } else { /*do nothing*/
              }
            }
            if (!found) {
              if (OB_FAIL(valid_index_ids.push_back(tids[i]))) {
                LOG_WARN("failed to push back array", K(ret));
              } else { /*do nothing*/
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_query_range_info(
    const uint64_t table_id, const uint64_t index_id, QueryRangeInfo& range_info, PathHelper& helper)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObQueryRange* query_range = NULL;
  const share::schema::ObTableSchema* index_schema = NULL;
  ObQueryRangeArray& ranges = range_info.get_ranges();
  ObIArray<ColumnItem>& range_columns = range_info.get_range_columns();
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(stmt), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("fail to get table schema", K(index_id), K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_schema), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_column_items(
                 stmt, opt_ctx->get_expr_factory(), table_id, *index_schema, range_columns))) {
    LOG_WARN("failed to generate rowkey column items", K(ret));
  } else {
    const ObSQLSessionInfo* session = opt_ctx->get_session_info();
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    const ParamStore* params = opt_ctx->get_params();
    bool all_single_value_range = false;
    int64_t equal_prefix_count = 0;
    int64_t equal_prefix_null_count = 0;
    int64_t range_prefix_count = 0;
    bool contain_always_false = false;
    bool has_exec_param = false;
    if (OB_FAIL(extract_preliminary_query_range(range_columns, helper.filters_, query_range))) {
      LOG_WARN("failed to extract query range", K(ret), K(index_id));
    } else if (OB_ISNULL(query_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(query_range), K(ret));
    } else if (OB_FAIL(
                   query_range->get_tablet_ranges(*allocator_, *params, ranges, all_single_value_range, dtc_params))) {
      LOG_WARN("failed to final extract query range", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_prefix_ranges_count(range_info.get_ranges(),
                   equal_prefix_count,
                   equal_prefix_null_count,
                   range_prefix_count,
                   contain_always_false))) {
      LOG_WARN("failed to compute query range prefix count", K(ret));
    } else if (OB_FAIL(check_has_exec_param(*query_range, has_exec_param))) {
      LOG_WARN("failed to check has exec param", K(ret));
    } else if (!has_exec_param) {
      range_info.set_equal_prefix_count(equal_prefix_count);
      range_info.set_range_prefix_count(range_prefix_count);
      range_info.set_contain_always_false(contain_always_false);
    } else if (OB_FAIL(get_preliminary_prefix_info(*query_range, range_info))) {
      LOG_WARN("failed to get preliminary prefix info", K(ret));
    }
    range_info.set_valid();
    range_info.set_query_range(query_range);
    range_info.set_equal_prefix_null_count(equal_prefix_null_count);
    range_info.set_index_column_count(
        index_schema->is_index_table() ? index_schema->get_index_column_num() : index_schema->get_rowkey_column_num());
    if (OB_SUCCESS != ret && NULL != query_range) {
      query_range->~ObQueryRange();
      query_range = NULL;
    } else {
      LOG_TRACE("succeed to get query range", K(ranges), K(*query_range), K(table_id), K(index_id));
    }
  }
  return ret;
}

int ObJoinOrder::check_has_exec_param(ObQueryRange& query_range, bool& has_exec_param)
{
  int ret = OB_SUCCESS;
  has_exec_param = false;
  const ObIArray<ObRawExpr*>& range_exprs = query_range.get_range_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && !has_exec_param && i < range_exprs.count(); ++i) {
    ObRawExpr* expr = range_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_EXEC_PARAM)) {
      has_exec_param = true;
    }
  }
  return ret;
}

int ObJoinOrder::get_preliminary_prefix_info(ObQueryRange& query_range, QueryRangeInfo& range_info)
{
  int ret = OB_SUCCESS;
  int64_t equal_prefix_count = 0;
  int64_t range_prefix_count = 0;
  bool contain_always_false = false;
  if (OB_ISNULL(query_range.get_table_grapth().key_part_head_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table_graph.key_part_head_ is not inited.", K(ret));
  } else if (query_range.get_table_grapth().key_part_head_->pos_.offset_ < 0) {
    // do nothing
  } else {
    get_prefix_info(
        query_range.get_table_grapth().key_part_head_, equal_prefix_count, range_prefix_count, contain_always_false);
    range_info.set_equal_prefix_count(equal_prefix_count);
    range_info.set_range_prefix_count(range_prefix_count);
    range_info.set_contain_always_false(contain_always_false);
    LOG_TRACE("success to get preliminary prefix info",
        K(equal_prefix_count),
        K(range_prefix_count),
        K(contain_always_false));
  }
  return ret;
}

void ObJoinOrder::get_prefix_info(
    ObKeyPart* key_part, int64_t& equal_prefix_count, int64_t& range_prefix_count, bool& contain_always_false)
{
  if (OB_NOT_NULL(key_part)) {
    equal_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    range_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    for (/*do nothing*/; NULL != key_part; key_part = key_part->or_next_) {
      int64_t cur_equal_prefix_count = 0;
      int64_t cur_range_prefix_count = 0;
      if (key_part->is_equal_condition()) {
        get_prefix_info(key_part->and_next_, cur_equal_prefix_count, cur_range_prefix_count, contain_always_false);
        ++cur_equal_prefix_count;
        ++cur_range_prefix_count;
      } else if (key_part->is_range_condition()) {
        ++cur_range_prefix_count;
      } else if (key_part->is_always_false_condition()) {
        contain_always_false = true;
      }
      equal_prefix_count = std::min(cur_equal_prefix_count, equal_prefix_count);
      range_prefix_count = std::min(cur_range_prefix_count, range_prefix_count);
    }
  }
}

int ObJoinOrder::add_table_by_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
    const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, bool& added,
    PathHelper& helper, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  uint64_t index_to_use = OB_INVALID_ID;
  added = false;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id) ||
      OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    if (is_virtual_table(ref_table_id)) {
      // check virtual table heuristics
      if (OB_FAIL(virtual_table_heuristics(table_id, ref_table_id, index_info_cache, valid_index_ids, index_to_use))) {
        LOG_WARN("failed to check virtual table heuristics", K(table_id), K(ref_table_id), K(ret));
      } else if (OB_INVALID_ID != index_to_use) {
        helper.table_opt_info_->optimization_method_ = OptimizationMethod::RULE_BASED;
        helper.table_opt_info_->heuristic_rule_ = HeuristicRule::VIRTUAL_TABLE_HEURISTIC;
        LOG_TRACE(
            "OPT:[RBO] choose index using heuristics for virtual table", K(table_id), K(ref_table_id), K(index_to_use));
      }
    } else {
      // check whether we can use single table heuristics:
      if (OB_FAIL(
              user_table_heuristics(table_id, ref_table_id, index_info_cache, valid_index_ids, index_to_use, helper))) {
        LOG_WARN("Failed to check user_table_heuristics", K(ret));
      } else if (OB_INVALID_ID != index_to_use) {
        LOG_TRACE("OPT:[RBO] choose primary/index using heuristics", K(table_id), K(index_to_use));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID != index_to_use) {
        AccessPath* access_path = NULL;
        if (OB_FAIL(create_access_path(table_id, ref_table_id, index_to_use, index_info_cache, helper, access_path))) {
          LOG_WARN("failed to create primary index path", K(ret), K(table_id), K(ref_table_id));
        } else if (NULL != access_path && OB_FAIL(access_paths.push_back(access_path))) {
          LOG_WARN("failed to push back access path");
        } else {
          added = true;
          LOG_TRACE("OPT:[RBO] table added using heuristics", K(table_id), K(ref_table_id), K(index_to_use));
        }
      } else {
        LOG_TRACE("OPT:[RBO] table not added using heuristics", K(table_id), K(ref_table_id));
      }
    }
  }
  return ret;
}

int ObJoinOrder::virtual_table_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
    const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, uint64_t& index_to_use)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(get_plan()));
  } else if (OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard should not be null", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    // for virtual table, choose any index to use
    uint64_t idx_id = OB_INVALID_ID;
    const ObTableSchema* index_schema = NULL;
    const QueryRangeInfo* query_range_info = NULL;
    LOG_TRACE("OPT:[VT] begin search index for virtual table", K(valid_index_ids.count()));
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == idx_id && i < valid_index_ids.count(); ++i) {
      if (OB_UNLIKELY(OB_INVALID_ID == valid_index_ids.at(i))) {
        LOG_WARN("index id invalid", K(table_id), K(valid_index_ids.at(i)), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(valid_index_ids.at(i), index_schema)) ||
                 OB_ISNULL(index_schema)) {
        LOG_WARN("fail to get table schema", K(index_schema), K(valid_index_ids.at(i)), K(ret));
      } else if (!index_schema->is_index_table()) {
        /*do nothing*/
      } else if (OB_FAIL(index_info_cache.get_query_range(table_id, valid_index_ids.at(i), query_range_info))) {
        LOG_WARN("failed to get query range", K(ret));
      } else if (OB_ISNULL(query_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_range_info should not be null", K(ret));
      } else if (query_range_info->is_index_column_get()) {
        idx_id = valid_index_ids.at(i);
        LOG_TRACE("OPT:[VT] found index to use", K(idx_id));
      } else {
        LOG_TRACE("OPT:[VT] not found index to use", K(idx_id));
      }
    }
    if (OB_INVALID_ID == idx_id) {
      index_to_use = ref_table_id;
    } else {
      index_to_use = idx_id;
    }
  }

  return ret;
}

/*
 * Different with cost-based approach, this function tries to use heuristics to generate access path
 * The heuristics used to generate access path is as follows:
 * 1 if search condition cover an unique index key (primary key is treated as unique key), and no need index_back, use
 * that key, if there is multiple such key, choose the one with the minimum index key count 2 if search condition cover
 * an unique index key, need index_back, use that key and follows a refine process to find a better index 3 otherwise
 * generate all the access path and choose access path using cost model
 */
int ObJoinOrder::user_table_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
    const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, uint64_t& index_to_use,
    PathHelper& helper)
{
  int ret = OB_SUCCESS;
  index_to_use = OB_INVALID_ID;
  ObDMLStmt* stmt = NULL;
  const ObTableSchema* table_schema = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  QueryRangeInfo* range_info = NULL;
  IndexInfoEntry* index_info_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(schema_guard), K(stmt), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    uint64_t ukey_idx_without_indexback = OB_INVALID_ID;
    int64_t minimal_ukey_size_without_indexback = 0;
    ObSEArray<uint64_t, 4> ukey_idx_with_indexback;
    ObSEArray<uint64_t, 4> candidate_refine_index;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
      int64_t index_col_num = 0;
      uint64_t index_id = valid_index_ids.at(i);
      if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id, index_info_entry))) {
        LOG_WARN("failed to get index info entry", K(ret));
      } else if (OB_ISNULL(index_info_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index info entry should not be null", K(ret));
      } else {
        range_info = &index_info_entry->get_range_info();
        index_col_num = range_info->get_index_column_count();
        LOG_TRACE("index info",
            K(index_info_entry->is_index_back()),
            K(index_info_entry->get_index_id()),
            K(index_info_entry->is_unique_index()),
            K(range_info->get_equal_prefix_count()),
            K(index_col_num),
            K(table_schema->get_column_count()));
        if (range_info->get_equal_prefix_count() >= index_col_num) {
          // all key covers
          if (!index_info_entry->is_index_back()) {
            if (index_info_entry->is_valid_unique_index()) {
              if (ukey_idx_without_indexback == OB_INVALID_ID ||
                  table_schema->get_column_count() < minimal_ukey_size_without_indexback) {
                ukey_idx_without_indexback = index_id;
                minimal_ukey_size_without_indexback = table_schema->get_column_count();
              }
            } else { /*do nothing*/
            }
          } else if (index_info_entry->is_valid_unique_index()) {
            if (OB_FAIL(ukey_idx_with_indexback.push_back(index_id))) {
              LOG_WARN("failed to push back unique index with indexback", K(index_id), K(ret));
            } else { /* do nothing */
            }
          } else { /* do nothing*/
          }
        } else if (!index_info_entry->is_index_back()) {
          if (OB_FAIL(candidate_refine_index.push_back(index_id))) {
            LOG_WARN("failed to push back refine index id", K(ret));
          } else { /* do nothing*/
          }
        } else { /* do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (ukey_idx_without_indexback != OB_INVALID_ID) {
        helper.table_opt_info_->optimization_method_ = OptimizationMethod::RULE_BASED;
        helper.table_opt_info_->heuristic_rule_ = HeuristicRule::UNIQUE_INDEX_WITHOUT_INDEXBACK;
        index_to_use = ukey_idx_without_indexback;
      } else if (ukey_idx_with_indexback.count() > 0) {
        helper.table_opt_info_->optimization_method_ = OptimizationMethod::RULE_BASED;
        helper.table_opt_info_->heuristic_rule_ = HeuristicRule::UNIQUE_INDEX_WITH_INDEXBACK;
        LOG_TRACE("start to refine table heuristics", K(index_to_use));
        if (OB_FAIL(refine_table_heuristics_result(table_id,
                ref_table_id,
                candidate_refine_index,
                ukey_idx_with_indexback,
                index_info_cache,
                index_to_use))) {
          LOG_WARN("failed to refine heuristic index choosing", K(ukey_idx_with_indexback), K(ret));
        } else if (index_to_use == OB_INVALID_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index id", K(ret));
        } else {
          LOG_TRACE("finish to refine table heuristics", K(index_to_use));
        };
      } else { /* do nothing */
      }
    }
  }
  return ret;
}

/*
 * this function try to refine rule 2
 * our heuristics sometimes may choose sub-optimal index
 * for example: create table t1(a int, b int, c int, unique key t1_b(b), unique key t1_b_c(b,c))
 * for query, select b, c from t1 where b = 5 and c > 10,
 * our heuristics will choose index t1_b, however, t1_b_c will be a much better choice since it does not need index back
 * this, if we meet rule 2, we will search again all other index to find a better one
 * candidate_refine_idx: all the candidate index we consider to refine
 * match_unique_idx: all the matched unique index
 */
int ObJoinOrder::refine_table_heuristics_result(const uint64_t table_id, const uint64_t ref_table_id,
    const ObIArray<uint64_t>& candidate_refine_idx, const ObIArray<uint64_t>& match_unique_idx,
    const ObIndexInfoCache& index_info_cache, uint64_t& index_to_use)
{
  int ret = OB_SUCCESS;
  index_to_use = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    uint64_t refined_idx = OB_INVALID_ID;
    int64_t minimal_range_count = 0;
    ObSEArray<const QueryRangeInfo*, 4> match_range_info;
    ObSEArray<const OrderingInfo*, 4> match_ordering_info;
    const QueryRangeInfo* temp_range_info = NULL;
    const OrderingInfo* temp_ordering_info = NULL;
    CompareRelation status = UNCOMPARABLE;
    for (int64_t i = 0; OB_SUCC(ret) && i < match_unique_idx.count(); ++i) {
      uint64_t index_id = match_unique_idx.at(i);
      if (OB_FAIL(index_info_cache.get_query_range(table_id, index_id, temp_range_info))) {
        LOG_WARN("failed to get query range info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range info is NULL", K(ret));
      } else if (OB_FAIL(match_range_info.push_back(temp_range_info))) {
        LOG_WARN("failed to push back range info", K(ret));
      } else if (OB_FAIL(index_info_cache.get_access_path_ordering(table_id, index_id, temp_ordering_info))) {
        LOG_WARN("failed to get ordering info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_ordering_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ordering info is null", K(ret));
      } else if (OB_FAIL(match_ordering_info.push_back(temp_ordering_info))) {
        LOG_WARN("failed to push back ordering info", K(ret));
      } else {
        if (index_to_use == OB_INVALID_ID || temp_range_info->get_ranges().count() < minimal_range_count) {
          index_to_use = index_id;
          minimal_range_count = temp_range_info->get_ranges().count();
        }
      }
    }
    // search all the candidate index to find a better one
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_refine_idx.count(); i++) {
      uint64_t index_id = candidate_refine_idx.at(i);
      if (OB_FAIL(index_info_cache.get_query_range(table_id, index_id, temp_range_info))) {
        LOG_WARN("failed to get range info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range info is null", K(ret));
      } else if (OB_FAIL(index_info_cache.get_access_path_ordering(table_id, index_id, temp_ordering_info))) {
        LOG_WARN("failed to get ordering info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_ordering_info)) {
        LOG_WARN("ordering info is null", K(ret));
      } else { /* do nothing*/
      }
      // examine all matched unique index
      for (int64_t j = 0; OB_SUCC(ret) && j < match_unique_idx.count(); j++) {
        if (temp_range_info->get_range_prefix_count() >= match_range_info.at(j)->get_range_prefix_count()) {
          if (OB_FAIL(check_index_subset(match_ordering_info.at(j),
                  match_range_info.at(j)->get_range_prefix_count(),
                  temp_ordering_info,
                  temp_range_info->get_range_prefix_count(),
                  status))) {
            LOG_WARN("failed to compare two index", K(ret));
          } else if (status == LEFT_DOMINATE || status == EQUAL) {
            if (refined_idx == OB_INVALID_ID || temp_range_info->get_ranges().count() < minimal_range_count) {
              refined_idx = index_id;
              minimal_range_count = temp_range_info->get_ranges().count();
            }
            break;
          } else { /* do nothing*/
          }
        }
      }
    }
    // finally refine the index if we get one
    if (OB_SUCC(ret)) {
      if (refined_idx != OB_INVALID_ID) {
        index_to_use = refined_idx;
      } else { /* do nothing*/
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_index_subset(const OrderingInfo* first_ordering_info, const int64_t first_index_key_count,
    const OrderingInfo* second_ordering_info, const int64_t second_index_key_count, CompareRelation& status)
{
  int ret = OB_SUCCESS;
  status = UNCOMPARABLE;
  bool is_subset = false;
  if (OB_ISNULL(first_ordering_info) || OB_ISNULL(second_ordering_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(first_ordering_info), K(second_ordering_info));
  } else if (first_index_key_count <= second_index_key_count) {
    if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(first_ordering_info->get_index_keys(),
            first_index_key_count,
            second_ordering_info->get_index_keys(),
            second_index_key_count,
            is_subset))) {
      LOG_WARN("failed to compare index keys", K(ret));
    } else if (is_subset) {
      status = first_index_key_count == second_index_key_count ? EQUAL : LEFT_DOMINATE;
    } else {
      status = UNCOMPARABLE;
    }
  } else {
    if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(second_ordering_info->get_index_keys(),
            second_index_key_count,
            first_ordering_info->get_index_keys(),
            first_index_key_count,
            is_subset))) {
      LOG_WARN("failed to compare index keys", K(ret));
    } else if (is_subset) {
      status = RIGHT_DOMINATE;
    } else {
      status = UNCOMPARABLE;
    }
  }
  LOG_TRACE("check index subset",
      K(*first_ordering_info),
      K(*second_ordering_info),
      K(first_index_key_count),
      K(second_index_key_count));
  return ret;
}

int ObJoinOrder::create_access_path(const uint64_t table_id, const uint64_t ref_id, const uint64_t index_id,
    const ObIndexInfoCache& index_info_cache, PathHelper& helper, AccessPath*& access_path)
{
  int ret = OB_SUCCESS;
  IndexInfoEntry* index_info_entry;
  access_path = NULL;
  AccessPath* ap = NULL;
  bool is_nl_with_extended_range = false;
  if (OB_UNLIKELY(OB_INVALID_ID == ref_id) || OB_UNLIKELY(OB_INVALID_ID == index_id) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(allocator_) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ref_id), K(index_id), K(get_plan()), K(allocator_), K(ret));
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id, index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info entry should not be null", K(ret));
  } else if (helper.is_inner_path_ && index_info_entry->get_ordering_info().get_index_keys().count() <= 0) {
    LOG_TRACE(
        "OPT:skip adding inner access path due to has domain filter/wrong index key count", K(table_id), K(ref_id));
  } else if (OB_ISNULL(ap = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate an AccessPath", K(ret));
  } else {
    LOG_TRACE("OPT:start to create access path", K(table_id), K(ref_id), K(index_id), K(helper.is_inner_path_));
    const QueryRangeInfo& range_info = index_info_entry->get_range_info();
    const OrderingInfo& ordering_info = index_info_entry->get_ordering_info();
    ap = new (ap) AccessPath(table_id, ref_id, index_id, this, ordering_info.get_scan_direction());
    ap->is_get_ = range_info.is_get();
    ap->is_global_index_ = index_info_entry->is_index_global();
    ap->is_hash_index_ = is_virtual_table(ref_id) && ref_id != index_id;
    ap->est_cost_info_.index_meta_info_.is_index_back_ = index_info_entry->is_index_back();
    ap->est_cost_info_.index_meta_info_.is_unique_index_ = index_info_entry->is_unique_index();
    ap->est_cost_info_.index_meta_info_.is_global_index_ = index_info_entry->is_index_global();
    ap->est_cost_info_.is_virtual_table_ = is_virtual_table(ref_id);
    ap->est_cost_info_.est_sel_info_ = &get_plan()->get_est_sel_info();
    ap->est_cost_info_.is_unique_ =
        ap->is_get_ || (index_info_entry->is_unique_index() && range_info.is_index_column_get() &&
                           index_info_entry->is_valid_unique_index());
    ap->table_opt_info_ = helper.table_opt_info_;
    ap->is_inner_path_ = helper.is_inner_path_;
    ap->range_prefix_count_ = index_info_entry->get_range_info().get_range_prefix_count();
    ap->interesting_order_info_ = index_info_entry->get_interesting_order_info();
    if (!get_plan()->get_stmt()->is_select_stmt()) {
      // do nothing
      // sample scan doesn't support DML other than SELECT.
    } else {
      ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_plan()->get_stmt());
      SampleInfo* sample_info = stmt->get_sample_info_by_table_id(table_id);
      if (sample_info != NULL) {
        ap->sample_info_ = *sample_info;
        ap->sample_info_.table_id_ = ap->get_index_table_id();
        ap->est_cost_info_.sample_info_ = ap->sample_info_;
      }
    }

    if (OB_FAIL(add_access_filters(ap, ordering_info.get_index_keys(), helper.filters_))) {
      LOG_WARN("failed to add access filters", K(*ap), K(ordering_info.get_index_keys()), K(ret));
    } else if ((!ap->is_global_index_ || !index_info_entry->is_index_back()) &&
               OB_FAIL(ap->set_ordering(ordering_info.get_ordering(), ordering_info.get_scan_direction()))) {
      LOG_WARN("failed to create index keys expression array", K(index_id), K(ret));
    } else if (ordering_info.get_index_keys().count() > 0) {
      ap->pre_query_range_ = const_cast<ObQueryRange*>(range_info.get_query_range());
      if (OB_FAIL(ap->index_keys_.assign(ordering_info.get_index_keys()))) {
        LOG_WARN("failed to get index keys", K(ret));
      } else if (OB_FAIL(ap->est_cost_info_.range_columns_.assign(range_info.get_range_columns()))) {
        LOG_WARN("failed to assign range columns", K(ret));
      } else if (OB_FAIL(fill_query_range_info(range_info, ap->est_cost_info_))) {
        LOG_WARN("failed to fill query range info", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_filters(ap->filter_, ap->pre_query_range_, ap->est_cost_info_, is_nl_with_extended_range))) {
        LOG_WARN("failed to fill filters for cost table info", K(ret));
      } else if (!helper.is_inner_path_ && OB_FAIL(increase_diverse_path_count(ap))) {
        LOG_WARN("failed to increase diverse path count", K(ret));
      } else if (!helper.is_inner_path_ || is_nl_with_extended_range) {
        access_path = ap;
      }
    }
    LOG_TRACE("OPT:succeed to create one access path", K(table_id), K(ref_id), K(index_id), K(helper.is_inner_path_));
  }
  return ret;
}

int ObJoinOrder::fill_acs_index_info(AccessPath& ap)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObCostTableScanInfo& est_cost_info = ap.est_cost_info_;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(ap.table_opt_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", K(get_plan()), K(stmt), K(schema_guard), K(allocator_), K(ret));
  } else if (OB_UNLIKELY(0 == est_cost_info.table_meta_info_.table_row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table row count should not be 0", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(est_cost_info.index_meta_info_.index_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else if (stmt->is_single_table_stmt() && stmt->is_select_stmt() &&
             !ObSQLMockSchemaUtils::is_mock_index(est_cost_info.index_meta_info_.index_id_) &&
             !is_sys_table(est_cost_info.ref_table_id_) && !is_virtual_table(est_cost_info.ref_table_id_) &&
             OptimizationMethod::COST_BASED == ap.table_opt_info_->optimization_method_ &&
             ap.table_opt_info_->available_index_id_.count() > 1) {
    // fill in acs plan info
    ObAcsIndexInfo* acs_index_info = static_cast<ObAcsIndexInfo*>(allocator_->alloc(sizeof(ObAcsIndexInfo)));
    if (OB_ISNULL(acs_index_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an memory", K(ret));
    } else {
      acs_index_info = new (acs_index_info) ObAcsIndexInfo(allocator_);
      acs_index_info->index_id_ = est_cost_info.index_meta_info_.index_id_;
      acs_index_info->is_index_back_ = est_cost_info.index_meta_info_.is_index_back_;
      acs_index_info->query_range_ = ap.pre_query_range_;
      if (est_cost_info.index_id_ == est_cost_info.ref_table_id_) {
        acs_index_info->index_name_ = table_schema->get_table_name_str();
      } else {
        ObString name;
        if (OB_FAIL(table_schema->get_index_name(name))) {
          LOG_WARN("failed to get index name", K(ret));
        } else {
          acs_index_info->index_name_ = name;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (est_cost_info.ranges_.count() == 1 && est_cost_info.ranges_.at(0).is_whole_range()) {
        acs_index_info->is_whole_range_ = true;
        acs_index_info->prefix_filter_sel_ = 1.0;
      } else {
        acs_index_info->is_whole_range_ = false;
        if (OB_UNLIKELY(est_cost_info.table_meta_info_.table_row_count_ <= 0) ||
            OB_UNLIKELY(est_cost_info.table_meta_info_.part_count_ <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table statistic",
              K(est_cost_info.table_meta_info_.table_row_count_),
              K(est_cost_info.table_meta_info_.part_count_));
        } else {
          acs_index_info->prefix_filter_sel_ =
              ap.query_range_row_count_ / static_cast<double>(est_cost_info.table_meta_info_.table_row_count_);
          if (acs_index_info->prefix_filter_sel_ >= 1.0) {
            acs_index_info->prefix_filter_sel_ = 1.0;
          }
        }
      }
    }
    // acs_index_info.prefix_filter_sel_
    if (OB_SUCC(ret)) {
      if (OB_FAIL(acs_index_info->column_id_.init(est_cost_info.table_scan_param_.column_ids_.count()))) {
        LOG_WARN("failed to init fixed array", K(ret));
      } else if (OB_FAIL(acs_index_info->column_id_.assign(est_cost_info.table_scan_param_.column_ids_))) {
        LOG_WARN("failed to assign column id", K(ret));
      } else if (OB_FAIL(plan_->get_acs_index_info().push_back(acs_index_info))) {
        LOG_WARN("failed to push back acs index info", K(ret));
      } else {
        LOG_TRACE("succeed to add acs index info", K(plan_->get_acs_index_info()));
      }
    }
  } else { /*do nothing */
  }
  return ret;
}

int ObJoinOrder::get_access_path_ordering(const uint64_t table_id, const uint64_t ref_table_id, const uint64_t index_id,
    common::ObIArray<ObRawExpr*>& index_keys, common::ObIArray<ObRawExpr*>& ordering, ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  direction = default_asc_direction();
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(opt_ctx), K(allocator_), K(ret));
  } else if (OB_FAIL(
                 ObOptimizerUtil::generate_rowkey_exprs(stmt, *opt_ctx, table_id, index_id, index_keys, ordering))) {
    LOG_WARN("failed to get rowkeys raw expr", K(table_id), K(ref_table_id), K(index_id), K(ret));
  } else if (ordering.count() > 0 && /* has row key*/
             OB_FAIL(get_index_scan_direction(ordering, stmt, get_plan()->get_equal_sets(), direction))) {
    LOG_WARN("failed to get index scan direction", K(ret));
  }
  return ret;
}

int ObJoinOrder::get_index_scan_direction(const ObIArray<ObRawExpr*>& keys, const ObDMLStmt* stmt,
    const EqualSets& equal_sets, ObOrderDirection& index_direction)
{
  int ret = OB_SUCCESS;
  index_direction = default_asc_direction();
  bool check_order_by = true;
  int64_t order_match_count = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(get_plan()));
  } else if (stmt->is_select_stmt()) {
    const ObSelectStmt* sel_stmt = static_cast<const ObSelectStmt*>(stmt);
    int64_t max_order_match_count = 0;
    const ObWinFunRawExpr* win_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_window_func_count(); ++i) {
      bool full_covered = false;
      int64_t partition_match_count = 0;
      ObOrderDirection tmp_direction = default_asc_direction();
      if (OB_ISNULL(win_expr = sel_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null win expr", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(win_expr->get_partition_exprs(),
                     keys,
                     equal_sets,
                     get_plan()->get_const_exprs(),
                     full_covered,
                     &partition_match_count))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      } else if (!full_covered) {
        check_order_by = false;
      } else if (win_expr->get_order_items().count() > 0) {
        check_order_by = false;
        if (OB_FAIL(get_direction_in_order_by(win_expr->get_order_items(),
                keys,
                partition_match_count,
                equal_sets,
                get_plan()->get_const_exprs(),
                tmp_direction,
                order_match_count))) {
          LOG_WARN("failed to match order-by against index", K(ret));
        } else if (order_match_count > max_order_match_count) {
          max_order_match_count = order_match_count;
          index_direction = tmp_direction;
        }
      }
    }
  }
  if (OB_SUCC(ret) && check_order_by) {
    ObOrderDirection tmp_direction = default_asc_direction();
    if (OB_FAIL(get_direction_in_order_by(stmt->get_order_items(),
            keys,
            0,  // index start offset
            equal_sets,
            get_plan()->get_const_exprs(),
            tmp_direction,
            order_match_count))) {
      LOG_WARN("failed to match order-by against index", K(ret));
    } else if (order_match_count > 0) {
      index_direction = tmp_direction;
    }
  }
  return ret;
}

int ObJoinOrder::get_direction_in_order_by(const ObIArray<OrderItem>& order_by, const ObIArray<ObRawExpr*>& index_keys,
    const int64_t index_start_offset, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    ObOrderDirection& direction, int64_t& order_match_count)
{
  int ret = OB_SUCCESS;
  order_match_count = 0;
  int64_t order_offset = order_by.count();
  int64_t index_offset = index_start_offset;
  bool is_const = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_const && i < order_by.count(); ++i) {
    if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_by.at(i).expr_, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (!is_const) {
      if (is_ascending_direction(order_by.at(i).order_type_)) {
        direction = default_asc_direction();
      } else {
        direction = default_desc_direction();
      }
      order_offset = i;
    }
  }
  ObRawExpr* index_expr = NULL;
  ObRawExpr* order_expr = NULL;
  int64_t order_start = order_offset;
  while (OB_SUCC(ret) && index_offset < index_keys.count() && order_offset < order_by.count()) {
    if (OB_ISNULL(index_expr = index_keys.at(index_offset)) ||
        OB_ISNULL(order_expr = order_by.at(order_offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs have null", K(ret), K(index_expr), K(order_expr));
    } else if (ObOptimizerUtil::is_expr_equivalent(index_expr, order_expr, equal_sets) &&
               (is_ascending_direction(order_by.at(order_offset).order_type_) == is_ascending_direction(direction))) {
      ++index_offset;
      ++order_offset;
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check order_expr is const expr", K(ret));
    } else if (is_const) {
      ++order_offset;
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(index_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check index_expr is const expr", K(ret));
    } else if (is_const) {
      ++index_offset;
    } else {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    order_match_count = order_offset - order_start;
  }
  return ret;
}

int ObJoinOrder::add_access_filters(
    AccessPath* path, const ObIArray<ObRawExpr*>& index_keys, const ObIArray<ObRawExpr*>& restrict_infos)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(path), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < restrict_infos.count(); ++i) {
      if (ObOptimizerUtil::find_equal_expr(stmt->get_deduced_exprs(), restrict_infos.at(i))) {
        if (path->ref_table_id_ == path->index_id_) {
        } else {
          bool index_match = false;
          if (OB_FAIL(check_expr_overlap_index(restrict_infos.at(i), index_keys, index_match))) {
            LOG_WARN("check quals match index error", K(restrict_infos.at(i)), K(index_keys), K(ret));
          } else if (index_match) {
            if (OB_FAIL(path->filter_.push_back(restrict_infos.at(i)))) {
              LOG_WARN("push back error", K(ret));
            }
          } else { /*do nothing*/
          }
        }
      } else {
        if (OB_FAIL(path->filter_.push_back(restrict_infos.at(i)))) {
          LOG_WARN("push back error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_and_extract_query_range(const uint64_t table_id, const uint64_t index_table_id,
    const ObIArray<ObRawExpr*>& index_keys, const ObIndexInfoCache& index_info_cache, bool& contain_always_false,
    ObIArray<uint64_t>& prefix_range_ids, ObIArray<ObRawExpr*>& restrict_infos)
{
  int ret = OB_SUCCESS;
  // do some quick check
  bool expr_match = false;  // some condition on index
  contain_always_false = false;
  if (OB_FAIL(check_exprs_overlap_index(restrict_infos, index_keys, expr_match))) {
    LOG_WARN("check quals match index error", K(restrict_infos), K(index_keys));
  } else if (expr_match) {
    prefix_range_ids.reset();
    const QueryRangeInfo* query_range_info = NULL;
    if (OB_FAIL(index_info_cache.get_query_range(table_id, index_table_id, query_range_info))) {
      LOG_WARN("get_range_columns failed", K(ret));
    } else if (OB_ISNULL(query_range_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query_range_info should not be null", K(ret));
    } else {
      contain_always_false = query_range_info->get_contain_always_false();
      uint64_t range_prefix_count = query_range_info->get_range_prefix_count();
      const ObIArray<ColumnItem>& range_columns = query_range_info->get_range_columns();
      if (OB_UNLIKELY(range_prefix_count > range_columns.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("range prefix count is invalid", K(range_prefix_count), K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < range_prefix_count; ++i) {
          if (OB_FAIL(prefix_range_ids.push_back(range_columns.at(i).column_id_))) {
            LOG_WARN("failed to push back column_id", K(ret), K(i));
          }
        }
      }
    }
  }  // not match, do nothing
  LOG_TRACE("extract prefix range ids", K(ret), K(contain_always_false), K(expr_match), K(prefix_range_ids));
  return ret;
}

int ObJoinOrder::cal_dimension_info(const uint64_t table_id,  // alias table id
    const uint64_t data_table_id,                             // real table id
    const uint64_t index_table_id, const ObDMLStmt* stmt, ObIndexSkylineDim& index_dim,
    const ObIndexInfoCache& index_info_cache, ObIArray<ObRawExpr*>& restrict_infos)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* guard = NULL;
  IndexInfoEntry* index_info_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(guard = OPT_CTX.get_sql_schema_guard()) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(guard), K(stmt));
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_table_id, index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(ret), K(data_table_id), K(index_table_id));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info entry should not be null", K(ret));
  } else {
    ObSEArray<uint64_t, 8> filter_column_ids;
    bool is_index_back = index_info_entry->is_index_back();
    const OrderingInfo* ordering_info = &index_info_entry->get_ordering_info();
    ObSEArray<uint64_t, 8> interest_column_ids;
    ObSEArray<bool, 8> const_column_info;
    ObSEArray<uint64_t, 8> prefix_range_ids;  // for query range compare
    bool contain_always_false = false;        // for always false condition, the index is not comparable
    if (OB_FAIL(extract_interesting_column_ids(ordering_info->get_index_keys(),
            index_info_entry->get_interesting_order_prefix_count(),
            interest_column_ids,
            const_column_info))) {
      LOG_WARN("failed to extract interest column ids", K(ret));
    } else if (OB_FAIL(check_and_extract_query_range(table_id,
                   index_table_id,
                   ordering_info->get_index_keys(),
                   index_info_cache,
                   contain_always_false,
                   prefix_range_ids,
                   restrict_infos))) {
      LOG_WARN("check_and_extract query range failed", K(ret));
    } else {
      const ObTableSchema* index_schema = NULL;
      if (OB_FAIL(guard->get_table_schema(index_table_id, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        LOG_WARN("index schema should not be null", K(ret));
      } else if (OB_FAIL(extract_filter_column_ids(
                     restrict_infos, data_table_id == index_table_id, *index_schema, filter_column_ids))) {
        LOG_WARN("extract filter column ids failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (contain_always_false) {
          index_dim.set_can_prunning(false);
        }
        if (OB_FAIL(index_dim.add_index_back_dim(is_index_back,
                interest_column_ids.count() > 0,
                prefix_range_ids.count() > 0,
                index_schema->get_column_count(),
                filter_column_ids,
                *allocator_))) {
          LOG_WARN("add index back dim failed", K(is_index_back), K(ret));
        } else if (OB_FAIL(index_dim.add_interesting_order_dim(is_index_back,
                       prefix_range_ids.count() > 0,
                       filter_column_ids,
                       interest_column_ids,
                       const_column_info,
                       *allocator_))) {
          LOG_WARN("add interesting order dim failed", K(interest_column_ids), K(ret));
        } else if (OB_FAIL(index_dim.add_query_range_dim(prefix_range_ids, *allocator_))) {
          LOG_WARN("add query range dimension failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::prunning_index(const uint64_t table_id, const uint64_t base_table_id, const ObDMLStmt* stmt,
    const bool do_prunning, const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids,
    ObIArray<uint64_t>& skyline_index_ids, ObIArray<ObRawExpr*>& restrict_infos)
{
  int ret = OB_SUCCESS;
  if (!do_prunning) {
    skyline_index_ids.reset();
    LOG_TRACE("do not do index prunning", K(table_id), K(base_table_id));
    if (OB_FAIL(append(skyline_index_ids, valid_index_ids))) {
      LOG_WARN("failed to append id", K(ret));
    } else { /*do nothing*/
    }
  } else {
    ObSkylineDimRecorder recorder;
    bool has_add = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
      const uint64_t tid = valid_index_ids.at(i);
      LOG_TRACE("cal dimension info of index", K(tid));
      ObIndexSkylineDim* index_dim = NULL;
      if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(*allocator_, index_dim))) {
        LOG_WARN("failed to create index skylined dimension", K(ret));
      } else if (OB_ISNULL(index_dim)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dimension should not be null", K(ret));
      } else if (FALSE_IT(index_dim->set_index_id(tid))) {
      } else if (OB_FAIL(cal_dimension_info(
                     table_id, base_table_id, tid, stmt, *index_dim, index_info_cache, restrict_infos))) {
        LOG_WARN("Failed to cal dimension info", K(ret), "index_id", valid_index_ids, K(i));
      } else if (OB_FAIL(recorder.add_index_dim(*index_dim, has_add))) {
        LOG_WARN("failed to add index dimension", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      skyline_index_ids.reset();
      if (OB_FAIL(recorder.get_dominated_idx_ids(skyline_index_ids))) {
        LOG_WARN("get dominated idx ids failed", K(ret));
      } else {
        LOG_TRACE("after prunning remain index ids", K(skyline_index_ids));
      }
    }
  }
  return ret;
}

int ObJoinOrder::fill_index_info_entry(const uint64_t table_id, const uint64_t base_table_id, const uint64_t index_id,
    IndexInfoEntry*& index_entry, PathHelper& helper)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* index_schema = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObDMLStmt* stmt = NULL;
  index_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    void* ptr = allocator_->alloc(sizeof(IndexInfoEntry));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("entry should not be null", K(ret));
    } else {
      IndexInfoEntry* entry = new (ptr) IndexInfoEntry();
      ObOrderDirection direction = default_asc_direction();
      bool is_index_back = false;
      bool is_unique_index = false;
      bool is_index_global = false;
      entry->set_index_id(index_id);
      int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
      int64_t max_prefix_count = 0;
      if (OB_FAIL(get_access_path_ordering(table_id,
              base_table_id,
              index_id,
              entry->get_ordering_info().get_index_keys(),
              entry->get_ordering_info().get_ordering(),
              direction))) {
        LOG_WARN("get_access_path_ordering ", K(ret));
      } else if (OB_FAIL(get_simple_index_info(
                     table_id, base_table_id, index_id, is_unique_index, is_index_back, is_index_global))) {
        LOG_WARN("failed to get simple index info", K(ret));
      } else {
        if (is_link_table_id(base_table_id)) {
          entry->get_ordering_info().get_ordering().reset();
        }
        entry->set_is_index_global(is_index_global);
        entry->set_is_index_back(is_index_back);
        entry->set_is_unique_index(is_unique_index);
        entry->get_ordering_info().set_scan_direction(direction);
      }
      if (OB_SUCC(ret)) {
        ObSEArray<OrderItem, 4> index_ordering;
        ObIArray<ObRawExpr*>& ordering_expr = entry->get_ordering_info().get_ordering();
        for (int64_t i = 0; OB_SUCC(ret) && i < ordering_expr.count(); ++i) {
          OrderItem order_item(ordering_expr.at(i), direction);
          if (OB_FAIL(index_ordering.push_back(order_item))) {
            LOG_WARN("failed to push back order item", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(
                       check_all_interesting_order(index_ordering, stmt, max_prefix_count, interesting_order_info))) {
          LOG_WARN("failed to check all interesting order", K(ret));
        } else {
          entry->set_interesting_order_prefix_count(max_prefix_count);
          entry->set_interesting_order_info(interesting_order_info);
        }
      }
      if (OB_SUCC(ret)) {
        if ((index_id == base_table_id && is_virtual_table(index_id) &&
                entry->get_ordering_info().get_index_keys().count() <= 0)) {
          // ignore extract query range
          LOG_TRACE("ignore virtual table", K(base_table_id), K(index_id));
        } else if (OB_FAIL(get_query_range_info(table_id, index_id, entry->get_range_info(), helper))) {
          LOG_WARN("failed to get query range", K(ret));
        } else {
          LOG_TRACE("finish extract query range", K(table_id), K(index_id));
        }
      }
      if (OB_SUCC(ret)) {
        index_entry = entry;
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(entry)) {
        entry->~IndexInfoEntry();
      }
    }
  }
  return ret;
}

// fill all index info entry
int ObJoinOrder::fill_index_info_cache(const uint64_t table_id, const uint64_t base_table_id,
    const ObIArray<uint64_t>& valid_index_ids, ObIndexInfoCache& index_info_cache, PathHelper& helper)
{
  int ret = OB_SUCCESS;
  index_info_cache.set_table_id(table_id);
  index_info_cache.set_base_table_id(base_table_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); i++) {
    IndexInfoEntry* index_info_entry = NULL;
    if (OB_FAIL(fill_index_info_entry(table_id, base_table_id, valid_index_ids.at(i), index_info_entry, helper))) {
      LOG_WARN("fill_index_info_entry failed", K(ret));
    } else if (OB_ISNULL(index_info_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fill index info entry failed", K(ret));
    } else if (OB_FAIL(index_info_cache.add_index_info_entry(index_info_entry))) {
      LOG_WARN("failed to add index info entry", K(ret));
    } else {
      LOG_TRACE("succeed to fill index index entry", K(*index_info_entry));
    }
  }
  return ret;
}

int ObJoinOrder::add_table(
    const uint64_t table_id, const uint64_t ref_table_id, PathHelper& helper, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> valid_index_ids;
  bool heuristics_used = false;
  ObIndexInfoCache index_info_cache;
  const ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  const ParamStore* params = NULL;
  bool is_valid = true;
  ObSEArray<uint64_t, 8> skyline_index_ids;
  bool is_contain_rowid = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) || OB_ISNULL(params = opt_ctx->get_params()) ||
      OB_ISNULL(opt_ctx->get_exec_ctx()) || OB_ISNULL(opt_ctx->get_exec_ctx()->get_sql_ctx()) ||
      OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(params), K(stmt), K(ret));
  } else if (OB_FAIL(get_valid_index_ids(table_id, ref_table_id, valid_index_ids))) {
    LOG_WARN("failed to get valid index ids", K(ret));
  } else if (OB_FAIL(fill_index_info_cache(table_id, ref_table_id, valid_index_ids, index_info_cache, helper))) {
    LOG_WARN("failed to fill index info cache", K(ret));
  } else if (opt_ctx->is_batched_multi_stmt() &&
             OB_FAIL(
                 check_multi_stmt_constraint(table_id, ref_table_id, *params, index_info_cache, helper, is_valid))) {
    LOG_WARN("failed to check multi stmt constraint", K(ret));
  } else if (!is_valid) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_TRACE("batched multi stmt needs rollback", K(ret));
  } else if (OB_FAIL(add_table_by_heuristics(
                 table_id, ref_table_id, index_info_cache, valid_index_ids, heuristics_used, helper, access_paths))) {
    LOG_WARN("failed to add table by heuristics", K(ret));
  } else if (heuristics_used) {
    LOG_TRACE("table added using heuristics", K(table_id));
  } else if (OB_FAIL(prunning_index(table_id,
                 ref_table_id,
                 stmt,
                 true,
                 index_info_cache,
                 valid_index_ids,
                 skyline_index_ids,
                 helper.filters_))) {
    LOG_WARN("failed to pruning_index", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_FAIL(compute_pruned_index(table_id, ref_table_id, skyline_index_ids, helper))) {
    LOG_WARN("failed to compute pruned index", K(table_id), K(ref_table_id), K(skyline_index_ids), K(ret));
  } else {
    LOG_TRACE("table added not using heuristics", K(table_id), K(skyline_index_ids));
    helper.table_opt_info_->optimization_method_ = OptimizationMethod::COST_BASED;
    for (int64_t i = 0; OB_SUCC(ret) && i < skyline_index_ids.count(); ++i) {
      AccessPath* access_path = NULL;
      if (OB_FAIL(create_access_path(
              table_id, ref_table_id, skyline_index_ids.at(i), index_info_cache, helper, access_path))) {
        LOG_WARN("failed to make index path", "index_table_id", skyline_index_ids.at(i), K(ret));
      } else if (NULL != access_path && OB_FAIL(access_paths.push_back(access_path))) {
        LOG_WARN("failed to push back access path", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_multi_stmt_constraint(uint64_t table_id, uint64_t ref_table_id, const ParamStore& param_store,
    const ObIndexInfoCache& index_info_cache, PathHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  const ObQueryRange* query_range = NULL;
  IndexInfoEntry* index_info_entry = NULL;
  ObLogPlan* log_plan = NULL;
  is_valid = false;
  if (OB_ISNULL(log_plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, ref_table_id, index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(ret));
  } else if (NULL == index_info_entry &&
             OB_FAIL(fill_index_info_entry(table_id, ref_table_id, ref_table_id, index_info_entry, helper))) {
    LOG_WARN("failed to fill index info entry", K(ret));
  } else if (OB_ISNULL(index_info_entry) ||
             OB_ISNULL(query_range = index_info_entry->get_range_info().get_query_range())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_info_entry), K(query_range), K(ret));
  } else if (OB_FAIL(query_range->is_get(is_get))) {
    LOG_WARN("failed to check whether is get", K(ret));
  } else if (!is_get) {
    is_valid = false;
    LOG_INFO("batched multi-stmt is not a get query");
  } else if (OB_FAIL(extract_param_for_query_range(
                 query_range->get_range_exprs(), log_plan->get_multi_stmt_rowkey_pos()))) {
    LOG_WARN("failed to extract param for query range", K(ret));
  } else if (OB_UNLIKELY(log_plan->get_multi_stmt_rowkey_pos().count() !=
                         index_info_entry->get_range_info().get_index_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count",
        K(log_plan->get_multi_stmt_rowkey_pos().count()),
        K(index_info_entry->get_range_info().get_index_column_count()),
        K(ret));
  } else if (OB_FAIL(ObPlanSet::match_multi_stmt_info(param_store, log_plan->get_multi_stmt_rowkey_pos(), is_valid))) {
    LOG_WARN("failed to check whether match multi stmt constrains", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObJoinOrder::extract_param_for_query_range(
    const ObIArray<ObRawExpr*>& range_conditions, ObIArray<int64_t>& param_pos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_conditions.count(); i++) {
    if (OB_FAIL(extract_param_for_query_range(range_conditions.at(i), param_pos))) {
      LOG_WARN("failed to extract param for query range", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to extract param for query range", K(param_pos));
  }
  return ret;
}

int ObJoinOrder::extract_param_for_query_range(const ObRawExpr* raw_expr, ObIArray<int64_t>& param_pos)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpecte null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (raw_expr->is_const_expr()) {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(raw_expr);
    if (const_expr->get_value().is_unknown()) {
      if (OB_FAIL(add_var_to_array_no_dup(param_pos, const_expr->get_value().get_unknown()))) {
        LOG_WARN("failed to push back into array", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_param_for_query_range(raw_expr->get_param_expr(i), param_pos)))) {
        LOG_WARN("failed to extract param for query range", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObJoinOrder::collect_table_est_info(const ObPartitionKey& pkey, obrpc::ObEstPartArg* est_arg)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("create table estimate argument", K(pkey));
  if (OB_ISNULL(est_arg) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("estimate argument is null", K(ret), K(est_arg), K(get_plan()));
  } else {
    est_arg->pkey_ = pkey;
    est_arg->schema_version_ = table_meta_info_.schema_version_;
    // init column ids
    if (OB_FAIL(extract_used_column_ids(
            table_id_, table_meta_info_.ref_table_id_, get_plan()->get_est_sel_info(), est_arg->column_ids_, true))) {
      LOG_WARN("failed to extract used column ids", K(ret));
    }
    // init partitions_keys
    const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list =
        table_partition_info_.get_phy_tbl_location_info().get_phy_part_loc_info_list();
    ObPartitionKey part_key;
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
      if (OB_FAIL(phy_part_loc_info_list.at(i).get_partition_location().get_partition_key(part_key))) {
        LOG_WARN("get partition key failed", K(ret), K(phy_part_loc_info_list.at(i).get_partition_location()));
      } else if (OB_FAIL(est_arg->partition_keys_.push_back(part_key))) {
        LOG_WARN("push partition key failed", K(ret), K(part_key));
      }
    }

    // init scan param
    if (OB_SUCC(ret)) {
      est_arg->scan_param_.scan_flag_.index_back_ = 0;
      est_arg->scan_param_.index_id_ = table_meta_info_.ref_table_id_;
      est_arg->scan_param_.range_columns_count_ = table_meta_info_.table_rowkey_count_;
      // NOTE ()
      // est_arg->scan_param_.batch_ is designed for whole range estimation
      // but it is never used. The estimation function would construct a
      // ObSimpleBatch structure by self.
      LOG_TRACE("succeed to collect est info", K(*est_arg));
    }
  }
  return ret;
}

int ObJoinOrder::collect_path_est_info(const AccessPath* ap, const ObPartitionKey& pkey, obrpc::ObEstPartArg* est_arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObNewRange, 4> get_ranges;
  ObSEArray<common::ObNewRange, 4> scan_ranges;
  obrpc::ObEstPartArgElement* index_est_arg = NULL;
  ObSqlSchemaGuard* schema_guard = OPT_CTX.get_sql_schema_guard();
  const ObTableSchema* table_schema = NULL;
  LOG_TRACE("create index estimate info", K(pkey));
  if (OB_ISNULL(ap) || OB_ISNULL(est_arg) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ap), K(est_arg));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard or table_schema is NULL", K(ret), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(ap->ref_table_id_, table_schema))) {
    LOG_WARN("failed to get talbe schema", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(ap->est_cost_info_.table_scan_param_.key_ranges_,
                 *allocator_,
                 get_ranges,
                 scan_ranges,
                 table_schema,
                 ObSQLMockSchemaUtils::is_mock_index(ap->index_id_)))) {
    LOG_WARN("failed to classify get scan ranges", K(ret));
  } else if (scan_ranges.count() <= 0) {
    // do nothing if the index does not have scan ranges
  } else if (OB_FAIL(est_arg->index_pkeys_.push_back(pkey))) {
    LOG_WARN("faield to push back partition key", K(ret));
  } else if (OB_ISNULL(index_est_arg = est_arg->index_params_.alloc_place_holder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate index argument", K(ret));
  } else {
    index_est_arg->index_id_ = ObSQLMockSchemaUtils::is_mock_index(ap->index_id_)
                                   ? ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(ap->index_id_)
                                   : ap->index_id_;
    index_est_arg->scan_flag_.index_back_ = ap->est_cost_info_.index_meta_info_.is_index_back_;
    index_est_arg->range_columns_count_ = ObSQLMockSchemaUtils::is_mock_index(ap->index_id_)
                                              ? table_meta_info_.table_rowkey_count_
                                              : ap->est_cost_info_.range_columns_.count();
    if (ap->is_global_index_) {
      for (int64_t i = 0; i < scan_ranges.count(); ++i) {
        scan_ranges.at(i).table_id_ = ap->index_id_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_scan_range_batch(scan_ranges, index_est_arg->batch_, ap, pkey))) {
        LOG_WARN("failed to construct scan range batch", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_estimate_task(ObIArray<ObRowCountEstTask>& task_list, const ObAddr& addr, ObRowCountEstTask*& task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  for (int64_t task_id = 0; task_id < task_list.count(); ++task_id) {
    if (addr == task_list.at(task_id).addr_) {
      task = &task_list.at(task_id);
      break;
    }
  }
  if (OB_NOT_NULL(task)) {
    // do nothing
  } else if (OB_ISNULL(task = task_list.alloc_place_holder()) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(allocator_), K(task));
  } else if (OB_ISNULL(
                 task->est_arg_ = static_cast<obrpc::ObEstPartArg*>(allocator_->alloc(sizeof(obrpc::ObEstPartArg))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for estimate argument", K(ret));
  } else {
    task->est_arg_ = new (task->est_arg_) obrpc::ObEstPartArg();
    task->addr_ = addr;
  }
  return ret;
}

/**
 * @brief ObJoinOrder::estimate_rowcount_for_access_path
 * STEP 1: choose a proper estimate partition for table info (best_table_part)
 * STEP 2: choose a proper estimate partition for each index path
 *        i. local index uses the best_table_part
 *       ii. global index choose a new estimate partition
 * STEP 3: process all estimation task using local/remote storage
 * STEP 4: fill estimate results and process filter selectivity
 * @return
 */
int ObJoinOrder::estimate_rowcount_for_access_path(const uint64_t table_id, const uint64_t ref_table_id,
    const ObIArray<AccessPath*>& all_paths, const bool is_inner_path, const bool no_use_remote_est)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRowCountEstTask, 4> tasks;
  ObRowCountEstTask* table_est_task = NULL;
  ObSEArray<ObAddr, 4> addr_list;

  obrpc::ObEstPartRes all_path_results;

  const EstimatedPartition& best_table_part = table_meta_info_.table_est_part_;
  // do not use storage estimation when
  // 1. all partitions are stored in the remote and we cannot use RPC estimation
  // 2. it is a virtual table, which uses default row count
  bool is_vt = is_oracle_mapping_real_virtual_table(table_meta_info_.ref_table_id_);
  const bool use_storage_estimation = best_table_part.is_valid() &&
                                      (!is_virtual_table(table_meta_info_.ref_table_id_) || is_vt) &&
                                      !OPT_CTX.use_default_stat();

  if (OB_FAIL(all_path_results.index_param_res_.prepare_allocate(all_paths.count()))) {
    LOG_WARN("failed to pre-allocate estimate results", K(ret));
  } else if (!use_storage_estimation) {
    // do nothing
  } else if (OB_FAIL(addr_list.push_back(best_table_part.addr_))) {
    LOG_WARN("failed to push back address", K(ret));
  } else if (OB_FAIL(get_estimate_task(tasks, best_table_part.addr_, table_est_task))) {
    LOG_WARN("failed to create est argument", K(ret), K(best_table_part));
  } else if (OB_ISNULL(table_est_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table estimation task is null", K(ret));
  } else if (OB_FAIL(collect_table_est_info(best_table_part.pkey_, table_est_task->est_arg_))) {
    LOG_WARN("failed to collect table est info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && use_storage_estimation && i < all_paths.count(); ++i) {
    AccessPath* ap = NULL;
    EstimatedPartition best_index_part;
    ObRowCountEstTask* index_est_task = NULL;
    if (OB_ISNULL(ap = all_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access path is null", K(ret));
    } else if (!ap->is_global_index_) {
      best_index_part = best_table_part;
      index_est_task = table_est_task;
    } else if (OB_ISNULL(ap->table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObSQLUtils::choose_best_partition_for_estimation(
                   ap->table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list(),
                   OPT_CTX.get_partition_service(),
                   *OPT_CTX.get_stat_manager(),
                   OPT_CTX.get_local_server_addr(),
                   addr_list,
                   no_use_remote_est,
                   best_index_part))) {
      LOG_WARN("failed to choose best partition for global index", K(ret));
    } else if (!best_index_part.is_valid()) {
      // does not find a non-empty partition for global index estimation
    } else if (OB_FAIL(get_estimate_task(tasks, best_index_part.addr_, index_est_task))) {
      LOG_WARN("failed to find estimate tasks", K(ret));
    } else if (OB_ISNULL(index_est_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("estimation task is null", K(ret));
    } else if (!index_est_task->est_arg_->index_params_.empty()) {
      // do nothing for used estimation task
    } else if (OB_FAIL(addr_list.push_back(best_index_part.addr_))) {
      LOG_WARN("failed to append addr list", K(ret));
    } else if (OB_FAIL(index_est_task->est_arg_->column_ids_.assign(table_est_task->est_arg_->column_ids_))) {
      LOG_WARN("failed to assgin table estimate task", K(ret));
    } else {
      index_est_task->est_arg_->schema_version_ = table_meta_info_.schema_version_;
    }
    if (OB_SUCC(ret) && best_index_part.is_valid()) {
      if (OB_FAIL(index_est_task->path_id_set_.add_member(i))) {
        LOG_WARN("failed to add path id in bit set", K(ret));
      } else if (OB_FAIL(collect_path_est_info(ap, best_index_part.pkey_, index_est_task->est_arg_))) {
        LOG_WARN("failed to collect path est info", K(ret));
      } else {
        LOG_TRACE("path est info", K(*index_est_task->est_arg_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_storage_rowcount_estimation(tasks, all_paths, all_path_results))) {
      LOG_WARN("failed to estimate rowcount with storage", K(ret));
    } else if (!is_inner_path && OB_FAIL(compute_table_rowcount_info(all_path_results, table_id, ref_table_id))) {
      LOG_WARN("failed to estimate scan param", K(ret));
    } else {
      LOG_TRACE("storage estimate results", K(all_path_results), K(is_inner_path));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_paths.count(); i++) {
    AccessPath* ap = all_paths.at(i);
    ap->table_row_count_ = table_meta_info_.table_row_count_;
    ap->est_cost_info_.table_meta_info_.assign(table_meta_info_);
    if (ap->is_global_index_) {
      if (OB_ISNULL(ap->table_partition_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        ap->est_cost_info_.table_meta_info_.part_count_ =
            ap->table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
      }
    }
    // estimate row count
    if (OB_SUCC(ret)) {
      const obrpc::ObEstPartResElement& result = all_path_results.index_param_res_.at(i);
      if (OB_FAIL(fill_table_scan_param(ap->est_cost_info_))) {
        LOG_WARN("failed to fill scan param for cost table info", K(ret));
      } else if (OB_FAIL(ap->est_records_.assign(result.est_records_))) {
        LOG_WARN("failed to assign estimate records", K(ret));
      } else if (OB_FAIL(ObOptEstCost::estimate_row_count(result,
                     ap->est_cost_info_,
                     &get_plan()->get_predicate_selectivities(),
                     ap->output_row_count_,
                     ap->query_range_row_count_,
                     ap->phy_query_range_row_count_,
                     ap->index_back_row_count_))) {
        LOG_WARN("failed to estimate row count and cost", K(ret));
      }
    }
  }  // for paths end
  return ret;
}

/**
 * @brief ObJoinOrder::process_storage_rowcount_estimation
 * process a estimation task
 * do  local storage estimation if the local addr is provided;
 * do remote storage estimation if a valid remote addr is provided;
 *
 * and keep storage estimation results in all_results
 * if the table is empty, use DEFAULT_STAT in the following
 * @return
 */
int ObJoinOrder::process_storage_rowcount_estimation(
    ObIArray<ObRowCountEstTask>& tasks, const ObIArray<AccessPath*>& all_paths, obrpc::ObEstPartRes& all_results)
{
  int ret = OB_SUCCESS;
  RowCountEstMethod est_method = RowCountEstMethod::BASIC_STAT;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(OPT_CTX.get_stat_manager())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(get_plan()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_paths.count(); ++i) {
    if (OB_ISNULL(all_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("path is null", K(ret), K(i));
    } else {  // in default, we use NDV to estimate rowcount
      all_paths.at(i)->est_cost_info_.row_est_method_ = est_method;
    }
  }
  for (int64_t task_id = 0; OB_SUCC(ret) && task_id < tasks.count(); ++task_id) {
    const ObRowCountEstTask& task = tasks.at(task_id);
    obrpc::ObEstPartRes result;
    if (OB_ISNULL(task.est_arg_) || OB_UNLIKELY(!task.addr_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param", K(ret), K(task.est_arg_), K(task.addr_));
    } else if (task.addr_ == OPT_CTX.get_local_server_addr()) {
      est_method = RowCountEstMethod::LOCAL_STORAGE;
      if (OB_FAIL(ObOptEstCost::storage_estimate_rowcount(
              OPT_CTX.get_partition_service(), *OPT_CTX.get_stat_manager(), *task.est_arg_, result))) {
        LOG_WARN("failed to estimate partition rows", K(ret));
        ret = OB_SUCCESS;
      }
    } else {
      est_method = RowCountEstMethod::REMOTE_STORAGE;
      if (OB_FAIL(remote_storage_estimate_rowcount(task.addr_, *task.est_arg_, result))) {
        LOG_WARN("failed to estimate use remote storage", K(ret));
        ret = OB_SUCCESS;
      }
    }
    // try to fill table stat
    if (OB_SUCC(ret) && task_id == 0) {
      all_results.part_rowcount_size_res_ = result.part_rowcount_size_res_;
      if (all_results.part_rowcount_size_res_.row_count_ == 0) {
        all_results.part_rowcount_size_res_.reliable_ = false;
      }
      if (!all_results.part_rowcount_size_res_.reliable_) {
        // table row count = 0, use default row count
        break;
      }
    }
    // try to fill access path stat
    const ObIArray<obrpc::ObEstPartResElement>& index_results = result.index_param_res_;
    for (int64_t i = 0, rid = 0; OB_SUCC(ret) && i < all_paths.count() && rid < index_results.count(); ++i) {
      if (!task.path_id_set_.has_member(i)) {
        continue;
      } else if (!index_results.at(rid).reliable_) {
        // do nothing if the storage estimation result is not reliable
      } else if (all_paths.at(i)->is_global_index_ && all_paths.at(i)->est_cost_info_.ranges_.count() == 1 &&
                 all_paths.at(i)->est_cost_info_.ranges_.at(0).is_whole_range() &&
                 index_results.at(rid).logical_row_count_ == 0) {
        // Although table has rows, we find the partition of the gindex is emtpy
        // It happens when there is no table stat for choosing estimated partition
        // Ignore the storage estimate results
      } else {
        all_results.index_param_res_.at(i) = index_results.at(rid);
        all_paths.at(i)->est_cost_info_.row_est_method_ = est_method;
      }
      ++rid;
    }
  }
  // release memory used by row count estimation task
  for (int64_t i = 0; i < tasks.count(); ++i) {
    if (OB_ISNULL(tasks.at(i).est_arg_)) {
      // do nothing
    } else {
      tasks.at(i).est_arg_->~ObEstPartArg();
      tasks.at(i).est_arg_ = NULL;
    }
  }
  return ret;
}

int ObJoinOrder::remote_storage_estimate_rowcount(
    const ObAddr& addr, const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& result)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy* rpc_proxy = NULL;
  const ObSQLSessionInfo* session_info = NULL;
  int64_t timeout = -1;
  if (OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(session_info = OPT_CTX.get_session_info()) || OB_ISNULL(rpc_proxy = OPT_CTX.get_srv_proxy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy or session is null", K(ret), K(rpc_proxy), K(session_info));
  } else if (0 >= (timeout = THIS_WORKER.get_timeout_remain())) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout is reached", K(ret), K(timeout));
  } else if (OB_FAIL(rpc_proxy->to(addr)
                         .timeout(timeout)
                         .by(session_info->get_rpc_tenant_id())
                         .estimate_partition_rows(arg, result))) {
    LOG_WARN("OPT:[REMOTE STORAGE EST FAILED]", K(ret));
  }
  LOG_INFO("remote estimate result", K(ret), K(addr), K(arg), K(result));
  return ret;
}

int ObJoinOrder::compute_table_rowcount_info(
    const obrpc::ObEstPartRes& result, const uint64_t table_id, const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!result.part_rowcount_size_res_.reliable_) {
    // use default row count if partition rowcount = 0
    const common::ObTableStat& tstat = ObStatManager::get_default_table_stat();
    table_meta_info_.cost_est_type_ = OB_DEFAULT_STAT_EST;
    table_meta_info_.average_row_size_ = static_cast<double>(tstat.get_average_row_size());
    table_meta_info_.table_row_count_ = table_meta_info_.part_count_ * tstat.get_row_count();
    table_meta_info_.part_size_ = static_cast<double>(tstat.get_data_size());
  } else {
    table_meta_info_.cost_est_type_ = OB_CURRENT_STAT_EST;
    table_meta_info_.average_row_size_ = result.part_rowcount_size_res_.avg_row_size_;
    table_meta_info_.table_row_count_ = result.part_rowcount_size_res_.row_count_;
    table_meta_info_.part_size_ = static_cast<double>(result.part_rowcount_size_res_.part_size_);
  }
  if (OB_SUCC(ret)) {
    double selectivity = 0.0;
    if (OB_FAIL(init_est_sel_info_for_access_path(table_id, ref_table_id))) {
      LOG_WARN("Failed to init est sel info", K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   get_restrict_infos(),
                   selectivity,
                   &get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("Failed to calc selectivities", K(get_restrict_infos()), K(ret));
    } else {
      table_meta_info_.row_count_ = static_cast<double>(table_meta_info_.table_row_count_) * selectivity;
      set_output_rows(table_meta_info_.row_count_);
    }
    LOG_TRACE("OPT: after fill table meta info", K(table_meta_info_), K(selectivity));
  }
  return ret;
}

int ObJoinOrder::get_valid_index_ids(
    const uint64_t table_id, const uint64_t ref_table_id, ObIArray<uint64_t>& valid_index_ids)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  const TableItem* table_item = NULL;
  const ObPartHint* part_hint = NULL;
  const ObIndexHint* index_hint = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(schema_guard), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table item should not be NULL", K(table_id), K(table_item), K(ret));
  } else if (FALSE_IT(part_hint = stmt->get_stmt_hint().get_part_hint(table_id))) {
  } else if (FALSE_IT(index_hint = stmt->get_stmt_hint().get_index_hint(table_id))) {
  } else if (table_item->is_materialized_view_) {
    if (OB_FAIL(valid_index_ids.push_back(table_id))) {
      LOG_WARN("failed to push back array", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(get_hint_index_ids(ref_table_id, index_hint, part_hint, valid_index_ids))) {
    LOG_WARN("failed to get hint index ids", K(ret));
  } else if (0 == valid_index_ids.count()) {
    uint64_t tids[OB_MAX_INDEX_PER_TABLE + 1];
    int64_t index_count = OB_MAX_INDEX_PER_TABLE + 1;
    const bool with_global_index = (nullptr == part_hint);
    const bool with_mv = false;
    if (OB_FAIL(schema_guard->get_can_read_index_array(
            ref_table_id, tids, index_count, with_mv, with_global_index, false /*domain index*/))) {
      LOG_WARN("failed to get can read index", K(ref_table_id), K(ret));
    } else if (index_count > OB_MAX_INDEX_PER_TABLE + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid index count", K(ref_table_id), K(index_count), K(ret));
    } else {
      for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
        const uint64_t tid = (i == -1) ? ref_table_id : tids[i];  // with base table
        if (OB_FAIL(valid_index_ids.push_back(tid))) {
          LOG_WARN("failed to push back index id", K(ret));
        } else { /*do nothing*/
        }
      }

      // push rowid index table id if possible
      bool is_contain_rowid = false;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(const_cast<ObDMLStmt*>(stmt)->check_rowid_column_exists(table_id, is_contain_rowid))) {
        LOG_WARN("failed to check rowid column exists", K(ret));
      } else if (is_contain_rowid && !is_virtual_table(ref_table_id) && share::is_oracle_mode() &&
                 OB_FAIL(valid_index_ids.push_back(ObSQLMockSchemaUtils::get_rowid_index_table_id(ref_table_id)))) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        // do nothing
      }
    }
  } else { /*do nothing*/
  }
  LOG_TRACE("all valid index id", K(valid_index_ids), K(ret));
  return ret;
}

int ObJoinOrder::add_function_table(const uint64_t table_id, const uint64_t ref_id)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(allocator_), K(stmt), K(ret));
  } else {
    uint64_t base_table_id = OB_INVALID_ID;
    const TableItem* table_item = stmt->get_table_item_by_id(table_id);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table item should not be NULL", K(table_id), K(table_item), K(ret));
    } else if (OB_INVALID_ID == (base_table_id = table_item->table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("base_table_id is invalid", K(base_table_id), K(*table_item), K(ret));
    } else {
      AccessPath* ap = NULL;
      if (OB_ISNULL(ap = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate an AccessPath", K(ret));
      }
      ap = new (ap) AccessPath(table_id, ref_id, table_id, this, default_asc_direction());
      ap->set_index_table_id(ref_id);
      ap->set_function_table_path(true);
      ap->table_opt_info_ = &table_opt_info_;
      ap->table_row_count_ = 1;
      ap->output_row_count_ = 1;
      if (OB_FAIL(add_path(ap))) {
        LOG_WARN("failed to add the interesting order");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < get_restrict_infos().count(); ++i) {
        if (OB_FAIL(ap->filter_.push_back(get_restrict_infos().at(i)))) {
          LOG_WARN("push back error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::add_cte_table(const uint64_t table_id, const uint64_t ref_id)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(allocator_), K(stmt), K(ret));
  } else {
    // bool succ_to_add_path_by_hint = false;
    uint64_t base_table_id = OB_INVALID_ID;
    const TableItem* table_item = stmt->get_table_item_by_id(table_id);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table item should not be NULL", K(table_id), K(table_item), K(ret));
    } else if (OB_INVALID_ID == (base_table_id = table_item->ref_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("base_table_id is invalid", K(base_table_id), K(*table_item), K(ret));
    } else {
      AccessPath* ap = NULL;
      if (OB_ISNULL(ap = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate an AccessPath", K(ret));
      } else {
        ap = new (ap) AccessPath(table_id, ref_id, table_id, this, default_asc_direction());
        ap->set_index_table_id(ref_id);
        ap->set_cte_path(true);
        ap->table_opt_info_ = &table_opt_info_;
        ap->table_row_count_ = 1;
        ap->output_row_count_ = 1;
        // last version we use calc_cte_table_location_info(table_id, ref_id) for table location
        if (OB_FAIL(add_path(ap))) {
          // This should always be the last step in this funtion
          LOG_WARN("failed to add the interesting order", K(ret));
        } else if (OB_FAIL(append(ap->filter_, get_restrict_infos()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_cost_and_prune_access_path(PathHelper& helper, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  ObSqlCtx* sql_ctx = NULL;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_optimizer_context().get_exec_ctx()) ||
      OB_ISNULL(task_exec_ctx = get_plan()->get_optimizer_context().get_task_exec_ctx()) ||
      OB_ISNULL(sql_ctx = get_plan()->get_optimizer_context().get_exec_ctx()->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(sql_ctx), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
      AccessPath* ap = access_paths.at(i);
      if (OB_ISNULL(ap) || OB_ISNULL(ap->get_sharding_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ap), K(ret));
      } else if (sql_ctx->is_remote_sql_ && ap->get_sharding_info()->is_remote_or_distribute()) {
        /*do nothing*/
      } else if (OB_FAIL(ap->estimate_cost())) {
        LOG_WARN("failed to get index access info", K(*ap), K(ret));
      } else if (!ap->is_inner_path()) {
        if (OB_FAIL(fill_acs_index_info(*ap))) {
          LOG_WARN("failed to fill acs index info", K(ret));
        } else if (OB_FAIL(add_path(ap))) {
          LOG_WARN("failed to add the interesting order");
        } else {
          LOG_TRACE("OPT:succeed to create normal access path", K(*ap));
        }
      } else if (!is_virtual_table(ap->get_ref_table_id()) ||
                 is_oracle_mapping_real_virtual_table(ap->get_ref_table_id()) || ap->is_get_) {
        if (OB_FAIL(helper.inner_paths_.push_back(ap))) {
          LOG_WARN("failed to push back inner path", K(ret));
        } else {
          LOG_TRACE("OPT:succeed to add inner access path", K(*ap));
        }
      }
    }  // add path end
    if (OB_SUCC(ret) && sql_ctx->is_remote_sql_ && interesting_paths_.empty() && !helper.is_inner_path_) {
      // for the purpose to refresh location cache
      ObSEArray<ObTablePartitionInfo*, 8> table_partitions;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); i++) {
        AccessPath* path = NULL;
        if (OB_ISNULL(path = access_paths.at(i)) || OB_ISNULL(path->table_partition_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(path));
        } else if (1 != path->table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count()) {
          // do nothing
        } else if (is_contain(table_partitions, path->table_partition_info_)) {
          /*do nothing*/
        } else if (OB_FAIL(path->table_partition_info_->set_log_op_infos(path->index_id_, path->order_direction_))) {
          LOG_WARN("failed to set log op infos", K(ret));
        } else if (OB_FAIL(table_partitions.push_back(path->table_partition_info_))) {
          LOG_WARN("failed to push back table partition", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_FAIL(task_exec_ctx->append_table_locations_no_dup(table_partitions))) {
        LOG_WARN("failed to append table locations no dup", K(ret));
      } else {
        ret = (get_plan()->get_optimizer_context().is_cost_evaluation() ? OB_SQL_OPT_GEN_PLAN_FALIED
                                                                        : OB_LOCATION_NOT_EXIST);
        LOG_WARN("no available local path for remote sql", K(ret));
      }
    }
  }
  return ret;
}

/*
 * this function try to revise output rows after creating all paths
 */
int ObJoinOrder::revise_output_rows_after_creating_path(PathHelper& helper, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  AccessPath* path = NULL;
  if (!helper.is_inner_path_) {
    LOG_TRACE("OPT:output row count before revising", K(output_rows_));
    // get the minimal output row count
    int64_t maximum_count = -1;
    int64_t range_prefix_count = -1;
    RowCountEstMethod estimate_method = INVALID_METHOD;
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
      AccessPath* path = access_paths.at(i);
      if (OB_ISNULL(path)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null path", K(ret));
      } else if (path->est_cost_info_.row_est_method_ == BASIC_STAT &&
                 (estimate_method == LOCAL_STORAGE || estimate_method == REMOTE_STORAGE)) {
        // do nothing if the path is estimated by ndv
      } else if (OB_UNLIKELY((range_prefix_count = path->range_prefix_count_) < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range prefix count", K(ret), K(range_prefix_count));
      } else if (maximum_count <= range_prefix_count) {
        LOG_TRACE("OPT:revise output rows",
            K(path->output_row_count_),
            K(output_rows_),
            K(maximum_count),
            K(range_prefix_count),
            K(ret));
        if (maximum_count == range_prefix_count) {
          output_rows_ = std::min(path->output_row_count_, output_rows_);
        } else {
          output_rows_ = path->output_row_count_;
          maximum_count = range_prefix_count;
        }
        estimate_method = path->est_cost_info_.row_est_method_;
      } else { /*do nothing*/
      }
    }

    // update index rows in normal path
    for (int64_t i = 0; OB_SUCC(ret) && i < interesting_paths_.count(); ++i) {
      if (OB_ISNULL(interesting_paths_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null path", K(ret));
      } else if (ACCESS != interesting_paths_.at(i)->path_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be access path", K(ret));
      } else {
        path = static_cast<AccessPath*>(interesting_paths_.at(i));
        path->est_cost_info_.row_est_method_ = estimate_method;
      }
    }

    LOG_TRACE("OPT:output row count after revising", K(output_rows_));

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_plan()->get_est_sel_info().update_table_stats(*this))) {
        LOG_WARN("failed to update left table stats", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan and stmt should not be null", K(ret));
    } else if (get_plan()->get_stmt()->has_limit()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < interesting_paths_.count(); ++i) {
        if (OB_ISNULL(interesting_paths_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null path", K(ret));
        } else if (ACCESS != interesting_paths_.at(i)->path_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should be access path", K(ret));
        } else {
          path = static_cast<AccessPath*>(interesting_paths_.at(i));
          if (OB_UNLIKELY(std::fabs(path->output_row_count_) < OB_DOUBLE_EPSINON)) {
            // do nothing
          } else {
            double revise_ratio = output_rows_ / path->output_row_count_;
            ObCostTableScanInfo& cost_info = path->get_cost_table_scan_info();
            bool has_table_filter = !(std::fabs(cost_info.table_filter_sel_ - 1.0) < OB_DOUBLE_EPSINON);
            bool has_postfix_filter = !(std::fabs(cost_info.postfix_filter_sel_ - 1.0) < OB_DOUBLE_EPSINON);
            if (!has_table_filter && !has_postfix_filter) {
              // do nothing
            } else if (has_table_filter && !has_postfix_filter) {
              cost_info.table_filter_sel_ *= revise_ratio;
            } else if (!has_table_filter && has_postfix_filter) {
              cost_info.postfix_filter_sel_ *= revise_ratio;
            } else {
              cost_info.table_filter_sel_ *= std::sqrt(revise_ratio);
              cost_info.postfix_filter_sel_ *= std::sqrt(revise_ratio);
            }
            cost_info.table_filter_sel_ = ObOptEstSel::revise_between_0_1(cost_info.table_filter_sel_);
            cost_info.postfix_filter_sel_ = ObOptEstSel::revise_between_0_1(cost_info.postfix_filter_sel_);
          }
        }
      }
    } else {
      // do nothing, we only revise selectivity when LIMIT is provided
    }
  } else {
    // update index rows in inner index path
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_paths_.count(); ++i) {
      if (OB_ISNULL(helper.inner_paths_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null path", K(ret));
      } else {
        path = static_cast<AccessPath*>(helper.inner_paths_.at(i));
        path->inner_row_count_ = std::min(path->output_row_count_, output_rows_);
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_pruned_index(
    const uint64_t table_id, const uint64_t base_table_id, ObIArray<uint64_t>& available_index_id, PathHelper& helper)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  uint64_t index_ids[OB_MAX_INDEX_PER_TABLE + 1];
  int64_t index_count = OB_MAX_INDEX_PER_TABLE + 1;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(schema_guard), K(helper.table_opt_info_), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == base_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(base_table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(
                 base_table_id, index_ids, index_count, false, true /*global index*/, false /*domain index*/))) {
    LOG_WARN("failed to get can read index", K(base_table_id), K(ret));
  } else if (index_count > OB_MAX_INDEX_PER_TABLE + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid index count", K(base_table_id), K(index_count), K(ret));
  } else if (OB_FAIL(helper.table_opt_info_->available_index_id_.assign(available_index_id))) {
    LOG_WARN("failed to assign available index id", K(ret));
  } else {
    // i == -1 represents primary key, other value of i represent index
    for (int64_t i = -1; OB_SUCC(ret) && i < index_count; i++) {
      ObString name;
      bool is_find = false;
      uint64_t index_id = (i == -1) ? base_table_id : index_ids[i];
      if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret), K(index_id));
      } else if (i == -1) {
        name = table_schema->get_table_name_str();
      } else if (OB_FAIL(table_schema->get_index_name(name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else { /*do nothing*/
      }
      for (int64_t j = 0; OB_SUCC(ret) && (!is_find) && j < available_index_id.count(); j++) {
        if (index_id == available_index_id.at(j)) {
          is_find = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_find) {
          if (OB_FAIL(helper.table_opt_info_->available_index_name_.push_back(name))) {
            LOG_WARN("failed to push back index name", K(name), K(ret));
          } else { /* do nothing */
          }
        } else {
          if (OB_FAIL(helper.table_opt_info_->pruned_index_name_.push_back(name))) {
            LOG_WARN("failed to push back index name", K(name), K(ret));
          } else { /* do nothing */
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (ObSQLMockSchemaUtils::contain_mock_index(base_table_id) &&
               OB_FAIL(helper.table_opt_info_->available_index_name_.push_back(
                   ObString::make_string(ObSQLMockSchemaUtils::get_rowid_index_name())))) {
      LOG_WARN("failed to push back rowid index name", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_all_interesting_order(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt,
    int64_t& max_prefix_count, int64_t& interesting_order_info)
{
  int ret = OB_SUCCESS;
  max_prefix_count = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(get_plan()));
  } else {
    int64_t prefix_count = 0;
    bool join_match = false;
    if (OB_FAIL(is_join_match(ordering, prefix_count, join_match))) {
      LOG_WARN("check_join_match failed", K(ret));
    } else if (join_match) {
      max_prefix_count = std::max(max_prefix_count, prefix_count);
      interesting_order_info |= OrderingFlag::JOIN_MATCH;
      LOG_TRACE("check is_join_match debug", K(join_match), K(max_prefix_count), K(prefix_count));
    }
    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::CHECK_ALL;
      prefix_count = 0;
      if (OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(ordering,
              stmt,
              get_plan()->get_is_subplan_scan(),
              get_plan()->get_equal_sets(),
              get_plan()->get_const_exprs(),
              check_scope,
              interesting_order_info,
              &prefix_count))) {
        LOG_WARN("failed to compute stmt interesting order", K(ret));
      } else {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_interesting_column_ids(const ObIArray<ObRawExpr*>& keys, const int64_t& max_prefix_count,
    ObIArray<uint64_t>& interest_column_ids, ObIArray<bool>& const_column_info)
{
  int ret = OB_SUCCESS;
  if (max_prefix_count > 0) {  // some sort match
    if (OB_FAIL(ObSkylineDimRecorder::extract_column_ids(keys, max_prefix_count, interest_column_ids))) {
      LOG_WARN("extract column ids failed", K(ret));
    } else {
      LOG_TRACE("check interesting order debug", K(max_prefix_count), K(keys.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < max_prefix_count; i++) {
        bool is_const = false;
        if (OB_ISNULL(keys.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(
                       keys.at(i), ordering_output_equal_sets_, get_const_exprs(), is_const))) {
          LOG_WARN("check expr is const expr failed", K(ret));
        } else if (OB_FAIL(const_column_info.push_back(is_const))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to push back const column info", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::is_join_match(const ObIArray<OrderItem>& ordering,
    int64_t& prefix_count,  // max join match prefix
    bool& sort_match)
{
  int ret = OB_SUCCESS;
  sort_match = false;
  prefix_count = 0;
  const JoinInfo* join_info = NULL;
  ObSEArray<ObRawExpr*, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  ObSEArray<ObRawExpr*, 4> related_join_keys;
  ObSEArray<ObRawExpr*, 4> other_join_keys;
  bool dummy_is_coverd = false;
  int64_t match_prefix_count = 0;
  ObLogPlan* plan = get_plan();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else {
    const ObIArray<ConflictDetector*>& conflict_detectors = plan->get_conflict_detectors();
    // get max prefix count from inner join infos
    for (int64_t i = 0; OB_SUCC(ret) && i < conflict_detectors.count(); i++) {
      related_join_keys.reuse();
      other_join_keys.reuse();
      if (OB_ISNULL(conflict_detectors.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detectors", K(ret));
      } else if (OB_FALSE_IT(join_info = &(conflict_detectors.at(i)->join_info_))) {
      } else if (!join_info->table_set_.overlap(get_tables()) ||
                 ObOptimizerUtil::find_item(used_conflict_detectors_, conflict_detectors.at(i))) {
        // do nothing
      } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(
                     join_info->equal_join_condition_, get_tables(), related_join_keys, other_join_keys))) {
        LOG_WARN("failed to get equal keys", K(ret));
      } else if (OB_FAIL(extract_real_join_keys(related_join_keys))) {
        LOG_WARN("failed to extract real join keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(related_join_keys,
                     ordering_exprs,
                     ordering_output_equal_sets_,
                     get_const_exprs(),
                     dummy_is_coverd,
                     &match_prefix_count))) {
        LOG_WARN("failed to fidn common prefix ordering", K(ret));
      } else {
        prefix_count = std::max(prefix_count, match_prefix_count);
      }
    }
    if (OB_SUCC(ret)) {
      if (prefix_count > 0) {
        sort_match = true;
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_expr_overlap_index(const ObRawExpr* qual, const ObIArray<ObRawExpr*>& keys, bool& overlap)
{
  int ret = OB_SUCCESS;
  overlap = false;
  ObArray<ObRawExpr*> cur_vars;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(qual, cur_vars))) {
    LOG_WARN("extract_column_exprs error", K(ret));
  } else if (ObOptimizerUtil::overlap_exprs(cur_vars, keys)) {
    overlap = true;
  } else { /*do nothing*/
  }
  return ret;
}

int ObJoinOrder::extract_filter_column_ids(const ObIArray<ObRawExpr*>& quals, const bool is_data_table,
    const ObTableSchema& index_schema, ObIArray<uint64_t>& filter_column_ids)
{
  int ret = OB_SUCCESS;
  filter_column_ids.reset();
  if (quals.count() > 0) {
    ObArray<ObRawExpr*> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(quals, column_exprs))) {
      LOG_WARN("extract_column_expr error", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
        const ObColumnRefRawExpr* column = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i));
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr should not be null", K(ret), K(i));
        } else {
          const uint64_t column_id = column->get_column_id();
          bool has = false;
          if (is_data_table) {
            has = true;
          } else if (OB_FAIL(index_schema.has_column(column_id, has))) {
            LOG_WARN("check has column failed", K(column_id), K(has), K(ret));
          }
          if (OB_SUCC(ret) && has) {
            if (!ObRawExprUtils::contain_id(filter_column_ids, column_id)) {
              if (OB_FAIL(filter_column_ids.push_back(column_id))) {
                LOG_WARN("failed to push back column_ids", K(ret));
              }
            }
          }
        }
      }
    }
  }
  LOG_TRACE("extract filter column ids finish", K(ret), K(filter_column_ids), K(quals.count()));
  return ret;
}

int ObJoinOrder::check_exprs_overlap_index(
    const ObIArray<ObRawExpr*>& quals, const ObIArray<ObRawExpr*>& keys, bool& match)
{
  LOG_TRACE("OPT:[CHECK MATCH]", K(keys));

  int ret = OB_SUCCESS;
  match = false;
  if (keys.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Index keys should not be empty", K(keys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !match && i < quals.count(); ++i) {
      ObRawExpr* expr = quals.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("qual expr should not be NULL", K(expr), K(i), K(ret));
      } else {
        LOG_TRACE("OPT:[CHECK MATCH]", K(*expr));
        if (OB_FAIL(check_expr_overlap_index(expr, keys, match))) {
          LOG_WARN("check_expr_overlap_index error", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_preliminary_query_range(
    const ObIArray<ColumnItem>& range_columns, const ObIArray<ObRawExpr*>& predicates, ObQueryRange*& query_range)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  const ParamStore* params = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(params = opt_ctx->get_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(allocator_), K(params), K(ret));
  } else {
    void* tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange* tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new (tmp_ptr) ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
      if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, predicates, dtc_params, params))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = tmp_qr;
    } else {
      if (NULL != tmp_qr) {
        tmp_qr->~ObQueryRange();
        tmp_qr = NULL;
      }
    }
  }
  return ret;
}

int sql::Path::deep_copy(const sql::Path& other, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  int64_t N = 0;
  int64_t i = 0;
  path_type_ = other.path_type_;
  parent_ = other.parent_;
  if (OB_FAIL(pushdown_filters_.assign(other.pushdown_filters_))) {
    LOG_WARN("failed to assign pushdown filters", K(ret));
  } else if (OB_FAIL(nl_params_.assign(other.nl_params_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  }
  N = other.ordering_.count();
  for (i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = ordering_.push_back(other.ordering_.at(i));
  }
  N = other.filter_.count();
  for (i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = filter_.push_back(other.filter_.at(i));
  }
  cost_ = other.cost_;
  op_cost_ = other.op_cost_;
  N = other.exec_params_.count();
  for (i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = exec_params_.push_back(other.exec_params_.at(i));
  }
  log_op_ = other.log_op_;
  interesting_order_info_ = other.interesting_order_info_;
  is_inner_path_ = other.is_inner_path_;
  return ret;
}

const ObShardingInfo* AccessPath::get_sharding_info() const
{
  const ObShardingInfo* ret = sharding_info_;
  if (!is_global_index_ && parent_ != NULL) {
    ret = &(parent_->get_sharding_info());
  }
  return ret;
}

int AccessPath::deep_copy(const AccessPath& other, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(allocator), K(ret));
  } else if (OB_FAIL(sql::Path::deep_copy(other, allocator))) {
    LOG_WARN("copy path error", K(ret));
  } else if (OB_FAIL(est_cost_info_.assign(other.est_cost_info_))) {
    LOG_WARN("Failed to assign re_est_param", K(ret));
  } else {
    sharding_info_ = other.sharding_info_;
    table_partition_info_ = other.table_partition_info_;
    table_id_ = other.table_id_;
    ref_table_id_ = other.ref_table_id_;
    index_id_ = other.index_id_;
    // deep copy query range
    if (NULL != other.pre_query_range_) {
      ObQueryRange* query_range = static_cast<ObQueryRange*>(allocator->alloc(sizeof(ObQueryRange)));
      if (OB_ISNULL(query_range)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory for query range");
      } else {
        query_range = new (query_range) ObQueryRange(*allocator);
        if (OB_FAIL(query_range->deep_copy(*other.pre_query_range_))) {
          query_range->~ObQueryRange();
          query_range = NULL;
        } else {
          pre_query_range_ = query_range;
        }
      }
    } else {
      pre_query_range_ = NULL;
    }
    inner_row_count_ = other.inner_row_count_;
    is_get_ = other.is_get_;
    order_direction_ = other.order_direction_;
    is_hash_index_ = other.is_hash_index_;
    table_row_count_ = other.table_row_count_;
    output_row_count_ = other.output_row_count_;
    phy_query_range_row_count_ = other.phy_query_range_row_count_;
    query_range_row_count_ = other.query_range_row_count_;
    index_back_row_count_ = other.index_back_row_count_;
    index_back_cost_ = other.index_back_cost_;
    sample_info_ = other.sample_info_;
  }
  return ret;
}

int AccessPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptEstCost::cost_table(
          est_cost_info_, query_range_row_count_, phy_query_range_row_count_, cost_, index_back_cost_))) {
    LOG_WARN("failed to get index access info", K(ret));
  } else {
    op_cost_ = cost_;
  }
  return ret;
}

int ObJoinOrder::estimate_size_and_width(
    const ObJoinOrder* lefttree, const ObJoinOrder* righttree, const PathType path_type, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  const ObOptimizerContext* opt_ctx = NULL;
  common::ObStatManager* stat_manager = NULL;
  double selectivity = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(stat_manager = opt_ctx->get_stat_manager())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(opt_ctx), K(stat_manager), K(ret));
  } else if (ACCESS == path_type) {
    // do nothing
  } else if (JOIN == path_type) {
    if (output_rows_ < 0.0) {
      double new_rows = 0;
      if (OB_ISNULL(lefttree) || OB_ISNULL(righttree)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument of NULL pointer", K(lefttree), K(righttree));
      } else if (OB_FAIL(estimate_join_width(lefttree, righttree, join_type))) {
        LOG_WARN("Failed to estimate join width", K(ret));
      } else if (OB_FAIL(calc_join_output_rows(*lefttree, *righttree, join_type, new_rows))) {
        LOG_WARN("Failed to calc join output rows", K(ret));
      } else {
        set_output_rows(new_rows);
        LOG_TRACE("estimate rows for join path", K(output_rows_), K(get_plan()->get_est_sel_info()));
      }
    }
  } else if (SUBQUERY == path_type) {
    SubQueryPath* ref_path = NULL;
    ObLogicalOperator* child_top = NULL;
    if (get_interesting_paths().empty() ||
        OB_ISNULL(ref_path = static_cast<SubQueryPath*>(get_interesting_paths().at(0))) ||
        OB_ISNULL(child_top = ref_path->root_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(get_interesting_paths()), K(ref_path), K(ret));
    } else if (OB_FAIL(init_est_sel_info_for_subplan_scan(ref_path))) {
      LOG_WARN("failed to init est sel info for subplan scan", K(ret));
    } else if (OB_FAIL(
                   ObOptEstCost::estimate_width_for_table(stmt->get_column_items(), table_id_, avg_output_row_size_))) {
      LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   get_restrict_infos(),
                   selectivity,
                   &get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("Failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
    } else {
      set_output_rows(child_top->get_card() * selectivity);
      LOG_TRACE("estimate rows for subplan path", K(output_rows_), K(get_plan()->get_est_sel_info()));
    }
  } else if (TEMP_TABLE_ACCESS == path_type) {
    const TableItem* table_item = NULL;
    if (OB_FAIL(ObOptEstCost::estimate_width_for_table(stmt->get_column_items(), table_id_, avg_output_row_size_))) {
      LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   get_restrict_infos(),
                   selectivity,
                   &get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
    } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(table_item), K(ret));
    } else {
      ObLogicalOperator* temp_table_plan = NULL;
      if (OB_FAIL(stmt->get_query_ctx()->get_temp_table_plan(table_item->ref_id_, temp_table_plan))) {
        LOG_WARN("failed to get temp table plan from query ctx.", K(ret));
      } else if (OB_ISNULL(temp_table_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        set_output_rows(temp_table_plan->get_card() * selectivity);
        LOG_TRACE("estimate rows for subplan path", K(output_rows_), K(get_plan()->get_est_sel_info()));
      }
    }
  } else if (FAKE_CTE_TABLE_ACCESS == path_type || FUNCTION_TABLE_ACCESS == path_type) {
    // to do: revise this
    avg_output_row_size_ = 199;
    output_rows_ = 199;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown path type", K(path_type), K(ret));
  }
  return ret;
}

int ObJoinOrder::estimate_size_and_width_for_access(PathHelper& helper, ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  const TableItem* table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_)) || OB_UNLIKELY(type_ != ACCESS) ||
      OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null or path type", K(ret), K(get_plan()), K(table_item), K(type_), K(stmt));
  } else if (!helper.is_inner_path_ && OB_FAIL(compute_table_meta_info(table_item->table_id_, table_item->ref_id_))) {
    LOG_WARN("failed to compute table meta info", K(ret));
  } else {
    get_plan()->get_est_sel_info().set_use_origin_stat(helper.is_inner_path_);
    bool no_use_remote_est = !can_use_remote_estimate(helper.table_opt_info_->optimization_method_);
    if (OB_FAIL(estimate_rowcount_for_access_path(
            table_item->table_id_, table_item->ref_id_, access_paths, helper.is_inner_path_, no_use_remote_est))) {
      LOG_WARN("failed to estimate and add access path", K(ret));
    } else {
      get_plan()->get_est_sel_info().set_use_origin_stat(false);
      LOG_TRACE(
          "estimate rows for base table", K(output_rows_), K(get_plan()->get_est_sel_info()), K(avg_output_row_size_));
    }
  }
  return ret;
}

int ObJoinOrder::estimate_join_width(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
    avg_output_row_size_ = left_tree->get_average_output_row_size();
  } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
    avg_output_row_size_ = right_tree->get_average_output_row_size();
  } else {
    avg_output_row_size_ = left_tree->get_average_output_row_size() + right_tree->get_average_output_row_size();
  }
  if (OB_SUCC(ret)) {
    avg_output_row_size_ = std::max(avg_output_row_size_, 4.0);
    LOG_TRACE("estimate_width for join", K(avg_output_row_size_));
  }
  return ret;
}

int ObJoinOrder::compute_const_exprs(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const PathType path_type, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (ACCESS == path_type) {
    if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else { /*do nothing*/
    }
  } else if (JOIN == path_type) {
    if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
    } else if (INNER_JOIN == join_type) {
      if (OB_ISNULL(join_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(join_info_), K(ret));
      } else if (OB_FAIL(append(const_exprs_, left_tree->get_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      } else if (OB_FAIL(append(const_exprs_, right_tree->get_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(join_info_->where_condition_, const_exprs_))) {
        LOG_WARN("failed to compute const exprs", K(ret));
      } else { /*do nothing*/
      }
    } else if (IS_OUTER_JOIN(join_type)) {
      if (OB_ISNULL(join_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(join_info_), K(ret));
      } else if (LEFT_OUTER_JOIN == join_type && OB_FAIL(append(const_exprs_, left_tree->get_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      } else if (RIGHT_OUTER_JOIN == join_type && OB_FAIL(append(const_exprs_, right_tree->get_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(join_info_->where_condition_, const_exprs_))) {
        LOG_WARN("failed to compute const exprs", K(ret));
      }
    } else if (IS_SEMI_ANTI_JOIN(join_type)) {
      if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
        if (OB_FAIL(append(const_exprs_, left_tree->get_const_exprs()))) {
          LOG_WARN("failed to append const exprs", K(ret));
        }
      } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
        if (OB_FAIL(append(const_exprs_, right_tree->get_const_exprs()))) {
          LOG_WARN("failed to append const exprs", K(ret));
        }
      }
    } else if (CONNECT_BY_JOIN == join_type) {
      if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, const_exprs_))) {
        LOG_WARN("failed to compute const exprs", K(ret));
      } else { /*do nothing*/
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid join type", K(join_type), K(ret));
    }
  } else if (SUBQUERY == path_type) {
    SubQueryPath* ref_path = NULL;
    ObLogPlan* log_plan = NULL;
    ObSelectLogPlan* sel_log_plan = NULL;
    ObLogicalOperator* root = NULL;
    ObDMLStmt* parent_stmt = NULL;
    if (OB_UNLIKELY(get_interesting_paths().empty()) ||
        OB_ISNULL(ref_path = static_cast<SubQueryPath*>(get_interesting_paths().at(0))) ||
        OB_ISNULL(root = ref_path->root_) || OB_ISNULL(log_plan = root->get_plan()) || OB_ISNULL(get_plan()) ||
        OB_ISNULL(parent_stmt = get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null",
          K(get_interesting_paths().count()),
          K(ref_path),
          K(log_plan),
          K(root),
          K(parent_stmt),
          K(ret));
    } else if (OB_ISNULL(log_plan->get_stmt()) || OB_UNLIKELY(!log_plan->get_stmt()->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(log_plan->get_stmt()), K(ret));
    } else if (FALSE_IT(sel_log_plan = static_cast<ObSelectLogPlan*>(log_plan))) {
      /*do nothing*/
    } else if (OB_FAIL(
                   ObOptimizerUtil::convert_subplan_scan_expr(sel_log_plan->get_optimizer_context().get_expr_factory(),
                       root->get_ordering_output_equal_sets(),
                       ref_path->subquery_id_,
                       *parent_stmt,
                       *sel_log_plan->get_stmt(),
                       true,
                       root->get_output_const_exprs(),
                       const_exprs_))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else { /*do nothing*/
    }
  } else if (TEMP_TABLE_ACCESS == path_type) {
    ObDMLStmt* parent_stmt = NULL;
    ObSelectLogPlan* temp_table_plan = NULL;
    ObLogicalOperator* temp_table_root = NULL;
    AccessPath* temp_path = NULL;
    if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
        OB_ISNULL(parent_stmt->get_query_ctx()) || OB_UNLIKELY(get_interesting_paths().empty()) ||
        OB_ISNULL(temp_path = static_cast<AccessPath*>(get_interesting_paths().at(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(parent_stmt), K(temp_path), K(ret));
    } else if (OB_FAIL(parent_stmt->get_query_ctx()->get_temp_table_plan(temp_path->ref_table_id_, temp_table_root))) {
      LOG_WARN("failed to get temp table plan", K(ret));
    } else if (OB_ISNULL(temp_table_root) ||
               OB_ISNULL(temp_table_plan = static_cast<ObSelectLogPlan*>(temp_table_root->get_plan())) ||
               OB_ISNULL(temp_table_plan->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(temp_table_root), K(temp_table_plan), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(
                   temp_table_plan->get_optimizer_context().get_expr_factory(),
                   temp_table_root->get_ordering_output_equal_sets(),
                   temp_path->table_id_,
                   *parent_stmt,
                   *temp_table_plan->get_stmt(),
                   true,
                   temp_table_root->get_output_const_exprs(),
                   const_exprs_))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }

  LOG_TRACE("succeed to compute const exprs",
      K(this),
      K(left_tree),
      K(right_tree),
      K(path_type),
      K(join_type),
      K(const_exprs_));
  return ret;
}

int ObJoinOrder::compute_equal_set(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const PathType path_type, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    is_init_ = true;
    EqualSets ordering_in_eset;
    if (ACCESS == path_type) {
      if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_, restrict_info_set_, ordering_output_equal_sets_))) {
        LOG_WARN("failed to compute equal set for access path", K(ret));
      } else { /*do nothing*/
      }
    } else if (JOIN == path_type) {
      if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null join order", K(ret));
      } else if (INNER_JOIN == join_type) {
        if (OB_FAIL(append(ordering_in_eset, left_tree->ordering_output_equal_sets_)) ||
            OB_FAIL(append(ordering_in_eset, right_tree->ordering_output_equal_sets_))) {
          LOG_WARN("failed to init input equal sets", K(ret));
        } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                       allocator_, join_info_->where_condition_, ordering_in_eset, ordering_output_equal_sets_))) {
          LOG_WARN("failed to compute equal sets for inner join", K(ret));
        } else { /*do nothing*/
        }
      } else if (IS_OUTER_JOIN(join_type)) {
        if (LEFT_OUTER_JOIN == join_type || RIGHT_OUTER_JOIN == join_type) {
          const EqualSets& child_eset = (LEFT_OUTER_JOIN == join_type) ? left_tree->ordering_output_equal_sets_
                                                                       : right_tree->ordering_output_equal_sets_;
          if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                  allocator_, join_info_->where_condition_, child_eset, ordering_output_equal_sets_))) {
            LOG_WARN("failed to compute ordering equal set for left/right outer join", K(ret));
          }
        } else if (FULL_OUTER_JOIN == join_type) {
          if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                  allocator_, join_info_->where_condition_, ordering_output_equal_sets_))) {
            LOG_WARN("failed to compute ordering equal set for full outer join", K(ret));
          }
        }
      } else if (IS_SEMI_ANTI_JOIN(join_type)) {
        if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
          if (OB_FAIL(append(ordering_output_equal_sets_, left_tree->ordering_output_equal_sets_))) {
            LOG_WARN("failed to append equal set for left semi/anti join", K(ret));
          }
        } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
          if (OB_FAIL(append(ordering_output_equal_sets_, right_tree->ordering_output_equal_sets_))) {
            LOG_WARN("failed to append equal set for right semi/anti join", K(ret));
          }
        } else {
        }
      } else if (CONNECT_BY_JOIN == join_type) {
        /* do nothing for connect by join */
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid join type", K(join_type), K(ret));
      }
    } else if (SUBQUERY == path_type) {
      SubQueryPath* ref_path = NULL;
      ObLogPlan* log_plan = NULL;
      ObSelectLogPlan* sel_log_plan = NULL;
      ObLogicalOperator* root = NULL;
      ObDMLStmt* parent_stmt = NULL;
      if (OB_UNLIKELY(get_interesting_paths().empty()) ||
          OB_ISNULL(ref_path = static_cast<SubQueryPath*>(get_interesting_paths().at(0))) ||
          OB_ISNULL(root = ref_path->root_) || OB_ISNULL(log_plan = root->get_plan()) || OB_ISNULL(get_plan()) ||
          OB_ISNULL(parent_stmt = get_plan()->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null",
            K(get_interesting_paths().count()),
            K(ref_path),
            K(log_plan),
            K(root),
            K(parent_stmt),
            K(ret));
      } else if (OB_ISNULL(log_plan->get_stmt()) || OB_UNLIKELY(!log_plan->get_stmt()->is_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(log_plan->get_stmt()), K(ret));
      } else if (FALSE_IT(sel_log_plan = static_cast<ObSelectLogPlan*>(log_plan))) {
        /*do nothing*/
      } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_equal_sets(allocator_,
                     sel_log_plan->get_optimizer_context().get_expr_factory(),
                     ref_path->subquery_id_,
                     *parent_stmt,
                     *sel_log_plan->get_stmt(),
                     root->get_ordering_output_equal_sets(),
                     ordering_in_eset))) {
        LOG_WARN("failed to generate subplan scan expr", K(ret));
      } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                     allocator_, restrict_info_set_, ordering_in_eset, ordering_output_equal_sets_))) {
        LOG_WARN("failed to compute equal set for subplan scan", K(ret));
      }
    } else if (TEMP_TABLE_ACCESS == path_type) {
      ObDMLStmt* parent_stmt = NULL;
      ObSelectLogPlan* temp_table_plan = NULL;
      ObLogicalOperator* temp_table_root = NULL;
      AccessPath* temp_path = NULL;
      if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
          OB_ISNULL(parent_stmt->get_query_ctx()) || OB_UNLIKELY(get_interesting_paths().empty()) ||
          OB_ISNULL(temp_path = static_cast<AccessPath*>(get_interesting_paths().at(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(parent_stmt), K(temp_path), K(ret));
      } else if (OB_FAIL(
                     parent_stmt->get_query_ctx()->get_temp_table_plan(temp_path->ref_table_id_, temp_table_root))) {
        LOG_WARN("failed to get temp table plan", K(ret));
      } else if (OB_ISNULL(temp_table_root) ||
                 OB_ISNULL(temp_table_plan = static_cast<ObSelectLogPlan*>(temp_table_root->get_plan())) ||
                 OB_ISNULL(temp_table_plan->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(temp_table_root), K(temp_table_plan), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_equal_sets(allocator_,
                     temp_table_plan->get_optimizer_context().get_expr_factory(),
                     temp_path->table_id_,
                     *parent_stmt,
                     *temp_table_plan->get_stmt(),
                     temp_table_root->get_ordering_output_equal_sets(),
                     ordering_in_eset))) {
        LOG_WARN("failed to generate subplan scan expr", K(ret));
      } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                     allocator_, restrict_info_set_, ordering_in_eset, ordering_output_equal_sets_))) {
        LOG_WARN("failed to compute equal set for subplan scan", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
    LOG_TRACE("compute equal set",
        K(this),
        K(left_tree),
        K(right_tree),
        K(path_type),
        K(join_type),
        K(ordering_output_equal_sets_));
  } else { /*do nothing*/
  }
  return ret;
}

int ObJoinOrder::convert_subplan_scan_order_item(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, const uint64_t table_id, ObDMLStmt& parent_stmt,
    const ObSelectStmt& child_stmt, const ObIArray<OrderItem>& input_order, ObIArray<OrderItem>& output_order)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_order.count(); i++) {
    ObRawExpr* temp_expr = NULL;
    if (OB_ISNULL(input_order.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(input_order.at(i).expr_), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(
                   expr_factory, equal_sets, table_id, parent_stmt, child_stmt, input_order.at(i).expr_, temp_expr))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (NULL == temp_expr) {
      bool is_const = false;
      if (OB_FAIL(ObOptimizerUtil::is_const_expr(input_order.at(i).expr_, equal_sets, const_exprs, is_const))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (is_const) {
        /*do nothing*/
      } else {
        break;
      }
    } else if (OB_FAIL(output_order.push_back(OrderItem(temp_expr, input_order.at(i).order_type_)))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("subplan scan order item", K(output_order));
  }
  return ret;
}

bool sql::Path::is_valid() const
{
  bool is_valid = true;
  if (ACCESS == path_type_ && static_cast<const AccessPath*>(this)->is_hash_index_ &&
      !static_cast<const AccessPath*>(this)->is_get_) {
    is_valid = false;
    LOG_WARN("OPT:[ADD PATH]", K(is_valid));
  }
  return is_valid;
}

int ObJoinOrder::add_path(sql::Path* path)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(path), K(get_plan()), K(stmt), K(ret));
  } else {
    bool should_add = true;
    CompareRelation plan_rel = CompareRelation::UNCOMPARABLE;
    for (int64_t i = interesting_paths_.count() - 1; OB_SUCC(ret) && should_add && i >= 0; --i) {
      sql::Path* cur_path = interesting_paths_.at(i);
      if (OB_ISNULL(cur_path)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(cur_path), K(ret));
      } else if (OB_FAIL(compute_path_relationship(cur_path,
                     path,
                     ordering_output_equal_sets_,
                     get_const_exprs(),
                     stmt->has_limit(),
                     stmt->get_order_items(),
                     plan_rel))) {
        LOG_WARN("failed to compute plan relationship", K(*cur_path), K(*path), K(ret));
      } else if (LEFT_DOMINATE == plan_rel || EQUAL == plan_rel) {
        should_add = false;
        LOG_TRACE("current path has been dominated, no need to add", K(*path), K(*cur_path), K(ret));
      } else if (RIGHT_DOMINATE == plan_rel) {
        if (OB_FAIL(interesting_paths_.remove(i))) {
          LOG_WARN("failed to remove dominated plans", K(i), K(ret));
        } else {
          LOG_TRACE("current path dominated interesting path", K(*path), K(*cur_path), K(ret));
        }
      } else { /* do nothing */
      }
    }
    if (OB_SUCC(ret) && should_add) {
      if (OB_FAIL(interesting_paths_.push_back(path))) {
        LOG_WARN("failed to add plan", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

/*
 * One path dominates another path if
 * 1 it has lower cost than another path
 * 2 its interesting order is a superset of another path
 */
int ObJoinOrder::compute_path_relationship(const sql::Path* first_path, const sql::Path* second_path,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, const bool has_limit,
    const ObIArray<OrderItem>& order_items, CompareRelation& relation)
{
  int ret = OB_SUCCESS;
  relation = CompareRelation::UNCOMPARABLE;
  if (OB_ISNULL(first_path) || OB_ISNULL(second_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(first_path), K(second_path), K(ret));
  } else if (PathType::JOIN != first_path->path_type_ && PathType::JOIN != second_path->path_type_ && has_limit) {
    /* do nothing */
  } else {
    int64_t left_dominated_count = 0;
    int64_t right_dominated_count = 0;
    int64_t uncompareable_count = 0;
    // compare cost
    if (fabs(first_path->cost_ - second_path->cost_) < OB_DOUBLE_EPSINON) {
      // do nothing
    } else if (first_path->cost_ < second_path->cost_) {
      left_dominated_count++;
    } else {
      right_dominated_count++;
    }
    // compare interesting order
    bool first_has_interesting_order = first_path->has_interesting_order();
    bool second_has_interesting_order = second_path->has_interesting_order();
    if (!first_has_interesting_order && !second_has_interesting_order) {
      // do nothing
    } else if (first_has_interesting_order && !second_has_interesting_order) {
      left_dominated_count++;
    } else if (!first_has_interesting_order && second_has_interesting_order) {
      right_dominated_count++;
    } else {
      bool left_prefix = false;
      bool right_prefix = false;
      if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
              first_path->ordering_, second_path->ordering_, equal_sets, const_exprs, left_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
            K(first_path->ordering_),
            K(second_path->ordering_),
            K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
                     second_path->ordering_, first_path->ordering_, equal_sets, const_exprs, right_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
            K(first_path->ordering_),
            K(second_path->ordering_),
            K(ret));
      } else if (left_prefix && right_prefix) {
        // do nothing
      } else if (left_prefix) {
        right_dominated_count++;
      } else if (right_prefix) {
        left_dominated_count++;
      } else {
        uncompareable_count++;
      }
    }

    if (OB_SUCC(ret) && PathType::JOIN == first_path->path_type_ && PathType::JOIN == second_path->path_type_ &&
        has_limit) {
      const JoinPath* first_join_path = static_cast<const JoinPath*>(first_path);
      const JoinPath* second_join_path = static_cast<const JoinPath*>(second_path);
      bool first_left_order = first_join_path->left_need_sort_;
      bool first_right_order = first_join_path->right_need_sort_;
      bool second_left_order = second_join_path->left_need_sort_;
      bool second_right_order = second_join_path->right_need_sort_;
      bool check_parallel = true;
      if (!order_items.empty()) {
        bool left_prefix = false;
        bool right_prefix = false;
        check_parallel = false;
        if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
                order_items, first_path->ordering_, equal_sets, const_exprs, left_prefix))) {
          LOG_WARN("failed to compute prefix ordering relationship", K(order_items), K(first_path->ordering_), K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
                       order_items, second_path->ordering_, equal_sets, const_exprs, right_prefix))) {
          LOG_WARN("failed to compute prefix ordering relationship", K(order_items), K(second_path->ordering_), K(ret));
        } else if (!left_prefix && !right_prefix) {
        } else if (left_prefix && !right_prefix) {
          left_dominated_count++;
        } else if (!left_prefix && right_prefix) {
          right_dominated_count++;
        } else {
          check_parallel = true;
        }
      }
      if (OB_SUCC(ret) && check_parallel) {
        if (first_join_path->join_algo_ == second_join_path->join_algo_ && first_left_order == second_left_order &&
            first_right_order == second_right_order && first_join_path->need_mat_ == second_join_path->need_mat_) {
          /*do nothing*/
        } else if (MERGE_JOIN == first_join_path->join_algo_ && first_left_order && first_right_order) {
          right_dominated_count++;
        } else if (MERGE_JOIN == second_join_path->join_algo_ && second_left_order && second_right_order) {
          left_dominated_count++;
        } else {
          uncompareable_count++;
        }
      }
    }

    // compute final result
    if (OB_SUCC(ret)) {
      if (left_dominated_count > 0 && right_dominated_count == 0 && uncompareable_count == 0) {
        relation = LEFT_DOMINATE;
      } else if (right_dominated_count > 0 && left_dominated_count == 0 && uncompareable_count == 0) {
        relation = RIGHT_DOMINATE;
      } else if (left_dominated_count == 0 && right_dominated_count == 0 && uncompareable_count == 0) {
        relation = EQUAL;
      } else {
        relation = UNCOMPARABLE;
      }
    }
  }
  return ret;
}

int JoinPath::re_est_cost(sql::Path* path, double need_row_count, double& cost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null path", K(ret));
  } else if (ACCESS == path->path_type_) {
    ret = re_est_access_cost(static_cast<AccessPath*>(path), need_row_count, cost);
  } else if (SUBQUERY == path->path_type_) {
    ret = re_est_subquery_cost(static_cast<SubQueryPath*>(path), need_row_count, cost);
  } else {
    cost = path->cost_;
  }
  return ret;
}

int JoinPath::re_est_access_cost(AccessPath* path, double need_row_count, double& cost)
{
  int ret = OB_SUCCESS;
  bool need_all = false;
  double need_count = 0.0;
  if (OB_UNLIKELY(need_row_count < 0.0) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Need row count is less than 0", K(ret), K(need_row_count));
  } else if (need_row_count >= path->get_output_row_count()) {
    need_all = true;
  } else if (OB_UNLIKELY(path->est_cost_info_.table_filter_sel_ <= 0.0)) {
    need_all = true;
  } else if (is_virtual_table(path->ref_table_id_)) {
    need_count = static_cast<double>(need_row_count) / path->est_cost_info_.table_filter_sel_;
  } else {
    need_count = static_cast<double>(need_row_count) / path->est_cost_info_.table_filter_sel_;
    if (OB_UNLIKELY(path->est_cost_info_.postfix_filter_sel_ <= 0.0)) {
      need_all = true;
    } else {
      need_count = static_cast<double>(need_count) / path->est_cost_info_.postfix_filter_sel_;
    }
  }
  if (OB_SUCC(ret)) {
    if (path->sample_info_.is_row_sample() && path->sample_info_.percent_ > 0.0) {
      need_count = static_cast<double>(need_count) / path->sample_info_.percent_;
    }
    if (need_count < 0.0) {  // too big, overflow
      need_all = true;
    }
    if (!need_all && path->query_range_row_count_ >= OB_DOUBLE_EPSINON) {
      double index_back_cost = 0;
      double physical_need_count = need_count * path->phy_query_range_row_count_ / path->query_range_row_count_;
      if (is_virtual_table(path->ref_table_id_)) {
        cost = VIRTUAL_INDEX_GET_COST * need_count;
      } else if (OB_FAIL(ObOptEstCost::cost_table_one_batch(path->est_cost_info_,
                     path->est_cost_info_.batch_type_,
                     path->is_global_index_ ? false : path->est_cost_info_.index_meta_info_.is_index_back_,
                     path->est_cost_info_.table_scan_param_.column_ids_.count(),
                     need_count,
                     physical_need_count,
                     cost,
                     index_back_cost))) {
        LOG_WARN("failed to estimate cost", K(ret));
      }
    } else {
      cost = path->cost_;
    }
  }
  return ret;
}

int JoinPath::re_est_subquery_cost(SubQueryPath* path, double need_row_count, double& cost)
{
  int ret = OB_SUCCESS;
  bool re_est = false;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null path", K(ret));
  } else if (OB_ISNULL(path->root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (OB_FAIL(const_cast<ObLogicalOperator*>(path->root_)->re_est_cost(NULL, need_row_count, re_est))) {
    LOG_WARN("failed to re est cost for subquery", K(ret));
  } else {
    cost = path->root_->get_cost();
  }
  return ret;
}

int JoinPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  double op_cost = 0.0;
  double cost = 0.0;
  if (NESTED_LOOP_JOIN == join_algo_) {
    if (OB_FAIL(cost_nest_loop_join(op_cost, cost))) {
      LOG_WARN("failed to cost nest loop join", K(*this), K(ret));
    }
  } else if (MERGE_JOIN == join_algo_) {
    if (OB_FAIL(cost_merge_join(op_cost, cost))) {
      LOG_WARN("failed to cost merge join", K(*this), K(ret));
    }
  } else if (HASH_JOIN == join_algo_) {
    if ((OB_FAIL(cost_hash_join(op_cost, cost)))) {
      LOG_WARN("failed to cost hash join", K(*this), K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown join algorithm", K(join_algo_));
  }
  if (OB_SUCC(ret)) {
    op_cost_ = op_cost;
    cost_ = cost;
  }
  return ret;
}

int JoinPath::cost_nest_loop_join(double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  op_cost = 0;
  cost = 0;
  ObLogPlan* plan = NULL;
  ObJoinOrder* left_join_order = NULL;
  ObJoinOrder* right_join_order = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(right_path_) ||
      OB_ISNULL(right_join_order = right_path_->parent_) || OB_ISNULL(left_path_) ||
      OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(ret),
        K(parent_),
        K(left_path_),
        K(left_join_order),
        K(right_path_),
        K(right_join_order));
  } else {
    bool with_nl_param = right_path_->is_inner_path();
    double right_rows = right_join_order->get_output_rows();
    double right_cost = right_path_->cost_;
    if (right_path_->is_inner_path_) {
      right_rows = right_path_->inner_row_count_;
    }
    if (with_nl_param && (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_)) {
      right_rows = 1;
      if (OB_FAIL(re_est_cost(const_cast<sql::Path*>(right_path_), right_rows, right_cost))) {
        LOG_WARN("failed to re est cost", K(ret));
      }
    }
    ObCostNLJoinInfo est_cost_info(left_join_order->get_output_rows(),
        left_path_->cost_,
        left_join_order->get_average_output_row_size(),
        right_rows,
        right_cost,
        right_join_order->get_average_output_row_size(),
        left_join_order->get_tables(),
        right_join_order->get_tables(),
        join_type_,
        parent_->get_anti_or_semi_match_sel(),
        with_nl_param,
        need_mat_,
        equal_join_condition_,
        other_join_condition_,
        &plan->get_est_sel_info());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   ObOptEstCost::cost_nestloop(est_cost_info, op_cost, cost, &plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to estimate nest loop join cost", K(est_cost_info), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int JoinPath::cost_merge_join(double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  op_cost = 0;
  cost = 0;
  ObLogPlan* plan = NULL;
  ObJoinOrder* left_join_order = NULL;
  ObJoinOrder* right_join_order = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(right_path_) ||
      OB_ISNULL(right_join_order = right_path_->parent_) || OB_ISNULL(left_path_) ||
      OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(ret),
        K(parent_),
        K(left_path_),
        K(left_join_order),
        K(right_path_),
        K(right_join_order));
  } else {
    double right_rows = right_join_order->get_output_rows();
    ObArray<OrderItem> empty_array;  // will not insert data
    ObIArray<OrderItem>* left_expected_ordering;
    ObIArray<OrderItem>* right_expected_ordering;
    if (left_need_sort_) {
      left_expected_ordering = &left_expected_ordering_;
    } else {
      left_expected_ordering = &empty_array;
    }

    if (right_need_sort_) {
      right_expected_ordering = &right_expected_ordering_;
    } else {
      right_expected_ordering = &empty_array;
    }

    ObCostMergeJoinInfo est_cost_info(left_join_order->get_output_rows(),
        left_path_->cost_,
        left_join_order->get_average_output_row_size(),
        right_rows,
        right_path_->cost_,
        right_join_order->get_average_output_row_size(),
        left_join_order->get_tables(),
        right_join_order->get_tables(),
        join_type_,
        equal_join_condition_,
        other_join_condition_,
        *left_expected_ordering,
        *right_expected_ordering,
        &plan->get_est_sel_info());
    if (OB_FAIL(ObOptEstCost::cost_mergejoin(est_cost_info, op_cost, cost, &plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to estimate merge join cost", K(est_cost_info), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int JoinPath::cost_hash_join(double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  op_cost = 0;
  cost = 0;
  ObLogPlan* plan = NULL;
  ObJoinOrder* left_join_order = NULL;
  ObJoinOrder* right_join_order = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(right_path_) ||
      OB_ISNULL(right_join_order = right_path_->parent_) || OB_ISNULL(left_path_) ||
      OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(ret),
        K(parent_),
        K(left_path_),
        K(left_join_order),
        K(right_path_),
        K(right_join_order));
  } else {
    double right_rows = right_join_order->get_output_rows();
    ObCostHashJoinInfo est_cost_info(left_join_order->get_output_rows(),
        left_path_->cost_,
        left_join_order->get_average_output_row_size(),
        right_rows,
        right_path_->cost_,
        right_join_order->get_average_output_row_size(),
        left_join_order->get_tables(),
        right_join_order->get_tables(),
        join_type_,
        equal_join_condition_,
        other_join_condition_,
        &plan->get_est_sel_info());
    if (OB_FAIL(ObOptEstCost::cost_hashjoin(est_cost_info, op_cost, cost, &plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to estimate hash join cost", K(est_cost_info), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int SubQueryPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(root_), K(ret));
  } else {
    op_cost_ = ObOptEstCost::cost_subplan_scan(root_->get_card(), filter_.count());
    cost_ = root_->get_cost() + op_cost_;
  }
  return ret;
}

int ObJoinOrder::init_base_join_order(const TableItem* table_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  int32_t table_bit_index = OB_INVALID_INDEX;
  if (OB_ISNULL(table_item) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(table_item), K(get_plan()), K(stmt), K(ret));
  } else if (table_item->is_joined_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect joined table item", K(ret));
  } else {
    table_id_ = table_item->table_id_;
    table_meta_info_.ref_table_id_ = table_item->ref_id_;
    table_bit_index = stmt->get_table_bit_index(table_item->table_id_);
    if (OB_FAIL(get_tables().add_member(table_bit_index)) || OB_FAIL(get_output_tables().add_member(table_bit_index))) {
      LOG_WARN("failed to add member",
          K(get_tables()),
          K(table_item->table_id_),
          K(stmt->get_table_bit_index(table_item->table_id_)),
          K(ret));
    } else if (table_item->is_basic_table()) {
      set_type(ACCESS);
    } else if (table_item->is_temp_table()) {
      set_type(TEMP_TABLE_ACCESS);
    } else if (table_item->is_generated_table()) {
      set_type(SUBQUERY);
    } else if (table_item->is_fake_cte_table()) {
      set_type(FAKE_CTE_TABLE_ACCESS);
    } else if (table_item->is_function_table()) {
      set_type(FUNCTION_TABLE_ACCESS);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type of table item", K(table_item->type_), K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::generate_base_paths()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(stmt->get_query_ctx()) ||
      OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(allocator_), K(ret));
  } else {
    ObOptimizerContext& optimizer_context = get_plan()->get_optimizer_context();
    const TableItem* table_item = NULL;
    switch (get_type()) {
      case FAKE_CTE_TABLE_ACCESS: {
        if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(estimate_size_and_width(NULL, NULL, type_, UNKNOWN_JOIN))) {
          LOG_WARN("failed to estimate_size", K(ret));
        } else {
          uint64_t table_id = table_item->table_id_;
          uint64_t ref_id = table_item->ref_id_;
          if (OB_FAIL(add_cte_table(table_id, ref_id))) {
            LOG_WARN("failed to add table to join order(single)", K(table_id), K(ret));
          } else {
            LOG_TRACE("table is added to the join order", K(table_id), K(ref_id));
          }
        }
        break;
      }
      case FUNCTION_TABLE_ACCESS: {
        if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get table item by id", K(ret), K(table_id_));
        } else if (OB_FAIL(estimate_size_and_width(NULL, NULL, type_, UNKNOWN_JOIN))) {
          LOG_WARN("failed to estimate_size", K(ret));
        } else {
          uint64_t table_id = table_item->table_id_;
          uint64_t ref_id = table_item->ref_id_;
          if (OB_FAIL(add_function_table(table_id, ref_id))) {
            LOG_WARN("failed to add t able to join order(single)", K(table_id), K(ret));
          } else {
            LOG_TRACE("table if add to join order", K(table_id), K(ref_id));
          }
        }
        break;
      }
      case TEMP_TABLE_ACCESS: {
        AccessPath* temp_table_path = NULL;
        ObSelectLogPlan* temp_table_plan = NULL;
        ObLogicalOperator* temp_table_root = NULL;
        if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(table_id_), K(table_item));
        } else if (OB_FAIL(estimate_size_and_width(NULL, NULL, type_, UNKNOWN_JOIN))) {
          LOG_WARN("failed to estimate_size", K(ret));
        } else if (OB_FAIL(stmt->get_query_ctx()->get_temp_table_plan(table_item->ref_id_, temp_table_root))) {
          LOG_WARN("failed to get temp table plan", K(ret));
        } else if (OB_ISNULL(temp_table_root) ||
                   OB_ISNULL(temp_table_plan = static_cast<ObSelectLogPlan*>(temp_table_root->get_plan())) ||
                   OB_ISNULL(temp_table_plan->get_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(temp_table_root), K(temp_table_plan), K(ret));
        } else if (OB_ISNULL(temp_table_path = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate an AccessPath", K(ret));
        } else {
          int64_t dummy_prefix_count = 0;
          int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
          temp_table_path = new (temp_table_path)
              AccessPath(table_id_, table_item->ref_id_, table_item->ref_id_, this, default_asc_direction());
          temp_table_path->set_temp_table_path(true);
          temp_table_path->table_opt_info_ = &table_opt_info_;
          temp_table_path->table_row_count_ = output_rows_;
          temp_table_path->output_row_count_ = output_rows_;
          temp_table_path->op_cost_ = ObOptEstCost::cost_temp_table(
              temp_table_root->get_card(), temp_table_root->get_width(), get_restrict_infos());
          ;
          temp_table_path->cost_ = temp_table_path->op_cost_;
          if (OB_FAIL(convert_subplan_scan_order_item(temp_table_plan->get_optimizer_context().get_expr_factory(),
                  temp_table_root->get_ordering_output_equal_sets(),
                  temp_table_root->get_output_const_exprs(),
                  table_id_,
                  *stmt,
                  *temp_table_plan->get_stmt(),
                  temp_table_root->get_op_ordering(),
                  temp_table_path->get_ordering()))) {
            LOG_WARN("failed to convert subplan scan order item", K(ret));
          } else if (OB_FAIL(check_all_interesting_order(
                         temp_table_path->get_ordering(), stmt, dummy_prefix_count, interesting_order_info))) {
            LOG_WARN("failed to check all interesting order", K(ret));
          } else if (FALSE_IT(temp_table_path->set_interesting_order_info(interesting_order_info))) {
          } else if (OB_FAIL(append(temp_table_path->filter_, get_restrict_infos()))) {
            LOG_WARN("failed to push back restrict infos.", K(ret));
          } else if (OB_FAIL(add_path(temp_table_path))) {
            LOG_WARN("failed to add path", K(ret));
          } else if (OB_FAIL(compute_const_exprs(NULL, NULL, type_, UNKNOWN_JOIN))) {
            LOG_WARN("failed to compute const exprs", K(ret));
          } else if (OB_FAIL(compute_equal_set(NULL, NULL, type_, UNKNOWN_JOIN))) {
            LOG_WARN("failed to compute equal set condition", K(ret));
          } else if (OB_FAIL(compute_fd_item_set_for_subplan_scan(
                         temp_table_plan->get_optimizer_context().get_expr_factory(),
                         temp_table_root->get_ordering_output_equal_sets(),
                         table_id_,
                         *stmt,
                         *temp_table_plan->get_stmt(),
                         temp_table_root->get_fd_item_set(),
                         fd_item_set_))) {
            LOG_WARN("failed to compute fd item set for subplan scan", K(ret));
          } else { /*do nothing*/
          }
        }
        break;
      }
      case ACCESS: {
        if (OB_FAIL(generate_normal_access_path())) {
          LOG_WARN("failed to generate normal access path", K(ret));
        }
        break;
      }
      case SUBQUERY: {
        if (OB_FAIL(generate_normal_subquery_path())) {
          LOG_WARN("failed to generate normal subquery path", K(ret));
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObJoinOrder::generate_normal_access_path()
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  helper.is_inner_path_ = false;
  helper.table_opt_info_ = &table_opt_info_;
  if (OB_FAIL(helper.filters_.assign(restrict_info_set_))) {
    LOG_WARN("failed to assign restrict info set", K(ret));
  } else if (OB_FAIL(generate_access_paths(helper))) {
    LOG_WARN("failed to generate access paths", K(ret));
  }
  return ret;
}

int ObJoinOrder::generate_access_paths(PathHelper& helper)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  const TableItem* table_item = NULL;
  ObSEArray<AccessPath*, 8> access_paths;
  ObSEArray<ObTablePartitionInfo*, 8> tbl_part_infos;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(stmt));
  } else if (!helper.is_inner_path_ && OB_FAIL(ObOptEstCost::estimate_width_for_table(
                                           stmt->get_column_items(), table_id_, avg_output_row_size_))) {
    // bad design resulted from the fact that add_table changes column items array
    LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
  } else if (OB_FAIL(add_table(table_id_, table_item->ref_id_, helper, access_paths))) {
    LOG_WARN("failed to add table to join order(single)", K(ret));
  } else if (OB_FAIL(compute_table_location_for_paths(helper.is_inner_path_, access_paths, tbl_part_infos))) {
    LOG_WARN("failed to calc table location", K(ret));
  } else if (OB_FAIL(estimate_size_and_width_for_access(helper, access_paths))) {
    LOG_WARN("failed to estimate_size", K(ret));
  } else if (!helper.is_inner_path_) {
    if (OB_FAIL(compute_const_exprs(NULL, NULL, type_, UNKNOWN_JOIN))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else if (OB_FAIL(compute_equal_set(NULL, NULL, type_, UNKNOWN_JOIN))) {
      LOG_WARN("failed to compute equal set", K(ret));
    } else if (OB_FAIL(compute_fd_item_set_for_table_scan(
                   table_item->table_id_, table_item->ref_id_, get_restrict_infos()))) {
      LOG_WARN("failed to extract fd item set", K(ret));
    } else if (!is_virtual_table(table_item->ref_id_) && OB_FAIL(compute_one_row_info_for_table_scan(access_paths))) {
      LOG_WARN("failed to compute one row info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_plan()->select_location(tbl_part_infos))) {
      LOG_WARN("failed to select location", K(ret));
    } else if (OB_FAIL(compute_sharding_info_for_paths(helper.is_inner_path_, access_paths))) {
      LOG_WARN("failed to calc sharding info", K(ret));
    } else if (OB_FAIL(compute_cost_and_prune_access_path(helper, access_paths))) {
      LOG_WARN("failed to compute cost and prune access path", K(ret));
    } else if (OB_FAIL(revise_output_rows_after_creating_path(helper, access_paths))) {
      LOG_WARN("failed to revise output rows after creating path", K(ret));
    } else if (OB_FAIL(append(available_access_paths_, access_paths))) {
      LOG_WARN("failed to append access paths", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::deep_copy_subquery(ObSelectStmt*& stmt)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* parent_stmt = NULL;
  ObSelectStmt* child_stmt = NULL;
  const TableItem* table_item = NULL;
  ObSelectStmt* copy_stmt = NULL;
  ObExecContext* exec_ctx = NULL;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(exec_ctx = get_plan()->get_optimizer_context().get_exec_ctx()) ||
      OB_ISNULL(stmt_factory = exec_ctx->get_stmt_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "get unexpected null", K(get_plan()), K(parent_stmt), K(expr_factory), K(exec_ctx), K(stmt_factory), K(ret));
  } else if (OB_ISNULL(table_item = parent_stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_ISNULL(child_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->deep_copy(*stmt_factory, *expr_factory, *child_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(stmt->adjust_subquery_stmt_parent(parent_stmt, NULL))) {
    LOG_WARN("failed to adjust subquery stmt parent", K(ret));
  } else if (OB_FAIL(stmt->reset_statement_id(*child_stmt))) {
    LOG_WARN("failed to reset statement id", K(ret));
  }
  return ret;
}

int ObJoinOrder::generate_subquery_path(PathHelper& helper)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* sub_query = NULL;
  ObLogPlan* log_plan = NULL;
  ObSelectLogPlan* select_log_plan = NULL;
  ObSelectStmt* child_stmt = NULL;
  ObDMLStmt* parent_stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(ret));
  } else if (OB_ISNULL(sub_query = static_cast<ObDMLStmt*>(helper.child_stmt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is NULL", K(sub_query), K(ret));
  } else if (OB_UNLIKELY(NULL == (log_plan = get_plan()->get_optimizer_context().get_log_plan_factory().create(
                                      get_plan()->get_optimizer_context(), *sub_query)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create plan", "stmt", sub_query->get_sql_stmt(), K(ret));
  } else if (OB_FAIL(log_plan->add_pushdown_filters(helper.pushdown_filters_))) {
    LOG_WARN("failed to add pushdown filters", K(ret));
  } else if (OB_FAIL(log_plan->init_plan_info())) {
    LOG_WARN("failed to init equal sets", K(ret));
  } else if (FALSE_IT(select_log_plan = static_cast<ObSelectLogPlan*>(log_plan))) {
    /*do nothing*/
  } else if (FALSE_IT(log_plan->set_is_subplan_scan(true))) {
    // never reach
  } else if (OB_FAIL(log_plan->add_startup_filters(get_plan()->get_startup_filters()))) {
    LOG_WARN("failed to add startup filters", K(ret));
  } else if (OB_FAIL(select_log_plan->generate_raw_plan())) {
    LOG_WARN("failed to optimize sub-select", K(ret));
  } else if (OB_ISNULL(select_log_plan->get_plan_root()) || OB_ISNULL(child_stmt = select_log_plan->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(log_plan->get_plan_root()), K(child_stmt), K(ret));
  } else {
    ObRawExprFactory& expr_factory = select_log_plan->get_optimizer_context().get_expr_factory();
    const EqualSets& ordering_equal_sets = select_log_plan->get_plan_root()->get_ordering_output_equal_sets();
    ObIArray<ObLogPlan::CandidatePlan>& candidate_plans = log_plan->get_candidate_plans().candidate_plans_;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
      SubQueryPath* sub_path = NULL;
      if (OB_ISNULL(candidate_plans.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_ISNULL(sub_path = static_cast<SubQueryPath*>(allocator_->alloc(sizeof(SubQueryPath))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate subquery path", K(ret));
      } else {
        sub_path = new (sub_path) SubQueryPath(candidate_plans.at(i).plan_tree_);
        sub_path->subquery_id_ = table_id_;
        sub_path->parent_ = this;
        int64_t dummy_prefix_count = 0;
        int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
        if (OB_FAIL(append(sub_path->filter_, helper.filters_))) {
          LOG_WARN("failed to append restrict info to sub_path", K(ret));
        } else if (OB_FAIL(convert_subplan_scan_order_item(expr_factory,
                       ordering_equal_sets,
                       candidate_plans.at(i).plan_tree_->get_output_const_exprs(),
                       table_id_,
                       *parent_stmt,
                       *child_stmt,
                       candidate_plans.at(i).plan_tree_->get_op_ordering(),
                       sub_path->get_ordering()))) {
          LOG_WARN("failed to convert subplan scan order item", K(ret));
        } else if (OB_FAIL(check_all_interesting_order(
                       sub_path->get_ordering(), parent_stmt, dummy_prefix_count, interesting_order_info))) {
          LOG_WARN("failed to check all interesting order", K(ret));
        } else if (FALSE_IT(sub_path->set_interesting_order_info(interesting_order_info))) {
        } else if (OB_FAIL(sub_path->estimate_cost())) {
          LOG_WARN("failed to calculate cost of subquery path", K(ret));
        } else if (helper.is_inner_path_) {
          if (OB_FAIL(estimate_size_for_inner_subquery_path(sub_path, helper.filters_))) {
            LOG_WARN("failed to estimate size for inner subquery path", K(ret));
          } else if (OB_FAIL(helper.inner_paths_.push_back(sub_path))) {
            LOG_WARN("failed to push back inner path", K(ret));
          } else {
            LOG_TRACE("succeed to generate inner subquery path", K(table_id_), K(sub_path->get_ordering()));
          }
        } else if (OB_FAIL(add_path(sub_path))) {
          LOG_WARN("failed to add path", K(ret));
        } else {
          LOG_TRACE("succeed to generate normal subquery path", K(table_id_), K(sub_path->get_ordering()));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!helper.is_inner_path_) {
      if (OB_FAIL(estimate_size_and_width(NULL, NULL, type_, UNKNOWN_JOIN))) {
        LOG_WARN("failed to estimate_size", K(ret));
      } else if (OB_FAIL(compute_const_exprs(NULL, NULL, type_, UNKNOWN_JOIN))) {
        LOG_WARN("failed to compute const exprs", K(ret));
      } else if (OB_FAIL(compute_equal_set(NULL, NULL, type_, UNKNOWN_JOIN))) {
        LOG_WARN("failed to compute equal set condition", K(ret));
      } else if (OB_FAIL(compute_fd_item_set_for_subplan_scan(expr_factory,
                     ordering_equal_sets,
                     table_id_,
                     *parent_stmt,
                     *child_stmt,
                     select_log_plan->get_plan_root()->get_fd_item_set(),
                     fd_item_set_))) {
        LOG_WARN("failed to compute fd item set for subplan scan", K(ret));
      } else {
        is_at_most_one_row_ = select_log_plan->get_plan_root()->get_is_at_most_one_row();
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_normal_subquery_path()
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  bool can_pushdown = false;
  ObDMLStmt* parent_stmt = NULL;
  const TableItem* table_item = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSEArray<ObRawExpr*, 4> candi_pushdown_quals;
  ObSelectStmt* subquery = NULL;
  helper.is_inner_path_ = false;
  LOG_TRACE("start to generate normal subquery path", K(table_id_));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(table_item = parent_stmt->get_table_item_by_id(table_id_)) ||
      OB_ISNULL(subquery = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(table_item), K(subquery), K(ret));
  } else if (OB_FAIL(deep_copy_subquery(subquery))) {
    LOG_WARN("faield to deep copy subquery", K(ret));
  } else if (OB_ISNULL(helper.child_stmt_ = subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::pushdown_filter_into_subquery(*parent_stmt,
                 *helper.child_stmt_,
                 get_plan()->get_optimizer_context(),
                 get_restrict_infos(),
                 candi_pushdown_quals,
                 helper.filters_,
                 can_pushdown))) {
    LOG_WARN("failed to pushdown filter into subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(*parent_stmt,
                 *helper.child_stmt_,
                 table_id_,
                 session_info,
                 *expr_factory,
                 candi_pushdown_quals,
                 helper.pushdown_filters_))) {
    LOG_WARN("failed to rename pushdown filter", K(ret));
  } else if (OB_FAIL(generate_subquery_path(helper))) {
    LOG_WARN("failed to generate subquery path", K(ret));
  }
  LOG_TRACE("succed to generate normal subquery path", K(table_id_), K(interesting_paths_));
  return ret;
}

int ObJoinOrder::get_range_params(ObLogicalOperator* root, ObIArray<ObRawExpr*>& range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    ObLogTableScan* scan = static_cast<ObLogTableScan*>(root);
    const ObCostTableScanInfo* info = scan->get_est_cost_info();
    if (NULL != info && info->pushdown_prefix_filters_.count() > 0) {
      if (OB_FAIL(append(range_exprs, info->pushdown_prefix_filters_))) {
        LOG_WARN("failed to append range exprs", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator* child = root->get_child(i);
      if (OB_FAIL(SMART_CALL(get_range_params(child, range_exprs)))) {
        LOG_WARN("failed to get range params", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::estimate_size_for_inner_subquery_path(SubQueryPath* path, const ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  double selectivity = 0.0;
  ObLogicalOperator* child_top = NULL;
  get_plan()->get_est_sel_info().set_use_origin_stat(true);
  if (OB_ISNULL(path) || OB_ISNULL(child_top = path->root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument", K(path), K(child_top), K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(
                 get_plan()->get_est_sel_info(), filters, selectivity, &get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("Failed to calc filter selectivities", K(filters), K(ret));
  } else {
    path->inner_row_count_ = child_top->get_card() * selectivity;
    get_plan()->get_est_sel_info().set_use_origin_stat(false);
    LOG_TRACE("estimate rows for inner subplan path", K(path->inner_row_count_), K(get_plan()->get_est_sel_info()));
  }
  return ret;
}

int ObJoinOrder::init_join_order(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(left_tree), K(right_tree), K(allocator_), K(ret));
  } else {
    if (OB_FAIL(get_tables().add_members(left_tree->get_tables()))) {
      LOG_WARN("fail to add left tree's tables", K(ret));
    } else if (OB_FAIL(get_tables().add_members(right_tree->get_tables()))) {
      LOG_WARN("fail to add left tree's tables", K(ret));
    } else if (IS_SEMI_ANTI_JOIN(join_info->join_type_)) {
      if (IS_LEFT_SEMI_ANTI_JOIN(join_info->join_type_)) {
        if (OB_FAIL(get_output_tables().add_members(left_tree->get_output_tables()))) {
          LOG_WARN("fail to add left tree's output tables", K(ret));
        }
      } else if (OB_FAIL(get_output_tables().add_members(right_tree->get_output_tables()))) {
        LOG_WARN("fail to add left tree's output tables", K(ret));
      }
    } else if (OB_FAIL(get_output_tables().add_members(left_tree->get_output_tables()))) {
      LOG_WARN("fail to add left tree's output tables", K(ret));
    } else if (OB_FAIL(get_output_tables().add_members(right_tree->get_output_tables()))) {
      LOG_WARN("fail to add left tree's output tables", K(ret));
    }
    if (OB_SUCC(ret)) {
      JoinInfo* temp_join_info = NULL;
      if (OB_ISNULL(temp_join_info = static_cast<JoinInfo*>(allocator_->alloc(sizeof(JoinInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create outer join info", K(ret));
      } else {
        temp_join_info = new (temp_join_info) JoinInfo(join_info->join_type_);
        if (OB_FAIL(temp_join_info->table_set_.add_members(join_info->table_set_))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (OB_FAIL(temp_join_info->on_condition_.assign(join_info->on_condition_))) {
          LOG_WARN("failed to assign on condition", K(ret));
        } else if (OB_FAIL(temp_join_info->where_condition_.assign(join_info->where_condition_))) {
          LOG_WARN("failed to assign where condition", K(ret));
        } else if (OB_FAIL(temp_join_info->equal_join_condition_.assign(join_info->equal_join_condition_))) {
          LOG_WARN("failed to assign equal join condition", K(ret));
        } else {
          join_info_ = temp_join_info;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (IS_OUTER_OR_CONNECT_BY_JOIN(join_info->join_type_)) {
        if (OB_FAIL(append(get_restrict_infos(), join_info->where_condition_))) {
          LOG_WARN("failed to append restrict info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(estimate_size_and_width(left_tree, right_tree, type_, join_info->join_type_))) {
        LOG_WARN("failed to estimate_size", K(ret));
      } else if (OB_FAIL(compute_const_exprs(left_tree, right_tree, type_, join_info->join_type_))) {
        LOG_WARN("failed to compute const exprs", K(ret));
      } else if (OB_FAIL(compute_equal_set(left_tree, right_tree, type_, join_info->join_type_))) {
        LOG_WARN("failed to compute equal set", K(ret));
      } else if (OB_FAIL(compute_fd_item_set_for_join(left_tree, right_tree, join_info, join_info->join_type_))) {
        LOG_WARN("failed to compute fd item set for join", K(ret));
      } else if (OB_FAIL(compute_one_row_info_for_join(left_tree,
                     right_tree,
                     IS_OUTER_OR_CONNECT_BY_JOIN(join_info->join_type_) ? join_info->on_condition_
                                                                        : join_info->where_condition_,
                     join_info->equal_join_condition_,
                     join_info->join_type_))) {
        LOG_WARN("failed to compute one row info for join", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

/*
 * We currently support left-deep tree, right_deep tree and zigzag tree
 */
int ObJoinOrder::generate_join_paths(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info, bool force_ordered)
{
  int ret = OB_SUCCESS;
  int8_t path_types = 0;
  int8_t reverse_path_types = 0;
  ObJoinType reverse_join_type = get_opposite_join_type(join_info->join_type_);
  // connect by can't change join order
  bool connect_by_join = CONNECT_BY_JOIN == join_info->join_type_;
  // get path types needed to generate
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (OB_FAIL(get_valid_path_types(left_tree, right_tree, join_info->join_type_, false, path_types))) {
    LOG_WARN("failed to get valid path types", K(join_info->join_type_), K(ret));
  } else if (!connect_by_join && !force_ordered &&
             OB_FAIL(get_valid_path_types(right_tree, left_tree, reverse_join_type, false, reverse_path_types))) {
    LOG_WARN("failed to get valid path types", K(join_info->join_type_), K(ret));
  } else if (OB_FAIL(inner_generate_join_paths(left_tree,
                 right_tree,
                 join_info->join_type_,
                 reverse_join_type,
                 join_info->on_condition_,
                 join_info->where_condition_,
                 path_types,
                 reverse_path_types))) {
    LOG_WARN("failed to generate join paths", K(ret));
  } else if (interesting_paths_.count() > 0) {
    LOG_TRACE("succeed to generate join paths using hint", K(ret));
  } else if (OB_FAIL(get_valid_path_types(left_tree, right_tree, join_info->join_type_, true, path_types))) {
    LOG_WARN("failed to get valid path types", K(join_info->join_type_), K(ret));
  } else if (!connect_by_join &&
             OB_FAIL(get_valid_path_types(right_tree, left_tree, reverse_join_type, true, reverse_path_types))) {
    LOG_WARN("failed to get valid path types", K(join_info->join_type_), K(ret));
  } else if (OB_FAIL(inner_generate_join_paths(left_tree,
                 right_tree,
                 join_info->join_type_,
                 reverse_join_type,
                 join_info->on_condition_,
                 join_info->where_condition_,
                 path_types,
                 reverse_path_types))) {
    LOG_WARN("failed to generate join path", K(ret));
  } else if (interesting_paths_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to generate join paths by ignoring hint", K(ret));
  } else {
    LOG_TRACE("succeed to generate join paths by ignoring hint", K(ret));
  }
  return ret;
}

int ObJoinOrder::inner_generate_join_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObJoinType reverse_join_type, const ObIArray<ObRawExpr*>& on_condition,
    const ObIArray<ObRawExpr*>& where_condition, const int8_t path_types, const int8_t reverse_path_types)
{
  int ret = OB_SUCCESS;
  bool has_non_nl_path = false;
  int64_t path_number = interesting_paths_.count();
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else {
    LOG_TRACE("valid join path types",
        K(path_types),
        K(reverse_path_types),
        K(where_condition),
        K(on_condition),
        K(join_type),
        K(reverse_join_type));
    // generate merge join paths
    if (((NEED_MJ & path_types) || (NEED_MJ & reverse_path_types))) {
      ObArray<ObRawExpr*> mj_equal_conditions;
      ObArray<ObRawExpr*> mj_other_conditions;
      ObArray<ObRawExpr*> mj_filters;
      bool check_both_side =
          !((NEED_MJ & path_types) && (NEED_MJ & reverse_path_types) && (ObJoinType::INNER_JOIN == join_type));
      LOG_TRACE("start to generate merge join paths");
      if (OB_FAIL(classify_mergejoin_conditions(left_tree,
              right_tree,
              join_type,
              on_condition,
              where_condition,
              mj_equal_conditions,
              mj_other_conditions,
              mj_filters))) {
        LOG_WARN("failed to classify merge join conditions", K(ret));
      } else if (mj_equal_conditions.count() > 0 && (NEED_MJ & path_types) &&
                 OB_FAIL(generate_mj_paths(left_tree,
                     right_tree,
                     join_type,
                     mj_equal_conditions,
                     mj_other_conditions,
                     mj_filters,
                     check_both_side))) {
        LOG_WARN("failed to generate merge join paths", K(ret));
      } else if (mj_equal_conditions.count() > 0 && (NEED_MJ & reverse_path_types) &&
                 OB_FAIL(generate_mj_paths(right_tree,
                     left_tree,
                     reverse_join_type,
                     mj_equal_conditions,
                     mj_other_conditions,
                     mj_filters,
                     check_both_side))) {
        LOG_WARN("failed to generate merge join paths", K(ret));
      } else {
        LOG_TRACE("succeed to generate merge join paths",
            K(mj_equal_conditions.count()),
            "merge_path_count",
            interesting_paths_.count() - path_number);
        path_number = interesting_paths_.count();
        if (mj_equal_conditions.count() > 0) {
          has_non_nl_path = true;
        }
      }
    }
    // generate hash join paths
    if (OB_SUCC(ret) && ((NEED_HASH & path_types) || (NEED_HASH & reverse_path_types))) {
      ObArray<ObRawExpr*> hash_equal_conditions;
      ObArray<ObRawExpr*> hash_other_conditions;
      ObArray<ObRawExpr*> hash_filters;
      LOG_TRACE("start to generate hash join paths");
      if (OB_FAIL(classify_hashjoin_conditions(left_tree,
              right_tree,
              join_type,
              on_condition,
              where_condition,
              hash_equal_conditions,
              hash_other_conditions,
              hash_filters))) {
        LOG_WARN("failed to classify hash join conditions", K(ret));
      } else if (hash_equal_conditions.count() > 0 && (NEED_HASH & path_types) &&
                 OB_FAIL(generate_hash_paths(
                     left_tree, right_tree, join_type, hash_equal_conditions, hash_other_conditions, hash_filters))) {
        LOG_WARN("failed to generate hash join paths", K(ret));
      } else if (hash_equal_conditions.count() > 0 && (NEED_HASH & reverse_path_types) &&
                 OB_FAIL(generate_hash_paths(right_tree,
                     left_tree,
                     reverse_join_type,
                     hash_equal_conditions,
                     hash_other_conditions,
                     hash_filters))) {
        LOG_WARN("failed to generate hash join paths", K(ret));
      } else {
        LOG_TRACE("succeed to generate hash join paths",
            K(hash_equal_conditions.count()),
            "hash_path_count",
            interesting_paths_.count() - path_number);
        path_number = interesting_paths_.count();
        if (hash_equal_conditions.count() > 0) {
          has_non_nl_path = true;
        }
      }
    }
    // generate nest loop join paths
    if (OB_SUCC(ret) && ((NEED_NL & path_types) || (NEED_NL & reverse_path_types))) {
      LOG_TRACE("start to generate nested loop join path");
      if ((NEED_NL & path_types) &&
          OB_FAIL(
              generate_nl_paths(left_tree, right_tree, join_type, on_condition, where_condition, has_non_nl_path))) {
        LOG_WARN("failed to generate nested loop join path", K(ret));
      } else if ((NEED_NL & reverse_path_types) &&
                 OB_FAIL(generate_nl_paths(
                     right_tree, left_tree, reverse_join_type, on_condition, where_condition, has_non_nl_path))) {
        LOG_WARN("failed to generate nested loop join path", K(ret));
      } else {
        LOG_TRACE(
            "succeed to generate all nested loop join path", "nl_path_count", interesting_paths_.count() - path_number);
        path_number = interesting_paths_.count();
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_hash_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& equal_join_conditions,
    const ObIArray<ObRawExpr*>& other_join_conditions, const ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  sql::Path* left_path = NULL;
  sql::Path* right_path = NULL;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left tree and right tree could not be null", K(left_tree), K(right_tree), K(ret));
  } else if (OB_FAIL(find_minimal_cost_path(left_tree->get_interesting_paths(), left_path))) {
    LOG_WARN("failed to get minimal cost left path", K(ret), K(left_path));
  } else if (OB_FAIL(find_minimal_cost_path(right_tree->get_interesting_paths(), right_path))) {
    LOG_WARN("failed to get minimal cost right path", K(ret), K(right_path));
  } else if (OB_ISNULL(left_path) || OB_ISNULL(right_path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path), K(right_path), K(ret));
  } else if (OB_FAIL(create_and_add_hash_path(
                 left_path, right_path, join_type, equal_join_conditions, other_join_conditions, filters))) {
    LOG_WARN("failed to create hash path", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObJoinOrder::generate_nl_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& on_condition, const ObIArray<ObRawExpr*>& where_condition,
    const bool has_non_nl_path)
{
  int ret = OB_SUCCESS;
  bool force_mat = false;     // force to add material
  bool force_no_mat = false;  // force to not add material
  sql::Path* right_normal_path = NULL;
  ObSEArray<sql::Path*, 4> right_inner_paths;
  const ObIArray<ObRawExpr*>& join_condition = IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ? on_condition : where_condition;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(get_plan()), K(ret));
  } else if (OB_FAIL(check_nl_materialization_hint(right_tree->get_tables(), force_mat, force_no_mat))) {
    LOG_WARN("failed to check nl materialization hint.", K(ret));
  } else {
    if (OB_FAIL(
            choose_best_inner_path(left_tree, right_tree, join_condition, join_type, force_mat, right_inner_paths))) {
      LOG_WARN("failed to choose best inner paths", K(ret));
    } else if (OB_UNLIKELY(right_inner_paths.count() != left_tree->get_interesting_paths().count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right patch does not mat left paths",
          K(right_inner_paths.count()),
          K(left_tree->get_interesting_paths().count()),
          K(ret));
    } else if (OB_FAIL(find_minimal_cost_path(right_tree->get_interesting_paths(), right_normal_path))) {
      LOG_WARN("failed to get minimal cost right path", K(right_tree->get_interesting_paths()));
    } else if (OB_ISNULL(right_normal_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("length of right_path_list is 0", K(ret));
    } else { /*do nothing*/
    }

    const ObIArray<sql::Path*>& left_path_list = left_tree->get_interesting_paths();
    for (int64_t i = 0; OB_SUCC(ret) && i < left_path_list.count(); i++) {
      sql::Path* left_path = left_path_list.at(i);
      if (NULL != right_inner_paths.at(i)) {
        if (OB_FAIL(create_and_add_nl_path(
                left_path, right_inner_paths.at(i), join_type, on_condition, where_condition, false))) {
          // generate nest loop join with param push down
          LOG_WARN("failed to create and add nl path with pushing down param", K(ret));
        }
      } else if (has_non_nl_path && !left_tree->get_is_at_most_one_row()) {
        // do nothing
        LOG_TRACE("[SQL OPT JOIN ORDER: DISABLE NESTED LOOP JOIN WHEN RIGHT PATH FULL SCAN]");
      } else if (CONNECT_BY_JOIN != join_type &&
                 (force_mat || (!force_no_mat && !left_tree->get_is_at_most_one_row())) &&
                 OB_FAIL(create_and_add_nl_path(
                     left_path, right_normal_path, join_type, on_condition, where_condition, true))) {
        LOG_WARN("failed to create and add nl path with materialization", K(ret));
      } else if ((force_no_mat ||
                     (!force_mat && (left_tree->get_is_at_most_one_row() || CONNECT_BY_JOIN == join_type))) &&
                 OB_FAIL(create_and_add_nl_path(
                     left_path, right_normal_path, join_type, on_condition, where_condition, false))) {
        LOG_WARN("failed to create and add nl path without materialization", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::choose_best_inner_path(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObIArray<ObRawExpr*>& join_condition, const ObJoinType join_type, const bool force_mat,
    ObIArray<sql::Path*>& right_inner_paths)
{
  int ret = OB_SUCCESS;
  EqualSets ordering_in_eset;
  ObSEArray<ObRawExpr*, 4> left_join_keys;
  ObSEArray<ObRawExpr*, 4> right_join_keys;
  ObSEArray<sql::Path*, 16> nl_with_param_path;
  const ObIArray<sql::Path*>& left_path_list = left_tree->get_interesting_paths();
  const ObShardingInfo* sharding_info = NULL;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(sharding_info = &(right_tree->get_sharding_info()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input params have null", K(ret), K(left_tree), K(right_tree));
  } else if (OB_FAIL(append(ordering_in_eset, left_tree->ordering_output_equal_sets_))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else if (OB_FAIL(append(ordering_in_eset, right_tree->ordering_output_equal_sets_))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(
                 join_condition, left_tree->get_tables(), left_join_keys, right_join_keys))) {
    LOG_WARN("failed to get join keys", K(ret));
  } else if (!force_mat) {
    bool connect_by_rownum = false;
    if (CONNECT_BY_JOIN == join_type && OB_FAIL(check_contain_rownum(join_condition, connect_by_rownum))) {
      LOG_WARN("failed to cehck contain rownum", K(ret));
    } else if (connect_by_rownum) {
      // do nothing
    } else if (OB_FAIL(find_nl_with_param_path(join_condition,
                   left_tree->get_tables(),
                   *(const_cast<ObJoinOrder*>(right_tree)),
                   nl_with_param_path))) {
      LOG_WARN("failed to find nl with param path", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left_path_list.count(); ++i) {
    sql::Path* best_nl_path = NULL;
    if (OB_ISNULL(left_path_list.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < nl_with_param_path.count(); ++j) {
        sql::Path* nl_path = NULL;
        bool is_subset = false;
        bool need_check = false;
        bool is_valid = false;
        if (OB_ISNULL(nl_path = nl_with_param_path.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("right inner path is null", K(ret), K(nl_path));
        } else if (best_nl_path == NULL) {
          need_check = true;
        } else if (ACCESS == right_tree->get_type()) {
          AccessPath* access_nl_path = static_cast<AccessPath*>(nl_path);
          AccessPath* best_access_nl_path = static_cast<AccessPath*>(best_nl_path);
          if (access_nl_path->est_cost_info_.is_unique_) {
            need_check = (!best_access_nl_path->est_cost_info_.is_unique_) ||
                         (best_access_nl_path->get_cost() > access_nl_path->get_cost());
          } else if (best_access_nl_path->est_cost_info_.is_unique_) {
            need_check = false;
          } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(best_access_nl_path->index_keys_,
                         best_access_nl_path->range_prefix_count_,
                         access_nl_path->index_keys_,
                         access_nl_path->range_prefix_count_,
                         is_subset))) {
            LOG_WARN("failed to check prefix subset exprs", K(ret));
          } else if (is_subset && best_access_nl_path->range_prefix_count_ < access_nl_path->range_prefix_count_ &&
                     ((!access_nl_path->est_cost_info_.index_meta_info_.is_index_back_) ||
                         (best_access_nl_path->est_cost_info_.index_meta_info_.is_index_back_ ==
                             access_nl_path->est_cost_info_.index_meta_info_.is_index_back_))) {
            need_check = true;
          } else {
            need_check = best_access_nl_path->get_cost() > access_nl_path->get_cost();
          }
        } else {
          need_check = best_nl_path->get_cost() > nl_path->get_cost();
        }
        if (OB_SUCC(ret) && need_check) {
          best_nl_path = nl_path;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(right_inner_paths.push_back(best_nl_path))) {
        LOG_WARN("failed to push back best inner path", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::classify_mergejoin_conditions(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& on_condition, const ObIArray<ObRawExpr*>& where_condition,
    ObIArray<ObRawExpr*>& equal_join_conditions, ObIArray<ObRawExpr*>& other_join_conditions,
    ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, left_tree and right_tree can not be NULL", K(left_tree), K(right_tree));
  } else if (OB_FAIL(
                 extract_mergejoin_conditions(IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ? on_condition : where_condition,
                     left_tree->get_tables(),
                     right_tree->get_tables(),
                     equal_join_conditions,
                     other_join_conditions))) {
    LOG_WARN("failed to extract merge-join conditions and filters", K(join_type), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type) && OB_FAIL(append(filters, where_condition))) {
    LOG_WARN("failed to append join quals", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObJoinOrder::generate_mj_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& equal_join_conditions,
    const ObIArray<ObRawExpr*>& other_join_conditions, const ObIArray<ObRawExpr*>& filters, const bool check_both_side)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSEArray<ObRawExpr*, 8> left_join_exprs;
  ObSEArray<ObRawExpr*, 8> right_join_exprs;
  ObSEArray<MergeKeyInfo*, 8> left_merge_keys;
  ObSEArray<MergeKeyInfo*, 8> right_merge_keys;
  ObSEArray<ObOrderDirection, 8> left_join_directions;
  ObSEArray<ObOrderDirection, 8> right_join_directions;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(get_plan()), K(ret));
  } else if (ObOptimizerUtil::get_equal_keys(
                 equal_join_conditions, left_tree->get_tables(), left_join_exprs, right_join_exprs)) {
    LOG_WARN("failed to get join exprs", K(ret));
  } else if (OB_UNLIKELY(left_join_exprs.count() != right_join_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unequal array count", K(left_join_exprs.count()), K(right_join_exprs.count()), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(
                 *get_plan()->get_stmt(), left_join_exprs, get_plan()->get_equal_sets(), left_join_directions))) {
    LOG_WARN("failed to find expr direction", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(
                 *get_plan()->get_stmt(), right_join_exprs, get_plan()->get_equal_sets(), right_join_directions))) {
    LOG_WARN("failed to find expr direction", K(ret));
  } else if (OB_FAIL(init_merge_join_structure(allocator,
                 left_tree->get_interesting_paths(),
                 left_join_exprs,
                 left_join_directions,
                 left_tree->get_const_exprs(),
                 left_merge_keys))) {
    LOG_WARN("failed to initialize merge key", K(ret));
  } else if (OB_FAIL(init_merge_join_structure(allocator,
                 right_tree->get_interesting_paths(),
                 right_join_exprs,
                 right_join_directions,
                 right_tree->get_const_exprs(),
                 right_merge_keys))) {
    LOG_WARN("failed to init merge key", K(ret));
  } else {
    sql::Path* left_path = NULL;
    sql::Path* right_path = NULL;
    bool best_need_sort = false;
    MergeKeyInfo* merge_key = NULL;
    ObSEArray<OrderItem, 4> best_order_items;
    ObSEArray<ObRawExpr*, 4> adjusted_join_conditions;
    const ObIArray<sql::Path*>& left_path_list = left_tree->get_interesting_paths();
    const ObIArray<sql::Path*>& right_path_list = right_tree->get_interesting_paths();
    /*
     * so far, for each left/right path, we find the minimal cost right/left path and create corresponding merge path
     * future optimization: for these left/right paths that need sort, we only need generate one best path
     * and create only one merge path
     */
    for (int64_t i = 0; OB_SUCC(ret) && i < left_path_list.count(); ++i) {
      if (OB_ISNULL(left_path = left_path_list.at(i)) || OB_ISNULL(merge_key = left_merge_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_path), K(merge_key), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(
                     equal_join_conditions, merge_key->map_array_, adjusted_join_conditions))) {
        LOG_WARN("failed to adjust expr by mapping", K(ret));
      } else if (OB_FAIL(find_minimal_cost_merge_path(
                     *merge_key, right_join_exprs, *right_tree, best_order_items, right_path, best_need_sort))) {
        LOG_WARN("failed to find minimal cost merge path", K(ret));
      } else if (OB_FAIL(create_and_add_mj_path(left_path,
                     right_path,
                     join_type,
                     merge_key->order_directions_,
                     adjusted_join_conditions,
                     other_join_conditions,
                     filters,
                     merge_key->order_items_,
                     best_order_items,
                     merge_key->need_sort_,
                     best_need_sort))) {
        LOG_WARN("failed to create and add merge join path", K(ret));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret) && check_both_side) {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_path_list.count(); i++) {
        if (OB_ISNULL(right_path = right_path_list.at(i)) || OB_ISNULL(merge_key = right_merge_keys.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(
                       equal_join_conditions, merge_key->map_array_, adjusted_join_conditions))) {
          LOG_WARN("failed to adjust exprs by mapping", K(ret));
        } else if (OB_FAIL(find_minimal_cost_merge_path(
                       *merge_key, left_join_exprs, *left_tree, best_order_items, left_path, best_need_sort))) {
          LOG_WARN("failed to find minimal cost merge path", K(ret));
        } else if (OB_FAIL(create_and_add_mj_path(left_path,
                       right_path,
                       join_type,
                       merge_key->order_directions_,
                       adjusted_join_conditions,
                       other_join_conditions,
                       filters,
                       best_order_items,
                       merge_key->order_items_,
                       best_need_sort,
                       merge_key->need_sort_))) {
          LOG_WARN("failed to create and add merge join path", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::find_minimal_cost_merge_path(const MergeKeyInfo& left_merge_key,
    const ObIArray<ObRawExpr*>& right_join_exprs, const ObJoinOrder& right_tree, ObIArray<OrderItem>& best_order_items,
    sql::Path*& best_path, bool& best_need_sort)
{
  int ret = OB_SUCCESS;
  double best_cost = 0.0;
  double right_path_cost = 0.0;
  double right_sort_cost = 0.0;
  bool right_need_sort = false;
  ObSEArray<ObRawExpr*, 8> right_order_exprs;
  ObSEArray<OrderItem, 8> right_order_items;
  const ObIArray<sql::Path*>& right_path_list = right_tree.get_interesting_paths();
  best_path = NULL;
  best_need_sort = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_path_list.count(); i++) {
    sql::Path* right_path = NULL;
    right_order_exprs.reset();
    right_order_items.reset();
    if (OB_ISNULL(right_path = right_path_list.at(i)) || OB_ISNULL(right_path->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(right_path), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(
                   right_join_exprs, left_merge_key.map_array_, right_order_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(
                   right_order_exprs, left_merge_key.order_directions_, right_order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(right_order_items,
                   right_path->ordering_,
                   right_path->parent_->get_fd_item_set(),
                   right_path->parent_->get_ordering_output_equal_sets(),
                   right_tree.get_const_exprs(),
                   right_path->parent_->get_is_at_most_one_row(),
                   right_need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (right_need_sort &&
               OB_FAIL(compute_sort_cost_for_merge_join(*right_path, right_order_items, right_sort_cost))) {
      LOG_WARN("failed to compute sort cost for merge join", K(ret));
    } else {
      right_path_cost = right_need_sort ? (right_sort_cost + right_path->cost_) : right_path->cost_;
      if (NULL == best_path || right_path_cost < best_cost) {
        if (OB_FAIL(best_order_items.assign(right_order_items))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          best_path = right_path;
          best_need_sort = right_need_sort;
          best_cost = right_path_cost;
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_sort_cost_for_merge_join(sql::Path& path, ObIArray<OrderItem>& expected_ordering, double& cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_ISNULL(path.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSortCostInfo sort_cost_info(
        path.parent_->get_output_rows(), path.parent_->get_average_output_row_size(), 0, NULL);
    if (OB_FAIL(ObOptEstCost::cost_sort(sort_cost_info, expected_ordering, cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::init_merge_join_structure(ObIAllocator& allocator, const ObIArray<sql::Path*>& paths,
    const ObIArray<ObRawExpr*>& join_exprs, const ObIArray<ObOrderDirection>& join_directions,
    const ObIArray<ObRawExpr*>& const_exprs, ObIArray<MergeKeyInfo*>& merge_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    MergeKeyInfo* merge_key = NULL;
    sql::Path* path = NULL;
    if (OB_ISNULL(path = paths.at(i)) || OB_ISNULL(path->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(path), K(ret));
    } else if (OB_ISNULL(merge_key = static_cast<MergeKeyInfo*>(allocator.alloc(sizeof(MergeKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc merge key info", K(ret));
    } else {
      merge_key = new (merge_key) MergeKeyInfo(allocator, join_exprs.count());
      if (OB_FAIL(ObOptimizerUtil::decide_sort_keys_for_merge_style_op(path->ordering_,
              path->parent_->get_fd_item_set(),
              path->parent_->get_ordering_output_equal_sets(),
              const_exprs,
              path->parent_->get_is_at_most_one_row(),
              join_exprs,
              join_directions,
              *merge_key))) {
        LOG_WARN("failed to decide sort keys for merge join", K(ret));
      } else if (OB_FAIL(merge_keys.push_back(merge_key))) {
        LOG_WARN("failed to push back merge key", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObJoinOrder::set_nl_filters(JoinPath* join_path, const sql::Path* right_path, const ObJoinType join_type,
    const ObIArray<ObRawExpr*>& on_condition, const ObIArray<ObRawExpr*>& where_condition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_path) || OB_ISNULL(right_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(join_path), K(right_path), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < where_condition.count(); ++i) {
      if (OB_ISNULL(where_condition.at(i))) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("raw expr is null", K(ret));
      } else if (where_condition.at(i)->get_relation_ids().is_subset(get_tables())) {
        if (right_path->is_inner_path() &&
            ObOptimizerUtil::find_item(right_path->pushdown_filters_, where_condition.at(i))) {
          /*do nothing*/
        } else {
          if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type)) {
            ret = join_path->filter_.push_back(where_condition.at(i));
          } else {
            ret = join_path->other_join_condition_.push_back(where_condition.at(i));
          }
        }
      } else {
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < on_condition.count(); ++i) {
      if (right_path->is_inner_path() &&
          ObOptimizerUtil::find_item(right_path->pushdown_filters_, on_condition.at(i))) {
        /*do nothing*/
      } else {
        ret = join_path->other_join_condition_.push_back(on_condition.at(i));
      }
    }
  }
  return ret;
}

int ObJoinOrder::create_and_add_hash_path(const sql::Path* left_path, const sql::Path* right_path,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& equal_join_conditions,
    const ObIArray<ObRawExpr*>& other_join_conditions, const ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  JoinPath* join_path = NULL;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_path), K(right_path), K(get_plan()), K(ret));
  } else if (OB_ISNULL(join_path = static_cast<JoinPath*>(allocator_->alloc(sizeof(JoinPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc a join path", K(join_path), K(ret));
  } else {
    join_path = new (join_path) JoinPath(this, left_path, right_path, HASH_JOIN, join_type);
    if (OB_FAIL(append(join_path->equal_join_condition_, equal_join_conditions))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(append(join_path->other_join_condition_, other_join_conditions))) {
      LOG_WARN("failed to append join filters", K(ret));
    } else if (OB_FAIL(append(join_path->filter_, filters))) {
      LOG_WARN("failed to append join quals", K(ret));
    } else if (OB_FAIL(join_path->estimate_cost())) {
      LOG_WARN("failed to calculate cost in create_nl_path", K(ret));
    } else if (OB_FAIL(add_path(join_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::create_and_add_mj_path(const sql::Path* left_path, const sql::Path* right_path,
    const ObJoinType join_type, const ObIArray<ObOrderDirection>& merge_directions,
    const common::ObIArray<ObRawExpr*>& equal_join_conditions,
    const common::ObIArray<ObRawExpr*>& other_join_conditions, const common::ObIArray<ObRawExpr*>& filters,
    const ObIArray<OrderItem>& left_sort_keys, const ObIArray<OrderItem>& right_sort_keys, const bool left_need_sort,
    const bool right_need_sort)
{
  int ret = OB_SUCCESS;
  JoinPath* join_path = NULL;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left_path), K(right_path), K(get_plan()), K(stmt), K(ret));
  } else if (RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("merge right [semi/anti] join is not supported", K(ret), K(join_type));
  } else if (OB_ISNULL(join_path = static_cast<JoinPath*>(allocator_->alloc(sizeof(JoinPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc join path in creating_mj_path", K(ret));
  } else {
    join_path = new (join_path) JoinPath(this, left_path, right_path, MERGE_JOIN, join_type);
    join_path->left_need_sort_ = left_need_sort;
    join_path->right_need_sort_ = right_need_sort;
    if (OB_FAIL(append(join_path->equal_join_condition_, equal_join_conditions))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(append(join_path->other_join_condition_, other_join_conditions))) {
      LOG_WARN("failed to append join filters", K(ret));
    } else if (OB_FAIL(append(join_path->filter_, filters))) {
      LOG_WARN("failed to append join quals", K(ret));
    } else if (OB_FAIL(append(join_path->left_expected_ordering_, left_sort_keys))) {
      LOG_WARN("failed to append left expected ordering", K(ret));
    } else if (OB_FAIL(append(join_path->right_expected_ordering_, right_sort_keys))) {
      LOG_WARN("failed to append right expected ordering", K(ret));
    } else if (OB_FAIL(append(join_path->merge_directions_, merge_directions))) {
      LOG_WARN("faield to append merge directions", K(ret));
    } else if (FULL_OUTER_JOIN != join_type && RIGHT_OUTER_JOIN != join_type) {
      if (!left_need_sort) {
        join_path->set_interesting_order_info(left_path->get_interesting_order_info());
        if (OB_FAIL(append(join_path->ordering_, left_path->ordering_))) {
          LOG_WARN("failed to append join orderign", K(ret));
        } else if (OB_FAIL(check_join_interesting_order(join_path))) {
          LOG_WARN("failed to update join interesting order info", K(ret));
        }
      } else {
        int64_t dummy_prefix_count = 0;
        int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
        if (OB_FAIL(append(join_path->ordering_, left_sort_keys))) {
          LOG_WARN("failed to append join ordering", K(ret));
        } else if (OB_FAIL(check_all_interesting_order(
                       join_path->get_ordering(), stmt, dummy_prefix_count, interesting_order_info))) {
          LOG_WARN("failed to check all interesting order", K(ret));
        } else {
          join_path->add_interesting_order_flag(interesting_order_info);
        }
      }
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(join_path->estimate_cost())) {
        LOG_WARN("failed to calculate cost in create_ml_path", K(ret));
      } else if (OB_FAIL(add_path(join_path))) {
        LOG_WARN("failed to add path", K(ret));
      } else {
        LOG_TRACE("succeed to create a merge join path",
            K(join_type),
            K(merge_directions),
            K(left_sort_keys),
            K(right_sort_keys),
            K(left_need_sort),
            K(right_need_sort));
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_hashjoin_conditions(const ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
    const ObRelIds& right_tables, ObIArray<ObRawExpr*>& equal_join_conditions,
    ObIArray<ObRawExpr*>& other_join_conditions)
{
  int ret = OB_SUCCESS;
  if (join_quals.count() > 0) {
    ObRawExpr* cur_expr = NULL;
    ObRawExpr* left_expr = NULL;
    ObRawExpr* right_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
      cur_expr = join_quals.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join qual should not be NULL", K(cur_expr), K(ret));
      } else if (cur_expr->has_flag(IS_JOIN_COND) && (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
                                                         OB_ISNULL(right_expr = cur_expr->get_param_expr(1)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join qual should not be NULL", K(*cur_expr), K(left_expr), K(right_expr), K(ret));
      } else if (cur_expr->has_flag(IS_JOIN_COND) && T_OP_ROW != left_expr->get_expr_type() &&
                 T_OP_ROW != right_expr->get_expr_type() &&
                 ((left_expr->get_relation_ids().is_subset(left_tables) &&
                      right_expr->get_relation_ids().is_subset(right_tables)) ||
                     (left_expr->get_relation_ids().is_subset(right_tables) &&
                         right_expr->get_relation_ids().is_subset(left_tables)))) {
        if (OB_FAIL(equal_join_conditions.push_back(cur_expr))) {
          LOG_WARN("fail to push back", K(cur_expr), K(ret));
        }
      } else if (cur_expr->get_relation_ids().is_subset(get_tables())) {
        if (OB_FAIL(other_join_conditions.push_back(cur_expr))) {
          LOG_WARN("fail to push back", K(cur_expr), K(ret));
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObJoinOrder::classify_hashjoin_conditions(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const ObIArray<ObRawExpr*>& on_condition, const ObIArray<ObRawExpr*>& where_condition,
    ObIArray<ObRawExpr*>& equal_join_conditions, ObIArray<ObRawExpr*>& other_join_conditions,
    ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, left_tree and right_tree can not be NULL", K(left_tree), K(right_tree));
  } else if (OB_FAIL(
                 extract_hashjoin_conditions(IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ? on_condition : where_condition,
                     left_tree->get_tables(),
                     right_tree->get_tables(),
                     equal_join_conditions,
                     other_join_conditions))) {
    LOG_WARN("failed to extract hash join condtiions and filters", K(join_type), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type) && OB_FAIL(append(filters, where_condition))) {
    LOG_WARN("failed to append join quals", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObJoinOrder::extract_mergejoin_conditions(const ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
    const ObRelIds& right_tables, ObIArray<ObRawExpr*>& equal_join_conditions,
    ObIArray<ObRawExpr*>& other_join_conditions)
{
  int ret = OB_SUCCESS;

  if (join_quals.count() > 0) {
    ObRawExpr* cur_expr = NULL;
    ObRawExpr* left_expr = NULL;
    ObRawExpr* right_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
      cur_expr = join_quals.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join qual should not be NULL", K(cur_expr), K(ret));
      } else if (cur_expr->has_flag(IS_JOIN_COND) && (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
                                                         OB_ISNULL(right_expr = cur_expr->get_param_expr(1)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join qual should not be NULL", K(*cur_expr), K(left_expr), K(right_expr), K(ret));
      } else if (cur_expr->has_flag(IS_JOIN_COND) && T_OP_ROW != left_expr->get_expr_type() &&
                 T_OP_ROW != right_expr->get_expr_type() &&
                 left_expr->get_collation_type() == right_expr->get_collation_type()  // TODO:@
                 && ((left_expr->get_relation_ids().is_subset(left_tables) &&
                         right_expr->get_relation_ids().is_subset(right_tables)) ||
                        (left_expr->get_relation_ids().is_subset(right_tables) &&
                            right_expr->get_relation_ids().is_subset(left_tables)))) {
        bool is_mj_able = false;
        common::ObObjType compare_type = cur_expr->get_result_type().get_calc_type();
        if (OB_FAIL(ObObjCaster::is_cast_monotonic(left_expr->get_data_type(), compare_type, is_mj_able))) {
          LOG_WARN("check cast monotonic error", K(left_expr->get_data_type()), K(compare_type), K(ret));
        } else if (is_mj_able) {
          if (OB_FAIL(ObObjCaster::is_cast_monotonic(right_expr->get_data_type(), compare_type, is_mj_able))) {
            LOG_WARN("check cast monotonic error", K(right_expr->get_data_type()), K(compare_type), K(ret));
          } else { /*do nothing*/
          }
        } else { /*do nothing*/
        }
        if (OB_SUCC(ret)) {
          if (is_mj_able) {
            ret = equal_join_conditions.push_back(cur_expr);
          } else {
            ret = other_join_conditions.push_back(cur_expr);
          }
        } else { /*do nothing*/
        }
      } else if (cur_expr->get_relation_ids().is_subset(get_tables())) {
        if (OB_FAIL(other_join_conditions.push_back(cur_expr))) {
          LOG_WARN("fail to push back", K(cur_expr), K(ret));
        }
      } else {
        LOG_TRACE("A qual references this join, but we can not resolve it in this level");
      }
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObJoinOrder::extract_params_for_expr(const uint64_t table_id, const ObRelIds& join_relids,
    ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, ObRawExpr*& new_expr, ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(new_expr) || OB_ISNULL(expr) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(new_expr), K(expr), K(get_plan()), K(stmt), K(opt_ctx), K(ret));
  } else {
    ObSQLSessionInfo* session_info = const_cast<ObSQLSessionInfo*>(opt_ctx->get_session_info());
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObRawExpr* expr_element = expr->get_param_expr(i);
      if (OB_ISNULL(expr_element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null poiner passed in", K(i), K(expr_element), K(ret));
      } else if (!expr_element->has_flag(CNT_COLUMN)) {
        if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, expr_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      } else if (expr_element->has_flag(IS_COLUMN) &&
                 static_cast<ObColumnRefRawExpr*>(expr_element)->get_expr_level() != stmt->get_current_level()) {
        if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, expr_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      } else if (expr_element->get_relation_ids().is_subset(join_relids) && T_OP_ROW != expr_element->get_expr_type()) {
        int64_t param_num = ObOptimizerUtil::find_exec_param(nl_params, expr_element);
        if (param_num >= 0) {
          ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(stmt->get_exec_param_ref_exprs(), param_num);
          if (param_expr != NULL) {
            expr_element = param_expr;
          }
        } else {
          std::pair<int64_t, ObRawExpr*> init_expr;
          if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
                  stmt, opt_ctx->get_expr_factory(), expr_element, session_info, init_expr))) {
            LOG_WARN("create param for stmt error in extract_params_for_inner_path", K(ret));
          } else if (OB_FAIL(nl_params.push_back(init_expr))) {
            LOG_WARN("push back error", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, expr_element, i))) {
            LOG_WARN("failed to add extracted expr", K(ret));
          } else { /*do nothing*/
          }
        }
      } else if (expr_element->get_relation_ids().overlap(join_relids)) {
        ObRawExpr* dest_element = NULL;
        // recursive to child expr extract params
        if (OB_FAIL(extract_params_for_inner_path(table_id, join_relids, nl_params, expr_element, dest_element))) {
          LOG_WARN("fail to extract param from raw expr", K(ret), K(expr_element));
        } else if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, dest_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      } else {
        if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, expr_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_params_for_inner_path(const uint64_t table_id, const ObRelIds& join_relids,
    ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, const ObIArray<ObRawExpr*>& exprs,
    ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr* temp_expr = NULL;
    if (OB_FAIL(extract_params_for_inner_path(table_id, join_relids, nl_params, exprs.at(i), temp_expr))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (OB_FAIL(temp_expr->extract_info())) {
      LOG_WARN("failed to extract expr info", K(ret));
    } else if (OB_FAIL(temp_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_exprs.push_back(temp_expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::extract_params_for_inner_path(const uint64_t table_id, const ObRelIds& join_relids,
    ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, ObRawExpr* expr, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = expr;
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(expr), K(get_plan()), K(stmt), K(opt_ctx), K(ret));
  } else {
    ObSQLSessionInfo* session_info = const_cast<ObSQLSessionInfo*>(opt_ctx->get_session_info());
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_CONST:
      case ObRawExpr::EXPR_SET_OP:
      case ObRawExpr::EXPR_QUERY_REF: {
        break;
      }
      case ObRawExpr::EXPR_COLUMN_REF: {
        // TODO(rudian:this branch may be unreachable)
        ObColumnRefRawExpr* expr_bin = static_cast<ObColumnRefRawExpr*>(expr);
        if (expr_bin->get_table_id() != table_id) {
          int64_t param_num = ObOptimizerUtil::find_exec_param(nl_params, expr);
          if (param_num >= 0) {
            ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(stmt->get_exec_param_ref_exprs(), param_num);
            if (param_expr != NULL) {
              new_expr = param_expr;
            }
          } else {
            std::pair<int64_t, ObRawExpr*> init_expr;
            if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
                    stmt, opt_ctx->get_expr_factory(), new_expr, session_info, init_expr))) {
              LOG_WARN("create param for stmt error in extract_params_for_inner_path", K(ret));
            } else if (OB_FAIL(nl_params.push_back(init_expr))) {
              LOG_WARN("push back error", K(ret));
            } else { /*do nothing*/
            }
          }
        } else { /*do nothing*/
        }

        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* case_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), case_expr))) {
          LOG_WARN("create case operator expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = case_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("case_expr is null", K(case_expr));
        } else {
          ret = case_expr->assign(static_cast<ObCaseOpRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            case_expr->clear_child();
            if (OB_FAIL(extract_params_for_expr(table_id, join_relids, nl_params, new_expr, expr))) {
              LOG_WARN("failed to extract child expr info for nl", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_AGGR: {
        ObAggFunRawExpr* aggr_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), aggr_expr))) {
          LOG_WARN("create aggr expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = aggr_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aggr_expr is null", K(aggr_expr));
        } else {
          ret = aggr_expr->assign(static_cast<ObAggFunRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            aggr_expr->clear_child();
            if (OB_FAIL(extract_params_for_expr(table_id, join_relids, nl_params, new_expr, expr))) {
              LOG_WARN("failed to extract child expr info for nl", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        ObSysFunRawExpr* func_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), func_expr))) {
          LOG_WARN("create system function expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = func_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("func_expr is null", K(func_expr));
        } else {
          ret = func_expr->assign(static_cast<ObSysFunRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            func_expr->clear_child();
            if (OB_FAIL(extract_params_for_expr(table_id, join_relids, nl_params, new_expr, expr))) {
              LOG_WARN("failed to extract child expr info for nl", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        ObOpRawExpr* op_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), op_expr))) {
          LOG_WARN("create operator expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = op_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op_expr is null", K(op_expr));
        } else {
          ret = op_expr->assign(static_cast<ObOpRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            op_expr->clear_child();
            if (OB_FAIL(extract_params_for_expr(table_id, join_relids, nl_params, new_expr, expr))) {
              LOG_WARN("failed to extract child expr info for nl", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr class", K(expr_class), K(ret));
        // should not reach here
        break;
    }  // switch case end
  }
  return ret;
}

int ObJoinOrder::check_nl_materialization_hint(const ObRelIds& table_ids, bool& force_mat, bool& force_no_mat)
{
  int ret = OB_SUCCESS;
  bool use_nl_mat = false;
  bool has_nl_mat_hint = false;
  force_mat = false;
  force_no_mat = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObDMLStmt* stmt = get_plan()->get_stmt();
    const ObStmtHint& stmt_hint = stmt->get_stmt_hint();
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_hint.use_nl_materialization_idxs_.count(); ++i) {
      const ObRelIds& rel_ids = stmt_hint.use_nl_materialization_idxs_[i];
      if (rel_ids.equal(table_ids)) {
        use_nl_mat = true;
        has_nl_mat_hint = true;
        break;
      } else { /* do nothing. */
      }
    }
    // if both use_material and no_use_material, no_use_material worked.
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_hint.no_use_nl_materialization_idxs_.count(); ++i) {
      const ObRelIds& rel_ids = stmt_hint.no_use_nl_materialization_idxs_[i];
      if (rel_ids.equal(table_ids)) {
        use_nl_mat = false;
        has_nl_mat_hint = true;
        break;
      } else { /* do nothing. */
      }
    }
    if (OB_SUCC(ret)) {
      force_mat = has_nl_mat_hint && use_nl_mat;
      force_no_mat = has_nl_mat_hint && !use_nl_mat;
    }
  }
  return ret;
}

int ObJoinOrder::check_right_tree_use_join_method(const ObJoinOrder* right_tree,
    const common::ObIArray<ObRelIds>& use_idxs, bool& hinted, int8_t& need_path_types, int8_t hint_type)
{
  int ret = OB_SUCCESS;
  if (use_idxs.count() > 0 && ObTransformUtils::is_subarray(right_tree->get_tables(), use_idxs)) {
    if (hinted) {
      need_path_types |= hint_type;
    } else {
      need_path_types = hint_type;
      hinted = true;
    }
  } else {
  }  // do nothing.
  return ret;
}

int ObJoinOrder::check_right_tree_no_use_join_method(const ObJoinOrder* right_tree,
    const common::ObIArray<ObRelIds>& no_use_idxs, int8_t& need_path_types, int8_t hint_type)
{
  int ret = OB_SUCCESS;
  if (no_use_idxs.count() > 0 && ObTransformUtils::is_subarray(right_tree->get_tables(), no_use_idxs)) {
    need_path_types &= ~hint_type;
  } else {
  }  // do nothing.
  return ret;
}

int ObJoinOrder::get_valid_path_types(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObJoinType join_type, const bool ignore_hint, int8_t& need_path_types)
{
  int ret = OB_SUCCESS;
  need_path_types = 0;
  const ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(left_tree), K(right_tree), K(get_plan()), K(stmt), K(ret));
  } else if (CONNECT_BY_JOIN == join_type) {
    need_path_types = NEED_NL;
  } else {
    ObStmtHint& stmt_hint = const_cast<ObDMLStmt*>(stmt)->get_stmt_hint();
    if (stmt_hint.bnl_allowed_) {
      need_path_types = NEED_NL | NEED_BNL | NEED_MJ | NEED_HASH;
    } else {
      need_path_types = NEED_NL | NEED_MJ | NEED_HASH;
    }

    if (OB_SUCC(ret) && !ignore_hint) {
      bool hinted = false;
      if (OB_FAIL(check_right_tree_use_join_method(
              right_tree, stmt_hint.use_merge_idxs_, hinted, need_path_types, NEED_MJ))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_use_join_method(
                     right_tree, stmt_hint.use_hash_idxs_, hinted, need_path_types, NEED_HASH))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_use_join_method(
                     right_tree, stmt_hint.use_nl_idxs_, hinted, need_path_types, NEED_NL))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_use_join_method(
                     right_tree, stmt_hint.use_bnl_idxs_, hinted, need_path_types, NEED_BNL))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_no_use_join_method(
                     right_tree, stmt_hint.no_use_merge_idxs_, need_path_types, NEED_MJ))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_no_use_join_method(
                     right_tree, stmt_hint.no_use_hash_idxs_, need_path_types, NEED_HASH))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_no_use_join_method(
                     right_tree, stmt_hint.no_use_nl_idxs_, need_path_types, NEED_NL))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else if (OB_FAIL(check_right_tree_no_use_join_method(
                     right_tree, stmt_hint.no_use_bnl_idxs_, need_path_types, NEED_BNL))) {
        LOG_WARN("failed to check right tree join method", K(ret));
      } else {
      }  // do nothing.
    }

    if (OB_FAIL(ret)) {
    } else if (RIGHT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      stmt_hint.reset_valid_join_type();
      need_path_types &= ~NEED_NL;
      need_path_types &= ~NEED_BNL;
    }
    // semi join nested loop block nested loop only
    if (OB_FAIL(ret)) {
    } else if (IS_SEMI_ANTI_JOIN(join_type)) {
      stmt_hint.reset_valid_join_type();
      if (RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type) {
        need_path_types &= ~NEED_NL;
        need_path_types &= ~NEED_BNL;
        need_path_types &= ~NEED_MJ;
      }
    }
  }
  return ret;
}

int ObJoinOrder::fill_table_scan_param(ObCostTableScanInfo& est_cost_info)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = est_cost_info.table_id_;
  const uint64_t ref_table_id = est_cost_info.ref_table_id_;
  const uint64_t index_id = est_cost_info.index_id_;
  const ObTableSchema* table_schema = NULL;
  ObEstSelInfo* est_sel_info = est_cost_info.est_sel_info_;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
  ObIndexMetaInfo& index_meta_info = est_cost_info.index_meta_info_;
  storage::ObTableScanParam& table_scan_param = est_cost_info.table_scan_param_;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id) ||
      OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(est_sel_info) || OB_ISNULL(schema_guard = est_sel_info->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(est_sel_info), K(table_meta_info), K(schema_guard), K(ret));
  } else {
    // set table scan related information
    if (0 != table_meta_info.part_count_) {
      table_scan_param.index_id_ = index_id;
      table_scan_param.scan_flag_.index_back_ = index_meta_info.is_index_back_;

      table_scan_param.pkey_ = table_meta_info_.table_est_part_.pkey_;

      ObOptimizerContext* opt_ctx = NULL;
      if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument of NULL pointer", K(get_plan()), K(opt_ctx), K(ret));
      } else if (OB_FAIL(
                     extract_used_column_ids(table_id, ref_table_id, *est_sel_info, table_scan_param.column_ids_))) {
        LOG_WARN("failed to extract column ids", K(table_id), K(ref_table_id), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
        LOG_WARN("failed to get table schema", K(ref_table_id), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null table schema", K(ret));
      } else {
        table_scan_param.schema_version_ = table_schema->get_schema_version();
      }
    }
    // set index related information
    if (OB_SUCC(ret)) {
      if (index_id != ref_table_id) {
        if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
          LOG_WARN("failed to get table schema", K(index_id), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema should not be null", K(ret), K(index_id));
        } else if (table_meta_info.table_column_count_ == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table column count is 0", K(ret));
        } else {
          index_meta_info.index_part_size_ = table_meta_info.part_size_ *
                                             static_cast<double>(table_schema->get_column_count()) /
                                             static_cast<double>(table_meta_info.table_column_count_);
          index_meta_info.index_micro_block_size_ = table_schema->get_block_size();
          index_meta_info.index_column_count_ = table_schema->get_column_count();
        }
      } else {
        index_meta_info.index_part_size_ = table_meta_info.part_size_;
        index_meta_info.index_micro_block_size_ = table_meta_info.micro_block_size_;
        index_meta_info.index_column_count_ = table_meta_info.table_column_count_;
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_used_column_ids(const uint64_t table_id, const uint64_t ref_table_id,
    ObEstSelInfo& est_sel_info, ObIArray<uint64_t>& column_ids, const bool eliminate_rowid_col /* false */)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  ObSqlSchemaGuard* schema_guard = est_sel_info.get_sql_schema_guard();
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    const TableItem* table_item = stmt->get_table_item_by_id(table_id);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret));
    } else {
      // add all rowkey info, always used when merge ss-table and mem-table
      const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
      uint64_t column_id = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          LOG_WARN("Fail to get column id", K(ret));
        } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id && eliminate_rowid_col) {
          // do nothing
        } else if (OB_FAIL(column_ids.push_back(column_id))) {
          LOG_WARN("Fail to add column id", K(ret));
        } else { /*do nothing*/
        }
      }
      // add common column ids
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
        const ColumnItem* col_item = stmt->get_column_item(i);
        if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item or item expr is NULL", K(ret), K(col_item));
        } else if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id && eliminate_rowid_col) {
          // do nothing
        } else if (col_item->table_id_ == table_id && col_item->expr_->is_explicited_reference() &&
                   OB_FAIL(add_var_to_array_no_dup(column_ids, col_item->expr_->get_column_id()))) {
          LOG_WARN("Fail to add column id", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

/**
 * compute whether is unique index and index need back
 */
int ObJoinOrder::get_simple_index_info(const uint64_t table_id, const uint64_t ref_table_id, const uint64_t index_id,
    bool& is_unique_index, bool& is_index_back, bool& is_index_global)
{
  int ret = OB_SUCCESS;
  is_unique_index = false;
  is_index_back = false;
  ObSEArray<uint64_t, 16> column_ids;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* index_schema = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == ref_table_id) || OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(ref_table_id), K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_FAIL(extract_used_column_ids(table_id, ref_table_id, get_plan()->get_est_sel_info(), column_ids))) {
    LOG_WARN("failed to extract column ids", K(table_id), K(ref_table_id), K(ret));
  } else if (index_id != ref_table_id && !ObSQLMockSchemaUtils::is_mock_index(ref_table_id)) {
    is_unique_index = index_schema->is_unique_index();
    is_index_global = index_schema->is_global_index_table();
    is_index_back = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < column_ids.count(); ++idx) {
      bool found = false;
      if (OB_FAIL(index_schema->has_column(column_ids.at(idx), found))) {
        LOG_WARN("check index_schema has column failed", K(found), K(idx), K(column_ids.at(idx)), K(ret));
      } else if (!found) {
        is_index_back = true;
        break;
      }
    }
  } else {
    is_index_global = false;
    is_unique_index = true;
    is_index_back = false;
  }
  return ret;
}

int ObJoinOrder::fill_filters(const ObIArray<ObRawExpr*>& all_filters, const ObQueryRange* query_range,
    ObCostTableScanInfo& est_cost_info, bool& is_nl_with_extended_range)
{
  int ret = OB_SUCCESS;
  is_nl_with_extended_range = false;
  if (NULL == query_range) {
    ret = est_cost_info.table_filters_.assign(all_filters);
  } else {
    ObEstSelInfo* est_sel_info = NULL;
    ObSqlSchemaGuard* schema_guard = NULL;
    const ObTableSchema* index_schema = NULL;
    ObBitSet<> expr_column_bs;
    ObBitSet<> index_column_bs;
    ObBitSet<> prefix_column_bs;
    ObBitSet<> ex_prefix_column_bs;
    ObSEArray<ObColDesc, 16> index_column_descs;
    if (OB_ISNULL(est_sel_info = est_cost_info.est_sel_info_) ||
        OB_ISNULL(schema_guard = est_cost_info.est_sel_info_->get_sql_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(est_sel_info), K(schema_guard), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(est_cost_info.index_id_, index_schema))) {
      LOG_WARN("failed to get index schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(index_schema), K(ret));
    } else if (OB_FAIL(index_schema->get_column_ids(index_column_descs))) {
      LOG_WARN("failed to extract column ids", K(ret));
    } else {
      // get index column bitsets
      for (int64_t i = 0; OB_SUCC(ret) && i < index_column_descs.count(); i++) {
        if (OB_FAIL(index_column_bs.add_member(index_column_descs.at(i).col_id_))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
      //
      if (OB_SUCC(ret)) {
        if (OB_FAIL(est_cost_info.prefix_filters_.assign(query_range->get_range_exprs()))) {
          LOG_WARN("failed to assign exprs", K(ret));
        }
      }
      ObSEArray<ObRawExpr*, 4> new_prefix_filters;
      ObBitSet<> column_bs;
      for (int64_t i = 0; OB_SUCC(ret) && i < est_cost_info.prefix_filters_.count(); ++i) {
        ObRawExpr* expr = est_cost_info.prefix_filters_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (expr->has_flag(CNT_EXEC_PARAM)) {
          ret = est_cost_info.pushdown_prefix_filters_.push_back(expr);
        } else if (ObOptimizerUtil::extract_column_ids(expr, est_cost_info.table_id_, column_bs)) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else {
          ret = new_prefix_filters.push_back(expr);
        }
      }

      int64_t first_param_column_idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && first_param_column_idx == -1 && i < est_cost_info.range_columns_.count();
           ++i) {
        ColumnItem& column = est_cost_info.range_columns_.at(i);
        if (!column_bs.has_member(column.column_id_)) {
          first_param_column_idx = i;
        } else {
          ret = prefix_column_bs.add_member(column.column_id_);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ex_prefix_column_bs.add_members(prefix_column_bs))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (-1 != first_param_column_idx) {
        uint64_t column_id = est_cost_info.range_columns_.at(first_param_column_idx).column_id_;
        ret = ex_prefix_column_bs.add_member(column_id);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < all_filters.count(); i++) {
        expr_column_bs.reset();
        ObRawExpr* filter = NULL;
        bool can_extract = false;
        if (OB_ISNULL(filter = all_filters.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (ObOptimizerUtil::find_equal_expr(est_cost_info.prefix_filters_, filter)) {
          /*do nothing*/
        } else if (ObOptimizerUtil::extract_column_ids(filter, est_cost_info.table_id_, expr_column_bs)) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else if (!expr_column_bs.is_subset(index_column_bs)) {
          ret = est_cost_info.table_filters_.push_back(filter);
        } else if (OB_FAIL(can_extract_unprecise_range(
                       est_cost_info.table_id_, filter, ex_prefix_column_bs, can_extract))) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else if (can_extract) {
          ret = est_cost_info.pushdown_prefix_filters_.push_back(filter);
        } else if (est_cost_info.ref_table_id_ != est_cost_info.index_id_) {
          ret = est_cost_info.postfix_filters_.push_back(filter);
        } else {
          ret = est_cost_info.table_filters_.push_back(filter);
        }
      }

      if (est_cost_info.pushdown_prefix_filters_.empty()) {
      } else if (!column_bs.is_empty()) {
        est_cost_info.prefix_filters_.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < new_prefix_filters.count(); ++i) {
          expr_column_bs.reset();
          ObRawExpr* filter = new_prefix_filters.at(i);
          if (OB_ISNULL(filter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (ObOptimizerUtil::extract_column_ids(filter, est_cost_info.table_id_, expr_column_bs)) {
            LOG_WARN("failed to extract column ids", K(ret));
          } else if (expr_column_bs.is_subset(prefix_column_bs)) {
            ret = est_cost_info.prefix_filters_.push_back(filter);
          } else {
            ret = est_cost_info.pushdown_prefix_filters_.push_back(filter);
          }
        }
      } else {
        est_cost_info.prefix_filters_.reset();
      }
      if (OB_SUCC(ret)) {
        if (!est_cost_info.pushdown_prefix_filters_.empty()) {
          is_nl_with_extended_range = true;
        }
        LOG_TRACE("succeed to classify filters",
            K(est_cost_info.prefix_filters_),
            K(est_cost_info.pushdown_prefix_filters_),
            K(est_cost_info.postfix_filters_),
            K(est_cost_info.table_filters_),
            K(is_nl_with_extended_range));
      }
    }
  }
  return ret;
}

int ObJoinOrder::can_extract_unprecise_range(
    const uint64_t table_id, const ObRawExpr* filter, const ObBitSet<>& ex_prefix_column_bs, bool& can_extract)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* l_expr = NULL;
  const ObRawExpr* r_expr = NULL;
  const ObRawExpr* exec_param = NULL;
  const ObColumnRefRawExpr* column = NULL;
  can_extract = false;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (T_OP_EQ != filter->get_expr_type() && T_OP_NSEQ != filter->get_expr_type()) {
    /*do nothing*/
  } else if (OB_ISNULL(l_expr = filter->get_param_expr(0)) || OB_ISNULL(r_expr = filter->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (l_expr->has_flag(IS_EXEC_PARAM) && r_expr->is_column_ref_expr()) {
    column = static_cast<const ObColumnRefRawExpr*>(r_expr);
    exec_param = l_expr;
  } else if (l_expr->is_column_ref_expr() && r_expr->has_flag(IS_EXEC_PARAM)) {
    column = static_cast<const ObColumnRefRawExpr*>(l_expr);
    exec_param = r_expr;
  }

  if (OB_SUCC(ret) && NULL != column && NULL != exec_param) {
    ObObjType column_type = column->get_result_type().get_type();
    ObObjType exec_param_type = exec_param->get_result_type().get_type();
    if (column->get_table_id() != table_id || !ex_prefix_column_bs.has_member(column->get_column_id())) {
      /*do nothing*/
    } else if ((ObCharType == column_type && ObVarcharType == exec_param_type) ||
               (ObNCharType == column_type && ObNVarchar2Type == exec_param_type)) {
      can_extract = true;
    } else {
      can_extract = false;
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_meta_info(const uint64_t table_id, const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  common::ObStatManager* stat_manager = NULL;
  ObArray<ObAddr> dummy_addr_list;
  const ObPhyPartitionLocationInfoIArray& part_loc_info_array =
      table_partition_info_.get_phy_tbl_location_info().get_phy_part_loc_info_list();
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
             OB_ISNULL(stat_manager = OPT_CTX.get_stat_manager()) || OB_UNLIKELY(part_loc_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nulll point error", K(schema_guard), K(get_plan()), K(stat_manager), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else if (OPT_CTX.use_default_stat()) {
    if (OB_FAIL(part_loc_info_array.at(0).get_partition_location().get_partition_key(
            table_meta_info_.table_est_part_.pkey_))) {
      LOG_WARN("failed to get partition key", K(ret));
    }
  } else if (OB_FAIL(ObSQLUtils::choose_best_partition_for_estimation(part_loc_info_array,
                 OPT_CTX.get_partition_service(),
                 *stat_manager,
                 OPT_CTX.get_local_server_addr(),
                 dummy_addr_list,
                 !can_use_remote_estimate(table_opt_info_.optimization_method_),
                 table_meta_info_.table_est_part_))) {
    LOG_WARN("Failed to Find local partition key", K(ret));
  }

  if (OB_SUCC(ret)) {
    table_meta_info_.ref_table_id_ = ref_table_id;
    table_meta_info_.table_rowkey_count_ = table_schema->get_rowkey_info().get_size();
    table_meta_info_.table_column_count_ = table_schema->get_column_count();
    table_meta_info_.micro_block_size_ = table_schema->get_block_size();
    table_meta_info_.part_count_ =
        table_partition_info_.get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
    table_meta_info_.schema_version_ = table_schema->get_schema_version();
    LOG_TRACE("after compute table meta info", K(table_meta_info_));
  }
  return ret;
}

// find minimal cost path among all the paths
int ObJoinOrder::find_minimal_cost_path(const ObIArray<sql::Path*>& all_paths, sql::Path*& minimal_cost_path)
{
  int ret = OB_SUCCESS;
  minimal_cost_path = NULL;
  int64_t N = all_paths.count();
  if (N > 0) {
    minimal_cost_path = all_paths.at(0);
    if (OB_ISNULL(minimal_cost_path)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("point is null", K(ret));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < N; ++i) {
        if (OB_ISNULL(all_paths.at(i))) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("point is null", K(i), K(ret));
        } else if (minimal_cost_path->cost_ > all_paths.at(i)->cost_) {
          minimal_cost_path = all_paths.at(i);
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::push_down_order_siblings(JoinPath* join_path, const sql::Path* right_path)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_plan()->get_stmt());
  if (OB_ISNULL(join_path) || OB_ISNULL(right_path) || OB_ISNULL(stmt) || false == stmt->is_hierarchical_query()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hierarchical query expected", K(ret));
  } else if (stmt->is_order_siblings()) {
    bool right_need_sort = true;
    if (OB_FAIL((ObOptimizerUtil::check_need_sort(stmt->get_order_items(),
            right_path->ordering_,
            right_path->parent_->get_fd_item_set(),
            right_path->parent_->get_ordering_output_equal_sets(),
            right_path->parent_->get_const_exprs(),
            right_path->parent_->get_is_at_most_one_row(),
            right_need_sort)))) {
      LOG_WARN("check need sort", K(ret));
    } else if (right_need_sort) {
      join_path->right_need_sort_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_order_item_size(); ++i) {
        if (OB_FAIL(join_path->right_expected_ordering_.push_back(stmt->get_order_item(i)))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::create_and_add_nl_path(const sql::Path* left_path, const sql::Path* right_path,
    const ObJoinType join_type, const common::ObIArray<ObRawExpr*>& on_condition,
    const common::ObIArray<ObRawExpr*>& where_condition, bool need_mat)
{
  int ret = OB_SUCCESS;
  JoinPath* join_path = NULL;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_path), K(right_path), K(get_plan()), K(ret));
  } else if (OB_ISNULL(join_path = static_cast<JoinPath*>(allocator_->alloc(sizeof(JoinPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc a join path", K(ret));
  } else {
    join_path = new (join_path) JoinPath(this, left_path, right_path, NESTED_LOOP_JOIN, join_type, need_mat);
    join_path->set_interesting_order_info(left_path->get_interesting_order_info());
    if (OB_FAIL(append(join_path->ordering_, left_path->ordering_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(check_join_interesting_order(join_path))) {
      LOG_WARN("failed to update join interesting order info", K(ret));
    } else if (OB_FAIL(set_nl_filters(join_path, right_path, join_type, on_condition, where_condition))) {
      LOG_WARN("failed to remove filters", K(ret));
    } else if (CONNECT_BY_JOIN == join_type && OB_FAIL(push_down_order_siblings(join_path, right_path))) {
      LOG_WARN("push down order siblings by condition failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(join_path->estimate_cost())) {
      LOG_WARN("failed to calculate cost in create_nl_path", K(ret));
    } else if (OB_FAIL(add_path(join_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::init_est_sel_info_for_access_path(const uint64_t table_id, const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  common::ObStatManager* stat_manager = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(stat_manager = OPT_CTX.get_stat_manager()) ||
             OB_UNLIKELY(!table_meta_info_.table_est_part_.pkey_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(get_plan()), K(stat_manager), K(ret), K(table_meta_info_.table_est_part_));
  } else {
    int64_t table_rows = 0;
    const ObPartitionKey& est_pkey = table_meta_info_.table_est_part_.pkey_;
    // sstable
    if (!is_virtual_table(ref_table_id) || is_oracle_mapping_real_virtual_table(ref_table_id)) {
      table_rows = table_meta_info_.table_row_count_;
    }
    // default
    if (OB_SUCC(ret) && 0 == table_rows) {
      ObTableStat tstat = ObStatManager::get_default_table_stat();
      table_rows = tstat.get_row_count();
    }

    ObSEArray<int64_t, 64> all_used_part_id;
    const ObPhyPartitionLocationInfoIArray& part_loc_info_array =
        table_partition_info_.get_phy_tbl_location_info().get_phy_part_loc_info_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
      const ObOptPartLoc& part_loc = part_loc_info_array.at(i).get_partition_location();
      ObPartitionKey key;
      if (OB_FAIL(part_loc.get_partition_key(key))) {
        LOG_WARN("fail to get partition key", K(ret));
      } else if (OB_FAIL(all_used_part_id.push_back(key.get_partition_id()))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_plan()->get_est_sel_info().add_table_for_table_stat(
              table_id, est_pkey, static_cast<double>(table_rows), all_used_part_id))) {
        LOG_WARN("Failed to add table to est sel info", K(ret));
      }
    }
  }

  return ret;
}

int ObJoinOrder::init_est_sel_info_for_subplan_scan(const SubQueryPath* path)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child_top = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(path) || OB_ISNULL(child_top = path->root_) ||
      OB_ISNULL(child_top->get_est_sel_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(get_plan()), K(path), K(child_top));
  } else if (OB_FAIL(child_top->get_est_sel_info()->update_table_stats(child_top))) {
    LOG_WARN("failed to update table statics", K(ret));
  } else if (OB_FAIL(get_plan()->get_est_sel_info().rename_statics(
                 path->subquery_id_, *child_top->get_est_sel_info(), child_top->get_card()))) {
    LOG_WARN("Failed to rename from child", K(ret));
  }
  return ret;
}

int ObJoinOrder::merge_conflict_detectors(
    ObJoinOrder* left_tree, ObJoinOrder* right_tree, const common::ObIArray<ConflictDetector*>& detectors)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null join order", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_, left_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_, right_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_, detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_and_remove_is_null_qual(const ObJoinType join_type, const ObRelIds& left_ids,
    const ObRelIds& right_ids, const ObIArray<ObRawExpr*>& quals, ObIArray<ObRawExpr*>& normal_quals,
    bool& left_has_is_null_qual, bool& right_has_is_null_qual)
{
  int ret = OB_SUCCESS;
  left_has_is_null_qual = false;
  right_has_is_null_qual = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObEstSelInfo& est_sel_info = get_plan()->get_est_sel_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
      bool is_null_qual = false;
      if (LEFT_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(est_sel_info, right_ids, quals.at(i), is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          right_has_is_null_qual = true;
        } else if (OB_FAIL(normal_quals.push_back(quals.at(i)))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(est_sel_info, left_ids, quals.at(i), is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          left_has_is_null_qual = true;
        } else if (OB_FAIL(normal_quals.push_back(quals.at(i)))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      } else if (FULL_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(est_sel_info, left_ids, quals.at(i), is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          left_has_is_null_qual = true;
        } else if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(est_sel_info, right_ids, quals.at(i), is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          right_has_is_null_qual = true;
        } else if (OB_FAIL(normal_quals.push_back(quals.at(i)))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::calc_join_output_rows(
    const ObJoinOrder& left_tree, const ObJoinOrder& right_tree, const ObJoinType join_type, double& new_rows)
{
  int ret = OB_SUCCESS;
  double selectivity = 0;
  const ObRelIds& left_ids = left_tree.get_tables();
  const ObRelIds& right_ids = right_tree.get_tables();

  if (OB_ISNULL(get_plan()) || OB_ISNULL(join_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (INNER_JOIN == join_type) {
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
            join_info_->where_condition_,
            selectivity,
            &get_plan()->get_predicate_selectivities(),
            join_type,
            &left_ids,
            &right_ids,
            left_tree.output_rows_,
            right_tree.output_rows_))) {
      LOG_WARN("Failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
    } else {
      new_rows = left_tree.output_rows_ * right_tree.output_rows_ * selectivity;
    }
  } else if (IS_OUTER_JOIN(join_type)) {
    double oj_qual_sel = 1.0;
    double oj_filter_sel = 1.0;
    bool left_has_is_null = false;
    bool right_has_is_null = false;
    ObSEArray<ObRawExpr*, 8> normal_quals;
    if (OB_FAIL(check_and_remove_is_null_qual(join_type,
            left_ids,
            right_ids,
            join_info_->where_condition_,
            normal_quals,
            left_has_is_null,
            right_has_is_null))) {
      LOG_WARN("failed to check and remove is null qual", K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   normal_quals,
                   oj_qual_sel,
                   &get_plan()->get_predicate_selectivities(),
                   join_type,
                   &left_ids,
                   &right_ids,
                   left_tree.output_rows_,
                   right_tree.output_rows_))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info_->where_condition_), K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   join_info_->on_condition_,
                   oj_filter_sel,
                   &get_plan()->get_predicate_selectivities(),
                   join_type,
                   &left_ids,
                   &right_ids,
                   left_tree.output_rows_,
                   right_tree.output_rows_))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info_->on_condition_), K(ret));
    } else {
      oj_filter_sel = ObOptEstSel::revise_between_0_1(oj_filter_sel);
      new_rows = left_tree.output_rows_ * right_tree.output_rows_ * oj_filter_sel;
      switch (join_type) {
        case LEFT_OUTER_JOIN:
          if (right_has_is_null) {
            new_rows = left_tree.output_rows_ * (1.0 - oj_filter_sel);
          } else {
            new_rows = new_rows < left_tree.output_rows_ ? left_tree.output_rows_ : new_rows;
          }
          break;
        case RIGHT_OUTER_JOIN:
          if (left_has_is_null) {
            new_rows = right_tree.output_rows_ * (1.0 - oj_filter_sel);
          } else {
            new_rows = new_rows < right_tree.output_rows_ ? right_tree.output_rows_ : new_rows;
          }
          break;
        case FULL_OUTER_JOIN:
          if (left_has_is_null && right_has_is_null) {
            new_rows = (left_tree.output_rows_ + right_tree.output_rows_) * (1.0 - oj_filter_sel);
          } else if (left_has_is_null) {
            new_rows = right_tree.output_rows_ * (1.0 - oj_filter_sel);
          } else if (right_has_is_null) {
            new_rows = left_tree.output_rows_ * (1.0 - oj_filter_sel);
          } else {
            new_rows = new_rows < left_tree.output_rows_ ? left_tree.output_rows_ : new_rows;
            new_rows = new_rows < right_tree.output_rows_ ? right_tree.output_rows_ : new_rows;
          }
          break;
        default:
          break;
      }
      new_rows = new_rows * oj_qual_sel;
    }
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
            join_info_->where_condition_,
            selectivity,
            &get_plan()->get_predicate_selectivities(),
            join_type,
            &left_ids,
            &right_ids,
            left_tree.output_rows_,
            right_tree.output_rows_))) {
      LOG_WARN("Failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
    } else {
      double outer_rows = IS_LEFT_SEMI_ANTI_JOIN(join_type) ? left_tree.output_rows_ : right_tree.output_rows_;
      anti_or_semi_match_sel_ = ObOptEstSel::revise_between_0_1(selectivity);
      if (LEFT_SEMI_JOIN == join_type || RIGHT_SEMI_JOIN == join_type) {
        new_rows = outer_rows * anti_or_semi_match_sel_;
      } else {
        new_rows = outer_rows - outer_rows * anti_or_semi_match_sel_;
        if (LEFT_ANTI_JOIN == join_type && std::fabs(right_tree.output_rows_) < OB_DOUBLE_EPSINON) {
          new_rows = left_tree.output_rows_;
        } else if (RIGHT_ANTI_JOIN == join_type && std::fabs(left_tree.output_rows_) < OB_DOUBLE_EPSINON) {
          new_rows = right_tree.output_rows_;
        }
      }
    }
  } else if (CONNECT_BY_JOIN == join_type) {
    double join_qual_sel = 1.0;
    double join_filter_sel = 1.0;
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
            join_info_->where_condition_,
            join_qual_sel,
            &get_plan()->get_predicate_selectivities(),
            join_type,
            &left_ids,
            &right_ids,
            left_tree.output_rows_,
            right_tree.output_rows_))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info_->where_condition_), K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(get_plan()->get_est_sel_info(),
                   join_info_->on_condition_,
                   join_filter_sel,
                   &get_plan()->get_predicate_selectivities(),
                   join_type,
                   &left_ids,
                   &right_ids,
                   left_tree.output_rows_,
                   right_tree.output_rows_))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info_->on_condition_), K(ret));
    } else {
      double connect_by_selectivity = 0.0;
      if (join_filter_sel < 0 || join_filter_sel >= 1) {
        connect_by_selectivity = 1.0;
      } else {
        connect_by_selectivity = 1.0 / (1.0 - join_filter_sel);
      }
      new_rows = left_tree.output_rows_ +
                 left_tree.output_rows_ * right_tree.output_rows_ * join_filter_sel * connect_by_selectivity;
      new_rows = new_rows * join_qual_sel;
      selectivity = connect_by_selectivity;
    }
  }
  LOG_TRACE("estimate join size and width",
      K(left_tree.output_rows_),
      K(right_tree.output_rows_),
      K(selectivity),
      K(new_rows));
  return ret;
}

int ObJoinOrder::construct_scan_range_batch(
    const ObIArray<ObNewRange>& scan_ranges, ObSimpleBatch& batch, const AccessPath* ap, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(allocator_));
  } else if (scan_ranges.count() < 1) {
    batch.type_ = ObSimpleBatch::T_NONE;
  } else if (scan_ranges.count() == 1) {  // T_SCAN
    void* ptr = allocator_->alloc(sizeof(SQLScanRange));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      SQLScanRange* range = new (ptr) SQLScanRange();
      *range = scan_ranges.at(0);
      batch.type_ = ObSimpleBatch::T_SCAN;
      batch.range_ = range;
    }
  } else {  // T_MULTI_SCAN
    SQLScanRangeArray* range_array = NULL;
    void* ptr = allocator_->alloc(sizeof(SQLScanRangeArray));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      range_array = new (ptr) SQLScanRangeArray();
    }

    if (OB_SUCC(ret)) {
      // get_valid_scan_range will call get_partition_ids_by_ranges, which is not supported when
      // using rowid to extract partition ids, so here we just get first 5 range to estimate row
      // count. This may lead to zero row count estimation
      bool is_mock_index = ObSQLMockSchemaUtils::is_mock_index(ap->index_id_);
      if (is_mock_index || scan_ranges.count() <= ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM) {
        if (OB_FAIL(get_first_n_scan_range(
                scan_ranges, *range_array, MIN(scan_ranges.count(), ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM)))) {
          LOG_WARN("failed to get first n scan range", K(ret));
        }
      } else if (OB_FAIL(get_valid_scan_range(scan_ranges, *range_array, ap, pkey))) {
        LOG_WARN("failed to get valid scan range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = range_array;
      if (OB_UNLIKELY(range_array->count() == 0)) {
        LOG_WARN("range array should have as least one range", K(ret));
        if (OB_FAIL(
                get_first_n_scan_range(scan_ranges, *range_array, ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM))) {
          LOG_WARN("failed to get first n scan range", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::increase_diverse_path_count(AccessPath* ap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is null", K(ret));
  } else if (NULL == ap->pre_query_range_ || (ap->get_cost_table_scan_info().ranges_.count() == 1 &&
                                                 ap->get_cost_table_scan_info().ranges_.at(0).is_whole_range())) {
    // ap is whole range
  } else {
    // ap has query ranges
    ++diverse_path_count_;
  }
  return ret;
}

int ObJoinOrder::deduce_const_exprs_and_ft_item_set()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is null", K(ret));
  } else if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    ObFdItemFactory& fd_item_factory = get_plan()->get_fd_item_factory();
    ret = fd_item_factory.deduce_fd_item_set(
        get_ordering_output_equal_sets(), column_exprs, get_const_exprs(), get_fd_item_set());
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_table_scan(
    const uint64_t table_id, const uint64_t table_ref_id, const ObIArray<ObRawExpr*>& quals)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObDMLStmt* stmt = NULL;
  uint64_t index_tids[OB_MAX_INDEX_PER_TABLE];
  int64_t index_count = OB_MAX_INDEX_PER_TABLE;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(stmt), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(
                 table_ref_id, index_tids, index_count, false, true /*global index*/, false /*domain index*/))) {
    LOG_WARN("failed to get can read index", K(ret), K(table_ref_id));
  }
  for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
    const ObTableSchema* index_schema = NULL;
    uint64_t index_id = (i == -1 ? table_ref_id : index_tids[i]);
    if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null index schema", K(ret));
    } else if (-1 != i && !index_schema->is_unique_index()) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::try_add_fd_item(stmt,
                   get_plan()->get_fd_item_factory(),
                   table_id,
                   get_tables(),
                   index_schema,
                   quals,
                   not_null_columns_,
                   fd_item_set_,
                   candi_fd_item_set_))) {
      LOG_WARN("failed to try add fd item", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("base table fd item set", K(fd_item_set_));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_join(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (INNER_JOIN == join_type) {
    ret = compute_fd_item_set_for_inner_join(left_tree, right_tree, join_info);
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    ret = compute_fd_item_set_for_semi_anti_join(left_tree, right_tree, join_info, join_type);
  } else {
    ret = compute_fd_item_set_for_outer_join(left_tree, right_tree, join_info, join_type);
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("join fd item set", K(fd_item_set_));
  } else {
    LOG_WARN("failed to compute fd item set for join", K(join_type));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_inner_join(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info)
{
  int ret = OB_SUCCESS;
  bool left_is_unique = false;
  bool right_is_unique = false;
  ObDMLStmt* stmt = NULL;
  ObSEArray<ObFdItem*, 8> left_fd_item_set;
  ObSEArray<ObFdItem*, 8> left_candi_fd_item_set;
  ObSEArray<ObFdItem*, 8> right_fd_item_set;
  ObSEArray<ObFdItem*, 8> right_candi_fd_item_set;
  ObSEArray<ObRawExpr*, 8> left_not_null;
  ObSEArray<ObRawExpr*, 8> right_not_null;
  ObSEArray<ObRawExpr*, 4> left_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr*, 4> right_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_tree), K(right_tree), K(get_plan()), K(stmt));
  } else if (OB_FAIL(left_fd_item_set.assign(left_tree->fd_item_set_)) ||
             OB_FAIL(left_candi_fd_item_set.assign(left_tree->candi_fd_item_set_)) ||
             OB_FAIL(right_fd_item_set.assign(right_tree->fd_item_set_)) ||
             OB_FAIL(right_candi_fd_item_set.assign(right_tree->candi_fd_item_set_)) ||
             OB_FAIL(left_not_null.assign(left_tree->not_null_columns_)) ||
             OB_FAIL(right_not_null.assign(right_tree->not_null_columns_)) ||
             (NULL != join_info && OB_FAIL(join_conditions.assign(join_info->equal_join_condition_)))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (NULL != join_info &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 join_info->where_condition_, left_candi_fd_item_set, left_fd_item_set, left_not_null))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (NULL != join_info &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 join_info->where_condition_, right_candi_fd_item_set, right_fd_item_set, right_not_null))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(append(not_null_columns_, left_not_null)) || OB_FAIL(append(not_null_columns_, right_not_null))) {
    LOG_WARN("failed to append not null columns", K(ret));
  } else if (NULL != join_info && OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(join_conditions,
                                      left_tree->get_tables(),
                                      right_tree->get_tables(),
                                      left_join_exprs,
                                      right_join_exprs,
                                      all_left_join_exprs,
                                      all_right_join_exprs))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(get_plan()->get_fd_item_factory(),
                 right_tree->get_output_tables(),
                 right_join_exprs,
                 right_tree->get_const_exprs(),
                 right_tree->get_ordering_output_equal_sets(),
                 right_fd_item_set,
                 all_left_join_exprs,
                 left_tree->get_ordering_output_equal_sets(),
                 left_fd_item_set,
                 left_candi_fd_item_set,
                 fd_item_set_,
                 candi_fd_item_set_))) {
    LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(get_plan()->get_fd_item_factory(),
                 left_tree->get_output_tables(),
                 left_join_exprs,
                 left_tree->get_const_exprs(),
                 left_tree->get_ordering_output_equal_sets(),
                 left_fd_item_set,
                 all_right_join_exprs,
                 right_tree->get_ordering_output_equal_sets(),
                 right_fd_item_set,
                 right_candi_fd_item_set,
                 fd_item_set_,
                 candi_fd_item_set_))) {
    LOG_WARN("failed to add right fd_item_set for inner join", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  } else {
    LOG_TRACE("inner join fd item set", K(fd_item_set_));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_semi_anti_join(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || !IS_SEMI_ANTI_JOIN(join_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(left_tree), K(right_tree), K(join_type));
  } else if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
    if (OB_FAIL(fd_item_set_.assign(left_tree->fd_item_set_)) ||
        OB_FAIL(candi_fd_item_set_.assign(left_tree->candi_fd_item_set_)) ||
        OB_FAIL(not_null_columns_.assign(left_tree->not_null_columns_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (IS_ANTI_JOIN(join_type)) {
    } else if (NULL != join_info &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                   join_info->where_condition_, candi_fd_item_set_, fd_item_set_, not_null_columns_))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    } else {
      LOG_TRACE("semi join fd item set", K(fd_item_set_));
    }
  } else if (OB_FAIL(compute_fd_item_set_for_semi_anti_join(
                 right_tree, left_tree, join_info, get_opposite_join_type(join_type)))) {
    LOG_WARN("failed to compute fd item set for right semi anti join", K(ret));
  }

  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_outer_join(
    const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info, const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info) ||
      !IS_OUTER_OR_CONNECT_BY_JOIN(join_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(left_tree), K(right_tree));
  } else if (CONNECT_BY_JOIN == join_type) {
    // do nothing
  } else if (FULL_OUTER_JOIN == join_type) {
    // do nothing
  } else if (LEFT_OUTER_JOIN == join_type) {
    ObSEArray<ObRawExpr*, 8> right_not_null;
    ObSEArray<ObFdItem*, 8> right_fd_item_set;
    ObSEArray<ObFdItem*, 8> right_candi_fd_item_set;
    ObSEArray<ObRawExpr*, 4> left_join_exprs;
    ObSEArray<ObRawExpr*, 4> right_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_right_join_exprs;

    if (OB_FAIL(right_fd_item_set.assign(right_tree->fd_item_set_)) ||
        OB_FAIL(right_candi_fd_item_set.assign(right_tree->candi_fd_item_set_)) ||
        OB_FAIL(right_not_null.assign(right_tree->not_null_columns_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                   join_info->on_condition_, right_candi_fd_item_set, right_fd_item_set, right_not_null))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else if (OB_FAIL(not_null_columns_.assign(left_tree->not_null_columns_))) {
      LOG_WARN("failed to assign not null columns", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(join_info->equal_join_condition_,
                   left_tree->get_tables(),
                   right_tree->get_tables(),
                   left_join_exprs,
                   right_join_exprs,
                   all_left_join_exprs,
                   all_right_join_exprs))) {
      LOG_WARN("failed to get type safe join exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(get_plan()->get_fd_item_factory(),
                   right_tree->get_output_tables(),
                   right_join_exprs,
                   right_tree->get_const_exprs(),
                   right_tree->get_ordering_output_equal_sets(),
                   right_fd_item_set,
                   all_left_join_exprs,
                   left_tree->get_ordering_output_equal_sets(),
                   left_tree->fd_item_set_,
                   left_tree->candi_fd_item_set_,
                   fd_item_set_,
                   candi_fd_item_set_))) {
      LOG_WARN("failed to add left fd_item_set for left outer join", K(ret));
    } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    }
  } else if (OB_FAIL(compute_fd_item_set_for_outer_join(right_tree, left_tree, join_info, LEFT_OUTER_JOIN))) {
    LOG_WARN("failed to compute fd item set for right outer join", K(ret));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_subplan_scan(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
    const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt,
    const ObFdItemSet& input_fd_item_sets, ObFdItemSet& output_fd_item_sets)
{
  int ret = OB_SUCCESS;
  ObFdItemFactory& fd_factory = get_plan()->get_fd_item_factory();
  ObSEArray<ObRawExpr*, 8> new_parent_exprs;
  int32_t stmt_level = parent_stmt.get_current_level();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(fd_factory,
                 expr_factory,
                 equal_sets,
                 table_id,
                 parent_stmt,
                 child_stmt,
                 input_fd_item_sets,
                 output_fd_item_sets))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  } else {
    LOG_TRACE("subplan scan fd item set", K(output_fd_item_sets));
  }
  return ret;
}

int ObJoinOrder::compute_one_row_info_for_table_scan(ObIArray<AccessPath*>& access_paths)
{
  int ret = OB_SUCCESS;
  AccessPath* access_path = NULL;
  is_at_most_one_row_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_at_most_one_row_ && i < access_paths.count(); i++) {
    bool is_get = false;
    if (OB_ISNULL(access_path = access_paths.at(i)) || OB_ISNULL(access_path->pre_query_range_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (access_path->is_inner_path_) {
      /*do nothing*/
    } else if (OB_FAIL(access_path->pre_query_range_->is_get(is_get))) {
      LOG_WARN("failed to check if query range is get", K(ret));
    } else if (is_get && (1 == access_path->est_cost_info_.ranges_.count())) {
      is_at_most_one_row_ = true;
    } else { /*do nothing*/
    }
  }
  LOG_TRACE("succeed to compute one row info for table scan", K(is_at_most_one_row_), K(table_id_));
  return ret;
}

int ObJoinOrder::compute_one_row_info_for_join(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
    const ObIArray<ObRawExpr*>& join_condition, const ObIArray<ObRawExpr*>& equal_join_condition,
    const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  bool left_is_unique = false;
  bool right_is_unique = false;
  ObSEArray<ObFdItem*, 8> left_fd_item_set;
  ObSEArray<ObFdItem*, 8> left_candi_fd_item_set;
  ObSEArray<ObFdItem*, 8> right_fd_item_set;
  ObSEArray<ObFdItem*, 8> right_candi_fd_item_set;
  ObSEArray<ObRawExpr*, 8> left_not_null;
  ObSEArray<ObRawExpr*, 8> right_not_null;
  ObSEArray<ObRawExpr*, 4> left_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr*, 4> right_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
  is_at_most_one_row_ = false;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (CONNECT_BY_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
    // do nothing
  } else if (!left_tree->get_is_at_most_one_row() && !right_tree->get_is_at_most_one_row()) {
    /*do nothing*/
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
      is_at_most_one_row_ = left_tree->get_is_at_most_one_row();
    } else {
      is_at_most_one_row_ = right_tree->get_is_at_most_one_row();
    }
  } else if (left_tree->get_is_at_most_one_row() && right_tree->get_is_at_most_one_row()) {
    is_at_most_one_row_ = true;
  } else if (equal_join_condition.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(left_fd_item_set.assign(left_tree->fd_item_set_)) ||
             OB_FAIL(left_candi_fd_item_set.assign(left_tree->candi_fd_item_set_)) ||
             OB_FAIL(right_fd_item_set.assign(right_tree->fd_item_set_)) ||
             OB_FAIL(right_candi_fd_item_set.assign(right_tree->candi_fd_item_set_)) ||
             OB_FAIL(left_not_null.assign(left_tree->not_null_columns_)) ||
             OB_FAIL(right_not_null.assign(right_tree->not_null_columns_))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 join_condition, left_candi_fd_item_set, left_fd_item_set, left_not_null))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(
                 join_condition, right_candi_fd_item_set, right_fd_item_set, right_not_null))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(equal_join_condition,
                 left_tree->get_tables(),
                 right_tree->get_tables(),
                 left_join_exprs,
                 right_join_exprs,
                 all_left_join_exprs,
                 all_right_join_exprs))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (left_tree->get_is_at_most_one_row() && (join_type == INNER_JOIN || join_type == LEFT_OUTER_JOIN)) {
    if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(right_join_exprs,
            right_tree->get_tables(),
            right_fd_item_set,
            right_tree->get_ordering_output_equal_sets(),
            right_tree->get_const_exprs(),
            right_is_unique))) {
      LOG_WARN("failed to check is order unique", K(ret));
    } else {
      is_at_most_one_row_ = right_is_unique;
    }
  } else if (right_tree->get_is_at_most_one_row() && (join_type == INNER_JOIN || join_type == RIGHT_OUTER_JOIN)) {
    if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_join_exprs,
            left_tree->get_tables(),
            left_fd_item_set,
            left_tree->get_ordering_output_equal_sets(),
            left_tree->get_const_exprs(),
            left_is_unique))) {
      LOG_WARN("failed to check is order unique", K(ret));
    } else {
      is_at_most_one_row_ = left_is_unique;
    }
  } else { /*do nothing*/
  }

  LOG_TRACE("succeed to compute one row info for join", K(is_at_most_one_row_));
  return ret;
}

int ObJoinOrder::check_match_remote_sharding(const sql::Path* path, const ObAddr& server, bool& is_match) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret));
  } else if (path->path_type_ == ACCESS) {
    const AccessPath* access_path = static_cast<const AccessPath*>(path);
    const ObPhyTableLocationInfo* phy_loc_info = NULL;
    if (OB_ISNULL(access_path->get_sharding_info()) ||
        OB_ISNULL(phy_loc_info = access_path->get_sharding_info()->get_phy_table_location_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(access_path->get_sharding_info()), K(phy_loc_info), K(ret));
    } else if (OB_FAIL(ObLogicalOperator::check_match_server_addr(*phy_loc_info, server, is_match))) {
      LOG_WARN("failed to check match server addr", K(ret));
    } else { /*do nothing*/
    }
  } else if (path->path_type_ == JOIN) {
    if (OB_FAIL(check_match_remote_sharding(static_cast<const JoinPath*>(path)->left_path_, server, is_match))) {
      LOG_WARN("failed to check has global index", K(ret));
    } else if (!is_match) {
      // do nothing
    } else if (OB_FAIL(
                   check_match_remote_sharding(static_cast<const JoinPath*>(path)->right_path_, server, is_match))) {
      LOG_WARN("failed to check has global index", K(ret));
    }
  } else if (path->path_type_ == SUBQUERY) {
    ObLogicalOperator* op = static_cast<const SubQueryPath*>(path)->root_;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is null", K(ret));
    } else if (OB_FAIL(op->check_match_remote_sharding(server, is_match))) {
      LOG_WARN("failed to check has global index", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::get_first_n_scan_range(
    const ObIArray<ObNewRange>& scan_ranges, ObIArray<ObNewRange>& valid_ranges, int64_t range_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_count > scan_ranges.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range count can not large than scan ranges count", K(ret), K(range_count), K(scan_ranges.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_count; ++i) {
    if (OB_FAIL(valid_ranges.push_back(scan_ranges.at(i)))) {
      LOG_WARN("failed to push back array", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::get_valid_scan_range(const ObIArray<ObNewRange>& scan_ranges, ObIArray<ObNewRange>& valid_ranges,
    const AccessPath* ap, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObTablePartitionInfo* table_partition_info = NULL;
  int64_t best_part_id = pkey.get_partition_id();
  if (OB_ISNULL(ap)) {
    LOG_WARN("get unexpected null", K(ret), K(ap));
  } else if (!ap->is_global_index_) {
    table_partition_info = &table_partition_info_;
  } else if (OB_ISNULL(ap->table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table partition info", K(ret));
  } else {
    table_partition_info = ap->table_partition_info_;
  }

  if (OB_SUCC(ret)) {
    ObTableLocation& table_location = table_partition_info->get_table_location();
    share::schema::ObPartitionLevel part_level = table_location.get_part_level();
    ObSEArray<int64_t, 4> part_projector;
    ObSEArray<int64_t, 4> sub_part_projector;
    ObSEArray<int64_t, 4> gen_projector;
    ObSEArray<int64_t, 4> sub_gen_projector;
    if (OB_UNLIKELY(share::schema::PARTITION_LEVEL_MAX == part_level)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected part level.", K(ret));
    } else if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
      if (OB_FAIL(get_first_n_scan_range(scan_ranges, valid_ranges, ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM))) {
        LOG_WARN("failed to get first n scan range", K(ret));
      }
    } else if (OB_FAIL(get_range_projector(
                   ap, part_level, part_projector, sub_part_projector, gen_projector, sub_gen_projector))) {
      LOG_WARN("failed to get range projector", K(ret));
    } else if ((share::schema::PARTITION_LEVEL_ONE == part_level && part_projector.empty() && gen_projector.empty()) ||
               (share::schema::PARTITION_LEVEL_TWO == part_level && part_projector.empty() && gen_projector.empty() &&
                   sub_part_projector.empty() && sub_gen_projector.empty())) {
      if (OB_FAIL(get_first_n_scan_range(scan_ranges, valid_ranges, ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM))) {
        LOG_WARN("failed to get first n scan range", K(ret));
      }
    } else if (share::schema::PARTITION_LEVEL_ONE == part_level) {
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count() &&
                          valid_ranges.count() < ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
           ++i) {
        ObSEArray<int64_t, 8> part_ids;
        if (OB_FAIL(get_scan_range_partitions(
                scan_ranges.at(i), part_projector, gen_projector, table_location, part_ids))) {
          LOG_WARN("failed to get scan range partitions", K(ret));
        } else if (!ObOptimizerUtil::find_item(part_ids, best_part_id)) {
          // do nothing
        } else if (OB_FAIL(valid_ranges.push_back(scan_ranges.at(i)))) {
          LOG_WARN("failed to push back scan range", K(ret));
        }
      }
    } else {
      bool level_one_projector_empty = part_projector.empty() && gen_projector.empty();
      bool level_two_projector_empty = sub_part_projector.empty() && sub_gen_projector.empty();
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count() &&
                          valid_ranges.count() < ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
           ++i) {
        ObSEArray<int64_t, 8> part_ids;
        ObSEArray<int64_t, 8> sub_part_ids;
        bool is_valid = false;
        if (level_one_projector_empty) {
          if (OB_FAIL(part_ids.push_back(extract_part_idx(best_part_id)))) {
            LOG_WARN("failed to push back part id", K(ret));
          }
        } else if (OB_FAIL(get_scan_range_partitions(
                       scan_ranges.at(i), part_projector, gen_projector, table_location, part_ids))) {
          LOG_WARN("failed to get scan range partitions", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (level_two_projector_empty) {
            // query range only match level one partition columns, check if sub partition id belong
            // to any level one partitions
            if (ObOptimizerUtil::find_item(part_ids, extract_part_idx(best_part_id))) {
              is_valid = true;
            }
          } else if (OB_FAIL(get_scan_range_partitions(scan_ranges.at(i),
                         sub_part_projector,
                         sub_gen_projector,
                         table_location,
                         sub_part_ids,
                         &part_ids))) {
            LOG_WARN("failed to get scan range partitions", K(ret));
          } else if (ObOptimizerUtil::find_item(sub_part_ids, best_part_id)) {
            is_valid = true;
          }
        }

        if (OB_SUCC(ret)) {
          if (is_valid && OB_FAIL(valid_ranges.push_back(scan_ranges.at(i)))) {
            LOG_WARN("failed to push back scan range", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_range_projector(const AccessPath* ap, const share::schema::ObPartitionLevel part_level,
    ObIArray<int64_t>& part_projector, ObIArray<int64_t>& sub_part_projector, ObIArray<int64_t>& gen_projector,
    ObIArray<int64_t>& sub_gen_projector)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 4> part_columns;
  ObSEArray<ColumnItem, 4> gen_columns;
  ObSEArray<ColumnItem, 4> sub_part_columns;
  ObSEArray<ColumnItem, 4> sub_gen_columns;
  ObSEArray<ObRawExpr*, 8> range_exprs;
  bool all_find = true;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(ap) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ap), K(get_plan()), K(stmt));
  } else {
    uint64_t ref_table_id = ap->is_global_index_ ? ap->get_index_table_id() : ap->get_ref_table_id();
    if (OB_FAIL(get_partition_columns(
            *stmt, ap->get_table_id(), ref_table_id, share::schema::PARTITION_LEVEL_ONE, part_columns, gen_columns))) {
      LOG_WARN("failed to get partition columns", K(ret));
    } else if (share::schema::PARTITION_LEVEL_TWO == part_level && OB_FAIL(get_partition_columns(*stmt,
                                                                       ap->get_table_id(),
                                                                       ref_table_id,
                                                                       share::schema::PARTITION_LEVEL_TWO,
                                                                       sub_part_columns,
                                                                       sub_gen_columns))) {
      LOG_WARN("failed to get partition columns", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ap->est_cost_info_.range_columns_.count(); ++i) {
    if (OB_FAIL(range_exprs.push_back(ap->est_cost_info_.range_columns_.at(i).expr_))) {
      LOG_WARN("failed to push back column exprs", K(ret));
    }
  }

#define EXTRACT_PROJECTOR(columns, projector)                                      \
  all_find = true;                                                                 \
  for (int64_t i = 0; OB_SUCC(ret) && all_find && i < columns.count(); ++i) {      \
    int64_t idx = -1;                                                              \
    if (ObOptimizerUtil::find_equal_expr(range_exprs, columns.at(i).expr_, idx)) { \
      if (OB_FAIL(projector.push_back(idx))) {                                     \
        LOG_WARN("failed to push back idx", K(ret));                               \
      }                                                                            \
    } else {                                                                       \
      all_find = false;                                                            \
      projector.reuse();                                                           \
    }                                                                              \
  }

  // get partition projector by partition columns
  EXTRACT_PROJECTOR(part_columns, part_projector);
  // get partition projector by generated columns
  EXTRACT_PROJECTOR(gen_columns, gen_projector);
  // get sub partition projector by sub partition columns
  EXTRACT_PROJECTOR(sub_part_columns, sub_part_projector);
  // get sub partition projector by sub generated columns
  EXTRACT_PROJECTOR(sub_gen_columns, sub_gen_projector);

#undef EXTRACT_PROJECTOR
  LOG_TRACE("get projector", K(part_projector), K(sub_part_projector), K(gen_projector), K(sub_gen_projector));
  return ret;
}

int ObJoinOrder::get_scan_range_partitions(const ObNewRange& scan_range, const ObIArray<int64_t>& part_projector,
    const ObIArray<int64_t>& gen_projector, ObTableLocation& table_location, ObIArray<int64_t>& part_ids,
    const ObIArray<int64_t>* level_one_part_ids /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObExecContext* exec_ctx = NULL;
  ObArenaAllocator alloc;
  ObNewRange part_range;
  ObNewRange gen_range;

  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(exec_ctx = get_plan()->get_optimizer_context().get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(schema_guard), K(exec_ctx));
  } else if (OB_UNLIKELY(part_projector.empty() && gen_projector.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part projector and gen projector both empty", K(ret));
  } else if (!part_projector.empty() &&
             OB_FAIL(construct_partition_range(alloc, scan_range, part_range, part_projector))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (!gen_projector.empty() &&
             OB_FAIL(construct_partition_range(alloc, scan_range, gen_range, gen_projector))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else {
    ObNewRange* part_range_ptr = part_projector.empty() ? NULL : &part_range;
    ObNewRange* gen_range_ptr = gen_projector.empty() ? NULL : &gen_range;
    if (OB_FAIL(table_location.get_partition_ids_by_range(
            *exec_ctx, schema_guard, part_range_ptr, gen_range_ptr, part_ids, level_one_part_ids))) {
      LOG_WARN("failed to get partition ids by range", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::construct_partition_range(ObArenaAllocator& allocator, const ObNewRange& scan_range,
    ObNewRange& part_range, const ObIArray<int64_t>& part_projector)
{
  int64_t ret = OB_SUCCESS;
  ObObj* start_key = NULL;
  ObObj* end_key = NULL;
  int64_t key_count = part_projector.count();
  if (OB_ISNULL(start_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * key_count))) ||
      OB_ISNULL(end_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * key_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(start_key), K(end_key));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < key_count; ++i) {
    int64_t pos = part_projector.at(i);
    if (OB_UNLIKELY(pos < 0 || pos >= scan_range.start_key_.get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(ret), K(pos), K(scan_range.start_key_.get_obj_cnt()));
    } else {
      start_key[i] = scan_range.start_key_.get_obj_ptr()[pos];
      end_key[i] = scan_range.end_key_.get_obj_ptr()[pos];
    }
  }
  if (OB_SUCC(ret)) {
    part_range.start_key_.assign(start_key, key_count);
    part_range.end_key_.assign(end_key, key_count);
    // create table t1 (c1 int, c2 int, index idx1 (c1,c2) local) partition by (c1) ...
    // select * from t1 where c1 = 1 and c2 < 10
    // get scan_range (1,NULL,MAX,MAX,MAX ; 1,10,MIN,MIN,MIN) for idx1
    // when calculate partition ids, we only need value from c1, if use scan_range's border flag,
    // then we will get a empty range (1,1)
    if (part_range.start_key_.compare(part_range.end_key_) == 0) {
      part_range.border_flag_.set_inclusive_start();
      part_range.border_flag_.set_inclusive_end();
    } else {
      part_range.border_flag_ = scan_range.border_flag_;
    }
  }
  return ret;
}

int ObJoinOrder::get_partition_columns(ObDMLStmt& stmt, const int64_t table_id, const int64_t index_id,
    const share::schema::ObPartitionLevel part_level, ObIArray<ColumnItem>& partition_columns,
    ObIArray<ColumnItem>& generate_columns)
{
  int ret = OB_SUCCESS;
  ObRawExpr* partition_expr = NULL;
  if (share::schema::PARTITION_LEVEL_ONE == part_level) {
    partition_expr = stmt.get_part_expr(table_id, index_id);
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
    partition_expr = stmt.get_subpart_expr(table_id, index_id);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part level should be one or two", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(partition_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null partition expr", K(ret), K(part_level), K(table_id), K(index_id));
    } else if (OB_FAIL(
                   extract_partition_columns(stmt, table_id, partition_expr, partition_columns, generate_columns))) {
      LOG_WARN("failed to extract partitoin column", K(ret));
    } else if (generate_columns.count() > 0 && partition_columns.count() > 1) {
      generate_columns.reset();
    }
  }

  return ret;
}

int ObJoinOrder::extract_partition_columns(ObDMLStmt& stmt, const int64_t table_id, const ObRawExpr* part_expr,
    ObIArray<ColumnItem>& partition_columns, ObIArray<ColumnItem>& generate_columns,
    const bool is_generate_column /* = false */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> column_exprs;
  ObRawExpr* cur_expr = NULL;
  ObColumnRefRawExpr* col_expr = NULL;
  uint64_t column_id = OB_INVALID_ID;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    if (OB_ISNULL(cur_expr = column_exprs.at(i)) || OB_UNLIKELY(!cur_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected column expr", K(ret), K(cur_expr));
    } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(cur_expr))) {
    } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = col_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid column id", K(ret));
    } else if (is_generate_column) {  // only deal dependented columns for generated partition column
      if (OB_FAIL(add_partition_column(stmt, table_id, column_id, generate_columns))) {
        LOG_WARN("Failed to add partiton column", K(ret));
      }
    } else if (col_expr->is_generated_column() && column_exprs.count() <= 1 &&
               OB_FAIL(extract_partition_columns(
                   stmt, table_id, col_expr->get_dependant_expr(), partition_columns, generate_columns, true))) {
      LOG_WARN("failed to extract partition columns for generated column", K(ret));
    } else if (OB_FAIL(add_partition_column(stmt, table_id, column_id, partition_columns))) {
      LOG_WARN("Failed to add partiton column", K(ret));
    }
  }

  return ret;
}

int ObJoinOrder::add_partition_column(
    ObDMLStmt& stmt, const uint64_t table_id, const uint64_t column_id, ObIArray<ColumnItem>& partition_columns)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < partition_columns.count(); ++i) {
    if (partition_columns.at(i).column_id_ == column_id) {
      find = true;
    }
  }
  if (OB_SUCC(ret) && !find) {
    ColumnItem* column_item = NULL;
    if (OB_ISNULL(column_item = stmt.get_column_item_by_id(table_id, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column item", K(ret), K(table_id), K(column_id));
    } else if (OB_FAIL(partition_columns.push_back(*column_item))) {
      LOG_WARN("Failed to add column item to partition columns", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_join_interesting_order(sql::Path* path)
{
  int ret = OB_SUCCESS;
  bool join_math = false;
  int64_t dummy_prefix_count = 0;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!path->has_interesting_order_flag(OrderingFlag::JOIN_MATCH)) {
    // ordering not used in join, no need to update
  } else if (OB_FAIL(is_join_match(path->get_ordering(), dummy_prefix_count, join_math))) {
    LOG_WARN("failed to check is join match", K(ret));
  } else if (!join_math) {
    path->clear_interesting_order_flag(OrderingFlag::JOIN_MATCH);
  }
  return ret;
}

int ObJoinOrder::try_calculate_partitions_with_rowid(const uint64_t table_id, const uint64_t ref_table_id,
    const bool is_dml_table, ObIArray<AccessPath*>& access_paths, const common::ObIArray<ObRawExpr*>& cond_exprs,
    ObSqlSchemaGuard& schema_guard, const ParamStore& param_store, ObExecContext& exec_ctx,
    ObIPartitionLocationCache& location_cache, ObSQLSessionInfo& session_info,
    ObTablePartitionInfo& table_partition_info, bool& calc_done)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  calc_done = false;
  int rowid_ap_idx = -1;
  for (int i = 0; OB_SUCC(ret) && rowid_ap_idx == -1 && i < access_paths.count(); i++) {
    LOG_TRACE("try calculate partitions with rowid",
        K(table_id),
        K(*access_paths.at(i)),
        K(access_paths.at(i)->index_id_),
        K(access_paths.at(i)->table_id_),
        K(access_paths.at(i)->ref_table_id_));
    if (OB_ISNULL(access_paths.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null access paths", K(ret));
    } else if (ObSQLMockSchemaUtils::is_mock_index(access_paths.at(i)->index_id_) && access_paths.at(i)->is_get()) {
      rowid_ap_idx = i;
    } else {
      // do nothing
    }
  }
  // get param index using query range and expr
  if (OB_SUCC(ret) && -1 != rowid_ap_idx) {
    uint64_t mocked_index_id = access_paths.at(rowid_ap_idx)->index_id_;
    uint64_t real_table_id = ObSQLMockSchemaUtils::get_baseid_from_rowid_index_id(mocked_index_id);
    const ObTableSchema* base_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(real_table_id, base_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null table schema", K(ret));
    } else if (base_table_schema->is_old_no_pk_table()) {
      // do nothing
    } else {
      ObArray<int64_t> param_indexes;
      if (OB_FAIL(find_all_rowid_param_index(cond_exprs, param_indexes))) {
        LOG_WARN("failed to find all rowid param indexes", K(ret));
      }
      ObQueryRange* range = access_paths.at(rowid_ap_idx)->pre_query_range_;
      if (OB_FAIL(ret) || param_indexes.count() <= 0) {
        // do nothing
      } else if (OB_ISNULL(range)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null query range", K(ret));
      } else if (OB_FAIL(table_partition_info.init_table_location_with_rowid(
                     schema_guard, param_indexes, table_id, ref_table_id, session_info, is_dml_table))) {
        LOG_WARN("failed to init table location with rowid", K(ret));
      } else if (OB_FAIL(table_partition_info.calculate_phy_table_location_info(
                     exec_ctx, schema_guard.get_schema_guard(), param_store, location_cache))) {
        LOG_WARN("failed to calculate physical table location info", K(ret));
      } else {
        calc_done = true;
      }
    }
  }
  return ret;
}

int ObJoinOrder::find_all_rowid_param_index(
    const common::ObIArray<ObRawExpr*>& cond_exprs, common::ObIArray<int64_t>& param_indexes)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < cond_exprs.count(); i++) {
    if (OB_FAIL(find_rowid_param_index_recursively(cond_exprs.at(i), param_indexes))) {
      LOG_WARN("failed to find all rowid param index", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::find_rowid_param_index_recursively(const ObRawExpr* raw_expr, common::ObIArray<int64_t>& param_indexes)
{
  int ret = OB_SUCCESS;
  bool stack_overflow = false;
  ObDMLStmt* stmt = static_cast<ObDMLStmt*>(get_plan()->get_stmt());
  ObIArray<ObHiddenColumnItem>& calculable_exprs = stmt->get_calculable_exprs();
  int64_t min_hidden_idx = INT64_MAX;
  for (int i = 0; i < calculable_exprs.count(); i++) {
    if (min_hidden_idx < calculable_exprs.at(i).hidden_idx_) {
      min_hidden_idx = calculable_exprs.at(i).hidden_idx_;
    }
  }
  if (OB_FAIL(check_stack_overflow(stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (OB_UNLIKELY(stack_overflow)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret));
  } else if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null raw expr", K(ret));
  } else if (T_OP_EQ == raw_expr->get_expr_type() && 2 == raw_expr->get_param_count() &&
             OB_NOT_NULL(raw_expr->get_param_expr(0)) && OB_NOT_NULL(raw_expr->get_param_expr(1))) {
    int idx = raw_expr->get_param_expr(0)->has_flag(IS_COLUMN) ? 0 : 1;
    if (raw_expr->get_param_expr(idx)->has_flag(IS_COLUMN)) {
      const ObColumnRefRawExpr* column_expr = static_cast<const ObColumnRefRawExpr*>(raw_expr->get_param_expr(idx));
      if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_expr->get_column_id() &&
          raw_expr->get_param_expr(1 - idx)->has_flag(IS_CONST) &&
          raw_expr->get_param_expr(1 - idx)->has_flag(IS_PARAM)) {
        const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(raw_expr->get_param_expr(1 - idx));
        if (min_hidden_idx > const_expr->get_value().get_unknown()) {
          ret = param_indexes.push_back(const_expr->get_value().get_unknown());
        }
      }
    }
  } else if (raw_expr->has_flag(CNT_COLUMN)) {
    for (int i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(find_rowid_param_index_recursively(raw_expr->get_param_expr(i), param_indexes)))) {
        LOG_WARN("failed to find rowid param index", K(ret));
      }
    }
  }
  return ret;
}

InnerPathInfo* ObJoinOrder::get_inner_path_info(const ObIArray<ObRawExpr*>& join_conditions)
{
  InnerPathInfo* info = NULL;
  for (int64_t i = 0; NULL == info && i < inner_path_infos_.count(); ++i) {
    InnerPathInfo& cur_info = inner_path_infos_.at(i);
    if (cur_info.join_conditions_.count() == join_conditions.count()) {
      bool is_equal = true;
      for (int64_t j = 0; is_equal && j < join_conditions.count(); ++j) {
        if (!ObOptimizerUtil::find_equal_expr(cur_info.join_conditions_, join_conditions.at(j))) {
          is_equal = false;
        }
      }
      if (is_equal) {
        info = &cur_info;
      }
    }
  }
  return info;
}

int ObJoinOrder::find_nl_with_param_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
    ObJoinOrder& right_tree, ObIArray<sql::Path*>& nl_with_param_path)
{
  int ret = OB_SUCCESS;
  InnerPathInfo* path_info = right_tree.get_inner_path_info(join_conditions);
  LOG_TRACE("OPT: find nl with param path",
      K(join_relids),
      K(right_tree.get_tables()),
      K(join_conditions),
      K(right_tree.get_inner_path_infos()));
  if (NULL == path_info) {
    // inner path not generate yet
    if (OB_ISNULL(path_info = right_tree.get_inner_path_infos().alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate place holder", K(ret));
    } else if (OB_FAIL(path_info->join_conditions_.assign(join_conditions))) {
      LOG_WARN("failed to assign join conditions", K(ret));
    } else if (OB_FAIL(generate_inner_base_paths(join_conditions, join_relids, right_tree, *path_info))) {
      LOG_WARN("failed to generate inner base paths", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(path_info)) {
    if (OB_FAIL(nl_with_param_path.assign(path_info->inner_paths_))) {
      LOG_WARN("failed to append inner path", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::generate_inner_base_paths(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
    ObJoinOrder& right_tree, InnerPathInfo& inner_path_info)
{
  int ret = OB_SUCCESS;
  if (ACCESS == right_tree.get_type()) {
    if (OB_FAIL(generate_inner_access_path(join_conditions, join_relids, right_tree, inner_path_info))) {
      LOG_WARN("failed to generate inner path for access", K(ret));
    }
    LOG_TRACE("OPT: succeed to generate inner path for access", K(inner_path_info));
  } else if (SUBQUERY == right_tree.get_type()) {
    if (OB_FAIL(generate_inner_subquery_path(join_conditions, join_relids, right_tree, inner_path_info))) {
      LOG_WARN("failed to generate inner subquery path", K(ret));
    }
    LOG_TRACE("OPT: succeed generate inner path for subquery", K(inner_path_info));
  } else {
    // do nothing
  }
  return ret;
}

int ObJoinOrder::generate_inner_access_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
    ObJoinOrder& right_tree, InnerPathInfo& inner_path_info)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSEArray<ObRawExpr*, 4> pushdown_quals;
  PathHelper helper;
  helper.is_inner_path_ = true;
  helper.table_opt_info_ = &inner_path_info.table_opt_info_;
  ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> nl_params;
  bool is_valid = false;
  LOG_TRACE("OPT: start to create inner path for access", K(right_tree.get_table_id()));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(check_inner_path_valid(join_conditions,
                 join_relids,
                 right_tree,
                 pushdown_quals,
                 helper.pushdown_filters_,
                 nl_params,
                 is_valid))) {
    LOG_WARN("failed to check inner path valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(helper.filters_.assign(right_tree.get_restrict_infos()))) {
    LOG_WARN("failed to assign restrict infos", K(ret));
  } else if (OB_FAIL(append(helper.filters_, helper.pushdown_filters_))) {
    LOG_WARN("failed to append pushdown quals", K(ret));
  } else if (OB_FAIL(right_tree.generate_access_paths(helper))) {
    LOG_WARN("failed to generate access paths", K(ret));
  } else if (helper.inner_paths_.count() <= 0) {
    // do nothing.
    // in non static typing engine. if c1 is primary key of t1, when a pushdown qual with
    // format `c1 (int) = ? (datetime)` appeared, check index match will consider pushdown is
    // valid. But this qual can't use to extract query range.
  } else if (OB_FAIL(check_and_fill_inner_path_info(helper, *stmt, inner_path_info, pushdown_quals, nl_params))) {
    LOG_WARN("failed to check and fill inner path info", K(ret));
  }
  LOG_TRACE("succeed to create inner access path", K(right_tree.get_table_id()), K(pushdown_quals));
  return ret;
}

int ObJoinOrder::check_inner_path_valid(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
    ObJoinOrder& right_tree, ObIArray<ObRawExpr*>& pushdown_quals, ObIArray<ObRawExpr*>& param_pushdown_quals,
    ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t table_id = right_tree.get_table_id();
  ObDMLStmt* stmt = NULL;
  TableItem* table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(right_tree.get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table_item));
  } else if ((is_virtual_table(table_item->ref_id_) && !is_oracle_mapping_real_virtual_table(table_item->ref_id_) &&
                 table_item->ref_id_ != OB_ALL_VIRTUAL_SQL_AUDIT_TID &&
                 table_item->ref_id_ != OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID)) {
    /* we do not generate inner access path for virtual table or has domain filter*/
    /* __all_virtual_sql_audit and gv$plan_cache_plan_explain can do table get*/
    LOG_TRACE("OPT:skip adding inner access path due to virtual table/has domain filter",
        K(table_id),
        K(table_item->ref_id_),
        K(is_virtual_table(table_item->ref_id_)));
  } else if (OB_FAIL(extract_pushdown_quals(join_conditions, pushdown_quals))) {
    LOG_WARN("failed to extract pushdown quals", K(ret));
  } else if (OB_FAIL(extract_params_for_inner_path(
                 table_id, join_relids, nl_params, pushdown_quals, param_pushdown_quals))) {
    LOG_WARN("failed to extract params for inner path", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_pushdown_filter_to_base_table(*get_plan(),
                 table_item->table_id_,
                 param_pushdown_quals,
                 right_tree.get_restrict_infos(),
                 is_valid))) {
    LOG_WARN("failed to check pushdown filter for access", K(ret));
  }
  return ret;
}

int ObJoinOrder::generate_inner_subquery_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
    ObJoinOrder& right_tree, InnerPathInfo& inner_path_info)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* parent_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> pushdown_quals;
  LOG_TRACE("OPT:start to generate inner subquery path", K(right_tree.table_id_), K(join_relids));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(ret));
  } else if (OB_FAIL(extract_pushdown_quals(join_conditions, pushdown_quals))) {
    LOG_WARN("failed to extract pushdown quals", K(ret));
  } else if (OB_FAIL(append(pushdown_quals, right_tree.get_restrict_infos()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(right_tree.generate_one_inner_subquery_path(
                 *parent_stmt, join_relids, pushdown_quals, inner_path_info))) {
    LOG_WARN("failed to generate inner subquery path", K(table_id_), K(join_relids), K(pushdown_quals), K(ret));
  } else {
    LOG_TRACE("succeed to generate one inner subquery path", K(table_id_), K(join_relids), K(pushdown_quals));
  }
  return ret;
}

int ObJoinOrder::generate_one_inner_subquery_path(ObDMLStmt& parent_stmt, const ObRelIds join_relids,
    const ObIArray<ObRawExpr*>& pushdown_quals, InnerPathInfo& inner_path_info)
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  bool can_pushdown = false;
  ObSelectStmt* subquery = NULL;
  ObSEArray<ObRawExpr*, 4> param_pushdown_quals;
  ObSEArray<ObRawExpr*, 4> candi_pushdown_quals;
  ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> nl_params;
  ObSQLSessionInfo* session_info = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSelectStmt* child_stmt = NULL;
  const TableItem* table_item = NULL;
  helper.is_inner_path_ = true;

  if (OB_ISNULL(get_plan()) || OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(table_item = parent_stmt.get_table_item_by_id(table_id_)) ||
      OB_ISNULL(child_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(table_item), K(child_stmt));
  } else if (OB_FAIL(extract_params_for_inner_path(
                 table_id_, join_relids, nl_params, pushdown_quals, param_pushdown_quals))) {
    LOG_WARN("failed to extract params for inner path", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::pushdown_filter_into_subquery(parent_stmt,
                 *child_stmt,
                 get_plan()->get_optimizer_context(),
                 param_pushdown_quals,
                 candi_pushdown_quals,
                 helper.filters_,
                 can_pushdown))) {
    LOG_WARN("failed to pushdown filter into subquery", K(ret));
  } else if (!can_pushdown) {
    // do thing
    LOG_TRACE("can not pushdown any filter into subquery");
  } else if (OB_FAIL(deep_copy_subquery(subquery))) {
    LOG_WARN("failed to deep copy subquery", K(ret));
  } else if (OB_FALSE_IT(helper.child_stmt_ = subquery)) {
  } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(parent_stmt,
                 *subquery,
                 table_id_,
                 session_info,
                 *expr_factory,
                 candi_pushdown_quals,
                 helper.pushdown_filters_))) {
    LOG_WARN("failed to rename pushdown filter", K(ret));
  } else if (OB_FAIL(generate_subquery_path(helper))) {
    LOG_WARN("failed to generate subquery path", K(ret));
  } else if (OB_FAIL(check_and_fill_inner_path_info(helper, parent_stmt, inner_path_info, pushdown_quals, nl_params))) {
    LOG_WARN("failed to check and fill inner path info", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_and_fill_inner_path_info(PathHelper& helper, ObDMLStmt& stmt, InnerPathInfo& inner_path_info,
    const ObIArray<ObRawExpr*>& pushdown_quals, ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exec_params;
  for (int64_t i = 0; OB_SUCC(ret) && i < nl_params.count(); ++i) {
    ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(stmt.get_exec_param_ref_exprs(), nl_params.at(i).first);
    if (param_expr == NULL) {
      // do nothing
    } else if (OB_FAIL(exec_params.push_back(param_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  // check inner path valid, and fill other informations
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_paths_.count(); ++i) {
    ObSEArray<ObRawExpr*, 32> range_exprs;
    ObSEArray<ObRawExpr*, 32> range_params;
    ObSEArray<ObRawExpr*, 8> pushdown_params;
    sql::Path* inner_path = helper.inner_paths_.at(i);

    if (OB_ISNULL(inner_path) || OB_ISNULL(inner_path->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null path", K(ret));
    } else if (OB_FAIL(get_range_params(inner_path, range_exprs))) {
      LOG_WARN("failed to get range_exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::extract_params(range_exprs, range_params))) {
      LOG_WARN("failed to extract range params", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::intersect(exec_params, range_params, pushdown_params))) {
      LOG_WARN("failed to get intersect params", K(ret));
    } else if (pushdown_params.empty()) {
      // pushdown quals do not contribute query range
      LOG_TRACE("pushdown filters can not extend any query range");
    } else if (OB_FAIL(append(inner_path->pushdown_filters_, pushdown_quals))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(inner_path->nl_params_, nl_params))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {
      inner_path->is_inner_path_ = true;
      ret = inner_path_info.inner_paths_.push_back(inner_path);
    }
  }
  return ret;
}

int ObJoinOrder::extract_pushdown_quals(const ObIArray<ObRawExpr*>& quals, ObIArray<ObRawExpr*>& pushdown_quals)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr* qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexplected null", K(qual), K(ret));
      // can not push down expr with subquery
    } else if (!qual->has_flag(CNT_SUB_QUERY) && !qual->has_flag(CNT_PSEUDO_COLUMN) && !qual->has_flag(CNT_ROWNUM) &&
               T_OP_NE != qual->get_expr_type()) {
      ret = pushdown_quals.push_back(qual);
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObJoinOrder::get_range_params(const sql::Path* path, ObIArray<ObRawExpr*>& range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (ACCESS == path->path_type_) {
    const AccessPath* access_path = static_cast<const AccessPath*>(path);
    if (OB_FAIL(append(range_exprs, access_path->est_cost_info_.pushdown_prefix_filters_))) {
      LOG_WARN("failed to append pushdown prefix filters", K(ret));
    }
  } else if (SUBQUERY == path->path_type_) {
    const SubQueryPath* sub_path = static_cast<const SubQueryPath*>(path);
    if (OB_FAIL(get_range_params(sub_path->root_, range_exprs))) {
      LOG_WARN("failed to get range params", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_contain_rownum(const ObIArray<ObRawExpr*>& join_condition, bool& contain_rownum)
{
  int ret = OB_SUCCESS;
  contain_rownum = false;
  ObRawExpr* qual = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !contain_rownum && i < join_condition.count(); ++i) {
    if (OB_ISNULL(qual = join_condition.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(qual), K(ret));
    } else if (qual->has_flag(CNT_ROWNUM)) {
      contain_rownum = true;
    }
  }
  return ret;
}

int ObJoinOrder::extract_real_join_keys(ObIArray<ObRawExpr*>& join_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_keys.count(); ++i) {
    ObRawExpr*& expr = join_keys.at(i);
    bool is_lossless = false;
    if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(expr, is_lossless))) {
      LOG_WARN("failed to check lossless column cast", K(ret));
    } else if (is_lossless) {
      expr = expr->get_param_expr(0);
    }
  }
  return ret;
}

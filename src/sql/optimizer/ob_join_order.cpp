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
#include "ob_join_order.h"
#include "sql/optimizer/ob_skyline_prunning.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_access_path_estimation.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/rewrite/ob_query_range_define.h"
#include "sql/das/iter/ob_das_text_retrieval_eval_node.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::ObColumnSchemaV2;
using oceanbase::share::schema::ObColDesc;
using share::schema::ObSchemaGetterGuard;
using common::ObArray;

class OrderingInfo;
class QueryRangeInfo;

#define OPT_CTX (get_plan()->get_optimizer_context())

ERRSIM_POINT_DEF(EN_FORCE_INDEX_MERGE_PLAN, "Force to use index merge if it is possible");

ObJoinOrder::~ObJoinOrder()
{

}

int ObJoinOrder::fill_query_range_info(const QueryRangeInfo &range_info,
                                       ObCostTableScanInfo &est_cost_info,
                                       bool use_skip_scan)
{
  int ret = OB_SUCCESS;
  const ObQueryRangeArray &ranges = range_info.get_ranges();
  const ObQueryRangeArray &ss_ranges = range_info.get_ss_ranges();
  const ObQueryRangeProvider *provider = range_info.get_query_range_provider();
  ObSEArray<uint64_t, 4> total_range_sizes;
  est_cost_info.ranges_.reset();
  est_cost_info.ss_ranges_.reset();
  est_cost_info.at_most_one_range_ = false;
  bool has_exec_param = false;
  if (OB_ISNULL(provider)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(provider));
  } else if (OB_FAIL(check_has_exec_param(*provider, has_exec_param))) {
    LOG_WARN("failed to check has exec param", K(ret));
  } else if (!has_exec_param) {
    est_cost_info.total_range_cnt_ = ranges.count();
  } else if (OB_FAIL(provider->get_total_range_sizes(total_range_sizes))) {
    LOG_WARN("failed to get range size", K(ret));
  } else {
    int64_t index_prefix = range_info.get_index_prefix();
    if (-1 == index_prefix || index_prefix >= total_range_sizes.count()) {
      index_prefix = total_range_sizes.count() - 1;
    }
    if (total_range_sizes.empty() || index_prefix < 0 ) {
      est_cost_info.total_range_cnt_ = ranges.count();
    } else {
      est_cost_info.total_range_cnt_ = total_range_sizes.at(index_prefix);
    }
  }
  // maintain query range info
  for(int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (OB_ISNULL(ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range is null", K(ret));
    } else if (OB_FAIL(est_cost_info.ranges_.push_back(*ranges.at(i)))) {
      LOG_WARN("failed to add range", K(ret));
    } else { /*do nothing*/ }
  }
  for(int64_t i = 0; use_skip_scan && OB_SUCC(ret) && i < ss_ranges.count(); ++i) {
    if (OB_ISNULL(ss_ranges.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range is null", K(ret));
    } else if (OB_FAIL(est_cost_info.ss_ranges_.push_back(*ss_ranges.at(i)))) {
      LOG_WARN("failed to add range", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret) && ranges.count() > 1) {
    // if there is more than one range and it is exists exec params in ranges_exprs, check at most one range.
    // for (min; max) range extract from range_exprs contain exec params, do nothing now.
    ObSEArray<ObRawExpr*, 4> cur_const_exprs;
    ObSEArray<ObRawExpr*, 16> columns;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(provider->get_range_exprs(),
                                                     columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (columns.empty() || !has_exec_param) {
      /* do nothing */
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(provider->get_range_exprs(),
                                                            cur_const_exprs))) {
      // for inner path, const expr is computed without pushdown filter.
      // need compute const expr by range_exprs.
      LOG_WARN("failed to compute const exprs", K(ret));
    } else {
      bool at_most_one_range = true;
      for (int64_t i = 0; at_most_one_range && i < columns.count(); ++i) {
        at_most_one_range = ObOptimizerUtil::find_equal_expr(cur_const_exprs, columns.at(i),
                                                             get_output_equal_sets());
      }
      est_cost_info.at_most_one_range_ = at_most_one_range;
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_location_for_index_info_entry(const uint64_t table_id,
                                                            const uint64_t base_table_id,
                                                            IndexInfoEntry *index_info_entry) {
  int ret = OB_SUCCESS;
  ObTablePartitionInfo *table_part_info = NULL;
  if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(index_info_entry));
  } else if (!index_info_entry->is_index_global()) {
    if (NULL == table_partition_info_) {
      // generate table location for main table
      if (OB_FAIL(compute_table_location(table_id_,
                                        table_meta_info_.ref_table_id_,
                                        false,
                                        table_partition_info_))) {
        LOG_WARN("failed to calc table location", K(ret));
      }
    }
    table_part_info = table_partition_info_;
  } else if (OB_FAIL(get_table_partition_info_from_available_access_paths(table_id, base_table_id,
                                                 index_info_entry->get_index_id(), table_part_info))) {
      LOG_WARN("failed to table partition info from available access paths", K(ret));
  } else if (NULL == table_part_info) {
    if (OB_FAIL(compute_table_location(table_id, index_info_entry->get_index_id(), true, table_part_info))) {
      LOG_WARN("failed to calc table location", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      index_info_entry->set_partition_info(table_part_info);
    }
  }
  return ret;
}

int ObJoinOrder::get_table_partition_info_from_available_access_paths(const uint64_t table_id,
                                                                      const uint64_t ref_table_id,
                                                                      const uint64_t index_id,
                                                                      ObTablePartitionInfo *&table_part_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < available_access_paths_.count(); ++i) {
    AccessPath *cur_path = available_access_paths_.at(i);
    if (OB_ISNULL(cur_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (table_id == cur_path->table_id_ &&
              ref_table_id == cur_path->ref_table_id_ &&
              index_id == cur_path->index_id_) {
      table_part_info = cur_path->table_partition_info_;
      break;
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_location(const uint64_t table_id,
                                        const uint64_t ref_table_id,
                                        const bool is_global_index,
                                        ObTablePartitionInfo *&table_partition_info)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  const ObDMLStmt *stmt = NULL;
  const ParamStore *params = NULL;
  const TableItem *table_item = NULL;
  ObExecContext *exec_ctx = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  table_partition_info = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_schema_guard()) ||
      OB_ISNULL(sql_schema_guard = opt_ctx->get_sql_schema_guard()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id)) ||
      OB_ISNULL(params = opt_ctx->get_params()) ||
      OB_ISNULL(exec_ctx = opt_ctx->get_exec_ctx()) ||
      OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx()) ||
      OB_ISNULL(phy_plan_ctx = exec_ctx->get_physical_plan_ctx())
   || OB_ISNULL(session_info = sql_ctx->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(schema_guard), K(sql_schema_guard),
        K(stmt), K(params), K(exec_ctx), K(sql_ctx), K(phy_plan_ctx),
        K(session_info), K(allocator_), K(ret));
  } else if (OB_ISNULL(table_partition_info = reinterpret_cast<ObTablePartitionInfo*>(
                       allocator_->alloc(sizeof(ObTablePartitionInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(table_partition_info), K(ret));
  } else {
    table_partition_info = new(table_partition_info) ObTablePartitionInfo(*allocator_);
    const ObDataTypeCastParams dtc_params =
          ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
    // check whether the ref table will be modified by dml operator
    bool is_dml_table = false;
    const ObDMLStmt *top_stmt = opt_ctx->get_root_stmt();
    if (NULL != top_stmt) {
      if (top_stmt->is_explain_stmt()) {
        top_stmt = static_cast<const ObExplainStmt*>(top_stmt)->get_explain_query_stmt();
      }
      if (OB_ISNULL(top_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("top stmt should not be null", K(ret));
      } else if (OB_FAIL(top_stmt->check_table_be_modified(ref_table_id, is_dml_table))) {
        LOG_WARN("failed to check table be modified", K(ret));
      }
    }

    //this place restrict_info include sub_query.
    ObArray<ObRawExpr *> correlated_filters;
    ObArray<ObRawExpr *> uncorrelated_filters;
    if (OB_FAIL(ObOptimizerUtil::extract_parameterized_correlated_filters(get_restrict_infos(),
                                                                          correlated_filters,
                                                                          uncorrelated_filters))) {
      LOG_WARN("Failed to extract correlated filters", K(ret));
    } else if (OB_FAIL(table_partition_info->init_table_location(*sql_schema_guard,
                                                                  *stmt,
                                                                  exec_ctx,
                                                                  uncorrelated_filters,
                                                                  table_id,
                                                                  ref_table_id,
                                                                  is_global_index ? NULL : &table_item->part_ids_,
                                                                  dtc_params,
                                                                  is_dml_table,
                                                                  NULL))) {
      LOG_WARN("Failed to initialize table location", K(ret));
    } else if (OB_FAIL(table_partition_info->calculate_phy_table_location_info(*exec_ctx,
                                                                              *params,
                                                                              dtc_params))) {
      LOG_WARN("failed to calculate table location", K(ret));
    } else {
      LOG_TRACE("succeed to calculate base table sharding info", K(table_id), K(ref_table_id),
          K(is_global_index));
    }
  }
  if (OB_SUCC(ret)) {
    //select location for the newly created table partition info
    ObSEArray<ObTablePartitionInfo*, 1> tbl_part_infos;
    if (OB_FAIL(tbl_part_infos.push_back(table_partition_info))) {
      LOG_WARN("push back table partition info failed", K(ret));
    } else if (OB_FAIL(get_plan()->select_location(tbl_part_infos))) {
      LOG_WARN("failed to select location", K(ret));
    }
  }
  return ret;
}

bool ObJoinOrder::is_main_table_use_das(const ObIArray<AccessPath *> &access_paths)
{
  bool use_das = false;
  for (int64_t i = 0; !use_das && i < access_paths.count(); ++i) {
    //all local index path use the same table location info with the main table
    //so any local index path use DAS means the main table path use DAS
    const AccessPath *path = access_paths.at(i);
    if (path != nullptr &&
        path->table_id_ == table_id_ &&
        path->ref_table_id_ == table_meta_info_.ref_table_id_ &&
        !path->is_global_index_) {
      use_das = path->use_das_;
    }
  }
  return use_das;
}

int ObJoinOrder::compute_sharding_info_for_base_paths(ObIArray<AccessPath *> &access_paths,
                                                      ObIndexInfoCache &index_info_cache)
{
  int ret = OB_SUCCESS;
  // compute path sharding info
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    if (OB_FAIL(set_sharding_info_for_base_path(access_paths, index_info_cache, i))) {
      LOG_WARN("failed to compute sharding info for base path", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(prune_paths_due_to_parallel(access_paths))) {
    LOG_WARN("failed to prune path due to parallel", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); i++) {
    AccessPath *path = NULL;
    if (OB_ISNULL(path = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(path), K(ret));
    } else if (OB_FAIL(compute_base_table_path_plan_type(path))) {
      LOG_WARN("failed to compute base table path plan type", K(ret));
    } else if (OB_FAIL(compute_base_table_path_ordering(path))) {
      LOG_WARN("failed to compute base table path ordering", K(ret));
    } else {
      LOG_TRACE("succeed to compute base sharding info", K(*path));
    }
  }
  return ret;
}

// prune paths added because of auto dop:
//  global index and is not inner path / subquery with pushdown filter
//    1. not use das but parallel = 1
//    2. use das and exits same index path with parallel > 1
int ObJoinOrder::prune_paths_due_to_parallel(ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  AccessPath *path = NULL;
  if (access_paths.count() < 2) {
    /* do nothing */
  } else if (OB_ISNULL(path = access_paths.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(path));
  } else if (path->is_rescan_path()) {
    /* do nothing */
  } else {
    ObSEArray<AccessPath*, 16> tmp_paths;
    AccessPath *path = NULL;
    bool need_prune = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); i++) {
      need_prune = false;
      if (OB_ISNULL(path = access_paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(path), K(ret));
      } else if (!path->is_global_index_) {
        /* do nothing */
      } else {
        AccessPath *target_path = NULL;
        for (int64_t j = 0; NULL == target_path && j < access_paths.count(); ++j) {
          if (i != j && NULL != access_paths.at(j) && path->index_id_ == access_paths.at(j)->index_id_
              && path->use_column_store_ == access_paths.at(j)->use_column_store_) {
            target_path = access_paths.at(j);
          }
        }
        if (NULL != target_path) {
          need_prune = path->use_das_ ? 1 < target_path->parallel_
                                      : 1 >= path->parallel_;
        }
      }
      if (OB_SUCC(ret) && !need_prune && OB_FAIL(tmp_paths.push_back(path))) {
        LOG_WARN("failed to push back access path", K(ret));
      }
    }
    if (OB_SUCC(ret) && tmp_paths.count() != access_paths.count()
        && OB_FAIL(access_paths.assign(tmp_paths))) {
      LOG_WARN("failed to assign paths", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::set_sharding_info_for_base_path(ObIArray<AccessPath *> &access_paths,
                                                     ObIndexInfoCache &index_info_cache,
                                                     const int64_t cur_idx)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
  AccessPath *path = NULL;
  ObShardingInfo *sharding_info = NULL;
  ObTablePartitionInfo *table_partition_info = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  IndexInfoEntry *index_info_entry = NULL;
  if (OB_UNLIKELY(access_paths.count() <= cur_idx) ||
      OB_ISNULL(path = access_paths.at(cur_idx)) ||
      OB_ISNULL(table_partition_info = path->table_partition_info_) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(access_paths.count()), K(path), K(table_partition_info),
                                  K(cur_idx), K(get_plan()), K(stmt), K(opt_ctx));
  } else if (path->use_das_) {
    sharding_info = opt_ctx->get_match_all_sharding();
  } else if (OB_FAIL(schema_guard->get_table_schema(table_partition_info->get_ref_table_id(),
                                      table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_schema->is_external_table()) {
    if (path->parallel_ > 1
        || ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location())) {
      sharding_info = opt_ctx->get_distributed_sharding();
    } else {
      sharding_info = opt_ctx->get_local_sharding();
    }
  } else if (OB_FAIL(table_partition_info->get_location_type(opt_ctx->get_local_server_addr(),
                                                                    location_type))) {
    LOG_WARN("failed to get location type", K(ret));
  } else if (ObGlobalHint::DEFAULT_PARALLEL < path->parallel_
             && (OB_TBL_LOCATION_LOCAL == location_type || OB_TBL_LOCATION_REMOTE == location_type)) {
    sharding_info = opt_ctx->get_distributed_sharding();
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(path->table_id_,
                                                           path->index_id_,
                                                           index_info_entry))) {
    LOG_WARN("get index info entry failed", K(ret));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    sharding_info = index_info_entry->get_sharding_info();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sharding_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to compute base table sharding info", K(ret), K(sharding_info));
  } else {
    path->strong_sharding_ = sharding_info;
  }
  return ret;
}

//select location and compute sharding info for index entry
int ObJoinOrder::compute_sharding_info_for_index_info_entry(const uint64_t table_id,
                                              const uint64_t base_table_id,
                                              IndexInfoEntry *index_info_entry)
{
  int ret = OB_SUCCESS;
  ObTablePartitionInfo *part_info = NULL;
  ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
  ObShardingInfo *sharding_info = NULL;
  if (OB_ISNULL(index_info_entry)
      || OB_ISNULL(part_info = index_info_entry->get_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }  else if (NULL != sharding_info_ && !index_info_entry->is_index_global()) {
    //all the local index share the same sharding info
    sharding_info = sharding_info_;
  } else if (OB_FAIL(get_sharding_info_from_available_access_paths(table_id, base_table_id,
                                                            index_info_entry->get_index_id(),
                                                            index_info_entry->is_index_global(),
                                                            sharding_info))) {
    LOG_WARN("failed to get sharding info from available access paths", K(ret));
  } else if (NULL != sharding_info) {
    //do nothing
  } else if (OB_FAIL(part_info->get_location_type(OPT_CTX.get_local_server_addr(),
                                                location_type))) {
    LOG_WARN("failed to get location type", K(ret));
  } else if (OB_FAIL(compute_sharding_info_with_part_info(location_type, part_info, sharding_info))) {
    LOG_WARN("compute sharding info with partition info failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    index_info_entry->set_sharding_info(sharding_info);
    if (NULL == sharding_info_ && !index_info_entry->is_index_global()) {
      sharding_info_ = sharding_info;
    }
  }
  return ret;
}

int ObJoinOrder::compute_sharding_info_with_part_info(ObTableLocationType location_type,
                                                      ObTablePartitionInfo* table_partition_info,
                                                      ObShardingInfo *&sharding_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  bool is_modified = false;
  if (OB_ISNULL(table_partition_info) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_partition_info), K(get_plan()), K(stmt), K(opt_ctx), K(session_info));
  } else if (OB_ISNULL(sharding_info = reinterpret_cast<ObShardingInfo*>(
                                       allocator_->alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory", K(ret));
  } else if (OB_FAIL(stmt->check_table_be_modified(table_partition_info->get_ref_table_id(), is_modified))) {
    LOG_WARN("failed to check table be modified", K(ret));
  } else {
    const ObCandiTableLoc &phy_tbl_info = table_partition_info->get_phy_tbl_location_info();
    bool can_reselect_replica = (phy_tbl_info.is_duplicate_table_not_in_dml() &&
        (1 == phy_tbl_info.get_phy_part_loc_info_list().count())
        && !session_info->get_is_in_retry_for_dup_tbl()
        && !is_modified);
    sharding_info = new(sharding_info) ObShardingInfo();
    sharding_info->set_location_type(location_type);
    if (OB_FAIL(sharding_info->init_partition_info(
                                  get_plan()->get_optimizer_context(),
                                  *get_plan()->get_stmt(),
                                  table_partition_info->get_table_id(),
                                  table_partition_info->get_ref_table_id(),
                                  table_partition_info->get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("failed to set partition key", K(ret));
    } else {
      sharding_info->set_can_reselect_replica(can_reselect_replica);
      LOG_TRACE("succeed to compute base table sharding info", K(*sharding_info));
    }
  }
  return ret;
}

int ObJoinOrder::get_sharding_info_from_available_access_paths(const uint64_t table_id,
                                                               const uint64_t ref_table_id,
                                                               const uint64_t index_id,
                                                               bool is_global_index,
                                                               ObShardingInfo *&sharding_info) const
{
  int ret = OB_SUCCESS;
  sharding_info = NULL;
  AccessPath *cur_path = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL == sharding_info && i < available_access_paths_.count(); ++i) {
    cur_path = available_access_paths_.at(i);
    if (OB_ISNULL(cur_path) || OB_ISNULL(cur_path->strong_sharding_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_path));
    } else if (cur_path->strong_sharding_ != opt_ctx->get_match_all_sharding() &&
                cur_path->strong_sharding_ != opt_ctx->get_distributed_sharding() &&
                cur_path->strong_sharding_ != opt_ctx->get_local_sharding() &&
                table_id == cur_path->table_id_ &&
                ref_table_id == cur_path->ref_table_id_ &&
                is_global_index == cur_path->is_global_index_ &&
                index_id == cur_path->index_id_) {
      sharding_info = cur_path->strong_sharding_;
    }
  }
  return ret;
}

int ObJoinOrder::compute_base_table_path_plan_type(AccessPath *path)
{
  int ret = OB_SUCCESS;
  ObShardingInfo *sharding_info = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(sharding_info = path->strong_sharding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(path), K(sharding_info), K(ret));
  } else {
    if (sharding_info->get_can_reselect_replica()) {
      path->phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
    } else if (sharding_info->is_local()) {
      path->phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_LOCAL;
    } else if (sharding_info->is_remote()) {
      path->phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_REMOTE;
    } else if (sharding_info->is_distributed()) {
      path->phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
    }
    if (path->use_das_ ||
        (path->is_global_index_ && path->est_cost_info_.index_meta_info_.is_index_back_)) {
      path->location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
    }
  }
  return ret;
}

int ObJoinOrder::compute_base_table_path_ordering(AccessPath *path)
{
  int ret = OB_SUCCESS;
  bool is_left_prefix = false;
  bool is_right_prefix = false;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 8> range_exprs;
  ObSEArray<OrderItem, 8> range_orders;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(path->strong_sharding_) || OB_ISNULL(path->table_partition_info_) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(path), K(ret));
  } else if (OB_FALSE_IT(path->is_local_order_ = false)) {
  } else if (OB_FALSE_IT(path->is_range_order_ = false)) {
  } else if (path->est_cost_info_.index_meta_info_.is_multivalue_index_) {
    // The Json multi-value index scan operator will perform sorting and deduplication based on the primary key or docid.
    // As a result, the final output order of the data may not match the original index table output order.
    // Therefore, it's necessary to add an additional sort operator at the upper level.
    path->ordering_.reset();
  } else if (share::is_oracle_mapping_real_virtual_table(path->ref_table_id_)) {
    // Oracle agent tabel may has different collation between schema and real table.
    // Hence we should not use the ordering from oracle agent table.
    path->ordering_.reset();
  } else if (path->use_das_ &&
             !path->ordering_.empty() &&
             path->table_partition_info_->get_phy_tbl_location_info().get_partition_cnt() > 1) {
    if (get_plan()->get_optimizer_context().is_das_keep_order_enabled()) {
      // when enable das keep order optimization, DAS layer can provide a guarantee of local order,
      // otherwise the order is totally not guaranteed.
      path->is_local_order_ = true;
    } else {
      path->ordering_.reset();
    }
  } else if (path->ordering_.empty() || is_at_most_one_row_ || !path->strong_sharding_->is_distributed()) {
    path->is_local_order_ = false;
  } else if (get_plan()->get_optimizer_context().is_online_ddl()) {
    path->is_local_order_ = true;
  } else if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3, COMPAT_VERSION_4_3_0,
                                                             COMPAT_VERSION_4_3_2) &&
            path->table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count() == 1 &&
            !is_virtual_table(path->ref_table_id_)) {
    path->is_range_order_ = true;
  } else if (OB_FAIL(path->table_partition_info_->get_not_insert_dml_part_sort_expr(*get_plan()->get_stmt(),
                                                                                    &range_exprs))) {
    LOG_WARN("fail to get_not_insert_dml_part_sort_expr", K(ret));
  } else if (range_exprs.empty()) {
    path->is_local_order_ = true;
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(range_exprs,
                                                     path->order_direction_,
                                                     range_orders))) {
    LOG_WARN("failed to make range orders", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(range_orders,
                                                         path->ordering_,
                                                         get_output_equal_sets(),
                                                         get_output_const_exprs(),
                                                         is_left_prefix,
                                                         is_right_prefix))) {
    LOG_WARN("failed to find common prefix ordering", K(ret));
  } else if (is_left_prefix || is_right_prefix) {
    path->is_local_order_ = false;
    path->is_range_order_ = true;
  } else {
    path->is_local_order_ = true;
  }
  return ret;
}

int ObJoinOrder::get_explicit_dop_for_path(const uint64_t index_id, int64_t &parallel)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  parallel = ObGlobalHint::UNSET_PARALLEL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  } else if (opt_ctx->force_disable_parallel()) {
    parallel = opt_ctx->get_parallel();
  } else if (ObGlobalHint::UNSET_PARALLEL != (parallel = get_plan()->get_log_plan_hint().get_parallel(get_table_id()))) {
    /* do nothing */
  } else if (opt_ctx->is_use_table_dop()) {
    if (OB_FAIL(get_base_path_table_dop(index_id, parallel))) {
      LOG_WARN("failed to get base table dop", K(ret));
    }
  } else if (opt_ctx->is_use_auto_dop()) {
    /* do nothing */
  } else {
    parallel = opt_ctx->get_parallel();
  }
  return ret;
}

int ObJoinOrder::compute_parallel_and_server_info_for_base_paths(ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  if (access_paths.empty()) {
    /* do nothing */
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  } else {
    OpParallelRule op_parallel_rule = OpParallelRule::OP_GLOBAL_DOP ;
    int64_t parallel = ObGlobalHint::UNSET_PARALLEL;
    if (opt_ctx->force_disable_parallel()) {  // force disable parallel
      parallel = ObGlobalHint::DEFAULT_PARALLEL;
    } else if (ObGlobalHint::UNSET_PARALLEL != (parallel = get_plan()->get_log_plan_hint().get_parallel(get_table_id()))) {
      op_parallel_rule = OpParallelRule::OP_HINT_DOP;
    } else if (opt_ctx->is_use_auto_dop()) {
      op_parallel_rule = OpParallelRule::OP_AUTO_DOP;
      if (OB_FAIL(compute_access_path_parallel(access_paths, parallel))) {
        LOG_WARN("failed to calculate base path parallel", K(ret));
      }
    } else if (opt_ctx->is_use_table_dop()) {
      op_parallel_rule = OpParallelRule::OP_TABLE_DOP;
    } else if (OB_UNLIKELY(opt_ctx->get_parallel() < ObGlobalHint::DEFAULT_PARALLEL
                           || !opt_ctx->is_parallel_rule_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parallel rule", K(ret), K(opt_ctx->get_parallel()), K(opt_ctx->is_parallel_rule_valid()));
    } else {
      parallel = opt_ctx->get_parallel();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); i++) {
      if (OB_FAIL(compute_base_table_parallel_and_server_info(op_parallel_rule,
                                                              parallel, access_paths.at(i)))) {
        LOG_WARN("failed to compute base table parallel and server info", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_base_path_table_dop(uint64_t index_id, int64_t &parallel)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  parallel = ObGlobalHint::DEFAULT_PARALLEL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), K(get_plan()), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_schema->get_dop() > ObGlobalHint::DEFAULT_PARALLEL) {
    parallel = table_schema->get_dop();
  }
  return ret;
}

// just generate random parallel for access paths when enable trace point test path
// alter system set_tp tp_no = 552, error_code = 4016, frequency = 1;
// When trace point is enabled, parallel is only limited by parallel_degree_limit.
int ObJoinOrder::get_random_parallel(const int64_t parallel_degree_limit,
                                     int64_t &parallel)
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::DEFAULT_PARALLEL;
  if (OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(table_partition_info_));
  } else if (ObGlobalHint::DEFAULT_PARALLEL == parallel_degree_limit
             || is_virtual_table(table_partition_info_->get_ref_table_id())) {
    /* do nothing */
    LOG_TRACE("Auto DOP get_random_parallel", K(parallel_degree_limit),
                                    K(table_partition_info_->get_ref_table_id()),
                                    K(is_virtual_table(table_partition_info_->get_ref_table_id())));
  } else {
    const int64_t part_cnt = table_partition_info_->get_phy_tbl_location_info().get_partition_cnt();
    const bool limit_beyond_part_cnt = ObGlobalHint::UNSET_PARALLEL == parallel_degree_limit
                                       || parallel_degree_limit > part_cnt;
    int64_t parallel_type = ObRandom::rand(0, limit_beyond_part_cnt ? 2 : 1);
    switch (parallel_type) {
      case 0: {
        parallel = 1;
        break;
      }
      case 1: {
        if (part_cnt > 1) {
          if (limit_beyond_part_cnt) {
            parallel = ObRandom::rand(2, part_cnt);
          } else {
            parallel = ObRandom::rand(2, parallel_degree_limit);
          }
          break;
        }
      }
      default:  {
        parallel = part_cnt + 1;
        break;
      }
    }
    LOG_TRACE("Auto DOP get_random_parallel", K(parallel_degree_limit), K(part_cnt),
                                            K(parallel_type), K(parallel));
  }
  return ret;
}

int ObJoinOrder::get_parallel_from_available_access_paths(int64_t &parallel) const
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::UNSET_PARALLEL;
  AccessPath *path = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < available_access_paths_.count(); ++i) {
    if (OB_ISNULL(path = available_access_paths_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (path->is_inner_path_ || path->use_das_) {
      /* do nothing */
    } else {
      parallel = path->parallel_;
      break;
    }
  }
  return ret;
}

// compute auto dop
int ObJoinOrder::compute_access_path_parallel(ObIArray<AccessPath *> &access_paths,
                                              int64_t &parallel)
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::UNSET_PARALLEL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  int64_t cur_min_parallel = ObGlobalHint::UNSET_PARALLEL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
      || OB_ISNULL(session_info = opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(get_plan()), K(opt_ctx), K(session_info));
  } else if (!opt_ctx->get_global_hint().has_parallel_hint() &&
             OB_FAIL(OB_E(EventTable::EN_ENABLE_AUTO_DOP_FORCE_PARALLEL_PLAN) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    if (!session_info->is_user_session()) {
      parallel = ObGlobalHint::DEFAULT_PARALLEL;
    } else if (OB_FAIL(get_random_parallel(opt_ctx->get_session_parallel_degree_limit(), parallel))) {
      LOG_WARN("failed to get random parallel", K(ret));
    }
    LOG_TRACE("Auto DOP trace point", K(session_info->is_user_session()), K(parallel));
  } else if (OB_FAIL(get_parallel_from_available_access_paths(cur_min_parallel))) {
    LOG_WARN("failed to get parallel from available access paths", K(ret));
  } else {
    int64_t calc_parallel = ObGlobalHint::UNSET_PARALLEL;
    int64_t das_path_cnt = 0;
    AccessPath *path = NULL;
    bool finish = false;
    OPT_TRACE_TITLE("begin compute auto dop for table");
    for (int64_t i = 0; !finish && OB_SUCC(ret) && i < access_paths.count(); i++) {
      if (OB_ISNULL(path = access_paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(path), K(ret));
      } else if (path->use_das_) {
        ++das_path_cnt;
      } else if (OB_FAIL(path->compute_parallel_degree(cur_min_parallel, calc_parallel))) {
        LOG_WARN("failed to compute parallel degree", K(ret));
      } else {
        LOG_TRACE("finish compute one path parallel degree", K(i), K(cur_min_parallel), K(calc_parallel),
                                                            K(path->table_id_), K(path->index_id_));
        cur_min_parallel = calc_parallel;
        finish = ObGlobalHint::DEFAULT_PARALLEL == calc_parallel;
      }
    }
    OPT_TRACE_TITLE("end compute auto dop for table");
    if (OB_SUCC(ret)) {
      parallel = access_paths.count() == das_path_cnt ? ObGlobalHint::DEFAULT_PARALLEL : calc_parallel;
      LOG_TRACE("finish compute paths parallel for Auto DOP", K(parallel), K(das_path_cnt),
                                                            K(access_paths.count()));
    }
  }
  return ret;
}

int ObJoinOrder::compute_base_table_parallel_and_server_info(const OpParallelRule op_parallel_rule,
                                                             const int64_t parallel,
                                                             AccessPath *path)
{
  int ret = OB_SUCCESS;
  int64_t final_parallel = parallel;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *index_schema = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(path) || OB_ISNULL(path->table_partition_info_) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), K(get_plan()), K(path));
  } else if (OB_FAIL(path->table_partition_info_->get_all_servers(path->server_list_))) {
    LOG_WARN("failed to get all servers", K(ret));
  } else if (path->use_das_) {
    // for das access path, fill server_list_
    path->op_parallel_rule_ = OpParallelRule::OP_DAS_DOP;
    path->parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
    path->server_cnt_ = path->server_list_.count();
    path->available_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
    if (path->is_index_merge_path()) {
      ObSEArray<AccessPath *, 4> index_merge_scan_ap;
      if(OB_FAIL(static_cast<IndexMergePath*>(path)->get_all_scan_access_paths(index_merge_scan_ap))) {
        LOG_WARN("failed to get all scan access paths", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_merge_scan_ap.count(); ++i) {
        if (OB_ISNULL(index_merge_scan_ap.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(index_merge_scan_ap.at(i)->server_list_.assign(path->server_list_))) {
          LOG_WARN("failed to assign server list", K(ret));
        } else {
          index_merge_scan_ap.at(i)->op_parallel_rule_ = OpParallelRule::OP_DAS_DOP;
          index_merge_scan_ap.at(i)->parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
          index_merge_scan_ap.at(i)->server_cnt_ = path->server_list_.count();
          index_merge_scan_ap.at(i)->available_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
        }
      }
    }
  } else if (OB_FAIL(schema_guard->get_table_schema(path->index_id_, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    if (OpParallelRule::OP_TABLE_DOP == op_parallel_rule) {
      final_parallel = index_schema->get_dop();
    }
    if ((index_schema->is_spatial_index() || index_schema->is_vec_index()) && index_schema->get_partition_num() <  final_parallel) {
      final_parallel = index_schema->get_partition_num();
    }
    if (final_parallel < ObGlobalHint::DEFAULT_PARALLEL) {
      final_parallel = ObGlobalHint::DEFAULT_PARALLEL;
    }
    path->op_parallel_rule_ = op_parallel_rule;
    path->parallel_ = final_parallel;
    path->server_cnt_ = path->server_list_.count();
    path->available_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
  }
  return ret;
}

int ObJoinOrder::get_valid_index_ids_with_no_index_hint(ObSqlSchemaGuard &schema_guard,
                                                        const uint64_t ref_table_id,
                                                        uint64_t *tids,
                                                        const int64_t index_count,
                                                        const ObIArray<uint64_t> &ignore_index_ids,
                                                        ObIArray<uint64_t> &valid_index_ids)
{
  int ret = OB_SUCCESS;
  valid_index_ids.reuse();
  const share::schema::ObTableSchema *index_schema = NULL;
  if (!ObOptimizerUtil::find_item(ignore_index_ids, ref_table_id) &&
      OB_FAIL(valid_index_ids.push_back(ref_table_id))) {
    LOG_WARN("fail to push back index id", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
    uint64_t index_id = tids[i];
    if (ObOptimizerUtil::find_item(ignore_index_ids, index_id)) {
      /* do nothing */
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))
                || OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get table schema", K(index_id), K(ret));
    } else if (index_schema->is_multivalue_index()) {
      /* do nothing */
    } else if (OB_FAIL(valid_index_ids.push_back(index_id))) {
      LOG_WARN("fail to push back index id", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::extract_geo_schema_info(const uint64_t table_id,
                                         const uint64_t index_id,
                                         ObWrapperAllocator &wrap_allocator,
                                         ColumnIdInfoMapAllocer &map_alloc,
                                         ColumnIdInfoMap &geo_columnInfo_map)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  const share::schema::ObTableSchema *index_schema = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("fail to get index schema", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(index_schema), K(ret));
  } else if (OB_FAIL(geo_columnInfo_map.create(OB_DEFAULT_SRID_BUKER, &map_alloc, &wrap_allocator))) {
    LOG_WARN("Init column_info_map failed", K(ret));
  } else {
    const ObRowkeyInfo* rowkey_info = NULL;
    const ObColumnSchemaV2 *column_schema = NULL;
    rowkey_info = &index_schema->get_rowkey_info();
    uint64_t column_id = OB_INVALID_ID;
    // traverse index schema
    for (int col_idx = 0;  OB_SUCC(ret) && col_idx < rowkey_info->get_size(); ++col_idx) {
      if (OB_FAIL(rowkey_info->get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column id", K(ret));
      } else if (OB_ISNULL(column_schema = (index_schema->get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else if (column_schema->is_spatial_cellid_column()) {
        const ObColumnSchemaV2 *geo_column_schema = NULL;
        uint64_t geo_col_id = column_schema->get_geo_col_id();
        ObGeoColumnInfo column_info;
        if (OB_ISNULL(geo_column_schema = (table_schema->get_column_schema(geo_col_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get column schema", K(column_id), K(ret));
        } else if (geo_column_schema->is_geometry()) {
          column_info.srid_ = geo_column_schema->get_srid();
          column_info.cellid_columnId_ = column_id;
          if (OB_FAIL(geo_columnInfo_map.set_refactored(geo_col_id, column_info))) {
            LOG_WARN("failed to set columnId_map", K(geo_col_id), K(ret));
          } else if (OB_FAIL(geo_columnInfo_map.set_refactored(column_id, column_info))) {
            LOG_WARN("failed to set columnId_map", K(column_id), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_query_range_info(const uint64_t table_id,
                                      const uint64_t base_table_id,
                                      const uint64_t index_id,
                                      QueryRangeInfo &range_info,
                                      PathHelper &helper)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObExecContext *exec_ctx = NULL;
  ObQueryRangeProvider *query_range_provider = NULL;
  const share::schema::ObTableSchema *index_schema = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  ObQueryRangeArray &ranges = range_info.get_ranges();
  ObQueryRangeArray &ss_ranges = range_info.get_ss_ranges();
  ObIArray<ColumnItem> &range_columns = range_info.get_range_columns();
  bool is_geo_index = false;
  bool is_multi_index = false;
  bool is_fts_index = false;
  bool is_domain_index = false;
  ObWrapperAllocator wrap_allocator(*allocator_);
  ColumnIdInfoMapAllocer map_alloc(OB_MALLOC_NORMAL_BLOCK_SIZE, wrap_allocator);
  ColumnIdInfoMap domain_columnInfo_map;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard()) ||
      OB_ISNULL(exec_ctx = opt_ctx->get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(schema_guard), K(exec_ctx), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema,
                                    ObSqlSchemaGuard::is_link_table(get_plan()->get_stmt(), table_id)))) {
    LOG_WARN("fail to get table schema", K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(base_table_id, table_schema,
                                    ObSqlSchemaGuard::is_link_table(get_plan()->get_stmt(), table_id)))) {
    LOG_WARN("fail to get table schema", K(index_id), K(ret));
  } else if (OB_ISNULL(index_schema) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_schema), K(ret));
  } else if (OB_FAIL(get_plan()->get_index_column_items(opt_ctx->get_expr_factory(),
                                                        table_id, *index_schema, range_columns))) {
    LOG_WARN("failed to generate rowkey column items", K(ret));
  } else if ((is_geo_index = index_schema->is_spatial_index())
              && OB_FAIL(extract_geo_schema_info(base_table_id,
                                                 index_id,
                                                 wrap_allocator,
                                                 map_alloc,
                                                 domain_columnInfo_map))) {
    LOG_WARN("failed to extract geometry schema info", K(ret), K(table_id), K(index_id));
  } else if (FALSE_IT(is_multi_index = index_schema->is_multivalue_index())) {
  } else if (FALSE_IT(is_fts_index = index_schema->is_fts_index_aux())) {
  } else {
    const ObSQLSessionInfo *session = opt_ctx->get_session_info();
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    bool all_single_value_range = false;
    int64_t equal_prefix_count = 0;
    int64_t equal_prefix_null_count = 0;
    int64_t range_prefix_count = 0;
    bool contain_always_false = false;
    bool has_exec_param = false;
    int64_t out_index_prefix = -1;
    bool is_domain_index = (is_geo_index || is_multi_index || is_fts_index);

    common::ObSEArray<ObRawExpr *, 4> agent_table_filter;
    bool is_oracle_inner_index_table = share::is_oracle_mapping_real_virtual_table(index_schema->get_table_id());
    if (is_oracle_inner_index_table
        && OB_FAIL(extract_valid_range_expr_for_oracle_agent_table(helper.filters_,
                                                                   agent_table_filter))) {
      LOG_WARN("failed to extract expr", K(ret));
    } else if (!is_domain_index && OB_FAIL(extract_preliminary_query_range(range_columns,
                                                                 is_oracle_inner_index_table
                                                                  ? agent_table_filter
                                                                    : helper.filters_,
                                                                 range_info.get_expr_constraints(),
                                                                 table_id,
								             query_range_provider,
                                                                 index_id,
                                                                 index_schema,
                                                                 out_index_prefix))) {
      LOG_WARN("failed to extract query range", K(ret), K(index_id));
    } else if (is_geo_index && OB_FAIL(extract_geo_preliminary_query_range(range_columns,
                                                                      is_oracle_inner_index_table
                                                                      ? agent_table_filter
                                                                        : helper.filters_,
                                                                      domain_columnInfo_map,
                                                                      query_range_provider))) {
      LOG_WARN("failed to extract query range", K(ret), K(index_id));
    } else if (is_multi_index
               && OB_FAIL(extract_multivalue_preliminary_query_range(range_columns,
                                                                     is_oracle_inner_index_table ?
                                                                       agent_table_filter : helper.filters_,
                                                                     query_range_provider))) {
      LOG_WARN("failed to extract query range", K(ret), K(index_id));
    } else if(is_fts_index && OB_FAIL(extract_fts_preliminary_query_range(range_columns,
                                                                          is_oracle_inner_index_table
                                                                          ? agent_table_filter
                                                                          : helper.filters_,
                                                                          table_schema,
                                                                          index_schema,
                                                                          helper,
                                                                          query_range_provider))) {
      LOG_WARN("failed to extract query range", K(ret), K(index_id));
    } else if (OB_ISNULL(query_range_provider)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(query_range_provider), K(ret));
    } else if (OB_FAIL(query_range_provider->get_tablet_ranges(*allocator_,
                                                               *exec_ctx,
                                                               ranges,
                                                               all_single_value_range,
                                                               dtc_params))) {
      LOG_WARN("failed to final extract query range", K(ret));
    } else if (OB_FAIL(query_range_provider->get_ss_tablet_ranges(*allocator_,
                                                                  *exec_ctx,
                                                                  ss_ranges,
                                                                  dtc_params))) {
      LOG_WARN("failed to final extract index skip query range", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_prefix_ranges_count(range_info.get_ranges(),
                                                                  equal_prefix_count,
                                                                  equal_prefix_null_count,
                                                                  range_prefix_count,
                                                                  contain_always_false))) {
      LOG_WARN("failed to compute query range prefix count", K(ret));
    } else if (OB_FAIL(check_has_exec_param(*query_range_provider, has_exec_param))) {
      LOG_WARN("failed to check has exec param", K(ret));
    } else if (!has_exec_param) {
      //没有exec param就使用真实query range计算equal prefix count
      //有exec param就使用query range的形状计算equal prefix count
      range_info.set_equal_prefix_count(equal_prefix_count);
      range_info.set_range_prefix_count(range_prefix_count);
      range_info.set_contain_always_false(contain_always_false);
    } else if (OB_FAIL(get_preliminary_prefix_info(*query_range_provider, range_info))) {
      LOG_WARN("failed to get preliminary prefix info", K(ret));
    }
    if (OB_SUCC(ret)) {
      range_info.set_valid();
      if (query_range_provider->is_new_query_range()) {
        range_info.set_pre_range_graph(static_cast<ObPreRangeGraph*>(query_range_provider));
      } else {
        range_info.set_query_range(static_cast<ObQueryRange*>(query_range_provider));
      }
      range_info.set_equal_prefix_null_count(equal_prefix_null_count);
      range_info.set_index_column_count(index_schema->is_index_table() ?
                                        index_schema->get_index_column_num() :
                                        index_schema->get_rowkey_column_num());
      range_info.set_index_prefix(out_index_prefix);
      LOG_TRACE("succeed to get query range", K(ranges), K(ss_ranges), K(helper.filters_),
                  KPC(query_range_provider), K(range_columns),
                  K(query_range_provider->get_range_exprs()), K(table_id), K(index_id));
    } else {
      if (NULL != query_range_provider) {
        query_range_provider->~ObQueryRangeProvider();
        query_range_provider = NULL;
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_has_exec_param(const ObQueryRangeProvider &query_range,
                                      bool &has_exec_param)
{
  int ret = OB_SUCCESS;
  has_exec_param = false;
  const ObIArray<ObRawExpr*> &range_exprs = query_range.get_range_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && !has_exec_param && i < range_exprs.count(); ++i) {
    ObRawExpr *expr = range_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
      has_exec_param = true;
    }
  }
  return ret;
}

int ObJoinOrder::get_preliminary_prefix_info(ObQueryRangeProvider &query_range_provider,
                                             QueryRangeInfo &range_info)
{
  int ret = OB_SUCCESS;
  int64_t equal_prefix_count = 0;
  int64_t range_prefix_count = 0;
  bool contain_always_false = false;
  if (OB_FAIL(query_range_provider.get_prefix_info(equal_prefix_count,
                                                   range_prefix_count,
                                                   contain_always_false))) {
    LOG_WARN("failed to get prefix info");
  } else {
    range_info.set_equal_prefix_count(equal_prefix_count);
    range_info.set_range_prefix_count(range_prefix_count);
    range_info.set_contain_always_false(contain_always_false);
    LOG_TRACE("success to get preliminary prefix info", K(equal_prefix_count),
                      K(range_prefix_count), K(contain_always_false));
  }
  return ret;
}

int ObJoinOrder::add_table_by_heuristics(const uint64_t table_id,
                                         const uint64_t ref_table_id,
                                         const ObIndexInfoCache &index_info_cache,
                                         const ObIArray<uint64_t> &candi_index_ids,
                                         ObIArray<uint64_t> &valid_index_ids,
                                         PathHelper &helper)
{
  int ret = OB_SUCCESS;
  uint64_t index_to_use = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) ||
      OB_UNLIKELY(OB_INVALID_ID == ref_table_id) ||
      OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    if (is_virtual_table(ref_table_id)
        && !share::is_oracle_mapping_real_virtual_table(ref_table_id)) {
      // check virtual table heuristics
      if (OB_FAIL(virtual_table_heuristics(table_id, ref_table_id, index_info_cache,
                                           candi_index_ids, index_to_use))) {
        LOG_WARN("failed to check virtual table heuristics", K(table_id), K(ref_table_id), K(ret));
      } else if (OB_INVALID_ID != index_to_use) {
        helper.table_opt_info_->optimization_method_ = OptimizationMethod::RULE_BASED;
        helper.table_opt_info_->heuristic_rule_ = HeuristicRule::VIRTUAL_TABLE_HEURISTIC;
        OPT_TRACE("choose index using heuristics for virtual table", index_to_use);
        LOG_TRACE("OPT:[RBO] choose index using heuristics for virtual table",
                  K(table_id),
                  K(ref_table_id),
                  K(index_to_use));
      }
    } else {
      //check whether we can use single table heuristics:
      if (OB_FAIL(user_table_heuristics(table_id, ref_table_id,
                                        index_info_cache,
                                        candi_index_ids,
                                        index_to_use,
                                        helper))) {
        LOG_WARN("Failed to check user_table_heuristics", K(ret));
      } else if (OB_INVALID_ID != index_to_use) {
        OPT_TRACE("choose primary/index using heuristics", index_to_use);
        LOG_TRACE("OPT:[RBO] choose primary/index using heuristics", K(table_id), K(index_to_use));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID != index_to_use) {
        if (OB_FAIL(valid_index_ids.push_back(index_to_use))) {
          LOG_WARN("failed to push back index id", K(ret));
        } else {
          OPT_TRACE("table added using heuristics", index_to_use);
          LOG_TRACE("OPT:[RBO] table added using heuristics",
               K(table_id), K(ref_table_id), K(index_to_use));
        }
      } else {
        OPT_TRACE("table not added using heuristics", ref_table_id);
        LOG_TRACE("OPT:[RBO] table not added using heuristics", K(table_id), K(ref_table_id));
      }
    }
  }
  return ret;
}

int ObJoinOrder::virtual_table_heuristics(const uint64_t table_id,
                                          const uint64_t ref_table_id,
                                          const ObIndexInfoCache &index_info_cache,
                                          const ObIArray<uint64_t> &valid_index_ids,
                                          uint64_t &index_to_use)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(get_plan()));
  } else if (OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard should not be null", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    // for virtual table, choose any index to use
    uint64_t idx_id = OB_INVALID_ID;
    const ObTableSchema *index_schema = NULL;
    const QueryRangeInfo *query_range_info = NULL;
    LOG_TRACE("OPT:[VT] begin search index for virtual table", K(valid_index_ids.count()));
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == idx_id &&  i < valid_index_ids.count(); ++i) {
      if (OB_UNLIKELY(OB_INVALID_ID == valid_index_ids.at(i))) {
        LOG_WARN("index id invalid", K(table_id), K(valid_index_ids.at(i)), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(valid_index_ids.at(i), index_schema))
                 || OB_ISNULL(index_schema)) {
        LOG_WARN("fail to get table schema", K(index_schema), K(valid_index_ids.at(i)), K(ret));
      } else if (!index_schema->is_index_table()) {
        /*do nothing*/
      } else if (OB_FAIL(index_info_cache.get_query_range(table_id, valid_index_ids.at(i),
                                                          query_range_info))) {
        LOG_WARN("failed to get query range", K(ret));
      } else if (OB_ISNULL(query_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_range_info should not be null", K(ret));
      } else if (query_range_info->is_index_column_get() ||
          (query_range_info->has_valid_range_condition() &&
          virtual_table_index_can_range_scan(valid_index_ids.at(i)))) {
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
 * 1 if search condition cover an unique index key (primary key is treated as unique key), and no need index_back, use that key,
 *   if there is multiple such key, choose the one with the minimum index key count
 * 2 if search condition cover an unique index key, need index_back, use that key
 *   and follows a refine process to find a better index
 * 3 otherwise generate all the access path and choose access path using cost model
 */
int ObJoinOrder::user_table_heuristics(const uint64_t table_id,
                                       const uint64_t ref_table_id,
                                       const ObIndexInfoCache &index_info_cache,
                                       const ObIArray<uint64_t> &valid_index_ids,
                                       uint64_t &index_to_use,
                                       PathHelper &helper)
{
  int ret = OB_SUCCESS;
  index_to_use = OB_INVALID_ID;
  const ObDMLStmt *stmt = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  QueryRangeInfo *range_info = NULL;
  IndexInfoEntry *index_info_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(schema_guard), K(stmt), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)
             || OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
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
      } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id,
                                                               index_info_entry))) {
        LOG_WARN("failed to get index info entry", K(ret));
      } else if (OB_ISNULL(index_info_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index info entry should not be null", K(ret));
      } else {
        range_info = &index_info_entry->get_range_info();
        index_col_num = range_info->get_index_column_count();
        LOG_TRACE("index info", K(index_info_entry->is_index_back()),
            K(index_info_entry->get_index_id()),
            K(index_info_entry->is_unique_index()), K(range_info->get_equal_prefix_count()),
            K(index_col_num), K(table_schema->get_column_count()));
        if (range_info->get_equal_prefix_count() >= index_col_num) {
          // all key covers
          if (!index_info_entry->is_index_back()) {
            if (index_info_entry->is_valid_unique_index()) {
              if (ukey_idx_without_indexback == OB_INVALID_ID ||
                  table_schema->get_column_count() < minimal_ukey_size_without_indexback) {
                ukey_idx_without_indexback = index_id;
                minimal_ukey_size_without_indexback = table_schema->get_column_count();
              }
            } else { /*do nothing*/ }
          } else if (index_info_entry->is_valid_unique_index()) {
            if (OB_FAIL(ukey_idx_with_indexback.push_back(index_id))) {
              LOG_WARN("failed to push back unique index with indexback", K(index_id), K(ret));
            } else { /* do nothing */ }
          } else { /* do nothing*/ }
        } else if (!index_info_entry->is_index_back()) {
          if (OB_FAIL(candidate_refine_index.push_back(index_id))) {
            LOG_WARN("failed to push back refine index id", K(ret));
          } else { /* do nothing*/ }
        } else { /* do nothing*/ }
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
          LOG_WARN("failed to refine heuristic index choosing",
              K(ukey_idx_with_indexback), K(ret));
        } else if (index_to_use == OB_INVALID_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index id", K(ret));
        } else {
          LOG_TRACE("finish to refine table heuristics", K(index_to_use));
        };
      } else { /* do nothing */ }
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
int ObJoinOrder::refine_table_heuristics_result(const uint64_t table_id,
                                                const uint64_t ref_table_id,
                                                const ObIArray<uint64_t> &candidate_refine_idx,
                                                const ObIArray<uint64_t> &match_unique_idx,
                                                const ObIndexInfoCache &index_info_cache,
                                                uint64_t &index_to_use)
{
  int ret = OB_SUCCESS;
  index_to_use = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) ||
      OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else {
    uint64_t refined_idx = OB_INVALID_ID;
    int64_t minimal_range_count = 0;
    ObSEArray<const QueryRangeInfo*, 4> match_range_info;
    ObSEArray<const OrderingInfo*, 4> match_ordering_info;
    const QueryRangeInfo *temp_range_info = NULL;
    const OrderingInfo *temp_ordering_info = NULL;
    DominateRelation status = DominateRelation::OBJ_UNCOMPARABLE;
    for(int64_t i = 0; OB_SUCC(ret) && i < match_unique_idx.count(); ++i) {
      uint64_t index_id = match_unique_idx.at(i);
      if (OB_FAIL(index_info_cache.get_query_range(table_id,
                                                   index_id,
                                                   temp_range_info))) {
        LOG_WARN("failed to get query range info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range info is NULL", K(ret));
      } else if (OB_FAIL(match_range_info.push_back(temp_range_info))) {
        LOG_WARN("failed to push back range info", K(ret));
      } else if (OB_FAIL(index_info_cache.get_access_path_ordering(table_id,
                                                                   index_id,
                                                                   temp_ordering_info))) {
        LOG_WARN("failed to get ordering info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_ordering_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ordering info is null", K(ret));
      } else if (OB_FAIL(match_ordering_info.push_back(temp_ordering_info))) {
        LOG_WARN("failed to push back ordering info", K(ret));
      } else {
        if (index_to_use == OB_INVALID_ID ||
            temp_range_info->get_ranges().count() < minimal_range_count) {
          index_to_use = index_id;
          minimal_range_count = temp_range_info->get_ranges().count();
        }
      }
    }
    // search all the candidate index to find a better one
    for(int64_t i = 0; OB_SUCC(ret) && i < candidate_refine_idx.count(); i++) {
      uint64_t index_id = candidate_refine_idx.at(i);
      if (OB_FAIL(index_info_cache.get_query_range(table_id,
                                                   index_id,
                                                   temp_range_info))) {
        LOG_WARN("failed to get range info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_range_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range info is null", K(ret));
      } else if (OB_FAIL(index_info_cache.get_access_path_ordering(table_id,
                                                                   index_id,
                                                                   temp_ordering_info))) {
        LOG_WARN("failed to get ordering info", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(temp_ordering_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ordering info is null", K(ret));
      } else { /* do nothing*/ }
      // examine all matched unique index
      for (int64_t j = 0; OB_SUCC(ret) && j < match_unique_idx.count(); j++) {
        if (temp_range_info->get_range_prefix_count()
            >= match_range_info.at(j)->get_range_prefix_count()) {
          if (OB_FAIL(check_index_subset(match_ordering_info.at(j),
                                         match_range_info.at(j)->get_range_prefix_count(),
                                         temp_ordering_info,
                                         temp_range_info->get_range_prefix_count(),
                                         status))) {
            LOG_WARN("failed to compare two index", K(ret));
          } else if (status == DominateRelation::OBJ_LEFT_DOMINATE ||
                     status == DominateRelation::OBJ_EQUAL) {
            if (refined_idx == OB_INVALID_ID ||
                temp_range_info->get_ranges().count() < minimal_range_count) {
              refined_idx = index_id;
              minimal_range_count = temp_range_info->get_ranges().count();
            }
            break;
          } else { /* do nothing*/ }
        }
      }
    }
    // finally refine the index if we get one
    if(OB_SUCC(ret)) {
      if (refined_idx != OB_INVALID_ID) {
        index_to_use = refined_idx;
      } else { /* do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::check_index_subset(const OrderingInfo *first_ordering_info,
                                    const int64_t first_index_key_count,
                                    const OrderingInfo *second_ordering_info,
                                    const int64_t second_index_key_count,
                                    DominateRelation &status)
{
  int ret = OB_SUCCESS;
  status = DominateRelation::OBJ_UNCOMPARABLE;
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
      status = first_index_key_count == second_index_key_count ?
          DominateRelation::OBJ_EQUAL : DominateRelation::OBJ_LEFT_DOMINATE;
    } else {
      status = DominateRelation::OBJ_UNCOMPARABLE;
    }
  } else {
    if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(second_ordering_info->get_index_keys(),
                                                     second_index_key_count,
                                                     first_ordering_info->get_index_keys(),
                                                     first_index_key_count,
                                                     is_subset))) {
      LOG_WARN("failed to compare index keys", K(ret));
    } else if (is_subset) {
      status = DominateRelation::OBJ_RIGHT_DOMINATE;
    } else {
      status = DominateRelation::OBJ_UNCOMPARABLE;
    }
  }
  LOG_TRACE("check index subset", K(*first_ordering_info), K(*second_ordering_info),
      K(first_index_key_count), K(second_index_key_count));
  return ret;
}

int ObJoinOrder::will_use_das(const uint64_t table_id,
                             const uint64_t index_id,
                             const ObIndexInfoCache &index_info_cache,
                             PathHelper &helper,
                             bool &create_das_path,
                             bool &create_basic_path)
{
  int ret = OB_SUCCESS;
  create_das_path = false;
  create_basic_path = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()));
  } else if (OB_FAIL(check_exec_force_use_das(table_id, create_das_path, create_basic_path))) {
    LOG_WARN("failed to check exec force use das", K(ret));
  } else if (create_das_path || create_basic_path) {
    LOG_TRACE("will use das by execution", K(create_das_path), K(create_basic_path));
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().check_use_das(table_id, create_das_path, create_basic_path))) {
    LOG_WARN("failed to check hint use das", K(ret));
  } else if (create_das_path || create_basic_path) {
    LOG_TRACE("will use das by hint", K(create_das_path), K(create_basic_path));
  } else if (OB_UNLIKELY(!get_plan()->get_optimizer_context().is_enable_distributed_das_scan())) {
    create_das_path = false;
    create_basic_path = true;
    LOG_TRACE("disable das scan by tenant config", K(create_das_path), K(create_basic_path));
  } else if (OB_FAIL(check_opt_rule_use_das(table_id,
                                            index_id,
                                            index_info_cache,
                                            helper.filters_,
                                            (get_plan()->get_is_rescan_subplan() || helper.is_inner_path_),
                                            create_das_path,
                                            create_basic_path))) {
    LOG_WARN("failed to check opt rule use das", K(ret));
  } else {
    LOG_TRACE("will use das by opt rule", K(create_das_path), K(create_basic_path));
  }
  return ret;
}

int ObJoinOrder::check_exec_force_use_das(const uint64_t table_id,
                                          bool &create_das_path,
                                          bool &create_basic_path)
{
  int ret = OB_SUCCESS;
  create_das_path = false;
  create_basic_path = false;
  const TableItem *table_item = nullptr;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(session_info), K(ret));
  } else if (OB_ISNULL(table_item = get_plan()->get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item is null", K(ret), K(table_id));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    // TODO: access virtual table by remote das task is not supported, it will report 4016 error in execute server
    // Ensure that the following scenarios will not combined with virtual table
    bool force_das_tsc = opt_ctx.in_nested_sql() || //contain nested sql(pl udf or in nested sql), trigger or foreign key in the top sql not force to use DAS TSC
                         opt_ctx.has_pl_udf() ||
                         opt_ctx.has_dblink() ||
                         opt_ctx.has_subquery_in_function_table() ||  //has function table
                         opt_ctx.has_cursor_expression() ||
                         (opt_ctx.has_var_assign() && session_info->is_var_assign_use_das_enabled()) ||
                         (opt_ctx.is_batched_multi_stmt() && table_item->is_basic_table()) || //batch update table(multi queries or arraybinding)
                         (table_item->for_update_ && table_item->skip_locked_ && session_info->get_pl_context()) // select for update skip locked stmt in PL use das force
                         ;
    bool is_select_sample_scan = get_plan()->get_stmt()->is_select_stmt()
                                && ((NULL != table_item->sample_info_ && !table_item->sample_info_->is_no_sample()) //block(row) sample scan do not support DAS TSC
                                    || (opt_ctx.is_online_ddl() && opt_ctx.get_root_stmt()->is_insert_stmt()) // online ddl plan use sample table scan, create index not support DAS TSC
                                   );

    if (EXTERNAL_TABLE == table_item->table_type_
        || is_select_sample_scan
        || is_virtual_table(table_item->ref_id_)) {
      create_das_path = false;
      create_basic_path = true;
    } else if (force_das_tsc) { //this sql force to use DAS TSC
      create_das_path = true;
      create_basic_path = false;
    }
  }
  return ret;
}

int ObJoinOrder::check_opt_rule_use_das(const uint64_t table_id,
                                        const uint64_t index_id,
                                        const ObIndexInfoCache &index_info_cache,
                                        const ObIArray<ObRawExpr*> &filters,
                                        const bool is_rescan,
                                        bool &create_das_path,
                                        bool &create_basic_path)
{
  int ret = OB_SUCCESS;
  create_das_path = false;
  create_basic_path = false;
  IndexInfoEntry *index_info_entry = NULL;
  if (is_rescan) {
    create_das_path = true;
    create_basic_path = (table_meta_info_.is_broadcast_table_
                         || (OB_SUCCESS != (OB_E(EventTable::EN_GENERATE_PLAN_WITH_NLJ) OB_SUCCESS)))
                        ? false : true;
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id, index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(index_info_entry));
  } else if (OB_FAIL(check_use_das_by_false_startup_filter(*index_info_entry, filters, create_das_path))) {
    LOG_WARN("failed to check use das by false startup filter", K(ret));
  } else if (create_das_path) {
    /* do nothing */
  } else if (index_info_entry->is_index_global() && index_info_entry->is_index_back()) {
    int64_t explicit_dop = ObGlobalHint::UNSET_PARALLEL;
    if (OB_FAIL(get_explicit_dop_for_path(index_id, explicit_dop))) {
      LOG_WARN("failed to get explicit dop", K(ret));
    } else if (ObGlobalHint::UNSET_PARALLEL == explicit_dop) {
      // for global index use auto dop, create das path and basic path, after get auto dop result, prune unnecessary path
      create_das_path = true;
      create_basic_path = true;
    } else if (ObGlobalHint::DEFAULT_PARALLEL == explicit_dop) {
      create_das_path = true;
    } else {
      create_basic_path = true;
    }
  } else {
    create_basic_path = true;
  }
  return ret;
}

int ObJoinOrder::check_use_das_by_false_startup_filter(const IndexInfoEntry &index_info_entry,
                                                       const ObIArray<ObRawExpr*> &filters,
                                                       bool &use_das)
{
  int ret = OB_SUCCESS;
  use_das = false;
  if (OB_ISNULL(index_info_entry.get_sharding_info()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_info_entry.get_sharding_info()), K(get_plan()), K(ret));
  } else if (!index_info_entry.get_sharding_info()->is_sharding()) {
    /* do nothing */
  } else {
    ObRawExpr *expr = NULL;
    for (int64_t i = 0; !use_das && OB_SUCC(ret) && i < filters.count(); ++i) {
      if (OB_ISNULL(expr = filters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (T_BOOL != expr->get_expr_type()) {
        /* do nothing */
      } else if (OB_FAIL(ObOptimizerUtil::check_is_static_false_expr(get_plan()->get_optimizer_context(),
                                                                     *expr,
                                                                     use_das))) {
        LOG_WARN("failed to check is static false", K(ret));
      }
    }
  }

  return ret;
}

void ObVecIdxExtraInfo::set_vec_algorithm_by_index_type(ObIndexType index_type)
{
  switch (index_type) {
    case ObIndexType::INDEX_TYPE_VEC_ROWKEY_VID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_VID_ROWKEY_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_INDEX_ID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL: {
      algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_HNSW;
      break;
    }
    case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL: {
      algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_IVF_FLAT;
      break;
    }
    case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_META_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL: {
      algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_IVF_SQ8;
      break;
    }
    case ObIndexType::INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL:
    case ObIndexType::INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL: {
      algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_IVF_PQ;
      break;
    }
    default: {
      algorithm_type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
      break;
    }
  }
}

int ObJoinOrder::process_vec_index_info(const ObDMLStmt *stmt,
                                        const uint64_t table_id,
                                        const uint64_t ref_table_id,
                                        const uint64_t index_id,
                                        AccessPath &access_path)
{
  int ret = OB_SUCCESS;

  bool found_scan_match_expr = false;
  ObRawExpr *vector_expr = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *vec_index_schema = nullptr;
  const ObSelectStmt *select_stmt = NULL;
  bool vector_index_match = false;
  uint64_t vec_index_id = OB_INVALID_ID;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())
    || OB_ISNULL(session_info = OPT_CTX.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KP(stmt), KP(schema_guard), KP(session_info), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index table schema", K(ret), K(index_id));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to index schema", K(ret));
  } else if (!index_schema->is_vec_index()) {
    bool has_aggr = false;
    vector_expr = stmt->get_first_vector_expr();
    if (stmt->is_select_stmt() && FALSE_IT(select_stmt = static_cast<const ObSelectStmt*>(stmt))) {
    } else if (nullptr != select_stmt && FALSE_IT(has_aggr = select_stmt->get_aggr_item_size() > 0)) {
    } else if (OB_FAIL(get_vector_index_tid_from_expr(schema_guard, vector_expr, table_id, ref_table_id, has_aggr, vector_index_match, vec_index_id))) {
      LOG_WARN("failed to get matched vector index table id", K(ret));
    } else if (vec_index_id == OB_INVALID_ID || !vector_index_match) {
      // not vec expr column, ignore
    } else if (OB_FAIL(access_path.domain_idx_info_.func_lookup_index_ids_.push_back(vec_index_id))) {
      LOG_WARN("failed to push_back vec index tale id", K(ret));
    } else if (OB_FAIL(access_path.domain_idx_info_.func_lookup_exprs_.push_back(vector_expr))) {
      LOG_WARN("failed to push back vector expr", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(vec_index_id, vec_index_schema))) {
      LOG_WARN("failed to get index table schema", K(ret), K(vec_index_id));
    } else if (OB_ISNULL(vec_index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to index schema", K(ret));
    } else {
      access_path.domain_idx_info_.vec_extra_info_.set_vec_idx_type(ObVecIndexType::VEC_INDEX_PRE);
      if (index_schema->is_spatial_index()) {
        access_path.domain_idx_info_.vec_extra_info_.set_force_vec_index_type(ObVecIndexType::VEC_INDEX_PRE);
      } else if (index_schema->is_multivalue_index()) {
        access_path.domain_idx_info_.vec_extra_info_.set_force_vec_index_type(ObVecIndexType::VEC_INDEX_POST);
      } else if (index_schema->is_fts_index()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "when using vector index, full-text index is");
      }
    }
  } else {
    vec_index_schema = index_schema;
    access_path.domain_idx_info_.vec_extra_info_.set_vec_idx_type(ObVecIndexType::VEC_INDEX_POST);
    vector_index_match = true;
  }

  if (OB_FAIL(ret) || !vector_index_match) {
  } else if (OB_ISNULL(vec_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_table_schema unexpected null", K(ret));
  } else {
    // forbit weak read in both pre/post vec filter
    bool is_weak_read = false;
    int64_t route_policy_type = 0;
    if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy_type))) {
      LOG_WARN("fail to get sys variable", K(ret));
    } else if (COLUMN_STORE_ONLY == static_cast<ObRoutePolicyType>(route_policy_type)) {
      // do not check weak read
    } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
      is_weak_read = true;
    } else {
      ObConsistencyLevel consistency_level = INVALID_CONSISTENCY;
      if (OB_UNLIKELY(INVALID_CONSISTENCY
              != OPT_CTX.get_query_ctx()->get_global_hint().read_consistency_)) {
        consistency_level = OPT_CTX.get_query_ctx()->get_global_hint().read_consistency_;
      } else {
        consistency_level = session_info->get_consistency_level();
      }
      if (WEAK == consistency_level || FROZEN == consistency_level) {
        is_weak_read = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_weak_read) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "when using vector index, weak read is");
    } else {
      double selectivity = 0.0;
      if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                        get_plan()->get_selectivity_ctx(),
                                                        get_restrict_infos(),
                                                        selectivity,
                                                        get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to calculate selectivity", K(ret));
      } else {
        access_path.domain_idx_info_.vec_extra_info_.set_vec_algorithm_by_index_type(vec_index_schema->get_index_type());
        access_path.domain_idx_info_.set_domain_idx_type(DomainIndexType::VEC_INDEX);
        access_path.domain_idx_info_.vec_extra_info_.set_selectivity(selectivity);
        // for optimize, distance expr just for order by needn't calculate
        // using vsag calc result is ok
        if (OB_NOT_NULL(vector_expr) &&
            access_path.domain_idx_info_.vec_extra_info_.is_hnsw_vec_scan()
            &&!stmt->is_contain_vector_origin_distance_calc()) {
          FLOG_INFO("distance needn't calc", K(ret));
          vector_expr->add_flag(IS_CUT_CALC_EXPR);
        }
      }
    } // not weak read
  }

  return ret;
}

int ObJoinOrder::create_one_access_path(const uint64_t table_id,
                                        const uint64_t ref_id,
                                        const uint64_t index_id,
                                        const ObIndexInfoCache &index_info_cache,
                                        PathHelper &helper,
                                        AccessPath *&access_path,
                                        bool use_das,
                                        bool use_column_store,
                                        OptSkipScanState use_skip_scan)
{
  int ret = OB_SUCCESS;
  IndexInfoEntry *index_info_entry = NULL;
  access_path = NULL;
  AccessPath *ap = NULL;
  bool is_nl_with_extended_range = false;
  const TableItem *table_item = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == ref_id) || OB_UNLIKELY(OB_INVALID_ID == index_id) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(allocator_) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = get_plan()->get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ref_id), K(index_id), K(get_plan()), K(allocator_),
                                    K(table_item), K(ret));
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id,
                                                           index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(index_info_entry) || OB_ISNULL(index_info_entry->get_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info entry should not be null", K(ret), KPC(index_info_entry));
  } else if (!helper.force_inner_nl_ && helper.is_inner_path_ &&
             (index_info_entry->get_ordering_info().get_index_keys().count() <= 0)) {
    LOG_TRACE("skip adding inner access path due to wrong index key count",
                K(table_id), K(ref_id), KPC(index_info_entry));
  } else if (OB_ISNULL(ap = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate an AccessPath", K(ret));
  } else {
    LOG_TRACE("OPT:start to create access path",
                K(table_id), K(ref_id), K(index_id), K(helper.is_inner_path_), K(use_das));
    const QueryRangeInfo &range_info = index_info_entry->get_range_info();
    const OrderingInfo &ordering_info = index_info_entry->get_ordering_info();
    ap = new(ap) AccessPath(table_id, ref_id, index_id, this, ordering_info.get_scan_direction());
    ap->is_get_ = range_info.is_get();
    ap->is_global_index_ = index_info_entry->is_index_global();
    ap->table_partition_info_ = index_info_entry->get_partition_info();
    ap->use_das_ = use_das;
    ap->contain_das_op_ = ap->use_das_;
    ap->est_cost_info_.is_das_scan_ = ap->use_das_;
    ap->is_hash_index_ = is_virtual_table(ref_id) && ref_id != index_id;
    ap->est_cost_info_.index_meta_info_.is_index_back_ = index_info_entry->is_index_back();
    ap->est_cost_info_.index_meta_info_.is_unique_index_ = index_info_entry->is_unique_index();
    ap->est_cost_info_.index_meta_info_.is_global_index_ = index_info_entry->is_index_global();
    ap->est_cost_info_.index_meta_info_.is_geo_index_ = index_info_entry->is_index_geo();
    ap->est_cost_info_.index_meta_info_.is_multivalue_index_ = index_info_entry->is_multivalue_index();
    ap->est_cost_info_.index_meta_info_.is_fulltext_index_ = index_info_entry->is_fulltext_index();
    ap->est_cost_info_.index_meta_info_.is_vector_index_ = index_info_entry->is_vector_index();
    ap->est_cost_info_.is_virtual_table_ = is_virtual_table(ref_id);
    ap->est_cost_info_.table_metas_ = &get_plan()->get_basic_table_metas();
    ap->est_cost_info_.sel_ctx_ = &get_plan()->get_selectivity_ctx();
    ap->est_cost_info_.is_unique_ = ap->is_get_ ||
                                     (index_info_entry->is_unique_index()
                                      && range_info.is_index_column_get()
                                      && index_info_entry->is_valid_unique_index());
    ap->est_cost_info_.rescan_server_list_ = &ap->server_list_;
    ap->table_opt_info_ = helper.table_opt_info_;
    ap->is_inner_path_ = helper.is_inner_path_;
    ap->est_cost_info_.is_rescan_ = helper.is_inner_path_ || get_plan()->get_is_rescan_subplan();
    ap->range_prefix_count_ = index_info_entry->get_range_info().get_range_prefix_count();
    ap->interesting_order_info_ = index_info_entry->get_interesting_order_info();
    ap->for_update_ = table_item->for_update_;
    ap->use_skip_scan_ = use_skip_scan;
    ap->index_prefix_ = index_info_entry->get_range_info().get_index_prefix();
    ap->use_column_store_ = use_column_store;
    ap->est_cost_info_.use_column_store_ = use_column_store;
    ap->force_direction_ = index_info_entry->is_force_direction();

    ap->contain_das_op_ = ap->use_das_;
    ap->is_ordered_by_pk_ = (ref_id == index_id) ? true
        : range_info.get_equal_prefix_count() >= range_info.get_index_column_count();
    if (get_plan()->get_stmt()->has_vec_approx() && OB_FAIL(process_vec_index_info(get_plan()->get_stmt(), table_id, ref_id, index_id, *ap))) {
      LOG_WARN("failed to init vec_index_info", K(ret));
    } else if (OB_FAIL(process_index_for_match_expr(table_id, ref_id, index_id, helper, *ap))) {
      LOG_WARN("failed to process index for match expr", K(ret));
    } else if (OB_FAIL(init_sample_info_for_access_path(ap, table_id, table_item))) {
      LOG_WARN("failed to init sample info", K(ret));
    } else if (OB_FAIL(add_access_filters(ap,
                                          range_info.get_range_columns(),
                                          (nullptr == range_info.get_query_range_provider()) ? nullptr : &range_info.get_query_range_provider()->get_range_exprs(),
                                          (nullptr == range_info.get_query_range_provider()) ? nullptr : &range_info.get_query_range_provider()->get_unprecise_range_exprs(),
                                          helper))) {
      LOG_WARN("failed to add access filters", K(*ap), K(ordering_info.get_index_keys()), K(ret));
    } else if (!helper.is_index_merge_
               && OB_FAIL(get_plan()->get_stmt()->get_column_items(table_id, ap->est_cost_info_.access_column_items_))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if ((!ap->is_global_index_ ||
                !index_info_entry->is_index_back() ||
                get_plan()->get_optimizer_context().is_das_keep_order_enabled())
        // for global index lookup without keep order, the ordering is wrong.
        && OB_FAIL(ObOptimizerUtil::make_sort_keys(ordering_info.get_ordering(),
                                                   ordering_info.get_scan_direction(),
                                                   ap->ordering_))) {
      LOG_WARN("failed to create index keys expression array", K(index_id), K(ret));
    } else if (ordering_info.get_index_keys().count() > 0) {
      ap->pre_query_range_ = const_cast<ObQueryRange *>(range_info.get_query_range());
      ap->pre_range_graph_ = const_cast<ObPreRangeGraph*>(range_info.get_pre_range_graph());
      if (OB_FAIL(ap->index_keys_.assign(ordering_info.get_index_keys()))) {
        LOG_WARN("failed to get index keys", K(ret));
      } else if (OB_FAIL(ap->est_cost_info_.range_columns_.assign(range_info.get_range_columns()))) {
        LOG_WARN("failed to assign range columns", K(ret));
      } else if (OB_FAIL(fill_query_range_info(range_info, ap->est_cost_info_,
                                               OptSkipScanState::SS_DISABLE != use_skip_scan))) {
        LOG_WARN("failed to fill query range info", K(ret));
      } else { /*do nothing*/ }
    } else { /*do nothing*/ }
    ObSEArray<ObRawExpr*, 4> index_merge_access_cols;
    if (OB_SUCC(ret) && helper.is_index_merge_) {
      if (OB_FAIL(get_plan()->get_rowkey_exprs(table_id, ref_id, index_merge_access_cols))) {
        LOG_WARN("failed to get rowkey exprs", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < helper.filters_.count(); ++i) {
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(helper.filters_.at(i), index_merge_access_cols))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < ap->est_cost_info_.range_columns_.count(); ++i) {
      ColumnItem &col = ap->est_cost_info_.range_columns_.at(i);
      ObRawExpr* col_expr = NULL;
      if (NULL == (col_expr = get_plan()->get_stmt()->get_column_expr_by_id(col.table_id_, col.column_id_))) {
        // do nothing
      } else if (helper.is_index_merge_ && !ObOptimizerUtil::find_item(index_merge_access_cols, col_expr)) {
        // do nothing
      } else if (OB_FAIL(ap->est_cost_info_.index_access_column_items_.push_back(col))) {
        LOG_WARN("failed to push back column item", K(ret));
      }
    }
    if (OB_SUCC(ret) && helper.is_index_merge_) {
      if (OB_FAIL(ap->est_cost_info_.access_column_items_.assign(ap->est_cost_info_.index_access_column_items_))) {
        LOG_WARN("failed to assign access column item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_filters(ap->filter_,
                               ap->get_query_range_provider(),
                               ap->est_cost_info_,
                               ap->domain_idx_info_,
                               is_nl_with_extended_range,
                               ObSqlSchemaGuard::is_link_table(get_plan()->get_stmt(), table_id),
                               OptSkipScanState::SS_DISABLE != use_skip_scan))) {
        LOG_WARN("failed to fill filters for cost table info", K(ret));
      } else if (OB_FAIL(init_filter_selectivity(ap->est_cost_info_))) {
        LOG_WARN("failed to calc filter sel", K(ret));
      } else if (!helper.is_inner_path_ &&
                OB_FAIL(increase_diverse_path_count(ap))) {
        LOG_WARN("failed to increase diverse path count", K(ret));
      } else if (OB_FAIL(ap->subquery_exprs_.assign(helper.subquery_exprs_))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else if (OB_FAIL(ap->equal_param_constraints_.assign(helper.equal_param_constraints_))) {
        LOG_WARN("failed to assign equal param constraints", K(ret));
      } else if (OB_FAIL(ap->const_param_constraints_.assign(helper.const_param_constraints_))) {
        LOG_WARN("failed to assign equal param constraints", K(ret));
      } else if (OB_FAIL(ap->expr_constraints_.assign(range_info.get_expr_constraints()))) {
        LOG_WARN("failed to assign expr constraints", K(ret));
      } else if (OB_FAIL(append(ap->expr_constraints_, helper.expr_constraints_))) {
        LOG_WARN("append expr constraints failed", K(ret));
      } else if (OB_FAIL(init_column_store_est_info(table_id, ref_id, ap->est_cost_info_))) {
        LOG_WARN("failed to init column store est cost info", K(ret));
      } else if (OB_FAIL(ap->compute_access_path_batch_rescan())) {
        LOG_WARN("failed to compute access path batch rescan", K(ret));
      } else {
        access_path = ap;
      }
    }
    LOG_TRACE("OPT:succeed to create one access path",
                K(table_id), K(ref_id), K(index_id), K(helper.is_inner_path_));
  }
  return ret;
}

int ObJoinOrder::init_sample_info_for_access_path(AccessPath *ap,
                                                  const uint64_t table_id,
                                                  const TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!get_plan()->get_stmt()->is_select_stmt()) {
    // do nothing
    // sample scan doesn't support DML other than SELECT.
  } else {
    const ObSelectStmt *stmt = static_cast<const ObSelectStmt *>(get_plan()->get_stmt());
    const SampleInfo *sample_info = table_item->sample_info_;

    if (sample_info != NULL) {
      ap->sample_info_ = *sample_info;
      ap->sample_info_.table_id_ = ap->get_index_table_id();
    } else if (get_plan()->get_optimizer_context().is_online_ddl() &&
               get_plan()->get_optimizer_context().get_root_stmt()->is_insert_stmt() &&
               !get_plan()->get_optimizer_context().is_heap_table_ddl()) {
      ap->sample_info_.method_ = SampleInfo::SampleMethod::BLOCK_SAMPLE;
      ap->sample_info_.scope_ = SampleInfo::SAMPLE_ALL_DATA;
      ap->sample_info_.percent_ = (double)get_plan()->get_optimizer_context().get_px_object_sample_rate() / 1000;
      ap->sample_info_.table_id_ = ap->get_index_table_id();
    }
    ap->est_cost_info_.sample_info_ = ap->sample_info_;
  }
  return ret;
}

int ObJoinOrder::init_filter_selectivity(ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = get_plan();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             est_cost_info.prefix_filters_,
                                                             est_cost_info.prefix_filter_sel_,
                                                             plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             est_cost_info.pushdown_prefix_filters_,
                                                             est_cost_info.pushdown_prefix_filter_sel_,
                                                             plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             est_cost_info.ss_postfix_range_filters_,
                                                             est_cost_info.ss_postfix_range_filters_sel_,
                                                             plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             est_cost_info.postfix_filters_,
                                                             est_cost_info.postfix_filter_sel_,
                                                             plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             est_cost_info.table_filters_,
                                                             est_cost_info.table_filter_sel_,
                                                             plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  }
  return ret;
}

int ObJoinOrder::init_column_store_est_info(const uint64_t table_id,
                                            const uint64_t ref_id,
                                            ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  bool index_back_will_use_row_store = false;
  bool index_back_will_use_column_store = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else if (OB_FAIL(get_plan()->will_use_column_store(OB_INVALID_ID,
                                                       ref_id,
                                                       ref_id,
                                                       index_back_will_use_column_store,
                                                       index_back_will_use_row_store))) {
    LOG_WARN("failed to check will use column store", K(ret));
  } else if (est_cost_info.use_column_store_ || !index_back_will_use_row_store) {
    FilterCompare filter_compare(get_plan()->get_predicate_selectivities());
    lib::ob_sort(est_cost_info.table_filters_.begin(), est_cost_info.table_filters_.end(), filter_compare);
    ObSqlBitSet<> used_column_ids;
    est_cost_info.index_back_with_column_store_ = !index_back_will_use_row_store;
    const OptTableMetas& table_opt_meta = get_plan()->get_basic_table_metas();
    ObIArray<ObCostColumnGroupInfo> &index_scan_column_group_infos = est_cost_info.index_scan_column_group_infos_;
    ObIArray<ObCostColumnGroupInfo> &index_back_column_group_infos = est_cost_info.index_meta_info_.is_index_back_ ?
                                                          est_cost_info.index_back_column_group_infos_
                                                          : est_cost_info.index_scan_column_group_infos_;
    //add column group with prefix filters
    if (OB_FAIL(init_column_store_est_info_with_filter(table_id,
                                                        est_cost_info,
                                                        table_opt_meta,
                                                        est_cost_info.prefix_filters_,
                                                        index_scan_column_group_infos,
                                                        used_column_ids,
                                                        filter_compare,
                                                        false))) {
      LOG_WARN("failed to init column store est info with filter", K(ret));
    }
    else if (OB_FAIL(init_column_store_est_info_with_filter(table_id,
                                                            est_cost_info,
                                                            table_opt_meta,
                                                            est_cost_info.pushdown_prefix_filters_,
                                                            index_scan_column_group_infos,
                                                            used_column_ids,
                                                            filter_compare,
                                                            false))) {
      LOG_WARN("failed to init column store est info with filter", K(ret));
    }
    //add column group with postfix filters
    else if (OB_FAIL(init_column_store_est_info_with_filter(table_id,
                                                            est_cost_info,
                                                            table_opt_meta,
                                                            est_cost_info.postfix_filters_,
                                                            index_scan_column_group_infos,
                                                            used_column_ids,
                                                            filter_compare,
                                                            true))) {
      LOG_WARN("failed to init column store est info with filter", K(ret));
    }
    //add column group with index back filters
    else if (OB_FAIL(init_column_store_est_info_with_filter(table_id,
                                                            est_cost_info,
                                                            table_opt_meta,
                                                            est_cost_info.table_filters_,
                                                            index_back_column_group_infos,
                                                            used_column_ids,
                                                            filter_compare,
                                                            true))) {
      LOG_WARN("failed to init column store est info with filter", K(ret));
    }
    //add other column group
    else if (OB_FAIL(init_column_store_est_info_with_other_column(table_id,
                                                                  est_cost_info,
                                                                  table_opt_meta,
                                                                  used_column_ids))) {
      LOG_WARN("failed to init column store est info with other column", K(ret));
    } else if (index_scan_column_group_infos.empty()) {
      //add dummy column group cost info for nil access exprs
      ObCostColumnGroupInfo cg_info;
      if (OB_FAIL(index_scan_column_group_infos.push_back(cg_info))) {
        LOG_WARN("failed to push back column group info", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::init_column_store_est_info_with_filter(const uint64_t table_id,
                                                        ObCostTableScanInfo &est_cost_info,
                                                        const OptTableMetas& table_opt_meta,
                                                        ObIArray<ObRawExpr*> &filters,
                                                        ObIArray<ObCostColumnGroupInfo> &column_group_infos,
                                                        ObSqlBitSet<> &used_column_ids,
                                                        FilterCompare &filter_compare,
                                                        const bool use_filter_sel)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> filter_columns;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObRawExpr *filter = filters.at(i);
    filter_columns.reuse();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter,
                                                    filter_columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    //init column group info
    for (int j = 0; OB_SUCC(ret) && j < filter_columns.count(); ++j) {
      ObRawExpr *expr = filter_columns.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        ObDMLStmt *stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt());
        ColumnItem *col_item = stmt->get_column_item(table_id, col_expr->get_column_id());
        const OptColumnMeta* col_opt_meta = table_opt_meta.get_column_meta_by_table_id(
                                    table_id,
                                    col_expr->get_column_id());
        if (used_column_ids.has_member(col_expr->get_column_id())) {
          //do nothing
        } else if (OB_FAIL(used_column_ids.add_member(col_expr->get_column_id()))) {
          LOG_WARN("failed to add memeber", K(ret));
        } else if (OB_ISNULL(col_opt_meta) || OB_ISNULL(col_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column meta", K(ret));
        } else {
          ObCostColumnGroupInfo cg_info;
          cg_info.micro_block_count_ = col_opt_meta->get_cg_micro_blk_cnt();
          cg_info.column_id_ = col_expr->get_column_id();
          cg_info.skip_rate_ = col_opt_meta->get_cg_skip_rate();
          if (OB_FAIL(cg_info.access_column_items_.push_back(*col_item))) {
            LOG_WARN("failed to push back filter", K(ret));
          } else if (OB_FAIL(column_group_infos.push_back(cg_info))) {
            LOG_WARN("failed to push back column group info", K(ret));
          }
        }
      }
    }
    //distribute filter
    int max_pos = -1;
    for (int j = 0; OB_SUCC(ret) && j < filter_columns.count(); ++j) {
      ObRawExpr *expr = filter_columns.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        int find_pos = -1;
        for (int k = 0; OB_SUCC(ret) && find_pos < 0 && k < column_group_infos.count(); ++k) {
          ObCostColumnGroupInfo &cg_info = column_group_infos.at(k);
          if (cg_info.column_id_ == col_expr->get_column_id()) {
            find_pos = k;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (find_pos < 0) {
          //ignore index column group
        } else if (find_pos > max_pos) {
          max_pos = find_pos;
        }
      }
    }
    if (OB_FAIL(ret) || filter_columns.empty()) {
    } else if (max_pos < 0 || max_pos >= column_group_infos.count()) {
      //table filter with index column group
    } else if (OB_FAIL(column_group_infos.at(max_pos).filters_.push_back(filter))) {
      LOG_WARN("failed to push back filter", K(ret));
    } else if (use_filter_sel) {
      column_group_infos.at(max_pos).filter_sel_ *= filter_compare.get_selectivity(filter);
    }
  }
  return ret;
}

int ObJoinOrder::init_column_store_est_info_with_other_column(const uint64_t table_id,
                                                              ObCostTableScanInfo &est_cost_info,
                                                              const OptTableMetas& table_opt_meta,
                                                              ObSqlBitSet<> &used_column_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObCostColumnGroupInfo> &column_group_infos = est_cost_info.index_meta_info_.is_index_back_ ?
                                          est_cost_info.index_back_column_group_infos_
                                          : est_cost_info.index_scan_column_group_infos_;
  for (int i = 0; OB_SUCC(ret) && i < est_cost_info.access_column_items_.count(); ++i) {
    uint64_t column_id = est_cost_info.access_column_items_.at(i).column_id_;
    const OptColumnMeta* col_opt_meta = table_opt_meta.get_column_meta_by_table_id(
                                    table_id,
                                    column_id);
    ObRawExpr *expr = est_cost_info.access_column_items_.at(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (used_column_ids.has_member(column_id)) {
      //do nothing
    } else if (expr->get_ref_count() <= 0) {
      //do nothing
    } else if (OB_ISNULL(col_opt_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null column meta", K(ret));
    } else {
      ObCostColumnGroupInfo cg_info;
      cg_info.micro_block_count_ = col_opt_meta->get_cg_micro_blk_cnt();
      cg_info.skip_rate_ = col_opt_meta->get_cg_skip_rate();
      cg_info.column_id_ = column_id;
      if (OB_FAIL(cg_info.access_column_items_.push_back(est_cost_info.access_column_items_.at(i)))) {
        LOG_WARN("failed to push back filter", K(ret));
      } else if (OB_FAIL(column_group_infos.push_back(cg_info))) {
        LOG_WARN("failed to push back column group info", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_access_path_ordering(const uint64_t table_id,
                                          const uint64_t ref_table_id,
                                          const uint64_t index_id,
                                          common::ObIArray<ObRawExpr*> &index_keys,
                                          common::ObIArray<ObRawExpr*> &ordering,
                                          ObOrderDirection &direction,
                                          bool &force_direction,
                                          const bool is_index_back)
{
  int ret = OB_SUCCESS;
  direction = default_asc_direction();
  const ObDMLStmt *stmt = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *index_schema = NULL;
  ObOrderDirection hint_direction = UNORDERED;
  force_direction = false;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(stmt = get_plan()->get_stmt())
      || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
      || OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())
      || OB_ISNULL(opt_ctx->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error",
             K(get_plan()), K(stmt), K(opt_ctx),  K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema, ObSqlSchemaGuard::is_link_table(stmt, table_id)))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(index_schema), K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema should not be null", K(index_schema), K(ret));
  } else if (OB_FAIL(get_plan()->get_rowkey_exprs(table_id, *index_schema, index_keys))) {
    LOG_WARN("NULL pointer error", K(ret), K(table_id), K(ref_table_id), K(index_id));
  } else if (!index_schema->is_ordered()) {
    // for virtual table, we have HASH index which offers no ordering on index keys
  } else if (index_schema->is_global_index_table() && is_index_back && !opt_ctx->is_das_keep_order_enabled()) {
    // for global index lookup without keep order, the ordering is wrong.
  } else if (OB_FAIL(append(ordering, index_keys))) {
    LOG_WARN("failed to append index ordering expr", K(ret));
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().check_scan_direction(*opt_ctx->get_query_ctx(),
                                                                          table_id,
                                                                          index_id,
                                                                          hint_direction))) {
    LOG_WARN("failed to check scan direction", K(ret), K(table_id));
  } else if (UNORDERED != hint_direction) {
    direction = hint_direction;
    force_direction = true;
  } else if (OB_FAIL(get_index_scan_direction(ordering, stmt,
                                              get_plan()->get_equal_sets(), direction))) {
    LOG_WARN("failed to get index scan direction", K(ret));
  }
  return ret;
}

/**
 * 获取索引扫描的序
 * 首先检查window function中的order by, 然后在检查stmt中的order by
 */
int ObJoinOrder::get_index_scan_direction(const ObIArray<ObRawExpr *> &keys,
                                          const ObDMLStmt *stmt,
                                          const EqualSets &equal_sets,
                                          ObOrderDirection &index_direction)
{
  int ret = OB_SUCCESS;
  index_direction = default_asc_direction();
  bool check_order_by = true;
  int64_t order_match_count = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(get_plan()));
  } else if (stmt->is_select_stmt()) {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(stmt);
    int64_t max_order_match_count = 0;
    const ObWinFunRawExpr *win_expr = NULL;
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
                                          0, // index start offset
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

int ObJoinOrder::get_direction_in_order_by(const ObIArray<OrderItem> &order_by,
                                           const ObIArray<ObRawExpr *> &index_keys,
                                           const int64_t index_start_offset,
                                           const EqualSets &equal_sets,
                                           const ObIArray<ObRawExpr *> &const_exprs,
                                           ObOrderDirection &direction,
                                           int64_t &order_match_count)
{
  int ret = OB_SUCCESS;
  order_match_count = 0;
  int64_t order_offset = order_by.count();
  int64_t index_offset = index_start_offset;
  bool is_const = true;
  //找到第一个非const的order项
  for (int64_t i = 0; OB_SUCC(ret) && is_const && i < order_by.count(); ++i) {
    if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_by.at(i).expr_, equal_sets,
                                               const_exprs, is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (!is_const) {
      // oracle的索引默认为NULLS_LAST_ASC，reverse之后为NULLS_FIRST_DESC
      // mysql只有nulls_first_asc和nulls_last_desc,都可以用到索引上的序
      if (is_ascending_direction(order_by.at(i).order_type_)) {
        direction = default_asc_direction();
      } else {
        direction = default_desc_direction();
      }
      order_offset = i;
    }
  }
  //计算index和order项的最大匹配，跳过const表达式
  ObRawExpr *index_expr = NULL;
  ObRawExpr *order_expr = NULL;
  int64_t order_start = order_offset;
  while (OB_SUCC(ret) && index_offset < index_keys.count() && order_offset < order_by.count()) {
    if (OB_ISNULL(index_expr = index_keys.at(index_offset)) ||
        OB_ISNULL(order_expr = order_by.at(order_offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs have null", K(ret), K(index_expr), K(order_expr));
    } else if (ObOptimizerUtil::is_expr_equivalent(index_expr, order_expr, equal_sets) &&
               (is_ascending_direction(order_by.at(order_offset).order_type_)
                == is_ascending_direction(direction))) {
      ++index_offset;
      ++order_offset;
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_expr, equal_sets,
                                                      const_exprs, is_const))) {
      LOG_WARN("failed to check order_expr is const expr", K(ret));
    } else if (is_const) {
      ++order_offset;
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(index_expr, equal_sets,
                                                      const_exprs, is_const))) {
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

int ObJoinOrder::add_access_filters(AccessPath *path,
                                    const ObIArray<ColumnItem> &range_cols,
                                    const ObIArray<ObRawExpr *> *range_exprs,
                                    const common::ObIArray<ObRawExpr*> *unprecise_range_exprs,
                                    PathHelper &helper)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const ObIArray<ObRawExpr *> &restrict_infos = helper.filters_;
  ObSEArray<uint64_t, 4> range_col_ids;
  ObSEArray<ObRawExpr *, 4> dummy;
  ObSEArray<ObRawExpr *, 4> remove_dup;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(path), K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < range_cols.count(); i++) {
      if (OB_FAIL(range_col_ids.push_back(range_cols.at(i).column_id_))) {
        LOG_WARN("failed to push back column_id", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < restrict_infos.count(); i++) {
      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found && j < deduced_exprs_info_.count(); j++) {
        const DeducedExprInfo &deduced_expr_info = deduced_exprs_info_.at(j);
        if (ObOptimizerUtil::is_expr_equivalent(deduced_expr_info.deduced_expr_,
                                                restrict_infos.at(i),
                                                get_plan()->get_equal_sets())) {
          found = true;
          bool extract_unprecise_range = false;
          // old query range unprecise check
          if (OB_FAIL(can_extract_unprecise_range(path->table_id_,
                                                  deduced_expr_info.deduced_expr_,
                                                  range_col_ids,
                                                  dummy,
                                                  extract_unprecise_range))) {
            LOG_WARN("failed to check can extract unprecise range", K(ret));
          } else if (extract_unprecise_range ||
                     (OB_NOT_NULL(range_exprs) &&
                     ObOptimizerUtil::find_equal_expr(*range_exprs, restrict_infos.at(i))) ||
                     (OB_NOT_NULL(unprecise_range_exprs) &&
                     ObOptimizerUtil::find_equal_expr(*unprecise_range_exprs, restrict_infos.at(i)))) {
            if (path->est_cost_info_.index_meta_info_.is_multivalue_index_ && !deduced_expr_info.is_precise_) {
              // deduced new pred for multivalue needn't add into new filter, just remain orginal filter is ok
            } else if (OB_FAIL(path->filter_.push_back(restrict_infos.at(i)))) {
              LOG_WARN("push back error", K(ret));
            } else if (OB_FAIL(append(helper.const_param_constraints_, deduced_expr_info.const_param_constraints_))) {
              LOG_WARN("append failed", K(ret));
            } else if (deduced_expr_info.is_precise_) {
              ObRawExpr *raw_expr = deduced_expr_info.deduced_from_expr_;
              if (OB_ISNULL(raw_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("raw expr is null", K(ret));
              } else if (OB_FAIL(remove_dup.push_back(raw_expr))) {
                LOG_WARN("push back error", K(ret));
              }
            } else {
              //do nothing
            }
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        if (OB_FAIL(path->filter_.push_back(restrict_infos.at(i)))) {
          LOG_WARN("push back error", K(ret));
        }
      }
    }
    if (FAILEDx(ObOptimizerUtil::remove_item(path->filter_, remove_dup))) {
      LOG_WARN("remove dup failed", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_and_extract_query_range(const uint64_t table_id,
                                               const uint64_t index_table_id,
                                               const IndexInfoEntry &index_info_entry,
                                               const ObIArray<ObRawExpr*> &index_keys,
                                               const ObIndexInfoCache &index_info_cache,
                                               bool &contain_always_false,
                                               ObIArray<uint64_t> &prefix_range_ids,
                                               ObIArray<ObRawExpr *> &restrict_infos)
{
  int ret = OB_SUCCESS;
  //do some quick check
  bool expr_match = false; //some condition on index
  contain_always_false = false;
  bool is_special_index = index_info_entry.is_index_geo() ||
                          index_info_entry.is_multivalue_index() ||
                          index_info_entry.is_fulltext_index();
  if (index_info_entry.is_multivalue_index() &&
      OB_FAIL(check_exprs_overlap_multivalue_index(table_id, index_table_id, restrict_infos, index_keys, expr_match))) {
    LOG_WARN("get_range_columns failed", K(ret));
  } else if (index_info_entry.is_index_geo() &&
             OB_FAIL(check_exprs_overlap_gis_index(restrict_infos, index_keys, expr_match))) {
    LOG_WARN("check quals match gis index error", K(restrict_infos), K(index_keys));
  } else if (index_info_entry.is_fulltext_index() &&
             OB_FALSE_IT(expr_match = index_info_entry.get_range_info().is_valid())) {
  } else if (!is_special_index && OB_FAIL(check_exprs_overlap_index(restrict_infos, index_keys, expr_match))) {
    LOG_WARN("check quals match index error", K(restrict_infos), K(index_keys));
  } else if (expr_match) {
    prefix_range_ids.reset();
    const QueryRangeInfo *query_range_info = NULL;
    if (OB_FAIL(index_info_cache.get_query_range(table_id, index_table_id,
                                                 query_range_info))) {
      LOG_WARN("get_range_columns failed", K(ret));
    } else if (OB_ISNULL(query_range_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query_range_info should not be null", K(ret));
    } else {
      contain_always_false = query_range_info->get_contain_always_false();
      uint64_t range_prefix_count = query_range_info->get_range_prefix_count();
      const ObIArray<ColumnItem> &range_columns = query_range_info->get_range_columns();
      if (index_info_entry.is_index_geo()) {
        range_prefix_count = 1;
      }
      ObSEArray<uint64_t, 4> range_col_ids;
      if (OB_UNLIKELY(range_prefix_count > range_columns.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("range prefix count is invalid", K(range_prefix_count), K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < range_prefix_count; ++i) {
          if (OB_FAIL(prefix_range_ids.push_back(range_columns.at(i).column_id_))) {
            LOG_WARN("failed to push back column_id", K(ret), K(i));
          }
        }
        // deal with unprecise dynamic range generated on execution stage
        for (int i = 0; OB_SUCC(ret) && i < range_columns.count(); i++) {
          if (OB_FAIL(range_col_ids.push_back(range_columns.at(i).column_id_))) {
            LOG_WARN("failed to push back column_id", K(ret), K(i));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < restrict_infos.count(); i++) {
          bool can_extract = false;
          ObSEArray<ObRawExpr *, 4> unprecise_exprs;
          ObSEArray<uint64_t, 4> expr_col_ids;
          if (OB_ISNULL(restrict_infos.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("restrict_infos.at(i) is null", K(ret), K(i));
          } else if (OB_NOT_NULL(query_range_info->get_pre_range_graph()) &&
                     OB_FAIL(unprecise_exprs.assign(query_range_info->get_pre_range_graph()->get_unprecise_range_exprs()))) {
            LOG_WARN("failed to assign unprecise range exprs", K(ret));
          } else if (OB_FAIL(can_extract_unprecise_range(table_id,
                                                        restrict_infos.at(i),
                                                        range_col_ids,
                                                        unprecise_exprs,
                                                        can_extract))) {
            LOG_WARN("failed to check can extract unprecise range", K(ret));
          } else if (!can_extract) {
            // do nothing
          } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(restrict_infos.at(i), table_id, expr_col_ids))) {
            LOG_WARN("failed to extract column ids", K(ret));
          } else if (!ObOptimizerUtil::is_subset(expr_col_ids, range_col_ids)) {
            // do nothing
          } else if (OB_FAIL(append_array_no_dup(prefix_range_ids, expr_col_ids))) {
            LOG_WARN("failed to append array no dup", K(ret));
          }
        }
      }
    }
  } //not match, do nothing
  LOG_TRACE("extract prefix range ids", K(ret),
            K(contain_always_false), K(expr_match),
            K(prefix_range_ids));
  return ret;
}

/*
 * 计算维度信息
 * @table_id 从table_item获取的table_id
 * @data_table_id 真实table_id
 * @index_table_id 索引的table_id
 * @stmt
 * @index_dim 记录了三种维度的信息
 * */
int ObJoinOrder::cal_dimension_info(const uint64_t table_id, //alias table id
                                    const uint64_t data_table_id, //real table id
                                    const uint64_t index_table_id,
                                    const ObDMLStmt *stmt,
                                    ObIndexSkylineDim &index_dim,
                                    const ObIndexInfoCache &index_info_cache,
                                    ObIArray<ObRawExpr *> &restrict_infos,
                                    bool ignore_index_back_dim)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *guard = NULL;
  IndexInfoEntry *index_info_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(guard = OPT_CTX.get_sql_schema_guard()) || OB_ISNULL(stmt) || OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(guard), K(stmt));
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id,
                                                           index_table_id,
                                                           index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(ret), K(data_table_id), K(index_table_id));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info entry should not be null", K(ret));
  } else {
    ObSEArray<uint64_t, 8> filter_column_ids;
    bool is_index_back = ignore_index_back_dim ? false : index_info_entry->is_index_back();
    const OrderingInfo *ordering_info = &index_info_entry->get_ordering_info();
    ObSEArray<uint64_t, 8> interest_column_ids;
    ObSEArray<bool, 8> const_column_info;
    ObSEArray<uint64_t, 8> prefix_range_ids;  //for query range compare
    bool contain_always_false = false;
    if (OB_FAIL(extract_interesting_column_ids(ordering_info->get_index_keys(),
                                               index_info_entry->get_interesting_order_prefix_count(),
                                               interest_column_ids,
                                               const_column_info))) {
      LOG_WARN("failed to extract interest column ids", K(ret));
    } else if (OB_FAIL(check_and_extract_query_range(table_id,
                                                     index_table_id,
                                                     *index_info_entry,
                                                     ordering_info->get_index_keys(),
                                                     index_info_cache,
                                                     contain_always_false,
                                                     prefix_range_ids,
                                                     restrict_infos))) {
      LOG_WARN("check_and_extract query range failed", K(ret));
    } else {
      const ObTableSchema *index_schema = NULL;
      if (OB_FAIL(guard->get_table_schema(index_table_id, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret));
      } else if (OB_FAIL(extract_filter_column_ids(restrict_infos,
                                                   data_table_id == index_table_id,
                                                   *index_schema,
                                                   filter_column_ids))) {
        LOG_WARN("extract filter column ids failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        /*
         * 添加三种维度的信息
         * (1) 是否回表
         * (2) interesting_order
         * (3) query range
         * */
        bool can_extract_range = prefix_range_ids.count() > 0 || contain_always_false;
        if (OB_FAIL(index_dim.add_index_back_dim(is_index_back,
                                                 *allocator_))) {
          LOG_WARN("add index back dim failed", K(is_index_back), K(ret));
        } else if (OB_FAIL(index_dim.add_interesting_order_dim(is_index_back,
                                                               can_extract_range,
                                                               filter_column_ids,
                                                               interest_column_ids,
                                                               const_column_info,
                                                               *allocator_))) {
          LOG_WARN("add interesting order dim failed", K(interest_column_ids), K(ret));
        } else if (OB_FAIL(index_dim.add_query_range_dim(prefix_range_ids,
                                                         *allocator_,
                                                         contain_always_false))) {
          LOG_WARN("add query range dimension failed", K(ret));
        } else if (OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3)
                  && OB_FAIL(index_dim.add_sharding_info_dim(index_info_entry->get_sharding_info(), *allocator_))) {
          LOG_WARN("add partition num dimension failed");
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::is_vector_inv_index_tid(const uint64_t index_table_id, bool& is_vec_tid)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *index_schema = NULL;
  if (OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_table_id, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema should not be null", K(ret));
  } else {
    is_vec_tid = index_schema->is_vec_index();
  }
  return ret;
}


/*
 * 裁剪索引
 * @table_id 从table_item里面获取的table_id
 * @base_table_id 真实的table_id
 * @stmt
 * @do_prunning 标记是否做skyline的分支裁剪
 * @index_info_cache 索引的query range缓存
 * @valid_index_ids 可用的索引id
 * @skyline_index_ids 裁剪完的索引id
 * */
int ObJoinOrder::skyline_prunning_index(const uint64_t table_id,
                                        const uint64_t base_table_id,
                                        const ObDMLStmt *stmt,
                                        const bool do_prunning,
                                        const ObIndexInfoCache &index_info_cache,
                                        const ObIArray<uint64_t> &valid_index_ids,
                                        ObIArray<uint64_t> &skyline_index_ids,
                                        ObIArray<ObRawExpr *> &restrict_infos,
                                        bool ignore_index_back_dim)
{
  int ret = OB_SUCCESS;
  if (!do_prunning) {
    skyline_index_ids.reset();
    LOG_TRACE("do not do index prunning", K(table_id), K(base_table_id));
    OPT_TRACE("do not do index prunning");
    if (OB_FAIL(append(skyline_index_ids, valid_index_ids))) {
      LOG_WARN("failed to append id", K(ret));
    } else { /*do nothing*/ }
  } else {
    //维度统计信息
    OPT_TRACE_TITLE("BEGIN SKYLINE INDEX PRUNNING");
    ObSkylineDimRecorder recorder;
    bool has_add = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
      const uint64_t tid = valid_index_ids.at(i);
      LOG_TRACE("cal dimension info of index", K(tid));
      ObIndexSkylineDim *index_dim = NULL;
      if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(*allocator_, index_dim))) {
        LOG_WARN("failed to create index skylined dimension", K(ret));
      } else if (OB_ISNULL(index_dim)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dimension should not be null", K(ret));
      } else if (FALSE_IT(index_dim->set_index_id(tid))) {
      //计算维度信息, 封装在 ObSkylineIndexDim 里面
      } else if (OB_FAIL(cal_dimension_info(table_id,
                                            base_table_id,
                                            tid, stmt,
                                            *index_dim, index_info_cache,
                                            restrict_infos,
                                            ignore_index_back_dim))) {
        LOG_WARN("Failed to cal dimension info", K(ret), "index_id", valid_index_ids, K(i));
      } else if (stmt->has_vec_approx()) {
        // check tid is vec_index
        bool is_vec_tid = false;
        if (OB_FAIL(is_vector_inv_index_tid(tid, is_vec_tid))) {
          LOG_WARN("Failed to check is vec index tid", K(ret), K(tid));
        } else {
          index_dim->set_can_prunning(false);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(recorder.add_index_dim(*index_dim, has_add))) {
        LOG_WARN("failed to add index dimension", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      skyline_index_ids.reset();
      //获取裁剪完的索引id
      if (OB_FAIL(recorder.get_dominated_idx_ids(skyline_index_ids))) {
        LOG_WARN("get dominated idx ids failed", K(ret));
      } else {
        LOG_TRACE("after prunning remain index ids", K(skyline_index_ids));
        OPT_TRACE("after prunning remain index ids", skyline_index_ids);
      }
    }
  }
  return ret;
}

int ObJoinOrder::fill_index_info_entry(const uint64_t table_id,
                                       const uint64_t base_table_id,
                                       const uint64_t index_id,
                                       IndexInfoEntry *&index_entry,
                                       PathHelper &helper)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObDMLStmt *stmt = NULL;
  index_entry = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(schema_guard), K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema, ObSqlSchemaGuard::is_link_table(stmt, table_id)))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_id), K(base_table_id), K(index_id));
  } else {
    void *ptr = allocator_->alloc(sizeof(IndexInfoEntry));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("entry should not be null", K(ret));
    } else {
      IndexInfoEntry *entry = new (ptr) IndexInfoEntry();
      ObOrderDirection direction = default_asc_direction();
      bool is_index_back = false;
      bool is_unique_index = false;
      bool is_index_global = false;
      bool is_index_geo = index_schema->is_spatial_index();
      entry->set_index_id(index_id);
      int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
      int64_t max_prefix_count = 0;
      bool force_direction = false;
      if (OB_FAIL(get_simple_index_info(table_id, base_table_id, index_id,
                                               is_unique_index, is_index_back, is_index_global))) {
        LOG_WARN("failed to get simple index info", K(ret));
      } else if (OB_FAIL(get_access_path_ordering(table_id, base_table_id, index_id,
                                           entry->get_ordering_info().get_index_keys(),
                                           entry->get_ordering_info().get_ordering(),
                                           direction,
                                           force_direction,
                                           is_index_back))) {
        LOG_WARN("get access path ordering ", K(ret));
      } else {
        entry->set_is_index_global(is_index_global);
        entry->set_is_index_geo(is_index_geo);
        entry->set_is_index_back(is_index_back);
        entry->set_is_unique_index(is_unique_index);
        entry->set_is_fulltext_index(index_schema->is_fts_index_aux());
        entry->set_is_multivalue_index(index_schema->is_multivalue_index_aux());
        entry->set_is_vector_index(index_schema->is_vec_index());
        entry->get_ordering_info().set_scan_direction(direction);
        entry->set_force_direction(force_direction);
      }
      if (OB_SUCC(ret)) {
        ObSEArray<OrderItem, 4> index_ordering;
        ObIArray<ObRawExpr*> &ordering_expr = entry->get_ordering_info().get_ordering();
        for (int64_t i = 0; OB_SUCC(ret) && i < ordering_expr.count(); ++i) {
          OrderItem order_item(ordering_expr.at(i), direction);
          if (OB_FAIL(index_ordering.push_back(order_item))) {
            LOG_WARN("failed to push back order item", K(ret));
          }
        }
        if (OB_FAIL(ret) || helper.is_inner_path_) {
          // The ordering of inner path can not be preserved
        } else if (OB_FAIL(check_all_interesting_order(index_ordering,
                                                       stmt,
                                                       max_prefix_count,
                                                       interesting_order_info))) {
          LOG_WARN("failed to check all interesting order", K(ret));
        } else {
          entry->set_interesting_order_prefix_count(max_prefix_count);
          entry->set_interesting_order_info(interesting_order_info);
        }
      }
      if (OB_SUCC(ret)) {
        if ((index_id == base_table_id && is_virtual_table(index_id)
             && entry->get_ordering_info().get_index_keys().count() <= 0)) {
           //ignore extract query range
          LOG_TRACE("ignore virtual table", K(base_table_id), K(index_id));
        } else if (OB_FAIL(get_query_range_info(table_id,
                                                base_table_id,
                                                index_id,
                                                entry->get_range_info(),
                                                helper))) {
          LOG_WARN("failed to get query range", K(ret), K(table_id), K(base_table_id), K(index_id));
        } else {
          LOG_TRACE("finish extract query range", K(table_id), K(index_id));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(compute_table_location_for_index_info_entry(table_id, base_table_id, entry))) {
          LOG_WARN("compte table location for index info entry failed", K(ret));
        } else if (OB_FAIL(compute_sharding_info_for_index_info_entry(table_id, base_table_id, entry))) {
          LOG_WARN("compte sharding info for index info entry failed", K(ret));
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

//fill all index info entry
int ObJoinOrder::fill_index_info_cache(const uint64_t table_id,
                                       const uint64_t base_table_id,
                                       const ObIArray<uint64_t> &valid_index_ids,
                                       ObIndexInfoCache &index_info_cache,
                                       PathHelper &helper)
{
  int ret = OB_SUCCESS;
  index_info_cache.set_table_id(table_id);
  index_info_cache.set_base_table_id(base_table_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); i++) {
    IndexInfoEntry *index_info_entry = NULL;
    if (OB_FAIL(fill_index_info_entry(table_id, base_table_id,
                                      valid_index_ids.at(i), index_info_entry, helper))) {
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

int ObJoinOrder::create_access_paths(const uint64_t table_id,
                                     const uint64_t ref_table_id,
                                     PathHelper &helper,
                                     ObIArray<AccessPath *> &access_paths,
                                     ObIndexInfoCache &index_info_cache)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> candi_index_ids;
  ObSEArray<uint64_t, 4> valid_index_ids;
  const ObDMLStmt *stmt = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  const ParamStore *params = NULL;
  bool is_valid = true;
  ObSQLSessionInfo *session_info = NULL;
  bool use_index_merge = false;
  bool use_index_merge_by_hint = false;
  ObArray<ObIndexMergeNode*> union_merge_nodes;
  bool ignore_normal_access_path = false;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(params = opt_ctx->get_params()) ||
      OB_ISNULL(opt_ctx->get_exec_ctx()) ||
      OB_ISNULL(opt_ctx->get_exec_ctx()->get_sql_ctx()) ||
      OB_ISNULL(helper.table_opt_info_) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(params),
        K(stmt), K(ret));
  } else if (OB_FAIL(get_generated_col_index_qual(table_id,
                                                  helper.filters_, helper))) {
    LOG_WARN("get prefix index qual failed");
  } else if (OB_FAIL(init_basic_text_retrieval_info(table_id,
                                                    ref_table_id,
                                                    helper))) {
    LOG_WARN("failed to init basic text retrieval info", K(ret));
  } else if (OB_FAIL(create_index_merge_access_paths(table_id,
                                                     ref_table_id,
                                                     helper,
                                                     index_info_cache,
                                                     access_paths,
                                                     ignore_normal_access_path))) {
    LOG_WARN("failed to create index merge access paths", K(ret));
  } else if (ignore_normal_access_path) {
    // do nothing
  } else if (OB_FAIL(get_valid_index_ids(table_id,
                                         ref_table_id,
                                         helper,
                                         candi_index_ids))) {
    LOG_WARN("failed to get valid index ids", K(ret));
  } else if (OB_FAIL(fill_index_info_cache(table_id, ref_table_id,
                                           candi_index_ids,
                                           index_info_cache,
                                           helper))) {
    LOG_WARN("failed to fill index info cache", K(ret));
  } else if (OB_FAIL(add_table_by_heuristics(table_id, ref_table_id,
                                             index_info_cache,
                                             candi_index_ids,
                                             valid_index_ids,
                                             helper))) {
    LOG_WARN("failed to add table by heuristics", K(ret));
  } else if (valid_index_ids.empty() && OB_FAIL(skyline_prunning_index(table_id,
                                                                       ref_table_id,
                                                                       stmt,
                                                                       true,
                                                                       index_info_cache,
                                                                       candi_index_ids,
                                                                       valid_index_ids,
                                                                       helper.filters_))) {
    LOG_WARN("failed to pruning_index", K(table_id), K(ref_table_id), K(ret));
  } else {
    helper.table_opt_info_->optimization_method_ = OptimizationMethod::COST_BASED;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
      bool is_create_basic_path = false;
      bool is_create_das_path = false;
      bool use_column_store = false;
      bool use_row_store = false;
      AccessPath *das_row_store_access_path = NULL;
      AccessPath *basic_row_store_access_path = NULL; // the path does not use DAS, maybe optimal sometime.
      AccessPath *das_column_store_access_path = NULL;
      AccessPath *basic_column_store_access_path = NULL;
      OptSkipScanState use_skip_scan = OptSkipScanState::SS_UNSET;
      if (OB_FAIL(will_use_das(table_id,
                               valid_index_ids.at(i),
                               index_info_cache,
                               helper,
                               is_create_das_path,
                               is_create_basic_path))) {
        LOG_WARN("failed to check will use das", K(ret));
      } else if (OB_FAIL(will_use_skip_scan(table_id,
                                            ref_table_id,
                                            valid_index_ids.at(i),
                                            index_info_cache,
                                            helper,
                                            session_info,
                                            use_skip_scan))) {
        LOG_WARN("failed to check will use skip scan", K(ret));
      } else if (OB_FAIL(get_plan()->will_use_column_store(table_id,
                                                          valid_index_ids.at(i),
                                                          ref_table_id,
                                                          use_column_store,
                                                          use_row_store))) {
        LOG_WARN("failed to check will use column store", K(ret));
      } else if (is_create_das_path &&
                 use_row_store &&
                 OB_FAIL(create_one_access_path(table_id,
                                                ref_table_id,
                                                valid_index_ids.at(i),
                                                index_info_cache,
                                                helper,
                                                das_row_store_access_path,
                                                true,
                                                false,
                                                use_skip_scan))) {
        LOG_WARN("failed to make index path", "index_table_id", valid_index_ids.at(i), K(ret));
      } else if (OB_NOT_NULL(das_row_store_access_path) &&
                 OB_FAIL(access_paths.push_back(das_row_store_access_path))) {
        LOG_WARN("failed to push back access path", K(ret));
      } else if (is_create_das_path &&
                 use_column_store &&
                 OB_FAIL(create_one_access_path(table_id,
                                                ref_table_id,
                                                valid_index_ids.at(i),
                                                index_info_cache,
                                                helper,
                                                das_column_store_access_path,
                                                true,
                                                true,
                                                use_skip_scan))) {
        LOG_WARN("failed to make index path", "index_table_id", valid_index_ids.at(i), K(ret));
      } else if (OB_NOT_NULL(das_column_store_access_path) &&
                 OB_FAIL(access_paths.push_back(das_column_store_access_path))) {
        LOG_WARN("failed to push back access path", K(ret));
      } else if (is_create_basic_path &&
                 use_row_store &&
                 OB_FAIL(create_one_access_path(table_id,
                                                ref_table_id,
                                                valid_index_ids.at(i),
                                                index_info_cache,
                                                helper,
                                                basic_row_store_access_path,
                                                false,
                                                false,
                                                use_skip_scan))) {
        LOG_WARN("failed to make index path", "index_table_id", valid_index_ids.at(i), K(ret));
      } else if(OB_NOT_NULL(basic_row_store_access_path) &&
                OB_FAIL(access_paths.push_back(basic_row_store_access_path))) {
        LOG_WARN("failed to push back access path", K(ret));
      } else if (is_create_basic_path &&
                 use_column_store &&
                 OB_FAIL(create_one_access_path(table_id,
                                                ref_table_id,
                                                valid_index_ids.at(i),
                                                index_info_cache,
                                                helper,
                                                basic_column_store_access_path,
                                                false,
                                                true,
                                                use_skip_scan))) {
        LOG_WARN("failed to make index path", "index_table_id", valid_index_ids.at(i), K(ret));
      } else if(OB_NOT_NULL(basic_column_store_access_path) &&
                OB_FAIL(access_paths.push_back(basic_column_store_access_path))) {
        LOG_WARN("failed to push back access path", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::create_index_merge_access_paths(const uint64_t table_id,
                                                 const uint64_t ref_table_id,
                                                 PathHelper &helper,
                                                 ObIndexInfoCache &index_info_cache,
                                                 ObIArray<AccessPath *> &access_paths,
                                                 bool &ignore_normal_access_path)
 {
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObQueryCtx *query_ctx = NULL;
  ObSEArray<ObIndexMergeNode*, 4> candi_index_trees;
  bool is_match_hint = false;
  bool contain_fts = false;
  ignore_normal_access_path = false;
  LOG_TRACE("check can use index merge begin", K(ref_table_id), K(helper.filters_));
  OPT_TRACE_TITLE("BEGIN CREATE INDEX MERGE PATHS");
  OPT_TRACE("table_id: ", table_id, "ref_table_id: ", ref_table_id);
  OPT_TRACE("full query filters: ", helper.filters_);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(stmt), K(query_ctx));
  } else if (query_ctx->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_5) {
    OPT_TRACE("can not create index merge paths due to optimizer feature control");
  } else if (is_virtual_table(ref_table_id)) {
    OPT_TRACE("can not create index merge paths for virtual table");
  } else if (OB_FAIL(get_candi_index_merge_trees(table_id,
                                                 ref_table_id,
                                                 helper,
                                                 candi_index_trees,
                                                 is_match_hint))) {
    LOG_WARN("failed to get valid index ids", K(ret));
  } else if (candi_index_trees.empty()) {
    // do nothing
  } else if (OB_FAIL(do_create_index_merge_paths(table_id,
                                                 ref_table_id,
                                                 helper,
                                                 index_info_cache,
                                                 candi_index_trees,
                                                 access_paths))) {
    LOG_WARN("failed to create index merge paths", K(ret));
  } else if (OB_UNLIKELY(EN_FORCE_INDEX_MERGE_PLAN)) {
    ignore_normal_access_path = true;
    LOG_TRACE("[EN_FORCE_INDEX_MERGE_PLAN] finish create index merge path ", K(ref_table_id), K(is_match_hint), K(contain_fts), K(access_paths));
  } else if (OB_FAIL(check_index_merge_paths_contain_fts(access_paths, contain_fts))) {
    LOG_WARN("failed to check index merge paths contain FTS", K(ret));
  } else if (!is_match_hint && !contain_fts
             && OB_FAIL(prune_index_merge_path(access_paths))) {
    LOG_WARN("failed to prune index merge path", K(ret));
  } else {
    ignore_normal_access_path = (is_match_hint || contain_fts) && !access_paths.empty();
    LOG_TRACE("finish create index merge path", K(ref_table_id), K(is_match_hint), K(contain_fts), K(access_paths));
  }
  return ret;
}

int ObJoinOrder::get_candi_index_merge_trees(const uint64_t table_id,
                                             const uint64_t ref_table_id,
                                             PathHelper &helper,
                                             ObIArray<ObIndexMergeNode*> &candi_index_trees,
                                             bool &is_match_hint)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<uint64_t, common::ObArray<uint64_t>>, 2> valid_indexes;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  is_match_hint = false;
  if (OB_ISNULL(get_plan()) || OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid plan or tenant config", K(ret), K(get_plan()), K(tenant_config.is_valid()));
  } else if (OB_FAIL(get_valid_index_merge_indexes(table_id,
                                                   ref_table_id,
                                                   valid_indexes,
                                                   false /* ignore_hint */ ))) {
    LOG_WARN("failed to get valid index merge ids with hint", K(ret));
  } else if (OB_FAIL(generate_candi_index_merge_trees(ref_table_id,
                                                      helper.filters_,
                                                      valid_indexes,
                                                      candi_index_trees))) {
    LOG_WARN("failed to generate candi index merge trees", K(ret));
  } else if (!candi_index_trees.empty()
             && OB_FAIL(check_candi_index_trees_match_hint(table_id, candi_index_trees))) {
    LOG_WARN("failed to check candi index trees match hint", K(ret));
  } else if (!candi_index_trees.empty()) {
    is_match_hint = true;
    OPT_TRACE("generate", candi_index_trees.count(), "candi index merge trees using hint");
    LOG_TRACE("generate candi index merge trees using hint", K(table_id), K(candi_index_trees));
  } else if (get_plan()->get_log_plan_hint().is_outline_data_
             || (!tenant_config->_enable_index_merge && OB_LIKELY(!EN_FORCE_INDEX_MERGE_PLAN))) {
    OPT_TRACE("can not create index merge paths due to outline or tenant config");
  } else if (OB_FAIL(get_valid_index_merge_indexes(table_id,
                                                   ref_table_id,
                                                   valid_indexes,
                                                   true /* ignore_hint */ ))) {
    LOG_WARN("failed to get valid index merge ids", K(ret));
  } else if (OB_FAIL(generate_candi_index_merge_trees(ref_table_id,
                                                      helper.filters_,
                                                      valid_indexes,
                                                      candi_index_trees))) {
    LOG_WARN("failed to generate candi index merge trees", K(ret));
  } else if (!candi_index_trees.empty()
             && OB_FAIL(prune_candi_index_merge_trees(candi_index_trees))) {
    LOG_WARN("failed to prune candi index trees", K(ret));
  } else {
    OPT_TRACE("generate", candi_index_trees.count(), "candi index merge trees");
    LOG_TRACE("generate candi index merge trees", K(table_id), K(candi_index_trees));
  }
  return ret;
}

int ObJoinOrder::get_valid_index_merge_indexes(const uint64_t table_id,
                                               const uint64_t ref_table_id,
                                               ObIArray<std::pair<uint64_t, common::ObArray<uint64_t>>> &valid_indexes,
                                               bool ignore_hint)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> index_ids;
  ObArray<uint64_t> index_column_ids;
  valid_indexes.reuse();
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(get_plan()), K(schema_guard), K(ret));
  } else if (ignore_hint) {
    ObSEArray<ObAuxTableMetaInfo, 4> simple_index_infos;
    if (OB_FAIL(index_ids.push_back(ref_table_id))) {
      LOG_WARN("failed to push back ref table id", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ref_table_id), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table schema", K(ret), K(ref_table_id));
    } else if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("failed to get simple index infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(index_ids.push_back(simple_index_infos.at(i).table_id_))) {
        LOG_WARN("failed to push back table id", K(ret), K(i));
      }
    }
  } else {
    const LogTableHint *log_table_hint = get_plan()->get_log_plan_hint().get_log_table_hint(table_id);
    if (NULL == log_table_hint || 2 > log_table_hint->union_merge_list_.count()) {
      // do nothing
    } else if (OB_FAIL(index_ids.assign(log_table_hint->union_merge_list_))) {
      LOG_WARN("failed to assign union merge list", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
    index_column_ids.reuse();
    const ObTableSchema *index_schema = NULL;
    uint64_t index_id = index_ids.at(i);
    if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
      LOG_WARN("failed to get index table schema", K(ret), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null index schema", K(ret));
    } else if (ref_table_id == index_id ||
               (index_schema->is_index_local_storage() &&
                (index_schema->is_normal_index() || index_schema->is_fts_index()))) {
      /* primary table, local normal index and local fts index are available for index merge now */
      if (OB_FAIL(index_schema->get_rowkey_column_ids(index_column_ids))) {
        LOG_WARN("failed to get all column ids", K(ret));
      } else if (OB_FAIL(valid_indexes.push_back(std::pair<uint64_t, common::ObArray<uint64_t>>(index_id, index_column_ids)))) {
        LOG_WARN("failed to push back index column ids", K(index_id));
      }
    } else if (!ignore_hint) {
      // hint is invalid
      valid_indexes.reuse();
      break;
    }
  }
  LOG_TRACE("get all valid indexes for index merge", K(table_id), K(ref_table_id), K(ignore_hint), K(valid_indexes));
  return ret;
}

int ObJoinOrder::generate_candi_index_merge_trees(const uint64_t ref_table_id,
                                                  ObIArray<ObRawExpr*> &filters,
                                                  ObIArray<std::pair<uint64_t, common::ObArray<uint64_t>>> &valid_indexes,
                                                  ObIArray<ObIndexMergeNode *> &candi_index_trees)
{
  int ret = OB_SUCCESS;
  if (valid_indexes.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      ObRawExpr *filter = filters.at(i);
      ObIndexMergeNode *candi_node = NULL;
      bool is_valid = false;
      if (OB_ISNULL(filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null filter", K(ret), K(i));
      } else if (filter->get_expr_type() != T_OP_OR) {
        // do nothing, only support UNION MERGE now
      } else if (OB_FAIL(generate_candi_index_merge_node(ref_table_id,
                                                         filter,
                                                         valid_indexes,
                                                         candi_node,
                                                         is_valid))) {
        LOG_WARN("failed to generate one index merge tree for filter", K(ret), KPC(filter));
      } else if (!is_valid || NULL == candi_node) {
        // do nothing
      } else if (OB_FAIL(candi_node->formalize_index_merge_tree())) {
        LOG_WARN("failed to formalize candi index merge tree", K(ret), KPC(candi_node));
      } else if (OB_FAIL(candi_index_trees.push_back(candi_node))) {
        LOG_WARN("failed to push back candi index tree", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_candi_index_merge_node(const uint64_t ref_table_id,
                                                 ObRawExpr *filter,
                                                 ObIArray<std::pair<uint64_t, common::ObArray<uint64_t>>> &valid_indexes,
                                                 ObIndexMergeNode *&candi_node,
                                                 bool &is_valid_node)
{
  int ret = OB_SUCCESS;
  candi_node = NULL;
  is_valid_node = false;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null filter", K(ret));
  } else if (T_OP_OR == filter->get_expr_type()
             || T_OP_AND == filter->get_expr_type()) {
    if (OB_ISNULL(candi_node = OB_NEWx(ObIndexMergeNode, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate index merge node", K(ret));
    } else {
      candi_node->node_type_ = (T_OP_OR == filter->get_expr_type()) ? INDEX_MERGE_UNION : INDEX_MERGE_INTERSECT;
      is_valid_node = (T_OP_OR == filter->get_expr_type()) ? true : false;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < filter->get_param_count(); ++i) {
      ObIndexMergeNode *child = NULL;
      bool is_child_valid = false;
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(generate_candi_index_merge_node(ref_table_id,
                                                                    filter->get_param_expr(i),
                                                                    valid_indexes,
                                                                    child,
                                                                    is_child_valid)))) {
        LOG_WARN("failed to construct index merge node", K(ret));
      } else if (!is_child_valid) {
        if (T_OP_OR == filter->get_expr_type()) {
          is_valid_node = false;
          break;
        }
      } else if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child node is null", K(ret), KPC(filter));
      } else if (OB_FAIL(candi_node->children_.push_back(child))) {
        LOG_WARN("failed to push back child node", K(ret));
      } else if (T_OP_AND == filter->get_expr_type()) {
        is_valid_node = true;
      }
    }
  } else {
    ObSEArray<uint64_t, 1> candicate_index_tids;
    if (!get_tables().equal(filter->get_relation_ids())) {
      is_valid_node = false;
    } else if (!(filter->has_flag(IS_SIMPLE_COND)
                 || filter->has_flag(IS_RANGE_COND)
                 || filter->has_flag(CNT_MATCH_EXPR))) {
      is_valid_node = false;
    } else if (OB_FAIL(collect_candicate_indexes(ref_table_id, filter, valid_indexes, candicate_index_tids))) {
      LOG_WARN("failed to choose candi index for filter", K(ret), K(filter));
    } else if (candicate_index_tids.empty()) {
      is_valid_node = false;
    } else if (OB_ISNULL(candi_node = OB_NEWx(ObIndexMergeNode, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate index merge node", K(ret));
    } else if (OB_FAIL(candi_node->filter_.push_back(filter))) {
      LOG_WARN("failed to push back filter", K(ret));
    } else if (OB_FAIL(candi_node->candicate_index_tids_.assign(candicate_index_tids))) {
      LOG_WARN("failed to assign candicate index tids", K(ret));
    } else {
      candi_node->node_type_ = filter->has_flag(CNT_MATCH_EXPR) ? INDEX_MERGE_FTS_INDEX : INDEX_MERGE_SCAN;
      candi_node->index_tid_ = candicate_index_tids.at(0);
      is_valid_node = true;
    }
  }
  return ret;
}

int ObJoinOrder::collect_candicate_indexes(const uint64_t ref_table_id,
                                           ObRawExpr *filter,
                                           ObIArray<std::pair<uint64_t, common::ObArray<uint64_t>>> &valid_indexes,
                                           ObIArray<uint64_t> &candicate_index_tids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(filter));
  } else if (filter->has_flag(CNT_MATCH_EXPR)) {
    /* direct choose corresponding fts index */
    ObMatchFunRawExpr *match_expr = NULL;
    if (filter->get_expr_type() == T_OP_BOOL && filter->has_flag(CNT_MATCH_EXPR)) {
      ObRawExpr *param_expr = filter->get_param_expr(0);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param expr for bool op", K(ret));
      } else if (param_expr->has_flag(IS_MATCH_EXPR)) {
        match_expr = static_cast<ObMatchFunRawExpr*>(param_expr);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(match_expr)) {
      uint64_t inv_index_tid = OB_INVALID_ID;
      bool is_valid = false;
      if (OB_FAIL(get_matched_inv_index_tid(match_expr, ref_table_id, inv_index_tid))) {
        LOG_WARN("failed to get matched inv index tid", K(match_expr), K(ref_table_id), K(ret));
      } else {
        for (int64_t i = 0; i < valid_indexes.count(); ++i) {
          if (inv_index_tid == valid_indexes.at(i).first) {
            is_valid = true;
            break;
          }
        }
        if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(candicate_index_tids.push_back(inv_index_tid))) {
          LOG_WARN("failed to push back index id", K(inv_index_tid), K(ret));
        }
      }
    }
  } else {
    /* record all indexes which satisfy the index prefix condition */
    ObSEArray<uint64_t, 4> column_ids;
    if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, column_ids))) {
      LOG_WARN("failed to extract column ids", K(filter), K(ret));
    } else if (OB_UNLIKELY(column_ids.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid filter node for index merge", K(filter), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < valid_indexes.count(); ++i) {
        const uint64_t index_id = valid_indexes.at(i).first;
        const ObIArray<uint64_t> &index_column_ids = valid_indexes.at(i).second;
        if (OB_UNLIKELY(index_column_ids.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index column ids", K(ret), K(index_id), K(index_column_ids));
        } else if (index_column_ids.at(0) == column_ids.at(0)) {
          if (OB_FAIL(candicate_index_tids.push_back(index_id))) {
            LOG_WARN("failed to push back index id", K(index_id), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexMergeNode::formalize_index_merge_tree()
{
  int ret = OB_SUCCESS;
  if (is_merge_node()) {
    ObSEArray<ObIndexMergeNode*, 2> new_children;
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      if (OB_ISNULL(children_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null child", K(ret), K(i), K(children_));
      } else if (OB_FAIL(SMART_CALL(children_.at(i)->formalize_index_merge_tree()))) {
        LOG_WARN("failed to formalize child node", K(ret), K(i), KPC(children_.at(i)));
      } else if (children_.at(i)->node_type_ == node_type_) {
        for (int64_t j = 0; OB_SUCC(ret) && j < children_.at(i)->children_.count(); ++j) {
          if (OB_FAIL(new_children.push_back(children_.at(i)->children_.at(j)))) {
            LOG_WARN("failed to push back new child", K(ret));
          }
        }
      } else if (OB_FAIL(new_children.push_back(children_.at(i)))) {
        LOG_WARN("failed to push back new child", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(children_.assign(new_children))) {
      LOG_WARN("failed to assign new children", K(ret), K(new_children));
    }
  }
  return ret;
}

/**
 * @brief ObJoinOrder::check_candi_index_trees_match_hint
 *
 * 1. Prune candi index merge trees which can not match the union_merge hint.
 * 2. Choose the index id for each candi index merge node based on union_merge hint.
 */
int ObJoinOrder::check_candi_index_trees_match_hint(const uint64_t table_id,
                                                    ObIArray<ObIndexMergeNode*> &candi_index_trees)
{
  int ret = OB_SUCCESS;
  const LogTableHint *log_table_hint = NULL;
  ObSEArray<uint64_t, 4> index_ids;
  ObSEArray<ObIndexMergeNode*, 4> new_candi_trees;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(log_table_hint = get_plan()->get_log_plan_hint().get_log_table_hint(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(log_table_hint));
  } else if (OB_FAIL(index_ids.assign(log_table_hint->union_merge_list_))) {
    LOG_WARN("failed to assign union merge list", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_index_trees.count(); ++i) {
    bool is_match_hint = false;
    int64_t idx = 0;
    if (OB_FAIL(check_one_candi_node_match_hint(candi_index_trees.at(i), index_ids, idx, is_match_hint))) {
      LOG_WARN("failed to check candi tree match hint", K(ret), K(i), KPC(candi_index_trees.at(i)));
    } else if (!is_match_hint) {
      // do nothing
    } else if (OB_FAIL(new_candi_trees.push_back(candi_index_trees.at(i)))) {
      LOG_WARN("failed to push back new candi tree", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(candi_index_trees.assign(new_candi_trees))) {
    LOG_WARN("failed to assign new candi trees", K(ret), K(new_candi_trees));
  }
  return ret;
}

int ObJoinOrder::check_one_candi_node_match_hint(ObIndexMergeNode *candi_node,
                                                 ObIArray<uint64_t> &index_ids,
                                                 int64_t &idx,
                                                 bool &is_match_hint)
{
  int ret = OB_SUCCESS;
  is_match_hint = false;
  if (OB_ISNULL(candi_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("candi node is null", K(ret), K(candi_node));
  } else if (idx >= index_ids.count()) {
    // do nothing
  } else {
    switch (candi_node->node_type_) {
      case INDEX_MERGE_UNION: {
        bool is_child_match_hint = true;
        for (int64_t i = 0; OB_SUCC(ret) && is_child_match_hint && i < candi_node->children_.count(); ++i) {
          if (OB_FAIL(SMART_CALL(check_one_candi_node_match_hint(candi_node->children_.at(i), index_ids, idx, is_child_match_hint)))) {
            LOG_WARN("failed to check candi tree match hint", K(ret), K(i), KPC(candi_node->children_.at(i)));
          }
        }
        is_match_hint = is_child_match_hint;
        break;
      }
      case INDEX_MERGE_INTERSECT: {
        for (int64_t i = 0; OB_SUCC(ret) && !is_match_hint && i < candi_node->children_.count(); ++i) {
          int64_t sub_idx = idx;
          if (OB_FAIL(SMART_CALL(check_one_candi_node_match_hint(candi_node->children_.at(i), index_ids, sub_idx, is_match_hint)))) {
            LOG_WARN("failed to check candi tree match hint", K(ret), K(i), KPC(candi_node->children_.at(i)));
          } else if (is_match_hint) {
            ObIndexMergeNode *tmp_node = candi_node->children_.at(i);
            candi_node->children_.reuse();
            idx = sub_idx;
            if (OB_FAIL(candi_node->children_.push_back(tmp_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            }
          }
        }
        break;
      }
      case INDEX_MERGE_SCAN:
      case INDEX_MERGE_FTS_INDEX: {
        for (int64_t i = 0; OB_SUCC(ret) && !is_match_hint && i < candi_node->candicate_index_tids_.count(); ++i) {
          if (candi_node->candicate_index_tids_.at(i) == index_ids.at(idx)) {
            is_match_hint = true;
            candi_node->index_tid_ = index_ids.at(idx);
            ++idx;
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected candi node type", K(ret), KPC(candi_node));
      }
    }
  }
  return ret;
}

/**
 * @brief ObJoinOrder::prune_candi_index_merge_trees
 *
 * 1. Prune the candi index merge trees which contains only one index.
 * 2. If there is a candi tree contains FTS scan, prune all other candi trees
 *    which only contain normal index scan.
 */
int ObJoinOrder::prune_candi_index_merge_trees(ObIArray<ObIndexMergeNode*> &candi_index_trees)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIndexMergeNode*, 4> normal_candi_trees;
  ObSEArray<ObIndexMergeNode*, 4> candi_trees_contain_fts;
  if (OB_UNLIKELY(EN_FORCE_INDEX_MERGE_PLAN)) {
    // do nothing
  } else {
    // Prune the candcandi index merge trees which contains only one index.
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_index_trees.count(); ++i) {
      bool is_valid = false;
      bool is_contain_fts = false;
      ObSEArray<ObIndexMergeNode*, 4> node_stack;
      uint64_t first_index_id = OB_INVALID_ID;
      if (OB_FAIL(node_stack.push_back(candi_index_trees.at(i)))) {
        LOG_WARN("failed to push back candi tree node", K(ret));
      }
      while(OB_SUCC(ret) && !node_stack.empty()) {
        ObIndexMergeNode *candi_node = node_stack.at(node_stack.count() - 1);
        node_stack.pop_back();
        if (OB_ISNULL(candi_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null candi node", K(ret), K(i));
        } else if (candi_node->is_merge_node()) {
          for (int64_t j = 0; OB_SUCC(ret) && j < candi_node->children_.count(); ++j) {
            if (OB_FAIL(node_stack.push_back(candi_node->children_.at(j)))) {
              LOG_WARN("failed to push back candi index merge node", K(ret), K(j));
            }
          }
        } else if (INDEX_MERGE_FTS_INDEX == candi_node->node_type_) {
          is_valid = true;
          is_contain_fts = true;
          break;
        } else if (OB_UNLIKELY(candi_node->candicate_index_tids_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null candi index id", K(ret), KPC(candi_node));
        } else if (OB_INVALID_ID == first_index_id) {
          first_index_id = candi_node->candicate_index_tids_.at(0);
        } else if (first_index_id != candi_node->candicate_index_tids_.at(0)) {
          is_valid = true;
        }
      }
      if (OB_FAIL(ret) || !is_valid) {
        // do nothing
      } else if (is_contain_fts) {
        if (OB_FAIL(candi_trees_contain_fts.push_back(candi_index_trees.at(i)))) {
          LOG_WARN("failed to push back candi tree", K(ret));
        }
      } else if (OB_FAIL(normal_candi_trees.push_back(candi_index_trees.at(i)))) {
        LOG_WARN("failed to push back new candi tree", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!candi_trees_contain_fts.empty()) {
      if (OB_FAIL(candi_index_trees.assign(candi_trees_contain_fts))) {
        LOG_WARN("failed to assign new candi trees", K(ret), K(candi_trees_contain_fts));
      }
    } else if (OB_FAIL(candi_index_trees.assign(normal_candi_trees))) {
      LOG_WARN("failed to assign new candi trees", K(ret), K(normal_candi_trees));
    }
  }
  return ret;
}

int ObJoinOrder::do_create_index_merge_paths(const uint64_t table_id,
                                             const uint64_t ref_table_id,
                                             PathHelper &helper,
                                             ObIndexInfoCache &index_info_cache,
                                             ObIArray<ObIndexMergeNode*> &candi_index_trees,
                                             ObIArray<AccessPath*> &access_paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_index_trees.count(); ++i) {
    ObIndexMergeNode* root_node = candi_index_trees.at(i);
    IndexMergePath* index_merge_path = NULL;
    int64_t scan_node_count = 0;
    if (OB_ISNULL(root_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index merge node is null", K(ret), KPC(root_node));
    } else if (OB_FAIL(choose_best_selectivity_branch(root_node))) {
      LOG_WARN("failed to choose best selectivity branch", K(ret), KPC(root_node));
    } else if (OB_FAIL(root_node->formalize_index_merge_tree())) {
      LOG_WARN("failed to formalize index merge tree", K(ret), KPC(root_node));
    } else if (OB_FAIL(build_access_path_for_scan_node(table_id,
                                                       ref_table_id,
                                                       helper,
                                                       index_info_cache,
                                                       root_node,
                                                       scan_node_count))) {
      LOG_WARN("failed to build access path for scan node", K(ret));
    } else if (OB_FAIL(create_one_index_merge_path(table_id,
                                                   ref_table_id,
                                                   helper,
                                                   root_node,
                                                   index_merge_path))) {
      LOG_WARN("failed to create one index merge path", K(ret), KPC(root_node));
    } else if (OB_ISNULL(index_merge_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index merge path is null", K(ret), KPC(root_node));
    } else if (OB_FALSE_IT(index_merge_path->index_cnt_ = scan_node_count)) {
    } else if (OB_FAIL(access_paths.push_back(static_cast<AccessPath*>(index_merge_path)))) {
      LOG_WARN("failed to push back index merge path", K(ret));
    } else {
      OPT_TRACE("generate one index merge path for", root_node->filter_);
      LOG_TRACE("generate one index merge path", KPC(index_merge_path));
    }
  }
  return ret;
}

int ObJoinOrder::choose_best_selectivity_branch(ObIndexMergeNode* &candi_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(candi_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(candi_node));
  } else if (INDEX_MERGE_UNION == candi_node->node_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_node->children_.count(); ++i) {
      if (OB_FAIL(SMART_CALL(choose_best_selectivity_branch(candi_node->children_.at(i))))) {
        LOG_WARN("failed to choose best branch for child", K(ret), KPC(candi_node->children_.at(i)));
      }
    }
  } else if (INDEX_MERGE_INTERSECT == candi_node->node_type_) {
    ObIndexMergeNode* best_child = NULL;
    double best_selectivity = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_node->children_.count(); ++i) {
      ObIndexMergeNode* child_candi_node = candi_node->children_.at(i);
      double child_selectivity = 1;
      if (OB_ISNULL(child_candi_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child node is null", K(ret), K(i), KPC(candi_node));
      } else if (INDEX_MERGE_FTS_INDEX == child_candi_node->node_type_) {
        best_child = child_candi_node;
        break;
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(choose_best_selectivity_branch(child_candi_node)))) {
        LOG_WARN("failed to choose best branch for child", K(ret), KPC(child_candi_node));
      } else if (OB_FAIL(calc_selectivity_for_index_merge_node(child_candi_node,
                                                               child_selectivity))) {
        LOG_WARN("failed to calculate selectivity", K(ret));
      } else if (NULL == best_child || child_selectivity < best_selectivity) {
        best_child = child_candi_node;
        best_selectivity = child_selectivity;
      }
    }
    if (OB_SUCC(ret)) {
      candi_node = best_child;
    }
  }
  return ret;
}

int ObJoinOrder::calc_selectivity_for_index_merge_node(ObIndexMergeNode* node,
                                                       double &child_selectivity)
{
  int ret = OB_SUCCESS;
  child_selectivity = 1.0;
  if (OB_ISNULL(node) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(node), K(get_plan()));
  } else {
    switch (node->node_type_) {
      case INDEX_MERGE_UNION: {
        ObSEArray<double, 4> selectivities;
        for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
          double child_selectivity;
          if (OB_FAIL(SMART_CALL(calc_selectivity_for_index_merge_node(node->children_.at(i),
                                                                       child_selectivity)))) {
            LOG_WARN("failed to calculate child selectivity", K(ret), K(node->children_.at(i)));
          } else if (OB_FAIL(selectivities.push_back(1.0 - child_selectivity))) {
            LOG_WARN("failed to push back selectivity", K(ret));
          }
        }
        child_selectivity = 1.0 - get_plan()->get_selectivity_ctx().get_correlation_model().combine_filters_selectivity(selectivities);
        break;
      }
      case INDEX_MERGE_SCAN:
      case INDEX_MERGE_FTS_INDEX: {
        if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            node->filter_,
                                                            child_selectivity,
                                                            get_plan()->get_predicate_selectivities()))) {
          LOG_WARN("failed to calculate selectivity", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected candi node type", K(ret), KPC(node));
      }
    }
  }
  return ret;
}

int ObJoinOrder::build_access_path_for_scan_node(const uint64_t table_id,
                                                 const uint64_t ref_table_id,
                                                 PathHelper &helper,
                                                 ObIndexInfoCache &index_info_cache,
                                                 ObIndexMergeNode* node,
                                                 int64_t &scan_node_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(node));
  } else if (node->is_merge_node()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      if (OB_ISNULL(node->children_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null child", K(ret), K(i), KPC(node));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(build_access_path_for_scan_node(table_id,
                                                                    ref_table_id,
                                                                    helper,
                                                                    index_info_cache,
                                                                    node->children_.at(i),
                                                                    scan_node_count)))) {
        LOG_WARN("failed to create index merge node", K(ret), KPC(node->children_.at(i)));
      } else if (OB_FAIL(append(node->filter_, node->children_.at(i)->filter_))) {
        LOG_WARN("failed to append child filters", K(ret));
      }
    }
  } else {
    PathHelper tmp_helper = helper;
    IndexInfoEntry *index_info_entry = NULL;
    bool use_row_store = false;
    bool use_column_store = false;
    index_info_cache.set_table_id(table_id);
    index_info_cache.set_base_table_id(ref_table_id);
    tmp_helper.is_index_merge_ = true;
    node->scan_node_idx_ = scan_node_count++;
    if (OB_UNLIKELY(OB_INVALID_ID == node->index_tid_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected index id", K(ret), KPC(node));
    } else if (OB_FAIL(tmp_helper.filters_.assign(node->filter_))) {
      LOG_WARN("failed to assign filter", K(ret));
    } else if (OB_FAIL(fill_index_info_entry(table_id,
                                             ref_table_id,
                                             node->index_tid_,
                                             index_info_entry,
                                             tmp_helper))) {
      LOG_WARN("failed to fill index info entry", K(ret));
    } else if (OB_ISNULL(index_info_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null index info entry", K(ret));
    } else if (OB_FALSE_IT(index_info_entry->set_is_index_back(false))) {
    } else if (OB_FAIL(index_info_cache.add_index_info_entry(index_info_entry))) {
      LOG_WARN("failed to add index info entry", K(ret));
    } else if (OB_FAIL(get_plan()->will_use_column_store(table_id,
                                                         node->index_tid_,
                                                         ref_table_id,
                                                         use_column_store,
                                                         use_row_store))) {
      LOG_WARN("failed to check will use column store", K(ret));
    } else if (OB_FAIL(create_one_access_path(table_id,
                                              ref_table_id,
                                              node->index_tid_,
                                              index_info_cache,
                                              tmp_helper,
                                              node->ap_,
                                              true,  /* use_das */
                                              use_row_store ? false : true, /* use_column_store */
                                              OptSkipScanState::SS_UNSET /* use_skip_scan */ ))) {
      LOG_WARN("failed to create one access path", K(table_id), K(ref_table_id), K(node->index_tid_));
    }
  }
  return ret;
}

int ObJoinOrder::create_one_index_merge_path(const uint64_t table_id,
                                             const uint64_t ref_table_id,
                                             PathHelper &helper,
                                             ObIndexMergeNode* root_node,
                                             IndexMergePath* &index_merge_path)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  ObSEArray<ObRawExpr*, 4> all_match_exprs;
  ObSEArray<ObRawExpr*, 4> rowkey_exprs;
  ObOrderDirection direction = default_asc_direction();
  if (OB_ISNULL(root_node)|| OB_ISNULL(allocator_)
      || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())
      || OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(root_node), K(allocator_), K(get_plan()), K(stmt), K(table_item));
  } else if (OB_ISNULL(index_merge_path = reinterpret_cast<IndexMergePath*>(allocator_->alloc(sizeof(IndexMergePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new a index merge path", K(ret));
  } else if (OB_FALSE_IT(index_merge_path = new(index_merge_path) IndexMergePath(table_id, ref_table_id, this))) {
  } else if (OB_FALSE_IT(index_merge_path->root_ = root_node)) {
  } else if (OB_FAIL(stmt->get_match_expr_on_table(table_id, all_match_exprs))) {
    LOG_WARN("failed to get all match exprs", K(ret), K(table_id));
  } else if (OB_FAIL(index_merge_path->est_cost_info_.table_filters_.assign(helper.filters_))) {
    LOG_WARN("failed to assign filters", K(ret));
  } else if (OB_FAIL(get_plan()->get_rowkey_exprs(table_id, ref_table_id, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (FALSE_IT(index_merge_path->ordering_.reuse())) {
  } else if (OB_FAIL(get_index_scan_direction(rowkey_exprs, stmt, get_plan()->get_equal_sets(), direction))) {
    LOG_WARN("failed to get index scan direction", K(ret));
  } else if (OB_FAIL(root_node->set_scan_direction(direction))) {
    LOG_WARN("failed to set scan direction for index merge path", K(ret), KPC(index_merge_path));
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(rowkey_exprs,
                                                     index_merge_path->order_direction_,
                                                     index_merge_path->ordering_))) {
    LOG_WARN("failed to make rowkey ordering for index merge path", K(ret));
  } else if (OB_FAIL(stmt->get_column_items(table_id, index_merge_path->est_cost_info_.access_column_items_))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    index_merge_path->table_id_ = table_id;
    index_merge_path->ref_table_id_ = ref_table_id;
    index_merge_path->index_id_ = ref_table_id;
    index_merge_path->use_das_ = true;
    index_merge_path->use_skip_scan_ = OptSkipScanState::SS_DISABLE;
    index_merge_path->table_opt_info_ = helper.table_opt_info_;
    index_merge_path->is_inner_path_ = helper.is_inner_path_;
    index_merge_path->for_update_ = table_item->for_update_;
    index_merge_path->use_column_store_ = false;
    index_merge_path->contain_das_op_ = index_merge_path->use_das_;
    index_merge_path->is_ordered_by_pk_ = true;
    index_merge_path->can_batch_rescan_ = false;
    index_merge_path->parent_ = this;
    index_merge_path->order_direction_ = direction;
    index_merge_path->est_cost_info_.index_meta_info_.is_index_back_ = true;
    index_merge_path->est_cost_info_.is_virtual_table_ = is_virtual_table(ref_table_id);
    index_merge_path->est_cost_info_.table_metas_ = &get_plan()->get_basic_table_metas();
    index_merge_path->est_cost_info_.sel_ctx_ = &get_plan()->get_selectivity_ctx();
    index_merge_path->est_cost_info_.rescan_server_list_ = &index_merge_path->server_list_;
    index_merge_path->est_cost_info_.is_rescan_ = helper.is_inner_path_ || get_plan()->get_is_rescan_subplan();
    index_merge_path->est_cost_info_.use_column_store_ = index_merge_path->use_column_store_;
    index_merge_path->domain_idx_info_.set_domain_idx_type(DomainIndexType::FTS_INDEX);
    if (NULL == table_partition_info_) {
      // generate table location for main table
      if (OB_FAIL(compute_table_location(table_id,
                                         ref_table_id,
                                         false,
                                         index_merge_path->table_partition_info_))) {
        LOG_WARN("failed to compute table location", K(ret), K(table_id), K(ref_table_id));
      }
    } else {
      index_merge_path->table_partition_info_ = table_partition_info_;
    }
  }

  // prepare func lookup tr_infos for index merge path, we only need match exprs which is not in index.
  ObSEArray<ObRawExpr*, 4> match_exprs_in_index;
  for (int64_t i = 0; OB_SUCC(ret) && i < root_node->filter_.count(); ++i) {
    ObRawExpr *filter = root_node->filter_.at(i);
    if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null filter", K(ret), K(i), KPC(root_node));
    } else if (filter->get_expr_type() == T_OP_BOOL && filter->has_flag(CNT_MATCH_EXPR)) {
      ObRawExpr *param_expr = NULL;
      if (OB_UNLIKELY(0 >= filter->get_param_count()) || OB_ISNULL(param_expr = filter->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param expr for bool op", K(ret), KPC(filter));
      } else if (!param_expr->has_flag(IS_MATCH_EXPR)) {
        // do nothing
      } else if (OB_FAIL(match_exprs_in_index.push_back(param_expr))) {
        LOG_WARN("failed to append match expr to array", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_match_exprs.count(); ++i) {
    ObRawExpr *cur_expr = all_match_exprs.at(i);
    const MatchExprInfo *match_expr_info = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null match expr", K(ret), K(i));
    } else if (ObOptimizerUtil::find_item(match_exprs_in_index, cur_expr)) {
      // do nothing
    } else if (OB_FAIL(find_match_expr_info(helper.match_expr_infos_, cur_expr, match_expr_info))) {
      LOG_WARN("failed to find match expr info", K(ret), KPC(cur_expr));
    } else if (OB_ISNULL(match_expr_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null match expr info", K(ret));
    } else if (OB_FAIL(index_merge_path->domain_idx_info_.func_lookup_exprs_.push_back(cur_expr))) {
      LOG_WARN("failed to append func lookup exprs", K(ret), KPC(cur_expr));
    } else if (OB_FAIL(index_merge_path->domain_idx_info_.func_lookup_index_ids_.push_back(match_expr_info->inv_idx_id_))) {
      LOG_WARN("failed to append func lookup index id", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::prune_index_merge_path(ObIArray<AccessPath*> &access_paths)
{
  int ret = OB_SUCCESS;
  ObSEArray<AccessPath*, 4> tmp_paths;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    ObSEArray<AccessPath*, 4> scan_paths;
    uint64_t first_index_id = OB_INVALID_ID;
    bool contain_multi_index = false;
    if (OB_ISNULL(access_paths.at(i))
        || OB_UNLIKELY(!access_paths.at(i)->is_index_merge_path())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected index merge path", K(ret), K(i), KPC(access_paths.at(i)));
    } else if (OB_FAIL(static_cast<IndexMergePath*>(access_paths.at(i))->get_all_scan_access_paths(scan_paths))) {
      LOG_WARN("failed to get all scan access paths", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !contain_multi_index && j < scan_paths.count(); ++j) {
      if (OB_ISNULL(scan_paths.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(j));
      } else if (OB_INVALID_ID == first_index_id) {
        first_index_id = scan_paths.at(j)->index_id_;
      } else if (first_index_id != scan_paths.at(j)->index_id_) {
        contain_multi_index = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!contain_multi_index) {
      OPT_TRACE("prune index merge path because it contains only one index:", static_cast<IndexMergePath*>(access_paths.at(i))->root_);
    } else if (OB_FAIL(tmp_paths.push_back(access_paths.at(i)))) {
      LOG_WARN("failed to push back access path", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_paths.assign(tmp_paths))) {
    LOG_WARN("failed to assign access paths", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_index_merge_paths_contain_fts(ObIArray<AccessPath*> &access_paths,
                                                     bool &contain_fts)
{
  int ret = OB_SUCCESS;
  ObIndexMergeNode *root_node = NULL;
  contain_fts = false;
  if (OB_UNLIKELY(access_paths.empty())
      || OB_ISNULL(access_paths.at(0))
      || OB_UNLIKELY(!access_paths.at(0)->is_index_merge_path())
      || OB_ISNULL(root_node = static_cast<IndexMergePath*>(access_paths.at(0))->root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index merge path", K(ret), K(access_paths), K(root_node));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < root_node->children_.count(); ++i) {
    ObIndexMergeNode *child_node = root_node->children_.at(i);
    if (OB_ISNULL(child_node)
        || OB_UNLIKELY(!child_node->is_scan_node())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected index merge node", K(ret), K(i), K(root_node));
    } else if (INDEX_MERGE_FTS_INDEX == child_node->node_type_) {
      contain_fts = true;
      break;
    }
  }
  return ret;
}

int IndexMergePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  ObSEArray<AccessPath*, 8> scan_aps;
  ObSEArray<ObRawExpr*, 4> rowkey_exprs;
  ObSEArray<OrderItem, 4> order_items;
  double all_rowcnt = 0.0;
  ENABLE_OPT_TRACE_COST_MODEL;
  OPT_TRACE_COST_MODEL("calc cost for index merge path:", root_);
  DISABLE_OPT_TRACE_COST_MODEL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(plan->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_), KPC(plan));
  } else if (OB_FAIL(get_all_scan_access_paths(scan_aps))) {
    LOG_WARN("failed to get all scan access paths", K(ret));
  } else if (OB_FAIL(plan->get_rowkey_exprs(table_id_, ref_table_id_, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
    if (OB_FAIL(order_items.push_back(OrderItem(rowkey_exprs.at(i), order_direction_)))) {
      LOG_WARN("failed to push back order item", K(ret));
    }
  }
  // scan and sort cost
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_aps.count(); ++i) {
    if (OB_ISNULL(scan_aps.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (OB_FAIL(scan_aps.at(i)->estimate_cost())) {
      LOG_WARN("failed to estimate cost", K(ret), KPC(scan_aps.at(i)));
    } else {
      ENABLE_OPT_TRACE_COST_MODEL;
      all_rowcnt += scan_aps.at(i)->get_output_row_count();
      cost_ += scan_aps.at(i)->cost_;
      OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") += scan_cost (", scan_aps.at(i)->cost_, ")");
      if (!scan_aps.at(i)->is_ordered_by_pk_) {
        double sort_cost = 0.0;
        double width = 0.0;
        ObSEArray<ObRawExpr*, 4> output_exprs;
        for (int64_t j = 0; OB_SUCC(ret) && j < scan_aps.at(i)->est_cost_info_.index_access_column_items_.count(); ++j) {
          ColumnItem &col = scan_aps.at(i)->est_cost_info_.index_access_column_items_.at(j);
          ObRawExpr *col_expr = NULL;
          if (OB_ISNULL(col_expr = plan->get_stmt()->get_column_expr_by_id(col.table_id_, col.column_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null column", K(ret), K(col.table_id_), K(col.column_id_));
          } else if (OB_FAIL(output_exprs.push_back(col_expr))) {
            LOG_WARN("failed to push back column item", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(plan->get_basic_table_metas(),
                                                                  plan->get_selectivity_ctx(),
                                                                  output_exprs,
                                                                  width))) {
          LOG_WARN("failed to estimate width for output join column exprs", K(ret));
        } else {
          ObSortCostInfo cost_info(scan_aps.at(i)->get_output_row_count(),
                                   width,
                                   -1, /* prefix_pos */
                                   order_items,
                                   false, /* is_local_merge_sort */
                                   &plan->get_update_table_metas(),
                                   &plan->get_selectivity_ctx(),
                                   -1, /* topn */
                                   0 /* part_cnt */ );
          if (OB_FAIL(ObOptEstCost::cost_sort(cost_info, sort_cost, plan->get_optimizer_context()))) {
            LOG_WARN("failed to calc sort cost", K(ret));
          } else {
            cost_ += sort_cost;
            OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") +=", KV(sort_cost));
          }
        }
      } else {
        OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") += sort_cost ( 0 ), scan is ordered by pk");
      }
      DISABLE_OPT_TRACE_COST_MODEL;
    }
  }
  // merge cost
  ENABLE_OPT_TRACE_COST_MODEL;
  if (OB_SUCC(ret)) {
    double merge_cost_per_row = 0.0;
    double merge_cost = 0.0;
    ObSEArray<ObExprResType, 4> types;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
      if (OB_ISNULL(rowkey_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (OB_FAIL(types.push_back(rowkey_exprs.at(i)->get_result_type()))) {
        LOG_WARN("failed to push back column type", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOptEstCost::get_sort_cmp_cost(types,
                                                       merge_cost_per_row,
                                                       plan->get_optimizer_context()))) {
      LOG_WARN("failed to est index back cost for inedx merge", K(ret));
    } else {
      merge_cost = (double)scan_aps.count() * all_rowcnt * merge_cost_per_row;
      cost_ += merge_cost;
      OPT_TRACE_COST_MODEL("merge_cost (", merge_cost, ") = scan_count (", scan_aps.count(), ") * all_row_count (", all_rowcnt, ") * merge_cost_per_row (", merge_cost_per_row, ")");
      OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") +=", KV(merge_cost));
    }
  }
  // index back cost
  if (OB_SUCC(ret)) {
    double index_back_cost = 0.0;
    if (OB_FAIL(ObOptEstCost::cost_index_back(est_cost_info_,
                                              est_cost_info_.index_back_row_count_,
                                              -1, /* limit_count */
                                              index_back_cost,
                                              plan->get_optimizer_context()))) {
      LOG_WARN("failed to est index back cost for inedx merge", K(ret));
    } else {
      cost_ += index_back_cost;
      OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") +=", KV(index_back_cost));
    }
  }
  // filter cost
  if (OB_SUCC(ret)) {
    double filter_cost = ObOptEstCost::cost_quals(est_cost_info_.index_back_row_count_, est_cost_info_.table_filters_, plan->get_optimizer_context());
    cost_ += filter_cost;
    OPT_TRACE_COST_MODEL("index_merge_cost (", cost_, ") +=", KV(filter_cost));
  }
  DISABLE_OPT_TRACE_COST_MODEL;
  return ret;
}

int IndexMergePath::get_all_scan_access_paths(ObIArray<AccessPath*> &scan_paths) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIndexMergeNode*, 8> node_stack;
  if (OB_FAIL(node_stack.push_back(root_))) {
    LOG_WARN("failed to push back index merge root node", K(ret));
  }
  while (OB_SUCC(ret) && !node_stack.empty()) {
    ObIndexMergeNode *node = node_stack.at(node_stack.count() - 1);
    node_stack.pop_back();
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (node->is_merge_node()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
        if (OB_FAIL(node_stack.push_back(node->children_.at(i)))) {
          LOG_WARN("failed to push back index merge node", K(ret), K(i));
        }
      }
    } else if (OB_ISNULL(node->ap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(scan_paths.push_back(node->ap_))) {
      LOG_WARN("failed to push back scan path", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::will_use_skip_scan(const uint64_t table_id,
                                    const uint64_t ref_id,
                                    const uint64_t index_id,
                                    const ObIndexInfoCache &index_info_cache,
                                    PathHelper &helper,
                                    ObSQLSessionInfo *session_info,
                                    OptSkipScanState &use_skip_scan)
{
  int ret = OB_SUCCESS;
  use_skip_scan = OptSkipScanState::SS_UNSET;
  IndexInfoEntry *index_info_entry = NULL;
  const ObQueryRangeProvider *query_range_provider = NULL;
  ObQueryCtx *query_ctx = NULL;
  bool hint_force_skip_scan = false;
  bool hint_force_no_skip_scan = false;
  if (OB_UNLIKELY(OB_INVALID_ID == ref_id) || OB_UNLIKELY(OB_INVALID_ID == index_id) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(query_ctx = get_plan()->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ref_id), K(index_id), K(get_plan()), K(query_ctx), K(ret));
  } else if (is_virtual_table(ref_id)) {
    use_skip_scan = OptSkipScanState::SS_DISABLE;
  } else if (OB_FAIL(index_info_cache.get_index_info_entry(table_id, index_id,
                                                           index_info_entry))) {
    LOG_WARN("failed to get index info entry", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(index_info_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info entry should not be null", K(ret));
  } else if (OB_ISNULL(query_range_provider = index_info_entry->get_range_info().get_query_range_provider()) ||
             !query_range_provider->is_ss_range()) {
    use_skip_scan = OptSkipScanState::SS_DISABLE;
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().check_use_skip_scan(table_id,
                                                                         index_id,
                                                                         hint_force_skip_scan,
                                                                         hint_force_no_skip_scan))) {
    LOG_WARN("failed to check use skip scan", K(ret), K(table_id));
  } else if (hint_force_skip_scan) {
    use_skip_scan = OptSkipScanState::SS_HINT_ENABLE;
  } else if (hint_force_no_skip_scan) {
    use_skip_scan = OptSkipScanState::SS_DISABLE;
  } else if (!OPT_CTX.get_is_skip_scan_enabled()) {
    use_skip_scan = OptSkipScanState::SS_DISABLE;
  } else if (get_plan()->get_is_rescan_subplan() || helper.is_inner_path_) {
    use_skip_scan = OptSkipScanState::SS_DISABLE;
  } else {
    // may use skip scan for SS_NDV_SEL_ENABLE after calculate ndv and selectivity
    use_skip_scan = OptSkipScanState::SS_UNSET;
  }

  if (OB_SUCC(ret) && OptSkipScanState::SS_DISABLE != use_skip_scan) {
    // OptColumnMeta for prefix columns may be not added. It's needed to calculate prefix NDV
    const ObIArray<ColumnItem> &column_items = index_info_entry->get_range_info().get_range_columns();
    const int64_t ss_offset = query_range_provider->get_skip_scan_offset();
    const OptSelectivityCtx &ctx = get_plan()->get_selectivity_ctx();
    OptTableMeta *table_meta = NULL;
    if (OB_UNLIKELY(column_items.count() < ss_offset) ||
        OB_ISNULL(table_meta = get_plan()->get_basic_table_metas().get_table_meta_by_table_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected params", K(ret), K(column_items.count()), K(ss_offset), K(table_meta));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_offset; ++i) {
      if (OB_FAIL(table_meta->add_column_meta_no_dup(column_items.at(i).column_id_ , ctx))) {
        LOG_WARN("failed to add column meta no duplicate", K(ret));
	  }
	}
  }
  LOG_TRACE("check use skip scan", K(helper.is_inner_path_),
                          K(hint_force_skip_scan), K(hint_force_no_skip_scan), K(use_skip_scan));
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
int ObJoinOrder::estimate_rowcount_for_access_path(ObIArray<AccessPath*> &all_paths,
                                                   const bool is_inner_path,
                                                   common::ObIArray<ObRawExpr*> &filter_exprs,
                                                   ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  bool is_use_ds = false;
  method = EST_INVALID;
  if (OB_FAIL(ObAccessPathEstimation::estimate_rowcount(OPT_CTX, all_paths,
                                                        is_inner_path,
                                                        filter_exprs,
                                                        method))) {
    LOG_WARN("failed to do access path estimation", K(ret));
  } else if (!is_inner_path && !(method & EST_DS_FULL) && OB_FAIL(compute_table_rowcount_info())) {
    LOG_WARN("failed to compute table rowcount info", K(ret));
  }
  return ret;
}

int ObJoinOrder::compute_table_rowcount_info()
{
  int ret = OB_SUCCESS;
  /*
   * 计算完所有的信息再计算选择率. 为了统计信息的准确,
   * 我们需要先将行数置为全表的行数.
   */
  if (OB_SUCC(ret)) {
    double selectivity = 0.0;
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                        get_plan()->get_selectivity_ctx(),
                                                        get_restrict_infos(),
                                                        selectivity,
                                                        get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret));
    } else {
      table_meta_info_.row_count_ =
          static_cast<double>(table_meta_info_.table_row_count_) * selectivity;
      set_output_rows(table_meta_info_.row_count_);
    }
    LOG_TRACE("OPT: after fill table meta info", K(table_meta_info_), K(selectivity));
  }
  return ret;
}

int ObJoinOrder::get_valid_index_ids(const uint64_t table_id,
                                     const uint64_t ref_table_id,
                                     PathHelper &helper,
                                     ObIArray<uint64_t> &valid_index_ids)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  uint64_t tids[OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1];
  int64_t index_count = OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1;
  const LogTableHint *log_table_hint = NULL;
  const ObSelectStmt *select_stmt = NULL;
  bool has_aggr = false; // defend aggr for ann search
  const bool has_match_expr_on_table = helper.match_expr_infos_.count() > 0;
  bool can_use_global_index = false;
  bool add_only_vec_index_id = false;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(session_info = OPT_CTX.get_session_info()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx()->get_sql_ctx()) ||
      OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(stmt), K(schema_guard), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table item should not be NULL", K(table_id), K(table_item), K(ret));
  } else if (stmt->is_select_stmt() && FALSE_IT(select_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (FALSE_IT(can_use_global_index = (table_item->access_all_part() && !has_match_expr_on_table && !stmt->has_vec_approx()))) {
  } else if (has_match_expr_on_table && stmt->has_vec_approx()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "when using match against expr, vector index is");
  } else if (nullptr != select_stmt && FALSE_IT(has_aggr = select_stmt->get_aggr_item_size() > 0)) {
  } else if (table_item->is_index_table_) {
    if (OB_FAIL(valid_index_ids.push_back(table_item->ref_id_))) {
      LOG_WARN("failed to push back array", K(ret));
    } else { /*do nothing*/ }
  } else if (FALSE_IT(log_table_hint = get_plan()->get_log_plan_hint().get_index_hint(table_id))) {
  } else if (NULL != log_table_hint && log_table_hint->is_use_index_hint()) {
    // for use index hint, get index ids from hint.
    ObSEArray<uint64_t, 4> valid_hint_index_list;
    const bool is_link = ObSqlSchemaGuard::is_link_table(stmt, table_id);
    if (OB_FAIL(get_valid_hint_index_list(*stmt, log_table_hint->index_list_, is_link, schema_guard, helper, valid_hint_index_list))) {
      LOG_WARN("failed to get valid hint index list", K(ret));
    } else if (OB_FAIL(valid_index_ids.assign(valid_hint_index_list))) {
      LOG_WARN("failed to assign index ids", K(ret));
    }
  }

  if (OB_FAIL(ret) || valid_index_ids.count() > 0) {
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(ref_table_id,
                                                            tids,
                                                            index_count,
                                                            false,
                                                            can_use_global_index,
                                                            false /*domain index*/,
                                                            false /*spatial index*/,
                                                            false /*vector index*/))) {
    LOG_WARN("failed to get can read index", K(ref_table_id), K(ret));
  } else if (OB_FAIL(add_valid_fts_index_ids(helper, tids, index_count))) {
    LOG_WARN("failed to add valid fts index ids", K(ret));
  } else if (OB_FAIL(add_valid_vec_index_ids(*stmt, schema_guard, table_id, ref_table_id, has_aggr, tids, index_count))) {
    LOG_WARN("failed to add valid vec index ids", K(ret));
  } else if (index_count > OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid index count", K(ref_table_id), K(index_count), K(ret));
  } else if (NULL != log_table_hint &&
             OB_FAIL(get_valid_index_ids_with_no_index_hint(*schema_guard, ref_table_id,
                                                            tids, index_count,
                                                            log_table_hint->index_list_,
                                                            valid_index_ids))) {
    LOG_WARN("failed to get hint index ids", K(ret));
  } else {
    if (0 == valid_index_ids.count()) {
      for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
        const uint64_t tid = (i == -1) ? ref_table_id : tids[i]; //with base table
        if (OB_FAIL(valid_index_ids.push_back(tid))) {
          LOG_WARN("failed to push back index id", K(ret));
        } else { /*do nothing*/ }
      }
    }

    //check table access policy
    const ObTableSchema *schema = NULL;
    bool is_link = false;
    bool has_row_store = false;
    bool has_column_store = false;
    ObSEArray<uint64_t, 4> new_valid_index_ids;
    if (ObTableAccessPolicy::ROW_STORE == OPT_CTX.get_table_acces_policy()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
        if (OB_FAIL(schema_guard->get_table_schema(valid_index_ids.at(i), schema, is_link))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null table schema", K(ret));
        } else if (OB_FAIL(schema->has_all_column_group(has_row_store))) {
          LOG_WARN("failed to check has row store", K(ret));
        } else if (!has_row_store) {
          //ignore index
        } else if (OB_FAIL(new_valid_index_ids.push_back(valid_index_ids.at(i)))) {
          LOG_WARN("failed to push back index id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (new_valid_index_ids.empty()) {
        //ignore table access policy
      } else if (OB_FAIL(valid_index_ids.assign(new_valid_index_ids))) {
        LOG_WARN("failed to assign index ids", K(ret));
      }
    } else if (ObTableAccessPolicy::COLUMN_STORE == OPT_CTX.get_table_acces_policy()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids.count(); ++i) {
        if (OB_FAIL(schema_guard->get_table_schema(valid_index_ids.at(i), schema, is_link))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null table schema", K(ret));
        } else if (OB_FAIL(schema->get_is_column_store(has_column_store))) {
          LOG_WARN("failed to get is column store", K(ret));
        } else if (!has_column_store) {
          //ignore index
        } else if (OB_FAIL(new_valid_index_ids.push_back(valid_index_ids.at(i)))) {
          LOG_WARN("failed to push back index id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (new_valid_index_ids.empty()) {
        //ignore table access policy
      } else if (OB_FAIL(valid_index_ids.assign(new_valid_index_ids))) {
        LOG_WARN("failed to assign index ids", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObTableSchema *schema = NULL;
    bool is_link = ObSqlSchemaGuard::is_link_table(stmt, table_id);
    OPT_TRACE("valid index:");
    for (int64_t i = 0; i < valid_index_ids.count(); ++i) {
      schema_guard->get_table_schema(valid_index_ids.at(i), schema, is_link);
      ObString name;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table schema", K(ret));
      } else if (ref_table_id == valid_index_ids.at(i)) {
        name = schema->get_table_name_str();
      } else if (OB_FAIL(schema->get_index_name(name))) {
        LOG_WARN("failed to get index name", K(ret));
      }
      OPT_TRACE(valid_index_ids.at(i), "->", name);
    }
  }

  if (OB_SUCC(ret) && is_virtual_table(ref_table_id) && !valid_index_ids.empty()
      && OB_FAIL(add_var_to_array_no_dup(valid_index_ids, ref_table_id))) {
    LOG_WARN("failed add primary key id to array no dup", K(ret));
  }
  LOG_TRACE("all valid index id", K(valid_index_ids), K(ret));
  return ret;
}

int ObJoinOrder::compute_cost_and_prune_access_path(PathHelper &helper,
                                                    ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = NULL;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(get_plan()->get_optimizer_context().get_exec_ctx()) ||
      OB_ISNULL(task_exec_ctx = get_plan()->get_optimizer_context().get_task_exec_ctx()) ||
      OB_ISNULL(sql_ctx = get_plan()->get_optimizer_context().get_exec_ctx()->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(sql_ctx), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
      AccessPath *ap = access_paths.at(i);
      if (OB_ISNULL(ap) || OB_ISNULL(ap->get_sharding())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ap), K(ret));
      } else if (OB_FAIL(ap->estimate_cost())) {
        LOG_WARN("failed to estimate cost", K(ret));
      } else if (OB_FAIL(ap->compute_pipeline_info())) {
        LOG_WARN("failed to compute pipelined path", K(ret));
      } else if (OB_FAIL(ap->compute_valid_inner_path())) {
        LOG_WARN("failed to compute inner path with pushdown filters", K(ret));
      } else if (!helper.is_inner_path_) {
        if (OB_FAIL(add_path(ap))) {
          LOG_WARN("failed to add the interesting order");
        } else {
          LOG_TRACE("OPT:succeed to create normal access path", K(*ap));
        }
      } else if (!is_virtual_table(ap->get_ref_table_id()) ||
                is_oracle_mapping_real_virtual_table(ap->get_ref_table_id()) ||
                ap->is_get_ ||
                helper.force_inner_nl_) {
        if (OB_FAIL(helper.inner_paths_.push_back(ap))) {
          LOG_WARN("failed to push back inner path", K(ret));
        } else {
          LOG_TRACE("OPT:succeed to add inner access path", K(*ap));
        }
      } else {
        LOG_TRACE("path not add ", K(helper.force_inner_nl_));
      }
    } // add path end
  }
  return ret;
}

/*
 * this function try to revise output rows after creating all paths
 */
int ObJoinOrder::revise_output_rows_after_creating_path(PathHelper &helper,
                                                        ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  AccessPath *path = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    if (OB_ISNULL(path = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access path is null", K(ret), K(i));
    } else if (path->is_false_range()) {
      path->set_output_row_count(0.0);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!helper.is_inner_path_) {
    LOG_TRACE("OPT:output row count before revising", K(output_rows_));
    // get the minimal output row count
    int64_t maximum_count = -1;
    int64_t range_prefix_count = -1;
    if (helper.est_method_ & EST_STORAGE) {
      bool contain_false_range_path = false;
      for (int64_t i = 0; OB_SUCC(ret) && !contain_false_range_path && i < access_paths.count(); ++i) {
        path = access_paths.at(i);
        if (OB_ISNULL(path)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null path", K(ret));
        } else if (OB_UNLIKELY((range_prefix_count = path->range_prefix_count_) < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected range prefix count", K(ret), K(range_prefix_count));
        } else if (path->is_false_range()) {
          contain_false_range_path = true;
          output_rows_ = 0.0;
          LOG_TRACE("OPT:revise output rows for false range", K(output_rows_));
        } else if (maximum_count <= range_prefix_count) {
          LOG_TRACE("OPT:revise output rows", K(path->get_output_row_count()),
              K(output_rows_), K(maximum_count), K(range_prefix_count), K(ret));
          if (maximum_count == range_prefix_count) {
            output_rows_ = std::min(path->get_output_row_count(), output_rows_);
          } else {
            output_rows_ = path->get_output_row_count();
            maximum_count = range_prefix_count;
          }
        } else { /*do nothing*/ }
      }
    } else {
      if (OB_UNLIKELY(access_paths.empty()) ||
          OB_ISNULL(access_paths.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(access_paths));
      } else {
        output_rows_ = access_paths.at(0)->get_output_row_count();
      }
    }

    LOG_TRACE("OPT:output row count after revising", K(output_rows_));

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::update_table_meta_info(
              get_plan()->get_basic_table_metas(),
              get_plan()->get_update_table_metas(),
              get_plan()->get_selectivity_ctx(),
              table_id_,
              output_rows_,
              get_restrict_infos(),
              get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to update table meta info", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan and stmt should not be null", K(ret));
    } else if (get_plan()->get_stmt()->has_limit()) {
      // 这部分代码主要处理有limit时re_est_cost()的时候行数估计的问题
      // 仅在含有limit的时候才调整选择率
      for (int64_t i = 0; OB_SUCC(ret) && i < interesting_paths_.count(); ++i) {
        if (OB_ISNULL(interesting_paths_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null path", K(ret));
        } else if (!interesting_paths_.at(i)->is_access_path()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should be access path", K(ret));
        } else {
          path = static_cast<AccessPath *> (interesting_paths_.at(i));
          if (OB_UNLIKELY(std::fabs(path->get_output_row_count()) < OB_DOUBLE_EPSINON)) {
            // do nothing
          } else {
            double revise_ratio = output_rows_ / path->get_output_row_count();
            ObCostTableScanInfo &cost_info = path->get_cost_table_scan_info();
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
            cost_info.table_filter_sel_ =
                ObOptSelectivity::revise_between_0_1(cost_info.table_filter_sel_);
            cost_info.postfix_filter_sel_ =
                ObOptSelectivity::revise_between_0_1(cost_info.postfix_filter_sel_);
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
        path = static_cast<AccessPath *> (helper.inner_paths_.at(i));
        path->inner_row_count_ = std::min(path->get_output_row_count(), output_rows_);
      }
    }
  }
  return ret;
}

int ObJoinOrder::fill_opt_info_index_name(const uint64_t table_id,
                                          const uint64_t base_table_id,
                                          ObIArray<uint64_t> &available_index_id,
                                          ObIArray<uint64_t> &unstable_index_id,
                                          BaseTableOptInfo *table_opt_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  uint64_t index_ids[OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 3];
  int64_t index_count = OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 3;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(table_opt_info) || OB_ISNULL(get_plan())
      || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())
      || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()),K(schema_guard), K(table_opt_info), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == base_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table id", K(base_table_id), K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(base_table_id,
                                                            index_ids,
                                                            index_count,
                                                            false,
                                                            true /*global index*/,
                                                            true /*domain index*/))) {
    LOG_WARN("failed to get can read index", K(base_table_id), K(ret));
  } else if (index_count > OB_MAX_AUX_TABLE_PER_MAIN_TABLE + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid index count", K(base_table_id), K(index_count), K(ret));
  } else if (OB_FAIL(table_opt_info->available_index_id_.assign(available_index_id))) {
    LOG_WARN("failed to assign available index id", K(ret));
  } else {
    index_ids[index_count++] = base_table_id;
    // i == -1 represents primary key, other value of i represent index
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
      ObString name;
      uint64_t index_id = index_ids[i];
      if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret), K(index_id));
      } else if (table_schema->is_built_in_fts_index()) {
        // do nothing
      } else if (base_table_id == index_id) {
        name = table_schema->get_table_name_str();
      } else if (OB_FAIL(table_schema->get_index_name(name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        if (name.empty()) {
          // do nothing
        } else if (OB_FAIL(table_opt_info->available_index_name_.push_back(name))) {
          LOG_WARN("failed to push back index name", K(name), K(ret));
        } else { /* do nothing */ }
      }

      if (OB_FAIL(ret)) {
      } else if (name.empty()) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(available_index_id, index_id)) {
        //do nothing
      } else if (ObOptimizerUtil::find_item(unstable_index_id, index_id)) {
        if (OB_FAIL(table_opt_info->unstable_index_name_.push_back(name))) {
          LOG_WARN("failed to push back index name", K(name), K(ret));
        } else { /* do nothing */ }
      } else if (OB_FAIL(table_opt_info->pruned_index_name_.push_back(name))) {
        LOG_WARN("failed to push back index name", K(name), K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

/*
 * 检查interesting order
 * @keys 索引列
 * @stmt
 * @interest_column_ids 匹配索引前缀的列id
 * @const_column_info   interest_column_ids对应的每一个column是否为const
 * 具体的匹配规则可以看
 *
 * */
int ObJoinOrder::check_all_interesting_order(const ObIArray<OrderItem> &ordering,
                                             const ObDMLStmt *stmt,
                                             int64_t &max_prefix_count,
                                             int64_t &interesting_order_info)
{
  int ret = OB_SUCCESS;
  max_prefix_count = 0;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(get_plan()));
  } else {
    int64_t prefix_count = 0;
    bool join_match = false;
    //连接条件上是否匹配, 外连接和内连接
    if (OB_FAIL(is_join_match(ordering, prefix_count, join_match))) {
      LOG_WARN("check_join_match failed", K(ret));
    } else if (join_match) {
      max_prefix_count = std::max(max_prefix_count, prefix_count);
      interesting_order_info |= OrderingFlag::JOIN_MATCH;
      LOG_TRACE("check is_join_match debug", K(join_match), K(max_prefix_count), K(prefix_count));
    }
    //检查是否有GroupBy/OrderBy/Distinct可用
    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::CHECK_ALL;
      prefix_count = 0;
      if (OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(ordering,
                                                                  stmt,
                                                                  get_plan()->get_is_subplan_scan(),
                                                                  get_plan()->get_equal_sets(),
                                                                  get_plan()->get_const_exprs(),
                                                                  get_plan()->get_is_parent_set_distinct(),
                                                                  check_scope,
                                                                  interesting_order_info,
                                                                  prefix_count))) {
        LOG_WARN("failed to compute stmt interesting order", K(ret));
      } else {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_all_interesting_order(const ObIArray<OrderItem> &ordering,
                                             const ObDMLStmt *stmt,
                                             int64_t &interesting_order_info)
{
  int ret = OB_SUCCESS;
  bool join_match = false;
  int64_t check_scope = OrderingCheckScope::CHECK_ALL;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(get_plan()));
  } else if (OB_FAIL(is_join_match(ordering, join_match))) {
    LOG_WARN("check_join_match failed", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(ordering, stmt,
                                                                get_plan()->get_is_subplan_scan(),
                                                                get_plan()->get_equal_sets(),
                                                                get_plan()->get_const_exprs(),
                                                                get_plan()->get_is_parent_set_distinct(),
                                                                check_scope,
                                                                interesting_order_info))) {
    LOG_WARN("failed to compute stmt interesting order", K(ret));
  } else if (join_match) {
    interesting_order_info |= OrderingFlag::JOIN_MATCH;
    LOG_TRACE("check is_join_match debug", K(join_match));
  }
  return ret;
}

int ObJoinOrder::extract_interesting_column_ids(const ObIArray<ObRawExpr*> &keys,
                                                const int64_t &max_prefix_count,
                                                ObIArray<uint64_t> &interest_column_ids,
                                                ObIArray<bool> &const_column_info)
{
  int ret = OB_SUCCESS;
  if (max_prefix_count > 0) {//some sort match
    if (OB_FAIL(ObSkylineDimRecorder::extract_column_ids(keys, max_prefix_count,
                                                         interest_column_ids))) {
      LOG_WARN("extract column ids failed", K(ret));
    } else {
      LOG_TRACE("check interesting order debug", K(max_prefix_count), K(keys.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < max_prefix_count; i++) {
        bool is_const = false;
        if (OB_ISNULL(keys.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(keys.at(i), output_equal_sets_,
                                                          get_output_const_exprs(),
                                                          is_const))) {
          LOG_WARN("check expr is const expr failed", K(ret));
        } else if (OB_FAIL(const_column_info.push_back(is_const))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to push back const column info", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObJoinOrder::is_join_match(const ObIArray<OrderItem> &ordering,
                               int64_t &prefix_count,   // max join match prefix
                               bool &sort_match)
{
  int ret = OB_SUCCESS;
  sort_match = false;
  prefix_count = 0;
  const JoinInfo *join_info = NULL;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  ObSEArray<ObRawExpr *, 4> related_join_keys;
  ObSEArray<ObRawExpr *, 4> other_join_keys;
  ObSEArray<bool, 4> null_safe_info;
  bool dummy_is_coverd = false;
  int64_t match_prefix_count = 0;
  ObLogPlan *plan = get_plan();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::split_expr_direction(ordering,
                                                          ordering_exprs,
                                                          ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else {
    const ObIArray<ObConflictDetector*> &conflict_detectors = plan->get_conflict_detectors();
    // get max prefix count from inner join infos
    for (int64_t i = 0; OB_SUCC(ret) && i < conflict_detectors.count(); i++) {
      related_join_keys.reuse();
      other_join_keys.reuse();
      if (OB_ISNULL(conflict_detectors.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null detectors", K(ret));
      } else if (OB_FALSE_IT(join_info = &(conflict_detectors.at(i)->get_join_info()))) {
      } else if (!join_info->table_set_.overlap(get_tables()) ||
                  ObOptimizerUtil::find_item(used_conflict_detectors_, conflict_detectors.at(i))) {
        //do nothing
      } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(join_info->equal_join_conditions_,
                                                        get_tables(),
                                                        related_join_keys,
                                                        other_join_keys,
                                                        null_safe_info))) {
        LOG_WARN("failed to get equal keys", K(ret));
      } else if (OB_FAIL(extract_real_join_keys(related_join_keys))) {
        LOG_WARN("failed to extract real join keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(related_join_keys,
                                                              ordering_exprs,
                                                              get_output_equal_sets(),
                                                              get_output_const_exprs(),
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

int ObJoinOrder::is_join_match(const ObIArray<OrderItem> &ordering,
                               bool &sort_match)
{
  int ret = OB_SUCCESS;
  sort_match = false;
  const JoinInfo *join_info = NULL;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  ObSEArray<ObRawExpr *, 4> related_join_keys;
  ObSEArray<ObRawExpr *, 4> other_join_keys;
  ObSEArray<bool, 4> null_safe_info;
  bool dummy_is_coverd = false;
  ObLogPlan *plan = get_plan();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::split_expr_direction(ordering,
                                                          ordering_exprs,
                                                          ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else {
    const ObIArray<ObConflictDetector*> &conflict_detectors = plan->get_conflict_detectors();
    for (int64_t i = 0; OB_SUCC(ret) && !sort_match && i < conflict_detectors.count(); i++) {
      related_join_keys.reuse();
      other_join_keys.reuse();
      if (OB_ISNULL(conflict_detectors.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null detectors", K(ret));
      } else if (OB_FALSE_IT(join_info = &(conflict_detectors.at(i)->get_join_info()))) {
      } else if (!join_info->table_set_.overlap(get_tables()) ||
                  ObOptimizerUtil::find_item(used_conflict_detectors_, conflict_detectors.at(i))) {
        //do nothing
      } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(join_info->equal_join_conditions_,
                                                         get_tables(),
                                                         related_join_keys,
                                                         other_join_keys,
                                                         null_safe_info))) {
        LOG_WARN("failed to get equal keys", K(ret));
      } else if (OB_FAIL(extract_real_join_keys(related_join_keys))) {
        LOG_WARN("failed to extract real join keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(related_join_keys,
                                                              ordering_exprs,
                                                              output_equal_sets_,
                                                              get_output_const_exprs(),
                                                              sort_match))) {
        LOG_WARN("failed to fidn common prefix ordering", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_expr_overlap_index(const ObRawExpr* qual,
                                          const ObIArray<ObRawExpr*>& keys,
                                          bool &overlap)
{
  int ret = OB_SUCCESS;
  overlap = false;
  ObArray<ObRawExpr*> cur_vars;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(qual, cur_vars))) {
    LOG_WARN("extract_column_exprs error", K(ret));
  } else if (ObOptimizerUtil::overlap_exprs(cur_vars, keys)) {
    overlap = true;
  } else { /*do nothing*/ }
  return ret;
}

/* 拿到quals中涉及的 column的列的id 这个函数在抽取不出query range和 interesting order的情况下调用 */
int ObJoinOrder::extract_filter_column_ids(const ObIArray<ObRawExpr*> &quals,
                                           const bool is_data_table,
                                           const ObTableSchema &index_schema,
                                           ObIArray<uint64_t> &filter_column_ids)
{
  int ret = OB_SUCCESS;
  filter_column_ids.reset();
  if (quals.count() > 0) {
    ObArray<ObRawExpr *> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(quals, column_exprs))) {
      LOG_WARN("extract_column_expr error", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
        const ObColumnRefRawExpr *column = static_cast<ObColumnRefRawExpr *>(column_exprs.at(i));
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr should not be null", K(ret), K(i));
        } else {
          uint64_t column_id = column->get_column_id();
          bool has = false;
          if (is_data_table) {
            has = true;
          } else if (index_schema.is_spatial_index()) {
            const ObRowkeyInfo* rowkey_info = &index_schema.get_rowkey_info();
            const ObColumnSchemaV2 *column_schema = NULL;
            uint64_t index_column_id = OB_INVALID_ID;
            for (int col_idx = 0;  OB_SUCC(ret) && col_idx < rowkey_info->get_size(); ++col_idx) {
              if (OB_FAIL(rowkey_info->get_column_id(col_idx, index_column_id))) {
                LOG_WARN("Failed to get column id", K(ret));
              } else if (OB_ISNULL(column_schema = (index_schema.get_column_schema(index_column_id)))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get column schema", K(index_column_id), K(ret));
              } else if (column_schema->is_spatial_cellid_column()) {
                uint64_t geo_col_id = column_schema->get_geo_col_id();
                if (geo_col_id == column_id) {
                  has = true;
                  column_id = index_column_id;
                }
              }
            }
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

int ObJoinOrder::check_exprs_overlap_index(const ObIArray<ObRawExpr*>& quals,
                                           const ObIArray<ObRawExpr*>& keys,
                                           bool &match)
{
  LOG_TRACE("OPT:[CHECK MATCH]", K(keys));

  int ret = OB_SUCCESS;
  match = false;
  if (keys.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Index keys should not be empty", K(keys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !match && i < quals.count(); ++i) {
      ObRawExpr *expr = quals.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("qual expr should not be NULL", K(expr), K(i), K(ret));
      } else {
        LOG_TRACE("OPT:[CHECK MATCH]", K(*expr));
        if (OB_FAIL(check_expr_overlap_index(expr, keys, match))) {
          LOG_WARN("check_expr_overlap_index error", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_exprs_overlap_gis_index(const ObIArray<ObRawExpr*>& quals,
                                               const ObIArray<ObRawExpr*>& keys,
                                               bool &match)
{
  LOG_TRACE("OPT:[CHECK GIS MATCH]", K(keys));

  int ret = OB_SUCCESS;
  match = false;
  ObColumnRefRawExpr *cell_id = nullptr;
  if (keys.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Index keys should not be empty", K(keys.count()), K(ret));
  } else if (OB_ISNULL(cell_id = static_cast<ObColumnRefRawExpr *>(keys.at(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spatial Index keys should not be empty", K(keys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !match && i < quals.count(); ++i) {
      ObRawExpr *expr = quals.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("qual expr should not be NULL", K(expr), K(i), K(ret));
      } else {
        LOG_TRACE("OPT:[CHECK MATCH]", K(*expr));
        ObArray<ObRawExpr*> cur_vars;
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, cur_vars))) {
          LOG_WARN("extract_column_exprs error", K(ret));
        } else {
          for (int64_t j = 0; j < cur_vars.count() && !match; j++) {
            ObColumnRefRawExpr *ref = static_cast<ObColumnRefRawExpr *>(cur_vars.at(j));
            if (OB_NOT_NULL(ref) && ref->get_data_type() == ObGeometryType
                && cell_id->get_srs_id() == ref->get_column_id()) {
              // srs_id of cellid_column is column id of which column gis index is built on
              match = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_exprs_overlap_multivalue_index(
  const uint64_t table_id,
  const uint64_t index_table_id,
  const ObIArray<ObRawExpr*>& quals,
  const ObIArray<ObRawExpr*>& keys,
  bool &match)
{
  LOG_TRACE("OPT:[CHECK GIS MATCH]", K(keys));

  int ret = OB_SUCCESS;
  match = false;
  const ObDMLStmt *stmt = nullptr;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt or plan unexpected null", K(ret), K(get_plan()));
  } else if (keys.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Index keys should not be empty", K(keys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !match && i < quals.count(); ++i) {
      ObRawExpr *qual = quals.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("qual expr should not be NULL", K(qual), K(i), K(ret));
      } else if (qual->get_expr_type() != T_OP_BOOL)  {
      } else if (!qual->is_domain_json_expr()) {
      } else {
        for (int64_t k = 0; k < keys.count() && !match; k++) {
          ObColumnRefRawExpr *ref = static_cast<ObColumnRefRawExpr *>(keys.at(k));
          ObRawExpr *column_expr = nullptr;
          ObRawExpr *depend_expr = nullptr;
          if (!ref->is_multivalue_generated_column()) {
          } else if (OB_ISNULL(column_expr = stmt->get_column_expr_by_id(table_id, ref->get_column_id()))) {
            // quals not contains json multivalue index is normal, need not return error
          } else if (OB_ISNULL(depend_expr = (static_cast<ObColumnRefRawExpr*>(column_expr))->get_dependant_expr())) {
            // quals not contains json multivalue index is normal, need not return error
          } else {
            qual = ObRawExprUtils::skip_inner_added_expr(qual);
            ObExprEqualCheckContext equal_ctx;
            equal_ctx.override_const_compare_ = true;
            match = depend_expr->same_as(*qual, &equal_ctx);
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::extract_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                                 const ObIArray<ObRawExpr*> &predicates,
                                                 ObIArray<ObExprConstraint> &expr_constraints,
                                                 int64_t table_id,
                                                 ObQueryRangeProvider *&query_range,
                                                 int64_t index_id,
                                                 const ObTableSchema *index_schema,
                                                 int64_t &out_index_prefix)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  const ParamStore *params = NULL;
  ObSQLSessionInfo *session_info = NULL;
  bool enable_better_inlist = false;
  bool enable_index_prefix_cost = false;
  ObSEArray<ObRawExpr*, 4> range_predicates;
  ObSEArray<uint64_t, 4> total_range_counts;
  const LogTableHint *log_table_hint = NULL;
  out_index_prefix = -1;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(allocator_) ||
      OB_ISNULL(params = opt_ctx->get_params()) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info())||
      OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx),
        K(allocator_), K(params), K(ret));
  } else if (OB_FAIL(check_enable_better_inlist(table_id, enable_better_inlist, enable_index_prefix_cost))) {
    LOG_WARN("failed to check better inlist enabled", K(ret));
  } else if (enable_better_inlist &&
             OB_FAIL(get_candi_range_expr(range_columns,
                                          predicates,
                                          range_predicates))) {
    LOG_WARN("failed to get candi range expr", K(ret));
  } else if (!enable_better_inlist &&
             OB_FAIL(range_predicates.assign(predicates))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().get_index_prefix(table_id,
                                                                      index_id,
                                                                      out_index_prefix))) {
    LOG_WARN("failed to get index prefix", K(table_id), K(index_id), K(out_index_prefix), K(ret));
  } else if (opt_ctx->enable_new_query_range()) {
    void *ptr = allocator_->alloc(sizeof(ObPreRangeGraph));
    ObPreRangeGraph *pre_range_graph = NULL;
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pre range graph", K(ret));
    } else {
      pre_range_graph = new(ptr)ObPreRangeGraph(*allocator_);
      if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, range_predicates,
                                                                   opt_ctx->get_exec_ctx(),
                                                                   &expr_constraints,
                                                                   params, false, true,
                                                                   out_index_prefix,
                                                                   index_schema))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      } else if (FALSE_IT(log_table_hint = get_plan()->get_log_plan_hint().get_index_hint(table_id))) {
      } else if (NULL != log_table_hint && log_table_hint->is_use_index_hint()) {
        // do nothing
      } else if (!enable_index_prefix_cost) {
        // do nothing
      } else if (OB_FAIL(pre_range_graph->get_total_range_sizes(total_range_counts))) {
        LOG_WARN("failed to get total range sizes", K(ret));
      } else if (total_range_counts.empty()) {
        // do nothing
      } else if (OB_FAIL(get_better_index_prefix(pre_range_graph->get_range_exprs(),
                                                 pre_range_graph->get_range_expr_max_offsets(),
                                                 total_range_counts,
                                                 out_index_prefix))) {
        LOG_WARN("failed to get better index prefix", K(ret));
      } else if (-1 == out_index_prefix) {
        // do nothing
      } else if (OB_FALSE_IT(pre_range_graph->reset())) {
      } else if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, range_predicates,
                                                                          opt_ctx->get_exec_ctx(),
                                                                          &expr_constraints,
                                                                          params, false, true,
                                                                          out_index_prefix,
                                                                          index_schema))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = pre_range_graph;
    } else {
      if (NULL != pre_range_graph) {
        pre_range_graph->~ObPreRangeGraph();
        pre_range_graph = NULL;
      }
    }
  } else {
    void *tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange *tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new(tmp_ptr)ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(session_info);
      bool is_in_range_optimization_enabled = false;
      if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(opt_ctx->get_global_hint(),
                                                                    session_info,
                                                                    is_in_range_optimization_enabled))) {
        LOG_WARN("failed to check in range optimization enabled", K(ret));
      } else if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, range_predicates,
                                                          dtc_params, opt_ctx->get_exec_ctx(),
                                                          opt_ctx->get_query_ctx(),
                                                          &expr_constraints,
                                                          params, false, true,
                                                          is_in_range_optimization_enabled,
                                                          out_index_prefix))) {
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

int ObJoinOrder::check_enable_better_inlist(int64_t table_id,
                                            bool &enable_better_inlist,
                                            bool &enable_index_prefix_cost)
{
  int ret = OB_SUCCESS;
  bool enable = false;
  enable_better_inlist = false;
  enable_index_prefix_cost = false;
  ObOptimizerContext *opt_ctx = NULL;
  ObQueryCtx *query_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  OptTableMeta *table_meta = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info()) ||
      OB_ISNULL(query_ctx = get_plan()->get_optimizer_context().get_query_ctx()) ||
      OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(session_info), K(query_ctx), K(ret));
  } else if (!session_info->is_user_session()) {
    enable = false;
  } else if (OB_FALSE_IT(enable = opt_ctx->get_enable_better_inlist_costing())) {
  } else if (!enable) {
    //do nothing
  } else if (OB_ISNULL(table_meta=get_plan()->get_basic_table_metas().
                                  get_table_meta_by_table_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table meta", K(ret));
  } else if (table_meta->use_default_stat()) {
    enable = false;
  }

  if (OB_SUCC(ret) && enable) {
    if (ObSQLUtils::is_opt_feature_version_ge_425_or_435(
                get_plan()->get_stmt()->get_query_ctx()->optimizer_features_enable_version_)) {
      enable_index_prefix_cost = true;
    } else  {
      enable_better_inlist = true;
    }
  }
  return ret;
}

int ObJoinOrder::extract_geo_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                                     const ObIArray<ObRawExpr*> &predicates,
                                                     const ColumnIdInfoMap &column_schema_info,
                                                     ObQueryRangeProvider *&query_range)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  const ParamStore *params = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(allocator_) ||
      OB_ISNULL(params = opt_ctx->get_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx),
        K(allocator_), K(params), K(ret));
  } else if (opt_ctx->enable_new_query_range()) {
    void *ptr = allocator_->alloc(sizeof(ObPreRangeGraph));
    ObPreRangeGraph *pre_range_graph = NULL;
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pre range graph", K(ret));
    } else {
      pre_range_graph = new(ptr)ObPreRangeGraph(*allocator_);
      if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, predicates,
                                                                   opt_ctx->get_exec_ctx(),
                                                                   nullptr,
                                                                   params, false, true,
                                                                   -1, NULL, &column_schema_info))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = pre_range_graph;
    } else {
      if (NULL != pre_range_graph) {
        pre_range_graph->~ObPreRangeGraph();
        pre_range_graph = NULL;
      }
    }
  } else {
    void *tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange *tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new(tmp_ptr)ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
      // deep copy
      ColumnIdInfoMap::const_iterator iter = column_schema_info.begin();
      if (OB_FAIL(tmp_qr->init_columnId_map())) {
        LOG_WARN("Init column_info_map failed", K(ret));
      }
      while (OB_SUCC(ret) && iter != column_schema_info.end()) {
        if (OB_FAIL(tmp_qr->set_columnId_map(iter->first, iter->second))) {
          LOG_WARN("set column info map failed", K(ret), K(iter->first));
        }
        iter++;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, predicates,
                                                                 dtc_params, opt_ctx->get_exec_ctx(),
                                                                 opt_ctx->get_query_ctx(),
                                                                 NULL, params))) {
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

int ObJoinOrder::extract_multivalue_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                                     const ObIArray<ObRawExpr*> &predicates,
                                                     ObQueryRangeProvider *&query_range)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  const ParamStore *params = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(allocator_) ||
      OB_ISNULL(params = opt_ctx->get_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx),
        K(allocator_), K(params), K(ret));
  } else if (opt_ctx->enable_new_query_range()) {
    void *ptr = allocator_->alloc(sizeof(ObPreRangeGraph));
    ObPreRangeGraph *pre_range_graph = NULL;
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pre range graph", K(ret));
    } else {
      pre_range_graph = new(ptr)ObPreRangeGraph(*allocator_);
      if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, predicates,
                                                                   opt_ctx->get_exec_ctx(),
                                                                   nullptr,
                                                                   params, false, true,
                                                                   -1, NULL))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = pre_range_graph;
    } else {
      if (NULL != pre_range_graph) {
        pre_range_graph->~ObPreRangeGraph();
        pre_range_graph = NULL;
      }
    }
  } else {
    void *tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange *tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new(tmp_ptr)ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
      bool is_in_range_optimization_enabled = false;
      if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(opt_ctx->get_global_hint(),
                                                                    opt_ctx->get_session_info(),
                                                                    is_in_range_optimization_enabled))) {
        LOG_WARN("failed to check in range optimization enabled", K(ret));
      } else if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, predicates,
                                                                 dtc_params, opt_ctx->get_exec_ctx(),
                                                                 opt_ctx->get_query_ctx(),
                                                                 NULL, params, false, true,
                                                                 is_in_range_optimization_enabled))) {
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

// If there is a fulltext search on main tabe,
// range for fts index is only used for storage estimation, not actually used on execution stage.
// If it's a direct query on inverted index table,
// range for fts index follows normal usage.
int ObJoinOrder::extract_fts_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                                     const ObIArray<ObRawExpr*> &predicates,
                                                     const ObTableSchema *table_schema,
                                                     const ObTableSchema *index_schema,
                                                     PathHelper &helper,
                                                     ObQueryRangeProvider *&query_range)
{
  int ret = OB_SUCCESS;
  bool direct_query_on_index = false;
  const ParamStore *params = NULL;
  if (OB_ISNULL(OPT_CTX.get_exec_ctx()) || OB_ISNULL(allocator_) || OB_ISNULL(table_schema) ||
      OB_ISNULL(index_schema) || OB_ISNULL(OPT_CTX.get_exec_ctx()->get_expr_factory()) ||
      OB_ISNULL(params = OPT_CTX.get_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(OPT_CTX.get_exec_ctx()), K(allocator_), K(ret));
  } else if (OB_FALSE_IT(direct_query_on_index = index_schema->get_table_id() == table_schema->get_table_id())) {
  } else if (!direct_query_on_index) {
    ObArray<ObMatchFunRawExpr*> match_exprs;
    ObArray<ObRawExpr*> match_filters;
    const MatchExprInfo *match_expr_info = NULL;
    if (OB_FAIL(extract_scan_match_expr_candidates(predicates, match_exprs, match_filters))) {
      LOG_WARN("failed to extract match expr candidates", K(ret));
    } else if (OB_FAIL(find_least_selective_expr_on_index(match_exprs,
                                                         helper.match_expr_infos_,
                                                         index_schema->get_table_id(),
                                                         match_expr_info))) {
      LOG_WARN("failed to find most selective expr on index", K(ret));
    } else if (OB_ISNULL(match_expr_info) || OB_ISNULL(match_expr_info->query_range_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(match_expr_info), K(ret));
    } else {
      query_range = match_expr_info->query_range_;
    }
  } else if (OPT_CTX.enable_new_query_range()) {
    void *ptr = allocator_->alloc(sizeof(ObPreRangeGraph));
    ObPreRangeGraph *pre_range_graph = NULL;
     if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pre range graph", K(ret));
    } else {
      pre_range_graph = new(ptr)ObPreRangeGraph(*allocator_);
      if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, predicates,
                                                                   OPT_CTX.get_exec_ctx(),
                                                                   nullptr,
                                                                   params))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = pre_range_graph;
    } else {
      if (NULL != pre_range_graph) {
        pre_range_graph->~ObPreRangeGraph();
        pre_range_graph = NULL;
      }
    }
  } else {
    void *tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange *tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new(tmp_ptr)ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(OPT_CTX.get_exec_ctx()->get_my_session());
      if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, predicates,
                                                          dtc_params, OPT_CTX.get_exec_ctx(),
                                                          OPT_CTX.get_query_ctx(),
                                                          NULL, params))) {
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

int ObJoinOrder::get_candi_range_expr(const ObIArray<ColumnItem> &range_columns,
                                      const ObIArray<ObRawExpr*> &predicates,
                                      ObIArray<ObRawExpr*> &range_predicates)
{
  int ret = OB_SUCCESS;
  double min_cost = 0;
  double cost = 0;
  bool has_in_pred = false;
  int64_t range_count = 1;
  ObSEArray<ObRawExpr*, 4> range_exprs;
  ObSEArray<ObRawExpr*, 4> ignore_predicates;
  ObSEArray<CandiRangeExprs*, 4> sorted_predicates;
  LOG_TRACE("check index", K(range_columns));
  if (OB_FAIL(sort_predicate_by_index_column(range_columns,
                                              predicates,
                                              sorted_predicates,
                                              has_in_pred))) {
    LOG_WARN("failed to sort predicate by index column", K(ret));
  } else if (!has_in_pred) {
    //do nothing
    //calculate full index scan cost
  } else if (OB_FAIL(calculate_range_expr_cost(sorted_predicates,
                                                range_exprs,
                                                range_columns.count(),
                                                range_count,
                                                min_cost))) {
    LOG_WARN("failed to calculate range expr cost", K(ret));
  }
  if (OB_SUCC(ret) && has_in_pred) {
    auto compare_op = [](CandiRangeExprs *lhs, CandiRangeExprs *rhs)
                      { bool b_ret = false;
                        if (NULL != lhs && NULL != rhs)
                        { b_ret = lhs->index_ < rhs->index_; }
                        return b_ret; };
    lib::ob_sort(sorted_predicates.begin(), sorted_predicates.end(), compare_op);
    LOG_TRACE("sort predicates and calc cost", K(min_cost), K(sorted_predicates));
  }
  //for each candi range expr, check scan cost
  for (int64_t i = 0; OB_SUCC(ret) && has_in_pred && i < sorted_predicates.count(); ++i) {
    CandiRangeExprs *candi_exprs = sorted_predicates.at(i);
    ObRawExpr *min_cost_in_expr = NULL;
    uint64_t min_cost_range_count = INT64_MAX;
    if (OB_ISNULL(candi_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null range exprs", K(ret));
    } else if (!candi_exprs->eq_exprs_.empty()) {
      //has equal condition, ignore all in exprs
      if (OB_FAIL(append(range_exprs, candi_exprs->eq_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(calculate_range_expr_cost(sorted_predicates,
                                                    range_exprs,
                                                    range_columns.count(),
                                                    range_count,
                                                    min_cost))) {
        LOG_WARN("failed to calculate range expr cost", K(ret));
      } else if (OB_FAIL(append(ignore_predicates, candi_exprs->in_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    } else {
      //choose less in list expr
      for (int64_t j = 0; OB_SUCC(ret) && j < candi_exprs->in_exprs_.count(); ++j) {
        ObRawExpr* in_expr = candi_exprs->in_exprs_.at(j);
        ObRawExpr* row_expr = NULL;
        if (OB_ISNULL(in_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null in expr", K(ret));
        } else if (2 != in_expr->get_param_count() ||
                    OB_ISNULL(row_expr=in_expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (row_expr->get_param_count() * range_count < min_cost_range_count) {
          min_cost_range_count = row_expr->get_param_count() * range_count;
          if (NULL == min_cost_in_expr) {
            min_cost_in_expr = in_expr;
          } else if (OB_FAIL(ignore_predicates.push_back(min_cost_in_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else {
            min_cost_in_expr = in_expr;
          }
        } else if (OB_FAIL(ignore_predicates.push_back(in_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && NULL != min_cost_in_expr) {
        //check cost
        if (OB_FAIL(range_exprs.push_back(min_cost_in_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(calculate_range_expr_cost(sorted_predicates,
                                                      range_exprs,
                                                      range_columns.count(),
                                                      min_cost_range_count,
                                                      cost))) {
          LOG_WARN("failed to calculate range expr cost", K(ret));
        } else if (cost >= min_cost && min_cost_range_count > 500) {
          //increase cost, ignore in expr
          range_exprs.pop_back();
          if (OB_FAIL(ignore_predicates.push_back(min_cost_in_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        } else {
          //reduce cost, use in expr
          range_count = min_cost_range_count;
          min_cost = cost;
        }
      }
    }
  }
  //remove ignore in expr
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    if (ObOptimizerUtil::find_item(ignore_predicates, predicates.at(i))) {
      //do nothing
    } else if (OB_FAIL(range_predicates.push_back(predicates.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  //destroy candi range exprs
  for (int64_t i = 0; i < sorted_predicates.count(); ++i) {
    if (NULL != sorted_predicates.at(i)) {
      sorted_predicates.at(i)->~CandiRangeExprs();
      sorted_predicates.at(i) = NULL;
    }
  }
  LOG_TRACE("used predicates calc query range:", K(range_predicates));
  return ret;
}

int ObJoinOrder::calculate_range_expr_cost(ObIArray<CandiRangeExprs*> &sorted_predicates,
                                           ObIArray<ObRawExpr*> &range_exprs,
                                           int64_t range_column_count,
                                           int64_t range_count,
                                           double &cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> filters;
  if (OB_FAIL(get_range_filter(sorted_predicates,
                               range_exprs,
                               filters))) {
    LOG_WARN("failed to get range filter", K(ret));
  } else if (OB_FAIL(calculate_range_and_filter_expr_cost(range_exprs,
                                                          filters,
                                                          range_column_count,
                                                          range_count,
                                                          cost))) {
    LOG_WARN("failed to calculate range expr cost", K(ret));
  }
  return ret;
}

int ObJoinOrder::calculate_range_and_filter_expr_cost(ObIArray<ObRawExpr*> &range_exprs,
                                                      ObIArray<ObRawExpr*> &filter_exprs,
                                                      int64_t range_column_count,
                                                      int64_t range_count,
                                                      double &cost)
{
  int ret = OB_SUCCESS;
  double range_sel = 1;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             range_exprs,
                                                             range_sel,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (OB_FAIL(ObOptEstCost::calc_range_cost(table_meta_info_,
                                                   filter_exprs,
                                                   range_column_count,
                                                   range_count,
                                                   range_sel,
                                                   cost,
                                                   get_plan()->get_optimizer_context()))) {
    LOG_WARN("failed to estimate range scan cost", K(ret));
  } else {
    LOG_TRACE("query range cost:", K(range_column_count), K(range_count), K(range_sel), K(cost));
    LOG_TRACE("candi range exprs:", K(range_exprs));
  }
  return ret;
}

int ObJoinOrder::sort_predicate_by_index_column(const ObIArray<ColumnItem> &range_columns,
                                                const ObIArray<ObRawExpr*> &predicates,
                                                ObIArray<CandiRangeExprs*> &sort_exprs,
                                                bool &has_in_pred)
{
  int ret = OB_SUCCESS;
  has_in_pred = false;
  int64_t column_id = 0;
  bool is_in_expr = false;
  bool is_valid = false;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null allocator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
    ObRawExpr* expr = predicates.at(i);
    bool find = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(is_eq_or_in_range_expr(expr,
                                              column_id,
                                              is_in_expr,
                                              is_valid))) {
      LOG_WARN("failed check is valid range expr", K(ret));
    } else if (!is_valid) {
      find = true;
    } else {
      has_in_pred |= is_in_expr;
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < sort_exprs.count(); ++j) {
      CandiRangeExprs *candi_exprs = sort_exprs.at(j);
      if (OB_ISNULL(candi_exprs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null range exprs", K(ret));
      } else if (column_id == candi_exprs->column_id_) {
        find = true;
        if (is_in_expr &&
            OB_FAIL(candi_exprs->in_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (!is_in_expr &&
                   OB_FAIL(candi_exprs->eq_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      int64_t index = OB_INVALID_INDEX;
      for (int64_t j = 0; OB_INVALID_INDEX == index && j < range_columns.count(); ++j) {
        if (column_id == range_columns.at(j).column_id_) {
          index = j;
        }
      }
      if (OB_INVALID_INDEX != index) {
        CandiRangeExprs *candi_exprs = NULL;
        if (OB_ISNULL(candi_exprs = static_cast<CandiRangeExprs*>(
                      allocator_->alloc(sizeof(CandiRangeExprs))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create outer join info", K(ret));
        } else {
          candi_exprs = new (candi_exprs) CandiRangeExprs();
          candi_exprs->index_ = index;
          candi_exprs->column_id_ = column_id;
          if (is_in_expr &&
              OB_FAIL(candi_exprs->in_exprs_.push_back(expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (!is_in_expr &&
                    OB_FAIL(candi_exprs->eq_exprs_.push_back(expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(sort_exprs.push_back(candi_exprs))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::is_eq_or_in_range_expr(ObRawExpr* expr,
                                        int64_t &column_id,
                                        bool &is_in_expr,
                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  column_id = OB_INVALID_ID;
  is_in_expr = false;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (T_OP_EQ == expr->get_expr_type() ||
             T_OP_NSEQ == expr->get_expr_type()) {
    ObRawExpr* l_expr = NULL;
    ObRawExpr* r_expr = NULL;
    if (expr->get_param_count() < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param count", K(ret));
    } else if (OB_ISNULL(l_expr=expr->get_param_expr(0)) ||
               OB_ISNULL(r_expr=expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (T_OP_ROW == l_expr->get_expr_type()) {
      //do nothing
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get lossless cast expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_expr, r_expr))) {
      LOG_WARN("failed to get lossless cast expr", K(ret));
    } else if (l_expr->has_flag(IS_COLUMN) && r_expr->is_const_expr()) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(l_expr);
      column_id = col_expr->get_column_id();
      is_in_expr = false;
      is_valid = true;
    } else if (l_expr->is_const_expr() && r_expr->has_flag(IS_COLUMN)) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(r_expr);
      column_id = col_expr->get_column_id();
      is_in_expr = false;
      is_valid = true;
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    ObRawExpr* l_expr = NULL;
    if (expr->get_param_count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param count", K(ret));
    } else if (OB_ISNULL(l_expr=expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get lossless cast expr", K(ret));
    } else if (l_expr->has_flag(IS_COLUMN)) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(l_expr);
      column_id = col_expr->get_column_id();
      is_in_expr = true;
      is_valid = true;
    }
  }
  return ret;
}

int ObJoinOrder::get_range_filter(ObIArray<CandiRangeExprs*> &sort_exprs,
                                  ObIArray<ObRawExpr*> &range_exprs,
                                  ObIArray<ObRawExpr*> &filters)
{
  int ret = OB_SUCCESS;
  filters.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
    CandiRangeExprs *candi_exprs = sort_exprs.at(i);
    if (OB_ISNULL(candi_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null range exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < candi_exprs->eq_exprs_.count(); ++j) {
      ObRawExpr *expr = candi_exprs->eq_exprs_.at(j);
      if (ObOptimizerUtil::find_item(range_exprs, expr)) {
        //do nothing
      } else if (OB_FAIL(filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < candi_exprs->in_exprs_.count(); ++j) {
      ObRawExpr *expr = candi_exprs->in_exprs_.at(j);
      if (ObOptimizerUtil::find_item(range_exprs, expr)) {
        //do nothing
      } else if (OB_FAIL(filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::estimate_size_for_base_table(PathHelper &helper,
                                              ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_)) ||
      OB_UNLIKELY(type_ != ACCESS) || OB_ISNULL(helper.table_opt_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null or path type",
                K(ret), K(get_plan()), K(table_item), K(type_), K(stmt));
  } else if (OB_FAIL(fill_path_index_meta_info(access_paths))) {
    LOG_WARN("failed to fill path index meta info", K(ret));
  } else if (OB_FAIL(estimate_rowcount_for_access_path(access_paths,
                                                  helper.is_inner_path_,
                                                  helper.filters_,
                                                  helper.est_method_))) {
    LOG_WARN("failed to estimate and add access path", K(ret));
  } else {
    LOG_TRACE("estimate rows for base table", K(output_rows_),
                K(get_plan()->get_basic_table_metas()), K(output_row_size_));
  }
  return ret;
}

int ObJoinOrder::estimate_size_and_width_for_join(const ObJoinOrder* left_tree,
                                                  const ObJoinOrder* right_tree,
                                                  const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  double new_rows = 0.0;
  double sel = 1.0;
  EqualSets equal_sets;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (OB_FAIL(est_join_width())) {
    LOG_WARN("failed to estimate join width", K(ret));
  } else if (OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_5, COMPAT_VERSION_4_3_0,
                                                               COMPAT_VERSION_4_3_5)) {
    // do nothing
  } else if (OB_FAIL(append(equal_sets, left_tree->get_output_equal_sets())) ||
             OB_FAIL(append(equal_sets, right_tree->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else if (OB_FAIL(calc_join_output_rows(get_plan(),
                                           left_tree->get_tables(),
                                           right_tree->get_tables(),
                                           left_tree->get_output_rows(),
                                           right_tree->get_output_rows(),
                                           *join_info_, new_rows,
                                           sel, equal_sets))) {
    LOG_WARN("failed to calc join output rows", K(ret));
  } else {
    set_output_rows(new_rows);
    current_join_output_rows_ = new_rows;
    LOG_TRACE("estimate rows for join path", K(output_rows_), K(get_plan()->get_update_table_metas()));
  }
  return ret;
}

int ObJoinOrder::estimate_size_and_width_for_subquery(uint64_t table_id,
                                                      ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  double selectivity = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (OB_FAIL(init_est_sel_info_for_subquery(table_id, root))) {
    LOG_WARN("failed to init est sel info for subplan scan", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_table(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            stmt->get_column_items(),
                                                            table_id_,
                                                            output_row_size_))) {
    LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
  } else if (FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(NULL, root->get_card()))) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             get_restrict_infos(),
                                                             selectivity,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
  } else {
    set_output_rows(root->get_card() * selectivity);
    if (OB_FAIL(ObOptSelectivity::update_table_meta_info(get_plan()->get_basic_table_metas(),
                                                         get_plan()->get_update_table_metas(),
                                                         get_plan()->get_selectivity_ctx(),
                                                         table_id,
                                                         output_rows_,
                                                         get_restrict_infos(),
                                                         get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to update table meta info", K(ret));
    }
    LOG_TRACE("estimate rows for subquery", K(output_rows_), K(get_plan()->get_basic_table_metas()));
  }
  return ret;
}

int ObJoinOrder::est_join_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(get_join_output_exprs(output_exprs))) {
    LOG_WARN("failed to compute join output column exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output join column exprs", K(ret));
  } else {
    set_output_row_size(width);
    LOG_TRACE("est_width for join", K(output_exprs), K(width));
  }
  return ret;
}

int ObJoinOrder::get_join_output_exprs(ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObSEArray<ObRawExpr*, 16> temp_exprs;
  ObSEArray<ObRawExpr*, 16> extracted_column_exprs;
  ObSEArray<ObRawExpr*, 16> excluded_condition_exprs;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret));
  } else if (OB_FAIL(get_excluded_condition_exprs(excluded_condition_exprs))) {
    LOG_WARN("failed to get excluded column exprs");
  } else if (OB_FAIL(ObOptimizerUtil::except_exprs(plan->get_condition_exprs_for_width_est(),
                                                   excluded_condition_exprs,
                                                   temp_exprs))) {
    LOG_WARN("failed to except excluded exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(temp_exprs, plan->get_groupby_rollup_exprs_for_width_est()))) {
    LOG_WARN("failed to append into width candi exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(temp_exprs, plan->get_having_exprs_for_width_est()))) {
    LOG_WARN("failed to append into width candi exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(temp_exprs, plan->get_winfunc_exprs_for_width_est()))) {
    LOG_WARN("failed to append into width candi exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(temp_exprs, plan->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to append into width candi exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(temp_exprs, plan->get_orderby_exprs_for_width_est()))) {
    LOG_WARN("failed to append into width candi exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(temp_exprs,
                                                          get_tables(),
                                                          extracted_column_exprs))) {
    LOG_WARN("failed to extract expr", K(ret));
  } else if (OB_FAIL(append_array_no_dup(output_exprs, extracted_column_exprs))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObJoinOrder::get_excluded_condition_exprs(ObIArray<ObRawExpr *> &excluded_conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < used_conflict_detectors_.count(); ++i) {
    JoinInfo &join_info = used_conflict_detectors_.at(i)->get_join_info();
    if (OB_FAIL(append_array_no_dup(excluded_conditions,
                                    join_info.on_conditions_))) {
      LOG_WARN("failed to append on condition exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(excluded_conditions,
                                           join_info.where_conditions_))) {
      LOG_WARN("failed to append where condition exprs", K(ret));
    }
  }
  return ret;
}

double ObJoinOrder::calc_single_parallel_rows(double rows, int64_t parallel)
{
  double ret = rows;
  if (parallel >= 1) {
    ret = rows / parallel;
  }
  ret = ret < 1 ? 1 : ret;
  return ret;
}

int ObJoinOrder::compute_const_exprs_for_subquery(uint64_t table_id,
                                                  ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(root) || OB_ISNULL(root->get_stmt()) || OB_ISNULL(root->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root), K(ret));
  } else if (OB_UNLIKELY(!root->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(ret));
  } else {
    const ObDMLStmt *parent_stmt = get_plan()->get_stmt();
    const ObSelectStmt *child_stmt = static_cast<const ObSelectStmt*>(root->get_stmt());
    if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(
                                        get_plan()->get_optimizer_context().get_expr_factory(),
                                        root->get_output_equal_sets(),
                                        table_id,
                                        *parent_stmt,
                                        *child_stmt,
                                        true,
                                        root->get_output_const_exprs(),
                                        output_const_exprs_))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_subplan_const_column(*parent_stmt,
                                                                 table_id,
                                                                 *child_stmt,
                                                                 root->get_plan()->get_onetime_query_refs(),
                                                                 output_const_exprs_))) {
      LOG_WARN("failed to get subplan const column expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, output_const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObJoinOrder::compute_const_exprs_for_join(const ObJoinOrder* left_tree,
                                              const ObJoinOrder* right_tree,
                                              const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (INNER_JOIN == join_type) {
    if (OB_FAIL(append(output_const_exprs_, left_tree->get_output_const_exprs()))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(append(output_const_exprs_, right_tree->get_output_const_exprs()))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(join_info_->where_conditions_,
                                                            output_const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else {/*do nothing*/}
  } else if (IS_OUTER_JOIN(join_type)) {
    if (OB_ISNULL(join_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(join_info_), K(ret));
    } else if (LEFT_OUTER_JOIN == join_type &&
               OB_FAIL(append(output_const_exprs_, left_tree->get_output_const_exprs()))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (RIGHT_OUTER_JOIN == join_type &&
               OB_FAIL(append(output_const_exprs_, right_tree->get_output_const_exprs()))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(join_info_->where_conditions_,
                                                            output_const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    }
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
      if (OB_FAIL(append(output_const_exprs_, left_tree->get_output_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      }
    } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
      if (OB_FAIL(append(output_const_exprs_, right_tree->get_output_const_exprs()))) {
        LOG_WARN("failed to append const exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && IS_SEMI_JOIN(join_type)
        && OB_FAIL(ObOptimizerUtil::compute_const_exprs(join_info_->where_conditions_,
                                                        output_const_exprs_))) {
      LOG_WARN("failed to compute const exprs for semi join", K(ret));
    }
  } else if (CONNECT_BY_JOIN == join_type) {
    if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, output_const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else {/*do nothing*/}
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid join type", K(join_type), K(ret));
  }
  return ret;
}

int ObJoinOrder::compute_equal_set_for_join(const ObJoinOrder* left_tree,
                                            const ObJoinOrder* right_tree,
                                            const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(ret));
  } else if (INNER_JOIN == join_type) {
    EqualSets input_equal_sets;
    if (OB_FAIL(append(input_equal_sets, left_tree->output_equal_sets_)) ||
        OB_FAIL(append(input_equal_sets, right_tree->output_equal_sets_))) {
      LOG_WARN("failed to init input equal sets", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                          join_info_->where_conditions_,
                                                          input_equal_sets,
                                                          output_equal_sets_))) {
      LOG_WARN("failed to compute equal sets for inner join", K(ret));
    } else { /*do nothing*/ }
  } else if (IS_OUTER_JOIN(join_type)) {
    if (LEFT_OUTER_JOIN == join_type || RIGHT_OUTER_JOIN == join_type) {
      const EqualSets &child_eset = (LEFT_OUTER_JOIN == join_type) ?
            left_tree->output_equal_sets_ : right_tree->output_equal_sets_;
      if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                     join_info_->where_conditions_,
                                                     child_eset,
                                                     output_equal_sets_))) {
        LOG_WARN("failed to compute ordering equal set for left/right outer join", K(ret));
      }
    } else if (FULL_OUTER_JOIN == join_type) {
      if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                     join_info_->where_conditions_,
                                                     output_equal_sets_))) {
        LOG_WARN("failed to compute ordering equal set for full outer join", K(ret));
      }
    }
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    const EqualSets &child_eset = IS_LEFT_SEMI_ANTI_JOIN(join_type)
                                  ? left_tree->output_equal_sets_
                                  : right_tree->output_equal_sets_;
    if (IS_ANTI_JOIN(join_type)) {
      if (OB_FAIL(append(output_equal_sets_, child_eset))) {
        LOG_WARN("failed to append equal set for left/right anti join", K(ret));
      }
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                          join_info_->where_conditions_,
                                                          child_eset,
                                                          output_equal_sets_))) {
      // semi right table exprs may exists in sharding info,
      //  need compute equal sets use semi condition
      LOG_WARN("failed to compute ordering equal set for left/right semi join", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObJoinOrder::compute_equal_set_for_subquery(uint64_t table_id, ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(root) || OB_ISNULL(root->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(root));
  } else if (OB_UNLIKELY(!root->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(ret));
  } else {
    EqualSets input_equal_sets;
    ObSEArray<ObRawExpr*, 4> preds;
    const ObDMLStmt *parent_stmt = get_plan()->get_stmt();
    const ObSelectStmt *child_stmt = static_cast<const ObSelectStmt*>(root->get_stmt());
    if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_equal_sets(
                allocator_,
                get_plan()->get_optimizer_context().get_expr_factory(),
                table_id,
                *parent_stmt,
                *child_stmt,
                root->get_output_equal_sets(),
                input_equal_sets))) {
      LOG_WARN("failed to generate subplan scan expr", K(ret));
    } else if (OB_FAIL(preds.assign(restrict_info_set_))) {
      LOG_WARN("failed to assign exprs", K(ret));
      // todo link.zt, the generated predicates is only used for deducing equal sets
      // i think there is a better implementation.
      // we do not need to build equal predicates here
    } else if (OB_FAIL(generate_const_predicates_from_view(parent_stmt,
                                                           child_stmt,
                                                           table_id,
                                                           preds))) {
      LOG_WARN("failed to generate const pred from view", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                          preds,
                                                          input_equal_sets,
                                                          output_equal_sets_))) {
      LOG_WARN("failed to compute equal set for subplan scan", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObJoinOrder::generate_const_predicates_from_view(const ObDMLStmt *stmt,
                                                    const ObSelectStmt *child_stmt,
                                                    uint64_t table_id,
                                                    ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sel_expr = NULL;
  ObRawExpr *column_expr = NULL;
  ObRawExpr *equal_expr = NULL;
  ObLogPlan *plan = get_plan();
  if (OB_ISNULL(child_stmt) || OB_ISNULL(plan) || OB_ISNULL(stmt) ||
      OB_ISNULL(plan->get_optimizer_context().get_params())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_select_item_size(); ++i) {
    int64_t idx = i;
    bool is_not_null = false;
    ObNotNullContext not_null_ctx(plan->get_optimizer_context().get_exec_ctx(),
                                  &plan->get_allocator(),
                                  child_stmt);
    if (OB_ISNULL(sel_expr = child_stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(idx), K(sel_expr));
    } else if (!sel_expr->is_const_expr() || sel_expr->get_result_type().is_lob()
                || ob_is_xml_sql_type(sel_expr->get_result_type().get_type(), sel_expr->get_result_type().get_subschema_id())
                || ob_is_geometry(sel_expr->get_result_type().get_type())
                || ob_is_roaringbitmap(sel_expr->get_result_type().get_type())) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx,
                                                          sel_expr,
                                                          is_not_null,
                                                          NULL))) {
      LOG_WARN("failed to check expr not null", K(ret));
    } else if (!is_not_null) {
      // column = null is invalid, do nothing
    } else if (OB_FALSE_IT(column_expr = stmt->get_column_expr_by_id(table_id, OB_APP_MIN_COLUMN_ID + idx))) {
    } else if (NULL == column_expr) {
      //not used,do nothing
    } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(plan->get_optimizer_context().get_expr_factory(),
                                                        plan->get_optimizer_context().get_session_info(),
                                                        column_expr,
                                                        sel_expr,
                                                        equal_expr))) {
      LOG_WARN("failed to create equal exprs", K(ret));
    } else if (OB_ISNULL(equal_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal expr is null", K(ret));
    } else if (OB_FAIL(equal_expr->formalize(plan->get_optimizer_context().get_session_info()))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(equal_expr->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else if (OB_FAIL(preds.push_back(equal_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::convert_subplan_scan_order_item(ObLogPlan &plan,
                                                 ObLogicalOperator &subplan_root,
                                                 const uint64_t table_id,
                                                 ObIArray<OrderItem> &output_order)
{
  int ret = OB_SUCCESS;
  const EqualSets &equal_sets = subplan_root.get_output_equal_sets();
  const ObIArray<ObRawExpr *> &const_exprs = subplan_root.get_output_const_exprs();
  const ObSelectStmt *child_stmt = static_cast<const ObSelectStmt *>(subplan_root.get_stmt());
  const ObIArray<OrderItem> &input_order = subplan_root.get_op_ordering();
  ObRawExprCopier copier(plan.get_optimizer_context().get_expr_factory());
  if (OB_ISNULL(subplan_root.get_stmt()) ||
      !OB_UNLIKELY(subplan_root.get_stmt()->is_select_stmt()) ||
      OB_ISNULL(plan.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_order.count(); ++i) {
    ObRawExpr *temp_expr = NULL;
    if (OB_ISNULL(input_order.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(input_order.at(i).expr_), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(copier,
                                                                  equal_sets,
                                                                  table_id,
                                                                  *plan.get_stmt(),
                                                                  *child_stmt,
                                                                  input_order.at(i).expr_,
                                                                  temp_expr))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (NULL == temp_expr) {
      bool is_const = false;
      if (OB_FAIL(ObOptimizerUtil::is_const_expr(input_order.at(i).expr_,
                                                 equal_sets,
                                                 const_exprs,
                                                 is_const))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (is_const) {
        /*do nothing*/
      } else {
        break;
      }
    } else if (OB_FAIL(output_order.push_back(OrderItem(temp_expr,
                                                        input_order.at(i).order_type_)))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else { /*do nothing*/}
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("subplan scan order item", K(output_order));
  }
  return ret;


  return ret;
}

int ObJoinOrder::convert_subplan_scan_sharding_info(ObLogPlan &plan,
                                                    ObLogicalOperator &subplan_root,
                                                    const uint64_t table_id,
                                                    ObShardingInfo *&output_strong_sharding,
                                                    ObIArray<ObShardingInfo*> &output_weak_sharding,
                                                    bool &is_inherited_sharding)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext &opt_ctx = plan.get_optimizer_context();
  ObShardingInfo *input_strong_sharding = subplan_root.get_strong_sharding();
  const ObIArray<ObShardingInfo*> &input_weak_sharding = subplan_root.get_weak_sharding();
  is_inherited_sharding = false;
  if (NULL != input_strong_sharding &&
      OB_FAIL(convert_subplan_scan_sharding_info(plan,
                                                 subplan_root,
                                                 table_id,
                                                 true,
                                                 input_strong_sharding,
                                                 output_strong_sharding,
                                                 is_inherited_sharding))) {
    LOG_WARN("failed to convert sharding info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_weak_sharding.count(); i++) {
      ObShardingInfo *temp_sharding = NULL;
      bool is_weak_sharding_inherited = false;
      if (OB_ISNULL(input_weak_sharding.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(convert_subplan_scan_sharding_info(plan,
                                                            subplan_root,
                                                            table_id,
                                                            false,
                                                            input_weak_sharding.at(i),
                                                            temp_sharding,
                                                            is_weak_sharding_inherited))) {
        LOG_WARN("failed to convert sharding info", K(ret));
      } else if (NULL != temp_sharding && OB_FAIL(output_weak_sharding.push_back(temp_sharding))) {
        LOG_WARN("failed to push back sharding", K(ret));
      } else {
        is_inherited_sharding |= is_weak_sharding_inherited;
      }
    }
    if (OB_SUCC(ret) && NULL == output_strong_sharding && output_weak_sharding.empty()) {
      output_strong_sharding = opt_ctx.get_distributed_sharding();
      is_inherited_sharding = false;
    }
  }
  return ret;
}

int ObJoinOrder::convert_subplan_scan_sharding_info(ObLogPlan &plan,
                                                    ObLogicalOperator &subplan_root,
                                                    const uint64_t table_id,
                                                    bool is_strong,
                                                    ObShardingInfo *input_sharding,
                                                    ObShardingInfo *&output_sharding,
                                                    bool &is_inherited_sharding)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> part_exprs;
  ObSEArray<ObRawExpr*, 8> subpart_exprs;
  ObSEArray<ObRawExpr*, 8> part_func;
  const ObSelectStmt *child_stmt = static_cast<const ObSelectStmt *>(subplan_root.get_stmt());
  ObIAllocator &allocator = plan.get_allocator();
  ObRawExprFactory &expr_factory = plan.get_optimizer_context().get_expr_factory();
  output_sharding = NULL;
  is_inherited_sharding = false;
  if (OB_ISNULL(input_sharding) || OB_ISNULL(plan.get_stmt()) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(input_sharding), K(ret));
  } else if (input_sharding->is_single() || input_sharding->is_distributed_without_partitioning()) {
    output_sharding = input_sharding;
    is_inherited_sharding = true;
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(expr_factory,
                                                                subplan_root.get_output_equal_sets(),
                                                                table_id,
                                                                *plan.get_stmt(),
                                                                *child_stmt,
                                                                false,
                                                                input_sharding->get_partition_keys(),
                                                                part_exprs))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(expr_factory,
                                                                subplan_root.get_output_equal_sets(),
                                                                table_id,
                                                                *plan.get_stmt(),
                                                                *child_stmt,
                                                                false,
                                                                input_sharding->get_sub_partition_keys(),
                                                                subpart_exprs))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(expr_factory,
                                                                subplan_root.get_output_equal_sets(),
                                                                table_id,
                                                                *plan.get_stmt(),
                                                                *child_stmt,
                                                                false,
                                                                input_sharding->get_partition_func(),
                                                                part_func))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else {
    bool is_converted = input_sharding->get_partition_keys().count() == part_exprs.count() &&
        input_sharding->get_sub_partition_keys().count() == subpart_exprs.count() &&
        input_sharding->get_partition_func().count() == part_func.count();
    if (!is_strong && !is_converted) {
      /*do nothing*/
    } else if (is_strong && !is_converted) {
      output_sharding = plan.get_optimizer_context().get_distributed_sharding();
      is_inherited_sharding = false;
    } else {
      ObShardingInfo *temp_sharding = NULL;
      if (OB_ISNULL(temp_sharding =
                static_cast<ObShardingInfo*>(allocator.alloc(sizeof(ObShardingInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        temp_sharding = new (temp_sharding) ObShardingInfo();
        if (OB_FAIL(temp_sharding->copy_without_part_keys(*input_sharding))) {
          LOG_WARN("failed to copy sharding info", K(ret));
        } else if (OB_FAIL(temp_sharding->get_partition_keys().assign(part_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(temp_sharding->get_sub_partition_keys().assign(subpart_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(temp_sharding->get_partition_func().assign(part_func))) {
          LOG_WARN("failed to assign part funcs", K(ret));
        } else {
          output_sharding = temp_sharding;
          is_inherited_sharding = true;
          LOG_TRACE("succeed to convert subplan scan sharding", K(*output_sharding));
        }
      }
    }
  }
  return ret;
}

double oceanbase::sql::Path::get_path_output_rows() const
{
  double card = 0.0;
  if (is_inner_path()) {
    card = inner_row_count_;
  } else if (NULL != parent_) {
    card = parent_->get_output_rows();
  }
  return card;
}

/*
目前ObJoinOrder在添加path的时候，会分成下面两种情况

1 当查询没有limit子句的时候，那么在add path的时候我们会定义如下的dominate关系：一条路径p1 dominate 另外一条路径 p2 如果

    1.1 p1的代价比p2的代价小

    1.2 p1的interesting order是p2的interesting order的superset

最终所有没有被dominate的path会保留下来

2 当查询有limit子句，我们把dominate关系修正为如下: 一条路径p1 dominate 另外一条路径 p1 如果

    2.1 p1的代价比p2的代价小

    2.2 p1的interesting order是p2的interesting order的superset

    2.3 p1的连接类型跟p2的连接类型是一样的，或者p2的连接类型是merge join，并且两边都需要排序

最终所有没有被dominate的path会保留下来, 虽然这种策略可能会导致选择的计划并不是最优的，但是个人认为概率比较小，相当于我们为每种join类型都会保留一些路径
*/
int ObJoinOrder::add_path(Path* path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(path), K(ret));
  } else {
    DominateRelation plan_rel = DominateRelation::OBJ_UNCOMPARABLE;
    OPT_TRACE_TITLE("new candidate path:", path);
    bool should_add = true;

    if (OB_UNLIKELY(get_plan()->get_optimizer_context().generate_random_plan())) {
      ObQueryCtx* query_ctx = get_plan()->get_optimizer_context().get_query_ctx();
      bool random_flag = !OB_ISNULL(query_ctx) && (query_ctx->rand_gen_.get(0, 1) == 1);
      should_add = interesting_paths_.empty() || random_flag ||
                   (!path->contain_match_all_fake_cte() && path->contain_fake_cte());
    }

    /**
     * fake cte会生成两条path，一条local、一条match all
     * match fake cte路径只用来生成remote的计划
     * 如果当前match all fake cte与其他表join之后sharding变成local了
     * 说明这条路径非预期，只用local的fake cte路径即可
     */
    if (!path->is_cte_path() &&
        path->contain_match_all_fake_cte() &&
        !path->is_remote()) {
      should_add = false;
      OPT_TRACE("contain match all fake cte, but not remote path, will not add path");
    }
    for (int64_t i = interesting_paths_.count() - 1; OB_SUCC(ret) && should_add && i >= 0; --i) {
      Path *cur_path = interesting_paths_.at(i);
      OPT_TRACE("compare with path:", cur_path);
      if (OB_ISNULL(cur_path)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(cur_path), K(ret));
      } else if (OB_FAIL(compute_path_relationship(*cur_path,
                                                   *path,
                                                   plan_rel))) {
        LOG_WARN("failed to compute plan relationship",
                 K(*cur_path), K(*path), K(ret));
      } else if (DominateRelation::OBJ_LEFT_DOMINATE == plan_rel ||
                 DominateRelation::OBJ_EQUAL == plan_rel) {
        should_add = false;
        LOG_TRACE("current path has been dominated, no need to add", K(*cur_path),
            K(*path), K(ret));
        OPT_TRACE("current path has been dominated");
      } else if (DominateRelation::OBJ_RIGHT_DOMINATE == plan_rel) {
        if (OB_FAIL(interesting_paths_.remove(i))) {
          LOG_WARN("failed to remove dominated plans", K(i), K(ret));
        } else {
          LOG_TRACE("current path dominated interesting path", K(*cur_path), K(*path), K(ret));
          OPT_TRACE("current path dominated interesting path");
        }
      } else {
        OPT_TRACE("path can not compare");
      }
    }
    if (OB_SUCC(ret)) {
      increase_total_path_num();
      if (should_add && OB_FAIL(interesting_paths_.push_back(path))) {
        LOG_WARN("failed to add plan into interesting paths", K(ret));
      } else if (!should_add && OB_FAIL(add_recycled_paths(path))) {
        LOG_WARN("failed to add recycled path", K(ret));
      } else if (should_add) {
        OPT_TRACE("this path is added, interesting path count:", interesting_paths_.count());
      } else {
        OPT_TRACE("this path is domained, interesting path count:", interesting_paths_.count());
      }
    }
  }
  return ret;
}

int ObJoinOrder::add_recycled_paths(Path* path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(path), K(ret));
  } else if (!path->is_join_path()) {
    // do nothing
  } else {
    static_cast<JoinPath*>(path)->reuse();
    if (OB_FAIL(get_plan()->get_recycled_join_paths().push_back(static_cast<JoinPath*>(path)))) {
      LOG_WARN("failed to add plan into recycled paths", K(ret));
    }
  }
  return ret;
}

bool ObJoinOrder::use_vec_index_cost_compare_strategy(const AccessPath &first_path,
                                                    const AccessPath &second_path)
{
  bool ret_bool = false;
  if (first_path.domain_idx_info_.domain_idx_type_ == VEC_INDEX
    && second_path.domain_idx_info_.domain_idx_type_ == VEC_INDEX) {
      // now, vec index only compare post_filter and pre_filter
      // if add cost estimate for vec index, might compare pre-filter and pre-filter
      ret_bool = (first_path.domain_idx_info_.vec_extra_info_.is_post_filter()
                ||second_path.domain_idx_info_.vec_extra_info_.is_post_filter());
    }
  return ret_bool;
}

int ObJoinOrder::compute_vec_idx_path_relationship(const AccessPath &first_path,
                                                    const AccessPath &second_path,
                                                    DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  AccessPath *final_res = nullptr;
  AccessPath *post_path = first_path.domain_idx_info_.vec_extra_info_.is_post_filter() ?
                          const_cast<AccessPath*>(&first_path) : const_cast<AccessPath*>(&second_path);
  AccessPath *pre_path = first_path.domain_idx_info_.vec_extra_info_.is_post_filter() ?
                          const_cast<AccessPath*>(&second_path) : const_cast<AccessPath*>(&first_path);
  double selectivity = 0.0;
  if (first_path.domain_idx_info_.vec_extra_info_.is_force_pre_filter()) {
    relation = first_path.domain_idx_info_.vec_extra_info_.is_pre_filter() ? DominateRelation::OBJ_LEFT_DOMINATE : DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (first_path.domain_idx_info_.vec_extra_info_.is_force_post_filter()) {
    relation = first_path.domain_idx_info_.vec_extra_info_.is_post_filter() ? DominateRelation::OBJ_LEFT_DOMINATE : DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                      get_plan()->get_selectivity_ctx(),
                                                      get_restrict_infos(),
                                                      selectivity,
                                                      get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else if (selectivity > ObVecIdxExtraInfo::DEFAULT_SELECTIVITY_RATE) {
    relation = first_path.domain_idx_info_.vec_extra_info_.is_post_filter() ? DominateRelation::OBJ_LEFT_DOMINATE : DominateRelation::OBJ_RIGHT_DOMINATE;
  } else {
    relation = first_path.domain_idx_info_.vec_extra_info_.is_pre_filter() ? DominateRelation::OBJ_LEFT_DOMINATE : DominateRelation::OBJ_RIGHT_DOMINATE;
  }

  final_res = relation == DominateRelation::OBJ_LEFT_DOMINATE ?
              const_cast<AccessPath*>(&first_path) : const_cast<AccessPath*>(&second_path);
  final_res->domain_idx_info_.vec_extra_info_.set_row_count(table_meta_info_.table_row_count_);
  return ret;
}

/*
 * One path dominates another path if
 * 1 it has lower cost than another path
 * 2 its interesting order is a superset of another path
 * 3 its sharding info dominate another path
 * 4 it is more pipeline than another one
 */
int ObJoinOrder::compute_path_relationship(const Path &first_path,
                                           const Path &second_path,
                                           DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  DominateRelation temp_relation;
  relation = DominateRelation::OBJ_EQUAL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(first_path), K(second_path), K(stmt), K(ret));
  } else if (first_path.is_access_path() && second_path.is_access_path()
            && use_vec_index_cost_compare_strategy(static_cast<const AccessPath&>(first_path), static_cast<const AccessPath&>(second_path))) {
    if (OB_FAIL(compute_vec_idx_path_relationship(static_cast<const AccessPath&>(first_path), static_cast<const AccessPath&>(second_path), relation))) {
      LOG_WARN("fail to compute_vec_idx_path_relationship", K(first_path), K(second_path), K(relation), K(ret));
    }
  } else if (first_path.contain_fake_cte() && second_path.contain_fake_cte() &&
             first_path.contain_match_all_fake_cte() != second_path.contain_match_all_fake_cte()) {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
    OPT_TRACE("sharding can not compare for fake cte");
  } else if (first_path.is_join_path() && second_path.is_join_path() &&
             OB_FAIL(compute_join_path_relationship(static_cast<const JoinPath&>(first_path),
                                                    static_cast<const JoinPath&>(second_path),
                                                    relation))) {
    LOG_WARN("failed to compute join path relationship", K(ret));
  } else if (DominateRelation::OBJ_EQUAL != relation) {
    /* do nothing */
  } else if (first_path.is_access_path() && second_path.is_access_path() &&
             !static_cast<const AccessPath&>(first_path).is_valid_inner_path_
             && static_cast<const AccessPath&>(second_path).is_valid_inner_path_) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
    OPT_TRACE("right path dominate left path because of pushdown prefix filters");
  } else if (first_path.is_access_path() && second_path.is_access_path() &&
             static_cast<const AccessPath&>(first_path).is_valid_inner_path_
             && !static_cast<const AccessPath&>(second_path).is_valid_inner_path_) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
    OPT_TRACE("left path dominate right path because of pushdown prefix filters");
  } else {
    int64_t left_dominated_count = 0;
    int64_t right_dominated_count = 0;
    int64_t uncompareable_count = 0;
    // check dominate relationship for cost
    if (fabs(first_path.cost_ - second_path.cost_) < OB_DOUBLE_EPSINON) {
      // do nothing
      OPT_TRACE("the cost of the two paths is equal");
    } else if (first_path.cost_ < second_path.cost_) {
      left_dominated_count++;
      OPT_TRACE("left path is cheaper");
    } else {
      right_dominated_count++;
      OPT_TRACE("right path is cheaper");
    }
    // check dominate relationship for parallel degree
    if (first_path.parallel_ == second_path.parallel_) {
      // do nothing
      OPT_TRACE("the parallel of the two paths is equal");
    } else if (first_path.parallel_ < second_path.parallel_) {
      left_dominated_count++;
      OPT_TRACE("left path use less parallel");
    } else {
      right_dominated_count++;
      OPT_TRACE("right path use less parallel");
    }
    // check dominate relationship for sharding info
    if (OB_FAIL(ObOptimizerUtil::compute_sharding_relationship(first_path.get_strong_sharding(),
                                                               first_path.get_weak_sharding(),
                                                               second_path.get_strong_sharding(),
                                                               second_path.get_weak_sharding(),
                                                               get_output_equal_sets(),
                                                               temp_relation))) {
      LOG_WARN("failed to compute sharding relationship", K(ret));
    } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
      /*do nothing*/
      OPT_TRACE("the sharding of the two paths is equal");
    } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
      left_dominated_count++;
      OPT_TRACE("left path dominate right path because of sharding");
      if (right_dominated_count > 0) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
      right_dominated_count++;
      OPT_TRACE("right path dominate left path because of sharding");
      if (left_dominated_count > 0) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    } else {
      OPT_TRACE("sharding can not compare");
      uncompareable_count++;
      relation = DominateRelation::OBJ_UNCOMPARABLE;
    }

    // check dominate relationship for pipeline info
    if (OB_SUCC(ret) && stmt->has_limit() && DominateRelation::OBJ_UNCOMPARABLE !=relation) {
      if (OB_FAIL(compute_pipeline_relationship(first_path,
                                                second_path,
                                                temp_relation))) {
        LOG_WARN("failed to check pipeline relationship", K(ret));
      } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
        /*do nothing*/
        OPT_TRACE("both path is pipeline");
      } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
        left_dominated_count++;
        OPT_TRACE("left path dominate right path because of pipeline");
        if (right_dominated_count > 0) {
          relation = DominateRelation::OBJ_UNCOMPARABLE;
        }
      } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
        OPT_TRACE("right path dominate left path because of pipeline");
        right_dominated_count++;
        if (left_dominated_count > 0) {
          relation = DominateRelation::OBJ_UNCOMPARABLE;
        }
      } else {
        OPT_TRACE("pipeline path can not compare");
        uncompareable_count++;
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    }

    // check dominate relationship for ordering info
    if (OB_FAIL(ret) || relation == DominateRelation::OBJ_UNCOMPARABLE) {
      //do nothing
    } else if (OB_FAIL(ObOptimizerUtil::compute_ordering_relationship(first_path.has_interesting_order(),
                                                                      second_path.has_interesting_order(),
                                                                      first_path.ordering_,
                                                                      second_path.ordering_,
                                                                      get_output_equal_sets(),
                                                                      get_output_const_exprs(),
                                                                      temp_relation))) {
      LOG_WARN("failed to compute ordering relationship", K(ret));
    } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
      /*do nothing*/
      OPT_TRACE("the interesting order of the two paths is equal");
    } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
      left_dominated_count++;
      OPT_TRACE("left path dominate right path because of interesting order");
      if (right_dominated_count > 0) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
      OPT_TRACE("right path dominate left path because of interesting order");
      right_dominated_count++;
      if (left_dominated_count > 0) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    } else {
      OPT_TRACE("interesting order can not compare");
      uncompareable_count++;
      relation = DominateRelation::OBJ_UNCOMPARABLE;
    }

    // relation is EQUAL now, check index column count when both not index back
    // remove this if adjusted estimate cost for table scan
    if (OB_SUCC(ret) && first_path.is_access_path() && second_path.is_access_path()
        && left_dominated_count == 0 && right_dominated_count == 0
        && uncompareable_count == 0) {
      const ObIndexMetaInfo &first_index_info =
          static_cast<const AccessPath&>(first_path).get_cost_table_scan_info().index_meta_info_;
      const ObIndexMetaInfo &second_index_info =
          static_cast<const AccessPath&>(second_path).get_cost_table_scan_info().index_meta_info_;
      if (first_index_info.is_index_back_ || second_index_info.is_index_back_) {
        // do nothing for this, will return EQUAL final
      } else if (first_index_info.index_column_count_ < second_index_info.index_column_count_) {
        ++left_dominated_count;
      } else if (first_index_info.index_column_count_ > second_index_info.index_column_count_) {
        ++right_dominated_count;
      } else {
        // do nothing
      }
    }

    // compute final result
    if (OB_SUCC(ret)) {
      if (left_dominated_count > 0 && right_dominated_count == 0 && uncompareable_count == 0) {
        relation = DominateRelation::OBJ_LEFT_DOMINATE;
      } else if (right_dominated_count > 0 && left_dominated_count == 0
                 && uncompareable_count == 0) {
        relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      } else if (left_dominated_count == 0 && right_dominated_count == 0
                 && uncompareable_count == 0) {
        relation = DominateRelation::OBJ_EQUAL;
      } else {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_join_path_relationship(const JoinPath &first_path,
                                                const JoinPath &second_path,
                                                DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = DominateRelation::OBJ_EQUAL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(first_path.right_path_) || OB_ISNULL(second_path.right_path_)
      || OB_ISNULL(first_path.right_path_->parent_) || OB_ISNULL(second_path.right_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(first_path.right_path_), K(second_path.right_path_));
  }

  if (OB_SUCC(ret) && DominateRelation::OBJ_EQUAL == relation && get_plan()->get_is_rescan_subplan()) {
    bool first_is_px_with_das = !first_path.is_match_all() && first_path.contain_das_op();
    bool second_is_px_with_das = !second_path.is_match_all() && second_path.contain_das_op();
    if (first_is_px_with_das && !second_is_px_with_das) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right path dominate left path because of px rescan plan use das op");
    } else if (!first_is_px_with_das && second_is_px_with_das) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left path dominate right path because of px rescan plan use das op");
    }
  }

  if (OB_SUCC(ret) && DominateRelation::OBJ_EQUAL == relation) {
    if (first_path.contain_normal_nl() && !second_path.contain_normal_nl()) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right path dominate left path because of normal nl");
    } else if (!first_path.contain_normal_nl() && second_path.contain_normal_nl()) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left path dominate right path because of normal nl");
    } else if (first_path.has_none_equal_join() && !second_path.has_none_equal_join()) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right path dominate left path because of none equal join");
    } else if (!first_path.has_none_equal_join() && second_path.has_none_equal_join()) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left path dominate right path because of none equal join");
    }
  }

  if (OB_SUCC(ret) && DominateRelation::OBJ_EQUAL == relation
      && first_path.is_nlj_with_param_down() && second_path.is_nlj_with_param_down()
      && first_path.right_path_->parent_->get_tables().equal(second_path.right_path_->parent_->get_tables())) {
    int64_t first_right_local_rescan = 0;
    int64_t second_right_local_rescan = 0;
    bool first_can_px_batch_rescan = false;
    bool second_can_px_batch_rescan = false;
    if (!first_path.can_use_batch_nlj_ && second_path.can_use_batch_nlj_) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right path dominate left path because of batch nl");
    } else if (first_path.can_use_batch_nlj_ && !second_path.can_use_batch_nlj_) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left path dominate right path because of batch nl");
    } else if (OB_FAIL(first_path.check_right_is_local_scan(first_right_local_rescan))
               || OB_FAIL(second_path.check_right_is_local_scan(second_right_local_rescan))) {
      LOG_WARN("failed to check right is lcoal rescan", K(ret));
    } else if (first_path.can_use_batch_nlj_ && second_path.can_use_batch_nlj_) {
      if (first_path.parallel_ != second_path.parallel_) {
        /* do nothing */
      } else if (first_right_local_rescan < second_right_local_rescan) {
        relation = DominateRelation::OBJ_RIGHT_DOMINATE;
        OPT_TRACE("right path dominate left path because of batch nl local rescan");
      } else if (first_right_local_rescan > second_right_local_rescan) {
        relation = DominateRelation::OBJ_LEFT_DOMINATE;
        OPT_TRACE("left path dominate right path because of batch nl local rescan");
      }
    } else if (first_right_local_rescan < second_right_local_rescan) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right path dominate left path because of nl local rescan");
    } else if (first_right_local_rescan > second_right_local_rescan) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left path dominate right path because of nl local rescan");
    } else if (0 < first_right_local_rescan && 0 < second_right_local_rescan) {
      /* do nothing */
    } else if (OB_FAIL(first_path.pre_check_nlj_can_px_batch_rescan(first_can_px_batch_rescan))
               || OB_FAIL(second_path.pre_check_nlj_can_px_batch_rescan(second_can_px_batch_rescan))) {
      LOG_WARN("failed to pre check spf can px batch rescan", K(ret));
    } else if (!first_can_px_batch_rescan && second_can_px_batch_rescan) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right plan dominate left plan because of nl px batch rescan");
    } else if (first_can_px_batch_rescan && !second_can_px_batch_rescan) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left plan dominate right plan because of nl px batch rescan");
    } else {
      /* do nothing */
    }
    LOG_TRACE("finish compute nlj rescan join path relationship",
                    K(first_path.can_use_batch_nlj_), K(second_path.can_use_batch_nlj_),
                    K(first_right_local_rescan), K(second_right_local_rescan),
                    K(first_can_px_batch_rescan), K(second_can_px_batch_rescan));
  }
  return ret;
}

int ObJoinOrder::compute_pipeline_relationship(const Path &first_path,
                                               const Path &second_path,
                                               DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(ret));
  } else {
    if (!stmt->get_order_items().empty()) {
      bool is_first_left_prefix = false;
      bool is_first_right_prefix = false;
      bool is_second_left_prefix = false;
      bool is_second_right_prefix = false;
      if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(stmt->get_order_items(),
                                                      first_path.ordering_,
                                                      get_output_equal_sets(),
                                                      get_output_const_exprs(),
                                                      is_first_left_prefix,
                                                      is_first_right_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
                K(stmt->get_order_items()), K(first_path.ordering_), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(stmt->get_order_items(),
                                                             second_path.ordering_,
                                                             get_output_equal_sets(),
                                                             get_output_const_exprs(),
                                                             is_second_left_prefix,
                                                             is_second_right_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
            K(stmt->get_order_items()), K(second_path.ordering_), K(ret));
      } else if (!is_first_left_prefix && !is_second_left_prefix) {
        relation = DominateRelation::OBJ_EQUAL;
      } else if (is_first_left_prefix && !is_second_left_prefix) {
        relation = DominateRelation::OBJ_LEFT_DOMINATE;
      } else if (!is_first_left_prefix && is_second_left_prefix) {
        relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && DominateRelation::OBJ_UNCOMPARABLE == relation) {
      bool first_is_pipelined = first_path.is_pipelined_path();
      bool second_is_pipelined = second_path.is_pipelined_path();
      bool first_is_nl_pipelined = first_path.is_nl_style_pipelined_path();
      bool second_is_nl_pipelined = second_path.is_nl_style_pipelined_path();
      if (!first_path.is_join_path() && !second_path.is_join_path()) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      } else if (first_is_pipelined && !second_is_pipelined) {
        relation = DominateRelation::OBJ_LEFT_DOMINATE;
      } else if (!first_is_pipelined && second_is_pipelined) {
        relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      } else if (first_is_pipelined &&
                  second_is_pipelined &&
                  first_is_nl_pipelined &&
                  second_is_nl_pipelined) {
        relation = DominateRelation::OBJ_EQUAL;
      } else if (first_is_pipelined &&
                  second_is_pipelined &&
                  !first_is_nl_pipelined &&
                  !second_is_nl_pipelined) {
        relation = DominateRelation::OBJ_EQUAL;
      } else if (first_is_pipelined && second_is_pipelined) {
        relation = DominateRelation::OBJ_UNCOMPARABLE;
      } else {
        //Both plans are non-pipeline and use cost competition
        relation = DominateRelation::OBJ_EQUAL;
      }
    }
  }
  return ret;
}

int oceanbase::sql::Path::assign(const Path &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  parent_ = other.parent_;
  is_local_order_ = other.is_local_order_;
  is_range_order_ = other.is_range_order_;
  cost_  = other.cost_;
  op_cost_ = other.op_cost_;
  log_op_ = other.log_op_;
  is_inner_path_ = other.is_inner_path_;
  inner_row_count_ = other.inner_row_count_;
  strong_sharding_ = other.strong_sharding_;
  exchange_allocated_ = other.exchange_allocated_;
  phy_plan_type_ = other.phy_plan_type_;
  location_type_ = other.location_type_;
  contain_fake_cte_ = other.contain_fake_cte_;
  contain_pw_merge_op_ = other.contain_pw_merge_op_;
  contain_match_all_fake_cte_ = other.contain_match_all_fake_cte_;
  contain_das_op_ = other.contain_das_op_;
  parallel_ = other.parallel_;
  op_parallel_rule_ = other.op_parallel_rule_;
  available_parallel_ = other.available_parallel_;
  server_cnt_ = other.server_cnt_;
  is_pipelined_path_ = other.is_pipelined_path_;
  is_nl_style_pipelined_path_ = other.is_nl_style_pipelined_path_;

  if (OB_FAIL(ordering_.assign(other.ordering_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  } else if (OB_FAIL(server_list_.assign(other.server_list_))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else if (OB_FAIL(filter_.assign(other.filter_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  } else if (OB_FAIL(pushdown_filters_.assign(other.pushdown_filters_))) {
    LOG_WARN("failed to assign pushdown filters", K(ret));
  } else if (OB_FAIL(nl_params_.assign(other.nl_params_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  } else if (OB_FAIL(subquery_exprs_.assign(other.subquery_exprs_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  } else if (OB_FAIL(weak_sharding_.assign(other.weak_sharding_))) {
    LOG_WARN("failed to assign nested loop params", K(ret));
  }
  return ret;
}

bool oceanbase::sql::Path::is_cte_path() const
{
  return NULL != parent_ && parent_->get_type() == FAKE_CTE_TABLE_ACCESS;
}

bool oceanbase::sql::Path::is_function_table_path() const
{
  return NULL != parent_ && parent_->get_type() == FUNCTION_TABLE_ACCESS;
}

bool oceanbase::sql::Path::is_json_table_path() const
{
  return NULL != parent_ && parent_->get_type() == JSON_TABLE_ACCESS;
}

bool oceanbase::sql::Path::is_temp_table_path() const
{
  return NULL != parent_ && parent_->get_type() == TEMP_TABLE_ACCESS;
}

bool oceanbase::sql::Path::is_access_path() const
{
  return NULL != parent_ && parent_->get_type() == ACCESS;
}

bool oceanbase::sql::Path::is_values_table_path() const
{
  return NULL != parent_ && parent_->get_type() == VALUES_TABLE_ACCESS;
}

bool oceanbase::sql::Path::is_join_path() const
{
  return NULL != parent_ && parent_->get_type() == JOIN;
}

bool oceanbase::sql::Path::is_subquery_path() const
{
  return NULL != parent_ && parent_->get_type() == SUBQUERY;
}

int oceanbase::sql::Path::check_is_base_table(bool &is_base_table)
{
  int ret = OB_SUCCESS;
  is_base_table = false;
  if (!subquery_exprs_.empty()) {
    is_base_table = false;
  } else if (is_access_path()) {
    is_base_table = true;
  } else if (is_subquery_path()) {
    ObLogicalOperator *root =  NULL;
    if (OB_ISNULL(root = static_cast<SubQueryPath*>(this)->root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObLogicalOperator::check_is_table_scan(*root, is_base_table))) {
      LOG_WARN("failed to check whether is table scan", K(ret));
    } else { /*do nothing*/ }
  } else {
    is_base_table = false;
  }
  return ret;
}

int oceanbase::sql::Path::re_estimate_cost(EstimateCostInfo &info, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  UNUSED(info);
  card = get_path_output_rows();
  cost = get_cost();
  return ret;
}

int oceanbase::sql::Path::compute_pipeline_info()
{
  int ret = OB_SUCCESS;
  is_pipelined_path_ = true;
  is_nl_style_pipelined_path_ = true;
  return ret;
}

int oceanbase::sql::Path::compute_path_property_from_log_op()
{
  int ret = OB_SUCCESS;
  int64_t interesting_order_info;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(log_op_) || OB_ISNULL(parent_) ||
      OB_ISNULL(parent_->get_plan()) ||
      OB_ISNULL(stmt=parent_->get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null log op", K(ret));
  } else if (OB_FAIL(get_ordering().assign(log_op_->get_op_ordering()))) {
    LOG_WARN("failed to assign ordering", K(ret));
  } else if (OB_FAIL(weak_sharding_.assign(log_op_->get_weak_sharding()))) {
    LOG_WARN("failed to assign weak sharding", K(ret));
  } else if (OB_FAIL(parent_->check_all_interesting_order(get_ordering(),
                                                          stmt,
                                                          interesting_order_info))) {
    LOG_WARN("failed to check all interesting order", K(ret));
  } else if (OB_FAIL(server_list_.assign(log_op_->get_server_list()))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    strong_sharding_ = log_op_->get_strong_sharding();
    set_interesting_order_info(interesting_order_info);
    is_local_order_ = log_op_->get_is_local_order();
    is_range_order_ = log_op_->get_is_range_order();
    exchange_allocated_ = log_op_->is_exchange_allocated();
    phy_plan_type_ = log_op_->get_phy_plan_type();
    location_type_ = log_op_->get_location_type();
    contain_fake_cte_ = log_op_->get_contains_fake_cte();
    contain_pw_merge_op_ = log_op_->get_contains_pw_merge_op();
    contain_match_all_fake_cte_ = log_op_->get_contains_match_all_fake_cte();
    contain_das_op_ = log_op_->get_contains_das_op();
    parallel_ = log_op_->get_parallel();
    op_parallel_rule_ = log_op_->get_op_parallel_rule();
    available_parallel_ = log_op_->get_available_parallel();
    server_cnt_ = log_op_->get_server_cnt();
    is_pipelined_path_ = log_op_->is_pipelined_plan();
    is_nl_style_pipelined_path_ = log_op_->is_nl_style_pipelined_plan();
    cost_ = log_op_->get_cost();
    op_cost_ = log_op_->get_op_cost();
    contain_pw_merge_op_ = log_op_->get_contains_pw_merge_op();
    inner_row_count_ = log_op_->get_card();
  }
  return ret;
}

int oceanbase::sql::Path::set_parallel_and_server_info_for_match_all()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null log plan", K(ret), K(parent_));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
    op_parallel_rule_ = OpParallelRule::OP_DAS_DOP;
    available_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
    server_cnt_ = 1;
    server_list_.reuse();
    if (OB_FAIL(server_list_.push_back(opt_ctx.get_local_server_addr()))) {
      LOG_WARN("failed to assign das path server list", K(ret));
    }
  }
  return ret;
}

int AccessPath::assign(const AccessPath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  ref_table_id_ = other.ref_table_id_;
  index_id_ = other.index_id_;
  is_global_index_ = other.is_global_index_;
  use_das_ = other.use_das_;
  table_partition_info_ = other.table_partition_info_;
  is_get_ = other.is_get_;
  order_direction_ = other.order_direction_;
  force_direction_ = other.force_direction_;
  is_hash_index_ = other.is_hash_index_;
  sample_info_ = other.sample_info_;
  range_prefix_count_ = other.range_prefix_count_;
  table_opt_info_ = other.table_opt_info_;
  for_update_ = other.for_update_;
  use_skip_scan_ = other.use_skip_scan_;
  use_column_store_ = other.use_column_store_;
  is_valid_inner_path_ = other.is_valid_inner_path_;
  pre_query_range_ = NULL;
  pre_range_graph_ = NULL;
  can_batch_rescan_ = other.can_batch_rescan_;
  can_das_dynamic_part_pruning_ = other.can_das_dynamic_part_pruning_;
  is_ordered_by_pk_ = other.is_ordered_by_pk_;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(allocator), K(ret));
  } else if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("copy path error", K(ret));
  } else if (OB_FAIL(index_keys_.assign(other.index_keys_))) {
    LOG_WARN("Failed to assign re_est_param", K(ret));
  } else if (OB_FAIL(est_cost_info_.assign(other.est_cost_info_))) {
    LOG_WARN("Failed to assign re_est_param", K(ret));
  } else if (OB_FAIL(est_records_.assign(other.est_records_))) {
    LOG_WARN("Failed to assign re_est_param", K(ret));
  } else if (other.pre_range_graph_ != NULL) {
    ObPreRangeGraph *range_graph = static_cast<ObPreRangeGraph*>(allocator->alloc(sizeof(ObPreRangeGraph)));
    if (OB_ISNULL(range_graph)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for pre range graph");
    } else {
      range_graph = new(range_graph)ObPreRangeGraph(*allocator);
      if (OB_FAIL(range_graph->deep_copy(*other.pre_range_graph_))) {
        range_graph->~ObPreRangeGraph();
        range_graph = NULL;
      } else {
        pre_range_graph_ = range_graph;
      }
    }
  } else if (other.pre_query_range_ != NULL) {
    ObQueryRange *query_range = static_cast<ObQueryRange*>(allocator->alloc(sizeof(ObQueryRange)));
    if (OB_ISNULL(query_range)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for query range");
    } else {
      query_range = new(query_range)ObQueryRange(*allocator);
      if (OB_FAIL(query_range->deep_copy(*other.pre_query_range_, true))) {
        query_range->~ObQueryRange();
        query_range = NULL;
      } else {
        pre_query_range_ = query_range;
      }
    }
  }
  return ret;
}

// compute auto dop for access path
int AccessPath::compute_parallel_degree(const int64_t cur_min_parallel_degree,
                                        int64_t &parallel)
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::UNSET_PARALLEL;
  const ObSimpleBatch::ObBatchType type = est_cost_info_.batch_type_;
  int64_t px_part_gi_min_part_per_dop = 0;
  double cost_threshold_us = 0.0;
  int64_t cur_parallel_degree_limit = ObGlobalHint::UNSET_PARALLEL;
  int64_t server_cnt = 0;
  if (use_das_ || is_virtual_table(est_cost_info_.ref_table_id_)
      || est_cost_info_.is_unique_) {
    parallel = ObGlobalHint::DEFAULT_PARALLEL;
  } else if (OB_FAIL(check_and_prepare_estimate_parallel_params(cur_min_parallel_degree,
                                                                px_part_gi_min_part_per_dop,
                                                                cost_threshold_us,
                                                                server_cnt,
                                                                cur_parallel_degree_limit))) {
    LOG_WARN("failed to check and prepare estimate parallel params", K(ret));
  } else {
    double pre_cost = -1.0;
    double cost = 0.0;
    double px_cost = 0.0;
    int64_t pre_parallel = ObGlobalHint::UNSET_PARALLEL;
    int64_t cur_parallel = ObGlobalHint::UNSET_PARALLEL;
    double part_cnt_per_dop = 0.0;
    while (OB_SUCC(ret) && ObGlobalHint::UNSET_PARALLEL == parallel) {
      if (OB_FAIL(prepare_estimate_parallel(pre_parallel,
                                            cur_parallel_degree_limit,
                                            cost_threshold_us,
                                            server_cnt,
                                            px_part_gi_min_part_per_dop,
                                            px_cost,
                                            cost,
                                            cur_parallel,
                                            part_cnt_per_dop))) {
        LOG_WARN("failed to prepare estimate next parallel", K(ret), K(pre_parallel),
                      K(cur_parallel_degree_limit), K(cost_threshold_us), K(cost), K(cur_parallel));
      } else if (pre_parallel == cur_parallel || cur_parallel >= cur_parallel_degree_limit) {
        parallel = cur_parallel;
      } else if (OB_FAIL(estimate_cost_for_parallel(cur_parallel,
                                                    part_cnt_per_dop,
                                                    px_cost,
                                                    cost))) {
        LOG_WARN("failed to estimate cost for parallel", K(ret), K(cur_parallel), K(part_cnt_per_dop));
      } else if (pre_parallel >= ObGlobalHint::DEFAULT_PARALLEL && pre_cost <= cost) {
        parallel = pre_parallel;
      } else if (cost - px_cost <= cost_threshold_us || px_cost >= cost - px_cost) {
        parallel = cur_parallel;
      } else {
        pre_cost = cost;
        pre_parallel = cur_parallel;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > parallel || cur_parallel_degree_limit < parallel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel result", K(ret), K(cur_parallel_degree_limit), K(parallel));
    } else {
      OPT_TRACE("finish compute one path parallel degree:", parallel);
      LOG_TRACE("finish compute parallel degree", K(est_cost_info_.phy_query_range_row_count_),
                                  K(est_cost_info_.logical_query_range_row_count_),
                                  K(parallel), K(cur_parallel_degree_limit),
                                  K(cost_threshold_us), K(px_cost), K(cost), K(pre_cost));
    }
  }
  return ret;
}

int AccessPath::check_and_prepare_estimate_parallel_params(const int64_t cur_min_parallel_degree,
                                                           int64_t &px_part_gi_min_part_per_dop,
                                                           double &cost_threshold_us,
                                                           int64_t &server_cnt,
                                                           int64_t &cur_parallel_degree_limit) const
{
  int ret = OB_SUCCESS;
  px_part_gi_min_part_per_dop = 0;
  cost_threshold_us = 0.0;
  server_cnt = 0;
  cur_parallel_degree_limit = ObGlobalHint::UNSET_PARALLEL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObAddr, 8> server_list;
  if (OB_ISNULL(table_partition_info_) ||
      OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()) ||
      OB_ISNULL(opt_ctx = &parent_->get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(table_partition_info_), K(parent_), K(session_info));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR__PX_MIN_GRANULES_PER_SLAVE, px_part_gi_min_part_per_dop))) {
    LOG_WARN("failed to get sys variable px min granule per slave", K(ret));
  } else if (OB_FAIL(table_partition_info_->get_all_servers(server_list))) {
    LOG_WARN("failed to get all servers", K(ret));
  } else {
    px_part_gi_min_part_per_dop = std::max(1L, px_part_gi_min_part_per_dop);
    cost_threshold_us = 1000.0 * std::max(10L, opt_ctx->get_parallel_min_scan_time_threshold());
    server_cnt = server_list.count();
    cur_parallel_degree_limit = opt_ctx->get_parallel_degree_limit(server_cnt);
    const int64_t row_parallel_limit = std::floor(get_phy_query_range_row_count() / ROW_COUNT_THRESHOLD_PER_DOP);
    if (cur_min_parallel_degree > ObGlobalHint::UNSET_PARALLEL && cur_min_parallel_degree < cur_parallel_degree_limit) {
      cur_parallel_degree_limit = cur_min_parallel_degree;
    }
    if (row_parallel_limit < cur_parallel_degree_limit) {
      cur_parallel_degree_limit = row_parallel_limit;
    }
    if (OptSkipScanState::SS_DISABLE != use_skip_scan_) {
      const int64_t ss_scan_parallel_limit = std::floor(est_cost_info_.ss_prefix_ndv_);
      if (ss_scan_parallel_limit < cur_parallel_degree_limit) {
        cur_parallel_degree_limit = ss_scan_parallel_limit;
      }
    }
    cur_parallel_degree_limit = std::max(1L, cur_parallel_degree_limit);
  }
  return ret;
}

int AccessPath::prepare_estimate_parallel(const int64_t pre_parallel,
                                          const int64_t parallel_degree_limit,
                                          const double cost_threshold_us,
                                          const int64_t server_cnt,
                                          const int64_t px_part_gi_min_part_per_dop,
                                          const double px_cost,
                                          const double cost,
                                          int64_t &parallel,
                                          double &part_cnt_per_dop) const
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::UNSET_PARALLEL;
  part_cnt_per_dop = 0.0;
  bool is_part_gi = false;
  const int64_t part_cnt = est_cost_info_.index_meta_info_.index_part_count_;
  const double part_cnt_double = static_cast<double>(part_cnt);
  #define IS_PART_GI(check_dop) (ObGranuleUtil::is_partition_granule(part_cnt, check_dop, 0, px_part_gi_min_part_per_dop, true))
  if (ObGlobalHint::DEFAULT_PARALLEL > pre_parallel) {
    parallel = ObGlobalHint::DEFAULT_PARALLEL;
  } else if (ObGlobalHint::DEFAULT_PARALLEL == pre_parallel && 1 < server_cnt) {
    parallel = std::min(parallel_degree_limit, server_cnt);
  } else if (OB_UNLIKELY(cost_threshold_us < 1000.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(cost_threshold_us));
  } else {
    int64_t cur_parallel = pre_parallel;
    int64_t step = std::ceil(cost / cost_threshold_us);
    if (px_cost / cost < 0.1 && step > 1) {  // zhanyuetodo: optimize this
      step = std::max(server_cnt, step);
      cur_parallel += std::min(step, 32L);
    } else {
      cur_parallel += server_cnt;
    }
    bool is_part_gi = IS_PART_GI(cur_parallel);
    if (is_part_gi) {
      const int64_t pre_part_cnt_per_dop = std::ceil(part_cnt_double / pre_parallel);
      if (OB_UNLIKELY(1 >= pre_part_cnt_per_dop)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected params", K(ret), K(cur_parallel), K(px_part_gi_min_part_per_dop),
                                      K(part_cnt_double), K(pre_parallel), K(pre_part_cnt_per_dop));
      } else {
        const int64_t tmp_parallel = std::ceil(part_cnt_double / (pre_part_cnt_per_dop - 1));
        while (is_part_gi && std::ceil(part_cnt_double / cur_parallel) == pre_part_cnt_per_dop) {
          cur_parallel = std::max(tmp_parallel, cur_parallel + 1);
          is_part_gi = IS_PART_GI(cur_parallel);
        }
      }
    }
    if (OB_SUCC(ret)) {
      parallel = std::min(cur_parallel, parallel_degree_limit);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > parallel
                         || pre_parallel > parallel
                         || 0.0 >= part_cnt_double)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to prepare estimate parallel", K(ret), K(part_cnt), K(pre_parallel),
                                                    K(parallel), K(part_cnt_double));
  } else {
    /* treate as use block gi, use parallel directly.
       treate as use partition gi, consider some worker may deal one more part. */
    part_cnt_per_dop = IS_PART_GI(parallel) ? std::ceil(part_cnt_double / parallel)
                                            : part_cnt_double / parallel;
    LOG_DEBUG("finish prepare estimate parallel", K(part_cnt), K(parallel), K(part_cnt_per_dop));
  }
  return ret;
}

int AccessPath::estimate_cost_for_parallel(const int64_t cur_parallel,
                                           const double part_cnt_per_dop,
                                           double &px_cost,
                                           double &cost)
{
  int ret = OB_SUCCESS;
  px_cost = 0.0;
  cost = 0.0;
  double stats_phy_query_range_row_count = 0;
  double stats_logical_query_range_row_count = 0;
  int64_t opt_stats_cost_percent = 0;
  bool adj_cost_is_valid = false;
  double storage_est_cost = 0.0;
  double stats_est_cost = 0.0;
  double storage_est_px_cost = 0.0;
  double stats_est_px_cost = 0.0;
  double opt_phy_query_range_row_count = est_cost_info_.phy_query_range_row_count_;
  double opt_logical_query_range_row_count = est_cost_info_.logical_query_range_row_count_;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else if (OB_FAIL(check_adj_index_cost_valid(stats_phy_query_range_row_count,
                                                stats_logical_query_range_row_count,
                                                opt_stats_cost_percent,
                                                adj_cost_is_valid))) {
    LOG_WARN("failed to check adj index cost valid", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    if (OB_FAIL(ObOptEstCost::cost_table_for_parallel(est_cost_info_,
                                                      cur_parallel,
                                                      part_cnt_per_dop,
                                                      storage_est_px_cost,
                                                      storage_est_cost,
                                                      opt_ctx))) {
      LOG_WARN("failed to calculated cost for parallel", K(ret));
    } else if (!adj_cost_is_valid) {
      cost = storage_est_cost;
      px_cost = storage_est_px_cost;
    } else if (OB_FALSE_IT(est_cost_info_.phy_query_range_row_count_ = stats_phy_query_range_row_count)) {
    } else if (OB_FALSE_IT(est_cost_info_.logical_query_range_row_count_ = stats_logical_query_range_row_count)) {
    } else if (OB_FAIL(ObOptEstCost::cost_table_for_parallel(est_cost_info_,
                                                              cur_parallel,
                                                              part_cnt_per_dop,
                                                              stats_est_px_cost,
                                                              stats_est_cost,
                                                              opt_ctx))) {
      LOG_WARN("failed to calculated cost for parallel", K(ret));
    } else {
      double rate = opt_stats_cost_percent * 1.0 / 100.0;
      cost = storage_est_cost * (1-rate) + stats_est_cost * rate;
      px_cost = storage_est_px_cost * (1-rate) + stats_est_px_cost * rate;
      est_cost_info_.phy_query_range_row_count_ = opt_phy_query_range_row_count;
      est_cost_info_.logical_query_range_row_count_ = opt_logical_query_range_row_count;
    }
  }
  return ret;
}

int AccessPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  double stats_phy_query_range_row_count = 0;
  double stats_logical_query_range_row_count = 0;
  int64_t opt_stats_cost_percent = 0;
  bool adj_cost_is_valid = false;
  double storage_est_cost = 0.0;
  double stats_est_cost = 0.0;
  double opt_phy_query_range_row_count = est_cost_info_.phy_query_range_row_count_;
  double opt_logical_query_range_row_count = est_cost_info_.logical_query_range_row_count_;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else if (OB_FAIL(check_adj_index_cost_valid(stats_phy_query_range_row_count,
                                                stats_logical_query_range_row_count,
                                                opt_stats_cost_percent,
                                                adj_cost_is_valid))) {
    LOG_WARN("failed to check adj index cost valid", K(ret));
  } else {
    ENABLE_OPT_TRACE_COST_MODEL;
    OPT_TRACE_COST_MODEL("calc cost for index:", index_id_);
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    if (OB_FAIL(ObOptEstCost::cost_table(est_cost_info_,
                                        parallel_,
                                        storage_est_cost,
                                        opt_ctx))) {
      LOG_WARN("failed to do cost table", K(ret), K(est_cost_info_), K(parallel_));
    } else if (!adj_cost_is_valid) {
      cost_ = storage_est_cost;
      OPT_TRACE_COST_MODEL(KV_(cost), "=", KV(storage_est_cost));
    } else if (OB_FALSE_IT(est_cost_info_.phy_query_range_row_count_ = stats_phy_query_range_row_count)) {
    } else if (OB_FALSE_IT(est_cost_info_.logical_query_range_row_count_ = stats_logical_query_range_row_count)) {
    } else if (OB_FAIL(ObOptEstCost::cost_table(est_cost_info_,
                                                parallel_,
                                                stats_est_cost,
                                                opt_ctx))) {
      LOG_WARN("failed to get index access info", K(ret));
    } else {
      double rate = opt_stats_cost_percent * 1.0 / 100.0;
      cost_ = storage_est_cost * (1-rate) + stats_est_cost * rate;
      OPT_TRACE_COST_MODEL(KV_(cost), "=", KV(storage_est_cost), "* (1-", KV(rate), ") +", KV(stats_est_cost), "*", KV(rate));
      est_cost_info_.phy_query_range_row_count_ = opt_phy_query_range_row_count;
      est_cost_info_.logical_query_range_row_count_ = opt_logical_query_range_row_count;
    }
    DISABLE_OPT_TRACE_COST_MODEL;
  }
  return ret;
}

int AccessPath::re_estimate_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  card = get_path_output_rows();
  ObOptimizerContext *opt_ctx = NULL;
  double stats_phy_query_range_row_count = 0;
  double stats_logical_query_range_row_count = 0;
  int64_t opt_stats_cost_percent = 0;
  bool adj_cost_is_valid = false;
  double storage_est_cost = 0.0;
  double stats_est_cost = 0.0;
  double storage_est_card = card;
  double stats_est_card = card;
  double opt_phy_query_range_row_count = est_cost_info_.phy_query_range_row_count_;
  double opt_logical_query_range_row_count = est_cost_info_.logical_query_range_row_count_;
  param.need_parallel_ = (ObGlobalHint::UNSET_PARALLEL == param.need_parallel_ || is_match_all())
                         ? parallel_ : param.need_parallel_;
  est_cost_info_.rescan_left_server_list_ = param.rescan_left_server_list_;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()) ||
      OB_ISNULL(opt_ctx = &parent_->get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(opt_ctx), K(ret));
  } else if (OB_FAIL(check_adj_index_cost_valid(stats_phy_query_range_row_count,
                                                stats_logical_query_range_row_count,
                                                opt_stats_cost_percent,
                                                adj_cost_is_valid))) {
    LOG_WARN("failed to check adj index cost valid", K(ret));
  } else if (OB_FAIL(re_estimate_cost(param, est_cost_info_, sample_info_,
                                      *opt_ctx,
                                      can_batch_rescan_,
                                      storage_est_card,
                                      storage_est_cost))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else if (!adj_cost_is_valid) {
    est_cost_info_.rescan_left_server_list_ = NULL;
    cost = storage_est_cost;
    card = storage_est_card;
    if (param.override_) {
      cost_ = cost;
    }
  } else if (OB_FALSE_IT(est_cost_info_.phy_query_range_row_count_ = stats_phy_query_range_row_count)) {
  } else if (OB_FALSE_IT(est_cost_info_.logical_query_range_row_count_ = stats_logical_query_range_row_count)) {
  } else if (OB_FAIL(re_estimate_cost(param, est_cost_info_, sample_info_,
                                      *opt_ctx,
                                      can_batch_rescan_,
                                      stats_est_card,
                                      stats_est_cost))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else {
    est_cost_info_.rescan_left_server_list_ = NULL;
    double rate = opt_stats_cost_percent * 1.0 / 100.0;
    cost = storage_est_cost * (1-rate) + stats_est_cost * rate;
    card = storage_est_card * (1-rate) + stats_est_card * rate;
    est_cost_info_.phy_query_range_row_count_ = opt_phy_query_range_row_count;
    est_cost_info_.logical_query_range_row_count_ = opt_logical_query_range_row_count;
    if (param.override_) {
      cost_ = cost;
    }
  }
  return ret;
}

int IndexMergePath::re_estimate_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  est_cost_info_.rescan_left_server_list_ = param.rescan_left_server_list_;
  cost = get_cost();
  if (param.need_row_count_ > 0) {
    card = std::min(param.need_row_count_, get_path_output_rows());
  }
  return ret;
}

int AccessPath::re_estimate_cost(const EstimateCostInfo &param,
                                 ObCostTableScanInfo &est_cost_info,
                                 const SampleInfo &sample_info,
                                 const ObOptimizerContext &opt_ctx,
                                 const bool can_batch_rescan,
                                 double &card,
                                 double &cost)
{
  int ret = OB_SUCCESS;
  const double orign_card = card;
  cost = 0;
  est_cost_info.join_filter_sel_ = 1.0;
  double table_filter_sel = est_cost_info.table_filter_sel_;
  if (OB_UNLIKELY(param.need_parallel_ < ObGlobalHint::DEFAULT_PARALLEL
                  || (!can_batch_rescan && param.need_batch_rescan_)
                  || orign_card < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(param), K(orign_card), K(can_batch_rescan));
  } else {
    est_cost_info.is_batch_rescan_ = param.need_batch_rescan_;
    //bloom filter selectivity
    for (int64_t i = 0; i < param.join_filter_infos_.count(); ++i) {
      const JoinFilterInfo &info = param.join_filter_infos_.at(i);
      if (info.table_id_ == est_cost_info.table_id_) {
        table_filter_sel *= info.join_filter_selectivity_;
        est_cost_info.join_filter_sel_ *= info.join_filter_selectivity_;
        card *= info.join_filter_selectivity_;
      }
    }
    //refine row count
    double orign_phy_query_range_row_count = est_cost_info.phy_query_range_row_count_;
    double orign_logical_query_range_row_count = est_cost_info.logical_query_range_row_count_;
    double phy_query_range_row_count = orign_phy_query_range_row_count;
    double logical_query_range_row_count = orign_logical_query_range_row_count;
    if (param.need_row_count_ >= 0) {
      if (OB_UNLIKELY(table_filter_sel <= 0.0)) {
        //do nothing
      } else if (is_virtual_table(est_cost_info.ref_table_id_)) {
        logical_query_range_row_count = static_cast<double>(param.need_row_count_) / table_filter_sel;
      } else {
        logical_query_range_row_count = static_cast<double>(param.need_row_count_) / table_filter_sel;
        if (sample_info.is_row_sample() && sample_info.percent_ > 0.0) {
          logical_query_range_row_count = static_cast<double>(logical_query_range_row_count) / sample_info.percent_;
        }
        if (est_cost_info.postfix_filter_sel_ > 0.0) {
          logical_query_range_row_count = static_cast<double>(logical_query_range_row_count) / est_cost_info.postfix_filter_sel_;
        }
        if (sample_info.is_row_sample() && sample_info.percent_ > 0.0) {
          logical_query_range_row_count = static_cast<double>(logical_query_range_row_count) / sample_info.percent_;
        }
        if (orign_logical_query_range_row_count >= OB_DOUBLE_EPSINON) {
          phy_query_range_row_count = logical_query_range_row_count * orign_phy_query_range_row_count / orign_logical_query_range_row_count;
        }
        phy_query_range_row_count = std::min(orign_phy_query_range_row_count, phy_query_range_row_count);
        logical_query_range_row_count = std::min(orign_logical_query_range_row_count, logical_query_range_row_count);
      }
      card = std::min(param.need_row_count_, card);
    }
    est_cost_info.phy_query_range_row_count_ = phy_query_range_row_count;
    est_cost_info.logical_query_range_row_count_ = logical_query_range_row_count;
    LOG_DEBUG("access path re estimate cost", K(param), K(orign_card), K(card),
                          K(orign_phy_query_range_row_count), K(orign_logical_query_range_row_count),
                          K(phy_query_range_row_count), K(logical_query_range_row_count));
    if (OB_FAIL(ObOptEstCost::cost_table(est_cost_info,
                                        param.need_parallel_,
                                        cost,
                                        opt_ctx))) {
      LOG_WARN("failed to get index access info", K(ret));
    } else {
      //restore query range row count
      est_cost_info.phy_query_range_row_count_ = orign_phy_query_range_row_count;
      est_cost_info.logical_query_range_row_count_ = orign_logical_query_range_row_count;
      est_cost_info.is_batch_rescan_ = false;
    }
  }
  return ret;
}

int AccessPath::check_adj_index_cost_valid(double &stats_phy_query_range_row_count,
                                          double &stats_logical_query_range_row_count,
                                          int64_t &opt_stats_cost_percent,
                                          bool &is_valid)const
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObQueryCtx *query_ctx = NULL;
  const OptTableMeta* table_meta = NULL;
  bool enable_adj_index_cost = false;
  opt_stats_cost_percent = 0;
  double selectivity = 0.0;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) ||
      OB_ISNULL(opt_ctx = &plan->get_optimizer_context()) ||
      OB_ISNULL(session_info = opt_ctx->get_session_info()) ||
      OB_ISNULL(query_ctx = opt_ctx->get_query_ctx()) ||
      OB_ISNULL(table_meta = plan->get_basic_table_metas().get_table_meta_by_table_id(table_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(plan), K(opt_ctx), K(query_ctx), K(ret));
  } else if (OB_FALSE_IT(opt_stats_cost_percent = opt_ctx->get_optimizer_index_cost_adj())) {
  } else if (0 == opt_stats_cost_percent || //session disable adjust
             est_cost_info_.prefix_filters_.empty() || //not have query range
             !est_cost_info_.pushdown_prefix_filters_.empty() ||  //can not use storage estimate
             table_meta->use_default_stat()) {  //not have optimzier stats
    is_valid = false;
    LOG_TRACE("disable adjust index cost", K(enable_adj_index_cost),
                                           K(est_cost_info_.prefix_filters_.empty()),
                                           K(est_cost_info_.pushdown_prefix_filters_.empty()),
                                           K(table_meta->use_default_stat()));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                            plan->get_selectivity_ctx(),
                                                            est_cost_info_.prefix_filters_,
                                                            selectivity,
                                                            plan->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else {
    stats_logical_query_range_row_count = get_table_row_count() * selectivity;
    stats_phy_query_range_row_count = stats_logical_query_range_row_count;
    is_valid = true;
    LOG_TRACE("enable adjust index cost, ", K(opt_stats_cost_percent), K(stats_logical_query_range_row_count));
  }
  return ret;
}

const ObIArray<ObNewRange>& AccessPath::get_query_ranges() const
{
  return est_cost_info_.ranges_;
}

int AccessPath::compute_valid_inner_path()
{
  int ret = OB_SUCCESS;
  is_valid_inner_path_ = false;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else {
    const ObIArray<ObRawExpr*> &filters = est_cost_info_.pushdown_prefix_filters_;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_inner_path_ && i < filters.count(); i ++) {
      const ObRawExpr *expr = filters.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!expr->has_flag(CNT_DYNAMIC_PARAM) ||
                  expr->has_flag(CNT_ONETIME)) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(parent_->get_plan()->get_pushdown_filters(), expr)) {
        is_valid_inner_path_ = true;
      }
    }
  }
  return ret;
}

int AccessPath::compute_access_path_batch_rescan()
{
  int ret = OB_SUCCESS;
  can_batch_rescan_ = false;
  ObLogPlan *plan = NULL;
  const TableItem *table_item = NULL;
  bool has_index_scan_filter = false;
  bool has_index_lookup_filter = false;
  bool can_batch_rescan = false;
  ObSEArray<ObRawExpr *, 8> non_match_filters;
  ObSEArray<ObRawExpr *, 2> match_filters;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(plan->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null log op", K(ret));
  } else if (OB_ISNULL(table_item = plan->get_stmt()->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get table item", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_match_against_filters(filter_,
                                                                  non_match_filters,
                                                                  match_filters))) {
    LOG_WARN("failed to extract ir fitler from filters", K(ret), K(filter_));
  } else if (is_virtual_table(ref_table_id_)
             || est_cost_info_.index_meta_info_.is_domain_index()
             || domain_idx_info_.has_vec_index()
             || domain_idx_info_.has_ir_scan()
             || domain_idx_info_.has_func_lookup()
             || for_update_
             || !subquery_exprs_.empty()
             || table_item->is_link_table()
             || match_filters.count() > 0
             || EXTERNAL_TABLE == table_item->table_type_
             || is_index_merge_path()) {
    can_batch_rescan = false;
  } else if (order_direction_ != default_asc_direction() && order_direction_ != ObOrderDirection::UNORDERED) {
    can_batch_rescan = false;
  } else if (est_cost_info_.index_meta_info_.is_global_index_ &&
             est_cost_info_.index_meta_info_.is_index_back_ &&
             OB_FAIL(ObOptimizerUtil::get_has_global_index_filters(plan->get_optimizer_context().get_sql_schema_guard(),
                                                                   index_id_,
                                                                   est_cost_info_.postfix_filters_,
                                                                   has_index_scan_filter,
                                                                   has_index_lookup_filter))) {
    LOG_WARN("failed to get has global index filters", K(ret));
  } else {
    // batch rescan when global lookup has index pushdown filter, enabled after 4.2.1.9.
    can_batch_rescan = !has_index_scan_filter || plan->get_optimizer_context().enable_global_index_filter_batch();
  }

  if (OB_SUCC(ret)) {
    can_batch_rescan_ = can_batch_rescan;
  }
  return ret;
}

int ObIndexMergeNode::set_scan_direction(const ObOrderDirection &direction)
{
  int ret = OB_SUCCESS;
  if (is_scan_node()) {
    ap_->order_direction_ = direction;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      ObIndexMergeNode *node = children_.at(i);
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr index merge node", K(ret));
      } else if (OB_FAIL(SMART_CALL(node->set_scan_direction(direction)))) {
        LOG_WARN("failed to set scan direction", KPC(node), K(ret));
      }
    }
  }
  return ret;
}

int ObIndexMergeNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (is_merge_node()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
      if (OB_ISNULL(children_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index merge child node", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(children_.at(i)->get_all_index_ids(index_ids)))) {
        LOG_WARN("failed to get child index ids", K(ret), KPC(children_.at(i)));
      }
    }
  } else {
    if (OB_ISNULL(ap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null access path", K(ret));
    } else if (OB_FAIL(index_ids.push_back(ap_->index_id_))) {
      LOG_WARN("failed to push back index id", K(ret));
    }
  }
  return ret;
}

int AccessPath::compute_is_das_dynamic_part_pruning(const EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr*> &src_keys,
                                                    const ObIArray<ObRawExpr*> &target_keys)
{
  int ret = OB_SUCCESS;
  if (-1 != can_das_dynamic_part_pruning_ || !use_das_) {
    /* do nothing */
  } else if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())
             || OB_ISNULL(parent_->get_plan()->get_stmt())
             || OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(parent_), K(table_partition_info_));
  } else if (!parent_->get_plan()->get_optimizer_context().enable_425_opt_batch_rescan()) {
    can_das_dynamic_part_pruning_ = false;
  } else if (table_partition_info_->get_phy_tbl_location_info().get_partition_cnt() <= 1) {
    can_das_dynamic_part_pruning_ = false;
  } else {
    ObSEArray<ObRawExpr*, 8> part_keys;
    bool is_match = false;
    if (OB_FAIL(ObShardingInfo::get_all_partition_key(parent_->get_plan()->get_optimizer_context(),
                                                      *parent_->get_plan()->get_stmt(),
                                                      table_id_,
                                                      index_id_,
                                                      part_keys))) {
      LOG_WARN("failed to get all partition key", K(ret));
    } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(equal_sets,
                                                                       src_keys,
                                                                       target_keys,
                                                                       part_keys,
                                                                       is_match))) {
      LOG_WARN("failed to check if match repartition", K(ret));
    } else {
      LOG_DEBUG("finish check is das dynamic part pruning", K(is_match), K(part_keys),
                                                            K(src_keys), K(target_keys));
      can_das_dynamic_part_pruning_ = is_match;
    }
  }
  return ret;
}

int FunctionTablePath::assign(const FunctionTablePath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else {
    table_id_ = other.table_id_;
    value_expr_ = other.value_expr_;
  }
  return ret;
}

int FunctionTablePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  op_cost_ = 1.0;
  cost_ = 1.0;
  return  ret;
}

int JsonTablePath::assign(const JsonTablePath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else if (OB_FAIL(value_exprs_.assign(other.value_exprs_))) {
    LOG_WARN("fail to assgin value expr", K(ret));
  } else if (OB_FAIL(column_param_default_exprs_.assign(other.column_param_default_exprs_))) {
    LOG_WARN("fail to assgin default expr", K(ret));
  } else {
    table_id_ = other.table_id_;
  }
  return ret;
}

int JsonTablePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  op_cost_ = 1.0;
  cost_ = 1.0;
  return  ret;
}

int TempTablePath::assign(const TempTablePath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else {
    table_id_ = other.table_id_;
    temp_table_id_ = other.temp_table_id_;
    root_ = other.root_;
  }
  return ret;
}

int TempTablePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_) || OB_ISNULL(parent_) ||
      OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    double per_dop_card = root_->get_card() / parallel_;
    op_cost_ = ObOptEstCost::cost_read_materialized(per_dop_card, opt_ctx) +
               ObOptEstCost::cost_quals(per_dop_card, filter_, opt_ctx);
    cost_ = op_cost_;
  }
  return ret;
}

int TempTablePath::re_estimate_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  card = get_path_output_rows();
  cost = cost_;
  const int64_t parallel = ObGlobalHint::UNSET_PARALLEL == param.need_parallel_
                           ? parallel_ : param.need_parallel_;
  if (OB_ISNULL(root_) || OB_ISNULL(parent_) || OB_UNLIKELY(parallel < 1)
      || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(root_), K(parent_), K(parallel), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    double read_card = root_->get_card();
    //bloom filter selectivity
    for (int64_t i = 0; i < param.join_filter_infos_.count(); ++i) {
      const JoinFilterInfo &info = param.join_filter_infos_.at(i);
      if (info.table_id_ == table_id_) {
        card *= info.join_filter_selectivity_;
      }
    }
    //refine row count
    if (param.need_row_count_ >= 0) {
      if (param.need_row_count_ >= get_path_output_rows()) {
        //do nothing
      } else {
        read_card = read_card / card * param.need_row_count_;
        card = param.need_row_count_;
      }
    }
    double per_dop_card = read_card / parallel;
    cost = ObOptEstCost::cost_read_materialized(per_dop_card, opt_ctx) +
                ObOptEstCost::cost_quals(per_dop_card, filter_, opt_ctx);
    if (param.override_) {
      cost_ = cost;
      op_cost_ = cost;
    }
  }
  return ret;
}

int TempTablePath::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else if (is_distributed()) {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    strong_sharding_ = opt_ctx.get_distributed_sharding();
    weak_sharding_.reset();
  } else if (is_match_all()) {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    strong_sharding_ = opt_ctx.get_local_sharding();
    weak_sharding_.reset();
  }
  return  ret;
}

int TempTablePath::compute_path_ordering()
{
  int ret = OB_SUCCESS;
  ordering_.reuse();
  is_local_order_ = false;
  is_range_order_ = false;
  return  ret;
}

int CteTablePath::assign(const CteTablePath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else {
    table_id_ = other.table_id_;
    ref_table_id_ = other.ref_table_id_;
  }
  return ret;
}

int CteTablePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  op_cost_ = 1.0;
  cost_ = 1.0;
  return ret;
}

int JoinPath::assign(const JoinPath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  left_path_ = other.left_path_;
  right_path_ = other.right_path_;
  join_algo_ = other.join_algo_;
  join_dist_algo_ = other.join_dist_algo_;
  is_slave_mapping_ = other.is_slave_mapping_;
  join_type_ = other.join_type_;
  need_mat_ = other.need_mat_;
  left_need_sort_ = other.left_need_sort_;
  left_prefix_pos_ = other.left_prefix_pos_;
  right_need_sort_ = other.right_need_sort_;
  right_prefix_pos_ = other.right_prefix_pos_;
  equal_cond_sel_ = other.equal_cond_sel_;
  other_cond_sel_ = other.other_cond_sel_;
  contain_normal_nl_ = other.contain_normal_nl_;
  has_none_equal_join_ = other.has_none_equal_join_;
  can_use_batch_nlj_ = other.can_use_batch_nlj_;
  is_naaj_ = other.is_naaj_;
  is_sna_ = other.is_sna_;
  inherit_sharding_index_ = other.inherit_sharding_index_;
  join_output_rows_ = other.join_output_rows_;

  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else if (OB_FAIL(left_sort_keys_.assign(other.left_sort_keys_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(right_sort_keys_.assign(other.right_sort_keys_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(merge_directions_.assign(other.merge_directions_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(equal_join_conditions_.assign(other.equal_join_conditions_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(other_join_conditions_.assign(other.other_join_conditions_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(join_filter_infos_.assign(other.join_filter_infos_))) {
    LOG_WARN("failed to assign array", K(ret));
  }
  return ret;
}

int JoinPath::compute_join_path_sharding()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_) ||
      OB_ISNULL(parent_) || OB_ISNULL(parent_->get_allocator()) ||
      OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(parent_), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    if (is_slave_mapping_) {
      if (DistAlgo::DIST_PARTITION_WISE == join_dist_algo_) {
        if (OB_FAIL(compute_hash_hash_sharding_info())) {
          LOG_WARN("failed to generate hash-hash sharding info", K(ret));
        } else { /*do nothing*/ }
      } else {
        strong_sharding_ = opt_ctx.get_distributed_sharding();
      }
    } else if (DistAlgo::DIST_BASIC_METHOD == join_dist_algo_) {
      ObSEArray<ObShardingInfo*, 2> input_shardings;
      if (OB_ISNULL(left_path_->strong_sharding_) || OB_ISNULL(right_path_->strong_sharding_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_path_->strong_sharding_),
            K(right_path_->strong_sharding_), K(ret));
      } else if (OB_FAIL(input_shardings.push_back(left_path_->strong_sharding_)) ||
                 OB_FAIL(input_shardings.push_back(right_path_->strong_sharding_))) {
        LOG_WARN("failed to input shardings", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_basic_sharding_info(
                                              opt_ctx.get_local_server_addr(),
                                              input_shardings,
                                              *parent_->get_allocator(),
                                              strong_sharding_,
                                              inherit_sharding_index_))) {
        LOG_WARN("failed to compute basic sharding info", K(ret));
      }
    } else if (DistAlgo::DIST_PULL_TO_LOCAL == join_dist_algo_) {
      strong_sharding_ = opt_ctx.get_local_sharding();
    } else if (DistAlgo::DIST_NONE_ALL == join_dist_algo_) {
      strong_sharding_ = left_path_->strong_sharding_;
      inherit_sharding_index_ = 0;
      if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_))) {
        LOG_WARN("failed to append weak sharding", K(ret));
      } else { /*do nothing*/ }
    } else if (DistAlgo::DIST_ALL_NONE == join_dist_algo_) {
      strong_sharding_ = right_path_->strong_sharding_;
      inherit_sharding_index_ = 1;
      if (OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
        LOG_WARN("failed to append weak sharding", K(ret));
      } else { /*do nothing*/ }
    } else if (DistAlgo::DIST_RANDOM_ALL == join_dist_algo_) {
      strong_sharding_ = opt_ctx.get_distributed_sharding();
      inherit_sharding_index_ = -1;
    } else if (DistAlgo::DIST_PARTITION_WISE == join_dist_algo_ ||
               DistAlgo::DIST_EXT_PARTITION_WISE == join_dist_algo_) {
      if (LEFT_OUTER_JOIN == join_type_) {
        strong_sharding_ = left_path_->strong_sharding_;
        inherit_sharding_index_ = 0;
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_)) ||
            OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else if (NULL != right_path_->strong_sharding_ &&
                  OB_FAIL(weak_sharding_.push_back(right_path_->strong_sharding_))) {
          LOG_WARN("failed to push back sharding", K(ret));
        } else { /*do nothing*/ }
      } else if (RIGHT_OUTER_JOIN == join_type_) {
        strong_sharding_ = right_path_->strong_sharding_;
        inherit_sharding_index_ = 1;
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_)) ||
            OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else if (NULL != left_path_->strong_sharding_ &&
                   OB_FAIL(weak_sharding_.push_back(left_path_->strong_sharding_))) {
          LOG_WARN("failed to push back sharding", K(ret));
        } else { /*do nothing*/ }
      } else if (FULL_OUTER_JOIN == join_type_) {
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_)) ||
            OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else if (NULL != left_path_->strong_sharding_ &&
                   OB_FAIL(weak_sharding_.push_back(left_path_->strong_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else if (NULL != right_path_->strong_sharding_ &&
                   OB_FAIL(weak_sharding_.push_back(right_path_->strong_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      } else if (LEFT_SEMI_JOIN == join_type_ ||
                 LEFT_ANTI_JOIN == join_type_ ||
                 INNER_JOIN == join_type_) {
        strong_sharding_ = left_path_->strong_sharding_;
        inherit_sharding_index_ = 0;
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      } else {
        strong_sharding_ = right_path_->strong_sharding_;
        inherit_sharding_index_ = 1;
        if (OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (DistAlgo::DIST_PARTITION_NONE == join_dist_algo_ ||
               DistAlgo::DIST_HASH_NONE == join_dist_algo_) {
      if (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_) {
        if (OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append append weak sharding", K(ret));
        } else if (NULL != right_path_->strong_sharding_ &&
                   OB_FAIL(weak_sharding_.push_back(right_path_->strong_sharding_))) {
          LOG_WARN("failed to push back sharding", K(ret));
        } else { /*do nothing*/ }
      } else {
        strong_sharding_ = right_path_->strong_sharding_;
        inherit_sharding_index_ = 1;
        if (OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo_ ||
        (DistAlgo::DIST_BC2HOST_NONE == join_dist_algo_ && JoinAlgo::HASH_JOIN == join_algo_)) {
      if (right_path_->parallel_more_than_part_cnt()) {
        //If the degree of parallelism is greater than the number of partitions,
        //sharding will not be inherited to avoid thread waste.
        strong_sharding_ = opt_ctx.get_distributed_sharding();
      } else {
        strong_sharding_ = right_path_->strong_sharding_;
        inherit_sharding_index_ = 1;
        if (OB_FAIL(append(weak_sharding_, right_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (DistAlgo::DIST_NONE_PARTITION == join_dist_algo_ ||
               DistAlgo::DIST_NONE_HASH == join_dist_algo_) {
      if (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_) {
        if (OB_FAIL(append(weak_sharding_, left_path_->get_weak_sharding()))) {
          LOG_WARN("failed to push back weak sharding", K(ret));
        } else if (NULL != left_path_->strong_sharding_ &&
                   OB_FAIL(weak_sharding_.push_back(left_path_->strong_sharding_))) {
          LOG_WARN("failed to push back sharding info", K(ret));
        } else { /*do nothing*/ }
      } else {
        strong_sharding_ = left_path_->strong_sharding_;
        inherit_sharding_index_ = 0;
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (DistAlgo::DIST_NONE_BROADCAST == join_dist_algo_) {
      if (left_path_->parallel_more_than_part_cnt()) {
        //If the degree of parallelism is greater than the number of partitions,
        //sharding will not be inherited to avoid thread waste.
        strong_sharding_ = opt_ctx.get_distributed_sharding();
      } else {
        strong_sharding_ = left_path_->strong_sharding_;
        inherit_sharding_index_ = 0;
        if (OB_FAIL(append(weak_sharding_, left_path_->weak_sharding_))) {
          LOG_WARN("failed to append weak sharding", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (DistAlgo::DIST_HASH_HASH == join_dist_algo_) {
      if (OB_FAIL(compute_hash_hash_sharding_info())) {
        LOG_WARN("failed to generate hash-hash sharding info", K(ret));
      } else { /*do nothing*/ }
    } else if (DistAlgo::DIST_BC2HOST_NONE == join_dist_algo_) {
      if (right_path_->is_single()) {
        strong_sharding_ = right_path_->strong_sharding_;
        inherit_sharding_index_ = 1;
      } else {
        strong_sharding_ = opt_ctx.get_distributed_sharding();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(join_dist_algo_), K(ret));
    }
  }
  return ret;
}

int JoinPath::compute_join_path_property()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(compute_join_path_sharding())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else if (OB_FAIL(compute_join_path_plan_type())) {
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (OB_FAIL(compute_join_path_info())) {
    LOG_WARN("failed to check and replace aggr exprs", K(ret));
  } else if (OB_FAIL(compute_join_path_ordering())) {
    LOG_WARN("failed to compute op ordering", K(ret));
  } else if (OB_FAIL(compute_join_path_parallel_and_server_info())) {
    LOG_WARN("failed to compute server info", K(ret));
  } else if OB_FAIL(compute_nlj_batch_rescan()) {
    LOG_WARN("failed to compute nlj batch rescan", K(ret));
  } else if (OB_FAIL(estimate_cost())) {
    LOG_WARN("failed to calculate cost in create_ml_path", K(ret));
  } else if (OB_FAIL(compute_pipeline_info())) {
    LOG_WARN("failed to compute pipelined path", K(ret));
  } else if (OB_FAIL(check_is_contain_normal_nl())) {
    LOG_WARN("failed to check is contain normal nl", K(ret));
  } else {
    LOG_TRACE("succeed to compute join path property");
  }
  return ret;
}

int JoinPath::compute_join_path_ordering()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_) ||
      OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(parent_), K(ret));
  } else if (JoinAlgo::MERGE_JOIN == join_algo_) {
    if (FULL_OUTER_JOIN != join_type_ && RIGHT_OUTER_JOIN != join_type_) {
      // 目前 ObMergeJoin 的实现只能继承左支的序
      if (!is_left_need_sort() && !is_left_need_exchange()) {
        set_interesting_order_info(left_path_->get_interesting_order_info());
        if(OB_FAIL(append(ordering_, left_path_->ordering_))) {
          LOG_WARN("failed to append join ordering", K(ret));
        } else if (OB_FAIL(parent_->check_join_interesting_order(this))) {
          LOG_WARN("failed to update join interesting order info", K(ret));
        } else {
          is_range_order_ = left_path_->is_range_order_;
          is_local_order_ = left_path_->is_local_order_;
        }
      } else {
        int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
        if (OB_FAIL(append(ordering_, left_sort_keys_))) {
          LOG_WARN("failed to append join ordering", K(ret));
        } else if (OB_FAIL(parent_->check_all_interesting_order(get_ordering(),
                                                                parent_->get_plan()->get_stmt(),
                                                                interesting_order_info))) {
          LOG_WARN("failed to check all interesting order", K(ret));
        } else {
          add_interesting_order_flag(interesting_order_info);
          is_local_order_ = is_fully_partition_wise() || join_dist_algo_ == DIST_NONE_ALL;
        }
      }
    } else { /*do nothing*/ }
  } else if (JoinAlgo::NESTED_LOOP_JOIN == join_algo_ && !left_path_->ordering_.empty()
             && CONNECT_BY_JOIN != join_type_) {
    set_interesting_order_info(left_path_->get_interesting_order_info());
    if (OB_FAIL(append(ordering_, left_path_->ordering_))) {
      LOG_WARN("failed to append ordering", K(ret));
    } else if (OB_FAIL(parent_->check_join_interesting_order(this))) {
      LOG_WARN("failed to update join interesting order info", K(ret));
    } else if (!is_left_need_exchange() || left_path_->is_single() || left_path_->is_local_order_) {
      is_range_order_ = left_path_->is_range_order_;
      is_local_order_ = left_path_->is_local_order_;
    } else {
      is_local_order_ = true;
      is_range_order_ = false;
    }
  } else { /*do nothing*/ }
  return ret;
}

int JoinPath::compute_join_path_plan_type()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(ret));
  } else if (NULL != strong_sharding_ && strong_sharding_->get_can_reselect_replica()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
  } else if (is_local()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_LOCAL;
  } else if (is_remote()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_REMOTE;
  } else if (is_distributed()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
  } else {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
  }
  if (OB_SUCC(ret)) {
    if (is_left_need_exchange() || is_right_need_exchange() ||
        left_path_->exchange_allocated_ || right_path_->exchange_allocated_) {
      exchange_allocated_ = true;
      phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
    }
    if (ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == left_path_->phy_plan_type_ ||
        ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == right_path_->phy_plan_type_) {
      phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
    }
    if (ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN == left_path_->location_type_ ||
        ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN == right_path_->location_type_) {
      location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
    }
    LOG_TRACE("succeed to compute join path type", K(phy_plan_type_), K(location_type_));
  }
  return ret;
}

int JoinPath::compute_hash_hash_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = NULL;
  ObIAllocator *allocator = NULL;
  if (OB_ISNULL(left_path_) || OB_ISNULL(left_path_->parent_) || OB_ISNULL(parent_) ||
      OB_ISNULL(allocator = left_path_->parent_->get_allocator()) ||
      OB_ISNULL(log_plan = left_path_->parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(allocator), K(log_plan), K(ret));
  } else if (OB_UNLIKELY(JoinAlgo::NESTED_LOOP_JOIN == join_algo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    bool use_left = false;
    bool use_right = false;
    ObSEArray<ObRawExpr*, 8> left_join_exprs;
    ObSEArray<ObRawExpr*, 8> right_join_exprs;
    switch (join_type_) {
      case INNER_JOIN:
        use_left = true;
        use_right = true;
        break;
      case LEFT_OUTER_JOIN:
      case LEFT_SEMI_JOIN:
      case LEFT_ANTI_JOIN:
        use_left = true;
        break;
      case RIGHT_OUTER_JOIN:
      case RIGHT_SEMI_JOIN:
      case RIGHT_ANTI_JOIN:
        use_right = true;
        break;
      default:
        break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < equal_join_conditions_.count(); i++) {
      ObRawExpr *join_expr = NULL;
      ObRawExpr *left_expr = NULL;
      ObRawExpr *right_expr = NULL;
      if (OB_ISNULL(join_expr = equal_join_conditions_.at(i)) ||
          OB_ISNULL(left_expr = join_expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = join_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(join_expr), K(left_expr), K(right_expr), K(ret));
      } else {
        if (!left_expr->get_relation_ids().is_subset(left_path_->parent_->get_tables())) {
          std::swap(left_expr, right_expr);
        }
        if (OB_FAIL(left_join_exprs.push_back(left_expr)) ||
            OB_FAIL(right_join_exprs.push_back(right_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          ObExprCalcType result_type = join_expr->get_result_type().get_calc_meta();
          if (!ObSQLUtils::is_same_type_for_compare(left_expr->get_result_type(),
                                                    result_type)) {
            use_left = false;
          }
          if (!ObSQLUtils::is_same_type_for_compare(right_expr->get_result_type(),
                                                    result_type)) {
            use_right = false;
          }
        }
      }
      // determine if hybrid hash dm can be enabled
      // should check is_naaj - #issue/46230785
      if (OB_SUCC(ret) && !is_naaj_ && 1 == equal_join_conditions_.count()) {
        ObArray<ObObj> popular_values;
        if (OB_FAIL(log_plan->check_if_use_hybrid_hash_distribution(
                    log_plan->get_optimizer_context(),
                    log_plan->get_stmt(),
                    join_type_,
                    *right_expr,
                    popular_values))) {
          LOG_WARN("fail check if use hybrid hash distribution", K(ret));
        } else if (popular_values.count() > 0) {
          use_hybrid_hash_dm_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (use_hybrid_hash_dm_) {
        // fix issue/45941566
        strong_sharding_ = log_plan->get_optimizer_context().get_distributed_sharding();
      } else if (use_left || use_right) {
        ObShardingInfo *target_sharding = NULL;
        if (use_left && OB_FAIL(log_plan->get_cached_hash_sharding_info(left_join_exprs,
                                                                        parent_->get_output_equal_sets(),
                                                                        target_sharding))) {
          LOG_WARN("failed to get cached sharding info", K(ret));
        } else if (use_right && NULL == target_sharding &&
                   OB_FAIL(log_plan->get_cached_hash_sharding_info(right_join_exprs,
                                                                   parent_->get_output_equal_sets(),
                                                                   target_sharding))) {
          LOG_WARN("failed to get cached sharding info", K(ret));
        } else if (NULL != target_sharding) {
          strong_sharding_ = target_sharding;
        } else if (OB_ISNULL(target_sharding = reinterpret_cast<ObShardingInfo*>(
                                        allocator->alloc(sizeof(ObShardingInfo))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          target_sharding = new (target_sharding) ObShardingInfo();
          target_sharding->set_distributed();
          if (use_left) {
            ret = target_sharding->get_partition_keys().assign(left_join_exprs);
          } else if (use_right) {
            ret = target_sharding->get_partition_keys().assign(right_join_exprs);
          } else { /*do nothing*/ }
          if (OB_FAIL(ret)) {
            /*do nothing*/
          } else if (OB_FAIL(log_plan->get_hash_dist_info().push_back(target_sharding))) {
            LOG_WARN("failed to push back sharding info", K(ret));
          } else {
            strong_sharding_ = target_sharding;
          }
        }
      } else {
        strong_sharding_ = log_plan->get_optimizer_context().get_distributed_sharding();
      }
    }
  }
  return ret;
}

int JoinPath::compute_join_path_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  if (OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else if (OB_FALSE_IT(opt_ctx = &parent_->get_plan()->get_optimizer_context())) {
  } else if (OB_FAIL(compute_join_path_parallel_and_server_info(opt_ctx,
                                                                left_path_,
                                                                right_path_,
                                                                join_dist_algo_,
                                                                join_algo_,
                                                                is_slave_mapping_,
                                                                parallel_,
                                                                available_parallel_,
                                                                server_cnt_,
                                                                server_list_))) {
    LOG_WARN("failed to compute server info", K(ret));
  }

  return ret;
}

int JoinPath::compute_join_path_parallel_and_server_info(ObOptimizerContext *opt_ctx,
                                                         const Path *left_path,
                                                         const Path *right_path,
                                                         const DistAlgo join_dist_algo,
                                                         const JoinAlgo join_algo,
                                                         bool const is_slave_mapping,
                                                         int64_t &parallel,
                                                         int64_t &available_parallel,
                                                         int64_t &server_cnt,
                                                         ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  parallel = ObGlobalHint::DEFAULT_PARALLEL;
  available_parallel = ObGlobalHint::DEFAULT_PARALLEL;
  int64_t px_expected_work_count = 0;
  const common::ObAddr &local_server_addr = opt_ctx->get_local_server_addr();
  server_cnt = 0;
  server_list.reuse();
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(right_path), K(right_path), K(ret));
  } else {
    LOG_TRACE("compute join path parallel and server info", K(join_dist_algo),
                              K(left_path->parallel_), K(right_path->parallel_),
                              K(left_path->is_single()), K(right_path->is_single()));
    const bool has_nl_param = right_path->is_inner_path() && !right_path->nl_params_.empty();
    if (is_slave_mapping) {
      const Path *inherit_child = (has_nl_param || left_path->parallel_ > right_path->parallel_)
                                  ? left_path : right_path;
      parallel = inherit_child->parallel_;
      server_cnt = inherit_child->server_cnt_;
      if (OB_FAIL(server_list.assign(inherit_child->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_BASIC_METHOD == join_dist_algo) {
      parallel = 1;
      server_cnt = 1;
      available_parallel = std::max(left_path->available_parallel_, right_path->available_parallel_);
      if (OB_FAIL(server_list.assign(left_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_PULL_TO_LOCAL == join_dist_algo) {
      parallel = 1;
      server_cnt = 1;
      const int64_t left_parallel = std::max(left_path->parallel_, left_path->available_parallel_);
      const int64_t right_parallel = std::max(right_path->parallel_, right_path->available_parallel_);
      available_parallel = std::max(left_parallel, right_parallel);
      if (OB_FAIL(server_list.push_back(local_server_addr))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_NONE_ALL == join_dist_algo) {
      parallel = left_path->parallel_;
      server_cnt = left_path->server_cnt_;
      if (OB_FAIL(server_list.assign(left_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_RANDOM_ALL == join_dist_algo) {
      common::ObAddr all_server_list;
      // like hash_hash, a special ALL server list indicating we would use all servers of this sql relate
      all_server_list.set_max();
      if (OB_FAIL(server_list.push_back(all_server_list))) {
        LOG_WARN("failed to assign all server list", K(ret));
      } else {
        parallel = left_path->parallel_;
        server_cnt = left_path->server_cnt_;
      }
    } else if (DistAlgo::DIST_ALL_NONE == join_dist_algo) {
      parallel = right_path->parallel_;
      server_cnt = right_path->server_cnt_;
      if (OB_FAIL(server_list.assign(right_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_PARTITION_WISE == join_dist_algo) {
      const Path *inherit_child = (has_nl_param || left_path->parallel_ >= right_path->parallel_)
                                  ? left_path : right_path;
      parallel = inherit_child->parallel_;
      server_cnt = inherit_child->server_cnt_;
      const ObShardingInfo *sharding = NULL;
      int64_t part_cnt = 0;
      if (OB_ISNULL(sharding = inherit_child->try_get_sharding_with_table_location())
          || OB_UNLIKELY((part_cnt = sharding->get_part_cnt()) <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected partition wise", K(ret), K(parallel), KPC(sharding), K(part_cnt));
      } else if (OB_FAIL(server_list.assign(inherit_child->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      } else {
        parallel = parallel > part_cnt ? part_cnt : parallel;
      }
    } else if (DistAlgo::DIST_EXT_PARTITION_WISE == join_dist_algo) {
      const Path *inherit_child = (has_nl_param || left_path->parallel_ >= right_path->parallel_)
                                  ? left_path : right_path;
      parallel = inherit_child->parallel_;
      server_cnt = inherit_child->server_cnt_;
      if (OB_FAIL(server_list.assign(inherit_child->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo
               || DistAlgo::DIST_BC2HOST_NONE == join_dist_algo) {
      parallel = right_path->parallel_;
      server_cnt = right_path->server_cnt_;
      if (OB_FAIL(server_list.assign(right_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_NONE_BROADCAST == join_dist_algo) {
      parallel = left_path->parallel_;
      server_cnt = left_path->server_cnt_;
      if (OB_FAIL(server_list.assign(left_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_HASH_HASH == join_dist_algo) {
      common::ObAddr all_server_list;
      // a special ALL server list indicating hash-hash data distribution
      all_server_list.set_max();
      if (OB_FAIL(server_list.push_back(all_server_list))) {
        LOG_WARN("failed to assign all server list", K(ret));
      } else if (left_path->parallel_ >= right_path->parallel_) {
        parallel = left_path->parallel_;
        server_cnt = left_path->server_cnt_;
      } else {
        parallel = right_path->parallel_;
        server_cnt = right_path->server_cnt_;
      }
    } else if (DistAlgo::DIST_PARTITION_NONE == join_dist_algo) {
      parallel = right_path->parallel_;
      server_cnt = right_path->server_cnt_;
      if (OB_FAIL(server_list.assign(right_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      } else if (OB_ISNULL(right_path->strong_sharding_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(right_path->strong_sharding_));
      } else {
        int64_t part_cnt = right_path->strong_sharding_->get_part_cnt();
        parallel = parallel > part_cnt ? part_cnt : parallel;
      }
    } else if (DistAlgo::DIST_HASH_NONE == join_dist_algo) {
      parallel = right_path->parallel_;
      server_cnt = right_path->server_cnt_;
      if (OB_FAIL(server_list.assign(right_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else if (DistAlgo::DIST_NONE_PARTITION == join_dist_algo) {
      parallel = left_path->parallel_;
      server_cnt = left_path->server_cnt_;
      if (OB_FAIL(server_list.assign(left_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      } else if (OB_ISNULL(left_path->strong_sharding_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(left_path->strong_sharding_));
      } else {
        int64_t part_cnt = left_path->strong_sharding_->get_part_cnt();
        parallel = parallel > part_cnt ? part_cnt : parallel;
      }
    } else if (DistAlgo::DIST_NONE_HASH == join_dist_algo) {
      parallel = left_path->parallel_;
      server_cnt = left_path->server_cnt_;
      if (OB_FAIL(server_list.assign(left_path->server_list_))) {
        LOG_WARN("failed to assign server list", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(join_dist_algo), K(ret));
    }
  }
  return ret;
}

int JoinPath::compute_nlj_batch_rescan()
{
  int ret = OB_SUCCESS;
  can_use_batch_nlj_ = false;
  ObLogPlan *plan = NULL;
  bool use_batch_nlj = false;
  bool right_has_gi_or_exchange = false;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(parent_), K(plan), K(right_path_));
  } else if (!plan->get_optimizer_context().enable_nlj_batch_rescan()) {
    /* do nothing */
  } else if (plan->get_disable_child_batch_rescan()) {
    /* disabled by upper log_plan */
  } else if (NESTED_LOOP_JOIN != join_algo_ || !is_nlj_with_param_down()
             || CONNECT_BY_JOIN == join_type_) {
    /* join type not support */
  } else if (!right_path_->subquery_exprs_.empty()) {
    /* subplan filter allocated for on condition subquery, not support */
  } else if (IS_SEMI_ANTI_JOIN(join_type_) && !plan->get_optimizer_context().enable_semi_anti_join_batch()) {
    /* semi/anti join batch, enabled after 4.2.5 */
  } else if (OB_FAIL(check_right_has_gi_or_exchange(right_has_gi_or_exchange))) {
    LOG_WARN("failed to check right has gi or exchange", K(ret));
  } else if (right_has_gi_or_exchange) {
    /* do nothing */
  } else if (right_path_->is_access_path()) {
    const AccessPath *ap = static_cast<const AccessPath*>(right_path_);
    can_use_batch_nlj_ = ap->can_batch_rescan_;
    if (can_use_batch_nlj_ && !plan->get_optimizer_context().enable_non_prefix_exec_param_batch()) {
      if (OB_FAIL(ObOptimizerUtil::check_exec_param_filter_exprs(ap->est_cost_info_.pushdown_prefix_filters_,
                                                                 right_path_->nl_params_,
                                                                 can_use_batch_nlj_))) {
        LOG_WARN("failed to check exec param filter exprs", K(ret));
      }
    }
  } else if (!right_path_->is_subquery_path()) {
    /* do nothing, only access_path and subquery_path may use batch nlj */
  } else if (plan->get_optimizer_context().enable_425_exec_batch_rescan() &&
             OB_FAIL(ObOptimizerUtil::check_can_batch_rescan(static_cast<const SubQueryPath*>(right_path_)->root_,
                                                             right_path_->nl_params_,
                                                             true,
                                                             can_use_batch_nlj_))) {
    LOG_WARN("failed to check plan can batch rescan", K(ret));
  } else if (!plan->get_optimizer_context().enable_425_exec_batch_rescan() &&
             OB_FAIL(ObOptimizerUtil::check_can_batch_rescan_compat(static_cast<const SubQueryPath*>(right_path_)->root_,
                                                                    right_path_->nl_params_,
                                                                    true,
                                                                    can_use_batch_nlj_))) {
    LOG_WARN("failed to check plan can batch rescan", K(ret));
  }
  return ret;
}

int JoinPath::check_right_has_gi_or_exchange(bool &right_has_gi_or_exchange)
{
  int ret = OB_SUCCESS;
  right_has_gi_or_exchange = true;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_path_), K(right_path_));
  } else if (DistAlgo::DIST_BASIC_METHOD == join_dist_algo_
             || DistAlgo::DIST_NONE_ALL == join_dist_algo_
	     || DistAlgo::DIST_RANDOM_ALL == join_dist_algo_) {
    right_has_gi_or_exchange = false;
  } else if (DistAlgo::DIST_PARTITION_WISE == join_dist_algo_ && !is_slave_mapping_
             && !left_path_->exchange_allocated_
             && !right_path_->exchange_allocated_) {
    right_has_gi_or_exchange = false;
  } else if (DistAlgo::DIST_BC2HOST_NONE == join_dist_algo_ && right_path_->is_single()) {
    right_has_gi_or_exchange = false;
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == join_dist_algo_ && !is_right_need_exchange()) {
    right_has_gi_or_exchange = false;
  } else {
    right_has_gi_or_exchange = true;
  }
  return ret;
}

int JoinPath::check_right_is_local_scan(int64_t &local_scan_type) const
{
  int ret = OB_SUCCESS;
  local_scan_type = 0;  // 0: dist scan, 1: local das scan, 2: local scan
  bool contain_dist_das = false;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_path_), K(right_path_));
  } else if (is_right_need_exchange() || right_path_->exchange_allocated_) {
    local_scan_type = 0;
  } else if (!right_path_->contain_das_op_) {
    local_scan_type = 2;
  } else if (1 != left_path_->get_server_list().count()
             || ObShardingInfo::is_shuffled_server_list(left_path_->get_server_list())) {
    local_scan_type = 0;
  } else if (OB_FAIL(check_contain_dist_das(left_path_->get_server_list(), right_path_, contain_dist_das))) {
    LOG_WARN("failed to check contain dist das", K(ret));
  } else if (contain_dist_das) {
    local_scan_type = 0;
  } else {
    local_scan_type = 1;
  }
  return ret;
}

int JoinPath::check_contain_dist_das(const ObIArray<ObAddr> &exec_server_list,
                                     const Path *cur_path,
                                     bool &contain_dist_das) const
{
  int ret = OB_SUCCESS;
  contain_dist_das = false;
  if (OB_ISNULL(cur_path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(cur_path));
  } else if (!cur_path->contain_das_op()) {
    /* do nothing */
  } else if (NULL != log_op_) {
    ret = log_op_->check_contain_dist_das(exec_server_list, contain_dist_das);
  } else if (cur_path->is_join_path()) {
    const JoinPath *join_path = static_cast<const JoinPath*>(cur_path);
    if (OB_FAIL(SMART_CALL(check_contain_dist_das(exec_server_list, join_path->left_path_, contain_dist_das)))) {
      LOG_WARN("failed to check contain dist das", K(ret));
    } else if (!contain_dist_das &&
               OB_FAIL(SMART_CALL(check_contain_dist_das(exec_server_list, join_path->right_path_, contain_dist_das)))) {
      LOG_WARN("failed to check contain dist das", K(ret));
    }
  } else if (cur_path->is_access_path()) {
    const AccessPath *access_path = static_cast<const AccessPath*>(cur_path);
    if (access_path->use_das_
        && (1 != exec_server_list.count()
            || 1 != get_server_list().count()
            || exec_server_list.at(0) != get_server_list().at(0))) {
      contain_dist_das = true;
    }
  }
  return ret;
}

int JoinPath::pre_check_nlj_can_px_batch_rescan(bool &can_px_batch_rescan) const
{
  int ret = OB_SUCCESS;
  can_px_batch_rescan = false;
  bool find_nested_rescan = false;
  bool find_rescan_px = false;
  const ObLogicalOperator *op = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(parent_), K(right_path_));
  } else if (1 < available_parallel_ || 1 < parallel_
             || !parent_->get_plan()->get_optimizer_context().enable_px_batch_rescan()
             || NESTED_LOOP_JOIN != join_algo_ || !is_nlj_with_param_down()
             || CONNECT_BY_JOIN == join_type_
             || IS_SEMI_ANTI_JOIN(join_type_)
             || GCONF._enable_px_ordered_coord) {
    /* when _enable_px_ordered_coord is enabled, px batch rescan is disabled due to is_task_order(）exchange op */
  } else if (is_right_need_exchange()) {
    can_px_batch_rescan = true;
  } else if (right_path_->is_join_path() &&
             OB_FAIL(SMART_CALL(static_cast<const JoinPath*>(right_path_)->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, false)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  } else if (right_path_->is_subquery_path() &&
             NULL != (op = static_cast<const SubQueryPath*>(right_path_)->root_) &&
             OB_FAIL(SMART_CALL(op->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, false)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  } else if (!find_nested_rescan && find_rescan_px) {
    can_px_batch_rescan = true;
  }
  return ret;
}

int JoinPath::pre_check_can_px_batch_rescan(bool &find_nested_rescan,
                                            bool &find_rescan_px,
                                            bool nested) const
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *op = NULL;
  if (find_nested_rescan) {
  } else if (NULL != log_op_) {
    ret = log_op_->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested);
  } else if (OB_FALSE_IT(nested |= NESTED_LOOP_JOIN == join_algo_)) {
  } else if (is_left_need_exchange() || is_right_need_exchange()) {
    find_nested_rescan = nested;
    find_rescan_px |= !nested;
  } else if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_path_), K(right_path_));
  } else if (left_path_->is_join_path() &&
             OB_FAIL(SMART_CALL(static_cast<const JoinPath*>(left_path_)->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  } else if (left_path_->is_subquery_path() &&
             NULL != (op = static_cast<const SubQueryPath*>(left_path_)->root_) &&
             OB_FAIL(SMART_CALL(op->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  } else if (right_path_->is_join_path() &&
             OB_FAIL(SMART_CALL(static_cast<const JoinPath*>(right_path_)->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  } else if (right_path_->is_subquery_path() &&
             NULL != (op = static_cast<const SubQueryPath*>(right_path_)->root_) &&
             OB_FAIL(SMART_CALL(op->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested)))) {
    LOG_WARN("fail to find px for batch rescan", K(ret));
  }
  return ret;
}

int JoinPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo info;
  info.need_parallel_ = parallel_;
  if (OB_FAIL(do_re_estimate_cost(info, card, op_cost, cost))) {
    LOG_WARN("failed to do re estimate cost", K(ret));
  } else {
    op_cost_ = op_cost;
    cost_ = cost;
  }
  return ret;
}

int JoinPath::compute_join_path_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(ret));
  } else {
    contain_fake_cte_ = left_path_->contain_fake_cte_ || right_path_->contain_fake_cte_;
    contain_pw_merge_op_ = (left_path_->contain_pw_merge_op_ && !is_left_need_exchange()) ||
                           (right_path_->contain_pw_merge_op_ && !is_right_need_exchange()) ||
                           (join_algo_ == JoinAlgo::MERGE_JOIN && is_partition_wise());
    contain_match_all_fake_cte_ = left_path_->contain_match_all_fake_cte_ ||
                                  right_path_->contain_match_all_fake_cte_;
    contain_das_op_ = left_path_->contain_das_op_ || right_path_->contain_das_op_;
  }
  return ret;
}

int JoinPath::re_estimate_cost(EstimateCostInfo &info, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t join_parallel = ObGlobalHint::UNSET_PARALLEL == info.need_parallel_
                                ? parallel_ : info.need_parallel_;
  info.need_parallel_ = join_parallel;
  double op_cost = 0.0;
  if (!info.need_re_est(parallel_, get_path_output_rows())) {  // no need to re est cost
    card = get_path_output_rows();
    op_cost = op_cost_;
    cost = get_cost();
  } else if (OB_FAIL(SMART_CALL(do_re_estimate_cost(info, card, op_cost, cost)))) {
    LOG_WARN("failed to do re estimate cost", K(ret));
  } else if (info.override_) {
    parallel_ = join_parallel;
    op_cost_ = op_cost;
    cost_ = cost;
  }
  return ret;
}

int JoinPath::do_re_estimate_cost(EstimateCostInfo &info, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  EstimateCostInfo left_param;
  EstimateCostInfo right_param;
  double left_output_rows = 0.0;
  double right_output_rows = 0.0;
  double left_cost = 0.0;
  double right_cost = 0.0;
  Path *left_path = const_cast<Path*>(left_path_);
  Path *right_path = const_cast<Path*>(right_path_);
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_) || OB_UNLIKELY(info.need_batch_rescan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(info), K(ret));
  } else if (OB_FAIL(get_re_estimate_param(info, left_param, right_param, false))) {
    LOG_WARN("failed to get re estimate param", K(ret));
  } else if (OB_FAIL(left_path->re_estimate_cost(left_param,
                                                 left_output_rows,
                                                 left_cost))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else if (NULL != right_path->log_op_ &&
             OB_FAIL(right_path->log_op_->re_est_cost(right_param,
                                                      right_output_rows,
                                                      right_cost))) {
    LOG_WARN("failed to re est cost for right op", K(ret));
  } else if (NULL == right_path->log_op_ &&
             OB_FAIL(right_path->re_estimate_cost(right_param,
                                                  right_output_rows,
                                                  right_cost))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else if (OB_FAIL(re_estimate_rows(info.join_filter_infos_, left_output_rows, right_output_rows, card))) {
    LOG_WARN("failed to re estimate rows", K(ret));
  } else if (NESTED_LOOP_JOIN == join_algo_) {
    if (OB_FAIL(cost_nest_loop_join(info.need_parallel_,
                                    left_output_rows,
                                    left_cost,
                                    right_output_rows,
                                    right_cost,
                                    false,
                                    op_cost,
                                    cost))) {
      LOG_WARN("failed to cost nest loop join", K(*this), K(ret));
    }
  } else if(MERGE_JOIN == join_algo_) {
    if (OB_FAIL(cost_merge_join(info.need_parallel_,
                                left_output_rows,
                                left_cost,
                                right_output_rows,
                                right_cost,
                                false,
                                op_cost,
                                cost))) {
      LOG_WARN("failed to cost merge join", K(*this), K(ret));
    }
  } else if(HASH_JOIN == join_algo_) {
    if ((OB_FAIL(cost_hash_join(info.need_parallel_,
                                left_output_rows,
                                left_cost,
                                right_output_rows,
                                right_cost,
                                false,
                                op_cost,
                                cost)))) {
      LOG_WARN("failed to cost hash join", K(*this), K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown join algorithm", K(join_algo_));
  }
  if (OB_SUCC(ret)) {
    if (info.need_row_count_ >= 0 && info.need_row_count_ < get_path_output_rows()) {
      card = info.need_row_count_;
    }
  }
  return ret;
}

int JoinPath::get_re_estimate_param(EstimateCostInfo &param,
                                    EstimateCostInfo &left_param,
                                    EstimateCostInfo &right_param,
                                    bool re_est_for_op)
{
  int ret = OB_SUCCESS;
  left_param.override_ = param.override_;
  right_param.override_ = param.override_;
  right_param.need_batch_rescan_ = can_use_batch_nlj_;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_) ||
      OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(ret));
  } else if (OB_UNLIKELY(param.need_parallel_ < ObGlobalHint::DEFAULT_PARALLEL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(param), K(ret));
  } else {
    left_param.rescan_left_server_list_ = param.rescan_left_server_list_;
    right_param.rescan_left_server_list_ = left_path_->is_match_all()
                                           ? param.rescan_left_server_list_
                                           : &left_path_->get_server_list();
    double card = get_path_output_rows();
    if (param.need_row_count_ >= card || param.need_row_count_ < 0) {
      param.need_row_count_ = card;
    } else if (JoinAlgo::HASH_JOIN == join_algo_ &&
               LEFT_SEMI_JOIN != join_type_ &&
               LEFT_ANTI_JOIN != join_type_) {
      right_param.need_row_count_ = right_path_->get_path_output_rows();
      right_param.need_row_count_ *= (param.need_row_count_ / card);
    } else if (JoinAlgo::NESTED_LOOP_JOIN == join_algo_) {
      left_param.need_row_count_ = left_path_->get_path_output_rows();
      left_param.need_row_count_ *= (param.need_row_count_ / card);
    } else if (JoinAlgo::MERGE_JOIN == join_algo_) {
      left_param.need_row_count_ = left_path_->get_path_output_rows();
      left_param.need_row_count_ *= sqrt(param.need_row_count_ / card);
      right_param.need_row_count_ = right_path_->get_path_output_rows();
      right_param.need_row_count_ *= sqrt(param.need_row_count_ / card);
    }

    if (right_path_->is_inner_path() && (right_param.need_row_count_ > 1 || right_param.need_row_count_ < 0)
        && (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_)
        && ObEnableOptRowGoal::OFF != parent_->get_plan()->get_optimizer_context().get_enable_opt_row_goal()) {
      right_param.need_row_count_ = 1;
    }

    if (re_est_for_op) {
      left_param.need_parallel_ = left_path_->is_match_all() ? ObGlobalHint::UNSET_PARALLEL : param.need_parallel_;
      right_param.need_parallel_ = right_path_->is_match_all() ? ObGlobalHint::UNSET_PARALLEL : param.need_parallel_;
    } else if (is_partition_wise()) {
      left_param.need_parallel_ = param.need_parallel_;
      right_param.need_parallel_ = param.need_parallel_;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(left_param.join_filter_infos_.assign(param.join_filter_infos_))) {
      LOG_WARN("failed to assign join filter infos", K(ret));
    } else if (OB_FAIL(right_param.join_filter_infos_.assign(param.join_filter_infos_))) {
      LOG_WARN("failed to assign join filter infos", K(ret));
    } else if (OB_FAIL(append(right_param.join_filter_infos_, join_filter_infos_))) {
      LOG_WARN("failed to append join filter infos", K(ret));
    }
  }
  return ret;
}

int JoinPath::re_estimate_rows(ObIArray<JoinFilterInfo> &pushdown_join_filter_infos,
                               double left_output_rows,
                               double right_output_rows,
                               double &row_count)
{
  int ret = OB_SUCCESS;
  double selectivity = 1.0;
  ObLogPlan *plan = NULL;
  ObJoinOrder *left_tree = NULL;
  ObJoinOrder *right_tree = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_) || OB_ISNULL(parent_) ||
      OB_ISNULL(plan = parent_->get_plan()) ||
      OB_ISNULL(left_tree = left_path_->parent_) ||
      OB_ISNULL(right_tree = right_path_->parent_) ||
      OB_ISNULL(stmt = plan->get_stmt()) ||
      OB_ISNULL(plan->get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(plan), K(ret));
  } else if (!plan->get_optimizer_context().get_query_ctx()->
                check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0, COMPAT_VERSION_4_3_3) ?
             HASH_JOIN == join_algo_ && !join_filter_infos_.empty() :
             HASH_JOIN == join_algo_ && pushdown_join_filter_infos.empty()) {
    row_count = get_path_output_rows();
  } else if (right_path_->is_inner_path()) {
    if (left_tree->get_output_rows() > 0) {
      row_count = parent_->get_output_rows() * left_output_rows / left_tree->get_output_rows();
    } else {
      row_count = 0;
    }
  } else {
    JoinInfo join_info;
    join_info.join_type_ = join_type_;
    if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type_)) {
      if (OB_FAIL(join_info.where_conditions_.assign(filter_))) {
        LOG_WARN("failed to assign join conditions", K(ret));
      } else if (OB_FAIL(join_info.on_conditions_.assign(other_join_conditions_))) {
        LOG_WARN("failed to assign join conditions", K(ret));
      } else if (OB_FAIL(append(join_info.on_conditions_, equal_join_conditions_))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    } else {
      if (OB_FAIL(join_info.where_conditions_.assign(filter_))) {
        LOG_WARN("failed to assign join conditions", K(ret));
      } else if (OB_FAIL(append(join_info.where_conditions_, other_join_conditions_))) {
        LOG_WARN("failed to assign join conditions", K(ret));
      } else if (OB_FAIL(append(join_info.where_conditions_, equal_join_conditions_))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
    EqualSets equal_sets;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(equal_sets, left_tree->get_output_equal_sets())) ||
               OB_FAIL(append(equal_sets, right_tree->get_output_equal_sets()))) {
      LOG_WARN("failed to append equal sets", K(ret));
    /*  zhanyuetodo bug fix: case: optimizer.subquery_in_on_condition_oracle
      filter contain subquery is calculated twice at subplan filter and t2 join t3
      select t1.* from t1
      left join (t2 join t3 on t2.c1 = t3.c1)
      on t1.c1 > all(select c1 from t3 where t2.c1+t1.c1 = t3.c1);
    */
    } else if (OB_FAIL(ObJoinOrder::calc_join_output_rows(plan,
                                                          left_tree->get_tables(),
                                                          right_tree->get_tables(),
                                                          left_output_rows,
                                                          right_output_rows,
                                                          join_info,
                                                          row_count,
                                                          selectivity,
                                                          equal_sets))) {
      LOG_WARN("failed to calc join output rows", K(ret));
    } else {
      for (int64_t i = 0; i < join_filter_infos_.count(); i ++) {
        if (join_filter_infos_.at(i).join_filter_selectivity_ > OB_DOUBLE_EPSINON) {
          row_count /= join_filter_infos_.at(i).join_filter_selectivity_;
        }
      }
      // refine the rowcnt based on the first estimated difference between ObJoinOrder and JoinPath
      if (join_output_rows_ > OB_DOUBLE_EPSINON) {
        row_count *= get_path_output_rows() / join_output_rows_;
      }
      row_count = std::min(row_count, get_path_output_rows());
    }
  }
  return ret;
}

int JoinPath::cost_nest_loop_join(int64_t join_parallel,
                                  double left_output_rows,
                                  double left_cost,
                                  double right_output_rows,
                                  double right_cost,
                                  bool re_est_for_op,
                                  double &op_cost,
                                  double &cost)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  int64_t in_parallel = 0;
  int64_t left_out_parallel = 0;
  int64_t right_out_parallel = 0;
  ObJoinOrder *left_join_order = NULL;
  ObJoinOrder *right_join_order = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) ||
      OB_ISNULL(right_path_) || OB_ISNULL(right_join_order = right_path_->parent_) ||
      OB_ISNULL(left_path_) || OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_), K(left_path_),
        K(left_join_order), K(right_path_), K(right_join_order));
  } else if (OB_UNLIKELY((in_parallel = join_parallel) < 1) ||
             OB_UNLIKELY((left_out_parallel = left_path_->parallel_) < 1) ||
             OB_UNLIKELY((right_out_parallel = right_path_->parallel_) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(in_parallel), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    bool with_nl_param = right_path_->is_inner_path();
    double left_rows = left_output_rows;
    double right_rows = right_output_rows;
    int64_t right_part_cnt = 1;
    double left_ex_cost = 0.0;
    double right_ex_cost = 0.0;
    if (DistAlgo::DIST_BC2HOST_NONE == join_dist_algo_) {
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, in_parallel/server_cnt_);
      right_cost = right_cost * right_out_parallel / server_cnt_;
      right_rows /= server_cnt_;
    } else if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo_ ||
               DistAlgo::DIST_ALL_NONE == join_dist_algo_) {
      right_rows /= in_parallel;
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, 1);
    } else if (DistAlgo::DIST_NONE_ALL == join_dist_algo_
	       || DistAlgo::DIST_RANDOM_ALL == join_dist_algo_) {
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, in_parallel);
      if (right_path_->is_access_path() &&
          static_cast<const AccessPath*>(right_path_)->can_das_dynamic_part_pruning()) {
        // das dynamic partition pruning
        right_part_cnt = static_cast<const AccessPath*>(right_path_)->get_part_cnt_before_das_dynamic_part_pruning();
        right_rows /= right_part_cnt;
        right_cost = right_cost / right_part_cnt;
      }
    } else if (DistAlgo::DIST_NONE_BROADCAST == join_dist_algo_) {
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, in_parallel);
    } else if (DistAlgo::DIST_PULL_TO_LOCAL == join_dist_algo_) {
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, in_parallel);
    } else {
      left_rows = ObJoinOrder::calc_single_parallel_rows(left_rows, in_parallel);
      if (NULL != right_path_->get_sharding() &&
          NULL != right_path_->get_sharding()->get_phy_table_location_info()) {
        right_part_cnt = right_path_->get_sharding()->get_part_cnt();
      }
      right_rows /= right_part_cnt;
      const int64_t right_real_parallel = is_partition_wise() ? in_parallel : right_out_parallel;
      right_cost = right_cost * right_real_parallel / right_part_cnt;
    }
    ObCostNLJoinInfo est_join_info(left_rows,
                                   left_cost,
                                   left_join_order->get_output_row_size(),
                                   right_rows,
                                   right_cost,
                                   right_join_order->get_output_row_size(),
                                   left_join_order->get_tables(),
                                   right_join_order->get_tables(),
                                   join_type_,
                                   other_cond_sel_,
                                   with_nl_param,
                                   need_mat_,
                                   is_right_need_exchange() ||
                                   right_path_->exchange_allocated_,
                                   in_parallel,
                                   equal_join_conditions_,
                                   other_join_conditions_,
                                   filter_,
                                   &plan->get_update_table_metas(),
                                   &plan->get_selectivity_ctx());
    ObExchCostInfo left_exch_info(left_output_rows,
                                  left_join_order->get_output_row_size(),
                                  get_left_dist_method(),
                                  left_out_parallel,
                                  in_parallel,
                                  false,
                                  left_sort_keys_,
                                  server_cnt_);
    ObExchCostInfo right_exch_info(right_output_rows,
                                   right_join_order->get_output_row_size(),
                                   get_right_dist_method(),
                                   right_out_parallel,
                                   in_parallel,
                                   false,
                                   right_sort_keys_,
                                   server_cnt_);
    if (OB_FAIL(ObOptEstCost::cost_nestloop(est_join_info, op_cost, opt_ctx))) {
      LOG_WARN("failed to estimate nest loop join cost", K(est_join_info), K(ret));
    } else if (!re_est_for_op && is_left_need_exchange() &&
               OB_FAIL(ObOptEstCost::cost_exchange(left_exch_info, left_ex_cost,
                                                   opt_ctx))) {
      LOG_WARN("failed to cost exchange", K(ret));
    } else if (!re_est_for_op && is_right_need_exchange() &&
               OB_FAIL(ObOptEstCost::cost_exchange(right_exch_info, right_ex_cost,
                                                   opt_ctx))) {
      LOG_WARN("failed to cost exchange", K(ret));
    } else {
      cost = op_cost + left_cost + ObOptEstCost::cost_get_rows(left_rows, opt_ctx)
          + left_ex_cost + right_ex_cost;
      if (need_mat_ && !re_est_for_op) {
        cost += ObOptEstCost::cost_get_rows(right_rows, opt_ctx) + right_path_->get_cost();
        cost += ObOptEstCost::cost_material(right_rows, right_join_order->get_output_row_size(),
                                            opt_ctx);
      }
      LOG_TRACE("succeed to compute nested loop join cost", K(cost), K(op_cost), K(re_est_for_op),
          K(in_parallel), K(left_out_parallel), K(right_out_parallel),
          K(left_ex_cost), K(right_ex_cost), K(left_output_rows), K(left_cost), K(right_output_rows), K(right_cost));
    }
  }
  return ret;
}

int JoinPath::cost_merge_join(int64_t join_parallel,
                              double left_output_rows,
                              double left_cost,
                              double right_output_rows,
                              double right_cost,
                              bool re_est_for_op,
                              double &op_cost,
                              double &cost)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  int64_t in_parallel = 0;
  int64_t left_out_parallel = 0;
  int64_t right_out_parallel = 0;
  ObJoinOrder *left_join_order = NULL;
  ObJoinOrder *right_join_order = NULL;
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) ||
      OB_ISNULL(right_path_) ||
      OB_ISNULL(right_join_order = right_path_->parent_) ||
      OB_ISNULL(left_path_) ||
      OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_), K(left_path_),
        K(left_join_order), K(right_path_), K(right_join_order));
  } else if (OB_UNLIKELY((in_parallel = join_parallel) < 1) ||
             OB_UNLIKELY((left_out_parallel = left_path_->parallel_) < 1) ||
             OB_UNLIKELY((right_out_parallel = right_path_->parallel_) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(in_parallel), K(left_out_parallel), K(right_out_parallel), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    double left_child_cost = left_cost;
    double right_child_cost = right_cost;
    double left_rows = left_output_rows;
    double right_rows = right_output_rows;
    if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo_ ||
        DistAlgo::DIST_ALL_NONE == join_dist_algo_) {
      right_rows /= in_parallel;
    } else if (DistAlgo::DIST_NONE_BROADCAST == join_dist_algo_ ||
               DistAlgo::DIST_NONE_ALL == join_dist_algo_) {
      left_rows /= in_parallel;
    } else {
      left_rows /= in_parallel;
      right_rows /= in_parallel;
    }
    ObCostMergeJoinInfo est_join_info(left_rows,
                                      left_join_order->get_output_row_size(),
                                      right_rows,
                                      right_join_order->get_output_row_size(),
                                      left_join_order->get_tables(),
                                      right_join_order->get_tables(),
                                      join_type_,
                                      equal_join_conditions_,
                                      other_join_conditions_,
                                      filter_,
                                      equal_cond_sel_,
                                      other_cond_sel_,
                                      &plan->get_update_table_metas(),
                                      &plan->get_selectivity_ctx());
    if (OB_FAIL(ObOptEstCost::cost_mergejoin(est_join_info, op_cost, opt_ctx))) {
      LOG_WARN("failed to estimate merge join cost", K(est_join_info), K(ret));
    } else if (!re_est_for_op &&
               OB_FAIL(ObOptEstCost::cost_sort_and_exchange(&plan->get_update_table_metas(),
                                                            &plan->get_selectivity_ctx(),
                                                            get_left_dist_method(),
                                                            left_path_->is_distributed(),
                                                            is_left_local_order(),
                                                            left_output_rows,
                                                            left_join_order->get_output_row_size(),
                                                            left_cost,
                                                            left_out_parallel,
                                                            server_cnt_,
                                                            in_parallel,
                                                            left_sort_keys_,
                                                            left_need_sort_,
                                                            left_prefix_pos_,
                                                            left_child_cost,
                                                            opt_ctx))) {
      LOG_WARN("failed to compute cost for merge style op", K(ret));
    } else if (!re_est_for_op &&
               OB_FAIL(ObOptEstCost::cost_sort_and_exchange(&plan->get_update_table_metas(),
                                                            &plan->get_selectivity_ctx(),
                                                            get_right_dist_method(),
                                                            right_path_->is_distributed(),
                                                            is_right_local_order(),
                                                            right_output_rows,
                                                            right_join_order->get_output_row_size(),
                                                            right_cost,
                                                            right_out_parallel,
                                                            server_cnt_,
                                                            in_parallel,
                                                            right_sort_keys_,
                                                            right_need_sort_,
                                                            right_prefix_pos_,
                                                            right_child_cost,
                                                            opt_ctx))) {
      LOG_WARN("failed to compute cost for merge style op", K(ret));
    } else {
      cost = op_cost + left_child_cost + right_child_cost;
      LOG_TRACE("succeed to compute merge join cost", K(cost), K(op_cost), K(left_child_cost),
          K(in_parallel), K(left_out_parallel), K(right_out_parallel),
          K(right_child_cost), K(left_output_rows), K(left_cost), K(right_output_rows), K(right_cost));
    }
  }
  return ret;
}

int JoinPath::cost_hash_join(int64_t join_parallel,
                            double left_output_rows,
                            double left_cost,
                            double right_output_rows,
                            double right_cost,
                            bool re_est_for_op,
                            double &op_cost,
                            double &cost)
{
  int ret = OB_SUCCESS;
  int64_t in_parallel = 0;
  int64_t left_out_parallel = 0;
  int64_t right_out_parallel = 0;
  double join_filter_selectivity = 1.0;
  ObLogPlan *plan = NULL;
  ObJoinOrder *left_join_order = NULL;
  ObJoinOrder *right_join_order = NULL;
  Path *right_path = const_cast<Path*>(right_path_);
  if (OB_ISNULL(parent_) || OB_ISNULL(plan = parent_->get_plan()) ||
      OB_ISNULL(right_path_) ||
      OB_ISNULL(right_join_order = right_path_->parent_) ||
      OB_ISNULL(left_path_) ||
      OB_ISNULL(left_join_order = left_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_), K(left_path_), K(left_join_order),
     K(right_path_), K(right_join_order));
  } else if (OB_UNLIKELY((in_parallel = join_parallel) < 1) ||
             OB_UNLIKELY((left_out_parallel = left_path_->parallel_) < 1) ||
             OB_UNLIKELY((right_out_parallel = right_path_->parallel_) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    double left_ex_cost = 0.0;
    double right_ex_cost = 0.0;
    double left_rows = left_output_rows;
    double right_rows = right_output_rows;
    if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo_ ||
        DistAlgo::DIST_ALL_NONE == join_dist_algo_) {
      right_rows /= in_parallel;
    } else if (DistAlgo::DIST_BC2HOST_NONE == join_dist_algo_) {
      // only for shared hash join
      // right_rows is same as the implementation of DIST_BROADCAST_NONE
      // left_rows is left_rows / parallel * server
      right_rows /= in_parallel;
      left_rows = left_rows / in_parallel * server_cnt_;
    } else if (DistAlgo::DIST_NONE_BROADCAST == join_dist_algo_ ||
               DistAlgo::DIST_NONE_ALL == join_dist_algo_ ||
               DistAlgo::DIST_RANDOM_ALL == join_dist_algo_) {
      left_rows /= in_parallel;
    } else {
      left_rows /= in_parallel;
      right_rows /= in_parallel;
    }
    ObCostHashJoinInfo est_join_info(left_rows,
                                     left_join_order->get_output_row_size(),
                                     right_rows,
                                     right_join_order->get_output_row_size(),
                                     left_join_order->get_tables(),
                                     right_join_order->get_tables(),
                                     join_type_,
                                     equal_join_conditions_,
                                     other_join_conditions_,
                                     filter_,
                                     join_filter_infos_,
                                     equal_cond_sel_,
                                     other_cond_sel_,
                                     &plan->get_update_table_metas(),
                                     &plan->get_selectivity_ctx());
    ObExchCostInfo left_exch_info(left_output_rows,
                                  left_join_order->get_output_row_size(),
                                  get_left_dist_method(),
                                  left_out_parallel,
                                  in_parallel,
                                  false,
                                  left_sort_keys_,
                                  server_cnt_);
    ObExchCostInfo right_exch_info(right_output_rows,
                                   right_join_order->get_output_row_size(),
                                   get_right_dist_method(),
                                   right_out_parallel,
                                   in_parallel,
                                   false,
                                   right_sort_keys_,
                                   server_cnt_);
    if (OB_FAIL(ObOptEstCost::cost_hashjoin(est_join_info, op_cost, opt_ctx))) {
      LOG_WARN("failed to estimate hash join cost", K(est_join_info), K(ret));
    } else if (!re_est_for_op && is_left_need_exchange() &&
               OB_FAIL(ObOptEstCost::cost_exchange(left_exch_info, left_ex_cost,
                                                   opt_ctx))) {
      LOG_WARN("failed to cost exchange", K(ret));
    } else if (!re_est_for_op && is_right_need_exchange() &&
               OB_FAIL(ObOptEstCost::cost_exchange(right_exch_info, right_ex_cost,
                                                   opt_ctx))) {
      LOG_WARN("failed to cost exchange", K(ret));
    } else {
      cost = op_cost + left_cost + right_cost + left_ex_cost + right_ex_cost;
      LOG_TRACE("succeed to compute hash join cost", K(cost), K(op_cost), K(re_est_for_op), K(left_ex_cost), K(right_ex_cost),
         K(in_parallel), K(left_out_parallel), K(right_out_parallel),
         K(left_output_rows), K(left_cost), K(right_output_rows), K(right_cost));
    }
  }
  return ret;
}

int JoinPath::check_is_contain_normal_nl()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_path_) || OB_ISNULL(right_path_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_path_), K(right_path_), K(ret));
  } else {
    bool contain_normal_nl = contain_normal_nl_;
    bool has_none_equal_join = has_none_equal_join_;
    if (left_path_->is_join_path()) {
      contain_normal_nl |= static_cast<const JoinPath*>(left_path_)->contain_normal_nl();
      has_none_equal_join |= static_cast<const JoinPath*>(left_path_)->has_none_equal_join();
    }
    if (right_path_->is_join_path()) {
      contain_normal_nl |= static_cast<const JoinPath*>(right_path_)->contain_normal_nl();
      has_none_equal_join |= static_cast<const JoinPath*>(right_path_)->has_none_equal_join();
    }
    set_contain_normal_nl(contain_normal_nl);
    set_has_none_equal_join(has_none_equal_join);
  }
  return ret;
}

void JoinPath::reuse()
{
  // base Path related
  parent_ = NULL;
  is_local_order_ = false;
  is_range_order_ = false;
  ordering_.reuse();
  interesting_order_info_ = OrderingFlag::NOT_MATCH;
  filter_.reuse();
  cost_ = 0.0;
  op_cost_ = 0.0;
  log_op_ = NULL;
  is_inner_path_ = false;
  inner_row_count_ = 0;
  pushdown_filters_.reuse();
  nl_params_.reuse();
  strong_sharding_ = NULL;
  weak_sharding_.reuse();
  exchange_allocated_ = false;
  phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
  location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
  contain_fake_cte_ = false;
  contain_pw_merge_op_ = false;
  contain_match_all_fake_cte_ = false;
  contain_das_op_ = false;
  parallel_ = 1;
  op_parallel_rule_ = OpParallelRule::OP_DOP_RULE_MAX;
  available_parallel_ = ObGlobalHint::DEFAULT_PARALLEL;
  server_cnt_ = 1;

  // JoinPath related
  left_path_ = NULL;
  right_path_ = NULL;
  join_algo_ = INVALID_JOIN_ALGO;
  join_dist_algo_ = DistAlgo::DIST_INVALID_METHOD;
  is_slave_mapping_ = false;
  join_type_ = UNKNOWN_JOIN;
  need_mat_ = false;
  left_need_sort_ = false;
  left_prefix_pos_ = 0;
  right_need_sort_ = false;
  right_prefix_pos_ = 0;
  left_sort_keys_.reuse();
  right_sort_keys_.reuse();
  merge_directions_.reuse();
  equal_join_conditions_.reuse();
  other_join_conditions_.reuse();
  server_list_.reuse();
  equal_cond_sel_ = -1.0;
  other_cond_sel_ = -1.0;
  contain_normal_nl_ = false;
  has_none_equal_join_ = false;
  is_naaj_ = false;
  is_sna_ = false;
  inherit_sharding_index_ = -1;
  join_output_rows_ = -1.0;
}

int JoinPath::compute_pipeline_info()
{
  int ret = OB_SUCCESS;
  is_pipelined_path_ = false;
  is_nl_style_pipelined_path_ = false;
  if (HASH_JOIN == join_algo_ ||
      left_need_sort_ ||
      right_need_sort_ ||
      need_mat_) {
    //do nothing
  } else if (left_path_->is_pipelined_path() &&
             right_path_->is_pipelined_path()) {
    is_pipelined_path_ = true;
    is_nl_style_pipelined_path_ = NESTED_LOOP_JOIN == join_algo_ &&
                            left_path_->is_nl_style_pipelined_path() &&
                            right_path_->is_nl_style_pipelined_path();
  }
  return ret;
}

int SubQueryPath::assign(const SubQueryPath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to deep copy path", K(ret));
  } else {
    subquery_id_ = other.subquery_id_;
    root_ = other.root_;
  }
  return ret;
}

int SubQueryPath::estimate_cost()
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  if (OB_ISNULL(root_) || OB_ISNULL(parent_) ||
      OB_ISNULL(parent_->get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(root_), K(ret));
  } else if (OB_UNLIKELY((parallel = root_->get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(parallel));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    double child_card = root_->get_card();
    double child_cost = root_->get_cost();
    op_cost_ = ObOptEstCost::cost_filter_rows(child_card / parallel, filter_,
                                              opt_ctx);
    cost_ = child_cost + op_cost_;
  }
  return ret;
}

int SubQueryPath::re_estimate_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = (ObGlobalHint::UNSET_PARALLEL == param.need_parallel_ || is_match_all())
                           ? parallel_ : param.need_parallel_;
  card = get_path_output_rows();
  if (OB_ISNULL(root_) || OB_ISNULL(parent_) ||
      OB_ISNULL(parent_->get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(root_), K(ret));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(parallel_), K(param.need_parallel_));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    if (param.need_row_count_ >= 0) {
      if (param.need_row_count_ < card && card > OB_DOUBLE_EPSINON) {
        double output_row_count = param.need_row_count_;
        param.need_row_count_ = (param.need_row_count_ / card) * root_->get_card();
        card = output_row_count;
      }
    }
    double child_card = root_->get_card();
    double child_cost = root_->get_cost();
    double op_cost = 0.0;
    if (OB_FAIL(root_->re_est_cost(param, child_card, child_cost))) {
      LOG_WARN("failed to est cost", K(ret));
    } else {
      op_cost = ObOptEstCost::cost_filter_rows(child_card / parallel, filter_,
                                               opt_ctx);
      cost = child_cost + op_cost;
      card = child_card < card ? child_card : card;
      if (param.override_) {
        op_cost_ = op_cost;
        cost_ = cost;
      }
    }
  }
  return ret;
}

int SubQueryPath::compute_pipeline_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null root", K(ret));
  } else {
    is_pipelined_path_ = root_->is_pipelined_plan();
    is_nl_style_pipelined_path_ = root_->is_nl_style_pipelined_plan();
  }
  return ret;
}

int ObJoinOrder::init_base_join_order(const TableItem *table_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  int32_t table_bit_index = OB_INVALID_INDEX;
  if (OB_ISNULL(table_item) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(table_item), K(get_plan()), K(stmt), K(ret));
  } else {
    table_id_ = table_item->table_id_;
    table_meta_info_.ref_table_id_ = table_item->ref_id_;
    table_meta_info_.table_type_ = table_item->table_type_;
    table_bit_index = stmt->get_table_bit_index(table_item->table_id_);
    if (OB_FAIL(get_tables().add_member(table_bit_index)) ||
        OB_FAIL(get_output_tables().add_member(table_bit_index))) {
      LOG_WARN("failed to add member", K(table_bit_index), K(ret));
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
    } else if (table_item->is_json_table()) {
      set_type(JSON_TABLE_ACCESS);
    } else if (table_item->is_values_table()) {
      set_type(VALUES_TABLE_ACCESS);
    } else if (table_item->is_lateral_table()) {
      set_type(SUBQUERY);
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
  if (FAKE_CTE_TABLE_ACCESS == get_type()) {
    ret = generate_cte_table_paths();
  } else if (FUNCTION_TABLE_ACCESS == get_type()) {
    ret = generate_function_table_paths();
  } else if (JSON_TABLE_ACCESS == get_type()) {
    ret = generate_json_table_paths();
  } else if (TEMP_TABLE_ACCESS == get_type()) {
    ret = generate_temp_table_paths();
  } else if (ACCESS == get_type()) {
    ret = generate_normal_base_table_paths();
  } else if (SUBQUERY == get_type()) {
    OPT_TRACE_TITLE("begin generate subplan");
    ret = generate_normal_subquery_paths();
    OPT_TRACE_TITLE("end generate subplan");
  } else if (VALUES_TABLE_ACCESS == get_type()) {
    ret = generate_values_table_paths();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected base path type", K(get_type()), K(ret));
  }
  if (FAILEDx(init_ambient_card())) {
    LOG_WARN("failed to init ambient cardinality", K(ret));
  }
  return ret;
}

int ObJoinOrder::init_ambient_card()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  int64_t idx = -1;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      FALSE_IT(idx = get_plan()->get_stmt()->get_table_bit_index(table_id_)) ||
      OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx > stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(get_plan()), K(table_id_), K(idx), KPC(stmt));
  } else if (OB_FAIL(ambient_card_.prepare_allocate(stmt->get_table_size() + 1))) {
    LOG_WARN("failed to allocate", K(ret));
  } else {
    for (int64_t i = 0; i < ambient_card_.count(); i ++) {
      ambient_card_.at(i) = -1;
    }
    ambient_card_.at(idx) = output_rows_;
  }
  return ret;
}

int ObJoinOrder::generate_json_table_paths()
{
  int ret = OB_SUCCESS;
  JsonTablePath *json_path = NULL;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(allocator_), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_)) ||
             OB_ISNULL(table_item->json_table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_id_), K(table_item), K(ret));
  } else if (OB_ISNULL(json_path = static_cast<JsonTablePath*>(allocator_->alloc(sizeof(JsonTablePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate access path", K(ret));
  } else {
    json_path = new(json_path) JsonTablePath();
    json_path->table_id_ = table_id_;
    json_path->parent_ = this;
    ObSEArray<ObExecParamRawExpr *, 4> nl_params;
    ObRawExpr* default_expr = NULL;
    ObArray<ColumnItem> column_items;
    // magic number ? todo refine this
    output_rows_ = 199;
    output_row_size_ = 199;
    json_path->strong_sharding_ = get_plan()->get_optimizer_context().get_match_all_sharding();
    if (OB_FAIL(json_path->set_parallel_and_server_info_for_match_all())) {
      LOG_WARN("failed set parallel and server info for match all", K(ret));
    } else if (OB_FAIL(append(json_path->filter_, get_restrict_infos()))) {
      LOG_WARN("failed to append filter", K(ret));
    } else if (table_item->json_table_def_->doc_exprs_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extract param for json table expr", K(ret));
    } else if (OB_FAIL(param_json_table_expr(table_item->json_table_def_->doc_exprs_,
                                             nl_params,
                                             json_path->subquery_exprs_))) {
      LOG_WARN("failed to extract param for json table expr", K(ret));
    } else if (OB_FAIL(json_path->nl_params_.assign(nl_params))) {
      LOG_WARN("failed to assign nl params", K(ret));
    } else if (OB_FAIL(append(json_path->value_exprs_, table_item->json_table_def_->doc_exprs_))) {
      LOG_WARN("failed to append value exprs", K(ret));
    }
    // deal non_const default value
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->get_column_items(table_id_, column_items))) {
      LOG_WARN("fail to get column item", K(ret));
    } else if (OB_FAIL(json_path->column_param_default_exprs_.reserve(column_items.count()))) {
      LOG_WARN("fail to init column default map", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); i++) {
      ColumnItem& col_item = column_items.at(i);
      ObColumnDefault col_val(col_item.column_id_);
      default_expr = col_item.default_value_expr_;
      if (OB_FAIL(generate_json_table_default_val(json_path->nl_params_,
                                                  json_path->subquery_exprs_,
                                                  default_expr))) { // default error
        LOG_WARN("fail to check default error value", K(ret));
      } else {
        col_val.default_error_expr_ = default_expr;
      }
      default_expr = col_item.default_empty_expr_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_json_table_default_val(json_path->nl_params_,
                                                         json_path->subquery_exprs_,
                                                         default_expr))) { // default empty
        LOG_WARN("fail to check default empty value", K(ret));
      } else {
        col_val.default_empty_expr_ = default_expr;
      }
      if (OB_SUCC(ret) && OB_FAIL(json_path->column_param_default_exprs_.push_back(col_val))) {
        LOG_WARN("fail to append col default into array", K(ret), K(col_val));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(json_path->estimate_cost())) {
        LOG_WARN("failed to estimate cost", K(ret));
      } else if (OB_FAIL(json_path->compute_pipeline_info())) {
        LOG_WARN("failed to compute pipelined path", K(ret));
      } else if (OB_FAIL(add_path(json_path))) {
        LOG_WARN("failed to add path", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::generate_json_table_default_val(ObIArray<ObExecParamRawExpr *> &nl_param,
                                                  ObIArray<ObRawExpr *> &subquery_exprs,
                                                  ObRawExpr*& default_expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(default_expr)) {
    ObArray<ObExecParamRawExpr *> t_nl_param;
    const ObDMLStmt *stmt = NULL;
    ObLogPlan *plan = get_plan();
    ObSEArray<ObRawExpr *, 1> old_func_exprs;
    ObSEArray<ObRawExpr *, 1> new_func_exprs;
    ObExecParamRawExpr *param = nullptr;
    if (OB_ISNULL(plan) || OB_ISNULL(stmt = plan->get_stmt())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(plan), K(ret));
    } else if (OB_FAIL(old_func_exprs.push_back(default_expr))) {
      LOG_WARN("failed to push back function table expr", K(ret));
    } else if (OB_FAIL(extract_params_for_inner_path(default_expr->get_relation_ids(),
                                                      t_nl_param,
                                                      subquery_exprs,
                                                      old_func_exprs,
                                                      new_func_exprs))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (OB_UNLIKELY(new_func_exprs.count() != 1) ||
                OB_ISNULL(new_func_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new function table expr is invalid", K(ret), K(new_func_exprs));
    } else {
      default_expr = new_func_exprs.at(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < t_nl_param.count(); i++) {
        if (OB_FAIL(nl_param.push_back(t_nl_param.at(i)))) {
          LOG_WARN("fail to push nl param", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_function_table_paths()
{
  int ret = OB_SUCCESS;
  FunctionTablePath *func_path = NULL;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(allocator_), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_)) ||
             OB_ISNULL(table_item->function_table_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_id_), K(table_item), K(ret));
  } else if (OB_ISNULL(func_path = reinterpret_cast<FunctionTablePath*>(
                       allocator_->alloc(sizeof(FunctionTablePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate access path", K(ret));
  } else {
    func_path = new(func_path) FunctionTablePath();
    func_path->table_id_ = table_id_;
    func_path->parent_ = this;
    ObSEArray<ObExecParamRawExpr *, 4> nl_params;
    ObRawExpr* function_table_expr = NULL;
    // magic number ? todo refine this
    output_rows_ = 199;
    output_row_size_ = 199;
    func_path->strong_sharding_ = get_plan()->get_optimizer_context().get_match_all_sharding();
    if (OB_FAIL(func_path->set_parallel_and_server_info_for_match_all())) {
      LOG_WARN("failed set parallel and server info for match all", K(ret));
    } else if (OB_FAIL(append(func_path->filter_, get_restrict_infos()))) {
      LOG_WARN("failed to append filter", K(ret));
    } else if (OB_ISNULL(function_table_expr = table_item->function_table_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null function table expr", K(ret));
    } else if (OB_FAIL(param_funct_table_expr(function_table_expr,
                                              nl_params,
                                              func_path->subquery_exprs_))) {
      LOG_WARN("failed to extract param for function table expr", K(ret));
    } else if (OB_FAIL(func_path->nl_params_.assign(nl_params))) {
      LOG_WARN("failed to assign nl params", K(ret));
    } else {
      func_path->value_expr_ = function_table_expr;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(func_path->estimate_cost())) {
        LOG_WARN("failed to estimate cost", K(ret));
      } else if (OB_FAIL(func_path->compute_pipeline_info())) {
        LOG_WARN("failed to compute pipelined path", K(ret));
      } else if (OB_FAIL(add_path(func_path))) {
        LOG_WARN("failed to add path", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::param_funct_table_expr(ObRawExpr* &function_table_expr,
                                        ObIArray<ObExecParamRawExpr *> &nl_params,
                                        ObIArray<ObRawExpr*> &subquery_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObLogPlan *plan = get_plan();
  ObSEArray<ObRawExpr *, 1> old_func_exprs;
  ObSEArray<ObRawExpr *, 1> new_func_exprs;
  if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(stmt = plan->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(plan), K(ret));
  } else if (OB_FAIL(old_func_exprs.push_back(function_table_expr))) {
    LOG_WARN("failed to push back function table expr", K(ret));
  } else if (OB_FAIL(extract_params_for_inner_path(function_table_expr->get_relation_ids(),
                                                    nl_params,
                                                    subquery_exprs,
                                                    old_func_exprs,
                                                    new_func_exprs))) {
    LOG_WARN("failed to extract params", K(ret));
  } else if (OB_UNLIKELY(new_func_exprs.count() != 1) ||
              OB_ISNULL(new_func_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new function table expr is invalid", K(ret), K(new_func_exprs));
  } else {
    function_table_expr = new_func_exprs.at(0);
  }
  return ret;
}


int ObJoinOrder::param_json_table_expr( ObIArray<ObRawExpr *> &json_table_exprs,
                                       ObIArray<ObExecParamRawExpr *> &nl_params,
                                       ObIArray<ObRawExpr*> &subquery_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObLogPlan *plan = get_plan();
  for (int64_t i = 0; OB_SUCC(ret) && i < json_table_exprs.count(); ++i) {
    ObSEArray<ObRawExpr *, 1> old_json_exprs;
    ObSEArray<ObRawExpr *, 1> new_json_exprs;
    ObSEArray<ObExecParamRawExpr *, 4> tmp_nl_params;
    if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(stmt = plan->get_stmt()) || OB_ISNULL(json_table_exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(plan), K(ret));
    } else if (OB_FAIL(old_json_exprs.push_back(json_table_exprs.at(i)))) {
      LOG_WARN("failed to push back function table expr", K(ret));
    } else if (OB_FAIL(extract_params_for_inner_path(json_table_exprs.at(i)->get_relation_ids(),
                                                     tmp_nl_params,
                                                     subquery_exprs,
                                                     old_json_exprs,
                                                     new_json_exprs))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (OB_UNLIKELY(new_json_exprs.count() != 1) || OB_ISNULL(new_json_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new values table expr is invalid", K(ret), K(new_json_exprs));
    } else if (OB_FAIL(append(nl_params, tmp_nl_params))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      json_table_exprs.at(i) = new_json_exprs.at(0);
    }
  }
  return ret;
}

int ObJoinOrder::create_one_cte_table_path(const TableItem* table_item,
                                           ObShardingInfo *sharding)
{
  int ret = OB_SUCCESS;
  CteTablePath *ap = NULL;
  if (OB_ISNULL(ap = reinterpret_cast<CteTablePath*>(allocator_->alloc(sizeof(CteTablePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate an AccessPath", K(ret));
  } else {
    ap = new(ap) CteTablePath();
    ap->table_id_ = table_id_;
    ap->ref_table_id_ = table_item->ref_id_;
    ap->parent_ = this;
    ap->contain_fake_cte_ = true;
    ap->strong_sharding_ = sharding;
    ap->contain_match_all_fake_cte_ = (table_item->is_recursive_union_fake_table_ &&
                                       sharding->is_match_all());
    if (OB_FAIL(ap->set_parallel_and_server_info_for_match_all())) {
      LOG_WARN("failed set parallel and server info for match all", K(ret));
    } else if (OB_FAIL(append(ap->filter_, get_restrict_infos()))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(ap->estimate_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else if (OB_FAIL(ap->compute_pipeline_info())) {
      LOG_WARN("failed to compute pipelined path", K(ret));
    } else if (OB_FAIL(add_path(ap))) {
      LOG_WARN("failed to add path", K(ret));
    } else {
      /* do nothing */
    }
  }

  return ret;
}

int ObJoinOrder::estimate_size_and_width_for_fake_cte(uint64_t table_id, ObSelectLogPlan *nonrecursive_plan)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  ObLogicalOperator *nonrecursive_root = NULL;
  double selectivity = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(nonrecursive_plan) || OB_ISNULL(nonrecursive_plan->get_stmt()) ||
      OB_UNLIKELY(!nonrecursive_plan->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(nonrecursive_plan),
              K(nonrecursive_root), K(stmt), K(ret));
  } else if (OB_FAIL(nonrecursive_plan->get_candidate_plans().get_best_plan(nonrecursive_root))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(nonrecursive_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(nonrecursive_root));
  } else if (FALSE_IT(nonrecursive_plan->get_selectivity_ctx().init_op_ctx(nonrecursive_root))) {
  } else if (OB_FAIL(get_plan()->get_basic_table_metas().add_generate_table_meta_info(
      get_plan()->get_stmt(),
      static_cast<const ObSelectStmt *>(nonrecursive_plan->get_stmt()),
      table_id,
      nonrecursive_plan->get_update_table_metas(),
      nonrecursive_plan->get_selectivity_ctx(),
      nonrecursive_root->get_card()))) {
    LOG_WARN("failed to add generate table meta info", K(ret));
  } else if (FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(NULL, nonrecursive_root->get_card()))) {
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_table(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            stmt->get_column_items(),
                                                            table_id,
                                                            output_row_size_))) {
    LOG_WARN("estimate width of row failed", K(table_id), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             get_restrict_infos(),
                                                             selectivity,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
  } else {
    set_output_rows(nonrecursive_root->get_card() * selectivity);
    if (OB_FAIL(ObOptSelectivity::update_table_meta_info(get_plan()->get_basic_table_metas(),
                                                         get_plan()->get_update_table_metas(),
                                                         get_plan()->get_selectivity_ctx(),
                                                         table_id,
                                                         get_output_rows(),
                                                         get_restrict_infos(),
                                                         get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to update table meta info", K(ret));
    }
    LOG_TRACE("estimate rows for fake cte", K(output_rows_), K(get_plan()->get_basic_table_metas()));
  }
  return ret;
}

int ObJoinOrder::generate_cte_table_paths()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get  unexpected null", K(get_plan()), K(stmt), K(allocator_), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_id_));
  } else if (OB_FAIL(estimate_size_and_width_for_fake_cte(table_id_, get_plan()->get_nonrecursive_plan_for_fake_cte()))) {
    LOG_WARN("failed to calc filter selectivities", K(get_restrict_infos()), K(ret));
  } else if (OB_FAIL(create_one_cte_table_path(table_item,
                                               get_plan()->get_optimizer_context().get_match_all_sharding()))) {
    LOG_WARN("failed to create one cte table path", K(ret));
  } else if (table_item->is_recursive_union_fake_table_ &&
             OB_FAIL(create_one_cte_table_path(table_item,
                                               get_plan()->get_optimizer_context().get_local_sharding()))) {
    LOG_WARN("failed to create one cte table path", K(ret));
  }
  return ret;
}

int ObJoinOrder::generate_normal_base_table_paths()
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  helper.is_inner_path_ = false;
  helper.table_opt_info_ = &table_opt_info_;
  if (OB_FAIL(helper.filters_.assign(restrict_info_set_))) {
    LOG_WARN("failed to assign restrict info set", K(ret));
  } else if (OB_FAIL(generate_base_table_paths(helper))) {
    LOG_WARN("failed to generate access paths", K(ret));
  }
  return ret;
}

int ObJoinOrder::generate_base_table_paths(PathHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<AccessPath *, 8> access_paths;
  uint64_t table_id = table_id_;
  uint64_t ref_table_id = table_meta_info_.ref_table_id_;
  ObIndexInfoCache index_info_cache;
  if (!helper.is_inner_path_ &&
      OB_FAIL(compute_base_table_property(table_id, ref_table_id))) {
    LOG_WARN("failed to compute base path property", K(ret));
  } else if (OB_FAIL(create_access_paths(table_id,
                                         ref_table_id,
                                         helper,
                                         access_paths,
                                         index_info_cache))) {
    LOG_WARN("failed to add table to join order(single)", K(ret));
  } else if (OB_FAIL(estimate_size_for_base_table(helper, access_paths))) {
    LOG_WARN("failed to estimate_size", K(ret));
  } else if (OB_FAIL(pruning_unstable_access_path(table_id,
                                                  ref_table_id,
                                                  helper,
                                                  index_info_cache,
                                                  helper.table_opt_info_,
                                                  access_paths))) {
    LOG_WARN("failed to pruning unstable access path", K(ret));
  } else if (OB_FAIL(compute_parallel_and_server_info_for_base_paths(access_paths))) {
    LOG_WARN("failed to compute", K(ret));
  } else if (OB_FAIL(compute_sharding_info_for_base_paths(access_paths, index_info_cache))) {
    LOG_WARN("failed to calc sharding info", K(ret));
  } else if (OB_FAIL(compute_cost_and_prune_access_path(helper, access_paths))) {
    LOG_WARN("failed to compute cost and prune access path", K(ret));
  } else if (OB_FAIL(revise_output_rows_after_creating_path(helper, access_paths))) {
    LOG_WARN("failed to revise output rows after creating path", K(ret));
  } else if (OB_FAIL(append(available_access_paths_, access_paths))) {
    LOG_WARN("failed to append access paths", K(ret));
  }
  return ret;
}

int ObJoinOrder::compute_base_table_property(uint64_t table_id,
                                             uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null or path type",
                K(ret), K(get_plan()), K(stmt));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(restrict_info_set_, output_const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                        restrict_info_set_,
                                                        output_equal_sets_))) {
    LOG_WARN("failed to compute equal set for access path", K(ret));
  } else if (OB_FAIL(compute_fd_item_set_for_table_scan(table_id,
                                                        ref_table_id,
                                                        get_restrict_infos()))) {
    LOG_WARN("failed to extract fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(get_output_const_exprs(),
                                                      get_fd_item_set(),
                                                      get_output_equal_sets(),
                                                      get_output_const_exprs(),
                                                      is_at_most_one_row_))) {
    LOG_WARN("failed to compute at most one row", K(ret));
  } else if (OB_FAIL(compute_table_location(table_id,
                                            ref_table_id,
                                            false,
                                            table_partition_info_))) {
    LOG_WARN("failed to calc table location", K(ret));
  } else if (OB_FAIL(compute_table_meta_info(table_id, ref_table_id))) {
    LOG_WARN("failed to compute table meta info", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_table(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            stmt->get_column_items(),
                                                            table_id_,
                                                            output_row_size_))) {
    LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
  } else {
    LOG_TRACE("succeed to compute base table property",
        K(restrict_info_set_), K(output_const_exprs_),
        K(output_equal_sets_));
  }
  return ret;
}

int ObJoinOrder::pruning_unstable_access_path(const uint64_t table_id,
                                              const uint64_t ref_table_id,
                                              PathHelper &helper,
                                              ObIndexInfoCache &index_info_cache,
                                              BaseTableOptInfo *table_opt_info,
                                              ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<uint64_t, 4> unstable_index_id;
  if (access_paths.count() <= 1) {
    /* do not pruning access path */
  } else if (OB_FAIL(try_pruning_base_table_access_path(table_id,
                                                        ref_table_id,
                                                        helper,
                                                        index_info_cache,
                                                        access_paths,
                                                        unstable_index_id))) {
    LOG_WARN("failed to pruning base table access path", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> available_index_id;
    uint64_t base_table_id = OB_INVALID_ID;
    uint64_t table_id = OB_INVALID_ID;
    AccessPath *ap = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
      if (OB_ISNULL(ap = access_paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(available_index_id.push_back(ap->index_id_))) {
        LOG_WARN("failed to push back index id", K(ret));
      } else if (0 == i) {
        base_table_id = ap->ref_table_id_;
        table_id = ap->table_id_;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(fill_opt_info_index_name(table_id, base_table_id, available_index_id,
                                                         unstable_index_id, table_opt_info))) {
      LOG_WARN("failed to fill opt info index name", K(ret), K(base_table_id),
                                          K(available_index_id), K(unstable_index_id));
    }
  }
  return ret;
}

int ObJoinOrder::try_pruning_base_table_access_path(const uint64_t table_id,
                                                    const uint64_t ref_table_id,
                                                    PathHelper &helper,
                                                    ObIndexInfoCache &index_info_cache,
                                                    ObIArray<AccessPath*> &access_paths,
                                                    ObIArray<uint64_t> &unstable_index_id)
{
  int ret = OB_SUCCESS;
  bool need_prune = false;
  ObSEArray<int64_t, 2> none_range_path_positions;
  AccessPath *ap = NULL;
  const QueryRangeInfo *query_range_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    if (OB_ISNULL(ap = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ap->is_index_merge_path()) {
      // do nothing
    } else if (OB_FAIL(index_info_cache.get_query_range(table_id, ap->index_id_,
                                                 query_range_info))) {
      LOG_WARN("get_range_columns failed", K(ret));
    } else if (OB_ISNULL(query_range_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query_range_info should not be null", K(ret));
    } else if (ap->range_prefix_count_ <= 0 &&
               !query_range_info->get_contain_always_false() &&
               !ap->domain_idx_info_.vec_extra_info_.is_post_filter() &&
               OB_FAIL(none_range_path_positions.push_back(i))) {
      LOG_WARN("failed to push back pos", K(ret));
    } else {
      need_prune |= ap->range_prefix_count_ > 0 &&
                    ap->get_logical_query_range_row_count() < PRUNING_ROW_COUNT_THRESHOLD;
      need_prune |= ap->range_prefix_count_ > 0 &&
                    ap->est_cost_info_.index_meta_info_.is_geo_index_;
    }
  }

  if (OB_SUCC(ret) && need_prune) {
    for (int64_t i = none_range_path_positions.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      int64_t base_path_pos = none_range_path_positions.at(i);
      if (OB_ISNULL(ap = access_paths.at(base_path_pos))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pos or access path", K(ret), K(base_path_pos),
                                                  K(access_paths.count()), K(ap));
      } else if (ap->range_prefix_count_ > 0) {
        /* do nothing */
      } else if (OB_FAIL(access_paths.remove(base_path_pos))) {
        LOG_WARN("failed to remove access path", K(ret), K(base_path_pos));
      } else if (OB_FAIL(unstable_index_id.push_back(ap->index_id_))) {
        LOG_WARN("failed to push back index id", K(ret));
      } else {
        LOG_TRACE("pruned none query range access paths", K(*ap));
      }
    }
  }

  ObSEArray<uint64_t, 8> valid_index_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    if (OB_ISNULL(ap = access_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ap->is_index_merge_path()) {
      // do nothing
    } else if (ObOptimizerUtil::find_item(valid_index_ids, ap->index_id_)) {
    } else if (OB_FAIL(valid_index_ids.push_back(ap->index_id_))) {
      LOG_WARN("failed to push back index id", K(ret));
    }
  }
  if (OB_SUCC(ret) && need_prune) {
    const ObDMLStmt *stmt = NULL;
    ObSEArray<uint64_t, 8> skyline_index_ids;
    if (OB_ISNULL(get_plan()) ||
        OB_ISNULL(stmt = get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null stmt", K(ret));
    } else if (OB_FAIL(skyline_prunning_index(table_id,
                                              ref_table_id,
                                              stmt,
                                              true,
                                              index_info_cache,
                                              valid_index_ids,
                                              skyline_index_ids,
                                              helper.filters_,
                                              true))) {
      LOG_WARN("failed to pruning_index", K(table_id), K(ref_table_id), K(ret));
    }
    for (int i = access_paths.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_ISNULL(ap = access_paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (ap->is_index_merge_path()) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(skyline_index_ids, ap->index_id_)) {
      } else if (OB_FAIL(unstable_index_id.push_back(ap->index_id_))) {
        LOG_WARN("failed to push back index id", K(ret));
      } else if (OB_FAIL(access_paths.remove(i))) {
        LOG_WARN("failed to remove access path", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_temp_table_paths()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  TempTablePath *temp_table_path = NULL;
  ObLogicalOperator *temp_table_root = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table_id_), K(table_item));
  } else if (OB_ISNULL(temp_table_root = get_plan()->get_optimizer_context().
                                         get_temp_table_plan(table_item->ref_query_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(temp_table_root), K(ret));
  } else if (OB_ISNULL(temp_table_path = reinterpret_cast<TempTablePath*>(
                       allocator_->alloc(sizeof(TempTablePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate an AccessPath", K(ret));
  } else {
    temp_table_path = new(temp_table_path) TempTablePath();
    temp_table_path->table_id_ = table_id_;
    temp_table_path->temp_table_id_ = table_item->ref_id_;
    temp_table_path->root_ = temp_table_root;
    temp_table_path->parent_ = this;
    if (OB_FAIL(append(temp_table_path->filter_, get_restrict_infos()))) {
      LOG_WARN("failed to push back restrict infos.", K(ret));
    } else if (OB_FAIL(compute_subquery_property(table_id_,
                                                 temp_table_root))) {
      LOG_WARN("failed to generate subquery property", K(ret));
    } else if (OB_FAIL(compute_subquery_path_property(table_id_,
                                                      temp_table_root,
                                                      temp_table_path))) {
      LOG_WARN("failed to generate subquery paths", K(ret));
    } else if (OB_FAIL(temp_table_path->compute_sharding_info())) {
      LOG_WARN("failed to reset temp table partition keys", K(ret));
    } else if (OB_FAIL(temp_table_path->compute_path_ordering())) {
      LOG_WARN("failed to compute path ordering", K(ret));
    } else if (OB_FAIL(temp_table_path->estimate_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else if (OB_FAIL(temp_table_path->compute_pipeline_info())) {
      LOG_WARN("failed to compute pipelined path", K(ret));
    } else if (OB_FAIL(add_path(temp_table_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else if (ObShardingInfo::is_shuffled_server_list(temp_table_path->server_list_)) {
      /** Two temp tables with shuffled server list might not be in the same real server list
       *  TEMP1 (shuffled : s1,s2)    TEMP2 (shuffled : s3,s4)
       *             |                           |
       *          GROUPY BY                   GROUP BY
       *             |                           |
       *        HASH EXCHANGE               HASH EXCHANGE
       *             |                           |
       *      TABLE SCAN(s1, s2)         TABLE SCAN(s3, s4)
       *  TEMP1 and TEMP2 should not be union all by ext partition wise
      */
      temp_table_path->server_list_.reuse();
    }
  }
  return ret;
}

int ObJoinOrder::extract_necessary_pushdown_quals(ObIArray<ObRawExpr *> &candi_quals,
                                                  ObIArray<ObRawExpr *> &necessary_pushdown_quals,
                                                  ObIArray<ObRawExpr *> &unnecessary_pushdown_quals)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt));
  } else if (stmt->is_hierarchical_query()) {
    /**
      * PredicateMoveAround skipped hierarchical query,
      * so we push down all of its quals here.
    */
    if (OB_FAIL(append(necessary_pushdown_quals, candi_quals))) {
      LOG_WARN("failed to append", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_quals.count(); i ++) {
      ObRawExpr *expr = candi_quals.at(i);
      /**
       * Only push down such filters here:
       * a. Filters contain dynamic param, such as onetime exprs: `a = ?`
       *      select * from (select * from t) where a = (select max(b) from t);
       * b. Filters generated by optimizer: `v1.a = 1 or v1.a = 2`
       *      select * from (select * from t) v1, v2 where (v1.a = 1 and v1.b = v2.b) or v1.a = 2;
       * c. Filters push down by upper log plan
       *
       * case a and case b should be removed after they can be push down in the transformer
      */
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (expr->has_flag(CNT_DYNAMIC_PARAM) ||
                ObOptimizerUtil::find_item(get_plan()->get_new_or_quals(), expr) ||
                ObOptimizerUtil::find_item(get_plan()->get_pushdown_filters(), expr)) {
        if (OB_FAIL(necessary_pushdown_quals.push_back(expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        if (OB_FAIL(unnecessary_pushdown_quals.push_back(expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_normal_subquery_paths()
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  bool can_pushdown = false;
  const ObDMLStmt *parent_stmt = NULL;
  const TableItem *table_item = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSEArray<ObRawExpr*, 4> candi_pushdown_quals;
  ObSEArray<ObRawExpr*, 4> candi_nonpushdown_quals;
  helper.is_inner_path_ = false;
  LOG_TRACE("start to generate normal subquery path", K(table_id_));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(table_item = parent_stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(table_item), K(ret));
  } else if (OB_ISNULL(helper.child_stmt_ = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(extract_necessary_pushdown_quals(get_restrict_infos(),
                                                      candi_pushdown_quals,
                                                      candi_nonpushdown_quals))) {
    LOG_WARN("failed to classify push down quals", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::pushdown_and_rename_filter_into_subquery(*parent_stmt,
                                                                               *helper.child_stmt_,
                                                                               table_id_,
                                                                               get_plan()->get_optimizer_context(),
                                                                               candi_pushdown_quals,
                                                                               helper.pushdown_filters_,
                                                                               helper.filters_,
                                                                               /*check_match_index*/false))) {
        LOG_WARN("failed to push down filter into subquery", K(ret));
  } else if (OB_FAIL(append(helper.filters_, candi_nonpushdown_quals))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_onetime_exprs(helper.pushdown_filters_,
                                                        helper.exec_params_))) {
    LOG_WARN("failed to get onetime exprs", K(ret));
  } else if (OB_FAIL(generate_subquery_paths(helper))) {
    LOG_WARN("failed to generate subquery path", K(ret));
  }
  LOG_TRACE("succed to generate normal subquery path", K(table_id_), K(interesting_paths_));
  return ret;
}

int ObJoinOrder::generate_subquery_paths(PathHelper &helper)
{
  int ret = OB_SUCCESS;
  double inner_row_count = 0.0;
  ObLogPlan* log_plan = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  const ObDMLStmt *child_stmt = NULL;
  ObLogicalOperator *best_child_plan = NULL;
  ObSEArray<ObExecParamRawExpr *, 4> pushdown_onetimes;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(child_stmt = static_cast<const ObDMLStmt*>(helper.child_stmt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(child_stmt), K(ret));
  } else if (OB_ISNULL(log_plan = get_plan()->get_optimizer_context().get_log_plan_factory().create(
             get_plan()->get_optimizer_context(), *child_stmt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create plan", K(ret));
  } else if (OB_FAIL(log_plan->add_pushdown_filters(helper.pushdown_filters_))) {
    LOG_WARN("failed to add pushdown filters", K(ret));
  } else if (OB_FAIL(log_plan->add_exec_params_meta(helper.exec_params_,
                                                    get_plan()->get_basic_table_metas(),
                                                    get_plan()->get_selectivity_ctx()))) {
    LOG_WARN("failed to prepare opt exec param meta", K(ret));
  } else if (OB_FAIL(log_plan->init_rescan_info_for_subquery_paths(*get_plan(),
                                                                   helper.is_inner_path_,
                                                                   helper.is_semi_anti_join_))) {
    LOG_WARN("failed to init rescan info", K(ret));
  } else {
    log_plan->set_is_subplan_scan(true);
    log_plan->set_nonrecursive_plan_for_fake_cte(get_plan()->get_nonrecursive_plan_for_fake_cte());
    if (parent_stmt->is_insert_stmt()) {
      log_plan->set_insert_stmt(static_cast<const ObInsertStmt*>(parent_stmt));
    }
    if (OB_FAIL(log_plan->generate_raw_plan())) {
      LOG_WARN("failed to optimize sub-select", K(ret));
    } else if (OB_FAIL(log_plan->get_candidate_plans().get_best_plan(best_child_plan))) {
      LOG_WARN("failed to get best plan", K(ret));
    } else if (OB_ISNULL(best_child_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!helper.is_inner_path_ &&
               OB_FAIL(compute_subquery_property(table_id_,
                                                 best_child_plan))) {
      LOG_WARN("failed to generate subquery property", K(ret));
    } else if (helper.is_inner_path_ &&
               OB_FAIL(estimate_size_for_inner_subquery_path(best_child_plan->get_card(),
                                                             helper.filters_,
                                                             inner_row_count))) {
      LOG_WARN("failed to estimate size for inner subquery path", K(ret));
    } else {
      ObIArray<CandidatePlan> &candidate_plans = log_plan->get_candidate_plans().candidate_plans_;
      for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
        ObLogicalOperator *root = NULL;
        SubQueryPath *sub_path = NULL;
        if (OB_ISNULL(root = candidate_plans.at(i).plan_tree_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(root), K(ret));
        } else if (OB_ISNULL(sub_path = static_cast<SubQueryPath*>(allocator_->alloc(sizeof(SubQueryPath))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate subquery path", K(ret));
        } else {
          sub_path = new (sub_path) SubQueryPath(root);
          sub_path->subquery_id_ = table_id_;
          sub_path->parent_ = this;
          if (OB_FAIL(append(sub_path->filter_, helper.filters_))) {
            LOG_WARN("failed to append expr", K(ret));
          } else if (OB_FAIL(sub_path->subquery_exprs_.assign(helper.subquery_exprs_))) {
            LOG_WARN("failed to assign exprs", K(ret));
          } else if (OB_FAIL(compute_subquery_path_property(table_id_,
                                                            root,
                                                            sub_path))) {
            LOG_WARN("failed to generate subquery property", K(ret));
          } else if (OB_FAIL(sub_path->estimate_cost())) {
            LOG_WARN("failed to calculate cost of subquery path", K(ret));
          } else if (OB_FAIL(sub_path->compute_pipeline_info())) {
            LOG_WARN("failed to compute pipelined path", K(ret));
          } else if (helper.is_inner_path_) {
            if (OB_FAIL(helper.inner_paths_.push_back(sub_path))) {
              LOG_WARN("failed to push back inner path", K(ret));
            } else {
              sub_path->inner_row_count_ = inner_row_count;
              LOG_TRACE("succeed to generate inner subquery path", K(table_id_), K(sub_path->get_ordering()));
            }
          } else if (OB_FAIL(add_path(sub_path))) {
            LOG_WARN("failed to add path", K(ret));
          } else {
            LOG_TRACE("succeed to generate normal subquery path", K(table_id_), K(sub_path->get_ordering()));
          }
        }
      }
    }
  }
  LOG_TRACE("succed to generate normal subquery path", K(table_id_), K(interesting_paths_));
  return ret;
}

// generate physical property for each subquery path, including ordering, sharding
int ObJoinOrder::compute_subquery_path_property(const uint64_t table_id,
                                                 ObLogicalOperator *root,
                                                 Path *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(root) || OB_ISNULL(root->get_stmt()) || OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root), K(path), K(allocator_), K(ret));
  } else if (OB_UNLIKELY(!root->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(ret));
  } else {
    int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
    const ObDMLStmt *parent_stmt = get_plan()->get_stmt();
    bool is_inherited_sharding = false;
    if (OB_FAIL(convert_subplan_scan_order_item(*get_plan(),
                                                *root,
                                                table_id,
                                                path->get_ordering()))) {
      LOG_WARN("failed to convert subplan scan order item", K(ret));
    } else if (OB_FAIL(convert_subplan_scan_sharding_info(*get_plan(),
                                                          *root,
                                                          table_id,
                                                          path->strong_sharding_,
                                                          path->weak_sharding_,
                                                          is_inherited_sharding))) {
      LOG_WARN("failed to convert subplan scan sharding info", K(ret));
    } else if (OB_FAIL(check_all_interesting_order(path->get_ordering(),
                                                   parent_stmt,
                                                   interesting_order_info))) {
      LOG_WARN("failed to check all interesting order", K(ret));
    } else {
      path->set_interesting_order_info(interesting_order_info);
      path->is_local_order_ = root->get_is_local_order();
      path->is_range_order_ = root->get_is_range_order();
      path->exchange_allocated_ = root->is_exchange_allocated();
      path->phy_plan_type_ = root->get_phy_plan_type();
      path->location_type_ = root->get_location_type();
      path->contain_fake_cte_ = root->get_contains_fake_cte();
      path->contain_pw_merge_op_ = root->get_contains_pw_merge_op();
      path->contain_match_all_fake_cte_ = root->get_contains_match_all_fake_cte();
      path->contain_das_op_ = root->get_contains_das_op();
      path->parallel_ = root->get_parallel();
      path->server_cnt_ = root->get_server_cnt();
      path->available_parallel_ = root->get_available_parallel();
      path->inherit_sharding_index_ = is_inherited_sharding ? 0 : -1;
      if (OB_FAIL(path->server_list_.assign(root->get_server_list()))) {
        LOG_WARN("failed to assign subquery path server list", K(ret));
      }
    }
  }
  return ret;
}

// generate logical property for each subquery, including const exprs, fd item sets
int ObJoinOrder::compute_subquery_property(const uint64_t table_id,
                                           ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(root) || OB_ISNULL(root->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root), K(ret));
  } else if (OB_UNLIKELY(!root->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(ret));
  } else if (OB_FAIL(estimate_size_and_width_for_subquery(table_id, root))) {
    LOG_WARN("failed to estimate size and width for subquery", K(ret));
  } else if (OB_FAIL(compute_const_exprs_for_subquery(table_id, root))) {
    LOG_WARN("failed to compute const exprs for subquery", K(ret));
  } else if (OB_FAIL(compute_equal_set_for_subquery(table_id, root))) {
    LOG_WARN("failed to compute equal set condition", K(ret));
  } else if (OB_FAIL(compute_fd_item_set_for_subquery(table_id, root))) {
    LOG_WARN("failed to compute fd item set for subplan scan", K(ret));
  } else {
    is_at_most_one_row_ = root->get_is_at_most_one_row();
  }
  return ret;
}

int ObJoinOrder::estimate_size_for_inner_subquery_path(double root_card,
                                                       const ObIArray<ObRawExpr*> &filters,
                                                       double &output_card)
{
  int ret = OB_SUCCESS;
  double selectivity = 0.0;
  if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                      get_plan()->get_selectivity_ctx(),
                                                      filters,
                                                      selectivity,
                                                      get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("Failed to calc filter selectivities", K(filters), K(ret));
  } else {
    output_card = root_card * selectivity;
    LOG_TRACE("estimate rows for inner subplan path", K(root_card), K(selectivity), K(output_card));
  }
  return ret;
}

int ObJoinOrder::init_join_order(const ObJoinOrder *left_tree,
                                 const ObJoinOrder *right_tree,
                                 const JoinInfo *join_info,
                                 const common::ObIArray<ObConflictDetector*> &detectors)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) ||
      OB_ISNULL(join_info) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(left_tree), K(right_tree),
        K(allocator_), K(ret));
  } else {
    //设置table_set_为左右的集合
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

    if (FAILEDx(merge_ambient_card(left_tree->get_ambient_card(),
                                   right_tree->get_ambient_card(),
                                   ambient_card_))) {
      LOG_WARN("failed to merge rowcnts", K(ret));
    } else {
      set_output_rows(-1.0);
    }

    //设置join info
    if (OB_SUCC(ret)) {
      JoinInfo* temp_join_info = NULL;
      if (OB_ISNULL(temp_join_info = static_cast<JoinInfo*>(
                         allocator_->alloc(sizeof(JoinInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create outer join info", K(ret));
      } else {
        temp_join_info = new (temp_join_info) JoinInfo(join_info->join_type_);
        if (OB_FAIL(temp_join_info->table_set_.add_members(join_info->table_set_))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (OB_FAIL(temp_join_info->on_conditions_.assign(join_info->on_conditions_))) {
          LOG_WARN("failed to assign on condition", K(ret));
        } else if (OB_FAIL(temp_join_info->where_conditions_.assign(join_info->where_conditions_))) {
          LOG_WARN("failed to assign where condition", K(ret));
        } else if (OB_FAIL(temp_join_info->equal_join_conditions_.assign(join_info->equal_join_conditions_))) {
          LOG_WARN("failed to assign equal join condition", K(ret));
        } else {
          join_info_ = temp_join_info;
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool has_rownum = false;
      if (left_tree->get_cnt_rownum() || right_tree->get_cnt_rownum()) {
        set_cnt_rownum(true);
      } else if (OB_FAIL(ObTransformUtils::check_has_rownum(join_info->on_conditions_,
                                                            has_rownum))) {
        LOG_WARN("failed to check has rownum", K(ret));
      } else if (!has_rownum && OB_FAIL(ObTransformUtils::check_has_rownum(join_info->where_conditions_,
                                                            has_rownum))) {
        LOG_WARN("failed to check has rownum", K(ret));
      } else if (!has_rownum &&
                 OB_FAIL(ObTransformUtils::check_has_rownum(join_info->equal_join_conditions_,
                                                            has_rownum))) {
        LOG_WARN("failed to check has rownum", K(ret));
      } else {
        set_cnt_rownum(has_rownum);
      }
    }

    if (OB_SUCC(ret)) {
      //outer join的join qual暂存于restrict info，用于equal set、const expr计算
      if (IS_OUTER_OR_CONNECT_BY_JOIN(join_info->join_type_)) {
        if (OB_FAIL(append(get_restrict_infos(), join_info->where_conditions_))) {
          LOG_WARN("failed to append restrict info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(merge_conflict_detectors(const_cast<ObJoinOrder*>(left_tree),
                                           const_cast<ObJoinOrder*>(right_tree),
                                           detectors))) {
        LOG_WARN("failed to merge conflict detectors", K(ret));
      } else if (OB_FAIL(compute_join_property(left_tree, right_tree, join_info))) {
        LOG_WARN("failed to compute join property", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::compute_join_property(const ObJoinOrder *left_tree,
                                       const ObJoinOrder *right_tree,
                                       const JoinInfo *join_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(join_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(join_info), K(ret));
  } else if (OB_FAIL(estimate_size_and_width_for_join(left_tree, right_tree, join_info->join_type_))) {
    LOG_WARN("failed to estimate_size", K(ret));
  } else if (OB_FAIL(compute_const_exprs_for_join(left_tree, right_tree, join_info->join_type_))) {
    LOG_WARN("failed to compute const exprs for join", K(ret));
  } else if (OB_FAIL(compute_equal_set_for_join(left_tree, right_tree, join_info->join_type_))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else if (OB_FAIL(compute_fd_item_set_for_join(left_tree,
                                                  right_tree,
                                                  join_info,
                                                  join_info->join_type_))) {
    LOG_WARN("failed to compute fd item set for join", K(ret));
  } else if (OB_FAIL(compute_one_row_info_for_join(left_tree,
                                                   right_tree,
                                                   IS_OUTER_OR_CONNECT_BY_JOIN(join_info->join_type_) ?
                                                   join_info->on_conditions_ : join_info->where_conditions_,
                                                   join_info->equal_join_conditions_,
                                                   join_info->join_type_))) {
    LOG_WARN("failed to compute one row info for join", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

/*
 * We currently support left-deep tree, right_deep tree and zigzag tree
 */
int ObJoinOrder::generate_join_paths(const ObJoinOrder &left_tree,
                                     const ObJoinOrder &right_tree,
                                     const JoinInfo &join_info,
                                     bool force_ordered)
{
  int ret = OB_SUCCESS;
  EqualSets equal_sets;
  ValidPathInfo path_info;
  ValidPathInfo reverse_path_info;
  ObJoinType join_type = join_info.join_type_;
  ObJoinType reverse_join_type = get_opposite_join_type(join_info.join_type_);
  bool is_connect_by = (join_info.join_type_ == CONNECT_BY_JOIN);
  typedef ObSEArray<ObSEArray<Path*, 16>, 4> PathArray;
  reverse_path_info.is_reverse_path_ = true;
  SMART_VARS_2((PathArray, left_paths),
               (PathArray, right_paths)) {
    if (OB_FAIL(append(equal_sets, left_tree.get_output_equal_sets())) ||
        OB_FAIL(append(equal_sets, right_tree.get_output_equal_sets()))) {
      LOG_WARN("failed to append equal sets", K(ret));
    } else if (OB_FAIL(classify_paths_based_on_sharding(left_tree.get_interesting_paths(),
                                                        left_tree.get_output_equal_sets(),
                                                        left_paths))) {
      LOG_WARN("failed to classify paths based on sharding", K(ret));
    } else if (OB_FAIL(classify_paths_based_on_sharding(right_tree.get_interesting_paths(),
                                                        right_tree.get_output_equal_sets(),
                                                        right_paths))) {
      LOG_WARN("failed to classify paths based on sharding", K(ret));
    } else if (OB_FAIL(get_valid_path_info(left_tree,
                                          right_tree,
                                          join_type,
                                          IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ?
                                          join_info.on_conditions_ :
                                          join_info.where_conditions_,
                                          false,
                                          false,
                                          path_info))) {
      LOG_WARN("failed to get valid path types", K(join_info.join_type_), K(ret));
    } else if (!is_connect_by && !force_ordered
              && OB_FAIL(get_valid_path_info(right_tree,
                                              left_tree,
                                              reverse_join_type,
                                              IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ?
                                              join_info.on_conditions_ :
                                              join_info.where_conditions_,
                                              false,
                                              true,
                                              reverse_path_info))) {
      LOG_WARN("failed to get valid path types", K(join_info.join_type_), K(ret));
    } else if (OB_FAIL(inner_generate_join_paths(left_tree,
                                                right_tree,
                                                equal_sets,
                                                left_paths,
                                                right_paths,
                                                join_info.on_conditions_,
                                                join_info.where_conditions_,
                                                path_info,
                                                reverse_path_info))) {
      LOG_WARN("failed to generate join paths", K(ret));
    } else if (interesting_paths_.count() > 0) {
      OPT_TRACE("succeed to generate join paths using hint");
      LOG_TRACE("succeed to generate join paths using hint", K(path_info), K(reverse_path_info), K(ret));
    } else if (OB_FAIL(get_plan()->get_log_plan_hint().check_status())) {
      LOG_WARN("failed to generate join paths with hint", K(ret));
    } else if (FALSE_IT(path_info.reset()) ||
              FALSE_IT(reverse_path_info.reset())) {
      /*do nothing*/
    } else if (OB_FAIL(get_valid_path_info(left_tree,
                                          right_tree,
                                          join_type,
                                          IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ?
                                          join_info.on_conditions_ :
                                          join_info.where_conditions_,
                                          true,
                                          false,
                                          path_info))) {
      LOG_WARN("failed to get valid path types", K(join_info.join_type_), K(ret));
    } else if (!is_connect_by && OB_FAIL(get_valid_path_info(right_tree,
                                                            left_tree,
                                                            reverse_join_type,
                                                            IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ?
                                                            join_info.on_conditions_ :
                                                            join_info.where_conditions_,
                                                            true,
                                                            true,
                                                            reverse_path_info))) {
      LOG_WARN("failed to get valid path types", K(join_info.join_type_), K(ret));
    } else if (OB_FAIL(inner_generate_join_paths(left_tree,
                                                right_tree,
                                                equal_sets,
                                                left_paths,
                                                right_paths,
                                                join_info.on_conditions_,
                                                join_info.where_conditions_,
                                                path_info,
                                                reverse_path_info))) {
      LOG_WARN("failed to generate join path", K(ret));
    } else if (interesting_paths_.empty()) {
      bool is_batch_stmt = get_plan()->get_optimizer_context().is_batched_multi_stmt();
      if (is_batch_stmt) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_TRACE("no validated join paths for batch stmt, need to rollback", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate join paths by ignoring hint", K(ret));
      }
    } else {
      OPT_TRACE("succeed to generate join paths by ignoring hint");
      LOG_TRACE("succeed to generate join paths by ignoring hint", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::classify_paths_based_on_sharding(const ObIArray<Path*> &input_paths,
                                                  const EqualSets &equal_sets,
                                                  ObIArray<ObSEArray<Path*, 16>> &output_list)
{
  int ret = OB_SUCCESS;
  Path *first_path = NULL;
  Path *second_path = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_paths.count(); i++) {
    if (OB_ISNULL(first_path = input_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(first_path), K(ret));
    } else {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < output_list.count(); j++) {
        bool is_equal = false;
        ObIArray<Path*> &path_list = output_list.at(j);
        if (OB_UNLIKELY(path_list.empty()) || OB_ISNULL(second_path = path_list.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(path_list.count()), K(second_path), K(ret));
        } else if (first_path->parallel_ != second_path->parallel_) {
          /*do nothing*/
        } else if (first_path->exchange_allocated_ != second_path->exchange_allocated_) {
          /*do nothing*/
        } else if (first_path->contain_pw_merge_op() != second_path->contain_pw_merge_op()) {
          /*do nothing*/
        } else if (OB_FAIL(ObShardingInfo::is_sharding_equal(first_path->get_strong_sharding(),
                                                             first_path->get_weak_sharding(),
                                                             second_path->get_strong_sharding(),
                                                             second_path->get_weak_sharding(),
                                                             equal_sets,
                                                             is_equal))) {
          LOG_WARN("failed to check whether sharding is equal", K(ret));
        } else if (!is_equal) {
          /*do nothing*/
        } else if (OB_FAIL(path_list.push_back(first_path))) {
          LOG_WARN("failed to push back first_path list", K(ret));
        } else {
          is_find = true;
        }
      }
      if (OB_SUCC(ret) && !is_find) {
        ObSEArray<Path*, 16> path_list;
        if (OB_FAIL(path_list.push_back(first_path))) {
          LOG_WARN("failed to push back first_path list", K(ret));
        } else if (OB_FAIL(output_list.push_back(path_list))) {
          LOG_WARN("failed to push back path list", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObJoinOrder::inner_generate_join_paths(const ObJoinOrder &left_tree,
                                           const ObJoinOrder &right_tree,
                                           const EqualSets &equal_sets,
                                           const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                                           const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                                           const ObIArray<ObRawExpr*> &on_conditions,
                                           const ObIArray<ObRawExpr*> &where_conditions,
                                           const ValidPathInfo &path_info,
                                           const ValidPathInfo &reverse_path_info)
{
  int ret = OB_SUCCESS;
  bool has_non_nl_path = false;
  bool has_equal_condition = false;
  double equal_cond_sel = 1.0;
  double other_cond_sel = 1.0;
  ObSEArray<ObRawExpr*, 4> merge_join_conditions;
  ObSEArray<ObRawExpr*, 4> merge_join_filters;
  ObSEArray<ObRawExpr*, 4> merge_filters;
  ObSEArray<ObRawExpr*, 4> left_merge_keys;
  ObSEArray<ObRawExpr*, 4> right_merge_keys;
  ObSEArray<bool, 4> null_safe_merge_info;
  ObSEArray<ObRawExpr*, 4> hash_join_conditions;
  ObSEArray<ObRawExpr*, 4> hash_join_filters;
  ObSEArray<ObRawExpr*, 4> hash_filters;
  ObSEArray<ObRawExpr*, 4> left_hash_keys;
  ObSEArray<ObRawExpr*, 4> right_hash_keys;
  // for naaj NLJ to avoid partition wise && pkey
  ObSEArray<ObRawExpr*, 4> empty_left_hash_keys;
  ObSEArray<ObRawExpr*, 4> empty_right_hash_keys;
  ObSEArray<bool, 4> empty_null_safe_hash_info;

  ObSEArray<bool, 4> null_safe_hash_info;
  int64_t path_number = interesting_paths_.count();
  NullAwareAntiJoinInfo naaj_info;
  LOG_TRACE("valid join path types", K(path_info), K(reverse_path_info));
  if (OB_FAIL(classify_mergejoin_conditions(left_tree,
                                            right_tree,
                                            path_info.join_type_,
                                            on_conditions,
                                            where_conditions,
                                            merge_join_conditions,
                                            merge_join_filters,
                                            merge_filters))) {
    LOG_WARN("failed to classify merge join conditions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(merge_join_conditions,
                                                     left_tree.get_tables(),
                                                     left_merge_keys,
                                                     right_merge_keys,
                                                     null_safe_merge_info))) {
    LOG_WARN("failed to get equal join keys", K(ret));
  } else if (OB_FAIL(classify_hashjoin_conditions(left_tree,
                                                  right_tree,
                                                  path_info.join_type_,
                                                  on_conditions,
                                                  where_conditions,
                                                  hash_join_conditions,
                                                  hash_join_filters,
                                                  hash_filters,
                                                  naaj_info))) {
    LOG_WARN("failed to classify hash join conditions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(hash_join_conditions,
                                                     left_tree.get_tables(),
                                                     left_hash_keys,
                                                     right_hash_keys,
                                                     null_safe_hash_info))) {
    LOG_WARN("failed to get equal join keys", K(ret));
  } else {
    has_equal_condition = !merge_join_conditions.empty() || !hash_join_conditions.empty();
    if (!has_equal_condition) {
      OPT_TRACE("do not have equal join condition, will not use merge/hash join");
    }
  }

  // generate hash join paths
  if (OB_SUCC(ret) && !hash_join_conditions.empty() &&
      ((HASH_JOIN & path_info.local_methods_) ||
      (HASH_JOIN & reverse_path_info.local_methods_))) {
    LOG_TRACE("start to generate hash join paths");
    get_plan()->get_selectivity_ctx().init_join_ctx(path_info.join_type_,
                                                    &left_tree.get_tables(),
                                                    &right_tree.get_tables(),
                                                    left_tree.get_output_rows(),
                                                    right_tree.get_output_rows());
    if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
        get_plan()->get_update_table_metas(),
        get_plan()->get_selectivity_ctx(),
        hash_join_conditions,
        equal_cond_sel,
        get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret), K(hash_join_conditions));
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
        get_plan()->get_update_table_metas(),
        get_plan()->get_selectivity_ctx(),
        hash_join_filters,
        other_cond_sel,
        get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret), K(hash_join_filters));
    } else if (FALSE_IT(naaj_info.set_is_sna(path_info.join_type_, false))) {
    } else if ((HASH_JOIN & path_info.local_methods_) &&
               OB_FAIL(generate_hash_paths(equal_sets,
                                           left_paths,
                                           right_paths,
                                           left_hash_keys,
                                           right_hash_keys,
                                           null_safe_hash_info,
                                           hash_join_conditions,
                                           hash_join_filters,
                                           hash_filters,
                                           equal_cond_sel,
                                           other_cond_sel,
                                           path_info,
                                           naaj_info))) {
      LOG_WARN("failed to generate hash join paths", K(ret));
    } else if (FALSE_IT(naaj_info.set_is_sna(reverse_path_info.join_type_, true))) {
    } else if ((HASH_JOIN & reverse_path_info.local_methods_) &&
               OB_FAIL(generate_hash_paths(equal_sets,
                                           right_paths,
                                           left_paths,
                                           right_hash_keys,
                                           left_hash_keys,
                                           null_safe_hash_info,
                                           hash_join_conditions,
                                           hash_join_filters,
                                           hash_filters,
                                           equal_cond_sel,
                                           other_cond_sel,
                                           reverse_path_info,
                                           naaj_info))) {
      LOG_WARN("failed to generate hash join paths", K(ret));
    } else {
      int64_t hash_join_path_num = interesting_paths_.count() - path_number;
      path_number = interesting_paths_.count();
      if (path_number > 0) {
        has_non_nl_path = true;
      }
      OPT_TRACE("generate", hash_join_path_num, "hash join path");
      LOG_TRACE("succeed to generate hash join paths", K(hash_join_conditions.count()),
          "hash_path_count", hash_join_path_num);
    }
  }
  // generate nest loop join paths
  if (OB_SUCC(ret) && ((NESTED_LOOP_JOIN & path_info.local_methods_) ||
                       (NESTED_LOOP_JOIN & reverse_path_info.local_methods_))) {
    LOG_TRACE("start to generate nested loop join path");
    if ((NESTED_LOOP_JOIN & path_info.local_methods_) &&
        OB_FAIL(generate_nl_paths(equal_sets,
                                  left_paths,
                                  right_paths,
                                  naaj_info.is_naaj_ ? empty_left_hash_keys
                                                       : left_hash_keys,
                                  naaj_info.is_naaj_ ? empty_right_hash_keys
                                                       : right_hash_keys,
                                  naaj_info.is_naaj_ ? empty_null_safe_hash_info
                                                       : null_safe_hash_info,
                                  on_conditions,
                                  where_conditions,
                                  path_info,
                                  has_non_nl_path,
                                  has_equal_condition))) {
      LOG_WARN("failed to generate nested loop join path", K(ret));
    } else if ((NESTED_LOOP_JOIN & reverse_path_info.local_methods_) &&
               OB_FAIL(generate_nl_paths(equal_sets,
                                         right_paths,
                                         left_paths,
                                         naaj_info.is_naaj_ ? empty_right_hash_keys
                                                              : right_hash_keys,
                                         naaj_info.is_naaj_ ? empty_left_hash_keys
                                                              : left_hash_keys,
                                         naaj_info.is_naaj_ ? empty_null_safe_hash_info
                                                              : null_safe_hash_info,
                                         on_conditions,
                                         where_conditions,
                                         reverse_path_info,
                                         has_non_nl_path,
                                         has_equal_condition))) {
      LOG_WARN("failed to generate nested loop join path", K(ret));
    } else {
      int64_t nl_join_path_num = interesting_paths_.count() - path_number;
      path_number = interesting_paths_.count();
      OPT_TRACE("generate", nl_join_path_num, "nl join path");
      LOG_TRACE("succeed to generate all nested loop join path",
                "nl_path_count", nl_join_path_num);
    }
  }
  // generate merge join paths
  if (OB_SUCC(ret) && !merge_join_conditions.empty() &&
      ((MERGE_JOIN & path_info.local_methods_) ||
       (MERGE_JOIN & reverse_path_info.local_methods_))) {
    LOG_TRACE("start to generate merge join paths");
    ObArenaAllocator allocator;
    bool can_ignore_merge_plan = !(interesting_paths_.empty() || !path_info.prune_mj_);
    typedef ObSEArray<ObSEArray<MergeKeyInfo*, 16>, 4> MergeKeyInfoArray;
    SMART_VARS_2((MergeKeyInfoArray, left_merge_infos),
                 (MergeKeyInfoArray, right_merge_infos)) {
      get_plan()->get_selectivity_ctx().init_join_ctx(path_info.join_type_,
                                                      &left_tree.get_tables(),
                                                      &right_tree.get_tables(),
                                                      left_tree.get_output_rows(),
                                                      right_tree.get_output_rows());
      if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
          get_plan()->get_update_table_metas(),
          get_plan()->get_selectivity_ctx(),
          merge_join_conditions,
          equal_cond_sel,
          get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to calculate selectivity", K(ret), K(merge_join_conditions));
      } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
          get_plan()->get_update_table_metas(),
          get_plan()->get_selectivity_ctx(),
          merge_join_filters,
          other_cond_sel,
          get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to calculate selectivity", K(ret), K(merge_join_filters));
      } else if (OB_FAIL(init_merge_join_structure(allocator,
                                                  left_paths,
                                                  left_merge_keys,
                                                  left_merge_infos,
                                                  can_ignore_merge_plan))) {
        LOG_WARN("failed to init merge join structure", K(ret));
      } else if (OB_FAIL(init_merge_join_structure(allocator,
                                                  right_paths,
                                                  right_merge_keys,
                                                  right_merge_infos,
                                                  can_ignore_merge_plan))) {
        LOG_WARN("failed to init merge join structure", K(ret));
      } else if ((MERGE_JOIN & path_info.local_methods_) &&
                  OB_FAIL(generate_mj_paths(equal_sets,
                                            left_paths,
                                            right_paths,
                                            left_merge_infos,
                                            left_merge_keys,
                                            right_merge_keys,
                                            null_safe_merge_info,
                                            merge_join_conditions,
                                            merge_join_filters,
                                            merge_filters,
                                            equal_cond_sel,
                                            other_cond_sel,
                                            path_info))) {
        LOG_WARN("failed to generate merge join paths", K(ret));
      } else if ((MERGE_JOIN & reverse_path_info.local_methods_) &&
                OB_FAIL(generate_mj_paths(equal_sets,
                                          right_paths,
                                          left_paths,
                                          right_merge_infos,
                                          right_merge_keys,
                                          left_merge_keys,
                                          null_safe_merge_info,
                                          merge_join_conditions,
                                          merge_join_filters,
                                          merge_filters,
                                          equal_cond_sel,
                                          other_cond_sel,
                                          reverse_path_info))) {
        LOG_WARN("failed to generate merge join paths", K(ret));
      } else {
        int64_t merge_join_path_num = interesting_paths_.count() - path_number;
        path_number = interesting_paths_.count();
        has_non_nl_path = true;

        OPT_TRACE("generate", merge_join_path_num, "merge join path");
        LOG_TRACE("succeed to generate merge join paths", K(merge_join_conditions.count()),
                "merge_path_count", merge_join_path_num);
      }
    }
  }
  get_plan()->get_selectivity_ctx().clear();
  return ret;
}

int ObJoinOrder::generate_hash_paths(const EqualSets &equal_sets,
                                     const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                                     const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                                     const ObIArray<ObRawExpr*> &left_join_keys,
                                     const ObIArray<ObRawExpr*> &right_join_keys,
                                     const ObIArray<bool> &null_safe_info,
                                     const ObIArray<ObRawExpr*> &join_conditions,
                                     const ObIArray<ObRawExpr*> &join_filters,
                                     const ObIArray<ObRawExpr*> &join_quals,
                                     const double equal_cond_sel,
                                     const double other_cond_sel,
                                     const ValidPathInfo &path_info,
                                     const NullAwareAntiJoinInfo &naaj_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<Path*, 8> left_best_paths;
  ObSEArray<Path*, 8> right_best_paths;
  bool can_slave_mapping = false;
  if (OB_FAIL(find_minimal_cost_path(left_paths, left_best_paths))) {
    LOG_WARN("failed to find minimal cost path", K(ret));
  } else if (OB_FAIL(find_minimal_cost_path(right_paths, right_best_paths))) {
    LOG_WARN("failed to find minimal cost path", K(ret));
  } else {
    Path *left_path = NULL;
    Path *right_path = NULL;
    if (path_info.is_reverse_path_) {
      OPT_TRACE_TITLE("Consider Reverse HASH", ob_join_type_str(path_info.join_type_));
    } else {
      OPT_TRACE_TITLE("Consider HASH", ob_join_type_str(path_info.join_type_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < left_best_paths.count(); i++) {
      if (OB_ISNULL(left_path = left_best_paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < right_best_paths.count(); j++) {
          int64_t dist_method = 0;
          if (OB_ISNULL(right_path = right_best_paths.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(right_path), K(ret));
          } else if (OB_FAIL(get_distributed_join_method(*left_path,
                                                         *right_path,
                                                         equal_sets,
                                                         left_join_keys,
                                                         right_join_keys,
                                                         null_safe_info,
                                                         path_info,
                                                         HASH_JOIN,
                                                         false,
                                                         naaj_info.is_naaj_,
                                                         dist_method,
                                                         can_slave_mapping))) {
            LOG_WARN("failed to get distributed join method", K(ret));
          } else {
            LOG_TRACE("succeed to get distributed hash join method", K(dist_method));
            for (int64_t k = DistAlgo::DIST_BASIC_METHOD;
                 OB_SUCC(ret) && k < DistAlgo::DIST_MAX_JOIN_METHOD; k = k << 1) {
              if (dist_method & k) {
                DistAlgo dist_algo = get_dist_algo(k);
                if (OB_FAIL(create_and_add_hash_path(left_path,
                                                     right_path,
                                                     path_info.join_type_,
                                                     dist_algo,
                                                     can_slave_mapping,
                                                     join_conditions,
                                                     join_filters,
                                                     join_quals,
                                                     equal_cond_sel,
                                                     other_cond_sel,
                                                     naaj_info))) {
                  LOG_WARN("failed to create and add hash path", K(ret));
                } else { /*do nothing*/ }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_nl_paths(const EqualSets &equal_sets,
                                   const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                                   const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                                   const ObIArray<ObRawExpr*> &left_join_keys,
                                   const ObIArray<ObRawExpr*> &right_join_keys,
                                   const ObIArray<bool> &null_safe_info,
                                   const ObIArray<ObRawExpr*> &on_conditions,
                                   const ObIArray<ObRawExpr*> &where_conditions,
                                   const ValidPathInfo &path_info,
                                   const bool has_non_nl_path,
                                   const bool has_equal_cond)
{
  int ret = OB_SUCCESS;
  Path *path = NULL;
  ObJoinOrder *left_tree = NULL;
  ObJoinOrder *right_tree = NULL;
  ObSEArray<Path*, 8> best_paths;
  const ObIArray<ObRawExpr*> &join_conditions =
      IS_OUTER_OR_CONNECT_BY_JOIN(path_info.join_type_) ? on_conditions : where_conditions;
  bool need_inner_path = false;
  if (path_info.is_reverse_path_) {
    OPT_TRACE_TITLE("Consider Reverse NL", ob_join_type_str(path_info.join_type_));
  } else {
    OPT_TRACE_TITLE("Consider NL", ob_join_type_str(path_info.join_type_));
  }
  if (OB_UNLIKELY(left_paths.empty()) || OB_UNLIKELY(right_paths.empty()) || OB_ISNULL(get_plan()) ||
      OB_UNLIKELY(left_paths.at(0).empty()) || OB_ISNULL(left_tree = left_paths.at(0).at(0)->parent_) ||
      OB_UNLIKELY(right_paths.at(0).empty()) || OB_ISNULL(right_tree = right_paths.at(0).at(0)->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(left_paths.count()), K(right_paths.count()),
        K(left_tree), K(right_tree), K(ret));
  } else if (OB_FAIL(check_valid_for_inner_path(join_conditions, path_info, *right_tree, need_inner_path))) {
    LOG_WARN("failed to check valid for inner path", K(ret));
  } else if (need_inner_path && OB_FAIL(get_cached_inner_paths(join_conditions,
                                                               *left_tree,
                                                               *right_tree,
                                                               path_info.force_inner_nl_,
                                                               path_info.join_type_,
                                                               best_paths))) {
    LOG_WARN("failed to generate best inner paths", K(ret));
  } else if (!best_paths.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < best_paths.count(); j++) {
        if (OB_FAIL(create_plan_for_inner_path(best_paths.at(j)))) {
          LOG_WARN("failed to create plan for inner path", K(ret));
        } else if (OB_FAIL(generate_inner_nl_paths(equal_sets,
                                            left_paths.at(i),
                                            best_paths.at(j),
                                            left_join_keys,
                                            right_join_keys,
                                            null_safe_info,
                                            on_conditions,
                                            where_conditions,
                                            path_info,
                                            has_equal_cond))) {
          LOG_WARN("failed to generate inner nl paths", K(ret));
        } else { /*do nothing*/ }
      }
    }
  } else if ((has_non_nl_path && !left_tree->get_is_at_most_one_row()) || path_info.force_inner_nl_) {
    /*do nothing*/
    OPT_TRACE("ignore normal NL join");
  } else {
    if (OB_FAIL(find_minimal_cost_path(right_paths, best_paths))) {
      LOG_WARN("failed to find minimal cost path", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
        for (int64_t j = 0; OB_SUCC(ret) && j < best_paths.count(); j++) {
          if (OB_FAIL(generate_normal_nl_paths(equal_sets,
                                               left_paths.at(i),
                                               best_paths.at(j),
                                               left_join_keys,
                                               right_join_keys,
                                               null_safe_info,
                                               on_conditions,
                                               where_conditions,
                                               path_info,
                                               has_equal_cond))) {
            LOG_WARN("failed to generate normal nl paths", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::create_plan_for_inner_path(Path *path)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null path", K(ret));
  } else if (path->subquery_exprs_.empty()) {
    //do nothing
  } else if (OB_FAIL(get_plan()->create_plan_tree_from_path(path, op))) {
    LOG_WARN("failed to create plan from path", K(ret));
  } else if (OB_FAIL(path->compute_path_property_from_log_op())) {
    LOG_WARN("failed to compute path property", K(ret));
  }
  return ret;
}

int ObJoinOrder::create_subplan_filter_for_join_path(Path *path,
                                                    ObIArray<ObRawExpr*> &subquery_filters)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null path", K(ret));
  } else if (subquery_filters.empty()) {
    //do nothing
  } else if (OB_FAIL(get_plan()->create_plan_tree_from_path(path, op))) {
    LOG_WARN("failed to create plan from path", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", K(ret));
  } else if (OB_FAIL(get_plan()->allocate_subplan_filter_as_top(op,
                                                                subquery_filters,
                                                                true,
                                                                true))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FALSE_IT(path->log_op_ = op)) {
  } else if (OB_FAIL(path->compute_path_property_from_log_op())) {
    LOG_WARN("failed to compute path property", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_valid_for_inner_path(const ObIArray<ObRawExpr*> &join_conditions,
                                            const ValidPathInfo &path_info,
                                            const ObJoinOrder &right_tree,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (path_info.force_inner_nl_) {
    is_valid = true;
  } else if (join_conditions.empty() || path_info.force_mat_ ||
      (ACCESS != right_tree.get_type() && SUBQUERY != right_tree.get_type())) {
    is_valid = false;
  } else if (!OPT_CTX.is_push_join_pred_into_view_enabled() &&
             SUBQUERY == right_tree.get_type()) {
    is_valid = false;
  } else if (CONNECT_BY_JOIN == path_info.join_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < join_conditions.count(); i++) {
      if (OB_ISNULL(join_conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (join_conditions.at(i)->has_flag(CNT_ROWNUM)) {
        is_valid = false;
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObJoinOrder::generate_inner_nl_paths(const EqualSets &equal_sets,
                                         const ObIArray<Path*> &left_paths,
                                         Path *right_path,
                                         const ObIArray<ObRawExpr*> &left_join_keys,
                                         const ObIArray<ObRawExpr*> &right_join_keys,
                                         const ObIArray<bool> &null_safe_info,
                                         const ObIArray<ObRawExpr*> &on_conditions,
                                         const ObIArray<ObRawExpr*> &where_conditions,
                                         const ValidPathInfo &path_info,
                                         const bool has_equal_cond)
{
  int ret = OB_SUCCESS;
  int64_t dist_method = 0;
  bool can_slave_mapping = false;
  if (OB_UNLIKELY(left_paths.empty()) || OB_ISNULL(left_paths.at(0)) || OB_ISNULL(right_path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(left_paths.count()), K(right_path), K(ret));
  } else if (OB_FAIL(get_distributed_join_method(*left_paths.at(0),
                                                 *right_path,
                                                 equal_sets,
                                                 left_join_keys,
                                                 right_join_keys,
                                                 null_safe_info,
                                                 path_info,
                                                 NESTED_LOOP_JOIN,
                                                 true,
                                                 false,
                                                 dist_method,
                                                 can_slave_mapping))) {
    LOG_WARN("failed to get distributed join method", K(ret));
  } else if (right_path->is_access_path() &&
             OB_FAIL(static_cast<AccessPath*>(right_path)->compute_is_das_dynamic_part_pruning(
                                                                                equal_sets,
                                                                                left_join_keys,
                                                                                right_join_keys))) {
    LOG_WARN("failed to compute is das dynamic part pruning", K(ret));
  } else if (dist_method == 0) {
    /*do nothing*/
  } else {
    // generate inner push down path
    LOG_TRACE("succeed to get distributed inner nested loop join method", K(dist_method));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
      for (int64_t j = DistAlgo::DIST_BASIC_METHOD;
           OB_SUCC(ret) && j < DistAlgo::DIST_MAX_JOIN_METHOD; j = (j << 1)) {
        if (dist_method & j) {
          DistAlgo dist_algo = get_dist_algo(j);
          if (OB_FAIL(create_and_add_nl_path(left_paths.at(i),
                                              right_path,
                                              path_info.join_type_,
                                              dist_algo,
                                              can_slave_mapping,
                                              on_conditions,
                                              where_conditions,
                                              has_equal_cond,
                                              false,
                                              false))) {
            LOG_WARN("failed to create and add hash path", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }

  return ret;
}

int ObJoinOrder::generate_normal_nl_paths(const EqualSets &equal_sets,
                                          const ObIArray<Path*> &left_paths,
                                          Path *right_path,
                                          const ObIArray<ObRawExpr*> &left_join_keys,
                                          const ObIArray<ObRawExpr*> &right_join_keys,
                                          const ObIArray<bool> &null_safe_info,
                                          const common::ObIArray<ObRawExpr*> &on_conditions,
                                          const common::ObIArray<ObRawExpr*> &where_conditions,
                                          const ValidPathInfo &path_info,
                                          const bool has_equal_cond)
{
  int ret = OB_SUCCESS;
  Path *left_path = NULL;
  ObJoinOrder *left_tree = NULL;
  int64_t dist_method = 0;
  bool can_slave_mapping = false;
  if (OB_UNLIKELY(left_paths.empty()) || OB_ISNULL(left_paths.at(0)) ||
      OB_ISNULL(left_tree = left_paths.at(0)->parent_) ||
      OB_ISNULL(right_path) || OB_ISNULL(right_path->get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(left_paths.count()), K(right_path), K(ret));
  } else if (OB_FAIL(get_distributed_join_method(*left_paths.at(0),
                                                 *right_path,
                                                 equal_sets,
                                                 left_join_keys,
                                                 right_join_keys,
                                                 null_safe_info,
                                                 path_info,
                                                 NESTED_LOOP_JOIN,
                                                 false,
                                                 false,
                                                 dist_method,
                                                 can_slave_mapping))) {
    LOG_WARN("failed to get distributed join method", K(ret));
  } else if (dist_method == 0) {
    /*do nothing*/
  } else {
    bool need_mat = CONNECT_BY_JOIN != path_info.join_type_ && (path_info.force_mat_ ||
                    (!path_info.force_no_mat_ && !left_tree->get_is_at_most_one_row()));
    bool need_no_mat = (path_info.force_no_mat_ || (!path_info.force_mat_ &&
                       (left_tree->get_is_at_most_one_row() || CONNECT_BY_JOIN == path_info.join_type_)));
    LOG_TRACE("succeed to get distributed normal nested loop join method", K(need_mat),
        K(need_no_mat), K(dist_method));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
      bool left_is_at_most_one_row = false;
      if (OB_ISNULL(left_paths.at(i)) || OB_ISNULL(left_paths.at(i)->parent_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        left_is_at_most_one_row = left_paths.at(i)->parent_->get_is_at_most_one_row();
      }
      for (int64_t j = DistAlgo::DIST_BASIC_METHOD;
           OB_SUCC(ret) && j < DistAlgo::DIST_MAX_JOIN_METHOD; j = (j << 1)) {
        DistAlgo dist_algo = get_dist_algo(j);
        if (dist_method & j) {
          bool right_need_exchange = (dist_algo == DIST_HASH_HASH ||
                                      dist_algo == DIST_NONE_BROADCAST ||
                                      dist_algo == DIST_NONE_PARTITION ||
                                      dist_algo == DIST_NONE_HASH);
          if (!ObOptimizerUtil::is_right_need_exchange(*right_path->get_sharding(), dist_algo) &&
              right_path->exchange_allocated_ &&
              right_path->get_sharding()->is_distributed()) {
            //如果右边不需要分配exchange，但是right path已经分配过exchange，
            //生成nl path会导致right path里的exchange打上px表，无法执行
          } else if ((need_mat || right_need_exchange) &&
               OB_FAIL(create_and_add_nl_path(left_paths.at(i),
                                              right_path,
                                              path_info.join_type_,
                                              dist_algo,
                                              can_slave_mapping,
                                              on_conditions,
                                              where_conditions,
                                              has_equal_cond,
                                              !left_is_at_most_one_row,
                                              true))) {
            LOG_WARN("failed to create and  add nl path with materialization", K(ret));
          } else if (need_no_mat && !right_need_exchange &&
                     OB_FAIL(create_and_add_nl_path(left_paths.at(i),
                                                    right_path,
                                                    path_info.join_type_,
                                                    dist_algo,
                                                    can_slave_mapping,
                                                    on_conditions,
                                                    where_conditions,
                                                    has_equal_cond,
                                                    !left_is_at_most_one_row,
                                                    false))) {
            LOG_WARN("failed to create and add nl path without materialization", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_distributed_join_method(Path &left_path,
                                             Path &right_path,
                                             const EqualSets &equal_sets,
                                             const ObIArray<ObRawExpr*> &left_join_keys,
                                             const ObIArray<ObRawExpr*> &right_join_keys,
                                             const ObIArray<bool> &null_safe_info,
                                             const ValidPathInfo &path_info,
                                             const JoinAlgo join_algo,
                                             const bool is_push_down,
                                             const bool is_naaj,
                                             int64_t &distributed_methods,
                                             bool &can_slave_mapping)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool is_remote = false;
  bool is_left_match_repart = false;
  bool is_right_match_repart = false;
  bool is_partition_wise = false;
  bool is_ext_partition_wise = false;
  bool right_is_base_table = false;
  ObSEArray<ObRawExpr*, 8> target_part_keys;
  ObShardingInfo *left_sharding = NULL;
  ObShardingInfo *right_sharding = NULL;
  distributed_methods = path_info.distributed_methods_;
  const bool is_force_dist_method = !path_info.ignore_hint_
                                    && (distributed_methods == get_dist_algo(distributed_methods));
  bool use_shared_hash_join = false;
  ObSQLSessionInfo *session = NULL;
  int64_t max_path_parallel = max(left_path.parallel_, right_path.parallel_);
  can_slave_mapping =
      path_info.force_slave_mapping_ && max_path_parallel > ObGlobalHint::DEFAULT_PARALLEL;
  if (path_info.force_slave_mapping_ && !can_slave_mapping) {
    OPT_TRACE("Disable slave mapping because parallel is 1");
  }
  if (OB_ISNULL(get_plan()) || OB_ISNULL(left_sharding = left_path.get_sharding()) ||
      OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(right_sharding = right_path.get_sharding()) ||
      OB_ISNULL(left_path.parent_) || OB_ISNULL(right_path.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(left_sharding),
                K(right_sharding), K(left_path.parent_), K(ret));
  } else if (OB_FAIL(session->get_px_shared_hash_join(use_shared_hash_join))) {
    LOG_WARN("get force parallel ddl dop failed", K(ret));
  } else if (HASH_JOIN == join_algo && is_naaj) {
    distributed_methods &= ~DIST_PARTITION_WISE;
    distributed_methods &= ~DIST_EXT_PARTITION_WISE;
    OPT_TRACE("HASH NAA JOIN can not use partition wise");
    if (LEFT_ANTI_JOIN == path_info.join_type_) {
      distributed_methods &= ~DIST_PARTITION_NONE;
      distributed_methods &= ~DIST_HASH_NONE;
      OPT_TRACE("left naaj can not use PKEY/HASH NONE");
    } else {
      distributed_methods &= ~DIST_NONE_PARTITION;
      distributed_methods &= ~DIST_NONE_HASH;
      OPT_TRACE("right naaj can not use NONE PKEY/HASH");
    }
  }
  if (OB_SUCC(ret)) {
    if (HASH_JOIN == join_algo) {
      if (use_shared_hash_join && right_path.parallel_ > ObGlobalHint::DEFAULT_PARALLEL) {
        if (is_force_dist_method && (DIST_BROADCAST_NONE == distributed_methods
                                     || DIST_ALL_NONE == distributed_methods)) {
          /* do nothing */
        } else {
          distributed_methods &= ~DIST_BROADCAST_NONE;
          distributed_methods &= ~DIST_ALL_NONE;
          OPT_TRACE("shared hash join will not use BROADCAST");
        }
        if (IS_LEFT_STYLE_JOIN(path_info.join_type_)) {
          distributed_methods &= ~DIST_BC2HOST_NONE;
        }
      } else {
        use_shared_hash_join = false;
        distributed_methods &= ~DIST_BC2HOST_NONE;
        OPT_TRACE("hash join will not use BC2HOST");
      }
    } else if (MERGE_JOIN == join_algo) {
      // disable dist algo except basic & pwj for mj
      distributed_methods &= ~DIST_BC2HOST_NONE;
      if (path_info.prune_mj_) {
        distributed_methods &= ~DIST_PULL_TO_LOCAL;
        distributed_methods &= ~DIST_HASH_HASH;
        distributed_methods &= ~DIST_BROADCAST_NONE;
        distributed_methods &= ~DIST_NONE_BROADCAST;
        distributed_methods &= ~DIST_NONE_ALL;
        distributed_methods &= ~DIST_RANDOM_ALL;
        distributed_methods &= ~DIST_ALL_NONE;
        distributed_methods &= ~DIST_PARTITION_NONE;
        distributed_methods &= ~DIST_HASH_NONE;
        distributed_methods &= ~DIST_NONE_PARTITION;
        distributed_methods &= ~DIST_NONE_HASH;
        OPT_TRACE("merge join prune normal path and will only use basic or partition wise");
      }
    } else if (!is_push_down) {
      distributed_methods &= ~DIST_BC2HOST_NONE;
      distributed_methods &= ~DIST_ALL_NONE;
      // @guoping.wgp release this constraint in future
      distributed_methods &= ~DIST_HASH_HASH;
      distributed_methods &= ~DIST_HASH_NONE;
      distributed_methods &= ~DIST_NONE_HASH;
      OPT_TRACE("normal NL join can not use BC2HOST and HASH HASH");
    } else {
      // nested loop join with pushdown
      distributed_methods &= ~DIST_HASH_HASH;
      distributed_methods &= ~DIST_NONE_PARTITION;
      distributed_methods &= ~DIST_NONE_HASH;
      distributed_methods &= ~DIST_HASH_NONE;
      distributed_methods &= ~DIST_NONE_BROADCAST;
      distributed_methods &= ~DIST_BROADCAST_NONE;
      distributed_methods &= ~DIST_ALL_NONE;
      if (right_path.exchange_allocated_) {
        distributed_methods &= ~DIST_PARTITION_WISE;
        distributed_methods &= ~DIST_EXT_PARTITION_WISE;
      }
      if (OB_FAIL(right_path.check_is_base_table(right_is_base_table))) {
        LOG_WARN("failed to check is base table", K(ret));
      } else if (!right_is_base_table ||
                 left_path.parent_->get_is_at_most_one_row() ||
                 !(ObJoinType::INNER_JOIN == path_info.join_type_ ||
                   (IS_LEFT_STYLE_JOIN(path_info.join_type_) && right_sharding->is_single()))) {
        distributed_methods &= ~DIST_BC2HOST_NONE;
      }
    }
    if (right_sharding->is_local() || right_sharding->is_match_all()) {
      distributed_methods &= ~DIST_BROADCAST_NONE;
      distributed_methods &= ~DIST_ALL_NONE;
      distributed_methods &= ~DIST_BC2HOST_NONE;
    }
    if (left_sharding->is_local() || left_sharding->is_match_all()) {
      distributed_methods &= ~DIST_NONE_BROADCAST;
      distributed_methods &= ~DIST_NONE_ALL;
      distributed_methods &= ~DIST_RANDOM_ALL;
    }
    if (left_sharding->is_match_all()) {
      distributed_methods &= ~DIST_BC2HOST_NONE;
      distributed_methods &= ~DIST_BROADCAST_NONE;
    }
    if (right_sharding->is_match_all()) {
      distributed_methods &= ~DIST_NONE_BROADCAST;
    }
    if (left_path.parallel_ <= 1) {
      distributed_methods &= ~DIST_NONE_BROADCAST;
    }
    if (right_path.parallel_ <= 1) {
      distributed_methods &= ~DIST_BROADCAST_NONE;
    }
    if (left_path.parallel_ <= 1 && right_path.parallel_ <= 1) {
      distributed_methods &= ~DIST_HASH_HASH;
    }
  }

  // if match none_all, check whether can use random all
  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_RANDOM_ALL)) {
    if (join_algo == NESTED_LOOP_JOIN && left_path.is_access_path()
        && left_sharding->is_distributed() && right_sharding->is_match_all()) {
      if (distributed_methods == DistAlgo::DIST_RANDOM_ALL) {
        distributed_methods = DistAlgo::DIST_RANDOM_ALL;
        OPT_TRACE("plan will use random all method by hint");
      } else {
        int enable_px_random_shuffle_only_statistic_exist = (OB_E(EventTable::EN_PX_RANDOM_SHUFFLE_WITHOUT_STATISTIC_INFORMATION) OB_SUCCESS);
        int64_t px_expected_work_count = 0;
        int64_t compute_parallel = left_path.parallel_;
        AccessPath *left_access_path = static_cast<AccessPath *>(&left_path);
        ObTableMetaInfo *table_meta_info = NULL;
        if (OB_ISNULL(table_meta_info = left_access_path->est_cost_info_.table_meta_info_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected null", KPC(table_meta_info), K(ret));
        } else if (OB_SUCC(enable_px_random_shuffle_only_statistic_exist) &&
          (!table_meta_info->has_opt_stat_ || table_meta_info->micro_block_count_ == 0)) {
          // Whether to use PX Random Shuffle is based on statistic infomation, so if we don't have
          // it, we just use normal NONE_ALL
          distributed_methods &= ~DistAlgo::DIST_RANDOM_ALL;
          OPT_TRACE("plan will not use random all because lack of statistic information");
        } else if (OB_FAIL(ObOptimizerUtil::compute_nlj_spf_storage_compute_parallel_skew(
                     &get_plan()->get_optimizer_context(), left_access_path->get_ref_table_id(),
                     table_meta_info, compute_parallel, px_expected_work_count))) {
          LOG_WARN("Fail to compute none_all nlj storage compute parallel skew", K(ret));
        } else if (px_expected_work_count < compute_parallel) {
          // we have more compute resources, so we should add a random shuffle, not choose none_all
          distributed_methods &= ~DIST_NONE_ALL;
          LOG_TRACE("NLJ none-all actual compute parallel:", K(compute_parallel),
                    K(px_expected_work_count));
        } else {
          distributed_methods &= ~DistAlgo::DIST_RANDOM_ALL;
        }
      }
    } else {
      distributed_methods &= ~DistAlgo::DIST_RANDOM_ALL;
    }
  }

  // check if match none_all sharding info
  if (OB_SUCC(ret) && distributed_methods & DIST_NONE_ALL) {
    if (left_sharding->is_distributed() && right_sharding->is_match_all()) {
      distributed_methods = DIST_NONE_ALL;
    } else {
      distributed_methods &= ~DIST_NONE_ALL;
    }
  }
  // check if match all_none sharding info
  if (OB_SUCC(ret) && (distributed_methods & DIST_ALL_NONE)) {
    if (right_sharding->is_distributed() && left_sharding->is_match_all() &&
        !left_path.contain_das_op()) {
      // all side is allowed for only EXPRESSION
      distributed_methods = DIST_ALL_NONE;
    } else {
      distributed_methods &= ~DIST_ALL_NONE;
    }
  }
  // check if match basic sharding info
  if (OB_SUCC(ret) && (distributed_methods & DIST_BASIC_METHOD)) {
    OPT_TRACE("check basic method");
    ObSEArray<ObShardingInfo*, 2> input_shardings;
    if (OB_FAIL(input_shardings.push_back(left_sharding)) ||
        OB_FAIL(input_shardings.push_back(right_sharding))) {
      LOG_WARN("failed to push back shardings", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(
                                 get_plan()->get_optimizer_context().get_local_server_addr(),
                                 input_shardings,
                                 is_basic,
                                 is_remote))) {
      LOG_WARN("failed to check basic sharding info", K(ret));
    } else if (get_cnt_rownum() && (!is_basic || is_remote)) {
      distributed_methods &= DIST_PULL_TO_LOCAL;
      OPT_TRACE("query with rownum, will use pull to local");
    } else if (is_basic) {
      distributed_methods = DIST_BASIC_METHOD;
      OPT_TRACE("plan will use basic method");
    } else {
      distributed_methods &= ~DIST_BASIC_METHOD;
      OPT_TRACE("plan will not use basic method");
    }
  }

  // check if match partition wise join
  if (OB_SUCC(ret) && (distributed_methods & DIST_PARTITION_WISE)) {
    OPT_TRACE("check partition wise method");
    if (!is_partition_wise_valid(left_path, right_path)) {
      distributed_methods &= ~DistAlgo::DIST_PARTITION_WISE;
      OPT_TRACE("contain merge op, can not use PARTITION WISE");
    } else if (OB_FAIL(check_if_match_partition_wise(equal_sets,
                                              left_path,
                                              right_path,
                                              left_join_keys,
                                              right_join_keys,
                                              null_safe_info,
                                              is_partition_wise))) {
      LOG_WARN("failed to check if match partition wise join", K(ret));
    } else if (is_partition_wise) {
      bool need_reduce_dop = left_path.parallel_more_than_part_cnt()
                             || right_path.parallel_more_than_part_cnt();
      if (!need_reduce_dop) {
        distributed_methods = DIST_PARTITION_WISE;
        OPT_TRACE("plan will use partition wise method");
      }
    } else {
      distributed_methods &= ~DIST_PARTITION_WISE;
      OPT_TRACE("plan will not use partition wise method");
    }
  }
  // check if match extended partition wise join
  if (OB_SUCC(ret) && (distributed_methods & DIST_EXT_PARTITION_WISE)) {
    OPT_TRACE("check extended partition wise method");
    if (!left_sharding->is_distributed_without_table_location_with_partitioning() ||
        !ObShardingInfo::is_shuffled_server_list(left_path.get_server_list()) ||
        !right_sharding->is_distributed_without_table_location_with_partitioning() ||
        !ObShardingInfo::is_shuffled_server_list(right_path.get_server_list())) {
      distributed_methods &= ~DIST_EXT_PARTITION_WISE;
      is_ext_partition_wise = false;
      OPT_TRACE("sharding is not expected, will not use extended partition wise");
    } else if (OB_FAIL(ObShardingInfo::check_if_match_extended_partition_wise(equal_sets,
                                                              left_path.get_server_list(),
                                                              right_path.get_server_list(),
                                                              left_join_keys,
                                                              right_join_keys,
                                                              null_safe_info,
                                                              left_path.get_strong_sharding(),
                                                              left_path.get_weak_sharding(),
                                                              right_path.get_strong_sharding(),
                                                              right_path.get_weak_sharding(),
                                                              is_ext_partition_wise))) {
      LOG_WARN("failed to check match extended partition wise", K(ret));
    } else if (is_ext_partition_wise) {
      distributed_methods = DIST_EXT_PARTITION_WISE;
      OPT_TRACE("plan will use ext partition wise method");
    } else {
      distributed_methods &= ~DIST_EXT_PARTITION_WISE;
      OPT_TRACE("plan will not use ext partition wise method");
    }
  }

  if (OB_SUCC(ret) &&
      ((distributed_methods & DIST_PARTITION_NONE)
       || (distributed_methods & DIST_HASH_NONE)
       || ((distributed_methods & DIST_BROADCAST_NONE) && can_slave_mapping))) {
    target_part_keys.reuse();
    if (OB_FAIL(right_sharding->get_all_partition_keys(target_part_keys, true))) {
      LOG_WARN("failed to get partition keys", K(ret));
    } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(equal_sets,
                                                                      left_join_keys,
                                                                      right_join_keys,
                                                                      target_part_keys,
                                                                      is_right_match_repart))) {
      LOG_WARN("failed to check if match repartition", K(ret));
    }
  }

  if (OB_SUCC(ret) &&
      ((distributed_methods & DIST_NONE_PARTITION)
       || (distributed_methods & DIST_NONE_HASH)
       || ((distributed_methods & DIST_NONE_BROADCAST) && can_slave_mapping))) {
    target_part_keys.reuse();
    if (OB_FAIL(left_sharding->get_all_partition_keys(target_part_keys, true))) {
      LOG_WARN("failed to get partition keys", K(ret));
    } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(equal_sets,
                                                                      right_join_keys,
                                                                      left_join_keys,
                                                                      target_part_keys,
                                                                      is_left_match_repart))) {
      LOG_WARN("failed to check if match repartition", K(ret));
    }
  }

  // check if match left re-partition
  if (OB_SUCC(ret) && (distributed_methods & DIST_PARTITION_NONE)) {
    OPT_TRACE("check partition none method");
    if (!is_repart_valid(left_path,
                        right_path,
                        DistAlgo::DIST_PARTITION_NONE,
                        NESTED_LOOP_JOIN == join_algo)) {
      distributed_methods &= ~DistAlgo::DIST_PARTITION_NONE;
      OPT_TRACE("contain merge op, can not use PARTITION NONE");
    } else if (NULL == right_path.get_strong_sharding()) {
      OPT_TRACE("strong sharding of right path is null, not use partition none");
      distributed_methods &= ~DIST_PARTITION_NONE;
    } else if (!right_path.get_sharding()->is_distributed_with_table_location_and_partitioning()
               || !is_right_match_repart) {
      OPT_TRACE("right path not meet repart, not use partition none");
      distributed_methods &= ~DIST_PARTITION_NONE;
    } else if (right_path.parallel_more_than_part_cnt()) {
      OPT_TRACE("plan will use partition none method with parallel degree reduced");
    } else {
      OPT_TRACE("plan will use partition none method and prune broadcast/bc2host/hash none method");
      distributed_methods &= ~DIST_BROADCAST_NONE;
      distributed_methods &= ~DIST_HASH_NONE;
      if (use_shared_hash_join && HASH_JOIN == join_algo) {
        distributed_methods &= ~DIST_BC2HOST_NONE;
      }
    }
  }
  // check if match hash none
  if (OB_SUCC(ret) && (distributed_methods & DIST_HASH_NONE)) {
    OPT_TRACE("check hash none method");
    if (NULL == right_path.get_strong_sharding()) {
      OPT_TRACE("strong sharding of right path is null, not use hash none");
      distributed_methods &= ~DIST_HASH_NONE;
    } else if (!right_sharding->is_distributed_without_table_location_with_partitioning() ||
               !ObShardingInfo::is_shuffled_server_list(right_path.get_server_list()) ||
               !is_right_match_repart) {
      OPT_TRACE("plan will not use hash none method");
      distributed_methods &= ~DIST_HASH_NONE;
    } else {
      OPT_TRACE("plan will use hash none method and prune broadcast none method");
      distributed_methods &= ~DIST_BROADCAST_NONE;
    }
  }
  // check if match right re-partition
  if (OB_SUCC(ret) && (distributed_methods & DIST_NONE_PARTITION)) {
    OPT_TRACE("check none partition method");
    if (!is_repart_valid(left_path,
                        right_path,
                        DistAlgo::DIST_NONE_PARTITION,
                        NESTED_LOOP_JOIN == join_algo)) {
      distributed_methods &= ~DistAlgo::DIST_NONE_PARTITION;
      OPT_TRACE("contain merge op, can not use NONE PARTITION");
    } else if (NULL == left_path.get_strong_sharding()) {
      OPT_TRACE("strong sharding of left path is null, not use none partition");
      distributed_methods &= ~DIST_NONE_PARTITION;
    } else if (!left_path.get_sharding()->is_distributed_with_table_location_and_partitioning()
               || !is_left_match_repart) {
      OPT_TRACE("left path not meet repart, not use none partition");
      distributed_methods &= ~DIST_NONE_PARTITION;
    } else if (left_path.parallel_more_than_part_cnt()) {
      OPT_TRACE("plan will use none partition method with parallel degree reduced");
    } else {
      OPT_TRACE("plan will use none partition method and prune none broadcast/hash method");
      distributed_methods &= ~DIST_NONE_BROADCAST;
      distributed_methods &= ~DIST_NONE_HASH;
    }
  }
  // check if match none-hash
  if (OB_SUCC(ret) && (distributed_methods & DIST_NONE_HASH)) {
    OPT_TRACE("check none hash method");
    if (NULL == left_path.get_strong_sharding()) {
      OPT_TRACE("strong sharding of left path is null, not use none hash");
      distributed_methods &= ~DIST_NONE_HASH;
    } else if (!left_sharding->is_distributed_without_table_location_with_partitioning() ||
               !ObShardingInfo::is_shuffled_server_list(left_path.get_server_list()) ||
               !is_left_match_repart) {
      OPT_TRACE("plan will not use none hash method");
      distributed_methods &= ~DIST_NONE_HASH;
    } else {
      OPT_TRACE("plan will use none hash method and prune none broadcast method");
      distributed_methods &= ~DIST_NONE_BROADCAST;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DIST_BROADCAST_NONE)
      && can_slave_mapping && !is_right_match_repart) {
    OPT_TRACE("force slave mapping and right path not meet repart, prune broadcast none method");
    distributed_methods &= ~DIST_BROADCAST_NONE;
  }

  if (OB_SUCC(ret) && (distributed_methods & DIST_NONE_BROADCAST)
      && can_slave_mapping && !is_left_match_repart) {
    OPT_TRACE("force slave mapping and left path not meet repart, prune none broadcast method");
    distributed_methods &= ~DIST_NONE_BROADCAST;
  }

  /*
   * if we have other parallel join methods, avoid pull to local execution,
   * we may change this strategy in future
   */
  if (OB_SUCC(ret) && distributed_methods != DIST_PULL_TO_LOCAL) {
    distributed_methods &= ~DIST_PULL_TO_LOCAL;
    OPT_TRACE("plan will not use pull to local method");
  }
  return ret;
}

bool ObJoinOrder::is_partition_wise_valid(const Path &left_path,
                                          const Path &right_path)
{
  bool is_valid = true;
  if ((left_path.exchange_allocated_ || right_path.exchange_allocated_) &&
      (left_path.contain_pw_merge_op() || right_path.contain_pw_merge_op())) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return is_valid;
}

bool ObJoinOrder::is_repart_valid(const Path &left_path, const Path &right_path, const DistAlgo dist_algo, const bool is_nl)
{
  bool is_valid = true;
  if (DistAlgo::DIST_PARTITION_NONE == dist_algo && right_path.exchange_allocated_ && is_nl) {
    is_valid = false;
  } else if (DistAlgo::DIST_PARTITION_NONE == dist_algo && right_path.contain_pw_merge_op()) {
    is_valid = true;
  } else if (DistAlgo::DIST_NONE_PARTITION == dist_algo && left_path.contain_pw_merge_op()) {
    is_valid = true;
  } else {
    is_valid = true;
  }
  return is_valid;
}

int ObJoinOrder::check_if_match_partition_wise(const EqualSets &equal_sets,
                                               Path &left_path,
                                               Path &right_path,
                                               const ObIArray<ObRawExpr*> &left_join_keys,
                                               const ObIArray<ObRawExpr*> &right_join_keys,
                                               const ObIArray<bool> &null_safe_exprs,
                                               bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  is_partition_wise = false;
  if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(equal_sets,
                                                            left_join_keys,
                                                            right_join_keys,
                                                            null_safe_exprs,
                                                            left_path.get_strong_sharding(),
                                                            left_path.get_weak_sharding(),
                                                            right_path.get_strong_sharding(),
                                                            right_path.get_weak_sharding(),
                                                            is_partition_wise))) {
    LOG_WARN("failed to check if match partition wise join", K(ret));
  } else {
    LOG_TRACE("succeed to check if match partition wise join", K(is_partition_wise));
  }
  return ret;
}

int ObJoinOrder::classify_mergejoin_conditions(const ObJoinOrder &left_tree,
                                               const ObJoinOrder &right_tree,
                                               const ObJoinType join_type,
                                               const ObIArray<ObRawExpr*> &on_condition,
                                               const ObIArray<ObRawExpr*> &where_condition,
                                               ObIArray<ObRawExpr*> &equal_join_conditions,
                                               ObIArray<ObRawExpr*> &other_join_conditions,
                                               ObIArray<ObRawExpr*> &filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extract_mergejoin_conditions(IS_OUTER_OR_CONNECT_BY_JOIN(join_type) ? on_condition : where_condition,
                                           left_tree.get_tables(),
                                           right_tree.get_tables(),
                                           equal_join_conditions,
                                           other_join_conditions))) {
    LOG_WARN("failed to extract merge-join conditions and on_conditions", K(join_type), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type) && OB_FAIL(append(filters, where_condition))) {
    LOG_WARN("failed to append join where_filters", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObJoinOrder::generate_mj_paths(const EqualSets &equal_sets,
                                   const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                                   const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                                   const ObIArray<ObSEArray<MergeKeyInfo*, 16>> &left_merge_keys,
                                   const ObIArray<ObRawExpr*> &left_join_keys,
                                   const ObIArray<ObRawExpr*> &right_join_keys,
                                   const ObIArray<bool> &null_safe_info,
                                   const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                   const common::ObIArray<ObRawExpr*> &other_join_conditions,
                                   const common::ObIArray<ObRawExpr*> &filters,
                                   const double equal_cond_sel,
                                   const double other_cond_sel,
                                   const ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_paths.count() != left_merge_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(left_paths.count()),
        K(left_merge_keys.count()), K(ret));
  } else {
    if (path_info.is_reverse_path_) {
      OPT_TRACE_TITLE("Consider Reverse Merge", ob_join_type_str(path_info.join_type_));
    } else {
      OPT_TRACE_TITLE("Consider Merge", ob_join_type_str(path_info.join_type_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < right_paths.count(); j++) {
        if (OB_FAIL(generate_mj_paths(equal_sets,
                                      left_paths.at(i),
                                      right_paths.at(j),
                                      left_merge_keys.at(i),
                                      left_join_keys,
                                      right_join_keys,
                                      null_safe_info,
                                      equal_join_conditions,
                                      other_join_conditions,
                                      filters,
                                      equal_cond_sel,
                                      other_cond_sel,
                                      path_info))) {
          LOG_WARN("failed to generated merge join paths", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_mj_paths(const EqualSets &equal_sets,
                                   const ObIArray<Path*> &left_paths,
                                   const ObIArray<Path*> &right_paths,
                                   const ObIArray<MergeKeyInfo*> &left_merge_keys,
                                   const ObIArray<ObRawExpr*> &left_join_keys,
                                   const ObIArray<ObRawExpr*> &right_join_keys,
                                   const ObIArray<bool> &null_safe_info,
                                   const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                   const common::ObIArray<ObRawExpr*> &other_join_conditions,
                                   const common::ObIArray<ObRawExpr*> &filters,
                                   const double equal_cond_sel,
                                   const double other_cond_sel,
                                   const ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  Path *left_path = NULL;
  Path *right_path = NULL;
  MergeKeyInfo *merge_key = NULL;
  int64_t dist_method = 0;
  int64_t best_prefix_pos = 0;
  bool best_need_sort = false;
  ObSEArray<OrderItem, 4> best_order_items;
  ObSEArray<ObRawExpr*, 4> adjusted_join_conditions;
  bool can_slave_mapping = false;
  if (OB_UNLIKELY(left_paths.empty() || OB_ISNULL(left_paths.at(0))) ||
      OB_UNLIKELY(right_paths.empty()) || OB_ISNULL(right_paths.at(0)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(left_paths.count()), K(right_paths.count()), K(ret));
  } else if (OB_FAIL(get_distributed_join_method(*left_paths.at(0),
                                                 *right_paths.at(0),
                                                 equal_sets,
                                                 left_join_keys,
                                                 right_join_keys,
                                                 null_safe_info,
                                                 path_info,
                                                 MERGE_JOIN,
                                                 false,
                                                 false,
                                                 dist_method,
                                                 can_slave_mapping))) {
    LOG_WARN("failed to get distributed join method", K(ret));
  } else if (0 == dist_method) {
    /*do nothing*/
  } else {
    LOG_TRACE("succeed to get distributed merge join method", K(dist_method));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_paths.count(); i++) {
      if (OB_ISNULL(left_path = left_paths.at(i)) || OB_ISNULL(merge_key = left_merge_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_path), K(merge_key), K(ret));
      } else if (merge_key->need_sort_ && !merge_key->order_needed_) {
        // if no further order needed, not generate merge style plan
      } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(equal_join_conditions,
                                                                  merge_key->map_array_,
                                                                  adjusted_join_conditions))) {
        LOG_WARN("failed to adjust exprs by mapping", K(ret));
      } else {
        for (int64_t j = DistAlgo::DIST_BASIC_METHOD;
             OB_SUCC(ret) && j < DistAlgo::DIST_MAX_JOIN_METHOD; j = (j << 1)) {
          if (dist_method & j) {
            DistAlgo dist_algo = get_dist_algo(j);
            if (OB_FAIL(find_minimal_cost_merge_path(*left_path,
                                                     *merge_key,
                                                     right_join_keys,
                                                     right_paths,
                                                     dist_algo,
                                                     can_slave_mapping,
                                                     best_order_items,
                                                     right_path,
                                                     best_need_sort,
                                                     best_prefix_pos,
                                                     path_info.prune_mj_))) {
              LOG_WARN("failed to find minimal cost merge path", K(ret));
            } else if (NULL != right_path &&
                       OB_FAIL(create_and_add_mj_path(left_path,
                                                      right_path,
                                                      path_info.join_type_,
                                                      dist_algo,
                                                      can_slave_mapping,
                                                      merge_key->order_directions_,
                                                      adjusted_join_conditions,
                                                      other_join_conditions,
                                                      filters,
                                                      equal_cond_sel,
                                                      other_cond_sel,
                                                      merge_key->order_items_,
                                                      merge_key->need_sort_,
                                                      merge_key->prefix_pos_,
                                                      best_order_items,
                                                      best_need_sort,
                                                      best_prefix_pos))) {
              LOG_WARN("failed to create and add merge join path", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::find_minimal_cost_merge_path(const Path &left_path,
                                              const MergeKeyInfo &left_merge_key,
                                              const ObIArray<ObRawExpr*> &right_join_exprs,
                                              const ObIArray<Path*> &right_path_list,
                                              const DistAlgo join_dist_algo,
                                              const bool is_slave_mapping,
                                              ObIArray<OrderItem> &best_order_items,
                                              Path *&best_path,
                                              bool &best_need_sort,
                                              int64_t &best_prefix_pos,
                                              bool prune_mj)
{
  int ret = OB_SUCCESS;
  double best_cost = 0.0;
  double right_path_cost = 0.0;
  double right_sort_cost = 0.0;
  int64_t right_prefix_pos = 0;
  bool right_need_sort = false;
  ObShardingInfo *sharding = NULL;
  int64_t out_parallel = ObGlobalHint::UNSET_PARALLEL;
  int64_t in_parallel = ObGlobalHint::UNSET_PARALLEL;
  int64_t available_parallel = ObGlobalHint::UNSET_PARALLEL;
  int64_t server_cnt = 0;
  ObSEArray<ObAddr, 8> server_list;
  ObSEArray<ObRawExpr*, 8> right_order_exprs;
  ObSEArray<OrderItem, 8> temp_order_items;
  ObSEArray<OrderItem, 8> right_order_items;
  best_path = NULL;
  best_need_sort = false;
  best_prefix_pos = 0;
  EstimateCostInfo info;
  double right_output_rows = 0.0;
  double right_orig_cost = 0.0;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_path_list.count(); i++) {
    Path *right_path = NULL;
    right_order_exprs.reset();
    temp_order_items.reset();
    right_order_items.reset();
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (OB_ISNULL(right_path = right_path_list.at(i)) ||
        OB_ISNULL(right_path->parent_) ||
        OB_ISNULL(sharding = right_path->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(right_path), K(sharding), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(right_join_exprs,
                                                                left_merge_key.map_array_,
                                                                right_order_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(right_order_exprs,
                                                       left_merge_key.order_directions_,
                                                       temp_order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(right_path->parent_->get_fd_item_set(),
                                                               right_path->parent_->get_output_equal_sets(),
                                                               right_path->parent_->get_output_const_exprs(),
                                                               get_plan()->get_onetime_query_refs(),
                                                               temp_order_items,
                                                               right_order_items))) {
      LOG_WARN("failed to simplify exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(right_order_items,
                                                        right_path->ordering_,
                                                        right_path->parent_->get_fd_item_set(),
                                                        right_path->parent_->get_output_equal_sets(),
                                                        right_path->parent_->get_output_const_exprs(),
                                                        get_plan()->get_onetime_query_refs(),
                                                        right_path->parent_->get_is_at_most_one_row(),
                                                        right_need_sort,
                                                        right_prefix_pos))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if ((DistAlgo::DIST_PARTITION_WISE == join_dist_algo ||
                DistAlgo::DIST_BASIC_METHOD == join_dist_algo) &&
                left_merge_key.need_sort_ && right_need_sort && prune_mj) {
      // do nothing
      OPT_TRACE("prune merge join,because both left and right path need sort");
    } else if (OB_FAIL(JoinPath::compute_join_path_parallel_and_server_info(&opt_ctx,
                                                                            &left_path,
                                                                            right_path,
                                                                            join_dist_algo,
                                                                            MERGE_JOIN,
                                                                            is_slave_mapping,
                                                                            in_parallel,
                                                                            available_parallel,
                                                                            server_cnt,
                                                                            server_list))) {
      LOG_WARN("failed to compute server info", K(ret));
    } else if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > (out_parallel = right_path->parallel_)
                           || ObGlobalHint::DEFAULT_PARALLEL > in_parallel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel", K(out_parallel), K(in_parallel), K(ret));
    } else {
      bool is_fully_partition_wise = (DistAlgo::DIST_PARTITION_WISE == join_dist_algo) &&
                                     !left_path.exchange_allocated_ && !right_path->exchange_allocated_;
      bool is_local_order = right_path->is_local_order_ && !is_fully_partition_wise;
      ObPQDistributeMethod::Type dist_method = ObOptimizerUtil::get_right_dist_method
                                               (*sharding, join_dist_algo);
      info.reset();
      // is single, may allocate exchange above, set need_parallel_ as 1 and compute exchange cost in cost_sort_and_exchange
      info.need_parallel_ = right_path->is_single() ? ObGlobalHint::DEFAULT_PARALLEL : in_parallel;
      if (OB_FAIL(right_path->re_estimate_cost(info, right_output_rows, right_orig_cost))) {
        LOG_WARN("failed to re estimate cost", K(ret));
      } else if (OB_FAIL(ObOptEstCost::cost_sort_and_exchange(&plan_->get_update_table_metas(),
                                                       &plan_->get_selectivity_ctx(),
                                                       dist_method,
                                                       right_path->is_distributed(),
                                                       is_local_order,
                                                       right_output_rows,
                                                       right_path->parent_->get_output_row_size(),
                                                       right_orig_cost,
                                                       out_parallel,
                                                       server_cnt,
                                                       in_parallel,
                                                       right_order_items,
                                                       right_need_sort,
                                                       right_prefix_pos,
                                                       right_path_cost,
                                                       opt_ctx))) {
        LOG_WARN("failed to compute cost for merge-join style op", K(ret));
      } else if (NULL == best_path || right_path_cost < best_cost) {
        if (OB_FAIL(best_order_items.assign(right_order_items))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          best_path = right_path;
          best_need_sort = right_need_sort;
          best_prefix_pos = right_prefix_pos;
          best_cost = right_path_cost;
        }
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::init_merge_join_structure(ObIAllocator &allocator,
                                           const ObIArray<ObSEArray<Path*, 16>> &paths,
                                           const ObIArray<ObRawExpr*> &join_exprs,
                                           ObIArray<ObSEArray<MergeKeyInfo*, 16>> &merge_keys,
                                           const bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  ObSEArray<MergeKeyInfo*, 16> temp_merge_keys;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); i++) {
    temp_merge_keys.reuse();
    if (OB_FAIL(init_merge_join_structure(allocator,
                                          paths.at(i),
                                          join_exprs,
                                          temp_merge_keys,
                                          can_ignore_merge_plan))) {
      LOG_WARN("failed to init merge join structure", K(ret));
    } else if (OB_FAIL(merge_keys.push_back(temp_merge_keys))) {
      LOG_WARN("failed to push back merge keys", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObJoinOrder::init_merge_join_structure(ObIAllocator &allocator,
                                           const ObIArray<Path*> &paths,
                                           const ObIArray<ObRawExpr*> &join_exprs,
                                           ObIArray<MergeKeyInfo*> &merge_keys,
                                           const bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  Path *path = NULL;
  ObSEArray<ObOrderDirection, 8> default_directions;
  MergeKeyInfo *interesting_key = NULL; // interesting ordering merge key
  MergeKeyInfo *merge_key = NULL;
  const ObDMLStmt *stmt = NULL;
  int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_default_directions(join_exprs.count(),
                                                             default_directions))) {
    LOG_WARN("failed to get default directions", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    if (OB_ISNULL(path = paths.at(i)) || OB_ISNULL(path->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(path), K(ret));
    } else if (OB_ISNULL(merge_key = static_cast<MergeKeyInfo*>(
                         allocator.alloc(sizeof(MergeKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc merge key info", K(ret));
    } else if (OB_FALSE_IT(merge_key = new(merge_key)MergeKeyInfo(allocator, join_exprs.count()))) {
    } else if (OB_FAIL(ObOptimizerUtil::decide_sort_keys_for_merge_style_op(
                                                  stmt,
                                                  get_plan()->get_equal_sets(),
                                                  path->ordering_,
                                                  path->parent_->get_fd_item_set(),
                                                  path->parent_->get_output_equal_sets(),
                                                  path->parent_->get_output_const_exprs(),
                                                  get_plan()->get_onetime_query_refs(),
                                                  path->parent_->get_is_at_most_one_row(),
                                                  join_exprs,
                                                  default_directions,
                                                  *merge_key,
                                                  interesting_key))) {
      LOG_WARN("failed to decide sort key for merge set", K(ret));
    } else if (OB_FAIL(merge_keys.push_back(merge_key))) {
      LOG_WARN("failed to push back merge key", K(ret));
    } else if (can_ignore_merge_plan) {
      if (OB_FAIL(check_all_interesting_order(merge_key->order_items_,
                                              stmt,
                                              interesting_order_info))) {
        LOG_WARN("failed to check interesting order", K(ret));
      } else if (OrderingFlag::NOT_MATCH == interesting_order_info) {
        merge_key->order_needed_ = false;
      }
    }
  }
  return ret;
}

int ObJoinOrder::set_nl_filters(JoinPath *join_path,
                                const Path *right_path,
                                const ObJoinType join_type,
                                const ObIArray<ObRawExpr*> &on_conditions,
                                const ObIArray<ObRawExpr*> &where_conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_path) || OB_ISNULL(right_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(join_path), K(right_path), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type)) {
    if (OB_FAIL(append_array_no_dup(join_path->filter_, where_conditions))) {
      LOG_WARN("failed to append conditions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < on_conditions.count(); ++i) {
      if (right_path->is_inner_path() && //如果是条件下降的NL
          ObOptimizerUtil::find_item(right_path->pushdown_filters_, on_conditions.at(i))) {
         /*do nothing*/
      } else if (OB_FAIL(join_path->other_join_conditions_.push_back(on_conditions.at(i)))) { //未下降的nl条件
        LOG_WARN("failed to push back conditions", K(ret));
      }
    }
  } else {
    if (OB_FAIL(append_array_no_dup(join_path->other_join_conditions_, on_conditions))) {
      LOG_WARN("failed to append conditions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < where_conditions.count(); ++i) {
      //只有能在本级处理的qual才能放在join条件里，处理AB时，遇到a+b=c条件是不能放在本级join条件里的
      if (OB_ISNULL(where_conditions.at(i))) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("raw expr is null", K(ret));
      } else if (right_path->is_inner_path() && //如果是条件下降的NL
                 ObOptimizerUtil::find_item(right_path->pushdown_filters_, where_conditions.at(i))) {
          /*do nothing*/
      } else if (OB_FAIL(join_path->other_join_conditions_.push_back(where_conditions.at(i)))) { //未下降的nl条件
        LOG_WARN("failed to push back conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::alloc_join_path(JoinPath *&join_path)
{
  int ret = OB_SUCCESS;
  join_path = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (!get_plan()->get_recycled_join_paths().empty() &&
             OB_FAIL(get_plan()->get_recycled_join_paths().pop_back(join_path))) {
    LOG_WARN("failed to pop back join path from recycled paths", K(ret));
  } else if (NULL == join_path &&
            OB_ISNULL(join_path = static_cast<JoinPath*>(allocator_->alloc(sizeof(JoinPath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate a join path", K(join_path), K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObJoinOrder::create_and_add_hash_path(const Path *left_path,
                                          const Path *right_path,
                                          const ObJoinType join_type,
                                          const DistAlgo join_dist_algo,
                                          const bool is_slave_mapping,
                                          const ObIArray<ObRawExpr*> &equal_join_conditions,
                                          const ObIArray<ObRawExpr*> &other_join_conditions,
                                          const ObIArray<ObRawExpr*> &filters,
                                          const double equal_cond_sel,
                                          const double other_cond_sel,
                                          const NullAwareAntiJoinInfo &naaj_info)
{
  int ret = OB_SUCCESS;
  JoinPath *join_path = NULL;
  ObSEArray<ObRawExpr*, 4> normal_filters;
  ObSEArray<ObRawExpr*, 4> subquery_filters;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_path), K(right_path), K(get_plan()), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(filters,
                                                              subquery_filters,
                                                              normal_filters,
                                                              false))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (OB_FAIL(alloc_join_path(join_path))) {
    LOG_WARN("failed to allocate a hash join path", K(ret));
  } else {
    join_path = new(join_path)JoinPath(this,
                                       left_path,
                                       right_path,
                                       HASH_JOIN,
                                       join_dist_algo,
                                       is_slave_mapping,
                                       join_type);
    join_path->equal_cond_sel_ = equal_cond_sel;
    join_path->other_cond_sel_ = other_cond_sel;
    join_path->is_naaj_ = naaj_info.is_naaj_;
    join_path->is_sna_ = naaj_info.is_sna_;
    join_path->is_slave_mapping_ &= (!naaj_info.is_naaj_);
    join_path->join_output_rows_ = current_join_output_rows_;
    OPT_TRACE("create new Hash Join path:", join_path);
    if (OB_FAIL(append(join_path->equal_join_conditions_, equal_join_conditions))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(append(join_path->other_join_conditions_, other_join_conditions))) {
      LOG_WARN("failed to append join filters", K(ret));
    } else if (OB_FAIL(append(join_path->filter_, normal_filters))) {
      LOG_WARN("failed to append join quals", K(ret));
    } else if (OB_FAIL(append(join_path->expr_constraints_, naaj_info.expr_constraints_))) {
      LOG_WARN("failed to append constraints", K(ret));
    } else if (OB_FAIL(generate_join_filter_infos(*left_path,
                                                  *right_path,
                                                  join_type,
                                                  join_dist_algo,
                                                  equal_join_conditions,
                                                  naaj_info.is_naaj_,
                                                  join_path->join_filter_infos_))) {
      LOG_WARN("failed to generate join filter info", K(ret));
    } else if (OB_FAIL(join_path->compute_join_path_property())) {
      LOG_WARN("failed to compute join path property", K(ret));
    } else if (OB_FAIL(create_subplan_filter_for_join_path(join_path,
                                                           subquery_filters))) {
      LOG_WARN("failed to create subplan filter for join path", K(ret));
    } else if (OB_FAIL(add_path(join_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else if (CONNECT_BY_JOIN == join_type &&
               OB_FAIL(push_down_order_siblings(join_path, right_path))) {
      LOG_WARN("push down order siblings by condition failed", K(ret));
    } else {
      LOG_TRACE("succeed to create a hash join path", K(join_type),
          K(join_dist_algo), K(equal_join_conditions), K(other_join_conditions));
    }
  }
  return ret;
}

int ObJoinOrder::generate_join_filter_infos(const Path &left_path,
                                            const Path &right_path,
                                            const ObJoinType join_type,
                                            const DistAlgo join_dist_algo,
                                            const ObIArray<ObRawExpr*> &equal_join_conditions,
                                            const bool is_naaj,
                                            ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  bool right_is_scan = false;
  bool can_use_join_filter = false;
  ObLogicalOperator *right_child = NULL;
  int64_t hash_join_parallel = ObGlobalHint::UNSET_PARALLEL;
  const int64_t join_parallel = ObOptimizerUtil::get_join_style_parallel(left_path.parallel_,
                                                                         right_path.parallel_,
                                                                         join_dist_algo);
  /*
  *  1. 检查并行度是否大于1, 检查是否使用新引擎.
  *  2. 检查Join类型.
  *  3. 检查计划形态.
  *     - 满足右侧基表或者跨exchange基表
  *     - 满足不跨exchange非基表
  *  4. 检查代价是否可以生成.
  *  5. 检查hint是否可以生成.
  */
  if (ObGlobalHint::DEFAULT_PARALLEL >= join_parallel) {
    OPT_TRACE("hash join parallel <= 1, plan will not use join filter");
  } else if (RIGHT_OUTER_JOIN == join_type ||
            FULL_OUTER_JOIN == join_type ||
            RIGHT_ANTI_JOIN == join_type ||
            CONNECT_BY_JOIN == join_type ||
            is_naaj) {
    //do nothing
  } else if (OB_FAIL(find_possible_join_filter_tables(left_path,
                                                      right_path,
                                                      join_dist_algo,
                                                      equal_join_conditions,
                                                      join_filter_infos))) {
    LOG_WARN("failed to find possible table scan for bf", K(ret));
  } else if (join_filter_infos.empty()) {
    OPT_TRACE("no valid join filter");
  } else if (OB_FAIL(check_normal_join_filter_valid(left_path,
                                                    right_path,
                                                    join_filter_infos))) {
    LOG_WARN("fail to check bloom filter gen rule", K(ret));
  } else if (OB_FAIL(check_partition_join_filter_valid(join_dist_algo,
                                                       right_path,
                                                       join_filter_infos))) {
    LOG_WARN("fail to check hint gen rule", K(ret));
  } else if (OB_FAIL(remove_invalid_join_filter_infos(join_filter_infos))) {
    LOG_WARN("failed to remove invalid join filter info", K(ret));
  }
  return ret;
}

int ObJoinOrder::find_possible_join_filter_tables(const Path &left_path,
                                                  const Path &right_path,
                                                  const DistAlgo join_dist_algo,
                                                  const ObIArray<ObRawExpr*> &equal_join_conditions,
                                                  ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  ObRelIds right_tables;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(right_path.get_sharding())
      || OB_ISNULL(left_path.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()), K(right_path.get_sharding()),
                              K(left_path.parent_));
  } else {
    bool right_need_exchange = ObOptimizerUtil::is_right_need_exchange(*right_path.get_sharding(), join_dist_algo);
    bool left_need_exchange = ObOptimizerUtil::is_left_need_exchange(*left_path.get_sharding(), join_dist_algo);
    bool is_fully_partition_wise = !left_need_exchange && !right_need_exchange && DIST_PARTITION_WISE == join_dist_algo;
    int64_t current_dfo_level = left_need_exchange ? -1 : (right_need_exchange ? 1 : 0);
    ObSEArray<ObRawExpr*, 4> left_exprs;
    ObSEArray<ObRawExpr*, 4> right_exprs;
    if (OB_FAIL(ObOptimizerUtil::extract_equal_join_conditions(equal_join_conditions,
                                                               left_path.parent_->get_tables(),
                                                               left_exprs,
                                                               right_exprs))) {
      LOG_WARN("failed format equal join conditions", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_rel_ids(right_exprs, right_tables))) {
      LOG_WARN("failed to get table ids by rexprs", K(ret));
    } else if (OB_FAIL(find_possible_join_filter_tables(
                       get_plan()->get_log_plan_hint(),
                       right_path,
                       left_path.parent_->get_tables(),
                       right_tables,
                       !get_plan()->get_optimizer_context().enable_runtime_filter(),
                       !right_need_exchange,
                       is_fully_partition_wise,
                       current_dfo_level,
                       left_exprs,
                       right_exprs,
                       join_filter_infos))) {
      LOG_WARN("failed to find subquery possible join filter table", K(ret));
    } else {
      for (int64_t i = 0; i < join_filter_infos.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(join_filter_infos.at(i).all_join_key_left_exprs_.assign(left_exprs))) {
          LOG_WARN("failed to assign all_join_key_left_exprs");
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::fill_join_filter_info(JoinFilterInfo &join_filter_info)
{
  int ret = OB_SUCCESS;
  uint64_t opt_version = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()));
  } else if (FALSE_IT(get_plan()->get_selectivity_ctx().clear())) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_plan()->get_update_table_metas(),
                                                          get_plan()->get_selectivity_ctx(),
                                                          join_filter_info.rexprs_,
                                                          join_filter_info.row_count_,
                                                          join_filter_info.right_distinct_card_))) {
    LOG_WARN("failed to calc distinct", K(ret));
  } else if (!OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                                COMPAT_VERSION_4_3_3)) {
    // do nothing
  } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(get_plan()->get_basic_table_metas(),
                                                               join_filter_info.rexprs_,
                                                               join_filter_info.is_right_contain_pk_,
                                                               join_filter_info.is_right_union_pk_))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (join_filter_info.is_right_contain_pk_ && join_filter_info.is_right_union_pk_) {
    const OptTableMeta *table_meta = get_plan()->get_update_table_metas().get_table_meta_by_table_id(join_filter_info.table_id_);
    if (OB_NOT_NULL(table_meta)) {
      join_filter_info.right_distinct_card_ = std::max(1.0, table_meta->get_rows());
    }
    if (OB_NOT_NULL(table_meta = get_plan()->get_basic_table_metas().get_table_meta_by_table_id(join_filter_info.table_id_))) {
      join_filter_info.right_origin_rows_ = std::max(1.0, table_meta->get_rows());
    }
  }

  return ret;
}

int ObJoinOrder::find_possible_join_filter_tables(const ObLogPlanHint &log_plan_hint,
                                                  const Path &right_path,
                                                  const ObRelIds &left_tables,
                                                  const ObRelIds &right_tables,
                                                  bool config_disable,
                                                  bool is_current_dfo,
                                                  bool is_fully_partition_wise,
                                                  int64_t current_dfo_level,
                                                  const ObIArray<ObRawExpr*> &left_join_conditions,
                                                  const ObIArray<ObRawExpr*> &right_join_conditions,
                                                  ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (right_path.is_access_path()) {
    const AccessPath &access = static_cast<const AccessPath&>(right_path);
    bool can_join_filter = false;
    const ObJoinFilterHint *force_hint = NULL;
    bool can_part_join_filter = false;
    const ObJoinFilterHint *force_part_hint = NULL;
    if (access.est_cost_info_.index_meta_info_.is_global_index_ &&
        access.est_cost_info_.index_meta_info_.is_index_back_) {
      //do nothing
    } else if (OB_FAIL(log_plan_hint.check_use_join_filter(access.table_id_,
                                                           left_tables,
                                                           false,
                                                           config_disable,
                                                           can_join_filter,
                                                           force_hint))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (!is_fully_partition_wise &&
               OB_FAIL(log_plan_hint.check_use_join_filter(access.table_id_,
                                                           left_tables,
                                                           true,
                                                           config_disable,
                                                           can_part_join_filter,
                                                           force_part_hint))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (!can_join_filter && !can_part_join_filter) {
      //do nothing
    } else {
      JoinFilterInfo info;
      info.table_id_ = access.table_id_;
      info.filter_table_id_ = access.table_id_;
      info.index_id_ = access.index_id_;
      info.ref_table_id_ = access.ref_table_id_;
      info.sharding_ = access.strong_sharding_;
      info.row_count_ = access.get_output_row_count();
      info.can_use_join_filter_ = can_join_filter;
      info.force_filter_ = force_hint;
      info.need_partition_join_filter_ = can_part_join_filter;
      info.force_part_filter_ = force_part_hint;
      info.in_current_dfo_ = is_current_dfo;
      if ((info.can_use_join_filter_ || info.need_partition_join_filter_)) {
        bool will_use_column_store = false;
        bool will_use_row_store = false;
        if (access.use_column_store_) {
          info.use_column_store_ = true;
        } else if (OB_FAIL(get_plan()->will_use_column_store(info.table_id_,
                                                            info.index_id_,
                                                            info.ref_table_id_,
                                                            will_use_column_store,
                                                            will_use_row_store))) {
          LOG_WARN("failed to check will use column store", K(ret));
        } else if (will_use_column_store) {
          info.use_column_store_ = true;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(get_join_filter_exprs(left_join_conditions,
                                          right_join_conditions,
                                          info))) {
          LOG_WARN("failed to get join filter exprs", K(ret));
        } else if (OB_FAIL(fill_join_filter_info(info))) {
          LOG_WARN("failed to fill join filter info", K(ret));
        } else if(OB_FAIL(join_filter_infos.push_back(info))) {
          LOG_WARN("failed to push back info", K(ret));
        }
      }
    }
  } else if (right_path.is_temp_table_path()) {
    const TempTablePath &temp_table = static_cast<const TempTablePath&>(right_path);
    bool can_join_filter = false;
    const ObJoinFilterHint *force_hint = NULL;
    if (OB_FAIL(log_plan_hint.check_use_join_filter(temp_table.table_id_,
                                                    left_tables,
                                                    false,
                                                    config_disable,
                                                    can_join_filter,
                                                    force_hint))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (can_join_filter) {
      JoinFilterInfo info;
      info.table_id_ = temp_table.table_id_;
      info.filter_table_id_ = temp_table.table_id_;
      info.row_count_ = temp_table.get_path_output_rows();
      info.can_use_join_filter_ = true;
      info.force_filter_ = force_hint;
      info.need_partition_join_filter_ = false;
      info.force_part_filter_ = NULL;
      info.in_current_dfo_ = is_current_dfo;
      if (OB_FAIL(get_join_filter_exprs(left_join_conditions,
                                        right_join_conditions,
                                        info))) {
        LOG_WARN("failed to get join filter exprs", K(ret));
      } else if (OB_FAIL(fill_join_filter_info(info))) {
        LOG_WARN("failed to fill join filter info", K(ret));
      } else if (OB_FAIL(join_filter_infos.push_back(info))) {
        LOG_WARN("failed to push back info", K(ret));
      }
    }
  } else if (right_path.is_join_path()) {
    const JoinPath &join_path = static_cast<const JoinPath&>(right_path);
    int64_t left_current_dfo_level = (current_dfo_level == -1 ? -1 : current_dfo_level +
        (int64_t)(join_path.is_left_need_exchange()));
    int64_t right_current_dfo_level = (current_dfo_level == -1 ? -1 : current_dfo_level +
        (int64_t)(join_path.is_right_need_exchange()));
    is_fully_partition_wise |= join_path.is_fully_partition_wise();
    if (left_current_dfo_level >= 2) {
      // If the root join path doesn't has the left child dfo,
      // It should not be generated cuz it cannot actually work by current scheduler.
    } else if (OB_ISNULL(join_path.left_path_) ||
               OB_ISNULL(join_path.left_path_->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (join_path.left_path_->parent_->get_tables().overlap(right_tables) &&
                OB_FAIL(SMART_CALL(find_possible_join_filter_tables(log_plan_hint,
                                                          *join_path.left_path_,
                                                          left_tables,
                                                          right_tables,
                                                          config_disable,
                                                          !join_path.is_left_need_exchange() && is_current_dfo,
                                                          is_fully_partition_wise,
                                                          left_current_dfo_level,
                                                          left_join_conditions,
                                                          right_join_conditions,
                                                          join_filter_infos)))) {
      LOG_WARN("failed to find shuffle table scan", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (right_current_dfo_level >= 2) {
      // If the root join path doesn't has the left child dfo,
      // It should not be generated cuz it cannot actually work by current scheduler.
    } else if (OB_ISNULL(join_path.right_path_) ||
               OB_ISNULL(join_path.right_path_->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (join_path.right_path_->parent_->get_tables().overlap(right_tables) &&
               OB_FAIL(SMART_CALL(find_possible_join_filter_tables(log_plan_hint,
                                                        *join_path.right_path_,
                                                        left_tables,
                                                        right_tables,
                                                        config_disable,
                                                        !join_path.is_right_need_exchange() && is_current_dfo,
                                                        is_fully_partition_wise,
                                                        right_current_dfo_level,
                                                        left_join_conditions,
                                                        right_join_conditions,
                                                        join_filter_infos)))) {
      LOG_WARN("failed to find shuffle table scan", K(ret));
    }
  } else if (right_path.is_subquery_path()) {
    ObSEArray<ObRawExpr*, 4> pushdown_left_quals;
    ObSEArray<ObRawExpr*, 4> pushdown_right_quals;
    JoinFilterPushdownHintInfo hint_info;
    ObSqlBitSet<> table_set;
    ObLogPlan* child_plan;
    const SubQueryPath& subquery_path = static_cast<const SubQueryPath&>(right_path);
    if (OB_ISNULL(subquery_path.root_) ||
        OB_ISNULL(child_plan = subquery_path.root_->get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(log_plan_hint.get_pushdown_join_filter_hints(subquery_path.subquery_id_,
                                                                    left_tables,
                                                                    config_disable,
                                                                    hint_info))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (OB_FAIL(table_set.add_member(stmt->get_table_bit_index(subquery_path.subquery_id_)))) {
      LOG_WARN("failed to add member into table set", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::extract_pushdown_join_filter_quals(left_join_conditions,
                                                                           right_join_conditions,
                                                                           table_set,
                                                                           pushdown_left_quals,
                                                                           pushdown_right_quals))) {
      LOG_WARN("failed to extract pushdown quals", K(ret));
    } else if (OB_FAIL(child_plan->pushdown_join_filter_into_subquery(stmt,
                                                                      subquery_path.root_,
                                                                      subquery_path.subquery_id_,
                                                                      hint_info,
                                                                      is_current_dfo,
                                                                      is_fully_partition_wise,
                                                                      current_dfo_level,
                                                                      pushdown_left_quals,
                                                                      pushdown_right_quals,
                                                                      join_filter_infos))) {
      LOG_WARN("failed to find pushdown join filter table", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::get_join_filter_exprs(const ObIArray<ObRawExpr*> &left_join_conditions,
                                       const ObIArray<ObRawExpr*> &right_join_conditions,
                                       JoinFilterInfo &join_filter_info)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  const ObDMLStmt* stmt;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()));
  } else if (OB_UNLIKELY(left_join_conditions.count() != right_join_conditions.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join condition length error", K(ret));
  } else if (OB_FAIL(table_set.add_member(stmt->get_table_bit_index(join_filter_info.table_id_)))) {
    LOG_WARN("failed to add member", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < right_join_conditions.count(); ++j) {
    ObRawExpr *lexpr = left_join_conditions.at(j);
    ObRawExpr *rexpr = right_join_conditions.at(j);
    if (OB_ISNULL(lexpr) || OB_ISNULL(rexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (rexpr->get_relation_ids().is_subset(table_set)) {
      if (OB_FAIL(join_filter_info.lexprs_.push_back(lexpr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(join_filter_info.rexprs_.push_back(rexpr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }

  return ret;
}

int ObJoinOrder::check_path_contain_filter(const Path* path, bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null path", K(ret));
  } else if (path->is_subquery_path()) {
    const SubQueryPath* subquery = static_cast<const SubQueryPath*>(path);
    if (0 != path->filter_.count()) {
      contain = true;
    } else if (OB_FAIL(SMART_CALL(check_path_contain_filter(subquery->root_, contain)))) {
      LOG_WARN("failed to check path contain filter", K(ret));
    }
  } else if (path->is_join_path()) {
    const JoinPath *join_path = static_cast<const JoinPath*>(path);
    bool left_contain = false;
    bool right_contain = false;
    if (OB_FAIL(SMART_CALL(check_path_contain_filter(join_path->left_path_, contain)))) {
      LOG_WARN("failed to check path contain filter", K(ret));
    } else if (contain) {
      //do nothing
    } else if (OB_FAIL(SMART_CALL(check_path_contain_filter(join_path->right_path_, contain)))) {
      LOG_WARN("failed to check path contain filter", K(ret));
    }
  } else {
    contain = 0 != path->filter_.count();
  }
  return ret;
}


int ObJoinOrder::check_path_contain_filter(const ObLogicalOperator* op, bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", K(ret));
  } else if (0 != op->get_filter_exprs().count()) {
    contain = true;
  } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
    const ObLogTableScan *table_scan = static_cast<const ObLogTableScan*>(op);
    contain = 0 != table_scan->get_range_conditions().count();
  } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type()) {
    if (OB_FAIL(SMART_CALL(check_path_contain_filter(op->get_child(0), contain)))) {
      LOG_WARN("failed to check path contain filter", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(check_path_contain_filter(op->get_child(i), contain)))) {
        LOG_WARN("failed to check path contain filter", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_normal_join_filter_valid(const Path& left_path,
                                                const Path& right_path,
                                                ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = get_plan();
  ObJoinOrder* left_tree = NULL;
  const ObDMLStmt *stmt = NULL;
  bool left_find = false;
  bool right_find = false;
  bool cur_dfo_has_shuffle_bf = false;
  bool left_path_contain_filter = false;
  if (OB_ISNULL(plan) ||
      OB_ISNULL(left_tree=left_path.parent_) ||
      OB_ISNULL(stmt = plan->get_stmt()) ||
      OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(check_path_contain_filter(&left_path, left_path_contain_filter))) {
    LOG_WARN("failed to check path contain filter", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < join_filter_infos.count(); ++i) {
    JoinFilterInfo &info = join_filter_infos.at(i);
    double join_filter_sel = 1.0;
    if (!info.can_use_join_filter_ || info.lexprs_.empty()) {
      info.can_use_join_filter_ = false;
    } else if (OB_FAIL(calc_join_filter_selectivity(left_path,
                                                    info,
                                                    join_filter_sel))) {
      LOG_WARN("failed to calc join filter sel", K(ret));
    } else {
      double rate = 1 - join_filter_sel;
      double threshold = 0.6;
      double misjudgment_rate = (static_cast<double>(GCONF._bloom_filter_ratio) / 100.0);
      if (info.in_current_dfo_) {
        threshold = 0.9;
      }
      info.join_filter_selectivity_ = join_filter_sel;
      if (OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                            COMPAT_VERSION_4_3_3) &&
          0 <= misjudgment_rate && misjudgment_rate <= 1.0) {
        info.join_filter_selectivity_ += (1 - join_filter_sel) * misjudgment_rate;
      }
      if (NULL != info.force_filter_) {
        info.can_use_join_filter_ = true;
      } else if (!OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5)) {
        info.can_use_join_filter_ = rate >= threshold;
      } else {
        info.can_use_join_filter_ = left_path_contain_filter &&
                                    info.row_count_ >= 100000 &&
                                    left_path.get_path_output_rows() < 64000000;
      }
      if (!info.can_use_join_filter_) {
        info.join_filter_selectivity_ = 1;
      }
      OPT_TRACE("join filter info:");
      OPT_TRACE("in current dfo:", info.in_current_dfo_);
      OPT_TRACE("right distinct card:", info.right_distinct_card_);
      OPT_TRACE("theoretical filter selectivity:", join_filter_sel);
      OPT_TRACE("actual filter selectivity:", info.join_filter_selectivity_);
      OPT_TRACE("force use join filter:", NULL != info.force_filter_);
      OPT_TRACE("use join filter:", info.can_use_join_filter_);
      LOG_TRACE("succeed to check normal join filter", K(info));
    }
  }
  return ret;
}

int ObJoinOrder::calc_join_filter_selectivity(const Path& left_path,
                                              JoinFilterInfo& info,
                                              double &join_filter_selectivity)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = get_plan();
  double left_distinct_card = 1.0;
  double right_distinct_card = 1.0;
  join_filter_selectivity = 1.0;
  bool is_pk_join_fk = false;
  bool est_enhance_enable = OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                                              COMPAT_VERSION_4_3_3);
  if (OB_ISNULL(plan) || OB_ISNULL(left_path.parent_) ||
      OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(&left_path.parent_->get_output_equal_sets(),
                                                                    left_path.get_path_output_rows(),
                                                                    &left_path.parent_->get_ambient_card()))) {
  } else if (est_enhance_enable &&
             OB_FAIL(calc_join_filter_sel_for_pk_join_fk(left_path, info, join_filter_selectivity, is_pk_join_fk))) {
    LOG_WARN("failed to calc pk join fk join filter sel", K(ret));
  } else if (is_pk_join_fk) {
    // do nothing
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                          plan->get_selectivity_ctx(),
                                                          info.lexprs_,
                                                          left_path.get_path_output_rows(),
                                                          left_distinct_card,
                                                          est_enhance_enable))) {
    LOG_WARN("failed to calc distinct", K(ret));
  } else {
    join_filter_selectivity = left_distinct_card / info.right_distinct_card_;
  }
  if (OB_SUCC(ret)) {
    if (join_filter_selectivity < 0) {
      join_filter_selectivity = 0;
    } else if (join_filter_selectivity > 0.9) {
      join_filter_selectivity = 0.9;
    }
    LOG_TRACE("succeed to calc join filter selectivity", K(is_pk_join_fk), K(join_filter_selectivity),
                                      K(left_distinct_card), K(info.right_distinct_card_));
  }
  return ret;
}

int ObJoinOrder::calc_join_filter_sel_for_pk_join_fk(const Path& left_path,
                                                     JoinFilterInfo& info,
                                                     double &join_filter_selectivity,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObLogPlan *plan = get_plan();
  bool left_contain_pk = false;
  bool is_left_union_pk = false;
  uint64_t left_table_id = OB_INVALID_ID;
  double pk_origin_rows = 1.0;
  double left_ndv = 1.0;
  if (OB_ISNULL(plan) || OB_ISNULL(left_path.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::is_columns_contain_pkey(plan->get_update_table_metas(),
                                                               info.lexprs_,
                                                               left_contain_pk,
                                                               is_left_union_pk,
                                                               &left_table_id))) {
    LOG_WARN("failed to check is columns contain pkey", K(ret));
  } else if (!info.is_right_contain_pk_ && left_contain_pk && is_left_union_pk) {
    pk_origin_rows = plan->get_basic_table_metas().get_rows(left_table_id);
    if (pk_origin_rows > OB_DOUBLE_EPSINON) {
      is_valid = true;
      if (OB_FAIL(plan->get_selectivity_ctx().get_ambient_card(left_table_id, left_ndv))) {
        LOG_WARN("failed to get ambient card", K(ret));
      } else {
        join_filter_selectivity = left_ndv / std::min(pk_origin_rows, info.right_distinct_card_);
      }
    }
  } else if (info.is_right_union_pk_ && info.is_right_contain_pk_ && !left_contain_pk && OB_INVALID_ID != left_table_id) {
    pk_origin_rows = info.right_origin_rows_;
    is_valid = true;
    double fk_origin_rows = plan->get_basic_table_metas().get_rows(left_table_id);
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                     plan->get_selectivity_ctx(),
                                                     info.lexprs_,
                                                     left_path.get_path_output_rows(),
                                                     left_ndv))) {
      LOG_WARN("failed to calculate distinct", K(ret), K(left_table_id), K(info));
    } else {
      double fk_ndv = ObOptSelectivity::scale_distinct(left_path.get_path_output_rows(), fk_origin_rows, pk_origin_rows);
      left_ndv = std::min(left_ndv, fk_ndv);
      join_filter_selectivity = left_ndv / info.right_distinct_card_;
    }
  }
  return ret;
}

int ObJoinOrder::find_shuffle_join_filter(const Path& path, bool &find)
{
  int ret = OB_SUCCESS;
  find = false;
  if (path.is_join_path()) {
    const JoinPath &join_path = static_cast<const JoinPath&>(path);
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < join_path.join_filter_infos_.count(); ++i) {
      find = !join_path.join_filter_infos_.at(i).in_current_dfo_;
    }
    if (OB_SUCC(ret) && !find && !join_path.is_left_need_exchange() &&
        OB_NOT_NULL(join_path.left_path_)) {
      if (OB_FAIL(SMART_CALL(find_shuffle_join_filter(*join_path.left_path_, find)))) {
        LOG_WARN("failed to find shuffle join filter", K(ret));
      }
    }
    if (OB_SUCC(ret) && !find && !join_path.is_right_need_exchange() &&
        OB_NOT_NULL(join_path.right_path_)) {
      if (OB_FAIL(SMART_CALL(find_shuffle_join_filter(*join_path.right_path_, find)))) {
        LOG_WARN("failed to find shuffle join filter", K(ret));
      }
    }
  } else if (path.is_subquery_path()) {
    ObLogicalOperator *op = static_cast<const SubQueryPath&>(path).root_;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(op->find_shuffle_join_filter(find))) {
      LOG_WARN("failed to find shuffle join filter", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_partition_join_filter_valid(const DistAlgo join_dist_algo,
                                                   const Path &right_path,
                                                   ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> target_part_keys;
  ObRawExpr *left_calc_part_id_expr = NULL;
  ObLogPlan *plan = get_plan();
  bool match = false;
  bool skip_subpart = false;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < join_filter_infos.count(); ++i) {
    JoinFilterInfo &info = join_filter_infos.at(i);
    target_part_keys.reuse();
    match = false;
    if (!info.need_partition_join_filter_ || DIST_PARTITION_WISE == join_dist_algo) {
      info.need_partition_join_filter_ = false;
    } else if (OB_ISNULL(info.sharding_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sharding", K(ret));
    } else if (OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5) &&
               (DIST_PARTITION_NONE != join_dist_algo || right_path.get_strong_sharding() != info.sharding_ ) &&
               NULL == info.force_part_filter_ &&
               info.sharding_->get_part_cnt() < 1000) {
      info.need_partition_join_filter_ = false;
    } else if (info.sharding_->is_single()) {
      info.need_partition_join_filter_ = false;
    } else if (OB_FAIL(info.sharding_->get_all_partition_keys(target_part_keys))) {
      LOG_WARN("fail to get all partion keys", K(ret));
    } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(get_plan()->get_equal_sets(),
                                                                        info.lexprs_,
                                                                        info.rexprs_,
                                                                        target_part_keys,
                                                                        match))) {
      LOG_WARN("fail to check if match repart", K(ret));
    } else if (!match) {
      // If the join condition includes all level 1 partition key,
      // partition bf can be generated based on the level 1 partition key
      if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(get_plan()->get_equal_sets(),
                                                                  info.lexprs_,
                                                                  info.rexprs_,
                                                                  info.sharding_->get_partition_keys(),
                                                                  match))) {
        LOG_WARN("fail to check if match repart", K(ret));
      } else {
        info.skip_subpart_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!match) {
      info.need_partition_join_filter_ = false;
      OPT_TRACE("hash join will not use partition join filter");
    } else if (OB_FAIL(build_join_filter_part_expr(info.index_id_,
                                                  info.lexprs_,
                                                  info.rexprs_,
                                                  info.sharding_,
                                                  info.calc_part_id_expr_,
                                                  info.skip_subpart_))) {
      LOG_WARN("fail to init bf part expr", K(ret));
    } else {
      OPT_TRACE("hash join will use partition join filter");
    }
  }
  return ret;
}

int ObJoinOrder::build_join_filter_part_expr(const int64_t ref_table_id,
                                            const common::ObIArray<ObRawExpr *> &lexprs ,
                                            const common::ObIArray<ObRawExpr *> &rexprs,
                                            ObShardingInfo *sharding_info,
                                            ObRawExpr *&left_calc_part_id_expr,
                                            bool skip_subpart)
{
  int ret = OB_SUCCESS;
  left_calc_part_id_expr = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(sharding_info) ||
      lexprs.empty() ||
      lexprs.count() != rexprs.count() ||
      sharding_info->get_partition_func().empty() ||
      (OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  } else {
    // build right calc part id expr
    ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    ObRawExprCopier copier(expr_factory);
    // build left calc part id expr
    ObSEArray<ObRawExpr*, 4> repart_exprs;
    ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
    ObSEArray<ObRawExpr*, 4> repart_func_exprs;
    // get repart keys
    for (int i = 0; i < sharding_info->get_partition_keys().count() && OB_SUCC(ret); ++i) {
      bool is_found = false;
      for (int j = 0; j < rexprs.count() && OB_SUCC(ret) && !is_found; ++j) {
        if (ObOptimizerUtil::is_expr_equivalent(sharding_info->get_partition_keys().at(i),
                                                rexprs.at(j),
                                                get_plan()->get_equal_sets())) {
          if (OB_FAIL(repart_exprs.push_back(lexprs.at(j)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else {
            is_found = true;
          }
        }
      }
    }
    // get subpart keys
    for (int i = 0; i < sharding_info->get_sub_partition_keys().count() &&
        !skip_subpart && OB_SUCC(ret);
        ++i) {
      bool is_found = false;
      for (int j = 0; j < rexprs.count() && OB_SUCC(ret) && !is_found; ++j) {
        if (ObOptimizerUtil::is_expr_equivalent(sharding_info->get_sub_partition_keys().at(i),
                                                rexprs.at(j),
                                                get_plan()->get_equal_sets())) {
          if (OB_FAIL(repart_sub_exprs.push_back(lexprs.at(j)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else {
            is_found = true;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(copier.add_replaced_expr(sharding_info->get_partition_keys(),
                                                repart_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (!skip_subpart &&
               OB_FAIL(copier.add_replaced_expr(sharding_info->get_sub_partition_keys(),
                                                repart_sub_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else {
      ObRawExpr *repart_func_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < sharding_info->get_partition_func().count(); i++) {
        repart_func_expr = NULL;
        ObRawExpr *target_func_expr = sharding_info->get_partition_func().at(i);
        if (OB_ISNULL(target_func_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (1 == i && skip_subpart) {
          ObConstRawExpr *const_expr = NULL;
          ObRawExpr *dummy_expr = NULL;
          int64_t const_value = 1;
          if (OB_FAIL(ObRawExprUtils::build_const_int_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                           ObIntType,
                                                           const_value,
                                                           const_expr))) {
            LOG_WARN("Failed to build const expr", K(ret));
          } else if (OB_ISNULL(dummy_expr = const_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(dummy_expr->formalize(session_info))) {
            LOG_WARN("Failed to formalize a new expr", K(ret));
          } else if (OB_FAIL(repart_func_exprs.push_back(dummy_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        } else if (OB_FAIL(copier.copy_on_replace(target_func_expr,
                                                  repart_func_expr))) {
          LOG_WARN("failed to copy on replace repart expr", K(ret));
        } else if (OB_FAIL(repart_func_exprs.push_back(repart_func_expr))) {
          LOG_WARN("failed to add repart func expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObRawExprUtils::build_calc_tablet_id_expr(expr_factory,
                                                              *session_info,
                                                              ref_table_id,
                                                              sharding_info->get_part_level(),
                                                              repart_func_exprs.at(0),
                                                              repart_func_exprs.count() > 1 ?
                                                              repart_func_exprs.at(1) : NULL,
                                                              left_calc_part_id_expr))) {
          LOG_WARN("fail to init calc part id expr", K(ret));
        } else if (OB_ISNULL(left_calc_part_id_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected calc part id expr", K(ret));
        } else if (skip_subpart) {
          left_calc_part_id_expr->set_partition_id_calc_type(CALC_IGNORE_SUB_PART);
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::remove_invalid_join_filter_infos(ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinFilterInfo, 4> new_infos;
  for (int i = 0; OB_SUCC(ret) && i < join_filter_infos.count(); ++i) {
    if (!join_filter_infos.at(i).can_use_join_filter_ &&
        !join_filter_infos.at(i).need_partition_join_filter_) {
      //do nothing
    } else if (OB_FAIL(new_infos.push_back(join_filter_infos.at(i)))) {
      LOG_WARN("failed to push back join filter info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(join_filter_infos.assign(new_infos))) {
      LOG_WARN("failed to assign join filter infos", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::create_and_add_mj_path(const Path *left_path,
                                        const Path *right_path,
                                        const ObJoinType join_type,
                                        const DistAlgo join_dist_algo,
                                        const bool is_slave_mapping,
                                        const ObIArray<ObOrderDirection> &merge_directions,
                                        const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                        const common::ObIArray<ObRawExpr*> &other_join_conditions,
                                        const common::ObIArray<ObRawExpr*> &filters,
                                        const double equal_cond_sel,
                                        const double other_cond_sel,
                                        const common::ObIArray<OrderItem> &left_sort_keys,
                                        const bool left_need_sort,
                                        const int64_t left_prefix_pos,
                                        const common::ObIArray<OrderItem> &right_sort_keys,
                                        const bool right_need_sort,
                                        const int64_t right_prefix_pos)
{
  int ret = OB_SUCCESS;
  JoinPath *join_path = NULL;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 4> normal_filters;
  ObSEArray<ObRawExpr*, 4> subquery_filters;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) || OB_ISNULL(left_path->get_sharding()) ||
      OB_ISNULL(right_path->get_sharding()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left_path), K(right_path), K(get_plan()), K(stmt), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(filters,
                                                              subquery_filters,
                                                              normal_filters,
                                                              false))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected right semt/anti merge join", K(ret), K(join_type));
  } else if (OB_FAIL(alloc_join_path(join_path))) {
    LOG_WARN("failed to allocate a merge join path", K(ret));
  } else {
    join_path = new(join_path)JoinPath(this,
                                       left_path,
                                       right_path,
                                       MERGE_JOIN,
                                       join_dist_algo,
                                       is_slave_mapping,
                                       join_type);
    join_path->left_need_sort_ = left_need_sort;
    join_path->right_need_sort_ = right_need_sort;
    join_path->left_prefix_pos_ = left_prefix_pos;
    join_path->right_prefix_pos_ = right_prefix_pos;
    join_path->equal_cond_sel_ = equal_cond_sel;
    join_path->other_cond_sel_ = other_cond_sel;
    join_path->join_output_rows_ = current_join_output_rows_;
    OPT_TRACE("create new Merge Join path:", join_path);
    if (OB_FAIL(append(join_path->equal_join_conditions_, equal_join_conditions))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(append(join_path->other_join_conditions_, other_join_conditions))) {
      LOG_WARN("failed to append join filters", K(ret));
    } else if (OB_FAIL(append(join_path->filter_, normal_filters))) {
      LOG_WARN("failed to append join quals", K(ret));
    } else if (OB_FAIL(append(join_path->left_sort_keys_, left_sort_keys))) {
      LOG_WARN("failed to append left expected ordering", K(ret));
    } else if (OB_FAIL(append(join_path->right_sort_keys_, right_sort_keys))) {
      LOG_WARN("failed to append right expected ordering", K(ret));
    } else if (OB_FAIL(append(join_path->merge_directions_, merge_directions))) {
      LOG_WARN("failed to append merge directions", K(ret));
    } else if (OB_FAIL(join_path->compute_join_path_property())) {
      LOG_WARN("failed to compute join path property", K(ret));
    } else if (OB_FAIL(create_subplan_filter_for_join_path(join_path,
                                                           subquery_filters))) {
      LOG_WARN("failed to create subplan filter for join path", K(ret));
    } else if (OB_FAIL(add_path(join_path))) {
      LOG_WARN("failed to add join path", K(ret));
    } else {
      LOG_TRACE("succeed to create a merge join path", K(join_type),
                K(merge_directions), K(left_sort_keys), K(right_sort_keys),
                K(left_need_sort), K(right_need_sort));
    }
  }
  return ret;
}

int ObJoinOrder::extract_hashjoin_conditions(const ObIArray<ObRawExpr*> &join_quals,
                                             const ObRelIds &left_tables,
                                             const ObRelIds &right_tables,
                                             ObIArray<ObRawExpr*> &equal_join_conditions,
                                             ObIArray<ObRawExpr*> &other_join_conditions,
                                             const ObJoinType &join_type,
                                             NullAwareAntiJoinInfo &naaj_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr *cur_expr = NULL;
  ObSQLSessionInfo *session = NULL;
  bool naaj_enabled = false;
  if ((LEFT_ANTI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type)) {
    if (OB_ISNULL(get_plan())
      || OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get session info", K(ret), K(get_plan()));
    } else if (OB_FAIL(session->get_enable_optimizer_null_aware_antijoin(naaj_enabled))) {
      LOG_WARN("failed to get sys var naaj enabled", K(ret));
    } else if (naaj_enabled) {
      if (OB_FAIL(extract_naaj_join_conditions(join_quals, left_tables, right_tables,
                                               equal_join_conditions, naaj_info))) {
        LOG_WARN("failed to extract naaj join conditions", K(ret));
      }
    }
  }
  for (int64_t i = 0 ; !naaj_info.is_naaj_ && OB_SUCC(ret) && i < join_quals.count(); ++i) {
    cur_expr = join_quals.at(i);
    bool is_equal_cond = false;
    if (OB_FAIL(check_is_join_equal_conditions(cur_expr, left_tables,
                                               right_tables, is_equal_cond))) {
      LOG_WARN("failed to check equal cond", K(cur_expr), K(ret));
    } else if (is_equal_cond) {
      if (OB_FAIL(equal_join_conditions.push_back(cur_expr))) {
        LOG_WARN("fail to push back", K(cur_expr), K(ret));
      }
    } else if (cur_expr->get_relation_ids().is_subset(get_tables())) {
      if (OB_FAIL(other_join_conditions.push_back(cur_expr))) {
        LOG_WARN("fail to push back", K(cur_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_is_join_equal_conditions(const ObRawExpr *equal_cond,
                                                const ObRelIds &left_tables,
                                                const ObRelIds &right_tables,
                                                bool &is_equal_cond)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  is_equal_cond = false;
  if (OB_ISNULL(equal_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(equal_cond), K(ret));
  } else if (equal_cond->has_flag(IS_JOIN_COND) &&
              (OB_ISNULL(left_expr = equal_cond->get_param_expr(0)) ||
              OB_ISNULL(right_expr = equal_cond->get_param_expr(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_expr), K(right_expr), K(ret));
  } else if (equal_cond->has_flag(IS_JOIN_COND)
              && T_OP_ROW != left_expr->get_expr_type()
              && T_OP_ROW != right_expr->get_expr_type()
              && ((left_expr->get_relation_ids().is_subset(left_tables)
                  && right_expr->get_relation_ids().is_subset(right_tables))
                  || (left_expr->get_relation_ids().is_subset(right_tables)
                      && right_expr->get_relation_ids().is_subset(left_tables)))) {
    is_equal_cond = true;
  }
  return ret;
}

int ObJoinOrder::classify_hashjoin_conditions(const ObJoinOrder &left_tree,
                                              const ObJoinOrder &right_tree,
                                              const ObJoinType join_type,
                                              const ObIArray<ObRawExpr*> &on_conditions,
                                              const ObIArray<ObRawExpr*> &where_filters,
                                              ObIArray<ObRawExpr*> &equal_join_conditions,
                                              ObIArray<ObRawExpr*> &other_join_conditions,
                                              ObIArray<ObRawExpr*> &filters,
                                              NullAwareAntiJoinInfo &naaj_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extract_hashjoin_conditions(IS_OUTER_OR_CONNECT_BY_JOIN(join_type)
                                             ? on_conditions : where_filters,
                                          left_tree.get_tables(),
                                          right_tree.get_tables(),
                                          equal_join_conditions,
                                          other_join_conditions,
                                          join_type,
                                          naaj_info))) {
    LOG_WARN("failed to extract hash join conditions and filters", K(join_type), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type)
            && OB_FAIL(append(filters, where_filters))) {
    LOG_WARN("failed to append join quals", K(ret));
  } else {}
  return ret;
}

int ObJoinOrder::extract_mergejoin_conditions(const ObIArray<ObRawExpr*> &join_quals,
                                              const ObRelIds &left_tables,
                                              const ObRelIds &right_tables,
                                              ObIArray<ObRawExpr*> &equal_join_conditions,
                                              ObIArray<ObRawExpr*> &other_join_conditions)
{
  int ret = OB_SUCCESS;
  ObRawExpr* cur_expr = NULL;
  ObRawExpr* left_expr = NULL;
  ObRawExpr* right_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
    cur_expr = join_quals.at(i);
    //我们相信joininfo里的join_quals是经过处理的，left=A，right=B，a=c是不可能出现在这里的，
    //所以大胆的利用IS_JOIN_COND进行判断即可
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(cur_expr), K(ret));
    } else if (cur_expr->has_flag(IS_JOIN_COND) &&
               (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
                OB_ISNULL(right_expr = cur_expr->get_param_expr(1)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(cur_expr), K(left_expr), K(right_expr), K(ret));
    } else if (cur_expr->has_flag(IS_JOIN_COND)
               && !cur_expr->has_flag(CNT_ROWNUM)
               && T_OP_ROW != left_expr->get_expr_type()
               && T_OP_ROW != right_expr->get_expr_type()
               && left_expr->get_collation_type() == right_expr->get_collation_type() //TODO:@ryan.ly
               && ((left_expr->get_relation_ids().is_subset(left_tables)
                    && right_expr->get_relation_ids().is_subset(right_tables))
                   || (left_expr->get_relation_ids().is_subset(right_tables)
                       && right_expr->get_relation_ids().is_subset(left_tables)))) {
      bool is_mj_able = false;
      common::ObObjType compare_type = cur_expr->get_result_type().get_calc_type();
      if (OB_FAIL(ObObjCaster::is_cast_monotonic(left_expr->get_data_type(), compare_type, is_mj_able))) {
        LOG_WARN("check cast monotonic error", K(left_expr->get_data_type()), K(compare_type), K(ret));
      } else if (is_mj_able) {
        if (OB_FAIL(ObObjCaster::is_cast_monotonic(right_expr->get_data_type(), compare_type, is_mj_able))) {
          LOG_WARN("check cast monotonic error", K(right_expr->get_data_type()), K(compare_type), K(ret));
        } else { /*do nothing*/ }
      } else { /*do nothing*/ }
      if (OB_SUCC(ret)) {
        if (is_mj_able) {
          ret = equal_join_conditions.push_back(cur_expr);
        } else {
          ret = other_join_conditions.push_back(cur_expr);
        }
      } else { /*do nothing*/ }
    } else if (cur_expr->get_relation_ids().is_subset(get_tables())) {
      //如果条件仅涉及左右分支，可以拿出来作为join filter
      if (OB_FAIL(other_join_conditions.push_back(cur_expr))) {
        LOG_WARN("fail to push back", K(cur_expr), K(ret));
      }
    } else {
      //其余条件是涉及本级表，但是不能在不本级处理的条件，例如：left=A，right=B，条件为a+b=c.
      //这样的条件会在下一次生成ABC的连接的过程中，处理joininfo的时候被处理
      LOG_TRACE("A qual references this join, but we can not resolve it in this level");
    }
  }
  return ret;
}

class NLParamReplacer : public ObIRawExprReplacer
{
public:
  int generate_new_expr(ObRawExprFactory &expr_factory,
                        ObRawExpr *old_expr,
                        ObRawExpr *&new_expr) override
  {
    int ret = OB_SUCCESS;
    new_expr = NULL;
    if (OB_ISNULL(old_expr) || OB_ISNULL(left_table_set_) || OB_ISNULL(copier_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params are invalid", K(ret), K(old_expr), K(left_table_set_));
    } else if (!old_expr->get_relation_ids().overlap(*left_table_set_) &&
               !old_expr->has_flag(CNT_LEVEL) &&
               !old_expr->has_flag(CNT_PRIOR)) {
      new_expr = old_expr;
    } else if (old_expr->is_query_ref_expr()) {
      // only function table subquery expr may come into here
      // normal push down predicates does not contain subquery
      ObQueryRefRawExpr *new_query_ref = NULL;
      ObSEArray<ObExecParamRawExpr *, 4> exec_params;
      if (OB_FAIL(expr_factory.create_raw_expr(T_REF_QUERY, new_query_ref))) {
        LOG_WARN("failed to create raw expr", K(ret));
      } else if (OB_FAIL(new_query_ref->assign(*old_expr))) {
        LOG_WARN("failed to assign old expr", K(ret));
      } else if (OB_FAIL(exec_params.assign(new_query_ref->get_exec_params()))) {
        LOG_WARN("failed to push back exec param", K(ret));
      } else {
        new_query_ref->get_exec_params().reuse();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < exec_params.count(); ++i) {
        ObExecParamRawExpr* expr = exec_params.at(i);
        ObRawExpr *ref_expr = NULL;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret));
        } else if (OB_ISNULL(ref_expr = expr->get_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret));
        } else if (ref_expr->get_relation_ids().is_subset(*left_table_set_)) {
          if (OB_FAIL(nl_params_.push_back(expr))) {
            LOG_WARN("failed to push back nl param", K(ret));
          } else {
            new_query_ref->set_has_nl_param(true);
          }
        } else if (OB_UNLIKELY(ref_expr->get_relation_ids().overlap(*left_table_set_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr", K(ret), K(*ref_expr));
        } else if (OB_FAIL(new_query_ref->get_exec_params().push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
      new_expr = new_query_ref;
    } else if (copier_->is_existed(old_expr)) {
      // do nothing
    } else if ((old_expr->is_column_ref_expr() &&
                old_expr->get_relation_ids().is_subset(*left_table_set_))
               || old_expr->has_flag(IS_LEVEL)
               || old_expr->has_flag(IS_PRIOR)) {
      new_expr = old_expr;
      if (OB_FAIL(ObRawExprUtils::create_new_exec_param(query_ctx_,
                                                        expr_factory,
                                                        new_expr))) {
        LOG_WARN("failed to create new exec param", K(ret));
      } else if (OB_FAIL(nl_params_.push_back(
                           static_cast<ObExecParamRawExpr *>(new_expr)))) {
        LOG_WARN("failed to push back new expr", K(ret));
      }
    }
    return ret;
  }

  ObArray<ObExecParamRawExpr *> nl_params_;
  ObQueryCtx *query_ctx_;
  const ObRelIds *left_table_set_;
  ObRawExpr *root_expr_;
  ObRawExprCopier *copier_;
};

int ObJoinOrder::extract_params_for_inner_path(const ObRelIds &join_relids,
                                               ObIArray<ObExecParamRawExpr *> &nl_params,
                                               ObIArray<ObRawExpr*> &subquery_exprs,
                                               const ObIArray<ObRawExpr*> &exprs,
                                               ObIArray<ObRawExpr*> &new_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObQueryRefRawExpr*, 4> query_ref_exprs;
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    NLParamReplacer replacer;
    replacer.query_ctx_ = stmt->get_query_ctx();
    replacer.left_table_set_ = &join_relids;
    replacer.copier_ = &copier;
    if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(exprs, query_ref_exprs))) {
      LOG_WARN("failed to extract query ref expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < query_ref_exprs.count(); ++i) {
      ObQueryRefRawExpr *query_ref_expr = query_ref_exprs.at(i);
      if (OB_ISNULL(query_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < query_ref_expr->get_param_count(); ++j) {
        ObExecParamRawExpr *exec_param = query_ref_expr->get_exec_param(j);
        ObRawExpr *ref_expr = NULL;
        if (OB_ISNULL(exec_param) || OB_ISNULL(ref_expr = exec_param->get_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret));
        } else if (!ref_expr->get_relation_ids().is_subset(join_relids) ||
                   copier.is_existed(ref_expr)) {
          // do nothing
        } else if (OB_FAIL(copier.add_replaced_expr(ref_expr, exec_param))) {
          LOG_WARN("failed to add replaced expr", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObRawExpr *temp_expr = NULL;
      replacer.root_expr_ = exprs.at(i);
      if (OB_FAIL(copier.copy_on_replace(exprs.at(i), temp_expr, &replacer))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(temp_expr->extract_info())) {
        LOG_WARN("failed to extract expr info", K(ret));
      } else if (OB_FAIL(temp_expr->pull_relation_id())) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (temp_expr->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(create_onetime_expr(join_relids, temp_expr))) {
          LOG_WARN("failed to create onetime expr", K(ret));
        } else if (OB_FAIL(temp_expr->extract_info())) {
          LOG_WARN("failed to extract expr info", K(ret));
        } else if (OB_FAIL(subquery_exprs.push_back(temp_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (!temp_expr->has_flag(CNT_SUB_QUERY) &&
                   OB_FAIL(new_exprs.push_back(temp_expr))) {
          LOG_WARN("failed to push back exprs", K(ret));
        }
      } else if (OB_FAIL(new_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(nl_params.assign(replacer.nl_params_))) {
      LOG_WARN("failed to assign nl params", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::is_onetime_expr(const ObRelIds &ignore_relids,ObRawExpr* expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    // if a expr contain psedu column, hierarchical expr, any column
    is_valid =
        !ObOptimizerUtil::has_psedu_column(*expr) &&
        !ObOptimizerUtil::has_hierarchical_expr(*expr) &&
        expr->get_relation_ids().is_subset(ignore_relids) &&
        !expr->has_flag(CNT_AGG) &&
        !expr->has_flag(CNT_WINDOW_FUNC) &&
        !expr->has_flag(CNT_ONETIME) &&
        !expr->has_flag(CNT_ALIAS) &&
        expr->get_ref_count() <= 1;
  }

  if (OB_SUCC(ret) && is_valid && expr->is_query_ref_expr()) {
    bool has_ref_assign_user_var = false;
    if (OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(
                         expr, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (has_ref_assign_user_var) {
      is_valid = false;
    } else if (!static_cast<ObQueryRefRawExpr *>(expr)->is_scalar()) {
      is_valid = false;
    }
  }

  if (OB_SUCC(ret) && is_valid && expr->has_flag(CNT_SUB_QUERY)) {
    if (expr->get_expr_type() == T_FUN_COLUMN_CONV ||
        expr->get_expr_type() == T_OP_ROW) {
      is_valid = false;
    }
  }
  return ret;
}

int ObJoinOrder::create_onetime_expr(const ObRelIds &ignore_relids, ObRawExpr* &expr)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(expr) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(stmt->query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY) &&
             OB_FAIL(is_onetime_expr(ignore_relids, expr, is_valid))) {
    LOG_WARN("failed to check is onetime expr", K(ret));
  } else if (is_valid) {
    ObExecParamRawExpr *new_expr = NULL;
    ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    if (OB_FAIL(expr_factory.create_raw_expr(T_QUESTIONMARK, new_expr))) {
      LOG_WARN("failed to create exec param expr", K(ret));
    } else if (OB_ISNULL(new_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new expr is null", K(ret), K(new_expr));
    } else {
      new_expr->set_ref_expr(expr, true);
      new_expr->set_param_index(*stmt->query_ctx_);
      new_expr->set_result_type(expr->get_result_type());
      ObSQLSessionInfo *session_info = get_plan()->get_optimizer_context().get_session_info();
      if (OB_FAIL(new_expr->formalize(session_info))) {
        LOG_WARN("failed to extract expr info", K(ret));
      } else {
        expr = new_expr;
      }
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(create_onetime_expr(ignore_relids, expr->get_param_expr(i))))) {
        LOG_WARN("failed to create onetime expr", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_valid_path_info_from_hint(const ObRelIds &table_set,
                                               bool both_access,
                                               bool contain_fake_cte,
                                               ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else {
    const ObLogPlanHint &log_hint = get_plan()->get_log_plan_hint();
    const LogJoinHint *log_join_hint = log_hint.get_join_hint(table_set);
    const bool ignore_dist_hint = contain_fake_cte && !log_hint.is_spm_evolution();
    if (ignore_dist_hint) {
      // do nothing.
      // When a join contains a fake CTE table, only the basic and pull-to-local distributed methods can be used.
      // Additionally, there is only one distributed method path that can be utilized to generate a valid plan
      // when allocating recursive UNION ALL. Therefore, for joins including a fake CTE,
      // we can directly ignore the distributed method hint to avoid the inability to generate a valid plan.
      // For SPM evolution, we will throw an error when a valid plan cannot be generated using the hint.
    } else if (NULL != log_join_hint && !log_join_hint->dist_method_hints_.empty()) {
      path_info.distributed_methods_ &= log_join_hint->dist_methods_;
    } else if (log_hint.is_outline_data_) {
      // outline data has no pq distributed hint
      path_info.distributed_methods_ &= DIST_BASIC_METHOD;
    } else if (!get_plan()->get_optimizer_context().is_partition_wise_plan_enabled()) {
      path_info.distributed_methods_ &= ~DIST_PARTITION_NONE;
      path_info.distributed_methods_ &= ~DIST_NONE_PARTITION;
      path_info.distributed_methods_ &= ~DIST_PARTITION_WISE;
      path_info.distributed_methods_ &= ~DIST_EXT_PARTITION_WISE;
    }

    if (NULL != log_join_hint && both_access && !ignore_dist_hint
        && NULL != log_join_hint->slave_mapping_) {
      path_info.force_slave_mapping_ = true;
      path_info.distributed_methods_ &= ~DIST_PULL_TO_LOCAL;
      path_info.distributed_methods_ &= ~DIST_HASH_HASH;
      path_info.distributed_methods_ &= ~DIST_BC2HOST_NONE;
      path_info.distributed_methods_ &= ~DIST_BASIC_METHOD;
    }
    if (NULL != log_join_hint && NULL != log_join_hint->nl_material_) {
      path_info.force_mat_ = log_join_hint->nl_material_->is_enable_hint();
      path_info.force_no_mat_ = log_join_hint->nl_material_->is_disable_hint();
    } else if (log_hint.is_outline_data_) {
      path_info.force_mat_ = false;
      path_info.force_no_mat_ = true;
    }
    if (NULL != log_join_hint && !log_join_hint->local_method_hints_.empty()) {
      path_info.prune_mj_ = !(log_join_hint->local_methods_ & MERGE_JOIN);
      path_info.local_methods_ &= log_join_hint->local_methods_;
    } else if (OB_FAIL(log_hint.check_status())) {  // spm outline mode, must get local_methods_ from hint
      LOG_WARN("failed to get valid local methods from hint", K(ret));
    } else {
      ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
      int64_t local_methods_mask = 0;
      if (opt_ctx.is_hash_join_enabled()) {
        local_methods_mask |= HASH_JOIN;
      }
      if (opt_ctx.is_merge_join_enabled()){
        local_methods_mask |= MERGE_JOIN;
      }
      if (opt_ctx.is_nested_join_enabled()) {
        local_methods_mask |= NESTED_LOOP_JOIN;
      }
      path_info.local_methods_ &= local_methods_mask;
      path_info.prune_mj_ = path_info.local_methods_ != MERGE_JOIN;
    }
  }
  return ret;
}

int ObJoinOrder::get_valid_path_info(const ObJoinOrder &left_tree,
                                     const ObJoinOrder &right_tree,
                                     const ObJoinType join_type,
                                     const ObIArray<ObRawExpr*> &join_conditions,
                                     const bool ignore_hint,
                                     const bool reverse_join_tree,
                                     ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  path_info.join_type_ = join_type;
  path_info.ignore_hint_ = ignore_hint;
  const ObIArray<Path*> &left_paths = left_tree.get_interesting_paths();
  const ObIArray<Path*> &right_paths = right_tree.get_interesting_paths();
  if (OB_ISNULL(get_plan()) || OB_UNLIKELY(left_paths.empty() || right_paths.empty())
      || OB_ISNULL(left_paths.at(0)) || OB_ISNULL(right_paths.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(get_plan()), K(left_paths.count()), K(right_paths.count()));
  } else {
    if (ignore_hint) {
      OPT_TRACE("start generate join method without hint");
    } else {
      OPT_TRACE("start generate join method with hint");
    }
    if (reverse_join_tree) {
      OPT_TRACE("start generate reverse join method");
    }
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    const bool both_access = ACCESS == left_tree.get_type() && ACCESS == right_tree.get_type();
    const bool contain_fake_cte = left_paths.at(0)->contain_fake_cte() || right_paths.at(0)->contain_fake_cte();
    if (CONNECT_BY_JOIN == path_info.join_type_) {
      path_info.local_methods_ = NESTED_LOOP_JOIN | HASH_JOIN;
      path_info.distributed_methods_ = DIST_PULL_TO_LOCAL | DIST_BASIC_METHOD;
      OPT_TRACE("connect by will use nl join");
      OPT_TRACE("connect by will use pull to local / basic method");
    } else if (contain_fake_cte) {
      path_info.local_methods_ = NESTED_LOOP_JOIN | MERGE_JOIN | HASH_JOIN;
      path_info.distributed_methods_ = DIST_PULL_TO_LOCAL | DIST_BASIC_METHOD;
      OPT_TRACE("fake cte table will use basic or pull to local");
    } else {
      path_info.local_methods_ = NESTED_LOOP_JOIN | MERGE_JOIN | HASH_JOIN ;
      path_info.distributed_methods_ = DIST_PULL_TO_LOCAL | DIST_HASH_HASH |
                                       DIST_HASH_NONE | DIST_NONE_HASH |
                                       DIST_BROADCAST_NONE | DIST_NONE_BROADCAST |
                                       DIST_BC2HOST_NONE | DIST_PARTITION_NONE |
                                       DIST_NONE_PARTITION | DIST_PARTITION_WISE |
                                       DIST_EXT_PARTITION_WISE | DIST_BASIC_METHOD |
                                       DIST_NONE_ALL | DIST_ALL_NONE |
                                       DIST_RANDOM_ALL;
      if (!get_plan()->get_optimizer_context().is_partition_wise_plan_enabled()) {
        path_info.distributed_methods_ &= ~DIST_PARTITION_NONE;
        path_info.distributed_methods_ &= ~DIST_NONE_PARTITION;
        path_info.distributed_methods_ &= ~DIST_PARTITION_WISE;
        path_info.distributed_methods_ &= ~DIST_EXT_PARTITION_WISE;
      }
    }
    if (!ignore_hint
        && OB_FAIL(get_valid_path_info_from_hint(right_tree.get_tables(), both_access,
                                                 contain_fake_cte, path_info))) {
      LOG_WARN("failed to get valid path info from hint", K(ret));
    } else if (RIGHT_OUTER_JOIN == path_info.join_type_
               || FULL_OUTER_JOIN == path_info.join_type_) {
      path_info.local_methods_ &= ~NESTED_LOOP_JOIN;
      OPT_TRACE("right or full outer join can not use nested loop join");
    } else if (RIGHT_SEMI_JOIN == path_info.join_type_ || RIGHT_ANTI_JOIN == path_info.join_type_) {
      path_info.local_methods_ &= ~NESTED_LOOP_JOIN;
      path_info.local_methods_ &= ~MERGE_JOIN;
      OPT_TRACE("right semi/anti join can not use nested loop/merge join");
    }
    if (OB_SUCC(ret)) {
      bool force_use_nlj = false;
      force_use_nlj = (OB_SUCCESS != (OB_E(EventTable::EN_GENERATE_PLAN_WITH_NLJ) OB_SUCCESS));
      //if tracepoint is triggered and the local methiods contain NLJ, remove the merge-join and hash-join to use nlj as possible
      if (force_use_nlj && (path_info.local_methods_ & NESTED_LOOP_JOIN)) {
        path_info.local_methods_ &= ~MERGE_JOIN;
        path_info.local_methods_ &= ~HASH_JOIN;
      }
    }
    //check batch update join type
    if (OB_SUCC(ret) && get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
      // left_tree is the generated table of batch params and right tree is other path -> NLJ
      // other join order we don't need care it
      bool left_is_batch_table = false;
      bool right_is_batch_table = false;
      if (INNER_JOIN != join_type) {
        //ignore this join path
        path_info.local_methods_ = 0;
      } else if (left_tree.type_ != JOIN &&
          OB_FAIL(ObTransformUtils::is_batch_stmt_write_table(left_tree.get_table_id(),
                                                              *get_plan()->get_stmt(),
                                                              left_is_batch_table))) {
        LOG_WARN("check left tree is the generated table of batch params failed",
                 K(ret), "left_tree table_id", left_tree.get_table_id(), "type", left_tree.type_);
      } else if (right_tree.type_ != JOIN &&
          OB_FAIL(ObTransformUtils::is_batch_stmt_write_table(right_tree.get_table_id(),
                                                              *get_plan()->get_stmt(),
                                                              right_is_batch_table))) {
        LOG_WARN("check right tree is the generated table of batch params failed",
                 K(ret), "right_tree table_id", right_tree.get_table_id(), "type", right_tree.type_);
      } else if (left_is_batch_table || right_is_batch_table) {
        if (left_is_batch_table && right_is_batch_table) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left tree and right tree are batch_stmt, invalid",
                   K(ret), K(join_type), KPC(get_plan()->get_stmt()));
        } else if (left_is_batch_table) {
          //force to use nested loop join in batch update
          //the update target table must be the right table
          path_info.local_methods_ = NESTED_LOOP_JOIN;
          path_info.force_inner_nl_ = true;
          OPT_TRACE("batch multi stmt force use nested loop join");
        } else {
          //ignore this join path
          path_info.local_methods_ = 0;
        }
      }
    }
    //check depend function table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_depend_table(left_tree,
                                      right_tree,
                                      join_type,
                                      path_info))) {
        LOG_WARN("failed to check depend function table", K(ret));
      } else if (OB_FAIL(check_subquery_in_join_condition(join_type,
                                                          join_conditions,
                                                          path_info))) {
        LOG_WARN("failed to check subquery in join condition", K(ret));
      }
    }
    // get distributed path types
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else {
      if (get_cnt_rownum() ||
         (!opt_ctx.is_var_assign_only_in_root_stmt() && opt_ctx.has_var_assign())) {
        path_info.distributed_methods_ &= DIST_PULL_TO_LOCAL | DIST_BASIC_METHOD;
        OPT_TRACE("query with rownum can only use pull to local or basic method");
      }
      if (IS_LEFT_STYLE_JOIN(path_info.join_type_)) {
        // without BC2HOST DIST_BROADCAST_NONE
        path_info.distributed_methods_ &= ~DIST_BROADCAST_NONE;
        path_info.distributed_methods_ &= ~DIST_ALL_NONE;
        OPT_TRACE("left anti/semi/outer join can not use broadcast none method");
      }
      if (IS_RIGHT_STYLE_JOIN(path_info.join_type_)) {
        // without BC2HOST DIST_NONE_BROADCAST
        path_info.distributed_methods_ &= ~DIST_NONE_BROADCAST;
        path_info.distributed_methods_ &= ~DIST_NONE_ALL;
        path_info.distributed_methods_ &= ~DIST_RANDOM_ALL;
        OPT_TRACE("right anti/semi/outer join can not use none broadcast method");
      }
      OPT_TRACE("candi local methods:");
      int64_t local_methods = path_info.local_methods_;
      const ObString join_algo_str[] =
      {
        "NESTED LOOP JOIN",
        "MERGE JOIN",
        "HASH JOIN"
      };
      for (int idx = 0; idx < sizeof(join_algo_str) / sizeof(ObString); ++idx) {
        if (local_methods & 1) {
          OPT_TRACE(join_algo_str[idx]);
        }
        local_methods >>= 1;
      }
      OPT_TRACE("candi distribute methods:");
      int64_t distributed_methods = path_info.distributed_methods_;
      for (int64_t k = 1; k < DistAlgo::DIST_MAX_JOIN_METHOD; k = k << 1) {
        if (distributed_methods & k) {
          DistAlgo dist_algo = get_dist_algo(k);
          OPT_TRACE(ob_dist_algo_str(dist_algo));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_depend_table(const ObJoinOrder &left_tree,
                                      const ObJoinOrder &right_tree,
                                      const ObJoinType join_type,
                                      ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(get_plan()), K(ret));
  } else {
    const ObIArray<TableDependInfo> &infos = get_plan()->get_table_depend_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      const TableDependInfo &info = infos.at(i);
      if (left_tree.get_tables().has_member(info.table_idx_) &&
          info.depend_table_set_.is_subset(right_tree.get_tables())) {
        path_info.local_methods_ = 0;
        OPT_TRACE("right tree has depend function table, ignore this path");
      } else if (right_tree.get_tables().has_member(info.table_idx_) &&
                  info.depend_table_set_.is_subset(left_tree.get_tables())) {
        if (RIGHT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
          ret = OB_NOT_SUPPORTED;
          OPT_TRACE("right/full outer join with depend function table not support");
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "depended function table in right/full outer join");
        } else {
          path_info.local_methods_ &= NESTED_LOOP_JOIN;
          path_info.force_inner_nl_ = true;
          OPT_TRACE("left tree has depend function table, force use nested loop join");
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_subquery_in_join_condition(const ObJoinType join_type,
                                                  const ObIArray<ObRawExpr*> &join_conditions,
                                                  ValidPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  bool has_subquery = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_subquery && i < join_conditions.count(); ++i) {
    const ObRawExpr *expr = join_conditions.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      has_subquery = true;
    }
  }
  if (OB_SUCC(ret) && has_subquery) {
    if (IS_RIGHT_STYLE_JOIN(join_type)) {
      path_info.local_methods_ = 0;
      OPT_TRACE("right style join has correlated subquery, ignore this path");
    } else {
      path_info.local_methods_ &= NESTED_LOOP_JOIN;
      path_info.force_inner_nl_ = true;
      OPT_TRACE("join condition has correlated subquery, force use nested loop join");
    }
  }
  return ret;
}

int ObJoinOrder::extract_used_columns(const uint64_t table_id,
                                      const uint64_t ref_table_id,
                                      bool only_normal_ref_expr,
                                      bool consider_rowkey,
                                      ObIArray<uint64_t> &column_ids,
                                      ObIArray<ColumnItem> &columns)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret), K(get_plan()), K(stmt), K(schema_guard));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id) ||
             OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema, ObSqlSchemaGuard::is_link_table(stmt, table_id)))) {
    LOG_WARN("failed to get table schema", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    const TableItem *table_item = stmt->get_table_item_by_id(table_id);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret));
    } else {
      if (consider_rowkey) {
        // for normal index, add all rowkey info, always used when merge ss-table and mem-table
        // for fulltext index, rowkey info is not necessary
        const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
        uint64_t column_id = OB_INVALID_ID;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
          if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
            LOG_WARN("Fail to get column id", K(ret));
          } else if (OB_FAIL(column_ids.push_back(column_id))) {
            LOG_WARN("Fail to add column id", K(ret));
          } else { /*do nothing*/ }
        }
      }
      // add common column ids
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
        const ColumnItem *col_item = stmt->get_column_item(i);
        if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item or item expr is NULL", K(ret), K(col_item));
        } else if (col_item->table_id_ == table_id &&
                   col_item->expr_->is_explicited_reference() &&
                   !col_item->expr_->is_only_referred_by_stored_gen_col()) {
          if (only_normal_ref_expr && !col_item->expr_->is_referred_by_normal()) {
            //do nothing
          } else if( OB_FAIL(add_var_to_array_no_dup(column_ids, col_item->expr_->get_column_id()))) {
            LOG_WARN("Fail to add column id", K(ret));
          } else if (OB_FAIL(columns.push_back(*col_item))) {
            LOG_WARN("failed to pushback column item", K(ret));
          }
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

/**
 * compute whether is unique index and index need back
 */
int ObJoinOrder::get_simple_index_info(const uint64_t table_id,
                                       const uint64_t ref_table_id,
                                       const uint64_t index_id,
                                       bool &is_unique_index,
                                       bool &is_index_back,
                                       bool &is_index_global)
{
  int ret = OB_SUCCESS;
  is_unique_index = false;
  is_index_back = false;
  ObSEArray<uint64_t, 16> column_ids;
  ObSEArray<ColumnItem, 2> dummy_columns;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *index_schema = NULL;
  const ObDMLStmt *stmt = nullptr;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret), K(get_plan()), K(stmt), K(schema_guard));
  } else if (OB_UNLIKELY(OB_INVALID_ID == ref_table_id) ||
             OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(ref_table_id), K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_FAIL(extract_used_columns(table_id,
                                          ref_table_id,
                                          true,
                                          !index_schema->is_fts_index_aux(),
                                          column_ids,
                                          dummy_columns))) {
    LOG_WARN("failed to extract column ids", K(table_id), K(ref_table_id), K(ret));
  } else if (index_id != ref_table_id) {
    is_unique_index = index_schema->is_unique_index();
    is_index_global = index_schema->is_global_index_table();
    is_index_back = index_schema->is_spatial_index() ? true : false;
    is_index_back = (is_index_back || index_schema->is_multivalue_index_aux());
    for (int64_t idx = 0; OB_SUCC(ret) && !is_index_back && idx < column_ids.count(); ++idx) {
      bool found = false;
      const uint64_t used_column_id = column_ids.at(idx);
      if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == used_column_id) {
        // rowid can be computed directly from index table.
      } else if (OB_FAIL(index_schema->has_column(used_column_id, found))) {
        LOG_WARN("check index_schema has column failed", K(found),
                 K(idx), K(column_ids.at(idx)), K(ret));
      } else if (!found) {
        is_index_back = true;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
      //index scan with ora rowscn must lookup the data table
      ObRawExpr *expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type() &&
          static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id) {
        is_index_back = true;
      } else { /*do nothing*/}
    }
  } else {
    is_index_global = false;
    is_unique_index = true;
    is_index_back = false;
  }
  return ret;
}

/**
  * prefix_filters: 影响query_range range范围的filter
  * pushdown prefix filters: push down filters that can contribute query range
  * postfix_filters: filters that can be evaluated on index
  * table_filters: filters that can be evaluated after index back
  */
int ObJoinOrder::fill_filters(const ObIArray<ObRawExpr*> &all_filters,
                              const ObQueryRangeProvider *query_range_provider,
                              ObCostTableScanInfo &est_cost_info,
                              const DomainIndexAccessInfo &tr_index_info,
                              bool &is_nl_with_extended_range,
                              bool is_link,
                              bool use_skip_scan)
{
  int ret = OB_SUCCESS;
  is_nl_with_extended_range = false;
  if (NULL == query_range_provider) {
    //如果没有抽出query range，那么所有的filter都是table filter
    ret = est_cost_info.table_filters_.assign(all_filters);
  } else {
    ObSqlSchemaGuard *schema_guard = NULL;
    const ObTableSchema* index_schema = NULL;
    ObSEArray<uint64_t, 4> expr_column_ids;
    ObSEArray<uint64_t, 4> index_column_ids;
    ObSEArray<uint64_t, 4> prefix_column_ids;
    ObSEArray<uint64_t, 4> ex_prefix_column_ids;
    ObSEArray<ObColDesc, 16> index_column_descs;
    const ObIArray<ObRawExpr*> &unprecise_range_exprs = query_range_provider->get_unprecise_range_exprs();
    if (OB_ISNULL(get_plan()) ||
        OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(get_plan()), K(schema_guard), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(est_cost_info.index_id_,
                                                      index_schema))) {
      LOG_WARN("failed to get index schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(index_schema), K(ret));
    } else if (OB_FAIL(index_schema->get_column_ids(index_column_descs))) {
      LOG_WARN("failed to extract column ids", K(ret));
    } else {
      uint64_t geo_column_id;
      bool is_geo_index = index_schema->is_spatial_index();
      if (is_geo_index && OB_FAIL(index_schema->get_spatial_geo_column_id(geo_column_id))) {
        LOG_WARN("failed to extract geo column id", K(ret));
      }
      // replace cellid column id with geo column id, since filters only record geo column id.
      for (int64_t i = 0; OB_SUCC(ret) && i < index_column_descs.count(); i++) {
        if (is_geo_index && (i == 0) && OB_FAIL(add_var_to_array_no_dup(index_column_ids, geo_column_id))) {
          LOG_WARN("failed to add geo member", K(ret), K(i), K(geo_column_id));
        } else if (OB_FAIL(add_var_to_array_no_dup(index_column_ids, static_cast<uint64_t>(index_column_descs.at(i).col_id_)))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
      //
      if (OB_SUCC(ret)) {
        if (OB_FAIL(est_cost_info.prefix_filters_.assign(query_range_provider->get_range_exprs()))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (use_skip_scan &&
                OB_FAIL(est_cost_info.ss_postfix_range_filters_.assign(query_range_provider->get_ss_range_exprs()))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (est_cost_info.index_meta_info_.is_fulltext_index_ &&
                   OB_FAIL(append_array_no_dup(est_cost_info.prefix_filters_, tr_index_info.index_scan_filters_))) {
          LOG_WARN("failed to assign exprs", K(ret));
        }
      }

      //prefix filter中可能存在条件下推的subquery
      //这种filter在存储层无法估行
      //例如：index(c1,c2,c3),filter：c1 = 1 and c2 = ? and c3 = 3 and c1 = ? and c2 = 1
      //真正的prefix filter是c1 = 1 and c2 = 1 and c3 = 3，
      //c1 = ? and c2 = ? 需要放到pushdown prefix filter中
      ObSEArray<ObRawExpr*, 4> new_prefix_filters;
      ObSEArray<uint64_t, 4> column_ids;
      //首先找到所有条件下推的表达式包含的index column ids
      for (int64_t i = 0; OB_SUCC(ret) && i < est_cost_info.prefix_filters_.count(); ++i) {
        ObRawExpr *expr = est_cost_info.prefix_filters_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret));
        } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
          ret = est_cost_info.pushdown_prefix_filters_.push_back(expr);
        } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(expr,
                                                              est_cost_info.table_id_,
                                                              column_ids))) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else {
          ret = new_prefix_filters.push_back(expr);
        }
      }

      int64_t first_param_column_idx = -1;
      //把range_columns分成有query range的column ids与没有query range的column ids
      for (int64_t i = 0; OB_SUCC(ret) && first_param_column_idx == -1 &&
                          i < est_cost_info.range_columns_.count(); ++i) {
        ColumnItem &column = est_cost_info.range_columns_.at(i);
        uint64_t column_id = column.column_id_;
        if (is_geo_index && (i == 0)) {
          column_id = geo_column_id;
        }
        if (!ObOptimizerUtil::find_item(column_ids, column_id)) {
          first_param_column_idx = i;
        } else {
          ret = add_var_to_array_no_dup(prefix_column_ids, column_id);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(append_array_no_dup(ex_prefix_column_ids, prefix_column_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (-1 != first_param_column_idx) {
        uint64_t column_id = est_cost_info.range_columns_.at(first_param_column_idx).column_id_;
        if (is_geo_index && (first_param_column_idx == 0)) {
          column_id = geo_column_id;
        }
        ret = add_var_to_array_no_dup(ex_prefix_column_ids, column_id);
      }

      //如果filter不在prefix filter中，但是是索引列的filter，那么应该在回表前计算
      for (int64_t i = 0; OB_SUCC(ret) && i < all_filters.count(); i++) {
        expr_column_ids.reset();
        ObRawExpr *filter = NULL;
        bool can_extract = false;
        if (OB_ISNULL(filter = all_filters.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret));
        } else if (ObOptimizerUtil::find_equal_expr(est_cost_info.prefix_filters_, filter)) {
          /*do nothing*/
        } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(filter, est_cost_info.table_id_,
                                                               expr_column_ids))) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else if (!ObOptimizerUtil::is_subset(expr_column_ids, index_column_ids)) {
          ret = est_cost_info.table_filters_.push_back(filter);
        } else if (OB_FAIL(can_extract_unprecise_range(est_cost_info.table_id_, filter,
                                                       ex_prefix_column_ids,
                                                       unprecise_range_exprs,
                                                       can_extract))) {
          LOG_WARN("failed to extract column ids", K(ret));
        } else if (can_extract) {
          ret = est_cost_info.pushdown_prefix_filters_.push_back(filter);
        } else if (est_cost_info.ref_table_id_ != est_cost_info.index_id_) {
          if (!use_skip_scan || !ObOptimizerUtil::find_item(est_cost_info.ss_postfix_range_filters_, filter)) {
            ret = est_cost_info.postfix_filters_.push_back(filter);
          }
          // 对于空间索引，空间谓词一定要回表计算
          if (OB_SUCC(ret) &&
              est_cost_info.index_meta_info_.is_domain_index()) {
            ret = est_cost_info.table_filters_.push_back(filter);
          }
        } else {
          if (!use_skip_scan || !ObOptimizerUtil::find_item(est_cost_info.ss_postfix_range_filters_, filter)) {
            ret = est_cost_info.table_filters_.push_back(filter);
          }
        }
      }

      if (est_cost_info.pushdown_prefix_filters_.empty()) {
        //没有EXEC_PARAM, do nothing
      } else if (!column_ids.empty()) {
        est_cost_info.prefix_filters_.reset();
        //没有query range的prefix_filters需要放到pushdown prefix filter中
        for (int64_t i = 0; OB_SUCC(ret) && i < new_prefix_filters.count(); ++i) {
          expr_column_ids.reset();
          ObRawExpr *filter = new_prefix_filters.at(i);
          if (OB_ISNULL(filter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null expr", K(ret));
          } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(filter,
                                                        est_cost_info.table_id_,
                                                        expr_column_ids))) {
            LOG_WARN("failed to extract column ids", K(ret));
          } else if (ObOptimizerUtil::is_subset(expr_column_ids, prefix_column_ids)) {
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
        LOG_TRACE("succeed to classify filters", K(est_cost_info.prefix_filters_),
            K(est_cost_info.pushdown_prefix_filters_), K(est_cost_info.ss_postfix_range_filters_),
            K(est_cost_info.postfix_filters_),
            K(est_cost_info.table_filters_), K(is_nl_with_extended_range));
      }
    }
  }
  return ret;
}

int ObJoinOrder::can_extract_unprecise_range(const uint64_t table_id,
                                             const ObRawExpr *filter,
                                             const ObIArray<uint64_t> &prefix_column_ids,
                                             const ObIArray<ObRawExpr*> &unprecise_exprs,
                                             bool &can_extract)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *exec_param = NULL;
  const ObColumnRefRawExpr *column = NULL;
  can_extract = false;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (ObOptimizerUtil::find_equal_expr(unprecise_exprs, filter) &&
             filter->has_flag(CNT_DYNAMIC_PARAM)) {
    can_extract = true;
  } else if (T_OP_EQ == filter->get_expr_type() || T_OP_NSEQ == filter->get_expr_type()) {
    const ObRawExpr *l_expr = filter->get_param_expr(0);
    const ObRawExpr *r_expr = filter->get_param_expr(1);
    if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(l_expr, l_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(r_expr, r_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (l_expr->is_dynamic_const_expr() && r_expr->is_column_ref_expr()) {
      column = static_cast<const ObColumnRefRawExpr*>(r_expr);
      exec_param = l_expr;
    } else if (l_expr->is_column_ref_expr() && r_expr->is_dynamic_const_expr()) {
      column = static_cast<const ObColumnRefRawExpr*>(l_expr);
      exec_param = r_expr;
    }

    if (OB_SUCC(ret) && NULL != column && NULL != exec_param) {
      ObObjType column_type = column->get_result_type().get_type();
      ObObjType exec_param_type = exec_param->get_result_type().get_type();
      if (column->get_table_id() != table_id
          || !ObOptimizerUtil::find_item(prefix_column_ids, column->get_column_id())) {
        /*do nothing*/
      } else if ((ObCharType == column_type && ObVarcharType == exec_param_type)
                || (ObNCharType == column_type && ObNVarchar2Type == exec_param_type)) {
        can_extract = true;
      } else {
        can_extract = false;
      }
    }
  } else if (T_OP_LIKE == filter->get_expr_type()) {
    const ObRawExpr *first_expr = filter->get_param_expr(0);
    const ObRawExpr *patten_expr = filter->get_param_expr(1);
    const ObRawExpr *escape_expr = filter->get_param_expr(2);
    if (OB_ISNULL(first_expr) || OB_ISNULL(patten_expr) || OB_ISNULL(escape_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(first_expr, first_expr))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (first_expr->is_column_ref_expr() &&
               patten_expr->is_dynamic_const_expr() &&
               escape_expr->is_static_const_expr()) {
      column = static_cast<const ObColumnRefRawExpr*>(first_expr);
      ObObjType column_type = column->get_result_type().get_type();
      ObObjType patten_type = patten_expr->get_result_type().get_type();
      if (column->get_table_id() != table_id
          || !ObOptimizerUtil::find_item(prefix_column_ids, column->get_column_id())) {
        /*do nothing*/
      } else if (column_type == patten_type && ob_is_string_type(column_type)) {
        can_extract = true;
      } else {
        can_extract = false;
      }
    }
  } else if (filter->is_spatial_expr()) {
    const ObRawExpr *geo_expr = ObRawExprUtils::skip_inner_added_expr(filter);
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    if (OB_ISNULL(geo_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_ISNULL(l_expr = geo_expr->get_param_expr(0))
              || OB_ISNULL(r_expr = geo_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (l_expr->has_flag(IS_DYNAMIC_PARAM) && r_expr->is_column_ref_expr()) {
      column = static_cast<const ObColumnRefRawExpr*>(r_expr);
      exec_param = l_expr;
    } else if (l_expr->is_column_ref_expr() && r_expr->has_flag(IS_DYNAMIC_PARAM)) {
      column = static_cast<const ObColumnRefRawExpr*>(l_expr);
      exec_param = r_expr;
    }

    if (OB_SUCC(ret) && NULL != column && NULL != exec_param) {
      ObObjType column_type = column->get_result_type().get_type();
      ObObjType exec_param_type = exec_param->get_result_type().get_type();
      if (column->get_table_id() != table_id
          || !ObOptimizerUtil::find_item(prefix_column_ids, column->get_column_id())) {
        /*do nothing*/
      } else if ((ob_is_string_type(column_type) || ob_is_geometry(column_type))
                && (ob_is_string_type(exec_param_type) || ob_is_geometry(exec_param_type))) {
        can_extract = true;
      } else {
        can_extract = false;
      }
    }
  }
  return ret;
}

int ObJoinOrder::compute_table_meta_info(const uint64_t table_id,
                                         const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(table_partition_info_) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(schema_guard), K(get_plan()), K(table_partition_info_), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    table_meta_info_.ref_table_id_ = ref_table_id;
    table_meta_info_.table_rowkey_count_ = table_schema->get_rowkey_info().get_size();
    table_meta_info_.table_column_count_ = table_schema->get_column_count();
    table_meta_info_.micro_block_size_ = table_schema->get_block_size();
    table_meta_info_.part_count_ =
        table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
    table_meta_info_.schema_version_ = table_schema->get_schema_version();
    table_meta_info_.is_broadcast_table_ = table_schema->is_broadcast_table();
    LOG_TRACE("after compute table meta info", K(table_meta_info_));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_est_sel_info_for_access_path(table_id,
                                                  ref_table_id,
                                                  *table_schema))) {
      LOG_WARN("failed to init estimation selectivity info", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::fill_path_index_meta_info(ObIArray<AccessPath *> &access_paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_paths.count(); ++i) {
    AccessPath *ap = access_paths.at(i);
    if (OB_ISNULL(ap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected access path", K(ret), KPC(ap));
    } else if (ap->is_index_merge_path()) {
      ObSEArray<AccessPath*, 4> scan_paths;
      if (OB_FAIL(static_cast<IndexMergePath*>(ap)->get_all_scan_access_paths(scan_paths))) {
        LOG_WARN("failed to get scan access paths from index merge path", K(ret), KPC(ap));
      } else if (OB_FAIL(fill_path_index_meta_info(scan_paths))) { // only two level recursion, does not need SMART_CALL
        LOG_WARN("failed to fill index meta info for scan access paths", K(ret));
      } else {
        ap->est_cost_info_.table_meta_info_ = &table_meta_info_;
      }
    } else if (OB_FAIL(fill_path_index_meta_info_for_one_ap(ap))) {
      LOG_WARN("failed to fill index meta info for one access path", K(ret), KPC(ap));
    }
  }
  return ret;
}

int ObJoinOrder::fill_path_index_meta_info_for_one_ap(AccessPath *access_path)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(access_path) || OB_ISNULL(access_path->table_partition_info_) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(access_path), K(get_plan()), K(schema_guard));
  } else {
    const uint64_t table_id = access_path->table_id_;
    const uint64_t ref_table_id = access_path->ref_table_id_;
    const uint64_t index_id = access_path->index_id_;

    ObIndexMetaInfo &index_meta_info = access_path->est_cost_info_.index_meta_info_;
    index_meta_info.index_part_count_ =
        access_path->table_partition_info_->get_phy_tbl_location_info().get_partition_cnt();
    index_meta_info.index_micro_block_size_ = table_meta_info_.micro_block_size_;
    index_meta_info.index_column_count_ = table_meta_info_.table_column_count_;
    index_meta_info.index_part_size_ = table_meta_info_.part_size_;
    index_meta_info.index_micro_block_count_ = table_meta_info_.has_opt_stat_ ?
                                        table_meta_info_.micro_block_count_ : -1;

    access_path->est_cost_info_.table_meta_info_ = &table_meta_info_;
    ObSEArray<ColumnItem, 2> dummy_columns;
    if (OB_FAIL(extract_used_columns(table_id,
                                    ref_table_id,
                                    index_id != ref_table_id && !access_path->est_cost_info_.index_meta_info_.is_index_back_,
                                    !index_meta_info.is_fulltext_index_,
                                    access_path->est_cost_info_.access_columns_,
                                    dummy_columns))) {
      LOG_WARN("failed to extract used column ids", K(ret));
    } else if (index_id != ref_table_id) {
      const ObTableSchema* index_schema = NULL;
      const ObTableSchema* table_schema = NULL;
      bool has_opt_stat = false;
      if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema,
                                  ObSqlSchemaGuard::is_link_table(get_plan()->get_stmt(), table_id)))) {
        LOG_WARN("failed to get table schema", K(index_id), K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema,
                                  ObSqlSchemaGuard::is_link_table(get_plan()->get_stmt(), table_id)))) {
        LOG_WARN("failed to get table schema", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(init_est_info_for_index(table_id,
                                                  index_id,
                                                  index_meta_info,
                                                  access_path->table_partition_info_,
                                                  *table_schema,
                                                  *index_schema,
                                                  has_opt_stat))) {
        LOG_WARN("failed to init index est info", K(ret));
      } else {
        index_meta_info.index_micro_block_size_ = index_schema->get_block_size();
        index_meta_info.index_column_count_ = index_schema->get_column_count();
        if (!has_opt_stat) {
          index_meta_info.index_part_size_ = table_meta_info_.part_size_
              * (static_cast<double>(index_meta_info.index_column_count_) /
                static_cast<double>(table_meta_info_.table_column_count_));
          index_meta_info.index_micro_block_count_ = table_meta_info_.has_opt_stat_ ?
              table_meta_info_.micro_block_count_
                  * (static_cast<double>(index_meta_info.index_column_count_) /
                    static_cast<double>(table_meta_info_.table_column_count_)) : -1;
        }
      }
    }
  }
  return ret;
}

//find minimal cost path among all the paths
int ObJoinOrder::find_minimal_cost_path(const ObIArray<Path *> &all_paths,
                                        Path *&minimal_cost_path)
{
  int ret = OB_SUCCESS;
  minimal_cost_path = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_paths.count(); ++i) {
    if (OB_ISNULL(all_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (minimal_cost_path == NULL ||
               minimal_cost_path->cost_ > all_paths.at(i)->cost_) {
      minimal_cost_path = all_paths.at(i);
    } else { /*do nothing*/}
  }
  return ret;
}

int ObJoinOrder::find_minimal_cost_path(const ObIArray<ObSEArray<Path*, 16>> &path_list,
                                        ObIArray<Path*> &best_paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); i++) {
    Path *best_path = NULL;
    if (OB_FAIL(find_minimal_cost_path(path_list.at(i), best_path))) {
      LOG_WARN("failed to find minimal cost path", K(ret));
    } else if (OB_ISNULL(best_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get best path", K(ret));
    } else if (OB_FAIL(best_paths.push_back(best_path))) {
      LOG_WARN("failed to push back best paths", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObJoinOrder::push_down_order_siblings(JoinPath *join_path, const Path *right_path)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = static_cast<const ObSelectStmt *>(get_plan()->get_stmt());
  if (OB_ISNULL(join_path) || OB_ISNULL(right_path) ||
      OB_ISNULL(stmt) || OB_ISNULL(right_path->get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(join_path), K(right_path), K(ret));
  } else if (stmt->is_order_siblings()) {
    int64_t right_prefix_pos = 0;
    bool right_need_sort = true;
    if (OB_FAIL((ObOptimizerUtil::check_need_sort(stmt->get_order_items(),
                                                  right_path->ordering_,
                                                  right_path->parent_->get_fd_item_set(),
                                                  right_path->parent_->get_output_equal_sets(),
                                                  right_path->parent_->get_output_const_exprs(),
                                                  get_plan()->get_onetime_query_refs(),
                                                  right_path->parent_->get_is_at_most_one_row(),
                                                  right_need_sort,
                                                  right_prefix_pos)))) {
      LOG_WARN("failed to check if need sort", K(ret));
    } else {
      join_path->right_need_sort_ = right_need_sort;
      join_path->right_prefix_pos_ = right_prefix_pos;
      if (join_path->is_right_need_sort() &&
          OB_FAIL(join_path->right_sort_keys_.assign(stmt->get_order_items()))) {
        LOG_WARN("failed to assign expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObJoinOrder::create_and_add_nl_path(const Path *left_path,
                                        const Path *right_path,
                                        const ObJoinType join_type,
                                        const DistAlgo join_dist_algo,
                                        const bool is_slave_mapping,
                                        const common::ObIArray<ObRawExpr*> &on_conditions,
                                        const common::ObIArray<ObRawExpr*> &where_conditions,
                                        const bool has_equal_cond,
                                        const bool is_normal_nl,
                                        bool need_mat)
{
  int ret = OB_SUCCESS;
  JoinPath *join_path = NULL;
  ObSEArray<ObRawExpr*, 4> normal_filters;
  ObSEArray<ObRawExpr*, 4> subquery_filters;
  if (OB_ISNULL(left_path) || OB_ISNULL(right_path) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(left_path->get_sharding()) ||
      OB_ISNULL(left_path->parent_) || OB_ISNULL(right_path->parent_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(left_path), K(right_path), K(get_plan()), K(ret));
  } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_type) &&
             OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(where_conditions,
                                                              subquery_filters,
                                                              normal_filters,
                                                              false))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (!(IS_OUTER_OR_CONNECT_BY_JOIN(join_type)) &&
             OB_FAIL(normal_filters.assign(where_conditions))) {
    LOG_WARN("failed to assign filters", K(ret));
  } else if (OB_FAIL(alloc_join_path(join_path))) {
    LOG_WARN("failed to allocate a nl join path", K(ret));
  } else {
    join_path = new (join_path) JoinPath(this,
                                         left_path,
                                         right_path,
                                         NESTED_LOOP_JOIN,
                                         join_dist_algo,
                                         is_slave_mapping,
                                         join_type,
                                         need_mat);
    join_path->contain_normal_nl_ = is_normal_nl;
    join_path->has_none_equal_join_ = !has_equal_cond;
    join_path->join_output_rows_ = current_join_output_rows_;
    OPT_TRACE("create new NL Join path:", join_path);
    if (OB_FAIL(set_nl_filters(join_path,
                               right_path,
                               join_type,
                               on_conditions,
                               normal_filters))) {
      LOG_WARN("failed to remove filters", K(ret));
    } else if (IS_SEMI_ANTI_JOIN(join_type)) {
      // nested loop join must be left semi/anti join
      double left_rows = left_path->get_path_output_rows();
      join_path->other_cond_sel_ = left_rows > OB_DOUBLE_EPSINON ?
                                   current_join_output_rows_ / left_rows :
                                   1.0;
    } else {
      get_plan()->get_selectivity_ctx().init_join_ctx(join_type,
                                                      &left_path->parent_->get_tables(),
                                                      &right_path->parent_->get_tables(),
                                                      left_path->get_path_output_rows(),
                                                      right_path->get_path_output_rows());
      if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
          get_plan()->get_update_table_metas(),
          get_plan()->get_selectivity_ctx(),
          join_path->other_join_conditions_,
          join_path->other_cond_sel_,
          get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to calculate selectivity", K(ret), K(join_path->other_join_conditions_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (CONNECT_BY_JOIN == join_type &&
               OB_FAIL(push_down_order_siblings(join_path, right_path))) {
      LOG_WARN("push down order siblings by condition failed", K(ret));
    } else if (OB_FAIL(join_path->compute_join_path_property())) {
      LOG_WARN("failed to compute join path property", K(ret));
    } else if (OB_FAIL(create_subplan_filter_for_join_path(join_path,
                                                           subquery_filters))) {
      LOG_WARN("failed to create subplan filter for join path", K(ret));
    } else if (OB_FAIL(add_path(join_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else {
      LOG_TRACE("succeed to create a nested loop join path", K(join_type),
          K(join_dist_algo), K(need_mat), K(on_conditions), K(where_conditions));
    }
  }
  return ret;
}

int ObJoinOrder::get_used_stat_partitions(const uint64_t ref_table_id,
                                          const share::schema::ObTableSchema &schema,
                                          ObIArray<int64_t> &all_used_part_ids,
                                          ObIArray<int64_t> &table_stat_part_ids,
                                          double &table_stat_scale_ratio,
                                          ObIArray<int64_t> *hist_stat_part_ids/* = NULL*/)
{
  int ret = OB_SUCCESS;
  table_stat_scale_ratio = -1.0;
  table_stat_part_ids.reuse();
  if (NULL != hist_stat_part_ids) {
    hist_stat_part_ids->reuse();
  }
  ObSQLSessionInfo *session_info = NULL;
  bool is_opt_stat_valid = false;
  ObArray<int64_t> table_id;
  ObArray<int64_t> part_ids;
  ObArray<int64_t> subpart_ids;
  ObArray<int64_t> new_used_part_ids;
  ObSchemaGetterGuard *schema_guard = NULL;
  common::ObOptStatManager *opt_stat_manager = NULL;
  int64_t subpart_cnt_in_parts = 0;
  int64_t partition_limit = 0;
  bool get_stat = false;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = OPT_CTX.get_session_info()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
      OB_ISNULL(opt_stat_manager = OPT_CTX.get_opt_stat_manager())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OPT_CTX.use_default_stat()) {
    get_stat = true;
  } else if (OB_FAIL(get_partition_infos(ref_table_id, schema, all_used_part_ids,
                                         table_id, part_ids, subpart_ids, subpart_cnt_in_parts))) {
    LOG_WARN("failed to get partitions ids", K(ret));
  } else if (NULL != hist_stat_part_ids &&
             OB_FAIL(OPT_CTX.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::PARTITION_INDEX_DIVE_LIMIT,
                                                                       session_info,
                                                                       share::SYS_VAR_PARTITION_INDEX_DIVE_LIMIT,
                                                                       partition_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else {
    /**
    * The priority for using statistics is as follows：
    *    0. Single partition table just use statistics of this partition;
    *    1. Table without any partition pruning use the table statistics;
    *    2. a. PARTITION_LEVEL_ONE table use level one partition statistics;
    *       b. PARTITION_LEVEL_TWO table with just some level one partitions after pruning
    *          use level one partition statistics;
    *    3. PARTITION_LEVEL_TWO table use level two partition statistics;
    *    4. PARTITION_LEVEL_TWO table use level one partition statistics with scaling;
    *    5. Use global table statistics with scaling.
    */
    static const int64_t STAT_PRIOR_CNT = 6;
    bool stat_condition[STAT_PRIOR_CNT] = {
      /*0*/ all_used_part_ids.count() == 1 && !is_virtual_table(ref_table_id),
      /*1*/ all_used_part_ids.count() == schema.get_all_part_num(),
      /*2*/ (PARTITION_LEVEL_ONE == schema.get_part_level() ||
            (PARTITION_LEVEL_TWO == schema.get_part_level() &&
             subpart_cnt_in_parts == subpart_ids.count())),
      /*3*/  PARTITION_LEVEL_TWO == schema.get_part_level(),
      /*4*/  PARTITION_LEVEL_TWO == schema.get_part_level(),
      /*5*/  true,
    };
    const ObIArray<int64_t> *stat_parts[STAT_PRIOR_CNT] = {
      /*0*/ &all_used_part_ids,
      /*1*/ &table_id,
      /*2*/ &part_ids,
      /*3*/ &subpart_ids,
      /*4*/ &part_ids,
      /*5*/ &table_id,
    };
    double scale_ratios[STAT_PRIOR_CNT] = {
      /*0*/ 1.0,
      /*1*/ 1.0,
      /*2*/ 1.0,
      /*3*/ 1.0,
      /*4*/ ObOptSelectivity::revise_between_0_1(1.0 * all_used_part_ids.count() / subpart_cnt_in_parts),
      /*5*/ ObOptSelectivity::revise_between_0_1(1.0 * all_used_part_ids.count() / schema.get_all_part_num()),
    };
    partition_limit = partition_limit > 0 ? partition_limit : INT64_MAX;
    bool get_stat = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < STAT_PRIOR_CNT && !get_stat; i ++) {
      bool is_opt_stat_valid = false;
      if (OB_ISNULL(stat_parts[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(stat_parts));
      } else if (!stat_condition[i] || stat_parts[i]->empty()) {
        // do nothing
      } else if (new_used_part_ids.empty() && scale_ratios[i] == 1.0 &&
                 OB_FAIL(new_used_part_ids.assign(*stat_parts[i]))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (!table_stat_part_ids.empty() && NULL != hist_stat_part_ids &&
                 partition_limit < stat_parts[i]->count()) {
        // partitions are too many to use hist, do nothing
      } else if (OB_FAIL(opt_stat_manager->check_opt_stat_validity(*(OPT_CTX.get_exec_ctx()),
                                                                  session_info->get_effective_tenant_id(),
                                                                  ref_table_id,
                                                                  *stat_parts[i],
                                                                  is_opt_stat_valid))) {
        LOG_WARN("failed to check stat version", K(ret));
      } else if (is_opt_stat_valid) {
        if (table_stat_part_ids.empty()) {
          if (OB_FAIL(table_stat_part_ids.assign(*stat_parts[i]))) {
            LOG_WARN("failed to assign", K(ret));
          } else {
            table_stat_scale_ratio = scale_ratios[i];
          }
        }
        if (OB_SUCC(ret) && NULL != hist_stat_part_ids &&
            hist_stat_part_ids->empty() &&
            stat_parts[i]->count() <= partition_limit) {
          if (OB_FAIL(hist_stat_part_ids->assign(*stat_parts[i]))) {
            LOG_WARN("failed to assign", K(ret));
          }
        }
        get_stat = !table_stat_part_ids.empty() &&
                   (NULL == hist_stat_part_ids || !hist_stat_part_ids->empty());
      }
    }
    if (OB_SUCC(ret) && !new_used_part_ids.empty() &&
        OB_FAIL(all_used_part_ids.assign(new_used_part_ids))) {
      LOG_WARN("failed to assign", K(ret));
    }
  }

  LOG_TRACE("get table statistics partition infos",
            K(ref_table_id), K(table_stat_part_ids), KPC(hist_stat_part_ids),
            K(table_id), K(part_ids), K(subpart_ids), K(table_stat_scale_ratio));

  return ret;
}

int ObJoinOrder::get_partition_infos(const uint64_t ref_table_id,
                                     const share::schema::ObTableSchema &schema,
                                     const ObIArray<int64_t> &all_used_part_id,
                                     ObIArray<int64_t> &table_id,
                                     ObIArray<int64_t> &part_ids,
                                     ObIArray<int64_t> &subpart_ids,
                                     int64_t &subpart_cnt_in_parts)
{
  int ret = OB_SUCCESS;
  table_id.reuse();
  part_ids.reuse();
  subpart_ids.reuse();
  subpart_cnt_in_parts = 0;
  if (OB_FAIL(table_id.push_back(schema.is_partitioned_table() ? -1 : ref_table_id))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (!schema.is_partitioned_table()) {
    // do nothing
  } else if (PARTITION_LEVEL_ONE == schema.get_part_level()) {
    if (OB_FAIL(part_ids.assign(all_used_part_id))) {
      LOG_WARN("failed to assign", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == schema.get_part_level()) {
    if (OB_FAIL(subpart_ids.assign(all_used_part_id))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (all_used_part_id.count() == schema.get_all_part_num()) {
      // full table
      if (OB_FAIL(schema.get_all_first_level_part_ids(part_ids))) {
        LOG_WARN("failed to get part ids", K(ret));
      } else {
        subpart_cnt_in_parts = all_used_part_id.count();
      }
    } else {
      if (OB_FAIL(schema.get_part_ids_by_subpart_ids(subpart_ids,
                                                     part_ids,
                                                     subpart_cnt_in_parts))) {
        LOG_WARN("failed to get part id", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected schema", K(schema));
  }
  return ret;
}

int ObJoinOrder::init_est_sel_info_for_access_path(const uint64_t table_id,
                                                   const uint64_t ref_table_id,
                                                   const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 16> column_exprs;
  ObSEArray<uint64_t, 16> column_ids;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id) ||
      OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(table_id), K(ref_table_id), K(ret));
  } else if (OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
             OB_ISNULL(get_plan()) ||
             OB_ISNULL(table_partition_info_) ||
             OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
             OB_ISNULL(schema_guard = OPT_CTX.get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(get_plan()),  K(ret));
  } else if (OB_FAIL(get_plan()->get_column_exprs(table_id, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    ObSEArray<int64_t, 64> all_used_part_id;
    ObSEArray<ObTabletID, 64> all_used_tablet_id;
    ObSEArray<int64_t, 10> table_stat_part_ids;
    ObSEArray<int64_t, 10> hist_stat_part_ids;
    const ObCandiTabletLocIArray &part_loc_info_array =
              table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
      const ObOptTabletLoc &part_loc = part_loc_info_array.at(i).get_partition_location();
      if (OB_FAIL(all_used_part_id.push_back(part_loc.get_partition_id()))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (OB_FAIL(all_used_tablet_id.push_back(part_loc.get_tablet_id()))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }
    LOG_TRACE("init_est_sel_info_for_access_path", K(all_used_part_id), K(all_used_tablet_id), K(ref_table_id));
    if (OB_SUCC(ret)) {
      // 1. try with statistics
      bool has_opt_stat = false;
      OptTableStatType stat_type = OptTableStatType::DEFAULT_TABLE_STAT;
      int64_t last_analyzed = 0;
      bool is_stat_locked = false;
      const int64_t origin_part_cnt = all_used_part_id.count();
      double scale_ratio = 1.0;
      bool stale_stats = false;
      if (OB_FAIL(get_used_stat_partitions(
            ref_table_id, table_schema, all_used_part_id, table_stat_part_ids, scale_ratio, &hist_stat_part_ids))) {
        LOG_WARN("failed to get stat partitions", K(ret));
      } else if (!table_stat_part_ids.empty()) {
        has_opt_stat = true;
        stat_type = scale_ratio == 1.0 ?
                      OptTableStatType::OPT_TABLE_STAT :
                      OptTableStatType::OPT_TABLE_GLOBAL_STAT;
      }

      // TODO, consider move the following codes into access_path_estimation
      if (OB_SUCC(ret) && has_opt_stat) {
        ObGlobalTableStat stat;
        if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->get_table_stat(session_info->get_effective_tenant_id(),
                                                                   ref_table_id,
                                                                   table_stat_part_ids,
                                                                   scale_ratio,
                                                                   stat))) {
          LOG_WARN("failed to get table stats", K(ret));
        } else {
          last_analyzed = stat.get_last_analyzed();
          is_stat_locked = stat.get_stat_locked();
          stale_stats = stat.get_stale_stats();
          table_meta_info_.table_row_count_ = stat.get_row_count();
          table_meta_info_.part_size_ = table_stat_part_ids.count() == origin_part_cnt ?
                                        static_cast<double>(stat.get_avg_data_size()) :
                                        static_cast<double>(stat.get_avg_data_size() * table_stat_part_ids.count()) / origin_part_cnt;
          table_meta_info_.average_row_size_ = static_cast<double>(stat.get_avg_row_size());
          table_meta_info_.micro_block_count_ = stat.get_micro_block_count();
          table_meta_info_.has_opt_stat_ = has_opt_stat;
          LOG_TRACE("total rowcount, use statistics", K(table_meta_info_.table_row_count_),
              K(table_meta_info_.average_row_size_), K(table_meta_info_.micro_block_count_),
              K(table_meta_info_.part_size_), K(has_opt_stat), K(is_stat_locked), K(stale_stats));
        }
      }

      //2. if the table row count is 0 and not to force use default stat, we try refine it.
      if (OB_SUCC(ret) && !table_schema.is_external_table() &&
          table_meta_info_.table_row_count_ <= 0 &&
          !OPT_CTX.use_default_stat()) {
        if (OB_FAIL(ObAccessPathEstimation::estimate_full_table_rowcount(OPT_CTX,
                                                                         *table_partition_info_,
                                                                         table_meta_info_))) {
          LOG_WARN("failed to estimate full table rowcount", K(ret));
        } else {
          LOG_TRACE("succeed to estimate full table rowcount", K(table_meta_info_.table_row_count_));
        }
      }

      //3. fallback with default stats temporary
      if (OB_SUCC(ret) && table_meta_info_.table_row_count_ <= 0) {
        table_meta_info_.table_row_count_ =
              table_schema.is_external_table() ? 100000.0 : ObOptStatManager::get_default_table_row_count();
        table_meta_info_.average_row_size_ = ObOptStatManager::get_default_avg_row_size();
        table_meta_info_.part_size_ = ObOptStatManager::get_default_data_size();
        LOG_TRACE("total rowcount, empty table", K(table_meta_info_.table_row_count_));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
        ObColumnRefRawExpr *col_expr = column_exprs.at(i);
        if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null col expr", K(ret));
        } else if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_plan()->get_basic_table_metas().add_base_table_meta_info(
                      get_plan()->get_selectivity_ctx(),
                      table_id,
                      ref_table_id,
                      table_schema.get_table_type(),
                      table_meta_info_.table_row_count_,
                      table_meta_info_.micro_block_count_,
                      all_used_part_id,
                      all_used_tablet_id,
                      column_ids,
                      stat_type,
                      table_stat_part_ids,
                      hist_stat_part_ids,
                      scale_ratio,
                      last_analyzed,
                      is_stat_locked,
                      table_partition_info_,
                      &table_meta_info_,
                      stale_stats))) {
          LOG_WARN("failed to add base table meta info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_index_partition_ids(const ObCandiTabletLocIArray &part_loc_info_array,
                                         const share::schema::ObTableSchema &table_schema,
                                         const share::schema::ObTableSchema &index_schema,
                                         ObIArray<int64_t> &index_part_ids)
{
  int ret = OB_SUCCESS;
  if (part_loc_info_array.count() == table_schema.get_all_part_num()) {
    // full table
    ObArray<ObTabletID> dummy;
    ObArray<ObObjectID> tmp_part_ids;
    if (OB_FAIL(index_schema.get_all_tablet_and_object_ids(dummy, tmp_part_ids))) {
      LOG_WARN("failed to get part ids", K(ret));
    } else if (OB_FAIL(append(index_part_ids, tmp_part_ids))) {
      LOG_WARN("failed to append", K(ret));
    }
  } else {
    ObArray<int64_t> table_part_ids;
    ObArray<int64_t> part_idx;
    ObArray<int64_t> subpart_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
      const ObOptTabletLoc &part_loc = part_loc_info_array.at(i).get_partition_location();
      if (OB_FAIL(table_part_ids.push_back(part_loc.get_partition_id()))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    }
    if (FAILEDx(table_schema.get_part_idx_by_part_id(table_part_ids, part_idx, subpart_idx))) {
      LOG_WARN("failed to get part idx", K(ret));
    } else if (OB_UNLIKELY(part_idx.count() != subpart_idx.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part idx", K(part_idx), K(subpart_idx));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_idx.count(); ++i) {
      ObObjectID object_id = -1;
      ObObjectID dummy1;
      ObTabletID dummy2;
      if (OB_FAIL(index_schema.get_part_id_and_tablet_id_by_idx(
              part_idx.at(i), subpart_idx.at(i), object_id, dummy1, dummy2))) {
        LOG_WARN("failed to get part id", K(ret));
      } else if (OB_FAIL(index_part_ids.push_back(static_cast<int64_t>(object_id)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::init_est_info_for_index(const uint64_t table_id,
                                         const uint64_t index_id,
                                         ObIndexMetaInfo &index_meta_info,
                                         ObTablePartitionInfo *table_partition_info,
                                         const share::schema::ObTableSchema &table_schema,
                                         const share::schema::ObTableSchema &index_schema,
                                         bool &has_opt_stat)
{
  int ret = OB_SUCCESS;
  has_opt_stat = false;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;

  if (OB_UNLIKELY(OB_INVALID_ID == index_id) ||
      OB_ISNULL(table_partition_info) ||
      OB_ISNULL(session_info = OPT_CTX.get_session_info()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index id", K(index_id), K(ret));
  } else {
    ObSEArray<int64_t, 64> all_used_part_id;
    ObSEArray<int64_t, 64> table_stat_part_ids;
    const ObCandiTabletLocIArray &part_loc_info_array =
              table_partition_info->get_phy_tbl_location_info().get_phy_part_loc_info_list();

    if (index_meta_info.is_global_index_ || is_virtual_table(index_meta_info.ref_table_id_)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
        const ObOptTabletLoc &part_loc = part_loc_info_array.at(i).get_partition_location();
        if (OB_FAIL(all_used_part_id.push_back(part_loc.get_partition_id()))) {
          LOG_WARN("failed to push back partition id", K(ret));
        }
      }
    } else if (!index_schema.is_partitioned_table()) {
      if (OB_FAIL(all_used_part_id.push_back(index_id))) {
        LOG_WARN("failed to push back partition id", K(ret));
      }
    } else if (OB_FAIL(get_index_partition_ids(part_loc_info_array,
                                               table_schema,
                                               index_schema,
                                               all_used_part_id))) {
      LOG_WARN("failed to get index partition ids", K(ret));
    }
    if (OB_SUCC(ret)) {
      const int64_t origin_part_cnt = all_used_part_id.count();
      double scale_ratio = 1.0;
      double index_rows = 0.;
      if (OB_FAIL(get_used_stat_partitions(
            index_id, index_schema, all_used_part_id, table_stat_part_ids, scale_ratio))) {
        LOG_WARN("failed to get stat partitions", K(ret));
      } else if (!table_stat_part_ids.empty()) {
        has_opt_stat = true;
      }
      if (OB_SUCC(ret) && has_opt_stat) {
        ObGlobalTableStat stat;
        if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->get_table_stat(session_info->get_effective_tenant_id(),
                                                                  index_id,
                                                                  table_stat_part_ids,
                                                                  scale_ratio,
                                                                  stat))) {
          LOG_WARN("failed to get table stats", K(ret));
        } else {
          index_meta_info.index_part_size_ = table_stat_part_ids.count() == origin_part_cnt ?
                                             static_cast<double>(stat.get_avg_data_size()) :
                                             static_cast<double>(stat.get_avg_data_size() * table_stat_part_ids.count()) / origin_part_cnt;
          index_meta_info.index_micro_block_count_ = stat.get_micro_block_count();
          index_rows = stat.get_row_count();
          LOG_TRACE("index table, use statistics", K(index_meta_info), K(stat));
        }
      }
      if (OB_SUCC(ret) && has_opt_stat && index_schema.is_global_index_table()) {
        OptTableMeta* table_meta = NULL;
        ObSEArray<ObColumnRefRawExpr*, 16> column_exprs;
        ObSEArray<uint64_t, 16> column_ids;
        if (OB_FAIL(get_plan()->get_column_exprs(table_id, column_exprs))) {
          LOG_WARN("failed to get column exprs", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
          ObColumnRefRawExpr *col_expr = column_exprs.at(i);
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null col expr", K(ret));
          } else if (!col_expr->is_hidden_column()) {
            // do nothing
          } else if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
            LOG_WARN("failed to push back column id", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (column_ids.empty()) {
          // do nothing
        } else if (OB_ISNULL(table_meta = get_plan()->get_basic_table_metas()
                                                     .get_table_meta_by_table_id(table_id))) {
          // do nothing
        } else {
          double rows = table_meta->get_rows();
          double function_index_scale_ratio = 1.0;
          if (rows < index_rows) {
            function_index_scale_ratio = rows / index_rows;
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
            int64_t global_ndv = 0;
            int64_t num_null = 0;
            ObGlobalColumnStat stat;
            OptColumnMeta *col_meta = table_meta->get_column_meta(column_ids.at(i));
            if (col_meta != NULL) {
              if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->get_column_stat(OPT_CTX.get_session_info()->get_effective_tenant_id(),
                                                                          index_id,
                                                                          table_stat_part_ids,
                                                                          column_ids.at(i),
                                                                          index_rows,
                                                                          function_index_scale_ratio,
                                                                          stat,
                                                                          &OPT_CTX.get_allocator()))) {
                LOG_WARN("failed to get column stats", K(ret));
              } else if (OB_FAIL(OptTableMeta::refine_column_stat(stat, rows, *col_meta))) {
                LOG_WARN("failed to refine column stat", K(ret));
              } else {
                global_ndv = col_meta->get_ndv();
                num_null = col_meta->get_num_null();
                col_meta->set_ndv(rows < global_ndv ? rows : global_ndv);
                col_meta->set_num_null(rows < num_null ? rows : num_null);
                col_meta->set_avg_len(stat.avglen_val_);
                col_meta->set_min_value(stat.min_val_);
                col_meta->set_max_value(stat.max_val_);
                col_meta->set_min_max_inited(true);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_use_global_stat(const uint64_t ref_table_id,
                                       const ObTableSchema &schema,
                                       ObIArray<int64_t> &all_used_parts,
                                       ObIArray<ObTabletID> &all_used_tablets,
                                       bool &can_use)
{
  int ret = OB_SUCCESS;
  bool is_opt_stat_valid = false;
  int64_t global_part_id = -1;
  ObArray<int64_t> part_ids;
  can_use = false;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (all_used_parts.count() <= 1 && !is_virtual_table(ref_table_id)) {
    // at most one partition are used
    // directly use the partition
  } else if (all_used_parts.count() == schema.get_all_part_num() ||
             is_virtual_table(ref_table_id)) {
    global_part_id = schema.is_partitioned_table() ? -1 : ref_table_id;
    if (OB_ISNULL(OPT_CTX.get_opt_stat_manager())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(OPT_CTX.get_opt_stat_manager()));
    } else if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->check_opt_stat_validity(*(OPT_CTX.get_exec_ctx()),
                                                                               session_info->get_effective_tenant_id(),
                                                                               ref_table_id,
                                                                               global_part_id,
                                                                               is_opt_stat_valid))) {
      LOG_WARN("failed to check stat version", K(ret));
    } else if (!is_opt_stat_valid) {
      // do nothing
    } else if (OB_FAIL(part_ids.push_back(global_part_id))) {
      LOG_WARN("failed to push back global partition id", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == schema.get_part_level()) {
    int64_t total_subpart_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_used_tablets.count(); ++i) {
      int64_t part_id = OB_INVALID_ID;
      int64_t subpart_id = OB_INVALID_ID;
      ObArray<int64_t> subpart_ids;
      if (OB_FAIL(schema.get_part_id_by_tablet(all_used_tablets.at(i), part_id, subpart_id))) {
        LOG_WARN("failed to get part id by tablet", K(ret), K(all_used_tablets.at(i)));
      } else if (!ObOptimizerUtil::find_item(part_ids, part_id)) {
        if (OB_FAIL(part_ids.push_back(part_id))) {
          LOG_WARN("failed to push back part id", K(ret));
        } else if (OB_ISNULL(OPT_CTX.get_opt_stat_manager())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(OPT_CTX.get_opt_stat_manager()));
        } else if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->check_opt_stat_validity(*(OPT_CTX.get_exec_ctx()),
                                                                                   session_info->get_effective_tenant_id(),
                                                                                   ref_table_id,
                                                                                   part_id,
                                                                                   is_opt_stat_valid))) {
          LOG_WARN("failed to get stat version", K(ret));
        } else if (!is_opt_stat_valid) {
          // the partition level stat isn't stat
          break;
        } else if (OB_FAIL(schema.get_subpart_ids(part_id, subpart_ids))) {
          LOG_WARN("failed to get subpart ids", K(ret));
        } else {
          total_subpart_cnt += subpart_ids.count();
        }
      }
    }
    if (OB_SUCC(ret) && !(total_subpart_cnt == all_used_parts.count() && is_opt_stat_valid)) {
      part_ids.reset();
    }
  }
  if (OB_SUCC(ret) && !part_ids.empty()) {
    can_use = true;
    if (OB_FAIL(all_used_parts.assign(part_ids))) {
      LOG_WARN("failed to assign partition ids", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::init_est_sel_info_for_subquery(const uint64_t table_id,
                                                ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  ObLogPlan *child_plan = NULL;
  const ObDMLStmt *child_stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(root) ||
      OB_ISNULL(child_plan = root->get_plan()) ||
      OB_ISNULL(child_stmt = child_plan->get_stmt()) ||
      OB_UNLIKELY(!child_stmt->is_select_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(get_plan()), K(root), K(child_plan), K(child_stmt));
  } else {
    child_plan->get_selectivity_ctx().init_op_ctx(root);
    if (OB_FAIL(get_plan()->get_basic_table_metas().add_generate_table_meta_info(
        get_plan()->get_stmt(),
        static_cast<const ObSelectStmt *>(child_stmt),
        table_id,
        child_plan->get_update_table_metas(),
        child_plan->get_selectivity_ctx(),
        root->get_card()))) {
      LOG_WARN("failed to add generate table meta info", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::merge_conflict_detectors(ObJoinOrder *left_tree,
                                          ObJoinOrder *right_tree,
                                          const common::ObIArray<ObConflictDetector*>& detectors)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null join order", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_,
                                         left_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_,
                                         right_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(used_conflict_detectors_, detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_and_remove_is_null_qual(ObLogPlan *plan,
                                              const ObJoinType join_type,
                                              const ObRelIds &left_ids,
                                              const ObRelIds &right_ids,
                                              const ObIArray<ObRawExpr*> &quals,
                                              ObIArray<ObRawExpr*> &normal_quals,
                                              bool &left_has_is_null_qual,
                                              bool &right_has_is_null_qual)
{
  int ret = OB_SUCCESS;
  left_has_is_null_qual = false;
  right_has_is_null_qual = false;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    OptSelectivityCtx& sel_ctx = plan->get_selectivity_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
      bool is_null_qual = false;
      if (LEFT_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(sel_ctx.get_params(),
                                                        sel_ctx.get_stmt(),
                                                        sel_ctx.get_opt_ctx().get_exec_ctx(),
                                                        sel_ctx.get_allocator(),
                                                        right_ids,
                                                        quals.at(i),
                                                        is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          right_has_is_null_qual = true;
        } else if (OB_FAIL(normal_quals.push_back(quals.at(i)))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(sel_ctx.get_params(),
                                                        sel_ctx.get_stmt(),
                                                        sel_ctx.get_opt_ctx().get_exec_ctx(),
                                                        sel_ctx.get_allocator(),
                                                        left_ids,
                                                        quals.at(i),
                                                        is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          left_has_is_null_qual = true;
        } else if (OB_FAIL(normal_quals.push_back(quals.at(i)))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      } else if (FULL_OUTER_JOIN == join_type) {
        if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(sel_ctx.get_params(),
                                                        sel_ctx.get_stmt(),
                                                        sel_ctx.get_opt_ctx().get_exec_ctx(),
                                                        sel_ctx.get_allocator(),
                                                        left_ids,
                                                        quals.at(i),
                                                        is_null_qual))) {
          LOG_WARN("failed to check is null qual", K(ret));
        } else if (is_null_qual) {
          left_has_is_null_qual = true;
        } else if (OB_FAIL(ObOptimizerUtil::check_is_null_qual(sel_ctx.get_params(),
                                                               sel_ctx.get_stmt(),
                                                               sel_ctx.get_opt_ctx().get_exec_ctx(),
                                                               sel_ctx.get_allocator(),
                                                               right_ids,
                                                               quals.at(i),
                                                               is_null_qual))) {
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

int ObJoinOrder::merge_ambient_card(const ObIArray<double> &left_ambient_card,
                                     const ObIArray<double> &right_ambient_card,
                                     ObIArray<double> &cur_ambient_card)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_ambient_card.count() != right_ambient_card.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(left_ambient_card), K(right_ambient_card));
  } else if (OB_FAIL(cur_ambient_card.prepare_allocate(left_ambient_card.count()))) {
    LOG_WARN("failed to allocate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_ambient_card.count(); i ++) {
    double left_rowcnt = left_ambient_card.at(i);
    double right_rowcnt = right_ambient_card.at(i);
    if (OB_UNLIKELY(left_rowcnt >= 0 && right_rowcnt >= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ambient card", K(left_ambient_card), K(right_ambient_card));
    } else if (left_rowcnt >= 0) {
      cur_ambient_card.at(i) = left_rowcnt;
    } else if (right_rowcnt >= 0) {
      cur_ambient_card.at(i) = right_rowcnt;
    } else {
      cur_ambient_card.at(i) = -1;
    }
  }
  return ret;
}

int ObJoinOrder::scale_ambient_card(const double origin_rows,
                                    const double new_rows,
                                    const ObIArray<double> &origin_ambient_card,
                                    ObIArray<double> &ambient_card)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ambient_card.assign(origin_ambient_card))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (new_rows < origin_rows) {
    for (int64_t i = 0; i < ambient_card.count(); i ++) {
      if (ambient_card.at(i) >= 0) {
        ambient_card.at(i) = ObOptSelectivity::scale_distinct(new_rows, origin_rows, ambient_card.at(i));
      }
    }
  }
  return ret;
}

int ObJoinOrder::revise_cardinality(const ObJoinOrder *left_tree,
                                    const ObJoinOrder *right_tree,
                                    const JoinInfo &join_info)
{
  int ret = OB_SUCCESS;
  double sel = 1.0;
  EqualSets equal_sets;
  ObSEArray<double, 8> cur_join_ambient_card;
  current_join_output_rows_ = 0.0;
  double new_rows = 0.0;
  double selectivity = 0.0;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) ||
      OB_ISNULL(get_plan()) || OB_ISNULL(OPT_CTX.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_tree), K(right_tree), K(get_plan()), K(ret));
  } else if (!OPT_CTX.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                                COMPAT_VERSION_4_3_3)) {
    // do nothing
  } else if (OB_FAIL(append(equal_sets, left_tree->get_output_equal_sets())) ||
             OB_FAIL(append(equal_sets, right_tree->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else if (OB_FAIL(merge_ambient_card(left_tree->get_ambient_card(), right_tree->get_ambient_card(), cur_join_ambient_card))) {
    LOG_WARN("failed to merge rowcnts", K(ret));
  } else if (OB_FAIL(calc_join_output_rows(get_plan(),
                                           left_tree->get_tables(),
                                           right_tree->get_tables(),
                                           left_tree->get_output_rows(),
                                           right_tree->get_output_rows(),
                                           join_info, current_join_output_rows_,
                                           selectivity, equal_sets))) {
    LOG_WARN("failed to calc join output rows", K(ret));
  } else if (OB_FAIL(calc_join_ambient_card(get_plan(),
                                            *left_tree,
                                            *right_tree,
                                            current_join_output_rows_,
                                            join_info, equal_sets,
                                            cur_join_ambient_card))) {
    LOG_WARN("failed to scale base table rowcnts", K(ret));
  } else if (OB_UNLIKELY(cur_join_ambient_card.count() != ambient_card_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ambient card", K(left_tree->get_ambient_card()),
      K(right_tree->get_ambient_card()), K(cur_join_ambient_card), K(ambient_card_));
  } else {
    get_plan()->get_selectivity_ctx().clear();

    // choose the minimal ambient cardinality and maximum output rowcnt
    for (int64_t i = 0; i < ambient_card_.count(); i ++) {
      ambient_card_.at(i) = std::min(ambient_card_.at(i), cur_join_ambient_card.at(i));
    }
    new_rows = std::max(current_join_output_rows_, get_output_rows());
    set_output_rows(new_rows);
    OPT_TRACE("left output rows :", left_tree->get_output_rows(), " ambient cardinality :", left_tree->get_ambient_card());
    OPT_TRACE("right output rows :", right_tree->get_output_rows(), " ambient cardinality :", right_tree->get_ambient_card());
    OPT_TRACE("output rows of", left_tree, "join", right_tree, ":", current_join_output_rows_, " ambient cardinality :", cur_join_ambient_card);
    OPT_TRACE("Revised ambient cardinality :", ambient_card_);
    OPT_TRACE("Revised output rows :", new_rows);
    LOG_DEBUG("estimate join ambient card", K(table_set_), K(left_tree->get_tables()), K(right_tree->get_tables()), K(cur_join_ambient_card));
  }
  return ret;
}


int ObJoinOrder::calc_join_ambient_card(ObLogPlan *plan,
                                        const ObJoinOrder &left_tree,
                                        const ObJoinOrder &right_tree,
                                        const double join_output_rows,
                                        const JoinInfo &join_info,
                                        EqualSets &equal_sets,
                                        ObIArray<double> &ambient_card)
{
  int ret = OB_SUCCESS;
  const ObJoinType join_type = join_info.join_type_;
  JoinInfo tmp_join_info;
  double left_ambient_card_sel = 1.0;
  double right_ambient_card_sel = 1.0;
  double where_sel_for_oj = 1.0;
  double tmp_rows = 0.0;
  ObSEArray<ObRawExpr *, 4> join_conditions;
  ObSEArray<double, 4> ambient_card_sels;
  const ObRelIds &left_ids = left_tree.get_tables();
  const ObRelIds &right_ids = right_tree.get_tables();
  double left_output_rows = left_tree.get_output_rows();
  double right_output_rows = right_tree.get_output_rows();
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(append(join_conditions, join_info.on_conditions_)) ||
             OB_FAIL(append(join_conditions, join_info.where_conditions_))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(ambient_card_sels.prepare_allocate(ambient_card.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    plan->get_selectivity_ctx().set_assumption_type(join_type);
  }
  for (int64_t i = 0; i < ambient_card_sels.count(); i ++) {
    ambient_card_sels.at(i) = 1.0;
  }

  // calculate selectivity for left ambient cardinality
  if (OB_SUCC(ret)) {
    tmp_join_info.join_type_ = IS_ANTI_JOIN(join_type) ? LEFT_ANTI_JOIN : LEFT_SEMI_JOIN;
    tmp_join_info.where_conditions_.reuse();
    if (CONNECT_BY_JOIN == join_type) {
      // todo
    } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_type)) {
      left_ambient_card_sel = 0.0;
      for (int64_t i = 0; i < ambient_card_sels.count(); i ++) {
        if (left_ids.has_member(i)) {
          ambient_card_sels.at(i) = 0.0;
        }
      }
    } else if (LEFT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      left_ambient_card_sel = 1.0;
      for (int64_t i = 0; i < ambient_card_sels.count(); i ++) {
        if (left_ids.has_member(i)) {
          ambient_card_sels.at(i) = 1.0;
        }
      }
    } else if (RIGHT_OUTER_JOIN == join_type && OB_FAIL(append(tmp_join_info.where_conditions_, join_info.on_conditions_))) {
      LOG_WARN("failed to append on conditions", K(ret));
    } else if (!IS_OUTER_JOIN(join_type) && OB_FAIL(append(tmp_join_info.where_conditions_, join_info.where_conditions_))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(calc_join_output_rows(plan,
                                             left_tree.get_tables(),
                                             right_tree.get_tables(),
                                             left_output_rows,
                                             right_output_rows,
                                             tmp_join_info,
                                             tmp_rows,
                                             left_ambient_card_sel,
                                             equal_sets))) {
      LOG_WARN("failed to calc join output rows", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ambient_card_sels.count(); i ++) {
        if (!left_ids.has_member(i) || !join_info.table_set_.has_member(i)) {
          // do nothing
        } else if (OB_FAIL(calc_table_ambient_card(plan,
                                                   i,
                                                   left_tree,
                                                   right_tree,
                                                   left_output_rows,
                                                   right_output_rows,
                                                   tmp_join_info,
                                                   equal_sets,
                                                   ambient_card,
                                                   ambient_card_sels.at(i),
                                                   join_type))) {
          LOG_WARN("failed to calc table ambient card", K(ret));
        }
      }
    }
  }

  // calculate selectivity for right ambient cardinality
  if (OB_SUCC(ret)) {
    tmp_join_info.join_type_ = IS_ANTI_JOIN(join_type) ? RIGHT_ANTI_JOIN : RIGHT_SEMI_JOIN;
    tmp_join_info.where_conditions_.reuse();
    if (CONNECT_BY_JOIN == join_type) {
      // todo
    } else if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
      right_ambient_card_sel = 0.0;
      for (int64_t i = 0; i < ambient_card_sels.count(); i ++) {
        if (right_ids.has_member(i)) {
          ambient_card_sels.at(i) = 0.0;
        }
      }
    } else if (RIGHT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      right_ambient_card_sel = 1.0;
      for (int64_t i = 0; i < ambient_card_sels.count(); i ++) {
        if (right_ids.has_member(i)) {
          ambient_card_sels.at(i) = 1.0;
        }
      }
    } else if (LEFT_OUTER_JOIN == join_type && OB_FAIL(append(tmp_join_info.where_conditions_, join_info.on_conditions_))) {
      LOG_WARN("failed to assign on conditions", K(ret));
    } else if (!IS_OUTER_JOIN(join_type) && OB_FAIL(append(tmp_join_info.where_conditions_, join_info.where_conditions_))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(calc_join_output_rows(plan,
                                             left_tree.get_tables(),
                                             right_tree.get_tables(),
                                             left_output_rows,
                                             right_output_rows,
                                             tmp_join_info,
                                             tmp_rows,
                                             right_ambient_card_sel,
                                             equal_sets))) {
      LOG_WARN("failed to calc join output rows", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ambient_card_sels.count(); i ++) {
        if (!right_ids.has_member(i) || !join_info.table_set_.has_member(i)) {
          // do nothing
        } else if (OB_FAIL(calc_table_ambient_card(plan,
                                                   i,
                                                   left_tree,
                                                   right_tree,
                                                   left_output_rows,
                                                   right_output_rows,
                                                   tmp_join_info,
                                                   equal_sets,
                                                   ambient_card,
                                                   ambient_card_sels.at(i),
                                                   join_type))) {
          LOG_WARN("failed to calc table ambient card", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && IS_OUTER_JOIN(join_type) && !join_info.where_conditions_.empty()) {
    plan->get_selectivity_ctx().init_join_ctx(join_type,
                                              &left_tree.get_tables(),
                                              &right_tree.get_tables(),
                                              left_output_rows,
                                              right_output_rows,
                                              &equal_sets);
    if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
                plan->get_update_table_metas(),
                plan->get_selectivity_ctx(),
                join_info.where_conditions_, where_sel_for_oj,
                plan->get_predicate_selectivities(),
                true/*is_outerjoin_filter*/))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info.where_conditions_), K(ret));
    }
  }

  OPT_TRACE("outer join filter selectivity :", where_sel_for_oj);
  OPT_TRACE("selectivity of the left side :", left_ambient_card_sel);
  OPT_TRACE("selectivity of the right side :", right_ambient_card_sel);
  OPT_TRACE("selectivity of each table :", ambient_card_sels);
  LOG_TRACE("succeed to calc selectivity of all ambient cardinality", K(ret), K(left_ids), K(right_ids),
      K(ambient_card), K(left_ambient_card_sel), K(right_ambient_card_sel), K(ambient_card_sels), K(where_sel_for_oj));

  /**
   * For (t1, t2) left join (t3, t4) on t1.c1 = t3.c1 and t2.c1 = t4.c1 and t1.c2 + t2.c2 < t3.c2 where t1.c3 <=> t3.c3
   * step 1 : table t1 is filtered by the direct join condition, `t1.c1 = t3.c1`
   * step 2 : table t1 is filtered by the indirect join condition, `t2.c1 = t4.c1 and t1.c2 + t2.c2 < t3.c2`
   * step 3 : table t1 is fiterred by the where condition, `t1.c3 <=> t3.c3`
  */
  for (int64_t i = 0; OB_SUCC(ret) && i < ambient_card.count(); i ++) {
    double step1_rows = 0;
    double step2_rows = 0;
    if (left_ids.has_member(i)) {
      ambient_card.at(i) *= ambient_card_sels.at(i);
      step1_rows = left_output_rows * ambient_card_sels.at(i);
      step2_rows = left_output_rows * left_ambient_card_sel;
    } else if (right_ids.has_member(i)) {
      ambient_card.at(i) *= ambient_card_sels.at(i);
      step1_rows = right_output_rows * ambient_card_sels.at(i);
      step2_rows = right_output_rows * right_ambient_card_sel;
    }
    if (ambient_card.at(i) >= 0) {
      if (step2_rows < step1_rows) {
        ambient_card.at(i) = ObOptSelectivity::scale_distinct(step2_rows, step1_rows, ambient_card.at(i));
      }
      if (std::fabs(where_sel_for_oj) <= OB_DOUBLE_EPSINON) {
        ambient_card.at(i) = 0;
      } else {
        ambient_card.at(i) = ObOptSelectivity::scale_distinct(join_output_rows, join_output_rows / where_sel_for_oj, ambient_card.at(i));
      }
      ambient_card.at(i) = std::min(join_output_rows, ambient_card.at(i));
    }
  }
  if (OB_SUCC(ret)) {
    plan->get_selectivity_ctx().set_assumption_type(UNKNOWN_JOIN);
  }
  return ret;
}

/**
 * (t1 join t2 on 1 = 1) join t3 on t1.c1 = t3.c1 and t2.c2 = t3.c2
 * In this case, the ambient cardinality selectivity of (t1, t2) is invalid for single table t1 or t2.
 * It is too small while the ambient cardinality of t1 and t2 might be lossless.
 * So, we calculate the ambient cardinality for each table.
*/
int ObJoinOrder::calc_table_ambient_card(ObLogPlan *plan,
                                         uint64_t table_index,
                                         const ObJoinOrder &left_tree,
                                         const ObJoinOrder &right_tree,
                                         double input_rows,
                                         double right_rows,
                                         const JoinInfo &join_info,
                                         EqualSets &equal_sets,
                                         const ObIArray<double> &ambient_card,
                                         double &ambient_card_sel,
                                         const ObJoinType assumption_type)
{
  int ret = OB_SUCCESS;
  JoinInfo table_join_info;
  table_join_info.join_type_ = join_info.join_type_;
  ObRelIds table_id;
  ObRelIds exclusion_ids;
  double tmp_rows = 1.0;
  ambient_card_sel = 1.0;
  const ObRelIds &left_ids = left_tree.get_tables();
  const ObRelIds &right_ids = right_tree.get_tables();
  bool in_left = left_ids.has_member(table_index);
  bool in_right = right_ids.has_member(table_index);
  if (OB_ISNULL(plan) ||
      OB_UNLIKELY(!IS_SEMI_ANTI_JOIN(join_info.join_type_)) ||
      OB_UNLIKELY(!in_left && !in_right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unepxected param", K(plan), K(join_info), K(left_ids), K(right_ids), K(table_index));
  } else if (OB_FAIL(table_id.add_member(table_index))) {
    LOG_WARN("failed to add member", K(ret));
  } else if (OB_FAIL(exclusion_ids.except(in_left ? left_ids : right_ids, table_id))) {
    LOG_WARN("failed to except", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_info.where_conditions_.count(); i ++) {
    bool is_direct_condition = false;
    if (OB_FAIL(check_direct_join_condition(join_info.where_conditions_.at(i),
                                            equal_sets,
                                            table_id,
                                            exclusion_ids,
                                            is_direct_condition))) {
      LOG_WARN("failed to check join condition", K(ret), K(left_ids), K(right_ids));
    } else if (!is_direct_condition) {
      // do nothing
    } else if (OB_FAIL(table_join_info.where_conditions_.push_back(join_info.where_conditions_.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (table_join_info.where_conditions_.empty()) {
    ambient_card_sel = 1.0;
  } else if (OB_FAIL(calc_join_output_rows(plan,
                                           left_tree.get_tables(),
                                           right_tree.get_tables(),
                                           input_rows,
                                           right_rows,
                                           table_join_info,
                                           tmp_rows,
                                           ambient_card_sel,
                                           equal_sets))) {
    LOG_WARN("failed to calc join output rows", K(ret));
  }
  return ret;
}

/**
 * For `(t1 join t2 on t1.c1 = t2.c1) join t3 on t1.c1 = t3.c1 and t1.c2 = t3.c2`,
 * `t1.c1 = t3.c1` is a direct join condition for `t1`, `t2` and `t3`,
 * `t1.c2 = t3.c2` is a direct join condition only for `t1` and `t3`.
*/
int ObJoinOrder::check_direct_join_condition(ObRawExpr *expr,
                                             const EqualSets &equal_sets,
                                             const ObRelIds &table_id,
                                             const ObRelIds &exclusion_ids,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> col_exprs;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join condition", K(ret), K(expr), K(table_id));
  } else if (expr->get_relation_ids().is_superset(table_id) &&
             !expr->get_relation_ids().overlap(exclusion_ids)) {
    is_valid = true;
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, exclusion_ids, col_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (col_exprs.count() == 1) {
    int64_t eq_set_idx = OB_INVALID_ID;
    if (OB_FAIL(ObOptimizerUtil::find_expr_in_equal_sets(equal_sets,
                                                         col_exprs.at(0),
                                                         eq_set_idx))) {
      LOG_WARN("failed to find expr", K(ret));
    } else if (eq_set_idx != OB_INVALID_ID) {
      const EqualSet& equal_set = *equal_sets.at(eq_set_idx);
      for (int64_t j = 0; OB_SUCC(ret) && !is_valid && j < equal_set.count(); j++) {
        ObRawExpr *equal_expr = equal_set.at(j);
        if (OB_ISNULL(equal_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (equal_expr->get_relation_ids().equal(table_id)) {
          is_valid = true;
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::calc_join_output_rows(ObLogPlan *plan,
                                       const ObRelIds &left_ids,
                                       const ObRelIds &right_ids,
                                       double left_output_rows,
                                       double right_output_rows,
                                       const JoinInfo &join_info,
                                       double &new_rows,
                                       double &selectivity,
                                       const EqualSets &equal_sets)
{
  int ret = OB_SUCCESS;
  const ObJoinType join_type = join_info.join_type_;
  if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(plan->get_selectivity_ctx().init_join_ctx(join_type,
                                                                   &left_ids,
                                                                   &right_ids,
                                                                   left_output_rows,
                                                                   right_output_rows,
                                                                   &equal_sets))) {
  } else if (INNER_JOIN == join_type) {
    if (join_info.where_conditions_.empty()) {
      selectivity = 1.0;
      new_rows = left_output_rows * right_output_rows;
      new_rows = MAX3(new_rows, left_output_rows, right_output_rows);
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(plan->get_update_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             join_info.where_conditions_,
                                                             selectivity,
                                                             plan->get_predicate_selectivities()))) {
      LOG_WARN("Failed to calc filter selectivities", K(ret));
    } else {
      new_rows = left_output_rows * right_output_rows * selectivity;
    }
  } else if (IS_OUTER_JOIN(join_type)) {
    double oj_qual_sel = 1.0;
    double oj_filter_sel = 1.0;
    bool left_has_is_null = false;
    bool right_has_is_null = false;
    ObSEArray<ObRawExpr*, 8> normal_quals;
    if (OB_FAIL(check_and_remove_is_null_qual(plan,
                                              join_type,
                                              left_ids,
                                              right_ids,
                                              join_info.where_conditions_,
                                              normal_quals,
                                              left_has_is_null,
                                              right_has_is_null))) {
      LOG_WARN("failed to check and remove is null qual", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
                plan->get_update_table_metas(),
                plan->get_selectivity_ctx(),
                normal_quals, oj_qual_sel,
                plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info.where_conditions_), K(ret));
    } else if ((LEFT_OUTER_JOIN == join_type && right_has_is_null) ||
               (RIGHT_OUTER_JOIN == join_type && left_has_is_null) ||
               (FULL_OUTER_JOIN == join_type && (right_has_is_null || left_has_is_null))) {
      // t1 left join t2 on xxx where t2.c1 is null => t1 anti join t2 on xxx
      bool is_left_anti = (LEFT_OUTER_JOIN == join_type && right_has_is_null) ||
                          (FULL_OUTER_JOIN == join_type && right_has_is_null);
      bool is_right_anti = (RIGHT_OUTER_JOIN == join_type && left_has_is_null) ||
                           (FULL_OUTER_JOIN == join_type && left_has_is_null);
      JoinInfo tmp_join_info;
      new_rows = 0.0;
      if (OB_FAIL(tmp_join_info.where_conditions_.assign(join_info.on_conditions_))) {
        LOG_WARN("failed to assign on conditions", K(ret));
      } else if (is_left_anti) {
        tmp_join_info.join_type_ = LEFT_ANTI_JOIN;
        double tmp_rows = 0.0;
        double tmp_sel = 1.0;
        if (OB_FAIL(calc_join_output_rows(plan,
                                          left_ids,
                                          right_ids,
                                          left_output_rows,
                                          right_output_rows,
                                          tmp_join_info,
                                          tmp_rows,
                                          tmp_sel,
                                          equal_sets))) {
          LOG_WARN("failed to calc join output rows", K(ret));
        } else {
          new_rows += tmp_rows;
        }
      }
      if (OB_SUCC(ret) && is_right_anti) {
        tmp_join_info.join_type_ = RIGHT_ANTI_JOIN;
        double tmp_rows = 0.0;
        double tmp_sel = 1.0;
        if (OB_FAIL(calc_join_output_rows(plan,
                                          left_ids,
                                          right_ids,
                                          left_output_rows,
                                          right_output_rows,
                                          tmp_join_info,
                                          tmp_rows,
                                          tmp_sel,
                                          equal_sets))) {
          LOG_WARN("failed to calc join output rows", K(ret));
        } else {
          new_rows += tmp_rows;
        }
      }
      if (OB_SUCC(ret)) {
        // although we compute join row count as anti join, but here selectivity is treated as join
        // selectivity. So refine selectivity as output_row / (left_row * right_row)
        selectivity = new_rows / (left_output_rows * right_output_rows);
      }
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
                plan->get_update_table_metas(),
                plan->get_selectivity_ctx(),
                join_info.on_conditions_, oj_filter_sel,
                plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info.on_conditions_), K(ret));
    } else {
      selectivity = oj_filter_sel;
      new_rows = left_output_rows * right_output_rows * selectivity;
      switch (join_type) {
        case LEFT_OUTER_JOIN:
          new_rows = new_rows < left_output_rows? left_output_rows : new_rows;
          break;
        case RIGHT_OUTER_JOIN:
          new_rows = new_rows < right_output_rows ? right_output_rows : new_rows;
          break;
        case FULL_OUTER_JOIN:
          new_rows = new_rows < left_output_rows ? left_output_rows : new_rows;
          new_rows = new_rows < right_output_rows ? right_output_rows : new_rows;
          break;
        default:
          break;
      }
    }
    if (OB_SUCC(ret)) {
      new_rows = new_rows * oj_qual_sel;
      selectivity *= oj_qual_sel;
    }
  } else if (IS_SEMI_ANTI_JOIN(join_type)) {
    double outer_rows = IS_LEFT_SEMI_ANTI_JOIN(join_type)?
          left_output_rows: right_output_rows;
    if (join_info.where_conditions_.empty()) {
      // It is difficult to esitmate whether the inner table is empty,
      // so we assume that this cartesian join filters nothing
      new_rows = outer_rows;
      selectivity = 1.0;
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(plan->get_update_table_metas(),
                                                             plan->get_selectivity_ctx(),
                                                             join_info.where_conditions_,
                                                             selectivity,
                                                             plan->get_predicate_selectivities()))) {
      LOG_WARN("Failed to calc filter selectivities", K(ret));
    } else {
      if (LEFT_SEMI_JOIN == join_type || RIGHT_SEMI_JOIN == join_type) {
        new_rows = outer_rows * selectivity;
      } else {
        selectivity = 1 - selectivity;
        new_rows = outer_rows * selectivity;
      }
    }
  } else if (CONNECT_BY_JOIN == join_type) {
    double join_qual_sel = 1.0;
    double join_filter_sel = 1.0;
    if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
                plan->get_update_table_metas(),
                plan->get_selectivity_ctx(),
                join_info.where_conditions_,
                join_qual_sel,
                plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info.where_conditions_), K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_join_selectivity(
                plan->get_update_table_metas(),
                plan->get_selectivity_ctx(),
                join_info.on_conditions_,
                join_filter_sel,
                plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(join_info.on_conditions_), K(ret));
    } else {
      double connect_by_selectivity = 0.0;
      if (join_filter_sel < 0 || join_filter_sel >= 1) {
        connect_by_selectivity = 1.0;
      } else {
        connect_by_selectivity = 1.0 / (1.0 - join_filter_sel);
      }
      new_rows = left_output_rows + left_output_rows * right_output_rows * join_filter_sel * connect_by_selectivity;
      new_rows = new_rows * join_qual_sel;
      selectivity = connect_by_selectivity;
    }
  }
  plan->get_selectivity_ctx().clear();
  LOG_TRACE("estimate join size and width", K(left_output_rows), K(right_output_rows),
            K(selectivity), K(new_rows));
  return ret;
}

int ObJoinOrder::increase_diverse_path_count(AccessPath *ap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is null", K(ret));
  } else if (NULL == ap->get_query_range_provider() ||
             (ap->get_cost_table_scan_info().ranges_.count() == 1
              && ap->get_cost_table_scan_info().ranges_.at(0).is_whole_range())) {
    // ap is whole range
  } else {
    // ap has query ranges
    ++diverse_path_count_;
  }
  return ret;
}

int ObJoinOrder::deduce_const_exprs_and_ft_item_set() {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is null", K(ret));
  } else if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    ObFdItemFactory &fd_item_factory = get_plan()->get_fd_item_factory();
    ret = fd_item_factory.deduce_fd_item_set(get_output_equal_sets(),
                                             column_exprs,
                                             get_output_const_exprs(),
                                             get_fd_item_set());
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_table_scan(const uint64_t table_id,
                                                    const uint64_t table_ref_id,
                                                    const ObIArray<ObRawExpr *> &quals)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObDMLStmt *stmt = NULL;
  uint64_t index_tids[OB_MAX_AUX_TABLE_PER_MAIN_TABLE];
  int64_t index_count = OB_MAX_AUX_TABLE_PER_MAIN_TABLE;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(stmt), K(schema_guard));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(table_ref_id,
                                                            index_tids,
                                                            index_count,
                                                            false,
                                                            true  /*global index*/,
                                                            false /*domain index*/))) {
    LOG_WARN("failed to get can read index", K(ret), K(table_ref_id));
  }
  for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
    const ObTableSchema *index_schema = NULL;
    uint64_t index_id = (i == -1 ? table_ref_id : index_tids[i]);
    if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null index schema", K(ret));
    } else if (-1 != i && !index_schema->is_unique_index()) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::try_add_fd_item(stmt, get_plan()->get_fd_item_factory(),
                                                        table_id, get_tables(),
                                                        index_schema, quals,
                                                        not_null_columns_, fd_item_set_,
                                                        candi_fd_item_set_))) {
      LOG_WARN("failed to try add fd item", K(ret));
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt()) {
    ObSqlBitSet<> table_set;
    if (OB_FAIL(stmt->get_table_rel_ids(table_id, table_set))) {
      LOG_WARN("fail to get table relids", K(ret));
    } else if OB_FAIL(ObTransformUtils::try_add_table_fd_for_rowid(
                                                 static_cast<const ObSelectStmt*>(stmt),
                                                 get_plan()->get_fd_item_factory(),
                                                 fd_item_set_,
                                                 table_set)) {
      LOG_WARN("fail to add table fd for rowid", K(ret));
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

int ObJoinOrder::compute_fd_item_set_for_join(const ObJoinOrder *left_tree,
                                              const ObJoinOrder *right_tree,
                                              const JoinInfo *join_info,
                                              const ObJoinType join_type)
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

/**
 * 1. 根据连接条件将 candi fd item 提升为 fd item
 * 2. 确定连接是否为 n to 1 连接
 * 3. 根据连接性质确定连接结果的 fd item
 * join_info == NULL means Cartesian join
 */
int ObJoinOrder::compute_fd_item_set_for_inner_join(const ObJoinOrder *left_tree,
                                                    const ObJoinOrder *right_tree,
                                                    const JoinInfo *join_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObFdItem *, 8> left_fd_item_set;
  ObSEArray<ObFdItem *, 8> left_candi_fd_item_set;
  ObSEArray<ObFdItem *, 8> right_fd_item_set;
  ObSEArray<ObFdItem *, 8> right_candi_fd_item_set;
  ObSEArray<ObRawExpr *, 8> left_not_null;
  ObSEArray<ObRawExpr *, 8> right_not_null;
  ObSEArray<ObRawExpr *, 4> left_join_exprs;
  ObSEArray<ObRawExpr *, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr *, 4> right_join_exprs;
  ObSEArray<ObRawExpr *, 4> all_right_join_exprs;
  ObSEArray<ObRawExpr *, 4> join_conditions;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_tree), K(right_tree),  K(get_plan()), K(stmt));
  } else if (OB_FAIL(left_fd_item_set.assign(left_tree->fd_item_set_)) ||
             OB_FAIL(left_candi_fd_item_set.assign(left_tree->candi_fd_item_set_)) ||
             OB_FAIL(right_fd_item_set.assign(right_tree->fd_item_set_)) ||
             OB_FAIL(right_candi_fd_item_set.assign(right_tree->candi_fd_item_set_)) ||
             OB_FAIL(left_not_null.assign(left_tree->not_null_columns_)) ||
             OB_FAIL(right_not_null.assign(right_tree->not_null_columns_)) ||
             (NULL != join_info && OB_FAIL(join_conditions.assign(join_info->equal_join_conditions_)))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (NULL != join_info &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_info->where_conditions_,
                                                          left_candi_fd_item_set,
                                                          left_fd_item_set,
                                                          left_not_null))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (NULL != join_info &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_info->where_conditions_,
                                                          right_candi_fd_item_set,
                                                          right_fd_item_set,
                                                          right_not_null))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(append(not_null_columns_, left_not_null)) ||
             OB_FAIL(append(not_null_columns_, right_not_null))) {
    LOG_WARN("failed to append not null columns", K(ret));
  } else if (NULL != join_info &&
             OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(join_conditions,
                                                               left_tree->get_tables(),
                                                               right_tree->get_tables(),
                                                               left_join_exprs,
                                                               right_join_exprs,
                                                               all_left_join_exprs,
                                                               all_right_join_exprs))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(
                                                      get_plan()->get_fd_item_factory(),
                                                      right_tree->get_output_tables(),
                                                      right_join_exprs,
                                                      right_tree->get_output_const_exprs(),
                                                      right_tree->get_output_equal_sets(),
                                                      right_fd_item_set,
                                                      all_left_join_exprs,
                                                      left_tree->get_output_equal_sets(),
                                                      left_fd_item_set,
                                                      left_candi_fd_item_set,
                                                      fd_item_set_,
                                                      candi_fd_item_set_))) {
    LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(
                                                      get_plan()->get_fd_item_factory(),
                                                      left_tree->get_output_tables(),
                                                      left_join_exprs,
                                                      left_tree->get_output_const_exprs(),
                                                      left_tree->get_output_equal_sets(),
                                                      left_fd_item_set,
                                                      all_right_join_exprs,
                                                      right_tree->get_output_equal_sets(),
                                                      right_fd_item_set,
                                                      right_candi_fd_item_set,
                                                      fd_item_set_,
                                                      candi_fd_item_set_))) {
    LOG_WARN("failed to add right fd_item_set for inner join", K(ret));
  } else if(OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  } else {
    LOG_TRACE("inner join fd item set", K(fd_item_set_));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_semi_anti_join(const ObJoinOrder *left_tree,
                                                        const ObJoinOrder *right_tree,
                                                        const JoinInfo *join_info,
                                                        const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) ||
      !IS_SEMI_ANTI_JOIN(join_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(left_tree), K(right_tree),
                                   K(join_type));
  } else if (IS_LEFT_SEMI_ANTI_JOIN(join_type)) {
    if (OB_FAIL(fd_item_set_.assign(left_tree->fd_item_set_)) ||
        OB_FAIL(candi_fd_item_set_.assign(left_tree->candi_fd_item_set_)) ||
        OB_FAIL(not_null_columns_.assign(left_tree->not_null_columns_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (IS_ANTI_JOIN(join_type)) {
      // anti join 的 filter 不能用来加强减少左表的空值列。
      // SELECT * from A where A.c1 > all (SELECT c1 from B);
      // 当 B 表为空时，A.c1 = NULL 的行依然可以输出
    } else if (NULL != join_info &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_info->where_conditions_,
                                                            candi_fd_item_set_,
                                                            fd_item_set_,
                                                            not_null_columns_))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    } else {
      LOG_TRACE("semi join fd item set", K(fd_item_set_));
    }
  } else if (OB_FAIL(compute_fd_item_set_for_semi_anti_join(right_tree, left_tree, join_info,
                                                            get_opposite_join_type(join_type)))) {
    LOG_WARN("failed to compute fd item set for right semi anti join", K(ret));
  }

  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_outer_join(const ObJoinOrder *left_tree,
                                                    const ObJoinOrder *right_tree,
                                                    const JoinInfo *join_info,
                                                    const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) ||
      OB_ISNULL(join_info) || !IS_OUTER_OR_CONNECT_BY_JOIN(join_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(left_tree), K(right_tree));
  } else if (CONNECT_BY_JOIN == join_type) {
    // do nothing
  } else if (FULL_OUTER_JOIN == join_type) {
    // do nothing
  } else if (LEFT_OUTER_JOIN == join_type) {
    ObSEArray<ObRawExpr *, 8> right_not_null;
    ObSEArray<ObFdItem *, 8> right_fd_item_set;
    ObSEArray<ObFdItem *, 8> right_candi_fd_item_set;
    ObSEArray<ObRawExpr *, 4> left_join_exprs;
    ObSEArray<ObRawExpr *, 4> right_join_exprs;
    ObSEArray<ObRawExpr *, 4> all_left_join_exprs;
    ObSEArray<ObRawExpr *, 4> all_right_join_exprs;

    if (OB_FAIL(right_fd_item_set.assign(right_tree->fd_item_set_)) ||
        OB_FAIL(right_candi_fd_item_set.assign(right_tree->candi_fd_item_set_)) ||
        OB_FAIL(right_not_null.assign(right_tree->not_null_columns_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_info->on_conditions_,
                                                            right_candi_fd_item_set,
                                                            right_fd_item_set,
                                                            right_not_null))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else if (OB_FAIL(not_null_columns_.assign(left_tree->not_null_columns_))) {
      LOG_WARN("failed to assign not null columns", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(join_info->equal_join_conditions_,
                                                                 left_tree->get_tables(),
                                                                 right_tree->get_tables(),
                                                                 left_join_exprs,
                                                                 right_join_exprs,
                                                                 all_left_join_exprs,
                                                                 all_right_join_exprs))) {
      LOG_WARN("failed to get type safe join exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(
                                                      get_plan()->get_fd_item_factory(),
                                                      right_tree->get_output_tables(),
                                                      right_join_exprs,
                                                      right_tree->get_output_const_exprs(),
                                                      right_tree->get_output_equal_sets(),
                                                      right_fd_item_set,
                                                      all_left_join_exprs,
                                                      left_tree->get_output_equal_sets(),
                                                      left_tree->fd_item_set_,
                                                      left_tree->candi_fd_item_set_,
                                                      fd_item_set_,
                                                      candi_fd_item_set_))) {
      /**
      * left tree fd item set 的继承处理与inner join相同
      * right tree fd item set 的继承暂时不处理，如果把right tree的fd item set移到candi fd item set后,
      * 后续要消除candi属性只要求right tree中任意一列上有控制拒绝条件即可.
      */
      LOG_WARN("failed to add left fd_item_set for left outer join", K(ret));
    } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    }
  } else if (OB_FAIL(compute_fd_item_set_for_outer_join(right_tree, left_tree,
                                                        join_info, LEFT_OUTER_JOIN))) {
    LOG_WARN("failed to compute fd item set for right outer join", K(ret));
  }
  return ret;
}

int ObJoinOrder::compute_fd_item_set_for_subquery(const uint64_t table_id,
                                                  ObLogicalOperator *subplan_root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(subplan_root->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(
                       get_plan()->get_fd_item_factory(),
                       get_plan()->get_optimizer_context().get_expr_factory(),
                       subplan_root->get_output_equal_sets(),
                       subplan_root->get_output_const_exprs(),
                       table_id,
                       *get_plan()->get_stmt(),
                       static_cast<const ObSelectStmt &>(*subplan_root->get_stmt()),
                       subplan_root->get_fd_item_set(),
                       fd_item_set_))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set())) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  } else {
    LOG_TRACE("subplan scan fd item set", K(fd_item_set_));
  }
  return ret;
}

int ObJoinOrder::compute_one_row_info_for_join(const ObJoinOrder *left_tree,
                                               const ObJoinOrder *right_tree,
                                               const ObIArray<ObRawExpr*> &join_condition,
                                               const ObIArray<ObRawExpr*> &equal_join_condition,
                                               const ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  bool left_is_unique = false;
  bool right_is_unique = false;
  ObSEArray<ObFdItem *, 8> left_fd_item_set;
  ObSEArray<ObFdItem *, 8> left_candi_fd_item_set;
  ObSEArray<ObFdItem *, 8> right_fd_item_set;
  ObSEArray<ObFdItem *, 8> right_candi_fd_item_set;
  ObSEArray<ObRawExpr *, 8> left_not_null;
  ObSEArray<ObRawExpr *, 8> right_not_null;
  ObSEArray<ObRawExpr *, 4> left_join_exprs;
  ObSEArray<ObRawExpr *, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr *, 4> right_join_exprs;
  ObSEArray<ObRawExpr *, 4> all_right_join_exprs;
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
  } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_condition,
                                                          left_candi_fd_item_set,
                                                          left_fd_item_set,
                                                          left_not_null))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(join_condition,
                                                          right_candi_fd_item_set,
                                                          right_fd_item_set,
                                                          right_not_null))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(equal_join_condition,
                                                               left_tree->get_tables(),
                                                               right_tree->get_tables(),
                                                               left_join_exprs,
                                                               right_join_exprs,
                                                               all_left_join_exprs,
                                                               all_right_join_exprs))) {
    LOG_WARN("failed to extract join exprs", K(ret));
  } else if (left_tree->get_is_at_most_one_row() &&
             (join_type == INNER_JOIN || join_type == LEFT_OUTER_JOIN)) {
    if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(right_join_exprs,
                                                 right_tree->get_tables(),
                                                 right_fd_item_set,
                                                 right_tree->get_output_equal_sets(),
                                                 right_tree->get_output_const_exprs(),
                                                 right_is_unique))) {
      LOG_WARN("failed to check is order unique", K(ret));
    } else {
      is_at_most_one_row_ = right_is_unique;
    }
  } else if (right_tree->get_is_at_most_one_row() &&
             (join_type == INNER_JOIN || join_type == RIGHT_OUTER_JOIN)) {
    if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_join_exprs,
                                                 left_tree->get_tables(),
                                                 left_fd_item_set,
                                                 left_tree->get_output_equal_sets(),
                                                 left_tree->get_output_const_exprs(),
                                                 left_is_unique))) {
     LOG_WARN("failed to check is order unique", K(ret));
   } else {
     is_at_most_one_row_ = left_is_unique;
   }
  } else { /*do nothing*/ }

  LOG_TRACE("succeed to compute one row info for join", K(is_at_most_one_row_));
  return ret;
}

int ObJoinOrder::check_join_interesting_order(Path* path)
{
  int ret = OB_SUCCESS;
  bool join_math = false;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!path->has_interesting_order_flag(OrderingFlag::JOIN_MATCH)) {
    // ordering not used in join, no need to update
  } else if (OB_FAIL(is_join_match(path->get_ordering(), join_math))) {
    LOG_WARN("failed to check is join match", K(ret));
  } else if (!join_math) {
    path->clear_interesting_order_flag(OrderingFlag::JOIN_MATCH);
  }
  return ret;
}


InnerPathInfo* ObJoinOrder::get_inner_path_info(const ObIArray<ObRawExpr *> &join_conditions)
{
  InnerPathInfo *info = NULL;
  for (int64_t i = 0; NULL == info && i < inner_path_infos_.count(); ++i) {
    InnerPathInfo &cur_info = inner_path_infos_.at(i);
    if (ObOptimizerUtil::same_exprs(join_conditions, cur_info.join_conditions_)) {
      info = &cur_info;
    }
  }
  return info;
}

int ObJoinOrder::get_cached_inner_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                        ObJoinOrder &left_tree,
                                        ObJoinOrder &right_tree,
                                        const bool force_inner_nl,
                                        const ObJoinType join_type,
                                        ObIArray<Path *> &inner_paths)
{
  int ret = OB_SUCCESS;
  InnerPathInfo *path_info = right_tree.get_inner_path_info(join_conditions);
  LOG_TRACE("OPT: find nl with param path",
              K(left_tree.get_tables()),
              K(right_tree.get_tables()),
              K(join_conditions),
              K(force_inner_nl),
              K(right_tree.get_inner_path_infos()));
  if (NULL == path_info) {
    // inner path not generate yet
    if (OB_ISNULL(path_info = right_tree.get_inner_path_infos().alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate place holder", K(ret));
    } else if (OB_FAIL(path_info->join_conditions_.assign(join_conditions))) {
      LOG_WARN("failed to assign join conditions", K(ret));
    } else if (OB_FALSE_IT(path_info->force_inner_nl_ = force_inner_nl)
               || OB_FALSE_IT(path_info->join_type_ = join_type)) {
    } else if (OB_FAIL(generate_inner_base_paths(join_conditions,
                                                 left_tree,
                                                 right_tree,
                                                 *path_info))) {
      LOG_WARN("failed to generate inner base paths", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(path_info)) {
    if (OB_FAIL(inner_paths.assign(path_info->inner_paths_))) {
      LOG_WARN("failed to append inner path", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::generate_inner_base_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                           ObJoinOrder &left_tree,
                                           ObJoinOrder &right_tree,
                                           InnerPathInfo &inner_path_info)
{
  int ret = OB_SUCCESS;
  if (ACCESS == right_tree.get_type()) {
    if (OB_FAIL(generate_inner_base_table_paths(join_conditions,
                                               left_tree,
                                               right_tree,
                                               inner_path_info))) {
      LOG_WARN("failed to generate inner path for access", K(ret));
    } else {
      LOG_TRACE("OPT: succeed to generate inner path for access" ,K(inner_path_info));
    }
  } else if (SUBQUERY == right_tree.get_type()) {
    if (OB_FAIL(generate_inner_subquery_paths(join_conditions,
                                              left_tree.get_tables(),
                                              right_tree,
                                              inner_path_info))) {
      LOG_WARN("failed to generate inner subquery path", K(ret));
    } else {
      LOG_TRACE("OPT: succeed generate inner path for subquery" ,K(inner_path_info));
    }
  } else if (inner_path_info.force_inner_nl_) {
    if (OB_FAIL(generate_force_inner_path(join_conditions,
                                          left_tree.get_tables(),
                                          right_tree,
                                          inner_path_info))) {
      LOG_WARN("failed to generate inner path", K(ret));
    } else {
      LOG_TRACE("OPT: succeed generate inner path force" ,K(inner_path_info));
    }
  } else {
    // todo: @guoping.wgp support join type in future
  }

  if (OB_UNLIKELY(OB_ERR_NO_PATH_GENERATED == ret && !inner_path_info.force_inner_nl_)) {
    ret = OB_SUCCESS;
    LOG_TRACE("OPT: generate no inner path" , K(right_tree.get_type()), K(inner_path_info));
  }
  return ret;
}

int ObJoinOrder::generate_inner_base_table_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                                 ObJoinOrder &left_tree,
                                                 ObJoinOrder &right_tree,
                                                 InnerPathInfo &inner_path_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 4> pushdown_quals;
  PathHelper helper;
  helper.is_inner_path_ = true;
  helper.force_inner_nl_ = inner_path_info.force_inner_nl_;
  helper.table_opt_info_ = &inner_path_info.table_opt_info_;
  ObSEArray<ObExecParamRawExpr *, 4> nl_params;
  bool is_valid = false;
  LOG_TRACE("OPT: start to create inner path for access", K(right_tree.get_table_id()));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(check_inner_path_valid(join_conditions,
                                            left_tree.get_tables(),
                                            right_tree,
                                            inner_path_info.force_inner_nl_,
                                            pushdown_quals,
                                            helper.pushdown_filters_,
                                            nl_params,
                                            helper.subquery_exprs_,
                                            helper,
                                            is_valid))) {
    LOG_WARN("failed to check inner path valid", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(helper.filters_.assign(right_tree.get_restrict_infos()))) {
    LOG_WARN("failed to assign restrict infos", K(ret));
  } else if (OB_FAIL(remove_redudant_filter(left_tree,
                                            right_tree,
                                            join_conditions,
                                            helper.filters_,
                                            helper.equal_param_constraints_))) {
    LOG_WARN("failed to remove filter", K(ret));
  } else if (OB_FAIL(append(helper.filters_, helper.pushdown_filters_))) {
    LOG_WARN("failed to append pushdown quals", K(ret));
  } else if (OB_FAIL(right_tree.generate_base_table_paths(helper))) {
    LOG_WARN("failed to generate access paths", K(ret));
  } else if (helper.inner_paths_.count() <= 0) {
    // do nothing.
    // in non static typing engine. if c1 is primary key of t1, when a pushdown qual with
    // format `c1 (int) = ? (datetime)` appeared, check index match will consider pushdown is
    // valid. But this qual can't use to extract query range.
  } else if (OB_FAIL(check_and_fill_inner_path_info(helper,
                                                    *stmt,
                                                    right_tree.get_output_equal_sets(),
                                                    inner_path_info,
                                                    pushdown_quals,
                                                    nl_params))) {
    LOG_WARN("failed to check and fill inner path info", K(ret));
  }
  LOG_TRACE("succeed to create inner access path",
              K(right_tree.get_table_id()), K(pushdown_quals));
  return ret;
}

int ObJoinOrder::check_inner_path_valid(const ObIArray<ObRawExpr *> &join_conditions,
                                        const ObRelIds &join_relids,
                                        ObJoinOrder &right_tree,
                                        const bool force_inner_nl,
                                        ObIArray<ObRawExpr *> &pushdown_quals,
                                        ObIArray<ObRawExpr *> &param_pushdown_quals,
                                        ObIArray<ObExecParamRawExpr *> &nl_params,
                                        ObIArray<ObRawExpr *> &subquery_exprs,
                                        PathHelper &helper,
                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t table_id = right_tree.get_table_id();
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(right_tree.get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table_item));
  } else if (!force_inner_nl && (is_virtual_table(table_item->ref_id_) &&
              !is_oracle_mapping_real_virtual_table(table_item->ref_id_) &&
              table_item->ref_id_ != OB_ALL_VIRTUAL_SQL_AUDIT_TID &&
              table_item->ref_id_ != OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID)) {
    /* __all_virtual_sql_audit and GV$OB_PLAN_CACHE_PLAN_EXPLAIN can do table get*/
    LOG_TRACE("OPT:skip adding inner access path due to virtual table",
                K(table_id), K(table_item->ref_id_),
                K(is_virtual_table(table_item->ref_id_)));
  } else if (OB_FAIL(extract_pushdown_quals(join_conditions,
                                            force_inner_nl,
                                            pushdown_quals))) {
    LOG_WARN("failed to extract pushdown quals", K(ret));
  } else if (OB_FAIL(extract_params_for_inner_path(join_relids,
                                                   nl_params,
                                                   subquery_exprs,
                                                   pushdown_quals,
                                                   param_pushdown_quals))) {
    LOG_WARN("failed to extract params for inner path", K(ret));
  } else if (OB_FAIL(get_generated_col_index_qual(table_item->table_id_,
                                                  param_pushdown_quals,
                                                  helper))) {
    LOG_WARN("failed to deduce generated col index expr", K(ret));
  } else if (force_inner_nl) {
    is_valid = true;
  } else if (OB_FAIL(ObOptimizerUtil::check_pushdown_filter_to_base_table(
              *get_plan(),
              table_item->table_id_,
              param_pushdown_quals,
              right_tree.get_restrict_infos(),
              is_valid))) {
    LOG_WARN("failed to check pushdown filter for access", K(ret));
  }
  return ret;
}

int ObJoinOrder::remove_redudant_filter(ObJoinOrder &left_tree,
                                        ObJoinOrder &right_tree,
                                        const ObIArray<ObRawExpr *> &join_conditions,
                                        ObIArray<ObRawExpr *> &filters,
                                        ObIArray<ObPCParamEqualInfo> &equal_param_constraints)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_filters;
  ObExprParamCheckContext context;
  EqualSets input_equal_sets;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(allocator_,
                                                        join_conditions,
                                                        left_tree.get_output_equal_sets(),
                                                        input_equal_sets))) {
    LOG_WARN("failed to compute equal sets for inner join", K(ret));
  } else {
    context.init(&stmt->get_query_ctx()->calculable_items_,
                 &equal_param_constraints,
                 &input_equal_sets);
  }
  for (int i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObRawExpr *expr = filters.at(i);
    bool is_redunant = false;
    if (OB_FAIL(check_filter_is_redundant(left_tree,
                                          expr,
                                          context,
                                          is_redunant))) {
      LOG_WARN("failed to check filter is redunant", K(ret));
    } else if (!is_redunant) {
      if (OB_FAIL(new_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && new_filters.count() != filters.count()) {
    if (OB_FAIL(filters.assign(new_filters))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::check_filter_is_redundant(ObJoinOrder &left_tree,
                                           ObRawExpr *expr,
                                           ObExprParamCheckContext &context,
                                           bool &is_redunant)
{
  int ret = OB_SUCCESS;
  if (ACCESS == left_tree.get_type() ||
      SUBQUERY == left_tree.get_type()) {
    if (ObPredicateDeduce::find_equal_expr(left_tree.get_restrict_infos(),
                                           expr,
                                           NULL,
                                           &context)) {
      is_redunant = true;
    }
  } else if (JOIN == left_tree.get_type()) {
    if (left_tree.interesting_paths_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect empty interesting paths", K(ret));
    } else {
      JoinPath *join_path = static_cast<JoinPath*>(left_tree.interesting_paths_.at(0));
      if (OB_ISNULL(join_path) ||
          OB_ISNULL(join_path->left_path_) ||
          OB_ISNULL(join_path->left_path_->parent_) ||
          OB_ISNULL(join_path->right_path_) ||
          OB_ISNULL(join_path->right_path_->parent_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null path", K(ret));
      } else if ((INNER_JOIN == join_path->join_type_ ||
                  LEFT_OUTER_JOIN == join_path->join_type_ ||
                  LEFT_SEMI_JOIN == join_path->join_type_ ||
                  LEFT_ANTI_JOIN == join_path->join_type_) &&
                 OB_FAIL(SMART_CALL(check_filter_is_redundant(*join_path->left_path_->parent_,
                                                              expr,
                                                              context,
                                                              is_redunant)))) {
        LOG_WARN("failed to check filter is redunant", K(ret));
      } else if (is_redunant) {
        //do nothing
      } else if ((INNER_JOIN == join_path->join_type_ ||
                  RIGHT_OUTER_JOIN == join_path->join_type_ ||
                  RIGHT_SEMI_JOIN == join_path->join_type_ ||
                  RIGHT_ANTI_JOIN == join_path->join_type_) &&
                 OB_FAIL(SMART_CALL(check_filter_is_redundant(*join_path->right_path_->parent_,
                                                              expr,
                                                              context,
                                                              is_redunant)))) {
        LOG_WARN("failed to check filter is redunant", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_inner_subquery_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                               const ObRelIds &join_relids,
                                               ObJoinOrder &right_tree,
                                               InnerPathInfo &inner_path_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *parent_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> pushdown_quals;
  ObSEArray<ObRawExpr*, 4> dummy;
  LOG_TRACE("OPT:start to generate inner subquery path", K(right_tree.table_id_), K(join_relids));
  if (OB_ISNULL(get_plan()) || OB_ISNULL(parent_stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(parent_stmt), K(ret));
  } else if (OB_FAIL(extract_pushdown_quals(join_conditions,
                                            inner_path_info.force_inner_nl_,
                                            pushdown_quals))) {
    LOG_WARN("failed to extract pushdown quals", K(ret));
  } else if (OB_FAIL(append(pushdown_quals, right_tree.get_restrict_infos()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(right_tree.generate_inner_subquery_paths(*parent_stmt,
                                                              join_relids,
                                                              pushdown_quals,
                                                              inner_path_info))) {
    LOG_WARN("failed to generate inner subquery path", K(table_id_),
        K(join_relids), K(pushdown_quals), K(ret));
  } else {
    LOG_TRACE("succeed to generate inner subquery paths", K(table_id_),
        K(join_relids), K(pushdown_quals));
  }
  return ret;
}

int ObJoinOrder::generate_inner_subquery_paths(const ObDMLStmt &parent_stmt,
                                               const ObRelIds join_relids,
                                               const ObIArray<ObRawExpr*> &pushdown_quals,
                                               InnerPathInfo &inner_path_info)
{
  int ret = OB_SUCCESS;
  PathHelper helper;
  bool can_pushdown = false;
  ObSEArray<ObRawExpr*, 4> param_pushdown_quals;
  ObSEArray<ObRawExpr*, 4> candi_pushdown_quals;
  ObSEArray<ObRawExpr*, 4> neccesary_pushdown_quals;
  ObSEArray<ObRawExpr*, 4> candi_nonpushdown_quals;
  ObSEArray<ObExecParamRawExpr *, 4> nl_params;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSelectStmt *child_stmt = NULL;
  const TableItem *table_item = NULL;
  helper.is_inner_path_ = true;
  helper.is_semi_anti_join_ = IS_SEMI_ANTI_JOIN(inner_path_info.join_type_);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(table_item = parent_stmt.get_table_item_by_id(table_id_)) ||
      OB_ISNULL(child_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(table_item), K(child_stmt));
  } else if (OB_FAIL(extract_params_for_inner_path(join_relids,
                                                   nl_params,
                                                   helper.subquery_exprs_,
                                                   pushdown_quals,
                                                   param_pushdown_quals))) {
    LOG_WARN("failed to extract params for inner path", K(ret));
  } else if (OB_FAIL(extract_necessary_pushdown_quals(param_pushdown_quals,
                                                      neccesary_pushdown_quals,
                                                      candi_nonpushdown_quals))) {
    LOG_WARN("failed to classify push down quals", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::pushdown_filter_into_subquery(parent_stmt,
                                                                    *child_stmt,
                                                                    get_plan()->get_optimizer_context(),
                                                                    neccesary_pushdown_quals,
                                                                    candi_pushdown_quals,
                                                                    helper.filters_,
                                                                    can_pushdown))) {
    LOG_WARN("failed to pushdown filter into subquery", K(ret));
  } else if (table_item->is_lateral_table() &&
             OB_FAIL(append(nl_params, table_item->exec_params_))) {
    LOG_WARN("failed to append exec params", K(ret));
  } else if (!inner_path_info.force_inner_nl_ && !can_pushdown) {
    //do thing
    LOG_TRACE("can not pushdown any filter into subquery");
  } else if (OB_FALSE_IT(helper.child_stmt_ = child_stmt)) {
  } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(parent_stmt,
                                                            *child_stmt,
                                                            table_id_,
                                                            session_info,
                                                            *expr_factory,
                                                            candi_pushdown_quals,
                                                            helper.pushdown_filters_))) {
    LOG_WARN("failed to rename pushdown filter", K(ret));
  } else if (OB_FAIL(append(helper.filters_, candi_nonpushdown_quals))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_onetime_exprs(helper.pushdown_filters_,
                                                        helper.exec_params_))) {
    LOG_WARN("failed to get onetime exprs", K(ret));
  } else if (OB_FAIL(append(helper.exec_params_, nl_params))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(generate_subquery_paths(helper))) {
    LOG_WARN("failed to generate subquery path", K(ret));
  } else if (OB_FAIL(check_and_fill_inner_path_info(helper,
                                                    parent_stmt,
                                                    get_output_equal_sets(),
                                                    inner_path_info,
                                                    pushdown_quals,
                                                    nl_params))) {
    LOG_WARN("failed to check and fill inner path info", K(ret));
  }
  return ret;
}

int ObJoinOrder::check_and_fill_inner_path_info(PathHelper &helper,
                                                const ObDMLStmt &stmt,
                                                const EqualSets &equal_sets,
                                                InnerPathInfo &inner_path_info,
                                                const ObIArray<ObRawExpr *> &pushdown_quals,
                                                ObIArray<ObExecParamRawExpr *> &nl_params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exec_params;
  ObSEArray<Path*, 8> valid_inner_paths;
  UNUSED(stmt);
  if (OB_FAIL(append(exec_params, nl_params))) {
    LOG_WARN("failed to append nl params", K(ret));
  }
  // check inner path valid, and fill other informations
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_paths_.count(); ++i) {
    ObSEArray<ObRawExpr*, 32> range_exprs;
    ObSEArray<ObRawExpr*, 32> all_table_filters;
    ObSEArray<ObRawExpr*, 32> range_params;
    ObSEArray<ObRawExpr*, 32> all_params;
    ObSEArray<ObRawExpr*, 8> pushdown_params;
    ObSEArray<ObRawExpr*, 8> all_pushdown_params;
    Path *inner_path = helper.inner_paths_.at(i);
    if (OB_ISNULL(inner_path) || OB_ISNULL(inner_path->parent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null path", K(ret));
    }  else if (OB_FAIL(get_range_params(inner_path, range_exprs, all_table_filters))) {
      LOG_WARN("failed to get range_exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_params(range_exprs, range_params))) {
      LOG_WARN("failed to extract range params", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_params(all_table_filters, all_params))) {
      LOG_WARN("failed to extract range params", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::intersect(exec_params, range_params, pushdown_params))) {
      LOG_WARN("failed to get intersect params", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::intersect(exec_params, all_params, all_pushdown_params))) {
      LOG_WARN("failed to get intersect params", K(ret));
    } else if (!inner_path_info.force_inner_nl_ && pushdown_params.empty()) {
      //pushdown quals do not contribute query range
      LOG_TRACE("pushdown filters can not extend any query range");
    } else if (OB_FAIL(append(inner_path->pushdown_filters_, pushdown_quals))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(inner_path->nl_params_, nl_params))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (FALSE_IT(inner_path->is_inner_path_ = true)) {
      /*do nothing*/
    } else if (OB_FAIL(valid_inner_paths.push_back(inner_path))) {
      LOG_WARN("failed to push back inner paths", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret) && !valid_inner_paths.empty()) {
    ObSEArray<ObSEArray<Path*, 16>, 16> path_list;
    if (OB_FAIL(classify_paths_based_on_sharding(valid_inner_paths,
                                                 equal_sets,
                                                 path_list))) {
      LOG_WARN("failed to classify paths based on sharding", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); i++) {
        Path *temp_path = NULL;
        if (OB_FAIL(find_best_inner_nl_path(path_list.at(i), temp_path))) {
          LOG_WARN("failed to find best nl path", K(ret));
        } else if (OB_ISNULL(temp_path)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(inner_path_info.inner_paths_.push_back(temp_path))) {
          LOG_WARN("failed to push back inner paths", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}


int ObJoinOrder::generate_force_inner_path(const ObIArray<ObRawExpr *> &join_conditions,
                                          const ObRelIds join_relids,
                                          ObJoinOrder &right_tree,
                                          InnerPathInfo &inner_path_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> pushdown_quals;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  ObSEArray<ObRawExpr*, 4> param_pushdown_quals;
  ObSEArray<ObExecParamRawExpr*, 4> nl_params;
  LOG_TRACE("OPT:start to generate force inner path");
  if (OB_FAIL(extract_pushdown_quals(join_conditions,
                                     inner_path_info.force_inner_nl_,
                                     pushdown_quals))) {
    LOG_WARN("failed to extract pushdown quals", K(ret));
  } else if (OB_FAIL(extract_params_for_inner_path(join_relids,
                                                  nl_params,
                                                  subquery_exprs,
                                                  pushdown_quals,
                                                  param_pushdown_quals))) {
    LOG_WARN("failed to extract params for inner path", K(ret));
  } else if (OB_FAIL(inner_path_info.join_conditions_.assign(join_conditions))) {
    LOG_WARN("failed to assign join condition", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_tree.get_interesting_paths().count(); ++i) {
    Path *right_path = right_tree.get_interesting_paths().at(i);
    Path *inner_path = NULL;
    if (OB_ISNULL(right_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null path", K(ret));
    } else if (OB_FAIL(copy_path(*right_path, inner_path))) {
      LOG_WARN("failed to copy path", K(ret));
    } else if (OB_ISNULL(inner_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null path", K(ret));
    } else if (OB_FAIL(append(inner_path->pushdown_filters_, pushdown_quals))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(inner_path->filter_, param_pushdown_quals))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(inner_path->nl_params_, nl_params))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(inner_path->subquery_exprs_, subquery_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(inner_path_info.inner_paths_.push_back(inner_path))) {
      LOG_WARN("failed to push back path", K(ret));
    } else {
      inner_path->is_inner_path_ = true;
      inner_path->inner_row_count_ = right_tree.get_output_rows();
      inner_path->log_op_ = NULL;
    }
  }
  return ret;
}

int ObJoinOrder::copy_path(const Path& src_path, Path* &dst_path)
{
  int ret = OB_SUCCESS;
  dst_path = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null allocator", K(ret));
  } else if (src_path.is_join_path()) {
    const JoinPath &join_path = static_cast<const JoinPath&>(src_path);
    JoinPath *new_join_path = NULL;
    if (OB_ISNULL(new_join_path = static_cast<JoinPath*>(allocator_->alloc(sizeof(JoinPath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc a join path", K(ret));
    } else {
      new_join_path = new (new_join_path) JoinPath();
      if (OB_FAIL(new_join_path->assign(join_path, allocator_))) {
        LOG_WARN("failed to assign join path", K(ret));
      } else {
        dst_path = new_join_path;
      }
    }
  } else if (src_path.is_subquery_path()) {
    const SubQueryPath &subquery_path = static_cast<const SubQueryPath&>(src_path);
    SubQueryPath *new_subquery_path = NULL;
    if (OB_ISNULL(new_subquery_path = static_cast<SubQueryPath*>(allocator_->alloc(sizeof(SubQueryPath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate subquery path", K(ret));
    } else {
      new_subquery_path = new (new_subquery_path) SubQueryPath();
      if (OB_FAIL(new_subquery_path->assign(subquery_path, allocator_))) {
        LOG_WARN("failed to assign subquery path", K(ret));
      } else {
        dst_path = new_subquery_path;
      }
    }
  } else if (src_path.is_access_path()) {
    const AccessPath &access_path = static_cast<const AccessPath&>(src_path);
    AccessPath *new_access_path = NULL;
    if (OB_ISNULL(new_access_path = reinterpret_cast<AccessPath*>(allocator_->alloc(sizeof(AccessPath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an AccessPath", K(ret));
    } else {
      new_access_path = new(new_access_path) AccessPath(OB_INVALID_ID, OB_INVALID_ID, OB_INVALID_ID, NULL, UNORDERED);
      if (OB_FAIL(new_access_path->assign(access_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_access_path;
      }
    }
  } else if (src_path.is_function_table_path()) {
    const FunctionTablePath &func_path = static_cast<const FunctionTablePath&>(src_path);
    FunctionTablePath *new_func_path = NULL;
    if (OB_ISNULL(new_func_path = reinterpret_cast<FunctionTablePath*>(allocator_->alloc(sizeof(FunctionTablePath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an FunctionTablePath", K(ret));
    } else {
      new_func_path = new(new_func_path) FunctionTablePath();
      if (OB_FAIL(new_func_path->assign(func_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_func_path;
      }
    }
  } else if (src_path.is_json_table_path()) {
    const JsonTablePath &json_table_path = static_cast<const JsonTablePath&>(src_path);
    JsonTablePath *new_jt_path = NULL;
    if (OB_ISNULL(new_jt_path = static_cast<JsonTablePath*>(allocator_->alloc(sizeof(JsonTablePath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an JsonTablePath", K(ret));
    } else {
      new_jt_path = new(new_jt_path) JsonTablePath();
      if (OB_FAIL(new_jt_path->assign(json_table_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_jt_path;
      }
    }
  } else if (src_path.is_temp_table_path()) {
    const TempTablePath &temp_path = static_cast<const TempTablePath&>(src_path);
    TempTablePath *new_temp_table = NULL;
    if (OB_ISNULL(new_temp_table = reinterpret_cast<TempTablePath*>(allocator_->alloc(sizeof(TempTablePath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an TempTablePath", K(ret));
    } else {
      new_temp_table = new(new_temp_table) TempTablePath();
      if (OB_FAIL(new_temp_table->assign(temp_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_temp_table;
      }
    }
  } else if (src_path.is_cte_path()) {
    const CteTablePath &cte_path = static_cast<const CteTablePath&>(src_path);
    CteTablePath *new_cte_path = NULL;
    if (OB_ISNULL(new_cte_path = reinterpret_cast<CteTablePath*>(allocator_->alloc(sizeof(CteTablePath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an CteTablePath", K(ret));
    } else {
      new_cte_path = new(new_cte_path) CteTablePath();
      if (OB_FAIL(new_cte_path->assign(cte_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_cte_path;
      }
    }
  } else if (src_path.is_values_table_path()) {
    const ValuesTablePath &values_table_path = static_cast<const ValuesTablePath&>(src_path);
    ValuesTablePath *new_values_table_path = NULL;
    if (OB_ISNULL(new_values_table_path = reinterpret_cast<ValuesTablePath*>(allocator_->alloc(sizeof(ValuesTablePath))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate an ValuesTablePath", K(ret));
    } else {
      new_values_table_path = new(new_values_table_path) ValuesTablePath();
      if (OB_FAIL(new_values_table_path->assign(values_table_path, allocator_))) {
        LOG_WARN("failed to assign access path", K(ret));
      } else {
        dst_path = new_values_table_path;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected path type", K(ret));
  }
  return ret;
}

int ObJoinOrder::extract_pushdown_quals(const ObIArray<ObRawExpr *> &quals,
                                        const bool force_inner_nl,
                                        ObIArray<ObRawExpr *> &pushdown_quals)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr *qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(qual), K(ret));
    // can not push down expr with subquery
    } else if (qual->has_flag(CNT_ROWNUM)) {
      if (force_inner_nl && qual->has_flag(CNT_SUB_QUERY)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "join condition contains rownum and subquery");
      }
    } else if ((T_OP_NE == qual->get_expr_type() ||
                T_OP_NOT_IN == qual->get_expr_type()) &&
               !force_inner_nl) {
      //do nothing
    } else if (OB_FAIL(pushdown_quals.push_back(qual))) {
      LOG_WARN("failed to push back qual", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObJoinOrder::DeducedExprInfo::assign(const DeducedExprInfo &other)
{
  int ret = OB_SUCCESS;
  deduced_expr_ = other.deduced_expr_;
  deduced_from_expr_ = other.deduced_from_expr_;
  is_precise_ = other.is_precise_;
  if (OB_FAIL(const_param_constraints_.assign(other.const_param_constraints_))) {
    LOG_WARN("failed to assign pc constraint", K(ret));
  }
  return ret;
}

int ObJoinOrder::add_deduced_expr(ObRawExpr *deduced_expr,
                      ObRawExpr *deduce_from,
                      bool is_persistent)
{
  ObSEArray<ObPCConstParamInfo, 2> param_infos;
  return add_deduced_expr(deduced_expr, deduce_from, is_persistent, param_infos);
}

int ObJoinOrder::add_deduced_expr(ObRawExpr *deduced_expr,
                                  ObRawExpr *deduced_from,
                                  bool is_precise,
                                  ObIArray<ObPCConstParamInfo> &param_infos)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    DeducedExprInfo deduced_expr_info;
    deduced_expr_info.deduced_expr_ = deduced_expr;
    deduced_expr_info.deduced_from_expr_ = deduced_from;
    deduced_expr_info.is_precise_ = is_precise;
    if (OB_FAIL(append(deduced_expr_info.const_param_constraints_, param_infos))) {
      LOG_WARN("failed to assign pc constraint", K(ret));
    } else if (OB_FAIL(deduced_exprs_info_.push_back(deduced_expr_info))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}


int ObJoinOrder::get_generated_col_index_qual(const int64_t table_id,
                                              ObIArray<ObRawExpr *> &quals,
                                              PathHelper &helper)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(table_item = get_plan()->get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else {
    int64_t N = quals.count();
    deduced_exprs_info_.reset();
    bool is_persistent = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      ObRawExpr *qual = quals.at(i);
      ObRawExpr *new_prefix_qual = NULL;
      bool match_prefix_idx = false;
      ObSEArray<ObColumnRefRawExpr*, 4> prefix_cols;
      ObSEArray<ObRawExpr*, 4> simple_prefix_preds;
      bool match_common_gen_col_idx = false;
      ObSEArray<ObColumnRefRawExpr*, 4> common_gen_cols;
      ObSEArray<ObRawExpr*, 4> simple_gen_col_preds;
      ObRawExpr *new_qual = NULL;
      // deduce prefix index predicate
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(prefix_cols))) {
        LOG_WARN("get column exprs failed", K(ret));
      } else if (OB_FAIL(check_match_prefix_index(qual,
                                                  table_item,
                                                  prefix_cols,
                                                  simple_prefix_preds,
                                                  match_prefix_idx))) {
        LOG_WARN("check match prefix idx failed", K(ret));
      } else if (!match_prefix_idx || prefix_cols.empty()) {
        // do nothing
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < prefix_cols.count(); j++) {
          if (OB_FAIL(deduce_prefix_str_idx_expr(qual,
                                                table_item,
                                                prefix_cols.at(j),
                                                simple_prefix_preds,
                                                new_prefix_qual,
                                                helper))) {
            LOG_WARN("deduce prefix str idx expr failed", K(ret));
          } else if (OB_ISNULL(new_prefix_qual)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("new qual is null", K(ret));
          } else if (ObOptimizerUtil::find_equal_expr(quals, new_prefix_qual)) {
            /*do nothing*/
          } else if (OB_FAIL(quals.push_back(new_prefix_qual))) {
            LOG_WARN("push back failed", K(ret));
          }
          LOG_TRACE("deduce prefix index expr", KPC(new_prefix_qual), KPC(qual));
        }
      }
      // deduce common generated column index predicate
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(common_gen_cols))) {
        LOG_WARN("get column exprs failed", K(ret));
      } else if (OB_FAIL(check_match_common_gen_col_index(qual,
                                                          table_item,
                                                          common_gen_cols,
                                                          simple_gen_col_preds,
                                                          match_common_gen_col_idx))) {
        LOG_WARN("check match prefix idx failed", K(ret));
      } else if (!match_common_gen_col_idx || common_gen_cols.empty()) {
        // do nothing
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < common_gen_cols.count(); j++) {
          if (OB_FAIL(deduce_common_gen_col_index_expr(qual,
                                                      table_item,
                                                      common_gen_cols.at(j),
                                                      simple_gen_col_preds,
                                                      new_qual,
                                                      helper))) {
            LOG_WARN("deduce gen col index expr failed", K(ret));
          } else if (OB_ISNULL(new_qual)) {
            // do nothing
          } else if (ObOptimizerUtil::find_equal_expr(quals, new_qual)) {
            /*do nothing*/
          } else if (OB_FAIL(quals.push_back(new_qual))) {
            LOG_WARN("push back failed", K(ret));
          }
          LOG_TRACE("deduce gen col index expr", KPC(new_qual), KPC(qual));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::build_prefix_index_compare_expr(ObRawExpr &column_expr,
                                                   ObRawExpr *prefix_expr,
                                                   ObItemType type,
                                                   ObRawExpr &value_expr,
                                                   ObRawExpr *escape_expr,
                                                   ObRawExpr *&new_op_expr,
                                                   PathHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *substr_expr = NULL;
  if (T_OP_LIKE == type) {
    //build value substr expr
    ObOpRawExpr *like_expr = NULL;
    bool got_result = false;
    ObObj result;
    ObOptimizerContext *opt_ctx = NULL;
    ObRawExpr *prefix_len_expr = NULL;
    if (OB_ISNULL(get_plan()) ||
        OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
        OB_ISNULL(allocator_) || OB_ISNULL(prefix_expr) ||
        OB_ISNULL(prefix_len_expr = prefix_expr->get_param_expr(2))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get unexpected null", K(get_plan()), K(opt_ctx), K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_prefix_pattern_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                            get_plan()->get_optimizer_context().get_session_info(),
                                                            &value_expr,
                                                            prefix_len_expr,
                                                            escape_expr,
                                                            substr_expr))) {
      LOG_WARN("create substr expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_like_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                       get_plan()->get_optimizer_context().get_session_info(),
                                                       &column_expr,
                                                       substr_expr,
                                                       escape_expr,
                                                       like_expr))) {
      LOG_WARN("build like expr failed", K(ret));
    } else if (OB_FAIL(like_expr->extract_info())) {
      LOG_WARN("extract info failed", K(ret));
    } else if (OB_FAIL(like_expr->pull_relation_id())) {
      LOG_WARN("pullup rel ids failed", K(ret));
    } else {
      new_op_expr = like_expr;
    }
  } else {
    ObRawExpr *right_expr = NULL;
    if (T_OP_IN == type) {
      ObOpRawExpr *row_expr = NULL;
      if (OB_FAIL(get_plan()->get_optimizer_context().get_expr_factory().create_raw_expr(T_OP_ROW, row_expr))) {
        LOG_WARN("create to_type expr failed", K(ret));
      } else if (OB_ISNULL(row_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("to_type is null");
      } else {
        right_expr = row_expr;
        for (int64_t k = 0; OB_SUCC(ret) && k < value_expr.get_param_count(); ++k) {
          if (OB_FAIL(ObRawExprUtils::create_substr_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                         get_plan()->get_optimizer_context().get_session_info(),
                                                         value_expr.get_param_expr(k),
                                                         prefix_expr->get_param_expr(1),
                                                         prefix_expr->get_param_expr(2),
                                                         substr_expr))) {
            LOG_WARN("create substr expr failed", K(ret));
          } else if (OB_FAIL(row_expr->add_param_expr(substr_expr))) {
            LOG_WARN("set param expr failed", K(ret));
          }
        }
      }
    } else {
      if (OB_FAIL(ObRawExprUtils::create_substr_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                     get_plan()->get_optimizer_context().get_session_info(),
                                                     &value_expr,
                                                     prefix_expr->get_param_expr(1),
                                                     prefix_expr->get_param_expr(2),
                                                     substr_expr))) {
        LOG_WARN("create substr expr failed", K(ret));
      } else {
        right_expr = substr_expr;
      }
    }
    // change to const
    if (OB_SUCC(ret)) {
      ObObj result;
      ObConstRawExpr *c_expr = NULL;
      if (OB_FAIL(substr_expr->extract_info())) {
        LOG_WARN("extract info failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                                        get_plan()->get_optimizer_context().get_session_info(),
                                                        type,
                                                        new_op_expr,
                                                        &column_expr,
                                                        right_expr))) {
        LOG_WARN("failed to create double op expr", K(ret), K(type), K(column_expr), KPC(right_expr));
      } else if (OB_FAIL(new_op_expr->extract_info())) {
        LOG_WARN("extract info failed", K(ret));
      } else if (OB_FAIL(new_op_expr->pull_relation_id())) {
        LOG_WARN("pullup rel ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_match_to_type(ObRawExpr *to_type_expr, ObRawExpr *candi_expr, bool &is_same, ObExprEqualCheckContext &equal_ctx) {
  int ret = OB_SUCCESS;
  bool is_valid = false;
  is_same = false;
  ObRawExpr *to_type_child = NULL;
  //check expr type and param type of to_<type> expr
  if (OB_ISNULL(to_type_expr) || OB_ISNULL(candi_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is unexpected null", K(ret), K(to_type_expr), K(candi_expr));
  } else {
    ObItemType type = to_type_expr->get_expr_type();
    ObItemType candi_type = candi_expr->get_expr_type();
    if (T_FUN_SYS_TO_CHAR == type
        || T_FUN_SYS_TO_NCHAR == type
        || T_FUN_SYS_TO_NUMBER == type
        || T_FUN_SYS_TO_BINARY_FLOAT == type
        || T_FUN_SYS_TO_BINARY_DOUBLE == type
        || T_FUN_SYS_DATE == type
        || T_FUN_SYS_TO_BINARY_DOUBLE == type) {
      is_valid = true;
    } else if (T_FUN_SYS_TO_CHAR == candi_type
        || T_FUN_SYS_TO_NCHAR == candi_type
        || T_FUN_SYS_TO_NUMBER == candi_type
        || T_FUN_SYS_TO_BINARY_FLOAT == candi_type
        || T_FUN_SYS_TO_BINARY_DOUBLE == candi_type
        || T_FUN_SYS_DATE == candi_type
        || T_FUN_SYS_TO_BINARY_DOUBLE == candi_type) {
      std::swap(to_type_expr, candi_expr);
      is_valid = true;
    }
    if (is_valid && to_type_expr->get_param_count() > 1) {
      is_valid = false; //TODO: if the fmt param is same as session default fmt, to_date/to_char/to_timestamp/to_timestamp_tz is the same as cast expr
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_ISNULL(to_type_child = to_type_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null",K(ret));
    } else if (ObOptimizerUtil::is_lossless_type_conv(to_type_child->get_result_type(),to_type_expr->get_result_type())) {
      if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(to_type_child, to_type_child))) {
        LOG_WARN("fail to get real child without lossless cast", K(ret));
      } else if (OB_ISNULL(to_type_child) || OB_ISNULL(candi_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(to_type_child), K(candi_expr));
      } else {
        is_same = to_type_child->same_as(*candi_expr, &equal_ctx);
      }
    } else if (to_type_expr->get_result_type().get_type() == candi_expr->get_result_type().get_type()
               && T_FUN_SYS_CAST == candi_expr->get_expr_type()) {
      if (ob_is_string_tc(to_type_expr->get_result_type().get_type())) {
        if (to_type_expr->get_result_type().get_accuracy() == candi_expr->get_result_type().get_accuracy()
            && to_type_expr->get_result_type().get_collation_type() == candi_expr->get_result_type().get_collation_type()) {
          is_same = true;
        }
      } else if (to_type_expr->get_result_type().get_scale() == candi_expr->get_result_type().get_scale()
                 && to_type_expr->get_result_type().get_precision() == candi_expr->get_result_type().get_precision()) {
        is_same = true;
      }
      if (is_same) {
        is_same = to_type_child->same_as(*(candi_expr->get_param_expr(0)), &equal_ctx);
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_range_params(const Path *path,
                                  ObIArray<ObRawExpr*> &range_exprs,
                                  ObIArray<ObRawExpr*> &all_table_filters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (path->is_access_path()) {
    const AccessPath *access_path = static_cast<const AccessPath *>(path);
    if (OB_FAIL(append(range_exprs, access_path->est_cost_info_.pushdown_prefix_filters_))) {
      LOG_WARN("failed to append pushdown prefix filters", K(ret));
    } else if (OB_FAIL(append(all_table_filters, access_path->est_cost_info_.pushdown_prefix_filters_))) {
      LOG_WARN("failed to append pushdown prefix filters", K(ret));
    } else if (OB_FAIL(append(all_table_filters, access_path->est_cost_info_.postfix_filters_))) {
      LOG_WARN("failed to append pushdown prefix filters", K(ret));
    } else if (OB_FAIL(append(all_table_filters, access_path->est_cost_info_.table_filters_))) {
      LOG_WARN("failed to append pushdown prefix filters", K(ret));
    } else if (access_path->is_index_merge_path()) {
      const IndexMergePath *index_merge_path = static_cast<const IndexMergePath *>(access_path);
      ObSEArray<AccessPath*, 4> scan_paths;
      if (OB_FAIL(index_merge_path->get_all_scan_access_paths(scan_paths))) {
        LOG_WARN("failed to get all scan access paths", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_paths.count(); ++i) {
        if (OB_FAIL(get_range_params(scan_paths.at(i), range_exprs, all_table_filters))) {
          LOG_WARN("failed to get scan node range param", K(ret), KPC(scan_paths.at(i)));
        }
      }
    }
  } else if (path->is_subquery_path()) {
    const SubQueryPath *sub_path = static_cast<const SubQueryPath *>(path);
    if (OB_FAIL(ObOptimizerUtil::get_range_params(sub_path->root_, range_exprs, all_table_filters))) {
      LOG_WARN("failed to get range params", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::extract_real_join_keys(ObIArray<ObRawExpr *> &join_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_keys.count(); ++i) {
    ObRawExpr *&expr = join_keys.at(i);
    bool is_lossless = false;
    if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(expr, is_lossless))) {
      LOG_WARN("failed to check lossless column cast", K(ret));
    } else if (is_lossless) {
      expr = expr->get_param_expr(0);
    }
  }
  return ret;
}

int ObJoinOrder::find_best_inner_nl_path(const ObIArray<Path*> &inner_paths,
                                         Path *&best_nl_path)
{
  int ret = OB_SUCCESS;
  best_nl_path = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_paths.count(); ++i) {
    Path *nl_path = NULL;
    bool is_subset = false;
    bool need_replace = false;
    if (OB_ISNULL(nl_path = inner_paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right inner path is null", K(ret), K(nl_path));
    } else if (best_nl_path == NULL) {
      need_replace = true;
    } else if (nl_path->is_access_path()) {
      AccessPath *access_nl_path = static_cast<AccessPath*>(nl_path);
      AccessPath *best_access_nl_path = static_cast<AccessPath*>(best_nl_path);
      if (access_nl_path->est_cost_info_.is_unique_) {
        need_replace = (!best_access_nl_path->est_cost_info_.is_unique_) ||
                    (best_access_nl_path->get_cost() > access_nl_path->get_cost());
      } else if (best_access_nl_path->est_cost_info_.is_unique_) {
        need_replace = false;
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
        need_replace = true;
      } else {
        need_replace = best_access_nl_path->get_cost() > access_nl_path->get_cost();
      }
    } else {
      need_replace = best_nl_path->get_cost() > nl_path->get_cost();
    }
    if (OB_SUCC(ret) && need_replace) {
      best_nl_path = nl_path;
    }
  }
  return ret;
}

int ObJoinOrder::extract_naaj_join_conditions(const ObIArray<ObRawExpr*> &join_quals,
                                              const ObRelIds &left_tables,
                                              const ObRelIds &right_tables,
                                              ObIArray<ObRawExpr*> &equal_join_conditions,
                                              NullAwareAntiJoinInfo &naaj_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr *cur_expr = NULL;
  ObSEArray <ObRawExpr *, 4> join_quals_naaj;
  ObSEArray <ObRawExpr *, 4>  join_other_naaj;
  if (1 != join_quals.count()) {
    // null aware anti join can only process single join equal cond
  } else if (OB_ISNULL(join_quals.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(join_quals.at(0)), K(ret));
  } else if (T_OP_OR == join_quals.at(0)->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.at(0)->get_param_count(); ++i) {
      bool is_equal_cond = false;
      cur_expr = join_quals.at(0)->get_param_expr(i);
      if (OB_FAIL(check_is_join_equal_conditions(cur_expr, left_tables,
                                                 right_tables, is_equal_cond))) {
        LOG_WARN("failed to check equal cond", K(cur_expr), K(ret));
      } else if (is_equal_cond) {
        if (OB_FAIL(join_quals_naaj.push_back(cur_expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        if (cur_expr->get_relation_ids().is_subset(get_tables()) &&
            OB_FAIL(join_other_naaj.push_back(cur_expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && 1 == join_quals_naaj.count()) {
      const ObRawExpr *left_join_key = join_quals_naaj.at(0)->get_param_expr(0);
      const ObRawExpr *right_join_key = join_quals_naaj.at(0)->get_param_expr(1);
      if (left_join_key->get_relation_ids().is_subset(right_tables)) {
        left_join_key = join_quals_naaj.at(0)->get_param_expr(1);
        right_join_key = join_quals_naaj.at(0)->get_param_expr(0);
      }
      bool left_has_is_null = false;
      bool right_has_is_null = false;
      bool is_valid = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < join_other_naaj.count(); ++i) {
        //eliminate join_key is null
        cur_expr = join_other_naaj.at(i);
        if (T_OP_IS == cur_expr->get_expr_type()
            && ObNullType == cur_expr->get_param_expr(1)->get_result_type().get_type()) {
          if (left_join_key == cur_expr->get_param_expr(0)) {
            left_has_is_null = true;
          } else if (right_join_key == cur_expr->get_param_expr(0)) {
            right_has_is_null = true;
          } else {
            is_valid = false;
          }
        } else {
          is_valid = false;
        }
      }

      if (OB_FAIL(ret) || !is_valid) {
        // do nothing
      } else if (left_has_is_null && right_has_is_null) {
        // Both "is null" was eliminated
        // anti join na
        // e.g. (a = b) or (a is null) or (b is null)
        naaj_info.is_naaj_ = true;
        if (OB_FAIL(equal_join_conditions.push_back(join_quals_naaj.at(0)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (left_has_is_null || right_has_is_null) {
        // Only one "is null" was eliminated,
        // Other side should be not null
        // e.g.   ((a = b) or (a is null)) and (b is not null)
        //    <=> ((a = b) or (a is null) or (b is null)) and (b is not null)
        ObNotNullContext not_null_ctx(get_plan()->get_optimizer_context().get_exec_ctx(),
                                      &get_plan()->get_allocator(),
                                      get_plan()->get_stmt());
        ObArray<ObRawExpr *> constraints;
        if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_WHERE))) {
          LOG_WARN("failed to generate stmt context", K(ret));
        } else if (OB_FAIL(not_null_ctx.remove_filter(join_quals.at(0)))){
          LOG_WARN("failed to remove filter", K(ret));
        } else if (!left_has_is_null &&
                   OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, left_join_key,
                                                              naaj_info.left_side_not_null_, &constraints))) {
          LOG_WARN("failed to check is expr not null");
        } else if (!right_has_is_null &&
                   OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, right_join_key,
                                                              naaj_info.right_side_not_null_, &constraints))) {
          LOG_WARN("failed to check is expr not null");
        } else if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(naaj_info.expr_constraints_, constraints))) {
          LOG_WARN("append expr constraints failed", K(ret));
        } else if (naaj_info.left_side_not_null_ || naaj_info.right_side_not_null_) {
          naaj_info.is_naaj_ = true;
          if (OB_FAIL(equal_join_conditions.push_back(join_quals_naaj.at(0)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
    }
    LOG_TRACE("extract naaj info", K(naaj_info), K(equal_join_conditions));
  }
  return ret;
}


int ObJoinOrder::check_can_use_global_stat_instead(const uint64_t ref_table_id,
                                                   const ObTableSchema &table_schema,
                                                   ObIArray<int64_t> &all_used_parts,
                                                   ObIArray<ObTabletID> &all_used_tablets,
                                                   bool &can_use,
                                                   ObIArray<int64_t> &global_part_ids,
                                                   double &scale_ratio)
{
  int ret = OB_SUCCESS;
  bool is_global_stat_valid = false;
  int64_t global_part_id = -1;
  can_use = false;
  scale_ratio = 1.0;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
      OB_ISNULL(OPT_CTX.get_opt_stat_manager())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!table_schema.is_partitioned_table()) {
    //do nothing
  } else if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->check_opt_stat_validity(*(OPT_CTX.get_exec_ctx()),
                                                                             session_info->get_effective_tenant_id(),
                                                                             ref_table_id,
                                                                             global_part_id,
                                                                             is_global_stat_valid))) {
    LOG_WARN("failed to check stat version", K(ret));
  } else if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
    if (!is_global_stat_valid) {
      //do nothing
    } else if (OB_FAIL(global_part_ids.push_back(global_part_id))) {
       LOG_WARN("failed to push back global partition id", K(ret));
    } else {
      can_use = true;
      scale_ratio = ObOptSelectivity::revise_between_0_1(1.0 * all_used_parts.count() / table_schema.get_all_part_num());
    }
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    int64_t total_subpart_cnt = 0;
    bool is_opt_stat_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_opt_stat_valid && i < all_used_tablets.count(); ++i) {
      int64_t part_id = OB_INVALID_ID;
      int64_t subpart_id = OB_INVALID_ID;
      ObArray<int64_t> subpart_ids;
      if (OB_FAIL(table_schema.get_part_id_by_tablet(all_used_tablets.at(i), part_id, subpart_id))) {
        LOG_WARN("failed to get part id by tablet", K(ret), K(all_used_tablets.at(i)));
      } else if (!ObOptimizerUtil::find_item(global_part_ids, part_id)) {
        if (OB_FAIL(OPT_CTX.get_opt_stat_manager()->check_opt_stat_validity(*(OPT_CTX.get_exec_ctx()),
                                                                            session_info->get_effective_tenant_id(),
                                                                            ref_table_id,
                                                                            part_id,
                                                                            is_opt_stat_valid))) {
          LOG_WARN("failed to get stat version", K(ret));
        } else if (!is_opt_stat_valid) {
          //do nothing
        } else if (OB_FAIL(global_part_ids.push_back(part_id))) {
          LOG_WARN("failed to push back part id", K(ret));
        } else if (OB_FAIL(table_schema.get_subpart_ids(part_id, subpart_ids))) {
          LOG_WARN("failed to get subpart ids", K(ret));
        } else {
          total_subpart_cnt += subpart_ids.count();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_opt_stat_valid ||
          (total_subpart_cnt == table_schema.get_all_part_num() && is_global_stat_valid)) {
        global_part_ids.reset();
        if (!is_global_stat_valid) {
          //do nothing
        } else if (OB_FAIL(global_part_ids.push_back(global_part_id))) {
          LOG_WARN("failed to push back global partition id", K(ret));
        } else {
          can_use = true;
          scale_ratio = ObOptSelectivity::revise_between_0_1(1.0 * all_used_parts.count() / table_schema.get_all_part_num());
        }
      } else {
        can_use = true;
        scale_ratio = ObOptSelectivity::revise_between_0_1(1.0 * all_used_parts.count() / total_subpart_cnt);
      }
    }
  }
  LOG_TRACE("succeed to check can use global stat instead", K(all_used_parts), K(all_used_tablets),
                                                    K(can_use), K(global_part_ids), K(scale_ratio));
  return ret;
}

int ObJoinOrder::is_valid_range_expr_for_oracle_agent_table(const ObRawExpr *range_expr,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_NOT_NULL(range_expr) && range_expr->is_op_expr()) {
    const ObOpRawExpr *expr = static_cast<const ObOpRawExpr *>(range_expr);
    if (IS_BASIC_CMP_OP(expr->get_expr_type()) && T_OP_EQ == expr->get_expr_type()) {
      is_valid = true;
    } else if (T_OP_IS == expr->get_expr_type()
              || T_OP_IN  == expr->get_expr_type()) {
      is_valid = true;
    } else if (T_OP_AND == expr->get_expr_type()) {
      is_valid = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(is_valid_range_expr_for_oracle_agent_table(expr->get_param_expr(i),
                                                               is_valid))) {
          LOG_WARN("failed to check expr", K(ret));
        }
      }
    } else if (T_OP_OR == expr->get_expr_type()) {
      is_valid = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
        if (OB_NOT_NULL(expr->get_param_expr(i))
            && (T_OP_AND == expr->get_param_expr(i)->get_expr_type()
                || T_OP_OR == expr->get_param_expr(i)->get_expr_type()
                || expr->get_param_expr(i)->get_expr_type() != expr->get_param_expr(0)->get_expr_type())) {
          is_valid = false;
          LOG_TRACE("invalid expr type for oracle agent table range",
                    K(expr->get_param_expr(i)->get_expr_type()),
                    K(expr->get_param_expr(0)->get_expr_type()));
        } else if (OB_FAIL(is_valid_range_expr_for_oracle_agent_table(expr->get_param_expr(i),
                                                                      is_valid))) {
          LOG_WARN("failed to check expr", K(ret));
        }
      }
    } else {
      LOG_TRACE("invalid expr type for oracle agent table range", K(expr->get_expr_type()));
    }
  }
  return ret;
}

int ObJoinOrder::extract_valid_range_expr_for_oracle_agent_table(const ObIArray<ObRawExpr *> &filters,
                                                                 ObIArray<ObRawExpr *> &new_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    bool is_valid_expr = false;
    if (OB_FAIL(is_valid_range_expr_for_oracle_agent_table(filters.at(i), is_valid_expr))) {
      LOG_WARN("failed to check range expr", K(ret));
    } else if (!is_valid_expr) {
      // skip
    } else if (OB_FAIL(new_filters.push_back(filters.at(i)))) {
      LOG_WARN("failed to push back filter exprs", K(ret));
    }
  }
  return ret;
}

static uint64_t virtual_table_index_scan_white_list[2]{
    OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TID,
    OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TID};

bool ObJoinOrder::virtual_table_index_can_range_scan(uint64_t table_id) {
  bool bret = false;
  for (int i = 0; i < ARRAYSIZEOF(virtual_table_index_scan_white_list); i++) {
    if (table_id == virtual_table_index_scan_white_list[i]) {
      bret = true;
      break;
    }
  }
  return bret;
}

int ValuesTablePath::assign(const ValuesTablePath &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(Path::assign(other, allocator))) {
    LOG_WARN("failed to assgin", K(ret));
  } else {
    table_id_ = other.table_id_;
    table_def_ = other.table_def_;
  }
  return ret;
}

int ValuesTablePath::estimate_cost()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()) || OB_ISNULL(table_def_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = parent_->get_plan()->get_optimizer_context();
    int64_t row_count = table_def_->row_cnt_;
    cost_ = ObOptEstCost::cost_values_table(row_count, filter_, opt_ctx);
    op_cost_ = cost_;
  }
  return ret;
}

int ValuesTablePath::estimate_row_count()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || OB_ISNULL(parent_->get_plan()) || OB_ISNULL(table_def_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(parent_), K(ret));
  } else {
    ObLogPlan *plan = parent_->get_plan();
    double selectivity = 1.0;
    int64_t row_count = table_def_->row_cnt_;
    ObOptimizerContext &opt_ctx = plan->get_optimizer_context();
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(plan->get_basic_table_metas(),
                                                        plan->get_selectivity_ctx(),
                                                        filter_,
                                                        selectivity,
                                                        plan->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc filter selectivities", K(ret));
    } else {
      parent_->set_output_rows(row_count * selectivity);
      if (OB_FAIL(ObOptSelectivity::update_table_meta_info(plan->get_basic_table_metas(),
                                                           plan->get_update_table_metas(),
                                                           plan->get_selectivity_ctx(),
                                                           table_id_,
                                                           parent_->get_output_rows(),
                                                           parent_->get_restrict_infos(),
                                                           plan->get_predicate_selectivities()))) {
        LOG_WARN("failed to update table meta info", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::generate_values_table_paths()
{
  int ret = OB_SUCCESS;
  ValuesTablePath *values_path = NULL;
  const ObDMLStmt *stmt = NULL;
  TableItem *table_item = NULL;
  ObValuesTableDef *values_table = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", KP(get_plan()), KP(stmt), KP(allocator_), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_)) ||
             OB_UNLIKELY(!table_item->is_values_table()) ||
             OB_ISNULL(values_table = table_item->values_table_def_) ||
             OB_UNLIKELY(values_table->column_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_id_), KPC(stmt), K(ret));
  } else if (OB_ISNULL(values_path = reinterpret_cast<ValuesTablePath*>(
                       allocator_->alloc(sizeof(ValuesTablePath))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate values path", K(ret));
  } else {
    values_path = new(values_path) ValuesTablePath();
    values_path->table_id_ = table_id_;
    values_path->parent_ = this;
    values_path->table_def_ = values_table;
    ObSEArray<ObExecParamRawExpr *, 4> nl_params;
    values_path->strong_sharding_ = get_plan()->get_optimizer_context().get_match_all_sharding();
    if (OB_FAIL(values_path->set_parallel_and_server_info_for_match_all())) {
      LOG_WARN("failed set parallel and server info for match all", K(ret));
    } else if (OB_FAIL(append(values_path->filter_, get_restrict_infos()))) {
      LOG_WARN("failed to append filter", K(ret));
    } else if (OB_FAIL(get_plan()->get_basic_table_metas().add_values_table_meta_info(stmt,
                       table_id_, get_plan()->get_selectivity_ctx(), values_table))) {
      LOG_WARN("failed to add values table meta info", K(ret));
    } else if (OB_FAIL(ObOptEstCost::estimate_width_for_table(get_plan()->get_basic_table_metas(),
                                                              get_plan()->get_selectivity_ctx(),
                                                              stmt->get_column_items(),
                                                              table_id_,
                                                              output_row_size_))) {
      LOG_WARN("estimate width of row failed", K(table_id_), K(ret));
    } else if (OB_FAIL(values_path->estimate_row_count())) {
      LOG_WARN("failed to estimate row count", K(ret));
    } else if (OB_FAIL(values_path->estimate_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else if (OB_FAIL(param_values_table_expr(values_table->access_exprs_,
                                               nl_params,
                                               values_path->subquery_exprs_))) {
      LOG_WARN("failed to extract param for values table expr", K(ret));
    } else if (OB_FAIL(values_path->nl_params_.assign(nl_params))) {
      LOG_WARN("failed to assign nl params", K(ret));
    } else if (OB_FAIL(values_path->compute_pipeline_info())) {
      LOG_WARN("failed to compute pipelined path", K(ret));
    } else if (OB_FAIL(add_path(values_path))) {
      LOG_WARN("failed to add path", K(ret));
    } else { /*do nothing*/ }
  }
  LOG_TRACE("after allocate values path", K(output_row_size_), K(output_rows_),
            K(values_path->filter_), K(values_path->subquery_exprs_));
  return ret;
}

int ObJoinOrder::param_values_table_expr(ObIArray<ObRawExpr*> &values_vector,
                                         ObIArray<ObExecParamRawExpr *> &nl_params,
                                         ObIArray<ObRawExpr*> &subquery_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObLogPlan *plan = get_plan();
  for (int64_t i = 0; OB_SUCC(ret) && i < values_vector.count(); ++i) {
    ObSEArray<ObRawExpr *, 1> old_values_exprs;
    ObSEArray<ObRawExpr *, 1> new_values_exprs;
    ObSEArray<ObExecParamRawExpr *, 4> tmp_nl_params;
    if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(stmt = plan->get_stmt())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(plan), K(ret));
    } else if (OB_FAIL(old_values_exprs.push_back(values_vector.at(i)))) {
      LOG_WARN("failed to push back function table expr", K(ret));
    } else if (OB_FAIL(extract_params_for_inner_path(values_vector.at(i)->get_relation_ids(),
                                                     tmp_nl_params,
                                                     subquery_exprs,
                                                     old_values_exprs,
                                                     new_values_exprs))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (OB_UNLIKELY(new_values_exprs.count() != 1) || OB_ISNULL(new_values_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new values table expr is invalid", K(ret), K(new_values_exprs));
    } else if (OB_FAIL(append(nl_params, tmp_nl_params))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      values_vector.at(i) = new_values_exprs.at(0);
    }
  }
  return ret;
}

int ObJoinOrder::add_valid_vec_index_ids(const ObDMLStmt &stmt,
                                        ObSqlSchemaGuard *schema_guard,
                                        const uint64_t table_id,
                                        const uint64_t ref_table_id,
                                        const bool has_aggr,
                                        uint64_t *index_tid_array,
                                        int64_t &size)
{
  int ret = OB_SUCCESS;
  uint64_t vec_index_tid = OB_INVALID_ID;
  bool vector_index_match = false;
  ObRawExpr *vector_expr = NULL;
  if (!stmt.has_vec_approx()
      || OB_ISNULL(vector_expr = stmt.get_first_vector_expr())) {
    // do nothing, not vec index
  } else if (OB_FAIL(get_vector_index_tid_from_expr(schema_guard, vector_expr, table_id, ref_table_id, has_aggr, vector_index_match, vec_index_tid))) {
      LOG_WARN("failed to get vector index tid", K(ret));
  } else if ((vec_index_tid != OB_INVALID_ID)) {
    index_tid_array[size++] = vec_index_tid;
  }
  return ret;
}

int ObJoinOrder::get_vector_index_tid_from_expr(ObSqlSchemaGuard *schema_guard,
                                                ObRawExpr *vector_expr,
                                                const uint64_t table_id,
                                                const uint64_t ref_table_id,
                                                const bool has_aggr,
                                                bool &vector_index_match,
                                                uint64_t& vec_index_tid)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  const ObTableSchema *table_schema = NULL;
  uint64_t inv_idx_tid = OB_INVALID_ID;
  vector_index_match = false;
  ObIndexType index_type = INDEX_TYPE_MAX;
  if (OB_ISNULL(vector_expr) || OB_ISNULL(schema_guard) ||
      OB_ISNULL(session_info = OPT_CTX.get_session_info()) ||
      OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get main table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    uint64_t vec_col_id = OB_INVALID_ID;
    bool column_exist = false;
    for (int i = 0; i < vector_expr->get_param_count() && OB_SUCC(ret) && !vector_index_match; ++i) {
      const ObRawExpr *tmp_expr = vector_expr->get_param_expr(i);
      const ObColumnSchemaV2 *tmp_index_col = nullptr;
      if (OB_NOT_NULL(tmp_expr) && tmp_expr->is_column_ref_expr()) {
        column_exist = true;
        const ObColumnRefRawExpr *col_ref = ObRawExprUtils::get_column_ref_expr_recursively(tmp_expr);
        if (col_ref->get_table_id() == table_id
            && OB_NOT_NULL(tmp_index_col = table_schema->get_column_schema(col_ref->get_column_id()))) {
          if (OB_FAIL(ObVectorIndexUtil::check_column_has_vector_index(*table_schema,
                                                                       *(schema_guard->get_schema_guard()),
                                                                       tmp_index_col->get_column_id(),
                                                                       vector_index_match,
                                                                       index_type))) {
            LOG_WARN("failed to check column has vector index", K(ret), K(tmp_index_col->get_column_id()), K(vector_index_match));
          } else if (vector_index_match) {
            vec_col_id = tmp_index_col->get_column_id();
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "should be vector column with vector index");
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!column_exist) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "should be vector column with vector index");
    } else if (vector_index_match && has_aggr) {
      vector_index_match = false;
    } else if (!vector_index_match) {
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid(schema_guard->get_schema_guard(),
                                                               *table_schema,
                                                               index_type,
                                                               vec_col_id,
                                                               inv_idx_tid))) {
      LOG_WARN("fail to get spec vector delta buffer table id", K(ret), K(vec_col_id), KPC(table_schema));
    } else if (inv_idx_tid == OB_INVALID_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("can not find vector index for spec col id", K(ref_table_id), K(vec_col_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "should be vector column with vector index");
    } else {
      vec_index_tid = inv_idx_tid;
    }
  }
  return ret;
}

int ObJoinOrder::get_matched_inv_index_tid(ObMatchFunRawExpr *match_expr,
                                           uint64_t ref_table_id,
                                           uint64_t &inv_idx_tid)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
  if (OB_ISNULL(match_expr) || OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) ||
      OB_ISNULL(session_info = OPT_CTX.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get main table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_schema->get_simple_index_infos(index_infos))) {
    LOG_WARN("failed to get index infos", K(ret));
  } else {
    ColumnReferenceSet column_set;
    ObIArray<ObRawExpr*> &column_list = match_expr->get_match_columns();
    bool found_matched_index = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_list.count(); ++i) {
      ObColumnRefRawExpr *col_ref = nullptr;
      if (OB_UNLIKELY(OB_ISNULL(column_list.at(i)) || !column_list.at(i)->is_column_ref_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match against column");
      } else if (FALSE_IT(col_ref = static_cast<ObColumnRefRawExpr*>(column_list.at(i)))) {
      } else if (OB_FAIL(column_set.add_member(col_ref->get_column_id()))) {
        LOG_WARN("add to column set failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count() && !found_matched_index; ++i) {
      const ObTableSchema *inv_idx_schema = nullptr;
      const ObAuxTableMetaInfo &index_info = index_infos.at(i);
      if (!share::schema::is_fts_index_aux(index_info.index_type_)) {
        // skip
      } else if (OB_FAIL(schema_guard->get_table_schema(index_info.table_id_, inv_idx_schema))) {
        LOG_WARN("failed to get index schema", K(ret));
      } else if (OB_ISNULL(inv_idx_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index schema", K(ret), KPC(inv_idx_schema));
      } else if (OB_FAIL(ObTransformUtils::check_fulltext_index_match_column(column_set,
                                                                             table_schema,
                                                                             inv_idx_schema,
                                                                             found_matched_index))) {
        LOG_WARN("failed to check fulltext index match column", K(ret));
      } else if (found_matched_index && inv_idx_schema->can_read_index() && inv_idx_schema->is_index_visible()) {
        inv_idx_tid = index_info.table_id_;
      } else {
        found_matched_index = false;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID == inv_idx_tid) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("all fulltext index not found", K(ret));
    }
  }
  return ret;
}

int ObJoinOrder::extract_scan_match_expr_candidates(const ObIArray<ObRawExpr *> &filters,
                                                    ObIArray<ObMatchFunRawExpr *> &scan_match_exprs,
                                                    ObIArray<ObRawExpr *> &scan_match_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObRawExpr *filter = filters.at(i);
    if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to filter expr", K(ret), K(i), KPC(filter));
    } else if (filter->get_expr_type() == T_OP_BOOL && filter->has_flag(CNT_MATCH_EXPR)) {
      ObRawExpr *param_expr = filter->get_param_expr(0);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param expr for bool op", K(ret));
      } else if (param_expr->has_flag(IS_MATCH_EXPR)) {
        if (OB_FAIL(scan_match_exprs.push_back(static_cast<ObMatchFunRawExpr*>(param_expr)))) {
          LOG_WARN("failed to append match expr to array", K(ret));
        } else if (OB_FAIL(scan_match_filters.push_back(filter))) {
          LOG_WARN("failed to append match filter to array", K(ret));
        }
      }
    }
  }
  return ret;
}

// classify index scan and functional lookup match exprs
int ObJoinOrder::process_index_for_match_expr(const uint64_t table_id,
                                              const uint64_t ref_table_id,
                                              const uint64_t index_id,
                                              PathHelper &helper,
                                              AccessPath &access_path)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> all_match_exprs;
  ObMatchFunRawExpr *match_expr_for_index_scan = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *index_schema = nullptr;
  if (OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(get_plan()->get_stmt()->get_match_expr_on_table(table_id, all_match_exprs))) {
    LOG_WARN("failed to get match exprs by table id", K(ret), K(table_id));
  } else if (all_match_exprs.empty()) {
    // do nothing
  } else if (OB_FALSE_IT(access_path.domain_idx_info_.set_domain_idx_type(DomainIndexType::FTS_INDEX))) {
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index table schema", K(ret), K(index_id));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to index schema", K(ret));
  } else if (index_schema->is_fts_index()) {
    ObSEArray<ObMatchFunRawExpr *, 4> scan_match_exprs;
    ObSEArray<ObRawExpr *, 4> scan_match_filters;
    const MatchExprInfo *match_expr_info = NULL;
    int64_t idx = -1;
    if (OB_FAIL(extract_scan_match_expr_candidates(helper.filters_,
                                                   scan_match_exprs,
                                                   scan_match_filters))) {
      LOG_WARN("failed to extract scan match expr", K(ret));
    } else if (OB_FAIL(find_least_selective_expr_on_index(scan_match_exprs,
                                                         helper.match_expr_infos_,
                                                         index_id,
                                                         match_expr_info))) {
      LOG_WARN("failed to find most selective expr on index", K(ret));
    } else if (OB_ISNULL(match_expr_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptimizerUtil::find_item(scan_match_exprs, match_expr_info->match_expr_, &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected match expr", K(ret));
    } else if (OB_UNLIKELY(idx < 0 || idx >= scan_match_filters.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected idx", K(ret), K(idx), K(scan_match_filters.count()));
    } else if (OB_FAIL(access_path.domain_idx_info_.index_scan_exprs_.push_back(match_expr_info->match_expr_))) {
      LOG_WARN("failed to append match expr", K(ret));
    } else if (OB_FAIL(access_path.domain_idx_info_.index_scan_filters_.push_back(scan_match_filters.at(idx)))) {
      LOG_WARN("failed to append scan match filter expr", K(ret));
    } else if (OB_FAIL(access_path.domain_idx_info_.index_scan_index_ids_.push_back(match_expr_info->inv_idx_id_))) {
      LOG_WARN("failed to append inverted index table id", K(ret));
    } else {
      match_expr_for_index_scan = match_expr_info->match_expr_;
    }
  }

  if (OB_FAIL(ret) || helper.is_index_merge_) {
    // for index merge, the func lookup exprs are collected only on root path
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_match_exprs.count(); ++i) {
      ObMatchFunRawExpr *curr_expr = static_cast<ObMatchFunRawExpr *>(all_match_exprs.at(i));
      const MatchExprInfo *match_expr_info = NULL;
      if (OB_ISNULL(curr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ");
      } else if (curr_expr == match_expr_for_index_scan) {
        // skip
      } else if (OB_FAIL(find_match_expr_info(helper.match_expr_infos_, curr_expr, match_expr_info))) {
        LOG_WARN("failed to find match expr info", K(ret), KPC(curr_expr));
      } else if (OB_ISNULL(match_expr_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null match expr info", K(ret));
      } else if (OB_FAIL(access_path.domain_idx_info_.func_lookup_exprs_.push_back(curr_expr))) {
        LOG_WARN("failed to append func lookup exprs", K(ret), KPC(curr_expr));
      } else if (OB_FAIL(access_path.domain_idx_info_.func_lookup_index_ids_.push_back(match_expr_info->inv_idx_id_))) {
        LOG_WARN("failed to append func lookup index id", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::init_basic_text_retrieval_info(uint64_t table_id,
                                                uint64_t ref_table_id,
                                                PathHelper &helper)
{
  int ret = OB_SUCCESS;
  helper.match_expr_infos_.reuse();
  ObSEArray<ObRawExpr*, 4> match_exprs;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSEArray<ObConstRawExpr*, 4> query_tokens;
  ObTablePartitionInfo *partition_info;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(schema_guard = OPT_CTX.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan or stmt", K(ret), KP(get_plan()), KP(get_plan()->get_stmt()));
  } else if (OB_FAIL(get_plan()->get_stmt()->get_match_expr_on_table(table_id, match_exprs))) {
    LOG_WARN("failed to get match exprs", K(ret), K(table_id));
  } else if (match_exprs.empty()) {
    //do nothing
  } else if (OB_FAIL(compute_table_location(table_id, ref_table_id, false, partition_info))) {
    LOG_WARN("failed to compute table location", K(ret));
  } else if (OB_ISNULL(partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null partition info", K(ret));
  } else {
    // generate selectivity info for each match against expr
    for (int64_t i = 0; OB_SUCC(ret) && i < match_exprs.count(); ++i) {
      ObMatchFunRawExpr *match_expr = NULL;
      uint64_t index_id = OB_INVALID_ID;
      const ObTableSchema *index_schema = NULL;
      MatchExprInfo match_expr_info;
      ObSEArray<ColumnItem, 4> range_columns;
      if (OB_ISNULL(match_exprs.at(i)) || OB_UNLIKELY(!match_exprs.at(i)->is_match_against_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null match expr", K(ret));
      } else if (OB_FALSE_IT(match_expr = static_cast<ObMatchFunRawExpr *>(match_exprs.at(i)))) {
      } else if (OB_FAIL(get_matched_inv_index_tid(match_expr, ref_table_id, index_id))) {
        LOG_WARN("failed to get matched inverted index table id", K(ret), KPC(match_expr));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
        LOG_WARN("failed to get index schema", K(ret), K(index_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index schema", K(ret), K(index_id));
      } else if (OB_FAIL(get_plan()->get_index_column_items(OPT_CTX.get_expr_factory(),
                                                            table_id,
                                                            *index_schema,
                                                            range_columns))) {
        LOG_WARN("failed to generate rowkey column items", K(ret));
      } else if (OB_FAIL(get_query_tokens(match_expr, index_schema, query_tokens))) {
        LOG_WARN("failed to get query tokens", K(ret));
      } else if (OB_FAIL(get_range_of_query_tokens(query_tokens,
                                                   *index_schema,
                                                   range_columns,
                                                   match_expr_info.query_range_))) {
        LOG_WARN("failed to get range of query tokens", K(ret));
      } else if (OB_FAIL(estimate_fts_index_scan(table_id,
                                                 ref_table_id,
                                                 index_id,
                                                 partition_info,
                                                 index_schema,
                                                 match_expr_info.query_range_,
                                                 match_expr_info.query_range_row_count_,
                                                 match_expr_info.selectivity_))) {
        LOG_WARN("failed to estimate fts index scan", K(ret));
      } else if (OB_FALSE_IT(match_expr_info.match_expr_ = match_expr)) {
      } else if (OB_FALSE_IT(match_expr_info.inv_idx_id_ = index_id)) {
      } else if (OB_FAIL(helper.match_expr_infos_.push_back(match_expr_info))) {
        LOG_WARN("failed to push back match expr info", K(ret));
        // add selectivity infos of match against exprs to LogPlan
      } else if (OB_FAIL(get_plan()->get_predicate_selectivities().
                         push_back(ObExprSelPair(match_expr, match_expr_info.selectivity_)))) {
        LOG_WARN("failed to push back predicate selectivities", K(ret));
      }
    }
    LOG_TRACE("OPT: selectivity infos of match exprs", K(helper.match_expr_infos_));
  }
  return ret;
}

int ObJoinOrder::get_query_tokens_by_boolean_mode(ObMatchFunRawExpr *match_expr,
                                                  const ObObj &result,
                                                  ObIArray<ObConstRawExpr*> &query_tokens)
{
  int ret = OB_SUCCESS;
  const ObString &search_text_string = result.get_string();
  const ObCollationType &cs_type = match_expr->get_search_key()->get_collation_type();
  ObString str_dest;
  ObCharset::tolower(cs_type, search_text_string, str_dest, *allocator_);
  void *buf = nullptr;
  FtsParserResult *fts_parser;
  if (OB_ISNULL(buf = allocator_->alloc(sizeof(FtsParserResult)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
  } else {
    fts_parser = static_cast<FtsParserResult *>(buf);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = allocator_->alloc(str_dest.length() + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
  } else {
    MEMSET(buf, 0, str_dest.length() + 1);
    MEMCPY(buf, str_dest.ptr(), str_dest.length());
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(fts_parse_docment(static_cast<char *>(buf), allocator_, fts_parser))) {
  } else if (FTS_OK != fts_parser->ret_) {
    if (FTS_ERROR_MEMORY == fts_parser->ret_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (FTS_ERROR_SYNTAX == fts_parser->ret_) {
      ret = OB_ERR_PARSER_SYNTAX;
    } else if (FTS_ERROR_OTHER == fts_parser->ret_) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to parse query text", K(ret), K(fts_parser->err_info_.str_));
  } else if (OB_ISNULL(fts_parser->root_)) {
    // do nothing
  } else {
    FtsNode *node = fts_parser->root_;
    ObFtsEvalNode *parant_node =nullptr;
    hash::ObHashMap<ObString, int32_t> tokens_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    ObArray<ObString> query_token;
    if (OB_FAIL(tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_node_create(parant_node, node, *allocator_, query_token, tokens_map))) {
      LOG_WARN("failed to get query tokens", K(ret));
    } else {
      for (hash::ObHashMap<ObString, int32_t>::const_iterator iter = tokens_map.begin();
          OB_SUCC(ret) && iter != tokens_map.end();
          ++iter) {
        const ObString &token = iter->first;
        ObString token_string;
        ObConstRawExpr *token_expr = NULL;
        if (OB_FAIL(ob_write_string(*allocator_, token, token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*OPT_CTX.get_exec_ctx()->get_expr_factory(),
                                                                   ObVarcharType,
                                                                   token_string,
                                                                   cs_type,
                                                                   token_expr))) {
          LOG_WARN("failed to build const string expr", K(ret));
        } else if (OB_FAIL(query_tokens.push_back(token_expr))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
      allocator_->free(buf);
      parant_node->release();
    }
  }
  return ret;
}

int ObJoinOrder::get_query_tokens(ObMatchFunRawExpr *match_expr,
                                  const ObTableSchema *index_schema,
                                  ObIArray<ObConstRawExpr*> &query_tokens)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool got_result = false;
  if (OB_ISNULL(allocator_) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(allocator_), KP(index_schema));
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(OPT_CTX.get_exec_ctx(),
                                                              match_expr->get_search_key(),
                                                              result,
                                                              got_result,
                                                              *allocator_))) {
    LOG_WARN("fail to calc prefix pattern expr", K(ret));
  } else if (!got_result || result.is_null() || (is_oracle_mode() && result.is_null_oracle())) {
    // do nothing
  } else if (BOOLEAN_MODE == match_expr->get_mode_flag()) {
    if (OB_FAIL(get_query_tokens_by_boolean_mode(match_expr, result, query_tokens))) {
      LOG_WARN("failed to get query tokens", K(ret));
    }
  } else {
    const ObString &search_text_string = result.get_string();
    const ObString &parser_name = index_schema->get_parser_name_str();
    const ObString &parser_properties = index_schema->get_parser_property_str();
    const ObCollationType &cs_type = match_expr->get_search_key()->get_collation_type();
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTWord, 16> tokens;
    hash::ObHashMap<ObFTWord, int64_t> token_map;
    int64_t doc_length = 0;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (search_text_string.length() == 0) {
      // do nothing
    } else if (OB_FAIL(tokenize_helper.init(allocator_, parser_name, parser_properties))) {
      LOG_WARN("failed to init tokenize helper", K(ret));
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(tokenize_helper.segment(
        cs_type, search_text_string.ptr(), search_text_string.length(), doc_length, token_map))) {
      LOG_WARN("failed to segment");
    } else {
      for (hash::ObHashMap<ObFTWord, int64_t>::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTWord &token = iter->first;
        ObString token_string;
        ObConstRawExpr *token_expr = NULL;
        if (OB_FAIL(ob_write_string(*allocator_, token.get_word(), token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*OPT_CTX.get_exec_ctx()->get_expr_factory(),
                                                                  ObVarcharType,
                                                                  token_string,
                                                                  cs_type,
                                                                  token_expr))) {
          LOG_WARN("failed to build const string expr", K(ret));
        } else if (OB_FAIL(query_tokens.push_back(token_expr))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_range_of_query_tokens(ObIArray<ObConstRawExpr*> &query_tokens,
                                           const ObTableSchema &index_schema,
                                           ObIArray<ColumnItem> &range_columns,
                                           ObQueryRangeProvider *&query_range)
{
  // jinmao TODO: 改成直接构造 query range，不要生成 IN 表达式间接去抽
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *word_col = NULL;
  ObOpRawExpr *in_expr = NULL;
  ObOpRawExpr *in_list_expr = NULL;
  ObSEArray<ObRawExpr*,2> tmp_range_exprs;
  const ParamStore *params = NULL;
  // find word segment column on fts index
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(word_col) && i < range_columns.count(); i++) {
    const ObColumnSchemaV2 *col_schema = index_schema.get_column_schema(range_columns.at(i).column_id_);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(col_schema), K(ret));
    } else if (col_schema->is_word_segment_column()) {
      word_col = range_columns.at(i).expr_;
    }
  }

  // construct in expr to integrate all tokens
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(word_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get word segment column", K(ret));
  } else if (!query_tokens.empty()) {
    if (OB_FAIL(OPT_CTX.get_exec_ctx()->get_expr_factory()->create_raw_expr(T_OP_ROW, in_list_expr))) {
      LOG_WARN("create to_type expr failed", K(ret));
    } else if (OB_FAIL(OPT_CTX.get_exec_ctx()->get_expr_factory()->create_raw_expr(T_OP_IN, in_expr))) {
      LOG_WARN("create to_type expr failed", K(ret));
    } else if (OB_ISNULL(in_list_expr) || OB_ISNULL(in_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(in_list_expr), K(in_expr), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); i++) {
        if (OB_FAIL(in_list_expr->add_param_expr(query_tokens.at(i)))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(in_expr->set_param_exprs(word_col, in_list_expr))) {
        LOG_WARN("failed to set param exprs", K(ret));
      } else if (OB_FAIL(in_expr->formalize(OPT_CTX.get_exec_ctx()->get_my_session()))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(tmp_range_exprs.push_back(in_expr))) {
        LOG_WARN("failed to push back range expr", K(ret));
      }
    }
  } else {
    // build an always false expr for empty query tokens
    ObRawExpr *eq_expr = NULL;
    ObConstRawExpr *empty_string_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*OPT_CTX.get_exec_ctx()->get_expr_factory(),
                                                        ObVarcharType,
                                                        ObString(),
                                                        word_col->get_collation_type(),
                                                        empty_string_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*OPT_CTX.get_exec_ctx()->get_expr_factory(),
                                                                  T_OP_EQ,
                                                                  word_col,
                                                                  empty_string_expr,
                                                                  eq_expr))) {
      LOG_WARN("failed to build common binary op expr", K(ret));
    } else if (OB_FAIL(eq_expr->formalize(OPT_CTX.get_exec_ctx()->get_my_session()))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(tmp_range_exprs.push_back(eq_expr))) {
      LOG_WARN("failed to push back range expr", K(ret));
    }
  }

  // generate query range
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(params = OPT_CTX.get_params())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(params), K(ret));
  } else if (OPT_CTX.enable_new_query_range()) {
    void *ptr = allocator_->alloc(sizeof(ObPreRangeGraph));
    ObPreRangeGraph *pre_range_graph = NULL;
     if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pre range graph", K(ret));
    } else {
      pre_range_graph = new(ptr)ObPreRangeGraph(*allocator_);
      if (OB_FAIL(pre_range_graph->preliminary_extract_query_range(range_columns, tmp_range_exprs,
                                                                   OPT_CTX.get_exec_ctx(),
                                                                   nullptr,
                                                                   params))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = pre_range_graph;
      // reset range exprs which should be invisible after query range extraction
      pre_range_graph->reset_range_exprs();
    } else {
      if (NULL != pre_range_graph) {
        pre_range_graph->~ObPreRangeGraph();
        pre_range_graph = NULL;
      }
    }
  } else {
    void *tmp_ptr = allocator_->alloc(sizeof(ObQueryRange));
    ObQueryRange *tmp_qr = NULL;
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for query range", K(ret));
    } else {
      tmp_qr = new(tmp_ptr)ObQueryRange(*allocator_);
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(OPT_CTX.get_exec_ctx()->get_my_session());
      if (OB_FAIL(tmp_qr->preliminary_extract_query_range(range_columns, tmp_range_exprs,
                                                          dtc_params, OPT_CTX.get_exec_ctx(),
                                                          OPT_CTX.get_query_ctx(),
                                                          NULL, params))) {
        LOG_WARN("failed to preliminary extract query range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      query_range = tmp_qr;
      // reset range exprs which should be invisible after query range extraction
      tmp_qr->reset_range_exprs();
    } else {
      if (NULL != tmp_qr) {
        tmp_qr->~ObQueryRange();
        tmp_qr = NULL;
      }
    }
  }
  return ret;
}

int ObJoinOrder::estimate_fts_index_scan(uint64_t table_id,
                                         uint64_t ref_table_id,
                                         uint64_t index_id,
                                         const ObTablePartitionInfo *partition_info,
                                         const ObTableSchema *index_schema,
                                         ObQueryRangeProvider *query_range,
                                         int64_t &query_range_row_count,
                                         double &selectivity)
{
  int ret = OB_SUCCESS;
  ObTableMetaInfo table_meta_range(index_id);
  const ObSQLSessionInfo *session = OPT_CTX.get_session_info();
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  ObQueryRangeArray range_array;
  ObRangesArray ranges;
  bool dummy_all_single_value_ranges = true;
  if (OB_ISNULL(index_schema) || OB_ISNULL(query_range) || OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
      OB_UNLIKELY(index_schema->is_global_index_table()) || OB_ISNULL(partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_schema), K(query_range), K(ret));
  } else if (OB_FAIL(query_range->get_tablet_ranges(OPT_CTX.get_allocator(),
                                                    *OPT_CTX.get_exec_ctx(),
                                                    range_array,
                                                    dummy_all_single_value_ranges,
                                                    dtc_params))) {
    LOG_WARN("failed to get tablet ranges", K(ret));
  } else {
    for(int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); ++i) {
      if (OB_ISNULL(range_array.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range is null", K(ret));
      } else if (OB_FAIL(ranges.push_back(*range_array.at(i)))) {
        LOG_WARN("failed to add range", K(ret));
      }
    }
    SMART_VARS_3((ObTablePartitionInfo, index_part_info),
                 (ObPhysicalPlanCtx, tmp_plan_ctx, OPT_CTX.get_allocator()),
                 (ObExecContext, tmp_exec_ctx, OPT_CTX.get_allocator())) {
      ObPhysicalPlanCtx *plan_ctx = OPT_CTX.get_exec_ctx()->get_physical_plan_ctx();
      const int64_t cur_time = plan_ctx->has_cur_time() ?
          plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
      tmp_exec_ctx.set_my_session(OPT_CTX.get_session_info());
      tmp_exec_ctx.set_physical_plan_ctx(&tmp_plan_ctx);
      tmp_exec_ctx.set_sql_ctx(OPT_CTX.get_exec_ctx()->get_sql_ctx());
      tmp_plan_ctx.set_timeout_timestamp(plan_ctx->get_timeout_timestamp());
      tmp_plan_ctx.set_cur_time(cur_time, *OPT_CTX.get_session_info());
      tmp_plan_ctx.set_rich_format(OPT_CTX.get_session_info()->use_rich_format());
      if (FAILEDx(tmp_plan_ctx.get_param_store_for_update().assign(plan_ctx->get_param_store()))) {
        LOG_WARN("failed to assign phy plan ctx");
      } else if (OB_FAIL(tmp_plan_ctx.init_datum_param_store())) {
        LOG_WARN("failed to init datum store", K(ret));
      } else if (OB_FAIL(index_part_info.assign(*partition_info))) {
        LOG_WARN("failed to assign table part info", K(ret));
      } else if (OB_FAIL(index_part_info.replace_final_location_key(tmp_exec_ctx, index_id, true))) {
        LOG_WARN("failed to replace final location key", K(ret));
      }
      // init table meta info
      table_meta_range.ref_table_id_ = index_id;
      table_meta_range.table_rowkey_count_ = index_schema->get_rowkey_info().get_size();
      table_meta_range.table_column_count_ = index_schema->get_column_count();
      table_meta_range.micro_block_size_ = index_schema->get_block_size();
      table_meta_range.part_count_ =
          index_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
      table_meta_range.schema_version_ = index_schema->get_schema_version();
      table_meta_range.is_broadcast_table_ = index_schema->is_broadcast_table();
      if (FAILEDx(ObAccessPathEstimation::storage_estimate_range_rowcount(OPT_CTX,
                        index_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list(),
                        false,
                        &ranges,
                        table_meta_range))) {
        LOG_WARN("failed to estimate table range rowcount", K(ret));
      } else {
        query_range_row_count = table_meta_range.table_row_count_;
        selectivity = get_table_meta().table_row_count_ == 0 ? 0 :
                      table_meta_range.table_row_count_ * 1.0 / get_table_meta().table_row_count_;
        // refine selectivity
        selectivity = std::min(selectivity, 1.0);
      }
    }
  }
  return ret;
}

int ObJoinOrder::add_valid_fts_index_ids(PathHelper &helper, uint64_t *index_tid_array, int64_t &size)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMatchFunRawExpr *, 4> scan_match_exprs;
  ObSEArray<ObRawExpr *, 4> scan_match_filters;
  ObSEArray<uint64_t, 4> fts_index_ids;
  if (OB_FAIL(extract_scan_match_expr_candidates(get_restrict_infos(),
                                                 scan_match_exprs,
                                                 scan_match_filters))) {
    LOG_WARN("failed to extract scan match expr candidates", K(ret));
  } else if (!scan_match_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_match_exprs.count(); ++i) {
      const MatchExprInfo *match_expr_info = NULL;
      if (OB_FAIL(find_match_expr_info(helper.match_expr_infos_, scan_match_exprs.at(i), match_expr_info))) {
        LOG_WARN("failed to find match expr info", K(ret));
      } else if (OB_ISNULL(match_expr_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(fts_index_ids, match_expr_info->inv_idx_id_))) {
        LOG_WARN("failed to add var to array no dup", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < fts_index_ids.count() && size < OB_MAX_INDEX_PER_TABLE + 1; ++i) {
        index_tid_array[size++] = fts_index_ids.at(i);
      }
    }
  }
  return ret;
}

int ObJoinOrder::find_match_expr_info(const ObIArray<MatchExprInfo> &match_expr_infos,
                                      ObRawExpr *match_expr,
                                      const MatchExprInfo *&match_expr_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(match_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!match_expr->is_match_against_expr())) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(match_expr_info) && i < match_expr_infos.count(); ++i) {
      if (match_expr_infos.at(i).match_expr_ == match_expr) {
        match_expr_info = &match_expr_infos.at(i);
      }
    }
  }
  return ret;
}

int ObJoinOrder::find_least_selective_expr_on_index(const ObIArray<ObMatchFunRawExpr*> &match_exprs,
                                                    const ObIArray<MatchExprInfo> &match_expr_infos,
                                                    uint64_t index_id,
                                                    const MatchExprInfo *&match_expr_info)
{
  int ret = OB_SUCCESS;
  double min_selectivity = 1.1;
  for (int64_t i = 0; OB_SUCC(ret) && i < match_exprs.count(); ++i) {
    const MatchExprInfo *tmp_match_expr_info = NULL;
    if (OB_FAIL(find_match_expr_info(match_expr_infos, match_exprs.at(i), tmp_match_expr_info))) {
      LOG_WARN("failed to find match expr info", K(ret));
    } else if (OB_ISNULL(tmp_match_expr_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (tmp_match_expr_info->inv_idx_id_ == index_id &&
               tmp_match_expr_info->selectivity_ < min_selectivity) {
      min_selectivity = tmp_match_expr_info->selectivity_;
      match_expr_info = tmp_match_expr_info;
    }
  }
  return ret;
}

bool ObJoinOrder::invalid_index_for_vec_pre_filter(const ObTableSchema *index_hint_table_schema) const
{
  bool ret_bool = false;
  if (OB_NOT_NULL(index_hint_table_schema)) {
    ret_bool = index_hint_table_schema->is_fts_index()
            || index_hint_table_schema->is_multivalue_index()
            || index_hint_table_schema->is_global_index_table();
  }
  return ret_bool;
}

int ObJoinOrder::get_valid_hint_index_list(const ObDMLStmt &stmt,
                                           const ObIArray<uint64_t> &hint_index_ids,
                                           const bool is_link_table,
                                           ObSqlSchemaGuard *schema_guard,
                                           PathHelper &helper,
                                           ObIArray<uint64_t> &valid_hint_index_ids)
{
  int ret = OB_SUCCESS;
  const bool has_match_expr_on_table = helper.match_expr_infos_.count() > 0;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < hint_index_ids.count(); ++i) {
    const ObTableSchema *index_hint_table_schema = nullptr;
    const uint64_t tid = hint_index_ids.at(i);
    if (OB_FAIL(schema_guard->get_table_schema(tid, index_hint_table_schema, is_link_table))) {
      LOG_WARN("failed to get table schema", K(ret), K(tid));
    } else if (OB_ISNULL(index_hint_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to index hint table schema", K(ret), K(tid));
    } else if (index_hint_table_schema->is_global_index_table() && has_match_expr_on_table) {
      // scan with both global index and fulltext index is not supported yet
    } else if (stmt.has_vec_approx() && invalid_index_for_vec_pre_filter(index_hint_table_schema)) {
      // scan with both global index/fts_index/multi_value_index and vector index is not supported yet
    } else if (index_hint_table_schema->is_fts_index()) {
      bool is_valid = true;
      if (OB_FAIL(has_valid_match_filter_on_index(helper, tid, is_valid))) {
        LOG_WARN("failed to check has valid match filter on index", K(ret));
      } else if (!is_valid) {
        //do nothing
      } else if (OB_FAIL(valid_hint_index_ids.push_back(tid))) {
        LOG_WARN("failed to append valid hint index list", K(ret), K(tid));
      }
    } else if (OB_FAIL(valid_hint_index_ids.push_back(tid))) {
      LOG_WARN("failed to append valid hint index list", K(ret), K(tid));
    }
  }
  return ret;
}

int ObJoinOrder::has_valid_match_filter_on_index(PathHelper &helper, uint64_t tid, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMatchFunRawExpr*, 4> match_exprs;
  ObSEArray<ObRawExpr*, 4> match_filters;
  is_valid = false;
  if (OB_FAIL(extract_scan_match_expr_candidates(helper.filters_, match_exprs, match_filters))) {
    LOG_WARN("failed to extract scan match expr candidates", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < match_exprs.count(); ++i) {
      const MatchExprInfo *tmp_match_expr_info = NULL;
      if (OB_FAIL(find_match_expr_info(helper.match_expr_infos_, match_exprs.at(i), tmp_match_expr_info))) {
        LOG_WARN("failed to find match expr info", K(ret));
      } else if (OB_ISNULL(tmp_match_expr_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (tmp_match_expr_info->inv_idx_id_ == tid) {
        is_valid = true;
      }
    }
  }
  return ret;
}

int ObJoinOrder::get_better_index_prefix(const ObIArray<ObRawExpr*> &range_exprs,
                                         const ObIArray<int64_t> &range_expr_max_offsets,
                                         const ObIArray<uint64_t> &total_range_counts,
                                         int64_t &better_index_prefix)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> used_range_exprs;
  ObSEArray<ObRawExpr*, 4> used_filters;
  double range_sel = 1;
  double cost = 1;
  double min_cost = DBL_MAX;
  int64_t min_cost_offset = -1;
  uint64_t min_cost_range_count = 0;
  better_index_prefix = -1;
  if (OB_UNLIKELY(range_exprs.count() != range_expr_max_offsets.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range exprs", K(ret));
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  }
  for (int64_t offset = total_range_counts.count() - 1; OB_SUCC(ret) && offset >= 0; --offset) {
    used_range_exprs.reuse();
    used_filters.reuse();
    range_sel = 1;
    uint64_t range_count = offset == -1 ? 1 : total_range_counts.at(offset);
    for (int64_t i = 0; OB_SUCC(ret) && i < range_exprs.count(); ++i) {
      if (range_expr_max_offsets.at(i) <= offset) {
        if (OB_FAIL(used_range_exprs.push_back(range_exprs.at(i)))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      } else {
        if (OB_FAIL(used_filters.push_back(range_exprs.at(i)))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calculate_range_and_filter_expr_cost(used_range_exprs,
                                                            used_filters,
                                                            total_range_counts.count(),
                                                            range_count,
                                                            cost))) {
      LOG_WARN("failed to calculate range and filter expr cost", K(ret));
    } else if (total_range_counts.count() - 1 == offset) {
      min_cost_offset = offset;
      min_cost = cost;
      min_cost_range_count = range_count;
    } else if ( cost < min_cost) {
      min_cost_offset = offset;
      min_cost_range_count = range_count;
    }
  }
  if (OB_SUCC(ret)) {
    if (total_range_counts.at(total_range_counts.count() - 1) - min_cost_range_count > 500) {
      better_index_prefix = min_cost_offset + 1;
      LOG_TRACE("choose better index prefix", K(better_index_prefix));
    }
  }
  return ret;
}

int ObJoinOrder::deduce_prefix_str_idx_expr(ObRawExpr *pred,
                                            const TableItem *table_item,
                                            ObColumnRefRawExpr *prefix_col,
                                            ObIArray<ObRawExpr*> &simple_prefix_preds,
                                            ObRawExpr *&new_pred,
                                            PathHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_simple_preds;
  if (OB_ISNULL(pred) || OB_ISNULL(table_item) || OB_ISNULL(prefix_col) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    // deduce for each simple prefix preds
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_prefix_preds.count(); i++) {
      ObColumnRefRawExpr *column_expr = NULL;
      ObRawExpr *value_expr = NULL;
      ObRawExpr *escape_expr = NULL;
      ObItemType type;
      bool is_simple_pred = false;
      if (OB_ISNULL(simple_prefix_preds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FALSE_IT(type = simple_prefix_preds.at(i)->get_expr_type())) {
      } else if (OB_FAIL(check_simple_prefix_cmp_expr(simple_prefix_preds.at(i),
                                                      column_expr,
                                                      value_expr,
                                                      escape_expr,
                                                      type,
                                                      is_simple_pred))) {
        LOG_WARN("check simple gen col cmp expr failed", K(ret));
      } else if (OB_ISNULL(column_expr) || OB_ISNULL(value_expr) || !is_simple_pred ||
                column_expr->get_table_id() != table_item->table_id_ || !prefix_col->is_generated_column()) {
        // do nothing
      } else {
        const ObRawExpr *substr_expr = NULL;
        const ObRawExpr *dep_expr = prefix_col->get_dependant_expr();
        ObRawExpr *deduced_simple_pred = NULL;
        if (OB_ISNULL(dep_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generated column expr is null", K(ret));
        } else if (ObRawExprUtils::has_prefix_str_expr(*dep_expr, *column_expr, substr_expr)) {
          ObItemType gen_type = type;
          if (T_OP_GT == type) {
            gen_type = T_OP_GE;
          } else if (T_OP_LT == type) {
            gen_type = T_OP_LE;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(build_prefix_index_compare_expr(*prefix_col,
                                                              const_cast<ObRawExpr*>(substr_expr),
                                                              gen_type,
                                                              *value_expr,
                                                              escape_expr,
                                                              deduced_simple_pred,
                                                              helper))) {
            LOG_WARN("build prefix index compare expr failed", K(ret));
          } else if (OB_ISNULL(deduced_simple_pred)) {
            //do nothing
          } else if (OB_FAIL(deduced_simple_pred->formalize(get_plan()->get_optimizer_context().get_session_info()))) {
            LOG_WARN("formalize failed", K(ret));
          } else if (OB_FAIL(deduced_simple_pred->pull_relation_id())) {
            LOG_WARN("pullup relids and level failed", K(ret));
          } else if (OB_FAIL(new_simple_preds.push_back(deduced_simple_pred))) {
            LOG_WARN("push back failed", K(ret));
          } else {
            prefix_col->set_explicited_reference();
          }
        }
      }
    }
    // copy and replace root predicate
    if (OB_SUCC(ret) && simple_prefix_preds.count() == new_simple_preds.count()) {
      ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
      if (OB_FAIL(copier.add_replaced_expr(simple_prefix_preds, new_simple_preds))) {
        LOG_WARN("add replaced expr failed", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(pred, new_pred))) {
        LOG_WARN("copy on replace failed", K(ret));
      } else if (OB_ISNULL(new_pred)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new pred is null", K(ret));
      } else if (OB_FAIL(new_pred->formalize(get_plan()->get_optimizer_context().get_session_info()))) {
        LOG_WARN("formalize failed", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("pullup relids and level failed", K(ret));
      } else if (OB_FAIL(add_deduced_expr(new_pred, pred, false))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_match_prefix_index(ObRawExpr *expr,
                                          const TableItem *table_item,
                                          ObIArray<ObColumnRefRawExpr*> &prefix_cols,
                                          ObIArray<ObRawExpr*> &simple_prefix_preds,
                                          bool &is_match)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *column_expr = NULL;
  ObRawExpr *value_expr = NULL;
  ObRawExpr *escape_expr = NULL;
  ObItemType type = expr->get_expr_type();
  ObSEArray<ObColumnRefRawExpr*, 4> candi_prefix_cols;
  if (OB_ISNULL(expr) || OB_ISNULL(table_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(table_item));
  } else if (expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR) {
    is_match = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < expr->get_param_count(); ++i) {
      ObRawExpr *param = expr->get_param_expr(i);
      bool child_match = false;
      if (OB_ISNULL(param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(param));
      } else if (OB_FAIL(SMART_CALL(check_match_prefix_index(param,
                                                            table_item,
                                                            prefix_cols,
                                                            simple_prefix_preds,
                                                            child_match)))) {
        LOG_WARN("failed to check match prefix index", K(ret));
      } else if (!child_match) {
        is_match = false;
      }
    }
  } else if (OB_FAIL(check_simple_expr_match_prefix_index(expr,
                                                          table_item,
                                                          column_expr,
                                                          value_expr,
                                                          escape_expr,
                                                          type,
                                                          candi_prefix_cols,
                                                          is_match))) {
    LOG_WARN("failed to check simple expr match prefix index", K(ret));
  } else if (is_match) {
    // candi_prefix_cols should be subset of prefix_cols
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < candi_prefix_cols.count(); i++) {
      if (ObOptimizerUtil::find_item(prefix_cols, candi_prefix_cols.at(i))) {
        // do nothing
      } else {
        is_match = false;
      }
    }
    if (OB_SUCC(ret) && is_match) {
      if (OB_FAIL(simple_prefix_preds.push_back(expr))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(prefix_cols.assign(candi_prefix_cols))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_simple_expr_match_prefix_index(ObRawExpr *expr,
                                                      const TableItem *table_item,
                                                      ObColumnRefRawExpr *&column_expr,
                                                      ObRawExpr *&value_expr,
                                                      ObRawExpr *&escape_expr,
                                                      ObItemType &type,
                                                      ObIArray<ObColumnRefRawExpr*> &candi_prefix_cols,
                                                      bool &is_match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (OB_FAIL(check_simple_prefix_cmp_expr(expr,
                                                   column_expr,
                                                   value_expr,
                                                   escape_expr,
                                                   type,
                                                   is_match))) {
    LOG_WARN("failed to check simple gen col cmp expr", K(ret));
  } else if (!is_match) {
    // do nothing
  } else if (OB_NOT_NULL(column_expr) && column_expr->has_generated_column_deps()) {
    ObSEArray<ObColumnRefRawExpr *, 4> column_exprs;
    if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(table_item->table_id_, column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
      ObColumnRefRawExpr *gen_column_expr = column_exprs.at(j);
      if (OB_ISNULL(gen_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (!gen_column_expr->is_generated_column()) {
        //do nothing
      } else {
        const ObRawExpr *dep_expr = gen_column_expr->get_dependant_expr();
        const ObRawExpr *substr_expr = NULL;
        if (OB_ISNULL(dep_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generated column expr is null", K(*gen_column_expr), K(j), K(ret));
        } else if (!ObRawExprUtils::has_prefix_str_expr(*dep_expr, *column_expr, substr_expr)) {
          // do nothing
        } else if (OB_FAIL(candi_prefix_cols.push_back(gen_column_expr))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
    is_match &= candi_prefix_cols.count() > 0;
  } else {
    is_match = false;
  }
  return ret;
}

int ObJoinOrder::check_simple_prefix_cmp_expr(ObRawExpr *expr,
                                              ObColumnRefRawExpr *&column_expr,
                                              ObRawExpr *&value_expr,
                                              ObRawExpr *&escape_expr,
                                              ObItemType &type,
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObRawExpr *param_expr1 = NULL;
  ObRawExpr *param_expr2 = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (!IS_BASIC_CMP_OP(expr->get_expr_type()) && !IS_SINGLE_VALUE_OP(expr->get_expr_type())) {
    //only =/</<=/>/>=/IN/like can deduce generated exprs
    is_valid = false;
  } else if (OB_ISNULL(param_expr1 = expr->get_param_expr(0)) || OB_ISNULL(param_expr2 = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(*expr), K(expr->get_param_expr(0)), K(expr->get_param_expr(1)), K(ret));
  } else if (T_OP_LIKE == expr->get_expr_type()) {
    if (3 != expr->get_param_count() || OB_ISNULL(expr->get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("escape param is unexpected null", K(ret));
    } else if (param_expr1->is_column_ref_expr() &&
              param_expr1->get_result_type().is_string_type() &&
              param_expr2->get_result_type().is_string_type() &&
              expr->get_param_expr(1)->is_const_expr() &&
              expr->get_param_expr(2)->is_const_expr()) {
      column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
      value_expr = expr->get_param_expr(1);
      escape_expr = expr->get_param_expr(2);
    } else {
      is_valid = false;
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    if (T_OP_ROW == param_expr2->get_expr_type() &&
        !param_expr2->has_generalized_column() &&
        param_expr1->get_result_type().is_string_type() &&
        param_expr1->is_column_ref_expr()) {
      bool all_match = true;
      for (int64_t j = 0; OB_SUCC(ret) && all_match && j < param_expr2->get_param_count(); ++j) {
        if (OB_ISNULL(param_expr2->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr2 is null");
        } else if (!param_expr2->get_param_expr(j)->get_result_type().is_string_type()
            || param_expr2->get_param_expr(j)->get_collation_type() != param_expr1->get_collation_type()) {
          all_match = false;
        }
      }
      if (OB_SUCC(ret) && all_match && expr->get_param_expr(0)->is_column_ref_expr()) {
        column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
        value_expr = expr->get_param_expr(1);
      } else {
        is_valid = false;
      }
    }
  } else if (param_expr1->is_column_ref_expr() && param_expr2->is_const_expr()) {
    if (param_expr1->get_result_type().is_string_type() //only for string and same collation
        && param_expr2->get_result_type().is_string_type()
        && param_expr1->get_collation_type() == param_expr2->get_collation_type()) {
      column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
      value_expr = expr->get_param_expr(1);
    } else {
      is_valid = false;
    }
  } else if (param_expr1->is_const_expr() && param_expr2->is_column_ref_expr()) {
    if (param_expr1->get_result_type().is_string_type()
        && param_expr2->get_result_type().is_string_type()) {
      type = get_opposite_compare_type(expr->get_expr_type());
      column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(1));
      value_expr = expr->get_param_expr(0);
    } else {
      is_valid = false;
    }
  } else if (param_expr1->is_column_ref_expr() && param_expr2->is_column_ref_expr()) {
    is_valid = false;
  }
  return ret;
}

int ObJoinOrder::check_match_common_gen_col_index(ObRawExpr *expr,
                                                  const TableItem *table_item,
                                                  ObIArray<ObColumnRefRawExpr*> &common_gen_cols,
                                                  ObIArray<ObRawExpr*> &simple_gen_col_preds,
                                                  bool &is_match)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 4> cur_gen_cols;
  if (OB_ISNULL(expr) || OB_ISNULL(table_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(table_item));
  } else if (expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR) {
    is_match = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < expr->get_param_count(); ++i) {
      ObRawExpr *param = expr->get_param_expr(i);
      bool child_match = false;
      if (OB_ISNULL(param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(param));
      } else if (OB_FAIL(SMART_CALL(check_match_common_gen_col_index(param,
                                                                    table_item,
                                                                    common_gen_cols,
                                                                    simple_gen_col_preds,
                                                                    child_match)))) {
        LOG_WARN("failed to check match common gen col index", K(ret));
      } else if (!child_match) {
        is_match = false;
      }
    }
  } else if (OB_FAIL(check_simple_expr_match_gen_col_index(expr, table_item, cur_gen_cols, is_match))) {
    LOG_WARN("failed to check simple gen col prefix index", K(ret));
  } else if (is_match) {
    // cur_gen_cols should be subset of common_gen_cols
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < cur_gen_cols.count(); i++) {
      if (ObOptimizerUtil::find_item(common_gen_cols, cur_gen_cols.at(i))) {
        // do nothing
      } else {
        is_match = false;
      }
    }
    if (OB_SUCC(ret) && is_match) {
      if (OB_FAIL(simple_gen_col_preds.push_back(expr))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(common_gen_cols.assign(cur_gen_cols))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_simple_expr_match_gen_col_index(ObRawExpr *expr,
                                                       const TableItem *table_item,
                                                       ObIArray<ObColumnRefRawExpr*> &candi_gen_cols,
                                                       bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObSEArray<ObColumnRefRawExpr*, 4> column_exprs;
  ObSEArray<ObPCConstParamInfo, 4> dummy;
  if (OB_ISNULL(expr) || OB_ISNULL(table_item) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (expr->get_expr_class() != ObRawExpr::EXPR_OPERATOR) { // these can extract query range
    //do nothing
  } else if (!expr->has_flag(CNT_COLUMN)) {
    //do nothing
  } else if (OB_FAIL(get_plan()->get_stmt()->get_column_exprs(table_item->table_id_, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    bool is_precise_deduced = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
      ObColumnRefRawExpr *gen_col_expr = column_exprs.at(i);
      ObRawExpr* depend_expr = NULL;
      bool cur_match = false;
      ObRawExpr *from_expr = nullptr;
      if (OB_ISNULL(gen_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col is null", K(ret));
      } else if (!gen_col_expr->is_generated_column()) {
        //do nothing
      } else if (OB_FAIL(check_simple_gen_col_cmp_expr(expr, gen_col_expr, dummy, from_expr, cur_match, is_precise_deduced))) {
        LOG_WARN("failed to check simple gen col cmp expr", K(ret));
      } else if (!cur_match) {
        // do nothing
      } else if (OB_FAIL(candi_gen_cols.push_back(gen_col_expr))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        is_match = true;
      }
    }
  }
  return ret;
}

int ObJoinOrder::check_simple_gen_col_cmp_expr(ObRawExpr *expr,
                                               ObColumnRefRawExpr *gen_col_expr,
                                               ObIArray<ObPCConstParamInfo> &param_infos,
                                               ObRawExpr *&matched_expr,
                                               bool &is_match,
                                               bool &is_precise_deduced)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObRawExpr* depend_expr = NULL;
  bool is_lossless = false;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(gen_col_expr) || OB_ISNULL(OPT_CTX.get_exec_ctx()) ||
      OB_ISNULL(phy_plan_ctx = OPT_CTX.get_exec_ctx()->get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(gen_col_expr));
  } else if (expr->get_expr_class() != ObRawExpr::EXPR_OPERATOR) { // these can extract query range
    //do nothing
  } else if (OB_FALSE_IT(depend_expr = gen_col_expr->get_dependant_expr())) {
  } else if (OB_ISNULL(depend_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("depend_expr is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_conv(depend_expr, is_lossless))) {
    LOG_WARN("check depend epxr lossless failed", K(ret));
  } else if(is_lossless) {
    depend_expr = depend_expr->get_param_expr(4);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(depend_expr, is_lossless))) {
    LOG_WARN("check depend epxr lossless failed", K(ret));
  } else if(is_lossless) {
    depend_expr = depend_expr->get_param_expr(0);
  }
  // compare each param expr with depend_expr
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < expr->get_param_count(); i++) {
    ObRawExpr *param_expr = expr->get_param_expr(i);
    ObExprEqualCheckContext equal_ctx;
    equal_ctx.override_const_compare_ = true;
    bool is_same = false;
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
      //remove inner cast's influence.
    } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(param_expr, param_expr))) {
      LOG_WARN("fail to get real child without lossless cast", K(ret));
    } else if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real child is null", K(ret));
    } else if (depend_expr->same_as(*param_expr, &equal_ctx)) {
      is_same = true;
    } else {
      equal_ctx.reset();
      equal_ctx.override_const_compare_ = true;
      if (OB_FAIL(check_match_to_type(depend_expr, param_expr, is_same, equal_ctx))) {
        LOG_WARN("fail to check if to_<type> expr can be extracted", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_same) {
      is_match = true;
      matched_expr = param_expr;
      if (depend_expr->is_multivalue_define_json_expr() && param_expr->is_domain_json_expr()) {
        ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(param_expr);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret));
        } else {
          matched_expr = expr->get_json_domain_param_expr();
          is_precise_deduced = false;
        }
      }
      // collect param constraints
      for(int64_t i = 0; OB_SUCC(ret) && i < equal_ctx.param_expr_.count(); i++) {
        int64_t param_idx = equal_ctx.param_expr_.at(i).param_idx_;
        ObPCConstParamInfo param_info;
        if (OB_UNLIKELY(param_idx < 0 || param_idx >= phy_plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(param_idx), K(phy_plan_ctx->get_param_store().count()));
        } else if (OB_FAIL(param_info.const_idx_.push_back(param_idx))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(param_info.const_params_.push_back(phy_plan_ctx->get_param_store().at(param_idx)))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(param_infos.push_back(param_info))) {
          LOG_WARN("failed to push back param info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinOrder::deduce_common_gen_col_index_expr(ObRawExpr *pred,
                                                  const TableItem *table_item,
                                                  ObColumnRefRawExpr *gen_col,
                                                  ObIArray<ObRawExpr*> &simple_gen_col_preds,
                                                  ObRawExpr *&new_pred,
                                                  PathHelper &helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> from_exprs;
  ObSEArray<ObRawExpr*, 4> to_exprs;
  ObSEArray<ObPCConstParamInfo, 4> const_param_infos;
  bool is_precise_deduced = true;
  if (OB_ISNULL(pred) || OB_ISNULL(table_item) || OB_ISNULL(gen_col) || OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pred), K(table_item), K(gen_col));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_gen_col_preds.count(); i++) {
      ObRawExpr *simple_pred = simple_gen_col_preds.at(i);
      bool is_match = false;
      ObRawExpr *from_expr = nullptr;
      if (OB_ISNULL(simple_pred)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("simple pred is null", K(ret));
      } else if (OB_FAIL(check_simple_gen_col_cmp_expr(simple_pred,
                                                       gen_col,
                                                       const_param_infos,
                                                       from_expr,
                                                       is_match,
                                                       is_precise_deduced))) {
        LOG_WARN("failed to check simple gen col cmp expr", K(ret));
      } else if (!is_match) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(is_match));
      } else if (OB_ISNULL(from_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (ObOptimizerUtil::find_item(from_exprs, from_expr)) {
        // do nothing
      } else if (OB_FAIL(from_exprs.push_back(from_expr))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(to_exprs.push_back(gen_col))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        gen_col->set_explicited_reference();
      }
    }
  }
  // copy and replace root predicate
  if (OB_SUCC(ret) && !from_exprs.empty()) {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_FAIL(copier.add_replaced_expr(from_exprs, to_exprs))) {
      LOG_WARN("add replaced expr failed", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(pred, new_pred))) {
      LOG_WARN("copy on replace failed", K(ret));
    } else if (OB_ISNULL(new_pred)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new pred is null", K(ret));
    } else if (OB_FAIL(new_pred->formalize(get_plan()->get_optimizer_context().get_session_info()))) {
      if (ret != OB_SUCCESS) {
        //probably type deduced failed. do nothing
        LOG_WARN("new qual is not formalized correctly", K(ret), KPC(new_pred));
        ret = OB_SUCCESS;
        new_pred = NULL;
      }
    } else if (OB_FAIL(new_pred->pull_relation_id())) {
      LOG_WARN("pullup relids and level failed", K(ret));
    } else if (OB_FAIL(add_deduced_expr(new_pred, pred, is_precise_deduced, const_param_infos))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

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
#include "sql/optimizer/ob_access_path_estimation.h"
#include "sql/optimizer/ob_join_order.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/optimizer/ob_storage_estimator.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/engine/table/ob_table_scan_op.h"
namespace oceanbase {
using namespace share::schema;
using namespace share;
namespace sql {

/// It is possible for us to find a better way to combine differnt kinds of cardinality
/// estimation methods. e.g. if a table is found to be uniformally distributed over partitions,
/// we can do a storage estimation on one of these parts, and then deduce the total row count.
int ObAccessPathEstimation::estimate_rowcount(ObOptimizerContext &ctx,
                                              ObTableMetaInfo &meta,
                                              common::ObIArray<AccessPath *> &paths)
{
  int ret = OB_SUCCESS;
  ObArray<AccessPath *> tmp;

  // wo do statistics estimation for all paths,
  // the storage estimation is an advanced tech, which introduces more accurate results
  // but it has serveral limitations, hence we check its usage here
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    RowCountEstMethod method;
    bool use_default_vt = false;
    if (OB_FAIL(choose_best_estimation_method(paths.at(i), meta, method, use_default_vt))) {
      LOG_WARN("failed to choose best estimation method", K(ret));
    } else if (use_default_vt) {
      if (OB_FAIL(process_vtable_estimation(paths.at(i)))) {
        LOG_WARN("failed to process virtual table estimation", K(ret));
      }
    } else if (OB_FAIL(process_statistics_estimation(meta, paths.at(i)))) {
      LOG_WARN("failed to process statistics estimation", K(ret));
    } else if (method != RowCountEstMethod::LOCAL_STORAGE) {
      // do nothing
    } else if (OB_FAIL(tmp.push_back(paths.at(i)))) {
      LOG_WARN("failed to push back path", K(ret));
    }
  }
  if (OB_SUCC(ret) && !tmp.empty()) {
    //try to use storage estimation to refine the statistics estimation result.
    if (OB_FAIL(process_storage_estimation(ctx, meta, tmp))) {
      LOG_WARN("failed to process storage estimation", K(ret));
    }
  }
  return ret;
}

int ObAccessPathEstimation::choose_best_estimation_method(const AccessPath *path,
                                                          const ObTableMetaInfo &meta,
                                                          RowCountEstMethod &method,
                                                          bool &use_default_vt)
{
  int ret = OB_SUCCESS;
  const ObTablePartitionInfo *part_info = NULL;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is invalid", K(ret), K(path));
  } else if (is_virtual_table(path->ref_table_id_) &&
             !share::is_oracle_mapping_real_virtual_table(path->ref_table_id_)) {
    use_default_vt = !meta.has_opt_stat_;
    method = meta.has_opt_stat_ ? RowCountEstMethod::BASIC_STAT : RowCountEstMethod::DEFAULT_STAT;
  } else {
    if (meta.is_empty_table_) {
      method = RowCountEstMethod::BASIC_STAT;
    } else if (OB_ISNULL(part_info = path->table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table partition info is null", K(ret), K(part_info));
    } else {
      int64_t scan_range_count = get_scan_range_count(path->get_query_ranges());
      int64_t partition_count = part_info->get_phy_tbl_location_info().get_partition_cnt();
      if (partition_count > 1 ||
          scan_range_count <= 0 ||
          scan_range_count > ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM) {
        method = RowCountEstMethod::BASIC_STAT;
      } else {
        method = RowCountEstMethod::LOCAL_STORAGE;
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::process_vtable_estimation(AccessPath *path)
{
  int ret = OB_SUCCESS;
  double output_row_count = 0.0;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else {
    const ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    if (est_cost_info.ranges_.empty()) {
      output_row_count = static_cast<double>(OB_EST_DEFAULT_VIRTUAL_TABLE_ROW_COUNT);
      LOG_TRACE("OPT:[VT] virtual table without range, use default stat", K(output_row_count));
    } else {
      output_row_count = static_cast<double>(est_cost_info.ranges_.count());
      if (!est_cost_info.is_unique_) {
        output_row_count *= 100.0;
      }
    }
    path->query_range_row_count_ = output_row_count;
    path->phy_query_range_row_count_ = output_row_count;
    path->index_back_row_count_ = 0;
    path->output_row_count_ = output_row_count;
  }
  return ret;
}

int ObAccessPathEstimation::process_storage_estimation(ObOptimizerContext &ctx,
                                                       ObTableMetaInfo &table_meta,
                                                       ObIArray<AccessPath *> &paths)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("CardEstimation");
  ObArray<ObBatchEstTasks *> tasks;
  ObArray<ObAddr> prefer_addrs;
  void *ptr = NULL;
  
  bool force_leader_estimation = false;
  
  force_leader_estimation = OB_FAIL(E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);
  ret = OB_SUCCESS;

  // for each access path, find a partition/server for estimation
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    AccessPath *ap = NULL;
    ObBatchEstTasks *task = NULL;
    EstimatedPartition best_index_part;
    SMART_VARS_3((ObTablePartitionInfo, tmp_part_info),
                 (ObPhysicalPlanCtx, tmp_plan_ctx, arena),
                 (ObExecContext, tmp_exec_ctx, arena)) {
      const ObTablePartitionInfo *table_part_info = NULL;
      if (OB_ISNULL(ap = paths.at(i)) ||
          OB_ISNULL(table_part_info = ap->table_partition_info_) ||
          OB_ISNULL(ctx.get_session_info()) ||
          OB_ISNULL(ctx.get_exec_ctx()) ||
          OB_ISNULL(ctx.get_exec_ctx()->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access path is invalid", K(ret), K(ap), K(table_part_info), K(ctx.get_exec_ctx()));
      } else {
        ObPhysicalPlanCtx *plan_ctx = ctx.get_exec_ctx()->get_physical_plan_ctx();
        const int64_t cur_time = plan_ctx->has_cur_time() ?
            plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
        tmp_exec_ctx.set_my_session(ctx.get_session_info());
        tmp_exec_ctx.set_physical_plan_ctx(&tmp_plan_ctx);
        tmp_exec_ctx.set_sql_ctx(ctx.get_exec_ctx()->get_sql_ctx());
        tmp_plan_ctx.set_timeout_timestamp(plan_ctx->get_timeout_timestamp());
        tmp_plan_ctx.set_cur_time(cur_time, *ctx.get_session_info());
        if (OB_FAIL(tmp_plan_ctx.get_param_store_for_update().assign(plan_ctx->get_param_store()))) {
          LOG_WARN("failed to assign phy plan ctx");
        } else if (OB_FAIL(tmp_plan_ctx.init_datum_param_store())) {
          LOG_WARN("failed to init datum store", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tmp_part_info.assign(*table_part_info))) {
        LOG_WARN("failed to assign table part info", K(ret));
      } else if (!ap->is_global_index_ && ap->ref_table_id_ != ap->index_id_ &&
                OB_FAIL(tmp_part_info.replace_final_location_key(tmp_exec_ctx,
                                                                 ap->index_id_,
                                                                 true))) {
        LOG_WARN("failed to replace final location key", K(ret));
      } else if (OB_FAIL(ObSQLUtils::choose_best_replica_for_estimation(
                          tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list(),
                          ctx.get_local_server_addr(),
                          prefer_addrs,
                          !ap->can_use_remote_estimate(),
                          best_index_part))) {
        LOG_WARN("failed to choose best partition for estimation", K(ret));
      } else if (force_leader_estimation &&
                 OB_FAIL(choose_leader_replica(tmp_part_info,
                                               ap->can_use_remote_estimate(),
                                               ctx.get_local_server_addr(),
                                               best_index_part))) {
        LOG_WARN("failed to choose leader replica", K(ret));
      } else if (!best_index_part.is_valid()) {
        // does not do storage estimation for the index
      } else if (OB_FAIL(get_task(tasks, best_index_part.addr_, task))) {
        LOG_WARN("failed to get task", K(ret));
      } else if (NULL != task) {
        // do nothing
      } else if (OB_FAIL(prefer_addrs.push_back(best_index_part.addr_))) {
        LOG_WARN("failed to push back new addr", K(ret));
      } else if (OB_ISNULL(ptr = arena.alloc(sizeof(ObBatchEstTasks)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret));
      } else {
        task = new (ptr) ObBatchEstTasks();
        task->addr_ = best_index_part.addr_;
        task->arg_.schema_version_ = table_meta.schema_version_;
        OZ (tasks.push_back(task));
      }
      if (OB_SUCC(ret) && NULL != task) {
        if (OB_FAIL(add_index_info(ctx, arena, task, best_index_part, ap))) {
          LOG_WARN("failed to add task info", K(ret));
        }
      }
    }
  }

  NG_TRACE(storage_estimation_begin);
  /// @brief need_fallback, abort storage estimation, just use statistics results
  bool need_fallback = false;
  // process each batch estimation task
  for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < tasks.count(); ++i) {
    if (OB_ISNULL(tasks.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", K(ret));
    } else if (OB_FAIL(do_storage_estimation(ctx, *tasks.at(i)))) {
      LOG_WARN("failed to process storage estimation", K(ret));
      need_fallback = true;
      ret = OB_SUCCESS;
      break;
    } else if (!tasks.at(i)->check_result_reliable()) {
      need_fallback = true;
    }
  }
  NG_TRACE(storage_estimation_end);

  if (!need_fallback) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObBatchEstTasks *task = tasks.at(i);
      RowCountEstMethod est_method = (task->addr_ == ctx.get_local_server_addr() ?
            RowCountEstMethod::LOCAL_STORAGE : RowCountEstMethod::REMOTE_STORAGE);
      for (int64_t j = 0; OB_SUCC(ret) && j < task->paths_.count(); ++j) {
        const obrpc::ObEstPartResElement &res = task->res_.index_param_res_.at(j);
        AccessPath *path = task->paths_.at(j);
        if (OB_FAIL(path->est_records_.assign(res.est_records_))) {
          LOG_WARN("failed to assign estimation records", K(ret));
        } else if (OB_FAIL(estimate_prefix_range_rowcount(res,
                                                          path->est_cost_info_,
                                                          path->query_range_row_count_,
                                                          path->phy_query_range_row_count_))) {
          LOG_WARN("failed to estimate prefix range rowcount", K(ret));
        } else if (OB_FAIL(fill_cost_table_scan_info(path->est_cost_info_,
                                                     est_method,
                                                     path->output_row_count_,
                                                     path->query_range_row_count_,
                                                     path->phy_query_range_row_count_,
                                                     path->index_back_row_count_))) {
          LOG_WARN("failed to fill cost table scan info", K(ret));
        }
      }
    }
  }

  // deconstruct ObBatchEstTasks
  for (int64_t i = 0; i < tasks.count(); ++i) {
    if (NULL != tasks.at(i)) {
      tasks.at(i)->~ObBatchEstTasks();
      tasks.at(i) = NULL;
    }
  }
  return ret;
}

int ObAccessPathEstimation::choose_leader_replica(const ObTablePartitionInfo &table_part_info,
                                                  const bool can_use_remote,
                                                  const ObAddr &local_addr,
                                                  EstimatedPartition &best_partition)
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLoc &part_loc_info = 
      table_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list().at(0);
  const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_array = 
      part_loc_info.get_partition_location().get_replica_locations();
  
  for (int64_t i = 0; i < replica_loc_array.count(); ++i) {
    if (replica_loc_array.at(i).is_strong_leader() &&
        (can_use_remote || local_addr == replica_loc_array.at(i).get_server())) {
      best_partition.set(replica_loc_array.at(i).get_server(),
                         part_loc_info.get_partition_location().get_tablet_id(),
                         part_loc_info.get_partition_location().get_ls_id());
      break;
    }
  }
  return ret;
}

int ObAccessPathEstimation::do_storage_estimation(ObOptimizerContext &ctx,
                                                  ObBatchEstTasks &tasks)
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = tasks.addr_;
  const obrpc::ObEstPartArg &arg = tasks.arg_;
  obrpc::ObEstPartRes &result = tasks.res_;
  if (addr == ctx.get_local_server_addr()) {
    if (OB_FAIL(ObStorageEstimator::estimate_row_count(arg, result))) {
      LOG_WARN("failed to estimate partition rows", K(ret));
    }
  } else {
    obrpc::ObSrvRpcProxy *rpc_proxy = NULL;
    const ObSQLSessionInfo *session_info = NULL;
    int64_t timeout = -1;
    if (OB_ISNULL(session_info = ctx.get_session_info()) ||
        OB_ISNULL(rpc_proxy = ctx.get_srv_proxy())) {
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
  }
  return ret;
}

int ObAccessPathEstimation::estimate_prefix_range_rowcount(
    const obrpc::ObEstPartResElement &result,
    ObCostTableScanInfo &est_cost_info,
    double &logical_row_count,
    double &physical_row_count)
{
  int ret = OB_SUCCESS;
  logical_row_count = 0;
  physical_row_count = 0;
  int64_t get_range_count = get_get_range_count(est_cost_info.ranges_);
  int64_t scan_range_count = get_scan_range_count(est_cost_info.ranges_);

  // at most N query ranges are used in storage estimation
  double range_sample_ratio = (scan_range_count * 1.0 )
      / ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
  range_sample_ratio = range_sample_ratio > 1.0 ? range_sample_ratio : 1.0;

  logical_row_count += range_sample_ratio * result.logical_row_count_ + get_range_count;
  physical_row_count += range_sample_ratio * result.physical_row_count_ + get_range_count;

  // number of index partition
  logical_row_count *= est_cost_info.index_meta_info_.index_part_count_;
  physical_row_count *= est_cost_info.index_meta_info_.index_part_count_;

  // NLJ or SPF push down prefix filters
  logical_row_count  *= est_cost_info.pushdown_prefix_filter_sel_;
  physical_row_count *= est_cost_info.pushdown_prefix_filter_sel_;

  LOG_TRACE("OPT:[STORAGE EST ROW COUNT]",
            K(logical_row_count), K(physical_row_count),
            K(get_range_count), K(scan_range_count),
            K(range_sample_ratio), K(result), K(est_cost_info.index_meta_info_.index_part_count_),
            K(est_cost_info.pushdown_prefix_filter_sel_));
  return ret;
}

int ObAccessPathEstimation::fill_cost_table_scan_info(ObCostTableScanInfo &est_cost_info,
                                                      const RowCountEstMethod est_method,
                                                      double &output_row_count,
                                                      double &logical_row_count,
                                                      double &physical_row_count,
                                                      double &index_back_row_count)
{
  int ret = OB_SUCCESS;
  est_cost_info.row_est_method_ = est_method;

  // we have exact query ranges on a unique index,
  // each range is expected to have at most one row
  if (est_cost_info.is_unique_) {
    logical_row_count  = est_cost_info.ranges_.count();
    physical_row_count = est_cost_info.ranges_.count();
  }

  // block sampling
  double block_sample_ratio = est_cost_info.sample_info_.is_block_sample() ?
        0.01 * est_cost_info.sample_info_.percent_ : 1.0;
  logical_row_count *= block_sample_ratio;
  physical_row_count *= block_sample_ratio;

  logical_row_count = std::max(logical_row_count, 1.0);
  physical_row_count = std::max(physical_row_count, 1.0);

  // index back row count
  if (est_cost_info.index_meta_info_.is_index_back_) {
    index_back_row_count = logical_row_count * est_cost_info.postfix_filter_sel_;
  }

  output_row_count = logical_row_count;
  // row sampling
  double row_sample_ratio = est_cost_info.sample_info_.is_row_sample() ?
        0.01 * est_cost_info.sample_info_.percent_ : 1.0;
  output_row_count *= row_sample_ratio;

  // postfix index filter and table filter
  output_row_count = output_row_count
      * est_cost_info.postfix_filter_sel_
      * est_cost_info.table_filter_sel_;

  if (OB_SUCC(ret)) {
    int64_t get_range_count = get_get_range_count(est_cost_info.ranges_);
    int64_t scan_range_count = get_scan_range_count(est_cost_info.ranges_);
    if (get_range_count + scan_range_count > 1) {
      if (scan_range_count >= 1) {
        est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_SCAN;
      } else {
        est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_GET;
      }
    } else {
      if (scan_range_count == 1) {
        est_cost_info.batch_type_ = ObSimpleBatch::T_SCAN;
      } else {
        est_cost_info.batch_type_ = ObSimpleBatch::T_GET;
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::add_index_info(ObOptimizerContext &ctx,
                                           ObIAllocator &allocator,
                                           ObBatchEstTasks *task,
                                           const EstimatedPartition &part,
                                           AccessPath *ap)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObNewRange, 4> tmp_ranges;
  ObSEArray<common::ObNewRange, 4> get_ranges;
  ObSEArray<common::ObNewRange, 4> scan_ranges;
  obrpc::ObEstPartArgElement *index_est_arg = NULL;
  if (OB_ISNULL(task) || OB_ISNULL(ap) || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid access path or batch task", K(ret), K(task), K(ap));
  } else if (OB_UNLIKELY(task->addr_ != part.addr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path uses invalid batch task", K(ret), K(task->addr_), K(part.addr_));
  } else if (OB_FAIL(task->paths_.push_back(ap))) {
    LOG_WARN("failed to push back access path", K(ret));
  } else if (OB_ISNULL(index_est_arg = task->arg_.index_params_.alloc_place_holder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate index argument", K(ret));
  } else if (OB_FAIL(get_key_ranges(ctx, allocator, part.tablet_id_, ap, tmp_ranges))) {
    LOG_WARN("failed to get key ranges", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(
                       tmp_ranges,
                       get_ranges,
                       scan_ranges))) {
    LOG_WARN("failed to clasiffy get scan ranges", K(ret));
  } else {
    index_est_arg->index_id_ =  ap->index_id_;
    index_est_arg->scan_flag_.index_back_ = ap->est_cost_info_.index_meta_info_.is_index_back_;
    index_est_arg->scan_flag_.disable_cache();
    index_est_arg->range_columns_count_ =  ap->est_cost_info_.range_columns_.count();
    index_est_arg->tablet_id_ = part.tablet_id_;
    index_est_arg->ls_id_ = part.ls_id_;
    index_est_arg->tenant_id_ = ctx.get_session_info()->get_effective_tenant_id();
  }
  // FIXME, move following codes
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; ap->is_global_index_ && i < scan_ranges.count(); ++i) {
      scan_ranges.at(i).table_id_ = ap->index_id_;
    }
    if (scan_ranges.count() > ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM) {
      ObArray<common::ObNewRange> valid_ranges;
      for (int64_t i = 0; OB_SUCC(ret) && i < ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM; ++i) {
        if (OB_FAIL(valid_ranges.push_back(scan_ranges.at(i)))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan_ranges.assign(valid_ranges))) {
          LOG_WARN("failed to assgin valid ranges", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_scan_range_batch(allocator, scan_ranges, index_est_arg->batch_))) {
        LOG_WARN("failed to construct scan range batch", K(ret));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::process_statistics_estimation(const ObTableMetaInfo &meta,
                                                          AccessPath *path)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObNewRange, 4> get_ranges;
  ObSEArray<common::ObNewRange, 4> scan_ranges;
  const ObTableMetaInfo *table_meta_info = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(table_meta_info = path->est_cost_info_.table_meta_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path), K(table_meta_info));
  } else if (OB_FAIL(ObOptEstCost::calculate_filter_selectivity(
                       path->est_cost_info_,
                       path->parent_->get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate filter selectivity", K(ret));
  } else {
    ObArenaAllocator allocator;
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    double &logical_row_count = path->query_range_row_count_;
    double &physical_row_count = path->phy_query_range_row_count_;

    // if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(est_cost_info.ranges_,
    //                                                       get_ranges,
    //                                                       scan_ranges))) {
    //   LOG_WARN("failed to classify get scan ranges", K(ret));
    // } else if (!scan_ranges.empty()) {
    //   if (OB_FAIL(ObOptEstCost::stat_estimate_partition_batch_rowcount(est_cost_info,
    //                                                                    scan_ranges,
    //                                                                    logical_row_count))) {
    //     LOG_WARN("failed to estimate partition batch row count", K(ret));
    //   }
    // }
    // logical_row_count += get_ranges.count();
    
    // TODO: @yibo need refine for unprecise query range
    logical_row_count = table_meta_info->table_row_count_ * est_cost_info.prefix_filter_sel_;
    physical_row_count = logical_row_count;

    // NLJ or SPF push down prefix filters
    logical_row_count  *= est_cost_info.pushdown_prefix_filter_sel_;
    physical_row_count *= est_cost_info.pushdown_prefix_filter_sel_;

    LOG_TRACE("OPT:[STATISTIC EST ROW COUNT",
              K(logical_row_count), K(physical_row_count),
              K(est_cost_info.pushdown_prefix_filter_sel_));

    RowCountEstMethod est_method = meta.has_opt_stat_ ? RowCountEstMethod::BASIC_STAT :
                                                        RowCountEstMethod::DEFAULT_STAT;

    OZ (fill_cost_table_scan_info(est_cost_info,
                                  est_method,
                                  path->output_row_count_,
                                  logical_row_count,
                                  physical_row_count,
                                  path->index_back_row_count_));
  }
  return ret;
}

int ObAccessPathEstimation::get_task(ObIArray<ObBatchEstTasks *> &tasks,
                                     const ObAddr &addr,
                                     ObBatchEstTasks *&task)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    if (OB_ISNULL(tasks.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid batch estimation task", K(ret));
    } else if (tasks.at(i)->addr_ == addr) {
      task = tasks.at(i);
    }
  }
  return ret;
}

int64_t ObAccessPathEstimation::get_get_range_count(const ObIArray<ObNewRange> &ranges)
{
  int64_t ret = 0;
  for (int64_t i = 0; i < ranges.count(); ++i) {
    if (ranges.at(i).is_single_rowkey()) {
      ++ ret;
    }
  }
  return ret;
}

int64_t ObAccessPathEstimation::get_scan_range_count(const ObIArray<ObNewRange> &ranges)
{
  int64_t ret = 0;
  for (int64_t i = 0; i < ranges.count(); ++i) {
    if (!ranges.at(i).is_single_rowkey()) {
      ++ ret;
    }
  }
   return ret;
}

int ObAccessPathEstimation::construct_scan_range_batch(ObIAllocator &allocator,
                                                       const ObIArray<ObNewRange> &scan_ranges,
                                                       ObSimpleBatch &batch)
{
  int ret = OB_SUCCESS;
  // FIXME, consider the lifetime of ObSimpleBatch, how to deconstruct the ObSEArray
  if (scan_ranges.empty()) {
    batch.type_ = ObSimpleBatch::T_NONE;
  } else if (scan_ranges.count() == 1) { //T_SCAN
    void *ptr = allocator.alloc(sizeof(SQLScanRange));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      SQLScanRange *range = new(ptr)SQLScanRange();
      *range = scan_ranges.at(0);
      batch.type_ = ObSimpleBatch::T_SCAN;
      batch.range_ = range;
    }
  } else { //T_MULTI_SCAN
    SQLScanRangeArray *range_array = NULL;
    void *ptr = allocator.alloc(sizeof(SQLScanRangeArray));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      range_array = new(ptr)SQLScanRangeArray();
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = range_array;
      int64_t size = std::min(scan_ranges.count(),
                              ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        if (OB_FAIL(range_array->push_back(scan_ranges.at(i)))) {
          LOG_WARN("failed to push back scan range", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObBatchEstTasks::check_result_reliable() const
{
  bool bret = paths_.count() == res_.index_param_res_.count();
  for (int64_t i = 0; bret && i < paths_.count(); ++i) {
    bret = res_.index_param_res_.at(i).reliable_;
    if (bret && NULL != paths_.at(i)) {
      if (paths_.at(i)->is_global_index_ &&
          paths_.at(i)->est_cost_info_.ranges_.count() == 1 &&
          paths_.at(i)->est_cost_info_.ranges_.at(0).is_whole_range() &&
          res_.index_param_res_.at(i).logical_row_count_ == 0) {
        bret = false;
      }
    }
  }
  return bret;
}

int ObAccessPathEstimation::estimate_full_table_rowcount(ObOptimizerContext &ctx,
                                                         const ObTablePartitionInfo &table_part_info,
                                                         ObTableMetaInfo &meta)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 1> prefer_addrs;
  EstimatedPartition best_index_part;
  ObArenaAllocator arena("CardEstimation");
  bool force_leader_estimation = false;
  
  force_leader_estimation = OB_FAIL(E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);
  ret = OB_SUCCESS;
  
  HEAP_VAR(ObBatchEstTasks, task) {
    obrpc::ObEstPartArg &arg = task.arg_;
    obrpc::ObEstPartRes &res = task.res_;

    arg.schema_version_ = meta.schema_version_;
    if (OB_ISNULL(ctx.get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (is_virtual_table(meta.ref_table_id_) &&
        !share::is_oracle_mapping_real_virtual_table(meta.ref_table_id_)) {
      // do nothing
    } else if (OB_FAIL(ObSQLUtils::choose_best_replica_for_estimation(
                table_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list(),
                ctx.get_local_server_addr(),
                prefer_addrs,
                false,
                best_index_part))) {
      LOG_WARN("failed to choose best partition", K(ret));
    } else if (force_leader_estimation && 
               OB_FAIL(choose_leader_replica(table_part_info,
                                             true,
                                             ctx.get_local_server_addr(),
                                             best_index_part))) {
      LOG_WARN("failed to choose leader replica", K(ret));
    } else if (best_index_part.is_valid()) {
      obrpc::ObEstPartArgElement path_arg;
      ObNewRange *range = NULL;

      task.addr_ = best_index_part.addr_;
      path_arg.scan_flag_.index_back_ = 0;
      path_arg.scan_flag_.disable_cache();
      path_arg.index_id_ = meta.ref_table_id_;
      path_arg.range_columns_count_ = meta.table_rowkey_count_;
      path_arg.batch_.type_ = ObSimpleBatch::T_SCAN;
      path_arg.tablet_id_ = best_index_part.tablet_id_;
      path_arg.ls_id_ = best_index_part.ls_id_;
      path_arg.tenant_id_ = ctx.get_session_info()->get_effective_tenant_id();
      if (OB_FAIL(ObSQLUtils::make_whole_range(arena,
                                               meta.ref_table_id_,
                                               meta.table_rowkey_count_,
                                               range))) {
        LOG_WARN("failed to make whole range", K(ret));
      } else if (OB_ISNULL(path_arg.batch_.range_ = range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate whole range", K(ret), K(range));
      } else if (OB_FAIL(arg.index_params_.push_back(path_arg))) {
        LOG_WARN("failed to add primary key estimation arg", K(ret));
      } else if (OB_FAIL(do_storage_estimation(ctx, task))) {
        LOG_WARN("failed to do storage estimation", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(res.index_param_res_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage estimation result size is unexpected", K(ret));
      } else if (res.index_param_res_.at(0).reliable_) {
        int64_t logical_row_count = res.index_param_res_.at(0).logical_row_count_;
        int64_t part_count = table_part_info.get_phy_tbl_location_info().get_partition_cnt();
        meta.table_row_count_ = logical_row_count * part_count;
        meta.average_row_size_ = static_cast<double>(ObOptStatManager::get_default_avg_row_size());
        meta.part_size_ = logical_row_count * meta.average_row_size_;
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::get_key_ranges(ObOptimizerContext &ctx,
                                           ObIAllocator &allocator,
                                           const ObTabletID &tablet_id,
                                           AccessPath *ap,
                                           ObIArray<common::ObNewRange> &new_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ap));
  } else if (OB_FAIL(new_ranges.assign(ap->est_cost_info_.ranges_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (!share::is_oracle_mapping_real_virtual_table(ap->ref_table_id_)) {
    //do nothing
  } else if (OB_FAIL(convert_agent_vt_key_ranges(ctx, allocator, ap, new_ranges))) {
    LOG_WARN("failed to convert agent vt key ranges", K(ret));
  } else {/*do nothing*/}
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_physical_rowid_ranges(ctx, allocator, tablet_id,
                                              ap->index_id_, new_ranges))) {
      LOG_WARN("failed to convert physical rowid ranges", K(ret));
    } else {
      LOG_TRACE("Succeed to get key ranges", K(new_ranges));
    }
  }
  return ret;
}

int ObAccessPathEstimation::convert_agent_vt_key_ranges(ObOptimizerContext &ctx,
                                                        ObIAllocator &allocator,
                                                        AccessPath *ap,
                                                        ObIArray<common::ObNewRange> &new_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ap) || !share::is_oracle_mapping_real_virtual_table(ap->ref_table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(ap));
  } else {
    void *buf = NULL;
    uint64_t vt_table_id = ap->ref_table_id_;
    uint64_t real_table_id = ObSchemaUtils::get_real_table_mappings_tid(ap->ref_table_id_);
    ObSqlSchemaGuard *schema_guard = NULL;
    const ObTableSchema *vt_table_schema = NULL;
    const ObTableSchema *real_table_schema = NULL;
    if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObVirtualTableResultConverter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret), K(buf));
    } else if (OB_FAIL(schema_guard->get_table_schema(vt_table_id, vt_table_schema)) ||
               OB_FAIL(schema_guard->get_table_schema(real_table_id, real_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(vt_table_schema) || OB_ISNULL(real_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(vt_table_schema), K(real_table_schema));
    } else {
      ObVirtualTableResultConverter *vt_converter = new (buf) ObVirtualTableResultConverter();
      ObSEArray<ObObjMeta, 4> key_types;
      bool has_tenant_id_col = false;
      if (OB_FAIL(gen_agent_vt_table_convert_info(vt_table_id,
                                                  vt_table_schema,
                                                  real_table_schema,
                                                  ap->est_cost_info_.range_columns_,
                                                  has_tenant_id_col,
                                                  key_types))) {
        LOG_WARN("failed to gen agent vt table convert info", K(ret));
      } else if (OB_FAIL(vt_converter->init_convert_key_ranges_info(&allocator,
                                                                    ctx.get_session_info(),
                                                                    vt_table_schema,
                                                                    &key_types,
                                                                    has_tenant_id_col))) {
        LOG_WARN("failed to init convert key ranges info", K(ret));
      } else if (OB_FAIL(vt_converter->convert_key_ranges(new_ranges))) {
        LOG_WARN("convert key ranges failed", K(ret));
      } else {
        LOG_TRACE("succeed to convert agent vt key ranges", K(new_ranges));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::gen_agent_vt_table_convert_info(const uint64_t vt_table_id,
                                                            const ObTableSchema *vt_table_schema,
                                                            const ObTableSchema *real_table_schema,
                                                            const ObIArray<ColumnItem> &range_columns,
                                                            bool &has_tenant_id_col,
                                                            ObIArray<ObObjMeta> &key_types)
{
  int ret = OB_SUCCESS;
  VTMapping *vt_mapping = NULL;
  has_tenant_id_col = false;
  key_types.reset();
  get_real_table_vt_mapping(vt_table_id, vt_mapping);
  if (OB_ISNULL(vt_table_schema) || OB_ISNULL(real_table_schema) || OB_ISNULL(vt_mapping)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(vt_table_schema), K(real_table_schema));
  } else {
    // set vt has tenant_id column
    for (int64_t i = 0;
         OB_SUCC(ret) && !has_tenant_id_col && i < real_table_schema->get_column_count();
         ++i) {
      const ObColumnSchemaV2 *col_schema = real_table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else if (0 == col_schema->get_column_name_str().case_compare("TENANT_ID")) {
        has_tenant_id_col = true;
      }
    }
    //set key types
    for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count() ; ++i) {
      bool find_it = false;
      const uint64_t range_column_id = range_columns.at(i).column_id_;
      const ObColumnSchemaV2 *vt_col_schema = vt_table_schema->get_column_schema(range_column_id);
      if (OB_ISNULL(vt_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(range_column_id), K(ret));
      } else if (has_tenant_id_col && 0 == i &&
                 0 != vt_col_schema->get_column_name_str().case_compare("TENANT_ID")) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(range_column_id), K(ret), K(i), K(has_tenant_id_col));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !find_it && j < real_table_schema->get_column_count(); ++j) {
          const ObColumnSchemaV2 *col_schema = real_table_schema->get_column_schema_by_idx(j);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(column_id));
          } else if (0 == col_schema->get_column_name_str().case_compare(vt_col_schema->get_column_name_str())) {
            find_it = true;
            ObObjMeta obj_meta;
            obj_meta.set_type(col_schema->get_data_type());
            obj_meta.set_collation_type(col_schema->get_collation_type());
            if (OB_FAIL(key_types.push_back(obj_meta))) {
              LOG_WARN("failed to push back", K(ret));
            } else {
              //do nothing
            }
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(!find_it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: column not found", K(ret), K(range_column_id), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(range_columns.count() != key_types.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key is not match", K(range_columns.count()), K(key_types.count()),
                                   K(range_columns), K(key_types), K(ret));
    }
  }
  return ret;
}

int ObAccessPathEstimation::convert_physical_rowid_ranges(ObOptimizerContext &ctx,
                                                          ObIAllocator &allocator,
                                                          const ObTabletID &tablet_id,
                                                          const uint64_t index_id,
                                                          ObIArray<common::ObNewRange> &new_ranges)
{
  int ret = OB_SUCCESS;
  bool is_gen_pk = false;
  ObSEArray<ObColDesc, 4> rowkey_cols;
  for (int64_t i = 0; OB_SUCC(ret) && i < new_ranges.count(); ++i) {
    if (new_ranges.at(i).is_physical_rowid_range_) {
      if (rowkey_cols.empty()) {
        ObSqlSchemaGuard *schema_guard = NULL;
        const ObTableSchema *table_schema = NULL;
        if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(table_schema));
        } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_cols))) {
          LOG_WARN("failed to get pk col ids", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObArrayWrap<ObColDesc> rowkey_descs(&rowkey_cols.at(0), rowkey_cols.count());
        if (OB_FAIL(ObTableScanOp::transform_physical_rowid(allocator,
                                                            tablet_id,
                                                            rowkey_descs,
                                                            new_ranges.at(i)))) {
          LOG_WARN("transform physical rowid for range failed", K(ret));
        } else {
          LOG_TRACE("Succeed to transform physical rowid", K(new_ranges.at(i)));
        }
      }
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase

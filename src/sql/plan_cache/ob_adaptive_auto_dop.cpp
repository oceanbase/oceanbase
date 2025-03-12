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
#include "ob_adaptive_auto_dop.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/optimizer/ob_access_path_estimation.h"
#include "sql/optimizer/ob_storage_estimator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
int ObAdaptiveAutoDop::calculate_table_auto_dop(const ObPhysicalPlan &plan, AutoDopHashMap &map,
                                                bool &is_single_part)
{
  int ret = OB_SUCCESS;
  is_single_part = false;
  int64_t table_dop = -1;
  ObMemAttr attr(MTL_ID(), "AutoDopMap");
  const ObOpSpec *root_spec = plan.get_root_op_spec();
  if (OB_ISNULL(root_spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root spec is null", K(ret));
  } else if (map.created()) {
    map.clear();
  } else if (OB_FAIL(map.create(common::hash::cal_next_prime(256), attr, attr))) {
    LOG_WARN("create hash map failed", K(ret));
  }
  if (OB_SUCC(ret)
      && OB_FAIL(inner_calculate_table_auto_dop(*root_spec, map, table_dop, is_single_part))) {
    LOG_WARN("failed to inner calculate table auto dop", K(ret));
  }
  if (OB_FAIL(ret)) {
    map.clear();
  }
  return ret;
}

int ObAdaptiveAutoDop::inner_calculate_table_auto_dop(const ObOpSpec &spec, AutoDopHashMap &map,
                                                      int64_t &table_dop, bool &is_single_part)
{
  int ret = OB_SUCCESS;
  table_dop = -1;
  if (spec.is_table_scan()) {
    if (OB_FAIL(calculate_tsc_auto_dop(spec, table_dop, is_single_part))) {
      LOG_WARN("failed to calculate tsc auto dop", K(ret));
    }
  } else {
    int64_t child_cnt = spec.get_child_cnt();
    for (int64_t i = 0; i < child_cnt; i++) {
      int64_t dop = -1;
      const ObOpSpec *child_spec = spec.get_child(i);
      if (OB_ISNULL(child_spec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("spec is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(
                   inner_calculate_table_auto_dop(*child_spec, map, dop, is_single_part)))) {
        LOG_WARN("failed to recalulate table dop", K(ret));
      } else {
        // currently only supports single table, non-table_scan operator, dop directly
        // inherits from the child
        table_dop = table_dop > dop ? table_dop : dop;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(map.set_refactored(spec.get_id(), table_dop))) {
    LOG_WARN("set refactored failed", K(ret), K(spec.get_id()), K(table_dop));
  }
  return ret;
}

int ObAdaptiveAutoDop::calculate_tsc_auto_dop(const ObOpSpec &spec, int64_t &table_dop,
                                              bool &is_single_part)
{
  int ret = OB_SUCCESS;
  table_dop = -1;
  int64_t part_cnt = 0;
  bool res_reliable = true;
  ObArray<ObBatchEstTasks *> tasks;
  common::ObIAllocator &allocator = ctx_.get_allocator();
  const ObTableScanSpec &tsc_spec = static_cast<const ObTableScanSpec &>(spec);
  const ObCostTableScanSimpleInfo &cost_tsc_info = tsc_spec.get_est_cost_simple_info();
  bool is_oracle_agent_table =
    share::is_oracle_mapping_real_virtual_table(tsc_spec.get_ref_table_id());
  ObQueryRangeArray key_ranges;
  const ObQueryRangeProvider &query_range_provider = tsc_spec.get_query_range_provider();

  if (is_oracle_agent_table || cost_tsc_info.get_is_spatial_index()) {
    // step1: calculate query range.
  } else if (query_range_provider.has_range()
             && OB_FAIL(ObSQLUtils::extract_pre_query_range(
                  query_range_provider, ctx_.get_allocator(), ctx_, key_ranges,
                  ObBasicSessionInfo::create_dtc_params(ctx_.get_my_session())))) {
    LOG_WARN("failed to extract pre query ranges", K(ret));
    // step2: build est tasks.
  } else if (OB_FAIL(build_storage_estimation_tasks(tsc_spec, cost_tsc_info, key_ranges, tasks,
                                                    is_single_part, part_cnt))) {
    LOG_WARN("failed to build storage estimation tasks", K(ret));
    // step3: do storeage estimation.
  } else if (OB_FAIL(do_storage_estimation(tasks, res_reliable))) {
    LOG_WARN("failed to do storage estimation", K(ret));
    // step4: calculate tsc auto dop
  } else if (res_reliable && tasks.count() > 0
             && OB_FAIL(calculate_tsc_auto_dop(tasks, cost_tsc_info, part_cnt, table_dop))) {
    LOG_WARN("failed to calculate table dop", K(ret));
  }
  return ret;
}

int ObAdaptiveAutoDop::choose_storage_estimation_partitions(const int64_t partition_limit,
                                                            const DASTabletLocSEArray &tablet_locs,
                                                            DASTabletLocSEArray &chosen_tablet_locs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> min_max_index;
  int64_t min_index = 0;
  int64_t max_index = 0;
  const int STORAGE_EST_SAMPLE_SEED = 1;
  if (partition_limit <= 0 || partition_limit >= tablet_locs.count()) {
    if (OB_FAIL(chosen_tablet_locs.assign(tablet_locs))) {
      LOG_WARN("failed to assign", K(ret));
    }
  } else {
    for (int64_t i = 1; i < tablet_locs.count(); i ++) {
      if (tablet_locs.at(i)->tablet_id_.id() <
          tablet_locs.at(min_index)->tablet_id_.id()) {
        min_index = i;
      }
      if (tablet_locs.at(i)->tablet_id_.id() >
          tablet_locs.at(max_index)->tablet_id_.id()) {
        max_index = i;
      }
    }
    if (OB_FAIL(min_max_index.add_member(min_index)) ||
        OB_FAIL(min_max_index.add_member(max_index))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::choose_random_members(
                          STORAGE_EST_SAMPLE_SEED, tablet_locs, partition_limit,
                          chosen_tablet_locs, &min_max_index))) {
      LOG_WARN("failed to choose random partitions", K(ret), K(partition_limit));
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::build_storage_estimation_tasks(
  const ObTableScanSpec &tsc_spec, const ObCostTableScanSimpleInfo &cost_tsc_info,
  ObQueryRangeArray &ranges, ObIArray<ObBatchEstTasks *> &tasks, bool &is_single_part,
  int64_t &part_cnt)
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *match_table_loc = nullptr;
  ObDASCtx &das_ctx = ctx_.get_das_ctx();
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  ObSqlCtx *sql_ctx = ctx_.get_sql_ctx();
  common::ObIAllocator &allocator = ctx_.get_allocator();
  const DASTableLocList &table_locs = das_ctx.get_table_loc_list();
  const int64_t index_id = cost_tsc_info.get_index_id();
  const int64_t MAX_EST_TASK_NUM = 10;

  FOREACH(table_loc, table_locs)
  {
    if (index_id == (*table_loc)->get_ref_table_id()) {
      match_table_loc = (*table_loc);
      break;
    }
  }
  if (OB_ISNULL(sql_ctx) || OB_ISNULL(match_table_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(ret), K(sql_ctx), K(match_table_loc));
  } else if (OB_FAIL(
               sql_ctx->schema_guard_->get_simple_table_schema(MTL_ID(), index_id, table_schema))) {
    LOG_WARN("failed to get simple table schema", K(ret), K(MTL_ID()), K(index_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null schema", K(ret));
  } else {
    const DASTabletLocList &tablet_locs = match_table_loc->get_tablet_locs();
    is_single_part = (1 == tablet_locs.size());
    part_cnt = tablet_locs.size();
    int64_t schema_version = table_schema->get_schema_version();
    DASTabletLocSEArray candi_tablet_locs;
    DASTabletLocSEArray chosen_tablet_locs;
    if (OB_FAIL(candi_tablet_locs.reserve(tablet_locs.size()))) {
      LOG_WARN("failed to reserve", K(tablet_locs.size()));
    } else {
      FOREACH_X(tablet_loc, tablet_locs, OB_SUCC(ret))
      {
        if (OB_FAIL(candi_tablet_locs.push_back(*tablet_loc))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(choose_storage_estimation_partitions(MAX_EST_TASK_NUM, candi_tablet_locs,
                                                            chosen_tablet_locs))) {
      LOG_WARN("failto to choose storage est partitions", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < chosen_tablet_locs.count(); i++) {
        if (OB_FAIL(add_estimation_tasks(tsc_spec, cost_tsc_info, schema_version,
                                         chosen_tablet_locs.at(i), ranges, tasks))) {
          LOG_WARN("failed to add estimation tasks", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::get_task(ObIArray<ObBatchEstTasks *> &tasks, const ObAddr &addr,
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

int ObAdaptiveAutoDop::add_estimation_tasks(const ObTableScanSpec &tsc_spec,
                                            const ObCostTableScanSimpleInfo &cost_tsc_info,
                                            const int64_t schema_version,
                                            ObDASTabletLoc *tablet_loc, ObQueryRangeArray &ranges,
                                            ObIArray<ObBatchEstTasks *> &tasks)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  ObBatchEstTasks *task = nullptr;
  ObSqlCtx *sql_ctx = ctx_.get_sql_ctx();
  common::ObIAllocator &allocator = ctx_.get_allocator();
  obrpc::ObEstPartArgElement *index_est_arg = NULL;
  if (OB_FAIL(get_task(tasks, tablet_loc->server_, task))) {
    LOG_WARN("failed to get task", K(ret));
  } else if (NULL != task) {
    // do nothing
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObBatchEstTasks)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret));
  } else if (FALSE_IT(task = new (ptr) ObBatchEstTasks())) {
  } else if (OB_FAIL(tasks.push_back(task))) {
    LOG_WARN("failed to add task", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_est_arg = task->arg_.index_params_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate index argument", K(ret));
  } else {
    task->addr_ = tablet_loc->server_;
    task->arg_.schema_version_ = schema_version;
    index_est_arg->index_id_ = cost_tsc_info.get_index_id();
    index_est_arg->scan_flag_ = tsc_spec.get_query_flag();
    index_est_arg->range_columns_count_ = cost_tsc_info.get_range_columns_count();
    index_est_arg->tablet_id_ = tablet_loc->tablet_id_;
    index_est_arg->ls_id_ = tablet_loc->ls_id_;
    index_est_arg->tenant_id_ = MTL_ID();
    index_est_arg->tx_id_ = sql_ctx->session_info_->get_tx_id();
    if (OB_FAIL(construct_scan_range_batch(allocator, ranges, index_est_arg->batch_))) {
      LOG_WARN("failed to construct scan range batch", K(ret));
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::construct_scan_range_batch(ObIAllocator &allocator,
                                                  const ObQueryRangeArray &scan_ranges,
                                                  ObSimpleBatch &batch)
{
  int ret = OB_SUCCESS;
  // FIXME, consider the lifetime of ObSimpleBatch, how to deconstruct the ObSEArray
  if (scan_ranges.empty()) {
    batch.type_ = ObSimpleBatch::T_NONE;
  } else if (scan_ranges.count() == 1) { // T_SCAN
    void *ptr = allocator.alloc(sizeof(SQLScanRange));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      SQLScanRange *range = new (ptr) SQLScanRange();
      *range = *(scan_ranges.at(0));
      batch.type_ = ObSimpleBatch::T_SCAN;
      batch.range_ = range;
    }
  } else { // T_MULTI_SCAN
    SQLScanRangeArray *range_array = NULL;
    void *ptr = allocator.alloc(sizeof(SQLScanRangeArray));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      range_array = new (ptr) SQLScanRangeArray();
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = range_array;
      int64_t size = std::min(scan_ranges.count(), ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        if (OB_FAIL(range_array->push_back(*(scan_ranges.at(i))))) {
          LOG_WARN("failed to push back scan range", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::do_storage_estimation(ObIArray<ObBatchEstTasks *> &tasks, bool &res_reliable)
{
  int ret = OB_SUCCESS;
  res_reliable = true;
  for (int64_t i = 0; OB_SUCC(ret) && res_reliable && i < tasks.count(); ++i) {
    if (OB_ISNULL(tasks.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", K(ret));
    } else if (OB_FAIL(do_storage_estimation(*tasks.at(i)))) {
      res_reliable = false;
      LOG_WARN("failed to do storage estimation", K(ret));
      break;
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::do_storage_estimation(ObBatchEstTasks &tasks)
{
  int ret = OB_SUCCESS;
  const ObAddr &addr = tasks.addr_;
  const obrpc::ObEstPartArg &arg = tasks.arg_;
  obrpc::ObEstPartRes &result = tasks.res_;
  ObSqlCtx *sql_ctx = ctx_.get_sql_ctx();
  if (addr == GCTX.self_addr()) {
    if (OB_FAIL(ObStorageEstimator::estimate_row_count(arg, result))) {
      LOG_WARN("failed to estimate partition rows", K(ret));
    }
  } else {
    obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
    int64_t timeout = -1;
    if (OB_ISNULL(sql_ctx->session_info_) || OB_ISNULL(rpc_proxy)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rpc_proxy or session is null", K(ret), K(rpc_proxy), K(sql_ctx->session_info_));
    } else if (0 >= (timeout = THIS_WORKER.get_timeout_remain())) {
      ret = OB_TIMEOUT;
      LOG_WARN("query timeout is reached", K(ret), K(timeout));
    } else if (OB_FAIL(rpc_proxy->to(addr)
                         .timeout(timeout)
                         .by(sql_ctx->session_info_->get_rpc_tenant_id())
                         .estimate_partition_rows(arg, result))) {
      LOG_WARN("OPT:[REMOTE STORAGE EST FAILED]", K(ret));
    }
  }
  return ret;
}

int ObAdaptiveAutoDop::calculate_tsc_auto_dop(const ObIArray<ObBatchEstTasks *> &tasks,
                                              const ObCostTableScanSimpleInfo &cost_tsc_info,
                                              int64_t part_cnt, int64_t &table_dop)
{
  int ret = OB_SUCCESS;
  table_dop = -1;
  int64_t range_row_count = 0;
  ObSqlCtx *sql_ctx = ctx_.get_sql_ctx();
  uint64_t parallel_degree_limit = 0;
  uint64_t parallel_min_scan_time_threshold = 1000;
  uint64_t parallel_servers_target = 0;
  uint64_t unit_min_cpu = 0;
  uint64_t cost_threshold_us = 0;
  const int64_t MAX_EST_TASK_NUM = 10;
  for (int64_t i = 0; i < tasks.count(); ++i) {
    const ObBatchEstTasks *task = tasks.at(i);
    for (int64_t j = 0; j < task->res_.index_param_res_.count(); ++j) {
      range_row_count += task->res_.index_param_res_.at(j).logical_row_count_;
    }
  }
  if (part_cnt > MAX_EST_TASK_NUM) {
    range_row_count = range_row_count / MAX_EST_TASK_NUM * part_cnt;
  }
  if (OB_FAIL(sql_ctx->session_info_->get_sys_variable(share::SYS_VAR_PARALLEL_DEGREE_LIMIT,
                                                       parallel_degree_limit))) {
    LOG_WARN("failed to get sys variable parallel degree limit", K(ret));
  } else if (OB_FAIL(sql_ctx->session_info_->get_sys_variable(
               share::SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD,
               parallel_min_scan_time_threshold))) {
    LOG_WARN("failed to get sys variable parallel threshold", K(ret));
  }
  if (OB_SUCC(ret) && 0 == parallel_degree_limit) {
    const ObTenantBase *tenant = NULL;
    int64_t parallel_servers_target = 0;
    if (OB_ISNULL(tenant = MTL_CTX())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (sql_ctx->session_info_->is_user_session()
               && OB_FAIL(ObSchemaUtils::get_tenant_int_variable(
                    sql_ctx->session_info_->get_effective_tenant_id(),
                    SYS_VAR_PARALLEL_SERVERS_TARGET, parallel_servers_target))) {
      LOG_WARN("fail read tenant variable", K(ret),
               K(sql_ctx->session_info_->get_effective_tenant_id()));
    } else {
      unit_min_cpu = std::max(tenant->unit_min_cpu(), 0.0);
      parallel_servers_target = std::max(parallel_servers_target, 0L);
    }
  }
  if (OB_SUCC(ret)) {
    int64_t server_cnt = 1;
    if (0 < parallel_degree_limit) {
      // do nothing
    } else if (0 >= parallel_servers_target || 0 >= unit_min_cpu || 0 >= server_cnt) {
      parallel_degree_limit = std::max(parallel_servers_target, server_cnt * unit_min_cpu);
    } else {
      parallel_degree_limit = std::min(parallel_servers_target, server_cnt * unit_min_cpu);
    }
    cost_threshold_us = 1000.0 * std::max(10UL, parallel_min_scan_time_threshold);
    parallel_degree_limit = std::max(parallel_degree_limit, 1UL);
  }
  BEGIN_OPT_TRACE(sql_ctx->session_info_, "");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cost_tsc_info.calculate_table_dop(range_row_count, range_row_count, part_cnt,
                                                       cost_threshold_us, parallel_degree_limit,
                                                       table_dop))) {
    LOG_WARN("failed to calculate table dop", K(ret));
  }
  END_OPT_TRACE(sql_ctx->session_info_);
  return ret;
}

} // namespace common
} // namespace oceanbase

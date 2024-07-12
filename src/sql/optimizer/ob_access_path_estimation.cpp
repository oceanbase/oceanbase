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
#include "ob_opt_est_parameter_normal.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "rootserver/ob_root_service.h"
#include "sql/optimizer/ob_sel_estimator.h"
namespace oceanbase {
using namespace share::schema;
using namespace share;
namespace sql {
static const int64_t SPATIAL_ROWKEY_MIN_NUM = 3;

int ObAccessPathEstimation::estimate_rowcount(ObOptimizerContext &ctx,
                                              common::ObIArray<AccessPath *> &paths,
                                              const bool is_inner_path,
                                              const ObIArray<ObRawExpr*> &filter_exprs,
                                              ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  ObSEArray<AccessPath *, 4> normal_paths;
  ObSEArray<AccessPath *, 4> geo_paths;
  ObBaseTableEstMethod geo_method;

  if (OB_UNLIKELY(paths.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_inner_path) {
    if (OB_FAIL(inner_estimate_rowcount(ctx, paths, is_inner_path, filter_exprs, method))) {
      LOG_WARN("failed to do estimate rowcount for paths", K(ret));
    }
  } else if (OB_FAIL(classify_paths(paths, normal_paths, geo_paths))) {
    LOG_WARN("failed to classify paths", K(ret));
  } else if (!normal_paths.empty() &&
             OB_FAIL(inner_estimate_rowcount(ctx, normal_paths, is_inner_path, filter_exprs, method))) {
    LOG_WARN("failed to do estimate rowcount for normal paths", K(ret));
  } else if (!geo_paths.empty() &&
             OB_FAIL(inner_estimate_rowcount(ctx, geo_paths, is_inner_path, filter_exprs, geo_method))) {
    LOG_WARN("failed to do estimate rowcount for geo paths", K(ret));
  } else if (normal_paths.empty() && !geo_paths.empty()) {
    method = geo_method;
  }
  return ret;
}

int ObAccessPathEstimation::inner_estimate_rowcount(ObOptimizerContext &ctx,
                                                    common::ObIArray<AccessPath *> &paths,
                                                    const bool is_inner_path,
                                                    const ObIArray<ObRawExpr*> &filter_exprs,
                                                    ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  ObBaseTableEstMethod valid_methods = 0;
  ObBaseTableEstMethod hint_specify_methods = 0;
  method = EST_INVALID;
  if (OB_FAIL(get_valid_est_methods(ctx, paths, filter_exprs, is_inner_path, valid_methods, hint_specify_methods))) {
    LOG_WARN("failed to get valid est methods", K(ret));
  } else if (OB_FAIL(choose_best_est_method(ctx, paths, filter_exprs,
                                            valid_methods & hint_specify_methods ? valid_methods & hint_specify_methods : valid_methods,
                                            method))) {
    LOG_WARN("failed to choose one est method", K(ret), K(valid_methods));
  } else if (OB_FAIL(do_estimate_rowcount(ctx, paths, is_inner_path, filter_exprs, valid_methods, method))) {
    LOG_WARN("failed to do estimate rowcount", K(ret), K(method), K(valid_methods));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); i ++) {
    if (OB_ISNULL(paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(paths.at(i)));
    } else {
      paths.at(i)->est_cost_info_.est_method_ = method;
    }
  }
  return ret;
}

int ObAccessPathEstimation::do_estimate_rowcount(ObOptimizerContext &ctx,
                                                 common::ObIArray<AccessPath*> &paths,
                                                 const bool is_inner_path,
                                                 const ObIArray<ObRawExpr*> &filter_exprs,
                                                 ObBaseTableEstMethod &valid_methods,
                                                 ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  bool is_success = true;
  LOG_TRACE("Try to do estimate rowcount", K(method), K(is_inner_path));

  if (OB_UNLIKELY(EST_INVALID == method) ||
      OB_UNLIKELY((method & EST_DS_FULL) && (method & EST_DS_BASIC)) ||
      OB_UNLIKELY((method & EST_DEFAULT) && (method & EST_STAT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected est method", K(ret), K(method));
  }

  if (OB_SUCC(ret) && (method & (EST_DS_BASIC | EST_DS_FULL))) {
    bool only_ds_basic_stat = (method & EST_DS_BASIC);
    if (OB_FAIL(process_dynamic_sampling_estimation(
        ctx, paths, is_inner_path, filter_exprs, only_ds_basic_stat, is_success))) {
      LOG_WARN("failed to process statistics estimation", K(ret));
    } else if (!is_success) {
      valid_methods &= ~EST_DS_BASIC;
      valid_methods &= ~EST_DS_FULL;
      if (OB_FAIL(choose_best_est_method(ctx, paths, filter_exprs, valid_methods, method))) {
        LOG_WARN("failed to choose one est method", K(ret), K(valid_methods));
      }
    }
  }

  if (OB_SUCC(ret) && (method & EST_DEFAULT)) {
    if (OB_FAIL(process_table_default_estimation(ctx, paths))) {
      LOG_WARN("failed to process table default estimation", K(ret));
    }
  }

  if (OB_SUCC(ret) && (method & EST_STAT)) {
    if (OB_FAIL(process_statistics_estimation(paths))) {
      LOG_WARN("failed to process statistics estimation", K(ret));
    }
  }

  if (OB_SUCC(ret) && (method & EST_STORAGE)) {
    if (OB_FAIL(process_storage_estimation(ctx, paths, is_success))) {
      LOG_WARN("failed to process storage estimation", K(ret));
    } else if (!is_success) {
      // The failure of storage estimation will not affect the result of statistics estimation
      method &= ~EST_STORAGE;
    }
  }
  return ret;
}

int ObAccessPathEstimation::get_valid_est_methods(ObOptimizerContext &ctx,
                                                  common::ObIArray<AccessPath*> &paths,
                                                  const ObIArray<ObRawExpr*> &filter_exprs,
                                                  bool is_inner_path,
                                                  ObBaseTableEstMethod &valid_methods,
                                                  ObBaseTableEstMethod &hint_specify_methods)
{
  int ret = OB_SUCCESS;
  valid_methods = EST_DEFAULT | EST_STAT | EST_STORAGE | EST_DS_BASIC | EST_DS_FULL;
  hint_specify_methods = 0;
  const ObBaseTableEstMethod EST_DS_METHODS =  EST_DS_BASIC | EST_DS_FULL;
  const ObLogPlan* log_plan = NULL;
  const OptTableMeta *table_meta = NULL;
  if (OB_UNLIKELY(paths.empty()) ||
      OB_ISNULL(paths.at(0)->parent_) ||
      OB_ISNULL(log_plan = paths.at(0)->parent_->get_plan()) ||
      OB_ISNULL(log_plan->get_stmt()) ||
      FALSE_IT(table_meta = log_plan->get_basic_table_metas().get_table_meta_by_table_id(paths.at(0)->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan));
  } else if (ctx.use_default_stat()) {
    valid_methods = EST_DEFAULT;
  } else {
    // some basic check
    share::schema::ObTableType table_type = paths.at(0)->est_cost_info_.table_meta_info_->table_type_;
    uint64_t ref_table_id = paths.at(0)->ref_table_id_;
    if (is_inner_path) {
      valid_methods &= ~EST_STORAGE;
      valid_methods &= ~EST_DS_FULL;
    }
    if (!log_plan->get_stmt()->is_select_stmt()) {
      valid_methods &= ~EST_DS_METHODS;
    }
    if (OB_ISNULL(table_meta) ||
        OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
      valid_methods &= ~EST_STAT;
      valid_methods &= ~EST_DS_METHODS;
    } else if (!table_meta->use_opt_stat()) {
      valid_methods &= ~EST_STAT;
    } else if (table_meta->is_stat_locked()) {
      valid_methods &= ~EST_STORAGE;
    }
    if (table_type == EXTERNAL_TABLE) {
      // TODO [EXTERNAL TABLE]
      valid_methods &= ~EST_STORAGE;
      valid_methods &= ~EST_DS_METHODS;
    }
    if (table_type == MATERIALIZED_VIEW) {
      // TODO [MATERIALIZED TABLE]
      valid_methods &= ~EST_STORAGE;
      valid_methods &= ~EST_DS_METHODS;
    }
    if (is_virtual_table(ref_table_id)) {
      if (!ObDynamicSamplingUtils::is_ds_virtual_table(ref_table_id)) {
        valid_methods &= ~EST_DS_METHODS;
      }
      if (!share::is_oracle_mapping_real_virtual_table(ref_table_id)) {
        valid_methods &= ~EST_STORAGE;
      }
    }
  }

  // check use storage estimation
  bool use_storage_est = (valid_methods | EST_STORAGE);
  for (int64_t i = 0; OB_SUCC(ret) && use_storage_est && i < paths.count(); i ++) {
    AccessPath *path = paths.at(i);
    const ObTablePartitionInfo *part_info = NULL;
    if (OB_FAIL(check_path_can_use_storage_estimation(path, use_storage_est, ctx))) {
      LOG_WARN("failed to check use storage est", K(ret), KPC(path));
    }
  }
  if (OB_SUCC(ret) && !use_storage_est) {
    valid_methods &= ~EST_STORAGE;
  }

  // check dynamic sampling
  if (OB_SUCC(ret) && (valid_methods & EST_DS_METHODS) &&
      OB_FAIL(check_can_use_dynamic_sampling(
          ctx, *log_plan, *table_meta, filter_exprs, valid_methods, hint_specify_methods))) {
    LOG_WARN("failed to check dynamic sampling", K(ret));
  }

  return ret;
}

int ObAccessPathEstimation::check_can_use_dynamic_sampling(ObOptimizerContext &ctx,
                                                           const ObLogPlan &log_plan,
                                                           const OptTableMeta &table_meta,
                                                           const ObIArray<ObRawExpr*> &filter_exprs,
                                                           ObBaseTableEstMethod &valid_methods,
                                                           ObBaseTableEstMethod &specify_methods)
{
  int ret = OB_SUCCESS;
  int64_t ds_level = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
  int64_t sample_block_cnt = 0;
  bool specify_ds = false;
  bool has_invalid_ds_filters = false;
  const ObBaseTableEstMethod EST_DS_METHODS =  EST_DS_BASIC | EST_DS_FULL;
  if (OB_FAIL(ObDynamicSamplingUtils::get_valid_dynamic_sampling_level(
      ctx.get_session_info(),
      log_plan.get_log_plan_hint().get_dynamic_sampling_hint(table_meta.get_table_id()),
      ctx.get_global_hint().get_dynamic_sampling(),
      ds_level,
      sample_block_cnt,
      specify_ds))) {
    LOG_WARN("failed to get valid dynamic sampling level", K(ret));
  } else if (ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING == ds_level ||
             ObDynamicSamplingUtils::get_dynamic_sampling_max_timeout(ctx) <= 0) {
    valid_methods &= ~EST_DS_METHODS;
  } else if (ObDynamicSamplingUtils::check_is_failed_ds_table(table_meta.get_ref_table_id(),
                                                              table_meta.get_all_used_parts(),
                                                              ctx.get_failed_ds_tab_list())) {
    valid_methods &= ~EST_DS_METHODS;
    LOG_TRACE("get failed ds table, not use dynamic sampling", K(table_meta), K(ctx.get_failed_ds_tab_list()));
  } else if (OB_FAIL(ObDynamicSamplingUtils::check_ds_can_use_filters(filter_exprs, has_invalid_ds_filters))) {
    LOG_WARN("failed to check ds can use filters", K(ret));
  } else if (has_invalid_ds_filters) {
    valid_methods &= ~EST_DS_FULL;
  } else {
    valid_methods &= ~EST_DS_BASIC;
  }
  if (OB_SUCC(ret) && specify_ds) {
    specify_methods |= EST_DS_METHODS;
  }
  return ret;
}

int ObAccessPathEstimation::choose_best_est_method(ObOptimizerContext &ctx,
                                                   common::ObIArray<AccessPath*> &paths,
                                                   const ObIArray<ObRawExpr*> &filter_exprs,
                                                   const ObBaseTableEstMethod &valid_methods,
                                                   ObBaseTableEstMethod& method)
{
  int ret = OB_SUCCESS;
 /**
  * There are seven est methods:
  * 1. EST_DS_FULL: Dynamic sampling collects basic statistics and the final rowcount.
  * 2. EST_DS_STORAGE: Dynamic sampling collects basic statistics and deduces the final rowcount with storage layer estimation.
  * 3. EST_DS_BASIC: Dynamic sampling collects basic statistics to deduce the final rowcount.
  * 4. EST_STORAGE_STAT: Use the storage layer and collected statistics to estimate rows.
  * 5. EST_STORAGE_DEFAULT: Use the storage layer and default statistics to estimate rows.
  * 6. EST_STAT: Use collected statistics to estimate rows.
  * 7. EST_DEFAULT: Use default statistics to estimate rows.
  *
  * We prioritize them based on three different scenarios:
  * 1. Normal scene: STORAGE > STAT > DYNAMIC SAMPLING > DEFAULT
  * 2. Simple scene (DYNAMIC SAMPLING is not helpful for final rowcount): lower the priority of dynamic sampling because of its high cost.
  * 3. Complex scene (with complex predicates): higher the priority of dynamic sampling because it is more accurate.
 */

  static const int64_t priority_cnt = 7;
  static const ObBaseTableEstMethod EST_STORAGE_DEFAULT = EST_STORAGE | EST_DEFAULT;
  static const ObBaseTableEstMethod EST_STORAGE_STAT = EST_STORAGE | EST_STAT;
  static const ObBaseTableEstMethod EST_DS_STORAGE = EST_DS_BASIC | EST_STORAGE;
  static const ObBaseTableEstMethod complex_est_priority[priority_cnt] =
      {EST_DS_FULL, EST_STORAGE_STAT, EST_STAT, EST_DS_STORAGE, EST_DS_BASIC, EST_STORAGE_DEFAULT, EST_DEFAULT};
  static const ObBaseTableEstMethod simple_est_priority[priority_cnt]  =
      {EST_STORAGE_STAT, EST_STAT, EST_STORAGE_DEFAULT, EST_DEFAULT, EST_DS_FULL, EST_DS_STORAGE, EST_DS_BASIC};
  static const ObBaseTableEstMethod default_est_priority[priority_cnt] =
      {EST_STORAGE_STAT, EST_STAT, EST_DS_FULL, EST_DS_STORAGE, EST_DS_BASIC, EST_STORAGE_DEFAULT, EST_DEFAULT};
  method = EST_INVALID;
  bool is_simple_scene = false;
  bool is_complex_scene = false;
  bool can_use_ds = valid_methods & (EST_DS_FULL | EST_DS_BASIC);
  bool can_use_storage = valid_methods & EST_STORAGE;

  // check is simple scene
  bool is_table_get = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_table_get && i < paths.count(); ++i) {
    if (OB_ISNULL(paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(paths.at(i)));
    } else if (paths.at(i)->pre_query_range_ != NULL &&
               OB_FAIL(paths.at(i)->pre_query_range_->is_get(is_table_get))) {
      LOG_WARN("check query range is table get", K(ret));
    }
  }
  is_simple_scene = is_table_get;
  if (OB_SUCC(ret) && !is_simple_scene && can_use_ds) {
    ObLogPlan *log_plan = paths.at(0)->parent_->get_plan();
    ObSEArray<ObRawExpr*, 16> ds_col_exprs;
    if (!(valid_methods & EST_STORAGE)) {
      // path which can not use storage estimation is not simple
    } else if (OB_FAIL(get_need_dynamic_sampling_columns(paths.at(0)->parent_->get_plan(),
                                                         paths.at(0)->table_id_,
                                                         filter_exprs, true, true,
                                                         ds_col_exprs))) {
      LOG_WARN("failed to get need dynamic sampling columns", K(ret));
    } else if (!ds_col_exprs.empty()) {
      // path which contains no ds_col_exprs is not simple
    } else {
      is_simple_scene = true;
    }
  }

  // check is complex scene
  if (OB_SUCC(ret) && !is_simple_scene && !is_complex_scene && (valid_methods | EST_DS_FULL)) {
    ObArenaAllocator tmp_alloc("ObOptSel");
    ObSelEstimatorFactory factory(tmp_alloc);
    const OptSelectivityCtx* sel_ctx = NULL;
    if (OB_UNLIKELY(paths.empty()) ||
        OB_ISNULL(paths.at(0)) ||
        OB_ISNULL(sel_ctx = paths.at(0)->est_cost_info_.sel_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpeted param", K(paths));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_complex_scene && i < filter_exprs.count(); ++i) {
      const ObRawExpr *filter = filter_exprs.at(i);
      ObSelEstimator *estimator = NULL;
      if (OB_FAIL(factory.create_estimator(*sel_ctx, filter, estimator))) {
        LOG_WARN("failed to create estimator", KPC(filter));
      } else if (estimator->tend_to_use_ds()) {
        is_complex_scene = true;
        // path which contains complex filters is complex
        LOG_PRINT_EXPR(TRACE, "Try to use dynamic sampling because of complex filter:", filter);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_simple_scene) {
    method = choose_one_est_method(valid_methods, simple_est_priority, priority_cnt);
  } else if (is_complex_scene) {
    method = choose_one_est_method(valid_methods, complex_est_priority, priority_cnt);
  } else {
    method = choose_one_est_method(valid_methods, default_est_priority, priority_cnt);
  }

  LOG_TRACE("choose a best est method", K(valid_methods), K(is_simple_scene), K(is_complex_scene), K(method));
  return ret;
}

int ObAccessPathEstimation::is_storage_estimation_enabled(const ObLogPlan* log_plan,
                                                           ObOptimizerContext &ctx,
                                                           uint64_t table_id,
                                                           uint64_t ref_table_id,
                                                           bool &can_use)
{
  int ret = OB_SUCCESS;
  can_use = ctx.is_storage_estimation_enabled();
  const OptTableMeta *table_meta = NULL;
  bool has_hint = false;
  bool is_hint_enabled = false;
  if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan));
  } else if (is_virtual_table(ref_table_id) &&
             !share::is_oracle_mapping_real_virtual_table(ref_table_id)) {
    //virtual table
    can_use = false;
  } else if (OB_ISNULL(table_meta = log_plan->get_basic_table_metas().get_table_meta_by_table_id(table_id)) ||
             OB_UNLIKELY(OB_INVALID_ID == table_meta->get_ref_table_id())) {
    //not basic table
  } else if (OB_FAIL(log_plan->get_stmt()->get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
             ObOptParamHint::_ENABLE_STORAGE_CARDINALITY_ESTIMATION, is_hint_enabled, has_hint))) {
    LOG_WARN("failed to check has opt param", K(ret));
  } else if (has_hint) {
    can_use = is_hint_enabled;
  }
  return ret;
}

int ObAccessPathEstimation::check_path_can_use_storage_estimation(const AccessPath *path,
                                                                  bool &can_use,
                                                                  ObOptimizerContext &ctx)
{
  int ret = OB_SUCCESS;
  can_use = false;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is invalid", K(ret), K(path));
  } else if (OB_FAIL(is_storage_estimation_enabled(path->parent_->get_plan(), ctx ,path->table_id_, path->ref_table_id_, can_use))) {
    LOG_WARN("fail to do check_path_can_use_storage_estimation ", K(ret), K(path));
  } else if (!can_use) {
    can_use = false;
  } else {
    const ObTablePartitionInfo *part_info = NULL;
    if (OB_ISNULL(part_info = path->table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table partition info is null", K(ret), K(part_info));
    } else {
      int64_t scan_range_count = get_scan_range_count(path->get_query_ranges());
      int64_t partition_count = part_info->get_phy_tbl_location_info().get_partition_cnt();
      if (partition_count > 1 ||
          scan_range_count <= 0 ||
          (!path->est_cost_info_.index_meta_info_.is_geo_index_ &&
           !path->est_cost_info_.index_meta_info_.is_multivalue_index_ &&
           scan_range_count > ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM)) {
        can_use = false;
      } else {
        can_use = true;
      }
    }
  }
  LOG_TRACE("check_path_can_use_storage_estimation", K(can_use));
  return ret;
}

int ObAccessPathEstimation::process_external_table_default_estimation(AccessPath *path)
{
  //TODO [ExternalTable] need refine
  int ret = OB_SUCCESS;
  double output_row_count = 0.0;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else {
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    est_cost_info.batch_type_ = ObSimpleBatch::T_SCAN;
    output_row_count = static_cast<double>(OB_EST_DEFAULT_VIRTUAL_TABLE_ROW_COUNT);
    path->est_cost_info_.logical_query_range_row_count_ = output_row_count;
    path->est_cost_info_.phy_query_range_row_count_ = output_row_count;
    path->est_cost_info_.index_back_row_count_ = 0;
    path->est_cost_info_.output_row_count_ = output_row_count;
  }
  return ret;
}

int ObAccessPathEstimation::process_vtable_default_estimation(AccessPath *path)
{
  int ret = OB_SUCCESS;
  double output_row_count = 0.0;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else {
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    est_cost_info.batch_type_ = ObSimpleBatch::T_SCAN;
    if (est_cost_info.ranges_.empty() || est_cost_info.prefix_filters_.empty()) {
      output_row_count = static_cast<double>(OB_EST_DEFAULT_VIRTUAL_TABLE_ROW_COUNT);
      LOG_TRACE("OPT:[VT] virtual table without range, use default stat", K(output_row_count));
    } else {
      output_row_count = static_cast<double>(est_cost_info.ranges_.count());
      if (!est_cost_info.is_unique_) {
        output_row_count *= 100.0;
      }
    }
    path->est_cost_info_.logical_query_range_row_count_ = output_row_count;
    path->est_cost_info_.phy_query_range_row_count_ = output_row_count;
    path->est_cost_info_.index_back_row_count_ = 0;
    path->est_cost_info_.output_row_count_ = output_row_count;
  }
  return ret;
}

int ObAccessPathEstimation::process_table_default_estimation(ObOptimizerContext &ctx, ObIArray<AccessPath *> &paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    AccessPath *path = paths.at(i);
    if (OB_ISNULL(path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(paths));
    } else if (ctx.use_default_stat()) {
      if (OB_FAIL(process_table_force_default_estimation(path))) {
        LOG_WARN("failed to process process vtable default estimation", K(ret));
      } else if (i == 0 && OB_FAIL(update_table_stat_info_by_default(path))) {
        LOG_WARN("failed to update table stat by default", K(ret));
      }
    } else if (is_virtual_table(path->ref_table_id_) &&
              !share::is_oracle_mapping_real_virtual_table(path->ref_table_id_)) {
      if (OB_FAIL(process_vtable_default_estimation(path))) {
        LOG_WARN("failed to process vtable default estimation", K(ret));
      } else if (i == 0 && OB_FAIL(update_table_stat_info_by_default(path))) {
        LOG_WARN("failed to update table stat by default", K(ret));
      }
    } else if (EXTERNAL_TABLE == path->est_cost_info_.table_meta_info_->table_type_) {
      if (OB_FAIL(process_external_table_default_estimation(path))) {
        LOG_WARN("failed to process external table default estimation", K(ret));
      } else if (i == 0 && OB_FAIL(update_table_stat_info_by_default(path))) {
        LOG_WARN("failed to update table stat by default", K(ret));
      }
    } else if (OB_FAIL(process_statistics_estimation(path))) {
      // use default opt table meta inited in ObJoinOrder::init_est_sel_info_for_access_path
      LOG_WARN("failed to process statistics estimation", K(ret));
    }
  }
  return ret;
}

int ObAccessPathEstimation::process_table_force_default_estimation(AccessPath *path)
{
  int ret = OB_SUCCESS;
  double output_row_count = ObOptStatManager::get_default_table_row_count();
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else if (OB_FAIL(reset_skip_scan_info(path->est_cost_info_,
                                          path->parent_->get_plan()->get_predicate_selectivities(),
                                          path->use_skip_scan_))) {
    LOG_WARN("failed to reset skip scan info", K(ret));
  } else {
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    path->est_cost_info_.logical_query_range_row_count_ = output_row_count;
    path->est_cost_info_.phy_query_range_row_count_ = output_row_count;
    path->est_cost_info_.index_back_row_count_ = 0;
    path->est_cost_info_.output_row_count_ = output_row_count;
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

int ObAccessPathEstimation::process_storage_estimation(ObOptimizerContext &ctx,
                                                       ObIArray<AccessPath *> &paths,
                                                       bool &is_success)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("CardEstimation");
  ObArray<ObBatchEstTasks *> tasks;
  ObArray<ObAddr> prefer_addrs;
  void *ptr = NULL;
  
  bool force_leader_estimation = false;
  
  force_leader_estimation = OB_FAIL(OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);
  ret = OB_SUCCESS;

  // for each access path, find a partition/server for estimation
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    AccessPath *ap = NULL;
    ObBatchEstTasks *task = NULL;
    EstimatedPartition best_index_part;
    const ObTableMetaInfo *table_meta = NULL;
    SMART_VARS_3((ObTablePartitionInfo, tmp_part_info),
                 (ObPhysicalPlanCtx, tmp_plan_ctx, arena),
                 (ObExecContext, tmp_exec_ctx, arena)) {
      const ObTablePartitionInfo *table_part_info = NULL;
      if (OB_ISNULL(ap = paths.at(i)) ||
          OB_ISNULL(table_part_info = ap->table_partition_info_) ||
          OB_ISNULL(ctx.get_session_info()) ||
          OB_ISNULL(ctx.get_exec_ctx()) ||
          OB_ISNULL(ctx.get_exec_ctx()->get_physical_plan_ctx()) ||
          OB_ISNULL(table_meta = ap->est_cost_info_.table_meta_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access path is invalid", K(ret), K(ap), K(table_part_info), K(ctx.get_exec_ctx()),
                                           K(table_meta));
      } else {
        ObPhysicalPlanCtx *plan_ctx = ctx.get_exec_ctx()->get_physical_plan_ctx();
        const int64_t cur_time = plan_ctx->has_cur_time() ?
            plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
        tmp_exec_ctx.set_my_session(ctx.get_session_info());
        tmp_exec_ctx.set_physical_plan_ctx(&tmp_plan_ctx);
        tmp_exec_ctx.set_sql_ctx(ctx.get_exec_ctx()->get_sql_ctx());
        tmp_plan_ctx.set_timeout_timestamp(plan_ctx->get_timeout_timestamp());
        tmp_plan_ctx.set_cur_time(cur_time, *ctx.get_session_info());
        tmp_plan_ctx.set_rich_format(ctx.get_session_info()->use_rich_format());
        if (OB_FAIL(tmp_plan_ctx.get_param_store_for_update().assign(plan_ctx->get_param_store()))) {
          LOG_WARN("failed to assign phy plan ctx");
        } else if (OB_FAIL(tmp_plan_ctx.init_datum_param_store())) {
          LOG_WARN("failed to init datum store", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tmp_part_info.assign(*table_part_info))) {
        LOG_WARN("failed to assign table part info", K(ret));
      } else if (OB_UNLIKELY(1 != tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access path is invalid", K(ret), K(tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list()));
      } else if (!ap->is_global_index_ && ap->ref_table_id_ != ap->index_id_ &&
                OB_FAIL(tmp_part_info.replace_final_location_key(tmp_exec_ctx,
                                                                 ap->index_id_,
                                                                 true))) {
        LOG_WARN("failed to replace final location key", K(ret));
      } else if (OB_FAIL(ObSQLUtils::choose_best_replica_for_estimation(
                          tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list().at(0),
                          ctx.get_local_server_addr(),
                          prefer_addrs,
                          !ap->can_use_remote_estimate(),
                          best_index_part))) {
        LOG_WARN("failed to choose best partition for estimation", K(ret));
      } else if (force_leader_estimation &&
                 OB_FAIL(choose_leader_replica(tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list().at(0),
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
        task->arg_.schema_version_ = table_meta->schema_version_;
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
      if (is_retry_ret(ret)) {
        //retry code throw error, and retry
      } else {
        LOG_WARN("failed to process storage estimation", K(ret));
        need_fallback = true;
        ret = OB_SUCCESS;
      }
      break;
    } else if (!tasks.at(i)->check_result_reliable()) {
      need_fallback = true;
    }
  }
  NG_TRACE(storage_estimation_end);

  if (!need_fallback) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObBatchEstTasks *task = tasks.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < task->paths_.count(); ++j) {
        const obrpc::ObEstPartResElement &res = task->res_.index_param_res_.at(j);
        AccessPath *path = task->paths_.at(j);
        if (OB_FAIL(path->est_records_.assign(res.est_records_))) {
          LOG_WARN("failed to assign estimation records", K(ret));
        } else if (OB_FAIL(estimate_prefix_range_rowcount(res,
                                                          path->est_cost_info_))) {
          LOG_WARN("failed to estimate prefix range rowcount", K(ret));
        } else if (OB_FAIL(fill_cost_table_scan_info(path->est_cost_info_))) {
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
  is_success = !need_fallback;
  return ret;
}

int ObAccessPathEstimation::choose_leader_replica(const ObCandiTabletLoc &part_loc_info,
                                                  const bool can_use_remote,
                                                  const ObAddr &local_addr,
                                                  EstimatedPartition &best_partition)
{
  int ret = OB_SUCCESS;
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
    ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  double &logical_row_count = est_cost_info.logical_query_range_row_count_;
  double &physical_row_count = est_cost_info.phy_query_range_row_count_;
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

  // skip scan postfix range conditions
  logical_row_count   *= est_cost_info.ss_postfix_range_filters_sel_;
  physical_row_count  *= est_cost_info.ss_postfix_range_filters_sel_;

  LOG_TRACE("OPT:[STORAGE EST ROW COUNT]",
            K(logical_row_count), K(physical_row_count),
            K(get_range_count), K(scan_range_count),
            K(range_sample_ratio), K(result), K(est_cost_info.index_meta_info_.index_part_count_),
            K(est_cost_info.pushdown_prefix_filter_sel_),
            K(est_cost_info.ss_postfix_range_filters_sel_));
  return ret;
}

int ObAccessPathEstimation::fill_cost_table_scan_info(ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  double &output_row_count = est_cost_info.output_row_count_;
  double &logical_row_count = est_cost_info.logical_query_range_row_count_;
  double &physical_row_count = est_cost_info.phy_query_range_row_count_;
  double &index_back_row_count = est_cost_info.index_back_row_count_;

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

  if (OB_FAIL(ret)) {
  } else if (!est_cost_info.ss_ranges_.empty()) {
    int64_t scan_range_count = get_scan_range_count(est_cost_info.ss_ranges_);
    if (scan_range_count == 1) {
      est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_SCAN;
    } else {
      est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_GET;
    }
  } else {
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
    ret = OB_ALLOCATE_MEMORY_FAILED;
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
    index_est_arg->tx_id_ = ctx.get_session_info()->get_tx_id();
  }
  // FIXME, move following codes
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; ap->is_global_index_ && i < scan_ranges.count(); ++i) {
      scan_ranges.at(i).table_id_ = ap->index_id_;
    }
    bool is_spatial_index = ap->est_cost_info_.index_meta_info_.is_geo_index_;
    if (!is_spatial_index && scan_ranges.count() > ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM) {
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
      if (!is_spatial_index && OB_FAIL(construct_scan_range_batch(allocator, scan_ranges, index_est_arg->batch_))) {
        LOG_WARN("failed to construct scan range batch", K(ret));
      } else if (is_spatial_index && OB_FAIL(construct_geo_scan_range_batch(allocator, scan_ranges, index_est_arg->batch_))) {
        LOG_WARN("failed to construct spatial scan range batch", K(ret));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::process_statistics_estimation(AccessPath *path)
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
  } else if (OB_FAIL(calc_skip_scan_prefix_ndv(*path, path->est_cost_info_.ss_prefix_ndv_))) {
    LOG_WARN("failed to calc skip scan prefix ndv", K(ret));
  } else if (OB_FAIL(update_use_skip_scan(path->est_cost_info_,
                                          path->parent_->get_plan()->get_predicate_selectivities(),
                                          path->use_skip_scan_))) {
    LOG_WARN("failed to update use skip scan", K(ret));
  } else {
    ObArenaAllocator allocator;
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    double &logical_row_count = est_cost_info.logical_query_range_row_count_;
    double &physical_row_count = est_cost_info.phy_query_range_row_count_;

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

    // skip scan postfix range conditions
    logical_row_count   *= est_cost_info.ss_postfix_range_filters_sel_;
    physical_row_count  *= est_cost_info.ss_postfix_range_filters_sel_;

    LOG_TRACE("OPT:[STATISTIC EST ROW COUNT",
              K(logical_row_count), K(physical_row_count),
              K(est_cost_info.pushdown_prefix_filter_sel_),
              K(est_cost_info.ss_postfix_range_filters_sel_));

    OZ (fill_cost_table_scan_info(est_cost_info));
  }
  return ret;
}

int ObAccessPathEstimation::process_statistics_estimation(ObIArray<AccessPath *> &paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    if (OB_FAIL(process_statistics_estimation(paths.at(i)))) {
      LOG_WARN("failed to process table default estimation", K(ret));
    }
  }
  return ret;
}

// calculate skip scan prefix range columns NDV and postfix range conditions selectivity.
// use the table_metas and origin_rows after extract prefix range.
int ObAccessPathEstimation::calc_skip_scan_prefix_ndv(AccessPath &ap, double &prefix_ndv)
{
  int ret = OB_SUCCESS;
  prefix_ndv = 1.0;
  ObJoinOrder *join_order = NULL;
  ObLogPlan *log_plan = NULL;
  const ObTableMetaInfo *table_meta_info = NULL;
  if (OB_ISNULL(ap.pre_query_range_) || !ap.pre_query_range_->is_ss_range()
      || OptSkipScanState::SS_DISABLE == ap.use_skip_scan_) {
    /* do nothing */
  } else if (OB_ISNULL(join_order = ap.parent_) || OB_ISNULL(log_plan = join_order->get_plan())
             || OB_ISNULL(table_meta_info = ap.est_cost_info_.table_meta_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",  K(ret), K(join_order), K(log_plan), K(table_meta_info));
  } else {
    // generate temporary update table metas use prefix range conditions
    SMART_VAR(OptTableMetas, tmp_metas) {
      ObSEArray<ObRawExpr*, 4> prefix_exprs;
      const double prefix_range_row_count = table_meta_info->table_row_count_
                                            * ap.est_cost_info_.prefix_filter_sel_
                                            * ap.est_cost_info_.pushdown_prefix_filter_sel_;
      const EqualSets *temp_equal_sets = log_plan->get_selectivity_ctx().get_equal_sets();
      const double temp_rows = log_plan->get_selectivity_ctx().get_current_rows();
      log_plan->get_selectivity_ctx().init_op_ctx(&join_order->get_output_equal_sets(), prefix_range_row_count);
      if (OB_FAIL(get_skip_scan_prefix_exprs(ap.est_cost_info_.range_columns_,
                                            ap.pre_query_range_->get_skip_scan_offset(),
                                            prefix_exprs))) {
        LOG_WARN("failed to get skip scan prefix expers", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::update_table_meta_info(log_plan->get_basic_table_metas(),
                                                                  tmp_metas,
                                                                  log_plan->get_selectivity_ctx(),
                                                                  ap.get_table_id(),
                                                                  prefix_range_row_count,
                                                                  ap.pre_query_range_->get_range_exprs(),
                                                                  log_plan->get_predicate_selectivities()))) {
        LOG_WARN("failed to update table meta info", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(tmp_metas,
                                                              log_plan->get_selectivity_ctx(),
                                                              prefix_exprs,
                                                              prefix_range_row_count,
                                                              prefix_ndv))) {
        LOG_WARN("failed to calculate distinct", K(ret), K(prefix_exprs));
      } else {
        double refine_ndv = 1.0;
        prefix_ndv = std::max(refine_ndv, prefix_ndv);
        log_plan->get_selectivity_ctx().init_op_ctx(temp_equal_sets, temp_rows);
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::get_skip_scan_prefix_exprs(ObIArray<ColumnItem> &column_items,
                                                       int64_t skip_scan_offset,
                                                       ObIArray<ObRawExpr*> &prefix_exprs)
{
  int ret = OB_SUCCESS;
  prefix_exprs.reuse();
  if (OB_UNLIKELY(skip_scan_offset < 0 || skip_scan_offset >= column_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params",  K(ret), K(skip_scan_offset), K(column_items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < skip_scan_offset; ++i) {
      if (OB_FAIL(prefix_exprs.push_back(column_items.at(i).expr_))) {
        LOG_WARN("failed to push back",  K(ret), K(skip_scan_offset));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::update_use_skip_scan(ObCostTableScanInfo &est_cost_info,
                                                 ObIArray<ObExprSelPair> &all_predicate_sel,
                                                 OptSkipScanState &use_skip_scan)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = NULL;
  if (OB_ISNULL(table_meta_info = est_cost_info.table_meta_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null",  K(ret), K(table_meta_info));
  } else {
    const double row_count = table_meta_info->table_row_count_
                             * est_cost_info.prefix_filter_sel_
                             * est_cost_info.pushdown_prefix_filter_sel_;
    const double row_count_per_range = std::max(row_count
                                                * est_cost_info.ss_postfix_range_filters_sel_
                                                / est_cost_info.ss_prefix_ndv_,
                                                1.0);
    const double ss_row_count = est_cost_info.ss_prefix_ndv_
                                    + row_count_per_range * est_cost_info.ss_prefix_ndv_;
    const double index_scan_cost = row_count * (NORMAL_CPU_TUPLE_COST);
    const double skip_scan_cost = ss_row_count * NORMAL_MICRO_BLOCK_RND_COST;
    LOG_TRACE("decide use skip scan by ndv and selectively", K(use_skip_scan), K(row_count), K(row_count_per_range),
                  K(ss_row_count), K(index_scan_cost), K(skip_scan_cost),
                  K(est_cost_info.ss_prefix_ndv_), K(est_cost_info.ss_postfix_range_filters_sel_),
                  K(est_cost_info.ss_postfix_range_filters_));
    bool reset_skip_scan = false;
    if (OptSkipScanState::SS_UNSET != use_skip_scan) {
      /* do nothing */
    } else if (!table_meta_info->has_opt_stat_) {
      reset_skip_scan = true;
    } else if (est_cost_info.ss_prefix_ndv_ > 1000 || est_cost_info.ss_postfix_range_filters_sel_ > 0.01) {
      reset_skip_scan = true;
    } else if (skip_scan_cost < index_scan_cost) {
      use_skip_scan = OptSkipScanState::SS_NDV_SEL_ENABLE;
    } else {
      reset_skip_scan = true;
    }
    if (OB_SUCC(ret) && reset_skip_scan) {
      if (OB_FAIL(reset_skip_scan_info(est_cost_info, all_predicate_sel, use_skip_scan))) {
        LOG_WARN("failed to reset skip scan info", K(ret));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::reset_skip_scan_info(ObCostTableScanInfo &est_cost_info,
                                                 ObIArray<ObExprSelPair> &all_predicate_sel,
                                                 OptSkipScanState &use_skip_scan)
{
  int ret = OB_SUCCESS;
  const bool is_full_scan = est_cost_info.ref_table_id_ == est_cost_info.index_id_;
  ObIArray<ObRawExpr*> &filters = is_full_scan ? est_cost_info.table_filters_
                                                : est_cost_info.postfix_filters_;
  double &filter_sel = is_full_scan ? est_cost_info.table_filter_sel_
                                    : est_cost_info.postfix_filter_sel_;
  if (OB_FAIL(append(filters, est_cost_info.ss_postfix_range_filters_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             filters,
                                                             filter_sel,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.postfix_filters_), K(ret));
  } else if (OptSkipScanState::SS_HINT_ENABLE != use_skip_scan) {
    // TODO: only for bug fix of
    // Here should be optimized later.
    est_cost_info.ss_ranges_.reuse();
    est_cost_info.ss_postfix_range_filters_.reuse();
    est_cost_info.ss_prefix_ndv_ = 1.0;
    est_cost_info.ss_postfix_range_filters_sel_ = 1.0;
    use_skip_scan = OptSkipScanState::SS_DISABLE;
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

bool ObAccessPathEstimation::is_multi_geo_range(const ObNewRange &range)
{
  return OB_NOT_NULL(range.get_start_key().get_obj_ptr()) &&
         range.get_start_key().get_obj_ptr()[0].get_uint64() != range.get_end_key().get_obj_ptr()[0].get_uint64();
}


int ObAccessPathEstimation::construct_geo_scan_range_batch(ObIAllocator &allocator,
                                                           const ObIArray<ObNewRange> &scan_ranges,
                                                           ObSimpleBatch &batch)
{
  int ret = OB_SUCCESS;
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
    void *ptr = NULL;
    if (scan_ranges.at(0).get_start_key().get_obj_cnt() < SPATIAL_ROWKEY_MIN_NUM) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The count of rowkey from spatial_index_table is wrong.", K(ret), K(scan_ranges.at(0).get_start_key().get_obj_cnt()));
    } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(SQLScanRangeArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ptr), K(ret));
    } else {
      range_array = new(ptr)SQLScanRangeArray();
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = range_array;
      int64_t ranges_count = 0;
      // push_back scan_range for first priority
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count(); ++i) {
        const ObNewRange &range = scan_ranges.at(i);
        if (is_multi_geo_range(range)) {
          if (OB_FAIL(range_array->push_back(range))) {
            LOG_WARN("failed to push back scan range", K(ret));
          } else {
            ranges_count++;
          }
        }
      }
      // push_back get_range
      for (int64_t i = 0;
          OB_SUCC(ret)
          && ranges_count < ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM
          && i < scan_ranges.count();
          ++i) {
        const ObNewRange &range = scan_ranges.at(i);
        if (!is_multi_geo_range(range)) {
          if (OB_FAIL(range_array->push_back(range))) {
            LOG_WARN("failed to push back scan range", K(ret));
          } else {
            ranges_count++;
          }
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
  const ObCandiTabletLocIArray &part_loc_info_array =
              table_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list();
  //if the part loc infos is only 1, we can use the storage estimate rowcount to get real time stat.
  if (is_virtual_table(meta.ref_table_id_) &&
      !share::is_oracle_mapping_real_virtual_table(meta.ref_table_id_)) {
    //do nothing
  } else if (part_loc_info_array.count() == 1) {
    if (OB_FAIL(storage_estimate_full_table_rowcount(ctx, part_loc_info_array.at(0), meta))) {
      LOG_WARN("failed to storage estimate full table rowcount", K(ret));
    } else {
      LOG_TRACE("succeed to storage estimate full table rowcount", K(meta));
    }
  //if the part loc infos more than 1, we see the dml info inner table and storage inner table.
  } else if (part_loc_info_array.count() > 1) {
    ObSEArray<ObTabletID, 64> all_tablet_ids;
    ObSEArray<ObLSID, 64> all_ls_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
      const ObOptTabletLoc &part_loc = part_loc_info_array.at(i).get_partition_location();
      if (OB_FAIL(all_tablet_ids.push_back(part_loc.get_tablet_id()))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      } else if (OB_FAIL(all_ls_ids.push_back(part_loc.get_ls_id()))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(estimate_full_table_rowcount_by_meta_table(ctx, all_tablet_ids,
                                                             all_ls_ids, meta))) {
        LOG_WARN("failed to estimate full table rowcount by meta table", K(ret));
      } else {
        LOG_TRACE("succeed to estimate full table rowcount", K(meta));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(part_loc_info_array));
  }
  return ret;
}

int ObAccessPathEstimation::storage_estimate_full_table_rowcount(ObOptimizerContext &ctx,
                                                                 const ObCandiTabletLoc &part_loc_info,
                                                                 ObTableMetaInfo &meta)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 1> prefer_addrs;
  EstimatedPartition best_index_part;
  ObArenaAllocator arena("CardEstimation");
  bool force_leader_estimation = false;

  force_leader_estimation = OB_FAIL(OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);


  ret = OB_SUCCESS;

  HEAP_VAR(ObBatchEstTasks, task) {
    obrpc::ObEstPartArg &arg = task.arg_;
    obrpc::ObEstPartRes &res = task.res_;

    arg.schema_version_ = meta.schema_version_;
    if (OB_ISNULL(ctx.get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if ((is_virtual_table(meta.ref_table_id_) &&
                !share::is_oracle_mapping_real_virtual_table(meta.ref_table_id_))
               || EXTERNAL_TABLE == meta.table_type_) {
      // do nothing
    } else if (OB_FAIL(ObSQLUtils::choose_best_replica_for_estimation(
                part_loc_info,
                ctx.get_local_server_addr(),
                prefer_addrs,
                false,
                best_index_part))) {
      LOG_WARN("failed to choose best partition", K(ret));
    } else if (force_leader_estimation &&
               OB_FAIL(choose_leader_replica(part_loc_info,
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
      path_arg.tx_id_ = ctx.get_session_info()->get_tx_id();
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
        if (is_retry_ret(ret)) {
          //retry code throw error, and retry
        } else {
          LOG_WARN("failed to do storage estimation", K(ret));
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(res.index_param_res_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage estimation result size is unexpected", K(ret));
      } else if (res.index_param_res_.at(0).reliable_) {
        int64_t logical_row_count = res.index_param_res_.at(0).logical_row_count_;
        meta.table_row_count_ = logical_row_count;
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
    LOG_WARN("failed to convert agent vt key ranges", K(ret), K(new_ranges));
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
    uint64_t real_index_id = ObSchemaUtils::get_real_table_mappings_tid(ap->index_id_);
    ObSqlSchemaGuard *schema_guard = NULL;
    const ObTableSchema *vt_table_schema = NULL;
    const ObTableSchema *real_index_schema = nullptr;
    if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObVirtualTableResultConverter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret), K(buf));
    } else if (OB_FAIL(schema_guard->get_table_schema(vt_table_id, vt_table_schema)) ||
               OB_FAIL(schema_guard->get_table_schema(real_index_id, real_index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(vt_table_schema) || OB_ISNULL(real_index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(vt_table_schema), K(real_index_schema));
    } else {
      ObVirtualTableResultConverter *vt_converter = new (buf) ObVirtualTableResultConverter();
      ObSEArray<ObObjMeta, 4> key_types;
      int64_t tenant_id_col_idx = -1;
      bool dummy_has_tenant_id = true;
      if (OB_FAIL(gen_agent_vt_table_convert_info(vt_table_id,
                                                  vt_table_schema,
                                                  real_index_schema,
                                                  ap->est_cost_info_.range_columns_,
                                                  tenant_id_col_idx,
                                                  key_types))) {
        LOG_WARN("failed to gen agent vt table convert info", K(ret));
      } else if (OB_FAIL(vt_converter->init_convert_key_ranges_info(&allocator,
                                                                    ctx.get_session_info(),
                                                                    vt_table_schema,
                                                                    &key_types,
                                                                    dummy_has_tenant_id,
                                                                    tenant_id_col_idx))) {
        LOG_WARN("failed to init convert key ranges info", K(ret));
      } else if (OB_FAIL(vt_converter->convert_key_ranges(new_ranges))) {
        LOG_WARN("convert key ranges failed", K(ret), K(new_ranges));
      } else {
        LOG_TRACE("succeed to convert agent vt key ranges", K(new_ranges));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::gen_agent_vt_table_convert_info(const uint64_t vt_table_id,
                                                            const ObTableSchema *vt_table_schema,
                                                            const ObTableSchema *real_index_schema,
                                                            const ObIArray<ColumnItem> &range_columns,
                                                            int64_t &tenant_id_col_idx_,
                                                            ObIArray<ObObjMeta> &key_types)
{
  int ret = OB_SUCCESS;
  key_types.reset();
  if (OB_ISNULL(vt_table_schema) || OB_ISNULL(real_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(vt_table_schema), K(real_index_schema));
  } else {
    // set vt has tenant_id column
    for (int64_t nth_col = 0; OB_SUCC(ret) && nth_col < range_columns.count(); ++nth_col) {
      const ColumnItem &col_item = range_columns.at(nth_col);
      if (0 == col_item.column_name_.case_compare("TENANT_ID")) {
        tenant_id_col_idx_ = nth_col;
        break;
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
      } else {
        for (int64_t j = 0;
             OB_SUCC(ret) && !find_it && j < real_index_schema->get_column_count();
             ++j) {
          const ObColumnSchemaV2 *col_schema = real_index_schema->get_column_schema_by_idx(j);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(j));
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

int ObAccessPathEstimation::estimate_full_table_rowcount_by_meta_table(ObOptimizerContext &ctx,
                                                                       const ObIArray<ObTabletID> &all_tablet_ids,
                                                                       const ObIArray<ObLSID> &all_ls_ids,
                                                                       ObTableMetaInfo &meta)
{
  int ret = OB_SUCCESS;
  if (all_tablet_ids.empty()) {
    //do nothing
  } else if (OB_ISNULL(ctx.get_session_info()) || OB_ISNULL(ctx.get_opt_stat_manager())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_session_info()), K(ctx.get_opt_stat_manager()));
  } else if (OB_FAIL(ctx.get_opt_stat_manager()->get_table_rowcnt(ctx.get_session_info()->get_effective_tenant_id(),
                                                                  meta.ref_table_id_,
                                                                  all_tablet_ids,
                                                                  all_ls_ids,
                                                                  meta.table_row_count_))) {
    LOG_WARN("failed to get table rowcnt", K(ret));
  } else {
    meta.average_row_size_ = static_cast<double>(ObOptStatManager::get_default_avg_row_size());
    meta.part_size_ = meta.table_row_count_ * meta.average_row_size_;
    LOG_TRACE("succeed to estimate full table rowcount by meta table", K(meta));
  }
  return ret;
}

int ObAccessPathEstimation::process_dynamic_sampling_estimation(ObOptimizerContext &ctx,
                                                                ObIArray<AccessPath *> &paths,
                                                                const bool is_inner_path,
                                                                const ObIArray<ObRawExpr*> &filter_exprs,
                                                                bool only_ds_basic_stat,
                                                                bool &is_success)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin process dynamic sampling estimation", K(paths), K(is_inner_path));
  ObDSTableParam ds_table_param;
  ObSEArray<ObDSResultItem, 4> ds_result_items;
  is_success = true;
  const ObLogPlan* log_plan = NULL;
  const OptTableMeta *table_meta = NULL;
  common::ObSEArray<AccessPath*, 4> ds_paths;
  bool no_ds_data = false;
  bool specify_ds = false;
  if (paths.empty()) {
    //do nothing
  } else if (OB_ISNULL(paths.at(0)->parent_) ||
             OB_ISNULL(log_plan = paths.at(0)->parent_->get_plan()) ||
             OB_ISNULL(table_meta = log_plan->get_basic_table_metas().get_table_meta_by_table_id(paths.at(0)->table_id_)) ||
             OB_UNLIKELY(OB_INVALID_ID == table_meta->get_ref_table_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan), KPC(table_meta));
  } else if (OB_FAIL(ObDynamicSamplingUtils::get_ds_table_param(ctx, log_plan, table_meta,
                                                                ds_table_param, specify_ds))) {
    LOG_WARN("failed to get ds table param", K(ret), K(ds_table_param));
  } else if (!ds_table_param.is_valid()) {
    is_success = false;
  } else if (OB_FAIL(add_ds_result_items(paths, filter_exprs, specify_ds,
                                         ds_result_items, only_ds_basic_stat))) {
    LOG_WARN("failed to init ds result items", K(ret));
  } else {
    OPT_TRACE_TITLE("BEGIN DYNAMIC SAMPLE ESTIMATION");
    ObArenaAllocator allocator("ObOpTableDS", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_session_info()->get_effective_tenant_id());
    ObDynamicSampling dynamic_sampling(ctx, allocator);
    int64_t start_time = ObTimeUtility::current_time();
    bool throw_ds_error = false;
    if (OB_FAIL(dynamic_sampling.estimate_table_rowcount(ds_table_param, ds_result_items, throw_ds_error))) {
      if (!throw_ds_error && !is_retry_ret(ret)) {
        LOG_WARN("failed to estimate table rowcount caused by some reason, please check!!!", K(ret),
                 K(start_time), K(ObTimeUtility::current_time() - start_time), K(ds_table_param),
                 K(ctx.get_session_info()->get_current_query_string()));
        if (OB_FAIL(ObDynamicSamplingUtils::add_failed_ds_table_list(table_meta->get_ref_table_id(),
                                                                     table_meta->get_all_used_parts(),
                                                                     ctx.get_failed_ds_tab_list()))) {
          LOG_WARN("failed to add failed ds table list", K(ret));
        } else {
          is_success = false;
        }
      } else {
        LOG_WARN("failed to dynamic sampling", K(ret), K(start_time), K(ds_table_param));
      }
    } else if (OB_FAIL(update_table_stat_info_by_dynamic_sampling(paths.at(0),
                                                                  ds_table_param.ds_level_,
                                                                  ds_result_items,
                                                                  no_ds_data))) {
      LOG_WARN("failed to update table stat info by dynamic sampling", K(ret));
    } else if (only_ds_basic_stat || no_ds_data) {
      if (OB_FAIL(process_statistics_estimation(paths))) {
        LOG_WARN("failed to process statistics estimation", K(ret));
      }
    } else if (OB_FAIL(estimate_path_rowcount_by_dynamic_sampling(ds_table_param.table_id_, paths,
                                                                  is_inner_path, ds_result_items))) {
      LOG_WARN("failed to estimate path rowcount by dynamic sampling", K(ret));
      LOG_TRACE("finish dynamic sampling", K(only_ds_basic_stat), K(no_ds_data), K(is_success));
    }
    OPT_TRACE("end to process table dynamic sampling estimation");
    OPT_TRACE("dynamic sampling estimation result:");
    OPT_TRACE(ds_result_items);
  }
  return ret;
}


int ObAccessPathEstimation::add_ds_result_items(ObIArray<AccessPath *> &paths,
                                                const ObIArray<ObRawExpr*> &filter_exprs,
                                                const bool specify_ds,
                                                ObIArray<ObDSResultItem> &ds_result_items,
                                                bool only_ds_basic_stat)
{
  int ret = OB_SUCCESS;
  bool all_path_is_get = false;
  if (OB_UNLIKELY(paths.empty()) ||
      OB_ISNULL(paths.at(0)) ||
      OB_ISNULL(paths.at(0)->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(paths));
  } else if (only_ds_basic_stat) {// some filters invalid, only dynamic basic stats
    ObDSResultItem basic_item(ObDSResultItemType::OB_DS_BASIC_STAT, paths.at(0)->ref_table_id_);
    if (OB_FAIL(get_need_dynamic_sampling_columns(paths.at(0)->parent_->get_plan(),
                                                  paths.at(0)->table_id_,
                                                  filter_exprs, false, false,
                                                  basic_item.exprs_))) {
      LOG_WARN("failed to get need dynamic sampling columns", K(ret));
    } else if (OB_FAIL(ds_result_items.push_back(basic_item))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  } else {
    //1.init ds basic stat item
    ObDSResultItem basic_item(ObDSResultItemType::OB_DS_BASIC_STAT, paths.at(0)->ref_table_id_);
    if (OB_FAIL(get_need_dynamic_sampling_columns(paths.at(0)->parent_->get_plan(),
                                                  paths.at(0)->table_id_,
                                                  filter_exprs, true, false,
                                                  basic_item.exprs_))) {
      LOG_WARN("failed to get need dynamic sampling columns", K(ret));
    } else if (OB_FAIL(ds_result_items.push_back(basic_item))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      //2.init all filter output stat item
      ObDSResultItem filter_item(ObDSResultItemType::OB_DS_OUTPUT_STAT, paths.at(0)->ref_table_id_);
      if (OB_FAIL(filter_item.exprs_.assign(filter_exprs))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(ds_result_items.push_back(filter_item))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        //3.init query range item
        for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
          if (OB_ISNULL(paths.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(paths.at(i)));
          } else if (paths.at(i)->est_cost_info_.prefix_filters_.empty()) {
            //do nothing
          } else {
            ObDSResultItem tmp_item(ObDSResultItemType::OB_DS_FILTER_OUTPUT_STAT, paths.at(i)->index_id_);
            if (OB_FAIL(tmp_item.exprs_.assign(paths.at(i)->est_cost_info_.prefix_filters_))) {
              LOG_WARN("failed to assign", K(ret));
            } else if (OB_FAIL(ds_result_items.push_back(tmp_item))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
    }
  }
  LOG_TRACE("succeed to add_ds result items", K(paths), K(all_path_is_get), K(filter_exprs),
                                              K(ds_result_items), K(only_ds_basic_stat));
  return ret;
}

int ObAccessPathEstimation::get_need_dynamic_sampling_columns(const ObLogPlan* log_plan,
                                                              const int64_t table_id,
                                                              const ObIArray<ObRawExpr*> &filter_exprs,
                                                              const bool need_except_filter,
                                                              const bool depend_on_join_filter,
                                                              ObIArray<ObRawExpr *> &ds_column_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> relation_raw_exprs;
  ObSEArray<ObRawExpr*, 16> condition_raw_exprs;
  if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan));
  } else if (OB_ISNULL(log_plan->get_stmt()) ||
             OB_UNLIKELY(!log_plan->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), KPC(log_plan->get_stmt()));
  } else if (OB_FAIL(log_plan->get_stmt()->get_where_scope_conditions(condition_raw_exprs))) {
    LOG_WARN("failed to get where scope conditions ", K(ret));
  } else if (need_except_filter &&
             OB_FAIL(ObOptimizerUtil::except_exprs(condition_raw_exprs, filter_exprs, relation_raw_exprs))) {
    LOG_WARN("failed to except exprs", K(ret));
  } else if (depend_on_join_filter && relation_raw_exprs.empty()) {
    //do nothing
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(log_plan->get_stmt());
    if (OB_FAIL(append_array_no_dup(relation_raw_exprs, select_stmt->get_group_exprs()))) {
      LOG_WARN("failed to add group exprs into output exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(relation_raw_exprs, select_stmt->get_rollup_exprs()))) {
      LOG_WARN("failed to add rollup exprs into output exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(relation_raw_exprs, select_stmt->get_having_exprs()))) {
      LOG_WARN("failed to add having exprs into output exprs", K(ret));
    } else if (select_stmt->is_distinct() || log_plan->get_is_subplan_scan()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
        if (OB_ISNULL(select_stmt->get_select_item(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(select_stmt->get_select_item(i)));
        } else if (!select_stmt->get_select_item(i).expr_->is_column_ref_expr()) {
          //do nothing
        } else if (OB_FAIL(relation_raw_exprs.push_back(select_stmt->get_select_item(i).expr_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(relation_raw_exprs, table_id, ds_column_exprs))) {
      LOG_WARN("failed to extract exprs", K(ret));
    } else {
      LOG_TRACE("succeed to get need dynamic sampling columns", K(filter_exprs),
                                                                K(need_except_filter),
                                                                K(depend_on_join_filter),
                                                                K(condition_raw_exprs),
                                                                K(relation_raw_exprs),
                                                                K(ds_column_exprs));
    }
  }
  return ret;
}

int ObAccessPathEstimation::update_table_stat_info_by_dynamic_sampling(AccessPath *path,
                                                                       int64_t ds_level,
                                                                       ObIArray<ObDSResultItem> &ds_result_items,
                                                                       bool &no_ds_data)
{
  int ret = OB_SUCCESS;
  const ObDSResultItem *item = NULL;
  no_ds_data = false;
  if (OB_ISNULL(path) ||
      OB_ISNULL(item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_BASIC_STAT,
                                                                  path->ref_table_id_,
                                                                  ds_result_items)) ||
      OB_ISNULL(item->stat_handle_.stat_) ||
      OB_UNLIKELY(item->stat_handle_.stat_->get_sample_block_ratio() <= 0 ||
                  item->stat_handle_.stat_->get_sample_block_ratio() > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(path), KPC(item), K(ds_result_items));
  } else if (item->stat_handle_.stat_->get_rowcount() == 0 && item->stat_handle_.stat_->get_sample_block_ratio() != 100.0) {
    no_ds_data = true;
  } else {
    int64_t row_count = item->stat_handle_.stat_->get_rowcount();
    row_count = row_count != 0 ? row_count : 1;
    ObCostTableScanInfo &est_cost_info = path->est_cost_info_;
    OptTableMetas &table_metas = path->parent_->get_plan()->get_basic_table_metas();
    OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(path->table_id_);
    bool no_add_micro_block = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
    if (!no_add_micro_block) {
      est_cost_info.table_meta_info_->micro_block_count_ = item->stat_handle_.stat_->get_micro_block_num();
    }
    est_cost_info.table_meta_info_->table_row_count_ = row_count;
    if (OB_ISNULL(table_meta) || OB_UNLIKELY(OB_INVALID_ID == table_meta->get_ref_table_id())) {
      //do nothing
    } else if (OB_FAIL(update_column_metas_by_ds_col_stat(row_count,
                                                          item->stat_handle_.stat_->get_ds_col_stats(),
                                                          table_meta->get_column_metas()))) {
      LOG_WARN("failed to fill ds col stat", K(ret));
    } else {
      table_meta->set_rows(row_count);
      table_meta->set_use_ds_stat();
      table_meta->set_ds_level(ds_level);
    }
  }
  return ret;
}

int ObAccessPathEstimation::update_table_stat_info_by_default(AccessPath *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(path));
  } else if (path->get_output_row_count() > 0) {
    OptTableMetas &table_metas = path->parent_->get_plan()->get_basic_table_metas();
    OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(path->table_id_);
    if (OB_NOT_NULL(table_meta)) {
      table_meta->set_rows(path->get_output_row_count());
      for (int64_t i = 0; i < table_meta->get_column_metas().count(); ++i) {
        table_meta->get_column_metas().at(i).set_default_meta(path->get_output_row_count());
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::estimate_path_rowcount_by_dynamic_sampling(const uint64_t table_id,
                                                                       ObIArray<AccessPath *> &paths,
                                                                       const bool is_inner_path,
                                                                       ObIArray<ObDSResultItem> &ds_result_items)
{
  int ret = OB_SUCCESS;
  const ObDSResultItem *all_filter_item = NULL;
  if (OB_ISNULL(all_filter_item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_OUTPUT_STAT,
                                                                             table_id,
                                                                             ds_result_items)) ||
      OB_ISNULL(all_filter_item->stat_handle_.stat_) ||
      OB_UNLIKELY(all_filter_item->stat_handle_.stat_->get_sample_block_ratio() <= 0 ||
                  all_filter_item->stat_handle_.stat_->get_sample_block_ratio() > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_id), K(ds_result_items));
  } else {
    int64_t output_rowcnt = all_filter_item->stat_handle_.stat_->get_rowcount();
    int64_t micro_block_count = all_filter_item->stat_handle_.stat_->get_micro_block_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
      if (OB_ISNULL(paths.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(paths.at(i)));
      } else if (OB_FAIL(reset_skip_scan_info(paths.at(i)->est_cost_info_,
                                              paths.at(i)->parent_->get_plan()->get_predicate_selectivities(),
                                              paths.at(i)->use_skip_scan_))) {
        LOG_WARN("failed to reset skip scan info", K(ret));
      } else {
        const ObDSResultItem *path_ds_result_item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_FILTER_OUTPUT_STAT,
                                                                                               paths.at(i)->index_id_,
                                                                                               ds_result_items);
        ObCostTableScanInfo &est_cost_info = paths.at(i)->est_cost_info_;
        double sample_ratio = all_filter_item->stat_handle_.stat_->get_sample_block_ratio();
        output_rowcnt = output_rowcnt != 0 ? output_rowcnt : static_cast<int64_t>(100.0 / sample_ratio);
        bool no_add_micro_block = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
        if (!no_add_micro_block) {
          est_cost_info.table_meta_info_->micro_block_count_ = micro_block_count;
          est_cost_info.index_meta_info_.index_micro_block_count_ =
                  est_cost_info.table_meta_info_->micro_block_count_ *
                    (static_cast<double>(est_cost_info.index_meta_info_.index_column_count_) /
                      static_cast<double>(est_cost_info.table_meta_info_->table_column_count_));
        }
        double &logical_row_count = est_cost_info.logical_query_range_row_count_;
        double &physical_row_count = est_cost_info.phy_query_range_row_count_;
        if (path_ds_result_item == NULL) {
          logical_row_count = est_cost_info.table_meta_info_->table_row_count_;
          physical_row_count = logical_row_count;
        } else {
          logical_row_count = path_ds_result_item->stat_handle_.stat_->get_rowcount();
          double tmp_ratio = path_ds_result_item->stat_handle_.stat_->get_sample_block_ratio();
          logical_row_count =  logical_row_count != 0 ? logical_row_count : static_cast<int64_t>(100.0 / tmp_ratio);
          physical_row_count = logical_row_count;
        }
        if (is_inner_path) {
          if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                              *est_cost_info.sel_ctx_,
                                                              est_cost_info.pushdown_prefix_filters_,
                                                              est_cost_info.pushdown_prefix_filter_sel_,
                                                              paths.at(i)->parent_->get_plan()->get_predicate_selectivities()))) {
            LOG_WARN("failed to calculate selectivity", K(est_cost_info.pushdown_prefix_filters_), K(ret));
          } else {
            logical_row_count = logical_row_count * est_cost_info.pushdown_prefix_filter_sel_;
            physical_row_count = logical_row_count;
            output_rowcnt = output_rowcnt * est_cost_info.pushdown_prefix_filter_sel_;
          }
        }
        if (OB_SUCC(ret)) {
          // block sampling
          double block_sample_ratio = est_cost_info.sample_info_.is_block_sample() ?
                0.01 * est_cost_info.sample_info_.percent_ : 1.0;
          logical_row_count *= block_sample_ratio;
          physical_row_count *= block_sample_ratio;

          logical_row_count = std::max(logical_row_count, 1.0);
          physical_row_count = std::max(physical_row_count, 1.0);
          est_cost_info.output_row_count_ = output_rowcnt;
          // row sampling
          double row_sample_ratio = est_cost_info.sample_info_.is_row_sample() ?
                0.01 * est_cost_info.sample_info_.percent_ : 1.0;
          est_cost_info.output_row_count_ *= row_sample_ratio;

          if (est_cost_info.index_meta_info_.is_index_back_) {
            est_cost_info.index_back_row_count_ = logical_row_count;
          }
          est_cost_info.table_filter_sel_ = output_rowcnt * 1.0 / physical_row_count;

          if (OB_FAIL(ret)) {
          } else if (!est_cost_info.ss_ranges_.empty()) {
            int64_t scan_range_count = get_scan_range_count(est_cost_info.ss_ranges_);
            if (scan_range_count == 1) {
              est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_SCAN;
            } else {
              est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_GET;
            }
          } else {
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
        }
        LOG_TRACE("OPT:[DYNAMIC SAPMLING EST ROW COUNT", K(logical_row_count), K(physical_row_count), K(est_cost_info), K(output_rowcnt));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::update_column_metas_by_ds_col_stat(const int64_t rowcount,
                                                               const common::ObOptDSStat::DSColStats &ds_col_stats,
                                                               ObIArray<OptColumnMeta> &col_metas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_metas.count(); ++i) {
    bool found_it = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found_it && j < ds_col_stats.count(); ++j) {
       if (col_metas.at(i).get_column_id() == ds_col_stats.at(j).column_id_) {
        found_it = true;
        if (ds_col_stats.at(j).num_distinct_ > 0) {
          col_metas.at(i).set_ndv(ds_col_stats.at(j).num_distinct_);
        }
        if (ds_col_stats.at(j).num_null_ > 0) {
          col_metas.at(i).set_num_null(ds_col_stats.at(j).num_null_);
        }
      }
    }
    if (!found_it) {
      col_metas.at(i).set_ndv(rowcount);
    }
  }
  LOG_TRACE("update column metas by ds col stat", K(ds_col_stats), K(col_metas), K(rowcount));
  return ret;
}

bool ObAccessPathEstimation::is_retry_ret(int ret)
{
  return ret == OB_NOT_MASTER ||
         ret == OB_RS_NOT_MASTER ||
         ret == OB_TENANT_NOT_IN_SERVER ||
         ret == OB_NO_READABLE_REPLICA ||
         ret == OB_LS_NOT_EXIST ||
         ret == OB_TABLET_NOT_EXIST;
}

int ObAccessPathEstimation::classify_paths(ObIArray<AccessPath *> &paths,
                                           ObIArray<AccessPath *> &normal_paths,
                                           ObIArray<AccessPath *> &geo_paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    if (OB_ISNULL(paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null path", K(ret));
    } else if (paths.at(i)->est_cost_info_.index_meta_info_.is_geo_index_) {
      if (OB_FAIL(geo_paths.push_back(paths.at(i)))) {
        LOG_WARN("failed to push back geo path", K(ret));
      }
    } else if (OB_FAIL(normal_paths.push_back(paths.at(i)))) {
      LOG_WARN("failed to push back normal path", K(ret));
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase

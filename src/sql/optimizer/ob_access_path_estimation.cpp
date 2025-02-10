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
#include "sql/optimizer/ob_storage_estimator.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "ob_opt_est_parameter_normal.h"
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
  ObSEArray<AccessPath *, 4> normal_paths; // incloud normal path and index merge leaf path
  ObSEArray<AccessPath *, 4> geo_paths;
  ObSEArray<IndexMergePath *, 4> index_merge_paths;
  ObBaseTableEstMethod geo_method;

  if (OB_UNLIKELY(paths.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(classify_paths(paths, normal_paths, geo_paths, index_merge_paths))) {
    LOG_WARN("failed to classify paths", K(ret));
  } else if (!normal_paths.empty() &&
             OB_FAIL(inner_estimate_rowcount(ctx, normal_paths, is_inner_path, filter_exprs, method))) {
    LOG_WARN("failed to do estimate rowcount for normal paths", K(ret));
  } else if (!geo_paths.empty() &&
             OB_FAIL(inner_estimate_rowcount(ctx, geo_paths, is_inner_path, filter_exprs, geo_method))) {
    LOG_WARN("failed to do estimate rowcount for geo paths", K(ret));
  } else if (!index_merge_paths.empty() && inner_estimate_index_merge_rowcount(index_merge_paths, method)) {
    LOG_WARN("failed to do estimate rowcount for index merge paths", K(ret));
  } else if (normal_paths.empty() && index_merge_paths.empty() && !geo_paths.empty()) {
    method = geo_method;
  }
  return ret;
}

int ObAccessPathEstimation::inner_estimate_index_merge_rowcount(common::ObIArray<IndexMergePath *> &paths,
                                                                ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  const OptSelectivityCtx* sel_ctx = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    ObSEArray<double, 4> selectivities;
    double sum_child_sel = 0.0;
    double sum_child_row = 0.0;
    const ObTableMetaInfo *table_meta_info = NULL;
    if (OB_ISNULL(paths.at(i)) || OB_ISNULL(paths.at(i)->root_)
        || OB_ISNULL(sel_ctx = paths.at(i)->est_cost_info_.sel_ctx_)
        || OB_ISNULL(table_meta_info = paths.at(i)->est_cost_info_.table_meta_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(paths.at(i)), K(sel_ctx), K(table_meta_info));
    } else if (OB_FAIL(ObOptEstCost::calculate_filter_selectivity(
                        paths.at(i)->est_cost_info_,
                        paths.at(i)->parent_->get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate filter selectivity", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < paths.at(i)->root_->children_.count(); ++j) {
      ObIndexMergeNode *child = paths.at(i)->root_->children_.at(j);
      if (OB_ISNULL(child) || OB_ISNULL(child->ap_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null child node", K(ret), KPC(child));
      } else if (OB_FAIL(selectivities.push_back(1.0 - child->ap_->est_cost_info_.prefix_filter_sel_))) {
        LOG_WARN("failed to push back selectivity", K(ret));
      } else {
        sum_child_sel += child->ap_->est_cost_info_.prefix_filter_sel_;
        sum_child_row += child->ap_->est_cost_info_.output_row_count_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (sum_child_sel < OB_DOUBLE_EPSINON) {
      paths.at(i)->est_cost_info_.prefix_filter_sel_ = 0.0;
      paths.at(i)->est_cost_info_.index_back_row_count_ = 1.0;
      paths.at(i)->est_cost_info_.phy_query_range_row_count_ = 1.0;
      paths.at(i)->est_cost_info_.logical_query_range_row_count_ = 1.0;
      paths.at(i)->est_cost_info_.output_row_count_ = 1.0;
      paths.at(i)->est_cost_info_.est_method_ = method;
    } else {
      paths.at(i)->est_cost_info_.prefix_filter_sel_ = 1.0 - sel_ctx->get_correlation_model().combine_filters_selectivity(selectivities);
      paths.at(i)->est_cost_info_.index_back_row_count_ = (paths.at(i)->est_cost_info_.prefix_filter_sel_ / sum_child_sel) * sum_child_row;
      paths.at(i)->est_cost_info_.phy_query_range_row_count_ = paths.at(i)->est_cost_info_.index_back_row_count_;
      paths.at(i)->est_cost_info_.logical_query_range_row_count_ = paths.at(i)->est_cost_info_.index_back_row_count_;
      paths.at(i)->est_cost_info_.output_row_count_ = (paths.at(i)->est_cost_info_.table_filter_sel_  / paths.at(i)->est_cost_info_.prefix_filter_sel_) * paths.at(i)->est_cost_info_.index_back_row_count_;
      paths.at(i)->est_cost_info_.est_method_ = method;
    }
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
  } else if (OB_FAIL(do_estimate_rowcount(ctx, paths, filter_exprs, valid_methods, method))) {
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
                                                 const ObIArray<ObRawExpr*> &filter_exprs,
                                                 ObBaseTableEstMethod &valid_methods,
                                                 ObBaseTableEstMethod &method)
{
  int ret = OB_SUCCESS;
  bool is_success = true;
  LOG_TRACE("Try to do estimate rowcount", K(method));

  if (OB_UNLIKELY(EST_INVALID == method) ||
      OB_UNLIKELY((method & EST_DS_FULL) && (method & EST_DS_BASIC)) ||
      OB_UNLIKELY((method & EST_DEFAULT) && (method & EST_STAT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected est method", K(ret), K(method));
  }

  if (OB_SUCC(ret) && (method & (EST_DS_BASIC | EST_DS_FULL))) {
    bool only_ds_basic_stat = (method & EST_DS_BASIC);
    if (OB_FAIL(process_dynamic_sampling_estimation(
        ctx, paths, filter_exprs, only_ds_basic_stat, is_success))) {
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

  if (OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  }

  // check is simple scene
  bool is_table_get = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_table_get && i < paths.count(); ++i) {
    if (OB_ISNULL(paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(paths.at(i)));
    } else if (paths.at(i)->get_query_range_provider() != NULL &&
               OB_FAIL(paths.at(i)->get_query_range_provider()->is_get(is_table_get))) {
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
  if (OB_SUCC(ret) && !is_simple_scene && !is_complex_scene && (valid_methods & EST_DS_FULL)) {
    ObSelEstimatorFactory factory(ctx.get_session_info()->get_effective_tenant_id());
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
      } else if (estimator->is_complex_filter_qual()) {
        is_complex_scene = true;
        // path which contains complex filters is complex
        LOG_PRINT_EXPR(TRACE, "Try to use dynamic sampling because of complex filter:", filter);
      }
    }
  }

  //check opt stats is expired
  if (OB_SUCC(ret) && !is_simple_scene && !is_complex_scene && can_use_ds) {
    const ObLogPlan* log_plan = NULL;
    const OptTableMeta *table_meta = NULL;
    if (!paths.empty() && paths.at(0)->parent_ != NULL &&
        (log_plan = paths.at(0)->parent_->get_plan()) != NULL &&
        (table_meta = log_plan->get_basic_table_metas().get_table_meta_by_table_id(paths.at(0)->table_id_)) != NULL &&
        table_meta->is_opt_stat_expired() &&
        !table_meta->is_stat_locked()) {
      is_simple_scene = false;
      is_complex_scene = true;
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
  int64_t range_limit = 0;
  int64_t partition_limit = 0;
  if (OB_ISNULL(path) || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is invalid", K(ret), K(path), K(ctx.get_session_info()));
  } else if (OB_FAIL(is_storage_estimation_enabled(path->parent_->get_plan(), ctx ,path->table_id_, path->ref_table_id_, can_use))) {
    LOG_WARN("fail to do check_path_can_use_storage_estimation ", K(ret), K(path));
  } else if (!can_use) {
    can_use = false;
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::RANGE_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_RANGE_INDEX_DIVE_LIMIT,
                                                                   range_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::PARTITION_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_PARTITION_INDEX_DIVE_LIMIT,
                                                                   partition_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else {
    const ObTablePartitionInfo *part_info = NULL;
    if (OB_ISNULL(part_info = path->table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table partition info is null", K(ret), K(part_info));
    } else {
      int64_t scan_range_count = get_scan_range_count(path->get_query_ranges());
      if (range_limit < 0 && partition_limit < 0) {
        // rollback to the old strategy iff both variables are negative
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
      } else {
        can_use = (scan_range_count > 0);
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
  int64_t partition_limit = 0;
  int64_t range_limit = 0;
  ObCandiTabletLocSEArray chosen_partitions;
  ObSEArray<common::ObNewRange, 4> chosen_scan_ranges;
  ObSEArray<EstResultHelper, 4> result_helpers;
  OPT_TRACE_TIME_USED;
  OPT_TRACE_MEM_USED;
  OPT_TRACE_TITLE("BEGIN STORAGE CARDINALITY ESTIMATION");
  bool force_leader_estimation = false;
  
  force_leader_estimation = OB_FAIL(OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);
  ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_session_info()) ||
      OB_ISNULL(ctx.get_exec_ctx()) ||
      OB_ISNULL(ctx.get_exec_ctx()->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is invalid", K(ret), K(ctx.get_session_info()), K(ctx.get_exec_ctx()));
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::PARTITION_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_PARTITION_INDEX_DIVE_LIMIT,
                                                                   partition_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::RANGE_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_RANGE_INDEX_DIVE_LIMIT,
                                                                   range_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else {
    if (partition_limit < 0 && range_limit < 0) {
      partition_limit = 1;
      range_limit = ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
    }
  }
  // for each access path, find a partition/server for estimation
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    AccessPath *ap = NULL;
    ObBatchEstTasks *task = NULL;
    EstResultHelper *result_helper = NULL;
    RangePartitionHelper calc_range_partition_helper;
    chosen_scan_ranges.reuse();
    chosen_partitions.reuse();
    SMART_VARS_3((ObTablePartitionInfo, tmp_part_info),
                 (ObPhysicalPlanCtx, tmp_plan_ctx, arena),
                 (ObExecContext, tmp_exec_ctx, arena)) {
      const ObTablePartitionInfo *table_part_info = NULL;
      int64_t total_part_cnt = 0;
      if (OB_ISNULL(ap = paths.at(i)) || OB_ISNULL(ap->parent_) || OB_ISNULL(ap->parent_->get_plan()) ||
          OB_ISNULL(table_part_info = ap->table_partition_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access path is invalid", K(ret), K(ap), K(table_part_info), K(ctx.get_exec_ctx()));
      } else if (OB_ISNULL(result_helper = result_helpers.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc", K(ret));
      } else {
        result_helper->path_ = ap;
        result_helper->result_.valid_partition_count_ = ap->est_cost_info_.index_meta_info_.index_part_count_;
        result_helper->total_scan_range_count_ = get_scan_range_count(ap->est_cost_info_.ranges_);
        total_part_cnt = table_part_info->get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
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
      } else if (!ap->is_global_index_ && ap->ref_table_id_ != ap->index_id_ &&
                OB_FAIL(tmp_part_info.replace_final_location_key(tmp_exec_ctx,
                                                                 ap->index_id_,
                                                                 true))) {
        LOG_WARN("failed to replace final location key", K(ret));
      } else if (OB_FAIL(calc_range_partition_helper.init(ap->get_table_id(),
                                                          ap->is_global_index_ ? ap->get_index_table_id() : ap->get_ref_table_id(),
                                                          ap->parent_->get_plan()->get_stmt(),
                                                          *table_part_info,
                                                          ap->est_cost_info_.range_columns_))) {
        LOG_WARN("failed to init range partition helper", K(ret));
      } else if (!calc_range_partition_helper.get_all_partition_is_valid()) {
        // choose partitions for each range
        const ObCandiTabletLocIArray &ori_partitions = table_part_info->get_phy_tbl_location_info().get_phy_part_loc_info_list();
        const ObCandiTabletLocIArray &index_partitions = tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list();
        if (OB_FAIL(add_storage_estimation_task_by_ranges(ctx,
                                                          arena,
                                                          tmp_exec_ctx,
                                                          calc_range_partition_helper,
                                                          prefer_addrs,
                                                          *ap,
                                                          tasks,
                                                          partition_limit,
                                                          range_limit,
                                                          ori_partitions,
                                                          index_partitions,
                                                          *result_helper))) {
          LOG_WARN("failed to add task by ranges", K(ret));
        }
      } else {
        const ObCandiTabletLocIArray &index_partitions = tmp_part_info.get_phy_tbl_location_info().get_phy_part_loc_info_list();
        if (OB_FAIL(add_storage_estimation_task(ctx,
                                                arena,
                                                prefer_addrs,
                                                *ap,
                                                tasks,
                                                partition_limit,
                                                range_limit,
                                                index_partitions,
                                                *result_helper))) {
          LOG_WARN("failed to add task by ranges", K(ret));
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
      OPT_TRACE("storage estimation result is not reliable");
      OPT_TRACE(*tasks.at(i));
      LOG_WARN("storage estimation result is not reliable", KPC(tasks.at(i)));
    }
  }
  NG_TRACE(storage_estimation_end);

  if (OB_SUCC(ret) && !need_fallback &&
      OB_FAIL(process_storage_estimation_result(tasks, result_helpers, is_success))) {
    LOG_WARN("failed to process result", K(ret));
  }

  // deconstruct ObBatchEstTasks
  for (int64_t i = 0; i < tasks.count(); ++i) {
    if (NULL != tasks.at(i)) {
      tasks.at(i)->~ObBatchEstTasks();
      tasks.at(i) = NULL;
    }
  }
  is_success &= !need_fallback;
  OPT_TRACE_TIME_USED;
  OPT_TRACE_MEM_USED;
  return ret;
}

int ObAccessPathEstimation::add_storage_estimation_task(ObOptimizerContext &ctx,
                                                        ObIAllocator &arena,
                                                        ObIArray<ObAddr> &prefer_addrs,
                                                        AccessPath &ap,
                                                        ObIArray<ObBatchEstTasks *> &tasks,
                                                        const int64_t partition_limit,
                                                        const int64_t range_limit,
                                                        const ObCandiTabletLocIArray &index_partitions,
                                                        EstResultHelper &result_helper)
{
  int ret = OB_SUCCESS;
  OPT_TRACE("Choose partitions and ranges for index", ap.index_id_, "to estimate rowcount");
  ObSEArray<common::ObNewRange, 4> chosen_scan_ranges;
  ObCandiTabletLocSEArray chosen_partitions;
  const ObTableMetaInfo *table_meta = NULL;
  if (OB_ISNULL(table_meta = ap.est_cost_info_.table_meta_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path is invalid", K(ret), K(ap), K(table_meta));
  } else if (OB_FAIL(choose_storage_estimation_partitions(partition_limit,
                                                          index_partitions,
                                                          chosen_partitions))) {
    LOG_WARN("failed to choose partitions", K(ret));
  } else if (OB_FAIL(choose_storage_estimation_ranges(range_limit,
                                                      ap.est_cost_info_.ranges_,
                                                      ap.est_cost_info_.index_meta_info_.is_geo_index_,
                                                      chosen_scan_ranges))) {
    LOG_WARN("failed to choose scan ranges", K(ret));
  } else {
    result_helper.est_scan_range_count_ = chosen_scan_ranges.count();
    OPT_TRACE_BEGIN_SECTION;
    OPT_TRACE("partitions :", chosen_partitions);
    OPT_TRACE("ranges :", chosen_scan_ranges);
    OPT_TRACE_END_SECTION;
    LOG_TRACE("choose partitions and ranges to estimate rowcount", K(ap.index_id_), K(chosen_partitions));
    LOG_TRACE("choose ranges to estimate rowcount", K(chosen_scan_ranges));
    for (int64_t i = 0; OB_SUCC(ret) && i < chosen_partitions.count(); i ++) {
      EstimatedPartition best_index_part;
      ObBatchEstTasks *task = NULL;
      if (OB_FAIL(get_storage_estimation_task(ctx,
                                              arena,
                                              chosen_partitions.at(i),
                                              *table_meta,
                                              prefer_addrs,
                                              tasks,
                                              best_index_part,
                                              task))) {
        LOG_WARN("failed to get task", K(ret));
      } else if (NULL != task) {
        if (OB_FAIL(add_index_info(ctx, arena, task, best_index_part, ap, chosen_scan_ranges))) {
          LOG_WARN("failed to add task info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::add_storage_estimation_task_by_ranges(ObOptimizerContext &ctx,
                                                                  ObIAllocator &arena,
                                                                  ObExecContext &exec_ctx,
                                                                  RangePartitionHelper &calc_range_partition_helper,
                                                                  ObIArray<ObAddr> &prefer_addrs,
                                                                  AccessPath &ap,
                                                                  ObIArray<ObBatchEstTasks *> &tasks,
                                                                  const int64_t partition_limit,
                                                                  const int64_t range_limit,
                                                                  const ObCandiTabletLocIArray &ori_partitions,
                                                                  const ObCandiTabletLocIArray &index_partitions,
                                                                  EstResultHelper &result_helper)
{
  int ret = OB_SUCCESS;
  OPT_TRACE("Index", ap.index_id_,
    "contains partition columns, choose different partitions for each range to estimate rowcount");
  ObSEArray<ObTabletID, 10> tablet_ids;
  ObSEArray<common::ObNewRange, 4> chosen_scan_ranges;
  ObSEArray<common::ObNewRange, 1> chosen_range;
  ObCandiTabletLocSEArray valid_partitions_for_range;
  ObCandiTabletLocSEArray chosen_partitions;
  result_helper.different_parts_ = true;
  const ObTableMetaInfo *table_meta = NULL;
  if (OB_ISNULL(table_meta = ap.est_cost_info_.table_meta_info_) ||
      OB_UNLIKELY(ori_partitions.count() != index_partitions.count()) ||
      OB_UNLIKELY(ori_partitions.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), K(table_meta), K(ori_partitions), K(index_partitions));
  } else if (OB_FAIL(choose_storage_estimation_ranges(range_limit,
                                                      ap.est_cost_info_.ranges_,
                                                      ap.est_cost_info_.index_meta_info_.is_geo_index_,
                                                      chosen_scan_ranges))) {
    LOG_WARN("failed to choose scan ranges", K(ret));
  } else if (OB_FAIL(result_helper.range_result_.prepare_allocate(chosen_scan_ranges.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    result_helper.est_scan_range_count_ = chosen_scan_ranges.count();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < chosen_scan_ranges.count(); i ++) {
    valid_partitions_for_range.reuse();
    chosen_range.reuse();
    chosen_partitions.reuse();
    tablet_ids.reuse();
    if (OB_FAIL(chosen_range.push_back(chosen_scan_ranges.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(calc_range_partition_helper.get_scan_range_partitions(exec_ctx,
                                                                             chosen_scan_ranges.at(i),
                                                                             tablet_ids))) {
      LOG_WARN("failed to get scan range partitions", K(chosen_scan_ranges.at(i)));
    } else if (!tablet_ids.empty()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < ori_partitions.count(); j ++) {
        if (ObOptimizerUtil::find_item(tablet_ids,
                        ori_partitions.at(j).get_partition_location().get_tablet_id()) &&
            OB_FAIL(valid_partitions_for_range.push_back(index_partitions.at(j)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!valid_partitions_for_range.empty()) {
        if (OB_FAIL(choose_storage_estimation_partitions(partition_limit,
                                                         valid_partitions_for_range,
                                                         chosen_partitions))) {
          LOG_WARN("failed to choose partitions", K(ret));
        } else {
          result_helper.range_result_.at(i).valid_partition_count_ = valid_partitions_for_range.count();
        }
      } else {
        // no valid partitions, choose random partitions from all partitions
        if (OB_FAIL(choose_storage_estimation_partitions(partition_limit,
                                                         index_partitions,
                                                         chosen_partitions))) {
          LOG_WARN("failed to choose partitions", K(ret));
        } else {
          result_helper.range_result_.at(i).valid_partition_count_ = index_partitions.count();
        }
      }
    }
    if (OB_SUCC(ret)) {
      OPT_TRACE("Range", chosen_scan_ranges.at(i), "has valid tablets:", tablet_ids);
      OPT_TRACE_BEGIN_SECTION;
      OPT_TRACE("Choose partitions", chosen_partitions);
      OPT_TRACE_END_SECTION;
      LOG_TRACE("choose range and partitions to estimate rowcount",
          K(ap.index_id_), K(chosen_scan_ranges.at(i)), K(chosen_partitions), K(valid_partitions_for_range), K(tablet_ids));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < chosen_partitions.count(); j ++) {
      EstimatedPartition best_index_part;
      ObBatchEstTasks *task = NULL;
      if (OB_FAIL(get_storage_estimation_task(ctx,
                                              arena,
                                              chosen_partitions.at(j),
                                              *table_meta,
                                              prefer_addrs,
                                              tasks,
                                              best_index_part,
                                              task))) {
        LOG_WARN("failed to get task", K(ret));
      } else if (NULL != task) {
        if (OB_FAIL(add_index_info(ctx, arena, task, best_index_part, ap, chosen_range, i))) {
          LOG_WARN("failed to add task info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::get_storage_estimation_task(ObOptimizerContext &ctx,
                                                        ObIAllocator &arena,
                                                        const ObCandiTabletLoc &partition,
                                                        const ObTableMetaInfo &table_meta,
                                                        ObIArray<ObAddr> &prefer_addrs,
                                                        ObIArray<ObBatchEstTasks *> &tasks,
                                                        EstimatedPartition &best_index_part,
                                                        ObBatchEstTasks *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  void *ptr = NULL;
  const bool can_use_remote = true;
  const bool force_leader_estimation = OB_SUCCESS != (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS);
  if (OB_FAIL(ObSQLUtils::choose_best_replica_for_estimation(
                      partition,
                      ctx.get_local_server_addr(),
                      prefer_addrs,
                      !can_use_remote,
                      best_index_part))) {
    LOG_WARN("failed to choose best partition for estimation", K(ret));
  } else if (force_leader_estimation &&
            OB_FAIL(choose_leader_replica(partition,
                                          can_use_remote,
                                          ctx.get_local_server_addr(),
                                          best_index_part))) {
    LOG_WARN("failed to choose leader replica", K(ret));
  } else if (!best_index_part.is_valid()) {
    // does not do storage estimation for this index partition
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
  return ret;
}

int ObAccessPathEstimation::process_storage_estimation_result(ObIArray<ObBatchEstTasks *> &tasks,
                                                              ObIArray<EstResultHelper> &result_helpers,
                                                              bool &is_reliable)
{
  int ret = OB_SUCCESS;
  is_reliable = true;
  OPT_TRACE("Process storage estimation result");
  OPT_TRACE_BEGIN_SECTION;

  // Group estimation results by path and range
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const ObBatchEstTasks *task = tasks.at(i);
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param", K(ret));
    } else {
      OPT_TRACE(*tasks.at(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < task->paths_.count(); ++j) {
      const obrpc::ObEstPartResElement &res = task->res_.index_param_res_.at(j);
      AccessPath *path = task->paths_.at(j);
      int64_t idx = -1;
      const ObEstPartArgElement &index_param = task->arg_.index_params_.at(j);
      if (OB_ISNULL(path) || OB_UNLIKELY(j >= task->arg_.index_params_.count()) ||
          OB_UNLIKELY(j >= task->range_idx_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null path", K(ret));
      } else if (OB_FAIL(append(path->est_records_, res.est_records_))) {
        LOG_WARN("failed to assign estimation records", K(ret));
      } else if (!res.reliable_) {
        // do nothing
      } else if (OB_FAIL(get_result_helper(result_helpers, path, idx))) {
        LOG_WARN("failed to get helper", K(ret));
      } else if (task->range_idx_.at(j) < 0) {
        EstResultHelper &helper = result_helpers.at(idx);
        helper.result_.logical_row_count_ += res.logical_row_count_;
        helper.result_.physical_row_count_ += res.physical_row_count_;
        helper.result_.est_partition_count_ += 1;
      } else if (task->range_idx_.at(j) >= result_helpers.at(idx).range_result_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", K(task->range_idx_.at(j)), K(result_helpers.at(idx)));
      } else {
        int64_t range_idx = task->range_idx_.at(j);
        EstResultHelper &helper = result_helpers.at(idx);
        helper.range_result_.at(range_idx).logical_row_count_ += res.logical_row_count_;
        helper.range_result_.at(range_idx).physical_row_count_ += res.physical_row_count_;
        helper.range_result_.at(range_idx).est_partition_count_ += 1.0;
      }
    }
  }
  OPT_TRACE_END_SECTION;

  // Scale up results of each range based on the partition sample ratio
  // Sum and scale up results of ranges for each path based on the range sample ratio
  for (int64_t i = 0; i < result_helpers.count(); i ++) {
    EstResultHelper &helper = result_helpers.at(i);
    double est_range_count = 0;
    if (helper.different_parts_) {
      for (int64_t j = 0; j < helper.range_result_.count(); j ++) {
        if (helper.range_result_.at(j).est_partition_count_ > 0) {
          double part_sample_ratio = 1.0 * helper.range_result_.at(j).valid_partition_count_ /
                                     helper.range_result_.at(j).est_partition_count_;
          part_sample_ratio = MAX(part_sample_ratio, 1.0);
          helper.result_.logical_row_count_ += helper.range_result_.at(j).logical_row_count_ * part_sample_ratio;
          helper.result_.physical_row_count_ += helper.range_result_.at(j).physical_row_count_ * part_sample_ratio;
          est_range_count += 1.0;
        }
      }
    } else {
      if (helper.result_.est_partition_count_ > 0) {
        double part_sample_ratio = 1.0 * helper.result_.valid_partition_count_ /
                                   helper.result_.est_partition_count_;
        part_sample_ratio = std::max(part_sample_ratio, 1.0);
        helper.result_.logical_row_count_ *= part_sample_ratio;
        helper.result_.physical_row_count_ *= part_sample_ratio;
        est_range_count = helper.est_scan_range_count_;
      }
    }
    if (est_range_count > 0) {
      double range_sample_ratio = 1.0 * helper.total_scan_range_count_ / est_range_count;
      range_sample_ratio = MAX(range_sample_ratio, 1.0);
      helper.result_.logical_row_count_ *= range_sample_ratio;
      helper.result_.physical_row_count_ *= range_sample_ratio;
    } else {
      helper.result_.logical_row_count_ = -1.0;
    }
  }

  // Check whether results are reliable
  for (int64_t i = 0; OB_SUCC(ret) && is_reliable && i < result_helpers.count(); ++i) {
    // all choosed partitions are empty, do not use the result
    if ((result_helpers.at(i).path_->is_global_index_ &&
        result_helpers.at(i).path_->est_cost_info_.ranges_.count() == 1 &&
        result_helpers.at(i).path_->est_cost_info_.ranges_.at(0).is_whole_range() &&
        result_helpers.at(i).result_.logical_row_count_ == 0) ||
        result_helpers.at(i).result_.logical_row_count_ < 0) {
      is_reliable = false;
      LOG_WARN("storage estimation result is not reliable", K(result_helpers.at(i)), K(tasks));
      OPT_TRACE("storage estimation result is not reliable for index ", result_helpers.at(i).path_->index_id_);
    }
  }

  // Put results into paths
  if (is_reliable) {
    for (int64_t i = 0; OB_SUCC(ret) && i < result_helpers.count(); ++i) {
      AccessPath *path = result_helpers.at(i).path_;
      bool new_range_with_exec_param = (path->get_query_range_provider() != NULL &&
                                        path->get_query_range_provider()->is_new_query_range() &&
                                        path->get_query_range_provider()->has_exec_param());
      if (result_helpers.at(i).result_.logical_row_count_ >= 0 &&
          OB_FAIL(estimate_prefix_range_rowcount(result_helpers.at(i).result_.logical_row_count_,
                                                 result_helpers.at(i).result_.physical_row_count_,
                                                 new_range_with_exec_param,
                                                 path->est_cost_info_))) {
        LOG_WARN("failed to estimate prefix range rowcount", K(ret));
      } else if (OB_FAIL(fill_cost_table_scan_info(path->est_cost_info_))) {
        LOG_WARN("failed to fill cost table scan info", K(ret));
      }
      OPT_TRACE("The storage estimation result of index", result_helpers.at(i).path_->index_id_, "is",
                result_helpers.at(i).result_.logical_row_count_, "(logical) and",
                result_helpers.at(i).result_.physical_row_count_, "(physical)");
    }
  }
  LOG_TRACE("succeed to process storage estimation result", K(is_reliable), K(result_helpers));
  return ret;
}

int ObAccessPathEstimation::get_result_helper(ObIArray<EstResultHelper> &result_helpers,
                                              AccessPath *path,
                                              int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  for (int64_t i = 0; -1 == idx && i < result_helpers.count(); i ++) {
    if (result_helpers.at(i).path_ == path) {
      idx = i;
    }
  }
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get result helper", K(ret));
  }
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
    const double res_logical_row_count,
    const double res_physical_row_count,
    bool new_range_with_exec_param,
    ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  double &logical_row_count = est_cost_info.logical_query_range_row_count_;
  double &physical_row_count = est_cost_info.phy_query_range_row_count_;
  logical_row_count = res_logical_row_count;
  physical_row_count = res_physical_row_count;
  double get_range_count = 1.0 * get_get_range_count(est_cost_info.ranges_);

  logical_row_count += get_range_count;
  physical_row_count += get_range_count;

  // NLJ or SPF push down prefix filters
  if (new_range_with_exec_param) {
    /**
     * new query range extraction always get (min; max) for range graph with exec param.
     * for NLJ push down path with expr (c1 = 1 and c2 = ?). old query range generate
     * (1, min; 1, max), New query range generate (min, min; max, max). This behavior will
     * cause row estimate with new query range get a larger result.Hence, we need multiple
     * prefix_filter_sel for push down path with exec param to get a more accurate row count.
    */
    logical_row_count  *= est_cost_info.prefix_filter_sel_;
    physical_row_count *= est_cost_info.prefix_filter_sel_;
  }
  logical_row_count  *= est_cost_info.pushdown_prefix_filter_sel_;
  physical_row_count *= est_cost_info.pushdown_prefix_filter_sel_;

  // skip scan postfix range conditions
  logical_row_count   *= est_cost_info.ss_postfix_range_filters_sel_;
  physical_row_count  *= est_cost_info.ss_postfix_range_filters_sel_;

  LOG_TRACE("OPT:[STORAGE EST ROW COUNT]",
            K(logical_row_count), K(physical_row_count), K(get_range_count),
            K(res_logical_row_count), K(res_physical_row_count),
            K(est_cost_info.index_meta_info_.index_part_count_),
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

int ObAccessPathEstimation::choose_storage_estimation_partitions(const int64_t partition_limit,
                                                                 const ObCandiTabletLocIArray &partitions,
                                                                 ObCandiTabletLocIArray &chosen_partitions)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> min_max_index;
  int64_t min_index = 0;
  int64_t max_index = 0;
  if (partition_limit <= 0 || partition_limit >= partitions.count()) {
    if (OB_FAIL(chosen_partitions.assign(partitions))) {
      LOG_WARN("failed to assign", K(ret));
    }
  } else {
    for (int64_t i = 1; i < partitions.count(); i ++) {
      if (partitions.at(i).get_partition_location().get_tablet_id().id() <
          partitions.at(min_index).get_partition_location().get_tablet_id().id()) {
        min_index = i;
      }
      if (partitions.at(i).get_partition_location().get_tablet_id().id() >
          partitions.at(max_index).get_partition_location().get_tablet_id().id()) {
        max_index = i;
      }
    }
    if (OB_FAIL(min_max_index.add_member(min_index)) ||
        OB_FAIL(min_max_index.add_member(max_index))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::choose_random_members(
                          STORAGE_EST_SAMPLE_SEED, partitions, partition_limit,
                          chosen_partitions, &min_max_index))) {
      LOG_WARN("failed to choose random partitions", K(ret), K(partition_limit));
    }
  }
  return ret;
}

int ObAccessPathEstimation::choose_storage_estimation_ranges(const int64_t range_limit,
                                                             const ObRangesArray &ranges,
                                                             bool is_geo_index,
                                                             ObIArray<common::ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObNewRange, 4> get_ranges;
  ObSEArray<common::ObNewRange, 4> valid_ranges;
  if (ranges.empty()) {
    // do nothing
  } else if (is_geo_index) {
    const ObIArray<common::ObNewRange> &geo_ranges = ranges;
    int64_t total_cnt = geo_ranges.count();
    if (geo_ranges.at(0).get_start_key().get_obj_cnt() < SPATIAL_ROWKEY_MIN_NUM) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The count of rowkey from spatial_index_table is wrong.", K(ret), K(geo_ranges.at(0).get_start_key().get_obj_cnt()));
    } else if (total_cnt <= range_limit || range_limit <= 0) {
      if (OB_FAIL(scan_ranges.assign(geo_ranges))) {
        LOG_WARN("failed to assgin valid ranges", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < geo_ranges.count(); ++i) {
        const ObNewRange &range = geo_ranges.at(i);
        if (is_multi_geo_range(range)) {
          if (OB_FAIL(scan_ranges.push_back(range))) {
            LOG_WARN("failed to push back scan range", K(ret));
          }
        } else {
          if (OB_FAIL(get_ranges.push_back(range))) {
            LOG_WARN("failed to push back scan range", K(ret));
          }
        }
      }
      // push_back scan_range for first priority
      if (OB_FAIL(ret) || scan_ranges.count() == range_limit) {
      } else if (scan_ranges.count() > range_limit) {
        if (OB_FAIL(ObOptimizerUtil::choose_random_members(STORAGE_EST_SAMPLE_SEED, scan_ranges, range_limit, valid_ranges))) {
          LOG_WARN("failed to choose random ranges", K(ret), K(range_limit), K(scan_ranges));
        } else if (OB_FAIL(scan_ranges.assign(valid_ranges))) {
          LOG_WARN("failed to assgin valid ranges", K(ret));
        }
      } else {
        if (OB_FAIL(ObOptimizerUtil::choose_random_members(STORAGE_EST_SAMPLE_SEED, get_ranges, range_limit - scan_ranges.count(), valid_ranges))) {
          LOG_WARN("failed to choose random ranges", K(ret), K(range_limit), K(scan_ranges));
        } else if (OB_FAIL(append(scan_ranges, valid_ranges))) {
          LOG_WARN("failed to append valid ranges", K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(
                    ranges,
                    get_ranges,
                    scan_ranges))) {
      LOG_WARN("failed to clasiffy get scan ranges", K(ret));
    } else if (scan_ranges.count() > range_limit && range_limit > 0) {
      if (OB_FAIL(ObOptimizerUtil::choose_random_members(STORAGE_EST_SAMPLE_SEED, scan_ranges, range_limit, valid_ranges))) {
        LOG_WARN("failed to choose random ranges", K(ret), K(range_limit), K(scan_ranges));
      } else if (OB_FAIL(scan_ranges.assign(valid_ranges))) {
        LOG_WARN("failed to assgin valid ranges", K(ret));
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::add_index_info(ObOptimizerContext &ctx,
                                           ObIAllocator &allocator,
                                           ObBatchEstTasks *task,
                                           const EstimatedPartition &part,
                                           AccessPath &ap,
                                           const ObIArray<common::ObNewRange> &chosen_scan_ranges,
                                           int64_t range_idx/* = -1*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObNewRange, 4> scan_ranges;
  obrpc::ObEstPartArgElement *index_est_arg = NULL;
  if (OB_ISNULL(task) || OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid access path or batch task", K(ret), K(task), K(ap));
  } else if (OB_FAIL(scan_ranges.assign(chosen_scan_ranges))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_UNLIKELY(task->addr_ != part.addr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access path uses invalid batch task", K(ret), K(task->addr_), K(part.addr_));
  } else if (OB_FAIL(task->paths_.push_back(&ap)) ||
             OB_FAIL(task->range_idx_.push_back(range_idx))) {
    LOG_WARN("failed to push back access path", K(ret));
  } else if (OB_ISNULL(index_est_arg = task->arg_.index_params_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate index argument", K(ret));
  } else if (OB_FAIL(get_key_ranges(ctx, allocator, part.tablet_id_, ap, scan_ranges))) {
    LOG_WARN("failed to get key ranges", K(ret));
  } else {
    index_est_arg->index_id_ = ap.index_id_;
    index_est_arg->scan_flag_.index_back_ = ap.est_cost_info_.index_meta_info_.is_index_back_;
    index_est_arg->range_columns_count_ = ap.est_cost_info_.range_columns_.count();
    index_est_arg->tablet_id_ = part.tablet_id_;
    index_est_arg->ls_id_ = part.ls_id_;
    index_est_arg->tenant_id_ = ctx.get_session_info()->get_effective_tenant_id();
    index_est_arg->tx_id_ = ctx.get_session_info()->get_tx_id();
  }
  // FIXME, move following codes
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; ap.is_global_index_ && i < scan_ranges.count(); ++i) {
      scan_ranges.at(i).table_id_ = ap.index_id_;
    }
    if (FAILEDx(construct_scan_range_batch(allocator, scan_ranges, index_est_arg->batch_))) {
      LOG_WARN("failed to construct scan range batch", K(ret));
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
  ObQueryRangeProvider *query_range_provider = ap.get_query_range_provider();
  if (OB_ISNULL(query_range_provider) || !query_range_provider->is_ss_range()
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
                                            query_range_provider->get_skip_scan_offset(),
                                            prefix_exprs))) {
        LOG_WARN("failed to get skip scan prefix expers", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::update_table_meta_info(log_plan->get_basic_table_metas(),
                                                                  tmp_metas,
                                                                  log_plan->get_selectivity_ctx(),
                                                                  ap.get_table_id(),
                                                                  prefix_range_row_count,
                                                                  query_range_provider->get_range_exprs(),
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
      int64_t size = scan_ranges.count();
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
  bool bret = true;
  for (int64_t i = 0; bret && i < res_.index_param_res_.count(); ++i) {
    bret = res_.index_param_res_.at(i).reliable_;
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
  int64_t partition_limit = 0;
  if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::PARTITION_INDEX_DIVE_LIMIT,
                                                            ctx.get_session_info(),
                                                            share::SYS_VAR_PARTITION_INDEX_DIVE_LIMIT,
                                                            partition_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else if (is_virtual_table(meta.ref_table_id_) &&
      !share::is_oracle_mapping_real_virtual_table(meta.ref_table_id_)) {
    //do nothing
  } else if (is_external_object_id(meta.ref_table_id_)) {
    //do nothing
  } else if (part_loc_info_array.count() == 1) {
    if (OB_FAIL(storage_estimate_full_table_rowcount(ctx, part_loc_info_array.at(0), meta))) {
      LOG_WARN("failed to storage estimate full table rowcount", K(ret));
    } else {
      LOG_TRACE("succeed to storage estimate full table rowcount", K(meta));
    }
  } else if (part_loc_info_array.count() > 1 && partition_limit >= 0) {
    if (OB_FAIL(storage_estimate_range_rowcount(ctx, part_loc_info_array, true, NULL, meta))) {
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

int ObAccessPathEstimation::storage_estimate_range_rowcount(ObOptimizerContext &ctx,
                                                            const ObCandiTabletLocIArray &part_loc_infos,
                                                            bool estimate_whole_range,
                                                            const ObRangesArray *ranges,
                                                            ObTableMetaInfo &meta)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("CardEstimation");
  ObArray<ObBatchEstTasks *> tasks;
  ObArray<ObAddr> prefer_addrs;
  ObCandiTabletLocSEArray chosen_partitions;
  ObSEArray<ObNewRange, 4> chosen_scan_ranges;
  ObRangesArray whole_range;
  bool need_fallback = false;
  int64_t partition_limit = 0;
  int64_t range_limit = 0;
  int64_t total_part_cnt = part_loc_infos.count();
  if (OB_ISNULL(ctx.get_session_info()) || (!estimate_whole_range && OB_ISNULL(ranges))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if ((is_virtual_table(meta.ref_table_id_) &&
              !share::is_oracle_mapping_real_virtual_table(meta.ref_table_id_))
             || EXTERNAL_TABLE == meta.table_type_) {
    need_fallback = true;
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::PARTITION_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_PARTITION_INDEX_DIVE_LIMIT,
                                                                   partition_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else if (OB_FAIL(ctx.get_global_hint().opt_params_.get_sys_var(ObOptParamHint::RANGE_INDEX_DIVE_LIMIT,
                                                                   ctx.get_session_info(),
                                                                   share::SYS_VAR_RANGE_INDEX_DIVE_LIMIT,
                                                                   range_limit))) {
    LOG_WARN("failed to get hint system variable", K(ret));
  } else {
    if (partition_limit < 0 && range_limit < 0) {
      partition_limit = 1;
      range_limit = ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
    }
    // make whole range if need
    if (estimate_whole_range) {
      ObNewRange *range = NULL;
      if (OB_FAIL(ObSQLUtils::make_whole_range(arena,
                                               meta.ref_table_id_,
                                               meta.table_rowkey_count_,
                                               range))) {
        LOG_WARN("failed to make whole range", K(ret));
      } else if (OB_ISNULL(range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null range", K(ret));
      } else if (OB_FAIL(whole_range.push_back(*range))) {
        LOG_WARN("failed to push back range", K(ret));
      } else {
        ranges = &whole_range;
      }
    }
  }

  if (OB_FAIL(ret) || need_fallback) {
  } else if (OB_ISNULL(ranges)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ranges is null", K(ret));
  } else if (OB_FAIL(choose_storage_estimation_ranges(range_limit, *ranges, false, chosen_scan_ranges))) {
    LOG_WARN("failed to choose scan ranges", K(ret));
  } else if (OB_FAIL(choose_storage_estimation_partitions(partition_limit,
                                                          part_loc_infos,
                                                          chosen_partitions))) {
    LOG_WARN("failed to choose partitions", K(ret));
  } else {
    LOG_TRACE("choose partitions to estimate rowcount", K(chosen_partitions));
    LOG_TRACE("choose ranges to estimate rowcount", K(chosen_scan_ranges));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < chosen_partitions.count(); i ++) {
    EstimatedPartition best_index_part;
    ObBatchEstTasks *task = NULL;
    if (OB_FAIL(get_storage_estimation_task(ctx,
                                            arena,
                                            chosen_partitions.at(i),
                                            meta,
                                            prefer_addrs,
                                            tasks,
                                            best_index_part,
                                            task))) {
      LOG_WARN("failed to get task", K(ret));
    } else if (NULL != task) {
      obrpc::ObEstPartArgElement path_arg;
      task->addr_ = best_index_part.addr_;
      path_arg.scan_flag_.index_back_ = 0;
      path_arg.index_id_ = meta.ref_table_id_;
      path_arg.range_columns_count_ = meta.table_rowkey_count_;
      path_arg.batch_.type_ = ObSimpleBatch::T_SCAN;
      path_arg.tablet_id_ = best_index_part.tablet_id_;
      path_arg.ls_id_ = best_index_part.ls_id_;
      path_arg.tenant_id_ = ctx.get_session_info()->get_effective_tenant_id();
      path_arg.tx_id_ = ctx.get_session_info()->get_tx_id();
      if (OB_FAIL(construct_scan_range_batch(ctx.get_allocator(), chosen_scan_ranges, path_arg.batch_))) {
        LOG_WARN("failed to construct scan range batch", K(ret));
      } else if (OB_FAIL(task->arg_.index_params_.push_back(path_arg))) {
        LOG_WARN("failed to add primary key estimation arg", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < tasks.count(); ++i) {
    ObBatchEstTasks *task = NULL;
    if (OB_ISNULL(task = tasks.at(i))) {
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
    } else if (!tasks.at(i)->check_result_reliable()) {
      need_fallback = true;
      LOG_WARN("storage estimation is not reliable", KPC(tasks.at(i)));
    }
  }
  NG_TRACE(storage_estimation_end);
  if (OB_SUCC(ret) && !need_fallback) {
    double row_count = 0.0;
    double partition_count = 0.0;
    for (int64_t i = 0; i < tasks.count(); ++i) {
      const ObBatchEstTasks *task = tasks.at(i);
      for (int64_t j = 0; j < task->res_.index_param_res_.count(); ++j) {
        const obrpc::ObEstPartResElement &res = task->res_.index_param_res_.at(j);
        row_count += res.logical_row_count_;
        partition_count += 1.0;
      }
    }
    if (partition_count > 0) {
      row_count *= part_loc_infos.count() / partition_count;
      meta.table_row_count_ = row_count;
      meta.average_row_size_ = static_cast<double>(ObOptStatManager::get_default_avg_row_size());
      meta.part_size_ = row_count * meta.average_row_size_;
    }
  }
  return ret;
}

int ObAccessPathEstimation::get_key_ranges(ObOptimizerContext &ctx,
                                           ObIAllocator &allocator,
                                           const ObTabletID &tablet_id,
                                           AccessPath &ap,
                                           ObIArray<common::ObNewRange> &new_ranges)
{
  int ret = OB_SUCCESS;
  if (!share::is_oracle_mapping_real_virtual_table(ap.ref_table_id_)) {
    //do nothing
  } else if (OB_FAIL(convert_agent_vt_key_ranges(ctx, allocator, ap, new_ranges))) {
    LOG_WARN("failed to convert agent vt key ranges", K(ret), K(new_ranges));
  } else {/*do nothing*/}
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_physical_rowid_ranges(ctx, allocator, tablet_id,
                                              ap.index_id_, new_ranges))) {
      LOG_WARN("failed to convert physical rowid ranges", K(ret));
    } else {
      LOG_TRACE("Succeed to get key ranges", K(new_ranges));
    }
  }
  return ret;
}

int ObAccessPathEstimation::convert_agent_vt_key_ranges(ObOptimizerContext &ctx,
                                                        ObIAllocator &allocator,
                                                        AccessPath &ap,
                                                        ObIArray<common::ObNewRange> &new_ranges)
{
  int ret = OB_SUCCESS;
  if (!share::is_oracle_mapping_real_virtual_table(ap.ref_table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ap));
  } else {
    void *buf = NULL;
    uint64_t vt_table_id = ap.ref_table_id_;
    uint64_t real_index_id = ObSchemaUtils::get_real_table_mappings_tid(ap.index_id_);
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
                                                  ap.est_cost_info_.range_columns_,
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
                                                                const ObIArray<ObRawExpr*> &filter_exprs,
                                                                bool only_ds_basic_stat,
                                                                bool &is_success)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin process dynamic sampling estimation", K(paths));
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
  } else {
    bool only_ds_filter = (table_meta->use_opt_stat() && !table_meta->is_opt_stat_expired()) || table_meta->is_stat_locked();
    if (OB_FAIL(add_ds_result_items(paths, filter_exprs, specify_ds,
                                    ds_result_items,
                                    only_ds_basic_stat,
                                    only_ds_filter))) {
      LOG_WARN("failed to init ds result items", K(ret));
    } else if (!ds_result_items.empty()) {
      OPT_TRACE_TITLE("BEGIN DYNAMIC SAMPLE ESTIMATION");
      ObArenaAllocator allocator("ObOpTableDS", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx.get_session_info()->get_effective_tenant_id());
      ObDynamicSampling dynamic_sampling(ctx, allocator);
      int64_t start_time = ObTimeUtility::current_time();
      bool throw_ds_error = false;
      if (OB_FAIL(dynamic_sampling.estimate_table_rowcount(ds_table_param, ds_result_items, throw_ds_error))) {
        if (!throw_ds_error) {
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
                                                                    only_ds_filter,
                                                                    no_ds_data))) {
        LOG_WARN("failed to update table stat info by dynamic sampling", K(ret));
      } else if (only_ds_basic_stat || no_ds_data) {
        if (OB_FAIL(process_statistics_estimation(paths))) {
          LOG_WARN("failed to process statistics estimation", K(ret));
        }
      } else if (OB_FAIL(estimate_path_rowcount_by_dynamic_sampling(ds_table_param.table_id_, paths, ds_result_items))) {
        LOG_WARN("failed to estimate path rowcount by dynamic sampling", K(ret));
      }
      LOG_TRACE("finish dynamic sampling", K(only_ds_basic_stat), K(only_ds_filter), K(no_ds_data), K(is_success));
      OPT_TRACE("end to process table dynamic sampling estimation");
      OPT_TRACE("dynamic sampling estimation result:");
      OPT_TRACE(ds_result_items);
    }
  }
  return ret;
}


int ObAccessPathEstimation::add_ds_result_items(ObIArray<AccessPath *> &paths,
                                                const ObIArray<ObRawExpr*> &filter_exprs,
                                                const bool specify_ds,
                                                ObIArray<ObDSResultItem> &ds_result_items,
                                                bool only_ds_basic_stat,
                                                bool only_ds_filter)
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
    if (!only_ds_filter &&
        OB_FAIL(get_need_dynamic_sampling_columns(paths.at(0)->parent_->get_plan(),
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
          } else if (!paths.at(i)->est_cost_info_.prefix_filters_.empty()) {
            ObDSResultItem tmp_item(ObDSResultItemType::OB_DS_INDEX_SCAN_STAT, paths.at(i)->index_id_);
            if (OB_FAIL(tmp_item.exprs_.assign(paths.at(i)->est_cost_info_.prefix_filters_))) {
              LOG_WARN("failed to assign", K(ret));
            } else if (OB_FAIL(ds_result_items.push_back(tmp_item))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
          if (OB_SUCC(ret) && (!paths.at(i)->est_cost_info_.prefix_filters_.empty() ||
                               !paths.at(i)->est_cost_info_.postfix_filters_.empty())) {
            ObDSResultItem tmp_item(ObDSResultItemType::OB_DS_INDEX_BACK_STAT, paths.at(i)->index_id_);
            if (OB_FAIL(tmp_item.exprs_.assign(paths.at(i)->est_cost_info_.prefix_filters_))) {
              LOG_WARN("failed to assign", K(ret));
            } else if (OB_FAIL(append(tmp_item.exprs_, paths.at(i)->est_cost_info_.postfix_filters_))) {
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
                                              K(ds_result_items), K(only_ds_basic_stat), K(only_ds_filter));
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
                                                                       bool only_ds_filter,
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
    if (OB_ISNULL(table_meta) || OB_UNLIKELY(OB_INVALID_ID == table_meta->get_ref_table_id())) {
      //do nothing
    } else {
      table_meta->set_ds_level(ds_level);
      if (!only_ds_filter) {
        bool no_add_micro_block = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
        if (!no_add_micro_block) {
          est_cost_info.table_meta_info_->micro_block_count_ = item->stat_handle_.stat_->get_micro_block_num();
        }
        est_cost_info.table_meta_info_->table_row_count_ = row_count;
        if (OB_FAIL(update_column_metas_by_ds_col_stat(row_count,
                                                       item->stat_handle_.stat_->get_ds_col_stats(),
                                                       table_meta->get_column_metas()))) {
          LOG_WARN("failed to fill ds col stat", K(ret));
        } else {
          table_meta->set_rows(row_count);
          table_meta->set_base_rows(row_count);
          table_meta->set_use_ds_stat();
        }
      }
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
      table_meta->set_base_rows(path->get_output_row_count());
      for (int64_t i = 0; i < table_meta->get_column_metas().count(); ++i) {
        table_meta->get_column_metas().at(i).set_default_meta(path->get_output_row_count());
      }
    }
  }
  return ret;
}

int ObAccessPathEstimation::estimate_path_rowcount_by_dynamic_sampling(const uint64_t table_id,
                                                                       ObIArray<AccessPath *> &paths,
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
        const ObDSResultItem *index_range_result_item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_INDEX_SCAN_STAT,
                                                                                               paths.at(i)->index_id_,
                                                                                               ds_result_items);
        const ObDSResultItem *index_back_result_item = ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType::OB_DS_INDEX_BACK_STAT,
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
        double dummy_row_count = 1.0;
        double &index_back_row_count = est_cost_info.index_meta_info_.is_index_back_ ?
                                        est_cost_info.index_back_row_count_ :
                                        dummy_row_count;
        if (index_range_result_item == NULL) {
          logical_row_count = est_cost_info.table_meta_info_->table_row_count_;
          physical_row_count = logical_row_count;
          index_back_row_count = logical_row_count;
        } else {
          logical_row_count = index_range_result_item->stat_handle_.stat_->get_rowcount();
          index_back_row_count = index_back_result_item->stat_handle_.stat_->get_rowcount();
          double tmp_ratio = index_range_result_item->stat_handle_.stat_->get_sample_block_ratio();
          logical_row_count =  logical_row_count != 0 ? logical_row_count : static_cast<int64_t>(100.0 / tmp_ratio);
          index_back_row_count = index_back_row_count != 0 ? index_back_row_count : logical_row_count;
          physical_row_count = logical_row_count;
        }
        if (OB_SUCC(ret)) {
          // block sampling
          double block_sample_ratio = est_cost_info.sample_info_.is_block_sample() ?
                0.01 * est_cost_info.sample_info_.percent_ : 1.0;
          logical_row_count *= block_sample_ratio;
          physical_row_count *= block_sample_ratio;
          index_back_row_count *= block_sample_ratio;

          logical_row_count = std::max(logical_row_count, 1.0);
          physical_row_count = std::max(physical_row_count, 1.0);
          index_back_row_count = std::max(index_back_row_count, 1.0);
          est_cost_info.output_row_count_ = output_rowcnt;
          // row sampling
          double row_sample_ratio = est_cost_info.sample_info_.is_row_sample() ?
                0.01 * est_cost_info.sample_info_.percent_ : 1.0;
          est_cost_info.output_row_count_ *= row_sample_ratio;
          est_cost_info.postfix_filter_sel_ = index_back_row_count * 1.0 / physical_row_count;
          est_cost_info.table_filter_sel_ = output_rowcnt * 1.0 / index_back_row_count;

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
    col_metas.at(i).set_base_ndv(col_metas.at(i).get_ndv());
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
                                           ObIArray<AccessPath *> &geo_paths,
                                           ObIArray<IndexMergePath *> &index_merge_paths)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
    if (OB_ISNULL(paths.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null path", K(ret));
    } else if (paths.at(i)->is_index_merge_path()) {
      if (OB_FAIL(index_merge_paths.push_back(static_cast<IndexMergePath*>(paths.at(i))))) {
        LOG_WARN("failed to push back index merge path", K(ret));
      } else if (OB_FAIL(static_cast<IndexMergePath*>(paths.at(i))->get_all_scan_access_paths(normal_paths))) {
        LOG_WARN("failed to get index merge scan path", K(ret));
      }
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

int RangePartitionHelper::init(uint64_t table_id,
                               uint64_t ref_table_id,
                               const ObDMLStmt *stmt,
                               const ObTablePartitionInfo &table_partition_info,
                               const ObIArray<ColumnItem> &range_columns)
{
  int ret = OB_SUCCESS;
  all_partition_is_valid_ = false;
  const ObTableLocation &table_location = table_partition_info.get_table_location();
  part_level_ = table_location.get_part_level();
  ref_table_id_ = ref_table_id;
  table_partition_info_ = &table_partition_info;
  if (OB_UNLIKELY(share::schema::PARTITION_LEVEL_MAX == part_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected part level.", K(ret), K(table_location));
  } else if (share::schema::PARTITION_LEVEL_ZERO == part_level_) {
    all_partition_is_valid_ = true;
  } else if (OB_FAIL(get_range_projector(table_id,
                                         ref_table_id,
                                         stmt,
                                         range_columns,
                                         part_level_,
                                         part_projector_,
                                         sub_part_projector_,
                                         gen_projector_,
                                         sub_gen_projector_))){
    LOG_WARN("failed to get range projector", K(ret));
  } else if ((share::schema::PARTITION_LEVEL_ONE == part_level_ &&
              part_projector_.empty() && gen_projector_.empty()) ||
             (share::schema::PARTITION_LEVEL_TWO == part_level_ &&
              part_projector_.empty() && gen_projector_.empty() &&
              sub_part_projector_.empty() && sub_gen_projector_.empty())) {
    all_partition_is_valid_ = true;
  }
  if (OB_SUCC(ret) && !all_partition_is_valid_) {
    const ObCandiTabletLocIArray &part_loc_info_array =
        table_partition_info.get_phy_tbl_location_info().get_phy_part_loc_info_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); i ++) {
      const ObOptTabletLoc &tablet_loc = part_loc_info_array.at(i).get_partition_location();
      if (share::schema::PARTITION_LEVEL_TWO == part_level_ &&
          OB_FAIL(add_var_to_array_no_dup(used_level_one_part_ids_,
                                          static_cast<ObObjectID>(tablet_loc.get_first_level_part_id())))) {
        LOG_WARN("failed to add var", K(ret));
      } else if (OB_FAIL(used_tablet_ids_.push_back(tablet_loc.get_tablet_id()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

/**
 * Find the projector of the partition column/partition generated column from the range columns of index
 * e.g. partition by (c1,c2)    index (c1,c3,c2)
 * partition_projector = [0,2]
 */
int RangePartitionHelper::get_range_projector(uint64_t table_id,
                                              uint64_t ref_table_id,
                                              const ObDMLStmt *stmt,
                                              const ObIArray<ColumnItem> &range_columns,
                                              const share::schema::ObPartitionLevel part_level,
                                              ObIArray<int64_t> &part_projector,
                                              ObIArray<int64_t> &sub_part_projector,
                                              ObIArray<int64_t> &gen_projector,
                                              ObIArray<int64_t> &sub_gen_projector)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 4> part_columns;
  ObSEArray<ColumnItem, 4> gen_columns;
  ObSEArray<ColumnItem, 4> sub_part_columns;
  ObSEArray<ColumnItem, 4> sub_gen_columns;
  ObSEArray<ObRawExpr *, 8> range_exprs;
  bool all_find = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else {
    if (OB_FAIL(stmt->get_partition_columns(table_id, ref_table_id,
                                            share::schema::PARTITION_LEVEL_ONE,
                                            part_columns, gen_columns))) {
      LOG_WARN("failed to get partition columns", K(ret));
    } else if (share::schema::PARTITION_LEVEL_TWO == part_level &&
              OB_FAIL(stmt->get_partition_columns(table_id, ref_table_id,
                                                  share::schema::PARTITION_LEVEL_TWO,
                                                  sub_part_columns, sub_gen_columns))) {
      LOG_WARN("failed to get partition columns", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count(); ++i) {
    if (OB_FAIL(range_exprs.push_back(range_columns.at(i).expr_))) {
      LOG_WARN("failed to push back column exprs", K(ret));
    }
  }
  // get partition projector by partition columns
  if (FAILEDx(extract_column_projector(range_exprs, part_columns, part_projector))) {
    LOG_WARN("failed to extract projector", K(ret), K(range_exprs), K(part_columns));
  // get partition projector by generated columns
  } else if (OB_FAIL(extract_column_projector(range_exprs, gen_columns, gen_projector))) {
    LOG_WARN("failed to extract projector", K(ret), K(range_exprs), K(gen_columns));
  // get sub partition projector by sub partition columns
  } else if (OB_FAIL(extract_column_projector(range_exprs, sub_part_columns, sub_part_projector))) {
    LOG_WARN("failed to extract projector", K(ret), K(range_exprs), K(sub_part_columns));
  // get sub partition projector by sub generated columns
  } else if (OB_FAIL(extract_column_projector(range_exprs, sub_gen_columns, sub_gen_projector))) {
    LOG_WARN("failed to extract projector", K(ret), K(range_exprs), K(sub_gen_columns));
  }
  LOG_TRACE("get projector", K(part_projector), K(sub_part_projector),
                             K(gen_projector), K(sub_gen_projector));
  return ret;
}

int RangePartitionHelper::extract_column_projector(const ObIArray<ObRawExpr *> &range_exprs,
                                                   const ObIArray<ColumnItem> &need_columns,
                                                   ObIArray<int64_t> &projector)
{
  int ret = OB_SUCCESS;
  bool all_find = true;
  for (int64_t i = 0; OB_SUCC(ret) && all_find && i < need_columns.count(); ++i) {
    int64_t idx = -1;
    if (ObOptimizerUtil::find_equal_expr(range_exprs, need_columns.at(i).expr_, idx)) {
      if (OB_FAIL(projector.push_back(idx))) {
        LOG_WARN("failed to push back idx", K(ret));
      }
    } else {
      all_find = false;
      projector.reuse();
    }
  }
  return ret;
}

int RangePartitionHelper::get_scan_range_partitions(ObExecContext &exec_ctx,
                                                    const ObNewRange &scan_range,
                                                    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 8> part_ids;
  ObSEArray<ObObjectID, 8> sub_part_ids;
  if (OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(table_partition_info_));
  } else if (all_partition_is_valid_) {
    // do nothing
  } else if (share::schema::PARTITION_LEVEL_ONE == part_level_) {
    if (OB_FAIL(get_scan_range_partitions(exec_ctx,
                                          scan_range,
                                          part_projector_,
                                          gen_projector_,
                                          table_partition_info_->get_table_location(),
                                          tablet_ids,
                                          part_ids))) {
        LOG_WARN("failed to get scan range partitions", K(ret));
      }
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level_) {
    bool level_one_projector_empty = part_projector_.empty() && gen_projector_.empty();
    bool level_two_projector_empty = sub_part_projector_.empty() && sub_gen_projector_.empty();
    ObSEArray<ObTabletID, 1> dummy;
    if (!level_one_projector_empty) {
      if (OB_FAIL(get_scan_range_partitions(exec_ctx,
                                            scan_range,
                                            part_projector_,
                                            gen_projector_,
                                            table_partition_info_->get_table_location(),
                                            dummy,
                                            part_ids))) {
        LOG_WARN("failed to get scan range partitions", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::intersect(part_ids,
                                                    used_level_one_part_ids_,
                                                    part_ids))) {
        LOG_WARN("failed to intersect", K(ret));
      }
    } else if (OB_FAIL(part_ids.assign(used_level_one_part_ids_))) {
      LOG_WARN("failed to assign", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (!level_two_projector_empty &&
               OB_FAIL(get_scan_range_partitions(exec_ctx,
                                                 scan_range,
                                                 sub_part_projector_,
                                                 sub_gen_projector_,
                                                 table_partition_info_->get_table_location(),
                                                 tablet_ids,
                                                 sub_part_ids,
                                                 &part_ids))) {
      LOG_WARN("failed to get scan range partitions", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition level", K(part_level_));
  }
  if (OB_SUCC(ret) && !tablet_ids.empty()) {
    if (OB_FAIL(ObOptimizerUtil::intersect(used_tablet_ids_, tablet_ids, tablet_ids))) {
      LOG_WARN("failed to intersect", K(ret));
    }
  }
  return ret;
}

int RangePartitionHelper::get_scan_range_partitions(ObExecContext &exec_ctx,
                                                    const ObNewRange &scan_range,
                                                    const ObIArray<int64_t> &part_projector,
                                                    const ObIArray<int64_t> &gen_projector,
                                                    const ObTableLocation &table_location,
                                                    ObIArray<ObTabletID> &tablet_ids,
                                                    ObIArray<ObObjectID> &partition_ids,
                                                    const ObIArray<ObObjectID> *level_one_part_ids/* = NULL */)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObNewRange part_range;
  ObNewRange gen_range;
  bool is_valid = true;
  if (OB_UNLIKELY(part_projector.empty() && gen_projector.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part projector and gen projector both empty", K(ret));
  } else if (!part_projector.empty() &&
             OB_FAIL(construct_partition_range(alloc, scan_range, part_range, part_projector, is_valid))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (!gen_projector.empty() &&
             OB_FAIL(construct_partition_range(alloc, scan_range, gen_range, gen_projector, is_valid))) {
    LOG_WARN("failed to construct partition range", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else {
    ObNewRange *part_range_ptr = part_projector.empty() ? NULL : &part_range;
    ObNewRange *gen_range_ptr = gen_projector.empty() ? NULL : &gen_range;
    if (OB_FAIL(table_location.get_partition_ids_by_range(exec_ctx,
                                                          part_range_ptr,
                                                          gen_range_ptr,
                                                          tablet_ids,
                                                          partition_ids,
                                                          level_one_part_ids))) {
      // Some constructed range may be invalid and will get an error during get_partition_ids_by_range.
      // We can ignore the error and consider the range to be invalid.
      LOG_TRACE("failed to get partition ids by range", K(ret), KPC(part_range_ptr), KPC(gen_range_ptr));
      ret = OB_SUCCESS;
      partition_ids.reuse();
    }
  }
  return ret;
}


/**
 * Construct the partition_range to calculate the partition by scan range and projector
 */
int RangePartitionHelper::construct_partition_range(ObArenaAllocator &allocator,
                                                    const ObNewRange &scan_range,
                                                    ObNewRange &part_range,
                                                    const ObIArray<int64_t> &part_projector,
                                                    bool &is_valid_range)
{
  int64_t ret = OB_SUCCESS;
  ObObj *start_key = NULL;
  ObObj *end_key = NULL;
  int64_t key_count = part_projector.count();
  is_valid_range = true;
  bool cnt_ext = false;
  int64_t next_pos = part_projector.empty() ? -1 : part_projector.at(part_projector.count() - 1) + 1;
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
      if (start_key[i].is_ext() || end_key[i].is_ext()) {
        cnt_ext = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    part_range.start_key_.assign(start_key, key_count);
    part_range.end_key_.assign(end_key, key_count);
    part_range.border_flag_ = scan_range.border_flag_;
    if (part_range.start_key_.is_max_row() || part_range.end_key_.is_min_row()) {
      is_valid_range = false;
    } else if (part_range.start_key_.compare(part_range.end_key_) == 0) {
      // create table t1 (c1 int, c2 int, index idx1 (c1,c2) local) partition by (c1) ...
      // select * from t1 where c1 = 1 and c2 < 10
      // get scan_range (1,NULL,MAX,MAX,MAX ; 1,10,MIN,MIN,MIN) for idx1
      // when calculate partition ids, we only need value from c1, if use scan_range's border flag,
      // then we will get a empty range (1,1)
      part_range.border_flag_.set_inclusive_start();
      part_range.border_flag_.set_inclusive_end();
      if (cnt_ext) {
        // single value range with min/max can not calculate partition ids
        is_valid_range = false;
      }
    } else if (next_pos >= 0 &&
               next_pos < scan_range.start_key_.get_obj_cnt() &&
               next_pos < scan_range.end_key_.get_obj_cnt()) {
      // When the scan range is (1,MIN,MIN ; 4,MAX,MAX), if use scan_range's border flag,
      // we will get the wrong partition range (1,4).
      // The right partition range should be [1,4].
      const ObObj &next_start_key = scan_range.start_key_.get_obj_ptr()[next_pos];
      const ObObj &next_end_key = scan_range.end_key_.get_obj_ptr()[next_pos];
      if (!part_range.start_key_.is_min_row() && !next_start_key.is_max_value()) {
        part_range.border_flag_.set_inclusive_start();
      }
      if (!part_range.end_key_.is_max_row() && !next_end_key.is_min_value()) {
        part_range.border_flag_.set_inclusive_end();
      }
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase
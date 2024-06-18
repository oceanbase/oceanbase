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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_conditional_aggr_coalesce.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
using namespace common;
namespace sql
{
const int64_t ObTransformConditionalAggrCoalesce::MIN_COND_AGGR_CNT_FOR_COALESCE = 20;
const int64_t ObTransformConditionalAggrCoalesce::MAX_GBY_NDV_PRODUCT_FOR_COALESCE = 1000;
const double ObTransformConditionalAggrCoalesce::MIN_CUT_RATIO_FOR_COALESCE = 100;

int ObTransformConditionalAggrCoalesce::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool force_trans_wo_pullup = false;
  bool force_no_trans_wo_pullup = false;
  bool force_trans_with_pullup = false;
  bool force_no_trans_with_pullup = false;
  bool valid_wo_pullup = false;
  bool valid_with_pullup = false;
  TransFlagPair trans_happen_flags(false, false);
  TransformParam trans_param;
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL;
  ObDMLStmt *parent_stmt = !parent_stmts.empty() ? parent_stmts.at(parent_stmts.count()-1).stmt_ : stmt;
  bool trace_ignore_cost = (OB_E(EventTable::EN_COALESCE_AGGR_IGNORE_COST) OB_SUCCESS) != OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_2_3 ||
             (stmt->get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_0 &&
              stmt->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_2)) {
    // do nothing
  } else if (OB_FAIL(check_hint_valid(*stmt,
                                      force_trans_wo_pullup,
                                      force_no_trans_wo_pullup,
                                      force_trans_with_pullup,
                                      force_no_trans_with_pullup))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (OB_FAIL(check_basic_validity(stmt, trans_param, select_stmt,
                                          valid_wo_pullup, valid_with_pullup))) {
    LOG_WARN("failed to check stmt validity", K(ret));
  } else if (!valid_wo_pullup && !valid_with_pullup) {
    // do nothing
  } else if (!force_no_trans_wo_pullup &&
             valid_wo_pullup && OB_FAIL(try_transform_wo_pullup(select_stmt,
                                                                parent_stmt,
                                                                force_trans_wo_pullup || trace_ignore_cost,
                                                                trans_param,
                                                                trans_happen_flags.first))) {
    LOG_WARN("failed to try transform without pullup", K(ret));
  } else if (!force_no_trans_with_pullup &&
             valid_with_pullup && OB_FAIL(try_transform_with_pullup(
                                                        select_stmt,
                                                        parent_stmt,
                                                        force_trans_with_pullup || trace_ignore_cost,
                                                        trans_param,
                                                        trans_happen_flags.second))) {
    LOG_WARN("failed to try transform with pullup", K(ret));
  } else if (!trans_happen_flags.first && !trans_happen_flags.second) {
    // do nothing
  } else if (OB_FAIL(add_transform_hint(*stmt, &trans_happen_flags))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to coalesce conditional aggregate functions", K(trans_happened));
    OPT_TRACE("succeed to coalesce conditional aggregate functions", K(trans_happened));
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::check_basic_validity(ObDMLStmt *stmt,
                                                            TransformParam &trans_param,
                                                            ObSelectStmt *&select_stmt,
                                                            bool &valid_wo_pullup,
                                                            bool &valid_with_pullup)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool cnt_unpullupable_aggr = false;
  valid_wo_pullup = false;
  valid_with_pullup = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to get rownum info", K(ret));
  } else if (select_stmt->get_aggr_item_size() < 1 || select_stmt->is_set_stmt() ||
             select_stmt->is_hierarchical_query() || select_stmt->has_sequence() ||
             select_stmt->has_rollup() || select_stmt->has_grouping_sets() ||
             select_stmt->has_cube() || has_rownum) {
    // do nothing
  } else if (OB_FAIL(collect_cond_aggrs_info(select_stmt, trans_param, cnt_unpullupable_aggr))) {
    LOG_WARN("failed to check aggr items", K(ret));
  } else {
    valid_wo_pullup = trans_param.cond_aggrs_wo_extra_dep_.count() > 0;
    valid_with_pullup = !cnt_unpullupable_aggr &&
                        trans_param.cond_aggrs_with_extra_dep_.count() > 0;
  }
  return ret;
}

// collect conditional aggregate functions that are possible candidates for coalescing along with
// their dependent columns beyond the group by exprs, and determine whether the aggr items can be
// entirely pulluped.
int ObTransformConditionalAggrCoalesce::collect_cond_aggrs_info(ObSelectStmt *select_stmt,
                                                                TransformParam &trans_param,
                                                                bool &cnt_unpullupable_aggr)
{
  int ret = OB_SUCCESS;
  cnt_unpullupable_aggr = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); i++) {
    ObAggFunRawExpr *aggr_expr = NULL;
    bool is_target_aggr_type = false;
    bool is_param_distinct = false;
    bool is_cond_aggr = false;
    ObCaseOpRawExpr *case_expr = NULL;
    bool is_case_when_valid = false;
    ObSEArray<ObRawExpr*,4> extra_dep_cols;
    if (OB_ISNULL(aggr_expr = select_stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FALSE_IT(is_param_distinct = aggr_expr->is_param_distinct())) {
    } else if (OB_FALSE_IT(is_target_aggr_type = check_aggr_type(aggr_expr->get_expr_type()))) {
    } else if (OB_FAIL(check_cond_aggr_form(aggr_expr, case_expr, is_cond_aggr))) {
      LOG_WARN("failed to check cond aggr form", K(ret));
    } else if (!is_cond_aggr) {
      // do nothing
    } else if (OB_FAIL(check_case_when_validity(select_stmt, aggr_expr, case_expr,
                                                is_case_when_valid))) {
      LOG_WARN("failed to check case expr rewritable", K(ret));
    } else if (!is_case_when_valid) {
      // do nothing
    } else if (OB_FAIL(extract_extra_dep_cols(case_expr->get_when_param_exprs(),
                                              select_stmt->get_group_exprs(),
                                              extra_dep_cols))) {
      LOG_WARN("failed to check extra dependent columns", K(ret));
    } else if (extra_dep_cols.count() == 0) {
      if (OB_FAIL(trans_param.cond_aggrs_wo_extra_dep_.push_back(aggr_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (trans_param.extra_dep_cols_.count() == 0) {
      if (OB_FAIL(trans_param.cond_aggrs_with_extra_dep_.push_back(aggr_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(trans_param.extra_dep_cols_.assign(extra_dep_cols))) {
        LOG_WARN("failed to assign expr array", K(ret));
      }
    } else if (ObOptimizerUtil::subset_exprs(extra_dep_cols, trans_param.extra_dep_cols_)) {
      if (OB_FAIL(trans_param.cond_aggrs_with_extra_dep_.push_back(aggr_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      // multiple sets of dependent columns are currently prohibited for trans_with_pullup
      cnt_unpullupable_aggr = true;
    }
    cnt_unpullupable_aggr |= (!is_target_aggr_type || is_param_distinct);
  }
  return ret;
}

bool ObTransformConditionalAggrCoalesce::check_aggr_type(ObItemType aggr_type)
{
  bool is_target_aggr_type = false;
  if (T_FUN_MAX == aggr_type ||
      T_FUN_MIN == aggr_type ||
      T_FUN_SUM == aggr_type ||
      T_FUN_COUNT == aggr_type ||
      T_FUN_COUNT_SUM == aggr_type ||
      T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_type ||
      T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_type ||
      T_FUN_SYS_BIT_AND == aggr_type ||
      T_FUN_SYS_BIT_OR == aggr_type ||
      T_FUN_SYS_BIT_XOR == aggr_type) {
    is_target_aggr_type = true;
  } else {
    is_target_aggr_type = false;
  }
  return is_target_aggr_type;
}

int ObTransformConditionalAggrCoalesce::check_cond_aggr_form(ObAggFunRawExpr *aggr_expr,
                                                             ObCaseOpRawExpr *&case_expr,
                                                             bool &is_cond_aggr)
{
  int ret = OB_SUCCESS;
  is_cond_aggr = true;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (aggr_expr->get_param_count() != 1) {
    is_cond_aggr = false;
  } else if (OB_ISNULL(aggr_expr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null param expr", K(ret));
  } else if (aggr_expr->get_param_expr(0)->is_case_op_expr()) {
    case_expr = static_cast<ObCaseOpRawExpr*>(aggr_expr->get_param_expr(0));
  } else {
    is_cond_aggr = false;
  }
  return ret;
}

// After rewriting, both when exprs and then exprs will have their computation times changed,
// so we need to make sure that their computation results do not change with the computation times.
// At the same time, the calculation of then exprs will be advanced, and it is necessary to ensure
// that the risk of reporting errors after they are advanced is low.
int ObTransformConditionalAggrCoalesce::check_case_when_validity(ObSelectStmt *select_stmt,
                                                                   ObAggFunRawExpr *cond_aggr,
                                                                   ObCaseOpRawExpr *case_expr,
                                                                   bool &is_case_when_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*,4> depend_exprs;
  ObSEArray<ObRawExpr*,4> then_else_exprs;
  is_case_when_valid = true;
  bool satisfy_fd = true;
  bool is_error_free = true;
  bool can_calc_times_change = true;
  if (OB_ISNULL(case_expr) || OB_ISNULL(select_stmt) || OB_ISNULL(cond_aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(select_stmt->get_column_exprs(depend_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(depend_exprs, select_stmt->get_group_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(then_else_exprs, case_expr->get_then_param_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(then_else_exprs.push_back(case_expr->get_default_param_expr()))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    // check when exprs
    for (int64_t i = 0; OB_SUCC(ret) && is_case_when_valid &&
                                        i < case_expr->get_when_param_exprs().count(); i++) {
      if (OB_FAIL(ObOptimizerUtil::expr_calculable_by_exprs(case_expr->get_when_param_expr(i),
                                                                   depend_exprs,
                                                                   true,
                                                                   true,
                                                                   satisfy_fd))) {
        LOG_WARN("failed to judge expr is calculable with dependent exprs given", K(ret));
      } else if (!satisfy_fd) {
        is_case_when_valid = false;
      }
    }

    // check then/else exprs
    for (int64_t i = 0; OB_SUCC(ret) && is_case_when_valid && i < then_else_exprs.count(); i++) {
      if (OB_FAIL(ObTransformUtils::check_error_free_expr(then_else_exprs.at(i), is_error_free))) {
        LOG_WARN("failed to check error free exprs", K(ret));
      } else if (!is_error_free) {
        is_case_when_valid = false;
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr_recursively(then_else_exprs.at(i),
                                                                    depend_exprs,
                                                                    can_calc_times_change))) {
        LOG_WARN("failed to check const expr recursively", K(ret));
      } else if (!can_calc_times_change) {
        is_case_when_valid = false;
      }
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::extract_extra_dep_cols(ObIArray<ObRawExpr*> &target_exprs,
                                                               ObIArray<ObRawExpr*> &exclude_exprs,
                                                               ObIArray<ObRawExpr*> &extra_dep_cols)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < target_exprs.count(); i++) {
    if (OB_FAIL(inner_extract_extra_dep_cols(target_exprs.at(i), exclude_exprs, extra_dep_cols))) {
      LOG_WARN("failed to inner extract extra dep cols", K(ret));
    }
  }
  return ret;
}

// Extract the columns that target expr depends on in addition to static consts and exclude exprs.
// Note: assume target expr has been determined to be rewritable already.
// e.g. {exclude_exprs(c2+c3),target_expr(c1+c2+c3)} => extra_dep_cols(c1)
int ObTransformConditionalAggrCoalesce::inner_extract_extra_dep_cols(
                                        ObRawExpr *target_expr,
                                        ObIArray<ObRawExpr*> &exclude_exprs,
                                        ObIArray<ObRawExpr*> &extra_dep_cols)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (ObOptimizerUtil::find_item(exclude_exprs, target_expr)) {
    // do nothing
  } else if (target_expr->is_column_ref_expr()) {
    if (OB_FAIL(add_var_to_array_no_dup(extra_dep_cols, target_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < target_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(inner_extract_extra_dep_cols(target_expr->get_param_expr(i),
                                                          exclude_exprs,
                                                          extra_dep_cols)))) {
        LOG_WARN("failed to inner extract extra dep cols", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::try_transform_wo_pullup(ObSelectStmt *select_stmt,
                                                                ObDMLStmt *parent_stmt,
                                                                bool force_trans,
                                                                TransformParam &trans_param,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> coalesced_case_exprs;
  ObSEArray<ObAggFunRawExpr*, 4> new_aggr_items;
  ObSEArray<ObPCConstParamInfo, 4> param_constraints;
  bool is_aggr_count_decrease = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (trans_param.cond_aggrs_wo_extra_dep_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty cond aggrs", K(ret));
  } else if (OB_FAIL(coalesce_cond_aggrs(select_stmt->get_aggr_items(),
                                        trans_param.cond_aggrs_wo_extra_dep_,
                                        coalesced_case_exprs,
                                        new_aggr_items,
                                        param_constraints))) {
    LOG_WARN("failed to coalesce conditional aggrs", K(ret));
  } else if (OB_FAIL(check_aggrs_count_decrease(select_stmt->get_aggr_items(),
                                                new_aggr_items,
                                                is_aggr_count_decrease))) {
    LOG_WARN("failed to check aggrs count decrease", K(ret));
  } else if (!force_trans && !is_aggr_count_decrease) {
    LOG_TRACE("reject coalesce without pullup due to increased aggregate functions");
    OPT_TRACE("reject coalesce without pullup due to increased aggregate functions");
  } else if (OB_FAIL(do_transform_wo_pullup(select_stmt,
                                            trans_param.cond_aggrs_wo_extra_dep_,
                                            coalesced_case_exprs,
                                            new_aggr_items,
                                            param_constraints))) {
    LOG_WARN("failed to do transform without pullup", K(ret));
  } else if (OB_FAIL(refresh_project_name(parent_stmt, select_stmt))) {
    LOG_WARN("failed to refresh column name", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::try_transform_with_pullup(ObSelectStmt *select_stmt,
                                                                  ObDMLStmt *parent_stmt,
                                                                  bool force_trans,
                                                                  TransformParam &trans_param,
                                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> coalesced_case_exprs;
  ObSEArray<ObAggFunRawExpr*, 4> new_aggr_items;
  ObSEArray<ObPCConstParamInfo, 4> param_constraints;
  bool is_aggr_count_decrease = false;
  bool hit_threshold = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (trans_param.cond_aggrs_with_extra_dep_.count() < 1 ||
             trans_param.extra_dep_cols_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty array", K(ret));
  } else if (OB_FAIL(coalesce_cond_aggrs(select_stmt->get_aggr_items(),
                                        trans_param.cond_aggrs_with_extra_dep_,
                                        coalesced_case_exprs,
                                        new_aggr_items,
                                        param_constraints))) {
    LOG_WARN("failed to coalesce cond aggrs", K(ret));
  } else if (OB_FAIL(check_aggrs_count_decrease(select_stmt->get_aggr_items(),
                                                new_aggr_items,
                                                is_aggr_count_decrease))) {
    LOG_WARN("failed to check aggrs count decrease", K(ret));
  } else if (!force_trans && !is_aggr_count_decrease) {
    LOG_TRACE("reject coalesce with pullup due to increased aggregate functions");
    OPT_TRACE("reject coalesce with pullup due to increased aggregate functions");
  } else if (!force_trans && OB_FAIL(check_statistics_threshold(select_stmt,
                                                                trans_param,
                                                                hit_threshold))) {
    LOG_WARN("failed to check statistics threshold", K(ret));
  } else if (!force_trans && !hit_threshold) {
    LOG_TRACE("reject coalesce with pullup due to statistics threshold");
    OPT_TRACE("reject coalesce with pullup due to statistics threshold");
  } else if (OB_FAIL(do_transform_with_pullup(select_stmt,
                                              trans_param.cond_aggrs_with_extra_dep_,
                                              trans_param.extra_dep_cols_,
                                              coalesced_case_exprs,
                                              new_aggr_items,
                                              param_constraints))) {
    LOG_WARN("failed to do transform with pullup", K(ret));
  } else if (OB_FAIL(refresh_project_name(parent_stmt, select_stmt))) {
    LOG_WARN("failed to refresh column name", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::check_statistics_threshold(ObSelectStmt *select_stmt,
                                                                   TransformParam &trans_param,
                                                                   bool &hit_threshold)
{
  int ret = OB_SUCCESS;
  hit_threshold = false;
  TableItem* base_table = NULL;
  ObSEArray<ObRawExpr*, 4> cols_in_groupby;
  const ObTableSchema *table_schema = NULL;
  int64_t row_num = 0;
  int64_t ndv_product = 1;
  double cut_ratio = 0;
  int64_t cond_aggr_cnt = trans_param.cond_aggrs_with_extra_dep_.count();
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(ctx_->opt_stat_mgr_) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt->get_table_size() != 1 ||
             select_stmt->get_joined_tables().count() > 0 ||
             select_stmt->get_semi_info_size() > 0) {
    LOG_TRACE("access more than one base table, disable rewrite");
    OPT_TRACE("access more than one base table, disable rewrite");
  } else if (OB_ISNULL(base_table = select_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (!base_table->is_basic_table()) {
    // in order to evaluate the rewrite gains more accurately,
    // stmt is required to access only one base table
    LOG_TRACE("access more than one base table, disable rewrite");
    OPT_TRACE("access more than one base table, disable rewrite");
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(select_stmt->get_group_exprs(),
                                                          cols_in_groupby))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(append(cols_in_groupby, trans_param.extra_dep_cols_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(
                                      ctx_->session_info_->get_effective_tenant_id(),
                                      base_table->ref_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // collect ndv product for cols_in_groupby
    for (int64_t i = 0; OB_SUCC(ret) && i < cols_in_groupby.count(); i++) {
      uint64_t column_id = 0;
      uint64_t table_id = 0;
      ObColumnRefRawExpr* col = NULL;
      ObOptColumnStatHandle handle;
      if (OB_ISNULL(cols_in_groupby.at(i)) || !cols_in_groupby.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column ref expr", K(ret));
      } else if (OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(cols_in_groupby.at(i)))) {
      } else if (OB_FALSE_IT(column_id = col->get_column_id())) {
      } else if (OB_FALSE_IT(table_id = col->get_table_id())) {
      } else if (OB_FAIL(ctx_->opt_stat_mgr_->get_column_stat(
                        ctx_->session_info_->get_effective_tenant_id(),
                        base_table->ref_id_,
                        table_schema->is_partitioned_table() ? -1 : base_table->ref_id_,
                        column_id, handle))) {
        LOG_WARN("fail get full table column stat", K(ret), K(table_id), K(column_id));
      } else if (OB_ISNULL(handle.stat_)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ndv_product = ndv_product * handle.stat_->get_num_distinct();
      }
    }

    // get estimated rownum of basic table
    if (OB_SUCC(ret)) {
      ObSEArray<ObTabletID, 4> tablet_ids;
      ObSEArray<ObObjectID, 4> partition_ids;
      ObSEArray<ObObjectID, 4> first_level_part_ids;
      ObSEArray<int64_t, 4> int64_partition_ids;
      ObSEArray<ObOptTableStat, 4> table_stats;
      if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(tablet_ids,
                                                              partition_ids,
                                                              &first_level_part_ids))) {
        LOG_WARN("failed to get all partition ids");
      } else if (OB_FAIL(append(int64_partition_ids, partition_ids))) {
        LOG_WARN("failed to append partition ids", K(ret));
      } else if (OB_FAIL(ctx_->opt_stat_mgr_->get_table_stat(
                                              ctx_->session_info_->get_effective_tenant_id(),
                                              table_schema->get_table_id(),
                                              int64_partition_ids,
                                              table_stats))) {
        LOG_WARN("failed to get table stats", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); i++) {
        row_num += table_stats.at(i).get_row_count();
      }
    }

    // check statistics thresholds
    if (OB_SUCC(ret)) {
      cut_ratio = (ndv_product == 0) ? 0 : row_num * 1.0 / ndv_product;
      if (cond_aggr_cnt < MIN_COND_AGGR_CNT_FOR_COALESCE) {
        OPT_TRACE("reject rewrite due to conditional aggr count", K(cond_aggr_cnt));
      } else if (ndv_product > MAX_GBY_NDV_PRODUCT_FOR_COALESCE) {
        OPT_TRACE("reject rewrite due to ndv product", K(ndv_product));
      } else if (cut_ratio < MIN_CUT_RATIO_FOR_COALESCE) {
        OPT_TRACE("reject rewrite due to cut ratio", K(cut_ratio), K(ndv_product), K(row_num));
      } else {
        hit_threshold = true;
      }
    }
  }
  return ret;
}

// 根据是否有 distinct 对两类聚合函数分别计数，不能糅在一起，因为带 distinct 聚合函数计算代价远超普通聚合函数
int ObTransformConditionalAggrCoalesce::check_aggrs_count_decrease(ObIArray<ObAggFunRawExpr*> &old_aggrs,
                                                                   ObIArray<ObAggFunRawExpr*> &new_aggrs,
                                                                   bool &is_cnt_decrease)
{
  int ret = OB_SUCCESS;
  int64_t old_distinct_cnt = 0;
  int64_t old_normal_cnt = 0;
  int64_t new_distinct_cnt = 0;
  int64_t new_normal_cnt = 0;
  if (old_aggrs.count() < new_aggrs.count()) {
    is_cnt_decrease = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_aggrs.count(); i++) {
      ObAggFunRawExpr* aggr = NULL;
      if OB_ISNULL(aggr = old_aggrs.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (aggr->is_param_distinct()) {
        old_distinct_cnt += 1;
      } else {
        old_normal_cnt += 1;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_aggrs.count(); i++) {
      ObAggFunRawExpr* aggr = NULL;
      if OB_ISNULL(aggr = new_aggrs.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (aggr->is_param_distinct()) {
        new_distinct_cnt += 1;
      } else {
        new_normal_cnt += 1;
      }
    }
    is_cnt_decrease = (old_distinct_cnt >= new_distinct_cnt) && (old_normal_cnt >= new_normal_cnt);
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::do_transform_wo_pullup(
                                        ObSelectStmt *select_stmt,
                                        ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                                        ObIArray<ObRawExpr*> &coalesced_case_exprs,
                                        ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                                        ObIArray<ObPCConstParamInfo> &constraints)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> cond_aggrs_for_replace;
  ObStmtExprReplacer replacer;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(append(ctx_->plan_const_param_constraints_, constraints))) {
    LOG_WARN("failed to append constraints", K(ret));
  } else if (OB_FAIL(append(cond_aggrs_for_replace, cond_aggrs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    // replace cond aggrs with new case exprs
    replacer.remove_all();
    replacer.add_scope(SCOPE_HAVING);
    replacer.add_scope(SCOPE_SELECT);
    replacer.add_scope(SCOPE_ORDERBY);
    replacer.set_recursive(false);
    if (OB_FAIL(replacer.add_replace_exprs(cond_aggrs_for_replace, coalesced_case_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(select_stmt->iterate_stmt_expr(replacer))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    } else if (OB_FAIL(select_stmt->get_aggr_items().assign(new_aggr_items))) {
      LOG_WARN("failed to assign aggr items", K(ret));
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::do_transform_with_pullup(
                                        ObSelectStmt *select_stmt,
                                        ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                                        ObIArray<ObRawExpr*> &extra_dep_cols,
                                        ObIArray<ObRawExpr*> &coalesced_case_exprs,
                                        ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                                        ObIArray<ObPCConstParamInfo> &constraints)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> from_tables;
  ObSEArray<SemiInfo *, 4> semi_infos;
  ObSEArray<ObRawExpr*, 4> condition_exprs;
  ObSEArray<ObRawExpr *, 4> pushdown_select;
  TableItem *view_table = NULL;
  ObSelectStmt *view_stmt = NULL;

  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));

  // 1. coalesce cond aggrs
  } else if (OB_FAIL(do_transform_wo_pullup(select_stmt,
                                            cond_aggrs,
                                            coalesced_case_exprs,
                                            new_aggr_items,
                                            constraints))) {
    LOG_WARN("failed to do transform without pullup", K(ret));

  // 2. create inline view
  } else if (OB_FAIL(select_stmt->get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(semi_infos.assign(select_stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(condition_exprs.assign(select_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(collect_pushdown_select(select_stmt,
                                             extra_dep_cols,
                                             coalesced_case_exprs,
                                             pushdown_select))) {
    LOG_WARN("failed to collect pushdown select", K(ret));
  } else if (OB_FALSE_IT(select_stmt->get_condition_exprs().reuse())) {
  } else if (OB_FALSE_IT(select_stmt->get_aggr_items().reuse())) {
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                              select_stmt,
                                                              view_table,
                                                              from_tables,
                                                              &semi_infos))) {
    LOG_WARN("failed to create empty view", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          select_stmt,
                                                          view_table,
                                                          from_tables,
                                                          &condition_exprs,
                                                          &semi_infos,
                                                          &pushdown_select,
                                                          &select_stmt->get_group_exprs()))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_ISNULL(view_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null view query", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_group_exprs(), extra_dep_cols))) {
    LOG_WARN("failed to append extra dependent cols", K(ret));

  // 3. construct aggr exprs to merge the aggregation results of view stmt
  } else if (OB_FAIL(create_and_replace_aggrs_for_merge(select_stmt, view_stmt))) {
    LOG_WARN("failed to create aggrs for merge", K(ret));
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::coalesce_cond_aggrs(ObIArray<ObAggFunRawExpr*> &base_aggrs,
                                                       ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                                                       ObIArray<ObRawExpr*> &case_exprs,
                                                       ObIArray<ObAggFunRawExpr*> &new_aggrs,
                                                       ObIArray<ObPCConstParamInfo> &constraints)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < base_aggrs.count(); i++) {
    ObAggFunRawExpr* aggr_expr = NULL;
    if (OB_ISNULL(aggr_expr = base_aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ObOptimizerUtil::find_item(cond_aggrs, aggr_expr)) {
      // do nothing
    } else if (OB_FAIL(new_aggrs.push_back(aggr_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_aggrs.count(); i++) {
    ObAggFunRawExpr *cond_aggr = NULL;
    ObCaseOpRawExpr *case_expr = NULL;
    ObSEArray<ObRawExpr*, 4> new_then_exprs;
    ObRawExpr *new_default_expr;
    ObCaseOpRawExpr *new_case_expr = NULL;
    ObRawExpr *cast_case_expr = NULL;
    bool is_cond_aggr = false;
    if (OB_ISNULL(cond_aggr = cond_aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(check_cond_aggr_form(cond_aggr, case_expr, is_cond_aggr))) {
      LOG_WARN("failed to check cond aggr form", K(ret));
    } else if (OB_UNLIKELY(!is_cond_aggr) || OB_ISNULL(case_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected aggr form", K(ret));
    } else {
      // build new then exprs
      ObAggFunRawExpr* new_aggr_expr = NULL;
      bool is_sharable = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_then_expr_size(); i++) {
        ObRawExpr* then_expr = NULL;
        if (OB_ISNULL(then_expr = case_expr->get_then_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(build_aggr_expr(cond_aggr->get_expr_type(),
                                           cond_aggr->is_param_distinct(),
                                           then_expr,
                                           new_aggr_expr))) {
          LOG_WARN("failed to build aggr expr", K(ret));
        } else if (OB_FAIL(try_share_aggr(new_aggrs, new_aggr_expr, is_sharable, constraints))) {
          LOG_WARN("failed to try to share aggr expr", K(ret));
        } else if (!is_sharable && OB_FAIL(new_aggrs.push_back(new_aggr_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(new_then_exprs.push_back(new_aggr_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }

      // build new default expr
      ObRawExpr* default_expr = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(default_expr = case_expr->get_default_param_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(build_aggr_expr(cond_aggr->get_expr_type(),
                                         cond_aggr->is_param_distinct(),
                                         default_expr,
                                         new_aggr_expr))) {
        LOG_WARN("failed to build aggr expr", K(ret));
      } else if (OB_FAIL(try_share_aggr(new_aggrs, new_aggr_expr, is_sharable, constraints))) {
        LOG_WARN("failed to try to share aggr expr", K(ret));
      } else if (!is_sharable && OB_FAIL(new_aggrs.push_back(new_aggr_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        new_default_expr = new_aggr_expr;
      }

      // build new case expr
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(ctx_,
                                           case_expr->get_when_param_exprs(),
                                           new_then_exprs,
                                           new_default_expr,
                                           new_case_expr))) {
        LOG_WARN("failed to build case when exprs", K(ret));
      } else if (OB_FALSE_IT(cast_case_expr = new_case_expr)) {
      } else if (ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                cond_aggr,
                                                                cast_case_expr,
                                                                ctx_->session_info_)) {
        LOG_WARN("failed to add cast", K(ret));
      } else if (OB_FAIL(case_exprs.push_back(cast_case_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::build_aggr_expr(ObItemType expr_type,
                                                        bool is_param_distinct,
                                                        ObRawExpr *param_expr,
                                                        ObAggFunRawExpr *&aggr_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(expr_factory));
  } else if (OB_FAIL(expr_factory->create_raw_expr(expr_type,
                                                   aggr_expr))) {
    LOG_WARN("create aggr expr failed", K(ret));
  } else if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(aggr_expr->add_real_param_expr(param_expr))) {
    LOG_WARN("fail to set param expr", K(ret));
  } else if (OB_FALSE_IT(aggr_expr->set_param_distinct(is_param_distinct))) {
  } else if (OB_FAIL(aggr_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize aggregate function", K(ret));
  } else if (OB_FAIL(aggr_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  }
  return ret;
}

// check if target expr already exists in base aggrs, and if so, share the same expression.
int ObTransformConditionalAggrCoalesce::try_share_aggr(ObIArray<ObAggFunRawExpr*> &base_aggrs,
                                                       ObAggFunRawExpr *&target_aggr,
                                                       bool &is_sharable,
                                                       ObIArray<ObPCConstParamInfo> &constraints)
{
  int ret = OB_SUCCESS;
  is_sharable = false;
  ObExprEqualCheckContext equal_ctx;
  equal_ctx.override_const_compare_ = true;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(target_aggr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_sharable && i < base_aggrs.count(); i++) {
    ObAggFunRawExpr *cur_aggr = NULL;
    if (OB_ISNULL(cur_aggr = base_aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (cur_aggr->same_as(*target_aggr, &equal_ctx)) {
      target_aggr = cur_aggr;
      is_sharable = true;
      cur_aggr->set_explicited_reference();
      // constraints need to be added if the same_as judgement relies on specific const value
      for(int64_t i = 0; OB_SUCC(ret) && i < equal_ctx.param_expr_.count(); i++) {
        ObPCConstParamInfo param_info;
        int64_t param_idx = equal_ctx.param_expr_.at(i).param_idx_;
        if (OB_UNLIKELY(param_idx < 0 || param_idx >= plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(param_idx),
                                            K(plan_ctx->get_param_store().count()));
        } else if (OB_FAIL(param_info.const_idx_.push_back(param_idx))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(param_info.const_params_.push_back(
                                                      plan_ctx->get_param_store().at(param_idx)))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(constraints.push_back(param_info))) {
          LOG_WARN("failed to push back param info", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

// collect view's select exprs according to pseudo columns、aggr items、groupby exprs
int ObTransformConditionalAggrCoalesce::collect_pushdown_select(
                                        ObSelectStmt *select_stmt,
                                        ObIArray<ObRawExpr*> &extra_cols,
                                        ObIArray<ObRawExpr*> &coalesced_case_exprs,
                                        ObIArray<ObRawExpr*> &pushdown_select)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::pushdown_pseudo_column_like_exprs(*select_stmt, true,
                                                                         pushdown_select))) {
    LOG_WARN("faile to pushdown pseudo column like exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(pushdown_select, coalesced_case_exprs))) {
    LOG_WARN("faile to pushdown coalesced case exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); i++) {
      if (OB_FAIL(add_var_to_array_no_dup(pushdown_select,
                              static_cast<ObRawExpr *>(select_stmt->get_aggr_items().at(i))))) {
        LOG_WARN("failed to add var", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::create_and_replace_aggrs_for_merge(ObSelectStmt *select_stmt,
                                                                           ObSelectStmt *view_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *select_expr = NULL;
  TableItem *table = NULL;
  ObSEArray<ObRawExpr*, 4> cols_for_replace;
  ObSEArray<ObRawExpr*, 4> aggrs_for_merge;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(1 != select_stmt->get_table_items().count())
             || OB_ISNULL(table = select_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select stmt", K(ret), K(select_stmt->get_from_item_size()), K(table));
  } else if (select_stmt->get_aggr_item_size() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect empty aggr items", K(ret), K(select_stmt->get_aggr_item_size()));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < view_stmt->get_select_item_size(); i++) {
      ObRawExpr *col_expr = NULL;
      ObAggFunRawExpr* aggr_for_merge = NULL;
      ObItemType aggr_type = T_INVALID;
      if (OB_ISNULL(select_expr = view_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!select_expr->has_flag(CNT_AGG)) {
        // do nothing
      } else if (OB_FAIL(get_aggr_type(select_expr, aggr_type))) {
        LOG_WARN("failed to get aggr type for merge", K(ret));
      } else if (OB_ISNULL(col_expr = select_stmt->get_column_expr_by_id(table->table_id_,
                                                                  i + OB_APP_MIN_COLUMN_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(create_aggr_for_merge(aggr_type, col_expr, aggr_for_merge))) {
        LOG_WARN("failed to create aggr for merge", K(ret));
      } else if (OB_FAIL(select_stmt->get_aggr_items().push_back(aggr_for_merge))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(cols_for_replace.push_back(col_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(aggrs_for_merge.push_back(aggr_for_merge))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObStmtExprReplacer replacer;
      replacer.remove_all();
      replacer.add_scope(SCOPE_HAVING);
      replacer.add_scope(SCOPE_SELECT);
      replacer.add_scope(SCOPE_ORDERBY);
      replacer.set_recursive(false);
      if (OB_FAIL(replacer.add_replace_exprs(cols_for_replace, aggrs_for_merge))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(select_stmt->iterate_stmt_expr(replacer))) {
        LOG_WARN("failed to iterate stmt expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::create_aggr_for_merge(ObItemType aggr_type,
                                                              ObRawExpr *param_expr,
                                                              ObAggFunRawExpr *&aggr_expr)
{
  int ret = OB_SUCCESS;
  ObItemType aggr_type_for_merge;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (T_FUN_MAX == aggr_type ||
             T_FUN_MIN == aggr_type ||
             T_FUN_SUM == aggr_type ||
             T_FUN_COUNT_SUM == aggr_type ||
             T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == aggr_type ||
             T_FUN_SYS_BIT_AND == aggr_type ||
             T_FUN_SYS_BIT_OR == aggr_type ||
             T_FUN_SYS_BIT_XOR == aggr_type) {
    aggr_type_for_merge = aggr_type;
  } else if (T_FUN_COUNT == aggr_type) {
    aggr_type_for_merge = T_FUN_COUNT_SUM;
  } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_type) {
    aggr_type_for_merge = T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggr type", K(ret), K(aggr_type));
  }
  if (OB_SUCC(ret) && OB_FAIL(build_aggr_expr(aggr_type_for_merge, false, param_expr, aggr_expr))) {
    LOG_WARN("failed to build aggr expr", K(ret));
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::refresh_project_name(ObDMLStmt *parent_stmt,
                                                            ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(select_stmt) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (parent_stmt == static_cast<ObDMLStmt*>(select_stmt)) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::refresh_select_items_name(*ctx_->allocator_, select_stmt))) {
    LOG_WARN("failed to refresh select items name", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < parent_stmt->get_table_size(); ++i) {
      TableItem *table_item = parent_stmt->get_table_item(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is null", K(i));
      } else if (!table_item->is_generated_table() || !table_item->is_temp_table()) {
        // do nothing
      } else if (table_item->ref_query_ != select_stmt) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::refresh_column_items_name(parent_stmt,
                                                                     table_item->table_id_))) {
        LOG_WARN("failed to refresh column items name", K(ret));
      } else {
        found = true;
      }
    }
  }
  return ret;
}

// 这个函数假设输入表达式中的聚合函数类型有且仅有一种 (纯聚合 / case when 聚合)
int ObTransformConditionalAggrCoalesce::get_aggr_type(ObRawExpr* expr, ObItemType &aggr_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!IS_AGGR_FUN(expr->get_expr_type())) {
    // do nothing
  } else if (aggr_type != T_INVALID && aggr_type != expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggr type", K(ret));
  } else {
    aggr_type = expr->get_expr_type();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
    if (OB_FAIL(SMART_CALL(get_aggr_type(expr->get_param_expr(i), aggr_type)))) {
      LOG_WARN("failed to get aggr type for merge", K(ret));
    }
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::check_hint_valid(ObDMLStmt &stmt,
                                                         bool &force_trans_wo_pullup,
                                                         bool &force_no_trans_wo_pullup,
                                                         bool &force_trans_with_pullup,
                                                         bool &force_no_trans_with_pullup)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL;
  const ObCoalesceAggrHint *myhint = static_cast<const ObCoalesceAggrHint*>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_hint));
  } else {
    force_trans_wo_pullup = NULL != myhint && myhint->enable_trans_wo_pullup();
    force_no_trans_wo_pullup = !force_trans_wo_pullup && query_hint->has_outline_data();
    force_trans_with_pullup = NULL != myhint && myhint->enable_trans_with_pullup();
    force_no_trans_with_pullup = !force_trans_with_pullup && query_hint->has_outline_data();
  }
  return ret;
}

int ObTransformConditionalAggrCoalesce::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObCoalesceAggrHint *hint = NULL;
  TransFlagPair *trans_flags = NULL;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_) ||
      OB_ISNULL(trans_flags = static_cast<TransFlagPair*>(trans_params))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_COALESCE_AGGR, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else {
    hint->set_enable_trans_wo_pullup(trans_flags->first);
    hint->set_enable_trans_with_pullup(trans_flags->second);
    const ObCoalesceAggrHint *myhint = static_cast<const ObCoalesceAggrHint*>(get_hint(stmt.get_stmt_hint()));
    bool use_hint = NULL != myhint && ((myhint->enable_trans_wo_pullup() && trans_flags->first) ||
                                       (myhint->enable_trans_with_pullup() && trans_flags->second));
    if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
      LOG_WARN("failed to push back hint", K(ret));
    } else if (use_hint && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else {
      hint->set_qb_name(ctx_->src_qb_name_);
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
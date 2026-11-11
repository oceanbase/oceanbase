/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_mv_printer_checker.h"

#include "lib/utility/ob_template_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/mv/ob_mv_checker_utils.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObMVPrinterChecker::check(const ObSelectStmt &stmt, const ObMVCheckerParams &params, ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  ObMVRefreshableType refresh_type = OB_MV_REFRESH_INVALID;
  ObMVPrinterChecker checker(stmt, params, result);
  if (OB_FAIL(checker.check_mv_stmt_refresh_type(stmt, refresh_type))) {
    LOG_WARN("failed to check printer refresh type", K(ret));
  } else if (OB_MV_REFRESH_INVALID == refresh_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid refresh type", K(ret));
  } else if (OB_FAIL(checker.set_final_refresh_type(refresh_type))) {
    LOG_WARN("failed to set printer refresh type", K(ret), K(refresh_type));
  }
  return ret;
}

int ObMVPrinterChecker::check_mv_stmt_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_REFRESH_INVALID;
  bool is_valid = false;
  if (OB_FAIL(check_mv_stmt_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check mv refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (stmt.is_set_stmt()) {
    if (OB_FAIL(check_union_all_refresh_type(refresh_type))) {
      LOG_WARN("failed to check mav refresh type", K(ret));
    }
  } else if (stmt.has_group_by()) {
    if (OB_FAIL(check_mav_refresh_type(stmt, refresh_type))) {
      LOG_WARN("failed to check mav refresh type", K(ret));
    }
  } else if (OB_FAIL(check_mjv_refresh_type(stmt, refresh_type))) {
    LOG_WARN("failed to check mjv refresh type", K(ret));
  }
  LOG_TRACE("finish check mv refresh type", K(refresh_type));
  return ret;
}

int ObMVPrinterCheckerBase::check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rowid = false;
  bool is_deterministic_query = false;
  bool has_cur_time = false;
  bool table_type_valid = false;
  bool has_non_proctime_table = false;
  bool enable_fast_refresh_with_cur_time = false;
  is_valid = true;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt.get_query_ctx()));
  } else if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
               ObOptParamHint::ENABLE_FAST_REFRESH_WITH_CUR_TIME,
               enable_fast_refresh_with_cur_time))) {
    LOG_WARN("failed to get enable fast refresh with cur time", K(ret));
  } else if (stmt.is_set_stmt() && is_child_stmt(&stmt)) {
    // child stmt is set stmt, has multi-level set stmt
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with set operators UNION/INTERSECT/EXCEPT/MINUS is not supported");
  } else if (stmt.has_subquery()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with subquery is not supported");
  } else if (stmt.is_distinct()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with DISTINCT operator is not supported");
  } else if (stmt.has_order_by() || stmt.has_limit()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with order by or limit operators is not supported");
  } else if ((stmt.has_rollup() || stmt.has_cube() || stmt.has_grouping_sets() || stmt.get_having_expr_size() > 0)) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with rollup/grouping sets/cube/having operators is not supported");
  } else if (stmt.is_hierarchical_query()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("hierarchical query is not supported");
  } else if (stmt.has_window_function()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with window functions is not supported");
  } else if (stmt.is_contains_assignment()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with assignment operators is not supported");
  } else if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (OB_FAIL(ObMVCheckerUtils::check_has_rowid_exprs(&stmt, has_rowid))) {
    LOG_WARN("failed to check stmt has rowid exprs", K(ret));
  } else if (has_rownum || has_rowid || stmt.has_ora_rowscn()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with rownum/rowid/ora_rowscn pseudo columns is not supported");
  } else if (OB_FAIL(stmt.is_query_deterministic(is_deterministic_query))) {
    LOG_WARN("failed to check mv stmt use special expr", K(ret));
  } else if (!is_deterministic_query) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("non-deterministic query is not supported");
  } else if (OB_FAIL(stmt.has_special_expr(CNT_CUR_TIME, has_cur_time))) {
    LOG_WARN("failed to check stmt has special expr", K(ret));
  } else if (has_cur_time && !enable_fast_refresh_with_cur_time) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with current time expression is not supported");
  } else if (OB_FAIL(check_mv_table_type_valid(stmt, table_type_valid))) {
    LOG_WARN("failed to check mv table mlog", K(ret));
  } else if (!table_type_valid) {
    is_valid = false;
  } else if (is_child_stmt(&stmt)) {
    // do nothing, check below is ONLY for ROOT STMT
  } else if (OB_FAIL(check_mv_has_non_proctime_table(stmt, has_non_proctime_table))) {
    LOG_WARN("failed to check mv has non proctime table", K(ret));
  } else if (!has_non_proctime_table) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with all tables are AS OF PROCTIME() is not supported");
  }
  return ret;
}

int ObMVPrinterCheckerBase::check_mv_table_type_valid(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableItem *> &tables = stmt.get_table_items();
  const TableItem *table = NULL;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < tables.count(); ++i) {
    if (OB_ISNULL(table = tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", K(ret), KPC(table));
    } else if (OB_UNLIKELY(!(table->is_basic_table()
                             || (table->is_generated_table() && table->is_mv_proctime_table_)))) {
      is_valid = false;
      result_.get_note().error_.assign_fmt(
        "query with %s is not supported, only basic table "
        "and materialized view are supported",
        ObMVCheckerUtils::get_table_type_str(table->type_));
    } else if (OB_UNLIKELY(!(ObTableType::USER_TABLE == table->table_type_
                             || ObTableType::MATERIALIZED_VIEW == table->table_type_
                             || table->is_generated_table()))) {
      is_valid = false;
      result_.get_note().error_.assign_fmt(
        "query with %s is not supported, only basic table "
        "and materialized view are supported",
        ob_table_type_str(table->table_type_));
    } else if (OB_NOT_NULL(table->flashback_query_expr_)) {
      is_valid = false;
      result_.get_note().error_.assign_fmt("flashback query is not supported");
    } else if (OB_UNLIKELY(!table->part_ids_.empty())) {
      is_valid = false;
      result_.get_note().error_.assign_fmt("query with partition specification for table is not supported");
    } else if (params_.need_on_query_computation_ && table->table_type_ == ObTableType::MATERIALIZED_VIEW) {
      is_valid = false;
      result_.get_note().error_.assign_fmt("query with enable on query computation for nested mview is not supported");
    }
  }
  return ret;
}

int ObMVPrinterCheckerBase::check_mv_has_non_proctime_table(const ObSelectStmt &stmt, bool &has_non_proctime_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  has_non_proctime_table = false;
  if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_non_proctime_table && i < stmt.get_table_size(); ++i) {
    const TableItem *table = stmt.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table", K(ret), K(i));
    } else if (!table->is_mv_proctime_table_) {
      has_non_proctime_table = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_non_proctime_table && i < child_stmts.count(); ++i) {
    const ObSelectStmt *child_stmt = child_stmts.at(i);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child stmt", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(check_mv_has_non_proctime_table(*child_stmt, has_non_proctime_table)))) {
      LOG_WARN("failed to check mv has non proctime table", K(ret));
    }
  }
  return ret;
}

int ObMVPrinterChecker::check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool has_outer_join = false;
  bool enable_simplify_aggr_dep = false;
  refresh_type = OB_MV_REFRESH_INVALID;
  if (OB_FAIL(check_mav_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_FAIL(check_mav_aggr_items(stmt, is_valid))) {
    LOG_WARN("failed to check mav aggr valid", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query context is unexpected null", K(ret));
  } else if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
               ObOptParamHint::ENABLE_FAST_REFRESH_SIMPLIFY_AGGR_DEP,
               enable_simplify_aggr_dep))) {
    LOG_WARN("failed to get enable simplify aggregate dependency", K(ret));
  } else if (OB_FAIL(ObMVCheckerUtils::collect_aggr_dep_cols(stmt,
                                                             params_.expr_factory_,
                                                             params_.session_info_,
                                                             stmt_idx_,
                                                             enable_simplify_aggr_dep,
                                                             result_))) {
    LOG_WARN("failed to collect aggregate dependent columns", K(ret));
  } else if (!stmt.is_single_table_stmt() && OB_FAIL(check_join_shape_valid(stmt, is_valid, has_outer_join))) {
    LOG_WARN("failed to check join shape", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, is_valid))) {
    LOG_WARN("failed to check mv table mlog", KR(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (stmt.is_single_table_stmt()) {  // single table MAV
    refresh_type = OB_MV_FAST_REFRESH_SIMPLE_MAV;
  } else {  // join MAV
    refresh_type = has_outer_join ? OB_MV_FAST_REFRESH_OUTER_JOIN_MAV : OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV;
  }
  return ret;
}

int ObMVPrinterChecker::check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (lib::is_mysql_mode() && OB_FAIL(check_is_valid_mysql_mode_group_by(stmt, is_valid))) {
    LOG_WARN("failed to check is valid mysql mode group by", K(ret));
  } else if (!is_valid) {
    result_.get_note().error_.assign_fmt("the select item list contains columns that are not in the group by clause");
  } else if (is_valid) {
    // check group by exprs exists in select list
    const ObIArray<ObRawExpr *> &group_exprs = stmt.get_group_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      ObRawExpr *group_expr = group_exprs.at(i);
      if (OB_ISNULL(group_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(group_exprs));
      } else if (stmt.check_is_select_item_expr(group_expr)) {
        // do nothing
      } else if (OB_FAIL(result_.add_dependent_column(group_expr, stmt_idx_))) {
        LOG_WARN("failed to add dependent column", K(ret));
      }
    }
  }
  return ret;
}

// for mysql mode, check is valid group by
int ObMVPrinterChecker::check_is_valid_mysql_mode_group_by(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  hash::ObHashSet<uint64_t> expr_set;
  bool enable_fast_refresh_without_only_full_group_by = false;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt.get_query_ctx()));
  } else if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
               ObOptParamHint::ENABLE_FAST_REFRESH_WITHOUT_ONLY_FULL_GROUP_BY,
               enable_fast_refresh_without_only_full_group_by))) {
    LOG_WARN("failed to get enable fast refresh without only full group by", K(ret));
  } else if (OB_FAIL(expr_set.create(32))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else {
    const ObIArray<ObRawExpr *> &group_exprs = stmt.get_group_exprs();
    const ObIArray<SelectItem> &select_items = stmt.get_select_items();
    const bool is_no_aggr_col_allowed = enable_fast_refresh_without_only_full_group_by
                                        && !is_child_stmt(&stmt)
                                        && !params_.need_on_query_computation_;
    ObRawExpr *expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      uint64_t key = reinterpret_cast<uint64_t>(group_exprs.at(i));
      if (OB_FAIL(expr_set.set_refactored(key, 0))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add expr into set", K(ret));
        }
      }
    }
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_ISNULL(expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(expr));
      } else if (OB_FAIL(is_standard_select_in_group_by(expr_set,
                                                        select_items.at(i).expr_,
                                                        is_no_aggr_col_allowed && !expr->has_flag(CNT_AGG),
                                                        is_valid))) {
        LOG_WARN("failed to check is standard select in group by", K(ret));
      } else if (!is_valid) {
        LOG_TRACE("expr can not use in select for group by", K(is_valid), K(i), KPC(select_items.at(i).expr_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_set.destroy())) {
        LOG_WARN("failed to destroy stmt expr set", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinterChecker::is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                                       const ObRawExpr *expr,
                                                       const bool is_no_aggr_col_allowed,
                                                       bool &is_standard)
{
  int ret = OB_SUCCESS;
  is_standard = true;
  int tmp_ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  bool is_const_inherit = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr));
  } else if (expr->is_aggr_expr() || !expr->has_flag(CNT_COLUMN)) {
    /* do nothing */
  } else if (OB_HASH_EXIST == (tmp_ret = expr_set.exist_refactored(key))) {
    /* do nothing */
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("failed to check hash set exists", K(ret));
  } else if (expr->is_column_ref_expr()) {
    is_standard = is_no_aggr_col_allowed;
  } else if (OB_FAIL(expr->is_const_inherit_expr(is_const_inherit, true))) {
    LOG_WARN("failed to check is const inherit expr", K(ret));
  } else if (!is_const_inherit) {
    is_standard = false;
  } else {
    for (int64_t i = 0; is_standard && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_standard_select_in_group_by(expr_set,
                                                            expr->get_param_expr(i),
                                                            is_no_aggr_col_allowed,
                                                            is_standard)))) {
        LOG_WARN("failed to visit first", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinterChecker::check_mav_aggr_items(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_aggr_item_size(); ++i) {
    if (OB_FAIL(check_mav_aggr_item(stmt, stmt.get_aggr_item(i), is_valid))) {
      LOG_WARN("failed to check mav aggr item", K(ret), K(i), KPC(stmt.get_aggr_item(i)));
    }
  }
  return ret;
}

// fast refres can not support for these funs if there is only insert dml on base table.
int ObMVPrinterChecker::check_mav_aggr_item(const ObSelectStmt &stmt, ObAggFunRawExpr *aggr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(aggr));
  } else if (aggr->in_inner_stmt()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with nested aggregate functions is not supported");
  } else if (!ObMVCheckerUtils::is_basic_aggr(*aggr)) {
    // aggregate functions that cannot be maintained incrementally,
    // use group recalculate mechanism instead.
    is_valid = true;
    check_group_recalculate_constraints(stmt, is_valid);
  } else {
    is_valid = true;
    switch (aggr->get_expr_type()) {
    case T_FUN_COUNT: {
      if (0 == aggr->get_real_param_count()) {
        // COUNT(*) is valid.
      } else if (1 != aggr->get_real_param_count()) {
        is_valid = false;
        result_.get_note().error_.assign_fmt("COUNT() with multiple parameters is not supported");
      }
      break;
    }
    case T_FUN_SUM: {
      if (OB_UNLIKELY(1 != aggr->get_real_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected sum expr", K(ret), KPC(aggr));
      }
      break;
    }
    default: {
      is_valid = false;
      break;
    }
    }
  }
  return ret;
}

void ObMVPrinterChecker::check_group_recalculate_constraints(const ObSelectStmt &stmt, bool &is_valid)
{
  if (!stmt.is_single_table_stmt()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt(
      "group recalculate aggregation like min/max/count(distinct) only support for single table query");
  } else if (stmt.is_scala_group_by()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt(
      "group recalculate aggregation like min/max/count(distinct) not support for scalar group by");
  } else if (params_.need_on_query_computation_) {
    is_valid = false;
    result_.get_note().error_.assign_fmt(
      "group recalculate aggregation like min/max/count(distinct) not support for on query computation");
  }
}

int ObMVPrinterChecker::check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_COMPLETE_REFRESH;
  bool mlog_valid = true;
  bool has_outer_join = false;
  bool is_valid = false;
  if (OB_FAIL(check_join_shape_valid(stmt, is_valid, has_outer_join))) {
    LOG_WARN("failed to check join shape", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(collect_join_dep_cols(stmt, has_outer_join))) {
    LOG_WARN("failed to collect join dependent columns", K(ret));
  } else if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, mlog_valid))) {
    LOG_WARN("failed to check mv dependency mlog tables", KR(ret));
  } else if (mlog_valid) {
    refresh_type = has_outer_join ? OB_MV_FAST_REFRESH_OUTER_JOIN_MJV : OB_MV_FAST_REFRESH_SIMPLE_MJV;
  }
  return ret;
}

int ObMVPrinterCheckerBase::check_join_shape_valid(const ObSelectStmt &stmt, bool &is_valid, bool &has_outer_join)
{
  int ret = OB_SUCCESS;
  bool is_valid_join = 0;
  is_valid = false;
  has_outer_join = false;
  if (stmt.get_table_size() <= 0) {
    result_.get_note().error_.assign_fmt("materialized view with empty FROM clause is not supported");
    // } else if (stmt.get_table_size() > 5) {
    //   append_fast_refreshable_note("join table size more than 5 not support");
  } else if (OB_FAIL(check_mv_join_type(stmt, is_valid_join, has_outer_join))) {
    LOG_WARN("failed to check mv join type", K(ret));
  } else if (!is_valid_join) {
    // do nothing
  } else if (params_.need_on_query_computation_ && has_outer_join) {
    result_.get_note().error_.assign_fmt("on query computation is not supported for materialized view with OUTER JOIN");
  } else {
    is_valid = true;
  }
  return ret;
}

int ObMVPrinterCheckerBase::check_mv_join_type(const ObSelectStmt &stmt, bool &is_valid_join, bool &has_outer_join)
{
  int ret = OB_SUCCESS;
  is_valid_join = true;
  const ObIArray<JoinedTable *> &joined_tables = stmt.get_joined_tables();
  bool null_side_has_non_proctime_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < joined_tables.count(); ++i) {
    bool table_is_valid = false;
    bool table_has_outer_join = false;
    bool table_null_side_has_non_proctime = false;
    ObRelIds null_side_tables;
    if (OB_FAIL(is_mv_join_type_valid(stmt,
                                      joined_tables.at(i),
                                      null_side_tables,
                                      table_is_valid,
                                      table_has_outer_join,
                                      table_null_side_has_non_proctime))) {
      LOG_WARN("failed to check mv join type valid", K(ret));
    } else {
      is_valid_join &= table_is_valid;
      has_outer_join |= table_has_outer_join;
      null_side_has_non_proctime_table |= table_null_side_has_non_proctime;
    }
  }
  if (OB_FAIL(ret) || !is_valid_join || !null_side_has_non_proctime_table) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (stmt.get_table_item(i)->is_generated_table()) {
        result_.get_note().error_.assign_fmt(
          "materialized view that contains both VIEW and OUTER JOIN only supports PROCTIME() table on the right side "
          "of left join (or the left side of right join)");
        is_valid_join = false;
      }
    }
  }
  return ret;
}

int ObMVPrinterCheckerBase::is_mv_join_type_valid(const ObSelectStmt &stmt,
                                                  const TableItem *table,
                                                  ObRelIds &null_side_tables,
                                                  bool &is_valid_join,
                                                  bool &has_outer_join,
                                                  bool &null_side_has_non_proctime_table)
{
  int ret = OB_SUCCESS;
  bool is_child_has_outer_join = false;
  bool is_child_null_side_has_non_proctime_table = false;
  const JoinedTable *joined_table = NULL;
  is_valid_join = true;
  has_outer_join = false;
  null_side_has_non_proctime_table = false;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table));
  } else if (table->is_basic_table() || table->is_generated_table()) {
    // do nothing
  } else if (!table->is_joined_table()) {
    result_.get_note().error_.assign_fmt("table type is not supported");
    is_valid_join = false;
  } else if (OB_FALSE_IT(joined_table = static_cast<const JoinedTable *>(table))) {
  } else if (OB_FAIL(SMART_CALL(is_mv_join_type_valid(stmt,
                                                      joined_table->left_table_,
                                                      null_side_tables,
                                                      is_valid_join,
                                                      is_child_has_outer_join,
                                                      is_child_null_side_has_non_proctime_table)))) {
    LOG_WARN("failed to check left child", K(ret), KPC(joined_table));
  } else if (!is_valid_join) {
    // do nothing
  } else if (OB_FALSE_IT(has_outer_join |= is_child_has_outer_join)) {
  } else if (OB_FALSE_IT(null_side_has_non_proctime_table |= is_child_null_side_has_non_proctime_table)) {
  } else if (OB_FAIL(SMART_CALL(is_mv_join_type_valid(stmt,
                                                      joined_table->right_table_,
                                                      null_side_tables,
                                                      is_valid_join,
                                                      is_child_has_outer_join,
                                                      is_child_null_side_has_non_proctime_table)))) {
    LOG_WARN("failed to check right child", K(ret), KPC(joined_table));
  } else if (!is_valid_join) {
    // do nothing
  } else if (OB_FALSE_IT(has_outer_join |= is_child_has_outer_join)) {
  } else if (OB_FALSE_IT(null_side_has_non_proctime_table |= is_child_null_side_has_non_proctime_table)) {
  } else if (joined_table->is_inner_join()) {
    if (!has_outer_join) {
      // do nothing, is valid
    } else if (joined_table->left_table_->is_joined_table() && joined_table->right_table_->is_joined_table()) {
      // not a left deep tree
      result_.get_note().error_.assign_fmt("only support left deep join tree for materialized view with OUTER JOIN");
      is_valid_join = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); ++i) {
        const ObRawExpr *cond = joined_table->get_join_conditions().at(i);
        if (OB_ISNULL(cond)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(cond));
        } else if (cond->get_relation_ids().overlap(null_side_tables)) {
          result_.get_note().error_.assign_fmt(
            "inner join can not reference null side tables for materialized view with OUTER JOIN");
          is_valid_join = false;
          break;
        }
      }
    }
  } else if (joined_table->is_left_join() || joined_table->is_right_join()) {
    has_outer_join = true;
    const TableItem *null_side_table = joined_table->is_left_join() ? joined_table->right_table_
                                                                    : joined_table->left_table_;
    int64_t null_side_table_relid = stmt.get_table_bit_index(null_side_table->table_id_);
    ObRelIds null_side_table_relid_set;
    bool join_cond_contain_other_table = false;
    ObSEArray<const ObRawExpr *, 4> col_exprs;
    null_side_has_non_proctime_table |= !null_side_table->is_mv_proctime_table_;
    if (!(null_side_table->is_basic_table() || null_side_table->is_generated_table())) {
      // not a left deep tree
      result_.get_note().error_.assign_fmt("only support left deep join tree for materialized view with OUTER JOIN");
      is_valid_join = false;
    } else if (OB_FAIL(null_side_table_relid_set.add_member(null_side_table_relid))) {
      LOG_WARN("failed to add member", K(ret));
    }
    // check whether where conditions contain null side tables
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_condition_size(); ++i) {
      const ObRawExpr *cond = stmt.get_condition_expr(i);
      if (OB_ISNULL(cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (cond->get_relation_ids().has_member(null_side_table_relid)) {
        result_.get_note().error_.assign_fmt("WHERE conditions should not contain any null side table");
        is_valid_join = false;
      }
    }
    // check whether join conditions contain other table
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < joined_table->get_join_conditions().count(); ++i) {
      const ObRawExpr *cond = joined_table->get_join_conditions().at(i);
      if (OB_ISNULL(cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), KPC(joined_table));
      } else if (cond->get_relation_ids().num_members() > 2) {
        // only for performance considerations, can be removed if necessary
        result_.get_note().error_.assign_fmt("can not contain more than two tables in one OUTER JOIN condition");
        is_valid_join = false;
        break;
      } else if (!null_side_table_relid_set.is_superset(cond->get_relation_ids())) {
        join_cond_contain_other_table = true;
        // break; // can not break, continue to check whether other join conditions contain more than two tables
      }
    }
    if (OB_FAIL(ret) || !is_valid_join) {
      // do nothing
    } else if (!join_cond_contain_other_table) {
      result_.get_note().error_.assign_fmt("OUTER JOIN condition should contain at least one table from each side");
      is_valid_join = false;
    }
    // check whether join conditions are null reject for other null side tables
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_join && i < stmt.get_table_size(); ++i) {
      col_exprs.reuse();
      if (!null_side_tables.has_member(i + 1)) {
        // do nothing
      } else if (OB_ISNULL(stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i), K(stmt.get_table_size()));
      } else if (OB_FAIL(stmt.get_column_exprs(stmt.get_table_item(i)->table_id_, col_exprs))) {
        LOG_WARN("failed to get table column exprs", K(ret), KPC(stmt.get_table_item(i)));
      } else if (OB_FAIL(check_null_reject_or_not_contain(joined_table->get_join_conditions(),
                                                          col_exprs,
                                                          i + 1,
                                                          is_valid_join))) {
        LOG_WARN("failed to check null reject or not contain", K(ret));
      } else if (!is_valid_join) {
        result_.get_note().error_.assign_fmt("join condition should be null reject for the other tables");
        break;
      }
    }
    if (OB_FAIL(ret) || !is_valid_join) {
      // do nothing
    } else if (OB_FAIL(null_side_tables.add_member(null_side_table_relid))) {
      LOG_WARN("failed to add member", K(ret));
    }
  } else {
    result_.get_note().error_.assign_fmt("query join type is not supported");
    is_valid_join = false;
  }
  return ret;
}

int ObMVPrinterCheckerBase::check_null_reject_or_not_contain(const ObIArray<ObRawExpr *> &conditions,
                                                             const ObIArray<const ObRawExpr *> &table_col_exprs,
                                                             const int64_t table_rel_id,
                                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_null_reject = false;
  bool contain_cur_table = false;
  is_valid = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    const ObRawExpr *cond = conditions.at(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!cond->get_relation_ids().has_member(table_rel_id)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(cond, table_col_exprs, has_null_reject))) {
      LOG_WARN("failed to check null reject condition", K(ret), KPC(cond));
    } else {
      contain_cur_table = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!contain_cur_table || has_null_reject) {
    is_valid = true;
  }
  return ret;
}

int ObMVPrinterChecker::collect_join_dep_cols(const ObSelectStmt &stmt, const bool has_outer_join)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collect_tables_primary_key_for_select(stmt, has_outer_join))) {
    LOG_WARN("failed to collect tables primary key for select", K(ret));
  } else if (!has_outer_join) {
    // do nothing
  } else if (OB_FAIL(ObMVCheckerUtils::collect_outer_join_output_dep_cols(stmt,
                                                                          stmt_idx_,
                                                                          result_))) {
    LOG_WARN("failed to collect outer join output dependent columns", K(ret));
  }
  return ret;
}

int ObMVPrinterChecker::collect_tables_primary_key_for_select(const ObSelectStmt &stmt, const bool has_outer_join)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 8> pk_ids;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())
      || OB_ISNULL(schema_guard = query_ctx->sql_schema_guard_.get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_ctx), K(schema_guard));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    const TableItem *table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    pk_ids.reuse();
    if (OB_ISNULL(table_item = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i));
    } else if (table_item->is_generated_table() || (table_item->is_mv_proctime_table_ && !has_outer_join)) {
      // do nothing
    } else if (OB_UNLIKELY(!table_item->is_basic_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table type", K(ret), K(i), KPC(table_item));
    } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(table_item->ref_id_, table_schema))) {
      LOG_WARN("table schema not found", K(ret), KPC(table_item));
    } else if (OB_FAIL(ObMVCheckerUtils::get_missing_table_key_column_ids(stmt,
                                                                          *table_item,
                                                                          table_schema,
                                                                          schema_guard,
                                                                          pk_ids))) {
      LOG_WARN("failed to get missing table key column ids", K(ret), KPC(table_schema));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < pk_ids.count(); ++j) {
        const ColumnItem *col_item = stmt.get_column_item(table_item->table_id_, pk_ids.at(j));
        ObColumnRefRawExpr *col_expr = NULL;
        const ObColumnSchemaV2 *col_schema = NULL;
        if (NULL != col_item) {
          if (OB_ISNULL(col_expr = col_item->expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null", K(ret), KPC(table_item), K(pk_ids.at(j)), KPC(col_item));
          }
        } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(pk_ids.at(j)))) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("column not found", K(ret), K(pk_ids.at(j)), K(table_schema));
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(params_.expr_factory_,
                                                             *col_schema,
                                                             &params_.session_info_,
                                                             col_expr))) {
          LOG_WARN("failed to create column ref raw expr", K(ret), K(pk_ids.at(j)), KPC(col_schema));
        } else if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr is null", K(ret), KPC(table_item), K(pk_ids.at(j)), KPC(col_item));
        } else {
          col_expr->set_table_item_info(*table_item);
          col_expr->set_column_id(col_schema->get_column_id());
          col_expr->set_column_name(col_schema->get_column_name_str());
          col_expr->get_relation_ids().reuse();
          if (OB_FAIL(col_expr->formalize(&params_.session_info_))) {
            LOG_WARN("formalize col_expr failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(result_.add_dependent_column(col_expr, stmt_idx_))) {
          LOG_WARN("failed to add dependent column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMVPrinterChecker::check_union_all_refresh_type(ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_REFRESH_INVALID;
  bool is_valid = true;
  const ObIArray<ObSelectStmt *> &set_queries = stmt_.get_set_query();
  ObMVRefreshableType child_refresh_type = OB_MV_REFRESH_INVALID;
  if (ObSetRelation::UNION != stmt_.get_set_op() || stmt_.is_set_distinct()) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("query with set operators UNION/INTERSECT/EXCEPT/MINUS is not supported");
  } else if (params_.need_on_query_computation_) {
    is_valid = false;
    result_.get_note().error_.assign_fmt("on query computation is not supported for materialized view with UNION ALL");
  } else if (OB_FAIL(collect_union_all_mv_marker_column())) {
    LOG_WARN("failed to collect union all mv marker column", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < set_queries.count(); ++i) {
      stmt_idx_ = i;
      if (OB_ISNULL(set_queries.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(stmt_));
      } else if (OB_FAIL(check_mv_stmt_refresh_type(*set_queries.at(i), child_refresh_type))) {
        LOG_WARN("failed to check mv stmt refresh type", K(ret));
      } else if (!is_child_refresh_type_supported(child_refresh_type)) {
        is_valid = false;
        if (result_.get_note().error_.empty()) {
          if (OB_MV_COMPLETE_REFRESH >= child_refresh_type) {
            result_.get_note().error_.assign_fmt("fast refresh is not supported");
          } else {
            result_.get_note().error_.assign_fmt(
              "refresh type(%d) is not supported for materialized view with union all",
              child_refresh_type);
          }
        }
        result_.get_note().error_.append_fmt(" in the %ld-th set child query", i + 1);
      } else if (OB_FAIL(result_.add_child_refresh_type(child_refresh_type))) {
        LOG_WARN("failed to push back", K(ret));
      }
      stmt_idx_ = OB_INVALID_INDEX;
    }
  }

  if (OB_SUCC(ret)) {
    refresh_type = is_valid ? OB_MV_FAST_REFRESH_UNION_ALL : OB_MV_COMPLETE_REFRESH;
  }
  return ret;
}

int ObMVPrinterChecker::collect_union_all_mv_marker_column()
{
  int ret = OB_SUCCESS;
  int64_t marker_idx = OB_INVALID_INDEX;
  if (OB_FAIL(ObMVCheckerUtils::find_union_all_marker_column(stmt_, marker_idx))) {
    LOG_WARN("failed to find union all marker column", K(ret));
  } else if (OB_FAIL(result_.set_union_all_marker_idx(marker_idx))) {
    LOG_WARN("failed to set union all marker index", K(ret), K(marker_idx));
  }
  // If the marker expression is not found, add it as a dependent column.
  ObConstRawExpr *marker_expr = NULL;
  if (OB_FAIL(ret) || OB_INVALID_INDEX != marker_idx) {
    // do nothing
  } else if (OB_FAIL(ObMVCheckerUtils::build_union_all_marker_expr(params_.expr_factory_,
                                                                   params_.session_info_,
                                                                   marker_expr))) {
    LOG_WARN("failed to build union all marker", K(ret));
  } else if (OB_FAIL(result_.add_dependent_column(marker_expr, OB_INVALID_INDEX))) {
    LOG_WARN("failed to add union all marker dependent column", K(ret));
  }
  return ret;
}

bool ObMVPrinterChecker::is_child_refresh_type_supported(const ObMVRefreshableType refresh_type)
{
  bool bret = OB_MV_FAST_REFRESH_SIMPLE_MAV == refresh_type
              || OB_MV_FAST_REFRESH_SIMPLE_MJV == refresh_type
              || OB_MV_FAST_REFRESH_OUTER_JOIN_MJV == refresh_type
              || OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV == refresh_type
              || OB_MV_FAST_REFRESH_OUTER_JOIN_MAV == refresh_type;
  return bret;
}

}  // end of namespace sql
}  // end of namespace oceanbase

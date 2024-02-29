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

#define USING_LOG_PREFIX SQL_RESV
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/optimizer/ob_optimizer_util.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

int ObMVChecker::check_mv_fast_refresh_valid(const ObSelectStmt *view_stmt,
                                             ObRawExprFactory *expr_factory,
                                             ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(view_stmt) || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(view_stmt), K(expr_factory), K(session_info));
  } else {
    FastRefreshableNotes notes;
    ObMVChecker checker(*view_stmt, *expr_factory, session_info);
    checker.set_fast_refreshable_note(&notes);
    if (OB_FAIL(checker.check_mv_refresh_type())) {
      LOG_WARN("failed to check mv refresh type", K(ret));
    } else if (OB_UNLIKELY(OB_MV_FAST_REFRESH_SIMPLE_MAV > checker.get_refersh_type())) {
      ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
      LOG_WARN("fast refresh is not supported for this mv", K(ret), K(checker.get_refersh_type()), K(notes));
    }
  }
  return ret;
}

int ObMVChecker::check_mv_refresh_type()
{
  int ret = OB_SUCCESS;
  mlog_tables_.reuse();
  refresh_type_ = OB_MV_REFRESH_INVALID;
  bool is_valid = false;
  if (OB_FAIL(check_mv_stmt_refresh_type_basic(stmt_, is_valid))) {
    LOG_WARN("failed to check mv refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type_ = OB_MV_COMPLETE_REFRESH;
  } else if (stmt_.has_group_by()) {
    if (OB_FAIL(check_mav_refresh_type(stmt_, refresh_type_))) {
      LOG_WARN("failed to check mav refresh type", K(ret));
    }
  } else if (OB_FAIL(check_mjv_refresh_type(stmt_, refresh_type_))) {
    LOG_WARN("failed to check mjv refresh type", K(ret));
  }
  return ret;
}

int ObMVChecker::check_mv_stmt_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_SUCC(ret) && stmt.is_set_stmt()) {
    is_valid = false;
    append_fast_refreshable_note("set query not support");
  }

  if (OB_SUCC(ret) && (stmt.has_subquery())) {
    is_valid = false;
    append_fast_refreshable_note("subquery not support");
  }

  if (OB_SUCC(ret) && (stmt.has_order_by() || stmt.has_limit())) {
    is_valid = false;
    append_fast_refreshable_note("order by and limit not support");
  }

  if (OB_SUCC(ret) && (stmt.has_rollup() || stmt.has_cube() || stmt.has_grouping_sets()
                       || stmt.get_having_expr_size() > 0)) {
    is_valid = false;
    append_fast_refreshable_note("rollup/grouping sets/cube and having not support");
  }

  if (OB_SUCC(ret) && stmt.is_hierarchical_query()) {
    is_valid = false;
    append_fast_refreshable_note("hierarchical query not support");
  }

  if (OB_SUCC(ret) && stmt.has_window_function()) {
    is_valid = false;
    append_fast_refreshable_note("window function not support");
  }

  if (OB_SUCC(ret) && stmt.is_contains_assignment()) {
    is_valid = false;
    append_fast_refreshable_note("assignment not support");
  }

  if (OB_SUCC(ret) && stmt.has_sequence()) {
    is_valid = false;
    append_fast_refreshable_note("sequence not support");
  }

  if (OB_SUCC(ret)) {
    bool has_rownum = false;
    bool has_special_expr = false;
    const ObExprInfoFlag flag = CNT_RAND_FUNC;
    if (OB_FAIL(stmt.has_rownum(has_rownum))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (OB_FAIL(stmt.has_special_expr(flag, has_special_expr))) {
      LOG_WARN("failed to check has special expr", K(ret));
    } else if (has_special_expr || has_rownum || stmt.has_ora_rowscn()) {
      is_valid = false;
      append_fast_refreshable_note("rownum/ora_rowscn/rand_func not support");
    }
  }

  if (OB_SUCC(ret)) {
    bool from_table_valid = false;
    bool mlog_valid = false;
    if (OB_FAIL(check_mv_from_tables(stmt, from_table_valid))) {
      LOG_WARN("failed to check mv from tables", K(ret));
    } else if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, mlog_valid))) {
      LOG_WARN("failed to check mv table mlog", K(ret));
    } else if (!from_table_valid || !mlog_valid) {
      is_valid = false;
    }
  }
  return ret;
}

int ObMVChecker::check_mv_from_tables(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<TableItem*, 8> from_tables;
  if (stmt.get_from_item_size() == 0) {
    is_valid = false;
  } else if (OB_FAIL(stmt.get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else {
    int64_t table_item_cnt = 0;
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < from_tables.count(); ++i) {
      if (OB_ISNULL(from_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(from_tables));
      } else if (from_tables.at(i)->is_basic_table()) {
        ++table_item_cnt;
      } else if (!from_tables.at(i)->is_joined_table()) {
        is_valid = false;
      } else {
        is_valid = is_mv_joined_tables_valid(from_tables.at(i));
        table_item_cnt += static_cast<JoinedTable*>(from_tables.at(i))->single_table_ids_.count();
      }
    }
    LOG_TRACE("finish check check_mv_from_tables", K(is_valid), K(stmt.get_table_size()), K(table_item_cnt));
    if (OB_SUCC(ret) && is_valid) {
      is_valid = stmt.get_table_size() == table_item_cnt;
    }
    if (OB_SUCC(ret) && !is_valid) {
      append_fast_refreshable_note("inline view/join/subquery not support");
    }
  }
  return ret;
}

bool ObMVChecker::is_mv_joined_tables_valid(const TableItem *table)
{
  int bret = false;
  if (OB_ISNULL(table)) {
    bret = false;
  } else if (table->is_basic_table()) {
    bret = true;
  } else if (!table->is_joined_table()) {
    bret = false;
  } else {
    const JoinedTable *joined_table = static_cast<const JoinedTable*>(table);
    bret =  joined_table->is_inner_join()
            && is_mv_joined_tables_valid(joined_table->left_table_)
            && is_mv_joined_tables_valid(joined_table->right_table_);
  }
  return bret;
}

int ObMVChecker::check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  mlog_tables_.reuse();
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  if (OB_ISNULL(stmt.get_query_ctx())
      || OB_ISNULL(sql_schema_guard = &stmt.get_query_ctx()->sql_schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(sql_schema_guard));
  } else {
    is_valid = true;
    const ObIArray<TableItem*> &tables = stmt.get_table_items();
    const share::schema::ObTableSchema *table_schema = NULL;
    const share::schema::ObTableSchema *mlog_schema = NULL;
    const TableItem *table = NULL;
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(table = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", K(ret), KPC(table));
      } else if (OB_UNLIKELY(!table->is_basic_table())) {
        /* ObMVChecker::check_mv_from_tables has set refreshable as false, ignore this table here */
      } else if (OB_FAIL(sql_schema_guard->get_table_schema(table->ref_id_, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table_schema));
      } else if (OB_FAIL(sql_schema_guard->get_table_mlog_schema(table->ref_id_, mlog_schema))
                 || OB_ISNULL(mlog_schema)) {
        is_valid = false;
        append_fast_refreshable_note("need create mlog table for table in mv");
        ret = OB_SUCCESS;
      } else if (OB_FAIL(check_mlog_table_valid(table_schema, stmt.get_column_items(), *mlog_schema, is_valid))) {
        LOG_WARN("failed to get and check mlog table", K(ret));
      } else if (!is_valid) {
        append_fast_refreshable_note("need add column used in mv to mlog table");
      } else if (OB_FAIL(mlog_tables_.push_back(std::make_pair(table, mlog_schema)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::get_mlog_table_schema(const TableItem *table,
                                       const share::schema::ObTableSchema *&mlog_schema) const
{
  int ret = OB_SUCCESS;
  mlog_schema = NULL;
  for (int64_t i = 0; NULL == mlog_schema && OB_SUCC(ret) && i < mlog_tables_.count(); ++i) {
    if (table != mlog_tables_.at(i).first) {
      /* do nothing */
    } else if (OB_ISNULL(mlog_schema = mlog_tables_.at(i).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    }
  }
  return ret;
}

// get mlog table schema, check columns exists in mlog table
bool ObMVChecker::check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                                         const ObIArray<ColumnItem> &columns,
                                         const share::schema::ObTableSchema &mlog_schema,
                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  uint64_t mlog_cid = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> unique_col_ids;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(table_schema));
  } else if (!table_schema->is_heap_table()) {
    if (OB_FAIL(table_schema->get_rowkey_column_ids(unique_col_ids))) {
      LOG_WARN("failed to get rowkey column ids", KR(ret));
    }
  } else if (table_schema->get_partition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_partition_key_info().get_column_ids(unique_col_ids))) {
    LOG_WARN("failed to add part column ids", K(ret));
  } else if (table_schema->get_subpartition_key_info().is_valid() &&
             OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(unique_col_ids))) {
    LOG_WARN("failed to add subpart column ids", K(ret));
  }

  for (int i = 0; is_valid && OB_SUCC(ret) && i < unique_col_ids.count(); ++i) {
    // todo wait for yuya
    //mlog_cid = ObTableSchema::gen_mlog_col_id_from_ref_col_id(unique_col_ids.at(i));
    is_valid = NULL != mlog_schema.get_column_schema(unique_col_ids.at(i));
    LOG_DEBUG("check mlog_table column is valid", K(is_valid), K(i), K(mlog_cid), K(columns));
  }
  for (int i = 0; is_valid && OB_SUCC(ret) && i < columns.count(); ++i) {
    if (columns.at(i).base_tid_ == table_schema->get_table_id()) {
      mlog_cid = ObTableSchema::gen_mlog_col_id_from_ref_col_id(columns.at(i).base_cid_);
      is_valid = NULL != mlog_schema.get_column_schema(mlog_cid);
      LOG_DEBUG("check mlog_table column is valid", K(is_valid), K(i), K(mlog_cid), K(columns));
    }
  }
  return ret;
}

int ObMVChecker::check_mav_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  refresh_type = OB_MV_REFRESH_INVALID;
  expand_aggrs_.reuse();
  if (OB_FAIL(check_mav_refresh_type_basic(stmt, is_valid))) {
    LOG_WARN("failed to check refresh type basic", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (OB_FAIL(check_and_expand_mav_aggrs(stmt, expand_aggrs_, is_valid))) {
    LOG_WARN("failed to check mav aggr valid", K(ret));
  } else if (!is_valid) {
    refresh_type = OB_MV_COMPLETE_REFRESH;
  } else if (stmt.is_single_table_stmt()) { // only support single table MAV
    refresh_type = OB_MV_FAST_REFRESH_SIMPLE_MAV;
  } else {
    append_fast_refreshable_note("group by with multi table not support now", OB_MV_FAST_REFRESH_SIMPLE_MAV);
    refresh_type = OB_MV_COMPLETE_REFRESH;
  }
  return ret;
}

int ObMVChecker::check_mav_refresh_type_basic(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const ObAggFunRawExpr *default_count = NULL;  // count(*) need for non scalar group by
  if (OB_FAIL(get_target_aggr(T_FUN_COUNT, NULL, stmt.get_aggr_items(), default_count))) {
    LOG_WARN("failed to check target aggr exist", K(ret));
  } else if (!stmt.is_scala_group_by() && NULL == default_count) {
    append_fast_refreshable_note("need count(*) as select output", OB_MV_FAST_REFRESH_SIMPLE_MAV);
    is_valid = false;
  } else if (lib::is_mysql_mode() && OB_FAIL(check_is_standard_group_by(stmt, is_valid))) {
    LOG_WARN("failed to check is standard group by", K(ret));
  } else if (!is_valid) {
    append_fast_refreshable_note("select output is not standard group by for mysql mode", OB_MV_FAST_REFRESH_SIMPLE_MAV);
  } else {
    // check group by exprs exists in select list
    const ObIArray<ObRawExpr*> &group_exprs = stmt.get_group_exprs();
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      is_valid = stmt.check_is_select_item_expr(group_exprs.at(i));
    }
    if (!is_valid) {
      append_fast_refreshable_note("group by column need add to select list", OB_MV_FAST_REFRESH_SIMPLE_MAV);
    }
  }
  return ret;
}

// for mysql mode, check is standard group by
int ObMVChecker::check_is_standard_group_by(const ObSelectStmt &stmt, bool &is_standard)
{
  int ret = OB_SUCCESS;
  is_standard = true;
  hash::ObHashSet<uint64_t> expr_set;
  if (OB_FAIL(expr_set.create(32))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &group_exprs = stmt.get_group_exprs();
    const ObIArray<SelectItem> &select_items = stmt.get_select_items();
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
    for (int64_t i = 0; is_standard && OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_FAIL(is_standard_select_in_group_by(expr_set, select_items.at(i).expr_, is_standard))) {
        LOG_WARN("failed to push back null safe equal expr", K(ret));
      } else if (!is_standard) {
        LOG_TRACE("expr can not use in select for group by", K(is_standard), K(i), KPC(select_items.at(i).expr_));
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

int ObMVChecker::is_standard_select_in_group_by(const hash::ObHashSet<uint64_t> &expr_set,
                                                const ObRawExpr *expr,
                                                bool &is_standard)
{
  int ret = OB_SUCCESS;
  is_standard = true;
  int tmp_ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
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
    is_standard = false;
  } else {
    for (int64_t i = 0; is_standard && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_standard_select_in_group_by(expr_set, expr->get_param_expr(i), is_standard)))) {
        LOG_WARN("failed to visit first", K(ret));
      }
    }
  }
  return ret;
}

int ObMVChecker::check_and_expand_mav_aggrs(const ObSelectStmt &stmt,
                                            ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObAggFunRawExpr*, 8> all_aggrs;
  const ObIArray<ObAggFunRawExpr*> &aggrs = stmt.get_aggr_items();
  if (OB_FAIL(all_aggrs.assign(aggrs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < aggrs.count(); ++i) {
      if (OB_FAIL(check_and_expand_mav_aggr(stmt, aggrs.at(i), all_aggrs, expand_aggrs, is_valid))) {
        LOG_WARN("failed to check and expand mav aggr", K(ret));
      }
    }
  }
  return ret;
}

// do not support MAX/MIN. fast refres can not support for these funs if there is only insert dml on base table.
int ObMVChecker::check_and_expand_mav_aggr(const ObSelectStmt &stmt,
                                           ObAggFunRawExpr *aggr,
                                           ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                           ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(aggr));
  } else if (aggr->is_param_distinct() || aggr->is_nested_aggr()) {
    is_valid = false;
    append_fast_refreshable_note("nested aggr and aggr with distinct param not support", OB_MV_FAST_REFRESH_SIMPLE_MAV);
  } else {
    const int64_t orig_aggr_count = all_aggrs.count();
    switch (aggr->get_expr_type()) {
      case T_FUN_COUNT: {
        if (!stmt.check_is_select_item_expr(aggr)) {
          append_fast_refreshable_note("count aggr need add as select output", OB_MV_FAST_REFRESH_SIMPLE_MAV);
        } else {
          is_valid = true;
        }
        break;
      }
      case T_FUN_SUM: {
        const ObAggFunRawExpr *dependent_aggr = NULL;
        if (!stmt.check_is_select_item_expr(aggr)) {
          append_fast_refreshable_note("sum aggr need add as select output", OB_MV_FAST_REFRESH_SIMPLE_MAV);
        } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(stmt, aggr, dependent_aggr))) {
          LOG_WARN("failed to check sum aggr fast refresh valid", K(ret));
        } else if (NULL == dependent_aggr) {
          append_fast_refreshable_note("dependent aggr of sum aggr need add as select output", OB_MV_FAST_REFRESH_SIMPLE_MAV);
        } else {
          is_valid = true;
        }
        break;
      }
      case T_FUN_AVG:
      case T_FUN_STDDEV:
      case T_FUN_VARIANCE:  {
        ObRawExpr *replace_expr = NULL;
        ObExpandAggregateUtils expand_aggr_utils(expr_factory_, session_info_);
        expand_aggr_utils.set_expand_for_mv();
        if (OB_FAIL(expand_aggr_utils.expand_common_aggr_expr(aggr, replace_expr, all_aggrs))) {
          LOG_WARN("failed to expand common aggr expr", K(ret));
        } else if (all_aggrs.count() != orig_aggr_count) {
          /* expand aggr generate new aggr, can not fast refresh */
          is_valid = false;
          LOG_TRACE("aggr can not fast refresh", KPC(aggr), KPC(replace_expr), K(orig_aggr_count), K(all_aggrs));
          append_fast_refreshable_note("dependent aggr of some aggr need add as select output", OB_MV_FAST_REFRESH_SIMPLE_MAV);
          ObOptimizerUtil::revert_items(all_aggrs, orig_aggr_count);
        } else if (OB_FAIL(expand_aggrs.push_back(std::make_pair(aggr, replace_expr)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          /* need not check this aggr in select item, expand aggr will check when call this function by itself */
          is_valid = true;
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN:
      default : {
        is_valid = false;
        append_fast_refreshable_note("min/max and other aggr not support", OB_MV_FAST_REFRESH_SIMPLE_MAV);
        break;
      }
    }
  }
  return ret;
}

bool ObMVChecker::is_basic_aggr(const ObItemType aggr_type)
{
  return  T_FUN_COUNT == aggr_type || T_FUN_SUM == aggr_type;
}

//  count(c1) is needed for refresh sum(c1)
int ObMVChecker::get_dependent_aggr_of_fun_sum(const ObSelectStmt &stmt,
                                               const ObAggFunRawExpr *aggr,
                                               const ObAggFunRawExpr *&dependent_aggr)
{
  int ret = OB_SUCCESS;
  dependent_aggr = NULL;
  const ObRawExpr *param_expr = NULL;
  const ObRawExpr *inner_param_expr = NULL;
  if (OB_ISNULL(aggr)
      || OB_UNLIKELY(T_FUN_SUM != aggr->get_expr_type() || 1 != aggr->get_real_param_count())
      || OB_ISNULL(param_expr = aggr->get_real_param_exprs().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sum aggr", K(ret), K(aggr));
  } else if (OB_FAIL(get_target_aggr(T_FUN_COUNT, param_expr, stmt.get_aggr_items(), dependent_aggr))) {
    LOG_WARN("failed to check target aggr exist", K(ret));
  } else if (NULL != dependent_aggr && stmt.check_is_select_item_expr(dependent_aggr)) {
    /* do nothing */
  } else if (OB_FAIL(get_equivalent_null_check_param(param_expr, inner_param_expr))) {
    LOG_WARN("failed to get equivalent null check param", K(ret));
  } else if (NULL == inner_param_expr) {
    /* can not get valid dependent aggr */
    dependent_aggr = NULL;
  } else if (OB_FAIL(get_target_aggr(T_FUN_COUNT, inner_param_expr, stmt.get_aggr_items(), dependent_aggr))) {
    LOG_WARN("failed to check target aggr exist", K(ret));
  } else if (NULL != dependent_aggr && !stmt.check_is_select_item_expr(dependent_aggr)) {
    dependent_aggr = NULL;
  }
  return ret;
}

//  We need calculate sum(c1*c1) to get the value of stddev(c1).
//  To refrsh sum(c1*c1) and avoid calculate count(c1*c1), count(c1) can also used to refrsh sum(c1*c1).
//  Here try to get c1 as equivalent aggr param of c1*c1 only.
int ObMVChecker::get_equivalent_null_check_param(const ObRawExpr *param_expr,
                                                 const ObRawExpr *&inner_param_expr)
{
  int ret = OB_SUCCESS;
  inner_param_expr = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(param_expr));
  } else if (T_OP_MUL == param_expr->get_expr_type()) {
    //  sum(c1*c1) can use count(c1)
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr*>(param_expr);
    if (op_expr->get_param_expr(0) == op_expr->get_param_expr(1)) {
      inner_param_expr = op_expr->get_param_expr(0);
    }
  } else {
    // zhanyuetodo: sum(1) / sum(not null column) need not count(*)
    inner_param_expr = param_expr;
  }
  return ret;
}

// only used to check COUNT/SUM aggr without distinct
int ObMVChecker::get_target_aggr(const ObItemType target_aggr_type,
                                 const ObRawExpr *param_expr,
                                 const ObIArray<ObAggFunRawExpr*> &aggrs,
                                 const ObAggFunRawExpr *&target_aggr)
{
  int ret = OB_SUCCESS;
  target_aggr = NULL;
  const ObAggFunRawExpr *aggr = NULL;
  const ObRawExpr *cur_param_expr = NULL;
  if (OB_UNLIKELY(T_FUN_COUNT != target_aggr_type && T_FUN_SUM != target_aggr_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggr type for this function", K(ret), K(target_aggr_type));
  }
  for (int64_t i = 0; NULL == target_aggr && OB_SUCC(ret) && i < aggrs.count(); ++i) {
    if (OB_ISNULL(aggr = aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (aggr->get_expr_type() != target_aggr_type) {
      /* do nothing */
    } else if (0 == aggr->get_real_param_count()) {
      target_aggr = NULL == param_expr ? aggr : NULL;
    } else if (NULL == param_expr || NULL == (cur_param_expr = aggr->get_real_param_exprs().at(0))) {
      /* do nothing */
    } else if (cur_param_expr->same_as(*param_expr)) {
      target_aggr = aggr;
    }
  }
  return ret;
}

int ObMVChecker::check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  refresh_type = OB_MV_COMPLETE_REFRESH;
  append_fast_refreshable_note("mv without group by not support now");
  return ret;
}

void ObMVChecker::append_fast_refreshable_note(const char *str,
                                               const ObMVRefreshableType type /* default OB_MV_COMPLETE_REFRESH */)
{
  int ret = OB_SUCCESS;
  if (NULL == fast_refreshable_note_) {
    /* do nothing */
  } else if (OB_FAIL(fast_refreshable_note_->inner_errcode_)) {
    /* do nothing */
  } else {
    ObSqlString *sql_str = NULL;
    switch (type) {
      case OB_MV_COMPLETE_REFRESH: // record basic info for fast refresh
        sql_str = &fast_refreshable_note_->basic_; break;
      case OB_MV_FAST_REFRESH_SIMPLE_MAV:
        sql_str = &fast_refreshable_note_->simple_mav_; break;
      default:
        sql_str = NULL;
    }
    if (OB_ISNULL(sql_str)) {
      /* do nothing */
    } else if (OB_FAIL(sql_str->append(str))) {
      LOG_WARN("failed to append sql string", K(ret));
    } else if (OB_FAIL(sql_str->append("; "))) {
      LOG_WARN("failed to append sql string", K(ret));
    }
    fast_refreshable_note_->inner_errcode_ = ret;
  }
}

}//end of namespace sql
}//end of namespace oceanbase

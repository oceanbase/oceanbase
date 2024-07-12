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
#include "sql/rewrite/ob_transform_utils.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

int ObMVChecker::check_mv_fast_refresh_valid(const ObSelectStmt *view_stmt,
                                             ObStmtFactory *stmt_factory,
                                             ObRawExprFactory *expr_factory,
                                             ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *copied_stmt = NULL;
  if (OB_ISNULL(view_stmt) || OB_ISNULL(stmt_factory)
      || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(view_stmt), K(expr_factory), K(session_info));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*stmt_factory, *expr_factory,
                                                      view_stmt, copied_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(copied_stmt->formalize_stmt_expr_reference(expr_factory, session_info, true))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
  } else {
    FastRefreshableNotes notes;
    ObMVChecker checker(*static_cast<ObSelectStmt*>(copied_stmt), *expr_factory, session_info);
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
    if (OB_FAIL(stmt.has_rownum(has_rownum))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (OB_FAIL(check_mv_stmt_use_special_expr(stmt, has_special_expr))) {
      LOG_WARN("failed to check mv stmt use special expr", K(ret));
    } else if (has_special_expr || has_rownum || stmt.has_ora_rowscn()) {
      is_valid = false;
      append_fast_refreshable_note("rownum/ora_rowscn/rand_func not support");
    }
  }

  if (OB_SUCC(ret)) {
    bool mlog_valid = false;
    if (OB_FAIL(check_mv_dependency_mlog_tables(stmt, mlog_valid))) {
      LOG_WARN("failed to check mv table mlog", K(ret));
    } else if (!mlog_valid) {
      is_valid = false;
    }
  }

  if (OB_SUCC(ret)) {
    bool has_dup_exprs = false;
    if (OB_FAIL(check_mv_duplicated_exprs(stmt, has_dup_exprs))) {
      LOG_WARN("failed to check mv table mlog", K(ret));
    } else if (has_dup_exprs) {
      is_valid = false;
    }
  }
  return ret;
}

int ObMVChecker::check_mv_stmt_use_special_expr(const ObSelectStmt &stmt, bool &has_special_expr)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> flags;
  ObExprInfoFlag flag_arr[] = { CNT_RAND_FUNC,
                                CNT_STATE_FUNC,
                                CNT_SEQ_EXPR,
                                CNT_VOLATILE_CONST,
                                CNT_DYNAMIC_USER_VARIABLE,
                                CNT_CUR_TIME,
                                /* add new check flag above CNT_ASSOCIATED_FLAG_END */
                                CNT_ASSOCIATED_FLAG_END };
  for (int64_t i = 0; OB_SUCC(ret) && CNT_ASSOCIATED_FLAG_END != flag_arr[i] ; ++i) {
    if (OB_FAIL(flags.add_member(flag_arr[i]))) {
      LOG_WARN("failed to add member", K(ret), K(i), K(flag_arr[i]));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt.has_special_exprs(flags, has_special_expr))) {
    LOG_WARN("failed to check has special exprs", K(ret));
  }
  return ret;
}

int ObMVChecker::check_mv_duplicated_exprs(const ObSelectStmt &stmt, bool &has_dup_exprs)
{
  int ret = OB_SUCCESS;
  has_dup_exprs = false;
  ObSEArray<ObRawExpr*, 16> tmp_exprs;
  const ObIArray<SelectItem> &select_items = stmt.get_select_items();
  const ObIArray<ObRawExpr*> &group_exprs = stmt.get_group_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (has_exist_in_array(tmp_exprs, select_items.at(i).expr_)) {
      has_dup_exprs = true;
      append_fast_refreshable_note("duplicated select output");
      LOG_WARN("fast refresh not support due to duplicated select output", K(i), K(select_items.at(i)));
    } else if (OB_FAIL(tmp_exprs.push_back(select_items.at(i).expr_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  tmp_exprs.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
    if (has_exist_in_array(tmp_exprs, group_exprs.at(i))) {
      has_dup_exprs = true;
      append_fast_refreshable_note("duplicated group by expr");
      LOG_WARN("fast refresh not support due to duplicated group by expr", K(i), KPC(group_exprs.at(i)));
    } else if (OB_FAIL(tmp_exprs.push_back(group_exprs.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
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
  } else if (stmt.get_table_size() == 0) {
    is_valid = false;
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
        is_valid = false;
        append_fast_refreshable_note("basic table allowed only");
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
  if (!stmt.is_scala_group_by() && OB_FAIL(get_mav_default_count(stmt.get_aggr_items(), default_count))) {
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
  } else if (aggr->is_param_distinct() || aggr->in_inner_stmt()) { // 这里判断完全没有has_nested_aggr_的时候in_inner_stmt 才会为true
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
        } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(stmt, aggr->get_param_expr(0), dependent_aggr))) {
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
        } else if (all_aggrs.count() != orig_aggr_count
                   && OB_FAIL(try_replace_equivalent_count_aggr(stmt, orig_aggr_count, all_aggrs, replace_expr))) {
          LOG_WARN("failed to try replace equivalent count aggr ", K(ret));
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

int ObMVChecker::try_replace_equivalent_count_aggr(const ObSelectStmt &stmt,
                                                   const int64_t orig_aggr_count,
                                                   ObIArray<ObAggFunRawExpr*> &all_aggrs,
                                                   ObRawExpr *&replace_expr)
{
  int ret = OB_SUCCESS;
  const ObAggFunRawExpr *aggr = NULL;
  const ObAggFunRawExpr *equal_aggr = NULL;
  bool aggr_not_support = false;
  ObRawExprCopier copier(expr_factory_);
  for (int64_t i = orig_aggr_count; !aggr_not_support && OB_SUCC(ret) && i < all_aggrs.count(); ++i) {
    if (OB_ISNULL(aggr = all_aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(aggr));
    } else if (T_FUN_COUNT != aggr->get_expr_type() || 1 != aggr->get_real_param_count()) {
      aggr_not_support = true;
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(stmt, aggr->get_param_expr(0), equal_aggr))) {
      LOG_WARN("failed to get equivalent count aggr", K(ret));
    } else if (NULL == equal_aggr) {
      aggr_not_support = true;
    } else if (OB_FAIL(copier.add_replaced_expr(aggr, equal_aggr))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }
  if (OB_SUCC(ret) && !aggr_not_support) {
    ObRawExpr *new_replace_expr = NULL;
    ObOptimizerUtil::revert_items(all_aggrs, orig_aggr_count);
    if (OB_FAIL(copier.copy_on_replace(replace_expr, new_replace_expr))) {
      LOG_WARN("failed to generate group by exprs", K(ret));
    } else {
      replace_expr = new_replace_expr;
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
                                               const ObRawExpr *sum_param,
                                               const ObAggFunRawExpr *&dep_aggr)
{
  int ret = OB_SUCCESS;
  dep_aggr = NULL;
  const ObRawExpr *check_param = NULL;
  if (OB_FAIL(get_equivalent_null_check_param(sum_param, check_param))) {
    LOG_WARN("failed to get null check param", K(ret));
  } else {
    const ObIArray<ObAggFunRawExpr*> &aggrs = stmt.get_aggr_items();
    const ObAggFunRawExpr *cur_aggr = NULL;
    const ObRawExpr *cur_check_param = NULL;
    for (int64_t i = 0; NULL == dep_aggr && OB_SUCC(ret) && i < aggrs.count(); ++i) {
      if (OB_ISNULL(cur_aggr = aggrs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (T_FUN_COUNT != cur_aggr->get_expr_type()
                 || cur_aggr->is_param_distinct()
                 || 1 != cur_aggr->get_real_param_count()) {
        /* do nothing */
      } else if (OB_FAIL(get_equivalent_null_check_param(cur_aggr->get_param_expr(0),
                                                         cur_check_param))) {
        LOG_WARN("failed to get null check param", K(ret));
      } else if (cur_check_param->same_as(*check_param)) {
        dep_aggr = cur_aggr;
      }
    }
  }
  return ret;
}

//  We need calculate sum(c1*c1) to get the value of stddev(c1).
//  To refrsh sum(c1*c1) and avoid calculate count(c1*c1), count(c1) can also used to refrsh sum(c1*c1).
//  Here try to get c1 as equivalent aggr param of c1*c1 only.
int ObMVChecker::get_equivalent_null_check_param(const ObRawExpr *param_expr,
                                                 const ObRawExpr *&check_param)
{
  int ret = OB_SUCCESS;
  check_param = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(param_expr));
  } else if (T_FUN_SYS_CAST == param_expr->get_expr_type()) {
    //  sum(cast(c1_varchar as int)) can use count(c1_varchar)
    if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(param_expr->get_param_expr(0), check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else if (T_OP_MUL == param_expr->get_expr_type()) {
    //  sum(c1*c1) can use count(c1)
    const ObRawExpr *l_expr = NULL;
    const ObRawExpr *r_expr = NULL;
    if (OB_ISNULL(l_expr = param_expr->get_param_expr(0))
        || OB_ISNULL(r_expr = param_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if (!l_expr->same_as(*r_expr)) {
      check_param = param_expr;
    } else if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(l_expr, check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else {
    check_param = param_expr;
  }
  return ret;
}

int ObMVChecker::get_mav_default_count(const ObIArray<ObAggFunRawExpr*> &aggrs,
                                       const ObAggFunRawExpr *&count_aggr)
{
  int ret = OB_SUCCESS;
  count_aggr = NULL;
  const ObAggFunRawExpr *aggr = NULL;
  for (int64_t i = 0; NULL == count_aggr && OB_SUCC(ret) && i < aggrs.count(); ++i) {
    if (OB_ISNULL(aggr = aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (T_FUN_COUNT == aggr->get_expr_type() && 0 == aggr->get_real_param_count()) {
      count_aggr = aggr;
    }
  }
  return ret;
}

int ObMVChecker::check_mjv_refresh_type(const ObSelectStmt &stmt, ObMVRefreshableType &refresh_type)
{
  int ret = OB_SUCCESS;
  refresh_type = OB_MV_COMPLETE_REFRESH;
  bool join_type_valid = true;
  bool select_valid = true;
  if (stmt.get_table_size() <= 1) {
    append_fast_refreshable_note("table size not support");
  // } else if (stmt.get_table_size() > 5) {
  //   append_fast_refreshable_note("join table size more than 5 not support");
  } else if (OB_FAIL(check_mv_join_type(stmt, join_type_valid))) {
    LOG_WARN("failed to check mv join type", K(ret));
  } else if (!join_type_valid) {
    append_fast_refreshable_note("outer join not support");
  } else if (OB_FAIL(check_select_contains_all_tables_primary_key(stmt, select_valid))) {
    LOG_WARN("failed to check check select contains all tables primary key", K(ret));
  } else if (!select_valid) {
    append_fast_refreshable_note("base table primary key need in select for MJV");
  } else {
    refresh_type = OB_MV_FAST_REFRESH_SIMPLE_MJV;
  }
  return ret;
}

int ObMVChecker::check_mv_join_type(const ObSelectStmt &stmt, bool &join_type_valid)
{
  int ret = OB_SUCCESS;
  join_type_valid = true;
  const ObIArray<JoinedTable*> &joined_tables = stmt.get_joined_tables();
  for (int64_t i = 0; join_type_valid && OB_SUCC(ret) && i < joined_tables.count(); ++i) {
    join_type_valid &= is_mv_join_type_valid(joined_tables.at(i));
  }
  return ret;
}

bool ObMVChecker::is_mv_join_type_valid(const TableItem *table)
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
            && is_mv_join_type_valid(joined_table->left_table_)
            && is_mv_join_type_valid(joined_table->right_table_);
  }
  return bret;
}

int ObMVChecker::check_select_contains_all_tables_primary_key(const ObSelectStmt &stmt,
                                                              bool &contain_all_rowkey)
{
  int ret = OB_SUCCESS;
  contain_all_rowkey = false;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt.get_query_ctx()));
  } else {
    contain_all_rowkey = true;
    int64_t all_rowkey_size = 0;
    ObSEArray<const ObRawExpr*, 8> rowkeys;
    ObSqlSchemaGuard &sql_schema_guard = stmt.get_query_ctx()->sql_schema_guard_;
    for (int64_t i = 0; contain_all_rowkey && OB_SUCC(ret) && i < stmt.get_table_items().count(); ++i) {
      TableItem *table_item = NULL;
      const ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(table_item = stmt.get_table_items().at(i))
          || OB_UNLIKELY(!table_item->is_basic_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", K(ret), K(i), KPC(table_item));
      } else if (OB_FAIL(sql_schema_guard.get_table_schema(table_item->ref_id_, table_schema))) {
        LOG_WARN("table schema not found", K(table_schema));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(ret), K(table_schema));
      } else if (table_schema->is_heap_table()) {
        contain_all_rowkey = false;
      } else {
        all_rowkey_size += table_schema->get_rowkey_info().get_size();
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_items().count(); ++i) {
      const ObRawExpr *expr = NULL;
      int64_t idx = OB_INVALID_INDEX;
      if (OB_ISNULL(expr = stmt.get_select_items().at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(stmt.get_select_items()));
      } else if (!expr->is_column_ref_expr() || !static_cast<const ObColumnRefRawExpr*>(expr)->is_rowkey_column()) {
        /* do nothing */
      } else if (OB_FAIL(add_var_to_array_no_dup(rowkeys, expr, &idx))) {
        LOG_WARN("failed to add_var to array no dup", K(ret));
      }
    }

    if (OB_FAIL(ret) || !contain_all_rowkey) {
      /* do nothing */
    } else if (all_rowkey_size > rowkeys.count()) {
      contain_all_rowkey = false;
    } else if (OB_UNLIKELY(rowkeys.count() != all_rowkey_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey size", K(ret), K(all_rowkey_size), K(rowkeys.count()), K(rowkeys));
    } else {
      contain_all_rowkey = true;
    }
  }
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

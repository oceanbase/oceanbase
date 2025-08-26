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
#include "sql/resolver/mv/ob_mv_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// view name
const ObString ObMVPrinter::DELTA_TABLE_VIEW_NAME  = "DLT_T$$";
const ObString ObMVPrinter::DELTA_BASIC_MAV_VIEW_NAME = "DLT_BASIC_MV$$";
const ObString ObMVPrinter::DELTA_MAV_VIEW_NAME = "DLT_MV$$";
const ObString ObMVPrinter::INNER_RT_MV_VIEW_NAME = "INNER_RT_MV$$";
// column name
const ObString ObMVPrinter::HEAP_TABLE_ROWKEY_COL_NAME  = "M_ROW$$";
const ObString ObMVPrinter::OLD_NEW_COL_NAME  = "OLD_NEW$$";
const ObString ObMVPrinter::SEQUENCE_COL_NAME  = "SEQUENCE$$";
const ObString ObMVPrinter::DML_FACTOR_COL_NAME  = "DMLFACTOR$$";
const ObString ObMVPrinter::WIN_MAX_SEQ_COL_NAME  = "MAXSEQ$$";
const ObString ObMVPrinter::WIN_MIN_SEQ_COL_NAME  = "MINSEQ$$";

int ObMVPrinter::print_mv_operators(ObIAllocator &str_alloc,
                                    ObIArray<ObString> &operators)
{
  int ret = OB_SUCCESS;
  operators.reuse();
  ObSEArray<ObDMLStmt*, 4> dml_stmts;
  if (OB_ISNULL(mv_def_stmt_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(mv_def_stmt_.get_query_ctx()));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init mv printer", K(ret));
  } else if (OB_FAIL(gen_mv_operator_stmts(dml_stmts))) {
    LOG_WARN("failed to print mv operators", K(ret));
  } else if (OB_UNLIKELY(dml_stmts.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty array", K(ret), K(dml_stmts.empty()));
  } else if (OB_FAIL(operators.prepare_allocate(dml_stmts.count()))) {
    LOG_WARN("failed to prepare allocate ObSqlString arrays", K(ret), K(dml_stmts.count()));
  } else {
    ObObjPrintParams obj_print_params(mv_def_stmt_.get_query_ctx()->get_timezone_info());
    obj_print_params.print_origin_stmt_ = true;
    obj_print_params.not_print_internal_catalog_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_stmts.count(); ++i) {
      if (OB_FAIL(ObSQLUtils::reconstruct_sql(str_alloc,
                                              dml_stmts.at(i),
                                              operators.at(i),
                                              mv_def_stmt_.get_query_ctx()->sql_schema_guard_.get_schema_guard(),
                                              obj_print_params))) {
        LOG_WARN("fail to reconstruct sql", K(ret));
      } else {
        LOG_TRACE("generate one mv operator", K(i), K(operators.at(i)));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_mv_operator_stmts(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObSelectStmt *sel_stmt = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("printer is not inited", K(ret));
  } else if (!ctx_.for_rt_expand()) {
    if (OB_FAIL(gen_refresh_dmls(dml_stmts))) {
      LOG_WARN("failed to gen refresh dmls for mv", K(ret));
    }
  } else if (OB_FAIL(gen_real_time_view(sel_stmt))) {
    LOG_WARN("failed to gen real time view for mv", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(sel_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObMVPrinter::get_mv_select_item_name(const ObRawExpr *expr, ObString &select_name)
{
  int ret = OB_SUCCESS;
  select_name = ObString::make_empty_string();
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  for (int64_t i = 0; select_name.empty() && OB_SUCC(ret) && i < select_items.count(); ++i) {
    const SelectItem &select_item = select_items.at(i);
    if (select_item.expr_ != expr) {
      /* do nothing */
    } else if (OB_UNLIKELY(!select_item.is_real_alias_ || select_item.alias_name_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else {
      select_name = select_item.alias_name_;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(select_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get select item name", K(ret), KPC(expr));
  }
  return ret;
}

//(old_new = 'N' and seq_no = "MAXSEQ$$") or (old_new = 'O' and seq_no = "MINSEQ$$")
int ObMVPrinter::append_old_new_row_filter(const TableItem &table_item,
                                           ObIArray<ObRawExpr*> &filters,
                                           const bool get_old_row,  /* default true */
                                           const bool get_new_row /* default true */)
{
  int ret = OB_SUCCESS;
  ObRawExpr *old_new = NULL;
  ObRawExpr *seq_no = NULL;
  ObRawExpr *win_seq = NULL;
  ObRawExpr *equal_old_new = NULL;
  ObRawExpr *equal_seq_no = NULL;
  ObRawExpr *new_row_filter = NULL;
  ObRawExpr *old_row_filter = NULL;
  ObRawExpr *filter = NULL;
  const ObString &table_name = table_item.get_table_name();
  const uint64_t table_id = table_item.table_id_;
  if (OB_UNLIKELY(!get_old_row && !get_new_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(get_old_row), K(get_new_row));
  } else if (OB_FAIL(create_simple_column_expr(table_name, OLD_NEW_COL_NAME, table_id, old_new))
             ||OB_FAIL(create_simple_column_expr(table_name, SEQUENCE_COL_NAME, table_id, seq_no))) {
    LOG_WARN("failed to create simple column exprs", K(ret));
  }

  if (OB_FAIL(ret) || !get_new_row) {
  } else if (OB_FAIL(create_simple_column_expr(table_name, WIN_MAX_SEQ_COL_NAME , table_id, win_seq))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, old_new, exprs_.str_n_, equal_old_new))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, seq_no, win_seq, equal_seq_no))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_AND, equal_old_new, equal_seq_no, new_row_filter))) {
    LOG_WARN("failed to build new row filter expr", K(ret));
  }

  if (OB_FAIL(ret) || !get_old_row) {
  } else if (OB_FAIL(create_simple_column_expr(table_name, WIN_MIN_SEQ_COL_NAME, table_id, win_seq))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, old_new, exprs_.str_o_, equal_old_new))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, seq_no, win_seq, equal_seq_no))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_AND, equal_old_new, equal_seq_no, old_row_filter))) {
    LOG_WARN("failed to build old row filter expr", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (get_old_row && !get_new_row) {
    if (OB_FAIL(filters.push_back(old_row_filter))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  } else if (!get_old_row && get_new_row) {
    if (OB_FAIL(filters.push_back(new_row_filter))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_OR, new_row_filter, old_row_filter, filter))) {
    LOG_WARN("failed to build or op expr", K(ret));
  } else if (OB_FAIL(filters.push_back(filter))) {
    LOG_WARN("failed to pushback", K(ret));
  }
  return ret;
}

int ObMVPrinter::get_mlog_table_schema(const TableItem *table,
                                       const share::schema::ObTableSchema *&mlog_schema) const
{
  int ret = OB_SUCCESS;
  mlog_schema = NULL;
  if (OB_NOT_NULL(mlog_tables_)) {
    for (int64_t i = 0; NULL == mlog_schema && OB_SUCC(ret) && i < mlog_tables_->count(); ++i) {
      if (table != mlog_tables_->at(i).first) {
        /* do nothing */
      } else if (OB_ISNULL(mlog_schema = mlog_tables_->at(i).second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      }
    }
  }
  return ret;
}

//  select  t.*,
//          min(sequence) over (...),
//          max(sequence) over (...),
//          (case when "OLD_NEW$$" = 'N' then 1 else -1 end) dml_factor
//  from mlog_tbale
//  where scn > xxx and scn <= scn;
int ObMVPrinter::gen_delta_table_view(const TableItem &source_table,
                                      ObSelectStmt *&view_stmt,
                                      const uint64_t ext_sel_flags)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  const ObTableSchema *mlog_schema = NULL;
  TableItem *table_item = NULL;
  ObSelectStmt *select_stmt = NULL;
  if (OB_FAIL(create_simple_stmt(view_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(get_mlog_table_schema(&source_table, mlog_schema))) {
    LOG_WARN("failed to get mlog schema", K(ret), K(source_table));
  } else if (OB_ISNULL(view_stmt) || OB_ISNULL(mlog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(view_stmt), K(mlog_schema), K(source_table));
  } else if (OB_FAIL(create_simple_table_item(view_stmt, mlog_schema->get_table_name_str(), table_item))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_delta_table_view_conds(*table_item, view_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to generate delta table view conds", K(ret));
  } else if (OB_FAIL(gen_delta_table_view_select_list(*table_item, source_table, *view_stmt, ext_sel_flags))) {
    LOG_WARN("failed to generate delta table view select lists", K(ret));
  } else {
    table_item->database_name_ = source_table.database_name_;
    // mlog need not flashback query
    table_item->flashback_query_expr_ = NULL;
    table_item->flashback_query_type_ = TableItem::NOT_USING;
  }
  return ret;
}

int ObMVPrinter::gen_delta_table_view_conds(const TableItem &table,
                                            ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  ObRawExpr *scn_column = NULL;
  ObRawExpr *filter = NULL;
  if (OB_FAIL(create_simple_column_expr(table.get_table_name(), ObString("ORA_ROWSCN"), 0, scn_column))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_GT, scn_column, exprs_.last_refresh_scn_, filter))) {
    LOG_WARN("failed to build greater op expr", K(ret));
  } else if (OB_FAIL(conds.push_back(filter))) {
    LOG_WARN("failed to pushback", K(ret));
  } else if (ctx_.for_rt_expand()) {
    /* do nothing */
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_LE, scn_column, exprs_.refresh_scn_, filter))) {
    LOG_WARN("failed to build less or equal op expr", K(ret));
  } else if (OB_FAIL(conds.push_back(filter))) {
    LOG_WARN("failed to pushback", K(ret));
  }
  return ret;
}

// generate select list for mlog table:
//  1. normal column matched datatable
//  2. old_new$$, sequence$$ column
//  3. dml_factor: (CASE WHEN OLD_NEW$$ = 'N' THEN 1 ELSE -1 END)
//  4. min/max window function: min(sequence$$) over (partition by unique_keys), max(sequence$$) over (partition by unique_keys)
int ObMVPrinter::gen_delta_table_view_select_list(const TableItem &table,
                                                  const TableItem &source_table,
                                                  ObSelectStmt &stmt,
                                                  const uint64_t ext_sel_flags)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem> &select_items = stmt.get_select_items();
  select_items.reuse();
  ObRawExpr *old_new_col = NULL;
  ObRawExpr *sequence_expr = NULL;
  const bool need_old_new_col = (ext_sel_flags & MLOG_EXT_COL_OLD_NEW)
                                || (ext_sel_flags & MLOG_EXT_COL_DML_FACTOR);
  const bool need_sql_col = (ext_sel_flags & MLOG_EXT_COL_SEQ)
                            || (ext_sel_flags & MLOG_EXT_COL_WIN_MIN_SEQ)
                            || (ext_sel_flags & MLOG_EXT_COL_WIN_MAX_SEQ);
  const bool need_dml_factor_col = ext_sel_flags & MLOG_EXT_COL_DML_FACTOR;
  const bool need_win_min_col = ext_sel_flags & MLOG_EXT_COL_WIN_MIN_SEQ;
  const bool need_win_max_col = ext_sel_flags & MLOG_EXT_COL_WIN_MAX_SEQ;
  if (OB_FAIL(ret) || !need_old_new_col) {
  } else if (OB_FAIL(add_normal_column_to_select_list(table, OLD_NEW_COL_NAME, select_items))) {
    LOG_WARN("failed to add normal column to select list", K(ret));
  } else {
    old_new_col = select_items.at(select_items.count() - 1).expr_;
  }

  if (OB_FAIL(ret) || !need_sql_col) {
  } else if (OB_FAIL(add_normal_column_to_select_list(table, SEQUENCE_COL_NAME, select_items))) {
    LOG_WARN("failed to add normal column to select list", K(ret));
  } else {
    sequence_expr = select_items.at(select_items.count() - 1).expr_;
  }

  if (OB_FAIL(ret)) {
  } else if (need_dml_factor_col
             && OB_FAIL(add_dml_factor_to_select_list(old_new_col, select_items))) {
    LOG_WARN("failed to add dml factor to select list", K(ret));
  } else if ((need_win_min_col || need_win_max_col)
             && OB_FAIL(add_max_min_seq_window_to_select_list(table, source_table,
                                                              sequence_expr,
                                                              select_items,
                                                              need_win_max_col,
                                                              need_win_min_col))) {
    LOG_WARN("failed to add max min seq window func to select list", K(ret));
  } else if (OB_FAIL(add_normal_column_to_select_list(table, source_table, select_items))) {
    LOG_WARN("failed to add normal column to select list", K(ret));
  }
  return ret;
}

int ObMVPrinter::add_dml_factor_to_select_list(ObRawExpr *old_new_col,
                                               ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *equal_expr = NULL;
  SelectItem *sel_item = NULL;
  if (OB_ISNULL(sel_item = select_items.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate select item from array error", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, old_new_col, exprs_.str_n_, equal_expr))) {
    LOG_WARN("failed to build mul expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(ctx_.expr_factory_, equal_expr, exprs_.int_one_, exprs_.int_neg_one_, sel_item->expr_))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else {
    sel_item->is_real_alias_ = true;
    sel_item->alias_name_ = DML_FACTOR_COL_NAME;
  }
  return ret;
}

int ObMVPrinter::add_normal_column_to_select_list(const TableItem &table,
                                                  const ObString &col_name,
                                                  ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  ObRawExpr *equal_expr = NULL;
  SelectItem *sel_item = NULL;
  if (OB_ISNULL(sel_item = select_items.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate select item from array error", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), col_name, table.table_id_, sel_item->expr_))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  } else {
    sel_item->is_real_alias_ = true;
    sel_item->alias_name_ = col_name;
  }
  return ret;
}

int ObMVPrinter::add_normal_column_to_select_list(const TableItem &table,
                                                  const TableItem &source_table,
                                                  ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> column_items;
  if (OB_FAIL(mv_def_stmt_.get_column_items(source_table.table_id_, column_items))) {
    LOG_WARN("failed to get table_id columns items", K(ret));
  } else {
    SelectItem *sel_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      const ObString &column_name = column_items.at(i).column_name_;
      if (OB_ISNULL(sel_item = select_items.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Allocate select item from array error", K(ret));
      } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), column_name, table.table_id_, sel_item->expr_))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else {
        sel_item->is_real_alias_ = true;
        sel_item->alias_name_ = column_name;
      }
    }
  }
  return ret;
}

int ObMVPrinter::add_max_min_seq_window_to_select_list(const TableItem &table,
                                                       const TableItem &source_table,
                                                       ObRawExpr *sequence_expr,
                                                       ObIArray<SelectItem> &select_items,
                                                       bool need_win_max_col,
                                                       bool need_win_min_col)
{
  int ret = OB_SUCCESS;
  SelectItem *sel_item = NULL;
  ObRawExpr *win_max_expr = NULL;
  ObRawExpr *win_min_expr = NULL;
  if (!need_win_max_col && !need_win_min_col) {
    /* do nothing */
  } else if (OB_FAIL(gen_max_min_seq_window_func_exprs(table, source_table, sequence_expr, win_max_expr, win_min_expr))) {
    LOG_WARN("failed to gen max min seq window func exprs", K(ret));
  }

  if (OB_SUCC(ret) && need_win_max_col) {
    if (OB_ISNULL(sel_item = select_items.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate select item from array error", K(ret));
    } else {
      sel_item->expr_ = win_max_expr;
      sel_item->is_real_alias_ = true;
      sel_item->alias_name_ = WIN_MAX_SEQ_COL_NAME;
    }
  }

  if (OB_SUCC(ret) && need_win_min_col) {
    if (OB_ISNULL(sel_item = select_items.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate select item from array error", K(ret));
    } else {
      sel_item->expr_ = win_min_expr;
      sel_item->is_real_alias_ = true;
      sel_item->alias_name_ = WIN_MIN_SEQ_COL_NAME;
    }
  }
  return ret;
}

int ObMVPrinter::gen_max_min_seq_window_func_exprs(const TableItem &table,
                                                   const TableItem &source_table,
                                                   ObRawExpr *sequence_expr,
                                                   ObRawExpr *&win_max_expr,
                                                   ObRawExpr *&win_min_expr)
{
  int ret = OB_SUCCESS;
  win_max_expr = NULL;
  win_min_expr = NULL;
  ObSEArray<uint64_t, 4> unique_col_ids;
  ObSEArray<ObRawExpr*, 4> unique_keys;
  const ObTableSchema *source_data_schema = NULL;
  const ObColumnSchemaV2 *rowkey_column = NULL;
  ObWinFunRawExpr *win_max = NULL;
  ObWinFunRawExpr *win_min = NULL;
  ObAggFunRawExpr *aggr_max = NULL;
  ObAggFunRawExpr *aggr_min = NULL;
  ObRawExpr *col_expr = NULL;
  ObQueryCtx *query_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_ISNULL(query_ctx = ctx_.stmt_factory_.get_query_ctx())
      || OB_ISNULL(schema_guard = query_ctx->sql_schema_guard_.get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_ctx), K(schema_guard));
  } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(source_table.ref_id_, source_data_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(source_data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(source_data_schema));
  } else if (!source_data_schema->is_table_with_hidden_pk_column()) {
    // For compatibility reasons (the logic pk of heap table may not exist in mlog),
    // only index organized tables with primary key use logic pk, heap tables and
    // index organized tables without primary key will use hidden pk.
    if (OB_FAIL(source_data_schema->get_logic_pk_column_ids(schema_guard, unique_col_ids))) {
      LOG_WARN("failed to get rowkey column ids", KR(ret));
    }
  } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), HEAP_TABLE_ROWKEY_COL_NAME, table.table_id_, col_expr))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  } else if (OB_FAIL(unique_keys.push_back(col_expr))) {
    LOG_WARN("failed to pushback", K(ret));
  } else if (source_data_schema->get_partition_key_info().is_valid() &&
             OB_FAIL(source_data_schema->get_partition_key_info().get_column_ids(unique_col_ids))) {
    LOG_WARN("failed to add part column ids", K(ret));
  } else if (source_data_schema->get_subpartition_key_info().is_valid() &&
             OB_FAIL(source_data_schema->get_subpartition_key_info().get_column_ids(unique_col_ids))) {
    LOG_WARN("failed to add subpart column ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < unique_col_ids.count(); ++i) {
    if (OB_ISNULL(rowkey_column = source_data_schema->get_column_schema(unique_col_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(rowkey_column));
    } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), rowkey_column->get_column_name(), table.table_id_, col_expr))) {
      LOG_WARN("failed to create simple column expr", K(ret));
    } else if (OB_FAIL(unique_keys.push_back(col_expr))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, win_max))
              || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, win_min))
              || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_MAX, aggr_max))
              || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_MIN, aggr_min))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(sequence_expr) || OB_ISNULL(win_max) || OB_ISNULL(win_min)
             || OB_ISNULL(aggr_max) || OB_ISNULL(aggr_min)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(sequence_expr), K(win_max), K(win_min), K(aggr_max), K(aggr_min));
  } else if (OB_FAIL(aggr_max->add_real_param_expr(sequence_expr))
              || OB_FAIL(aggr_min->add_real_param_expr(sequence_expr))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(win_max->set_partition_exprs(unique_keys))
              || OB_FAIL(win_min->set_partition_exprs(unique_keys))) {
    LOG_WARN("fail to set partition exprs", K(ret));
  } else {
    win_max->set_func_type(T_FUN_MAX);
    win_min->set_func_type(T_FUN_MIN);
    win_max->set_agg_expr(aggr_max);
    win_min->set_agg_expr(aggr_min);
    win_max_expr = win_max;
    win_min_expr = win_min;
  }
  return ret;
}

int ObMVPrinter::create_simple_column_expr(const ObString &table_name,
                                           const ObString &column_name,
                                           const uint64_t table_id,
                                           ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObColumnRefRawExpr *column_ref = NULL;
  if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, column_ref))) {
    LOG_WARN("failed to create a new column ref expr", K(ret));
  } else if (OB_ISNULL(column_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_column_ref should not be null", K(ret));
  } else {
    column_ref->set_table_name(table_name);
    column_ref->set_column_name(column_name);
    column_ref->set_ref_id(table_id, OB_INVALID_ID);
    expr = column_ref;
  }
  return ret;
}

int ObMVPrinter::create_simple_table_item(ObDMLStmt *stmt,
                                          const ObString &table_name,
                                          TableItem *&table_item,
                                          ObSelectStmt *view_stmt /* default null */,
                                          const bool add_to_from /* default true */)
{
  int ret = OB_SUCCESS;
  table_item = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (OB_ISNULL(table_item = stmt->create_table_item(ctx_.alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
    LOG_WARN("add table item failed", K(ret));
  } else {
    table_item->table_name_ = table_name;
    table_item->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
    table_item->type_ = NULL == view_stmt ? TableItem::BASE_TABLE : TableItem::GENERATED_TABLE;
    table_item->ref_query_ = view_stmt;
    if (OB_SUCC(ret) && add_to_from && OB_FAIL(stmt->add_from_item(table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::create_joined_table_item(ObDMLStmt *stmt,
                                          const ObJoinType joined_type,
                                          const TableItem &left_table,
                                          const TableItem &right_table,
                                          const bool is_top,
                                          JoinedTable *&joined_table)
{
  int ret = OB_SUCCESS;
  joined_table = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (OB_ISNULL(ptr = ctx_.alloc_.alloc(sizeof(JoinedTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("falied to allocate memory", K(ret), K(ptr));
  } else {
    joined_table = new (ptr) JoinedTable();
    joined_table->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
    joined_table->type_ = TableItem::JOINED_TABLE;
    joined_table->joined_type_ = joined_type;
    joined_table->left_table_ = const_cast<TableItem*>(&left_table);
    joined_table->right_table_ = const_cast<TableItem*>(&right_table);
    if (OB_FAIL(ret) || !is_top) {
    } else if (OB_FAIL(stmt->add_joined_table(joined_table))) {
      LOG_WARN("failed to add joined table", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::init()
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  ObQueryCtx *query_ctx = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mv printer is inited twice", K(ret));
  } else if (OB_ISNULL(query_ctx = ctx_.stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_ctx));
  } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_database_schema(mv_schema_.get_database_id(), db_schema))) {
    LOG_WARN("fail to get data base schema", K(ret), K(mv_schema_.get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(db_schema));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(ctx_.expr_factory_, ObIntType, 0, exprs_.int_zero_))
             || OB_FAIL(ObRawExprUtils::build_const_int_expr(ctx_.expr_factory_, ObIntType, 1, exprs_.int_one_))
             || OB_FAIL(ObRawExprUtils::build_const_int_expr(ctx_.expr_factory_, ObIntType, -1, exprs_.int_neg_one_))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(ctx_.expr_factory_, ObVarcharType, ObString("N"), cs_type, exprs_.str_n_))
             || OB_FAIL(ObRawExprUtils::build_const_string_expr(ctx_.expr_factory_, ObVarcharType, ObString("O"), cs_type, exprs_.str_o_))) {
    LOG_WARN("fail to build const string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(ctx_.expr_factory_, exprs_.null_expr_))) {
    LOG_WARN("failed to create const null expr", K(ret));
  } else if (NULL == ctx_.refresh_info_) { // for real-time view
    ObSysFunRawExpr *sys_last_refresh_scn = NULL;
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_LAST_REFRESH_SCN, sys_last_refresh_scn))) {
      LOG_WARN("failed to create last_refresh_scn sys expr", K(ret));
    } else {
      sys_last_refresh_scn->set_mview_id(mv_schema_.get_table_id());
      sys_last_refresh_scn->set_func_name(ObString::make_string(N_SYS_LAST_REFRESH_SCN));
      exprs_.last_refresh_scn_ = sys_last_refresh_scn;
    }
  } else {
    ObConstRawExpr *const_last_refresh_scn = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(ctx_.expr_factory_, ObUInt64Type, ctx_.refresh_info_->last_refresh_scn_.get_val_for_sql(), const_last_refresh_scn))
             || OB_FAIL(ObRawExprUtils::build_const_uint_expr(ctx_.expr_factory_, ObUInt64Type, ctx_.refresh_info_->refresh_scn_.get_val_for_sql(), exprs_.refresh_scn_))) {
      LOG_WARN("failed to build const uint expr", K(ret));
    } else {
      exprs_.last_refresh_scn_ = const_last_refresh_scn;
    }
  }

  if (OB_SUCC(ret)) {
    mv_db_name_ = db_schema->get_database_name_str();
    inited_ = true;
  }
  return ret;
}


// rowkeys must exist on source_table.
//  generate: not exists (select 1 from mlog$_t1 t1 where mv.t1_pk = t1.pk and ora_rowscn > last_refresh_scn(mv_id))
int ObMVPrinter::gen_exists_cond_for_table(const TableItem *source_table,
                                           const TableItem *outer_table,
                                           const bool is_exists,
                                           const bool use_orig_sel_alias,
                                           ObRawExpr *&exists_expr)
{
  int ret = OB_SUCCESS;
  exists_expr = NULL;
  ObOpRawExpr *exists_op_expr = NULL;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  const ObTableSchema *mlog_schema = NULL;
  ObSelectStmt *subquery = NULL;
  TableItem *delta_src_table = NULL;
  SelectItem sel_item;
  sel_item.expr_ = exprs_.int_one_;
  if (OB_ISNULL(outer_table) || OB_ISNULL(source_table) || OB_ISNULL(ctx_.stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(outer_table), K(source_table), K(ctx_.stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(get_mlog_table_schema(source_table, mlog_schema))) {
    LOG_WARN("failed to get mlog schema", K(ret), KPC(source_table));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(is_exists ? T_OP_EXISTS : T_OP_NOT_EXISTS, exists_op_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(exists_op_expr)
             || OB_UNLIKELY(NULL == mlog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr), K(exists_op_expr), K(mlog_schema));
  } else if (OB_FAIL(exists_op_expr->set_param_expr(query_ref_expr))) {
    LOG_WARN("failed to set param expr", K(ret));
  } else if (OB_FAIL(create_simple_stmt(subquery))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(subquery, mlog_schema->get_table_name_str(),
                                              delta_src_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(subquery->get_select_items().push_back(sel_item))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else if (OB_FAIL(gen_delta_table_view_conds(*delta_src_table, subquery->get_condition_exprs()))) {
    LOG_WARN("failed to generate delta table view conds", K(ret));
  } else if (OB_FAIL(gen_rowkey_join_conds_for_table(*source_table,
                                                     *delta_src_table,
                                                     *outer_table,
                                                     use_orig_sel_alias,
                                                     subquery->get_condition_exprs()))) {
    LOG_WARN("failed to generate rowkey join conds for table", K(ret));
  } else {
    exists_expr = exists_op_expr;
    delta_src_table->database_name_ = source_table->database_name_;
    query_ref_expr->set_ref_stmt(subquery);
  }
  return ret;
}

int ObMVPrinter::gen_exists_cond_for_mview(const TableItem &source_table,
                                           const TableItem &outer_table,
                                           ObRawExpr *&exists_expr)
{
  int ret = OB_SUCCESS;
  exists_expr = NULL;
  ObOpRawExpr *exists_op_expr = NULL;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  ObSelectStmt *subquery = NULL;
  TableItem *mv_table = NULL;
  SelectItem sel_item;
  sel_item.expr_ = exprs_.int_one_;
  if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT_EXISTS, exists_op_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(exists_op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(query_ref_expr), K(exists_op_expr));
  } else if (OB_FAIL(exists_op_expr->set_param_expr(query_ref_expr))) {
    LOG_WARN("failed to set param expr", K(ret));
  } else if (OB_FAIL(create_simple_stmt(subquery))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(subquery, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(subquery->get_select_items().push_back(sel_item))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else if (OB_FAIL(gen_rowkey_join_conds_for_table(source_table, outer_table,
                                                     *mv_table, true, subquery->get_condition_exprs()))) {
    LOG_WARN("failed to generate rowkey join conds for table", K(ret));
  } else {
    exists_expr = exists_op_expr;
    mv_table->database_name_ = mv_db_name_;
    mv_table->flashback_query_expr_ = exprs_.last_refresh_scn_;
    mv_table->flashback_query_type_ = TableItem::USING_SCN;
    query_ref_expr->set_ref_stmt(subquery);
  }
  return ret;
}

int ObMVPrinter::gen_rowkey_join_conds_for_table(const TableItem &origin_table,
                                                 const TableItem &left_table,
                                                 const TableItem &right_table,
                                                 const bool right_use_orig_sel_alias,
                                                 ObIArray<ObRawExpr*> &all_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> rowkey_column_ids;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(ctx_.stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(ctx_.stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(origin_table.ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(get_table_logic_pk_ids(table_schema, rowkey_column_ids))) {
    LOG_WARN("failed to get table logic pk", K(ret), KPC(table_schema));
  } else {
    const ObColumnSchemaV2 *rowkey_column = NULL;
    ObRawExpr *l_col = NULL;
    ObRawExpr *r_col = NULL;
    ObRawExpr *join_cond = NULL;
    const ObString *orig_sel_alias = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); ++i) {
      if (OB_ISNULL(rowkey_column = table_schema->get_column_schema(rowkey_column_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(rowkey_column));
      } else if (right_use_orig_sel_alias &&
                 OB_FAIL(get_column_name_from_origin_select_items(origin_table.table_id_,
                                                                  rowkey_column_ids.at(i),
                                                                  orig_sel_alias))) {
        LOG_WARN("failed to get column_name from origin select_items", K(ret));
      } else if (OB_FAIL(create_simple_column_expr(left_table.get_object_name(), rowkey_column->get_column_name_str(), left_table.table_id_, l_col))
                 || OB_FAIL(create_simple_column_expr(right_table.get_object_name(), right_use_orig_sel_alias ? *orig_sel_alias : rowkey_column->get_column_name_str(), right_table.table_id_, r_col))) {
        LOG_WARN("failed to build column expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, l_col, r_col, join_cond))) {
        LOG_WARN("failed to build equal expr", K(ret));
      } else if (OB_FAIL(all_conds.push_back(join_cond))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::get_column_name_from_origin_select_items(const uint64_t table_id,
                                                          const uint64_t column_id,
                                                          const ObString *&col_name)
{
  int ret = OB_SUCCESS;
  col_name = NULL;
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  ObColumnRefRawExpr *col_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == col_name && i < select_items.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(select_items.at(i)));
    } else if (NULL == (col_expr = dynamic_cast<ObColumnRefRawExpr*>(select_items.at(i).expr_))) {
      /* do nothing */
    } else if (col_expr->get_table_id() != table_id || col_expr->get_column_id() != column_id) {
      /* do nothing */
    } else {
      col_name = &select_items.at(i).alias_name_;
      if (col_name->empty()) {
        col_name = &col_expr->get_column_name();
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(col_name) || OB_UNLIKELY(col_name->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column name", K(ret), KPC(col_name));
  }
  return ret;
}


void ObMVPrinter::set_info_for_simple_table_item(TableItem &table, const TableItem &source_table)
{
  table.alias_name_ = source_table.alias_name_;
  table.synonym_name_ = source_table.synonym_name_;
  table.database_name_ = source_table.database_name_;
  table.synonym_db_name_ = source_table.synonym_db_name_;
  if (!ctx_.for_rt_expand()) {
    table.flashback_query_expr_ = exprs_.refresh_scn_;
    table.flashback_query_type_ = TableItem::USING_SCN;
  }
}

int ObMVPrinter::init_expr_copier_for_stmt(ObSelectStmt &target_stmt, ObRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
  const ObIArray<TableItem*> &target_table_items = target_stmt.get_table_items();
  ObSEArray<ColumnItem, 8> column_items;
  const TableItem *orig_table = NULL;
  const TableItem *target_table = NULL;
  ObRawExpr *new_col = NULL;
  if (OB_UNLIKELY(orig_table_items.count() != target_table_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table items", K(ret), K(orig_table_items.count()), K(target_table_items.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
    column_items.reuse();
    if (OB_ISNULL(orig_table = orig_table_items.at(i))
        || OB_ISNULL(target_table = target_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(orig_table), K(target_table));
    } else if (OB_FAIL(mv_def_stmt_.get_column_items(orig_table->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    }

    for (int64_t j = 0; OB_SUCC(ret) && j < column_items.count(); ++j) {
      if (OB_FAIL(create_simple_column_expr(target_table->get_table_name(),
                                            column_items.at(j).column_name_,
                                            target_table->table_id_, new_col))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(column_items.at(j).expr_, new_col))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
  }
  return ret;
}

// generate joined table and from item
int ObMVPrinter::construct_from_items_for_simple_mjv_delta_data(ObRawExprCopier &copier,
                                                                ObSelectStmt &target_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(target_stmt.deep_copy_join_tables(ctx_.alloc_, copier, mv_def_stmt_))) {
    LOG_WARN("failed to deep copy join tables", K(ret));
  } else if (OB_FAIL(target_stmt.get_from_items().assign(mv_def_stmt_.get_from_items()))) {
    LOG_WARN("failed to assign from items", K(ret));
  } else {
    // for non joined table, adjust table id in from item
    ObIArray<FromItem> &from_items = target_stmt.get_from_items();
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
      if (from_items.at(i).is_joined_) {
        /* do nothing */
      } else if (OB_FAIL(mv_def_stmt_.get_table_item_idx(from_items.at(i).table_id_, idx))) {
        LOG_WARN("failed to get table item", K(ret));
      } else if (OB_UNLIKELY(idx < 0 || idx >= target_stmt.get_table_size())
                  || OB_ISNULL(target_stmt.get_table_item(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", K(ret), K(idx), K(target_stmt.get_table_size()));
      } else {
        from_items.at(i).table_id_ = target_stmt.get_table_item(idx)->table_id_;
      }
    }
  }
  return ret;
}


int ObMVPrinter::assign_simple_sel_stmt(ObSelectStmt &target_stmt, ObSelectStmt &source_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(target_stmt.get_joined_tables().assign(source_stmt.get_joined_tables()))
      || OB_FAIL(target_stmt.get_table_items().assign(source_stmt.get_table_items()))
      || OB_FAIL(target_stmt.get_from_items().assign(source_stmt.get_from_items()))
      || OB_FAIL(target_stmt.get_select_items().assign(source_stmt.get_select_items()))
      || OB_FAIL(target_stmt.get_condition_exprs().assign(source_stmt.get_condition_exprs()))
      || OB_FAIL(target_stmt.get_order_items().assign(source_stmt.get_order_items()))) {
    LOG_WARN("failed to assign structure", K(ret));
  }
  return ret;
}

/* t.`OLD_NEW$$` = 'N' or t.`OLD_NEW$$` is NULL */
int ObMVPrinter::append_old_new_col_filter(const TableItem &table,
                                           ObIArray<ObRawExpr*>& conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *col = NULL;
  ObRawExpr *equal_filter = NULL;
  ObRawExpr *is_null_filter = NULL;
  ObRawExpr *filter = NULL;
  if (OB_FAIL(create_simple_column_expr(table.get_table_name(), OLD_NEW_COL_NAME, table.table_id_, col))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, col, exprs_.str_n_, equal_filter))) {
    LOG_WARN("failed to build equal expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(ctx_.expr_factory_, col, false, is_null_filter))) {
    LOG_WARN("failed to build is null expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_OR, equal_filter, is_null_filter, filter))) {
    LOG_WARN("failed to build or expr", K(ret));
  } else if (OB_FAIL(conds.push_back(filter))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObMVPrinter::get_table_logic_pk_ids(const ObTableSchema *table_schema,
                                        ObIArray<uint64_t> &logic_pk_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = NULL;
  bool has_pk = false;
  if (OB_ISNULL(table_schema) || OB_ISNULL(ctx_.stmt_factory_.get_query_ctx())
      || OB_ISNULL(schema_guard = ctx_.stmt_factory_.get_query_ctx()->sql_schema_guard_.get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(ctx_.stmt_factory_.get_query_ctx()), K(schema_guard));
  } else if (OB_FAIL(table_schema->is_table_with_logic_pk(*schema_guard, has_pk))) {
    LOG_WARN("failed to check table with logic pk", K(ret), KPC(table_schema));
  } else if (OB_UNLIKELY(!has_pk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table schema without logic pk", K(ret), KPC(table_schema));
  } else if (OB_FAIL(table_schema->get_logic_pk_column_ids(schema_guard, logic_pk_ids))) {
    LOG_WARN("failed to get table logic pk", K(ret), KPC(table_schema));
  } else if (OB_UNLIKELY(logic_pk_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty array", K(ret));
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase

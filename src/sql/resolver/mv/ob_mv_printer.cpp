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

int ObMVPrinter::print_mv_operators(const share::schema::ObTableSchema &mv_schema,
                                    const share::schema::ObTableSchema &mv_container_schema,
                                    const ObSelectStmt &view_stmt,
                                    const bool for_rt_expand,
                                    const share::SCN &last_refresh_scn,
                                    const share::SCN &refresh_scn,
                                    const MajorRefreshInfo *major_refresh_info,
                                    ObIAllocator &alloc,
                                    ObIAllocator &str_alloc,
                                    ObSchemaGetterGuard *schema_guard,
                                    ObStmtFactory &stmt_factory,
                                    ObRawExprFactory &expr_factory,
                                    ObSQLSessionInfo *session_info,
                                    ObIArray<ObString> &operators,
                                    ObMVRefreshableType &refreshable_type)
{
  int ret = OB_SUCCESS;
  operators.reuse();
  refreshable_type = OB_MV_REFRESH_INVALID;
  ObMVChecker checker(view_stmt, expr_factory, session_info, mv_container_schema);
  ObMVPrinter printer(alloc, mv_schema, checker, for_rt_expand, stmt_factory, expr_factory);
  ObSEArray<ObDMLStmt*, 4> dml_stmts;
  if (OB_ISNULL(view_stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(view_stmt.get_query_ctx()));
  } else if (OB_FAIL(checker.check_mv_refresh_type())) {
    LOG_WARN("failed to check mv refresh type", K(ret));
  } else if (OB_MV_COMPLETE_REFRESH >= (refreshable_type = checker.get_refersh_type())) {
    LOG_TRACE("mv not support fast refresh", K(refreshable_type), K(mv_schema.get_table_name()));
  } else if (OB_FAIL(printer.init(last_refresh_scn, refresh_scn, major_refresh_info))) {
    LOG_WARN("failed to init mv printer", K(ret));
  } else if (OB_FAIL(printer.gen_mv_operator_stmts(dml_stmts))) {
    LOG_WARN("failed to print mv operators", K(ret));
  } else if (OB_UNLIKELY(dml_stmts.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty array", K(ret), K(dml_stmts.empty()));
  } else if (OB_FAIL(operators.prepare_allocate(dml_stmts.count()))) {
    LOG_WARN("failed to prepare allocate ObSqlString arrays", K(ret), K(dml_stmts.count()));
  } else {
    ObObjPrintParams obj_print_params(view_stmt.get_query_ctx()->get_timezone_info());
    obj_print_params.print_origin_stmt_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_stmts.count(); ++i) {
      if (OB_FAIL(ObSQLUtils::reconstruct_sql(str_alloc, dml_stmts.at(i), operators.at(i), schema_guard, obj_print_params))) {
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
  } else if (!for_rt_expand_) {
    if (OB_FAIL(gen_refresh_dmls_for_mv(dml_stmts))) {
      LOG_WARN("failed to gen refresh dmls for mv", K(ret));
    }
  } else if (OB_FAIL(gen_real_time_view_for_mv(sel_stmt))) {
    LOG_WARN("failed to gen real time view for mv", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(sel_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_refresh_dmls_for_mv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  const bool mysql_mode = lib::is_mysql_mode();
  switch (mv_checker_.get_refersh_type()) {
    case OB_MV_FAST_REFRESH_SIMPLE_MAV: {
      ObMergeStmt *merge_stmt = NULL;
      if (mysql_mode) {
        if (OB_FAIL(gen_update_insert_delete_for_simple_mav(dml_stmts))) {
          LOG_WARN("failed to gen update insert delete for simple mav", K(ret));
        }
      } else if (OB_FAIL(gen_merge_for_simple_mav(merge_stmt))) {
        LOG_WARN("failed to gen merge into for simple mav", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(merge_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_SIMPLE_MJV: {
      if (OB_FAIL(gen_delete_insert_for_simple_mjv(dml_stmts))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to gen refresh dmls for simple mjv", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV: {
      if (mysql_mode && OB_FAIL(gen_update_insert_delete_for_simple_join_mav(dml_stmts))) {
        LOG_WARN("failed to gen update insert delete for simple mav", K(ret));
      } else if (!mysql_mode && OB_FAIL(gen_merge_for_simple_join_mav(dml_stmts))) {
        LOG_WARN("failed to gen merge into for simple mav", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV: {
      if (OB_FAIL(gen_refresh_select_for_major_refresh_mjv(dml_stmts))) {
        LOG_WARN("fail to gen refresh select for major refresh mjv", K(ret));
      }
      break;
    }
    default:  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected refresh type", K(ret), K(mv_checker_.get_refersh_type()));
      break;
    }
  }
  return ret;
}

int ObMVPrinter::gen_real_time_view_for_mv(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  switch (mv_checker_.get_refersh_type()) {
    case OB_MV_FAST_REFRESH_SIMPLE_MAV:
    case OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV: {
      if (OB_FAIL(gen_real_time_view_for_mav(sel_stmt))) {
        LOG_WARN("failed to gen real time view for mav", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_SIMPLE_MJV: {
      if (OB_FAIL(gen_real_time_view_for_simple_mjv(sel_stmt))) {
        LOG_WARN("fail to gen real time view for simple mjv", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV: {
      if (OB_FAIL(gen_real_time_view_for_major_refresh_mjv(sel_stmt))) {
        LOG_WARN("fail to gen real time view for simple mjv", K(ret));
      }
      break;
    }
    default:  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mv type for real-time mview", K(ret), K(mv_checker_.get_refersh_type()));
      break;
    }
  }
  return ret;
}

int ObMVPrinter::gen_delete_insert_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  if (OB_FAIL(gen_delete_for_simple_mjv(dml_stmts))) {
    LOG_WARN("failed to gen delete operators for simple mjv", K(ret));
  } else if (OB_FAIL(gen_insert_into_select_for_simple_mjv(dml_stmts))) {
    LOG_WARN("failed to gen delete operators for simple mjv", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_real_time_view_for_simple_mjv(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  ObSelectStmt *access_mv_stmt = NULL;
  ObSEArray<ObSelectStmt*, 8> access_delta_stmts;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_access_mv_data_for_simple_mjv(access_mv_stmt))) {
    LOG_WARN("failed to generate access mv data for simple mjv", K(ret));
  } else if (OB_FAIL(gen_access_delta_data_for_simple_mjv(access_delta_stmts))) {
    LOG_WARN("failed to generate access delta data for simple mjv", K(ret));
  } else if (OB_FAIL(sel_stmt->get_set_query().push_back(access_mv_stmt)
             || OB_FAIL(append(sel_stmt->get_set_query(), access_delta_stmts)))) {
    LOG_WARN("failed to set set query", K(ret));
  } else {
    sel_stmt->assign_set_all();
    sel_stmt->assign_set_op(ObSelectStmt::UNION);
  }
  return ret;
}

// generate update/insert/delete dml to fast refresh simple mav in mysql mode
int ObMVPrinter::gen_update_insert_delete_for_simple_mav(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObUpdateStmt *update_stmt = NULL;
  ObInsertStmt *insert_stmt = NULL;
  ObDeleteStmt *delete_stmt = NULL;
  ObSelectStmt *delta_mv_stmt = NULL;
  ObSEArray<ObRawExpr*, 16> values;
  if (OB_FAIL(gen_simple_mav_delta_mv_view(delta_mv_stmt))) {
    LOG_WARN("failed gen simple source stmt", K(ret));
  } else if (OB_FAIL(gen_insert_for_mav(delta_mv_stmt, values, insert_stmt))) {
    LOG_WARN("failed to gen insert for mav", K(ret));
  } else if (OB_FAIL(gen_update_for_mav(delta_mv_stmt, insert_stmt->get_values_desc(),
                                        values, update_stmt))) {
    LOG_WARN("failed to gen update for mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(update_stmt))  // pushback and execute in this ordering
             || OB_FAIL(dml_stmts.push_back(insert_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (mv_checker_.get_stmt().is_scala_group_by()) {
    /* no need delete for scalar group by */
  } else if (OB_FAIL(gen_delete_for_mav(insert_stmt->get_values_desc(), delete_stmt))) {
    LOG_WARN("failed gen delete for mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(delete_stmt))) { // pushback and execute in this ordering
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

/*
  insert into mv
  select ... from delta_mv
  where not exists (select 1 from mv where delta_mv.c1 <=> mv.c1);
*/
int ObMVPrinter::gen_insert_for_mav(ObSelectStmt *delta_mv_stmt,
                                    ObIArray<ObRawExpr*> &values,
                                    ObInsertStmt *&insert_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  ObSelectStmt *sel_stmt = NULL;
  ObRawExpr *filter_expr = NULL;
  if (OB_FAIL(create_simple_stmt(sel_stmt))
      || OB_FAIL(create_simple_stmt(insert_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, DELTA_BASIC_MAV_VIEW_NAME, source_table, delta_mv_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_simple_table_item(insert_stmt, mv_schema_.get_table_name(), target_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_insert_values_and_desc(target_table,
                                                source_table,
                                                insert_stmt->get_values_desc(),
                                                values))) {
    LOG_WARN("failed to gen insert values and desc", K(ret), K(*target_table), K(*source_table));
  } else if (OB_FAIL(create_simple_table_item(insert_stmt, DELTA_MAV_VIEW_NAME, source_table, sel_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_select_for_insert_subquery(values, sel_stmt->get_select_items()))) {
    LOG_WARN("failed to gen select for insert subquery ", K(ret));
  } else if (OB_FAIL(gen_exists_cond_for_insert(values, sel_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to gen conds for insert subquery", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
  }
  return ret;
}

int ObMVPrinter::gen_select_for_insert_subquery(const ObIArray<ObRawExpr*> &values,
                                                ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  SelectItem sel_item;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
    sel_item.expr_ = values.at(i);
    sel_item.is_real_alias_ = true;
    sel_item.alias_name_ = orig_select_items.at(i).alias_name_;
    if (OB_FAIL(select_items.push_back(sel_item))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  return ret;
}

//  generate: not exists (select 1 from mv where delta_mv.c1 <=> mv.c1)
int ObMVPrinter::gen_exists_cond_for_insert(const ObIArray<ObRawExpr*> &values,
                                            ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  ObOpRawExpr *not_exists_expr = NULL;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  ObSelectStmt *subquery = NULL;
  TableItem *mv_table = NULL;
  SelectItem sel_item;
  sel_item.expr_ = exprs_.int_one_;
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  if (OB_UNLIKELY(values.count() != select_items.count())) {
    LOG_WARN("unexpected params", K(ret), K(values.count()), K(select_items.count()));
  } else if (OB_FAIL(create_simple_stmt(subquery))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(expr_factory_.create_raw_expr(T_OP_NOT_EXISTS, not_exists_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(not_exists_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr), K(not_exists_expr));
  } else if (OB_FAIL(not_exists_expr->add_param_expr(query_ref_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FAIL(conds.push_back(not_exists_expr))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else if (OB_FAIL(create_simple_table_item(subquery, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(subquery->get_select_items().push_back(sel_item))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    query_ref_expr->set_ref_stmt(subquery);
    ObRawExpr *expr = NULL;
    ObRawExpr *match_cond = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (!ObOptimizerUtil::find_item(group_by_exprs, select_items.at(i).expr_)) {
        /* not group by exprs, do nothing */
      } else if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(), select_items.at(i).alias_name_, mv_table->table_id_, expr))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_NSEQ, values.at(i), expr, match_cond))) {
        LOG_WARN("failed to build null safe equal expr", K(ret));
      } else if (OB_FAIL(subquery->get_condition_exprs().push_back(match_cond))) {
        LOG_WARN("failed to push back null safe equal expr", K(ret));
      }
    }
  }
  return ret;
}

/*
  update mv, delta_mv
  set mv.cnt = mv.cnt + delta_mv.d_cnt,
      ...
  where delta_mv.c1 <=> mv.c1);
*/
int ObMVPrinter::gen_update_for_mav(ObSelectStmt *delta_mv_stmt,
                                    const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                                    const ObIArray<ObRawExpr*> &values,
                                    ObUpdateStmt *&update_stmt)
{
  int ret = OB_SUCCESS;
  update_stmt = NULL;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  void *ptr = NULL;
  ObUpdateTableInfo *table_info = NULL;
  if (OB_FAIL(create_simple_stmt(update_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL((ptr = alloc_.alloc(sizeof(ObUpdateTableInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table info", K(ret));
  } else if (OB_FALSE_IT(table_info = new(ptr)ObUpdateTableInfo())) {
  } else if (OB_FAIL(update_stmt->get_update_table_info().push_back(table_info))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(create_simple_table_item(update_stmt, mv_schema_.get_table_name(), target_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_simple_table_item(update_stmt, DELTA_BASIC_MAV_VIEW_NAME, source_table, delta_mv_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_update_assignments(mv_columns, values, source_table, table_info->assignments_, true))) {
    LOG_WARN("failed gen update assignments", K(ret));
  } else if (OB_FAIL(gen_update_conds(mv_columns, values, update_stmt->get_condition_exprs()))) {
    LOG_WARN("failed gen update conds", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
  }
  return ret;
}

int ObMVPrinter::gen_update_conds(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                                  const ObIArray<ObRawExpr*> &values,
                                  ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
  ObRawExpr *cond = NULL;
  if (OB_UNLIKELY(mv_columns.count() != select_items.count() || mv_columns.count() != values.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_columns.count()), K(values.count()), K(select_items.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (!ObOptimizerUtil::find_item(group_by_exprs, select_items.at(i).expr_)) {
      /* do nothing */
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_NSEQ, mv_columns.at(i), values.at(i), cond))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
    } else if (OB_FAIL(conds.push_back(cond))) {
      LOG_WARN("failed to push back null safe equal expr", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::gen_delete_for_mav(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                                    ObDeleteStmt *&delete_stmt)
{
  int ret = OB_SUCCESS;
  delete_stmt = NULL;
  TableItem *target_table = NULL;
  if (OB_FAIL(create_simple_stmt(delete_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(delete_stmt, mv_schema_.get_table_name(), target_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_delete_conds(mv_columns, delete_stmt->get_condition_exprs()))) {
    LOG_WARN("failed gen update conds", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
  }
  return ret;
}

int ObMVPrinter::gen_delete_conds(const ObIArray<ObColumnRefRawExpr*> &mv_columns,
                                  ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
  ObRawExpr *expr = NULL;
  ObRawExpr *cond = NULL;
  if (OB_UNLIKELY(mv_columns.count() != select_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_columns.count()), K(select_items.count()));
  }
  for (int64_t i = 0; NULL == cond && OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (T_FUN_COUNT != expr->get_expr_type() ||
               0 != static_cast<ObAggFunRawExpr*>(expr)->get_real_param_count()) {
      /* do nothing */
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, mv_columns.at(i), exprs_.int_zero_, cond))) {
      LOG_WARN("failed to build equal expr", K(ret));
    } else if (OB_FAIL(conds.push_back(cond))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

/*
for real-time mv:
    select t1.c1 c1, count(*) cnt, count(t2.c2) cnt_c2, sum(t2.c2) sum_c2, avg(t2.c2) avg_c2
    from t1, t2
    where t1.c1 = t2.c1
    group by t1.c1;
generate real time view as:
    select c1,
           cnt,
           nvl(cnt_c2, 0) cnt_c2,
           case when cnt_c2 <> 0 then sum_c2 else null end sum_c2
           (case when cnt_c2 <> 0 then sum_c2 else null end)/nvl(cnt_c2, 0) avg_c2
    from inner_rt_view
    where cnt > 0;
*/
int ObMVPrinter::gen_real_time_view_for_mav(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *view_table = NULL;
  ObSelectStmt *view_stmt = NULL;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_inner_real_time_view_for_mav(view_stmt))) {
    LOG_WARN("failed to generate inner real time view for simple mav", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, INNER_RT_MV_VIEW_NAME, view_table, view_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_select_items_for_mav(view_table->get_table_name(),
                                              view_table->table_id_,
                                              sel_stmt->get_select_items()))) {
    LOG_WARN("failed to generate select items for mav", K(ret));
  } else if (!mv_checker_.get_stmt().is_scala_group_by()
             && OB_FAIL(gen_real_time_view_filter_for_mav(*sel_stmt))) {
    LOG_WARN("failed to generate real time view filter", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt.get_condition_exprs().reuse();
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  ObIArray<SelectItem> &select_items = sel_stmt.get_select_items();
  int64_t idx = OB_INVALID_INDEX;
  ObRawExpr *expr = NULL;
  ObRawExpr *filter_expr = NULL;
  for (int64_t i = 0; OB_INVALID_INDEX == idx && OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
    if (OB_ISNULL(expr = orig_select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(expr));
    } else if (T_FUN_COUNT == expr->get_expr_type()
               && static_cast<ObAggFunRawExpr*>(expr)->get_real_param_exprs().empty()) {
      idx = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(idx < 0 || idx >= select_items.count()
                         || orig_select_items.count() != select_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx or select item count", K(ret), K(idx), K(orig_select_items.count()), K(select_items.count()));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_GT, select_items.at(idx).expr_, exprs_.int_zero_, filter_expr))) {
    LOG_WARN("failed to build greater op expr", K(ret));
  } else if (OB_FAIL(sel_stmt.get_condition_exprs().push_back(filter_expr))) {
    LOG_WARN("failed to push back where conds", K(ret));
  }
  return ret;
}

/*
for real-time mv:
    select t1.c1 c1, count(*) cnt, count(t2.c2) cnt_c2, sum(t2.c2) sum_c2, avg(t2.c2) avg_c2
    from t1, t2
    where t1.c1 = t2.c1
    group by t1.c1;
generate inner real time view as:
    select c1, sum(cnt) cnt, sum(cnt_c2) cnt_c2, sum(sum_c2) sum_c2
    from (  select c1, cnt, cnt_c2, sum_c2 from rt_mv
            union all
            inner_delta_mav_1
            union all
            inner_delta_mav_2)
    group by c1;
only group by and basic aggregate functions added to select list
*/
int ObMVPrinter::gen_inner_real_time_view_for_mav(ObSelectStmt *&inner_rt_view)
{
  int ret = OB_SUCCESS;
  inner_rt_view = NULL;
  ObSelectStmt *access_mv_stmt = NULL;
  ObSelectStmt *set_stmt = NULL;
  ObSEArray<ObSelectStmt*, 4> inner_delta_mavs;
  TableItem *mv_table = NULL;
  TableItem *view_table = NULL;
  if (OB_FAIL(create_simple_stmt(inner_rt_view))
      || OB_FAIL(create_simple_stmt(access_mv_stmt))
      || OB_FAIL(create_simple_stmt(set_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(access_mv_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_simple_join_mav_basic_select_list(*mv_table, access_mv_stmt->get_select_items(), NULL))) {
    LOG_WARN("failed to generate simple join mav basic select list", K(ret));
  } else if (OB_FAIL(gen_inner_delta_mav_for_mav(inner_delta_mavs))) {
    LOG_WARN("failed to gen inner delta mav for mav", K(ret));
  } else if (OB_FAIL(set_stmt->get_set_query().push_back(access_mv_stmt)
             || OB_FAIL(append(set_stmt->get_set_query(), inner_delta_mavs)))) {
    LOG_WARN("failed to set set query", K(ret));
  } else if (OB_FAIL(create_simple_table_item(inner_rt_view, INNER_RT_MV_VIEW_NAME, view_table, set_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_simple_join_mav_basic_select_list(*view_table,
                                                           inner_rt_view->get_select_items(),
                                                           &inner_rt_view->get_group_exprs()))) {
    LOG_WARN("failed to generate simple join mav basic select list", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    set_stmt->assign_set_all();
    set_stmt->assign_set_op(ObSelectStmt::UNION);
  }
  return ret;
}

int ObMVPrinter::gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs)
{
  int ret = OB_SUCCESS;
  inner_delta_mavs.reuse();
  if (OB_MV_FAST_REFRESH_SIMPLE_MAV == mv_checker_.get_refersh_type()) {
    ObSelectStmt *inner_delta_mav = NULL;
    if (OB_FAIL(gen_simple_mav_delta_mv_view(inner_delta_mav))) {
      LOG_WARN("failed gen simple mav delta mv view", K(ret));
    } else if (OB_FAIL(inner_delta_mavs.push_back(inner_delta_mav))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else if (OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV == mv_checker_.get_refersh_type()) {
    if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(inner_delta_mavs))) {
      LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt)
{
  int ret = OB_SUCCESS;
  merge_stmt = NULL;
  ObSelectStmt *source_stmt = NULL;
  if (OB_FAIL(gen_simple_mav_delta_mv_view(source_stmt))) {
    LOG_WARN("failed to gen simple source stmt", K(ret));
  } else if (OB_FAIL(gen_merge_for_simple_mav_use_delta_view(source_stmt, merge_stmt))) {
    LOG_WARN("failed to gen merge for simple mav", K(ret));
  }
  return ret;
}

/*
  merge into mv
  using delta_mv d_mv
  on (mv.c1 <=> d_mv.c1 and mv.c2 <=> d_mv.c2)
  when matched then update set
      cnt = cnt + d_cnt,
      cnt_c3 = cnt_c3 + d_cnt_c3,
      sum_c3 = case when cnt_c3 + d_cnt_c3 = 0 then null
                    when cnt_c3 = 0 then d_sum_c3
                    when d_cnt_c3 = 0 then sum_c3
                    else d_sum_c3 + sum_c3 end,
      avg_c3 = case when ... sum_c3 new value ... end
                        / (cnt_c3 + d_cnt_c3),
      calc_1 = ...
    delete where cnt = 0
  when not matched then insert
      (c1, c2, cnt, sum_c3, avg_c3, cnt_c3, calc_1, calc_2)
      values (d_mv.c1,
              d_mv.c2,
              d_mv.d_cnt,    -- count(*)
              case when d_mv.d_cnt_c3 = 0 then null else d_mv.d_sum_c3 end,    -- sum(c3)
              (case when d_mv.d_cnt_c3 = 0 then null else d_mv.d_sum_c3 end)
              / d_mv.d_cnt_c3,    -- avg(c3)
              d_mv.d_cnt_c3,    -- count(c3)
              ...
    where d_mv.d_cnt <> 0;
 */
// generate merge into to fast refresh simple mav in oracle mode
int ObMVPrinter::gen_merge_for_simple_mav_use_delta_view(ObSelectStmt *delta_mav, ObMergeStmt *&merge_stmt)
{
  int ret = OB_SUCCESS;
  merge_stmt = NULL;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  if (OB_ISNULL(delta_mav)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target table or source table is null", K(ret), K(delta_mav));
  } else if (OB_FAIL(create_simple_stmt(merge_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(merge_stmt, mv_schema_.get_table_name(), target_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_simple_table_item(merge_stmt, DELTA_BASIC_MAV_VIEW_NAME, source_table, delta_mav, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_insert_values_and_desc(target_table,
                                                source_table,
                                                merge_stmt->get_values_desc(),
                                                merge_stmt->get_values_vector()))) {
    LOG_WARN("failed to gen insert values and desc", K(ret), K(*target_table), K(*source_table));
  } else if (OB_FAIL(gen_update_assignments(merge_stmt->get_values_desc(),
                                            merge_stmt->get_values_vector(),
                                            source_table,
                                            merge_stmt->get_table_assignments()))) {
    LOG_WARN("failed to gen update assignments", K(ret));
  } else if (OB_FAIL(gen_merge_conds(*merge_stmt))) {
    LOG_WARN("failed to gen merge conds", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
    merge_stmt->set_target_table_id(target_table->table_id_);
    merge_stmt->set_source_table_id(source_table->table_id_);
  }
  return ret;
}

int ObMVPrinter::gen_insert_values_and_desc(const TableItem *target_table,
                                            const TableItem *source_table,
                                            ObIArray<ObColumnRefRawExpr*> &target_columns,
                                            ObIArray<ObRawExpr*> &values_exprs)
{
  int ret = OB_SUCCESS;
  target_columns.reuse();
  values_exprs.reuse();
  typedef ObSEArray<SelectItem, 8> SelectItemArray;
  SMART_VAR(SelectItemArray, select_items) {
    if (OB_ISNULL(target_table) || OB_ISNULL(source_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target table or source table is null", K(ret), K(target_table), K(source_table));
    } else if (OB_FAIL(gen_select_items_for_mav(source_table->get_table_name(),
                                                source_table->table_id_,
                                                select_items))) {
    } else if (OB_FAIL(target_columns.prepare_allocate(select_items.count()))
              || OB_FAIL(values_exprs.prepare_allocate(select_items.count()))) {
      LOG_WARN("failed to prepare allocate arrays", K(ret));
    } else {
      ObRawExpr *target_col = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
        if (OB_FAIL(create_simple_column_expr(target_table->get_table_name(), select_items.at(i).alias_name_,
                                              target_table->table_id_, target_col))) {
          LOG_WARN("failed to create simple column exprs", K(ret));
        } else {
          target_columns.at(i) = static_cast<ObColumnRefRawExpr*>(target_col);
          values_exprs.at(i) = select_items.at(i).expr_;
        }
      }
    }
  }
  return ret;
}

// generate select items in 3 steps:
// 1. group by exprs and count aggr
// 2. sum aggr
// 3. other select exprs
int ObMVPrinter::gen_select_items_for_mav(const ObString &table_name,
                                          const uint64_t table_id,
                                          ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  ObRawExpr *orig_expr = NULL;
  ObRawExpr *expr = NULL;
  ObRawExprCopier copier(expr_factory_);
  if (OB_FAIL(select_items.prepare_allocate(orig_select_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  }

  // 1. add group by exprs and count aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
    SelectItem &select_item = select_items.at(i);
    select_item.is_real_alias_ = true;
    select_item.alias_name_ = orig_select_items.at(i).alias_name_;
    if (OB_ISNULL(orig_expr = orig_select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(orig_select_items));
    } else if (!ObOptimizerUtil::find_item(orig_group_by_exprs, orig_expr)
                && T_FUN_COUNT != orig_expr->get_expr_type()) {
      select_item.expr_ = NULL;
    } else if (OB_FAIL(create_simple_column_expr(table_name, select_item.alias_name_, table_id, select_item.expr_))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(orig_expr, select_item.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  // 2. add sum aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
    ObRawExpr *source_count = NULL;
    SelectItem &select_item = select_items.at(i);
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(orig_expr = orig_select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(orig_select_items));
    } else if (NULL != select_item.expr_ || T_FUN_SUM != orig_expr->get_expr_type()) {
      /* do nothing */
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(orig_expr, orig_select_items, idx))) {
      LOG_WARN("failed to get dependent aggr of fun sum", K(ret));
    } else if (OB_UNLIKELY(0 > idx || select_items.count() <= idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(select_items.count()));
    } else if (OB_FAIL(create_simple_column_expr(table_name, orig_select_items.at(i).alias_name_, table_id, expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(gen_calc_expr_for_insert_clause_sum(select_items.at(idx).expr_, expr, select_item.expr_))) {
      LOG_WARN("failed to gen calc expr for aggr sum", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(orig_expr, select_item.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  // 3. add other select exprs
  if (OB_SUCC(ret)) {
    const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs = mv_checker_.get_expand_aggrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < expand_aggrs.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(expand_aggrs.at(i).second, expr))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(expand_aggrs.at(i).first, expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (NULL == select_items.at(i).expr_ &&
          OB_FAIL(copier.copy_on_replace(orig_select_items.at(i).expr_, select_items.at(i).expr_))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      }
    }
  }
  return ret;
}

//  sum(c3)
//  --> d_cnt_c3 and d_sum_c3 is output from merge source_table view
//  case when d_cnt_c3 = 0 then null else d_sum_c3 end
int ObMVPrinter::gen_calc_expr_for_insert_clause_sum(ObRawExpr *source_count,
                                                     ObRawExpr *source_sum,
                                                     ObRawExpr *&calc_sum)
{
  int ret = OB_SUCCESS;
  calc_sum = NULL;
  ObRawExpr *equal_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, source_count, exprs_.int_zero_, equal_expr))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_, equal_expr, exprs_.null_expr_, source_sum, calc_sum))) {
      LOG_WARN("failed to build case when expr", K(ret));
  }
  return ret;
}

//  sum(c3)
//  --> d_cnt_c3 and d_sum_c3 is output from merge source_table view
//  case when d_cnt_c3 = 0 then null else d_sum_c3 end
//  sum_c3 = case when cnt_c3 + d_cnt_c3 = 0 then null
//                else nvl(d_sum_c3, 0) + nvl(sum_c3) end,
int ObMVPrinter::gen_calc_expr_for_update_clause_sum(ObRawExpr *target_count,
                                                     ObRawExpr *source_count,
                                                     ObRawExpr *target_sum,
                                                     ObRawExpr *source_sum,
                                                     ObRawExpr *&calc_sum)
{
  int ret = OB_SUCCESS;
  calc_sum = NULL;
  ObOpRawExpr *add_expr = NULL;
  ObRawExpr *when_expr = NULL;
  ObCaseOpRawExpr *case_when_expr = NULL;
  ObRawExpr *nvl_target_sum = NULL;
  ObRawExpr *nvl_source_sum = NULL;
  if (OB_ISNULL(target_count) || OB_ISNULL(source_count) || OB_ISNULL(target_sum) || OB_ISNULL(source_sum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(target_count), K(source_count), K(target_sum), K(source_sum));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_OP_CASE, case_when_expr))) {
    LOG_WARN("create add expr failed", K(ret));
  } else if (OB_ISNULL(case_when_expr) || OB_ISNULL(exprs_.null_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(case_when_expr));
  } else if (OB_FAIL(ObRawExprUtils::build_add_expr(expr_factory_, target_count, source_count, add_expr))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, add_expr, exprs_.int_zero_, when_expr))
             || OB_FAIL(case_when_expr->add_when_param_expr(when_expr))
             || OB_FAIL(case_when_expr->add_then_param_expr(exprs_.null_expr_))) {
    LOG_WARN("failed to build and add when/then exprs", K(ret));
  } else if (OB_FAIL(add_nvl_above_exprs(target_sum, exprs_.int_zero_, nvl_target_sum))
             || OB_FAIL(add_nvl_above_exprs(source_sum, exprs_.int_zero_, nvl_source_sum))) {
    LOG_WARN("failed to add nvl above exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_add_expr(expr_factory_, nvl_target_sum, nvl_source_sum, add_expr))) {
    LOG_WARN("failed to build add expr", K(ret));
  } else {
    case_when_expr->set_default_param_expr(add_expr);
    calc_sum = case_when_expr;
  }
  return ret;
}

int ObMVPrinter::get_dependent_aggr_of_fun_sum(const ObRawExpr *expr,
                                               const ObIArray<SelectItem> &select_items,
                                               int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  const ObAggFunRawExpr *dep_aggr = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(T_FUN_SUM != expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), KPC(expr));
  } else if (OB_FAIL(ObMVChecker::get_dependent_aggr_of_fun_sum(mv_checker_.get_stmt(),
                                        static_cast<const ObAggFunRawExpr*>(expr)->get_param_expr(0),
                                        dep_aggr))) {
    LOG_WARN("failed to get dependent aggr of fun sum", K(ret));
  }
  for (int64_t i = 0; OB_INVALID_INDEX == idx && OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (select_items.at(i).expr_ == dep_aggr) {
      idx = i;
    }
  }
  return ret;
}

// call gen_update_assignments after gen_insert_values_and_desc,
// target_columns and values_exprs is needed for this function
// generate update values in 3 steps:
// 1. count aggr
// 2. sum aggr
// 3. other select exprs except group by exprs
int ObMVPrinter::gen_update_assignments(const ObIArray<ObColumnRefRawExpr*> &target_columns,
                                        const ObIArray<ObRawExpr*> &values_exprs,
                                        const TableItem *source_table,
                                        ObIArray<ObAssignment> &assignments,
                                        const bool for_mysql_update /* default false */)
{
  int ret = OB_SUCCESS;
  assignments.reuse();
  ObRawExprCopier copier(expr_factory_);
  ObRawExpr *expr = NULL;
  ObOpRawExpr *op_expr = NULL;
  ObRawExpr *source_sum = NULL;
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
  ObAssignment assign;
  ObSEArray<ObAssignment, 4> inner_assigns;
  if (OB_ISNULL(source_table) ||
      OB_UNLIKELY(target_columns.count() != select_items.count()
                  || target_columns.count() != values_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(source_table), K(target_columns.count()),
                                          K(values_exprs.count()), K(select_items.count()));
  }
  // 1. add count aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    assign.column_expr_ = target_columns.at(i);
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (ObOptimizerUtil::find_item(group_by_exprs, expr)) {
      if (OB_FAIL(copier.add_replaced_expr(expr, target_columns.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    } else if (T_FUN_COUNT != expr->get_expr_type()) {
      /* do nothing */
    } else if (OB_FAIL(ObRawExprUtils::build_add_expr(expr_factory_, target_columns.at(i), values_exprs.at(i), op_expr))) {
      LOG_WARN("failed to build add expr", K(ret));
    } else if (OB_FALSE_IT(assign.expr_ = op_expr)) {
    } else if (OB_FAIL(inner_assigns.push_back(assign))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(expr, assign.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  // 2. add sum aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    assign.column_expr_ = target_columns.at(i);
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (T_FUN_SUM != expr->get_expr_type()) {
      /* do nothing */
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(expr, select_items, idx))) {
      LOG_WARN("failed to get dependent aggr of fun sum", K(ret));
    } else if (OB_UNLIKELY(0 > idx || values_exprs.count() <= idx || target_columns.count() <= idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(values_exprs.count()), K(target_columns.count()));
    } else if (OB_FAIL(create_simple_column_expr(source_table->get_table_name(), select_items.at(i).alias_name_,
                                                 source_table->table_id_, source_sum))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(gen_calc_expr_for_update_clause_sum(target_columns.at(idx), values_exprs.at(idx),
                                                           target_columns.at(i),
                                                           source_sum, assign.expr_))) {
      LOG_WARN("failed to gen calc expr for aggr sum", K(ret));
    } else if (OB_FAIL(inner_assigns.push_back(assign))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(expr, assign.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  // 3. other select exprs except group by exprs
  if (OB_SUCC(ret)) {
    const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs = mv_checker_.get_expand_aggrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < expand_aggrs.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(expand_aggrs.at(i).second, expr))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(expand_aggrs.at(i).first, expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      assign.column_expr_ = target_columns.at(i);
      if (OB_ISNULL(expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
      } else if (T_FUN_COUNT == expr->get_expr_type()
                 || T_FUN_SUM == expr->get_expr_type()
                 || ObOptimizerUtil::find_item(group_by_exprs, expr)) {
        /* do nothing */
      } else if (OB_FAIL(copier.copy_on_replace(expr, assign.expr_))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(inner_assigns.push_back(assign))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !inner_assigns.empty()) {
    if (!for_mysql_update) {
      if (OB_FAIL(assignments.assign(inner_assigns))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    } else if (OB_FAIL(assignments.prepare_allocate(inner_assigns.count()))) {
      LOG_WARN("failed to prepare allocate array", K(ret));
    } else {
      int64_t idx = inner_assigns.count() - 1;
      for (int64_t i = 0; OB_SUCC(ret) && i < inner_assigns.count(); ++i) {
        if (OB_FAIL(assignments.at(i).assign(inner_assigns.at(idx - i)))) {
          LOG_WARN("failed to assign ObAssignment", K(ret));
        }
      }
    }
  }
  return ret;
}

// call gen_merge_conds after gen_merge_insert_clause,
// merge_stmt.get_values_desc() and merge_stmt.get_values_vector() is needed for this function
int ObMVPrinter::gen_merge_conds(ObMergeStmt &merge_stmt)
{
  int ret = OB_SUCCESS;
  merge_stmt.get_match_condition_exprs().reuse();
  merge_stmt.get_insert_condition_exprs().reuse();
  merge_stmt.get_delete_condition_exprs().reuse();

  const ObIArray<ObRawExpr*> &group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<ObColumnRefRawExpr*> &target_columns = merge_stmt.get_values_desc();
  const ObIArray<ObRawExpr*> &values_exprs = merge_stmt.get_values_vector();
  ObRawExpr *expr = NULL;
  ObRawExpr *match_cond = NULL;
  ObRawExpr *insert_count_expr = NULL;
  ObRawExpr *delete_count_expr = NULL;
  if (OB_UNLIKELY(target_columns.count() != select_items.count() ||
                  target_columns.count() != values_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(target_columns.count()), K(values_exprs.count()), K(select_items.count()));
  }
  // add on condition
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (T_FUN_COUNT == expr->get_expr_type()) {
      if (0 == static_cast<ObAggFunRawExpr*>(expr)->get_real_param_count()) {
        delete_count_expr = target_columns.at(i);
        insert_count_expr = values_exprs.at(i);
      }
    } else if (!ObOptimizerUtil::find_item(group_by_exprs, expr)) {
      /* do nothing */
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_NSEQ, target_columns.at(i), values_exprs.at(i), match_cond))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
    } else if (OB_FAIL(merge_stmt.get_match_condition_exprs().push_back(match_cond))) {
      LOG_WARN("failed to push back null safe equal expr", K(ret));
    }
  }

  // add insert/delete condition
  if (OB_SUCC(ret) && !mv_checker_.get_stmt().is_scala_group_by()) {
    ObRawExpr *insert_cond = NULL;
    ObRawExpr *delete_cond = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_NE, insert_count_expr, exprs_.int_zero_, insert_cond))
               || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, delete_count_expr, exprs_.int_zero_, delete_cond))) {
      LOG_WARN("failed to build equal expr", K(ret));
    } else if (OB_FAIL(merge_stmt.get_insert_condition_exprs().push_back(insert_cond))
               || OB_FAIL(merge_stmt.get_delete_condition_exprs().push_back(delete_cond))) {
      LOG_WARN("failed to push back equal expr", K(ret));
    }
  }
  return ret;
}

/*
  select  c1,
          c2,
          sum(dml_factor)                                as d_cnt,
          sum(dml_factor
              * (case when c3 is null then 0 else 1))    as d_cnt_c3,
          sum(dml_factor * c3)    as d_sum_c3
  from delta_t1
  where (old_new = 'N' and seq_no = \"MAXSEQ$$\") or (old_new = 'o' and seq_no = \"MINSEQ$$\")
      and ...
  group by c1, c2;
*/
int ObMVPrinter::gen_simple_mav_delta_mv_view(ObSelectStmt *&view_stmt)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  ObString empty_name;
  ObSelectStmt *delta_view = NULL;
  const TableItem *source_table = NULL;
  TableItem *table_item = NULL;
  ObRawExprCopier copier(expr_factory_);
  if (OB_UNLIKELY(1 != mv_checker_.get_stmt().get_table_size() ||
      OB_ISNULL(source_table = mv_checker_.get_stmt().get_table_item(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret), K(mv_checker_.get_stmt()));
  } else if (OB_FAIL(create_simple_stmt(view_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_delta_table_view(*source_table, delta_view))) {
    LOG_WARN("failed to gen delta table view", K(ret));
  } else if (OB_FAIL(create_simple_table_item(view_stmt, DELTA_TABLE_VIEW_NAME, table_item, delta_view))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*view_stmt, copier))) {
    LOG_WARN("failed to init expr copier", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_group_exprs(),
                                            view_stmt->get_group_exprs()))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier, *table_item,
                                                         view_stmt->get_group_exprs(),
                                                         view_stmt->get_select_items()))) {
    LOG_WARN("failed to gen select list ", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_condition_exprs(), view_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to convert conds exprs", K(ret));
  } else if (OB_FAIL(append_old_new_row_filter(*table_item, view_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to append old new row filter ", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                                     const TableItem &table,
                                                     const ObIArray<ObRawExpr*> &group_by_exprs,
                                                     ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  const ObIArray<ObAggFunRawExpr*> &orig_aggr_exprs = mv_checker_.get_stmt().get_aggr_items();
  ObRawExpr *dml_factor = NULL;
  ObAggFunRawExpr *aggr_expr = NULL;
  if (OB_UNLIKELY(orig_group_by_exprs.count() != group_by_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected group by size", K(ret), K(group_by_exprs), K(orig_group_by_exprs));
  } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), DML_FACTOR_COL_NAME, table.table_id_, dml_factor))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  }
  // add select list for group by
  for (int64_t i = 0; OB_SUCC(ret) && i < group_by_exprs.count(); ++i) {
    SelectItem sel_item;
    sel_item.expr_ = group_by_exprs.at(i);
    sel_item.is_real_alias_ = true;
    if (OB_FAIL(get_mv_select_item_name(orig_group_by_exprs.at(i), sel_item.alias_name_))) {
      LOG_WARN("failed to get mv select item name", K(ret));
    } else if (OB_FAIL(select_items.push_back(sel_item))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  // add select list for basic aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_aggr_exprs.count(); ++i) {
    SelectItem sel_item;
    sel_item.is_real_alias_ = true;
    if (OB_ISNULL(aggr_expr = orig_aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(aggr_expr));
    } else if (!ObMVChecker::is_basic_aggr(aggr_expr->get_expr_type())) {
      /* do nothing */
    } else if (OB_FAIL(get_mv_select_item_name(aggr_expr, sel_item.alias_name_))) {
      LOG_WARN("failed to get mv select item name", K(ret));
    } else if (OB_FAIL(gen_basic_aggr_expr(copier, dml_factor, *aggr_expr, sel_item.expr_))) {
      LOG_WARN("failed to gen basic aggr expr", K(ret));
    } else if (OB_FAIL(select_items.push_back(sel_item))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  return ret;
}

// keep the select items ordering with gen_simple_mav_delta_mv_select_list
int ObMVPrinter::gen_simple_join_mav_basic_select_list(const TableItem &table,
                                                       ObIArray<SelectItem> &select_items,
                                                       ObIArray<ObRawExpr*> *group_by_exprs)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const bool stmt_need_aggr = NULL != group_by_exprs;
  if (NULL != group_by_exprs) {
    group_by_exprs->reuse();
  }
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_checker_.get_stmt().get_group_exprs();
  const ObIArray<ObAggFunRawExpr*> &orig_aggr_exprs = mv_checker_.get_stmt().get_aggr_items();
  ObAggFunRawExpr *aggr_expr = NULL;
  ObRawExpr *col_expr = NULL;
  // 1. add select list for group by
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_group_by_exprs.count(); ++i) {
    SelectItem sel_item;
    sel_item.is_real_alias_ = true;
    if (OB_FAIL(get_mv_select_item_name(orig_group_by_exprs.at(i), sel_item.alias_name_))) {
      LOG_WARN("failed to get mv select item name", K(ret));
    } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(),
                                                 sel_item.alias_name_,
                                                 table.table_id_,
                                                 sel_item.expr_))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(select_items.push_back(sel_item))) {
      LOG_WARN("failed to pushback", K(ret));
    } else if (stmt_need_aggr && OB_FAIL(group_by_exprs->push_back(sel_item.expr_))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  // 2. add select list for basic aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_aggr_exprs.count(); ++i) {
    SelectItem sel_item;
    sel_item.is_real_alias_ = true;
    sel_item.expr_ = NULL;
    if (OB_ISNULL(aggr_expr = orig_aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(aggr_expr));
    } else if (!ObMVChecker::is_basic_aggr(aggr_expr->get_expr_type())) {
      /* do nothing */
    } else if (OB_FAIL(get_mv_select_item_name(aggr_expr, sel_item.alias_name_))) {
      LOG_WARN("failed to get mv select item name", K(ret));
    } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(),
                                                 sel_item.alias_name_,
                                                 table.table_id_, col_expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (!stmt_need_aggr) {
      sel_item.expr_ = col_expr;
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SUM, aggr_expr))) {
      LOG_WARN("create ObAggFunRawExpr failed", K(ret));
    } else if (OB_ISNULL(aggr_expr) || OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(aggr_expr), K(col_expr));
    } else if (OB_FAIL(aggr_expr->add_real_param_expr(col_expr))) {
      LOG_WARN("failed to add param expr to agg expr", K(ret));
    } else {
      sel_item.expr_ = aggr_expr;
    }

    if (OB_SUCC(ret) && NULL != sel_item.expr_ && OB_FAIL(select_items.push_back(sel_item))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::get_mv_select_item_name(const ObRawExpr *expr, ObString &select_name)
{
  int ret = OB_SUCCESS;
  select_name = ObString::make_empty_string();
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
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

// count(*)   --> sum(dml_factor)
// count(expr)  --> sum(dml_factor * (case when expr is null then 0 else 1 end))
// sum(expr)  --> sum(dml_factor * expr)
int ObMVPrinter::gen_basic_aggr_expr(ObRawExprCopier &copier,
                                     ObRawExpr *dml_factor,
                                     ObAggFunRawExpr &aggr_expr,
                                     ObRawExpr *&aggr_print_expr)
{
  int ret = OB_SUCCESS;
  aggr_print_expr = NULL;
  ObAggFunRawExpr *new_aggr_expr = NULL;
  ObRawExpr *param = NULL;
  ObRawExpr *print_param = NULL;
  if (T_FUN_COUNT == aggr_expr.get_expr_type() && 0 == aggr_expr.get_real_param_count()) {
    print_param = dml_factor;
  } else if (OB_UNLIKELY(1 != aggr_expr.get_real_param_count())
             || OB_ISNULL(param = aggr_expr.get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggr", K(ret), K(aggr_expr));
  } else if (OB_FAIL(copier.copy_on_replace(param, param))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (T_FUN_COUNT == aggr_expr.get_expr_type()) {
    ObRawExpr *is_null = NULL;
    ObRawExpr *case_when = NULL;
    if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory_, param, false, is_null))) {
      LOG_WARN("failed to build is null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_, is_null, exprs_.int_zero_, exprs_.int_one_, case_when))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_MUL, dml_factor, case_when, print_param))) {
      LOG_WARN("failed to build mul expr", K(ret));
    }
  } else if (T_FUN_SUM == aggr_expr.get_expr_type()) {
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_MUL, dml_factor, param, print_param))) {
      LOG_WARN("failed to build mul expr", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggr", K(ret), K(aggr_expr));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SUM, new_aggr_expr))) {
    LOG_WARN("create ObAggFunRawExpr failed", K(ret));
  } else if (OB_ISNULL(new_aggr_expr) || OB_ISNULL(print_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(new_aggr_expr), K(print_param));
  } else if (OB_FAIL(new_aggr_expr->add_real_param_expr(print_param))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (T_FUN_COUNT != aggr_expr.get_expr_type() || !mv_checker_.get_stmt().is_scala_group_by()) {
    aggr_print_expr = new_aggr_expr;
  } else if (OB_FAIL(add_nvl_above_exprs(new_aggr_expr, exprs_.int_zero_, aggr_print_expr))) {
    //  for scalar group by, d_cnt from sum(dml_factor) may get null, need convert null to 0
    //  count(*) --> nvl(d_cnt, 0)
    //  count(c3) --> nvl(d_cnt_3, 0)
    LOG_WARN("failed to gen calc expr for scalar count", K(ret));
  }
  return ret;
}

int ObMVPrinter::add_nvl_above_exprs(ObRawExpr *expr, ObRawExpr *default_expr, ObRawExpr *&res_expr)
{
  int ret = OB_SUCCESS;
  res_expr = NULL;
  ObSysFunRawExpr *nvl_expr = NULL;
  if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_NVL, nvl_expr))) {
    LOG_WARN("fail to create nvl expr", K(ret));
  } else if (OB_ISNULL(expr) || OB_ISNULL(default_expr) || OB_ISNULL(nvl_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(default_expr), K(nvl_expr));
  } else if (OB_FAIL(nvl_expr->add_param_expr(expr))
             || OB_FAIL(nvl_expr->add_param_expr(default_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else {
    nvl_expr->set_expr_type(T_FUN_SYS_NVL);
    nvl_expr->set_func_name(ObString::make_string(N_NVL));
    res_expr = nvl_expr;
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
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, old_new, exprs_.str_n_, equal_old_new))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, seq_no, win_seq, equal_seq_no))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_AND, equal_old_new, equal_seq_no, new_row_filter))) {
    LOG_WARN("failed to build new row filter expr", K(ret));
  }

  if (OB_FAIL(ret) || !get_old_row) {
  } else if (OB_FAIL(create_simple_column_expr(table_name, WIN_MIN_SEQ_COL_NAME, table_id, win_seq))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, old_new, exprs_.str_o_, equal_old_new))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, seq_no, win_seq, equal_seq_no))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_AND, equal_old_new, equal_seq_no, old_row_filter))) {
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
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_OR, new_row_filter, old_row_filter, filter))) {
    LOG_WARN("failed to build or op expr", K(ret));
  } else if (OB_FAIL(filters.push_back(filter))) {
    LOG_WARN("failed to pushback", K(ret));
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
  } else if (OB_FAIL(mv_checker_.get_mlog_table_schema(&source_table, mlog_schema))) {
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
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_GT, scn_column, exprs_.last_refresh_scn_, filter))) {
    LOG_WARN("failed to build greater op expr", K(ret));
  } else if (OB_FAIL(conds.push_back(filter))) {
    LOG_WARN("failed to pushback", K(ret));
  } else if (for_rt_expand_) {
    /* do nothing */
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_LE, scn_column, exprs_.refresh_scn_, filter))) {
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
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, old_new_col, exprs_.str_n_, equal_expr))) {
    LOG_WARN("failed to build mul expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_, equal_expr, exprs_.int_one_, exprs_.int_neg_one_, sel_item->expr_))) {
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
  if (OB_FAIL(mv_checker_.get_stmt().get_column_items(source_table.table_id_, column_items))) {
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
  if (OB_ISNULL(stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(source_table.ref_id_, source_data_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(source_data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(source_data_schema));
  } else if (!source_data_schema->is_heap_table()) {
    if (OB_FAIL(source_data_schema->get_rowkey_column_ids(unique_col_ids))) {
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
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, win_max))
              || OB_FAIL(expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, win_min))
              || OB_FAIL(expr_factory_.create_raw_expr(T_FUN_MAX, aggr_max))
              || OB_FAIL(expr_factory_.create_raw_expr(T_FUN_MIN, aggr_min))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(sequence_expr), OB_ISNULL(win_max) || OB_ISNULL(win_min)
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
  if (OB_FAIL(expr_factory_.create_raw_expr(T_REF_COLUMN, column_ref))) {
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
  } else if (OB_ISNULL(table_item = stmt->create_table_item(alloc_))) {
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
  } else if (OB_ISNULL(ptr = alloc_.alloc(sizeof(JoinedTable)))) {
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

int ObMVPrinter::init(const share::SCN &last_refresh_scn,
                      const share::SCN &refresh_scn,
                      const MajorRefreshInfo *major_refresh_info)
{
  int ret = OB_SUCCESS;
  inited_ = false;
  ObCollationType cs_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  ObQueryCtx *query_ctx = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  major_refresh_info_ = major_refresh_info;
  if (OB_ISNULL(query_ctx = stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_ctx));
  } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_database_schema(mv_schema_.get_database_id(), db_schema))) {
    LOG_WARN("fail to get data base schema", K(ret), K(mv_schema_.get_database_id()));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(db_schema));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, 0, exprs_.int_zero_))
             || OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, 1, exprs_.int_one_))
             || OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, -1, exprs_.int_neg_one_))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(expr_factory_, ObVarcharType, ObString("N"), cs_type, exprs_.str_n_))
             || OB_FAIL(ObRawExprUtils::build_const_string_expr(expr_factory_, ObVarcharType, ObString("O"), cs_type, exprs_.str_o_))) {
    LOG_WARN("fail to build const string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory_, exprs_.null_expr_))) {
    LOG_WARN("failed to create const null expr", K(ret));
  } else if (for_rt_expand_) {
    ObSysFunRawExpr *sys_last_refresh_scn = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_LAST_REFRESH_SCN, sys_last_refresh_scn))) {
      LOG_WARN("failed to create last_refresh_scn sys expr", K(ret));
    } else {
      sys_last_refresh_scn->set_mview_id(mv_schema_.get_table_id());
      sys_last_refresh_scn->set_func_name(ObString::make_string(N_SYS_LAST_REFRESH_SCN));
      exprs_.last_refresh_scn_ = sys_last_refresh_scn;
    }
  } else {
    ObConstRawExpr *const_last_refresh_scn = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(expr_factory_, ObUInt64Type, last_refresh_scn.get_val_for_sql(), const_last_refresh_scn))
             || OB_FAIL(ObRawExprUtils::build_const_uint_expr(expr_factory_, ObUInt64Type, refresh_scn.get_val_for_sql(), exprs_.refresh_scn_))) {
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

int ObMVPrinter::gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt *base_del_stmt = NULL;
  ObDeleteStmt *del_stmt = NULL;
  TableItem *mv_table = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  if (OB_FAIL(create_simple_stmt(base_del_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(base_del_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
    ObRawExpr *semi_filter = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i), mv_table, true, true, semi_filter))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (orig_table_items.count() - 1 == i) {
        if (OB_FAIL(base_del_stmt->get_condition_exprs().push_back(semi_filter))) {
          LOG_WARN("failed to push back semi filter", K(ret));
        } else if (OB_FAIL(dml_stmts.push_back(base_del_stmt))) {
          LOG_WARN("failed to push back delete stmt", K(ret));
        }
      } else if (OB_FAIL(create_simple_stmt(del_stmt))) {
        LOG_WARN("failed to create simple stmt", K(ret));
      } else if (OB_FAIL(del_stmt->get_table_items().assign(base_del_stmt->get_table_items()))
                || OB_FAIL(del_stmt->get_from_items().assign(base_del_stmt->get_from_items()))) {
        LOG_WARN("failed to assign structure", K(ret));
      } else if (OB_FAIL(del_stmt->get_condition_exprs().push_back(semi_filter))) {
        LOG_WARN("failed to push back semi filter", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(del_stmt))) {
        LOG_WARN("failed to push back delete stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  ObInsertStmt *base_insert_stmt = NULL;
  ObSEArray<ObSelectStmt*, 8> access_delta_stmts;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  if (OB_FAIL(gen_access_delta_data_for_simple_mjv(access_delta_stmts))) {
    LOG_WARN("failed to generate access delta data for simple mjv", K(ret));
  } else if (OB_UNLIKELY(orig_table_items.count() != access_delta_stmts.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(orig_table_items.count()), K(access_delta_stmts.count()));
  } else if (OB_FAIL(create_simple_stmt(base_insert_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(base_insert_stmt, mv_schema_.get_table_name(), target_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
    ObRawExpr *target_col = NULL;
    ObInsertStmt *insert_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
      if (OB_FAIL(create_simple_column_expr(target_table->get_table_name(), orig_select_items.at(i).alias_name_,
                                            target_table->table_id_, target_col))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (OB_FAIL(base_insert_stmt->get_values_desc().push_back(static_cast<ObColumnRefRawExpr*>(target_col)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (orig_table_items.count() - 1 == i) {
        insert_stmt = base_insert_stmt;
      } else if (OB_FAIL(create_simple_stmt(insert_stmt))) {
        LOG_WARN("failed to create simple stmt", K(ret));
      } else if (OB_FAIL(insert_stmt->get_values_desc().assign(base_insert_stmt->get_values_desc()))
                 || OB_FAIL(insert_stmt->get_table_items().assign(base_insert_stmt->get_table_items()))) {
        LOG_WARN("failed to assign structure", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(create_simple_table_item(insert_stmt, DELTA_MAV_VIEW_NAME, source_table, access_delta_stmts.at(i)))) {
        LOG_WARN("failed to create simple table item", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(insert_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_access_mv_data_for_simple_mjv(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *mv_table = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(sel_stmt->get_select_items().prepare_allocate(orig_select_items.count()))
             || OB_FAIL(sel_stmt->get_condition_exprs().prepare_allocate(orig_table_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    ObIArray<SelectItem> &select_items = sel_stmt->get_select_items();
    ObIArray<ObRawExpr*> &conds = sel_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(), orig_select_items.at(i).alias_name_,
                                            mv_table->table_id_, select_items.at(i).expr_))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else {
        select_items.at(i).alias_name_ = orig_select_items.at(i).alias_name_;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i), mv_table, false, true, conds.at(i)))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      }
    }
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
  if (OB_ISNULL(outer_table) || OB_ISNULL(source_table) || OB_ISNULL(stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(outer_table), K(source_table), K(stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(mv_checker_.get_mlog_table_schema(source_table, mlog_schema))) {
    LOG_WARN("failed to get mlog schema", K(ret), KPC(source_table));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(expr_factory_.create_raw_expr(is_exists ? T_OP_EXISTS : T_OP_NOT_EXISTS, exists_op_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(exists_op_expr)
             || OB_UNLIKELY(NULL == mlog_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr), K(exists_op_expr), K(mlog_schema));
  } else if (OB_FAIL(exists_op_expr->add_param_expr(query_ref_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
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
  if (OB_FAIL(expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(expr_factory_.create_raw_expr(T_OP_NOT_EXISTS, exists_op_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(exists_op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(query_ref_expr), K(exists_op_expr));
  } else if (OB_FAIL(exists_op_expr->add_param_expr(query_ref_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
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
   if (OB_ISNULL(stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(origin_table.ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table schema", K(ret), KPC(table_schema));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_column_ids))) {
    LOG_WARN("failed to get rowkey column ids", K(ret));
  } else if (OB_UNLIKELY(rowkey_column_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty array", K(ret));
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
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, l_col, r_col, join_cond))) {
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
  const ObIArray<SelectItem> &select_items = mv_checker_.get_stmt().get_select_items();
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

int ObMVPrinter::gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *base_delta_stmt = NULL;
  ObSEArray<ObRawExpr*, 2> semi_filters;
  ObSEArray<ObRawExpr*, 2> anti_filters;
  const int64_t table_size = mv_checker_.get_stmt().get_table_items().count();
  if (OB_UNLIKELY(table_size < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(table_size));
  } else if (OB_FAIL(access_delta_stmts.prepare_allocate(table_size))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret), K(table_size));
  } else if (OB_FAIL(prepare_gen_access_delta_data_for_simple_mjv(base_delta_stmt,
                                                                  semi_filters,
                                                                  anti_filters))) {
    LOG_WARN("failed to prepare generate access delta data for simple_mjv", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_size - 1; ++i) {
    if (OB_FAIL(gen_one_access_delta_data_for_simple_mjv(*base_delta_stmt, i, semi_filters, anti_filters,
                                                         access_delta_stmts.at(i)))) {
      LOG_WARN("failed to generate one access delta data for simple_mjv", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(base_delta_stmt->get_condition_exprs().push_back(semi_filters.at(table_size - 1)))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    access_delta_stmts.at(table_size - 1) = base_delta_stmt;
  }
  return ret;
}

int ObMVPrinter::prepare_gen_access_delta_data_for_simple_mjv(ObSelectStmt *&base_delta_stmt,
                                                              ObIArray<ObRawExpr*> &semi_filters,
                                                              ObIArray<ObRawExpr*> &anti_filters)
{
  int ret = OB_SUCCESS;
  base_delta_stmt = NULL;
  ObRawExprCopier copier(expr_factory_);
  ObSemiToInnerHint *semi_to_inner_hint = NULL;
  ObSEArray<ObItemType, 1> conflict_hints;
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  if (OB_FAIL(semi_filters.prepare_allocate(orig_table_items.count()))
      || OB_FAIL(anti_filters.prepare_allocate(orig_table_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret), K(orig_table_items.count()));
  } else if (OB_FAIL(create_simple_stmt(base_delta_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(construct_table_items_for_simple_mjv_delta_data(base_delta_stmt))) {
    LOG_WARN("failed to construct table items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*base_delta_stmt, copier))) {
    LOG_WARN("failed to init expr copier for stmt", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint(&alloc_, T_SEMI_TO_INNER, semi_to_inner_hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(base_delta_stmt->get_stmt_hint().merge_hint(*semi_to_inner_hint,
                                                                  ObHintMergePolicy::HINT_DOMINATED_EQUAL,
                                                                  conflict_hints))) {
    LOG_WARN("failed to merge hint", K(ret));
  } else if (OB_FAIL(construct_from_items_for_simple_mjv_delta_data(copier, *base_delta_stmt))) {
    LOG_WARN("failed to construct from items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_condition_exprs(), base_delta_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else {
    SelectItem sel_item;
    const ObIArray<TableItem*> &cur_table_items = base_delta_stmt->get_table_items();
    const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
    ObIArray<SelectItem> &select_items = base_delta_stmt->get_select_items();
    if (OB_UNLIKELY(cur_table_items.count() != orig_table_items.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table items count", K(ret), K(cur_table_items.count()), K(orig_table_items.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
      sel_item.is_real_alias_ = true;
      sel_item.alias_name_ = orig_select_items.at(i).alias_name_;
      if (OB_FAIL(copier.copy_on_replace(orig_select_items.at(i).expr_, sel_item.expr_))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(select_items.push_back(sel_item))) {
        LOG_WARN("failed to pushback", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i),
                                            cur_table_items.at(i),
                                            true, false,
                                            semi_filters.at(i)))) {
        LOG_WARN("failed to generate exists filter", K(ret));
      } else if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i),
                                                   cur_table_items.at(i),
                                                   false, false,
                                                   anti_filters.at(i)))) {
        LOG_WARN("failed to generate not exists filter", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::construct_table_items_for_simple_mjv_delta_data(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  const TableItem *orig_table = NULL;
  TableItem *table = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
    if (OB_ISNULL(orig_table = orig_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(orig_table_items));
    } else if (OB_FAIL(create_simple_table_item(stmt, orig_table->table_name_, table, NULL, false))) {
      LOG_WARN("failed to create simple table item", K(ret));
    } else {
      set_info_for_simple_table_item(*table, *orig_table);
    }
  }
  return ret;
}

void ObMVPrinter::set_info_for_simple_table_item(TableItem &table, const TableItem &source_table)
{
  table.alias_name_ = source_table.alias_name_;
  table.synonym_name_ = source_table.synonym_name_;
  table.database_name_ = source_table.database_name_;
  table.synonym_db_name_ = source_table.synonym_db_name_;
  if (!for_rt_expand_) {
    table.flashback_query_expr_ = exprs_.refresh_scn_;
    table.flashback_query_type_ = TableItem::USING_SCN;
  }
}

int ObMVPrinter::init_expr_copier_for_stmt(ObSelectStmt &target_stmt, ObRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
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
    } else if (OB_FAIL(mv_checker_.get_stmt().get_column_items(orig_table->table_id_, column_items))) {
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
  if (OB_FAIL(target_stmt.deep_copy_join_tables(alloc_, copier, mv_checker_.get_stmt()))) {
    LOG_WARN("failed to deep copy join tables", K(ret));
  } else if (OB_FAIL(target_stmt.get_from_items().assign(mv_checker_.get_stmt().get_from_items()))) {
    LOG_WARN("failed to assign from items", K(ret));
  } else {
    // for non joined table, adjust table id in from item
    ObIArray<FromItem> &from_items = target_stmt.get_from_items();
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
      if (from_items.at(i).is_joined_) {
        /* do nothing */
      } else if (OB_FAIL(mv_checker_.get_stmt().get_table_item_idx(from_items.at(i).table_id_, idx))) {
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

// delta_mv = delta_t1 join pre_t2 join pre_t3 ... join pre_tn
//            union all t1 join delta_t2 join pre_t3 ... join pre_tn
//            ...
//            union all t1 join t2 join t3 ... join delta_tn
// input table_idx specify the delta table
int ObMVPrinter::gen_one_access_delta_data_for_simple_mjv(const ObSelectStmt &base_delta_stmt,
                                                          const int64_t table_idx,
                                                          const ObIArray<ObRawExpr*> &semi_filters,
                                                          const ObIArray<ObRawExpr*> &anti_filters,
                                                          ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  if (OB_UNLIKELY(table_idx < 0 || table_idx >= semi_filters.count()
                  || semi_filters.count() != anti_filters.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(table_idx), K(semi_filters.count()), K(anti_filters.count()));
  } else if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(sel_stmt->get_joined_tables().assign(base_delta_stmt.get_joined_tables()))
             || OB_FAIL(sel_stmt->get_table_items().assign(base_delta_stmt.get_table_items()))
             || OB_FAIL(sel_stmt->get_from_items().assign(base_delta_stmt.get_from_items()))
             || OB_FAIL(sel_stmt->get_select_items().assign(base_delta_stmt.get_select_items()))
             || OB_FAIL(sel_stmt->get_condition_exprs().assign(base_delta_stmt.get_condition_exprs()))
             || OB_FAIL(sel_stmt->get_stmt_hint().merge_stmt_hint(base_delta_stmt.get_stmt_hint()))) {
    LOG_WARN("failed to assign structure", K(ret));
  } else if (OB_FAIL(sel_stmt->get_condition_exprs().push_back(semi_filters.at(table_idx)))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else {
    for (int64_t i = table_idx + 1; OB_SUCC(ret) && i < anti_filters.count(); ++i) {
      if (OB_FAIL(sel_stmt->get_condition_exprs().push_back(anti_filters.at(i)))) {
        LOG_WARN("failed to push back anti filter", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_update_insert_delete_for_simple_join_mav(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObSEArray<ObSelectStmt*, 4> inner_delta_mavs;
  if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(inner_delta_mavs))) {
    LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
  } else {
    // zhanyue todo: call gen_merge_for_simple_mav once, and assign stmt, adjust inner_delta_mavs
    ObUpdateStmt *update_stmt = NULL;
    ObInsertStmt *insert_stmt = NULL;
    ObSEArray<ObRawExpr*, 16> values;
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_delta_mavs.count(); ++i) {
      // zhanyue todo: call gen_merge_for_simple_mav once, and assign stmt, adjust inner_delta_mavs
      values.reuse();
      if (OB_FAIL(gen_insert_for_mav(inner_delta_mavs.at(i), values, insert_stmt))) {
        LOG_WARN("failed to gen insert for mav", K(ret));
      } else if (OB_FAIL(gen_update_for_mav(inner_delta_mavs.at(i), insert_stmt->get_values_desc(),
                                            values, update_stmt))) {
        LOG_WARN("failed to gen update for mav", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(update_stmt))  // pushback and execute in this ordering
                 || OB_FAIL(dml_stmts.push_back(insert_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }

    if (OB_SUCC(ret) && !mv_checker_.get_stmt().is_scala_group_by()) {
      ObDeleteStmt *delete_stmt = NULL;
      if (OB_ISNULL(insert_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(insert_stmt));
      } else if (OB_FAIL(gen_delete_for_mav(insert_stmt->get_values_desc(), delete_stmt))) {
        LOG_WARN("failed gen delete for mav", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(delete_stmt))) { // pushback and execute in this ordering
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_merge_for_simple_join_mav(ObIArray<ObDMLStmt *> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObSEArray<ObSelectStmt *, 4> inner_delta_mavs;
  if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(inner_delta_mavs))) {
    LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
  } else {
    ObMergeStmt *merge_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_delta_mavs.count(); ++i) {
      // zhanyue todo: call gen_merge_for_simple_mav once, and assign stmt, adjust inner_delta_mavs
      if (OB_FAIL(gen_merge_for_simple_mav_use_delta_view(inner_delta_mavs.at(i), merge_stmt))) {
        LOG_WARN("failed to gen merge for simple mav", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(merge_stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObMVPrinter::gen_inner_delta_mav_for_simple_join_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs)
{
  int ret = OB_SUCCESS;
  inner_delta_mavs.reuse();
  ObSEArray<ObSelectStmt*, 4> delta_datas;
  ObSEArray<ObSelectStmt*, 4> pre_datas;
  const ObIArray<TableItem*> &source_tables = mv_checker_.get_stmt().get_table_items();
  const TableItem *source_table = NULL;
  if (OB_FAIL(inner_delta_mavs.prepare_allocate(source_tables.count()))
      || OB_FAIL(pre_datas.prepare_allocate(source_tables.count()))
      || OB_FAIL(delta_datas.prepare_allocate(source_tables.count()))) {
    LOG_WARN("failed to prepare allocate ObSelectStmt pointer arrays", K(ret), K(source_tables.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < source_tables.count(); ++i) {
    pre_datas.at(i) = NULL;
    delta_datas.at(i) = NULL;
    if (OB_ISNULL(source_table = source_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(source_table));
    } else if (OB_FAIL(gen_delta_data_access_stmt(*source_tables.at(i), delta_datas.at(i)))) {
      LOG_WARN("failed to gen delta data access stmt", K(ret));
    } else if (0 < i && OB_FAIL(gen_pre_data_access_stmt(*source_tables.at(i), pre_datas.at(i)))) {
      LOG_WARN("failed to gen pre data access stmt", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < source_tables.count(); ++i) {
    if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(i, delta_datas, pre_datas, inner_delta_mavs.at(i)))) {
      LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::gen_inner_delta_mav_for_simple_join_mav(const int64_t inner_delta_no,
                                                         const ObIArray<ObSelectStmt*> &all_delta_datas,
                                                         const ObIArray<ObSelectStmt*> &all_pre_datas,
                                                         ObSelectStmt *&inner_delta_mav)
{
  int ret = OB_SUCCESS;
  inner_delta_mav = NULL;
  ObRawExprCopier copier(expr_factory_);
  const TableItem *delta_table = NULL;
  if (OB_FAIL(create_simple_stmt(inner_delta_mav))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(construct_table_items_for_simple_join_mav_delta_data(inner_delta_no,
                                                                          all_delta_datas,
                                                                          all_pre_datas,
                                                                          inner_delta_mav))) {
    LOG_WARN("failed to construct table items for simple join mav delta data", K(ret));
  } else if (OB_UNLIKELY(inner_delta_mav->get_table_size() <= inner_delta_no)
             || OB_ISNULL(delta_table = inner_delta_mav->get_table_item(inner_delta_no))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table items", K(ret), K(inner_delta_no), K(inner_delta_mav->get_table_items()));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*inner_delta_mav, copier))) {
    LOG_WARN("failed to init expr copier for stmt", K(ret));
  } else if (OB_FAIL(construct_from_items_for_simple_mjv_delta_data(copier, *inner_delta_mav))) {
    LOG_WARN("failed to construct from items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_condition_exprs(),
                                            inner_delta_mav->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_group_exprs(),
                                            inner_delta_mav->get_group_exprs()))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier, *delta_table,
                                                         inner_delta_mav->get_group_exprs(),
                                                         inner_delta_mav->get_select_items()))) {
    LOG_WARN("failed to gen select list ", K(ret));
  }
  return ret;
}

//  mjv: t1 join t2 join t3
//  delta_mjv = delta_t1 join pre_t2 join pre_t3
//              union all t1 join delta_t2 join pre_t3
//              union all t1 join t2 join delta_t3
//  this function generate table items for one branch of union all
int ObMVPrinter::construct_table_items_for_simple_join_mav_delta_data(const int64_t inner_delta_no,
                                                                      const ObIArray<ObSelectStmt*> &all_delta_datas,
                                                                      const ObIArray<ObSelectStmt*> &all_pre_datas,
                                                                      ObSelectStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > inner_delta_no || inner_delta_no >= all_pre_datas.count()
                  || all_pre_datas.count() != all_delta_datas.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(inner_delta_no), K(all_pre_datas.count()), K(all_delta_datas.count()));
  } else {
    const TableItem *orig_table = NULL;
    TableItem *table = NULL;
    const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
    ObSelectStmt *view_stmt = NULL;
    const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
    int64_t pos = 0;
    char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
    int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      pos = 0;
      view_stmt = inner_delta_no == i
                  ? all_delta_datas.at(i)
                  : (inner_delta_no < i ? all_pre_datas.at(i) : NULL);
      if (OB_ISNULL(orig_table = orig_table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(orig_table_items));
      } else if (OB_FAIL(create_simple_table_item(stmt, orig_table->table_name_, table, view_stmt ,false))) {
        LOG_WARN("failed to create simple table item", K(ret));
      } else if (inner_delta_no > i)  { //  access current data
        set_info_for_simple_table_item(*table, *orig_table);
      } else if (OB_FAIL(BUF_PRINTF(inner_delta_no == i ? "DLT_%.*s" : "PRE_%.*s",
                                    orig_table->get_object_name().length(),
                                    orig_table->get_object_name().ptr()))) {
        LOG_WARN("failed to buf print for delta/pre view name", K(ret));
      } else if (OB_FAIL(ob_write_string(alloc_, ObString(pos, buf), table->alias_name_))) {
        LOG_WARN("failed to write string", K(ret));
      }
    }
  }
  return ret;
}

// todo: for multi dml operators on the same rowkey, we need access at most two rows actually.
int ObMVPrinter::gen_delta_data_access_stmt(const TableItem &source_table,
                                            ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  access_sel = NULL;
  const uint64_t mlog_sel_flags = MLOG_EXT_COL_DML_FACTOR;
  if (OB_FAIL(gen_delta_table_view(source_table, access_sel, mlog_sel_flags))) {
    LOG_WARN("failed to gen delta table view", K(ret));
  }
  return ret;
}

// select * from t where not exists (select 1 from mlog_t
//                                   where old_new = 'I' and seq_no = "MAXSEQ$$"
//                                         and mlog_t.pk <=> t.pk);
// union all
// select	...
// from (select ... from mlog_t where ora_rowscn > xxx and ora_rowscn < xxx)
// where (old_new = 'O' and seq_no = "MINSEQ$$")
// ;
int ObMVPrinter::gen_pre_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *union_stmt = NULL;
  ObSelectStmt *unchanged_data_stmt = NULL;
  ObSelectStmt *deleted_data_stmt = NULL;
  if (OB_FAIL(create_simple_stmt(union_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_unchanged_data_access_stmt(source_table, unchanged_data_stmt))) {
    LOG_WARN("failed to unchanged deleted data access stmt ", K(ret));
  } else if (OB_FAIL(gen_deleted_data_access_stmt(source_table, deleted_data_stmt))) {
    LOG_WARN("failed to generate deleted data access stmt ", K(ret));
  } else if (OB_FAIL(union_stmt->get_set_query().push_back(unchanged_data_stmt) ||
                     OB_FAIL(union_stmt->get_set_query().push_back(deleted_data_stmt)))) {
    LOG_WARN("failed to set set query", K(ret));
  } else {
    union_stmt->assign_set_all();
    union_stmt->assign_set_op(ObSelectStmt::UNION);
    access_sel = union_stmt;
  }
  return ret;
}

int ObMVPrinter::gen_unchanged_data_access_stmt(const TableItem &source_table,
                                                ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  access_sel = NULL;
  TableItem *table = NULL;
  ObRawExpr *anti_filter = NULL;
  const uint64_t access_sel_flags = 0;
  if (OB_FAIL(create_simple_stmt(access_sel))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(access_sel, source_table.table_name_, table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FALSE_IT(set_info_for_simple_table_item(*table, source_table))) {
  } else if (OB_FAIL(gen_delta_table_view_select_list(*table, source_table, *access_sel, access_sel_flags))) {
    LOG_WARN("failed to generate delta table view select lists", K(ret));
  } else if (OB_FAIL(gen_exists_cond_for_table(&source_table, table, false, false, anti_filter))) {
    LOG_WARN("failed to create simple column exprs", K(ret));
  } else if (OB_FAIL(access_sel->get_condition_exprs().push_back(anti_filter))) {
    LOG_WARN("failed to push back anti filter", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_deleted_data_access_stmt(const TableItem &source_table,
                                              ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  access_sel = NULL;
  const uint64_t mlog_sel_flags = MLOG_EXT_COL_OLD_NEW | MLOG_EXT_COL_SEQ | MLOG_EXT_COL_WIN_MIN_SEQ;
  const uint64_t access_sel_flags = 0;
  ObSelectStmt *mlog_delta_sel = NULL;
  TableItem *cur_table = NULL;
  if (OB_FAIL(gen_delta_table_view(source_table, mlog_delta_sel, mlog_sel_flags))) {
    LOG_WARN("failed to gen delta table view", K(ret));
  } else if (OB_FAIL(create_simple_stmt(access_sel))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(access_sel, DELTA_TABLE_VIEW_NAME, cur_table, mlog_delta_sel))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_delta_table_view_select_list(*cur_table, source_table, *access_sel, access_sel_flags))) {
    LOG_WARN("failed to generate delta table view select lists", K(ret));
  } else if (OB_FAIL(append_old_new_row_filter(*cur_table, access_sel->get_condition_exprs(), true, false))) {
    LOG_WARN("failed to append old new row filter ", K(ret));
  }
  return ret;
}


int ObMVPrinter::gen_refresh_select_for_major_refresh_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObSelectStmt *left_delta_stmt = NULL;
  ObSelectStmt *right_delta_stmt = NULL;
  ObSelectStmt *validation_stmt = NULL;
  ObSEArray<int64_t, 8> rowkey_sel_pos;
  if (OB_FAIL(get_rowkey_pos_in_select(rowkey_sel_pos))) {
    LOG_WARN("failed to get rowkey pos in select", K(ret));
  } else if (OB_FAIL(gen_one_refresh_select_for_major_refresh_mjv(rowkey_sel_pos, true, left_delta_stmt))) {
    LOG_WARN("failed to generate gen left refresh select for major refresh mjv ", K(ret));
  } else if (OB_FAIL(gen_one_refresh_select_for_major_refresh_mjv(rowkey_sel_pos, false, right_delta_stmt))) {
    LOG_WARN("failed to generate gen right refresh select for major refresh mjv ", K(ret));
  } else if (OB_FAIL(gen_refresh_validation_select_for_major_refresh_mjv(rowkey_sel_pos, validation_stmt))) {
    LOG_WARN("failed to generate refresh validation select for major refresh mjv ", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(left_delta_stmt))
             || OB_FAIL(dml_stmts.push_back(right_delta_stmt))
             || OB_FAIL(dml_stmts.push_back(validation_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObMVPrinter::set_refresh_table_scan_flag_for_mr_mv(ObSelectStmt &refresh_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  TableItem *table = NULL;
  if (OB_UNLIKELY(refresh_stmt.get_select_items().empty())
      || OB_ISNULL(expr = refresh_stmt.get_select_items().at(refresh_stmt.get_select_item_size() - 1).expr_)) {

  } else if (T_PSEUDO_OLD_NEW_COL != expr->get_expr_type()
             || 2 != refresh_stmt.get_table_items().count()
             || !refresh_stmt.has_order_by()
             || refresh_stmt.has_group_by()
             || refresh_stmt.get_order_item_size() >= refresh_stmt.get_select_item_size()) {
    /* do noting */
  } else if (OB_ISNULL(table = refresh_stmt.get_table_item_by_id(static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table", K(ret));
  } else {
    table->mr_mv_flags_ = common::ObQueryFlag::RefreshMode;
    LOG_INFO("set refresh mode for major refresh mview refresh query", K(table->get_table_name()));
  }
  return ret;
}

int ObMVPrinter::get_rowkey_pos_in_select(ObIArray<int64_t> &rowkey_sel_pos)
{
  int ret = OB_SUCCESS;
  rowkey_sel_pos.reuse();
  const ObTableSchema *container_table = NULL;
  if (OB_ISNULL(stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt_factory_.get_query_ctx()));
  } else if (OB_FAIL(stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(mv_schema_.get_data_table_id(), container_table))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(container_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(container_table));
  } else {
    const ObRowkeyInfo &rowkey_info = container_table->get_rowkey_info();
    const ObRowkeyColumn *rowkey_column = NULL;
    const int64_t sel_count = mv_checker_.get_stmt().get_select_items().count();
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_ISNULL(rowkey_column = rowkey_info.get_column(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The rowkey column is NULL, ", K(i));
      } else if (OB_UNLIKELY(0 > (pos = rowkey_column->column_id_ - OB_APP_MIN_COLUMN_ID) || sel_count <= pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowkey column", K(pos), K(rowkey_column->column_id_));
      } else if (OB_FAIL(rowkey_sel_pos.push_back(pos))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    LOG_TRACE("finish get rowkey pos in select", K(rowkey_sel_pos));
  }
  return ret;
}

/*
  select  mv.t1_pk,
          case when delta_t1.`OLD_NEW$$` is NULL then mv.t1_c1 else t1.c1 end as t1_c1,
          ...
          case when delta_t2.`OLD_NEW$$` is NULL then mv.t2_pk else t2.pk end as t2_pk,
          case when delta_t2.`OLD_NEW$$` is NULL then mv.t2_c1 else t2.c1 end as t2_c1,
          ...
  from mv as of snapshot last_refresh_scn(xx)
    left join (select t2.*, t2.`OLD_NEW$$` from t2 with delete where t2.ora_rowscn > last_refresh_scn(xx)) delta_t2
      on mv.t1_c1 = delta_t2.c1
    left join (select t1.*, t1.`OLD_NEW$$` from t1 with delete where t1.ora_rowscn > last_refresh_scn(xx)) delta_t1
      on mv.t1_pk = delta_t1.pk
  where (delta_t2.`OLD_NEW$$` = 'N' or delta_t2.`OLD_NEW$$` is NULL)
      and (delta_t1.`OLD_NEW$$` = 'N' or delta_t1.`OLD_NEW$$` is NULL)

  union all
    select ...
    from t1, t2
    where t1.c1 = t2.c1
      and not exists (select 1 from mv as of snapshot last_refresh_scn where t1.pk = mv.t1_pk);
*/
int ObMVPrinter::gen_real_time_view_for_major_refresh_mjv(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  ObSelectStmt *access_mv_stmt = NULL;
  ObSelectStmt *delta_left_stmt = NULL;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_mr_rt_mv_access_mv_data_stmt(access_mv_stmt))) {
    LOG_WARN("failed to generate major refresh real time view access mv data stmt", K(ret));
  } else if (OB_FAIL(gen_mr_rt_mv_left_delta_data_stmt(delta_left_stmt))) {
    LOG_WARN("failed to generate major refresh real time view left delta data stmt", K(ret));
  } else if (OB_FAIL(sel_stmt->get_set_query().push_back(access_mv_stmt))
             || OB_FAIL(sel_stmt->get_set_query().push_back(delta_left_stmt))) {
    LOG_WARN("failed to set set query", K(ret));
  } else {
    sel_stmt->assign_set_all();
    sel_stmt->assign_set_op(ObSelectStmt::UNION);
  }
  return ret;
}

int ObMVPrinter::set_real_time_table_scan_flag_for_mr_mv(ObSelectStmt &rt_mv_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *left_stmt = NULL;
  ObSelectStmt *right_stmt = NULL;
  TableItem *table = NULL;
  if (OB_UNLIKELY(!rt_mv_stmt.is_set_stmt() || 2 != rt_mv_stmt.get_set_query().count())
      || OB_ISNULL(left_stmt = rt_mv_stmt.get_set_query().at(0))
      || OB_ISNULL(right_stmt = rt_mv_stmt.get_set_query().at(1))
      || OB_UNLIKELY(2 != right_stmt->get_table_items().count())
      || OB_ISNULL(table = right_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret), K(rt_mv_stmt.is_set_stmt()), K(rt_mv_stmt.get_set_query().count()),
                                K(left_stmt), K(right_stmt), K(table));
  } else {
    right_stmt->get_table_item(0)->mr_mv_flags_ = MR_MV_RT_QUERY_LEADING_TABLE_FLAG;
    LOG_TRACE("set leading table flag for major refresh mview real time query",
                                              K(right_stmt->get_table_item(0)->get_table_name()));
    TableItem *base_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < left_stmt->get_table_items().count(); ++i) {
      if (OB_ISNULL(table = left_stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret), K(i));
      } else if (!table->is_generated_table()) {
        // do nothing
      } else if (OB_ISNULL(table->ref_query_)
                 || OB_UNLIKELY(1 != table->ref_query_->get_table_items().count())
                 || OB_ISNULL(base_table = table->ref_query_->get_table_items().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect tables", K(ret));
      } else {
        base_table->mr_mv_flags_ = common::ObQueryFlag::RealTimeMode;
        LOG_TRACE("set real time mode for major refresh mview real time query", K(base_table->get_table_name()));
      }
    }
  }
  return ret;
}

int ObMVPrinter::fill_table_partition_name(const TableItem &src_table,
                                           TableItem &table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *src_schema = NULL;
  table.part_names_.reuse();
  table.part_ids_.reuse();
  ObPartition *part = NULL;
  ObSubPartition *subpart = NULL;
  int64_t part_idx = NULL != major_refresh_info_ ? major_refresh_info_->part_idx_ : OB_INVALID_INDEX;
  int64_t sub_part_idx = NULL != major_refresh_info_ ? major_refresh_info_->sub_part_idx_ : OB_INVALID_INDEX;
  if (OB_ISNULL(stmt_factory_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt_factory_.get_query_ctx()));
  } else if (OB_INVALID_INDEX == part_idx && OB_INVALID_INDEX == sub_part_idx) {
    /* do nothing */
  } else if (OB_FAIL(stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(src_table.ref_id_, src_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(src_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(src_schema));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != src_schema->get_part_level()
                         && PARTITION_LEVEL_TWO != src_schema->get_part_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table", K(ret), K(part_idx), K(sub_part_idx), KPC(src_schema));
  } else if (OB_UNLIKELY(part_idx < 0 || part_idx >= src_schema->get_partition_capacity())
             || OB_ISNULL(part = src_schema->get_part_array()[part_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition", K(ret), K(part_idx), K(part), KPC(src_schema));
  } else if (OB_FAIL(table.part_ids_.push_back(part_idx))) { // just push a invalid part id to print partition name
    LOG_WARN("failed to push back", K(ret));
  } else if (PARTITION_LEVEL_ONE == src_schema->get_part_level()) {
    if (OB_FAIL(table.part_names_.push_back(part->get_part_name()))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else if (OB_UNLIKELY(sub_part_idx < 0 || sub_part_idx >= part->get_subpartition_num())
             || OB_ISNULL(subpart = part->get_subpart_array()[sub_part_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected subpartition", K(ret), K(sub_part_idx), KPC(part), K(subpart), KPC(src_schema));
  } else if (OB_FAIL(table.part_names_.push_back(subpart->get_part_name()))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObMVPrinter::append_rowkey_range_filter(const ObIArray<SelectItem> &select_items,
                                            uint64_t rowkey_count,
                                            ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  const ObNewRange *range = NULL != major_refresh_info_ ? &major_refresh_info_->range_ : NULL;
  ObOpRawExpr *start_const_row = NULL;
  ObOpRawExpr *end_const_row = NULL;
  ObOpRawExpr *start_col_row = NULL;
  ObOpRawExpr *end_col_row = NULL;
  if (NULL == range) {
    /* do nothing */
  } else if (OB_UNLIKELY(select_items.count() < rowkey_count
                         || rowkey_count < range->start_key_.get_obj_cnt()
                         || rowkey_count <  range->end_key_.get_obj_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(select_items.count()), KPC(range));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_OP_ROW, start_const_row))
             || OB_FAIL(expr_factory_.create_raw_expr(T_OP_ROW, end_const_row))
             || OB_FAIL(expr_factory_.create_raw_expr(T_OP_ROW, start_col_row))
             || OB_FAIL(expr_factory_.create_raw_expr(T_OP_ROW, end_col_row))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(start_const_row) || OB_ISNULL(end_const_row)
             || OB_ISNULL(start_col_row) || OB_ISNULL(end_col_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create raw expr", K(ret));
  } else {
    ObRawExpr *filter = NULL;
    uint64_t s_cnt = range->start_key_.get_obj_cnt();
    uint64_t e_cnt = range->end_key_.get_obj_cnt();
    const ObObj *s_obj = range->start_key_.get_obj_ptr();
    const ObObj *e_obj = range->end_key_.get_obj_ptr();
    ObItemType cmp_type1 = range->border_flag_.inclusive_start() ? T_OP_GE : T_OP_GT;
    ObItemType cmp_type2 = range->border_flag_.inclusive_end() ? T_OP_LE : T_OP_LT;
    ObConstRawExpr *const_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < s_cnt && !s_obj[i].is_min_value(); ++i) {
      if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(expr_factory_, s_obj[i], const_expr))) {
        LOG_WARN("failed to build const obj expr", K(ret));
      } else if (OB_FAIL(start_col_row->add_param_expr(select_items.at(i).expr_))
                 || OB_FAIL(start_const_row->add_param_expr(const_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < e_cnt && !e_obj[i].is_max_value(); ++i) {
      if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(expr_factory_, e_obj[i], const_expr))) {
        LOG_WARN("failed to build const obj expr", K(ret));
      } else if (OB_FAIL(end_col_row->add_param_expr(select_items.at(i).expr_))
                 || OB_FAIL(end_const_row->add_param_expr(const_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 < start_col_row->get_param_count() &&
               (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, cmp_type1, start_col_row, start_const_row, filter))
                || OB_FAIL(conds.push_back(filter)))) {
      LOG_WARN("failed to build and push back binary op expr", K(ret));
    } else if (0 < end_col_row->get_param_count() &&
               (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, cmp_type2, end_col_row, end_const_row, filter))
                || OB_FAIL(conds.push_back(filter)))) {
      LOG_WARN("failed to build and push back binary op expr", K(ret));
    }
  }
  return ret;
}

/*
  generate stmt:
  select  mv.t1_pk,
          case when delta_t1.`OLD_NEW$$` is NULL then mv.t1_c1 else t1.c1 end as t1_c1,
          ...
          case when delta_t2.`OLD_NEW$$` is NULL then mv.t2_pk else t2.pk end as t2_pk,
          case when delta_t2.`OLD_NEW$$` is NULL then mv.t2_c1 else t2.c1 end as t2_c1,
          ...
  from mv as of snapshot last_refresh_scn(xx)
    left join (select t2.*, t2.`OLD_NEW$$` from t2 with delete where t2.ora_rowscn > last_refresh_scn(xx)) delta_t2
      on mv.t1_c1 = delta_t2.c1
    left join (select t1.*, t1.`OLD_NEW$$` from t1 with delete where t1.ora_rowscn > last_refresh_scn(xx)) delta_t1
      on mv.t1_pk = delta_t1.pk
  where (delta_t2.`OLD_NEW$$` = 'N' or delta_t2.`OLD_NEW$$` is NULL)
      and (delta_t1.`OLD_NEW$$` = 'N' or delta_t1.`OLD_NEW$$` is NULL)
*/
int ObMVPrinter::gen_mr_rt_mv_access_mv_data_stmt(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *mv_table = NULL;
  const TableItem *orig_left_table = NULL;
  TableItem *delta_left_table = NULL;
  const TableItem *orig_right_table = NULL;
  TableItem *delta_right_table = NULL;
  ObSelectStmt *view_stmt = NULL;
  ObRawExpr *expr = NULL;
  if (OB_UNLIKELY(2 != mv_checker_.get_stmt().get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_checker_.get_stmt().get_table_item(0))
      || OB_ISNULL(orig_right_table = mv_checker_.get_stmt().get_table_item(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_checker_.get_stmt().get_table_items()));
  } else if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, mv_schema_.get_table_name(), mv_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_mr_rt_mv_delta_stmt(*orig_left_table, view_stmt))
             || OB_FAIL(create_simple_table_item(sel_stmt, orig_left_table->get_table_name(),
                                                 delta_left_table, view_stmt, false))) {
    LOG_WARN("failed to create delta left table", K(ret));
  } else if (OB_FAIL(create_mr_rt_mv_delta_stmt(*orig_right_table, view_stmt))
             || OB_FAIL(create_simple_table_item(sel_stmt, orig_right_table->get_table_name(),
                                                 delta_right_table, view_stmt, false))) {
    LOG_WARN("failed to create delta right table", K(ret));
  } else if (OB_FAIL(create_mr_rt_mv_access_mv_from_table(*sel_stmt, *mv_table, *delta_left_table, *delta_right_table))) {
    LOG_WARN("failed to create major refresh real time mview access mv from table", K(ret));
  } else if (OB_FAIL(append_old_new_col_filter(*delta_left_table, sel_stmt->get_condition_exprs()))
             || OB_FAIL(append_old_new_col_filter(*delta_right_table, sel_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to build old_new filter", K(ret));
  } else if (OB_FAIL(gen_mr_rt_mv_access_mv_data_select_list(*sel_stmt, *mv_table, *delta_left_table, *delta_right_table))) {
    LOG_WARN("failed to generate major refresh real time mview access mv select list", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    mv_table->flashback_query_expr_ = exprs_.last_refresh_scn_;
    mv_table->flashback_query_type_ = TableItem::USING_SCN;
  }
  return ret;
}

/*
  select t.*, `OLD_NEW$$` from t with delete where ora_rowscn > last_refresh_scn(xx)
*/
int ObMVPrinter::create_mr_rt_mv_delta_stmt(const TableItem &orig_table, ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *table = NULL;
  ObSEArray<ColumnItem, 8> column_items;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, orig_table.table_name_, table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(mv_checker_.get_stmt().get_column_items(orig_table.table_id_, column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else if (OB_FAIL(sel_stmt->get_select_items().prepare_allocate(column_items.count() + 1))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else {
    table->alias_name_ = orig_table.alias_name_;
    table->synonym_name_ = orig_table.synonym_name_;
    table->database_name_ = orig_table.database_name_;
    table->synonym_db_name_ = orig_table.synonym_db_name_;
    ObRawExpr *expr = NULL;
    ObRawExpr *scn_gt = NULL;
    ObIArray<SelectItem> &select_items = sel_stmt->get_select_items();
    SelectItem &item = select_items.at(column_items.count());
    item.is_real_alias_ = true;
    item.alias_name_ = OLD_NEW_COL_NAME;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      if (OB_FAIL(create_simple_column_expr(table->get_table_name(), column_items.at(i).column_name_,
                                            table->table_id_, select_items.at(i).expr_))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else {
        select_items.at(i).is_real_alias_ = true;
        select_items.at(i).alias_name_ = column_items.at(i).column_name_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_simple_column_expr(table->get_object_name(), OLD_NEW_COL_NAME, table->table_id_, item.expr_))) {
      LOG_WARN("failed to create old new column", K(ret));
    } else if (OB_FAIL(create_simple_column_expr(table->get_object_name(), ObString("ORA_ROWSCN"), table->table_id_, expr))
               || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_GT, expr, exprs_.last_refresh_scn_, scn_gt))
               || OB_FAIL(sel_stmt->get_condition_exprs().push_back(scn_gt))) {
      LOG_WARN("failed to build greater op expr", K(ret));
    }
  }
  return ret;
}

int ObMVPrinter::create_mr_rt_mv_access_mv_from_table(ObSelectStmt &sel_stmt,
                                                      const TableItem &mv_table,
                                                      const TableItem &delta_left_table,
                                                      const TableItem &delta_right_table)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(expr_factory_);
  JoinedTable *mv_join_delta_right = NULL;
  JoinedTable *mv_join_delta_left = NULL;
  ObSEArray<ObRawExpr*, 8> all_conds;
  ObSEArray<ObRawExpr*, 8> exprs;
  ObRawExpr *expr = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<JoinedTable*> &orig_joined_tables = mv_checker_.get_stmt().get_joined_tables();
  const TableItem *orig_left_table = NULL;
  const TableItem *orig_right_table = NULL;
  if (OB_UNLIKELY(2 != mv_checker_.get_stmt().get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_checker_.get_stmt().get_table_item(0))
      || OB_ISNULL(orig_right_table = mv_checker_.get_stmt().get_table_item(1))
      || OB_UNLIKELY(1 != orig_joined_tables.count()) || OB_ISNULL(orig_joined_tables.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected major refresh mview", K(ret), K(mv_checker_.get_stmt().get_table_items()),
                                              K(orig_joined_tables));
  } else if (OB_FAIL(append(all_conds, orig_joined_tables.at(0)->get_join_conditions()))
             || OB_FAIL(append(all_conds, mv_checker_.get_stmt().get_condition_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(all_conds, exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret), K(all_conds));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    expr = NULL;
    if (OB_ISNULL(col_expr = dynamic_cast<ObColumnRefRawExpr*>(exprs.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(exprs));
    } else if (col_expr->get_table_id() == orig_right_table->table_id_) {
      if (OB_FAIL(create_simple_column_expr(delta_right_table.get_object_name(),
                                            col_expr->get_column_name(),
                                            delta_right_table.table_id_,
                                            expr))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      }
    } else if (OB_UNLIKELY(col_expr->get_table_id() != orig_left_table->table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column", K(ret), KPC(col_expr), KPC(orig_left_table), KPC(orig_right_table));
    } else {
      for(int64_t j = 0; OB_SUCC(ret) && NULL == expr && j < orig_select_items.count(); ++j) {
        if (orig_select_items.at(j).expr_ == col_expr
            && OB_FAIL(create_simple_column_expr(mv_table.get_object_name(),
                                                 orig_select_items.at(j).alias_name_,
                                                 mv_table.table_id_, expr))) {
          LOG_WARN("failed to create simple column expr", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(i), K(exprs));
    } else if (OB_FAIL(copier.add_replaced_expr(col_expr, expr))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_joined_table_item(&sel_stmt, ObJoinType::LEFT_OUTER_JOIN, mv_table, delta_right_table, false, mv_join_delta_right))) {
    LOG_WARN("failed to create joined table item", K(ret));
  } else if (OB_FAIL(create_joined_table_item(&sel_stmt, ObJoinType::LEFT_OUTER_JOIN, *mv_join_delta_right, delta_left_table, true, mv_join_delta_left))) {
    LOG_WARN("failed to create joined table item", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(all_conds, mv_join_delta_right->get_join_conditions()))) {
    LOG_WARN("failed to copy on replace exprs", K(ret), K(all_conds));
  } else if (OB_FAIL(gen_rowkey_join_conds_for_table(*orig_left_table, delta_left_table,
                                                     mv_table, true, mv_join_delta_left->get_join_conditions()))) {
    LOG_WARN("failed to generate rowkey join conds for table", K(ret));
  }
  return ret;
}

int ObMVPrinter::gen_mr_rt_mv_access_mv_data_select_list(ObSelectStmt &sel_stmt,
                                                         const TableItem &mv_table,
                                                         const TableItem &delta_left_table,
                                                         const TableItem &delta_right_table)
{
  int ret = OB_SUCCESS;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  ObIArray<SelectItem> &select_items = sel_stmt.get_select_items();
  ObSEArray<int64_t, 8> rowkey_sel_pos;
  ObRawExpr *expr = NULL;
  ObRawExpr *left_is_null_filter = NULL;
  ObRawExpr *right_is_null_filter = NULL;
  const TableItem *orig_left_table = NULL;
  if (OB_UNLIKELY(2 != mv_checker_.get_stmt().get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_checker_.get_stmt().get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_checker_.get_stmt().get_table_items()));
  } else if (OB_FAIL(get_rowkey_pos_in_select(rowkey_sel_pos))) {
    LOG_WARN("failed to get rowkey pos in select", K(ret));
  } else if (OB_FAIL(sel_stmt.get_select_items().prepare_allocate(orig_select_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(delta_left_table.get_object_name(), OLD_NEW_COL_NAME, delta_left_table.table_id_, expr))
             || OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory_, expr, false, left_is_null_filter))) {
    LOG_WARN("failed to build left is null expr", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(delta_right_table.get_object_name(), OLD_NEW_COL_NAME, delta_right_table.table_id_, expr))
             || OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory_, expr, false, right_is_null_filter))) {
    LOG_WARN("failed to build right is null expr", K(ret));
  } else {
    const ObColumnRefRawExpr *col_expr = NULL;
    const TableItem *table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
      select_items.at(i).is_real_alias_ = true;
      select_items.at(i).alias_name_ = orig_select_items.at(i).alias_name_;
      table = &delta_right_table;
      if (OB_ISNULL(col_expr = dynamic_cast<ObColumnRefRawExpr*>(orig_select_items.at(i).expr_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(i), K(orig_select_items));
      } else if (OB_FAIL(create_simple_column_expr(mv_table.get_object_name(), orig_select_items.at(i).alias_name_,
                                                   mv_table.table_id_, select_items.at(i).expr_))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else if (ObOptimizerUtil::find_item(rowkey_sel_pos, i) && col_expr->get_table_id() == orig_left_table->table_id_) {
        /* left table rowkey, use value from mview */
      } else if (col_expr->get_table_id() == orig_left_table->table_id_ && OB_FALSE_IT(table = &delta_left_table)) {
      } else if (OB_FAIL(create_simple_column_expr(table->get_object_name(),
                                                   col_expr->get_column_name(),
                                                   table->table_id_,
                                                   expr))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                              (col_expr->get_table_id() == orig_left_table->table_id_
                                                               ? left_is_null_filter : right_is_null_filter),
                                                              select_items.at(i).expr_,
                                                              expr,
                                                              select_items.at(i).expr_))) {
        LOG_WARN("failed to build case when expr", K(ret));
      }
    }
  }
  return ret;
}

/*
  generate delat_t1 join t2 stmt:
    select * from t1, t2
    where t1.c1 = t2.c1
      and not exists (select 1 from mv as of snapshot last_refresh_scn where t1.pk = mv.t1_pk);
*/
int ObMVPrinter::gen_mr_rt_mv_left_delta_data_stmt(ObSelectStmt *&stmt)
{
  int ret = OB_SUCCESS;
  stmt = NULL;
  ObRawExpr *exists_expr = NULL;
  ObSEArray<int64_t, 1> dummy_array;
  if (OB_FAIL(create_simple_stmt(stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(prepare_gen_access_delta_data_for_major_refresh_mjv(dummy_array, *stmt))) {
    LOG_WARN("failed to prepare generate access delta data for major refresh mjv", K(ret));
  } else if (OB_UNLIKELY(2 != stmt->get_table_items().count()
                         || 2 != mv_checker_.get_stmt().get_table_items().count())
             || OB_ISNULL(stmt->get_table_item(0))
             || OB_ISNULL(mv_checker_.get_stmt().get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(stmt->get_table_items()));
  } else if (OB_FAIL(gen_exists_cond_for_mview(*mv_checker_.get_stmt().get_table_item(0),
                                               *stmt->get_table_item(0), exists_expr))) {
    LOG_WARN("failed to generate exists filter", K(ret));
  } else if (OB_FAIL(stmt->get_condition_exprs().push_back(exists_expr))) {
    LOG_WARN("failed to push back", K(ret));

  }
  return ret;
}

/*
  generate stmt:
  1. delat_t1 join pre_t2
    select pk1, pk2, ..., t1.$$old_new from t1 with delete as of snapshot current_scn, t2 as of snapshot last_scn
    where t1.c1 = t2.c1
        and t1.ora_rowscn > last_scn
    order by 1,2;
  2. t1 join delta_t2
    select pk1, pk2, ..., t2.$$old_new from t1 as of snapshot current_scn, t2 with delete as of snapshot current_scn
    where t1.c1 = t2.c1
        and t2.ora_rowscn > last_scn
    order by 1,2;
*/
int ObMVPrinter::gen_one_refresh_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                              const bool is_delta_left,
                                                              ObSelectStmt *&delta_stmt)
{
  int ret = OB_SUCCESS;
  delta_stmt = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  const TableItem *orig_left_table = NULL;
  TableItem *delta_table = NULL;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  ObRawExpr *col = NULL;
  ObRawExpr *scn_filter = NULL;
  SelectItem sel_item;
  sel_item.is_real_alias_ = true;
  sel_item.alias_name_ = OLD_NEW_COL_NAME;
  if (OB_UNLIKELY(2 != orig_table_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(orig_table_items));
  } else if (OB_FAIL(create_simple_stmt(delta_stmt))
             || OB_FAIL(prepare_gen_access_delta_data_for_major_refresh_mjv(rowkey_sel_pos, *delta_stmt))) {
    LOG_WARN("failed to prepare generate access delta data for major refresh mjv", K(ret));
  } else if (OB_UNLIKELY(2 != delta_stmt->get_table_items().count() || 2 != mv_checker_.get_stmt().get_table_items().count())
             || OB_ISNULL(orig_left_table = mv_checker_.get_stmt().get_table_item(0))
             || OB_ISNULL(left_table = delta_stmt->get_table_item(0))
             || OB_ISNULL(right_table = delta_stmt->get_table_item(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  } else if (OB_FALSE_IT(delta_table = is_delta_left ? left_table : right_table)) {
  } else if (OB_FAIL(create_simple_column_expr(delta_table->get_table_name(), ObString("ORA_ROWSCN"), delta_table->table_id_, col))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_GT, col, exprs_.last_refresh_scn_, scn_filter))
             || OB_FAIL(delta_stmt->get_condition_exprs().push_back(scn_filter))) {
    LOG_WARN("failed to add filter for delta left stmt", K(ret));
  } else if (OB_FAIL(fill_table_partition_name(*orig_left_table, *left_table))) {
    LOG_WARN("failed to fill table partition name", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(delta_table->get_table_name(), OLD_NEW_COL_NAME, delta_table->table_id_, sel_item.expr_))
             || OB_FAIL(delta_stmt->get_select_items().push_back(sel_item))) {
    LOG_WARN("failed to add old_new to select items", K(ret));
  } else if (OB_FAIL(gen_refresh_select_hint_for_major_refresh_mjv(*left_table, *right_table, delta_stmt->get_stmt_hint()))) {
    LOG_WARN("failed to gen refresh select hint for major refresh mjv", K(ret));
  } else {
    left_table->flashback_query_expr_ = exprs_.refresh_scn_;
    left_table->flashback_query_type_ = TableItem::USING_SCN;
    right_table->flashback_query_expr_ = is_delta_left ? exprs_.last_refresh_scn_ : exprs_.refresh_scn_;
    right_table->flashback_query_type_ = TableItem::USING_SCN;
  }
  return ret;
}

/*
  generate validation stmt:
    select count(*) cnt from t1 as of snapshot current_scn, t2 as of snapshot current_scn
    where t1.c1 = t2.c1;
*/
int ObMVPrinter::gen_refresh_validation_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                                     ObSelectStmt *&delta_stmt)
{
  int ret = OB_SUCCESS;
  delta_stmt = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  const TableItem *orig_left_table = NULL;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  ObAggFunRawExpr *aggr_expr = NULL;
  if (OB_UNLIKELY(2 != orig_table_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(orig_table_items));
  } else if (OB_FAIL(create_simple_stmt(delta_stmt))
             || OB_FAIL(prepare_gen_access_delta_data_for_major_refresh_mjv(rowkey_sel_pos, *delta_stmt))) {
    LOG_WARN("failed to prepare generate access delta data for major refresh mjv", K(ret));
  } else if (OB_UNLIKELY(2 != delta_stmt->get_table_items().count() || 2 != mv_checker_.get_stmt().get_table_items().count())
             || OB_ISNULL(orig_left_table = mv_checker_.get_stmt().get_table_item(0))
             || OB_ISNULL(left_table = delta_stmt->get_table_item(0))
             || OB_ISNULL(right_table = delta_stmt->get_table_item(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  } else if (OB_FAIL(fill_table_partition_name(*orig_left_table, *left_table))) {
    LOG_WARN("failed to fill table partition name", K(ret));
  } else if (OB_FAIL(gen_refresh_select_hint_for_major_refresh_mjv(*left_table, *right_table, delta_stmt->get_stmt_hint()))) {
    LOG_WARN("failed to gen refresh select hint for major refresh mjv", K(ret));
  } else if (OB_FALSE_IT(delta_stmt->get_select_items().reset())) {
  } else if (OB_FAIL(delta_stmt->get_select_items().prepare_allocate(1))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc_, ObString("CNT"), delta_stmt->get_select_item(0).alias_name_))) {
    LOG_WARN("ob_write_string failed", K(ret));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_COUNT, aggr_expr))) {
    LOG_WARN("create ObAggFunRawExpr failed", K(ret));
  } else {
    delta_stmt->get_order_items().reset();
    delta_stmt->get_select_item(0).is_real_alias_ = true;
    delta_stmt->get_select_item(0).expr_ = aggr_expr;
    left_table->flashback_query_expr_ = exprs_.refresh_scn_;
    left_table->flashback_query_type_ = TableItem::USING_SCN;
    right_table->flashback_query_expr_ = exprs_.refresh_scn_;
    right_table->flashback_query_type_ = TableItem::USING_SCN;
  }
  return ret;
}

int ObMVPrinter::gen_refresh_select_hint_for_major_refresh_mjv(const TableItem &left_table,
                                                               const TableItem &right_table,
                                                               ObStmtHint &stmt_hint)
{
  int ret = OB_SUCCESS;
  ObJoinOrderHint *leading_left = NULL;
  ObJoinHint *use_nl_right = NULL;
  ObIndexHint *left_full_hint = NULL;
  ObIndexHint *right_full_hint = NULL;
  ObSEArray<ObItemType, 1> conflict_hints;
  if (OB_FAIL(ObQueryHint::create_hint(&alloc_, T_LEADING, leading_left)) ||
      OB_FAIL(ObQueryHint::create_hint(&alloc_, T_FULL_HINT, left_full_hint)) ||
      OB_FAIL(ObQueryHint::create_hint(&alloc_, T_FULL_HINT, right_full_hint)) ||
      OB_FAIL(ObQueryHint::create_hint(&alloc_, T_USE_NL, use_nl_right))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint_table(&alloc_, leading_left->get_table().table_))) {
      LOG_WARN("fail to create hint table", K(ret));
  } else if (OB_FALSE_IT(leading_left->get_table().table_->set_table(left_table))) {
  } else if (OB_FALSE_IT(left_full_hint->get_table().set_table(left_table))) {
  } else if (OB_FALSE_IT(right_full_hint->get_table().set_table(right_table))) {
  } else if (OB_FAIL(use_nl_right->get_tables().push_back(ObTableInHint(right_table)))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(stmt_hint.merge_hint(*leading_left, ObHintMergePolicy::HINT_DOMINATED_EQUAL, conflict_hints))
             || OB_FAIL(stmt_hint.merge_hint(*left_full_hint, ObHintMergePolicy::HINT_DOMINATED_EQUAL, conflict_hints))
             || OB_FAIL(stmt_hint.merge_hint(*right_full_hint, ObHintMergePolicy::HINT_DOMINATED_EQUAL, conflict_hints))
             || OB_FAIL(stmt_hint.merge_hint(*use_nl_right, ObHintMergePolicy::HINT_DOMINATED_EQUAL, conflict_hints))) {
    LOG_WARN("failed to merge hint", K(ret));
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

int ObMVPrinter::prepare_gen_access_delta_data_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                                     ObSelectStmt &base_delta_stmt)
{
  int ret = OB_SUCCESS;
  const ObIArray<SelectItem> &orig_select_items = mv_checker_.get_stmt().get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_checker_.get_stmt().get_table_items();
  ObIArray<SelectItem> &select_items = base_delta_stmt.get_select_items();
  ObIArray<OrderItem> &order_items = base_delta_stmt.get_order_items();
  ObSEArray<ColumnItem, 8> column_items;
  ObRawExpr *expr = NULL;
  const TableItem *orig_table = NULL;
  TableItem *table = NULL;
  ObRawExprCopier copier(expr_factory_);
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!for_rt_expand_ && rowkey_sel_pos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(rowkey_sel_pos));
  } else if (OB_FAIL(select_items.prepare_allocate(orig_select_items.count()))
             || OB_FAIL(order_items.prepare_allocate(rowkey_sel_pos.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
    column_items.reuse();
    if (OB_ISNULL(orig_table = orig_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(orig_table_items));
    } else if (OB_FAIL(mv_checker_.get_stmt().get_column_items(orig_table->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(create_simple_table_item(&base_delta_stmt, orig_table->table_name_, table))) {
      LOG_WARN("failed to create simple table item", K(ret));
    } else {
      table->alias_name_ = orig_table->alias_name_;
      table->synonym_name_ = orig_table->synonym_name_;
      table->database_name_ = orig_table->database_name_;
      table->synonym_db_name_ = orig_table->synonym_db_name_;
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_items.count(); ++j) {
      if (OB_FAIL(create_simple_column_expr(table->get_table_name(), column_items.at(j).column_name_,
                                            table->table_id_, expr))) {
        LOG_WARN("failed to create simple column expr", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(column_items.at(j).expr_, expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(base_delta_stmt.deep_copy_join_tables(alloc_, copier, mv_checker_.get_stmt()))) {
    LOG_WARN("failed to deep copy join tables", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_checker_.get_stmt().get_condition_exprs(), base_delta_stmt.get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (OB_FAIL(base_delta_stmt.get_from_items().assign(mv_checker_.get_stmt().get_from_items()))) {
    LOG_WARN("failed to assign from items", K(ret));
  }

  // for non joined table, adjust table id in from item
  ObIArray<FromItem> &from_items = base_delta_stmt.get_from_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
    if (from_items.at(i).is_joined_) {
      /* do nothing */
    } else if (OB_FAIL(mv_checker_.get_stmt().get_table_item_idx(from_items.at(i).table_id_, idx))) {
      LOG_WARN("failed to get table item", K(ret));
    } else if (OB_UNLIKELY(idx < 0 || idx >= base_delta_stmt.get_table_size())
                || OB_ISNULL(base_delta_stmt.get_table_item(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(base_delta_stmt.get_table_size()));
    } else {
      from_items.at(i).table_id_ = base_delta_stmt.get_table_item(idx)->table_id_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (for_rt_expand_) {  // for rt-mview access, generate original select list
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(orig_select_items.at(i).expr_, select_items.at(i).expr_))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else {
        select_items.at(i).is_real_alias_ = true;
        select_items.at(i).alias_name_ = orig_select_items.at(i).alias_name_;
      }
    }
  } else {  // for refresh, generate select list as pk and other column, add order by
    int64_t idx = OB_INVALID_INDEX;
    int64_t pos = rowkey_sel_pos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(orig_select_items.at(i).expr_, expr))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (ObOptimizerUtil::find_item(rowkey_sel_pos, i, &idx)) {
        order_items.at(idx).expr_ = expr;
        select_items.at(idx).expr_ = expr;
        select_items.at(idx).alias_name_ = orig_select_items.at(i).alias_name_;
        select_items.at(idx).is_real_alias_ = true;
      } else {
        select_items.at(pos).expr_ = expr;
        select_items.at(pos).alias_name_ = orig_select_items.at(i).alias_name_;
        select_items.at(pos).is_real_alias_ = true;
        ++pos;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(append_rowkey_range_filter(select_items,
                                                           rowkey_sel_pos.count(),
                                                           base_delta_stmt.get_condition_exprs()))) {
      LOG_WARN("failed to append rowkey range filter", K(ret));
    }
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
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_EQ, col, exprs_.str_n_, equal_filter))) {
    LOG_WARN("failed to build equal expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(expr_factory_, col, false, is_null_filter))) {
    LOG_WARN("failed to build is null expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_, T_OP_OR, equal_filter, is_null_filter, filter))) {
    LOG_WARN("failed to build or expr", K(ret));
  } else if (OB_FAIL(conds.push_back(filter))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase

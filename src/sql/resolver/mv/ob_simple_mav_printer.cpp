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
#include "sql/resolver/mv/ob_simple_mav_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObSimpleMAVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  const TableItem *source_table = NULL;
  ObMergeStmt *merge_stmt = NULL;
  if (OB_UNLIKELY(1 != mv_def_stmt_.get_table_size()
      || OB_ISNULL(source_table = mv_def_stmt_.get_table_item(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt", K(ret), K(mv_def_stmt_.get_table_items()));
  } else if (is_table_skip_refresh(*source_table)) {
    // do nothing, no need to gen refresh dmls
  } else if (lib::is_mysql_mode()) {
    if (OB_FAIL(gen_update_insert_delete_for_simple_mav(dml_stmts))) {
      LOG_WARN("failed to gen update insert delete for simple mav", K(ret));
    }
  } else if (OB_FAIL(gen_merge_for_simple_mav(merge_stmt))) {
    LOG_WARN("failed to gen merge into for simple mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(merge_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

// generate update/insert/delete dml to fast refresh simple mav in mysql mode
int ObSimpleMAVPrinter::gen_update_insert_delete_for_simple_mav(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObUpdateStmt *update_stmt = NULL;
  ObInsertStmt *insert_stmt = NULL;
  ObDeleteStmt *delete_stmt = NULL;
  ObSelectStmt *delta_mv_stmt = NULL;
  if (OB_FAIL(gen_simple_mav_delta_mv_view(delta_mv_stmt))) {
    LOG_WARN("failed gen simple source stmt", K(ret));
  } else if (OB_FAIL(gen_insert_for_mav(delta_mv_stmt, insert_stmt))) {
    LOG_WARN("failed to gen insert for mav", K(ret));
  } else if (OB_FAIL(gen_update_for_mav(delta_mv_stmt, update_stmt))) {
    LOG_WARN("failed to gen update for mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(update_stmt))  // pushback and execute in this ordering
             || OB_FAIL(dml_stmts.push_back(insert_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (mv_def_stmt_.is_scala_group_by()) {
    /* no need delete for scalar group by */
  } else if (OB_FAIL(gen_delete_for_mav(delete_stmt))) {
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
int ObSimpleMAVPrinter::gen_insert_for_mav(ObSelectStmt *delta_mv_stmt,
                                           ObInsertStmt *&insert_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  ObSelectStmt *sel_stmt = NULL;
  ObRawExpr *filter_expr = NULL;
  ObSEArray<ObRawExpr*, 16> values;
  if (OB_FAIL(create_simple_stmt(sel_stmt))
      || OB_FAIL(create_simple_stmt(insert_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, DELTA_BASIC_MV_VIEW_NAME, source_table, delta_mv_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_simple_table_item(insert_stmt, mv_schema_.get_table_name(), target_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_insert_values_and_desc(target_table,
                                                source_table,
                                                insert_stmt->get_values_desc(),
                                                values))) {
    LOG_WARN("failed to gen insert values and desc", K(ret), K(*target_table), K(*source_table));
  } else if (OB_FAIL(create_simple_table_item(insert_stmt, DELTA_MV_VIEW_NAME, source_table, sel_stmt))) {
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

int ObSimpleMAVPrinter::gen_select_for_insert_subquery(const ObIArray<ObRawExpr*> &values,
                                                      ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  SelectItem sel_item;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
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
int ObSimpleMAVPrinter::gen_exists_cond_for_insert(const ObIArray<ObRawExpr*> &values,
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
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_def_stmt_.get_group_exprs();
  if (OB_UNLIKELY(values.count() != select_items.count())) {
    LOG_WARN("unexpected params", K(ret), K(values.count()), K(select_items.count()));
  } else if (OB_FAIL(create_simple_stmt(subquery))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT_EXISTS, not_exists_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(not_exists_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr), K(not_exists_expr));
  } else if (OB_FAIL(not_exists_expr->set_param_expr(query_ref_expr))) {
    LOG_WARN("failed to set param expr", K(ret));
  } else if (OB_FAIL(conds.push_back(not_exists_expr))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else if (OB_FAIL(create_simple_table_item(subquery, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(subquery->get_select_items().push_back(sel_item))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  } else if (OB_FAIL(add_union_all_child_refresh_filter_if_needed(subquery, mv_table))) {
    LOG_WARN("failed to add union all child refresh filter", K(ret));
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
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_NSEQ, values.at(i), expr, match_cond))) {
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
int ObSimpleMAVPrinter::gen_update_for_mav(ObSelectStmt *delta_mv_stmt,
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
  } else if (OB_ISNULL((ptr = ctx_.alloc_.alloc(sizeof(ObUpdateTableInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table info", K(ret));
  } else if (OB_FALSE_IT(table_info = new(ptr)ObUpdateTableInfo())) {
  } else if (OB_FAIL(update_stmt->get_update_table_info().push_back(table_info))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(create_simple_table_item(update_stmt, mv_schema_.get_table_name(), target_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(create_simple_table_item(update_stmt, DELTA_BASIC_MV_VIEW_NAME, source_table, delta_mv_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_update_assignments(*target_table, *source_table, table_info->assignments_))) {
    LOG_WARN("failed gen update assignments", K(ret));
  } else if (OB_FAIL(gen_update_conds(*target_table, *source_table, update_stmt->get_condition_exprs()))) {
    LOG_WARN("failed gen update conds", K(ret));
  } else if (OB_FAIL(add_union_all_child_refresh_filter_if_needed(update_stmt, target_table))) {
    LOG_WARN("failed to add union all child refresh filter", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_update_conds(const TableItem &target_table,
                                         const TableItem &source_table,
                                         ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_def_stmt_.get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  ObRawExpr *target_col = NULL;
  ObRawExpr *source_col = NULL;
  ObRawExpr *cond = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (!ObOptimizerUtil::find_item(group_by_exprs, select_items.at(i).expr_)) {
      /* do nothing */
    } else if (OB_FAIL(create_simple_column_expr(target_table.get_table_name(), select_items.at(i).alias_name_, target_table.table_id_, target_col))
               || OB_FAIL(create_simple_column_expr(source_table.get_table_name(), select_items.at(i).alias_name_, source_table.table_id_, source_col))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_NSEQ, target_col, source_col, cond))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
    } else if (OB_FAIL(conds.push_back(cond))) {
      LOG_WARN("failed to push back null safe equal expr", K(ret));
    }
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_delete_for_mav(ObDeleteStmt *&delete_stmt)
{
  int ret = OB_SUCCESS;
  delete_stmt = NULL;
  TableItem *target_table = NULL;
  if (OB_FAIL(create_simple_stmt(delete_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(delete_stmt, mv_schema_.get_table_name(), target_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_delete_conds(*target_table, delete_stmt->get_condition_exprs()))) {
    LOG_WARN("failed gen update conds", K(ret));
  } else if (OB_FAIL(add_union_all_child_refresh_filter_if_needed(delete_stmt, target_table))) {
    LOG_WARN("failed to add union all child refresh filter", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_delete_conds(const TableItem &target_table,
                                         ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  conds.reuse();
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  ObRawExpr *expr = NULL;
  ObRawExpr *target_col = NULL;
  ObRawExpr *cond = NULL;
  for (int64_t i = 0; NULL == cond && OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (T_FUN_COUNT != expr->get_expr_type() ||
               0 != static_cast<ObAggFunRawExpr*>(expr)->get_real_param_count()) {
      /* do nothing */
    } else if (OB_FAIL(create_simple_column_expr(target_table.get_table_name(), select_items.at(i).alias_name_,
                                                 target_table.table_id_, target_col))) {
          LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, target_col, exprs_.int_zero_, cond))) {
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
int ObSimpleMAVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
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
  } else if (OB_FAIL(gen_select_items_for_mav(*view_table, sel_stmt->get_select_items()))) {
    LOG_WARN("failed to generate select items for mav", K(ret));
  } else if (!mv_def_stmt_.is_scala_group_by()
             && OB_FAIL(gen_real_time_view_filter_for_mav(*sel_stmt))) {
    LOG_WARN("failed to generate real time view filter", K(ret));
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_real_time_view_filter_for_mav(ObSelectStmt &sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt.get_condition_exprs().reuse();
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
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
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_GT, select_items.at(idx).expr_, exprs_.int_zero_, filter_expr))) {
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
int ObSimpleMAVPrinter::gen_inner_real_time_view_for_mav(ObSelectStmt *&inner_rt_view)
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

int ObSimpleMAVPrinter::gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs)
{
  int ret = OB_SUCCESS;
  inner_delta_mavs.reuse();
  ObSelectStmt *inner_delta_mav = NULL;
  if (OB_FAIL(gen_simple_mav_delta_mv_view(inner_delta_mav))) {
    LOG_WARN("failed gen simple mav delta mv view", K(ret));
  } else if (OB_FAIL(inner_delta_mavs.push_back(inner_delta_mav))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_merge_for_simple_mav(ObMergeStmt *&merge_stmt)
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
int ObSimpleMAVPrinter::gen_merge_for_simple_mav_use_delta_view(ObSelectStmt *delta_mav, ObMergeStmt *&merge_stmt)
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
  } else if (OB_FAIL(create_simple_table_item(merge_stmt, DELTA_BASIC_MV_VIEW_NAME, source_table, delta_mav, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FALSE_IT(merge_stmt->set_target_table_id(target_table->table_id_))) {
  } else if (OB_FAIL(gen_insert_values_and_desc(target_table,
                                                source_table,
                                                merge_stmt->get_values_desc(),
                                                merge_stmt->get_values_vector()))) {
    LOG_WARN("failed to gen insert values and desc", K(ret), K(*target_table), K(*source_table));
  } else if (OB_FAIL(gen_update_assignments(*target_table,
                                            *source_table,
                                            merge_stmt->get_table_assignments()))) {
    LOG_WARN("failed to gen update assignments", K(ret));
  } else if (OB_FAIL(gen_merge_conds(*merge_stmt))) {
    LOG_WARN("failed to gen merge conds", K(ret));
  } else {
    target_table->database_name_ = mv_db_name_;
    merge_stmt->set_source_table_id(source_table->table_id_);
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_insert_values_and_desc(const TableItem *target_table,
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
    } else if (OB_FAIL(gen_select_items_for_mav(*source_table, select_items))) {
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
int ObSimpleMAVPrinter::gen_select_items_for_mav(const TableItem &table,
                                                ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_def_stmt_.get_group_exprs();
  ObRawExpr *orig_expr = NULL;
  ObRawExpr *expr = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
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
    } else if (copier.is_existed(orig_expr)) {
      // do nothing
    } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), select_item.alias_name_, table.table_id_, select_item.expr_))) {
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
    } else if (T_FUN_SUM != orig_expr->get_expr_type()) {
      // do nothing
    } else if (copier.is_existed(orig_expr)) {
      // do nothing
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(orig_expr, orig_select_items, idx))) {
      LOG_WARN("failed to get dependent aggr of fun sum", K(ret));
    } else if (OB_UNLIKELY(0 > idx || select_items.count() <= idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(select_items.count()));
    } else if (OB_FAIL(create_simple_column_expr(table.get_table_name(), orig_select_items.at(i).alias_name_, table.table_id_, expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(gen_calc_expr_for_insert_clause_sum(select_items.at(idx).expr_, expr, select_item.expr_))) {
      LOG_WARN("failed to gen calc expr for aggr sum", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(orig_expr, select_item.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }

  // 3. add other select exprs
  if (OB_SUCC(ret)) {
    const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs = get_expand_aggrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < expand_aggrs.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(expand_aggrs.at(i).second, expr))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(expand_aggrs.at(i).first, expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    if (OB_SUCC(ret) && add_replaced_expr_for_min_max_aggr(table, copier)) {
      LOG_WARN("failed to add replace pair for min max aggr", K(ret));
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
int ObSimpleMAVPrinter::gen_calc_expr_for_insert_clause_sum(ObRawExpr *source_count,
                                                            ObRawExpr *source_sum,
                                                            ObRawExpr *&calc_sum)
{
  int ret = OB_SUCCESS;
  calc_sum = NULL;
  ObRawExpr *equal_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, source_count, exprs_.int_zero_, equal_expr))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(ctx_.expr_factory_, equal_expr, exprs_.null_expr_, source_sum, calc_sum))) {
      LOG_WARN("failed to build case when expr", K(ret));
  }
  return ret;
}

//  sum(c3)
//  --> d_cnt_c3 and d_sum_c3 is output from merge source_table view
//  sum_c3 = case when cnt_c3 + d_cnt_c3 = 0 then null
//                else nvl(d_sum_c3, 0) + nvl(sum_c3) end,
int ObSimpleMAVPrinter::gen_calc_expr_for_update_clause_sum(ObRawExpr *sum_count,
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
  if (OB_ISNULL(sum_count) || OB_ISNULL(target_sum) || OB_ISNULL(source_sum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(sum_count), K(target_sum), K(source_sum));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_CASE, case_when_expr))) {
    LOG_WARN("create add expr failed", K(ret));
  } else if (OB_ISNULL(case_when_expr) || OB_ISNULL(exprs_.null_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(case_when_expr));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, sum_count, exprs_.int_zero_, when_expr))
             || OB_FAIL(case_when_expr->add_when_param_expr(when_expr))
             || OB_FAIL(case_when_expr->add_then_param_expr(exprs_.null_expr_))) {
    LOG_WARN("failed to build and add when/then exprs", K(ret));
  } else if (OB_FAIL(add_nvl_above_exprs(target_sum, exprs_.int_zero_, nvl_target_sum))
             || OB_FAIL(add_nvl_above_exprs(source_sum, exprs_.int_zero_, nvl_source_sum))) {
    LOG_WARN("failed to add nvl above exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_add_expr(ctx_.expr_factory_, nvl_target_sum, nvl_source_sum, add_expr))) {
    LOG_WARN("failed to build add expr", K(ret));
  } else {
    case_when_expr->set_default_param_expr(add_expr);
    calc_sum = case_when_expr;
  }
  return ret;
}

int ObSimpleMAVPrinter::get_dependent_aggr_of_fun_sum(const ObRawExpr *expr,
                                               const ObIArray<SelectItem> &select_items,
                                               int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  const ObAggFunRawExpr *dep_aggr = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(T_FUN_SUM != expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), KPC(expr));
  } else if (OB_FAIL(ObMVChecker::get_dependent_aggr_of_fun_sum(mv_def_stmt_,
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

// generate update values in 3 steps:
// 1. count aggr
// 2. sum aggr
// 3. other select exprs except group by exprs
int ObSimpleMAVPrinter::gen_update_assignments(const TableItem &target_table,
                                               const TableItem &source_table,
                                               ObIArray<ObAssignment> &assignments)
{
  int ret = OB_SUCCESS;
  assignments.reuse();
  ObRawExprCopier copier(ctx_.expr_factory_);
  ObRawExpr *expr = NULL;
  ObRawExpr *target_col = NULL;
  ObRawExpr *source_col = NULL;
  ObOpRawExpr *op_expr = NULL;
  ObRawExpr *add_expr = NULL;
  const ObIArray<ObRawExpr*> &group_by_exprs = mv_def_stmt_.get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
  ObAssignment assign;
  ObSEArray<ObAssignment, 4> inner_assigns;
  ObSqlBitSet<> visited_select_items;
  // 1. add count aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (!ObOptimizerUtil::find_item(group_by_exprs, expr)
               && T_FUN_COUNT != expr->get_expr_type()) {
      // do nothing
    } else if (copier.is_existed(expr)) {
      // do nothing
    } else if (OB_FAIL(create_simple_column_expr(target_table.get_table_name(), select_items.at(i).alias_name_,
                                                 target_table.table_id_, target_col))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (T_FUN_COUNT != expr->get_expr_type()) {  // group by expr
      if (OB_FAIL(copier.add_replaced_expr(expr, target_col))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    } else if (OB_FAIL(create_simple_column_expr(source_table.get_table_name(), select_items.at(i).alias_name_,
                                                 source_table.table_id_, source_col))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_add_expr(ctx_.expr_factory_, target_col, source_col, op_expr))) {
      LOG_WARN("failed to build add expr", K(ret));
    } else if (OB_FALSE_IT(assign.column_expr_ = static_cast<ObColumnRefRawExpr*>(target_col))) {
    } else if (OB_FALSE_IT(assign.expr_ = op_expr)) {
    } else if (OB_FAIL(inner_assigns.push_back(assign))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(expr, assign.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(visited_select_items.add_member(i))) {
      LOG_WARN("failed to set bit", K(ret));
    }
  }

  // 2. add sum aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    const ObString &col_name = select_items.at(i).alias_name_;
    if (OB_ISNULL(expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
    } else if (T_FUN_SUM != expr->get_expr_type()) {
      // do nothing
    } else if (copier.is_existed(expr)) {
      // do nothing
    } else if (OB_FAIL(get_dependent_aggr_of_fun_sum(expr, select_items, idx))) {
      LOG_WARN("failed to get dependent aggr of fun sum", K(ret));
    } else if (OB_UNLIKELY(0 > idx || select_items.count() <= idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(select_items.count()));
    } else if (OB_FAIL(create_simple_column_expr(target_table.get_table_name(), col_name, target_table.table_id_, target_col))
               || OB_FAIL(create_simple_column_expr(source_table.get_table_name(), col_name, source_table.table_id_, source_col))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FALSE_IT(assign.column_expr_ = static_cast<ObColumnRefRawExpr*>(target_col))) {
    } else if (OB_FAIL(copier.copy_on_replace(select_items.at(idx).expr_, add_expr))) {
      LOG_WARN("failed to generate group by exprs", K(ret));
    } else if (OB_FAIL(gen_calc_expr_for_update_clause_sum(add_expr, target_col, source_col, assign.expr_))) {
      LOG_WARN("failed to gen calc expr for aggr sum", K(ret));
    } else if (OB_FAIL(inner_assigns.push_back(assign))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(expr, assign.expr_))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(visited_select_items.add_member(i))) {
      LOG_WARN("failed to set bit", K(ret));
    }
  }

  // 3. other select exprs except group by exprs
  if (OB_SUCC(ret)) {
    const ObIArray<std::pair<ObAggFunRawExpr*, ObRawExpr*>> &expand_aggrs = get_expand_aggrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < expand_aggrs.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(expand_aggrs.at(i).second, expr))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(expand_aggrs.at(i).first, expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    if (OB_SUCC(ret) && add_replaced_expr_for_min_max_aggr(source_table, copier)) {
      LOG_WARN("failed to add replace pair for min max aggr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (visited_select_items.has_member(i)) {
        // do nothing, already added into inner_assigns
      } else if (OB_ISNULL(expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected select item", K(ret), K(i), K(select_items));
      } else if (expr->is_const_raw_expr()
                 || ObOptimizerUtil::find_item(group_by_exprs, expr)) {
        // do nothing, no need to add
      } else if (OB_FAIL(copier.copy_on_replace(expr, assign.expr_))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(create_simple_column_expr(target_table.get_table_name(), select_items.at(i).alias_name_,
                                                   target_table.table_id_, target_col))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (OB_FALSE_IT(assign.column_expr_ = static_cast<ObColumnRefRawExpr*>(target_col))) {
      } else if (OB_FAIL(inner_assigns.push_back(assign))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !inner_assigns.empty()) {
    if (!lib::is_mysql_mode()) {
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
int ObSimpleMAVPrinter::gen_merge_conds(ObMergeStmt &merge_stmt)
{
  int ret = OB_SUCCESS;
  merge_stmt.get_match_condition_exprs().reuse();
  merge_stmt.get_insert_condition_exprs().reuse();
  merge_stmt.get_delete_condition_exprs().reuse();

  const ObIArray<ObRawExpr*> &group_by_exprs = mv_def_stmt_.get_group_exprs();
  const ObIArray<SelectItem> &select_items = mv_def_stmt_.get_select_items();
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
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_NSEQ, target_columns.at(i), values_exprs.at(i), match_cond))) {
      LOG_WARN("failed to build null safe equal expr", K(ret));
    } else if (OB_FAIL(merge_stmt.get_match_condition_exprs().push_back(match_cond))) {
      LOG_WARN("failed to push back null safe equal expr", K(ret));
    }
  }

  // add insert/delete condition
  if (OB_SUCC(ret) && !mv_def_stmt_.is_scala_group_by()) {
    ObRawExpr *insert_cond = NULL;
    ObRawExpr *delete_cond = NULL;
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_NE, insert_count_expr, exprs_.int_zero_, insert_cond))
               || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_EQ, delete_count_expr, exprs_.int_zero_, delete_cond))) {
      LOG_WARN("failed to build equal expr", K(ret));
    } else if (OB_FAIL(merge_stmt.get_insert_condition_exprs().push_back(insert_cond))
               || OB_FAIL(merge_stmt.get_delete_condition_exprs().push_back(delete_cond))) {
      LOG_WARN("failed to push back equal expr", K(ret));
    }
  }

  // add union all child query marker filter
  if (OB_SUCC(ret) && ctx_.for_union_all_child_query()) {
    ObRawExpr *marker_filter = NULL;
    const TableItem *target_table = NULL;
    if (OB_ISNULL(target_table = merge_stmt.get_table_item_by_id(merge_stmt.get_target_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(merge_stmt.get_target_table_id()), K(merge_stmt.get_table_items()));
    } else if (OB_FAIL(create_union_all_child_refresh_filter(target_table, marker_filter))) {
      LOG_WARN("failed to create union all child refresh filter", K(ret));
    } else if (OB_FAIL(merge_stmt.get_match_condition_exprs().push_back(marker_filter))) {
      LOG_WARN("failed to push back null safe equal expr", K(ret));
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
int ObSimpleMAVPrinter::gen_simple_mav_delta_mv_view(ObSelectStmt *&view_stmt)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  ObString empty_name;
  ObSelectStmt *delta_view = NULL;
  const TableItem *source_table = NULL;
  TableItem *table_item = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  if (OB_UNLIKELY(1 != mv_def_stmt_.get_table_size() ||
      OB_ISNULL(source_table = mv_def_stmt_.get_table_item(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret), K(mv_def_stmt_));
  } else if (OB_FAIL(create_simple_stmt(view_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_delta_mlog_table_view(*source_table, delta_view))) {
    LOG_WARN("failed to gen delta table view", K(ret));
  } else if (OB_FAIL(create_simple_table_item(view_stmt, DELTA_TABLE_VIEW_NAME, table_item, delta_view))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*view_stmt, copier))) {
    LOG_WARN("failed to init expr copier", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_group_exprs(),
                                            view_stmt->get_group_exprs()))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier, table_item, 0,
                                                         view_stmt->get_group_exprs(),
                                                         view_stmt->get_select_items()))) {
    LOG_WARN("failed to gen select list ", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(), view_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to convert conds exprs", K(ret));
  } else if (OB_FAIL(append_old_new_row_filter(*table_item, view_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to append old new row filter ", K(ret));
  }
  return ret;
}

int ObSimpleMAVPrinter::gen_simple_mav_delta_mv_select_list(ObRawExprCopier &copier,
                                                            const TableItem *table,
                                                            const int64_t explicit_dml_factor,
                                                            const ObIArray<ObRawExpr*> &group_by_exprs,
                                                            ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_def_stmt_.get_group_exprs();
  const ObIArray<ObAggFunRawExpr*> &orig_aggr_exprs = mv_def_stmt_.get_aggr_items();
  ObRawExpr *dml_factor = NULL;
  ObAggFunRawExpr *aggr_expr = NULL;
  if (OB_UNLIKELY(orig_group_by_exprs.count() != group_by_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected group by size", K(ret), K(group_by_exprs), K(orig_group_by_exprs));
  } else if (NULL == table) {
    if (OB_UNLIKELY(1 != explicit_dml_factor && -1 != explicit_dml_factor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected explicit dml factor", K(ret), K(explicit_dml_factor));
    } else {
      dml_factor = 1 == explicit_dml_factor ? exprs_.int_one_ : exprs_.int_neg_one_;
    }
  } else if (OB_FAIL(create_simple_column_expr(table->get_table_name(), DML_FACTOR_COL_NAME, table->table_id_, dml_factor))) {
    LOG_WARN("failed to create simple column expr", K(ret));
  }
  // add select list for group by
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    const SelectItem &ori_select_item = mv_def_stmt_.get_select_item(i);
    SelectItem sel_item;
    int64_t group_by_idx = -1;
    if (!ObOptimizerUtil::find_item(orig_group_by_exprs, ori_select_item.expr_, &group_by_idx)) {
      // do nothing
    } else if (OB_UNLIKELY(group_by_idx < 0 || group_by_idx >= group_by_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected group by idx", K(ret), K(group_by_idx), K(ori_select_item));
    } else {
      sel_item.expr_ = group_by_exprs.at(group_by_idx);
      sel_item.alias_name_ = ori_select_item.alias_name_;
      sel_item.is_real_alias_ = true;
      if (OB_FAIL(select_items.push_back(sel_item))) {
        LOG_WARN("failed to pushback", K(ret));
      }
    }
  }
  // add select list for basic aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_aggr_exprs.count(); ++i) {
    SelectItem sel_item;
    sel_item.is_real_alias_ = true;
    if (OB_ISNULL(aggr_expr = orig_aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(aggr_expr));
    } else if ((T_FUN_MIN == aggr_expr->get_expr_type() || T_FUN_MAX == aggr_expr->get_expr_type())
               && !ctx_.for_rt_expand()) {
      if (OB_FAIL(get_inner_sel_name_for_aggr(*aggr_expr, sel_item.alias_name_))) {
        LOG_WARN("failed to get inner select name for aggr", K(ret));
      } else if (OB_FAIL(gen_min_max_aggr_print_expr(group_by_exprs, *aggr_expr, sel_item.expr_))) {
        LOG_WARN("failed to gen min max aggr expr", K(ret));
      } else if (OB_FAIL(select_items.push_back(sel_item))) {
        LOG_WARN("failed to pushback", K(ret));
      }
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
int ObSimpleMAVPrinter::gen_simple_join_mav_basic_select_list(const TableItem &table,
                                                              ObIArray<SelectItem> &select_items,
                                                              ObIArray<ObRawExpr*> *group_by_exprs)
{
  int ret = OB_SUCCESS;
  select_items.reuse();
  const bool stmt_need_aggr = NULL != group_by_exprs;
  if (NULL != group_by_exprs) {
    group_by_exprs->reuse();
  }
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<ObRawExpr*> &orig_group_by_exprs = mv_def_stmt_.get_group_exprs();
  const ObIArray<ObAggFunRawExpr*> &orig_aggr_exprs = mv_def_stmt_.get_aggr_items();
  ObAggFunRawExpr *aggr_expr = NULL;
  ObRawExpr *col_expr = NULL;
  // 1. add select list for group by
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_select_items.count(); ++i) {
    SelectItem sel_item;
    sel_item.is_real_alias_ = true;
    if (!ObOptimizerUtil::find_item(orig_group_by_exprs, orig_select_items.at(i).expr_)) {
      // do nothing
    } else if (OB_FALSE_IT(sel_item.alias_name_ = orig_select_items.at(i).alias_name_)) {
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
    } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SUM, aggr_expr))) {
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

// count(*)   --> sum(dml_factor)
// count(expr)  --> sum(dml_factor * (case when expr is null then 0 else 1 end))
// sum(expr)  --> sum(dml_factor * expr)
int ObSimpleMAVPrinter::gen_basic_aggr_expr(ObRawExprCopier &copier,
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
    if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(ctx_.expr_factory_, param, false, is_null))) {
      LOG_WARN("failed to build is null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(ctx_.expr_factory_, is_null, exprs_.int_zero_, exprs_.int_one_, case_when))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_MUL, dml_factor, case_when, print_param))) {
      LOG_WARN("failed to build mul expr", K(ret));
    }
  } else if (T_FUN_SUM == aggr_expr.get_expr_type()) {
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_MUL, dml_factor, param, print_param))) {
      LOG_WARN("failed to build mul expr", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggr", K(ret), K(aggr_expr));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SUM, new_aggr_expr))) {
    LOG_WARN("create ObAggFunRawExpr failed", K(ret));
  } else if (OB_ISNULL(new_aggr_expr) || OB_ISNULL(print_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(new_aggr_expr), K(print_param));
  } else if (OB_FAIL(new_aggr_expr->add_real_param_expr(print_param))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (T_FUN_COUNT != aggr_expr.get_expr_type() || !mv_def_stmt_.is_scala_group_by()) {
    aggr_print_expr = new_aggr_expr;
  } else if (OB_FAIL(add_nvl_above_exprs(new_aggr_expr, exprs_.int_zero_, aggr_print_expr))) {
    //  for scalar group by, d_cnt from sum(dml_factor) may get null, need convert null to 0
    //  count(*) --> nvl(d_cnt, 0)
    //  count(c3) --> nvl(d_cnt_3, 0)
    LOG_WARN("failed to gen calc expr for scalar count", K(ret));
  }
  return ret;
}

int ObSimpleMAVPrinter::add_nvl_above_exprs(ObRawExpr *expr, ObRawExpr *default_expr, ObRawExpr *&res_expr)
{
  int ret = OB_SUCCESS;
  res_expr = NULL;
  ObSysFunRawExpr *nvl_expr = NULL;
  if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_NVL, nvl_expr))) {
    LOG_WARN("fail to create nvl expr", K(ret));
  } else if (OB_ISNULL(expr) || OB_ISNULL(default_expr) || OB_ISNULL(nvl_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(default_expr), K(nvl_expr));
  } else if (OB_FAIL(nvl_expr->set_param_exprs(expr, default_expr))) {
    LOG_WARN("fail to set param exprs", K(ret));
  } else {
    nvl_expr->set_expr_type(T_FUN_SYS_NVL);
    nvl_expr->set_func_name(ObString::make_string(N_NVL));
    res_expr = nvl_expr;
  }
  return ret;
}

int ObSimpleMAVPrinter::add_replaced_expr_for_min_max_aggr(const TableItem &source_table,
                                                           ObRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAggFunRawExpr*> &orig_aggr_exprs = mv_def_stmt_.get_aggr_items();
  ObAggFunRawExpr *orig_aggr = NULL;
  ObRawExpr *print_expr = NULL;
  ObString col_name;
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_aggr_exprs.count(); ++i) {
    if (OB_ISNULL(orig_aggr = orig_aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(orig_aggr));
    } else if (T_FUN_MIN != orig_aggr->get_expr_type()
                && T_FUN_MAX != orig_aggr->get_expr_type()) {
      // do nothing
    } else if (copier.is_existed(orig_aggr)) {
      // do nothing
    } else if (ctx_.for_rt_expand()) {
      ObSEArray<ObRawExpr*, 16> cur_group_by_exprs;
      if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_group_exprs(), cur_group_by_exprs))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(gen_min_max_aggr_print_expr(cur_group_by_exprs, *orig_aggr, print_expr))) {
        LOG_WARN("failed to gen min max aggr expr", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(orig_aggr, print_expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    } else if (OB_FAIL(get_inner_sel_name_for_aggr(*orig_aggr, col_name))) {
      LOG_WARN("failed to get inner select name for aggr", K(ret));
    } else if (OB_FAIL(create_simple_column_expr(source_table.get_table_name(), col_name,
                                                 source_table.table_id_,
                                                 print_expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(orig_aggr, print_expr))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
  }
  return ret;
}

int ObSimpleMAVPrinter::get_inner_sel_name_for_aggr(const ObAggFunRawExpr &aggr, ObString &sel_name)
{
  int ret = OB_SUCCESS;
  sel_name.reset();
  const ObRawExpr *aggr_param = NULL;
  if (OB_FAIL(get_mv_select_item_name(&aggr, sel_name, true))) {
    LOG_WARN("failed to get mv select item name", K(ret));
  } else if (!sel_name.empty()) {
    /* do nothing */
  } else if (OB_UNLIKELY(T_FUN_MIN != aggr.get_expr_type() && T_FUN_MAX != aggr.get_expr_type())
             || OB_UNLIKELY(1 != aggr.get_real_param_count())
             || OB_ISNULL(aggr_param = aggr.get_param_expr(0))
             || OB_UNLIKELY(!aggr_param->is_column_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggr", K(ret), K(aggr));
  } else {
    const ObString &param_name = static_cast<const ObColumnRefRawExpr*>(aggr_param)->get_column_name();
    const char* name_fmt = T_FUN_MIN == aggr.get_expr_type()
                           ? "MIN_%.*s$$" : "MAX_%.*s$$";
    if (OB_FAIL(gen_format_string_name(name_fmt, param_name, sel_name, OB_MAX_COLUMN_NAME_BUF_LENGTH))) {
      LOG_WARN("failed to generate format string name", K(ret));
    }
  }
  return ret;
}

// max(expr)  --> (select max(expr) from base_table where base_table.gby_expr1 <=> gby_expr1 and ...)
// min(expr)  --> (select min(expr) from base_table where base_table.gby_expr1 <=> gby_expr1 and ...)
int ObSimpleMAVPrinter::gen_min_max_aggr_print_expr(const ObIArray<ObRawExpr*> &outer_group_by_exprs,
                                                    ObAggFunRawExpr &aggr_expr,
                                                    ObRawExpr *&aggr_print_expr)
{
  int ret = OB_SUCCESS;
  aggr_print_expr = NULL;
  const TableItem *source_table = NULL;
  TableItem *cur_table = NULL;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  ObSelectStmt *subquery = NULL;
  ObRawExpr *aggr_param = NULL;
  ObRawExpr *min_max_param = NULL;
  ObAggFunRawExpr *min_max_aggr = NULL;
  ObRawExpr *match_cond = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  if (OB_UNLIKELY(1 != mv_def_stmt_.get_table_size()
      || OB_ISNULL(source_table = mv_def_stmt_.get_table_item(0)))
      || OB_UNLIKELY(1 != aggr_expr.get_real_param_count())
      || OB_ISNULL(aggr_param = aggr_expr.get_param_expr(0))
      || OB_UNLIKELY(!aggr_param->is_column_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(aggr_expr), K(mv_def_stmt_.get_table_size()),
                                  K(source_table), KPC(aggr_param));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, query_ref_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(query_ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr));
  } else if (OB_FAIL(create_simple_stmt(subquery))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(subquery, source_table->table_name_, cur_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FALSE_IT(set_info_for_simple_table_item(*cur_table, *source_table))) {
  } else if (OB_FAIL(init_expr_copier_for_stmt(*subquery, copier))) {
    LOG_WARN("failed to init expr copier", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(), subquery->get_condition_exprs()))) {
    LOG_WARN("failed to generate condition exprs", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(aggr_param, min_max_param))) {
    LOG_WARN("failed to generate min/max aggr params", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(aggr_expr.get_expr_type(), min_max_aggr))) {
    LOG_WARN("create ObAggFunRawExpr failed", K(ret));
  } else if (OB_ISNULL(min_max_param) || OB_ISNULL(min_max_aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(min_max_param), K(min_max_aggr));
  } else if (OB_FAIL(min_max_aggr->add_real_param_expr(min_max_param))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(subquery->get_select_items().prepare_allocate(1))) {
    LOG_WARN("fail to prepare allocate", K(ret));
  } else {
    SelectItem &sel_item = subquery->get_select_items().at(0);
    sel_item.expr_ = min_max_aggr;
    sel_item.is_real_alias_ = true;
    sel_item.alias_name_ = static_cast<ObColumnRefRawExpr*>(aggr_param)->get_column_name();
    query_ref_expr->set_ref_stmt(subquery);
    ObRawExpr *cur_col = NULL;
    const ObIArray<ObRawExpr*> &group_by_exprs = mv_def_stmt_.get_group_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_exprs.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(group_by_exprs.at(i), cur_col))) {
        LOG_WARN("failed to generate group by exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_,
                                                                     T_OP_NSEQ,
                                                                     outer_group_by_exprs.at(i),
                                                                     cur_col,
                                                                     match_cond))) {
        LOG_WARN("failed to build null safe equal expr", K(ret));
      } else if (OB_FAIL(subquery->get_condition_exprs().push_back(match_cond))) {
        LOG_WARN("failed to push back null safe equal expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      aggr_print_expr = query_ref_expr;
    }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase

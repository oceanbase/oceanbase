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
#include "sql/resolver/mv/ob_major_refresh_mjv_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObMajorRefreshMJVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
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

int ObMajorRefreshMJVPrinter::set_refresh_table_scan_flag_for_mr_mv(ObSelectStmt &refresh_stmt)
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

int ObMajorRefreshMJVPrinter::get_rowkey_pos_in_select(ObIArray<int64_t> &rowkey_sel_pos)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = mv_container_schema_.get_rowkey_info();
  const ObRowkeyColumn *rowkey_column = NULL;
  const int64_t sel_count = mv_def_stmt_.get_select_items().count();
  int64_t pos = 0;
  rowkey_sel_pos.reuse();
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
int ObMajorRefreshMJVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
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

int ObMajorRefreshMJVPrinter::set_real_time_table_scan_flag_for_mr_mv(ObSelectStmt &rt_mv_stmt)
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

int ObMajorRefreshMJVPrinter::fill_table_partition_name(const TableItem &src_table,
                                                        TableItem &table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *src_schema = NULL;
  table.part_names_.reuse();
  table.part_ids_.reuse();
  ObPartition *part = NULL;
  ObSubPartition *subpart = NULL;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t sub_part_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(ctx_.stmt_factory_.get_query_ctx()) || OB_ISNULL(ctx_.refresh_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.stmt_factory_.get_query_ctx()), K(ctx_.refresh_info_));
  } else if (OB_FALSE_IT(part_idx = ctx_.refresh_info_->part_idx_)
             || OB_FALSE_IT(sub_part_idx = ctx_.refresh_info_->sub_part_idx_)) {
  } else if (OB_INVALID_INDEX == part_idx && OB_INVALID_INDEX == sub_part_idx) {
    /* do nothing */
  } else if (OB_FAIL(ctx_.stmt_factory_.get_query_ctx()->sql_schema_guard_.get_table_schema(src_table.ref_id_, src_schema))) {
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

int ObMajorRefreshMJVPrinter::append_rowkey_range_filter(const ObIArray<SelectItem> &select_items,
                                                         uint64_t rowkey_count,
                                                         ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  const ObNewRange *range = NULL != ctx_.refresh_info_ ? ctx_.refresh_info_->range_ : NULL;
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
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, start_const_row))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, end_const_row))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, start_col_row))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, end_col_row))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(start_const_row) || OB_ISNULL(end_const_row)
             || OB_ISNULL(start_col_row) || OB_ISNULL(end_col_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(start_col_row->init_param_exprs(range->start_key_.get_obj_cnt()))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(start_const_row->init_param_exprs(range->start_key_.get_obj_cnt()))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(end_col_row->init_param_exprs(range->end_key_.get_obj_cnt()))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(end_const_row->init_param_exprs(range->end_key_.get_obj_cnt()))) {
    LOG_WARN("failed to init param exprs", K(ret));
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
      if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(ctx_.expr_factory_, s_obj[i], const_expr))) {
        LOG_WARN("failed to build const obj expr", K(ret));
      } else if (OB_FAIL(start_col_row->add_param_expr(select_items.at(i).expr_))
                 || OB_FAIL(start_const_row->add_param_expr(const_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < e_cnt && !e_obj[i].is_max_value(); ++i) {
      if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(ctx_.expr_factory_, e_obj[i], const_expr))) {
        LOG_WARN("failed to build const obj expr", K(ret));
      } else if (OB_FAIL(end_col_row->add_param_expr(select_items.at(i).expr_))
                 || OB_FAIL(end_const_row->add_param_expr(const_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 < start_col_row->get_param_count() &&
               (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, cmp_type1, start_col_row, start_const_row, filter))
                || OB_FAIL(conds.push_back(filter)))) {
      LOG_WARN("failed to build and push back binary op expr", K(ret));
    } else if (0 < end_col_row->get_param_count() &&
               (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, cmp_type2, end_col_row, end_const_row, filter))
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
int ObMajorRefreshMJVPrinter::gen_mr_rt_mv_access_mv_data_stmt(ObSelectStmt *&sel_stmt)
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
  if (OB_UNLIKELY(2 != mv_def_stmt_.get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_def_stmt_.get_table_item(0))
      || OB_ISNULL(orig_right_table = mv_def_stmt_.get_table_item(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_def_stmt_.get_table_items()));
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
int ObMajorRefreshMJVPrinter::create_mr_rt_mv_delta_stmt(const TableItem &orig_table, ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *table = NULL;
  ObSEArray<ColumnItem, 8> column_items;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, orig_table.table_name_, table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(mv_def_stmt_.get_column_items(orig_table.table_id_, column_items))) {
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
               || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_GT, expr, exprs_.last_refresh_scn_, scn_gt))
               || OB_FAIL(sel_stmt->get_condition_exprs().push_back(scn_gt))) {
      LOG_WARN("failed to build greater op expr", K(ret));
    }
  }
  return ret;
}

int ObMajorRefreshMJVPrinter::create_mr_rt_mv_access_mv_from_table(ObSelectStmt &sel_stmt,
                                                                   const TableItem &mv_table,
                                                                   const TableItem &delta_left_table,
                                                                   const TableItem &delta_right_table)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(ctx_.expr_factory_);
  JoinedTable *mv_join_delta_right = NULL;
  JoinedTable *mv_join_delta_left = NULL;
  ObSEArray<ObRawExpr*, 8> all_conds;
  ObSEArray<ObRawExpr*, 8> exprs;
  ObRawExpr *expr = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<JoinedTable*> &orig_joined_tables = mv_def_stmt_.get_joined_tables();
  const TableItem *orig_left_table = NULL;
  const TableItem *orig_right_table = NULL;
  if (OB_UNLIKELY(2 != mv_def_stmt_.get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_def_stmt_.get_table_item(0))
      || OB_ISNULL(orig_right_table = mv_def_stmt_.get_table_item(1))
      || OB_UNLIKELY(1 != orig_joined_tables.count()) || OB_ISNULL(orig_joined_tables.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected major refresh mview", K(ret), K(mv_def_stmt_.get_table_items()),
                                              K(orig_joined_tables));
  } else if (OB_FAIL(append(all_conds, orig_joined_tables.at(0)->get_join_conditions()))
             || OB_FAIL(append(all_conds, mv_def_stmt_.get_condition_exprs()))) {
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
                                                     mv_table, false, true, mv_join_delta_left->get_join_conditions()))) {
    LOG_WARN("failed to generate rowkey join conds for table", K(ret));
  }
  return ret;
}

int ObMajorRefreshMJVPrinter::gen_mr_rt_mv_access_mv_data_select_list(ObSelectStmt &sel_stmt,
                                                                      const TableItem &mv_table,
                                                                      const TableItem &delta_left_table,
                                                                      const TableItem &delta_right_table)
{
  int ret = OB_SUCCESS;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  ObIArray<SelectItem> &select_items = sel_stmt.get_select_items();
  ObSEArray<int64_t, 8> rowkey_sel_pos;
  ObRawExpr *expr = NULL;
  ObRawExpr *left_is_null_filter = NULL;
  ObRawExpr *right_is_null_filter = NULL;
  const TableItem *orig_left_table = NULL;
  if (OB_UNLIKELY(2 != mv_def_stmt_.get_table_items().count())
      || OB_ISNULL(orig_left_table = mv_def_stmt_.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(mv_def_stmt_.get_table_items()));
  } else if (OB_FAIL(get_rowkey_pos_in_select(rowkey_sel_pos))) {
    LOG_WARN("failed to get rowkey pos in select", K(ret));
  } else if (OB_FAIL(sel_stmt.get_select_items().prepare_allocate(orig_select_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(delta_left_table.get_object_name(), OLD_NEW_COL_NAME, delta_left_table.table_id_, expr))
             || OB_FAIL(ObRawExprUtils::build_is_not_null_expr(ctx_.expr_factory_, expr, false, left_is_null_filter))) {
    LOG_WARN("failed to build left is null expr", K(ret));
  } else if (OB_FAIL(create_simple_column_expr(delta_right_table.get_object_name(), OLD_NEW_COL_NAME, delta_right_table.table_id_, expr))
             || OB_FAIL(ObRawExprUtils::build_is_not_null_expr(ctx_.expr_factory_, expr, false, right_is_null_filter))) {
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
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(ctx_.expr_factory_,
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
int ObMajorRefreshMJVPrinter::gen_mr_rt_mv_left_delta_data_stmt(ObSelectStmt *&stmt)
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
                         || 2 != mv_def_stmt_.get_table_items().count())
             || OB_ISNULL(stmt->get_table_item(0))
             || OB_ISNULL(mv_def_stmt_.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(stmt->get_table_items()));
  } else if (OB_FAIL(gen_exists_cond_for_mview(*mv_def_stmt_.get_table_item(0),
                                               *stmt->get_table_item(0), exists_expr))) {
    LOG_WARN("failed to generate exists filter", K(ret));
  } else if (OB_FAIL(stmt->get_condition_exprs().push_back(exists_expr))) {
    LOG_WARN("failed to push back", K(ret));

  }
  return ret;
}

int ObMajorRefreshMJVPrinter::gen_exists_cond_for_mview(const TableItem &source_table,
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
                                                     *mv_table, false, true, subquery->get_condition_exprs()))) {
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
int ObMajorRefreshMJVPrinter::gen_one_refresh_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                                           const bool is_delta_left,
                                                                           ObSelectStmt *&delta_stmt)
{
  int ret = OB_SUCCESS;
  delta_stmt = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
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
  } else if (OB_UNLIKELY(2 != delta_stmt->get_table_items().count() || 2 != mv_def_stmt_.get_table_items().count())
             || OB_ISNULL(orig_left_table = mv_def_stmt_.get_table_item(0))
             || OB_ISNULL(left_table = delta_stmt->get_table_item(0))
             || OB_ISNULL(right_table = delta_stmt->get_table_item(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  } else if (OB_FALSE_IT(delta_table = is_delta_left ? left_table : right_table)) {
  } else if (OB_FAIL(create_simple_column_expr(delta_table->get_table_name(), ObString("ORA_ROWSCN"), delta_table->table_id_, col))
             || OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_, T_OP_GT, col, exprs_.last_refresh_scn_, scn_filter))
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
int ObMajorRefreshMJVPrinter::gen_refresh_validation_select_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                                                  ObSelectStmt *&delta_stmt)
{
  int ret = OB_SUCCESS;
  delta_stmt = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
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
  } else if (OB_UNLIKELY(2 != delta_stmt->get_table_items().count() || 2 != mv_def_stmt_.get_table_items().count())
             || OB_ISNULL(orig_left_table = mv_def_stmt_.get_table_item(0))
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
  } else if (OB_FAIL(ob_write_string(ctx_.alloc_, ObString("CNT"), delta_stmt->get_select_item(0).alias_name_))) {
    LOG_WARN("ob_write_string failed", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_COUNT, aggr_expr))) {
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

int ObMajorRefreshMJVPrinter::gen_refresh_select_hint_for_major_refresh_mjv(const TableItem &left_table,
                                                                            const TableItem &right_table,
                                                                            ObStmtHint &stmt_hint)
{
  int ret = OB_SUCCESS;
  ObJoinOrderHint *leading_left = NULL;
  ObJoinHint *use_nl_right = NULL;
  ObIndexHint *left_full_hint = NULL;
  ObIndexHint *right_full_hint = NULL;
  ObSEArray<ObItemType, 1> conflict_hints;
  if (OB_FAIL(ObQueryHint::create_hint(&ctx_.alloc_, T_LEADING, leading_left)) ||
      OB_FAIL(ObQueryHint::create_hint(&ctx_.alloc_, T_FULL_HINT, left_full_hint)) ||
      OB_FAIL(ObQueryHint::create_hint(&ctx_.alloc_, T_FULL_HINT, right_full_hint)) ||
      OB_FAIL(ObQueryHint::create_hint(&ctx_.alloc_, T_USE_NL, use_nl_right))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint_table(&ctx_.alloc_, leading_left->get_table().table_))) {
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

int ObMajorRefreshMJVPrinter::prepare_gen_access_delta_data_for_major_refresh_mjv(const ObIArray<int64_t> &rowkey_sel_pos,
                                                                                  ObSelectStmt &base_delta_stmt)
{
  int ret = OB_SUCCESS;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
  ObIArray<SelectItem> &select_items = base_delta_stmt.get_select_items();
  ObIArray<OrderItem> &order_items = base_delta_stmt.get_order_items();
  ObSEArray<ColumnItem, 8> column_items;
  ObRawExpr *expr = NULL;
  const TableItem *orig_table = NULL;
  TableItem *table = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!ctx_.for_rt_expand() && rowkey_sel_pos.empty())) {
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
    } else if (OB_FAIL(mv_def_stmt_.get_column_items(orig_table->table_id_, column_items))) {
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
  } else if (OB_FAIL(base_delta_stmt.deep_copy_join_tables(ctx_.alloc_, copier, mv_def_stmt_))) {
    LOG_WARN("failed to deep copy join tables", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(), base_delta_stmt.get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (OB_FAIL(base_delta_stmt.get_from_items().assign(mv_def_stmt_.get_from_items()))) {
    LOG_WARN("failed to assign from items", K(ret));
  }

  // for non joined table, adjust table id in from item
  ObIArray<FromItem> &from_items = base_delta_stmt.get_from_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
    if (from_items.at(i).is_joined_) {
      /* do nothing */
    } else if (OB_FAIL(mv_def_stmt_.get_table_item_idx(from_items.at(i).table_id_, idx))) {
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
  } else if (ctx_.for_rt_expand()) {  // for rt-mview access, generate original select list
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

}//end of namespace sql
}//end of namespace oceanbase

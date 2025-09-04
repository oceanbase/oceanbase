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
#include "sql/resolver/mv/ob_outer_join_mjv_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObOuterJoinMJVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> refreshed_table_idxs;
  dml_stmts.reuse();
  if (OB_FAIL(gen_delta_pre_table_views())) {
    LOG_WARN("failed to generate delta pre table views", K(ret));
  } else if (OB_FAIL(right_table_idxs_.prepare_allocate(mv_def_stmt_.get_table_size()))) {
    LOG_WARN("failed to prepare allocate table idx array", K(ret), K(mv_def_stmt_.get_table_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_from_item_size(); ++i) {
    TableItem *from_table = NULL;
    if (OB_FAIL(mv_def_stmt_.get_from_table(i, from_table))) {
      LOG_WARN("failed to get from table", K(ret));
    } else if (OB_FAIL(gen_refresh_dmls_for_table(from_table,
                                                  NULL,
                                                  refreshed_table_idxs,
                                                  dml_stmts))) {
      LOG_WARN("failed to gen refresh dmls for table", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObOuterJoinMJVPrinter::gen_delta_pre_table_views()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(all_delta_table_views_.prepare_allocate(mv_def_stmt_.get_table_size()))
      || OB_FAIL(all_pre_table_views_.prepare_allocate(mv_def_stmt_.get_table_size()))) {
    LOG_WARN("failed to prepare allocate view arrays", K(ret), K(mv_def_stmt_.get_table_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    const TableItem *ori_table = mv_def_stmt_.get_table_item(i);
    if (OB_ISNULL(ori_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source table is null", K(ret), K(i));
    } else if (OB_FAIL(gen_delta_pre_table_view(ori_table, all_pre_table_views_.at(i), false))) {
      LOG_WARN("failed to generate pre table view", K(ret), K(i));
    } else if (is_table_skip_refresh(*ori_table)) {
      // do nothing, no need to gen delta table view
    } else if (OB_FAIL(gen_delta_pre_table_view(ori_table, all_delta_table_views_.at(i), true))) {
      LOG_WARN("failed to generate delta table view", K(ret), K(i));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_refresh_dmls_for_table(const TableItem *table,
                                                      const JoinedTable *upper_table,
                                                      ObSqlBitSet<> &refreshed_table_idxs,
                                                      ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    const TableItem *first_table = NULL;  // outer join null side
    const TableItem *second_table = NULL; // outer join not null side
    if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      first_table = joined_table->left_table_;
      second_table = joined_table->right_table_;
    } else {
      first_table = joined_table->right_table_;
      second_table = joined_table->left_table_;
    }
    if (OB_FAIL(SMART_CALL(gen_refresh_dmls_for_table(first_table,
                                                      joined_table,
                                                      refreshed_table_idxs,
                                                      dml_stmts)))) {
      LOG_WARN("failed to gen refresh dmls for the first table", K(ret));
    } else if (OB_FAIL(SMART_CALL(gen_refresh_dmls_for_table(second_table,
                                                             upper_table,
                                                             refreshed_table_idxs,
                                                             dml_stmts)))) {
      LOG_WARN("failed to gen refresh dmls for the second table", K(ret));
    }
  } else {
    int64_t delta_table_idx = -1;
    if (OB_FAIL(mv_def_stmt_.get_table_item_idx(table, delta_table_idx))) {
      LOG_WARN("failed to get delta table idx", K(ret));
    } else if (is_table_skip_refresh(*table)) {
      // do nothing, no need to refresh
    } else if (NULL == upper_table || INNER_JOIN == upper_table->joined_type_) {
      if (OB_FAIL(gen_refresh_dmls_for_inner_join(table,
                                                  delta_table_idx,
                                                  refreshed_table_idxs,
                                                  dml_stmts))) {
        LOG_WARN("failed to gen refresh dmls for inner join", K(ret));
      }
    } else if (OB_FAIL(gen_refresh_dmls_for_left_join(table,
                                                      delta_table_idx,
                                                      upper_table,
                                                      refreshed_table_idxs,
                                                      dml_stmts))) {
      LOG_WARN("failed to gen refresh dmls for left join", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(update_table_idx_array(delta_table_idx,
                                                       upper_table,
                                                       refreshed_table_idxs))) {
      LOG_WARN("failed to update table idx array", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::update_table_idx_array(const int64_t delta_table_idx,
                                                  const JoinedTable *upper_table,
                                                  ObSqlBitSet<> &refreshed_table_idxs)
{
  int ret = OB_SUCCESS;
  ObRelIds join_cond_relids;
  if (OB_FAIL(refreshed_table_idxs.add_member(delta_table_idx))) {
    LOG_WARN("failed to add member", K(ret), K(delta_table_idx));
  } else if (NULL != upper_table) {
    for (int64_t i = 0; OB_SUCC(ret) && i < upper_table->join_conditions_.count(); ++i) {
      const ObRawExpr *expr = upper_table->join_conditions_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null expr", K(ret), K(i));
      } else if (OB_FAIL(join_cond_relids.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_table_idxs_.count(); ++i) {
      if (!join_cond_relids.has_member(i + 1) || delta_table_idx == i) {
        // do nothing
      } else if (OB_FAIL(right_table_idxs_.at(i).add_member(delta_table_idx))) {
        LOG_WARN("failed to add member", K(ret), K(delta_table_idx));
      } else if (OB_FAIL(right_table_idxs_.at(i).add_members(right_table_idxs_.at(delta_table_idx)))) {
        LOG_WARN("failed to add members", K(ret), K(delta_table_idx));
      }
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_refresh_dmls_for_inner_join(const TableItem *delta_table,
                                                           const int64_t delta_table_idx,
                                                           const ObSqlBitSet<> &refreshed_table_idxs,
                                                           ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_delete_for_inner_join(delta_table, dml_stmts))) {
    LOG_WARN("failed to gen delete stmt for inner join", K(ret));
  } else if (OB_FAIL(gen_insert_for_outer_join_mjv(delta_table_idx,
                                                   refreshed_table_idxs,
                                                   NULL, /* upper_table */
                                                   false, /* is_outer_join */
                                                   dml_stmts))) {
    LOG_WARN("failed to gen insert stmt for inner join", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_refresh_dmls_for_left_join(const TableItem *delta_table,
                                                          const int64_t delta_table_idx,
                                                          const JoinedTable *upper_table,
                                                          const ObSqlBitSet<> &refreshed_table_idxs,
                                                          ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_delete_for_left_join(delta_table,
                                       delta_table_idx,
                                       upper_table,
                                       refreshed_table_idxs,
                                       true, /* is_first_delete */
                                       dml_stmts))) {
    LOG_WARN("failed to generate the first delete stmt for left join", K(ret));
  } else if (OB_FAIL(gen_update_for_left_join(delta_table,
                                              delta_table_idx,
                                              refreshed_table_idxs,
                                              dml_stmts))) {
    LOG_WARN("failed to generate update stmt for left join", K(ret));
  } else if (OB_FAIL(gen_insert_for_outer_join_mjv(delta_table_idx,
                                                   refreshed_table_idxs,
                                                   upper_table,
                                                   true, /* is_outer_join */
                                                   dml_stmts))) {
    LOG_WARN("failed to gen insert stmt for left join", K(ret));
  } else if (OB_FAIL(gen_delete_for_left_join(delta_table,
                                              delta_table_idx,
                                              upper_table,
                                              refreshed_table_idxs,
                                              false, /* is_first_delete */
                                              dml_stmts))) {
    LOG_WARN("failed to generate the second delete stmt for left join", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_delete_for_inner_join(const TableItem *delta_table,
                                                     ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt *del_stmt = NULL;
  TableItem *mv_table = NULL;
  ObRawExpr *semi_filter = NULL;
  if (OB_ISNULL(delta_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(create_simple_stmt(del_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(del_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_ISNULL(del_stmt) || OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(del_stmt), K(mv_table));
  } else if (OB_FALSE_IT(mv_table->database_name_ = mv_db_name_)) {
  } else if (OB_FAIL(gen_exists_cond_for_table(delta_table, mv_table, true, true, semi_filter))) {
    LOG_WARN("failed to create simple column exprs", K(ret));
  } else if (OB_FAIL(del_stmt->get_condition_exprs().push_back(semi_filter))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(del_stmt))) {
    LOG_WARN("failed to push back delete stmt", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_insert_for_outer_join_mjv(const int64_t delta_table_idx,
                                                         const ObSqlBitSet<> &refreshed_table_idxs,
                                                         const JoinedTable *upper_table,
                                                         const bool is_outer_join,
                                                         ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObInsertStmt *ins_stmt = NULL;
  ObSelectStmt *ins_sel_stmt = NULL;
  TableItem *mv_table = NULL;
  TableItem *ins_source_table = NULL;
  ObRawExpr *target_col = NULL;
  if (OB_FAIL(create_simple_stmt(ins_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(ins_stmt, mv_schema_.get_table_name(), mv_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_ISNULL(ins_stmt) || OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ins_stmt), K(mv_table));
  } else {
    mv_table->database_name_ = mv_db_name_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(),
                                          mv_def_stmt_.get_select_item(i).alias_name_,
                                          mv_table->table_id_,
                                          target_col))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(ins_stmt->get_values_desc().push_back(static_cast<ObColumnRefRawExpr*>(target_col)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_insert_select_stmt(delta_table_idx, refreshed_table_idxs, upper_table, is_outer_join, ins_sel_stmt))) {
    LOG_WARN("failed to gen insert select stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(ins_stmt, DELTA_MV_VIEW_NAME, ins_source_table, ins_sel_stmt))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(ins_stmt))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_insert_select_stmt(const int64_t delta_table_idx,
                                                  const ObSqlBitSet<> &refreshed_table_idxs,
                                                  const JoinedTable *upper_table,
                                                  const bool is_outer_join,
                                                  ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(ctx_.expr_factory_);
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(gen_tables_for_insert_select_stmt(delta_table_idx,
                                                       refreshed_table_idxs,
                                                       sel_stmt))) {
    LOG_WARN("failed to generate table items for select stmt", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*sel_stmt, copier))) {
    LOG_WARN("failed to init expr copier for stmt", K(ret));
  } else if (OB_FAIL(construct_from_items_for_simple_mjv_delta_data(copier, *sel_stmt))) {
    LOG_WARN("failed to construct from items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(),
                                            sel_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (is_outer_join) {
    // replace the outer join for delta table as inner join
    JoinedTable *cur_upper_table = NULL;
    if (OB_FAIL(ObOptimizerUtil::find_joined_table(sel_stmt, upper_table->table_id_, cur_upper_table))) {
      LOG_WARN("failed to find cur upper table", K(ret), K(upper_table->table_id_), KPC(sel_stmt));
    } else if (OB_ISNULL(cur_upper_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur upper table is null", K(ret), K(upper_table->table_id_), KPC(sel_stmt), K(mv_def_stmt_));
    } else {
      cur_upper_table->joined_type_ = INNER_JOIN;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    const SelectItem &ori_sel_item = mv_def_stmt_.get_select_item(i);
    SelectItem new_sel_item;
    new_sel_item.is_real_alias_ = true;
    new_sel_item.alias_name_ = ori_sel_item.alias_name_;
    if (OB_FAIL(copier.copy_on_replace(ori_sel_item.expr_, new_sel_item.expr_))) {
      LOG_WARN("failed to copy select expr", K(ret));
    } else if (OB_FAIL(sel_stmt->get_select_items().push_back(new_sel_item))) {
      LOG_WARN("failed to push back select item", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_tables_for_insert_select_stmt(const int64_t delta_table_idx,
                                                             const ObSqlBitSet<> &refreshed_table_idxs,
                                                             ObSelectStmt *sel_stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
  char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
  int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
  if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(sel_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    const TableItem *ori_table = NULL;
    TableItem *new_table = NULL;
    ObSelectStmt *view_stmt = NULL;
    int64_t pos = 0;
    if (delta_table_idx == i) {
      if (OB_ISNULL(view_stmt = all_delta_table_views_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      }
    } else if (!refreshed_table_idxs.has_member(i)) {
      if (OB_ISNULL(view_stmt = all_pre_table_views_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      }
    } else { /* do nothing */ }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ori_table = mv_def_stmt_.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i), K(ori_table));
    } else if (OB_FAIL(create_simple_table_item(sel_stmt, ori_table->table_name_, new_table, view_stmt, false))) {
      LOG_WARN("failed to create simple table item", K(ret));
    } else if (OB_ISNULL(new_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table item is null", K(ret));
    } else if (new_table->is_basic_table()) {
      // access current data
      set_info_for_simple_table_item(*new_table, *ori_table);
    } else if (OB_FAIL(BUF_PRINTF(delta_table_idx == i ? DELTA_TABLE_FORMAT_NAME : PRE_TABLE_FORMAT_NAME,
                                  ori_table->get_object_name().length(),
                                  ori_table->get_object_name().ptr()))) {
      LOG_WARN("failed to buf print for delta/pre view name", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx_.alloc_, ObString(pos, buf), new_table->alias_name_))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_delete_for_left_join(const TableItem *delta_table,
                                                    const int64_t delta_table_idx,
                                                    const JoinedTable *upper_table,
                                                    const ObSqlBitSet<> &refreshed_table_idxs,
                                                    const bool is_first_delete,
                                                    ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt *del_stmt = NULL;
  ObSelectStmt *cond_sel_stmt = NULL;
  TableItem *mv_table = NULL;
  ObOpRawExpr *mv_pk_expr = NULL;
  ObQueryRefRawExpr *cond_query_expr = NULL;
  ObOpRawExpr *semi_filter = NULL;
  if (OB_FAIL(create_simple_stmt(del_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(del_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_ISNULL(del_stmt) || OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(del_stmt), K(mv_table));
  } else if (OB_FALSE_IT(mv_table->database_name_ = mv_db_name_)) {
  } else if (is_first_delete
             && OB_FAIL(gen_select_for_left_join_first_delete(delta_table,
                                                              delta_table_idx,
                                                              upper_table,
                                                              refreshed_table_idxs,
                                                              cond_sel_stmt))) {
    LOG_WARN("failed to generate select stmt for semi filter", K(ret));
  } else if (!is_first_delete
             && OB_FAIL(gen_select_for_left_join_second_delete(delta_table,
                                                               delta_table_idx,
                                                               upper_table,
                                                               refreshed_table_idxs,
                                                               cond_sel_stmt))) {
    LOG_WARN("failed to generate select stmt for semi filter", K(ret));
  } else if (OB_FAIL(gen_mv_rowkey_expr(mv_table, mv_pk_expr))) {
    LOG_WARN("failed to generate mv rowkey expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, cond_query_expr))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_FALSE_IT(cond_query_expr->set_ref_stmt(cond_sel_stmt))) {
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_IN, semi_filter))) {
    LOG_WARN("failed to create op in raw expr", K(ret));
  } else if (OB_FAIL(semi_filter->set_param_exprs(mv_pk_expr, cond_query_expr))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(del_stmt->get_condition_exprs().push_back(semi_filter))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(del_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(del_stmt))) {
    LOG_WARN("failed to push back delete stmt", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_select_for_left_join_first_delete(const TableItem *delta_table,
                                                                 const int64_t delta_table_idx,
                                                                 const JoinedTable *upper_table,
                                                                 const ObSqlBitSet<> &refreshed_table_idxs,
                                                                 ObSelectStmt *&cond_sel_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stat_stmt = NULL;
  ObSelectStmt *dlt_t_mlog_stmt = NULL;
  TableItem *mv_table = NULL;
  TableItem *dlt_t_mlog_table = NULL;
  JoinedTable *mv_stat_from_table = NULL;
  ObSEArray<ObRawExpr*, 4> dlt_t_mlog_rowkeys;
  ObSEArray<int64_t, 4> other_join_table_idxs; // useless for the first delete
  ObSEArray<ObRawExpr*, 16> other_join_table_rowkeys;
  ObSEArray<ObRawExpr*, 16> win_func_partiton_key;
  ObOpRawExpr *mv_stat_cond_expr = NULL;
  ObQueryRefRawExpr *in_sel_query_expr = NULL;
  ObOpRawExpr *left_table_rowkey_row = NULL;
  ObSelectStmt *in_sel_stmt = NULL;
  ObRawExpr *in_sel_cond_expr = NULL;
  cond_sel_stmt = NULL;
  // build mv_stat subquery
  if (OB_ISNULL(delta_table) || OB_ISNULL(upper_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret), K(delta_table), K(upper_table));
  } else if (OB_FAIL(create_simple_stmt(mv_stat_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(mv_stat_stmt, mv_schema_.get_table_name(), mv_table, NULL, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(gen_delta_mlog_table_view(*delta_table, dlt_t_mlog_stmt, MLOG_EXT_COL_ROWKEY_ONLY))) {
    LOG_WARN("failed to generate dlt_t_mlog view", K(ret), KPC(delta_table));
  } else if (OB_ISNULL(mv_stat_stmt) || OB_ISNULL(mv_table) || OB_ISNULL(dlt_t_mlog_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_stat_stmt), K(mv_table), K(dlt_t_mlog_stmt));
  } else if (OB_FALSE_IT(dlt_t_mlog_stmt->assign_distinct())) {
  } else if (OB_FAIL(create_simple_table_item(mv_stat_stmt, DELTA_TABLE_VIEW_NAME, dlt_t_mlog_table, dlt_t_mlog_stmt, false))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_ISNULL(dlt_t_mlog_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(dlt_t_mlog_table));
  } else if (OB_FAIL(create_joined_table_item(mv_stat_stmt, LEFT_OUTER_JOIN, *mv_table, *dlt_t_mlog_table, true, mv_stat_from_table))) {
    LOG_WARN("failed to create joined table", K(ret));
  } else if (OB_ISNULL(mv_stat_from_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_stat_from_table));
  } else if (OB_FAIL(gen_rowkey_join_conds_for_table(*delta_table,
                                                     *dlt_t_mlog_table,
                                                     *mv_table,
                                                     true,
                                                     mv_stat_from_table->get_join_conditions()))) {
    LOG_WARN("failed to generate rowkey join conds", K(ret));
  } else if (OB_FAIL(add_mv_rowkey_into_select(mv_stat_stmt, mv_table))) {
    LOG_WARN("failed to add mv pk into select", K(ret));
  } else if (OB_FAIL(get_table_rowkey_exprs(*dlt_t_mlog_table, *delta_table, false, dlt_t_mlog_rowkeys))) {
    LOG_WARN("failed to get table rowkey exprs", K(ret));
  } else if (OB_UNLIKELY(dlt_t_mlog_rowkeys.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected rowkey count", K(ret));
  } else if (OB_FAIL(get_left_table_rowkey_exprs(delta_table_idx,
                                                 mv_table,
                                                 upper_table->get_join_conditions(),
                                                 right_table_idxs_.at(delta_table_idx),
                                                 other_join_table_idxs,
                                                 other_join_table_rowkeys,
                                                 win_func_partiton_key))) {
    LOG_WARN("failed to get left table rowkey exprs", K(ret));
  } else if (OB_FAIL(gen_mv_stat_winfunc_expr(dlt_t_mlog_rowkeys.at(0),
                                              win_func_partiton_key,
                                              mv_stat_stmt))) {
    LOG_WARN("failed to generate mv_stat winfunc expr", K(ret));
  // generate where conditions for mv_stat
  } else if (OB_FAIL(create_simple_stmt(in_sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL(in_sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else if (OB_FAIL(in_sel_stmt->get_table_items().push_back(mv_table))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(in_sel_stmt->add_from_item(mv_table->table_id_))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(gen_exists_cond_for_table(delta_table, mv_table, true, true, in_sel_cond_expr))) {
    LOG_WARN("failed to generate exists cond for mv_stat cond", K(ret));
  } else if (OB_FAIL(in_sel_stmt->get_condition_exprs().push_back(in_sel_cond_expr))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(in_sel_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  } else if (OB_FAIL(add_col_exprs_into_select(in_sel_stmt->get_select_items(), other_join_table_rowkeys))) {
    LOG_WARN("failed to add col exprs into select", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, in_sel_query_expr))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_FALSE_IT(in_sel_query_expr->set_ref_stmt(in_sel_stmt))) {
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, left_table_rowkey_row))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_FAIL(left_table_rowkey_row->set_param_exprs(other_join_table_rowkeys))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_IN, mv_stat_cond_expr))) {
    LOG_WARN("failed to create op in raw expr", K(ret));
  } else if (OB_FAIL(mv_stat_cond_expr->set_param_exprs(left_table_rowkey_row, in_sel_query_expr))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(mv_stat_stmt->get_condition_exprs().push_back(mv_stat_cond_expr))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(mv_stat_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  }
  // build cond_sel_stmt
  TableItem *mv_stat_table = NULL;
  ObRawExpr *col_rownum = NULL;         // ROWNUM
  ObRawExpr *col_cnt_star = NULL;       // CNT
  ObRawExpr *col_cnt_pk = NULL;         // CNT_PK
  ObRawExpr *cnt_star_eq_cnt_pk = NULL; // CNT = CNT_PK
  ObRawExpr *cnt_star_gt_cnt_pk = NULL; // CNT > CNT_PK
  ObRawExpr *rownum_gt_1 = NULL;        // ROWNUM > 1
  ObRawExpr *rownum_le_cnt_pk = NULL;   // ROWNUM <= CNT_PK
  ObRawExpr *or_param_1 = NULL;         // (CNT = CNT_PK) AND (ROWNUM > 1)
  ObRawExpr *or_param_2 = NULL;         // (CNT > CNT_PK) AND (ROWNUM <= CNT_PK)
  ObRawExpr *cond_sel_stmt_cond = NULL; // (or_param_1) OR (or_param_2)
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_simple_stmt(cond_sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(cond_sel_stmt, MV_STAT_VIEW_NAME, mv_stat_table, mv_stat_stmt, true))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(add_mv_rowkey_into_select(cond_sel_stmt, mv_stat_table))) {
    LOG_WARN("failed to add mv pk into select", K(ret));
  } else if (OB_ISNULL(cond_sel_stmt) || OB_ISNULL(mv_stat_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cond_sel_stmt), K(mv_stat_table));
  } else if (OB_FAIL(create_simple_column_expr(mv_stat_table->get_table_name(), WIN_ROW_NUM_COL_NAME, mv_stat_table->table_id_, col_rownum))
             || OB_FAIL(create_simple_column_expr(mv_stat_table->get_table_name(), WIN_CNT_ALL_COL_NAME, mv_stat_table->table_id_, col_cnt_star))
             || OB_FAIL(create_simple_column_expr(mv_stat_table->get_table_name(), WIN_CNT_NOT_NULL_COL_NAME, mv_stat_table->table_id_, col_cnt_pk))) {
    LOG_WARN("failed to create simple column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_EQ, col_cnt_star, col_cnt_pk, cnt_star_eq_cnt_pk))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_GT, col_cnt_star, col_cnt_pk, cnt_star_gt_cnt_pk))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_GT, col_rownum, exprs_.int_one_, rownum_gt_1))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_LE, col_rownum, col_cnt_pk, rownum_le_cnt_pk))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_AND, cnt_star_eq_cnt_pk, rownum_gt_1, or_param_1))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_AND, cnt_star_gt_cnt_pk, rownum_le_cnt_pk, or_param_2))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_OR, or_param_1, or_param_2, cond_sel_stmt_cond))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(cond_sel_stmt->get_condition_exprs().push_back(cond_sel_stmt_cond))) {
    LOG_WARN("failed to push back where condition", K(ret));
  }
  return ret;
}

/**
 * OUTPUT:
 * other_join_table_idxs : table idx for tables in join_conditions except delta table
 * other_join_table_rowkeys : table rowkeys for tables in join_conditions except delta table
 * all_other_table_rowkeys : table rowkeys for tables in mv def stmt except except_table_idxs
 */
int ObOuterJoinMJVPrinter::get_left_table_rowkey_exprs(const int64_t delta_table_idx,
                                                       const TableItem *outer_table,
                                                       const ObIArray<ObRawExpr*> &join_conditions,
                                                       const ObSqlBitSet<> &except_table_idxs,
                                                       ObIArray<int64_t> &other_join_table_idxs,
                                                       ObIArray<ObRawExpr*> &other_join_table_rowkeys,
                                                       ObIArray<ObRawExpr*> &all_other_table_rowkeys)
{
  int ret = OB_SUCCESS;
  ObRelIds join_cond_relids;
  if (OB_ISNULL(outer_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(outer_table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions.count(); ++i) {
    const ObRawExpr *expr = join_conditions.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret), K(i));
    } else if (OB_FAIL(join_cond_relids.add_members(expr->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    const TableItem *cur_table = NULL;
    ObSEArray<ObRawExpr*, 4> cur_table_rowkeys;
    if (delta_table_idx == i || except_table_idxs.has_member(i)) {
      // do nothing
     } else if (OB_ISNULL(cur_table = mv_def_stmt_.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (OB_FAIL(get_table_rowkey_exprs(*outer_table, *cur_table, true, cur_table_rowkeys))) {
      LOG_WARN("failed to get table rowkey exprs", K(ret), KPC(cur_table));
    } else if (OB_FAIL(append(all_other_table_rowkeys, cur_table_rowkeys))) {
      LOG_WARN("failed to append table rowkey", K(ret));
    } else if (!join_cond_relids.has_member(i + 1)) {
      // do nothing
    } else if (OB_FAIL(other_join_table_idxs.push_back(i))) {
      LOG_WARN("failed to push back table idx", K(ret));
    } else if (OB_FAIL(append(other_join_table_rowkeys, cur_table_rowkeys))) {
      LOG_WARN("failed to append table rowkey", K(ret));
    }
  }
  return ret;
}

/**
 * row_num_expr :    ROW_NUMBER() OVER (PARTITION BY "part_exprs" ORDER BY "pk_expr" NULLS LAST)
 * count_star_expr : COUNT(*) OVER (PARTITION BY "part_exprs")
 * count_pk_expr :   COUNT("pk_expr") OVER (PARTITION BY "part_exprs")
 */
int ObOuterJoinMJVPrinter::gen_mv_stat_winfunc_expr(ObRawExpr *pk_expr,
                                                    const ObIArray<ObRawExpr*> &part_exprs,
                                                    ObSelectStmt *sel_stmt)
{
  int ret = OB_SUCCESS;
  ObWinFunRawExpr *row_num_expr = NULL;
  ObWinFunRawExpr *count_star_expr = NULL;
  ObWinFunRawExpr *count_pk_expr = NULL;
  ObAggFunRawExpr *aggr_row_num = NULL;
  ObAggFunRawExpr *aggr_count_star = NULL;
  ObAggFunRawExpr *aggr_count_pk = NULL;
  OrderItem row_num_order_item(pk_expr, NULLS_LAST_DESC);
  if (OB_ISNULL(pk_expr) || OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(pk_expr), K(sel_stmt));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, row_num_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, count_star_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, count_pk_expr))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WIN_FUN_ROW_NUMBER, aggr_row_num))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_COUNT, aggr_count_star))
             || OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_COUNT, aggr_count_pk))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(row_num_expr) || OB_ISNULL(count_star_expr) || OB_ISNULL(count_pk_expr)
             || OB_ISNULL(aggr_row_num) || OB_ISNULL(aggr_count_star) || OB_ISNULL(aggr_count_pk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(row_num_expr), K(count_star_expr), K(count_pk_expr), K(aggr_row_num), K(aggr_count_star), K(aggr_count_pk));
  } else if (OB_FAIL(aggr_count_star->add_real_param_expr(exprs_.int_one_))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(aggr_count_pk->add_real_param_expr(pk_expr))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(row_num_expr->set_partition_exprs(part_exprs)) ||
             OB_FAIL(count_star_expr->set_partition_exprs(part_exprs)) ||
             OB_FAIL(count_pk_expr->set_partition_exprs(part_exprs))) {
    LOG_WARN("fail to set partition exprs", K(ret));
  } else if (OB_FAIL(row_num_expr->get_order_items().push_back(row_num_order_item))) {
    LOG_WARN("fail to push back order item", K(ret));
  } else {
    SelectItem sel_row_num, sel_count_star, sel_count_pk;
    row_num_expr->set_func_type(T_WIN_FUN_ROW_NUMBER);
    count_star_expr->set_func_type(T_FUN_COUNT);
    count_pk_expr->set_func_type(T_FUN_COUNT);
    row_num_expr->set_agg_expr(aggr_row_num);
    count_star_expr->set_agg_expr(aggr_count_star);
    count_pk_expr->set_agg_expr(aggr_count_pk);
    sel_row_num.expr_ = row_num_expr;
    sel_row_num.is_real_alias_ = true;
    sel_row_num.alias_name_ = WIN_ROW_NUM_COL_NAME;
    sel_count_star.expr_ = count_star_expr;
    sel_count_star.is_real_alias_ = true;
    sel_count_star.alias_name_ = WIN_CNT_ALL_COL_NAME;
    sel_count_pk.expr_ = count_pk_expr;
    sel_count_pk.is_real_alias_ = true;
    sel_count_pk.alias_name_ = WIN_CNT_NOT_NULL_COL_NAME;
    if (OB_FAIL(sel_stmt->add_select_item(sel_row_num))) {
      LOG_WARN("failed to add row num select item", K(ret));
    } else if (OB_FAIL(sel_stmt->add_select_item(sel_count_star))) {
      LOG_WARN("failed to add count * select item", K(ret));
    } else if (OB_FAIL(sel_stmt->add_select_item(sel_count_pk))) {
      LOG_WARN("failed to add count pk select item", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_update_for_left_join(const TableItem *delta_table,
                                                    const int64_t delta_table_idx,
                                                    const ObSqlBitSet<> &refreshed_table_idxs,
                                                    ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt *upd_stmt = NULL;
  TableItem *mv_table = NULL;
  ObRawExpr *semi_filter = NULL;
  void *ptr = NULL;
  ObUpdateTableInfo *table_info = NULL;
  ObRelIds set_null_tables;
  if (OB_FAIL(create_simple_stmt(upd_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(upd_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_ISNULL(upd_stmt) || OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(upd_stmt), K(mv_table));
  } else if (OB_FAIL(gen_exists_cond_for_table(delta_table, mv_table, true, true, semi_filter))) {
    LOG_WARN("failed to gen exists semi filter", K(ret));
  } else if (OB_FAIL(upd_stmt->get_condition_exprs().push_back(semi_filter))) {
    LOG_WARN("failed to push back semi filter", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(upd_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  } else if (OB_ISNULL((ptr = ctx_.alloc_.alloc(sizeof(ObUpdateTableInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table info", K(ret));
  } else if (OB_FALSE_IT(table_info = new(ptr)ObUpdateTableInfo())) {
  } else if (OB_FAIL(upd_stmt->get_update_table_info().push_back(table_info))) {
    LOG_WARN("failed to push back", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    if (right_table_idxs_.at(delta_table_idx).has_member(i) || delta_table_idx == i) {
      if (OB_FAIL(set_null_tables.add_member(i + 1))) {
        LOG_WARN("failed to add set null table rel id", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_set_null_to_upd_stmt(set_null_tables, mv_table, table_info->assignments_))) {
    LOG_WARN("failed to add set null to update", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(upd_stmt))) {
    LOG_WARN("failed to push back delete stmt", K(ret));
  }
  return ret;
}

int ObOuterJoinMJVPrinter::add_set_null_to_upd_stmt(const ObRelIds &set_null_tables,
                                                    const TableItem *mv_table,
                                                    ObIArray<ObAssignment> &assignments)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(ctx_.expr_factory_);
  if (OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_table));
  }
  // 1. fill copier
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    const SelectItem &sel_item = mv_def_stmt_.get_select_item(i);
    ObColumnRefRawExpr *col_expr = NULL;
    const ObString *orig_sel_alias = NULL;
    ObRawExpr *tmp_expr = NULL;
    if (OB_ISNULL(sel_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(sel_item), K(i));
    } else if (!sel_item.expr_->is_column_ref_expr()) {
      // do nothing
    } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(sel_item.expr_))) {
    } else if (copier.is_existed(col_expr)) {
      // do nothing
    } else if (set_null_tables.is_superset(col_expr->get_relation_ids())) {
      // replace as NULL
      if (OB_FAIL(copier.add_replaced_expr(col_expr, exprs_.null_expr_))) {
        LOG_WARN("failed to add replaced expr", K(ret), KPC(col_expr), KPC(exprs_.null_expr_));
      }
    } else if (OB_FAIL(get_column_name_from_origin_select_items(col_expr->get_table_id(),
                                                                col_expr->get_column_id(),
                                                                orig_sel_alias))) {
      LOG_WARN("failed to get column_name from origin select_items", K(ret), KPC(col_expr));
    } else if (OB_ISNULL(orig_sel_alias)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(col_expr));
    } else if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(),
                                                 *orig_sel_alias,
                                                 mv_table->table_id_,
                                                 tmp_expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(col_expr, tmp_expr))) {
      LOG_WARN("failed to add replaced expr", K(ret), KPC(col_expr), KPC(tmp_expr));
    }
  }
  // 2. generate assignments
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    const SelectItem &sel_item = mv_def_stmt_.get_select_item(i);
    ObRawExpr *mv_col_expr = NULL;
    ObAssignment assign;
    if (OB_ISNULL(sel_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(sel_item), K(i));
    } else if (!set_null_tables.overlap(sel_item.expr_->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(),
                                                 sel_item.alias_name_,
                                                 mv_table->table_id_,
                                                 mv_col_expr))) {
      LOG_WARN("failed to create simple column exprs", K(ret));
    } else if (OB_FALSE_IT(assign.column_expr_ = static_cast<ObColumnRefRawExpr*>(mv_col_expr))) {
    } else if (OB_FAIL(copier.copy_on_replace(sel_item.expr_, assign.expr_))) {
      LOG_WARN("failed to copy target expr", K(ret), KPC(sel_item.expr_));
    } else if (OB_FAIL(assignments.push_back(assign))) {
      LOG_WARN("failed to push back assign", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMJVPrinter::gen_select_for_left_join_second_delete(const TableItem *delta_table,
                                                                  const int64_t delta_table_idx,
                                                                  const JoinedTable *upper_table,
                                                                  const ObSqlBitSet<> &refreshed_table_idxs,
                                                                  ObSelectStmt *&cond_sel_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *mv_stat_stmt = NULL;
  ObSelectStmt *in_sel_stmt = NULL;
  TableItem *mv_table = NULL;
  ObSEArray<ObRawExpr*, 4> delta_table_rowkeys;
  ObSEArray<int64_t, 4> other_join_table_idxs;
  ObSEArray<ObRawExpr*, 16> other_join_table_rowkeys;
  ObSEArray<ObRawExpr*, 16> all_other_table_rowkeys; // useless for the second delete
  TableItem *new_table = NULL;
  ObOpRawExpr *mv_stat_cond_expr = NULL;
  ObOpRawExpr *left_table_rowkey_row = NULL;
  ObSEArray<ObRawExpr*, 16> left_table_rowkey_row_params;
  ObQueryRefRawExpr *in_sel_query_expr = NULL;
  ObRawExprCopier in_sel_stmt_copier(ctx_.expr_factory_);
  const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
  char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
  int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
  int64_t pos = 0;
  cond_sel_stmt = NULL;
  if (OB_ISNULL(delta_table) || OB_ISNULL(upper_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret), K(delta_table), K(upper_table));
  } else if (OB_FAIL(create_simple_stmt(mv_stat_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(mv_stat_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(add_mv_rowkey_into_select(mv_stat_stmt, mv_table))) {
    LOG_WARN("failed to add mv pk into select", K(ret));
  } else if (OB_ISNULL(mv_stat_stmt) || OB_ISNULL(mv_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_stat_stmt), K(mv_table));
  } else if (OB_FAIL(get_table_rowkey_exprs(*mv_table, *delta_table, true, delta_table_rowkeys))) {
    LOG_WARN("failed to get table rowkey exprs", K(ret));
  } else if (OB_UNLIKELY(delta_table_rowkeys.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected rowkey count", K(ret));
  } else if (OB_FAIL(get_left_table_rowkey_exprs(delta_table_idx,
                                                 mv_table,
                                                 upper_table->get_join_conditions(),
                                                 refreshed_table_idxs,
                                                 other_join_table_idxs,
                                                 other_join_table_rowkeys,
                                                 all_other_table_rowkeys))) {
    LOG_WARN("failed to get left table rowkey exprs", K(ret));
  } else if (OB_FAIL(gen_mv_stat_winfunc_expr(delta_table_rowkeys.at(0),
                                              other_join_table_rowkeys,
                                              mv_stat_stmt))) {
    LOG_WARN("failed to generate mv_stat winfunc expr", K(ret));
  // generate where conditions for mv_stat
  } else if (OB_FAIL(create_simple_stmt(in_sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, in_sel_query_expr))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_FALSE_IT(in_sel_query_expr->set_ref_stmt(in_sel_stmt))) {
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_ROW, left_table_rowkey_row))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_FAIL(left_table_rowkey_row->set_param_exprs(other_join_table_rowkeys))) {
    LOG_WARN("failed to set rowkey row params", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_IN, mv_stat_cond_expr))) {
    LOG_WARN("failed to create op in raw expr", K(ret));
  } else if (OB_FAIL(mv_stat_cond_expr->set_param_exprs(left_table_rowkey_row, in_sel_query_expr))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(mv_stat_stmt->get_condition_exprs().push_back(mv_stat_cond_expr))) {
    LOG_WARN("failed to push back where condition", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(mv_stat_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  } else if (OB_UNLIKELY(0 > delta_table_idx || all_delta_table_views_.count() <= delta_table_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table idx", K(ret), K(delta_table_idx), K(all_delta_table_views_.count()));
  } else if (OB_FAIL(create_simple_table_item(in_sel_stmt,
                                              delta_table->get_object_name(),
                                              new_table,
                                              all_delta_table_views_.at(delta_table_idx)))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(DELTA_TABLE_FORMAT_NAME,
                                delta_table->get_object_name().length(),
                                delta_table->get_object_name().ptr()))) {
    LOG_WARN("failed to buf print for delta/pre view name", K(ret));
  } else if (OB_FAIL(ob_write_string(ctx_.alloc_, ObString(pos, buf), new_table->alias_name_))) {
    LOG_WARN("failed to write string", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_table(delta_table, new_table, in_sel_stmt_copier))) {
    LOG_WARN("failed to init expr copier for delta table", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_join_table_idxs.count(); ++i) {
    const TableItem *left_table = NULL;
    ObSEArray<ObRawExpr*, 4> table_rowkeys;
    int64_t table_idx = other_join_table_idxs.at(i);
    pos = 0;
    new_table = NULL;
    if (OB_UNLIKELY(0 > table_idx || all_pre_table_views_.count() <= table_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table idx", K(ret), K(table_idx), K(all_pre_table_views_.count()));
    } else if (OB_ISNULL(left_table = mv_def_stmt_.get_table_item(table_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(other_join_table_idxs.at(i)), K(table_idx));
    } else if (OB_FAIL(create_simple_table_item(in_sel_stmt,
                                                left_table->get_object_name(),
                                                new_table,
                                                all_pre_table_views_.at(table_idx)))) {
      LOG_WARN("failed to create simple table item", K(ret));
    } else if (OB_ISNULL(new_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(new_table));
    } else if (OB_FAIL(BUF_PRINTF(PRE_TABLE_FORMAT_NAME,
                                  left_table->get_object_name().length(),
                                  left_table->get_object_name().ptr()))) {
      LOG_WARN("failed to buf print for delta/pre view name", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx_.alloc_, ObString(pos, buf), new_table->alias_name_))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(init_expr_copier_for_table(left_table, new_table, in_sel_stmt_copier))) {
      LOG_WARN("failed to init expr copier for left table", K(ret), KPC(left_table));
    } else if (OB_FAIL(get_table_rowkey_exprs(*new_table, *left_table, false, table_rowkeys))) {
      LOG_WARN("failed to get table rowkey exprs", K(ret));
    } else if (OB_FAIL(add_col_exprs_into_select(in_sel_stmt->get_select_items(), table_rowkeys))) {
      LOG_WARN("failed to add col exprs into select", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(in_sel_stmt_copier.copy_on_replace(upper_table->get_join_conditions(),
                                                        in_sel_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to copy condition exprs", K(ret));
  } else if (OB_FAIL(add_semi_to_inner_hint(in_sel_stmt))) {
    LOG_WARN("failed to add semi to inner hint", K(ret));
  }
  // build cond_sel_stmt
  TableItem *mv_stat_table = NULL;
  ObRawExpr *col_rownum = NULL;         // ROWNUM
  ObRawExpr *col_cnt_pk = NULL;         // CNT_PK
  ObRawExpr *rownum_gt_cnt_pk = NULL;   // ROWNUM > CNT_PK
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_simple_stmt(cond_sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(cond_sel_stmt, MV_STAT_VIEW_NAME, mv_stat_table, mv_stat_stmt, true))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(add_mv_rowkey_into_select(cond_sel_stmt, mv_stat_table))) {
    LOG_WARN("failed to add mv pk into select", K(ret));
  } else if (OB_ISNULL(cond_sel_stmt) || OB_ISNULL(mv_stat_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cond_sel_stmt), K(mv_stat_table));
  } else if (OB_FAIL(create_simple_column_expr(mv_stat_table->get_table_name(), WIN_ROW_NUM_COL_NAME, mv_stat_table->table_id_, col_rownum))
             || OB_FAIL(create_simple_column_expr(mv_stat_table->get_table_name(), WIN_CNT_NOT_NULL_COL_NAME, mv_stat_table->table_id_, col_cnt_pk))) {
    LOG_WARN("failed to create simple column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 ctx_.expr_factory_, T_OP_GT, col_rownum, col_cnt_pk, rownum_gt_cnt_pk))) {
    LOG_WARN("failed to build binary op expr", K(ret));
  } else if (OB_FAIL(cond_sel_stmt->get_condition_exprs().push_back(rownum_gt_cnt_pk))) {
    LOG_WARN("failed to push back where condition", K(ret));
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
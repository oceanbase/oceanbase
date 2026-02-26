/**
 * Copyright (c) 2025 OceanBase
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
#include "sql/resolver/mv/ob_outer_join_mav_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObOuterJoinMAVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObOuterJoinMAVPrinter::gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt*, 8> inner_delta_dml_stmts;
  inner_delta_mavs.reuse();
  if (OB_FAIL(gen_delta_pre_data_views())) {
    LOG_WARN("failed to generate delta pre data views", K(ret));
  } else if (OB_FAIL(init_outer_join_mv_printer_helper())) {
    LOG_WARN("failed to init outer join mv printer helper", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_from_item_size(); ++i) {
    TableItem *from_table = NULL;
    if (OB_FAIL(mv_def_stmt_.get_from_table(i, from_table))) {
      LOG_WARN("failed to get from table", K(ret));
    // generate inner delta mav for table
    } else if (OB_FAIL(gen_refresh_dmls_for_table(from_table,
                                                  NULL,
                                                  inner_delta_dml_stmts))) {
      LOG_WARN("failed to gen inner delta mav for table", K(ret), KPC(from_table));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_delta_dml_stmts.count(); ++i) {
    ObDMLStmt *inner_delta_mav = inner_delta_dml_stmts.at(i);
    if (OB_ISNULL(inner_delta_mav) || OB_UNLIKELY(!inner_delta_mav->is_select_stmt())) {
      LOG_WARN("get unexpected inner delta mav stmt", K(ret), KPC(inner_delta_mav));
    } else if (OB_FAIL(inner_delta_mavs.push_back(static_cast<ObSelectStmt*>(inner_delta_mav)))) {
      LOG_WARN("failed to push back inner delta mav", K(ret));
    }
  }
  return ret;
}

// generate inner delta mav for inner join
int ObOuterJoinMAVPrinter::gen_refresh_dmls_for_inner_join(const TableItem *delta_table,
                                                           const int64_t delta_table_idx,
                                                           ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *inner_delta_mav = NULL;
  if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(delta_table_idx, inner_delta_mav))) {
    LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(inner_delta_mav))) {
    LOG_WARN("failed to push back inner delta mav", K(ret));
  }
  return ret;
}

// generate inner delta mav for left join
int ObOuterJoinMAVPrinter::gen_refresh_dmls_for_left_join(const TableItem *delta_table,
                                                          const int64_t delta_table_idx,
                                                          const JoinedTable *upper_table,
                                                          ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *inner_delta_mav_joined = NULL;   // joined part
  ObSelectStmt *inner_delta_mav_new_null = NULL; // non-padded NULL TO padded NULL
  ObSelectStmt *inner_delta_mav_pre_null = NULL; // padded NULL TO non-padded NULL
  if (OB_FAIL(gen_inner_delta_mav_for_left_join_joined_data(delta_table_idx,
                                                            upper_table,
                                                            inner_delta_mav_joined))) {
    LOG_WARN("failed to generate the first inner delta mav for left join", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(inner_delta_mav_joined))) {
    LOG_WARN("failed to push back inner delta mav", K(ret));
  } else if (OB_FAIL(gen_inner_delta_mav_for_left_join_padded_null_data(delta_table,
                                                                        delta_table_idx,
                                                                        upper_table,
                                                                        inner_delta_mav_new_null,
                                                                        inner_delta_mav_pre_null))) {
    LOG_WARN("failed to generate the second inner delta mav for left join", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(inner_delta_mav_new_null))) {
    LOG_WARN("failed to push back inner delta mav", K(ret));
  } else if (OB_FAIL(dml_stmts.push_back(inner_delta_mav_pre_null))) {
    LOG_WARN("failed to push back inner delta mav", K(ret));
  }
  return ret;
}

/**
 * @brief ObOuterJoinMAVPrinter::get_delta_pre_view_stmt
 *
 * For inner_delta_no table, return delta data view
 * For unrefreshed table, return pre data view
 * For refreshed table, return NULL
 */
int ObOuterJoinMAVPrinter::get_delta_pre_view_stmt(const int64_t table_idx,
                                                   const int64_t inner_delta_no,
                                                   ObSelectStmt *&view_stmt) const
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  if (table_idx == inner_delta_no) {
    if (OB_ISNULL(view_stmt = all_delta_datas_.at(table_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null delta data view", K(ret), K(table_idx));
    }
  } else if (!refreshed_table_idxs_.has_member(table_idx)) {
    if (OB_ISNULL(view_stmt = all_pre_datas_.at(table_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pre data view", K(ret), K(table_idx));
    }
  } else {
    // do nothing, access current data
  }
  return ret;
}

int ObOuterJoinMAVPrinter::gen_inner_delta_mav_for_left_join_joined_data(const int64_t delta_table_idx,
                                                                         const JoinedTable *upper_table,
                                                                         ObSelectStmt *&inner_delta_mav)
{
  int ret = OB_SUCCESS;
  JoinedTable *cur_upper_table = NULL;
  inner_delta_mav = NULL;
  if (OB_ISNULL(upper_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper table is null", K(ret), K(delta_table_idx), K(upper_table));
  } else if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(delta_table_idx, inner_delta_mav))) {
    LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_joined_table(inner_delta_mav, upper_table->table_id_, cur_upper_table))) {
    LOG_WARN("failed to find cur upper table", K(ret), K(upper_table->table_id_), KPC(inner_delta_mav));
  } else if (OB_ISNULL(cur_upper_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur upper table is null", K(ret), K(upper_table->table_id_), KPC(inner_delta_mav), K(mv_def_stmt_));
  } else {
    cur_upper_table->joined_type_ = INNER_JOIN;
  }
  return ret;
}

int ObOuterJoinMAVPrinter::gen_inner_delta_mav_for_left_join_padded_null_data(const TableItem *delta_table,
                                                                              const int64_t delta_table_idx,
                                                                              const JoinedTable *upper_table,
                                                                              ObSelectStmt *&inner_delta_mav_new_null,
                                                                              ObSelectStmt *&inner_delta_mav_pre_null)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier_with_null_table(ctx_.expr_factory_);
  ObRawExprCopier copier_without_null_table(ctx_.expr_factory_);
  ObSEArray<ObRawExpr*, 8> join_conds;
  inner_delta_mav_new_null = NULL;
  inner_delta_mav_pre_null = NULL;
  if (OB_ISNULL(upper_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper table is null", K(ret), K(upper_table));
  } else if (OB_FAIL(create_simple_stmt(inner_delta_mav_new_null))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_stmt(inner_delta_mav_pre_null))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL(inner_delta_mav_new_null) || OB_ISNULL(inner_delta_mav_pre_null)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null inner delta mav stmt", K(ret), K(inner_delta_mav_new_null), K(inner_delta_mav_pre_null));
  } else if (OB_FAIL(construct_tables_for_padded_null_data(delta_table_idx,
                                                           copier_with_null_table,
                                                           copier_without_null_table,
                                                           inner_delta_mav_new_null))) {
    LOG_WARN("failed to construct table items for left join mav padded null data", K(ret));
  } else if (OB_FAIL(copier_with_null_table.copy_on_replace(mv_def_stmt_.get_condition_exprs(),
                                                            inner_delta_mav_new_null->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (OB_FAIL(copier_with_null_table.copy_on_replace(mv_def_stmt_.get_group_exprs(),
                                                            inner_delta_mav_new_null->get_group_exprs()))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (OB_FAIL(inner_delta_mav_pre_null->assign(*inner_delta_mav_new_null))) {
    LOG_WARN("failed to assign inner delta mav pre null", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier_with_null_table, NULL, 1,
                                                         inner_delta_mav_new_null->get_group_exprs(),
                                                         inner_delta_mav_new_null->get_select_items()))) {
    LOG_WARN("failed to generate select list ", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier_with_null_table, NULL, -1,
                                                         inner_delta_mav_pre_null->get_group_exprs(),
                                                         inner_delta_mav_pre_null->get_select_items()))) {
    LOG_WARN("failed to generate select list ", K(ret));
  } else if (OB_FAIL(copier_without_null_table.copy_on_replace(upper_table->join_conditions_, join_conds))) {
    LOG_WARN("failed to copy join conditions", K(ret));
  } else if (OB_FAIL(gen_exists_padded_null_conds(delta_table,
                                                  delta_table_idx,
                                                  join_conds,
                                                  true, /* is_new_null */
                                                  inner_delta_mav_new_null->get_condition_exprs()))) {
    LOG_WARN("failed to generate new null conds", K(ret));
  } else if (OB_FAIL(gen_exists_padded_null_conds(delta_table,
                                                  delta_table_idx,
                                                  join_conds,
                                                  false, /* is_new_null */
                                                  inner_delta_mav_pre_null->get_condition_exprs()))) {
    LOG_WARN("failed to generate pre null conds", K(ret));
  }
  return ret;
}

int ObOuterJoinMAVPrinter::construct_tables_for_padded_null_data(const int64_t delta_table_idx,
                                                                 ObRawExprCopier &copier_with_null_table,
                                                                 ObRawExprCopier &copier_without_null_table,
                                                                 ObSelectStmt *inner_delta_mav)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem*, 8> table_item_map; // original table item idx -> new table item pointer
  // 1. construct table items, and fill expr copier
  if (OB_ISNULL(inner_delta_mav)
      || OB_UNLIKELY(0 > delta_table_idx || mv_def_stmt_.get_table_size() <= delta_table_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(inner_delta_mav), K(delta_table_idx));
  } else if (OB_FAIL(table_item_map.prepare_allocate(mv_def_stmt_.get_table_size()))) {
    LOG_WARN("failed to prepare allocate table item map", K(ret), K(mv_def_stmt_.get_table_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    const TableItem *orig_table = NULL;
    TableItem *new_table = NULL;
    ObSelectStmt *view_stmt = NULL;
    if (OB_ISNULL(orig_table = mv_def_stmt_.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i));
    } else if (i == delta_table_idx || right_table_idxs_.at(delta_table_idx).has_member(i)) {
      // do nothing
    } else if (OB_FAIL(get_delta_pre_view_stmt(i, delta_table_idx, view_stmt))) {
      LOG_WARN("failed to get view stmt", K(ret));
    } else if (OB_FAIL(create_table_item_with_infos(inner_delta_mav,
                                                    orig_table,
                                                    new_table,
                                                    view_stmt,
                                                    PRE_TABLE_FORMAT_NAME,
                                                    false))) {
      LOG_WARN("failed to create simple table item", K(ret));
    } else if (OB_FAIL(init_expr_copier_for_table(orig_table,
                                                  new_table,
                                                  copier_without_null_table))) {
      LOG_WARN("failed to init expr copier for not null table", K(ret), K(i));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_expr_copier_for_table(orig_table,
                                                  new_table,
                                                  copier_with_null_table))) {
      LOG_WARN("failed to init expr copier for all table", K(ret), K(i));
    } else {
      table_item_map.at(i) = new_table;
    }
  }
  // 2. construct from items
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_from_item_size(); ++i) {
    TableItem *ori_from_table = NULL;
    TableItem *new_from_table = NULL;
    if (OB_FAIL(mv_def_stmt_.get_from_table(i, ori_from_table))) {
      LOG_WARN("failed to get from table", K(ret));
    } else if (OB_FAIL(construct_joined_table_for_padded_null_data(ori_from_table,
                                                                   table_item_map,
                                                                   inner_delta_mav,
                                                                   copier_with_null_table,
                                                                   new_from_table))) {
      LOG_WARN("failed to construct join table for left join mav padded null data", K(ret));
    } else if (NULL == new_from_table) {
      // do nothing, no need to add from item
    } else if (new_from_table->is_joined_table()
               && OB_FAIL(inner_delta_mav->add_joined_table(static_cast<JoinedTable*>(new_from_table)))) {
      LOG_WARN("failed to add joined table", K(ret));
    } else if (OB_FAIL(inner_delta_mav->add_from_item(new_from_table->table_id_, new_from_table->is_joined_table()))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObOuterJoinMAVPrinter::construct_joined_table_for_padded_null_data(const TableItem *ori_table,
                                                                       const ObIArray<TableItem*> &table_item_map,
                                                                       ObSelectStmt *inner_delta_mav,
                                                                       ObRawExprCopier &expr_copier,
                                                                       TableItem *&new_table)
{
  int ret = OB_SUCCESS;
  int64_t ori_table_idx = -1;
  new_table = NULL;
  if (OB_ISNULL(ori_table) || OB_ISNULL(inner_delta_mav)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ori_table), K(inner_delta_mav));
  } else if (ori_table->is_joined_table()) {
    const JoinedTable* ori_joined_table = static_cast<const JoinedTable*>(ori_table);
    TableItem *new_left_table = NULL;
    TableItem *new_right_table = NULL;
    if (OB_FAIL(SMART_CALL(construct_joined_table_for_padded_null_data(ori_joined_table->left_table_,
                                                                       table_item_map,
                                                                       inner_delta_mav,
                                                                       expr_copier,
                                                                       new_left_table)))) {
      LOG_WARN("failed to construct joined table for left child table", K(ret));
    } else if (OB_FAIL(SMART_CALL(construct_joined_table_for_padded_null_data(ori_joined_table->right_table_,
                                                                              table_item_map,
                                                                              inner_delta_mav,
                                                                              expr_copier,
                                                                              new_right_table)))) {
      LOG_WARN("failed to construct joined table for right child table", K(ret));
    } else if (NULL != new_left_table && NULL != new_right_table) {
      JoinedTable *new_joined_table = NULL;
      if (OB_ISNULL(new_joined_table = inner_delta_mav->create_joined_table(ctx_.alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("create joined table failed", K(ret));
      } else if (OB_FAIL(static_cast<TableItem*>(new_joined_table)->deep_copy(expr_copier, *ori_table, &ctx_.alloc_))) {
        LOG_WARN("failed to deep copy table item", K(ret));
      } else if (OB_FAIL(expr_copier.copy_on_replace(ori_joined_table->join_conditions_, new_joined_table->join_conditions_))) {
        LOG_WARN("failed to copy join condition exprs", K(ret));
      } else {
        new_joined_table->left_table_ = new_left_table;
        new_joined_table->right_table_ = new_right_table;
        new_joined_table->joined_type_ = ori_joined_table->joined_type_;
        new_joined_table->is_straight_join_ = ori_joined_table->is_straight_join_;
        new_table = new_joined_table;
      }
    } else if (NULL != new_left_table || NULL != new_right_table) {
      new_table = NULL != new_left_table ? new_left_table : new_right_table;
    } else {
      // do nothing
    }
  } else if (OB_FAIL(mv_def_stmt_.get_table_item_idx(ori_table, ori_table_idx))) {
    LOG_WARN("failed to get delta table idx", K(ret));
  } else if (OB_UNLIKELY(0 > ori_table_idx || table_item_map.count() <= ori_table_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table idx", K(ret), KPC(ori_table), K(ori_table_idx), K(table_item_map.count()));
  } else {
    new_table = table_item_map.at(ori_table_idx);
  }
  return ret;
}

int ObOuterJoinMAVPrinter::gen_exists_padded_null_conds(const TableItem *delta_table,
                                                        const int64_t delta_table_idx,
                                                        const ObIArray<ObRawExpr*> &join_conds,
                                                        bool is_new_null,
                                                        ObIArray<ObRawExpr*> &conds)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *delete_insert_data_view = NULL;
  ObRawExpr *exists_expr = NULL;
  ObRawExpr *not_exists_expr = NULL;
  if (OB_ISNULL(delta_table)
      || OB_UNLIKELY(0 > delta_table_idx || mv_def_stmt_.get_table_size() <= delta_table_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(delta_table), K(delta_table_idx), K(mv_def_stmt_.get_table_size()));
  } else if (OB_FAIL(gen_delete_insert_data_access_stmt(*delta_table,
                                                        is_new_null, /* is_delete_data */
                                                        delete_insert_data_view))) {
    LOG_WARN("failed to gen delete insert data view", K(ret));
  } else if OB_FAIL(create_simple_exists_expr(delta_table,
                                              delete_insert_data_view,
                                              is_new_null ? DELETE_TABLE_FORMAT_NAME : INSERT_TABLE_FORMAT_NAME,
                                              join_conds,
                                              true, /* is_exists */
                                              exists_expr)) {
    LOG_WARN("failed to create exists expr", K(ret));
  } else if OB_FAIL(create_simple_exists_expr(delta_table,
                                              is_new_null ? NULL : all_pre_datas_.at(delta_table_idx),
                                              is_new_null ? NULL : PRE_TABLE_FORMAT_NAME,
                                              join_conds,
                                              false, /* is_exists */
                                              not_exists_expr)) {
    LOG_WARN("failed to create exists expr", K(ret));
  } else if (OB_FAIL(conds.push_back(exists_expr))) {
    LOG_WARN("failed to push back exists expr", K(ret));
  } else if (OB_FAIL(conds.push_back(not_exists_expr))) {
    LOG_WARN("failed to push back not exists expr", K(ret));
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
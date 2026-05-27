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
#include "sql/resolver/mv/ob_simple_join_mav_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObSimpleJoinMAVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  const bool mysql_mode = lib::is_mysql_mode();
  if (OB_FAIL(gen_merge_for_simple_join_mav(dml_stmts))) {
    LOG_WARN("failed to gen merge into for simple mav", K(ret));
  }
  return ret;
}

int ObSimpleJoinMAVPrinter::gen_inner_delta_mav_for_mav(ObIArray<ObSelectStmt*> &inner_delta_mavs)
{
  int ret = OB_SUCCESS;
  inner_delta_mavs.reuse();
  const ObIArray<TableItem*> &source_tables = mv_def_stmt_.get_table_items();
  const TableItem *source_table = NULL;
  if (OB_FAIL(gen_delta_pre_data_views())) {
    LOG_WARN("failed to generate delta pre data views", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < source_tables.count(); ++i) {
    ObSelectStmt *inner_delta_mav = NULL;
    if (OB_ISNULL(source_table = source_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i));
    } else if (is_table_skip_refresh(*source_table)) {
      // do nothing, no need to gen inner delta mav
    } else if (OB_FAIL(gen_inner_delta_mav_for_simple_join_mav(i,
                                                               inner_delta_mav))) {
      LOG_WARN("failed to gen inner delta mav for simple join mav", K(ret));
    } else if (OB_FAIL(inner_delta_mavs.push_back(inner_delta_mav))) {
      LOG_WARN("failed to push back inner delta mav", K(ret));
    }
  }
  return ret;
}

int ObSimpleJoinMAVPrinter::gen_delta_pre_data_views()
{
  int ret = OB_SUCCESS;
  const TableItem *source_table = NULL;
  if (OB_FAIL(all_delta_datas_.prepare_allocate(mv_def_stmt_.get_table_size()))
      || OB_FAIL(all_pre_datas_.prepare_allocate(mv_def_stmt_.get_table_size()))) {
    LOG_WARN("failed to prepare allocate pre delta datas array", K(ret), K(mv_def_stmt_.get_table_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    all_delta_datas_.at(i) = NULL;
    all_pre_datas_.at(i) = NULL;
    if (OB_ISNULL(source_table = mv_def_stmt_.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(source_table));
    } else if (OB_FAIL(gen_pre_data_access_stmt(*source_table, all_pre_datas_.at(i)))) {
      LOG_WARN("failed to gen pre data access stmt", K(ret));
    } else if (is_table_skip_refresh(*source_table)) {
      // do nothing, no need to gen delta data access stmt
    } else if (OB_FAIL(gen_delta_data_access_stmt(*source_table, all_delta_datas_.at(i)))) {
      LOG_WARN("failed to gen delta data access stmt", K(ret));
    }
  }
  return ret;
}

int ObSimpleJoinMAVPrinter::gen_merge_for_simple_join_mav(ObIArray<ObDMLStmt *> &dml_stmts)
{
  int ret = OB_SUCCESS;
  dml_stmts.reuse();
  ObSEArray<ObSelectStmt *, 4> inner_delta_mavs;
  if (OB_FAIL(gen_inner_delta_mav_for_mav(inner_delta_mavs))) {
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

int ObSimpleJoinMAVPrinter::gen_inner_delta_mav_for_simple_join_mav(const int64_t inner_delta_no,
                                                                    ObSelectStmt *&inner_delta_mav)
{
  int ret = OB_SUCCESS;
  inner_delta_mav = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  const TableItem *delta_table = NULL;
  if (OB_FAIL(create_simple_stmt(inner_delta_mav))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(construct_table_items_for_simple_join_mav_delta_data(inner_delta_no,
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
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(),
                                            inner_delta_mav->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_group_exprs(),
                                            inner_delta_mav->get_group_exprs()))) {
    LOG_WARN("failed to generate group by exprs", K(ret));
  } else if (OB_FAIL(gen_simple_mav_delta_mv_select_list(copier, delta_table, 0,
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
int ObSimpleJoinMAVPrinter::construct_table_items_for_simple_join_mav_delta_data(const int64_t inner_delta_no,
                                                                                 ObSelectStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > inner_delta_no || inner_delta_no >= all_pre_datas_.count()
                  || all_pre_datas_.count() != all_delta_datas_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(inner_delta_no), K(all_pre_datas_.count()), K(all_delta_datas_.count()));
  } else {
    const TableItem *orig_table = NULL;
    TableItem *table = NULL;
    const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
    ObSelectStmt *view_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (OB_ISNULL(orig_table = orig_table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(orig_table_items));
      } else if (OB_FAIL(get_delta_pre_view_stmt(i, inner_delta_no, view_stmt))) {
        LOG_WARN("failed to get view stmt", K(ret));
      } else if (OB_FAIL(create_table_item_with_infos(stmt,
                                                      orig_table,
                                                      table,
                                                      view_stmt,
                                                      inner_delta_no == i ?
                                                      DELTA_TABLE_FORMAT_NAME :
                                                      PRE_TABLE_FORMAT_NAME,
                                                      false))) {
        LOG_WARN("failed to create simple table item", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObSimpleJoinMAVPrinter::get_delta_pre_view_stmt
 *
 * For inner_delta_no table, return delta data view
 * For unrefreshed table, return pre data view
 * For refreshed table, return NULL
 */
int ObSimpleJoinMAVPrinter::get_delta_pre_view_stmt(const int64_t table_idx,
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
  } else if (table_idx > inner_delta_no) {
    if (OB_ISNULL(view_stmt = all_pre_datas_.at(table_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pre data view", K(ret), K(table_idx));
    }
  } else {
    // do nothing, access current data
  }
  return ret;
}

// todo: for multi dml operators on the same rowkey, we need access at most two rows actually.
int ObSimpleJoinMAVPrinter::gen_delta_data_access_stmt(const TableItem &source_table,
                                                       ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  access_sel = NULL;
  const uint64_t mlog_sel_flags = MLOG_EXT_COL_DML_FACTOR | MLOG_EXT_COL_ALL_NORMAL_COL;
  if (OB_FAIL(gen_delta_mlog_table_view(source_table, access_sel, mlog_sel_flags))) {
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
int ObSimpleJoinMAVPrinter::gen_pre_data_access_stmt(const TableItem &source_table, ObSelectStmt *&access_sel)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *union_stmt = NULL;
  ObSelectStmt *unchanged_data_stmt = NULL;
  ObSelectStmt *deleted_data_stmt = NULL;
  access_sel = NULL;
  if (OB_FAIL(gen_pre_table_view(source_table, unchanged_data_stmt))) {
    LOG_WARN("failed to unchanged deleted data access stmt ", K(ret));
  } else if (is_table_skip_refresh(source_table)) {
    // only unchanged data for skip refresh table
    // access_sel: SELECT * FROM source_table;
    access_sel = unchanged_data_stmt;
  } else if (OB_FAIL(gen_delete_insert_data_access_stmt(source_table,
                                                        true, /* is_delete_data */
                                                        deleted_data_stmt))) {
    LOG_WARN("failed to generate deleted data access stmt ", K(ret));
  } else if (OB_FAIL(create_simple_stmt(union_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL(union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null union stmt", K(ret), K(union_stmt));
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

}//end of namespace sql
}//end of namespace oceanbase

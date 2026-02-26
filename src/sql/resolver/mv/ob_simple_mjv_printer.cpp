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
#include "sql/resolver/mv/ob_simple_mjv_printer.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObSimpleMJVPrinter::gen_refresh_dmls(ObIArray<ObDMLStmt*> &dml_stmts)
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

int ObSimpleMJVPrinter::gen_real_time_view(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  ObSelectStmt *access_mv_stmt = NULL;
  ObSEArray<ObSelectStmt*, 8> access_delta_stmts;
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(gen_access_mv_data_for_simple_mjv(access_mv_stmt))) {
    LOG_WARN("failed to generate access mv data for simple mjv", K(ret));
  } else if (OB_FAIL(sel_stmt->get_set_query().push_back(access_mv_stmt))) {
    LOG_WARN("failed to push back mv data into set query", K(ret));
  } else if (OB_FAIL(gen_access_delta_data_for_simple_mjv(access_delta_stmts))) {
    LOG_WARN("failed to generate access delta data for simple mjv", K(ret));
  } else if (OB_FAIL(append(sel_stmt->get_set_query(), access_delta_stmts))) {
    LOG_WARN("failed to append access delta stmts into set query", K(ret));
  } else {
    sel_stmt->assign_set_all();
    sel_stmt->assign_set_op(ObSelectStmt::UNION);
  }
  return ret;
}

int ObSimpleMJVPrinter::gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt *base_del_stmt = NULL;
  ObDeleteStmt *del_stmt = NULL;
  TableItem *mv_table = NULL;
  ObRawExpr *marker_filter = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  if (OB_FAIL(create_simple_stmt(base_del_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(base_del_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (ctx_.for_union_all_child_query()
             && OB_FAIL(create_union_all_child_refresh_filter(mv_table, marker_filter))) {
    LOG_WARN("failed to create union all child refresh filter", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
    ObRawExpr *semi_filter = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      if (OB_ISNULL(orig_table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table item", K(ret), K(i));
      } else if (is_table_skip_refresh(*orig_table_items.at(i))) {
        // do nothing, no need to gen delete stmt
      } else if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i), mv_table, true, false, true, semi_filter))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (orig_table_items.count() - 1 == i) {
        if (OB_FAIL(base_del_stmt->add_condition_expr(semi_filter))) {
          LOG_WARN("failed to push back semi filter", K(ret));
        } else if (OB_FAIL(add_semi_to_inner_hint(base_del_stmt))) {
          LOG_WARN("failed to add semi to inner hint", K(ret));
        } else if (NULL != marker_filter
                   && OB_FAIL(base_del_stmt->add_condition_expr(marker_filter))) {
          LOG_WARN("failed to push back maker filter", K(ret));
        } else if (OB_FAIL(dml_stmts.push_back(base_del_stmt))) {
          LOG_WARN("failed to push back delete stmt", K(ret));
        }
      } else if (OB_FAIL(create_simple_stmt(del_stmt))) {
        LOG_WARN("failed to create simple stmt", K(ret));
      } else if (OB_FAIL(del_stmt->get_table_items().assign(base_del_stmt->get_table_items()))
                || OB_FAIL(del_stmt->get_from_items().assign(base_del_stmt->get_from_items()))) {
        LOG_WARN("failed to assign structure", K(ret));
      } else if (OB_FAIL(del_stmt->add_condition_expr(semi_filter))) {
        LOG_WARN("failed to push back semi filter", K(ret));
      } else if (OB_FAIL(add_semi_to_inner_hint(del_stmt))) {
        LOG_WARN("failed to add semi to inner hint", K(ret));
      } else if (NULL != marker_filter
                 && OB_FAIL(del_stmt->add_condition_expr(marker_filter))) {
        LOG_WARN("failed to push back maker filter", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(del_stmt))) {
        LOG_WARN("failed to push back delete stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleMJVPrinter::gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  ObInsertStmt *base_insert_stmt = NULL;
  ObSEArray<ObSelectStmt*, 8> access_delta_stmts;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  if (OB_FAIL(gen_access_delta_data_for_simple_mjv(access_delta_stmts))) {
    LOG_WARN("failed to generate access delta data for simple mjv", K(ret));
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
    for (int64_t i = 0; OB_SUCC(ret) && i < access_delta_stmts.count(); ++i) {
      if (access_delta_stmts.count() - 1 == i) {
        insert_stmt = base_insert_stmt;
      } else if (OB_FAIL(create_simple_stmt(insert_stmt))) {
        LOG_WARN("failed to create simple stmt", K(ret));
      } else if (OB_FAIL(insert_stmt->get_values_desc().assign(base_insert_stmt->get_values_desc()))
                 || OB_FAIL(insert_stmt->get_table_items().assign(base_insert_stmt->get_table_items()))) {
        LOG_WARN("failed to assign structure", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(create_simple_table_item(insert_stmt, DELTA_MV_VIEW_NAME, source_table, access_delta_stmts.at(i)))) {
        LOG_WARN("failed to create simple table item", K(ret));
      } else if (OB_FAIL(dml_stmts.push_back(insert_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleMJVPrinter::gen_access_mv_data_for_simple_mjv(ObSelectStmt *&sel_stmt)
{
  int ret = OB_SUCCESS;
  sel_stmt = NULL;
  TableItem *mv_table = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
  if (OB_FAIL(create_simple_stmt(sel_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(sel_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else if (OB_FAIL(sel_stmt->get_select_items().prepare_allocate(orig_select_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    ObIArray<SelectItem> &select_items = sel_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_FAIL(create_simple_column_expr(mv_table->get_table_name(), orig_select_items.at(i).alias_name_,
                                            mv_table->table_id_, select_items.at(i).expr_))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else {
        select_items.at(i).alias_name_ = orig_select_items.at(i).alias_name_;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
      ObRawExpr *anti_filter = NULL;
      if (OB_ISNULL(orig_table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table item", K(ret), K(i));
      } else if (is_table_skip_refresh(*orig_table_items.at(i))) {
        // do nothing, no need to gen exists cond
      } else if (OB_FAIL(gen_exists_cond_for_table(orig_table_items.at(i), mv_table, false, false, true, anti_filter))) {
        LOG_WARN("failed to create simple column exprs", K(ret));
      } else if (OB_FAIL(sel_stmt->add_condition_expr(anti_filter))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleMJVPrinter::gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> anti_filters;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
  if (OB_UNLIKELY(orig_table_items.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(orig_table_items.count()));
  } else if (OB_FAIL(anti_filters.prepare_allocate(orig_table_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret), K(orig_table_items.count()));
  }

  // generate anti filters
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
    if (OB_ISNULL(orig_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table item", K(ret), K(i));
    } else if (is_table_skip_refresh(*orig_table_items.at(i))) {
      // do nothing, no need to gen exists cond
    } else if (OB_FAIL(gen_pre_scn_filter_for_table(*orig_table_items.at(i),
                                                    anti_filters.at(i)))) {
      LOG_WARN("failed to generate pre scn filter", K(ret));
    }
  }

  // generate access delta data stmt for each delta table
  for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_items.count(); ++i) {
    const TableItem *ori_table = mv_def_stmt_.get_table_item(i);
    ObSelectStmt *delta_stmt = NULL;
    if (OB_ISNULL(orig_table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table item", K(ret), K(i));
    } else if (is_table_skip_refresh(*orig_table_items.at(i))) {
      // do nothing, no need to gen exists cond
    } else if (OB_FAIL(gen_one_access_delta_data_for_simple_mjv(i, anti_filters, delta_stmt))) {
      LOG_WARN("failed to generate one access delta data stmt", K(ret));
    } else if (OB_FAIL(access_delta_stmts.push_back(delta_stmt))) {
      LOG_WARN("failed to push back stmt", K(ret));
    }
  }
  return ret;
}

int ObSimpleMJVPrinter::gen_one_access_delta_data_for_simple_mjv(const int64_t delta_table_idx,
                                                                 ObIArray<ObRawExpr*> &anti_filters,
                                                                 ObSelectStmt *&delta_stmt)
{
  int ret = OB_SUCCESS;
  delta_stmt = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  ObSelectStmt *delta_table_stmt = NULL;
  const TableItem *ori_delta_table = NULL;
  if (OB_UNLIKELY(delta_table_idx < 0 || delta_table_idx >= mv_def_stmt_.get_table_size()
                  || mv_def_stmt_.get_table_size() != anti_filters.count())
      || OB_ISNULL(ori_delta_table = mv_def_stmt_.get_table_item(delta_table_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(delta_table_idx), K(anti_filters.count()),
                                      K(mv_def_stmt_.get_table_size()));
  } else if (OB_FAIL(gen_delta_table_view(*ori_delta_table, delta_table_stmt))) {
    LOG_WARN("failed to generate delta table view", K(ret), K(delta_table_idx));
  } else if (OB_FAIL(create_simple_stmt(delta_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_ISNULL(delta_stmt) || OB_ISNULL(delta_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(delta_stmt), K(delta_table_stmt));
  }

  // 1. construct table items
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    const TableItem *ori_table = mv_def_stmt_.get_table_item(i);
    TableItem *new_table = NULL;
    if (OB_FAIL(create_table_item_with_infos(delta_stmt,
                                             ori_table,
                                             new_table,
                                             delta_table_idx == i ? delta_table_stmt : NULL,
                                             delta_table_idx == i ? DELTA_TABLE_FORMAT_NAME : NULL,
                                             false))) {
      LOG_WARN("failed to create simple table item", K(ret));
    }
  }

  // 2. construct joined table, from items, condition exprs
  if OB_FAIL(ret) {
  } else if (OB_FAIL(init_expr_copier_for_stmt(*delta_stmt, copier))) {
    LOG_WARN("failed to init expr copier for stmt", K(ret));
  } else if (OB_FAIL(construct_from_items_for_simple_mjv_delta_data(copier, *delta_stmt))) {
    LOG_WARN("failed to construct from items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(), delta_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  }

  // 3. construct select items
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_def_stmt_.get_select_item_size(); ++i) {
    const SelectItem &ori_sel_item = mv_def_stmt_.get_select_item(i);
    SelectItem new_sel_item;
    new_sel_item.is_real_alias_ = true;
    new_sel_item.alias_name_ = ori_sel_item.alias_name_;
    if (OB_FAIL(copier.copy_on_replace(ori_sel_item.expr_, new_sel_item.expr_))) {
      LOG_WARN("failed to copy select expr", K(ret));
    } else if (OB_FAIL(delta_stmt->get_select_items().push_back(new_sel_item))) {
      LOG_WARN("failed to push back select item", K(ret));
    }
  }

  // 4. add anti join filter
  for (int64_t i = delta_table_idx + 1; OB_SUCC(ret) && i < mv_def_stmt_.get_table_size(); ++i) {
    if (NULL == anti_filters.at(i)) {
      /* do nothing */
    } else if (OB_FAIL(delta_stmt->add_condition_expr(anti_filters.at(i)))) {
      LOG_WARN("failed to push back anti join filter", K(ret));
    }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase

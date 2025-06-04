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

int ObSimpleMJVPrinter::gen_delete_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt *base_del_stmt = NULL;
  ObDeleteStmt *del_stmt = NULL;
  TableItem *mv_table = NULL;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  if (OB_FAIL(create_simple_stmt(base_del_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(create_simple_table_item(base_del_stmt, mv_schema_.get_table_name(), mv_table))) {
    LOG_WARN("failed to create simple table item", K(ret));
  } else {
    mv_table->database_name_ = mv_db_name_;
    const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
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

int ObSimpleMJVPrinter::gen_insert_into_select_for_simple_mjv(ObIArray<ObDMLStmt*> &dml_stmts)
{
  int ret = OB_SUCCESS;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  ObInsertStmt *base_insert_stmt = NULL;
  ObSEArray<ObSelectStmt*, 8> access_delta_stmts;
  const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
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

int ObSimpleMJVPrinter::gen_access_delta_data_for_simple_mjv(ObIArray<ObSelectStmt*> &access_delta_stmts)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *base_delta_stmt = NULL;
  ObSEArray<ObRawExpr*, 2> semi_filters;
  ObSEArray<ObRawExpr*, 2> anti_filters;
  const int64_t table_size = mv_def_stmt_.get_table_items().count();
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

int ObSimpleMJVPrinter::prepare_gen_access_delta_data_for_simple_mjv(ObSelectStmt *&base_delta_stmt,
                                                                     ObIArray<ObRawExpr*> &semi_filters,
                                                                     ObIArray<ObRawExpr*> &anti_filters)
{
  int ret = OB_SUCCESS;
  base_delta_stmt = NULL;
  ObRawExprCopier copier(ctx_.expr_factory_);
  ObSemiToInnerHint *semi_to_inner_hint = NULL;
  ObSEArray<ObItemType, 1> conflict_hints;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
  if (OB_FAIL(semi_filters.prepare_allocate(orig_table_items.count()))
      || OB_FAIL(anti_filters.prepare_allocate(orig_table_items.count()))) {
    LOG_WARN("failed to prepare allocate arrays", K(ret), K(orig_table_items.count()));
  } else if (OB_FAIL(create_simple_stmt(base_delta_stmt))) {
    LOG_WARN("failed to create simple stmt", K(ret));
  } else if (OB_FAIL(construct_table_items_for_simple_mjv_delta_data(base_delta_stmt))) {
    LOG_WARN("failed to construct table items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(init_expr_copier_for_stmt(*base_delta_stmt, copier))) {
    LOG_WARN("failed to init expr copier for stmt", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint(&ctx_.alloc_, T_SEMI_TO_INNER, semi_to_inner_hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(base_delta_stmt->get_stmt_hint().merge_hint(*semi_to_inner_hint,
                                                                  ObHintMergePolicy::HINT_DOMINATED_EQUAL,
                                                                  conflict_hints))) {
    LOG_WARN("failed to merge hint", K(ret));
  } else if (OB_FAIL(construct_from_items_for_simple_mjv_delta_data(copier, *base_delta_stmt))) {
    LOG_WARN("failed to construct from items for simple mjv delta data", K(ret));
  } else if (OB_FAIL(copier.copy_on_replace(mv_def_stmt_.get_condition_exprs(), base_delta_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to deep copy where conditions", K(ret));
  } else {
    SelectItem sel_item;
    const ObIArray<TableItem*> &cur_table_items = base_delta_stmt->get_table_items();
    const ObIArray<SelectItem> &orig_select_items = mv_def_stmt_.get_select_items();
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

int ObSimpleMJVPrinter::construct_table_items_for_simple_mjv_delta_data(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  const TableItem *orig_table = NULL;
  TableItem *table = NULL;
  const ObIArray<TableItem*> &orig_table_items = mv_def_stmt_.get_table_items();
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

// delta_mv = delta_t1 join pre_t2 join pre_t3 ... join pre_tn
//            union all t1 join delta_t2 join pre_t3 ... join pre_tn
//            ...
//            union all t1 join t2 join t3 ... join delta_tn
// input table_idx specify the delta table
int ObSimpleMJVPrinter::gen_one_access_delta_data_for_simple_mjv(const ObSelectStmt &base_delta_stmt,
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

}//end of namespace sql
}//end of namespace oceanbase

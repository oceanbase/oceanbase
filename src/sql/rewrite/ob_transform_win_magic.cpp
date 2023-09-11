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

#define USING_LOG_PREFIX SQL_REWRITE
#include "ob_transform_win_magic.h"
#include "ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/schema/ob_table_schema.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_equal_analysis.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

/**
 * The origin win magic implements the paper <WinMagic: Subquery Elimination Using Window Aggregation> 
 * And the subquery is in where condition in the paper.
 * Now we try realizing it in another way. The subquery is not in where condition, while the from item is view. 
 * Thus we can merge some rewrite rules, and make the rewrite path more clear. 
 * @brief ObTransformWinMagic::transform_one_stmt
 * @return
 */

int ObTransformWinMagic::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                            ObDMLStmt *&stmt,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  int64_t drill_down_idx = -1;
  int64_t roll_up_idx = -2; // -1 means main stmt
  ObStmtCompareContext context;
  ObStmtMapInfo map_info;
  ObSEArray<TableItem *, 4> trans_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(get_view_to_trans(stmt, drill_down_idx, roll_up_idx, context, map_info, trans_tables))) {
    LOG_WARN("get view to trans failed", K(ret));
  } else if (drill_down_idx == -1 || roll_up_idx == -2) {
    // no valid item to do trans
  } else if (OB_FAIL(do_transform(parent_stmts, stmt, drill_down_idx, 
                                  roll_up_idx, map_info, context, trans_happened, trans_tables))) {
    LOG_WARN("do win magic transform for from failed", K(ret));
  } 
  return ret;
}

int ObTransformWinMagic::check_hint_valid(ObDMLStmt &stmt, TableItem &table, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL; 
  const ObWinMagicHint *hint = static_cast<const ObWinMagicHint*>(get_hint(stmt.get_stmt_hint()));
  if (NULL == hint) {
    is_valid = true;
    LOG_TRACE("check win magic hint is null", K(is_valid), K(table), K(stmt.get_stmt_hint()));
  } else if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_hint));
  } else {
    is_valid = hint->enable_win_magic(query_hint->cs_type_, table);
    LOG_TRACE("succeed to check win magic hint valid", K(is_valid), K(table), K(*hint));
  }
  return ret;
}

int ObTransformWinMagic::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<TableItem *, 4> single_or_joined_table;
  ObIArray<single_or_joined_table> *win_magic_tables = NULL;
  ObWinMagicHint *hint = NULL;
  
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params) ||
      OB_ISNULL(win_magic_tables = static_cast<ObIArray<single_or_joined_table>*>(trans_params)) ||
      OB_UNLIKELY(win_magic_tables->count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(win_magic_tables));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_WIN_MAGIC, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
    for (int64_t i = 0; OB_SUCC(ret) && i < win_magic_tables->count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < win_magic_tables->at(i).count(); ++j) {
        ObTableInHint *hint_table = NULL; 
        TableItem *table = win_magic_tables->at(i).at(j);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(hint->get_tb_name_list().push_back(ObTableInHint(table->qb_name_,
                                              table->database_name_, table->get_object_name())))) {
          LOG_WARN("failed to push back hint table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::construct_trans_table(const ObDMLStmt *stmt,
                                              const TableItem *table,
                                              ObIArray<ObSEArray<TableItem *, 4>> &trans_basic_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> trans_table;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(i));
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(trans_table.push_back(table))) {
        LOG_WARN("failed to push back trans table", K(ret));
      }
    }
  } else if (OB_FAIL(trans_table.push_back(const_cast<TableItem *>(table)))) {
    LOG_WARN("failed to push back table", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(trans_basic_tables.push_back(trans_table))) {
    LOG_WARN("failed to push back trans tables", K(ret));
  }
  return ret;
}

int ObTransformWinMagic::construct_trans_tables(const ObDMLStmt *stmt,
                                                const ObDMLStmt *trans_stmt,
                                                ObIArray<ObSEArray<TableItem *, 4>> &trans_basic_tables,
                                                ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_tables.count(); ++i) {
      int64_t idx;
      const TableItem *table_item = NULL;
      if (OB_ISNULL(trans_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(stmt->get_table_item_idx(trans_tables.at(i), idx))) {
        LOG_WARN("get table item idx failed", K(ret));
      } else if (OB_LIKELY(idx >= 0 && idx < trans_stmt->get_table_size()) &&
                 OB_ISNULL(table_item = trans_stmt->get_table_item(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(construct_trans_table(stmt,
                                               table_item,
                                               trans_basic_tables))) {
        LOG_WARN("failed to construct eliminated table", K(ret));
      }
    }
  }
  return ret;
}
/*
After aggr first join aggregation, we may get a sql like:

SELECT * 
FROM TICKETS P,
    (SELECT film, max(price) as agg FROM TICKETS T GROUP BY film) V
WHERE P.film = V.film AND P.price < V.agg;

The join can be eliminated by the window functions.

SELECT * 
FROM (SELECT film, max(price) over (partition by film), ... 
      from  TICKETS T where ...) V
WHERE V.price < V.agg;

This is a more intuitive transformation for win magic.
*/
int ObTransformWinMagic::do_transform(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                      ObDMLStmt *&stmt,
                                      int64_t drill_down_idx,
                                      int64_t roll_up_idx,
                                      ObStmtMapInfo &map_info,
                                      ObStmtCompareContext &context,
                                      bool &trans_happened,
                                      ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  ObDMLStmt *trans_stmt = NULL;
  bool accepted = false;
  ObSEArray<ObSEArray<TableItem *, 4>, 4> trans_basic_tables;
  ObTryTransHelper try_trans_helper;
  const ObWinMagicHint *myhint = static_cast<const ObWinMagicHint*>(stmt->get_stmt_hint().get_normal_hint(T_WIN_MAGIC));
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx()) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or stmt ctx is null", K(ret));
  } else if (NULL != myhint && myhint->is_disable_hint()) {
    LOG_TRACE("hint is diabled", K(*myhint));
  } else if (OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      stmt,
                                                      trans_stmt))) {
    LOG_WARN("deep copy stmt failed", K(ret)); 
  } else if (OB_FAIL(construct_trans_tables(stmt, trans_stmt, trans_basic_tables, trans_tables))) {
    LOG_WARN("construct tans tables faield", K(ret));
  } else if (OB_FAIL(do_transform_from_type(trans_stmt, drill_down_idx, roll_up_idx, 
                                            context, map_info))) {
    LOG_WARN("win magic do transform from type failed", K(ret));
  } else if (OB_FAIL(try_to_push_down_join(trans_stmt))) {
    LOG_WARN("try to push down join failed.", K(*trans_stmt));
  } else if (OB_FAIL(accept_transform(parent_stmts,
                                      stmt,
                                      trans_stmt,
                                      NULL != myhint && myhint->is_enable_hint(),
                                      false,
                                      accepted))) {
    LOG_WARN("accept transform failed", K(ret));
  } else if (!accepted) {
    LOG_TRACE("win magic transform not accpeted", K(ret));
    if (OB_FAIL(try_trans_helper.recover(stmt->get_query_ctx()))) {
      LOG_WARN("failed to recover params", K(ret));
    } 
  } else if (OB_FAIL(add_transform_hint(*stmt, &trans_basic_tables))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_,
                            map_info.equal_param_map_))) {
    LOG_WARN("failed to append equal params constraints", K(ret));
  } else {
    add_trans_type(ctx_->happened_cost_based_trans_, WIN_MAGIC);
    trans_happened = true;
    LOG_TRACE("equal param constraints", K(map_info.equal_param_map_));
  } 
  return ret;
}

/*
select ...
from T, (select agg(c1) aggr, c2
        from T
        group by c2
        ) v
where T.c2  = v.c2
      and ...;
=> 
select ...
from (select agg(c1) over(partition by c2) as aggr, c2, ... //partition by's expr is same as group by's
      from T //if has numerous tables,then they should be lossless join
      where c2 is not null) v  //the c2 is not null is referred by the T.c2 = V.c2;
where ...
*/
int ObTransformWinMagic::do_transform_from_type(ObDMLStmt *&stmt, 
                                                const int64_t drill_down_idx, 
                                                const int64_t roll_up_idx, 
                                                ObStmtCompareContext &context,
                                                ObStmtMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *main_stmt = stmt;
  TableItem *drill_down_table = NULL;
  TableItem *roll_up_table = NULL;
  ObSelectStmt *drill_down_stmt = NULL;
  ObDMLStmt *roll_up_stmt = NULL;
  bool match_main =false; 

  if (OB_ISNULL(stmt) ||
      OB_ISNULL(drill_down_table = main_stmt->get_table_item(main_stmt->get_from_item(drill_down_idx))) ||
      OB_ISNULL(drill_down_stmt = drill_down_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(drill_down_table), K(drill_down_stmt));
  } else if (roll_up_idx == -1) {
    roll_up_stmt = main_stmt;
    match_main = true;
  } else if (OB_ISNULL(roll_up_table = main_stmt->get_table_item(main_stmt->get_from_item(roll_up_idx))) ||
             OB_ISNULL(roll_up_stmt = roll_up_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rollup table is invalid", K(roll_up_table), K(roll_up_stmt));
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (match_main) {
    if (OB_FAIL(adjust_column_and_table(main_stmt, drill_down_table, map_info))) {
      LOG_WARN("adjust column and table failed");
    } else if (OB_FAIL(adjust_agg_to_win(drill_down_stmt))) {
      LOG_WARN("adjust agg to win faield", K(ret));
    }
  } else {
    TableItem *transed_view_table = NULL;
    ObSEArray<ObRawExpr *, 4> transed_output;
    if (OB_FAIL(adjust_view_for_trans(main_stmt, drill_down_table, roll_up_table, 
                                      transed_view_table, transed_output, context, map_info))) {
      LOG_WARN("adjust view for trans failed", K(ret));
    } else if (OB_FAIL(adjust_win_after_group_by(main_stmt, drill_down_table, roll_up_table, 
                                                 transed_view_table, transed_output, context, map_info))) {
      LOG_WARN("create view and win failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_dup_condition(main_stmt))) {
      LOG_WARN("remove dup condition failed", K(ret));
    } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item relation id", K(ret));
    } else if (OB_FAIL(drill_down_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(drill_down_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item relation id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt info", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~(1 << transformer_type_));
  }
  return ret;
}

int ObTransformWinMagic::check_aggr_expr_validity(ObSelectStmt &subquery, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (subquery.get_select_item_size() != 1) {
    is_valid = false;
  } else if (OB_ISNULL(subquery.get_select_item(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr is null", K(ret));
  } else {
    is_valid = subquery.get_select_item(0).expr_->has_flag(CNT_AGG);
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_aggr_item_size(); ++i) {
    if (OB_ISNULL(subquery.get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregation expr is null", K(ret));
    } else if (subquery.get_aggr_item(i)->is_param_distinct()) {
      is_valid = false;
    } else if (subquery.get_aggr_item(i)->get_expr_type() != T_FUN_MIN &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_MAX &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_SUM &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_COUNT) {
      is_valid = false;
    }
  }
  return ret;
}

// TODO (link.zt)
int ObTransformWinMagic::check_lossess_join(ObSelectStmt &subquery,
                                            ObIArray<FromItem> &from_items,
                                            ObIArray<ObRawExpr *> &conditions,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  UNUSED(subquery);
  is_valid = from_items.empty() && conditions.empty();
  return ret;
}

int ObTransformWinMagic::create_window_function(ObAggFunRawExpr *agg_expr,
                                                ObIArray<ObRawExpr *> &partition_exprs,
                                                ObWinFunRawExpr *&win_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_) || OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(expr_factory), K(agg_expr));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_WINDOW_FUNCTION,
                                                   win_expr))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(win_expr));
  } else if (OB_FAIL(win_expr->set_partition_exprs(partition_exprs))) {
    LOG_WARN("fail to set partition exprs", K(ret));
  } else {
    ObItemType func_type = agg_expr->get_expr_type();
    Bound upper;
    Bound lower;
    upper.type_ = BOUND_UNBOUNDED;
    lower.type_ = BOUND_UNBOUNDED;
    upper.is_preceding_ = true;
    lower.is_preceding_ = false;
    win_expr->set_func_type(func_type);
    win_expr->set_window_type(WINDOW_RANGE);
    win_expr->set_is_between(true);
    win_expr->set_upper(upper);
    win_expr->set_lower(lower);
    win_expr->set_agg_expr(agg_expr);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(win_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize windown function", K(ret));
    } else if (OB_FAIL(win_expr->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

/*-from type-*/
int ObTransformWinMagic::check_view_table_basic(ObSelectStmt *stmt, bool &is_valid) 
{
  int ret = OB_SUCCESS;
  bool has_rand = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
    //roll up stmt should have coarser granularity.
  } else if (stmt->has_limit()
            || stmt->has_having()
            || stmt->is_hierarchical_query()
            || stmt->is_set_stmt()
            || stmt->has_window_function()
            || stmt->has_rollup()
            || stmt->get_table_size() < 1
            || stmt->get_user_var_size() > 0) {
    is_valid = false;
  } else if (OB_FAIL(stmt->has_rand(has_rand))) {
    LOG_WARN("has rand failed", K(ret));
  } else if (has_rand) {
    is_valid = false;
  } else if (OB_FAIL(check_select_expr_validity(*stmt, is_valid))) {
    LOG_WARN("check view select expr validity failed");
  }
  return ret;
}

int ObTransformWinMagic::check_view_valid_to_trans(ObSelectStmt *view, ObStmtMapInfo &map_info, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<FromItem, 4> lossless_froms;
  ObSEArray<ObRawExpr *, 4> lossless_conditions;
  if (OB_ISNULL(view)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  }
  // for select * from t1 a, t1 b, (select * from t1 c) v where ...
  // table c should match which table? a or b ?
  for (int64_t i = 0; is_valid && i < view->get_from_item_size(); ++i) {
    // the from item in view has no match, have to check lossless
    if (OB_INVALID_ID == map_info.from_map_.at(i)) {
      if (OB_FAIL(lossless_froms.push_back(view->get_from_item(i)))) {
        LOG_WARN("failed to push from item into lossless froms array", K(ret));
      }
    }
  }
  
  if (OB_FAIL(ret)) {
    // no table is matched
  } else if (lossless_froms.count() == view->get_from_item_size()) {
    is_valid = false;
  }
  // check if the condition is in main stmt.
  for (int64_t i = 0; is_valid && i < view->get_condition_size(); ++i) {
    if (OB_INVALID_ID == map_info.cond_map_.at(i)) {
      if (OB_FAIL(lossless_conditions.push_back(view->get_condition_expr(i)))) {
        LOG_WARN("failed to push from item into lossless froms array", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(check_lossess_join(*view, lossless_froms, lossless_conditions, is_valid))) {
      LOG_WARN("failed check is lossess join", K(ret));
    } else if (!is_valid) {
      LOG_TRACE("has extra conditions or from items", K(ret));
      OPT_TRACE("view has extra conditions or from items");
    }
  }

  return ret;
}

int ObTransformWinMagic::sanity_check_and_init(ObDMLStmt *stmt, 
                                               ObDMLStmt *view, 
                                               ObStmtMapInfo &map_info, 
                                               ObStmtCompareContext &context) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(view)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (view->get_from_item_size() != map_info.from_map_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("from item size does not match to from map count", K(ret));
  } else if (view->get_condition_size() != map_info.cond_map_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condtition size does not match to cond map count", K(ret));
  } else if (view->get_table_size() != map_info.table_map_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table size does not match to table map count", K(ret));
  } else if (FALSE_IT(context.init(view, stmt, map_info, nullptr))) {
    //nvr
  }
  return ret;
}


/* if a view can match main stmt ,and also another view.
  * we do the transform with the main stmt first.
  */
int ObTransformWinMagic::get_view_to_trans(ObDMLStmt *&stmt, 
                                           int64_t &drill_down_idx,
                                           int64_t &roll_up_idx, 
                                           ObStmtCompareContext &context,
                                           ObStmtMapInfo &map_info,
                                           ObIArray<TableItem *> &trans_tables) 
{
  int ret = OB_SUCCESS;
  drill_down_idx = -1;
  roll_up_idx = -2; //-1 means main stmt
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  }
  // compare generated_table with stmt
  for (int64_t i = 0; OB_SUCC(ret) && drill_down_idx == -1 && i < stmt->get_from_item_size(); i++) {
    bool is_valid = true;
    TableItem *rewrite_table = NULL;
    ObSelectStmt *rewrite_view = NULL;
    if (stmt->get_from_item(i).is_joined_) {
      //do nothing
    } else if (OB_ISNULL(rewrite_table = stmt->get_table_item_by_id(stmt->get_from_item(i).table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret));
    } else if (rewrite_table->is_generated_table()) {
      rewrite_view = rewrite_table->ref_query_;
      OPT_TRACE("try to transform view:", rewrite_table);
      if (OB_ISNULL(rewrite_view)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view stmt is null", K(ret));
      } else if (OB_FAIL(check_view_table_basic(rewrite_view, is_valid))) {
        LOG_WARN("check view table basic failed", K(ret));
      } else if (!is_valid) {
        //do nothing
        OPT_TRACE("not a valid stmt");
      } else if (OB_FAIL(ObStmtComparer::compute_stmt_overlap(rewrite_view, 
                                                              stmt, 
                                                              map_info))) {
        LOG_WARN("compute stmt overlap failed", K(ret));
      } else if (OB_FAIL(sanity_check_and_init(stmt, rewrite_view, map_info, context))) {
        LOG_WARN("sanity check and init failed", K(ret));
      } else if (OB_FAIL(check_view_valid_to_trans(rewrite_view, map_info, is_valid))) {
        LOG_WARN("check view valid to trans failed", K(ret));
      } else if (OB_FAIL(check_stmt_and_view(stmt, rewrite_table, map_info, is_valid, trans_tables))) {
        LOG_WARN("check stmt and view failed", K(ret));
      } else if (is_valid) {
        drill_down_idx = i; //rewrite_view idx
        roll_up_idx = -1;
      } else {
        OPT_TRACE("can not transform");
      }
    }
  }

  //compare generated_table with generated table
  for (int64_t i = 0; OB_SUCC(ret) && drill_down_idx == -1 && i < stmt->get_from_item_size(); i++) {
    ObSelectStmt *drill_down_view = NULL;
    TableItem *drill_down_table = NULL;
    if (OB_ISNULL(drill_down_table = stmt->get_table_item(stmt->get_from_item(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret));
    } else if (drill_down_table->is_generated_table()) {
      drill_down_view = drill_down_table->ref_query_;
      bool is_valid = true;
      if (OB_ISNULL(drill_down_view)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view stmt is null", K(ret));
      } else if (OB_FAIL(check_view_table_basic(drill_down_view, is_valid))) {
        LOG_WARN("check table basic failed", K(ret));
      } else if (is_valid) {
        for (int64_t j = 0; OB_SUCC(ret) && drill_down_idx == -1 && j < stmt->get_from_item_size(); j++) {
          bool is_valid = true;
          TableItem *roll_up_table = NULL;
          ObSelectStmt *roll_up_view = NULL;
          if (i == j) {
            //do nothing
          } else if (OB_ISNULL(roll_up_table = stmt->get_table_item(stmt->get_from_item(j)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("view stmt is null", K(ret));
          } else if (roll_up_table->is_generated_table()) {
            roll_up_view = roll_up_table->ref_query_;
            if (OB_ISNULL(roll_up_view)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("view stmt is null", K(ret));
            } else if (OB_FAIL(check_view_table_basic(roll_up_view, is_valid))) {
              LOG_WARN("check view table basic failed", K(ret));
            } else if (!is_valid) {
              //do nothing
            } else if (drill_down_view->get_from_item_size() != roll_up_view->get_from_item_size()
                      || drill_down_view->get_table_size() != roll_up_view->get_table_size()) {
              is_valid = false;
            } else if (OB_FAIL(ObStmtComparer::compute_stmt_overlap(drill_down_view,
                                                                    roll_up_view, 
                                                                    map_info))) {
              LOG_WARN("compute stmt overlap failed", K(ret));
            } else if (OB_FAIL(sanity_check_and_init(roll_up_view, drill_down_view, map_info, context))) {
              LOG_WARN("sanity check and init failed", K(ret));
            } else if (OB_FAIL(check_view_valid_to_trans(drill_down_view, map_info, is_valid))) {
              LOG_WARN("sanity view valid to trans failed", K(ret));
            } else if (OB_FAIL(check_view_and_view(stmt, drill_down_table, roll_up_table, 
                                                   map_info, is_valid, trans_tables))) {
              LOG_WARN("check view and view failed", K(ret));
            } else if (!is_valid) {
              //do nothing
              //T_FUN_COUNT_SUM window function not implemented.
            } else if (OB_FAIL(check_mode_and_agg_type(roll_up_view, is_valid))) {
              LOG_WARN("check mode and agg type failed", K(ret));
            } else if (is_valid) {
              drill_down_idx = i;
              roll_up_idx = j;
            }
          }
        }
      }
    }
  }

  // avoid empty table name produce internal error. should be removed after bugfix.
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_tables.count(); i++) {
    if (trans_tables.at(i)->get_object_name().empty()) {
      drill_down_idx = -1;
      roll_up_idx = -2;
      break;
    }
  }
  LOG_DEBUG("drill_down_idx roll_up_idx", K(drill_down_idx), K(roll_up_idx));
  return ret;
}

int ObTransformWinMagic::check_mode_and_agg_type(ObSelectStmt *stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (!lib::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
      if (OB_ISNULL(stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregation expr is null", K(ret));
      } else if (stmt->get_aggr_item(i)->get_expr_type() == T_FUN_COUNT) {
        is_valid = false;
        LOG_DEBUG("T_FUN_COUNT_SUM in mysql mode not implemented");
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::get_reverse_map(ObIArray<int64_t> &map, ObIArray<int64_t> &reverse_map, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reverse_map.prepare_allocate(size))) {
    LOG_WARN("pre allocate failed", K(ret));
  } else {
    for (int64_t i = 0; i < reverse_map.count(); i++) {
      reverse_map.at(i) = OB_INVALID_ID;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < map.count(); i++) {
      if (map.at(i) == OB_INVALID_ID) {
        //do nothing
      } else if (OB_UNLIKELY(map.at(i) >= size || map.at(i) < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("out of range", K(ret));
      } else {
        reverse_map.at(map.at(i)) = i;
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::check_stmt_and_view(ObDMLStmt *stmt, 
                                             TableItem *rewrite_table, 
                                             ObStmtMapInfo &map_info,
                                             bool &is_valid,
                                             ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> tables;
  ObSqlBitSet<> tables_id;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  ObSEArray<ObRawExpr *, 4> view_group_exprs;
  ObSEArray<int64_t, 4> expr_map;
  EqualSets dummy_set;
  ObSelectStmt *rewrite_view = NULL;
  int64_t match_count = 0;
  
  if (OB_ISNULL(stmt) || OB_ISNULL(rewrite_table) || OB_ISNULL(rewrite_view = rewrite_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (!is_valid) {
  } else if (OB_FAIL(check_hint_valid(*stmt, *rewrite_table, is_valid))) {
    LOG_WARN("check hint valid", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("hint reject transform");
  }

  for (int64_t k = 0; OB_SUCC(ret) && is_valid && k < map_info.table_map_.count(); k++) {
    TableItem *table = NULL;
    if (map_info.table_map_.at(k) == OB_INVALID_ID) {
      //do nothing 
    } else if (OB_ISNULL(table = stmt->get_table_item(map_info.table_map_.at(k)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(tables.push_back(table))) {
      LOG_WARN("push back table item failed", K(ret));
    } else if (OB_FAIL(tables_id.add_member(map_info.table_map_.at(k) + 1))) {
      LOG_WARN("push back item failed", K(ret));
    } else if (OB_FAIL(check_hint_valid(*stmt, *table, is_valid))) {
      LOG_WARN("check table valid faield", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_condition_exprs().count(); i++) {
    ObRawExpr *expr = stmt->get_condition_exprs().at(i);
    if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
    } else if (expr->get_expr_type() == T_OP_EQ) {
      if (expr->get_param_count() < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param count not expected", K(ret));
      } else if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->get_param_expr(0)->get_relation_ids().is_subset(tables_id) &&
                 OB_FAIL(column_exprs.push_back(expr->get_param_expr(0)))) {
        LOG_WARN("push back item failed", K(ret));
      } else if (expr->get_param_expr(1)->get_relation_ids().is_subset(tables_id) &&
                 OB_FAIL(column_exprs.push_back(expr->get_param_expr(1)))) {
        LOG_WARN("push back item failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) || !is_valid) {
    //do nothing
  } else if (OB_FAIL(ObStmtComparer::compute_semi_infos_map(rewrite_view, stmt,
                                                            map_info, match_count))) {
    LOG_WARN("failed to compute semi info map", K(ret));
  } else if (match_count != rewrite_view->get_semi_info_size() ||
             match_count != stmt->get_semi_info_size()) {
    is_valid = false;
    OPT_TRACE("semi info not match");
  } else if (OB_FAIL(stmt->get_column_exprs(tables, column_exprs))) {
    LOG_WARN("get column exprs failed", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_conditions_map(rewrite_view,
                                            stmt, rewrite_view->get_group_exprs(),
                                           column_exprs, map_info, expr_map, match_count))) {
    LOG_WARN("compute expr map failed", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_parent_stmt_exprs(dummy_set, rewrite_table->table_id_, 
                                                            *stmt, *rewrite_view,
                                                            rewrite_view->get_group_exprs(),
                                                            view_group_exprs))) {
    LOG_WARN("get parent stmt exprs failed", K(ret));
  } else if (OB_FAIL(check_outer_stmt_conditions(stmt, view_group_exprs, column_exprs,
                                                  expr_map, is_valid))) {
    LOG_WARN("check outer stmt condition failed", K(ret)); 
  } else if (!is_valid) {
    OPT_TRACE("group expr not match");
  }

  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(trans_tables.push_back(rewrite_table))) {
      LOG_WARN("push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
         if (OB_FAIL(trans_tables.push_back(tables.at(i)))) {
            LOG_WARN("push back", K(ret));
          }
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::check_view_and_view(ObDMLStmt *main_stmt,
                                             TableItem *drill_down_table, 
                                             TableItem *roll_up_table, 
                                             ObStmtMapInfo &map_info, 
                                             bool &is_valid,
                                             ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *drill_down_view = NULL;
  ObSelectStmt *roll_up_view = NULL;
  ObSEArray<ObRawExpr *, 4> drill_group_exprs;
  ObSEArray<ObRawExpr *, 4> roll_group_exprs;
  ObSEArray<int64_t, 4> reverse_map;
  ObSqlBitSet<> matched_cond;
  int64_t match_count = 0;
  EqualSets dummy_set;
  if (OB_ISNULL(main_stmt) || OB_ISNULL(drill_down_table) || OB_ISNULL(roll_up_table) 
      || OB_ISNULL(drill_down_view = drill_down_table->ref_query_) 
      || OB_ISNULL(roll_up_view = roll_up_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", K(ret));
  // drill map roll => roll map drill
  } else if (!is_valid) {
    //do nothing
  } else if (OB_FAIL(check_hint_valid(*main_stmt, *drill_down_table, is_valid))) {
    LOG_WARN("check table valid faield", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_FAIL(check_hint_valid(*main_stmt, *roll_up_table, is_valid))) {
    LOG_WARN("check table valid faield", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_FAIL(ObStmtComparer::compute_semi_infos_map(drill_down_view, roll_up_view,
                                                            map_info, match_count))) {
    LOG_WARN("failed to compute semi info map", K(ret));
  } else if (match_count != drill_down_view->get_semi_info_size() ||
             match_count != roll_up_view->get_semi_info_size()) {
    is_valid = false;
  } else if (OB_FAIL(ObStmtComparer::compute_conditions_map(drill_down_view,
                                        roll_up_view, drill_down_view->get_group_exprs(),
                                        roll_up_view->get_group_exprs(), map_info, 
                                        map_info.group_map_, match_count))) {
    LOG_WARN("compute expr map failed", K(ret));
  } else if (OB_FAIL(get_reverse_map(map_info.group_map_, reverse_map, roll_up_view->get_group_exprs().count()))) { 
    LOG_WARN("get reverse map failed", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_parent_stmt_exprs(dummy_set, drill_down_table->table_id_, 
                                                     *main_stmt, *drill_down_view,
                                                     drill_down_view->get_group_exprs(),
                                                     drill_group_exprs))) {
    LOG_WARN("get parent stmt exprs failed", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_parent_stmt_exprs(dummy_set, roll_up_table->table_id_, 
                                                     *main_stmt, *roll_up_view,
                                                     roll_up_view->get_group_exprs(),
                                                     roll_group_exprs))) {
    LOG_WARN("get parent stmt exprs failed", K(ret));
  } else if (OB_FAIL(check_outer_stmt_conditions(main_stmt, roll_group_exprs, drill_group_exprs, 
                                                 reverse_map, is_valid))) {
    LOG_WARN("check outer stmt condition failed", K(ret));
  }

  if (OB_SUCC(ret) && map_info.from_map_.count() != roll_up_view->get_from_item_size()) {
    is_valid = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < map_info.from_map_.count(); i++) {
    if (map_info.from_map_.at(i) == OB_INVALID_ID) {
      is_valid = false;
    }
  }

  //check if cnt agg
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < roll_up_view->get_select_item_size(); i++) {
    ObRawExpr *expr = roll_up_view->get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (!expr->has_flag(CNT_AGG) && !expr->has_flag(CNT_COLUMN)) { // T_FUN_AVG is ok
      is_valid = false;
    }
  }

  // simple check: conditions should one to one match. 
  // begin check
  if (OB_FAIL(ret) || !is_valid) {
    //do nothing
  } else if (roll_up_view->get_condition_size() != drill_down_view->get_condition_size()) {
    is_valid = false;
  }

  //the condition should be one to one match.
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < roll_up_view->get_condition_size(); i++) {
    if (map_info.cond_map_.at(i) == OB_INVALID_ID) {
      is_valid = false;
    } else if (OB_FAIL(matched_cond.add_member(map_info.cond_map_.at(i)))) {
      LOG_WARN("bitset add member failed", K(ret));
    }
  }

  if (OB_FAIL(ret) || !is_valid) {
    //do nothing
    //the condition num should be same
  } else if (matched_cond.num_members() != roll_up_view->get_condition_size()) {
    is_valid = false;
    //record agg map in select item map
  }

  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(trans_tables.push_back(drill_down_table)) ||
        OB_FAIL(trans_tables.push_back(roll_up_table))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::remove_dup_condition(ObDMLStmt *stmt) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  }
  ObSEArray<ObRawExpr *, 4> new_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); i++) {
    ObRawExpr *expr = stmt->get_condition_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->get_expr_type() == T_OP_EQ) {
      if (expr->get_param_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal op expr's child less than 2", K(ret));
      } else if (!expr->get_param_expr(0)->same_as(*expr->get_param_expr(1))) {
        if (OB_FAIL(new_conditions.push_back(expr))) {
          LOG_WARN("push back failed", K(ret));
        }
      } else {
        ObOpRawExpr *is_not_null = NULL;
        if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, expr->get_param_expr(0), is_not_null))) {
          LOG_WARN("failed to add is not null expr", K(ret));
        } else if (OB_FAIL(new_conditions.push_back(is_not_null))) {
          LOG_WARN("failed to append is not null exprs to where conditions", K(ret));
        }
      }
    } else if (OB_FAIL(new_conditions.push_back(expr))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(stmt->get_condition_exprs().assign(new_conditions))) {
    LOG_WARN("assign exprs failed", K(ret));
  }
  return ret;
}

// the group by column must be the join condition.
// for group by c1,c2
// the outer stmt must have where v.c1=T.c1 and v.c2=T.c2 and ...
// roll group exprs is subset of drill group exprs
int ObTransformWinMagic::check_outer_stmt_conditions(ObDMLStmt *stmt, 
                                                     ObIArray<ObRawExpr *> &roll_group_exprs,
                                                     ObIArray<ObRawExpr *> &drill_group_exprs, 
                                                     ObIArray<int64_t> &map,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  EqualSets equal_sets;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (roll_group_exprs.count() > drill_group_exprs.count() ||
      map.count() != roll_group_exprs.count()) {
    is_valid = false;
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(ctx_->allocator_, stmt->get_condition_exprs(), equal_sets))) {
    LOG_WARN("compute equal set failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < map.count(); i++) {
      if (map.at(i) == OB_INVALID_ID) {
        is_valid = false;
      } else {
        ObRawExpr *roll_expr = roll_group_exprs.at(i);
        ObRawExpr *drill_expr = drill_group_exprs.at(map.at(i));
        if (roll_expr == NULL || drill_expr == NULL) {
          is_valid = false;
        } else if (!ObOptimizerUtil::is_expr_equivalent(roll_expr, drill_expr, equal_sets)) {
          is_valid = false;
        }
      }
    }
  }
  return ret;
}

// simple check only full group by
int ObTransformWinMagic::check_expr_in_group(ObRawExpr *expr, 
                                             ObIArray<ObRawExpr *> &group_exprs, 
                                             bool &is_valid) {
  int ret = OB_SUCCESS;
  bool found_match = false;

  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && !found_match && i < group_exprs.count(); ++i) {
    if (expr == group_exprs.at(i) || expr->same_as(*group_exprs.at(i))) {
      found_match = true;
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (!found_match) {
    if (expr->is_column_ref_expr()) {
      is_valid = false;
    } else if (IS_AGGR_FUN(expr->get_expr_type()) ||
               expr->get_expr_type() == T_FUN_MIN ||
               expr->get_expr_type() == T_FUN_MAX ||
               expr->get_expr_type() == T_FUN_COUNT ||
               expr->get_expr_type() == T_FUN_SUM ||
              !expr->has_flag(CNT_COLUMN)) {
      // do nothing
    } else {
      // simple check only full group by 
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
        if (OB_ISNULL(expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr child is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(check_expr_in_group(expr->get_param_expr(i), 
                                                          group_exprs, is_valid)))) {
          LOG_WARN("failed to recursive check", K(ret));
        }
      }
    }
  } 
  
  return ret;
}

int ObTransformWinMagic::simple_check_group_by(ObIArray<ObRawExpr *> &exprs, 
                                               ObIArray<ObRawExpr *> &group_exprs, 
                                               bool &is_valid) {
  int ret = OB_SUCCESS;
  
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(SMART_CALL(check_expr_in_group(exprs.at(i), group_exprs, is_valid)))) {
      LOG_WARN("failed to recursive check", K(ret));
    }
  }

  return ret;
}

// subquery select item shold contain aggr function
int ObTransformWinMagic::check_select_expr_validity(ObSelectStmt &subquery, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool found_agg = false;
  ObSEArray<ObRawExpr *, 4> check_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_select_item_size(); ++i) {
    if (OB_ISNULL(subquery.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (subquery.get_select_item(i).expr_->has_flag(CNT_AGG)) {
      found_agg = true;
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (FALSE_IT(is_valid = found_agg)) {
    //never reach
  } else if (!is_valid) {
    //do nothing
  } else if (lib::is_oracle_mode()) {
    //do nothing
  } else if (OB_FAIL(subquery.get_select_exprs(check_exprs))) {
    LOG_WARN("subquery get select exprs failed", K(ret));
  } else if (OB_FAIL(subquery.get_order_exprs(check_exprs))) {
    LOG_WARN("append failed", K(ret));
  } else if (OB_FAIL(append(check_exprs, subquery.get_having_exprs()))) {
    LOG_WARN("append failed", K(ret));
  } else if (OB_FAIL(simple_check_group_by(check_exprs, subquery.get_group_exprs(), is_valid))) {
    LOG_WARN("simple check group by failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_aggr_item_size(); ++i) {
    if (OB_ISNULL(subquery.get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregation expr is null", K(ret));
    } else if (subquery.get_aggr_item(i)->is_param_distinct()) {
      is_valid = false;
    } else if (subquery.get_aggr_item(i)->get_expr_type() != T_FUN_MIN &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_MAX &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_SUM &&
               subquery.get_aggr_item(i)->get_expr_type() != T_FUN_COUNT) {
      is_valid = false;
    }
  }

  return ret;
}

int ObTransformWinMagic::create_aggr_expr(ObItemType type, 
                                          ObAggFunRawExpr *&agg_expr, 
                                          ObRawExpr *child_expr) 
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(expr_factory), K(agg_expr));
  } else if (OB_FAIL(expr_factory->create_raw_expr(type,
                                                   agg_expr))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(agg_expr));
  } else if (OB_FAIL(agg_expr->add_real_param_expr(child_expr))) {
    LOG_WARN("fail to set partition exprs", K(ret));
  } else if (OB_FAIL(agg_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize windown function", K(ret));
  } else if (OB_FAIL(agg_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  }
  return ret;
}

int ObTransformWinMagic::adjust_column_and_table(ObDMLStmt *main_stmt, 
                                                 TableItem *view, 
                                                 ObStmtMapInfo &map_info) 
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObSEArray<TableItem *, 4> main_tables;
  ObSEArray<TableItem *, 4> view_tables;
  ObSEArray<uint64_t, 4> main_table_ids;
  ObSEArray<FromItem, 4> rm_from_items;
  
  ObSEArray<ObRawExpr *, 4> merged_column_expr;
  ObSEArray<ObRawExpr *, 4> pushed_column_exprs;
  
  ObSEArray<ObRawExpr *, 4> merged_pseudo_column_exprs;
  ObSEArray<ObRawExpr *, 4> pushed_pseudo_column_exprs;

  ObArray<SemiInfo *> rm_semi_infos;
  
  if (OB_ISNULL(main_stmt) || OB_ISNULL(view) || 
      OB_ISNULL(view_stmt = view->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret), K(main_stmt), K(view), K(view_stmt));
  }
  //init viriables and push down table
  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.table_map_.count(); ++i) {
    int64_t idx = map_info.table_map_.at(i);
    TableItem *table = NULL;
    if (idx == OB_INVALID_ID) {
      // do nothing
    } else if (OB_UNLIKELY(idx < 0 || idx >= main_stmt->get_table_size()) ||
               OB_ISNULL(table = main_stmt->get_table_item(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is invalid", K(ret), K(idx), K(table));
    } else if (OB_FAIL(main_tables.push_back(table))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(view_tables.push_back(view_stmt->get_table_item(i)))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(main_table_ids.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    } else if (OB_FAIL(main_stmt->remove_part_expr_items(table->table_id_))) {
      LOG_WARN("failed to remove part exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_table_item(view_stmt, table))) {
      LOG_WARN("failed to add table item", K(ret));
    }
  }
  
  // push down column
  ObArray<ColumnItem> column_items;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(main_stmt->get_column_items(main_table_ids, column_items))) {
    LOG_WARN("get column items failed", K(ret));
  } else if (OB_FAIL(view_stmt->add_column_item(column_items))) {
    LOG_WARN("add column item failed", K(ret));
  }

  // push down pseudo column
  ObArray<ObRawExpr *> pseudo_columns;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(main_stmt->get_table_pseudo_column_like_exprs(main_table_ids, pseudo_columns))) {
    LOG_WARN("get table pseudo column like exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_pseudo_column_like_exprs(), pseudo_columns))) {
    LOG_WARN("append failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.semi_info_map_.count(); ++i) {
    int64_t idx = map_info.semi_info_map_.at(i);
    SemiInfo *semi = NULL;
    if (idx == OB_INVALID_ID) {
      // do nothing
    } else if (OB_UNLIKELY(idx < 0 || idx >= main_stmt->get_semi_info_size()) ||
               OB_ISNULL(semi = main_stmt->get_semi_infos().at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret), K(idx), K(semi));
    } else if (OB_FAIL(rm_semi_infos.push_back(semi))) {
      LOG_WARN("failed to push back semi info", K(ret));
    }
  }
  
  if (OB_SUCC(ret) && ObOptimizerUtil::remove_item(main_stmt->get_semi_infos(),
                                                   rm_semi_infos)) {
    LOG_WARN("failed to remove semi infos", K(ret));
  }
  
  //merge table
  for (int64_t i = 0; OB_SUCC(ret) && i < main_tables.count(); ++i) {
    TableItem *main_table = main_tables.at(i);
    TableItem *view_table = view_tables.at(i);
    if (OB_ISNULL(main_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_table_items(view_stmt, 
                                                           view_table, 
                                                           main_table, 
                                                           NULL, 
                                                           &pushed_column_exprs, 
                                                           &merged_column_expr,
                                                           &pushed_pseudo_column_exprs,
                                                           &merged_pseudo_column_exprs))) {
      LOG_WARN("failed to merge table items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(main_stmt, view_table,
                                                                     main_table))) {
      LOG_WARN("failed to replace table info in semi infos", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.from_map_.count(); i++) {
    int64_t from_idx = map_info.from_map_.at(i);
    if (from_idx != OB_INVALID_ID && 
        OB_FAIL(rm_from_items.push_back(main_stmt->get_from_item(from_idx)))) {
      LOG_WARN("failed to push back from item", K(ret));
    }
  }
  
  //remove joined table
  for (int64_t i = 0; OB_SUCC(ret) && i < rm_from_items.count(); i++) {
    if (OB_FAIL(main_stmt->remove_from_item(rm_from_items.at(i).table_id_))) {
      LOG_WARN("failed to remove from item", K(ret));
    } else if (!rm_from_items.at(i).is_joined_) {
      // do nothing
    } else if (OB_FAIL(main_stmt->remove_joined_table_item(
                         main_stmt->get_joined_table(rm_from_items.at(i).table_id_)))) {
      LOG_WARN("remove joined table item failed", K(ret));
    }
  }
  
  if (OB_SUCC(ret) && (main_stmt->is_update_stmt() || main_stmt->is_delete_stmt())) {
    ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(main_stmt);
    ObArray<ObDmlTableInfo *> table_infos;
    if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get dml table info", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      int64_t idx = -1;
      if (OB_ISNULL(table_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table info is null", K(ret));
      } else if (!ObOptimizerUtil::find_item(main_table_ids,
                                             table_infos.at(i)->table_id_,
                                             &idx)) {
        // do nothing
      } else {
        table_infos.at(i)->table_id_ = view->table_id_;
        table_infos.at(i)->loc_table_id_ = view_tables.at(idx)->table_id_;
      }
    }
  }


  ObSEArray<ObRawExpr *, 4> new_col_for_pseudo_cols;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(main_stmt->get_pseudo_column_like_exprs(),
                                                  pushed_pseudo_column_exprs))) {
    LOG_WARN("remove item failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                               *view,
                                                               main_stmt,
                                                               merged_pseudo_column_exprs,
                                                               new_col_for_pseudo_cols))) {
    LOG_WARN("create columns for view failed", K(ret));
  } else if (OB_FAIL(main_stmt->replace_relation_exprs(pushed_pseudo_column_exprs, new_col_for_pseudo_cols))) {
    LOG_WARN("replace inner stmt expr failed", K(ret));
  }

  ObSEArray<ObRawExpr *, 4> new_col_in_main;
  //create select item for view
  if (OB_FAIL(ret)) {
    //do nothing.
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, 
                                                               *view, 
                                                               main_stmt, 
                                                               merged_column_expr, 
                                                               new_col_in_main))) {
    LOG_WARN("create columns for view failed", K(ret));
  } else if (OB_FAIL(main_stmt->remove_table_item(main_tables))) {
    LOG_WARN("failed to remove table item", K(ret));
  } else if (OB_FAIL(view_stmt->remove_table_item(main_tables))) {
    LOG_WARN("failed to remove table item", K(ret));
  } else if (OB_FAIL(main_stmt->remove_column_item(main_table_ids))) {
    LOG_WARN("remove column item failed", K(ret));
  } else if (OB_FAIL(main_stmt->replace_relation_exprs(pushed_column_exprs, new_col_in_main))) {
    LOG_WARN("replace inner stmt expr failed", K(ret));
  }
  
  //add is not null expr
  for (int64_t i = 0; OB_SUCC(ret) && i < view_stmt->get_group_expr_size(); i++) {
    ObRawExpr *expr = view_stmt->get_group_exprs().at(i);
    ObSEArray<ObRawExpr *, 1> is_not_null_exprs;
    ObOpRawExpr *is_not_null = NULL;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gourp by expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, view_stmt, expr, is_not_null))) {
      LOG_WARN("failed to add is not null expr", K(ret));
    } else if (OB_FAIL(is_not_null_exprs.push_back(is_not_null))) {
      LOG_WARN("failed to push is not null expr into array", K(ret));
    } else if (OB_FAIL(append(view_stmt->get_condition_exprs(), is_not_null_exprs))) {
      LOG_WARN("failed to append is not null exprs to where conditions", K(ret));
    }
  }
  if (OB_SUCC(ret) && (main_stmt->is_update_stmt() || main_stmt->is_delete_stmt())) {
    ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(main_stmt);
    if (OB_FAIL(ObTransformUtils::adjust_updatable_view(
                  *ctx_->expr_factory_,
                  del_upd_stmt,
                  *view))) {
      LOG_WARN("failed to adjust updatable view", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::adjust_agg_to_win(ObSelectStmt *view_stmt) 
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> aggr_exprs;
  ObSEArray<ObRawExpr *, 4> win_exprs;
  ObSEArray<ObRawExpr *, 4> partition_exprs;
  if (OB_FAIL(partition_exprs.assign(view_stmt->get_group_exprs()))) {
    LOG_WARN("failed to assign group exprs", K(ret));
  } else if (OB_FAIL(append(aggr_exprs, view_stmt->get_aggr_items()))) {
    LOG_WARN("failed to append aggr exprs", K(ret));
  } else {
    view_stmt->get_group_exprs().reset();
    view_stmt->clear_aggr_item();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); ++i) {
    ObWinFunRawExpr *win_expr = NULL;
    if (OB_FAIL(create_window_function(static_cast<ObAggFunRawExpr *>(aggr_exprs.at(i)),
                                       partition_exprs,
                                       win_expr))) {
      LOG_WARN("failed to create window function", K(ret));
    } else if (OB_FAIL(view_stmt->add_window_func_expr(win_expr))) {
      LOG_WARN("failed to add window func expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(aggr_exprs, view_stmt->get_aggr_items()))) {
      LOG_WARN("failed to convert aggregation exprs", K(ret));
    } else if (OB_FAIL(append(win_exprs, view_stmt->get_window_func_exprs()))) {
      LOG_WARN("failed to convert window function exprs", K(ret));
    } else if (OB_FAIL(view_stmt->replace_relation_exprs(aggr_exprs, win_exprs))) {
      LOG_WARN("failed to replace relation expr", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::adjust_view_for_trans(ObDMLStmt *main_stmt, 
                                               TableItem *&drill_down_table,
                                               TableItem *roll_up_table,
                                               TableItem *&transed_view_table,
                                               ObIArray<ObRawExpr *> &new_transed_output,
                                               ObStmtCompareContext &context,
                                               ObStmtMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *drill_down_stmt = NULL;
  ObSelectStmt *roll_up_stmt = NULL;
  ObSEArray<ObRawExpr *, 4> new_drill_down_view_column_exprs;
  ObSEArray<ObRawExpr *, 4> pushed_column_exprs;
  ObSEArray<ObRawExpr *, 4> new_select_exprs_for_drill;
  ObSEArray<ObRawExpr *, 4> new_drill_down_view_output_columns;
  ObSEArray<ObRawExpr *, 4> new_select_exprs_for_trans;
  ObSEArray<ObRawExpr *, 4> old_roll_up_view_output_columns;
  ObSEArray<ObRawExpr *, 4> new_transed_view_output_columns;

  if (OB_ISNULL(main_stmt) || OB_ISNULL(drill_down_table) || OB_ISNULL(roll_up_table) ||
      OB_ISNULL(drill_down_stmt = drill_down_table->ref_query_) ||
      OB_ISNULL(roll_up_stmt = roll_up_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               main_stmt,
                                                               transed_view_table,
                                                               drill_down_table))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          main_stmt,
                                                          transed_view_table,
                                                          drill_down_table))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_ISNULL(transed_view_table) ||
             OB_ISNULL(transed_view_table->ref_query_) ||
             OB_UNLIKELY(transed_view_table->ref_query_->get_table_size() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transed view table is null", K(ret), K(transed_view_table));
  } else {
    drill_down_table = transed_view_table->ref_query_->get_table_item(0);
  }

  // push down table
  for (int64_t i = 0; OB_SUCC(ret) && i < roll_up_stmt->get_table_size(); i++) {
    TableItem *table = NULL;
    ObSEArray<ObDMLStmt::PartExprItem, 4> part_exprs;
    if (OB_ISNULL(table = roll_up_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_table_item(drill_down_stmt, table))) {
      LOG_WARN("add table item failed", K(ret));
    } else if (OB_FAIL(main_stmt->get_part_expr_items(table->table_id_, part_exprs))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (!part_exprs.empty() && OB_FAIL(drill_down_stmt->set_part_expr_items(part_exprs))) {
      LOG_WARN("failed to set part expr item", K(ret));
    } else if (OB_FAIL(main_stmt->remove_part_expr_items(table->table_id_))) {
      LOG_WARN("failed to remove part epxr", K(ret));
    }
  }
  
  // push down column
  for (int64_t i = 0; OB_SUCC(ret) && i < roll_up_stmt->get_column_size(); i++) {
    if (OB_ISNULL(roll_up_stmt->get_column_item(i)) ||
        OB_ISNULL(roll_up_stmt->get_column_item(i)->get_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column or column expr is null", K(ret));
    } else if (OB_FAIL(drill_down_stmt->add_column_item(roll_up_stmt->get_column_items().at(i)))) {
      LOG_WARN("add column item failed", K(ret));
    }
  }
  
  ObSEArray<ObRawExpr *, 4> agg_in_roll;
  ObSEArray<ObRawExpr *, 4> agg_in_drill;
  // push aggr item
  for (int64_t i = 0; OB_SUCC(ret) && i < roll_up_stmt->get_aggr_item_size(); i++) {
    if (OB_ISNULL(roll_up_stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("agg expr is null", K(ret));
    } else {
      bool found = false;
      for(int64_t j = 0; OB_SUCC(ret) && !found && j < drill_down_stmt->get_aggr_item_size(); j++) {
        if (drill_down_stmt->get_aggr_item(j)->same_as(*roll_up_stmt->get_aggr_item(i), &context)) {
          found = true;
          if (OB_FAIL(agg_in_roll.push_back(roll_up_stmt->get_aggr_item(i))) ||
              OB_FAIL(agg_in_drill.push_back(drill_down_stmt->get_aggr_item(j)))) {
            LOG_WARN("push back expr into array failed", K(ret));
          } else if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
            LOG_WARN("failed to append equal param map", K(ret));
          } else {
            context.equal_param_info_.reset();
          }
        } 
      }
      if (OB_FAIL(ret) || found) {
        //do nothing
      } else if (OB_FAIL(drill_down_stmt->add_agg_item(*roll_up_stmt->get_aggr_item(i)))) {
        LOG_WARN("add agg item failed", K(ret));
      }
    }
  }
  //push 
  //merge table
  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.table_map_.count(); i++) {
    TableItem *table_in_roll_up = NULL;
    TableItem *table_in_drill_down = NULL;
    if (map_info.table_map_.at(i) == OB_INVALID_ID) {
      //do nothing
    } else if (OB_ISNULL(table_in_roll_up = roll_up_stmt->get_table_item(map_info.table_map_.at(i))) ||
               OB_ISNULL(table_in_drill_down = drill_down_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_table_items(drill_down_stmt, 
                                                           table_in_drill_down,
                                                           table_in_roll_up,
                                                           NULL, 
                                                           &pushed_column_exprs, 
                                                           &new_drill_down_view_column_exprs))) {
      LOG_WARN("failed to merge table items", K(ret));
    } else if (OB_FAIL(drill_down_stmt->remove_table_item(table_in_roll_up))) {
      LOG_WARN("remove table item failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(main_stmt, transed_view_table, 
                                                                     table_in_drill_down))) {
      LOG_WARN("failed to replace table info in semi infos", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(main_stmt, transed_view_table,
                                                                     table_in_roll_up))) {
      LOG_WARN("failed to replace table info in semi infos", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
    //create new select for drill down table begin
  } else if (OB_FAIL(append(new_select_exprs_for_drill, roll_up_stmt->get_group_exprs()) ||
             OB_FAIL(append(new_select_exprs_for_drill, roll_up_stmt->get_aggr_items())))) {
    LOG_WARN("append exprs after array failed", K(ret));
    //make the exprs in roll up stmt become exprs in drill down stmt
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(pushed_column_exprs, 
                                                     new_drill_down_view_column_exprs, 
                                                     new_select_exprs_for_drill))) {
    LOG_WARN("replace exprs failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(agg_in_roll, agg_in_drill, 
                                                     new_select_exprs_for_drill))) {             
    LOG_WARN("replace exprs failed", K(ret));
    //create select item for drill_down_stmt
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *drill_down_table, 
                                                            transed_view_table->ref_query_, 
                                                            new_select_exprs_for_drill, 
                                                            new_drill_down_view_output_columns))) {
    LOG_WARN("create column for view failed", K(ret));
    //create new select for drill down table end
    //create new select for transed table begin
  } else if (OB_FAIL(main_stmt->get_view_output(*roll_up_table, new_select_exprs_for_trans, 
                                                old_roll_up_view_output_columns))) {
    LOG_WARN("get view output failed", K(ret));
    //make the exprs in roll up stmt become exprs in drill down stmt
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(pushed_column_exprs, 
                                                     new_drill_down_view_column_exprs, 
                                                     new_select_exprs_for_trans))) {
    LOG_WARN("replace exprs failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(agg_in_roll, agg_in_drill, 
                                                     new_select_exprs_for_trans))) {
    LOG_WARN("replace exprs failed", K(ret));
    //map the select exprs in drill to column exprs in transed
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(new_select_exprs_for_drill, 
                                                     new_drill_down_view_output_columns, 
                                                     new_select_exprs_for_trans))) {
    LOG_WARN("replace exprs failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *transed_view_table, 
                                                               main_stmt, 
                                                               new_select_exprs_for_trans, 
                                                               new_transed_view_output_columns, 
                                                               true, true))) {
    LOG_WARN("create columns for view failed", K(ret));
    //create new select for transed table end
  } else if (OB_FAIL(main_stmt->remove_column_item(roll_up_table->table_id_))) {
    LOG_WARN("remove column item failed", K(ret));
  } else if (OB_FAIL(main_stmt->replace_relation_exprs(old_roll_up_view_output_columns,
                                                       new_transed_view_output_columns))) {
    LOG_WARN("replace inner stmt expr failed", K(ret));
  } else if (OB_FAIL(new_transed_output.assign(new_transed_view_output_columns))) {
    LOG_WARN("assign array failed", K(ret));
  }
  return ret;
}

int ObTransformWinMagic::change_agg_to_win_func(ObDMLStmt *main_stmt, 
                                                ObSelectStmt *roll_up_stmt, 
                                                ObSelectStmt *drill_down_stmt,
                                                TableItem *transed_view_table, 
                                                ObStmtMapInfo &map_info, 
                                                ObStmtCompareContext &context,
                                                ObIArray<ObRawExpr *> &old_col,
                                                ObIArray<ObRawExpr *> &new_win)
{
  int ret = OB_SUCCESS;
  UNUSED(main_stmt);
  ObSEArray<ObRawExpr *, 4> partition_exprs;
  ObRawExpr *col_expr = NULL;
  ObItemType type;
  ObSelectStmt *transed_stmt = NULL;
  ObAggFunRawExpr *new_agg_expr = NULL;
  ObWinFunRawExpr *win_expr = NULL;
  
  ObSEArray<ObRawExpr *, 4> drill_select_exprs;

  if (OB_ISNULL(main_stmt) || OB_ISNULL(roll_up_stmt) || OB_ISNULL(drill_down_stmt) 
      || OB_ISNULL(transed_view_table) || OB_ISNULL(transed_stmt = transed_view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (OB_FAIL(drill_down_stmt->get_select_exprs(drill_select_exprs))) {
    LOG_WARN("get select exprs failed", K(ret));
  }

  //get partition exprs
  for(int64_t i = 0; OB_SUCC(ret) && i < roll_up_stmt->get_group_exprs().count(); i++) {
    bool found = false;
    ObRawExpr *group_expr_in_roll = NULL;
    ObRawExpr *group_expr_in_drill = NULL;
    for(int64_t j = 0; OB_SUCC(ret) && !found && j < drill_down_stmt->get_group_exprs().count(); j++) {
      if (OB_ISNULL(group_expr_in_roll = roll_up_stmt->get_group_exprs().at(i)) || 
          OB_ISNULL(group_expr_in_drill = drill_down_stmt->get_group_exprs().at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group expr is null", K(ret));
      } else if (group_expr_in_drill->same_as(*group_expr_in_roll, &context)) {
        found = true;
        int64_t idx = -1;
        if (OB_FAIL(ObTransformUtils::get_expr_idx(drill_select_exprs, group_expr_in_drill, idx))) {
          LOG_WARN("get expr idx faield", K(ret));
        } else if (idx == -1) {
          // do nothing
        } else if (OB_FAIL(partition_exprs.push_back(transed_stmt->get_column_expr_by_id(
                                    transed_stmt->get_table_item(0)->table_id_, idx + OB_APP_MIN_COLUMN_ID)))) {
          LOG_WARN("push back expr failed", K(ret));
        } 
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); i++) {
    ObRawExpr *expr = partition_exprs.at(i);
    ObSEArray<ObRawExpr *, 1> is_not_null_exprs;
    ObOpRawExpr *is_not_null = NULL;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gourp by expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, transed_stmt, expr, is_not_null))) {
      LOG_WARN("failed to add is not null expr", K(ret));
    } else if (OB_FAIL(is_not_null_exprs.push_back(is_not_null))) {
      LOG_WARN("failed to push is not null expr into array", K(ret));
    } else if (OB_FAIL(append(transed_stmt->get_condition_exprs(), is_not_null_exprs))) {
      LOG_WARN("failed to append is not null exprs to where conditions", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < roll_up_stmt->get_aggr_item_size(); i++) {
    ObRawExpr *agg_expr = roll_up_stmt->get_aggr_item(i);
    int64_t sel_idx = -1;

    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    }

    for (int64_t j = 0; OB_SUCC(ret) && sel_idx == -1 && j < drill_down_stmt->get_select_item_size(); j++) {
      if (drill_down_stmt->get_select_item(j).expr_->same_as(*agg_expr, &context)) {
        sel_idx = j;
        if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
          LOG_WARN("failed to append equal param map", K(ret));
        } else {
          context.equal_param_info_.reset();
        }
      }
    }
    ColumnItem *col_in_transed = NULL;
    if (sel_idx == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item idx is out of range", K(ret));
    } else if (OB_ISNULL(col_in_transed = transed_stmt->get_column_item_by_id(
                                                        transed_stmt->get_table_item(0)->table_id_, 
                                                        sel_idx + OB_APP_MIN_COLUMN_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find the corresponding select item", K(ret), K(sel_idx));
    } else if (OB_ISNULL(col_expr = col_in_transed->get_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col expr is null", K(ret));
    } else if (FALSE_IT(type = (lib::is_oracle_mode() ? 
               (agg_expr->get_expr_type() == T_FUN_COUNT ? 
                                             T_FUN_SUM : agg_expr->get_expr_type()) : 
               (agg_expr->get_expr_type() == T_FUN_COUNT ? 
                                             T_FUN_COUNT_SUM : agg_expr->get_expr_type())))) {
      //never reach
    } else if (OB_FAIL(create_aggr_expr(type, new_agg_expr, col_expr))) {
      LOG_WARN("creat aggr expr failed", K(ret));
    } else if (OB_ISNULL(new_agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new agg expr is null", K(ret));
    } else if (OB_FAIL(create_window_function(new_agg_expr, partition_exprs, win_expr))) {
      LOG_WARN("create window function failed", K(ret));
    } else if (OB_ISNULL(win_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("win function expr is null", K(ret));
    } else if (OB_FAIL(transed_stmt->add_window_func_expr(win_expr))) {
      LOG_WARN("add windon func expr failed", K(ret));
    } else if (OB_FAIL(old_col.push_back(col_expr))) {
      LOG_WARN("failed to push expr into array", K(ret));
    } else if (OB_FAIL(new_win.push_back(win_expr))) {
      LOG_WARN("failed to push expr into array", K(ret));
    } else {
    }
  }

  return ret;
}

int ObTransformWinMagic::adjust_win_after_group_by(ObDMLStmt *main_stmt, 
                                             TableItem *drill_down_table, 
                                             TableItem *roll_up_table,
                                             TableItem *transed_view_table,
                                             ObIArray<ObRawExpr *> &new_transed_view_output_columns,
                                             ObStmtCompareContext &context,
                                             ObStmtMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *drill_down_stmt = NULL;
  ObSelectStmt *roll_up_stmt = NULL;
  ObSelectStmt *transed_stmt = NULL;
  ObSEArray<ObRawExpr *, 4> old_agg;
  ObSEArray<ObRawExpr *, 4> new_win;
  if (OB_ISNULL(main_stmt) || OB_ISNULL(drill_down_table) || OB_ISNULL(transed_view_table) 
      || OB_ISNULL(roll_up_table) || OB_ISNULL(drill_down_stmt = drill_down_table->ref_query_) 
      || OB_ISNULL(roll_up_stmt = roll_up_table->ref_query_) 
      || OB_ISNULL(transed_stmt = transed_view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (OB_FAIL(change_agg_to_win_func(main_stmt, roll_up_stmt, drill_down_stmt, transed_view_table, 
                                     map_info, context, 
                                     old_agg, new_win))) {
    LOG_WARN("change agg to window function failed", K(ret));
  } 

  for (int64_t i = 0 ;OB_SUCC(ret) && i < new_transed_view_output_columns.count(); i++) {
    ObRawExpr *&expr = transed_stmt->get_select_item(
      static_cast<ObColumnRefRawExpr *>(new_transed_view_output_columns.at(i))->get_column_id() -
                                                                        OB_APP_MIN_COLUMN_ID).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(old_agg, new_win, expr))) {  
      LOG_WARN("replace exprs failed", K(ret));
    }
    ObSEArray<ObRawExpr *, 4> col_exprs;
    ObSEArray<ObRawExpr *, 4> sel_exprs;
    transed_stmt->get_column_exprs(drill_down_table->table_id_, col_exprs);
    transed_stmt->get_select_exprs(sel_exprs);
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(main_stmt->remove_column_item(roll_up_table->table_id_))) {
    LOG_WARN("remove column item failed", K(ret));
  } else if (OB_FAIL(main_stmt->remove_table_item(roll_up_table))) {
    LOG_WARN("remove table item failed", K(ret));
  } else if (OB_FAIL(main_stmt->remove_from_item(roll_up_table->table_id_))) {
    LOG_WARN("remove from item failed", K(ret));
  }
  return ret;
}

int ObTransformWinMagic::check_join_push_down(ObDMLStmt *main_stmt, 
                                              TableItem *view_table, 
                                              TableItem *push_down_table, 
                                              ObIArray<ObRawExpr *> &cond_to_push_down, 
                                              bool &is_valid) 
{
  int ret = OB_SUCCESS;
  int64_t view_table_idx = -1;
  int64_t push_down_table_idx = -1;
  EqualSets dummy_set;
  if (OB_ISNULL(main_stmt) || OB_ISNULL(view_table) || OB_ISNULL(view_table->ref_query_) ||
      OB_ISNULL(push_down_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view table ref query is null", K(ret));
  } else if (view_table->ref_query_->has_group_by() ||
             view_table->ref_query_->has_limit() ||
             view_table->ref_query_->is_hierarchical_query() ||
             view_table->ref_query_->has_rollup() ||
             view_table->ref_query_->is_set_stmt() ||
             view_table->ref_query_->has_sequence() ||
             !view_table->ref_query_->has_window_function()) {
    is_valid = false;
  } else {
    // to calculate the parition exprs intersection among win functions.
    ObSEArray<ObSEArray<ObRawExpr *, 4>, 2> partition_expr_sets; 
    ObSEArray<ObRawExpr *, 4> intersection;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < view_table->ref_query_->get_window_func_exprs().count(); i++) {
      ObSEArray<ObRawExpr *, 4> parition_exprs;
      if (OB_FAIL(append(parition_exprs, view_table->ref_query_->get_window_func_expr(i)->get_partition_exprs()))) {
        LOG_WARN("append partition exprs failed", K(ret));
      } else if (OB_FAIL(partition_expr_sets.push_back(parition_exprs))) {
        LOG_WARN("failed to push back exprs into array", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(ObOptimizerUtil::intersect(partition_expr_sets, intersection))) {
      LOG_WARN("failed to calculate the intersection of sets", K(ret));
    } else if (intersection.count() == 0) {
      is_valid = false;
    }
    
    ObSEArray<ObRawExpr *, 4> intersection_in_parent;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < intersection.count(); i++) {
      ObRawExpr *parent_stmt_expr = NULL;
      if (OB_FAIL(ObOptimizerUtil::get_parent_stmt_expr(dummy_set, view_table->table_id_, *main_stmt,
                                                        *view_table->ref_query_, intersection.at(i),
                                                        parent_stmt_expr))) {
        LOG_WARN("get parent stmt expr failed", K(ret));
      } else if (parent_stmt_expr == NULL) {
        //do nothing 
      } else if (OB_FAIL(intersection_in_parent.push_back(parent_stmt_expr))) {
        LOG_WARN("failed to push back intersection in parent", K(ret));
      }
    }
    //select * from T2, (select ..., agg(..) over(parition by c1,c2) from T1) v where T2.c1 = v.c1
    //to push down T2
    //1. the join condition should be equal condition
    //2. the join column c from v should be in the partition exprs intersection. It can be function f(c).
    //3. the set of join column from T2 should be unique. It can't be f(c), like abs(c) may break the unique property.
    ObSEArray<ObRawExpr *, 4> exprs_unqiue_check;
    bool is_unique = false;
    if (OB_FAIL(ret) || !is_valid) {
      //do nothing
    } else if (intersection_in_parent.count() <= 0) {
      is_valid = false;
    } else if (OB_FAIL(main_stmt->get_table_item_idx(view_table->table_id_, view_table_idx))) {
      LOG_WARN("get table item idx failed", K(ret));
    } else if (OB_FAIL(main_stmt->get_table_item_idx(push_down_table->table_id_, push_down_table_idx))) {
      LOG_WARN("get table item idx failed", K(ret));
    } else {
      view_table_idx++;
      push_down_table_idx++;
    }

    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < main_stmt->get_condition_size(); i++) {
      ObRawExpr *expr = main_stmt->get_condition_expr(i);
      
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        //if has condition like T1.c1 op v.c1 op T3.c2   T2 could not be push down.
      } else if (expr->get_relation_ids().num_members() > 2 &&
                expr->get_relation_ids().has_member(view_table_idx) &&
                expr->get_relation_ids().has_member(push_down_table_idx)) {
        is_valid = false;
        //for condition like T2.c1 = const, it could be push down.
      } else if (expr->get_relation_ids().num_members() == 1 &&
                expr->get_relation_ids().has_member(push_down_table_idx)) {
        if (OB_FAIL(cond_to_push_down.push_back(expr))) {
          LOG_WARN("failed to push expr into array",K(ret));
        }
        // for condition like v.c1 op T2.c1, 
        //we now only support f(v.c1)=T2.c1 for simple, v.c1+T2.c1+1=0 won't be recognized.
      } else if (expr->get_relation_ids().num_members() == 2 && 
                expr->get_relation_ids().has_member(view_table_idx) &&
                expr->get_relation_ids().has_member(push_down_table_idx)) {
        ObSEArray<ObRawExpr *, 4> column_exprs;
        if (!expr->is_op_expr() || expr->get_param_count() < 2) {
          //do nothing
        } else if (OB_ISNULL(expr->get_param_expr(0)) ||
                  OB_ISNULL(expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr's param is null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
          LOG_WARN("extract column exprs failed", K(ret));
        } else {
          bool found = true;
          for (int64_t j = 0; OB_SUCC(ret) && found && j < column_exprs.count(); j++) {
            int64_t idx =-1;
            if (static_cast<ObColumnRefRawExpr *>(column_exprs.at(j))->get_table_id() != view_table->table_id_) {
              //do nothing
            } else if (OB_FAIL(ObTransformUtils::get_expr_idx(intersection_in_parent, 
                                                              column_exprs.at(j), idx))) { 
              LOG_WARN("get expr idx failed", K(ret));
            } else if (idx == -1) {
              found = false;
            }
          }

          if (OB_FAIL(ret) || !found) {
            //do nothing
          } else if (OB_FAIL(cond_to_push_down.push_back(expr))) {
            LOG_WARN("failed to push expr into array",K(ret));
          } else if (T_OP_EQ == expr->get_expr_type() && 
                    expr->get_param_expr(0)->get_relation_ids().num_members() == 1 &&
                    expr->get_param_expr(1)->get_relation_ids().num_members() == 1) {
            if (expr->get_param_expr(0)->get_relation_ids().has_member(push_down_table_idx)) {
              // expr0 is from view , expr1 is from push down table.
              std::swap(expr->get_param_expr(0), expr->get_param_expr(1)); 
            }

            if (OB_FAIL(exprs_unqiue_check.push_back(expr->get_param_expr(1)))) {
              LOG_WARN("failed to push back expr into array", K(ret));
            }
          } 
        }
      }
    }

    if (OB_FAIL(ret) || !is_valid) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*main_stmt, push_down_table, exprs_unqiue_check,
                                    ctx_->session_info_, ctx_->schema_checker_, is_valid))) {
      LOG_WARN("check exprs unique failed", K(ret));
    }
  }
  return ret;
}

//cost based trans, treat as rule trans here
int ObTransformWinMagic::try_to_push_down_join(ObDMLStmt *&main_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(main_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < main_stmt->get_from_item_size(); i++) {
    FromItem view_from = main_stmt->get_from_item(i);
    TableItem *view_table = NULL;
    if (view_from.is_joined_) {
      //do nothing
    } else if (OB_ISNULL(view_table = main_stmt->get_table_item_by_id(view_from.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get table item by id failed", K(ret), K(view_from), K(i));
    } else if (!view_table->is_generated_table()) {
      //do nothing
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < main_stmt->get_from_item_size(); j++) {
        bool is_valid = true;
        TableItem *push_down_table = NULL;
        FromItem push_down_from = main_stmt->get_from_item(j);
        ObSEArray<ObRawExpr *, 8> cond_to_push_down;
        if (i == j) {
          //do nothing
        } else if (push_down_from.is_joined_) {
          //do nothing
        } else if (OB_ISNULL(push_down_table = main_stmt->get_table_item_by_id(push_down_from.table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get table item by id failed", K(ret), K(i), K(j));
          //do nothing
        } else if (OB_FAIL(check_join_push_down(main_stmt, view_table, 
                                                push_down_table, cond_to_push_down, is_valid))) {
          LOG_WARN("check join push down failed", K(ret));
        } else if (!is_valid) {
          // do nothing
        } else if (OB_FAIL(push_down_join(main_stmt, view_table, push_down_table, cond_to_push_down))) {
          LOG_WARN("push down join failed", K(ret));
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::push_down_join(ObDMLStmt *main_stmt, 
                                        TableItem *view_table,
                                        TableItem *push_down_table, 
                                        ObIArray<ObRawExpr *> &cond_to_push_down)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObSEArray<ObRawExpr *, 4> renamed_cond;
  ObSEArray<ObDMLStmt::PartExprItem,4> part_exprs;
  if (OB_ISNULL(main_stmt) || OB_ISNULL(view_table) || OB_ISNULL(push_down_table) || 
      OB_ISNULL(view_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::rename_subquery_pushdown_filter(*main_stmt, *view_stmt, view_table->table_id_,
                                                               ctx_->session_info_, *ctx_->expr_factory_, 
                                                               cond_to_push_down, renamed_cond))) {
    LOG_WARN("rename subuqery push down filter failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_table_item(view_stmt, push_down_table))) {
    LOG_WARN("add table item failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_condition_exprs(), renamed_cond))) {
    LOG_WARN("append cond to push down after condition exprs failed", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(main_stmt->get_condition_exprs(), cond_to_push_down))) {
    LOG_WARN("remove item failed", K(ret));
  } else if (OB_FAIL(main_stmt->remove_table_item(push_down_table))) {
    LOG_WARN("remove tbale item failed", K(ret));
  } else if (OB_FAIL(main_stmt->remove_from_item(push_down_table->table_id_))) {
    LOG_WARN("remove from item faield", K(ret));
  } else if (OB_FAIL(view_stmt->add_from_item(push_down_table->table_id_))) {
    LOG_WARN("add from item failed", K(ret));
  } else if (OB_FAIL(main_stmt->get_part_expr_items(push_down_table->table_id_, part_exprs))) {
    LOG_WARN("failed to get part expr items", K(ret));
  } else if (!part_exprs.empty() && OB_FAIL(view_stmt->set_part_expr_items(part_exprs))) {
    LOG_WARN("failed to set part expr item", K(ret));
  } else if (OB_FAIL(main_stmt->remove_part_expr_items(push_down_table->table_id_))) {
      LOG_WARN("failed to remove part epxr", K(ret));
  }

  ObSEArray<ObRawExpr *, 4> old_column;
  ObSEArray<ObRawExpr *, 4> new_column;
  for (int64_t i = 0; OB_SUCC(ret) && i < main_stmt->get_column_size(); i++) {
    if (OB_ISNULL(main_stmt->get_column_item(i)) || OB_ISNULL(main_stmt->get_column_item(i)->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret));
    } else if (main_stmt->get_column_item(i)->expr_->get_table_id() == push_down_table->table_id_) {
      if (OB_FAIL(view_stmt->add_column_item(*main_stmt->get_column_item(i)))) {
        LOG_WARN("add column item failed", K(ret));
      } else if (OB_FAIL(old_column.push_back(main_stmt->get_column_item(i)->expr_))) {
        LOG_WARN("push back expr into array failed", K(ret));
      }
    }
  }
  
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(main_stmt->remove_column_item(push_down_table->table_id_))) {
    LOG_WARN("remove column item failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(
                  ctx_, *view_table, main_stmt, old_column, new_column))) {
    LOG_WARN("create columns for view failed", K(ret));
  } else if (OB_FAIL(main_stmt->replace_relation_exprs(old_column, new_column))) {
    LOG_WARN("replace inner stmt expr failed", K(ret));
  } else if (OB_FAIL(main_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(main_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item relation id", K(ret));
  } else if (OB_FAIL(view_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(view_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item relation id", K(ret));
  } else if (OB_FAIL(main_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt info", K(ret));
  } else {
    LOG_DEBUG("push down join", K(*main_stmt));
  }
  return ret;
}

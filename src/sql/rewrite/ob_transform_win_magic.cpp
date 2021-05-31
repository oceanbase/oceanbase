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

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObTransformWinMagic::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool accepted = false;
  bool is_valid = false;
  bool contain_inner_table = false;
  ObSEArray<ObQueryRefRawExpr*, 8> subqueries;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(ObTransformUtils::get_subquery_expr_from_joined_table(stmt, subqueries))) {
    LOG_WARN("failed to get subquery expr from joined table", K(ret));
  } else if (OB_FAIL(stmt->check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < stmt->get_subquery_expr_size(); ++i) {
    ObStmtMapInfo map_info;
    ObDMLStmt* trans_stmt = NULL;
    ObSelectStmt* subquery = NULL;
    ObQueryRefRawExpr* query_ref = stmt->get_subquery_exprs().at(i);
    if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is invalid", K(ret), K(query_ref), K(subquery));
    } else if (ObOptimizerUtil::find_item(subqueries, query_ref)) {
      // subquery from joined table, do nothing
    } else if (contain_inner_table && !subquery->get_stmt_hint().enable_unnest()) {
      // do not rewrite stmt with inner table with a cost-based rule
    } else if (OB_FAIL(check_subquery_validity(stmt, query_ref, map_info, is_valid))) {
      LOG_WARN("faield to check subquery validity", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(do_transform(stmt, i, map_info, trans_stmt))) {
      LOG_WARN("failed to do transform", K(ret));
    } else if (subquery->get_stmt_hint().enable_unnest()) {
      stmt = trans_stmt;
      accepted = true;
      cost_based_trans_tried_ = true;
    } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, accepted))) {
      LOG_WARN("failed to accept transform", K(ret));
    } else {
    }  // do nothing if the transformation is rejected due to the cost

    if (OB_SUCC(ret) && accepted) {
      if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, map_info.equal_param_map_))) {
        LOG_WARN("failed to append equal params constraints", K(ret));
      } else {
        ctx_->happened_cost_based_trans_ = WIN_MAGIC;
        trans_happened = true;
        LOG_TRACE("equal param constraints", K(map_info.equal_param_map_));
      }
    }
  }
  return ret;
}

int ObTransformWinMagic::adjust_transform_types(uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~transformer_type_);
  }
  return ret;
}

/**
 * @brief ObTransformWinMagic::check_subquery_validity
 * 1. the from list of the subquery should be a subset of the from list of the main query
 * 2. for each condition in the subquery, it should satisfy one of the following:
 *   a. same as a condition in the outer stmt
 *   b. simple comparision, the left and the right param should be the same
 *
 * @return
 */
int ObTransformWinMagic::check_subquery_validity(
    ObDMLStmt* stmt, ObQueryRefRawExpr* query_ref, ObStmtMapInfo& map_info, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool is_correlated = false;
  is_valid = true;
  ObStmtCompareContext context;
  ObSelectStmt* subquery = NULL;
  ObRawExpr* root = NULL;
  ObRawExpr* parent = NULL;
  bool has_ref_assign_user_var = false;
  // 1. check stmt components
  if (OB_ISNULL(stmt) || OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref is invalid", K(ret), K(stmt), K(query_ref), K(subquery));
  } else if (subquery->get_stmt_hint().enable_no_unnest()) {
    is_valid = false;
    LOG_TRACE("has no unnest hint.", K(ret));
  } else if (subquery->get_select_item_size() != 1 || subquery->get_group_expr_size() > 0 || subquery->has_order_by() ||
             subquery->has_rollup() || subquery->has_having() || subquery->has_limit() ||
             subquery->has_window_function() || subquery->has_set_op() || !subquery->get_joined_tables().empty() ||
             subquery->is_hierarchical_query()) {
    is_valid = false;
    LOG_TRACE("subquery is invalid for transform", K(is_valid));
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    LOG_TRACE("has rownum expr", K(is_valid));
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    is_valid = false;
    LOG_TRACE("has assignment ref user variable", K(is_valid));
  } else if (OB_FAIL(ObTransformUtils::is_ref_outer_block_relation(
                 subquery, subquery->get_current_level() - 1, is_correlated))) {
    LOG_WARN("failed to check is correlated stmt", K(ret));
  } else if (is_correlated) {
    // TODO () try to relax this constraint
    is_valid = false;
    LOG_TRACE("stmt ref grand parent expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(
                 subquery->get_select_item(0).expr_, subquery->get_current_level() - 1, is_correlated))) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else if (is_correlated) {
    is_valid = false;
    LOG_TRACE("select expr is correlated", K(ret));
  } else if (OB_FAIL(check_aggr_expr_validity(*subquery, is_valid))) {
    LOG_WARN("failed to check aggr expr validity", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("invalid select expr", K(is_valid));
  } else if (OB_FAIL(ObTransformUtils::find_parent_expr(stmt, query_ref, root, parent))) {
    LOG_WARN("failed to find parent expr", K(ret));
  } else if (OB_ISNULL(parent) || OB_ISNULL(root)) {
    // the subquery has been removed from the stmt
    // but the subquery array is not reset
    is_valid = false;
  } else if (IS_SUBQUERY_COMPARISON_OP(parent->get_expr_type()) || parent->get_expr_type() == T_OP_EXISTS ||
             parent->get_expr_type() == T_OP_NOT_EXISTS) {
    is_valid = false;
    LOG_TRACE("invalid subquery comparison expr", K(is_valid));
  } else if (OB_FAIL(ObStmtComparer::compute_stmt_overlap(subquery, stmt, map_info))) {
    LOG_WARN("failed to compute overlap between stmts", K(ret));
  } else if (OB_UNLIKELY(map_info.cond_map_.count() != subquery->get_condition_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition map size", K(ret), K(map_info.cond_map_.count()), K(subquery->get_condition_size()));
  } else if (OB_FAIL(context.init(subquery, stmt, map_info.table_map_))) {
    LOG_WARN("failed to init stmt compare context", K(ret));
  }
  /**
   * all correlated condition must be equal condition
   *    i.e. only equal condition can be rewrite as a partition by expr
   * non correlated condition either exists in the main query, or is a lossless join condition
   **/
  ObRawExpr* outer_param = NULL;
  ObRawExpr* inner_param = NULL;
  ObSEArray<ObRawExpr*, 4> lossless_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_condition_size(); ++i) {
    ObRawExpr* cond = subquery->get_condition_expr(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(cond));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(cond, subquery->get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      if (map_info.cond_map_.at(i) != OB_INVALID_ID) {
        // existed in the main query
      } else if (OB_FAIL(lossless_conditions.push_back(cond))) {
        LOG_WARN("failed to push back condition", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(
                   cond, subquery->get_current_level(), is_valid, &outer_param, &inner_param))) {
      LOG_WARN("failed to check is euqal correlation", K(ret));
    } else if (!is_valid) {
      LOG_TRACE("is not valid correlated condition", K(is_valid), K(i));
    } else if (OB_ISNULL(outer_param) || OB_ISNULL(inner_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal param have null", K(ret), K(outer_param), K(inner_param));
    } else if (map_info.cond_map_.at(i) != OB_INVALID_ID) {
      // existed in the main query
    } else if (!inner_param->same_as(*outer_param, &context)) {
      // self mapped condition (i.e. t1.c1 = t1'.c1)
      is_valid = false;
      LOG_TRACE("is not valid self mapped condition", K(is_valid), K(i));
    } else if (OB_FAIL(append(map_info.equal_param_map_, context.equal_param_info_))) {
      LOG_WARN("failed to append equal param map", K(ret));
    } else {
      context.equal_param_info_.reset();
    }
  }

  ObSEArray<FromItem, 4> lossless_from;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_from_item_size(); ++i) {
    if (OB_INVALID_ID != map_info.from_map_.at(i)) {
      // do nothing
    } else if (OB_FAIL(lossless_from.push_back(subquery->get_from_item(i)))) {
      LOG_WARN("failed to push back from item", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(check_lossess_join(*subquery, lossless_from, lossless_conditions, is_valid))) {
      LOG_WARN("failed check is lossess join", K(ret));
    } else if (!is_valid) {
      LOG_TRACE("has extra conditions or from items", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::check_aggr_expr_validity(ObSelectStmt& subquery, bool& is_valid)
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

// TODO ()
int ObTransformWinMagic::check_lossess_join(
    ObSelectStmt& subquery, ObIArray<FromItem>& from_items, ObIArray<ObRawExpr*>& conditions, bool& is_valid)
{
  int ret = OB_SUCCESS;
  UNUSED(subquery);
  is_valid = from_items.empty() && conditions.empty();
  return ret;
}

/**
 * @brief ObTransformWinMagic::do_transform
 * @return
 */
int ObTransformWinMagic::do_transform(
    ObDMLStmt* stmt, int64_t query_ref_expr_id, ObStmtMapInfo& map_info, ObDMLStmt*& trans_stmt)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref = NULL;
  ObSqlBitSet<> push_down_table_ids;
  ObSqlBitSet<> push_down_from_ids;
  ObSqlBitSet<> push_down_filter_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_, stmt, trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(trans_stmt) || OB_ISNULL(query_ref = trans_stmt->get_subquery_exprs().at(query_ref_expr_id)) ||
             OB_ISNULL(query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans stmt is invalid", K(ret), K(trans_stmt), K(query_ref));
  } else if (OB_FAIL(compute_push_down_items(
                 *trans_stmt, map_info, push_down_table_ids, push_down_from_ids, push_down_filter_ids))) {
    LOG_WARN("failed to compute push down items", K(ret));
  } else if (OB_FAIL(transform_child_stmt(*trans_stmt,
                 *query_ref->get_ref_stmt(),
                 map_info,
                 push_down_table_ids,
                 push_down_from_ids,
                 push_down_filter_ids))) {
    LOG_WARN("failed to transform child stmts", K(ret));
  } else if (OB_FAIL(transform_upper_stmt(
                 *trans_stmt, query_ref, push_down_table_ids, push_down_from_ids, push_down_filter_ids))) {
    LOG_WARN("failed to transform upper stmt", K(ret));
  } else {
    LOG_TRACE("after win magic", K(*trans_stmt));
  }
  return ret;
}

int ObTransformWinMagic::compute_push_down_items(ObDMLStmt& stmt, ObStmtMapInfo& map_info,
    ObSqlBitSet<>& push_down_table_ids, ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_filter_ids)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> correlated_table_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.table_map_.count(); ++i) {
    int64_t index = map_info.table_map_.at(i);
    if (OB_INVALID_ID == index) {
      // do nothing
    } else if (OB_FAIL(push_down_table_ids.add_member(index + 1))) {
      LOG_WARN("failed to add relation id", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.from_map_.count(); ++i) {
    int64_t index = map_info.from_map_.at(i);
    if (OB_INVALID_ID == index) {
      // do nothing
    } else if (OB_FAIL(push_down_from_ids.add_member(index))) {
      LOG_WARN("failed to add relation id", K(ret));
    }
  }
  // i.e. select * from t,s where t.c = s.c and
  //                              s.c1 = ?  and
  //                              t.c1 > (select min(v.c1) from t v where v.c = s.c);
  for (int64_t i = 0; OB_SUCC(ret) && i < map_info.cond_map_.count(); ++i) {
    int64_t index = map_info.cond_map_.at(i);
    ObRawExpr* cond = NULL;
    ObSEArray<int64_t, 4> relation_ids;
    bool can_cond_pushed_down = true;
    if (OB_INVALID_ID == index) {
      // do nothing
    } else if (OB_UNLIKELY(index < 0 || index >= stmt.get_condition_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is invalid", K(ret), K(index), K(stmt.get_condition_size()));
    } else if (OB_ISNULL(cond = stmt.get_condition_expr(index))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(cond));
    } else if (!cond->get_expr_levels().has_member(stmt.get_current_level())) {
      // do nothing since it does not use current level column
    } else if (OB_FAIL(cond->get_relation_ids().to_array(relation_ids))) {
      LOG_WARN("failed to convert bit set to array", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < relation_ids.count(); ++j) {
        int64_t relation_id = relation_ids.at(j);
        int64_t from_index = OB_INVALID_ID;
        TableItem* table_item = NULL;
        if (push_down_table_ids.has_member(relation_id)) {
          // do nothing
        } else if (OB_ISNULL(table_item = stmt.get_table_item(relation_id - 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if (table_item->is_generated_table()) {
          // do not push down generated table because its may not have unique keys
          can_cond_pushed_down = false;
        } else if (OB_FAIL(is_simple_from_item(stmt, table_item->table_id_, from_index))) {
          LOG_WARN("failed to check is simple from item", K(ret));
        } else if (OB_INVALID_ID == from_index) {
          can_cond_pushed_down = false;
        } else if (OB_FAIL(correlated_table_ids.add_member(relation_id))) {
          LOG_WARN("failed to add correlated table index", K(ret));
        } else if (OB_FAIL(push_down_table_ids.add_member(relation_id))) {
          LOG_WARN("failed to add table index", K(ret));
        } else if (OB_FAIL(push_down_from_ids.add_member(from_index))) {
          LOG_WARN("failed to add from index", K(ret));
        }
      }
      if (OB_SUCC(ret) && can_cond_pushed_down) {
        if (OB_FAIL(push_down_filter_ids.add_member(index))) {
          LOG_WARN("failed to add push down filter id", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr* cond = NULL;
    if (push_down_filter_ids.has_member(i)) {
      // do nothing
    } else if (OB_ISNULL(cond = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(cond));
    } else if (!cond->get_expr_levels().has_member(stmt.get_current_level())) {
      // do nothing
    } else if (!correlated_table_ids.is_superset2(cond->get_relation_ids())) {
      // do nothing
    } else if (cond->has_flag(CNT_SUB_QUERY)) {
      // do not push down condition with subquery
    } else if (OB_FAIL(push_down_filter_ids.add_member(i))) {
      LOG_WARN("failed to add filter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("push down infos", K(push_down_table_ids), K(push_down_filter_ids), K(push_down_from_ids));
  }
  return ret;
}

int ObTransformWinMagic::transform_child_stmt(ObDMLStmt& stmt, ObSelectStmt& subquery, ObStmtMapInfo& map_info,
    ObSqlBitSet<>& push_down_table_ids, ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_filter_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_condition_list;
  ObSEArray<FromItem, 4> new_from_list;
  if (OB_UNLIKELY(map_info.cond_map_.count() != subquery.get_condition_size()) ||
      OB_UNLIKELY(map_info.from_map_.count() != subquery.get_from_item_size()) ||
      OB_UNLIKELY(map_info.table_map_.count() != subquery.get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid map size",
        K(ret),
        K(map_info.cond_map_.count()),
        K(subquery.get_condition_size()),
        K(map_info.from_map_.count()),
        K(subquery.get_from_item_size()),
        K(map_info.table_map_.count()),
        K(subquery.get_table_size()));
  } else if (OB_FAIL(transform_aggr_to_winfunc(subquery, stmt, map_info, push_down_table_ids))) {
    LOG_WARN("failed to transform aggregation expr to winfunc", K(ret));
  }
  // 1. merge condition list
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_condition_size(); ++i) {
    bool correlated = false;
    if (OB_INVALID_ID != map_info.cond_map_.at(i)) {
      // remove mapped condition
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(
                   subquery.get_condition_expr(i), subquery.get_current_level() - 1, correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (correlated) {
      // all correlated expr are converted as window function partition-by
    } else if (OB_FAIL(new_condition_list.push_back(subquery.get_condition_expr(i)))) {
      LOG_WARN("failed to push back condition", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    if (!push_down_filter_ids.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(new_condition_list.push_back(stmt.get_condition_expr(i)))) {
      LOG_WARN("failed to push back new condition", K(ret));
    }
  }
  // 2. merge from list
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_from_item_size(); ++i) {
    if (OB_INVALID_ID != map_info.from_map_.at(i)) {
      // remove mapped from item
    } else if (OB_FAIL(new_from_list.push_back(subquery.get_from_item(i)))) {
      LOG_WARN("failed to push back from item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    if (!push_down_from_ids.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(new_from_list.push_back(stmt.get_from_item(i)))) {
      LOG_WARN("failed to push back outer from item", K(ret));
    }
  }
  // 3. merge table item
  if (OB_SUCC(ret)) {
    ObSEArray<TableItem*, 4> old_table_list;
    if (OB_FAIL(subquery.get_condition_exprs().assign(new_condition_list))) {
      LOG_WARN("failed to assign new condition list", K(ret));
    } else if (OB_FAIL(subquery.reset_from_item(new_from_list))) {
      LOG_WARN("failed to assign new from list", K(ret));
    } else if (OB_FAIL(old_table_list.assign(subquery.get_table_items()))) {
      LOG_WARN("failed to assign table list", K(ret));
    }
    // add push down table
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      ObSEArray<ObDMLStmt::PartExprItem, 4> part_exprs;
      if (!push_down_table_ids.has_member(i + 1)) {
        // do nothing
      } else if (OB_FAIL(subquery.get_table_items().push_back(stmt.get_table_item(i)))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else if (OB_FAIL(stmt.get_part_expr_items(stmt.get_table_item(i)->table_id_, part_exprs))) {
        LOG_WARN("failed to get part expr items", K(ret));
      } else if (part_exprs.empty()) {
        // do nothing
      } else if (OB_FAIL(subquery.set_part_expr_items(part_exprs))) {
        LOG_WARN("failed to set part expr item", K(ret));
      }
    }
    // add push down column
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
      ObRawExpr* col_expr = NULL;
      if (OB_ISNULL(stmt.get_column_item(i)) || OB_ISNULL(col_expr = stmt.get_column_item(i)->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret), K(col_expr));
      } else if (!push_down_table_ids.is_superset2(col_expr->get_relation_ids())) {
        // do nothing
      } else if (OB_FAIL(subquery.get_column_items().push_back(stmt.get_column_items().at(i)))) {
        LOG_WARN("failed to add column item", K(ret));
      }
    }
    // remove redundant Table Item and Column Item
    for (int64_t i = 0; OB_SUCC(ret) && i < map_info.table_map_.count(); ++i) {
      TableItem* old_table_item = old_table_list.at(i);
      TableItem* new_table_item = NULL;
      int64_t outer_table_index = map_info.table_map_.at(i);
      if (outer_table_index == OB_INVALID_ID) {
        // do nothing
      } else if (outer_table_index < 0 || outer_table_index >= stmt.get_table_size() ||
                 OB_ISNULL(new_table_item = stmt.get_table_item(outer_table_index))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid outer table index", K(ret), K(outer_table_index), K(new_table_item));
      } else if (OB_FAIL(ObTransformUtils::merge_table_items(&subquery, new_table_item, old_table_item, NULL))) {
        LOG_WARN("failed to merge table items", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(&subquery, new_table_item, old_table_item))) {
        LOG_WARN("failed to replace table info in semi infos", K(ret));
      } else if (OB_FAIL(subquery.remove_table_item(old_table_item))) {
        LOG_WARN("failed to remove table items", K(ret));
      }
    }
    // remove distinct
    if (OB_SUCC(ret)) {
      subquery.assign_all();
    }
  }
  return ret;
}

int ObTransformWinMagic::transform_upper_stmt(ObDMLStmt& stmt, ObQueryRefRawExpr* query_ref,
    ObSqlBitSet<>& push_down_table_ids, ObSqlBitSet<>& push_down_from_ids, ObSqlBitSet<>& push_down_cond_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> remain_conds;
  ObSEArray<FromItem, 4> remain_froms;
  ObSEArray<TableItem*, 4> remain_tables;
  ObSEArray<ColumnItem, 4> remain_columns;
  ObSEArray<ObRawExpr*, 8> output_cols;
  ObSEArray<uint64_t, 4> removed_table_ids;
  ObSelectStmt* subquery = NULL;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt()) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is invalid", K(ret), K(query_ref), K(subquery), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    TableItem* item = NULL;
    if (OB_ISNULL(item = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (push_down_table_ids.has_member(i + 1)) {
      if (OB_FAIL(stmt.remove_part_expr_items(item->table_id_))) {
        LOG_WARN("failed to remove part expr item", K(ret));
      } else if (OB_FAIL(removed_table_ids.push_back(item->table_id_))) {
        LOG_WARN("failed to push back removed table id", K(ret));
      }
    } else if (OB_FAIL(remain_tables.push_back(item))) {
      LOG_WARN("failed to push back table item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    const ColumnItem& col_item = stmt.get_column_items().at(i);
    if (OB_ISNULL(col_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col expr is null", K(ret));
    } else if (push_down_table_ids.is_superset2(col_item.expr_->get_relation_ids())) {
      if (OB_FAIL(output_cols.push_back(col_item.expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    } else if (OB_FAIL(remain_columns.push_back(col_item))) {
      LOG_WARN("failed to push back remain column item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    if (push_down_from_ids.has_member(i)) {
      // remove pushed from item
    } else if (OB_FAIL(remain_froms.push_back(stmt.get_from_item(i)))) {
      LOG_WARN("failed to push back from item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    if (push_down_cond_ids.has_member(i)) {
      // remove pushed condition
    } else if (OB_FAIL(remain_conds.push_back(stmt.get_condition_expr(i)))) {
      LOG_WARN("failed to push back condition expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.reset_table_item(remain_tables))) {
      LOG_WARN("failed to reset table items", K(ret));
    } else if (OB_FAIL(stmt.get_column_items().assign(remain_columns))) {
      LOG_WARN("failed to reset column items", K(ret));
    } else if (OB_FAIL(stmt.reset_from_item(remain_froms))) {
      LOG_WARN("failed to reset from items", K(ret));
    } else if (OB_FAIL(stmt.get_condition_exprs().assign(remain_conds))) {
      LOG_WARN("failed to reset condition exprs", K(ret));
    } else { /*do nothing*/
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_cols.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, output_cols.at(i), subquery))) {
        LOG_WARN("failed to create select expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> select_exprs;
    ObSEArray<ObRawExpr*, 4> view_output;
    TableItem* new_table_item = NULL;
    if (OB_FAIL(select_exprs.push_back(query_ref))) {
      LOG_WARN("failed to push back query ref expr", K(ret));
    } else if (OB_FAIL(append(select_exprs, output_cols))) {
      LOG_WARN("failed to append column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, subquery, new_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(new_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view table item is null", K(ret), K(new_table_item));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *new_table_item, &stmt, view_output))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(), query_ref))) {
      LOG_WARN("failed to remove query ref expr", K(ret));
    } else if ((stmt.is_delete_stmt() || stmt.is_update_stmt()) &&
               OB_FAIL(ObTransformUtils::adjust_updatable_view(
                   *ctx_->expr_factory_, static_cast<ObDelUpdStmt*>(&stmt), *new_table_item))) {
      LOG_WARN("failed to adjust updatable view", K(ret));
    } else {
      stmt.get_table_items().pop_back();  // removed for replace_inner_stmt_expr
      if (OB_FAIL(stmt.replace_inner_stmt_expr(select_exprs, view_output))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
      } else if (OB_FAIL(stmt.get_table_items().push_back(new_table_item))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else if (OB_FAIL(stmt.add_from_item(new_table_item->table_id_))) {
        LOG_WARN("failed to add from item", K(ret));
      }
    }
    for (int64_t s_id = 0; OB_SUCC(ret) && s_id < stmt.get_semi_infos().count(); ++s_id) {
      SemiInfo* semi = NULL;
      bool removed = false;
      if (OB_ISNULL(semi = stmt.get_semi_infos().at(s_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semi info is null", K(ret), K(s_id));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi->left_table_ids_, removed_table_ids, &removed))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (!removed) {
        /*do nothing*/
      } else if (OB_FAIL(semi->left_table_ids_.push_back(new_table_item->table_id_))) {
        LOG_WARN("failed to push back table id", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt.update_column_item_rel_id())) {
      LOG_WARN("failed to update column item relation id", K(ret));
    } else if (OB_FAIL(subquery->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(subquery->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item relation id", K(ret));
    } else if (OB_FAIL(subquery->pullup_stmt_level())) {
      LOG_WARN("failed to pull up stmt level", K(ret));
    } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(stmt.get_parent_namespace_stmt()))) {
      LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt info", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::transform_aggr_to_winfunc(
    ObSelectStmt& subquery, ObDMLStmt& stmt, ObStmtMapInfo& map_info, ObSqlBitSet<>& push_down_table_ids)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObRawExpr* outer_param = NULL;
  ObRawExpr* inner_param = NULL;
  // 1. compute partition exprs
  ObSEArray<ObRawExpr*, 4> partition_exprs;
  ObSEArray<ObRawExpr*, 4> inner_exprs;
  ObSEArray<ObRawExpr*, 4> outer_exprs;
  ObSEArray<ObRawExpr*, 4> type_safe_join_expr;
  ObSEArray<uint64_t, 4> tables_with_pk;
  ObSEArray<ObRawExpr*, 4> nullable_part_exprs;
  bool is_type_safe = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_condition_size(); ++i) {
    ObRawExpr* expr = NULL;
    bool is_correlated = false;
    if (OB_ISNULL(expr = subquery.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(
                   expr, subquery.get_current_level(), is_valid, &outer_param, &inner_param))) {
      LOG_WARN("failed to check is equal condition", K(ret));
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("a correlated condition is expected to be equal correlation", K(ret));
    } else if (OB_FAIL(inner_exprs.push_back(inner_param))) {
      LOG_WARN("failed to push back inner param", K(ret));
    } else if (OB_FAIL(outer_exprs.push_back(outer_param))) {
      LOG_WARN("failed to push back outer param", K(ret));
    } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(inner_param->get_result_type(),
                   outer_param->get_result_type(),
                   outer_param->get_result_type(),
                   is_type_safe))) {
      LOG_WARN("failed to check is type safe compare for outer param", K(ret));
    } else if (!is_type_safe) {
      // do nothing
    } else if (OB_FAIL(type_safe_join_expr.push_back(outer_param))) {
      LOG_WARN("failed to push back outer pararm", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    bool is_unique = false;
    TableItem* item = stmt.get_table_item(i);
    ObSEArray<ObRawExpr*, 4> unique_keys;
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(item));
    } else if (!push_down_table_ids.has_member(i + 1) || ObOptimizerUtil::find_item(map_info.table_map_, i)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_columns_unique(
                   type_safe_join_expr, item->ref_id_, ctx_->schema_checker_, is_unique))) {
      LOG_WARN("failed to check is join exprs unique", K(ret));
    } else if (is_unique) {
      // do nothing
    } else if (OB_FAIL(tables_with_pk.push_back(item->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    } else if (OB_FAIL(ObTransformUtils::generate_unique_key(ctx_, &stmt, item, unique_keys))) {
      LOG_WARN("failed to generate unique key", K(ret));
    } else if (OB_FAIL(append(partition_exprs, unique_keys))) {
      LOG_WARN("failed to append unique keys", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < outer_exprs.count(); ++i) {
    bool is_valid = true;
    bool is_nullable = true;
    if (OB_FAIL(use_given_tables(outer_exprs.at(i), tables_with_pk, is_valid))) {
      LOG_WARN("failed to check is useless partition expr", K(ret));
    } else if (is_valid) {
      // do nothing
    } else if (OB_FAIL(partition_exprs.push_back(inner_exprs.at(i)))) {
      LOG_WARN("failed to push bakc outer expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_expr_nullable(&stmt, outer_exprs.at(i), is_nullable))) {
      LOG_WARN("failed to check expr nullable", K(ret));
    } else if (!is_nullable) {
      // do nothing
    } else if (OB_FAIL(nullable_part_exprs.push_back(inner_exprs.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_aggr_item_size(); ++i) {
    ObWinFunRawExpr* win_expr = NULL;
    if (OB_ISNULL(subquery.get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregation item is null", K(ret));
    } else if (OB_FAIL(wrap_case_when_if_necessary(&subquery, *subquery.get_aggr_item(i), nullable_part_exprs))) {
      LOG_WARN("failed to wrap case when if necessary", K(ret));
    } else if (OB_FAIL(create_window_function(subquery.get_aggr_item(i), partition_exprs, win_expr))) {
      LOG_WARN("failed to create window function", K(ret));
    } else if (OB_FAIL(subquery.add_window_func_expr(win_expr))) {
      LOG_WARN("failed to add window func expr", K(ret));
    } else {
      // hack for replace expr
      // we do not want win_expr.aggr is replaced as win_expr.win_expr...
      win_expr->set_agg_expr(NULL);
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> aggr_exprs;
    ObSEArray<ObRawExpr*, 4> win_exprs;
    if (OB_FAIL(append(aggr_exprs, subquery.get_aggr_items()))) {
      LOG_WARN("failed to convert aggregation exprs", K(ret));
    } else if (OB_FAIL(append(win_exprs, subquery.get_window_func_exprs()))) {
      LOG_WARN("failed to convert window function exprs", K(ret));
    } else if (OB_FAIL(subquery.replace_inner_stmt_expr(aggr_exprs, win_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
    // hack for replace expr
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_aggr_item_size(); ++i) {
      if (OB_UNLIKELY(!aggr_exprs.at(i)->is_aggr_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the expr is expected to aggregation expr", K(ret));
      } else {
        subquery.get_window_func_expr(i)->set_agg_expr(static_cast<ObAggFunRawExpr*>(aggr_exprs.at(i)));
      }
    }
    subquery.clear_aggr_item();
  }
  return ret;
}

int ObTransformWinMagic::create_window_function(
    ObAggFunRawExpr* agg_expr, ObIArray<ObRawExpr*>& partition_exprs, ObWinFunRawExpr*& win_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(expr_factory), K(agg_expr));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_WINDOW_FUNCTION, win_expr))) {
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
    win_expr->set_expr_level(agg_expr->get_expr_level());
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(win_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize windown function", K(ret));
    } else if (OB_FAIL(win_expr->pull_relation_id_and_levels(agg_expr->get_expr_level()))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

int ObTransformWinMagic::wrap_case_when_if_necessary(
    ObDMLStmt* stmt, ObAggFunRawExpr& aggr_expr, ObIArray<ObRawExpr*>& nullable_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> check_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < nullable_exprs.count(); ++i) {
    ObOpRawExpr* check_not_null = NULL;
    if (ObTransformUtils::add_is_not_null(ctx_, stmt, nullable_exprs.at(i), check_not_null)) {
      LOG_WARN("failed to add is not null expr", K(ret));
    } else if (OB_FAIL(check_exprs.push_back(check_not_null))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !nullable_exprs.empty()) {
    ObRawExpr* when_expr = NULL;
    ObRawExpr* then_expr = NULL;
    ObRawExpr* default_expr = NULL;
    if (nullable_exprs.count() >= 2) {
      ObOpRawExpr* and_expr = NULL;
      if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_AND, and_expr))) {
        LOG_WARN("failed to create and expr", K(ret));
      } else if (OB_ISNULL(and_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("and expr is null", K(ret), K(and_expr));
      } else if (OB_FAIL(and_expr->get_param_exprs().assign(check_exprs))) {
        LOG_WARN("failed to assign param expr", K(ret));
      } else {
        when_expr = and_expr;
      }
    } else if (nullable_exprs.count() == 1) {
      when_expr = check_exprs.at(0);
    }
    if (OB_SUCC(ret)) {
      ObConstRawExpr* const_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, default_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      } else if (aggr_expr.get_param_count() != 0) {
        then_expr = aggr_expr.get_param_expr(0);
      } else if (aggr_expr.get_expr_type() != T_FUN_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the expr is expected to count(*)", K(ret), K(aggr_expr));
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 1L, const_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else {
        then_expr = const_expr;
      }
    }
    if (OB_SUCC(ret)) {
      ObCaseOpRawExpr* case_expr = NULL;
      if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_CASE, case_expr))) {
        LOG_WARN("failed to create case expr", K(ret));
      } else if (OB_FAIL(case_expr->add_when_param_expr(when_expr))) {
        LOG_WARN("failed to add when param expr", K(ret));
      } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
        LOG_WARN("failed to add then expr", K(ret));
      } else {
        case_expr->set_default_param_expr(default_expr);
        aggr_expr.clear_child();
        if (OB_FAIL(aggr_expr.add_real_param_expr(case_expr))) {
          LOG_WARN("failed to add real param expr", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief is_simple_from_item
 * not used in joined_table
 * not has semi info filters
 * TODO try to relax this constraint
 * @return
 */
int ObTransformWinMagic::is_simple_from_item(ObDMLStmt& stmt, uint64_t table_id, int64_t& from_index)
{
  int ret = OB_SUCCESS;
  from_index = stmt.get_from_item_idx(table_id);
  if (from_index < 0 || from_index >= stmt.get_from_item_size()) {
    from_index = OB_INVALID_ID;
  } else if (stmt.get_from_item(from_index).is_joined_) {
    from_index = OB_INVALID_ID;
  }
  return ret;
}

int ObTransformWinMagic::use_given_tables(const ObRawExpr* expr, const ObIArray<uint64_t>& table_ids, bool& is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_column_ref_expr()) {
    uint64_t tid = static_cast<const ObColumnRefRawExpr*>(expr)->get_table_id();
    is_valid = ObOptimizerUtil::find_item(table_ids, tid);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(use_given_tables(expr->get_param_expr(i), table_ids, is_valid))) {
        LOG_WARN("failed to check is useless partiton expr", K(ret));
      }
    }
  }
  return ret;
}

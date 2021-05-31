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
#include "ob_transform_groupby_placement.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformGroupByPlacement::transform_one_stmt
 *  select sum(r.value) from r, t, p where r.c1 = t.c1 and t.c2 = p.c2 and p.c3 > 0 group by p.c4;
 *  ==>
 *  select sum(r_sum * t_cnt * p_cnt) from
 *      (select r.c1, sum(r.value) as r_sum from r group by r.c1) v1,
 *      (select t.c1, t.c2, count(*) as t_cnt from t group by t.c1, t.c2) v2,
 *      (select p.c2, p.c4, count(*) as p_cnt from p where p.c3 > 0 group by p.c2, p.c4) v3
 *  where v1.c1 = v2.c1 and v2.c2 = v3.c2 group by v3.c4;
 *
 *  select sum(r.value) from r, t where r.ukey = t.c1 group by t.c2;
 *  ==>
 *  select sum(r.value * v2.t_cnt) from
 *       r, (select c1, c2, count(*) as t_cnt from t group by t.c1, t.c2) v2
 *  where r.ukey = v2.c1 group by v2.c2;
 *
 * select sum(r.value) from r left join t on r.c1 = t.c1 where r.c3 > 0 group by r.c2;
 * ==>
 * select sum(r_sum * case when v2.c1 is null then 1 else v2.t_cnt) from
 *     (select r.c1, r.c2, sum(r.value) as r_sum where r.c3 > 0 from r group by r.c1, r.c2) v1,
 *     (select t.c1, count(*) as t_cnt from t group by t.c1) v2
 * where v1.c1 = v2.c1 group by v1.c2;
 *
 */
int ObTransformGroupByPlacement::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transform_groupby_push_down(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to transfrom group by push down", K(ret));
  } else if (trans_happened) {
    // do nothing
  } else if (OB_FAIL(transform_groupby_pull_up(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("faield to transform group by pull up", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::adjust_transform_types(uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~transformer_type_);
  }
  return ret;
}

int ObTransformGroupByPlacement::transform_groupby_push_down(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<PushDownParam, 4> params;
  ObSEArray<uint64_t, 4> flattern_joined_tables;
  ObSelectStmt* trans_stmt = NULL;
  bool is_valid = false;
  bool is_happend = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(check_groupby_push_down_validity(static_cast<ObSelectStmt*>(stmt), is_valid))) {
    LOG_WARN("failed to check group by push down validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(
                 compute_push_down_param(static_cast<ObSelectStmt*>(stmt), params, flattern_joined_tables, is_valid))) {
    LOG_WARN("failed to compute push down param", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(do_groupby_push_down(
                 static_cast<ObSelectStmt*>(stmt), params, flattern_joined_tables, trans_stmt, is_happend))) {
    LOG_WARN("failed to transform stmt", K(ret));
  } else if (!is_happend) {
    // do nothing
  } else if (stmt->get_stmt_hint().enable_place_groupby()) {
    stmt = trans_stmt;
    trans_happened = true;
    cost_based_trans_tried_ = true;
    LOG_TRACE("has place group by hint.");
  } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, trans_happened))) {
    LOG_WARN("failed to accept transform", K(ret));
  } else if (!trans_happened) {
    LOG_TRACE("group by push down is reject due to cost", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::check_groupby_push_down_validity(ObSelectStmt* stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rand = false;
  bool contain_inner_table = false;
  is_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (stmt->get_stmt_hint().enable_no_place_groupby()) {
    is_valid = false;
    LOG_TRACE("has no place groupby hint.", K(is_valid));
  } else if (OB_FAIL(stmt->check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  } else if (contain_inner_table && !stmt->get_stmt_hint().enable_place_groupby()) {
    is_valid = false;
    // do not rewrite inner table stmt with cost-based rule
  } else if (stmt->has_window_function() || stmt->has_rollup() || stmt->get_semi_infos().count() > 0 ||
             stmt->get_subquery_exprs().count() > 0 || stmt->get_aggr_item_size() <= 0 ||
             stmt->get_group_exprs().empty() || stmt->is_hierarchical_query() || stmt->is_set_stmt()) {
    // do not rewrite scalar group by
    // select count(*) from t1, t2 where t1.c1 = t2.c1;
    // select sum(t1_cnt * t2_cnt) from (select c1, count(*) from t1 group by c1),
    //                                  (select c1, count(*) from t2 group by c1);
    //                                  where t1.c1 = t2.c1;
    is_valid = false;
    LOG_TRACE("invalid stmt for eager aggregation", K(is_valid));
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
  } else if (OB_FAIL(stmt->has_rand(has_rand))) {
    LOG_WARN("failed to check stmt has rand", K(ret));
  } else if (has_rand) {
    is_valid = false;
  } else if (OB_FAIL(check_groupby_validity(*stmt, is_valid))) {
    LOG_WARN("failed to check group by validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_collation_validity(*stmt, is_valid))) {
    LOG_WARN("failed to check collation validity", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
    ObAggFunRawExpr* aggr_expr = NULL;
    if (OB_ISNULL(aggr_expr = stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params are invalid", K(ret), K(aggr_expr));
    } else if ((aggr_expr->get_expr_type() != T_FUN_SUM && aggr_expr->get_expr_type() != T_FUN_COUNT &&
                   aggr_expr->get_expr_type() != T_FUN_MIN && aggr_expr->get_expr_type() != T_FUN_MAX) ||
               aggr_expr->is_param_distinct()) {
      is_valid = false;
      LOG_TRACE("invalid aggregation type for group by placement",
          K(is_valid),
          K(aggr_expr->get_expr_type()),
          K(aggr_expr->is_param_distinct()));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::check_groupby_validity(const ObSelectStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObRawExpr*, 4> exprs;
  if (OB_FAIL(stmt.get_select_exprs(exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(append(exprs, stmt.get_having_exprs()))) {
    LOG_WARN("failed to get having exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_order_item_size(); ++i) {
      if (OB_FAIL(exprs.push_back(stmt.get_order_item(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    ObSEArray<ObRawExpr*, 4> col_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      if (OB_FAIL(extract_non_agg_columns(exprs.at(i), stmt.get_current_level(), col_exprs))) {
        LOG_WARN("failed to extract non agg columns", K(ret));
      } else {
        is_valid = ObOptimizerUtil::subset_exprs(col_exprs, stmt.get_group_exprs());
      }
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::check_collation_validity(const ObDMLStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_str = false;
  ObRawExpr* col_expr = NULL;
  ObCollationType type = CS_TYPE_INVALID;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_column_size(); ++i) {
    if (OB_ISNULL(col_expr = stmt.get_column_items().at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret), K(col_expr));
    } else if (!ob_is_string_or_lob_type(col_expr->get_data_type())) {
      // do nothing
    } else if (!has_str) {
      type = col_expr->get_collation_type();
      has_str = true;
    } else {
      is_valid = (type == col_expr->get_collation_type());
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::compute_push_down_param(
    ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObIArray<uint64_t>& flattern_joined_tables, bool& is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(params.prepare_allocate(stmt->get_table_size()))) {
    LOG_WARN("failed to preallocate table", K(ret));
  }
  /// assume table bit index is valid for the stmt
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(params.at(i).table_bit_index_.add_member(i + 1))) {
      LOG_WARN("failed to add table bit index", K(ret));
    }
  }

  /// 1. merge tables according to aggregation exprs
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
    /// each group expr uses columns from the same table
    ObRawExpr* aggr_item = NULL;
    bool has = false;
    ObSqlBitSet<> table_set;
    if (OB_ISNULL(aggr_item = stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group expr is null", K(ret), K(aggr_item));
    } else if (OB_FAIL(has_stmt_column(aggr_item, stmt->get_current_level(), has))) {
      LOG_WARN("failed to check has current stmt column", K(ret));
    } else if (!has) {
      // do nothing
    } else if (OB_FAIL(table_set.add_members2(aggr_item->get_relation_ids()))) {
      LOG_WARN("failed to add table indexes", K(ret));
    } else if (OB_FAIL(merge_tables(params, table_set))) {
      LOG_WARN("failed to merge tables", K(ret));
    }
  }

  /// 2. merge tables according to filterable join conditions
  if (OB_SUCC(ret) && is_valid && stmt->get_table_size() > 2) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      bool is_valid = false;
      ObRawExpr* cond = NULL;
      ObSqlBitSet<> table_set;
      if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret), K(cond));
      } else if (OB_FAIL(is_filterable_join(stmt, cond, params, is_valid))) {
        LOG_WARN("failed to check is filterable join", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(table_set.add_members2(cond->get_relation_ids()))) {
        LOG_WARN("failed to add table indexes", K(ret));
      } else if (OB_FAIL(merge_tables(params, table_set))) {
        LOG_WARN("failed to merge tables", K(ret));
      }
    }
  }

  /// 3. merge tables acccording to joined tables
  /// can improve. (a join b) left join (c join d)
  /// (a, b) can be put into the same view
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_joined_tables().count(); ++i) {
    JoinedTable* joined_table = stmt->get_joined_tables().at(i);
    ObSqlBitSet<> table_bit_set;
    bool should_merge = false;
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *joined_table, table_bit_set))) {
      LOG_WARN("failed to convert table id array to bit set", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !should_merge && j < params.count(); ++j) {
        if (params.at(j).table_bit_index_.overlap(table_bit_set) && params.at(j).table_bit_index_.num_members() >= 2) {
          should_merge = true;
        }
      }
    }
    if (OB_SUCC(ret) && !should_merge) {
      bool is_valid_aggr = false;
      if (OB_FAIL(check_outer_join_aggr(stmt, joined_table, is_valid_aggr))) {
        LOG_WARN("failed to check outer join aggr", K(ret));
      } else if (!is_valid_aggr) {
        should_merge = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (should_merge) {
        if (OB_FAIL(merge_tables(params, table_bit_set))) {
          LOG_WARN("failed to merge tables", K(ret));
        }
      } else if (OB_FAIL(flattern_joined_tables.push_back(joined_table->table_id_))) {
        LOG_WARN("failed to push back joined table", K(ret));
      }
    }
  }
  LOG_TRACE("after pull up groupby", K(params));
  if (OB_SUCC(ret) && is_valid) {
    is_valid = is_valid && get_valid_eager_aggr_num(params) > 1;
  }
  return ret;
}

int ObTransformGroupByPlacement::check_outer_join_aggr(ObSelectStmt* stmt, JoinedTable* joined_table, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> null_table_set;
  is_valid = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (OB_FAIL(get_null_side_tables(*stmt, *joined_table, null_table_set))) {
    LOG_WARN("failed to get null side tables", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
    ObRawExpr* param = NULL;
    ObSEArray<ObRawExpr*, 4> columns;
    ObSEArray<ObRawExpr*, 4> null_table_columns;
    bool is_valid_aggr = true;
    if (OB_ISNULL(stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr item is null", K(ret));
    } else if (stmt->get_aggr_item(i)->get_param_count() <= 0) {
      // do nothing
    } else if (OB_ISNULL(param = stmt->get_aggr_item(i)->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(param));
    } else if (!param->get_expr_levels().has_member(stmt->get_current_level()) ||
               !param->get_relation_ids().overlap2(null_table_set)) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(param, columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, columns, null_table_set, null_table_columns))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(param, null_table_columns, is_valid_aggr))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    } else if (!is_valid_aggr) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::get_null_side_tables(
    ObDMLStmt& stmt, JoinedTable& joined_table, ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  TableItem* null_table = NULL;
  if (OB_ISNULL(joined_table.left_table_) || OB_ISNULL(joined_table.right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (joined_table.is_left_join()) {
    null_table = joined_table.right_table_;
  } else if (joined_table.is_right_join()) {
    null_table = joined_table.left_table_;
  } else if (joined_table.is_full_join()) {
    null_table = &joined_table;
  }
  if (OB_SUCC(ret) && NULL != null_table) {
    if (OB_FAIL(ObTransformUtils::get_table_rel_ids(stmt, *null_table, table_set))) {
      LOG_WARN("failed to get table relation ids", K(ret));
    }
  }
  if (OB_SUCC(ret) && (joined_table.is_left_join() || joined_table.is_inner_join()) &&
      joined_table.left_table_->is_joined_table()) {
    if (OB_FAIL(get_null_side_tables(stmt, static_cast<JoinedTable&>(*joined_table.left_table_), table_set))) {
      LOG_WARN("failed to get null side tables", K(ret));
    }
  }
  if (OB_SUCC(ret) && (joined_table.is_right_join() || joined_table.is_inner_join()) &&
      joined_table.right_table_->is_joined_table()) {
    if (OB_FAIL(get_null_side_tables(stmt, static_cast<JoinedTable&>(*joined_table.right_table_), table_set))) {
      LOG_WARN("failed to get null side tables", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::extract_non_agg_columns(
    ObRawExpr* expr, const int64_t stmt_level, ObIArray<ObRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (!expr->get_expr_levels().has_member(stmt_level)) {
    // do nothing
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(col_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else if (expr->is_aggr_expr()) {
    // do nothing
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_non_agg_columns(expr->get_param_expr(i), stmt_level, col_exprs)))) {
        LOG_WARN("failed to extract non agg columns", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::is_filterable_join(
    ObSelectStmt* stmt, ObRawExpr* join_cond, ObIArray<PushDownParam>& params, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  UNUSED(params);
  ObRawExpr* left_expr = NULL;
  ObRawExpr* right_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(join_cond));
  } else if (OB_FAIL(has_stmt_column(join_cond, stmt->get_current_level(), is_valid))) {
    LOG_WARN("failed to check expr has current stmt column", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (join_cond->get_relation_ids().num_members() <= 1) {
    is_valid = false;
  } else if (!(join_cond->get_expr_type() >= T_OP_EQ && join_cond->get_expr_type() <= T_OP_GT)) {
    is_valid = false;
  } else if (OB_ISNULL(left_expr = join_cond->get_param_expr(0)) ||
             OB_ISNULL(right_expr = join_cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are null", K(ret), K(left_expr), K(right_expr));
  } else if (!left_expr->get_expr_levels().has_member(stmt->get_current_level()) ||
             !right_expr->get_expr_levels().has_member(stmt->get_current_level()) ||
             left_expr->get_relation_ids().overlap(right_expr->get_relation_ids())) {
    is_valid = false;
  } else if (OB_FAIL(check_join_expr_validity(stmt, params, left_expr, is_valid))) {
    LOG_WARN("failed to check filter join", K(ret));
  } else if (is_valid) {
    // do nothing
  } else if (OB_FAIL(check_join_expr_validity(stmt, params, right_expr, is_valid))) {
    LOG_WARN("failed to check filter join", K(ret));
  }
  if (OB_SUCC(ret) && is_valid) {
    LOG_TRACE("filter join", K(*join_cond));
  }
  return ret;
}

int ObTransformGroupByPlacement::check_join_expr_validity(
    ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObRawExpr* expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  UNUSED(params);
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < stmt->get_aggr_item_size(); ++i) {
      ObAggFunRawExpr* aggr = stmt->get_aggr_item(i);
      ObRawExpr* param_expr = NULL;
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregation expr is null", K(ret), K(aggr));
      } else if (aggr->get_param_count() == 0) {
        is_valid = true;
      } else if (OB_ISNULL(param_expr = aggr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (!param_expr->get_expr_levels().has_member(stmt->get_current_level())) {
        // do nothing
      } else {
        is_valid = param_expr->get_relation_ids().overlap(col->get_relation_ids());
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_, stmt, col, is_valid, &ctx_->equal_sets_))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::do_groupby_push_down(ObSelectStmt* stmt, ObIArray<PushDownParam>& params,
    ObIArray<uint64_t>& flattern_joined_tables, ObSelectStmt*& trans_stmt, bool& trans_happend)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> outer_table_set;
  trans_happend = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(stmt));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt(trans_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(trans_stmt->deep_copy(*ctx_->stmt_factory_, *ctx_->expr_factory_, *stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  }
  /// maintain tables on outer join null side
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    bool on_null_side = false;
    if (OB_ISNULL(stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is invalid", K(ret));
    } else if (OB_FAIL(
                   ObOptimizerUtil::is_table_on_null_side(stmt, stmt->get_table_item(i)->table_id_, on_null_side))) {
      LOG_WARN("failed to check is table on null side", K(ret));
    } else if (!on_null_side) {
      // do nothing
    } else if (OB_FAIL(outer_table_set.add_member(i + 1))) {
      LOG_WARN("failed to add table index", K(ret));
    }
  }
  /// distribute where filters
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmt->get_condition_size(); ++i) {
    ObRawExpr* cond_expr = trans_stmt->get_condition_expr(i);
    if (OB_FAIL(distribute_filter(trans_stmt, params, outer_table_set, cond_expr))) {
      LOG_WARN("failed to distributed filter to views", K(ret));
    }
  }
  /// distribute outer join on conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < flattern_joined_tables.count(); ++i) {
    uint64_t table_id = flattern_joined_tables.at(i);
    if (OB_FAIL(distribute_joined_on_conds(trans_stmt, params, trans_stmt->get_joined_table(table_id)))) {
      LOG_WARN("failed to distributed joined condition to view", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(distribute_group_aggr(trans_stmt, params))) {
      LOG_WARN("faield to distribute expr into view", K(ret));
    } else if (get_valid_eager_aggr_num(params) <= 0) {
      // do nothing
    } else if (OB_FAIL(transform_groupby_push_down(trans_stmt, flattern_joined_tables, outer_table_set, params))) {
      LOG_WARN("failed to transform group by push down", K(ret));
    } else {
      trans_happend = true;
      LOG_TRACE("trans stmt", K(*trans_stmt));
    }
  }
  return ret;
}

/// distribute group, aggregation expr into view
int ObTransformGroupByPlacement::distribute_group_aggr(ObSelectStmt* stmt, ObIArray<PushDownParam>& params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> aggr_list;
  ObSEArray<ObRawExpr*, 4> group_cols;
  int64_t total_sum_count = 0;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(append(aggr_list, stmt->get_aggr_items()))) {
    LOG_WARN("failed to append aggregation exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(stmt->get_group_exprs(), group_cols))) {
    LOG_WARN("failed to extract group columns", K(ret));
  } else {
    total_sum_count = get_count_sum_num(aggr_list);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(add_exprs(
            group_cols, stmt->get_current_level(), params.at(i).table_bit_index_, params.at(i).join_columns_))) {
      LOG_WARN("failed to add owned group columns", K(ret));
    } else if (OB_FAIL(add_exprs(
                   aggr_list, stmt->get_current_level(), params.at(i).table_bit_index_, params.at(i).aggr_exprs_))) {
      LOG_WARN("failed to add owned aggr exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObAggFunRawExpr* count_expr = NULL;
    PushDownParam& param = params.at(i);
    int64_t view_sum_count_num = get_count_sum_num(param.aggr_exprs_);
    bool is_unique = false;
    if (param.table_bit_index_.is_empty()) {
      continue;
    } else if (OB_FAIL(check_unique(stmt, param, is_unique))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    } else if (total_sum_count == view_sum_count_num || is_unique) {
      LOG_TRACE("no need to add count expr", K(i), K(is_unique), K(total_sum_count), K(view_sum_count_num));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_COUNT, count_expr))) {
      LOG_WARN("failed to create new aggregation expr", K(ret));
    } else if (OB_ISNULL(count_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the copied aggregation expr is null", K(ret), K(count_expr));
    } else if (OB_FAIL(count_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize count expr", K(ret));
    } else if (OB_FAIL(param.aggr_exprs_.push_back(count_expr))) {
      LOG_WARN("failed to push back count expr", K(ret));
    } else {
      count_expr->set_expr_level(stmt->get_current_level());
    }
    if (OB_SUCC(ret) &&
        (is_unique || param.aggr_exprs_.empty() || (param.join_columns_.empty() && param.group_exprs_.empty()))) {
      // its group-by exprs are unique
      // there is no need to do eager aggregation
      // scalar aggregation may change semantics: e.g.
      // Q1: select count(*) from t1, t2;
      // Q2: select sum(t1_cnt * t2_cnt) from (select count(*) as t1_cnt from t1),
      //                                      (select count(*) as t2_cnt from t2);
      // if t1, t2 are empty, Q1 return 0, Q2 return NULL
      if (OB_UNLIKELY(param.correlated_joined_tables_.count() != param.filter_exprs_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("correlated joined tables count should equal to filter exprs count",
            K(ret),
            K(param.correlated_joined_tables_.count()),
            K(param.filter_exprs_.count()));
      } else {
        JoinedTable* joined_table = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < param.filter_exprs_.count(); ++i) {
          if (NULL == (joined_table = param.correlated_joined_tables_.at(i))) {
            // NULL means this filter from where condition, do nothing
          } else if (OB_FAIL(joined_table->join_conditions_.push_back(param.filter_exprs_.at(i)))) {
            LOG_WARN("faield to push back expr", K(ret));
          }
        }
        param.reset();
      }
    }
  }
  LOG_TRACE("transform params", K(params));
  return ret;
}

/// distribute where contiditons into views
int ObTransformGroupByPlacement::distribute_filter(
    ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObSqlBitSet<>& outer_join_tables, ObRawExpr* cond)
{
  int ret = OB_SUCCESS;
  bool is_simple_filter = false;
  bool can_push_down = false;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(cond));
  } else if (!cond->get_expr_levels().has_member(stmt->get_current_level())) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_UNLIKELY(column_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column exprs number is invalid", K(ret), K(*cond));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_simple_filter && i < params.count(); ++i) {
      PushDownParam& view = params.at(i);
      if (cond->get_relation_ids().overlap2(view.table_bit_index_)) {
        is_simple_filter = cond->get_relation_ids().is_subset2(view.table_bit_index_);
        can_push_down = is_simple_filter && !outer_join_tables.overlap2(cond->get_relation_ids());
        if (can_push_down) {
          if (OB_FAIL(view.filter_exprs_.push_back(cond))) {
            LOG_WARN("failed to push back condition expr", K(ret));
          } else if (OB_FAIL(view.correlated_joined_tables_.push_back(NULL))) {
            LOG_WARN("failed to push back correlated joined tables", K(ret));
          }
        } else if (OB_FAIL(
                       add_exprs(column_exprs, stmt->get_current_level(), view.table_bit_index_, view.join_columns_))) {
          LOG_WARN("failed to add owned exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

/// distribute l left join on 'cond' into views
int ObTransformGroupByPlacement::distribute_joined_on_conds(
    ObDMLStmt* stmt, ObIArray<PushDownParam>& params, JoinedTable* joined_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  ObSqlBitSet<> left_table_set;
  ObSqlBitSet<> right_table_set;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(joined_table));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *joined_table->left_table_, left_table_set))) {
    LOG_WARN("failed to get left table set", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *joined_table->right_table_, right_table_set))) {
    LOG_WARN("failed to get right table set", K(ret));
  }
  ObSEArray<ObRawExpr*, 4> filter_conds;
  ObSEArray<ObRawExpr*, 4> join_conds;
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
    ObRawExpr* expr = joined_table->join_conditions_.at(i);
    bool has = false;
    bool is_simple_filter = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (OB_FAIL(has_stmt_column(expr, stmt->get_current_level(), has))) {
      LOG_WARN("failed to check has stmt column", K(ret));
    } else if (!has) {
      // do nothing
    } else if (expr->get_relation_ids().num_members() == 1) {
      // joined on condition can not filter left table output
      if (expr->get_relation_ids().is_superset2(left_table_set)) {
        is_simple_filter = joined_table->joined_type_ == RIGHT_OUTER_JOIN;
      } else if (expr->get_relation_ids().is_superset2(right_table_set)) {
        is_simple_filter = joined_table->joined_type_ == LEFT_OUTER_JOIN;
      }
    }
    if (OB_SUCC(ret)) {
      if (is_simple_filter) {
        if (OB_FAIL(filter_conds.push_back(expr))) {
          LOG_WARN("failed to push back filter", K(ret));
        }
      } else if (OB_FAIL(join_conds.push_back(expr))) {
        LOG_WARN("failed to push back join cond", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(join_conds, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(joined_table->join_conditions_.assign(join_conds))) {
      LOG_WARN("failed to assign join conditions", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    int64_t old_filter_count = params.at(i).filter_exprs_.count();
    if (OB_FAIL(add_exprs(
            column_exprs, stmt->get_current_level(), params.at(i).table_bit_index_, params.at(i).join_columns_))) {
      LOG_WARN("failed to add owned expr", K(ret));
    } else if (OB_FAIL(add_exprs(filter_conds,
                   stmt->get_current_level(),
                   params.at(i).table_bit_index_,
                   params.at(i).filter_exprs_))) {
      LOG_WARN("failed to add owned expr", K(ret));
    }
    int64_t new_filter_count = params.at(i).filter_exprs_.count();
    for (int64_t j = old_filter_count; OB_SUCC(ret) && j < new_filter_count; ++j) {
      if (OB_FAIL(params.at(i).correlated_joined_tables_.push_back(joined_table))) {
        LOG_WARN("failed to push back correlated joined tables", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && joined_table->left_table_->is_joined_table()) {
    if (OB_FAIL(distribute_joined_on_conds(stmt, params, static_cast<JoinedTable*>(joined_table->left_table_)))) {
      LOG_WARN("failed to distribute join condition to view", K(ret));
    }
  }
  if (OB_SUCC(ret) && joined_table->right_table_->is_joined_table()) {
    if (OB_FAIL(distribute_joined_on_conds(stmt, params, static_cast<JoinedTable*>(joined_table->right_table_)))) {
      LOG_WARN("failed to distribute join condition to view", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::transform_groupby_push_down(ObSelectStmt* stmt,
    ObIArray<uint64_t>& flattern_joined_tables, ObSqlBitSet<>& outer_join_tables, ObIArray<PushDownParam>& params)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem*, 4> eager_aggr_tables;
  ObSEArray<bool, 4> table_types;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<TableItem*, 4> table_items;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(append(table_items, stmt->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  }
  // step 1
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObSelectStmt* sub_stmt = NULL;
    TableItem* new_table_item = NULL;
    ObSEArray<ObRawExpr*, 4> view_columns;
    if (params.at(i).table_bit_index_.is_empty()) {
      continue;
    } else if (OB_FAIL(push_down_group_by_into_view(
                   stmt, table_items, flattern_joined_tables, params.at(i), new_table_item))) {
      LOG_WARN("failed to push down group by into view", K(ret));
    } else if (OB_ISNULL(new_table_item) || OB_ISNULL(sub_stmt = new_table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated table item is null", K(ret), K(new_table_item), K(sub_stmt));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *new_table_item, stmt, view_columns))) {
      LOG_WARN("failed to add column exprs for view", K(ret));
    } else if (OB_FAIL(eager_aggr_tables.push_back(new_table_item))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(table_types.push_back(params.at(i).table_bit_index_.num_members() == 1 &&
                                             params.at(i).table_bit_index_.is_subset(outer_join_tables)))) {
      LOG_WARN("failed to push back flags", K(ret));
    } else {
      stmt->get_table_items().pop_back();
      // replace join columns, replace group columns
      for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_select_item_size(); ++i) {
        if (OB_FAIL(old_exprs.push_back(sub_stmt->get_select_item(i).expr_))) {
          LOG_WARN("failed to push back select expr", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(view_columns.at(i)))) {
          LOG_WARN("failed to push back view column", K(ret));
        }
      }
    }
  }
  // step 2
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> origin_aggr_exprs;
    ObSEArray<ObRawExpr*, 4> deduce_aggr_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_aggr_item_size(); ++i) {
      ObRawExpr* deduced_expr = NULL;
      if (OB_FAIL(origin_aggr_exprs.push_back(stmt->get_aggr_item(i)))) {
        LOG_WARN("failed to push back origin aggregation expr", K(ret));
      } else if (OB_FAIL(transform_aggregation_expr(
                     *stmt, *stmt->get_aggr_item(i), eager_aggr_tables, table_types, deduced_expr))) {
        LOG_WARN("failed to transform aggregation expr", K(ret));
      } else if (OB_FAIL(deduce_aggr_exprs.push_back(deduced_expr))) {
        LOG_WARN("failed to push back deduced aggregation expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->replace_inner_stmt_expr(origin_aggr_exprs, deduce_aggr_exprs))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
      }
    }
  }

  // step 3
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_table_items(), eager_aggr_tables))) {
      LOG_WARN("failed to append aggregation tables", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < eager_aggr_tables.count(); ++i) {
    if (OB_FAIL(eager_aggr_tables.at(i)->ref_query_->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(eager_aggr_tables.at(i)->ref_query_->update_column_item_rel_id())) {
      LOG_WARN("failed to update column rel id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hashes", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item relation id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt info", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::push_down_group_by_into_view(ObSelectStmt* stmt,
    const ObIArray<TableItem*>& table_items, ObIArray<uint64_t>& flattern_joined_tables, PushDownParam& params,
    TableItem*& new_table_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sub_stmt = NULL;
  ObSEArray<int64_t, 4> table_index_array;
  ObSEArray<uint64_t, 4> joined_table_ids;
  bool is_added = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(sub_stmt))) {
    LOG_WARN("faile to create stmt", K(ret));
  } else if (OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub stmt is null", K(ret), K(sub_stmt));
  } else if (OB_FAIL(sub_stmt->ObStmt::assign(*stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, sub_stmt, new_table_item))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_FAIL(sub_stmt->get_condition_exprs().assign(params.filter_exprs_))) {
    LOG_WARN("failed to assign filter exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), params.filter_exprs_))) {
    LOG_WARN("failed to remove push down filters", K(ret));
  } else if (OB_FAIL(params.table_bit_index_.to_array(table_index_array))) {
    LOG_WARN("failed to convert bit set to array", K(ret));
  } else {
    sub_stmt->set_current_level(stmt->get_current_level());
    sub_stmt->get_stmt_hint().assign(stmt->get_stmt_hint());
  }
  /// 1. build table and from list
  for (int64_t i = 0; OB_SUCC(ret) && i < table_index_array.count(); ++i) {
    int64_t idx = table_index_array.at(i);
    TableItem* table_item = NULL;
    ObSEArray<ObDMLStmt::PartExprItem, 4> part_exprs;
    FromItem from_item;
    if (idx <= 0 || idx > table_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is invalid", K(ret), K(idx));
    } else if (OB_ISNULL(table_item = table_items.at(idx - 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(table_item));
    } else if (OB_FAIL(sub_stmt->get_table_items().push_back(table_item))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(table_item))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_from_item(stmt, table_item, from_item))) {
      LOG_WARN("failed to from item", K(ret));
    } else if (!from_item.is_joined_) {
      // case 1. for basic table
      if (OB_FAIL(sub_stmt->add_from_item(table_item->table_id_, false))) {
        LOG_WARN("failed to add from item", K(ret));
      } else if (OB_FAIL(stmt->remove_from_item(table_item->table_id_))) {
        LOG_WARN("failed to remove from item", K(ret));
      }
    } else if (ObOptimizerUtil::find_item(flattern_joined_tables, from_item.table_id_)) {
      // case 2. for flattern joined table
      if (OB_FAIL(sub_stmt->add_from_item(table_item->table_id_, false))) {
        LOG_WARN("failed to add from item", K(ret));
      } else if (OB_FAIL(update_joined_table(
                     stmt->get_joined_table(from_item.table_id_), table_item, new_table_item, is_added))) {
        LOG_WARN("failed to update joined table", K(ret));
      } else if (OB_UNLIKELY(!is_added)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to update joined table", K(ret));
      }
    } else if (OB_FAIL(add_var_to_array_no_dup(joined_table_ids, from_item.table_id_))) {
      // case. for joined table
      LOG_WARN("failed to add var to array", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->get_part_expr_items(table_item->table_id_, part_exprs))) {
        LOG_WARN("failed to get part expr items", K(ret));
      } else if (part_exprs.empty()) {
        // do nothing
      } else if (OB_FAIL(sub_stmt->set_part_expr_items(part_exprs))) {
        LOG_WARN("failed to set part expr item", K(ret));
      } else if (OB_FAIL(stmt->remove_part_expr_items(table_item->table_id_))) {
        LOG_WARN("failed to remove part epxr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_added && OB_FAIL(stmt->add_from_item(new_table_item->table_id_, false))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(sub_stmt->adjust_statement_id())) {
      LOG_WARN("failed to adjust stmt id", K(ret));
    }
  }
  /// 2. add joined tables
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table_ids.count(); ++i) {
    uint64_t table_id = joined_table_ids.at(i);
    JoinedTable* table = NULL;
    if (OB_ISNULL(table = stmt->get_joined_table(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is not exists", K(ret), K(table_id));
    } else if (OB_FAIL(sub_stmt->add_joined_table(table))) {
      LOG_WARN("failed to add joined table", K(ret));
    } else if (OB_FAIL(sub_stmt->add_from_item(table_id, true))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_joined_tables(), table))) {
      LOG_WARN("failed to remove joined table", K(ret));
    } else if (OB_FAIL(stmt->remove_from_item(table_id))) {
      LOG_WARN("failed to remove from item", K(ret));
    }
  }
  /// 3. push down columns
  ObSEArray<ColumnItem, 4> new_column_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
    ColumnItem& col_item = stmt->get_column_items().at(i);
    if (OB_ISNULL(col_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (!params.table_bit_index_.is_superset2(col_item.expr_->get_relation_ids())) {
      if (OB_FAIL(new_column_list.push_back(col_item))) {
        LOG_WARN("failed to push bakc column item", K(ret));
      }
    } else if (OB_FAIL(sub_stmt->get_column_items().push_back(col_item))) {
      LOG_WARN("failed to add column item", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_column_items().assign(new_column_list))) {
      LOG_WARN("failed to assign new column list", K(ret));
    } else if (OB_FAIL(append_array_no_dup(sub_stmt->get_group_exprs(), params.join_columns_))) {
      LOG_WARN("failed to append array without duplicate", K(ret));
    } else if (OB_FAIL(append_array_no_dup(sub_stmt->get_group_exprs(), params.group_exprs_))) {
      LOG_WARN("failed to append array wihtout duplicates", K(ret));
    }
  }
  /// 4. build group by
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_group_expr_size(); ++i) {
    if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, sub_stmt->get_group_exprs().at(i), sub_stmt))) {
      LOG_WARN("failed to create select expr", K(ret));
    }
  }
  /// 5. build aggregation
  for (int64_t i = 0; OB_SUCC(ret) && i < params.aggr_exprs_.count(); ++i) {
    ObRawExpr* expr = params.aggr_exprs_.at(i);
    if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_aggr_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else if (OB_FAIL(sub_stmt->add_agg_item(static_cast<ObAggFunRawExpr&>(*expr)))) {
      LOG_WARN("failed to add aggr item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, expr, sub_stmt))) {
      LOG_WARN("failed to add select item", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::update_joined_table(
    TableItem* table, const TableItem* old_table, TableItem* new_table, bool& is_found)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(table) || OB_ISNULL(old_table) || OB_ISNULL(new_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined table is null", K(ret), K(table), K(old_table), K(new_table));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (table->is_joined_table()) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table);
    bool is_contain = false;
    for (int64_t i = 0; i < joined_table->single_table_ids_.count(); ++i) {
      if (joined_table->single_table_ids_.at(i) == old_table->table_id_) {
        joined_table->single_table_ids_.at(i) = new_table->table_id_;
        is_contain = true;
      }
    }
    if (joined_table->left_table_ == old_table) {
      is_found = true;
      joined_table->left_table_ = new_table;
    } else if (joined_table->right_table_ == old_table) {
      is_found = true;
      joined_table->right_table_ = new_table;
    }
    if (!is_contain) {
      // do nothing
    } else if (is_found) {
      if (OB_ISNULL(new_table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new table is expected to be generate table", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(
                     joined_table->join_conditions_, new_table->ref_query_->get_condition_exprs()))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
    } else if (OB_FAIL(update_joined_table(joined_table->left_table_, old_table, new_table, is_found))) {
      LOG_WARN("failed to update joined table", K(ret));
    } else if (is_found) {
      // do nothing
    } else if (OB_FAIL(update_joined_table(joined_table->right_table_, old_table, new_table, is_found))) {
      LOG_WARN("failed to update joined table", K(ret));
    }
  }
  return ret;
}

/// use eager aggreagtion result to deduce origin aggregation expr
int ObTransformGroupByPlacement::transform_aggregation_expr(ObDMLStmt& stmt, ObAggFunRawExpr& aggr_expr,
    ObIArray<TableItem*>& eager_aggr_views, ObIArray<bool>& table_types, ObRawExpr*& new_aggr_expr)
{
  int ret = OB_SUCCESS;
  ObItemType aggr_type = aggr_expr.get_expr_type();
  ObItemType group_aggr_type = (aggr_type == T_FUN_COUNT ? T_FUN_COUNT_SUM : aggr_type);
  ObSEArray<ObRawExpr*, 4> mul_params;
  ObRawExpr* aggr_column = NULL;
  bool need_mul_count = (group_aggr_type == T_FUN_SUM || group_aggr_type == T_FUN_COUNT_SUM);
  new_aggr_expr = NULL;
  if (OB_UNLIKELY(
          aggr_type != T_FUN_MAX && aggr_type != T_FUN_MIN && aggr_type != T_FUN_SUM && aggr_type != T_FUN_COUNT) ||
      OB_UNLIKELY(table_types.count() != eager_aggr_views.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregation type is invalid", K(ret), K(aggr_type), K(table_types.count()), K(eager_aggr_views.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < eager_aggr_views.count(); ++i) {
    ObRawExpr* view_column = NULL;
    if (OB_FAIL(get_view_column(stmt, eager_aggr_views.at(i), table_types.at(i), &aggr_expr, view_column))) {
      LOG_WARN("failed to get aggregation column", K(ret));
    } else if (OB_NOT_NULL(view_column)) {
      aggr_column = view_column;
    } else if (!need_mul_count) {
      // do nothing
    } else if (OB_FAIL(get_count_star(stmt, eager_aggr_views.at(i), table_types.at(i), view_column))) {
      LOG_WARN("failed to get view count", K(ret));
    } else if (OB_ISNULL(view_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected count star from the view", K(ret));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(view_column)) {
      if (OB_FAIL(mul_params.push_back(view_column))) {
        LOG_WARN("failed to push back count column", K(ret));
      } else if (!need_mul_count) {
        break;
      }
    }
  }
  LOG_TRACE("transform aggregation", K(mul_params), K(aggr_column));
  if (OB_SUCC(ret) && OB_ISNULL(aggr_column)) {
    // the aggregation expr is not pushed into eager view
    if (OB_FAIL(convert_aggr_expr(&stmt, &aggr_expr, aggr_column))) {
      LOG_WARN("failed to convert aggr expr to plain expr", K(ret));
    } else if (OB_ISNULL(aggr_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregation column is null", K(ret));
    } else if (aggr_column == &aggr_expr) {
      // do nothing for count star
    } else if (OB_FAIL(mul_params.push_back(aggr_column))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mul_params.count(); ++i) {
    if (0 == i) {
      new_aggr_expr = mul_params.at(i);
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                   ctx_->session_info_,
                   T_OP_MUL,
                   new_aggr_expr,
                   new_aggr_expr,
                   mul_params.at(i)))) {
      LOG_WARN("failed to create multiple expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObAggFunRawExpr* group_aggr = NULL;
    if (OB_ISNULL(new_aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new aggregation expr is null", K(ret), K(new_aggr_expr), K(mul_params));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr<ObAggFunRawExpr>(group_aggr_type, group_aggr))) {
      LOG_WARN("failed to create new aggregation expr", K(ret));
    } else if (OB_ISNULL(group_aggr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the copied aggregation expr is null", K(ret), K(group_aggr));
    } else {
      group_aggr->add_real_param_expr(new_aggr_expr);
      group_aggr->set_expr_level(stmt.get_current_level());
      new_aggr_expr = group_aggr;
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::convert_aggr_expr(ObDMLStmt* stmt, ObAggFunRawExpr* aggr_expr, ObRawExpr*& output_expr)
{
  int ret = OB_SUCCESS;
  // for sum
  bool is_nullable = false;
  ObConstRawExpr* value_one = NULL;
  ObConstRawExpr* value_zero = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (aggr_expr->get_expr_type() == T_FUN_MAX || aggr_expr->get_expr_type() == T_FUN_MIN ||
             aggr_expr->get_expr_type() == T_FUN_SUM) {
    output_expr = aggr_expr->get_param_expr(0);
  } else if (aggr_expr->get_expr_type() != T_FUN_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid aggregation type", K(ret));
  } else if (aggr_expr->get_param_count() == 0) {
    // Note 1: actually output expr should be "expr 1L"
    // we do not create the dummy const expr, instead we let
    // the caller to handle this case, which will ignore the expr
    // in deduce the aggreagtion result
    output_expr = aggr_expr;
  } else if (OB_FAIL(
                 ObTransformUtils::check_expr_nullable(stmt, aggr_expr->get_real_param_exprs().at(0), is_nullable))) {
    LOG_WARN("failed to check expr nullable", K(ret));
  } else if (!is_nullable) {
    // See Note 1
    output_expr = aggr_expr;
  } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 1L, value_one))) {
    LOG_WARN("failed to create const int expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 0L, value_zero))) {
    LOG_WARN("failed to create const int expr", K(ret));
  } else if (OB_FAIL(
                 build_case_when(stmt, aggr_expr->get_real_param_exprs().at(0), value_one, value_zero, output_expr))) {
    LOG_WARN("failed to build case when", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::build_case_when(
    ObDMLStmt* stmt, ObRawExpr* when_expr, ObRawExpr* then_expr, ObRawExpr* else_expr, ObRawExpr*& case_when)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr* case_expr = NULL;
  ObOpRawExpr* is_not_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_CASE, case_expr))) {
    LOG_WARN("failed to create case expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, when_expr, is_not_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_FAIL(case_expr->add_when_param_expr(is_not_expr))) {
    LOG_WARN("failed to add when param expr", K(ret));
  } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
    LOG_WARN("failed to add then expr", K(ret));
  } else {
    case_expr->set_default_param_expr(else_expr);
    case_when = case_expr;
    if (OB_FAIL(case_when->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize case when expr", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::has_stmt_column(ObRawExpr* expr, const int64_t stmt_level, bool& bret)
{
  int ret = OB_SUCCESS;
  bret = true;
  ObRawExpr* param_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!expr->is_aggr_expr()) {
    bret = expr->get_expr_levels().has_member(stmt_level);
  } else if (expr->get_param_count() == 0) {
    bret = false;
  } else if (OB_ISNULL(param_expr = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret));
  } else {
    bret = param_expr->get_expr_levels().has_member(stmt_level);
  }
  return ret;
}

int ObTransformGroupByPlacement::get_view_column(
    ObDMLStmt& stmt, TableItem* table_item, bool is_outer_join_table, ObRawExpr* aggr_expr, ObRawExpr*& aggr_column)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  uint64_t column_id = OB_INVALID_ID;
  aggr_column = NULL;
  if (OB_ISNULL(table_item) || OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(table_item), K(aggr_expr));
  } else if (OB_ISNULL(select_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(*table_item));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
    if (select_stmt->get_select_item(i).expr_ == aggr_expr) {
      column_id = OB_APP_MIN_COLUMN_ID + i;
      break;
    }
  }
  if (OB_SUCC(ret) && column_id != OB_INVALID_ID) {
    ObColumnRefRawExpr* col_expr = NULL;
    if (OB_ISNULL(col_expr = stmt.get_column_expr_by_id(table_item->table_id_, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column item", K(*table_item), K(column_id));
    } else if (aggr_expr->get_expr_type() != T_FUN_COUNT || !is_outer_join_table) {
      aggr_column = col_expr;
    } else if (OB_FAIL(wrap_case_when_for_count(&stmt, col_expr, aggr_column))) {
      LOG_WARN("failed to convert outer join count", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::get_count_star(
    ObDMLStmt& stmt, TableItem* table_item, bool is_outer_join_table, ObRawExpr*& count_column)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  ObRawExpr* last_select_expr = NULL;
  ObColumnRefRawExpr* col_expr = NULL;
  int64_t N = -1;
  count_column = NULL;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(table_item));
  } else if (!table_item->is_generated_table() || OB_ISNULL(select_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(*table_item));
  } else if (0 > (N = select_stmt->get_select_item_size() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item size is invalid", K(ret));
  } else if (OB_ISNULL(last_select_expr = select_stmt->get_select_item(N).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr is null", K(ret));
  } else if (last_select_expr->get_expr_type() != T_FUN_COUNT || last_select_expr->get_param_count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last select expr is not count(*)", K(ret));
  } else if (OB_ISNULL(col_expr = stmt.get_column_expr_by_id(table_item->table_id_, OB_APP_MIN_COLUMN_ID + N))) {
    LOG_WARN("failed to get column expr", K(*table_item));
  } else if (!is_outer_join_table) {
    count_column = col_expr;
  } else if (OB_FAIL(wrap_case_when_for_count(&stmt, col_expr, count_column, true))) {
    LOG_WARN("failed to convert count star", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::wrap_case_when_for_count(
    ObDMLStmt* stmt, ObColumnRefRawExpr* view_count, ObRawExpr*& output, bool is_count_star /*= false*/)
{
  int ret = OB_SUCCESS;
  const int64_t const_int_value = (is_count_star ? 1 : 0);
  ObRawExpr* case_when = NULL;
  ObConstRawExpr* int_value = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(view_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(view_count));
  } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, const_int_value, int_value))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(build_case_when(stmt, view_count, view_count, int_value, case_when))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else {
    output = case_when;
  }
  return ret;
}

int ObTransformGroupByPlacement::check_unique(ObSelectStmt* stmt, PushDownParam& param, bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr*, 8> conditions;
  ObSEArray<int64_t, 4> table_indexes;
  ObSEArray<ObRawExpr*, 4> exprs;
  TableItem* table_item = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(param.table_bit_index_.to_array(table_indexes))) {
    LOG_WARN("failed to convert bit set to array", K(ret));
  } else if (table_indexes.count() != 1) {
    // do nothing
  } else if (OB_FAIL(append(exprs, param.join_columns_))) {
    LOG_WARN("failed to append join columns", K(ret));
  } else if (OB_FAIL(append(exprs, param.group_exprs_))) {
    LOG_WARN("failed to append group exprs", K(ret));
  } else if (table_indexes.at(0) <= 0 || table_indexes.at(0) > stmt->get_table_size() ||
             OB_ISNULL(table_item = stmt->get_table_item(table_indexes.at(0) - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table index is invalid", K(ret), K(table_indexes));
  } else if (OB_FAIL(
                 ObTransformUtils::extract_table_exprs(*stmt, stmt->get_condition_exprs(), *table_item, conditions))) {
    LOG_WARN("failed to extract columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(
                 *stmt, table_item, exprs, conditions, ctx_->session_info_, ctx_->schema_checker_, is_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::add_exprs(
    const ObIArray<ObRawExpr*>& exprs, int64_t stmt_level, ObSqlBitSet<>& table_set, ObIArray<ObRawExpr*>& dest)
{
  int ret = OB_SUCCESS;
  bool has = false;
  for (int64_t i = 0; OB_SUCC(ret) && !table_set.is_empty() && i < exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (OB_FAIL(has_stmt_column(expr, stmt_level, has))) {
      LOG_WARN("failed to check has current stmt column", K(ret));
    } else if (!has || !table_set.is_superset2(expr->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(dest, expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::PushDownParam::merge(ObTransformGroupByPlacement::PushDownParam& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_bit_index_.add_members(other.table_bit_index_))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(append(join_columns_, other.join_columns_))) {
    LOG_WARN("failed to append join exprs", K(ret));
  } else if (OB_FAIL(append(group_exprs_, other.group_exprs_))) {
    LOG_WARN("failed to append group exprs", K(ret));
  } else if (OB_FAIL(append(aggr_exprs_, other.aggr_exprs_))) {
    LOG_WARN("failed to append aggr exprs", K(ret));
  } else if (OB_FAIL(append(filter_exprs_, other.filter_exprs_))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (OB_FAIL(append(correlated_joined_tables_, other.correlated_joined_tables_))) {
    LOG_WARN("failed to append correlated joined tables", K(ret));
  } else {
    other.reset();
  }
  return ret;
}

int ObTransformGroupByPlacement::merge_tables(ObIArray<PushDownParam>& params, const ObSqlBitSet<>& table_set)
{
  int ret = OB_SUCCESS;
  int64_t first_index = -1;
  if (table_set.num_members() > 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      if (!params.at(i).table_bit_index_.overlap(table_set)) {
        // do nothing
      } else if (-1 == first_index) {
        first_index = i;
      } else if (OB_FAIL(params.at(first_index).merge(params.at(i)))) {
        LOG_WARN("failed to merge transform params", K(ret));
      }
    }
  }
  return ret;
}

/////////////////////////////////////////////////////////////////
///////////             Group By Pull Up      ///////////////////
/////////////////////////////////////////////////////////////////

int ObTransformGroupByPlacement::transform_groupby_pull_up(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<PullupHelper, 4> valid_views;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(check_groupby_pullup_validity(stmt, valid_views))) {
    LOG_WARN("failed to check group by pullup validity", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < valid_views.count(); ++i) {
    ObDMLStmt* trans_stmt = NULL;
    ObSelectStmt* view_stmt = NULL;
    if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_, stmt, trans_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_FAIL(get_trans_view(trans_stmt, view_stmt))) {
      LOG_WARN("failed to get transform view", K(ret));
    } else if (OB_FAIL(do_groupby_pull_up(view_stmt, valid_views.at(i)))) {
      LOG_WARN("failed to do pull up group by", K(ret));
    } else if (valid_views.at(i).need_merge_) {
      stmt = trans_stmt;
      trans_happened = true;
      cost_based_trans_tried_ = true;
      LOG_TRACE("has merge hint for the rewrite.");
    } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, trans_happened))) {
      LOG_WARN("failed to accept transform", K(ret));
    }
  }
  return ret;
}

// one generated table is valid simple group by
// the others must be has unique keys
int ObTransformGroupByPlacement::check_groupby_pullup_validity(ObDMLStmt* stmt, ObIArray<PullupHelper>& valid_views)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool is_collation_valid = false;
  bool has_for_update = false;
  bool contain_inner_table = false;
  bool has_unique_keys = false;
  ObSqlBitSet<> ignore_tables;
  valid_views.reset();
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(ctx_), K(ret));
  } else if (!stmt->is_sel_del_upd() || stmt->is_set_stmt() || stmt->is_hierarchical_query()) {
    // do nothing
  } else if (stmt->get_from_item_size() == 0) {
    // do nothing
  } else if (stmt->get_from_item_size() == 1 && !stmt->get_from_item(0).is_joined_) {
    // do nothing
  } else if (OB_FAIL(check_collation_validity(*stmt, is_collation_valid))) {
    LOG_WARN("failed to check collation validity", K(ret));
  } else if (!is_collation_valid) {
    // do nothing
  } else if (OB_FAIL(stmt->check_if_contain_select_for_update(has_for_update))) {
    LOG_WARN("failed to check if contain for update", K(ret));
  } else if (has_for_update) {
    // do nothing
  } else if (OB_FAIL(stmt->check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_can_set_stmt_unique(stmt, has_unique_keys))) {
    LOG_WARN("failed to check stmt has unique keys", K(ret));
  } else if (!has_unique_keys) {
  } else {
    is_valid = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_semi_info_size(); ++i) {
    if (OB_ISNULL(stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret), K(stmt->get_semi_infos().at(i)));
    } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(
                   *stmt, stmt->get_semi_infos().at(i)->left_table_ids_, ignore_tables))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_subquery_expr_size(); ++i) {
    if (OB_ISNULL(stmt->get_subquery_exprs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery is null", K(ret));
    } else if (OB_FAIL(ignore_tables.add_members2(stmt->get_subquery_exprs().at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  ObSEArray<ObRawExpr*, 16> on_conditions;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::get_on_conditions(*stmt, on_conditions))) {
    LOG_WARN("failed to get all on conditions", K(ret));
  } else if (OB_FAIL(check_ignore_views(*stmt, on_conditions, ignore_tables))) {
    LOG_WARN("failed to check ignore views", K(ret));
  }
  // check view validity
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_from_item_size(); ++i) {
    const FromItem from_item = stmt->get_from_item(i);
    TableItem* table = NULL;
    PullupHelper helper;
    if (from_item.is_joined_ && OB_ISNULL(table = stmt->get_joined_table(from_item.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table item is null", K(ret));
    } else if (!from_item.is_joined_ && OB_ISNULL(table = stmt->get_table_item(from_item))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(check_groupby_pullup_validity(
                   stmt, table, helper, contain_inner_table, ignore_tables, valid_views, is_valid))) {
      LOG_WARN("failed to check group by pull up validity", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::check_groupby_pullup_validity(ObDMLStmt* stmt, TableItem* table, PullupHelper& helper,
    bool contain_inner_table, ObSqlBitSet<>& ignore_tables, ObIArray<PullupHelper>& valid_views, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool can_pullup = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(table), K(ret));
  } else if (table->is_basic_table()) {
    // do nothing
  } else if (table->is_generated_table()) {
    bool is_valid_group = false;
    ObSelectStmt* sub_stmt = NULL;
    if (OB_ISNULL(sub_stmt = table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated table item", K(ret), K(*table));
    } else if (ignore_tables.has_member(stmt->get_table_bit_index(table->table_id_))) {
      // skip the generated table
    } else if (contain_inner_table && !sub_stmt->get_stmt_hint().enable_view_merge()) {
      // do not rewrite inner table stmt with a cost-based rule
    } else if (OB_FAIL(is_valid_group_stmt(sub_stmt, is_valid_group))) {
      LOG_WARN("failed to check is valid group stmt", K(ret));
    } else if (!is_valid_group) {
      // do nothing
    } else if (helper.need_check_having_ && sub_stmt->get_having_expr_size() > 0) {
      // do nothing
    } else if (OB_FALSE_IT(helper.table_id_ = table->table_id_)) {
    } else if (OB_FAIL(check_null_propagate(stmt, sub_stmt, helper, can_pullup))) {
      LOG_WARN("failed to check null propagate select expr", K(ret));
    } else if (!can_pullup) {
      // do nothing
    } else if (OB_FALSE_IT(helper.need_merge_ = sub_stmt->get_stmt_hint().enable_view_merge())) {
    } else if (OB_FAIL(valid_views.push_back(helper))) {
      LOG_WARN("failed to push back group stmt index", K(ret));
    } else {
      // do nothing
    }
  } else if (table->is_joined_table()) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table);
    PullupHelper left_helper = helper;
    PullupHelper right_helper = helper;
    left_helper.parent_table_ = joined_table;
    right_helper.parent_table_ = joined_table;
    if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      right_helper.need_check_having_ = true;
      right_helper.need_check_null_propagate_ = true;
    } else if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      left_helper.need_check_having_ = true;
      left_helper.need_check_null_propagate_ = true;
    } else if (INNER_JOIN == joined_table->joined_type_) {
    } else if (FULL_OUTER_JOIN == joined_table->joined_type_) {
      if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joined table has null child table", K(ret));
      } else if (!joined_table->left_table_->is_basic_table() && !joined_table->right_table_->is_basic_table()) {
        is_valid = false;
      } else {
        left_helper.need_check_having_ = true;
        left_helper.need_check_null_propagate_ = true;
        right_helper.need_check_having_ = true;
        right_helper.need_check_null_propagate_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(check_groupby_pullup_validity(stmt,
                   joined_table->left_table_,
                   left_helper,
                   contain_inner_table,
                   ignore_tables,
                   valid_views,
                   is_valid)))) {
      LOG_WARN("failed to check group by pull up validity", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_groupby_pullup_validity(stmt,
                   joined_table->right_table_,
                   right_helper,
                   contain_inner_table,
                   ignore_tables,
                   valid_views,
                   is_valid)))) {
      LOG_WARN("failed to check group by pull up validity", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::check_ignore_views(
    ObDMLStmt& stmt, ObIArray<ObRawExpr*>& conditions, ObSqlBitSet<>& ignore_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt*, 2> ignore_stmts;
  ObSEArray<ObRawExpr*, 16> columns;
  ObSqlBitSet<> check_table_set;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    TableItem* table = stmt.get_table_item(i);
    int64_t idx = OB_INVALID;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!table->is_generated_table()) {
      // do nothing
    } else if (OB_FALSE_IT(idx = stmt.get_table_bit_index(table->table_id_))) {
      // do nothing
    } else if (ignore_tables.has_member(idx)) {
      // do nothing
    } else if (OB_FAIL(check_table_set.add_member(idx))) {
      LOG_WARN("failed to add table index", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
    ObRawExpr* expr = conditions.at(i);
    if (OB_FAIL(ObTransformUtils::extract_column_exprs(
            expr, stmt.get_current_level(), check_table_set, ignore_stmts, columns))) {
      LOG_WARN("failed to extract column expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    int64_t idx = OB_INVALID;
    TableItem* table = NULL;
    ObRawExpr* expr = columns.at(i);
    ObColumnRefRawExpr* column_expr = NULL;
    bool is_contain_agg = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect column ref expr", K(ret));
    } else if (OB_FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
      // do nothing
    } else if (OB_ISNULL(table = stmt.get_table_item_by_id(column_expr->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FALSE_IT(idx = stmt.get_table_bit_index(table->table_id_))) {
      // do nothing
    } else if (ignore_tables.has_member(idx)) {
      // do nothing
    } else if (OB_FAIL(is_contain_aggr_item(column_expr, table, is_contain_agg))) {
      LOG_WARN("failed to check contain aggr item", K(ret));
    } else if (!is_contain_agg) {
      // do nothing
    } else if (OB_FAIL(ignore_tables.add_member(idx))) {
      LOG_WARN("failed to add table index", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::is_contain_aggr_item(
    ObColumnRefRawExpr* column_expr, TableItem* view_table, bool& is_contain)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* subquery = NULL;
  is_contain = false;
  if (OB_ISNULL(column_expr) || OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(column_expr), K(view_table), K(ret));
  } else if (!view_table->is_generated_table()) {
    // do nothing
  } else if (OB_ISNULL(subquery = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    uint64_t column_id = column_expr->get_column_id();
    uint64_t sel_idx = column_id - OB_APP_MIN_COLUMN_ID;
    ObRawExpr* select_expr = NULL;
    if (sel_idx < 0 || sel_idx >= subquery->get_select_item_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select index is incorrect", K(sel_idx), K(ret));
    } else if (OB_ISNULL(select_expr = subquery->get_select_item(sel_idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null select expr", K(ret));
    } else if (select_expr->has_flag(CNT_AGG)) {
      is_contain = true;
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::is_valid_group_stmt(ObSelectStmt* sub_stmt, bool& is_valid_group)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  is_valid_group = false;
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub stmt is null", K(ret), K(sub_stmt));
  } else if (sub_stmt->get_stmt_hint().enable_no_view_merge()) {
    is_valid_group = false;
    LOG_TRACE("has no v merge hint.", K(is_valid_group));
  } else if (sub_stmt->get_group_expr_size() <= 0 || sub_stmt->get_aggr_item_size() <= 0 || sub_stmt->has_rollup() ||
             sub_stmt->get_window_func_exprs().count() > 0 || sub_stmt->has_limit() || sub_stmt->has_order_by() ||
             sub_stmt->has_distinct() || sub_stmt->is_hierarchical_query() || sub_stmt->get_semi_infos().count() > 0 ||
             sub_stmt->is_contains_assignment()) {
    is_valid_group = false;
  } else if (OB_FAIL(sub_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check sub stmt has row num", K(ret));
  } else if (has_rownum) {
    is_valid_group = false;
  } else if (OB_FAIL(check_groupby_validity(*sub_stmt, is_valid_group))) {
    LOG_WARN("failed to check is valid group", K(ret));
  }
  return ret;
}

int ObTransformGroupByPlacement::check_null_propagate(
    ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, PullupHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(child_stmt) || OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (helper.need_check_null_propagate_) {
    ObRelIds rel_ids;
    ObSqlBitSet<> from_tables;
    ObColumnRefRawExpr* col_expr = NULL;
    ObSEArray<ObRawExpr*, 4> columns;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    if (OB_FAIL(ObTransformUtils::get_from_tables(*child_stmt, rel_ids))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(from_tables.add_members2(rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(child_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*child_stmt, columns, from_tables, column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else {
      bool find = false;
      ObRawExpr* not_null_column = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !find && i < child_stmt->get_select_item_size(); i++) {
        ObRawExpr* expr = child_stmt->get_select_item(i).expr_;
        bool is_null_propagate = true;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr", K(ret));
        } else if (!expr->has_flag(CNT_AGG)) {
          // do nothing
        } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, column_exprs, is_null_propagate))) {
          LOG_WARN("failed to is null propagate expr", K(ret));
        } else if (!is_null_propagate) {
          find = true;
        } else { /*do nothing*/
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (!find) {
        /*do nothing*/
      } else if (OB_FAIL(find_not_null_column(*parent_stmt, *child_stmt, helper, column_exprs, not_null_column))) {
        LOG_WARN("failed to find not null column", K(ret));
      } else if (OB_ISNULL(not_null_column)) {
        is_valid = false;
        LOG_TRACE("can not find not null column");
      } else if (!not_null_column->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is not column expr", K(ret));
      } else {
        col_expr = static_cast<ObColumnRefRawExpr*>(not_null_column);
        helper.not_null_column_table_id_ = col_expr->get_table_id();
        helper.not_null_column_id_ = col_expr->get_column_id();
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformGroupByPlacement::find_not_null_column(ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt,
    PullupHelper& helper, ObIArray<ObRawExpr*>& column_exprs, ObRawExpr*& not_null_column)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  not_null_column = NULL;
  if (OB_FAIL(find_not_null_column_with_condition(parent_stmt, child_stmt, helper, column_exprs, not_null_column))) {
    LOG_WARN("failed to find not null column with join condition", K(ret));
  } else if (OB_NOT_NULL(not_null_column)) {
    // find not null column, do nothing
  } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(child_stmt, not_null_column, is_valid))) {
    LOG_WARN("failed to find not null expr", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformGroupByPlacement::find_not_null_column_with_condition(ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt,
    PullupHelper& helper, ObIArray<ObRawExpr*>& column_exprs, ObRawExpr*& not_null_column)
{
  int ret = OB_SUCCESS;
  not_null_column = NULL;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr*, 16> temp_exprs;
  if (OB_ISNULL(helper.parent_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(join_conditions.assign(helper.parent_table_->join_conditions_))) {
    LOG_WARN("failed to assign join conditions");
  } else if (OB_FAIL(parent_stmt.get_column_exprs(helper.table_id_, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                 old_column_exprs, child_stmt, new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < old_column_exprs.count(); ++i) {
      bool has_null_reject = false;
      if (OB_FAIL(
              ObTransformUtils::has_null_reject_condition(join_conditions, old_column_exprs.at(i), has_null_reject))) {
        LOG_WARN("failed to check has null reject condition", K(ret));
      } else if (!has_null_reject) {
        // do nothing
      } else if (OB_FAIL(find_null_propagate_column(new_column_exprs.at(i), column_exprs, not_null_column, find))) {
        LOG_WARN("failed to find null propagate column", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::find_null_propagate_column(
    ObRawExpr* condition, ObIArray<ObRawExpr*>& columns, ObRawExpr*& null_propagate_column, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  null_propagate_column = NULL;
  bool is_null_propagate = false;
  ObSEArray<const ObRawExpr*, 4> dummy_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < columns.count(); ++i) {
    dummy_exprs.reuse();
    if (OB_FAIL(dummy_exprs.push_back(columns.at(i)))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(condition, dummy_exprs, is_null_propagate))) {
      LOG_WARN("failed to check null propagate expr", K(ret));
    } else if (!is_null_propagate) {
      // do nothing
    } else {
      null_propagate_column = columns.at(i);
      is_valid = true;
    }
  }
  return ret;
}

/**
 * @brief ObTransformGroupByPlacement::get_trans_view
 *  get/create a select stmt for transformation
 * @return
 */
int ObTransformGroupByPlacement::get_trans_view(ObDMLStmt* stmt, ObSelectStmt*& view_stmt)
{
  int ret = OB_SUCCESS;
  bool need_create_view = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (stmt->is_delete_stmt() || stmt->is_update_stmt()) {
    need_create_view = true;
  } else if (stmt->is_select_stmt()) {
    bool has_rownum = false;
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(sel_stmt->has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (has_rownum) {
      need_create_view = true;
    } else if (sel_stmt->has_sequence()) {
      // actually we can directly rewrite the sel_stmt;
      // however, the result stmt is invalid
      need_create_view = true;
    } else if (sel_stmt->has_group_by()) {
      need_create_view = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (!need_create_view) {
      view_stmt = static_cast<ObSelectStmt*>(stmt);
    } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, stmt, view_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::do_groupby_pull_up(ObSelectStmt* stmt, PullupHelper& helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> view_columns;
  ObSEArray<ObRawExpr*, 4> stmt_columns;
  ObSEArray<ObRawExpr*, 4> unique_exprs;
  ObSEArray<ObRawExpr*, 4> aggr_column;
  ObSEArray<ObRawExpr*, 4> aggr_select;
  ObSEArray<ObRawExpr*, 4> pullup_exprs;
  TableItem* table_item = NULL;
  ObSelectStmt* subquery = NULL;
  ObSqlBitSet<> ignore_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(helper.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_ISNULL(subquery = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(*table_item), K(ret));
  } else if (OB_FAIL(ignore_tables.add_member(stmt->get_table_bit_index(table_item->table_id_)))) {
    LOG_WARN("failed to add ignore table index", K(ret));
  } else if (OB_FAIL(ObTransformUtils::generate_unique_key(ctx_, stmt, ignore_tables, unique_exprs))) {
    LOG_WARN("failed to generated unique keys", K(ret));
  } else if (OB_FAIL(append(stmt->get_group_exprs(), unique_exprs))) {
    LOG_WARN("failed to append group exprs", K(ret));
  }
  ObSqlBitSet<> removed_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
    ObRawExpr* select_expr = subquery->get_select_item(i).expr_;
    ObColumnRefRawExpr* col_expr = NULL;
    int64_t column_id = OB_INVALID;
    if (OB_ISNULL(select_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null select expr", K(ret));
    } else if (!select_expr->has_flag(CNT_AGG)) {
      // do nothing
    } else if (OB_FALSE_IT(column_id = i + OB_APP_MIN_COLUMN_ID)) {
    } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_item->table_id_, column_id))) {
      if (OB_FAIL(removed_idx.add_member(i))) {
        LOG_WARN("failed to add remove idx", K(ret));
      }
    } else if (OB_FAIL(aggr_select.push_back(select_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    } else if (OB_FAIL(aggr_column.push_back(col_expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else if (OB_FAIL(removed_idx.add_member(i))) {
      LOG_WARN("failed to add remove idx", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(wrap_case_when_if_necessary(*subquery, helper, aggr_select))) {
      LOG_WARN("failed to wrap case when", K(ret));
    } else if (OB_FAIL(append(pullup_exprs, aggr_select))) {
      LOG_WARN("failed to append pullup exprs", K(ret));
    } else if (OB_FAIL(append(pullup_exprs, subquery->get_group_exprs()))) {
      LOG_WARN("failed to append pullup exprs", K(ret));
    } else if (OB_FAIL(append(pullup_exprs, subquery->get_aggr_items()))) {
      LOG_WARN("failed to append pullup exprs", K(ret));
    } else if (OB_FAIL(append(pullup_exprs, subquery->get_having_exprs()))) {
      LOG_WARN("failed to append pullup exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pullup_exprs, view_columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(
                   ObTransformUtils::create_columns_for_view(ctx_, *table_item, stmt, view_columns, stmt_columns))) {
      LOG_WARN("failed to create view columns", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::replace_exprs(view_columns, stmt_columns, aggr_select))) {
      LOG_WARN("failed to replace group by exprs", K(ret));
    } else if (OB_FAIL(stmt->replace_inner_stmt_expr(aggr_column, aggr_select))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_group_exprs(), subquery->get_group_exprs()))) {
      LOG_WARN("failed to append group by exprs", K(ret));
    } else if (OB_FALSE_IT(subquery->get_group_exprs().reset())) {
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(view_columns, stmt_columns, stmt->get_group_exprs()))) {
      LOG_WARN("failed to replace group by exprs", K(ret));
    } else if (OB_FAIL(append(stmt->get_aggr_items(), subquery->get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (OB_FALSE_IT(subquery->get_aggr_items().reset())) {
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(view_columns, stmt_columns, stmt->get_aggr_items()))) {
      LOG_WARN("failed to replace aggr item exprs", K(ret));
    } else if (OB_FAIL(append(stmt->get_having_exprs(), subquery->get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    } else if (OB_FALSE_IT(subquery->get_having_exprs().reset())) {
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(view_columns, stmt_columns, stmt->get_having_exprs()))) {
      LOG_WARN("failed to replace having exprs", K(ret));
    } else if (OB_FAIL(
                   ObTransformUtils::remove_select_items(ctx_, table_item->table_id_, *subquery, *stmt, removed_idx))) {
      LOG_WARN("failed to remove select items", K(ret));
    } else if (OB_FAIL(subquery->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(stmt->adjust_subquery_stmt_parent(subquery, stmt))) {
      LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      // do nothing
    }
  }
  // classify where conditions
  ObSEArray<ObRawExpr*, 4> new_conds;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    ObRawExpr* cond = NULL;
    if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond->has_flag(CNT_AGG)) {
      if (OB_FAIL(stmt->add_having_expr(cond))) {
        LOG_WARN("failed to add having condition", K(ret));
      }
    } else if (OB_FAIL(new_conds.push_back(cond))) {
      LOG_WARN("failed to push back new condition exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_condition_exprs().assign(new_conds))) {
      LOG_WARN("failed to assign where conditions", K(ret));
    } else {
      LOG_TRACE("group pull up stmt", K(*stmt));
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::wrap_case_when_if_necessary(
    ObSelectStmt& child_stmt, PullupHelper& helper, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObRelIds rel_ids;
  ObSqlBitSet<> from_tables;
  ObRawExpr* not_null_column = NULL;
  ObSEArray<ObRawExpr*, 4> columns;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (helper.not_null_column_id_ == OB_INVALID) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::get_from_tables(child_stmt, rel_ids))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(from_tables.add_members2(rel_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(child_stmt.get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(child_stmt, columns, from_tables, column_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_ISNULL(not_null_column = child_stmt.get_column_expr_by_id(
                           helper.not_null_column_table_id_, helper.not_null_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not find column", K(helper.not_null_column_table_id_), K(helper.not_null_column_id_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      bool is_null_propagate = false;
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(exprs.at(i), column_exprs, is_null_propagate))) {
        LOG_WARN("failed to is null propagate expr", K(ret));
      } else if (is_null_propagate) {
        // do nothing
      } else if (OB_FAIL(wrap_case_when(child_stmt, not_null_column, exprs.at(i)))) {
        LOG_WARN("failed to wrap case when", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTransformGroupByPlacement::wrap_case_when(ObSelectStmt& child_stmt, ObRawExpr* not_null_column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ctx_), K(ret));
  } else if (OB_ISNULL(not_null_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null column expr", K(ret));
  } else {
    ObRawExpr* null_expr = NULL;
    ObRawExpr* case_when_expr = NULL;
    ObRawExprFactory* factory = ctx_->expr_factory_;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*factory, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(
                   child_stmt, not_null_column, expr, null_expr, case_when_expr, ctx_))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else {
      expr = case_when_expr;
    }
  }
  return ret;
}

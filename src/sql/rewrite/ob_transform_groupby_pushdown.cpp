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
#include "ob_transform_groupby_pushdown.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformGroupByPushdown::transform_one_stmt
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
int ObTransformGroupByPushdown::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                             ObDMLStmt *&stmt,
                                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<PushDownParam, 4> params;
  ObSEArray<ObSEArray<TableItem *, 4>, 4> trans_tables;
  ObSEArray<uint64_t, 4> flattern_joined_tables;
  ObSelectStmt *trans_stmt = NULL;
  ObCostBasedPushDownCtx push_down_ctx;
  push_down_ctx.stmt_id_ = stmt->get_stmt_id(); //after deep copy stmt id is still the same.
  const ObGroupByPlacementHint *myhint = 
              static_cast<const ObGroupByPlacementHint*>(stmt->get_stmt_hint().get_normal_hint(T_PLACE_GROUP_BY));
  bool is_valid = false;
  bool is_happened = false;
  ObTryTransHelper try_trans_helper;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(check_groupby_push_down_validity(
                       static_cast<ObSelectStmt *>(stmt), is_valid))) {
    LOG_WARN("failed to check group by push down validity", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("push down is not valid");
  } else if (OB_FAIL(compute_push_down_param(static_cast<ObSelectStmt *>(stmt),
                                             params,
                                             flattern_joined_tables,
                                             is_valid))) {
    LOG_WARN("failed to compute push down param", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("param is not valid");
  } else if (OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(do_groupby_push_down(static_cast<ObSelectStmt *>(stmt),
                                          params,
                                          flattern_joined_tables,
                                          trans_stmt,
                                          push_down_ctx,
                                          is_happened))) {
    LOG_WARN("failed to transform stmt", K(ret));
  } else if (!is_happened) {
    LOG_TRACE("is happened");
  } else if (OB_FAIL(get_tables_from_params(*stmt, params, trans_tables))) {
    LOG_WARN("get tables failed", K(ret));
  } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                      NULL != myhint && myhint->is_enable_hint(), false,
                                      trans_happened, &push_down_ctx))) {
    LOG_WARN("failed to accept transform", K(ret));
  } else if (!trans_happened) {
    //do nothing
  } else if (OB_FAIL(add_transform_hint(*stmt, &trans_tables))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else if (OB_FAIL(ctx_->groupby_pushdown_stmts_.push_back(stmt->get_stmt_id()))) {
    LOG_WARN("failed to add stmt id", K(ret));
  }

  if (OB_SUCC(ret) && !trans_happened && try_trans_helper.is_filled()
      && OB_FAIL(try_trans_helper.recover(stmt->get_query_ctx()))) {
    LOG_WARN("failed to recover params", K(ret));
  }
  return ret;
}

int ObTransformGroupByPushdown::get_tables_from_params(ObDMLStmt &stmt,
                                                       ObIArray<PushDownParam> &params, 
                                                       ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                                       bool disassemble_join /*true*/)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    PushDownParam &param= params.at(i);
    ObSEArray<TableItem *, 4> table_items;
    ObSEArray<int64_t, 4> table_indexes;
    if (param.table_bit_index_.is_empty()) {
      //do nothing
    } else if (OB_FAIL(param.table_bit_index_.to_array(table_indexes))) {
      LOG_WARN("sqlbits to arrary failed", K(ret));
    } else {
      LOG_TRACE("show table index", K(table_indexes));
      for (int64_t j = 0; OB_SUCC(ret) && j < table_indexes.count(); ++j) {
        TableItem *table_item = NULL;
        if (table_indexes.at(j) <= 0 || table_indexes.at(j) > stmt.get_table_size() ||
                OB_ISNULL(table_item = stmt.get_table_item(table_indexes.at(j) - 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table index is invalid", K(ret), K(table_indexes));
        } else if (disassemble_join) {
          if (OB_FAIL(ObTransformUtils::construct_trans_table(&stmt, table_item, table_items))) {
            LOG_WARN("construct tans tables faield", K(ret));
          }
        } else if (OB_FAIL(table_items.push_back(table_item))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans_tables.push_back(table_items))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~(1 << transformer_type_));
  }
  return ret;
}

int ObTransformGroupByPushdown::check_groupby_push_down_validity(ObSelectStmt *stmt,
                                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rand = false;
  bool contain_inner_table = false;
  bool contain_lateral_table = false;
  is_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  } else if (contain_inner_table && !stmt->get_stmt_hint().has_enable_hint(T_PLACE_GROUP_BY)) {
    is_valid = false;
    OPT_TRACE("do not rewrite inner table stmt with cost-based rule");
    // do not rewrite inner table stmt with cost-based rule
  } else if (stmt->has_window_function() ||
             stmt->has_rollup() ||
             stmt->get_semi_infos().count() > 0 ||
             stmt->get_subquery_exprs().count() > 0 ||
             stmt->get_aggr_item_size() <= 0 ||
             stmt->get_group_exprs().empty() ||
             stmt->is_hierarchical_query() ||
             stmt->is_set_stmt()) {
    // do not rewrite scalar group by
    // select count(*) from t1, t2 where t1.c1 = t2.c1;
    // select sum(t1_cnt * t2_cnt) from (select c1, count(*) from t1 group by c1),
    //                                  (select c1, count(*) from t2 group by c1);
    //                                  where t1.c1 = t2.c1;
    is_valid = false;
    LOG_TRACE("invalid stmt for eager aggregation", K(is_valid));
    OPT_TRACE("invalid stmt for eager aggregation");
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    OPT_TRACE("stmt contain rownum, can not transform");
  } else if (OB_FAIL(stmt->has_rand(has_rand))) {
    LOG_WARN("failed to check stmt has rand", K(ret));
  } else if (has_rand) {
    is_valid = false;
    OPT_TRACE("stmt has rand expr, can not transform");
  } else if (OB_FAIL(check_groupby_validity(*stmt, is_valid))) {
    LOG_WARN("failed to check group by validity", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("not a valid group stmt");
  } else if (OB_FAIL(check_collation_validity(*stmt, is_valid))) {
    LOG_WARN("failed to check collation validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::check_contain_correlated_lateral_table(stmt,
                                                                              contain_lateral_table))) {
    LOG_WARN("failed to check contain correlated lateral table", K(ret));
  } else if (contain_lateral_table) {
    is_valid = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
    ObAggFunRawExpr *aggr_expr = NULL;
    if (OB_ISNULL(aggr_expr = stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params are invalid", K(ret), K(aggr_expr));
    } else if ((aggr_expr->get_expr_type() != T_FUN_SUM &&
                aggr_expr->get_expr_type() != T_FUN_COUNT &&
                aggr_expr->get_expr_type() != T_FUN_MIN &&
                aggr_expr->get_expr_type() != T_FUN_MAX) ||
               aggr_expr->is_param_distinct()) {
      is_valid = false;
      OPT_TRACE("invalid aggregation type for group by placement", aggr_expr);
      LOG_TRACE("invalid aggregation type for group by placement", K(is_valid),
                K(aggr_expr->get_expr_type()), K(aggr_expr->is_param_distinct()));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::check_groupby_validity(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObRawExpr *, 4> exprs;
  if (OB_FAIL(stmt.get_select_exprs(exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(stmt.get_order_exprs(exprs))) {
    LOG_WARN("failed to get order exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < exprs.count(); i++) {
    if (OB_FAIL(check_group_by_subset(exprs.at(i), stmt.get_group_exprs(), is_valid))) {
      LOG_WARN("check group by exprs failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_having_exprs().count(); i++) {
    if (OB_FAIL(check_group_by_subset(stmt.get_having_exprs().at(i), stmt.get_group_exprs(), is_valid))) {
      LOG_WARN("check group by exprs failed", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::check_group_by_subset(ObRawExpr *expr, 
                                                       const ObIArray<ObRawExpr *> &group_exprs, 
                                                       bool &bret)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    bret = true;
    int64_t idx = -1;
    if (expr->has_flag(IS_AGG) || expr->has_flag(IS_CONST)) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::get_expr_idx(group_exprs, expr, idx))) {
      LOG_WARN("get expr idx failed", K(ret));
    } else if (idx == -1) { //not found
      if (expr->get_param_count() == 0) {
        bret = false;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr->get_param_count(); i++) {
          if (OB_FAIL(SMART_CALL(check_group_by_subset(expr->get_param_expr(i), group_exprs, bret)))) {
            LOG_WARN("check group by subset faield", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::check_collation_validity(const ObDMLStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_str = false;
  ObRawExpr *col_expr = NULL;
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


/// 根据 group, aggregation exprs 决定 group by 可以 push 到哪些 view 上
/// 如果最后计算出来所有的 table 都要放到一个 view 里面，那说明没办法做 push down
int ObTransformGroupByPushdown::compute_push_down_param(ObSelectStmt *stmt,
                                                         ObIArray<PushDownParam> &params,
                                                         ObIArray<uint64_t> &flattern_joined_tables,
                                                         bool &is_valid)
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
    ObRawExpr *aggr_item = NULL;
    ObSqlBitSet<> table_set;
    if (OB_ISNULL(aggr_item = stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr item is null", K(ret), K(aggr_item));
    } else if (OB_FAIL(table_set.add_members2(aggr_item->get_relation_ids()))) {
      LOG_WARN("failed to add table indexes", K(ret));
    } else if (OB_FAIL(merge_tables(params, table_set))) {
      LOG_WARN("failed to merge tables", K(ret));
    }
  }

  /// 2. merge tables according to filterable join conditions
  if (OB_SUCC(ret) && is_valid) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      bool need_merge = false;
      bool is_valid_filter = false;
      ObRawExpr *cond = NULL;
      ObSqlBitSet<> table_set;
      if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret), K(cond));
      } else if (cond->get_relation_ids().num_members() <= 1) {
        // do nothing
      } else if (OB_FAIL(is_filterable_join(stmt, cond, params, is_valid_filter))) {
        LOG_WARN("failed to check is filterable join", K(ret));
      } else if (is_valid_filter && stmt->get_table_size() > 2) {
        need_merge = true;
      } else if (OB_FAIL(is_lob_filter(cond, is_valid_filter))) {
        LOG_WARN("failed to check is lob filter", K(ret));
      } else if (is_valid_filter) {
        need_merge = true;
      }

      if (OB_SUCC(ret) && need_merge) {
        if (OB_FAIL(table_set.add_members2(cond->get_relation_ids()))) {
          LOG_WARN("failed to add table indexes", K(ret));
        } else if (OB_FAIL(merge_tables(params, table_set))) {
          LOG_WARN("failed to merge tables", K(ret));
        }
      }
    }
  }

  /// 3. merge tables acccording to joined tables
  /// outer join 不具备结合律，给定一个 joined_table，如果有多个 basic table 被压到了一个 view 里面
  /// 那么我们只能把整个 joined table 压到一个 view 里面
  /// TODO can improve. (a join b) left join (c join d)
  /// (a, b) can be put into the same view
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_joined_tables().count(); ++i) {
    JoinedTable *joined_table = stmt->get_joined_tables().at(i);
    ObSqlBitSet<> table_bit_set;
    bool should_merge = false;
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table, table_bit_set))) {
      LOG_WARN("failed to convert table id array to bit set", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !should_merge && j < params.count(); ++j) {
        if (params.at(j).table_bit_index_.overlap(table_bit_set) &&
            params.at(j).table_bit_index_.num_members() >= 2) {
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
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < joined_table->join_conditions_.count(); ++j) {
          ObRawExpr *join_cond = joined_table->join_conditions_.at(j);
          bool has_lob = false;
          if (OB_FAIL(is_lob_filter(join_cond, has_lob))) {
            LOG_WARN("failed to is lob filter", K(ret));
          } else if (has_lob) {
            should_merge = true;
            break;
          }
        }
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
  LOG_TRACE("after push down groupby", K(params));
  if (OB_SUCC(ret)) {
    is_valid = is_valid && get_valid_eager_aggr_num(params) > 1;
  }
  return ret;
}

int ObTransformGroupByPushdown::check_outer_join_aggr(ObSelectStmt *stmt,
                                                       JoinedTable *joined_table,
                                                       bool &is_valid)
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
    ObRawExpr *param = NULL;
    ObSEArray<ObRawExpr *, 4> columns;
    ObSEArray<ObRawExpr *, 4> null_table_columns;
    bool is_valid_aggr = true;
    if (OB_ISNULL(stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr item is null", K(ret));
    } else if (stmt->get_aggr_item(i)->get_param_count() <= 0) {
      // do nothing
    } else if (OB_ISNULL(param = stmt->get_aggr_item(i)->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(param));
    } else if (!param->get_relation_ids().overlap2(null_table_set)) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(param, columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt,
                                                             columns,
                                                             null_table_set,
                                                             null_table_columns))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(param,
                                                                null_table_columns,
                                                                is_valid_aggr))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    } else if (!is_valid_aggr) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::get_null_side_tables(ObDMLStmt &stmt,
                                                      JoinedTable &joined_table,
                                                      ObSqlBitSet<> &table_set)
{
  int ret = OB_SUCCESS;
  TableItem *null_table = NULL;
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
    if (OB_FAIL(stmt.get_table_rel_ids(*null_table, table_set))) {
      LOG_WARN("failed to get table relation ids", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      (joined_table.is_left_join() || joined_table.is_inner_join()) &&
      joined_table.left_table_->is_joined_table()) {
    if (OB_FAIL(get_null_side_tables(stmt,
                                     static_cast<JoinedTable&>(*joined_table.left_table_),
                                     table_set))) {
      LOG_WARN("failed to get null side tables", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      (joined_table.is_right_join() || joined_table.is_inner_join()) &&
      joined_table.right_table_->is_joined_table()) {
    if (OB_FAIL(get_null_side_tables(stmt,
                                     static_cast<JoinedTable&>(*joined_table.right_table_),
                                     table_set))) {
      LOG_WARN("failed to get null side tables", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformGroupByPushdown::is_filterable_join
 * 如果一个 Join 条件对一侧的过滤性非常的强，那么我们应该先做 join，再做 group by
 * 判定的标准
 *   1. Join 条件有一侧是表 A 的 column
 *   2. A 的 column 是某个索引的第一列
 *   3. A 上有 group by 任务
 * @return
 */
int ObTransformGroupByPushdown::is_filterable_join(ObSelectStmt *stmt,
                                                    ObRawExpr *join_cond,
                                                    ObIArray<PushDownParam> &params,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  UNUSED(params);
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(join_cond));
  } else if (join_cond->get_relation_ids().num_members() <= 1) {
    is_valid = false;
  } else if (!(join_cond->get_expr_type() >= T_OP_EQ &&
               join_cond->get_expr_type() <= T_OP_GT)) {
    is_valid = false;
  } else if (OB_ISNULL(left_expr = join_cond->get_param_expr(0)) ||
             OB_ISNULL(right_expr = join_cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are null", K(ret), K(left_expr), K(right_expr));
  } else if (!left_expr->has_flag(CNT_COLUMN) ||
             !right_expr->has_flag(CNT_COLUMN) ||
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

int ObTransformGroupByPushdown::is_lob_filter(ObRawExpr *expr, bool &has)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> column_exprs;
  has = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    if (OB_ISNULL(column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (column_exprs.at(i)->get_result_type().is_lob() ||
               column_exprs.at(i)->get_result_type().is_lob_locator()) {
      has = true;
      break;
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::check_join_expr_validity(ObSelectStmt *stmt,
                                                          ObIArray<PushDownParam> &params,
                                                          ObRawExpr *expr,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  UNUSED(params);
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < stmt->get_aggr_item_size(); ++i) {
      ObAggFunRawExpr *aggr = stmt->get_aggr_item(i);
      ObRawExpr *param_expr = NULL;
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregation expr is null", K(ret), K(aggr));
      } else if (aggr->get_param_count() == 0) {
        is_valid = true;
      } else if (OB_ISNULL(param_expr = aggr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (!param_expr->has_flag(CNT_COLUMN)) {
        // do nothing
      } else {
        is_valid = param_expr->get_relation_ids().overlap(col->get_relation_ids());
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      ObArenaAllocator alloc;
      EqualSets &equal_sets = ctx_->equal_sets_;
      ObSEArray<ObRawExpr *, 4> const_exprs;
      if (OB_FAIL(stmt->get_stmt_equal_sets(equal_sets, alloc, true))) {
        LOG_WARN("failed to get stmt equal sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt->get_condition_exprs(),
                                                              const_exprs))) {
        LOG_WARN("failed to compute const equivalent exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          stmt,
                                                          col,
                                                          is_valid,
                                                          &equal_sets, &const_exprs))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
      equal_sets.reuse();
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::do_groupby_push_down(ObSelectStmt *stmt,
                                                      ObIArray<PushDownParam> &params,
                                                      ObIArray<uint64_t> &flattern_joined_tables,
                                                      ObSelectStmt *&trans_stmt,
                                                      ObCostBasedPushDownCtx &push_down_ctx,
                                                      bool &trans_happend)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> outer_table_set;
  trans_happend = false;
  ObSQLSessionInfo *session_info = NULL;
  bool enable_group_by_placement_transform = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(stmt));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt(trans_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(trans_stmt->deep_copy(*ctx_->stmt_factory_,
                                           *ctx_->expr_factory_,
                                           *stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  }
  /// maintain tables on outer join null side
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    bool on_null_side = false;
    if (OB_ISNULL(stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is invalid", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(
                         stmt, stmt->get_table_item(i)->table_id_, on_null_side))) {
      LOG_WARN("failed to check is table on null side", K(ret));
    } else if (!on_null_side) {
      // do nothing
    } else if (OB_FAIL(outer_table_set.add_member(i + 1))) {
      LOG_WARN("failed to add table index", K(ret));
    }
  }
  /// distribute where filters
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmt->get_condition_size(); ++i) {
    ObRawExpr *cond_expr = trans_stmt->get_condition_expr(i);
    if (OB_FAIL(distribute_filter(
                         trans_stmt, params, outer_table_set, cond_expr))) {
      LOG_WARN("failed to distributed filter to views", K(ret));
    }
  }
  /// distribute outer join on conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < flattern_joined_tables.count(); ++i) {
    uint64_t table_id = flattern_joined_tables.at(i);
    if (OB_FAIL(distribute_joined_on_conds(
                        trans_stmt, params, trans_stmt->get_joined_table(table_id)))) {
      LOG_WARN("failed to distributed joined condition to view", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_valid = true;
    if (OB_FAIL(distribute_group_aggr(trans_stmt, params))) {
      LOG_WARN("faield to distribute expr into view", K(ret));
    } else if (get_valid_eager_aggr_num(params) <= 0) {
      is_valid = false;
    }
    LOG_TRACE("push down params", K(ret));
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < params.count(); i++) {
      if (params.at(i).group_exprs_.empty() &&
          params.at(i).aggr_exprs_.empty() &&
          params.at(i).join_columns_.empty() &&
          params.at(i).filter_exprs_.empty() &&
          params.at(i).correlated_joined_tables_.empty()) {
        if (OB_FALSE_IT(params.remove(i))) {
          LOG_WARN("remove item failed", K(ret));
        } else { i--; }
      }
    }
    LOG_TRACE("push down params", K(ret));
    bool hint_force_pushdown = false;
    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(session_info->is_groupby_placement_transformation_enabled(enable_group_by_placement_transform))) {
      LOG_WARN("failed to check group by placement transform enabled", K(ret));
    } else if (OB_FAIL(check_hint_valid(static_cast<ObDMLStmt &>(*stmt),
                                        params,
                                        hint_force_pushdown,
                                        is_valid))) {
      LOG_WARN("check hint failed", K(ret));
    } else if (!is_valid) {
      OPT_TRACE("hint disable group by pushdown");
    } else if (!enable_group_by_placement_transform && !hint_force_pushdown) {
      OPT_TRACE("system variable disable group by pushdown");
    } else if (OB_FAIL(transform_groupby_push_down(trans_stmt,
                                                   flattern_joined_tables,
                                                   outer_table_set,
                                                   push_down_ctx,
                                                   params))) {
      LOG_WARN("failed to transform group by push down", K(ret));
    } else {
      trans_happend = true;
    }
  }
  LOG_DEBUG("distribute", K(params));
  return ret;
}

/// distribute group, aggregation expr into view
int ObTransformGroupByPushdown::distribute_group_aggr(ObSelectStmt *stmt,
                                                       ObIArray<PushDownParam> &params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> aggr_list;
  ObSEArray<ObRawExpr *, 4> group_cols;
  int64_t total_sum_count = 0;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(append(aggr_list, stmt->get_aggr_items()))) {
    LOG_WARN("failed to append aggregation exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(
                       stmt->get_group_exprs(), group_cols))) {
    LOG_WARN("failed to extract group columns", K(ret));
  } else {
    total_sum_count = get_count_sum_num(aggr_list);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_FAIL(add_exprs(group_cols,
                          params.at(i).table_bit_index_,
                          params.at(i).join_columns_))) {
      LOG_WARN("failed to add owned group columns", K(ret));
    } else if (OB_FAIL(add_exprs(aggr_list,
                                 params.at(i).table_bit_index_,
                                 params.at(i).aggr_exprs_))) {
      LOG_WARN("failed to add owned aggr exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObAggFunRawExpr *count_expr = NULL;
    PushDownParam &param = params.at(i);
    int64_t view_sum_count_num = get_count_sum_num(param.aggr_exprs_);
    bool is_unique = false;
    if (param.table_bit_index_.is_empty()) {
      continue;
    } else if (OB_FAIL(check_unique(stmt, param, is_unique))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    } else if (total_sum_count == view_sum_count_num || is_unique) {
      LOG_TRACE("no need to add count expr",
                K(i), K(is_unique), K(total_sum_count), K(view_sum_count_num));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(
                         T_FUN_COUNT, count_expr))) {
      LOG_WARN("failed to create new aggregation expr", K(ret));
    } else if (OB_ISNULL(count_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the copied aggregation expr is null", K(ret), K(count_expr));
    } else if (OB_FAIL(count_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize count expr", K(ret));
    } else if (OB_FAIL(param.aggr_exprs_.push_back(count_expr))) {
      LOG_WARN("failed to push back count expr", K(ret));
    }
    if (OB_SUCC(ret) && (is_unique || param.aggr_exprs_.empty() ||
                         (param.join_columns_.empty() && param.group_exprs_.empty()))) {
      // its group-by exprs are unique
      // there is no need to do eager aggregation
      // scalar aggregation may change semantics: e.g.
      // Q1: select count(*) from t1, t2;
      // Q2: select sum(t1_cnt * t2_cnt) from (select count(*) as t1_cnt from t1),
      //                                      (select count(*) as t2_cnt from t2);
      // if t1, t2 are empty, Q1 return 0, Q2 return NULL
      if (OB_UNLIKELY(param.correlated_joined_tables_.count() != param.filter_exprs_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("correlated joined tables count should equal to filter exprs count", K(ret),
                  K(param.correlated_joined_tables_.count()), K(param.filter_exprs_.count()));
      } else {
        JoinedTable *joined_table = NULL;
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
int ObTransformGroupByPushdown::distribute_filter(ObSelectStmt *stmt,
                                                   ObIArray<PushDownParam> &params,
                                                   ObSqlBitSet<> &outer_join_tables,
                                                   ObRawExpr *cond)
{
  int ret = OB_SUCCESS;
  bool is_simple_filter = false;
  bool can_push_down = false;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(cond));
  } else if (!cond->has_flag(CNT_COLUMN)) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_UNLIKELY(column_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column exprs number is invalid", K(ret), K(*cond));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_simple_filter && i < params.count(); ++i) {
      PushDownParam &view = params.at(i);
      if (cond->get_relation_ids().overlap2(view.table_bit_index_)) {
        is_simple_filter = cond->get_relation_ids().is_subset2(view.table_bit_index_);
        can_push_down = is_simple_filter && !outer_join_tables.overlap2(cond->get_relation_ids());
        if (can_push_down) {
          if (OB_FAIL(view.filter_exprs_.push_back(cond))) {
            LOG_WARN("failed to push back condition expr", K(ret));
          } else if (OB_FAIL(view.correlated_joined_tables_.push_back(NULL))) {
            LOG_WARN("failed to push back correlated joined tables", K(ret));
          }
        } else if (OB_FAIL(add_exprs(column_exprs,
                                     view.table_bit_index_,
                                     view.join_columns_))) {
          LOG_WARN("failed to add owned exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

/// distribute l left join on 'cond' into views
int ObTransformGroupByPushdown::distribute_joined_on_conds(ObDMLStmt *stmt,
                                                            ObIArray<PushDownParam> &params,
                                                            JoinedTable *joined_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> column_exprs;
  ObSqlBitSet<> left_table_set;
  ObSqlBitSet<> right_table_set;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) ||
      OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(joined_table));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->left_table_, left_table_set))) {
    LOG_WARN("failed to get left table set", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_table_set))) {
    LOG_WARN("failed to get right table set", K(ret));
  }
  ObSEArray<ObRawExpr *, 4> filter_conds;
  ObSEArray<ObRawExpr *, 4> join_conds;
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
    ObRawExpr *expr = joined_table->join_conditions_.at(i);
    bool is_simple_filter = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (!expr->has_flag(CNT_COLUMN)) {
      // do nothing
    } else if (expr->get_relation_ids().num_members() == 1) {
      // joined on condition can not filter left table output
      if (expr->get_relation_ids().is_superset2(left_table_set)) {
        is_simple_filter = joined_table->is_right_join();
      } else if (expr->get_relation_ids().is_superset2(right_table_set)) {
        is_simple_filter = joined_table->is_left_join();
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
    if (OB_FAIL(add_exprs(column_exprs,
                          params.at(i).table_bit_index_,
                          params.at(i).join_columns_))) {
      LOG_WARN("failed to add owned expr", K(ret));
    } else if (OB_FAIL(add_exprs(filter_conds,
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
    if (OB_FAIL(distribute_joined_on_conds(
                         stmt, params, static_cast<JoinedTable*>(joined_table->left_table_)))) {
      LOG_WARN("failed to distribute join condition to view", K(ret));
    }
  }
  if (OB_SUCC(ret) && joined_table->right_table_->is_joined_table()) {
    if (OB_FAIL(distribute_joined_on_conds(
                         stmt, params, static_cast<JoinedTable*>(joined_table->right_table_)))) {
      LOG_WARN("failed to distribute join condition to view", K(ret));
    }
  }
  return ret;
}

/**
 * 1. 构建 STMT 做 eager aggregation
 * 2. 用 eager aggrgation 的结果来推导原来 aggregation 的结果。替换掉原始 aggregation 的引用。
 * 3. 用 generated table 替换原来的 table
**/
int ObTransformGroupByPushdown::transform_groupby_push_down(ObSelectStmt *stmt,
                                                             ObIArray<uint64_t> &flattern_joined_tables,
                                                             ObSqlBitSet<> &outer_join_tables,
                                                             ObCostBasedPushDownCtx &push_down_ctx,
                                                             ObIArray<PushDownParam> &params)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> eager_aggr_tables;
  ObSEArray<bool, 4> table_types;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<TableItem*, 4> table_items;
  ObSEArray<TableItem *, 4> new_table_items;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(append(table_items, stmt->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  }
  // step 1
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObSelectStmt *sub_stmt = NULL;
    TableItem *new_table_item = NULL;
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
    } else if (OB_FAIL(new_table_items.push_back(new_table_item))) {
      LOG_WARN("push back new table item failed", K(ret));
    } else {
      stmt->get_table_items().pop_back();
      //replace join columns, replace group columns
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
    ObSEArray<ObRawExpr *, 4> origin_aggr_exprs;
    ObSEArray<ObRawExpr *, 4> deduce_aggr_exprs;
    ObSEArray<ObRawExpr *, 4> cast_deduce_aggr_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *deduced_expr = NULL;
      if (OB_FAIL(origin_aggr_exprs.push_back(stmt->get_aggr_item(i)))) {
        LOG_WARN("failed to push back origin aggregation expr", K(ret));
      } else if (OB_FAIL(transform_aggregation_expr(*stmt,
                                                    *stmt->get_aggr_item(i),
                                                    eager_aggr_tables,
                                                    table_types,
                                                    deduced_expr))) {
        LOG_WARN("failed to transform aggregation expr", K(ret));
      } else if (OB_FAIL(deduce_aggr_exprs.push_back(deduced_expr))) {
        LOG_WARN("failed to push back deduced aggregation expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(
                                    *ctx_->expr_factory_, stmt->get_aggr_item(i), deduced_expr, ctx_->session_info_))) {
        LOG_WARN("failed to add cast", K(ret));
      } else if (OB_FAIL(cast_deduce_aggr_exprs.push_back(deduced_expr))) {
        LOG_WARN("failed to push back deduced aggregation expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->replace_relation_exprs(origin_aggr_exprs, cast_deduce_aggr_exprs))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
        // TODO link.zt seems to be useless
      } else if (OB_FAIL(ObTransformUtils::replace_exprs(origin_aggr_exprs,
                                                         deduce_aggr_exprs,
                                                         stmt->get_aggr_items()))) {
        LOG_WARN("failed to replace exprs", K(ret));
      } else {
      }
    }
  }

  // step 3
  if (OB_SUCC(ret)) {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(stmt->copy_and_replace_stmt_expr(copier))) {
      LOG_WARN("failed to copy and replace stmt expr", K(ret));
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
    } else if (OB_FAIL(stmt->get_table_rel_ids(new_table_items, push_down_ctx.new_table_relids_))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::push_down_group_by_into_view(ObSelectStmt *stmt,
                                                              const ObIArray<TableItem*> &table_items,
                                                              ObIArray<uint64_t> &flattern_joined_tables,
                                                              PushDownParam &params,
                                                              TableItem *&new_table_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sub_stmt = NULL;
  ObSEArray<int64_t, 4> table_index_array;
  ObSEArray<uint64_t, 4> joined_table_ids;
  bool is_added = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
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
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(),
                                                  params.filter_exprs_))) {
    LOG_WARN("failed to remove push down filters", K(ret));
  } else if (OB_FAIL(params.table_bit_index_.to_array(table_index_array))) {
    LOG_WARN("failed to convert bit set to array", K(ret));
  } else if (OB_FAIL(sub_stmt->get_stmt_hint().assign(stmt->get_stmt_hint()))) { 
    // zhanyue todo: remove some hint for sub stmt
    LOG_WARN("failed to assign stmt hint", K(ret));
  }
  /// 1. build table and from list
  for (int64_t i = 0; OB_SUCC(ret) && i < table_index_array.count(); ++i) {
    int64_t idx = table_index_array.at(i);
    TableItem *table_item = NULL;
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
    } else if (OB_FAIL(stmt->remove_check_constraint_item(table_item->table_id_))) {
      LOG_WARN("failed to remove table item info", K(ret));
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
      } else if (OB_FAIL(update_joined_table(stmt->get_joined_table(from_item.table_id_),
                                             table_item, new_table_item, is_added))) {
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
    } else if (OB_FAIL(sub_stmt->adjust_statement_id(ctx_->allocator_,
                                                     ctx_->src_qb_name_,
                                                     ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    }
  }
  /// 2. add joined tables
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table_ids.count(); ++i) {
    uint64_t table_id = joined_table_ids.at(i);
    JoinedTable *table = NULL;
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
    ColumnItem &col_item = stmt->get_column_items().at(i);
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
    if (OB_FAIL(ObTransformUtils::create_select_item(
                        *ctx_->allocator_, sub_stmt->get_group_exprs().at(i), sub_stmt))) {
      LOG_WARN("failed to create select expr", K(ret));
    }
  }
  /// 5. build aggregation
  for (int64_t i = 0; OB_SUCC(ret) && i < params.aggr_exprs_.count(); ++i) {
    ObRawExpr *expr = params.aggr_exprs_.at(i);
    if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_aggr_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else if (OB_FAIL(sub_stmt->add_agg_item(
                         static_cast<ObAggFunRawExpr&>(*expr)))) {
      LOG_WARN("failed to add aggr item", K(ret));
    } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(
                         *ctx_->allocator_, expr, sub_stmt))) {
      LOG_WARN("failed to add select item", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::update_joined_table(TableItem *table,
                                                     const TableItem *old_table,
                                                     TableItem *new_table,
                                                     bool &is_found)
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
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
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
                           joined_table->join_conditions_,
                           new_table->ref_query_->get_condition_exprs()))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
    } else if (OB_FAIL(update_joined_table(joined_table->left_table_,
                                           old_table,
                                           new_table,
                                           is_found))) {
      LOG_WARN("failed to update joined table", K(ret));
    } else if (is_found) {
      // do nothing
    } else if (OB_FAIL(update_joined_table(joined_table->right_table_,
                                           old_table,
                                           new_table,
                                           is_found))) {
      LOG_WARN("failed to update joined table", K(ret));
    }
  }
  return ret;
}

/// use eager aggreagtion result to deduce origin aggregation expr
int ObTransformGroupByPushdown::transform_aggregation_expr(ObDMLStmt &stmt,
                                                            ObAggFunRawExpr &aggr_expr,
                                                            ObIArray<TableItem *> &eager_aggr_views,
                                                            ObIArray<bool> &table_types,
                                                            ObRawExpr *&new_aggr_expr)
{
  int ret = OB_SUCCESS;
  ObItemType aggr_type = aggr_expr.get_expr_type();
  ObItemType group_aggr_type = (aggr_type == T_FUN_COUNT ? T_FUN_COUNT_SUM : aggr_type);
  ObSEArray<ObRawExpr *, 4> mul_params;
  ObRawExpr *aggr_column = NULL;
  bool need_mul_count = (group_aggr_type == T_FUN_SUM || group_aggr_type == T_FUN_COUNT_SUM);
  new_aggr_expr = NULL;
  if (OB_UNLIKELY(aggr_type != T_FUN_MAX && aggr_type != T_FUN_MIN &&
                  aggr_type != T_FUN_SUM && aggr_type != T_FUN_COUNT) ||
      OB_UNLIKELY(table_types.count() != eager_aggr_views.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregation type is invalid", K(ret), K(aggr_type),
             K(table_types.count()), K(eager_aggr_views.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < eager_aggr_views.count(); ++i) {
    ObRawExpr *view_column = NULL;
    if (OB_FAIL(ObTransformUtils::get_view_column(ctx_,
                                                  stmt,
                                                  eager_aggr_views.at(i),
                                                  table_types.at(i),
                                                  &aggr_expr,
                                                  view_column))) {
      LOG_WARN("failed to get aggregation column", K(ret));
    } else if (OB_NOT_NULL(view_column)) {
      aggr_column = view_column;
    } else if (!need_mul_count) {
      // do nothing
    } else if (OB_FAIL(get_count_star(stmt,
                                      eager_aggr_views.at(i),
                                      table_types.at(i),
                                      view_column))) {
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
    if (OB_FAIL(ObTransformUtils::convert_aggr_expr(ctx_, &stmt, &aggr_expr, aggr_column))) {
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
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                         *ctx_->expr_factory_, ctx_->session_info_, T_OP_MUL,
                         new_aggr_expr, new_aggr_expr, mul_params.at(i)))) {
      LOG_WARN("failed to create multiple expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObAggFunRawExpr *group_aggr = NULL;
    if (OB_ISNULL(new_aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new aggregation expr is null", K(ret), K(new_aggr_expr), K(mul_params));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr<ObAggFunRawExpr>(
                    group_aggr_type, group_aggr))) {
      LOG_WARN("failed to create new aggregation expr", K(ret));
    } else if (OB_ISNULL(group_aggr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the copied aggregation expr is null", K(ret), K(group_aggr));
    } else {
      group_aggr->add_real_param_expr(new_aggr_expr);
      new_aggr_expr = group_aggr;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(new_aggr_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  }
  return ret;
}

int ObTransformGroupByPushdown::get_count_star(ObDMLStmt &stmt,
                                                TableItem *table_item,
                                                bool is_outer_join_table,
                                                ObRawExpr *&count_column)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  ObRawExpr *last_select_expr = NULL;
  ObColumnRefRawExpr *col_expr = NULL;
  int64_t N = -1;
  count_column = NULL;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(table_item));
  } else if (!table_item->is_generated_table() ||
             OB_ISNULL(select_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(*table_item));
  } else if (0 > (N = select_stmt->get_select_item_size() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item size is invalid", K(ret));
  } else if (OB_ISNULL(last_select_expr = select_stmt->get_select_item(N).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr is null", K(ret));
  } else if (last_select_expr->get_expr_type() != T_FUN_COUNT ||
             last_select_expr->get_param_count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last select expr is not count(*)", K(ret));
  } else if (OB_ISNULL(col_expr = stmt.get_column_expr_by_id(
                               table_item->table_id_, OB_APP_MIN_COLUMN_ID + N))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column expr", K(*table_item));
  } else if (!is_outer_join_table) {
    count_column = col_expr;
  } else if (OB_FAIL(ObTransformUtils::wrap_case_when_for_count(ctx_, &stmt, col_expr,
                                                                count_column, true))) {
    LOG_WARN("failed to convert count star", K(ret));
  }
  return ret;
}

int ObTransformGroupByPushdown::check_unique(ObSelectStmt *stmt, PushDownParam &param, bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr *, 8> conditions;
  ObSEArray<int64_t, 4> table_indexes;
  ObSEArray<ObRawExpr *, 4> exprs;
  TableItem *table_item = NULL;
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
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, stmt->get_condition_exprs(),
                                                           *table_item, conditions))) {
    LOG_WARN("failed to extract columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt, table_item, exprs, conditions,
                                  ctx_->session_info_, ctx_->schema_checker_, is_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  }
  return ret;
}

int ObTransformGroupByPushdown::add_exprs(const ObIArray<ObRawExpr *> &exprs,
                                           ObSqlBitSet<> &table_set,
                                           ObIArray<ObRawExpr *> &dest)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !table_set.is_empty() && i < exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (!expr->has_flag(CNT_COLUMN) ||
               !table_set.is_superset2(expr->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(dest, expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::PushDownParam::merge(
    ObTransformGroupByPushdown::PushDownParam &other)
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

int ObTransformGroupByPushdown::merge_tables(ObIArray<PushDownParam> &params,
                                              const ObSqlBitSet<> &table_set)
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

int ObTransformGroupByPushdown::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<TableItem *, 4> single_or_joined_table;
  ObIArray<single_or_joined_table> *transed_tables = NULL;
  ObGroupByPlacementHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params) ||
      OB_ISNULL(transed_tables = static_cast<ObIArray<single_or_joined_table>*>(trans_params)) ||
      OB_UNLIKELY(transed_tables->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(transed_tables));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_PLACE_GROUP_BY, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
    for (int64_t i = 0; OB_SUCC(ret) && i < transed_tables->count(); ++i) {
      ObSEArray<ObTableInHint, 4> single_or_joined_hint_table;
      if (OB_FAIL(ObTransformUtils::get_sorted_table_hint(transed_tables->at(i),
                                                          single_or_joined_hint_table))) {
        LOG_WARN("failed to get table hint", K(ret));
      } else if (OB_FAIL(hint->get_tb_name_list().push_back(single_or_joined_hint_table))) {
        LOG_WARN("failed to push back table name list", K(ret));
      }
    }
  }
  LOG_DEBUG("show eval", K(ctx_->eval_cost_), K(ret));
  return ret;
}

int ObTransformGroupByPushdown::is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObCostBasedPushDownCtx *push_down_ctx = static_cast<ObCostBasedPushDownCtx *>(check_ctx);
  if (OB_ISNULL(plan) || OB_ISNULL(push_down_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!is_trans_plan) {
    // do nothing
  } else if (OB_FAIL(check_nl_operator(plan->get_plan_root(), push_down_ctx, is_valid))) {
    LOG_WARN("check nl operator failed", K(ret));
  } 
  return ret;
}

int ObTransformGroupByPushdown::check_nl_operator(ObLogicalOperator *op, ObCostBasedPushDownCtx *push_down_ctx, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const int64_t stmt_id = push_down_ctx->stmt_id_;
  ObLogJoin *join = NULL;
  if (OB_ISNULL(op) || OB_ISNULL(op->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else if (stmt_id == op->get_stmt()->get_stmt_id()) {
    if (log_op_def::LOG_JOIN == op->get_type()) {
      if (OB_ISNULL(join = static_cast<ObLogJoin *>(op))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("static cast failed", K(ret));
      } else if (JoinAlgo::NESTED_LOOP_JOIN == join->get_join_algo() && join->get_nl_params().count() > 0) {
        ObLogicalOperator *right_table = join->get_right_table();
        bool exist_group_by_op = false;
        if (OB_ISNULL(right_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("right table is null", K(ret));
        } else if (push_down_ctx->new_table_relids_.overlap(right_table->get_table_set())) {
          if (OB_FAIL(has_group_by_op(right_table, exist_group_by_op))) {
            LOG_WARN("has group by op failed", K(ret));
          } else {
            is_valid = !exist_group_by_op;
          }
        }
      } else {}
    } else {}
  } else {}

  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < op->get_num_of_child(); i++) {
    if (OB_FAIL(SMART_CALL(check_nl_operator(op->get_child(i), push_down_ctx, is_valid)))) {
      LOG_WARN("check nl operator failed", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPushdown::has_group_by_op(ObLogicalOperator *op, bool &bret)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else if (log_op_def::LOG_GROUP_BY == op->get_type()) {
    bret = true;
  } else if (op->get_num_of_child() != 1) {
    //do nothing
  } else if (OB_FAIL(SMART_CALL(has_group_by_op(op->get_child(0), bret)))) {
    LOG_WARN("check group by operator failed", K(ret));
  }
  return ret;
}

int ObTransformGroupByPushdown::check_hint_valid(ObDMLStmt &stmt,
                                                 ObIArray<PushDownParam> &params,
                                                 bool &hint_force_pushdown,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  hint_force_pushdown = false;
  const ObQueryHint *query_hint = NULL; 
  const ObGroupByPlacementHint *hint = static_cast<const ObGroupByPlacementHint*>(get_hint(stmt.get_stmt_hint()));
  const ObHint *no_rewrite = stmt.get_stmt_hint().get_no_rewrite_hint();
  ObSEArray<ObSEArray<TableItem *, 4>, 4> trans_tables;
  if (NULL == hint) {
    is_valid = (no_rewrite == NULL);
    LOG_TRACE("check group by hint is null", K(is_valid), K(stmt.get_stmt_hint()));
  } else if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_hint));
  } else  if (OB_FAIL(get_tables_from_params(static_cast<ObDMLStmt &>(stmt), params, trans_tables))) {
    LOG_WARN("get table failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_tables.count(); i++) {
      if (OB_UNLIKELY(trans_tables.at(i).count() <= 0) ||
          OB_ISNULL(trans_tables.at(i).at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(trans_tables));
      } else {
        is_valid = hint->enable_groupby_placement(query_hint->cs_type_, trans_tables.at(i));
        if (is_valid) {
          hint_force_pushdown = true;
        }
        LOG_TRACE("succeed hint valid", K(is_valid), K(*trans_tables.at(i).at(0)), K(*hint));
        if (!is_valid) { break; }
      }
    }
  }
  LOG_TRACE("succeed to check group by hint valid", K(is_valid), K(trans_tables), K(params), K(hint));
  return ret;
}

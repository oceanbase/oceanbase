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
#include "ob_transform_groupby_pullup.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformGroupByPullup::transform_one_stmt
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

int ObTransformGroupByPullup::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                           ObDMLStmt *&stmt,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<PullupHelper, 4> valid_views;
  ObCostBasedPullupCtx pullup_ctx;
  ObTryTransHelper try_trans_helper;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(check_groupby_pullup_validity(stmt, valid_views))) {
    LOG_WARN("failed to check group by pullup validity", K(ret));
  } else if (!valid_views.empty() && OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < valid_views.count(); ++i) {
    ObDMLStmt *trans_stmt = NULL;
    ObSelectStmt *view_stmt = NULL;
    int64_t view_id = valid_views.at(i).table_id_;
    TableItem *view = NULL;
    StmtUniqueKeyProvider unique_key_provider;
    try_trans_helper.unique_key_provider_ = &unique_key_provider;
    LOG_DEBUG("begin pull up", K(valid_views.count()), K(valid_views.at(i).need_merge_));
    if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                 *ctx_->expr_factory_,
                                                 stmt,
                                                 trans_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_ISNULL(view = trans_stmt->get_table_item_by_id(view_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view is null", K(ret));
    } else if (OB_FALSE_IT(pullup_ctx.view_talbe_id_ = valid_views.at(i).table_id_)) {
    } else if (OB_FAIL(get_trans_view(trans_stmt, view_stmt))) {
      LOG_WARN("failed to get transform view", K(ret));
    } else if (OB_FAIL(do_groupby_pull_up(view_stmt, valid_views.at(i), unique_key_provider))) {
      LOG_WARN("failed to do pull up group by", K(ret));
    } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                        valid_views.at(i).need_merge_, true,
                                        trans_happened, &pullup_ctx))) {
      LOG_WARN("failed to accept transform", K(ret));
    } else if (!trans_happened) {
      LOG_DEBUG("pull up not happen", K(trans_happened));
      if (OB_FAIL(try_trans_helper.recover(stmt->get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      }
    } else if (OB_ISNULL(view) || !view->is_generated_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view is not valid", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt, view->ref_query_))) {
      LOG_WARN("failed to add transform hint", K(ret));
    } else {
      LOG_DEBUG("add transform hint", K(view));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~(1 << transformer_type_));
  }
  return ret;
}


int ObTransformGroupByPullup::check_groupby_validity(const ObSelectStmt &stmt, bool &is_valid)
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

int ObTransformGroupByPullup::check_group_by_subset(ObRawExpr *expr, 
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

int ObTransformGroupByPullup::check_collation_validity(const ObDMLStmt &stmt, bool &is_valid)
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


// one generated table is valid simple group by
// the others must be has unique keys
int ObTransformGroupByPullup::check_groupby_pullup_validity(ObDMLStmt *stmt,
                                                               ObIArray<PullupHelper> &valid_views)
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
  } else if (!stmt->is_sel_del_upd() || stmt->is_set_stmt() ||
             stmt->is_hierarchical_query()) {
    // do nothing
  } else if (ObOptimizerUtil::find_item(ctx_->groupby_pushdown_stmts_, stmt->get_stmt_id())) {
    // do nothing
  } else if (stmt->get_from_item_size() == 0) {
    //do nothing
  } else if (stmt->get_from_item_size() == 1 &&
            !stmt->get_from_item(0).is_joined_) {
    //do nothing
  } else if (OB_FAIL(check_collation_validity(*stmt, is_collation_valid))) {
    LOG_WARN("failed to check collation validity", K(ret));
  } else if (!is_collation_valid) {
    // do nothing
  } else if (OB_FAIL(stmt->check_if_contain_select_for_update(has_for_update))) {
    LOG_WARN("failed to check if contain for update", K(ret));
  } else if (has_for_update) {
    OPT_TRACE("stmt contain for update, can not transform");
  } else if (OB_FAIL(stmt->check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  } else if (OB_FAIL(StmtUniqueKeyProvider::check_can_set_stmt_unique(stmt, has_unique_keys))) {
    LOG_WARN("failed to check stmt has unique keys", K(ret));
  } else if (!has_unique_keys) {
    //如果当前stmt不能生成唯一键，do nothing
    OPT_TRACE("stmt can not generate unique keys, can not transform");
  } else {
    is_valid = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_semi_info_size(); ++i) {
    if (OB_ISNULL(stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret), K(stmt->get_semi_infos().at(i)));
    } else if (OB_FAIL(stmt->get_table_rel_ids(stmt->get_semi_infos().at(i)->left_table_ids_,
                                               ignore_tables))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_subquery_expr_size(); ++i) {
    if (OB_ISNULL(stmt->get_subquery_exprs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery is null", K(ret));
    } else if (OB_FAIL(ignore_tables.add_members2(
                         stmt->get_subquery_exprs().at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  //如果generated table输出的聚合函数出现在join on condition，那么它也要被忽略
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_on_conditions(*stmt, ignore_tables))) {
      LOG_WARN("failed to check ignore views", K(ret));
    }
  }
  // check view validity
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_from_item_size(); ++i) {
    const FromItem from_item = stmt->get_from_item(i);
    TableItem *table = NULL;
    PullupHelper helper;
    if (OB_ISNULL(table = stmt->get_table_item(from_item))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(check_groupby_pullup_validity(stmt,
                                                    table,
                                                    helper,
                                                    contain_inner_table,
                                                    ignore_tables,
                                                    valid_views,
                                                    is_valid))) {
      LOG_WARN("failed to check group by pull up validity", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::check_groupby_pullup_validity(ObDMLStmt *stmt,
                                                              TableItem *table,
                                                              PullupHelper &helper,
                                                              bool contain_inner_table,
                                                              ObSqlBitSet<> &ignore_tables,
                                                              ObIArray<PullupHelper> &valid_views,
                                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool can_pullup = false;
  bool hint_valid = false;
  bool has_rand = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(table), K(ret));
  } else if (table->is_basic_table()) {
    //do nothing
  } else if (table->is_generated_table()) {
    bool is_valid_group = false;
    ObSelectStmt *sub_stmt = NULL;
    ObString dummy_str;
    const ObViewMergeHint *myhint = NULL;
    ObSQLSessionInfo *session_info = NULL;
    bool enable_group_by_placement_transform = false;
    OPT_TRACE("try", table);
    if (OB_ISNULL(ctx_) ||
        OB_ISNULL(session_info = ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ctx_), K(ret));
    } else if (OB_FAIL(session_info->is_groupby_placement_transformation_enabled(enable_group_by_placement_transform))) {
      LOG_WARN("failed to check group by placement transform enabled", K(ret));
    } else if (OB_ISNULL(sub_stmt = table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated table item", K(ret), K(*table));
    } else if (OB_FAIL(check_hint_valid(*stmt, *table->ref_query_, hint_valid))) {
      LOG_WARN("check hint failed", K(ret));
    } else if (!hint_valid) {
      // can not set is_valid as false, may pullup other table
      OPT_TRACE("hint reject transform");
    } else if (OB_FALSE_IT(myhint = static_cast<const ObViewMergeHint*>(sub_stmt->get_stmt_hint().get_normal_hint(T_MERGE_HINT)))) {
    } else if (!enable_group_by_placement_transform && (NULL == myhint || myhint->enable_no_group_by_pull_up())) {
      OPT_TRACE("system var disable group by placemebt");
    } else if (ignore_tables.has_member(stmt->get_table_bit_index(table->table_id_))) {
      // skip the generated table
      OPT_TRACE("ignore this table");
    } else if (contain_inner_table && (NULL == myhint || myhint->enable_no_group_by_pull_up())) {
      // do not rewrite inner table stmt with a cost-based rule
      OPT_TRACE("stmt contain inner table, can not transform");
    } else if (OB_FAIL(is_valid_group_stmt(sub_stmt, is_valid_group))) {
      LOG_WARN("failed to check is valid group stmt", K(ret));
    } else if (!is_valid_group) {
      // do nothing
      OPT_TRACE("not a valid group stmt");
    } else if (helper.need_check_having_ && sub_stmt->get_having_expr_size() > 0) {
      //do nothing
      OPT_TRACE("view can not have having exprs");
    } else if (OB_FALSE_IT(helper.table_id_ = table->table_id_)) {
    } else if (OB_FAIL(check_null_propagate(stmt, sub_stmt, helper, can_pullup))) {
      LOG_WARN("failed to check null propagate select expr", K(ret));
    } else if (!can_pullup) {
      //do nothing
    } else if (OB_FAIL(sub_stmt->has_rand(has_rand))) {
      LOG_WARN("failed to check stmt has rand func", K(ret));
      //stmt不能包含rand函数
    } else if (!(can_pullup = !has_rand)) {
      // do nothing
      OPT_TRACE("view has rand expr, can not transform");
    } else if (OB_FALSE_IT(helper.need_merge_ = (NULL != myhint 
                          && myhint->enable_group_by_pull_up(ctx_->src_qb_name_)))) {
    } else if (!helper.need_merge_ && stmt->get_table_size() > 1 && sub_stmt->get_table_size() > 1 &&
                stmt->get_table_size() + sub_stmt->get_table_size() - 1 > 10) {
      // More than 10 tables may result in the inability to enumerate a valid join order.
      OPT_TRACE("Too Many Table Items");
    } else if (OB_FAIL(valid_views.push_back(helper))) {
      LOG_WARN("failed to push back group stmt index", K(ret));
    } else {
      //do nothing
    }
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    PullupHelper left_helper = helper;
    PullupHelper right_helper = helper;
    bool check_left = true;
    bool check_right = true;
    left_helper.parent_table_ = joined_table;
    right_helper.parent_table_ = joined_table;
    if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      //LEFT OUTER JOIN的左表行为跟parent table相同
      //LEFT OUTER JOIN的右表上拉group by要求不能有having条件
      right_helper.need_check_having_ = true;
      right_helper.need_check_null_propagate_ = true;
      check_left = false;
    } else if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      //RIGHT OUTER JOIN的左表上拉group by要求不能有having条件
      left_helper.need_check_having_ = true;
      left_helper.need_check_null_propagate_ = true;
      check_right = false;
      //LEFT OUTER JOIN的右表行为跟parent table相同
    } else if (INNER_JOIN == joined_table->joined_type_) {
      //INNER JOIN的左表行为跟parent table相同
      //INNER JOIN的右表行为跟parent table相同
    } else if (FULL_OUTER_JOIN == joined_table->joined_type_) {
      if (OB_ISNULL(joined_table->left_table_) ||
          OB_ISNULL(joined_table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joined table has null child table", K(ret));
      } else if (!joined_table->left_table_->is_basic_table() &&
                !joined_table->right_table_->is_basic_table()) {
        //full join要求两侧至少有一个basic table，否则不能保证能够生成严格唯一键
        is_valid = false;
      } else {
        check_left = false;
        check_right = false;
        left_helper.need_check_having_ = true;
        left_helper.need_check_null_propagate_ = true;
        right_helper.need_check_having_ = true;
        right_helper.need_check_null_propagate_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (!is_valid) {
      //do nothing
    } else if (check_left &&
               OB_FAIL(SMART_CALL(check_groupby_pullup_validity(stmt,
                                                                joined_table->left_table_,
                                                                left_helper,
                                                                contain_inner_table,
                                                                ignore_tables,
                                                                valid_views,
                                                                is_valid)))) {
      LOG_WARN("failed to check group by pull up validity", K(ret));
    } else if (check_right &&
               OB_FAIL(SMART_CALL(check_groupby_pullup_validity(stmt,
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

int ObTransformGroupByPullup::check_on_conditions(ObDMLStmt &stmt,
                                                     ObSqlBitSet<> &ignore_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> columns;
  ObSEArray<ObRawExpr *, 4> conditions;
  if (OB_FAIL(ObTransformUtils::get_on_conditions(stmt, conditions))) {
    LOG_WARN("failed to get joined on conditions", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(conditions, columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    TableItem *table = NULL;
    ObRawExpr *expr = columns.at(i);
    ObColumnRefRawExpr *column_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(expr));
    } else if (OB_ISNULL(table = stmt.get_table_item_by_id(column_expr->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (table->is_generated_table()) {
      int64_t sel_idx = column_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
      ObRawExpr *select_expr = NULL;
      if (OB_UNLIKELY(sel_idx < 0 || sel_idx >= table->ref_query_->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select index is invalid", K(ret), K(sel_idx));
      } else if (OB_ISNULL(select_expr = table->ref_query_->get_select_item(sel_idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid select expr", K(ret), K(select_expr));
      } else if (!select_expr->has_flag(CNT_AGG)) {
        // do nothing
      } else if (OB_FAIL(ignore_tables.add_member(stmt.get_table_bit_index(table->table_id_)))) {
        LOG_WARN("failed to add ignore table set", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPullup::is_valid_group_stmt(ObSelectStmt *sub_stmt,
                                                     bool &is_valid_group)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  is_valid_group = false;
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub stmt is null", K(ret), K(sub_stmt));
  } else if (sub_stmt->get_group_expr_size() <= 0 ||
             sub_stmt->get_aggr_item_size() <= 0 ||
             sub_stmt->has_rollup() ||
             sub_stmt->get_window_func_exprs().count() > 0 ||
             sub_stmt->has_limit() ||
             sub_stmt->has_order_by() ||
             sub_stmt->has_distinct() ||
             sub_stmt->is_hierarchical_query() ||
             sub_stmt->get_semi_infos().count() > 0 ||
             sub_stmt->is_contains_assignment()) {
    is_valid_group = false;
  } else if (OB_FAIL(sub_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check sub stmt has row num", K(ret));
  } else if (has_rownum) {
    is_valid_group = false;
  } else if (OB_FAIL(check_groupby_validity(*sub_stmt, is_valid_group))) {
    LOG_WARN("failed to check is valid group", K(ret));
  }
  LOG_DEBUG("if valid group stmt", K(is_valid_group));
  return ret;
}

int ObTransformGroupByPullup::check_null_propagate(ObDMLStmt *parent_stmt,
                                                      ObSelectStmt *child_stmt,
                                                      PullupHelper &helper,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(child_stmt) || OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (helper.need_check_null_propagate_){
    ObSqlBitSet<> from_tables;
    ObColumnRefRawExpr *col_expr = NULL;
    ObSEArray<ObRawExpr *, 4> columns;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    if (OB_FAIL(child_stmt->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(child_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*child_stmt, columns, from_tables, column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else {
      //检查是否有空值拒绝表达式
      bool find = false;
      ObRawExpr *not_null_column = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !find && i < child_stmt->get_select_item_size(); i++) {
        ObRawExpr *expr = child_stmt->get_select_item(i).expr_;
        bool is_null_propagate = true;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr", K(ret));
        } else if (!expr->has_flag(CNT_AGG)) {
          //do nothing
        } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, column_exprs, is_null_propagate))) {
          LOG_WARN("failed to is null propagate expr", K(ret));
        } else if (!is_null_propagate) {
          find = true;
        } else {/*do nothing*/}
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (!find) {
        /*do nothing*/
      } else if (OB_FAIL(find_not_null_column(*parent_stmt, *child_stmt, helper, column_exprs, not_null_column))){
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
  } else {/*do nothing*/}
  return ret;
}

int ObTransformGroupByPullup::find_not_null_column(ObDMLStmt &parent_stmt,
                                              ObSelectStmt &child_stmt,
                                              PullupHelper &helper,
                                              ObIArray<ObRawExpr *> &column_exprs,
                                              ObRawExpr *&not_null_column)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  not_null_column = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_FAIL(find_not_null_column_with_condition(parent_stmt,
                                                  child_stmt,
                                                  helper,
                                                  column_exprs,
                                                  not_null_column))) {
    LOG_WARN("failed to find not null column with join condition", K(ret));
  } else if (OB_NOT_NULL(not_null_column)) {
    //find not null column, do nothing
  } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(child_stmt,
                                                          not_null_column,
                                                          is_valid,
                                                          ctx_))) {
    LOG_WARN("failed to find not null expr", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObTransformGroupByPullup::find_not_null_column_with_condition(
                                        ObDMLStmt &parent_stmt,
                                        ObSelectStmt &child_stmt,
                                        PullupHelper &helper,
                                        ObIArray<ObRawExpr *> &column_exprs,
                                        ObRawExpr *&not_null_column)
{
  int ret = OB_SUCCESS;
  not_null_column = NULL;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> temp_exprs;
  if (OB_ISNULL(helper.parent_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(join_conditions.assign(helper.parent_table_->join_conditions_))) {
    LOG_WARN("failed to assign join conditions");
  } else if (OB_FAIL(parent_stmt.get_column_exprs(helper.table_id_, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_column_exprs,
                                                                          child_stmt,
                                                                          new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < old_column_exprs.count(); ++i) {
      bool has_null_reject = false;
      //首先找到null reject的select expr
      if (OB_FAIL(ObTransformUtils::has_null_reject_condition(join_conditions,
                                                              old_column_exprs.at(i),
                                                              has_null_reject))) {
        LOG_WARN("failed to check has null reject condition", K(ret));
      } else if (!has_null_reject) {
        //do nothing
      } else if (OB_FAIL(find_null_propagate_column(new_column_exprs.at(i),
                                                    column_exprs,
                                                    not_null_column,
                                                    find))) {
        LOG_WARN("failed to find null propagate column", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformGroupByPullup::find_null_propagate_column(ObRawExpr *condition,
                                                            ObIArray<ObRawExpr*> &columns,
                                                            ObRawExpr *&null_propagate_column,
                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  null_propagate_column = NULL;
  bool is_null_propagate = false;
  ObSEArray<const ObRawExpr *, 4> dummy_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < columns.count(); ++i) {
    dummy_exprs.reuse();
    if (OB_FAIL(dummy_exprs.push_back(columns.at(i)))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(condition,
                                                                dummy_exprs,
                                                                is_null_propagate))) {
      LOG_WARN("failed to check null propagate expr", K(ret));
    } else if (!is_null_propagate) {
      //do nothing
    } else {
      null_propagate_column = columns.at(i);
      is_valid = true;
    }
  }
  return ret;
}

/**
 * @brief ObTransformGroupByPullup::get_trans_view
 *  get/create a select stmt for transformation
 * @return
 */
int ObTransformGroupByPullup::get_trans_view(ObDMLStmt *stmt, ObSelectStmt *&view_stmt)
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
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
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
      view_stmt = static_cast<ObSelectStmt *>(stmt);
    } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, stmt, view_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::do_groupby_pull_up(ObSelectStmt *stmt,
                                                 PullupHelper &helper,
                                                 StmtUniqueKeyProvider &unique_key_provider)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> unique_exprs;
  ObSEArray<ObRawExpr *, 4> aggr_column;
  ObSEArray<ObRawExpr *, 4> aggr_select;
  TableItem *table_item = NULL;
  ObSelectStmt *subquery = NULL;
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
  } else if (OB_FAIL(unique_key_provider.generate_unique_key(ctx_,
                                                            stmt,
                                                            ignore_tables,
                                                            unique_exprs))) {
    LOG_WARN("failed to generated unique keys", K(ret));
  } else if (OB_FAIL(append(stmt->get_group_exprs(), unique_exprs))) {
    LOG_WARN("failed to append group exprs", K(ret));
  }
  /** 找到所有包含聚合函数的select item，拉出
    * 拉出group by expr
    * 拉出having condition
    * 拉出包含的子查询
    * 提取select item、group by expr、having condition的column expr，在视图内创建select item*/
  //找到包含聚合函数的select expr，以及对应的column expr
  ObSqlBitSet<> removed_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
    ObRawExpr *select_expr = subquery->get_select_item(i).expr_;
    ObColumnRefRawExpr *col_expr = NULL;
    int64_t column_id = OB_INVALID_ID;
    if (OB_ISNULL(select_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null select expr", K(ret));
    } else if (!select_expr->has_flag(CNT_AGG)) {
      //do nothing
    } else if (OB_FALSE_IT(column_id = i + OB_APP_MIN_COLUMN_ID)) {
    } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_item->table_id_, column_id))) {
      //未引用的，直接删除
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
    } else if (OB_FAIL(stmt->replace_relation_exprs(aggr_column, aggr_select))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_group_exprs(), subquery->get_group_exprs()))) {
      LOG_WARN("failed to append group exprs", K(ret));
    } else if (OB_FAIL(append(stmt->get_aggr_items(), subquery->get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (OB_FAIL(append(stmt->get_having_exprs(), subquery->get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_,
                                                             table_item->table_id_,
                                                             *subquery,
                                                             *stmt,
                                                             removed_idx))) {
      LOG_WARN("failed to remove select items", K(ret));
    } else {
      subquery->get_group_exprs().reset();
      subquery->get_aggr_items().reset();
      subquery->get_having_exprs().reset();
      if (OB_FAIL(ObTransformUtils::generate_select_list(ctx_, stmt, table_item))) {
        LOG_WARN("failed to generate select list", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      }
    }
  }
  // classify where conditions
  ObSEArray<ObRawExpr *, 4> new_conds;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    ObRawExpr *cond = NULL;
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

int ObTransformGroupByPullup::wrap_case_when_if_necessary(ObSelectStmt &child_stmt,
                                                            PullupHelper &helper,
                                                            ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> from_tables;
  ObRawExpr *not_null_column = NULL;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (helper.not_null_column_id_ == OB_INVALID_ID) {
    //do nothing
  } else if (OB_FAIL(child_stmt.get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(child_stmt.get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(child_stmt,
                                                          columns,
                                                          from_tables,
                                                          column_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_ISNULL(not_null_column = child_stmt.get_column_expr_by_id(helper.not_null_column_table_id_,
                                                                          helper.not_null_column_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not find column", K(helper.not_null_column_table_id_),
                                    K(helper.not_null_column_id_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      bool is_null_propagate = false;
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(exprs.at(i),
                                                                  column_exprs,
                                                                  is_null_propagate))) {
        LOG_WARN("failed to is null propagate expr", K(ret));
      } else if (is_null_propagate) {
        //do nothing
      } else if (OB_FAIL(wrap_case_when(child_stmt, not_null_column, exprs.at(i)))) {
        LOG_WARN("failed to wrap case when", K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}


int ObTransformGroupByPullup::wrap_case_when(ObSelectStmt &child_stmt,
                                                ObRawExpr *not_null_column,
                                                ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ctx_), K(ret));
  } else if (OB_ISNULL(not_null_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null column expr", K(ret));
  } else {
    ObRawExpr *null_expr = NULL;
    ObRawExpr *cast_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObRawExprFactory *factory = ctx_->expr_factory_;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*factory, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_ISNULL(null_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                          ctx_->expr_factory_,
                          ctx_->session_info_,
                          *null_expr,
                          expr->get_result_type(),
                          cast_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(child_stmt,
                                                              not_null_column,
                                                              expr,
                                                              cast_expr,
                                                              case_when_expr,
                                                              ctx_))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_ISNULL(case_when_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("case when expr is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *case_when_expr,
                                                               expr->get_result_type(),
                                                               expr))) {
      LOG_WARN("failed to add cast expr", K(ret));
    }
  }
  return ret;
}


int ObTransformGroupByPullup::is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObCostBasedPullupCtx *ctx = static_cast<ObCostBasedPullupCtx*>(check_ctx);
  if (OB_ISNULL(ctx) || OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (is_trans_plan) {
    //do nothing
  } else if (OB_FAIL(check_original_plan_validity(plan->get_plan_root(),
                                                  ctx->view_talbe_id_,
                                                  is_valid))) {
    LOG_WARN("failed to check plan validity", K(ret));
  }
  return ret;
}


int ObTransformGroupByPullup::has_group_by_op(ObLogicalOperator *op, bool &bret)
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

int ObTransformGroupByPullup::check_hint_valid(const ObDMLStmt &stmt,
                                               const ObSelectStmt &ref_query,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const ObViewMergeHint *myhint = static_cast<const ObViewMergeHint*>(get_hint(ref_query.get_stmt_hint()));
  bool is_disable = (NULL != myhint && myhint->enable_no_group_by_pull_up());
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = ref_query.get_stmt_hint().get_no_rewrite_hint();
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (query_hint->has_outline_data()) {
    if (myhint == NULL || myhint->is_disable_hint()) {
      is_valid = false;
    }
  } else if (NULL != myhint && myhint->enable_group_by_pull_up(ctx_->src_qb_name_)) {
    // enable transform hint added after transform in construct_transform_hint()
    is_valid = true;
  } else if (is_disable || NULL != no_rewrite1 || NULL != no_rewrite2) {
    // add disable transform hint here
    is_valid = false;
    if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite1))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite2))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *merged_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(merged_stmt = static_cast<ObSelectStmt*>(trans_params))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(merged_stmt));
  } else {
    ObViewMergeHint *hint = NULL;
    ObString child_qb_name;
    const ObViewMergeHint *myhint = NULL;
    if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_MERGE_HINT, hint))) {
      LOG_WARN("failed to create hint", K(ret));
    } else if (OB_FAIL(merged_stmt->get_qb_name(child_qb_name))) {
      LOG_WARN("failed to get qb name", K(ret), K(merged_stmt->get_stmt_id()));
    } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
      LOG_WARN("failed to push back hint", K(ret));
    } else if (NULL != (myhint = static_cast<const ObViewMergeHint*>(get_hint(merged_stmt->get_stmt_hint())))
                && myhint->enable_group_by_pull_up(ctx_->src_qb_name_)
                && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (OB_FAIL(merged_stmt->adjust_qb_name(ctx_->allocator_,
                                                   ctx_->src_qb_name_,
                                                   ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust qb name", K(ret));
    } else if (OB_FAIL(ctx_->add_src_hash_val(child_qb_name))) {
      LOG_WARN("failed to add src hash val", K(ret));
    } else {
      hint->set_qb_name(child_qb_name);
      hint->set_parent_qb_name(ctx_->src_qb_name_);
    }
  }
  return ret;
}

int ObTransformGroupByPullup::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                    const int64_t current_level,
                                    const ObDMLStmt &stmt,
                                    bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  UNUSED(parent_stmts);
  UNUSED(current_level);
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (!stmt.is_sel_del_upd() || stmt.has_instead_of_trigger() || stmt.is_set_stmt()
      || stmt.is_hierarchical_query()) {
    need_trans = false;
  } else if (stmt.get_from_item_size() == 0) {
    need_trans = false;
  } else if (stmt.get_from_item_size() == 1 &&
            !stmt.get_from_item(0).is_joined_) {
    need_trans = false;
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = !query_hint->global_hint_.disable_cost_based_transform();
  } else if (NULL == (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))
             || !trans_hint->is_view_merge_hint()
             || !static_cast<const ObViewMergeHint*>(trans_hint)->enable_group_by_pull_up(ctx_->src_qb_name_)) {
    /*do nothing*/
  } else {
    const TableItem *table = NULL;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(table = stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (!table->is_generated_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(*table));
      } else {
        need_trans = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                  get_hint(table->ref_query_->get_stmt_hint()));
        LOG_DEBUG("need trans pullup0", K(need_trans));
      }
    }
    if (OB_SUCC(ret) && !need_trans) {
      OPT_TRACE("outline reject transform");
    }
  }
  LOG_DEBUG("need trans pullup", K(need_trans));
  return ret;
}

int ObTransformGroupByPullup::check_original_plan_validity(ObLogicalOperator* root,
                                                           uint64_t view_table_id,
                                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObSEArray<ObLogicalOperator*, 4> parent_ops;
  ObLogicalOperator *subplan = NULL;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> group_exprs;
  uint64_t groupby_nopushdown_cut_ratio = 1;
  double group_ndv = 1.0;
  double card = 1.0;
  bool has_stats = true;
  const ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(root) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root), K(ret));
  } else if (OB_FAIL(find_operator(root, parent_ops, view_table_id, subplan))) {
    LOG_WARN("failed to find subplan scan operator", K(root), K(view_table_id), K(ret));
  } else if (OB_ISNULL(subplan) || parent_ops.empty()) {
    //do nothing
  } else if (OB_UNLIKELY(subplan->get_num_of_child() == 0) ||
             OB_ISNULL(subplan = subplan->get_child(ObLogicalOperator::first_child)) ||
             OB_ISNULL(subplan->get_stmt()) ||
             OB_UNLIKELY(!subplan->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(child_stmt = static_cast<const ObSelectStmt*>(subplan->get_stmt()))) {
    // do nothing
  } else if (OB_FAIL(check_all_table_has_statistics(subplan, has_stats))) {
    LOG_WARN("failed to check all table has statistics", K(ret));
  } else if (!has_stats) {
    is_valid = false;
    OPT_TRACE("check original plan has statistics:", has_stats);
  } else if (OB_FAIL(extract_columns_in_join_conditions(parent_ops,
                                                        view_table_id,
                                                        column_exprs))) {
    LOG_WARN("failed to extract columns in join conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(column_exprs,
                                                                          *child_stmt,
                                                                          select_exprs))) {
    LOG_WARN("failed to convert column exprs to select exprs", K(ret));
  } else if (OB_FAIL(get_group_by_subset(select_exprs,
                                         child_stmt->get_group_exprs(),
                                         group_exprs))) {
    LOG_WARN("failed to get group by subset", K(ret));
  } else if (OB_FAIL(ctx_->session_info_->get_sys_variable(share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
                                                           groupby_nopushdown_cut_ratio))) {
    LOG_WARN("failed to get session variable", K(ret));
  } else if (OB_FAIL(calc_group_exprs_ndv(group_exprs, subplan, group_ndv, card))) {
      LOG_WARN("failed to check group exprs", K(ret));
  } else {
    double expansion_rate = card / group_ndv;
    is_valid = expansion_rate < groupby_nopushdown_cut_ratio;
    LOG_TRACE("check original plan", K(is_valid), K(group_exprs), K(group_ndv), K(expansion_rate));
    OPT_TRACE("check original plan group by exprs:", group_exprs);
    OPT_TRACE("check original plan group by ndv:", group_ndv);
    OPT_TRACE("check original plan expansion rate:", expansion_rate);
  }
  return ret;
}

int ObTransformGroupByPullup::find_operator(ObLogicalOperator* root,
                                            ObIArray<ObLogicalOperator*> &parents,
                                            uint64_t view_table_id,
                                            ObLogicalOperator *&subplan_root)
{
  int ret = OB_SUCCESS;
  subplan_root = NULL;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (log_op_def::LOG_SUBPLAN_SCAN == root->get_type() &&
             static_cast<ObLogSubPlanScan *>(root)->get_subquery_id() == view_table_id) {
    subplan_root = root;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == subplan_root && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_FAIL(SMART_CALL(find_operator(child, parents, view_table_id, subplan_root)))) {
        LOG_WARN("failed to find operator", K(ret));
      } else if (NULL == subplan_root) {
        //do nothing
      } else if (parents.empty() ||
                 parents.at(0)->get_stmt() == root->get_stmt()) {
        if (OB_FAIL(parents.push_back(root))) {
          LOG_WARN("failed to push back operator", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformGroupByPullup::calc_group_exprs_ndv(const ObIArray<ObRawExpr*> &group_exprs,
                                                   ObLogicalOperator *subplan_root,
                                                   double &group_ndv,
                                                   double &card)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObLogicalOperator *child_op = subplan_root;
  group_ndv = 1.0;
  if (OB_ISNULL(subplan_root) ||
      OB_ISNULL(plan = subplan_root->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (OB_FAIL(find_base_operator(child_op))) {
    LOG_WARN("failed to find base operator", K(ret));
  } else if (OB_ISNULL(child_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else {
    card = child_op->get_card();
    plan->get_selectivity_ctx().init_op_ctx(&child_op->get_output_equal_sets(), card);
    if (group_exprs.empty()) {
      group_ndv = 1.0;
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(plan->get_update_table_metas(),
                                                            plan->get_selectivity_ctx(),
                                                            group_exprs,
                                                            card,
                                                            group_ndv))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObTransformGroupByPullup::find_base_operator(ObLogicalOperator *&root)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && root != NULL &&
         (root->get_type() ==log_op_def::LOG_GROUP_BY ||
          root->get_type() == log_op_def::LOG_EXCHANGE)) {
    if (OB_UNLIKELY(root->get_num_of_child() != 1) ||
        OB_ISNULL(root = root->get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null logical operator", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::extract_columns_in_join_conditions(
                              ObIArray<ObLogicalOperator*> &parent_ops,
                              uint64_t table_id,
                              ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *parent = NULL;
  ObSEArray<ObRawExpr*, 4> tmp_column_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_ops.count(); ++i) {
    if (OB_ISNULL(parent = parent_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(parent), K(ret));
    } else if (log_op_def::LOG_JOIN == parent->get_type()) {
      ObLogJoin *join_op = static_cast<ObLogJoin*>(parent);

      if (HASH_JOIN == join_op->get_join_algo() ||
          MERGE_JOIN == join_op->get_join_algo()) {
        tmp_column_exprs.reuse();
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(join_op->get_equal_join_conditions(),
                                                         table_id,
                                                         tmp_column_exprs))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        } else if (OB_FAIL(append_array_no_dup(column_exprs, tmp_column_exprs))) {
          LOG_WARN("failed to append array no dup", K(ret));
        }
      } else if (NESTED_LOOP_JOIN == join_op->get_join_algo()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < join_op->get_other_join_conditions().count(); ++i) {
          ObRawExpr *cond = NULL;
          if (OB_ISNULL(cond = join_op->get_other_join_conditions().at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (!cond->has_flag(IS_JOIN_COND)) {
            // do nothing
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond,
                                                                  table_id,
                                                                  tmp_column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else if (OB_FAIL(append_array_no_dup(column_exprs, tmp_column_exprs))) {
            LOG_WARN("failed to append array no dup", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < join_op->get_nl_params().count(); ++i) {
          tmp_column_exprs.reuse();
          if (OB_ISNULL(join_op->get_nl_params().at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("nl param is null", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(join_op->get_nl_params().at(i)->get_ref_expr(),
                                                                  table_id,
                                                                  tmp_column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else if (OB_FAIL(append_array_no_dup(column_exprs, tmp_column_exprs))) {
            LOG_WARN("failed to append array no dup", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformGroupByPullup::get_group_by_subset(ObRawExpr *expr,
                                                  const ObIArray<ObRawExpr *> &group_exprs,
                                                  ObIArray<ObRawExpr *> &subset_group_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    int64_t idx = -1;
    if (expr->has_flag(IS_AGG) || expr->has_flag(IS_CONST)) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::get_expr_idx(group_exprs, expr, idx))) {
      LOG_WARN("get expr idx failed", K(ret));
    } else if (idx == -1) { //not found
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(get_group_by_subset(expr->get_param_expr(i), group_exprs, subset_group_exprs)))) {
          LOG_WARN("check group by subset faield", K(ret));
        }
      }
    } else if (OB_FAIL(add_var_to_array_no_dup(subset_group_exprs, expr))) {
      LOG_WARN("failed to add var to array no dump", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::get_group_by_subset(ObIArray<ObRawExpr *> &exprs,
                                                  const ObIArray<ObRawExpr *> &group_exprs,
                                                  ObIArray<ObRawExpr *> &subset_group_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(get_group_by_subset(exprs.at(i), group_exprs,
                                    subset_group_exprs))) {
      LOG_WARN("check group by exprs failed", K(ret));
    }
  }
  return ret;
}

int ObTransformGroupByPullup::check_all_table_has_statistics(ObLogicalOperator *op, bool &has_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (op->get_type() == log_op_def::LOG_TABLE_SCAN) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(op);
    ObLogPlan *plan = table_scan->get_plan();
    OptTableMeta* meta = NULL;
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(meta = plan->get_basic_table_metas()
                                     .get_table_meta_by_table_id(table_scan->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      has_stats = meta->get_version() > 0;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && has_stats && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(check_all_table_has_statistics(op->get_child(i), has_stats)))) {
        LOG_WARN("failed to check all table has statistics", K(ret));
      }
    }
  }
  return ret;
}
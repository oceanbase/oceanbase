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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_outerjoin_limit_pushdown.h"

namespace oceanbase {
using namespace common;
namespace sql {
/**
 * @brief ObTransformOuterJoinLimitPushDown::transform_one_stmt
 * Pushdown limit/orderby limit into outer join
 *
 * Scenarios:
 * 1. left outer join + limit: push down limit into left table
 * 2. left outer join + order by left table column + limit: push down order by limit into left table
 *
 * NOTE: The rule only supports LEFT outer join by converting right to left at the very beginning.
 * The mixed join type, such as left-inner-left, is NOT supported. Besides, rownum also is expected to
 * be transformed to limit before entering into this rule, and those remained will NOT be supported.
 *
 * Order suggestion with other transformation:
 *  [after]  simplify : unnecessary order by removal
 *  [after]  set_op : (order by) limit pushing down set op
 *  [before] or_expansion : new set op generated
 *
 */
int ObTransformOuterJoinLimitPushDown::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_valid = false;
  OjLimitPushDownHelper helper;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_FAIL(check_stmt_validity(stmt, helper, is_valid))) {
    LOG_WARN("failed to check stmt validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(do_transform(helper))) {
    LOG_WARN("failed to do outer join limit push down", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_stmt_validity(
    ObDMLStmt* stmt, OjLimitPushDownHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else if (OB_FAIL(check_basic(stmt, helper, is_valid))) {
    LOG_WARN("failed to check basic", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_orderby_and_condition(helper, is_valid))) {
    LOG_WARN("failed to check validity for (orderby) limit", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_basic(ObDMLStmt* stmt, OjLimitPushDownHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool has_rownum = false;
  bool is_valid_limit = false;
  TableItem* table_item = NULL;
  ObSelectStmt* select_stmt;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else if (!stmt->is_select_stmt()) {
    is_valid = false;
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    is_valid = false;
  } else if (select_stmt->is_set_stmt()) {
    // self will be ignored and children will be handled recursively
    is_valid = false;
  } else if (!select_stmt->has_limit() || select_stmt->is_hierarchical_query() || select_stmt->is_calc_found_rows() ||
             select_stmt->has_group_by() || select_stmt->has_having() || select_stmt->has_rollup() ||
             select_stmt->has_window_function() || select_stmt->has_sequence() ||
             select_stmt->get_semi_infos().count() > 0 ||
             select_stmt->has_distinct()) {
    is_valid = false;
  } else if (OB_FAIL(check_limit(select_stmt, is_valid_limit))) {
    LOG_WARN("failed to check the validity of limit expr", K(ret));
  } else if (!is_valid_limit) {
    is_valid = false;
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    // rownum should be replaced by preprocess stage.
    // NOT support those left.
    is_valid = false;
  } else if (select_stmt->get_from_items().count() != 1) {
    // only one from item is allowed
    is_valid = false;
  } else if (OB_ISNULL(table_item = select_stmt->get_table_item(select_stmt->get_from_item(0)))) {
    is_valid = false;
  } else if (!table_item->is_joined_table()) {
    is_valid = false;
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(select_stmt))) {
    LOG_WARN("failed to change right outer join to left.", K(ret));
  } else {
    const FromItem from_item = select_stmt->get_from_item(0);
    table_item = select_stmt->get_table_item(from_item);
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    bool is_single_type = false;
    bool has_generated_table = false;
    // only left outer allowed after right->left transformation
    // mixed join type, such as left & inner is NOT allowed
    if (OB_FAIL(check_join_type(joined_table, LEFT_OUTER_JOIN, is_single_type, has_generated_table))) {
      LOG_WARN("failed to check deep tree type", K(ret));
    } else if (!is_single_type) {
      is_valid = false;
    } else if (has_generated_table && select_stmt->get_condition_size() > 0) {
      is_valid = false;
    } else {
      helper.select_stmt_ = select_stmt;
      helper.is_limit_only_ = !select_stmt->has_order_by();
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_orderby_and_condition(OjLimitPushDownHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSqlBitSet<> table_ids;
  ObSqlBitSet<> condition_ids;
  ObSqlBitSet<> orderby_ids;
  bool is_valid_target_table = false;
  bool is_valid_condition = false;
  bool is_valid_orderby = false;
  if (OB_ISNULL(helper.select_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt in helper", K(ret));
  } else if (OB_FAIL(collect_condition_exprs_table_ids(
                 helper.select_stmt_, condition_ids, is_valid_condition, helper.extracted_conditions_))) {
    LOG_WARN("failed to collect condition table ids", K(ret));
  } else if (!is_valid_condition) {
    is_valid = false;
  } else if (OB_FAIL(collect_orderby_table_ids(helper.select_stmt_, orderby_ids, is_valid_orderby))) {
    LOG_WARN("failed to collect orderby table ids", K(ret));
  } else if (!is_valid_orderby) {
    is_valid = false;
  } else if (OB_FAIL(table_ids.add_members2(condition_ids))) {
    LOG_WARN("failed to add condition ids", K(ret));
  } else if (OB_FAIL(table_ids.add_members2(orderby_ids))) {
    LOG_WARN("failed to add orderby ids", K(ret));
  } else if (OB_FAIL(find_target_table(helper.select_stmt_, table_ids, helper.target_table_))) {
    LOG_WARN("failed to find target joined table", K(ret));
  } else if (OB_ISNULL(helper.target_table_)) {
    is_valid = false;
  } else if (OB_FAIL(check_validity_for_target_table(helper, is_valid_target_table))) {
    LOG_WARN("failed to check validity for target table", K(ret));
  } else if (!is_valid_target_table) {
    is_valid = false;
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_limit(ObSelectStmt* select_stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt found", K(ret));
  } else if (!select_stmt->has_limit()) {
    // do nothing
  } else if (OB_NOT_NULL(select_stmt->get_limit_percent_expr())) {
    is_valid = false;
  } else if (select_stmt->is_fetch_with_ties() || OB_ISNULL(select_stmt->get_limit_expr())) {
    is_valid = false;
  } else {
    ObRawExpr* offset_expr = select_stmt->get_offset_expr();
    ObRawExpr* limit_expr = select_stmt->get_limit_expr();
    bool is_offset_valid;
    bool is_limit_valid;
    if (OB_FAIL(check_offset_limit_expr(limit_expr, is_limit_valid))) {
      LOG_WARN("failed to check limit expr", K(ret));
    } else if (!is_limit_valid) {
      is_valid = false;
    } else if (OB_NOT_NULL(offset_expr) && OB_FAIL(check_offset_limit_expr(offset_expr, is_offset_valid))) {
      LOG_WARN("failed to check offset expr", K(ret));
    } else if (OB_NOT_NULL(offset_expr) && !is_offset_valid) {
      is_valid = false;
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_offset_limit_expr(ObRawExpr* offset_limit_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(offset_limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal limit expr", K(ret));
  } else if (T_NULL == offset_limit_expr->get_expr_type() || T_QUESTIONMARK == offset_limit_expr->get_expr_type()) {
    // do nothing
  } else if (offset_limit_expr->is_const_expr()) {
    const ObObj& value = static_cast<const ObConstRawExpr*>(offset_limit_expr)->get_value();
    if (value.is_invalid_type() || !value.is_integer_type()) {
      is_valid = false;
    } else if (value.get_int() < 0) {
      is_valid = false;
    }
  } else {
    // ignore cast format introduced by rownum
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_join_type(
    TableItem *table_item, ObJoinType joined_type, bool &is_single_type, bool &has_generated_table)
{
  int ret = OB_SUCCESS;
  is_single_type = true;
  bool is_stack_overflow = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table item", K(ret));
  } else if (table_item->is_basic_table()) {
    // do nothing
  } else if (table_item->is_generated_table()) {
    has_generated_table = true;
  } else if (table_item->is_joined_table() && static_cast<JoinedTable *>(table_item)->joined_type_ == joined_type) {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    // only need to check left side recursively after right to left formalization
    bool is_left_single_type = false;
    if (OB_FAIL(SMART_CALL(
            check_join_type(joined_table->left_table_, joined_type, is_left_single_type, has_generated_table)))) {
      LOG_WARN("failed to check left child deep tree type", K(ret));
    } else if (!is_left_single_type) {
      is_single_type = false;
    } else {
      // do nothing
    }
  } else {
    is_single_type = false;
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::collect_condition_exprs_table_ids(ObSelectStmt* select_stmt,
    ObSqlBitSet<>& table_ids, bool& is_valid_condition, ObIArray<ObRawExpr*>& extracted_conditions)
{
  int ret = OB_SUCCESS;
  is_valid_condition = true;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_condition && i < select_stmt->get_condition_exprs().count(); ++i) {
      ObRawExpr* condition_expr = select_stmt->get_condition_exprs().at(i);
      if (OB_ISNULL(condition_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid condition expr", K(ret));
      } else if (condition_expr->has_flag(CNT_SUB_QUERY)) {
        // subquery support needs to judge is_query_ref_expr, do the collection,
        // remove from upper stmt and assign to generate_view.
        is_valid_condition = false;
      } else if (!condition_expr->get_expr_levels().has_member(select_stmt->get_current_level())) {
        // do nothing
      } else if (OB_FAIL(table_ids.add_members2(condition_expr->get_relation_ids()))) {
        LOG_WARN("failed to collect condition expr table ids", K(ret));
      } else if (OB_FAIL(extracted_conditions.push_back(condition_expr))) {
        // exclude startup filters
        LOG_WARN("failed to push back condition expr", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::collect_orderby_table_ids(
    ObSelectStmt* select_stmt, ObSqlBitSet<>& table_ids, bool& is_valid_orderby)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  is_valid_orderby = true;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!select_stmt->has_order_by()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid_orderby && i < select_stmt->get_order_item_size(); ++i) {
      OrderItem item = select_stmt->get_order_items().at(i);
      OrderItem new_item;
      ObRawExpr* order_expr = item.expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid orderby expr", K(ret));
      } else if (order_expr->has_flag(CNT_RAND_FUNC) || order_expr->has_flag(CNT_STATE_FUNC) ||
                 order_expr->has_flag(CNT_SUB_QUERY)) {
        // avoid pushing down non-deterministic func and subquery
        is_valid_orderby = false;
      } else if (!order_expr->get_expr_levels().has_member(select_stmt->get_current_level())) {
        // do nothing
      } else if (OB_FAIL(table_ids.add_members2(order_expr->get_relation_ids()))) {
        LOG_WARN("failed to collect orderby table sets", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::find_target_table(
    ObSelectStmt* select_stmt, ObSqlBitSet<> table_ids, TableItem*& target_table)
{
  int ret = OB_SUCCESS;
  bool is_top_joinedtable = false;
  ObJoinType joined_type = LEFT_OUTER_JOIN;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else {
    const FromItem from_item = select_stmt->get_from_item(0);
    TableItem* table_item = select_stmt->get_table_item(from_item);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table item", K(ret));
    } else {
      JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
      // find target joined_table based on table_ids
      TableItem* table = joined_table;
      bool found = false;
      // this rule happens before outer join reorder, maybe it contains any
      // reordered order meets the table_ids requirement.
      // we ignore this consideration and only follow the fixed rule match currently.
      while (OB_SUCC(ret) && OB_NOT_NULL(table) && !found) {
        // find target joined_table based on order by column relation_ids
        ObSqlBitSet<> cur_table_ids;
        if (OB_FAIL(select_stmt->get_table_rel_ids(*table, cur_table_ids))) {
          LOG_WARN("failed to get table rel ids", K(ret));
        } else if (cur_table_ids.equal(table_ids)) {
          found = true;
          // if only the top joined table meets the requirment,
          // no need to do the transformation.
          is_top_joinedtable = (joined_table == table);
        } else if (table->is_joined_table()) {
          table = static_cast<JoinedTable*>(table)->left_table_;
        } else if (table_ids.is_empty()) {
          // no orderby and where condition, find the left deep table
          found = true;
        } else {
          break;
        }
      }
      if (OB_SUCC(ret) && found && !is_top_joinedtable) {
        target_table = table;
      }
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::check_validity_for_target_table(OjLimitPushDownHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSelectStmt* ref_query = NULL;
  bool has_rownum = false;
  if (OB_ISNULL(helper.target_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid target table item", K(ret));
  } else if (helper.target_table_->is_basic_table() || helper.target_table_->is_joined_table()) {
    // do nothing
  } else if (helper.target_table_->is_generated_table()) {
    if (OB_ISNULL(ref_query = helper.target_table_->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid target table ref query", K(ret));
    } else if (ref_query->has_window_function() || ref_query->is_hierarchical_query() || ref_query->has_sequence() ||
               ref_query->is_calc_found_rows() || ref_query->has_order_by() || ref_query->has_limit()) {
      // ignore push down when ref_query has (orderby) limit
      is_valid = false;
    } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (has_rownum) {
      is_valid = false;
    } else {
      if (ref_query->is_set_stmt()) {
        helper.need_create_view_ = true;
      } else {
        // no need to create new view
        helper.need_create_view_ = false;
      }
      helper.need_rename_ = true;
      helper.view_table_ = helper.target_table_;
    }
  } else {
    is_valid = false;
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::do_transform(OjLimitPushDownHelper& helper)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *ref_query = NULL;
  if (OB_ISNULL(helper.select_stmt_) || OB_ISNULL(helper.target_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter in helper", K(ret));
  } else if (OB_FAIL(remove_and_copy_condition_orderby(helper.select_stmt_,
                                                       helper.extracted_conditions_,
                                                       helper.saved_order_items_))) {
    LOG_WARN("failed to do remove and copy for condition and orderby", K(ret));
  } else if (helper.need_create_view_ &&
             OB_FAIL(ObTransformUtils::create_view_with_table(helper.select_stmt_,
                                                              ctx_,
                                                              helper.target_table_,
                                                              helper.view_table_))) {
    LOG_WARN("failed to prepare new view for pushing down", K(ret));
  } else if (OB_ISNULL(helper.view_table_) ||
            !helper.view_table_->is_generated_table() ||
            OB_ISNULL(ref_query = helper.view_table_->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid view table", K(ret));
  } else if (OB_FAIL(pushdown_view_table(helper.select_stmt_,
                                        helper.view_table_,
                                        helper.extracted_conditions_,
                                        helper.saved_order_items_,
                                        helper.need_rename_,
                                        helper.is_limit_only_))) {
    LOG_WARN("failed to push down view table", K(ret));
  } else { /* do nothing */
  }

  if (OB_SUCC(ret) && OB_FAIL(helper.select_stmt_->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize generated_view", K(ret));
  } else { /* do nothing */
  }

  return ret;
}

int ObTransformOuterJoinLimitPushDown::remove_and_copy_condition_orderby(
    ObSelectStmt* stmt, ObIArray<ObRawExpr*>& extracted_conditions, ObIArray<OrderItem>& saved_order_items)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_condition_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), extracted_conditions))) {
    LOG_WARN("failed to remove extracted conditions from stmt", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 *(ctx_->expr_factory_), extracted_conditions, new_condition_exprs, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
    // remove extracted_conditions first and then do the copy.
    // re-assign extracted_conditions in helper with copied new_condition_exprs.
  } else if (OB_FAIL(extracted_conditions.assign(new_condition_exprs))) {
    LOG_WARN("failed to reassign helper conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_order_items(
                 *(ctx_->expr_factory_), stmt->get_order_items(), saved_order_items, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to do orderby item copy", K(ret));
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::pushdown_view_table(ObSelectStmt* stmt, TableItem* view_table,
    ObIArray<ObRawExpr*>& extracted_conditions, ObIArray<OrderItem>& saved_order_items, bool need_rename,
    bool is_limit_only)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* ref_query = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_ISNULL(ref_query = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected ref query", K(ret));
  } else if (OB_FAIL(add_condition_expr_for_viewtable(ref_query, extracted_conditions, need_rename))) {
    LOG_WARN("failed to add condition expr for original view", K(ret));
  } else if (!is_limit_only && OB_FAIL(add_orderby_for_viewtable(ref_query, stmt, saved_order_items, need_rename))) {
    LOG_WARN("failed to add order by limit for original view", K(ret));
  } else if (OB_FAIL(add_limit_for_viewtable(ref_query, stmt))) {
    LOG_WARN("failed to add limit for generated view table", K(ret));
  } else if (OB_FAIL(ref_query->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize generated_view", K(ret));
  } else { /* do nothing */
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::add_condition_expr_for_viewtable(
    ObSelectStmt* generated_view, ObIArray<ObRawExpr*>& extracted_conditions, bool need_rename)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(generated_view)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid generated_view", K(ret));
  } else if (need_rename) {
    // renaming for saved condition expr from upper level
    // after pushdown into generated_view condition exprs.
    ObSEArray<ObRawExpr*, 16> old_column_exprs;
    ObSEArray<ObRawExpr*, 16> new_column_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < extracted_conditions.count(); ++i) {
      old_column_exprs.reuse();
      new_column_exprs.reuse();
      ObRawExpr* expr = extracted_conditions.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid condition", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, old_column_exprs))) {
        LOG_WARN("failed to extract column expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                     old_column_exprs, *generated_view, new_column_exprs))) {
        LOG_WARN("failed to convert columnexpr to select expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(old_column_exprs, new_column_exprs, expr))) {
        LOG_WARN("failed to replace expr for condition expr", K(ret));
      } else { /* do nothing */
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(generated_view->get_condition_exprs(), extracted_conditions))) {
      LOG_WARN("failed to append condition exprs back", K(ret));
    } else {/* do nothing */}
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::add_orderby_for_viewtable(
    ObSelectStmt* generated_view, ObSelectStmt* upper_stmt, ObIArray<OrderItem>& saved_order_items, bool need_rename)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(generated_view) || OB_ISNULL(upper_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (need_rename) {
    // order by column needs to be renamed before moving
    // from upper_stmt to inner generated_view.
    ObSEArray<ObRawExpr*, 16> old_order_exprs;
    ObSEArray<ObRawExpr*, 16> new_order_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < saved_order_items.count(); i++) {
      old_order_exprs.reuse();
      new_order_exprs.reuse();
      if (OB_ISNULL(saved_order_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(saved_order_items.at(i).expr_,
                                                              old_order_exprs))) {
        LOG_WARN("failed to extract column expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(old_order_exprs,
                                                                              *generated_view,
                                                                              new_order_exprs))) {
        LOG_WARN("failed to convert columnexpr to select expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(old_order_exprs,
                                                        new_order_exprs,
                                                        saved_order_items.at(i).expr_))) {
        LOG_WARN("failed to replace expr for order item", K(ret));
      } else {/* do nothing */
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(generated_view->get_order_items(), saved_order_items))) {
      LOG_WARN("failed to append order item", K(ret));
    } else {/* do nothing */
    }
  }
  return ret;
}

int ObTransformOuterJoinLimitPushDown::add_limit_for_viewtable(ObSelectStmt* generated_view, ObSelectStmt* upper_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(generated_view) || OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else {
    ObRawExpr* offset_expr = upper_stmt->get_offset_expr();
    ObRawExpr* limit_expr = upper_stmt->get_limit_expr();
    if (OB_ISNULL(limit_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("illegal limit expr", K(ret));
    } else if (NULL == offset_expr) {
      generated_view->set_limit_offset(upper_stmt->get_limit_expr(), NULL);
    } else {
      ObRawExpr* new_limit_count_expr = NULL;
      // need to cast result to integer in static typing engine
      if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(
              *ctx_->expr_factory_, *ctx_->session_info_, limit_expr, offset_expr, new_limit_count_expr))) {
        LOG_WARN("make pushdown limit expr failed", K(ret));
      } else {
        generated_view->set_limit_offset(new_limit_count_expr, NULL);
      }
      generated_view->set_limit_percent_expr(NULL);
      generated_view->set_fetch_with_ties(false);
      generated_view->set_has_fetch(upper_stmt->has_fetch());
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

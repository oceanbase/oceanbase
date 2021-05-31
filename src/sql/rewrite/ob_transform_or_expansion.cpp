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
#include "sql/rewrite/ob_transform_or_expansion.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
namespace sql {
const int64_t ObTransformOrExpansion::MAX_STMT_NUM_FOR_OR_EXPANSION = 10;
const int64_t ObTransformOrExpansion::MAX_TIMES_FOR_OR_EXPANSION = 5;

int ObTransformOrExpansion::transform_one_stmt(
    ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool is_topk = false;
  ObDMLStmt* trans_stmt = NULL;
  ObDMLStmt* upper_stmt = NULL;
  ObSelectStmt* spj_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> generated_exprs;
  ObSEArray<ObRawExpr*, 4> conds;
  ObSEArray<int64_t, 4> expr_pos;
  ObSEArray<int64_t, 4> or_expr_counts;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (try_times_ >= MAX_TIMES_FOR_OR_EXPANSION) {
    // do nothing
  } else if (OB_FAIL(check_stmt_validity(*stmt, is_valid))) {
    LOG_WARN("failed to check stmt validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(get_trans_view(stmt, upper_stmt, spj_stmt))) {
    LOG_WARN("failed to get spj stmt", K(ret));
  } else {
    is_topk = stmt->has_order_by() && stmt->has_limit();
    if (OB_FAIL(prepare_or_condition(spj_stmt, conds, expr_pos, or_expr_counts, is_topk))) {
      LOG_WARN("failed to prepare or condition", K(ret));
    } else if (conds.count() != or_expr_counts.count() || conds.count() != expr_pos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected or expr counts and conditions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < conds.count(); ++i) {
      ObSelectStmt* transformed_union_stmt = NULL;
      bool can_union_distinct = false;
      bool spj_is_unique = false;
      bool skip_this_cond = false;
      trans_stmt = upper_stmt;
      if (OB_FAIL(preprocess_or_condition(
              spj_stmt, conds.at(i), can_union_distinct, spj_is_unique, skip_this_cond, is_topk))) {
        LOG_WARN("failed to preprocess or condition", K(ret));
      } else if (skip_this_cond) {
        /*do nothing*/
      } else if (OB_FAIL(transform_or_expansion(parent_stmts,
                     spj_stmt,
                     expr_pos.at(i),
                     or_expr_counts.at(i),
                     can_union_distinct,
                     spj_is_unique,
                     transformed_union_stmt))) {
        LOG_WARN("failed to do transformation", K(ret));
      } else if (OB_FAIL(merge_stmt(trans_stmt, spj_stmt, transformed_union_stmt))) {
        LOG_WARN("failed to merge stmt", K(ret));
      } else if (spj_stmt->get_stmt_hint().enable_use_concat()) {
        ++try_times_;
        stmt = trans_stmt;
        trans_happened = true;
        cost_based_trans_tried_ = true;
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, trans_happened))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else {
        ++try_times_;
      }
      if (OB_SUCC(ret) && trans_happened) {
        ctx_->happened_cost_based_trans_ |= OR_EXPANSION;
        LOG_TRACE("has use concat hint, after or expansion", K(*stmt));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::adjust_transform_types(uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~transformer_type_);
  }
  return ret;
}

int ObTransformOrExpansion::has_odd_function(const ObDMLStmt& stmt, bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (stmt.is_select_stmt()) {
    const ObSelectStmt& select_stmt = static_cast<const ObSelectStmt&>(stmt);
    has = (select_stmt.is_contains_assignment() || NULL != select_stmt.get_select_into() ||
           !select_stmt.get_cte_exprs().empty() || !select_stmt.get_cycle_items().empty() ||
           !select_stmt.get_search_by_items().empty() || select_stmt.has_materalized_view());
  }
  if (OB_SUCC(ret) && !has) {
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmt", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(has_odd_function(*child_stmts.at(i), has)))) {
        LOG_WARN("failed to check has odd function", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_stmt_validity(ObDMLStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_odd_func = false;
  bool contain_inner_table = false;
  bool check_status = true;
  is_valid = false;
  if (stmt.get_stmt_hint().enable_no_expand()) {
    is_valid = false;
    LOG_TRACE("has no expand hint.", K(is_valid));
  } else if (stmt.is_set_stmt() || stmt.get_from_item_size() == 0 || !stmt.is_sel_del_upd()) {
    // do nothing
  } else if (OB_FAIL(stmt.check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check if contain inner table", K(ret));
  } else if (contain_inner_table && !stmt.get_stmt_hint().enable_use_concat()) {
    // do not rewrite a inner table stmt with a cost-based rule
  } else if ((stmt.is_update_stmt() || stmt.is_delete_stmt()) &&
             OB_FAIL(check_upd_del_stmt_validity(static_cast<ObDelUpdStmt*>(&stmt), check_status))) {
    LOG_WARN("failed to check upd del stmt validity", K(ret));
  } else if (!check_status) {
    /*do nothing */
  } else if (stmt.is_select_stmt() &&
             OB_FAIL(check_select_expr_validity(static_cast<ObSelectStmt&>(stmt), check_status))) {
    LOG_WARN("failed to check select expr validity", K(ret));
  } else if (!check_status) {
    /*do nothing */
  } else if (stmt.is_hierarchical_query()) {
    /*do nothing */
  } else if (OB_FAIL(has_odd_function(stmt, has_odd_func))) {
    LOG_WARN("failed to check has odd function", K(ret));
  } else if (has_odd_func) {
    // do nothing
  } else {
    // pre-check conditions
    int64_t or_expr_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < stmt.get_condition_size(); ++i) {
      if (OB_ISNULL(stmt.get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret));
      } else if (OB_FAIL(is_condition_valid(stmt,
                     *stmt.get_condition_expr(i),
                     or_expr_cnt,
                     is_valid,
                     stmt.has_order_by() && stmt.has_limit()))) {
        LOG_WARN("failed to check condition validity", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_select_expr_validity(ObSelectStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObIArray<SelectItem>& select_items = stmt.get_select_items();
  ObRawExpr* select_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < select_items.count(); ++i) {
    if (OB_ISNULL(select_expr = select_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret), K(select_items.at(i)));
    } else if (ObLongTextType == select_expr->get_data_type() || ObLobType == select_expr->get_data_type()) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_upd_del_stmt_validity(ObDelUpdStmt* stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have invalid params", K(ret), K(stmt));
  } else {
    const ObIArray<TableColumns>& table_columns = stmt->get_all_table_columns();
    if (1 != table_columns.count()) {
      is_valid = false;
    } else {
      const ObIArray<ColumnItem>& column_items = stmt->get_column_items();
      ObRawExpr* column_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < column_items.count(); ++i) {
        if (OB_ISNULL(column_expr = column_items.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret), K(column_items.at(i)));
        } else if (ObLongTextType == column_expr->get_data_type() || ObLobType == column_expr->get_data_type()) {
          is_valid = false;
        }
      }
      const ObIArray<IndexDMLInfo>& index_infos = table_columns.at(0).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < index_infos.count(); ++i) {
        if (NULL != stmt->get_part_expr(index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_)) {
          is_valid = false;
        }
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::get_trans_view(ObDMLStmt* stmt, ObDMLStmt*& upper_stmt, ObSelectStmt*& child_stmt)
{
  int ret = OB_SUCCESS;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  child_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) || OB_UNLIKELY(!stmt->is_sel_del_upd())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "have invalid params", K(ret), K(ctx_), K(stmt), K(stmt_factory), K(expr_factory), K(stmt->is_sel_del_upd()));
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_spj()) {
    upper_stmt = stmt;
    child_stmt = static_cast<ObSelectStmt*>(stmt);
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*stmt_factory, *expr_factory, stmt, upper_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(upper_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(upper_stmt));
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, upper_stmt, child_stmt, false))) {
    LOG_WARN("failed to create simple view", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::merge_stmt(ObDMLStmt*& upper_stmt, ObSelectStmt* input_stmt, ObSelectStmt* union_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(union_stmt) || OB_ISNULL(input_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(upper_stmt), K(union_stmt), K(input_stmt));
  } else if (upper_stmt == input_stmt) {
    int64_t origin_select_item_count = input_stmt->get_select_item_size();
    if (origin_select_item_count == union_stmt->get_select_item_size()) {
      upper_stmt = union_stmt;
    } else {
      ObSelectStmt* temp_stmt = NULL;
      if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, union_stmt, temp_stmt))) {
        LOG_WARN("create stmt with generated_table failed", K(ret));
      } else if (OB_FAIL(remove_stmt_select_item(temp_stmt, origin_select_item_count))) {
        LOG_WARN("just stmt select item failed", K(ret));
      } else {
        upper_stmt = temp_stmt;
      }
    }
  } else if (OB_UNLIKELY(upper_stmt->get_table_size() != 1) || OB_ISNULL(upper_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid upper stmt", K(ret));
  } else {
    upper_stmt->get_table_item(0)->ref_query_ = union_stmt;
  }
  return ret;
}

int ObTransformOrExpansion::remove_stmt_select_item(ObSelectStmt* select_stmt, int64_t select_item_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || select_item_count < 0 || select_stmt->get_select_item_size() < select_item_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(select_stmt), K(select_item_count), K(select_stmt->get_select_item_size()), K(ret));
  } else {
    int64_t i_select_item = select_stmt->get_select_item_size();
    do {
      if (OB_FAIL(select_stmt->get_select_items().remove(i_select_item - 1))) {
        LOG_WARN("remove select item failed", K(ret));
      } else {
        --i_select_item;
      }
    } while (OB_SUCC(ret) && i_select_item != select_item_count);
  }
  return ret;
}

int ObTransformOrExpansion::check_condition_on_same_columns(
    const ObDMLStmt& stmt, const ObRawExpr& expr, bool& using_same_cols)
{
  int ret = OB_SUCCESS;
  using_same_cols = false;
  if (T_OP_OR == expr.get_expr_type()) {
    int64_t table_id = OB_INVALID_ID;
    ColumnBitSet column_bit_set;
    using_same_cols = true;
    for (int64_t i = 0; OB_SUCC(ret) && using_same_cols && i < expr.get_param_count(); ++i) {
      bool from_same_table = true;
      ColumnBitSet tmp;
      if (OB_FAIL(extract_columns(expr.get_param_expr(i), stmt.get_current_level(), table_id, from_same_table, tmp))) {
        LOG_WARN("failed to extract columns info", K(ret));
      } else if (!from_same_table) {
        using_same_cols = false;
      } else if (0 == i) {
        column_bit_set.add_members(tmp);
        using_same_cols = column_bit_set.bit_count() > 0;
      } else if (!column_bit_set.equal(tmp)) {
        using_same_cols = false;
      }
    }
  } else if (T_OP_IN == expr.get_expr_type() && OB_NOT_NULL(expr.get_param_expr(1)) &&
             T_OP_ROW == expr.get_param_expr(1)->get_expr_type()) {
    using_same_cols = true;
  }
  return ret;
}

int ObTransformOrExpansion::extract_columns(const ObRawExpr* expr, const int64_t stmt_level, int64_t& table_id,
    bool& from_same_table, ColumnBitSet& col_bit_set)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_column_ref_expr()) {
    if (expr->get_expr_level() == stmt_level) {
      const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(expr);
      if (OB_INVALID_ID == table_id) {
        table_id = col_expr->get_table_id();
      }
      if (table_id != col_expr->get_table_id()) {
        from_same_table = false;
      } else if (OB_FAIL(col_bit_set.add_member(col_expr->get_column_id()))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(
              extract_columns(expr->get_param_expr(i), stmt_level, table_id, from_same_table, col_bit_set)))) {
        LOG_WARN("failed to extract columns", K(ret));
      } else if (!from_same_table) {
        break;
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_contain_join_cond(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->has_flag(IS_JOIN_COND)) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_correlated_expr(expr, stmt.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      is_contain = true;
    }
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_contain_join_cond(stmt, expr->get_param_expr(i), is_contain)))) {
        LOG_WARN("failed to check is contain join cond", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_match_index(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (expr->has_flag(IS_SIMPLE_COND) || expr->has_flag(IS_RANGE_COND)) {
    ObSEArray<ObRawExpr*, 2> column_exprs;
    ObColumnRefRawExpr* col_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
      LOG_WARN("failed to extrace column exprs", K(ret));
    } else if (1 != column_exprs.count()) {
      // do nothing
    } else if (OB_ISNULL(column_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!column_exprs.at(0)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect column ref expr", K(*column_exprs.at(0)), K(ret));
    } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(0)))) {
    } else if (OB_ISNULL(stmt.get_table_item_by_id(col_expr->get_table_id()))) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_match_index(
                   ctx_->sql_schema_guard_, &stmt, col_expr, is_match, &ctx_->equal_sets_))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    const ObRawExpr* right_expr = NULL;
    if (OB_UNLIKELY(2 != expr->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in expr should have 2 param", K(expr->get_param_count()), K(ret));
    } else if (OB_ISNULL(right_expr = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (T_OP_ROW == right_expr->get_expr_type()) {
      bool is_const = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_const && i < right_expr->get_param_count(); i++) {
        const ObRawExpr* temp_expr = right_expr->get_param_expr(i);
        if (OB_ISNULL(temp_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (!temp_expr->is_const_expr()) {
          is_const = false;
        } else { /*do nothing*/
        }
      }
      const ObColumnRefRawExpr* col_expr = NULL;
      if (OB_FAIL(ret) || !is_const) {
        // do nothing
      } else if (OB_ISNULL(expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->get_param_expr(0)->is_column_ref_expr()) {
        // do nothing
      } else if (OB_FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr*>(expr->get_param_expr(0)))) {
      } else if (OB_ISNULL(stmt.get_table_item_by_id(col_expr->get_table_id()))) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_match_index(
                     ctx_->sql_schema_guard_, &stmt, col_expr, is_match, &ctx_->equal_sets_))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
    } else { /*do nothing*/
    }
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_match_index(stmt, expr->get_param_expr(i), is_match)))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_simple_cond(const ObDMLStmt& stmt, const ObRawExpr* expr, bool& is_simple)
{
  int ret = OB_SUCCESS;
  is_simple = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (1 >= expr->get_relation_ids().num_members()) {
    is_simple = true;
  } else if (T_OP_AND == expr->get_expr_type()) {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_simple && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(is_simple_cond(stmt, expr->get_param_expr(i), is_simple)))) {
        LOG_WARN("failed to check is simple cond", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_condition_valid(
    const ObDMLStmt& stmt, const ObRawExpr& expr, int64_t& or_expr_count, bool& is_valid, const bool is_topk)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  or_expr_count = 0;
  bool using_same_col = false;
  if (expr.has_flag(CNT_ROWNUM)) {
    // do nothing
  } else if (T_OP_OR != expr.get_expr_type() && T_OP_IN != expr.get_expr_type()) {
    // do nothing
  } else if (!is_topk && OB_FAIL(check_condition_on_same_columns(stmt, expr, using_same_col))) {
    LOG_WARN("failed to check condition on same colums", K(ret));
  } else if (!using_same_col || is_topk || expr.has_flag(CNT_SUB_QUERY)) {
    /**
     * Given an order-by + limit query,
     * it possible to push limit down to down by after the query is expanded
     * select * from t where a = 1 or a = 2 order by b limit 10;
     * =>
     * (select * from t where a = 1 order by b limit 10)
     * union all
     * (select * from t where a = 2 and lnnvl(a=1) order by b limit 10)
     * order by b limit 10;
     *
     * when there is an index (a, b), then we can make use of the index order
     * */
    if (T_OP_OR == expr.get_expr_type() && expr.get_param_count() <= MAX_STMT_NUM_FOR_OR_EXPANSION) {
      bool is_all_simple_cond = true;
      for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < expr.get_param_count(); i++) {
        const ObRawExpr* temp_expr = expr.get_param_expr(i);
        bool is_contain = false;
        bool is_match = false;
        bool is_simple = false;
        if (OB_ISNULL(temp_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (temp_expr->has_flag(CNT_SUB_QUERY)) {
          is_valid = true;
        } else if (OB_FAIL(is_contain_join_cond(stmt, temp_expr, is_contain))) {
          LOG_WARN("failed to check contain join cond", K(ret));
        } else if (is_contain) {
          is_valid = true;
        } else if (OB_FAIL(is_match_index(stmt, temp_expr, is_match))) {
          LOG_WARN("failed to check is match index", K(ret));
        } else if (is_match) {
          is_valid = true;
        } else if (OB_FAIL(is_simple_cond(stmt, temp_expr, is_simple))) {
          LOG_WARN("failed to check is simple cond", K(ret));
        } else if (!is_simple) {
          is_all_simple_cond = false;
        }
      }
      if (is_all_simple_cond && !is_valid) {
        if (expr.get_relation_ids().num_members() > 1) {
          is_valid = true;
        }
      }
      if (is_valid || stmt.get_stmt_hint().enable_use_concat()) {
        or_expr_count = expr.get_param_count();
        is_valid = true;
      }
    } else if (T_OP_IN == expr.get_expr_type() && !expr.has_flag(CNT_SUB_QUERY)) {
      const ObRawExpr* right_expr = NULL;
      if (OB_UNLIKELY(2 != expr.get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in expr should have 2 param", K(expr.get_param_count()), K(ret));
      } else if (OB_ISNULL(right_expr = expr.get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (T_OP_ROW == right_expr->get_expr_type() &&
                 right_expr->get_param_count() <= MAX_STMT_NUM_FOR_OR_EXPANSION) {
        or_expr_count = right_expr->get_param_count();
        is_valid = true;
        for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < right_expr->get_param_count(); i++) {
          const ObRawExpr* temp_expr = right_expr->get_param_expr(i);
          if (OB_ISNULL(temp_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null expr", K(ret));
          } else if (!temp_expr->has_flag(IS_CONST)) {
            or_expr_count = 0;
            is_valid = false;
          } else { /*do nothing*/
          }
        }
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }
  LOG_TRACE("finish to check transform validity", K(expr.get_expr_type()), K(is_valid), K(ret));
  return ret;
}

int ObTransformOrExpansion::transform_or_expansion(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt,
    const int64_t transformed_expr_pos, const int64_t or_expr_count, bool can_union_distinct, bool spj_is_unique,
    ObSelectStmt*& transformed_stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ctx_), K(session_info), K(stmt_factory), K(expr_factory), K(ret));
  } else if (OB_UNLIKELY(or_expr_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("or expr should large than 0", K(or_expr_count), K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* input_stmt = static_cast<ObSelectStmt*>(stmt);
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    ObSelectStmt* union_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < or_expr_count; i++) {
      ObSelectStmt* child_stmt = NULL;
      if (OB_FAIL(stmt_factory->create_stmt(child_stmt)) || OB_ISNULL(child_stmt)) {
        LOG_WARN("failed to create stmt factory", K(child_stmt), K(ret));
      } else if (OB_FAIL(child_stmt->deep_copy(*stmt_factory, *expr_factory, *input_stmt))) {
        LOG_WARN("failed to deep copy child statement", K(ret));
      } else if (OB_FAIL(child_stmt->update_stmt_table_id(*input_stmt))) {
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FAIL(adjust_or_expansion_stmt(transformed_expr_pos, i, can_union_distinct, child_stmt))) {
        LOG_WARN("failed to adjust children stmt", K(ret));
      } else if (can_union_distinct && !spj_is_unique &&
                 OB_FAIL(ObTransformUtils::recursive_set_stmt_unique(child_stmt, ctx_, true))) {
        LOG_WARN("failed to set stmt unique", K(ret));
      } else if (OB_FAIL(child_stmts.push_back(child_stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::create_union_stmt(ctx_, can_union_distinct, child_stmts, union_stmt))) {
      LOG_WARN("failed to create union stmt", K(union_stmt), K(ret));
    } else if (OB_ISNULL(union_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(union_stmt));
    } else if (OB_FAIL(union_stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      transformed_stmt = union_stmt;
    }
  }
  return ret;
}

int ObTransformOrExpansion::adjust_or_expansion_stmt(const int64_t transformed_expr_pos, const int64_t param_pos,
    bool can_union_distinct, ObSelectStmt*& or_expansion_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObRawExpr* transformed_expr = NULL;
  ObSEArray<ObQueryRefRawExpr*, 4> removed_subqueries;
  ObSEArray<ObQueryRefRawExpr*, 4> need_add_subqueries;
  if (OB_ISNULL(ctx_) || OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ctx_), K(session_info), K(expr_factory), K(ret));
  } else if (OB_ISNULL(or_expansion_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_UNLIKELY(transformed_expr_pos < 0) ||
             OB_UNLIKELY(transformed_expr_pos >= or_expansion_stmt->get_condition_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition expr pos",
        K(transformed_expr_pos),
        K(or_expansion_stmt->get_condition_exprs().count()),
        K(ret));
  } else if (OB_ISNULL(transformed_expr = or_expansion_stmt->get_condition_exprs().at(transformed_expr_pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null transformed expr", K(ret));
  } else if (OB_FAIL(or_expansion_stmt->get_condition_exprs().remove(transformed_expr_pos))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (transformed_expr->has_flag(CNT_SUB_QUERY) &&
             OB_FAIL(ObTransformUtils::extract_query_ref_expr(transformed_expr, removed_subqueries))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(or_expansion_stmt->get_subquery_exprs(), removed_subqueries))) {
    LOG_WARN("failed to remove subquery exprs", K(ret));
  } else if (T_OP_OR == transformed_expr->get_expr_type()) {
    ObSEArray<ObRawExpr*, 4> generated_exprs;
    if (OB_FAIL(create_expr_for_or_expr(
            *transformed_expr, param_pos, can_union_distinct, generated_exprs, need_add_subqueries))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(append(or_expansion_stmt->get_condition_exprs(), generated_exprs))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append(or_expansion_stmt->get_subquery_exprs(), need_add_subqueries))) {
      LOG_WARN("failed to append expr", K(ret));
    } else { /*do nothing*/
    }
  } else if (T_OP_IN == transformed_expr->get_expr_type()) {
    ObSEArray<ObRawExpr*, 4> generated_exprs;
    if (OB_FAIL(create_expr_for_in_expr(
            *transformed_expr, param_pos, can_union_distinct, generated_exprs, need_add_subqueries))) {
      LOG_WARN("failed to create expr", K(ret), K(generated_exprs));
    } else if (OB_FAIL(append(or_expansion_stmt->get_condition_exprs(), generated_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(or_expansion_stmt->get_subquery_exprs(), need_add_subqueries))) {
      LOG_WARN("failed to append expr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(transformed_expr->get_expr_type()), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(or_expansion_stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformOrExpansion::create_expr_for_in_expr(const ObRawExpr& transformed_expr, const int64_t param_pos,
    bool can_union_distinct, ObIArray<ObRawExpr*>& generated_exprs, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* left_expr = NULL;
  const ObRawExpr* right_expr = NULL;
  ObRawExpr* temp_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx_), K(session_info), K(expr_factory), K(ret));
  } else if (OB_UNLIKELY(T_OP_IN != transformed_expr.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(transformed_expr.get_expr_type()), K(ret));
  } else if (OB_UNLIKELY(2 != transformed_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in expr should have 2 param", K(transformed_expr.get_param_count()), K(ret));
  } else if (OB_ISNULL(left_expr = transformed_expr.get_param_expr(0)) ||
             OB_ISNULL(right_expr = transformed_expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(left_expr), K(right_expr), K(ret));
  } else if (T_OP_ROW != right_expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(right_expr->get_expr_type()), K(ret));
  } else if (OB_UNLIKELY(param_pos < 0) || OB_UNLIKELY(param_pos >= right_expr->get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr pos", K(param_pos), K(right_expr->get_param_count()), K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(const_cast<ObRawExpr*>(left_expr), subqueries))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  }
  for (int64_t i = can_union_distinct ? param_pos : 0; OB_SUCC(ret) && i <= param_pos; ++i) {
    if (OB_FAIL(ObRawExprUtils::create_equal_expr(
            *expr_factory, session_info, left_expr, right_expr->get_param_expr(i), temp_expr))) {
      LOG_WARN("failed to create double op expr", K(ret));
    } else if (i == param_pos) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, temp_expr, temp_expr))) {
      LOG_WARN("failed to build lnnvl expr", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(generated_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back temp expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::create_expr_for_or_expr(ObRawExpr& transformed_expr, const int64_t param_pos,
    bool can_union_distinct, common::ObIArray<ObRawExpr*>& generated_exprs, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ctx_), K(expr_factory), K(ret));
  } else if (OB_UNLIKELY(T_OP_OR != transformed_expr.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(transformed_expr.get_expr_type()), K(ret));
  } else if (OB_UNLIKELY(param_pos < 0) || OB_UNLIKELY(param_pos >= transformed_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr pos", K(param_pos), K(transformed_expr.get_param_count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i <= param_pos; i++) {
      ObRawExpr* expr = NULL;
      ObRawExpr* temp_expr = NULL;
      if (OB_ISNULL(expr = transformed_expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (i == param_pos) {
        temp_expr = expr;
        if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, subqueries))) {
          LOG_WARN("failed to extract query ref expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::flatten_expr(temp_expr, generated_exprs))) {
          LOG_WARN("failed to flatten expr", K(ret));
        } else { /*do nothing*/
        }
      } else if (can_union_distinct) {
        /*do nothing */
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, subqueries))) {
        LOG_WARN("failed to extract query ref expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, expr, temp_expr))) {
        LOG_WARN("failed to create lnnvl expr", K(ret));
      } else if (OB_FAIL(generated_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::prepare_or_condition
 * prepare or expansion condition,find out conditions which can be expanded and record or expr counts.
 * conditions contain sub query placed at head of output arrays, transform first.
 */
int ObTransformOrExpansion::prepare_or_condition(ObSelectStmt* stmt, common::ObIArray<ObRawExpr*>& conds,
    common::ObIArray<int64_t>& expr_pos, common::ObIArray<int64_t>& or_expr_counts, const bool is_topk)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> temp_conds;
  ObSEArray<int64_t, 4> temp_expr_pos;
  ObSEArray<int64_t, 4> temp_or_expr_counts;
  ObRawExpr* cond = NULL;
  int64_t or_expr_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt condition is null", K(ret));
    } else if (OB_FAIL(is_condition_valid(*stmt, *cond, or_expr_count, is_valid, is_topk))) {
      LOG_WARN("failed to check condition is valid", K(ret));
    } else if (is_valid && cond->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(conds.push_back(cond))) {
        LOG_WARN("failed to push back conds", K(ret));
      } else if (OB_FAIL(expr_pos.push_back(i))) {
        LOG_WARN("failed to push back expr pos", K(ret));
      } else if (OB_FAIL(or_expr_counts.push_back(or_expr_count))) {
        LOG_WARN("failed to push back or expr counts", K(ret));
      }
    } else if (is_valid) {
      if (OB_FAIL(temp_conds.push_back(cond))) {
        LOG_WARN("failed to push back temp conds", K(ret));
      } else if (OB_FAIL(temp_expr_pos.push_back(i))) {
        LOG_WARN("failed to push back temp expr pos", K(ret));
      } else if (OB_FAIL(temp_or_expr_counts.push_back(or_expr_count))) {
        LOG_WARN("failed to push back temp or expr counts", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(conds, temp_conds))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else if (OB_FAIL(append(expr_pos, temp_expr_pos))) {
      LOG_WARN("failed to append expr pos", K(ret));
    } else if (OB_FAIL(append(or_expr_counts, temp_or_expr_counts))) {
      LOG_WARN("failed to append or expr counts", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::preprocess_or_condition
 * move params with subquery to the tail of the expr, and judge can do union distinct
 */
int ObTransformOrExpansion::preprocess_or_condition(ObSelectStmt* select_stmt, ObRawExpr* expr,
    bool& can_union_distinct, bool& spj_is_unique, bool& skip_this_cond, const bool is_topk)
{
  int ret = OB_SUCCESS;
  int64_t tail = 0;
  int num_sub = 0;
  bool using_same_col = false;
  can_union_distinct = false;
  spj_is_unique = false;
  skip_this_cond = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(expr), K(ctx_), K(ret));
  } else if (OB_FAIL(check_condition_on_same_columns(*select_stmt, *expr, using_same_col))) {
    LOG_WARN("failed to check condition on same colums", K(ret));
  } else if (T_OP_OR == expr->get_expr_type()) {
    tail = expr->get_param_count() - 1;
    for (int64_t i = expr->get_param_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObRawExpr* param = NULL;
      if (OB_ISNULL(param = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (param->has_flag(CNT_SUB_QUERY)) {
        expr->get_param_expr(i) = expr->get_param_expr(tail);
        expr->get_param_expr(tail--) = param;
        ++num_sub;
      }
    }
  }
  if (OB_SUCC(ret) && ((is_topk && using_same_col) || num_sub >= 2)) {
    if (OB_FAIL(ObTransformUtils::check_can_set_stmt_unique(select_stmt, can_union_distinct))) {
      LOG_WARN("failed to check can set stmt unique", K(ret));
    } else if (can_union_distinct &&
               OB_FAIL(ObTransformUtils::check_stmt_unique(
                   select_stmt, ctx_->session_info_, ctx_->schema_checker_, true /* strict */, spj_is_unique))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    } else { /*do nothing */
    }
  }
  if (OB_SUCC(ret) && is_topk && using_same_col && !can_union_distinct) {
    skip_this_cond = true;
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

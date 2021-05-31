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
#include "ob_transform_aggregate.h"
#include "ob_transformer_impl.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

const char* ObTransformAggregate::SUBQUERY_COL_ALIAS = "subquery_col_alias";

ObTransformAggregate::ObTransformAggregate(ObTransformerCtx* ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER), idx_aggr_column_(-1), is_column_aggregate_(false)
{}

ObTransformAggregate::~ObTransformAggregate()
{}

int ObTransformAggregate::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (sel_stmt->has_recusive_cte() || sel_stmt->has_hierarchical_query()) {
      // do nothing
    } else if (OB_FAIL(transform_column_aggregate(sel_stmt, trans_happened))) {
      LOG_WARN("failed to transform column aggregate", K(ret));
    } else if (trans_happened) {
      LOG_TRACE("succeed to transfrom column aggregate stmt");
    } else if (OB_FAIL(transform_const_aggregate(sel_stmt, trans_happened))) {
      LOG_WARN("failed to transform const aggregate", K(ret));
    } else if (trans_happened) {
      LOG_TRACE("succeed to transfrom const aggregate stmt");
    }
  }
  return ret;
}

int ObTransformAggregate::transform_column_aggregate(ObSelectStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_need = false;
  ObAggFunRawExpr* aggr_expr = NULL;
  trans_happened = false;
  if (OB_FAIL(is_valid_column_aggregate(stmt, aggr_expr, is_need))) {
    LOG_WARN("fail to judge whether to need transform ", K(ret));
  } else if (is_need) {
    // do nothing
    is_column_aggregate_ = true;
    if (OB_FAIL(transform_aggregate(stmt, aggr_expr, trans_happened))) {
      LOG_WARN("failed to transform aggregate expr", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::is_valid_column_aggregate(
    ObSelectStmt* select_stmt, ObAggFunRawExpr*& aggr_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* select_expr = NULL;
  is_valid = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (select_stmt->get_from_item_size() != 1 || select_stmt->get_from_item(0).is_joined_ ||
             select_stmt->get_group_expr_size() > 0 || select_stmt->has_rollup() ||
             select_stmt->get_aggr_item_size() != 1) {
    is_valid = false;
  } else if (OB_FAIL(is_valid_select_list(*select_stmt, select_expr, is_valid))) {
    LOG_WARN("failed to check is valid select list", K(ret));
  } else if (!is_valid) {
    // not valid
  } else if (OB_FAIL(is_valid_aggr_expr(select_stmt, select_expr, aggr_expr, is_valid))) {
    LOG_WARN("failed to check is valid expr", K(ret));
  } else if (!is_valid) {
    // not valid
  } else if (OB_FAIL(is_valid_having(select_stmt, aggr_expr, is_valid))) {
    LOG_WARN("fail to check is valid having", K(ret));
  } else {
    LOG_TRACE("find valid column aggregate", K(is_valid));
  }
  return ret;
}

int ObTransformAggregate::is_valid_select_list(const ObSelectStmt& stmt, const ObRawExpr*& aggr_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t num_non_const_exprs = 0;
  aggr_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    const ObRawExpr* expr = stmt.get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be NULL", K(ret));
    } else if (expr->is_const_expr()) {
      /*do nothing */
    } else {
      idx_aggr_column_ = i;
      aggr_expr = expr;
      ++num_non_const_exprs;
    }
  }
  if (OB_SUCC(ret) && num_non_const_exprs == 1) {
    is_valid = true;
  }
  return ret;
}

int ObTransformAggregate::is_valid_aggr_expr(
    const ObSelectStmt* stmt, const ObRawExpr* expr, ObAggFunRawExpr*& column_aggr_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_valid = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if ((T_FUN_MAX == expr->get_expr_type() || T_FUN_MIN == expr->get_expr_type())) {
    const ObAggFunRawExpr* aggr_expr = static_cast<const ObAggFunRawExpr*>(expr);
    if (1 == aggr_expr->get_real_param_count() && aggr_expr->get_expr_level() == stmt->get_current_level()) {
      if (OB_FAIL(is_valid_index_column(stmt, aggr_expr->get_param_expr(0), is_valid))) {
        LOG_WARN("failed to check is valid index column", K(ret));
      } else if (is_valid) {
        column_aggr_expr = const_cast<ObAggFunRawExpr*>(aggr_expr);
      }
    }
  } else if (expr->has_flag(CNT_AGG)) {
    const ObRawExpr* aggr_param = NULL;
    const ObRawExpr* param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_ISNULL(param = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is null", K(ret));
      } else if (param->is_const_expr()) {
        /* do nothing */
      } else if (param->has_flag(CNT_AGG) && OB_ISNULL(aggr_param)) {
        aggr_param = param;
      } else {  // not valid
        aggr_param = NULL;
        break;
      }
    }
    if (OB_ISNULL(aggr_param)) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(is_valid_aggr_expr(stmt, aggr_param, column_aggr_expr, is_valid)))) {
      LOG_WARN("failed to check is_valid_expr", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::is_valid_having(
    const ObSelectStmt* stmt, const ObAggFunRawExpr* column_aggr_expr, bool& is_expected)
{
  int ret = OB_SUCCESS;
  is_expected = true;
  bool is_unexpected = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_unexpected && i < stmt->get_having_expr_size(); ++i) {
    if (OB_FAIL(
            recursive_find_unexpected_having_expr(column_aggr_expr, stmt->get_having_exprs().at(i), is_unexpected))) {
      LOG_WARN("fail to find unexpected having expr", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    is_expected = !is_unexpected;
  }
  return ret;
}

int ObTransformAggregate::recursive_find_unexpected_having_expr(
    const ObAggFunRawExpr* aggr_expr, const ObRawExpr* cur_expr, bool& is_unexpected)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_unexpected = false;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(cur_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter", K(ret), K(aggr_expr), K(cur_expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (cur_expr->has_flag(CNT_COLUMN) || cur_expr->has_flag(CNT_SUB_QUERY)) {
    ObRawExpr::ExprClass expr_class = cur_expr->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_CONST: {
        is_unexpected = false;
        break;
      }
      case ObRawExpr::EXPR_SET_OP: {
        is_unexpected = false;
        break;
      }
      case ObRawExpr::EXPR_QUERY_REF: {
        is_unexpected = true;
        break;
      }
      case ObRawExpr::EXPR_COLUMN_REF: {
        is_unexpected = true;
        break;
      }
      case ObRawExpr::EXPR_AGGR: {
        if (cur_expr->same_as(*aggr_expr)) {
          is_unexpected = false;
        } else {
          if (OB_FAIL(SMART_CALL(
                  recursive_find_unexpected_having_expr(aggr_expr, cur_expr->get_param_expr(0), is_unexpected)))) {
            LOG_WARN("fail to find unexpected having expr", K(ret));
          }
        }
        break;
      }
      case ObRawExpr::EXPR_OPERATOR:
      case ObRawExpr::EXPR_CASE_OPERATOR:
      case ObRawExpr::EXPR_SYS_FUNC:
      case ObRawExpr::EXPR_UDF: {
        for (int64_t i = 0; OB_SUCC(ret) && !is_unexpected && i < cur_expr->get_param_count(); ++i) {
          if (OB_FAIL(SMART_CALL(
                  recursive_find_unexpected_having_expr(aggr_expr, cur_expr->get_param_expr(i), is_unexpected)))) {
            LOG_WARN("fail to find unexpected having expr", K(ret), K(i));
          }
        }
        break;
      }
      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        // should not reach here
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr class type", K(ret), K(expr_class));
        break;
    }
  } else {
    is_unexpected = false;
  }
  return ret;
}

int ObTransformAggregate::is_valid_index_column(const ObSelectStmt* stmt, const ObRawExpr* expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = NULL;
  const ObColumnRefRawExpr* col_expr = NULL;
  bool is_match_index = false;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(expr));
  } else if (!expr->is_column_ref_expr() || expr->get_expr_level() != stmt->get_current_level()) {
    /* do nothing */
  } else if (FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr*>(expr))) {
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!table_item->is_basic_table()) {
    /* do nothing */
  } else if (OB_FAIL(ObTransformUtils::is_match_index(
                 ctx_->sql_schema_guard_, stmt, col_expr, is_match_index, &ctx_->equal_sets_))) {
    LOG_WARN("failed to check whether column matches index", K(ret));
  } else if (is_match_index) {
    is_valid = true;
  }
  return ret;
}

int ObTransformAggregate::transform_aggregate(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt* upper_stmt = stmt;
  ObSelectStmt* child_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (OB_FAIL(create_stmt(upper_stmt, child_stmt))) {
    LOG_WARN("fail to create stmt", K(ret));
  } else if (OB_FAIL(transform_child_stmt(child_stmt, aggr_expr))) {
    LOG_WARN("fail to transform child item", K(ret));
  } else if (OB_FAIL(transform_upper_stmt(upper_stmt, child_stmt, aggr_expr))) {
    LOG_WARN("fail to transform upper item", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformAggregate::transform_const_aggregate(ObSelectStmt* select_stmt, bool& trans_happened)
{
  // 1. With group by
  //    select max(1) from t1 group by c1; -> select 1 from t1 group by c1;
  // 2. Without group by && not from dual
  //    select max(1) from t1; -> select max(t.a) from (select 1 as a from t1 limit 1) t;
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObRawExpr* expr = NULL;
  TableItem* table = NULL;
  trans_happened = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(is_valid_const_aggregate(select_stmt, is_valid))) {
    LOG_WARN("failed to check is valid const aggregate", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_UNLIKELY(1 != select_stmt->get_select_item_size()) ||
             OB_ISNULL(expr = select_stmt->get_select_item(0).expr_) || OB_UNLIKELY(!expr->is_aggr_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item is invalid", K(ret));
  } else if (select_stmt->get_group_expr_size() > 0) {
    // with group by
    select_stmt->get_select_item(0).expr_ = expr->get_param_expr(0);
    select_stmt->get_aggr_items().reset();
    select_stmt->get_order_items().reset();
    trans_happened = true;
  } else if (0 == select_stmt->get_from_items().count()) {
    // from dual
  } else if (select_stmt->is_single_table_stmt() && OB_NOT_NULL(table = select_stmt->get_table_item(0)) &&
             table->is_generated_table()) {
    // add limit 1
    ObSelectStmt* ref_query = NULL;
    if (OB_ISNULL(ref_query = table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ref_query->has_limit() || ref_query->is_scala_group_by()) {
      /*do nothing*/
    } else if (OB_FAIL(set_child_limit_item(ref_query))) {
      LOG_WARN("fail to set child limit item", K(ret));
    } else {
      trans_happened = true;
    }
  } else {
    ObAggFunRawExpr* aggr_expr = static_cast<ObAggFunRawExpr*>(expr);
    is_column_aggregate_ = false;
    if (OB_FAIL(transform_aggregate(select_stmt, aggr_expr, trans_happened))) {
      LOG_WARN("fail to transform aggregate", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::is_valid_const_aggregate(ObSelectStmt* stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (1 == stmt->get_select_item_size() && !stmt->has_having() && 1 == stmt->get_aggr_item_size()) {
    idx_aggr_column_ = 0;
    SelectItem& select_item = stmt->get_select_item(0);
    if (OB_FAIL(is_min_max_const(stmt, select_item.expr_, is_valid))) {
      LOG_WARN("is_min_max_const() fails", K(ret), K(is_valid));
    }
  }
  return ret;
}

int ObTransformAggregate::create_stmt(ObSelectStmt* upper_stmt, ObSelectStmt*& child_stmt)
{
  int ret = OB_SUCCESS;
  ObStmtFactory* stmt_factory = NULL;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(upper_stmt), K_(ctx), K(stmt_factory));
  } else if (OB_FAIL(
                 ObTransformUtils::copy_stmt(*stmt_factory, upper_stmt, reinterpret_cast<ObDMLStmt*&>(child_stmt)))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(child_stmt->adjust_subquery_stmt_parent(upper_stmt, child_stmt))) {
    LOG_WARN("failed to adjust subquery namespace", K(ret));
  } else {
    child_stmt->set_current_level(upper_stmt->get_current_level());
    child_stmt->set_stmt_id();
    ObQueryCtx* query_ctx = child_stmt->get_query_ctx();
    if (OB_ISNULL(query_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid query_ctx is NULL", K(ret));
    } else if (OB_FAIL(
                   query_ctx->add_stmt_id_name(child_stmt->get_stmt_id(), ObString::make_empty_string(), child_stmt))) {
      LOG_WARN("failed to add_stmt_id_name", "stmt_id", child_stmt->get_stmt_id(), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformAggregate::transform_upper_stmt(
    ObSelectStmt* upper_stmt, ObSelectStmt* child_stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(child_stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have NULL", K(ret), K(upper_stmt), K(child_stmt), K(aggr_expr));
  } else if (OB_FAIL(set_upper_from_item(upper_stmt, child_stmt))) {
    LOG_WARN("fail to set upper stmt table item", K(ret), K(upper_stmt), K(child_stmt));
  } else if (OB_FAIL(set_upper_select_item(upper_stmt, aggr_expr))) {
    LOG_WARN("fail to set upper stmt column item", K(ret), K(upper_stmt));
  } else if (OB_FAIL(clear_unused_attribute(upper_stmt))) {
    LOG_WARN("fail to clear unused attribute", K(ret), K(upper_stmt));
  } else if (OB_FAIL(upper_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("formalize stmt failed", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformAggregate::set_upper_from_item(ObSelectStmt* stmt, ObSelectStmt* child_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(child_stmt));
  } else {
    TableItem* table_item = NULL;
    stmt->get_from_items().reset();
    stmt->reset_table_items();
    stmt->get_semi_infos().reset();
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, child_stmt, table_item))) {
      LOG_WARN("failed to add subquery table item", K(ret));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(table_item->table_id_, false))) {
      LOG_WARN("fail to add from item", K(ret), K(table_item->table_id_));
    }
  }
  return ret;
}

int ObTransformAggregate::set_upper_select_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  ColumnItem new_column_item;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr) || OB_ISNULL(aggr_expr->get_param_expr(0)) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr), K(ctx_));
  } else if (1 != stmt->get_from_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper stmt has invalid from list", K(ret), K(stmt->get_from_item_size()));
  } else {
    // create new column item
    uint64_t table_id = stmt->get_from_item(0).table_id_;
    const TableItem* table_item = stmt->get_table_item_by_id(table_id);
    stmt->get_column_items().reset();
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get generate table", K(ret), K(table_id));
    } else if (OB_FAIL(ObResolverUtils::create_generate_table_column(
                   *ctx_->expr_factory_, *table_item, OB_APP_MIN_COLUMN_ID, new_column_item))) {
      LOG_WARN("create generated table column failed", K(ret));
    } else if (OB_ISNULL(new_column_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper aggr column item expr is null", K(ret));
    } else if (!is_column_aggregate_) {
      new_column_item.expr_->set_column_name(ObString::make_string(SUBQUERY_COL_ALIAS));
      new_column_item.column_name_ = ObString::make_string(SUBQUERY_COL_ALIAS);
    }
  }

  // set aggregate expr
  if (OB_SUCC(ret)) {
    aggr_expr->clear_child();
    if (OB_FAIL(aggr_expr->add_real_param_expr(new_column_item.expr_))) {
      LOG_WARN("fail to add aggregate function param expr", K(ret));
    } else if (OB_FAIL(stmt->add_column_item(new_column_item))) {
      LOG_WARN("fail to add column item into upper_stmt", K(ret));
    } else if (FALSE_IT(stmt->get_aggr_items().reset())) {
      // do nothing
    } else if (OB_FAIL(stmt->add_agg_item(*aggr_expr))) {
      LOG_WARN("fail to add aggregate item", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::clear_unused_attribute(ObSelectStmt* stmt) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    stmt->get_subquery_exprs().reset();
    stmt->get_condition_exprs().reset();
    stmt->get_part_exprs().reset();
    stmt->get_order_items().reset();
    stmt->get_joined_tables().reset();
    stmt->get_pseudo_column_like_exprs().reset();
  }
  return ret;
}

int ObTransformAggregate::transform_child_stmt(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (OB_FAIL(set_child_select_item(stmt, aggr_expr))) {
    LOG_WARN("fail to set child select item", K(ret));
  } else if (OB_FAIL(set_child_condition(stmt, aggr_expr))) {
    LOG_WARN("fail to set child condition", K(ret));
  } else if (OB_FAIL(set_child_order_item(stmt, aggr_expr))) {
    LOG_WARN("fail to set child order item", K(ret));
  } else if (OB_FAIL(set_child_limit_item(stmt))) {
    LOG_WARN("fail to set child limit item", K(ret));
  } else {
    stmt->get_aggr_items().reset();
    stmt->get_having_exprs().reset();
    stmt->get_window_func_exprs().reset();
    stmt->set_select_into(NULL);
    if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("formalize stmt failed", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::set_child_select_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  SelectItem select_item;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (idx_aggr_column_ < 0 || idx_aggr_column_ >= stmt->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregation expr index is invalid", K(ret));
  } else if (FALSE_IT(select_item = stmt->get_select_item(idx_aggr_column_))) {
    // do nothing
  } else if (is_column_aggregate_) {
    // 1. handle select max(c1) from t1 ...
    ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(aggr_expr->get_param_expr(0));
    ColumnItem* column_item = NULL;
    if (OB_ISNULL(column_item = stmt->get_column_item_by_id(col_expr->get_table_id(), col_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column item", K(ret), K(col_expr->get_table_id()), K(col_expr->get_column_id()));
    } else {
      select_item.expr_ = column_item->expr_;
      select_item.alias_name_ = column_item->column_name_;
      select_item.default_value_ = column_item->default_value_;
      select_item.default_value_expr_ = column_item->default_value_expr_;
    }
  } else {
    // 2. handle select max (1) from t1 ...
    if (OB_ISNULL(select_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select_item.expr_ is null", K(ret));
    } else {
      select_item.expr_ = select_item.expr_->get_param_expr(0);
    }
  }
  if (OB_SUCC(ret)) {
    stmt->get_select_items().reset();
    if (OB_FAIL(stmt->add_select_item(select_item))) {
      LOG_WARN("add select item failed", K(ret));
    }
  }
  return ret;
}

int ObTransformAggregate::set_child_order_item(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr) || OB_ISNULL(aggr_expr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (OB_UNLIKELY(1 != stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt should contain only one select item", K(ret));
  } else if (is_column_aggregate_) {
    OrderItem new_order_item;
    new_order_item.expr_ = stmt->get_select_item(0).expr_;
    if (T_FUN_MAX == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_desc_direction();
    } else if (T_FUN_MIN == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_asc_direction();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate function type must by max or min", K(ret));
    }
    if (OB_SUCC(ret)) {
      stmt->get_order_items().reset();
      if (OB_FAIL(stmt->add_order_item(new_order_item))) {
        LOG_WARN("fail to add order item", K(ret), K(stmt), K(new_order_item));
      }
    }
  } else {
    stmt->get_order_items().reset();
  }
  return ret;
}

int ObTransformAggregate::set_child_limit_item(ObSelectStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt), K_(ctx), K(expr_factory));
  } else {
    ObObj value;
    value.set_int(1);
    ObConstRawExpr* new_limit_count_expr = NULL;
    if (OB_FAIL(expr_factory->create_raw_expr(static_cast<ObItemType>(value.get_type()), new_limit_count_expr))) {
      LOG_WARN("fail to create const raw expr", K(ret));
    } else {
      new_limit_count_expr->set_data_type(ObIntType);
      new_limit_count_expr->set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
      if (OB_FAIL(new_limit_count_expr->add_flag(IS_CONST))) {
        LOG_WARN("failed to add flag IS_CONST", K(ret));
      } else if (OB_FAIL(new_limit_count_expr->add_flag(CNT_CONST))) {
        LOG_WARN("failed to add flag CNT_CONST", K(ret));
      } else {
        new_limit_count_expr->set_value(value);
        ObAccuracy accuracy;
        accuracy.set_length(1);
        accuracy.set_precision(1);
        accuracy.set_scale(0);
        new_limit_count_expr->set_accuracy(accuracy);
      }
    }
    if (OB_SUCC(ret)) {
      stmt->set_limit_offset(new_limit_count_expr, NULL);
    }
  }
  return ret;
}

int ObTransformAggregate::set_child_condition(ObSelectStmt* stmt, ObAggFunRawExpr* aggr_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* not_null_expr = NULL;
  ObRawExpr* aggr_param = NULL;
  bool is_nullable = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr) || OB_ISNULL(aggr_param = aggr_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (!is_column_aggregate_) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::check_expr_nullable(stmt, aggr_param, is_nullable))) {
    LOG_WARN("fail to check is column not null", K(ret));
  } else if (!is_nullable) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, aggr_param, not_null_expr))) {
    LOG_WARN("failed to add is not null", K(ret));
  } else if (OB_FAIL(stmt->add_condition_expr(not_null_expr))) {
    LOG_WARN("failed to add condition expr", K(ret));
  }
  return ret;
}

int ObTransformAggregate::is_min_max_const(ObSelectStmt* stmt, ObRawExpr* expr, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr from select_item is NULL", K(ret), K(expr));
  } else if ((T_FUN_MAX == expr->get_expr_type() || T_FUN_MIN == expr->get_expr_type()) &&
             expr->get_expr_level() == stmt->get_current_level()) {
    bool not_const = false;
    if (OB_FAIL(is_not_const(stmt, expr->get_param_expr(0), not_const))) {
      LOG_WARN("is_column_on_current_level() fails", K(ret));
    } else {
      is_const = !not_const;
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObTransformAggregate::is_not_const(ObSelectStmt* stmt, ObRawExpr* expr, bool& not_const)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  not_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    if (T_OP_ASSIGN == expr->get_expr_type() || expr->has_flag(CNT_USER_VARIABLE) || expr->has_flag(CNT_WINDOW_FUNC)) {
      not_const = true;
    } else if (expr->has_flag(IS_COLUMN) && expr->get_expr_level() == stmt->get_current_level()) {
      not_const = true;
    } else { /* Do nothing */
    }

    // Search all child exprs recursively
    for (int64_t i = 0; OB_SUCC(ret) && !not_const && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(is_not_const(stmt, expr->get_param_expr(i), not_const)))) {
        LOG_WARN("is_not_const() fails", K(ret), K(i), K(not_const));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

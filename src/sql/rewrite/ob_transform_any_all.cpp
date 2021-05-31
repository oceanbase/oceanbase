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
#include "sql/rewrite/ob_transform_any_all.h"
#include "share/schema/ob_table_schema.h"
#include "sql/rewrite/ob_transform_aggregate.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {
ObTransformAnyAll::ObTransformAnyAll(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
{}

ObTransformAnyAll::~ObTransformAnyAll()
{}

int ObTransformAnyAll::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameter stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_any_all(stmt, trans_happened))) {
    LOG_WARN("fail to transform any all", K(ret));
  }
  return ret;
}

int ObTransformAnyAll::transform_any_all(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_sel_del_upd()) {
    ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
    if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
      LOG_WARN("failed to get_relation_exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
      ObRawExpr* target = NULL;
      if (OB_FAIL(relation_expr_pointers.at(i).get(target))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find to get expr from group", K(ret), K(target));
      } else if (OB_FAIL(try_transform_any_all(stmt, target, is_happened))) {
        LOG_WARN("fail to transform expr", K(ret));
      } else if (is_happened) {
        trans_happened = true;
        if (OB_FAIL(relation_expr_pointers.at(i).set(target))) {
          LOG_WARN("failed to set expr", K(ret));
        }
      }
    }  // for end
  }
  return ret;
}

int ObTransformAnyAll::try_transform_any_all(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  bool is_stack_overflow = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_FAIL(do_transform_any_all(stmt, expr, is_happened))) {
      LOG_WARN("failed to do_trans_any_all", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    // check children
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_transform_any_all(stmt, expr->get_param_expr(i), is_happened)))) {
        LOG_WARN("failed to try_transform_any_all for param", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformAnyAll::do_transform_any_all(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_FAIL(transform_any_all_as_min_max(stmt, expr, is_happened))) {
      LOG_WARN("failed to trans_any_all_as_min_max", K(ret));
    } else {
      trans_happened |= is_happened;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eliminate_any_all_before_subquery(stmt, expr, is_happened))) {
      LOG_WARN("failed to eliminate_any_all_before_scalar_query", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformAnyAll::check_any_all_as_min_max(ObRawExpr* expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* child_stmt = NULL;
  is_valid = false;

  // check op_expr
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type()) && T_OP_SQ_EQ != expr->get_expr_type() &&
             T_OP_SQ_NSEQ != expr->get_expr_type() && T_OP_SQ_NE != expr->get_expr_type()) {
    if (OB_ISNULL(expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else if (expr->get_param_expr(1)->is_query_ref_expr()) {
      child_stmt = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1))->get_ref_stmt();
      if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is null", K(ret));
      }
    }
  }

  // check child_stmt
  if (OB_FAIL(ret) || OB_ISNULL(child_stmt)) {
  } else if (!child_stmt->has_group_by() && !child_stmt->has_having() && !child_stmt->has_set_op() &&
             !child_stmt->has_limit() && !child_stmt->is_hierarchical_query() &&
             1 == child_stmt->get_select_item_size() && 1 == child_stmt->get_table_size()) {
    ObRawExpr* sel_expr = child_stmt->get_select_item(0).expr_;
    if (OB_ISNULL(sel_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is NULL", K(ret));
    } else if (sel_expr->is_column_ref_expr()) {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(sel_expr);
      bool is_nullable = true;
      bool is_match = false;
      if (col_expr->get_expr_level() != child_stmt->get_current_level()) {
      } else if (OB_FAIL(ObTransformUtils::is_match_index(
                     ctx_->sql_schema_guard_, child_stmt, col_expr, is_match, &ctx_->equal_sets_))) {
        LOG_WARN("failed to check is match index prefix", K(ret));
      } else if (!is_match) {
      } else if (expr->has_flag(IS_WITH_ANY)) {
        is_valid = true;
      } else if (expr->has_flag(IS_WITH_ALL)) {
        if (OB_FAIL(ObTransformUtils::is_column_nullable(child_stmt, ctx_->schema_checker_, col_expr, is_nullable))) {
          LOG_WARN("failed to check is column nullable", K(ret));
        } else if (!is_nullable) {
          is_valid = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformAnyAll::transform_any_all_as_min_max(ObDMLStmt* stmt, ObRawExpr* expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_any_all_as_min_max(expr, is_valid))) {
    LOG_WARN("failed to check_any_all_as_min_max", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_ISNULL(expr->get_param_expr(1)) || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr has invalid param", K(ret), K(expr->get_param_expr(1)));
  } else {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
    ObSelectStmt* child_stmt = query_ref->get_ref_stmt();
    ObItemType aggr_type = get_aggr_type(expr->get_expr_type(), expr->has_flag(IS_WITH_ALL));
    if (OB_FAIL(do_transform_any_all_as_min_max(child_stmt, aggr_type, expr->has_flag(IS_WITH_ALL), trans_happened))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to do_trans_any_all_as_min_max", K(ret));
    }
  }
  return ret;
}

ObItemType ObTransformAnyAll::get_aggr_type(ObItemType op_type, bool is_with_all)
{
  ObItemType aggr_type = T_INVALID;
  if (T_OP_SQ_LE == op_type || T_OP_SQ_LT == op_type) {
    if (is_with_all) {
      aggr_type = T_FUN_MIN;
    } else { /*with_any*/
      aggr_type = T_FUN_MAX;
    }
  } else if (T_OP_SQ_GE == op_type || T_OP_SQ_GT == op_type) {
    if (is_with_all) {
      aggr_type = T_FUN_MAX;
    } else { /*with_all*/
      aggr_type = T_FUN_MIN;
    }
  }
  return aggr_type;
}

int ObTransformAnyAll::do_transform_any_all_as_min_max(
    ObSelectStmt* stmt, const ObItemType aggr_type, bool is_with_all, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr* col_expr = NULL;
  ObAggFunRawExpr* aggr_expr = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params or data member is NULL", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_select_item_size() <= 0 || OB_ISNULL(col_expr = stmt->get_select_item(0).expr_) ||
             (T_FUN_MAX != aggr_type && T_FUN_MIN != aggr_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have incorrect value", K(ret), K(stmt->get_select_items()), K(col_expr), K(aggr_type));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(aggr_type, aggr_expr))) {
    LOG_WARN("fail to create raw expr", K(ret), K(aggr_expr));
  } else if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create aggr expr", K(ret), K(aggr_type));
  } else if (OB_FAIL(aggr_expr->add_real_param_expr(col_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (FALSE_IT(aggr_expr->set_expr_level(stmt->get_current_level()))) {
  } else if (OB_FAIL(aggr_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_FAIL(aggr_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
    LOG_WARN("failed to pull relation id", K(ret));
  } else {
    stmt->get_select_item(0).expr_ = aggr_expr;
    if (OB_FAIL(stmt->add_agg_item(*aggr_expr))) {
      LOG_WARN("fail to add agg_item", K(ret));
    } else if (is_with_all) {
      ObOpRawExpr* is_not_expr = NULL;
      if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, aggr_expr, is_not_expr))) {
        LOG_WARN("failed to add is not null", K(ret));
      } else if (OB_FAIL(stmt->add_having_expr(is_not_expr))) {
        LOG_WARN("failed to add having expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformAnyAll::eliminate_any_all_before_subquery(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool can_be_removed = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr));
  } else if (OB_FAIL(check_any_all_removeable(expr, can_be_removed))) {
    LOG_WARN("failed to check is any all removeable", K(ret));
  } else if (!can_be_removed) {
    /* do nothing */
  } else if (OB_ISNULL(expr->get_param_expr(1)) || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr has invalid param", K(ret), K(expr->get_param_expr(1)));
  } else {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
    if (OB_FAIL(clear_any_all_flag(stmt, expr, query_ref))) {
      LOG_WARN("failed to clear any all flag", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformAnyAll::check_any_all_removeable(ObRawExpr* expr, bool& can_be_removed)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sub_stmt = NULL;
  bool is_expr_query = false;
  //  bool is_aggr_query = false;
  can_be_removed = false;
  // check op
  if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_ISNULL(expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else if (expr->get_param_expr(1)->is_query_ref_expr()) {
      ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(1));
      if (OB_ISNULL(sub_stmt = query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub_stmt is null", K(ret));
      }
    }
  }

  // check sub_stmt
  if (OB_FAIL(ret) || OB_ISNULL(sub_stmt)) {
  } else if (OB_FAIL(ObTransformUtils::is_expr_query(sub_stmt, is_expr_query))) {
    LOG_WARN("failed to check sub stmt is expr query", K(ret));
  } else if (is_expr_query) {
    can_be_removed = true;
  }
  return ret;
}

int ObTransformAnyAll::clear_any_all_flag(ObDMLStmt* stmt, ObRawExpr*& expr, ObQueryRefRawExpr* query_ref)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(query_ref) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr), K(query_ref), K(ctx_));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    query_ref->set_is_set(false);

    ObOpRawExpr* tmp_op = NULL;
    ObItemType op_type = query_cmp_to_value_cmp(expr->get_expr_type());
    if (T_INVALID == op_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op type is not correct", K(ret), K(op_type));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(op_type, tmp_op))) {
      LOG_WARN("failed to create tmp op", K(ret));
    } else if (OB_ISNULL(tmp_op)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tmp_op->set_param_exprs(expr->get_param_expr(0), expr->get_param_expr(1)))) {
      LOG_WARN("failed to set params", K(ret));
    } else if (OB_FAIL(tmp_op->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize tmp op", K(ret));
    } else if (OB_FAIL(tmp_op->pull_relation_id_and_levels(stmt->get_current_level()))) {
      LOG_WARN("failed to pull relation id", K(ret));
    } else {
      expr = tmp_op;
    }
  }
  return ret;
}

ObItemType ObTransformAnyAll::query_cmp_to_value_cmp(const ObItemType cmp_type)
{
  ObItemType ret = T_INVALID;
  switch (cmp_type) {
    case T_OP_SQ_EQ:
      ret = T_OP_EQ;
      break;
    case T_OP_SQ_NSEQ:
      ret = T_OP_NSEQ;
      break;
    case T_OP_SQ_LE:
      ret = T_OP_LE;
      break;
    case T_OP_SQ_LT:
      ret = T_OP_LT;
      break;
    case T_OP_SQ_GE:
      ret = T_OP_GE;
      break;
    case T_OP_SQ_GT:
      ret = T_OP_GT;
      break;
    case T_OP_SQ_NE:
      ret = T_OP_NE;
      break;
    default:
      ret = T_INVALID;
      break;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

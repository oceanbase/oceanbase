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
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_transform_win_groupby.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;

namespace sql {

int ObTransformWinGroupBy::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  WinGroupByHelper helper;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_transform_validity(stmt, helper, is_valid))) {
    LOG_WARN("failed to check transform validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(do_win_groupby_transform(helper))) {
    LOG_WARN("failed to do win groupby transformation", K(ret));
  } else if (OB_ISNULL(helper.transformed_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    stmt = helper.transformed_stmt_;
    trans_happened = true;
  }
  return ret;
}

int ObTransformWinGroupBy::do_win_groupby_transform(WinGroupByHelper& helper)
{
  int ret = OB_SUCCESS;
  int64_t const_value = 1;
  if (OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(helper.outer_stmt_), K(ctx_), K(ret));
  } else if (OB_FAIL(
                 ObTransformUtils::create_stmt_with_generated_table(ctx_, helper.outer_stmt_, helper.winfun_stmt_))) {
    LOG_WARN("failed to create select stmt", K(ret));
  } else if (OB_ISNULL(helper.winfun_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(
                 ctx_, helper.winfun_stmt_, helper.transformed_stmt_))) {
    LOG_WARN("failed to create select stmt", K(ret));
  } else if (OB_ISNULL(helper.transformed_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(
                 ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, const_value, helper.dummy_expr_))) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (OB_FAIL(adjust_inner_stmt(helper))) {
    LOG_WARN("failed to adjust inner stmt", K(ret));
  } else if (OB_FAIL(adjust_outer_stmt(helper))) {
    LOG_WARN("failed to adjust outer stmt", K(ret));
  } else {
    LOG_TRACE("succeed to do win groupby transformation", K(ret));
  }

  return ret;
}

int ObTransformWinGroupBy::adjust_inner_stmt(WinGroupByHelper& helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> win_exprs;
  if (OB_ISNULL(helper.inner_stmt_) || OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(helper.winfun_stmt_) ||
      OB_ISNULL(helper.transformed_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(helper.inner_stmt_),
        K(helper.outer_stmt_),
        K(helper.winfun_stmt_),
        K(helper.transformed_stmt_),
        K(ret));
  } else if (OB_FAIL(append(win_exprs, helper.inner_winexprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    // adjust select expr
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_pos_.count(); i++) {
      int64_t pos = helper.inner_pos_.at(i);
      if (OB_UNLIKELY(pos < 0 || pos >= helper.inner_stmt_->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array pos", K(pos), K(helper.inner_stmt_->get_select_item_size()), K(ret));
      } else if (OB_FAIL(adjust_stmt_expr(helper.winfun_stmt_,
                     helper.outer_stmt_,
                     helper.inner_stmt_,
                     win_exprs,
                     NULL,
                     helper.inner_stmt_->get_select_item(pos).expr_))) {
        LOG_WARN("failed to adjust expr", K(ret));
      } else { /*do nothing*/
      }
    }
    // adjust window function expr
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_winexprs_.count(); i++) {
      ObWinFunRawExpr* win_expr = NULL;
      if (OB_ISNULL(win_expr = helper.inner_winexprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(adjust_win_func_expr(helper, win_expr))) {
        LOG_WARN("failed to adjust win expr", K(ret));
      } else if (OB_FAIL(helper.inner_stmt_->remove_window_func_expr(win_expr))) {
        LOG_WARN("failed to remove window func expr", K(ret));
      } else if (OB_FAIL(helper.winfun_stmt_->add_window_func_expr(win_expr))) {
        LOG_WARN("failed to add window function expr", K(ret));
      } else { /*do nothing*/
      }
    }
    // move select expr to winfun stmt
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_pos_.count(); i++) {
      ObRawExpr* expr = NULL;
      int64_t pos = helper.inner_pos_.at(i);
      if (OB_UNLIKELY(pos < 0 || pos >= helper.inner_stmt_->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array pos", K(pos), K(helper.inner_stmt_->get_select_item_size()), K(ret));
      } else if (OB_ISNULL(expr = helper.inner_stmt_->get_select_item(pos).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(adjust_select_item(helper.transformed_stmt_, helper.winfun_stmt_, expr))) {
        LOG_WARN("failed to adjust select item", K(ret));
      } else if (OB_FAIL(helper.outer_new_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        helper.inner_stmt_->get_select_item(pos).expr_ = helper.dummy_expr_;
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::adjust_select_item(
    ObSelectStmt* outer_stmt, ObSelectStmt* inner_stmt, ObRawExpr*& inner_expr)
{
  int ret = OB_SUCCESS;
  int64_t pos = OB_INVALID_ID;
  TableItem* outer_table_item = NULL;
  ObSEArray<ObRawExpr*, 16> inner_select_exprs;
  ObColumnRefRawExpr* column_expr = NULL;
  if (OB_ISNULL(outer_stmt) || OB_ISNULL(inner_stmt) || OB_ISNULL(inner_expr) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(outer_stmt), K(inner_stmt), K(inner_expr), K(ctx_), K(ret));
  } else if (OB_UNLIKELY(1 != outer_stmt->get_from_item_size() || outer_stmt->get_from_item(0).is_joined_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(inner_stmt->get_from_item_size()), K(ret));
  } else if (OB_ISNULL(outer_table_item = outer_stmt->get_table_item_by_id(outer_stmt->get_from_item(0).table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table item", K(ret));
  } else if (OB_UNLIKELY(!outer_table_item->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table item type", K(outer_table_item->type_), K(ret));
  } else if (OB_FAIL(inner_stmt->get_select_exprs(inner_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObOptimizerUtil::find_item(inner_select_exprs, inner_expr, &pos)) {
    if (OB_NOT_NULL(
            inner_expr = outer_stmt->get_column_expr_by_id(outer_table_item->table_id_, pos + OB_APP_MIN_COLUMN_ID))) {
      /*do nothing*/
    } else if (OB_FAIL(ObTransformUtils::create_new_column_expr(ctx_,
                   *outer_table_item,
                   pos + OB_APP_MIN_COLUMN_ID,
                   inner_stmt->get_select_item(pos),
                   outer_stmt,
                   column_expr))) {
      LOG_WARN("failed to create column expr", K(ret));
    } else {
      inner_expr = column_expr;
    }
  } else {
    ObColumnRefRawExpr* column_expr = NULL;
    if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, inner_expr, inner_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_new_column_expr(ctx_,
                   *outer_table_item,
                   inner_stmt->get_select_item_size() - 1 + OB_APP_MIN_COLUMN_ID,
                   inner_stmt->get_select_item(inner_stmt->get_select_item_size() - 1),
                   outer_stmt,
                   column_expr))) {
      LOG_WARN("failed to create column expr", K(ret));
    } else {
      inner_expr = column_expr;
    }
  }
  return ret;
}

int ObTransformWinGroupBy::adjust_win_func_expr(WinGroupByHelper& helper, ObWinFunRawExpr* win_expr)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObAggFunRawExpr* same_expr = NULL;
  ObAggFunRawExpr* aggr_expr = NULL;
  ObSEArray<ObRawExpr*, 16> dummy_exprs;
  if (OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(helper.inner_stmt_) || OB_ISNULL(helper.winfun_stmt_) ||
      OB_ISNULL(win_expr) || OB_ISNULL(aggr_expr = win_expr->get_agg_expr()) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",
        K(helper.outer_stmt_),
        K(helper.inner_stmt_),
        K(helper.winfun_stmt_),
        K(win_expr),
        K(aggr_expr),
        K(ctx_),
        K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < win_expr->get_param_count(); i++) {
      if (i < aggr_expr->get_param_count()) {
        // is param of agg expr
        if (OB_FAIL(adjust_stmt_expr(
                NULL, helper.outer_stmt_, helper.inner_stmt_, dummy_exprs, NULL, win_expr->get_param_expr(i)))) {
          LOG_WARN("failed to adjust stmt expr", K(ret));
        } else { /*do nothing*/
        }
      } else {
        // is not param of agg expr
        if (OB_FAIL(adjust_stmt_expr(helper.winfun_stmt_,
                helper.outer_stmt_,
                helper.inner_stmt_,
                dummy_exprs,
                NULL,
                win_expr->get_param_expr(i)))) {
          LOG_WARN("failed to adjust stmt expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObItemType new_agg_type = aggr_expr->get_expr_type();
      ObAggFunRawExpr* new_agg_expr = NULL;
      if (T_FUN_COUNT != new_agg_type) {
        /*do nothing*/
      } else if (lib::is_oracle_mode()) {
        new_agg_type = T_FUN_SUM;
      } else {
        new_agg_type = T_FUN_COUNT_SUM;
      }
      if (OB_FAIL(helper.outer_stmt_->check_and_get_same_aggr_item(aggr_expr, same_expr))) {
        LOG_WARN("failed to check and get same aggr item.", K(ret));
      } else if (same_expr != NULL) {
        is_found = true;
      } else {
        same_expr = aggr_expr;
      }
      if (!is_found && OB_FAIL(helper.outer_stmt_->add_agg_item(*same_expr))) {
        LOG_WARN("failed to add agg expr", K(ret));
      } else if (OB_FAIL(adjust_select_item(
                     helper.winfun_stmt_, helper.outer_stmt_, reinterpret_cast<ObRawExpr*&>(same_expr)))) {
        LOG_WARN("failed to adjust select item", K(ret));
      } else if (OB_ISNULL(same_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr<ObAggFunRawExpr>(new_agg_type, new_agg_expr))) {
        LOG_WARN("failed to create aggregation expr", K(ret));
      } else if (OB_ISNULL(new_agg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(new_agg_expr));
      } else {
        new_agg_expr->add_real_param_expr(same_expr);
        new_agg_expr->set_expr_level(helper.outer_stmt_->get_current_level());
        if (OB_FAIL(new_agg_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else {
          win_expr->set_agg_expr(new_agg_expr);
        }
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::adjust_outer_stmt(WinGroupByHelper& helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 16> expr_pointers;
  int32_t ignore_scope =
      RelExprCheckerBase::FIELD_LIST_SCOPE | RelExprCheckerBase::WHERE_SCOPE | RelExprCheckerBase::GROUP_SCOPE;
  if (OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(helper.winfun_stmt_) || OB_ISNULL(helper.transformed_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(helper.transformed_stmt_), K(helper.outer_stmt_), K(helper.winfun_stmt_), K(ret));
  } else if (OB_FAIL(helper.outer_stmt_->get_relation_exprs(expr_pointers, ignore_scope))) {
    LOG_WARN("failed to get_relation_exprs", K(ret));
  } else if (OB_FAIL(mark_select_expr(*helper.outer_stmt_))) {
    LOG_WARN("failed to mark share expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_pointers.count(); ++i) {
      ObRawExpr* target = NULL;
      if (OB_FAIL(expr_pointers.at(i).get(target))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find to get expr from group", K(ret), K(target));
      } else if (OB_FAIL(adjust_stmt_expr(helper.transformed_stmt_,
                     helper.winfun_stmt_,
                     helper.outer_stmt_,
                     helper.outer_old_exprs_,
                     &helper.outer_new_exprs_,
                     target))) {
        LOG_WARN("fail to transform expr", K(ret));
      } else if (OB_FAIL(expr_pointers.at(i).set(target))) {
        LOG_WARN("failed to set expr", K(ret));
      }
    }  // for end
    if (OB_SUCC(ret) && OB_FAIL(mark_select_expr(*helper.outer_stmt_, false))) {
      LOG_WARN("failed to clear share expr", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < helper.outer_pos_.count(); i++) {
      int64_t pos = helper.outer_pos_.at(i);
      if (OB_UNLIKELY(pos < 0 || pos > helper.outer_stmt_->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array pos", K(pos), K(helper.outer_stmt_->get_select_item_size()), K(ret));
      } else if (OB_FAIL(adjust_stmt_expr(helper.transformed_stmt_,
                     helper.winfun_stmt_,
                     helper.outer_stmt_,
                     helper.outer_old_exprs_,
                     &helper.outer_new_exprs_,
                     helper.outer_stmt_->get_select_item(pos).expr_))) {
        LOG_WARN("failed to adjust stmt expr", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(helper.transformed_stmt_->get_condition_exprs(), helper.outer_stmt_->get_having_exprs()))) {
        LOG_WARN("failed to append expr", K(ret));
      } else if (OB_FAIL(append(helper.transformed_stmt_->get_order_items(), helper.outer_stmt_->get_order_items()))) {
        LOG_WARN("failed to appene expr", K(ret));
      } else {
        helper.transformed_stmt_->set_limit_offset(
            helper.outer_stmt_->get_limit_expr(), helper.outer_stmt_->get_offset_expr());
        helper.transformed_stmt_->set_fetch_with_ties(helper.outer_stmt_->is_fetch_with_ties());
        helper.transformed_stmt_->set_limit_percent_expr(helper.outer_stmt_->get_limit_percent_expr());
        helper.outer_stmt_->get_having_exprs().reset();
        helper.outer_stmt_->get_order_items().reset();
        helper.outer_stmt_->set_limit_offset(NULL, NULL);
        helper.outer_stmt_->set_fetch_with_ties(false);
        // adjust select expr
        for (int64_t i = 0; OB_SUCC(ret) && i < helper.outer_pos_.count(); i++) {
          int64_t pos = helper.outer_pos_.at(i);
          if (OB_UNLIKELY(pos < 0 || pos >= helper.outer_stmt_->get_select_item_size() ||
                          pos >= helper.winfun_stmt_->get_select_item_size() ||
                          pos >= helper.transformed_stmt_->get_select_item_size())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected pos",
                K(pos),
                K(helper.outer_stmt_->get_select_item_size()),
                K(helper.winfun_stmt_->get_select_item_size()),
                K(helper.transformed_stmt_->get_select_item_size()),
                K(ret));
          } else {
            helper.transformed_stmt_->get_select_item(pos).expr_ = helper.outer_stmt_->get_select_item(pos).expr_;
            helper.winfun_stmt_->get_select_item(pos).expr_ = helper.dummy_expr_;
            helper.outer_stmt_->get_select_item(pos).expr_ = helper.dummy_expr_;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::adjust_stmt_expr(ObSelectStmt* outer_stmt, ObSelectStmt* middle_stmt,
    ObSelectStmt* inner_stmt, const common::ObIArray<ObRawExpr*>& bypass_exprs,
    const common::ObIArray<ObRawExpr*>* replace_exprs, ObRawExpr*& inner_expr)
{
  int ret = OB_SUCCESS;
  int64_t pos = OB_INVALID_ID;
  bool is_stack_overflow = false;
  if (OB_ISNULL(middle_stmt) || OB_ISNULL(inner_stmt) || OB_ISNULL(inner_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(middle_stmt), K(inner_stmt), K(inner_expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (ObOptimizerUtil::find_item(bypass_exprs, inner_expr, &pos)) {
    if (NULL != replace_exprs) {
      if (OB_UNLIKELY(pos < 0 || pos >= replace_exprs->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array pos", K(pos), K(replace_exprs->count()), K(ret));
      } else {
        inner_expr = replace_exprs->at(pos);
      }
    }
  } else if (inner_expr->is_win_func_expr() || inner_expr->is_aggr_expr() || inner_expr->is_column_ref_expr() ||
             inner_expr->has_flag(IS_SEQ_EXPR) || inner_expr->has_flag(IS_RAND_FUNC) ||
             inner_expr->has_flag((IS_SHARED_REF))) {
    if (OB_FAIL(adjust_select_item(middle_stmt, inner_stmt, inner_expr))) {
      LOG_WARN("failed to adjust select item", K(ret));
    } else if (NULL != outer_stmt && OB_FAIL(adjust_select_item(outer_stmt, middle_stmt, inner_expr))) {
      LOG_WARN("failed to adjust select item", K(ret));
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(adjust_stmt_expr(
              outer_stmt, middle_stmt, inner_stmt, bypass_exprs, replace_exprs, inner_expr->get_param_expr(i))))) {
        LOG_WARN("failed to adjust outer stmt expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::check_transform_validity(ObDMLStmt* stmt, WinGroupByHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!stmt->is_select_stmt() || stmt->get_from_item_size() != 1 || !stmt->get_semi_infos().empty() ||
             stmt->get_from_item(0).is_joined_) {
    /*do nothing*/
  } else if (OB_ISNULL(helper.outer_table_item_ = stmt->get_table_item_by_id(stmt->get_from_item(0).table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!helper.outer_table_item_->is_generated_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(helper.inner_stmt_ = helper.outer_table_item_->ref_query_) ||
             OB_ISNULL(helper.outer_stmt_ = static_cast<ObSelectStmt*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(helper.inner_stmt_), K(helper.outer_stmt_), K(ret));
  } else if (OB_FAIL(check_outer_stmt_validity(helper, is_valid))) {
    LOG_WARN("failed to check outer stmt validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_inner_stmt_validity(helper, is_valid))) {
    LOG_WARN("failed to check inner stmt validity", K(ret));
  } else {
    LOG_TRACE("succeed to check validity of groupby/window-function transformation", K(is_valid), K(helper));
  }
  return ret;
}

int ObTransformWinGroupBy::check_outer_stmt_validity(WinGroupByHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(helper.inner_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(helper.outer_stmt_), K(helper.inner_stmt_), K(ret));
  } else if (helper.outer_stmt_->get_group_exprs().empty() || !helper.outer_stmt_->get_condition_exprs().empty() ||
             !helper.outer_stmt_->get_window_func_exprs().empty() ||
             !helper.outer_stmt_->get_subquery_exprs().empty() || helper.outer_stmt_->has_sequence() ||
             helper.outer_stmt_->has_distinct() || helper.outer_stmt_->has_recusive_cte() ||
             helper.outer_stmt_->has_hierarchical_query() || helper.outer_stmt_->has_rollup() ||
             helper.outer_stmt_->is_contains_assignment()) {
    /*todo release some of these constraints in future */
  } else {
    is_valid = true;
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < helper.outer_stmt_->get_group_expr_size(); i++) {
      int64_t pos = OB_INVALID_ID;
      if (OB_ISNULL(expr = helper.outer_stmt_->get_group_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!expr->is_column_ref_expr()) {
        // @guoping.wgp release this constraint in future
        is_valid = false;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      ObSEArray<ObRawExpr*, 16> aggr_exprs;
      ObSEArray<ObRawExpr*, 16> column_exprs;
      if (OB_FAIL(append(aggr_exprs, helper.outer_stmt_->get_aggr_items()))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(aggr_exprs, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                     helper.outer_stmt_->get_group_exprs(), *helper.inner_stmt_, helper.ref_groupby_exprs_))) {
        LOG_WARN("failed to convert column expr to select expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                     column_exprs, *helper.inner_stmt_, helper.ref_column_exprs_))) {
        LOG_WARN("failed to convert column expr to select expr", K(ret));
      } else if (OB_FAIL(append_array_no_dup(helper.ref_column_exprs_, helper.ref_groupby_exprs_))) {
        LOG_WARN("failed to append expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::check_inner_stmt_validity(WinGroupByHelper& helper, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> stmt_exprs;
  is_valid = false;
  if (OB_ISNULL(helper.inner_stmt_) || OB_ISNULL(helper.outer_stmt_) || OB_ISNULL(helper.outer_table_item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(helper.inner_stmt_), K(helper.outer_stmt_), K(helper.outer_table_item_), K(ret));
  } else if (helper.inner_stmt_->has_limit() || helper.inner_stmt_->get_window_func_exprs().empty() ||
             helper.inner_stmt_->is_contains_assignment()) {
    /*do nothing*/
  } else if (OB_FAIL(helper.inner_stmt_->get_relation_exprs(stmt_exprs, RelExprCheckerBase::FIELD_LIST_SCOPE))) {
    LOG_WARN("failed to get all exprs", K(ret));
  } else {
    ObRawExpr* expr = NULL;
    ObSEArray<ObWinFunRawExpr*, 16> win_exprs;
    ObSEArray<ObRawExpr*, 16> remaining_column_exprs;
    // check select exprs in inner statement that can be transformed (pulled up)
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_stmt_->get_select_item_size(); i++) {
      win_exprs.reset();
      remaining_column_exprs.reset();
      if (OB_ISNULL(expr = helper.inner_stmt_->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!expr->has_flag(CNT_WINDOW_FUNC) || ObOptimizerUtil::find_item(helper.ref_column_exprs_, expr) ||
                 ObOptimizerUtil::find_item(stmt_exprs, expr)) {
        /*do nothing*/
      } else if (OB_FAIL(check_win_expr_valid(
                     expr, stmt_exprs, helper.ref_groupby_exprs_, win_exprs, remaining_column_exprs))) {
        LOG_WARN("failed to check whether window expr is valid", K(ret));
      } else if (win_exprs.empty()) {
        /*do nothing*/
      } else if (!ObOptimizerUtil::subset_exprs(remaining_column_exprs, helper.ref_groupby_exprs_)) {
        /*do nothing*/
      } else if (OB_ISNULL(helper.outer_stmt_->get_column_expr_by_id(
                     helper.outer_table_item_->table_id_, i + OB_APP_MIN_COLUMN_ID))) {
      } else if (OB_FAIL(append_array_no_dup(helper.inner_winexprs_, win_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(helper.inner_pos_.push_back(i))) {
        LOG_WARN("failed to push back select_expr info", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !helper.inner_pos_.empty()) {
      // check select exprs in outer statement that can be pulled up
      for (int64_t i = 0; OB_SUCC(ret) && i < helper.inner_pos_.count(); i++) {
        if (OB_ISNULL(expr = helper.outer_stmt_->get_column_expr_by_id(
                          helper.outer_table_item_->table_id_, helper.inner_pos_.at(i) + OB_APP_MIN_COLUMN_ID))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpecated null", K(ret));
        } else if (OB_FAIL(helper.outer_old_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back column exprs", K(ret));
        } else { /*do nothing*/
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < helper.outer_stmt_->get_select_item_size(); i++) {
        if (OB_ISNULL(expr = helper.outer_stmt_->get_select_item(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get expected null", K(ret));
        } else {
          bool find = false;
          for (int64_t j = 0; !find && j < helper.outer_old_exprs_.count(); j++) {
            if (ObOptimizerUtil::is_sub_expr(helper.outer_old_exprs_.at(j), expr)) {
              find = true;
            }
          }
          if (find) {
            ret = helper.outer_pos_.push_back(i);
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_valid = true;
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::check_win_expr_valid(ObRawExpr* expr, const ObIArray<ObRawExpr*>& stmt_exprs,
    const ObIArray<ObRawExpr*>& groupby_exprs, ObIArray<ObWinFunRawExpr*>& win_exprs,
    ObIArray<ObRawExpr*>& remaining_column_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_win_func_expr()) {
    /*
     * @guoping.wgp in future we should support some win exprs can be produced in inner stmt,
     * and some can be produced in outer stmt
     */
    ObWinFunRawExpr* win_expr = static_cast<ObWinFunRawExpr*>(expr);
    if (win_expr->get_agg_expr() != NULL &&
        (win_expr->get_agg_expr()->get_expr_type() == T_FUN_MAX ||
            win_expr->get_agg_expr()->get_expr_type() == T_FUN_MIN ||
            win_expr->get_agg_expr()->get_expr_type() == T_FUN_COUNT ||
            win_expr->get_agg_expr()->get_expr_type() == T_FUN_SUM) &&
        win_expr->get_func_params().empty() && win_expr->get_order_items().empty() &&
        NULL == win_expr->get_upper().interval_expr_ && NULL == win_expr->get_lower().interval_expr_ &&
        !win_expr->is_distinct() && !ObOptimizerUtil::is_sub_expr(expr, stmt_exprs) &&
        ObOptimizerUtil::subset_exprs(win_expr->get_partition_exprs(), groupby_exprs)) {
      ret = win_exprs.push_back(win_expr);
    } else { /*do nothing*/
    }
  } else if (expr->is_column_ref_expr()) {
    ret = remaining_column_exprs.push_back(expr);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(check_win_expr_valid(
              expr->get_param_expr(i), stmt_exprs, groupby_exprs, win_exprs, remaining_column_exprs)))) {
        LOG_WARN("failed to check whether window expr is valid", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformWinGroupBy::mark_select_expr(ObSelectStmt& stmt, const bool add_flag /*= true*/)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (add_flag) {
      if (OB_FAIL(stmt.get_select_item(i).expr_->add_flag(IS_SHARED_REF))) {
        LOG_WARN("failed to add flag", K(ret));
      }
    } else {
      if (OB_FAIL(stmt.get_select_item(i).expr_->clear_flag(IS_SHARED_REF))) {
        LOG_WARN("failed to clear flag", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

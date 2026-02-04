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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/optimizer_plan_rewriter/ob_groupby_pushdown_rule.h"
#include "sql/optimizer/optimizer_plan_rewriter/ob_set_parent_visitor.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
namespace sql
{

bool ObGroupByPushdownRule::is_enabled(const ObOptimizerContext &ctx)
{
  return ctx.enable_partial_group_by_pushdown() && ctx.get_query_ctx() != NULL &&
         ctx.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_6_0);
}

int ObGroupByPushdownRule::apply_rule(ObLogPlan *root_plan,
                                     ObOptimizerContext &ctx,
                                     ObRuleResult &result)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *root_op = NULL;
  if (OB_ISNULL(root_plan) || OB_ISNULL(root_op = root_plan->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!is_enabled(ctx)) {
    // do nothing
  } else {
    ObSetParentVisitor set_parent_visitor;
    Void void_context;
    Void *void_result;
    ObGroupByPushDownPlanRewriter rewriter(ctx);
    ObGroupByPushdownResult rewriter_result;
    // TODO tuliwei.tlw: rewrite the result logic, replace the dummy_pointer
    ObGroupByPushdownResult* dummy_pointer = &rewriter_result;
    if (OB_FAIL(set_parent_visitor.visit(root_op, &void_context, void_result))) {
      LOG_WARN("failed to set parent", K(ret));
    } else if (OB_FAIL((rewriter.visit(root_op, NULL, dummy_pointer)))) {
      LOG_WARN("failed to traverse plan tree", K(ret));
    } else {
      result.set_transformed(rewriter.transform_happened());
      OPT_TRACE("rewriter result", root_op);
    }
  }

  return ret;
}

int ObGroupByPushdownContext::assign(const ObGroupByPushdownContext &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggr_exprs_.assign(other.aggr_exprs_))) {
    LOG_WARN("failed to assign aggr exprs", K(ret));
  } else if (OB_FAIL(groupby_exprs_.assign(other.groupby_exprs_))) {
    LOG_WARN("failed to assign groupby exprs", K(ret));
  } else {
    need_count_ = other.need_count_;
    enable_reshuffle_ = other.enable_reshuffle_;
    enable_place_groupby_ = other.enable_place_groupby_;
  }
  return ret;
}

int ObGroupByPushdownContext::get_aggr_items(common::ObIArray<ObAggFunRawExpr*> &aggr_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); ++i) {
    if (OB_ISNULL(aggr_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_UNLIKELY(!aggr_exprs_.at(i)->is_aggr_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-aggr expr", K(ret));
    } else if (OB_FAIL(aggr_items.push_back(static_cast<ObAggFunRawExpr *>(aggr_exprs_.at(i))))) {
      LOG_WARN("failed push back aggr item", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushdownContext::get_dependent_exprs(common::ObIArray<ObRawExpr*> &dependent_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dependent_exprs.assign(groupby_exprs_))) {
    LOG_WARN("failed push back group by exprs", K(ret));
  } else {
    // get_param_expr
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); ++i) {
      ObAggFunRawExpr *aggr_item = static_cast<ObAggFunRawExpr *>(aggr_exprs_.at(i));
      if (OB_FAIL(append_array_no_dup(dependent_exprs, aggr_item->get_real_param_exprs_for_update()))) {
        LOG_WARN("failed push back aggr params", K(ret));
      }
    }
  }
  return ret;
}

int ObGroupByPushdownContext::map_and_check(const common::ObIArray<ObRawExpr *> &from_exprs,
  const common::ObIArray<ObRawExpr *> &to_exprs,
  ObRawExprFactory &expr_factory,
  const ObSQLSessionInfo *session_info,
  bool check_valid,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  // do the mapping for distinct exprs
  is_valid = true;
  ObRawExprCopier copier(expr_factory);
  ObSEArray<ObRawExpr*, 4> replaced_exprs;
  ObSEArray<ObRawExpr*, 4> replaced_aggr_exprs;
  ObSEArray<ObRawExpr*, 4> replaced_original_exprs;
  if (OB_FAIL(copier.add_replaced_expr(from_exprs, to_exprs))) {
    LOG_WARN("failed to add replace pair", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < groupby_exprs_.count(); ++i) {
    ObRawExpr*  replaced_expr;
    if (OB_FAIL(copier.copy_on_replace(groupby_exprs_.at(i), replaced_expr))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (OB_FAIL(replaced_exprs.push_back(replaced_expr))) {
      LOG_WARN("failed to pushback expr", K(ret));
    } else if (check_valid && groupby_exprs_.at(i) == replaced_expr) {
      if (replaced_expr->is_const_expr()) {
        // ok
      } else {
        is_valid = false;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < aggr_exprs_.count(); ++i) {
    ObRawExpr*  replaced_expr;
    if (OB_FAIL(copier.copy_on_replace(aggr_exprs_.at(i), replaced_expr))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (OB_FAIL(replaced_aggr_exprs.push_back(replaced_expr))) {
      LOG_WARN("failed to pushback expr", K(ret));
    } else if (check_valid && aggr_exprs_.at(i) == replaced_expr) {
      if (replaced_expr->is_const_expr()) {
        // ok
      } else {
        is_valid = false;
      }
    }
  }

  if (OB_FAIL(ret) || !is_valid) {
  } else {
    // do formalize for all exprs
    groupby_exprs_.reuse();
    aggr_exprs_.reuse();
    if (OB_FAIL(groupby_exprs_.assign(replaced_exprs))) {
      LOG_WARN("failed to assign group by exprs", K(ret));
    } else if (OB_FAIL(aggr_exprs_.assign(replaced_aggr_exprs))) {
      LOG_WARN("failed to assign aggr exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs_.count(); ++i) {
      if (OB_FAIL(groupby_exprs_.at(i)->formalize(session_info))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(groupby_exprs_.at(i)->pull_relation_id())) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); ++i) {
      if (OB_FAIL(aggr_exprs_.at(i)->formalize(session_info))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(aggr_exprs_.at(i)->pull_relation_id())) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObGroupByPushdownContext::map(const common::ObIArray<ObRawExpr *> &from_exprs,
                                const common::ObIArray<ObRawExpr *> &to_exprs,
                                ObRawExprFactory &expr_factory,
                                const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  if (OB_FAIL(this->map_and_check(from_exprs, to_exprs, expr_factory, session_info, false, is_valid))) {
    LOG_WARN("failed to map group by context", K(ret));
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_groupby(ObLogGroupBy *groupby,
                                                 ObGroupByPushdownContext *ctx,
                                                 ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_push = false;
  if (OB_ISNULL(groupby) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument(s)", K(ret), KP(groupby), KP(result));
  } else if (OB_FAIL(can_pushdown_groupby(groupby, can_push))) {
    LOG_WARN("failed to check if can pushdown groupby", K(ret));
  } else if (can_push) {
    ObGroupByPushdownContext new_ctx;
    ObGroupByPushdownResult new_result;
    ObGroupByPushdownResult* dummy_pointer = &new_result;
    OPT_TRACE("start a groupby pushdown process at groupby op", groupby->get_op_id(), ":");
    OPT_TRACE_BEGIN_SECTION;
    if (OB_FAIL(generate_context(groupby, ctx, new_ctx))) {
      LOG_WARN("failed to generate context", K(ret));
    } else if (OB_FAIL(rewrite_single_child(groupby, &new_ctx, dummy_pointer))) {
      LOG_WARN("failed to rewrite child", K(ret));
    } else if (new_result.is_materialized_ && OB_FAIL(rewrite_groupby_node(groupby, &new_ctx, &new_result))) {
      LOG_WARN("failed to rewrite groupby node", K(ret));
    } else {
      // rewrite groupby node
    }
    OPT_TRACE_END_SECTION;
    OPT_TRACE("groupby pushdown process for groupby op", groupby->get_op_id(), "finished");
  } else {
    if (OB_FAIL(default_rewrite_children(groupby, result))) {
      LOG_WARN("failed to trigger rewrite children", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_join(ObLogJoin *join,
                                              ObGroupByPushdownContext *context,
                                              ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  bool can_push_to_left = false;
  bool can_push_to_right = false;
  if (OB_ISNULL(join) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(join), KP(result));
  } else if (join->get_num_of_child() != 2 || OB_ISNULL(join->get_child(0)) || OB_ISNULL(join->get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join", K(ret), KP(join), K(join->get_num_of_child()), KP(join->get_child(0)), KP(join->get_child(1)));
  } else if (NULL == context || context->is_empty() || join->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else {
    ObGroupByPushdownContext left_ctx;
    ObGroupByPushdownContext right_ctx;
    ObGroupByPushdownResult left_result;
    ObGroupByPushdownResult right_result;
    ObGroupByPushdownResult* dummy_pointer_left = &left_result;
    ObGroupByPushdownResult* dummy_pointer_right = &right_result;
    OPT_TRACE("try pushdown groupby through join op: ", join->get_op_id());
    if (OB_FAIL(compute_push_through_join_params(join,
                                                 context,
                                                 left_ctx,
                                                 right_ctx,
                                                 can_push_to_left,
                                                 can_push_to_right))) {
      LOG_WARN("failed to compute push through join params", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (can_push_to_left) {
      if (OB_FAIL(SMART_CALL(rewrite_child(join->get_child(0), &left_ctx, dummy_pointer_left)))) {
        LOG_WARN("failed to rewrite left child", K(ret));
      }
    } else if (OB_FAIL(default_rewrite_child(join->get_child(0), &left_result))) {
      LOG_WARN("failed to trigger rewrite child", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (can_push_to_right) {
      if (OB_FAIL(SMART_CALL(rewrite_child(join->get_child(1), &right_ctx, dummy_pointer_right)))) {
        LOG_WARN("failed to rewrite right child", K(ret));
      }
    } else if (OB_FAIL(default_rewrite_child(join->get_child(1), &right_result))) {
      LOG_WARN("failed to trigger rewrite child", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (!left_result.is_materialized_ && !right_result.is_materialized_) {
      OPT_TRACE("Neither left nor right child are materialized, skip");
    } else if (OB_FAIL(rewrite_join_node(join,
                                         can_push_to_left,
                                         can_push_to_right,
                                         &left_ctx,
                                         &left_result,
                                         &right_ctx,
                                         &right_result,
                                         context,
                                         result))) {
      LOG_WARN("failed to replace exprs for join", K(ret));
    } else {
      result->is_materialized_ = true;
    }
    // 暂时决定在child下压成功后不进一步下压
    if (OB_SUCC(ret) && !result->is_materialized_ && OB_FAIL(try_place_groupby(join, context, result))) {
      LOG_WARN("failed to try place groupby", K(ret));
    }
  }

  if (OB_SUCC(ret) && !can_transform) {
    // default behavior: trigger rewrite for children
    if (OB_FAIL(default_rewrite_children(join, result))) {
      LOG_WARN("failed to trigger rewrite children", K(ret));
    }
    // TODO tuliwei.tlw: 好像还是应该在这里尝试放置一个groupby的
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_exchange(ObLogExchange *exchange,
                                                  ObGroupByPushdownContext *ctx,
                                                  ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(exchange) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument(s)", K(ret), KP(exchange), KP(result));
  } else if (exchange->get_num_of_child() != 1 || OB_ISNULL(exchange->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exchange", K(ret), KP(exchange), K(exchange->get_num_of_child()), KP(exchange->get_child(0)));
  } else if (NULL == ctx || ctx->is_empty() || exchange->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (OB_FAIL(can_push_through_exchange(exchange, ctx, can_transform))) {
    LOG_WARN("failed to check if can push through exchange", K(ret));
  } else if (!can_transform) {
    OPT_TRACE("can not push through exchange: ", exchange->get_name(), exchange->get_op_id());
    // do nothing
  } else if (exchange->is_consumer()) {
    // TODO tuliwei.tlw: use rewrite_single_child
    if (OB_FAIL(SMART_CALL(rewrite_child(exchange->get_child(0), ctx, result)))) {
      LOG_WARN("failed to rewrite child", K(ret));
    } else if (!result->is_materialized_ && OB_FAIL(try_place_groupby(exchange, ctx, result))) {
      LOG_WARN("failed to try place groupby", K(ret));
    }
  } else /* producer */ {
    ObGroupByPushdownContext pushdown_ctx;
    if (OB_FAIL(pushdown_ctx.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));
    } else if (OB_FAIL(set_context_flags_for_exchange(exchange, &pushdown_ctx))) {
      LOG_WARN("failed to set context flags for exchange", K(ret));
    } else if (OB_FAIL(rewrite_child(exchange->get_child(0), &pushdown_ctx, result))) {
      LOG_WARN("failed to rewrite child", K(ret));
    }
  }

  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(default_rewrite_children(exchange, result))) {
      LOG_WARN("failed to trigger rewrite children", K(ret));
    } else if (NULL != ctx && !ctx->is_empty() && exchange->is_consumer()) {
      if (OB_FAIL(try_place_groupby(exchange, ctx, result))) {
        LOG_WARN("failed to try place groupby", K(ret));
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_material(ObLogMaterial *material,
                                                  ObGroupByPushdownContext *ctx,
                                                  ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(material) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument(s)", K(ret), KP(material), KP(result));
  } else if (material->get_num_of_child() != 1 || OB_ISNULL(material->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected material", K(ret), KP(material), K(material->get_num_of_child()), KP(material->get_child(0)));
  } else if (NULL == ctx || ctx->is_empty() || material->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (!can_transform) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(rewrite_child(material->get_child(0), ctx, result)))) {
    LOG_WARN("failed to rewrite child", K(ret));
  } else if (!result->is_materialized_ && OB_FAIL(try_place_groupby(material, ctx, result))) {
    LOG_WARN("failed to try place groupby", K(ret));
  } else {
  }

  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(material, ctx, result))) {
      LOG_WARN("failed to do default visit node for material", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_table_scan(ObLogTableScan *table_scan,
                                                    ObGroupByPushdownContext *ctx,
                                                    ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(table_scan) || OB_ISNULL(result) || OB_ISNULL(table_scan->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(table_scan), KP(result), KP(table_scan->get_plan()));
  } else if (NULL == ctx || ctx->is_empty()) {
    can_transform = false;
    OPT_TRACE("ctx is empty, skip table scan: ", table_scan->get_name(), table_scan->get_op_id());
  } else if (!table_scan->get_pushdown_aggr_exprs().empty()) {
    can_transform = false;
    OPT_TRACE("table scan already has pushdown aggr exprs, skip: ", table_scan->get_name(), table_scan->get_op_id());
  } else {
    ObSEArray<ObRawExpr *, 4> pushdown_groupby_columns;
    bool can_pushdown = false;
    ObArray<ObAggFunRawExpr *> aggr_items;
    OPT_TRACE("try pushdown groupby into storage: ", table_scan->get_name(), table_scan->get_op_id());
    if (OB_FAIL(ctx->get_aggr_items(aggr_items))) {
      LOG_WARN("failed to get aggr context", K(ret));
    } else if (OB_FAIL(table_scan->get_plan()->
                        check_storage_groupby_pushdown(aggr_items, ctx->groupby_exprs_,
                                                      pushdown_groupby_columns,
                                                      can_pushdown))) {
      LOG_WARN("failed to check aggr storage pushdown", K(ret));
    } else if (can_pushdown
               && OB_FAIL(table_scan->get_plan()->try_push_aggr_into_table_scan(table_scan,
                                                                                aggr_items,
                                                                                ctx->groupby_exprs_))) {
      LOG_WARN("failed to pushdown aggr into scan", K(ret));
    } else {
      if (!table_scan->get_pushdown_aggr_exprs().empty()) {
        OPT_TRACE("pushed down aggr exprs into storage: ", table_scan->get_name(), table_scan->get_op_id());
        // if pushed down, need to update result
        result->is_materialized_ = true;
        if (OB_FAIL(result->new_aggr_exprs_.assign(ctx->aggr_exprs_))) {
          LOG_WARN("failed to assign new aggr exprs", K(ret));
        }
      } else {
        OPT_TRACE("no aggr exprs pushed down into storage");
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(try_place_groupby(table_scan, ctx, result))) {
        LOG_WARN("failed to add partial group by", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(table_scan, ctx, result))) {
      LOG_WARN("failed to do default visit node for table scan", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_subplan_scan(ObLogSubPlanScan *subplan_scan,
                                                     ObGroupByPushdownContext *ctx,
                                                     ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  bool not_valid = false;
  const ObDMLStmt *child_stmt = NULL;
  const ObSelectStmt *child_select_stmt = NULL;
  // child of subplan scan is root
  ObLogicalOperator* child_op = subplan_scan->get_child(0);
  if (OB_ISNULL(subplan_scan) || OB_ISNULL(result) || OB_ISNULL(child_op) || OB_ISNULL(child_op->get_plan())
      || OB_ISNULL(subplan_scan->get_plan()) || OB_ISNULL(child_stmt = child_op->get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(subplan_scan), KP(result), KP(child_op),
             KP(child_op->get_plan()), KP(subplan_scan->get_plan()), KP(child_stmt));
  } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
  } else if (OB_ISNULL(child_select_stmt = static_cast<const ObSelectStmt *>(child_stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast select stmt", K(ret));
  } else if (NULL == ctx || ctx->is_empty() || subplan_scan->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (OB_FAIL(ObOptimizerUtil::contains_virtual_column(child_op, not_valid))) {
    LOG_WARN("failed to check virtual columns", K(ret));
  } else if (not_valid) {
    OPT_TRACE("child stmt contains virtual column, cannot push through");
    can_transform = false;
  } else if (OB_FAIL(ObOptimizerUtil::contains_group_by(child_op, not_valid))) {
    LOG_WARN("failed to check virtual columns", K(ret));
  } else if (not_valid) {
    OPT_TRACE("child stmt contains group by, cannot push through");
    can_transform = false;
  } else if (OB_FALSE_IT(not_valid = child_select_stmt->is_unpivot_select())) {
  } else if (not_valid) {
    OPT_TRACE("child stmt is unpivot select, cannot push through");
    can_transform = false;
  } else {
    // current context
    // sum(v1.c1) , count(v1.c2)
    // output -> input
    // v1.c1 -> t1.c1, v1.c2 -> v2.c2
    // map context: output -> input
    //              sum(v1.c1) -> sum(t1.c1)
    //              count(v1.c2) -> count(v2.c2)
    // child result is
    //              sum(t1.c1) -> sum(t1.c1)
    //              count(v2.c2) ->  v2.'count(t2.c2)'
    // add select item: select -> column
    //               sum(t1.c1) -> v1.'sum(t1.c1)'
    //               v2.'count(t2.c2)' ->  v1.'v2.count(t2.c2)'
    // map result:  new column by map context
    //              sum(v1.c1) -> v1.'sum(t1.c1)'
    //              count(v1.c2) -> v1.'v2.count(t2.c2)'
    ObSEArray<ObRawExpr*, 4> output_cols;
    ObSEArray<ObRawExpr*, 4> input_cols;
    ObSEArray<ObRawExpr*, 4> dependent_exprs;
    ObGroupByPushdownContext child_ctx;
    ObGroupByPushdownResult child_result;
    ObGroupByPushdownResult* dummy_pointer = &child_result;
    bool is_valid = false;
    if (OB_FAIL(child_ctx.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));
    } else if (OB_FAIL(child_ctx.get_dependent_exprs(dependent_exprs))) {
      LOG_WARN("failed to extract dependent exprs from pushdown context", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_subplan_scan_output_to_input_mapping(*child_select_stmt,
                                                                                  dependent_exprs,
                                                                                  input_cols,
                                                                                  output_cols))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (OB_FAIL(child_ctx.map_and_check(output_cols,
                                               input_cols,
                                               opt_ctx_.get_expr_factory(),
                                               opt_ctx_.get_session_info(),
                                               true,
                                               is_valid))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (!is_valid) {
      OPT_TRACE("check failed when mapping dependent exprs, cannot push through");
      can_transform = false;
    }
    if (OB_FAIL(ret) || !can_transform) {
    } else if (OB_FAIL(SMART_CALL(rewrite_child(child_op, &child_ctx, dummy_pointer)))) {
      LOG_WARN("failed to rewrite child", K(ret), K(child_op->get_name()));
    } else {
      if (child_result.is_materialized_) {
        OPT_TRACE("child result is materialized, add new column for view: ", subplan_scan->get_name(), subplan_scan->get_op_id());
        // add new column for this view
        // using ObTransformUtils::create_columns_for_view
        // ObTransformUtils::create_new_column_expr
        ObSEArray<ObRawExpr*, 4> new_columns_list;
        ObSEArray<ObRawExpr*, 4> new_select_list;
        // add mapped aggr to select list of view
        // add column to table item
        if (OB_FAIL(append(new_select_list, child_result.new_aggr_exprs_))) {
          LOG_WARN("failed to append append exprs to select list", K(ret));
        } else if (child_ctx.need_count_ && OB_ISNULL(child_result.count_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null count expr", K(ret), KP(child_result.count_expr_));
        } else if (child_ctx.need_count_ && OB_FAIL(new_select_list.push_back(child_result.count_expr_))) {
          LOG_WARN("failed to append count expr to select list", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::add_new_select_items_to_view(
                  opt_ctx_.get_session_info(),
                  child_op->get_plan()->get_allocator(),
                  &opt_ctx_.get_expr_factory(),
                  opt_ctx_.get_exec_ctx()->get_physical_plan_ctx(),
                  *subplan_scan->get_plan()->get_stmt()->get_table_item_by_id(subplan_scan->get_subquery_id()),
                  *const_cast<ObDMLStmt *>(subplan_scan->get_plan()->get_stmt()),
                  new_select_list,
                  new_columns_list))) {
          LOG_WARN("failed to add new select items to view");
        }
        if (OB_SUCC(ret) && child_ctx.need_count_) {
          if (OB_ISNULL(result->count_expr_ = new_columns_list.at(new_columns_list.count() - 1))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null count expr", K(ret));
          } else {
            new_columns_list.pop_back();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(result->new_aggr_exprs_.assign(new_columns_list))) {
          LOG_WARN("failed to assign new aggr exprs", K(ret));
        } else {
          // build result from child result and return
          // todo do we need to do a copy or what?
          result->is_materialized_ = true;
        }
      }
      if (OB_FAIL(ret) || result->is_materialized_) {
      } else if (OB_FAIL(try_place_groupby(subplan_scan, ctx, result))) {
        LOG_WARN("failed to try place groupby", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(subplan_scan, ctx, result))) {
      LOG_WARN("failed to do default visit node for subplan scan", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_set(ObLogSet *set,
                                            ObGroupByPushdownContext *ctx,
                                            ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(set) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(set), KP(result));
  } else if (NULL == ctx || ctx->is_empty() || set->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (set->is_recursive_union()) {
    can_transform = false;
    OPT_TRACE("set is recursive union, can not push through");
  } else if (!(ObSelectStmt::UNION == set->get_set_op() && !set->is_set_distinct())) {
    // only union all
    can_transform = false;
    OPT_TRACE("set is not union all, can not push through");
  } else {
    bool has_pushed_down = false;
    // union all
    // do a mapping
    // current context
    // sum(v1.c1)
    // output -> input
    // v1.c1 -> t1.c1, v1.c1 -> t2.c1
    // map context: output -> input
    //              child0 : sum(v1.c1) -> sum(t1.c1)
    //              child1 : sum(v1.c1) -> sum(t2.c1)
    // child result is
    //              child0 :sum(t1.c1) -> sum(t1.c1)
    //              child1 :sum(t2.c1) -> sum(t2.c1)
    // add select item to child stmt: select -> view column
    //              child0: sum(t1.c1) -> 'sum(t1.c1)'
    //              child1: sum(t2.c1) -> 'sum(t2.c1)'
    // map result:  new column by map context
    //              sum(v1.c1) -> 'UNION([idx]'
    //              'UNION([idx]' is added to select item of union (what if union is in the same stage with agg)?
    // T_OP_UNION
    const ObSelectStmt *stmt = NULL;
    const ObSelectStmt *pushdown_child_stmt = NULL;
    ObIAllocator &allocator = opt_ctx_.get_allocator();
    int64_t child_count = set->get_num_of_child();
    ObFixedArray<ObGroupByPushdownContext, ObIAllocator> child_contexts(allocator, child_count);
    ObFixedArray<ObGroupByPushdownResult, ObIAllocator> child_results(allocator, child_count);
    OPT_TRACE("try rewrite set children: ", set->get_name(), set->get_op_id());
    if (OB_ISNULL(stmt = static_cast<const ObSelectStmt *>(set->get_stmt()))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(stmt), K(ret));
    } else if (OB_FAIL(child_contexts.prepare_allocate(child_count))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(child_results.prepare_allocate(child_count))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else if (OB_FAIL(try_rewrite_set_children(set, ctx, child_contexts, child_results, has_pushed_down, pushdown_child_stmt))) {
      LOG_WARN("failed to try rewrite set children", K(ret));
    } else if (!has_pushed_down) {
      OPT_TRACE("none of children pushed down, do nothing");
      // do nothing
    } else if (OB_FAIL(add_projection_for_set_children(set, child_contexts, child_results))) {
      LOG_WARN("failed to add projection for set children", K(ret));
    } else if (OB_FAIL(rewrite_set_node(set, ctx, pushdown_child_stmt, result))) {
      LOG_WARN("failed to rewrite set node", K(ret));
    // Calculating the NDV (number of distinct values) for union expressions is extremely troublesome,
    // so disabling place groupby above union all for now
    // } else if (!result->is_materialized_ && OB_FAIL(try_place_groupby(set, ctx, result))) {
    //   LOG_WARN("failed to try place groupby", K(ret));
    }
  }

  if (OB_SUCC(ret) && !can_transform) {
    // Calculating the NDV (number of distinct values) for union expressions is extremely troublesome,
    // so disabling place groupby above union all for now
    if (OB_FAIL(default_rewrite_children(set, result))) {
      LOG_WARN("failed to trigger rewrite children", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::visit_node(ObLogicalOperator *op,
                                              ObGroupByPushdownContext *context,
                                              ObGroupByPushdownResult *&result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(op) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(op), KP(result));
  } else if (OB_FAIL(default_rewrite_children(op, result))) {
    LOG_WARN("failed to trigger rewrite children", K(ret));
  } else if (NULL != context && !context->is_empty()) {
    if (OB_FAIL(try_place_groupby(op, context, result))) {
      LOG_WARN("failed to try place groupby", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::default_rewrite_children(ObLogicalOperator *op, ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(op), KP(result));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    ObLogicalOperator *child = op->get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(child));
    } else if (OB_FAIL(SMART_CALL(rewrite_child(child, NULL, result)))) {
      LOG_WARN("failed to rewrite child", K(ret), KP(child));
    } else if (result->is_materialized_) {
      // Default rewriting is not expected to return a materialized result
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected materialized result", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    OPT_TRACE("default rewrite children for", op->get_name(), op->get_op_id());
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::default_rewrite_child(ObLogicalOperator *child_op, ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(child_op), KP(result));
  } else if (OB_FAIL(SMART_CALL(rewrite_child(child_op, NULL, result)))) {
    LOG_WARN("failed to rewrite child", K(ret), KP(child_op));
  } else if (result->is_materialized_) {
    // Default rewriting is not expected to return a materialized result
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected materialized result", K(ret));
  } else {
    OPT_TRACE("default rewrite child for", child_op->get_name(), child_op->get_op_id());
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::can_pushdown_groupby(ObLogGroupBy *groupby, bool &can_push)
{
  // TODO tuliwei.tlw: need to check more, such as if group by expr is deterministic, etc.
  int ret = OB_SUCCESS;
  can_push = false;
  ObLogPlan *plan = NULL;
  const ObDMLStmt *stmt = NULL;
  ObLogicalOperator *child = NULL;
  ObSEArray<ObAggFunRawExpr *, 4> aggr_items;
  if (OB_ISNULL(groupby)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(groupby));
  } else if (OB_ISNULL(plan = groupby->get_plan()) ||
             OB_ISNULL(stmt = plan->get_stmt()) ||
             OB_UNLIKELY(groupby->get_num_of_child() != 1) ||
             OB_ISNULL(child = groupby->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(plan), KP(stmt), KP(child));
  } else if (!stmt->is_select_stmt()) {
    can_push = false;
    OPT_TRACE("stmt is not select stmt, can not push");
  } else if (groupby->has_rollup()) {
    can_push = false;
    OPT_TRACE("groupby has rollup, can not push");
  } else if (groupby->get_algo() == MERGE_AGGREGATE && groupby->get_group_by_exprs().count() != 0) {
    can_push = false;
    OPT_TRACE("groupby algo is MERGE_AGGREGATE with group by exprs, can not push");
  } else if (!groupby->is_distributed()) {
    can_push = false;
    OPT_TRACE("groupby is not distributed, can not push");
  } else if (groupby->is_three_stage_aggr()) {
    OPT_TRACE("groupby is three stage aggr, skip");
    // final agg, do nothing
  } else {
    // 2. 收集聚合函数
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby->get_aggr_funcs().count(); ++i) {
      ObRawExpr *raw_expr = groupby->get_aggr_funcs().at(i);
      ObAggFunRawExpr *aggr_expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (OB_UNLIKELY(!raw_expr->is_aggr_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-aggr expr", K(ret));
      } else if (OB_FALSE_IT(aggr_expr = static_cast<ObAggFunRawExpr *>(raw_expr))) {
      } else if (OB_FAIL(aggr_items.push_back(aggr_expr))) {
        LOG_WARN("failed to push back aggr expr", K(ret));
      }
    }
    // 3. 检查是否满足 only_full_group_by
    bool is_only_full_group_by = false;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::check_stmt_is_only_full_group_by(
                        static_cast<const ObSelectStmt*>(stmt),
                        is_only_full_group_by))) {
      LOG_WARN("failed to check stmt is only full group by", K(ret));
    } else if (!is_only_full_group_by) {
      can_push = false;
      OPT_TRACE("stmt is not only full group by, can not push");
    } else if (OB_FAIL(plan->check_basic_groupby_pushdown(aggr_items,
                                                          false,  // use_grouping_sets_expansion
                                                          child->get_output_equal_sets(),
                                                          can_push))) {
      LOG_WARN("failed to check basic groupby pushdown", K(ret));
    }
    // 4. 检查有没有lob
    bool has_lob = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOptimizerUtil::contains_lob_type(groupby->get_group_by_exprs(), has_lob))) {
      LOG_WARN("failed to check contains lob type", K(ret));
    } else if (has_lob) {
      can_push = false;
      OPT_TRACE("groupby exprs contains lob type, can not push");
    }
  }

  if (OB_SUCC(ret)) {
    OPT_TRACE("check can pushdown groupby, can_push:", can_push);
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::generate_context(ObLogGroupBy *groupby,
                                                    ObGroupByPushdownContext *upper_ctx,
                                                    ObGroupByPushdownContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(groupby)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(groupby));
  } else if (OB_FAIL(ctx.groupby_exprs_.assign(groupby->get_group_by_exprs()))) {
    LOG_WARN("failed to assign groupby exprs", K(ret));
  } else {
    // 拷贝 aggr_exprs_的聚合节点，保证指针不同, 不然后续替换上层表达式的时候很麻烦。
    ObRawExprCopier copier(opt_ctx_.get_expr_factory());
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby->get_aggr_funcs().count(); ++i) {
      ObRawExpr *aggr_expr = groupby->get_aggr_funcs().at(i);
      ObRawExpr *new_aggr_expr = NULL;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null aggr expr", K(ret), K(i));
      } else if (OB_FAIL(copier.copy_expr_node(aggr_expr, new_aggr_expr))) {
        LOG_WARN("failed to copy aggr expr", K(ret), K(i));
      } else if (OB_FAIL(ctx.aggr_exprs_.push_back(new_aggr_expr))) {
        LOG_WARN("failed to push back aggr expr", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ctx.need_count_ = false;
      ctx.enable_reshuffle_ = (NULL != upper_ctx) ? upper_ctx->enable_reshuffle_ : false;
      OPT_TRACE("context created", groupby->get_op_id(),
                "aggr_exprs_: ", ctx.aggr_exprs_,
                "groupby_exprs_: ", ctx.groupby_exprs_,
                "enable_reshuffle_: ", ctx.enable_reshuffle_);
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::rewrite_groupby_node(ObLogGroupBy *groupby,
                                                        ObGroupByPushdownContext *ctx,
                                                        ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAggFunRawExpr *, 4> new_aggr_exprs;
  if (OB_ISNULL(groupby) || OB_ISNULL(ctx) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(groupby), KP(ctx), KP(result));
  } else if (result->is_materialized_) {
    const ObIArray<ObRawExpr *> &old_aggr_exprs = groupby->get_aggr_funcs();
    OPT_TRACE("rewrite groupby node",
              "ctx->aggr_exprs_: ", ctx->aggr_exprs_,
              "result->new_aggr_exprs: ", result->new_aggr_exprs_);
    if (OB_UNLIKELY(ctx->need_count_) || OB_UNLIKELY(old_aggr_exprs.count() != ctx->aggr_exprs_.count())
        || OB_UNLIKELY(result->new_aggr_exprs_.count() != ctx->aggr_exprs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count of aggr exprs", K(ret), K(old_aggr_exprs.count()),
                    K(ctx->aggr_exprs_.count()), K(result->new_aggr_exprs_.count()));
    } else {
      if (!groupby->has_push_down() && !groupby->is_push_down()) {
        // todo set final
        groupby->set_step_final();
      }
      // 将改写发生后的替换关系存起来，后续有一个阶段会集中替换聚合表达式(ObLogPlan::perform_group_by_pushdown)
      for (int64_t i = 0; OB_SUCC(ret) && i < groupby->get_aggr_funcs().count(); ++i) {
        if (OB_FAIL(groupby->get_plan()->
                    add_overwrite_group_replaced_exprs(
                      std::pair<ObRawExpr *, ObRawExpr *>(groupby->get_aggr_funcs().at(i),
                                                          result->new_aggr_exprs_.at(i))))) {
          LOG_WARN("failed push back overwrite mapping expr pair");
        }
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::compute_push_through_join_params(ObLogJoin *join,
                                                                    ObGroupByPushdownContext *ctx,
                                                                    ObGroupByPushdownContext &left_ctx,
                                                                    ObGroupByPushdownContext &right_ctx,
                                                                    bool &can_push_to_left,
                                                                    bool &can_push_to_right)
{
  // TODO tuliwei.tlw: check this function more carefully
  int ret = OB_SUCCESS;
  can_push_to_left = false;
  can_push_to_right = false;
  ObLogPlan *plan = NULL;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  ObSEArray<ObRawExpr *, 4> left_join_keys;
  ObSEArray<ObRawExpr *, 4> right_join_keys;
  bool is_left_not_valid = false;
  bool is_right_not_valid = false;
  bool join_is_valid = false;
  if (OB_ISNULL(join) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(join), KP(ctx));
  } else if (OB_ISNULL(plan = join->get_plan()) ||
             OB_ISNULL(left_child = join->get_child(0)) ||
             OB_ISNULL(right_child = join->get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(plan), KP(left_child), KP(right_child));
  } else {

    ObJoinType join_type = join->get_join_type();
    // 根据 JOIN 类型判断是否可以下推
    bool try_push_to_left = (INNER_JOIN == join_type ||
                             LEFT_OUTER_JOIN == join_type ||
                             (LEFT_SEMI_JOIN == join_type) ||
                             (LEFT_ANTI_JOIN == join_type));
    bool try_push_to_right = (INNER_JOIN == join_type ||
                              RIGHT_OUTER_JOIN == join_type ||
                              (RIGHT_SEMI_JOIN == join_type) ||
                              (RIGHT_ANTI_JOIN == join_type));
    // 对于 MERGE_JOIN，不做处理，因为 MERGE_JOIN 需要保持顺序
    if (MERGE_JOIN == join->get_join_algo()) {
      OPT_TRACE("join algo is MERGE_JOIN, can not push");
    } else if (!try_push_to_left && !try_push_to_right) {
      OPT_TRACE("join type is not supported, can not push");
    } else if (OB_FAIL(get_join_keys(join, left_join_keys, right_join_keys, join_is_valid))) {
      LOG_WARN("failed to get join keys", K(ret));
    } else if (!join_is_valid) {
      OPT_TRACE("join keys are not valid, can not push");
    } else if (OB_FAIL(ObOptimizerUtil::contains_lob_type(left_join_keys, is_left_not_valid))) {
      LOG_WARN("failed to check contains lob type", K(ret));
    } else if (OB_FALSE_IT(try_push_to_left = try_push_to_left && !is_left_not_valid)) {
    } else if (OB_FAIL(ObOptimizerUtil::contains_lob_type(right_join_keys, is_right_not_valid))) {
      LOG_WARN("failed to check contains lob type", K(ret));
    } else if (OB_FALSE_IT(try_push_to_right = try_push_to_right && !is_right_not_valid)) {
    } else if (!try_push_to_right && !try_push_to_left) {
      OPT_TRACE("contains lob type in join keys, can not push");
    } else {
      bool can_push = true;
      bool disable_reshuffle = join->get_dist_method() != DistAlgo::DIST_BC2HOST_NONE
                              && join->get_dist_method() != DistAlgo::DIST_BROADCAST_NONE
                              && join->get_dist_method() != DistAlgo::DIST_NONE_BROADCAST;
      left_ctx.reset();
      right_ctx.reset();
      can_push_to_left = try_push_to_left;
      can_push_to_right = try_push_to_right;
      left_ctx.need_count_ = ctx->need_count_;
      right_ctx.need_count_ = ctx->need_count_;
      left_ctx.enable_reshuffle_ = ctx->enable_reshuffle_ && !disable_reshuffle;
      right_ctx.enable_reshuffle_ = ctx->enable_reshuffle_ && !disable_reshuffle;
      left_ctx.enable_place_groupby_ = true;
      right_ctx.enable_place_groupby_ = true;
      left_ctx.benefit_from_reshuffle_ = true;
      right_ctx.benefit_from_reshuffle_ = true;
      if (OB_FAIL(distribute_groupby_exprs(ctx->groupby_exprs_,
                                           left_child,
                                           right_child,
                                           left_ctx,
                                           right_ctx,
                                           can_push))) {
        LOG_WARN("failed to distribute groupby exprs", K(ret));
      } else if (!can_push) {
        OPT_TRACE("groupby exprs is not subset of any child table set, can not push");
      } else if (OB_FAIL(append_array_no_dup(left_ctx.groupby_exprs_, left_join_keys))) {
        LOG_WARN("failed to assign left join keys", K(ret));
      } else if (OB_FAIL(append_array_no_dup(right_ctx.groupby_exprs_, right_join_keys))) {
        LOG_WARN("failed to assign right join keys", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && can_push && i < ctx->aggr_exprs_.count(); ++i) {
        ObRawExpr *aggr_expr = ctx->aggr_exprs_.at(i);
        bool is_from_single_side = false;
        if (OB_ISNULL(aggr_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null aggr expr", K(ret));
        } else if (aggr_expr->get_expr_type() != T_FUN_SUM &&
                   aggr_expr->get_expr_type() != T_FUN_COUNT &&
                   aggr_expr->get_expr_type() != T_FUN_MIN &&
                   aggr_expr->get_expr_type() != T_FUN_MAX) {
          OPT_TRACE("aggr expr is not supported, can not push", aggr_expr);
          can_push = false;
        } else if (OB_FAIL(distribute_aggr_expr(aggr_expr,
                                                left_child,
                                                right_child,
                                                left_ctx,
                                                right_ctx,
                                                is_from_single_side))) {
          LOG_WARN("failed to distribute aggr expr", K(ret));
        } else if (is_from_single_side) {
          // already distributed to single side, continue to next aggr expr
        } else {
          // not from single side, need to check mask aggr expr
          int64_t left_groupby_count = left_ctx.groupby_exprs_.count();
          int64_t right_groupby_count = right_ctx.groupby_exprs_.count();
          if (OB_FAIL(try_pushdown_as_mask_aggr_expr(aggr_expr,
                                                     left_child,
                                                     right_child,
                                                     left_ctx,
                                                     right_ctx,
                                                     ctx,
                                                     can_push))) {
            LOG_WARN("failed to try pushdown as mask aggr expr", K(ret));
          } else {
            // TODO tuliwei.tlw
            // 暂时禁用 eager count 侧的 mask aggr expr 下推，为了减少改动，先使用这种trick的方法判断
            if (left_groupby_count != left_ctx.groupby_exprs_.count()) {
              OPT_TRACE("left groupby count changed, can not push");
              can_push_to_left = false;
            }
            if (right_groupby_count != right_ctx.groupby_exprs_.count()) {
              OPT_TRACE("right groupby count changed, can not push");
              can_push_to_right = false;
            }
          }
        }
      }
      if (OB_FAIL(ret)){
      } else if (!can_push) {
        OPT_TRACE("groupby can not be pushed down through join op", join->get_op_id());
        // groupby exprs or aggr exprs is not subset of any child table set, or aggr expr is not supported
        can_push_to_left = false;
        can_push_to_right = false;
        left_ctx.reset();
        right_ctx.reset();
      } else {
        // TODO tuliwei.tlw 这里如果左侧ctx有被分配聚合表达式或groupby表达式，但是被禁用，会有问题
        // 现在先work_around，后续放开 eager count 侧 mask aggr expr 下推后就没这个问题了
        if (!can_push_to_left) {
          // TODO tuliwei.tlw this is working around code
          if (left_ctx.aggr_exprs_.count() > 0) {
            can_push_to_right = false;
          }
          left_ctx.reset();
        }
        if (!can_push_to_right) {
          // TODO tuliwei.tlw this is working around code
          if (right_ctx.aggr_exprs_.count() > 0) {
            can_push_to_left = false;
          }
          right_ctx.reset();
        }
        OPT_TRACE("distribte context to left and right child of join op", join->get_op_id(), ":");
        OPT_TRACE_BEGIN_SECTION;
        OPT_TRACE("can_push_to_left: ", can_push_to_left, ", can_push_to_right: ", can_push_to_right);
        if (can_push_to_left) {
          OPT_TRACE("left context: ", left_ctx.groupby_exprs_, left_ctx.aggr_exprs_, left_ctx.need_count_);
        }
        if (can_push_to_right) {
          OPT_TRACE("right context: ", right_ctx.groupby_exprs_, right_ctx.aggr_exprs_, right_ctx.need_count_);
        }
        OPT_TRACE_END_SECTION;
      }
    }
  }

  return ret;
}

int ObGroupByPushDownPlanRewriter::distribute_groupby_expr(ObRawExpr *groupby_expr,
                                                             ObLogicalOperator *left_child,
                                                             ObLogicalOperator *right_child,
                                                             ObGroupByPushdownContext &left_ctx,
                                                             ObGroupByPushdownContext &right_ctx,
                                                             bool &can_push)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(groupby_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null groupby expr", K(ret));
  } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret), KP(left_child), KP(right_child));
  } else if (groupby_expr->get_relation_ids().is_subset(left_child->get_table_set())) {
    if (OB_FAIL(left_ctx.groupby_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back groupby expr", K(ret));
    }
  } else if (groupby_expr->get_relation_ids().is_subset(right_child->get_table_set())) {
    if (OB_FAIL(right_ctx.groupby_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back groupby expr", K(ret));
    }
  } else {
    OPT_TRACE("groupby expr is not subset of any child table set, can not push", groupby_expr);
    can_push = false;
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::generate_mask_aggr_param_expr(ObItemType aggr_type,
                                                                 ObRawExpr *param_expr,
                                                                 ObAggFunRawExpr *&new_param_expr)
{
  int ret = OB_SUCCESS;
  new_param_expr = NULL;
  if (OB_ISNULL(param_expr) || OB_ISNULL(opt_ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (aggr_type != T_FUN_SUM &&
             aggr_type != T_FUN_COUNT &&
             aggr_type != T_FUN_MIN &&
             aggr_type != T_FUN_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported aggr type for mask aggr", K(ret), K(aggr_type));
  } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(opt_ctx_.get_expr_factory(),
                                                            opt_ctx_.get_session_info(),
                                                            aggr_type,
                                                            param_expr,
                                                            new_param_expr))) {
    LOG_WARN("failed to build common aggr expr", K(ret));
  } else if (OB_ISNULL(new_param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null new param expr", K(ret));
  } else if (OB_FAIL(new_param_expr->formalize(opt_ctx_.get_session_info()))) {
    LOG_WARN("failed to formalize new param expr", K(ret));
  } else if (OB_FAIL(new_param_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id", K(ret));
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::distribute_aggr_expr(ObRawExpr *aggr_expr,
                                                          ObLogicalOperator *left_child,
                                                          ObLogicalOperator *right_child,
                                                          ObGroupByPushdownContext &left_ctx,
                                                          ObGroupByPushdownContext &right_ctx,
                                                          bool &is_from_single_side)
{
  int ret = OB_SUCCESS;
  is_from_single_side = true;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null aggr expr", K(ret));
  } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret), KP(left_child), KP(right_child));
  } else if (aggr_expr->get_relation_ids().is_subset(left_child->get_table_set())) {
    right_ctx.need_count_ |= aggr_need_count(aggr_expr);
    if (OB_FAIL(left_ctx.aggr_exprs_.push_back(aggr_expr))) {
      LOG_WARN("failed to push back aggr expr", K(ret));
    }
  } else if (aggr_expr->get_relation_ids().is_subset(right_child->get_table_set())) {
    left_ctx.need_count_ |= aggr_need_count(aggr_expr);
    if (OB_FAIL(right_ctx.aggr_exprs_.push_back(aggr_expr))) {
      LOG_WARN("failed to push back aggr expr", K(ret));
    }
  } else {
    is_from_single_side = false;
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::try_pushdown_as_mask_aggr_expr(ObRawExpr *aggr_expr,
                                                                    ObLogicalOperator *left_child,
                                                                    ObLogicalOperator *right_child,
                                                                    ObGroupByPushdownContext &left_ctx,
                                                                    ObGroupByPushdownContext &right_ctx,
                                                                    ObGroupByPushdownContext *ctx,
                                                                    bool &can_push)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(aggr_expr), KP(ctx));
  } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret), KP(left_child), KP(right_child));
  } else if (aggr_expr->get_param_count() != 1) {
    can_push = false;
    OPT_TRACE("aggr expr param count is not 1, can not push as mask aggr", aggr_expr);
  } else if (OB_ISNULL(aggr_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param expr", K(ret));
  } else if (!aggr_expr->get_param_expr(0)->is_case_op_expr()) {
    can_push = false;
    OPT_TRACE("aggr expr param is not case op expr, can not push as mask aggr", aggr_expr);
  } else {
    ObCaseOpRawExpr *case_expr = static_cast<ObCaseOpRawExpr *>(aggr_expr->get_param_expr(0));
    ObRawExpr *mask_expr = NULL;
    ObRawExpr *param_expr = NULL;
    ObRawExpr *default_expr = NULL;
    if (case_expr->get_when_expr_size() != 1 || case_expr->get_then_expr_size() != 1) {
      can_push = false;
      OPT_TRACE("mask aggr case expr has multiple when/then branches, can not push as mask aggr", aggr_expr);
    } else if (OB_ISNULL(mask_expr = case_expr->get_when_param_expr(0))
               || OB_ISNULL(param_expr = case_expr->get_then_param_expr(0))
               || OB_ISNULL(default_expr = case_expr->get_default_param_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null mask expr or param expr", K(ret));
    } else if (!default_expr->is_static_const_expr()
               || !static_cast<ObConstRawExpr *>(default_expr)->get_param().is_null()) {
      can_push = false;
      OPT_TRACE("default expr is not null, can not push as mask aggr", aggr_expr);
    } else if (OB_FAIL(distribute_groupby_expr(mask_expr, left_child, right_child, left_ctx, right_ctx, can_push))) {
      LOG_WARN("failed to distribute groupby expr", K(ret));
    } else if (!can_push) {
      // do nothing
    } else {
      OPT_TRACE("try to pushdown as mask aggr expr", aggr_expr);
      // generate and distribute mask aggr param expr
      if (OB_FAIL(distribute_mask_aggr_param_expr(aggr_expr,
                                                  param_expr,
                                                  left_child,
                                                  right_child,
                                                  left_ctx,
                                                  right_ctx,
                                                  ctx,
                                                  can_push))) {
        LOG_WARN("failed to distribute mask aggr param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::distribute_mask_aggr_param_expr(ObRawExpr *aggr_expr,
                                                                   ObRawExpr *param_expr,
                                                                   ObLogicalOperator *left_child,
                                                                   ObLogicalOperator *right_child,
                                                                   ObGroupByPushdownContext &left_ctx,
                                                                   ObGroupByPushdownContext &right_ctx,
                                                                   ObGroupByPushdownContext *ctx,
                                                                   bool &can_push)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *new_param_expr = NULL;
  can_push = true;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(param_expr) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(aggr_expr), KP(param_expr), KP(ctx));
  } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret), KP(left_child), KP(right_child));
  } else if (OB_UNLIKELY(!aggr_expr->is_aggr_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_expr is not aggr expr", K(ret));
  } else {
    ObAggFunRawExpr *aggr_fun_expr = static_cast<ObAggFunRawExpr *>(aggr_expr);
    ObItemType aggr_type = aggr_fun_expr->get_expr_type();
    // 先检查是否已经有相同 param_expr 的 mask_info
    ObRawExpr *existing_new_param_expr = NULL;
    bool found_existing = ctx->find_existing_mask_aggr_param_expr(param_expr, existing_new_param_expr);
    if (found_existing) {
      OPT_TRACE("reuse existing mask aggr param expr", param_expr, existing_new_param_expr);
      // 复用现有的 new_param_expr，不需要再次 distribute
      new_param_expr = static_cast<ObAggFunRawExpr *>(existing_new_param_expr);
    } else {
      OPT_TRACE("generate new mask aggr param expr", param_expr);
      // 生成新的 new_param_expr
      if (OB_FAIL(generate_mask_aggr_param_expr(aggr_type,
                                                param_expr,
                                                new_param_expr))) {
        LOG_WARN("failed to generate mask aggr param expr", K(ret));
      } else if (OB_ISNULL(new_param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null new param expr", K(ret));
      } else {
        // 只有新生成的 new_param_expr 才需要 distribute
        bool param_is_from_single_side = false;
        if (OB_FAIL(distribute_aggr_expr(new_param_expr,
                                         left_child,
                                         right_child,
                                         left_ctx,
                                         right_ctx,
                                         param_is_from_single_side))) {
          LOG_WARN("failed to distribute new param expr", K(ret));
        } else if (!param_is_from_single_side) {
          can_push = false;
          OPT_TRACE("new param expr is not from single side, can not push", new_param_expr);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null new param expr", K(ret));
    } else if (!can_push) {
      // can_push 已经被设置为 false，不需要继续处理
    } else {
      // 生成 mask_info 并 push_back（不管是新生成的还是复用的，都需要记录）
      ObGroupByPushdownContext::MaskAggrExprInfo mask_info;
      mask_info.aggr_expr_ = aggr_expr;
      mask_info.param_expr_ = param_expr;
      mask_info.new_param_expr_ = new_param_expr;
      if (OB_FAIL(ctx->mask_aggr_infos_.push_back(mask_info))) {
        LOG_WARN("failed to push back mask aggr info", K(ret));
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::try_rewrite_as_mask_aggr_expr(ObRawExpr *aggr_expr,
                                                                 ObGroupByPushdownContext *ctx,
                                                                 const common::ObIArray<ObRawExpr *> &from_exprs,
                                                                 const common::ObIArray<ObRawExpr *> &to_exprs,
                                                                 ObGroupByPushdownResult *result,
                                                                 bool &rewrite_happened)
{
  int ret = OB_SUCCESS;
  rewrite_happened = false;
  ObRawExpr *param_expr = NULL;
  ObRawExpr *new_param_expr = NULL;
  ObRawExpr *rewrited_param_expr = NULL;
  ObRawExpr *rewrited_expr = NULL;
  int64_t index = -1;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(ctx) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(aggr_expr), KP(ctx), KP(result));
  } else {
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < ctx->mask_aggr_infos_.count(); ++j) {
      ObGroupByPushdownContext::MaskAggrExprInfo &mask_info = ctx->mask_aggr_infos_.at(j);
      if (mask_info.aggr_expr_ == aggr_expr) {
        param_expr = mask_info.param_expr_;
        new_param_expr = mask_info.new_param_expr_;
        found = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!found) {
      LOG_WARN("aggr not found", K(ret), KPC(aggr_expr), K(ctx->mask_aggr_infos_));
      // not a mask aggr expr, which is unexpected
    } else if (OB_ISNULL(param_expr) || OB_ISNULL(new_param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param expr or new param expr", K(ret));
    } else if (!ObOptimizerUtil::find_item(from_exprs, new_param_expr, &index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not found item", K(ret));
    } else {
      rewrited_param_expr = to_exprs.at(index);
      ObRawExpr *case_when_expr = aggr_expr->get_param_expr(0);
      ObRawExprCopier copier(opt_ctx_.get_expr_factory());
      if (OB_FAIL(copier.add_replaced_expr(param_expr, rewrited_param_expr))) {
        LOG_WARN("failed to add replaced expr", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(case_when_expr, rewrited_expr))) {
        LOG_WARN("failed to copy on replace", K(ret));
      } else if (OB_FAIL(result->new_aggr_exprs_.push_back(rewrited_expr))) {
        LOG_WARN("failed to push back new expr", K(ret));
      } else {
        rewrite_happened = true;
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::distribute_groupby_exprs(const common::ObIArray<ObRawExpr *> &groupby_exprs,
                                                            ObLogicalOperator *left_child,
                                                            ObLogicalOperator *right_child,
                                                            ObGroupByPushdownContext &left_ctx,
                                                            ObGroupByPushdownContext &right_ctx,
                                                            bool &can_push)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret), KP(left_child), KP(right_child));
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < groupby_exprs.count(); ++i) {
    ObRawExpr *groupby_expr = groupby_exprs.at(i);
    if (OB_FAIL(distribute_groupby_expr(groupby_expr,
                                         left_child,
                                         right_child,
                                         left_ctx,
                                         right_ctx,
                                         can_push))) {
      LOG_WARN("failed to distribute groupby expr", K(ret));
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::get_join_keys(ObLogJoin *join,
                                                 ObSEArray<ObRawExpr *, 4> &left_join_keys,
                                                 ObSEArray<ObRawExpr *, 4> &right_join_keys,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObLogicalOperator *join_node = join;
  ObSEArray<ObRawExpr *, 4> left_mapping_keys;
  ObSEArray<ObRawExpr *, 4> right_mapping_keys;
  bool extract_join_key_failed = false;
  is_valid = true;

  if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(join));
  } else if (OB_ISNULL(plan = join->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(plan->extract_strict_equal_keys(join_node,
                                                     left_mapping_keys,
                                                     right_mapping_keys,
                                                     left_join_keys,
                                                     right_join_keys,
                                                     extract_join_key_failed))) {
    LOG_WARN("failed to extract strict equal keys", K(ret));
  } else if (extract_join_key_failed) {
    // 提取失败，清空结果
    left_join_keys.reset();
    right_join_keys.reset();
    is_valid = false;
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::rewrite_join_node(ObLogJoin *join,
                                                     bool can_push_to_left,
                                                     bool can_push_to_right,
                                                     ObGroupByPushdownContext *left_ctx,
                                                     ObGroupByPushdownResult *left_result,
                                                     ObGroupByPushdownContext *right_ctx,
                                                     ObGroupByPushdownResult *right_result,
                                                     ObGroupByPushdownContext *ctx,
                                                     ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  result->reset();
  if (OB_ISNULL(join) || OB_ISNULL(left_ctx) || OB_ISNULL(left_result) ||
      OB_ISNULL(right_ctx) || OB_ISNULL(right_result) || OB_ISNULL(ctx) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(join), KP(left_ctx), KP(left_result),
             KP(right_ctx), KP(right_result), KP(ctx), KP(result));
  } else {
    ObSEArray<ObRawExpr *, 4> new_left_exprs;
    ObSEArray<ObRawExpr *, 4> new_right_exprs;
    ObSEArray<ObRawExpr *, 4> from_exprs;
    ObSEArray<ObRawExpr *, 4> to_exprs;
    result->is_materialized_ = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rewrite_join_aggr_exprs(left_ctx->aggr_exprs_,
                                               *left_ctx,
                                               *left_result,
                                               *right_ctx,
                                               *right_result,
                                               new_left_exprs))) {
      LOG_WARN("failed to rewrite join aggr exprs", K(ret));
    } else if (OB_FAIL(rewrite_join_aggr_exprs(right_ctx->aggr_exprs_,
                                               *right_ctx,
                                               *right_result,
                                               *left_ctx,
                                               *left_result,
                                               new_right_exprs))) {
      LOG_WARN("failed to rewrite join aggr exprs", K(ret));
    } else if (ctx->need_count_
               && OB_FAIL(build_count_expr_for_join(*ctx,
                                                    *left_ctx,
                                                    *left_result,
                                                    *right_ctx,
                                                    *right_result,
                                                    result->count_expr_))) {
      LOG_WARN("failed to build count expr for join", K(ret));
    } else if (new_left_exprs.count() != left_ctx->aggr_exprs_.count() ||
               new_right_exprs.count() != right_ctx->aggr_exprs_.count() ||
               (ctx->need_count_ && OB_ISNULL(result->count_expr_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count of new expr", K(ret), K(new_left_exprs.count()), K(left_ctx->aggr_exprs_.count()),
               K(new_right_exprs.count()), K(right_ctx->aggr_exprs_.count()));
    } else if (OB_FAIL(append(from_exprs, left_ctx->aggr_exprs_))) {
      LOG_WARN("failed to append from_exprs", K(ret));
    } else if (OB_FAIL(append(from_exprs, right_ctx->aggr_exprs_))) {
      LOG_WARN("failed to append from_exprs", K(ret));
    } else if (OB_FAIL(append(to_exprs, new_left_exprs))) {
      LOG_WARN("failed to append to_exprs", K(ret));
    } else if (OB_FAIL(append(to_exprs, new_right_exprs))) {
      LOG_WARN("failed to append to_exprs", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->aggr_exprs_.count(); ++i) {
      ObRawExpr *aggr_expr = ctx->aggr_exprs_.at(i);
      int64_t index = -1;
      bool rewrite_happened = false;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pushdown expr", K(ret), KP(aggr_expr));
      } else if (ObOptimizerUtil::find_item(from_exprs, aggr_expr, &index)) {
        if (OB_FAIL(result->new_aggr_exprs_.push_back(to_exprs.at(index)))) {
          LOG_WARN("failed to push back new expr", K(ret));
        }
      } else if (OB_FAIL(try_rewrite_as_mask_aggr_expr(aggr_expr,
                                                       ctx,
                                                       from_exprs,
                                                       to_exprs,
                                                       result,
                                                       rewrite_happened))) {
        LOG_WARN("failed to try rewrite as mask aggr expr", K(ret));
      } else if (!rewrite_happened) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected not found mask aggr expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    OPT_TRACE("Rewrite join node:");
    OPT_TRACE("required exprs: ", ctx->aggr_exprs_);
    OPT_TRACE("new exprs: ", result->new_aggr_exprs_);
    OPT_TRACE("count expr: ", result->count_expr_);
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::rewrite_join_aggr_exprs(ObSEArray<ObRawExpr *, 4> &aggr_exprs,
                                                           ObGroupByPushdownContext &source_ctx,
                                                           ObGroupByPushdownResult &source_result,
                                                           ObGroupByPushdownContext &other_ctx,
                                                           ObGroupByPushdownResult &other_result,
                                                           ObIArray<ObRawExpr *> &new_aggr_exprs)
{
  int ret = OB_SUCCESS;
  new_aggr_exprs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); ++i) {
    ObRawExpr *aggr_expr = aggr_exprs.at(i);
    ObRawExpr *source_expr = NULL;
    ObRawExpr *count_expr = NULL;
    int64_t index = -1;
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null aggr expr", K(ret));
    } else if (source_result.is_materialized_) {
      if (!ObOptimizerUtil::find_item(source_ctx.aggr_exprs_, aggr_expr, &index) || index == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected not found item", K(ret), KP(aggr_expr));
      } else if (OB_ISNULL(source_expr = source_result.new_aggr_exprs_.at(index))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null source expr", K(ret));
      }
    } else {
      // count(*)
      if (aggr_expr->get_expr_type() == T_FUN_COUNT && aggr_expr->get_param_count() == 0) {
        ObConstRawExpr *const_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(opt_ctx_.get_expr_factory(), ObIntType, 1, const_expr))) {
          LOG_WARN("failed to build const expr", K(ret));
        } else if (OB_ISNULL(const_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null const expr", K(ret));
        } else {
          source_expr = const_expr;
        }
      } else if (OB_UNLIKELY(aggr_expr->get_param_count() != 1) || OB_ISNULL(aggr_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null source expr", K(ret));
      } else if (aggr_expr->get_expr_type() == T_FUN_COUNT) {
        // build case when expr for count, (param_expr)
        ObRawExpr *case_when_expr = NULL;
        if (OB_FAIL(build_case_when_expr_for_count(aggr_expr->get_param_expr(0), case_when_expr))) {
          LOG_WARN("failed to build case when expr for count", K(ret));
        } else {
          source_expr = case_when_expr;
        }
      } else {
        source_expr = aggr_expr->get_param_expr(0);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (other_result.is_materialized_) {
      if (aggr_need_count(aggr_expr) && OB_ISNULL(count_expr = other_result.count_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null count expr", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObRawExpr *new_expr = NULL;
      switch (aggr_expr->get_expr_type()) {
      case T_FUN_SUM:
      case T_FUN_COUNT: {
        if (other_result.is_materialized_) {
          // source_expr * count_expr
          ObOpRawExpr *mul_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::build_mul_expr(opt_ctx_.get_expr_factory(), source_expr, count_expr, mul_expr))) {
            LOG_WARN("failed to build mul expr", K(ret));
          } else if (OB_ISNULL(mul_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null new expr", K(ret));
          } else {
            new_expr = mul_expr;
          }
        } else {
          new_expr = source_expr;
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN: {
        new_expr = source_expr;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected aggr expr type", K(ret), K(aggr_expr->get_expr_type()));
        break;
      }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(new_expr->formalize(opt_ctx_.get_session_info()))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_aggr_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back new expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    OPT_TRACE("Success to rewrite join aggr exprs");
    OPT_TRACE("aggr_exprs: ", aggr_exprs);
    OPT_TRACE("new_aggr_exprs: ", new_aggr_exprs);
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::build_count_expr_for_join(ObGroupByPushdownContext &ctx,
                                                             ObGroupByPushdownContext &left_ctx,
                                                             ObGroupByPushdownResult &left_result,
                                                             ObGroupByPushdownContext &right_ctx,
                                                             ObGroupByPushdownResult &right_result,
                                                             ObRawExpr *&count_expr)
{
  int ret = OB_SUCCESS;
  bool left_transformed = left_result.is_materialized_;
  bool right_transformed = right_result.is_materialized_;
  if (!ctx.need_count_) {
    // do nothing
  } else if (OB_UNLIKELY((left_transformed && !left_ctx.need_count_)
                         || (right_transformed && !right_ctx.need_count_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not need count", K(ret));
  } else if ((left_transformed && OB_ISNULL(left_result.count_expr_))
             || (right_transformed && OB_ISNULL(right_result.count_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null count expr", K(ret));
  } else if (!left_transformed && !right_transformed) {
    // build const expr 1
    ObConstRawExpr *const_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(opt_ctx_.get_expr_factory(), ObIntType, 1, const_expr))) {
      LOG_WARN("failed to build const expr", K(ret));
    } else {
      count_expr = const_expr;
    }
  } else if (left_transformed && right_transformed) {
    // left_result.count_expr_ * right_result.count_expr_
    ObOpRawExpr *mul_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_mul_expr(opt_ctx_.get_expr_factory(),
                                               left_result.count_expr_,
                                               right_result.count_expr_,
                                               mul_expr))) {
      LOG_WARN("failed to build mul expr", K(ret));
    } else {
      count_expr = mul_expr;
    }
  } else if (left_transformed) {
    OPT_TRACE("inherit count expr from left: ", left_result.count_expr_);
    count_expr = left_result.count_expr_;
  } else {
    OPT_TRACE("inherit count expr from right: ", right_result.count_expr_);
    count_expr = right_result.count_expr_;
  }
  if (OB_FAIL(ret) || !ctx.need_count_) {
    // do nothing
  } else if (OB_ISNULL(count_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null count expr", K(ret));
  } else if (OB_FAIL(count_expr->formalize(opt_ctx_.get_session_info()))) {
    LOG_WARN("failed to formalize count expr", K(ret));
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::can_push_through_exchange(ObLogExchange *exchange,
                                                             ObGroupByPushdownContext *ctx,
                                                             bool &can_push)
{
  int ret = OB_SUCCESS;
  can_push = false;
  if (OB_ISNULL(exchange) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(exchange), KP(ctx));
  } else if (exchange->is_merge_sort() || exchange->get_sample_type() != NOT_INIT_SAMPLE_TYPE
             || exchange->is_task_order()) {
    // push down only when not merge && not px-sample && not task-order
    OPT_TRACE("exchange op is merge sort or px-sample or task-order, cannot push through");
    can_push = false;
  } else {
    // TODO tuliwei.tlw: check if exchange exprs match group by exprs
    can_push = true;
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::set_context_flags_for_exchange(ObLogExchange *exchange,
                                                                  ObGroupByPushdownContext *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exchange) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(exchange), KP(ctx));
  } else if (exchange->is_slave_mapping()) {
    ctx->enable_reshuffle_ = false;
    ctx->enable_place_groupby_ = false;
    OPT_TRACE("set context flags for exchange, is slave mapping, disable reshuffle and place groupby");
  } else {
    OPT_TRACE("set context flags for exchange: ", exchange->get_name(), exchange->get_op_id());
    switch (exchange->get_dist_method()) {
      case ObPQDistributeMethod::HASH:
      case ObPQDistributeMethod::PARTITION: {
        ctx->enable_reshuffle_ = true;
        ctx->enable_place_groupby_ = true;
        ctx->benefit_from_reshuffle_ = false;
        break;
      }
      case ObPQDistributeMethod::BC2HOST:
      case ObPQDistributeMethod::BROADCAST:
      case ObPQDistributeMethod::RANDOM: {
        ctx->enable_reshuffle_ = true;
        ctx->enable_place_groupby_ = true;
        ctx->benefit_from_reshuffle_ = true;
        break;
      }
      default: {
        ctx->enable_reshuffle_ = false;
        ctx->enable_place_groupby_ = false;
        ctx->benefit_from_reshuffle_ = false;
        break;
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::try_rewrite_set_children(ObLogSet *set,
                                                            ObGroupByPushdownContext *ctx,
                                                            ObFixedArray<ObGroupByPushdownContext, ObIAllocator> &child_contexts,
                                                            ObFixedArray<ObGroupByPushdownResult, ObIAllocator> &child_results,
                                                            bool &has_pushed_down,
                                                            const ObSelectStmt *&pushdown_child_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> select_exprs;
  ObSEArray<ObRawExpr*, 4> child_select_exprs;
  const ObSelectStmt *child_stmt = NULL;
  int64_t child_count = set->get_num_of_child();
  bool enable_reshuffle = false;

  if (OB_ISNULL(set) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(set), KP(ctx));
  } else if (OB_FAIL(set->get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else {
    // TODO tuliwei.tlw: check union all algorithm to determine if enable reshuffle
    enable_reshuffle = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_count; ++i) {
    ObLogicalOperator* child_op = set->get_child(i);
    ObGroupByPushdownContext &child_ctx = child_contexts.at(i);
    ObGroupByPushdownResult &child_result = child_results.at(i);
    ObGroupByPushdownResult *dummy_pointer = &child_result;
    if (OB_ISNULL(child_op = set->get_child(i)) ||
        OB_ISNULL(child_op->get_plan()) ||
        OB_ISNULL(child_stmt = static_cast<const ObSelectStmt *>(child_op->get_plan()->get_stmt()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(child_op), K(child_stmt));
    } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
    } else if (OB_FALSE_IT(child_select_exprs.reset())) {
    } else if (OB_FAIL(child_stmt->get_select_exprs(child_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(child_ctx.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));
    } else if (FALSE_IT(child_ctx.enable_reshuffle_ = enable_reshuffle)) {
    } else if (OB_FAIL(child_ctx.map(select_exprs, child_select_exprs, opt_ctx_.get_expr_factory(), opt_ctx_.get_session_info()))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (OB_FAIL(SMART_CALL(rewrite_child(child_op, &child_ctx, dummy_pointer)))) {
      LOG_WARN("failed to rewrite child", K(ret), K(child_op->get_name()));
    } else {
      if (child_result.is_materialized_) {
        // TODO tuliwei.tlw: remove original select items and add new select items may be better

        // The reason for using >= here is that, for some operators such as
        // subplan_scan, child_stmt's select_item_size may have already been
        // updated. In such cases, we do not need to create select items again.
        if (child_select_exprs.count() >= child_stmt->get_select_item_size()) {
          ObSEArray<ObRawExpr*, 4> new_select_list;
          if (OB_FAIL(new_select_list.assign(child_result.new_aggr_exprs_))) {
            LOG_WARN("failed to assign new aggr exprs", K(ret));
          } else if (child_ctx.need_count_ && OB_ISNULL(child_result.count_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null count expr", K(ret), KP(child_result.count_expr_));
          } else if (child_ctx.need_count_ && OB_FAIL(new_select_list.push_back(child_result.count_expr_))) {
            LOG_WARN("failed to append count expr to select list", K(ret));
          } else if (OB_FAIL(ObTransformUtils::create_select_item(opt_ctx_.get_allocator(),
                                                                  new_select_list,
                                                                  const_cast<ObSelectStmt*>(child_stmt)))) {
            LOG_WARN("failed to add select expr to child stmt", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!has_pushed_down) {
          has_pushed_down = true;
          pushdown_child_stmt = child_stmt;
          // only do it once
        }
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::add_projection_for_set_children(ObLogSet *set,
                                                                   ObFixedArray<ObGroupByPushdownContext, ObIAllocator> &child_contexts,
                                                                   ObFixedArray<ObGroupByPushdownResult, ObIAllocator> &child_results)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(set));
  } else {
    OPT_TRACE("add projection for set children: ", set->get_name(), set->get_op_id());
    ObSEArray<ObRawExpr*, 4> new_select_list;
    ObIAllocator &allocator = opt_ctx_.get_allocator();
    for (int64_t i = 0; OB_SUCC(ret) && i < set->get_num_of_child(); ++i) {
      if (!child_results.at(i).is_materialized_) {
        ObLogicalOperator* child_op = set->get_child(i);
        ObSelectStmt *child_stmt = NULL;
        if (OB_ISNULL(child_op) || OB_ISNULL(child_op->get_plan())
            || OB_ISNULL(child_stmt = const_cast<ObSelectStmt *>(static_cast<const ObSelectStmt *>(child_op->get_plan()->get_stmt())))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(child_op));
        } else if (OB_FAIL(do_place_groupby(child_op, &child_contexts.at(i), &child_results.at(i), child_op->get_card(), false))) {
          LOG_WARN("failed to add partial group by", K(ret), K(child_op->get_name()));
        } else if (OB_FAIL(new_select_list.assign(child_results.at(i).new_aggr_exprs_))) {
          LOG_WARN("failed to append afft expr to select", K(ret));
        } else if (child_contexts.at(i).need_count_ && OB_ISNULL(child_results.at(i).count_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null count expr", K(ret), KP(child_results.at(i).count_expr_));
        } else if (child_contexts.at(i).need_count_ && OB_FAIL(new_select_list.push_back(child_results.at(i).count_expr_))) {
          LOG_WARN("failed to append count expr to select list", K(ret));
        } else if (OB_FAIL(ObTransformUtils::create_select_item(allocator, new_select_list, child_stmt))) {
          LOG_WARN("failed to add select expr to child stmt", K(ret));
        } else {
          OPT_TRACE("add select item to child stmt successfully: ", child_op->get_name(), child_op->get_op_id());
          new_select_list.reuse();
        }
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::rewrite_set_node(ObLogSet *set,
                                                     ObGroupByPushdownContext *ctx,
                                                     const ObSelectStmt *pushdown_child_stmt,
                                                     ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = NULL;
  if (OB_ISNULL(set) || OB_ISNULL(ctx) || OB_ISNULL(pushdown_child_stmt) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(set), KP(ctx), KP(pushdown_child_stmt), KP(result));
  } else if (OB_ISNULL(stmt = static_cast<const ObSelectStmt *>(set->get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> new_aggr_list;
    SelectItem new_select_item;
    ObRawExprResType res_type;
    int64_t index_for_union = stmt->get_select_item_size();
    int64_t start_idx = ctx->need_count_ ? pushdown_child_stmt->get_select_item_size() - ctx->aggr_exprs_.count() - 1
                                         : pushdown_child_stmt->get_select_item_size() - ctx->aggr_exprs_.count();
    int64_t end_idx = pushdown_child_stmt->get_select_item_size();
    for (int64_t j = start_idx; OB_SUCC(ret) && j < end_idx; j++) {
      const SelectItem child_select_item = pushdown_child_stmt->get_select_item(j);
      // unused
      // ObString set_column_name = left_select_item.alias_name_;
      ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + set->get_set_op());
      res_type.reset();
      new_select_item.alias_name_ = child_select_item.alias_name_;
      new_select_item.expr_name_ = child_select_item.expr_name_;
      new_select_item.is_real_alias_ = child_select_item.is_real_alias_ || child_select_item.expr_->is_column_ref_expr();
      res_type = child_select_item.expr_->get_result_type();
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(opt_ctx_.get_expr_factory(),
                                                   index_for_union,
                                                   set_op_type,
                                                   res_type,
                                                   NULL,
                                                   new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FALSE_IT(index_for_union++)) {
      } else if (OB_FAIL(const_cast<ObSelectStmt *>(stmt)->add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      } else {
        // update result mapping
        if (ctx->need_count_ && j == end_idx - 1) {
          // count(*) expr
          result->count_expr_ = new_select_item.expr_;
        } else {
          if (OB_FAIL(new_aggr_list.push_back(new_select_item.expr_))) {
            LOG_WARN("failed to push back new aggr expr", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result->new_aggr_exprs_.assign(new_aggr_list))) {
      LOG_WARN("failed to assign new aggr exprs", K(ret));
    } else if (ctx->need_count_ && OB_FAIL(new_aggr_list.push_back(result->count_expr_))) {
      LOG_WARN("failed to push back count expr", K(ret));
    } else if (OB_FAIL(set->append_set_exprs(new_aggr_list))) {
      LOG_WARN("failed to append set exprs", K(ret));
    } else {
      result->is_materialized_ = true;
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::get_aggr_item_for_new_groupby(ObLogicalOperator *op,
                                                                 ObGroupByPushdownContext *ctx,
                                                                 ObGroupByPushdownResult *result,
                                                                 ObIArray<ObAggFunRawExpr *> &aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(ctx) || OB_ISNULL(result) || OB_ISNULL(opt_ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op), KP(ctx), KP(result), KP(opt_ctx_.get_session_info()));
  } else if (!result->is_materialized_) {
    ObAggFunRawExpr *count_expr = NULL;
    if (OB_FAIL(ctx->get_aggr_items(aggr_items))) {
      LOG_WARN("failed to get aggr items", K(ret));
    } else if (OB_FAIL(result->new_aggr_exprs_.assign(ctx->aggr_exprs_))) {
      LOG_WARN("failed to assign new aggr exprs", K(ret));
    }
    if (OB_FAIL(ret) || !ctx->need_count_) {
    } else if (OB_FAIL(ObRawExprUtils::build_dummy_count_expr(opt_ctx_.get_expr_factory(), opt_ctx_.get_session_info(), count_expr))) {
      LOG_WARN("failed to build dummy count expr", K(ret));
    } else if (OB_ISNULL(count_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null count expr", K(ret));
    } else if (OB_FAIL(count_expr->formalize(opt_ctx_.get_session_info()))) {
      LOG_WARN("failed to formalize count expr", K(ret));
    } else if (OB_FAIL(aggr_items.push_back(count_expr))) {
      LOG_WARN("failed to push back count expr", K(ret));
    } else {
      result->count_expr_ = count_expr;
    }
  } else {
    if (OB_UNLIKELY(result->new_aggr_exprs_.count() != ctx->aggr_exprs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count of new aggr exprs", K(ret), K(result->new_aggr_exprs_.count()), K(ctx->aggr_exprs_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->aggr_exprs_.count(); ++i) {
      ObAggFunRawExpr *new_aggr_expr = NULL;
      if (OB_ISNULL(ctx->aggr_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null aggr expr", K(ret));
      } else if (!ctx->aggr_exprs_.at(i)->is_aggr_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-aggr expr", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::generate_pullup_aggr_expr(
                             opt_ctx_.get_expr_factory(),
                             opt_ctx_.get_session_info(),
                             ctx->aggr_exprs_.at(i)->get_expr_type(),
                             static_cast<ObAggFunRawExpr *>(ctx->aggr_exprs_.at(i)),
                             result->new_aggr_exprs_.at(i),
                             new_aggr_expr))) {
        LOG_WARN("failed to generate pullup aggr expr", K(ret));
      } else if (OB_ISNULL(new_aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null new aggr expr", K(ret));
      } else if (OB_FAIL(new_aggr_expr->formalize(opt_ctx_.get_session_info()))) {
        LOG_WARN("failed to formalize new aggr expr", K(ret));
      } else if (OB_FAIL(aggr_items.push_back(new_aggr_expr))) {
        LOG_WARN("failed to push back new aggr expr", K(ret));
      } else {
        result->new_aggr_exprs_.at(i) = new_aggr_expr;
      }
    } // for end
    // build count(*) expr
    if (OB_SUCC(ret) && ctx->need_count_) {
      ObAggFunRawExpr *count_expr = NULL;
      if (OB_ISNULL(result->count_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null count expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(opt_ctx_.get_expr_factory(),
                                                                opt_ctx_.get_session_info(),
                                                                T_FUN_COUNT_SUM,
                                                                result->count_expr_,
                                                                count_expr))) {
        LOG_WARN("failed to build common aggr expr", K(ret));
      } else if (OB_ISNULL(count_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null count expr", K(ret));
      } else if (OB_FAIL(count_expr->formalize(opt_ctx_.get_session_info()))) {
        LOG_WARN("failed to formalize count expr", K(ret));
      } else if (OB_FAIL(aggr_items.push_back(count_expr))) {
        LOG_WARN("failed to push back count expr", K(ret));
      } else {
        result->count_expr_ = count_expr;
      }
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::do_place_groupby(ObLogicalOperator *op,
                                                    ObGroupByPushdownContext *ctx,
                                                    ObGroupByPushdownResult *result,
                                                    double ndv,
                                                    bool need_reshuffle)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *parent = op->get_parent();
  ObLogicalOperator *top = op;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(op) || OB_ISNULL(ctx) || OB_ISNULL(result) || OB_ISNULL(parent) || OB_ISNULL(plan = op->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op), KP(ctx), KP(result), KP(parent), KP(plan));
  } else {
    OPT_TRACE("do place groupby");
    OPT_TRACE_BEGIN_SECTION;
    OPT_TRACE("op name: ", op->get_name());
    OPT_TRACE("ctx->groupby_exprs_: ", ctx->groupby_exprs_);
    OPT_TRACE("ctx->aggr_exprs_: ", ctx->aggr_exprs_);
    OPT_TRACE("result->new_aggr_exprs_: ", result->new_aggr_exprs_);
    OPT_TRACE("card: ", op->get_card());
    OPT_TRACE("ndv: ", ndv);
    OPT_TRACE("need_reshuffle: ", need_reshuffle);
    OPT_TRACE_END_SECTION;
  }
  if (OB_FAIL(ret)) {
  } else if (need_reshuffle && !ctx->groupby_exprs_.empty() /* do not allocate exchange for scalar */) {
    // TODO tuliwei.tlw: mark as special shuffle, only local shuffle is needed
    ObExchangeInfo exch_info;
    if (OB_FAIL(plan->get_grouping_style_exchange_info(ctx->groupby_exprs_, top->get_output_equal_sets(), exch_info))) {
      LOG_WARN("failed to get grouping style exchange info", K(ret));
    } else if (OB_FAIL(plan->allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (top->get_op_id() == OB_INVALID_ID
               && FALSE_IT(top->set_op_id(opt_ctx_.get_next_op_id()))) {
    } else {
      OPT_TRACE("exchange placed");
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObAggFunRawExpr*, 4> aggr_items;
    if (OB_FAIL(get_aggr_item_for_new_groupby(op, ctx, result, aggr_items))) {
      LOG_WARN("failed to build new aggr expr", K(ret));
    } else if (OB_FAIL(plan->add_partial_groupby_as_top(top, ctx->groupby_exprs_, aggr_items, ndv, op->get_card()))) {
      LOG_WARN("failed to add partial groupby as top", K(ret));
    } else if (OB_FAIL(plan->insert_new_node_into_plan(parent, op, top))) {
      LOG_WARN("failed to insert new node into plan", K(ret));
    } else {
      OPT_TRACE("groupby placed", "aggr_items: ", aggr_items);
      result->is_materialized_ = true;
      transform_happened_ = true;
    }
  }
  return ret;
}

int ObGroupByPushDownPlanRewriter::try_place_groupby(ObLogicalOperator *op,
                                                     ObGroupByPushdownContext *ctx,
                                                     ObGroupByPushdownResult *result)
{
  int ret = OB_SUCCESS;
  bool should_place = false;
  bool is_unique = false;
  bool is_sharding_match = false;
  OPT_TRACE("try place groupby:");
  OPT_TRACE_BEGIN_SECTION;
  if (OB_ISNULL(op) || OB_ISNULL(ctx) || OB_ISNULL(result) || OB_ISNULL(op->get_plan())
      || OB_ISNULL(op->get_parent()) || ctx->is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op), KP(ctx), KP(result), KP(op->get_plan()), KP(op->get_parent()));
  } else {
    OPT_TRACE("current operator: ", op->get_name(), op->get_op_id());
  }
  if (OB_FAIL(ret)) {
  } else if (!ctx->enable_place_groupby_) {
    OPT_TRACE("enable_place_groupby_ is false, do nothing");
  } else if (op->get_parent()->get_type() == log_op_def::LOG_GROUP_BY) {
    OPT_TRACE("parent is groupby, do nothing");
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(ctx->groupby_exprs_,
                                                      op->get_table_set(),
                                                      op->get_fd_item_set(),
                                                      op->get_output_equal_sets(),
                                                      op->get_output_const_exprs(),
                                                      is_unique))) {
    LOG_WARN("failed to check unique property", K(ret));
  } else if (is_unique) {
    OPT_TRACE("groupby exprs is unique, skip");
  } else if (OB_FAIL(op->check_sharding_compatible_with_reduce_expr(ctx->groupby_exprs_, is_sharding_match))) {
    LOG_WARN("failed to check is sharding match", K(ret));
  } else if (NULL != op->get_strong_sharding()
             && FALSE_IT(is_sharding_match &= op->get_strong_sharding()->get_part_cnt() >= op->get_parallel())) {
  } else {
    double ndv = 0;
    double rowcnt = -1;
    double cut_ratio = 0;
    bool need_reshuffle = false;
    uint64_t nopushdown_cut_ratio = 3;
    ObSEArray<ObRawExpr*, 4> simplified_exprs;
    // Because of the nature of pushing down GROUP BY through JOINs,
    // hash/partition exchange operators encountered in the pushdown
    // process generally provide the required sharding for GROUP BY.
    // At the same time, the sharding of exchange operators is unusual
    // and not easy to judge directly. In this case, we can simply
    // assume the sharding requirement is satisfied.
    bool is_hash_or_pkey_exchange_op = (op->get_type() == log_op_def::LOG_EXCHANGE
                                        && (static_cast<ObLogExchange *>(op)->get_dist_method()
                                            == ObPQDistributeMethod::HASH
                                            || static_cast<ObLogExchange *>(op)->get_dist_method()
                                               == ObPQDistributeMethod::PARTITION));
    // per_dop_ndv = ObOptSelectivity::scale_distinct(per_dop_card, child_card, child_ndv);
    if (OB_FAIL(ObOptimizerUtil::simplify_exprs(op->get_fd_item_set(),
                                                op->get_output_equal_sets(),
                                                op->get_output_const_exprs(),
                                                ctx->groupby_exprs_,
                                                simplified_exprs))) {
      LOG_WARN("failed to simplify pushdown context", K(ret));
    } else if (FALSE_IT(op->get_plan()->get_selectivity_ctx().init_op_ctx(op))) {
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(op->get_plan()->get_update_table_metas(),
                                                            op->get_plan()->get_selectivity_ctx(),
                                                            simplified_exprs,
                                                            op->get_card(),
                                                            ndv))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else if (OB_FAIL(opt_ctx_.get_session_info()->get_sys_variable(
                  share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO, nopushdown_cut_ratio))) {
      LOG_WARN("failed to get session variable", K(ret));
    } else if (ctx->groupby_exprs_.empty()) {
      // TODO tuliwei.tlw: 这个情况应该直接下推
      // do not allocate exchange for scalar-like group by
      rowcnt = op->get_parallel();
      need_reshuffle = false;
    } else if (op->is_distributed() && !is_sharding_match && !is_hash_or_pkey_exchange_op) {
      OPT_TRACE("ndv: ", ndv,
                "; op->get_card(): ", op->get_card(),
                "; op->get_parallel(): ", op->get_parallel(),
                "; ctx->enable_reshuffle_: ", ctx->enable_reshuffle_);
      double partial_rowcnt = ObOptSelectivity::scale_distinct(op->get_card() / op->get_parallel(), op->get_card(), ndv) * op->get_parallel();
      double partial_cut_ratio = op->get_card() / std::max(partial_rowcnt, 1.0);
      if (partial_cut_ratio < nopushdown_cut_ratio) {
        double final_cut_ratio = op->get_card() / std::max(ndv, 1.0);
        bool enable_reshuffle = ctx->enable_reshuffle_ && ctx->benefit_from_reshuffle_;
        if (enable_reshuffle && (final_cut_ratio > nopushdown_cut_ratio)) {
          // allocate exchange only when cut_ratio is not enough, and become enough after shuffle
          rowcnt = ndv;
          need_reshuffle = true;
        } else {
          rowcnt = partial_rowcnt;
          need_reshuffle = false;
        }
      } else {
        rowcnt = partial_rowcnt;
        need_reshuffle = false;
      }
    } else {
      rowcnt = ndv;
      need_reshuffle = false;
      OPT_TRACE("no need reshuffle: ",
                "is_sharding_match", is_sharding_match,
                "; is_hash_or_pkey_exchange_op", is_hash_or_pkey_exchange_op);
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(cut_ratio = op->get_card() / std::max(rowcnt, 1.0))) {
    } else if (cut_ratio < nopushdown_cut_ratio) {
      OPT_TRACE("cut_ratio < nopushdown_cut_ratio, do not place groupby: ",
                "cut_ratio", cut_ratio,
                "; nopushdown_cut_ratio", nopushdown_cut_ratio);
      // do nothing
    } else if (OB_FAIL(do_place_groupby(op, ctx, result, ndv, need_reshuffle))) {
      LOG_WARN("failed to place groupby", K(ret));
    }
  }
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObGroupByPushDownPlanRewriter::build_case_when_expr_for_count(ObRawExpr *param_expr, ObRawExpr *&case_when_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *is_not_null_expr = NULL;
  ObConstRawExpr *const_expr_0 = NULL;
  ObConstRawExpr *const_expr_1 = NULL;
  case_when_expr = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(opt_ctx_.get_expr_factory(),
                                                             param_expr,
                                                             true,
                                                             is_not_null_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(opt_ctx_.get_expr_factory(),
                                                          ObIntType,
                                                          0,
                                                          const_expr_0))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(opt_ctx_.get_expr_factory(),
                                                          ObIntType,
                                                          1,
                                                          const_expr_1))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_ISNULL(is_not_null_expr) || OB_ISNULL(const_expr_0) || OB_ISNULL(const_expr_1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(opt_ctx_.get_expr_factory(),
                                                          is_not_null_expr,
                                                          const_expr_1,
                                                          const_expr_0,
                                                          case_when_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else if (OB_ISNULL(case_when_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null case when expr", K(ret));
  } else if (OB_FAIL(case_when_expr->formalize(opt_ctx_.get_session_info()))) {
    LOG_WARN("failed to formalize case when expr", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

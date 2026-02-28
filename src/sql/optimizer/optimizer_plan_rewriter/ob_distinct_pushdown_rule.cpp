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
#include "sql/optimizer/optimizer_plan_rewriter/ob_distinct_pushdown_rule.h"
#include "sql/optimizer/optimizer_plan_rewriter/ob_set_parent_visitor.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_group_by.h"
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

bool ObDistinctPushdownRule::is_enabled(const ObOptimizerContext &ctx)
{
    return ctx.enable_partial_distinct_pushdown() && ctx.get_query_ctx() != NULL &&
           ctx.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_6_0);
}

int ObDistinctPushdownRule::apply_rule(ObLogPlan *root_plan,
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
    ObDistinctPushDownPlanRewriter rewriter(ctx);
    ObDistinctPushdownResult rewriter_result;
    ObDistinctPushdownResult* result_ptr = &rewriter_result;
    if (OB_FAIL(set_parent_visitor.visit(root_op, &void_context, void_result))) {
      LOG_WARN("failed to set parent", K(ret));
    } else if (OB_FAIL((rewriter.visit(root_op, NULL, result_ptr)))) {
      LOG_WARN("failed to traverse plan tree", K(ret));
    } else {
      OPT_TRACE("rewriter result", root_op);
      result.set_transformed(rewriter.transform_happened());
    }
  }

  return ret;
}

int ObDistinctPushdownContext::assign(const ObDistinctPushdownContext &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(distinct_exprs_.assign(other.distinct_exprs_))) {
    LOG_WARN("failed to assign distinct exprs", K(ret));
  } else {
    enable_reshuffle_ = other.enable_reshuffle_;
    enable_place_distinct_ = other.enable_place_distinct_;
    benefit_from_reshuffle_ = other.benefit_from_reshuffle_;
  }
  return ret;
}

int ObDistinctPushdownContext::map_and_check(const common::ObIArray<ObRawExpr *> &from_exprs,
  const common::ObIArray<ObRawExpr *> &to_exprs,
  ObRawExprFactory &expr_factory,
  const ObSQLSessionInfo *session_info,
  bool check_valid,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObRawExprCopier copier(expr_factory);
  ObSEArray<ObRawExpr*, 4> replaced_exprs;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument(s)", K(ret), KP(session_info));
  } else if (OB_FAIL(copier.add_replaced_expr(from_exprs, to_exprs))) {
    LOG_WARN("failed to add replace pair", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < distinct_exprs_.count(); ++i) {
    ObRawExpr* replaced_expr;
    if (OB_FAIL(copier.copy_on_replace(distinct_exprs_.at(i), replaced_expr))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (OB_FAIL(replaced_exprs.push_back(replaced_expr))) {
      LOG_WARN("failed to pushback expr", K(ret));
    } else if (check_valid && distinct_exprs_.at(i) == replaced_expr) {
      if (replaced_expr->is_const_expr()) {
        // ok
      } else {
        is_valid = false;
      }
    }
  }

  if (OB_FAIL(ret) || !is_valid) {
  } else {
    distinct_exprs_.reuse();
    if (OB_FAIL(distinct_exprs_.assign(replaced_exprs))) {
      LOG_WARN("failed to assign distinct exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < distinct_exprs_.count(); ++i) {
      if (OB_FAIL(distinct_exprs_.at(i)->formalize(session_info))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(distinct_exprs_.at(i)->pull_relation_id())) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDistinctPushdownContext::map(const common::ObIArray<ObRawExpr *> &from_exprs,
                                const common::ObIArray<ObRawExpr *> &to_exprs,
                                ObRawExprFactory &expr_factory,
                                const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  if (OB_FAIL(this->map_and_check(from_exprs, to_exprs, expr_factory, session_info, false, is_valid))) {
    LOG_WARN("failed to map distinct context", K(ret));
  }
  return ret;
}

/*
 * merge current context with other distinct exprs
 * current context [t1.c1, t1.c2]
 * when meet a distinct on [t1.c1]
 * context becomes [t1.c1] and merged = true
 * ObOptimizerUtil::subset_exprs
 *
 * should_update = false means lower distinct is better
 * so we ignore upper and pushdown lower
 * if distinct context(a) and distinct (b)
 * should update is false, ignore context and pushdown b
 * for example, distinct context(a), distinct (b) any (a)
 *   should_update is false, ignore context a and pushdown b.
 *   should put distinct above this node
 *
 *
 * for example, distinct context(a), distinct(a, b)
 *  should_update is true, and pushdown (a), update lower distinct to distinct(a)
 *  through shuffle is whether upper is through shuffle,
 *  result is child result
 *  ignore_pushed is false.
 *
 * for example, distinct context(a, b), distinct(a)
 * lower is better
 * should_update is false, and pushdown (a).
 * pushthrough shuffle is reset whether this node is partial
 * tell parent context is not pushed and do not leave context here
 *
 * should_update . whether update current node
 * ignore_context. whether check and add distinct
 * pushed_down. when false. tell parent not materilaized
 */
int ObDistinctPushdownContext::merge_with(const common::ObIArray<ObRawExpr*> &lower_distinct_exprs,
                                        bool &pushdown_upper_and_update,
                                        bool &pushdown_lower_and_ignore,
                                        bool &pushdown_lower_and_check)
{
  int ret = OB_SUCCESS;

  pushdown_upper_and_update = false;
  pushdown_lower_and_ignore = false;
  pushdown_lower_and_check = false;
  // lower is better
  // do not merge, and use lower distinct
  if (ObOptimizerUtil::subset_exprs(lower_distinct_exprs, distinct_exprs_) &&
      lower_distinct_exprs.count() < distinct_exprs_.count()) {
    pushdown_lower_and_ignore = true;
    distinct_exprs_.reset();
    // we should ignore current context and leave a distinct there
    if (OB_FAIL(distinct_exprs_.assign(lower_distinct_exprs))) {
      LOG_WARN("failed to assign context", K(ret));
    }
  } else if (ObOptimizerUtil::subset_exprs(distinct_exprs_, lower_distinct_exprs)) {
    // pushdown context/upper distinct is better
    // we should update current node
    // and treat context as already pushed down
    pushdown_upper_and_update = true;
  } else {
    // can not compare
    // do not update current
    // and put a distinct above
    pushdown_lower_and_check = true;
    distinct_exprs_.reset();
    if (OB_FAIL(distinct_exprs_.assign(lower_distinct_exprs))) {
      LOG_WARN("failed to assign context", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_distinct(ObLogDistinct *distinct,
                                                 ObDistinctPushdownContext *ctx,
                                                 ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  ObLogicalOperator* child = distinct->get_child(0);
  if (OB_ISNULL(distinct) || OB_ISNULL(child) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(distinct), KP(child), KP(result));
  } else if (HASH_AGGREGATE != distinct->get_algo()) {
    // do not handle distinct other than hash
    can_transform = false;
  } else if (!distinct->is_distributed()) {
    // do nothing for non-distributed distinct for now, to avoid affecting TP scenarios
    can_transform = false;
  } else if (NULL == ctx || ctx->is_empty() || distinct->get_filter_exprs().count() > 0) {
    // If the upper context is empty (or blocked by filters), push down the current distinct.
    ObDistinctPushdownContext pushdown_context;
    ObDistinctPushdownResult child_result;
    ObDistinctPushdownResult *child_result_ptr = &child_result;
    if (OB_FAIL(generate_context(distinct, pushdown_context))) {
      LOG_WARN("failed to extract partial distinct info", K(ret), K(distinct->get_name()));
    } else if (OB_FAIL(rewrite_child(child, &pushdown_context, child_result_ptr))) {
      LOG_WARN("failed to rewrite child", K(ret), K(distinct->get_name()));
    } else if (child_result.is_materialized()) {
      OPT_TRACE("distinct is pushed down", K(distinct->get_op_id()));
    }
  } else {
    ObDistinctPushdownContext pushdown_context;
    bool pushdown_upper_and_update = false;
    bool pushdown_lower_and_ignore = false;
    bool pushdown_lower_and_check = false;
    ObDistinctPushdownResult child_result;
    ObDistinctPushdownResult *child_result_ptr = &child_result;
    ObSEArray<ObRawExpr*, 4> reduced_exprs;
    if (OB_FAIL(pushdown_context.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));

      // do a merge of context
      // if upper distinct is a super set of lower distinct
      // use lower distinct and inherit the mark
      // otherwise, use the upper context and ignore the pushed down context
    } else if (OB_FAIL(pushdown_context.merge_with(distinct->get_distinct_exprs(),
                                                   pushdown_upper_and_update,
                                                   pushdown_lower_and_ignore,
                                                   pushdown_lower_and_check))) {
      LOG_WARN("failed to merge pushdown context", K(ret));
    } else {
      OPT_TRACE("we push to distinct", distinct->get_distinct_exprs(),
                                        pushdown_upper_and_update,
                                        pushdown_lower_and_ignore,
                                        pushdown_lower_and_check);
      if (pushdown_upper_and_update) {
        // it could be the case that distinct exprs are empty
        // but we cannot delete this final distinct for outline issue
        ObSEArray<ObRawExpr*, 4> simplified_exprs;
        if (OB_FAIL(simplify_distinct_exprs(distinct, pushdown_context.distinct_exprs_, simplified_exprs))) {
          LOG_WARN("failed to simplify distinct exprs", K(ret));
        } else if (OB_FAIL(distinct->set_distinct_exprs(simplified_exprs))) {
          LOG_WARN("failed to set distinct exprs", K(ret));
        } else {
          // mark it as pushed down, as it has been used to simplify current distinct
          OPT_TRACE("distinct is simplified", K(distinct->get_op_id()));
          result->set_is_materialized(true);
          transform_happened_ = true;
        }
      } else {
        pushdown_context.enable_place_distinct_ = false;
        pushdown_context.benefit_from_reshuffle_ = false;
        // (inherit enable_reshuffle from parent ctx)
      }
    }
    if OB_FAIL(ret) {
      // do nothing
    } else if (OB_FAIL(rewrite_child(child, &pushdown_context, child_result_ptr))) {
      LOG_WARN("failed to rewrite child", K(ret), K(distinct->get_name()));

    // If distinct is pushed down to the lower node, further check whether it is necessary to
    // add distinct at the current distinct node based on the upper context.
    } else if (pushdown_lower_and_ignore) {
      // ignore pushdown context, do nothing
    } else if (pushdown_lower_and_check && OB_FAIL(try_place_distinct(distinct, ctx, result))) {
      LOG_WARN("failed to check and add partial distinct");
    }
  }

  if (OB_FAIL(ret) || can_transform) {
    // do nothing
  } else if (OB_FAIL(default_rewrite_children(distinct, result))) {
    LOG_WARN("failed to default visit node for child", K(ret));
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_groupby(ObLogGroupBy *groupby,
                                                 ObDistinctPushdownContext *ctx,
                                                 ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  // stop the pushdown if context is not null??
  // check whether the group by is all duplicate-insensitive
  // if so, extract distinct symbold := group by + distinct arguments
  // do the pushdown
  // if not pushed down, do nothing
  // how to know whether we have any() or not..
  // check_stmt_is_all_distinct_col
  ObLogicalOperator* child = NULL;
  bool is_duplicate_insensitive = false;
  // only handle hash group by
  // only init pushdown from a single group by
  if (OB_ISNULL(groupby) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(groupby), KP(result));
  } else if (OB_UNLIKELY(groupby->get_num_of_child() != 1 || OB_ISNULL(child = groupby->get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected groupby", K(ret), KP(groupby), KP(result), KP(child));
  } else if (!groupby->is_distributed()) {
    // do nothing for non-distributed group by for now, to avoid affecting TP scenarios
    can_transform = false;
  } else if (HASH_AGGREGATE != groupby->get_algo() || !(groupby->get_step() == AggregatePathType::SINGLE)) {
    // do not handle distinct other than hash
    can_transform = false;
  } else if (groupby->is_three_stage_aggr()) {
    // do not handle three stage group by
    can_transform = false;
  } else if (OB_FAIL(groupby->is_duplicate_insensitive_aggregation(is_duplicate_insensitive))) {
    LOG_WARN("failed to check duplicate insensitive aggregation", K(ret));
  } else if (!is_duplicate_insensitive) {
    // do nothing
    // this will be handled by partial groupby pushdown
    can_transform = false;
  } else {
    OPT_TRACE("try place distinct from group by", groupby->get_name(), groupby->get_op_id(), groupby->get_aggr_funcs());
    bool can_pushdown = false;
    ObDistinctPushdownResult child_result;
    ObDistinctPushdownResult* child_result_ptr = &child_result;
    ObDistinctPushdownContext pushdown_context;
    // exprs in having or order by should be added to distinct exprs
    // get_partial_distinct_context may returns NULL
    // if it cannot extract xx from any(xx)
    // but sometimes for example, we know group by op is root
    // then we can extract
    if (OB_FAIL(generate_context_from_groupby(groupby, pushdown_context, can_pushdown))) {
      LOG_WARN("failed extract distinct context from group by", K(ret));
    } else if (!can_pushdown) {
      can_transform = false;
    } else if (OB_FAIL(rewrite_child(child, &pushdown_context, child_result_ptr))) {
      LOG_WARN("failed to rewrite child", K(ret), K(groupby->get_name()));
    } else if (NULL != ctx) {
      if (OB_FAIL(try_place_distinct(groupby, ctx, result))) {
        LOG_WARN("failed to try place distinct", K(ret), K(groupby->get_name()));
      }
      // todo update result by child result
    }
  }

  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(groupby, ctx, result))) {
      LOG_WARN("failed to do default visit node for child", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_join(ObLogJoin *join,
                                               ObDistinctPushdownContext *ctx,
                                               ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(join) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument(s)", K(ret), KP(join), KP(result));
  } else if (OB_UNLIKELY(join->get_num_of_child() != 2 || OB_ISNULL(join->get_child(0)) || OB_ISNULL(join->get_child(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join", K(ret), KP(join), K(join->get_num_of_child()), KP(join->get_child(0)), KP(join->get_child(1)));
  } else if (!join->is_distributed()) {
    // do nothing for non-distributed join for now, to avoid affecting TP scenarios
    can_transform = false;
  } else {
    ObJoinType join_type = join->get_join_type();
    bool try_push_to_left = (ctx != NULL) &&
                            (INNER_JOIN == join_type ||
                              LEFT_OUTER_JOIN == join_type ||
                              LEFT_SEMI_JOIN == join_type ||
                              LEFT_ANTI_JOIN == join_type);
    bool try_push_to_right = (ctx != NULL) &&
                              (INNER_JOIN == join_type ||
                              RIGHT_OUTER_JOIN == join_type ||
                              RIGHT_SEMI_JOIN == join_type ||
                              RIGHT_ANTI_JOIN == join_type);
    bool equal_derive = RIGHT_SEMI_JOIN == join_type ||
                        LEFT_SEMI_JOIN == join_type ||
                        INNER_JOIN == join_type;
    bool left_implicit_distinct = RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type;
    bool right_implicit_distinct = LEFT_SEMI_JOIN == join_type || LEFT_ANTI_JOIN == join_type;
    bool can_push_to_right = false;
    bool can_push_to_left = false;
    bool do_pushed = false;
    bool is_partially_pushdown = false;
    // do not push though join with unequal join conditions
    // for nl joins, extract from other conditions
    // when can push to both sides
    // push to one that is not unique
    if (MERGE_JOIN == join->get_join_algo()) {
      // do nothing
      // default rewrite logic
      can_transform = false;
    } else if ((!(try_push_to_left || left_implicit_distinct) &&
                !(try_push_to_right || right_implicit_distinct))) {
      can_transform = false;
    } else if (join->get_filter_exprs().count() > 0) {
      can_transform = false;
    } else {
      ObSEArray<ObRawExpr*, 4> reduce_exprs;
      ObLogicalOperator* left_child = join->get_child(0);
      ObLogicalOperator* right_child = join->get_child(1);
      ObSEArray<ObRawExpr *, 4> left_mapping_keys;
      ObSEArray<ObRawExpr *, 4> right_mapping_keys;
      ObSEArray<ObRawExpr *, 4> left_join_keys;
      ObSEArray<ObRawExpr *, 4> right_join_keys;
      bool extract_join_key_failed = false;
      ObLogicalOperator* top_ = join;
      bool disable_reshuffle = join->get_dist_method() != DistAlgo::DIST_BC2HOST_NONE
                              && join->get_dist_method() != DistAlgo::DIST_BROADCAST_NONE
                              && join->get_dist_method() != DistAlgo::DIST_NONE_BROADCAST;
      if (OB_FAIL(join->get_plan()->extract_strict_equal_keys(top_,
                                                              left_mapping_keys,
                                                              right_mapping_keys,
                                                              left_join_keys,
                                                              right_join_keys,
                                                              extract_join_key_failed))) {
        LOG_WARN("failed to get equal keys from join condition", K(ret));
      } else if (extract_join_key_failed) {
        can_transform = false;
      } else {
        ObDistinctPushdownResult left_result;
        ObDistinctPushdownResult right_result;
        ObDistinctPushdownResult *left_result_ptr = &left_result;
        ObDistinctPushdownResult *right_result_ptr = &right_result;
        ObDistinctPushdownContext left_context;
        ObDistinctPushdownContext right_context;
        bool current_context_empty = false;
        // prepare left context
        if (left_implicit_distinct) {
          // update left_context by merging distinct
          if (OB_FAIL(left_context.distinct_exprs_.assign(left_join_keys))) {
            LOG_WARN("failed to assign distinct context", K(ret));
          } else {
            left_context.enable_place_distinct_ = false;
            left_context.enable_reshuffle_ = false;
            left_context.benefit_from_reshuffle_ = false;
          }
        } else if (try_push_to_left) {
          if (NULL == ctx || ctx->is_empty()) {
            // do nothing
          } else {
            current_context_empty = ctx->distinct_exprs_.empty();
            if (OB_FAIL(left_context.assign(*ctx))) {
              LOG_WARN("failed to assign pushdown context", K(ret));
            } else {
              left_context.enable_place_distinct_ = true;
              left_context.enable_reshuffle_ &= !disable_reshuffle;
              left_context.benefit_from_reshuffle_ = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (equal_derive
                    && OB_FAIL(left_context.map(right_mapping_keys,
                                                left_mapping_keys,
                                                opt_ctx_.get_expr_factory(),
                                                opt_ctx_.get_session_info()))) {
            LOG_WARN("failed to map pushdown context", K(ret));
          } else if (OB_FAIL(filter_distinct_exprs_by(&left_context,
                                                      left_child,
                                                      join,
                                                      left_join_keys,
                                                      can_push_to_left,
                                                      is_partially_pushdown))) {
            LOG_WARN("failed to filter distinct exprs", K(ret));
          // } else if (!can_push_to_left) {
          //   left_context = dummy_context;
          // } else if (!is_enforced_pushdown && (left_context->get_distinct_exprs().empty() && !current_context_empty)) {
          //   // this should be an optional behavior
          //   // should we pushdown if it does not involve in distinct?
          //   left_context = dummy_context;
          } else if (OB_FAIL(append(left_context.distinct_exprs_, left_join_keys))) {
            LOG_WARN("failed to append distinct exprs", K(ret));
          } else {
            do_pushed = true;
          }
        }
        // prepare context for right child
        if (OB_FAIL(ret)) {
        } else if (right_implicit_distinct) {
          // ignore distinct pushed down from current context
          if (OB_FAIL(right_context.distinct_exprs_.assign(right_join_keys))) {
            LOG_WARN("failed to assign distinct context", K(ret));
          } else {
            right_context.enable_place_distinct_ = false;
            right_context.enable_reshuffle_ = false;
            right_context.benefit_from_reshuffle_ = false;
          }
        } else if (try_push_to_right) {
          if (NULL == ctx || ctx->is_empty()) {
            // do nothing
          } else {
            // distinguish between dummy and empty!
            // filter with scope
            current_context_empty = ctx->distinct_exprs_.empty();
            if (OB_FAIL(right_context.assign(*ctx))) {
              LOG_WARN("failed to assign pushdown context", K(ret));
            } else {
              right_context.enable_place_distinct_ = true;
              right_context.enable_reshuffle_ &= !disable_reshuffle;
              right_context.benefit_from_reshuffle_ = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (equal_derive && OB_FAIL(right_context.map(left_mapping_keys,
                                                                right_mapping_keys,
                                                                opt_ctx_.get_expr_factory(),
                                                                opt_ctx_.get_session_info()))) {
            LOG_WARN("failed to map pushdown context", K(ret));
          } else if (OB_FAIL(filter_distinct_exprs_by(&right_context,
                                                      right_child,
                                                      join,
                                                      right_join_keys,
                                                      can_push_to_right,
                                                      is_partially_pushdown))) {
            LOG_WARN("failed to filter distinct exprs", K(ret));
          } else if (!can_push_to_right) {
          //   right_context = dummy_context;
          // } else if (!is_enforced_pushdown && (right_context->get_distinct_exprs().empty()
          //                                       && !current_context_empty)) {
          //   // this should be an optional behavior
          //   right_context = dummy_context;
          } else if (OB_FAIL(append(right_context.distinct_exprs_, right_join_keys))) {
            LOG_WARN("failed to append distinct exprs", K(ret));
          } else {
            do_pushed = true;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (!can_push_to_left && !left_implicit_distinct) {
          if (OB_FAIL(default_rewrite_child(left_child, left_result_ptr))) {
            LOG_WARN("failed to rewrite child", K(ret), K(join->get_name()));
          }
        } else {
          if (OB_FAIL(rewrite_child(left_child, &left_context, left_result_ptr))) {
            LOG_WARN("failed to rewrite child", K(ret), K(join->get_name()));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!can_push_to_right && !right_implicit_distinct) {
          if (OB_FAIL(default_rewrite_child(right_child, right_result_ptr))) {
            LOG_WARN("failed to rewrite child", K(ret), K(join->get_name()));
          }
        } else {
          if (OB_FAIL(rewrite_child(right_child, &right_context, right_result_ptr))) {
            LOG_WARN("failed to rewrite child", K(ret), K(join->get_name()));
          }
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (do_pushed
                  && ((left_result.is_materialized() && can_push_to_left)
                      || (right_result.is_materialized() && can_push_to_right))) {
          result->set_is_materialized(true);
        } else {
          // add distinct above join
          if (NULL == ctx || ctx->is_empty()) {
            // do nothing
            // otherwise, materialize distinct context here
          } else if (OB_FAIL(try_place_distinct(join, ctx, result))) {
            LOG_WARN("failed to add partial distinct for join", K(ret), K(join->get_name()));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) || can_transform) {
  } else if (OB_FAIL(visit_node(join, ctx, result))) {
    LOG_WARN("failed to default visit node", K(ret), K(join->get_name()));
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_exchange(ObLogExchange *exchange,
                                                  ObDistinctPushdownContext *ctx,
                                                  ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(exchange) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(exchange), KP(result));
  } else if (exchange->get_num_of_child() != 1 || OB_ISNULL(exchange->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exchange", K(ret), KP(exchange), K(exchange->get_num_of_child()), KP(exchange->get_child(0)));
  } else if (NULL == ctx || ctx->is_empty() || exchange->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (OB_FAIL(can_push_through_exchange(exchange, ctx, can_transform))) {
    LOG_WARN("failed to check if can push through exchange", K(ret));
  } else if (!can_transform) {
    // do nothing
  } else if (exchange->is_consumer()) {
    if (OB_FAIL(SMART_CALL(rewrite_child(exchange->get_child(0), ctx, result)))) {
      LOG_WARN("failed to rewrite child", K(ret));
    } else if (!result->is_materialized() && OB_FAIL(try_place_distinct(exchange, ctx, result))) {
      LOG_WARN("failed to try place distinct", K(ret));
    }
  } else {
    ObDistinctPushdownContext pushdown_ctx;
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
      if (OB_FAIL(try_place_distinct(exchange, ctx, result))) {
        LOG_WARN("failed to try place distinct", K(ret));
      }
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_material(ObLogMaterial *material,
                                                  ObDistinctPushdownContext *ctx,
                                                  ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  if (OB_ISNULL(material) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(material), KP(result));
  } else if (material->get_num_of_child() != 1 || OB_ISNULL(material->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected material", K(ret), KP(material), K(material->get_num_of_child()), KP(material->get_child(0)));
  } else if (NULL == ctx || ctx->is_empty() || material->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (!material->is_distributed()) {
    // do nothing for non-distributed material for now, to avoid affecting TP scenarios
    can_transform = false;
  } else if (OB_FAIL(SMART_CALL(rewrite_child(material->get_child(0), ctx, result)))) {
    LOG_WARN("failed to rewrite child", K(ret));
  } else if (!result->is_materialized() && OB_FAIL(try_place_distinct(material, ctx, result))) {
    LOG_WARN("failed to try place distinct", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(material, ctx, result))) {
      LOG_WARN("failed to do default visit node for material", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_table_scan(ObLogTableScan *table_scan,
                                                    ObDistinctPushdownContext *ctx,
                                                    ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  bool push_into_scan = false;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(table_scan) || OB_ISNULL(result) || OB_ISNULL(plan = table_scan->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(table_scan), KP(result), KP(plan));
  } else if (NULL == ctx || ctx->is_empty()) {
    can_transform = false;
  } else if (!table_scan->is_distributed()) {
    // do nothing for non-distributed table scan for now, to avoid affecting TP scenarios
    can_transform = false;
  } else {
    push_into_scan = false;
    ObSEArray<ObAggFunRawExpr*, 1> dummy_aggr;
    bool can_pushdown = table_scan->get_pushdown_aggr_exprs().empty() &&
                        table_scan->get_pushdown_groupby_columns().empty();
    if (!can_pushdown) {
      // do nothing
    } else if (OB_FAIL(plan->check_storage_distinct_pushdown(ctx->distinct_exprs_, push_into_scan))) {
      LOG_WARN("failed to check can storage distinct pushdown", K(ret));
    } else if (push_into_scan && OB_FAIL(plan->try_push_aggr_into_table_scan(table_scan, dummy_aggr, ctx->distinct_exprs_))) {
      LOG_WARN("failed to try push aggr into table scan", K(ret));
    } else if (!push_into_scan && table_scan->get_pushdown_groupby_columns().empty()) {
      // add partial
      // it could be the case that distinct already pushed down
      if (OB_FAIL(try_place_distinct(table_scan, ctx, result))) {
        LOG_WARN("failed to try place distinct", K(ret));
      }
    } else {
      // pushed into scan
      result->set_is_materialized(true);
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_subplan_scan(ObLogSubPlanScan *subplan_scan,
                                                       ObDistinctPushdownContext *ctx,
                                                       ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  // need to do a mapping
  // subplan scan will do column pruning
  // size of access may not be the same with that for child
  // refer to compute_const_exprs_for_subquery
  // ObOptimizerUtil::convert_subplan_scan_expr
  // outer -> inner may be many -> one mapping
  // subquery_id_ == table_id_
  // child of subplan scan is root
  ObLogicalOperator* child_op = NULL;
  const ObDMLStmt *child_stmt = NULL;
  const ObSelectStmt *child_select_stmt = NULL;
  if (OB_ISNULL(subplan_scan) || OB_ISNULL(child_op = subplan_scan->get_child(0)) ||
        OB_ISNULL(child_op->get_plan()) ||
        OB_ISNULL(subplan_scan->get_plan()) ||
        OB_ISNULL(child_stmt = child_op->get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(subplan_scan), KP(child_op));
  } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
  } else if (OB_ISNULL(child_select_stmt = static_cast<const ObSelectStmt *>(child_stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast select stmt", K(ret));
  } else if (NULL == ctx || ctx->is_empty() || subplan_scan->get_filter_exprs().count() > 0) {
    can_transform = false;
  } else if (child_select_stmt->is_unpivot_select()) {
    // because of unpivot bugs, unpivot select is not supported for now
    can_transform = false;
  } else if (!subplan_scan->is_distributed()) {
    // do nothing for non-distributed subplan scan for now, to avoid affecting TP scenarios
    can_transform = false;
  } else {
    ObSEArray<ObRawExpr*, 4> output_cols;
    ObSEArray<ObRawExpr*, 4> input_cols;
    ObDistinctPushdownContext pushdown_context;
    bool can_pushdown = false;
    if (OB_FAIL(pushdown_context.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));
    // use optimizerutil::convert_subplan_scan_expr
    } else if (OB_FAIL(ObOptimizerUtil::get_subplan_scan_output_to_input_mapping(*child_select_stmt,
                                                                                  pushdown_context.distinct_exprs_,
                                                                                  input_cols,
                                                                                  output_cols))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (OB_FAIL(pushdown_context.map_and_check(output_cols,
                                                      input_cols,
                                                      opt_ctx_.get_expr_factory(),
                                                      opt_ctx_.get_session_info(),
                                                      true,
                                                      can_pushdown))) {
      LOG_WARN("failed to map pushdown context", K(ret));
    } else if (!can_pushdown) {
      can_transform = false;
    }
    if (OB_FAIL(ret) || !can_transform) {
      // rewrite child, pass result directly
    } else if (OB_FAIL(rewrite_child(child_op, &pushdown_context, result))) {
      LOG_WARN("failed to rewrite child", K(ret), K(child_op->get_name()));
    } else if (!result->is_materialized()) {
      if (OB_FAIL(try_place_distinct(subplan_scan, ctx, result))) {
        LOG_WARN("failed to try place distinct", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !can_transform) {
    if (OB_FAIL(visit_node(subplan_scan, ctx, result))) {
      LOG_WARN("failed to do default visit node for child", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_set(ObLogSet *set,
                                              ObDistinctPushdownContext *ctx,
                                              ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool can_transform = true;
  // is not distinct, push through
  // need to map the pushdown context
  // do nothing if is merge
  if (set->is_recursive_union()) {
    can_transform = false;
  } else if (!set->is_distributed()) {
    // do nothing for non-distributed set for now, to avoid affecting TP scenarios
    can_transform = false;
  } else if (HASH_SET != set->get_algo() && set->is_set_distinct()) {
    can_transform = false;
  } else if ((NULL == ctx || ctx->is_empty() || set->get_filter_exprs().count() > 0) && !set->is_set_distinct()) {
    can_transform = false;
  } else if (ObSelectStmt::UNION == set->get_set_op() && !set->is_set_distinct()) {
    // handle union all
    // directly pushdown to all input
    // todo do a pull up if not pushdown far away
    ObSEArray<ObRawExpr *, 8> select_exprs;
    ObSEArray<ObRawExpr*, 4> child_select_exprs;
    const ObDMLStmt *child_stmt = NULL;
    bool can_pushdown = false;
    bool any_pushdown = false;
    bool pushdown_distinct_count = -1;
    if (OB_FAIL(set->get_set_exprs(select_exprs))) {
      LOG_WARN("failed to get set exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < set->get_num_of_child(); ++i) {
      ObLogicalOperator* child_op = set->get_child(i);
      ObDistinctPushdownResult child_result;
      ObDistinctPushdownResult *child_result_ptr = &child_result;
      ObDistinctPushdownContext pushdown_context;
      if (OB_ISNULL(child_op = set->get_child(i)) || OB_ISNULL(child_op->get_plan())
                 || OB_ISNULL(child_stmt = child_op->get_plan()->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(child_op), K(child_stmt));
      } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
      } else if (OB_FALSE_IT(child_select_exprs.reset())) {
        LOG_WARN("failed to reset array", K(ret));
      } else if (OB_FAIL(static_cast<const ObSelectStmt *>(child_stmt)->get_select_exprs(child_select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(pushdown_context.assign(*ctx))) {
        LOG_WARN("failed to assign pushdown context", K(ret));
      } else if (OB_FAIL(pushdown_context.enable_reshuffle_ = false)) {
      } else if (OB_FAIL(pushdown_context.map(select_exprs,
                                              child_select_exprs,
                                              opt_ctx_.get_expr_factory(),
                                              opt_ctx_.get_session_info()))) {
        LOG_WARN("failed to map pushdown context", K(ret));
      } else if (OB_FAIL(all_distinct_exprs_depdend_on(pushdown_context.distinct_exprs_,
                                                       child_op,
                                                       can_pushdown))) {
        LOG_WARN("failed to check distinct expr dependency", K(ret));
      } else if (!can_pushdown) {
        if (OB_FAIL(default_rewrite_child(child_op, child_result_ptr))) {
          LOG_WARN("failed to default rewrite child", K(ret));
        }
      } else if (OB_FAIL(rewrite_child(child_op, &pushdown_context, child_result_ptr))) {
        LOG_WARN("failed to rewrite child", K(ret), K(child_op->get_name()));
      } else {
        any_pushdown = any_pushdown || child_result.is_materialized();
        result->set_is_materialized(result->is_materialized() || child_result.is_materialized());
      }
    }
    // todo what if result == false, which means it is not materialized any where below union all
    // we should add context here
    // if all children are partial distinct
    // do a pull up
    // todo should be handled by later merge partial distinct rule
    if (!any_pushdown) {
      // Calculating the NDV (number of distinct values) for union expressions is extremely troublesome,
      // so disabling place distinct above union all for now

      // OPT_TRACE("no child pushed down, try place distinct above union");
      // if (OB_FAIL(try_place_distinct(set, ctx, result))) {
      //   LOG_WARN("failed to try place distinct", K(ret));
      // }
    }
  } else if (set->is_set_distinct()) {
    bool any_pushdown = false;
    // if current context is not null
    // do a merge before rewrite children
    ObSEArray<ObRawExpr *, 8> select_exprs;
    ObSEArray<ObRawExpr*, 4> reduced_exprs;
    ObDistinctPushdownContext simplified_pushdown_context;
    bool pushdown_upper_and_update = false;
    bool pushdown_lower_and_ignore = false;
    bool pushdown_lower_and_check = false;
    if (OB_FAIL(set->get_set_exprs(select_exprs))) {
      LOG_WARN("failed to get set exprs", K(ret));
    } else if (NULL == ctx || ctx->is_empty() || set->get_filter_exprs().count() > 0
               || ObSelectStmt::UNION != set->get_set_op()) {
      // get_current_context
      if (OB_FAIL(simplified_pushdown_context.distinct_exprs_.assign(select_exprs))) {
        LOG_WARN("failed to assign distinct context", K(ret));
      }
    } else if (OB_FAIL(simplified_pushdown_context.assign(*ctx))) {
      LOG_WARN("failed to assign pushdown context", K(ret));
    } else if (OB_FAIL(simplified_pushdown_context.merge_with(select_exprs,
                                                              pushdown_upper_and_update,
                                                              pushdown_lower_and_ignore,
                                                              pushdown_lower_and_check))) {
      LOG_WARN("failed to assign expr array", K(ret));
    } else if (pushdown_lower_and_ignore || pushdown_lower_and_check) {
      simplified_pushdown_context.enable_place_distinct_ = false;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < set->get_num_of_child(); i++) {
      ObLogicalOperator *child_op = NULL;
      const ObDMLStmt *child_stmt = NULL;
      ObSEArray<ObRawExpr*, 4> child_select_exprs;
      ObDistinctPushdownContext pushdown_context;
      ObDistinctPushdownResult child_result;
      ObDistinctPushdownResult *child_result_ptr = &child_result;
      bool can_pushdown = false;
      if (OB_ISNULL(child_op = set->get_child(i)) ||
          OB_ISNULL(child_op->get_plan()) ||
          OB_ISNULL(child_stmt = child_op->get_plan()->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(child_op), K(child_stmt));
      } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is not select stmt", K(ret), KPC(child_stmt));
      } else if (OB_FALSE_IT(child_select_exprs.reset())) {
      } else if (OB_FAIL(static_cast<const ObSelectStmt *>(child_stmt)->get_select_exprs(child_select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      }
      // todo check about the nondeterministic case
      if (OB_FAIL(pushdown_context.assign(simplified_pushdown_context))) {
        LOG_WARN("failed to assign pushdown context", K(ret));
      } else if (FALSE_IT(pushdown_context.enable_reshuffle_ = false)) {
      } else if (OB_FAIL(pushdown_context.map(select_exprs,
                                              child_select_exprs,
                                              opt_ctx_.get_expr_factory(),
                                              opt_ctx_.get_session_info()))) {
        LOG_WARN("failed to map pushdown context", K(ret));
      } else if (OB_FAIL(all_distinct_exprs_depdend_on(pushdown_context.distinct_exprs_,
                                                       child_op,
                                                       can_pushdown))) {
        LOG_WARN("failed to check distinct expr dependency", K(ret));
      } else if (!can_pushdown) {
        // do nothing
      } else {
        any_pushdown = any_pushdown || can_pushdown;
      }
      if (OB_FAIL(ret)) {
      } else if (!can_pushdown) {
        if (OB_FAIL(default_rewrite_child(child_op, child_result_ptr))) {
          LOG_WARN("failed to default rewrite child", K(ret));
        }
      } else {
        // rewrite children
        if (OB_FAIL(rewrite_child(child_op, &pushdown_context, child_result_ptr))) {
          LOG_WARN("failed to rewrite child", K(ret), K(child_op->get_name()));
        } else {
          result->set_is_materialized(result->is_materialized() || child_result.is_materialized());
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (NULL == ctx || ctx->is_empty() || set->get_filter_exprs().count() > 0) {
      result->set_is_materialized(false);
    } else if (pushdown_lower_and_ignore) {
      result->set_is_materialized(false);
    } else if (pushdown_lower_and_check && OB_FAIL(try_place_distinct(set, ctx, result))) {
      LOG_WARN("failed to try place distinct", K(ret));
    }
  }

  if (OB_FAIL(ret) || can_transform) {
    // do nothing
  } else if (OB_FAIL(visit_node(set, ctx, result))) {
    LOG_WARN("failed to default visit node", K(ret), K(set->get_name()));
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::visit_node(ObLogicalOperator *op,
                                              ObDistinctPushdownContext *context,
                                              ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(op), KP(result));
  } else if (OB_FAIL(default_rewrite_children(op, result))) {
    LOG_WARN("failed to trigger rewrite children", K(ret));
  } else if (NULL != context && !context->is_empty()) {
    if (OB_FAIL(try_place_distinct(op, context, result))) {
      LOG_WARN("failed to try place distinct", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::default_rewrite_children(ObLogicalOperator *op, ObDistinctPushdownResult *&result)
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
    } else if (OB_FAIL(rewrite_child(child, NULL, result))) {
      LOG_WARN("failed to rewrite child", K(ret), KP(child));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::default_rewrite_child(ObLogicalOperator *child_op, ObDistinctPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op) || OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret), KP(child_op), KP(result));
  } else if (OB_FAIL(rewrite_child(child_op, NULL, result))) {
    LOG_WARN("failed to rewrite child", K(ret), KP(child_op));
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::generate_context(ObLogDistinct *distinct, ObDistinctPushdownContext &ctx)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(distinct)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(distinct));

    // if distinct_node is single && distinct_pushdown is disabled by hint
    // do not derive pushdown context
  } else if (OB_FAIL(ctx.distinct_exprs_.assign(distinct->get_distinct_exprs()))) {
    LOG_WARN("failed to assign distinct exprs", K(ret));
  } else {
    ctx.enable_place_distinct_ = false;
    ctx.enable_reshuffle_ = false;
    ctx.benefit_from_reshuffle_ = false;
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::generate_context_from_groupby(ObLogGroupBy *groupby,
                                                                  ObDistinctPushdownContext &ctx,
                                                                  bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObSEArray<ObRawExpr *, 4> distinct_exprs;
  ObLogPlan *plan = NULL;
  const ObDMLStmt *stmt = NULL;
  can_pushdown = false;
  if (OB_ISNULL(plan = groupby->get_plan()) ||
      OB_ISNULL(stmt = groupby->get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_UNLIKELY(!stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is not select stmt", K(ret), KPC(stmt));
  } else if (OB_FAIL(append(distinct_exprs, groupby->get_group_by_exprs()))) {
    LOG_WARN("failed append group by exprs", K(ret));
  } else {
    // extract all argument
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby->get_aggr_funcs().count(); ++i) {
      ObAggFunRawExpr *aggr_expr = static_cast<ObAggFunRawExpr *>(groupby->get_aggr_funcs().at(i));
      if (OB_ISNULL(aggr_expr) || OB_UNLIKELY(!aggr_expr->is_aggr_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid aggr expr", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(distinct_exprs,
                                                              aggr_expr->get_real_param_exprs()))) {
        LOG_WARN("failed to assign aggr param expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(plan->check_stmt_is_all_distinct_col(static_cast<const ObSelectStmt *>(stmt),
                                                            distinct_exprs,
                                                            can_pushdown))) {
    }
  }
  if (can_pushdown) {
    ctx.enable_place_distinct_ = false;
    ctx.enable_reshuffle_ = true;
    if (OB_FAIL(ctx.distinct_exprs_.assign(distinct_exprs))) {
      LOG_WARN("failed to assign distinct exprs", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::can_push_through_exchange(ObLogExchange *exchange,
                                                              ObDistinctPushdownContext *ctx,
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
    can_push = false;
  } else {
    // TODO tuliwei.tlw: check if exchange exprs match distinct exprs
    can_push = true;
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::set_context_flags_for_exchange(ObLogExchange *exchange,
                                                                   ObDistinctPushdownContext *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exchange) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(exchange), KP(ctx));
  } else {
    if (exchange->is_slave_mapping()) {
      OPT_TRACE("set context flags for exchange", exchange->get_name(), "is slave mapping, disable reshuffle and place distinct");
      ctx->enable_reshuffle_ = false;
      ctx->enable_place_distinct_ = false;
    } else {
      switch (exchange->get_dist_method()) {
        case ObPQDistributeMethod::HASH:
        case ObPQDistributeMethod::PARTITION: {
          ctx->enable_reshuffle_ = true;
          ctx->enable_place_distinct_ = true;
          // Set benefit_from_reshuffle to false here, because in most cases, a Hash or Partition exchange
          // already provides the required sharding for distinct.
          // If no further expensive operators need to be pushed down, a group by can be placed above this
          // exchange operator to avoid unnecessary shuffles.
          ctx->benefit_from_reshuffle_ = false;
          break;
        }
        case ObPQDistributeMethod::BC2HOST:
        case ObPQDistributeMethod::BROADCAST:
        case ObPQDistributeMethod::RANDOM: {
          ctx->enable_reshuffle_ = true;
          ctx->enable_place_distinct_ = true;
          ctx->benefit_from_reshuffle_ = true;
          break;
        }
        default: {
          // For distribution methods not explicitly listed, adopt a conservative strategy:
          // disable reshuffle and place distinct
          ctx->enable_reshuffle_ = false;
          ctx->enable_place_distinct_ = false;
          ctx->benefit_from_reshuffle_ = false;
          break;
        }
      }
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::do_place_distinct(ObLogicalOperator *op,
                                                    //  ObDistinctPushdownContext *ctx,
                                                     ObSEArray<ObRawExpr *, 4> &distinct_exprs,
                                                     ObDistinctPushdownResult *result,
                                                     double ndv,
                                                     bool need_reshuffle)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *parent = op->get_parent();
  ObLogicalOperator *top = op;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(op) || OB_ISNULL(result) || OB_ISNULL(parent) || OB_ISNULL(plan = op->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op), KP(result), KP(parent), KP(plan));
  } else {
    OPT_TRACE("do place distinct",
              "op name: ", op->get_name(),
              "distinct_exprs_: ", distinct_exprs,
              "card: ", op->get_card(),
              "ndv: ", ndv,
              "need_reshuffle: ", need_reshuffle);
  }
  if (OB_FAIL(ret)) {
  } else if (need_reshuffle && !distinct_exprs.empty() /* do not allocate exchange for scalar */) {
    ObExchangeInfo exch_info;
    if (OB_FAIL(plan->get_grouping_style_exchange_info(distinct_exprs, top->get_output_equal_sets(), exch_info))) {
      LOG_WARN("failed to get grouping style exchange info", K(ret));
    } else if (OB_FAIL(plan->allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (top->get_op_id() == OB_INVALID_ID
               && FALSE_IT(top->set_op_id(opt_ctx_.get_next_op_id()))) {
    } else {
      OPT_TRACE("exchange placed");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(plan->add_partial_distinct_as_top(top, distinct_exprs, ndv))) {
    LOG_WARN("failed to allocate distinct as top", K(ret));
  } else if (OB_FAIL(plan->insert_new_node_into_plan(parent, op, top))) {
    LOG_WARN("failed to insert new node into plan", K(ret));
  } else {
    OPT_TRACE("distinct placed");
    result->set_is_materialized(true);
    transform_happened_ = true;
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::try_place_distinct(ObLogicalOperator *op,
                                                      ObDistinctPushdownContext *ctx,
                                                      ObDistinctPushdownResult *result)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool contains_lob_type = false;
  bool is_sharding_match = false;
  ObSEArray<ObRawExpr*, 4> simplified_exprs;
  if (OB_NOT_NULL(op)) {
    OPT_TRACE("try place distinct", op->get_name());
  }
  if (OB_ISNULL(op) || OB_ISNULL(ctx) || OB_ISNULL(result) || OB_ISNULL(op->get_plan())
      || OB_ISNULL(op->get_parent()) || ctx->is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op), KP(ctx), KP(result), KP(op->get_plan()), KP(op->get_parent()));
  } else if (!op->is_distributed()) {
    // do nothing for non-distributed operator for now, to avoid affecting TP scenarios
  } else if (op->get_parent()->get_type() == log_op_def::LOG_DISTINCT) {
    // do nothing
    OPT_TRACE("try place distinct", op->get_name(), "parent is distinct, do nothing");
  } else if (!ctx->enable_place_distinct_) {
    // do nothing
    OPT_TRACE("try place distinct", op->get_name(), "enable_place_distinct_ is false, do nothing");
  } else if (ctx->distinct_exprs_.empty()) {
    // TODO tuliwei.tlw: change to add limit 1
    // do nothing
    OPT_TRACE("try place distinct", op->get_name(), "distinct_exprs_ is empty, do nothing");
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(ctx->distinct_exprs_,
                                                      op->get_table_set(),
                                                      op->get_fd_item_set(),
                                                      op->get_output_equal_sets(),
                                                      op->get_output_const_exprs(),
                                                      is_unique))) {
    LOG_WARN("failed to check unique property", K(ret));
  } else if (is_unique) {
    OPT_TRACE("try place distinct", op->get_name(), "is unique, do nothing");
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::contains_lob_type(ctx->distinct_exprs_, contains_lob_type))) {
    LOG_WARN("failed to check contains lob type", K(ret));
  } else if (contains_lob_type) {
    OPT_TRACE("try place distinct", op->get_name(), "contains lob type, do nothing");
    // do nothing
  } else if (OB_FAIL(op->check_sharding_compatible_with_reduce_expr(ctx->distinct_exprs_, is_sharding_match))) {
    LOG_WARN("failed to check is sharding match", K(ret));
  } else if (NULL != op->get_strong_sharding()
             && FALSE_IT(is_sharding_match &= (op->get_strong_sharding()->get_part_cnt() >= op->get_parallel()))) {
  } else if (OB_FAIL(simplify_distinct_exprs(op, ctx->distinct_exprs_, simplified_exprs))) {
    LOG_WARN("failed to simplify distinct exprs", K(ret));
  } else if (simplified_exprs.empty()) {
    // add limit 1
    OPT_TRACE("distinct exprs is empty after simplify, add limit 1");
    ObLogicalOperator *parent = op->get_parent();
    ObLogicalOperator *top = op;
    if (OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(parent));
    } else if (OB_FAIL(op->get_plan()->add_partial_limit_1_as_top(top))) {
      LOG_WARN("failed to add partial limit for partial distinct", K(op), K(ret));
    } else if (OB_FAIL(op->get_plan()->insert_new_node_into_plan(parent, op, top))) {
      LOG_WARN("failed to insert new node into plan", K(ret));
    } else {
      result->set_is_materialized(true);
      transform_happened_ = true;
    }
  } else {
    double ndv = 0;
    double cut_ratio = 0;
    bool need_reshuffle = false;
    uint64_t nopushdown_cut_ratio = 3;
    bool is_hash_or_pkey_exchange_op = (op->get_type() == log_op_def::LOG_EXCHANGE
                                        && (static_cast<ObLogExchange *>(op)->get_dist_method()
                                            == ObPQDistributeMethod::HASH
                                            || static_cast<ObLogExchange *>(op)->get_dist_method()
                                               == ObPQDistributeMethod::PARTITION));
    op->get_plan()->get_selectivity_ctx().init_op_ctx(op);
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(op->get_plan()->get_update_table_metas(),
                                                     op->get_plan()->get_selectivity_ctx(),
                                                     simplified_exprs,
                                                     op->get_card(),
                                                     ndv))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else if (OB_FAIL(opt_ctx_.get_session_info()->get_sys_variable(share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
                                                                     nopushdown_cut_ratio))) {
      LOG_WARN("failed to get session variable", K(ret));
    } else if (op->is_distributed() && !is_sharding_match && !is_hash_or_pkey_exchange_op) {
      OPT_TRACE("try place distinct: ",
                "ndv", ndv,
                "op->get_card()", op->get_card(),
                "op->get_parallel()", op->get_parallel(),
                "ctx->enable_reshuffle_", ctx->enable_reshuffle_,
                "simplified_exprs", simplified_exprs);
      double partial_ndv = ObOptSelectivity::scale_distinct(op->get_card() / op->get_parallel(), op->get_card(), ndv) * op->get_parallel();
      double partial_cut_ratio = op->get_card() / std::max(partial_ndv, 1.0);
      if (partial_cut_ratio < nopushdown_cut_ratio) {
        double final_cut_ratio = op->get_card() / std::max(ndv, 1.0);
        bool enable_reshuffle = ctx->enable_reshuffle_ && ctx->benefit_from_reshuffle_;
        if (enable_reshuffle && (final_cut_ratio > nopushdown_cut_ratio)) {
          // allocate exchange only when cut_ratio is not enough, and become enough after shuffle
          need_reshuffle = true;
        } else {
          ndv = partial_ndv;
          need_reshuffle = false;
        }
      } else {
        ndv = partial_ndv;
        need_reshuffle = false;
      }
    } else {
      OPT_TRACE("try place distinct, no reshuffle: ",
                "is_sharding_match", is_sharding_match,
                "is_hash_or_pkey_exchange_op", is_hash_or_pkey_exchange_op);
      need_reshuffle = false;
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(cut_ratio = op->get_card() / std::max(ndv, 1.0))) {
    } else if (cut_ratio < nopushdown_cut_ratio) {
      OPT_TRACE("try place distinct, cut_ratio < nopushdown_cut_ratio: ",
                "cut_ratio", cut_ratio,
                "nopushdown_cut_ratio", nopushdown_cut_ratio);
      // do nothing
    } else if (OB_FAIL(do_place_distinct(op, simplified_exprs, result, ndv, need_reshuffle))) {
      LOG_WARN("failed to place distinct", K(ret));
    }
  }
  return ret;
}

int ObDistinctPushDownPlanRewriter::simplify_distinct_exprs(ObLogicalOperator *op,
                                                            ObIArray<ObRawExpr *> &distinct_exprs,
                                                            ObSEArray<ObRawExpr *, 4> &simplified_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> reduce_exprs;
  ObSEArray<ObRawExpr*, 4> reduce_exprs_remove_const;
  bool is_const = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(op));
  } else if (OB_FAIL(ObOptimizerUtil::simplify_exprs(op->get_fd_item_set(),
                                                     op->get_output_equal_sets(),
                                                     op->get_output_const_exprs(),
                                                     distinct_exprs,
                                                     reduce_exprs))) {
    LOG_WARN("failed to simplify pushdown context", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < reduce_exprs.count(); ++i) {
    if (OB_FAIL(ObOptimizerUtil::is_const_expr(reduce_exprs.at(i),
                                               op->get_output_equal_sets(),
                                               op->get_output_const_exprs(),
                                               is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (!is_const) {
      reduce_exprs_remove_const.push_back(reduce_exprs.at(i));
      is_const = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (reduce_exprs_remove_const.count() > 0) {
    // deduplicate
    if (OB_FAIL(append_array_no_dup(simplified_exprs, reduce_exprs_remove_const))) {
      LOG_WARN("faild to append array", K(op), K(ret));
    }
  }

  return ret;
}

int ObDistinctPushDownPlanRewriter::filter_distinct_exprs_by(ObDistinctPushdownContext *context,
                                                             ObLogicalOperator *op,
                                                             ObLogicalOperator *join_op,
                                                             const common::ObIArray<ObRawExpr *> &join_exprs,
                                                             bool &can_push,
                                                             bool &is_partially_pushdown)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> reduced_exprs;
  ObSEArray<ObRawExpr*, 4> exprs_from_both_sides;
  ObSEArray<ObRawExpr*, 4> join_with_groupby_keys;
  ObRelIds intersect_rel_ids;
  can_push = true;
  is_partially_pushdown = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < context->distinct_exprs_.count(); ++i) {
    if (context->distinct_exprs_.at(i)->get_relation_ids().is_subset(op->get_table_set())) {
      reduced_exprs.push_back(context->distinct_exprs_.at(i));
    } else {
      // if not subset but has intersect, can not push
      is_partially_pushdown = true;
      if (OB_FAIL(intersect_rel_ids.add_members(context->distinct_exprs_.at(i)->get_relation_ids()))){
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(intersect_rel_ids.intersect_members(op->get_table_set()))) {
        LOG_WARN("failed to do intersect", K(ret));
      } else if (intersect_rel_ids.is_empty()) {
      } else if OB_FAIL(exprs_from_both_sides.push_back(context->distinct_exprs_.at(i))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    }
    intersect_rel_ids.reuse();
  }
  // todo reducted + join keys
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(join_with_groupby_keys, join_exprs))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(append(join_with_groupby_keys, reduced_exprs))) {
      LOG_WARN("failed to push back exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < exprs_from_both_sides.count(); ++i) {
    if (OB_FAIL(ObOptimizerUtil::is_expr_is_determined(join_with_groupby_keys,
                                                       join_op->get_fd_item_set(),
                                                       join_op->get_output_equal_sets(),
                                                       join_op->get_output_const_exprs(),
                                                       exprs_from_both_sides.at(i),
                                                       can_push))) {
      LOG_WARN("failed to derive is determined for expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (can_push) {
    context->distinct_exprs_.reset();
    if (OB_FAIL(context->distinct_exprs_.assign(reduced_exprs))) {
      LOG_WARN("failed to assign distinct exprs", K(ret));
    }
  } else {
    context->distinct_exprs_.reset();
  }
  return ret;
}

/*
 * this will not work for union as relation id is merged from all children
 * this only works for join to check join side
 * if we only want to push to one side
 */
int ObDistinctPushDownPlanRewriter::all_distinct_exprs_depdend_on(const ObIArray<ObRawExpr *> &exprs,
                                                                  ObLogicalOperator *&op,
                                                                  bool &is_true)
{
  int ret = OB_SUCCESS;
  is_true = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_true && i < exprs.count(); ++i) {
    if (exprs.at(i)->get_relation_ids().is_subset(op->get_table_set())) {
      // do nothing
    } else {
      is_true = false;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

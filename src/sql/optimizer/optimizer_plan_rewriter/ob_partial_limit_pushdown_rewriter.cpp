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
#include "sql/optimizer/optimizer_plan_rewriter/ob_partial_limit_pushdown_rewriter.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
using namespace log_op_def;

LimitPushdownRewriter::LimitPushdownRewriter(common::ObIAllocator &allocator,
                                             sql::ObRawExprFactory &expr_factory,
                                             ObSQLSessionInfo *session_info,
                                             ObOptimizerContext* ctx)
  : allocator_(allocator),
    expr_factory_(expr_factory),
    session_info_(session_info),
    ctx_(ctx)
{
}

LimitPushdownRewriter::~LimitPushdownRewriter()
{
}

int LimitPushdownRewriter::is_simple_limit(ObLogLimit *limit, bool &is_simple)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    is_simple = limit->get_percent_expr() == NULL
              && !limit->get_is_calc_found_rows()
              && !limit->is_fetch_with_ties()
              && limit->get_ties_ordering().empty();
  }
  return ret;
}

/*
*
*/
int LimitPushdownRewriter::extract_pushdown_context_from_limit(ObLogLimit *&limit,
                                                               LimitPushdownContext *&pushdown_context, // NULLABLE
                                                               LimitPushdownContext *&child_context)
{
  int ret = OB_SUCCESS;
  ObRawExpr * cur_limit_expr = NULL;
  int64_t cur_limit_value = -1;
  bool is_null_value = true;
  ObRawExpr *new_limit_count_expr = NULL;
  int64_t min_limit_value = -1;
  // limit expr might be null
  if (OB_ISNULL(limit) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(limit->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit expr is null", K(ret));
  } else if (OB_FALSE_IT(cur_limit_expr = limit->get_limit_expr())) {
  } else if (cur_limit_expr != NULL && OB_FAIL(ObTransformUtils::get_expr_int_value(cur_limit_expr,
                                                          ctx_->get_params(),
                                                          ctx_->get_exec_ctx(),
                                                          &limit->get_plan()->get_allocator(),
                                                          cur_limit_value,
                                                          is_null_value))) {
    LOG_WARN("failed to get expr int value", K(ret), KPC(cur_limit_expr));
  } else if (!is_null_value && cur_limit_value >= 0) {
    // merge if they are different
    if (pushdown_context != NULL &&
        pushdown_context->limit_count_ >= 0 &&
        pushdown_context->limit_expr_ != cur_limit_expr) {
      // create a min of both expr
      ObRawExpr *ge_expr = NULL;
      ObRawExpr *case_when = NULL;
      if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(expr_factory_,
                                                              T_OP_GE,
                                                              pushdown_context->limit_expr_,
                                                              cur_limit_expr, ge_expr))) {
        LOG_WARN("failed to build null safe equal expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(expr_factory_,
                                                              ge_expr,
                                                              cur_limit_expr,
                                                              pushdown_context->limit_expr_,
                                                              case_when))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else {
        // check whether cast
        min_limit_value = MIN(cur_limit_value, pushdown_context->limit_count_);
        new_limit_count_expr = case_when;
      }
      if (OB_FAIL(ret) || OB_ISNULL(new_limit_count_expr)) {
        /*do nothing*/
      } else if (OB_FAIL(new_limit_count_expr->formalize(session_info_))) {
        LOG_WARN("failed formalize expr", K(ret));
      } else {
        ObConstRawExpr * const_expr = NULL;
        if (new_limit_count_expr->is_const_expr() &&
                !new_limit_count_expr->has_flag(CNT_STATIC_PARAM) &&
                !new_limit_count_expr->has_flag(CNT_DYNAMIC_PARAM)) {
          if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_,
                                                           ObIntType,
                                                           min_limit_value,
                                                           const_expr))) {
            LOG_WARN("failed to build int expr", K(ret));
          } else if (OB_ISNULL(const_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("limit expr is null", K(ret));
          } else if (OB_FAIL(const_expr->formalize(session_info_))) {
            LOG_WARN("failed formalize expr", K(ret));
          } else {
            new_limit_count_expr = const_expr;
          }
        }
        if (OB_SUCC(ret)) {
          limit->set_limit_expr(new_limit_count_expr);
        }
      }
    } else if (cur_limit_expr != NULL) {
      // pushdown contxt is null
      min_limit_value = cur_limit_value;
      new_limit_count_expr = cur_limit_expr;
    }
    // handle offset
    if (OB_FAIL(ret)) {
    } else if (NULL != limit->get_offset_expr() && new_limit_count_expr != NULL) {
        ObRawExpr *pushed_expr = NULL;
        if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(
                                              expr_factory_,
                                              *session_info_,
                                              new_limit_count_expr,
                                              limit->get_offset_expr(),
                                              pushed_expr))) {
          LOG_WARN("failed to make push down limit count", K(ret));
        } else if (OB_FAIL(ObTransformUtils::get_expr_int_value(pushed_expr,
                                                          ctx_->get_params(),
                                                          ctx_->get_exec_ctx(),
                                                          &limit->get_plan()->get_allocator(),
                                                          min_limit_value,
                                                          is_null_value))) {
          LOG_WARN("failed to get expr int value", K(ret), KPC(cur_limit_expr));
        } else if (!is_null_value && min_limit_value >= 0 && pushed_expr != NULL) {
          new_limit_count_expr = pushed_expr;
        } else {
          new_limit_count_expr = NULL;
          min_limit_value = -1;
        }
    }
  } else if (OB_SUCC(ret) &&
             cur_limit_expr == NULL &&
             pushdown_context != NULL &&
             pushdown_context->limit_count_ >= 0) {
    cur_limit_expr = pushdown_context->limit_expr_;
    cur_limit_value = pushdown_context->limit_count_;
    limit->set_limit_expr(cur_limit_expr);
    if (NULL != limit->get_offset_expr()) {
      ObRawExpr *pushed_expr = NULL;
      if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(expr_factory_,
                                              *session_info_,
                                              cur_limit_expr,
                                              limit->get_offset_expr(),
                                              pushed_expr))) {
        LOG_WARN("failed to make push down limit count", K(ret));
      } else if (OB_FAIL(ObTransformUtils::get_expr_int_value(pushed_expr,
                                                          ctx_->get_params(),
                                                          ctx_->get_exec_ctx(),
                                                          &limit->get_plan()->get_allocator(),
                                                          cur_limit_value,
                                                          is_null_value))) {
        LOG_WARN("failed to get expr int value", K(ret), KPC(cur_limit_expr));
      } else if (!is_null_value && cur_limit_value >= 0 && pushed_expr != NULL) {
        min_limit_value = cur_limit_value;
        new_limit_count_expr = pushed_expr;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (limit->is_final() || new_limit_count_expr == NULL) {
  } else if (OB_FAIL(LimitPushdownContext::init(allocator_, new_limit_count_expr, min_limit_value, limit->is_partial(), child_context))) {
    LOG_WARN("failed to init pushdown context", K(ret));
  }
  return ret;
}

/*
 * visit function usually include four steps
 * 1. handle pushdown context passed by parent
 *   - check whether there is filter on limit, if so, can not push further
 * 2. producer what can be passed to children
 *  - merge parent context with current limit value, and pruduce what can be pushded down for child
 * 3. call visit_child with the children context
 * 4. handle children result, and produce result for parent
 *  - if child pushed down, delete this limit node (is partial)
 *  - if child not pushed down, merge partial limit
 *  - return whether parent contexted is pushed down and updated plan
 */
int LimitPushdownRewriter::visit_limit(ObLogLimit *limit, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool pushed_down = false;
  LimitPushdownResult *child_result = NULL;
  LimitPushdownContext *child_context = NULL;
  bool is_simple = false;
  if (OB_ISNULL(limit) || OB_ISNULL(limit->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_simple_limit(limit, is_simple))) {
    LOG_WARN("failed to check limit simple", K(ret));
  } else if (is_simple) {
  // split context into pushdown and not pushdown
  LimitPushdownContext *pushdown_context = NULL;
  LimitPushdownContext *not_pushdown_context = NULL;
  ObLogicalOperator* child = limit->get_child(0);
  bool is_partial = limit->is_partial();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(extract_pushdown_context(limit, context, pushdown_context, not_pushdown_context))) {
    LOG_WARN("failed to extract pushdown context", K(ret));
  } else if (OB_FAIL(extract_pushdown_context_from_limit(limit, pushdown_context, child_context))) {
    LOG_WARN("failed to extract pushdown context from limit", K(ret));
  } else {
    // merge pushdown context with current limit node
    // extract context from current limit node
    pushed_down = true;
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(pushdown_limit_for_child(child, child_context, child_result))){
      LOG_WARN("failed to push down limit for child", K(ret), K(child->get_name()));
    } else {
      // init current result from child result
      if (OB_FAIL(child_result->assign_to(allocator_, result))) {
        LOG_WARN("failed to assign result", K(ret));
      } else {
        // set child
        ObLogicalOperator* child_op = child_result->rewrited_plan_;
        limit->set_child(0, child_op);
        result->set_plan(limit);
        if (is_partial) {
          if (limit->is_plan_root()) {
            child_op->mark_is_plan_root();
            limit->get_plan()->set_plan_root(child_op);
          }
          result->set_plan(child_op);
        } else {
          // always do a merge
          // check whether child is partial limit
          if (LOG_LIMIT == child_op->get_type()) {
            ObLogLimit* child_limit = static_cast<ObLogLimit*>(child_op);
            bool is_child_simple = false;
            if (OB_FAIL(is_simple_limit(child_limit, is_child_simple))) {
              LOG_WARN("failed to check whether child limit is simple", K(ret));
            } else if (is_child_simple && child_limit->is_partial()) {
              // merge
              limit->set_child(0, child_limit->get_child(0));
              result->set_plan(limit);
            }
          }
        }
      }
    }
    // allocate partial limit for not pushdown context
    if (OB_FAIL(ret)) {
    } else {
      ObLogicalOperator *op = result == NULL ? limit : result->rewrited_plan_;
      if (OB_FAIL(allocate_partial_limit(op, not_pushdown_context, result))) {
        LOG_WARN("failed to allocate partial limit", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = limit;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_node(ObLogicalOperator *plannode, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  // do default rewriter
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plannode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(default_rewrite(plannode, context, result))) {
    LOG_WARN("failed to default rewrite", K(ret));
  }
  return ret;
}

int LimitPushdownRewriter::extract_pushdown_context(ObLogicalOperator *plannode,
                                                     LimitPushdownContext *current_context,
                                                     LimitPushdownContext *&pushdown_context,
                                                     LimitPushdownContext *&not_pushdown_context)
{
  // this is used for any spectial visit function
  int ret = OB_SUCCESS;
  if (plannode->get_filter_exprs().empty() && plannode->get_num_of_child() != 0) {
    // every thing is pushdown context
    pushdown_context = current_context;
    not_pushdown_context = NULL;
  } else {
    not_pushdown_context = current_context;
    pushdown_context = NULL;
  }
  return ret;
}
int LimitPushdownRewriter::do_pushdown_limit_for_children(ObLogicalOperator *&plannode,
                                                          LimitPushdownContext *context,
                                                          LimitPushdownResult *&result,
                                                          bool through_valuable)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  LimitPushdownContext *pushdown_context = NULL;
  LimitPushdownContext *not_pushdown_context = NULL;
  LimitPushdownResult *child_result = NULL;
  if (OB_ISNULL(plannode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (result == NULL && OB_FAIL(LimitPushdownResult::init(allocator_, result))) {
    LOG_WARN("failed to init limit pushdown result", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null", K(ret));
  } else if (OB_FALSE_IT(result->set_plan(plannode))) {
  } else if (OB_FAIL(extract_pushdown_context(plannode, context, pushdown_context, not_pushdown_context))) {
    LOG_WARN("failed to extract pushdown context", K(ret));
  } else if (pushdown_context == NULL) {
    // do default rewrite
    if (OB_FAIL(default_rewrite(plannode, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    } else {
      result->set_plan(plannode);
    }
  } else if (pushdown_context != NULL &&
             through_valuable &&
             OB_FALSE_IT(pushdown_context->becomes_valuable())) {
  } else {
    // do pushdown limit for each child
    // and set child
    for (int64_t i = 0; OB_SUCC(ret) && i < plannode->get_num_of_child(); ++i) {
      ObLogicalOperator *child = plannode->get_child(i);
      if (OB_FAIL(pushdown_limit_for_child(child, pushdown_context, child_result))) {
        LOG_WARN("failed to push down limit for child", K(ret));
      } else if (OB_ISNULL(child_result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child result is null", K(ret));
      } else {
        plannode->set_child(i, child_result->rewrited_plan_);
      }
    }
    // init current result from child result
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      result->set_plan(plannode);
      if (OB_FAIL(allocate_partial_limit(plannode, not_pushdown_context, result))) {
        LOG_WARN("failed to add partial limit", K(ret));
      }
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_set(ObLogSet *set, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  bool pushed_down = false;
  LimitPushdownContext *pushdown_context = NULL;
  LimitPushdownContext *not_pushdown_context = NULL;
  if (OB_ISNULL(set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context != NULL && OB_FAIL(extract_pushdown_context(set, context, pushdown_context, not_pushdown_context))) {
    LOG_WARN("failed to extract pushdown context", K(ret));
  } else if (pushdown_context != NULL && !set->is_recursive_union()) {
    // union all
    if (ObSelectStmt::UNION == set->get_set_op() && !set->is_set_distinct()) {
      pushed_down = true;
      ObLogicalOperator *op = set;
      if (OB_FAIL(do_pushdown_limit_for_children(op, context, result, false))) {
        LOG_WARN("failed to push down partial limit", K(ret));
      }
    } else if (pushdown_context->is_limit_one() &&
               ObSelectStmt::UNION == set->get_set_op() &&
               set->is_set_distinct()) {
      pushed_down = true;
      // note that we use context here
      // todo if none of them push limit down
      // we should add limit above union
      ObLogicalOperator *op = set;
      if (OB_FAIL(do_pushdown_limit_for_children(op, context, result, true))) {
        LOG_WARN("failed to push down partial limit", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = set;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_join(ObLogJoin *join, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  bool pushed_down = false;
  if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context == NULL || !join->get_filter_exprs().empty()) {
  } else {
    // do pushdown limit for each child
    ObJoinType join_type = join->get_join_type();
    ObLogicalOperator* left_child = join->get_child(0);
    ObLogicalOperator* right_child = join->get_child(1);
    LimitPushdownResult *left_result = NULL;
    LimitPushdownResult *right_result = NULL;
    LimitPushdownContext *dummy_context = NULL;
    if (LEFT_OUTER_JOIN == join_type) {
      // push to left
      pushed_down = true;
      if (OB_FAIL(pushdown_limit_for_child(left_child,
                                           context,
                                           left_result,
                                           true))) {
      LOG_WARN("failed to rewrite child", K(ret), K(join->get_name()));
      } else if (OB_FAIL(pushdown_limit_for_child(right_child,
                                                 dummy_context,
                                                 right_result))) {
        LOG_WARN("failed to push down limit for right child", K(ret), K(join->get_name()));
      } else {
        // init current result from child result
        if (OB_ISNULL(left_result) || OB_ISNULL(right_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child result is null", K(ret));
        } else if (OB_FAIL(left_result->assign_to(allocator_, result))) {
          LOG_WARN("failed to assign result", K(ret));
        } else {
          join->set_child(0, left_result->rewrited_plan_);
          join->set_child(1, right_result->rewrited_plan_);
          result->set_plan(join);
        }
      }
    } else if (RIGHT_OUTER_JOIN == join_type) {
      // push to right
      pushed_down = true;
      if (OB_FAIL(pushdown_limit_for_child(left_child,
                                            dummy_context,
                                            left_result))) {
        LOG_WARN("failed to push down limit for left child", K(ret), K(join->get_name()));
      } else if (OB_FAIL(pushdown_limit_for_child(right_child,
                                                   context,
                                                   right_result,
                                                   true))) {
        LOG_WARN("failed to push down limit for right child", K(ret), K(join->get_name()));
      } else {
        // init current result from child result
        if (OB_ISNULL(left_result) || OB_ISNULL(right_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child result is null", K(ret));
        } else if (OB_FAIL(left_result->assign_to(allocator_, result))) {
          LOG_WARN("failed to assign result", K(ret));
        } else {
          join->set_child(0, left_result->rewrited_plan_);
          join->set_child(1, right_result->rewrited_plan_);
          result->set_plan(join);
        }
      }
    } else if (INNER_JOIN == join_type && join->is_cartesian()) {
      // push to both side
      pushed_down = true;
      if (OB_FAIL(pushdown_limit_for_child(left_child,
                                            context,
                                            left_result,
                                            true))) {
        LOG_WARN("failed to push down limit for left child", K(ret), K(join->get_name()));
      } else if (OB_FAIL(pushdown_limit_for_child(right_child,
                                                   context,
                                                   right_result,
                                                   true))) {
        LOG_WARN("failed to push down limit for right child", K(ret), K(join->get_name()));
      } else {
        // init current result from child result
        if (OB_ISNULL(left_result) || OB_ISNULL(right_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child result is null", K(ret));
        } else if (OB_FAIL(left_result->assign_to(allocator_, result))) {
          LOG_WARN("failed to assign result", K(ret));
        } else {
          join->set_child(0, left_result->rewrited_plan_);
          join->set_child(1, right_result->rewrited_plan_);
          result->set_plan(join);
        }
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = join;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::contains_linked_scan(ObLogicalOperator* root, bool & contains)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (LOG_LINK_SCAN == root->get_type()) {
    contains = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contains && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_FAIL(SMART_CALL(contains_linked_scan(child, contains)))) {
        LOG_WARN("failed to check linked table", K(ret));
      }
    }
  }
  return ret;
}
int LimitPushdownRewriter::visit_material(ObLogMaterial *material, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  bool contains_linked_table = false;
  if (OB_ISNULL(material)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObLogicalOperator *op = material;
    // do not push if contains linkscan below
    if (OB_FAIL(contains_linked_scan(op, contains_linked_table))) {
      LOG_WARN("failed to check linked table", K(ret));
    } else if (contains_linked_table) {
    } else if (OB_FAIL(do_pushdown_limit_for_children(op, context, result, true))) {
      LOG_WARN("failed to push down limit through material", K(ret));
    }
  }
  if (OB_SUCC(ret) && contains_linked_table) {
    // do default rewrite
    ObLogicalOperator *op = material;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_subplan_scan(ObLogSubPlanScan *subplan_scan, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  if (OB_ISNULL(subplan_scan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObLogicalOperator *op = subplan_scan;
    if (OB_FAIL(do_pushdown_limit_for_children(op, context, result, context != NULL && context->through_valuable_node_))) {
      LOG_WARN("failed to push down limit through subplan scan", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_table_scan(ObLogTableScan *table_scan, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  if (OB_ISNULL(table_scan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(LimitPushdownResult::init(allocator_, result))) {
    LOG_WARN("failed to init limit pushdown result", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null", K(ret));
  } else if (OB_FALSE_IT(result->set_plan(table_scan))) {
  } else if (context != NULL) {
    bool is_pushed = false;
    ObLogPlan *plan = table_scan->get_plan();
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(plan->try_push_limit_into_table_scan(table_scan, context->limit_expr_, context->limit_expr_, NULL, is_pushed))) {
      LOG_WARN("failed to push limit into table scan", K(ret));
    } else if (is_pushed && table_scan->is_single()) {
      // do nothing
    } else if (!is_pushed && table_scan->get_limit_expr() != NULL) {
      // no need to add partial limit above
      // result->set_pushed_down(true);
    } else {
      ObLogicalOperator *op = table_scan;
      if (OB_FAIL(allocate_partial_limit(op, context, result))) {
        LOG_WARN("failed to allocate partial limit above tablescan", K(ret));
      }
    }
  } else {
    result->set_plan(table_scan);
  }
  return ret;
}

int LimitPushdownRewriter::visit_subplan_filter(ObLogSubPlanFilter *subplan_filter, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool pushed_down = false;
  // do pushdown limit to source child
  if (OB_ISNULL(subplan_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context != NULL) {
    // extract pushdown context
    LimitPushdownContext *pushdown_context = NULL;
    LimitPushdownContext *not_pushdown_context = NULL;
    LimitPushdownResult *child_result = NULL;
    LimitPushdownResult *other_child_result = NULL;
    if (OB_FAIL(extract_pushdown_context(subplan_filter, context, pushdown_context, not_pushdown_context))) {
      LOG_WARN("failed to extract pushdown context", K(ret));
    } else if (pushdown_context != NULL) {
      // if limit count is 1 and subplan filter is distinct
      pushed_down = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < subplan_filter->get_num_of_child(); ++i) {
        ObLogicalOperator *child = subplan_filter->get_child(i);
        if (i == 0) {
          if (OB_FAIL(pushdown_limit_for_child(child, pushdown_context, child_result))) {
            LOG_WARN("failed to push down limit for child", K(ret));
          } else if (OB_ISNULL(child_result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child result is null", K(ret));
          } else {
            subplan_filter->set_child(i, child_result->rewrited_plan_);
          }
        } else {
          if (OB_FAIL(pushdown_limit_for_child(child, NULL, other_child_result))) {
            LOG_WARN("failed to push down limit for child", K(ret));
          } else if (OB_ISNULL(other_child_result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("other child result is null", K(ret));
          } else {
            subplan_filter->set_child(i, other_child_result->rewrited_plan_);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(child_result->assign_to(allocator_, result))) {
        LOG_WARN("failed to assign result", K(ret));
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        result->set_plan(subplan_filter);
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = subplan_filter;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::valid_for_push_limit_into_group_by(ObLogGroupBy *group_by, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(group_by) || OB_ISNULL(group_by->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    is_valid = group_by->get_plan()->get_optimizer_context().enable_hash_groupby_limit_pushdown() &&
               HASH_AGGREGATE == group_by->get_algo()
              && group_by->get_filter_exprs().empty()
              && !group_by->has_rollup()
              && !group_by->is_three_stage_aggr();
  }
  return ret;
}
// todo make this logic a new rule
// do not mixed with limit pushdown
int LimitPushdownRewriter::visit_groupby(ObLogGroupBy *group_by, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit into group by
  bool pushed_down = false;
  bool is_valid_pushdown = false;
  if (OB_ISNULL(group_by)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context != NULL) {
    if (OB_FAIL(valid_for_push_limit_into_group_by(group_by, is_valid_pushdown))) {
    } else if (is_valid_pushdown) {
      LimitPushdownContext *pushdown_context = NULL;
      LimitPushdownContext *not_pushdown_context = NULL;
      if (OB_FAIL(extract_pushdown_context(group_by, context, pushdown_context, not_pushdown_context))) {
        LOG_WARN("failed to extract pushdown context", K(ret));
      } else if (pushdown_context != NULL) {
        pushed_down = true;
        group_by->set_limit_expr(pushdown_context->limit_expr_);
        LimitPushdownResult *child_result = NULL;
        ObLogicalOperator *op = group_by;
        if (OB_FAIL(do_pushdown_limit_for_children(op, NULL, result))) {
          LOG_WARN("failed to push down limit for child", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = group_by;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_sort(ObLogSort *sort, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  bool pushed_down = false;
  if (OB_ISNULL(sort)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context != NULL) {
    // extract pushdown context
    LimitPushdownContext *pushdown_context = NULL;
    LimitPushdownContext *not_pushdown_context = NULL;
    if (OB_FAIL(extract_pushdown_context(sort, context, pushdown_context, not_pushdown_context))) {
      LOG_WARN("failed to extract pushdown context", K(ret));
    } else if (pushdown_context != NULL && sort->is_topn_op()) {
      // we should stop pushdown limit here
      // todo update topn expr to min of both
      pushed_down = true;
      ObLogicalOperator *op = sort;
      if (OB_FAIL(do_pushdown_limit_for_children(op, NULL, result))) {
         LOG_WARN("failed to push down limit for child", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = sort;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::visit_distinct(ObLogDistinct *distinct, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  bool pushed_down = false;
  if (OB_ISNULL(distinct)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (context != NULL) {
    // extract pushdown context
    LimitPushdownContext *pushdown_context = NULL;
    LimitPushdownContext *not_pushdown_context = NULL;
    if (OB_FAIL(extract_pushdown_context(distinct, context, pushdown_context, not_pushdown_context))) {
      LOG_WARN("failed to extract pushdown context", K(ret));
    } else if (pushdown_context != NULL) {
      // if limit count is 1 and set is distinct
      if (pushdown_context->is_limit_one()) {
        // partial distinct
        pushed_down = true;
        bool is_partial = distinct->is_push_down();
        // note that we use context here
        // todo if none of them push limit down
        // we should add limit above union
        ObLogicalOperator *op = distinct;
        if (OB_FAIL(do_pushdown_limit_for_children(op, context, result, true))) {
          LOG_WARN("failed to push down partial limit", K(ret));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else {
          // delete this partial distinct
          ObLogicalOperator *child_op = op->get_child(0);
          if (is_partial) {
            if (op->is_plan_root()) {
              child_op->mark_is_plan_root();
              child_op->get_plan()->set_plan_root(child_op);
            }
            op = child_op;
          }
          result->set_plan(op);
        }
      }
    }
  }
  if (OB_SUCC(ret) && !pushed_down) {
    // do default rewrite
    ObLogicalOperator *op = distinct;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}
int LimitPushdownRewriter::visit_exchange(ObLogExchange *exchange, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  // do pushdown limit
  // exchange may also has filter on it
  bool pushdown = false;
  if (OB_ISNULL(exchange)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
    // is local order shuffle or merge shuffle
  } else if (context != NULL) {
    // extract pushdown context
    LimitPushdownContext *pushdown_context = NULL;
    LimitPushdownContext *not_pushdown_context = NULL;
    if (OB_FAIL(extract_pushdown_context(exchange, context, pushdown_context, not_pushdown_context))) {
      LOG_WARN("failed to extract pushdown context", K(ret));
    } else if (pushdown_context != NULL) {
      if (!exchange->is_merge_sort()
             && !exchange->is_sort_local_order()
             && exchange->get_sample_type() == NOT_INIT_SAMPLE_TYPE
             && !exchange->is_task_order()
             && exchange->is_consumer()) {
        // do pushdown limit to its children
        // and set child
        ObLogicalOperator* child = exchange->get_child(0);
        // if child is also exchange
        if (child->get_type() == LOG_EXCHANGE) {
          pushdown = true;
          LimitPushdownResult *child_result = NULL;
          ObLogicalOperator *child_of_child = child->get_child(0);
          if (OB_FAIL(pushdown_limit_for_child(child_of_child, context, child_result, true))) {
            LOG_WARN("failed to push down limit for child", K(ret));
          } else {
            if (OB_ISNULL(child_result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("child result is null", K(ret));
            } else if (OB_FAIL(child_result->assign_to(allocator_, result))) {
              LOG_WARN("failed to assign result", K(ret));
            } else {
              child->set_child(0, child_result->rewrited_plan_);
              exchange->set_child(0, child);
              result->set_plan(exchange);
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !pushdown) {
    // do default rewrite
    ObLogicalOperator *op = exchange;
    if (OB_FAIL(default_rewrite(op, context, result))) {
      LOG_WARN("failed to default rewrite", K(ret));
    }
  }
  return ret;
}

int LimitPushdownRewriter::pushdown_limit_for_child(ObLogicalOperator *&child,
                                                    LimitPushdownContext *context,
                                                    LimitPushdownResult *&result,
                                                    bool valuable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (valuable && context != NULL && OB_FALSE_IT(context->becomes_valuable())) {
  } else if (OB_FAIL(this->dispatch_visit(child, context, result))){
    LOG_WARN("failed to rewrite child", K(ret));
  }
  return ret;
}

int LimitPushdownRewriter::allocate_partial_limit(ObLogicalOperator *&plannode, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    if (OB_FAIL(LimitPushdownResult::init(allocator_, result))) {
      LOG_WARN("failed to init limit pushdown result", K(ret));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      result->set_plan(plannode);
    }
  } else {
    result->set_plan(plannode);
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (context == NULL) {
    // do nothing
  } else {
    // allocate limit here
    ObLogPlan *plan = plannode->get_plan();
    if (OB_ISNULL(plan) || OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (context != NULL && context->limit_count_ >= 0 && context->through_valuable_node_) {
      if (OB_FAIL(plan->add_partial_limit_as_top(plannode, context->limit_expr_))) {
        LOG_WARN("failed adding partial limit above node", K(plannode->get_name()));
      } else if (plannode->get_op_id() == OB_INVALID_ID &&
                 OB_FALSE_IT(plannode->set_op_id(ctx_->get_next_op_id()))) {
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        result->set_plan(plannode);
      }
    }
  }
  return ret;
}

int LimitPushdownRewriter::default_rewrite(ObLogicalOperator *&plannode, LimitPushdownContext *context, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  result = NULL;
  // by default we push NULL to each child
  for (int64_t i = 0; OB_SUCC(ret) && i < plannode->get_num_of_child(); ++i) {
    ObLogicalOperator* child = plannode->get_child(i);
    LimitPushdownResult *child_result = NULL;
    if (OB_FAIL(this->dispatch_visit(child, NULL, child_result))) {
      LOG_WARN("failed to rewrite child", K(ret), K(plannode->get_name()));
    } else if (OB_ISNULL(child_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child result is null", K(ret));
    } else {
      plannode->set_child(i, child_result->rewrited_plan_);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(allocate_partial_limit(plannode, context, result))) {
    LOG_WARN("failed to allocate partial limit", K(ret));
  }
  return ret;
}

int LimitPushdownContext::init(common::ObIAllocator &allocator, ObRawExpr *limit_expr, int64_t limit_count, bool through_valuable_node, LimitPushdownContext *&context)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = static_cast<LimitPushdownContext *>(allocator.alloc(sizeof(LimitPushdownContext))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc limit pushdown context", K(ret));
  } else {
    context = new (ptr) LimitPushdownContext();
    context->limit_expr_ = limit_expr;
    context->limit_count_ = limit_count;
    context->through_valuable_node_ = through_valuable_node;
  }
  return ret;
}
int LimitPushdownResult::init(common::ObIAllocator &allocator, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = static_cast<LimitPushdownResult *>(allocator.alloc(sizeof(LimitPushdownResult))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc limit pushdown result", K(ret));
  } else {
    result = new (ptr) LimitPushdownResult();
  }
  return ret;
}

int LimitPushdownResult::assign_to(common::ObIAllocator &allocator, LimitPushdownResult *&result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(LimitPushdownResult::init(allocator, result))) {
    LOG_WARN("failed to init limit pushdown result", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null", K(ret));
  } else {
    result->set_plan(rewrited_plan_);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

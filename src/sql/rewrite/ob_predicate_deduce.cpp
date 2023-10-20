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

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObPredicateDeduce::add_predicate(ObRawExpr *pred, bool &is_added)
{
  int ret = OB_SUCCESS;
  ObObjMeta cmp_meta;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  is_added = false;
  if (OB_ISNULL(pred) || OB_ISNULL(left_expr = pred->get_param_expr(0)) ||
      OB_ISNULL(right_expr = pred->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("predicate is invalid", K(ret), K(pred), K(left_expr), K(right_expr));
  } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(
                       cmp_meta,
                       left_expr->get_result_type(),
                       right_expr->get_result_type()))) {
    LOG_WARN("failed to get equal meta", K(ret), K(*pred));
  } else if (cmp_type_ == cmp_meta || input_preds_.empty()) {
    cmp_type_ = cmp_meta;
    if (OB_FAIL(input_preds_.push_back(pred))) {
      LOG_WARN("failed to push bach edges", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(input_exprs_, left_expr))) {
      LOG_WARN("failed to push back nodes", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(input_exprs_, right_expr))) {
      LOG_WARN("failed to push back nodes", K(ret));
    } else {
      is_added = true;
    }
  }
  return ret;
}

int ObPredicateDeduce::check_deduce_validity(ObRawExpr *cond, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  is_valid = true;
  if (OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition is null", K(ret));
  } else if (!is_simple_condition(cond->get_expr_type()) ||
             contain_special_expr(*cond) ||
             cond->is_static_const_expr()) {
    is_valid = false;
  } else if (OB_ISNULL(left_expr = cond->get_param_expr(0)) ||
             OB_ISNULL(right_expr = cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param exprs are null", K(ret), K(*cond));
  } else if (!left_expr->get_result_type().is_valid() ||
             !right_expr->get_result_type().is_valid()) {
    is_valid = false;
  } else if (left_expr->get_expr_type() == T_OP_ROW || right_expr->get_expr_type() == T_OP_ROW) {
    is_valid = false;
  } else if (left_expr == right_expr || left_expr->same_as(*right_expr)) {
    is_valid = false;
  }
  return ret;
}

int ObPredicateDeduce::deduce_simple_predicates(ObTransformerCtx &ctx,
                                               ObIArray<ObRawExpr *> &result)
{
  int ret = OB_SUCCESS;
  ObArray<uint8_t> chosen;
  if (OB_FAIL(init())) {
    LOG_WARN("failed to init graph", K(ret));
  } else if (OB_FAIL(chosen.prepare_allocate(N * N))) {
    LOG_WARN("failed to prepaer allocate graph", K(ret));
  } else {
    for (int64_t i = 0; i < chosen.count(); ++i) {
      chosen.at(i) = 0;
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlBitSet<> expr_equal_with_const;
    if (OB_FAIL(choose_equal_preds(chosen, expr_equal_with_const))) {
      LOG_WARN("failed to choose equal predicates", K(ret));
    } else if (OB_FAIL(choose_unequal_preds(ctx, chosen, expr_equal_with_const))) {
      LOG_WARN("failed to choose unequal predicates", K(ret));
    } else if (OB_FAIL(choose_input_preds(chosen, result))) {
      LOG_WARN("failed to choose input preds", K(ret));
    } else if (OB_FAIL(create_simple_preds(ctx, chosen, result))) {
      LOG_WARN("failed to create simple predicates", K(ret));
    }
  }
  return ret;
}

int ObPredicateDeduce::init()
{
  int ret = OB_SUCCESS;
  N = input_exprs_.count();
  if (OB_FAIL(graph_.prepare_allocate(N * N))) {
    LOG_WARN("failed to prepare allocate graph", K(ret));
  } else if (OB_FAIL(type_safety_.prepare_allocate(N * N))) {
    LOG_WARN("failed to prepare allocate type safe array", K(ret));
  }
  for (int64_t i = 0 ; OB_SUCC(ret) && i < N; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < N; ++j) {
      bool &type_safe = type_safety_.at(i * N + j);
      type_safe = false;
      graph_.at(i * N + j) = 0;
      if (OB_FAIL(check_type_safe(i, j, type_safe))) {
        LOG_WARN("failed to check type safe", K(ret));
      }
    }
    set(graph_.at(i * N + i), EQ);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_preds_.count(); ++i) {
    int64_t left_id  = -1;
    int64_t right_id = -1;
    Type type;
    if (OB_FAIL(convert_pred(input_preds_.at(i), left_id, right_id, type))) {
      LOG_WARN("failed to check predicate", K(ret));
    } else {
      set(graph_, left_id, right_id, type);
    }
  }
  if (OB_SUCC(ret)) {
    deduce(graph_);
  }
  return ret;
}

int ObPredicateDeduce::choose_equal_preds(ObIArray<uint8_t> &chosen,
                                          ObSqlBitSet<> &expr_equal_with_const)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> table_filter;
  ObSEArray<int64_t, 8> equal_pairs;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(equal_pairs.push_back(INT64_MAX))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    for (int64_t j = i + 1; OB_SUCC(ret) && j < N; ++j) {
      if (!has(graph_, i, j, EQ) || i == j || !is_type_safe(i, j)) {
        // do nothing
      } else if (is_raw_const(i) && is_raw_const(j)) {
        // do nothing for startup filter
      } else if (is_raw_const(i) || is_raw_const(j)) {
        set(chosen, i, j, EQ);
        int64_t const_id = is_raw_const(i) ?  i : j;
        int64_t var_id = (const_id != i) ? i : j;

        if (equal_pairs.at(var_id) > const_id) {
          equal_pairs.at(var_id) = const_id;
        }
        if (OB_FAIL(expr_equal_with_const.add_member(var_id))) {
          LOG_WARN("failed to add member", K(ret));
        }
      } else if (is_const(i) && is_const(j)) {
        set(chosen, i, j, EQ);
      } else if (!is_table_filter(i, j)) {
        // do nothing
      } else if (OB_FAIL(table_filter.push_back(i * N + j))) {
        LOG_WARN("failed to push back table filter", K(ret));
      }
//      bool is_eq = has(graph_, i, j, EQ);
//      bool is_ch = has(chosen, i, j, EQ);
//      LOG_TRACE("print predicate",
//               "first expr",
//               ObLogPrintName<ObRawExpr>(*input_exprs_.at(i)),
//               "second expr",
//               ObLogPrintName<ObRawExpr>(*input_exprs_.at(j)), K(is_eq), K(is_ch));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_filter.count(); ++i) {
    int64_t left = table_filter.at(i) / N;
    int64_t right = table_filter.at(i) % N;
    if (equal_pairs.at(left) == INT64_MAX || equal_pairs.at(right) == INT64_MAX) {
      set(chosen, left, right, EQ);
      equal_pairs.at(left) = left;
      equal_pairs.at(right) = left;
    }
  }
  return ret;
}

int ObPredicateDeduce::choose_unequal_preds(ObTransformerCtx &ctx,
                                            ObIArray<uint8_t> &chosen,
                                            ObSqlBitSet<> &expr_equal_with_const)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> ordered_list;
  if (OB_FAIL(topo_sort(topo_order_))) {
    LOG_WARN("failed to topo sort", K(ret));
  }
  for (int64_t i = 0; i < topo_order_.count(); ++i) {
    // if a variable A equal with a const B,
    // there is no need to deduce unequal predicates like A > c1 for A
    // because, it is better to replace that with B > c1
    if (topo_order_.at(i) < 0 || topo_order_.at(i) >= N) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr id", K(ret), K(topo_order_.at(i)));
    } else if (expr_equal_with_const.has_member(topo_order_.at(i))) {
      // do nothing
    } else if (OB_FAIL(ordered_list.push_back(topo_order_.at(i)))) {
      LOG_WARN("failed to push back member", K(ret));
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < ordered_list.count(); ++i) {
    int64_t left = ordered_list.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      int64_t right = ordered_list.at(j);
      bool skip_check = false;
      Type type = has(graph_, left, right, GT) ?
                    GT : has(graph_, left, right, GE) ? GE : EQ;
      if (!is_type_safe(left, right) || (type == EQ)) {
        // do nothing
      } else if (!is_table_filter(left, right)) {
        // do nothing
      } else if (OB_FAIL(check_index_part_cond(ctx,
                                               input_exprs_.at(left),
                                               input_exprs_.at(right),
                                               skip_check))) {
        LOG_WARN("failed to check is index partition condition", K(ret));
      } else {
        bool can_deduced = false;
        for (int64_t k = j + 1; !skip_check && !can_deduced && k < i; ++k) {
          int64_t mid = ordered_list.at(k);
          if (is_const(mid)) {
            can_deduced = check_deduciable(graph_, mid, left, right, type);
          } else if (skip_check) {
            // do nothing
          } else if (is_table_filter(left, mid) &&
                     is_table_filter(mid, right)) {
            can_deduced = check_deduciable(graph_, mid, left, right, type);
          }
        }
        if (!can_deduced) {
          set(chosen, left, right, type);
        }
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::check_index_part_cond(ObTransformerCtx &ctx,
                                             ObRawExpr *left_expr,
                                             ObRawExpr *right_expr,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObRawExpr *check_expr = NULL;
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(ret), K(left_expr), K(right_expr));
  } else if (left_expr->is_column_ref_expr() && right_expr->is_const_expr()) {
    check_expr = left_expr;
  } else if (right_expr->is_column_ref_expr() && left_expr->is_const_expr()) {
    check_expr = right_expr;
  }
  if (OB_SUCC(ret) && NULL != check_expr) {
    if (OB_FAIL(ObTransformUtils::check_is_index_part_key(ctx, stmt_, check_expr, is_valid))) {
      LOG_WARN("fail to check if check_expr is index or part key", K(ret));
    }
  }
  return ret;
}

int ObPredicateDeduce::choose_input_preds(ObIArray<uint8_t> &chosen,
                                          ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  int64_t left = -1;
  int64_t right = -1;
  Type type = EQ;
  ObArray<uint8_t> deduced;
  ObArray<bool> keep;
  if (OB_FAIL(deduced.assign(chosen))) {
    LOG_WARN("failed to assign array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (i * N + i >= chosen.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret));
    } else {
      set(deduced, i, i, EQ);
    }
  }
  if (OB_SUCC(ret)) {
    deduce(deduced);
  }
  const Type types[3] = {EQ, GT, GE};
  for (int64_t op_type = 0; OB_SUCC(ret) && op_type <= 2; ++op_type) {
    const Type check_type = types[op_type];
    for (int64_t i = 0; OB_SUCC(ret) && i < input_preds_.count(); ++i) {
      ObRawExpr *pred = input_preds_.at(i);
      if (op_type == 0 && OB_FAIL(keep.push_back(false)))  {
        LOG_WARN("failed to init array", K(ret));
      } else if (OB_FAIL(convert_pred(pred, left, right, type))) {
        LOG_WARN("failed to convert predicate", K(ret));
      } else if (check_type == type) {
        if (has(chosen, left, right, type)) {
          keep.at(i) = true;
          clear(chosen, left, right, type);
        } else if (!has(deduced, left, right, type)) {
          keep.at(i) = true;
          set(deduced, left, right, type);
          expand_graph(deduced, left, right);
          if (type == EQ) {
            expand_graph(deduced, right, left);
          }
        }
      }
    }
  }
  // we do not want to change the order of input preds
  // hence, all selected input preds are added into output in the order.
  for (int64_t i = 0; OB_SUCC(ret) && i < input_preds_.count(); ++i) {
    if (!keep.at(i)) {
      // do nothing
    } else if (OB_FAIL(output_exprs.push_back(input_preds_.at(i)))) {
      LOG_WARN("failed to push back input predicate", K(ret));
    }
  }
  return ret;
}

int ObPredicateDeduce::create_simple_preds(ObTransformerCtx &ctx,
                                           ObIArray<uint8_t> &chosen,
                                           ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *pred = NULL;
  ObRawExprFactory *expr_factory = ctx.expr_factory_;
  ObSQLSessionInfo *session_info = ctx.session_info_;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(session_info), K(expr_factory));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < N; ++j) {
      const uint8_t edge = chosen.at(i * N + j);
      pred = NULL;
      if (OB_SUCC(ret) && has(edge, EQ)) {
        clear(chosen, i, j, EQ);
        if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                      *expr_factory, session_info, T_OP_EQ,
                      pred, input_exprs_.at(i), input_exprs_.at(j)))) {
          LOG_WARN("failed to create double op expr", K(ret));
        } else if (OB_FAIL(pred->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(output_exprs.push_back(pred))) {
          LOG_WARN("failed to push back pred", K(ret));
        }
      }
      if (OB_SUCC(ret) && has(edge, GT)) {
        clear(chosen, i, j, GT);
        if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                      *expr_factory, session_info, T_OP_GT,
                      pred, input_exprs_.at(i), input_exprs_.at(j)))) {
          LOG_WARN("failed to create double op expr", K(ret));
        } else if (OB_FAIL(pred->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(output_exprs.push_back(pred))) {
          LOG_WARN("failed to push back pred", K(ret));
        }
      }
      if (OB_SUCC(ret) && has(edge, GE)) {
        clear(chosen, i, j, GE);
        if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                      *expr_factory, session_info, T_OP_GE,
                      pred, input_exprs_.at(i), input_exprs_.at(j)))) {
          LOG_WARN("failed to create double op expr", K(ret));
        } else if (OB_FAIL(pred->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(output_exprs.push_back(pred))) {
          LOG_WARN("failed to push back pred", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::convert_pred(const ObRawExpr *pred,
                                      int64_t &left_id,
                                      int64_t &right_id,
                                      Type &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pred)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("predicate is null", K(ret), K(pred));
  } else if (!find_equal_expr(input_exprs_, pred->get_param_expr(0), &left_id) ||
             !find_equal_expr(input_exprs_, pred->get_param_expr(1), &right_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("does not find expr", K(ret), K(*pred), K(left_id), K(right_id));
  } else if (pred->get_expr_type() == T_OP_EQ) {
    type = EQ;
  } else if (pred->get_expr_type() == T_OP_GT ||
             pred->get_expr_type() == T_OP_GE) {
    type = (pred->get_expr_type() == T_OP_GT ? GT : GE);
  } else if (pred->get_expr_type() == T_OP_LT ||
             pred->get_expr_type() == T_OP_LE) {
    type = (pred->get_expr_type() == T_OP_LT ? GT : GE);
    std::swap(left_id, right_id);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid predicate", K(ret), K(*pred));
  }
  return ret;
}

/**
 * @brief ObRawExprDeducer::deduce
 * @return 推导所有表达式之间两两可能的关系
 * 输入限制：不能带 const-const 条件
 *          不能带 c1 <-> c1 条件
 */
int ObPredicateDeduce::deduce(ObIArray<uint8_t> &graph)
{
  int ret = OB_SUCCESS;
  for (int64_t hub = 0; hub < N; ++hub) {
    for (int64_t left = 0; left < N; ++left) {
      for (int64_t right = 0; right < N; ++right) {
        connect(graph.at(left * N + right),
                graph.at(left * N + hub),
                graph.at(hub * N + right));
      }
    }
  }
  return ret;
}

void ObPredicateDeduce::expand_graph(ObIArray<uint8_t> &graph, int64_t hub1, int64_t hub2)
{
  int64_t left = 0;
  int64_t right = hub2;
  for (left = 0; left < N; ++left) {
    connect(graph.at(left * N + right),
            graph.at(left * N + hub1),
            graph.at(hub1 * N + right));
  }
  for (int64_t left = 0; left < N; ++left) {
    for (int64_t right = 0; right < N; ++right) {
      connect(graph.at(left * N + right),
              graph.at(left * N + hub2),
              graph.at(hub2 * N + right));
    }
  }
}

/**
 * @brief ObPredicateDeduce::connect
 * @param left_right
 * @param left_hub
 * @param hub_right
 * 给定 left -> hub, hub -> 推导 left -> right 的关系
 */
void ObPredicateDeduce::connect(uint8_t &left_right,
                               const uint8_t left_mid,
                               const uint8_t mid_right)
{
  if (!has(left_right, EQ)) {
    if (has(left_mid, EQ) && has(mid_right, EQ)) {
      set(left_right, EQ);
    }
  }
  if (!has(left_right, GE)) {
    if ((has(left_mid, EQ) && has(mid_right, GE)) ||
        (has(left_mid, GE) && has(mid_right, EQ))) {
      set(left_right, GE);
    }
  }
  if (!has(left_right, GT)) {
    if ((has(left_mid, GT) && mid_right != 0) ||
        (left_mid != 0 && has(mid_right, GT))) {
      set(left_right, GT);
    }
  }
}

int ObPredicateDeduce::check_type_safe(int64_t first, int64_t second, bool &type_safe)
{
  int ret = OB_SUCCESS;
  ObRawExpr *first_expr = NULL;
  ObRawExpr *second_expr = NULL;
  ObObjMeta cmp_meta;
  type_safe = false;
  if (first < 0 || first >= input_exprs_.count() ||
      second < 0 || second >= input_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr id", K(ret), K(first), K(second));
  } else if (OB_ISNULL(first_expr = input_exprs_.at(first)) ||
             OB_ISNULL(second_expr = input_exprs_.at(second))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(first_expr), K(second_expr));
  } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(
                       cmp_meta,
                       first_expr->get_result_type(),
                       second_expr->get_result_type()))) {
    LOG_WARN("failed to get equal meta", K(ret));
  } else {
    type_safe = (cmp_meta == cmp_type_);
  }
  return ret;
}

int ObPredicateDeduce::deduce_general_predicates(ObTransformerCtx &ctx,
                                                ObIArray<ObRawExpr *> &target_exprs,
                                                ObIArray<ObRawExpr *> &general_preds,
                                                ObIArray<std::pair<ObRawExpr *, ObRawExpr *>> &lossless_cast_preds,
                                                ObIArray<ObRawExpr *> &result)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < general_preds.count(); ++i) {
    ObSEArray<ObRawExpr *, 4> equal_exprs;
    bool is_valid = false;
    if (OB_FAIL(check_general_expr_validity(general_preds.at(i), is_valid))) {
      LOG_WARN("failed to check valid general expr", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(get_equal_exprs(general_preds.at(i),
                                       general_preds,
                                       target_exprs,
                                       equal_exprs))) {
      LOG_WARN("failed to find equal columns", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < equal_exprs.count(); ++j) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(ObRawExprCopier::copy_expr_node(*ctx.expr_factory_,
                                                  general_preds.at(i),
                                                  new_pred))) {
        LOG_WARN("failed to copy the predicate", K(ret));
      } else {
        new_pred->get_param_expr(0) = equal_exprs.at(j);
        if (OB_FAIL(new_pred->formalize(ctx.session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (OB_FAIL(new_pred->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(result.push_back(new_pred))) {
          LOG_WARN("failed to push back new pred", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < lossless_cast_preds.count(); ++i) {
    ObRawExpr *cast_expr = NULL;
    ObRawExpr *pred = NULL;
    const ObRawExpr *column_expr = NULL;
    int64_t param_idx = -1;
    if (OB_ISNULL(cast_expr = lossless_cast_preds.at(i).first) ||
        OB_ISNULL(pred = lossless_cast_preds.at(i).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(cast_expr, column_expr))) {
      LOG_WARN("fail to get real expr", K(ret), K(cast_expr));
    } else if (!ObOptimizerUtil::find_item(input_exprs_, column_expr, &param_idx)) {
      // do nothing
    } else {
      ObSEArray<const ObRawExpr *, 4> equal_exprs;
      for (int64_t j = 0; OB_SUCC(ret) && j < N; ++j) {
        const ObRawExpr *expr = input_exprs_.at(j);
        if (!has(graph_, param_idx, j, EQ) || j == param_idx) {
          // do nothing
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input expr is null", K(ret));
        } else if (!expr->is_column_ref_expr()) {
          // do nothing
        } else if (OB_FAIL(equal_exprs.push_back(expr))) {
          LOG_WARN("failed to push back exprs");
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < equal_exprs.count(); ++j) {
        ObRawExpr *new_pred = NULL;
        ObRawExprCopier copier(*ctx.expr_factory_);
        if (OB_FAIL(copier.add_replaced_expr(cast_expr, equal_exprs.at(j)))) {
          LOG_WARN("failed to add replaced expr", K(ret));
        } else if (OB_FAIL(copier.copy_on_replace(pred, new_pred))) {
          LOG_WARN("failed to copy expr node", K(ret));
        } else if (OB_FAIL(new_pred->formalize(ctx.session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (OB_FAIL(new_pred->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(result.push_back(new_pred))) {
          LOG_WARN("failed to push back new pred", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::check_general_expr_validity(ObRawExpr *general_expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(general_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (contain_special_expr(*general_expr)) {
    // do nothing
  } else if (!is_general_condition(general_expr->get_expr_type())) {
    // do nothing
  } else {
    is_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < general_expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = NULL;
      if (OB_ISNULL(param_expr = general_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret), K(param_expr));
      } else if (i == 0) {
        is_valid = param_expr->has_flag(IS_COLUMN);
      } else {
        is_valid = param_expr->is_const_expr();
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::get_equal_exprs(ObRawExpr *pred,
                                      ObIArray<ObRawExpr *> &general_preds,
                                      ObIArray<ObRawExpr *> &target_exprs,
                                      ObIArray<ObRawExpr *> &equal_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> first_params;
  ObSEArray<ObRawExpr *, 4> candi_exprs;
  int64_t param_idx = -1;
  ObRawExpr *param_expr = NULL;
  const TableItem* table_item = NULL;
  if (OB_ISNULL(pred) || OB_ISNULL(param_expr = pred->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prediate is invalid", K(ret), K(pred), K(param_expr));
  } else if (OB_FAIL(find_similar_expr(pred, general_preds, first_params))) {
    LOG_WARN("failed to find general expr", K(ret));
  } else if (ObOptimizerUtil::find_item(input_exprs_, param_expr, &param_idx)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr *expr = input_exprs_.at(i);
      const ObRawExpr *real_expr = expr;
      bool need_check_type_safe = false;
      if (!has(graph_, param_idx, i, EQ) || i == param_idx) {
        // do nothing
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input expr is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(expr, real_expr))) {
        LOG_WARN("fail to get real expr", K(ret), K(expr));
      } else if (!real_expr->is_column_ref_expr()) {
        // do nothing
      } else if (!ObOptimizerUtil::find_item(target_exprs, real_expr)) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(first_params, expr)) {
        // do nothing
      } else if (OB_ISNULL(table_item = stmt_.get_table_item_by_id(
                                              static_cast<const ObColumnRefRawExpr*>(real_expr)->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_OP_IN == pred->get_expr_type() &&
                 (!table_item->is_basic_table() ||
                  has_raw_const_equal_condition(i))) {
        // deduce IN predicates for column parameters that only have basic tables and do not contain const equal predicates and IN predicates
        // do nothing
      } else if (param_expr->get_result_type().get_type() != expr->get_result_type().get_type()) {
        need_check_type_safe = is_type_safe(param_idx, i);
      } else if (ob_is_string_or_lob_type(param_expr->get_result_type().get_type())
                && ((param_expr->get_result_type().get_collation_level() != expr->get_result_type().get_collation_level())
                    || (param_expr->get_result_type().get_collation_type() != expr->get_result_type().get_collation_type()))) {
        need_check_type_safe = is_type_safe(param_idx, i);
      } else if (OB_FAIL(equal_exprs.push_back(expr))) {
        LOG_WARN("failed to push back equal expr", K(ret));
      }
      if (OB_SUCC(ret) && need_check_type_safe && OB_FAIL(candi_exprs.push_back(expr))) {
        LOG_WARN("failed to push back candi exprs whose result type is different from param_expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && !candi_exprs.empty()) {
      bool type_safe = false;
      if (OB_FAIL(check_cmp_metas_for_general_preds(param_expr, pred, type_safe))) {
        LOG_WARN("fail to get cmp metas for the param expr", K(ret));
      } else if (type_safe) {
        for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
          type_safe = false;
          if (OB_FAIL(check_cmp_metas_for_general_preds(candi_exprs.at(i), pred, type_safe))) {
            LOG_WARN("fail to get cmp metas for candi exprs", K(ret));
          } else if (type_safe && OB_FAIL(equal_exprs.push_back(candi_exprs.at(i)))) {
            LOG_WARN("failed to push back equal expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::check_cmp_metas_for_general_preds(ObRawExpr *left_expr, ObRawExpr *pred, bool &type_safe) {
  int ret = OB_SUCCESS;
  ObObjMeta cmp_meta;
  if (OB_ISNULL(left_expr) || OB_ISNULL(pred)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_expr), K(pred));
  } else if (T_OP_IN == pred->get_expr_type()) {
    //params of preds like 'A in (a,b,c...)' has been grouped by result types in pre-process phase,
    //for example: 'A in (int_a, int_b, float_a, float_b)' <=> A in (int_a, int_b) or A in (float_a, float_b)
    //so only check the cmp type of A and the first param of row_op
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(right_expr = pred->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(right_expr));
    } else if (T_OP_ROW != right_expr->get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the second param of in expr is not row_op", K(ret), K(right_expr->get_expr_type()));
    } else if (OB_ISNULL(right_expr = right_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(right_expr));
    } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(cmp_meta, left_expr->get_result_type(),right_expr->get_result_type()))) {
      LOG_WARN("failed to get equal meta", K(ret));
    } else {
      type_safe = (cmp_meta == cmp_type_);
    }
  } else if (T_OP_NE == pred->get_expr_type()) {
    if (OB_ISNULL(pred->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(pred->get_param_expr(1)));
    } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(cmp_meta, left_expr->get_result_type(),pred->get_param_expr(1)->get_result_type()))) {
      LOG_WARN("failed to get equal meta", K(ret));
    } else {
      type_safe = (cmp_meta == cmp_type_);
    }
  } else if (T_OP_BTW == pred->get_expr_type()) {
    if (OB_ISNULL(pred->get_param_expr(1)) || OB_ISNULL(pred->get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(pred->get_param_expr(1)), K(pred->get_param_expr(2)));
    } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(cmp_meta, left_expr->get_result_type(), pred->get_param_expr(1)->get_result_type()))) {
      LOG_WARN("failed to get equal meta", K(ret));
    } else if (cmp_meta != cmp_type_) {
    } else if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(cmp_meta, left_expr->get_result_type(), pred->get_param_expr(2)->get_result_type()))) {
      LOG_WARN("failed to get equal meta", K(ret));
    } else {
      type_safe = (cmp_meta == cmp_type_);
    }
  } else {
    type_safe = false;
  }
  return ret;
}

int ObPredicateDeduce::find_similar_expr(ObRawExpr *pred,
                                         ObIArray<ObRawExpr *> &general_preds,
                                         ObIArray<ObRawExpr *> &first_params)
{
  int ret = OB_SUCCESS;
  ObExprEqualCheckContext equal_ctx;
  equal_ctx.override_const_compare_ = true;
  if (OB_ISNULL(pred)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param expr", K(ret), K(pred));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < general_preds.count(); ++i) {
    bool is_similar = true;
    if (OB_ISNULL(general_preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("general predicate is null", K(ret));
    } else if (general_preds.at(i) == pred) {
      is_similar = true;
    } else if (T_OP_IN == pred->get_expr_type() &&
               T_OP_IN == general_preds.at(i)->get_expr_type()) {
      // deduce IN predicates for column parameters that only have basic tables and do not contain const equal predicates and IN predicates
      is_similar = true;
    } else if (general_preds.at(i)->get_expr_type() == pred->get_expr_type() &&
               general_preds.at(i)->get_param_count() == pred->get_param_count()) {
      for (int64_t j = 1; OB_SUCC(ret) && is_similar && j < pred->get_param_count(); ++j) {
        ObRawExpr *param1 = NULL;
        ObRawExpr *param2 = NULL;
        if (OB_ISNULL(param1 = pred->get_param_expr(j)) ||
            OB_ISNULL(param2 = general_preds.at(i)->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret));
        } else {
          is_similar = param1->same_as(*param2, &equal_ctx);
        }
      }
    }
    if (OB_SUCC(ret) && is_similar) {
      if (OB_FAIL(first_params.push_back(general_preds.at(i)->get_param_expr(0)))) {
        LOG_WARN("failed to push back param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::topo_sort(ObIArray<int64_t> &order)
{
  int ret = OB_SUCCESS;
  ObSEArray<bool, 8> visited;
  order.reuse();
  if (OB_FAIL(visited.prepare_allocate(N))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    visited.at(i) = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(topo_sort(i, visited, order))) {
      LOG_WARN("failed to topo sort", K(ret));
    }
  }
  return ret;
}

int ObPredicateDeduce::topo_sort(int64_t id,
                                 ObIArray<bool> &visited,
                                 ObIArray<int64_t> &order)
{
  int ret = OB_SUCCESS;
  if (!visited.at(id)) {
    visited.at(id) = true;
    for (int64_t i = 0; i < N; ++i) {
      if (has(graph_, id, i, GT) || has(graph_, id, i, GE)) {
        topo_sort(i, visited, order);
      }
    }
    if (OB_FAIL(order.push_back(id))) {
      LOG_WARN("failed to push back id", K(ret));
    }
  }
  return ret;
}

int ObPredicateDeduce::deduce_aggr_bound_predicates(ObTransformerCtx &ctx,
                                                    ObIArray<ObRawExpr *> &target_exprs,
                                                    ObIArray<ObRawExpr *> &aggr_bound_preds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *param_expr = NULL;
  bool is_valid = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < target_exprs.count(); ++i) {
    ObRawExpr *lower_expr = NULL;
    ObRawExpr *upper_expr = NULL;
    Type lower_type = EQ;
    Type upper_type = EQ;
    if (OB_ISNULL(target_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target expr is null", K(ret));
    } else if (OB_FAIL(check_aggr_validity(target_exprs.at(i), param_expr, is_valid))) {
      LOG_WARN("failed to check aggr validity", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(get_expr_bound(param_expr,
                                      lower_expr,
                                      lower_type,
                                      upper_expr,
                                      upper_type))) {
      LOG_WARN("failed to get expr bound", K(ret));
    }
    if (OB_SUCC(ret) && NULL != lower_expr && (lower_type == GT || lower_type == GE)) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_,
                                                        ctx.session_info_,
                                                        lower_type == GT ? T_OP_GT : T_OP_GE,
                                                        new_pred,
                                                        target_exprs.at(i),
                                                        lower_expr))) {
        LOG_WARN("failed to create compare expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(aggr_bound_preds.push_back(new_pred))) {
        LOG_WARN("failed to push back new predicate", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != upper_expr && (upper_type == GT || upper_type == GE)) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_,
                                                        ctx.session_info_,
                                                        upper_type == GT ? T_OP_GT : T_OP_GE,
                                                        new_pred,
                                                        upper_expr,
                                                        target_exprs.at(i)))) {
        LOG_WARN("failed to create compare expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(aggr_bound_preds.push_back(new_pred))) {
        LOG_WARN("failed to push back new predicate", K(ret));
      }
    }
  }
  return ret;
}

int ObPredicateDeduce::check_aggr_validity(ObRawExpr *expr,
                                           ObRawExpr *&param_expr,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->get_expr_type() == T_FUN_MAX ||
             expr->get_expr_type() == T_FUN_MIN) {
    if (OB_ISNULL(param_expr = expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else {
      is_valid = true;
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    ObObjMeta cmp_meta;
    if (OB_FAIL(ObRelationalExprOperator::get_equal_meta(
                  cmp_meta,
                  expr->get_result_type(),
                  param_expr->get_result_type()))) {
      LOG_WARN("failed to get equal meta", K(ret));
    } else {
      is_valid = (cmp_meta == cmp_type_);
    }
  }
  return ret;
}

int ObPredicateDeduce::get_expr_bound(ObRawExpr *target,
                                      ObRawExpr *&lower,
                                      Type &lower_type,
                                      ObRawExpr *&upper,
                                      Type &upper_type)
{
  int ret = OB_SUCCESS;
  int64_t target_idx = -1;
  bool check_lower = true;
  lower = upper = NULL;
  lower_type = upper_type = EQ;
  if (find_equal_expr(input_exprs_, target, &target_idx)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      int64_t expr_idx = topo_order_.at(i);
      bool has_gt = false;
      bool has_ge = false;
      if (target_idx == expr_idx) {
        check_lower = false;
      } else if (!is_const(expr_idx) || !is_type_safe(expr_idx, target_idx)) {
        // do nothing
      } else if (check_lower) {
        has_gt = has(graph_, target_idx, expr_idx, GT);
        has_ge = has(graph_, target_idx, expr_idx, GE);
        if (has_gt || has_ge) {
          lower = input_exprs_.at(expr_idx);
          lower_type = has_gt ? GT : GE;
        }
      } else {
        has_gt = has(graph_, expr_idx, target_idx, GT);
        has_ge = has(graph_, expr_idx, target_idx, GE);
        if (has_gt || has_ge) {
          upper = input_exprs_.at(expr_idx);
          upper_type = has_gt ? GT :GE;
          break;
        }
      }
    }
  }
  return ret;
}

// TODO (link.zt), try to remove the function
bool ObPredicateDeduce::find_equal_expr(const ObIArray<ObRawExpr *> &exprs,
                                        const ObRawExpr *target,
                                        int64_t *idx,
                                        ObExprParamCheckContext *context)
{
  bool bret = false;
  int64_t target_idx = -1;
  if (NULL != target) {
    for (int64_t i = 0; !bret && i < exprs.count(); ++i) {
      if (NULL != exprs.at(i)) {
        if (target == exprs.at(i)) {
          bret = true;
          target_idx = i;
        } else if (target->is_generalized_column()) {
          // do nothing
        } else if (target->same_as(*exprs.at(i), context)) {
          bret = true;
          target_idx = i;
        }
      }
    }
  }
  if (NULL != idx) {
    *idx = target_idx;
  }
  return bret;
}

bool ObPredicateDeduce::has_raw_const_equal_condition(int64_t param_idx)
{
  bool has_const_condition = false;
  for (int64_t i = 0; !has_const_condition && i < N; ++i) {
    if (!has(graph_, param_idx, i, EQ) || i == param_idx) {
      // do nothing
    } else if (is_raw_const(i)) {
      has_const_condition = true;
    }
  }
  return has_const_condition;
}

int ObPredicateDeduce::check_lossless_cast_table_filter(ObRawExpr *expr,
                                                        ObRawExpr *&cast_expr,
                                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  cast_expr = NULL;
  is_valid = true;
  ObRawExpr *check_expr = NULL;
  if (OB_ISNULL(expr) ||
      OB_UNLIKELY(expr->get_param_count() != 2) ||
      OB_ISNULL(left_expr = expr->get_param_expr(0)) ||
      OB_ISNULL(right_expr = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(left_expr), K(right_expr));
  } else if (expr->get_expr_type() != T_OP_EQ) {
    is_valid = false;
  } else if (left_expr->is_const_expr()) {
    check_expr = right_expr;
  } else if (right_expr->is_const_expr()) {
    check_expr = left_expr;
  } else {
    is_valid = false;
  }
  if (OB_SUCC(ret) && is_valid) {
    const ObRawExpr *column_expr = NULL;
    bool is_lossless = false;
    if (check_expr->get_expr_type() != T_FUN_SYS_CAST) {
      is_valid = false;
    } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(check_expr, column_expr))) {
      LOG_WARN("failed to get real expr without cast", K(ret));
    } else if (!column_expr->is_column_ref_expr()) {
      is_valid = false;
    } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(check_expr, is_lossless))) {
      LOG_WARN("failed to check expr is lossless column cast", K(ret));
    } else if (!is_lossless) {
      is_valid = false;
    } else {
      cast_expr = check_expr;
    }
  }
  return ret;
}

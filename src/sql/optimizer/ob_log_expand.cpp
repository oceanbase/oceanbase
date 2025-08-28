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

#include "ob_log_expand.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
namespace sql
{

int ObLogExpand::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_rollup_info_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null rollup info", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("get plan item info failed", K(ret));
  } else if (OB_UNLIKELY(hash_rollup_info_->expand_exprs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expand exprs", K(ret));
  } else {
    ObSEArray<ObRawExpr *, 8> uniq_rollup_exprs;
    if (OB_FAIL(append_array_no_dup(uniq_rollup_exprs, hash_rollup_info_->expand_exprs_))) {
      LOG_WARN("append array failed", K(ret));
    }
    BEGIN_BUF_PRINT;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF("repeat(%ld)[", hash_rollup_info_->expand_exprs_.count() + 1))) {
      LOG_WARN("printf failed", K(ret));
    } else {
      // output origin exprs
      OX(gen_duplicate_expr_text(plan_text, uniq_rollup_exprs));
      OX(BUF_PRINTF(", "))
      int nil_idx = uniq_rollup_exprs.count();
      for (int i = hash_rollup_info_->expand_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        bool exists_dup = false;
        for (int j = i - 1; OB_SUCC(ret) && !exists_dup && j >= 0; j--) {
          if (hash_rollup_info_->expand_exprs_.at(i) == hash_rollup_info_->expand_exprs_.at(j)) {
            // duplicate expr exists, output current duplicate exprs
            exists_dup = true;
          }
        }
        bool is_const_expr = hash_rollup_info_->expand_exprs_.at(i)->is_const_expr();
        bool is_real_static_const =
          (hash_rollup_info_->expand_exprs_.at(i)->get_expr_type() == T_FUN_SYS_REMOVE_CONST
          && hash_rollup_info_->expand_exprs_.at(i)->get_param_expr(0)->is_static_const_expr());
        if (exists_dup
            || is_const_expr
            || (lib::is_oracle_mode() && is_real_static_const)
            || has_exist_in_array(hash_rollup_info_->gby_exprs_, hash_rollup_info_->expand_exprs_.at(i))) {
          ret = gen_duplicate_expr_text(plan_text, uniq_rollup_exprs);
          if (!exists_dup) { nil_idx--; }
        } else {
          uniq_rollup_exprs.at(--nil_idx) = nullptr;
          ret = gen_duplicate_expr_text(plan_text, uniq_rollup_exprs);
        }
        if (i > 0) {
          OX(BUF_PRINTF(", "))
        }
      }
      OX(BUF_PRINTF("]"));
    }
    END_BUF_PRINT(plan_item.special_predicates_, plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogExpand::gen_duplicate_expr_text(PlanText &plan_text, ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  OX(BUF_PRINTF("("))
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (exprs.at(i) == nullptr) {
      OX(BUF_PRINTF("NULL"));
    } else if (OB_FAIL(exprs.at(i)->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("get expr name failed", K(ret));
    }
    if (OB_SUCC(ret) && i < exprs.count() - 1) {
      OX(BUF_PRINTF(", "))
    }
  }
  OX(BUF_PRINTF(")"));
  return ret;
}

int ObLogExpand::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  int64_t parallel = 0;
  if (OB_ISNULL(hash_rollup_info_) || OB_ISNULL(child) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null parameters", K(ret), K(child), K(get_plan()));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parallel degree", K(ret), K(parallel));
  } else if (OB_UNLIKELY(hash_rollup_info_->expand_exprs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expand exprs", K(ret), K(hash_rollup_info_->expand_exprs_));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double op_cost = ObOptEstCost::cost_get_rows(child->get_card() / parallel, opt_ctx);
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card(child->get_card() * (hash_rollup_info_->expand_exprs_.count() + 1));
  }
  return ret;
}

int ObLogExpand::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = param.need_parallel_;
  double child_card = 0.0;
  double child_cost = 0.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || OB_ISNULL(hash_rollup_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null parameters", K(ret), K(child), K(get_plan()));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected parallel degree", K(ret), K(parallel));
  } else if (OB_UNLIKELY(hash_rollup_info_->expand_exprs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expand exprs", K(ret), K(hash_rollup_info_->expand_exprs_));
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("re-estimate cost failed", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost = ObOptEstCost::cost_get_rows(child_card / parallel, opt_ctx);
    card = child_card * (hash_rollup_info_->expand_exprs_.count() + 1);
    cost = op_cost + child_cost;
  }
  return ret;
}

int ObLogExpand::get_op_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_rollup_info_) || OB_ISNULL(hash_rollup_info_->rollup_grouping_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping id expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(hash_rollup_info_->rollup_grouping_id_))) {
    LOG_WARN("append element failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, hash_rollup_info_->expand_exprs_))) {
    LOG_WARN("append elements failed", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < hash_rollup_info_->dup_expr_pairs_.count(); i++) {
    if (OB_FAIL(all_exprs.push_back(hash_rollup_info_->dup_expr_pairs_.at(i).element<1>()))) {
      LOG_WARN("push back element failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append(all_exprs, hash_rollup_info_->gby_exprs_))) {
    LOG_WARN("append array failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("get op exprs failed", K(ret));
  }
  return ret;
}

int ObLogExpand::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_rollup_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null rollup info", K(ret));
  } else {
    is_fixed = (expr == hash_rollup_info_->rollup_grouping_id_);
    for (int i = 0; !is_fixed && i < hash_rollup_info_->expand_exprs_.count(); i++) {
      is_fixed = (expr == hash_rollup_info_->expand_exprs_.at(i));
    }
    for (int i = 0; !is_fixed && i < hash_rollup_info_->dup_expr_pairs_.count(); i++) {
      is_fixed = (expr == hash_rollup_info_->dup_expr_pairs_.at(i).element<1>());
    }
  }
  return ret;
}

int ObLogExpand::dup_and_replace_exprs_within_aggrs(ObRawExprFactory &factory,
                                                    ObSQLSessionInfo *sess,
                                                    ObIArray<ObExprConstraint> &constraints,
                                                    const ObIArray<ObRawExpr *> &rollup_exprs,
                                                    const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                                    ObIArray<DupRawExprPair> &dup_expr_pairs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> uniq_expand_exprs;
  ObRawExprCopier copier(factory);
  if (OB_UNLIKELY(rollup_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expand exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(uniq_expand_exprs, rollup_exprs))) {
    LOG_WARN("append elements failed", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < uniq_expand_exprs.count(); i++) {
    bool found = false;
    for (int j = 0; OB_SUCC(ret) && !found && j < aggr_items.count(); j++) {
      if (aggr_items.at(j)->get_expr_type() == T_FUN_GROUPING
                 || aggr_items.at(j)->get_expr_type() == T_FUN_GROUPING_ID) {
        // do nothing
      } else if (OB_FAIL(find_expr_within_aggr_item(aggr_items.at(j), uniq_expand_exprs.at(i), found))) {
        LOG_WARN("found expr failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (found) {
      ObRawExpr *dup_expr = nullptr;
      ObRawExpr *new_const_expr = nullptr;
      if (OB_FAIL(ObRawExprUtils::build_dup_data_expr(factory, uniq_expand_exprs.at(i), dup_expr))) {
        LOG_WARN("build duplicate expr failed", K(ret));
      } else if (OB_FAIL(dup_expr->formalize(sess))) {
        LOG_WARN("formalize expr failed", K(ret));
      } else if (OB_FAIL(dup_expr_pairs.push_back(DupRawExprPair(uniq_expand_exprs.at(i), dup_expr)))) {
        LOG_WARN("push back element failed", K(ret));
      } else if (!uniq_expand_exprs.at(i)->is_static_scalar_const_expr()) {
      } else if (OB_FAIL(copier.copy(uniq_expand_exprs.at(i), new_const_expr))) {
        LOG_WARN("copy expr failed", K(ret));
      }
    }
  }
  // before replacing rollup exprs in place, it is necessary to unshare the related expr constraints to avoid incorrect replacements.
  if (OB_SUCC(ret) && OB_FAIL(ObLogExpand::unshare_constraints(copier, constraints))) {
    LOG_WARN("unshare constraints failed", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < dup_expr_pairs.count(); i++) {
    for (int j = 0; OB_SUCC(ret) && j < aggr_items.count(); j++) {
      ObAggFunRawExpr *aggr_item = aggr_items.at(j);
      if (OB_ISNULL(aggr_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null aggr item", K(ret));
      } else if (aggr_item->get_expr_type() == T_FUN_GROUPING
                 || aggr_item->get_expr_type() == T_FUN_GROUPING_ID) {
      } else if (OB_FAIL(replace_expr_with_aggr_item(aggr_item, dup_expr_pairs.at(i).element<0>(),
                                                     dup_expr_pairs.at(i).element<1>()))) {
        LOG_WARN("replace expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogExpand::find_expr(ObRawExpr *root, const ObRawExpr *expected, bool &found)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
  } else if (root == expected) {
    found = true;
  } else {
    for (int i = 0; OB_SUCC(ret) && !found && i < root->get_param_count(); i++) {
      ret = SMART_CALL(find_expr(root->get_param_expr(i), expected, found));
    }
  }
  return ret;
}

int ObLogExpand::find_expr_within_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *expected, bool &found)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr *> &real_param_exprs = aggr_item->get_real_param_exprs();
  for (int i = 0; OB_SUCC(ret) && !found && i < real_param_exprs.count(); i++) {
    if (aggr_item->get_expr_type() == T_FUN_GROUP_CONCAT
        && lib::is_oracle_mode()
        && i == real_param_exprs.count() - 1) {
      // do not replace seperator expr with duplicated expr
      // do nothing
    } else {
      ret = find_expr(real_param_exprs.at(i), expected, found);
    }
  }
  // listagg(c1, c3) within group (order by c3) from t1 group by rollup(c3);
  // in rollup process, c3 is set to null, thus order is changed, so as result
  // for result compatibility, we replace order item with dup(c3), i.e.
  //  listagg(c1, c3) within group (order by dup(c3)) from t1 group by rollup(c3);
  for (int i = 0; OB_SUCC(ret) && !found && i < aggr_item->get_order_items().count(); i++) {
    ret = find_expr(aggr_item->get_order_items().at(i).expr_, expected, found);
  }
  return ret;
}

int ObLogExpand::replace_expr(ObRawExpr *&root, const ObRawExpr *from, ObRawExpr *to)
{
  int ret = OB_SUCCESS;
  if (root == from) { root = to; }
  for (int i = 0; OB_SUCC(ret) && i < root->get_param_count(); i++) {
    ret = SMART_CALL(replace_expr(root->get_param_expr(i), from, to));
  }
  return ret;
}

int ObLogExpand::replace_expr_with_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *from, ObRawExpr *to)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &real_param_exprs = aggr_item->get_real_param_exprs_for_update();
  for (int i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); i++) {
    if (aggr_item->get_expr_type() == T_FUN_GROUP_CONCAT
        && lib::is_oracle_mode()
        && i == real_param_exprs.count() - 1) {
      // do not replace seperator expr with duplicated expr
      // do nothing
    } else {
      ret = replace_expr(real_param_exprs.at(i), from, to);
    }
  }
  ObIArray<OrderItem> &order_items = aggr_item->get_order_items_for_update();
  for (int i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    ret = replace_expr(order_items.at(i).expr_, from, to);
  }
  return ret;
}

int ObLogExpand::gen_expand_exprs(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                                  ObIArray<ObExprConstraint> &constraints,
                                  ObIArray<ObRawExpr *> &rollup_exprs,
                                  ObIArray<ObRawExpr *> &gby_exprs,
                                  ObIArray<DupRawExprPair> &dup_expr_pairs)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(factory);
  int64_t origin_size = dup_expr_pairs.count();
  if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null arguments", K(ret));
  } else if (lib::is_oracle_mode()) {
    // `sum(b) from t group by a, b, rollup(b)`
    // result of rollup b is same as `sum(b) from t group by a, b`
    //
    // `group by a, length(b), rollup(b)`
    // result of rollup b is same as `sum(NULL) from t group by a, length(dup(b))`
    for (int i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); i++) {
      if (has_exist_in_array(gby_exprs, rollup_exprs.at(i))) { // do nothing
      } else {
        bool found = false;
        ObRawExpr *rollup_expr = rollup_exprs.at(i);
        ObRawExpr *new_const_expr = nullptr;
        for (int j = 0; !found && OB_SUCC(ret) && j < gby_exprs.count(); j++) {
          if (OB_FAIL(find_expr(gby_exprs.at(j), rollup_expr, found))) {
            LOG_WARN("find expr failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && found) {
          ObRawExpr *new_expr = nullptr;
          if (OB_FAIL(ObRawExprUtils::build_dup_data_expr(factory, rollup_expr, new_expr))) {
            LOG_WARN("build dup data expr failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid null expr", K(ret));
          } else if (OB_FAIL(new_expr->formalize(sess))) {
            LOG_WARN("formalize failed", K(ret));
          } else if (OB_FAIL(dup_expr_pairs.push_back(DupRawExprPair(rollup_expr, new_expr)))) {
            LOG_WARN("push back element failed", K(ret));
          } else if (!rollup_expr->is_static_scalar_const_expr()) {
          } else if (OB_FAIL(copier.copy(rollup_expr, new_const_expr))) {
            LOG_WARN("copy expr failed", K(ret));
          }
        }
      }
    }
    // before replacing rollup exprs in place, it is necessary to unshare the related expr constraints to avoid incorrect replacements.
    if (OB_SUCC(ret) && OB_FAIL(ObLogExpand::unshare_constraints(copier, constraints))) {
      LOG_WARN("unshare constraints failed", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < gby_exprs.count(); i++) {
      for (int j = origin_size; OB_SUCC(ret) && j < dup_expr_pairs.count(); j++) {
        if (OB_FAIL(replace_expr(gby_exprs.at(i), 
                                dup_expr_pairs.at(j).element<0>(), 
                                dup_expr_pairs.at(j).element<1>()))) {
          LOG_WARN("replace expr failed", K(ret));
        }
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLogExpand::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(hash_rollup_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, hash_rollup_info_->gby_exprs_))) {
    LOG_WARN("replace exprs failed", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, hash_rollup_info_->expand_exprs_))) {
    LOG_WARN("replace exprs failed", K(ret));
  }
  constexpr int name_buf_len = 128;
  char name_buf[name_buf_len] = {0};
  char * replaced_name = nullptr;
  int64_t pos = 0;
  for (int i = 0; OB_SUCC(ret) && i < hash_rollup_info_->dup_expr_pairs_.count(); i++) {
    ObRawExpr *&expr = hash_rollup_info_->dup_expr_pairs_.at(i).element<0>();
    ObRawExpr *dup_expr = hash_rollup_info_->dup_expr_pairs_.at(i).element<1>();
    pos = 0;
    if (OB_FAIL(replace_expr_action(replacer, expr))) {
      LOG_WARN("replace expr failed", K(ret));
    } else if (OB_FAIL(databuff_printf(name_buf, name_buf_len, pos, "dup("))) {
      LOG_WARN("buf printf failed", K(ret));
    } else if (OB_FAIL(expr->get_name(name_buf, name_buf_len, pos))) {
      LOG_WARN("get expr name failed", K(ret));
    } else {
      ObIAllocator &allocator = get_plan()->get_optimizer_context().get_expr_factory().get_allocator();
      if (OB_ISNULL(replaced_name = (char *)allocator.alloc(pos + 2))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(replaced_name, name_buf, pos);
        replaced_name[pos] = ')';
        replaced_name[pos + 1] = '\0';
        static_cast<ObOpPseudoColumnRawExpr *>(dup_expr)->set_name(replaced_name);
      }
    }
  }
  return ret;
}

int ObLogExpand::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(0)) || OB_ISNULL(hash_rollup_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null child", K(ret));
  } else {
    ObIArray<ObRawExpr *> &child_const_exprs = child->get_output_const_exprs();
    ObIArray<ObRawExpr *> &output_const_exprs = get_output_const_exprs();
    for (int i = 0; OB_SUCC(ret) && i < child_const_exprs.count(); i++) {
      if (has_exist_in_array(hash_rollup_info_->expand_exprs_, child_const_exprs.at(i))) {
      } else if (OB_FAIL(output_const_exprs.push_back(child_const_exprs.at(i)))) {
        LOG_WARN("push back element failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_filter_exprs(), output_const_exprs))) {
      LOG_WARN("compute const exprs failed", K(ret));
    }
  }
  return ret;
}

int ObLogExpand::compute_equal_set()
{
  int ret = OB_SUCCESS;
  EqualSets *ordering_esets = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null plan", K(ret));
  } else if (filter_exprs_.empty()) {
    set_output_equal_sets(&empty_expr_sets_);
  } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&get_plan()->get_allocator(), filter_exprs_,
                                                        *ordering_esets))) {
    LOG_WARN("failed to compute ordering output equal set", K(ret));
  } else {
    set_output_equal_sets(ordering_esets);
  }
  return ret;
}

int ObLogExpand::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObFdItemSet *fd_item_set = NULL;
  set_fd_item_set(fd_item_set);
  return ret;
}

int ObLogExpand::compute_one_row_info()
{
  set_is_at_most_one_row(false);
  return OB_SUCCESS;
}

int ObLogExpand::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  return ret;
}

int ObLogExpand::unshare_constraints(ObRawExprCopier &copier, ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraints.count(); ++i) {
    ObExprConstraint &constraint = constraints.at(i);
    if (OB_FAIL(copier.copy_on_replace(constraint.pre_calc_expr_, constraint.pre_calc_expr_))) {
      LOG_WARN("failed to unshare expr", K(ret));
    }
  }
  return ret;
}
} // end sql
} // end oceanbase
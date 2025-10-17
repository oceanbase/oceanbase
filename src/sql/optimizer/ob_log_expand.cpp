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

#define DUP_EXPR_IDX 1
#define ORG_EXPR_IDX 0

int ObLogExpand::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(grouping_set_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping set info", K(ret));
  } else {
    if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
      LOG_WARN("get plan item info failed", K(ret));
    } else {
      BEGIN_BUF_PRINT;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF("repeat(%ld)[", grouping_set_info_->groupset_exprs_.count()))) {
        LOG_WARN("printf failed", K(ret));
      }
      for (int i = grouping_set_info_->groupset_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        if (OB_FAIL(gen_grouping_set_text(plan_text, i))) {
          LOG_WARN("gen nth group text failed", K(ret));
        }
        if (OB_SUCC(ret) && i > 0) {
          OX(BUF_PRINTF(","));
        }
      }
      OX(BUF_PRINTF("]"));
      if (OB_SUCC(ret) && grouping_set_info_->pruned_groupset_exprs_.count() > 0) {
        OX(BUF_PRINTF(", pruned(%ld)[", grouping_set_info_->pruned_groupset_exprs_.count()));
        for (int i = 0; OB_SUCC(ret) && i < grouping_set_info_->pruned_groupset_exprs_.count(); i++) {
          if (OB_FAIL(gen_pruned_grouping_set_text(
                plan_text, grouping_set_info_->pruned_groupset_exprs_.at(i).groupby_exprs_))) {
            LOG_WARN("gen pruned grouping sets failed", K(ret));
          } else if (i < grouping_set_info_->pruned_groupset_exprs_.count() - 1) {
            BUF_PRINTF(",");
          }
        }
        OX(BUF_PRINTF("]"));
      }
      END_BUF_PRINT(plan_item.special_predicates_, plan_item.special_predicates_len_);
    }
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

int ObLogExpand::gen_grouping_set_text(PlanText &plan_text, const int64_t n_group)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  if (OB_ISNULL(grouping_set_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping set info", K(ret));
  } else if (OB_UNLIKELY(n_group >= grouping_set_info_->groupset_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group idx", K(ret), K(n_group), K(grouping_set_info_->groupset_exprs_.count()));
  } else {
    BUF_PRINTF("(");
    ObIArray<ObRawExpr *> &group_exprs = grouping_set_info_->groupset_exprs_.at(n_group).groupby_exprs_;
    for (int i = 0; OB_SUCC(ret) && i < grouping_set_info_->group_exprs_.count(); i++) {
      ObRawExpr *gby_expr = grouping_set_info_->group_exprs_.at(i);
      if (OB_ISNULL(gby_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null expr", K(ret));
      } else if (has_exist_in_array(group_exprs, gby_expr)) {
        if (OB_FAIL(gby_expr->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("get expr name failed", K(ret));
        }
      } else if (OB_FAIL(BUF_PRINTF("NULL"))) {
        LOG_WARN("buf printf failed", K(ret));
      }
      if (OB_SUCC(ret) && i < grouping_set_info_->group_exprs_.count() - 1) {
        OX(BUF_PRINTF(","))
      }
    }
    OX(BUF_PRINTF(")"));
  }
  return ret;
}

int ObLogExpand::gen_pruned_grouping_set_text(PlanText &plan_text, ObIArray<ObRawExpr *> &pruned_gby_exprs)
{
  int ret = OB_SUCCESS;
  BEGIN_BUF_PRINT;
  BUF_PRINTF("(");
  for (int i = 0; OB_SUCC(ret) && i < pruned_gby_exprs.count(); i++) {
    ObRawExpr *expr = pruned_gby_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null expr", K(ret));
    } else if (OB_FAIL(expr->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("get expr name failed", K(ret));
    } else if (i < pruned_gby_exprs.count() - 1) {
      BUF_PRINTF(",");
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
  int64_t dup_cnt = 0;
  if (OB_ISNULL(grouping_set_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping set info", K(ret));
  } else if (OB_ISNULL(child) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null parameters", K(ret), K(child), K(get_plan()));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parallel degree", K(ret), K(parallel));
  } else if (grouping_set_info_ != NULL) {
    dup_cnt = grouping_set_info_->groupset_exprs_.count();
  }
  if (OB_FAIL(ret)) {
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double op_cost = ObOptEstCost::cost_get_rows(child->get_card() / parallel, opt_ctx);
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card(child->get_card() * dup_cnt);
  }
  return ret;
}

int ObLogExpand::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = param.need_parallel_;
  double child_card = 0.0;
  double child_cost = 0.0;
  int64_t dup_cnt = 0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(grouping_set_info_) || OB_ISNULL(child) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null parameters", K(ret), K(child), K(get_plan()));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected parallel degree", K(ret), K(parallel));
  } else if (grouping_set_info_ != NULL) {
    dup_cnt = grouping_set_info_->groupset_exprs_.count();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("re-estimate cost failed", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost = ObOptEstCost::cost_get_rows(child_card / parallel, opt_ctx);
    card = child_card * dup_cnt;
    cost = op_cost + child_cost;
  }
  return ret;
}

int ObLogExpand::get_op_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(grouping_set_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping set info", K(ret));
  } else if (grouping_set_info_ != NULL) {
    if (OB_ISNULL(grouping_set_info_->grouping_set_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null grouping id expr", K(ret));
    } else if (OB_FAIL(all_exprs.push_back(grouping_set_info_->grouping_set_id_))) {
      LOG_WARN("push back element faield", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_exprs, grouping_set_info_->group_exprs_))) {
      LOG_WARN("append array no dup failed", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < grouping_set_info_->dup_expr_pairs_.count(); i++) {
      if (OB_FAIL(all_exprs.push_back(grouping_set_info_->dup_expr_pairs_.at(i).element<ORG_EXPR_IDX>()))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(grouping_set_info_->dup_expr_pairs_.at(i).element<DUP_EXPR_IDX>()))) {
        LOG_WARN("push back element failed", K(ret));
      }
    }
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
  if (OB_ISNULL(grouping_set_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null rollup info", K(ret));
  } else {
    is_fixed = (expr == grouping_set_info_->grouping_set_id_);
    bool is_common_gby = has_exist_in_array(grouping_set_info_->common_group_exprs_, const_cast<ObRawExpr *>(expr));
    for (int i = 0; !is_fixed && i < grouping_set_info_->group_exprs_.count(); i++) {
      is_fixed = (expr == grouping_set_info_->group_exprs_.at(i) && !is_common_gby);
    }
    for (int i = 0; !is_fixed && i < grouping_set_info_->dup_expr_pairs_.count(); i++) {
      is_fixed = (expr == grouping_set_info_->dup_expr_pairs_.at(i).element<DUP_EXPR_IDX>());
    }
  }
  return ret;
}

int ObLogExpand::dup_and_replace_exprs_within_aggrs(ObRawExprFactory &factory,
                                                    ObSQLSessionInfo *sess,
                                                    ObIArray<ObExprConstraint> &constraints,
                                                    const ObIArray<ObRawExpr *> &rollup_exprs,
                                                    const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                                    ObIArray<ObAggFunRawExpr *> &new_agg_items,
                                                    ObIArray<DupRawExprPair> &dup_expr_pairs,
                                                    ObIArray<DupRawExprPair> &replaced_aggr_pairs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> uniq_expand_exprs;
  ObRawExprCopier copier(factory);
  ObAggFunRawExpr *new_agg = nullptr;
  ObSqlBitSet<> replace_aggr_idx;
  if (OB_UNLIKELY(rollup_exprs.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expand exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(uniq_expand_exprs, rollup_exprs))) {
    LOG_WARN("append elements failed", K(ret));
  }
  for(int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); i++) {
    if (replace_aggr_idx.has_member(i)
        || aggr_items.at(i)->get_expr_type() == T_FUN_GROUPING
        || aggr_items.at(i)->get_expr_type() == T_FUN_GROUPING_ID) {
      continue;
    }
    bool found = false;
    for (int j = 0; OB_SUCC(ret) && !found && j < uniq_expand_exprs.count(); j++) {
     if (OB_FAIL(find_expr_within_aggr_item(aggr_items.at(i), uniq_expand_exprs.at(j), found))) {
        LOG_WARN("found expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && found) {
      ret = replace_aggr_idx.add_member(i);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(new_agg_items.assign(aggr_items))) {
    LOG_WARN("assign array failed", K(ret));
  }
  for(int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); i++) {
    new_agg = nullptr;
    if (!replace_aggr_idx.has_member(i)) { continue; }
    if (OB_FAIL(factory.create_raw_expr(aggr_items.at(i)->get_expr_type(), new_agg))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(create_aggr_with_dup_params(factory, aggr_items.at(i), sess, dup_expr_pairs, new_agg))) {
      LOG_WARN("create aggr failed", K(ret));
    } else if (OB_FAIL(replaced_aggr_pairs.push_back(DupRawExprPair(aggr_items.at(i), new_agg)))) {
      LOG_WARN("push back failed", K(ret));
    } else {
      // const_cast<ObIArray<ObAggFunRawExpr *> &>(aggr_items).at(i) = new_agg;
      new_agg_items.at(i) = new_agg;
    }
  }
  LOG_TRACE("replace agg items", K(ret), K(replaced_aggr_pairs), K(aggr_items));
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
      ret = ObRawExprUtils::find_expr(real_param_exprs.at(i), expected, found);
    }
  }
  // listagg(c1, c3) within group (order by c3) from t1 group by rollup(c3);
  // in rollup process, c3 is set to null, thus order is changed, so as result
  // for result compatibility, we replace order item with dup(c3), i.e.
  //  listagg(c1, c3) within group (order by dup(c3)) from t1 group by rollup(c3);
  for (int i = 0; OB_SUCC(ret) && !found && i < aggr_item->get_order_items().count(); i++) {
    ret = ObRawExprUtils::find_expr(aggr_item->get_order_items().at(i).expr_, expected, found);
  }
  return ret;
}

int ObLogExpand::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (grouping_set_info_ != NULL) {
    if (OB_FAIL(replace_exprs_action(replacer, grouping_set_info_->group_exprs_))) {
      LOG_WARN("replace exprs failed", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < grouping_set_info_->groupset_exprs_.count(); i++) {
      if (OB_FAIL(replace_exprs_action(replacer, grouping_set_info_->groupset_exprs_.at(i).groupby_exprs_))) {
        LOG_WARN("replace exprs action failed", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < grouping_set_info_->pruned_groupset_exprs_.count(); i++) {
      if (OB_FAIL(replace_exprs_action(replacer, grouping_set_info_->pruned_groupset_exprs_.at(i).groupby_exprs_))) {
        LOG_WARN("replace exprs action failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObIArray<DupRawExprPair> *dup_expr_pairs = &grouping_set_info_->dup_expr_pairs_;
    constexpr int name_buf_len = 128;
    char name_buf[name_buf_len] = {0};
    char *replaced_name = nullptr;
    int64_t pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < dup_expr_pairs->count(); i++) {
      ObRawExpr *&expr = dup_expr_pairs->at(i).element<ORG_EXPR_IDX>();
      ObRawExpr *dup_expr = dup_expr_pairs->at(i).element<DUP_EXPR_IDX>();
      pos = 0;
      if (OB_FAIL(replace_expr_action(replacer, expr))) {
        LOG_WARN("replace expr failed", K(ret));
      } else if (OB_FAIL(databuff_printf(name_buf, name_buf_len, pos, "dup("))) {
        LOG_WARN("buf printf failed", K(ret));
      } else if (OB_FAIL(expr->get_name(name_buf, name_buf_len, pos))) {
        LOG_WARN("get expr name failed", K(ret));
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;// if name exceeds, just truncate name and return
        }
      }
      if (OB_SUCC(ret)) {
      } else {
        ObIAllocator &allocator =
          get_plan()->get_optimizer_context().get_expr_factory().get_allocator();
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
  }
  return ret;
}

int ObLogExpand::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  ObIArray<ObRawExpr *> &output_const_exprs = get_output_const_exprs();
  if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null child", K(ret));
  } else if (grouping_set_info_ != NULL) {
    ObIArray<ObRawExpr *> &child_const_exprs = child->get_output_const_exprs();
    ObIArray<ObRawExpr *> &output_const_exprs = get_output_const_exprs();
    for (int i = 0; OB_SUCC(ret) && i < child_const_exprs.count(); i++) {
      if (has_exist_in_array(grouping_set_info_->group_exprs_, child_const_exprs.at(i))
          && !has_exist_in_array(grouping_set_info_->common_group_exprs_, child_const_exprs.at(i))) {
        // do nothing
      } else if (OB_FAIL(output_const_exprs.push_back(child_const_exprs.at(i)))) {
        LOG_WARN("push back element failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_filter_exprs(), output_const_exprs))) {
    LOG_WARN("compute const exprs failed", K(ret));
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

int ObLogExpand::create_aggr_with_dup_params(ObRawExprFactory &factory, ObAggFunRawExpr *aggr_item,
                                             ObSQLSessionInfo *sess,
                                             ObIArray<DupRawExprPair> &dup_expr_pairs,
                                             ObAggFunRawExpr *&new_agg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(factory.create_raw_expr(aggr_item->get_expr_type(), new_agg))) {
    LOG_WARN("create expr failed", K(ret));
  } else if (OB_ISNULL(new_agg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null raw expr", K(ret));
  } else if (OB_FAIL(new_agg->assign(*aggr_item))) {
    LOG_WARN("assign expr failed", K(ret));
  } else {
    ObIArray<ObRawExpr *> &real_param_exprs = new_agg->get_real_param_exprs_for_update();
    for (int i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); i++) {
      ObRawExpr *dup_expr = nullptr;
      if (aggr_item->get_expr_type() == T_FUN_GROUP_CONCAT
          && lib::is_oracle_mode()
          && i == real_param_exprs.count() - 1) {
        // do not replace seperator expr with duplicated expr
        // do nothing
        continue;
      }
      if (real_param_exprs.at(i)->is_const_expr()) {
        continue;
      }
      if (OB_FAIL(build_dup_expr(factory, sess, real_param_exprs.at(i), dup_expr_pairs, dup_expr))) {
      } else if (OB_ISNULL(dup_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null dup expr", K(ret));
      } else {
        real_param_exprs.at(i) = dup_expr;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_agg->get_order_items().count(); i++) {
      ObRawExpr *expr = new_agg->get_order_items().at(i).expr_;
      ObRawExpr *dup_expr = nullptr;
      if (expr->is_const_expr()) {// do nothing
      } else if (OB_FAIL(build_dup_expr(factory, sess, expr, dup_expr_pairs, dup_expr))) {
        LOG_WARN("build dup expr failed", K(ret));
      } else if (OB_ISNULL(dup_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null dup expr", K(ret));
      } else {
        new_agg->get_order_items_for_update().at(i).expr_ = dup_expr;
      }
    }
  }
  return ret;
}

int ObLogExpand::build_dup_expr(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                                ObRawExpr *input, ObIArray<DupRawExprPair> &dup_expr_pairs,
                                ObRawExpr *&output)
{
  int ret = OB_SUCCESS;
  output = nullptr;
  for (int64_t j = 0; OB_SUCC(ret) && output == nullptr && j < dup_expr_pairs.count(); j++) {
    if (input == dup_expr_pairs.at(j).element<ORG_EXPR_IDX>()) {
      output = dup_expr_pairs.at(j).element<DUP_EXPR_IDX>();
    }
  }
  if (output != nullptr) {
  } else if (OB_FAIL(ObRawExprUtils::build_dup_data_expr(factory, input, output))) {
    LOG_WARN("build duplicate expr failed", K(ret));
  } else if (OB_FAIL(output->formalize(sess))) {
    LOG_WARN("formalize failed", K(ret));
  } else if (OB_FAIL(dup_expr_pairs.push_back(DupRawExprPair(input, output)))) {
    LOG_WARN("push back failed", K(ret));
  }
  return ret;
}
} // end sql
} // end oceanbase
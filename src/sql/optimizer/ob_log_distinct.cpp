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
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"
#include "ob_optimizer_util.h"
#include "ob_opt_selectivity.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const char *ObLogDistinct::get_name() const
{
  return MERGE_AGGREGATE == algo_ ? "MERGE DISTINCT" : "HASH DISTINCT";
}

int ObLogDistinct::get_plan_item_info(PlanText &plan_text,
                                      ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*> &distinct = distinct_exprs_;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get base plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_EXPRS(distinct, type);
    if (OB_SUCC(ret) && is_block_mode_) {
      ret = BUF_PRINTF(", block");
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}


int ObLogDistinct::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(replace_exprs_action(replacer, distinct_exprs_))) {
    SQL_OPT_LOG(WARN, "failed to replace agg exprs in distinct op", K(ret));
  }
  return ret;
}

int ObLogDistinct::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (algo_ == HASH_AGGREGATE) {
    // do nothing
  } else if (OB_FAIL(ObLogicalOperator::compute_op_ordering())) {
    LOG_WARN("failed to compute op ordering", K(ret));
  } else {
    is_local_order_ = is_fully_partition_wise() && !get_op_ordering().empty() && !is_range_order_;
  }
  return ret;
}

int ObLogDistinct::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(get_distinct_output_exprs(output_exprs))) {
    LOG_WARN("failed to get distinct output exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output distinct exprs", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for distinct", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogDistinct::get_distinct_output_exprs(ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObSEArray<ObRawExpr*, 16> candi_exprs;
  ObSEArray<ObRawExpr*, 16> extracted_col_aggr_winfunc_exprs;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_orderby_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));  
  } else if (OB_FAIL(ObRawExprUtils::extract_col_aggr_winfunc_exprs(candi_exprs,
                                                                    extracted_col_aggr_winfunc_exprs,
                                                                    true))) {
  } else if (OB_FAIL(append_array_no_dup(output_exprs, extracted_col_aggr_winfunc_exprs))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogDistinct::est_cost()
{
  int ret = OB_SUCCESS;
  double distinct_cost = 0.0;
  ObLogicalOperator *child = NULL;
  double child_ndv = total_ndv_;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  double child_card = 0;
  double child_cost = 0;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY(child_ndv < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected total ndv", K(child_ndv), K(ret));
  } else if (OB_FAIL(inner_est_cost(get_parallel(), child->get_card(), child_ndv, distinct_cost))) {
    LOG_WARN("failed to est distinct cost", K(ret));
  } else if (need_re_est_child_cost() &&
             OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("failed to re est child cost", K(ret));
  } else if (!need_re_est_child_cost() &&
             OB_FALSE_IT(child_cost=child->get_cost())) {
  } else {
    set_op_cost(distinct_cost);
    set_cost(child_cost + distinct_cost);
    set_card(child_ndv);
  }
  return ret;
}

int ObLogDistinct::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child", K(ret));
  } else if (OB_UNLIKELY(total_ndv_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected total ndv", K(total_ndv_), K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    double child_ndv = total_ndv_;
    const int64_t parallel = param.need_parallel_;
    double origin_child_card = child_card;
    bool need_scale_ndv = false;
    if (param.need_row_count_ >= 0 && 
        child_card > 0 &&
        total_ndv_ > 0 &&
        param.need_row_count_ < child_card &&
        param.need_row_count_ < total_ndv_) {
      child_ndv = param.need_row_count_;
      if (input_sorted_) {
        if (param.need_row_count_ > 1.0) {
          param.need_row_count_ = child_card * (param.need_row_count_ - 1.0) / total_ndv_ + 1.0;
        } else {
          // do nothing
        }
      } else {
        param.need_row_count_ = child_card * (1 - std::pow((1 - child_ndv / total_ndv_), total_ndv_ / child_card));
      }
    } else {
      param.need_row_count_ = -1;
      need_scale_ndv = true;
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est child cost", K(ret));
    } else if (need_scale_ndv &&
               FALSE_IT(child_ndv = std::min(child_ndv, ObOptSelectivity::scale_distinct(child_card, origin_child_card, child_ndv)))) {
      // do nothing
    } else if (OB_FAIL(inner_est_cost(parallel, child_card, child_ndv, op_cost))) {
      LOG_WARN("failed to est distinct cost", K(ret));
    } else {
      cost = child_cost + op_cost;
      card = child_ndv;
      if (param.override_) {
        total_ndv_ = child_ndv;
      }
    }
  }
  return ret;
}

int ObLogDistinct::inner_est_cost(const int64_t parallel, double child_card, double &child_ndv, double &op_cost)
{
  int ret = OB_SUCCESS;
  double per_dop_card = 0.0;
  double per_dop_ndv = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel", K(parallel), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    per_dop_card = child_card / parallel;
    if (parallel > 1) {
      if (is_push_down()) {
        per_dop_ndv = ObOptSelectivity::scale_distinct(per_dop_card, child_card, child_ndv);
      } else {
        per_dop_ndv = child_ndv / parallel;
      }
    } else {
      per_dop_ndv = child_ndv;
    }

    if (MERGE_AGGREGATE == algo_) {
      op_cost = ObOptEstCost::cost_merge_distinct(per_dop_card,
                                                  per_dop_ndv,
                                                  child->get_width(),
                                                  distinct_exprs_,
                                                  opt_ctx);
    } else {
      op_cost = ObOptEstCost::cost_hash_distinct(per_dop_card,
                                                per_dop_ndv,
                                                child->get_width(),
                                                distinct_exprs_,
                                                opt_ctx);
    }

    if (opt_ctx.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                          COMPAT_VERSION_4_3_3)) {
      child_ndv = std::min(child_card, per_dop_ndv * parallel);
    }
  }
  return ret;
}

uint64_t ObLogDistinct::hash(uint64_t seed) const
{
  seed = do_hash(algo_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogDistinct::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *child = NULL;
  ObFdItemSet *fd_item_set = NULL;
  ObTableFdItem *fd_item = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else if (OB_FAIL(fd_item_set->assign(child->get_fd_item_set()))) {
    LOG_WARN("failed to assign fd item set", K(ret));
  } else if (!ObTransformUtils::need_compute_fd_item_set(distinct_exprs_)) {
    //do nothing
  }else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(fd_item,
                                                                          true,
                                                                          distinct_exprs_,
                                                                          get_table_set()))) {
    LOG_WARN("failed to create fd item", K(ret));
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogDistinct::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pw_allocate_granule_pre(ctx))) {
    LOG_WARN("failed to allocate partition wise granule", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogDistinct::allocate_granule_post(AllocGIContext &ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogDistinct::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, distinct_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogDistinct::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  const ObDMLStmt *stmt = NULL;
  ObString qb_name;
  const ObLogicalOperator *child = NULL;
  const ObLogicalOperator *op = NULL;
  if (is_push_down()) {
    /* print outline in top distinct */
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())
      || OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()), K(stmt), K(child));
  } else if (OB_FAIL(child->get_pushdown_op(log_op_def::LOG_DISTINCT, op))) {
    LOG_WARN("failed to get push down distinct", K(ret));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else if (NULL != op &&
             OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\")",
                                ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                ObHint::get_hint_name(T_DISTINCT_PUSHDOWN),
                                qb_name.length(),
                                qb_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else if (HASH_AGGREGATE == algo_ &&
             OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\")",
                                ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                ObHint::get_hint_name(T_USE_HASH_DISTINCT),
                                qb_name.length(),
                                qb_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else if (NULL != op || is_partition_wise()) {
    ObPQHint pq_hint(T_PQ_DISTINCT_HINT);
    pq_hint.set_qb_name(qb_name);
    pq_hint.set_dist_method(is_partition_wise() ? T_DISTRIBUTE_NONE : T_DISTRIBUTE_HASH);
    if (OB_FAIL(pq_hint.print_hint(plan_text))) {
      LOG_WARN("failed to print pq hint", K(ret), K(pq_hint));
    }
  } else {/*do nothing*/}
  return ret;
}

int ObLogDistinct::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (is_push_down()) {
    /* print outline in top distinct */
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else {
    const ObHint *use_hash = get_plan()->get_log_plan_hint().get_normal_hint(T_USE_HASH_DISTINCT);
    const ObHint *pushdown_hint = get_plan()->get_log_plan_hint().get_normal_hint(T_DISTINCT_PUSHDOWN);
    const ObPQHint *pq_hint = dynamic_cast<const ObPQHint*>(get_plan()->get_log_plan_hint().get_normal_hint(T_PQ_DISTINCT_HINT));
    if (NULL != use_hash) {
      bool match_hint = (HASH_AGGREGATE == algo_ && use_hash->is_enable_hint())
                        || (MERGE_AGGREGATE == algo_ && use_hash->is_disable_hint());
      if (match_hint && OB_FAIL(use_hash->print_hint(plan_text))) {
        LOG_WARN("failed to print used hint for group by", K(ret), K(*use_hash));
      }
    }
    if (OB_SUCC(ret) && (NULL != pushdown_hint || NULL != pq_hint)) {
      const ObLogicalOperator *child = NULL;
      const ObLogicalOperator *op = NULL;
      if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(child));
      } else if (OB_FAIL(child->get_pushdown_op(log_op_def::LOG_DISTINCT, op))) {
        LOG_WARN("failed to get push down distinct", K(ret));
      } else if (NULL != pushdown_hint && (NULL == op ? pushdown_hint->is_disable_hint() : pushdown_hint->is_enable_hint())
                 && OB_FAIL(pushdown_hint->print_hint(plan_text))) {
        LOG_WARN("failed to print used hint for group by", K(ret), KPC(pushdown_hint));
      } else if (NULL != pq_hint
                 && ((NULL != op && pq_hint->is_force_dist_hash())
                     || (is_partition_wise() && pq_hint->is_force_partition_wise()))
                 && OB_FAIL(pq_hint->print_hint(plan_text))) {
        LOG_WARN("failed to print used hint for pq group by", K(ret), KPC(pq_hint));
      }
    }
  }
  return ret;
}

int ObLogDistinct::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  card = get_total_ndv();
  return ret;
}

int ObLogDistinct::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  used = true;
  inherit_child_ordering_index = first_child;
  if (HASH_AGGREGATE == get_algo()) {
    inherit_child_ordering_index = -1;
    used = false;
  }
  return ret;
}

int ObLogDistinct::compute_property()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *top = get_child(first_child);
  bool need_sort = false;
  int64_t prefix_pos = 0;
  if (OB_ISNULL(top) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_property())) {
    LOG_WARN("failed to compute op property", K(ret));
  } else if (MERGE_AGGREGATE == algo_) {
    input_sorted_ = true;
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(distinct_exprs_,
                                                      NULL/*directions*/,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      get_plan()->get_onetime_query_refs(),
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (!need_sort) {
    input_sorted_ = true;
  } else {
    input_sorted_ = false;
  }
  return ret;
}

int ObLogDistinct::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::compute_op_parallel_and_server_info())) {
    LOG_WARN("failed to compute parallel and server info", K(ret));
  } else if (is_partition_wise() && !is_push_down()) {
    ObLogicalOperator *child = get_child(first_child);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null child op", K(ret));
    } else if (child->get_part_cnt() > 0 &&
               get_parallel() > child->get_part_cnt()) {
      int64_t reduce_parallel = child->get_part_cnt();
      reduce_parallel = reduce_parallel < 2 ? 2 : reduce_parallel;
      set_parallel(reduce_parallel);
      need_re_est_child_cost_ = true;
    }
  }
  return ret;
}

}
}

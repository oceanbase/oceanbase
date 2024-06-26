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
#include "sql/optimizer/ob_log_sort.h"
#include "ob_optimizer_context.h"
#include "ob_opt_est_cost.h"
#include "ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "ob_log_exchange.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_join_order.h"
#include "share/ob_order_perserving_encoder.h"
#include "common/ob_smart_call.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/optimizer/ob_log_table_scan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define MIN_TSC_OUTPUT_ROWS_FOR_PD_TOPN_FILTER 256

int ObLogSort::set_sort_keys(const common::ObIArray<OrderItem> &order_keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_keys_.assign(order_keys))) {
    LOG_WARN("failed to set sort keys", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObLogSort::create_encode_sortkey_expr(const common::ObIArray<OrderItem> &order_keys) {
  int ret = OB_SUCCESS;
  ObOpRawExpr* encode_expr = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (OB_ISNULL(get_plan()->get_optimizer_context().get_exec_ctx())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else {
    int64_t ecd_pos = 0;

    // Prefix sort and hash-based sort both can combine with encode sort.
    // And prefix sort is prior to hash-based sort(part sort).
    if (is_prefix_sort() || is_part_sort()) {
      int64_t orig_pos = is_prefix_sort() ? get_prefix_pos() : get_part_cnt();
      for (int64_t i = 0; OB_SUCC(ret) && i < orig_pos; ++i) {
        if (OB_FAIL(encode_sortkeys_.push_back(order_keys.at(i)))) {
          LOG_WARN("failed to add encodekey", K(ret));
        } else {
          ecd_pos++;
        }
      }
    } else {
      ecd_pos = 0;
    }
    ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    ObExecContext* exec_ctx = get_plan()->get_optimizer_context().get_exec_ctx();
    OrderItem encode_sortkey;
    if (OB_FAIL(ObSQLUtils::create_encode_sortkey_expr(
        expr_factory, exec_ctx, order_keys, ecd_pos, encode_sortkey))) {
      LOG_WARN("failed to create encode sortkey expr", K(ret));
    } else if (OB_FAIL(encode_sortkeys_.push_back(encode_sortkey))) {
      LOG_WARN("failed to push back encode sortkey", K(ret));
    } else { /* do nothing*/ }
  }
  return ret;
}

int ObLogSort::get_sort_exprs(common::ObIArray<ObRawExpr*> &sort_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
    if (OB_FAIL(sort_exprs.push_back(sort_keys_.at(i).expr_))) {
      LOG_WARN("push back order key expr failed", K(ret));
    }
  }
  return ret;
}

int ObLogSort::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  bool can_sort_opt = true;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL != topn_expr_ && OB_FAIL(all_exprs.push_back(topn_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != topk_limit_expr_ && OB_FAIL(all_exprs.push_back(topk_limit_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != topk_offset_expr_ && OB_FAIL(all_exprs.push_back(topk_offset_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_can_encode_sortkey(sort_keys_,
                              can_sort_opt, *get_plan(), child->get_card()))) {
    LOG_WARN("failed to check encode sortkey expr", K(ret));
  } else if (NULL != topn_expr_ && FALSE_IT(can_sort_opt = false)) {
    // do nothing
  } else if ((is_prefix_sort() ? get_prefix_pos() : get_part_cnt() == sort_keys_.count()) &&
        FALSE_IT(can_sort_opt = false)) {
    // do nothing
  } else if (can_sort_opt && OB_FAIL(create_encode_sortkey_expr(sort_keys_))) {
    LOG_WARN("failed to create encode sortkey expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
      if (OB_ISNULL(sort_keys_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(sort_keys_.at(i).expr_))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/ }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < encode_sortkeys_.count(); i++) {
      if (OB_ISNULL(encode_sortkeys_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (OB_FAIL(all_exprs.push_back(encode_sortkeys_.at(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret)) {
      if (part_cnt_ > 0) {
        if (OB_ISNULL(hash_sortkey_.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(all_exprs.push_back(hash_sortkey_.expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
      if (FAILEDx(ObLogicalOperator::get_op_exprs(all_exprs))) {
        LOG_WARN("failed to get op exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

uint64_t ObLogSort::hash(uint64_t seed) const
{
  bool is_topn = NULL != topn_expr_;
  seed = do_hash(is_topn, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogSort::get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<OrderItem, 1> sort_keys;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  }
  BEGIN_BUF_PRINT;
  if (OB_FAIL(ret)) {
  } else if (NULL != get_hash_sortkey().expr_ &&
             OB_FAIL(sort_keys.push_back(get_hash_sortkey()))) {
    LOG_WARN("failed to push back sortkeys", K(ret));
  } else if (OB_FAIL(append(sort_keys, get_sort_keys()))) {
    LOG_WARN("failed to append sortkeys", K(ret));
  } else {
    EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type);
  }
  if (OB_SUCC(ret) && NULL != topn_expr_) {
    ObRawExpr *topn = topn_expr_;
    BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPR(topn, type);
  }
  ObRawExpr *limit = topk_limit_expr_;
  if (OB_SUCC(ret) && NULL != limit) {
    if (OB_FAIL(BUF_PRINTF(", minimum_row_count:%ld top_precision:%ld ",
                            minimum_row_count_, topk_precision_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      ObRawExpr *offset = topk_offset_expr_;
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(limit, type);
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(offset, type);
    }
  } else { /* Do nothing */ }

  if (OB_SUCC(ret) && prefix_pos_> 0) {
    BUF_PRINTF(", prefix_pos(");
    if (OB_FAIL(BUF_PRINTF("%ld)", prefix_pos_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret), K(prefix_pos_));
    }
  }
  if (OB_SUCC(ret) && is_local_merge_sort_) {
    BUF_PRINTF(", local merge sort");
  }
  // this will be opened later, when newsort enabled by default
  //if (OB_SUCC(ret) && !enable_encode_sortkey_opt()) {
  //  BUF_PRINTF(", not encoded");
  //}
  if (OB_SUCC(ret) && is_fetch_with_ties_) {
    BUF_PRINTF(", with_ties(true)");
  }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
  return ret;
}

int ObLogSort::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  int64_t N = sort_keys_.count();
  if (NULL != topn_expr_ && OB_FAIL(replace_expr_action(replacer, topn_expr_))) {
    LOG_WARN("failed to replace topn expr", K(ret));
  } else if (NULL != topk_limit_expr_ &&
             OB_FAIL(replace_expr_action(replacer, topk_limit_expr_))) {
    LOG_WARN("failed to replace topk limit expr", K(ret));
  } else if (NULL != topk_offset_expr_ &&
             OB_FAIL(replace_expr_action(replacer, topk_offset_expr_))) {
    LOG_WARN("failed to replace topk offset expr", K(ret));
  }
  for(int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    OrderItem &cur_order_item = sort_keys_.at(i);
    if (OB_FAIL(replace_expr_action(replacer, cur_order_item.expr_))) {
      LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
    } else { /* Do nothing */ }
  }
  for(int64_t i = 0; OB_SUCC(ret) && i < encode_sortkeys_.count(); ++i) {
    OrderItem &cur_order_item = encode_sortkeys_.at(i);
    if (OB_FAIL(replace_expr_action(replacer, cur_order_item.expr_))) {
      LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
    } else { /* Do nothing */ }
  }
  if (OB_SUCC(ret) && part_cnt_ > 0) {
    if (OB_FAIL(replace_expr_action(replacer, hash_sortkey_.expr_))) {
      LOG_WARN("failed to resolve ref params of hash sortkey", K(hash_sortkey_), K(ret));
    } else { /* Do nothing */ }
  }
  return ret;
}

const char *ObLogSort::get_name() const
{
  const char *ret = NULL;
  if (NULL != topn_expr_) {
    if (part_cnt_ > 0) {
      ret = "PARTITION TOP-N SORT";
    } else {
      ret = "TOP-N SORT";
    }
  } else if (NULL == topk_limit_expr_ && prefix_pos_ <= 0 && part_cnt_ > 0) {
    ret = "PARTITION SORT";
  }
  return NULL != ret ? ret : log_op_def::get_op_name(type_);
}

int ObLogSort::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (!get_plan()->get_candidate_plans().is_final_sort_) {
    width = child->get_width();
    set_width(width);
    if (OB_FAIL(est_sort_key_width())) {
      LOG_WARN("failed to est sort key width", K(ret));
    }
    LOG_TRACE("est width for non-final sort", K(output_exprs), K(width));
  } else if (OB_FAIL(get_sort_output_exprs(output_exprs))) {
    LOG_WARN("failed to get sort output exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output orderby exprs", K(ret));
  } else if (OB_FAIL(est_sort_key_width())) {
    LOG_WARN("failed to est sort key width", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for final sort", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogSort::est_sort_key_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  sort_key_width_ = 0.0;
  ObSEArray<ObRawExpr*, 16> sortkey_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
    if (OB_FAIL(sortkey_exprs.push_back(sort_keys_.at(i).expr_))) {
      LOG_WARN("failed to add sort key expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            sortkey_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for sortkey orderby exprs", K(ret));
  } else {
    if (enable_encode_sortkey_opt()) {
      // A rough estimate of the memory size used by encode is equal to the size of the sort key.
      width *= 2;
    }
    if (part_cnt_ > 0) {
      width += sizeof(int64_t);
    }
    sort_key_width_ = width;
  }
  return ret;
}

int ObLogSort::get_sort_output_exprs(ObIArray<ObRawExpr *> &output_exprs)
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
  } else if (OB_FAIL(ObRawExprUtils::extract_col_aggr_winfunc_exprs(candi_exprs,
                                                                    extracted_col_aggr_winfunc_exprs))) {
  } else if (OB_FAIL(append_array_no_dup(output_exprs, extracted_col_aggr_winfunc_exprs))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogSort::est_cost()
{
  int ret = OB_SUCCESS;
  double sort_cost = 0.0;
  double double_topn_count = -1;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(inner_est_cost(get_parallel(), child->get_card(), double_topn_count, sort_cost))) {
    LOG_WARN("failed to est sort cost", K(ret));
  } else {
    set_op_cost(sort_cost);
    set_cost(child->get_cost() + sort_cost);
    if (double_topn_count >= 0 && child->get_card() > double_topn_count) {
      set_card(double_topn_count);
    } else {
      set_card(child->get_card());
    }
    LOG_TRACE("cost for sort operator", K(sort_cost), K(get_cost()),
              K(get_card()));
  }
  return ret;
}

int ObLogSort::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double child_card = 0.0;
  double child_cost = 0.0;
  double double_topn_count = -1;
  card = get_card();
  const int64_t parallel = param.need_parallel_;
  if (param.need_row_count_ >=0 && param.need_row_count_ < card) {
    card = param.need_row_count_;
  }
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt())
      || OB_ISNULL(get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (get_stmt()->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_2_1_BP4) {
    param.need_row_count_ = -1;
  } else if (-1 == param.need_row_count_) {
    //do nothing
  } else if (!is_prefix_sort()) {
    param.need_row_count_ = -1;
  } else {
    ObSEArray<ObRawExpr*, 4> prefix_ordering;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_prefix_pos(); ++i) {
      if (OB_FAIL(prefix_ordering.push_back(sort_keys_.at(i).expr_))) {
        LOG_WARN("push back order key expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      double prefix_ndv = 0.0;
      if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_plan()->get_update_table_metas(),
                                                        get_plan()->get_selectivity_ctx(),
                                                        prefix_ordering,
                                                        child->get_card(),
                                                        prefix_ndv))) {
        LOG_WARN("failed to calculate distinct", K(ret));
      } else if (OB_UNLIKELY(std::fabs(prefix_ndv) < 1.0)) {
        param.need_row_count_ = -1;
      } else {
        double num_rows_per_group = child->get_card() / prefix_ndv;
        double num_groups = std::ceil(param.need_row_count_ / num_rows_per_group);
        param.need_row_count_ = num_groups * num_rows_per_group;
        if (param.need_row_count_ >= child->get_card()) {
          param.need_row_count_ = -1;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("failed to re est cost", K(ret));
  } else if (OB_FAIL(inner_est_cost(parallel, child_card, double_topn_count, op_cost))) {
    LOG_WARN("failed to est sort cost", K(ret));
  } else {
    cost = child_cost + op_cost;
    card = child_card < card ? child_card : card;
    if (double_topn_count >= 0 && card > double_topn_count) {
      card = double_topn_count;
    }
  }
  return ret;
}

int ObLogSort::inner_est_cost(const int64_t parallel, double child_card, double &double_topn_count, double &op_cost)
{
  int ret = OB_SUCCESS;
  int64_t topn_count = -1;
  bool is_null_value = false;
  double_topn_count = -1;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_stmt()),
        K(get_plan()), K(ret));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else if (NULL != topn_expr_ &&
             OB_FAIL(ObTransformUtils::get_limit_value(topn_expr_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       topn_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get value", K(ret));
  } else {
    if (NULL != topn_expr_) {
      double_topn_count = static_cast<double>(topn_count);
    }
    double child_card_per_dop = child_card / parallel;
    if (double_topn_count > child_card_per_dop) {
      double_topn_count = child_card_per_dop;
    }
    get_plan()->get_selectivity_ctx().init_op_ctx(&child->get_output_equal_sets(), child_card);
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    ObSortCostInfo cost_info(child_card_per_dop,
                             child->get_width(),
                             get_prefix_pos(),
                             get_sort_keys(),
                             is_local_merge_sort_,
                             &get_plan()->get_update_table_metas(),
                             &get_plan()->get_selectivity_ctx(),
                             double_topn_count,
                             part_cnt_);
    if (OB_FAIL(ObOptEstCost::cost_sort(cost_info, op_cost, opt_ctx))) {
      LOG_WARN("failed to calc cost", K(ret), K(child->get_type()));
    } else if (NULL != topn_expr_) {
      if (part_cnt_ > 0) {
        //partition topn sort
        ObSEArray<ObRawExpr*, 4> part_exprs;
        for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
          if (i < cost_info.part_cnt_) {
            if (OB_FAIL(part_exprs.push_back(sort_keys_.at(i).expr_))) {
              LOG_WARN("fail to push back expr", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          double child_rows = child_card / parallel;
          double distinct_parts = child_rows;
          if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_plan()->get_update_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            part_exprs,
                                                            child_rows,
                                                            distinct_parts))) {
            LOG_WARN("failed to calculate distinct", K(ret));
          } else if (OB_UNLIKELY(distinct_parts < 1.0 || distinct_parts > child_rows)) {
            distinct_parts = child_rows;
          }
          double_topn_count = std::min(distinct_parts * double_topn_count * parallel, child_card);
        }
      } else {
        double_topn_count = std::min(double_topn_count * parallel, child_card);
      }
    }
  }
  return ret;
}

int ObLogSort::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<OrderItem, 1> op_ordering;
  if (part_cnt_ > 0 && OB_FAIL(op_ordering.push_back(hash_sortkey_))) {
    LOG_WARN("failed to push back hash sortkey", K(ret));
  } else if (OB_FAIL(append(op_ordering, sort_keys_))) {
    LOG_WARN("failed to append sort keys", K(ret));
  } else if (OB_FAIL(set_op_ordering(op_ordering))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else {
    is_local_order_ = false;
  }
  return ret;
}

int ObLogSort::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_FUN_SYS_ENCODE_SORTKEY == expr->get_expr_type() || expr == hash_sortkey_.expr_) {
    is_fixed = true;
  }
  return ret;
}

int ObLogSort::try_allocate_pushdown_topn_runtime_filter()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *node = nullptr;
  double tsc_output_rows = 0;
  ObOpRawExpr *pushdown_topn_filter_expr = nullptr;
  ObSQLSessionInfo *session_info = nullptr;
  uint64_t table_id = OB_INVALID_ID;
  int64_t p2p_sequence_id = OB_INVALID_ID;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> candidate_sk_exprs;
  int64_t effective_sk_cnt = 0;
  bool tsc_has_exchange = false;
  bool tsc_has_px_coord = false;
  bool can_allocate = false;
  uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  int ecode = EventTable::EN_PX_DISABLE_PD_TOPN_FILTER;
  if (min_cluster_version < DATA_VERSION_4_3_2_0) {
    can_allocate = false;
  } else if (OB_SUCCESS != ecode) {
    can_allocate = false;
    LOG_TRACE("[TopN Filter]disable push down topn filter by tracepoint");
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log_plan unexpected null");
  } else if (OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info unexpected null");
  } else if (OB_ISNULL(topn_expr_)) {
    // not topn scene
    can_allocate = false;
  } else if (prefix_pos_ > 0) {
    // For index (c1, c2 c3), if order by c1, c3, its prefix sort scene
    // if all the date of c1 is unique, topn runtime filter is useless,
    // otherwise if most of the date of c1 is unique, the topn runtime filter
    // can filter on c3.
    // Since we can't distinguish these two scene accurately, topn runtime filter
    // is forbiddened in the prefix sort scene. Enable it by judging condition
    // prefix_pos_ < effective_sk_cnt.
    can_allocate = false;
    LOG_TRACE("[TopN Filter]can not pushdown when prefix sort ", K(prefix_pos_), K(sort_keys_.count()));
  } else if (OB_FAIL(get_candidate_pushdown_sort_keys(table_id, candidate_sk_exprs))) {
    LOG_WARN("failed to get_candidate_pushdown_sort_keys");
  } else if (OB_INVALID_ID == table_id) {
    // such as order by sqrt(3.3), the sort key is a const expr, can not pushdown
    can_allocate = false;
  } else if (0 == candidate_sk_exprs.count()) {
    // such as order by t1.c1 + t2.c1, the sort key is related to 2 tables, can not pushdown
    can_allocate = false;
    LOG_TRACE("[TopN Filter]the first sort key from different tables");
  } else if (OB_FAIL(check_sort_key_can_pushdown_to_tsc(get_child(first_child), candidate_sk_exprs,
                                                        table_id, node, tsc_has_exchange,
                                                        tsc_has_px_coord, effective_sk_cnt))) {
    LOG_WARN("failed to find check check_sort_key_can_pushdown_to_tsc", K(ret));
  } else if (0 == effective_sk_cnt) {
    can_allocate = false;
    LOG_TRACE("[TopN Filter]no effective_sk");
  } else if (OB_ISNULL(node)) {
    // tsc node is in block path, or not tsc node
    // can not pushdown topn filter
    can_allocate = false;
    LOG_TRACE("[TopN Filter]block path, or no tsc node");
  } else if (tsc_has_px_coord) {
    // not support
    can_allocate = false;
  } else if (tsc_has_exchange) {
    // TODO XUNSI: global topn filter, impl topn filter in neighbour dfos scenes.
    can_allocate = false;
    LOG_TRACE("[TopN Filter]global topn filter is not support now");
  } else {
    const OptTableMetas &table_metas = get_plan()->get_basic_table_metas();
    const OptTableMeta *table_meta = nullptr;
    int ecode = EventTable::EN_PX_PD_TOPN_FILTER_IGNORE_TABLE_CARD;
    if (OB_ISNULL(table_meta = table_metas.get_table_meta_by_table_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_meta unexpected null");
    } else if (FALSE_IT(tsc_output_rows = table_meta->get_rows())) {
    } else if (ecode != OB_SUCCESS) {
      can_allocate = true;
      LOG_TRACE("[TopN Filter] force enable topn filter ignore less card", K(tsc_output_rows));
    } else if (MIN_TSC_OUTPUT_ROWS_FOR_PD_TOPN_FILTER > tsc_output_rows) {
      // table scan output first batch rows after a batch prepared, after that
      // the topn sort operator can get the first batch data, if the data num
      // of the table is too small, the topn filter will be invalid
      can_allocate = false;
      LOG_TRACE("[TopN Filter] disable topn filter because of less rows", K(tsc_output_rows));
    } else {
      can_allocate = true;
    }
  }

  if (OB_SUCC(ret) && can_allocate) {
    ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    if (OB_FAIL(
            expr_factory.create_raw_expr(T_OP_PUSHDOWN_TOPN_FILTER, pushdown_topn_filter_expr))) {
      LOG_WARN("fail to create raw expr", K(ret));
    } else {
      bool only_white_filter =
          T_REF_COLUMN == candidate_sk_exprs.at(0)->get_expr_type() && effective_sk_cnt > 1;
      if (only_white_filter) {
        LOG_TRACE("multi sort keys, and the first column can be pushdown as white");
        effective_sk_cnt = 1;
      }
      for (int64_t i = 0; i < effective_sk_cnt && OB_SUCC(ret); ++i) {
        ObRawExpr *sort_key = candidate_sk_exprs.at(i);
        if (OB_FAIL(pushdown_topn_filter_expr->add_param_expr(sort_key))) {
          LOG_WARN("fail to add param expr", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pushdown_topn_filter_expr->formalize(session_info))) {
      LOG_WARN("fail to formalize expr", K(ret));
    } else if (OB_FAIL(node->get_filter_exprs().push_back(pushdown_topn_filter_expr))) {
      LOG_WARN("fail to push back expr", K(ret));
    } else if (OB_FAIL(PX_P2P_DH.generate_p2p_dh_id(p2p_sequence_id))) {
      LOG_WARN("fail to generate p2p dh id", K(ret));
    } else {
      (void)topn_filter_info_.init(p2p_sequence_id, pushdown_topn_filter_expr, effective_sk_cnt,
                                   tsc_has_exchange);
    }
  }

  LOG_TRACE("[TopN Filter] allocate pushdown topn filter expr", K(can_allocate),
            K(sort_keys_.count()), K(prefix_pos_), K(candidate_sk_exprs.count()), K(table_id),
            K(node), K(tsc_has_px_coord), K(tsc_has_exchange), K(tsc_output_rows),
            K(topn_filter_info_));
  return ret;
}

// get the front sort keys from same table, topn filter can pushdown to this table
int ObLogSort::get_candidate_pushdown_sort_keys(
    uint64_t &table_id,
    common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> &candidate_sk_exprs)
{
  int ret = OB_SUCCESS;
  bool can_expr_pushdown = true;
  for (int64_t i = 0; i < sort_keys_.count() && OB_SUCC(ret) && can_expr_pushdown; ++i) {
    ObRawExpr *sort_key = sort_keys_.at(i).expr_;
    if (OB_ISNULL(sort_key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr");
    } else if (OB_FAIL(check_expr_can_pushdown(sort_key, table_id, can_expr_pushdown))) {
      LOG_WARN("failed to check_sort_key_can_pushdown");
    } else if (can_expr_pushdown && OB_FAIL(candidate_sk_exprs.push_back(sort_key))) {
      LOG_WARN("failed to pushback");
    }
  }
  return ret;
}

int ObLogSort::check_expr_can_pushdown(ObRawExpr *expr, uint64_t &table_id, bool &can_push_down)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr");
  } else if (expr->is_const_expr()) {
    // skip check const expr and its children
  } else if (OB_FAIL(is_expr_in_pushdown_whitelist(expr, can_push_down))) {
    LOG_WARN("failed to check expr_in_pushdown_whitelist");
  } else if (!can_push_down) {
    // not in whitelist, can not pushdown
  } else if (expr->is_column_ref_expr()) {
    // check if all columns from the same table
    ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (OB_INVALID_ID == table_id) {
      table_id = col_ref_expr->get_table_id();
    } else if (table_id != col_ref_expr->get_table_id()) {
      // column from different tables, this sort key and the succeed sort key can not pushdown
      can_push_down = false;
    }
  } else {
    for (int64_t i = 0; i < expr->get_param_count() && OB_SUCC(ret) && can_push_down; ++i) {
      ObRawExpr *child_expr = expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(check_expr_can_pushdown(child_expr, table_id, can_push_down)))) {
        LOG_WARN("failed to do check_expr_can_pushdown");
      }
    }
  }
  return ret;
}

int ObLogSort::is_expr_in_pushdown_whitelist(ObRawExpr *expr, bool &in_pushdown_whitelist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr");
  } else if (expr->is_column_ref_expr() || expr->is_const_raw_expr() || expr->is_op_expr()) {
    // const expr, column ref, or +-*/ op
    in_pushdown_whitelist = true;
  } else {
    // some system functions can also be pushdown
    switch (expr->get_expr_type()) {
      case T_OP_ABS:
      case T_FUN_SYS_CAST:
      case T_FUN_SYS_SUBSTR:
      case T_FUN_SYS_LENGTH:
      case T_FUN_SYS_UPPER:
      case T_FUN_SYS_LOWER:
      case T_FUN_SYS_ROUND:
      case T_FUN_SYS_FLOOR:
      case T_FUN_SYS_CEIL:
      case T_FUN_SYS_SQRT:
      case T_FUN_SYS_EXP:
      case T_FUN_SYS_POWER:
      case T_FUN_SYS_LN:
      case T_FUN_SYS_LOG:
      case T_FUN_SYS_ASIN:
      case T_FUN_SYS_ACOS:
      case T_FUN_SYS_ATAN:
      case T_FUN_SYS_ATAN2:
      case T_FUN_SYS_COS:
      case T_FUN_SYS_TAN:
      case T_FUN_SYS_SIN:
      case T_FUN_SYS_SINH:
      case T_FUN_SYS_COSH:
      case T_FUN_SYS_TANH: {
        in_pushdown_whitelist = true;
        break;
      }
      default: {
        in_pushdown_whitelist = false;
        LOG_TRACE("this expr can not be pushdown", K(expr->get_expr_type()));
        break;
      }
    }
  }
  return ret;
}

int ObLogSort::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  used = true;
  inherit_child_ordering_index = -1;
  if (!is_prefix_sort() && !is_local_merge_sort()) {
    used = false;
  }
  return ret;
}

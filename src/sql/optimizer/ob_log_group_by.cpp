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
#include "ob_log_group_by.h"
#include "lib/allocator/page_arena.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
#include "ob_log_operator_factory.h"
#include "ob_log_exchange.h"
#include "ob_log_sort.h"
#include "ob_log_topk.h"
#include "ob_log_material.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_context.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "ob_raw_expr_pull_up_aggr_expr.h"
#include "ob_raw_expr_push_down_aggr_expr.h"
#include "ob_select_log_plan.h"
#include "common/ob_smart_call.h"
#include "ob_log_window_function.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObLogGroupBy::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogGroupBy* group_by = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogGroupBy", K(ret));
  } else if (OB_ISNULL(group_by = static_cast<ObLogGroupBy*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogGroupBy *", K(ret));
  } else {
    group_by->set_algo_type(algo_);
    group_by->aggr_exprs_ = aggr_exprs_;
    group_by->group_exprs_ = group_exprs_;
    group_by->rollup_exprs_ = rollup_exprs_;
    group_by->distinct_card_ = distinct_card_;
    group_by->from_pivot_ = from_pivot_;
    out = static_cast<ObLogicalOperator*>(group_by);
  }
  return ret;
}

int32_t ObLogGroupBy::get_explain_name_length() const
{
  int32_t length = 0;
  if (SCALAR_AGGREGATE == algo_) {
    length += (int32_t)strlen("SCALAR ");
  } else if (HASH_AGGREGATE == algo_) {
    length += (int32_t)strlen("HASH ");
  } else {
    length += (int32_t)strlen("MERGE ");
  }
  length += (int32_t)strlen(get_name());

  if (from_pivot_) {
    length += (int32_t)strlen(" PIVOT");
  }
  return length;
}

int ObLogGroupBy::get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (SCALAR_AGGREGATE == algo_) {
    ret = BUF_PRINTF("SCALAR ");
  } else if (HASH_AGGREGATE == algo_) {
    ret = BUF_PRINTF("HASH ");
  } else {
    ret = BUF_PRINTF("MERGE ");
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF("%s", get_name());
  }

  if (OB_SUCC(ret) && from_pivot_) {
    ret = BUF_PRINTF(" PIVOT");
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }

  return ret;
}

int ObLogGroupBy::set_group_by_exprs(const common::ObIArray<ObRawExpr*>& group_by_exprs)
{
  return group_exprs_.assign(group_by_exprs);
}

int ObLogGroupBy::set_rollup_exprs(const common::ObIArray<ObRawExpr*>& rollup_exprs)
{
  return rollup_exprs_.assign(rollup_exprs);
}

int ObLogGroupBy::set_aggr_exprs(const common::ObIArray<ObAggFunRawExpr*>& aggr_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
    ObAggFunRawExpr* aggr_expr = aggr_exprs.at(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_expr is null", K(ret));
    } else {
      ret = aggr_exprs_.push_back(aggr_expr);
    }
  }
  return ret;
}

int ObLogGroupBy::get_child_groupby_algorithm(const bool is_distinct, const bool can_push_down_distinct,
    const bool need_sort, const bool child_need_sort, AggregateAlgo& aggr_algo)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    LOG_WARN("plan is null or stmt is null", K(ret), K(get_plan()));
  } else if (is_distinct) {
    const int64_t group_by_type = stmt->get_stmt_hint().aggregate_;
    if (SCALAR_AGGREGATE == get_algo()) {
      if (can_push_down_distinct) {
        aggr_algo = MERGE_AGGREGATE;
      } else {
        aggr_algo = ((group_by_type & OB_NO_USE_HASH_AGGREGATE) ||
                        (!child_need_sort && !(group_by_type & OB_USE_HASH_AGGREGATE)))
                        ? MERGE_AGGREGATE
                        : HASH_AGGREGATE;
      }
    } else if (!can_push_down_distinct && MERGE_AGGREGATE == get_algo()) {
      aggr_algo =
          ((group_by_type & OB_NO_USE_HASH_AGGREGATE) || (!need_sort && !(group_by_type & OB_USE_HASH_AGGREGATE)))
              ? MERGE_AGGREGATE
              : HASH_AGGREGATE;
    } else {
      aggr_algo = get_algo();
    }
  } else {
    if (SCALAR_AGGREGATE == get_algo()) {
      aggr_algo = MERGE_AGGREGATE;
    } else {
      aggr_algo = get_algo();
    }
  }
  return ret;
}

int ObLogGroupBy::get_child_groupby_expected_ordering(
    const ObIArray<ObRawExpr*>& distinct_exprs, ObIArray<OrderItem>& expected_ordering)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(child), K(ret));
  } else if (OB_FAIL(expected_ordering.assign(expected_ordering_))) {
    LOG_WARN("failed to assign expected ordering", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_ordering.count(); i++) {
      if (OB_ISNULL(expected_ordering.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(temp_exprs.push_back(expected_ordering.at(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < distinct_exprs.count(); i++) {
      bool is_const = false;
      if (OB_FAIL(ObOptimizerUtil::is_const_expr(distinct_exprs.at(i),
              child->get_ordering_output_equal_sets(),
              child->get_output_const_exprs(),
              is_const))) {
        LOG_WARN("failed to check const expr", K(ret));
      } else if (is_const) {
        /*do nothing*/
      } else if (ObOptimizerUtil::find_equal_expr(
                     temp_exprs, distinct_exprs.at(i), child->get_ordering_output_equal_sets())) {
        /*do nothing*/
      } else if (OB_FAIL(expected_ordering.push_back(OrderItem(distinct_exprs.at(i), default_asc_direction())))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogGroupBy::allocate_groupby_below(const ObIArray<ObRawExpr*>& distinct_exprs, const bool should_push_distinct,
    ObLogicalOperator*& exchange_point, ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& group_push_down_replaced_exprs)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  bool child_need_sort = false;
  ObLogicalOperator* child = NULL;
  ObLogGroupBy* child_group_by = NULL;
  ObSEArray<ObRawExpr*, 8> child_group_exprs;
  AggregateAlgo child_group_algo = AggregateAlgo::AGGREGATE_UNINITIALIZED;
  ObSEArray<OrderItem, 8> child_expected_ordering;
  exchange_point = NULL;
  if ((MERGE_AGGREGATE == get_algo() ||
          (!distinct_exprs.empty() && SCALAR_AGGREGATE == get_algo() && !should_push_distinct)) &&
      OB_FAIL(get_child_groupby_expected_ordering(distinct_exprs, child_expected_ordering))) {
    LOG_WARN("failed to get child groupby expected ordering", K(ret));
  } else if (OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (MERGE_AGGREGATE == get_algo() && log_op_def::LOG_SORT == get_child(first_child)->get_type()) {
    need_sort = true;
    child_need_sort = true;
    exchange_point = get_child(first_child);
  } else {
    exchange_point = this;
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(get_group_rollup_exprs(child_group_exprs))) {
    LOG_WARN("failed to get groupby/rollup exprs", K(ret));
  } else if (!should_push_distinct && OB_FAIL(append_array_no_dup(child_group_exprs, distinct_exprs))) {
    LOG_WARN("failed to append distinct exprs", K(ret));
  } else if (!need_sort && !child_expected_ordering.empty() && !distinct_exprs.empty() &&
             OB_FAIL(check_need_sort_for_grouping_op(first_child, child_expected_ordering, child_need_sort))) {
    LOG_WARN("failed to check need sort for grouping op", K(ret));
  } else if (OB_FAIL(get_child_groupby_algorithm(
                 !distinct_exprs.empty(), should_push_distinct, need_sort, child_need_sort, child_group_algo))) {
    LOG_WARN("failed to get child group by algo", K(ret));
  } else if (child_need_sort && HASH_AGGREGATE != child_group_algo &&
             OB_FAIL(exchange_point->allocate_sort_below(first_child, child_expected_ordering))) {
    LOG_WARN("failed to allocate sort below", K(ret));
  } else if (OB_FAIL(exchange_point->allocate_group_by_below(
                 first_child, child_group_exprs, child_group_algo, from_pivot_))) {
    LOG_WARN("failed to allocate group by below", K(ret));
  } else if (OB_ISNULL(child = exchange_point->get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(log_op_def::LOG_GROUP_BY != child->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected operator type", K(child->get_type()), K(ret));
  } else if (FALSE_IT(child_group_by = static_cast<ObLogGroupBy*>(child))) {
    /*do nothing*/
  } else if (child_need_sort && HASH_AGGREGATE != child_group_algo &&
             OB_FAIL(child_group_by->set_expected_ordering(child_expected_ordering))) {
    LOG_WARN("failed to set expected ordering", K(ret));
  } else {
    if (SCALAR_AGGREGATE == get_algo() || get_algo() == child_group_algo) {
      child_group_by->set_card(get_card());
      child_group_by->set_op_cost(get_op_cost());
      child_group_by->set_width(get_width());
    } else if (need_sort && OB_FAIL(exchange_point->est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else if (OB_FAIL(est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    }

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (should_push_distinct ||
               (MERGE_AGGREGATE == child_group_algo && !child_need_sort && !child_group_exprs.empty())) {
      // pullup gi operator above child-group-by in the following suitaitons
      // 1. merge group by without sort operator,
      //    allocate gi above group-by can make use of the table scan's ordering
      // 2. distint expr is pushed down, such as count(distinct c1)
      //    a partition should not be processed by two px tasks, other the distinct result
      //    will be mis-estimated
      bool child_can_pullup_gi = true;
      if (OB_ISNULL(child = child_group_by->get_child(first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child), K(ret));
      } else if (OB_FAIL(child_group_by->get_sharding_info().copy_with_part_keys(child->get_sharding_info()))) {
        LOG_WARN("failed to copy sharding info", K(ret));
      } else if (OB_FAIL(check_can_pullup_gi(child, child_can_pullup_gi))) {
        LOG_WARN("fail to find tsc recursive");
      } else if (child_can_pullup_gi) {
        child_group_by->set_is_partition_wise(true);
        child_group_by->set_is_block_gi_allowed(!should_push_distinct);
      } else { /*do nothing*/
      }
    }
  }

  if (OB_SUCC(ret) && (distinct_exprs.empty() || should_push_distinct)) {
    ObArray<std::pair<ObRawExpr*, ObRawExpr*> > push_down_agg_expr;
    if (OB_FAIL(produce_pushed_down_expressions(child_group_by, group_push_down_replaced_exprs, push_down_agg_expr))) {
      LOG_WARN("failed to produced push-down exprs", K(ret), K(child_group_by));
    } else if (distinct_exprs.empty() &&
               OB_FAIL(allocate_topk_if_needed(exchange_point, child_group_by, push_down_agg_expr))) {
      LOG_WARN("failed to alloc_topk_if needed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

// child has allocate exchange or parallel window function, can not pullup gi
int ObLogGroupBy::check_can_pullup_gi(const ObLogicalOperator *op, bool &can_pullup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || !can_pullup) {
    /*do nothing*/
  } else if (log_op_def::LOG_EXCHANGE == op->get_type() ||
             (log_op_def::LOG_WINDOW_FUNCTION == op->get_type() &&
                 static_cast<const ObLogWindowFunction *>(op)->is_parallel())) {
    can_pullup = false;
  } else {
    for (int i = 0; OB_SUCC(ret) && can_pullup && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(check_can_pullup_gi(op->get_child(i), can_pullup)))) {
        LOG_WARN("fail to find tsc recursive", K(ret));
      }
    }
  }
  return ret;
}

int ObLogGroupBy::get_group_rollup_exprs(common::ObIArray<ObRawExpr *> &group_rollup_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(group_rollup_exprs, group_exprs_))) {
    LOG_WARN("failed to append group exprs into group rollup exprs.", K(ret));
  } else if (OB_FAIL(append(group_rollup_exprs, rollup_exprs_))) {
    LOG_WARN("failed to append rollup exprs into group rollup exprs.", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogGroupBy::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool group_compatible = false;
  bool group_rollup_compatible = false;
  ObLogicalOperator* child = NULL;
  ObLogicalOperator* exchange_point = NULL;
  bool should_push_groupby = false;
  bool should_push_distinct = false;
  ObSEArray<ObRawExpr*, 8> group_key_exprs;
  ObSEArray<ObRawExpr*, 8> rollup_key_exprs;
  ObSEArray<ObRawExpr*, 16> distinct_exprs;
  ObSEArray<ObRawExpr*, 8> group_rollup_exprs;
  ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4> group_push_down_replaced_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(child), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(prune_weak_part_exprs(*ctx, get_group_by_exprs(), group_key_exprs))) {
    LOG_WARN("failed to prune weak part exprs", K(ret));
  } else if (OB_FAIL(check_sharding_compatible_with_reduce_expr(child->get_sharding_info(),
                 group_key_exprs,
                 child->get_sharding_output_equal_sets(),
                 group_compatible))) {
    LOG_WARN("failed to check sharding compatible with groupby expr", K(ret));
  } else if (group_compatible) {
    if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
      LOG_WARN("failed to copy sharding info from child", K(ret));
    } else {
      is_partition_wise_ = (NULL != sharding_info_.get_phy_table_location_info());
    }
  } else if (OB_FAIL(should_push_down_group_by(*ctx, distinct_exprs, should_push_groupby, should_push_distinct))) {
    LOG_WARN("failed to check should push down group by", K(ret));
  } else if (!should_push_groupby) {
    // if we can not push down groupby operator, we directly allocate exchange operator
    if (MERGE_AGGREGATE == get_algo() && log_op_def::LOG_SORT == child->get_type()) {
      exchange_point = child;
    } else {
      exchange_point = this;
    }
    if (OB_FAIL(exchange_point->allocate_grouping_style_exchange_below(ctx, get_group_by_exprs()))) {
      LOG_WARN("failed to allocate exchange below", K(ret));
    } else if (OB_FAIL(sharding_info_.copy_with_part_keys(exchange_point->get_sharding_info()))) {
      LOG_WARN("failed to copy with partition keys", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(get_group_rollup_exprs(group_rollup_exprs))) {
    LOG_WARN("failed to get groupby rollup exprs.", K(ret));
  } else if (OB_FAIL(prune_weak_part_exprs(*ctx, group_rollup_exprs, rollup_key_exprs))) {
    LOG_WARN("failed to prune weak part exprs", K(ret));
  } else if (OB_FAIL(check_sharding_compatible_with_reduce_expr(child->get_sharding_info(),
                 rollup_key_exprs,
                 child->get_sharding_output_equal_sets(),
                 group_rollup_compatible))) {
    LOG_WARN("failed to check sharding compatible with groupby rollup expr", K(ret));
  } else {
    if (!group_rollup_compatible) {
      if (OB_FAIL(allocate_groupby_below(
              distinct_exprs, should_push_distinct, exchange_point, group_push_down_replaced_exprs))) {
        LOG_WARN("failed to allocate group by below op.", K(ret));
      } else if (OB_ISNULL(exchange_point)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exchange_point->allocate_grouping_style_exchange_below(ctx, group_rollup_exprs))) {
        LOG_WARN("failed to allocate exchange below op.", K(ret));
      } else if (OB_FAIL(sharding_info_.copy_with_part_keys(exchange_point->get_sharding_info()))) {
        LOG_WARN("failed to copy with partition keys", K(ret));
      } else if (OB_FAIL(append(ctx->group_push_down_replaced_exprs_, group_push_down_replaced_exprs))) {
        LOG_WARN("failed to append group push down replace exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && has_rollup()) {
      if (sharding_info_.is_local() || sharding_info_.is_remote() || sharding_info_.is_match_all()) {
        /*do nothing*/
      } else if (OB_FAIL(allocate_groupby_below(
                     distinct_exprs, should_push_distinct, exchange_point, group_push_down_replaced_exprs))) {
        LOG_WARN("failed to allocate group by below op.", K(ret));
      } else if (OB_ISNULL(exchange_point)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exchange_point->allocate_grouping_style_exchange_below(ctx, group_exprs_))) {
        LOG_WARN("failed to allocate exchange below op.", K(ret));
      } else if (OB_FAIL(sharding_info_.copy_with_part_keys(exchange_point->get_sharding_info()))) {
        LOG_WARN("failed to copy sharding info", K(ret));
      } else if (OB_FAIL(append(ctx->group_push_down_replaced_exprs_, group_push_down_replaced_exprs))) {
        LOG_WARN("failed to append group push down replace exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }

  return ret;
}

int ObLogGroupBy::should_push_down_group_by(
    AllocExchContext& ctx, ObIArray<ObRawExpr*>& distinct_exprs, bool& should_push_groupby, bool& should_push_distinct)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child_op = NULL;
  ObSQLSessionInfo* session_info = NULL;
  should_push_groupby = false;
  should_push_distinct = false;
  int64_t distinct_aggr_count = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child_op = get_child(first_child)) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child_op), K(get_plan()), K(session_info), K(ret));
  } else if (OB_FAIL(session_info->if_aggr_pushdown_allowed(should_push_groupby))) {
    LOG_WARN("fail to get aggr_pushdown_allowed", K(ret));
  } else if (!should_push_groupby) {
    /*do nothing*/
  } else {
    should_push_groupby = true;
    // check whether contain agg expr can not be pushed down
    for (int64_t i = 0; OB_SUCC(ret) && should_push_groupby && i < aggr_exprs_.count(); ++i) {
      ObAggFunRawExpr* aggr_expr = NULL;
      if (OB_ISNULL(aggr_exprs_.at(i)) || OB_UNLIKELY(!aggr_exprs_.at(i)->is_aggr_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(aggr_exprs_.at(i)));
      } else if (FALSE_IT(aggr_expr = static_cast<ObAggFunRawExpr*>(aggr_exprs_.at(i)))) {
        /*do nothing*/
      } else if (T_FUN_GROUP_CONCAT == aggr_expr->get_expr_type() || T_FUN_GROUP_RANK == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_DENSE_RANK == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_PERCENT_RANK == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_CUME_DIST == aggr_expr->get_expr_type() || T_FUN_MEDIAN == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_PERCENTILE_CONT == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_PERCENTILE_DISC == aggr_expr->get_expr_type() ||
                 T_FUN_AGG_UDF == aggr_expr->get_expr_type() || T_FUN_KEEP_MAX == aggr_expr->get_expr_type() ||
                 T_FUN_KEEP_MIN == aggr_expr->get_expr_type() || T_FUN_KEEP_SUM == aggr_expr->get_expr_type() ||
                 T_FUN_KEEP_COUNT == aggr_expr->get_expr_type() || T_FUN_KEEP_WM_CONCAT == aggr_expr->get_expr_type() ||
                 T_FUN_WM_CONCAT == aggr_expr->get_expr_type() ||
                 T_FUN_JSON_ARRAYAGG == aggr_expr->get_expr_type() ||
                 T_FUN_JSON_OBJECTAGG == aggr_expr->get_expr_type()) {
        should_push_groupby = false;
      } else if (!aggr_expr->is_param_distinct() && !distinct_exprs.empty()) {
        should_push_groupby = false;
      } else if (!aggr_expr->is_param_distinct()) {
        /*do nothing*/
      } else if (distinct_exprs.empty()) {
        if (OB_FAIL(append(distinct_exprs, aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to append expr", K(ret));
        } else {
          ++distinct_aggr_count;
        }
      } else {
        // check expr are the same
        should_push_groupby =
            ObOptimizerUtil::subset_exprs(
                distinct_exprs, aggr_expr->get_real_param_exprs(), child_op->get_ordering_output_equal_sets()) &&
            ObOptimizerUtil::subset_exprs(
                aggr_expr->get_real_param_exprs(), distinct_exprs, child_op->get_ordering_output_equal_sets());
        ++distinct_aggr_count;
      }
    }
    if (OB_SUCC(ret) && distinct_aggr_count > 0 && distinct_aggr_count < aggr_exprs_.count()) {
      should_push_groupby = false;
    }
    if (OB_SUCC(ret) && should_push_groupby) {
      if (SCALAR_AGGREGATE == algo_ || distinct_card_ <= OB_DOUBLE_EPSINON) {
        should_push_groupby = true;
      } else if (OB_FAIL(check_fulfill_cut_ratio_condition(ctx.parallel_, distinct_card_, should_push_groupby))) {
        LOG_WARN("failed to check fulfill cut ratio condition", K(ret));
      }
    }
    if (OB_SUCC(ret) && should_push_groupby && !distinct_exprs.empty() && get_group_by_exprs().empty()) {
      if (OB_FAIL(check_sharding_compatible_with_reduce_expr(child_op->get_sharding_info(),
              distinct_exprs,
              child_op->get_sharding_output_equal_sets(),
              should_push_distinct))) {
        LOG_WARN("failed to check sharding compatible with reduce expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogGroupBy::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
    LOG_WARN("failed to get non exchange child id", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, group_exprs_, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, rollup_exprs_, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, aggr_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, avg_div_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, approx_count_distinct_estimate_ndv_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> temp_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); i++) {
      ObRawExpr* raw_expr = NULL;
      if (OB_ISNULL(raw_expr = aggr_exprs_.at(i)) || OB_ISNULL(get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(raw_expr), K(get_plan()));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < raw_expr->get_param_count(); j++) {
          bool should_add = false;
          ObRawExpr* param_expr = raw_expr->get_param_expr(j);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(param_expr));
          } else if (OB_FAIL(check_param_expr_should_be_added(param_expr, should_add))) {
            LOG_WARN("failed to check whether param expr should be added", K(ret));
          } else if (should_add && OB_FAIL(temp_exprs.push_back(param_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, temp_exprs, producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
        LOG_WARN("failed to allocate expr pre", K(ret));
      } else { /*do nothing*/
      }
    }

    // set producer id for aggr expr
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
      ExprProducer& producer = ctx.expr_producers_.at(i);
      if (OB_ISNULL(producer.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_INVALID_ID == producer.producer_id_ && producer.expr_->has_flag(IS_AGG) &&
                 is_my_aggr_expr(producer.expr_)) {
        producer.producer_id_ = id_;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObLogGroupBy::is_my_aggr_expr(const ObRawExpr* expr)
{
  return ObOptimizerUtil::find_item(aggr_exprs_, expr);
}

uint64_t ObLogGroupBy::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = ObOptimizerUtil::hash_exprs(hash_value, group_exprs_);
  hash_value = ObOptimizerUtil::hash_exprs(hash_value, rollup_exprs_);
  hash_value = ObOptimizerUtil::hash_exprs(hash_value, aggr_exprs_);
  hash_value = ObOptimizerUtil::hash_exprs(hash_value, avg_div_exprs_);
  hash_value = ObOptimizerUtil::hash_exprs(hash_value, approx_count_distinct_estimate_ndv_exprs_);
  hash_value = do_hash(algo_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}

// This is invoked for push-down group-by
int ObLogGroupBy::push_down_aggr_exprs_analyze(
    ObIArray<ObRawExpr*>& aggr_exprs, ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_avg_arr)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), KP(get_plan()), KP(opt_ctx));
  } else {
    int64_t N = aggr_exprs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExprPushDownAggrExpr push_down_visitor(opt_ctx->get_session_info(), opt_ctx->get_expr_factory());
      if (OB_ISNULL(aggr_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr_exprs.at(i) returns null", K(ret));
      } else if (OB_FAIL(push_down_visitor.analyze(*aggr_exprs.at(i)))) {
        LOG_WARN("analyze aggr push down failed", K(ret));
      } else if (0 == push_down_visitor.get_exprs_count()) {
        LOG_TRACE("expression can be pushed down directly to sub-group-by", K(aggr_exprs.at(i)), K(i));
        ret = aggr_exprs_.push_back(aggr_exprs.at(i));
      } else {
        // During the push-down analysis, we check if we need to generate new expressions
        // ie - in the case of avg, we have to generate sum() and count() to push down
        // instead. If we have not, we may use the expressions directly; otherwise, we
        // have to remove the expression from the aggregation and output expressions and
        // use the new one instead.
        LOG_TRACE("expression can not be pushed down directly to sub-group-by", K(aggr_exprs.at(i)), K(i));
        const ObIArray<ObRawExpr*>& new_expr = push_down_visitor.get_new_exprs();
        if (OB_FAIL(append(aggr_exprs_, new_expr))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (T_FUN_AVG == aggr_exprs.at(i)->get_expr_type()) {
          ObOpRawExpr* push_down_avg_expr = push_down_visitor.get_push_down_avg_expr();
          if (OB_ISNULL(push_down_avg_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("push down avg expr is NULL", K(i), K(ret));
          } else if (OB_FAIL(push_down_avg_arr.push_back(std::make_pair(aggr_exprs.at(i), push_down_avg_expr)))) {
            LOG_WARN("failed to push back expr", K(i), K(ret));
          } else { /*do nothing*/
          }
        } else if (T_FUN_APPROX_COUNT_DISTINCT == aggr_exprs.at(i)->get_expr_type() ||
                   T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == aggr_exprs.at(i)->get_expr_type()) {
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("INVALID expr type", K(*aggr_exprs.at(i)), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogGroupBy::pull_up_aggr_exprs_analyze(
    ObIArray<ObRawExpr*>& aggr_exprs, common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& ctx_record_arr)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(get_plan()), K(get_stmt()), K(opt_ctx));
  } else {
    int64_t N = aggr_exprs.count();
    ObSEArray<ObRawExpr*, 16> tmp_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExprPullUpAggrExpr pull_up_visitor(opt_ctx->get_expr_factory(), opt_ctx->get_session_info());
      if (OB_ISNULL(aggr_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr_exprs.at(i) returns null", K(ret), K(i));
      } else if (OB_FAIL(pull_up_visitor.analyze(*aggr_exprs.at(i)))) {
        LOG_WARN("failed to do pull up analyze", K(aggr_exprs.at(i)), K(i));
      } else {
        /*
         * In order to generate the correct output, we need to replace the replaced
         * expr in the select list with the new expression, which will be used later
         * during output exprs generation.
         */
        if (pull_up_visitor.get_sum_sum_expr() && pull_up_visitor.get_sum_count_expr()) {
          // This is the case for avg. We put the newly generated sum(sum)
          // and sum(count) aggregation functions into aggr_exprs_ and remember
          // the division function in avg_div_exprs_, which will be used in expr
          // generation but is invisible in the explain and code generation
          if (OB_FAIL(tmp_exprs.push_back(pull_up_visitor.get_sum_sum_expr()))) {
            LOG_WARN("Push back failed for tmp_exprs", K(ret));
          } else if (OB_FAIL(tmp_exprs.push_back(pull_up_visitor.get_sum_count_expr()))) {
            LOG_WARN("Push back failed for tmp_exprs", K(ret));
          } else {
            ret = avg_div_exprs_.push_back(pull_up_visitor.get_new_expr());
          }
        } else if (pull_up_visitor.get_merge_synopsis_expr()) {
          if (OB_FAIL(tmp_exprs.push_back(pull_up_visitor.get_merge_synopsis_expr()))) {
            LOG_WARN("Push back failed for tmp_exprs", K(ret));
          } else if (OB_FAIL(approx_count_distinct_estimate_ndv_exprs_.push_back(pull_up_visitor.get_new_expr()))) {
            LOG_WARN("Push back failed for approx_count_distinct_esitmate_ndv_exprs_", K(ret));
          } else { /* Do nothing */
          }
        } else {
          if (OB_FAIL(tmp_exprs.push_back(pull_up_visitor.get_new_expr()))) {
            LOG_WARN("Push back failed for tmp_exprs", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ctx_record_arr.push_back(std::make_pair(aggr_exprs.at(i), pull_up_visitor.get_new_expr())))) {
            LOG_WARN("failed to push back to ctx record array", K(ret));
          } else { /* Do nothing */
          }
        } else { /* Do nothing */
        }
      }
    }

    if (OB_SUCC(ret)) {
      ret = ObOptimizerUtil::copy_exprs(opt_ctx->get_expr_factory(), tmp_exprs, aggr_exprs_);
    } else { /* Do nothing */
    }
  }

  return ret;
}

int ObLogGroupBy::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& group = get_group_by_exprs();
    EXPLAIN_PRINT_EXPRS(group, type);
    const ObIArray<ObRawExpr*>& rollup = get_rollup_exprs();
    if (OB_SUCC(ret) && (rollup.count() > 0)) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXPRS(rollup, type);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      const ObIArray<ObRawExpr*>& agg_func = get_aggr_funcs();
      EXPLAIN_PRINT_EXPRS(agg_func, type);
    }
  }
  return ret;
}

int ObLogGroupBy::produce_pushed_down_expressions(ObLogGroupBy* child_group_by,
    ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& ctx_record_arr,
    ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_avg_arr)
{
  int ret = OB_SUCCESS;
  /*
   *             group by
   *               |
   *              (sort)
   *               |
   *              ex in
   *               |
   *              ex out
   *               |
   *           group by(child)
   *               |
   *              (sort)
   */
  if (OB_ISNULL(child_group_by)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else if (OB_FAIL(child_group_by->push_down_aggr_exprs_analyze(aggr_exprs_, push_down_avg_arr))) {
    LOG_WARN("failed to push down aggr exprs", K(ret));
  } else if (OB_FAIL(pull_up_aggr_exprs_analyze(aggr_exprs_, ctx_record_arr))) {
    LOG_WARN("Failed to analyze pull up aggr exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogGroupBy::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  // group by exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_.count(); ++i) {
    if (OB_ISNULL(group_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group_exprs_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*group_exprs_.at(i)))) {
      LOG_WARN("failed to check group_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  // rollup exprs.
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs_.count(); ++i) {
    if (OB_ISNULL(rollup_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rollup_exprs.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*rollup_exprs_.at(i)))) {
      LOG_WARN("failed to check rollup_exprs.at(i)", K(ret), K(i));
    } else {
    }
  }
  // aggregations
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); ++i) {
    if (OB_ISNULL(aggr_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_exprs_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*aggr_exprs_.at(i)))) {
      LOG_WARN("failed to check aggr_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  // avg->div expressions
  for (int64_t i = 0; OB_SUCC(ret) && i < avg_div_exprs_.count(); ++i) {
    if (OB_ISNULL(avg_div_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("avg_div_exprs_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*avg_div_exprs_.at(i)))) {
      LOG_WARN("failed to check avg_div_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  // approx_count_distinct->estimate_ndv expressions
  for (int64_t i = 0; i < OB_SUCC(ret) && i < approx_count_distinct_estimate_ndv_exprs_.count(); ++i) {
    if (OB_ISNULL(approx_count_distinct_estimate_ndv_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("approx_count_distinct_estimate_ndv_exprs_.at(i) si null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*approx_count_distinct_estimate_ndv_exprs_.at(i)))) {
      LOG_WARN("failed to check approx_count_distinct_estimate_ndv_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  return ret;
}

int ObLogGroupBy::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  reset_local_ordering();
  if (HASH_AGGREGATE == get_algo() || SCALAR_AGGREGATE == get_algo()) {
    // do nothing
  } else {
    ObLogicalOperator* child = get_child(first_child);
    bool need_sort = false;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret));
    } else if (OB_FAIL(check_need_sort_for_grouping_op(0, expected_ordering_, need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (need_sort && OB_FAIL(allocate_sort_below(0, expected_ordering_))) {  // need alloc sort
      LOG_WARN("failed to allocate implicit sort", K(ret));
    } else if (has_rollup()) {
      ObSEArray<OrderItem, 4> ordering;
      for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_.count(); i++) {
        if (OB_FAIL(ordering.push_back(expected_ordering_.at(i)))) {
          LOG_WARN("failed to push back into ordering.", K(ret));
        } else {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(set_op_ordering(ordering))) {
        LOG_WARN("failed to set op ordering.", K(ret));
      } else {
      }
    } else if (FALSE_IT(child = get_child(first_child))) {
    } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op's ordering", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObLogGroupBy::est_cost()
{
  int ret = OB_SUCCESS;
  double distinct_card = 0.0;
  double selectivity = 1.0;
  double group_cost = 0.0;

  common::ObSEArray<ObRawExpr*, 8> group_rollup_exprs;
  ObLogicalOperator* child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_est_sel_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if (OB_FAIL(get_group_rollup_exprs(group_rollup_exprs))) {
    LOG_WARN("failed to get group rollup exprs.", K(ret));
  } else if (group_rollup_exprs.empty() || SCALAR_AGGREGATE == algo_) {
    distinct_card = 1.0;
  } else if (OB_FAIL(ObOptEstSel::calculate_distinct(
                 child->get_card(), *get_est_sel_info(), group_rollup_exprs, distinct_card))) {
    LOG_WARN("failed to calculate distinct", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (SCALAR_AGGREGATE == algo_) {
      group_cost = ObOptEstCost::cost_scalar_group(child->get_card(), get_aggr_funcs().count());
    } else if (MERGE_AGGREGATE == algo_) {
      group_cost = ObOptEstCost::cost_merge_group(
          child->get_card(), distinct_card, group_rollup_exprs, get_aggr_funcs().count());
    } else {
      group_cost =
          ObOptEstCost::cost_hash_group(child->get_card(), distinct_card, group_exprs_, get_aggr_funcs().count());
    }
    // process having filters
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(*get_est_sel_info(),
            get_filter_exprs(),
            selectivity,
            &get_plan()->get_predicate_selectivities(),
            UNKNOWN_JOIN,
            NULL,
            NULL,
            child->get_card(),
            distinct_card))) {
      LOG_WARN("Failed to calculate selectivity", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    distinct_card_ = distinct_card;
    set_card(distinct_card * selectivity);
    set_cost(child->get_cost() + group_cost);
    set_op_cost(group_cost);
    set_width(child->get_width());
  }
  return ret;
}

int ObLogGroupBy::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child_op = NULL;
  UNUSED(parent);
  re_est = false;
  common::ObSEArray<ObRawExpr*, 8> group_rollup_exprs;
  if (OB_ISNULL(child_op = get_child(0)) || OB_ISNULL(get_est_sel_info()) || OB_UNLIKELY(need_row_count < 0.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(child_op), K(get_est_sel_info()), K(need_row_count), K(ret));
  } else if (need_row_count >= get_card() || get_card() <= OB_DOUBLE_EPSINON) {
    // need row count >= card_, need not re_est cost
  } else if (SCALAR_AGGREGATE == algo_) {
    // do nothing
  } else if (HASH_AGGREGATE == algo_) {
    re_est = true;
    distinct_card_ *= need_row_count / get_card();
    double group_cost =
        ObOptEstCost::cost_hash_group(child_op->get_card(), distinct_card_, group_exprs_, aggr_exprs_.count());
    card_ = need_row_count;
    op_cost_ = group_cost;
    cost_ = group_cost + child_op->get_cost();
  } else if (OB_FAIL(get_group_rollup_exprs(group_rollup_exprs))) {
    LOG_WARN("failed to get group rollup exprs.", K(ret));
  } else if (MERGE_AGGREGATE == algo_) {
    bool child_re_est = false;
    double child_need_card = need_row_count * child_op->get_card() / get_card();
    if (OB_FAIL(child_op->re_est_cost(this, child_need_card, child_re_est))) {
      LOG_WARN("failed to re_est_cost", K(ret));
    } else {
      re_est = true;
      distinct_card_ *= need_row_count / get_card();
      double group_cost =
          ObOptEstCost::cost_merge_group(child_op->get_card(), distinct_card_, group_rollup_exprs, aggr_exprs_.count());
      card_ = need_row_count;
      op_cost_ = group_cost;
      cost_ = group_cost + child_op->get_cost();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggregation type", K(algo_), K(ret));
  }
  return ret;
}

int ObLogGroupBy::inner_replace_generated_agg_expr(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_group_by_exprs()))) {
    LOG_WARN("failed to extract subplan params in log group by exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_rollup_exprs()))) {
    LOG_WARN("failed to extract subplan params in log rollup exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_aggr_funcs()))) {
    LOG_WARN("failed to extract subplan params in log agg funcs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, avg_div_exprs_))) {
    LOG_WARN("failed to extract subplan params in avg_div_exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, approx_count_distinct_estimate_ndv_exprs_))) {
    LOG_WARN("failed to extract subplan params in approx_count_distinct_estimate_ndv_exprs", K(ret));
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogGroupBy::print_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_one_line = plan_text.is_oneline_;
  OutlineType type = plan_text.outline_type_;
  bool is_need = false;
  ObString stmt_name;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(get_stmt()));
  } else if (USED_HINT == type && OB_FAIL(get_stmt()->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret));
  } else if (OUTLINE_DATA == type && OB_FAIL(get_stmt()->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    bool is_need = false;
    if (OB_FAIL(is_need_print_agg_type(plan_text, stmt_hint, is_need))) {
      LOG_WARN("failed to get the aggregate type", K(ret));
    } else if (!is_need) {  // do nothing.
    } else if (MERGE_AGGREGATE == algo_) {
      if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
      } else if (OB_FAIL(BUF_PRINTF(
                     "%s(@\"%.*s\")", ObStmtHint::NO_USE_HASH_AGGREGATE, stmt_name.length(), stmt_name.ptr()))) {
        LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
      } else { /*do nohting*/
      }
    } else if (HASH_AGGREGATE == algo_) {
      if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
      } else if (OB_FAIL(BUF_PRINTF(
                     "%s(@\"%.*s\")", ObStmtHint::USE_HASH_AGGREGATE, stmt_name.length(), stmt_name.ptr()))) {
        LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
      } else { /*do nohting*/
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogGroupBy::is_need_print_agg_type(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = true;
  } else if (USED_HINT == outline_type) {
    is_need = (stmt_hint.aggregate_ != OB_UNSET_AGGREGATE_TYPE);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogGroupBy::allocate_topk_if_needed(ObLogicalOperator* exchange_point, const ObLogGroupBy* child_group_by,
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_avg_arr)
{
  /*
   *        merge group by                                hash group by
   *             |                                              |
   *            sort                                          ex in
   *             |                                              |
   *            ex in                                         ex in
   *             |                                              |
   *            ex out                                         topk
   *             |                                              |
   *             topk                                           |
   *          /         \                                  /         \
   *    sort             material                      sort           \
   *      |                |                           |                \
   * group by(child)    group by(child)       hash group by(child)    hash group by(child)
   *      |                |
   *    (sort)           (sort)
   */
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = NULL;
  if (OB_ISNULL(exchange_point) || OB_ISNULL(child_group_by)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exchange_point or child_group_by is NULL", KPC(exchange_point), KPC(child_group_by), K(ret));
  } else if (!get_stmt()->is_select_stmt()) {
    // do nothing, insert/update/delete returning can produce scalar group by
  } else if (OB_ISNULL(stmt = static_cast<ObSelectStmt*>(get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_match_topk()) {
    ObLogicalOperator* parent_op = get_parent();
    ObLogSort* order_by_sort = NULL;
    ObArray<OrderItem> topk_sort_keys;
    bool need_sort = true;
    if (OB_ISNULL(parent_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent_op should not be NULL", K(ret));
    } else if (log_op_def::LOG_SORT != parent_op->get_type()) {
      need_sort = false;
    } else {
      // topk do not support distinct,so the sort here must be order by
      order_by_sort = static_cast<ObLogSort*>(parent_op);
      if (OB_ISNULL(order_by_sort)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order_by_sort is null", K(ret));
      } else if (OB_FAIL(order_by_sort->clone_sort_keys_for_topk(topk_sort_keys, push_down_avg_arr))) {
        LOG_WARN("failed to clone_sort_key", K(ret));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret)) {
      if (MERGE_AGGREGATE == get_algo()) {
        if (need_sort) {
          if (OB_FAIL(exchange_point->allocate_sort_below(first_child, topk_sort_keys))) {
            LOG_WARN("Failed to allocate sort", K(ret));
          } else { /*do nothing*/
          }
        } else if (OB_FAIL(exchange_point->allocate_material_below(first_child))) {
          LOG_WARN("failed to allocate material below", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(exchange_point->allocate_topk_below(first_child, stmt))) {
            LOG_WARN("failed to allocate top below", K(ret));
          } else { /*do nothing*/
          }
        }
      } else if (HASH_AGGREGATE == get_algo()) {
        if (need_sort) {
          ObLogSort* topk_sort = NULL;  // topk_sort
          ObStmtHint& stmt_hint = stmt->get_stmt_hint();
          if (OB_FAIL(exchange_point->allocate_sort_below(first_child, topk_sort_keys))) {
            LOG_WARN("failed to allocate child sort by", K(ret));
          } else if (OB_ISNULL(topk_sort = static_cast<ObLogSort*>(exchange_point->get_child(first_child)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("topk_sort is null", K(ret));
          } else if (OB_FAIL(topk_sort->set_topk_params(stmt->get_limit_expr(),
                         stmt->get_offset_expr(),
                         stmt_hint.sharding_minimum_row_count_,
                         stmt_hint.topk_precision_))) {
            LOG_WARN("failed to set_topk_params", K(ret));
          } else {
            LOG_INFO("failed to do agg expr replace op", K(topk_sort->get_sort_keys()));
            topk_sort->set_cost(get_cost());
            topk_sort->set_op_cost(get_op_cost());
            topk_sort->set_card(get_card());
          }
        } else if (OB_FAIL(exchange_point->allocate_topk_below(first_child, stmt))) {
          LOG_WARN("failed to allocate top below", K(ret));
        }
      } else { /*no need allocate topk*/
      }
    }
  }
  return ret;
}

int ObLogGroupBy::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (OB_FAIL(link_stmt->try_fill_select_strs(output_exprs_))) {
    LOG_WARN("failed to fill link stmt select strs", K(ret), K(output_exprs_));
  } else if (OB_FAIL(link_stmt->fill_groupby_strs(group_exprs_))) {
    LOG_WARN("failed to fill link stmt groupby strs", K(ret), K(group_exprs_));
  } else if (OB_FAIL(link_stmt->fill_having_strs(filter_exprs_))) {
    LOG_WARN("failed to fill link stmt having strs", K(ret), K(filter_exprs_));
  }
  return ret;
}

int ObLogGroupBy::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (!has_rollup() && OB_FAIL(append(get_output_const_exprs(), child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_filter_exprs(), get_output_const_exprs()))) {
    LOG_WARN("failed to compute const conditionexprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogGroupBy::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  ObFdItemSet* fd_item_set = NULL;
  ObTableFdItem* fd_item = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else if (has_rollup()) {
    // do nothing
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else if (OB_FAIL(fd_item_set->assign(child->get_fd_item_set()))) {
    LOG_WARN("failed to assign fd item set", K(ret));
  } else if (group_exprs_.empty()) {
    // scalar group by
    if (get_stmt()->is_select_stmt() && OB_FAIL(create_fd_item_from_select_list(fd_item_set))) {
      LOG_WARN("failed to create fd item from select list", K(ret));
    }
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(
                 fd_item, true, group_exprs_, get_stmt()->get_current_level(), get_table_set()))) {
    LOG_WARN("failed to create fd item", K(ret));
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_NOT_NULL(fd_item_set) && OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogGroupBy::create_fd_item_from_select_list(ObFdItemSet* fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  int32_t stmt_level = -1;
  ObTableFdItem* fd_item = NULL;
  if (OB_ISNULL(fd_item_set) || OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect parameter", K(ret), K(fd_item_set), K(my_plan_), K(get_stmt()));
  } else if (OB_FAIL(static_cast<ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else {
    stmt_level = get_stmt()->get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 1> value_exprs;
    if (OB_FAIL(value_exprs.push_back(select_exprs.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(
                   fd_item, true, value_exprs, stmt_level, get_table_set()))) {
      LOG_WARN("failed to create fd item", K(ret));
    } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
      LOG_WARN("failed to push back fd item", K(ret));
    }
  }
  return ret;
}

int ObLogGroupBy::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (HASH_AGGREGATE == algo_) {
    // do nothing
    reset_op_ordering();
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (has_rollup()) {
    ObSEArray<OrderItem, 4> ordering;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_.count(); i++) {
      if (OB_FAIL(ordering.push_back(child->get_op_ordering().at(i)))) {
        LOG_WARN("failed to push back into ordering.", K(ret));
      } else {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(set_op_ordering(ordering))) {
      LOG_WARN("failed to set op ordering.", K(ret));
    } else {
    }
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogGroupBy::allocate_granule_pre(AllocGIContext& ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogGroupBy::allocate_granule_post(AllocGIContext& ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogGroupBy::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs_.count(); ++i) {
    ObRawExpr* expr = NULL;
    switch (aggr_exprs_.at(i)->get_expr_type()) {
      case T_FUN_GROUP_CONCAT: {
        expr = static_cast<ObAggFunRawExpr*>(aggr_exprs_.at(i))->get_separator_param_expr();
        OZ(raw_exprs.append(expr));
        break;
      }
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT: {
        expr = static_cast<ObAggFunRawExpr*>(aggr_exprs_.at(i))->get_linear_inter_expr();
        OZ(raw_exprs.append(expr));
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObLogGroupBy::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  if (group_exprs_.empty() && rollup_exprs_.empty()) {
    set_is_at_most_one_row(true);
  } else if (OB_FAIL(ObLogicalOperator::compute_one_row_info())) {
    LOG_WARN("failed to compute one row info", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogGroupBy::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (SCALAR_AGGREGATE == algo_) {
    // do nothing
  } else if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post())) {
    LOG_WARN("failed to allocate startup exprs post", K(ret));
  }
  return ret;
}
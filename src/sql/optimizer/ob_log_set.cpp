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
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "ob_opt_est_cost.h"
#include "ob_join_order.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

ObSelectLogPlan* ObLogSet::get_left_plan() const
{
  ObSelectLogPlan* ret = NULL;
  ObLogicalOperator* left_child = get_child(ObLogicalOperator::first_child);
  if (!OB_ISNULL(left_child) && !OB_ISNULL(left_child->get_plan()) && !OB_ISNULL(left_child->get_plan()->get_stmt()) &&
      left_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<ObSelectLogPlan*>(left_child->get_plan());
  }
  return ret;
}

ObSelectLogPlan* ObLogSet::get_right_plan() const
{
  ObSelectLogPlan* ret = NULL;
  ObLogicalOperator* right_child = get_child(ObLogicalOperator::second_child);
  if (!OB_ISNULL(right_child) && !OB_ISNULL(right_child->get_plan()) &&
      !OB_ISNULL(right_child->get_plan()->get_stmt()) && right_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<ObSelectLogPlan*>(right_child->get_plan());
  }
  return ret;
}

ObSelectStmt* ObLogSet::get_left_stmt() const
{
  ObSelectStmt* ret = NULL;
  ObLogicalOperator* left_child = get_child(ObLogicalOperator::first_child);
  if (!OB_ISNULL(left_child) && !OB_ISNULL(left_child->get_plan()) && !OB_ISNULL(left_child->get_plan()->get_stmt()) &&
      left_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<ObSelectStmt*>(left_child->get_plan()->get_stmt());
  }
  return ret;
}

ObSelectStmt* ObLogSet::get_right_stmt() const
{
  ObSelectStmt* ret = NULL;
  ObLogicalOperator* right_child = get_child(ObLogicalOperator::second_child);
  if (!OB_ISNULL(right_child) && !OB_ISNULL(right_child->get_plan()) &&
      !OB_ISNULL(right_child->get_plan()->get_stmt()) && right_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<ObSelectStmt*>(right_child->get_plan()->get_stmt());
  }
  return ret;
}

const char* ObLogSet::get_name() const
{
  static const char* set_op_all[ObSelectStmt::SET_OP_NUM] = {
      "NONE",
      "UNION ALL",
      "INTERSECT ALL",
      "EXCEPT ALL",
      "RECURSIVE UNION ALL",
  };
  static const char* merge_set_op_distinct[ObSelectStmt::SET_OP_NUM] = {
      "NONE",
      "MERGE UNION DISTINCT",
      "MERGE INTERSECT DISTINCT",
      "MERGE EXCEPT DISTINCT",
  };
  static const char* hash_set_op_distinct[ObSelectStmt::SET_OP_NUM] = {
      "NONE",
      "HASH UNION DISTINCT",
      "HASH INTERSECT DISTINCT",
      "HASH EXCEPT DISTINCT",
  };
  const char* ret_char = "Unknown type";
  if (set_op_ >= 0 && set_op_ < ObSelectStmt::SET_OP_NUM && set_algo_ != INVALID_SET_ALGO) {
    ret_char = !is_distinct_ ? set_op_all[set_op_]
                             : (HASH_SET == set_algo_ ? hash_set_op_distinct[set_op_] : merge_set_op_distinct[set_op_]);
    if (is_recursive_union_) {
      ret_char = set_op_all[ObSelectStmt::SetOperator::RECURSIVE];
    }
  } else { /* Do nothing */
  }
  return ret_char;
}

int ObLogSet::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogSet* set = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogSet", K(ret));
  } else if (OB_ISNULL(set = static_cast<ObLogSet*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator to ObLogSet", K(ret));
  } else if (OB_FAIL(set->set_set_directions(set_directions_))) {
    LOG_WARN("failed to set cmp directions for copy", K(ret));
  } else if (OB_FAIL(append(set->left_expected_ordering_, left_expected_ordering_))) {
    LOG_WARN("failed to set left expected_ordering for copy", K(ret));
  } else if (OB_FAIL(append(set->right_expected_ordering_, right_expected_ordering_))) {
    LOG_WARN("failed to set right expected_ordering for copy", K(ret));
  } else if (OB_FAIL(append(set->map_array_, map_array_))) {
    LOG_WARN("failed to append map array", K(ret));
  } else if (OB_FAIL(append(set->search_ordering_, search_ordering_))) {
    LOG_WARN("failed to set search ordering for copy", K(ret));
  } else if (OB_FAIL(append(set->cycle_items_, cycle_items_))) {
    LOG_WARN("failed to set cycle ordering for copy", K(ret));
  } else if (OB_FAIL(append(set->domain_index_exprs_, domain_index_exprs_))) {
    LOG_WARN("failed to copy domain index exprs", K(ret));
  } else {
    set->is_distinct_ = is_distinct_;
    set->is_recursive_union_ = is_recursive_union_;
    set->is_breadth_search_ = is_breadth_search_;
    set->set_algo_ = set_algo_;
    set->set_dist_algo_ = set_dist_algo_;
    set->set_op_ = set_op_;
    out = set;
  }
  return ret;
}

int ObLogSet::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(my_plan_), K(left_child), K(right_child));
  } else if ((ObSelectStmt::EXCEPT == get_set_op() || ObSelectStmt::INTERSECT == get_set_op()) &&
             OB_FAIL(append(const_exprs_, left_child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (ObSelectStmt::INTERSECT == get_set_op() &&
             OB_FAIL(append(const_exprs_, right_child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (ObSelectStmt::UNION == get_set_op()) {

  } else {
    /*do nothing*/
  }
  return ret;
}

int ObLogSet::compute_equal_set()
{
  int ret = OB_SUCCESS;
  EqualSets* ordering_esets = NULL;
  EqualSets temp_ordering_esets;
  ObSEArray<ObRawExpr*, 8> ordering_eset_conditions;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret), K(get_plan()));
  } else if (!is_set_distinct()) {
    // do nothing
  } else if (OB_FAIL(get_equal_set_conditions(ordering_eset_conditions))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (ObSelectStmt::UNION == get_set_op()) {
    // do nothing
  } else if (ObSelectStmt::EXCEPT == get_set_op()) {
    if (OB_FAIL(temp_ordering_esets.assign(get_child(first_child)->get_ordering_output_equal_sets()))) {
      LOG_WARN("failed to get ordering output equal sets", K(ret));
    }
  } else if (OB_FAIL(get_ordering_input_equal_sets(temp_ordering_esets))) {
    LOG_WARN("failed to get ordering input equal sets", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create equal sets", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                   &get_plan()->get_allocator(), ordering_eset_conditions, temp_ordering_esets, *ordering_esets))) {
      LOG_WARN("failed to compute sharding output equal set", K(ret));
    } else {
      set_ordering_output_equal_sets(ordering_esets);
    }
  }
  return ret;
}

int ObLogSet::get_equal_set_conditions(ObIArray<ObRawExpr*>& equal_conds)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  ObSEArray<ObRawExpr*, 8> set_exprs;
  ObSEArray<ObSelectStmt*, 2> child_stmts;
  ObSelectStmt* stmt = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(get_stmt()), K(session_info), K(ret));
  } else if (OB_FALSE_IT(stmt = static_cast<ObSelectStmt*>(get_stmt()))) {
  } else if (!domain_index_exprs_.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_expr_in_cast(select_exprs, set_exprs))) {
    LOG_WARN("failed to expr in cast", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_equal_set_conditions(
                 *expr_factory, session_info, stmt, set_exprs, equal_conds))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else {
    LOG_TRACE("succeed to get equal set conditions for set op", K(equal_conds));
  }
  return ret;
}

int ObLogSet::deduce_const_exprs_and_ft_item_set(ObFdItemSet& fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  if (OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().deduce_fd_item_set(
                 get_ordering_output_equal_sets(), select_exprs, get_output_const_exprs(), fd_item_set))) {
    LOG_WARN("falied to remove const in fd item", K(ret));
  }
  return ret;
}

int ObLogSet::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObFdItemSet* fd_item_set = NULL;
  ObExprFdItem* fd_item = NULL;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(my_plan_), K(left_child), K(right_child));
  } else if (!is_set_distinct()) {
    // do nothing
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else if (OB_FAIL(get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_expr_fd_item(
                 fd_item, true, select_exprs, get_stmt()->get_current_level(), select_exprs))) {
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  } else if ((ObSelectStmt::INTERSECT == set_op_ || ObSelectStmt::EXCEPT == set_op_) &&
             OB_FAIL(append(*fd_item_set, left_child->get_fd_item_set()))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (ObSelectStmt::INTERSECT == set_op_ && OB_FAIL(append(*fd_item_set, right_child->get_fd_item_set()))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogSet::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  if (MERGE_SET == set_algo_ && is_set_distinct()) {
    ObSEArray<OrderItem, 8> ordering;
    ;
    // only merge union generate sorted output
    ObArray<ObRawExpr*> mapped_exprs;
    ObSEArray<ObRawExpr*, 8> select_exprs;
    if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret), K(get_stmt()));
    } else if (OB_FAIL(static_cast<ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (map_array_.count() != select_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid mapping array", K(map_array_), K(select_exprs));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(select_exprs, map_array_, mapped_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(mapped_exprs, set_directions_, ordering))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(set_op_ordering(ordering))) {
      LOG_WARN("failed to set op ordering", K(ret));
    }
  }
  return ret;
}

int ObLogSet::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  if (OB_ISNULL(left_child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(right_child = get_child(ObLogicalOperator::second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (set_op_ == ObSelectStmt::UNION || is_recursive_union()) {
    set_is_at_most_one_row(false);
  } else if (set_op_ == ObSelectStmt::INTERSECT) {
    set_is_at_most_one_row((left_child->get_is_at_most_one_row() || right_child->get_is_at_most_one_row()));
  } else if (set_op_ == ObSelectStmt::EXCEPT) {
    set_is_at_most_one_row(left_child->get_is_at_most_one_row());
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogSet::inner_replace_generated_agg_expr(
    const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  FOREACH_X(it, left_expected_ordering_, OB_SUCC(ret))
  {
    if (OB_FAIL(replace_expr_action(to_replace_exprs, it->expr_))) {
      LOG_WARN("replace generated agg expr failed", K(ret));
    }
  }
  FOREACH_X(it, right_expected_ordering_, OB_SUCC(ret))
  {
    if (OB_FAIL(replace_expr_action(to_replace_exprs, it->expr_))) {
      LOG_WARN("replace generated agg expr failed", K(ret));
    }
  }
  return ret;
}

int ObLogSet::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  uint64_t candidate_method = 0;
  EqualSets sharding_esets;
  // set keys and types for hash-hash distribution
  ObSEArray<ObRawExpr*, 4> hash_left_set_keys;
  ObSEArray<ObRawExpr*, 4> hash_right_set_keys;
  ObSEArray<ObExprCalcType, 4> hash_calc_types;
  // set keys for non-hash-hash distribution
  ObSEArray<ObRawExpr*, 4> left_set_keys;
  ObSEArray<ObRawExpr*, 4> right_set_keys;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if ((!is_distinct_ && ObSelectStmt::UNION == set_op_)) {
    if (OB_FAIL(allocate_exchange_for_union_all(ctx))) {
      LOG_WARN("failed to allocate exchange for union all", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_UNLIKELY(2 != get_num_of_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected num of child", K(ret), K(get_num_of_child()));
  } else if ((OB_ISNULL(left_child = get_child(first_child)) || (OB_ISNULL(right_child = get_child(second_child))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (OB_FAIL(get_set_child_exprs(hash_left_set_keys, hash_right_set_keys))) {
    LOG_WARN("failed to get unique key", K(ret));
  } else if (OB_FAIL(get_calc_types(hash_calc_types))) {
    LOG_WARN("failed to get calc types", K(ret));
  } else if (OB_FAIL(
                 prune_weak_part_exprs(*ctx, hash_left_set_keys, hash_right_set_keys, left_set_keys, right_set_keys))) {
    LOG_WARN("failed to prune weak part exprs", K(ret));
  } else if (OB_FAIL(get_sharding_input_equal_sets(sharding_esets))) {
    LOG_WARN("failed to get sharding input equal sets", K(ret));
  } else if (OB_FAIL(get_candidate_join_distribution_method(
                 sharding_esets, left_set_keys, right_set_keys, candidate_method))) {
    LOG_WARN("failed to get candidate join distribution method", K(ret));
  } else if (OB_FAIL(
                 choose_best_distribution_method(*ctx, candidate_method, false, set_dist_algo_, slave_mapping_type_))) {
    LOG_WARN("failed to choose best distribution method", K(ret));
  } else if (OB_FAIL(compute_sharding_and_allocate_exchange(ctx,
                 sharding_esets,
                 hash_left_set_keys,
                 hash_right_set_keys,
                 hash_calc_types,
                 left_set_keys,
                 right_set_keys,
                 *left_child,
                 *right_child,
                 set_dist_algo_,
                 slave_mapping_type_,
                 convert_set_op(),
                 sharding_info_))) {
    LOG_WARN("failed to compute sharding and allocate exchange", K(ret));
  } else {
    is_partition_wise_ = (set_dist_algo_ == JoinDistAlgo::DIST_PARTITION_WISE) && !use_slave_mapping();
  }

  if (OB_SUCC(ret)) {
    sharding_info_.set_can_reselect_replica(false);
    if (OB_FAIL(replace_generated_agg_expr(ctx->group_push_down_replaced_exprs_))) {
      LOG_WARN("failed to replace generated agg expr", K(ret));
    } else if (OB_FAIL(update_sharding_conds(ctx->sharding_conds_))) {
      LOG_WARN("failed to update sharding conds", K(ret));
    }
  }
  return ret;
}

int ObLogSet::allocate_exchange_for_union_all(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  bool can_seq_exec = false;
  ObExchangeInfo exch_info;
  const int64_t num_of_child = get_num_of_child();
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(get_child(first_child)), K(ret));
  } else if (OB_FAIL(check_if_match_partition_wise(is_match))) {
    LOG_WARN("failed to check is physically equal partitioned", K(ret));
  } else if (is_match) {
    if (OB_FAIL(sharding_info_.copy_without_part_keys(get_child(first_child)->get_sharding_info()))) {
      LOG_WARN("failed to copy with sharding info without part keys", K(ret));
    } else {
      is_partition_wise_ = true;
      set_dist_algo_ = DIST_PARTITION_WISE;
    }
  } else if (OB_FAIL(should_use_sequential_execution(can_seq_exec))) {
    LOG_WARN("failed to check should use sequential execution", K(ret));
  } else if (can_seq_exec || get_plan()->get_optimizer_context().get_parallel() <= 1) {
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
    set_dist_algo_ = DIST_PULL_TO_LOCAL;
    ObLogicalOperator* child_op = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < num_of_child; ++i) {
      if (OB_ISNULL(child_op = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
      } else if (child_op->get_sharding_info().is_sharding() && OB_FAIL(child_op->allocate_exchange(ctx, exch_info))) {
        LOG_WARN("failed to allocate exchange", K(ret));
      } else { /*do noting*/
      }
    }
  } else {  // DIST_NONE_RANDOM
    exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
    ObLogicalOperator* left_child = get_child(first_child);
    ObLogicalOperator* other_child = NULL;
    bool need_swap = false;
    if (OB_ISNULL(left_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < num_of_child; ++i) {
      need_swap = false;
      if (OB_ISNULL(other_child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (left_child->get_sharding_info().is_sharding() && other_child->get_sharding_info().is_local()) {
        need_swap = false;
      } else if (left_child->get_sharding_info().is_local() && other_child->get_sharding_info().is_sharding()) {
        need_swap = true;
      } else if (left_child->get_card() < other_child->get_card()) {
        need_swap = true;
      }
      if (OB_SUCC(ret)) {
        if (need_swap) {
          std::swap(left_child, other_child);
        }
        if (OB_FAIL(other_child->allocate_exchange(ctx, exch_info))) {
          LOG_WARN("failed to allocate exchange", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_dist_algo_ = left_child == get_child(first_child) ? DIST_NONE_RANDOM : DIST_RANDOM_NONE;
      ret = sharding_info_.copy_without_part_keys(left_child->get_sharding_info());
    }
  }
  return ret;
}

int ObLogSet::check_if_match_partition_wise(bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  const int64_t num_of_child = get_num_of_child();
  if (num_of_child < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logset get unexpected child of num", K(ret), K(num_of_child));
  } else if (OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_child(first_child)), K(ret));
  } else {
    is_match = true;
    const ObShardingInfo& sharding_info = get_child(first_child)->get_sharding_info();
    for (int64_t i = 1; OB_SUCC(ret) && is_match && i < num_of_child; ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
      } else if (OB_FAIL(ObShardingInfo::is_physically_equal_partitioned(
                     sharding_info, get_child(i)->get_sharding_info(), is_match))) {
        LOG_WARN("failed to check is physically equal partitioned", K(ret));
      }
    }
  }
  return ret;
}

int ObLogSet::get_candidate_join_distribution_method(const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& left_set_keys, const ObIArray<ObRawExpr*>& right_set_keys, uint64_t& candidate_method)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  candidate_method = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(left_child), K(right_child), K(ret));
  } else {
    add_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL);
    add_join_dist_flag(candidate_method, DIST_PARTITION_NONE);
    add_join_dist_flag(candidate_method, DIST_NONE_PARTITION);
    add_join_dist_flag(candidate_method, DIST_PARTITION_WISE);
    add_join_dist_flag(candidate_method, DIST_HASH_HASH);

    if (1 >= get_plan()->get_optimizer_context().get_parallel()) {
      REMOVE_PX_PARALLEL_DFO_DIST_METHOD(candidate_method);
    }

    // remove invalid method
    if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_WISE)) {
      bool is_partition_wise = false;
      if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(*get_plan(),
              equal_sets,
              left_set_keys,
              right_set_keys,
              left_child->get_sharding_info(),
              right_child->get_sharding_info(),
              is_partition_wise))) {
        LOG_WARN("failed to check if match partition wise join", K(ret));
      } else if (!is_partition_wise) {
        remove_join_dist_flag(candidate_method, DIST_PARTITION_WISE);
      }
    }
    if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_NONE)) {
      bool left_match_repart = false;
      bool need_check_unique = (get_set_op() != ObSelectStmt::SetOperator::INTERSECT);
      bool is_left_unique = false;
      if (need_check_unique && OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_set_keys,
                                   left_child->get_table_set(),
                                   left_child->get_fd_item_set(),
                                   left_child->get_ordering_output_equal_sets(),
                                   left_child->get_output_const_exprs(),
                                   is_left_unique))) {
        LOG_WARN("failed to check is left unique", K(ret));
      } else if (need_check_unique && !is_left_unique) {
        // px use random repartition for unmatched rows, hence the left output should be unique
        // other, duplicates can not be removed
        // no-px pulls all unmatched rows to the local
        remove_join_dist_flag(candidate_method, DIST_PARTITION_NONE);
      } else if (OB_FAIL(check_if_match_repart(
                     equal_sets, left_set_keys, right_set_keys, *right_child, left_match_repart))) {
        LOG_WARN("failed to check if match repart", K(ret));
      } else if (!left_match_repart) {
        remove_join_dist_flag(candidate_method, DIST_PARTITION_NONE);
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_NONE_PARTITION)) {
      bool right_match_repart = false;
      bool need_check_unique = (get_set_op() == ObSelectStmt::SetOperator::UNION);
      bool is_right_unique = false;
      if (need_check_unique && OB_FAIL(ObOptimizerUtil::is_exprs_unique(right_set_keys,
                                   right_child->get_table_set(),
                                   right_child->get_fd_item_set(),
                                   right_child->get_ordering_output_equal_sets(),
                                   right_child->get_output_const_exprs(),
                                   is_right_unique))) {
        LOG_WARN("failed to check is right unique", K(ret));
      } else if (need_check_unique && !is_right_unique) {
        remove_join_dist_flag(candidate_method, DIST_NONE_PARTITION);
      } else if (OB_FAIL(check_if_match_repart(
                     equal_sets, right_set_keys, left_set_keys, *left_child, right_match_repart))) {
        LOG_WARN("failed to check_and_extract_repart_info", K(ret));
      } else if (!right_match_repart) {
        remove_join_dist_flag(candidate_method, DIST_NONE_PARTITION);
      }
    }
    // get hint method
    if (OB_SUCC(ret)) {
      bool use_seq_exec = false;
      if (has_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL) &&
          !has_join_dist_flag(candidate_method, DIST_PARTITION_WISE) &&
          OB_FAIL(should_use_sequential_execution(use_seq_exec))) {
        LOG_WARN("failed to check should use sequential execution", K(ret));
      } else if (use_seq_exec) {
        candidate_method = DIST_PULL_TO_LOCAL;
      }
      LOG_TRACE("succeed to get candidate methods",
          "DIST_PARTITION_WISE",
          has_join_dist_flag(candidate_method, DIST_PARTITION_WISE),
          "DIST_PARTITION_NONE",
          has_join_dist_flag(candidate_method, DIST_PARTITION_NONE),
          "DIST_NONE_PARTITION",
          has_join_dist_flag(candidate_method, DIST_NONE_PARTITION),
          "DIST_HASH_HASH",
          has_join_dist_flag(candidate_method, DIST_HASH_HASH),
          "DIST_BROADCAST_NONE",
          has_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL));
    }
  }
  return ret;
}

int ObLogSet::update_sharding_conds(ObIArray<ObRawExpr*>& sharding_conds)
{
  int ret = OB_SUCCESS;
  if (is_set_distinct() && ObSelectStmt::UNION == get_set_op() &&
      (set_dist_algo_ == DIST_PARTITION_WISE || set_dist_algo_ == DIST_HASH_HASH)) {
    ObSEArray<ObRawExpr*, 4> part_keys;
    ObSEArray<ObRawExpr*, 4> select_exprs;
    ObLogicalOperator* child_op = NULL;
    ObSelectStmt* child_stmt = NULL;
    ObRawExpr* set_expr = NULL;
    ObRawExpr* child_expr = NULL;
    int64_t set_idx = -1;
    if (OB_FAIL(get_sharding_info().get_all_partition_keys(part_keys))) {
      LOG_WARN("failed to get all partition keys", K(ret));
    } else if (OB_FAIL(get_set_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      int64_t idx = -1;
      if (OB_ISNULL(set_expr = ObTransformUtils::get_expr_in_cast(select_exprs.at(i))) ||
          OB_UNLIKELY(!set_expr->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected set expr", K(ret), K(set_expr));
      } else if (OB_UNLIKELY((set_idx = static_cast<ObSetOpRawExpr*>(set_expr)->get_idx()) < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid set expr", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && idx == -1 && j < get_num_of_child(); ++j) {
        if (OB_ISNULL(child_op = get_child(j)) || OB_ISNULL(child_op->get_stmt()) ||
            !child_op->get_stmt()->is_select_stmt()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected logical operator", K(ret));
        } else if (OB_FALSE_IT(child_stmt = static_cast<ObSelectStmt*>(child_op->get_stmt()))) {
          LOG_WARN("failed to pushback stmt", K(ret));
        } else if (child_stmt->get_select_item_size() <= set_idx ||
                   OB_ISNULL(child_expr = child_stmt->get_select_item(set_idx).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected child stmt", K(ret));
        } else if (ObOptimizerUtil::find_equal_expr(
                       part_keys, child_expr, child_op->get_sharding_output_equal_sets(), idx)) {
          /*do nothing*/
        }
      }
      if (OB_SUCC(ret) && idx >= 0 && idx < part_keys.count()) {
        ObRawExpr* equal_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::create_equal_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                get_plan()->get_optimizer_context().get_session_info(),
                set_expr,
                part_keys.at(idx),
                equal_expr))) {
          LOG_WARN("failed to create equal expr", K(ret));
        } else if (OB_FAIL(sharding_conds.push_back(equal_expr))) {
          LOG_WARN("failed to push back equal expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogSet::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate_expr_post", K(ret));
  } else if (!is_recursive_union_) {
    // do nothing
  } else if (OB_UNLIKELY(!stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", KPC(stmt), K(ret));
  } else if (OB_UNLIKELY(false == (select_stmt = static_cast<ObSelectStmt*>(stmt))->is_recursive_union())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recursive union's stmt is not recursive cte stmt", KPC(stmt), K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogSet::get_set_child_exprs(ObIArray<ObRawExpr*>& left_keys, ObIArray<ObRawExpr*>& right_keys)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* left_stmt = NULL;
  ObSelectStmt* right_stmt = NULL;
  if (2 != get_num_of_child()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected num of child", K(ret), K(get_num_of_child()));
  } else if (OB_ISNULL(left_stmt = get_left_stmt()) || OB_ISNULL(right_stmt = get_right_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_stmt), K(right_stmt), K(ret));
  } else if (OB_FAIL(left_stmt->get_select_exprs(left_keys)) || OB_FAIL(right_stmt->get_select_exprs(right_keys))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogSet::get_calc_types(ObIArray<ObExprCalcType>& calc_types)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObRawExpr* expr = NULL;
  if (OB_FAIL(get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(select_exprs.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(calc_types.push_back(expr->get_result_type().get_obj_meta()))) {
      LOG_WARN("failed to push back calc type", K(ret));
    }
  }
  return ret;
}

int ObLogSet::make_sort_keys(ObIArray<ObRawExpr*>& sort_expr, ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  int64_t sort_expr_count = sort_expr.count();
  int64_t direction_count = set_directions_.count();
  if (OB_UNLIKELY(ObSelectStmt::UNION == set_op_ && !is_distinct_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union all do not need sort", K(set_op_), K(is_distinct_), K(ret));
  } else if (OB_UNLIKELY(direction_count <= 0 || sort_expr_count != direction_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "in set operator, invalid direction_count or sort_expr_count", K(direction_count), K(sort_expr_count), K(ret));
  } else {
    OrderItem order_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < direction_count; ++i) {
      order_item.reset();
      order_item.expr_ = sort_expr.at(i);
      order_item.order_type_ = set_directions_.at(i);
      if (OB_FAIL(order_keys.push_back(order_item))) {
        LOG_WARN("failed to push back order item", K(i), K(order_item), K(ret));
      }
    }
  }
  return ret;
}

int ObLogSet::is_my_set_expr(const ObRawExpr* expr, bool& bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_stmt());
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect NULL pointer", K(stmt), K(expr));
  } else if (!expr->is_set_op_expr()) {
    bret = false;
  } else if (static_cast<const ObSetOpRawExpr*>(expr)->get_expr_level() != stmt->get_current_level()) {
    bret = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < stmt->get_select_item_size(); ++i) {
      bret = expr == ObTransformUtils::get_expr_in_cast(stmt->get_select_item(i).expr_);
    }
  }
  return ret;
}

int ObLogSet::set_left_expected_ordering(const ObIArray<OrderItem>& left_expected_ordering)
{
  return left_expected_ordering_.assign(left_expected_ordering);
}

int ObLogSet::set_search_ordering(const common::ObIArray<OrderItem>& search_ordering)
{
  int ret = OB_SUCCESS;
  search_ordering_.reset();
  for (int i = 0; OB_SUCC(ret) && i < search_ordering.count(); ++i) {
    if (OB_FAIL(search_ordering_.push_back(search_ordering.at(i)))) {
      LOG_WARN("failed to push back order item", K(ret));
    }
  }
  return ret;
}

int ObLogSet::set_cycle_items(const common::ObIArray<ColumnItem>& cycle_items)
{
  int ret = OB_SUCCESS;
  cycle_items_.reset();
  for (int i = 0; OB_SUCC(ret) && i < cycle_items.count(); ++i) {
    if (OB_FAIL(cycle_items_.push_back(cycle_items.at(i)))) {
      LOG_WARN("failed to push back cycle item", K(ret));
    }
  }
  return ret;
}

int ObLogSet::set_right_expected_ordering(const ObIArray<OrderItem>& right_expected_ordering)
{
  return right_expected_ordering_.assign(right_expected_ordering);
}

int ObLogSet::allocate_implicit_sort_v2_for_set(const int64_t index, ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  ObLogSort* sort = NULL;
  if (OB_FAIL(insert_operator_for_set(LOG_SORT, index))) {
    LOG_WARN("failed to insert sort op", K(ret));
  } else if (OB_ISNULL(sort = static_cast<ObLogSort*>(get_child(index)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort is null", K(ret));
  } else if (OB_FAIL(sort->set_sort_keys(order_keys))) {
    LOG_WARN("failed to set sort keys", K(ret));
  } else {
  }
  // set op_cost and other info
  if (OB_SUCC(ret)) {
    double sort_cost = 0;
    ObSortCostInfo cost_info(sort->get_child(first_child)->get_card(),
        sort->get_child(first_child)->get_width(),
        sort->get_prefix_pos(),
        sort->get_est_sel_info());
    if (OB_FAIL(ObOptEstCost::cost_sort(cost_info, order_keys, sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      sort->set_card(sort->get_child(first_child)->get_card());
      sort->set_op_cost(sort_cost);
      // no need to set cost, cost maybe change, finally there will be a re_calc_cost()
    }
  }
  return ret;
}

int ObLogSet::insert_operator_for_set(ObLogOpType type, int64_t index)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  ObLogPlan* child_plan = NULL;
  if (OB_ISNULL(child = get_child(index)) || OB_ISNULL(child_plan = child->get_plan())) {
    ret = OB_ERROR;
    LOG_WARN("invalid argument", K(ret), K(index), K(child), K(child_plan));
  } else {
    ObLogicalOperator* op = NULL;
    if (OB_ISNULL(op = child_plan->get_log_op_factory().allocate(*child_plan, type))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for operatory", K(type));
    } else {
      LOG_TRACE("insert operator", "parent", get_name(), "child", child->get_name(), "inserted", op->get_name());
      /**
       *       this                       this
       *         |                          |
       *       child         ==>          new op
       *                                    |
       *                                  child
       */
      op->set_child(first_child, child);
      op->set_parent(this);
      child->set_parent(op);
      set_child(index, op);
      if (child->is_plan_root()) {
        child->set_is_plan_root(false);
        op->mark_is_plan_root();
        child_plan->set_plan_root(op);
      }
    }
  }
  // op's ordering need be set outside
  return ret;
}

int ObLogSet::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  if (is_distinct_) {
    bool need_sort = true;
    ObLogicalOperator* left = NULL;
    ObLogicalOperator* right = NULL;
    ObSEArray<OrderItem, 4> opt_order_items;
    if (OB_ISNULL(left = get_child(first_child)) || OB_ISNULL(right = get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(left), K(right));
    } else if (OB_FAIL(left->simplify_ordered_exprs(left_expected_ordering_, opt_order_items))) {
      LOG_WARN("failed to simplify ordered exprs", K(ret));
    } else if (OB_FAIL(left->check_need_sort_above_node(opt_order_items, need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (need_sort && OB_FAIL(allocate_implicit_sort_v2_for_set(0, opt_order_items))) {
      LOG_WARN("failed to allocate implicit sort", K(ret));
    } else if (OB_FAIL(right->simplify_ordered_exprs(right_expected_ordering_, opt_order_items))) {
      LOG_WARN("failed to simplify ordered exprs", K(ret));
    } else if (OB_FAIL(right->check_need_sort_above_node(opt_order_items, need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (need_sort && OB_FAIL(allocate_implicit_sort_v2_for_set(1, opt_order_items))) {
      LOG_WARN("failed to allocate implicit sort", K(ret));
    }
  } else {
    reset_op_ordering();
  }
  return ret;
}

int ObLogSet::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = get_child(ObLogicalOperator::first_child);
  ObLogicalOperator* right_child = get_child(ObLogicalOperator::second_child);
  ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_stmt());
  double card = 0;
  double op_cost = 0;
  double cost = 0;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) || OB_ISNULL(stmt) ||
      OB_UNLIKELY(INVALID_SET_ALGO == set_algo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set op is not inited", K(ret));
  } else if (MERGE_SET == set_algo_) {
    ObSEArray<ObBasicCostInfo, 4> children_cost_info;
    ObCostMergeSetInfo cost_info(children_cost_info, get_set_op(), stmt->get_select_item_size());
    if (OB_FAIL(get_children_cost_info(children_cost_info))) {
      LOG_WARN("failed to get children cost info", K(ret));
    } else if (OB_FAIL(ObOptEstCost::cost_merge_set(cost_info, card, op_cost, cost))) {
      LOG_WARN("estimate cost of SET operator failed", K(ret));
    }
  } else if (HASH_SET == set_algo_) {
    ObArray<ObRawExpr*> select_exprs;
    ObCostHashSetInfo hash_cost_info(left_child->get_card(),
        left_child->get_cost(),
        left_child->get_width(),
        right_child->get_card(),
        right_child->get_cost(),
        right_child->get_width(),
        get_set_op(),
        select_exprs);
    if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("faield to get select exprs", K(ret));
    } else if (OB_FAIL(ObOptEstCost::cost_hash_set(hash_cost_info, card, op_cost, cost))) {
      LOG_WARN("Fail to calcuate hash set cost", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_card(card);
    set_op_cost(op_cost);
    set_cost(cost);
    set_width(std::max(left_child->get_width(), right_child->get_width()));
  }
  return ret;
}

int ObLogSet::get_children_cost_info(ObIArray<ObBasicCostInfo>& children_cost_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator* child = get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set operator i-th child is null", K(ret), K(i));
    } else {
      ObBasicCostInfo info(child->get_card(), child->get_cost(), child->get_width());
      if (OB_FAIL(children_cost_info.push_back(info))) {
        LOG_WARN("push back child's cost info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogSet::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  if (get_card() > need_row_count) {
    ObSelectStmt* stmt = NULL;
    double dummy_card = 0.0;
    double op_cost = get_op_cost();
    double cost = get_cost();
    if (ObSelectStmt::UNION == get_set_op()) {
      if (OB_ISNULL(stmt = static_cast<ObSelectStmt*>(get_stmt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set operator statement is NULL", K(ret), K(stmt));
      } else if (MERGE_SET == set_algo_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
          if (OB_ISNULL(get_child(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set operator child is NULL", K(ret), K(i));
          } else if (OB_FAIL(get_child(i)->re_est_cost(this, need_row_count, re_est))) {
            LOG_WARN("re-estimate cost first child failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObSEArray<ObBasicCostInfo, 4> children_cost_info;
          ObCostMergeSetInfo cost_info(children_cost_info, get_set_op(), stmt->get_select_item_size());
          if (OB_FAIL(get_children_cost_info(children_cost_info))) {
            LOG_WARN("failed to get children cost info", K(ret));
          } else if (OB_FAIL(ObOptEstCost::cost_merge_set(cost_info, dummy_card, op_cost, cost))) {
            LOG_WARN("estimate cost of SET operator failed", K(ret));
          }
        }
      } else if (HASH_SET == set_algo_) {
        ObArray<ObRawExpr*> select_exprs;
        ObLogicalOperator* first = NULL;
        ObLogicalOperator* second = NULL;
        if (OB_UNLIKELY(2 != get_num_of_child()) || OB_ISNULL(first = get_child(first_child)) ||
            OB_ISNULL(second = get_child(second_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected set operator child is NULL", K(ret), K(get_num_of_child()), K(first), K(second));
        } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
          LOG_WARN("failed to get select exprs", K(ret));
        } else {
          ObCostHashSetInfo info(first->get_card(),
              first->get_cost(),
              first->get_width(),
              second->get_card(),
              second->get_cost(),
              second->get_width(),
              get_set_op(),
              select_exprs);
          if (OB_FAIL(ObOptEstCost::cost_hash_set(info, dummy_card, op_cost, cost))) {
            LOG_WARN("estimate cost of SET operator failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      re_est = true;
      set_op_cost(op_cost);
      set_cost(cost);
      set_card(need_row_count);
    }
  }
  return ret;
}

int ObLogSet::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> set_exprs;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(ret));
  } else if (OB_FAIL(get_set_exprs(set_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, set_exprs))) {
    LOG_WARN("failed to set exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  } else if (is_recursive_union_ && get_stmt()->is_select_stmt()) {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(get_stmt());
    if (OB_FAIL(add_exprs_to_ctx(ctx, select_stmt->get_cte_exprs()))) {
      LOG_WARN("fail to add cte exprs", K(ret));
    }
  } else { /*do nothing*/
  }

  // set producer id for set expr
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
    ExprProducer& producer = ctx.expr_producers_.at(i);
    if (OB_ISNULL(producer.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_INVALID_ID == producer.producer_id_ && producer.expr_->has_flag(IS_SET_OP)) {
      bool is_my_set = false;
      if (OB_FAIL(is_my_set_expr(producer.expr_, is_my_set))) {
        LOG_WARN("failed to check whether is my set expr", K(ret));
      } else if (is_my_set) {
        producer.producer_id_ = id_;
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogSet::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(ctx));
  } else if (use_slave_mapping()) {
    ctx.slave_mapping_type_ = slave_mapping_type_;
  } else if (!ctx.is_in_partition_wise_state() && !ctx.is_in_pw_affinity_state() && is_partition_wise_) {
    /**
     *        (partition wise join below)
     *                   |
     *                 JOIN(1)
     *                   |
     *             --------------
     *             |            |
     *           JOIN(2)       ...
     *             |
     *            ...
     *   JOIN(1) come into this code block.
     *   he will set 'this' ptr to gi allocate ctx as a reset token,
     *   and in the allocate-granule post stage JOIN(1) will
     *   reset the state of gi-allocate ctx.
     */
    ctx.set_in_partition_wise_state(this);
    LOG_TRACE("in find partition wise state", K(sharding_info_), K(ctx));
  } else if (ctx.is_in_partition_wise_state()) {
    /**
     *       (partition wise join below)
     *                   |
     *                 JOIN(1)
     *                   |
     *             --------------
     *             |            |
     *           JOIN(2)       ...
     *             |
     *        ------------
     *        |          |
     *       ...        EX(pkey)
     *                   |
     *                  ...
     *   JOIN(2) come into this code block.
     *   If there is a repartition(by key) below this partition wise plan,
     *   for some complex reason, we can not do partition wise join/union.
     *   We allocate gi above the table scan as usual, and set a affinitize property to these GI.
     */
    if (DIST_PARTITION_NONE == set_dist_algo_ || DIST_NONE_PARTITION == set_dist_algo_ ||
        DIST_NONE_RANDOM == set_dist_algo_ || DIST_RANDOM_NONE == set_dist_algo_) {
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      } else { /*do nothing*/
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogSet::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  /**
   *       (partition wise join below)
   *                   |
   *                 JOIN(1)
   *                   |
   *             --------------
   *             |            |
   *           JOIN(2)       ...
   *             |
   *            ...
   *   JOIN(1) will reset the state of gi allocate ctx.
   *   As the ctx has record the state was changed by JOIN(2),
   *   so JOIN(2) can not reset this state.
   */
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(sharding_info_));
  } else if (ctx.is_in_partition_wise_state()) {
    if (ctx.is_op_set_pw(this)) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
      IGNORE_RETURN ctx.reset_info();
    }
  } else if (ctx.is_in_pw_affinity_state()) {
    if (ctx.is_op_set_pw(this)) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_gi_recursively(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
      IGNORE_RETURN ctx.reset_info();
    }
  } else if (DIST_NONE_PARTITION == set_dist_algo_) {
    if (OB_FAIL(set_granule_nodes_affinity(ctx, 0))) {
      LOG_WARN("set granule nodes affinity failed", K(ret));
    }
    LOG_TRACE("set left child gi to affinity");
  } else if (DIST_PARTITION_NONE == set_dist_algo_) {
    if (OB_FAIL(set_granule_nodes_affinity(ctx, 1))) {
      LOG_WARN("set granule nodes affinity failed", K(ret));
    }
    LOG_TRACE("set right child gi to affinity");
  } else if (DIST_PULL_TO_LOCAL == set_dist_algo_) {
    ObLogicalOperator* op = NULL;
    if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
      LOG_WARN("find granule iterator in right failed", K(ret));
    } else if (NULL == op) {
      // granule iterator not found, do nothing
    } else {
      static_cast<ObLogGranuleIterator*>(op)->add_flag(GI_ACCESS_ALL);
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogSet::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (OB_FAIL(link_stmt->fill_from_set(set_op_, is_distinct_))) {
    LOG_WARN("failed to fill from set", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is NULL", K(ret), K(get_num_of_child()), K(get_child(i)));
      } else {
        ObLinkStmt sub_link_stmt(link_stmt->alloc_, get_child(i)->get_output_exprs());
        if (OB_FAIL(sub_link_stmt.init(&link_ctx))) {
          LOG_WARN("failed to init sub link stmts", K(ret), K(i));
        } else if (FALSE_IT(link_ctx.link_stmt_ = &sub_link_stmt) ||
                   OB_FAIL(get_child(i)->do_plan_tree_traverse(GEN_LINK_STMT, &link_ctx))) {
          LOG_WARN("failed to gen link stmt", K(ret), K(i));
        } else if (OB_FAIL(link_stmt->fill_from_strs(sub_link_stmt))) {
          LOG_WARN("failed to fill link stmt from strs with sub stmt", K(ret), K(i));
        }
      }
    }
    link_ctx.link_stmt_ = link_stmt;
  }
  return ret;
}

uint64_t ObLogSet::hash(uint64_t seed) const
{
  seed = do_hash(is_distinct_, seed);
  seed = do_hash(is_recursive_union_, seed);
  seed = do_hash(is_breadth_search_, seed);
  seed = do_hash(set_op_, seed);
  HASH_ARRAY(set_directions_, seed);
  HASH_ARRAY(left_expected_ordering_, seed);
  HASH_ARRAY(right_expected_ordering_, seed);
  HASH_ARRAY(search_ordering_, seed);
  HASH_ARRAY(cycle_items_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogSet::get_set_exprs(ObIArray<ObRawExpr*>& set_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else if (domain_index_exprs_.count() > 0) {
    if (OB_FAIL(set_exprs.assign(domain_index_exprs_))) {
      LOG_WARN("failed to assign domain index exprs", K(ret));
    }
  } else {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(get_stmt());
    if (OB_FAIL(sel_stmt->get_select_exprs(set_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogSet::extra_set_exprs(ObIArray<ObRawExpr*>& set_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> out_raw_exprs;
  if (OB_FAIL(get_set_exprs(out_raw_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_expr_in_cast(out_raw_exprs, set_exprs))) {
    LOG_WARN("failed to get expr in cast", K(ret));
  }
  return ret;
}

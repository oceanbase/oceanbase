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
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

const char *ObLogSet::get_name() const
{
  static const char *set_op_all[ObSelectStmt::SET_OP_NUM] =
  {
    "NONE",
    "UNION ALL",
    "INTERSECT ALL",
    "EXCEPT ALL",
    "RECURSIVE UNION ALL",
  };
  static const char *merge_set_op_distinct[ObSelectStmt::SET_OP_NUM] =
  {
    "NONE",
    "MERGE UNION DISTINCT",
    "MERGE INTERSECT DISTINCT",
    "MERGE EXCEPT DISTINCT",
  };
  static const char *hash_set_op_distinct[ObSelectStmt::SET_OP_NUM] =
  {
    "NONE",
    "HASH UNION DISTINCT",
    "HASH INTERSECT DISTINCT",
    "HASH EXCEPT DISTINCT",
  };
  const char *ret_char = "Unknown type";
  if (set_op_ >= 0 && set_op_ < ObSelectStmt::SET_OP_NUM
      && set_algo_ != INVALID_SET_ALGO) {
    ret_char = !is_distinct_ ? set_op_all[set_op_] :
               (HASH_SET == set_algo_ ? hash_set_op_distinct[set_op_] : merge_set_op_distinct[set_op_]);
    if (is_recursive_union_) {
      ret_char = set_op_all[ObSelectStmt::SetOperator::RECURSIVE];
    }
  } else { /* Do nothing */ }
  return ret_char;
}

const ObSelectStmt *ObLogSet::get_left_stmt() const
{
  const ObSelectStmt *ret = NULL;
  ObLogicalOperator *left_child = get_child(ObLogicalOperator::first_child);
  if (!OB_ISNULL(left_child) && !OB_ISNULL(left_child->get_plan())
      && !OB_ISNULL(left_child->get_plan()->get_stmt())
      && left_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<const ObSelectStmt*>(left_child->get_plan()->get_stmt());
  }
  return ret;
}

const ObSelectStmt *ObLogSet::get_right_stmt() const
{
  const ObSelectStmt *ret = NULL;
  ObLogicalOperator *right_child = get_child(ObLogicalOperator::second_child);
  if (!OB_ISNULL(right_child) && !OB_ISNULL(right_child->get_plan())
      && !OB_ISNULL(right_child->get_plan()->get_stmt())
      && right_child->get_plan()->get_stmt()->is_select_stmt()) {
    ret = static_cast<const ObSelectStmt*>(right_child->get_plan()->get_stmt());
  }
  return ret;
}

int ObLogSet::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(my_plan_), K(left_child), K(right_child));
  } else if ((ObSelectStmt::EXCEPT == get_set_op() || ObSelectStmt::INTERSECT == get_set_op())
             && OB_FAIL(append(output_const_exprs_, left_child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (ObSelectStmt::INTERSECT == get_set_op() &&
             OB_FAIL(append(output_const_exprs_, right_child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (ObSelectStmt::UNION == get_set_op()) {
    //union, left/right 均为 const 且值相等添加 const, 暂不维护
    //union 可能有多于两个分支
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObLogSet::compute_equal_set()
{
  int ret = OB_SUCCESS;
  EqualSets *ordering_esets = NULL;
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
    if (OB_FAIL(temp_ordering_esets.assign(
                  get_child(first_child)->get_output_equal_sets()))) {
      LOG_WARN("failed to get ordering output equal sets", K(ret));
    }
  } else if (OB_FAIL(get_input_equal_sets(temp_ordering_esets))) {
    LOG_WARN("failed to get ordering input equal sets", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create equal sets", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&get_plan()->get_allocator(),
                                                          ordering_eset_conditions,
                                                          temp_ordering_esets,
                                                          *ordering_esets))) {
      LOG_WARN("failed to compute sharding output equal set", K(ret));
    } else {
      set_output_equal_sets(ordering_esets);
    }
  }
  return ret;
}

int ObLogSet::get_equal_set_conditions(ObIArray<ObRawExpr*> &equal_conds)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr*, 8> set_exprs;
  const ObSelectStmt *stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())
      || OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())
      || OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(get_stmt()), K(session_info), K(ret));
  } else if (OB_FALSE_IT(stmt = static_cast<const ObSelectStmt*>(get_stmt()))) {
  } else if (OB_FAIL(stmt->get_pure_set_exprs(set_exprs))) {
    LOG_WARN("failed to expr in cast", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_equal_set_conditions(*expr_factory, session_info,
                                                                stmt, set_exprs, equal_conds))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else {
    LOG_TRACE("succeed to get equal set conditions for set op", K(equal_conds));
  }
  return ret;
}

int ObLogSet::deduce_const_exprs_and_ft_item_set(ObFdItemSet &fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> select_exprs;
  if (OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_set_exprs(select_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().deduce_fd_item_set(
                                                          get_output_equal_sets(),
                                                          select_exprs,
                                                          get_output_const_exprs(),
                                                          fd_item_set))) {
    LOG_WARN("falied to remove const in fd item", K(ret));
  }
  return ret;
}

int ObLogSet::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObFdItemSet *fd_item_set = NULL;
  ObExprFdItem *fd_item = NULL;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  ObSEArray<ObRawExpr *, 8> select_exprs;
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
  } else if (!ObTransformUtils::need_compute_fd_item_set(select_exprs)) {
    //do nothing
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_expr_fd_item(
                                                        fd_item,
                                                        true,
                                                        select_exprs,
                                                        select_exprs))) {
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  } else if ((ObSelectStmt::INTERSECT == set_op_ || ObSelectStmt::EXCEPT == set_op_) &&
            OB_FAIL(append_child_fd_item_set(*fd_item_set, left_child->get_fd_item_set()))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (ObSelectStmt::INTERSECT == set_op_ &&
            OB_FAIL(append_child_fd_item_set(*fd_item_set, right_child->get_fd_item_set()))) {
    LOG_WARN("failed to append fd item set", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

// just ignore table fd item now
// todo: adjust log set fd, convert child fd set to currnet level stmt
int ObLogSet::append_child_fd_item_set(ObFdItemSet &all_fd_item_set, const ObFdItemSet &child_fd_item_set)
{
  int ret = OB_SUCCESS;
  ObFdItem *fd_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = child_fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (fd_item->is_table_fd_item()) {
      /* do nothing */
    } else if (OB_FAIL(all_fd_item_set.push_back(fd_item))) {
      LOG_WARN("failed to push back fd item", K(ret));
    }
  }
  return ret;
}

int ObLogSet::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  if (MERGE_SET == set_algo_ && is_set_distinct()) {
    ObSEArray<OrderItem, 8> ordering;;
    //查询项和索引key可能不是一一对应, 这时需要放入映射后的exprs到ordering
    //only merge union generate sorted output
    ObArray<ObRawExpr*> mapped_exprs; //按照map array的顺序调整后得到的
    ObSEArray<ObRawExpr *, 8> select_exprs;
    ObLogicalOperator *left_child = NULL;
    if (OB_ISNULL(left_child = get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret), K(get_stmt()));
    } else if (OB_FAIL(static_cast<const ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (map_array_.count() != select_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid mapping array", K(map_array_), K(select_exprs));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(
                         select_exprs, map_array_, mapped_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(mapped_exprs, set_directions_, ordering))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(set_op_ordering(ordering))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else if (!exchange_allocated_ && (DistAlgo::DIST_PARTITION_WISE == set_dist_algo_)) {
      is_range_order_ = left_child->get_is_range_order();
      is_local_order_ = !is_range_order_;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogSet::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  if (OB_ISNULL(left_child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(right_child = get_child(ObLogicalOperator::second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (set_op_ == ObSelectStmt::UNION || is_recursive_union()) {
    set_is_at_most_one_row(false);
  } else if (set_op_ == ObSelectStmt::INTERSECT) {
    set_is_at_most_one_row(
        (left_child->get_is_at_most_one_row() || right_child->get_is_at_most_one_row()));
  } else if (set_op_ == ObSelectStmt::EXCEPT) {
    set_is_at_most_one_row(left_child->get_is_at_most_one_row());
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSet::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *first_child = NULL;
  ObLogicalOperator *second_child = NULL;
  is_partition_wise_ = (set_dist_algo_ == DistAlgo::DIST_PARTITION_WISE ||
                        set_dist_algo_ == DistAlgo::DIST_EXT_PARTITION_WISE);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(second_child = get_child(ObLogicalOperator::second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (DistAlgo::DIST_BASIC_METHOD == set_dist_algo_) {
    if (OB_FAIL(ObOptimizerUtil::compute_basic_sharding_info(
                                    get_plan()->get_optimizer_context().get_local_server_addr(),
                                    get_child_list(),
                                    get_plan()->get_allocator(),
                                    strong_sharding_,
                                    inherit_sharding_index_))) {
      LOG_WARN("failed to compute basic sharding info", K(ret));
    }
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == set_dist_algo_) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (DistAlgo::DIST_SET_RANDOM == set_dist_algo_) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
  } else if ((DistAlgo::DIST_PARTITION_WISE == set_dist_algo_ ||
              DistAlgo::DIST_EXT_PARTITION_WISE == set_dist_algo_) &&
            (ObSelectStmt::UNION == set_op_) && !is_set_distinct()) {
    is_partition_wise_ = true;
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
  } else if (DistAlgo::DIST_SET_PARTITION_WISE == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
  } else if (DistAlgo::DIST_NONE_HASH == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = first_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::first_child;
  } else if (DistAlgo::DIST_HASH_NONE == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = second_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::second_child;
  } else if (DistAlgo::DIST_NONE_ALL == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = first_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::first_child;
  } else if (DistAlgo::DIST_ALL_NONE == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = second_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::second_child;
  } else if (DistAlgo::DIST_PARTITION_NONE == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = second_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::second_child;
  } else if (DistAlgo::DIST_NONE_PARTITION == set_dist_algo_) {
    is_partition_wise_ = false;
    strong_sharding_ = first_child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::first_child;
  } else if (OB_FAIL(ObLogicalOperator::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSet::set_search_ordering(const common::ObIArray<OrderItem> &search_ordering)
{
  int ret = OB_SUCCESS;
  search_ordering_.reset();
  for (int i = 0 ; OB_SUCC(ret) && i < search_ordering.count(); ++i) {
    if (OB_FAIL(search_ordering_.push_back(search_ordering.at(i)))) {
      LOG_WARN("failed to push back order item", K(ret));
    }
  }
  return ret;
}

int ObLogSet::set_cycle_items(const common::ObIArray<ColumnItem> &cycle_items)
{
  int ret = OB_SUCCESS;
  cycle_items_.reset();
  for (int i = 0 ; OB_SUCC(ret) && i < cycle_items.count(); ++i) {
    if (OB_FAIL(cycle_items_.push_back(cycle_items.at(i)))) {
      LOG_WARN("failed to push back cycle item", K(ret));
    }
  }
  return ret;
}

int ObLogSet::est_cost()
{
  int ret = OB_SUCCESS;
  double card = 0;
  double op_cost = 0;
  double cost = 0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to get re est cost infos", K(ret));
  } else {
    set_card(card);
    set_op_cost(op_cost);
    set_cost(cost);
  }
  return ret;
}

int ObLogSet::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObLogicalOperator *left_child = get_child(ObLogicalOperator::first_child);
  ObLogicalOperator *right_child = get_child(ObLogicalOperator::second_child);
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set op is not inited", K(ret));
  } else {
    width = std::max(left_child->get_width(), right_child->get_width());
    set_width(width);
  }
  return ret;
}

int ObLogSet::get_re_est_cost_infos(const EstimateCostInfo &param,
                                    ObIArray<ObBasicCostInfo> &cost_infos,
                                    double &child_cost,
                                    double &card)
{
  int ret = OB_SUCCESS;
  child_cost = 0.0;
  card = 0.0;
  EstimateCostInfo cur_param;
  const double need_row_count = (is_recursive_union() || !is_set_distinct())
                                && param.need_row_count_ >= 0 && param.need_row_count_ < card_
                                ? param.need_row_count_ : -1;
  bool need_scale_ndv = (need_row_count == -1);
  double cur_child_card = 0.0;
  double cur_child_cost = 0.0;
  if (OB_UNLIKELY(is_set_distinct() && get_num_of_child() != child_ndv_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child ndv count", K(child_ndv_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator *child = get_child(i);
    cur_param.reset();
    double origin_child_card = 0;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set operator i-th child is null", K(ret), K(i));
    } else if (OB_FAIL(cur_param.assign(param))) {
      LOG_WARN("failed to assign param", K(ret));
    } else {
      cur_param.need_row_count_ = need_row_count;
      origin_child_card = child->get_card();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_child(i)->re_est_cost(cur_param, cur_child_card, cur_child_cost))) {
      LOG_WARN("failed to re-est child cost", K(ret), K(i));
    } else if (OB_FAIL(cost_infos.push_back(ObBasicCostInfo(cur_child_card, cur_child_cost,
                                                            child->get_width())))) {
      LOG_WARN("push back child's cost info failed", K(ret));
    } else if (ObSelectStmt::UNION == get_set_op() && !is_set_distinct()) {
      ObSelectStmt::SetOperator set_type = is_recursive_union() ? ObSelectStmt::RECURSIVE : ObSelectStmt::UNION;
      if (0 == i) {
        card = cur_child_card;
      } else {
        card = ObOptSelectivity::get_set_stmt_output_count(card, cur_child_card, set_type);
      }
      child_cost += cur_child_cost;
    } else {
      double cur_child_ndv = child_ndv_.at(i);
      if (need_scale_ndv) {
        cur_child_ndv = std::min(
            cur_child_ndv,
            ObOptSelectivity::scale_distinct(cur_child_card, origin_child_card, cur_child_ndv));
      }
      if (0 == i) {
        card = cur_child_ndv;
      } else {
        card = ObOptSelectivity::get_set_stmt_output_count(card, cur_child_ndv, get_set_op());
      }
      child_cost += cur_child_cost;
    }
  }
  return ret;
}

int ObLogSet::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  op_cost = 0.0;
  cost = 0.0;
  card = 0.0;
  double tmp_card = 0.0;
  double child_cost = 0.0;
  ObSEArray<ObBasicCostInfo, 4> cost_infos;
  const ObSelectStmt *stmt = dynamic_cast<const ObSelectStmt*>(get_stmt());
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_re_est_cost_infos(param, cost_infos, child_cost, tmp_card))) {
    LOG_WARN("failed to get re est cost infos", K(ret));
  } else if (is_recursive_union() || !is_set_distinct()) {
    ObCostMergeSetInfo cost_info(cost_infos, get_set_op(), stmt->get_select_item_size());
    if (OB_FAIL(ObOptEstCost::cost_union_all(cost_info,
                                             op_cost,
                                             get_plan()->get_optimizer_context()))) {
      LOG_WARN("estimate cost of SET operator failed", K(ret));
    }
  } else if (MERGE_SET == set_algo_) {
    ObCostMergeSetInfo cost_info(cost_infos, get_set_op(), stmt->get_select_item_size());
    if (OB_FAIL(ObOptEstCost::cost_merge_set(cost_info,
                                             op_cost,
                                             get_plan()->get_optimizer_context()))) {
      LOG_WARN("estimate cost of SET operator failed", K(ret));
    }
  } else if (HASH_SET == set_algo_) {
    ObSEArray<ObRawExpr*, 8> select_exprs;
    if (OB_UNLIKELY(2 != cost_infos.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cost infos count", K(ret), K(cost_infos.count()));
    } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("faield to get select exprs", K(ret));
    } else {
      ObCostHashSetInfo hash_cost_info(cost_infos.at(0).rows_, cost_infos.at(0).width_,
                                       cost_infos.at(1).rows_, cost_infos.at(1).width_,
                                       get_set_op(), select_exprs,
                                       NULL, NULL /* no need for hash set*/ );
      if (OB_FAIL(ObOptEstCost::cost_hash_set(hash_cost_info,
                                              op_cost,
                                              get_plan()->get_optimizer_context()))) {
        LOG_WARN("Fail to calcuate hash set cost", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected else", K(ret));
  }

  if (OB_SUCC(ret)) {
    cost = child_cost + op_cost;
    card = param.need_row_count_ >= 0 && param.need_row_count_ < tmp_card
           ? param.need_row_count_ : tmp_card;
    LOG_TRACE("succeed to re-estimate cost for set op", K(op_cost), K(cost));
  }
  return ret;
}

int ObLogSet::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(ret));
  } else if (OB_FAIL(get_set_exprs(all_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else if (is_recursive_union_ && get_stmt()->is_select_stmt()) {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(get_stmt());
    // 伪列的产生，search/cycle的伪列都要求在recursive union算子产生
    if (NULL != identify_seq_expr_ && OB_FAIL(all_exprs.push_back(identify_seq_expr_))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(append(all_exprs, select_stmt->get_cte_exprs()))) {
      LOG_WARN("fail to add cte exprs", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }

  return ret;
}

int ObLogSet::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(ctx));
  } else if (!ctx.is_in_partition_wise_state()
             && !ctx.is_in_pw_affinity_state()
             && DistAlgo::DIST_PARTITION_WISE == set_dist_algo_) {
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
    LOG_TRACE("in find partition wise state", K(ctx));
  } else if (DistAlgo::DIST_SET_PARTITION_WISE == set_dist_algo_) {
    if (!ctx.is_in_partition_wise_state() &&
        !ctx.is_in_pw_affinity_state()) {
      ctx.set_in_partition_wise_state(this);
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      }
    }
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
    if (DIST_PARTITION_NONE == set_dist_algo_ ||
        DIST_NONE_PARTITION == set_dist_algo_ ||
        DIST_SET_RANDOM == set_dist_algo_) {
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      } else { /*do nothing*/ }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSet::allocate_granule_post(AllocGIContext &ctx)
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
    LOG_TRACE("no exchange above, do nothing");
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
  } else { /*do nothing*/ }

  return ret;
}

uint64_t ObLogSet::hash(uint64_t seed) const
{
  seed = do_hash(is_distinct_, seed);
  seed = do_hash(set_op_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogSet::get_set_exprs(ObIArray<ObRawExpr *> &set_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(get_stmt());
    if (OB_FAIL(sel_stmt->get_select_exprs(set_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogSet::get_pure_set_exprs(ObIArray<ObRawExpr *> &set_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(get_stmt());
    if (OB_FAIL(sel_stmt->get_pure_set_exprs(set_exprs))) {
      LOG_WARN("failed to get set op exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogSet::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (ObSelectStmt::UNION == set_op_) {
    //do nothing
  } else if (ObSelectStmt::INTERSECT == set_op_) {
    if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post())) {
      LOG_WARN("failed to allocate startup expr post", K(ret));
    }
  } else if (ObSelectStmt::EXCEPT == set_op_) {
    if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post(first_child))) {
      LOG_WARN("failed to allocate startup expr post", K(ret));
    }
  }
  return ret;
}

int ObLogSet::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  const ObDMLStmt *stmt = NULL;
  ObString qb_name;
  ObPQSetHint hint;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()), K(stmt));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else if (HASH_SET == set_algo_ &&
             OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\")",
                                ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                ObHint::get_hint_name(T_USE_HASH_SET),
                                qb_name.length(), qb_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(construct_pq_set_hint(hint))) {
    LOG_WARN("fail to construct pq set hint", K(ret));
  } else if (hint.get_dist_methods().empty() && hint.get_left_branch().empty()) {
    /*do nothing*/
  } else if (OB_FALSE_IT(hint.set_qb_name(qb_name))) {
  } else if (OB_FAIL(hint.print_hint(plan_text))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSet::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else {
    const ObHint *use_hash = get_plan()->get_log_plan_hint().get_normal_hint(T_USE_HASH_SET);
    const bool algo_match = NULL != use_hash &&
                            ((HASH_SET == set_algo_ && use_hash->is_enable_hint())
                             || (MERGE_SET == set_algo_ && use_hash->is_disable_hint()));
    const ObPQSetHint *used_pq_hint = NULL;
    if (algo_match && OB_FAIL(use_hash->print_hint(plan_text))) {
      LOG_WARN("failed to print use hash hint for set", K(ret), K(*use_hash));
    } else if (OB_FAIL(get_used_pq_set_hint(used_pq_hint))) {
      LOG_WARN("failed to get used pq set hint", K(ret));
    } else if (NULL != used_pq_hint && OB_FAIL(used_pq_hint->print_hint(plan_text))) {
      LOG_WARN("failed to print pq_set hint for set", K(ret), K(*used_pq_hint));
    }
  }
  return ret;
}

int ObLogSet::get_used_pq_set_hint(const ObPQSetHint *&used_hint)
{
  int ret = OB_SUCCESS;
  used_hint = NULL;
  const ObHint *stmt_pq_set = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else if (NULL != (stmt_pq_set = get_plan()->get_log_plan_hint().get_normal_hint(T_PQ_SET))) {
    ObPQSetHint hint;
    used_hint = static_cast<const ObPQSetHint*>(stmt_pq_set);
    const ObIArray<ObItemType> &dist_methods = used_hint->get_dist_methods();
    if (OB_FAIL(construct_pq_set_hint(hint))) {
      LOG_WARN("fail to construct pq set hint", K(ret));
    } else if (dist_methods.count() != hint.get_dist_methods().count()
               || 0 != hint.get_left_branch().case_compare(used_hint->get_left_branch())) {
      used_hint = NULL;
    } else {
      for (int64_t i = 0; NULL != used_hint && i < dist_methods.count(); ++i) {
        if (dist_methods.at(i) != hint.get_dist_methods().at(i)) {
          used_hint = NULL;
        }
      }
    }
  }
  return ret;
}

int ObLogSet::construct_pq_set_hint(ObPQSetHint &hint)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = static_cast<const ObSelectStmt *>(get_stmt());
  const ObSelectStmt *left_stmt = get_left_stmt();
  ObString left_branch;
  if (OB_ISNULL(stmt) || OB_ISNULL(left_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(stmt), K(left_stmt));
  } else if (!stmt->is_set_distinct()
             || 2 < stmt->get_set_query().count()
             || stmt->get_set_query(0) == left_stmt) {
    /* do nothing */
  } else if (OB_FAIL(left_stmt->get_qb_name(left_branch))) {
    LOG_WARN("unexpected NULL", K(ret), K(stmt));
  } else {
    hint.set_left_branch(left_branch);
  }

  if (DistAlgo::DIST_BASIC_METHOD != set_dist_algo_) {
    int64_t random_none_idx = OB_INVALID_INDEX;
    if (DistAlgo::DIST_SET_RANDOM == set_dist_algo_) {
      const ObLogicalOperator *child = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
        if (OB_ISNULL(child = get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i), K(child));
        } else if (DistAlgo::DIST_SET_RANDOM == set_dist_algo_
                   && LOG_EXCHANGE != child->get_type()) {
          random_none_idx = i;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(hint.set_pq_set_hint(set_dist_algo_, get_num_of_child(), random_none_idx))) {
      LOG_WARN("failed to get dist methods", K(ret), K(set_dist_algo_), K(random_none_idx));
    }
  }
  return ret;
}

int ObLogSet::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(compute_normal_multi_child_parallel_and_server_info())) {
    LOG_WARN("failed to compute multi child parallel and server info", K(ret), K(get_distributed_algo()));
  }
  return ret;
}

int ObLogSet::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = false;
  ObSEArray<ObRawExpr*, 8> set_exprs;
  if (OB_FAIL(get_set_exprs(set_exprs))) {
    LOG_WARN("failed to get set exprs", K(ret));
  } else {
    is_fixed = ObOptimizerUtil::find_item(set_exprs, expr);
  }
  return ret;
}

int ObLogSet::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  card = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator *child = get_child(i);
    if (ObSelectStmt::UNION == get_set_op() && !is_set_distinct()) {
      ObSelectStmt::SetOperator set_type = is_recursive_union() ? ObSelectStmt::RECURSIVE : ObSelectStmt::UNION;
      if (0 == i) {
        card = child->get_card();
      } else {
        card = ObOptSelectivity::get_set_stmt_output_count(card, child->get_card(), set_type);
      }
    } else if (0 == i) {
      card = child_ndv_.at(i);
    } else {
      card = ObOptSelectivity::get_set_stmt_output_count(card, child_ndv_.at(i), get_set_op());
    }
  }
  return ret;
}

int ObLogSet::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  used = true;
  inherit_child_ordering_index = first_child;
  if (HASH_SET == get_algo()) {
    inherit_child_ordering_index = -1;
    used = false;
  } else if (!is_set_distinct()) {
    used = false;
  }
  if (is_recursive_union()) {
    inherit_child_ordering_index = -1;
  }
  return ret;
}
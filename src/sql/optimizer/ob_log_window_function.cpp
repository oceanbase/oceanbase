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
#include "ob_log_window_function.h"
#include "ob_opt_est_cost.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogWindowFunction::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogWindowFunction* win_func = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone sort operator", K(ret));
  } else if (OB_ISNULL(win_func = static_cast<ObLogWindowFunction*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(win_func), K(ret));
  } else {
    for (int64_t i = 0; i < win_exprs_.count() && OB_SUCC(ret); ++i) {
      ret = win_func->win_exprs_.push_back(win_exprs_.at(i));
    }
    if (OB_SUCC(ret)) {
      out = win_func;
    }
  }
  return ret;
}
int ObLogWindowFunction::match_parallel_condition(bool& can_parallel)
{
  int ret = OB_SUCCESS;
  bool flag = true;
  ObWinFunRawExpr* win_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && flag && i < win_exprs_.count(); ++i) {
    win_expr = win_exprs_.at(i);
    switch (win_expr->get_func_type()) {
      case T_FUN_COUNT:
      case T_FUN_SUM:
      case T_FUN_MAX:
      case T_FUN_MIN: {
        /*do nothing*/
        break;
      }
      default: {
        flag = false;
        break;
      }
    }
    if (flag) {
      if (win_expr->get_upper().type_ != BoundType::BOUND_UNBOUNDED ||
          win_expr->get_lower().type_ != BoundType::BOUND_UNBOUNDED) {
        flag = false;
      }
    }
    if (flag) {
      if (win_expr->get_partition_exprs().count() > 0) {
        flag = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    can_parallel = flag;
  }
  return ret;
}

int ObLogWindowFunction::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_UNLIKELY(win_exprs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty window exprs", K(ret));
  } else {
    bool sharding_compatible = false;
    ObSEArray<ObRawExpr*, 4> win_part_exprs;
    ObSEArray<ObRawExpr*, 4> key_exprs;
    if (OB_FAIL(get_win_partition_intersect_exprs(win_exprs_, win_part_exprs))) {
      LOG_WARN("failed to get win partition intersect exprs", K(ret));
    } else if (OB_FAIL(prune_weak_part_exprs(*ctx, win_part_exprs, key_exprs))) {
      LOG_WARN("failed to prune weak part exprs", K(ret));
    } else if (OB_FAIL(check_sharding_compatible_with_reduce_expr(child->get_sharding_info(),
                   key_exprs,
                   child->get_sharding_output_equal_sets(),
                   sharding_compatible))) {
      LOG_WARN("Failed to check sharding compatible with reduce expr", K(ret), K(*child));
    } else if (sharding_compatible) {
      if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
        LOG_WARN("failed to deep copy sharding info from child", K(ret));
      } else {
        is_partition_wise_ = (NULL != sharding_info_.get_phy_table_location_info());
      }
    } else {
      ObExchangeInfo exch_info;
      bool can_parallel = false;
      if (!win_part_exprs.empty()) {
        if (OB_FAIL(exch_info.append_hash_dist_expr(win_part_exprs))) {
          LOG_WARN("failed to append hash dist expr", K(ret));
        } else if (OB_FAIL(sharding_info_.get_partition_keys().assign(win_part_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          sharding_info_.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
        }
      } else if (OB_FAIL(match_parallel_condition(can_parallel))) {
        LOG_WARN("fail to check match parallel condition", K(ret));
      } else if (can_parallel) {
        is_parallel_ = true;
        if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
          LOG_WARN("failed to deep copy sharding info from child", K(ret));
        }
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
        sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
      }
      if (OB_SUCC(ret)) {
        if (!is_parallel_ && OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
          LOG_WARN("failed to allocate exchange", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogWindowFunction::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  ObSEArray<ObRawExpr*, 8> win_exprs;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs_.count(); ++i) {
      ObWinFunRawExpr* raw_expr = win_exprs_.at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(win_exprs.push_back(raw_expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < raw_expr->get_param_count(); ++j) {
          bool should_add = false;
          ObRawExpr* param_expr = raw_expr->get_param_expr(j);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(check_param_expr_should_be_added(param_expr, should_add))) {
            LOG_WARN("failed to check if param should be added", K(ret));
          } else if (should_add && OB_FAIL(temp_exprs.push_back(param_expr))) {
            LOG_WARN("failed to push back exprs", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, win_exprs))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, temp_exprs, producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
        LOG_WARN("failed to add parent need expr", K(ret));
      } else { /*do nothing*/
      }
    }

    // set producer id for aggr expr
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
      ExprProducer& producer = ctx.expr_producers_.at(i);
      if (OB_ISNULL(producer.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_INVALID_ID == producer.producer_id_ && producer.expr_->has_flag(IS_WINDOW_FUNC) &&
                 ObOptimizerUtil::find_item(win_exprs, producer.expr_)) {
        producer.producer_id_ = id_;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogWindowFunction::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs_.count(); ++i) {
    ObWinFunRawExpr* raw_expr = win_exprs_.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < raw_expr->get_param_count(); ++j) {
        if (OB_ISNULL(raw_expr->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(temp_exprs.push_back(raw_expr->get_param_expr(j)))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_exprs.count(); i++) {
    if (OB_ISNULL(temp_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*temp_exprs.at(i)))) {
      LOG_WARN("failed to check output expr", K(ret), K(i));
    } else {
    }
  }
  return ret;
}

int ObLogWindowFunction::is_my_window_expr(const ObRawExpr* expr, bool& is_mine)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> win_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs_.count(); ++i) {
    ObWinFunRawExpr* raw_expr = win_exprs_.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(win_exprs.push_back(raw_expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    is_mine = ObOptimizerUtil::find_item(win_exprs, expr);
  }
  return ret;
  ;
}

int ObLogWindowFunction::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < win_exprs_.count() && OB_SUCC(ret); ++i) {
      if (i != 0 && OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
      ObWinFunRawExpr* win_expr = win_exprs_.at(i);
      EXPLAIN_PRINT_EXPR(win_expr, type);
      // partition by
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else {
          const ObIArray<ObRawExpr*>& partition_by = win_expr->get_partition_exprs();
          EXPLAIN_PRINT_EXPRS(partition_by, type);
        }
      }
      // order by
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else {
          const ObIArray<OrderItem>& order_by = win_expr->get_order_items();
          EXPLAIN_PRINT_SORT_ITEMS(order_by, type);
        }
      }
      // win_type
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
        if (OB_SUCC(ret)) {
          const char* win_type_str = "MAX";
          if (WINDOW_ROWS == win_expr->get_window_type()) {
            win_type_str = "window_type(ROWS), ";
          } else if (WINDOW_RANGE == win_expr->get_window_type()) {
            win_type_str = "window_type(RANGE), ";
          }
          if (OB_FAIL(BUF_PRINTF(win_type_str))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          }
        }
#define PRINT_BOUND(bound_name, bound)                                                                         \
  if (OB_SUCC(ret)) {                                                                                          \
    if (OB_FAIL(BUF_PRINTF(#bound_name "("))) {                                                                \
      LOG_WARN("BUF_PRINTF fails", K(ret));                                                                    \
    } else {                                                                                                   \
      bool print_dir = false;                                                                                  \
      if (BOUND_UNBOUNDED == bound.type_) {                                                                    \
        print_dir = true;                                                                                      \
        if (OB_FAIL(BUF_PRINTF("UNBOUNDED"))) {                                                                \
          LOG_WARN("BUF_PRINTF fails", K(ret));                                                                \
        }                                                                                                      \
      } else if (BOUND_CURRENT_ROW == bound.type_) {                                                           \
        if (OB_FAIL(BUF_PRINTF("CURRENT ROW"))) {                                                              \
          LOG_WARN("BUF_PRINTF fails", K(ret));                                                                \
        }                                                                                                      \
      } else if (BOUND_INTERVAL == bound.type_) {                                                              \
        print_dir = true;                                                                                      \
        if (OB_FAIL(bound.interval_expr_->get_name(buf, buf_len, pos, type))) {                                \
          LOG_WARN("print expr name failed", K(ret));                                                          \
        } else if (!bound.is_nmb_literal_) {                                                                   \
          int64_t date_unit_type = DATE_UNIT_MAX;                                                              \
          ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(bound.date_unit_expr_);                      \
          if (OB_ISNULL(con_expr)) {                                                                           \
            ret = OB_ERR_UNEXPECTED;                                                                           \
            LOG_WARN("con_expr should not be NULL", K(ret));                                                   \
          } else {                                                                                             \
            con_expr->get_value().get_int(date_unit_type);                                                     \
            const static char* date_unit = ob_date_unit_type_str(static_cast<ObDateUnitType>(date_unit_type)); \
            if (OB_FAIL(BUF_PRINTF(" %s", date_unit))) {                                                       \
              LOG_WARN("BUF_PRINTF fails", K(ret));                                                            \
            }                                                                                                  \
          }                                                                                                    \
        }                                                                                                      \
      }                                                                                                        \
      if (OB_SUCC(ret) && print_dir) {                                                                         \
        if (bound.is_preceding_) {                                                                             \
          if (OB_FAIL(BUF_PRINTF(" PRECEDING"))) {                                                             \
            LOG_WARN("BUF_PRINTF fails", K(ret));                                                              \
          }                                                                                                    \
        } else {                                                                                               \
          if (OB_FAIL(BUF_PRINTF(" FOLLOWING"))) {                                                             \
            LOG_WARN("BUF_PRINTF fails", K(ret));                                                              \
          }                                                                                                    \
        }                                                                                                      \
      }                                                                                                        \
      if (OB_SUCC(ret)) {                                                                                      \
        if (OB_FAIL(BUF_PRINTF(")"))) {                                                                        \
          LOG_WARN("BUF_PRINTF fails", K(ret));                                                                \
        }                                                                                                      \
      }                                                                                                        \
    }                                                                                                          \
  }

        PRINT_BOUND(upper, win_expr->get_upper());
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
        PRINT_BOUND(lower, win_expr->get_lower());
      }
    }
  }

  return ret;
}

int ObLogWindowFunction::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* first_child = get_child(ObLogicalOperator::first_child);
  double op_cost = 0.0;
  // compute window for operator
  if (OB_ISNULL(first_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret), K(first_child));
  } else if (OB_FAIL(ObOptEstCost::cost_window_function(first_child->get_card(), op_cost))) {
    LOG_WARN("calculate cost of window function failed", K(ret));
  } else {
    set_card(first_child->get_card());
    set_op_cost(op_cost);
    set_cost(first_child->get_cost() + op_cost);
    // TODO () add widths of winexprs
    set_width(first_child->get_width());
  }
  return ret;
}

int ObLogWindowFunction::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  double op_cost = op_cost_;
  ObLogicalOperator* child = NULL;
  UNUSED(parent);
  re_est = false;
  if (need_row_count >= get_card()) {
    /* do nothing */
  } else if (OB_ISNULL(child = get_child(first_child))) {
    LOG_WARN("get child failed", K(ret), K_(card), K(need_row_count));
  } else if (OB_FAIL(child->re_est_cost(this, need_row_count, re_est))) {
    LOG_WARN("re-estimate cost of child failed", K(ret), K(re_est));
  } else if (OB_FAIL(ObOptEstCost::cost_window_function(need_row_count, op_cost))) {
    LOG_WARN("cost window function failed", K(ret));
  } else {
    re_est = true;
    op_cost_ = op_cost;
    cost_ = op_cost + child->get_cost();
    set_card(need_row_count);
  }
  return ret;
}

int ObLogWindowFunction::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  reset_local_ordering();
  ObLogicalOperator* child = get_child(first_child);
  bool need_sort = false;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(check_need_sort_below_node(0, expected_ordering_, need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (need_sort && OB_FAIL(allocate_sort_below(0, expected_ordering_))) {
    LOG_WARN("failed to allocate implicit sort", K(ret));
  } else if (OB_ISNULL(child = get_child(first_child))) {  // get child again for sort and defend null value
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op's ordering", K(ret));
  }
  return ret;
}

uint64_t ObLogWindowFunction::hash(uint64_t seed) const
{
  HASH_PTR_ARRAY(win_exprs_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogWindowFunction::allocate_granule_pre(AllocGIContext& ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogWindowFunction::allocate_granule_post(AllocGIContext& ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogWindowFunction::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs_.count(); ++i) {
    if (win_exprs_.at(i)->get_agg_expr() != NULL) {
      ObRawExpr* expr = NULL;
      switch (win_exprs_.at(i)->get_agg_expr()->get_expr_type()) {
        case T_FUN_GROUP_CONCAT: {
          expr = win_exprs_.at(i)->get_agg_expr()->get_separator_param_expr();
          OZ(raw_exprs.append(expr));
          break;
        }
        case T_FUN_MEDIAN:
        case T_FUN_GROUP_PERCENTILE_CONT: {
          expr = win_exprs_.at(i)->get_agg_expr()->get_linear_inter_expr();
          OZ(raw_exprs.append(expr));
          break;
        }
        default:
          break;
      }
    }
  }
  return ret;
}

int ObLogWindowFunction::get_win_partition_intersect_exprs(
    ObIArray<ObWinFunRawExpr*>& win_exprs, ObIArray<ObRawExpr*>& win_part_exprs)
{
  int ret = OB_SUCCESS;
  if (win_exprs.count() > 0) {
    if (OB_ISNULL(win_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(append(win_part_exprs, win_exprs.at(0)->get_partition_exprs()))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < win_exprs.count(); ++i) {
        ObWinFunRawExpr* win_expr = win_exprs.at(i);
        if (OB_ISNULL(win_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(
                       win_part_exprs, win_expr->get_partition_exprs(), win_part_exprs))) {
          LOG_WARN("failed to intersect exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

bool ObLogWindowFunction::is_block_op() const
{
  bool is_block_op = true;
  // 对于window function算子, 在没有partition by以及完整窗口情况下,
  // 所有数据作为一个窗口, 认为是block算子
  // 在其他情况下, 认为是非block算子
  ObWinFunRawExpr* win_expr = NULL;
  for (int64_t i = 0; i < win_exprs_.count(); ++i) {
    if (OB_ISNULL(win_expr = win_exprs_.at(i))) {
      LOG_ERROR("win expr is null");
    } else if (win_expr->get_partition_exprs().count() > 0 ||
               win_expr->get_upper().type_ != BoundType::BOUND_UNBOUNDED ||
               win_expr->get_lower().type_ != BoundType::BOUND_UNBOUNDED) {
      is_block_op = false;
      break;
    }
  }
  return is_block_op;
}

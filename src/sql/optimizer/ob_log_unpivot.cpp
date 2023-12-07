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
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogUnpivot::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  }
  return ret;
}

int ObLogUnpivot::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObColumnRefRawExpr*, 4> column_exprs;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(get_subquery_id(), column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ColumnItem has a NULL expr", K(expr), K(ret));
      } else if (expr->is_explicited_reference() &&
                 OB_FAIL(access_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, static_cast<ObRawExpr*>(expr)))){
        LOG_PRINT_EXPR(WARN, "failed to add expr to access", expr, K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogUnpivot::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); ++i) {
    if (OB_ISNULL(access_exprs_.at(i)) || OB_UNLIKELY(!access_exprs_.at(i)->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret));
    } else {
      ObColumnRefRawExpr *expr = static_cast<ObColumnRefRawExpr*>(access_exprs_.at(i));
      LOG_PRINT_EXPR(DEBUG, "succ to add expr to access", expr, "producer", id_);
      if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
        LOG_PRINT_EXPR(WARN, "failed to mark expr as produced", expr, "producer", id_);
      } else {
        LOG_PRINT_EXPR(DEBUG, "succ to produce expr", expr, "producer", id_, K(expr));
        if (!is_plan_root()) {
          if (!expr->is_unpivot_mocked_column()) {
            if (OB_FAIL(add_var_to_array_no_dup(output_exprs_,
                                                static_cast<ObRawExpr*>(expr)))) {
              LOG_PRINT_EXPR(WARN, "failed to add expr to output_exprs_", expr, K(ret));
            } else {
              LOG_PRINT_EXPR(DEBUG, "succ to add expr to output_exprs_", expr, K(ret));
            }
          } else {
            LOG_PRINT_EXPR(DEBUG, "expr in unpivot no need output", expr, K(ret));
          }
        } else {
          LOG_PRINT_EXPR(DEBUG, "succ to add expr to output_exprs_", expr, K(ret));
        }
      }
    }
  }

  if(OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogUnpivot::get_plan_item_info(PlanText &plan_text,
                                     ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF("unpivot(%s,%ld,%ld,%ld), ",
        unpivot_info_.is_include_null_ ? "include" : "exclude",
        unpivot_info_.old_column_count_,
        unpivot_info_.for_column_count_,
        unpivot_info_.unpivot_column_count_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    const ObIArray<ObRawExpr*> &access = get_access_exprs();
    EXPLAIN_PRINT_EXPRS(access, type);
    END_BUF_PRINT(plan_item.access_predicates_,
                  plan_item.access_predicates_len_);
  }
  if (OB_SUCC(ret)) {
    ObString &name = get_subquery_name();
    BUF_PRINT_OB_STR(name.ptr(),
                     name.length(),
                     plan_item.object_alias_,
                     plan_item.object_alias_len_);
  }
  return ret;
}

int ObLogUnpivot::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  const ObSelectStmt *child_stmt = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(child->get_stmt()) ||
      OB_ISNULL(parent_stmt = get_stmt()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_plan()), K(parent_stmt), K(ret));
  } else if (OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(child->get_stmt()->get_stmt_type()), K(ret));
  } else if (FALSE_IT(child_stmt = static_cast<const ObSelectStmt*>(child->get_stmt()))) {
    /*do nothing*/
  } else if (!child->is_distributed()) {
    ret = ObLogicalOperator::compute_sharding_info();
  } else {
    ObShardingInfo *strong_sharding = NULL;
    ObSEArray<ObShardingInfo*, 8> weak_sharding;
    for (int64_t i = -1; OB_SUCC(ret) && i < child->get_weak_sharding().count(); i++) {
      bool exist_in_unpivot = false;
      ObShardingInfo *sharding =
          (i == -1) ? child->get_strong_sharding() : child->get_weak_sharding().at(i);
      if (NULL != sharding) {
        for (int64_t j = unpivot_info_.old_column_count_;
            !exist_in_unpivot && j < child_stmt->get_select_items().count(); ++j) {
          const ObRawExpr *expr = child_stmt->get_select_item(j).expr_;
          exist_in_unpivot = ObOptimizerUtil::find_item(sharding->get_partition_keys(), expr)
              || ObOptimizerUtil::find_item(sharding->get_sub_partition_keys(), expr);
        }
        if (exist_in_unpivot && i == -1) {
          strong_sharding = get_plan()->get_optimizer_context().get_distributed_sharding();
        } else if (!exist_in_unpivot && i == -1) {
          strong_sharding = child->get_strong_sharding();
        } else if (!exist_in_unpivot && i != -1) {
          ret = weak_sharding.push_back(child->get_weak_sharding().at(i));
        } else { /*do nothing*/ }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObJoinOrder::convert_subplan_scan_sharding_info(*get_plan(),
                                                                  *child,
                                                                  subquery_id_,
                                                                  strong_sharding_,
                                                                  weak_sharding_))) {
        LOG_WARN("failed to convert subplan scan sharding info", K(ret));
      } else {
        LOG_TRACE("succeed to convert unpovit scan sharding info", K(ret));
      }
    }
  }
  return ret;
}

int ObLogUnpivot::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(append_array_no_dup(output_exprs,
                                         get_plan()->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output unpivot exprs", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for unpivot", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogUnpivot::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));  
  } else {
    card_ = child->get_card();
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost_ = ObOptEstCost::cost_filter_rows(card_ /parallel, 
                                              get_filter_exprs(),
                                              opt_ctx);
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogUnpivot::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> select_exprs;
  if (OB_ISNULL(get_stmt())
      || OB_UNLIKELY(!get_stmt()->is_select_stmt())
      || OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else {
    ObLogicalOperator *child = NULL;
    if (OB_ISNULL(child= get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator does not have child", K(ret));
    } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    }
    const ObSelectStmt &select_stmt = *static_cast<const ObSelectStmt *>(get_stmt());
    const ObUnpivotInfo unpivot_info = select_stmt.get_unpivot_info();
    if (unpivot_info.old_column_count_ > 0) {
      if (OB_FAIL(select_stmt.get_select_exprs(select_exprs, true))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else {
        int64_t expr_count = select_exprs.count();
        for (int64_t i = expr_count - 1;  OB_SUCC(ret)&& i >= unpivot_info.old_column_count_; i--) {
          ret = select_exprs.remove(i);
        }
        expr_count = select_exprs.count();
        for (int64_t i = expr_count - 1;  OB_SUCC(ret) && i >= 0; i--) {
          bool found = false;
          for (int64_t j = 0;  !found && j < child->get_op_ordering().count(); ++j) {
            found = select_exprs.at(i) == child->get_op_ordering().at(j).expr_;
          }
          if (!found) {
            ret = select_exprs.remove(i);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObOptimizerUtil::make_sort_keys(select_exprs, get_op_ordering()))) {
          LOG_WARN("failed to copy sort keys", K(ret));
        }
      }
      LOG_DEBUG("finish compute_op_ordering", K(select_exprs.count()), K(unpivot_info),
                K(get_op_ordering()), K(select_exprs));
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObLogUnpivot::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))
      || OB_ISNULL(my_plan_)
      || OB_ISNULL(get_stmt())
      || OB_UNLIKELY(!get_stmt()->is_select_stmt())
      || OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(*get_stmt());
    const ObUnpivotInfo unpivot_info = select_stmt.get_unpivot_info();
    ObFdItemSet *fd_item_set = NULL;
    const ObFdItemSet &child_fd_item_set = child->get_fd_item_set();
    if (0 == unpivot_info.old_column_count_ || child_fd_item_set.empty()) {
      //do nothing
    } else {
      ObSEArray<ObRawExpr *, 1> value_exprs;
      for (int64_t i = 0;  OB_SUCC(ret) && i < child_fd_item_set.count(); i++) {
        ObRawExpr *expr = NULL;
        if (OB_NOT_NULL(child_fd_item_set[i])
            && OB_NOT_NULL(child_fd_item_set[i]->get_parent_exprs())
            && child_fd_item_set[i]->get_parent_exprs()->count() == 1
            && OB_NOT_NULL(expr = child_fd_item_set[i]->get_parent_exprs()->at(0))) {
          for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_info.old_column_count_; ++i) {
            if (expr == select_stmt.get_select_item(i).expr_) {
              ObTableFdItem *fd_item = NULL;
              value_exprs.reuse();
              if (OB_FAIL(value_exprs.push_back(expr))) {
                LOG_WARN("failed to push back expr", K(ret));
              } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(fd_item,
                  true, value_exprs, get_table_set()))) {
                LOG_WARN("failed to create fd item", K(ret));
              } else if (NULL == fd_item_set) {
                if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
                  LOG_WARN("failed to create fd item set", K(ret));
                }
              }

              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
                LOG_WARN("failed to push back fd item", K(ret));
              }
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_NOT_NULL(fd_item_set) && // rollup 时 fd_item_set is null
               OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    } else {
      set_fd_item_set(fd_item_set);
    }
  }
  return ret;
}

int ObLogUnpivot::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  set_is_at_most_one_row(false);
  return ret;
}

int ObLogUnpivot::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(access_exprs_, expr);
  return OB_SUCCESS;
}

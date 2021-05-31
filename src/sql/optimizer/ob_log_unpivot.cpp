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

int ObLogUnpivot::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogUnpivot* unpivot = NULL;
  out = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogUnpivot", K(ret));
  } else if (OB_ISNULL(unpivot = static_cast<ObLogUnpivot*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperaotr* to ObLogSubplanScan*", K(ret));
  } else {
    unpivot->unpivot_info_ = unpivot_info_;
    unpivot->set_subquery_id(get_subquery_id());
    unpivot->get_subquery_name().assign_ptr(get_subquery_name().ptr(), get_subquery_name().length());
    if (OB_FAIL(append(unpivot->access_exprs_, access_exprs_))) {
      LOG_WARN("failed to copy access exprs", K(ret));
    } else {
      out = unpivot;
    }
  }
  return ret;
}

int ObLogUnpivot::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL Pointer Error", K(get_plan()), K(get_plan()->get_stmt()));
  } else {
    // see if we can produce some expr
    // all column related expressions should have been added as column items
    const ObDMLStmt* stmt = get_plan()->get_stmt();
    LOG_DEBUG("before allocate_expr_post",
        "stmt",
        stmt->is_select_stmt() ? *(static_cast<const ObSelectStmt*>(stmt)) : *stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (NULL != col_item && col_item->table_id_ == get_subquery_id()) {
        ObColumnRefRawExpr* expr = const_cast<ObColumnRefRawExpr*>(col_item->get_expr());
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ColumnItem has a NULL expr", K(*col_item), K(ret));
        } else if (!expr->is_explicited_reference()) {
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, static_cast<ObRawExpr*>(expr)))) {
          LOG_PRINT_EXPR(WARN, "failed to add expr to access", expr, K(ret));
        } else {
          LOG_PRINT_EXPR(DEBUG, "succ to add expr to access", expr, "producer", id_);
          bool expr_is_required = false;
          if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx, expr_is_required))) {
            LOG_PRINT_EXPR(WARN, "failed to mark expr as produced", expr, "producer", id_);
          } else {
            LOG_PRINT_EXPR(DEBUG, "succ to produce expr", expr, "producer", id_, K(expr));
            if (!is_plan_root()) {
              if (!expr->is_unpivot_mocked_column()) {
                if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, static_cast<ObRawExpr*>(expr)))) {
                  LOG_PRINT_EXPR(WARN, "failed to add expr to output_exprs_", expr, K(ret));
                } else {
                  LOG_PRINT_EXPR(DEBUG, "succ to add expr to output_exprs_", expr, K(ret));
                }
              } else {
                LOG_PRINT_EXPR(DEBUG, "expr in unpivot no need output", expr, K(ret));
              }
            } else {
              LOG_PRINT_EXPR(DEBUG, "expr is produced but no one has requested it", expr, K(ret));
            }
          }
        }
      } else {
        LOG_TRACE("skip column item that does not belong to this table",
            "column table id",
            col_item->table_id_,
            K(get_subquery_id()));
      }
    }
  }

  // check if we can produce some more exprs, such as 1 + 'c1' after we have produced 'c1'
  if (OB_SUCC(ret)) {
    ret = ObLogicalOperator::allocate_expr_post(ctx);
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogUnpivot::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int64_t old_column_count_;
  int64_t unpivot_column_count_;
  int64_t for_column_count_;
  bool is_include_null_;
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("unpivot(%s,%ld,%ld,%ld), ",
                 unpivot_info_.is_include_null_ ? "include" : "exclude",
                 unpivot_info_.old_column_count_,
                 unpivot_info_.for_column_count_,
                 unpivot_info_.unpivot_column_count_))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access = get_access_exprs();
    EXPLAIN_PRINT_EXPRS(access, type);
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogUnpivot::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObLogicalOperator* child = NULL;
  ObSelectStmt* child_stmt = NULL;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(child->get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_plan()), K(ctx), K(ret));
  } else if (OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(child->get_stmt()->get_stmt_type()), K(ret));
  } else if (FALSE_IT(child_stmt = static_cast<ObSelectStmt*>(child->get_stmt()))) {
    /*do nothing*/
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else {
    ObShardingInfo& sharding_info = child->get_sharding_info();
    bool exist_in_unpivot = false;
    for (int64_t i = unpivot_info_.old_column_count_; !exist_in_unpivot && i < child_stmt->get_select_items().count();
         ++i) {
      ObRawExpr* expr = child_stmt->get_select_item(i).expr_;
      exist_in_unpivot = has_exist_in_array(sharding_info.get_partition_keys(), expr) ||
                         has_exist_in_array(sharding_info.get_sub_partition_keys(), expr);
    }

    if (exist_in_unpivot) {
      sharding_info_.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
    } else {
      ObSEArray<ObRawExpr*, 4> out_part_keys;
      ObSEArray<ObRawExpr*, 4> out_sub_part_keys;
      ObDMLStmt* parent_stmt = get_stmt();
      ObSEArray<ObRawExpr*, 4> new_sharding_conds;
      if (OB_FAIL(get_sharding_info().copy_without_part_keys(child->get_sharding_info()))) {
        LOG_WARN("failed to copy sharding info from children", K(ret));
      } else if (OB_FAIL(
                     ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                         child->get_sharding_output_equal_sets(),
                         subquery_id_,
                         *parent_stmt,
                         *child_stmt,
                         false,
                         child->get_sharding_info().get_partition_keys(),
                         sharding_info_.get_partition_keys()))) {
        LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (OB_FAIL(
                     ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                         child->get_sharding_output_equal_sets(),
                         subquery_id_,
                         *parent_stmt,
                         *child_stmt,
                         false,
                         child->get_sharding_info().get_sub_partition_keys(),
                         sharding_info_.get_sub_partition_keys()))) {
        LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (OB_FAIL(
                     ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                         child->get_sharding_output_equal_sets(),
                         subquery_id_,
                         *parent_stmt,
                         *child_stmt,
                         false,
                         child->get_sharding_info().get_partition_func(),
                         sharding_info_.get_partition_func()))) {
        LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (OB_FAIL(
                     ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                         child->get_sharding_output_equal_sets(),
                         subquery_id_,
                         *parent_stmt,
                         *child_stmt,
                         true,
                         ctx->sharding_conds_,
                         new_sharding_conds))) {
        LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (OB_FAIL(ctx->sharding_conds_.assign(new_sharding_conds))) {
        LOG_WARN("failed to assign expr array", K(ret));
      }
    }
    LOG_DEBUG("unpivot sharding info",
        K(sharding_info_),
        K(child->get_sharding_info()),
        K(child_stmt->get_select_items()),
        K(unpivot_info_),
        K(exist_in_unpivot));
  }
  return ret;
}

int ObLogUnpivot::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child pointer is null", K(ret));
  } else {
    card_ = child->get_card();
    // same with subplan scan
    op_cost_ = ObOptEstCost::cost_subplan_scan(card_, get_filter_exprs().count());
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogUnpivot::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  ObLogicalOperator* child = NULL;
  if (need_row_count >= get_card() || std::fabs(get_card()) < OB_DOUBLE_EPSINON) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(child->re_est_cost(this, child->get_card() * need_row_count / get_card(), re_est))) {
    LOG_WARN("Failed to re_est child cost", K(ret));
  } else {
    card_ = need_row_count;
    // same with subplan scan
    op_cost_ = ObOptEstCost::cost_subplan_scan(child->get_card(), get_filter_exprs().count());
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogUnpivot::calc_cost()
{
  return OB_SUCCESS;
}

int ObLogUnpivot::gen_filters()
{
  return OB_SUCCESS;
}

int ObLogUnpivot::gen_output_columns()
{
  return OB_SUCCESS;
}

int ObLogUnpivot::set_properties()
{
  return OB_SUCCESS;
}

int ObLogUnpivot::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  ObLogicalOperator* child = get_child(first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(set_local_ordering(child->get_local_ordering()))) {
    LOG_WARN("failed to set local ordering", K(ret));
  }
  return ret;
}

int ObLogUnpivot::transmit_op_ordering()
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
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to set op's ordering", K(ret));
  }
  LOG_DEBUG("finish compute_op_ordering", K(need_sort), K(get_op_ordering()), K(expected_ordering_));
  return ret;
}

int ObLogUnpivot::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> select_exprs;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else {
    ObLogicalOperator* child = NULL;
    if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator does not have child", K(ret));
    } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    }
    ObSelectStmt& select_stmt = *static_cast<ObSelectStmt*>(get_stmt());
    const ObUnpivotInfo unpivot_info = select_stmt.get_unpivot_info();
    if (unpivot_info.old_column_count_ > 0) {
      if (OB_FAIL(select_stmt.get_select_exprs(select_exprs, true))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else {
        int64_t expr_count = select_exprs.count();
        for (int64_t i = expr_count - 1; OB_SUCC(ret) && i >= unpivot_info.old_column_count_; i--) {
          ret = select_exprs.remove(i);
        }
        expr_count = select_exprs.count();
        for (int64_t i = expr_count - 1; OB_SUCC(ret) && i >= 0; i--) {
          bool found = false;
          for (int64_t j = 0; !found && j < child->get_op_ordering().count(); ++j) {
            found = select_exprs.at(i) == child->get_op_ordering().at(j).expr_;
          }
          if (!found) {
            ret = select_exprs.remove(i);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObOptimizerUtil::copy_sort_keys(select_exprs, get_op_ordering()))) {
          LOG_WARN("failed to copy sort keys", K(ret));
        }
      }
      LOG_DEBUG("finish compute_op_ordering",
          K(select_exprs.count()),
          K(unpivot_info),
          K(get_op_ordering()),
          K(select_exprs));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogUnpivot::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_select_stmt()) || OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else {
    ObSelectStmt& select_stmt = *static_cast<ObSelectStmt*>(get_stmt());
    const ObUnpivotInfo unpivot_info = select_stmt.get_unpivot_info();
    ObFdItemSet* fd_item_set = NULL;
    const ObFdItemSet& child_fd_item_set = child->get_fd_item_set();
    if (0 == unpivot_info.old_column_count_ || child_fd_item_set.empty()) {
      // do nothing
    } else {
      const int32_t stmt_level = get_stmt()->get_current_level();
      ObSEArray<ObRawExpr*, 1> value_exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_fd_item_set.count(); i++) {
        ObRawExpr* expr = NULL;
        if (OB_NOT_NULL(child_fd_item_set[i]) && OB_NOT_NULL(child_fd_item_set[i]->get_parent_exprs()) &&
            child_fd_item_set[i]->get_parent_exprs()->count() == 1 &&
            OB_NOT_NULL(expr = child_fd_item_set[i]->get_parent_exprs()->at(0))) {
          for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_info.old_column_count_; ++i) {
            if (expr == select_stmt.get_select_item(i).expr_) {
              ObTableFdItem* fd_item = NULL;
              value_exprs.reuse();
              if (OB_FAIL(value_exprs.push_back(expr))) {
                LOG_WARN("failed to push back expr", K(ret));
              } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(
                             fd_item, true, value_exprs, stmt_level, get_table_set()))) {
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
    } else if (OB_NOT_NULL(fd_item_set) && OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
      LOG_WARN("falied to deduce fd item set", K(ret));
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

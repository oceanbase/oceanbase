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

bool ObLogUnpivot::is_in_old_columns(const ObSelectStmt &select_stmt,
                                     const ObUnpivotInfo &unpivot_info,
                                     ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  bool is_in = false;
  if (0 == unpivot_info.old_column_count_) {
    //do nothing
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < unpivot_info_.old_column_count_; ++j) {
      if (expr == select_stmt.get_select_item(j).expr_) {
        is_in = true;
        break;
      }
    }
  }
  return is_in;
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
  if (OB_ISNULL(child = get_child(first_child))
      || OB_ISNULL(child->get_stmt()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child), K(get_plan()));
  } else if (OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(child->get_stmt()->get_stmt_type()), K(ret));
  } else if (FALSE_IT(child_stmt = static_cast<const ObSelectStmt*>(child->get_stmt()))) {
    /*do nothing*/
  } else if (!child->is_distributed()) {
    ret = ObLogicalOperator::compute_sharding_info();
  } else {
    for (int64_t i = -1; OB_SUCC(ret) && i < child->get_weak_sharding().count(); ++i) {
      bool exist_in_unpivot = false;
      bool is_inherited_sharding = false;
      ObShardingInfo *sharding =
          (i == -1) ? child->get_strong_sharding() : child->get_weak_sharding().at(i);
      if (NULL != sharding) {
        for (int64_t j = unpivot_info_.old_column_count_;
            !exist_in_unpivot && j < child_stmt->get_select_items().count(); ++j) {
          const ObRawExpr *expr = child_stmt->get_select_item(j).expr_;
          exist_in_unpivot = ObOptimizerUtil::find_item(sharding->get_partition_keys(), expr)
              || ObOptimizerUtil::find_item(sharding->get_sub_partition_keys(), expr);
        }
        // Only inherit the sharding when partition keys do not exist in the unpivot
        // "for" clause, because those columns will be output by unpivot op directly.
        if (-1 == i) {
          // strong sharding
          if (exist_in_unpivot) {
            // strong sharding, can not inherit
            strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
            inherit_sharding_index_ = -1;
          } else {
            // strong sharding, can inherit
            if (OB_FAIL(ObJoinOrder::convert_subplan_scan_sharding_info(*get_plan(),
                                                                        *child,
                                                                        subquery_id_,
                                                                        true, // is_strong
                                                                        sharding,
                                                                        strong_sharding_,
                                                                        is_inherited_sharding))) {
              LOG_WARN("failed to convert sharding info", K(ret));
            } else {
              inherit_sharding_index_ = is_inherited_sharding ? 0 : -1;
            }
          }
        } else if (!exist_in_unpivot) {
          // weak sharding, can inherit
          ObShardingInfo *temp_sharding = NULL;
          if (OB_FAIL(ObJoinOrder::convert_subplan_scan_sharding_info(*get_plan(),
                                                                      *child,
                                                                      subquery_id_,
                                                                      false, // is_strong
                                                                      sharding,
                                                                      temp_sharding,
                                                                      is_inherited_sharding))) {
            LOG_WARN("failed to convert sharding info", K(ret));
          } else if (OB_NOT_NULL(temp_sharding)) {
            inherit_sharding_index_ = 0;
            if (OB_FAIL(weak_sharding_.push_back(temp_sharding))) {
              LOG_WARN("failed to push back weak sharding", K(ret));
            }
          }
        } else { /* weak sharding, can not inherit, do nothing */ }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to convert unpovit scan sharding info", KPC(strong_sharding_), K(weak_sharding_), K(inherit_sharding_index_));
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
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
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
    }
    bool found = true;
    const ObSelectStmt &select_stmt = *static_cast<const ObSelectStmt *>(get_stmt());
    const ObUnpivotInfo &unpivot_info = select_stmt.get_unpivot_info();
    for (int64_t j = 0; OB_SUCC(ret) && found && j < child->get_op_ordering().count(); ++j) {
      if (is_in_old_columns(select_stmt, unpivot_info, child->get_op_ordering().at(j).expr_)) {
        if (OB_FAIL(ordering_exprs.push_back(child->get_op_ordering().at(j).expr_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        found = false;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptimizerUtil::make_sort_keys(ordering_exprs, get_op_ordering()))) {
        LOG_WARN("failed to copy sort keys", K(ret));
      }
    }
    LOG_DEBUG("finish compute_op_ordering", K(unpivot_info), K(get_op_ordering()));
  }
  return ret;
}

int ObLogUnpivot::compute_fd_item_set()
{

  int ret = OB_SUCCESS;
  set_fd_item_set(NULL);
  return ret;
}

int ObLogUnpivot::compute_const_exprs()
{
  // output_const_exprs should be the intersection of 'const_expr of child' and 'old_columns'
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0) ||
      OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(*get_stmt());
    const ObUnpivotInfo &unpivot_info = select_stmt.get_unpivot_info();
    const ObIArray<ObRawExpr *> &child_output_const_exprs= child->get_output_const_exprs();
    if (0 == unpivot_info.old_column_count_ || child_output_const_exprs.empty()) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_output_const_exprs.count(); ++i) {
        if (is_in_old_columns(select_stmt, unpivot_info, child_output_const_exprs.at(i))) {
          if (OB_FAIL(output_const_exprs_.push_back(child_output_const_exprs.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret) || filter_exprs_.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(filter_exprs_, output_const_exprs_))) {
      LOG_WARN("failed to compute const conditionexprs", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObLogUnpivot::compute_equal_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  EqualSets *ordering_esets = NULL;
  EqualSets *old_col_esets = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0) ||
      OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_unpivot_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    // do nothing
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_ISNULL(old_col_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else {
    // get old_col_esets (equal set of old columns)
    const EqualSets &child_esets = child->get_output_equal_sets();
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(*get_stmt());
    const ObUnpivotInfo &unpivot_info = select_stmt.get_unpivot_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_esets.count(); ++i) {
      ObSEArray<ObRawExpr *, 4> eset_builder;
      EqualSet *child_eset = child_esets.at(i);
      if (OB_ISNULL(child_eset)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child_eset));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < child_eset->count(); ++j) {
        if (is_in_old_columns(select_stmt, unpivot_info, child_eset->at(j))) {
          if (OB_FAIL(eset_builder.push_back(child_eset->at(j)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObRawExprSetUtils::add_expr_set(
              &get_plan()->get_allocator(), eset_builder, *old_col_esets))) {
        LOG_WARN("failed to add expr set", K(ret));
      }
    }

    // compute equal set of unpivot log plan
    if (OB_FAIL(ret)) {
    } else if (filter_exprs_.empty()) {
      // inherit equal sets from old_col_esets
      set_output_equal_sets(old_col_esets);
    } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create equal sets", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                        &my_plan_->get_allocator(),
                        filter_exprs_,
                        *old_col_esets,
                        *ordering_esets))) {
      LOG_WARN("failed to compute ordering output equal set", K(ret));
    } else {
      set_output_equal_sets(ordering_esets);
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

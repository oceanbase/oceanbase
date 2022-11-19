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
#include "sql/optimizer/ob_log_table_lookup.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

int ObLogTableLookup::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_plan()->get_rowkey_exprs(table_id_,
                                                  ref_table_id_,
                                                  rowkey_exprs_))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, rowkey_exprs_))) {
    LOG_WARN("failed to append rowkey exprs", K(ret));
  } else if (NULL != calc_part_id_expr_ && OB_FAIL(all_exprs.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to add expr to ctx", K(ret));
  } else if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append access exprs", K(ret));
  } else if (NULL != fq_expr_ && OB_FAIL(all_exprs.push_back(fq_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableLookup::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(stmt), KP(get_plan()));
  } else if (use_batch()
      && OB_FAIL(ObOptimizerUtil::allocate_group_id_expr(get_plan(), group_id_expr_))) {
    LOG_WARN("failed to allocate group id expr", K(ret));
  } else if (use_batch() && OB_FAIL(access_exprs_.push_back(group_id_expr_))) {
    LOG_WARN("failed to push back group id expr", K(ret));
  } else if (OB_FAIL(append(access_exprs_, rowkey_exprs_))) {
    LOG_WARN("failed to append rowkey exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      const ObRawExpr *expr = NULL;
      if (OB_ISNULL(col_item) || OB_ISNULL(expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(expr), K(col_item));
      } else if (col_item->table_id_ == table_id_ &&
                 expr->is_explicited_reference() &&
                 OB_FAIL(add_var_to_array_no_dup(access_exprs_, const_cast<ObRawExpr *>(expr)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/}
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
      ObRawExpr *expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type() &&
          static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id_ &&
          OB_FAIL(access_exprs_.push_back(expr))) {
        LOG_WARN("failed to get next row", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObLogTableLookup::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); i++) {
    ObRawExpr *expr = access_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
      LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_plan_root() && OB_FAIL(append(output_exprs_, access_exprs_))) {
    LOG_WARN("failed to append output exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  }
  return ret;
}

int ObLogTableLookup::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  ObLogTableScan *scan = NULL;
  int parallel = 1.0;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN != child->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected child type", K(child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else {
    double selectivity = 1.0;
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double op_cost = ObOptEstCost::cost_get_rows(child_card / parallel, opt_ctx.get_cost_model_type());
    double index_back_cost = 0.0;
    scan = static_cast<ObLogTableScan*>(child);
    if (OB_FAIL(scan->re_est_cost(param, child_card, index_back_cost, child_cost))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                              get_plan()->get_selectivity_ctx(),
                                                              get_filter_exprs(),
                                                              selectivity,
                                                              get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret));
    } else {
      op_cost += index_back_cost;
      cost = child_cost + op_cost;
      card = child_card * selectivity;
      if (param.override_) {
        set_op_cost(op_cost);
        set_cost(cost);
        set_card(card);
      }
    }
  }
  return ret;
}

int ObLogTableLookup::compute_property(Path *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(path->parent_)
      || OB_UNLIKELY(!path->is_access_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path or join order is null", K(ret), K(path));
  } else if (OB_FAIL(ObLogicalOperator::compute_property(path))) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
    set_card(path->get_path_output_rows());
    set_op_cost(static_cast<const AccessPath*>(path)->index_back_cost_);
    set_cost(path->cost_);
  }
  return ret;
}

uint64_t ObLogTableLookup::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(table_name_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}


int ObLogTableLookup::print_my_plan_annotation(char *buf,
                                               int64_t &buf_len,
                                               int64_t &pos,
                                               ExplainType type)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  // print partition
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(explain_print_partitions(*table_partition_info_, buf, buf_len, pos))) {
    LOG_WARN("failed to explain print partitions", K(ret));
  }
  return ret;
}

int ObLogTableLookup::replace_gen_column(ObRawExpr *part_expr, ObRawExpr *&new_part_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  new_part_expr = part_expr;
  if (OB_ISNULL(part_expr)) {
    // do nothing
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("fail to extract column exprs", K(part_expr), K(ret));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    bool cnt_gen_columns = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      if (OB_ISNULL(column_exprs.at(i)) ||
          OB_UNLIKELY(!column_exprs.at(i)->is_column_ref_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(column_exprs.at(i));
        if (!col->is_generated_column()) {
          // do nothing
        } else if (OB_ISNULL(col->get_dependant_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dependant expr is null", K(ret), K(*col));
        } else if (OB_FAIL(copier.add_replaced_expr(col, col->get_dependant_expr()))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else {
          cnt_gen_columns = true;
        }
      }
    }
    if (OB_SUCC(ret) && cnt_gen_columns) {
      if (OB_FAIL(copier.copy_on_replace(part_expr, new_part_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogTableLookup::check_output_dependance(ObIArray<ObRawExpr *> &child_output, PPDeps &deps)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start to check output exprs", K(deps));
  ObRawExprCheckDep dep_checker(child_output, deps, false);
  ObSEArray<ObRawExpr *, 8> rowkey_and_calc_part_exprs;
  if (OB_FAIL(rowkey_and_calc_part_exprs.assign(rowkey_exprs_))) {
    LOG_WARN("failed to assign rowkey exprs", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(rowkey_and_calc_part_exprs, calc_part_id_expr_))) {
    LOG_WARN("failed to add part id expr", K(ret));
  } else if (OB_FAIL(dep_checker.check(rowkey_and_calc_part_exprs))) {
    LOG_WARN("failed to check rowkey and part expr", K(ret));
  } else {
    LOG_TRACE("succeed to check output exprs", K(deps), K(child_output));
  }
  return ret;
}

int ObLogTableLookup::check_access_dependance(PPDeps &deps)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start to check output and filter exprs", K(deps));
  ObSEArray<ObRawExpr *, 8> dep_exprs;
  ObRawExprCheckDep dep_checker(access_exprs_, deps, true);
  if (OB_FAIL(dep_exprs.assign(output_exprs_))) {
    LOG_WARN("failed to assign output exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(dep_exprs, filter_exprs_))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (use_batch()
             && OB_FAIL(add_var_to_array_no_dup(dep_exprs, group_id_expr_))) {
    LOG_WARN("failed to add group id expr", K(ret));
  } else if (OB_FAIL(append_array_no_dup(dep_exprs, rowkey_exprs_))) {
    LOG_WARN("failed to append rowkey exprs", K(ret));
  } else if (OB_FAIL(dep_checker.check(dep_exprs))) {
    LOG_WARN("failed to check output and filter exprs", K(ret));
  } else {
    LOG_TRACE("succeed to check output and filter exprs", K(deps), K(dep_exprs));
  }
  return ret;
}

int ObLogTableLookup::copy_part_expr_pre(CopyPartExprCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(copy_part_expr(ctx, calc_part_id_expr_))) {
    LOG_WARN("failed to copy part expr", K(ret));
  } else if (OB_FAIL(do_copy_calc_part_id_expr(ctx))) {
    LOG_WARN("failed to assign lookup calc part id exprs", K(ret));
  } else {
    LOG_TRACE("succeed to deep copy calc_part_id_expr_ in lookup",
              K(ret), K(*calc_part_id_expr_));
  }
  return ret;
}

/**
 * @brief 
 *  For tlu, we always copy the calc_part_id_expr and it is
 *  only copied for one time
 *  The calc_part_id_expr in tlu's child op must be shared with tlu's,
 *  so after copied, child's calc_part_id_expr must be set to copied expr
 * @param ctx 
 * @return int 
 */
int ObLogTableLookup::do_copy_calc_part_id_expr(CopyPartExprCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx.cache_lookup_calc_part_id_exprs_.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to push back calc part id expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                                               calc_part_id_expr_,
                                               calc_part_id_expr_,
                                               COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(ctx.new_lookup_calc_part_id_exprs_.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to push back calc part id expr", K(ret));
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */

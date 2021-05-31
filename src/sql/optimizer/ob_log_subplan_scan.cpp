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
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogSubPlanScan::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogSubPlanScan* sub_scan = NULL;
  out = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogSubplanScan", K(ret));
  } else if (OB_ISNULL(sub_scan = static_cast<ObLogSubPlanScan*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperaotr* to ObLogSubplanScan*", K(ret));
  } else {
    sub_scan->set_subquery_id(get_subquery_id());
    sub_scan->get_subquery_name().assign_ptr(get_subquery_name().ptr(), get_subquery_name().length());
    if (OB_FAIL(append(sub_scan->access_exprs_, access_exprs_))) {
      LOG_WARN("failed to copy access exprs", K(ret));
    } else {
      out = sub_scan;
    }
  }
  return ret;
}

int ObLogSubPlanScan::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child pointer is null", K(ret));
  } else {
    card_ = child->get_card();
    op_cost_ = ObOptEstCost::cost_subplan_scan(card_, get_filter_exprs().count());
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogSubPlanScan::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
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
    op_cost_ = ObOptEstCost::cost_subplan_scan(child->get_card(), get_filter_exprs().count());
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogSubPlanScan::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL Pointer Error", K(get_plan()), K(get_plan()->get_stmt()));
  } else {
    // see if we can produce some expr
    // all column related expressions should have been added as column items
    const ObDMLStmt* stmt = get_plan()->get_stmt();
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (NULL != col_item && col_item->table_id_ == get_subquery_id()) {
        ObColumnRefRawExpr* expr = const_cast<ObColumnRefRawExpr*>(col_item->get_expr());
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ColumnItem has a NULL expr", K(*col_item), K(ret));
        } else if (!expr->is_explicited_reference() || expr->is_unpivot_mocked_column()) {
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, static_cast<ObRawExpr*>(expr)))) {
          LOG_PRINT_EXPR(WARN, "failed to add expr to access", expr, K(ret));
        } else {
          LOG_PRINT_EXPR(DEBUG, "succ to add expr to access", expr, "producer", id_, KPC(col_item));
          bool expr_is_required = false;
          if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx, expr_is_required))) {
            LOG_PRINT_EXPR(WARN, "failed to mark expr as produced", expr, "producer", id_);
          } else {
            LOG_PRINT_EXPR(DEBUG, "succ to produce expr", expr, "producer", id_, K(expr));
            if (!is_plan_root()) {
              if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, static_cast<ObRawExpr*>(expr)))) {
                LOG_PRINT_EXPR(WARN, "failed to add expr to output_exprs_", expr, K(ret));
              } else {
                LOG_PRINT_EXPR(DEBUG, "succ to add expr to output_exprs_", expr, K(ret));
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

int ObLogSubPlanScan::transmit_local_ordering()
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

int ObLogSubPlanScan::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (child->get_op_ordering().count() < get_op_ordering().count()) {
    ObSEArray<OrderItem, 8> ordering;
    common::ObIArray<OrderItem>& op_ordering = get_op_ordering();
    for (int64_t i = 0; OB_SUCC(ret) && i < child->get_op_ordering().count(); ++i) {
      if (OB_FAIL(ordering.push_back(op_ordering.at(i)))) {  // here is op_ordering_
        LOG_WARN("failed to push back item", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(set_op_ordering(ordering))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else if (OB_FAIL(transmit_local_ordering())) {
      LOG_WARN("failed to transmit local ordering", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanScan::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  const ObIArray<ObRawExpr*>& access = get_access_exprs();
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXPRS(access, type);
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogSubPlanScan::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> out_part_keys;
  ObSEArray<ObRawExpr*, 4> out_sub_part_keys;
  ObLogicalOperator* child = NULL;
  ObDMLStmt* parent_stmt = NULL;
  ObSelectStmt* child_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> new_sharding_conds;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(child->get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(parent_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_plan()), K(parent_stmt), K(ret));
  } else if (OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(child->get_stmt()->get_stmt_type()), K(ret));
  } else if (FALSE_IT(child_stmt = static_cast<ObSelectStmt*>(child->get_stmt()))) {
    /*do nothing*/
  } else if (OB_FAIL(get_sharding_info().copy_without_part_keys(child->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info from children", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                 child->get_sharding_output_equal_sets(),
                 subquery_id_,
                 *parent_stmt,
                 *child_stmt,
                 false,
                 child->get_sharding_info().get_partition_keys(),
                 sharding_info_.get_partition_keys()))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                 child->get_sharding_output_equal_sets(),
                 subquery_id_,
                 *parent_stmt,
                 *child_stmt,
                 false,
                 child->get_sharding_info().get_sub_partition_keys(),
                 sharding_info_.get_sub_partition_keys()))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                 child->get_sharding_output_equal_sets(),
                 subquery_id_,
                 *parent_stmt,
                 *child_stmt,
                 false,
                 child->get_sharding_info().get_partition_func(),
                 sharding_info_.get_partition_func()))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(get_plan()->get_optimizer_context().get_expr_factory(),
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
  } else if (OB_FAIL(update_weak_part_exprs(ctx))) {
    LOG_WARN("failed to update weak part exprs", K(ret));
  } else {
    LOG_TRACE("subplan scan sharding info", K(sharding_info_));
  }
  return ret;
}

int ObLogSubPlanScan::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (1 != get_num_of_child() || OB_ISNULL(get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child num is not 1 or child is NULL", K(ret), K(get_num_of_child()), KP(get_child(0)));
  } else if (OB_FAIL(link_stmt->force_fill_select_strs())) {
    LOG_WARN("failed to fill link stmt select strs", K(ret), K(output_exprs_));
  } else {
    ObLinkStmt sub_link_stmt(link_stmt->alloc_, get_child(0)->get_output_exprs());
    if (OB_FAIL(sub_link_stmt.init(&link_ctx))) {
      LOG_WARN("failed to init sub link stmt", K(ret), K(dblink_id_));
    } else if (FALSE_IT(link_ctx.link_stmt_ = &sub_link_stmt) ||
               OB_FAIL(get_child(0)->do_plan_tree_traverse(GEN_LINK_STMT, &link_ctx))) {
      LOG_WARN("failed to gen link stmt", K(ret));
    } else if (OB_FAIL(link_stmt->fill_from_strs(sub_link_stmt, subquery_name_))) {
      LOG_WARN("failed to fill link stmt from strs with sub stmt", K(ret));
    }
    link_ctx.link_stmt_ = link_stmt;
  }
  return ret;
}

int ObLogSubPlanScan::calc_cost()
{
  return OB_SUCCESS;
}

int ObLogSubPlanScan::gen_filters()
{
  return OB_SUCCESS;
}

int ObLogSubPlanScan::gen_output_columns()
{
  return OB_SUCCESS;
}

int ObLogSubPlanScan::set_properties()
{
  return OB_SUCCESS;
}

int ObLogSubPlanScan::print_operator_for_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  OutlineType type = plan_text.outline_type_;
  TableItem* table_item = NULL;
  ObString stmt_name;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (USED_HINT == type && OB_FAIL(stmt_->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret));
  } else if (OUTLINE_DATA == type && OB_FAIL(stmt_->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get org stmt_name", K(ret));
  } else if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(subquery_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "subquer_id", subquery_id_);
  } else if (OB_FAIL(BUF_PRINTF("\"%.*s\"@\"%.*s\"",
                 table_item->get_object_name().length(),
                 table_item->get_object_name().ptr(),
                 stmt_name.length(),
                 stmt_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else { /*do nohting*/
  }
  return ret;
}

int ObLogSubPlanScan::is_used_join_type_hint(JoinAlgo join_algo, bool& is_used)
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    int32_t table_id_idx = stmt_->get_table_bit_index(subquery_id_);
    if (OB_FAIL(stmt_hint.is_used_join_type(join_algo, table_id_idx, is_used))) {
      LOG_WARN("fail to judge whether use join type hint", K(ret), K(join_algo), K(table_id_idx));
    }
  }
  return ret;
}

int ObLogSubPlanScan::is_used_in_leading_hint(bool& is_used)
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OB_FAIL(stmt_hint.is_used_in_leading(subquery_id_, is_used))) {
      LOG_WARN("fail to judge whether use leading hint", K(ret), K(subquery_id_), K(is_used));
    }
  }
  return ret;
}

int ObLogSubPlanScan::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(access_exprs_));
  return ret;
}

int ObLogSubPlanScan::update_weak_part_exprs(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObSelectStmt* sub_stmt = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_stmt()) || OB_ISNULL(table = get_stmt()->get_table_item_by_id(subquery_id_)) ||
      OB_ISNULL(sub_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub stmt is null", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> new_weak_part_exprs;
    ObSEArray<ObRawExpr*, 4> input_weak_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->weak_part_exprs_.count(); ++i) {
      ObRawExpr* expr = NULL;
      if (OB_ISNULL(expr = ctx->weak_part_exprs_.at(i)) ||
          OB_UNLIKELY(!ctx->weak_part_exprs_.at(i)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid weak part expr", K(ret), K(expr));
      } else if (NULL != sub_stmt->get_table_item_by_id(static_cast<ObColumnRefRawExpr*>(expr)->get_table_id())) {
        if (OB_FAIL(input_weak_exprs.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(new_weak_part_exprs.push_back(expr))) {
        LOG_WARN("failed to push back weak part expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_select_item_size(); ++i) {
      ObRawExpr* sel_expr = NULL;
      ObRawExpr* col_expr = NULL;
      if (OB_ISNULL(sel_expr = sub_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret), K(sel_expr));
      } else if (!check_weak_part_expr(sel_expr, input_weak_exprs)) {
        // do nothing
      } else if (OB_ISNULL(col_expr = get_stmt()->get_column_expr_by_id(subquery_id_, i + OB_APP_MIN_COLUMN_ID))) {
        // do nothing
      } else if (OB_FAIL(new_weak_part_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back new weak part exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx->weak_part_exprs_.assign(new_weak_part_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

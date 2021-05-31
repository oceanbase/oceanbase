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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_append.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_conflict_row_fetcher.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_insert_all.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObInsertLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null", K(ret));
  } else if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("failed to generate raw plan", K(ret));
  } else if (OB_ISNULL(get_plan_root()) || OB_ISNULL(insert_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  if (OB_SUCC(ret)) {
    // init multi table assign info
    const ObTablesAssignments& table_assign = insert_stmt->get_tables_assignments();
    ObIArray<TableColumns>& table_columns = insert_stmt->get_all_table_columns();
    if (!insert_stmt->is_multi_insert_stmt()) {
      CK(table_columns.count() == 1);
    }
    CK(OB_NOT_NULL(optimizer_context_.get_session_info()));
    if (OB_SUCC(ret) && OB_LIKELY(table_assign.count() == 1)) {
      if (OB_SUCC(ret) && !table_assign.at(0).assignments_.empty()) {
        ObConstRawExpr* lock_row_flag_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_var_int_expr(optimizer_context_.get_expr_factory(), lock_row_flag_expr))) {
          LOG_WARN("fail to create expr", K(ret));
        } else if (OB_FAIL(lock_row_flag_expr->formalize(optimizer_context_.get_session_info()))) {
          LOG_WARN("fail to formalize", K(ret));
        } else {
          insert_op_->set_lock_row_flag_expr(lock_row_flag_expr);
        }
      }
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(0).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
        LOG_DEBUG("table_assign", K(table_assign.at(0).assignments_));
        if (OB_FAIL(index_infos.at(i).init_assignment_info(table_assign.at(0).assignments_))) {
          LOG_WARN("init index assignment info failed", K(ret));
        } else if (optimizer_context_.get_session_info()->use_static_typing_engine()) {
          if (OB_FAIL(index_infos.at(i).add_spk_assignment_info(optimizer_context_.get_expr_factory()))) {
            LOG_WARN("fail to add spk assignment info", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_plan_root()->adjust_parent_child_relationship())) {
    LOG_WARN("failed to adjust parent-child relationship", K(ret));
  } else if (OB_FAIL(plan_traverse_loop(ALLOC_LINK,
                 ALLOC_EXCH,
                 ALLOC_GI,
                 ADJUST_SORT_OPERATOR,
                 PX_PIPE_BLOCKING,
                 PX_RESCAN,
                 RE_CALC_OP_COST,
                 ALLOC_MONITORING_DUMP,
                 OPERATOR_NUMBERING,
                 EXCHANGE_NUMBERING,
                 ALLOC_EXPR,
                 PROJECT_PRUNING,
                 ALLOC_DUMMY_OUTPUT,
                 CG_PREPARE,
                 GEN_SIGNATURE,
                 GEN_LOCATION_CONSTRAINT,
                 PX_ESTIMATE_SIZE,
                 GEN_LINK_STMT))) {
    LOG_WARN("failed to do plan traverse", K(ret));
  } else if (location_type_ != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
    location_type_ = phy_plan_type_;
  }
  if (OB_SUCC(ret) && is_self_part_insert()) {
    if (OB_FAIL(map_value_param_index())) {
      LOG_WARN("map value param index failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && insert_op_->is_multi_part_dml()) {
    if (optimizer_context_.get_session_info()->use_static_typing_engine() && insert_stmt->get_row_count() > 1) {
      ret = STATIC_ENG_NOT_IMPLEMENT;
      LOG_WARN("static engine not support multi part dml with multi values, will retry", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_duplicate_key_checker(*insert_op_))) {
        LOG_WARN("generate duplicae key checker failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_plan_resource())) {
      LOG_WARN("fail calc plan resource", K(ret));
    }
  }
  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250 && insert_stmt->is_returning()) {
    // handle upgrade
    if (get_phy_plan_type() == OB_PHY_PLAN_UNCERTAIN || get_phy_plan_type() == OB_PHY_PLAN_LOCAL) {
      // the plan is executed on 2260 server if it is a multi part dml or a local plan
    } else {
      // distributed exuecution may involve 2250 server
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("returning is not compatible with previous version", K(ret));
    }
  }
  return ret;
}

int ObInsertLogPlan::allocate_insert_op(ObInsertStmt& insert_stmt, ObLogInsert*& insert_op)
{
  int ret = OB_SUCCESS;
  insert_op = static_cast<ObLogInsert*>(get_log_op_factory().allocate(*this, LOG_INSERT));
  if (OB_ISNULL(insert_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate insert operator failed", K(ret));
  } else {
    insert_op->set_all_table_columns(&insert_stmt.get_all_table_columns());
    insert_op->set_value_columns(&insert_stmt.get_values_desc());
    insert_op->set_column_convert_exprs(&insert_stmt.get_column_conv_functions());
    insert_op->set_primary_key_ids(&insert_stmt.get_primary_key_ids());
    insert_op->set_replace(insert_stmt.is_replace());
    insert_op->set_only_one_unique_key(insert_stmt.is_only_one_unique_key());
    insert_op->set_ignore(insert_stmt.is_ignore());
    insert_op->set_check_constraint_exprs(&(insert_stmt.get_check_constraint_exprs()));
    // If assign partitions to scan, set partition hint
    if (insert_stmt.get_stmt_hint().part_hints_.count() > 0) {
      insert_op->set_part_hint(insert_stmt.get_stmt_hint().get_part_hint(insert_stmt.get_insert_table_id()));
    }
    if (insert_stmt.get_insert_up()) {
      if (insert_stmt.get_tables_assignments().count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Assignment num should be one", K(ret));
      } else {
        insert_op->set_insert_up(true);
        insert_op->set_all_table_columns(&insert_stmt.get_all_table_columns());
        insert_op->set_tables_assignments(&insert_stmt.get_tables_assignments());
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::allocate_insert_all_op(ObInsertStmt& insert_stmt, ObLogInsert*& insert_op)
{
  int ret = OB_SUCCESS;
  ObLogInsertAll* insert_all_op = static_cast<ObLogInsertAll*>(get_log_op_factory().allocate(*this, LOG_INSERT_ALL));
  if (OB_ISNULL(insert_all_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate insert operator failed", K(ret));
  } else {
    insert_all_op->set_all_table_columns(&insert_stmt.get_all_table_columns());
    insert_all_op->set_is_multi_table_insert(true);
    insert_all_op->set_is_multi_insert_first(insert_stmt.is_multi_insert_first());
    insert_all_op->set_is_multi_conditions_insert(insert_stmt.is_multi_conditions_insert());
    insert_all_op->set_multi_value_columns(&insert_stmt.get_multi_values_desc());
    insert_all_op->set_multi_column_convert_exprs(&insert_stmt.get_multi_insert_col_conv_funcs());
    insert_all_op->set_multi_insert_table_info(&insert_stmt.get_multi_insert_table_info());
    // If assign partitions to scan, set partition hint
    if (insert_stmt.get_stmt_hint().part_hints_.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt.get_all_table_columns().count(); ++i) {
        const ObPartHint* part_hint = insert_stmt.get_stmt_hint().get_part_hint(insert_stmt.get_insert_table_id(i));
        if (part_hint == NULL) {
          /*do nothing*/
        } else if (OB_FAIL(insert_all_op->add_part_hint(part_hint))) {
          LOG_WARN("failed to add part hint", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      insert_op = insert_all_op;
    }
  }
  return ret;
}

int ObInsertLogPlan::allocate_pdml_insert_as_top(ObLogicalOperator*& top_op)
{
  int ret = OB_SUCCESS;

  ObInsertStmt* stmt = static_cast<ObInsertStmt*>(get_stmt());
  CK(OB_NOT_NULL(stmt));
  if (OB_SUCC(ret)) {
    int64_t dml_op_count = stmt->get_all_table_columns().at(0).index_dml_infos_.count();
    ObLogInsert* insert_log_op = nullptr;
    ObLogicalOperator* values_root = top_op;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < dml_op_count; ++idx) {

      const ObIArray<TableColumns>* table_columns = NULL;

      if (OB_ISNULL(insert_log_op = static_cast<ObLogInsert*>(log_op_factory_.allocate(*this, LOG_INSERT)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory", K(ret));
      } else if (OB_ISNULL(table_columns = stmt->get_slice_from_all_table_columns(allocator_, 0, idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table column slice", K(ret), K(idx));
      } else if (OB_UNLIKELY(table_columns->count() != 1 || table_columns->at(0).index_dml_infos_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table_columns", K(ret), KPC(table_columns));
      }

      // fill insert op params
      if (OB_SUCC(ret)) {
        insert_log_op->set_all_table_columns(table_columns);
        insert_log_op->set_value_columns(&stmt->get_values_desc());
        insert_log_op->set_column_convert_exprs(
            &stmt->get_all_table_columns().at(0).index_dml_infos_.at(idx).column_convert_exprs_);
        insert_log_op->set_primary_key_ids(&stmt->get_primary_key_ids());
        insert_log_op->set_replace(stmt->is_replace());
        insert_log_op->set_only_one_unique_key(stmt->is_only_one_unique_key());
        insert_log_op->set_ignore(stmt->is_ignore());
        insert_log_op->set_check_constraint_exprs(&(stmt->get_check_constraint_exprs()));

        insert_log_op->set_first_dml_op(idx == 0);
        insert_log_op->set_is_pdml(true);
        if (idx > 0) {
          insert_log_op->set_index_maintenance(true);
        }
        if (idx == dml_op_count - 1) {
          insert_log_op->set_is_returning(stmt->is_returning());
          insert_log_op->set_pdml_is_returning(stmt->is_returning());
        } else {
          insert_log_op->set_pdml_is_returning(true);
        }

        // If assign partitions to scan, set partition hint
        if (stmt->get_stmt_hint().part_hints_.count() > 0) {
          insert_log_op->set_part_hint(stmt->get_stmt_hint().get_part_hint(stmt->get_insert_table_id()));
        }
        if (stmt->get_insert_up()) {
          if (stmt->get_tables_assignments().count() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Assignment num should be one", K(ret));
          } else {
            insert_log_op->set_insert_up(true);
            insert_log_op->set_all_table_columns(&stmt->get_all_table_columns());
            insert_log_op->set_tables_assignments(&stmt->get_tables_assignments());
          }
        }

        insert_log_op->set_card(values_root->get_card());
        insert_log_op->set_op_cost(ObOptEstCost::get_cost_params().CPU_TUPLE_COST * values_root->get_card());
        insert_log_op->set_cost(values_root->get_cost() + insert_log_op->get_op_cost());
        insert_log_op->set_is_insert_select(stmt->value_from_select());
      }

      if (OB_SUCC(ret)) {
        insert_log_op->set_child(ObLogicalOperator::first_child, top_op);
        top_op = insert_log_op;
        if (OB_FAIL(insert_log_op->compute_property())) {
          LOG_WARN("fail to compute property", K(ret));
        }
      }

    }  // end for
  }

  return ret;
}

int ObInsertLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
  ObLogicalOperator* top = NULL;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else if (OB_FAIL(set_autoinc_params(insert_stmt->get_autoinc_params()))) {
    // set auto-increment params
    LOG_WARN("failed to set auto-increment params", K(ret));
  } else {
    ObLogicalOperator* values_root = NULL;
    if (!insert_stmt->value_from_select()) {
      // step. allocate inserted value exprs
      if (get_stmt()->has_sequence() && OB_FAIL(allocate_sequence_as_top(values_root))) {
        LOG_WARN("failed to allocate sequence as top", K(ret));
      } else if (OB_FAIL(generate_values_op_as_child(values_root))) {
        LOG_WARN("generate values op as child failed", K(ret));
      } else { /*do nothing*/
      }
    } else if (OB_FAIL(generate_plan_tree())) {
      LOG_WARN("failed to generate plan tree", K(ret));
    } else if (OB_FAIL(get_current_best_plan(values_root))) {
      LOG_WARN("failed to chooose best plan", K(ret));
    }

    // allocate subplan filter for "INSERT .. ON DUPLICATE KEY UPDATE c1 = (select...)"
    if (OB_SUCC(ret) && insert_stmt->get_insert_up()) {
      ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM> assign_exprs;
      ObSEArray<ObRawExpr*, 4> subqueries;
      if (OB_FAIL(insert_stmt->get_tables_assignments_exprs(assign_exprs))) {
        LOG_WARN("failed to get assignment exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs, subqueries))) {
        LOG_WARN("failed to get subqueries", K(ret));
      } else if (subqueries.empty()) {
        // do nothing
      } else if (OB_FAIL(allocate_subplan_filter_as_top(subqueries, false, values_root))) {
        LOG_WARN("failed to allocate subplan", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (optimizer_context_.use_pdml()) {
        top = values_root;
        if (OB_FAIL(allocate_pdml_insert_as_top(top))) {
          LOG_WARN("fail to allocate insert ops", K(ret));
        } else {
          insert_op_ = static_cast<ObLogInsert*>(top);
        }
      } else {
        if (insert_stmt->is_multi_insert_stmt()) {
          if (OB_FAIL(allocate_insert_all_op(*insert_stmt, insert_op_))) {
            LOG_WARN("allocate logical insert operator failed", K(ret));
          } else if (insert_stmt->has_sequence() && OB_FAIL(allocate_sequence_as_top(values_root))) {
            LOG_WARN("failed to allocate sequence as top", K(ret));
          } else { /*do nothing*/
          }
        } else if (OB_FAIL(allocate_insert_op(*insert_stmt, insert_op_))) {
          LOG_WARN("allocate logical insert operator failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          insert_op_->set_child(ObLogicalOperator::first_child, values_root);
          insert_op_->set_card(values_root->get_card());
          insert_op_->set_op_cost(ObOptEstCost::get_cost_params().CPU_TUPLE_COST * values_root->get_card());
          insert_op_->set_cost(values_root->get_cost() + insert_op_->get_op_cost());
          insert_op_->set_is_insert_select(insert_stmt->value_from_select());
          insert_op_->set_is_returning(insert_stmt->is_returning());
          if (OB_FAIL(insert_op_->extract_value_exprs())) {
            LOG_WARN("failed to extract value exprs", K(ret));
          } else if (OB_FAIL(insert_op_->compute_property())) {
            LOG_WARN("failed to compute equal set", K(ret));
          } else {
            top = insert_op_;
          }
        }
      }
    }
    if (OB_SUCC(ret) && insert_stmt->get_returning_aggr_item_size() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert stmt should not have single-set aggregate", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_final_plan_root(top))) {
        LOG_WARN("failed to set plan root", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::generate_duplicate_key_checker(ObLogInsert& insert_op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* dupkey_op = NULL;
  const ObInsertStmt* insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null");
  } else {
    ObRawExpr* calc_part_expr = NULL;
    ObLogDupKeyChecker& dupkey_checker = insert_op.get_dupkey_checker();
    const ObUniqueConstraintCheckStmt& constraint_stmt = insert_stmt->get_constraint_check_stmt();
    dupkey_checker.set_unique_index_cnt(constraint_stmt.constraint_infos_.count());
    dupkey_checker.set_constraint_infos(&constraint_stmt.constraint_infos_);
    if (OB_SUCC(ret)) {
      const ObIArray<ObUniqueConstraintInfo>& constraint_infos = constraint_stmt.constraint_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos.count(); ++i) {
        const ObIArray<ObColumnRefRawExpr*>& constraint_columns = constraint_infos.at(i).constraint_columns_;
        for (int64_t j = 0; OB_SUCC(ret) && j < constraint_columns.count(); ++j) {
          OZ(get_all_exprs().append(static_cast<ObRawExpr*>(constraint_columns.at(j))));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < constraint_stmt.gui_scan_infos_.count(); ++i) {
      const ObDupKeyScanInfo& dupkey_scan_info = constraint_stmt.gui_scan_infos_.at(i);
      OZ(generate_dupkey_scan_info(dupkey_scan_info, dupkey_op, calc_part_expr));
      OZ(dupkey_checker.add_gui_scan_root(dupkey_op));
      OZ(dupkey_checker.add_gui_scan_calc_part_exprs(calc_part_expr));
    }
    if (OB_SUCC(ret) && !constraint_stmt.gui_scan_infos_.empty()) {
      if (OB_FAIL(generate_dupkey_scan_info(constraint_stmt.gui_lookup_info_, dupkey_op, calc_part_expr))) {
        LOG_WARN("generate dupkey scan info failed", K(ret));
      } else {
        dupkey_checker.set_gui_lookup_root(dupkey_op);
        dupkey_checker.set_gui_lookup_calc_part_expr(calc_part_expr);
      }
    }
    if (OB_SUCC(ret)) {
      const ObDupKeyScanInfo& dupkey_scan_info = constraint_stmt.table_scan_info_;
      if (OB_FAIL(generate_dupkey_scan_info(dupkey_scan_info, dupkey_op, calc_part_expr))) {
        LOG_WARN("generate dupkey scan info failed", K(ret));
      } else {
        dupkey_checker.set_table_scan_root(dupkey_op);
        dupkey_checker.set_tsc_calc_part_expr(calc_part_expr);
      }
    }
    LOG_TRACE("generate duplicate key checker", K(ret), K(dupkey_checker));
  }
  return ret;
}

int ObInsertLogPlan::generate_dupkey_scan_info(
    const ObDupKeyScanInfo& dupkey_info, ObLogicalOperator*& dupkey_op, ObRawExpr*& calc_part_expr)
{
  int ret = OB_SUCCESS;
  ObLogConflictRowFetcher* fetcher_op = NULL;
  if (OB_ISNULL(fetcher_op = static_cast<ObLogConflictRowFetcher*>(
                    get_log_op_factory().allocate(*this, log_op_def::LOG_CONFLICT_ROW_FETCHER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate log table scan operator", K(ret));
  } else if (OB_FAIL(append(fetcher_op->get_conflict_exprs(), dupkey_info.conflict_exprs_))) {
    LOG_WARN("append conflict expr expr to log conflict row fetcher failed", K(ret), K(dupkey_info));
  } else if (OB_FAIL(fetcher_op->get_access_exprs().assign(dupkey_info.access_exprs_))) {
    LOG_WARN("construct access expr to log conflict row fetcher failed", K(ret));
  } else if (OB_FAIL(fetcher_op->get_output_exprs().assign(dupkey_info.output_exprs_))) {
    LOG_WARN("construct output expr to log conflict row fetcher failed", K(ret));
  } else if (OB_FAIL(static_cast<ObLogicalOperator*>(insert_op_)
                         ->gen_calc_part_id_expr(dupkey_info.loc_table_id_, dupkey_info.index_tid_, calc_part_expr))) {
    LOG_WARN("fail to gen calc part id expr", K(dupkey_info), K(ret));
  }
  if (OB_SUCC(ret)) {
    fetcher_op->set_table_id(dupkey_info.table_id_);
    fetcher_op->set_index_tid(dupkey_info.index_tid_);
    fetcher_op->set_only_data_table(dupkey_info.only_data_table_);
    dupkey_op = fetcher_op;
  }
  OZ(get_all_exprs().append(dupkey_info.conflict_exprs_));
  OZ(get_all_exprs().append(dupkey_info.access_exprs_));
  OZ(get_all_exprs().append(dupkey_info.output_exprs_));
  OZ(get_all_exprs().append(calc_part_expr));
  return ret;
}

int ObInsertLogPlan::generate_values_op_as_child(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObLogExprValues* values_op = NULL;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
  const ObExecContext* exec_ctx = get_optimizer_context().get_exec_ctx();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_stmt), K(exec_ctx), K(ret));
  } else if (OB_ISNULL(
                 values_op = static_cast<ObLogExprValues*>(get_log_op_factory().allocate(*this, LOG_EXPR_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogInsertValues failed", K(ret));
  } else {
    values_op->set_card(static_cast<double>(insert_stmt->get_row_count()));
    values_op->set_cost(
        ObOptEstCost::get_cost_params().CPU_TUPLE_COST * static_cast<double>(insert_stmt->get_row_count()));
    values_op->set_op_cost(values_op->get_cost());
    values_op->set_need_columnlized(true);
    if (NULL != top) {
      values_op->add_child(top);
    }
    top = values_op;

    if (!insert_stmt->get_subquery_exprs().empty()) {
      if (OB_FAIL(allocate_subplan_filter_as_top(insert_stmt->get_value_vectors(), false, top))) {
        LOG_WARN("failed to allocate subplan filter as top", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(values_op->add_values_expr(insert_stmt->get_value_vectors()))) {
      LOG_WARN("failed to add values expr", K(ret));
    } else if (NULL != exec_ctx && NULL != exec_ctx->get_my_session() &&
               exec_ctx->get_my_session()->use_static_typing_engine() &&
               OB_FAIL(values_op->add_str_values_array(insert_stmt->get_column_conv_functions()))) {
      LOG_WARN("fail to add_str_values_array", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

bool ObInsertLogPlan::is_self_part_insert()
{
  bool is_append = false;
  const ObInsertStmt* insert_stmt = static_cast<const ObInsertStmt*>(stmt_);
  if (insert_stmt != NULL && OB_PHY_PLAN_DISTRIBUTED == phy_plan_type_ && !insert_stmt->value_from_select() &&
      !insert_stmt->has_global_index()) {
    is_append = true;
  }
  return is_append;
}

int ObInsertLogPlan::map_value_param_index()
{
  int ret = OB_SUCCESS;
  ObInsertStmt* insert_stmt = NULL;
  int64_t param_cnt = 0;
  ObArray<ObSEArray<int64_t, 1>> params_row_map;
  if (OB_ISNULL(get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get execute context failed", K(ret));
  } else if (OB_ISNULL(get_optimizer_context().get_exec_ctx()->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(insert_stmt = static_cast<ObInsertStmt*>(get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null");
  } else {
    param_cnt = get_optimizer_context().get_exec_ctx()->get_physical_plan_ctx()->get_param_store().count() +
                insert_stmt->get_exec_param_ref_exprs().count();
    ObSEArray<int64_t, 1> row_indexs;
    if (OB_FAIL(params_row_map.prepare_allocate(param_cnt))) {
      LOG_WARN("reserve row params map failed", K(ret), K(param_cnt));
    } else if (OB_FAIL(row_indexs.push_back(OB_INVALID_INDEX))) {
      LOG_WARN("store row index failed", K(ret));
    }
    // init to OB_INVALID_INDEX
    for (int64_t i = 0; OB_SUCC(ret) && i < params_row_map.count(); ++i) {
      if (OB_FAIL(params_row_map.at(i).assign(row_indexs))) {
        LOG_WARN("init row params map failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObIArray<ObRawExpr*>& insert_values = insert_stmt->get_value_vectors();
    int64_t insert_column_cnt = insert_stmt->get_values_desc().count();
    ObSEArray<int64_t, 1> param_idxs;
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_values.count(); ++i) {
      param_idxs.reset();
      if (OB_FAIL(ObRawExprUtils::extract_param_idxs(insert_values.at(i), param_idxs))) {
        LOG_WARN("extract param idxs failed", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < param_idxs.count(); ++j) {
        if (OB_UNLIKELY(param_idxs.at(j) < 0) || OB_UNLIKELY(param_idxs.at(j) >= param_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param idx is invalid", K(param_idxs.at(j)), K(param_cnt));
        } else if (params_row_map.at(param_idxs.at(j)).count() == 1 &&
                   params_row_map.at(param_idxs.at(j)).at(0) == OB_INVALID_INDEX) {
          params_row_map.at(param_idxs.at(j)).at(0) = i / insert_column_cnt;
        } else if (OB_FAIL(add_var_to_array_no_dup(params_row_map.at(param_idxs.at(j)), i / insert_column_cnt))) {
          LOG_WARN("add index no duplicate failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_params_map_.prepare_allocate(insert_stmt->get_row_count() + 1))) {
      LOG_WARN("prepare allocate row params map failed", K(ret), K(insert_stmt->get_row_count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params_row_map.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < params_row_map.at(i).count(); ++j) {
        if (params_row_map.at(i).at(j) == OB_INVALID_INDEX) {
          if (OB_FAIL(row_params_map_.at(0).push_back(i))) {
            LOG_WARN("add param index to row params map failed", K(ret), K(i), K(j));
          }
        } else {
          if (OB_FAIL(row_params_map_.at(params_row_map.at(i).at(j) + 1).push_back(i))) {
            LOG_WARN("add param index to row params map failed", K(ret), K(i), K(j));
          }
        }
      }
    }
  }
  return ret;
}

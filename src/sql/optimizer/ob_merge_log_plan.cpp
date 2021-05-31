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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_merge_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_append.h"
#include "sql/rewrite/ob_transform_utils.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObMergeLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* best_plan = NULL;
  if (OB_FAIL(generate_plan_tree())) {
    LOG_WARN("fail to generate plan tree", K(ret));
  } else if (OB_FAIL(allocate_merge_subquery())) {
    LOG_WARN("failed to allocate merge subquery", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_FAIL(allocate_merge_operator_as_top(best_plan))) {
    LOG_WARN("fail to allocate merge operator", K(ret));
  } else if (OB_FAIL(set_final_plan_root(best_plan))) {
    LOG_WARN("failed to use final plan", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObMergeLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* best_plan = NULL;
  ObMergeStmt* merge_stmt = NULL;
  if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("failed to generate raw plan", K(ret));
  } else if (OB_ISNULL(best_plan = get_plan_root()) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KP(stmt_), KP(best_plan), K(ret));
  } else if (OB_FAIL(best_plan->adjust_parent_child_relationship())) {
    LOG_WARN("failed to adjust parent-child relationship", K(ret));
  } else {
    ObLogMerge* merge_op = static_cast<ObLogMerge*>(best_plan);
    // init multi table assign info
    merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
    const ObTablesAssignments& table_assign = merge_stmt->get_tables_assignments();
    ObIArray<TableColumns>& table_columns = merge_stmt->get_all_table_columns();
    CK(table_columns.count() == 1);
    CK(OB_NOT_NULL(optimizer_context_.get_session_info()));
    if (OB_SUCC(ret) && OB_LIKELY(table_assign.count() == 1)) {
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(0).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
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
    // do nothing
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
                 GEN_LINK_STMT))) {
    LOG_WARN("fail to travers logical plan", K(ret));
  } else if (location_type_ != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
    location_type_ = phy_plan_type_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_plan_resource())) {
      LOG_WARN("fail calc plan resource", K(ret));
    }
  }
  return ret;
}

int ObMergeLogPlan::allocate_merge_subquery()
{
  int ret = OB_SUCCESS;
  ObRawExpr* null_expr = NULL;
  ObRawExpr* matched_expr = NULL;
  bool update_has_subquery = false;
  bool insert_has_subquery = false;
  ObSEArray<ObRawExpr*, 8> condition_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> target_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> delete_subquery_exprs;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(optimizer_context_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(merge_stmt), K(optimizer_context_.get_session_info()), K(ret));
  } else if (merge_stmt->get_subquery_exprs().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(get_optimizer_context().get_expr_factory(), null_expr))) {
    LOG_WARN("failed to build null expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(
                 get_optimizer_context().get_expr_factory(), merge_stmt->get_match_condition_exprs(), matched_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_ISNULL(null_expr) || OB_ISNULL(matched_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(null_expr), K(matched_expr), K(ret));
  } else if (OB_FAIL(get_update_insert_condition_subquery(
                 matched_expr, null_expr, update_has_subquery, insert_has_subquery, condition_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert condition subquery", K(ret));
  } else if (OB_FAIL(get_update_insert_target_subquery(
                 matched_expr, null_expr, update_has_subquery, insert_has_subquery, target_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert target subquery", K(ret));
  } else if (OB_FAIL(
                 get_delete_condition_subquery(matched_expr, null_expr, update_has_subquery, delete_subquery_exprs))) {
    LOG_WARN("failed to allocate delete condition subquery", K(ret));
  } else if (OB_FAIL(merge_stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt expr reference", K(ret));
  } else if (!condition_subquery_exprs.empty() &&
             OB_FAIL(candi_allocate_subplan_filter(condition_subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(find_and_replace_onetime_expr_with_param(get_onetime_exprs(), target_subquery_exprs))) {
    LOG_WARN("failed to find and replace onetime expr", K(ret));
  } else if (!target_subquery_exprs.empty() && OB_FAIL(candi_allocate_subplan_filter(target_subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(find_and_replace_onetime_expr_with_param(get_onetime_exprs(), delete_subquery_exprs))) {
    LOG_WARN("failed to find and replace onetime expr", K(ret));
  } else if (!delete_subquery_exprs.empty() && OB_FAIL(candi_allocate_subplan_filter(delete_subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObMergeLogPlan::get_update_insert_condition_subquery(ObRawExpr* matched_expr, ObRawExpr* null_expr,
    bool& update_has_subquery, bool& insert_has_subquery, ObIArray<ObRawExpr*>& new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
  update_has_subquery = false;
  insert_has_subquery = false;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(merge_stmt), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(
                 merge_stmt->get_update_condition_exprs(), NULL, update_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(
                 merge_stmt->get_insert_condition_exprs(), NULL, insert_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (update_has_subquery && OB_FAIL(generate_merge_conditions_subquery(
                                        matched_expr, null_expr, true, merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (insert_has_subquery && OB_FAIL(generate_merge_conditions_subquery(
                                        matched_expr, null_expr, false, merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (update_has_subquery && OB_FAIL(append(new_subquery_exprs, merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (insert_has_subquery && OB_FAIL(append(new_subquery_exprs, merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr* raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(optimizer_context_.get_session_info()))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::get_update_insert_target_subquery(ObRawExpr* matched_expr, ObRawExpr* null_expr,
    bool update_has_subquery, bool insert_has_subquery, ObIArray<ObRawExpr*>& new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  ObSEArray<ObRawExpr*, 8> update_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> insert_values_subquery_exprs;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(merge_stmt->get_tables_assignments_exprs(assign_exprs))) {
    LOG_WARN("failed to get table assignment exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs, update_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(
                 ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_value_vectors(), insert_values_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else {
    if (!update_subquery_exprs.empty()) {
      ObRawExpr* update_matched_expr = NULL;
      if (merge_stmt->get_update_condition_exprs().empty()) {
        update_matched_expr = matched_expr;
      } else if (update_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else {
          update_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
        }
      } else if (OB_FAIL(append(temp_exprs, merge_stmt->get_match_condition_exprs()) ||
                         OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs())))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(
                     get_optimizer_context().get_expr_factory(), temp_exprs, update_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(update_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < update_subquery_exprs.count(); i++) {
        ObRawExpr* raw_expr = NULL;
        ObRawExpr* case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = update_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                       update_matched_expr,
                       raw_expr,
                       null_expr,
                       case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back case when expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && !insert_values_subquery_exprs.empty()) {
      ObRawExpr* insert_matched_expr = NULL;
      ObRawExpr* condition_expr = NULL;
      if (merge_stmt->get_insert_condition_exprs().empty()) {
        insert_matched_expr = matched_expr;
      } else if (insert_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_insert_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_insert_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          insert_matched_expr = merge_stmt->get_insert_condition_exprs().at(0);
        }
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(get_optimizer_context().get_expr_factory(),
                     merge_stmt->get_insert_condition_exprs(),
                     condition_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(condition_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                     matched_expr,
                     null_expr,
                     condition_expr,
                     insert_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(insert_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_subquery_exprs.count(); i++) {
        ObRawExpr* raw_expr = NULL;
        ObRawExpr* case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = insert_values_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (merge_stmt->get_insert_condition_exprs().empty() &&
                   OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                       insert_matched_expr,
                       null_expr,
                       raw_expr,
                       case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (!merge_stmt->get_insert_condition_exprs().empty() &&
                   OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                       insert_matched_expr,
                       raw_expr,
                       null_expr,
                       case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back subquery exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr* raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(optimizer_context_.get_session_info()))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !new_subquery_exprs.empty()) {
      ObSEArray<ObRawExpr*, 8> old_subquery_exprs;
      if (OB_FAIL(append(old_subquery_exprs, update_subquery_exprs)) ||
          OB_FAIL(append(old_subquery_exprs, insert_values_subquery_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_UNLIKELY(old_subquery_exprs.count() != new_subquery_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(old_subquery_exprs.count()), K(new_subquery_exprs.count()), K(ret));
      } else if (OB_FAIL(merge_stmt->replace_inner_stmt_expr(old_subquery_exprs, new_subquery_exprs))) {
        LOG_WARN("failed to replace merge stmt", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::get_delete_condition_subquery(
    ObRawExpr* matched_expr, ObRawExpr* null_expr, bool update_has_subquery, ObIArray<ObRawExpr*>& new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  bool delete_has_subquery = false;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(
                 merge_stmt->get_delete_condition_exprs(), NULL, delete_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (!delete_has_subquery) {
    /*do nothing*/
  } else {
    ObSqlBitSet<> check_table_set;
    ObSEArray<ObRawExpr*, 8> temp_exprs;
    ObSEArray<ObDMLStmt*, 2> ignore_stmts;
    ObRawExpr* delete_matched_expr = NULL;
    ObSEArray<ObRawExpr*, 8> delete_column_exprs;
    if (OB_FAIL(check_table_set.add_member(merge_stmt->get_table_bit_index(merge_stmt->get_target_table_id())))) {
      LOG_WARN("failed to add table set", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_column_exprs(merge_stmt->get_delete_condition_exprs(),
                   merge_stmt->get_current_level(),
                   check_table_set,
                   ignore_stmts,
                   delete_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (delete_column_exprs.empty()) {
      /*do nothing*/
    } else {
      ObSEArray<ObRawExpr*, 8> old_exprs;
      ObSEArray<ObRawExpr*, 8> new_exprs;
      ObSEArray<ObQueryRefRawExpr*, 8> temp_subquery_exprs;
      ObTablesAssignments& assignments = merge_stmt->get_table_assignments();
      for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); i++) {
        ObAssignments& table_assigns = assignments.at(i).assignments_;
        for (int64_t j = 0; OB_SUCC(ret) && j < table_assigns.count(); j++) {
          if (OB_ISNULL(table_assigns.at(j).column_expr_) || OB_ISNULL(table_assigns.at(j).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (!ObOptimizerUtil::find_item(delete_column_exprs, table_assigns.at(j).column_expr_)) {
            /*do nothing*/
          } else if (table_assigns.at(j).expr_->has_flag(CNT_SUB_QUERY)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support replace column expr with subquery expr", K(ret));
          } else if (OB_FAIL(old_exprs.push_back(table_assigns.at(j).column_expr_)) ||
                     OB_FAIL(new_exprs.push_back(table_assigns.at(j).expr_))) {
            LOG_WARN("failed to push back exprs", K(ret));
          } else { /*do nothing*/
          }
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(
                     merge_stmt->get_delete_condition_exprs(), temp_subquery_exprs))) {
        LOG_WARN("failed to extract query ref exprs", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < merge_stmt->get_delete_condition_exprs().count(); i++) {
          if (OB_FAIL(ObTransformUtils::replace_expr(
                  old_exprs, new_exprs, merge_stmt->get_delete_condition_exprs().at(i)))) {
            LOG_WARN("failed to replace expr", K(ret));
          } else { /*do nothing*/
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < temp_subquery_exprs.count(); i++) {
          ObSelectStmt* ref_stmt = NULL;
          if (OB_ISNULL(temp_subquery_exprs.at(i)) || OB_ISNULL(ref_stmt = temp_subquery_exprs.at(i)->get_ref_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(temp_subquery_exprs.at(i)), K(ref_stmt), K(ret));
          } else if (OB_FAIL(ref_stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
            LOG_WARN("failed to replace stmt expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (merge_stmt->get_update_condition_exprs().empty()) {
      delete_matched_expr = matched_expr;
    } else if (update_has_subquery) {
      if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
          OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else {
        delete_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
      }
    } else if (OB_FAIL(append(temp_exprs, merge_stmt->get_match_condition_exprs()) ||
                       OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs())))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(
                   get_optimizer_context().get_expr_factory(), temp_exprs, delete_matched_expr))) {
      LOG_WARN("failed to build matched expr", K(ret));
    } else if (OB_ISNULL(delete_matched_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing*/
    }

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(generate_merge_conditions_subquery(
                   delete_matched_expr, null_expr, true, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to generate merge conditions", K(ret));
    } else if (OB_FAIL(append(new_subquery_exprs, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to append new exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
        ObRawExpr* raw_expr = NULL;
        if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(raw_expr->formalize(optimizer_context_.get_session_info()))) {
          LOG_WARN("failed to formalize case expr", K(ret));
        } else if (OB_FAIL(raw_expr->pull_relation_id_and_levels(merge_stmt->get_current_level()))) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::allocate_merge_operator_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObLogMerge* merge_op = NULL;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(const_cast<ObDMLStmt*>(stmt_));
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(merge_stmt), K(top), K(ret));
  } else if (OB_ISNULL(merge_op = static_cast<ObLogMerge*>(get_log_op_factory().allocate(*this, LOG_MERGE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate MERGE operator", K(ret));
  } else if (OB_FAIL(set_autoinc_params(merge_stmt->get_autoinc_params()))) {
    LOG_WARN("failed to set auto-increment params", K(ret));
  } else if (merge_stmt->has_sequence() && OB_FAIL(allocate_sequence_as_top(top))) {
    LOG_WARN("failed to allocate sequence as top", K(ret));
  } else if (OB_FAIL(append(merge_op->get_output_exprs(), merge_stmt->get_column_conv_functions()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    merge_op->set_column_convert_exprs(&merge_stmt->get_column_conv_functions());
    merge_op->set_all_table_columns(&merge_stmt->get_all_table_columns());
    merge_op->set_match_condition(&merge_stmt->get_match_condition_exprs());
    merge_op->set_update_condition(&merge_stmt->get_update_condition_exprs());
    merge_op->set_delete_condition(&merge_stmt->get_delete_condition_exprs());
    merge_op->set_insert_condition(&merge_stmt->get_insert_condition_exprs());
    merge_op->set_rowkey_exprs(&merge_stmt->get_rowkey_exprs());
    merge_op->set_value_vector(&merge_stmt->get_value_vectors());
    merge_op->set_primary_key_ids(&merge_stmt->get_primary_key_ids());
    merge_op->set_tables_assignments(&merge_stmt->get_tables_assignments());
    merge_op->set_all_table_columns(&(merge_stmt->get_all_table_columns()));
    merge_op->set_check_constraint_exprs(&(merge_stmt->get_check_constraint_exprs()));
    merge_op->set_child(ObLogicalOperator::first_child, top);
    merge_op->set_cost(top->get_card() * UPDATE_ONE_ROW_COST);
    merge_op->set_card(top->get_card());
    top = merge_op;
  }
  return ret;
}

int ObMergeLogPlan::generate_merge_conditions_subquery(
    ObRawExpr* matched_expr, ObRawExpr* null_expr, bool is_matched, ObIArray<ObRawExpr*>& condition_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* case_when_expr = NULL;
  ObRawExpr* case_param_expr = NULL;
  ObSEArray<ObRawExpr*, 8> subquery_exprs;
  ObSEArray<ObRawExpr*, 8> non_subquery_exprs;
  if (OB_ISNULL(matched_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(condition_exprs, subquery_exprs, non_subquery_exprs))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(append(non_subquery_exprs, subquery_exprs))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(
                 get_optimizer_context().get_expr_factory(), non_subquery_exprs, case_param_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_ISNULL(case_param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_matched && OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                               matched_expr,
                               case_param_expr,
                               null_expr,
                               case_when_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else if (!is_matched && OB_FAIL(ObRawExprUtils::build_case_when_expr(get_optimizer_context().get_expr_factory(),
                                matched_expr,
                                null_expr,
                                case_param_expr,
                                case_when_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else if (OB_ISNULL(case_when_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    condition_exprs.reuse();
    if (OB_FAIL(condition_exprs.push_back(case_when_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

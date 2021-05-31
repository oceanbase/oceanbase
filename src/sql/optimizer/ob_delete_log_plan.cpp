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
#include "sql/optimizer/ob_delete_log_plan.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "ob_log_operator_factory.h"
#include "ob_log_table_scan.h"
#include "ob_log_sort.h"
#include "ob_log_limit.h"
#include "ob_log_table_scan.h"
#include "ob_join_order.h"
#include "ob_opt_est_cost.h"
/**
 * DELETE syntax from MySQL 5.7
 *
 * Single-Table Syntax:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
 *   [PARTITION (partition_name,...)]
 *   [WHERE where_condition]
 *   [ORDER BY ...]
 *   [LIMIT row_count]
 *
 * Multiple-Table Syntax
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   tbl_name[.*] [, tbl_name[.*]] ...
 *   FROM table_references
 *   [WHERE where_condition]
 *  Or:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   FROM tbl_name[.*] [, tbl_name[.*]] ...
 *   USING table_references
 *   [WHERE where_condition]
 */
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

int ObDeleteLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* top = NULL;
  ObDeleteStmt* delete_stmt = static_cast<ObDeleteStmt*>(get_stmt());

  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(generate_plan_tree())) {
    LOG_WARN("failed to generate plan tree for plain select", K(ret));
  } else {
    LOG_TRACE("succ to generate plan tree");
  }

  if (OB_SUCC(ret)) {
    // allocate subplan filter if needed, mainly for the subquery in where statement
    if (get_subquery_filters().count() > 0) {
      LOG_TRACE("start to allocate subplan filter for where statement", K(ret));
      if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
        LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for where statement", K(ret));
      }
    }
  }

  // 1. allocate 'count' for rownum if needed
  if (OB_SUCC(ret)) {
    bool has_rownum = false;
    if (OB_FAIL(get_stmt()->has_rownum(has_rownum))) {
      LOG_WARN("failed to get rownum info", "sql", get_stmt()->get_sql_stmt(), K(ret));
    } else if (has_rownum) {
      LOG_TRACE("SQL has rownum expr", "sql", get_stmt()->get_sql_stmt(), K(ret));
      if (OB_FAIL(candi_allocate_count())) {
        LOG_WARN("failed to allocate rownum(count)", "sql", get_stmt()->get_sql_stmt(), K(ret));
      } else {
        LOG_TRACE("'COUNT' operator is allocated", "sql", get_stmt()->get_sql_stmt(), K(ret));
      }
    }
  }

  // 2 allocate 'order-by' if needed
  ObSEArray<OrderItem, 4> order_items;
  if (OB_SUCC(ret) && get_stmt()->has_order_by()) {
    if (OB_FAIL(candi_allocate_order_by(order_items))) {
      LOG_WARN("failed to allocate order by operator", "sql", get_stmt()->get_sql_stmt(), K(ret));
    } else {
      LOG_TRACE("succeed to allocate order by operator", "sql", get_stmt()->get_sql_stmt(), K(ret));
    }
  }

  // 3. allocate 'limit' if needed
  if (OB_SUCC(ret)) {
    if (get_stmt()->has_limit()) {
      LOG_TRACE("SQL has limit clause", "sql", get_stmt()->get_sql_stmt(), K(ret));
      if (OB_FAIL(candi_allocate_limit(order_items))) {
        LOG_WARN("failed to allocate 'LIMIT' operator", "sql", get_stmt()->get_sql_stmt(), K(ret));
      }
    } else {
      LOG_TRACE("SQL has no limit clause", "sql", get_stmt()->get_sql_stmt(), K(ret));
    }
  }

  // 2.2 get cheapest path
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_current_best_plan(top))) {
      LOG_WARN("failed to get best plan", K(ret));
    } else if (OB_ISNULL(top)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(top), K(ret));
    } else { /*do nothing*/
    }
  }

  // 4. allocate delete operator
  // there are two type log plan for delete: pdml delete plan and normal delete plan.
  // there is a table p1, which have a global index i. the diff of two delete plan:
  // (1) pdml delete plan:
  //      ....
  //          DELETE (P1.i)
  //                EX IN
  //                  EX OUT
  //                     DELETE (p1)
  //                        .....
  // (2) norale delete plan:
  //      ....
  //         MULTI PART DELETE
  //                 EX IN
  //                  EX OUT
  //                    TSC
  if (OB_SUCC(ret)) {
    if (optimizer_context_.use_pdml()) {
      if (OB_FAIL(allocate_pdml_delete_as_top(top))) {
        LOG_WARN("failed to allocate pdml delete op", K(ret));
      }
    } else if (OB_FAIL(allocate_delete_as_top(top))) {
      LOG_WARN("failed to allocate delete op", K(ret));
    }
  }

  // 5. allocate scalar operator, just for agg func returning
  if (OB_SUCC(ret) && delete_stmt->get_returning_aggr_item_size() > 0) {
    ObArray<ObRawExpr*> dummy_exprs;
    ObArray<OrderItem> dummy_ordering;
    if (OB_FAIL(allocate_group_by_as_top(top,
            SCALAR_AGGREGATE,
            dummy_exprs /*group by*/,
            dummy_exprs /*rollup*/,
            delete_stmt->get_returning_aggr_items(),
            dummy_exprs /*having*/,
            dummy_ordering))) {
      LOG_WARN("failed to allocate group by as top", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    set_plan_root(top);
    top->mark_is_plan_root();
    if (share::is_mysql_mode() && !get_stmt()->has_limit()) {
      if (OB_FAIL(check_fullfill_safe_update_mode(top))) {
        LOG_WARN("failed to check fullfill safe update mode", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(get_plan_root())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the root plan is null", K(ret));
    } else {
      if (OB_FAIL(delete_op_->compute_property())) {
        LOG_WARN("failed to compute equal set", K(ret));
      } else if (OB_ISNULL(delete_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the delete_op_ is null", K(ret));
      } else if (delete_op_->check_rowkey_distinct()) {
        LOG_WARN("check rowkey distinct with delete failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDeleteLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  ObDeleteStmt* del_stmt = NULL;
  if (OB_ISNULL(del_stmt = static_cast<ObDeleteStmt*>(get_stmt())) || OB_UNLIKELY(!get_stmt()->is_delete_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete stmt is null", K(ret));
  } else if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("faield to generate raw plan", K(ret));
  } else if (OB_ISNULL(get_plan_root()) || OB_ISNULL(delete_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan_root()), K(delete_op_));
  } else if (OB_FAIL(get_plan_root()->adjust_parent_child_relationship())) {
    LOG_WARN("failed to adjust parent-child relationship", K(ret));
  } else if (OB_FAIL(plan_traverse_loop(ALLOC_LINK,
                 ALLOC_EXCH,
                 ALLOC_GI,
                 ADJUST_SORT_OPERATOR,
                 PX_PIPE_BLOCKING,
                 PX_RESCAN,
                 RE_CALC_OP_COST,
                 OPERATOR_NUMBERING,
                 EXCHANGE_NUMBERING,
                 ALLOC_EXPR,
                 PROJECT_PRUNING,
                 ALLOC_MONITORING_DUMP,
                 ALLOC_DUMMY_OUTPUT,
                 CG_PREPARE,
                 GEN_SIGNATURE,
                 GEN_LOCATION_CONSTRAINT,
                 REORDER_PROJECT_COLUMNS,
                 PX_ESTIMATE_SIZE,
                 GEN_LINK_STMT))) {
    SQL_OPT_LOG(WARN, "failed to do plan traverse", K(ret));
  } else {
    SQL_OPT_LOG(DEBUG, "succ to do all plan traversals");
    if (location_type_ != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
      location_type_ = phy_plan_type_;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_plan_resource())) {
      LOG_WARN("fail calc plan resource", K(ret));
    }
  }
  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250 && del_stmt->is_returning()) {
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

int ObDeleteLogPlan::allocate_delete_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObDeleteStmt* delete_stmt = static_cast<ObDeleteStmt*>(get_stmt());
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_delete_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt or operator", K(ret), K(top), K(get_stmt()));
  } else if (OB_ISNULL(delete_op_ = static_cast<ObLogDelete*>(get_log_op_factory().allocate(*this, LOG_DELETE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_OPT_LOG(ERROR, "failed to allocate DELETE operator");
  } else {
    delete_op_->set_all_table_columns(&(delete_stmt->get_all_table_columns()));
    delete_op_->set_child(ObLogicalOperator::first_child, top);
    delete_op_->set_is_returning(delete_stmt->is_returning());
    top = delete_op_;
  }
  return ret;
}

int ObDeleteLogPlan::allocate_pdml_delete_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt* delete_stmt = static_cast<const ObDeleteStmt*>(get_stmt());
  int64_t global_index_cnt = delete_stmt->get_all_table_columns().at(0).index_dml_infos_.count();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < global_index_cnt; idx++) {
    const common::ObIArray<TableColumns>* table_column =
        delete_stmt->get_slice_from_all_table_columns(allocator_, 0, idx);
    ObLogDelete* temp_delete_op = NULL;
    temp_delete_op = static_cast<ObLogDelete*>(log_op_factory_.allocate(*this, LOG_DELETE));
    if (OB_ISNULL(temp_delete_op)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate delete logical operator", K(ret));
    } else if (OB_ISNULL(table_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table columns is null", K(ret));
    } else if (1 != table_column->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of table colums is error", K(ret), K(table_column->count()));
    } else if (1 != table_column->at(0).index_dml_infos_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of index dml info is error", K(ret), K(table_column->at(0).index_dml_infos_.count()));
    } else {
      temp_delete_op->set_is_pdml(true);
      temp_delete_op->set_first_dml_op(idx == 0);
      temp_delete_op->set_pdml_is_returning(true);
      if (idx > 0) {
        temp_delete_op->set_index_maintenance(true);
      }
      temp_delete_op->set_all_table_columns(table_column);
      if (OB_NOT_NULL(top)) {
        temp_delete_op->set_child(ObLogicalOperator::first_child, top);
        double op_cost = top->get_card() * static_cast<double>(DELETE_ONE_ROW_COST);
        temp_delete_op->set_op_cost(op_cost);
        temp_delete_op->set_cost(top->get_cost() + op_cost);
        temp_delete_op->set_card(top->get_card());
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the top operator should not be null", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (idx == global_index_cnt - 1) {
          temp_delete_op->set_pdml_is_returning(delete_stmt->is_returning());
          temp_delete_op->set_is_returning(delete_stmt->is_returning());
        }
      }

      if (OB_SUCC(ret)) {
        top = temp_delete_op;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(top)) {
      delete_op_ = static_cast<ObLogDelete*>(top);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the top operator should not be null", K(ret));
    }
  }
  return ret;
}

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

#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_update_log_plan.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_append.h"
#include "sql/optimizer/ob_log_subplan_filter.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

/*
create table t1 (i int, j int);
create table t2 (i int, j int);
create table t3 (i int, j int);
Mysql:
explain update t2 set j = (select i from t1 limit 1) where i in (select j from t3) order by j limit 3\G
===============================================
|ID|OPERATOR           |NAME |EST. ROWS|COST  |
-----------------------------------------------
|0 |UPDATE             |     |3        |339326|
|1 | SUBPLAN FILTER    |     |3        |339323|
|2 |  LIMIT            |     |3        |339287|
|3 |   TOP-N SORT      |     |3        |339286|
|4 |    HASH JOIN      |     |98011    |258722|
|5 |     SUBPLAN SCAN  |VIEW1|101      |99182 |
|6 |      HASH DISTINCT|     |101      |99169 |
|7 |       TABLE SCAN  |t3   |100000   |66272 |
|8 |     TABLE SCAN    |t2   |100000   |68478 |
|9 |  TABLE SCAN       |t1   |1        |36    |
===============================================
Oracle:
explain update t2 set j = (select i from t1 where rownum < 2) where i in (select j from t3) and rownum < j\G
==============================================
|ID|OPERATOR          |NAME |EST. ROWS|COST  |
----------------------------------------------
|0 |UPDATE            |     |98011    |394701|
|1 | SUBPLAN FILTER   |     |98011    |296691|
|2 |  COUNT           |     |98011    |283128|
|3 |   HASH JOIN      |     |98011    |269600|
|4 |    SUBPLAN SCAN  |VIEW1|101      |99182 |
|5 |     HASH DISTINCT|     |101      |99169 |
|6 |      TABLE SCAN  |T3   |100000   |66272 |
|7 |    TABLE SCAN    |T2   |100000   |68478 |
|8 |  TABLE SCAN      |T1   |1        |36    |
==============================================
*/

int ObUpdateLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  /**
   *  Currently we only support update statement with just one table. Having more
   *  than one table would require rewriting the logic in this function.
   */
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObLogicalOperator* top = NULL;
    ObUpdateStmt* update_stmt = static_cast<ObUpdateStmt*>(get_stmt());

    // step. generate access paths
    if (OB_FAIL(generate_plan_tree())) {
      LOG_WARN("failed to generate plan tree for plain select", K(ret));
    } else {
      LOG_TRACE("succ to generate plan tree");
    }
    // allocate subplan filter if needed, mainly for the subquery in where statement
    if (OB_SUCC(ret)) {
      if (get_subquery_filters().count() > 0) {
        LOG_TRACE("start to allocate subplan filter for where statement", K(ret));
        if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
          LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
        } else {
          LOG_TRACE("succeed to allocate subplan filter for where statement", K(ret));
        }
      }
    }
    // step. allocate 'count' for rownum if needed, Oracle mode only
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

    // step. allocate 'order-by' if needed, MySQL mode only
    ObSEArray<OrderItem, 4> order_items;
    if (OB_SUCC(ret)) {
      if (get_stmt()->has_order_by()) {
        if (OB_FAIL(candi_allocate_order_by(order_items))) {
          LOG_WARN("failed to allocate order by operator", "sql", get_stmt()->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("succeed to allocate order by operator is", "sql", get_stmt()->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no order by clause", "sql", get_stmt()->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate 'limit' if needed
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

    // step. allocate 'sequence' if needed
    if (OB_SUCC(ret)) {
      if (get_stmt()->has_sequence()) {
        LOG_TRACE("SQL has sequence clause", "sql", get_stmt()->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_sequence())) {
          LOG_WARN("failed to allocate 'SEQUENCE' operator", "sql", get_stmt()->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no sequence clause", "sql", get_stmt()->get_sql_stmt(), K(ret));
      }
    }

    // step. get cheapest path
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_current_best_plan(top))) {
        LOG_WARN("failed to get best plan", K(ret));
      } else if (OB_ISNULL(top)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(top), K(ret));
      } else { /*do nothing*/
      }
    }

    // step. allocate subplan filter for "update .. set c1 = (select)"
    if (OB_SUCC(ret) && !update_stmt->is_update_set()) {
      ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM> assign_exprs;
      ObSEArray<ObRawExpr*, 4> subqueries;
      if (OB_FAIL(update_stmt->get_tables_assignments_exprs(assign_exprs))) {
        LOG_WARN("failed to get assignment exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs, subqueries))) {
        LOG_WARN("failed to get subqueries", K(ret));
      } else if (subqueries.empty()) {
        // do nothing
      } else if (OB_FAIL(allocate_subplan_filter_as_top(subqueries, false, top))) {
        LOG_WARN("failed to allocate subplan", K(ret));
      }
    }

    // step. allocate subplanfilter for "update .. set (a,b)=(select..), (c,d)=(select..)"
    if (OB_SUCC(ret) && update_stmt->is_update_set()) {
      ObSEArray<ObQueryRefRawExpr*, 4> subqueries;
      ObSEArray<ObRawExpr*, 4> subquery_exprs;
      if (update_stmt->get_tables_assignments().count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udpate stmt table assignments count wrong", K(ret), K(update_stmt->get_tables_assignments().count()));
      } else if (OB_FAIL(adjust_assignment_exprs(update_stmt->get_tables_assignments(), subqueries))) {
        LOG_WARN("failed to adjust assignment exprs", K(ret));
      } else if (OB_FAIL(append(subquery_exprs, subqueries))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(allocate_subplan_filter_as_top(subquery_exprs, false, top))) {
        LOG_WARN("failed to allocate subplan filter", K(ret));
      } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_SUBPLAN_FILTER != top->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the operator is expected to be subplan filter", K(ret));
      } else {
        static_cast<ObLogSubPlanFilter*>(top)->set_update_set(true);
      }
    }

    if (OB_SUCC(ret)) {
      const ObTablesAssignments& table_assign = update_stmt->get_tables_assignments();
      ObIArray<TableColumns>& all_table_columns = update_stmt->get_all_table_columns();
      CK(table_assign.count() == all_table_columns.count());
      CK(OB_NOT_NULL(optimizer_context_.get_session_info()));
      for (int64_t i = 0; OB_SUCC(ret) && i < table_assign.count(); ++i) {
        const ObAssignments& assignments = table_assign.at(i).assignments_;
        ObIArray<IndexDMLInfo>& index_infos = all_table_columns.at(i).index_dml_infos_;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_infos.count(); ++j) {
          if (optimizer_context_.get_session_info()->use_static_typing_engine()) {
            if (OB_FAIL(index_infos.at(j).add_spk_assignment_info(optimizer_context_.get_expr_factory()))) {
              LOG_WARN("fail to add spk assignment info", K(ret));
            }
          }
        }
      }
    }

    // step. allocate update operator
    if (OB_SUCC(ret)) {
      if (optimizer_context_.use_pdml()) {
        if (OB_FAIL(allocate_pdml_update_as_top(top))) {
          LOG_WARN("failed to allocate pdml update op", K(ret));
        }
      } else if (OB_FAIL(allocate_update_as_top(top))) {
        LOG_WARN("failed to allocate update op", K(ret));
      }
    }

    // step. allocate scalar operator
    if (OB_SUCC(ret) && update_stmt->get_returning_aggr_item_size() > 0) {
      // returning
      //
      // DECLARE
      //   l_max_id NUMBER;
      // BEGIN
      //   UPDATE t1
      //   SET    description = description
      //   RETURNING MAX(id) INTO l_max_id;
      //
      //   DBMS_OUTPUT.put_line('l_max_id=' || l_max_id);
      //
      //   COMMIT;
      // END;
      //
      ObArray<ObRawExpr*> dummy_exprs;
      ObArray<OrderItem> dummy_ordering;
      if (OB_FAIL(allocate_group_by_as_top(top,
              SCALAR_AGGREGATE,
              dummy_exprs /*group by*/,
              dummy_exprs /*rollup*/,
              update_stmt->get_returning_aggr_items(),
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
  }
  return ret;
}

int ObUpdateLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = static_cast<ObUpdateStmt*>(get_stmt());
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update stmt is null", K(ret));
  } else if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("failed to generate raw plan", K(ret));
  } else if (OB_ISNULL(get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_plan_root()->adjust_parent_child_relationship())) {
    LOG_WARN("failed to adjust preant-child relationship", K(ret));
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
                 GEN_LOCATION_CONSTRAINT,
                 REORDER_PROJECT_COLUMNS,
                 PX_ESTIMATE_SIZE,
                 GEN_LINK_STMT))) {
    SQL_OPT_LOG(WARN, "failed to do plan traverse", K(ret));
  } else if (OB_FAIL(plan_traverse_loop(GEN_SIGNATURE))) {
    SQL_OPT_LOG(WARN, "failed to get plan signature", K(ret));
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
  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250 && update_stmt->is_returning()) {
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

int ObUpdateLogPlan::allocate_update_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObUpdateStmt* update_stmt = static_cast<ObUpdateStmt*>(get_stmt());
  ObLogUpdate* update_op = nullptr;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_update_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt or operator", K(ret), K(top), K(get_stmt()));
  } else if (OB_ISNULL(update_op = static_cast<ObLogUpdate*>(get_log_op_factory().allocate(*this, LOG_UPDATE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_OPT_LOG(ERROR, "failed to allocate UPDATE operator");
  } else {
    update_op->set_all_table_columns(&(update_stmt->get_all_table_columns()));
    update_op->set_tables_assignments(&(update_stmt->get_tables_assignments()));
    update_op->set_child(ObLogicalOperator::first_child, top);
    update_op->set_ignore(update_stmt->is_ignore());
    update_op->set_check_constraint_exprs(&(update_stmt->get_check_constraint_exprs()));
    update_op->set_update_set(update_stmt->is_update_set());
    update_op->set_is_returning(update_stmt->is_returning());
    if (OB_FAIL(update_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(update_op->check_rowkey_distinct())) {
      LOG_WARN("failed to check rowkey distinct", K(ret));
    } else {
      top = update_op;
      ObConstRawExpr* lock_row_flag_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_var_int_expr(optimizer_context_.get_expr_factory(), lock_row_flag_expr))) {
        LOG_WARN("fail to create expr", K(ret));
      } else if (OB_FAIL(lock_row_flag_expr->formalize(optimizer_context_.get_session_info()))) {
        LOG_WARN("fail to formalize", K(ret));
      } else {
        update_op->set_lock_row_flag_expr(lock_row_flag_expr);
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::adjust_assignment_exprs(ObTablesAssignments& assigments, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < assigments.count(); ++i) {
    ObTableAssignment& table_assign = assigments.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_assign.assignments_.count(); ++j) {
      ObAssignment& assign = table_assign.assignments_.at(j);
      ObRawExpr** value = NULL;
      if (OB_ISNULL(assign.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign value expr is null", K(ret));
      } else if (assign.expr_->get_expr_type() != T_FUN_COLUMN_CONV) {
        value = &assign.expr_;
      } else if (OB_ISNULL(assign.expr_->get_param_expr(4))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value expr is null", K(ret));
      } else {
        value = &assign.expr_->get_param_expr(4);
      }
      if (NULL != value && OB_FAIL(replace_alias_ref_expr(*value, subqueries))) {
        LOG_WARN("failed to replace alias ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::allocate_pdml_update_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt* update_stmt = static_cast<const ObUpdateStmt*>(get_stmt());
  int64_t global_index_cnt = update_stmt->get_all_table_columns().at(0).index_dml_infos_.count();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < global_index_cnt; idx++) {
    const common::ObIArray<TableColumns>* table_column =
        update_stmt->get_slice_from_all_table_columns(allocator_, 0, idx);
    const ObTablesAssignments* one_table_assignment =
        update_stmt->get_slice_from_all_table_assignments(allocator_, 0, idx);

    if (OB_ISNULL(one_table_assignment) || (one_table_assignment->count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ObTableAssignment& assign = one_table_assignment->at(0);
      const IndexDMLInfo& dml_info = update_stmt->get_all_table_columns().at(0).index_dml_infos_.at(idx);
      if (assign.is_update_part_key_) {

        LOG_TRACE("partition key updated, gen delete-insert as raw plan", K(assign));
        int64_t binlog_row_image = share::ObBinlogRowImage::FULL;
        if (OB_FAIL(optimizer_context_.get_session_info()->get_binlog_row_image(binlog_row_image))) {
          LOG_WARN("fail to get binlog row image", K(ret));
        } else if (share::ObBinlogRowImage::FULL != binlog_row_image) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "when binlog_row_image is not set to FULL,"
              "parallel update with row movement");
        } else if (OB_FAIL(allocate_pdml_delete_op(top, idx > 0, table_column, one_table_assignment))) {
          LOG_WARN("fail alloc delete op for row-movement update", K(idx), K(global_index_cnt), K(ret));
        } else if (OB_FAIL(allocate_pdml_insert_op(
                       top, idx > 0, idx == global_index_cnt - 1, table_column, one_table_assignment))) {
          LOG_WARN("fail alloc insert op for row-movement update", K(idx), K(global_index_cnt), K(ret));
        }
      } else {
        if (OB_FAIL(allocate_pdml_update_op(
                top, idx > 0, idx == global_index_cnt - 1, table_column, one_table_assignment))) {
          LOG_WARN("fail alloc update op for non-row-movement update", K(idx), K(global_index_cnt), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::allocate_pdml_update_op(ObLogicalOperator*& top, bool is_index_maintenace, bool is_last_dml_op,
    const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment)
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt* update_stmt = static_cast<const ObUpdateStmt*>(get_stmt());
  if (OB_ISNULL(update_stmt) || OB_ISNULL(table_column) || OB_ISNULL(one_table_assignment)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret));
  } else {
    ObLogUpdate* temp_update_op = NULL;
    temp_update_op = static_cast<ObLogUpdate*>(log_op_factory_.allocate(*this, LOG_UPDATE));
    if (OB_ISNULL(temp_update_op)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate update logical operator", K(ret));
    } else if (OB_ISNULL(table_column) || OB_ISNULL(one_table_assignment)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table columns is null", KP(table_column), KP(one_table_assignment), K(ret));
    } else if (1 != table_column->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of table colums is error", K(ret), K(table_column->count()));
    } else if (1 != table_column->at(0).index_dml_infos_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of index dml info is error", K(ret), K(table_column->at(0).index_dml_infos_.count()));
    } else {
      LOG_TRACE("generate pdml update op",
          "table_name",
          table_column->at(0).table_name_,
          "index_name",
          table_column->at(0).index_dml_infos_.at(0).index_name_);
      temp_update_op->set_is_pdml(true);
      temp_update_op->set_first_dml_op(!is_index_maintenace);
      temp_update_op->set_index_maintenance(is_index_maintenace);
      temp_update_op->set_all_table_columns(table_column);
      temp_update_op->set_tables_assignments(one_table_assignment);
      temp_update_op->set_ignore(update_stmt->is_ignore());
      temp_update_op->set_update_set(update_stmt->is_update_set());
      if (!is_index_maintenace) {
        temp_update_op->set_check_constraint_exprs(&(update_stmt->get_check_constraint_exprs()));
      }

      if (OB_NOT_NULL(top)) {
        temp_update_op->set_child(ObLogicalOperator::first_child, top);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the top operator should not be null", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (is_last_dml_op) {
          temp_update_op->set_is_returning(update_stmt->is_returning());
          temp_update_op->set_pdml_is_returning(update_stmt->is_returning());
        } else {
          temp_update_op->set_pdml_is_returning(true);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(temp_update_op->compute_property())) {
          LOG_WARN("failed to compute property", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        top = temp_update_op;
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::allocate_pdml_delete_op(ObLogicalOperator*& top, bool is_index_maintenace,
    const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment)
{
  UNUSED(one_table_assignment);
  int ret = OB_SUCCESS;
  ObLogDelete* temp_delete_op = NULL;
  temp_delete_op = static_cast<ObLogDelete*>(log_op_factory_.allocate(*this, LOG_DELETE));
  if (OB_ISNULL(temp_delete_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate delete logical operator", K(ret));
  } else if (OB_ISNULL(table_column) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table columns or top is null", K(ret), KP(top));
  } else if (1 != table_column->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of table colums is error", K(ret), K(table_column->count()));
  } else if (1 != table_column->at(0).index_dml_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of index dml info is error", K(ret), K(table_column->at(0).index_dml_infos_.count()));
  } else {
    temp_delete_op->set_first_dml_op(!is_index_maintenace);
    temp_delete_op->set_is_pdml(true);
    temp_delete_op->set_need_barrier(true);
    temp_delete_op->set_pdml_is_returning(true);
    temp_delete_op->set_index_maintenance(is_index_maintenace);
    temp_delete_op->set_all_table_columns(table_column);
    temp_delete_op->set_child(ObLogicalOperator::first_child, top);
    top = temp_delete_op;
  }
  return ret;
}

int ObUpdateLogPlan::allocate_pdml_insert_op(ObLogicalOperator*& top, bool is_index_maintenace, bool is_last_dml_op,
    const common::ObIArray<TableColumns>* table_column, const ObTablesAssignments* one_table_assignment)
{
  UNUSED(one_table_assignment);
  int ret = OB_SUCCESS;
  ObLogInsert* insert_op = NULL;
  insert_op = static_cast<ObLogInsert*>(log_op_factory_.allocate(*this, LOG_INSERT));
  if (OB_ISNULL(insert_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(table_column) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table columns or top is null", K(ret), KP(top));
  } else if (1 != table_column->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of table colums is error", K(ret), K(table_column->count()));
  } else if (1 != table_column->at(0).index_dml_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of index dml info is error", K(ret), K(table_column->at(0).index_dml_infos_.count()));
  } else {
    IndexDMLInfo& dml_info = const_cast<IndexDMLInfo&>(table_column->at(0).index_dml_infos_.at(0));
    if (OB_SUCC(ret)) {
      insert_op->set_all_table_columns(table_column);
      insert_op->set_is_pdml(true);
      insert_op->set_need_barrier(false);
      insert_op->set_table_location_uncertain(true);
      insert_op->set_index_maintenance(is_index_maintenace);
      insert_op->set_pdml_is_returning(!is_last_dml_op);
      insert_op->set_value_columns(&dml_info.column_exprs_);
      insert_op->set_primary_key_ids(&dml_info.primary_key_ids_);
      insert_op->set_column_convert_exprs(&dml_info.column_convert_exprs_);
      if (!is_index_maintenace) {
        const ObUpdateStmt* update_stmt = static_cast<const ObUpdateStmt*>(get_stmt());
        insert_op->set_check_constraint_exprs(&(update_stmt->get_check_constraint_exprs()));
      }
    }

    if (OB_SUCC(ret)) {
      insert_op->set_child(ObLogicalOperator::first_child, top);
    }

    top = insert_op;
  }
  return ret;
}

int ObUpdateLogPlan::replace_alias_ref_expr(ObRawExpr*& expr, ObIArray<ObQueryRefRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (expr->is_alias_ref_expr()) {
    ObAliasRefRawExpr* alias = static_cast<ObAliasRefRawExpr*>(expr);
    if (OB_UNLIKELY(!alias->is_ref_query_output())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alias expr", K(ret), K(*alias));
    } else if (OB_FAIL(
                   add_var_to_array_no_dup(subqueries, static_cast<ObQueryRefRawExpr*>(alias->get_param_expr(0))))) {
      LOG_WARN("failed to add var to array with out duplicate", K(ret));
    } else {
      expr = alias->get_ref_expr();
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(replace_alias_ref_expr(expr->get_param_expr(i), subqueries))) {
        LOG_WARN("failed to replace alias ref expr", K(ret));
      }
    }
  }
  return ret;
}

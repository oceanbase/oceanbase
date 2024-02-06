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
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_insert_all_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "common/ob_smart_call.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

int ObInsertAllLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
      OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(session_info), K(get_stmt()), K(ret));
  } else {
    const ObInsertAllStmt *insert_all_stmt = get_stmt();
    LOG_TRACE("start to allocate operators for ", "sql", get_optimizer_context().get_query_ctx()->get_sql_stmt());
    if (!insert_all_stmt->value_from_select()) {
      // insert into values xxxx
      ObLogicalOperator *top = NULL;
      if (insert_all_stmt->has_sequence() && OB_FAIL(allocate_sequence_as_top(top))) {
        LOG_WARN("failed to allocate sequence as top", K(ret));
      } else if (OB_FAIL(allocate_insert_values_as_top(top))) {
        LOG_WARN("failed to allocate expr values as top", K(ret));
      } else if (OB_FAIL(make_candidate_plans(top))) {
        LOG_WARN("failed to make candidate plans", K(ret));
      } else { /*do nothing*/ }
    } else {
      // insert into select xxx
      if (OB_FAIL(generate_plan_tree())) {
        LOG_WARN("failed to generate plan tree", K(ret));
      } else if (insert_all_stmt->has_sequence() &&
                 OB_FAIL(candi_allocate_sequence())) {
        LOG_WARN("failed to allocate sequence", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate subplan filter for "INSERT ALL when subquery then INTO xx VALUES((subquery1), ....)"
    if (OB_SUCC(ret) && !insert_all_stmt->get_subquery_exprs().empty()) {
      ObSEArray<ObRawExpr*, 4> all_values_vector;
      ObSEArray<ObRawExpr*, 4> all_when_cond_exprs;
      ObSEArray<ObRawExpr*, 4> all_relation_subquery_exprs;
      if (OB_FAIL(insert_all_stmt->get_all_values_vector(all_values_vector))) {
        LOG_WARN("failed to get all values vector", K(ret));
      } else if (OB_FAIL(insert_all_stmt->get_all_when_cond_exprs(all_when_cond_exprs))) {
        LOG_WARN("failed to get all when cond exprs", K(ret));
      } else if (OB_FAIL(append(all_relation_subquery_exprs, all_values_vector)) ||
                 OB_FAIL(append(all_relation_subquery_exprs, all_when_cond_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(candi_allocate_subplan_filter(all_relation_subquery_exprs))) {
        LOG_WARN("failed to allocate subplan", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_dml_infos())) {
        LOG_WARN("failed to prepare dml infos", K(ret));
      } else if (OB_FAIL(candi_allocate_insert_all())) {
        LOG_WARN("failed to allocate insert all operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate insert all operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    //allocate temp-table transformation if needed.
    if (OB_SUCC(ret) && !get_optimizer_context().get_temp_table_infos().empty() && is_final_root_plan()) {
      if (OB_FAIL(candi_allocate_temp_table_transformation())) {
        LOG_WARN("failed to allocate transformation operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate temp-table transformation",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // allocate root exchange
    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_root_exchange())) {
        LOG_WARN("failed to allocate root exchange", K(ret));
      } else {
        LOG_TRACE("succeed to allocate root exchange",
            K(candidates_.candidate_plans_.count()));
      }
    }
  }
  return ret;
}

int ObInsertAllLogPlan::allocate_insert_values_as_top(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogExprValues *values_op = NULL;
  const ObInsertAllStmt *insert_all_stmt = get_stmt();
  ObSQLSessionInfo *session_info = get_optimizer_context().get_session_info();
  if (OB_ISNULL(insert_all_stmt) || OB_ISNULL(session_info)) {
    LOG_WARN("get unexpected null", K(insert_all_stmt), K(session_info), K(ret));
  } else if (OB_ISNULL(values_op = static_cast<ObLogExprValues*>(get_log_op_factory().
                                   allocate(*this, LOG_EXPR_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate values op", K(ret));
  } else if (insert_all_stmt->is_error_logging() && OB_FAIL(values_op->extract_err_log_info())) {
    LOG_WARN("failed to extract error log exprs", K(ret));
  } else if (OB_FAIL(values_op->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    if (NULL != top) {
      values_op->add_child(top);
    }
    top = values_op;
    // if (!insert_stmt->get_subquery_exprs().empty() &&
    //     OB_FAIL(allocate_subplan_filter_as_top(top, insert_all_stmt->get_values_vector()))) {
    //   LOG_WARN("failed to allocate subplan filter as top", K(ret));
    // } else if (OB_FAIL(values_op->add_values_expr(insert_stmt->get_values_vector()))) {
    //   LOG_WARN("failed to add values expr", K(ret));
    // } else { /*do nothing*/ }
  }
  return ret;
}

int ObInsertAllLogPlan::candi_allocate_insert_all()
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> best_plans;
  if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                          best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      if (OB_FAIL(create_insert_all_plan(best_plans.at(i).plan_tree_))) {
        LOG_WARN("failed to create delete plan", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObInsertAllLogPlan::create_insert_all_plan(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (top->is_sharding() &&
             OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_insert_all_as_top(top))) {
    LOG_WARN("failed to allocate insert all as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObInsertAllLogPlan::allocate_insert_all_as_top(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogInsertAll *insert_all_op = NULL;
  const ObInsertAllStmt *insert_all_stmt = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(insert_all_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(insert_all_op = static_cast<ObLogInsertAll*>(
                       get_log_op_factory().allocate(*this, LOG_INSERT_ALL)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate insert all operator failed", K(ret));
  } else if (OB_FAIL(insert_all_op->assign_dml_infos(index_dml_infos_))) {
    LOG_WARN("failed to assign index dml infos", K(ret));
  } else {
    insert_all_op->set_child(ObLogicalOperator::first_child, top);
    insert_all_op->set_is_multi_table_insert(true);
    insert_all_op->set_is_multi_insert_first(insert_all_stmt->get_is_multi_insert_first());
    insert_all_op->set_is_multi_conditions_insert(insert_all_stmt->get_is_multi_condition_insert());
    insert_all_op->set_insert_all_table_info(&insert_all_stmt->get_insert_all_table_info());
    insert_all_op->set_is_returning(false);
    insert_all_op->set_is_multi_part_dml(true);
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(insert_all_op->compute_property())) {
      LOG_WARN("failed to compute equal set", K(ret));
    } else {
      top = insert_all_op;
    }
  }
  return ret;
}

int ObInsertAllLogPlan::prepare_dml_infos()
{
  int ret = OB_SUCCESS;
  const ObInsertAllStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else {
    const ObIArray<ObInsertAllTableInfo*>& table_infos = stmt->get_insert_all_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObInsertAllTableInfo* table_info = table_infos.at(i);
      IndexDMLInfo* table_dml_info = nullptr;
      ObSEArray<IndexDMLInfo*, 8> index_dml_infos;
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (OB_FAIL(prepare_table_dml_info_basic(*table_info, table_dml_info,
                                                      index_dml_infos, false))) {
        LOG_WARN("failed to prepare table dml info basic", K(ret));
      } else if (OB_FAIL(prepare_table_dml_info_special(*table_info, table_dml_info,
                                                        index_dml_infos, index_dml_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertAllLogPlan::prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                                       IndexDMLInfo* table_dml_info,
                                                       ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                       ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  const ObInsertAllTableInfo& insert_all_info = static_cast<const ObInsertAllTableInfo&>(table_info);
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(schema_guard), K(session_info));
  } else if (OB_FAIL(table_dml_info->column_convert_exprs_.assign(insert_all_info.column_conv_exprs_))) {
    LOG_WARN("failed to assign column convert exprs", K(ret));
  } else {
    ObRawExprCopier copier(optimizer_context_.get_expr_factory());
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_all_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(copier.add_replaced_expr(insert_all_info.column_exprs_.at(i),
                                           insert_all_info.column_conv_exprs_.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_info->ck_cst_exprs_.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(table_dml_info->ck_cst_exprs_.at(i),
                                         table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      IndexDMLInfo* index_dml_info = index_dml_infos.at(i);
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                        index_dml_info->ref_table_id_,
                                                        index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
      } else if (OB_FAIL(generate_index_column_exprs(insert_all_info.table_id_,
                                                    *index_schema,
                                                    index_dml_info->column_exprs_))) {
        LOG_WARN("resolve index related column exprs failed", K(ret));
      } else if (OB_FAIL(fill_index_column_convert_exprs(copier,
                                                        index_dml_info->column_exprs_,
                                                        index_dml_info->column_convert_exprs_))) {
        LOG_WARN("failed to fill index column convert exprs", K(ret));
      }
    }
  }

  if (FAILEDx(ObDelUpdLogPlan::prepare_table_dml_info_special(table_info,
                                                              table_dml_info,
                                                              index_dml_infos,
                                                              all_index_dml_infos))) {
    LOG_WARN("failed to prepare table dml info special", K(ret));
  }
  return ret;
}

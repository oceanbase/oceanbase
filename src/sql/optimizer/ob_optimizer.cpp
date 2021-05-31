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
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObOptimizer::optimize(ObDMLStmt& stmt, ObLogPlan*& logical_plan)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  const ObQueryCtx* query_ctx = stmt.get_query_ctx();
  const ObSQLSessionInfo* session = ctx_.get_session_info();
  int64_t last_mem_usage = ctx_.get_allocator().total();
  int64_t optimizer_mem_usage = 0;
  if (OB_ISNULL(query_ctx) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(query_ctx), K(session));
  } else if (OB_FAIL(init_env_info(stmt))) {
    LOG_WARN("failed to init px info", K(ret));
  } else if (OB_FAIL(generate_plan_for_temp_table(stmt))) {
    LOG_WARN("failed to generate plan for temp table", K(ret));
  } else if (OB_ISNULL(plan = ctx_.get_log_plan_factory().create(ctx_, stmt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create plan", "stmt", stmt.get_sql_stmt(), K(ret));
  } else if (OB_FAIL(plan->init_plan_info())) {
    LOG_WARN("failed to init equal sets", K(ret));
  } else if (OB_FAIL(plan->generate_plan())) {
    LOG_WARN("failed to perform optimization", K(ret));
  } else if (OB_FAIL(plan->set_affected_last_insert_id(stmt.get_affected_last_insert_id()))) {
    LOG_WARN("fail to set affected last insert id", K(ret));
  } else if (query_ctx->is_cross_tenant_query(session->get_effective_tenant_id())) {
    // In order to avoid such a query causing GTS disordered
    // some cross-tenant queries will not be allowed:
    bool is_forbidden = false;
    if (plan->get_location_type() == OB_PHY_PLAN_UNCERTAIN) {
      // uncertain reading and writing
      is_forbidden = true;
    }
    if (ObStmt::is_dml_write_stmt(stmt.get_stmt_type())) {
      // all cross-tenant writing
      is_forbidden = true;
    }
    if (is_forbidden) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Access tables from multiple tenants");
    }
  }
  if (OB_SUCC(ret)) {
    logical_plan = plan;
    LOG_TRACE("succ to optimize statement", "stmt", stmt.get_sql_stmt(), K(logical_plan->get_optimization_cost()));
  }
  optimizer_mem_usage = ctx_.get_allocator().total() - last_mem_usage;
  LOG_TRACE("[SQL MEM USAGE]", K(optimizer_mem_usage), K(last_mem_usage));
  return ret;
}

int ObOptimizer::get_optimization_cost(ObDMLStmt& stmt, double& cost)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  cost = 0.0;
  ctx_.set_cost_evaluation();
  if (OB_ISNULL(plan = ctx_.get_log_plan_factory().create(ctx_, stmt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create plan", "stmt", stmt.get_sql_stmt(), K(ret));
  } else if (OB_FAIL(init_env_info(stmt))) {
  } else if (OB_FAIL(plan->init_plan_info())) {
    LOG_WARN("failed to init equal sets", K(ret));
  } else if (OB_FAIL(plan->generate_raw_plan())) {
    if (OB_SQL_OPT_GEN_PLAN_FALIED == ret) {
      ret = OB_SUCCESS;
      cost = DBL_MAX;
    } else {
      LOG_WARN("failed to perform optimization", K(ret));
    }
  } else {
    cost = plan->get_optimization_cost();
  }
  return ret;
}

int ObOptimizer::generate_plan_for_temp_table(ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(query_ctx), K(ret));
  } else {
    ObIArray<ObSqlTempTableInfo*>& temp_table_infos = query_ctx->temp_table_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i++) {
      ObSelectLogPlan* temp_plan = NULL;
      ObSelectStmt* temp_query = NULL;
      if (OB_ISNULL(temp_table_infos.at(i)) || OB_ISNULL(temp_query = temp_table_infos.at(i)->table_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_ISNULL(temp_plan =
                               static_cast<ObSelectLogPlan*>(ctx_.get_log_plan_factory().create(ctx_, *temp_query)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create logical plan", K(temp_plan), K(ret));
      } else if (OB_FAIL(temp_plan->init_plan_info())) {
        LOG_WARN("failed to init equal sets", K(ret));
      } else if (OB_FAIL(temp_plan->generate_raw_plan())) {
        LOG_WARN("Failed to generate temp_plan for sub_stmt", K(ret));
      } else if (OB_ISNULL(temp_plan->get_plan_root())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        temp_table_infos.at(i)->table_plan_ = temp_plan->get_plan_root();
      }
    }
  }
  return ret;
}

int ObOptimizer::get_stmt_max_table_dop(ObDMLStmt& stmt, int64_t& max_table_dop)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  ObQueryCtx* query_ctx = NULL;
  share::schema::ObSchemaGetterGuard* schema_guard = ctx_.get_schema_guard();
  if (OB_ISNULL(schema_guard) || OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(schema_guard), K(query_ctx), K(ret));
  } else if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_items().count(); i++) {
      TableItem* table_item = NULL;
      const share::schema::ObTableSchema* table_schema = NULL;
      if (OB_ISNULL(table_item = stmt.get_table_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (table_item->is_function_table() || table_item->is_link_table() || table_item->is_fake_cte_table() ||
                 table_item->is_joined_table()) {
        /*do nothing*/
      } else if (table_item->is_temp_table() || table_item->is_generated_table()) {
        if (OB_ISNULL(table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(SMART_CALL(get_stmt_max_table_dop(*table_item->ref_query_, max_table_dop)))) {
          LOG_WARN("failed to get stmt max table dop", K(ret));
        } else { /*do nothing*/
        }
      } else {
        if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          max_table_dop = std::max(max_table_dop, table_schema->get_dop());
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_stmt_max_table_dop(*child_stmts.at(i), max_table_dop)))) {
        LOG_WARN("failed to get stmt max table dop", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizer::get_session_parallel_info(
    ObDMLStmt& stmt, bool use_pdml, bool& session_enable_parallel, uint64_t& session_force_parallel_dop)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = NULL;
  session_enable_parallel = false;
  session_force_parallel_dop = 1;
  if (OB_ISNULL(session_info = ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (!session_info->is_inner()) {
    if (!stmt.is_px_dml_supported_stmt()) {
      if (OB_FAIL(session_info->get_enable_parallel_query(session_enable_parallel))) {
        LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
      } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
        LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
      }
    } else {
      if (use_pdml) {
        if (OB_FAIL(session_info->get_enable_parallel_dml(session_enable_parallel))) {
          LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
        } else if (OB_FAIL(session_info->get_force_parallel_dml_dop(session_force_parallel_dop))) {
          LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
        } else if (session_force_parallel_dop == 1) {
          if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
            LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
          } else { /*do nothing*/
          }
        }
      } else {
        if (OB_FAIL(session_info->get_enable_parallel_query(session_enable_parallel))) {
          LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
        } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
          LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::check_pdml_enabled(const ObDMLStmt& stmt, const ObSQLSessionInfo& session, bool& is_use_pdml)
{
  // 1. pdml: force parallel dml & no DISABLE_PARALLEL_DML hint
  // 2. enable parallel query: parallel hint | sess enable_parallel_query
  //    pdml: enable parallel dml + enable parallel query
  int ret = OB_SUCCESS;
  uint64_t force_pdml_dop = 1;
  is_use_pdml = false;
  const ObStmtHint& hint = stmt.get_stmt_hint();
  // case 1
  if (!session.use_static_typing_engine() || !stmt.is_pdml_supported_stmt()) {
    is_use_pdml = false;
  } else if (stmt::T_INSERT == stmt.get_stmt_type() && !static_cast<const ObInsertStmt&>(stmt).value_from_select()) {
    is_use_pdml = false;
  } else if (OB_FAIL(session.get_force_parallel_dml_dop(force_pdml_dop))) {
    LOG_WARN("fail get force parallel dml session val", K(ret));
  } else if (force_pdml_dop > 1) {
    ObPDMLOption hint_opt = hint.get_pdml_option();
    if (hint_opt == ObPDMLOption::DISABLE) {
      is_use_pdml = false;
    } else if (hint.get_parallel_hint() == ObStmtHint::UNSET_PARALLEL) {
      is_use_pdml = true;
    } else {
      is_use_pdml = (hint.get_parallel_hint() > ObStmtHint::DEFAULT_PARALLEL);
    }
  } else {
    // case 2
    uint64_t query_dop = 1;
    if (OB_FAIL(session.get_force_parallel_query_dop(query_dop))) {
      LOG_WARN("fail get query dop", K(ret));
    }

    if (OB_SUCC(ret) && (hint.parallel_ > ObStmtHint::DEFAULT_PARALLEL || query_dop > ObStmtHint::DEFAULT_PARALLEL)) {
      ObPDMLOption hint_opt = hint.get_pdml_option();
      if (hint_opt != ObPDMLOption::NOT_SPECIFIED) {
        is_use_pdml = (hint_opt == ObPDMLOption::ENABLE);
      } else {
        OZ(session.get_enable_parallel_dml(is_use_pdml));
      }
    }
  }
  // check pdml enable sql case
  if (OB_SUCC(ret) && is_use_pdml) {
    OZ(check_pdml_supported_feature(stmt, session, is_use_pdml));
  }
  LOG_DEBUG("check pdml enable", K(ret), K(is_use_pdml));
  return ret;
}

int ObOptimizer::check_pdml_supported_feature(const ObDMLStmt& stmt, const ObSQLSessionInfo& session, bool& is_use_pdml)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard* schema_guard = ctx_.get_schema_guard();
  const share::schema::ObTableSchema* table_schema = NULL;
  const ObDelUpdStmt& pdml_stmt = static_cast<const ObDelUpdStmt&>(stmt);
  bool enable_all_pdml_feature = false;
  ret = E(EventTable::EN_ENABLE_PDML_ALL_FEATURE) OB_SUCCESS;
  LOG_TRACE("event: check pdml all feature", K(ret));
  if (OB_FAIL(ret)) {
    enable_all_pdml_feature = true;
    ret = OB_SUCCESS;
  }
  LOG_TRACE("event: check pdml all feature result", K(ret), K(enable_all_pdml_feature));
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the schema guard is null", K(ret));
  } else if (enable_all_pdml_feature) {
    is_use_pdml = true;
  } else if (pdml_stmt.get_all_table_columns().count() != 1) {
    is_use_pdml = false;
  } else if (stmt::T_INSERT == stmt.get_stmt_type() && static_cast<const ObInsertStmt&>(stmt).get_insert_up()) {
    is_use_pdml = false;
  } else if (OB_FAIL(schema_guard->get_table_schema(
                 pdml_stmt.get_all_table_columns().at(0).index_dml_infos_.at(0).index_tid_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (table_schema->get_foreign_key_infos().count() > 0) {
    LOG_TRACE("dml has foreign key, disable pdml", K(ret));
    is_use_pdml = false;
  } else if (!session.use_static_typing_engine() && stmt.get_check_constraint_exprs_size() > 0) {
    LOG_TRACE("dml has constraint, old engine, disable pdml", K(ret));
    is_use_pdml = false;
  } else {
    // check global unique index, update(row movement)
    int global_index_cnt = pdml_stmt.get_all_table_columns().at(0).index_dml_infos_.count();
    for (int idx = 0; idx < global_index_cnt && OB_SUCC(ret) && is_use_pdml; idx++) {
      const ObIArray<ObColumnRefRawExpr*>& column_exprs =
          pdml_stmt.get_all_table_columns().at(0).index_dml_infos_.at(idx).column_exprs_;
      bool has_unique_index = false;
      LOG_TRACE("check pdml unique index", K(column_exprs));
      if (OB_FAIL(check_unique_index(column_exprs, has_unique_index))) {
        LOG_WARN("failed to check has unique index", K(ret));
      } else if (has_unique_index) {
        LOG_TRACE("dml has unique index, disable pdml", K(ret));
        is_use_pdml = false;
        break;
      }
    }
  }
  LOG_TRACE("check use all pdml feature", K(ret), K(is_use_pdml));
  return ret;
}

int ObOptimizer::init_env_info(ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;
  int64_t parallel = 1;
  bool use_pdml = false;
  bool session_enable_parallel = false;
  uint64_t session_force_parallel_dop = 1;
  ObDMLStmt* target_stmt = &stmt;
  if (stmt.is_explain_stmt()) {
    target_stmt = static_cast<ObExplainStmt*>(&stmt)->get_explain_query_stmt();
  }
  if (OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_pdml_enabled(*target_stmt, *(ctx_.get_session_info()), use_pdml))) {
    LOG_WARN("fail to check enable pdml", K(ret));
  } else if (OB_FAIL(get_session_parallel_info(
                 *target_stmt, use_pdml, session_enable_parallel, session_force_parallel_dop))) {
    LOG_WARN("failed to get session parallel info", K(ret));
  } else {
    parallel = target_stmt->get_stmt_hint().parallel_;
    if (parallel <= 0) {
      parallel = ObStmtHint::DEFAULT_PARALLEL;
    }
    ctx_.set_parallel(parallel);
    ctx_.set_use_pdml(use_pdml);
    int64_t max_table_dop = 1;
    if (OB_FAIL(get_stmt_max_table_dop(*target_stmt, max_table_dop))) {
      LOG_WARN("failed to get stmt max table dop", K(ret));
    } else if (OB_UNLIKELY(max_table_dop < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table dop", K(max_table_dop), K(ret));
    } else if (target_stmt->get_stmt_hint().parallel_ != ObStmtHint::UNSET_PARALLEL) {
      ctx_.set_parallel_rule(PXParallelRule::MANUAL_HINT);
      ctx_.set_parallel(parallel);
    } else if (session_force_parallel_dop > 1) {
      ctx_.set_parallel_rule(PXParallelRule::SESSION_FORCE_PARALLEL);
      ctx_.set_parallel(session_force_parallel_dop);
    } else if (max_table_dop > 1 && session_enable_parallel) {
      ctx_.set_parallel_rule(PXParallelRule::MANUAL_TABLE_DOP);
      ctx_.set_parallel(max_table_dop);
    } else {
      ctx_.set_parallel_rule(PXParallelRule::USE_PX_DEFAULT);
      ctx_.set_parallel(ObStmtHint::DEFAULT_PARALLEL);
    }
  }
  return ret;
}

int ObOptimizer::check_unique_index(const ObIArray<ObColumnRefRawExpr*>& column_exprs, bool& has_unique_index) const
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
    if (NULL == column_exprs.at(j)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObRawExpr* target_expr = column_exprs.at(j);
      if (target_expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* column_ref_expr = (ObColumnRefRawExpr*)(target_expr);
        if (column_ref_expr->is_virtual_generated_column() && !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
            column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          has_unique_index = true;
          break;
        }
      }
    }
  }
  return ret;
}

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
#include "ob_optimizer.h"
#include "sql/optimizer/ob_explain_note.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_opt_cost_model_parameter.h"
#include "src/share/stat/ob_opt_stat_manager.h"
#include "src/sql/engine/px/ob_dfo_scheduler.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObOptimizer::optimize(ObDMLStmt &stmt, ObLogPlan *&logical_plan)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_sql_optimize);
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  const ObQueryCtx *query_ctx = ctx_.get_query_ctx();
  const ObSQLSessionInfo *session = ctx_.get_session_info();
  int64_t last_mem_usage = ctx_.get_allocator().total();
  int64_t optimizer_mem_usage = 0;
  ObDMLStmt *target_stmt = &stmt;
  ObTaskExecutorCtx *task_exec_ctx = ctx_.get_task_exec_ctx();
  if (stmt.is_explain_stmt()) {
    target_stmt = static_cast<ObExplainStmt*>(&stmt)->get_explain_query_stmt();
  }
  if (OB_ISNULL(query_ctx) || OB_ISNULL(session) ||
      OB_ISNULL(target_stmt)|| OB_ISNULL(task_exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(query_ctx), K(session), K(target_stmt), K(task_exec_ctx));
  } else if (OB_FAIL(init_env_info(*target_stmt))) {
    LOG_WARN("failed to init px info", K(ret));
  } else if (!target_stmt->is_reverse_link() &&
             OB_FAIL(generate_plan_for_temp_table(*target_stmt))) {
    LOG_WARN("failed to generate plan for temp table", K(ret));
  } else if (OB_ISNULL(plan = ctx_.get_log_plan_factory().create(ctx_, stmt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create plan", K(ret));
  } else if (OB_FAIL(plan->generate_plan())) {
    LOG_WARN("failed to perform optimization", K(ret));
  } else if (OB_FAIL(plan->add_extra_dependency_table())) {
    LOG_WARN("failed to add extra dependency tables", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (ctx_.get_exec_ctx()->get_sql_ctx()->is_remote_sql_ &&
        ctx_.get_phy_plan_type() != OB_PHY_PLAN_LOCAL) {
      // set table location to refresh location cache
      ObSEArray<ObTablePartitionInfo*, 8> table_partitions;
      if (OB_FAIL(plan->get_global_table_partition_info(table_partitions))) {
        LOG_WARN("failed to get global table partition info", K(ret));
      } else if (OB_FAIL(task_exec_ctx->set_table_locations(table_partitions))) {
        LOG_WARN("failed to set table locations", K(ret));
      }

      if (OB_SUCC(ret)) {
        ret = OB_LOCATION_NOT_EXIST;
        LOG_WARN("best plan for remote sql is not local", K(ret), K(ctx_.get_phy_plan_type()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    logical_plan = plan;
    LOG_TRACE("succ to optimize statement", "stmt", query_ctx->get_sql_stmt(),
                K(logical_plan->get_optimization_cost()));
  }
  optimizer_mem_usage = ctx_.get_allocator().total() - last_mem_usage;
  LOG_TRACE("[SQL MEM USAGE]", K(optimizer_mem_usage), K(last_mem_usage));
  return ret;
}

int ObOptimizer::get_optimization_cost(ObDMLStmt &stmt,
                                       ObLogPlan *&plan,
                                       double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  ctx_.set_cost_evaluation();
  if (OB_ISNULL(ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is nul", K(ret));
  } else if (OB_ISNULL(plan = ctx_.get_log_plan_factory().create(ctx_, stmt)) ||
      OB_ISNULL(ctx_.get_session_info())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create plan", "stmt", ctx_.get_query_ctx()->get_sql_stmt(), K(ret));
  } else if (OB_FAIL(init_env_info(stmt))) {
    LOG_WARN("failed to init env info", K(ret));
  } else if (OB_FAIL(generate_plan_for_temp_table(stmt))) {
    LOG_WARN("failed to generate plan for temp table", K(ret));
  } else if (OB_FAIL(plan->generate_raw_plan())) {
    LOG_WARN("failed to perform optimization", K(ret));
  } else {
    cost = plan->get_optimization_cost();
    OPT_TRACE(plan);
  }
  return ret;
}

int ObOptimizer::get_cte_optimization_cost(ObDMLStmt &root_stmt,
                                           ObSelectStmt *cte_query,
                                           ObIArray<ObSelectStmt *> &stmts,
                                           double &cte_cost,
                                           ObIArray<double> &costs)
{
  int ret = OB_SUCCESS;
  cte_cost = 0.0;
  ctx_.set_cost_evaluation();
  costs.reuse();
  if (OB_ISNULL(ctx_.get_query_ctx()) ||
      OB_ISNULL(ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx or session info is null", K(ret));
  } else if (OB_FAIL(init_env_info(root_stmt))) {
    LOG_WARN("failed to init env info", K(ret));
  } else if (OB_FAIL(generate_plan_for_temp_table(root_stmt))) {
    LOG_WARN("failed to generate plan for temp table", K(ret));
  }
  if (OB_SUCC(ret) && NULL != cte_query) {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < ctx_.get_temp_table_infos().count(); i ++) {
      ObSqlTempTableInfo *temp_table_info = ctx_.get_temp_table_infos().at(i);
      ObLogPlan *plan = NULL;
      if (OB_ISNULL(temp_table_info) || OB_ISNULL(temp_table_info->table_plan_)
          || OB_ISNULL(plan = temp_table_info->table_plan_->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (cte_query == temp_table_info->table_query_) {
        find = true;
        if (OB_FAIL(plan->allocate_temp_table_insert_as_top(temp_table_info->table_plan_,
                                                            temp_table_info))) {
          LOG_WARN("failed to allocate temp table insert", K(ret));
        } else {
          cte_cost = temp_table_info->table_plan_->get_cost();
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); i ++) {
    ObLogPlan *plan = NULL;
    ObSelectStmt *stmt = stmts.at(i);
    ObLogicalOperator *best_plan = NULL;
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(plan = ctx_.get_log_plan_factory().create(ctx_, *stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create plan", "stmt", ctx_.get_query_ctx()->get_sql_stmt(), K(ret));
    } else if (OB_FAIL(plan->generate_raw_plan())) {
      LOG_WARN("failed to perform optimization", K(ret));
    } else if (OB_FAIL(plan->init_candidate_plans(plan->get_candidate_plans().candidate_plans_))) {
      LOG_WARN("failed to do candi into", K(ret));
    } else if (OB_FAIL(plan->get_candidate_plans().get_best_plan(best_plan))) {
      LOG_WARN("failed to get best plan", K(ret));
    } else if (OB_FAIL(costs.push_back(best_plan->get_cost()))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObOptimizer::generate_plan_for_temp_table(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSqlTempTableInfo*> &temp_table_infos = ctx_.get_temp_table_infos();
  if (OB_FAIL(ObSqlTempTableInfo::collect_temp_tables(ctx_.get_allocator(), stmt, temp_table_infos,
                                                      ctx_.get_query_ctx(), true))) {
    LOG_WARN("failed to add all temp tables", K(ret));
  } else if (temp_table_infos.empty()) {
    //do nothing
  } else {
    ObSqlTempTableInfo *temp_table_info = NULL;
    ObSelectStmt *ref_query = NULL;
    ObSelectLogPlan *temp_plan = NULL;
    ObLogicalOperator *temp_op = NULL;
    ObShardingInfo *sharding_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i++) {
      ObRawExpr *temp_table_nonwhere_filter = NULL;
      ObRawExpr *temp_table_where_filter = NULL;
      bool can_push_to_where = true;
      if (OB_ISNULL(temp_table_info = temp_table_infos.at(i)) ||
          OB_ISNULL(ref_query = temp_table_info->table_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(temp_table_info), K(ref_query));
      } else if (OB_ISNULL(temp_plan = static_cast<ObSelectLogPlan*>
                                        (ctx_.get_log_plan_factory().create(ctx_, *ref_query)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create logical plan", K(temp_plan), K(ret));
      } else if (OB_FALSE_IT(temp_plan->set_temp_table_info(temp_table_info))) {
      } else {
        OPT_TRACE_TITLE("begin generate plan for temp table ", temp_table_info->table_name_);
      }
      /**
       * In table scan op, filters are calculated before `limit`.
       * So, we can not push filter into temp table which contain `limit`.
      */
      if (OB_FAIL(ret)) {
      } else if (!ref_query->has_limit() &&
                 OB_FAIL(ObOptimizerUtil::try_push_down_temp_table_filter(ctx_,
                                                                          *temp_table_info,
                                                                          temp_table_nonwhere_filter,
                                                                          temp_table_where_filter))) {
        LOG_WARN("failed to push down filter for temp table", K(ret));
      } else if (NULL != temp_table_where_filter &&
                 OB_FAIL(temp_plan->get_pushdown_filters().push_back(temp_table_where_filter))) {
        LOG_WARN("failed to push down filter", K(ret));
      } else if (OB_FAIL(temp_plan->generate_raw_plan())) {
        LOG_WARN("Failed to generate temp_plan for sub_stmt", K(ret));
      } else if (OB_FAIL(temp_plan->get_candidate_plans().get_best_plan(temp_op))) {
        LOG_WARN("failed to get best plan", K(ret));
      } else if (OB_FAIL(temp_plan->choose_duplicate_table_replica(temp_op,
                                                                   ctx_.get_local_server_addr(),
                                                                   true))) {
        LOG_WARN("failed to choose duplicate table replica", K(ret));
      } else if (OB_ISNULL(temp_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        sharding_info = temp_op->get_strong_sharding();
        if (NULL != sharding_info && sharding_info->get_can_reselect_replica()) {
          sharding_info->set_can_reselect_replica(false);
        }
        if (NULL != temp_table_nonwhere_filter) {
          ObSEArray<ObRawExpr *, 1> expr_array;
          if (OB_FAIL(expr_array.push_back(temp_table_nonwhere_filter))) {
            LOG_WARN("failed to push back");
          } else if (OB_FAIL(temp_plan->candi_allocate_filter(expr_array))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        temp_table_info->table_plan_ = temp_op;
        OPT_TRACE_TITLE("end generate plan for temp table ", temp_table_info->table_name_);
      }
    }
  }
  return ret;
}

bool ObOptimizer::exists_temp_table(const ObIArray<ObSqlTempTableInfo*> &temp_table_infos,
                                    const ObSelectStmt *table_query) const
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < temp_table_infos.count(); i++) {
    bret = NULL != temp_table_infos.at(i) &&
           temp_table_infos.at(i)->table_query_ == table_query;
  }
  return bret;
}

int ObOptimizer::get_session_parallel_info(int64_t &force_parallel_dop,
                                           bool &enable_auto_dop,
                                           bool &enable_manual_dop)
{
  int ret = OB_SUCCESS;
  force_parallel_dop = ObGlobalHint::UNSET_PARALLEL;
  enable_auto_dop = false;
  enable_manual_dop = false;
  ObSQLSessionInfo *session_info = NULL;
  uint64_t session_force_dop = ObGlobalHint::UNSET_PARALLEL;
  if (OB_ISNULL(session_info = ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (!session_info->is_user_session() && !session_info->get_ddl_info().is_refreshing_mview()) {
    // sys var是依赖于schema的方式实现的，获得最新的sys var需要通过inner SQL的方式，会产生循环依赖
    // 因此inner SQL情况下不考虑系统变量`SYS_VAR__ENABLE_PARALLEL_QUERY`的值
  } else if (OB_FAIL(session_info->get_parallel_degree_policy_enable_auto_dop(enable_auto_dop))) {
    LOG_WARN("failed to get sys variable for parallel degree policy", K(ret));
  } else if (enable_auto_dop) {
    /* do nothing */
  } else if (!ctx_.can_use_pdml()) {
    bool session_enable_parallel = false;
    if (OB_FAIL(session_info->get_enable_parallel_query(session_enable_parallel))) {
      LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
    } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_dop))) {
      LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
    } else if (!session_enable_parallel) {
      /* disable parallel */
    } else if (ObGlobalHint::DEFAULT_PARALLEL < session_force_dop) {
      force_parallel_dop = session_force_dop;
    } else {
      enable_manual_dop = true;
    }
  } else if (OB_FAIL(session_info->get_force_parallel_dml_dop(session_force_dop))) {
    LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
  } else if (ObGlobalHint::DEFAULT_PARALLEL < session_force_dop) {
    force_parallel_dop = session_force_dop;
  } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_dop))) {
    LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
  } else if (ObGlobalHint::DEFAULT_PARALLEL < session_force_dop) {
    force_parallel_dop = session_force_dop;
  } else {
    enable_manual_dop = true;
  }

  LOG_TRACE("finish get parallel from session", K(ret), K(ctx_.can_use_pdml()), K(force_parallel_dop),
                                                K(enable_auto_dop), K(enable_manual_dop));
  return ret;
}

int ObOptimizer::check_dml_parallel_mode()
{
  int ret = OB_SUCCESS;
  if (ctx_.can_use_pdml() && ctx_.get_can_use_parallel_das_dml()) {
    if (ctx_.get_global_hint().get_parallel_das_dml_option() == ObParallelDASOption::ENABLE) {
      ctx_.set_can_use_pdml(false);
      LOG_TRACE("parallel_das_dml hint cause force use das parallel");
    } else {
      ctx_.set_can_use_parallel_das_dml(false);
      LOG_TRACE("pdml and parallel das dml all supported, choose pdml");
    }
  }
  return ret;
}

int ObOptimizer::check_parallel_das_dml_supported_feature(const ObDelUpdStmt &pdml_stmt,
                                                          const ObSQLSessionInfo &session,
                                                          bool &use_parallel_das_dml)
{
  int ret = OB_SUCCESS;
  if (pdml_stmt.is_ignore()) {
    use_parallel_das_dml = false;
  } else if (ctx_.has_dblink()) {
    use_parallel_das_dml = false;
    LOG_TRACE("has dblink not use parallel das dml");
  } else if (!ctx_.has_fk() && ctx_.contain_user_nested_sql()) {
    use_parallel_das_dml = false;
    LOG_TRACE("ctx has user nexted sql not use parallel das dml");
  } else if (ctx_.is_online_ddl()) {
    use_parallel_das_dml = false;
    LOG_TRACE("is online ddl not use parallel das dml");
  }
  return ret;
}

int ObOptimizer::check_parallel_das_dml_enabled(const ObDMLStmt &stmt,
                                                ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool can_use_parallel_das_dml = true;
  ObSqlCtx *sql_ctx = NULL;
  ObQueryCtx *query_ctx = NULL;
  bool session_enable_pdml = false;
  bool session_enable_auto_dop = false;
  uint64_t session_pdml_dop = ObGlobalHint::UNSET_PARALLEL;

  if (OB_ISNULL(ctx_.get_exec_ctx()) || OB_ISNULL(query_ctx = ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()), K(query_ctx));
  } else if (OB_ISNULL(sql_ctx = ctx_.get_exec_ctx()->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()));
  } else if (!session.enable_parallel_das_dml()) {
    can_use_parallel_das_dml = false;
    LOG_TRACE("enable_das_parallel_execution is false, can't submit task parallel");
  } else if (!stmt.is_px_dml_supported_stmt()) {
    can_use_parallel_das_dml = false;
    LOG_TRACE("stmt can't support parallel_das_dml");
  } else if (ctx_.has_var_assign() && !ctx_.is_var_assign_only_in_root_stmt()) {
    // It can be supported, but we won’t let it go for now and wait for the follow-up.
    can_use_parallel_das_dml = false;
    LOG_TRACE("has var_assign, can't support parallel_das_dml now");
  } else if (!session.is_user_session()) {
    // not user request
    can_use_parallel_das_dml = false;
    LOG_TRACE("not user request, can't support parallel_das_dml");
  } else if (!is_strict_mode(session.get_sql_mode())) {
    can_use_parallel_das_dml = false;
    LOG_TRACE("not strict mode, can't support parallel_das_dml");
  } else if (OB_FAIL(check_parallel_das_dml_supported_feature(static_cast<const ObDelUpdStmt&>(stmt),
                                                              session,
                                                              can_use_parallel_das_dml))) {

  } else if (!can_use_parallel_das_dml) {
    LOG_TRACE("not support parallel das dml");
  } else if (ctx_.get_global_hint().get_parallel_das_dml_option() == ObParallelDASOption::DISABLE) {
    can_use_parallel_das_dml = false;
    LOG_TRACE("hint force close das parallel, can't support parallel_das_dml");
  } else if (query_ctx->get_query_hint().has_outline_data() &&
      ctx_.get_global_hint().get_parallel_das_dml_option() != ObParallelDASOption::ENABLE) {
    can_use_parallel_das_dml = false;
    LOG_TRACE("has outline data and hint disable, can't support parallel_das_dml",
            K(ctx_.get_global_hint().get_parallel_das_dml_option()),
            K(query_ctx->get_query_hint().has_outline_data()));
  } else if (ctx_.can_use_pdml()) {
    // can use pdml must can use parallel_das_dml
    LOG_TRACE("pdml is enabled, can use parallel das");
  } else if (ctx_.get_global_hint().get_pdml_option() == ObPDMLOption::ENABLE ||
      ctx_.get_global_hint().get_parallel_das_dml_option() == ObParallelDASOption::ENABLE) {
    LOG_TRACE("can use parallel_das_dml by pdml hint",
        K(ctx_.get_global_hint().get_pdml_option()),
        K(ctx_.get_global_hint().get_parallel_das_dml_option()));
  } else if (ctx_.get_global_hint().enable_auto_dop()) {
    // enable parallel das dml by auto dop hint
  } else if (OB_FAIL(session.get_parallel_degree_policy_enable_auto_dop(session_enable_auto_dop))) {
    LOG_WARN("failed to get sys variable for parallel degree policy", K(ret));
  } else if (session_enable_auto_dop && !ctx_.get_global_hint().has_parallel_hint()) {
    // session enable parallel dml by auto dop
  } else if (OB_FAIL(session.get_enable_parallel_dml(session_enable_pdml))
      || OB_FAIL(session.get_force_parallel_dml_dop(session_pdml_dop))) {
    LOG_WARN("failed to get sys variable for parallel dml", K(ret));
  } else if (session_enable_pdml || ObGlobalHint::DEFAULT_PARALLEL < session_pdml_dop) {
    // enable parallel das dml by session
  } else {
    can_use_parallel_das_dml = false;
    LOG_TRACE("other scense, can't support parallel_das_dml");
  }

  if (OB_SUCC(ret)) {
    ctx_.set_can_use_parallel_das_dml(can_use_parallel_das_dml);
  }

  return ret;
}

int ObOptimizer::check_pdml_enabled(const ObDMLStmt &stmt,
                                    const ObSQLSessionInfo &session)
{
  //
  // 1. decided by enable_parallel_dml and disable_parallel_dml hint
  // 2. decided by auto dop (by parallel(auto) hint or session variable parallel_degree_policy)
  // 3. decided by session variable: _enable_parallel_dml is true or _force_parallel_dml_dop > 1;
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = NULL;
  ObQueryCtx *query_ctx = NULL;
  bool can_use_pdml = true;
  bool session_enable_pdml = false;
  bool enable_auto_dop = false;
  uint64_t session_pdml_dop = ObGlobalHint::UNSET_PARALLEL;
  bool disable_pdml = false;
  bool is_pk_auto_inc = false;
  if (OB_ISNULL(ctx_.get_exec_ctx()) || OB_ISNULL(query_ctx = ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()), K(query_ctx));
  } else if (OB_ISNULL(sql_ctx = ctx_.get_exec_ctx()->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()));
  } else if (sql_ctx->is_batch_params_execute()) {
    can_use_pdml = false;
    // 当batch优化打开时，不支持pdml
  } else if (!stmt.is_pdml_supported_stmt()) {
    // pdml 支持新引擎和老引擎
    // 3.1 及之前的版本，老引擎走 dml + px。3.2 起老引擎也能走 pdml
    can_use_pdml = false;
  } else if (ctx_.has_var_assign() && !ctx_.is_var_assign_only_in_root_stmt()) {
    can_use_pdml = false;
  } else if (stmt::T_INSERT == stmt.get_stmt_type() &&
      OB_FAIL(static_cast< const ObInsertStmt &>(stmt).check_pdml_disabled(ctx_.is_online_ddl(),
                                                                           disable_pdml,
                                                                           is_pk_auto_inc))) {
    LOG_WARN("fail to check pdml disabled for insert stmt", K(ret));
  } else if (disable_pdml) {
    can_use_pdml = false;
    if (is_pk_auto_inc) {
      ctx_.add_plan_note(PDML_DISABLED_BY_INSERT_PK_AUTO_INC);
    }
  } else if (OB_FAIL(check_pdml_supported_feature(static_cast<const ObDelUpdStmt&>(stmt),
                                                  session, can_use_pdml))) {
    LOG_WARN("failed to check pdml supported feature", K(ret));
  } else if (!can_use_pdml || ctx_.is_online_ddl() ||
             (stmt::T_INSERT == stmt.get_stmt_type() && static_cast< const ObInsertStmt &>(stmt).is_normal_table_overwrite())) {
    // do nothing
  } else if (!is_strict_mode(session.get_sql_mode())) {
    can_use_pdml = false;
    LOG_TRACE("not strict mode, can't support PDML");
  } else if (ctx_.get_global_hint().get_pdml_option() == ObPDMLOption::ENABLE) {
    // 1. enable parallel dml by hint
  } else if (ctx_.get_global_hint().get_pdml_option() == ObPDMLOption::DISABLE
             || query_ctx->get_query_hint().has_outline_data()) {
    can_use_pdml = false; // 1. disable parallel dml by hint
  } else if (ctx_.get_global_hint().enable_auto_dop()) {
    // 2.1 enable parallel dml by auto dop
  } else if (session.is_user_session() &&
             OB_FAIL(session.get_parallel_degree_policy_enable_auto_dop(enable_auto_dop))) {
    LOG_WARN("failed to get sys variable for parallel degree policy", K(ret));
  } else if (enable_auto_dop && !ctx_.get_global_hint().has_parallel_hint()) {
    // 2.2 enable parallel dml by auto dop
  } else if (!session.is_user_session() && !session.get_ddl_info().is_refreshing_mview()) {
    can_use_pdml = false;
  } else if (OB_FAIL(session.get_enable_parallel_dml(session_enable_pdml))
             || OB_FAIL(session.get_force_parallel_dml_dop(session_pdml_dop))) {
    LOG_WARN("failed to get sys variable for parallel dml", K(ret));
  } else if (session_enable_pdml || ObGlobalHint::DEFAULT_PARALLEL < session_pdml_dop) {
    // 3. enable parallel dml by session
  } else {
    can_use_pdml = false;
  }

  if (OB_FAIL(ret)) {
  } else if (can_use_pdml && OB_FAIL(check_is_heap_table(stmt))) {
    LOG_WARN("failed to check is heap table", K(ret));
  } else {
    ctx_.set_can_use_pdml(can_use_pdml);
    LOG_TRACE("check use all pdml feature", K(ret), K(can_use_pdml), K(ctx_.is_online_ddl()), K(session_enable_pdml));
  }
  return ret;
}

int ObOptimizer::check_direct_load_enabled(const ObDMLStmt &stmt, const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (stmt::T_INSERT == stmt.get_stmt_type()) {
    const ObInsertStmt &insert_stmt = static_cast<const ObInsertStmt &>(stmt);
    if (insert_stmt.value_from_select() && !insert_stmt.is_external_table_overwrite() && !insert_stmt.is_ignore()) {
      ObDirectLoadOptimizerCtx &direct_load_optimize_ctx = ctx_.get_direct_load_optimizer_ctx();
      if (OB_FAIL(direct_load_optimize_ctx.init_direct_load_ctx(insert_stmt, ctx_))) {
        LOG_WARN("fail to init direct load ctx", K(ret));
      }
    }
  }
  return ret;
}

// check pdml enable sql case
int ObOptimizer::check_pdml_supported_feature(const ObDelUpdStmt &pdml_stmt,
    const ObSQLSessionInfo &session, bool &is_use_pdml)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
  ObSEArray<const ObDmlTableInfo*, 2> table_infos;
  // 依次检查被禁止的不稳定的功能，如果存在被禁止的不稳定功能 is open = false
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the schema guard is null", K(ret));
  } else if (pdml_stmt.is_ignore()) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_IGNORE);
  } else if (OB_FAIL(pdml_stmt.get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (table_infos.count() != 1) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_JOINED_TABLES);
  } else if (stmt::T_INSERT == pdml_stmt.get_stmt_type() &&
             static_cast< const ObInsertStmt &>(pdml_stmt).is_insert_up()) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_INSERT_UP);
  } else if (pdml_stmt.is_pdml_disabled()) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_TRANSFORMATIONS);
  } else if (ctx_.has_dblink()) {
    is_use_pdml = false;
  } else if (ctx_.contain_user_nested_sql()) {
    //user nested sql can't use PDML plan, force to use DAS plan
    //if online ddl has pl udf, only this way, allow it use PDML plan
    //such as:
    //create table t1(a int primary key, b int as(udf()));
    //create index i1 on t1(b);
    //create index with PL UDF allow to use PDML plan during build index table
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_NESTED_SQL);
  } else if (stmt::T_DELETE == pdml_stmt.get_stmt_type()) {
    //
    // if no trigger, no foreign key, delete can do pdml, even if with local unique index
    is_use_pdml = true;
  } else if (!ctx_.is_online_ddl()) {
    // check enabling parallel with local unique index
    //  1. disable parallel insert. because parallel unique check not supported
    //     (storage does not support parallel unique check in one update/insert statement.)
    //  2. disable parallel update. only if the unqiue column is updated.
    //     [FIXME] for now, we blindly disable PDML if table has unique local index
    //  3. disable global index if main table has only one partition issue#35726194
    //
    // future work:
    // data is reshuffled by partition key, so that same unique value may be reshuffled
    // to different thread. To make same unique value reshuffled to same thread, we can
    // do a hybrid reshuffle: map partition key to server, map unique key to thread.
    // However, if there are more than one unique local index, this method will still fail.
    uint64_t main_table_tid = table_infos.at(0)->ref_table_id_;
    bool with_unique_local_idx = false;
    if (OB_FAIL(schema_guard->check_has_local_unique_index(
                session.get_effective_tenant_id(),
                main_table_tid, with_unique_local_idx))) {
      LOG_WARN("fail check if table with local unqiue index", K(main_table_tid), K(ret));
    } else if (stmt::T_UPDATE == pdml_stmt.get_stmt_type()) {
      for (int i = 0; OB_SUCC(ret) && is_use_pdml && i <
          table_infos.at(0)->column_exprs_.count(); i++) {
        ObColumnRefRawExpr* column_expr = table_infos.at(0)->column_exprs_.at(i);
        if (column_expr->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG)) {
          is_use_pdml = false;
          ctx_.add_plan_note(PDML_DISABLED_BY_UPDATE_NOW);
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::check_is_heap_table(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_session_info();
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
  const share::schema::ObTableSchema *table_schema = NULL;
  const ObDelUpdStmt *pdml_stmt = NULL;
  ObSEArray<const ObDmlTableInfo*, 1> dml_table_infos;
  // check if the target table is heap table
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session));
  } else if (ctx_.is_online_ddl()) {
    ctx_.set_is_heap_table_ddl(session->get_ddl_info().is_heap_table_ddl());
  } else if (NULL == (pdml_stmt = dynamic_cast<const ObDelUpdStmt*>(&stmt))) {
    // do nothing
  } else if (OB_FAIL(pdml_stmt->get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (OB_UNLIKELY(dml_table_infos.count() != 1) || OB_ISNULL(dml_table_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dml table infos", K(ret), K(dml_table_infos));
  } else if (OB_INVALID_ID == dml_table_infos.at(0)->ref_table_id_) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                    dml_table_infos.at(0)->ref_table_id_,
                                                    table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if(OB_NOT_NULL(table_schema) && table_schema->is_table_without_pk()) {
    ctx_.set_is_pdml_heap_table(true);
  }
  return ret;
}

int ObOptimizer::init_env_info(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  int64_t rowgoal_type = -1;
  const ObOptParamHint &opt_params = ctx_.get_global_hint().opt_params_;
  if (OB_ISNULL(session_info = ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(extract_column_usage_info(stmt))) {
    LOG_WARN("failed to extract column usage info", K(ret));
  } else if (OB_FAIL(extract_opt_ctx_basic_flags(stmt, *session_info))) {
    LOG_WARN("fail to extract opt ctx basic flags", K(ret));
  } else if (OB_FAIL(check_pdml_enabled(stmt, *session_info))) {
    LOG_WARN("fail to check enable pdml", K(ret));
  } else if (OB_FAIL(check_direct_load_enabled(stmt, *session_info))) {
    LOG_WARN("fail to check enable direct load", K(ret));
  } else if (OB_FAIL(check_parallel_das_dml_enabled(stmt, *session_info))) {
    LOG_WARN("fail to check enable parallel das dml", K(ret));
  } else if (OB_FAIL(check_dml_parallel_mode())) {
    LOG_WARN("fail to check force use parallel das dml", K(ret));
  } else if (OB_FAIL(init_parallel_policy(stmt, *session_info))) { // call after check pdml enabled
    LOG_WARN("fail to check enable pdml", K(ret));
  } else if (OB_FAIL(init_replica_policy(stmt, *session_info))) {
    LOG_WARN("fail to check enable column store replica", K(ret));
  } else if (OB_FAIL(init_correlation_model(stmt, *session_info))) {
    LOG_WARN("failed to init correlation model", K(ret));
  } else if (OB_FAIL(init_table_access_policy(stmt, *session_info))) {
    LOG_WARN("failed to init table access policy", K(ret));
  } else if (OB_FAIL(opt_params.get_enum_sys_var(ObOptParamHint::ENABLE_OPTIMIZER_ROWGOAL,
                                                 session_info,
                                                 share::SYS_VAR_ENABLE_OPTIMIZER_ROWGOAL,
                                                 rowgoal_type))) {
    LOG_WARN("failed to get hint param", K(ret));
  } else if (FALSE_IT(ctx_.set_enable_opt_row_goal(rowgoal_type))) {
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizer::extract_opt_ctx_basic_flags(const ObDMLStmt &stmt, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool has_var_assign = false;
  bool is_var_assign_only_in_root_stmt = false;
  bool has_subquery_in_function_table = false;
  bool has_dblink = false;
  bool force_serial_set_order = false;
  bool storage_estimation_enabled = false;
  bool has_cursor_expr = false;
  int64_t link_stmt_count = 0;
  ObQueryCtx* query_ctx = ctx_.get_query_ctx();
  bool push_join_pred_into_view_enabled = true;
  bool partition_wise_plan_enabled = true;
  bool exists_partition_wise_plan_enabled_hint = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session.get_effective_tenant_id()));
  bool rowsets_enabled = tenant_config.is_valid() && tenant_config->_rowsets_enabled;
  ctx_.set_is_online_ddl(session.get_ddl_info().is_ddl());  // set is online ddl first, is used by other extract operations
  bool das_keep_order_enabled = tenant_config.is_valid() && tenant_config->_enable_das_keep_order;
  bool hash_join_enabled = tenant_config.is_valid() && tenant_config->_hash_join_enabled;
  bool optimizer_sortmerge_join_enabled = tenant_config.is_valid() && tenant_config->_optimizer_sortmerge_join_enabled;
  bool nested_loop_join_enabled = tenant_config.is_valid() && tenant_config->_nested_loop_join_enabled;
  bool enable_adj_index_cost = false;
  int64_t optimizer_index_cost_adj = 0;
  bool is_skip_scan_enable = session.is_index_skip_scan_enabled();
  bool enable_use_batch_nlj = false;
  bool better_inlist_costing = false;
  bool enable_spf_batch_rescan = session.is_spf_mlj_group_rescan_enabled();
  bool enable_px_ordered_coord = GCONF._enable_px_ordered_coord;
  int64_t das_batch_rescan_flag = tenant_config.is_valid() ? tenant_config->_enable_das_batch_rescan_flag : 0;
  bool enable_distributed_das_scan = tenant_config.is_valid() ? tenant_config->_enable_distributed_das_scan : true;
  const ObOptParamHint &opt_params = ctx_.get_global_hint().opt_params_;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null query ctx");
  } else if (OB_FAIL(check_whether_contain_nested_sql(stmt))) {
    LOG_WARN("check whether contain nested sql failed", K(ret));
  } else if (OB_FAIL(stmt.check_has_subquery_in_function_table(has_subquery_in_function_table))) {
    LOG_WARN("failed to check stmt has function table", K(ret));
  } else if (OB_FAIL(stmt.check_var_assign(has_var_assign, is_var_assign_only_in_root_stmt))) {
    LOG_WARN("failed to check has ref assign user var", K(ret));
  } else if (OB_FAIL(session.is_serial_set_order_forced(force_serial_set_order, lib::is_oracle_mode()))) {
    LOG_WARN("fail to get force_serial_set_order", K(ret));
  } else if (OB_FAIL(check_force_default_stat())) {
    LOG_WARN("failed to check force default stat", K(ret));
  } else if (OB_FAIL(init_system_stat())) {
    LOG_WARN("failed to init system stat", K(ret));
  } else if (OB_FAIL(calc_link_stmt_count(stmt, link_stmt_count))) {
    LOG_WARN("calc link stmt count failed", K(ret));
  } else if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(&stmt, has_dblink, true))) {
    LOG_WARN("failed to find dblink in stmt", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ROWSETS_ENABLED, rowsets_enabled))) {
    LOG_WARN("fail to check rowsets enabled", K(ret));
  } else if (OB_FAIL(stmt.check_has_cursor_expression(has_cursor_expr))) {
    LOG_WARN("fail to check cursor expression info", K(ret));
  } else if (OB_FAIL(session.is_storage_estimation_enabled(storage_estimation_enabled))) {
    LOG_WARN("fail to get storage_estimation_enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_DAS_KEEP_ORDER, das_keep_order_enabled))) {
    LOG_WARN("failed to check das keep order enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::HASH_JOIN_ENABLED, hash_join_enabled))) {
    LOG_WARN("failed to check hash join enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::OPTIMIZER_SORTMERGE_JOIN_ENABLED, optimizer_sortmerge_join_enabled))) {
    LOG_WARN("failed to check merge join enabled", K(ret));
  } else if (OB_FALSE_IT(nested_loop_join_enabled |= query_ctx->is_contain_inner_table_)) {
    //ignore tenant config for inner table
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::NESTED_LOOP_JOIN_ENABLED, nested_loop_join_enabled))) {
    LOG_WARN("failed to check nested loop join enabled", K(ret));
  } else if (OB_FAIL(session.is_adj_index_cost_enabled(enable_adj_index_cost, optimizer_index_cost_adj))) {
    LOG_WARN("failed to check adjust index cost", K(ret));
  } else if (OB_FAIL(opt_params.get_integer_opt_param(ObOptParamHint::OPTIMIZER_INDEX_COST_ADJ, optimizer_index_cost_adj))) {
    LOG_WARN("fail to check opt param adjust index cost", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::OPTIMIZER_SKIP_SCAN_ENABLED, is_skip_scan_enable))) {
    LOG_WARN("failed to get opt param skip scan enabled", K(ret));
  } else if (OB_FAIL(session.is_better_inlist_enabled(better_inlist_costing))) {
    LOG_WARN("failed to check better inlist enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::OPTIMIZER_BETTER_INLIST_COSTING, better_inlist_costing))) {
    LOG_WARN("failed to get opt param better inlist costing", K(ret));
  } else if (OB_FAIL(session.get_nlj_batching_enabled(enable_use_batch_nlj))) {
    LOG_WARN("failed to get join cache size variable", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::NLJ_BATCHING_ENABLED, enable_use_batch_nlj))) {
    LOG_WARN("failed to get opt param nlj batching enable", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_SPF_BATCH_RESCAN, enable_spf_batch_rescan))) {
    LOG_WARN("failed to get opt param enable spf batch rescan", K(ret));
  } else if (OB_FAIL(ctx_.get_global_hint().opt_params_.get_bool_opt_param(ObOptParamHint::_PUSH_JOIN_PREDICATE, push_join_pred_into_view_enabled))) {
    LOG_WARN("fail to check rowsets enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::PARTITION_WISE_PLAN_ENABLED,
                                                   partition_wise_plan_enabled,
                                                   exists_partition_wise_plan_enabled_hint))) {
    LOG_WARN("failed to check partition wise plan enabled", K(ret));
  } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_PX_ORDERED_COORD, enable_px_ordered_coord))) {
    LOG_WARN("failed to get opt param enable px ordered coord", K(ret));
  } else if (OB_FAIL(opt_params.get_integer_opt_param(ObOptParamHint::DAS_BATCH_RESCAN_FLAG, das_batch_rescan_flag))) {
    LOG_WARN("failed to get das batch rescan flag", K(ret));
  } else {
    ctx_.init_batch_rescan_flags(enable_use_batch_nlj, enable_spf_batch_rescan,
      query_ctx->optimizer_features_enable_version_, das_batch_rescan_flag);
    ctx_.set_storage_estimation_enabled(storage_estimation_enabled);
    ctx_.set_serial_set_order(force_serial_set_order);
    ctx_.set_has_multiple_link_stmt(link_stmt_count > 1);
    ctx_.set_has_var_assign(has_var_assign);
    ctx_.set_is_var_assign_only_in_root_stmt(is_var_assign_only_in_root_stmt);
    ctx_.set_has_subquery_in_function_table(has_subquery_in_function_table);
    ctx_.set_has_dblink(has_dblink);
    ctx_.set_cost_model_type(rowsets_enabled ? ObOptEstCost::VECTOR_MODEL : ObOptEstCost::NORMAL_MODEL);
    ctx_.set_has_cursor_expression(has_cursor_expr);
    ctx_.set_das_keep_order_enabled(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_2_0 ? false : das_keep_order_enabled);
    ctx_.set_optimizer_index_cost_adj(optimizer_index_cost_adj);
    ctx_.set_is_skip_scan_enabled(is_skip_scan_enable);
    ctx_.set_enable_better_inlist_costing(better_inlist_costing);
    ctx_.set_push_join_pred_into_view_enabled(push_join_pred_into_view_enabled);
    ctx_.set_enable_px_ordered_coord(enable_px_ordered_coord);
    ctx_.set_enable_distributed_das_scan(enable_distributed_das_scan);
    if (!hash_join_enabled
        && !optimizer_sortmerge_join_enabled
        && !nested_loop_join_enabled) {
      ctx_.set_hash_join_enabled(true);
      ctx_.set_merge_join_enabled(true);
      ctx_.set_nested_join_enabled(true);
      LOG_TRACE("all join types are set to disable");
    } else {
      ctx_.set_hash_join_enabled(hash_join_enabled);
      ctx_.set_merge_join_enabled(optimizer_sortmerge_join_enabled);
      ctx_.set_nested_join_enabled(nested_loop_join_enabled);
    }
    if (tenant_config.is_valid()) {
      ctx_.set_partition_wise_plan_enabled(tenant_config->_partition_wise_plan_enabled);
    }
    if (exists_partition_wise_plan_enabled_hint) {
      ctx_.set_partition_wise_plan_enabled(partition_wise_plan_enabled);
    }
    if (!session.is_inner() && stmt.get_query_ctx()->get_injected_random_status()) {
      ctx_.set_generate_random_plan(true);
    }
    if (session.is_enable_new_query_range() &&
        ObSQLUtils::is_min_cluster_version_ge_425_or_435() &&
        ObSQLUtils::is_opt_feature_version_ge_425_or_435(query_ctx->optimizer_features_enable_version_)) {
      ctx_.set_enable_new_query_range(true);
    }
  }
  return ret;
}

/* parallel policy priority:
    1. not support parallel: pl udf, dblink
    2. global parallel hint:
      a. force parallel dop: parallel(4)
      b. auto dop: parallel(auto)
      c. manual table dop: parallel(manual)
      d. disable parallel: parallel(1) or no_parallel
    3. session variable:
      a. force parallel dop: _force_parallel_query_dop / _force_parallel_dml_dop > 1
      b. auto dop: _enable_parallel_query / _enable_parallel_dml = 1,
                   _force_parallel_query_dop / _force_parallel_dml_dop = 1,
                   parallel_degree_policy = aoto
      c. manual table dop: _enable_parallel_query / _enable_parallel_dml = 1,
                           _force_parallel_query_dop / _force_parallel_dml_dop = 1,
                           parallel_degree_policy = manual
      d. disable parallel: _enable_parallel_query / _enable_parallel_dml = 0
*/
int ObOptimizer::init_parallel_policy(ObDMLStmt &stmt, const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ctx_.set_parallel(ObGlobalHint::DEFAULT_PARALLEL);
  int64_t session_force_parallel_dop = ObGlobalHint::UNSET_PARALLEL;
  bool session_enable_auto_dop = false;
  bool session_enable_manual_dop = false;
  if (OB_ISNULL(ctx_.get_query_ctx()) || OB_ISNULL(ctx_.get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is nul", K(ret));
  } else if (ctx_.get_exec_ctx()->is_force_gen_local_plan()) {
     ctx_.set_parallel_rule(PXParallelRule::PL_UDF_DAS_FORCE_SERIALIZE);
  } else if (ctx_.has_pl_udf()) {
    //following above rule, but if stmt contain pl_udf, force das, parallel should be 1
    ctx_.set_parallel_rule(PXParallelRule::PL_UDF_DAS_FORCE_SERIALIZE);
  } else if (ctx_.has_cursor_expression()) {
    // if stmt contain cursor expression, cannot remote execute, force das, parallel should be 1
    ctx_.set_parallel_rule(PXParallelRule::PL_UDF_DAS_FORCE_SERIALIZE);
  } else if (ctx_.has_dblink()) {
    //if stmt contain dblink, force das, parallel should be 1
    ctx_.set_parallel_rule(PXParallelRule::DBLINK_FORCE_SERIALIZE);
  } else if (ctx_.get_global_hint().has_parallel_degree()) {
    ctx_.set_parallel_rule(PXParallelRule::MANUAL_HINT);
    ctx_.set_parallel(ctx_.get_global_hint().get_parallel_degree());
  } else if (ctx_.get_global_hint().enable_auto_dop()) {
    ctx_.set_parallel_rule(PXParallelRule::AUTO_DOP);
  } else if (ctx_.get_query_ctx()->get_query_hint().has_outline_data()) {
    ctx_.set_parallel_rule(PXParallelRule::MANUAL_HINT);
    ctx_.set_parallel(ObGlobalHint::DEFAULT_PARALLEL);
  } else if (session.is_user_session() && !ctx_.get_global_hint().enable_manual_dop() &&
             OB_FAIL(OB_E(EventTable::EN_ENABLE_AUTO_DOP_FORCE_PARALLEL_PLAN) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    ctx_.set_parallel_rule(PXParallelRule::AUTO_DOP);
  } else if (OB_FAIL(get_session_parallel_info(session_force_parallel_dop,
                                               session_enable_auto_dop,
                                               session_enable_manual_dop))) {
    LOG_WARN("failed to get session parallel info", K(ret));
  } else if (session_enable_auto_dop && !ctx_.get_global_hint().enable_manual_dop()) {
    ctx_.set_parallel_rule(PXParallelRule::AUTO_DOP);
  } else if (ObGlobalHint::UNSET_PARALLEL != session_force_parallel_dop) {
    ctx_.set_parallel(session_force_parallel_dop);
    ctx_.set_parallel_rule(PXParallelRule::SESSION_FORCE_PARALLEL);
  } else if (session_enable_manual_dop || ctx_.get_global_hint().enable_manual_dop()) {
    ctx_.set_parallel_rule(PXParallelRule::MANUAL_TABLE_DOP);
  } else {
    ctx_.set_parallel_rule(PXParallelRule::USE_PX_DEFAULT);
  }

  if (OB_FAIL(ret)) {
  } else if (ctx_.is_use_auto_dop() && OB_FAIL(set_auto_dop_params(session))) {
    LOG_WARN("failed to set auto dop params", K(ret));
  } else if (OB_FAIL(init_px_node_opt_info(session.get_effective_tenant_id()))) {
    LOG_WARN("failed to init px node opt info", K(ret));
  } else {
    LOG_TRACE("succeed to init parallel policy", K(session.is_user_session()),
                        K(ctx_.can_use_pdml()), K(ctx_.get_parallel_rule()), K(ctx_.get_parallel()),
                        K(ctx_.get_auto_dop_params()));
  }
  return ret;
}

int ObOptimizer::init_px_node_opt_info(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObGlobalHint &global_hint = ctx_.get_global_hint();
  // PX_NODE_ADDRS has a higher priority than PX_NODE_COUNT and PX_NODE_POLICY.
  if (!global_hint.px_node_hint_.px_node_addrs_.empty()) {
    ctx_.set_px_node_selection_mode(ObPxNodeSelectionMode::SPECIFY_NODE);
  } else {
    if (global_hint.px_node_hint_.px_node_count_ != ObPxNodeHint::UNSET_PX_NODE_COUNT) {
      ctx_.set_px_node_selection_mode(ObPxNodeSelectionMode::SPECIFY_COUNT);
    } else {
      ctx_.set_px_node_selection_mode(ObPxNodeSelectionMode::DEFAULT);
    }
    // For PX_NODE_POLICY,
    // the priority of hints is higher than that of tenant configuration settings.
    if (global_hint.px_node_hint_.px_node_policy_ != ObPxNodePolicy::INVALID) {
      ctx_.set_px_node_policy(global_hint.px_node_hint_.px_node_policy_);
    } else {
      ObPxNodePolicy tenant_config_px_node_policy;
      if (OB_FAIL(ObPxNodePool::get_tenant_config_px_node_policy(tenant_id,
                          tenant_config_px_node_policy))) {
        LOG_WARN("Failed to get tenant config px_node_policy", K(ret));
      } else {
        ctx_.set_px_node_policy(tenant_config_px_node_policy);
      }
    }
  }
  return ret;
}

int ObOptimizer::init_replica_policy(ObDMLStmt &dml_stmt, const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t route_policy_type = 0;
  if (OB_FAIL(session.get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy_type))) {
    LOG_WARN("fail to get sys variable", K(ret));
  } else if (COLUMN_STORE_ONLY == static_cast<ObRoutePolicyType>(route_policy_type)) {
    if (dml_stmt.get_query_ctx()->has_dml_write_stmt_ ||
        dml_stmt.get_query_ctx()->is_contain_select_for_update_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "when route policy is COLUMN_STORE_ONLY, read query request");
    } else {
      ctx_.set_use_column_store_replica(true);
    }
  }
  return ret;
}

int ObOptimizer::init_correlation_model(ObDMLStmt &stmt, const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObEstCorrelationModel* correlation_model = NULL;
  int64_t type = 0;
  bool has_new_hint = false;
  if (OB_ISNULL(ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctx", K(ret));
  } else if (!ctx_.get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_4, COMPAT_VERSION_4_3_0,
                                                             COMPAT_VERSION_4_3_3)) {
    type = static_cast<int64_t>(ObEstCorrelationType::INDEPENDENT);
  } else if (OB_FAIL(ctx_.get_global_hint().opt_params_.has_opt_param(ObOptParamHint::CARDINALITY_ESTIMATION_MODEL, has_new_hint))) {
    LOG_WARN("failed to check whether has hint param", K(ret));
  } else {
    /**
     * 'cardinality_estimation_model' is same as the name of the system variable.
     * 'correlation_for_cardinality_estimation' should be deprecated.
     * So the former has a higher priority.
    */
    ObOptParamHint::OptParamType opt_param_type = ObOptParamHint::CORRELATION_FOR_CARDINALITY_ESTIMATION;
    if (has_new_hint) {
      opt_param_type = ObOptParamHint::CARDINALITY_ESTIMATION_MODEL;
    }
    if (OB_FAIL(ctx_.get_global_hint().opt_params_.get_enum_sys_var(opt_param_type,
                                                                    &session,
                                                                    share::SYS_VAR_CARDINALITY_ESTIMATION_MODEL,
                                                                    type))) {
      LOG_WARN("failed to get hint param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(type < 0) ||
        OB_UNLIKELY(type >= static_cast<int64_t>(ObEstCorrelationType::MAX))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected correlation type", K(type));
    } else {
      ctx_.set_correlation_type(static_cast<ObEstCorrelationType>(type));
    }
  }
  return ret;
}

int ObOptimizer::init_table_access_policy(ObDMLStmt &stmt, const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t policy = 0;
  bool has_hint = false;
  if (OB_ISNULL(ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctx", K(ret));
  } else if (OB_FAIL(ctx_.get_global_hint().opt_params_.has_opt_param(ObOptParamHint::OB_TABLE_ACCESS_POLICY, has_hint))) {
    LOG_WARN("failed to check whether has hint param", K(ret));
  } else if (has_hint) {
    if (OB_FAIL(ctx_.get_global_hint().opt_params_.get_enum_opt_param(ObOptParamHint::OB_TABLE_ACCESS_POLICY, policy))) {
      LOG_WARN("failed to get enum hint param", K(ret));
    }
  } else if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_OB_TABLE_ACCESS_POLICY, policy))) {
    LOG_WARN("failed to get sys variable", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(policy < 0) ||
        OB_UNLIKELY(policy >= static_cast<int64_t>(ObTableAccessPolicy::MAX))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table access polisy", K(policy));
    } else {
      ctx_.set_table_access_policy(static_cast<ObTableAccessPolicy>(policy));
    }
  }
  return ret;
}

int ObOptimizer::set_auto_dop_params(const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  uint64_t parallel_degree_limit = 0;
  uint64_t parallel_min_scan_time_threshold = 1000;
  AutoDOPParams params;
  if (!session.is_user_session()) {
    /* do nothing */
  } else if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_PARALLEL_DEGREE_LIMIT, parallel_degree_limit))) {
    LOG_WARN("failed to get sys variable parallel degree limit", K(ret));
  } else if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_PARALLEL_MIN_SCAN_TIME_THRESHOLD, parallel_min_scan_time_threshold))) {
    LOG_WARN("failed to get sys variable parallel threshold", K(ret));
  }

  if (OB_SUCC(ret) && 0 == parallel_degree_limit) {
    const ObTenantBase *tenant = NULL;
    int64_t parallel_servers_target = 0;
    if (OB_ISNULL(tenant = MTL_CTX())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (session.is_user_session() &&
               OB_FAIL(ObSchemaUtils::get_tenant_int_variable(session.get_effective_tenant_id(),
                                                              SYS_VAR_PARALLEL_SERVERS_TARGET,
                                                              parallel_servers_target))) {
      LOG_WARN("fail read tenant variable", K(ret), K(session.get_effective_tenant_id()));
    } else {
      params.unit_min_cpu_ = std::max(tenant->unit_min_cpu(), 0.0);
      params.parallel_servers_target_ = std::max(parallel_servers_target, 0L);
    }
  }

  if (OB_SUCC(ret)) {
    params.parallel_min_scan_time_threshold_ = parallel_min_scan_time_threshold;
    params.parallel_degree_limit_ = parallel_degree_limit;
    ctx_.set_auto_dop_params(params);
  }
  return ret;
}

//to check whether contain trigger, foreign key, PL UDF or this sql is triggered by these object
int ObOptimizer::check_whether_contain_nested_sql(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *del_upd_stmt = nullptr;
  // dml + select need run das path
  if (!stmt.get_query_ctx()->disable_udf_parallel_ && stmt.get_query_ctx()->udf_has_select_stmt_) {
    stmt.get_query_ctx()->disable_udf_parallel_ |= ((stmt.is_select_stmt() && stmt.has_for_update())
                                                   || (stmt.is_dml_write_stmt()));
  }
  if (stmt.get_query_ctx()->disable_udf_parallel_) {
    ctx_.set_has_pl_udf(true);
  }
  if (ObSQLUtils::is_nested_sql(ctx_.get_exec_ctx())) {
    ctx_.set_in_nested_sql(true);
  }
  if ((del_upd_stmt = dynamic_cast<const ObDelUpdStmt*>(&stmt)) != nullptr) {
    ObSEArray<const ObDmlTableInfo*,2> table_infos;
    if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get dml table infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObDmlTableInfo* table_info = table_infos.at(i);
      ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
      const ObTableSchema *table_schema = nullptr;
      ObSQLSessionInfo *session = ctx_.get_session_info();
      bool trigger_exists = false;
      if (OB_ISNULL(table_info) || OB_ISNULL(schema_guard) || OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql schema guard is nullptr", K(ret), K(table_info), K(schema_guard), K(session));
      } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                        table_info->ref_table_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(table_info->ref_table_id_));
      } else if (!table_schema->get_foreign_key_infos().empty()) {
        ctx_.set_has_fk(true);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_schema->check_has_trigger_on_table(*schema_guard,
                                                             trigger_exists,
                                                             del_upd_stmt->get_trigger_events()))) {
          LOG_WARN("check has trigger on table failed", K(ret));
        } else if (trigger_exists) {
          ctx_.set_has_trigger(true);
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::calc_link_stmt_count(const ObDMLStmt &stmt, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (stmt.is_dblink_stmt()) {
    count += 1;
  } else {
    ObSEArray<ObSelectStmt *, 4> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(calc_link_stmt_count(*child_stmts.at(i), count)))) {
        LOG_WARN("failed to extract column usage info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const common::ObIArray<TableItem*> &table_items = stmt.get_table_items();
      for (int64_t i = 0; i < table_items.count() && OB_SUCC(ret); i++) {
        const TableItem *table_item = table_items.at(i);
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null ptr", K(ret));
        } else if (table_item->is_temp_table()) {
          if (OB_ISNULL(table_item->ref_query_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null ptr", K(ret));
          } else if (OB_FAIL(SMART_CALL(calc_link_stmt_count(*table_item->ref_query_, count)))) {
            LOG_WARN("failed to extract column usage info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::extract_column_usage_info(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  ObSEArray<ObRawExpr *, 32> condition_exprs;
  if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else if (OB_FAIL(stmt.get_where_scope_conditions(condition_exprs))) {
    LOG_WARN("failed to get where scope conditions", K(ret));
  } else if (stmt.is_select_stmt() &&
             OB_FAIL(append(condition_exprs, static_cast<const ObSelectStmt&>(stmt).get_having_exprs()))) {
    LOG_WARN("failed to append", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_ISNULL(child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_column_usage_info(*child_stmts.at(i))))) {
      LOG_WARN("failed to extract column usage info", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    if (OB_FAIL(analyze_one_expr(stmt, condition_exprs.at(i)))) {
      LOG_WARN("failed to analyze one expr", K(ret));
    }
  }

  if (OB_SUCC(ret) && stmt.is_select_stmt()) {
    const ObSelectStmt &sel_stmt = static_cast<const ObSelectStmt &>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_group_expr_size(); ++i) {
      const ObRawExpr *expr = sel_stmt.get_group_exprs().at(i);
      if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->is_column_ref_expr()) {
        ret = add_column_usage_arg(stmt,
                                   *(static_cast<const ObColumnRefRawExpr *>(expr)),
                                   ColumnUsageFlag::GROUPBY_MEMBER);
      }
    }
    if (sel_stmt.is_distinct() && !sel_stmt.is_set_stmt()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_select_item_size(); ++i) {
        const ObRawExpr *expr = sel_stmt.get_select_item(i).expr_;
        if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (expr->is_column_ref_expr()) {
          ret = add_column_usage_arg(stmt,
                                    *(static_cast<const ObColumnRefRawExpr *>(expr)),
                                    ColumnUsageFlag::DISTINCT_MEMBER);
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::analyze_one_expr(const ObDMLStmt &stmt, const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->get_expr_type() == T_OP_OR || expr->get_expr_type() == T_OP_AND) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(analyze_one_expr(stmt, expr->get_param_expr(i)))) {
        LOG_WARN("failed to analyze one expr", K(ret));
      }
    }
  } else if (expr->get_expr_type() == T_OP_IS || expr->get_expr_type() == T_OP_IS_NOT) {
    const ObRawExpr *left_expr = expr->get_param_expr(0);
    const ObRawExpr *right_expr = expr->get_param_expr(1);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (left_expr->is_column_ref_expr() && T_NULL == right_expr->get_expr_type()) {
      ret = add_column_usage_arg(stmt,
                                 *(static_cast<const ObColumnRefRawExpr *>(left_expr)),
                                 ColumnUsageFlag::NULL_PREDS);
    }
  } else if (expr->get_expr_type() == T_OP_LIKE) {
    const ObRawExpr *left_expr = expr->get_param_expr(0);
    if (OB_ISNULL(left_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (left_expr->is_column_ref_expr()) {
      ret = add_column_usage_arg(stmt,
                                 *(static_cast<const ObColumnRefRawExpr *>(left_expr)),
                                 ColumnUsageFlag::LIKE_PREDS);
    }
  } else if (expr->get_expr_type() == T_OP_EQ || expr->get_expr_type() == T_OP_NSEQ) {
    const ObRawExpr *left_expr = expr->get_param_expr(0);
    const ObRawExpr *right_expr = expr->get_param_expr(1);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (left_expr->is_column_ref_expr() && right_expr->is_column_ref_expr()) {
      if (OB_FAIL(add_column_usage_arg(stmt,
                                       *(static_cast<const ObColumnRefRawExpr *>(left_expr)),
                                       ColumnUsageFlag::EQUIJOIN_PREDS))) {
        LOG_WARN("failed to add column usage arg", K(ret), K(*expr));
      } else if (OB_FAIL(add_column_usage_arg(stmt,
                                              *(static_cast<const ObColumnRefRawExpr *>(right_expr)),
                                              ColumnUsageFlag::EQUIJOIN_PREDS))) {
        LOG_WARN("failed to add column usage arg", K(ret));
      }
    } else if (left_expr->is_column_ref_expr() || right_expr->is_column_ref_expr()) {
      const ObRawExpr *column_expr = left_expr->is_column_ref_expr() ? left_expr :right_expr;
      ret = add_column_usage_arg(stmt,
                                 *(static_cast<const ObColumnRefRawExpr *>(column_expr)),
                                 ColumnUsageFlag::EQUALITY_PREDS);
    } else { /*do nothing*/ }
  } else if (expr->get_expr_type() == T_OP_IN || expr->get_expr_type() == T_OP_NOT_IN) {
    const ObRawExpr *left_expr = expr->get_param_expr(0);
    if (OB_ISNULL(left_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (left_expr->is_column_ref_expr()) {
      ret = add_column_usage_arg(stmt,
                                 *(static_cast<const ObColumnRefRawExpr *>(left_expr)),
                                 ColumnUsageFlag::EQUALITY_PREDS);
    }
  } else if (IS_COMMON_COMPARISON_OP(expr->get_expr_type())) {
    const ObRawExpr *left_expr = expr->get_param_expr(0);
    const ObRawExpr *right_expr = expr->get_param_expr(1);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (left_expr->is_column_ref_expr() && right_expr->is_column_ref_expr()) {
      if (OB_FAIL(add_column_usage_arg(stmt,
                                       *(static_cast<const ObColumnRefRawExpr *>(left_expr)),
                                       ColumnUsageFlag::EQUIJOIN_PREDS))) {
        LOG_WARN("failed to add column usage arg", K(ret));
      } else if (OB_FAIL(add_column_usage_arg(stmt,
                                              *(static_cast<const ObColumnRefRawExpr *>(right_expr)),
                                              ColumnUsageFlag::NONEQUIJOIN_PREDS))) {
        LOG_WARN("failed to add column usage arg", K(ret));
      }
    } else if (left_expr->is_column_ref_expr() || right_expr->is_column_ref_expr()) {
      const ObRawExpr *column_expr = left_expr->is_column_ref_expr() ? left_expr :right_expr;
      ret = add_column_usage_arg(stmt,
                                 *(static_cast<const ObColumnRefRawExpr *>(column_expr)),
                                 ColumnUsageFlag::RANGE_PREDS);
    } else { /*do nothing*/ }
  } else if (expr->get_expr_type() == T_OP_BTW || expr->get_expr_type() == T_OP_NOT_BTW) {
    // now between has been rewrote to >= and <= at resolver phase
  }
  return ret;
}

int ObOptimizer::add_column_usage_arg(const ObDMLStmt &stmt,
                                      const ObColumnRefRawExpr &column_expr,
                                      int64_t flag)
{
  int ret = OB_SUCCESS;
  const TableItem *table = stmt.get_table_item_by_id(column_expr.get_table_id());
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret), K(column_expr), K(table), K(stmt.get_table_items()));
  } else if (table->is_basic_table()) {
    bool find = false;
    for (int64_t i = 0; i < ctx_.get_column_usage_infos().count(); ++i) {
      if (ctx_.get_column_usage_infos().at(i).table_id_ == table->ref_id_ &&
          ctx_.get_column_usage_infos().at(i).column_id_ == column_expr.get_column_id()) {
        ctx_.get_column_usage_infos().at(i).flags_ |= flag;
      }
    }
    if (!find) {
      ColumnUsageArg col_arg;
      col_arg.table_id_ = table->ref_id_;
      col_arg.column_id_ = column_expr.get_column_id();
      col_arg.flags_ = flag;
      ret = ctx_.get_column_usage_infos().push_back(col_arg);
    }
  }
  return ret;
}

int ObOptimizer::update_column_usage_infos()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_session_info();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session));
  } else {
    MTL_SWITCH(session->get_effective_tenant_id()) {
      ObOptStatMonitorManager *optstat_monitor_mgr = NULL;
      if (OB_ISNULL(optstat_monitor_mgr = MTL(ObOptStatMonitorManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(optstat_monitor_mgr));
      } else if (OB_FAIL(optstat_monitor_mgr->update_local_cache(ctx_.get_column_usage_infos()))) {
        LOG_WARN("failed to update local cache", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptimizer::check_force_default_stat()
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard* schema_guard = ctx_.get_schema_guard();
  ObSQLSessionInfo* session = ctx_.get_session_info();
  ObQueryCtx* query_ctx = ctx_.get_query_ctx();
  bool is_restore = false;
  bool use_default_opt_stat = false;
  bool is_exists_opt = false;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(schema_guard), K(session), K(query_ctx));
  } else if (OB_FAIL(schema_guard->check_tenant_is_restore(session->get_effective_tenant_id(),
                                                           is_restore))) {
    LOG_WARN("fail to check if tenant is restore", K(session->get_effective_tenant_id()));
  } else if (is_restore) {
    // 为避免物理恢复阶段系统表恢复过程中，SQL依赖需要恢复的统计信息表，
    // 对恢复中租户，仅需获取缺省统计信息即可
    ctx_.set_use_default_stat();
  } else if (query_ctx->get_global_hint().has_dbms_stats_hint()) {
    ctx_.set_use_default_stat();
  } else if (OB_FAIL(query_ctx->get_global_hint().opt_params_.get_bool_opt_param(ObOptParamHint::USE_DEFAULT_OPT_STAT,
                                                                                 use_default_opt_stat,
                                                                                 is_exists_opt))) {
    LOG_WARN("fail to check use default opt stat", K(ret));
  } else if (is_exists_opt && use_default_opt_stat) {
    ctx_.set_use_default_stat();
  }
  return ret;
}

int ObOptimizer::init_system_stat()
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  OptSystemStat &meta = ctx_.get_system_stat();
  ObOptStatManager *opt_stat_manager = ctx_.get_opt_stat_manager();
  ObSQLSessionInfo* session = ctx_.get_session_info();
  if (OB_ISNULL(opt_stat_manager) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  } else if (OB_FAIL(opt_stat_manager->check_system_stat_validity(ctx_.get_exec_ctx(),
                                                                  session->get_effective_tenant_id(),
                                                                  is_valid))) {
    LOG_WARN("failed to check system stat is valid", K(ret));
  } else if (!ctx_.use_default_stat() && is_valid) {
    ObOptSystemStat stat;
    if (OB_FAIL(opt_stat_manager->get_system_stat(session->get_effective_tenant_id(), stat))) {
      LOG_WARN("failed to get system stat", K(ret));
    } else {
      meta.set_cpu_speed(stat.get_cpu_speed());
      meta.set_disk_seq_read_speed(stat.get_disk_seq_read_speed());
      meta.set_disk_rnd_read_speed(stat.get_disk_rnd_read_speed());
      meta.set_network_speed(stat.get_network_speed());
    }
  }
  if (OB_SUCC(ret)) {
    //refine default stat
    if (meta.get_cpu_speed() <= 0) {
      meta.set_cpu_speed(DEFAULT_CPU_SPEED);
    }
    if (meta.get_disk_seq_read_speed() <= 0) {
      meta.set_disk_seq_read_speed(DEFAULT_DISK_SEQ_READ_SPEED);
    }
    if (meta.get_disk_rnd_read_speed() <= 0) {
      meta.set_disk_rnd_read_speed(DEFAULT_DISK_RND_READ_SPEED);
    }
    if (meta.get_network_speed() <= 0) {
      meta.set_network_speed(DEFAULT_NETWORK_SPEED);
    }
  }
  return ret;
}

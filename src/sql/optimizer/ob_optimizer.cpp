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
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_explain_note.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "common/ob_smart_call.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "sql/dblink/ob_dblink_utils.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObOptimizer::optimize(ObDMLStmt &stmt, ObLogPlan *&logical_plan)
{
  ObActiveSessionGuard::get_stat().in_sql_optimize_ = true;
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
  ObActiveSessionGuard::get_stat().in_sql_optimize_ = false;
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
  }
  return ret;
}

int ObOptimizer::generate_plan_for_temp_table(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSqlTempTableInfo*> &temp_table_infos = ctx_.get_temp_table_infos();
  if (OB_FAIL(collect_temp_tables(ctx_.get_allocator(), stmt, temp_table_infos))) {
    LOG_WARN("failed to add all temp tables", K(ret));
  } else if (temp_table_infos.empty()) {
    //do nothing
  } else {
    ObSqlTempTableInfo *temp_table_info = NULL;
    ObSelectStmt *ref_query = NULL;
    ObSelectLogPlan *temp_plan = NULL;
    ObLogicalOperator *temp_op = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i++) {
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
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(temp_plan->generate_raw_plan())) {
        LOG_WARN("Failed to generate temp_plan for sub_stmt", K(ret));
      } else if (OB_FAIL(temp_plan->get_candidate_plans().get_best_plan(temp_op))) {
        LOG_WARN("failed to get best plan", K(ret));
      } else if (OB_ISNULL(temp_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        temp_table_info->table_plan_ = temp_op;
        OPT_TRACE_TITLE("end generate plan for temp table ", temp_table_info->table_name_);
      }
    }
  }
  return ret;
}

int ObOptimizer::collect_temp_tables(ObIAllocator &allocator,
                                     ObDMLStmt &stmt,
                                     ObIArray<ObSqlTempTableInfo*> &temp_table_infos)
{
  int ret = OB_SUCCESS;
  ObSqlTempTableInfo *temp_table_info = NULL;
  void *ptr = NULL;
  TableItem *table = NULL;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
    if (OB_ISNULL(child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(collect_temp_tables(allocator, *child_stmts.at(i),
                                                      temp_table_infos)))) {
      LOG_WARN("failed to add all temp tables", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_items().count(); i++) {
    bool find = true;
    if (OB_ISNULL(table = stmt.get_table_items().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!table->is_temp_table()) {
      //do nothing
    } else {
      ObIArray<ObSqlTempTableInfo*> &temp_table_infos_ = ctx_.get_temp_table_infos();
      find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_table_infos_.count(); j++) {
        ObSqlTempTableInfo* info = temp_table_infos_.at(j);
        if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null info", K(ret));
        } else if (info->table_query_ == table->ref_query_) {
          find = true;
          table->ref_id_ = info->temp_table_id_;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(collect_temp_tables(allocator, *table->ref_query_,
                                                        temp_table_infos)))) {
        LOG_WARN("failed to add all temp tables", K(ret));
      } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObSqlTempTableInfo)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        temp_table_info = new (ptr) ObSqlTempTableInfo();
        table->ref_id_ = ctx_.get_query_ctx()->available_tb_id_--;
        temp_table_info->temp_table_id_ = table->ref_id_;
        temp_table_info->table_name_ = table->table_name_;
        temp_table_info->table_query_ = table->ref_query_;
        if (OB_FAIL(temp_table_infos.push_back(temp_table_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
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

/**
* @brief Get the attr from stmt recursively object
*
* @param stmt
* @param max_dop used for table's parallel attribute
* @param max_table_parallel used for parallelism at the statement or object level
* @return int
*/
int ObOptimizer::get_stmt_parallel_info(ObDMLStmt *stmt,
                                        int64_t &max_table_dop,
                                        int64_t &max_table_parallel)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  int64_t cur_max_table_hint = ObGlobalHint::UNSET_PARALLEL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmt", K(ret));
  } else if (OB_FAIL(SMART_CALL(get_stmt_max_table_dop(*stmt, max_table_dop)))) {
    LOG_WARN("failed to get stmt max table dop", K(ret));
  } else if (OB_FAIL(stmt->get_stmt_hint().get_max_table_parallel(*stmt, cur_max_table_hint))) {
    LOG_WARN("failed to get max table parallel", K(ret));
  } else {
    max_table_parallel = std::max(max_table_parallel, cur_max_table_hint);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_stmt_parallel_info(child_stmts.at(i),
                                                           max_table_dop,
                                                           max_table_parallel)))) {
        LOG_WARN("failed to get stmt max table dop", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizer::get_stmt_max_table_dop(ObDMLStmt &stmt,
                                        int64_t &max_table_dop)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
  if (OB_ISNULL(schema_guard) ||
      OB_ISNULL(session_info = ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(schema_guard), K(session_info), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_items().count(); i++) {
      TableItem *table_item = NULL;
      const share::schema::ObTableSchema *table_schema = NULL;
      if (OB_ISNULL(table_item = stmt.get_table_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (table_item->is_function_table() ||
                 table_item->is_json_table() ||
                 table_item->is_link_table() ||
                 table_item->is_fake_cte_table() ||
                 table_item->is_joined_table()) {
      } else if (table_item->is_temp_table()) {
        if (OB_FAIL(SMART_CALL(get_stmt_max_table_dop(*table_item->ref_query_, max_table_dop)))) {
          LOG_WARN("failed to get max table dop from ref query", K(ret));
        }
      } else if (table_item->is_generated_table()) {
      } else {
        uint64_t tids[OB_MAX_INDEX_PER_TABLE + 1];
        int64_t index_count = OB_MAX_INDEX_PER_TABLE + 1;
        const bool with_global_index = true;
        const bool with_mv = false;
        //FIXME: can't get tenant_id from table_id
        const uint64_t tenant_id = session_info->get_effective_tenant_id();
        if (OB_FAIL(schema_guard->get_can_read_index_array(tenant_id,
                                                           table_item->ref_id_,
                                                           tids,
                                                           index_count,
                                                           with_mv,
                                                           with_global_index,
                                                           false /*domain index*/))) {
          LOG_WARN("failed to get can read index", K(tenant_id), K(table_item->ref_id_), K(ret));
        } else if (OB_UNLIKELY(index_count > OB_MAX_INDEX_PER_TABLE + 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid index count", K(table_item->ref_id_), K(index_count), K(ret));
        } else {
          for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
            const uint64_t tid = (i == -1) ? table_item->ref_id_: tids[i]; //with base table
            if (OB_FAIL(schema_guard->get_table_schema(
                session_info->get_effective_tenant_id(), tid, table_schema))) {
              LOG_WARN("failed to get table schema", K(ret));
            } else if (OB_ISNULL(table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret), K(lbt()));
            } else {
              max_table_dop = std::max(max_table_dop, table_schema->get_dop());
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizer::get_session_parallel_info(ObDMLStmt &stmt,
                                           bool use_pdml,
                                           bool &session_enable_parallel,
                                           uint64_t &session_force_parallel_dop)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  session_enable_parallel = false;
  session_force_parallel_dop = 1;
  if (OB_ISNULL(session_info = ctx_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (session_info->is_user_session()) {
    // sys var是依赖于schema的方式实现的，获得最新的sys var需要通过inner SQL的方式，会产生循环依赖
    // 因此inner SQL情况下不考虑系统变量`SYS_VAR__ENABLE_PARALLEL_QUERY`的值
    if (!stmt.is_px_dml_supported_stmt()) {
      if (OB_FAIL(session_info->get_enable_parallel_query(session_enable_parallel))) {
        LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
      } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
        LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
      }
    } else {
      // DML 情况比较特殊：
      // 如果是 PX+DML 模式，那么并发度应该是读取 query 的，
      // 而不是 force dml parallel 的值
      if (use_pdml) {
        if (OB_FAIL(session_info->get_enable_parallel_dml(session_enable_parallel))) {
          LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
        } else if (OB_FAIL(session_info->get_force_parallel_dml_dop(session_force_parallel_dop))) {
          LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
        } else if (session_force_parallel_dop == 1) {
          // 此时是 ENABLE_PARALLEL_DML HINT + FORCE PARALLEL QUERY 的组合
          // session_force_parallel_dop 需要填 Query 的 dop
          if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
            LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
          } else { /*do nothing*/ }
        }
      } else {
        if (OB_FAIL(session_info->get_enable_parallel_query(session_enable_parallel))) {
          LOG_WARN("failed to get sys variable for enable parallel query", K(ret));
        } else if (OB_FAIL(session_info->get_force_parallel_query_dop(session_force_parallel_dop))) {
          LOG_WARN("failed to get sys variable for force parallel query dop", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObOptimizer::check_pdml_enabled(const ObDMLStmt &stmt,
                                    const ObSQLSessionInfo &session,
                                    bool &is_use_pdml)
{
  //
  // 1. pdml: force parallel dml & no DISABLE_PARALLEL_DML hint
  // 2. enable parallel query: parallel hint | sess enable_parallel_query
  //    pdml: enable parallel dml + enable parallel query
  int ret = OB_SUCCESS;
  uint64_t force_pdml_dop = 1;
  ObSqlCtx *sql_ctx = NULL;
  is_use_pdml = false;
  // case 1
  if (OB_ISNULL(ctx_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_query_ctx()));
  } else if (OB_ISNULL(ctx_.get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()));
  } else if (OB_ISNULL(sql_ctx = ctx_.get_exec_ctx()->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.get_exec_ctx()));
  } else if (sql_ctx->multi_stmt_item_.is_batched_multi_stmt()) {
    is_use_pdml = false;
    // 当batch优化打开时，不支持pdml
  } else if (!stmt.is_pdml_supported_stmt()) {
    // pdml 支持新引擎和老引擎
    // 3.1 及之前的版本，老引擎走 dml + px。3.2 起老引擎也能走 pdml
    is_use_pdml = false;
  } else if (stmt::T_INSERT == stmt.get_stmt_type() &&
             !static_cast< const ObInsertStmt &>(stmt).value_from_select()) {
    is_use_pdml = false;
  } else if ((stmt.is_update_stmt() || stmt.is_delete_stmt())
             && static_cast<const ObDelUpdStmt &>(stmt).is_dml_table_from_join()) {
    is_use_pdml = false;
  } else if (OB_FAIL(session.get_force_parallel_dml_dop(force_pdml_dop))) {
    LOG_WARN("fail get force parallel dml session val", K(ret));
  } else if (force_pdml_dop > 1) {
    const ObGlobalHint &global_hint = stmt.get_query_ctx()->get_global_hint();
    if (global_hint.get_pdml_option() == ObPDMLOption::DISABLE) {
      // 如果显式 DISABLE_PARALLEL_DML 则不能开启 pdml
      is_use_pdml = false;
    } else if (global_hint.get_parallel_hint() == ObGlobalHint::UNSET_PARALLEL) {
      // 如果没有显式指定 PARALLEL(X) 则以 force_pdml_dop 为准，开启 pdml
      is_use_pdml = true;
    } else {
      // 如果显式指定了 PARALLEL(X) 则要求必须 parallel > 1 才开启 pdml，否则并行无效
      is_use_pdml = (global_hint.get_parallel_hint() > ObGlobalHint::DEFAULT_PARALLEL);
    }
  } else {
  // case 2
    uint64_t query_dop = 1;
    const ObGlobalHint &global_hint = ctx_.get_query_ctx()->get_global_hint();
    if (OB_FAIL(session.get_force_parallel_query_dop(query_dop))) {
      LOG_WARN("fail get query dop", K(ret));
    } else if (global_hint.get_parallel_hint() > ObGlobalHint::DEFAULT_PARALLEL ||
               query_dop >  ObGlobalHint::DEFAULT_PARALLEL || ctx_.is_online_ddl()) {
      // 当 px 开启，hint 中指定的 parallel > 1，或者 force parallel query > 1，并且有：
      //  - HINT: /*+ ENABLE_PARALLEL_DML */
      //  - 或 set _enable_parallel_dml = 1
      // 时开启 pdml
      if (global_hint.get_pdml_option() != ObPDMLOption::NOT_SPECIFIED) {
        is_use_pdml = (global_hint.get_pdml_option() == ObPDMLOption::ENABLE);
      } else {
        OZ (session.get_enable_parallel_dml(is_use_pdml));
      }
    }
  }
  // check pdml enable sql case
  if (OB_SUCC(ret) && is_use_pdml) {
    OZ(check_pdml_supported_feature(stmt, session, is_use_pdml));
    OZ(check_is_heap_table(stmt));
  }
  LOG_DEBUG("check pdml enable", K(ret), K(is_use_pdml));
  return ret;
}

int ObOptimizer::check_pdml_supported_feature(const ObDMLStmt &stmt,
    const ObSQLSessionInfo &session, bool &is_use_pdml)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
  const ObDelUpdStmt &pdml_stmt = static_cast<const ObDelUpdStmt &>(stmt);
  ObSEArray<const ObDmlTableInfo*, 2> table_infos;
  bool enable_all_pdml_feature = false; // 默认非注入错误情况下，关闭PDML不稳定feature
  bool stmt_has_dblink = false;
  // 目前通过注入错误的方式来打开PDML不稳定功能，用于PDML全部功能的case回归
  // 对应的event注入任何类型的错误，都会打开PDML非稳定功能
  ret = OB_E(EventTable::EN_ENABLE_PDML_ALL_FEATURE) OB_SUCCESS;
  LOG_TRACE("event: check pdml all feature", K(ret));
  if (OB_FAIL(ret)) {
    enable_all_pdml_feature = true;
    ret = OB_SUCCESS;
    ctx_.add_plan_note(PDML_ENABLE_BY_TRACE_EVENT);
  }
  LOG_TRACE("event: check pdml all feature result", K(ret), K(enable_all_pdml_feature));
  // 检查是否开启全部pdml feature：
  // 1. 如果开启，is open = true
  // 2. 如果没有开启，需要依次检查被禁止的不稳定的功能，如果存在被禁止的不稳定功能 is open = false
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the schema guard is null", K(ret));
  } else if (enable_all_pdml_feature) {
    is_use_pdml = true;
  } else if (pdml_stmt.is_ignore()) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_IGNORE);
  } else if (OB_FAIL(pdml_stmt.get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (table_infos.count() != 1) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_JOINED_TABLES);
  } else if (stmt::T_INSERT == stmt.get_stmt_type() &&
             static_cast< const ObInsertStmt &>(stmt).is_insert_up()) {
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_INSERT_UP);
  } else if (OB_FAIL(ObDblinkUtils::has_reverse_link_or_any_dblink(&stmt, stmt_has_dblink, true))) {
    LOG_WARN("failed to find dblink in stmt", K(ret));
  } else if (stmt_has_dblink) {
    is_use_pdml = false;
    ctx_.set_has_dblink(true);
  } else if (!ctx_.has_trigger() && ctx_.contain_user_nested_sql()) {
    //user nested sql can't use PDML plan, force to use DAS plan
    //if online ddl has pl udf, only this way, allow it use PDML plan
    //such as:
    //create table t1(a int primary key, b int as(udf()));
    //create index i1 on t1(b);
    //create index with PL UDF allow to use PDML plan during build index table
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_NESTED_SQL);
  } else if (ctx_.has_trigger() && !ctx_.is_allow_parallel_trigger()) {
    // if sql linked trigger, and trigger do not access package var, sequence, sql stmt etc..,
    // allow it use PDML plan
    is_use_pdml = false;
    ctx_.add_plan_note(PDML_DISABLED_BY_NESTED_SQL);
  } else if (stmt::T_DELETE == stmt.get_stmt_type()) {
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
    } else if (stmt::T_UPDATE == stmt.get_stmt_type()) {
      for (int i = 0; OB_SUCC(ret) && is_use_pdml && i <
          table_infos.at(0)->column_exprs_.count(); i++) {
        ObColumnRefRawExpr* column_expr = table_infos.at(0)->column_exprs_.at(i);
        if (column_expr->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG)) {
          is_use_pdml = false;
          ctx_.add_plan_note(PDML_DISABLED_BY_UPDATE_NOW);
        }
      }
    } else if (stmt::T_MERGE == stmt.get_stmt_type()) {
      bool with_unique_global_idx = false;
      if (OB_FAIL(schema_guard->check_has_global_unique_index(
                  session.get_effective_tenant_id(),
                  main_table_tid, with_unique_global_idx))) {
        LOG_WARN("fail check if table with global unqiue index", K(main_table_tid), K(ret));
      } else if (with_unique_global_idx) {
        is_use_pdml = false;
        ctx_.add_plan_note(PDML_DISABLED_BY_GLOBAL_UK);
      }
    }
  }
  if (OB_SUCC(ret) && !is_use_pdml && ctx_.is_online_ddl()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("a online ddl expect PDML enabled. but it does not!", K(is_use_pdml), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "online ddl without pdml");
  }
  if (OB_SUCC(ret) && ctx_.has_var_assign() && !ctx_.is_var_assign_only_in_root_stmt()) {
    is_use_pdml = false;
  }
  LOG_TRACE("check use all pdml feature", K(ret), K(is_use_pdml), K(ctx_.is_online_ddl()));
  return ret;
}

int ObOptimizer::check_is_heap_table(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx_.get_session_info();
  share::schema::ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
  const share::schema::ObTableSchema *table_schema = NULL;
  const ObDelUpdStmt &pdml_stmt = static_cast<const ObDelUpdStmt &>(stmt);
  ObSEArray<const ObDmlTableInfo*, 1> dml_table_infos;
  // check if the target table is heap table
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session));
  } else if (ctx_.is_online_ddl()) {
    ctx_.set_is_heap_table_ddl(session->get_ddl_info().is_heap_table_ddl());
  } else if (OB_FAIL(pdml_stmt.get_dml_table_infos(dml_table_infos))) {
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
  } else if(OB_NOT_NULL(table_schema) && table_schema->is_heap_table()) {
    ctx_.set_is_pdml_heap_table(true);
  }
  return ret;
}

int ObOptimizer::init_env_info(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t parallel = 1;
  bool use_pdml = false;
  bool session_enable_parallel = false;
  bool has_var_assign = false;
  bool is_var_assign_only_in_root_stmt = false;
  bool stmt_has_dblink = false;
  uint64_t session_force_parallel_dop = 1;
  int64_t max_table_dop = 1;
  int64_t max_table_hint = 1;
  ObDMLStmt *target_stmt = &stmt;
  ObSQLSessionInfo *session = ctx_.get_session_info();
  int64_t link_stmt_count = 0;
  if (OB_ISNULL(target_stmt) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(ctx_.set_is_online_ddl(session->get_ddl_info().is_ddl()))) {
  } else if (OB_FAIL(check_whether_contain_nested_sql(stmt))) {
    LOG_WARN("check whether contain nested sql failed", K(ret));
  } else if (OB_FAIL(get_stmt_parallel_info(target_stmt,
                                            max_table_dop,
                                            max_table_hint))) {
    LOG_WARN("failed to get attributes from stmt", K(ret));
  } else if (OB_FAIL(target_stmt->check_var_assign(has_var_assign,
                                                   is_var_assign_only_in_root_stmt))) {
    LOG_WARN("failed to check has ref assign user var", K(ret));
  } else if (OB_FALSE_IT(ctx_.set_has_var_assign(has_var_assign)) ||
             OB_FALSE_IT(ctx_.set_is_var_assign_only_in_root_stmt(is_var_assign_only_in_root_stmt))) {
    // do nothing
  } else if (OB_FAIL(check_pdml_enabled(*target_stmt, *session, use_pdml))) {
    LOG_WARN("fail to check enable pdml", K(ret));
  } else if (OB_FAIL(get_session_parallel_info(*target_stmt,
                                               use_pdml,
                                               session_enable_parallel,
                                               session_force_parallel_dop))) {
    LOG_WARN("failed to get session parallel info", K(ret));
  } else if (OB_FAIL(calc_link_stmt_count(*target_stmt, link_stmt_count))) {
    LOG_WARN("calc link stmt count failed", K(ret));
  } else {
    ctx_.set_has_multiple_link_stmt(link_stmt_count > 1);
    parallel = ctx_.get_global_hint().get_parallel_hint();
    if (parallel <= 0) {
      parallel = ObGlobalHint::DEFAULT_PARALLEL;
    }
    ctx_.set_parallel(parallel);
    ctx_.set_use_pdml(use_pdml);
    if (ctx_.get_global_hint().get_parallel_hint() != ObGlobalHint::UNSET_PARALLEL) {
      ctx_.set_parallel_rule(PXParallelRule::MANUAL_HINT);
      ctx_.set_parallel(parallel);
      ctx_.add_plan_note(PARALLEL_ENABLED_BY_GLOBAL_HINT, parallel);
    } else if (max_table_hint > 1) {
      ctx_.set_parallel_rule(PXParallelRule::MANUAL_TABLE_HINT);
      ctx_.set_parallel(max_table_hint); // FIXME:nonsense, will use table hint in stmt level
      ctx_.add_plan_note(PARALLEL_ENABLED_BY_TABLE_HINT, max_table_hint);
    } else if (session_force_parallel_dop > 1) {
      ctx_.set_parallel_rule(PXParallelRule::SESSION_FORCE_PARALLEL);
      ctx_.set_parallel(session_force_parallel_dop);
      ctx_.add_plan_note(PARALLEL_ENABLED_BY_SESSION, session_force_parallel_dop);
    } else if (max_table_dop > 1 && session_enable_parallel) {
      ctx_.set_parallel_rule(PXParallelRule::MANUAL_TABLE_DOP);
      ctx_.set_parallel(max_table_dop);
      ctx_.add_plan_note(PARALLEL_ENABLED_BY_TABLE_PROPERTY, max_table_dop);
    } else {
      ctx_.set_parallel_rule(PXParallelRule::USE_PX_DEFAULT);
      ctx_.set_parallel(ObGlobalHint::DEFAULT_PARALLEL);
    }
    //following above rule, but if stmt contain pl_udf, force das, parallel should be 1
    if (ctx_.get_parallel() > 1 && ctx_.has_pl_udf()) {
      ctx_.set_parallel_rule(PXParallelRule::PL_UDF_DAS_FORCE_SERIALIZE);
      ctx_.set_parallel(1);
      ctx_.add_plan_note(PARALLEL_DISABLED_BY_PL_UDF_DAS, 1);
    }
    if (ctx_.has_dblink()) {
      //if stmt contain dblink, force das, parallel should be 1
      ctx_.set_parallel(1);
      ctx_.add_plan_note(PARALLEL_DISABLED_BY_DBLINK, 1);
    }
    bool is_direct_insert = false;
    if (OB_FAIL(ObTableDirectInsertService::check_direct_insert(ctx_, stmt, is_direct_insert))) {
      LOG_WARN("failed to check direct insert", KR(ret));
    } else if (is_direct_insert) {
      ctx_.add_plan_note(DIRECT_MODE_INSERT_INTO_SELECT);
    }
  }

  // init column usage info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extract_column_usage_info(stmt))) {
      LOG_WARN("failed to extract column usage info", K(ret));
    }
  }

  //init cost model
  uint64_t tenant_id = session->get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  bool rowsets_enabled = tenant_config.is_valid() && tenant_config->_rowsets_enabled;
  if (OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
                                      ObOptParamHint::ROWSETS_ENABLED, rowsets_enabled))) {
    LOG_WARN("fail to check rowsets enabled", K(ret));
  } else if (rowsets_enabled) {
    ctx_.set_cost_model_type(ObOptEstCost::VECTOR_MODEL);
  } else {
    ctx_.set_cost_model_type(ObOptEstCost::NORMAL_MODEL);
  }

  // check if stmt has subquery in function table
  if (OB_SUCC(ret)){
    bool has_subquery_in_function_table = false;
    if (OB_FAIL(stmt.check_has_subquery_in_function_table(has_subquery_in_function_table))) {
      LOG_WARN("failed to check stmt has function table", K(ret));
    } else {
      ctx_.set_has_subquery_in_function_table(has_subquery_in_function_table);
    }
  }

  LOG_TRACE("succeed to init optimization env", K(ctx_.use_pdml()), K(ctx_.get_parallel()));
  return ret;
}

int ObOptimizer::check_unique_index(const ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                    bool &has_unique_index) const
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
    if (NULL == column_exprs.at(j)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObRawExpr *target_expr = column_exprs.at(j);
      if (target_expr->is_column_ref_expr()) {
        ObColumnRefRawExpr *column_ref_expr = (ObColumnRefRawExpr*)(target_expr);
        if (column_ref_expr->is_virtual_generated_column() &&
            !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
            column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          has_unique_index = true;
          break;
        }
      }
    }
  }
  return ret;
}

//to check whether contain trigger, foreign key, PL UDF or this sql is triggered by these object
int ObOptimizer::check_whether_contain_nested_sql(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *del_upd_stmt = nullptr;
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
    for (int64_t i = 0; OB_SUCC(ret) && !ctx_.contain_nested_sql() && i < table_infos.count(); ++i) {
      const ObDmlTableInfo* table_info = table_infos.at(i);
      ObSchemaGetterGuard *schema_guard = ctx_.get_schema_guard();
      const ObTableSchema *table_schema = nullptr;
      ObSQLSessionInfo *session = ctx_.get_session_info();
      bool trigger_exists = false;
      bool is_forbid_parallel = false;
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
      if (OB_SUCC(ret) && trigger_exists) {
        if (OB_FAIL(table_schema->is_allow_parallel_of_trigger(*schema_guard, is_forbid_parallel))) {
          LOG_WARN("check allow parallel failed", K(ret));
        } else if (!is_forbid_parallel) {
          ctx_.set_allow_parallel_trigger(true);
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
  const ObSQLSessionInfo *session = ctx_.get_session_info();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ret = ObOptStatMonitorManager::get_instance().update_local_cache(
                session->get_effective_tenant_id(),
                ctx_.get_column_usage_infos());
  }

  return ret;
}

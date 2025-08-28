/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include "ob_table_sql_utils.h"
#include "share/ob_server_struct.h"
#include "share/ob_rpc_struct.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/table/trans/ob_table_trans_ctrl.h"
#include "share/schema/ob_schema_utils.h"
#include "share/table/ob_table_ddl_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::table;

namespace oceanbase
{
namespace table
{

int ObTableSqlUtils::write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global sql proxy is null", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to write sql", K(ret), K(tenant_id), K(sql));
  }

  return ret;
}

int ObTableSqlUtils::read(const uint64_t tenant_id, const char *sql, ObISQLClient::ReadResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global sql proxy is null", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql))) {
    LOG_WARN("fail to read sql", K(ret), K(tenant_id), K(sql));
  }

  return ret;
}

int ObTableSqlUtils::parse(ObIAllocator &allocator,
                           ObSQLSessionInfo &session,
                           const ObString &stmt,
                           ParseResult &result)
{
  int ret = OB_SUCCESS;

  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  ParseMode parse_mode = STD_MODE;
  return parser.parse(stmt, result, parse_mode);
}

int ObTableSqlUtils::parse(ObIAllocator &allocator,
                           ObSQLSessionInfo &session,
                           const ObIArray<ObString> &stmts,
                           ParseResult *results,
                           const int64_t results_count)
{
  int ret = OB_SUCCESS;

  if (stmts.empty() || results_count == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statements or results are empty", K(ret), K(stmts));
  } else if (stmts.count() != results_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statements count should equal to results count", K(ret), K(stmts.count()), K(results_count));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < stmts.count(); i++) {
      const ObString &stmt = stmts.at(i);
      ParseResult &result = results[i];
      if (OB_FAIL(parse(allocator, session, stmt, result))) {
        LOG_WARN("fail to parse", K(ret), K(stmt), K(i));
      }
    }
  }

  return ret;
}

int ObTableSqlUtils::resolve(ObResolver &resolver,
                             const ParseResult &parse_result,
                             ObStmt *&stmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(parse_result.result_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result.result_tree_ is null", K(ret));
  } else if (OB_ISNULL(parse_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result.result_tree_->children_[0] is null", K(ret));
  } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt))) {
    LOG_WARN("fail to resolve", K(ret));
  }

  return ret;
}

int ObTableSqlUtils::resolve(ObResolver &resolver,
                             const ParseResult *parse_results,
                             const int64_t parse_results_count,
                             ObIArray<ObStmt*> &stmts)
{
  int ret = OB_SUCCESS;

  if (parse_results_count == 0 || !stmts.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_results is empty or statements not empty", K(ret), K(stmts), K(parse_results_count));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < parse_results_count; i++) {
      const ParseResult &parse_result = parse_results[i];
      ObStmt *stmt = nullptr;
      if (OB_FAIL(resolve(resolver, parse_result, stmt))) {
        LOG_WARN("fail to resolve", K(ret), K(i));
      } else if (OB_FAIL(stmts.push_back(stmt))) {
        OB_DELETEx(ObStmt, resolver.get_params().allocator_, stmt);
        LOG_WARN("fail to push back stmt", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTableSqlUtils::check_parallel_ddl_schema_in_sync(sql::ObSQLSessionInfo &session,
                                                       const int64_t schema_version,
                                                       const int64_t timeout)
{
  int ret = OB_SUCCESS;

  ObTimeoutCtx timeout_ctx;
  if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout ctx", K(ret), K(timeout));
  } else if (OB_FAIL(ObSchemaUtils::try_check_parallel_ddl_schema_in_sync(
      timeout_ctx, &session, MTL_ID(), schema_version, false/*skip_consensus*/))) {
    LOG_WARN("fail to check paralleld ddl schema in sync", KR(ret), K(schema_version), K(timeout));
  }

  return ret;
}

int ObTableSqlUtils::execute_create_table(sql::ObExecContext &ctx,
                                          const ObString &create_tablegroup_sql,
                                          const ObIArray<ObString> &create_table_sqls,
                                          ObStmt &tablegroup_stmt,
                                          const ObIArray<ObStmt*> &table_stmts,
                                          int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (table_stmts.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statements is null", K(ret));
  } else {
    HEAP_VAR(ObHTableDDLArg, arg) {
      arg.exec_tenant_id_ = MTL_ID();
      arg.is_parallel_ = true;
      ObHTableDDLRes res;
      ObCreateTablegroupStmt &tg_stmt = static_cast<ObCreateTablegroupStmt &>(tablegroup_stmt);
      arg.ddl_type_ = ObHTableDDLType::CREATE_TABLE;
      ObCreateHTableDDLParam param;
      arg.ddl_param_ = &param;
      param.table_group_arg_ = tg_stmt.get_create_tablegroup_arg();
      param.table_group_arg_.ddl_stmt_str_ = create_tablegroup_sql;
      ObSQLSessionInfo *my_session = ctx.get_my_session();
      for (int i = 0; i < table_stmts.count() && OB_SUCC(ret); i++) {
        ObCreateTableStmt *tb_stmt = static_cast<ObCreateTableStmt *>(table_stmts.at(i));
        if (OB_ISNULL(tb_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create table stmt is null", K(ret), K(i));
        } else {
          tb_stmt->get_create_table_arg().ddl_stmt_str_ = create_table_sqls.at(i);
          if (OB_FAIL(param.cf_arg_list_.push_back(tb_stmt->get_create_table_arg()))) {
            LOG_WARN("fail to push back create table arg", K(ret), K(i));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(my_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(ObTableRsExecutor::execute(ctx, arg, res))) {
        LOG_WARN("fail to execute", K(ret), K(arg));
      } else if (OB_FAIL(check_parallel_ddl_schema_in_sync(*my_session, res.schema_version_, timeout))) {
        LOG_WARN("fail to check_parallel_ddl_schema_in_sync", K(ret), K(res), K(timeout));
      }
    }
  }

  return ret;
}

int ObTableSqlUtils::execute_drop_table(sql::ObExecContext &ctx,
                                        const ObString &drop_tablegroup_sql,
                                        ObStmt &tablegroup_stmt,
                                        int64_t timeout)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObHTableDDLArg, arg) {
    arg.exec_tenant_id_ = MTL_ID();
    arg.is_parallel_ = true;
    ObHTableDDLRes res;
    ObDropTablegroupStmt &tg_stmt = static_cast<ObDropTablegroupStmt &>(tablegroup_stmt);
    arg.ddl_type_ = ObHTableDDLType::DROP_TABLE;
    ObDropHTableDDLParam param;
    arg.ddl_param_ = &param;
    param.table_group_arg_ = tg_stmt.get_drop_tablegroup_arg();
    param.table_group_arg_.ddl_stmt_str_ = drop_tablegroup_sql;
    ObSQLSessionInfo *my_session = ctx.get_my_session();
    if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObTableRsExecutor::execute(ctx, arg, res))) {
      LOG_WARN("fail to execute", K(ret), K(arg));
    } else if (OB_FAIL(check_parallel_ddl_schema_in_sync(*my_session, res.schema_version_, timeout))) {
      LOG_WARN("fail to check_parallel_ddl_schema_in_sync", K(ret), K(res), K(timeout));
    }
  }

  return ret;
}

int ObTableSqlUtils::create_table(ObIAllocator &allocator,
                                  ObSchemaGetterGuard &schema_guard,
                                  ObSQLSessionInfo &session,
                                  int64_t timeout,
                                  const ObString &database,
                                  const ObString &create_tablegroup_sql,
                                  const ObIArray<ObString> &create_table_sqls)
{
  int ret = OB_SUCCESS;
  const int64_t table_stmt_count = create_table_sqls.count();
  SMART_VARS_3((ObExecContext, exec_ctx, allocator),
              (ObPhysicalPlanCtx, phy_plan_ctx, allocator),
              (ObSqlCtx, sql_ctx)) {
    sql_ctx.schema_guard_ = &schema_guard;
    exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
    exec_ctx.set_sql_ctx(&sql_ctx);
    exec_ctx.set_my_session(&session);
    exec_ctx.get_task_exec_ctx().schema_service_ = GCTX.schema_service_;
    typedef ObSQLSessionInfo::ExecCtxSessionRegister MyExecCtxSessionRegister;
    MyExecCtxSessionRegister ctx_register(session, &exec_ctx);
    session.set_default_database(database);
    ParseResult tablegroup_parse_result;
    ParseResult *table_parse_results = nullptr; // ParseResult doesn't have method to_string(), so ObSEArray cannot be used
    if (OB_ISNULL(table_parse_results = static_cast<ParseResult*>(allocator.alloc(sizeof(ParseResult) * table_stmt_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ParseResult", K(ret), K(table_stmt_count), K(sizeof(table_parse_results)));
    } else if (FALSE_IT(MEMSET(table_parse_results, 0, sizeof(ParseResult) * table_stmt_count))) {
    } else if (OB_FAIL(parse(allocator, session, create_tablegroup_sql, tablegroup_parse_result))) {
      LOG_WARN("fail to parse", K(ret), K(create_tablegroup_sql));
    } else if (OB_FAIL(parse(allocator, session, create_table_sqls, table_parse_results, table_stmt_count))) {
      LOG_WARN("fail to parse", K(ret), K(create_table_sqls));
    } else {
      SMART_VAR(ObResolverParams, resolver_ctx) {
        ObSchemaChecker schema_checker;
        ObStmtFactory stmt_factory(allocator);
        ObRawExprFactory expr_factory(allocator);
        if (OB_FAIL(schema_checker.init(schema_guard))) {
          LOG_WARN("fail to init schema checker", K(ret));
        } else {
          resolver_ctx.allocator_  = &allocator;
          resolver_ctx.session_info_ = &session;
          resolver_ctx.schema_checker_ = &schema_checker;
          resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
          resolver_ctx.stmt_factory_ = &stmt_factory;
          resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
          resolver_ctx.expr_factory_ = &expr_factory;
          resolver_ctx.is_htable_ = true;
          ObResolver resolver(resolver_ctx);
          ObStmt *tablegroup_stmt = nullptr;
          ObSEArray<ObStmt*, 4> table_stmts;
          table_stmts.set_attr(ObMemAttr(MTL_ID(), "TmpTbStmts"));
          if (OB_FAIL(resolve(resolver, tablegroup_parse_result, tablegroup_stmt))) {
            LOG_WARN("fail to resolve tablegroup", K(ret));
          } else if (OB_FAIL(resolve(resolver, table_parse_results, table_stmt_count, table_stmts))) {
            LOG_WARN("fail to resolve tables", K(ret), K(create_tablegroup_sql), K(create_table_sqls));
          } else if (OB_ISNULL(tablegroup_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablegroup statement is null", K(ret));
          } else if (OB_FAIL(execute_create_table(exec_ctx, create_tablegroup_sql, create_table_sqls,
              *tablegroup_stmt, table_stmts, timeout))) {
            LOG_WARN("fail to execute create table", K(ret), K(create_tablegroup_sql),
              K(create_table_sqls), K(timeout));
          }
          OB_DELETEx(ObStmt, resolver_ctx.allocator_, tablegroup_stmt);
          for (int64_t i = 0; OB_SUCC(ret) && i < table_stmts.count(); ++i) {
            OB_DELETEx(ObStmt, resolver_ctx.allocator_, table_stmts.at(i));
          }
        }
      }
    }
    // exec_ctx expired, reset session cur_exec_ctx
    MyExecCtxSessionRegister ctx_unregister(session, nullptr);
    OB_DELETEx(ParseResult, &allocator, table_parse_results);
  }

  return ret;
}

int ObTableSqlUtils::drop_table(ObIAllocator &allocator,
                                ObSchemaGetterGuard &schema_guard,
                                ObSQLSessionInfo &session,
                                int64_t timeout,
                                const ObString &database,
                                const ObString &tablegroup)
{
  int ret = OB_SUCCESS;
  ObSqlString drop_tg_sql;
  if (OB_FAIL(drop_tg_sql.assign_fmt(DROP_TABLEGROUP_SQL, tablegroup.length(), tablegroup.ptr()))) {
    LOG_WARN("failed to assign drop tablegroup sql", K(ret), K(tablegroup));
  }

  SMART_VARS_3((ObExecContext, exec_ctx, allocator),
               (ObPhysicalPlanCtx, phy_plan_ctx, allocator),
               (ObSqlCtx, sql_ctx)) {
    sql_ctx.schema_guard_ = &schema_guard;
    exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
    exec_ctx.set_sql_ctx(&sql_ctx);
    exec_ctx.set_my_session(&session);
    exec_ctx.get_task_exec_ctx().schema_service_ = GCTX.schema_service_;
    typedef ObSQLSessionInfo::ExecCtxSessionRegister MyExecCtxSessionRegister;
    MyExecCtxSessionRegister ctx_register(session, &exec_ctx);
    session.set_default_database(database);
    ParseResult tablegroup_parse_result;
    ParseResult *table_parse_results = nullptr; // ParseResult doesn't have method to_string(), so ObSEArray cannot be used
    if (OB_ISNULL(table_parse_results = static_cast<ParseResult*>(allocator.alloc(sizeof(ParseResult) * 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ParseResult", K(ret), K(sizeof(table_parse_results)));
    } else if (FALSE_IT(MEMSET(table_parse_results, 0, sizeof(ParseResult)))) {
    } else if (OB_FAIL(parse(allocator, session, drop_tg_sql.string(), tablegroup_parse_result))) {
      LOG_WARN("fail to parse", K(ret), K(drop_tg_sql));
    } else {
      SMART_VAR(ObResolverParams, resolver_ctx) {
        ObSchemaChecker schema_checker;
        ObStmtFactory stmt_factory(allocator);
        ObRawExprFactory expr_factory(allocator);
        if (OB_FAIL(schema_checker.init(schema_guard))) {
          LOG_WARN("fail to init schema checker", K(ret));
        } else {
          resolver_ctx.allocator_  = &allocator;
          resolver_ctx.session_info_ = &session;
          resolver_ctx.schema_checker_ = &schema_checker;
          resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
          resolver_ctx.stmt_factory_ = &stmt_factory;
          resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
          resolver_ctx.expr_factory_ = &expr_factory;
          ObResolver resolver(resolver_ctx);
          ObStmt *tablegroup_stmt = nullptr;
          if (OB_FAIL(resolve(resolver, tablegroup_parse_result, tablegroup_stmt))) {
            LOG_WARN("fail to resolve tablegroup", K(ret));
          } else if (OB_FAIL(execute_drop_table(exec_ctx, drop_tg_sql.string(),
              *tablegroup_stmt, timeout))) {
            LOG_WARN("fail to execute create table", K(ret), K(drop_tg_sql), K(timeout));
          }
          OB_DELETEx(ObStmt, resolver_ctx.allocator_, tablegroup_stmt);
        }
      }
    }
    // exec_ctx expired, reset session cur_exec_ctx
    MyExecCtxSessionRegister ctx_unregister(session, nullptr);
    OB_DELETEx(ParseResult, &allocator, table_parse_results);
  }

  return ret;
}

int ObTableSqlUtils::disable_table(ObIAllocator &allocator,
                                   ObSchemaGetterGuard &schema_guard,
                                   ObSQLSessionInfo &session,
                                   int64_t timeout,
                                   const ObString &database,
                                   const ObString &tablegroup)
{
  int ret = OB_SUCCESS;
  SMART_VARS_3((ObExecContext, exec_ctx, allocator),
               (ObPhysicalPlanCtx, phy_plan_ctx, allocator),
               (ObSqlCtx, sql_ctx)) {
    sql_ctx.schema_guard_ = &schema_guard;
    exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
    exec_ctx.set_sql_ctx(&sql_ctx);
    exec_ctx.get_task_exec_ctx().schema_service_ = GCTX.schema_service_;
    exec_ctx.set_my_session(&session);
    if (OB_FAIL(execute_set_kv_attribute(allocator, exec_ctx, 
        timeout, database, tablegroup, true/* is_disable */))) {
      LOG_WARN("fail to disable table", K(ret), K(timeout), K(database), K(tablegroup));
    }
  }
  return ret;
}

int ObTableSqlUtils::enable_table(ObIAllocator &allocator,
                                  ObSchemaGetterGuard &schema_guard,
                                  ObSQLSessionInfo &session,
                                  int64_t timeout,
                                  const ObString &database,
                                  const ObString &tablegroup)
{
  int ret = OB_SUCCESS;
  SMART_VARS_3((ObExecContext, exec_ctx, allocator),
               (ObPhysicalPlanCtx, phy_plan_ctx, allocator),
               (ObSqlCtx, sql_ctx)) {
    sql_ctx.schema_guard_ = &schema_guard;
    exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
    exec_ctx.set_sql_ctx(&sql_ctx);
    exec_ctx.get_task_exec_ctx().schema_service_ = GCTX.schema_service_;
    exec_ctx.set_my_session(&session);
    if (OB_FAIL(execute_set_kv_attribute(allocator, exec_ctx, 
          timeout, database, tablegroup, false/* is_disable */))) {
      LOG_WARN("fail to enable table", K(ret), K(timeout), K(database), K(tablegroup));
    }
  }
  return ret;
}

int ObTableSqlUtils::execute_set_kv_attribute(ObIAllocator &allocator,
                                              ObExecContext &ctx,
                                              int64_t timeout,
                                              const common::ObString &database,
                                              const common::ObString &tablegroup,
                                              bool is_disable)
{
  int ret = OB_SUCCESS;
  ObHTableDDLArg arg;
  ObHTableDDLRes res;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  ObSetKvAttributeParam param;
  arg.exec_tenant_id_ = MTL_ID();
  param.session_id_ = ObTableTransCtrl::OB_KV_DEFAULT_SESSION_ID;
  if (is_disable) {
    arg.ddl_type_ = ObHTableDDLType::DISABLE_TABLE;
    param.is_disable_ = true;
  } else {
    arg.ddl_type_ = ObHTableDDLType::ENABLE_TABLE;
    param.is_disable_ = false;
  }
  arg.ddl_param_ = &param;
  
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, database, param.database_name_))) {
    LOG_WARN("fail to write tablegroup name", K(ret), K(tablegroup));
  } else if (OB_FAIL(ob_write_string(allocator, tablegroup, param.table_group_name_))) {
    LOG_WARN("fail to write tablegroup name", K(ret), K(tablegroup));
  } else if (OB_FAIL(ObTableRsExecutor::execute(ctx, arg, res))) {
    LOG_WARN("fail executr set kv attribute", K(ret), K(arg));
  } else if (OB_FAIL(check_parallel_ddl_schema_in_sync(*my_session, res.schema_version_, timeout))) {
    LOG_WARN("fail to check_parallel_ddl_schema_in_sync", K(ret), K(res), K(timeout));
  }
  return ret;
}

int ObTableRsExecutor::execute(ObExecContext &ctx, obrpc::ObHTableDDLArg &arg, obrpc::ObHTableDDLRes &res)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  int64_t ori_query_timeout = 0;
  int64_t ori_trx_timeout = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  bool has_get_sys_var = false;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is null", K(ret));
  } else {
    tenant_id = my_session->get_effective_tenant_id();
    ObTenantDDLCountGuard tenant_ddl_guard(tenant_id);
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    // set update sys variable for ddl timeout and set timeout_ts in phy_plan_ctx and worker thread
    my_session->get_query_timeout(ori_query_timeout);
    my_session->get_tx_timeout(ori_trx_timeout);
    has_get_sys_var = true;
    ObObj val;
    val.set_int(GCONF._ob_ddl_timeout);
    if (OB_FAIL(my_session->update_sys_variable(
                        share::SYS_VAR_OB_QUERY_TIMEOUT, val))) {
      LOG_WARN("set sys variable failed", K(ret), K(val.get_int()));
    } else if (OB_FAIL(my_session->update_sys_variable(
                        share::SYS_VAR_OB_TRX_TIMEOUT, val))) {
      LOG_WARN("set sys variable failed", K(ret), K(val.get_int()));
    } else {
      ctx.get_physical_plan_ctx()->set_timeout_timestamp(
          my_session->get_query_start_time() + GCONF._ob_ddl_timeout);
      THIS_WORKER.set_timeout_ts(
          my_session->get_query_start_time() + GCONF._ob_ddl_timeout);
    }
    // reset schema guard in sql ctx and inc ddl count
    if (OB_SUCC(ret)) {
      ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
      if (OB_ISNULL(sql_ctx) || OB_ISNULL(sql_ctx->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_ctx or schema_guard is null", K(ret), KP(sql_ctx));
      } else if (OB_FAIL(sql_ctx->schema_guard_->reset())){
        LOG_WARN("schema_guard reset failed", K(ret));
      } else if (tenant_config.is_valid() && tenant_config->_enable_ddl_worker_isolation) {
        if (OB_FAIL(tenant_ddl_guard.try_inc_ddl_count(tenant_config->cpu_quota_concurrency))) {
          LOG_WARN("fail to inc tenant ddl count", KR(ret), K(tenant_id));
        }
      }
    }
    // do rpc call
    if (OB_SUCC(ret)) {
      arg.is_parallel_ = true;
      ObTaskExecutorCtx *task_exec_ctx = nullptr;
      obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
      if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
        ret = OB_NOT_INIT;
        LOG_WARN("get task executor context failed");
      } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
        ret = OB_NOT_INIT;
        LOG_WARN("get common rpc proxy failed", K(ret));
      } else if (OB_FAIL(common_rpc_proxy->parallel_htable_ddl(arg, res))) {
        LOG_WARN("rpc proxy drop resource_pool failed", K(ret));
      }
    }
  }
  // reset the sys var origin value before update when success or not
  if (has_get_sys_var) {
    int tmp_ret = ret;
    ObObj ori_query_timeout_obj;
    ObObj ori_trx_timeout_obj;
    ori_query_timeout_obj.set_int(ori_query_timeout);
    ori_trx_timeout_obj.set_int(ori_trx_timeout);
    if (OB_ISNULL(my_session)) { // overwrite ret
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_ISNULL(ctx.get_task_exec_ctx().schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service_ is null", K(ret));
    } else if (OB_FAIL(my_session->update_sys_variable(
                      share::SYS_VAR_OB_QUERY_TIMEOUT,
                      ori_query_timeout_obj))) {
      LOG_WARN("set sys variable failed", K(ret), K(ori_query_timeout_obj.get_int()));
    } else if (OB_FAIL(my_session->update_sys_variable(
                      share::SYS_VAR_OB_TRX_TIMEOUT,
                      ori_trx_timeout_obj))) {
      LOG_WARN("set sys variable failed", K(ret), K(ori_trx_timeout_obj.get_int()));
    } else if (OB_FAIL(ctx.get_task_exec_ctx().schema_service_->get_tenant_schema_guard(
                        my_session->get_effective_tenant_id(),
                        *(ctx.get_sql_ctx()->schema_guard_)))) {
      LOG_WARN("failed to get schema guard", K(ret));
    }
    if (OB_FAIL(tmp_ret)) {
      // overwrite ret
      ret = tmp_ret;
    }
  }
  
  return ret;
}

}  // namespace table
}  // namespace oceanbase

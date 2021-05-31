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

#define USING_LOG_PREFIX RS

#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_upgrade_utils.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/ob_schema_revise_executor.h"
#include "rootserver/ob_ddl_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

int64_t ObSchemaReviseTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObSchemaReviseTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = nullptr;
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_size < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(deep_copy_size), K(buf_size));
  } else {
    task = new (buf) ObSchemaReviseTask(*executor_);
  }
  return task;
}

int ObSchemaReviseTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to execute revise schema task", K(start));
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, executor must not be NULL", K(ret));
  } else if (OB_FAIL(executor_->execute())) {
    LOG_WARN("fail to execute revise schema task", K(ret));
  }
  LOG_INFO("[UPGRADE] finish revise schema task", K(ret), "cost_time", ObTimeUtility::current_time() - start);
  return ret;
}

ObSchemaReviseExecutor::ObSchemaReviseExecutor()
    : is_inited_(false),
      is_stopped_(false),
      execute_(false),
      rwlock_(),
      schema_service_(nullptr),
      ddl_service_(nullptr),
      sql_proxy_(nullptr),
      rpc_proxy_(nullptr),
      server_mgr_(nullptr)
{}

int ObSchemaReviseExecutor::init(share::schema::ObMultiVersionSchemaService& schema_service, ObDDLService& ddl_service,
    common::ObMySQLProxy& sql_proxy, obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSchemaReviseExecutor has been inited twice", K(ret));
  } else {
    is_inited_ = true;
    is_stopped_ = false;
    schema_service_ = &schema_service;
    ddl_service_ = &ddl_service;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    server_mgr_ = &server_mgr;
  }
  return ret;
}

int ObSchemaReviseExecutor::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L;            // 100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L;  // 10s
  const int64_t start = ObTimeUtility::current_time();
  {
    SpinWLockGuard guard(rwlock_);
    is_stopped_ = true;
  }
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!execute_) {
      break;
    } else {
      usleep(WAIT_US);
    }
  }
  return ret;
}

int ObSchemaReviseExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  SpinRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", K(ret));
  } else if (enable_ddl) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", K(ret));
  }
  return ret;
}

void ObSchemaReviseExecutor::start()
{
  SpinWLockGuard guard(rwlock_);
  is_stopped_ = false;
}

int ObSchemaReviseExecutor::set_execute_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret), K(is_stopped_), K(execute_));
  } else {
    execute_ = true;
  }
  return ret;
}

int ObSchemaReviseExecutor::execute()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSchemaReviseExecutor has not been inited", K(ret));
  } else if (OB_FAIL(set_execute_mark())) {
    LOG_WARN("fail to set execute mark", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    bool can_run_job = false;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_SCHEMA_REVISE;
    if (OB_FAIL(ObUpgradeUtils::can_run_upgrade_job(job_type, can_run_job))) {
      LOG_WARN("fail to check if can run upgrade job now", K(ret), K(job_type));
    } else if (!can_run_job) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("no job exist or success job exist", K(ret));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is is invalid", K(ret), K(job_id));
    } else if (OB_FAIL(do_schema_revise())) {
      LOG_WARN("fail to do schema revise job", K(ret));
    }
    if (job_id > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    execute_ = false;
  }
  return ret;
}

int ObSchemaReviseExecutor::check_standby_dml_sync(const obrpc::ObClusterTenantStats& primary_tenant_stats,
    const obrpc::ObClusterTenantStats& standby_tenant_stats, bool& schema_sync)
{
  int ret = OB_SUCCESS;
  int64_t primary_tenant_cnt = primary_tenant_stats.tenant_stats_array_.count();
  int64_t standby_tenant_cnt = standby_tenant_stats.tenant_stats_array_.count();

  if (primary_tenant_cnt <= 0 || standby_tenant_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (primary_tenant_cnt != standby_tenant_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "primary_tenant_cnt is not equal to standby_tenant_cnt", K(ret), K(primary_tenant_cnt), K(standby_tenant_cnt));
  } else {
    schema_sync = true;
    for (int64_t i = 0; schema_sync && OB_SUCC(ret) && i < primary_tenant_cnt; i++) {
      const TenantIdAndStats& primary = primary_tenant_stats.tenant_stats_array_.at(i);
      const TenantIdAndStats& standby = primary_tenant_stats.tenant_stats_array_.at(i);
      if (OB_SYS_TENANT_ID == primary.tenant_id_) {
        // check normal tenant only
        continue;
      } else if (primary.tenant_id_ != standby.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary.tenant_id_ is not equal to stanby.tenant_id_",
            K(ret),
            K(primary.tenant_id_),
            K(standby.tenant_id_));
      } else {
        schema_sync = (primary.min_sys_table_scn_ <= standby.min_sys_table_scn_);
        LOG_INFO("check if tenant schema is sync",
            K(ret),
            K(primary.min_sys_table_scn_),
            K(standby.min_sys_table_scn_),
            K(schema_sync));
      }
    }
  }

  return ret;
}

int ObSchemaReviseExecutor::check_schema_sync()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (enable_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl));
  } else {
    const int64_t WAIT_US = 1000 * 1000L;  // 1 second
    bool is_sync = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stop", K(ret));
      } else if (OB_FAIL(ObUpgradeUtils::check_schema_sync(is_sync))) {
        LOG_WARN("fail to check schema sync", K(ret));
      } else if (is_sync) {
        break;
      } else {
        LOG_INFO("schema not sync, should wait", K(ret));
        usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaReviseExecutor::compute_check_cst_count_by_tenant(const uint64_t tenant_id, int64_t& check_constraint_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(sql.append_fmt("SELECT count(*) as check_constraint_count FROM"))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(sql.append_fmt(" %s WHERE CONSTRAINT_TYPE = %d AND TENANT_ID = %lu",
                   OB_ALL_CONSTRAINT_TNAME,
                   CONSTRAINT_TYPE_CHECK,
                   tenant_id))) {
      LOG_WARN("failed to append sql", K(ret), K(OB_ALL_CONSTRAINT_TNAME), K(tenant_id));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else {
      if (OB_SUCC(ret) && OB_SUCC(ret = result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, "check_constraint_count", check_constraint_count, int64_t);
      }
      if (OB_SUCC(ret)) {
        if (OB_SUCC(ret = result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql result should be only one row", K(ret), K(sql));
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("get sql result", K(ret), K(sql));
        } else {
          LOG_WARN("iter quit", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}

// Generate a constraint schema with constraint column information
int ObSchemaReviseExecutor::generate_constraint_schema(
    const uint64_t tenant_id, ObIAllocator& allocator, ObSchemaGetterGuard& schema_guard, ObSArray<ObConstraint>& csts)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  ObSQLSessionInfo session;
  ObRawExpr* expr = NULL;
  ObSqlString sql;

  if (OB_FAIL(
          session.init(0 /*default session version*/, 0 /*default session id*/, 0 /*default proxy id*/, &allocator))) {
    LOG_WARN("init session failed", K(ret));
  } else if (OB_FAIL(session.load_default_sys_variable(false, false))) {
    LOG_WARN("session load default system variable failed", K(ret));
  } else {
    // Generate a check constraint object with complete column information
    ObResolverParams params;
    params.expr_factory_ = &expr_factory;
    params.allocator_ = &allocator;
    params.session_info_ = &session;
    sql.reset();
    // Used to query the smallest row of schema version in each check constraint that has not been deleted from
    // __all_constraint_history
    const static char* SELECT_CST_INFO_SQL =
        "SELECT table_id, constraint_id, check_expr, min(schema_version) as schema_version "
        "FROM %s "
        "WHERE constraint_type = %d or constraint_type is null "
        "GROUP BY tenant_id, table_id, constraint_id HAVING SUM(is_deleted) = 0";

    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(sql.append_fmt(SELECT_CST_INFO_SQL, OB_ALL_CONSTRAINT_HISTORY_TNAME, CONSTRAINT_TYPE_CHECK))) {
        LOG_WARN("failed to append sql", K(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else {
        oceanbase::lib::Worker::CompatMode compat_mode;
        while (OB_SUCC(ret) && OB_SUCC(ret = result->next())) {
          ObConstraint cst;
          const ParseNode* node = NULL;
          ObRawExpr* check_constraint_expr = NULL;
          const ObTableSchema* table_schema = NULL;
          cst.set_tenant_id(tenant_id);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(*result, table_id, cst, tenant_id);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, constraint_id, cst, uint64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, schema_version, cst, int64_t);
          EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL(*result, check_expr, cst, OB_MAX_CONSTRAINT_EXPR_LENGTH);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
              LOG_WARN("get tenant compat mode failed", K(ret));
            }
            share::CompatModeGuard g(compat_mode);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(check_stop())) {
              LOG_WARN("executor is stopped", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                           cst.get_check_expr_str(), *params.allocator_, node))) {
              LOG_WARN("parse expr node from string failed", K(ret), K(cst));
            } else if (OB_FAIL(schema_guard.get_table_schema(cst.get_table_id(), table_schema))) {
              LOG_WARN("get table schema failed", K(ret));
            } else if (OB_ISNULL(table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table_schema is null", K(ret));
            } else if (OB_FAIL(ObResolverUtils::resolve_check_constraint_expr(
                           params, node, *table_schema, cst, check_constraint_expr))) {
              LOG_WARN("resolve check constraint expr", K(ret), K(cst));
            } else if (OB_FAIL(csts.push_back(cst))) {
              LOG_WARN("push back cst to csts failed", K(ret), K(cst));
            }
          }
        }
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get sql result, iter quit", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
          LOG_INFO("get sql result", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObSchemaReviseExecutor::replace_constraint_column_info_in_inner_table(
    const uint64_t tenant_id, ObSArray<ObConstraint>& csts, bool is_history)
{
  int ret = OB_SUCCESS;

  // Batch replace, replace 100 check constraints at a time
  int64_t cst_count = csts.count();
  int64_t i = 0;
  // First history and then non-history to ensure re-entry
  share::ObDMLSqlSplicer dml;
  share::ObDMLSqlSplicer history_dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t is_deleted = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", K(ret));
  } else {
    const int64_t batch_replace_num = 100;
    const char* table_name;
    if (is_history) {
      table_name = OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME;
    } else {
      table_name = OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME;
    }
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < cst_count; ++i) {
      for (ObConstraint::const_cst_col_iterator iter = csts.at(i).cst_col_begin();
           OB_SUCC(ret) && (iter != csts.at(i).cst_col_end());
           ++iter) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor is stopped", K(ret));
        } else if (OB_FAIL(dml.add_pk_column("tenant_id",
                       ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, csts.at(i).get_tenant_id()))) ||
                   OB_FAIL(dml.add_pk_column(
                       "table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, csts.at(i).get_table_id()))) ||
                   OB_FAIL(dml.add_pk_column("constraint_id", csts.at(i).get_constraint_id())) ||
                   OB_FAIL(dml.add_pk_column("column_id", *iter)) ||
                   OB_FAIL(dml.add_column("schema_version", csts.at(i).get_schema_version())) ||
                   OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("dml add constraint column failed", K(ret));
        } else if (is_history && OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add is_deleted column failed", K(ret));
        } else if (dml.finish_row()) {
          LOG_WARN("fail to finish row", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == ((i + 1) % batch_replace_num)) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor is stopped", K(ret));
        } else if (OB_FAIL(dml.splice_batch_replace_sql_without_plancache(table_name, sql))) {
          LOG_WARN("splice sql failed", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else {
          dml.reset();
          sql.reset();
          LOG_INFO("[UPGRADE] batch replace rows", K(ret), K(tenant_id), K(table_name), K(sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 != (i % batch_replace_num)) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor is stopped", K(ret));
        } else if (OB_FAIL(dml.splice_batch_replace_sql_without_plancache(table_name, sql))) {
          LOG_WARN("splice sql failed", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else {
          dml.reset();
          LOG_INFO("[UPGRADE] batch replace rows", K(ret), K(tenant_id), K(table_name), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObSchemaReviseExecutor::do_inner_table_revise_by_tenant(
    const uint64_t tenant_id, ObIAllocator& allocator, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t check_constraint_count = 0;
  ObSArray<ObConstraint> csts;

  if (OB_FAIL(compute_check_cst_count_by_tenant(tenant_id, check_constraint_count))) {
    LOG_WARN("compute check cst count in tenant failed", K(ret), K(tenant_id), K(check_constraint_count));
  } else if (OB_FAIL(csts.reserve(check_constraint_count))) {
    LOG_WARN("reserve failed for csts", K(ret), K(check_constraint_count));
  } else if (OB_FAIL(generate_constraint_schema(tenant_id, allocator, schema_guard, csts))) {
    LOG_WARN("generate constraint schema failed", K(ret), K(tenant_id), K(csts));
  } else if (OB_FAIL(replace_constraint_column_info_in_inner_table(tenant_id, csts, true))) {
    LOG_WARN("replace constraint_column_info in __all_tenant_constraint_column failed", K(ret), K(tenant_id), K(csts));
  } else if (OB_FAIL(replace_constraint_column_info_in_inner_table(tenant_id, csts, false))) {
    LOG_WARN("replace constraint_column_info in __all_tenant_constraint_column_history failed",
        K(ret),
        K(tenant_id),
        K(csts));
  } else {
    LOG_INFO("[UPGRRADE] do inner table revise for one tenant finish", K(ret), K(tenant_id));
  }

  return ret;
}

// Supplementary data:
// If it is the main database:
// need to go to each tenant to supplement the data;
// if it is the standby database:
// need to process the process of sys tenant supplement data,
// and other tenants will synchronize to the standby database according to the modification of the main database.
int ObSchemaReviseExecutor::do_inner_table_revise()
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(
                 ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    bool is_standby_cluster = GCTX.is_standby_cluster();
    for (int64_t i = tenant_ids.size() - 1; (i >= 0) && OB_SUCC(ret); --i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", K(ret));
      } else if (is_standby_cluster && (OB_SYS_TENANT_ID == tenant_id)) {
        LOG_INFO("skip normal tenant in standby cluster", K(ret), K(tenant_id));
      } else if (OB_FAIL(check_schema_sync())) {
        LOG_WARN("fail to check schema sync", K(ret));
      } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(do_inner_table_revise_by_tenant(tenant_id, allocator, schema_guard))) {
        LOG_WARN("fail to do inner table revise", K(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("[UPGRRADE] do inner table revise for all tenants finish", K(ret));
    }
  }

  return ret;
}

int ObSchemaReviseExecutor::flush_schema_kv_cache()
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT = 100 * 1000 * 1000L;  // 100s for handle flush schema kv cache

  if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret));
  } else {
    int tmp_ret;
    int64_t retry_times_left = 10;
    obrpc::ObFlushCacheArg arg;
    arg.cache_type_ = CACHE_TYPE_SCHEMA;
    ObSEArray<ObAddr, SEARRAY_INIT_NUM> active_server_list;
    ObSEArray<ObAddr, SEARRAY_INIT_NUM> inactive_server_list;
    if (OB_FAIL(server_mgr_->get_servers_by_status(active_server_list, inactive_server_list))) {
      LOG_WARN("fail to get all servers", K(ret));
    } else if (inactive_server_list.count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("cannot do this job when exist a server is inactive", K(ret), K(inactive_server_list));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < active_server_list.count()); ++i) {
        retry_times_left = 10;
        while (OB_SUCC(ret) && (retry_times_left > 0)) {
          if (OB_FAIL(rpc_proxy_->to(active_server_list.at(i)).timeout(TIMEOUT).flush_cache(arg))) {
            if (OB_TIMEOUT == ret || OB_WAITQUEUE_TIMEOUT == ret) {
              LOG_INFO("flush schema kv cache timeout, need retry",
                  K(ret),
                  K(retry_times_left),
                  K(active_server_list.at(i)));
              --retry_times_left;
              tmp_ret = ret;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to flush schema kv cache", K(ret), K(active_server_list.at(i)));
            }
          } else {
            LOG_INFO("[UPGRRADE] flush schema kv cache of observer finish", K(ret), K(active_server_list.at(i)));
            break;
          }
        }
        if (OB_SUCC(ret) && (0 == retry_times_left)) {
          ret = tmp_ret;
          LOG_WARN("fail to flush schema kv cache", K(ret), K(active_server_list.at(i)));
        }
      }
    }
  }

  return ret;
}

int ObSchemaReviseExecutor::do_schema_revise()
{
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;

  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] execute job schema_revise start", K(start));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (enable_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("split schema while enable_ddl is on not allowed", K(ret), K(enable_ddl));
  } else if (OB_FAIL(do_inner_table_revise())) {
    LOG_WARN("do inner table revise failed", K(ret));
  } else if (OB_FAIL(flush_schema_kv_cache())) {
    LOG_WARN("flush schema kv cache failed", K(ret));
  }
  LOG_INFO("[UPGRADE] execute job schema_revise finish", K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObSchemaReviseExecutor::can_execute()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("status not matched", K(ret), "stopped", is_stopped_ ? "true" : "false");
  }
  return ret;
}

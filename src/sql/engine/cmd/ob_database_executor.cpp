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

#include "sql/engine/cmd/ob_database_executor.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/ddl/ob_alter_database_stmt.h"
#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObDropTableBatchScheduler::ProxyCtx::ProxyCtx()
  : allocator_("DropNonAtoProxy"),
    proxy_(nullptr)
{}

ObDropTableBatchScheduler::ProxyCtx::~ProxyCtx()
{
  reset();
}

void ObDropTableBatchScheduler::ProxyCtx::reset()
{
  using DropProxy = rootserver::ObNonAtomicDropTableInDBProxy;
  OB_DELETEx(DropProxy, &allocator_, proxy_);
  allocator_.reset();
}

int ObDropTableBatchScheduler::ProxyCtx::init(obrpc::ObCommonRpcProxy &base_rpc_proxy, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  reset();
  allocator_.set_tenant_id(tenant_id);
  proxy_ = OB_NEWx(rootserver::ObNonAtomicDropTableInDBProxy,
                   &allocator_,
                   base_rpc_proxy,
                   &obrpc::ObCommonRpcProxy::non_atomic_drop_table_in_database);
  if (OB_ISNULL(proxy_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "fail to alloc drop table proxy", KR(ret));
  }
  return ret;
}

void ObDropTableBatchScheduler::ProxyCtx::reset_round()
{
  if (OB_NOT_NULL(proxy_)) {
    proxy_->reuse();
  }
}

ObDropTableBatchScheduler::ObDropTableBatchScheduler()
  : tenant_id_(OB_INVALID_TENANT_ID),
    table_count_(0),
    next_table_idx_(0),
    database_name_(),
    table_names_(),
    rpc_proxy_(nullptr),
    proxy_ctxs_(),
    allocator_("DropNonAto")
{
}

int ObDropTableBatchScheduler::init(const uint64_t tenant_id,
    obrpc::ObCommonRpcProxy &rpc_proxy,
    const ObString &database_name,
    const common::ObIArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "database name is empty", KR(ret), K(database_name));
  } else {
    tenant_id_ = tenant_id;
    rpc_proxy_ = &rpc_proxy;
    database_name_.reset();
    table_names_.reset();
    next_table_idx_ = 0;
    table_count_ = 0;
    proxy_ctxs_.reset();
    allocator_.reset();
    allocator_.set_tenant_id(tenant_id_);
    if (OB_FAIL(ob_write_string(allocator_, database_name, database_name_))) {
      SQL_ENG_LOG(WARN, "fail to deep copy database name", KR(ret), K(database_name));
    } else if (OB_FAIL(table_names_.reserve(table_names.count()))) {
      SQL_ENG_LOG(WARN, "fail to reserve table descs", KR(ret), K(table_names.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_names.count(); ++i) {
      ObString tmp_name;
      if (OB_FAIL(ob_write_string(allocator_, table_names.at(i), tmp_name))) {
        SQL_ENG_LOG(WARN, "fail to deep copy table name", KR(ret), K(table_names.at(i)));
      } else if (OB_FAIL(table_names_.push_back(tmp_name))) {
        SQL_ENG_LOG(WARN, "fail to push back table name", KR(ret), K(tmp_name));
      }
    }
    if (OB_SUCC(ret)) {
      table_count_ = table_names_.count();
    }
  }
  return ret;
}

ObDropTableBatchScheduler::~ObDropTableBatchScheduler()
{
  for (int64_t i = 0; i < proxy_ctxs_.count(); ++i) {
    OB_DELETEx(ProxyCtx, &allocator_, proxy_ctxs_.at(i));
  }
  proxy_ctxs_.reset();
  allocator_.reset();
}

int ObDropTableBatchScheduler::execute()
{
  int ret = OB_SUCCESS;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT);
  int64_t parallel_count = 0;
  double min_cpu = 0.0, max_cpu = 0.0;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "omt is NULL", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id_, min_cpu, max_cpu))) {
    SQL_ENG_LOG(WARN, "fail to get tenant cpu", KR(ret));
  } else {
    // parallel ddl thread max is 24
    parallel_count = min(static_cast<int64_t>(ceil(min_cpu) * 2), 48);
  }
  if (FAILEDx(proxy_ctxs_.reserve(parallel_count))) {
    SQL_ENG_LOG(WARN, "fail to reserve proxy ctxs", KR(ret), K(parallel_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parallel_count; ++i) {
      ProxyCtx *ctx = OB_NEWx(ProxyCtx, &allocator_);
      if (OB_ISNULL(ctx)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "fail to alloc proxy ctx", KR(ret), K(i));
      } else if (OB_FAIL(ctx->init(*rpc_proxy_, tenant_id_))) {
        SQL_ENG_LOG(WARN, "fail to init proxy ctx", KR(ret), K(i));
      } else if (OB_FAIL(proxy_ctxs_.push_back(ctx))) {
        SQL_ENG_LOG(WARN, "fail to push back proxy ctx", KR(ret), K(i));
      }
      if (OB_FAIL(ret)) {
        OB_DELETEx(ProxyCtx, &allocator_, ctx);
      }
    }
  }

  SQL_ENG_LOG(INFO, "execute drop table batch scheduler", KR(ret), K(parallel_count), K(table_count_));
  while (OB_SUCC(ret) && (next_table_idx_ < table_count_ || has_pending_proxy())) {
    if (THIS_WORKER.get_timeout_remain() <= 0) {
      ret = OB_TIMEOUT;
      SQL_ENG_LOG(WARN, "this worker is timeout", KR(ret), K(THIS_WORKER.get_timeout_remain()));
    } else {
      bool sent_rpc = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < proxy_ctxs_.count(); ++i) {
        ProxyCtx &ctx = *proxy_ctxs_.at(i);
        // if rpc are finished, wait for the result
        if (has_finished_rpc(ctx)) {
          if (OB_FAIL(wait_proxy(ctx))) {
            SQL_ENG_LOG(WARN, "fail to wait proxy", KR(ret), K(i));
          }
        }
        // if the proxy is finished and there are still tables to process, send the next batch
        if (OB_SUCC(ret) && !has_sent_rpc(ctx) && next_table_idx_ < table_count_) {
          if (OB_FAIL(send_one_rpc(ctx))) {
            SQL_ENG_LOG(WARN, "fail to send drop table rpc", KR(ret));
          } else {
            sent_rpc = true;
          }
        }
      }
      if (OB_SUCC(ret) && !sent_rpc && has_pending_proxy()) {
        ob_usleep(5_ms);
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = cleanup_pending_proxy();
    if (OB_SUCCESS != tmp_ret) {
      SQL_ENG_LOG(WARN, "cleanup pending proxy failed", KR(ret), KR(tmp_ret));
    }
  }
  if (OB_NOT_NULL(rpc_proxy_)) {
    SERVER_EVENT_ADD("ddl", "non atomic drop table in database execute finish",
      "tenant_id", tenant_id_,
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", rpc_proxy_->get_server(),
      "database_name", database_name_,
      "table_count", table_count_);
  }
  return ret;
}

int ObDropTableBatchScheduler::wait_proxy(ProxyCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (has_sent_rpc(ctx)) {
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx.proxy_->wait_all(return_code_array))) {
      SQL_ENG_LOG(WARN, "wait result failed", KR(tmp_ret), KR(ret));
      ret = tmp_ret;
    }
    if (OB_SUCC(ret)) {
      if (return_code_array.count() != ctx.proxy_->get_args().count()) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "return code count not match", KR(ret), K(return_code_array.count()));
      }
    }
    // ignore return code result to tolerate the error, drop database will hanlde the error in its own way.
  }
  ctx.reset_round();
  return ret;
}

int ObDropTableBatchScheduler::send_one_rpc(ProxyCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  int64_t carried_tables_count = 0;
  if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(ctx.proxy_)) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "rpc proxy is null", KR(ret), KP(rpc_proxy_), KP(ctx.proxy_));
  } else if (next_table_idx_ >= table_count_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "next table idx is greater than table count", KR(ret), K(next_table_idx_), K(table_count_));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "rs mgr is NULL", KR(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    SQL_ENG_LOG(WARN, "failed to get rootservice address", KR(ret));
  } else {
    ObDropTableArg arg;
    arg.tenant_id_ = tenant_id_;
    arg.exec_tenant_id_ = tenant_id_;
    // this parameter only distinguish drop table or drop view, not used in drop table type
    arg.table_type_ = USER_TABLE;
    arg.foreign_key_checks_ = false;
    for (; OB_SUCC(ret) && next_table_idx_ < table_count_ && carried_tables_count < MAX_TABLES_PER_RPC;
       ++carried_tables_count, ++next_table_idx_) {
      ObTableItem item;
      item.database_name_ = database_name_;
      item.table_name_ = table_names_.at(next_table_idx_);
      if (OB_FAIL(arg.tables_.push_back(item))) {
        SQL_ENG_LOG(WARN, "fail to push back table item", KR(ret), K(item));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t timeout = THIS_WORKER.get_timeout_remain();
      if (timeout <= 0) {
        ret = OB_TIMEOUT;
        SQL_ENG_LOG(WARN, "already timeout", KR(ret), K(timeout));
      } else if (arg.tables_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "no table carried in drop table rpc", KR(ret));
      } else if (OB_FAIL(ctx.proxy_->call(rs_addr, timeout, OB_SYS_TENANT_ID, arg))) {
        SQL_ENG_LOG(WARN, "fail to send drop table batch rpc", KR(ret), K(arg.tables_.count()));
      }
    }
  }
  return ret;
}

int ObDropTableBatchScheduler::cleanup_pending_proxy()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < proxy_ctxs_.count(); ++i) {
    ProxyCtx *ctx_ptr = proxy_ctxs_.at(i);
    if (OB_ISNULL(ctx_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "proxy ctx is null", KR(ret), K(i));
    } else {
      ProxyCtx &ctx = *ctx_ptr;
      if (has_pending_rpc(ctx)) {
        int tmp_ret = wait_proxy(ctx);
        if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
          ret = tmp_ret; // preserve first failure
        }
        SQL_ENG_LOG(INFO, "cleanup pending proxy", KR(ret), K(tmp_ret), K(i));
      }
    }
  }
  return ret;
}

bool ObDropTableBatchScheduler::has_pending_proxy() const
{
  bool pending = false;
  for (int64_t i = 0; !pending && i < proxy_ctxs_.count(); ++i) {
    if (OB_NOT_NULL(proxy_ctxs_.at(i))) {
      pending = has_pending_rpc(*proxy_ctxs_.at(i)) > 0;
    }
  }
  return pending;
}

bool ObDropTableBatchScheduler::has_pending_rpc(const ProxyCtx &ctx) const
{
  bool has_pending_rpc = false;
  if (OB_NOT_NULL(ctx.proxy_)) {
    has_pending_rpc = ctx.proxy_->get_args().count() - ctx.proxy_->get_response_count() > 0;
  }
  return has_pending_rpc;
}

bool ObDropTableBatchScheduler::has_sent_rpc(const ProxyCtx &ctx) const
{
  bool sent = false;
  if (OB_NOT_NULL(ctx.proxy_)) {
    sent = ctx.proxy_->get_args().count() > 0;
  }
  return sent;
}

bool ObDropTableBatchScheduler::has_finished_rpc(const ProxyCtx &ctx) const
{
  bool finished = false;
  if (OB_NOT_NULL(ctx.proxy_)) {
    const int64_t arg_cnt = ctx.proxy_->get_args().count();
    const int64_t resp_cnt = ctx.proxy_->get_response_count();
    finished = arg_cnt > 0 && arg_cnt == resp_cnt;
  }
  return finished;
}

ObCreateDatabaseExecutor::ObCreateDatabaseExecutor()
{
}

ObCreateDatabaseExecutor::~ObCreateDatabaseExecutor()
{
}

int ObCreateDatabaseExecutor::execute(ObExecContext &ctx, ObCreateDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateDatabaseArg &create_database_arg = stmt.get_create_database_arg();
  obrpc::ObCreateDatabaseArg &tmp_arg = const_cast<obrpc::ObCreateDatabaseArg&>(create_database_arg);
  ObString first_stmt;
  obrpc::UInt64 database_id(0);
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
   } else if (OB_ISNULL(common_rpc_proxy)
              || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx), K(common_rpc_proxy));
  } else {
    //为什么create database的协议需要返回database_id，暂时没有用上。
    if (OB_FAIL(common_rpc_proxy->create_database(create_database_arg, database_id))) {
      SQL_ENG_LOG(WARN, "rpc proxy create table failed", K(ret));
    } else {
      ctx.get_physical_plan_ctx()->set_affected_rows(1);
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "create database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", database_id,
      "schema_version", create_database_arg.database_schema_.get_schema_version());
  }
  SQL_ENG_LOG(INFO, "finish execute create database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

/////////////////////
ObUseDatabaseExecutor::ObUseDatabaseExecutor()
{
}

ObUseDatabaseExecutor::~ObUseDatabaseExecutor()
{
}

int ObUseDatabaseExecutor::execute(ObExecContext &ctx, ObUseDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "session is NULL");
  } else {
    const bool is_oracle_mode = lib::is_oracle_mode();
    const bool is_oceanbase_db = is_oceanbase_sys_database_id(stmt.get_db_id());
    if (session->is_tenant_changed() && !is_oceanbase_db) {
      ret = OB_OP_NOT_ALLOW;
      SQL_ENG_LOG(WARN, "tenant changed, access non oceanbase database not allowed", K(ret), K(stmt),
                  "login_tenant_id", session->get_login_tenant_id(),
                  "effective_tenant_id", session->get_effective_tenant_id());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant changed, access non oceanbase database");
    } else if (is_oracle_mode && is_oceanbase_db) {
      ret = OB_OP_NOT_ALLOW;
      SQL_ENG_LOG(WARN, "cannot access oceanbase database on tenant with oracle compatiblity", K(ret),
                  "effective_tenant_id", session->get_effective_tenant_id());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "access oceanbase database");
    } else {
      ObCollationType db_coll_type = ObCharset::collation_type(stmt.get_db_collation());
      ObObj catalog_id_obj;
      catalog_id_obj.set_uint64(stmt.get_catalog_id());
      if (OB_UNLIKELY(CS_TYPE_INVALID == db_coll_type)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(ERROR, "invalid collation", K(ret), K(stmt.get_db_name()), K(stmt.get_db_collation()));
      } else if (OB_FAIL(session->update_sys_variable(ObSysVarClassType::SYS_VAR__CURRENT_DEFAULT_CATALOG, catalog_id_obj))) {
        SQL_ENG_LOG(WARN, "set catalog id session variable failed", K(ret));
      } else if (OB_FAIL(session->set_default_database(stmt.get_db_name(), db_coll_type))) {
        SQL_ENG_LOG(WARN, "fail to set default database", K(ret), K(stmt.get_db_name()), K(stmt.get_db_collation()), K(db_coll_type));
      } else {
        session->set_db_priv_set(stmt.get_db_priv_set());
        SQL_ENG_LOG(INFO, "use default database", "db", stmt.get_db_name());
        session->set_database_id(stmt.get_db_id());
      }
    }
  }
  return ret;
}

//////////////////
ObAlterDatabaseExecutor::ObAlterDatabaseExecutor()
{
}

ObAlterDatabaseExecutor::~ObAlterDatabaseExecutor()
{
}

int ObAlterDatabaseExecutor::execute(ObExecContext &ctx, ObAlterDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObAlterDatabaseArg &alter_database_arg = stmt.get_alter_database_arg();
  obrpc::ObAlterDatabaseArg &tmp_arg = const_cast<obrpc::ObAlterDatabaseArg&>(alter_database_arg);
  ObString first_stmt;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "session is NULL");
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->alter_database(alter_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy alter table failed", K(ret));
  } else if (! stmt.get_alter_option_set().has_member(obrpc::ObAlterDatabaseArg::COLLATION_TYPE)) {
    // do nothing
  } else if (0 == stmt.get_database_name().compare(session->get_database_name())) {
    const int64_t db_coll = static_cast<int64_t>(stmt.get_collation_type());
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_CHARACTER_SET_DATABASE, db_coll))) {
      SQL_ENG_LOG(WARN, "failed to update sys variable", K(ret));
    } else if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_COLLATION_DATABASE, db_coll))) {
      SQL_ENG_LOG(WARN, "failed to update sys variable", K(ret));
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "alter database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", alter_database_arg.database_schema_.get_database_id(),
      "schema_version", alter_database_arg.database_schema_.get_schema_version());
  }
    SQL_ENG_LOG(INFO, "finish execute alter database", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


//////////////////
ObDropDatabaseExecutor::ObDropDatabaseExecutor()
{
}

ObDropDatabaseExecutor::~ObDropDatabaseExecutor()
{
}

int non_atomic_drop_table_in_database(
    const uint64_t tenant_id,
    const ObString &database_name,
    obrpc::ObCommonRpcProxy &common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObSchemaGetterGuard schema_guard;
  uint64_t database_id = OB_INVALID_ID;
  int64_t start_time = ObTimeUtil::current_time();
  ObArray<const ObSimpleTableSchemaV2 *> simple_table_schemas;
  omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "tenant config invalid, can not drop database", KR(ret), K(tenant_id));
  } else if (tenant_config->_enable_atomic_drop_database) {
    SQL_ENG_LOG(INFO, "atomic drop table in database is enabled", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    SQL_ENG_LOG(WARN, "fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    SQL_ENG_LOG(INFO, "non atomic drop table in database only after 4.5.1.0", KR(ret), K(data_version));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "pointer is null", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    SQL_ENG_LOG(WARN, "get schema guard failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, database_name, database_id))) {
    SQL_ENG_LOG(WARN, "fail to get database id", KR(ret), K(database_name));
  } else if (OB_INVALID_ID == database_id) {
    // drop database if exists or local schema is behind, send to rs.
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id, database_id, simple_table_schemas))) {
    SQL_ENG_LOG(WARN, "get_table_names_in_database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    ObArray<ObString> table_names;
    int64_t table_counts = simple_table_schemas.count();
    if (OB_FAIL(table_names.reserve(table_counts))) {
      SQL_ENG_LOG(WARN, "fail to reserve table names", KR(ret), K(table_counts));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_counts; ++i) {
      const ObSimpleTableSchemaV2 *simple_table_schema = simple_table_schemas.at(i);
      if (OB_ISNULL(simple_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "table schema is null", KR(ret), K(i));
      } else if (ObSchemaUtils::is_support_parallel_drop(simple_table_schema->get_table_type())) {
        if (OB_FAIL(table_names.push_back(simple_table_schema->get_table_name()))) {
          SQL_ENG_LOG(WARN, "fail to push back table name", KR(ret), K(simple_table_schema->get_table_name()));
        }
      }
    }
    ObTimeoutCtx timeout_ctx;
    if (FAILEDx(timeout_ctx.set_timeout(common_rpc_proxy.get_timeout()))) {
      SQL_ENG_LOG(WARN,"fail to set timeout ctx", KR(ret));
    } else {
      ObDropTableBatchScheduler scheduler;
      if (OB_FAIL(scheduler.init(tenant_id,
                                 common_rpc_proxy,
                                 database_name,
                                 table_names))) {
        SQL_ENG_LOG(WARN, "fail to init drop table batch scheduler", KR(ret), K(database_name));
      } else {
        // to avoid hold one version's schema guard too much time
        schema_guard.reset();
        if (OB_FAIL(scheduler.execute())) {
          SQL_ENG_LOG(WARN, "fail to execute drop table batch scheduler", KR(ret),
                      K(tenant_id), K(database_name));
        }
      }
    }
  }
  SQL_ENG_LOG(INFO, "finish drop table non atomic in database", KR(ret), K(tenant_id), K(database_name), "cost", ObTimeUtil::current_time() - start_time);
  return ret;
}


int ObDropDatabaseExecutor::execute(ObExecContext &ctx, ObDropDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropDatabaseArg &drop_database_arg = stmt.get_drop_database_arg();
  obrpc::ObDropDatabaseArg &tmp_arg = const_cast<obrpc::ObDropDatabaseArg&>(drop_database_arg);
  ObString first_stmt;
  uint64_t database_id = 0;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ctx), K(common_rpc_proxy));
  } else {
    obrpc::UInt64 affected_row(0);
    obrpc::ObDropDatabaseRes drop_database_res;
    const_cast<obrpc::ObDropDatabaseArg&>(drop_database_arg).compat_mode_ = ORACLE_MODE == ctx.get_my_session()->get_compatibility_mode()
        ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
    const uint64_t tenant_id = drop_database_arg.tenant_id_;
    const ObString &database_name = drop_database_arg.database_name_;
    if (!drop_database_arg.to_recyclebin_) {
      if (OB_FAIL(non_atomic_drop_table_in_database(tenant_id, database_name, *common_rpc_proxy))) {
        SQL_ENG_LOG(WARN, "fail to drop table non atomic in database", KR(ret), K(tenant_id), K(database_name));
      }
    }
    // after non_atomic_drop_table_in_database
    // not supported parallel drop table type and outline etc will be dropped by drop database
    if (FAILEDx(common_rpc_proxy->drop_database(drop_database_arg, drop_database_res))) {
      SQL_ENG_LOG(WARN, "rpc proxy drop table failed",
                  "timeout", THIS_WORKER.get_timeout_remain(), K(ret));
    } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx));
    } else {
      ObString null_string;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(ctx.get_my_session()->get_name_case_mode(case_mode))) {
        SQL_ENG_LOG(WARN, "fail to get name case mode from session", K(ret));
      } else if (ObCharset::case_mode_equal(case_mode,
                                            ctx.get_my_session()->get_database_name(),
                                            drop_database_arg.database_name_)) {
        ObCollationType server_coll_type = ObCharset::collation_type(stmt.get_server_collation());
        if (OB_UNLIKELY(CS_TYPE_INVALID == server_coll_type)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(ERROR, "invalid collation", K(ret), K(stmt.get_server_collation()));
        } else if (OB_FAIL(ctx.get_my_session()->set_default_database(null_string, server_coll_type))) {
          SQL_ENG_LOG(WARN, "fail to set default database", K(ret), K(stmt.get_server_collation()), K(server_coll_type));
        } else {
          ctx.get_my_session()->set_database_id(OB_INVALID_ID);
        }
      }
    }
    if (OB_SUCC(ret)) {
      ctx.get_physical_plan_ctx()->set_affected_rows(drop_database_res.affected_row_);
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "drop database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", database_id);
  }
  SQL_ENG_LOG(INFO, "finish execute drop database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObFlashBackDatabaseExecutor::execute(ObExecContext &ctx, ObFlashBackDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObFlashBackDatabaseArg &flashback_database_arg = stmt.get_flashback_database_arg();
  obrpc::ObFlashBackDatabaseArg &tmp_arg = const_cast<obrpc::ObFlashBackDatabaseArg&>(flashback_database_arg);
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->flashback_database(flashback_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy flashback database failed", K(ret));
  }

  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "flashback database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "origin_db_name", flashback_database_arg.origin_db_name_,
      "new_db_name", flashback_database_arg.new_db_name_);
  }
  SQL_ENG_LOG(INFO, "finish execute flashback database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObPurgeDatabaseExecutor::execute(ObExecContext &ctx, ObPurgeDatabaseStmt &stmt)
{
  int ret = OB_SUCCESS;
  const obrpc::ObPurgeDatabaseArg &purge_database_arg = stmt.get_purge_database_arg();
  obrpc::ObPurgeDatabaseArg &tmp_arg = const_cast<obrpc::ObPurgeDatabaseArg&>(purge_database_arg);
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "common rpc proxy should not be null", K(ret));
  }
  const uint64_t tenant_id = purge_database_arg.tenant_id_;
  if (FAILEDx(non_atomic_drop_table_in_database(tenant_id, purge_database_arg.db_name_, *common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "rpc proxy drop table failed",
                "timeout", THIS_WORKER.get_timeout_remain(), KR(ret));
  }
  // after non_atomic_drop_table_in_database
  // not supported parallel drop table type and outline etc will be dropped by purge database
  if (FAILEDx(common_rpc_proxy->purge_database(purge_database_arg))) {
    SQL_ENG_LOG(WARN, "rpc proxy purge database failed", K(ret));
  }

  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "purge database execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "database_info", purge_database_arg.db_name_);
  }
  SQL_ENG_LOG(INFO, "finish purge database.", K(ret), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


}
}

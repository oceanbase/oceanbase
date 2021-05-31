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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_index_executor.h"

#include "share/ob_common_rpc_proxy.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_drop_index_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
namespace oceanbase {
using namespace oceanbase::share::schema;

namespace sql {

ObCreateIndexExecutor::ObCreateIndexExecutor()
{}

ObCreateIndexExecutor::~ObCreateIndexExecutor()
{}

int ObCreateIndexExecutor::execute(ObExecContext& ctx, ObCreateIndexStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  obrpc::ObCreateIndexArg& create_index_arg = stmt.get_create_index_arg();
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  const bool is_sys_index = is_inner_table(create_index_arg.index_table_id_);
  obrpc::ObAlterTableRes res;
  ObString first_stmt;
  bool is_sync_ddl_user = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);

  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    create_index_arg.ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (NULL == my_session) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", K(ret), K(ctx));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, stmt))) {
    LOG_WARN("fail to compare range partition expr", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_INVALID_ID == create_index_arg.session_id_ &&
             FALSE_IT(create_index_arg.session_id_ = my_session->get_sessid_for_table())) {
    // impossible
  } else if (FALSE_IT(create_index_arg.is_inner_ = my_session->is_inner())) {
  } else if (OB_FAIL(common_rpc_proxy->create_index(create_index_arg, res))) {
    LOG_WARN("rpc proxy create index failed", K(create_index_arg), "dst", common_rpc_proxy->get_server(), K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(my_session, is_sync_ddl_user))) {
    LOG_WARN("Failed to check sync_dll_user", K(ret));
  } else if (!is_sys_index && !is_sync_ddl_user) {
    // only check sync index status for non-sys table and when it's not backup or restore
    create_index_arg.index_schema_.set_table_id(res.index_table_id_);
    create_index_arg.index_schema_.set_schema_version(res.schema_version_);
    if (OB_FAIL(sync_check_index_status(*my_session, *common_rpc_proxy, create_index_arg, allocator))) {
      LOG_WARN("failed to sync_check_index_status", K(create_index_arg), K(ret));
    }
  }
  return ret;
}

int ObCreateIndexExecutor::set_drop_index_stmt_str(
    obrpc::ObDropIndexArg& drop_index_arg, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;

  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (is_mysql_mode() && OB_FAIL(databuff_printf(buf,
                                    buf_len,
                                    pos,
                                    "ALTER TABLE `%.*s`.`%.*s` DROP INDEX `%.*s`",
                                    drop_index_arg.database_name_.length(),
                                    drop_index_arg.database_name_.ptr(),
                                    drop_index_arg.table_name_.length(),
                                    drop_index_arg.table_name_.ptr(),
                                    drop_index_arg.index_name_.length(),
                                    drop_index_arg.index_name_.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if (is_oracle_mode() && OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "DROP INDEX \"%.*s\"",
                                     drop_index_arg.index_name_.length(),
                                     drop_index_arg.index_name_.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else {
    drop_index_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

// is_update_global_indexes = true: creating index caused by drop/truncate partition,don't need to drop failed index
// when reports error. is_update_global_indexes = false:creating index caused by create index/alter table add index,need
// to drop failed index when reports error.
int ObCreateIndexExecutor::sync_check_index_status(sql::ObSQLSessionInfo& my_session,
    obrpc::ObCommonRpcProxy& common_rpc_proxy, const obrpc::ObCreateIndexArg& create_index_arg,
    common::ObIAllocator& allocator, bool is_update_global_indexes)
{
  int ret = OB_SUCCESS;
  // force refresh schema version, make sure version of observer is latest
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  bool is_finish = false;
  const static int CHECK_INTERVAL = 100 * 1000;  // 100ms
  obrpc::ObDropIndexArg drop_index_arg;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = my_session.get_effective_tenant_id();
  const uint64_t index_table_id = create_index_arg.index_schema_.get_table_id();
  ObSqlString drop_index_sql;

  if (!is_update_global_indexes) {
    if (share::is_oracle_mode()) {
      ret = drop_index_sql.append_fmt(
          "drop index \"%.*s\"", create_index_arg.index_name_.length(), create_index_arg.index_name_.ptr());
    } else {
      ret = drop_index_sql.append_fmt("drop index `%.*s` on `%.*s`",
          create_index_arg.index_name_.length(),
          create_index_arg.index_name_.ptr(),
          create_index_arg.table_name_.length(),
          create_index_arg.table_name_.ptr());
    }
    if (!OB_SUCC(ret)) {
      OB_LOG(WARN, "fail to append drop index sql", KR(ret));
    } else {
      drop_index_arg.tenant_id_ = create_index_arg.tenant_id_;
      drop_index_arg.exec_tenant_id_ = create_index_arg.tenant_id_;
      drop_index_arg.index_table_id_ = index_table_id;
      drop_index_arg.session_id_ = create_index_arg.session_id_;
      drop_index_arg.index_name_ = create_index_arg.index_name_;
      drop_index_arg.table_name_ = create_index_arg.table_name_;
      drop_index_arg.database_name_ = create_index_arg.database_name_;
      drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
      drop_index_arg.to_recyclebin_ = false;
      drop_index_arg.ddl_stmt_str_ = drop_index_sql.string();
    }
  }

  while (OB_SUCC(ret) && !is_finish) {
    // check whether index_table_id received from rs is valid.
    if (OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
      is_finish = true;
      if (true == create_index_arg.if_not_exist_) {
        // if not exist, ignore error code and exit check_index_status
        // since the index is not created, don't need to rollback.
        break;
      } else {
        ret = OB_ERR_ADD_INDEX;
        LOG_WARN("index table id is invalid", KR(ret), K(index_table_id));
      }
    }
    // handle session timeout or killed first.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handle_session_exception(my_session))) {
      if (!is_update_global_indexes && (OB_ERR_QUERY_INTERRUPTED == ret || OB_SESSION_KILLED == ret)) {
        LOG_WARN("handle_session_exception", KR(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = common_rpc_proxy.drop_index(drop_index_arg))) {
          LOG_WARN("rpc proxy drop index failed",
              "dst",
              common_rpc_proxy.get_server(),
              K(tmp_ret),
              K(drop_index_arg.table_name_),
              K(drop_index_arg.index_name_));
        }
      } else {
        LOG_WARN("failed to handle_session_exception", KR(ret));
      }
    }

    // If switch to standby db when it start to work,
    // return session killed, and the index is handled by standby.
    if (OB_FAIL(ret)) {
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      // no need to process sys tenant
    } else if (OB_FAIL(handle_switchover())) {
      if (OB_SESSION_KILLED != ret) {
        LOG_WARN("fail to handle switchover status", KR(ret));
      } else {
        LOG_WARN("fail to add index while swithover", KR(ret));
      }
    }

    share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
    const ObTableSchema* index_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(ret)) {
    } else if (NULL == schema_service) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema servicie", KR(ret));
    } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("fail to get tenant refreshed schema version", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (refreshed_schema_version < create_index_arg.index_schema_.get_schema_version()) {
      // not refresh schema after the index is created, check again after sleep
      usleep(CHECK_INTERVAL);
      LOG_INFO("we have not gotten the index schema",
          K(index_table_id),
          K(refreshed_schema_version),
          K(create_index_arg.index_schema_.get_schema_version()));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
      LOG_WARN("fail to get index table schema", KR(ret), K(refreshed_schema_version), K(index_table_id));
    } else if (OB_ISNULL(index_schema)) {
      // maybe ddl(drop index,drop table,truncate table) in another session has dropped this index.
      if (!is_update_global_indexes) {
        ret = OB_ERR_ADD_INDEX;
        LOG_USER_ERROR(OB_ERR_ADD_INDEX);
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("index table schema is null",
          KR(ret),
          K(index_table_id),
          K(refreshed_schema_version),
          K(create_index_arg.index_schema_.get_schema_version()));
    } else if (!is_final_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
      usleep(CHECK_INTERVAL);
      LOG_INFO("index status is not final", K(index_table_id));
    } else {
      is_finish = true;
      LOG_INFO("index status is final", K(index_table_id), K(index_schema->get_index_status()));
      if (is_error_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
        // status of index is ERROR, need to rollback.
        if (is_update_global_indexes) {
          ret = OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX;
          LOG_USER_ERROR(OB_ERR_DROP_TRUNCATE_PARTITION_REBUILD_INDEX,
              create_index_arg.index_name_.length(),
              create_index_arg.index_name_.ptr());
        } else {
          ret = OB_ERR_ADD_INDEX;
          LOG_USER_ERROR(OB_ERR_ADD_INDEX);
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = set_drop_index_stmt_str(drop_index_arg, allocator))) {
            LOG_WARN("fail to set drop index ddl_stmt_str", K(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy.drop_index(drop_index_arg))) {
            LOG_WARN("rpc proxy drop index failed",
                "dst",
                common_rpc_proxy.get_server(),
                K(tmp_ret),
                K(drop_index_arg.table_name_),
                K(drop_index_arg.index_name_));
          }
        }
      }
    }
  }

  return ret;
}

int ObCreateIndexExecutor::handle_session_exception(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(session.is_query_killed())) {
    ret = OB_ERR_QUERY_INTERRUPTED;
    LOG_WARN("query is killed", K(ret));
  } else if (OB_UNLIKELY(session.is_zombie())) {
    ret = OB_SESSION_KILLED;
    LOG_WARN("session is killed", K(ret));
  }

  return ret;
}

int ObCreateIndexExecutor::handle_switchover()
{
  int ret = OB_SUCCESS;
  if (GCTX.is_standby_cluster()) {
    ret = OB_SESSION_KILLED;
    LOG_INFO("create index while switchoverd, kill session", KR(ret));
  }
  return ret;
}

ObDropIndexExecutor::ObDropIndexExecutor()
{}

ObDropIndexExecutor::~ObDropIndexExecutor()
{}

int ObDropIndexExecutor::execute(ObExecContext& ctx, ObDropIndexStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObDropIndexArg& drop_index_arg = stmt.get_drop_index_arg();
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObDropIndexArg&>(drop_index_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (NULL == my_session) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", K(ret), K(ctx));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_INVALID_ID == drop_index_arg.session_id_ &&
             FALSE_IT(
                 const_cast<obrpc::ObDropIndexArg&>(drop_index_arg).session_id_ = my_session->get_sessid_for_table())) {
    // impossible
  } else if (OB_FAIL(common_rpc_proxy->drop_index(drop_index_arg))) {
    LOG_WARN("rpc proxy drop index failed", "dst", common_rpc_proxy->get_server(), K(ret));
  }
  return ret;
}

int ObPurgeIndexExecutor::execute(ObExecContext& ctx, ObPurgeIndexStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  const obrpc::ObPurgeIndexArg& purge_index_arg = stmt.get_purge_index_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else {
    const_cast<obrpc::ObPurgeIndexArg&>(purge_index_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->purge_index(purge_index_arg))) {
    LOG_WARN("rpc proxy purge index failed", "dst", common_rpc_proxy->get_server(), K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

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
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_drop_index_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
namespace oceanbase
{
using namespace oceanbase::share::schema;

namespace sql
{

ObCreateIndexExecutor::ObCreateIndexExecutor()
{
}

ObCreateIndexExecutor::~ObCreateIndexExecutor()
{
}

int ObCreateIndexExecutor::execute(ObExecContext &ctx, ObCreateIndexStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObCreateIndexArg &create_index_arg = stmt.get_create_index_arg();
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  const bool is_sys_index = is_inner_table(create_index_arg.index_table_id_);
  obrpc::ObAlterTableRes res;
  ObString first_stmt;
  bool is_sync_ddl_user = false;
  ObArenaAllocator allocator("CreateIndexExec");

  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
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
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_INVALID_ID == create_index_arg.session_id_
             && FALSE_IT(create_index_arg.session_id_ = my_session->get_sessid_for_table())) {
    //impossible
  } else if (FALSE_IT(create_index_arg.is_inner_ = my_session->is_inner())) {
  } else if (FALSE_IT(create_index_arg.parallelism_ = stmt.get_parallelism())) {
  } else if (FALSE_IT(create_index_arg.consumer_group_id_ = THIS_WORKER.get_group_id())) {
  } else if (OB_FAIL(common_rpc_proxy->create_index(create_index_arg, res))) {    //send the signal of creating index to rs
    LOG_WARN("rpc proxy create index failed", K(create_index_arg),
             "dst", common_rpc_proxy->get_server(), K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(my_session, is_sync_ddl_user))) {
    LOG_WARN("Failed to check sync_dll_user", K(ret));
  } else if (!is_sys_index && !is_sync_ddl_user) {
    // 只考虑非系统表和非备份恢复时的索引同步检查
    create_index_arg.index_schema_.set_table_id(res.index_table_id_);
    create_index_arg.index_schema_.set_schema_version(res.schema_version_);
    if (OB_UNLIKELY(OB_INVALID_ID == create_index_arg.index_schema_.get_table_id())) {
      if (create_index_arg.if_not_exist_) {
        // if not exist ignore err code
      } else {
        ret = OB_ERR_ADD_INDEX;
        LOG_WARN("index table id is invalid", KR(ret));
      }
    } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(create_index_arg.tenant_id_, res.task_id_, my_session, common_rpc_proxy))) {
      LOG_WARN("failed to wait ddl finish", K(ret));
    }
  }
  return ret;
}

int ObCreateIndexExecutor::set_drop_index_stmt_str(
    obrpc::ObDropIndexArg &drop_index_arg,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(OB_MAX_SQL_LENGTH));
  } else if (is_mysql_mode()
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                        "ALTER TABLE `%.*s`.`%.*s` DROP INDEX `%.*s`",
                        drop_index_arg.database_name_.length(),
                        drop_index_arg.database_name_.ptr(),
                        drop_index_arg.table_name_.length(),
                        drop_index_arg.table_name_.ptr(),
                        drop_index_arg.index_name_.length(),
                        drop_index_arg.index_name_.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else if (is_oracle_mode()
             && OB_FAIL(databuff_printf(buf, buf_len, pos,
                        "DROP INDEX \"%.*s\"",
                        drop_index_arg.index_name_.length(),
                        drop_index_arg.index_name_.ptr()))) {
    LOG_WARN("fail to print ddl_stmt_str for rollback", K(ret));
  } else {
    drop_index_arg.ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
  }

  return ret;
}

// is_update_global_indexes = true: drop/truncate partition will trigger index building, no need delete failed index at exception
// is_update_global_indexes = false: create index/alter table add index will trigger index building, need delete failed index at exception
int ObCreateIndexExecutor::sync_check_index_status(sql::ObSQLSessionInfo &my_session,
    obrpc::ObCommonRpcProxy &common_rpc_proxy,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const obrpc::ObAlterTableRes &res,
    common::ObIAllocator &allocator,
    bool is_update_global_indexes)
{
  int ret = OB_SUCCESS;
  // 强制刷schema版本, 保证observer版本最新
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  bool is_finish = false;
  const static int CHECK_INTERVAL = 100 * 1000; // 100ms
  obrpc::ObDropIndexArg drop_index_arg;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = my_session.get_effective_tenant_id();
  const uint64_t index_table_id = create_index_arg.index_schema_.get_table_id();
  ObSqlString           drop_index_sql;

  if (!is_update_global_indexes) {
    if (lib::is_oracle_mode()){
      ret = drop_index_sql.append_fmt("drop index \"%.*s\"",
                                      create_index_arg.index_name_.length(),
                                      create_index_arg.index_name_.ptr());
    }
    else{
      ret = drop_index_sql.append_fmt("drop index `%.*s` on `%.*s`",
                                      create_index_arg.index_name_.length(),
                                      create_index_arg.index_name_.ptr(),
                                      create_index_arg.table_name_.length(),
                                      create_index_arg.table_name_.ptr());
    }
    if (!OB_SUCC(ret)){
      OB_LOG(WARN, "fail to append drop index sql", KR(ret));
    } else {
      drop_index_arg.tenant_id_         = tenant_id;
      drop_index_arg.exec_tenant_id_         = tenant_id;
      drop_index_arg.index_table_id_    = index_table_id;
      drop_index_arg.session_id_        = create_index_arg.session_id_;
      drop_index_arg.index_name_        = create_index_arg.index_name_;
      drop_index_arg.table_name_        = create_index_arg.table_name_;
      drop_index_arg.database_name_     = create_index_arg.database_name_;
      drop_index_arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
      drop_index_arg.ddl_stmt_str_      = drop_index_sql.string();
      drop_index_arg.is_add_to_scheduler_ = false;
    }
  }

  while (OB_SUCC(ret) && !is_finish) {
    // 判断rs端返回的index_table_id是否合法
    if (OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
      is_finish = true; // 不合法的index_table_id直接直接终止check index status
      if (true == create_index_arg.if_not_exist_) {
        // if not exist忽略错误码
        // 并直接退出check index status
        // 由于该索引并为创建，因此无需drop index进行回滚
        break;
      } else {
        ret = OB_ERR_ADD_INDEX;
        LOG_WARN("index table id is invalid", KR(ret), K(index_table_id));
      }
    }
    // 先处理session超时或者kill异常场景
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handle_session_exception(my_session))) {
      if (is_query_killed_return(ret)
          || OB_SESSION_KILLED == ret) {
        LOG_WARN("handle_session_exception", K(ret));
      }
      if (!is_update_global_indexes
          && (OB_ERR_QUERY_INTERRUPTED == ret || OB_SESSION_KILLED == ret)) {
        LOG_WARN("handle_session_exception", KR(ret));
        int tmp_ret = OB_SUCCESS;
        ObDropIndexRes drop_index_res;
        if (OB_SUCCESS != (tmp_ret = set_drop_index_stmt_str(drop_index_arg, allocator))) {
          LOG_WARN("fail to set drop index ddl_stmt_str", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy.drop_index(drop_index_arg, drop_index_res))) {
          LOG_WARN("rpc proxy drop index failed", "dst", common_rpc_proxy.get_server(), K(tmp_ret),
              K(drop_index_arg.table_name_), K(drop_index_arg.index_name_));
        }
      } else {
        LOG_WARN("failed to handle_session_exception", KR(ret));
      }
    }

    //处理主备库切换的场景，生效过程中发生切换的话，直接返回用户session_killed;
    //后续有备库来处理该索引；
    if (OB_FAIL(ret)) {
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      //no need to process sys tenant
    } else if (OB_FAIL(handle_switchover())) {
      if (OB_SESSION_KILLED != ret) {
        LOG_WARN("fail to handle switchover status", KR(ret));
      } else {
        LOG_WARN("fail to add index while swithover", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLExecutorUtil::wait_build_index_finish(tenant_id, res.task_id_, is_finish))) {
      LOG_WARN("wait build index finish failed", K(ret), K(tenant_id), K(res.task_id_));
    } else if (!is_finish) {
      ob_usleep(CHECK_INTERVAL);
      LOG_INFO("index status is not final", K(index_table_id));
    } else {
      LOG_INFO("index status is final", K(ret), K(index_table_id));
    }
  }

  return ret;
}

int ObCreateIndexExecutor::handle_session_exception(ObSQLSessionInfo &session)
{
  return session.check_session_status();
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
{
}

ObDropIndexExecutor::~ObDropIndexExecutor()
{
}

int ObDropIndexExecutor::wait_drop_index_finish(
    const uint64_t tenant_id,
    const int64_t task_id,
    sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id));
  } else {
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
    ObAddr unused_addr;
    share::ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
    int64_t unused_user_msg_len = 0;
    const int64_t retry_interval = 100 * 1000;
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      bool is_tenant_dropped = false;
      bool is_tenant_standby = false;
      if (OB_SUCCESS == share::ObDDLErrorMessageTableOperator::get_ddl_error_message(
          tenant_id, task_id, -1 /* target_object_id */, unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, unused_user_msg_len)) {
        ret = error_message.ret_code_;
        if (OB_SUCCESS != ret) {
          FORWARD_USER_ERROR(ret, error_message.user_message_);
        }
        break;
      } else {
        if (OB_FAIL(ret)) {
        } else if (OB_TMP_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(
                                tenant_id, is_tenant_dropped))) {
          LOG_WARN("check if tenant has been dropped failed", K(tmp_ret), K(tenant_id));
        } else if (is_tenant_dropped) {
          ret = OB_TENANT_HAS_BEEN_DROPPED;
          LOG_WARN("tenant has been dropped", K(ret), K(tenant_id));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_TMP_FAIL(ObAllTenantInfoProxy::is_standby_tenant(GCTX.sql_proxy_, tenant_id, is_tenant_standby))) {
          LOG_WARN("check is standby tenant failed", K(tmp_ret), K(tenant_id));
        } else if (is_tenant_standby) {
          ret = OB_STANDBY_READ_ONLY;
          FORWARD_USER_ERROR(ret, "DDL not finish, need check");
          LOG_WARN("tenant is standby now, stop wait", K(ret), K(tenant_id));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(session.check_session_status())) {
          LOG_WARN("session exception happened", K(ret));
        } else {
          ob_usleep(retry_interval);
        }
      }
    }
  }
  return ret;
}

int ObDropIndexExecutor::execute(ObExecContext &ctx, ObDropIndexStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropIndexArg &drop_index_arg = stmt.get_drop_index_arg();
  obrpc::ObDropIndexArg &tmp_arg = const_cast<obrpc::ObDropIndexArg&>(drop_index_arg);
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  ObString first_stmt;
  ObDropIndexRes res;
  tmp_arg.is_add_to_scheduler_ = true;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;

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
  }  else if (OB_INVALID_ID == drop_index_arg.session_id_
             && FALSE_IT(tmp_arg.session_id_ = my_session->get_sessid_for_table())) {
    //impossible
  } else if (FALSE_IT(tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id())) {
  } else if (OB_FAIL(common_rpc_proxy->drop_index(drop_index_arg, res))) {
    LOG_WARN("rpc proxy drop index failed", "dst", common_rpc_proxy->get_server(), K(ret));
  } else if (OB_FAIL(wait_drop_index_finish(res.tenant_id_, res.task_id_, *my_session))) {
    LOG_WARN("wait drop index finish failed", K(ret));
  }
  return ret;
}

int ObFlashBackIndexExecutor::execute(ObExecContext &ctx, ObFlashBackIndexStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObFlashBackIndexArg &flashback_index_arg = stmt.get_flashback_index_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObFlashBackIndexArg&>(flashback_index_arg).ddl_stmt_str_ = first_stmt;
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
  } else if (OB_FAIL(common_rpc_proxy->flashback_index(flashback_index_arg))) {
    LOG_WARN("rpc proxy flashback index failed", "dst", common_rpc_proxy->get_server(), K(ret));
  }
  return ret;
}

int ObPurgeIndexExecutor::execute(ObExecContext &ctx, ObPurgeIndexStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObPurgeIndexArg &purge_index_arg = stmt.get_purge_index_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
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

}/* ns sql*/
}/* ns oceanbase */

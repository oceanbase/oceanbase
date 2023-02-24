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

#define USING_LOG_PREFIX STORAGE

#include "ob_ddl_server_client.h"
#include "observer/ob_server_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_ddl_common.h"
#include "storage/ddl/ob_ddl_heart_beat_task.h"
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"

namespace oceanbase
{
namespace storage
{

int ObDDLServerClient::create_hidden_table(const obrpc::ObCreateHiddenTableArg &arg, obrpc::ObCreateHiddenTableRes &res, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->timeout(GCONF._ob_ddl_timeout).create_hidden_table(arg, res))) {
    LOG_WARN("failed to create hidden table", KR(ret), K(arg));
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.set_register_task_id(res.task_id_, res.tenant_id_))) {
    LOG_WARN("failed to set register task id", K(ret), K(res));
  } else if (OB_FAIL(wait_task_reach_pending(arg.tenant_id_, res.task_id_, *GCTX.sql_proxy_, session))) {
    LOG_WARN("failed to wait table lock. remove register task id and abort redef table task.", K(ret), K(arg), K(res));
    int tmp_ret = OB_SUCCESS;
    obrpc::ObAbortRedefTableArg abort_redef_table_arg;
    abort_redef_table_arg.task_id_ = res.task_id_;
    abort_redef_table_arg.tenant_id_ = arg.tenant_id_;
    if (OB_TMP_FAIL(abort_redef_table(abort_redef_table_arg, session))) {
      LOG_WARN("failed to abort redef table", K(tmp_ret), K(abort_redef_table_arg));
    }
    if (OB_TMP_FAIL(heart_beat_clear(res.task_id_))) {
      LOG_WARN("heart beat clear failed", K(tmp_ret), K(res.task_id_));
    }
  }
  return ret;
}

int ObDDLServerClient::start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->start_redef_table(arg, res))) {
    LOG_WARN("failed to start redef table", KR(ret), K(arg));
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.set_register_task_id(res.task_id_, res.tenant_id_))) {
    LOG_WARN("failed to set register task id", K(ret), K(res));
  } else if (OB_FAIL(wait_task_reach_pending(arg.orig_tenant_id_, res.task_id_, *GCTX.sql_proxy_, session))) {
    LOG_WARN("failed to wait table lock. remove register task id and abort redef table task.", K(ret), K(arg), K(res));
    int tmp_ret = OB_SUCCESS;
    obrpc::ObAbortRedefTableArg abort_redef_table_arg;
    abort_redef_table_arg.task_id_ = res.task_id_;
    abort_redef_table_arg.tenant_id_ = arg.orig_tenant_id_;
    if (OB_TMP_FAIL(abort_redef_table(abort_redef_table_arg, session))) {
      LOG_WARN("failed to abort redef table", K(tmp_ret), K(abort_redef_table_arg));
    }
    if (OB_TMP_FAIL(heart_beat_clear(res.task_id_))) {
      LOG_WARN("heart beat clear failed", K(tmp_ret), K(res.task_id_));
    }
  }
  return ret;
}
int ObDDLServerClient::copy_table_dependents(const obrpc::ObCopyTableDependentsArg &arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->copy_table_dependents(arg))) {
    LOG_WARN("failed to copy table dependents", KR(ret), K(arg));
  }
  return ret;
}

int ObDDLServerClient::abort_redef_table(const obrpc::ObAbortRedefTableArg &arg, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval = 100 * 1000L;
  ObAddr rs_leader_addr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
        LOG_WARN("fail to get rootservice address", K(tmp_ret));
      } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).abort_redef_table(arg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          break;
        } else {
          ret = OB_SUCCESS;
          ob_usleep(retry_interval);
        }
      } else {
        LOG_INFO("abort task success");
        break;
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(sql::ObDDLExecutorUtil::wait_ddl_finish(arg.tenant_id_, arg.task_id_, session, common_rpc_proxy))) {
      LOG_WARN("wait ddl finish failed", K(ret), K(arg.tenant_id_), K(arg.task_id_));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(heart_beat_clear(arg.task_id_))) {
      LOG_WARN("heart beat clear failed", K(tmp_ret), K(arg.task_id_));
    }
  }
  return ret;
}

int ObDDLServerClient::finish_redef_table(const obrpc::ObFinishRedefTableArg &finish_redef_arg,
                                          const obrpc::ObDDLBuildSingleReplicaResponseArg &build_single_arg,
                                          sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!finish_redef_arg.is_valid() || !build_single_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(finish_redef_arg), K(build_single_arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->finish_redef_table(finish_redef_arg))) {
    LOG_WARN("failed to finish redef table", K(ret), K(finish_redef_arg));
  } else if (OB_FAIL(build_ddl_single_replica_response(build_single_arg))) {
    LOG_WARN("build ddl single replica response", K(ret), K(build_single_arg));
  } else if (OB_FAIL(sql::ObDDLExecutorUtil::wait_ddl_finish(finish_redef_arg.tenant_id_, finish_redef_arg.task_id_, session, common_rpc_proxy))) {
    LOG_WARN("failed to wait ddl finish", K(ret), K(finish_redef_arg.tenant_id_), K(finish_redef_arg.task_id_));
  }
  if (OB_TMP_FAIL(heart_beat_clear(finish_redef_arg.task_id_))) {
    LOG_WARN("heart beat clear failed", K(tmp_ret), K(finish_redef_arg.task_id_));
  }
  return ret;
}

int ObDDLServerClient::build_ddl_single_replica_response(const obrpc::ObDDLBuildSingleReplicaResponseArg &arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->build_ddl_single_replica_response(arg))) {
    LOG_WARN("failed to finish redef table", K(ret), K(arg));
  }
  return ret;
}

int ObDDLServerClient::wait_task_reach_pending(const uint64_t tenant_id, const int64_t task_id, ObMySQLProxy &sql_proxy, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval = 100 * 1000;
  ObSqlString sql_string;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(task_id <= 0 || OB_INVALID_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.assign_fmt("SELECT status FROM %s WHERE task_id = %lu", share::OB_ALL_DDL_TASK_STATUS_TNAME, task_id))) {
          LOG_WARN("assign sql string failed", K(ret), K(task_id));
        } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql_string.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql_string));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, query result must not be NULL", K(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          int task_status = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "status", task_status, int);
          share::ObDDLTaskStatus task_cur_status = static_cast<share::ObDDLTaskStatus>(task_status);
          if (rootserver::ObTableRedefinitionTask::check_task_status_before_pending(task_cur_status)) {
            LOG_INFO("task status not equal REPENDING, Please Keep Waiting", K(task_status));
            if (OB_FAIL(sql::ObDDLExecutorUtil::handle_session_exception(session))) {
              break;
            } else {
              ob_usleep(retry_interval);
            }
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLServerClient::heart_beat_clear(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.remove_register_task_id(task_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("failed to remove register task id", K(ret), K(task_id));
    }
  }
  return ret;
}


}  // end of namespace storage
}  // end of namespace oceanbase

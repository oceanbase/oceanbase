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
#include "share/ob_ddl_sim_point.h"
#include "storage/ddl/ob_ddl_heart_beat_task.h"
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace storage
{

int ObDDLServerClient::execute_recover_restore_table(const obrpc::ObRecoverRestoreTableDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ObAddr rs_leader_addr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  const uint64_t dst_tenant_id = arg.target_schema_.get_tenant_id();
  const int64_t ddl_task_id = arg.ddl_task_id_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
    LOG_WARN("fail to rootservice address", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).timeout(GCONF._ob_ddl_timeout).recover_restore_table_ddl(arg))) {
    LOG_WARN("fail to create not major sstable table", K(ret), K(arg));
  }
  return ret;
}

int ObDDLServerClient::create_hidden_table(
    const obrpc::ObCreateHiddenTableArg &arg,
    obrpc::ObCreateHiddenTableRes &res,
    int64_t &snapshot_version,
    uint64_t &data_format_version,
    sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObAddr rs_leader_addr;
  const int64_t retry_interval = 100 * 1000L;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
    LOG_WARN("fail to rootservice address", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).timeout(GCONF._ob_ddl_timeout).create_hidden_table(arg, res))) {
      LOG_WARN("failed to create hidden table", KR(ret), K(arg));
    } else {
      break;
    }
    if (OB_FAIL(ret) && is_ddl_stmt_packet_retry_err(ret)) {
      ob_usleep(retry_interval);
      if (OB_FAIL(THIS_WORKER.check_status())) { // overwrite ret
        LOG_WARN("failed to check status", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.set_register_task_id(res.task_id_, res.tenant_id_))) {
    LOG_WARN("failed to set register task id", K(ret), K(res));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(wait_task_reach_pending(arg.tenant_id_, res.task_id_, snapshot_version, data_format_version, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to wait table lock. remove register task id and abort redef table task.", K(ret), K(arg), K(res));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_DDL_DIRECT_LOAD_WAIT_TABLE_LOCK_FAIL) OB_SUCCESS;
      LOG_INFO("wait table lock failed errsim", K(ret));
    }
#endif
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      obrpc::ObAbortRedefTableArg abort_redef_table_arg;
      abort_redef_table_arg.task_id_ = res.task_id_;
      abort_redef_table_arg.tenant_id_ = arg.tenant_id_;
      if (OB_TMP_FAIL(abort_redef_table(abort_redef_table_arg, &session))) {
        LOG_WARN("failed to abort redef table", K(tmp_ret), K(abort_redef_table_arg));
      }
      // abort_redef_table() function last step must remove heart_beat task, so there is no need to call heart_beat_clear()
    }
  }
  char tenant_id_buffer[256];
  snprintf(tenant_id_buffer, sizeof(tenant_id_buffer), "tenant_id:%ld, dest_tenant_id:%ld",
            arg.tenant_id_, arg.dest_tenant_id_);
  SERVER_EVENT_ADD("ddl", "create hidden table",
    "tenant_id", tenant_id_buffer,
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", res.task_id_,
    "table_id", res.table_id_,
    "schema_version", res.schema_version_);
  LOG_INFO("finish create hidden table.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(arg), K(res));
  return ret;
}

int ObDDLServerClient::start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObAddr rs_leader_addr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  int64_t unused_snapshot_version = OB_INVALID_VERSION;
  uint64_t unused_data_format_version = 0;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
    LOG_WARN("fail to rootservice address", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).start_redef_table(arg, res))) {
    LOG_WARN("failed to start redef table", KR(ret), K(arg));
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.set_register_task_id(res.task_id_, res.tenant_id_))) {
    LOG_WARN("failed to set register task id", K(ret), K(res));
  } else if (OB_FAIL(wait_task_reach_pending(arg.orig_tenant_id_, res.task_id_, unused_snapshot_version, unused_data_format_version, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to wait table lock. remove register task id and abort redef table task.", K(ret), K(arg), K(res));
    int tmp_ret = OB_SUCCESS;
    obrpc::ObAbortRedefTableArg abort_redef_table_arg;
    abort_redef_table_arg.task_id_ = res.task_id_;
    abort_redef_table_arg.tenant_id_ = arg.orig_tenant_id_;
    if (OB_TMP_FAIL(abort_redef_table(abort_redef_table_arg, &session))) {
      LOG_WARN("failed to abort redef table", K(tmp_ret), K(abort_redef_table_arg));
    }
    // abort_redef_table() function last step must remove heart_beat task, so there is no need to call heart_beat_clear()
  }
  char tenant_id_buffer[256];
  snprintf(tenant_id_buffer, sizeof(tenant_id_buffer), "orig_tenant_id:%ld, target_tenant_id:%ld",
            arg.orig_tenant_id_, arg.target_tenant_id_);
  char table_id_buffer[256];
  snprintf(tenant_id_buffer, sizeof(tenant_id_buffer), "orig_table_id:%ld, target_table_id:%ld",
            arg.orig_table_id_, arg.target_table_id_);

  SERVER_EVENT_ADD("ddl", "start redef table",
    "tenant_id", tenant_id_buffer,
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", res.task_id_,
    "table_id", table_id_buffer,
    "schema_version", res.schema_version_);
  LOG_INFO("start redef table.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(arg), K(res));

  return ret;
}

int ObDDLServerClient::copy_table_dependents(
    const obrpc::ObCopyTableDependentsArg &arg,
    sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
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
      if (OB_FAIL(check_need_stop(tenant_id))) {
        LOG_WARN("fail to basic check", K(ret), K(tenant_id));
      } else if (OB_FAIL(ObDDLExecutorUtil::handle_session_exception(session))) {
        LOG_WARN("fail to handle session exception", K(ret));
        if (OB_TMP_FAIL(ObDDLExecutorUtil::cancel_ddl_task(tenant_id, common_rpc_proxy))) {
          LOG_WARN("cancel ddl task failed", K(tmp_ret), K(tenant_id));
        }
      } else if (OB_TMP_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
        LOG_WARN("fail to get rootservice address", K(ret));
      } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).copy_table_dependents(arg))) {
        LOG_WARN("copy table dependents failed", K(ret), K(arg));
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("ddl task not exist", K(ret), K(arg));
          break;
        } else if (OB_NOT_SUPPORTED == ret) {
          LOG_WARN("not supported copy table dependents", K(ret), K(arg));
          break;
        } else {
          LOG_INFO("ddl task exist, try again", K(arg));
          ret = OB_SUCCESS;
          ob_usleep(retry_interval);
        }
      } else {
        LOG_INFO("copy table dependents success", K(arg));
        break;
      }
    }
  }

  SERVER_EVENT_ADD("ddl", "copy table dependents",
    "tenant_id", arg.tenant_id_,
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", arg.task_id_,
    "rpc_dst", rs_leader_addr);
  LOG_INFO("finish copy table dependents.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(arg), K(rs_leader_addr));
  return ret;
}

int ObDDLServerClient::abort_redef_table(const obrpc::ObAbortRedefTableArg &arg, sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
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
      if (OB_FAIL(check_need_stop(tenant_id))) {
        LOG_WARN("fail to basic check", K(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
        LOG_WARN("fail to get rootservice address", K(tmp_ret));
      } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).abort_redef_table(arg))) {
        LOG_WARN("abort redef table failed", K(ret), K(arg));
        if (OB_ENTRY_NOT_EXIST == ret) {
          break;
        } else if (OB_NOT_SUPPORTED == ret) {
          LOG_WARN("not supported abort direct load task", K(ret), K(arg));
          break;
        } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          LOG_WARN("no enough memory to abort", K(ret), K(arg));
          break;
        } else if (OB_SIZE_OVERFLOW == ret) {
          LOG_WARN("no enough queue size to abort", K(ret), K(arg), K(rs_leader_addr));
          break;
        } else {
          LOG_INFO("ddl task exist, try again", K(arg));
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
    if (OB_SUCC(ret)) {
      const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
      int64_t MAX_ABORT_WAIT_TIMEOUT = 60 * 1000 * 1000; //60s
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + MAX_ABORT_WAIT_TIMEOUT);
      if (OB_FAIL(sql::ObDDLExecutorUtil::wait_ddl_finish(arg.tenant_id_, arg.task_id_, DDL_DIRECT_LOAD, session, common_rpc_proxy))) {
        if (OB_CANCELED == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("ddl abort success", K_(arg.task_id));
        } else {
          LOG_WARN("wait ddl finish failed", K(ret), K(arg.tenant_id_), K(arg.task_id_));
        }
      }
      THIS_WORKER.set_timeout_ts(origin_timeout_ts);
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(heart_beat_clear(arg.task_id_, tenant_id))) {
      LOG_WARN("heart beat clear failed", K(tmp_ret), K(arg.task_id_));
    }
  }

  SERVER_EVENT_ADD("ddl", "abort redef table",
    "tenant_id", arg.tenant_id_,
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", arg.task_id_,
    "rpc_dst", rs_leader_addr);
  LOG_INFO("abort redef table.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(arg), K(rs_leader_addr));
  return ret;
}

int ObDDLServerClient::finish_redef_table(const obrpc::ObFinishRedefTableArg &finish_redef_arg,
                                          const obrpc::ObDDLBuildSingleReplicaResponseArg &build_single_arg,
                                          sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = finish_redef_arg.tenant_id_;
  const int64_t retry_interval = 100 * 1000L;
  ObAddr rs_leader_addr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!finish_redef_arg.is_valid() || !build_single_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(finish_redef_arg), K(build_single_arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(check_need_stop(tenant_id))) {
        LOG_WARN("fail to basic check", K(ret), K(tenant_id));
      } else if (OB_FAIL(ObDDLExecutorUtil::handle_session_exception(session))) {
        LOG_WARN("session execption happened", K(ret));
        if (OB_TMP_FAIL(ObDDLExecutorUtil::cancel_ddl_task(tenant_id, common_rpc_proxy))) {
          LOG_WARN("cancel ddl task failed", K(tmp_ret));
          ret = OB_SUCCESS;
        }
      } else if (OB_TMP_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
        LOG_WARN("fail to rootservice address", K(tmp_ret));
      } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).finish_redef_table(finish_redef_arg))) {
        LOG_WARN("finish redef table failed", K(ret), K(finish_redef_arg));
        if (OB_ENTRY_NOT_EXIST == ret) {
          break;
        } else if (OB_NOT_SUPPORTED == ret) {
          LOG_WARN("not supported finish redef table", K(ret), K(finish_redef_arg));
          break;
        } else {
          LOG_INFO("ddl task exist, try again", K(finish_redef_arg));
          ret = OB_SUCCESS;
          ob_usleep(retry_interval);
        }
      } else {
        LOG_INFO("finish redef table success", K(finish_redef_arg));
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_ddl_single_replica_response(build_single_arg))) {
        LOG_WARN("build ddl single replica response", K(ret), K(build_single_arg));
    } else if (OB_FAIL(sql::ObDDLExecutorUtil::wait_ddl_finish(finish_redef_arg.tenant_id_, finish_redef_arg.task_id_, DDL_DIRECT_LOAD, &session, common_rpc_proxy))) {
      LOG_WARN("failed to wait ddl finish", K(ret), K(finish_redef_arg.tenant_id_), K(finish_redef_arg.task_id_));
    }
    if (OB_TMP_FAIL(heart_beat_clear(finish_redef_arg.task_id_, tenant_id))) {
      LOG_WARN("heart beat clear failed", K(tmp_ret), K(finish_redef_arg.task_id_));
    }
  }

  SERVER_EVENT_ADD("ddl", "finish redef table",
    "tenant_id", finish_redef_arg.tenant_id_,
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", finish_redef_arg.task_id_,
    "snapshot_version", build_single_arg.snapshot_version_,
    "rpc_dst", rs_leader_addr,
    build_single_arg.ls_id_);
  LOG_INFO("finish redef table.", K(ret), "ddl_event_info", ObDDLEventInfo(), K(finish_redef_arg), K(build_single_arg), K(rs_leader_addr));
  return ret;
}

int ObDDLServerClient::build_ddl_single_replica_response(const obrpc::ObDDLBuildSingleReplicaResponseArg &arg)
{
  int ret = OB_SUCCESS;
  ObAddr rs_leader_addr;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_leader_addr))) {
    LOG_WARN("fail to rootservice address", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->to(rs_leader_addr).build_ddl_single_replica_response(arg))) {
    LOG_WARN("failed to finish redef table", K(ret), K(arg));
  }
  return ret;
}

int ObDDLServerClient::wait_task_reach_pending(
    const uint64_t tenant_id,
    const int64_t task_id,
    int64_t &snapshot_version,
    uint64_t &data_format_version,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  snapshot_version = 0;
  data_format_version = 0;
  const int64_t retry_interval = 100 * 1000;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(task_id <= 0 || OB_INVALID_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id));
    } else if (OB_FAIL(DDL_SIM(tenant_id, task_id, WAIT_REDEF_TASK_REACH_PENDING_FAILED))) {
      LOG_WARN("ddl sim failure", K(ret), K(tenant_id), K(task_id));
    } else {
      while (OB_SUCC(ret)) {
        share::ObDDLTaskStatus task_status = share::ObDDLTaskStatus::PREPARE;
        if (OB_FAIL(ObDDLUtil::get_data_information(tenant_id, task_id, data_format_version,
            snapshot_version, task_status))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_ENTRY_NOT_EXIST;
            ObAddr unused_addr;
            int64_t forward_user_msg_len = 0;
            ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
            if (OB_SUCCESS == ObDDLErrorMessageTableOperator::get_ddl_error_message(
                              tenant_id, task_id, -1 /*target_object_id*/,
                              unused_addr, false/*is_ddl_retry_task*/,
                              *GCTX.sql_proxy_, error_message, forward_user_msg_len)) {
              if (OB_SUCCESS != error_message.ret_code_) {
                ret = error_message.ret_code_;
              }
            }
            LOG_WARN("ddl task execute end", K(ret));
          } else {
            LOG_WARN("get information failed", K(ret), K(tenant_id), K(task_id));
          }
        } else if (rootserver::ObTableRedefinitionTask::check_task_status_is_pending(task_status)) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDDLServerClient::heart_beat_clear(const int64_t task_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.remove_register_task_id(task_id, tenant_id))) {
    LOG_WARN("failed to remove register task id", K(ret), K(task_id));
  }
  return ret;
}

int ObDDLServerClient::check_need_stop(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_tenant_dropped = false;
  bool is_tenant_standby = false;
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(ObDDLUtil::check_tenant_status_normal(GCTX.sql_proxy_, tenant_id))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == tmp_ret) {
      ret = OB_TENANT_HAS_BEEN_DROPPED;
      LOG_WARN("tenant has been dropped", K(ret), K(tenant_id));
    } else if (OB_STANDBY_READ_ONLY == tmp_ret) {
      ret = OB_STANDBY_READ_ONLY;
      LOG_WARN("tenant is standby now, stop wait", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (observer::ObServer::get_instance().is_stopped()) {
    ret = OB_TIMEOUT;
    LOG_WARN("server is stopping", K(ret), K(tenant_id));
  }
  return ret;
}


}  // end of namespace storage
}  // end of namespace oceanbase

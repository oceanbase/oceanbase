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

#include "ob_disaster_recovery_task_mgr.h"
#include "ob_disaster_recovery_task.h"
#include "ob_disaster_recovery_worker.h"
#include "ob_disaster_recovery_info.h"
#include "ob_rs_event_history_table_operator.h"
#include "observer/ob_inner_sql_connection.h"
#include "rootserver/ob_root_utils.h"
#include "share/ob_unit_table_operator.h"
#include "src/rootserver/ob_root_utils.h"
#include "share/ob_all_server_tracer.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace transaction::tablelock;
using namespace share;

namespace rootserver
{

#define FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY                            \
  for (int64_t i = 0; i < dr_tasks_.count(); ++i) {                           \
    ObDRTask *task = dr_tasks_.at(i);                                         \
    if (OB_NOT_NULL(task)) {                                                  \
      task->~ObDRTask();                                                      \
    }                                                                         \
  }                                                                           \
  task_alloc_.reset();                                                        \
  dr_tasks_.reset();                                                          \

#define COMMIT_DISASTER_RECOVERY_MGR_TRANS                                   \
  if (trans.is_started()) {                                                  \
    int tmp_ret = OB_SUCCESS;                                                \
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {                 \
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));              \
      ret = OB_SUCC(ret) ? tmp_ret : ret;                                    \
    }                                                                        \
  }                                                                          \

static const char* ls_replica_parallel_migration_mode[] = {
  "AUTO",
  "ON",
  "OFF"
};

const char* ObParallelMigrationMode::get_mode_str() const {
  STATIC_ASSERT(ARRAYSIZEOF(ls_replica_parallel_migration_mode) == (int64_t)MAX,
                "ls_replica_parallel_migration_mode string array size mismatch enum ParallelMigrationMode count");
  const char *str = NULL;
  if (mode_ >= AUTO && mode_ < MAX) {
    str = ls_replica_parallel_migration_mode[static_cast<int64_t>(mode_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ParallelMigrationMode", K_(mode));
  }
  return str;
}

int64_t ObParallelMigrationMode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(mode), "mode", get_mode_str());
  J_OBJ_END();
  return pos;
}

int ObParallelMigrationMode::parse_from_string(const ObString &mode)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(ls_replica_parallel_migration_mode) == (int64_t)MAX,
                "ls_replica_parallel_migration_mode string array size mismatch enum ParallelMigrationMode count");
  for (int64_t i = 0; i < ARRAYSIZEOF(ls_replica_parallel_migration_mode) && !found; i++) {
    if (0 == mode.case_compare(ls_replica_parallel_migration_mode[i])) {
      mode_ = static_cast<ParallelMigrationMode>(i);
      found = true;
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse type from string", KR(ret), K(mode), K_(mode));
  }
  return ret;
}

ObDRTaskMgr::ObDRTaskMgr(const int64_t service_epoch, const uint64_t tenant_id)
  : service_epoch_(service_epoch),
    tenant_id_(tenant_id),
    dr_tasks_(),
    task_alloc_("ObDRTaskMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    table_operator_()
{
}

ObDRTaskMgr::~ObDRTaskMgr()
{
  FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY
}

int ObDRTaskMgr::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_) || is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_id is invalid", KR(ret), K_(tenant_id));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_META_DISASTER_RECOVERY_POP_AND_EXECUTE_TASK);
ERRSIM_POINT_DEF(ERRSIM_DISASTER_RECOVERY_POP_AND_EXECUTE_TASK);
int ObDRTaskMgr::try_pop_and_execute_task(
    const uint64_t table_tenant_id)
{
  // process all tasks in the table (table_tenant_id)
  FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(ERRSIM_DISASTER_RECOVERY_POP_AND_EXECUTE_TASK)) {
    // for test, return success, skip pop task
    LOG_INFO("errsim disaster recovery pop and execute task", KR(ret));
  } else if (is_meta_tenant(table_tenant_id) && OB_UNLIKELY(ERRSIM_META_DISASTER_RECOVERY_POP_AND_EXECUTE_TASK)) {
    LOG_INFO("errsim meta tenant disaster recovery pop and execute task", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE task_status = '%s' "
                                    " ORDER BY priority ASC, gmt_create ASC",
                                    share::OB_ALL_LS_REPLICA_TASK_TNAME,
                                    ObDRLSReplicaTaskStatus(ObDRLSReplicaTaskStatus::WAITING).get_status_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(table_tenant_id));
  } else if (OB_FAIL(table_operator_.load_task_from_inner_table(*GCTX.sql_proxy_, table_tenant_id, sql, task_alloc_, dr_tasks_))) {
    LOG_WARN("failed to load task from inner table", KR(ret), K(table_tenant_id), K(sql));
  }
  if (OB_FAIL(ret) || is_zero_row(dr_tasks_.count())) {
    // skip
    LOG_TRACE("skip execute task", KR(ret), "count", dr_tasks_.count());
  } else if (OB_FAIL(check_and_set_parallel_migrate_task_())) {
    LOG_WARN("failed to check tenant enable_parallel_migration", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    // ignore ret code for isolation different task
    for (int64_t i = 0; OB_SUCC(ret) && i < dr_tasks_.count(); ++i) {
      ObDRTask *task = dr_tasks_.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is null ptr", KR(ret), KP(task));
      } else if (OB_TMP_FAIL(execute_task_(*task))) {
        LOG_WARN("failed to execute dr task", KR(ret), KR(tmp_ret), K(*task));
      }
    }
  }
  FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISASTER_RECOVERY_CLEAN_TASK);
int ObDRTaskMgr::try_clean_and_cancel_task(
    const uint64_t table_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_DISASTER_RECOVERY_CLEAN_TASK)) {
    // for test, return success, skip clean task
    LOG_INFO("errsim disaster recovery clean task", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE task_status = '%s'",
                                share::OB_ALL_LS_REPLICA_TASK_TNAME,
                                ObDRLSReplicaTaskStatus(ObDRLSReplicaTaskStatus::INPROGRESS).get_status_str()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(table_tenant_id));
    } else if (OB_FAIL(table_operator_.load_task_from_inner_table(*GCTX.sql_proxy_, table_tenant_id, sql, task_alloc_, dr_tasks_))) {
      LOG_WARN("failed to load task from inner table", KR(ret), K(table_tenant_id),K(sql));
    } else if (OB_FAIL(check_clean_and_cancel_task_())) {
      LOG_WARN("failed to do clean task", KR(ret), K(table_tenant_id));
    }
    FREE_DISASTER_RECOVERY_MANAGER_TASK_MEMORY
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISASTER_RECOVERY_SKIP_SEND_TASK_RPC);
int ObDRTaskMgr::execute_task_(
    ObDRTask &task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDRTaskRetComment ret_comment = ObDRTaskRetComment::MAX;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else {
    DEBUG_SYNC(BEFORE_SEND_DRTASK_RPC);
    task.log_execute_start();
    if (OB_FAIL(check_befor_execute_dr_task_(task, ret_comment))) {
      LOG_WARN("failed to check before execute dr task", KR(ret), K(task));
    } else if (OB_FAIL(update_task_schedule_status_(task))) {
      LOG_WARN("failed to update task status", KR(ret), K(task));
    } else if (OB_UNLIKELY(ERRSIM_DISASTER_RECOVERY_SKIP_SEND_TASK_RPC)) {
      LOG_INFO("errsim disaster recovery skip send task rpc");
    } else if (OB_FAIL(task.execute(*GCTX.srv_rpc_proxy_, ret_comment))) {
      // task can only be executed after task status update success
      LOG_WARN("fail to execute task", KR(ret), K(task));
    } else {
      LOG_INFO("succeed to execute task", K(task));
    }
    // if a task execute failed, it should be cleaned up for any reason.
    // avoid duplicate scheduling tasks.
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to execute task", KR(ret), KR(tmp_ret), K(task));
      if (OB_TMP_FAIL(DisasterRecoveryUtils::record_history_and_clean_task(task, ret, ret_comment))) {
        LOG_WARN("clean task while task execute failed", KR(ret), KR(tmp_ret), K(task));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISASTER_RECOVERY_EXECUTE_TASK_ERROR);
int ObDRTaskMgr::check_befor_execute_dr_task_(
    const ObDRTask &task,
    ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  const ObAddr &dst_server = task.get_dst_server();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_UNLIKELY(ERRSIM_DISASTER_RECOVERY_EXECUTE_TASK_ERROR)) {
    ret = ERRSIM_DISASTER_RECOVERY_EXECUTE_TASK_ERROR;
    LOG_WARN("errsim disaster recovery check task", KR(ret));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator is null", KR(ret), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(dst_server, server_info))) {
    LOG_WARN("fail to get server_info", KR(ret), K(dst_server));
  } else if (!server_info.is_alive()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_SERVER_NOT_ALIVE;
    LOG_WARN("dst server not alive", KR(ret), K(dst_server));
  } else if (OB_FAIL(task.check_before_execute(*GCTX.lst_operator_, ret_comment))) {
    LOG_WARN("fail to check before execute", KR(ret));
  }
  return ret;
}

int ObDRTaskMgr::update_task_schedule_status_(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[DRTASK_NOTICE] update task schedule status", K(task));
  ObSqlString sql;
  ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  int64_t service_epoch = OB_INVALID_ID;
  uint64_t service_epoch_tenant = OB_INVALID_TENANT_ID;
  uint64_t persistent_tenant = OB_INVALID_TENANT_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(DisasterRecoveryUtils::get_service_epoch_value_to_check(task, tenant_id_, service_epoch_, service_epoch))) {
    LOG_WARN("failed to get service epoch value to check", KR(ret), K(task), K_(tenant_id), K_(service_epoch));
  } else if (OB_FAIL(DisasterRecoveryUtils::get_service_epoch_and_persistent_tenant(
                                              task.get_tenant_id(),
                                              task.get_disaster_recovery_task_type(),
                                              service_epoch_tenant,
                                              persistent_tenant))) {
    LOG_WARN("failed to get service epoch and persistent tenant id", KR(ret), K(task));
  } else if (false == task.get_task_id().to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), K(task));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, persistent_tenant))) {
    LOG_WARN("failed to start trans", KR(ret), K(task), K(persistent_tenant));
  } else if (OB_FAIL(DisasterRecoveryUtils::lock_service_epoch(trans, service_epoch_tenant, service_epoch))) {
    LOG_WARN("failed to lock server epoch", KR(ret), K(task), K(service_epoch_tenant), K(service_epoch));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET task_status = '%s', schedule_time = now() WHERE tenant_id = %lu AND ls_id = %lu "
                          "AND task_type = '%s' AND task_id = '%s' AND task_status = '%s'",
                          share::OB_ALL_LS_REPLICA_TASK_TNAME,
                          ObDRLSReplicaTaskStatus(ObDRLSReplicaTaskStatus::INPROGRESS).get_status_str(),
                          task.get_tenant_id(),
                          task.get_ls_id().id(),
                          ob_disaster_recovery_task_type_strs(task.get_disaster_recovery_task_type()),
                          task_id_to_set,
                          ObDRLSReplicaTaskStatus(ObDRLSReplicaTaskStatus::WAITING).get_status_str()))) {
    LOG_WARN("assign sql string failed", KR(ret), K(task));
  } else if (OB_FAIL(trans.write(persistent_tenant, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql), K(task), K(persistent_tenant));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  }
  COMMIT_DISASTER_RECOVERY_MGR_TRANS
  return ret;
}

int ObDRTaskMgr::check_and_set_parallel_migrate_task_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dr_tasks_.count(); ++i) {
      bool enable_parallel_migration = false;
      ObDRTask *task = dr_tasks_.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is null ptr", KR(ret), KP(task));
      } else if (ObDRTaskType::LS_MIGRATE_REPLICA != task->get_disaster_recovery_task_type()) {
        // skip
      } else if (OB_FAIL(DisasterRecoveryUtils::check_tenant_enable_parallel_migration(task->get_tenant_id(), enable_parallel_migration))) {
        LOG_WARN("failed to check tenant enable_parallel_migration", KR(ret), KPC(task));
      } else if (!enable_parallel_migration) {
        // skip
        LOG_TRACE("enable_parallel_migration is false", K_(tenant_id), KPC(task), K(enable_parallel_migration));
      } else {
        ObMigrateLSReplicaTask *migrate_task = static_cast<ObMigrateLSReplicaTask*>(task);
        if (OB_ISNULL(migrate_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("migrate_task is nullptr", KR(ret), K(*task));
        } else {
          migrate_task->set_prioritize_same_zone_src(true);
          LOG_INFO("task has parallel migrate task", K(*task));
        }
      }
    }
  }
  return ret;
}

int ObDRTaskMgr::check_clean_and_cancel_task_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_cleaning = false;
  bool need_cancel = false;
  ObDRTaskRetComment ret_comment = ObDRTaskRetComment::MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < dr_tasks_.count(); ++i) {
    // ignore ret code for isolation different task
    need_cleaning = false;
    need_cancel = false;
    ret_comment = ObDRTaskRetComment::MAX;
    ObDRTask *task = dr_tasks_.at(i);
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is nullptr", KR(ret), KP(task));
    } else {
      if (task->get_task_status().is_inprogress_status() && OB_TMP_FAIL(check_task_need_cleaning_(*task, need_cleaning, ret_comment))) {
        LOG_WARN("fail to check task need cleaning", KR(tmp_ret), KP(task));
      }
      // ignore tmp_ret
      if (!need_cleaning && OB_TMP_FAIL(check_cleanup_tasks_for_cross_az_dr_(*task, need_cleaning, ret_comment))) {
        LOG_WARN("failed to cleanup task for cross az dr", KR(tmp_ret), KP(task));
      }
      if (need_cleaning) {
        LOG_INFO("[DRTASK_NOTICE] need clean invalid dr task", KPC(task));
        if (OB_TMP_FAIL(DisasterRecoveryUtils::record_history_and_clean_task(*task, OB_LS_REPLICA_TASK_RESULT_UNCERTAIN, ret_comment))) {
          LOG_WARN("failed to remove task and record task history", KR(tmp_ret), KP(task));
        }
      } else if (task->get_task_status().is_inprogress_status() && OB_TMP_FAIL(check_need_cancel_migrate_task_(*task, need_cancel))) {
        LOG_WARN("fail to check need cancel migrate task", KR(tmp_ret), KP(task));
      } else if (need_cancel) {
        LOG_INFO("[DRTASK_NOTICE] need cancel migrate task in schedule list", KPC(task));
        if (OB_TMP_FAIL(DisasterRecoveryUtils::send_rpc_to_cancel_task(*task))) {
          LOG_WARN("fail to send rpc to cancel migrate task", KR(tmp_ret), KP(task));
        }
      }
    } // end else
  }
  return ret;
}

int ObDRTaskMgr::check_cleanup_tasks_for_cross_az_dr_(
    const ObDRTask &task,
    bool &need_cleanning,
    ObDRTaskRetComment &ret_comment)
{
  // When the LS needs to perform a replace replica task, other invalid DR tasks need to be cleaned up,
  // regardless of whether the task status INPROGRESS a or WAITING.
  int ret = OB_SUCCESS;
  need_cleanning = false;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (ObDRTaskType::LS_REPLACE_REPLICA == task.get_disaster_recovery_task_type()) {
    // skip
  } else if (!ObRootUtils::is_dr_replace_deployment_mode_match()) {
    LOG_TRACE("no need check for corss az dr");
  } else {
    common::ObZone dest_zone; // not used
    ObServerInfoInTable source_server; // not used
    palf::LogConfigVersion config_version; // not used
    ObDRWorker dr_worker;
    bool found_replace_task = false;
    ObLSStatusOperator ls_status_operator;
    ObLSStatusInfo ls_status_info;
    DRLSInfo dr_ls_info_with_flag(gen_user_tenant_id(task.get_tenant_id()), GCTX.schema_service_);
    if (OB_FAIL(ls_status_operator.get_ls_status_info(task.get_tenant_id(), task.get_ls_id(), ls_status_info, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to get ls status info", KR(ret), K(task), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(dr_worker.build_ls_info_for_cross_az_dr(ls_status_info, dr_ls_info_with_flag))) {
      LOG_WARN("fail to init dr log stream info", KR(ret), K(ls_status_info));
    } else if (OB_FAIL(dr_worker.check_ls_single_replica_dr_tasks(dr_ls_info_with_flag,
                                                                  config_version,
                                                                  source_server,
                                                                  dest_zone,
                                                                  found_replace_task))) {
      LOG_WARN("fail to check ls single replica tasks", KR(ret), K(dr_ls_info_with_flag));
    } else if (found_replace_task) {
      need_cleanning = true;
      ret_comment = ObDRTaskRetComment::CANNOT_EXECUTE_DUE_TO_NEED_GENERATE_REPLACE_REPLACA_TASK;
      LOG_INFO("need clean for corss az dr has replace replica task");
    }
  }
  return ret;
}

int ObDRTaskMgr::check_task_need_cleaning_(
    const ObDRTask &task,
    bool &need_cleanning,
    ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  need_cleanning = false;
  Bool task_exist = false;
  const ObAddr &dst_server = task.get_dst_server();
  share::ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(dst_server, server_info))) {
    LOG_WARN("fail to get server_info", KR(ret), "server", dst_server);
    // case 1. server not exist
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_cleanning = true;
      ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_SERVER_NOT_EXIST;
      FLOG_INFO("[DRTASK_NOTICE] the reason to clean this task: server not exist", K(task));
    }
  } else if (server_info.is_permanent_offline()) {
    // case 2. server is permanant offline
    need_cleanning = true;
    ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_SERVER_PERMANENT_OFFLINE;
    FLOG_INFO("[DRTASK_NOTICE] the reason to clean this task: server permanent offline", K(task), K(server_info));
  } else if (server_info.is_alive()) {
    // case 3. rpc ls_check_dr_task_exist successfully told us task not exist
    ObDRTaskExistArg arg;
    arg.task_id_ = task.get_task_id();
    arg.tenant_id_ = task.get_tenant_id();
    arg.ls_id_ = task.get_ls_id();
    if (OB_FAIL(GCTX.srv_rpc_proxy_->to(task.get_dst_server()).by(task.get_tenant_id()).ls_check_dr_task_exist(arg, task_exist))) {
      LOG_WARN("fail to check task exist", KR(ret), K(task));
    } else if (!task_exist) {
      need_cleanning = true;
      ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_TASK_NOT_RUNNING;
      FLOG_INFO("[DRTASK_NOTICE] the reason to clean this task: task not running", K(task));
    }
  } else if (server_info.is_temporary_offline()) {
    ret = OB_SERVER_NOT_ALIVE;
    LOG_WARN("server status is not alive, task may be cleanned later", KR(ret), K(server_info), K(task));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected server status", KR(ret), K(server_info), K(task));
  }
  if (OB_FAIL(ret) && task.is_already_timeout()) {
    // case 4. task is timeout while any OB_FAIL occurs
    ret = OB_SUCCESS;
    need_cleanning = true;
    ret_comment = ObDRTaskRetComment::CLEAN_TASK_DUE_TO_TASK_TIMEOUT;
    FLOG_INFO("[DRTASK_NOTICE] the reason to clean this task: task is timeout", KR(ret), K(task));
  }
  return ret;
}

int ObDRTaskMgr::check_need_cancel_migrate_task_(
    const ObDRTask &task,
    bool &need_cancel)
{
  int ret = OB_SUCCESS;
  need_cancel = false;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dr task", KR(ret), K(task));
  } else if (ObDRTaskType::LS_MIGRATE_REPLICA != task.get_disaster_recovery_task_type()) {
    LOG_TRACE("skip, task is not a migration task", K(task));
  } else if ((0 == task.get_comment().case_compare(drtask::MIGRATE_REPLICA_DUE_TO_UNIT_NOT_MATCH)
           || 0 == task.get_comment().case_compare(drtask::REPLICATE_REPLICA))) {
    // only surpport cancel unit not match migration task
    // not include manual migration tasks
    bool dest_server_has_unit = false;
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(task.get_tenant_id()), tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(task));
    } else if (!(tenant_data_version >= DATA_VERSION_4_3_3_0 || (tenant_data_version >= MOCK_DATA_VERSION_4_2_3_0 && tenant_data_version < DATA_VERSION_4_3_0_0))) {
      need_cancel = false;
      LOG_INFO("tenant data_version is not match", KR(ret), K(task), K(tenant_data_version));
    } else if (OB_FAIL(check_tenant_has_unit_in_server_(task.get_tenant_id(), task.get_dst_server(), dest_server_has_unit))) {
      LOG_WARN("fail to check tenant has unit in server", KR(ret), K(task));
    } else if (!dest_server_has_unit) {
      need_cancel = true;
      FLOG_INFO("[DRTASK_NOTICE] need cancel migrate task", KR(ret), K(need_cancel), K(task));
    }
  }
  return ret;
}

int ObDRTaskMgr::check_tenant_has_unit_in_server_(
    const uint64_t tenant_id,
    const common::ObAddr &server_addr,
    bool &has_unit)
{
  int ret = OB_SUCCESS;
  has_unit = false;
  share::ObUnitTableOperator unit_operator;
  common::ObArray<share::ObUnit> unit_info_array;
  if (OB_UNLIKELY(!server_addr.is_valid() || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_addr), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("unit operator init failed", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(gen_user_tenant_id(tenant_id), unit_info_array))) {
    LOG_WARN("fail to get unit info array", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_unit && i < unit_info_array.count(); ++i) {
      if (unit_info_array.at(i).server_ == server_addr) {
        has_unit = true;
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase

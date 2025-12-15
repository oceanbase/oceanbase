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
#include "ob_ls_migration_handler.h"
#include "ob_ls_migration.h"
#include "ob_ls_prepare_migration.h"
#include "ob_ls_complete_migration.h"
#include "ob_storage_ha_service.h"
#include "rootserver/ob_disaster_recovery_task_utils.h"
#include "share/ob_io_device_helper.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "ob_rebuild_service.h"
#include "observer/omt/ob_tenant.h"
#include "ob_ha_rebuild_tablet.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/high_availability/ob_ss_ls_migration.h"
#endif

namespace oceanbase
{
using namespace share;
namespace storage
{
ERRSIM_POINT_DEF(EN_MIGRATION_INIT_STATUS_FAIL);
ERRSIM_POINT_DEF(EN_ENABLE_MIGRATION_HANDLER_SCHEDULE_SERVER_EVENT);

/******************ObLSMigrationHandlerStatusHelper*********************/
bool ObLSMigrationHandlerStatusHelper::is_valid(
    const ObLSMigrationHandlerStatus &status)
{
  return status >= ObLSMigrationHandlerStatus::INIT && status < ObLSMigrationHandlerStatus::MAX_STATUS;
}

int ObLSMigrationHandlerStatusHelper::get_next_change_status(
    const ObLSMigrationHandlerStatus &curr_status,
    const int32_t result,
    ObLSMigrationHandlerStatus &next_status)
{
  int ret = OB_SUCCESS;
  next_status = ObLSMigrationHandlerStatus::MAX_STATUS;

  if (!is_valid(curr_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next change status get invalid argument", K(ret), K(curr_status));
  } else {
    switch (curr_status) {
    case ObLSMigrationHandlerStatus::INIT: {
      if (OB_SUCCESS != result) {
        next_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
      } else {
        next_status = ObLSMigrationHandlerStatus::PREPARE_LS;
      }
      break;
    }
    case ObLSMigrationHandlerStatus::PREPARE_LS: {
      next_status = ObLSMigrationHandlerStatus::WAIT_PREPARE_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::WAIT_PREPARE_LS: {
      if (OB_SUCCESS != result) {
        next_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
      } else {
        next_status = ObLSMigrationHandlerStatus::BUILD_LS;
      }
      break;
    }
    case ObLSMigrationHandlerStatus::BUILD_LS: {
      next_status = ObLSMigrationHandlerStatus::WAIT_BUILD_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::WAIT_BUILD_LS: {
      next_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::COMPLETE_LS: {
      next_status = ObLSMigrationHandlerStatus::WAIT_COMPLETE_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::WAIT_COMPLETE_LS: {
      next_status = ObLSMigrationHandlerStatus::FINISH;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid curr status for fail", K(ret), K(curr_status));
    }
    }
  }
  return ret;
}

//TODO(jyx441808): add unit test for this function
const char *ObLSMigrationHandlerStatusHelper::get_status_str(
    const ObLSMigrationHandlerStatus &status)
{
  static const char *status_strs[] = {
    "INIT",
    "PREPARE_LS",
    "WAIT_PREPARE_LS",
    "BUILD_LS",
    "WAIT_BUILD_LS",
    "COMPLETE_LS",
    "WAIT_COMPLETE_LS",
    "FINISH"
  };
  STATIC_ASSERT(ARRAYSIZEOF(status_strs) == (int64_t)ObLSMigrationHandlerStatus::MAX_STATUS,
                "ls_migration_handler_status string array size mismatch enum ObLSMigrationHandlerStatus count");

  const char *str = "UNKNOWN";

  if (status < ObLSMigrationHandlerStatus::INIT || status >= ObLSMigrationHandlerStatus::MAX_STATUS) {
    str = "UNKNOWN";
  } else {
    str = status_strs[static_cast<int>(status)];
  }
  return str;
}


/******************ObLSMigrationTask*********************/
ObLSMigrationTask::ObLSMigrationTask()
  : arg_(),
    task_id_()
{
}

ObLSMigrationTask::~ObLSMigrationTask()
{
}

void ObLSMigrationTask::reset()
{
  arg_.reset();
  task_id_.reset();
}

bool ObLSMigrationTask::is_valid() const
{
  return arg_.is_valid()
      && !task_id_.is_invalid();
}

/******************ObLSMigrationHandler*********************/
ObLSMigrationHandler::ObLSMigrationHandler()
  : is_inited_(false),
    ls_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr),
    start_ts_(0),
    finish_ts_(0),
    task_list_(),
    lock_(),
    status_(ObLSMigrationHandlerStatus::INIT),
    result_(OB_SUCCESS),
    is_stop_(false),
    is_cancel_(false),
    chosen_src_(),
    is_complete_(false),
    is_dag_net_cleared_(true) // default is true, set to false before generate dag net
#ifdef OB_BUILD_SHARED_STORAGE
    , switch_leader_cond_()
    , switch_leader_cnt_(0)
    , leader_proposal_id_(0)
#endif
{
}

ObLSMigrationHandler::~ObLSMigrationHandler()
{
}

int ObLSMigrationHandler::init(
    ObLS *ls,
    common::ObInOutBandwidthThrottle *bandwidth_throttle,
    obrpc::ObStorageRpcProxy *svr_rpc_proxy,
    storage::ObStorageRpc *storage_rpc,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls migration handler init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls migration handler init get invalid argument", K(ret), KP(ls));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (OB_FAIL(switch_leader_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init switch leader thread cond", K(ret));
#endif
  } else {
    ls_ = ls;
    bandwidth_throttle_ = bandwidth_throttle;
    svr_rpc_proxy_ = svr_rpc_proxy;
    storage_rpc_ = storage_rpc;
    sql_proxy_ = sql_proxy;
    is_dag_net_cleared_ = true;
    is_inited_ = true;
  }
  return ret;
}

int ObLSMigrationHandler::get_ls_migration_handler_status_(ObLSMigrationHandlerStatus &status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    status = status_;
  }
  return ret;
}

int ObLSMigrationHandler::check_task_list_empty_(bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    const int64_t count = task_list_.count();
    if (count > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls migration task count should not more than 1", K(ret), K(count));
    } else {
      is_empty = 0 == count;
    }
  }
  return ret;
}

int ObLSMigrationHandler::set_result(const int32_t result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_SUCCESS == result_ && OB_SUCCESS != result) {
      FLOG_INFO("set first error result", K(result));
      result_ = result;
    }
  }
  return ret;
}

int ObLSMigrationHandler::get_result_(int32_t &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    result = result_;
  }
  return ret;
}

bool ObLSMigrationHandler::is_migration_failed_() const
{
  common::SpinRLockGuard guard(lock_);
  return OB_SUCCESS != result_;
}

void ObLSMigrationHandler::reuse_()
{
  common::SpinWLockGuard guard(lock_);
  start_ts_ = 0;
  finish_ts_ = 0;
  task_list_.reset();
  status_ = ObLSMigrationHandlerStatus::INIT;
  result_ = OB_SUCCESS;
  is_cancel_ = false;
  is_complete_ = false;
  is_dag_net_cleared_ = true;
  chosen_src_.reset();
}

void ObLSMigrationHandler::wakeup_()
{
  int ret = OB_SUCCESS;
  ObStorageHAService *storage_ha_service = MTL(ObStorageHAService*);
  if (OB_ISNULL(storage_ha_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage ha handler service should not be NULL", K(ret), KP(storage_ha_service));
  } else {
    storage_ha_service->wakeup();
  }
}

int ObLSMigrationHandler::get_ls_migration_task_(ObLSMigrationTask &task)
{
  int ret = OB_SUCCESS;
  task.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(get_ls_migration_task_with_nolock_(task))) {
      LOG_WARN("failed to get ls migration task", K(ret));
    }
  }
  return ret;
}

int ObLSMigrationHandler::check_task_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObLSMigrationTask ls_migration_task;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task should not be invalid", K(ret), K(ls_migration_task));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KPC(ls_));
  } else if (OB_FAIL(scheduler->check_dag_net_exist(ls_migration_task.task_id_, is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret));
  }

  return ret;
}

int ObLSMigrationHandler::handle_current_task_(
    bool &need_wait,
    int32_t &task_result)
{
  int ret = OB_SUCCESS;
  need_wait = false;
  task_result = OB_SUCCESS;
  bool is_exist = false;
  bool is_migration_failed = false;
  ObLSMigrationTask ls_migration_task;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(check_task_exist_(is_exist))) {
    LOG_WARN("failed to check task exist", K(ret));
  } else if (OB_FAIL(get_result_(task_result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (FALSE_IT(is_migration_failed = (OB_SUCCESS != task_result))) {
  }
  // the order between erasing dag net from map and clearing dag net is not guaranteed (when start_running failed)
  // therefore, only when dag net is not exist and dag net is cleared, state machine can be switched to next stage
  else if (is_exist || !is_dag_net_cleared()) {
    need_wait = true;
    if (is_migration_failed) {
      // if dag net is exist and migration is failed, cancel the migration task
      // migration task won't be cleared immediately, so we still need to wait
      if (OB_FAIL(cancel_current_task_())) {
        LOG_WARN("failed to cancel current task", K(ret), KPC(ls_));
      }
    }
  } else {
    // if dag net is not exist, no need to wait
    need_wait = false;
  }
  return ret;
}

int ObLSMigrationHandler::cancel_current_task_() {
  int ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;
  ObTenantDagScheduler *scheduler = nullptr;

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KPC(ls_));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task should not be invalid", K(ret), K(ls_migration_task));
  } else if (OB_FAIL(scheduler->cancel_dag_net(ls_migration_task.task_id_))) {
    LOG_WARN("failed to cancel dag net", K(ret), K(this), K(ls_migration_task));
  } else {
    common::SpinWLockGuard guard(lock_);
    is_cancel_ = true;
  }

  return ret;
}

int ObLSMigrationHandler::add_ls_migration_task(
    const share::ObTaskId &task_id, const ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (task_id.is_invalid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add ls migration task get invalid argument", K(ret), K(task_id), K(arg));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (!task_list_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls already has migration task", K(ret), K(task_list_), K(arg), K(task_id));
    } else if (is_stop_) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("ls migration handler is int stop status", K(ret), K(task_id), K(arg));
    } else {
      ObLSMigrationTask task;
      task.task_id_ = task_id;
      task.arg_ = arg;
      if (OB_FAIL(task_list_.push_back(task))) {
        LOG_WARN("failed to push task into list", K(ret), K(task));
      } else {
        wakeup_();
      }
    }
  }
  return ret;
}

int ObLSMigrationHandler::switch_next_stage(const int32_t result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(switch_next_stage_with_nolock_(result))) {
      LOG_WARN("failed to switch next stage", K(ret), K(result));
    }
  }
  return ret;
}

int ObLSMigrationHandler::check_task_exist(const share::ObTaskId &task_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObLSMigrationTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check task exist get invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
    }
  } else if (task_id == task.task_id_) {
    is_exist = true;
  } else {
    is_exist = false;
  }
  return ret;
}

int ObLSMigrationHandler::get_migration_task_and_handler_status(
    ObLSMigrationTask &task,
    ObLSMigrationHandlerStatus &status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (task_list_.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      task = task_list_.at(0);
      status = status_;
    }
  }
  return ret;
}

void ObLSMigrationHandler::destroy()
{
  if (is_inited_) {
    ls_ = nullptr;
    bandwidth_throttle_ = nullptr;
    svr_rpc_proxy_ = nullptr;
    storage_rpc_ = nullptr;
    sql_proxy_ = nullptr;

    start_ts_ = 0;
    finish_ts_ = 0;
    task_list_.reset();
    is_inited_ = false;
  }
}

int ObLSMigrationHandler::process()
{
  int ret = OB_SUCCESS;
  ObLSMigrationHandlerStatus status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_handler_status_(status))) {
    LOG_WARN("failed to get ls migration handler status", K(ret));
  } else {
#ifdef ERRSIM
    if (OB_SUCCESS != EN_ENABLE_MIGRATION_HANDLER_SCHEDULE_SERVER_EVENT) {
      SERVER_EVENT_SYNC_ADD("storage_ha", "migration_handler_schedule_server_event",
        "tenant_id", ls_->get_tenant_id(),
        "ls_id", ls_->get_ls_id().id());
    }
#endif

    switch (status) {
    case ObLSMigrationHandlerStatus::INIT: {
      if (OB_FAIL(do_init_status_())) {
        LOG_WARN("failed to do init status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::PREPARE_LS: {
      if (OB_FAIL(do_prepare_ls_status_())) {
        LOG_WARN("failed to do prepare ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::BUILD_LS: {
      if (OB_FAIL(do_build_ls_status_())) {
        LOG_WARN("failed to do build ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::COMPLETE_LS: {
      if (OB_FAIL(do_complete_ls_status_())) {
        LOG_WARN("failed to do complete ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::FINISH: {
      if (OB_FAIL(do_finish_status_())) {
        LOG_WARN("failed to do finish status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::WAIT_PREPARE_LS:
    case ObLSMigrationHandlerStatus::WAIT_BUILD_LS:
    case ObLSMigrationHandlerStatus::WAIT_COMPLETE_LS: {
      if (OB_FAIL(do_wait_status_())) {
        LOG_WARN("failed to do wait status", K(ret), K(status));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(status));
    }
    }
  }
  return ret;
}

int ObLSMigrationHandler::cancel_task(const share::ObTaskId &task_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handle do not init", K(ret));
  } else if (!task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(check_task_exist_with_nolock_(task_id, is_exist))) {
      LOG_WARN("fail to check task exist", K(ret), K(task_id));
    } else if (!is_exist) {
      LOG_INFO("task is not exist in migration task", K(task_id));
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get ObTenantDagScheduler from MTL", K(ret));
    }
    // If task not exist, cancel_dag_net return OB_SUCCESS
    else if (OB_FAIL(scheduler->cancel_dag_net(task_id))) {
      LOG_WARN("failed to cancel dag net", K(ret), K(this), K(task_id));
    } else {
      is_cancel_ = true;
    }
  }
  return ret;
}

bool ObLSMigrationHandler::is_cancel() const
{
  common::SpinRLockGuard guard(lock_);
  return is_cancel_;
}

bool ObLSMigrationHandler::is_complete() const
{
  common::SpinRLockGuard guard(lock_);
  return is_complete_;
}

bool ObLSMigrationHandler::is_dag_net_cleared() const
{
  common::SpinRLockGuard guard(lock_);
  return is_dag_net_cleared_;
}

void ObLSMigrationHandler::set_dag_net_cleared()
{
  common::SpinWLockGuard guard(lock_);
  is_dag_net_cleared_ = true;
}

int ObLSMigrationHandler::do_init_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::INIT;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  bool is_empty = false;
  bool need_to_abort = false;

  DEBUG_SYNC(BEFORE_MIGRATION_DO_INIT_STATUS);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    // this lock make sure the ls creating is not scheduled to migrate.
    ObLSLockGuard lock_ls(ls_, true /* read lock */);
    start_ts_ = ObTimeUtil::current_time();
    if (OB_FAIL(ls_->get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), KPC(ls_));
    } else if (ls_->get_persistent_state().is_need_gc()) {
      // ls persistent state is not ha state, which means ls hasn't been completely created / failed to create
      // do nothing
      FLOG_INFO("ls persistent state is not ha state", K(migration_status), KPC(ls_));
#ifdef ERRSIM
      if (migration_status == ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE) {
        SERVER_EVENT_ADD("storage_ha", "migration_when_ls_is_initializing",
            "tenant_id", ls_->get_tenant_id(),
            "ls_id", ls_->get_ls_id().id(),
            "migration_status", migration_status);
      }
#endif
    } else if (OB_FAIL(check_task_list_empty_(is_empty))) {
      LOG_WARN("failed to check task list empty", K(ret), KPC(ls_));
    } else if (is_empty) {
      if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == migration_status
          || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
        //do nothing
      } else if (ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE_HOLD == migration_status) {
        ObMigrationStatus new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
        bool is_valid_member = false;
        if (OB_FAIL(ObStorageHADagUtils::check_self_is_valid_member_after_inc_config_version(ls_->get_ls_id(),
                                                                                             false /* with_leader */,
                                                                                             is_valid_member))) {
          LOG_WARN("failed check self valid member after inc config version", K(ret), K(migration_status));
        } else if (is_valid_member) {
          new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
          LOG_INFO("self is valid member after inc config version", "ls_id", ls_->get_ls_id(),
            K(migration_status), K(new_migration_status));
        } else if (OB_FAIL(ObMigrationStatusHelper::trans_fail_status(migration_status, new_migration_status))) {
          LOG_WARN("failed to trans fail status", K(ret), "ls_id", ls_->get_ls_id(), K(migration_status));
        } else {
          LOG_INFO("self is not valid member after inc config version", "ls_id", ls_->get_ls_id(),
            K(migration_status), K(new_migration_status));
        }

        if (FAILEDx(ls_->set_migration_status(new_migration_status, ls_->get_rebuild_seq()))) {
          LOG_WARN("failed to set migration status", K(ret), "ls_id", ls_->get_ls_id(), K(new_migration_status));
        }

        if (OB_SUCC(ret)) {
          SERVER_EVENT_ADD("storage_ha", "check_self_is_valid_member_after_inc_config_version",
                           "tenant_id", MTL_ID(),
                           "ls_id", ls_->get_ls_id().id(),
                           "current_migration_status", migration_status,
                           "new_migration_status", new_migration_status,
                           "ret", ret);
        }
      } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL != migration_status
          && ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL != migration_status
          && ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE_FAIL != migration_status
          && ObMigrationStatus::OB_MIGRATION_STATUS_GC != migration_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls migration handler in init status but ls migration status is not in failed status",
            K(ret), K(is_empty), K(migration_status), KPC(ls_));
      }
    }
    // migration task list is not empty, need to schedule migration
    else {
      if (OB_FAIL(check_need_to_abort_(need_to_abort))) {
        LOG_WARN("failed to check need to abort", K(ret));
      } else if (!need_to_abort) {
        ObLSMigrationTask task;
#ifdef ERRSIM
        if (OB_FAIL(EN_MIGRATION_INIT_STATUS_FAIL)) {
          STORAGE_LOG(ERROR, "[ERRSIM] fake EN_MIGRATION_INIT_STATUS_FAIL", K(ret));
        }
#endif
        if (FAILEDx(check_before_do_task_())) {
          LOG_WARN("failed to check before do task", K(ret), KPC(ls_));
        } else if (OB_FAIL(get_ls_migration_task_(task))) {
          LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
        } else {
          SERVER_EVENT_ADD("storage_ha", "ls_ha_start",
              "tenant_id", ls_->get_tenant_id(),
              "ls_id", ls_->get_ls_id().id(),
              "src", task.arg_.data_src_.get_server(),
              "dst", task.arg_.dst_.get_server(),
              "task_id", task.task_id_,
              "is_failed", OB_SUCCESS,
              ObMigrationOpType::get_str(task.arg_.type_));
          wakeup_();
        }
      }

      // INIT -> PREPARE_LS
      if (OB_TMP_FAIL(switch_next_stage(ret))) {
        LOG_WARN("failed to switch next stage", K(tmp_ret), K(ret), K(status));
      }
    }
  }

  return ret;
}

int ObLSMigrationHandler::do_prepare_ls_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::PREPARE_LS;
  bool need_to_abort = false;
  bool can_skip_prepare = false;
  int32_t result = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_MIGRATION_DO_PREPARE_LS_STATUS);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(check_need_to_abort_(need_to_abort))) {
    LOG_WARN("failed to change status", K(ret));
  } else if (!need_to_abort) {
    if (OB_FAIL(check_can_skip_prepare_status_(can_skip_prepare))) {
      LOG_WARN("failed to check can skip prepare status", K(ret));
    } else if (can_skip_prepare) {
      // skip generate prepare ls dag net
    } else if (OB_FAIL(generate_prepare_ls_dag_net_())) {
      LOG_WARN("failed to generate prepare ls dag net", K(ret), K(status), KPC(ls_));
    }
  }

  // PREPARE_LS -> WAIT_PREPARE_LS
  if (OB_TMP_FAIL(switch_next_stage(ret))) {
    LOG_WARN("failed to switch next stage", K(tmp_ret), K(ret), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::do_build_ls_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::BUILD_LS;
  bool need_to_abort = false;
  ObLSMigrationTask task;

  DEBUG_SYNC(BEFORE_MIGRATION_DO_BUILD_LS_STATUS);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(check_need_to_abort_(need_to_abort))) {
    LOG_WARN("failed to change status", K(ret));
  } else if (!need_to_abort) {
    if (OB_FAIL(get_ls_migration_task_(task))) {
      LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
    } else if (ObMigrationOpType::REBUILD_TABLET_OP == task.arg_.type_) {
      if (OB_FAIL(generate_build_tablet_dag_net_())) {
        LOG_WARN("failed to generate build tablet dag net", K(ret), KPC(ls_));
      }
    } else if (OB_FAIL(generate_build_ls_dag_net_())) {
      LOG_WARN("failed to generate build ls dag net", K(ret), K(status), KPC(ls_));
    }
  }

  // BUILD_LS -> WAIT_BUILD_LS
  if (OB_TMP_FAIL(switch_next_stage(ret))) {
    LOG_WARN("failed to switch next stage", K(tmp_ret), K(ret), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::do_complete_ls_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::COMPLETE_LS;
  const ObLSMigrationHandlerStatus retry_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
  bool can_skip_complete = false;
  ObLSMigrationTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (FALSE_IT(can_skip_complete = (ObMigrationOpType::REBUILD_TABLET_OP == task.arg_.type_))) {
  } else if (can_skip_complete) {
    // skip generate complete ls dag net
  } else if (OB_FAIL(generate_complete_ls_dag_net_())) {
    LOG_WARN("failed to generate complete ls dag net", K(ret), K(status), KPC(ls_));
  }

  if (is_complete() || can_skip_complete) {
    // COMPLETE_LS -> WAIT_COMPLETE_LS
    if (OB_TMP_FAIL(switch_next_stage(ret))) {
      LOG_WARN("failed to switch next stage", K(tmp_ret), K(ret), KPC(ls_));
    }
  }
  // else, COMPLETE_LS -> COMPLETE_LS
  // COMPLETE_LS stage can't skipped, complete ls migration dag net must be generated

  return ret;
}

int ObLSMigrationHandler::do_wait_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;
  bool need_wait = false;
  int32_t task_result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else if (OB_FAIL(handle_current_task_(need_wait, task_result))) {
    LOG_WARN("failed to handle current task", K(ret), KPC(ls_));
  } else if (need_wait) {
    // do nothing, won't switch status
  } else if (OB_FAIL(switch_next_stage(task_result))) {
    // WAIT_PREPARE_LS -> BUILD_LS
    // WAIT_BUILD_LS -> COMPLETE_LS
    // WAIT_COMPLETE_LS -> FINISH
    // if task_result!=OB_SUCCESS, switch to COMPLETE_LS (except for WAIT_COMPLETE_LS)
    LOG_WARN("failed to switch next stage", K(ret), K(task_result), KPC(ls_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(switch_next_stage(ret))) { // WAIT -> COMPLETE_LS (except for WAIT_COMPLETE_LS)
      LOG_WARN("failed to report result at wait status", K(tmp_ret), K(ret), K(status_));
    }
  }

  return ret;
}

int ObLSMigrationHandler::do_finish_status_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  ObLSMigrationTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (OB_FAIL(get_result_(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(report_result_())) {
    LOG_WARN("failed to report result", K(ret), KPC(ls_));
  } else {
    SERVER_EVENT_ADD("storage_ha", "ls_ha_finish",
        "tenant_id", ls_->get_tenant_id(),
        "ls_id", ls_->get_ls_id().id(),
        "src", task.arg_.src_.get_server(),
        "dst", task.arg_.dst_.get_server(),
        "task_id", task.task_id_,
        "is_failed", result,
        ObMigrationOpType::get_str(task.arg_.type_));

    if (ObMigrationOpType::REBUILD_LS_OP == task.arg_.type_ && OB_SUCCESS != result) {
      wakeup_();
    }
    finish_ts_ = ObTimeUtil::current_time();
    FLOG_INFO("do finish ls migration task", K(task), K(result), "cost_ts", finish_ts_ - start_ts_);
    reuse_();
  }
  return ret;
}

int ObLSMigrationHandler::generate_build_ls_dag_net_()
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else {
    common::SpinWLockGuard guard(lock_);
#ifdef ERRSIM
    const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
    const ObLSID errsim_ls_id(errsim_migration_ls_id);
    if (!is_tenant_sslog_ls(ls_->get_tenant_id(), ls_->get_ls_id()) && ls_->get_ls_id() == errsim_ls_id) {
      SERVER_EVENT_SYNC_ADD("storage_ha", "before_add_build_ls_dag_net",
                            "tenant_id", ls_->get_tenant_id(),
                            "ls_id", ls_->get_ls_id().id(),
                            "src", ls_migration_task.arg_.src_.get_server(),
                            "dst", ls_migration_task.arg_.dst_.get_server(),
                            "task_id", ls_migration_task.task_id_);
      DEBUG_SYNC(BEFORE_ADD_BUILD_LS_MIGRATION_DAG_NET);
    }
#endif
    if (OB_FAIL(ret)) {
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (is_shared_storage
        && (ObMigrationOpType::REPLACE_LS_OP == ls_migration_task.arg_.type_
          || ObMigrationOpType::ADD_LS_OP == ls_migration_task.arg_.type_
          || ObMigrationOpType::MIGRATE_LS_OP == ls_migration_task.arg_.type_)) {
      ObTenantDagScheduler *scheduler = nullptr;
      ObSSMigrationDagNetInitParam param;
      param.arg_ = ls_migration_task.arg_;
      param.task_id_ = ls_migration_task.task_id_;

      if (OB_FAIL(schedule_dag_net_<ObSSMigrationDagNet>(&param, true /* check_cancel */))) {
        LOG_WARN("failed to schedule build ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
      }
#endif
    } else {
      ObTenantDagScheduler *scheduler = nullptr;
      ObMigrationDagNetInitParam param;
      param.arg_ = ls_migration_task.arg_;
      param.task_id_ = ls_migration_task.task_id_;
      param.bandwidth_throttle_ = bandwidth_throttle_;
      param.storage_rpc_ = storage_rpc_;
      param.svr_rpc_proxy_ = svr_rpc_proxy_;
      param.sql_proxy_ = sql_proxy_;

      if (OB_FAIL(schedule_dag_net_<ObMigrationDagNet>(&param, true /* check_cancel */))) {
        LOG_WARN("failed to schedule build ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
      }
    }
  }
  return ret;
}

int ObLSMigrationHandler::generate_prepare_ls_dag_net_()
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObLSPrepareMigrationParam param;
    param.arg_ = ls_migration_task.arg_;
    param.task_id_ = ls_migration_task.task_id_;
#ifdef ERRSIM
    const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
    const ObLSID errsim_ls_id(errsim_migration_ls_id);
    if (!is_tenant_sslog_ls(ls_->get_tenant_id(), ls_->get_ls_id()) && ls_->get_ls_id() == errsim_ls_id) {
      SERVER_EVENT_SYNC_ADD("storage_ha", "before_add_prepare_ls_dag_net",
                            "tenant_id", ls_->get_tenant_id(),
                            "ls_id", ls_->get_ls_id().id(),
                            "src", ls_migration_task.arg_.src_.get_server(),
                            "dst", ls_migration_task.arg_.dst_.get_server(),
                            "task_id", ls_migration_task.task_id_);
      DEBUG_SYNC(BEFORE_ADD_PREPARE_LS_MIGRATION_DAG_NET);
    }
#endif
    if (OB_FAIL(schedule_dag_net_<ObLSPrepareMigrationDagNet>(&param, true /* check_cancel */))) {
      LOG_WARN("failed to schedule prepare ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
    }
  }
  return ret;
}

int ObLSMigrationHandler::generate_complete_ls_dag_net_()
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObLSCompleteMigrationParam param;
    param.arg_ = ls_migration_task.arg_;
    param.task_id_ = ls_migration_task.task_id_;
    param.rebuild_seq_ = ls_->get_rebuild_seq();
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.storage_rpc_ = storage_rpc_;
    param.chosen_src_ = chosen_src_;
    param.result_ = result_;
#ifdef ERRSIM
    const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
    const ObLSID errsim_ls_id(errsim_migration_ls_id);
    if (!is_tenant_sslog_ls(ls_->get_tenant_id(), ls_->get_ls_id()) && ls_->get_ls_id() == errsim_ls_id) {
      SERVER_EVENT_SYNC_ADD("storage_ha", "before_add_complete_ls_dag_net",
                            "tenant_id", ls_->get_tenant_id(),
                            "ls_id", ls_->get_ls_id().id(),
                            "src", ls_migration_task.arg_.src_.get_server(),
                            "dst", ls_migration_task.arg_.dst_.get_server(),
                            "task_id", ls_migration_task.task_id_);
      DEBUG_SYNC(BEFORE_ADD_COMPLETE_LS_MIGRATION_DAG_NET);
    }
#endif
    if (OB_FAIL(schedule_dag_net_<ObLSCompleteMigrationDagNet>(&param, false /* check_cancel */))) {
      LOG_WARN("failed to schedule complete ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
    } else {
      is_complete_ = true;
    }
  }
  return ret;
}

int ObLSMigrationHandler::report_result_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSMigrationTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else {

    if (OB_SUCCESS != (tmp_ret = report_meta_table_())) {
      LOG_WARN("failed to report meta table", K(ret), K(task), KPC(ls_));
    }

    if (OB_SUCCESS != (tmp_ret = inner_report_result_(task))) {
      LOG_WARN("failed to do inner report result", K(ret), K(task), KPC(ls_));
    }
  }
  return ret;
}


int ObLSMigrationHandler::report_meta_table_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_result_(result))) {
    LOG_WARN("failed ot get result", K(ret));
  } else if (OB_SUCCESS != result) {
    //do nothing
  } else if (OB_FAIL(ls_->report_replica_info())) {
    LOG_WARN("failed to report replica info", K(ret), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::inner_report_result_(
    const ObLSMigrationTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner report result get invalid argument", K(ret), K(task));
  } else if (ObMigrationOpType::REBUILD_LS_OP == task.arg_.type_
      || ObMigrationOpType::REBUILD_TABLET_OP == task.arg_.type_) {
    if (OB_FAIL(report_to_rebuild_service_())) {
      LOG_WARN("failed to report to rebuild service", K(ret), K(task));
    }
  } else if (OB_FAIL(report_to_disaster_recovery_())) {
    LOG_WARN("failed to report to disaster_recovery", KR(ret), K(task));
  }
  return ret;
}

int ObLSMigrationHandler::report_to_rebuild_service_()
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = MTL(ObRebuildService*);
  ObLSMigrationTask task;
  int32_t result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_ISNULL(rebuild_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild service should not be NULL", K(ret), KP(rebuild_service));
  } else if (OB_FAIL(get_result_(result))) {
    LOG_WARN("failed to get ls migration result", K(ret), KPC(ls_), K(task));
  } else if (OB_FAIL(rebuild_service->finish_rebuild_ls(ls_->get_ls_id(), result))) {
    LOG_WARN("failed to finish rebuild ls", K(ret), KPC(ls_), K(result));
  }
  return ret;
}

int ObLSMigrationHandler::report_to_disaster_recovery_()
{
  int ret = OB_SUCCESS;
  obrpc::ObDRTaskReplyResult res;
  ObLSMigrationTask task;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", KR(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", KR(ret));
  } else {
    int result = OB_SUCCESS;
    obrpc::ObDRTaskType dr_type = obrpc::ObDRTaskType::MAX_TYPE;
    uint64_t sys_data_version = 0;
    if (OB_FAIL(ObMigrationOpType::convert_to_dr_type(task.arg_.type_, dr_type))) {
      LOG_WARN("fail to convert dr task type", KR(ret), K(task));
    } else if (OB_FAIL(get_result_(result))) {
      LOG_WARN("failed to get ls migration result", KR(ret), K(task));
    } else if (OB_FAIL(res.init(task.task_id_, MTL_ID(), task.arg_.ls_id_, result, dr_type))) {
      // for dr_type, no compatibility processing required
      LOG_WARN("failed to init res", KR(ret), K(task), K(result), K(dr_type));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret));
    } else if (sys_data_version >= DATA_VERSION_4_3_5_1) {
      if (OB_FAIL(rootserver::DisasterRecoveryUtils::report_to_disaster_recovery(res))) {
        LOG_WARN("failed to report to meta tenant", KR(ret), K(res));
      }
    } else if (OB_FAIL(rootserver::DisasterRecoveryUtils::report_to_rs(res))) {
      LOG_WARN("failed to report to rs", KR(ret), K(res));
    }
  }
  return ret;
}

int ObLSMigrationHandler::check_can_skip_prepare_status_(bool &can_skip)
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask task;
  can_skip = false;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(task.arg_.dst_.get_replica_type())) {
    can_skip = true;
    LOG_INFO("replica without ssstore, skip prepare status", K(task));
  } else if (ObMigrationOpType::REBUILD_LS_OP == task.arg_.type_) {
    if (OB_FAIL(ls_->get_migration_status(status))) {
      LOG_WARN("failed to get migration status", K(ret), K(status));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == status) {
      can_skip = false;
    } else {
      can_skip = true;
      LOG_INFO("rebuild and migration status is not none, skip ls migration prepare status", K(status), K(task));
    }
  } else {
    can_skip = true;
    LOG_INFO("not rebuild ls, skip ls migration prepare status", K(task));
  }
  return ret;
}

int ObLSMigrationHandler::check_before_do_task_()
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (ObMigrationOpType::REMOVE_LS_OP == task.arg_.type_) {
    LOG_INFO("no need to check remove ls op", K(task));
  } else if (OB_FAIL(check_disk_space_(task.arg_))) {
    STORAGE_LOG(WARN, "failed to check_disk_space_", K(ret), K(task));
  }
  return ret;
}

int ObLSMigrationHandler::check_disk_space_(const ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t required_size = 0;
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not inited", K(ret));
  } else if (ObMigrationOpType::MIGRATE_LS_OP != arg.type_
             && ObMigrationOpType::ADD_LS_OP != arg.type_
             && ObMigrationOpType::REPLACE_LS_OP != arg.type_) {
    LOG_INFO("only migration or add or replace ls need check disk space, others skip it",
        K(arg));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(arg.dst_.get_replica_type())) {
    LOG_INFO("dst has no ssstore, no need check disk space", K(arg));
  } else if (tenant_config.is_valid() && tenant_config->_skip_checking_ls_migration_required_size) {
    // the required_size read from virtual table may not be accurate,
    // so we set it to 1 to bypass the check in emergency
    required_size = 1;
    LOG_INFO("skip checking ls migration required size", K(tenant_id));
  } else if (OB_TMP_FAIL(get_ls_required_size_(arg, required_size))) {
    // use OB_TMP_FAIL to ignore return value because when tenant is restoring,
    // inner table may not created yet, which is not readable,
    // get_ls_required_size may fail, migration task could fail all the time,
    // so we have to ignore the return value
    LOG_WARN("failed to get ls required size", KR(tmp_ret), K(arg));
  }
  if (OB_FAIL(ret)) {
  } else if (required_size > 0) {
    if (!GCTX.is_shared_storage_mode()) {
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.check_space_full(required_size))) {
        // if disk space is not enough, return OB_SERVER_OUTOF_DISK_SPACE
        FLOG_ERROR( "failed to check_is_disk_full, cannot migrate in",
            KR(ret), K(required_size));
      }
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.check_ls_migration_space_full(arg, required_size))) {
        // if disk space is not enough, return OB_SERVER_OUTOF_DISK_SPACE. if auto expand data_disk_size, need wait OBCloud platform expand datafile_size
        FLOG_ERROR( "failed to check ls migration space full, cannot migrate in",
            KR(ret), K(required_size));
      }
#endif
    }
  }
  return ret;
}

int ObLSMigrationHandler::get_ls_required_size_(
    const ObMigrationOpArg &arg,
    int64_t &required_size)
{
  int ret = OB_SUCCESS;
  required_size = 0;
  const uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  ObSqlString sql;
  int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start to get log stream required data disk size before migration", K(tenant_id), K(arg));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not inited", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id));
  } else if (MOCK_DATA_VERSION_4_2_5_3 > data_version
             || (DATA_VERSION_4_3_0_0 <= data_version && DATA_VERSION_4_3_3_0 >= data_version)) {
    LOG_TRACE("data version not promoted, do no check required size", K(tenant_id), K(data_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server rpc proxy", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT MAX(required_data_disk_size) AS required_size "
                                    "FROM %s "
                                    "WHERE tenant_id = %lu AND ls_id = %ld",
                                    OB_ALL_VIRTUAL_LS_INFO_TNAME, tenant_id, arg.ls_id_.id()))) {
    LOG_WARN("fail to construct sql to get required_data_disk_size", KR(ret), K(tenant_id), K(arg));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      // TODO@jingyu.cr: need to avoid reading virtual table during migration, get ls required size through RPC
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(sql));
      } else {
        ret = result->next();
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("ls replica not found", KR(ret), K(sql));
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get required_data_disk_size", KR(ret), K(sql));
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "required_size", required_size, int64_t);
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to get required_data_disk_size from result", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ITER_END != result->next()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expect only one row", KR(ret), K(sql));
          }
        }
      }
    }
  }
  int64_t cost = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("finish get log stream required data disk size before migration",
           KR(ret), K(tenant_id), K(arg), K(required_size), K(data_version), K(begin_time), K(cost));
  return ret;
}

void ObLSMigrationHandler::stop()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;

  common::SpinWLockGuard guard(lock_);
  is_stop_ = true;
  result_ = OB_SUCCESS != result_ ? result_ : OB_IN_STOP_STATE;
  if (task_list_.empty()) {
  } else if (task_list_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls migration task count is unexpected", K(ret), K(task_list_));
  } else {
    ObLSMigrationTask &task = task_list_.at(0);
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get ObTenantDagScheduler from MTL", K(ret), KPC(ls_));
    } else if (OB_FAIL(scheduler->cancel_dag_net(task.task_id_))) {
      LOG_ERROR("failed to cancel dag net", K(ret), K(task), KPC(ls_));
    }
  }
#ifdef ERRSIM
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObLSID ls_id(ObLSID::INVALID_LS_ID);
  if (OB_NOT_NULL(ls_)) {
    tenant_id = ls_->get_tenant_id();
    ls_id = ls_->get_ls_id();
  }
  SERVER_EVENT_ADD("storage_ha", "migration_handler_stop",
      "tenant_id", tenant_id,
      "ls_id", ls_id.id(),
      "is_failed", ret,
      "result", result_);
#endif
}

void ObLSMigrationHandler::wait(bool &wait_finished)
{
  int ret = OB_SUCCESS;
  wait_finished = false;
  ObLSMigrationTask task;
  share::ObTenantBase *tenant_base = MTL_CTX();
  omt::ObTenant *tenant = nullptr;

  if (!is_inited_) {
    wait_finished = true;
  } else if (OB_ISNULL(tenant_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant base should not be NULL", K(ret), KP(tenant_base));
  } else if (FALSE_IT(tenant = static_cast<omt::ObTenant *>(tenant_base))) {
  } else if (tenant->has_stopped()) {
    LOG_INFO("tenant has stop, no need wait ls migration handler task finish");
    wait_finished = true;
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      wait_finished = true;
    } else {
      LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
    }
  } else {
    wait_finished = false;
    wakeup_();
  }
}

int ObLSMigrationHandler::set_ha_src_info(const ObStorageHASrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(src_info));
  } else {
    common::SpinWLockGuard guard(lock_);
    chosen_src_ = src_info;
  }
  return ret;
}

int ObLSMigrationHandler::get_ls_migration_task_with_nolock_(ObLSMigrationTask &task) const
{
  int ret = OB_SUCCESS;
  task.reset();
  if (task_list_.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("migration task is empty", K(ret), KPC(ls_));
  } else if (task_list_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task count should not more than 1", K(ret), K(task_list_), KPC(ls_));
  } else {
    task = task_list_.at(0);
  }
  return ret;
}

int ObLSMigrationHandler::get_ha_src_info_(ObStorageHASrcInfo &src_info) const
{
  int ret = OB_SUCCESS;
  src_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    src_info = chosen_src_;
  }
  return ret;
}

int ObLSMigrationHandler::generate_build_tablet_dag_net_()
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask ls_migration_task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()
      || ObMigrationOpType::REBUILD_TABLET_OP != ls_migration_task.arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObRebuildTabletDagNetInitParam param;
    param.arg_ = ls_migration_task.arg_;
    param.task_id_ = ls_migration_task.task_id_;
    param.bandwidth_throttle_ = bandwidth_throttle_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.sql_proxy_ = sql_proxy_;
    if (OB_FAIL(schedule_dag_net_<ObRebuildTabletDagNet>(&param, false /* check_cancel */))) {
      LOG_WARN("failed to schedule build tablet dag net", K(ret), K(ls_migration_task), KPC(ls_));
    }
  }
  return ret;
}

int ObLSMigrationHandler::check_task_exist_with_nolock_(const share::ObTaskId &task_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObLSMigrationTask task;
  if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(get_ls_migration_task_with_nolock_(task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
    }
  } else if (task_id == task.task_id_) {
    is_exist = true;
  } else {
    is_exist = false;
  }
  return ret;
}

int ObLSMigrationHandler::switch_next_stage_with_nolock_(const int32_t result)
{
  int ret = OB_SUCCESS;
  ObLSMigrationHandlerStatus next_status = ObLSMigrationHandlerStatus::MAX_STATUS;
  int32_t new_result = OB_SUCCESS;

  new_result = OB_SUCCESS != result_ ? result_ : result;

  if (OB_FAIL(ObLSMigrationHandlerStatusHelper::get_next_change_status(status_, new_result, next_status))) {
    LOG_WARN("failed to get next change status", K(ret), K(status_), K(result), K(new_result));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("storage_ha", "migration_handler_change_status",
        "tenant_id", ls_->get_tenant_id(),
        "ls_id", ls_->get_ls_id().id(),
        "current_status", status_,
        "next_status", next_status,
        "result", new_result);
#endif
    FLOG_INFO("report result", K(result), K(new_result), K(result_), K(status_), K(next_status));
    result_ = new_result;
    status_ = next_status;
  }
  wakeup_();
  return ret;
}

int ObLSMigrationHandler::check_need_to_abort_(bool &need_to_abort)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_to_abort = false;

  if (is_migration_failed_()) {
    int32_t result = OB_SUCCESS;
    if (OB_FAIL(get_result_(result))) {
      LOG_WARN("failed to get result", K(ret));
    } else {
      tmp_ret = result;
      LOG_INFO("migration is already failed, skip current status", K(result), K(status_), KPC(ls_));
    }
  } else if (is_cancel()) {
    tmp_ret = OB_CANCELED;
    LOG_INFO("migration is cancelled, skip current status", K(tmp_ret), K(status_), KPC(ls_));
  }

  if (OB_TMP_FAIL(tmp_ret)) {
    if (OB_FAIL(set_result(tmp_ret))) {
      LOG_WARN("failed to set result", K(ret), K(tmp_ret), K(status_), KPC(ls_));
    } else {
      need_to_abort = true;
    }
  }

  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLSMigrationHandler::notify_switch_to_leader_and_wait_replace_complete(const int64_t new_proposal_id)
{
  int ret = OB_SUCCESS;
  ObLSMigrationTask task;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // skip when no migration task exist
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
    }
  } else if (ObMigrationOpType::REPLACE_LS_OP != task.arg_.type_) {
    // skip when migration task is not replace ls
  } else {

    // notify switch to leader
    {
      ObThreadCondGuard cond_guard(switch_leader_cond_);
      leader_proposal_id_ = new_proposal_id;
      switch_leader_cnt_++;
      switch_leader_cond_.signal();
      LOG_INFO("notify switch to leader", "ls_id", ls_->get_ls_id(), K_(leader_proposal_id), K_(switch_leader_cnt));
    }

    // wait replace complete
    const int64_t start_ts = ObTimeUtility::current_time();
    ObTimeoutCtx timeout_ctx;
    int64_t timeout = 10_s;
    const ObLSID ls_id = ls_->get_ls_id();
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    const uint64_t CHECK_CONDITION_INTERVAL = 200_ms;

    if (OB_FAIL(timeout_ctx.set_timeout(timeout))) {
      LOG_WARN("failed to set timeout", K(ret), K(timeout));
    }

    while(OB_SUCC(ret)) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_WAIT_LS_REPLACE_COMPLETE_TIMEOUT;
        LOG_WARN("wait replace complete timeout", K(ret), K(ls_id), K(timeout));
      } else if (OB_FAIL(ls_->get_migration_status(migration_status))) {
        LOG_WARN("failed to get migration status", K(ret), KPC(ls_));
      } else if (OB_MIGRATION_STATUS_REPLACE_FAIL == migration_status) {
        ret = OB_LS_REPLACE_FAILED;
        LOG_WARN("ls replace failed, switch to leader is not allowed", K(ret),
          K(ls_id), K(migration_status));
      } else if (OB_MIGRATION_STATUS_NONE == migration_status) {
        LOG_INFO("ls replace complete, switch to leader is allowed", K(ls_id));
        break;
      } else {
        ob_usleep(CHECK_CONDITION_INTERVAL);
        LOG_INFO("ls replace not complete, need wait", K(ls_id), K(migration_status));
      }
    }

    const int64_t cost = ObTimeUtility::current_time() - start_ts;
    LOG_INFO("notify switch to leader and wait replace complete", K(ret), K(ls_id), K(cost), K_(switch_leader_cnt));
  }

  return ret;
}

int ObLSMigrationHandler::wait_notified_switch_to_leader(
    const ObTimeoutCtx &timeout_ctx,
    int64_t &leader_proposal_id)
{
  int ret = OB_SUCCESS;
  const uint64_t CHECK_CONDITION_INTERVAL_MS = 200;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  }

  while(OB_SUCC(ret)) {
    if (timeout_ctx.is_timeouted()) {
      ret = OB_WAIT_ELEC_LEADER_TIMEOUT;
      LOG_WARN("wait ls elect leader timeout", K(ret), K(timeout_ctx));
    } else {
      ObThreadCondGuard cond_guard(switch_leader_cond_);
      if (switch_leader_cnt_ > 0) {
        leader_proposal_id = leader_proposal_id_;
        switch_leader_cnt_ = 0;
        LOG_INFO("wait elect as leader success", "ls_id", ls_->get_ls_id(), K_(switch_leader_cnt), K_(leader_proposal_id));
        break;
      } else {
        switch_leader_cond_.wait(CHECK_CONDITION_INTERVAL_MS);
      }
    }
  }

  return ret;
}
#endif

}
}

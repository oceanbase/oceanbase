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
#include "share/ls/ob_ls_table_operator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "ob_rebuild_service.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObLSMigrationHandlerStatusHelper*********************/
bool ObLSMigrationHandlerStatusHelper::is_valid(
    const ObLSMigrationHandlerStatus &status)
{
  return status >= ObLSMigrationHandlerStatus::INIT && status < ObLSMigrationHandlerStatus::MAX_STATUS;
}

int ObLSMigrationHandlerStatusHelper::check_can_change_status(
    const ObLSMigrationHandlerStatus &curr_status,
    const ObLSMigrationHandlerStatus &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;
  if (!is_valid(curr_status) || !is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(curr_status), K(change_status));
  }else {
    switch (curr_status) {
    case ObLSMigrationHandlerStatus::INIT: {
      if (ObLSMigrationHandlerStatus::INIT == change_status
          || ObLSMigrationHandlerStatus::COMPLETE_LS == change_status
          || ObLSMigrationHandlerStatus::PREPARE_LS == change_status) {
        can_change = true;
      }
      break;
    }
    case ObLSMigrationHandlerStatus::PREPARE_LS: {
      if (ObLSMigrationHandlerStatus::PREPARE_LS == change_status
          || ObLSMigrationHandlerStatus::BUILD_LS == change_status
          || ObLSMigrationHandlerStatus::COMPLETE_LS == change_status) {
        can_change = true;
      }
      break;
    }
    case ObLSMigrationHandlerStatus::BUILD_LS: {
      if (ObLSMigrationHandlerStatus::BUILD_LS == change_status
          || ObLSMigrationHandlerStatus::COMPLETE_LS == change_status) {
        can_change = true;
      }
      break;
    }
    case ObLSMigrationHandlerStatus::COMPLETE_LS: {
      if (ObLSMigrationHandlerStatus::COMPLETE_LS == change_status
          || ObLSMigrationHandlerStatus::FINISH == change_status) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid curr status for fail", K(ret), K(curr_status), K(change_status));
    }
    }
  }
  return ret;
}

int ObLSMigrationHandlerStatusHelper::get_next_change_status(
    const ObLSMigrationHandlerStatus &curr_status,
    const int32_t result,
    ObLSMigrationHandlerStatus &next_status)
{
  int ret = OB_SUCCESS;
  next_status = ObLSMigrationHandlerStatus::MAX_STATUS;
  bool can_change = false;

  if (!is_valid(curr_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next change status get invalid argument", K(ret), K(curr_status));
  } else if (OB_SUCCESS != result
      && ObLSMigrationHandlerStatus::COMPLETE_LS != curr_status) {
    next_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
  } else {
    switch (curr_status) {
    case ObLSMigrationHandlerStatus::INIT: {
      next_status = ObLSMigrationHandlerStatus::PREPARE_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::PREPARE_LS: {
      next_status = ObLSMigrationHandlerStatus::BUILD_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::BUILD_LS: {
      next_status = ObLSMigrationHandlerStatus::COMPLETE_LS;
      break;
    }
    case ObLSMigrationHandlerStatus::COMPLETE_LS: {
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
    is_stop_(false)
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
  } else {
    ls_ = ls;
    bandwidth_throttle_ = bandwidth_throttle;
    svr_rpc_proxy_ = svr_rpc_proxy;
    storage_rpc_ = storage_rpc;
    sql_proxy_ = sql_proxy;
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

int ObLSMigrationHandler::change_status_(const ObLSMigrationHandlerStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!ObLSMigrationHandlerStatusHelper::is_valid(new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("change status get invalid argument", K(ret), K(new_status));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(ObLSMigrationHandlerStatusHelper::check_can_change_status(status_, new_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(status_), K(new_status));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change ls migration handler status", K(ret), K(status_), K(new_status));
    } else {
      status_ = new_status;
    }
  }
  return ret;
}

int ObLSMigrationHandler::set_result_(const int32_t result)
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
    if (task_list_.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("migration task is empty", K(ret), KPC(ls_));
    } else if (task_list_.count() > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls migration task count should not more than 1", K(ret), K(task_list_), KPC(ls_));
    } else {
      task = task_list_.at(0);
    }
  }
  return ret;
}

int ObLSMigrationHandler::check_task_exist_(
    const ObLSMigrationHandlerStatus &status,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObLSMigrationTask ls_migration_task;
  ObTenantDagScheduler *scheduler = nullptr;
  ObLSMigrationHandlerStatus current_status = ObLSMigrationHandlerStatus::MAX_STATUS;
  ObLSMigrationHandlerStatus next_status = ObLSMigrationHandlerStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!ObLSMigrationHandlerStatusHelper::is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check task exist get invalid argument", K(ret), K(status), KPC(ls_));
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
  } else if (is_exist) {
    //do nothing
  } else if (!is_exist) {
    if (OB_FAIL(get_ls_migration_handler_status_(current_status))) {
      LOG_WARN("failed to get ls migration handler status", K(ret), KPC(ls_), K(ls_migration_task));
    } else if (current_status != status) {
      int32_t result = OB_SUCCESS;
      if (OB_FAIL(get_result_(result))) {
        LOG_WARN("failed to get result", K(ret), KPC(ls_));
      } else if (OB_FAIL(ObLSMigrationHandlerStatusHelper::get_next_change_status(status, result, next_status))) {
        LOG_WARN("failed to get next change status", K(ret), KPC(ls_));
      } else if (next_status != current_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls migration handler status is unexpected", K(ret), KPC(ls_),
            K(ls_migration_task), K(status), K(current_status), K(next_status));
      } else {
        is_exist = true;
      }
    }
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
  ObLSMigrationHandlerStatus next_status = ObLSMigrationHandlerStatus::MAX_STATUS;
  bool can_change = false;
  int32_t new_result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    new_result = OB_SUCCESS != result_ ? result_ : result;

    if (OB_FAIL(ObLSMigrationHandlerStatusHelper::get_next_change_status(status_, new_result, next_status))) {
      LOG_WARN("failed to get next change status", K(ret), K(status_), K(result), K(new_result));
    } else if (OB_FAIL(ObLSMigrationHandlerStatusHelper::check_can_change_status(status_, next_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(status_), K(next_status));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change ls migration handler status", K(ret), K(status_), K(next_status));
    } else {
      FLOG_INFO("report result", K(result), K(new_result), K(result_), K(status_), K(next_status));
      result_ = new_result;
      status_ = next_status;
    }
    wakeup_();
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
    switch (status) {
    case ObLSMigrationHandlerStatus::INIT: {
      if (OB_FAIL(do_init_status_())) {
        LOG_WARN("failed to do init status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::PREPARE_LS : {
      if (OB_FAIL(do_prepare_ls_status_())) {
        LOG_WARN("failed to do prepare ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::BUILD_LS : {
      if (OB_FAIL(do_build_ls_status_())) {
        LOG_WARN("failed to do build ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::COMPLETE_LS : {
      if (OB_FAIL(do_complete_ls_status_())) {
        LOG_WARN("failed to do complete ls status", K(ret), K(status));
      }
      break;
    }
    case ObLSMigrationHandlerStatus::FINISH : {
      if (OB_FAIL(do_finish_status_())) {
        LOG_WARN("failed to do finish status", K(ret), K(status));
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

int ObLSMigrationHandler::do_init_status_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  bool is_empty = false;
  bool can_switch_next_stage = true;
  ObLSMigrationHandlerStatus new_status = ObLSMigrationHandlerStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (is_migration_failed_()) {
    //do nothing
  } else {
    // this lock make sure the ls creating is not scheduled to migrate.
    ObLSLockGuard lock_ls(ls_, true /* read lock */);
    start_ts_ = ObTimeUtil::current_time();
    if (OB_FAIL(ls_->get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), KPC(ls_));
    } else if (OB_FAIL(check_task_list_empty_(is_empty))) {
      LOG_WARN("failed to check task list empty", K(ret), KPC(ls_));
    } else if (is_empty) {
      if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == migration_status
          || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
        //do nothing
      } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL != migration_status
          && ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL != migration_status
          && ObMigrationStatus::OB_MIGRATION_STATUS_GC != migration_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls migration handler in init status but ls migration status is in failed status",
            K(ret), K(is_empty), K(migration_status), KPC(ls_));
      }
    } else {
      new_status = ObLSMigrationHandlerStatus::PREPARE_LS;
      ObLSMigrationTask task;
      if (OB_FAIL(check_before_do_task_())) {
        LOG_WARN("failed to check before do task", K(ret), KPC(ls_));
      } else if (OB_FAIL(change_status_(new_status))) {
        LOG_WARN("failed to change status", K(ret), K(new_status), KPC(ls_));
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
  }

  if (OB_FAIL(ret)) {
    if (!can_switch_next_stage) {
      //do nothing
    } else if (OB_SUCCESS != (tmp_ret = switch_next_stage(ret))) {
      LOG_WARN("failed to report result", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObLSMigrationHandler::do_prepare_ls_status_()
{
  int ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::PREPARE_LS;
  bool is_exist = false;
  bool can_skip_prepare = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (is_migration_failed_()) {
    //do nothing
  } else if (OB_FAIL(check_can_skip_prepare_status_(can_skip_prepare))) {
    LOG_WARN("failed to check can skip prepare status", K(ret));
  } else if (can_skip_prepare) {
    if (OB_FAIL(switch_next_stage(OB_SUCCESS))) {
      LOG_WARN("failed to report result", K(ret), KPC(ls_), K(status));
    }
  } else if (OB_FAIL(check_task_exist_(status, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), K(status), KPC(ls_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(generate_prepare_ls_dag_net_())) {
    LOG_WARN("failed to generate prepare ls dag net", K(ret), K(status), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::do_build_ls_status_()
{
  int ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::BUILD_LS;
  bool is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (is_migration_failed_()) {
    //do nothing
  } else if (OB_FAIL(check_task_exist_(status, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), K(status), KPC(ls_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(generate_build_ls_dag_net_())) {
    LOG_WARN("failed to generate build ls dag net", K(ret), K(status), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::do_complete_ls_status_()
{
  int ret = OB_SUCCESS;
  const ObLSMigrationHandlerStatus status = ObLSMigrationHandlerStatus::COMPLETE_LS;
  bool is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(check_task_exist_(status, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), K(status), KPC(ls_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(generate_complete_ls_dag_net_())) {
    LOG_WARN("failed to generate complete ls dag net", K(ret), K(status), KPC(ls_));
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
        "src", task.arg_.data_src_.get_server(),
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

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_FAIL(get_ls_migration_task_(ls_migration_task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else if (!ls_migration_task.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration task is not valid", K(ret), K(ls_migration_task), KPC(ls_));
  } else if (OB_FAIL(schedule_build_ls_dag_net_(ls_migration_task))) {
    LOG_WARN("failed to schedule build ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::schedule_build_ls_dag_net_(
    const ObLSMigrationTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule build ls dag net get invalid argument", K(ret), K(task));
  } else {
    DEBUG_SYNC(BEFORE_BUILD_LS_MIGRATION_DAG_NET);
    ObTenantDagScheduler *scheduler = nullptr;
    ObMigrationDagNetInitParam param;
    param.arg_ = task.arg_;
    param.task_id_ = task.task_id_;
    param.bandwidth_throttle_ = bandwidth_throttle_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.sql_proxy_ = sql_proxy_;

    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObMigrationDagNet>(&param))) {
      LOG_WARN("failed to create and add migration dag net", K(ret), K(task), KPC(ls_));
    } else {
      LOG_INFO("success to create migration dag net", K(ret), K(task));
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
  } else if (OB_FAIL(schedule_prepare_ls_dag_net_(ls_migration_task))) {
    LOG_WARN("failed to schedule prepare ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::schedule_prepare_ls_dag_net_(
    const ObLSMigrationTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule prepare ls dag net get invalid argument", K(ret), K(task));
  } else {
    ObTenantDagScheduler *scheduler = nullptr;
    ObLSPrepareMigrationParam param;
    param.arg_ = task.arg_;
    param.task_id_ = task.task_id_;

    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObLSPrepareMigrationDagNet>(&param))) {
      LOG_WARN("failed to create and add migration dag net", K(ret), K(task), KPC(ls_));
    } else {
      LOG_INFO("success to create ls prepare migration dag net", K(ret), K(task));
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
  } else if (OB_FAIL(schedule_complete_ls_dag_net_(ls_migration_task))) {
    LOG_WARN("failed to schedule complete ls dag net", K(ret), K(ls_migration_task), KPC(ls_));
  }
  return ret;
}

int ObLSMigrationHandler::schedule_complete_ls_dag_net_(
    const ObLSMigrationTask &task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule complete ls dag net get invalid argument", K(ret), K(task));
  } else {
    int32_t result = OB_SUCCESS;
    ObTenantDagScheduler *scheduler = nullptr;
    ObLSCompleteMigrationParam param;
    param.arg_ = task.arg_;
    param.task_id_ = task.task_id_;
    param.rebuild_seq_ = ls_->get_rebuild_seq();

    if (OB_FAIL(get_result_(result))) {
      LOG_WARN("failed to get result", K(ret), KPC(ls_), K(task));
    } else if (FALSE_IT(param.result_ = result)) {
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObLSCompleteMigrationDagNet>(&param))) {
      LOG_WARN("failed to create and add migration dag net", K(ret), K(task), KPC(ls_));
    } else {
      LOG_INFO("success to create ls complete migration dag net", K(ret), K(task), K(param));
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
  } else if (ObMigrationOpType::REBUILD_LS_OP == task.arg_.type_) {
    if (OB_FAIL(report_to_rebuild_service_())) {
      LOG_WARN("failed to report to rebuild service", K(ret), K(task));
    }
  } else {
    if (OB_FAIL(report_to_rs_())) {
      LOG_WARN("failed to report to rs", K(ret), KPC(ls_));
    }
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

int ObLSMigrationHandler::report_to_rs_()
{
  int ret = OB_SUCCESS;
  obrpc::ObDRTaskReplyResult res;
  ObRsMgr *rs_mgr = GCTX.rs_mgr_;
  int64_t retry_count = 0;
  const int64_t MAX_RETRY_TIMES = 3;
  ObAddr rs_addr;
  const int64_t REPORT_RETRY_INTERVAL_MS = 100 * 1000; //100ms
  ObLSMigrationTask task;
  const uint64_t tenant_id = MTL_ID();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (OB_ISNULL(rs_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr should not be NULL", K(ret), KP(rs_mgr));
  } else if (OB_FAIL(get_ls_migration_task_(task))) {
    LOG_WARN("failed to get ls migration task", K(ret), KPC(ls_));
  } else {
    res.task_id_ = task.task_id_;
    res.tenant_id_ = tenant_id;
    res.ls_id_ = task.arg_.ls_id_;
    if (OB_FAIL(get_result_(res.result_))) {
      LOG_WARN("failed to get ls migration result", K(ret), KPC(ls_), K(task));
    } else {
      while (retry_count++ < MAX_RETRY_TIMES) {
        if (OB_FAIL(rs_mgr->get_master_root_server(rs_addr))) {
          STORAGE_LOG(WARN, "get master root service failed", K(ret));
        } else if (OB_FAIL(storage_rpc_->post_ls_disaster_recovery_res(rs_addr, res))) {
          LOG_WARN("failed to post ls diaster recovery res", K(ret), K(rs_addr), K(res));
        }
        if (OB_RS_NOT_MASTER != ret) {
          if (OB_SUCC(ret)) {
            STORAGE_LOG(INFO, "post migration result success",
                K(rs_addr), K(task));
          }
          break;
        } else if (OB_FAIL(rs_mgr->renew_master_rootserver())) {
          STORAGE_LOG(WARN, "renew master root service failed", K(ret));
        }

        if (OB_FAIL(ret)) {
          ob_usleep(REPORT_RETRY_INTERVAL_MS);
        }
      }
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
  } else if (ObMigrationOpType::REBUILD_LS_OP == task.arg_.type_) {
    if (OB_FAIL(ls_->get_migration_status(status))) {
      LOG_WARN("failed to get migration status", K(ret), K(status));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == status) {
      can_skip = false;
    } else {
      can_skip = true;
      LOG_INFO("skip ls migration prepare status", K(status));
    }
  } else {
    can_skip = true;
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
  int64_t required_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not inited", K(ret));
  } else if (ObMigrationOpType::MIGRATE_LS_OP != arg.type_ && ObMigrationOpType::ADD_LS_OP != arg.type_) {
    LOG_INFO("only migration or add ls need check disk space, others skip it",
        K(arg));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(arg.dst_.get_replica_type())) {
    LOG_INFO("dst has no ssstore, no need check disk space", K(arg));
  } else if (OB_FAIL(get_ls_required_size_(arg, required_size))) {
    LOG_WARN("failed to get ls required size", K(ret), K(arg));
  } else if (required_size > 0) {
    if (OB_FAIL(THE_IO_DEVICE->check_space_full(required_size))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        ret = OB_SERVER_MIGRATE_IN_DENIED;
      }
      FLOG_ERROR( "failed to check_is_disk_full, cannot migrate in",
          K(ret), K(required_size));
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
  ObLSInfo ls_info;

  if (OB_FAIL(get_ls_info_(arg.cluster_id_, tenant_id, arg.ls_id_, ls_info))) {
    LOG_WARN("failed to get ls info", K(ret), K(arg), K(tenant_id), KPC(ls_));
  } else {
    const common::ObIArray<ObLSReplica> &replicas = ls_info.get_replicas();
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      if (replicas.at(i).get_required_size() > required_size) {
        required_size = replicas.at(i).get_required_size();
      }
    }
  }
  return ret;
}

int ObLSMigrationHandler::get_ls_info_(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  share::ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration handler do not init", K(ret));
  } else if (cluster_id < 0 || OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls info get invalid argument", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else if (nullptr == lst_operator) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator ptr is null", K(ret));
  } else if (OB_FAIL(lst_operator->get(cluster_id, tenant_id,
             ls_->get_ls_id(), share::ObLSTable::DEFAULT_MODE, ls_info))) {
    LOG_WARN("failed to get log stream info", K(ret), K(cluster_id), K(tenant_id), "ls id", ls_->get_ls_id());
  }
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



}
}

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
#include "ob_backup_clean_ls_task_mgr.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_backup_clean_struct.h"
#include "ob_backup_schedule_task.h"
#include "ob_backup_task_scheduler.h"

namespace oceanbase
{
namespace rootserver
{

using namespace share;

// ObIBackupTaskStatusAdvancer
ObBackupCleanLSTaskMgr::ObBackupCleanLSTaskMgr()
 : is_inited_(false),
   task_attr_(nullptr),
   ls_attr_(nullptr),
   task_scheduler_(nullptr),
   sql_proxy_(nullptr),
   backup_service_(nullptr) 
{
}

ObBackupCleanLSTaskMgr::~ObBackupCleanLSTaskMgr()
{
}

int ObBackupCleanLSTaskMgr::init(
    ObBackupCleanTaskAttr &task_attr,
    ObBackupCleanLSTaskAttr &ls_attr,
    ObBackupTaskScheduler &task_scheduler,
    common::ObISQLClient &sql_proxy,
    ObBackupCleanService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ls_attr.is_valid() || !task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_attr), K(task_attr));
  } else {
    task_attr_ = &task_attr;
    ls_attr_ = &ls_attr;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    backup_service_ = &backup_service; 
    is_inited_ = true;
  }
  return ret;
}

int ObBackupCleanLSTaskMgr::process(int64_t &finish_cnt)
{
  //execute different operations according to the status of the task
  //status including : INIT PENDING DOING FINISH 
  //INIT: add log stream task into task scheduler and advance status to pending
  //PENDING: do nothing, task scheduler will advance status to doing
  //DOING: do nothing, observer will advance status to finish
  //FINISH: rely on execution of observer. only when task finish successfully or error which can't retry happen,
  //        observer will advance stautus to finish. 
  //        when task status is in finish, process only to update can_advance_ls_task_status to  true and result = ls_attr.result
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("schedule backup ls task", K(*ls_attr_));
    switch (ls_attr_->status_.status_) {
      case ObBackupTaskStatus::Status::INIT: {
        if (OB_FAIL(add_task_())) {
          LOG_WARN("failed to add task into task schedulers", K(ret), KP(ls_attr_));
        }
        break;
      } 
      case ObBackupTaskStatus::Status::PENDING:
      case ObBackupTaskStatus::Status::DOING: 
        break;
      case ObBackupTaskStatus::Status::FINISH: {
        if (OB_FAIL(finish_(finish_cnt))) {
          LOG_WARN("failed to finish task", K(ret), KP(ls_attr_));
        }
        break;
      } 
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown backup ls task status", K(ret), KP(ls_attr_));
      }
    }
  }
  return ret;
}

int ObBackupCleanLSTaskMgr::add_task_()
{
  // add log stream task into task scheduler
  // then advance log stream task status to PENDING
  int ret = OB_SUCCESS;
  ObBackupCleanLSTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  if (OB_FAIL(task.build(*task_attr_, *ls_attr_))) {
    LOG_WARN("failed to build task", K(ret), KP(task_attr_), KP(ls_attr_));
  } else if (OB_FAIL(advance_ls_task_status(*backup_service_, *sql_proxy_, *ls_attr_, next_status))) {
    LOG_WARN("failed to advance ls task status", K(ret), K(*task_attr_));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("failed to add task", K(ret), K(*task_attr_), K(task));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success add task", K(*task_attr_), K(*ls_attr_), K(task)); 
  } 
  return ret;
}

int ObBackupCleanLSTaskMgr::advance_ls_task_status(
    ObBackupCleanService &backup_service,
    common::ObISQLClient &sql_proxy, 
    const ObBackupCleanLSTaskAttr &ls_attr, 
    const ObBackupTaskStatus &next_status, 
    const int result,
    const int64_t end_ts)
{
  int ret  = OB_SUCCESS;
  if (!next_status.is_valid() || !ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next_status or invalid ls_attr", K(ret), K(next_status), K(ls_attr));
  } else if (OB_FAIL(backup_service.check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanLSTaskOperator::advance_ls_task_status(sql_proxy, ls_attr, next_status, result, end_ts))) {
    LOG_WARN("failed to advance log stream status", K(ret), K(ls_attr), K(result), K(next_status));
  }
  return ret;
}

int ObBackupCleanLSTaskMgr::redo_ls_task(
    ObBackupCleanService &backup_service,
    common::ObISQLClient &sql_proxy, 
    const ObBackupCleanLSTaskAttr &ls_attr,
    const int64_t retry_id)
{
  int ret  = OB_SUCCESS;
  if (!ls_attr.is_valid() || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_attr));
  } else if (OB_FAIL(backup_service.check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanLSTaskOperator::redo_ls_task(sql_proxy, ls_attr, retry_id))) {
    LOG_WARN("failed to redo ls task", K(ret), K(ls_attr));
  } 
  return ret;
}

int ObBackupCleanLSTaskMgr::finish_(int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ls_attr_->result_) {
    finish_cnt++;
  } else if (ObBackupUtils::is_need_retry_error(ls_attr_->result_)) {
    int64_t cur_ts = ObTimeUtility::current_time();
    if (cur_ts > ls_attr_->start_ts_ + OB_BACKUP_RETRY_TIME_INTERVAL) {
      if (OB_FAIL(redo_ls_task(*backup_service_, *sql_proxy_, *ls_attr_, ls_attr_->retry_id_ + 1/*increase retry id*/))) {
        LOG_WARN("failed to redo ls task", K(ret), KP(ls_attr_));
      } else {
        backup_service_->wakeup();
      }
    }
  } else {
    ret = ls_attr_->result_;
  }
  return ret;
}

int ObBackupCleanLSTaskMgr::statistic_info(
    ObBackupCleanService &backup_service,
    common::ObISQLClient &sql_proxy, 
    const ObBackupCleanLSTaskAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ObBackupCleanStats stats = ls_attr.stats_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(backup_service.check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanLSTaskOperator::update_stats_(
      sql_proxy, ls_attr.task_id_, ls_attr.tenant_id_, ls_attr.ls_id_, stats))) {
    LOG_WARN("failed to update stats", K(ret));
  }
  return ret;
}

int ObBackupCleanLSTaskMgr::cancel(int64_t &finish_cnt)
{
  // INIT: just update status to FINISH and result to OB_CANCELED
  // PENDING: call cancel_task interface of task_scheduler, and update status to FINISH and result to OB_CANCELED;
  // DOING: call cancel_task interface of task_scheduler and update status to FINISH and result to OB_CANCELED;
  // FINISH: just update status to FINISH and result to OB_CANCELED
  int ret = OB_SUCCESS;
  int64_t end_ts = ObTimeUtility::current_time();
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::FINISH;
  if (ObBackupTaskStatus::Status::PENDING == ls_attr_->status_.status_
      || ObBackupTaskStatus::Status::DOING == ls_attr_->status_.status_) {
    if (OB_FAIL(task_scheduler_->cancel_tasks(
        BackupJobType::BACKUP_CLEAN_JOB, ls_attr_->job_id_, ls_attr_->tenant_id_))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to cancel task", K(ret), K(*ls_attr_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(advance_ls_task_status(*backup_service_, *sql_proxy_, *ls_attr_, next_status, OB_CANCELED, end_ts))) {
    LOG_WARN("failed to advance status", K(ret), K(*ls_attr_), K(next_status));
  } else {
    ++finish_cnt;
  }
  return ret;
}
}
}
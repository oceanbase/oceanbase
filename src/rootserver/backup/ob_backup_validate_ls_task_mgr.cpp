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
#include "ob_backup_validate_ls_task_mgr.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "ob_backup_task_scheduler.h"

namespace oceanbase
{
namespace rootserver
{

using namespace share;

ObBackupValidateLSTaskMgr::ObBackupValidateLSTaskMgr()
 : is_inited_(false),
   job_attr_(nullptr),
   task_attr_(nullptr),
   ls_attr_(nullptr),
   task_scheduler_(nullptr),
   sql_proxy_(nullptr),
   validate_service_(nullptr)
{
}

ObBackupValidateLSTaskMgr::~ObBackupValidateLSTaskMgr()
{
}

int ObBackupValidateLSTaskMgr::init(
    share::ObBackupValidateJobAttr &job_attr,
    share::ObBackupValidateTaskAttr &task_attr,
    share::ObBackupValidateLSTaskAttr &ls_attr,
    ObBackupTaskScheduler &task_scheduler,
    common::ObISQLClient &sql_proxy,
    ObBackupService &validate_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_INIT)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!ls_attr.is_valid() || !task_attr.is_valid() || !job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_attr), K(task_attr), K(job_attr));
  } else {
    job_attr_ = &job_attr;
    task_attr_ = &task_attr;
    ls_attr_ = &ls_attr;
    task_scheduler_ = &task_scheduler;
    sql_proxy_ = &sql_proxy;
    validate_service_ = &validate_service;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupValidateLSTaskMgr::reset()
{
  job_attr_ = nullptr;
  task_attr_ = nullptr;
  ls_attr_ = nullptr;
  task_scheduler_ = nullptr;
  sql_proxy_ = nullptr;
  validate_service_ = nullptr;
  is_inited_ = false;
}

int ObBackupValidateLSTaskMgr::process(int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_NOT_INIT)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    LOG_INFO("schedule backup validate ls task", K(*ls_attr_));
    switch (ls_attr_->status_.status_) {
      case ObBackupTaskStatus::Status::INIT: {
        if (OB_FAIL(add_task_())) {
          LOG_WARN("failed to add task into task schedulers", KR(ret), KP(ls_attr_));
        }
        break;
      }
      case ObBackupTaskStatus::Status::PENDING:
      case ObBackupTaskStatus::Status::DOING:
        break;
      case ObBackupTaskStatus::Status::FINISH: {
        if (OB_FAIL(finish_(finish_cnt))) {
          LOG_WARN("failed to finish task", KR(ret), KP(ls_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown backup validate ls task status", KR(ret), KP(ls_attr_));
      }
    }
  }
  return ret;
}

int ObBackupValidateLSTaskMgr::add_task_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateLSTask task;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::PENDING;
  if (OB_FAIL(task.build(*task_attr_, *ls_attr_))) {
    LOG_WARN("failed to build task", KR(ret), KP(task_attr_), KP(ls_attr_));
  } else if (OB_FAIL(validate_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupValidateLSTaskOperator::advance_ls_task_status(*sql_proxy_, *ls_attr_, next_status,
                                                OB_SUCCESS, 0/*end_ts*/))) {
    LOG_WARN("failed to advance ls task status", KR(ret), K(*task_attr_));
  } else if (OB_FAIL(task_scheduler_->add_task(task))) {
    LOG_WARN("failed to add validate ls task", KR(ret), K(*task_attr_));
  } else {
    LOG_INFO("[BACKUP_VALIDATE]success add validate ls task", K(*task_attr_), K(*ls_attr_));
  }
  return ret;
}

int ObBackupValidateLSTaskMgr::finish_(int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ls_attr_->result_) {
    finish_cnt++;
  } else if (ObBackupUtils::is_need_retry_error(ls_attr_->result_)) {
    int64_t cur_ts = ObTimeUtility::current_time();
    if (cur_ts < ls_attr_->end_ts_ + OB_BACKUP_RETRY_TIME_INTERVAL) {
      validate_service_->set_idle_time(OB_BACKUP_RETRY_TIME_INTERVAL);
    } else {
      if (OB_FAIL(validate_service_->check_leader())) {
        LOG_WARN("failed to check leader", K(ret));
      } else if (OB_FAIL(ObBackupValidateLSTaskOperator::redo_ls_task(*sql_proxy_, *ls_attr_, ls_attr_->retry_id_ + 1))) {
        LOG_WARN("failed to redo ls task", K(ret), KP(ls_attr_));
      } else {
        validate_service_->wakeup();
      }
    }
  } else {
    ret = ls_attr_->result_;
    int tmp_ret = OB_SUCCESS;
    if (!ls_attr_->comment_.is_empty() && OB_TMP_FAIL(job_attr_->comment_.assign(ls_attr_->comment_))) {
      LOG_WARN("failed to assign comment", K(ret), K(tmp_ret));
    }
    LOG_WARN("ls task failed, validate can't not continue", K(ret), KPC(ls_attr_));
  }
  return ret;
}

int ObBackupValidateLSTaskMgr::cancel(int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  int64_t end_ts = ObTimeUtility::current_time();
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::FINISH;
  if (ObBackupTaskStatus::Status::PENDING == ls_attr_->status_.status_
      || ObBackupTaskStatus::Status::DOING == ls_attr_->status_.status_) {
    if (OB_FAIL(task_scheduler_->cancel_tasks(
        BackupJobType::VALIDATE_JOB, ls_attr_->job_id_, ls_attr_->tenant_id_))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to cancel task", K(ret), K(*ls_attr_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(validate_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupValidateLSTaskOperator::advance_ls_task_status(*sql_proxy_, *ls_attr_,
                                                next_status, OB_CANCELED, end_ts))) {
    LOG_WARN("failed to advance status", KR(ret), K(*ls_attr_), K(next_status));
  } else {
    ++finish_cnt;
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase

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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_validate_scheduler.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "rootserver/backup/ob_root_validate.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace share {

ObValidateScheduler::ObValidateScheduler() : is_inited_(false), task_info_(), sql_proxy_(NULL), root_validate_(NULL)
{}

ObValidateScheduler::~ObValidateScheduler()
{}

int ObValidateScheduler::init(const uint64_t tenant_id, const int64_t backup_set_id, common::ObMySQLProxy& sql_proxy,
    rootserver::ObRootValidate& root_validate)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("validate scheduler init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("validate scheduler get invalid argument", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(backup_history_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init backup history updater", K(ret));
  } else if (OB_FAIL(task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init backup validate task updater", K(ret));
  } else if (OB_FAIL(backup_info_mgr_.init(OB_SYS_TENANT_ID, sql_proxy))) {
    LOG_WARN("failed to init backup info manager", K(ret));
  } else {
    task_info_.tenant_id_ = tenant_id;
    task_info_.tenant_name_ = OB_SYS_TENANT_ID == tenant_id ? OB_SYS_TENANT_NAME : "normal tenant";
    task_info_.backup_set_id_ = backup_set_id;
    task_info_.incarnation_ = OB_START_INCARNATION;
    task_info_.progress_percent_ = 0;
    sql_proxy_ = &sql_proxy;
    root_validate_ = &root_validate;
    is_inited_ = true;
  }
  return ret;
}

int ObValidateScheduler::start_schedule_validate()
{
  int ret = OB_SUCCESS;
  int64_t job_id = 0;
  const uint64_t tenant_id = task_info_.tenant_id_;
  const int64_t backup_set_id = task_info_.backup_set_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_backup_set_id_valid(tenant_id, backup_set_id))) {
    LOG_WARN("backup set id is not valid", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(backup_info_mgr_.get_job_id(job_id))) {
    LOG_WARN("failed to get job id", K(ret));
  } else if (FALSE_IT(task_info_.job_id_ = job_id)) {
    // do nothing
  } else if (OB_FAIL(start_validate())) {
    LOG_WARN("failed to schedule validate", K(ret));
  } else {
    LOG_INFO("start schedule validate");
  }
  return ret;
}

int ObValidateScheduler::get_log_archive_time_range(const uint64_t tenant_id, int64_t& start_ts, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo log_archive_info;
  bool for_update = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else if (OB_FAIL(
                 log_archive_mgr_.get_log_archive_backup_info(*sql_proxy_, for_update, tenant_id, log_archive_info))) {
    LOG_WARN("failed to get log archive backup info", K(ret));
  } else {
    start_ts = log_archive_info.status_.start_ts_;
    checkpoint_ts = log_archive_info.status_.checkpoint_ts_;
  }
  return ret;
}

int ObValidateScheduler::check_backup_set_id_valid(const uint64_t tenant_id, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = 0;
  int64_t checkpoint_ts = 0;
  ObTenantBackupTaskInfo backup_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else if (0 == backup_set_id) {
    // do nothing
  } else if (OB_FAIL(get_log_archive_time_range(tenant_id, start_ts, checkpoint_ts))) {
    LOG_WARN("failed to get log archive time range", K(tenant_id));
  } else if (OB_FAIL(backup_history_updater_.get_tenant_backup_task(
                 tenant_id, backup_set_id, OB_START_INCARNATION, backup_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(backup_set_id));
  } else if (!backup_info.is_valid()) {
    ret = OB_INVALID_BACKUP_SET_ID;
    LOG_WARN("get tenant backup task get invalid argument", K(ret));
  }
  return ret;
}

int ObValidateScheduler::start_validate()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("validate scheduler do not init", K(ret));
  } else {
    task_info_.status_ = ObBackupValidateTaskInfo::SCHEDULE;
    if (OB_FAIL(task_updater_.insert_task(task_info_))) {
      LOG_WARN("failed to insert backup validate task info", K(ret), K(task_info_));
    } else {
      root_validate_->update_prepare_flag(false);
      root_validate_->wakeup();
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

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
#include "share/backup/ob_validate_task_updater.h"
#include "share/backup/ob_validate_operator.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace share {

ObBackupValidateTaskUpdater::ObBackupValidateTaskUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

ObBackupValidateTaskUpdater::~ObBackupValidateTaskUpdater()
{}

int ObBackupValidateTaskUpdater::init(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup validate task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_client;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateTaskUpdater::insert_task(const ObBackupValidateTaskInfo& validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::insert_task(validate_tasks, *sql_proxy_))) {
    LOG_WARN("failed to insert task", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::get_task(const int64_t job_id, ObBackupValidateTaskInfo& validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(job_id));
  } else if (OB_FAIL(ObBackupValidateOperator::get_task(job_id, *sql_proxy_, validate_task))) {
    LOG_WARN("failed to get task", K(ret), K(job_id));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
    ObBackupValidateTaskInfo& validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(
                 ObBackupValidateOperator::get_task(job_id, tenant_id, backup_set_id, *sql_proxy_, validate_task))) {
    LOG_WARN("failed to get task", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::get_tasks(
    const int64_t job_id, const uint64_t tenant_id, common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::get_tasks(job_id, tenant_id, *sql_proxy_, validate_tasks))) {
    LOG_WARN("failed to get task", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::get_all_tasks(common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::get_all_tasks(*sql_proxy_, validate_tasks))) {
    LOG_WARN("failed to get all tasks", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::get_not_finished_tasks(common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::get_not_finished_tasks(*sql_proxy_, validate_tasks))) {
    LOG_WARN("failed to get task", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::update_task(
    const ObBackupValidateTaskInfo& src_task_info, const ObBackupValidateTaskInfo& dst_task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (!src_task_info.is_valid() || !dst_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::report_task(dst_task_info, *sql_proxy_))) {
    LOG_WARN("failed to update task", K(ret));
  }
  return ret;
}

int ObBackupValidateTaskUpdater::remove_task(const int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateOperator::remove_task(job_id, *sql_proxy_))) {
    LOG_WARN("failed to update task", K(ret));
  }
  return ret;
}

ObBackupValidateHistoryUpdater::ObBackupValidateHistoryUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

ObBackupValidateHistoryUpdater::~ObBackupValidateHistoryUpdater()
{}

int ObBackupValidateHistoryUpdater::init(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup validate task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_client;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateHistoryUpdater::insert_task(const ObBackupValidateTaskInfo& validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_FAIL(ObBackupValidateHistoryOperator::insert_task(validate_task, *sql_proxy_))) {
    LOG_WARN("failed to insert task", K(ret), K(validate_task));
  }
  return ret;
}

int ObBackupValidateHistoryUpdater::get_task(const int64_t job_id, const uint64_t tenant_id,
    const int64_t backup_set_id, ObBackupValidateTaskInfo& validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateHistoryOperator::get_task(
                 job_id, tenant_id, backup_set_id, *sql_proxy_, validate_task))) {
    LOG_WARN("failed to get task", K(ret));
  }
  return ret;
}

int ObBackupValidateHistoryUpdater::get_tasks(
    const int64_t job_id, const uint64_t tenant_id, common::ObIArray<ObBackupValidateTaskInfo>& validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateHistoryOperator::get_tasks(job_id, tenant_id, *sql_proxy_, validate_tasks))) {
    LOG_WARN("failed to get task", K(ret));
  }
  return ret;
}

int ObBackupValidateHistoryUpdater::remove_task(const int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate task updater do not init", K(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(ObBackupValidateHistoryOperator::remove_task(job_id, *sql_proxy_))) {
    LOG_WARN("failed to update task", K(ret));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

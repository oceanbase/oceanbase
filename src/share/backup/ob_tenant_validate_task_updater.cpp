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
#include "share/backup/ob_tenant_validate_task_updater.h"
#include "share/backup/ob_validate_operator.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace share {

ObTenantValidateTaskUpdater::ObTenantValidateTaskUpdater()
    : is_inited_(false), is_dropped_tenant_(false), sql_proxy_(NULL)
{}

int ObTenantValidateTaskUpdater::init(common::ObISQLClient& sql_client, bool is_dropped_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant validate task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_client;
    is_inited_ = true;
    is_dropped_tenant_ = is_dropped_tenant;
  }
  return ret;
}

int ObTenantValidateTaskUpdater::insert_task(const ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : tenant_validate_task.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (!tenant_validate_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::insert_task(tenant_id, tenant_validate_task, *sql_proxy_))) {
    LOG_WARN("failed to insert tenant validate task");
  }
  return ret;
}

int ObTenantValidateTaskUpdater::get_task(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (task_tenant_id == OB_INVALID_ID || job_id < 0 || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::get_task(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, *sql_proxy_, tenant_validate_task))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::get_task(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (task_tenant_id == OB_INVALID_ID || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::get_task(
                 tenant_id, job_id, task_tenant_id, incarnation, *sql_proxy_, tenant_validate_task))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::get_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (task_tenant_id == OB_INVALID_ID || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(task_tenant_id), K(job_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::get_tasks(
                 tenant_id, job_id, task_tenant_id, *sql_proxy_, tenant_validate_tasks))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::get_not_finished_tasks(
    const uint64_t task_tenant_id, common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (task_tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::get_not_finished_tasks(
                 tenant_id, task_tenant_id, *sql_proxy_, tenant_validate_tasks))) {
    LOG_WARN("failed to get not finished tasks", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::get_finished_task(
    const int64_t job_id, const uint64_t task_tenant_id, ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (task_tenant_id == OB_INVALID_ID || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(job_id), K(task_tenant_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::get_finished_task(
                 tenant_id, job_id, task_tenant_id, *sql_proxy_, tenant_validate_task))) {
    LOG_WARN("failed to get finished task", K(ret), K(job_id), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::update_task(
    const ObTenantValidateTaskInfo& src_task_info, const ObTenantValidateTaskInfo& dst_task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : src_task_info.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (!src_task_info.is_valid() || !dst_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::report_task(tenant_id, dst_task_info, *sql_proxy_))) {
    LOG_WARN("failed to update tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidateTaskUpdater::remove_task(
    const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate task updater get invalid argument", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(ObTenantValidateTaskOperator::remove_task(
                 tenant_id, job_id, task_tenant_id, incarnation, *sql_proxy_))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

ObTenantValidateHistoryUpdater::ObTenantValidateHistoryUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObTenantValidateHistoryUpdater::init(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant validate history updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_client;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantValidateHistoryUpdater::insert_task(const ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!tenant_validate_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate history updater get invalid arguments");
  } else if (OB_FAIL(ObTenantValidateHistoryOperator::insert_task(tenant_validate_task, *sql_proxy_))) {
    LOG_WARN("failed to insert tenant validate history task", K(ret));
  }
  return ret;
}

int ObTenantValidateHistoryUpdater::get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, ObTenantValidateTaskInfo& tenant_validate_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate history updater get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantValidateHistoryOperator::get_task(
                 job_id, tenant_id, incarnation, backup_set_id, *sql_proxy_, tenant_validate_task))) {
    LOG_WARN("failed to get tenant validate task", K(ret));
  }
  return ret;
}

int ObTenantValidateHistoryUpdater::get_tasks(
    const int64_t job_id, const uint64_t tenant_id, common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate history updater do not init", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 ObTenantValidateHistoryOperator::get_tasks(job_id, tenant_id, *sql_proxy_, tenant_validate_tasks))) {
    LOG_WARN("failed to get tenant validate history task", K(ret));
  }
  return ret;
}

int ObTenantValidateHistoryUpdater::remove_task(
    const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant validate history updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant validate history updater get invalid arguments", K(ret));
  } else if (OB_FAIL(ObTenantValidateHistoryOperator::remove_task(
                 job_id, tenant_id, incarnation, backup_set_id, *sql_proxy_))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

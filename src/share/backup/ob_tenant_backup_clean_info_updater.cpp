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
#include "ob_tenant_backup_clean_info_updater.h"

using namespace oceanbase;
using namespace common;
using namespace share;

ObTenantBackupCleanInfoUpdater::ObTenantBackupCleanInfoUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObTenantBackupCleanInfoUpdater::init(common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup clean info updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::insert_backup_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clean info updater do not init", K(ret), K(clean_info));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup clean info is invalid", K(ret), K(clean_info));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::insert_clean_info(tenant_id, clean_info, *sql_proxy_))) {
    LOG_WARN("failed to insert tenant backup task", K(ret), K(clean_info));
  }

  return ret;
}

int ObTenantBackupCleanInfoUpdater::get_backup_clean_info(const uint64_t tenant_id, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clean info updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == clean_info.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup clean info get invalid argument", K(ret), K(tenant_id), K(clean_info));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::get_tenant_clean_info(tenant_id, clean_info, *sql_proxy_))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get tenant backup clean info", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::update_backup_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& src_clean_info, const ObBackupCleanInfo& dest_clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clean info updater do not init", K(ret));
  } else if (!src_clean_info.is_valid() || !dest_clean_info.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tenant backup clean info get invalid argument",
        K(ret),
        K(src_clean_info),
        K(dest_clean_info),
        K(tenant_id));
  } else if (OB_FAIL(check_can_update_backup_clean_info(src_clean_info.status_, dest_clean_info.status_))) {
    LOG_WARN("failed to check can update backup task", K(ret), K(src_clean_info), K(dest_clean_info));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::update_clean_info(tenant_id, dest_clean_info, *sql_proxy_))) {
    LOG_WARN("failed to update tenant backup clean info", K(ret), K(dest_clean_info));
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::remove_clean_info(const uint64_t tenant_id, const ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(clean_info));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::remove_clean_info(tenant_id, clean_info, *sql_proxy_))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id), K(clean_info));
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::check_can_update_backup_clean_info(
    const ObBackupCleanInfoStatus::STATUS& src_status, const ObBackupCleanInfoStatus::STATUS& dest_status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (src_status > ObBackupCleanInfoStatus::MAX || dest_status > ObBackupCleanInfoStatus::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src or dest status is invalid", K(ret), K(src_status), K(dest_status));
  } else {
    switch (src_status) {
      case ObBackupCleanInfoStatus::STOP:
        if (ObBackupCleanInfoStatus::PREPARE != dest_status && ObBackupCleanInfoStatus::DOING != dest_status &&
            ObBackupCleanInfoStatus::STOP != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup clean info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupCleanInfoStatus::PREPARE:
        if (ObBackupCleanInfoStatus::DOING != dest_status && ObBackupCleanInfoStatus::STOP != dest_status &&
            ObBackupCleanInfoStatus::PREPARE != dest_status && ObBackupCleanInfoStatus::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup clean info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupCleanInfoStatus::DOING:
        if (ObBackupCleanInfoStatus::STOP != dest_status && ObBackupCleanInfoStatus::DOING != dest_status &&
            ObBackupCleanInfoStatus::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup clean info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupCleanInfoStatus::CANCEL:
        if (ObBackupCleanInfoStatus::STOP != dest_status && ObBackupCleanInfoStatus::CANCEL != dest_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not update backup clean info", K(ret), K(src_status), K(dest_status));
        }
        break;
      case ObBackupCleanInfoStatus::MAX:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not update backup clean info", K(ret), K(src_status), K(dest_status));
        break;
    }
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::get_backup_clean_info_status(
    const uint64_t tenant_id, ObISQLClient& trans, ObBackupCleanInfoStatus::STATUS& status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup task updater do not init", K(ret));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::get_clean_info_status(tenant_id, trans, status))) {
    LOG_WARN("failed to remove task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantBackupCleanInfoUpdater::get_deleted_tenant_clean_infos(
    common::ObIArray<ObBackupCleanInfo>& deleted_tenant_clean_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clean info updater do not init", K(ret));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::get_deleted_tenant_clean_infos(
                 *sql_proxy_, deleted_tenant_clean_infos))) {
    LOG_WARN("failed to get tenant backup clean info", K(ret));
  }
  return ret;
}

ObBackupCleanInfoHistoryUpdater::ObBackupCleanInfoHistoryUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObBackupCleanInfoHistoryUpdater::init(common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup clean info history update init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupCleanInfoHistoryUpdater::insert_backup_clean_info(const ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean info history is not init", K(ret));
  } else if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean info is invalid", K(ret), K(clean_info));
  } else if (OB_FAIL(ObBackupCleanInfoHistoryOperator::insert_clean_info(clean_info, *sql_proxy_))) {
    if (OB_ERR_DUP_KEY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to insert clean info into history", K(ret), K(clean_info));
    }
  }
  return ret;
}

int ObBackupCleanInfoHistoryUpdater::remove_backup_clean_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean info history is not init", K(ret));
  } else if (OB_FAIL(ObBackupCleanInfoHistoryOperator::remove_tenant_clean_info(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to insert clean info into history", K(ret));
  }
  return ret;
}

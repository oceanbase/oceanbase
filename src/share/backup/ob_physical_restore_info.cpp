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
#include "share/backup/ob_physical_restore_info.h"
using namespace oceanbase;
using namespace common;
using namespace share;

void ObRestoreProgressInfo::reset()
{
  total_pg_cnt_ = 0;
  finish_pg_cnt_ = 0;
  total_partition_cnt_ = 0;
  finish_partition_cnt_ = 0;
}

ObPhysicalRestoreJob::ObPhysicalRestoreJob()
{
  reset();
}

bool ObPhysicalRestoreJob::is_valid() const
{
  return OB_INVALID_ID != job_id_ && PhysicalRestoreStatus::PHYSICAL_RESTORE_MAX_STATUS != status_;
}

int ObPhysicalRestoreJob::assign(const ObPhysicalRestoreJob& other)
{
  int ret = OB_SUCCESS;
  /* rs */
  job_id_ = other.job_id_;
  tenant_id_ = other.tenant_id_;
  restore_data_version_ = other.restore_data_version_;
  status_ = other.status_;
  restore_start_ts_ = other.restore_start_ts_;
  STRNCPY(info_, other.info_, common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  info_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH - 1] = '\0';
  pre_cluster_version_ = other.pre_cluster_version_;
  post_cluster_version_ = other.post_cluster_version_;
  /* uri */
  restore_job_id_ = other.restore_job_id_;
  restore_timestamp_ = other.restore_timestamp_;
  cluster_id_ = other.cluster_id_;
  STRNCPY(restore_option_, other.restore_option_, common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  STRNCPY(backup_dest_, other.backup_dest_, share::OB_MAX_BACKUP_DEST_LENGTH);
  STRNCPY(tenant_name_, other.tenant_name_, common::OB_MAX_TENANT_NAME_LENGTH_STORE);
  STRNCPY(backup_tenant_name_, other.backup_tenant_name_, common::OB_MAX_TENANT_NAME_LENGTH_STORE);
  STRNCPY(backup_cluster_name_, other.backup_cluster_name_, common::OB_MAX_CLUSTER_NAME_LENGTH);
  STRNCPY(pool_list_, other.pool_list_, common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  STRNCPY(locality_, other.locality_, common::MAX_LOCALITY_LENGTH);
  STRNCPY(primary_zone_, other.primary_zone_, common::MAX_ZONE_LENGTH);
  restore_option_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH - 1] = '\0';
  backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH - 1] = '\0';
  tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE - 1] = '\0';
  backup_tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE - 1] = '\0';
  backup_cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH - 1] = '\0';
  pool_list_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH - 1] = '\0';
  locality_[common::MAX_LOCALITY_LENGTH - 1] = '\0';
  primary_zone_[common::MAX_ZONE_LENGTH - 1] = '\0';
  MEMCPY(passwd_array_, other.passwd_array_, sizeof(passwd_array_));
  /* oss */
  STRNCPY(backup_locality_, other.backup_locality_, common::MAX_LOCALITY_LENGTH);
  STRNCPY(backup_primary_zone_, other.backup_primary_zone_, common::MAX_ZONE_LENGTH);
  backup_locality_[common::MAX_LOCALITY_LENGTH - 1] = '\0';
  backup_primary_zone_[common::MAX_ZONE_LENGTH - 1] = '\0';
  compat_mode_ = other.compat_mode_;
  backup_tenant_id_ = other.backup_tenant_id_;
  incarnation_ = other.incarnation_;
  full_backup_set_id_ = other.full_backup_set_id_;
  inc_backup_set_id_ = other.inc_backup_set_id_;
  log_archive_round_ = other.log_archive_round_;
  snapshot_version_ = other.snapshot_version_;
  schema_version_ = other.schema_version_;
  frozen_data_version_ = other.frozen_data_version_;
  frozen_snapshot_version_ = other.frozen_snapshot_version_;
  frozen_schema_version_ = other.frozen_schema_version_;
  source_cluster_version_ = other.source_cluster_version_;
  compatible_ = other.compatible_;
  return ret;
}

int ObPhysicalRestoreJob::assign(ObRestoreBackupInfo& backup_info)
{
  int ret = OB_SUCCESS;
  STRNCPY(backup_locality_, backup_info.locality_, common::MAX_LOCALITY_LENGTH);
  STRNCPY(backup_primary_zone_, backup_info.primary_zone_, common::MAX_ZONE_LENGTH);
  backup_locality_[common::MAX_LOCALITY_LENGTH - 1] = '\0';
  backup_primary_zone_[common::MAX_ZONE_LENGTH - 1] = '\0';
  compat_mode_ = backup_info.compat_mode_;
  backup_tenant_id_ = backup_info.physical_restore_info_.tenant_id_;
  incarnation_ = backup_info.physical_restore_info_.incarnation_;
  full_backup_set_id_ = backup_info.physical_restore_info_.full_backup_set_id_;
  inc_backup_set_id_ = backup_info.physical_restore_info_.inc_backup_set_id_;
  log_archive_round_ = backup_info.physical_restore_info_.log_archive_round_;
  snapshot_version_ = backup_info.snapshot_version_;
  schema_version_ = backup_info.schema_version_;
  frozen_data_version_ = backup_info.frozen_data_version_;
  frozen_snapshot_version_ = backup_info.frozen_snapshot_version_;
  frozen_schema_version_ = backup_info.frozen_schema_version_;
  source_cluster_version_ = backup_info.physical_restore_info_.cluster_version_;
  // init
  pre_cluster_version_ = backup_info.physical_restore_info_.cluster_version_;
  post_cluster_version_ = backup_info.physical_restore_info_.cluster_version_;
  compatible_ = backup_info.physical_restore_info_.compatible_;

  return ret;
}

int ObPhysicalRestoreJob::copy_to(ObPhysicalRestoreInfo& restore_info) const
{
  int ret = OB_SUCCESS;
  STRNCPY(restore_info.backup_dest_, backup_dest_, share::OB_MAX_BACKUP_DEST_LENGTH);
  STRNCPY(restore_info.cluster_name_, backup_cluster_name_, common::OB_MAX_CLUSTER_NAME_LENGTH);
  restore_info.backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH - 1] = '\0';
  restore_info.cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH - 1] = '\0';
  restore_info.cluster_id_ = cluster_id_;
  restore_info.incarnation_ = incarnation_;
  restore_info.tenant_id_ = backup_tenant_id_;
  restore_info.full_backup_set_id_ = full_backup_set_id_;
  restore_info.inc_backup_set_id_ = inc_backup_set_id_;
  restore_info.log_archive_round_ = log_archive_round_;
  restore_info.restore_snapshot_version_ = restore_timestamp_;
  restore_info.restore_start_ts_ = restore_start_ts_;
  restore_info.compatible_ = compatible_;
  restore_info.cluster_version_ = source_cluster_version_;
  return ret;
}

int ObPhysicalRestoreJob::copy_to(ObSimplePhysicalRestoreJob& simple_job_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_to(simple_job_info.restore_info_))) {
    LOG_WARN("failed to copy physical restore info", K(ret), K(*this));
  } else {
    simple_job_info.snapshot_version_ = snapshot_version_;
    simple_job_info.schema_version_ = schema_version_;
    simple_job_info.job_id_ = job_id_;
  }
  return ret;
}

void ObPhysicalRestoreJob::reset()
{
  /* rs */
  job_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  restore_data_version_ = 0;
  status_ = PhysicalRestoreStatus::PHYSICAL_RESTORE_MAX_STATUS;
  restore_start_ts_ = 0;
  MEMSET(info_, '\0', common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  pre_cluster_version_ = 0;
  post_cluster_version_ = 0;
  /* uri */
  restore_job_id_ = OB_INVALID_ID;
  restore_timestamp_ = OB_INVALID_VERSION;
  cluster_id_ = OB_INVALID_ID;
  MEMSET(restore_option_, '\0', common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  MEMSET(backup_dest_, '\0', share::OB_MAX_BACKUP_DEST_LENGTH);
  MEMSET(tenant_name_, '\0', common::OB_MAX_TENANT_NAME_LENGTH_STORE);
  MEMSET(backup_tenant_name_, '\0', common::OB_MAX_TENANT_NAME_LENGTH_STORE);
  MEMSET(backup_cluster_name_, '\0', common::OB_MAX_CLUSTER_NAME_LENGTH);
  MEMSET(pool_list_, '\0', common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
  MEMSET(locality_, '\0', common::MAX_LOCALITY_LENGTH);
  MEMSET(primary_zone_, '\0', common::MAX_ZONE_LENGTH);
  passwd_array_[0] = '\0';
  /* oss */
  MEMSET(backup_locality_, '\0', common::MAX_LOCALITY_LENGTH);
  MEMSET(backup_primary_zone_, '\0', common::MAX_ZONE_LENGTH);
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  backup_tenant_id_ = OB_INVALID_TENANT_ID;
  incarnation_ = OB_INVALID_ID;
  full_backup_set_id_ = OB_INVALID_ID;
  inc_backup_set_id_ = OB_INVALID_ID;
  log_archive_round_ = OB_INVALID_ID;
  snapshot_version_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_ID;
  frozen_data_version_ = OB_INVALID_ID;
  frozen_snapshot_version_ = OB_INVALID_ID;
  frozen_schema_version_ = OB_INVALID_ID;
  source_cluster_version_ = 0;
  compatible_ = 0;
}

ObSimplePhysicalRestoreJob::ObSimplePhysicalRestoreJob()
    : restore_info_(), snapshot_version_(0), schema_version_(0), job_id_(0)
{}

bool ObSimplePhysicalRestoreJob::is_valid() const
{
  return restore_info_.is_valid() && snapshot_version_ > 0 && schema_version_ > 0 && job_id_ > 0;
}

int ObSimplePhysicalRestoreJob::assign(const ObSimplePhysicalRestoreJob& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_.assign(other.restore_info_))) {
    LOG_WARN("failed to assign restore info", K(ret), K(other));
  } else {
    snapshot_version_ = other.snapshot_version_;
    schema_version_ = other.schema_version_;
    job_id_ = other.job_id_;
  }
  return ret;
}

int ObSimplePhysicalRestoreJob::copy_to(ObPhysicalRestoreInfo& resotre_info) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("simple physical restore job do not init", K(ret), K(*this));
  } else if (OB_FAIL(resotre_info.assign(restore_info_))) {
    LOG_WARN("failed to assign restore info", K(ret), K(*this));
  }
  return ret;
}

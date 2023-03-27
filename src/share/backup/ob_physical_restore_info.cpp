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
#include "share/backup/ob_backup_struct.h"
#include <algorithm>
#include "share/ob_rpc_struct.h"
using namespace oceanbase;
using namespace common;
using namespace share;

ObPhysicalRestoreWhiteList::ObPhysicalRestoreWhiteList()
    : allocator_("PhyReWhiteList"), table_items_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_))
{}

ObPhysicalRestoreWhiteList::~ObPhysicalRestoreWhiteList()
{}

void ObPhysicalRestoreWhiteList::reset()
{
  table_items_.reset();
  allocator_.reset();
}

int ObPhysicalRestoreWhiteList::assign(const ObPhysicalRestoreWhiteList& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(table_items_.reserve(other.table_items_.count()))) {
      LOG_WARN("fail to reserve", KR(ret), K(other));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.table_items_.count(); i++) {
      const obrpc::ObTableItem& item = other.table_items_.at(i);
      if (OB_FAIL(add_table_item(item))) {
        LOG_WARN("fail to add table item", KR(ret), K(item));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreWhiteList::add_table_item(const obrpc::ObTableItem& other)
{
  int ret = OB_SUCCESS;
  obrpc::ObTableItem item;
  if (OB_FAIL(ob_write_string(allocator_, other.table_name_, item.table_name_))) {
    LOG_WARN("fail to assign table_name", KR(ret), K(other));
  } else if (OB_FAIL(ob_write_string(allocator_, other.database_name_, item.database_name_))) {
    LOG_WARN("fail to assign database_name", KR(ret), K(other));
  } else if (OB_FAIL(table_items_.push_back(item))) {
    LOG_WARN("fail to push back table_item", KR(ret), K(item));
  }
  return ret;
}

// str without '\0'
int64_t ObPhysicalRestoreWhiteList::get_format_str_length() const
{
  int64_t length = 0;
  for (int64_t i = 0; i < table_items_.count(); i++) {
    const obrpc::ObTableItem& item = table_items_.at(i);
    length += (item.database_name_.length() + item.table_name_.length() + 5  // '`' & '.'
               + (0 == i ? 0 : 1));                                          // ','
  }
  return length;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::get_format_str(common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* format_str_buf = NULL;
  int64_t format_str_length = get_format_str_length() + 1;
  if (format_str_length > OB_MAX_LONGTEXT_LENGTH + 1) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format str is too long", KR(ret), K(format_str_length));
  } else if (OB_ISNULL(format_str_buf = static_cast<char*>(allocator.alloc(format_str_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(format_str_length));
  } else if (table_items_.count() <= 0) {
    MEMSET(format_str_buf, '\0', format_str_length);
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const obrpc::ObTableItem& item = table_items_.at(i);
      if (OB_FAIL(databuff_printf(format_str_buf,
              format_str_length,
              pos,
              "%s`%.*s`.`%.*s`",
              0 == i ? "" : ",",
              item.database_name_.length(),
              item.database_name_.ptr(),
              item.table_name_.length(),
              item.table_name_.ptr()))) {
        LOG_WARN("fail to append str", KR(ret), K(item), K(pos), K(format_str_length));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(format_str_buf) || format_str_length <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format str", KR(ret), K(format_str_buf), K(format_str_length));
  } else {
    str.assign_ptr(format_str_buf, format_str_length - 1);
    LOG_DEBUG("get format white_list str", KR(ret), K(str));
  }
  return ret;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::get_hex_str(common::ObIAllocator& allocator, common::ObString& str) const
{
  int ret = OB_SUCCESS;
  char* serialize_buf = NULL;
  int64_t serialize_size = table_items_.get_serialize_size();
  int64_t serialize_pos = 0;
  char* hex_buf = NULL;
  int64_t hex_size = 2 * serialize_size;
  int64_t hex_pos = 0;
  if (serialize_size > OB_MAX_LONGTEXT_LENGTH / 2) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize_size is too long", KR(ret), K(serialize_size));
  } else if (OB_ISNULL(serialize_buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(table_items_.serialize(serialize_buf, serialize_size, serialize_pos))) {
    LOG_WARN("fail to serialize table_items", KR(ret), K_(table_items));
  } else if (serialize_pos > serialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialize error", KR(ret), K(serialize_pos), K(serialize_size));
  } else if (OB_ISNULL(hex_buf = static_cast<char*>(allocator.alloc(hex_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(hex_size));
  } else if (OB_FAIL(hex_print(serialize_buf, serialize_pos, hex_buf, hex_size, hex_pos))) {
    LOG_WARN("fail to print hex", KR(ret), K(serialize_pos), K(hex_size));
  } else if (hex_pos > hex_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("encode error", KR(ret), K(hex_pos), K(hex_size));
  } else {
    str.assign_ptr(hex_buf, hex_size);
    LOG_DEBUG("get hex white_list str", KR(ret), K(str));
  }
  return ret;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::assign_with_hex_str(const common::ObString& str)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t str_size = str.length();
  char* deserialize_buf = NULL;
  int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  if (str_size <= 0) {
    // skip
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator_.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", KR(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(table_items_.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize table_items",
        KR(ret),
        K(str),
        "deserialize_buf",
        ObString(deserialize_size, deserialize_buf));
  } else if (deserialize_pos > deserialize_size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
  } else {
    LOG_DEBUG("assign with hex str", KR(ret), K(str), K_(table_items));
  }
  return ret;
}

DEF_TO_STRING(ObPhysicalRestoreWhiteList)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_items));
  J_OBJ_END();
  return pos;
}

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
  return OB_INVALID_ID != job_id_ && PhysicalRestoreStatus::PHYSICAL_RESTORE_MAX_STATUS != status_ &&
         !(0 == strlen(backup_dest_) && 0 == multi_restore_path_list_.get_backup_set_path_list().count());
}

DEF_TO_STRING(ObPhysicalRestoreJob)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(job_id),
      K_(tenant_id),
      K_(restore_data_version),
      K_(status),
      K_(restore_start_ts),
      K_(restore_schema_version),
      K_(rebuild_index_schema_version),
      K_(restore_job_id),
      K_(restore_timestamp),
      K_(cluster_id),
      "restore_option",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, restore_option_),
      "backup_dest",
      common::ObString(share::OB_MAX_BACKUP_DEST_LENGTH, backup_dest_),
      "tenant_name",
      common::ObString(common::OB_MAX_TENANT_NAME_LENGTH_STORE, tenant_name_),
      "backup_tenant_name",
      common::ObString(common::OB_MAX_TENANT_NAME_LENGTH_STORE, backup_tenant_name_),
      "backup_cluster_name",
      common::ObString(common::OB_MAX_CLUSTER_NAME_LENGTH, backup_cluster_name_),
      "pool_list",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, pool_list_),
      "locality",
      common::ObString(common::MAX_LOCALITY_LENGTH, locality_),
      "primary_zone",
      common::ObString(common::MAX_ZONE_LENGTH, primary_zone_),
      "backup_locality",
      common::ObString(common::MAX_LOCALITY_LENGTH, backup_locality_),
      "backup_primary_zone",
      common::ObString(common::MAX_ZONE_LENGTH, backup_primary_zone_),
      "info",
      common::ObString(common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH, info_));
  J_KV(K_(compat_mode),
      K_(backup_tenant_id),
      K_(incarnation),
      K_(full_backup_set_id),
      K_(inc_backup_set_id),
      K_(log_archive_round),
      K_(snapshot_version),
      K_(schema_version),
      K_(frozen_data_version),
      K_(frozen_snapshot_version),
      K_(frozen_schema_version),
      K_(passwd_array),
      K_(source_cluster_version),
      K_(pre_cluster_version),
      K_(post_cluster_version),
      K_(compatible),
      K_(white_list),
      K_(multi_restore_path_list));
  J_OBJ_END();
  return pos;
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
  restore_schema_version_ = other.restore_schema_version_;
  rebuild_index_schema_version_ = other.rebuild_index_schema_version_;
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
  if (FAILEDx(multi_restore_path_list_.assign(other.multi_restore_path_list_))) {
    LOG_WARN("failed to assign multi_restore_path_list_", K(ret));
  }
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
  backup_date_ = other.backup_date_;
  if (FAILEDx(white_list_.assign(other.white_list_))) {
    LOG_WARN("fail to assign white list", KR(ret), K(other));
  }
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
  backup_date_ = backup_info.physical_restore_info_.backup_date_;
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
  restore_info.restore_schema_version_ = restore_schema_version_;
  restore_info.compatible_ = compatible_;
  restore_info.cluster_version_ = source_cluster_version_;
  restore_info.backup_date_ = backup_date_;
  restore_info.multi_restore_path_list_.assign(multi_restore_path_list_);
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
  restore_schema_version_ = OB_INVALID_VERSION;
  rebuild_index_schema_version_ = OB_INVALID_VERSION;
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
  multi_restore_path_list_.reset();
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
  backup_date_ = 0;
  white_list_.reset();
}

ObSimplePhysicalRestoreJob::ObSimplePhysicalRestoreJob()
    : restore_info_(), snapshot_version_(0), schema_version_(0), job_id_(0)
{}

void ObSimplePhysicalRestoreJob::reset()
{
  restore_info_.reset();
  snapshot_version_ = 0;
  schema_version_ = 0;
  job_id_ = 0;
}

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

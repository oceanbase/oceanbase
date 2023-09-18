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
#include "share/restore/ob_physical_restore_info.h"
#include "share/backup/ob_backup_struct.h"
#include <algorithm>
#include "share/ob_rpc_struct.h"
using namespace oceanbase;
using namespace common;
using namespace share;

ObPhysicalRestoreWhiteList::ObPhysicalRestoreWhiteList()
  : allocator_("PhyReWhiteList"),
    table_items_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_))
{
}

ObPhysicalRestoreWhiteList::~ObPhysicalRestoreWhiteList()
{
}

void ObPhysicalRestoreWhiteList::reset()
{
  table_items_.reset();
  allocator_.reset();
}

int ObPhysicalRestoreWhiteList::assign(const ObPhysicalRestoreWhiteList &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(table_items_.reserve(other.table_items_.count()))) {
      LOG_WARN("fail to reserve", KR(ret), K(other));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.table_items_.count(); i++) {
      const obrpc::ObTableItem &item = other.table_items_.at(i);
      if (OB_FAIL(add_table_item(item))) {
        LOG_WARN("fail to add table item", KR(ret), K(item));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreWhiteList::add_table_item(const obrpc::ObTableItem &other)
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
    const obrpc::ObTableItem &item = table_items_.at(i);
    length += (item.database_name_.length()
               + item.table_name_.length()
               + 5  // '`' & '.'
               + (0 == i ? 0 : 1)); // ','
  }
  return length;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::get_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  char *format_str_buf = NULL;
  int64_t format_str_length = get_format_str_length() + 1;
  if (format_str_length > OB_MAX_LONGTEXT_LENGTH + 1) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format str is too long", KR(ret), K(format_str_length));
  } else if (OB_ISNULL(format_str_buf = static_cast<char *>(allocator.alloc(format_str_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(format_str_length));
  } else if (table_items_.count() <= 0) {
    MEMSET(format_str_buf, '\0', format_str_length);
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); i++) {
      const obrpc::ObTableItem &item = table_items_.at(i);
      if (OB_FAIL(databuff_printf(format_str_buf, format_str_length, pos, "%s`%.*s`.`%.*s`",
                  0 == i ? "" : ",",
                  item.database_name_.length(), item.database_name_.ptr(),
                  item.table_name_.length(), item.table_name_.ptr()))) {
        LOG_WARN("fail to append str", KR(ret), K(item), K(pos), K(format_str_length));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(format_str_buf) || format_str_length <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format str", KR(ret), K(format_str_buf), K(format_str_length));
  } else {
    str.assign_ptr(format_str_buf, static_cast<int32_t>(format_str_length - 1));
    LOG_DEBUG("get format white_list str", KR(ret), K(str));
  }
  return ret;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::get_hex_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  char *serialize_buf = NULL;
  int64_t serialize_size = table_items_.get_serialize_size();
  int64_t serialize_pos = 0;
  char *hex_buf = NULL;
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
    str.assign_ptr(hex_buf, static_cast<int32_t>(hex_size));
    LOG_DEBUG("get hex white_list str", KR(ret), K(str));
  }
  return ret;
}

// str without '\0'
int ObPhysicalRestoreWhiteList::assign_with_hex_str(
    const common::ObString &str)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t str_size = str.length();
  char *deserialize_buf = NULL;
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
    LOG_WARN("fail to deserialize table_items", KR(ret), K(str),
             "deserialize_buf", ObString(deserialize_size, deserialize_buf));
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
  : allocator_("PhyRestoreJob")
{
  reset();
}
int ObPhysicalRestoreJob::init_restore_key(const uint64_t tenant_id, const int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(tenant_id), K(job_id));
  } else {
    restore_key_.tenant_id_ = tenant_id;
    restore_key_.job_id_ = job_id;
  }
  return ret;
}
bool ObPhysicalRestoreJob::is_valid() const
{
  return restore_key_.is_pkey_valid() 
        && PhysicalRestoreStatus::PHYSICAL_RESTORE_MAX_STATUS != status_;
}

DEF_TO_STRING(ObPhysicalRestoreJob)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
    K_(restore_key),
    K_(initiator_job_id),
    K_(initiator_tenant_id),
    K_(tenant_id),
    K_(backup_tenant_id),
    K_(restore_type),
    K_(status),
    K_(comment),
    K_(restore_start_ts),
    K_(restore_scn),
    K_(consistent_scn),
    K_(post_data_version),
    K_(source_cluster_version),
    K_(source_data_version),
    K_(restore_option),
    K_(backup_dest),
    K_(description),
    K_(tenant_name),
    K_(pool_list),
    K_(locality),
    K_(primary_zone),
    K_(compat_mode),
    K_(compatible),
    K_(kms_info),
    K_(kms_encrypt),
    K_(concurrency),
    K_(passwd_array),
    K_(multi_restore_path_list),
    K_(white_list),
    K_(recover_table)
  );
  J_OBJ_END();
  return pos;
}

int ObPhysicalRestoreJob::assign(const ObPhysicalRestoreJob &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    restore_key_ = other.restore_key_;
    initiator_job_id_ = other.initiator_job_id_;
    initiator_tenant_id_ = other.initiator_tenant_id_;
    tenant_id_ = other.tenant_id_;
    backup_tenant_id_ = other.backup_tenant_id_;
    restore_type_ = other.restore_type_;
    status_ = other.status_;
    restore_start_ts_ = other.restore_start_ts_;
    restore_scn_ = other.restore_scn_;
    consistent_scn_ = other.consistent_scn_;
    post_data_version_ = other.post_data_version_;
    source_cluster_version_ = other.source_cluster_version_;
    source_data_version_ = other.source_data_version_;
    compat_mode_ = other.compat_mode_;
    compatible_ = other.compatible_;
    kms_encrypt_ = other.kms_encrypt_;
    concurrency_ = other.concurrency_;
    recover_table_ = other.recover_table_;

    if (FAILEDx(deep_copy_ob_string(allocator_, other.comment_, comment_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.restore_option_, restore_option_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.backup_dest_, backup_dest_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.description_, description_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.tenant_name_, tenant_name_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.pool_list_, pool_list_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.locality_, locality_))) { 
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.primary_zone_, primary_zone_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.kms_info_, kms_info_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.encrypt_key_, encrypt_key_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.kms_dest_, kms_dest_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.kms_encrypt_key_, kms_encrypt_key_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.passwd_array_, passwd_array_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.backup_tenant_name_, backup_tenant_name_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.backup_cluster_name_, backup_cluster_name_))) {
      LOG_WARN("failed to copy string", KR(ret), K(other));
    } else if (OB_FAIL(multi_restore_path_list_.assign(other.multi_restore_path_list_))) {
      LOG_WARN("failed to assign path list", KR(ret), K(other));
    } else if (OB_FAIL(white_list_.assign(other.white_list_))) {
      LOG_WARN("failed to assign white list", KR(ret), K(other));
    }

  }
  return ret;
}

void ObPhysicalRestoreJob::reset()
{
  /* rs */
  restore_key_.reset();
  initiator_job_id_ = OB_INVALID_ID;
  initiator_tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  backup_tenant_id_ = OB_INVALID_TENANT_ID;
  restore_type_ = ObRestoreType::RESTORE_TYPE_MAX;
  status_ = PhysicalRestoreStatus::PHYSICAL_RESTORE_MAX_STATUS;
  comment_.reset();
  restore_start_ts_ = 0;
  restore_scn_ = SCN::min_scn();
  consistent_scn_ = SCN::min_scn();
  post_data_version_ = 0;
  source_cluster_version_ = 0;
  source_data_version_ = 0;
  restore_option_.reset();
  backup_dest_.reset();
  description_.reset();
  tenant_name_.reset();
  pool_list_.reset();
  locality_.reset();
  primary_zone_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  
  compatible_ = 0;
  kms_info_.reset();
  kms_encrypt_ = false;
  encrypt_key_.reset();
  kms_dest_.reset();
  kms_encrypt_key_.reset();
  concurrency_ = 0;
  recover_table_ = false;


  passwd_array_.reset();
  multi_restore_path_list_.reset();
  white_list_.reset();
  allocator_.reset();
}

int ObPhysicalRestoreJob::copy_to(ObSimplePhysicalRestoreJob &simple_job_info) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t len = OB_MAX_BACKUP_DEST_LENGTH;
  if (OB_FAIL(databuff_printf(simple_job_info.restore_info_.backup_dest_, len, pos, "%.*s",
          backup_dest_.length(), backup_dest_.ptr()))) {
    LOG_WARN("failed to copy to restore job", KR(ret), K(backup_dest_));
  } else if (OB_UNLIKELY(pos >= len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough", KR(ret), K(pos), K(len), K(backup_dest_));
  } else if (OB_FAIL(simple_job_info.restore_info_.multi_restore_path_list_.assign(multi_restore_path_list_))) {
    LOG_WARN("failed to assign multi path", KR(ret), K(multi_restore_path_list_));
  } else {
    simple_job_info.job_id_ = restore_key_.job_id_;
    simple_job_info.restore_info_.backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH - 1] = '\0';
    simple_job_info.restore_info_.tenant_id_ = backup_tenant_id_;
    simple_job_info.restore_info_.restore_snapshot_version_ = restore_scn_.get_val_for_inner_table_field();
    simple_job_info.restore_info_.restore_start_ts_ = restore_start_ts_;
    simple_job_info.restore_info_.compatible_ = compatible_;
    simple_job_info.restore_info_.cluster_version_ = source_cluster_version_;
  }
  return ret;
}

ObSimplePhysicalRestoreJob::ObSimplePhysicalRestoreJob()
  : restore_info_(),
    restore_data_version_(0),
    snapshot_version_(0),
    schema_version_(0),
    job_id_(0)
{
}

bool ObSimplePhysicalRestoreJob::is_valid() const
{
  return restore_info_.is_valid()
      && restore_data_version_ > 0
      && snapshot_version_ > 0
      && schema_version_ > 0
      && job_id_ > 0;
}

int ObSimplePhysicalRestoreJob::assign(const ObSimplePhysicalRestoreJob &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_.assign(other.restore_info_))) {
    LOG_WARN("failed to assign restore info", K(ret), K(other));
  } else {
    restore_data_version_ = other.restore_data_version_;
    snapshot_version_ = other.snapshot_version_;
    schema_version_ = other.schema_version_;
    job_id_ = other.job_id_;
  }
  return ret;
}

int ObSimplePhysicalRestoreJob::copy_to(ObPhysicalRestoreInfo &resotre_info) const
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



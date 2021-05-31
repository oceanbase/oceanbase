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
#include "ob_backup_struct.h"
#include "common/ob_record_header.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_lease_info_mgr.h"

using namespace oceanbase;
using namespace lib;
using namespace common;
using namespace share;

/******************ObBackupCommonHeader*******************/
ObBackupCommonHeader::ObBackupCommonHeader()
{
  reset();
}

void ObBackupCommonHeader::reset()
{
  memset(this, 0, sizeof(ObBackupCommonHeader));
  header_version_ = COMMON_HEADER_VERSION;
  compressor_type_ = static_cast<uint8_t>(common::ObCompressorType::INVALID_COMPRESSOR);
  data_type_ = static_cast<uint16_t>(ObBackupFileType::BACKUP_TYPE_MAX);
  header_length_ = static_cast<uint16_t>(sizeof(ObBackupCommonHeader));
}

void ObBackupCommonHeader::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupCommonHeader::set_checksum(const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_zlength_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_zlength));
  } else {
    data_checksum_ = ob_crc64(buf, len);
    header_checksum_ = calc_header_checksum();
  }
  return ret;
}

int ObBackupCommonHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupCommonHeader::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(compressor_type_));
  checksum = checksum ^ data_type_;
  checksum = checksum ^ header_length_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  format_i64(align_length_, checksum);
  return checksum;
}

int ObBackupCommonHeader::check_valid() const
{
  int ret = OB_SUCCESS;

  if (COMMON_HEADER_VERSION != header_version_ || ObCompressorType::INVALID_COMPRESSOR == compressor_type_ ||
      ((ObCompressorType::NONE_COMPRESSOR == compressor_type_) && (data_length_ != data_zlength_)) ||
      data_type_ >= ObBackupFileType::BACKUP_TYPE_MAX || header_length_ != sizeof(ObBackupCommonHeader) ||
      align_length_ < 0) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(ret), K(*this), "ObBackupCommonHeader size", sizeof(ObBackupCommonHeader));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

bool ObBackupCommonHeader::is_compresssed_data() const
{
  return (data_length_ != data_zlength_) || (compressor_type_ > ObCompressorType::NONE_COMPRESSOR);
}

int ObBackupCommonHeader::check_data_checksum(const char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(len));
  } else if ((0 == len) && (0 != data_length_ || 0 != data_zlength_ || 0 != data_checksum_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(len), K_(data_length), K_(data_zlength), K_(data_checksum));
  } else if (data_zlength_ != len) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(len), K_(data_zlength));
  } else {
    int64_t crc_check_sum = ob_crc64(buf, len);
    if (crc_check_sum != data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "checksum error", K(crc_check_sum), K_(data_checksum), K(ret));
    }
  }
  return ret;
}

/******************ObBackupMetaHeader*******************/
ObBackupMetaHeader::ObBackupMetaHeader()
{
  reset();
}

void ObBackupMetaHeader::reset()
{
  memset(this, 0, sizeof(ObBackupMetaHeader));
  header_version_ = META_HEADER_VERSION;
  meta_type_ = static_cast<uint8_t>(ObBackupMetaType::META_TYPE_MAX);
}

void ObBackupMetaHeader::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupMetaHeader::set_checksum(const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_length_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_length));
  } else {
    data_checksum_ = ob_crc64(buf, len);
    header_checksum_ = calc_header_checksum();
  }
  return ret;
}

int ObBackupMetaHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupMetaHeader::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(meta_type_));
  format_i32(data_length_, checksum);
  format_i64(data_checksum_, checksum);

  return checksum;
}

int ObBackupMetaHeader::check_data_checksum(const char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || data_length_ != len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_length));
  } else {
    int64_t checksum = ob_crc64(buf, len);
    if (OB_UNLIKELY(data_checksum_ != checksum)) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "check data checksum failed.", K(*this), K(checksum), K(ret));
    }
  }
  return ret;
}

int ObBackupMetaHeader::check_valid() const
{
  int ret = OB_SUCCESS;

  if (META_HEADER_VERSION != header_version_ || meta_type_ >= ObBackupFileType::BACKUP_TYPE_MAX) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

/******************ObBackupMetaIndex*******************/
OB_SERIALIZE_MEMBER(ObBackupMetaIndex, meta_type_, table_id_, partition_id_, offset_, data_length_, task_id_);
ObBackupMetaIndex::ObBackupMetaIndex()
{
  reset();
}

void ObBackupMetaIndex::reset()
{
  memset(this, 0, sizeof(ObBackupMetaIndex));
  meta_type_ = static_cast<uint8_t>(ObBackupMetaType::META_TYPE_MAX);
}

int ObBackupMetaIndex::check_valid() const
{
  int ret = OB_SUCCESS;

  if (meta_type_ >= ObBackupFileType::BACKUP_TYPE_MAX) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (partition_id_ < 0 || offset_ < 0 || data_length_ < 0 || task_id_ < 0) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid  index", K(ret), K(*this));
  }

  return ret;
}

/******************ObBackupMacroIndex*******************/
ObBackupMacroIndex::ObBackupMacroIndex()
{
  reset();
}

void ObBackupMacroIndex::reset()
{
  memset(this, 0, sizeof(ObBackupMacroIndex));
}

OB_SERIALIZE_MEMBER(ObBackupMacroIndex, table_id_, partition_id_, index_table_id_, sstable_macro_index_, data_version_,
    data_seq_, backup_set_id_, sub_task_id_, offset_, data_length_);

int ObBackupMacroIndex::check_valid() const
{
  int ret = OB_SUCCESS;

  if (!(table_id_ != OB_INVALID_ID && partition_id_ >= 0 && index_table_id_ != OB_INVALID_ID &&
          sstable_macro_index_ >= 0 && data_version_ >= 0 && data_seq_ >= 0 && backup_set_id_ >= 0 &&
          sub_task_id_ >= 0 && offset_ >= 0 && data_length_ >= 0)) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid index", K(*this), K(ret));
  }

  return ret;
}

/******************ObBackupSStableMacroIndex*******************/
ObBackupSStableMacroIndex::ObBackupSStableMacroIndex()
{
  reset();
}

void ObBackupSStableMacroIndex::reset()
{
  memset(this, 0, sizeof(ObBackupSStableMacroIndex));
  header_version_ = SSTABLE_MACRO_INDEX_VERSION;
  data_type_ = static_cast<uint8_t>(ObBackupFileType::BACKUP_SSTABLE_MACRO_INDEX);
  header_length_ = static_cast<uint16_t>(sizeof(ObBackupSStableMacroIndex));
}

void ObBackupSStableMacroIndex::set_header_checksum()
{
  header_checksum_ = calc_header_checksum();
}

int ObBackupSStableMacroIndex::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(header_checksum_ != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed.", K(*this), K(checksum), K(ret));
  }
  return ret;
}

int16_t ObBackupSStableMacroIndex::calc_header_checksum() const
{
  int16_t checksum = 0;

  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_version_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(data_type_));
  checksum = checksum ^ header_length_;
  checksum = checksum ^ reserved_;
  format_i64(table_id_, checksum);
  format_i64(partition_id_, checksum);
  format_i64(index_table_id_, checksum);
  format_i64(offset_, checksum);
  format_i64(count_, checksum);

  return checksum;
}

int ObBackupSStableMacroIndex::is_valid() const
{
  int ret = OB_SUCCESS;

  if (SSTABLE_MACRO_INDEX_VERSION != header_version_ || ObBackupFileType::BACKUP_SSTABLE_MACRO_INDEX != data_type_ ||
      header_length_ != sizeof(ObBackupSStableMacroIndex)) {
    ret = OB_INVALID_ERROR;
    COMMON_LOG(WARN, "invalid header", K(*this), K(ret));
  } else if (OB_FAIL(check_header_checksum())) {
    COMMON_LOG(WARN, "header checksum error", K(ret));
  }

  return ret;
}

ObMetaIndexKey::ObMetaIndexKey()
{
  reset();
}

ObMetaIndexKey::ObMetaIndexKey(const int64_t table_id, const int64_t partition_id, const uint8_t meta_type)
{
  table_id_ = table_id;
  partition_id_ = partition_id;
  meta_type_ = meta_type;
}

void ObMetaIndexKey::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  meta_type_ = ObBackupMetaType::META_TYPE_MAX;
}

bool ObMetaIndexKey::operator==(const ObMetaIndexKey& other) const
{
  return table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && meta_type_ == other.meta_type_;
}

bool ObMetaIndexKey::operator!=(const ObMetaIndexKey& other) const
{
  return !(*this == other);
}

uint64_t ObMetaIndexKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  hash_val = murmurhash(&meta_type_, sizeof(meta_type_), hash_val);
  return hash_val;
}

bool ObMetaIndexKey::is_valid() const
{
  return OB_INVALID_ID != table_id_ && partition_id_ >= 0 && meta_type_ < ObBackupMetaType::META_TYPE_MAX;
}

static const char* status_strs[] = {
    "INVALID",
    "STOP",
    "BEGINNING",
    "DOING",
    "STOPPING",
    "INTERRUPTED",
    "MIXED",
};

const char* ObLogArchiveStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= MAX) {
    str = "UNKNOWN";
  } else {
    str = status_strs[status];
  }
  return str;
}

ObLogArchiveStatus::STATUS ObLogArchiveStatus::get_status(const char* status_str)
{
  ObLogArchiveStatus::STATUS status = ObLogArchiveStatus::MAX;

  const int64_t count = ARRAYSIZEOF(status_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObLogArchiveStatus::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, status_strs[i])) {
      status = static_cast<ObLogArchiveStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

OB_SERIALIZE_MEMBER(ObGetTenantLogArchiveStatusArg, incarnation_, round_);

ObTenantLogArchiveStatus::ObTenantLogArchiveStatus()
    : tenant_id_(OB_INVALID_ID),
      start_ts_(0),
      checkpoint_ts_(0),
      incarnation_(0),
      round_(0),
      status_(ObLogArchiveStatus::MAX),
      is_mark_deleted_(false),
      is_mount_file_created_(false),
      compatible_(COMPATIBLE::NONE)
{}

void ObTenantLogArchiveStatus::reset()
{
  tenant_id_ = OB_INVALID_ID;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  incarnation_ = 0;
  round_ = 0;
  status_ = ObLogArchiveStatus::MAX;
  is_mark_deleted_ = false;
  is_mount_file_created_ = false;
  compatible_ = COMPATIBLE::NONE;
}

bool ObTenantLogArchiveStatus::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && ObLogArchiveStatus::is_valid(status_) && incarnation_ >= 0 && round_ >= 0 &&
         start_ts_ >= -1 && checkpoint_ts_ >= -1 && compatible_ >= COMPATIBLE::NONE;
}

bool ObTenantLogArchiveStatus::is_compatible_valid(COMPATIBLE compatible)
{
  return compatible >= COMPATIBLE::NONE && compatible < COMPATIBLE::MAX;
}

// STOP->STOP
// BEGINNING -> BEGINNING\DOING\STOPPING\INERRUPTED
// DOING -> DOING\STOPPING\INERRUPTED
// STOPPING -> STOPPIONG\STOP
// INTERRUPTED -> INERRUPTED\STOPPING
int ObTenantLogArchiveStatus::update(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not valid status", K(ret), K(*this));
  } else if (!new_status.is_valid() || new_status.incarnation_ != incarnation_ || new_status.round_ != round_ ||
             new_status.tenant_id_ != tenant_id_ || new_status.compatible_ != compatible_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "is_new_status_valid", new_status.is_valid(), K(new_status), K(*this));
  } else if (is_mount_file_created_ && !new_status.is_mount_file_created_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_mount_file_created_ cannot change", K(ret), K(*this), K(new_status));
  } else {
    is_mount_file_created_ = new_status.is_mount_file_created_;
    switch (status_) {
      case ObLogArchiveStatus::STOP: {
        if (OB_FAIL(update_stop_(new_status))) {
          LOG_WARN("failed to update stop", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::BEGINNING: {
        if (OB_FAIL(update_beginning_(new_status))) {
          LOG_WARN("failed to update beginning", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::DOING: {
        if (OB_FAIL(update_doing_(new_status))) {
          LOG_WARN("failed to update doing", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::STOPPING: {
        if (OB_FAIL(update_stopping_(new_status))) {
          LOG_WARN("failed to update stopping", K(ret));
        }
        break;
      }
      case ObLogArchiveStatus::INTERRUPTED: {
        if (OB_FAIL(update_interrupted_(new_status))) {
          LOG_WARN("failed to update interrupted", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid status", K(ret), K(*this), K(new_status));
      }
    }  // end switch
  }

  return ret;
}

int ObTenantLogArchiveStatus::update_stop_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::STOP != new_status.status_) {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stop status can only update with stop", K(ret), K(*this), K(new_status));
  } else {
    LOG_INFO("no need update stop status", K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_beginning_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::DOING == new_status.status_) {
    if (0 >= new_status.start_ts_ || 0 >= new_status.checkpoint_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_ERROR("[LOG_ARCHIVE] checkpoint_ts or start_ts must not less than 0", K(ret), K(*this), K(new_status));
    } else {
      LOG_INFO("[LOG_ARCHIVE] update beginning to doing", K(*this), K(new_status));
      status_ = new_status.status_;
      start_ts_ = new_status.start_ts_;
      checkpoint_ts_ = new_status.checkpoint_ts_;
    }
  } else if (ObLogArchiveStatus::BEGINNING == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_ ||
             ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    LOG_INFO("[LOG_ARCHIVE] update beginning to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_doing_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::DOING == new_status.status_) {
    if (start_ts_ < new_status.start_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_ERROR("new start_ts must not larger than the prev one", K(ret), K(*this), K(new_status));
    } else if (checkpoint_ts_ > new_status.checkpoint_ts_) {
      ret = OB_INVALID_LOG_ARCHIVE_STATUS;
      LOG_ERROR("new checkpoint_ts must not less than the prev one", K(ret), K(*this), K(new_status));
    } else {
      LOG_INFO("update doing stat", K(*this), K(new_status));
      checkpoint_ts_ = new_status.checkpoint_ts_;
    }
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_ ||
             ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update doing to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_stopping_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::STOPPING == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOP == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update stopping to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

int ObTenantLogArchiveStatus::update_interrupted_(const ObTenantLogArchiveStatus& new_status)
{
  int ret = OB_SUCCESS;

  if (ObLogArchiveStatus::INTERRUPTED == new_status.status_) {
    // do nothing
  } else if (ObLogArchiveStatus::STOPPING == new_status.status_ || ObLogArchiveStatus::STOP == new_status.status_) {
    FLOG_INFO("[LOG_ARCHIVE] update interrupted to new stat", K(*this), K(new_status));
    status_ = new_status.status_;
  } else {
    ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
    LOG_ERROR("stat not match", K(ret), K(*this), K(new_status));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTenantLogArchiveStatus, tenant_id_, start_ts_, checkpoint_ts_, incarnation_, round_, status_,
    is_mark_deleted_, is_mount_file_created_, compatible_);
ObTenantLogArchiveStatusWrapper::ObTenantLogArchiveStatusWrapper() : result_code_(OB_SUCCESS), status_array_()
{}

OB_SERIALIZE_MEMBER(ObTenantLogArchiveStatusWrapper, result_code_, status_array_);

ObLogArchiveBackupInfo::ObLogArchiveBackupInfo() : status_()
{
  backup_dest_[0] = '\0';
}

void ObLogArchiveBackupInfo::reset()
{
  status_.reset();
  backup_dest_[0] = '\0';
}

bool ObLogArchiveBackupInfo::is_valid() const
{
  return status_.is_valid();
}

bool ObLogArchiveBackupInfo::is_same(const ObLogArchiveBackupInfo& other) const
{
  return 0 == strncmp(backup_dest_, other.backup_dest_, sizeof(backup_dest_)) &&
         status_.tenant_id_ == other.status_.tenant_id_ && status_.start_ts_ == other.status_.start_ts_ &&
         status_.checkpoint_ts_ == other.status_.checkpoint_ts_ && status_.incarnation_ == other.status_.incarnation_ &&
         status_.round_ == other.status_.round_ && status_.status_ == other.status_.status_ &&
         status_.is_mark_deleted_ == other.status_.is_mark_deleted_;
}

ObBackupDest::ObBackupDest() : device_type_(OB_STORAGE_MAX_TYPE)
{
  root_path_[0] = '\0';
  storage_info_[0] = '\0';
}

bool ObBackupDest::is_valid() const
{
  return device_type_ >= 0 && device_type_ < OB_STORAGE_MAX_TYPE && strlen(root_path_) > 0;
}

void ObBackupDest::reset()
{
  device_type_ = OB_STORAGE_MAX_TYPE;
  root_path_[0] = '\0';
  storage_info_[0] = '\0';
}

int ObBackupDest::set(const char* backup_dest)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  common::ObStorageType type;
  ObString bakup_dest_str(backup_dest);

  if (device_type_ != OB_STORAGE_MAX_TYPE) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (OB_ISNULL(backup_dest)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(backup_dest));
  } else if (OB_FAIL(get_storage_type_from_path(bakup_dest_str, type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else {
    // oss://backup_dir/?host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222
    // file:///root_backup_dir"
    while (backup_dest[pos] != '\0') {
      if (backup_dest[pos] == '?') {
        break;
      }
      ++pos;
    }
    int64_t left_count = strlen(backup_dest) - pos;

    if (pos >= sizeof(root_path_) || left_count >= sizeof(storage_info_)) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_ERROR("backup dest is too long, cannot work", K(ret), K(pos), K(backup_dest), K(left_count));
    } else {
      MEMCPY(root_path_, backup_dest, pos);
      root_path_[pos] = '\0';
      ++pos;
      if (left_count > 0) {
        STRNCPY(storage_info_, backup_dest + pos, left_count);
        storage_info_[left_count] = '\0';
      }
      device_type_ = type;
      LOG_TRACE("succeed to set backup dest", K(ret), K(backup_dest), K(*this));
    }
  }

  return ret;
}

int ObBackupDest::set(const char* root_path, const char* storage_info)
{
  int ret = OB_SUCCESS;
  common::ObStorageType type;
  ObString root_path_str(root_path);

  if (device_type_ != OB_STORAGE_MAX_TYPE) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (OB_ISNULL(root_path) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(root_path), KP(storage_info));
  } else if (OB_FAIL(get_storage_type_from_path(root_path_str, type))) {
    LOG_WARN("failed to get storage type", K(ret));
  } else if (OB_FAIL(databuff_printf(root_path_, OB_MAX_BACKUP_PATH_LENGTH, "%s", root_path))) {
    LOG_WARN("failed to set root path", K(ret), K(root_path), K(strlen(root_path)));
  } else if (OB_FAIL(databuff_printf(storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", storage_info))) {
    LOG_WARN("failed to set storage info", K(ret), K(storage_info), K(strlen(storage_info)));
  } else {
    device_type_ = type;
  }
  return ret;
}

const char* ObBackupDest::get_type_str() const
{
  return get_storage_type_str(device_type_);
}

int ObBackupDest::get_backup_dest_str(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup dest is not init", K(ret), K(*this));
  } else if (OB_ISNULL(buf) || buf_size < share::OB_MAX_BACKUP_DEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup dest str get invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%s", root_path_))) {
    LOG_WARN("failed to copy backup dest", K(ret), K(root_path_), K(sizeof(root_path_)));
  } else if (ObStorageType::OB_STORAGE_OSS == device_type_) {
    const int64_t str_len = strlen(buf);
    if (OB_FAIL(databuff_printf(buf + str_len, buf_size - str_len, "?%s", storage_info_))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(storage_info_), K(str_len), K(sizeof(storage_info_)));
    }
  } else if (ObStorageType::OB_STORAGE_COS == device_type_) {
    const int64_t str_len = strlen(buf);
    if (OB_FAIL(databuff_printf(buf + str_len, buf_size - str_len, "?%s", storage_info_))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(storage_info_), K(str_len), K(sizeof(storage_info_)));
    }
  }
  return ret;
}

bool ObBackupDest::operator==(const ObBackupDest& backup_dest) const
{
  return device_type_ == backup_dest.device_type_ && (0 == STRCMP(root_path_, backup_dest.root_path_)) &&
         (0 == STRCMP(storage_info_, backup_dest.storage_info_));
}

uint64_t ObBackupDest::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&device_type_, sizeof(device_type_), hash_val);
  hash_val = murmurhash(root_path_, static_cast<int32_t>(strlen(root_path_)), hash_val);
  hash_val = murmurhash(storage_info_, static_cast<int32_t>(strlen(storage_info_)), hash_val);
  return hash_val;
}

ObBackupBaseDataPathInfo::ObBackupBaseDataPathInfo()
    : dest_(), tenant_id_(OB_INVALID_ID), full_backup_set_id_(0), inc_backup_set_id_(0)
{}

void ObBackupBaseDataPathInfo::reset()
{
  dest_.reset();
  tenant_id_ = OB_INVALID_ID;
  full_backup_set_id_ = 0;
  inc_backup_set_id_ = 0;
}

bool ObBackupBaseDataPathInfo::is_valid() const
{
  return dest_.is_valid() && OB_INVALID_ID != tenant_id_ && inc_backup_set_id_ >= full_backup_set_id_ &&
         full_backup_set_id_ > 0;
}

const char* ObBackupInfoStatus::get_status_str(const BackupStatus& status)
{
  const char* str = "UNKNOWN";
  const char* info_backup_status_strs[] = {
      "STOP",
      "PREPARE",
      "SCHEDULE",
      "DOING",
      "CANCEL",
      "CLEANUP",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(info_backup_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = info_backup_status_strs[status];
  }
  return str;
}

const char* ObBackupInfoStatus::get_info_backup_status_str() const
{
  return get_status_str(status_);
}

int ObBackupInfoStatus::set_info_backup_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set info backup status get invalid argument", K(ret), KP(buf));
  } else {
    const char* info_backup_status_strs[] = {
        "STOP",
        "PREPARE",
        "SCHEDULE",
        "DOING",
        "CANCEL",
        "CLEANUP",
    };
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(info_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(info_backup_status_strs); i++) {
      if (0 == STRCMP(info_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup info status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObBackupType::get_backup_type_str() const
{
  const char* str = "UNKNOWN";
  const char* backup_func_type_strs[] = {
      "",
      "D",
      "I",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_func_type_strs), "types count mismatch");
  if (type_ < 0 || type_ >= MAX) {
    LOG_ERROR("invalid backup type", K(type_));
  } else {
    str = backup_func_type_strs[type_];
  }
  return str;
}

int ObBackupType::set_backup_type(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup type get invalid argument", K(ret), KP(buf));
  } else {
    const char* backup_func_type_strs[] = {
        "",
        "D",
        "I",
    };
    BackupType tmp_type = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_func_type_strs), "types count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_func_type_strs); i++) {
      if (0 == STRCMP(backup_func_type_strs[i], buf)) {
        tmp_type = static_cast<BackupType>(i);
      }
    }

    if (MAX == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup type str not found", K(ret), K(buf));
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

const char* ObBackupDeviceType::get_backup_device_str() const
{
  const char* str = "UNKNOWN";
  const char* backup_device_type_strs[] = {
      "file://",
      "oss://",
      "ofs://",
      "cos://",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_device_type_strs), "types count mismatch");
  if (type_ < 0 || type_ >= MAX) {
    LOG_ERROR("invalid backup device type", K(type_));
  } else {
    str = backup_device_type_strs[type_];
  }
  return str;
}

int ObBackupDeviceType::set_backup_device_type(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup device get invalid argument", K(ret), KP(buf));
  } else {
    const char* backup_device_type_strs[] = {
        "file://",
        "oss://",
        "ofs://",
        "cos://",
    };
    BackupDeviceType tmp_type = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_device_type_strs), "types count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_device_type_strs); i++) {
      if (0 == STRCMP(backup_device_type_strs[i], buf)) {
        tmp_type = static_cast<BackupDeviceType>(i);
      }
    }

    if (MAX == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup device type str not found", K(ret), K(buf));
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

ObBaseBackupInfoStruct::ObBaseBackupInfoStruct()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      backup_dest_(),
      backup_snapshot_version_(0),
      backup_schema_version_(0),
      backup_data_version_(0),
      detected_backup_region_(),
      backup_type_(),
      backup_status_(),
      backup_task_id_(0),
      encryption_mode_(share::ObBackupEncryptionMode::MAX_MODE),
      passwd_()
{}

void ObBaseBackupInfoStruct::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  incarnation_ = 0;
  backup_dest_.reset();
  backup_snapshot_version_ = 0;
  backup_schema_version_ = 0;
  backup_data_version_ = 0;
  detected_backup_region_.reset();
  backup_type_.reset();
  backup_status_.reset();
  backup_task_id_ = 0;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
}

bool ObBaseBackupInfoStruct::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && tenant_id_ > 0 && backup_status_.is_valid() &&
         share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

bool ObBaseBackupInfoStruct::has_cleaned() const
{
  return is_valid() && backup_status_.is_stop_status() && 0 == backup_snapshot_version_ &&
         0 == backup_schema_version_ && 0 == backup_data_version_ && ObBackupType::EMPTY == backup_type_.type_;
}

ObBaseBackupInfoStruct& ObBaseBackupInfoStruct::operator=(const ObBaseBackupInfoStruct& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(info))) {
    LOG_ERROR("failed to assign backup info", K(ret), K(info));
  }
  return *this;
}

int ObBaseBackupInfoStruct::assign(const ObBaseBackupInfoStruct& backup_info_struct)
{
  int ret = OB_SUCCESS;
  if (!backup_info_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(backup_info_struct));
  } else {
    backup_dest_ = backup_info_struct.backup_dest_;
    detected_backup_region_ = backup_info_struct.detected_backup_region_;
    tenant_id_ = backup_info_struct.tenant_id_;
    backup_set_id_ = backup_info_struct.backup_set_id_;
    incarnation_ = backup_info_struct.incarnation_;
    backup_snapshot_version_ = backup_info_struct.backup_snapshot_version_;
    backup_schema_version_ = backup_info_struct.backup_schema_version_;
    backup_data_version_ = backup_info_struct.backup_data_version_;
    backup_type_ = backup_info_struct.backup_type_;
    backup_status_ = backup_info_struct.backup_status_;
    backup_task_id_ = backup_info_struct.backup_task_id_;
    encryption_mode_ = backup_info_struct.encryption_mode_;
    passwd_ = backup_info_struct.passwd_;
  }
  return ret;
}

int ObBaseBackupInfoStruct::check_backup_info_match(const ObBaseBackupInfoStruct& backup_info_struct) const
{
  int ret = OB_SUCCESS;
  if (!backup_info_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(backup_info_struct));
  } else if (backup_dest_ != backup_info_struct.backup_dest_ || tenant_id_ != backup_info_struct.tenant_id_ ||
             backup_set_id_ != backup_info_struct.backup_set_id_ || incarnation_ != backup_info_struct.incarnation_ ||
             backup_snapshot_version_ != backup_info_struct.backup_snapshot_version_ ||
             backup_schema_version_ != backup_info_struct.backup_schema_version_ ||
             backup_data_version_ != backup_info_struct.backup_data_version_ ||
             backup_type_.type_ != backup_info_struct.backup_type_.type_ ||
             backup_status_.status_ != backup_info_struct.backup_status_.status_ ||
             encryption_mode_ != backup_info_struct.encryption_mode_ || passwd_ != backup_info_struct.passwd_) {
    ret = OB_BACKUP_INFO_NOT_MATCH;
    LOG_WARN("backup info is not match", K(ret), K(*this), K(backup_info_struct));
  }
  return ret;
}

ObClusterBackupDest::ObClusterBackupDest() : dest_(), cluster_id_(0), incarnation_(0)
{
  cluster_name_[0] = '\0';
}

bool ObClusterBackupDest::is_valid() const
{
  return dest_.is_valid() && strlen(cluster_name_) > 0 && cluster_id_ > 0 && incarnation_ == OB_START_INCARNATION;
}

bool ObClusterBackupDest::is_same(const ObClusterBackupDest& other) const
{
  return dest_ == other.dest_ && 0 == STRCMP(cluster_name_, other.cluster_name_) && cluster_id_ == other.cluster_id_ &&
         incarnation_ == other.incarnation_;
}

void ObClusterBackupDest::reset()
{
  dest_.reset();
  cluster_id_ = 0;
  incarnation_ = 0;
  cluster_name_[0] = '\0';
}

int ObClusterBackupDest::set(const char* backup_dest, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  const char* cluster_name = GCONF.cluster;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(set(backup_dest, cluster_name, cluster_id, incarnation))) {
    LOG_WARN("failed to set", K(ret), K(backup_dest), K(cluster_name), K(cluster_id), K(incarnation));
  }
  return ret;
}

int ObClusterBackupDest::set(
    const char* backup_dest, const char* cluster_name, const int64_t cluster_id, const int64_t incarnation)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(backup_dest) || OB_ISNULL(cluster_name) || cluster_id <= 0 || incarnation != OB_START_INCARNATION) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), KP(backup_dest), KP(cluster_name), K(cluster_id), K(incarnation));
  } else if (strlen(cluster_name) >= sizeof(cluster_name_)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cluster is too long, cannot work", K(ret), K(cluster_name));
  } else if (OB_FAIL(dest_.set(backup_dest))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_dest));
  } else {
    const int64_t len = strlen(cluster_name);
    STRNCPY(cluster_name_, cluster_name, len);
    cluster_name_[len] = '\0';
    cluster_id_ = cluster_id;
    incarnation_ = incarnation;
    LOG_TRACE("succeed to set cluster backup dest", K(ret), K(*this), K(backup_dest));
  }

  return ret;
}

int ObClusterBackupDest::set(const ObBackupDest& backup_dest, const int64_t incarnation)
{
  int ret = OB_SUCCESS;
  const char* cluster_name = GCONF.cluster;
  if (incarnation <= 0 || !backup_dest.is_valid()) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid args", K(ret), K(backup_dest), K(incarnation));
  } else if (strlen(cluster_name) >= sizeof(cluster_name_)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cluster is too long, cannot work", K(ret), K(cluster_name));
  } else {
    const int64_t len = strlen(cluster_name);
    STRNCPY(cluster_name_, cluster_name, len);
    cluster_name_[len] = '\0';
    cluster_id_ = GCONF.cluster_id;
    incarnation_ = incarnation;
    dest_ = backup_dest;
    LOG_INFO("succeed to set cluster backup dest", K(ret), K(*this), K(dest_));
  }
  return ret;
}

uint64_t ObClusterBackupDest::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(cluster_name_, static_cast<int32_t>(strlen(cluster_name_)), hash_val);
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  hash_val = murmurhash(&incarnation_, sizeof(incarnation_), hash_val);
  hash_val += dest_.hash();
  return hash_val;
}

bool ObClusterBackupDest::operator==(const ObClusterBackupDest& other) const
{
  return is_same(other);
}

ObTenantBackupTaskItem::ObTenantBackupTaskItem()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      incarnation_(0),
      snapshot_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_backup_data_version_(0),
      pg_count_(0),
      macro_block_count_(0),
      finish_pg_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      start_time_(0),
      end_time_(0),
      compatible_(0),
      cluster_version_(0),
      backup_type_(),
      status_(BackupStatus::MAX),
      device_type_(ObStorageType::OB_STORAGE_MAX_TYPE),
      result_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID),
      backup_dest_(),
      backup_data_version_(0),
      backup_schema_version_(0),
      partition_count_(0),
      finish_partition_count_(0),
      encryption_mode_(ObBackupEncryptionMode::MAX_MODE),
      passwd_(),
      is_mark_deleted_(false)
{}

void ObTenantBackupTaskItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  incarnation_ = 0;
  snapshot_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_backup_data_version_ = 0;
  pg_count_ = 0;
  macro_block_count_ = 0;
  finish_pg_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  backup_type_.reset();
  status_ = BackupStatus::MAX;
  device_type_ = ObStorageType::OB_STORAGE_MAX_TYPE;
  result_ = 0;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  backup_dest_.reset();
  backup_data_version_ = 0;
  backup_schema_version_ = 0;
  partition_count_ = 0;
  finish_partition_count_ = 0;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
  is_mark_deleted_ = false;
}

bool ObTenantBackupTaskItem::is_key_valid() const
{
  return tenant_id_ != OB_INVALID_ID && backup_set_id_ > 0 && incarnation_ > 0;
}

bool ObTenantBackupTaskItem::is_valid() const
{
  return is_key_valid() && BackupStatus::MAX != status_ && share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

bool ObTenantBackupTaskItem::is_same_task(const ObTenantBackupTaskItem& other) const
{
  return tenant_id_ == other.tenant_id_ && backup_set_id_ == other.backup_set_id_ && incarnation_ == other.incarnation_;
}

bool ObTenantBackupTaskItem::is_result_succeed() const
{
  return OB_SUCCESS == result_;
}

const char* ObTenantBackupTaskItem::get_backup_task_status_str() const
{
  const char* str = "UNKNOWN";
  const char* tenant_task_backup_status_strs[] = {"GENERATE", "DOING", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_task_backup_status_strs), "status count mismatch");
  if (status_ < 0 || status_ >= MAX) {
    LOG_WARN("invalid backup task status", K(status_));
  } else {
    str = tenant_task_backup_status_strs[status_];
  }
  return str;
}

int ObTenantBackupTaskItem::set_backup_task_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup task status get invalid argument", K(ret), KP(buf));
  } else {
    const char* tenant_task_backup_status_strs[] = {"GENERATE", "DOING", "FINISH", "CANCEL"};
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_task_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_task_backup_status_strs); i++) {
      if (0 == STRCMP(tenant_task_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

ObPGBackupTaskItem::ObPGBackupTaskItem()
    : tenant_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      partition_id_(-1),
      incarnation_(0),
      backup_set_id_(0),
      backup_type_(),
      snapshot_version_(0),
      partition_count_(0),
      macro_block_count_(0),
      finish_partition_count_(0),
      finish_macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      start_time_(0),
      end_time_(0),
      retry_count_(0),
      role_(INVALID_ROLE),
      replica_type_(REPLICA_TYPE_MAX),
      server_(),
      status_(BackupStatus::MAX),
      result_(0),
      task_id_(0),
      trace_id_()
{}

void ObPGBackupTaskItem::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = -1;
  incarnation_ = 0;
  backup_set_id_ = 0;
  backup_type_.reset();
  snapshot_version_ = 0;
  partition_count_ = 0;
  macro_block_count_ = 0;
  finish_partition_count_ = 0;
  finish_macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  retry_count_ = 0;
  role_ = INVALID_ROLE;
  replica_type_ = REPLICA_TYPE_MAX;
  server_.reset();
  status_ = BackupStatus::MAX;
  result_ = 0;
  task_id_ = 0;
  trace_id_.reset();
}

bool ObPGBackupTaskItem::is_key_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != table_id_ && partition_id_ >= 0 && incarnation_ > 0 &&
         backup_set_id_ > 0;
}

bool ObPGBackupTaskItem::is_same_task(const ObPGBackupTaskItem& item) const
{
  return tenant_id_ == item.tenant_id_ && table_id_ == item.table_id_ && partition_id_ == item.partition_id_ &&
         incarnation_ == item.incarnation_ && backup_set_id_ == item.backup_set_id_;
}

bool ObPGBackupTaskItem::is_valid() const
{
  return is_key_valid() && BackupStatus::MAX != status_;
}

const char* ObPGBackupTaskItem::get_status_str(const BackupStatus& status)
{
  const char* str = "UNKNOWN";
  const char* pg_task_backup_status_strs[] = {"PENDING", "DOING", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_task_backup_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_WARN("invalid backup task status", K(status));
  } else {
    str = pg_task_backup_status_strs[status];
  }
  return str;
}

const char* ObPGBackupTaskItem::get_backup_task_status_str() const
{
  return get_status_str(status_);
}

int ObPGBackupTaskItem::set_backup_task_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup task status get invalid argument", K(ret), KP(buf));
  } else {
    const char* pg_task_backup_status_strs[] = {"PENDING", "DOING", "FINISH", "CANCEL"};
    BackupStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(pg_task_backup_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(pg_task_backup_status_strs); i++) {
      if (0 == STRCMP(pg_task_backup_status_strs[i], buf)) {
        tmp_status = static_cast<BackupStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", K(ret), K(buf));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

int ObPGBackupTaskItem::set_trace_id(const char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  uint64_t value[2];
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set trace id get invalid argument", K(ret), KP(buf), K(buf_size));
  } else if (0 == strlen(buf)) {
    // do nothing
  } else {
    int32_t return_value = sscanf(buf, "%lu_%lu", &value[0], &value[1]);
    if (0 != return_value && 2 != return_value) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sscanf get unexpected value", K(ret), K(return_value), K(buf));
    } else if (OB_FAIL(trace_id_.set(value))) {
      LOG_WARN("failed to set trace id", K(ret), K(value), K(buf));
    }
  }
  return ret;
}

int ObPGBackupTaskItem::get_trace_id(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_trace_id(trace_id_, buf_size, buf))) {
    LOG_WARN("failed to get trace id", K(ret), K(trace_id_), KP(buf), K(buf_size));
  }
  return ret;
}

int ObPGBackupTaskItem::get_trace_id(const ObTaskId& trace_id, const int64_t buf_size, char* buf)
{
  int ret = OB_SUCCESS;
  const uint64_t* value = NULL;
  if (OB_ISNULL(buf) || buf_size < common::OB_MAX_TRACE_ID_BUFFER_SIZE) {
    LOG_WARN("get trace id get invalid argument", K(ret), K(trace_id), K(buf_size));
  } else if (trace_id.is_invalid()) {
    // do noting
  } else if (FALSE_IT(value = trace_id.get())) {
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%lu_%lu", value[0], value[1]))) {
    LOG_WARN("failed to print trace id", K(ret), K(trace_id), K(buf_size));
  }
  return ret;
}

static const char* backup_validate_status_strs[] = {
    "SCHEDULE",
    "DOING",
    "FINISHED",
    "CANCEL",
};

ObBackupValidateTenant::ObBackupValidateTenant() : tenant_id_(OB_INVALID_ID), is_dropped_(false)
{}

ObBackupValidateTenant::~ObBackupValidateTenant()
{
  reset();
}

void ObBackupValidateTenant::reset()
{
  tenant_id_ = OB_INVALID_ID;
  is_dropped_ = false;
}

bool ObBackupValidateTenant::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

ObBackupValidateTaskItem::ObBackupValidateTaskItem()
    : job_id_(-1),
      tenant_id_(OB_INVALID_ID),
      tenant_name_(""),
      incarnation_(-1),
      backup_set_id_(-1),
      progress_percent_(0),
      status_(ObBackupValidateTaskItem::MAX)
{}

ObBackupValidateTaskItem::~ObBackupValidateTaskItem()
{
  reset();
}

void ObBackupValidateTaskItem::reset()
{
  job_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  tenant_name_ = "";
  incarnation_ = -1;
  backup_set_id_ = -1;
  progress_percent_ = 0;
  status_ = ObBackupValidateTaskItem::MAX;
}

bool ObBackupValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && OB_INVALID_ID != tenant_id_ && incarnation_ > 0 && backup_set_id_ >= 0 &&
         status_ < ValidateStatus::MAX;
}

bool ObBackupValidateTaskItem::is_same_task(const ObBackupValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_ && incarnation_ == other.incarnation_ &&
         backup_set_id_ == other.backup_set_id_;
}

const char* ObBackupValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = backup_validate_status_strs[status];
  }
  return str;
}

int ObBackupValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(backup_validate_status_strs); ++i) {
      if (0 == STRCMP(backup_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

static const char* tenant_validate_status_strs[] = {
    "SCHEDULE",
    "DOING",
    "FINISHED",
    "CANCEL",
};

ObTenantValidateTaskItem::ObTenantValidateTaskItem()
    : job_id_(-1),
      task_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      backup_set_id_(-1),
      status_(ObTenantValidateTaskItem::MAX),
      backup_dest_(""),
      start_time_(-1),
      end_time_(-1),
      total_pg_count_(-1),
      finish_pg_count_(0),
      total_partition_count_(-1),
      finish_partition_count_(0),
      total_macro_block_count_(-1),
      finish_macro_block_count_(0),
      log_size_(-1),
      result_(-1),
      comment_()
{}

ObTenantValidateTaskItem::~ObTenantValidateTaskItem()
{
  reset();
}

void ObTenantValidateTaskItem::reset()
{
  job_id_ = -1;
  task_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  incarnation_ = -1;
  backup_set_id_ = -1;
  status_ = ObTenantValidateTaskItem::MAX;
  backup_dest_[0] = '\0';
  start_time_ = -1;
  end_time_ = -1;
  total_pg_count_ = -1;
  finish_pg_count_ = -1;
  total_partition_count_ = -1;
  finish_partition_count_ = -1;
  total_macro_block_count_ = -1;
  finish_macro_block_count_ = -1;
  log_size_ = -1;
}

bool ObTenantValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && OB_INVALID_ID != tenant_id_ && incarnation_ > 0 && backup_set_id_ >= 0 &&
         status_ < ObTenantValidateTaskItem::MAX;
}

bool ObTenantValidateTaskItem::is_same_task(const ObTenantValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && task_id_ == other.task_id_ && tenant_id_ == other.tenant_id_ &&
         incarnation_ == other.incarnation_ && backup_set_id_ == other.backup_set_id_;
}

const char* ObTenantValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = tenant_validate_status_strs[status];
  }
  return str;
}

int ObTenantValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_validate_status_strs); ++i) {
      if (0 == STRCMP(tenant_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

static const char* pg_validate_status_strs[] = {
    "PENDING",
    "DOING",
    "FINISHED",
};

ObPGValidateTaskItem::ObPGValidateTaskItem()
    : job_id_(-1),
      task_id_(-1),
      tenant_id_(OB_INVALID_ID),
      table_id_(-1),
      partition_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      archive_round_(-1),
      total_partition_count_(-1),
      finish_partition_count_(-1),
      total_macro_block_count_(-1),
      finish_macro_block_count_(-1),
      log_info_(""),
      log_size_(-1),
      result_(-1),
      trace_id_(),
      status_(ObPGValidateTaskItem::MAX)
{}

ObPGValidateTaskItem::~ObPGValidateTaskItem()
{
  reset();
}

void ObPGValidateTaskItem::reset()
{
  job_id_ = -1;
  task_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  table_id_ = -1;
  partition_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  archive_round_ = -1;
  total_partition_count_ = -1;
  finish_partition_count_ = -1;
  total_macro_block_count_ = -1;
  finish_macro_block_count_ = -1;
  log_info_[0] = '\0';
  log_size_ = -1;
  result_ = -1;
  trace_id_.reset();
  status_ = ObPGValidateTaskItem::MAX;
}

bool ObPGValidateTaskItem::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && OB_INVALID_ID != tenant_id_ && backup_set_id_ >= 0 &&
         archive_round_ >= 0 /*&& status_ < ObPGValidateTaskItem::MAX*/;
}

bool ObPGValidateTaskItem::is_same_task(const ObPGValidateTaskItem& other)
{
  return job_id_ == other.job_id_ && task_id_ == other.task_id_ && tenant_id_ == other.tenant_id_ &&
         table_id_ == other.table_id_ && partition_id_ == other.partition_id_ && incarnation_ == other.incarnation_ &&
         backup_set_id_ == other.backup_set_id_ && archive_round_ == other.archive_round_;
}

const char* ObPGValidateTaskItem::get_status_str(const ValidateStatus& status)
{
  const char* str = "UNKNOWN";
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup info status", K(status));
  } else {
    str = pg_validate_status_strs[status];
  }
  return str;
}

int ObPGValidateTaskItem::set_status(const char* status_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(status_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get status get invalid argument", K(ret), K(status_str));
  } else {
    ValidateStatus tmp_status = MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(pg_validate_status_strs); ++i) {
      if (0 == STRCMP(pg_validate_status_strs[i], status_str)) {
        tmp_status = static_cast<ValidateStatus>(i);
      }
    }
    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("validate status str do not found", K(ret), K(status_str));
    } else {
      status_ = tmp_status;
    }
  }
  return ret;
}

ObPGValidateTaskRowKey::ObPGValidateTaskRowKey()
    : tenant_id_(0), job_id_(0), task_id_(0), incarnation_(0), backup_set_id_(0), table_id_(0), partition_id_(0)
{}

ObPGValidateTaskRowKey::~ObPGValidateTaskRowKey()
{}

int ObPGValidateTaskRowKey::set(const ObPGValidateTaskInfo* pg_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pg_task)) {
    // use the default value
  } else {
    tenant_id_ = pg_task->tenant_id_;
    job_id_ = pg_task->job_id_;
    task_id_ = pg_task->task_id_;
    incarnation_ = pg_task->incarnation_;
    backup_set_id_ = pg_task->backup_set_id_;
    table_id_ = pg_task->table_id_;
    partition_id_ = pg_task->partition_id_;
  }
  return ret;
}

int ObBackupUtils::get_backup_info_default_timeout_ctx(ObTimeoutCtx& ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 2 * 1000 * 1000;  // 2s
  int64_t abs_timeout_us = ctx.get_abs_timeout();
  int64_t worker_timeout_us = THIS_WORKER.get_timeout_ts();

  if (abs_timeout_us < 0) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  }

  if (INT64_MAX == worker_timeout_us) {
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  } else if (worker_timeout_us > 0 && worker_timeout_us < abs_timeout_us) {
    abs_timeout_us = worker_timeout_us;
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }
  return ret;
}

bool ObBackupUtils::is_need_retry_error(const int err)
{
  bool bret = true;
  switch (err) {
    case OB_NOT_INIT:
    case OB_INVALID_ARGUMENT:
    case OB_ERR_UNEXPECTED:
    case OB_ERR_SYS:
    case OB_INIT_TWICE:
    case OB_SRC_DO_NOT_ALLOWED_MIGRATE:
    case OB_CANCELED:
    case OB_BACKUP_DATA_VERSION_GAP_OVER_LIMIT:
    case OB_LOG_ARCHIVE_STAT_NOT_MATCH:
    case OB_NOT_SUPPORTED:
    case OB_TENANT_HAS_BEEN_DROPPED:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}

bool ObBackupUtils::is_extern_device_error(const int err)
{
  bool bret = false;
  switch (err) {
    case OB_IO_ERROR:
    case OB_OSS_ERROR:
      bret = true;
      break;
    default:
      break;
  }
  return bret;
}

int ObBackupUtils::retry_get_tenant_schema_guard(const uint64_t tenant_id,
    schema::ObMultiVersionSchemaService& schema_service, const int64_t tenant_schema_version,
    schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_mode =
      schema::ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_FALLBACK;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || tenant_schema_version < OB_INVALID_VERSION)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(tenant_schema_version));
  } else {
    int retry_times = 0;
    const int64_t sys_schema_version = OB_INVALID_VERSION;  // sys_schema_version use latest
    while (retry_times < MAX_RETRY_TIMES) {
      if (OB_FAIL(schema_service.get_tenant_schema_guard(
              tenant_id, schema_guard, tenant_schema_version, sys_schema_version, refresh_mode))) {
        STORAGE_LOG(WARN,
            "fail to get schema, sleep 1s and retry",
            K(ret),
            K(tenant_id),
            K(tenant_schema_version),
            K(sys_schema_version));
        usleep(RETRY_INTERVAL);
      } else {
        break;
      }
      ++retry_times;
    }
  }
  return ret;
}

ObPhysicalRestoreInfo::ObPhysicalRestoreInfo()
    : cluster_id_(0),
      incarnation_(0),
      tenant_id_(0),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      log_archive_round_(0),
      restore_snapshot_version_(OB_INVALID_TIMESTAMP),
      restore_start_ts_(0),
      compatible_(OB_BACKUP_COMPATIBLE_VERSION),
      cluster_version_(0)
{
  backup_dest_[0] = '\0';
  cluster_name_[0] = '\0';
}

bool ObPhysicalRestoreInfo::is_valid() const
{
  return strlen(backup_dest_) > 0 && strlen(cluster_name_) > 0 && cluster_id_ > 0 &&
         OB_START_INCARNATION == incarnation_ && tenant_id_ > 0 && full_backup_set_id_ > 0 && inc_backup_set_id_ > 0 &&
         log_archive_round_ > 0 && restore_snapshot_version_ > 0 && restore_start_ts_ > 0 && compatible_ > 0 &&
         cluster_version_ > 0;
}

int ObPhysicalRestoreInfo::assign(const ObPhysicalRestoreInfo& other)
{
  int ret = OB_SUCCESS;
  STRNCPY(backup_dest_, other.backup_dest_, share::OB_MAX_BACKUP_DEST_LENGTH);
  STRNCPY(cluster_name_, other.cluster_name_, common::OB_MAX_CLUSTER_NAME_LENGTH);
  cluster_id_ = other.cluster_id_;
  incarnation_ = other.incarnation_;
  tenant_id_ = other.tenant_id_;
  full_backup_set_id_ = other.full_backup_set_id_;
  inc_backup_set_id_ = other.inc_backup_set_id_;
  log_archive_round_ = other.log_archive_round_;
  restore_snapshot_version_ = other.restore_snapshot_version_;
  restore_start_ts_ = other.restore_start_ts_;
  compatible_ = other.compatible_;
  cluster_version_ = other.cluster_version_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalRestoreInfo, backup_dest_, cluster_name_, cluster_id_, incarnation_, tenant_id_,
    full_backup_set_id_, inc_backup_set_id_, log_archive_round_, restore_snapshot_version_, restore_start_ts_,
    compatible_, cluster_version_);

ObRestoreBackupInfo::ObRestoreBackupInfo()
    : compat_mode_(Worker::CompatMode::INVALID),
      snapshot_version_(0),
      schema_version_(0),
      frozen_data_version_(0),
      frozen_snapshot_version_(0),
      frozen_schema_version_(0),
      physical_restore_info_(),
      sys_pg_key_list_()
{
  locality_[0] = '\0';
  primary_zone_[0] = '\0';
}

bool ObRestoreBackupInfo::is_valid() const
{
  return Worker::CompatMode::INVALID != compat_mode_ && snapshot_version_ > 0 && schema_version_ > 0 &&
         frozen_data_version_ > 0 && frozen_snapshot_version_ > 0 && frozen_schema_version_ > 0 &&
         physical_restore_info_.is_valid();
}

ObPhysicalRestoreArg::ObPhysicalRestoreArg() : restore_info_(), pg_key_(), restore_data_version_(0)
{}

ObPhysicalRestoreArg::ObPhysicalRestoreArg(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_ERROR("failed to assign arg", K(ret), K(other));
  }
}

ObPhysicalRestoreArg& ObPhysicalRestoreArg::operator=(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_ERROR("failed to assign arg", K(ret), K(other));
  }
  return *this;
}

bool ObPhysicalRestoreArg::is_valid() const
{
  return restore_info_.is_valid() && pg_key_.is_valid() && restore_data_version_ > 0;
}

int ObPhysicalRestoreArg::assign(const ObPhysicalRestoreArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_.assign(other.restore_info_))) {
    LOG_WARN("fail to assign restore info", K(ret), K(other));
  } else {
    pg_key_ = other.pg_key_;
    restore_data_version_ = other.restore_data_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalRestoreArg, restore_info_, pg_key_, restore_data_version_);

ObPhysicalValidateArg::ObPhysicalValidateArg()
    : backup_dest_(""),
      cluster_name_(""),
      uri_header_(""),
      storage_info_(""),
      job_id_(-1),
      task_id_(-1),
      trace_id_(),
      server_(),
      cluster_id_(-1),
      pg_key_(),
      table_id_(-1),
      partition_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      archive_round_(-1),
      backup_set_id_(-1),
      total_partition_count_(-1),
      total_macro_block_count_(-1),
      clog_end_timestamp_(-1),
      start_log_id_(-1),
      end_log_id_(-1),
      log_size_(-1),
      is_dropped_tenant_(false),
      need_validate_clog_(true),
      full_backup_set_id_(-1),
      inc_backup_set_id_(-1),
      cluster_version_(0)
{}

ObPhysicalValidateArg& ObPhysicalValidateArg::operator=(const ObPhysicalValidateArg& other)
{
  if (this != &other) {
    STRNCPY(backup_dest_, other.backup_dest_, OB_MAX_BACKUP_DEST_LENGTH);
    STRNCPY(cluster_name_, other.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH);
    STRNCPY(uri_header_, other.uri_header_, OB_MAX_URI_HEADER_LENGTH);
    STRNCPY(storage_info_, other.storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    job_id_ = other.job_id_;
    task_id_ = other.task_id_;
    trace_id_.set(other.trace_id_);
    // server_
    cluster_id_ = other.cluster_id_;
    pg_key_ = other.pg_key_;
    table_id_ = other.table_id_;
    partition_id_ = other.partition_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_ = other.incarnation_;
    archive_round_ = other.archive_round_;
    backup_set_id_ = other.backup_set_id_;
    total_partition_count_ = other.total_partition_count_;
    total_macro_block_count_ = other.total_macro_block_count_;
    clog_end_timestamp_ = other.clog_end_timestamp_;
    start_log_id_ = other.start_log_id_;
    end_log_id_ = other.end_log_id_;
    log_size_ = other.log_size_;
    is_dropped_tenant_ = other.is_dropped_tenant_;
    need_validate_clog_ = other.need_validate_clog_;
    full_backup_set_id_ = other.full_backup_set_id_;
    inc_backup_set_id_ = other.inc_backup_set_id_;
    cluster_version_ = other.cluster_version_;
  }
  return *this;
}

int ObPhysicalValidateArg::assign(const ObPhysicalValidateArg& other)
{
  int ret = OB_SUCCESS;
  *this = other;
  return ret;
}

bool ObPhysicalValidateArg::is_valid() const
{
  return job_id_ >= 0 && task_id_ >= 0 && cluster_id_ >= 0 && table_id_ >= 0 && OB_INVALID_ID != tenant_id_ &&
         incarnation_ >= 0 && archive_round_ >= 0 && backup_set_id_ >= 0 && total_partition_count_ >= 0 &&
         total_macro_block_count_ >= 0 && clog_end_timestamp_ >= 0 && start_log_id_ >= 0 && end_log_id_ >= 0 &&
         log_size_ >= 0 && cluster_version_ > 0;
}

int ObPhysicalValidateArg::get_validate_pgkey(common::ObPartitionKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_key.init(pg_key_.get_table_id(), pg_key_.get_partition_id(), 0))) {
    STORAGE_LOG(WARN, "init pg key failed", K(ret), K(pg_key_));
  }
  return ret;
}

int ObPhysicalValidateArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalValidateArg", K(ret), K(*this));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest_, cluster_name_, cluster_id_, incarnation_))) {
    STORAGE_LOG(WARN, "failed to set backup dest", K(ret), K(*this));
  } else {
    // TODO
    path_info.tenant_id_ = tenant_id_;
    path_info.full_backup_set_id_ = 1;
    path_info.inc_backup_set_id_ = 1;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalValidateArg, backup_dest_, cluster_name_, uri_header_, storage_info_, job_id_, task_id_,
    trace_id_, server_, cluster_id_, pg_key_, table_id_, partition_id_, tenant_id_, incarnation_, archive_round_,
    backup_set_id_, total_partition_count_, total_macro_block_count_, clog_end_timestamp_, start_log_id_, end_log_id_,
    log_size_, is_dropped_tenant_, need_validate_clog_, full_backup_set_id_, inc_backup_set_id_, cluster_version_);

ObPhysicalBackupArg::ObPhysicalBackupArg()
    : uri_header_(),
      storage_info_(),
      incarnation_(0),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      backup_data_version_(0),
      backup_schema_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_data_version_(0),
      task_id_(0),
      backup_type_(ObBackupType::MAX),
      backup_snapshot_version_(0)
{}

void ObPhysicalBackupArg::reset()
{
  MEMSET(uri_header_, 0, sizeof(uri_header_));
  MEMSET(storage_info_, 0, sizeof(storage_info_));
  incarnation_ = 0;
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  backup_data_version_ = 0;
  backup_schema_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_data_version_ = 0;
  task_id_ = 0;
  backup_type_ = ObBackupType::MAX;
  backup_snapshot_version_ = 0;
}

bool ObPhysicalBackupArg::is_valid() const
{
  bool ret = true;
  if (STRLEN(uri_header_) == 0 || incarnation_ < 0 || tenant_id_ == OB_INVALID_ID || backup_set_id_ <= 0 ||
      backup_data_version_ <= 0 || backup_schema_version_ <= 0 || prev_full_backup_set_id_ < 0 ||
      prev_inc_backup_set_id_ < 0 || task_id_ < 0 ||
      (backup_type_ >= ObBackupType::MAX && backup_type_ < ObBackupType::FULL_BACKUP) ||
      backup_snapshot_version_ <= 0) {
    ret = false;
  }
  return ret;
}

bool ObPhysicalBackupArg::is_incremental_backup() const
{
  return ObBackupType::INCREMENTAL_BACKUP == backup_type_;
}

int ObPhysicalBackupArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(uri_header_, storage_info_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(*this));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation_))) {
    LOG_WARN("failed to set path info dest", K(ret), K(*this));
  } else {
    path_info.tenant_id_ = tenant_id_;
    if (ObBackupType::FULL_BACKUP == backup_type_) {
      path_info.full_backup_set_id_ = backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    } else if (ObBackupType::INCREMENTAL_BACKUP == backup_type_) {
      path_info.full_backup_set_id_ = prev_full_backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected ObBackupType", K(ret), K(*this));
    }
  }
  return ret;
}

int ObPhysicalBackupArg::get_prev_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_UNLIKELY(ObBackupType::INCREMENTAL_BACKUP != backup_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "only incremental backup has prev backup data", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(uri_header_, storage_info_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(*this));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation_))) {
    LOG_WARN("failed to set path info dest", K(ret), K(*this));
  } else {
    path_info.tenant_id_ = tenant_id_;
    path_info.full_backup_set_id_ = prev_full_backup_set_id_;
    path_info.inc_backup_set_id_ = prev_inc_backup_set_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPhysicalBackupArg, storage_info_, uri_header_, incarnation_, tenant_id_, backup_set_id_,
    backup_data_version_, backup_schema_version_, prev_full_backup_set_id_, prev_inc_backup_set_id_, prev_data_version_,
    task_id_, backup_type_, backup_snapshot_version_);

ObBackupBackupsetArg::ObBackupBackupsetArg()
    : src_uri_header_(),
      src_storage_info_(),
      dst_uri_header_(),
      dst_storage_info_(),
      cluster_name_(),
      cluster_id_(-1),
      job_id_(-1),
      tenant_id_(OB_INVALID_ID),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      backup_data_version_(-1),
      backup_schema_version_(-1),
      prev_full_backup_set_id_(-1),
      prev_inc_backup_set_id_(-1),
      prev_data_version_(-1),
      pg_key_(),
      server_(),
      backup_type_(ObBackupType::MAX),
      delete_input_(false),
      cluster_version_(0)
{}

void ObBackupBackupsetArg::reset()
{
  MEMSET(src_uri_header_, 0, sizeof(src_uri_header_));
  MEMSET(src_storage_info_, 0, sizeof(src_storage_info_));
  MEMSET(dst_uri_header_, 0, sizeof(dst_uri_header_));
  MEMSET(dst_storage_info_, 0, sizeof(dst_storage_info_));
  MEMSET(cluster_name_, 0, sizeof(cluster_name_));
  job_id_ = -1;
  tenant_id_ = OB_INVALID_ID;
  cluster_id_ = -1;
  incarnation_ = -1;
  backup_set_id_ = -1;
  copy_id_ = -1;
  prev_full_backup_set_id_ = -1;
  prev_inc_backup_set_id_ = -1;
  prev_data_version_ = -1;
  pg_key_.reset();
  server_.reset();
  backup_type_ = ObBackupType::MAX;
  delete_input_ = false;
  cluster_version_ = 0;
}

bool ObBackupBackupsetArg::is_valid() const
{
  return job_id_ > 0 && OB_INVALID_ID != tenant_id_ && cluster_id_ > 0 && incarnation_ > 0 && backup_set_id_ > 0 &&
         copy_id_ > 0 && pg_key_.is_valid() && server_.is_valid() && backup_type_ >= ObBackupType::FULL_BACKUP &&
         backup_type_ < ObBackupType::MAX && cluster_version_ > 0;
}

int ObBackupBackupsetArg::get_src_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObBackupBackupsetArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(src_uri_header_, src_storage_info_))) {
    STORAGE_LOG(WARN, "failed to set src backup dest", KR(ret), K(src_uri_header_), K(src_storage_info_));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation_))) {
    STORAGE_LOG(WARN, "failed to set src backup dest", K(ret), K(*this));
  } else {
    path_info.tenant_id_ = pg_key_.get_tenant_id();
    if (0 == prev_full_backup_set_id_ && 0 == prev_inc_backup_set_id_) {
      path_info.full_backup_set_id_ = backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    } else {
      path_info.full_backup_set_id_ = prev_full_backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    }
  }
  return ret;
}

int ObBackupBackupsetArg::get_dst_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObBackupBackupsetArg", K(ret), K(*this));
  } else if (OB_FAIL(backup_dest.set(dst_uri_header_, dst_storage_info_))) {
    STORAGE_LOG(WARN, "failed to set src backup dest", KR(ret), K(dst_uri_header_), K(dst_storage_info_));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation_))) {
    LOG_WARN("failed to set dst backup dest", KR(ret), K(*this));
  } else {
    path_info.tenant_id_ = pg_key_.get_tenant_id();
    if (0 == prev_full_backup_set_id_ && 0 == prev_inc_backup_set_id_) {
      path_info.full_backup_set_id_ = backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    } else {
      path_info.full_backup_set_id_ = prev_full_backup_set_id_;
      path_info.inc_backup_set_id_ = backup_set_id_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupBackupsetArg, src_uri_header_, src_storage_info_, dst_uri_header_, dst_storage_info_,
    cluster_name_, cluster_id_, job_id_, tenant_id_, incarnation_, backup_set_id_, copy_id_, backup_data_version_,
    backup_schema_version_, prev_full_backup_set_id_, prev_inc_backup_set_id_, pg_key_, server_, backup_type_,
    delete_input_, cluster_version_);

int ObPhysicalRestoreArg::trans_schema_id(
    const uint64_t schema_id, const uint64_t trans_tenant_id_, uint64_t& trans_schema_id) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_id(schema_id) || !is_valid_tenant_id(trans_tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid id", K(ret), K(schema_id), K(trans_tenant_id_));
  } else {
    trans_schema_id = combine_id(trans_tenant_id_, schema_id);
  }
  return ret;
}

int ObPhysicalRestoreArg::get_backup_pgkey(common::ObPartitionKey& pg_key) const
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  if (OB_FAIL(trans_schema_id(pg_key_.get_tablegroup_id(), restore_info_.tenant_id_, table_id))) {
    pg_key.reset();
    STORAGE_LOG(WARN, "get backup pgkey fail", K(ret), K(pg_key_), K(restore_info_));
  } else if (OB_FAIL(pg_key.init(table_id, pg_key_.get_partition_id(), pg_key_.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(pg_key_));
  }
  return ret;
}

int ObPhysicalRestoreArg::change_dst_pkey_to_src_pkey(
    const common::ObPartitionKey& dst_pkey, common::ObPartitionKey& src_pkey) const
{
  int ret = OB_SUCCESS;
  uint64_t src_table_id = 0;
  if (!dst_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "dst pkey is invalid", K(ret), K(dst_pkey));
  } else if (OB_FAIL(trans_schema_id(dst_pkey.get_table_id(), restore_info_.tenant_id_, src_table_id))) {
    STORAGE_LOG(WARN, "failed to change dst table id to src table id", K(dst_pkey), K(restore_info_));
  } else if (OB_FAIL(src_pkey.init(src_table_id, dst_pkey.get_partition_id(), dst_pkey.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(dst_pkey));
  }
  return ret;
}

int ObPhysicalRestoreArg::change_src_pkey_to_dst_pkey(
    const common::ObPartitionKey& src_pkey, common::ObPartitionKey& dst_pkey) const
{
  int ret = OB_SUCCESS;
  uint64_t dst_table_id = 0;
  dst_pkey.reset();
  if (!src_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "src pkey is invalid", K(ret), K(src_pkey));
  } else if (OB_FAIL(trans_schema_id(src_pkey.get_table_id(), pg_key_.get_tenant_id(), dst_table_id))) {
    STORAGE_LOG(WARN, "failed to change src table id to dst table id", K(src_pkey), K(restore_info_));
  } else if (OB_FAIL(dst_pkey.init(dst_table_id, src_pkey.get_partition_id(), src_pkey.get_partition_cnt()))) {
    STORAGE_LOG(WARN, "init pgkey fail", K(ret), K(src_pkey));
  }
  return ret;
}

int ObPhysicalRestoreArg::trans_to_backup_schema_id(const uint64_t schema_id, uint64_t& backup_schema_id) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(trans_schema_id(schema_id, restore_info_.tenant_id_, backup_schema_id))) {
    STORAGE_LOG(WARN, "tran schema is fail", K(ret), K(*this), K(schema_id), K(backup_schema_id));
  }
  return ret;
}

int ObPhysicalRestoreArg::trans_from_backup_schema_id(const uint64_t backup_schema_id, uint64_t& schema_id) const
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(trans_schema_id(backup_schema_id, pg_key_.get_tenant_id(), schema_id))) {
    STORAGE_LOG(WARN, "tran schema is fail", K(ret), K(*this), K(backup_schema_id), K(schema_id));
  }
  return ret;
}

int ObPhysicalRestoreArg::get_backup_base_data_info(share::ObBackupBaseDataPathInfo& path_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObPhysicalBackupArg", K(ret), K(*this));
  } else if (OB_FAIL(path_info.dest_.set(restore_info_.backup_dest_,
                 restore_info_.cluster_name_,
                 restore_info_.cluster_id_,
                 restore_info_.incarnation_))) {
    STORAGE_LOG(WARN, "fail to set backup dest", K(ret), K(*this));
  } else {
    path_info.tenant_id_ = restore_info_.tenant_id_;
    path_info.full_backup_set_id_ = restore_info_.full_backup_set_id_;
    path_info.inc_backup_set_id_ = restore_info_.inc_backup_set_id_;
  }
  return ret;
}

ObExternBackupInfo::ObExternBackupInfo()
    : full_backup_set_id_(0),
      inc_backup_set_id_(0),
      backup_data_version_(0),
      backup_snapshot_version_(0),
      backup_schema_version_(0),
      frozen_snapshot_version_(0),
      frozen_schema_version_(0),
      prev_full_backup_set_id_(0),
      prev_inc_backup_set_id_(0),
      prev_backup_data_version_(0),
      compatible_(0),
      cluster_version_(0),
      backup_type_(ObBackupType::MAX),
      status_(ObExternBackupInfo::DOING),
      encryption_mode_(share::ObBackupEncryptionMode::MAX_MODE),
      passwd_(),
      is_mark_deleted_(false)
{}

void ObExternBackupInfo::reset()
{
  full_backup_set_id_ = 0;
  inc_backup_set_id_ = 0;
  backup_data_version_ = 0;
  backup_snapshot_version_ = 0;
  backup_schema_version_ = 0;
  frozen_snapshot_version_ = 0;
  frozen_schema_version_ = 0;
  prev_full_backup_set_id_ = 0;
  prev_inc_backup_set_id_ = 0;
  prev_backup_data_version_ = 0;
  compatible_ = 0;
  cluster_version_ = 0;
  backup_type_ = ObBackupType::MAX;
  status_ = ObExternBackupInfo::DOING;
  encryption_mode_ = share::ObBackupEncryptionMode::MAX_MODE;
  passwd_.reset();
  is_mark_deleted_ = false;
}

bool ObExternBackupInfo::is_valid() const
{
  return full_backup_set_id_ > 0 && inc_backup_set_id_ > 0 && backup_data_version_ > 0 &&
         backup_snapshot_version_ > 0 && backup_schema_version_ > 0 && frozen_snapshot_version_ > 0 &&
         frozen_schema_version_ >= 0 && backup_type_ >= ObBackupType::FULL_BACKUP && backup_type_ < ObBackupType::MAX &&
         status_ >= SUCCESS && status_ <= FAILED && share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

bool ObExternBackupInfo::is_equal_without_status(const ObExternBackupInfo& extern_backup_info) const
{
  return full_backup_set_id_ == extern_backup_info.full_backup_set_id_ &&
         inc_backup_set_id_ == extern_backup_info.inc_backup_set_id_ &&
         backup_data_version_ == extern_backup_info.backup_data_version_ &&
         backup_snapshot_version_ == extern_backup_info.backup_snapshot_version_ &&
         backup_schema_version_ == extern_backup_info.backup_schema_version_ &&
         frozen_snapshot_version_ == extern_backup_info.frozen_snapshot_version_ &&
         frozen_schema_version_ == extern_backup_info.frozen_schema_version_ &&
         prev_full_backup_set_id_ == extern_backup_info.prev_full_backup_set_id_ &&
         prev_inc_backup_set_id_ == extern_backup_info.prev_inc_backup_set_id_ &&
         prev_backup_data_version_ == extern_backup_info.prev_backup_data_version_ &&
         compatible_ == extern_backup_info.compatible_ && cluster_version_ == extern_backup_info.cluster_version_ &&
         backup_type_ == extern_backup_info.backup_type_ && encryption_mode_ == extern_backup_info.encryption_mode_ &&
         passwd_ == extern_backup_info.passwd_;
}

bool ObExternBackupInfo::is_equal(const ObExternBackupInfo& extern_backup_info) const
{
  return is_equal_without_status(extern_backup_info) && status_ == extern_backup_info.status_ &&
         is_mark_deleted_ == extern_backup_info.is_mark_deleted_;
}

OB_SERIALIZE_MEMBER(ObExternBackupInfo, full_backup_set_id_, inc_backup_set_id_, backup_data_version_,
    backup_snapshot_version_, backup_schema_version_, frozen_snapshot_version_, frozen_schema_version_,
    prev_full_backup_set_id_, prev_inc_backup_set_id_, prev_backup_data_version_, compatible_, cluster_version_,
    backup_type_, status_, encryption_mode_, passwd_, is_mark_deleted_);

ObExternBackupSetInfo::ObExternBackupSetInfo()
    : backup_set_id_(0),
      backup_snapshot_version_(0),
      compatible_(OB_BACKUP_COMPATIBLE_VERSION),
      pg_count_(0),
      macro_block_count_(0),
      input_bytes_(0),
      output_bytes_(0),
      cluster_version_(0),
      compress_type_(ObCompressorType::INVALID_COMPRESSOR)
{}

void ObExternBackupSetInfo::reset()
{
  backup_set_id_ = 0;
  backup_snapshot_version_ = 0;
  compatible_ = 0;
  pg_count_ = 0;
  macro_block_count_ = 0;
  input_bytes_ = 0;
  output_bytes_ = 0;
  cluster_version_ = 0;
  compress_type_ = ObCompressorType::INVALID_COMPRESSOR;
}

bool ObExternBackupSetInfo::is_valid() const
{
  return backup_set_id_ >= 0 && backup_snapshot_version_ >= 0 && pg_count_ >= 0 && macro_block_count_ >= 0 &&
         input_bytes_ >= 0 && output_bytes_ >= 0 && cluster_version_ > 0 &&
         compress_type_ > ObCompressorType::INVALID_COMPRESSOR;
}

OB_SERIALIZE_MEMBER(ObExternBackupSetInfo, backup_set_id_, backup_snapshot_version_, compatible_, pg_count_,
    macro_block_count_, input_bytes_, output_bytes_, cluster_version_, compress_type_);

ObExternTenantInfo::ObExternTenantInfo()
    : tenant_id_(OB_INVALID_ID),
      create_timestamp_(0),
      delete_timestamp_(0),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      tenant_name_(),
      backup_snapshot_version_(0)
{}

void ObExternTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  create_timestamp_ = 0;
  delete_timestamp_ = 0;
  tenant_name_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  backup_snapshot_version_ = 0;
}

bool ObExternTenantInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && create_timestamp_ >= 0 && !tenant_name_.is_empty() &&
         compat_mode_ != lib::Worker::CompatMode::INVALID && backup_snapshot_version_ > 0;
}

OB_SERIALIZE_MEMBER(ObExternTenantInfo, tenant_id_, create_timestamp_, delete_timestamp_, compat_mode_, tenant_name_,
    backup_snapshot_version_);

ObExternTenantLocalityInfo::ObExternTenantLocalityInfo()
    : tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      backup_snapshot_version_(),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      tenant_name_(),
      locality_(),
      primary_zone_()
{}

void ObExternTenantLocalityInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  backup_set_id_ = 0;
  backup_snapshot_version_ = 0;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  tenant_name_.reset();
  locality_.reset();
  primary_zone_.reset();
}

bool ObExternTenantLocalityInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && backup_set_id_ > 0 && backup_snapshot_version_ > 0 &&
         compat_mode_ != lib::Worker::CompatMode::INVALID && !tenant_name_.is_empty() && !locality_.is_empty() &&
         !primary_zone_.is_empty();
}

bool ObExternTenantLocalityInfo::is_equal(const ObExternTenantLocalityInfo& tenant_locality_info) const
{
  return tenant_id_ == tenant_locality_info.tenant_id_ && backup_set_id_ == tenant_locality_info.backup_set_id_ &&
         backup_snapshot_version_ == tenant_locality_info.backup_snapshot_version_ &&
         compat_mode_ == tenant_locality_info.compat_mode_ && tenant_name_ == tenant_locality_info.tenant_name_ &&
         locality_ == tenant_locality_info.locality_ && primary_zone_ == tenant_locality_info.primary_zone_;
}

OB_SERIALIZE_MEMBER(ObExternTenantLocalityInfo, tenant_id_, backup_set_id_, backup_snapshot_version_, compat_mode_,
    tenant_name_, locality_, primary_zone_);

ObExternBackupDiagnoseInfo::ObExternBackupDiagnoseInfo()
    : tenant_id_(OB_INVALID_ID), tenant_locality_info_(), backup_set_info_(), extern_backup_info_()
{}

void ObExternBackupDiagnoseInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  tenant_locality_info_.reset();
  backup_set_info_.reset();
  extern_backup_info_.reset();
}

static const char* backup_clean_status_strs[] = {
    "STOP",
    "PREPARE",
    "DOING",
    "CANCEL",
};

const char* ObBackupCleanInfoStatus::get_str(const STATUS& status)
{
  const char* str = nullptr;

  if (status < 0 || status >= MAX) {
    str = "UNKOWN";
  } else {
    str = backup_clean_status_strs[status];
  }
  return str;
}

ObBackupCleanInfoStatus::STATUS ObBackupCleanInfoStatus::get_status(const char* status_str)
{
  ObBackupCleanInfoStatus::STATUS status = ObBackupCleanInfoStatus::MAX;

  const int64_t count = ARRAYSIZEOF(backup_clean_status_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupCleanInfoStatus::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, backup_clean_status_strs[i])) {
      status = static_cast<ObBackupCleanInfoStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

static const char* backup_clean_type_str[] = {
    "",
    "DELETE OBSOLETE BACKUP",
    "DELETE BACKUP SET",
};

const char* ObBackupCleanType::get_str(const TYPE& type)
{
  const char* str = nullptr;

  if (type < 0 || type >= MAX) {
    str = "UNKOWN";
  } else {
    str = backup_clean_type_str[type];
  }
  return str;
}

ObBackupCleanType::TYPE ObBackupCleanType::get_type(const char* type_str)
{
  ObBackupCleanType::TYPE type = ObBackupCleanType::MAX;

  const int64_t count = ARRAYSIZEOF(backup_clean_type_str);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupCleanType::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, backup_clean_type_str[i])) {
      type = static_cast<ObBackupCleanType::TYPE>(i);
      break;
    }
  }
  return type;
}

ObBackupCleanInfo::ObBackupCleanInfo()
    : tenant_id_(OB_INVALID_ID),
      job_id_(0),
      start_time_(0),
      end_time_(0),
      incarnation_(0),
      type_(ObBackupCleanType::MAX),
      status_(ObBackupCleanInfoStatus::MAX),
      expired_time_(0),
      backup_set_id_(0),
      error_msg_(),
      comment_(),
      clog_gc_snapshot_(0),
      result_(OB_SUCCESS)
{}

void ObBackupCleanInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  job_id_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  incarnation_ = 0;
  type_ = ObBackupCleanType::MAX;
  status_ = ObBackupCleanInfoStatus::MAX;
  expired_time_ = 0;
  backup_set_id_ = 0;
  error_msg_.reset();
  comment_.reset();
  clog_gc_snapshot_ = 0;
  result_ = OB_SUCCESS;
}

bool ObBackupCleanInfo::is_valid() const
{
  bool is_valid = true;
  is_valid = tenant_id_ != OB_INVALID_ID && ObBackupCleanInfoStatus::is_valid(status_);
  if (is_valid) {
    if (ObBackupCleanInfoStatus::STOP != status_) {
      is_valid = ObBackupCleanType::is_valid(type_) && job_id_ >= 0 && incarnation_ > 0 &&
                 ((is_expired_clean() && expired_time_ > 0) || (is_backup_set_clean() && backup_set_id_ > 0));
    }
  }
  return is_valid;
}

int ObBackupCleanInfo::get_clean_parameter(int64_t& parameter) const
{
  int ret = OB_SUCCESS;
  parameter = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup clean info is invalid", K(*this));
  } else if (is_expired_clean()) {
    parameter = expired_time_;
  } else if (is_backup_set_clean()) {
    parameter = backup_set_id_;
  } else if (is_empty_clean_type()) {
    // do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get clean paramater", K(*this));
  }
  return ret;
}

int ObBackupCleanInfo::set_clean_parameter(const int64_t parameter)
{
  int ret = OB_SUCCESS;
  if (parameter < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set clean parameter get invalid argument", K(ret), K(parameter));
  } else if (is_empty_clean_type() && ObBackupCleanInfoStatus::STOP == status_) {
    // do nothing
  } else if (is_expired_clean()) {
    expired_time_ = parameter;
  } else if (is_backup_set_clean()) {
    backup_set_id_ = parameter;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean info is invalid, can not set parameter", K(ret), K(*this), K(parameter));
  }
  return ret;
}

// TODOcheck
/* ObTenantBackupBackupsetTaskItem */
ObTenantBackupBackupsetTaskItem::ObTenantBackupBackupsetTaskItem()
    : tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      incarnation_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      backup_type_(),
      snapshot_version_(-1),
      prev_full_backup_set_id_(-1),
      prev_inc_backup_set_id_(-1),
      prev_backup_data_version_(-1),
      input_bytes_(0),
      output_bytes_(0),
      start_ts_(-1),
      end_ts_(-1),
      compatible_(false),
      cluster_id_(-1),
      cluster_version_(-1),
      task_status_(TaskStatus::MAX),
      src_backup_dest_(),
      dst_backup_dest_(),
      backup_data_version_(-1),
      backup_schema_version_(-1),
      total_pg_count_(-1),
      finish_pg_count_(-1),
      total_partition_count_(-1),
      finish_partition_count_(-1),
      total_macro_block_count_(-1),
      finish_macro_block_count_(-1),
      result_(-1),
      encryption_mode_(),
      passwd_(),
      is_mark_deleted_(false)
{}

ObTenantBackupBackupsetTaskItem::~ObTenantBackupBackupsetTaskItem()
{}

void ObTenantBackupBackupsetTaskItem::reset()
{}

bool ObTenantBackupBackupsetTaskItem::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && job_id_ > 0 && incarnation_ > 0 && backup_set_id_ >= 0 && copy_id_ >= 0 &&
         backup_type_.is_valid() && task_status_ >= ObTenantBackupBackupsetTaskItem::GENERATE &&
         task_status_ < ObTenantBackupBackupsetTaskItem::MAX;
}

int ObTenantBackupBackupsetTaskItem::set_status(const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tenant backup backupset task status get invalid argument", KR(ret), KP(buf));
  } else {
    const char* tenant_backup_backupset_task_status_strs[] = {"GENERATE", "BACKUP", "FINISH", "CANCEL"};
    TaskStatus tmp_status = MAX;
    STATIC_ASSERT(MAX == ARRAYSIZEOF(tenant_backup_backupset_task_status_strs), "status count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(tenant_backup_backupset_task_status_strs); i++) {
      if (0 == STRCMP(tenant_backup_backupset_task_status_strs[i], buf)) {
        tmp_status = static_cast<TaskStatus>(i);
      }
    }

    if (MAX == tmp_status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("backup status str not found", KR(ret), K(buf));
    } else {
      task_status_ = tmp_status;
    }
  }
  return ret;
}

const char* ObTenantBackupBackupsetTaskItem::get_status_str(const TaskStatus& status)
{
  const char* str = "UNKNOWN";
  const char* backup_backupset_task_status_strs[] = {"GENERATE", "BACKUP", "FINISH", "CANCEL"};
  STATIC_ASSERT(MAX == ARRAYSIZEOF(backup_backupset_task_status_strs), "status count mismatch");
  if (status < 0 || status >= MAX) {
    LOG_ERROR("invalid backup backupset task info status", K(status));
  } else {
    str = backup_backupset_task_status_strs[status];
  }
  return str;
}

int ObTenantBackupBackupsetTaskItem::convert_to_backup_task_info(ObTenantBackupTaskInfo& info) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("self should be valid", KR(ret), K(this));
  } else {
    info.tenant_id_ = tenant_id_;
    info.backup_set_id_ = backup_set_id_;
    info.incarnation_ = incarnation_;
    info.snapshot_version_ = snapshot_version_;
    info.prev_full_backup_set_id_ = prev_full_backup_set_id_;
    info.prev_inc_backup_set_id_ = prev_inc_backup_set_id_;
    info.prev_backup_data_version_ = prev_backup_data_version_;
    info.pg_count_ = total_pg_count_;
    info.macro_block_count_ = total_macro_block_count_;
    info.finish_pg_count_ = finish_pg_count_;
    info.finish_macro_block_count_ = finish_macro_block_count_;
    info.input_bytes_ = input_bytes_;
    info.output_bytes_ = output_bytes_;
    info.start_time_ = start_ts_;
    info.end_time_ = end_ts_;
    info.compatible_ = compatible_;
    info.cluster_version_ = cluster_version_;
    info.backup_type_ = backup_type_;
    info.status_ = ObTenantBackupTaskItem::FINISH;
    info.device_type_ = dst_backup_dest_.device_type_;
    info.result_ = result_;
    info.cluster_id_ = cluster_id_;
    info.backup_dest_ = dst_backup_dest_;
    info.backup_data_version_ = backup_data_version_;
    info.backup_schema_version_ = backup_schema_version_;
    info.partition_count_ = total_partition_count_;
    info.finish_partition_count_ = total_partition_count_;
    info.encryption_mode_ = encryption_mode_;
    info.passwd_ = passwd_;
    info.is_mark_deleted_ = is_mark_deleted_;
  }
  return ret;
}

int ObBackupCleanInfo::check_backup_clean_info_match(const ObBackupCleanInfo& clean_info) const
{
  int ret = OB_SUCCESS;
  if (!clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(clean_info));
  } else if (tenant_id_ != clean_info.tenant_id_ || job_id_ != clean_info.job_id_ ||
             incarnation_ != clean_info.incarnation_ || type_ != clean_info.type_ || status_ != clean_info.status_) {
    ret = OB_BACKUP_CLEAN_INFO_NOT_MATCH;
    LOG_WARN("backup clean info is not match", K(ret), K(*this), K(clean_info));
  }
  return ret;
}

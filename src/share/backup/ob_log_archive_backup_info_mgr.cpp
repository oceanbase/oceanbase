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
#include "ob_log_archive_backup_info_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/restore/ob_storage.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "ob_backup_path.h"
#include "ob_backup_file_lock_mgr.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

OB_SERIALIZE_MEMBER(ObExternLogArchiveBackupInfo, status_array_);
ObExternLogArchiveBackupInfo::ObExternLogArchiveBackupInfo() : status_array_()
{}

ObExternLogArchiveBackupInfo::~ObExternLogArchiveBackupInfo()
{}

void ObExternLogArchiveBackupInfo::reset()
{
  status_array_.reset();
}

bool ObExternLogArchiveBackupInfo::is_valid() const
{
  bool bool_ret = true;
  const int64_t count = status_array_.count();
  for (int64_t i = 0; bool_ret && i < count; ++i) {
    bool_ret = status_array_.at(i).is_valid();
  }
  return bool_ret;
}

int64_t ObExternLogArchiveBackupInfo::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternLogArchiveBackupInfo::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_LOG_ARCHIVE_BACKUP_INFO;
    common_header->data_version_ = LOG_ARCHIVE_BACKUP_INFO_FILE_VERSION;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObExternLogArchiveBackupInfo::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check archive backup info", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize archive backup ifno", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::update(const ObTenantLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;

  if (!status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(status));
  } else if (status_array_.empty()) {
    if (OB_FAIL(status_array_.push_back(status))) {
      LOG_WARN("Failed to add new status", K(ret));
    } else {
      FLOG_INFO("add first new status", K(status));
    }
  } else {
    ObTenantLogArchiveStatus& last = status_array_.at(status_array_.count() - 1);

    if (last.incarnation_ != status.incarnation_) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("not support diff incarnation", K(ret), K(last), K(status));
    } else if (last.round_ < status.round_) {
      if (last.status_ != ObLogArchiveStatus::STOP) {
        if (ObLogArchiveStatus::BEGINNING == status.status_) {
          ObTenantLogArchiveStatus new_status = last;
          new_status.status_ = ObLogArchiveStatus::STOP;
          if (OB_FAIL(last.update(new_status))) {
            LOG_WARN("failed to update last", K(ret), K(last), K(new_status));
          }
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("last status must be stop", K(ret), K(last), K(status));
        }
      }

      if (FAILEDx(status_array_.push_back(status))) {
        LOG_WARN("failed to add status", K(ret));
      } else {
        LOG_INFO("[LOG_ARCHIVE] add new log archive status", K(status), K(last));
      }
    } else if (last.round_ == status.round_) {
      if (OB_FAIL(last.update(status))) {
        LOG_WARN("failed to update last", K(ret), K(last), K(status));
      }
    } else {
      ret = OB_LOG_ARCHIVE_INVALID_ROUND;
      LOG_ERROR("invalid round", K(ret), K(last), K(status));
    }
  }

  return ret;
}

int ObExternLogArchiveBackupInfo::get_last(ObTenantLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  status.reset();

  if (status_array_.empty()) {
    ret = OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST;
  } else {
    status = status_array_.at(status_array_.count() - 1);
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::get_log_archive_status(
    const int64_t restore_timestamp, ObTenantLogArchiveStatus& status)
{
  int ret = OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST;

  status.reset();

  for (int64_t i = 0; i < status_array_.count(); ++i) {
    const ObTenantLogArchiveStatus& cur_status = status_array_.at(i);
    if (cur_status.start_ts_ <= restore_timestamp && restore_timestamp <= cur_status.checkpoint_ts_) {
      status = status_array_.at(i);
      ret = OB_SUCCESS;
      break;
    }
  }

  if (OB_FAIL(ret)) {
    FLOG_INFO("not found need status", K(ret), K(restore_timestamp), K(status_array_));
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::get_log_archive_status(ObIArray<ObTenantLogArchiveStatus>& status_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(status_array.assign(status_array_))) {
    LOG_WARN("failed to get log archive status", K(ret), K(status_array_));
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::mark_log_archive_deleted(const ObIArray<int64_t>& round_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < round_ids.count(); ++i) {
    const int64_t round_id = round_ids.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < status_array_.count(); ++j) {
      ObTenantLogArchiveStatus& tenant_archive_status = status_array_.at(j);
      if (tenant_archive_status.round_ != round_id) {
        // do nothing
      } else {
        tenant_archive_status.is_mark_deleted_ = true;
        break;
      }
    }
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::delete_marked_log_archive_info(const common::ObIArray<int64_t>& round_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < round_ids.count(); ++i) {
    const int64_t round_id = round_ids.at(i);
    for (int64_t j = status_array_.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      ObTenantLogArchiveStatus& tenant_archive_status = status_array_.at(j);
      if (tenant_archive_status.is_mark_deleted_ && round_id == tenant_archive_status.round_) {
        if (OB_FAIL(status_array_.remove(j))) {
          LOG_WARN("failed to remove tenant archive status", K(ret), K(tenant_archive_status));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExternalBackupPieceInfo, piece_array_);
ObExternalBackupPieceInfo::ObExternalBackupPieceInfo() : piece_array_()
{}

ObExternalBackupPieceInfo::~ObExternalBackupPieceInfo()
{}

void ObExternalBackupPieceInfo::reset()
{
  piece_array_.reset();
}

bool ObExternalBackupPieceInfo::is_valid() const
{
  bool bool_ret = true;
  const int64_t count = piece_array_.count();
  for (int64_t i = 0; bool_ret && i < count; ++i) {
    bool_ret = piece_array_.at(i).is_valid();
  }
  return bool_ret;
}

int64_t ObExternalBackupPieceInfo::get_write_buf_size() const
{
  int64_t size = sizeof(ObBackupCommonHeader);
  size += this->get_serialize_size();
  return size;
}

int ObExternalBackupPieceInfo::write_buf(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader* common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_PIECE_INFO;
    common_header->data_version_ = LOG_ARCHIVE_BACKUP_PIECE_FILE_VERSION;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(this->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObExternalBackupPieceInfo::read_buf(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check archive backup info", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize archive backup ifno", K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternalBackupPieceInfo::update(const share::ObBackupPieceInfo& piece)  // update or insert
{
  int ret = OB_SUCCESS;
  common::ObSArray<share::ObBackupPieceInfo> tmp_array;
  bool is_added = false;

  if (OB_FAIL(tmp_array.reserve(piece_array_.count() + 1))) {
    LOG_WARN("failed to reserve array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < piece_array_.count(); ++i) {
    share::ObBackupPieceInfo& cur_piece = piece_array_.at(i);
    if (piece.key_ == cur_piece.key_) {
      is_added = true;
      FLOG_INFO("update exist piece for external piece info", K(cur_piece), K(piece));
      if (OB_FAIL(tmp_array.push_back(piece))) {
        LOG_WARN("failed to add piece", K(ret));
      }
    } else {
      if (!is_added && piece.key_ < cur_piece.key_) {
        if (OB_FAIL(tmp_array.push_back(piece))) {
          LOG_WARN("failed to add cur piece", K(ret));
        } else {
          is_added = true;
          FLOG_INFO("add new piece to external piece info", K(cur_piece));
        }
      }

      if (FAILEDx(tmp_array.push_back(cur_piece))) {
        LOG_WARN("failed to add cur piece", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !is_added) {
    is_added = true;
    if (OB_FAIL(tmp_array.push_back(piece))) {
      LOG_WARN("failed to add piece", K(ret), K(piece));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("after update external piece info",
        K(piece),
        "new_count",
        tmp_array.count(),
        "old_count",
        piece_array_.count(),
        "new_piece_array",
        tmp_array,
        "old_piece_array",
        piece_array_);
    if (OB_FAIL(piece_array_.assign(tmp_array))) {
      LOG_WARN("failed to copy piece array_", K(ret), K(tmp_array));
    }
  }

  return ret;
}

int ObExternalBackupPieceInfo::get_piece_array(common::ObIArray<share::ObBackupPieceInfo>& piece_array)
{
  int ret = OB_SUCCESS;
  piece_array.reset();

  if (OB_FAIL(piece_array.assign(piece_array_))) {
    LOG_WARN("failed to copy piece array", K(ret));
  }
  return ret;
}

int ObExternalBackupPieceInfo::mark_deleting(
    const common::ObIArray<ObBackupPieceInfoKey>& piece_keys)  // update or insert
{
  int ret = OB_SUCCESS;
  const ObBackupFileStatus::STATUS dest_file_status = ObBackupFileStatus::BACKUP_FILE_DELETING;
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_keys.count(); ++i) {
    const ObBackupPieceInfoKey& piece_key = piece_keys.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < piece_array_.count(); ++j) {
      ObBackupPieceInfo& cur_piece = piece_array_.at(j);
      const ObBackupPieceInfoKey& cur_piece_key = cur_piece.key_;
      if (cur_piece_key == piece_key) {
        if (ObBackupFileStatus::BACKUP_FILE_DELETING == cur_piece.file_status_ ||
            ObBackupFileStatus::BACKUP_FILE_DELETED == cur_piece.file_status_) {
          // do nothing
        } else {
          cur_piece.file_status_ = dest_file_status;
          break;
        }
      }
    }
  }
  return ret;
}

int ObExternalBackupPieceInfo::mark_deleted(const common::ObIArray<share::ObBackupPieceInfoKey>& piece_keys)
{
  int ret = OB_SUCCESS;
  const ObBackupFileStatus::STATUS dest_file_status = ObBackupFileStatus::BACKUP_FILE_DELETED;
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_keys.count(); ++i) {
    const ObBackupPieceInfoKey& piece_key = piece_keys.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < piece_array_.count(); ++j) {
      ObBackupPieceInfo& cur_piece = piece_array_.at(j);
      const ObBackupPieceInfoKey& cur_piece_key = cur_piece.key_;
      if (cur_piece_key == piece_key) {
        if (ObBackupFileStatus::BACKUP_FILE_DELETED == cur_piece.file_status_) {
          // do nothing
        } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(cur_piece.file_status_, dest_file_status))) {
          LOG_WARN("failed to check can change file stauts", K(ret), K(cur_piece));
        } else {
          cur_piece.file_status_ = dest_file_status;
          break;
        }
      }
    }
  }
  return ret;
}

bool ObExternalBackupPieceInfo::is_all_piece_info_deleted() const
{
  bool ret = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_array_.count(); ++i) {
    const ObBackupPieceInfo& cur_piece = piece_array_.at(i);
    if (ObBackupFileStatus::BACKUP_FILE_DELETED != cur_piece.file_status_) {
      ret = false;
      break;
    }
  }
  return ret;
}

ObLogArchiveBackupInfoMgr::ObLogArchiveBackupInfoMgr() : is_backup_backup_(false)
{}

ObLogArchiveBackupInfoMgr::~ObLogArchiveBackupInfoMgr()
{}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info(common::ObISQLClient& sql_client, const bool for_update,
    const uint64_t tenant_id, const ObBackupInnerTableVersion& version, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (tenant_id == OB_INVALID_ID || !is_valid_backup_inner_table_version(version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(version));
  } else if (!sql_client.is_active()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("sql client is not active", K(ret));
  } else if (version >= OB_BACKUP_INNER_TABLE_V2) {
    if (OB_FAIL(get_log_archive_backup_info_v2_(sql_client, for_update, tenant_id, info))) {
      LOG_WARN("failed to get_log_archive_backup_info_v2", K(ret), K(tenant_id));
    }
  } else {
    if (OB_FAIL(get_log_archive_backup_info_v1_(sql_client, for_update, tenant_id, info))) {
      LOG_WARN("failed to get_log_archive_backup_info_v1", K(ret), K(tenant_id));
    }
  }
  return ret;
}

// only used to read
int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info_compatible(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  info.reset();

  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (!sql_client.is_active()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("sql client is not active", K(ret));
  } else if (is_backup_backup_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("not support backup backup for this function", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_log_archive_backup_info_v2_(sql_client, for_update, tenant_id, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_log_archive_backup_info_v2", K(ret), K(tenant_id));
    } else {
      if (OB_FAIL(get_log_archive_backup_info_v1_(sql_client, for_update, tenant_id, info))) {
        LOG_WARN("failed to get_log_archive_backup_info_v1", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

// create sys log archive status if not exist
int ObLogArchiveBackupInfoMgr::check_sys_log_archive_status(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo info;
  const bool for_update = false;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t init_round_id = OB_START_ROUND_ID - 1;

  if (OB_FAIL(get_log_archive_backup_info_v2_(sql_client, for_update, tenant_id, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get log archive backup info", K(ret));
    } else {
      ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.assign_fmt(
              "insert into %s(%s, %s, %s, %s, %s, %s) values(%lu, %ld, %ld, usec_to_time(0), usec_to_time(0), '%s')",
              OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME,
              OB_STR_TENANT_ID,
              OB_STR_INCARNATION,
              OB_STR_LOG_ARCHIVE_ROUND,
              OB_STR_MIN_FIRST_TIME,
              OB_STR_MAX_NEXT_TIME,
              OB_STR_STATUS,
              tenant_id,
              OB_START_INCARNATION,
              init_round_id,
              ObLogArchiveStatus::get_str(ObLogArchiveStatus::STOP)))) {
        LOG_WARN("failed to set sql", K(ret));
      } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("affected row should be one", K(ret), K(affected_rows));
      }
    }
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info_v1_(
    common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (!sql_client.is_active()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("sql client is not active", K(ret));
  } else if (for_update && OB_FAIL(get_log_archive_backup_info_with_lock_(sql_client, tenant_id, info))) {
    LOG_WARN("failed to get_log_archive_backup_info_", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_log_archive_status_(sql_client, for_update, tenant_id, info))) {
    LOG_WARN("failed to get_log_archive_status_", K(ret), K(tenant_id));
  } else {
    LOG_INFO("succeed to get_log_archive_backup_info", K(tenant_id), K(info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info_v2_(
    common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    const char* dest_name = is_backup_backup_ ? OB_STR_BACKUP_BACKUP_DEST : OB_STR_BACKUP_DEST;

    if (OB_FAIL(sql.assign_fmt("select  %s, %s, %s, %s, time_to_usec(%s) %s, time_to_usec(%s) %s, status",
            dest_name,
            OB_STR_TENANT_ID,
            OB_STR_INCARNATION,
            OB_STR_LOG_ARCHIVE_ROUND,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MAX_NEXT_TIME,
            OB_STR_MAX_NEXT_TIME))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (!is_backup_backup_) {  // TODO(yanfeng): use same as backup backup?
      if (OB_FAIL(sql.append_fmt(" , %s, %s, %s, %s ",
              OB_STR_IS_MOUNT_FILE_CREATED,
              OB_STR_COMPATIBLE,
              OB_STR_BACKUP_PIECE_ID,
              OB_STR_START_PIECE_ID))) {
        LOG_WARN("failed to append sql", K(ret), K(sql));
      }
    }

    if (FAILEDx(sql.append_fmt(" from %s where tenant_id=%lu ", get_cur_table_name_(), tenant_id))) {
      LOG_WARN("failed to append sql", K(ret), K(sql));
    } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("failed to add for update", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(parse_log_archive_status_result_(*result, info))) {
      LOG_WARN("failed to get_log_archive_status_", K(ret), K(sql), K(info));
    } else {
      LOG_INFO("succeed to get backup log archive backup status", K(sql), K(info));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_backup_info(
    common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;

    if (OB_FAIL(sql.assign_fmt(
            "select %s, %s, %s, %s, time_to_usec(%s) %s, time_to_usec(%s) %s, status from %s where tenant_id = %ld",
            OB_STR_TENANT_ID,
            OB_STR_INCARNATION,
            OB_STR_LOG_ARCHIVE_ROUND,
            OB_STR_COPY_ID,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MAX_NEXT_TIME,
            OB_STR_MAX_NEXT_TIME,
            OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME,
            tenant_id))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("failed to add for update", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, real_tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next()) && OB_ITER_END != ret) {
      OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
    } else if (OB_UNLIKELY(OB_ITER_END == ret)) {
      // 仅租户级归档backup info未持久化之前获取出现OB_ITER_END的情况
      ret = OB_ENTRY_NOT_EXIST;
      OB_LOG(WARN, "tenant backup info not exist", K(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(parse_backup_backup_log_archive_status_(*result, info))) {
      LOG_WARN("failed to parse_log_archive_status_", K(ret), K(sql));
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to get log archive backup status", K(sql), K(info));
      int tmp_ret = result->next();
      if (OB_ITER_END != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        ObLogArchiveBackupInfo tmp_info = info;
        if (OB_SUCCESS != (tmp_ret = parse_backup_backup_log_archive_status_(*result, tmp_info))) {
          LOG_WARN("failed to parse_log_archive_status_", K(ret), K(tmp_ret));
        } else {
          LOG_WARN("failed to get next", K(ret), K(tmp_ret), K(tmp_info), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_checkpoint(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLogArchiveBackupInfo info;
  checkpoint_ts = 0;

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
  } else if (OB_FAIL(get_log_archive_backup_info_compatible(sql_client, tenant_id, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get get_log_archive_backup_info_compatible", K(ret), K(tenant_id));
    } else {
      checkpoint_ts = 0;
      ret = OB_SUCCESS;
    }
  } else if (ObLogArchiveStatus::DOING != info.status_.status_ &&
             ObLogArchiveStatus::BEGINNING != info.status_.status_) {
    checkpoint_ts = 0;
  } else {
    checkpoint_ts = info.status_.checkpoint_ts_;
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_create_tenant_timestamp(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, int64_t& create_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    create_ts = 0;

    if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
    } else if (OB_FAIL(sql.assign_fmt("select time_to_usec(gmt_create) create_ts from oceanbase.__all_tenant_history "
                                      " where tenant_id = %lu order by schema_version asc limit 1;",
                   tenant_id))) {
      LOG_WARN("failed to init get create tenant timestamp sql", K(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
      } else {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", K(ret), K(tenant_id), K(sql));
      }
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "create_ts", create_ts, int64_t);

      LOG_INFO("get_create_tenant_timestamp", K(tenant_id), K(create_ts));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info_with_lock_(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
    int real_length = 0;
    int64_t read_count = 0;
    const int64_t expect_read_count = 2;

    if (OB_FAIL(sql.assign_fmt("select name, value from %s where name in ('%s','%s') and tenant_id=%lu for update",
            OB_ALL_TENANT_BACKUP_INFO_TNAME,
            OB_STR_LOG_ARCHIVE_STATUS,
            OB_STR_BACKUP_DEST,
            tenant_id))) {
      LOG_WARN("failed to init backup info sql", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }

      EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name_str, sizeof(name_str), real_length);
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "value", value_str, sizeof(value_str), real_length);

      if (OB_FAIL(ret)) {
      } else if (0 == strcmp(name_str, OB_STR_LOG_ARCHIVE_STATUS)) {
        ++read_count;
        ObLogArchiveStatus::STATUS status = ObLogArchiveStatus::get_status(value_str);
        if (!ObLogArchiveStatus::is_valid(status)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("invalid log archive status", K(ret), K(status), K(name_str), K(value_str));
        } else {
          info.status_.status_ = status;
        }
      } else if (0 == strcmp(name_str, OB_STR_BACKUP_DEST)) {
        ++read_count;
        if (OB_FAIL(databuff_printf(info.backup_dest_, sizeof(info.backup_dest_), "%s", value_str))) {
          LOG_WARN("failed to copy backup dest", K(ret), K(name_str), K(value_str));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == read_count) {
        ret = OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST;
        LOG_INFO("log archive backup info is not exist", K(ret), K(tenant_id), K(sql));
      } else if (expect_read_count != read_count) {
        ret = OB_BACKUP_INFO_NOT_MATCH;
        LOG_WARN("log archive backup info count is not match", K(ret), K(tenant_id), K(read_count), K(sql));
      } else {
        LOG_INFO("succeed to get log archive backup info", K(sql));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_log_archive_status_result_(
    sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(result.next()) && OB_ITER_END != ret) {
    OB_LOG(WARN, "fail to get next result", K(ret));
  } else if (OB_UNLIKELY(OB_ITER_END == ret)) {
    ret = OB_ENTRY_NOT_EXIST;
    OB_LOG(WARN, "tenant log archive status info not exist", K(ret));
  } else if (OB_FAIL(parse_log_archive_status_(result, info))) {
    LOG_WARN("failed to parse_log_archive_status_", K(ret));
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = result.next();
    if (OB_ITER_END != tmp_ret) {
      ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
      ObLogArchiveBackupInfo tmp_info = info;
      if (OB_SUCCESS != (tmp_ret = parse_log_archive_status_(result, tmp_info))) {
        LOG_WARN("failed to parse_log_archive_status_", K(ret), K(tmp_ret));
      } else {
        LOG_WARN("should not get more than one row", K(ret), K(tmp_ret), K(tmp_info));
      }
    }
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_log_archive_status_(sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int real_length = 0;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  ObTenantLogArchiveStatus& status = info.status_;
  UNUSED(real_length);  // only used for EXTRACT_STRBUF_FIELD_MYSQL

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, status.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, status.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LOG_ARCHIVE_ROUND, status.round_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MIN_FIRST_TIME, status.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_NEXT_TIME, status.checkpoint_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  EXTRACT_BOOL_FIELD_MYSQL(result, OB_STR_IS_MOUNT_FILE_CREATED, status.is_mount_file_created_);

  if (!is_backup_backup_) {
    int64_t tmp_compatible = 0;
    EXTRACT_BOOL_FIELD_MYSQL(result, OB_STR_IS_MOUNT_FILE_CREATED, status.is_mount_file_created_);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, tmp_compatible, int64_t);
    status.compatible_ = static_cast<ObTenantLogArchiveStatus::COMPATIBLE>(tmp_compatible);

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!ObTenantLogArchiveStatus::is_compatible_valid(status.compatible_))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid compatible version", K(ret), K(tmp_compatible));
      }
    }
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BACKUP_PIECE_ID, status.backup_piece_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_PIECE_ID, status.start_piece_id_, int64_t);
  }
  if (is_backup_backup_) {
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  } else {
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(info.backup_dest_, sizeof(info.backup_dest_), "%s", value_str))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(value_str));
    }
  }
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    ObLogArchiveStatus::STATUS tmp_status = ObLogArchiveStatus::get_status(value_str);

    // for 2.2.40 upgrade cluster
    if (strlen(value_str) == 0) {
      tmp_status = ObLogArchiveStatus::STOP;
    }
    if (!ObLogArchiveStatus::is_valid(tmp_status)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid log archive status", K(ret), K(tmp_status), K(value_str));
    } else {
      status.status_ = tmp_status;
      LOG_INFO("get status", K(tmp_status), K(value_str));
    }
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_status_(
    common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;

    if (OB_FAIL(
            sql.assign_fmt("select a.value %s, b.%s,b.%s,b.%s, time_to_usec(b.%s) %s, time_to_usec(b.%s) %s, b.status",
                OB_STR_BACKUP_DEST,
                OB_STR_TENANT_ID,
                OB_STR_INCARNATION,
                OB_STR_LOG_ARCHIVE_ROUND,
                OB_STR_MIN_FIRST_TIME,
                OB_STR_MIN_FIRST_TIME,
                OB_STR_MAX_NEXT_TIME,
                OB_STR_MAX_NEXT_TIME))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (OB_FAIL(sql.append_fmt(" , b.%s, b.%s, 0 as %s, 0 as %s ",
                   OB_STR_IS_MOUNT_FILE_CREATED,
                   OB_STR_COMPATIBLE,
                   OB_STR_BACKUP_PIECE_ID,
                   OB_STR_START_PIECE_ID))) {
      LOG_WARN("failed to append sql", K(ret), K(sql));
    } else if (is_backup_backup_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("backup backup should not use old get_log_archive_status", K(ret), K(tenant_id));
    }

    if (FAILEDx(sql.append_fmt(" from %s a, %s b where a.tenant_id=%lu and a.name = '%s' and b.tenant_id=a.tenant_id",
            OB_ALL_TENANT_BACKUP_INFO_TNAME,
            OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
            tenant_id,
            OB_STR_BACKUP_DEST))) {
      LOG_WARN("failed to append sql", K(ret), K(sql));
    } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("failed to add for update", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(parse_log_archive_status_result_(*result, info))) {
      LOG_WARN("failed to get_log_archive_status_", K(ret), K(sql), K(info));
    } else {
      LOG_INFO("succeed to get backup log archive backup status", K(sql), K(info));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_last_extern_log_archive_backup_info(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, ObTenantLogArchiveStatus& last_status)
{
  int ret = OB_SUCCESS;
  ObExternLogArchiveBackupInfo extern_info;
  last_status.reset();
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (!cluster_backup_dest.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(
                 path, cluster_backup_dest.get_storage_info(), extern_info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(extern_info.get_last(last_status))) {
    if (OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get last status", K(ret), K(cluster_backup_dest), K(tenant_id));
    } else {
      LOG_INFO("extern log archive backup info not exist", K(ret), K(cluster_backup_dest), K(tenant_id));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info_v1_(
    common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info));
  } else if (OB_FAIL(update_log_archive_backup_info_(sql_client, info))) {
    LOG_WARN("failed to update_log_archive_backup_info_", K(ret), K(info));
  } else if (OB_FAIL(update_log_archive_status_(sql_client, info))) {
    LOG_WARN("failed to update_log_archive_status_", K(ret), K(info));
  } else {
    LOG_INFO("succeed to update_log_archive_backup_info", K(info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info(
    common::ObISQLClient& sql_client, const ObBackupInnerTableVersion& version, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;

  if (!is_valid_backup_inner_table_version(version) || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(version), K(info));
  } else if (OB_BACKUP_INNER_TABLE_V1 == version) {
    if (OB_FAIL(update_log_archive_backup_info_v1_(sql_client, info))) {
      LOG_WARN("failed to update_log_archive_backup_info_v1_", K(ret), K(info));
    }
  } else {
    if (OB_FAIL(update_log_archive_backup_info_v2_(sql_client, info))) {
      LOG_WARN("failed to update_log_archive_backup_info_v2_", K(ret), K(version), K(info));
    }
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info_v2_(
    common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;
  const char* status_str = ObLogArchiveStatus::get_str(info.status_.status_);

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid info", K(ret), K(info));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret));
  } else if (is_backup_backup_ && OB_FAIL(dml_splicer.add_column(OB_STR_COPY_ID, info.status_.copy_id_))) {
    LOG_WARN("failed to set copy id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_INCARNATION, info.status_.incarnation_))) {
    LOG_WARN("failed to set incarnation id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_LOG_ARCHIVE_ROUND, info.status_.round_))) {
    LOG_WARN("failed to set round id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MIN_FIRST_TIME, info.status_.start_ts_))) {
    LOG_WARN("failed to set start ts", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MAX_NEXT_TIME, info.status_.checkpoint_ts_))) {
    LOG_WARN("failed to set checkpoint", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_STATUS, status_str))) {
    LOG_WARN("failed to set checkpoint", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_IS_MOUNT_FILE_CREATED, info.status_.is_mount_file_created_))) {
    LOG_WARN("failed to set is_mount_file_created", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_COMPATIBLE, info.status_.compatible_))) {
    LOG_WARN("failed to set compatible", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_PIECE_ID, info.status_.backup_piece_id_))) {
    LOG_WARN("failed to set backup_piece_id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_START_PIECE_ID, info.status_.start_piece_id_))) {
    LOG_WARN("failed to set piece_switch_interval_us", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_DEST, info.backup_dest_))) {
    LOG_WARN("failed to set piece_switch_interval_us", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(get_cur_table_name_(), sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to exec sql", K(affected_rows), K(sql));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_status_history(common::ObISQLClient& sql_proxy,
    const ObLogArchiveBackupInfo& info, const int64_t inner_table_version,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (!info.is_valid() || ObLogArchiveStatus::STOP != info.status_.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args or status is not STOP", K(ret), K(info));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_INCARNATION, info.status_.incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_LOG_ARCHIVE_ROUND, info.status_.round_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (is_backup_backup_ && OB_FAIL(dml_splicer.add_column(OB_STR_COPY_ID, info.status_.copy_id_))) {
    LOG_WARN("failed to add colomn", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MIN_FIRST_TIME, info.status_.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MAX_NEXT_TIME, info.status_.checkpoint_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_DEST, info.backup_dest_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_IS_MARK_DELETED, info.status_.is_mark_deleted_))) {
    LOG_WARN("failed to set compatible", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_COMPATIBLE, info.status_.compatible_))) {
    LOG_WARN("failed to set compatible", K(ret));
  } else if (inner_table_version < ObBackupInnerTableVersion::OB_BACKUP_INNER_TABLE_V2) {
    FLOG_INFO(
        "inner table version is old, skip update start piece id and backup piece id", K(ret), K(inner_table_version));
  } else {
    if (OB_FAIL(dml_splicer.add_column(OB_STR_START_PIECE_ID, info.status_.start_piece_id_))) {
      LOG_WARN("failed to set start piece id", K(ret));
    } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_PIECE_ID, info.status_.backup_piece_id_))) {
      LOG_WARN("failed to set backup piece id", K(ret));
    }
  }

  if (FAILEDx(dml_splicer.splice_replace_sql(get_his_table_name_(), sql))) {
    LOG_WARN("failed to splice replace sql", K(ret));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    FLOG_INFO("succeed to update_log_archive_status_history", K(affected_rows), K(sql), K(info));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_tenant_log_archive_status_v2(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (tenant_id <= 0 || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("failed to add pk column", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml_splicer.splice_delete_sql(OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME, sql))) {
    LOG_WARN("failed to splice delete sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete", K(ret), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected row must be 1", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to delete tenant log archive status v2", K(ret), K(sql));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_tenant_log_archive_status_v1(
    common::ObISQLClient& sql_proxy, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, tenant_id))) {
    LOG_WARN("failed to add pk column", K(ret), K(tenant_id));
  } else if (OB_FAIL(dml_splicer.splice_delete_sql(OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME, sql))) {
    LOG_WARN("failed to splice delete sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to delete tenant log archive status v1", K(ret), K(sql));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_all_active_log_archive_tenants(
    common::ObISQLClient& sql_proxy, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  tenant_ids.reset();

  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(sql.assign_fmt("select tenant_id from %s", OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          }
        }
        int64_t tenant_id = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_TENANT_ID, tenant_id, uint64_t);
        if (FAILEDx(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("failed to add tenant_id", K(ret));
        }
      }
    }
  }

  LOG_INFO("get_all_active_log_archive_tenants", K(ret), K(sql), K(tenant_ids));
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info_(
    common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char* status_str = ObLogArchiveStatus::get_str(info.status_.status_);
  const char* dest_name = is_backup_backup_ ? OB_STR_BACKUP_BACKUP_DEST : OB_STR_BACKUP_DEST;

  if (is_backup_backup_) {
    // do nothing
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '%s' where name= '%s' and tenant_id=%lu",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 status_str,
                 OB_STR_LOG_ARCHIVE_STATUS,
                 info.status_.tenant_id_))) {
    LOG_WARN("failed to assign sql", K(ret), K(info), K(status_str));
  } else if (OB_FAIL(sql_client.write(info.status_.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(info), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(info), K(sql));
  } else {
    LOG_INFO("succeed to exec sql", K(sql));
  }

  if (is_backup_backup_) {
    // do nothing
  } else if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '%s' where name = '%s' and tenant_id=%lu",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 info.backup_dest_,
                 dest_name,
                 info.status_.tenant_id_))) {
    LOG_WARN("failed to assign sql", K(ret), K(info));
  } else if (OB_FAIL(sql_client.write(info.status_.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(info), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(info), K(sql));
  } else {
    LOG_INFO("succeed to exec sql", K(sql));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_status_(
    common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;
  const char* status_str = ObLogArchiveStatus::get_str(info.status_.status_);
  const uint64_t real_tenant_id = get_real_tenant_id(info.status_.tenant_id_);

  if (!info.is_valid() || is_backup_backup_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid info", K(ret), K(is_backup_backup_), K(info));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_INCARNATION, info.status_.incarnation_))) {
    LOG_WARN("failed to set incarnation id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_LOG_ARCHIVE_ROUND, info.status_.round_))) {
    LOG_WARN("failed to set round id", K(ret));
  } else if (is_backup_backup_ && OB_FAIL(dml_splicer.add_column(OB_STR_COPY_ID, info.status_.copy_id_))) {
    LOG_WARN("failed to set copy id", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MIN_FIRST_TIME, info.status_.start_ts_))) {
    LOG_WARN("failed to set start ts", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MAX_NEXT_TIME, info.status_.checkpoint_ts_))) {
    LOG_WARN("failed to set checkpoint", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_STATUS, status_str))) {
    LOG_WARN("failed to set checkpoint", K(ret));
  } else if (!is_backup_backup_ &&
             OB_FAIL(dml_splicer.add_column(OB_STR_IS_MOUNT_FILE_CREATED, info.status_.is_mount_file_created_))) {
    LOG_WARN("failed to set is_mount_file_created", K(ret));
  } else if (!is_backup_backup_ && OB_FAIL(dml_splicer.add_column(OB_STR_COMPATIBLE, info.status_.compatible_))) {
    LOG_WARN("failed to set compatible", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql_client.write(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid affected_rows", K(ret), K(affected_rows), K(sql));
  } else {
    LOG_INFO("succeed to exec sql", K(sql));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_extern_log_archive_backup_info(
    const ObLogArchiveBackupInfo& info, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObExternLogArchiveBackupInfo extern_info;
  const uint64_t tenant_id = info.status_.tenant_id_;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (OB_FAIL(cluster_backup_dest.set(info.backup_dest_, info.status_.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(info));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(
                 path, cluster_backup_dest.get_storage_info(), extern_info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(extern_info.update(info.status_))) {
    LOG_WARN("failed to update extern info", K(ret), K(info));
  } else if (OB_FAIL(write_extern_log_archive_backup_info(
                 cluster_backup_dest, tenant_id, extern_info, backup_lease_service))) {
    LOG_WARN("failed to write_extern_log_archive_backup_info", K(ret), K(info));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (OB_SYS_TENANT_ID == tenant_id) {
      ret = E(EventTable::EN_BACKUP_AFTER_UPDATE_EXTERNAL_ROUND_INFO_FOR_SYS) OB_SUCCESS;
    } else {
      ret = E(EventTable::EN_BACKUP_AFTER_UPDATE_EXTERNAL_ROUND_INFO_FOR_USER) OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fake failed after update_extern_log_archive_backup_info", K(ret), K(info));
    }
  }
#endif
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_extern_backup_info_path_(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, share::ObBackupPath& path)
{
  int ret = OB_SUCCESS;

  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_backup_info_path(cluster_backup_dest, path))) {
      LOG_WARN("failed to get_cluster_clog_backup_info_path", K(ret), K(cluster_backup_dest));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_info_path(cluster_backup_dest, tenant_id, path))) {
      LOG_WARN("failed to get_tenant_clog_backup_info_path", K(ret), K(cluster_backup_dest), K(tenant_id));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::read_extern_log_archive_backup_info(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, ObExternLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, cluster_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::write_extern_log_archive_backup_info(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const ObExternLogArchiveBackupInfo& info,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  char* buf = nullptr;
  int64_t buf_size = info.get_write_buf_size();
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  DEBUG_SYNC(WRTIE_EXTERN_LOG_ARCHIVE_BACKUP_INFO);
  if (!info.is_valid() || OB_INVALID_TENANT_ID == tenant_id || !cluster_backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info), K(tenant_id), K(cluster_backup_dest));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(info.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(info));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(info));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed tog mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), cluster_backup_dest.get_storage_info(), buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    FLOG_INFO("succeed to write_extern_log_archive_backup_info", K(path), K(info));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_history_info(common::ObISQLClient& sql_client, const uint64_t tenant_id,
    const int64_t archive_round, const int64_t copy_id, const bool for_update,
    ObLogArchiveBackupInfo& archive_backup_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  ObArray<ObLogArchiveBackupInfo> archive_backup_infos;
  archive_backup_info.reset();

  if (tenant_id == OB_INVALID_ID || archive_round <= 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", KR(ret), K(tenant_id), K(archive_round), K(copy_id));
  } else if (!is_backup_backup_ && copy_id > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not backup backup while copy_id greater than 0 is not expected", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s, %s, %s, %s ",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_BACKUP_DEST,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_IS_MARK_DELETED,
                 OB_STR_COMPATIBLE,
                 OB_STR_BACKUP_PIECE_ID,
                 OB_STR_START_PIECE_ID))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (copy_id > 0 && OB_FAIL(sql.append_fmt(" , %s", OB_STR_COPY_ID))) {
    LOG_WARN("failed to apend sql", K(ret), K(copy_id), K(sql));
  } else if (OB_FAIL(sql.append_fmt(" from %s where tenant_id=%lu and log_archive_round=%ld",
                 get_his_table_name_(),
                 tenant_id,
                 archive_round))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql));
  } else if (copy_id > 0 && OB_FAIL(sql.append_fmt(" and copy_id = %ld", copy_id))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", KR(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, archive_backup_infos))) {
    LOG_WARN("failed to get log archive status", KR(ret), K(tenant_id));
  } else if (archive_backup_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("do not exist such history entry", KR(ret), K(tenant_id), K(archive_round), K(copy_id));
  } else if (1 != archive_backup_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive backup infos count is unexpected",
        K(ret),
        K(tenant_id),
        K(archive_round),
        K(copy_id),
        K(archive_backup_infos));
  } else {
    archive_backup_info = archive_backup_infos.at(0);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_history_infos(common::ObISQLClient& sql_client, const uint64_t tenant_id,
    const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s, %s, %s, %s ",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_BACKUP_DEST,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_IS_MARK_DELETED,
                 OB_STR_COMPATIBLE,
                 OB_STR_BACKUP_PIECE_ID,
                 OB_STR_START_PIECE_ID))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" from %s where tenant_id=%lu", get_his_table_name_(), tenant_id))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, infos))) {
    LOG_WARN("failed to get log archive status", K(ret), K(tenant_id));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_all_log_archive_history_infos(
    common::ObISQLClient& sql_client, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s,%s,%s,%s from %s",
          OB_STR_TENANT_ID,
          OB_STR_INCARNATION,
          OB_STR_LOG_ARCHIVE_ROUND,
          OB_STR_BACKUP_DEST,
          OB_STR_MIN_FIRST_TIME,
          OB_STR_MIN_FIRST_TIME,
          OB_STR_MAX_NEXT_TIME,
          OB_STR_MAX_NEXT_TIME,
          OB_STR_IS_MARK_DELETED,
          OB_STR_COMPATIBLE,
          OB_STR_BACKUP_PIECE_ID,
          OB_STR_START_PIECE_ID,
          get_his_table_name_()))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, infos))) {
    LOG_WARN("failed to get log archive status", K(ret));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_same_round_log_archive_history_infos(common::ObISQLClient& sql_client,
    const int64_t round, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(round));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s, %s, %s, %s ",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_BACKUP_DEST,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_IS_MARK_DELETED,
                 OB_STR_COMPATIBLE,
                 OB_STR_BACKUP_PIECE_ID,
                 OB_STR_START_PIECE_ID))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(" from %s where log_archive_round =%ld", get_his_table_name_(), round))) {
    LOG_WARN("failed to append sql", KR(ret), K(sql));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, infos))) {
    LOG_WARN("failed to get log archive status", K(ret), K(round));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::inner_get_log_archvie_history_infos(
    sqlclient::ObMySQLResult& result, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObLogArchiveBackupInfo info;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(parse_log_archvie_history_status_(result, info))) {
      LOG_WARN("failed to parse log archive status", K(ret));
    } else {
      // set log archive history info status stop
      info.status_.status_ = ObLogArchiveStatus::STOP;
      if (OB_FAIL(infos.push_back(info))) {
        LOG_WARN("failed to push info into array", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_log_archvie_history_status_(
    common::sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  char backup_dest[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  int64_t tmp_real_str_len = 0;
  int64_t tmp_compatible = 0;
  ObTenantLogArchiveStatus& status = info.status_;
  UNUSED(tmp_real_str_len);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, status.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, status.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LOG_ARCHIVE_ROUND, status.round_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MIN_FIRST_TIME, status.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_NEXT_TIME, status.checkpoint_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, "backup_dest", backup_dest, OB_MAX_BACKUP_DEST_LENGTH, tmp_real_str_len);

  if (OB_SUCC(ret)) {
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, tmp_compatible, int64_t);
    status.compatible_ = static_cast<ObTenantLogArchiveStatus::COMPATIBLE>(tmp_compatible);
    if (OB_UNLIKELY(!ObTenantLogArchiveStatus::is_compatible_valid(status.compatible_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid compatible version", K(ret), K(tmp_compatible));
    }
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BACKUP_PIECE_ID, status.backup_piece_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_PIECE_ID, status.start_piece_id_, int64_t);
  }
  UNUSED(tmp_real_str_len);
  if (OB_SUCC(ret)) {
    EXTRACT_BOOL_FIELD_MYSQL(result, OB_STR_IS_MARK_DELETED, status.is_mark_deleted_);
    if (OB_ERR_NULL_VALUE == ret) {
      LOG_DEBUG("is mark deleted is null", KR(ret));
      status.is_mark_deleted_ = false;
      // override return code if OB_ERR_NULL_VALUE on is_mark_deleted field
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t str_len = strlen(backup_dest);
    if (str_len >= OB_MAX_BACKUP_DEST_LENGTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup dest over size", K(ret), K(backup_dest), K(str_len));
    } else {
      MEMCPY(info.backup_dest_, backup_dest, strlen(backup_dest));
      info.backup_dest_[str_len] = '\0';
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::inner_read_extern_log_archive_backup_info(
    const ObBackupPath& path, const char* storage_info, ObExternLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need_retry*/);
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read extern log archive backup info get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), storage_info, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(storage_info));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("extern log archive backup info not exist", K(ret), K(path), K(storage_info));
    }
  } else if (0 == file_length) {
    FLOG_INFO("extern log archive backup info is empty", K(ret), K(storage_info), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(util.read_single_file(path.get_ptr(), storage_info, buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(info.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    FLOG_INFO("succeed to read_extern_log_archive_backup_info", K(path), K(info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::mark_extern_log_archive_backup_info_deleted(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id,
    const common::ObIArray<int64_t>& round_ids, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternLogArchiveBackupInfo info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark extern log archive backup info delete get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(tenant_id));
  } else if (round_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, cluster_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(info.mark_log_archive_deleted(round_ids))) {
    LOG_WARN("failed to mark log archive deleted", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(
                 write_extern_log_archive_backup_info(cluster_backup_dest, tenant_id, info, backup_lease_service))) {
    LOG_WARN("failed to write extern log archive backup info", K(ret), K(cluster_backup_dest), K(tenant_id));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_marked_extern_log_archive_backup_info(
    const ObClusterBackupDest& current_backup_dest, const uint64_t tenant_id,
    const common::ObIArray<int64_t>& round_ids, bool& is_empty, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternLogArchiveBackupInfo info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  is_empty = false;

  if (!current_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete marked extern log archive backup info get invalid argument",
        K(ret),
        K(current_backup_dest),
        K(tenant_id),
        K(current_backup_dest));
  } else if (OB_FAIL(get_extern_backup_info_path_(current_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(current_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, current_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(current_backup_dest));
  } else if (OB_FAIL(info.delete_marked_log_archive_info(round_ids))) {
    LOG_WARN("failed to mark log archive deleted", K(ret), K(path), K(current_backup_dest));
  } else if (OB_FAIL(
                 write_extern_log_archive_backup_info(current_backup_dest, tenant_id, info, backup_lease_service))) {
    LOG_WARN("failed to write extern log archive backup info", K(ret), K(current_backup_dest), K(tenant_id));
  } else {
    is_empty = info.is_empty();
  }
  return ret;
}

// for backup backup
int ObLogArchiveBackupInfoMgr::get_all_backup_backup_log_archive_status(
    common::ObISQLClient& sql_client, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  if (!is_backup_backup_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can only deal with copy");
  } else if (OB_FAIL(sql.assign_fmt("select %s, %s, %s, %s, time_to_usec(%s) %s, time_to_usec(%s) %s, status from %s",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_COPY_ID,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME))) {
    LOG_WARN("failed to init log_archive_status_sql", K(ret));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      ObLogArchiveBackupInfo info;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(parse_backup_backup_log_archive_status_(*result, info))) {
        LOG_WARN("failed to parse log archive status", K(ret));
      } else if (OB_FAIL(infos.push_back(info))) {
        LOG_WARN("failed to push info into array", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_backup_log_archive_round_list(common::ObISQLClient& sql_client,
    const int64_t log_archive_round, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (!is_backup_backup_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can only deal with copy");
  } else if (log_archive_round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(log_archive_round));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select %s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s from %s where log_archive_round =%ld",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 get_cur_table_name_(),
                 log_archive_round))) {
    LOG_WARN("failed to init log_archive_status_sql", K(ret));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      ObLogArchiveBackupInfo info;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(parse_backup_backup_log_archive_status_(*result, info))) {
        LOG_WARN("failed to parse log archive status", K(ret));
      } else {
        // set log archive history info status stop
        info.status_.status_ = ObLogArchiveStatus::STOP;
        if (OB_FAIL(infos.push_back(info))) {
          LOG_WARN("failed to push info into array", K(ret), K(info));
        }
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::mark_log_archive_history_info_deleted(
    const ObLogArchiveBackupInfo& info, common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_INCARNATION, info.status_.incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_LOG_ARCHIVE_ROUND, info.status_.round_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (is_backup_backup_ && OB_FAIL(dml_splicer.add_pk_column(OB_STR_COPY_ID, info.status_.copy_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column("is_mark_deleted", true))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_update_sql(get_his_table_name_(), sql))) {
    LOG_WARN("failed to splice replace sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark log archive history info deleted affected rows are unexpected", K(ret), K(sql), K(affected_rows));
  } else {
    LOG_INFO("succeed to update_log_archive_status_history", K(affected_rows), K(sql), K(info));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_log_archive_info(
    const ObLogArchiveBackupInfo& info, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObSqlString sql;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete marked log archive status get invalid argument", K(ret), K(info));
  } else if (!info.status_.is_mark_deleted_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info delete status is unexpected", K(ret), K(info));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND log_archive_round = %ld",
                 get_his_table_name_(),
                 info.status_.tenant_id_,
                 info.status_.round_))) {
    LOG_WARN("failed to assign sql", K(ret), K(info));
  } else if (is_backup_backup_ && OB_FAIL(sql.append_fmt(" and copy_id = %ld", info.status_.copy_id_))) {
    LOG_WARN("failed to apend copy id", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_all_backup_backup_log_archive_info(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObSqlString sql;
  if (!is_backup_backup_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support in backup mode", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s", get_cur_table_name_()))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_backup_backup_log_archive_info(
    const uint64_t tenant_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObSqlString sql;
  if (!is_backup_backup_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support in backup mode", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu", get_cur_table_name_(), tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_all_same_round_in_progress_backup_info(
    common::ObISQLClient& sql_client, const int64_t round, common::ObIArray<share::ObLogArchiveBackupInfo>& info_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  ObDMLSqlSplicer dml;
  sqlclient::ObMySQLResult* result = NULL;

  if (round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(round));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select %s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s, %s, %s, %s, %s, %s from %s "
                 " where log_archive_round =%ld",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_BACKUP_DEST,
                 OB_STR_IS_MOUNT_FILE_CREATED,
                 OB_STR_COMPATIBLE,
                 OB_STR_BACKUP_PIECE_ID,
                 OB_STR_START_PIECE_ID,
                 OB_STR_STATUS,
                 get_cur_table_name_(),
                 round))) {
    LOG_WARN("failed to init log_archive_status_sql", K(ret));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      ObLogArchiveBackupInfo info;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(parse_log_archive_status_(*result, info))) {
        LOG_WARN("failed to parse log archive status", K(ret));
      } else if (OB_FAIL(info_list.push_back(info))) {
        LOG_WARN("failed to push info into array", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_all_same_round_log_archive_infos(common::ObISQLClient& sql_client,
    const uint64_t tenant_id, const int64_t round, common::ObIArray<share::ObLogArchiveBackupInfo>& info_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  ObDMLSqlSplicer dml;
  sqlclient::ObMySQLResult* result = NULL;

  if (OB_INVALID_ID == tenant_id || round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tenant_id), K(round));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s from %s "
                                    " where tenant_id = %ld and log_archive_round =%ld",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_COPY_ID,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_BACKUP_DEST,
                 get_his_table_name_(),
                 tenant_id,
                 round))) {
    LOG_WARN("failed to init log_archive_status_sql", K(ret));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      ObLogArchiveBackupInfo info;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else if (OB_FAIL(parse_his_backup_backup_log_archive_status_(*result, info))) {
        LOG_WARN("failed to parse log archive status", K(ret));
      } else if (OB_FAIL(info_list.push_back(info))) {
        LOG_WARN("failed to push info into array", K(ret), K(info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece_list(common::ObISQLClient& sql_client, const uint64_t tenant_id,
    const int64_t copy_id, common::ObIArray<share::ObBackupPieceInfo>& info_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  ObSqlString sql;
  if (copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu AND copy_id = %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 OB_START_INCARNATION,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(copy_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::check_has_round_mode_archive_in_dest(common::ObISQLClient& sql_client,
    const share::ObBackupDest& backup_dest, const uint64_t tenant_id, bool& has_round_mode)
{
  int ret = OB_SUCCESS;
  has_round_mode = false;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu AND backup_piece_id = 0"
                                " AND backup_dest = '%s' LIMIT 1",
                     OB_ALL_BACKUP_PIECE_FILES_TNAME,
                     OB_START_INCARNATION,
                     tenant_id,
                     backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_dest_str));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (!info_list.empty()) {
    has_round_mode = true;
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::check_has_piece_mode_archive_in_dest(common::ObISQLClient& sql_client,
    const share::ObBackupDest& backup_dest, const uint64_t tenant_id, bool& has_piece_mode)
{
  int ret = OB_SUCCESS;
  has_piece_mode = false;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(backup_dest));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu AND backup_piece_id <> 0"
                                " AND backup_dest = '%s' LIMIT 1",
                     OB_ALL_BACKUP_PIECE_FILES_TNAME,
                     OB_START_INCARNATION,
                     tenant_id,
                     backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_dest_str));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (!info_list.empty()) {
    has_piece_mode = true;
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece_tenant_list(common::ObISQLClient& sql_client,
    const int64_t backup_piece_id, const int64_t copy_id, common::ObIArray<share::ObBackupPieceInfo>& info_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  ObSqlString sql;
  if (backup_piece_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(backup_piece_id), K(copy_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE incarnation=%ld and backup_piece_id = %ld AND copy_id = %ld",
                     OB_ALL_BACKUP_PIECE_FILES_TNAME,
                     OB_START_INCARNATION,
                     backup_piece_id,
                     copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(backup_piece_id), K(copy_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece_copy_list(const int64_t incarnation, const uint64_t tenant_id,
    const int64_t round_id, const int64_t piece_id, const ObBackupBackupCopyIdLevel copy_id_level,
    common::ObISQLClient& sql_client, common::ObIArray<share::ObBackupPieceInfo>& info_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t left_copy_id = 0;
  int64_t right_copy_id = 0;
  if (copy_id_level < OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST || copy_id_level >= OB_BB_COPY_ID_LEVEL_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy id level unexpected", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || round_id < 0 || piece_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(share::get_backup_copy_id_range_by_copy_level(copy_id_level, left_copy_id, right_copy_id))) {
    LOG_WARN("failed to get backup copy id range by copy level", KR(ret), K(copy_id_level));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    " AND round_id = %ld AND backup_piece_id = %ld AND file_status <> 'BROKEN'"
                                    " AND copy_id >= %ld AND copy_id < %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 round_id,
                 piece_id,
                 left_copy_id,
                 right_copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(incarnation), K(tenant_id), K(round_id), K(piece_id), K(copy_id_level));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_all_cluster_level_backup_piece_copy_count(const int64_t incarnation,
    const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, common::ObISQLClient& sql_client,
    int64_t& copy_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  copy_count = 0;
  sqlclient::ObMySQLResult* result = NULL;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(
            sql.assign_fmt("select count(*) as count from %s"
                           " where incarnation = %ld and tenant_id = %lu and round_id = %ld and backup_piece_id = %ld"
                           " AND file_status <> 'INCOMPLETE' AND file_status <> 'BROKEN'"
                           " AND copy_id < %ld",
                OB_ALL_BACKUP_PIECE_FILES_TNAME,
                incarnation,
                tenant_id,
                round_id,
                piece_id,
                OB_TENANT_GCONF_DEST_START_COPY_ID))) {
      LOG_WARN("failed to init count sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", copy_count, int64_t);
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_max_backup_piece(common::ObISQLClient& sql_client, const int64_t incarnation,
    const uint64_t tenant_id, const int64_t copy_id, share::ObBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  if (OB_INVALID_ID == tenant_id || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    "AND copy_id = %ld ORDER BY backup_piece_id DESC LIMIT 1",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("info list not exist", KR(ret));
  } else if (info_list.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", K(ret), K(info_list));
  } else {
    piece = info_list.at(0);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_min_available_backup_piece_id_in_backup_dest(const share::ObBackupDest& backup_dest,
    const int64_t incarnation, const uint64_t tenant_id, const int64_t copy_id, common::ObISQLClient& sql_client,
    int64_t& piece_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    " AND copy_id = %ld AND file_status = 'AVAILABLE' and backup_dest = '%s'"
                                    " ORDER BY backup_piece_id LIMIT 1",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 copy_id,
                 backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (info_list.empty()) {
    piece_id = 0;
    LOG_WARN("info list not exist", KR(ret));
  } else if (info_list.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", K(ret), K(info_list));
  } else {
    piece_id = info_list.at(0).key_.backup_piece_id_;
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::check_has_incomplete_file_info_smaller_than_backup_piece_id(const int64_t incarnation,
    const uint64_t tenant_id, const int64_t backup_piece_id, const share::ObBackupDest& backup_dest,
    common::ObISQLClient& sql_client, bool& has_incomplete)
{
  int ret = OB_SUCCESS;
  has_incomplete = false;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (OB_INVALID_ID == tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(incarnation));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
  } else if (OB_FAIL(
                 sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu AND backup_piece_id < %ld"
                                " AND file_status = 'INCOMPLETE' and backup_dest = '%s' LIMIT 1",
                     OB_ALL_BACKUP_PIECE_FILES_TNAME,
                     incarnation,
                     tenant_id,
                     backup_piece_id,
                     backup_dest_str))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(backup_piece_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (info_list.empty()) {
    has_incomplete = false;
  } else {
    has_incomplete = true;
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_max_frozen_backup_piece(common::ObISQLClient& sql_client, const int64_t incarnation,
    const uint64_t tenant_id, const int64_t copy_id, share::ObBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  if (OB_INVALID_ID == tenant_id || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    "AND copy_id = %ld AND status = 'FROZEN' ORDER BY backup_piece_id DESC LIMIT 1",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 copy_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(copy_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("info list not exist", KR(ret));
  } else if (info_list.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", K(ret), K(info_list));
  } else {
    piece = info_list.at(0);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_last_piece_in_round(common::ObISQLClient& sql_client, const int64_t incarnation,
    const uint64_t tenant_id, const int64_t round_id, share::ObBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObBackupPieceInfo> info_list;
  if (OB_INVALID_ID == tenant_id || round_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(round_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    "AND round_id = %ld ORDER BY backup_piece_id DESC LIMIT 1",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 round_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(incarnation), K(tenant_id), K(round_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, info_list))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("info list not exist", KR(ret));
  } else {
    piece = info_list.at(0);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_external_backup_piece_info(const share::ObBackupPath& path,
    const common::ObString& storage_info, share::ObExternalBackupPieceInfo& info,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupFileSpinLock lock;
  if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(path));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", KR(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", KR(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(path, storage_info.ptr(), info, lease_service))) {
    LOG_WARN("failed to read external backup piece info", KR(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::sync_backup_backup_piece_info(const uint64_t tenant_id,
    const ObClusterBackupDest& src_backup_dest, const ObClusterBackupDest& dst_backup_dest,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  ObBackupPath src_path, dst_path;
  ObStorageUtil util(false /*need_retry*/);
  int64_t file_length = 0;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  if (!src_backup_dest.is_valid() || !dst_backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(src_backup_dest), K(dst_backup_dest));
  } else if (OB_FAIL(
                 get_external_backup_piece_path_(src_backup_dest, tenant_id, true /*is_backup_backup*/, src_path))) {
    LOG_WARN("failed to get external backup piece path", K(ret), K(src_backup_dest), K(tenant_id));
  } else if (OB_FAIL(
                 get_external_backup_piece_path_(dst_backup_dest, tenant_id, true /*is_backup_backup*/, dst_path))) {
    LOG_WARN("failed to get external backup piece path", K(ret), K(dst_backup_dest), K(tenant_id));
  } else if (OB_FAIL(lease_service.check_lease())) {
    LOG_WARN("failed to check lease", K(ret));
  } else if (OB_FAIL(util.get_file_length(src_path.get_ptr(), src_backup_dest.get_storage_info(), file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(src_path));
  } else if (0 == file_length) {
    FLOG_INFO("external backup piece file is empty", K(ret), K(src_path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(src_path));
  } else if (OB_FAIL(util.read_single_file(
                 src_path.get_ptr(), src_backup_dest.get_storage_info(), buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(src_path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(src_path));
  } else if (OB_FAIL(util.write_single_file(dst_path.get_ptr(), dst_backup_dest.get_storage_info(), buf, read_size))) {
    LOG_WARN("failed to write single file", K(ret), K(dst_path));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_backup_backup_log_archive_status_(
    sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int real_length = 0;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  ObTenantLogArchiveStatus& status = info.status_;
  UNUSED(real_length);  // only used for EXTRACT_STRBUF_FIELD_MYSQL
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, status.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, status.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LOG_ARCHIVE_ROUND, status.round_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COPY_ID, status.copy_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MIN_FIRST_TIME, status.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_NEXT_TIME, status.checkpoint_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    ObLogArchiveStatus::STATUS tmp_status = ObLogArchiveStatus::get_status(value_str);

    // for 2.2.40 upgrade cluster
    if (strlen(value_str) == 0) {
      tmp_status = ObLogArchiveStatus::STOP;
    }
    if (!ObLogArchiveStatus::is_valid(tmp_status)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid log archive status", K(ret), K(tmp_status), K(value_str));
    } else {
      status.status_ = tmp_status;
      LOG_INFO("get status", K(tmp_status), K(value_str));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_his_backup_backup_log_archive_status_(
    sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int real_length = 0;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  ObTenantLogArchiveStatus& status = info.status_;

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, status.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, status.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LOG_ARCHIVE_ROUND, status.round_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COPY_ID, status.copy_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MIN_FIRST_TIME, status.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_NEXT_TIME, status.checkpoint_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(info.backup_dest_, sizeof(info.backup_dest_), "%s", value_str))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(value_str));
    }
  }
  UNUSED(real_length);
  return ret;
}

const char* ObLogArchiveBackupInfoMgr::get_cur_table_name_() const
{
  const char* name = NULL;
  if (is_backup_backup_) {
    name = OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME;
  } else {
    name = OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_V2_TNAME;
  }
  return name;
}

const char* ObLogArchiveBackupInfoMgr::get_his_table_name_() const
{
  const char* name = NULL;
  if (is_backup_backup_) {
    name = OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME;
  } else {
    name = OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME;
  }
  return name;
}

uint64_t ObLogArchiveBackupInfoMgr::get_real_tenant_id(const uint64_t normal_tenant_id) const
{
  uint64_t real_tenant_id = 0;
  if (is_backup_backup_) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else {
    real_tenant_id = normal_tenant_id;
  }
  return real_tenant_id;
}

int ObLogArchiveBackupInfoMgr::get_non_frozen_backup_piece(common::ObISQLClient& sql_client, const bool for_update,
    const ObLogArchiveBackupInfo& info, ObNonFrozenBackupPieceInfo& non_frozen_piece)
{
  int ret = OB_SUCCESS;
  non_frozen_piece.reset();
  share::ObBackupPieceInfoKey cur_key;

  cur_key.incarnation_ = info.status_.incarnation_;
  cur_key.tenant_id_ = info.status_.tenant_id_;
  cur_key.round_id_ = info.status_.round_;
  cur_key.backup_piece_id_ = 0;
  if (info.status_.need_switch_piece()) {
    cur_key.backup_piece_id_ = info.status_.backup_piece_id_;
  }
  cur_key.copy_id_ = info.status_.copy_id_;

  if (ObLogArchiveStatus::STOP == info.status_.status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("log archive is stop, cannot get non frozen piece", K(ret), K(info));
  } else if (OB_FAIL(get_non_frozen_backup_piece_(sql_client, for_update, cur_key, non_frozen_piece))) {
    LOG_WARN("failed to get non frozen backup piece", K(ret), K(info), K(cur_key));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_non_frozen_backup_piece_(common::ObISQLClient& sql_client, const bool for_update,
    const share::ObBackupPieceInfoKey& cur_key, ObNonFrozenBackupPieceInfo& non_frozen_piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  int64_t prev_piece_id = 0;
  non_frozen_piece.reset();

  if (cur_key.backup_piece_id_ > 0) {
    prev_piece_id = cur_key.backup_piece_id_ - 1;
  }

  if (!cur_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_key));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select * from %s where incarnation = %ld and tenant_id = %ld and "
                 " round_id = %ld and backup_piece_id in (%ld, %ld) and copy_id = %ld and status != '%s' "
                 " order by backup_piece_id desc",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 cur_key.incarnation_,
                 cur_key.tenant_id_,
                 cur_key.round_id_,
                 prev_piece_id,
                 cur_key.backup_piece_id_,
                 cur_key.copy_id_,
                 ObBackupPieceStatus::get_str(ObBackupPieceStatus::BACKUP_PIECE_FROZEN)))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(cur_key), K(prev_piece_id));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("failed to get cur piece", K(ret), K(sql));
  } else if (OB_FAIL(parse_backup_piece_(*result, non_frozen_piece.cur_piece_info_))) {
    LOG_WARN("failed to parse cur log archive status", K(ret));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("succeed to get non frozen pieces not freezing", K(sql), K(non_frozen_piece));
    } else {
      LOG_WARN("fail to get prev piece", K(ret), K(sql), K(non_frozen_piece));
    }
  } else if (OB_FAIL(parse_backup_piece_(*result, non_frozen_piece.prev_piece_info_))) {
    LOG_WARN("failed to parse prev log archive status", K(ret));
  } else {
    non_frozen_piece.has_prev_piece_info_ = true;

    if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("succeed to get non frozen pieces during freezing", K(sql), K(non_frozen_piece));
      } else {
        LOG_WARN("fail to get prev piece", K(ret), K(sql), K(non_frozen_piece));
      }
    } else {
      ret = OB_ERR_SYS;
      ObBackupPieceInfo tmp_piece;
      if (OB_SUCCESS != (tmp_ret = parse_backup_piece_(*result, tmp_piece))) {
        LOG_WARN("failed to parse tmp backup piece", K(tmp_ret), K(sql));
      } else {
        LOG_ERROR("get more than two non frozen pieces", K(ret), K(sql), K(non_frozen_piece), K(tmp_piece));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece_list_(
    common::ObISQLClient& proxy, const common::ObSqlString& sql, common::ObIArray<share::ObBackupPieceInfo>& piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_UNLIKELY(!sql.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", KR(ret), K(sql));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObBackupPieceInfo piece;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next row", KR(ret));
          }
        } else if (OB_FAIL(parse_backup_piece_(*result, piece))) {
          LOG_WARN("failed to extract backup backupset job", KR(ret), K(piece));
        } else if (OB_FAIL(piece_list.push_back(piece))) {
          LOG_WARN("failed to push back item", KR(ret), K(piece));
        }
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::parse_backup_piece_(sqlclient::ObMySQLResult& result, ObBackupPieceInfo& info)
{
  int ret = OB_SUCCESS;
  int real_length = 0;
  int64_t tmp_compatible = 0;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  info.reset();

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, info.key_.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, info.key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, info.key_.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BACKUP_PIECE_ID, info.key_.backup_piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COPY_ID, info.key_.copy_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_CREATE_DATE, info.create_date_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_TS, info.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_TS, info.checkpoint_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_TS, info.max_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_START_PIECE_ID, info.start_piece_id_, int64_t);

  // for status
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    info.status_ = ObBackupPieceStatus::get_status(value_str);
    if (!ObBackupPieceStatus::is_valid(info.status_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid status", K(ret), K(info), K(value_str));
    }
  }

  // for file status
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_FILE_STATUS, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    info.file_status_ = ObBackupFileStatus::get_status(value_str);
    if (!ObBackupFileStatus::is_valid(info.file_status_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid status", K(ret), K(info), K(value_str));
    }
  }

  // for backup dest
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(info.backup_dest_.assign(value_str))) {
      LOG_WARN("failed to copy backup dest", K(ret), K(value_str));
    }
  }
  UNUSED(real_length);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, tmp_compatible, int64_t);
  info.compatible_ = static_cast<ObTenantLogArchiveStatus::COMPATIBLE>(tmp_compatible);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!ObTenantLogArchiveStatus::is_compatible_valid(info.compatible_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid compatible version", K(ret), K(tmp_compatible));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_non_frozen_backup_piece(common::ObISQLClient& sql_client, const bool for_update,
    const share::ObBackupPieceInfoKey& cur_key, share::ObNonFrozenBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  piece.reset();

  if (!cur_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_key));
  } else if (OB_FAIL(get_non_frozen_backup_piece_(sql_client, for_update, cur_key, piece))) {
    LOG_WARN("failed to get prev backup piece", K(ret), K(cur_key));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece(common::ObISQLClient& sql_client, const bool for_update,
    const share::ObBackupPieceInfoKey& cur_key, share::ObBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  piece.reset();

  if (!cur_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_key));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where incarnation = %ld and tenant_id = %ld and "
                                    " round_id = %ld and backup_piece_id = %ld  and copy_id = %ld ",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 cur_key.incarnation_,
                 cur_key.tenant_id_,
                 cur_key.round_id_,
                 cur_key.backup_piece_id_,
                 cur_key.copy_id_))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(cur_key));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get cur piece", K(ret), K(sql));
    }
  } else if (OB_FAIL(parse_backup_piece_(*result, piece))) {
    LOG_WARN("failed to parse cur log archive status", K(ret));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get prev piece", K(ret), K(sql), K(piece));
    }
  } else {
    share::ObBackupPieceInfo tmp_piece;
    ret = OB_ERR_UNEXPECTED;  // overwrite ret
    if (OB_SUCCESS != (tmp_ret = parse_backup_piece_(*result, tmp_piece))) {
      LOG_WARN("failed to parse tmp log archive status", K(ret), K(sql), K(piece));
    } else {
      LOG_ERROR("get more than one backup piece, unexpected", K(ret), K(sql), K(cur_key), K(piece), K(tmp_piece));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_backup_piece(
    ObMySQLTransaction& trans, const ObNonFrozenBackupPieceInfo& non_frozen_piece)
{
  int ret = OB_SUCCESS;

  if (!trans.is_started() || !non_frozen_piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "is_trans_started", trans.is_started(), K(non_frozen_piece));
  } else if (OB_FAIL(update_backup_piece(trans, non_frozen_piece.cur_piece_info_))) {
    LOG_WARN("failed to update cur backup piece", K(ret), K(non_frozen_piece));
  } else if (non_frozen_piece.has_prev_piece_info_ &&
             OB_FAIL(update_backup_piece(trans, non_frozen_piece.prev_piece_info_))) {
    LOG_WARN("failed to update freeze backup piece", K(ret), K(non_frozen_piece));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_backup_piece(
    common::ObISQLClient& sql_client, const share::ObBackupPieceInfo& piece)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (!piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(piece));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_INCARNATION, piece.key_.incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, piece.key_.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_ROUND_ID, piece.key_.round_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_BACKUP_PIECE_ID, piece.key_.backup_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_COPY_ID, piece.key_.copy_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_CREATE_DATE, piece.create_date_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_START_TS, piece.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_CHECKPOINT_TS, piece.checkpoint_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_MAX_TS, piece.max_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_COMPATIBLE, piece.compatible_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_START_PIECE_ID, piece.start_piece_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_STATUS, piece.get_status_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_FILE_STATUS, piece.get_file_status_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_DEST, piece.backup_dest_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_PIECE_FILES_TNAME, sql))) {
    LOG_WARN("failed to splice_insert_update_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    FLOG_INFO("succeed to update backup piece", K(ret), K(affected_rows), K(piece), K(sql));
  }

  return ret;
}

// update tenant_clog_piece_info/cluster_clog_piece_info and single piece info
int ObLogArchiveBackupInfoMgr::update_external_backup_piece(
    const share::ObNonFrozenBackupPieceInfo& piece, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const bool is_backup_backup = false;

  if (piece.has_prev_piece_info_) {
    if (OB_FAIL(write_external_single_backup_piece_info_(piece.prev_piece_info_, backup_lease_service))) {
      LOG_WARN("failed to update single prev external backup piece", K(ret), K(piece));
    }
  }

  if (FAILEDx(write_external_single_backup_piece_info_(piece.cur_piece_info_, backup_lease_service))) {
    LOG_WARN("failed to update single external backup piece", K(ret), K(piece));
  } else if (OB_FAIL(update_external_backup_piece_(piece, is_backup_backup, backup_lease_service))) {
    LOG_WARN("failed to update_external_prev_backup_piece", K(ret), K(piece));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (OB_SYS_TENANT_ID == piece.cur_piece_info_.key_.tenant_id_) {
      ret = E(EventTable::EN_BACKUP_AFTER_UPDATE_EXTERNAL_BOTH_PIECE_INFO_FOR_SYS) OB_SUCCESS;
    } else {
      ret = E(EventTable::EN_BACKUP_AFTER_UPDATE_EXTERNAL_BOTH_PIECE_INFO_FOR_USER) OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fake failed after update_external_backup_piece", K(ret), K(piece));
    }
  }
#endif
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_external_backup_piece(
    const ObBackupPieceInfo& piece, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const bool is_backup_backup = false;

  if (OB_FAIL(update_external_backup_piece_(piece, is_backup_backup, backup_lease_service))) {
    LOG_WARN("failed to update_external_backup_piece", K(ret), K(piece));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_external_backup_backup_piece(
    const share::ObClusterBackupDest& cluster_backup_dest, const ObBackupPieceInfo& piece,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const bool is_backup_backup = true;

  if (OB_FAIL(update_external_backup_piece_(cluster_backup_dest, piece, is_backup_backup, backup_lease_service))) {
    LOG_WARN("failed to update_external_backup_backup_piece", K(ret), K(piece));
  } else {
    LOG_INFO("update external backup backup piece", K(cluster_backup_dest), K(piece));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_external_single_backup_piece_info(
    const ObBackupPieceInfo& piece, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_external_single_backup_piece_info_(piece, backup_lease_service))) {
    LOG_WARN("failed to write external single backup piece info", K(ret), K(piece));
  }
  return ret;
}

// for cluster or tenant
int ObLogArchiveBackupInfoMgr::update_external_backup_piece_(
    const ObBackupPieceInfo& piece, bool is_backup_backup, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;

  if (OB_FAIL(cluster_backup_dest.set(piece.backup_dest_.ptr(), piece.key_.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(piece));
  } else if (OB_FAIL(
                 update_external_backup_piece_(cluster_backup_dest, piece, is_backup_backup, backup_lease_service))) {
    LOG_WARN("failed to update external backup piece", K(ret), K(cluster_backup_dest), K(piece), K(is_backup_backup));
  }

  return ret;
}

// for cluster or tenant
int ObLogArchiveBackupInfoMgr::update_external_backup_piece_(
    const ObNonFrozenBackupPieceInfo& piece, bool is_backup_backup, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;

  if (OB_FAIL(
          cluster_backup_dest.set(piece.cur_piece_info_.backup_dest_.ptr(), piece.cur_piece_info_.key_.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(piece));
  } else if (OB_FAIL(
                 update_external_backup_piece_(cluster_backup_dest, piece, is_backup_backup, backup_lease_service))) {
    LOG_WARN("failed to update external backup piece", K(ret), K(cluster_backup_dest), K(piece), K(is_backup_backup));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_external_backup_piece_(const share::ObClusterBackupDest& cluster_backup_dest,
    const ObBackupPieceInfo piece, bool is_backup_backup, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternalBackupPieceInfo external_info;
  const uint64_t tenant_id = piece.key_.tenant_id_;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to inner read external log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(external_info.update(piece))) {
    LOG_WARN("failed to update external info", K(ret), K(piece));
  } else if (OB_FAIL(inner_write_extern_log_archive_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to write_extern_log_archive_backup_info", K(ret), K(external_info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_external_backup_piece_(const share::ObClusterBackupDest& cluster_backup_dest,
    const share::ObNonFrozenBackupPieceInfo& piece, bool is_backup_backup,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternalBackupPieceInfo external_info;
  const uint64_t tenant_id = piece.cur_piece_info_.key_.tenant_id_;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to inner read external log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(external_info.update(piece.cur_piece_info_))) {
    LOG_WARN("failed to update external info", K(ret), K(piece));
  } else if (piece.has_prev_piece_info_ && OB_FAIL(external_info.update(piece.prev_piece_info_))) {
    LOG_WARN("failed to update prev piece info", K(ret), K(piece));
  } else if (OB_FAIL(inner_write_extern_log_archive_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to write_extern_log_archive_backup_info", K(ret), K(external_info));
  }
  return ret;
}
int ObLogArchiveBackupInfoMgr::inner_write_extern_log_archive_backup_piece_info_(const ObBackupPath& path,
    const char* storage_info, const share::ObExternalBackupPieceInfo& info,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  int64_t buf_size = info.get_write_buf_size();
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;

  DEBUG_SYNC(WRTIE_EXTERN_LOG_ARCHIVE_BACKUP_INFO);
  if (!info.is_valid() || OB_ISNULL(storage_info) || path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info), K(storage_info), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(info.write_buf(buf, buf_size, pos))) {
    LOG_WARN("failed to write buf", K(ret), K(info));
  } else if (pos != buf_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("write buf size not match", K(ret), K(pos), K(buf_size), K(info));
  } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to mk parent dir", K(ret), K(path));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(util.write_single_file(path.get_ptr(), storage_info, buf, buf_size))) {
    LOG_WARN("failed to write single file", K(ret), K(path));
  } else {
    FLOG_INFO("succeed to update_external_backup_piece_", K(path), K(info));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::inner_read_external_backup_piece_info_(const ObBackupPath& path,
    const char* storage_info, ObExternalBackupPieceInfo& info, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need_retry*/);

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.get_file_length(path.get_ptr(), storage_info, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      LOG_WARN("failed to get file length", K(ret), K(path), K(storage_info));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("external log archive backup piece info not exist", K(ret), K(path), K(storage_info));
    }
  } else if (0 == file_length) {
    FLOG_INFO("external log archive backup piece info is empty", K(ret), K(storage_info), K(path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(util.read_single_file(path.get_ptr(), storage_info, buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else if (OB_FAIL(info.read_buf(buf, read_size))) {
    LOG_WARN("failed to read info from buf", K(ret), K(path));
  } else {
    FLOG_INFO("succeed to inner_read_external_backup_piece_info_", K(path), K(info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_external_backup_piece_path_(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const bool is_backup_backup, ObBackupPath& path)
{
  int ret = OB_SUCCESS;

  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(
            ObBackupPathUtil::get_cluster_clog_backup_piece_info_path(cluster_backup_dest, is_backup_backup, path))) {
      LOG_WARN("failed to get cluster clog backup piece info path", K(ret), K(cluster_backup_dest));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_info_path(
            cluster_backup_dest, tenant_id, is_backup_backup, path))) {
      LOG_WARN("failed to get tenant clog backup piece info path", K(ret), K(cluster_backup_dest), K(is_backup_backup));
    }
  }
  return ret;
}

// for single piece of tenant
int ObLogArchiveBackupInfoMgr::write_external_single_backup_piece_info_(
    const ObBackupPieceInfo& piece, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObClusterBackupDest cluster_backup_dest;
  int64_t buf_len = piece.get_serialize_size() + sizeof(ObBackupCommonHeader);
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need retry*/);
  int64_t pos = 0;
  char* buf = nullptr;
  ObBackupCommonHeader* common_header = nullptr;

  if (!path.is_empty() || !piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(path), K(piece));
  } else if (OB_FAIL(cluster_backup_dest.set(piece.backup_dest_.ptr(), piece.key_.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(piece));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
  } else {
    common_header = new (buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header->data_type_ = ObBackupFileType::BACKUP_SINGLE_PIECE_INFO;
    common_header->data_version_ = LOG_ARCHIVE_BACKUP_SINGLE_PIECE_FILE_VERSION;

    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(piece.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize info", K(ret), K(piece));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObBackupFileSpinLock lock;
    if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_single_piece_info_path(cluster_backup_dest,
            piece.key_.tenant_id_,
            piece.key_.round_id_,
            piece.key_.backup_piece_id_,
            piece.create_date_,
            path))) {
      LOG_WARN("failed to get_cluster_clog_backup_info_path", K(ret), K(piece));
    } else if (OB_FAIL(lock.init(path))) {
      LOG_WARN("failed to init lock", K(ret), K(path));
    } else if (OB_FAIL(lock.lock())) {
      LOG_WARN("failed to lock backup file", K(ret), K(path));
    } else if (OB_FAIL(util.mk_parent_dir(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
      LOG_WARN("failed to mk parent dir", K(ret), K(path));
    } else if (OB_FAIL(backup_lease_service.check_lease())) {
      LOG_WARN("failed to check can backup", K(ret));
    } else if (OB_FAIL(util.write_single_file(path.get_ptr(), cluster_backup_dest.get_storage_info(), buf, buf_len))) {
      LOG_WARN("failed to write single file", K(ret), K(path));
    } else {
      FLOG_INFO("succeed to write_external_single_backup_piece_info", K(path), K(piece));
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::read_external_single_backup_piece_info(const ObBackupPath& path,
    const ObString& storage_info, ObBackupPieceInfo& piece, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char* buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false /*need_retry*/);
  ObBackupFileSpinLock lock;
  piece.reset();

  if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(path));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(util.get_file_length(path.get_obstr(), storage_info, file_length))) {
    LOG_WARN("Failed to get file length", K(ret), K(path), K(storage_info));
  } else if (0 == file_length || file_length < sizeof(ObBackupCommonHeader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single piece info file is not incomplete", K(ret), K(path), K(storage_info));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(file_length), K(path));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(util.read_single_file(path.get_ptr(), storage_info, buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", K(ret), K(path), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read file length not match", K(ret), K(file_length), K(read_size), K(path));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader* common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > read_size - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(read_size), K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check archive backup single piece", K(ret), K(*common_header));
    } else if (OB_FAIL(piece.deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize piece", K(ret), K(*common_header));
    } else {
      FLOG_INFO("succeed to read_external_single_backup_piece_info", K(path), K(piece));
    }
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_round_backup_piece_infos(common::ObISQLClient& sql_client, const bool for_update,
    const uint64_t tenant_id, const int64_t incarnation, const int64_t log_archive_round, const bool is_backup_backup,
    common::ObIArray<share::ObBackupPieceInfo>& piece_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  piece_infos.reset();

  if (tenant_id == OB_INVALID_ID || incarnation <= 0 || log_archive_round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(incarnation), K(log_archive_round));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where incarnation = %ld and tenant_id = %ld and "
                                    " round_id = %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 log_archive_round))) {
    LOG_WARN(
        "failed to init get non frozen backup piece sql", K(ret), K(tenant_id), K(incarnation), K(log_archive_round));
  } else if (!is_backup_backup) {
    if (OB_FAIL(sql.append_fmt(" and copy_id = 0"))) {
      LOG_WARN("failed to add copy id", K(ret), K(tenant_id), K(log_archive_round));
    }
  } else {
    if (OB_FAIL(sql.append_fmt(" and copy_id > 0"))) {
      LOG_WARN("failed to add copy id", K(ret), K(tenant_id), K(log_archive_round));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    ObBackupPieceInfo piece_info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        piece_info.reset();
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get prev piece", K(ret), K(sql), K(tenant_id), K(log_archive_round));
        }
      } else if (OB_FAIL(parse_backup_piece_(*result, piece_info))) {
        LOG_WARN("failed to parse cur log archive status", K(ret));
      } else if (OB_FAIL(piece_infos.push_back(piece_info))) {
        LOG_WARN("failed to add piece info into array", K(ret), K(piece_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_log_archive_history_infos(common::ObISQLClient& sql_client,
    const uint64_t tenant_id, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s,%s,%s, %s "
                                    "from %s where tenant_id = %lu",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_COPY_ID,
                 OB_STR_BACKUP_DEST,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_IS_MARK_DELETED,
                 OB_STR_COMPATIBLE,
                 OB_STR_BACKUP_PIECE_ID,
                 OB_STR_START_PIECE_ID,
                 OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, infos))) {
    LOG_WARN("failed to get log archive status", K(ret), K(tenant_id));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_original_backup_log_piece_infos(common::ObISQLClient& sql_client,
    const bool for_update, const uint64_t tenant_id, common::ObIArray<share::ObBackupPieceInfo>& piece_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  const int64_t incarnation = OB_START_INCARNATION;
  piece_infos.reset();
  const ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::BACKUP_FILE_DELETED;
  const int64_t MAX_GCONF_BACKUP_COPY_ID = OB_CLUSTER_USER_DEST_START_COPY_ID - 1;

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where incarnation = %ld and tenant_id = %ld"
                                    " and copy_id > 0 and copy_id <= %ld and file_status != '%s'",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 MAX_GCONF_BACKUP_COPY_ID,
                 ObBackupFileStatus::get_str(file_status)))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    ObBackupPieceInfo piece_info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get prev piece", K(ret), K(sql), K(tenant_id));
        }
      } else if (OB_FAIL(parse_backup_piece_(*result, piece_info))) {
        LOG_WARN("failed to parse cur log archive status", K(ret));
      } else if (OB_FAIL(piece_infos.push_back(piece_info))) {
        LOG_WARN("failed to add piece info into array", K(ret), K(piece_info));
      }
    }
  }
  return ret;
}

// NOTE : if piece interval > 0, now log archive info struct like
// log_archive_info (round information)
//    piece 1
//    ...
//    piece n
// And backup backup log use piece as basic cell to backup data.
// Inorder to deal with this condition, we use some piece infos to get round information for compatibility
int ObLogArchiveBackupInfoMgr::get_backup_log_archive_info_from_original_piece_infos(common::ObISQLClient& sql_client,
    const uint64_t tenant_id, const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  ObArray<ObBackupPieceInfo> piece_infos;
  CompareBackupPieceInfo backup_piece_info_cmp(ret);
  ObLogArchiveBackupInfo log_archive_info;
  ObBackupPieceInfo prev_piece_info;

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
  } else if (OB_FAIL(get_original_backup_log_piece_infos(sql_client, for_update, tenant_id, piece_infos))) {
    LOG_WARN("failed to get backup backup piece infos", K(ret), K(tenant_id));
  } else if (piece_infos.empty()) {
    // do nothing
  } else if (FALSE_IT(std::sort(piece_infos.begin(), piece_infos.end(), backup_piece_info_cmp))) {
  } else if (OB_SUCCESS != ret) {
    LOG_WARN("backup piece infos can not sort", K(ret), K(piece_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < piece_infos.count(); ++i) {
      const ObBackupPieceInfo& backup_piece = piece_infos.at(i);
      if (0 == i) {
        if (OB_FAIL(trans_log_archive_info_from_piece_info_(backup_piece, log_archive_info))) {
          LOG_WARN("failed to trans log archive info from piece info", K(ret), K(backup_piece));
        }
      } else if (log_archive_info.status_.copy_id_ == backup_piece.key_.copy_id_ &&
                 log_archive_info.status_.incarnation_ == backup_piece.key_.incarnation_ &&
                 log_archive_info.status_.round_ == backup_piece.key_.round_id_ &&
                 prev_piece_info.max_ts_ == backup_piece.start_ts_) {
        log_archive_info.status_.backup_piece_id_ = backup_piece.key_.backup_piece_id_;
        log_archive_info.status_.checkpoint_ts_ = backup_piece.checkpoint_ts_;
        log_archive_info.status_.start_ts_ = std::min(log_archive_info.status_.start_ts_, backup_piece.start_ts_);
        log_archive_info.status_.status_ = ObBackupFileStatus::BACKUP_FILE_COPYING == backup_piece.file_status_
                                               ? ObLogArchiveStatus::DOING
                                               : ObLogArchiveStatus::STOP;
      } else {
        if (!log_archive_info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("log archive info is invalid", K(ret), K(log_archive_info));
        } else if (OB_FAIL(infos.push_back(log_archive_info))) {
          LOG_WARN("failed to push log archive info into array", K(ret), K(log_archive_info));
        } else if (OB_FAIL(trans_log_archive_info_from_piece_info_(backup_piece, log_archive_info))) {
          LOG_WARN("failed to trans log archive info from piece info", K(ret), K(backup_piece));
        }
      }
      prev_piece_info = backup_piece;
    }

    if (OB_SUCC(ret)) {
      if (!log_archive_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log archive info is invalid", K(ret), K(log_archive_info));
      } else if (OB_FAIL(infos.push_back(log_archive_info))) {
        LOG_WARN("failed to push log archive info into array", K(ret), K(log_archive_info));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_log_archive_info_from_piece_info(common::ObISQLClient& sql_client,
    const uint64_t tenant_id, const int64_t piece_id, const int64_t copy_id, const bool for_update,
    ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();
  ObBackupPieceInfo piece_info;

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID || piece_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active(), K(piece_id), K(copy_id));
  } else if (OB_FAIL(get_backup_piece(sql_client, for_update, tenant_id, piece_id, copy_id, piece_info))) {
    LOG_WARN("failed to get backup piece info", K(ret), K(tenant_id), K(piece_id), K(copy_id));
  } else if (OB_FAIL(trans_log_archive_info_from_piece_info_(piece_info, info))) {
    LOG_WARN("failed to trans log archive info from piece info", K(ret), K(piece_info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::trans_log_archive_info_from_piece_info_(
    const ObBackupPieceInfo& piece_info, ObLogArchiveBackupInfo& log_archive_info)
{
  int ret = OB_SUCCESS;
  log_archive_info.reset();

  if (!piece_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans log archive info from piece info get invalid argument", K(ret), K(piece_info));
  } else {
    log_archive_info.status_.tenant_id_ = piece_info.key_.tenant_id_;
    log_archive_info.status_.backup_piece_id_ = piece_info.key_.backup_piece_id_;
    log_archive_info.status_.checkpoint_ts_ = piece_info.checkpoint_ts_;
    log_archive_info.status_.compatible_ = piece_info.compatible_;
    log_archive_info.status_.copy_id_ = piece_info.key_.copy_id_;
    log_archive_info.status_.incarnation_ = piece_info.key_.incarnation_;
    log_archive_info.status_.is_mark_deleted_ = false;
    log_archive_info.status_.is_mount_file_created_ = false;
    // mock piece switch interval
    log_archive_info.status_.start_piece_id_ = piece_info.start_piece_id_;
    log_archive_info.status_.round_ = piece_info.key_.round_id_;
    log_archive_info.status_.start_ts_ = piece_info.start_ts_;
    log_archive_info.status_.status_ = ObBackupUtils::can_backup_pieces_be_deleted(piece_info.status_)
                                           ? ObLogArchiveStatus::STOP
                                           : ObLogArchiveStatus::DOING;
    MEMCPY(log_archive_info.backup_dest_, piece_info.backup_dest_.ptr(), OB_MAX_BACKUP_DEST_LENGTH);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_piece(common::ObISQLClient& sql_client, const bool for_update,
    const uint64_t tenant_id, const int64_t backup_piece_id, const int64_t copy_id, ObBackupPieceInfo& piece_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  piece_info.reset();

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID || backup_piece_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(backup_piece_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where incarnation = %ld and tenant_id = %ld"
                                    " and backup_piece_id = %ld and copy_id = %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 OB_START_INCARNATION,
                 tenant_id,
                 backup_piece_id,
                 copy_id))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(tenant_id));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else if (OB_FAIL(result->next())) {
    LOG_WARN("failed to get backup piece info", K(ret), K(backup_piece_id), K(copy_id));
  } else if (OB_FAIL(parse_backup_piece_(*result, piece_info))) {
    LOG_WARN("failed to parse cur log archive status", K(ret));
  } else if (OB_ITER_END != result->next()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get backup piece has multi pieces infos", K(ret), K(backup_piece_id), K(copy_id));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_tenant_ids_with_piece_id(common::ObISQLClient& sql_client,
    const int64_t backup_piece_id, const int64_t copy_id, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  tenant_ids.reset();

  if (!sql_client.is_active() || backup_piece_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(backup_piece_id), K(copy_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT(tenant_id) from %s WHERE"
                                    " backup_piece_id = %ld and incarnation=%ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 backup_piece_id,
                 OB_START_INCARNATION))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(backup_piece_id));
  } else if (copy_id > 0 && OB_FAIL(sql.append_fmt(" and copy_id = %ld", copy_id))) {
    LOG_WARN("failed to add copy id", K(ret), K(copy_id));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      int64_t tenant_id = 0;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }

      EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_tenant_ids_with_round_id(common::ObISQLClient& sql_client, const bool for_update,
    const int64_t backup_round_id, const int64_t copy_id, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  tenant_ids.reset();
  const char* table_name = NULL;

  if (!sql_client.is_active() || backup_round_id < 0 || copy_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(backup_round_id), K(copy_id));
  } else if (FALSE_IT(table_name = copy_id > 0 ? OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME
                                               : OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME)) {
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT(tenant_id) from %s WHERE"
                                    " log_archive_round = %ld",
                 table_name,
                 backup_round_id))) {
    LOG_WARN("failed to init get non frozen backup piece sql", K(ret), K(backup_round_id));
  } else if (copy_id > 0 && OB_FAIL(sql.append_fmt(" and copy_id = %ld", copy_id))) {
    LOG_WARN("failed to add copy id", K(ret), K(copy_id));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      int64_t tenant_id = 0;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }

      EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_backup_tenant_ids_with_snapshot(common::ObISQLClient& sql_client,
    const int64_t snapshot_version, const bool is_backup_backup, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  tenant_ids.reset();

  if (!sql_client.is_active() || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(snapshot_version));
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT(tenant_id) from %s WHERE"
                                    " start_ts <= %ld and incarnation = %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 snapshot_version,
                 OB_START_INCARNATION))) {
    LOG_WARN("failed to init sql", K(ret), K(snapshot_version));
  } else if (is_backup_backup && OB_FAIL(sql.append_fmt(" and copy_id > 0"))) {
    LOG_WARN("failed to add copy id", K(ret));
  } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", K(ret), K(sql));
  } else {
    while (OB_SUCC(ret)) {
      int64_t tenant_id = 0;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }

      EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, int64_t);
      if (FAILEDx(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::mark_extern_backup_piece_deleting(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const common::ObIArray<share::ObBackupPieceInfoKey>& piece_keys,
    const bool is_backup_backup, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternalBackupPieceInfo external_info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (piece_keys.empty()) {
    // do nothing
  } else if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to inner read external log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(external_info.mark_deleting(piece_keys))) {
    LOG_WARN("failed to mark deleting piece info", K(ret), K(tenant_id));
  } else if (OB_FAIL(inner_write_extern_log_archive_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to write_extern_log_archive_backup_info", K(ret), K(external_info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::mark_extern_backup_piece_deleted(const ObClusterBackupDest& current_backup_dest,
    const uint64_t tenant_id, const common::ObIArray<share::ObBackupPieceInfoKey>& piece_keys,
    const bool is_backup_backup, bool& is_all_deleted, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternalBackupPieceInfo external_info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  is_all_deleted = false;

  if (OB_FAIL(get_external_backup_piece_path_(current_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(current_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(
                 path, current_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to inner read external log archive backup info", K(ret), K(path), K(current_backup_dest));
  } else if (OB_FAIL(external_info.mark_deleted(piece_keys))) {
    LOG_WARN("failed to mark deleting piece info", K(ret), K(tenant_id));
  } else if (OB_FAIL(inner_write_extern_log_archive_backup_piece_info_(
                 path, current_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to write_extern_log_archive_backup_info", K(ret), K(external_info));
  } else {
    is_all_deleted = external_info.is_all_piece_info_deleted();
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::get_extern_backup_info_path(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, share::ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get extern backup info path", K(ret), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_external_backup_piece_path(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const bool is_backup_backup, share::ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get extern backup info path", K(ret), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_extern_backup_info_file(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  ObExternLogArchiveBackupInfo info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete extern backup info file get invalid argument", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, cluster_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (!info.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log archive info is not empty, cannot delete it", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(util.del_file(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete file", K(ret), K(path), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_extern_backup_info_file_timestamp(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObStorageUtil util(false /*need_retry*/);
  ObBackupFileSpinLock lock;
  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "update extern backup info file timestmap get invalid argument", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(util.update_file_modify_time(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete file", K(ret), K(path), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_extern_backup_piece_file(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const bool is_backup_backup, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  ObExternalBackupPieceInfo external_info;

  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete extern backup piece file get invalid argument", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_external_backup_piece_info_(
                 path, cluster_backup_dest.get_storage_info(), external_info, backup_lease_service))) {
    LOG_WARN("failed to inner read external log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (!external_info.is_all_piece_info_deleted()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log archive info is not empty, cannot delete it", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(util.del_file(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete file", K(ret), K(path), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::update_extern_backup_piece_file_timestamp(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, const bool is_backup_backup)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObStorageUtil util(false /*need_retry*/);
  ObBackupFileSpinLock lock;
  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "update extern backup piece file timestamp get invalid argument", K(ret), K(cluster_backup_dest), K(tenant_id));
  } else if (OB_FAIL(get_external_backup_piece_path_(cluster_backup_dest, tenant_id, is_backup_backup, path))) {
    LOG_WARN("failed to get cluster clog backup piece path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(util.update_file_modify_time(path.get_ptr(), cluster_backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete file", K(ret), K(path), K(cluster_backup_dest));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_tenant_backup_piece_infos(common::ObISQLClient& sql_client,
    const int64_t incarnation, const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id,
    common::ObIArray<share::ObBackupPieceInfo>& piece_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  piece_infos.reset();

  if (OB_INVALID_ID == tenant_id || round_id < 0 || piece_id < 0 || incarnation <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(round_id), K(piece_id), K(incarnation));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu "
                                    " AND round_id = %ld AND backup_piece_id = %ld",
                 OB_ALL_BACKUP_PIECE_FILES_TNAME,
                 incarnation,
                 tenant_id,
                 round_id,
                 piece_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(incarnation), K(tenant_id), K(round_id), K(piece_id));
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, piece_infos))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_max_backup_piece_id_in_backup_dest(
    const share::ObBackupBackupCopyIdLevel copy_id_level, const share::ObBackupDest& backup_dest,
    const uint64_t tenant_id, common::ObISQLClient& sql_client, int64_t& max_piece_id)
{
  int ret = OB_SUCCESS;
  max_piece_id = 0;
  int64_t left_copy_id = 0;
  int64_t right_copy_id = 0;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    if (OB_FAIL(share::get_backup_copy_id_range_by_copy_level(copy_id_level, left_copy_id, right_copy_id))) {
      LOG_WARN("failed to get backup copy id range by copy level", KR(ret), K(copy_id_level));
    } else if (OB_FAIL(backup_dest.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
      LOG_WARN("failed to get backup dest str", KR(ret), K(backup_dest));
    } else if (OB_FAIL(sql.assign_fmt("select max(backup_piece_id) as max_piece_id from %s"
                                      " where incarnation = %ld and tenant_id = %lu and backup_dest = '%s'"
                                      " and copy_id >= %ld and copy_id < %ld and file_status <> 'INCOMPLETE'",
                   OB_ALL_BACKUP_PIECE_FILES_TNAME,
                   OB_START_INCARNATION,
                   tenant_id,
                   backup_dest_str,
                   left_copy_id,
                   right_copy_id))) {
      LOG_WARN("failed to init sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else if (OB_ISNULL(result)) {
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "max_piece_id", max_piece_id, int64_t);
      if (OB_ERR_NULL_VALUE == ret) {
        max_piece_id = 0;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

bool ObLogArchiveBackupInfoMgr::CompareBackupPieceInfo::operator()(
    const share::ObBackupPieceInfo& lhs, const share::ObBackupPieceInfo& rhs)
{
  bool b_ret = false;
  result_ = OB_SUCCESS;
  if (lhs.key_.incarnation_ < rhs.key_.incarnation_) {
    b_ret = true;
  } else if (lhs.key_.incarnation_ > rhs.key_.incarnation_) {
    b_ret = false;
  } else if (lhs.key_.copy_id_ < rhs.key_.copy_id_) {
    b_ret = true;
  } else if (lhs.key_.copy_id_ > rhs.key_.copy_id_) {
    b_ret = false;
  } else if (lhs.key_.round_id_ < rhs.key_.round_id_) {
    b_ret = true;
  } else if (lhs.key_.round_id_ > rhs.key_.round_id_) {
    b_ret = false;
  } else if (lhs.key_.backup_piece_id_ < rhs.key_.backup_piece_id_) {
    b_ret = true;
  } else if (lhs.key_.backup_piece_id_ > rhs.key_.backup_piece_id_) {
    b_ret = false;
  } else {
    b_ret = false;
    result_ = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup piece info can not compare", K(lhs), K(rhs), KP(result_));
  }

  return b_ret;
}

int ObLogArchiveBackupInfoMgr::get_tenant_backup_piece_infos_with_file_status(
    common::ObISQLClient &sql_client,
    const int64_t incarnation,
    const uint64_t tenant_id,
    const ObBackupFileStatus::STATUS &file_status,
    const bool is_backup_backup,
    common::ObIArray<share::ObBackupPieceInfo> &piece_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  piece_infos.reset();

  if (OB_INVALID_ID == tenant_id || incarnation <= 0 || !ObBackupFileStatus::is_valid(file_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(incarnation), K(file_status));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT * FROM %s WHERE incarnation = %ld AND tenant_id = %lu AND file_status = '%s'",
      OB_ALL_BACKUP_PIECE_FILES_TNAME, incarnation, tenant_id, ObBackupFileStatus::get_str(file_status)))) {
    LOG_WARN("failed to assign sql", KR(ret), K(incarnation), K(tenant_id));
  } else if (is_backup_backup) {
    if (OB_FAIL(sql.append_fmt(" AND copy_id > 0"))) {
      LOG_WARN("failed to apend sql", K(ret));
    }
  } else {
    if (OB_FAIL(sql.append_fmt(" AND copy_id = 0"))) {
      LOG_WARN("failed to apend sql", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_backup_piece_list_(sql_client, sql, piece_infos))) {
    LOG_WARN("failed to get backup piece list", KR(ret), K(sql));
  }
  return ret;
}

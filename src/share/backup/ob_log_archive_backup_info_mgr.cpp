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
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "ob_backup_path.h"
#include "ob_backup_file_lock_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/backup/ob_backup_io_adapter.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace share::schema;

OB_SERIALIZE_MEMBER(ObExternLogArchiveBackupInfo, status_array_);
ObExternLogArchiveBackupInfo::ObExternLogArchiveBackupInfo()
  : status_array_()
{
}

ObExternLogArchiveBackupInfo::~ObExternLogArchiveBackupInfo()
{
}

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

int ObExternLogArchiveBackupInfo::write_buf(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = get_write_buf_size();
  ObBackupCommonHeader *common_header = nullptr;

  if (OB_ISNULL(buf) || buf_len - pos < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos), K(need_size));
  } else {
    common_header = new(buf + pos) ObBackupCommonHeader;
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

int ObExternLogArchiveBackupInfo::read_buf(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader *common_header =
        reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > buf_len - pos) {
      ret = OB_ERR_SYS;
      LOG_ERROR("need more data then buf len", K(ret), KP(buf), K(buf_len),
          K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      LOG_ERROR("failed to check archive backup info", K(ret), K(*common_header));
    } else if (OB_FAIL(this->deserialize(buf, pos + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to deserialize archive backup ifno",
          K(ret), K(*common_header));
    }
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::update(const ObTenantLogArchiveStatus &status)
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
    ObTenantLogArchiveStatus &last = status_array_.at(status_array_.count() - 1);

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

int ObExternLogArchiveBackupInfo::get_last(ObTenantLogArchiveStatus &status)
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
    const int64_t restore_timestamp, ObTenantLogArchiveStatus &status)
{
  int ret = OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST;

  status.reset();

  for (int64_t i = 0; i < status_array_.count(); ++i) {
    const ObTenantLogArchiveStatus &cur_status = status_array_.at(i);
    if (cur_status.start_ts_ <= restore_timestamp
        && restore_timestamp <= cur_status.checkpoint_ts_) {
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

int ObExternLogArchiveBackupInfo::get_log_archive_status(ObIArray<ObTenantLogArchiveStatus> &status_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(status_array.assign(status_array_))) {
    LOG_WARN("failed to get log archive status", K(ret), K(status_array_));
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::mark_log_archive_deleted(
    const ObIArray<int64_t> &round_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < round_ids.count(); ++i) {
    const int64_t round_id = round_ids.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < status_array_.count(); ++j) {
      ObTenantLogArchiveStatus &tenant_archive_status = status_array_.at(j);
      if (tenant_archive_status.round_ != round_id) {
        //do nothing
      } else {
        tenant_archive_status.is_mark_deleted_ = true;
        break;
      }
    }
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::delete_marked_log_archive_info(
    const common::ObIArray<int64_t> &round_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < round_ids.count(); ++i) {
    const int64_t round_id = round_ids.at(i);
    for (int64_t j = status_array_.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      ObTenantLogArchiveStatus &tenant_archive_status = status_array_.at(j);
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

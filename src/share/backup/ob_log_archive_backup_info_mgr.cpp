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
    // LOG_ARCHIVE_BACKUP_INFO_CONTENT_VERSION
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

int ObExternLogArchiveBackupInfo::mark_log_archive_deleted(const int64_t max_clog_delete_snapshot)
{
  int ret = OB_SUCCESS;
  if (max_clog_delete_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark log archive deleted get invalid argument", K(ret), K(max_clog_delete_snapshot));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < status_array_.count(); ++i) {
      ObTenantLogArchiveStatus& tenant_archive_status = status_array_.at(i);
      if (ObLogArchiveStatus::STOP == tenant_archive_status.status_ &&
          max_clog_delete_snapshot > tenant_archive_status.checkpoint_ts_) {
        tenant_archive_status.is_mark_deleted_ = true;
      }
    }
  }
  return ret;
}

int ObExternLogArchiveBackupInfo::delete_marked_log_archive_info()
{
  int ret = OB_SUCCESS;
  for (int64_t i = status_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    ObTenantLogArchiveStatus& tenant_archive_status = status_array_.at(i);
    if (tenant_archive_status.is_mark_deleted_) {
      if (OB_FAIL(status_array_.remove(i))) {
        LOG_WARN("failed to remove tenant archive status", K(ret), K(tenant_archive_status));
      }
    }
  }
  return ret;
}

ObLogArchiveBackupInfoMgr::ObLogArchiveBackupInfoMgr() : deal_with_copy_(false), copy_id_(0)
{}

ObLogArchiveBackupInfoMgr::~ObLogArchiveBackupInfoMgr()
{}

int ObLogArchiveBackupInfoMgr::get_log_archive_backup_info(
    common::ObISQLClient& sql_client, const bool for_update, const uint64_t tenant_id, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
  } else if (for_update && OB_FAIL(get_log_archive_backup_info_with_lock_(sql_client, tenant_id, info))) {
    LOG_WARN("failed to get_log_archive_backup_info_", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_log_archive_status_(sql_client, for_update, tenant_id, info))) {
    LOG_WARN("failed to get_log_archive_status_", K(ret), K(tenant_id));
  } else {
    LOG_INFO("succeed to get_log_archive_backup_info", K(tenant_id), K(info));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archive_checkpoint(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, int64_t& checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
    int real_length = 0;
    checkpoint_ts = 0;

    if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
    } else if (OB_FAIL(
                   sql.assign_fmt("select time_to_usec(%s) %s from %s where tenant_id=%lu and status in ('%s', '%s')",
                       OB_STR_MAX_NEXT_TIME,
                       OB_STR_MAX_NEXT_TIME,
                       OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
                       tenant_id,
                       ObLogArchiveStatus::get_str(ObLogArchiveStatus::BEGINNING),
                       ObLogArchiveStatus::get_str(ObLogArchiveStatus::DOING)))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
      } else {
        checkpoint_ts = 0;
        ret = OB_SUCCESS;
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_MAX_NEXT_TIME, checkpoint_ts, int64_t);

      int tmp_ret = result->next();
      if (OB_ITER_END != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        LOG_WARN("faied to get next", K(ret), K(tmp_ret), K(checkpoint_ts));
      } else {
        LOG_INFO("get_log_archive_checkpoint", K(tenant_id), K(checkpoint_ts));
      }
    }
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::is_doing_log_archive(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, bool& doing)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    doing = false;

    if (!sql_client.is_active() || tenant_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(tenant_id), "sql_client_active", sql_client.is_active());
    } else if (OB_FAIL(
                   sql.assign_fmt("select time_to_usec(%s) %s from %s where tenant_id=%lu and status in ('%s', '%s')",
                       OB_STR_MAX_NEXT_TIME,
                       OB_STR_MAX_NEXT_TIME,
                       OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
                       tenant_id,
                       ObLogArchiveStatus::get_str(ObLogArchiveStatus::BEGINNING),
                       ObLogArchiveStatus::get_str(ObLogArchiveStatus::DOING)))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
      } else {
        doing = 0;
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else {
      int tmp_ret = result->next();
      if (OB_ITER_END != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        LOG_WARN("failed to get next", K(ret), K(tmp_ret), K(doing));
      } else {
        doing = true;
        LOG_INFO("is_doing_log_archive", K(tenant_id), K(doing));
      }
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

int ObLogArchiveBackupInfoMgr::parse_log_archive_status_(sqlclient::ObMySQLResult& result, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  int real_length = 0;
  int64_t tmp_compatible = 0;
  char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
  ObTenantLogArchiveStatus& status = info.status_;

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, status.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, status.incarnation_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LOG_ARCHIVE_ROUND, status.round_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MIN_FIRST_TIME, status.start_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_MAX_NEXT_TIME, status.checkpoint_ts_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_BACKUP_DEST, value_str, sizeof(value_str), real_length);
  EXTRACT_BOOL_FIELD_MYSQL(result, OB_STR_IS_MOUNT_FILE_CREATED, status.is_mount_file_created_);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, tmp_compatible, int64_t);
  status.compatible_ = static_cast<ObTenantLogArchiveStatus::COMPATIBLE>(tmp_compatible);
  if (OB_UNLIKELY(!status.is_compatible_valid(status.compatible_))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid compatible version", K(ret), K(tmp_compatible));
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
    char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};
    char value_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};
    int real_length = 0;

    if (OB_FAIL(sql.assign_fmt(
            "select a.value %s, b.%s,b.%s,b.%s, time_to_usec(b.%s) %s, time_to_usec(b.%s) %s, b.status, b.%s, b.%s"
            " from %s a, %s b where a.tenant_id=%lu and a.name = '%s' and b.tenant_id=a.tenant_id",
            OB_STR_BACKUP_DEST,
            OB_STR_TENANT_ID,
            OB_STR_INCARNATION,
            OB_STR_LOG_ARCHIVE_ROUND,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MIN_FIRST_TIME,
            OB_STR_MAX_NEXT_TIME,
            OB_STR_MAX_NEXT_TIME,
            OB_STR_IS_MOUNT_FILE_CREATED,
            OB_STR_COMPATIBLE,
            OB_ALL_TENANT_BACKUP_INFO_TNAME,
            OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
            tenant_id,
            OB_STR_BACKUP_DEST))) {
      LOG_WARN("failed to init log_archive_status_sql", K(ret));
    } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("failed to add for update", K(ret), K(sql));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(result->next()) && OB_ITER_END != ret) {
      OB_LOG(WARN, "fail to get next result", K(ret), K(sql));
    } else if (OB_UNLIKELY(OB_ITER_END == ret)) {

      ret = OB_ENTRY_NOT_EXIST;
      OB_LOG(WARN, "tenant backup info not exist", K(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret), K(sql));
    } else if (OB_FAIL(parse_log_archive_status_(*result, info))) {
      LOG_WARN("failed to parse_log_archive_status_", K(ret), K(sql));
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to get log archive backup status", K(sql), K(info));
      int tmp_ret = result->next();
      if (OB_ITER_END != tmp_ret) {
        ret = OB_SUCCESS == tmp_ret ? OB_ERR_UNEXPECTED : tmp_ret;
        ObLogArchiveBackupInfo tmp_info = info;
        if (OB_SUCCESS != (tmp_ret = parse_log_archive_status_(*result, tmp_info))) {
          LOG_WARN("failed to parse_log_archive_status_", K(ret), K(tmp_ret));
        } else {
          LOG_WARN("failed to get next", K(ret), K(tmp_ret), K(tmp_info), K(sql));
        }
      }
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

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info(
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

int ObLogArchiveBackupInfoMgr::update_log_archive_status_history(common::ObMySQLProxy& sql_proxy,
    const ObLogArchiveBackupInfo& info, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  ObDMLSqlSplicer dml_splicer;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_INCARNATION, info.status_.incarnation_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_LOG_ARCHIVE_ROUND, info.status_.round_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MIN_FIRST_TIME, info.status_.start_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_raw_time_column(OB_STR_MAX_NEXT_TIME, info.status_.checkpoint_ts_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_BACKUP_DEST, info.backup_dest_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.add_column(OB_STR_COMPATIBLE, info.status_.compatible_))) {
    LOG_WARN("failed to set compatible", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_replace_sql(OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME, sql))) {
    LOG_WARN("failed to splice replace sql", K(ret));
  } else if (OB_FAIL(backup_lease_service.check_lease())) {
    LOG_WARN("failed to check can backup", K(ret));
  } else if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to update_log_archive_status_history", K(affected_rows), K(sql), K(info));
  }

  return ret;
}

int ObLogArchiveBackupInfoMgr::update_log_archive_backup_info_(
    common::ObISQLClient& sql_client, const ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const char* status_str = ObLogArchiveStatus::get_str(info.status_.status_);

  if (OB_FAIL(sql.assign_fmt("update %s set value = '%s' where name= '%s' and tenant_id=%lu",
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt("update %s set value = '%s' where name = '%s' and tenant_id=%lu",
                 OB_ALL_TENANT_BACKUP_INFO_TNAME,
                 info.backup_dest_,
                 OB_STR_BACKUP_DEST,
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

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid info", K(ret), K(info));
  } else if (OB_FAIL(dml_splicer.add_pk_column(OB_STR_TENANT_ID, info.status_.tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret));
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
  } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret));
  } else if (OB_FAIL(sql_client.write(info.status_.tenant_id_, sql.ptr(), affected_rows))) {
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
    const int64_t archive_round, const bool for_update, ObLogArchiveBackupInfo& info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;
  ObArray<ObLogArchiveBackupInfo> info_list;

  if (tenant_id == OB_INVALID_ID || archive_round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", KR(ret), K(tenant_id), K(archive_round));
  } else if (OB_FAIL(sql.assign_fmt("select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s from %s "
                                    " where tenant_id=%lu and log_archive_round=%ld",
                 OB_STR_TENANT_ID,
                 OB_STR_INCARNATION,
                 OB_STR_LOG_ARCHIVE_ROUND,
                 OB_STR_BACKUP_DEST,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MIN_FIRST_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_MAX_NEXT_TIME,
                 OB_STR_IS_MARK_DELETED,
                 get_his_table_name_(),
                 tenant_id,
                 archive_round))) {
    LOG_WARN("failed to init log_archive_status_sql", KR(ret));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to add for update", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    OB_LOG(WARN, "fail to execute sql", KR(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "result is NULL", KR(ret), K(sql));
  } else if (OB_FAIL(inner_get_log_archvie_history_infos(*result, info_list))) {
    LOG_WARN("failed to get log archive status", KR(ret), K(tenant_id));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("do not exist such history entry", KR(ret), K(tenant_id), K(archive_round));
  } else if (info_list.size() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not exist more than one entry", KR(ret), K(info_list));
  } else {
    info = info_list.at(0);
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::get_log_archvie_history_infos(common::ObISQLClient& sql_client, const uint64_t tenant_id,
    const bool for_update, common::ObIArray<ObLogArchiveBackupInfo>& infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  sqlclient::ObMySQLResult* result = NULL;

  if (tenant_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select %s,%s,%s,%s, time_to_usec(%s) %s, time_to_usec(%s) %s, %s, %s from %s where tenant_id=%lu",
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
                 get_his_table_name_(),
                 tenant_id))) {
    LOG_WARN("failed to init log_archive_status_sql", K(ret));
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
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_COMPATIBLE, tmp_compatible, int64_t);
  status.compatible_ = static_cast<ObTenantLogArchiveStatus::COMPATIBLE>(tmp_compatible);
  if (OB_UNLIKELY(!status.is_compatible_valid(status.compatible_))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid compatible version", K(ret), K(tmp_compatible));
  }
  UNUSED(tmp_real_str_len);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_IS_MARK_DELETED, status.is_mark_deleted_, bool);
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

int ObLogArchiveBackupInfoMgr::delete_log_archvie_history_infos(common::ObISQLClient& sql_client,
    const uint64_t tenant_id, const int64_t incarnation, const int64_t log_archive_round)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::ReadResult res;
  ObDMLSqlSplicer dml;

  if (OB_INVALID_ID == tenant_id || incarnation < 0 || log_archive_round <= 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "delete log archive status get invalid argument", K(ret), K(tenant_id), K(incarnation), K(log_archive_round));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id)) ||
             OB_FAIL(dml.add_pk_column("incarnation", incarnation)) ||
             OB_FAIL(dml.add_pk_column("log_archive_round", log_archive_round))) {
    LOG_WARN("fail to add column", K(ret), K(tenant_id), K(log_archive_round), K(incarnation));
  } else {
    ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_BACKUP_TASK_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(affected_rows));
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
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id, const int64_t max_delete_clog_snapshot,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternLogArchiveBackupInfo info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;
  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id || max_delete_clog_snapshot < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mark extern log archive backup info delete get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(tenant_id),
        K(max_delete_clog_snapshot));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, cluster_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(info.mark_log_archive_deleted(max_delete_clog_snapshot))) {
    LOG_WARN("failed to mark log archive deleted", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(
                 write_extern_log_archive_backup_info(cluster_backup_dest, tenant_id, info, backup_lease_service))) {
    LOG_WARN("failed to write extern log archive backup info", K(ret), K(cluster_backup_dest), K(tenant_id));
  }
  return ret;
}

int ObLogArchiveBackupInfoMgr::delete_marked_extern_log_archive_backup_info(
    const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  ObExternLogArchiveBackupInfo info;
  ObBackupPath path;
  ObBackupFileSpinLock lock;

  if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete marked extern log archive backup info get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(tenant_id));
  } else if (OB_FAIL(get_extern_backup_info_path_(cluster_backup_dest, tenant_id, path))) {
    LOG_WARN("failed to get cluster clog backup info path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(lock.init(path))) {
    LOG_WARN("failed to init lock", K(ret), K(path));
  } else if (OB_FAIL(lock.lock())) {
    LOG_WARN("failed to lock backup file", K(ret), K(path));
  } else if (OB_FAIL(inner_read_extern_log_archive_backup_info(path, cluster_backup_dest.get_storage_info(), info))) {
    LOG_WARN("failed to inner read extern log archive backup info", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(info.delete_marked_log_archive_info())) {
    LOG_WARN("failed to mark log archive deleted", K(ret), K(path), K(cluster_backup_dest));
  } else if (OB_FAIL(
                 write_extern_log_archive_backup_info(cluster_backup_dest, tenant_id, info, backup_lease_service))) {
    LOG_WARN("failed to write extern log archive backup info", K(ret), K(cluster_backup_dest), K(tenant_id));
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
  } else if (OB_FAIL(dml_splicer.add_column("is_mark_deleted", true))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME, sql))) {
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

int ObLogArchiveBackupInfoMgr::delete_marked_log_archive_history_infos(
    const uint64_t tenant_id, common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  ObSqlString sql;

  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete marked log archive status get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s where tenant_id = %lu AND "
                                    "is_mark_deleted = true",
                 OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME,
                 tenant_id))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql_client.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  }
  return ret;
}

const char* ObLogArchiveBackupInfoMgr::get_cur_table_name_() const
{
  const char* name = NULL;
  if (deal_with_copy_) {
    name = OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TNAME;
  } else {
    name = OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME;
  }
  return name;
}

const char* ObLogArchiveBackupInfoMgr::get_his_table_name_() const
{
  const char* name = NULL;
  if (deal_with_copy_) {
    name = OB_ALL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME;
  } else {
    name = OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME;
  }
  return name;
}

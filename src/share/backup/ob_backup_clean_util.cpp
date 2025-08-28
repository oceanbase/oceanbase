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
#include "share/backup/ob_backup_clean_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/tablelock/ob_lock_utils.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "share/ob_debug_sync.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

using namespace oceanbase;
using namespace share;

int ObBackupCleanFileOp::func(const dirent *entry) 
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath tmp_path;
  total_file_num_++;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, entry is null");
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list entry, d_name is null");
  } else if (OB_FAIL(tmp_path.init(path_.get_ptr()))) {
    LOG_WARN("failed to init tmp_path", K(ret), K(path_)); 
  } else if (OB_FAIL(tmp_path.join(entry->d_name, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join file name", K(ret), K(entry->d_name));
  } else if (OB_FAIL(util.del_file(tmp_path.get_ptr(), storage_info_))) {
    // File does not exist should be considered successful
    if (OB_OBJECT_NOT_EXIST == ret) {
      LOG_INFO("file is not exist", K(ret), K(tmp_path));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to delete file", K(ret), K(tmp_path));
    } 
  } else {
    LOG_INFO("success to delete file", K(ret), K(tmp_path)); 
  }
  return ret;
}

ObBackupPrefixDeleteFileOp::ObBackupPrefixDeleteFileOp()
  : is_inited_(false),
    path_(),
    storage_info_(NULL)
{
  filter_str_[0] = '\0';
}

int ObBackupPrefixDeleteFileOp::init(
    const char *filter_str,
    const int32_t filter_str_len,
    const ObBackupPath& path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(filter_str) || 0 == filter_str_len || path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (filter_str_len > (sizeof(filter_str_) - 1)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the length of dir prefix too long", K(ret), K(filter_str_len));
  } else if (OB_FAIL(databuff_printf(filter_str_, sizeof(filter_str_), "%.*s", filter_str_len, filter_str))) {
    OB_LOG(WARN, "failed to init filter_str", K(ret), K(filter_str), K(filter_str_len));
  } else {
    path_ = path;
    storage_info_ = storage_info;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupPrefixDeleteFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupPath tmp_path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "dir prefix filter not init", K(ret));
  } else if (OB_ISNULL(entry) || OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (STRLEN(entry->d_name) < STRLEN(filter_str_)) {
    // do nothing
  } else if (0 != STRNCMP(entry->d_name, filter_str_, STRLEN(filter_str_))) {
    // do nothing
  } else if (OB_FAIL(tmp_path.init(path_.get_ptr()))) {
    LOG_WARN("failed to init tmp_path", K(ret), K(path_));
  } else if (OB_FAIL(tmp_path.join(entry->d_name, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join file name", K(ret));
  } else if (OB_FAIL(util.del_file(tmp_path.get_ptr(), storage_info_))) {
    // File does not exist should be considered successful
    if (OB_OBJECT_NOT_EXIST == ret) {
      LOG_INFO("file is not exist", K(ret), K(tmp_path));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to delete file", K(ret), K(tmp_path));
    }
  } else {
    LOG_INFO("success to delete file", K(ret), K(tmp_path));
  }


  return ret;
}

int ObBackupCleanUtil::delete_backup_dir_files(
    const ObBackupPath &path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupCleanFileOp del_file_op(path, storage_info, ObBackupCleanFileOp::ObBackUpFile::BACKUP_NORMAL_FILE);
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is empty, or storage info is null", K(ret), K(path), KP(storage_info));
  } else {
    while (need_retry) {
      if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, del_file_op))) {
        LOG_WARN("failed to list files", K(ret), K(path));
      } else if (OB_FAIL(delete_backup_dir_(path, storage_info))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path), KP(storage_info));
      } else {
        break;
      }
      check_need_retry(ret, start_ts, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        ob_usleep(1 * 1000 * 1000); //1s
        if (OB_IO_LIMIT == ret) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { //60s
            LOG_INFO("[BACKUP_CLEAN]backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
          }
        } else {
          LOG_WARN("failed to delete backup dir files, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupCleanUtil::delete_clog_dir_files(
    const ObBackupPath &path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObBackupCleanFileOp del_clog_op(path, storage_info, ObBackupCleanFileOp::ObBackUpFile::BACKUP_CLOG);
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is empty, or storage info is null", K(ret), K(path), KP(storage_info));
  } else {
    while (need_retry) {
      if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, del_clog_op))) {
        LOG_WARN("failed to list files", K(ret), K(path));
      } else if (OB_FAIL(delete_backup_dir_(path, storage_info))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path), KP(storage_info));
      } else {
        break;
      }
      check_need_retry(ret, start_ts, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        ob_usleep(1 * 1000 * 1000); //1s
        if (OB_IO_LIMIT == ret) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { //60s
            LOG_INFO("[BACKUP_CLEAN]backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
          }
        } else {
          LOG_WARN("failed to delete backup clog dir files, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupCleanUtil::delete_backup_dir_(
    const ObBackupPath &path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (OB_FAIL(util.del_dir(path.get_ptr(), storage_info))) {
    if (OB_DIR_NOT_EXIST == ret) {
      LOG_INFO("dir is not exist", K(ret), K(path), KP(storage_info));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to del dir time", K(ret), K(path), KP(storage_info));
    }
  } else {
    LOG_INFO("[BACKUP_CLEAN]success delete backup dir", K(path));
  }
  return ret;
}

int ObBackupCleanUtil::delete_backup_dir(
    const ObBackupPath &path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is empty, or storage info is null", K(ret), K(path), KP(storage_info));
  } else {
    while (need_retry) {
      if (OB_FAIL(delete_backup_dir_(path, storage_info))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      } else {
        break;
      }

      check_need_retry(ret, start_ts, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        ob_usleep(1 * 1000 * 1000); //1s
        if (OB_IO_LIMIT == ret) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { //60s
            LOG_INFO("[BACKUP_CLEAN]backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
          }
        } else {
          LOG_WARN("failed to delete backup dir, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObBackupCleanUtil::lock_policy_table_then_check(
      common::ObMySQLTransaction &trans, const uint64_t tenant_id, bool &policy_exists, bool log_only) {
  int ret = OB_SUCCESS;
  ObSqlString check_sql;
  if (OB_FAIL(oceanbase::transaction::tablelock::ObInnerTableLockUtil::lock_inner_table_in_trans(
      trans,
      gen_meta_tenant_id(tenant_id),
      share::OB_ALL_BACKUP_DELETE_POLICY_TID,
      log_only ? transaction::tablelock::SHARE : transaction::tablelock::SHARE_ROW_EXCLUSIVE,
      /*if log_only is true, this lock is used when setting backup_dest, will not modify this policy table*/
      false))) {
    COMMON_LOG(WARN, "failed to lock inner table", K(ret), K(tenant_id));
  } else {
    if (log_only) {
      DEBUG_SYNC(BACKUP_DELETE_AFTER_LOCK_POLICY_TABLE_LOG_ONLY);
      if (OB_FAIL(check_sql.assign_fmt("SELECT COUNT(*) as cnt FROM %s WHERE %s=%lu AND %s='%s'",
      share::OB_ALL_BACKUP_DELETE_POLICY_TNAME, share::OB_STR_TENANT_ID, tenant_id,
      share::OB_STR_POLICY_NAME, share::OB_STR_BACKUP_CLEAN_POLICY_NAME_LOG_ONLY))) {
        LOG_WARN("failed to generate check sql", K(ret), K(tenant_id));
      }
    } else {
      if (OB_FAIL(check_sql.assign_fmt("SELECT COUNT(*) as cnt FROM %s WHERE %s=%lu",
      share::OB_ALL_BACKUP_DELETE_POLICY_TNAME, share::OB_STR_TENANT_ID, tenant_id))) {
        LOG_WARN("failed to generate check sql", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), check_sql.ptr()))) {
        LOG_WARN("failed to check log only policy", K(ret), K(check_sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(check_sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next row", K(ret), K(check_sql));
      } else {
        int64_t policy_count = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", policy_count, int64_t);
        policy_exists = (policy_count > 0);
        if (policy_count > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(ret), K(policy_count), K(policy_exists), K(tenant_id));
        }
        LOG_INFO("query result", K(policy_count), K(policy_exists), K(tenant_id));
      }
    }
  }
  LOG_INFO("check log only policy exist", K(ret), K(tenant_id), K(policy_exists));
  return ret;
}

int ObBackupCleanUtil::delete_backup_file(
    const ObBackupPath &path,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is empty, or storage info is null", K(ret), K(path), KP(storage_info));
  } else {
    while (need_retry) {
      bool is_exist = true;
      if (OB_FAIL(util.is_exist(path.get_ptr(), storage_info, is_exist))) {
        LOG_WARN("[BACKUP_CLEAN]failed to check path is exist", K(ret), K(path), KP(storage_info), K(is_exist));
      } else if (!is_exist) {
        //do nothing
      } else if (OB_FAIL(util.del_file(path.get_ptr(), storage_info))) {
        // File does not exist should be considered successful
        if (OB_OBJECT_NOT_EXIST == ret) {
          LOG_INFO("file is not exist", K(ret), K(path), KP(storage_info));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[BACKUP_CLEAN]failed to delete file", K(ret), K(path), KP(storage_info));
        }
      } else {
        break;
      }

      check_need_retry(ret, start_ts, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        ob_usleep(1 * 1000 * 1000); //1s
        if (OB_IO_LIMIT == ret) {
          if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { //60s
            LOG_INFO("[BACKUP_CLEAN]backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
          }
        } else {
          LOG_WARN("failed to delete backup file, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }
  return ret;
}


void ObBackupCleanUtil::check_need_retry(
    const int64_t result,
    const int64_t start_ts,
    int64_t &retry_count,
    int64_t &io_limit_retry_count,
    bool &need_retry)
{
  need_retry = true;
  const int64_t MAX_RETRY_DURATION_US = 60 * 1000 * 1000L; //60s
  const int64_t total_execute_time = ObTimeUtil::current_time() - start_ts;

  if (OB_SUCCESS == result || OB_LEASE_NOT_ENOUGH == result) {
    need_retry = false;
  } else if (OB_IO_LIMIT == result) {
    ++io_limit_retry_count;
  } else {
    if (total_execute_time < MAX_RETRY_DURATION_US) {
      ++retry_count;
      need_retry = true;
    } else if (retry_count < OB_MAX_RETRY_TIMES) {
      need_retry = true;
      ++retry_count;
    } else {
      need_retry = false;
    }
  }
}

int ObBackupCleanUtil::string_to_number_(const char *token, char *&p_end, int64_t &value)
{
  return ob_strtoll(token, p_end, value);
}

int ObBackupCleanUtil::string_to_number_(const char *token, char *&p_end, uint64_t &value)
{
  return ob_strtoull(token, p_end, value);
}

int ObBackupCleanUtil::parse_int64_list(const ObString &str, ObIArray<int64_t> &value_list)
{
  return parse_list<int64_t>(str, value_list);
}

int ObBackupCleanUtil::parse_uint64_list(const ObString &str, ObIArray<uint64_t> &value_list)
{
  return parse_list<uint64_t>(str, value_list);
}

int ObBackupCleanUtil::format_int64_list(const ObIArray<int64_t> &value_list, char *buffer,
                                         int64_t buffer_size, int64_t &cur_pos)
{
  return format_list<int64_t>(value_list, buffer, buffer_size, cur_pos);
}

int ObBackupCleanUtil::format_uint64_list(const ObIArray<uint64_t> &value_list, char *buffer,
                                         int64_t buffer_size, int64_t &cur_pos)
{
  return format_list<uint64_t>(value_list, buffer, buffer_size, cur_pos);
}

int ObBackupCleanUtil::parse_time_interval(const char *str, int64_t &val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;

  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    val = ObConfigTimeParser::get(str, is_valid);
    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid time interval str", K(ret), K(str));
    }
  }
  return ret;
}
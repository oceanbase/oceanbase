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

#ifndef OCEANBASE_SHARE_OB_BACKUP_CEALN_UTIL_H_
#define OCEANBASE_SHARE_OB_BACKUP_CEALN_UTIL_H_

#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "common/storage/ob_device_common.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

class ObBackupCleanFileOp : public ObBaseDirEntryOperator
{
public:
  enum ObBackUpFile
  {
    BACKUP_NORMAL_FILE = 1,
    BACKUP_CLOG = 2
  };
  ObBackupCleanFileOp(
      const ObBackupPath& path, 
      const share::ObBackupStorageInfo *storage_info,
      ObBackUpFile file_type)
        : path_(path),
          file_type_(file_type),
          storage_info_(storage_info),
          total_file_num_(0),
          handled_file_num_(0)
  {}
  virtual ~ObBackupCleanFileOp() {}
  int func(const dirent *entry) override;
  int64_t get_total_file_num() {return total_file_num_;}
  int64_t get_handled_file_num() {return handled_file_num_;}

public:
  ObBackupPath path_;
  ObBackUpFile file_type_;
  const share::ObBackupStorageInfo *storage_info_;
  int64_t total_file_num_;
  int64_t handled_file_num_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanFileOp);
};

class ObBackupPrefixDeleteFileOp : public ObBaseDirEntryOperator
{
public:
  ObBackupPrefixDeleteFileOp();
  virtual ~ObBackupPrefixDeleteFileOp() {}
  int func(const dirent *entry) override;
  int init(
      const char *filter_str,
      const int32_t filter_str_len,
      const ObBackupPath& path,
      const share::ObBackupStorageInfo *storage_info);

public:
  bool is_inited_;
  ObBackupPath path_;
  char filter_str_[common::MAX_PATH_SIZE];
  const share::ObBackupStorageInfo *storage_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupPrefixDeleteFileOp);
};

class ObBackupCleanUtil
{
public:
  static int delete_backup_dir_files(
      const ObBackupPath &path,
      const share::ObBackupStorageInfo *storage_info);
  static int delete_clog_dir_files(
      const ObBackupPath &path,
      const share::ObBackupStorageInfo *storage_info);
  static int delete_backup_dir(
      const ObBackupPath &path,
      const share::ObBackupStorageInfo *storage_info);
  static int delete_backup_file(
      const ObBackupPath &path,
      const share::ObBackupStorageInfo *storage_info);
  static int lock_policy_table_then_check(
      common::ObMySQLTransaction &trans, const uint64_t tenant_id, const bool log_only, bool &policy_exists);
  static int parse_int64_list(const ObString &str, ObIArray<int64_t> &value_list);
  static int parse_uint64_list(const ObString &str, ObIArray<uint64_t> &value_list);
  static int format_int64_list(const ObIArray<int64_t> &value_list, char *buffer,
                               int64_t buffer_size, int64_t &cur_pos);
  static int format_uint64_list(const ObIArray<uint64_t> &value_list, char *buffer,
                                int64_t buffer_size, int64_t &cur_pos);
  static int parse_time_interval(const char *str, int64_t &val);

private:
  static int delete_backup_dir_(
      const ObBackupPath &path,
      const share::ObBackupStorageInfo *storage_info);
  static void check_need_retry(
    const int64_t result,
    const int64_t start_ts,
    int64_t &retry_count,
    int64_t &io_limit_retry_count,
    bool &need_retry);


  template<typename T>
  static int parse_list(const ObString &str, ObIArray<T> &value_list);
  template<typename T>
  static int format_list(const ObIArray<T> &value_list, char *buffer, int64_t buffer_size,
                         int64_t &cur_pos);
  static int string_to_number_(const char *token, char *&p_end, int64_t &value);
  static int string_to_number_(const char *token, char *&p_end, uint64_t &value);
  template<typename T>
  struct NumberFormat;

  template<>
  struct NumberFormat<int64_t> {
    static const char* specifier() { return "%ld"; }
  };

  template<>
  struct NumberFormat<uint64_t> {
    static const char* specifier() { return "%lu"; }
  };

};

template<typename T>
int ObBackupCleanUtil::parse_list(const ObString &str, ObIArray<T> &value_list)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };

  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: input string is empty", K(ret));
  } else if (str.length() >= sizeof(tmp_str)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer is not enough to hold the string", K(ret), "str_len", str.length(), "buf_size", sizeof(tmp_str));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", str.ptr()))) {
    LOG_WARN("failed to copy string for parsing", K(ret), K(str));
  } else {
    char *token = nullptr;
    char *saveptr = nullptr;
    token = ::STRTOK_R(tmp_str, ",", &saveptr);
    while (OB_SUCC(ret) && nullptr != token) {
      T value{};
      char *p_end = nullptr;
      if (OB_FAIL(string_to_number_(token, p_end, value))) {
        LOG_WARN("failed to convert string to number", K(ret), K(token));
      } else if (p_end != nullptr && '\0' == *p_end) {
        if (OB_FAIL(value_list.push_back(value))) {
          LOG_WARN("fail to push back value", K(ret), K(value));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to convert string to number", K(ret), K(token));
      }
      token = STRTOK_R(nullptr, ",", &saveptr);
    }
  }
  return ret;
}

template<typename T>
int ObBackupCleanUtil::format_list(const ObIArray<T> &value_list, char *buffer, int64_t buffer_size, int64_t &cur_pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer", K(ret), KP(buffer), K(buffer_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value_list.count(); ++i) {
      if (i > 0) {
        if (OB_FAIL(databuff_printf(buffer, buffer_size, cur_pos, ","))) {
          LOG_WARN("failed to print comma", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(buffer, buffer_size, cur_pos, NumberFormat<T>::specifier(), value_list.at(i)))) {
          LOG_WARN("failed to print value", K(ret), K(value_list.at(i)));
        }
      }
    }
  }
  return ret;
}

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_OB_BACKUP_CEALN_UTIL_H_ */

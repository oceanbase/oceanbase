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

#include "ob_archive_file_utils.h"
#include "common/storage/ob_device_common.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace archive
{
class ObFileRangeOp : public ObBaseDirEntryOperator
{
public:
  ObFileRangeOp() : min_file_id_(0), max_file_id_(0), file_num_(0) {}
  ~ObFileRangeOp() {}
  int func(const dirent *entry) override;
  void get_file_id(int64_t &min_file_id, int64_t &max_file_id) {min_file_id = min_file_id_; max_file_id = max_file_id_;}
  int get_file_num() {return file_num_;}
private:
  int64_t min_file_id_;
  int64_t max_file_id_;
  int file_num_;
};

int ObFileRangeOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_file_id = 0;
  bool file_match = false;

  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid list entry, entry is null", K(ret), K(entry));
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid list entry, d_name is null", K(ret), K(entry->d_name));
  } else {
    ObString file_name(entry->d_name);
    ARCHIVE_LOG(TRACE, "valid entry ", K(entry->d_name));
    if (OB_FAIL(ObArchiveFileUtils::extract_file_id(file_name, tmp_file_id, file_match))) {
      ARCHIVE_LOG(WARN, "extract_file_id fail", K(ret), K(file_name), K(tmp_file_id));
    } else if (OB_UNLIKELY(! file_match)) {
      // file_name not match, skip
    } else {
      ARCHIVE_LOG(TRACE, "extract_file_id succ", K(file_name), K(tmp_file_id));
      min_file_id_ = (0 == min_file_id_) ? tmp_file_id : std::min(min_file_id_, tmp_file_id);
      max_file_id_ = std::max(max_file_id_, tmp_file_id);
      file_num_++;
    }
  }
  return ret;
}

int ObArchiveFileUtils::get_file_range(const ObString &prefix,
    const share::ObBackupStorageInfo *storage_info,
    int64_t &min_file_id,
    int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  min_file_id = 0;
  max_file_id = 0;
  ObBackupIoAdapter util;
  ObFileRangeOp file_range_op;

  if (OB_FAIL(util.list_files(prefix, storage_info, file_range_op))) {
    ARCHIVE_LOG(WARN, "list_files fail", K(ret), K(prefix));
  } else if (0 == file_range_op.get_file_num()) {
    ret = OB_ENTRY_NOT_EXIST;
    ARCHIVE_LOG(TRACE, "no files", K(prefix));
  } else {
    file_range_op.get_file_id(min_file_id, max_file_id);
  }

  return ret;
}

int ObArchiveFileUtils::read_file(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    char *buf,
    const int64_t file_length,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;

  if (OB_FAIL(util.read_single_file(uri, storage_info, buf, file_length, read_size))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "read_single_file fail", K(ret), K(uri));
    } else {
      // rewrite ret
      ret = OB_ENTRY_NOT_EXIST;
      ARCHIVE_LOG(INFO, "read_single_file fail", K(ret), K(uri));
    }
  }

  return ret;
}

int ObArchiveFileUtils::range_read(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    char *buf,
    const int64_t buf_size,
    const int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (OB_UNLIKELY(NULL == buf || buf_size  < 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(offset), K(uri));
  } else if (OB_FAIL(util.read_part_file(uri, storage_info, buf, buf_size, offset, read_size))) {
    ARCHIVE_LOG(WARN, "read part file failed", K(ret), K(uri), K(buf_size), K(offset));
  }
  return ret;
}

int ObArchiveFileUtils::get_file_length(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    int64_t &file_len)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;

  if (OB_FAIL(util.get_file_length(uri, storage_info, file_len))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "get_file_length fail", K(ret), K(uri), KP(storage_info));
    } else {
      // rewrite ret
      ret = OB_ENTRY_NOT_EXIST;
      ARCHIVE_LOG(WARN, "file not exist", K(ret), K(uri), KP(storage_info));
    }
  }

  return ret;
}

int ObArchiveFileUtils::extract_file_id(const ObString &file_name, int64_t &file_id, bool &match)
{
  int ret = OB_SUCCESS;
  const int64_t length = file_name.length();
  const char *str = file_name.ptr();
  int64_t tmp_file_id = 0;
  match = true;
  file_id = 0;

  if (OB_UNLIKELY(file_name.empty()) || OB_ISNULL(str) || OB_UNLIKELY('\0' == (*str))) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(file_name));
  } else if (OB_UNLIKELY('0' == (*str))) {
    match = false;
  } else {
    for (int64_t i = 0; match && i < length; i++) {
      if (!isdigit(str[i]) && '\0' != str[i]) {
        match = false;
      } else if ('\0' == str[i]) {
        break;
      } else {
        tmp_file_id = tmp_file_id * 10 + (str[i] - '0');
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (! match) {
      ARCHIVE_LOG(WARN, "invalid file_name", K(file_name));
    } else {
      file_id = tmp_file_id;
    }
  }
  return ret;
}
} // namespace archive
} // namespace oceanbase

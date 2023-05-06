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
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_archive_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/rc/ob_tenant_base.h"
#include <algorithm>
#include <cstdint>

namespace oceanbase
{
using namespace share;
namespace archive
{
class ObFileRangeOp : public ObBaseDirEntryOperator
{
public:
  ObFileRangeOp() : min_file_id_(0), max_file_id_(0), file_num_(0) {}
  virtual ~ObFileRangeOp() {}
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

class ObFileListOp : public ObBaseDirEntryOperator
{
public:
  ObFileListOp() : array_() {}
  virtual ~ObFileListOp() {}
  int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<int64_t> &array);
private:
    ArchiveFileIdArray array_;
};

int ObFileListOp::func(const dirent *entry)
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
    } else if (OB_FAIL(array_.push_back(tmp_file_id))) {
      ARCHIVE_LOG(WARN, "push_back failed", K(ret), K(tmp_file_id));
    } else {
      ARCHIVE_LOG(TRACE, "extract_file_id succ", K(file_name), K(tmp_file_id));
    }
  }
  return ret;
}

int ObFileListOp::get_file_list(common::ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  std::sort(array_.begin(), array_.end());
  if (array_.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(array.assign(array_))){
    ARCHIVE_LOG(WARN, "array assign failed", K(ret), K(array_));
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

int ObArchiveFileUtils::list_files(const ObString &prefix,
    const share::ObBackupStorageInfo *storage_info,
    common::ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObFileListOp file_list_op;
  if (OB_FAIL(util.list_files(prefix, storage_info, file_list_op))) {
    ARCHIVE_LOG(WARN, "list_files fail", K(ret), K(prefix));
  } else if (OB_FAIL(file_list_op.get_file_list(array))) {
    ARCHIVE_LOG(WARN, "get_file_list fail", K(ret), K(prefix));
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

int ObArchiveFileUtils::write_file(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_len), K(uri));
  } else if (OB_FAIL(util.write_single_file(uri, storage_info, buf, buf_len))) {
    ARCHIVE_LOG(WARN, "write single file failed", K(ret), K(uri));
  }
  return ret;
}

int ObArchiveFileUtils::is_exist(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    bool &exist)
{
  ObBackupIoAdapter util;
  return util.is_exist(uri, storage_info, exist);
}

int ObArchiveFileUtils::mkdir(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info)
{
  ObBackupIoAdapter util;
  return util.mkdir(uri, storage_info);
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
  int64_t tmp_file_id = 0;
  int32_t len = file_name.length() - strlen(OB_ARCHIVE_SUFFIX);
  char pure_name[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  match = true;
  file_id = 0;

  if (OB_UNLIKELY(file_name.empty())) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(file_name));
  } else if (OB_UNLIKELY(len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "file name without a unified suffix", K(file_name), K(OB_ARCHIVE_SUFFIX));
  } else if (OB_FAIL(databuff_printf(pure_name, sizeof(pure_name), "%.*s", len, file_name.ptr()))) {
    ARCHIVE_LOG(WARN, "databuff_printf failed", K(file_name), K(len));
  } else if (OB_FAIL(ob_atoll(pure_name, tmp_file_id))) {
    ARCHIVE_LOG(WARN, "ignore invalid file name", K(file_name), K(pure_name));
    ret = OB_SUCCESS;
    match = false;
  } else {
    file_id = tmp_file_id;
    match = true;
  }

  return ret;
}

int ObArchiveFileUtils::locate_file_by_scn(share::ObBackupDest &dest,
    const int64_t dest_id,
    const int64_t round,
    const int64_t piece_id,
    const share::ObLSID &ls_id,
    const SCN &scn,
    int64_t &file_id)
{
  int ret = OB_SUCCESS;
  int64_t min_file_id = 0;
  int64_t max_file_id = 0;
  share::ObBackupPath piece_path;
  if (OB_FAIL(share::ObArchivePathUtil::get_piece_ls_log_dir_path(dest, dest_id, round, piece_id, ls_id, piece_path))) {
    ARCHIVE_LOG(WARN, "get piece dir failed", K(ret), K(dest_id), K(round), K(piece_id), K(ls_id), K(dest));
  } else if (OB_FAIL(get_file_range(piece_path.get_obstr(), dest.get_storage_info(), min_file_id, max_file_id))) {
    ARCHIVE_LOG(WARN, "get file range failed", K(ret), K(dest_id), K(round), K(piece_id), K(dest), K(piece_path));
  } else {
    ret = locate_file_by_scn_in_piece(dest, dest_id, round, piece_id, ls_id, min_file_id, max_file_id, scn, file_id);
  }
  return ret;
}

int ObArchiveFileUtils::locate_file_by_scn_in_piece(share::ObBackupDest &dest,
    const int64_t dest_id,
    const int64_t round,
    const int64_t piece_id,
    const share::ObLSID &ls_id,
    const int64_t min_file_id,
    const int64_t max_file_id,
    const SCN &ref_scn,
    int64_t &file_id)
{
  auto get_min_log_info = [&](const int64_t tmp_file_id, SCN &scn, palf::LSN &lsn) -> int
  {
    int ret = OB_SUCCESS;
    share::ObBackupPath path;
    if (OB_FAIL(share::ObArchivePathUtil::get_ls_archive_file_path(dest, dest_id, round, piece_id, ls_id, tmp_file_id, path))) {
      ARCHIVE_LOG(WARN, "get piece dir failed", K(ret), K(dest_id), K(round), K(piece_id), K(dest));
    } else if (OB_FAIL(extract_file_min_log_info_(path.get_obstr(), dest.get_storage_info(), lsn, scn))) {
      ARCHIVE_LOG(WARN, "extract_file_min_log_info_ failed", K(ret));
    }
    return ret;
  };
  return locate_(min_file_id, max_file_id, ref_scn, file_id, get_min_log_info);
}

int ObArchiveFileUtils::extract_file_min_log_info_(const ObString &uri,
    share::ObBackupStorageInfo *storage_info,
    palf::LSN &lsn,
    SCN &scn)
{
  int ret = OB_SUCCESS;
  palf::LogGroupEntryHeader header;
  ObArchiveFileHeader file_header;
  int64_t pos = 0;
  int64_t log_pos = 0;
  int64_t offset = 0;
  int64_t read_size = 0;
  char *data = NULL;
  const int64_t data_len = ARCHIVE_FILE_HEADER_SIZE + header.get_serialize_size();
  if (OB_ISNULL(data = (char*)share::mtl_malloc(data_len, "ArcMinInfo"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc memory failed", K(ret), K(data_len));
  } else if (OB_FAIL(range_read(uri, storage_info, data, data_len, offset, read_size))) {
    ARCHIVE_LOG(WARN, "read file failed", K(ret));
  } else if (OB_FAIL(file_header.deserialize(data, data_len, pos))) {
    ARCHIVE_LOG(WARN, "file header deserialize failed", K(ret), K(file_header));
  } else if (OB_UNLIKELY(!file_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid file header", K(ret), K(file_header));
  } else if (OB_FAIL(header.deserialize(data + ARCHIVE_FILE_HEADER_SIZE,
          data_len - ARCHIVE_FILE_HEADER_SIZE, log_pos))) {
    ARCHIVE_LOG(WARN, "log header deserialize failed", K(ret), K(file_header));
  } else if (OB_UNLIKELY(! header.check_header_integrity())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid log header", K(ret), K(header));
  } else {
    lsn = palf::LSN(file_header.start_lsn_);
    scn = header.get_max_scn();
  }

  if (NULL != data) {
    share::mtl_free(data);
    data = NULL;
  }
  return ret;
}

int ObArchiveFileUtils::locate_(const int64_t min_file_id,
    const int64_t max_file_id,
    const SCN &ref_scn,int64_t &file_id,
    const std::function<int (const int64_t file_id, SCN &scn_param, palf::LSN &lsn)> &get_min_log_info)
{
  int ret = OB_SUCCESS;
  palf::LSN lsn;
  SCN scn;
  bool locate = false;
  int64_t base_file_id = min_file_id;
  int64_t left = min_file_id;
  int64_t right = max_file_id + 1;
  int64_t middle = 0;
  while (!locate && OB_SUCC(ret) && left < right) {
    middle = (left + right) / 2;
    if ((OB_FAIL(get_min_log_info(middle, scn, lsn)))) {
      ARCHIVE_LOG(WARN, "get min log info failed", K(ret), K(middle));
    } else if (scn == ref_scn) {
      locate = true;
      file_id = middle;
    } else if (scn > ref_scn) {
      right = middle;
    } else {
      base_file_id = std::max(base_file_id, middle);
      left = middle + 1;
    }
  }

  if (! locate) {
    file_id = base_file_id;
  }
  return ret;
}

} // namespace archive
} // namespace oceanbase

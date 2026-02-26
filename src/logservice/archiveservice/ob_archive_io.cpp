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

#include "ob_archive_io.h"
#include "share/backup/ob_backup_io_adapter.h"    // ObBackupIoAdapter
#include "share/ob_debug_sync.h"                  // DEBUG_SYNC

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;
namespace oceanbase
{
namespace archive
{
int ObArchiveIO::push_log(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    const int64_t backup_dest_id,
    char *data,
    const int64_t data_len,
    const int64_t offset,
    const bool is_full_file,
    const bool is_can_seal)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  int64_t write_size = -1;

  DEBUG_SYNC(LOG_ARCHIVE_PUSH_LOG);

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_LOG_ARCHIVE_BEFORE_PUSH_LOG_FAILED) OB_SUCCESS;
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == data || data_len < 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(data), K(data_len));
  } else {
    if (is_full_file) {
      if (OB_FAIL(io_adapter.write_single_file(uri, storage_info, data, data_len,
          common::ObStorageIdMod(backup_dest_id, common::ObStorageUsedMod::STORAGE_USED_ARCHIVE)))) {
        ARCHIVE_LOG(WARN, "fail to write file", K(ret), K(uri), KP(storage_info), K(data), K(data_len));
      }
    } else {
      if (OB_FAIL(io_adapter.pwrite(uri, storage_info, data, offset, data_len,
          common::ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER, write_size, is_can_seal,
          common::ObStorageIdMod(backup_dest_id, common::ObStorageUsedMod::STORAGE_USED_ARCHIVE)))) {
        ARCHIVE_LOG(WARN, "fail to write file", K(ret), K(uri), KP(storage_info), K(data), K(data_len));
      }
    }
  }

  if (OB_CLOUD_OBJECT_NOT_APPENDABLE == ret) {
    if (OB_FAIL(check_context_match_in_normal_file_(uri, storage_info,
        common::ObStorageIdMod(backup_dest_id, common::ObStorageUsedMod::STORAGE_USED_ARCHIVE),
        data, data_len, offset))) {
      ARCHIVE_LOG(WARN, "check_context_match_ failed", K(uri));
    }
  }

  return ret;
}

int ObArchiveIO::mkdir(const ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  ObBackupIoAdapter util;
  return util.mkdir(uri, storage_info);
}

int ObArchiveIO::check_context_match_in_normal_file_(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    const common::ObStorageIdMod &storage_id_mod,
    char *data,
    const int64_t data_len,
    const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  char *read_buffer = NULL;
  int64_t read_size = 0;
  ObBackupIoAdapter reader;
  ObArenaAllocator allocator;

  if (OB_FAIL(reader.adaptively_get_file_length(uri, storage_info, length))) {
    ARCHIVE_LOG(WARN, "get file_length failed", K(uri));
  } else if (OB_UNLIKELY(length <= offset)) {
    ret = OB_BACKUP_PWRITE_CONTENT_NOT_MATCH;
    ARCHIVE_LOG(ERROR, "object length smaller than offset", K(uri), K(length), K(offset));
  } else if (OB_UNLIKELY(length < offset + data_len)) {
    ret = OB_BACKUP_PWRITE_CONTENT_NOT_MATCH;
    ARCHIVE_LOG(ERROR, "object length smaller than write buffer total offset", K(uri), K(length), K(offset), K(data_len));
  } else if (OB_ISNULL(read_buffer = static_cast<char*>(allocator.alloc(data_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "allocate memory failed", K(uri), K(data_len));
  } else if (OB_FAIL(reader.adaptively_read_part_file(uri, storage_info, read_buffer, data_len, offset, read_size,
                                                      storage_id_mod))) {
    ARCHIVE_LOG(WARN, "pread failed", K(uri), K(read_buffer), K(data_len), K(offset));
  } else if (read_size < data_len) {
    ret = OB_BACKUP_PWRITE_CONTENT_NOT_MATCH;
    ARCHIVE_LOG(ERROR, "read_size smaller than data_len",  K(uri), K(length), K(offset), K(data_len), K(read_size));
  } else if (0 != MEMCMP(data, read_buffer, data_len)) {
    ret = OB_BACKUP_PWRITE_CONTENT_NOT_MATCH;
    ARCHIVE_LOG(ERROR, "data inconsistent", K(uri), K(length), K(offset), K(data_len), K(read_size));
  }
  return ret;
}
} // namespace archive
} // namespace oceanbase

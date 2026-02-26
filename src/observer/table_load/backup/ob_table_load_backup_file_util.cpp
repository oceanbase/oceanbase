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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/backup/ob_table_load_backup_file_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

int ObTableLoadBackupFileUtil::list_directories(const common::ObString &path,
                                                const share::ObBackupStorageInfo *storage_info,
                                                ObIArray<ObString> &part_list,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObBackupIoAdapter adapter;
  ObFileListArrayOp op(part_list, allocator);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(adapter.list_directories(path, storage_info, op))) {
      LOG_WARN("fail to list directories", K(ret), K(retry_count));
      if (ret == OB_OBJECT_STORAGE_IO_ERROR) {
        retry_count++;
        if (retry_count <= MAX_RETRY_COUNT) {
          ret = OB_SUCCESS;
          ob_usleep(RETRY_INTERVAL);
        }
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObTableLoadBackupFileUtil::get_file_length(const common::ObString &path,
                                               const share::ObBackupStorageInfo *storage_info,
                                               int64_t &file_length)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObBackupIoAdapter adapter;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(adapter.get_file_length(path, storage_info, file_length))) {
      LOG_WARN("fail to list directories", K(ret), K(retry_count));
      if (ret == OB_OBJECT_STORAGE_IO_ERROR) {
        retry_count++;
        if (retry_count <= MAX_RETRY_COUNT) {
          ret = OB_SUCCESS;
          ob_usleep(RETRY_INTERVAL);
        }
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObTableLoadBackupFileUtil::read_single_file(const common::ObString &path,
                                                const share::ObBackupStorageInfo *storage_info,
                                                char *buf,
                                                const int64_t buf_size,
                                                int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObBackupIoAdapter adapter;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(adapter.read_single_file(path, storage_info, buf, buf_size, read_size, ObStorageIdMod::get_default_ddl_id_mod()))) {
      LOG_WARN("fail to list directories", K(ret), K(retry_count));
      if (ret == OB_OBJECT_STORAGE_IO_ERROR) {
        retry_count++;
        if (retry_count <= MAX_RETRY_COUNT) {
          ret = OB_SUCCESS;
          ob_usleep(RETRY_INTERVAL);
        }
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObTableLoadBackupFileUtil::read_part_file(const common::ObString &path,
                                              const share::ObBackupStorageInfo *storage_info,
                                              char *buf,
                                              const int64_t buf_size,
                                              const int64_t offset,
                                              int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  ObBackupIoAdapter adapter;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(adapter.read_part_file(path, storage_info, buf, buf_size, offset, read_size, ObStorageIdMod::get_default_ddl_id_mod()))) {
      LOG_WARN("fail to read part file", K(ret), K(retry_count), K(path));
      if (ret == OB_OSS_ERROR) {
        retry_count++;
        if (retry_count <= MAX_RETRY_COUNT) {
          ret = OB_SUCCESS;
          usleep(RETRY_INTERVAL);
        }
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObTableLoadBackupFileUtil::split_reverse(ObString &str,
                                             const char separator,
                                             ObIArray<ObString> &result,
                                             int64_t limit,
                                             bool ignore_empty)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  while (OB_SUCC(ret) && !str.empty() && count < limit) {
    ObString tmp_str = str.split_on(str.reverse_find('/'));
    if (!str.empty() || !ignore_empty) {
      if (OB_FAIL(result.push_back(str))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        count++;
      }
    }
    str = tmp_str;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

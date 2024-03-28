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
    if (OB_FAIL(adapter.read_single_file(path, storage_info, buf, buf_size, read_size))) {
      LOG_WARN("fail to list directories", K(ret), K(retry_count));
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

} // namespace observer
} // namespace oceanbase

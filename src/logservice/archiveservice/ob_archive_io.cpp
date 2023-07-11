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
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_tracepoint.h"            // EventTable
#include "share/ob_device_manager.h"              // ObIODevice
#include "share/backup/ob_backup_io_adapter.h"    // ObBackupIoAdapter
#include "share/ob_debug_sync.h"                  // DEBUG_SYNC
#include "share/ob_debug_sync_point.h"            // LOG_ARCHIVE_PUSH_LOG

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;
namespace oceanbase
{
namespace archive
{
int ObArchiveIO::push_log(const ObString &uri,
    const share::ObBackupStorageInfo *storage_info,
    char *data,
    const int64_t data_len,
    const int64_t offset,
    const bool is_full_file)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObIOFd fd;
  int64_t write_size = -1;
  ObIODevice*  device_handle = NULL;

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
      if (OB_FAIL(util.open_with_access_type(device_handle, fd, storage_info, uri,
              common::ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER))) {
        ARCHIVE_LOG(INFO, "open_with_access_type failed", K(ret), K(uri), KP(storage_info));
      } else if (OB_ISNULL(device_handle)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "device_handle is NULL", K(ret), K(device_handle), K(uri));
      } else if (OB_FAIL(device_handle->write(fd, data, data_len, write_size))) {
        ARCHIVE_LOG(WARN, "fail to write file", K(ret), K(uri), KP(storage_info), K(data), K(data_len));
      }
    } else {
      if (OB_FAIL(util.open_with_access_type(device_handle, fd, storage_info, uri,
              common::ObStorageAccessType::OB_STORAGE_ACCESS_RANDOMWRITER))) {
        ARCHIVE_LOG(INFO, "open_with_access_type failed", K(ret), K(uri), KP(storage_info));
      } else if (OB_ISNULL(device_handle)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "device_handle is NULL", K(ret), K(device_handle), K(uri));
      } else if (OB_FAIL(device_handle->pwrite(fd, offset, data_len, data, write_size))) {
        ARCHIVE_LOG(WARN, "fail to write file", K(ret), K(uri), KP(storage_info), K(data), K(data_len));
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = util.close_device_and_fd(device_handle, fd))) {
    ARCHIVE_LOG(WARN, "fail to close file and release device!", K(tmp_ret), K(uri), KP(storage_info));
  }
  return ret;
}

int ObArchiveIO::mkdir(const ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  ObBackupIoAdapter util;
  return util.mkdir(uri, storage_info);
}

} // namespace archive
} // namespace oceanbase

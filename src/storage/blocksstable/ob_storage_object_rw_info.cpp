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

#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_rw_info.h"
#include "storage/backup/ob_backup_device_wrapper.h"

namespace oceanbase
{
namespace blocksstable
{

int ObStorageObjectWriteInfo::fill_io_info_for_backup(const blocksstable::MacroBlockId &macro_id, ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  if (!backup::ObBackupDeviceMacroBlockId::is_backup_block_file(macro_id.first_id())) {
    // do nothing
  } else if (!has_backup_device_handle_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "device handle should not be null", K(ret));
  } else {
    backup::ObBackupWrapperIODevice *device = static_cast<backup::ObBackupWrapperIODevice *>(device_handle_);
    io_info.fd_.fd_id_ = device->simulated_fd_id();
    io_info.fd_.slot_version_ = device->simulated_slot_version();
  }
  return ret;
}

}
}
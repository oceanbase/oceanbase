/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_file_system_common.h
/// \brief Storage-side helpers shared by the generic external-table plugin host
/// file system (`ObExtDefaultFileSystem`). It keeps storage resolution inside the
/// generic host and independent of every plugin SDK.
///
/// - `StorageInfoHolder` parses the storage type from the path
///   (`get_storage_type_from_path_for_external_table` -> OSS/S3/HDFS/AZBLOB/FILE)
///   and owns the matching `ObObjectStorageInfo`. This is where device
///   auto-selection is decided: the storage type drives `ObStorageUtil::open`
///   (metadata) and `ObExternalFileAccess`/`ObExternalDataAccessMgr` (read) to
///   pick the right device.
/// - `ExtStorageTenantGuard` scopes a metadata op under the scan's tenant with
///   the object-storage I/O timeout.
///
/// Pure path/ObString helpers (normalize_file_uri, join_path, parent_path,
/// make_ob_stringview, ...) are reused from
/// `share/external_table/ob_external_table_path_util.h` and are NOT redefined here.

#ifndef OB_EXT_FILE_SYSTEM_COMMON_H
#define OB_EXT_FILE_SYSTEM_COMMON_H

#include "lib/restore/ob_storage.h"  // ObStorageUtil, ObStorageType, get_storage_type_from_path_for_external_table
#include "lib/restore/ob_object_storage_base.h"  // ObObjectStorageTenantGuard
#include "share/backup/ob_backup_struct.h"  // ObExternalTableStorageInfo
#include "share/external_table/ob_hdfs_storage_info.h"  // ObHDFSStorageInfo
#include "share/external_table/ob_external_table_path_util.h"  // share::make_ob_stringview
#include "share/io/ob_io_manager.h"  // OB_IO_MANAGER
#include "lib/oblog/ob_log_module.h"

#include <cstdint>
#include <string>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

#define USING_LOG_PREFIX SQL
/// Owns the `ObObjectStorageInfo` for one external path. The storage type is
/// auto-detected from the path prefix (oss:// s3:// hdfs:// azblob:// file://
/// local://), so callers do not pick a device themselves: `ObStorageUtil::open`
/// and `ObExternalFileAccess` both branch on `storage_type_` internally.
struct StorageInfoHolder
{
  StorageInfoHolder()
      : storage_type_(common::ObStorageType::OB_STORAGE_MAX_TYPE), storage_info_(nullptr)
  {
  }

  // path/access_info buffers must remain valid for the lifetime of this holder
  // and must be NUL-terminated (both hold when they come from the ext fs
  // arena).
  int init(const common::ObString &ob_path, const common::ObString &ob_access_info)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(get_storage_type_from_path_for_external_table(ob_path, storage_type_))) {
      LOG_WARN("ext get_storage_type_from_path_for_external_table failed", K(ret), K(ob_path));
    } else if (common::ObStorageType::OB_STORAGE_HDFS == storage_type_) {
      storage_info_ = &hdfs_info_;
    } else {
      storage_info_ = &object_info_;
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(storage_info_)) {
      if (OB_FAIL(storage_info_->set(ob_path.ptr(), ob_access_info.ptr()))) {
        LOG_WARN("ext StorageInfoHolder set failed", K(ret), K(ob_path));
      }
    }
    return ret;
  }

  common::ObStorageType storage_type_;
  share::ObExternalTableStorageInfo object_info_;
  share::ObHDFSStorageInfo hdfs_info_;
  common::ObObjectStorageInfo *storage_info_;
  DISALLOW_COPY_AND_ASSIGN(StorageInfoHolder);
};

/// RAII tenant scope for an external-storage metadata op. Pairs with
/// `StorageInfoHolder` + `ObStorageUtil` for its metadata calls.
class ExtStorageTenantGuard final : public common::ObObjectStorageTenantGuard
{
public:
  explicit ExtStorageTenantGuard(const uint64_t tenant_id)
      : common::ObObjectStorageTenantGuard(
            tenant_id, OB_IO_MANAGER.get_object_storage_io_timeout_ms(tenant_id) * 1000LL)
  {
  }
};
#undef USING_LOG_PREFIX

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_FILE_SYSTEM_COMMON_H

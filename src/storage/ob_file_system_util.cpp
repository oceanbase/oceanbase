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

#include "ob_file_system_util.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace blocksstable;
using namespace rootserver;
namespace storage {
/**
 * ------------------------------ObFileSystemUtil-----------------------------------
 */
int ObFileSystemUtil::open_tenant_info_file(const ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id,
    const int open_flag, ObStorageFile*& info_file)
{
  int ret = OB_SUCCESS;
  ObStorageFile* tmp_info_file = nullptr;
  bool need_close = false;
  info_file = nullptr;
  if (OB_FAIL(OB_FILE_SYSTEM.alloc_file(tmp_info_file))) {
    LOG_WARN("fail to alloc file", K(ret));
  } else if (OB_FAIL(tmp_info_file->init(svr_addr, tenant_id, file_id, ObStorageFile::FileType::TENANT_DATA_INFO))) {
    LOG_WARN("fail to init tenant info file", K(ret));
  } else if (OB_FAIL(tmp_info_file->open(open_flag))) {
    LOG_WARN("fail to open tenant info file", K(ret));
  } else {
    info_file = tmp_info_file;
    need_close = true;
  }
  if (OB_FAIL(ret) && nullptr != tmp_info_file) {
    int tmp_ret = OB_SUCCESS;
    if (need_close) {
      if (OB_SUCCESS != (tmp_ret = tmp_info_file->close())) {
        LOG_WARN("fail to close file", K(tmp_ret));
      }
    }
    if (OB_SUCCESS != (tmp_ret = OB_FILE_SYSTEM.free_file(tmp_info_file))) {
      LOG_ERROR("fail to free file", K(ret));
    }
  }
  return ret;
}

/*static*/ int ObFileSystemUtil::open_tenant_file(const common::ObAddr& svr_addr, const uint64_t tenant_id,
    const int64_t file_id, const int open_flag, ObStorageFile*& tenant_file)
{
  int ret = OB_SUCCESS;
  tenant_file = nullptr;
  ObStorageFile* tmp_tenant_file = nullptr;
  bool need_close = false;
  if (OB_FAIL(OB_FILE_SYSTEM.alloc_file(tmp_tenant_file))) {
    LOG_WARN("fail to alloc pg_file", K(ret));
  } else if (OB_FAIL(tmp_tenant_file->init(svr_addr, tenant_id, file_id, ObStorageFile::FileType::TENANT_DATA))) {
    LOG_WARN("fail to init pg file", K(ret), K(tenant_id), K(file_id));
  } else if (OB_FAIL(tmp_tenant_file->open(open_flag))) {
    LOG_WARN("fail to open pg file", K(ret), K(open_flag));
  } else {
    need_close = true;
    tenant_file = tmp_tenant_file;
    tmp_tenant_file = nullptr;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_tenant_file)) {
    int tmp_ret = OB_SUCCESS;
    if (need_close && OB_SUCCESS != (tmp_ret = tmp_tenant_file->close())) {
      LOG_WARN("fail to close tmp_tenant_file", K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = OB_FILE_SYSTEM.free_file(tmp_tenant_file))) {
      LOG_ERROR("fail to free tmp_tenant_file", K(tmp_ret));
    }
  }
  return ret;
}

/*static*/ int ObFileSystemUtil::close_file(ObStorageFile* storage_file)
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  if (OB_ISNULL(storage_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_file));
  } else {
    if (OB_SUCCESS != (close_ret = storage_file->close())) {
      LOG_WARN("fail to close storage file", K(close_ret), KP(storage_file));
    }
    if (OB_FAIL(OB_FILE_SYSTEM.free_file(storage_file))) {
      LOG_WARN("fail to free storage file", K(ret), KP(storage_file));
    } else {
      ret = close_ret;
    }
  }
  return ret;
}

/*static*/ int ObFileSystemUtil::get_pg_file_with_guard(
    const common::ObPartitionKey& pkey, storage::ObIPartitionGroupGuard& pg_guard, ObStorageFile*& pg_file)
{
  int ret = OB_SUCCESS;
  pg_guard.reset();
  pg_file = nullptr;
  ObPartitionService& pts = OB_FILE_SYSTEM.get_partition_service();
  ObPGKey pg_key;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(pts.get_pg_key(pkey, pg_key))) {
    LOG_WARN("fail to get pg_key", K(ret), K(pkey));
  } else if (OB_FAIL(get_pg_file_with_guard_by_pg_key(pg_key, pg_guard, pg_file))) {
    LOG_WARN("fail to get pg file", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg_file)) {
    ret = OB_ERR_SYS;
    LOG_WARN("pg_file is null", K(ret), KP(pg_file));
  }
  if (OB_FAIL(ret)) {
    pg_guard.reset();
    pg_file = nullptr;
  }
  return ret;
}

/*static*/ int ObFileSystemUtil::get_pg_file_with_guard_by_pg_key(
    const common::ObPGKey& pg_key, storage::ObIPartitionGroupGuard& pg_guard, ObStorageFile*& pg_file)
{
  int ret = OB_SUCCESS;
  pg_guard.reset();
  pg_file = nullptr;
  ObPartitionService& pts = OB_FILE_SYSTEM.get_partition_service();
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(pts.get_partition(pg_key, pg_guard))) {
    LOG_WARN("fail to get pg_guard", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to get pg", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg_file = pg->get_storage_file())) {
    ret = OB_ERR_SYS;
    LOG_WARN("pg_file is null", K(ret), KP(pg_file));
  }
  if (OB_FAIL(ret)) {
    pg_guard.reset();
    pg_file = nullptr;
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase

/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_STORAGE_META_STORE_STORAGE_META_IO_UTIL_
#define OCEANBASE_STORAGE_META_STORE_STORAGE_META_IO_UTIL_

#include <stdint.h>
#include "storage/blocksstable/ob_object_manager.h"

namespace oceanbase
{
namespace storage
{

class ObStorageMetaIOUtil
{
public:
#ifdef OB_BUILD_SHARED_STORAGE
  template<typename T>
  static int write_storage_meta_object(
      const blocksstable::ObStorageObjectOpt &opt, const T &meta_object,
      ObIAllocator &allocator, const uint64_t mtl_tenant_id, const int64_t ls_epoch)
  {
    int ret = OB_SUCCESS;
    const int64_t serialize_size = meta_object.get_serialize_size();
    const int64_t buf_len = upper_align(serialize_size, DIO_ALIGN_SIZE);
    int64_t pos = 0;
    char *buf = nullptr;

    if (OB_UNLIKELY(!meta_object.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid meta object", K(ret), K(meta_object));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(buf_len));
    } else {
      memset(buf, '\0', buf_len);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_object.serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "fail to serialize meta object", K(ret), K(meta_object));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      STORAGE_LOG(WARN, "unexpected pos", K(ret), K(pos), K(serialize_size));
    } else {
      blocksstable::ObStorageObjectWriteInfo write_info;
      blocksstable::ObStorageObjectHandle object_handle;
      write_info.buffer_ = buf;
      write_info.size_ = buf_len;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
      write_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      write_info.ls_epoch_id_ = ls_epoch;
      write_info.mtl_tenant_id_ = mtl_tenant_id;
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, object_handle))) {
        STORAGE_LOG(WARN, "fail to write tenant super block", K(ret), K(opt), K(meta_object));
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
    }
    return ret;
  }

  template<typename T>
  static int read_storage_meta_object(
      const blocksstable::ObStorageObjectOpt &opt,
      ObIAllocator &allocator,
      const uint64_t mtl_tenant_id,
      const int64_t ls_epoch,
      T &meta_object)
  {
    int ret = OB_SUCCESS;
    blocksstable::MacroBlockId object_id;
    int64_t object_size = 0;
    char *buf = nullptr;

    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
      STORAGE_LOG(WARN, "fail to get object id", K(ret), K(opt));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.get_object_size(object_id, ls_epoch, object_size))) {
      STORAGE_LOG(WARN, "fail to get object size", K(ret), K(opt));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(object_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc mem", K(ret), K(object_size));
    } else {
      blocksstable::ObStorageObjectHandle object_handle;
      blocksstable::ObStorageObjectReadInfo read_info;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      read_info.buf_ = buf;
      read_info.macro_block_id_ = object_id;
      read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
      read_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      read_info.offset_ = 0;
      read_info.size_ = object_size;
      read_info.ls_epoch_id_ = ls_epoch;
      read_info.mtl_tenant_id_ = mtl_tenant_id;
      int64_t pos = 0;

      if (OB_FAIL(object_handle.async_read(read_info))) {
        STORAGE_LOG(WARN, "fail to async read tenant super block", K(ret), K(read_info));
      } else if (OB_FAIL(object_handle.wait())) {
        STORAGE_LOG(WARN, "fail to wait", K(ret), K(read_info));
      } else if (OB_FAIL(meta_object.deserialize(
          object_handle.get_buffer(), object_handle.get_data_size(), pos))) {
        STORAGE_LOG(WARN, "fail to deserialize", K(ret), K(object_id));
      } else if (OB_UNLIKELY(!meta_object.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        allocator.free(buf);
        STORAGE_LOG(WARN, "invalid meta object", K(ret), K(meta_object));
      }
    }

    return ret;
  }

  static int check_meta_existence(const blocksstable::ObStorageObjectOpt &opt, const int64_t ls_epoch, bool &is_exist);


#endif

};


} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_META_STORE_STORAGE_META_IO_UTIL_
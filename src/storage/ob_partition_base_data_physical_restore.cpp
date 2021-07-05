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
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_partition_migrator.h"
#include "ob_partition_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_partition_base_data_physical_restore.h"
#include "storage/ob_pg_storage.h"

using namespace oceanbase;
using namespace storage;
using namespace blocksstable;

/***********************ObPhyRestoreMetaIndexStore***************************/
int ObRestoreFileUtil::read_one_file(
    const ObString& path, const ObString& storage_info, ObIAllocator& allocator, char*& buf, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  int64_t file_len = 0;
  read_size = 0;
  int64_t buffer_len = 0;

  if (0 == path.length()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(path), K(ret));
  } else if (OB_FAIL(util.get_file_length(path, storage_info, file_len))) {
    STORAGE_LOG(WARN, "fail to get file len", K(ret), K(path));
  } else if (file_len <= 0) {  // empty file, just return success
    STORAGE_LOG(INFO, "current index file is empty", K(path), K(file_len));
  } else {
    buffer_len = file_len + 1;  // last for '\0'
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret), K(file_len), K(buffer_len));
    } else if (OB_FAIL(util.read_single_file(path, storage_info, buf, file_len, read_size))) {
      STORAGE_LOG(WARN, "fail to read all data", K(ret), K(path));
    } else if (OB_UNLIKELY(read_size != file_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size is invalid", K(ret), K(read_size), K(file_len), K(buffer_len));
    } else {
      buf[file_len] = '\0';
    }
  }
  return ret;
}

int ObRestoreFileUtil::pread_file(
    const ObString& path, const ObString& storage_info, const int64_t offset, const int64_t read_size, char* buf)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  int64_t real_read_size = 0;

  if (OB_UNLIKELY(0 == path.length())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(path), K(ret));
  } else if (OB_UNLIKELY(read_size <= 0)) {  // no data need read
    STORAGE_LOG(INFO, "read data len is zero", K(path), K(read_size));
  } else if (OB_FAIL(util.read_part_file(path, storage_info, buf, read_size, offset, real_read_size))) {
    STORAGE_LOG(WARN, "fail to pread file", K(ret), K(path), K(offset));
  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "not read enough file buffer", K(ret), K(read_size), K(real_read_size), K(path));
  }
  return ret;
}

int ObRestoreFileUtil::read_partition_group_meta(const ObString& path, const ObString& storage_info,
    const ObBackupMetaIndex& meta_index, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  if (OB_UNLIKELY(meta_index.meta_type_ != ObBackupMetaType::PARTITION_GROUP_META)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "meta index type not match", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(meta_index.data_length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    const int64_t read_size = meta_index.data_length_;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader* meta_header = NULL;
    if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(read_size), K(pos), K(meta_index), K(path));
    } else if (OB_FAIL(buffer_reader.get(meta_header))) {
      STORAGE_LOG(WARN, "read meta header fail", K(ret));
    } else if (OB_ISNULL(meta_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is null", K(ret));
    } else if (OB_FAIL(meta_header->check_valid())) {
      STORAGE_LOG(WARN, "meta_header is invalid", K(ret), K(path), K(meta_index));
    } else if (meta_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
    } else if (OB_FAIL(meta_header->check_data_checksum(buffer_reader.current(), meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret), K(path), K(meta_index));
    } else if (OB_FAIL(buffer_reader.read_serialize(pg_meta))) {
      STORAGE_LOG(WARN, "read pg meta fail", K(ret), K(path), K(meta_index));
    } else if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(path), K(meta_index));
    }
  }
  return ret;
}

int ObRestoreFileUtil::read_partition_meta(const ObString& path, const ObString& storage_info,
    const ObBackupMetaIndex& meta_index, const int64_t& compatible, storage::ObPGPartitionStoreMeta& partition_meta)
{
  {
    int ret = OB_SUCCESS;
    common::ObArenaAllocator allocator(ObModIds::RESTORE);
    char* read_buf = NULL;
    if (OB_UNLIKELY(meta_index.meta_type_ != ObBackupMetaType::PARTITION_META)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "meta index type not match", K(ret), K(meta_index));
    } else if (OB_FAIL(meta_index.check_valid())) {
      STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
    } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(meta_index.data_length_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
    } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                   path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
      STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
    } else {
      const int64_t read_size = meta_index.data_length_;
      int64_t pos = 0;
      ObBufferReader buffer_reader;
      const ObBackupMetaHeader* meta_header = NULL;

      if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
        LOG_WARN("failed to deserialize buffer reader", K(ret), K(read_size), K(pos), K(meta_index), K(path));
      } else if (OB_FAIL(buffer_reader.get(meta_header))) {
        STORAGE_LOG(WARN, "read meta header fail", K(ret));
      } else if (OB_ISNULL(meta_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta header is null", K(ret));
      } else if (OB_FAIL(meta_header->check_valid())) {
        STORAGE_LOG(WARN, "meta_header is invaild", K(ret));
      } else if (meta_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
      } else if (OB_FAIL(meta_header->check_data_checksum(buffer_reader.current(), meta_header->data_length_))) {
        STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
      } else if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible) {
        ObPartitionStoreMeta tmp_meta;
        if (OB_FAIL(buffer_reader.read_serialize(tmp_meta))) {
          LOG_WARN("failed to read tmp meta", K(ret));
        } else if (OB_FAIL(partition_meta.copy_from_old_meta(tmp_meta))) {
          LOG_WARN("failed to copy_from_old_meta", K(ret), K(tmp_meta));
        }
      } else if (OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
        if (OB_FAIL(buffer_reader.read_serialize(partition_meta))) {
          STORAGE_LOG(WARN, "read partition meta fail", K(ret), K(path), K(meta_index));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unkown compatible", K(ret), K(compatible));
      }

      if (OB_SUCC(ret)) {
        if (0 != buffer_reader.remain()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(path), K(meta_index));
        }
      }
    }
    return ret;
  }
}

int ObRestoreFileUtil::read_sstable_metas(const ObString& path, const ObString& storage_info,
    const ObBackupMetaIndex& meta_index, common::ObArenaAllocator& meta_allocator,
    common::ObIArray<blocksstable::ObSSTableBaseMeta>& sstable_info_array)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  sstable_info_array.reuse();
  if (OB_UNLIKELY(meta_index.meta_type_ != ObBackupMetaType::SSTABLE_METAS)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "meta index type not match", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(meta_index.data_length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    const int64_t read_size = meta_index.data_length_;
    int64_t table_count = 0;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader* meta_header = NULL;

    if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(read_size), K(pos), K(meta_index), K(path));
    } else if (OB_FAIL(buffer_reader.get(meta_header))) {
      STORAGE_LOG(WARN, "read meta header fail", K(ret));
    } else if (OB_ISNULL(meta_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is null", K(ret));
    } else if (OB_FAIL(meta_header->check_valid())) {
      STORAGE_LOG(WARN, "meta_header is invalid", K(ret));
    } else if (meta_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
    } else if (OB_FAIL(meta_header->check_data_checksum(buffer_reader.current(), meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
    } else if (OB_FAIL(buffer_reader.read(table_count))) {
      STORAGE_LOG(WARN, "read sstable count", K(ret), K(path), K(meta_index));
    } else {
      ObSSTableBaseMeta meta(meta_allocator);
      for (int i = 0; OB_SUCC(ret) && i < table_count; ++i) {
        meta.reset();
        if (OB_FAIL(buffer_reader.read_serialize(meta))) {
          STORAGE_LOG(WARN, "read sstable base meta fail", K(ret), K(path), K(meta_index));
        } else if (OB_FAIL(sstable_info_array.push_back(meta))) {
          STORAGE_LOG(WARN, "push back meta fail", K(ret), K(path), K(meta_index));
        }
      }
      if (0 != buffer_reader.remain()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(path), K(meta_index));
      }
    }
  }
  return ret;
}

int ObRestoreFileUtil::read_table_keys(const ObString& path, const ObString& storage_info,
    const ObBackupMetaIndex& meta_index, common::ObIArray<storage::ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  if (OB_UNLIKELY(meta_index.meta_type_ != ObBackupMetaType::TABLE_KEYS)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "meta index type not match", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(meta_index.data_length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    const int64_t read_size = meta_index.data_length_;
    int64_t table_count = 0;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader* meta_header = NULL;
    storage::ObITable::TableKey table_key;

    if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(read_size), K(pos), K(meta_index), K(path));
    } else if (OB_FAIL(buffer_reader.get(meta_header))) {
      STORAGE_LOG(WARN, "read meta header fail", K(ret));
    } else if (OB_ISNULL(meta_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is null", K(ret));
    } else if (OB_FAIL(meta_header->check_valid())) {
      STORAGE_LOG(WARN, "meta_header is invaild", K(ret), K(meta_index), K(*meta_header));
    } else if (meta_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "meta_header data_length may be wrong, buffer_reader not enough", K(ret));
    } else if (OB_FAIL(meta_header->check_data_checksum(buffer_reader.current(), meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
    } else if (OB_FAIL(buffer_reader.read(table_count))) {
      STORAGE_LOG(WARN, "read table count", K(ret), K(path), K(meta_index));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < table_count; ++i) {
        if (OB_FAIL(buffer_reader.read_serialize(table_key))) {
          STORAGE_LOG(WARN, "read table key fail", K(ret), K(i), K(path), K(meta_index));
        } else if (OB_FAIL(table_keys.push_back(table_key))) {
          STORAGE_LOG(WARN, "push back table key fail", K(ret), K(i), K(path), K(meta_index));
        }
      }
      if (0 != buffer_reader.remain()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(path), K(meta_index));
      }
    }
  }
  return ret;
}

int ObRestoreFileUtil::read_macroblock_data(const ObString& path, const ObString& storage_info,
    const ObBackupMacroIndex& meta_index, common::ObArenaAllocator& allocator, ObMacroBlockSchemaInfo*& new_schema,
    ObMacroBlockMetaV2*& new_meta, blocksstable::ObBufferReader& macro_data)
{
  int ret = OB_SUCCESS;
  char* read_buf = NULL;
  int64_t buf_size = meta_index.data_length_ + DIO_READ_ALIGN_SIZE;
  new_schema = nullptr;
  new_meta = nullptr;

  if (OB_FAIL(meta_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    ObBufferReader buffer_reader(read_buf, meta_index.data_length_);
    const ObBackupCommonHeader* common_header = NULL;
    blocksstable::ObBufferReader tmp_macro_meta_reader;
    blocksstable::ObBufferReader tmp_macro_data;
    ObBackupMacroData backup_macro_data(tmp_macro_meta_reader, tmp_macro_data);
    common::ObObj* endkey = NULL;
    ObMacroBlockMetaV2 tmp_meta;
    ObMacroBlockSchemaInfo tmp_schema;
    ObFullMacroBlockMetaEntry marco_block_meta_entry(tmp_meta, tmp_schema);
    if (OB_ISNULL(endkey = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory for macro block end key", K(ret));
    } else if (OB_ISNULL(tmp_meta.endkey_ = new (endkey) ObObj[common::OB_MAX_COLUMN_NUMBER])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro meta endkey is null", K(ret));
    } else if (OB_FAIL(buffer_reader.get(common_header))) {
      STORAGE_LOG(WARN, "read macro data common header fail", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta index common header is null", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_valid())) {
      STORAGE_LOG(WARN, "common_header is not valid", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(buffer_reader.read_serialize(backup_macro_data))) {
      STORAGE_LOG(WARN, "read data_header fail", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(tmp_macro_meta_reader.read_serialize(marco_block_meta_entry))) {
      LOG_WARN("failed to read tmp meta",
          K(ret),
          K(*common_header),
          K(path),
          K(backup_macro_data),
          K(meta_index),
          K(buffer_reader));
    } else if (is_sys_table(tmp_meta.table_id_)) {
      // TODO wait for __all_core_table become __all_tenant_core_table
      // set schema version 0, then next major merge become full major merge
      tmp_meta.schema_version_ = 0;
      tmp_schema.schema_version_ = 0;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_meta.deep_copy(new_meta, allocator))) {
      LOG_WARN("failed to copy macro meta", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(tmp_schema.deep_copy(new_schema, allocator))) {
      LOG_WARN("failed to copy schema", K(ret), K(path), K(meta_index));
    } else {
      int64_t write_buf_size = common::upper_align(tmp_macro_data.capacity(), DIO_READ_ALIGN_SIZE);
      macro_data.assign(tmp_macro_data.data(), write_buf_size, tmp_macro_data.capacity());
    }
  }
  return ret;
}

int ObRestoreFileUtil::read_macroblock_data(const ObString& path, const ObString& storage_info,
    const ObBackupTableMacroIndex& meta_index, common::ObArenaAllocator& allocator, ObMacroBlockSchemaInfo*& new_schema,
    ObMacroBlockMetaV2*& new_meta, blocksstable::ObBufferReader& macro_data)
{
  int ret = OB_SUCCESS;
  char* read_buf = NULL;
  int64_t buf_size = meta_index.data_length_ + DIO_READ_ALIGN_SIZE;
  new_schema = nullptr;
  new_meta = nullptr;

  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    ObBufferReader buffer_reader(read_buf, meta_index.data_length_);
    const ObBackupCommonHeader* common_header = NULL;
    blocksstable::ObBufferReader tmp_macro_meta_reader;
    blocksstable::ObBufferReader tmp_macro_data;
    ObBackupMacroData backup_macro_data(tmp_macro_meta_reader, tmp_macro_data);
    common::ObObj* endkey = NULL;
    ObMacroBlockMetaV2 tmp_meta;
    ObMacroBlockSchemaInfo tmp_schema;
    ObFullMacroBlockMetaEntry marco_block_meta_entry(tmp_meta, tmp_schema);
    if (OB_ISNULL(endkey = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory for macro block end key", K(ret));
    } else if (OB_ISNULL(tmp_meta.endkey_ = new (endkey) ObObj[common::OB_MAX_COLUMN_NUMBER])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro meta endkey is null", K(ret));
    } else if (OB_FAIL(buffer_reader.get(common_header))) {
      STORAGE_LOG(WARN, "read macro data common header fail", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta index common header is null", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_valid())) {
      STORAGE_LOG(WARN, "common_header is not valid", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(buffer_reader.read_serialize(backup_macro_data))) {
      STORAGE_LOG(WARN, "read data_header fail", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(tmp_macro_meta_reader.read_serialize(marco_block_meta_entry))) {
      LOG_WARN("failed to read tmp meta",
          K(ret),
          K(*common_header),
          K(path),
          K(backup_macro_data),
          K(meta_index),
          K(buffer_reader));
    } else if (OB_FAIL(tmp_meta.deep_copy(new_meta, allocator))) {
      LOG_WARN("failed to copy macro meta", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(tmp_schema.deep_copy(new_schema, allocator))) {
      LOG_WARN("failed to copy schema", K(ret), K(path), K(meta_index));
    } else {
      int64_t write_buf_size = common::upper_align(tmp_macro_data.capacity(), DIO_READ_ALIGN_SIZE);
      macro_data.assign(tmp_macro_data.data(), write_buf_size, tmp_macro_data.capacity());
    }
  }
  return ret;
}

int ObRestoreFileUtil::fetch_max_backup_file_id(const ObString& path, const ObString& storage_info,
    const int64_t& backup_set_id, int64_t& max_index_id, int64_t& max_data_id)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> file_names;
  ObArray<int64_t> file_ids;
  max_index_id = -1;
  max_data_id = -1;
  if (OB_FAIL(util.list_files(path, storage_info, allocator, file_names))) {
    STORAGE_LOG(WARN, "list files fail", K(ret), K(path));
  } else if (0 == file_names.count()) {  // empty
  } else {
    char index_prefix[share::OB_MAX_BACKUP_ID_LENGTH];
    char data_prefix[share::OB_MAX_BACKUP_ID_LENGTH];
    int pos = databuff_printf(
        index_prefix, common::OB_MAX_TABLE_NAME_LENGTH, "%s_%ld", OB_STRING_BACKUP_MACRO_BLOCK_INDEX, backup_set_id);
    if (pos < 0 || pos >= share::OB_MAX_BACKUP_ID_LENGTH) {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    pos = databuff_printf(
        data_prefix, common::OB_MAX_TABLE_NAME_LENGTH, "%s_%ld.", OB_STRING_BACKUP_MACRO_BLOCK_FILE, backup_set_id);
    if (pos < 0 || pos >= share::OB_MAX_BACKUP_ID_LENGTH) {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    for (int i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      ObString& file_name = file_names.at(i);
      int64_t file_id = -1;
      if (file_name.prefix_match(index_prefix)) {
        if (0 == file_name.compare(index_prefix)) {
          file_id = 0;
          if (OB_FAIL(file_ids.push_back(file_id))) {
            STORAGE_LOG(WARN, "fail to push back file id ", K(file_id));
          }
        } else if (OB_FAIL(get_file_id(file_name, file_id))) {
          STORAGE_LOG(WARN, "fail to get file id", K(file_name), K(file_id));
        } else if (file_id >= 0) {
          if (OB_FAIL(file_ids.push_back(file_id))) {
            STORAGE_LOG(WARN, "fail to push back file id ", K(file_id));
          }
        }
      } else if (file_name.prefix_match(data_prefix)) {
        if (OB_FAIL(get_file_id(file_name, file_id))) {
          STORAGE_LOG(WARN, "fail to get file id", K(file_name), K(file_id));
        } else if (file_id > max_data_id) {
          max_data_id = file_id;
        }
      }
    }
    // check if file id continued
    std::sort(file_ids.begin(), file_ids.end());
    for (int i = 0; OB_SUCC(ret) && i < file_ids.count(); ++i) {
      if (i != file_ids.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro file index is not continued", K(file_ids));
      }
    }
    max_index_id = file_ids.count() - 1;
    STORAGE_LOG(INFO, "fetch history backup data id succ", K(path), K(max_index_id), K(max_data_id));
  }
  return ret;
}

int ObRestoreFileUtil::get_file_id(const ObString& file_name, int64_t& file_id)
{
  int ret = OB_SUCCESS;
  ObString id_str = file_name.after('.').trim();
  file_id = 0 == id_str.length() ? -1 : 0;
  // check num
  for (int i = 0; OB_SUCC(ret) && i < id_str.length(); ++i) {
    if (id_str[i] < '0' || id_str[i] > '9') {
      file_id = -1;
      break;
    }
    file_id = file_id * 10 + id_str[i] - '0';
  }
  return ret;
}

int ObRestoreFileUtil::limit_bandwidth_and_sleep(
    ObInOutBandwidthThrottle& throttle, const int64_t cur_data_size, int64_t& last_read_size)
{
  int ret = OB_SUCCESS;
  const int64_t last_active_time = ObTimeUtility::current_time();
  const int64_t read_size = cur_data_size - last_read_size;
  last_read_size = cur_data_size;

  if (OB_FAIL(throttle.limit_in_and_sleep(read_size, last_active_time, MAX_IDLE_TIME))) {
    STORAGE_LOG(WARN, "failed to limit_in_and_sleep", K(ret));
  }
  return ret;
}

int ObRestoreFileUtil::read_backup_pg_meta_info(const ObString& path, const ObString& storage_info,
    const ObBackupMetaIndex& meta_index, ObBackupPGMetaInfo& backup_pg_meta_info)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  if (OB_UNLIKELY(ObBackupMetaType::PARTITION_GROUP_META_INFO != meta_index.meta_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "meta index type not match", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_index.check_valid())) {
    STORAGE_LOG(WARN, "invalid meta index", K(ret), K(meta_index));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(meta_index.data_length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(
                 path, storage_info, meta_index.offset_, meta_index.data_length_, read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(path), K(meta_index));
  } else {
    const int64_t read_size = meta_index.data_length_;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader* meta_header = NULL;
    if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(read_size), K(pos), K(meta_index), K(path));
    } else if (OB_FAIL(buffer_reader.get(meta_header))) {
      STORAGE_LOG(WARN, "read meta header fail", K(ret));
    } else if (OB_ISNULL(meta_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is null", K(ret));
    } else if (OB_FAIL(meta_header->check_valid())) {
      STORAGE_LOG(WARN, "meta_header is invalid", K(ret), K(path), K(meta_index));
    } else if (meta_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
    } else if (OB_FAIL(meta_header->check_data_checksum(buffer_reader.current(), meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret), K(path), K(meta_index));
    } else if (OB_FAIL(buffer_reader.read_serialize(backup_pg_meta_info))) {
      STORAGE_LOG(WARN, "read pg meta fail", K(ret), K(path), K(meta_index), K(*meta_header));
    } else if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(path), K(meta_index));
    }
  }
  return ret;
}

/***********************ObPhyRestoreMetaIndexStore***************************/
ObPhyRestoreMetaIndexStore::ObPhyRestoreMetaIndexStore() : is_inited_(false), index_map_()
{}

ObPhyRestoreMetaIndexStore::~ObPhyRestoreMetaIndexStore()
{
  reset();
}

void ObPhyRestoreMetaIndexStore::reset()
{
  is_inited_ = false;
  index_map_.clear();
}

int ObPhyRestoreMetaIndexStore::init(
    const ObBackupBaseDataPathInfo& path_info, const int64_t compatible, const bool need_check_compeleted)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t file_length = 0;
  int64_t total_length = 0;

  int64_t parsed_count = 0;
  ObBackupPath path;
  ObStorageUtil util(true /*need retry*/);
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  ObArray<common::ObString> index_file_names;
  if (OB_UNLIKELY(!path_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path info is invalid", K(ret), K(path_info));
  } else if (OB_FAIL(index_map_.create(BUCKET_SIZE, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(
                 util.list_files(path.get_obstr(), path_info.dest_.get_storage_info(), allocator, index_file_names))) {
    STORAGE_LOG(WARN, "list files fail", K(ret), K(path));
  } else {
    ObBackupPath index_path;
    for (int i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      index_path.reset();
      ObString& file_name = index_file_names.at(i);
      if (!(file_name.prefix_match(OB_STRING_BACKUP_META_INDEX))) {
        // filter meta data file
        STORAGE_LOG(INFO, "skip not meta data file", K(file_name));
      } else if (OB_FAIL(index_path.init(path.get_obstr()))) {
        STORAGE_LOG(WARN, "fail to init index base path", K(ret), K(path));
      } else if (OB_FAIL(index_path.join(file_name))) {
        STORAGE_LOG(WARN, "fail to init index path", K(ret), K(index_path), K(file_name));
      } else if (OB_FAIL(init_one_file(
                     index_path.get_obstr(), path_info.dest_.get_storage_info(), file_length, total_length))) {
        STORAGE_LOG(
            ERROR, "fail to init index file", K(ret), K(tmp_ret), K(index_path), K(file_length), K(total_length));
        if (total_length >= file_length) {
          // last flushed buffer is not completed, may next file has completed one
          // overwrite ret
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        ++parsed_count;
      }
    }
  }

  if (OB_SUCC(ret) && 0 == parsed_count) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "no meta index is parsed", K(ret), K(index_file_names));
  }

  if (OB_SUCC(ret) && need_check_compeleted) {
    if (OB_FAIL(check_meta_index_completed(compatible, path_info))) {
      STORAGE_LOG(WARN, "failed to check meta index completed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    index_map_.clear();
  }
  return ret;
}

int ObPhyRestoreMetaIndexStore::init_one_file(
    const ObString& path, const ObString& storage_info, int64_t& file_length, int64_t& total_length)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  file_length = 0;
  total_length = 0;
  if (OB_FAIL(ObRestoreFileUtil::read_one_file(path, storage_info, allocator, read_buf, file_length))) {
    STORAGE_LOG(WARN, "fail to read index file", K(ret), K(path));
  } else if (file_length <= 0) {
    STORAGE_LOG(INFO, "may be empty meta index file", K(ret), K(path), K(file_length));
  } else {
    ObBufferReader buffer_reader(read_buf, file_length, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupMetaIndex meta_index;
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "read meta index common header fail", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "common_header is not valid", K(ret));
      } else if (FALSE_IT(total_length += common_header->data_length_)) {
      } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
        STORAGE_LOG(INFO, "file reach the end mark, ", K(path));
        break;
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        STORAGE_LOG(WARN, "common header data checksum fail", K(ret));
      } else {
        int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
        for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
          if (OB_FAIL(buffer_reader.read_serialize(meta_index))) {
            STORAGE_LOG(WARN, "read meta index fail", K(ret));
          } else {
            const share::ObMetaIndexKey key(meta_index.table_id_, meta_index.partition_id_, meta_index.meta_type_);
            if (OB_FAIL(index_map_.set_refactored(key, meta_index, 1 /*overwrite*/))) {
              STORAGE_LOG(WARN, "fail to set index map", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret) && common_header->align_length_ > 0) {
        if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
          STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
        }
      }
    }
  }
  return ret;
}

bool ObPhyRestoreMetaIndexStore::is_inited() const
{
  return is_inited_;
}

int ObPhyRestoreMetaIndexStore::get_meta_index(
    const ObPartitionKey& part_key, const ObBackupMetaType& type, ObBackupMetaIndex& meta_index) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta index store do not init", K(ret));
  } else if (OB_UNLIKELY(!part_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid partition key", K(ret), K(part_key));
  } else {
    const share::ObMetaIndexKey key(part_key.get_table_id(), part_key.get_partition_id(), type);
    if (OB_FAIL(index_map_.get_refactored(key, meta_index))) {
      STORAGE_LOG(WARN, "fail to get meta index", K(ret), K(key));
    }
  }
  return ret;
}

int ObPhyRestoreMetaIndexStore::check_meta_index_completed(
    const int64_t compatible, const ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  ObExternPGListMgr pg_list_mgr;
  ObArray<ObPGKey> pg_key_list;
  ObBackupMetaType type = ObBackupMetaType::META_TYPE_MAX;
  ObBackupMetaIndex meta_index;
  ObFakeBackupLeaseService fake_lease_service;
  if (ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible) {
    type = ObBackupMetaType::PARTITION_GROUP_META;
  } else if (ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
    type = ObBackupMetaType::PARTITION_GROUP_META_INFO;
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "compatible is invalid", K(ret), K(compatible), K(path_info));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pg_list_mgr.init(path_info.tenant_id_,
                 path_info.full_backup_set_id_,
                 path_info.inc_backup_set_id_,
                 path_info.dest_,
                 fake_lease_service))) {
    STORAGE_LOG(WARN, "failed to init pg list mgr", K(ret), K(path_info));
  } else if (OB_FAIL(pg_list_mgr.get_sys_pg_list(pg_key_list))) {
    STORAGE_LOG(WARN, "failed to get sys pg list", K(ret), K(path_info));
  } else if (OB_FAIL(pg_list_mgr.get_normal_pg_list(pg_key_list))) {
    STORAGE_LOG(WARN, "failed to get normal pg list", K(ret), K(path_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_key_list.count(); ++i) {
      const ObPGKey& pg_key = pg_key_list.at(i);
      const share::ObMetaIndexKey key(pg_key.get_table_id(), pg_key.get_partition_id(), type);
      if (OB_FAIL(index_map_.get_refactored(key, meta_index))) {
        STORAGE_LOG(WARN, "fail to get meta index", K(ret), K(key));
      }
    }
  }
  return ret;
}

/***********************ObPhyRestoreMacroIndexStore***************************/
ObPhyRestoreMacroIndexStore::ObPhyRestoreMacroIndexStore()
    : is_inited_(false), allocator_(ObModIds::RESTORE), index_map_()
{}

ObPhyRestoreMacroIndexStore::~ObPhyRestoreMacroIndexStore()
{
  reset();
}

void ObPhyRestoreMacroIndexStore::reset()
{
  is_inited_ = false;
  index_map_.clear();
  allocator_.reset();
}

bool ObPhyRestoreMacroIndexStore::is_inited() const
{
  return is_inited_;
}

int ObPhyRestoreMacroIndexStore::init(
    const share::ObPhysicalRestoreArg& arg, const ObReplicaRestoreStatus& restore_status)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath path;
  common::ObPartitionKey pkey;
  int64_t file_length = 0;
  int64_t total_length = 0;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg is invalid", K(ret));
  } else if (OB_FAIL(arg.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(arg.get_backup_pgkey(pkey))) {
    STORAGE_LOG(WARN, "failed to get backup pgkey", K(ret));
  } else if (OB_FAIL(index_map_.create(BUCKET_SIZE, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
  } else {
    bool is_exist = true;
    bool last_file_complete = false;
    for (int i = 0; OB_SUCC(ret) && is_exist; ++i) {  // retry cnt
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_macro_block_index_path(
              path_info, pkey.get_table_id(), pkey.get_partition_id(), i, path))) {
        STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
      } else if (OB_FAIL(util.is_exist(path.get_obstr(), path_info.dest_.get_storage_info(), is_exist))) {
        STORAGE_LOG(WARN, "fail to check index file exist or not", K(ret));
      } else if (!is_exist) {
        break;
      } else if (OB_FAIL(
                     init_one_file(path.get_obstr(), path_info.dest_.get_storage_info(), file_length, total_length))) {
        STORAGE_LOG(WARN, "fail to init index file", K(ret), K(path), K(file_length), K(total_length));
        if (total_length >= file_length) {
          // last flushed buffer is not completed, may next file has completed one
          // overwrite ret
          ret = OB_SUCCESS;
        }
      } else {
        last_file_complete = true;
      }
    }

    if (OB_SUCC(ret) && !last_file_complete && REPLICA_RESTORE_DATA == restore_status) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "last macro index file is not complete", K(ret), K(pkey), K(arg), K(restore_status));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    index_map_.clear();
  }
  return ret;
}

int ObPhyRestoreMacroIndexStore::init(const common::ObPartitionKey& pkey, const share::ObPhysicalBackupArg& arg)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath path;
  int64_t file_length = 0;
  int64_t total_length = 0;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg is invalid", K(ret));
  } else if (OB_FAIL(arg.get_prev_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(index_map_.create(BUCKET_SIZE, ObModIds::BACKUP))) {
    STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
  } else {
    bool is_exist = true;
    for (int i = 0; OB_SUCC(ret) && is_exist; ++i) {  // retry cnt
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_macro_block_index_path(
              path_info, pkey.get_table_id(), pkey.get_partition_id(), i, path))) {
        STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
      } else if (OB_FAIL(util.is_exist(path.get_obstr(), path_info.dest_.get_storage_info(), is_exist))) {
        STORAGE_LOG(WARN, "fail to check index file exist or not", K(ret));
      } else if (!is_exist) {
        FLOG_INFO("backup path is not exist, please check", K(path), K(pkey), K(arg));
        break;
      } else if (OB_FAIL(
                     init_one_file(path.get_obstr(), path_info.dest_.get_storage_info(), file_length, total_length))) {
        STORAGE_LOG(WARN, "fail to init index file", K(ret), K(path), K(file_length), K(total_length));
        if (total_length >= file_length) {
          // last flushed buffer is not completed, may next file has completed one
          // overwrite ret
          ret = OB_SUCCESS;
        }
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    index_map_.clear();
  }
  return ret;
}

int ObPhyRestoreMacroIndexStore::init_one_file(
    const ObString& path, const ObString& storage_info, int64_t& file_length, int64_t& total_length)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  file_length = 0;
  total_length = 0;
  if (OB_FAIL(ObRestoreFileUtil::read_one_file(path, storage_info, allocator, read_buf, file_length))) {
    STORAGE_LOG(WARN, "fail to read index file", K(ret), K(path));
  } else if (file_length <= 0) {
    STORAGE_LOG(INFO, "may be empty block index file", K(ret), K(path));
  } else {
    ObBufferReader buffer_reader(read_buf, file_length, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupMacroIndex macro_index;
    ObArray<ObBackupMacroIndex> index_list;
    uint64_t cur_index_id = 0;
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "read macro index common header fail", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
      } else if (FALSE_IT(total_length += common_header->data_length_)) {
      } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
        STORAGE_LOG(INFO, "file reach the end mark, ", K(path));
        break;
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        STORAGE_LOG(WARN, "common header data checksum fail", K(ret), K(*common_header));
      } else {
        int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
        for (int i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
          if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
            STORAGE_LOG(WARN, "read macro index fail", K(ret));
          } else {
            if (0 == index_list.count()) {
              if (OB_FAIL(index_list.push_back(macro_index))) {
                STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
              }
              cur_index_id = macro_index.index_table_id_;
            } else if (cur_index_id == macro_index.index_table_id_) {
              if (OB_FAIL(index_list.push_back(macro_index))) {
                STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
              }
            } else {
              if (OB_FAIL(add_sstable_index(cur_index_id, index_list))) {
                STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_index_id));
              } else {
                index_list.reuse();
                cur_index_id = macro_index.index_table_id_;
                if (OB_FAIL(index_list.push_back(macro_index))) {
                  STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && index_list.count() > 0) {
          if (OB_FAIL(add_sstable_index(cur_index_id, index_list))) {
            STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_index_id));
          } else {
            index_list.reset();
          }
        }

        if (OB_SUCC(ret) && common_header->align_length_ > 0) {
          if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
            STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
          }
        }
      }
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStore::add_sstable_index(
    const uint64_t index_id, const common::ObIArray<ObBackupMacroIndex>& index_list)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObArray<ObBackupMacroIndex>* new_block_list = NULL;

  int64_t block_count = index_list.count();
  if (0 == block_count) {
    STORAGE_LOG(INFO, "no block index need add", K(block_count), K(index_id));
  } else if (OB_FAIL(index_map_.get_refactored(index_id, new_block_list))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get old block list fail", K(ret), K(index_id));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<ObBackupMacroIndex>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
      } else if (OB_ISNULL(new_block_list = new (buf) ObArray<ObBackupMacroIndex>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to new block list", K(ret));
      } else if (OB_FAIL(index_map_.set_refactored(index_id, new_block_list, 1 /*rewrite*/))) {
        STORAGE_LOG(WARN, "failed to set macro index map", K(ret));
        new_block_list->~ObArray();
      }
    }
  } else if (OB_ISNULL(new_block_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exist block list should not be null here", K(ret), K(index_id));
  }

  for (int i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupMacroIndex& index = index_list.at(i);  // check
    if (index.sstable_macro_index_ < new_block_list->count()) {
      // skip
    } else if (OB_UNLIKELY(index.sstable_macro_index_ != new_block_list->count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "sstable macro index is not continued",
          K(ret),
          K(index_id),
          K(index),
          "new_block_list_count",
          new_block_list->count(),
          K(*new_block_list));
    } else if (OB_FAIL(new_block_list->push_back(index))) {
      STORAGE_LOG(WARN, "failed to push back block index", K(ret), K(i));
    }
  }

  return ret;
}

int ObPhyRestoreMacroIndexStore::get_macro_index_array(
    const uint64_t index_id, const common::ObArray<ObBackupMacroIndex>*& index_list) const
{
  int ret = OB_SUCCESS;
  index_list = NULL;
  common::ObArray<ObBackupMacroIndex>* tmp_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (OB_FAIL(index_map_.get_refactored(index_id, tmp_list))) {
    STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(index_id));
  } else {
    index_list = tmp_list;
  }
  return ret;
}

int ObPhyRestoreMacroIndexStore::get_macro_index(
    const uint64_t index_id, const int64_t sstable_macro_idx, ObBackupMacroIndex& macro_index) const
{
  int ret = OB_SUCCESS;
  common::ObArray<ObBackupMacroIndex>* index_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (OB_FAIL(index_map_.get_refactored(index_id, index_list))) {
    STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(index_id));
  } else if (OB_ISNULL(index_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get sstable index array fail", K(ret), K(index_id));
  } else if (sstable_macro_idx >= index_list->count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    STORAGE_LOG(
        WARN, "sstable idx not match with sstable index array", K(ret), K(sstable_macro_idx), K(index_list->count()));
  } else {
    macro_index = index_list->at(sstable_macro_idx);
    if (sstable_macro_idx != macro_index.sstable_macro_index_ || index_id != macro_index.index_table_id_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block index not match", K(ret), K(macro_index), K(index_id), K(sstable_macro_idx));
    }
    if (OB_FAIL(ret)) {
      macro_index.reset();
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStore::get_sstable_pair_list(
    const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list) const
{
  int ret = OB_SUCCESS;
  common::ObArray<ObBackupMacroIndex>* index_list = NULL;
  pair_list.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (OB_FAIL(index_map_.get_refactored(index_id, index_list))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "macro index not exist, may be empty sstable", K(ret), K(index_id));
    } else {
      STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(index_id));
    }
  } else if (OB_ISNULL(index_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get sstable index array fail", K(ret), K(index_id));
  } else {
    blocksstable::ObSSTablePair pair;
    for (int i = 0; OB_SUCC(ret) && i < index_list->count(); ++i) {
      pair.data_version_ = index_list->at(i).data_version_;
      pair.data_seq_ = index_list->at(i).data_seq_;
      if (OB_FAIL(pair_list.push_back(pair))) {
        STORAGE_LOG(WARN, "push sstable pair to pair list fail", K(ret), K(index_id), K(pair));
      }
    }
    if (OB_FAIL(ret)) {
      pair_list.reset();
    }
  }
  return ret;
}

/************************ObPartitionMetaPhysicalReader************************/
ObPartitionMetaPhysicalReader::ObPartitionMetaPhysicalReader()
    : is_inited_(false),
      sstable_index_(0),
      data_size_(0),
      sstable_meta_array_(),
      table_keys_array_(),
      allocator_(ObModIds::RESTORE),
      arg_(NULL),
      meta_indexs_(NULL),
      macro_indexs_(NULL),
      table_count_(0),
      pkey_()
{}

ObPartitionMetaPhysicalReader::~ObPartitionMetaPhysicalReader()
{
  reset();
}

void ObPartitionMetaPhysicalReader::reset()
{
  is_inited_ = false;
  sstable_index_ = 0;
  data_size_ = 0;
  for (int i = 0; i < sstable_meta_array_.count(); ++i) {
    sstable_meta_array_.at(i).reset();
  }
  for (int i = 0; i < table_keys_array_.count(); ++i) {
    table_keys_array_.at(i).reset();
  }
  sstable_meta_array_.reset();
  table_keys_array_.reset();
  allocator_.reset();
  table_count_ = 0;
  pkey_.reset();
}

int ObPartitionMetaPhysicalReader::init(const ObPhysicalRestoreArg& arg, const ObPhyRestoreMetaIndexStore& meta_indexs,
    const ObPhyRestoreMacroIndexStore& macro_indexs, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited", K(ret));
  } else if (!pkey.is_valid() || !arg.is_valid() || !meta_indexs.is_inited() || !macro_indexs.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(pkey), K(arg), K(meta_indexs), K(macro_indexs));
  } else {
    pkey_ = pkey;
    arg_ = &arg;
    meta_indexs_ = &meta_indexs;
    macro_indexs_ = &macro_indexs;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_table_keys())) {
    STORAGE_LOG(WARN, "fail to read table keys", K(ret));
  } else if (OB_FAIL(read_all_sstable_meta())) {
    STORAGE_LOG(WARN, "fail to read all sstable meta", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionMetaPhysicalReader::read_all_sstable_meta()
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  if (OB_ISNULL(arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg_ is null", K(ret));
  } else if (OB_FAIL(arg_->get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pkey_, ObBackupMetaType::SSTABLE_METAS, meta_index))) {
    STORAGE_LOG(WARN, "fail to get sstable meta", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_sstable_metas(
                 path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, allocator_, sstable_meta_array_))) {
    STORAGE_LOG(WARN, "fail to get sstable meta array", K(ret), K(path), K(meta_index));
  } else if (sstable_meta_array_.count() != table_keys_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "sstable meta array count is not equal table keys array count",
        K(ret),
        K(sstable_meta_array_.count()),
        K(table_keys_array_.count()));
  } else {
    data_size_ += meta_index.data_length_;
  }
  return ret;
}

int ObPartitionMetaPhysicalReader::read_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  partition_store_meta.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg_ is null", K(ret));
  } else if (OB_FAIL(arg_->get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pkey_, ObBackupMetaType::PARTITION_META, meta_index))) {
    STORAGE_LOG(WARN, "fail to get partition meta", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_partition_meta(path.get_obstr(),
                 path_info.dest_.get_storage_info(),
                 meta_index,
                 arg_->restore_info_.compatible_,
                 partition_store_meta))) {
    STORAGE_LOG(WARN, "fail to get partition meta", K(ret), K(path), K(meta_index));
  } else {
    data_size_ += meta_index.data_length_;
  }
  return ret;
}

int ObPartitionMetaPhysicalReader::read_sstable_meta(const uint64_t backup_index_id, ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == backup_index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "backup index id is invalid", K(ret), K(backup_index_id));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < sstable_meta_array_.count(); ++i) {
      if (backup_index_id == sstable_meta_array_.at(i).index_id_) {
        if (OB_FAIL(meta.assign(sstable_meta_array_.at(i)))) {
          STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret));
        } else {
          found = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          WARN, "can not find backup index id sstable meta", K(backup_index_id), K(sstable_meta_array_.count()));
    }
  }

  return ret;
}

int ObPartitionMetaPhysicalReader::read_sstable_pair_list(const uint64_t index_tid, ObIArray<ObSSTablePair>& part_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(macro_indexs_->get_sstable_pair_list(index_tid, part_list))) {
    STORAGE_LOG(WARN, "fail to get sstable pair list", K(ret), K(index_tid));
  }
  return ret;
}

int ObPartitionMetaPhysicalReader::read_table_keys()
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  sstable_meta_array_.reuse();

  if (OB_ISNULL(arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg_ is null", K(ret));
  } else if (OB_FAIL(arg_->get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pkey_, ObBackupMetaType::TABLE_KEYS, meta_index))) {
    STORAGE_LOG(WARN, "fail to get table keys index", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_table_keys(
                 path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, table_keys_array_))) {
    STORAGE_LOG(WARN, "fail to get sstable meta array", K(ret), K(path), K(meta_index));
  } else {
    table_count_ = table_keys_array_.count();
    data_size_ += meta_index.data_length_;
  }

  return ret;
}

int ObPartitionMetaPhysicalReader::read_table_ids(ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else if (get_sstable_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta array count is unexpected", K(ret), K(get_sstable_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sstable_count(); ++i) {
      if (OB_FAIL(table_id_array.push_back(static_cast<uint64_t>(sstable_meta_array_.at(i).index_id_)))) {
        STORAGE_LOG(WARN, "fail to push table id in to array", K(ret), K(sstable_meta_array_.at(i).index_id_));
      }
    }
  }
  return ret;
}

int ObPartitionMetaPhysicalReader::read_table_keys_by_table_id(
    const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table id is invalid", K(ret), K(table_id));
  } else if (table_keys_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "table_keys_array count is unexpected", K(ret), K(table_keys_array_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_keys_array_.count(); ++i) {
      ObITable::TableKey table_key = table_keys_array_.at(i);
      if (table_id == table_key.table_id_) {
        if (OB_FAIL(table_keys_array.push_back(table_key))) {
          STORAGE_LOG(WARN, "fail to put table key into table array", K(ret), K(table_key));
        }
      }
    }
  }
  return ret;
}

/************************ObPGMetaPhysicalReader************************/
ObPGMetaPhysicalReader::ObPGMetaPhysicalReader() : is_inited_(false), data_size_(0), arg_(NULL), meta_indexs_(NULL)
{}

ObPGMetaPhysicalReader::~ObPGMetaPhysicalReader()
{
  reset();
}

void ObPGMetaPhysicalReader::reset()
{
  arg_ = NULL;
  meta_indexs_ = NULL;
  is_inited_ = false;
}

int ObPGMetaPhysicalReader::init(const share::ObPhysicalRestoreArg& arg, const ObPhyRestoreMetaIndexStore& meta_indexs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited", K(ret));
  } else if (!arg.is_valid() || !meta_indexs.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(arg), K(meta_indexs));
  } else {
    arg_ = &arg;
    meta_indexs_ = &meta_indexs;
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaPhysicalReader::read_partition_group_meta(ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  common::ObPartitionKey pgkey;
  pg_meta.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg_ is null", K(ret));
  } else if (OB_FAIL(arg_->get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(arg_->get_backup_pgkey(pgkey))) {
    STORAGE_LOG(WARN, "get backup pgkey fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pgkey, ObBackupMetaType::PARTITION_GROUP_META, meta_index))) {
    STORAGE_LOG(WARN, "fail to get pg meta index", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_partition_group_meta(
                 path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, pg_meta))) {
    STORAGE_LOG(WARN, "fail to get pg meta", K(ret), K(path), K(meta_index));
  } else {
    data_size_ += meta_index.data_length_;
  }
  return ret;
}

int ObPGMetaPhysicalReader::read_backup_pg_meta_info(ObBackupPGMetaInfo& backup_pg_meta_info)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  common::ObPartitionKey pgkey;
  backup_pg_meta_info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg_ is null", K(ret));
  } else if (OB_FAIL(arg_->get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(arg_->get_backup_pgkey(pgkey))) {
    STORAGE_LOG(WARN, "get backup pgkey fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pgkey, ObBackupMetaType::PARTITION_GROUP_META_INFO, meta_index))) {
    STORAGE_LOG(WARN, "fail to get pg meta index", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_backup_pg_meta_info(
                 path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, backup_pg_meta_info))) {
    STORAGE_LOG(WARN, "fail to get pg meta", K(ret), K(path), K(meta_index));
  } else {
    data_size_ += meta_index.data_length_;
  }
  return ret;
}

/************************ObPartitionBaseDataMetaRestoreReaderV1************************/

ObPartitionBaseDataMetaRestoreReaderV1::ObPartitionBaseDataMetaRestoreReaderV1()
    : is_inited_(false),
      pkey_(),
      restore_info_(NULL),
      reader_(),
      bandwidth_throttle_(NULL),
      last_read_size_(0),
      partition_store_meta_(),
      data_version_(0)
{}

ObPartitionBaseDataMetaRestoreReaderV1::~ObPartitionBaseDataMetaRestoreReaderV1()
{
  reset();
}

void ObPartitionBaseDataMetaRestoreReaderV1::reset()
{
  is_inited_ = false;
  pkey_.reset();
  restore_info_ = NULL;
  reader_.reset();
  bandwidth_throttle_ = NULL;
  last_read_size_ = 0;
  partition_store_meta_.reset();
  data_version_ = 0;
}

int ObPartitionBaseDataMetaRestoreReaderV1::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const common::ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info,
    const ObPhyRestoreMetaIndexStore& meta_indexs, const ObPhyRestoreMacroIndexStore& macro_indexs)
{
  int ret = OB_SUCCESS;
  ObPartitionKey src_pkey;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!pkey.is_valid() || !restore_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(pkey), K(restore_info));
  } else if (OB_FAIL(restore_info.change_dst_pkey_to_src_pkey(pkey, src_pkey))) {
    STORAGE_LOG(WARN, "failed to change dst pkey to src pkey", K(ret), K(pkey), K(restore_info));
  } else if (OB_FAIL(reader_.init(restore_info, meta_indexs, macro_indexs, src_pkey))) {
    STORAGE_LOG(WARN, "failed to init meta reader", K(ret), K(src_pkey));
  } else if (OB_FAIL(prepare(pkey))) {
    STORAGE_LOG(WARN, "fail to prepare partition meta", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    last_read_size_ = 0;
    data_version_ = restore_info.restore_data_version_;
    is_inited_ = true;
  }

  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPGPartitionStoreMeta part_meta;
  uint64_t backup_table_id = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(reader_.read_partition_meta(part_meta))) {
    STORAGE_LOG(WARN, "failed to read partition meta", K(ret));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(pkey_.get_table_id(), backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans to backup table id", K(ret), K(pkey_), K(backup_table_id));
  } else if (OB_UNLIKELY(part_meta.pkey_.table_id_ != backup_table_id)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR,
        "backup table id not match",
        K(ret),
        K(part_meta.pkey_.table_id_),
        "backup_table_id",
        backup_table_id,
        K(part_meta),
        K(*restore_info_));
  } else if (OB_FAIL(partition_store_meta.deep_copy(partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(partition_store_meta_));
  } else if (OB_SUCCESS != (tmp_ret = ObRestoreFileUtil::limit_bandwidth_and_sleep(
                                *bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
    STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to fetch partition meta", K(partition_store_meta));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::fetch_sstable_meta(
    const uint64_t index_id, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t backup_index_id = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(index_id, backup_index_id))) {
    STORAGE_LOG(WARN, "failed to trans_from_backup_index_id", K(ret), K(index_id), K(*restore_info_));
  } else if (OB_FAIL(reader_.read_sstable_meta(backup_index_id, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(index_id), K(backup_index_id));
  } else {
    // physical restore, use backup meta info
    sstable_meta.index_id_ = index_id;
    sstable_meta.data_version_ = data_version_;
    sstable_meta.available_version_ = data_version_;
    if (sstable_meta.sstable_format_version_ >= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_5) {
      sstable_meta.logical_data_version_ = sstable_meta.logical_data_version_ < sstable_meta.data_version_
                                               ? sstable_meta.data_version_
                                               : sstable_meta.logical_data_version_;
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "not support restore sstable version", K(ret), K(sstable_meta));
      sstable_meta.reset();
    }

    if (OB_SUCCESS != (tmp_ret = ObRestoreFileUtil::limit_bandwidth_and_sleep(
                           *bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
      STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::fetch_sstable_pair_list(
    const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t backup_index_id = 0;
  ObArray<blocksstable::ObSSTablePair> backup_pair_list;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (index_id == 0 || common::OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(index_id));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(index_id, backup_index_id))) {
    STORAGE_LOG(WARN, "fail to get backup index id", K(ret), K(index_id), K(backup_index_id));
  } else if (OB_FAIL(reader_.read_sstable_pair_list(backup_index_id, backup_pair_list))) {
    STORAGE_LOG(WARN, "fail to read sstable pair list", K(ret), K(backup_index_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pair_list.count(); ++i) {
      blocksstable::ObSSTablePair pair = backup_pair_list.at(i);
      if (pair.data_seq_ < 0) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "backup data data_seq is invalid", K(ret), K(pair));
      } else {
        pair.data_seq_ = i;
        pair.data_version_ = data_version_;
        if (OB_FAIL(pair_list.push_back(pair))) {
          STORAGE_LOG(WARN, "fail to push sstable pair into pair list", K(ret));
        }
      }
    }
    if (OB_SUCCESS != (tmp_ret = ObRestoreFileUtil::limit_bandwidth_and_sleep(
                           *bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
      STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::prepare(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* partition_storage = NULL;

  if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    STORAGE_LOG(WARN, "partition_group is null", K(ret), K(pkey));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard))) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg_partition is null ", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_storage = reinterpret_cast<ObPartitionStorage*>(
                           pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the partition storage is NULL", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().get_pg_partition_store_meta(
                 pkey, partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> backup_table_id_array;
  uint64_t index_id = 0;
  if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_ISNULL(restore_info_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "restore_info_ must not null", K(ret), KP(restore_info_));
  } else if (OB_FAIL(reader_.read_table_ids(backup_table_id_array))) {
    STORAGE_LOG(WARN, "fail to read table ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_table_id_array.count(); ++i) {
      if (OB_FAIL(restore_info_->trans_from_backup_schema_id(backup_table_id_array.at(i), index_id))) {
        STORAGE_LOG(
            WARN, "failed to trans_from_backup_index_id", K(ret), K(backup_table_id_array.at(i)), K(*restore_info_));
      } else if (OB_FAIL(table_id_array.push_back(index_id))) {
        STORAGE_LOG(WARN, "fail to push index id into array", K(ret), K(index_id));
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV1::fetch_table_keys(
    const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    uint64_t backup_index_id = 0;
    if (OB_FAIL(restore_info_->trans_to_backup_schema_id(index_id, backup_index_id))) {
      STORAGE_LOG(WARN, "failed to trans_from_backup_index_id", K(ret), K(index_id), K(*restore_info_));
    } else if (OB_FAIL(reader_.read_table_keys_by_table_id(backup_index_id, table_res.table_keys_))) {
      STORAGE_LOG(WARN, "fail to get table keys", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_res.table_keys_.count(); ++i) {
        ObITable::TableKey& table_key = table_res.table_keys_.at(i);
        if (!table_key.is_major_sstable() || 0 != table_key.trans_version_range_.base_version_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table key has wrong table type", K(ret), K(table_key));
        } else {
          // physical restore, use backup meta info
          table_key.pkey_ = pkey_;
          table_key.table_id_ = index_id;
          table_key.version_ = data_version_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    table_res.is_ready_for_read_ = true;
    table_res.multi_version_start_ = ObTimeUtility::current_time();
  }

  return ret;
}

/********************ObPhysicalBaseMetaRestoreReaderV1*********************/

ObPhysicalBaseMetaRestoreReaderV1::ObPhysicalBaseMetaRestoreReaderV1()
    : is_inited_(false),
      restore_info_(NULL),
      reader_(NULL),
      allocator_(common::ObModIds::RESTORE),
      bandwidth_throttle_(NULL),
      table_key_()

{}

int ObPhysicalBaseMetaRestoreReaderV1::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObPhysicalRestoreArg& restore_info, const ObITable::TableKey& table_key,
    ObIPartitionGroupMetaRestoreReader& reader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (!restore_info.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table is is invalid", K(ret), K(restore_info), K(table_key));
  } else {
    is_inited_ = true;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    table_key_ = table_key;
    reader_ = &reader;
  }
  return ret;
}

int ObPhysicalBaseMetaRestoreReaderV1::fetch_sstable_meta(ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_meta(table_key_, sstable_meta))) {
    STORAGE_LOG(WARN, "fail to get sstable meta", K(ret), K(table_key_));
  } else if (OB_UNLIKELY(!sstable_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta is invalid", K(ret), K(sstable_meta));
  }
  return ret;
}

int ObPhysicalBaseMetaRestoreReaderV1::fetch_macro_block_list(ObIArray<ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_pair_list(table_key_, macro_block_list))) {
    STORAGE_LOG(WARN, "fail to fetch sstable pair list", K(ret), K(table_key_));
  }
  return ret;
}

/******ObPartitionMacroBlockRestoreReaderV1************************/

ObPartitionMacroBlockRestoreReaderV1::ObPartitionMacroBlockRestoreReaderV1()
    : is_inited_(false),
      macro_list_(),
      macro_idx_(0),
      read_size_(0),
      table_id_(OB_INVALID_ID),
      backup_path_info_(),
      macro_indexs_(nullptr),
      bandwidth_throttle_(nullptr),
      backup_index_id_(OB_INVALID_ID),
      backup_pgkey_(),
      allocator_()
{}

ObPartitionMacroBlockRestoreReaderV1::~ObPartitionMacroBlockRestoreReaderV1()
{}

int ObPartitionMacroBlockRestoreReaderV1::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const ObPhysicalRestoreArg& restore_info,
    const ObPhyRestoreMacroIndexStore& macro_indexs, const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_UNLIKELY(!restore_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(restore_info));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table key is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(restore_info.get_backup_base_data_info(backup_path_info_))) {
    LOG_WARN("failed to get backup_base_data_info", K(ret));
  } else if (OB_FAIL(restore_info.get_backup_pgkey(backup_pgkey_))) {
    LOG_WARN("failed to get backup pgkey", K(ret));
  } else if (OB_FAIL(restore_info.trans_to_backup_schema_id(table_key.table_id_, backup_index_id_))) {
    STORAGE_LOG(WARN, "failed to get backup block info", K(ret), K(table_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      if (OB_FAIL(macro_list_.push_back(list.at(i).fetch_arg_))) {
        LOG_WARN("failed to add macro list", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    macro_idx_ = 0;
    read_size_ = 0;
    table_id_ = table_key.table_id_;
    macro_indexs_ = &macro_indexs;
    bandwidth_throttle_ = &bandwidth_throttle;
  }
  return ret;
}

int ObPartitionMacroBlockRestoreReaderV1::get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta,
    blocksstable::ObBufferReader& data, blocksstable::MacroBlockId& src_macro_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupMacroIndex macro_index;
  ObMacroBlockSchemaInfo* new_schema = nullptr;
  ObMacroBlockMetaV2* new_meta = nullptr;

  allocator_.reuse();
  src_macro_id.reset();  // src_macro_id only used for fast migrator on ofs

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (macro_idx_ >= macro_list_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(macro_indexs_->get_macro_index(
                 backup_index_id_, macro_list_.at(macro_idx_).macro_block_index_, macro_index))) {
    STORAGE_LOG(WARN, "fail to get table keys index", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_file_path(backup_path_info_,
                 backup_pgkey_.get_table_id(),
                 backup_pgkey_.get_partition_id(),
                 macro_index.backup_set_id_,
                 macro_index.sub_task_id_,
                 path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_macroblock_data(path.get_obstr(),
                 backup_path_info_.dest_.get_storage_info(),
                 macro_index,
                 allocator_,
                 new_schema,
                 new_meta,
                 data))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(trans_macro_block(table_id_, *new_meta, data))) {
    STORAGE_LOG(WARN, "failed to trans_macro_block", K(ret));
  } else {
    meta.schema_ = new_schema;
    meta.meta_ = new_meta;
    read_size_ += macro_index.data_length_;
    ++macro_idx_;
  }

  return ret;
}

int ObPartitionMacroBlockRestoreReaderV1::trans_macro_block(
    const uint64_t table_id, blocksstable::ObMacroBlockMetaV2& meta, blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), K(meta));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data", K(ret), K(data));
  } else if (data.length() < common_header.get_serialize_size()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "data buf not enough for ObMacroBlockCommonHeader",
        K(ret),
        K(data),
        "need_size",
        common_header.get_serialize_size());
  } else if (OB_FAIL(common_header.deserialize(data.data(), data.length(), pos))) {
    STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), K(data), K(pos), K(common_header));
  } else if (data.length() + pos < sizeof(ObSSTableMacroBlockHeader)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "data buf not enough for ObSSTableMacroBlockHeader",
        K(ret),
        K(data),
        "need_size",
        sizeof(ObSSTableMacroBlockHeader));
  } else {
    ObSSTableMacroBlockHeader* sstable_macro_block_header =
        reinterpret_cast<ObSSTableMacroBlockHeader*>(data.data() + pos);
    meta.table_id_ = table_id;
    sstable_macro_block_header->table_id_ = meta.table_id_;
    sstable_macro_block_header->data_version_ = meta.data_version_;
    sstable_macro_block_header->data_seq_ = meta.data_seq_;
  }

  return ret;
}

/******************ObPartitionGroupMetaRestoreReaderV1******************/

ObPartitionGroupMetaRestoreReaderV1::ObPartitionGroupMetaRestoreReaderV1()
    : is_inited_(false),
      restore_info_(NULL),
      meta_indexs_(NULL),
      reader_(),
      pg_meta_(),
      bandwidth_throttle_(NULL),
      last_read_size_(0),
      allocator_()
{}

ObPartitionGroupMetaRestoreReaderV1::~ObPartitionGroupMetaRestoreReaderV1()
{
  reset();
}

void ObPartitionGroupMetaRestoreReaderV1::reset()
{
  is_inited_ = false;
  restore_info_ = NULL;
  meta_indexs_ = NULL;
  reader_.reset();
  pg_meta_.reset();
  bandwidth_throttle_ = NULL;
  last_read_size_ = 0;
  for (MetaReaderMap::iterator iter = partition_reader_map_.begin(); iter != partition_reader_map_.end(); ++iter) {
    ObPartitionBaseDataMetaRestoreReaderV1* meta_reader = iter->second;
    if (NULL != meta_reader) {
      meta_reader->~ObPartitionBaseDataMetaRestoreReaderV1();
    }
  }
  partition_reader_map_.clear();
  allocator_.reset();
}

int ObPartitionGroupMetaRestoreReaderV1::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMetaIndexStore& meta_indexs,
    const ObPhyRestoreMacroIndexStore& macro_indexs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!restore_info.is_valid() || !meta_indexs.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(restore_info), K(meta_indexs));
  } else if (OB_FAIL(reader_.init(restore_info, meta_indexs))) {
    STORAGE_LOG(WARN, "failed to init meta reader", K(ret));
  } else {
    meta_indexs_ = &meta_indexs;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    last_read_size_ = 0;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare(bandwidth_throttle, restore_info, meta_indexs, macro_indexs))) {
    STORAGE_LOG(WARN, "failed to prepare", K(ret), K(restore_info));
  } else {
    is_inited_ = true;
    STORAGE_LOG(INFO, "succeed to init partition group restore reader", K(restore_info_));
  }

  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::fetch_partition_group_meta(ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!pg_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cannot restore a invalid partition", K(ret), K(pg_meta_));
  } else if (OB_FAIL(pg_meta.deep_copy(pg_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(pg_meta_));
  }

  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::trans_backup_pgmeta(ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pg_meta_.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_meta_), K(backup_pg_meta));
  } else {
    const ObSavedStorageInfoV2& cur_info = pg_meta_.storage_info_;
    const int64_t cur_schema_version = cur_info.get_data_info().get_schema_version();
    ObSavedStorageInfoV2& backup_info = backup_pg_meta.storage_info_;
    backup_info.get_data_info().set_schema_version(cur_schema_version);
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::read_partition_meta(
    const ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info, ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;

  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObBackupMetaIndex meta_index;
  partition_store_meta.reset();
  if (OB_UNLIKELY(!pkey.is_valid() || !restore_info.is_valid() || NULL == meta_indexs_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(restore_info), KP(meta_indexs_));
  } else if (OB_FAIL(restore_info.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(meta_indexs_->get_meta_index(pkey, ObBackupMetaType::PARTITION_META, meta_index))) {
    STORAGE_LOG(WARN, "fail to get partition meta", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_partition_meta(path.get_obstr(),
                 path_info.dest_.get_storage_info(),
                 meta_index,
                 restore_info_->restore_info_.compatible_,
                 partition_store_meta))) {
    STORAGE_LOG(WARN, "fail to get partition meta", K(ret), K(path), K(meta_index));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::create_pg_partition_if_need(
    const ObPhysicalRestoreArg& restore_info, const ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  ObIPartitionGroup* pg = NULL;
  const ObPartitionKey& pg_key = restore_info.pg_key_;

  if (OB_UNLIKELY(!restore_info.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard))) {
    STORAGE_LOG(WARN, "fail to get restore pg", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group should not be null", K(ret), K(pg_key));
  } else {
    const ObSavedStorageInfoV2& storage_info = backup_pg_meta.storage_info_;
    ObPGPartitionStoreMeta partition_meta;
    bool succ_create_partition = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta.partitions_.count(); ++i) {
      const ObPartitionKey& src_pkey = backup_pg_meta.partitions_.at(i);
      ObPartitionKey dst_pkey;
      ObIPartitionGroupGuard guard;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(restore_info.change_src_pkey_to_dst_pkey(src_pkey, dst_pkey))) {
        STORAGE_LOG(WARN, "get dst pkey fail", K(ret), K(restore_info), K(src_pkey));
      } else if (OB_SUCCESS == (tmp_ret = ObPartitionService::get_instance().get_partition(dst_pkey, guard))) {
        STORAGE_LOG(INFO, "partition exist, not need to create", K(ret), K(dst_pkey));
      } else if (OB_PARTITION_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        STORAGE_LOG(WARN, "restore get partition fail", K(ret), K(dst_pkey));
      } else if (OB_FAIL(read_partition_meta(src_pkey, restore_info, partition_meta))) {
        STORAGE_LOG(WARN, "read restore partition meta fail", K(ret), K(src_pkey));
      } else {
        obrpc::ObCreatePartitionArg arg;
        arg.schema_version_ = storage_info.get_data_info().get_schema_version();
        arg.lease_start_ = partition_meta.create_timestamp_;
        arg.restore_ = REPLICA_RESTORE_DATA;
        const bool is_replay = false;
        const uint64_t unused = 0;
        const bool in_slog_trans = false;
        const uint64_t data_table_id = replace_tenant_id(restore_info.get_tenant_id(), partition_meta.data_table_id_);
        const int64_t multi_version_start = restore_info.restore_info_.restore_snapshot_version_;
        ObTablesHandle sstables_handle;
        if (OB_FAIL(pg->create_pg_partition(dst_pkey,
                multi_version_start,
                data_table_id,
                arg,
                in_slog_trans,
                is_replay,
                unused,
                sstables_handle))) {
          STORAGE_LOG(WARN, "fail to create pg partition", K(ret), K(partition_meta));
        } else {
          succ_create_partition = true;
          sstables_handle.reset();
          STORAGE_LOG(INFO, "succeed to create pg partiton during physical restore", K(dst_pkey), K(partition_meta));
        }
      }
    }

    if (OB_SUCC(ret) && succ_create_partition) {
      pg_meta_.reset();
      if (OB_FAIL(pg->get_pg_storage().get_pg_meta(pg_meta_))) {
        STORAGE_LOG(WARN, "fail to get partition group meta", K(ret), K(pg_key));
      } else if (pg_meta_.partitions_.count() != backup_pg_meta.partitions_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "restore pg partition count is not match", K(ret), K(pg_meta_), K(backup_pg_meta));
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::prepare_pg_meta(
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObPhysicalRestoreArg& restore_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  ObIPartitionGroup* partition_group = NULL;
  ObPartitionGroupMeta backup_pg_meta;
  const ObPartitionKey& pg_key = restore_info.pg_key_;
  if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard))) {
    STORAGE_LOG(WARN, "fail to get pg guard", K(ret), K(pg_key));
  } else if (OB_ISNULL(partition_group = pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group should not be null", K(ret), K(pg_key));
  } else if (OB_FAIL(partition_group->get_pg_storage().get_pg_meta(pg_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition group meta", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_FAIL(reader_.read_partition_group_meta(backup_pg_meta))) {
    STORAGE_LOG(WARN, "failed to read partition meta", K(ret));
  } else if (OB_UNLIKELY(!pg_meta_.is_valid() || !backup_pg_meta.is_valid() ||
                         REPLICA_RESTORE_DATA != pg_meta_.is_restore_ ||
                         REPLICA_NOT_RESTORE != backup_pg_meta.is_restore_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cannot restore a invalid partition", K(ret), K(pg_meta_), K(backup_pg_meta));
  } else if (OB_FAIL(trans_backup_pgmeta(backup_pg_meta))) {
    STORAGE_LOG(WARN, "trans backup pg meta fail", K(ret), K(pg_meta_));
  } else if (OB_FAIL(check_backup_partitions_in_pg(restore_info, backup_pg_meta))) {
    STORAGE_LOG(WARN, "failed to check back partitions in schema", K(ret), K(backup_pg_meta));
  } else if (OB_FAIL(create_pg_partition_if_need(restore_info, backup_pg_meta))) {
    STORAGE_LOG(WARN, "fail to create pg partition", K(ret), K(backup_pg_meta));
  } else if (OB_FAIL(pg_meta_.storage_info_.deep_copy(backup_pg_meta.storage_info_))) {
    STORAGE_LOG(WARN, "restore storage info fail", K(ret), K(pg_meta_), K(backup_pg_meta));
  } else if (OB_SUCCESS != (tmp_ret = ObRestoreFileUtil::limit_bandwidth_and_sleep(
                                bandwidth_throttle, reader_.get_data_size(), last_read_size_))) {
    STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(tmp_ret));
  }

  if (OB_FAIL(ret)) {
    pg_meta_.reset();
  } else {
    STORAGE_LOG(INFO, "fetch restore pg meta succ", K(pg_meta_));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::prepare(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMetaIndexStore& meta_indexs,
    const ObPhyRestoreMacroIndexStore& macro_indexs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_FAIL(prepare_pg_meta(bandwidth_throttle, restore_info))) {
    STORAGE_LOG(WARN, "prepare pg meta fail", K(ret));
  } else {
    const ObPartitionArray& partitions = pg_meta_.partitions_;
    if (0 == partitions.count()) {
      STORAGE_LOG(INFO, "empty pg, no partitions");
    } else if (OB_FAIL(partition_reader_map_.create(partitions.count(), ObModIds::RESTORE))) {
      STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
        const ObPartitionKey& pkey = partitions.at(i);
        ObPartitionBaseDataMetaRestoreReaderV1* reader = NULL;
        void* buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionBaseDataMetaRestoreReaderV1)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
        } else if (OB_ISNULL(reader = new (buf) ObPartitionBaseDataMetaRestoreReaderV1())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc reader", K(ret));
        } else if (OB_FAIL(reader->init(bandwidth_throttle, pkey, restore_info, meta_indexs, macro_indexs))) {
          STORAGE_LOG(WARN, "failed to init ObPartitionGroupMetaRestoreReaderV1", K(ret), K(pkey));
        } else if (OB_FAIL(partition_reader_map_.set_refactored(pkey, reader))) {
          STORAGE_LOG(WARN, "failed to set reader into map", K(ret), K(pkey));
        } else {
          reader = NULL;
        }

        if (NULL != reader) {
          reader->~ObPartitionBaseDataMetaRestoreReaderV1();
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::get_partition_readers(
    const ObPartitionArray& partitions, ObIArray<ObPartitionBaseDataMetaRestoreReaderV1*>& partition_reader_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionKey& pkey = partitions.at(i);
      ObPartitionBaseDataMetaRestoreReaderV1* reader = NULL;
      if (OB_FAIL(partition_reader_map_.get_refactored(pkey, reader))) {
        STORAGE_LOG(WARN, "failed to get partition base data meta restore reader", K(ret), K(pkey));
      } else if (OB_ISNULL(reader)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition base data meta restore reader is NULL", K(ret), K(pkey), KP(reader));
      } else if (OB_FAIL(partition_reader_array.push_back(reader))) {
        STORAGE_LOG(WARN, "failed to push reader into partition reader array", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::fetch_sstable_meta(
    const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  ObPartitionBaseDataMetaRestoreReaderV1* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(table_key.get_partition_key(), reader))) {
    STORAGE_LOG(WARN, "failed to get partition base meta retore reader", K(ret), K(table_key));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition base meta restore reader is NULL", K(ret), K(table_key), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_meta(table_key.table_id_, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(table_key));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::fetch_sstable_pair_list(
    const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::ObSSTablePair> backup_pair_list;
  ObPartitionBaseDataMetaRestoreReaderV1* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(table_key.get_partition_key(), reader))) {
    STORAGE_LOG(WARN, "failed to partition base meta restore reader", K(ret), K(table_key));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition base meta restore reader is NULL", K(ret), K(table_key), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_pair_list(table_key.table_id_, pair_list))) {
    STORAGE_LOG(WARN, "failed to fetch sstable pair list", K(ret), K(table_key));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::check_backup_partitions_in_pg(
    const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> restore_table_ids_set;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  const uint64_t tenant_id = restore_info.pg_key_.get_tenant_id();
  const uint64_t tablegroup_id = restore_info.pg_key_.get_tablegroup_id();
  ObArray<uint64_t> table_ids;
  int64_t max_bucket = 0;
  int64_t tenant_schema_version = 0;

  if (OB_UNLIKELY(!restore_info.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (is_sys_table(restore_info.pg_key_.get_table_id())) {
    // do nothing
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_base_data_restore_schema_version(
                 tenant_id, tenant_schema_version))) {
    STORAGE_LOG(WARN, "failed to  get base data restore schema version", K(ret), K(tenant_id), K(restore_info));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, schema_service, tenant_schema_version, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (!restore_info.pg_key_.is_pg()) {
    if (OB_FAIL(table_ids.push_back(restore_info.pg_key_.get_table_id()))) {
      STORAGE_LOG(WARN, "failed to push table id into array", K(ret), K(restore_info));
    }
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, tablegroup_id, table_ids))) {
    STORAGE_LOG(WARN, "failed to get table ids in tablegroup", K(ret), K(restore_info), K(backup_pg_meta));
  }

  if (OB_FAIL(ret)) {
  } else if (backup_pg_meta.partitions_.empty() && table_ids.empty()) {
    // do nothing
  } else if (FALSE_IT(max_bucket = std::max(backup_pg_meta.partitions_.count(), table_ids.count()))) {
  } else if (OB_FAIL(restore_table_ids_set.create(max_bucket))) {
    STORAGE_LOG(WARN, "failed to create restore table ids set", K(ret), K(max_bucket));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta.partitions_.count(); ++i) {
      const uint64_t backup_table_id = backup_pg_meta.partitions_.at(i).get_table_id();
      uint64_t table_id = 0;
      if (OB_FAIL(restore_info.trans_from_backup_schema_id(backup_table_id, table_id))) {
        STORAGE_LOG(WARN, "get dst table id fail", K(ret), K(restore_info), K(backup_table_id));
      } else if (OB_FAIL(restore_table_ids_set.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "failed to set table id into hash set", K(ret), K(table_id));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        const uint64_t table_id = table_ids.at(i);
        const ObTableSchema* table_schema = NULL;
        int hash_ret = OB_SUCCESS;
        if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
          STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table schema should not be NULL", K(ret), K(table_id), KP(table_schema));
        }

        if (OB_SUCC(ret)) {
          hash_ret = restore_table_ids_set.exist_refactored(table_id);
          if (OB_HASH_EXIST == hash_ret) {
            if (table_schema->is_dropped_schema()) {
              // skip ret, just print log
              STORAGE_LOG(WARN, "restore table should not be dropped schema", K(ret), K(table_id), K(backup_pg_meta));
            }
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            bool need_skip = false;
            if (OB_FAIL(
                    ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(table_schema, need_skip))) {
              STORAGE_LOG(WARN, "failed to check backup restore need skip table", K(ret));
            } else if (!need_skip) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(ERROR, "restore table not in pg, may lost data!", K(ret), K(table_id), K(backup_pg_meta));
            }
          } else {
            ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
            STORAGE_LOG(WARN, "failed to get table id in hash set", K(ret), K(table_id), K(backup_pg_meta));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV1::get_restore_tenant_id(uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader is not init", K(ret));
  } else {
    tenant_id = pg_meta_.pg_key_.get_tenant_id();
  }
  return ret;
}

/**********************ObPGPartitionBaseDataMetaRestoreReaderV1**********************/
ObPGPartitionBaseDataMetaRestoreReaderV1::ObPGPartitionBaseDataMetaRestoreReaderV1()
    : is_inited_(false), reader_index_(0), partition_reader_array_(), schema_version_(0)
{}

ObPGPartitionBaseDataMetaRestoreReaderV1::~ObPGPartitionBaseDataMetaRestoreReaderV1()
{
  reset();
}

void ObPGPartitionBaseDataMetaRestoreReaderV1::reset()
{
  is_inited_ = false;
  reader_index_ = 0;
  partition_reader_array_.reset();
  schema_version_ = 0;
}

int ObPGPartitionBaseDataMetaRestoreReaderV1::init(
    const ObPartitionArray& partitions, ObPartitionGroupMetaRestoreReaderV1* reader)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(reader));
  } else if (OB_FAIL(reader->get_partition_readers(partitions, partition_reader_array_))) {
    STORAGE_LOG(WARN, "failed to get partition readers", K(ret));
  } else if (OB_FAIL(reader->get_restore_tenant_id(tenant_id))) {
    STORAGE_LOG(WARN, "failed to get restore tenant id", K(ret));
  } else if (OB_FAIL(
                 ObBackupInfoMgr::get_instance().get_base_data_restore_schema_version(tenant_id, schema_version_))) {
    STORAGE_LOG(WARN, "failed to get base data restore schema version", K(ret), K(tenant_id));
  } else {
    reader_index_ = 0;
    is_inited_ = true;
    STORAGE_LOG(INFO, "succeed to init fetch_pg partition_meta_info", K(partition_reader_array_.count()));
  }

  return ret;
}

int ObPGPartitionBaseDataMetaRestoreReaderV1::fetch_pg_partition_meta_info(
    obrpc::ObPGPartitionMetaInfo& partition_meta_info)
{
  int ret = OB_SUCCESS;
  partition_meta_info.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else if (reader_index_ == partition_reader_array_.count()) {
    ret = OB_ITER_END;
  } else {
    ObPartitionBaseDataMetaRestoreReaderV1* reader = partition_reader_array_.at(reader_index_);
    if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition base data meta restore reader is NULL", K(ret), KP(reader), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_partition_meta(partition_meta_info.meta_))) {
      STORAGE_LOG(WARN, "failed to fetch partition meta", K(ret), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_all_table_ids(partition_meta_info.table_id_list_))) {
      STORAGE_LOG(WARN, "failed to fetch all table ids", K(ret), K(reader_index_));
    } else if (OB_FAIL(check_sstable_table_ids_in_table(
                   partition_meta_info.meta_.pkey_, partition_meta_info.table_id_list_))) {
      STORAGE_LOG(WARN, "failed to check table ids in schema", K(ret), K(partition_meta_info));
    } else {
      obrpc::ObFetchTableInfoResult table_info_result;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_meta_info.table_id_list_.count(); ++i) {
        table_info_result.reset();
        const uint64_t index_id = partition_meta_info.table_id_list_.at(i);
        if (OB_FAIL(reader->fetch_table_keys(index_id, table_info_result))) {
          STORAGE_LOG(WARN, "failed to fetch table keys", K(ret), K(index_id));
        } else if (OB_FAIL(partition_meta_info.table_info_.push_back(table_info_result))) {
          STORAGE_LOG(WARN, "failed to push table info result into array", K(ret), K(index_id));
        }
      }
      if (OB_SUCC(ret)) {
        reader_index_++;
      }
    }
  }
  return ret;
}

int ObPGPartitionBaseDataMetaRestoreReaderV1::check_sstable_table_ids_in_table(
    const ObPartitionKey& pkey, const common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> table_ids_set;
  const uint64_t data_table_id = pkey.get_table_id();
  const uint64_t tenant_id = pkey.get_tenant_id();
  ObArray<ObIndexTableStat> index_stats;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else if (!pkey.is_valid() || table_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check table ids in schema get invalid argument", K(ret), K(pkey), K(table_ids));
  } else if (is_sys_table(data_table_id)) {
    // do nothing
  } else if (OB_FAIL(table_ids_set.create(table_ids.count()))) {
    STORAGE_LOG(WARN, "failed to create table ids set", K(ret));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, schema_service, schema_version_, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(tenant_id), K(schema_version_));
  } else if (OB_FAIL(schema_guard.get_index_status(data_table_id, false /*with global index*/, index_stats))) {
    STORAGE_LOG(WARN, "failed to get index status", K(ret), K(data_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      if (OB_FAIL(table_ids_set.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "can not set table id into set", K(ret), K(table_id));
      }
    }

    // data table or global index
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_sstable_ids_contain_schema_table_id(table_ids_set, data_table_id, schema_guard))) {
        STORAGE_LOG(WARN, "failed to check sstable ids contain schema table id", K(ret), K(data_table_id));
      }
    }

    // local index table
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_stats.count(); ++i) {
        const ObIndexTableStat& index_stat = index_stats.at(i);
        const uint64_t index_table_id = index_stat.index_id_;
        if (OB_FAIL(check_sstable_ids_contain_schema_table_id(table_ids_set, index_table_id, schema_guard))) {
          STORAGE_LOG(WARN, "failed to check sstable ids contain schema table id", K(ret), K(index_table_id));
        }
      }
    }
  }
  return ret;
}

int ObPGPartitionBaseDataMetaRestoreReaderV1::check_sstable_ids_contain_schema_table_id(
    const hash::ObHashSet<uint64_t>& table_ids_set, const uint64_t schema_table_id,
    schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else {
    bool need_skip = false;
    int hash_ret = table_ids_set.exist_refactored(schema_table_id);
    if (OB_HASH_EXIST == hash_ret) {
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(schema_table_id, table_schema))) {
        STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(schema_table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table schema should not be NULL", K(ret), KP(table_schema));
      } else if (OB_FAIL(ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(
                     table_schema, need_skip))) {
        STORAGE_LOG(WARN, "failed to check backup restore need skip table", K(ret));
      } else if (!need_skip) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "restore index table in schema, but not in partition, may los data!",
            K(ret),
            K(schema_table_id),
            K(*table_schema));
      }
    } else {
      ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
      STORAGE_LOG(WARN, "can not check table id in hash set", K(ret), K(table_ids_set));
    }
  }
  return ret;
}

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
#include "ob_admin_dump_backup_data_executor.h"
#include "storage/ob_partition_base_data_backup.h"
#include "../archive_tool/ob_admin_log_archive_executor.h"
#include "share/ob_encrypt_kms.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase
{
namespace tools
{

int ObAdminDumpBackupDataReaderUtil::get_data_type(
    const char *data_path,
    const char *storage_info,
    ObBackupFileType &data_type)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false/*need retry*/);
  int64_t need_size = sizeof(uint16_t);
  data_type = BACKUP_TYPE_MAX;

  if (OB_ISNULL(data_path) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup data common header get invalid argument", K(ret), KP(data_path), KP(storage_info));
  } else if (OB_FAIL(util.get_file_length(data_path,
      storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get backup file length", K(ret), K(data_path), K(storage_info));
  } else if (file_length < need_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file length too small", K(ret), K(need_size), K(file_length), K(data_path), K(storage_info));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(need_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(need_size), K(data_path), K(storage_info));
  } else if (OB_FAIL(util.read_part_file(data_path, storage_info,
      buf, need_size, 0 /*offset*/, read_size))) {
    STORAGE_LOG(WARN, "failed to read single file", K(ret), K(file_length), K(need_size), K(read_size));
  } else if (need_size != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match", K(ret), K(file_length), K(need_size), K(read_size), K(data_path), K(storage_info));
  } else {
    uint16_t tmp_uint16 = *(reinterpret_cast<uint16_t *>(buf));
    int16_t tmp_int16 = 0;
    if (tmp_uint16 <= BACKUP_MAX_DIRECT_WRITE_TYPE) {
      data_type = static_cast<ObBackupFileType>(tmp_uint16);
    } else {
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i16(buf, need_size, pos, &tmp_int16))) {
        LOG_WARN("failed to decode_i16", K(ret), K(need_size), K(pos));
      } else {
        data_type = static_cast<ObBackupFileType>(tmp_int16);
      }
    }
    STORAGE_LOG(INFO, "succeed to get data type", K(data_path), K(storage_info), K(data_type),
        K(tmp_int16), K(tmp_uint16));
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_backup_data_common_header(
    const char *data_dir, const char *storage_info,
    ObBackupCommonHeader &header)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(false/*need retry*/);
  const int64_t common_header_size = sizeof(ObBackupCommonHeader);
  const int64_t offset = 0;

  if (OB_ISNULL(data_dir) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup data common header get invalid argument", K(ret), KP(data_dir), KP(storage_info));
  } else if (OB_FAIL(util.get_file_length(data_dir,
      storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get backup file length", K(ret), K(data_dir), K(storage_info));
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file length should not be empty", K(ret), K(data_dir), K(storage_info));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(common_header_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(common_header_size), K(data_dir), K(storage_info));
  } else if (OB_FAIL(util.read_part_file(data_dir, storage_info,
      buf, common_header_size, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to read single file", K(ret), K(file_length), K(common_header_size), K(read_size));
  } else if (common_header_size != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match", K(ret), K(file_length), K(common_header_size), K(read_size), K(data_dir), K(storage_info));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader *common_header =
        reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      STORAGE_LOG(WARN, "failed to check common header", K(ret));
    } else {
      header = *common_header;
    }
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_backup_data(
    const char *data_dir,
    const char *storage_info,
    char *&buf,
    int64_t &file_length,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  buf = NULL;
  int64_t read_size = 0;
  ObStorageUtil util(false/*need retry*/);
  const int64_t offset = 0;

  if (OB_ISNULL(data_dir) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup data get invalid argument", K(ret), KP(data_dir), KP(storage_info));
  } else if (OB_FAIL(util.get_file_length(data_dir,
      storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get backup file length", K(ret), K(data_dir), K(storage_info));
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file length should not be empty", K(ret), K(data_dir), K(storage_info));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(file_length), K(data_dir), K(storage_info));
  } else if (OB_FAIL(util.read_part_file(data_dir, storage_info,
      buf, file_length, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to read single file", K(ret), K(file_length), K(read_size));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match", K(ret), K(file_length), K(read_size), K(data_dir), K(storage_info));
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_backup_data(
    const char *data_dir,
    const char *storage_info,
    char *&buf,
    const int64_t offset,
    const int64_t data_length,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  buf = NULL;
  int64_t read_size = 0;
  ObStorageUtil util(false/*need retry*/);

  if (OB_ISNULL(data_dir) || OB_ISNULL(storage_info) || data_length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup data get invalid argument", K(ret), KP(data_dir), KP(storage_info), K(data_length));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(data_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(data_length), K(data_dir), K(storage_info));
  } else if (OB_FAIL(util.read_part_file(data_dir, storage_info,
      buf, data_length, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to read single file", K(ret), K(data_length), K(read_size));
  } else if (data_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match", K(ret), K(data_length), K(read_size), K(data_dir), K(storage_info));
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_meta_index(
    blocksstable::ObBufferReader &buffer_reader,
    share::ObBackupCommonHeader &header,
    common::ObIArray<ObBackupMetaIndex> &meta_index_array)
{
  int ret = OB_SUCCESS;

  const ObBackupCommonHeader *common_header = NULL;
  ObBackupMetaIndex meta_index;

  if (OB_FAIL(buffer_reader.get(common_header))) {
    STORAGE_LOG(WARN, "read meta index common header fail", K(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta index common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
  } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
    STORAGE_LOG(INFO, "file reach the end mark, ", K(*common_header));
  } else if (common_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
  } else if (OB_FAIL(common_header->check_data_checksum(
      buffer_reader.current(),
      common_header->data_length_))) {
    STORAGE_LOG(WARN, "commonheader data checksum fail", K(ret));
  } else {
    int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
    for (int64_t i=0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
      if (OB_FAIL(buffer_reader.read_serialize(meta_index))) {
        STORAGE_LOG(WARN, "read meta index fail", K(ret));
      } else if (OB_FAIL(meta_index_array.push_back(meta_index))) {
        STORAGE_LOG(WARN, "failed to push meta index into array", K(ret), K(meta_index));
      }
    }

    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
      }
    }
  }

  if (OB_SUCC(ret)) {
    header = *common_header;
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_meta_header(
    const char *read_buf,
    const int64_t read_size,
    ObBackupMetaHeader &header)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(read_buf) || read_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup meta index get invalid argument", K(ret), KP(read_buf), K(read_size));
  } else {
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader *meta_header = NULL;
    int64_t pos = 0;
    if (OB_FAIL(buffer_reader.deserialize(read_buf, read_size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret));
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
    } else if (OB_FAIL(meta_header->check_data_checksum(
        buffer_reader.current(),
        meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
    } else {
      header = *meta_header;
    }
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_macro_block_index(
    ObBufferReader &buffer_reader,
    share::ObBackupCommonHeader &header,
    common::ObIArray<ObBackupMacroIndex> &macro_index_array)
{
  int ret = OB_SUCCESS;

  const ObBackupCommonHeader *common_header = NULL;
  ObBackupMacroIndex macro_index;

  if (OB_FAIL(buffer_reader.get(common_header))) {
    STORAGE_LOG(WARN, "read macro index common header fail", K(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro index common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
  } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
    STORAGE_LOG(INFO, "file reach the end mark, ", K(buffer_reader));
  } else if (common_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
  } else if (OB_FAIL(common_header->check_data_checksum(
      buffer_reader.current(),
      common_header->data_length_))) {
    STORAGE_LOG(WARN, "common header data checksum fail", K(ret));
  } else {
    int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
    for (int i=0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
      if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
        STORAGE_LOG(WARN, "read macro index fail", K(ret));
      } else if (OB_FAIL(macro_index_array.push_back(macro_index))) {
        STORAGE_LOG(WARN, "failed to push macro index into array", K(ret), K(macro_index));
      }
    }

    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
      }
    }
  }

  if (OB_SUCC(ret)) {
    header = *common_header;
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_macro_block_index_v2(
    blocksstable::ObBufferReader &buffer_reader,
    common::ObIAllocator &allocator,
    TableMacroIndexMap &index_map)
{
  int ret = OB_SUCCESS;
  const ObBackupCommonHeader *common_header = NULL;
  ObBackupTableMacroIndex macro_index;
  ObArray<ObBackupTableMacroIndex> index_list;
  ObITable::TableKey cur_table_key;
  ObBackupTableKeyInfo table_key_info;
  int64_t sstable_macro_block_index = -1;
  ObArray<ObITable::TableKey *> table_keys_ptr;
  const ObITable::TableKey *table_key_ptr = NULL;
  while(OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    if (OB_FAIL(buffer_reader.get(common_header))) {
      STORAGE_LOG(WARN, "read macro index common header fail", K(ret));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro index common header is null", K(ret));
    } else if (OB_FAIL(common_header->check_valid())) {
      STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
    } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
      STORAGE_LOG(INFO, "file reach the end mark", K(ret));
      break;
    } else if (common_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
    } else if (OB_FAIL(common_header->check_data_checksum(
        buffer_reader.current(), common_header->data_length_))) {
      STORAGE_LOG(WARN, "common header data checksum fail", K(ret), K(*common_header));
    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      while (OB_SUCC(ret) && buffer_reader.pos() < end_pos) {
        if (-1 == sstable_macro_block_index) {
          int64_t start_pos = buffer_reader.pos();
          if (OB_FAIL(buffer_reader.read_serialize(table_key_info))) {
            STORAGE_LOG(WARN, "failed to read backup table key meta", K(ret),
                K(start_pos), K(end_pos), K(*common_header));
          } else if (0 == table_key_info.total_macro_block_count_) {
            sstable_macro_block_index = -1;
            table_key_info.reset();
          } else {
            sstable_macro_block_index = 0;
          }
        } else {
          if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
            STORAGE_LOG(WARN, "read macro index fail", K(ret));
          } else if (OB_FAIL(get_table_key_ptr(allocator, table_keys_ptr, table_key_info.table_key_, table_key_ptr))) {
            STORAGE_LOG(WARN, "failed to get table key ptr", K(ret), K(table_key_info));
          } else if (FALSE_IT(macro_index.table_key_ptr_ = table_key_ptr)) {
          } else if (!macro_index.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "macro index is invalid", K(ret), K(macro_index));
          } else {
            sstable_macro_block_index = macro_index.sstable_macro_index_;
            if (0 == index_list.count()) {
              if (OB_FAIL(index_list.push_back(macro_index))) {
                STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
              }
              cur_table_key = *macro_index.table_key_ptr_;
            } else if (cur_table_key == *macro_index.table_key_ptr_) {
              if (OB_FAIL(index_list.push_back(macro_index))) {
                STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
              }
            } else {
              if (OB_FAIL(add_sstable_index(cur_table_key, index_list, allocator, index_map))) {
                STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_table_key));
              } else {
                index_list.reuse();
                cur_table_key = *macro_index.table_key_ptr_;
                if (OB_FAIL(index_list.push_back(macro_index))) {
                  STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                }
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (table_key_info.total_macro_block_count_ - 1 == sstable_macro_block_index) {
              sstable_macro_block_index = -1;
              table_key_info.reset();
            }
          }
        }
      }

      if (OB_SUCC(ret) && index_list.count() > 0 ) {
        if (OB_FAIL(add_sstable_index(cur_table_key, index_list, allocator, index_map))) {
          STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_table_key));
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
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::add_sstable_index(
    const ObITable::TableKey &table_key,
    const common::ObIArray<ObBackupTableMacroIndex> &index_list,
    common::ObIAllocator &allocator,
    TableMacroIndexMap &index_map)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObArray<ObBackupTableMacroIndex> *new_block_list = NULL;
  int64_t block_count = index_list.count();

  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "add sstable index get invalid argument", K(ret), K(table_key));
  } else if (index_list.empty()) {
    //do nothing
  } else if (OB_FAIL(index_map.get_refactored(table_key, new_block_list))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObArray<ObBackupTableMacroIndex>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
      } else if (OB_ISNULL(new_block_list = new(buf) ObArray<ObBackupTableMacroIndex>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to new block list", K(ret));
      } else if (OB_FAIL(index_map.set_refactored(table_key, new_block_list, 1 /*rewrite*/))) {
        STORAGE_LOG(WARN, "failed to set macro index map", K(ret));
        new_block_list->~ObArray();
      }
    } else {
      STORAGE_LOG(WARN, "get old block list fail", K(ret), K(table_key));
    }
  } else if (OB_ISNULL(new_block_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exist block list should not be null here", K(ret), K(table_key));
  }

  for (int i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupTableMacroIndex &index = index_list.at(i);//check
    if (index.sstable_macro_index_ < new_block_list->count()) {
      // skip
    } else if (OB_UNLIKELY(index.sstable_macro_index_ != new_block_list->count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sstable macro index is not continued", K(ret),
          K(table_key), K(index), "new_block_list_count", new_block_list->count(),
          K(*new_block_list));
    } else if (OB_FAIL(new_block_list->push_back(index))) {
      STORAGE_LOG(WARN, "failed to push back block index", K(ret), K(i));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataReaderUtil::get_table_key_ptr(
    common::ObIAllocator &allocator,
    ObArray<ObITable::TableKey *> &table_keys_ptr,
    const ObITable::TableKey &table_key,
    const ObITable::TableKey *&table_key_ptr)
{
  int ret = OB_SUCCESS;
  table_key_ptr = NULL;
  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get table key ptr get invalid argument", K(ret), K(table_key));
  } else {
    bool found = false;
    for (int64_t i = 0; i < table_keys_ptr.count() && !found; ++i) {
      const ObITable::TableKey &tmp_table_key = *table_keys_ptr.at(i);
      if (tmp_table_key == table_key) {
        table_key_ptr = table_keys_ptr.at(i);
        found = true;
      }
    }

    if (!found) {
      void *buf = allocator.alloc(sizeof(ObITable::TableKey));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else {
        ObITable::TableKey *tmp_table_key_ptr = new(buf) ObITable::TableKey();
        *tmp_table_key_ptr = table_key;
        if (OB_FAIL(table_keys_ptr.push_back(tmp_table_key_ptr))) {
          STORAGE_LOG(WARN, "failed to push table key into array", K(ret), K(*tmp_table_key_ptr));
        } else {
          table_key_ptr = tmp_table_key_ptr;
        }
      }
    }
  }
  return ret;
}

ObAdminDumpBackupDataExecutor::ObAdminDumpBackupDataExecutor()
  : common_header_(),
    allocator_(),
    is_quiet_(false),
    offset_(0),
    data_length_(0),
    check_exist_(false)
{
  MEMSET(data_path_, 0, common::OB_MAX_URI_LENGTH);
  MEMSET(storage_info_, 0, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
}

ObAdminDumpBackupDataExecutor::~ObAdminDumpBackupDataExecutor()
{
}

int ObAdminDumpBackupDataExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  UNUSED(argc);
  UNUSED(argv);
  ObBackupFileType data_type = BACKUP_TYPE_MAX;

  if (OB_SUCC(parse_cmd(argc, argv))) {
    if (is_quiet_) {
      OB_LOGGER.set_log_level("ERROR");
    } else {
      OB_LOGGER.set_log_level("INFO");
    }

    if (OB_SUCC(ret) && check_exist_) {
      if (OB_FAIL(check_exist(data_path_, storage_info_))) {
        STORAGE_LOG(WARN, "check file is exist fail", K(ret));
      }
    }

    if (OB_SUCC(ret) && !check_exist_) {
      if (OB_FAIL(ret)) {
#ifdef _WITH_OSS
      } else if (OB_FAIL(ObOssEnvIniter::get_instance().global_init())) {
        STORAGE_LOG(WARN, "init oss storage fail", K(ret));
#endif
      } else if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_data_type(data_path_, storage_info_, data_type))) {
        STORAGE_LOG(WARN, "failed to get backup data common header", K(ret), K(data_path_), K(storage_info_));
      }
    }
    lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
    lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);
    if (OB_SUCC(ret) && !check_exist_) {
      switch (data_type) {
        case BACKUP_INFO:
          print_backup_info();
          break;
        case BACKUP_TENANT_INFO:
          print_backup_tenant_info();
          break;
        case BACKUP_LOG_ARCHIVE_BACKUP_INFO:
          print_clog_backup_info();
          break;
        case BACKUP_SET_INFO:
          print_backup_set_info();
          break;
        case BACKUP_PG_LIST:
          print_pg_list();
          break;
        case BACKUP_META_INDEX:
          print_meta_index();
          break;
        case BACKUP_META:
          print_meta_data();
          break;
        case BACKUP_MACRO_DATA_INDEX:
          print_macro_data_index();
          break;
        case BACKUP_MACRO_DATA:
          print_macro_block();
          break;
        case BACKUP_TENANT_NAME_INFO:
          print_tenant_name_info();
          break;
        case BACKUP_PIECE_INFO: {
          print_backup_piece_info();
          break;
        }
        case BACKUP_SINGLE_PIECE_INFO: {
          print_backup_single_piece_info();
          break;
        }
        case BACKUP_SET_FILE_INFO: {
          print_backup_set_file_info();
          break;
        }
        case BACKUP_ARCHIVE_BLOCK_META:{
          print_archive_block_meta();
          break;
        }
        case BACKUP_ARCHIVE_INDEX_FILE: {
          print_archive_index_file();
          break;
        }
        default: {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "invalid data type", K(ret), K(data_type), K(data_path_), K(storage_info_));
        }
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  int opt = 0;
  const char* opt_string = "hd:s:o:l:f:q:c";

  struct option longopts[] = {
    // commands
    { "help", 0, NULL, 'h' },
    // options
    { "data_uri", 1, NULL, 'd' },
    { "storage_info", 1, NULL, 's' },
    { "offset", 1, NULL, 'o'},
    { "data_length", 1, NULL, 'l'},
    { "file_uri", 1, NULL , 'f'},
    { "quiet", 0, NULL, 'q' },
    { "check_exist", 0, NULL, 'c'}
  };

  int index = -1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, &index)) != -1) {
    switch (opt) {
    case 'h': {
      print_usage();
      exit(1);
    }
    case 'd': {
      if (OB_FAIL(databuff_printf(data_path_, sizeof(data_path_), "%s", optarg))) {
        STORAGE_LOG(WARN, "failed to printf data_dir", K(ret), K(optarg));
      }
      break;
    }
    case 's': {
      if (OB_FAIL(databuff_printf(storage_info_, share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH, "%s", optarg))) {
        STORAGE_LOG(WARN, "failed to printf storage info", K(ret), K(optarg));
      }
      break;
    }
    case 'o': {
      offset_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'l': {
      data_length_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'c': {
      check_exist_ = true;
      break;
    }
    case 'f': {
      char current_absolute_path[MAX_PATH_SIZE] = "";
      if (optarg[0] != '/') {
        int64_t length = 0;
        if (OB_ISNULL(getcwd(current_absolute_path, MAX_PATH_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "failed to get current absolute path", K(ret), K(current_absolute_path));
        } else if ((length = strlen(current_absolute_path)) >= MAX_PATH_SIZE) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "current absolte path is overflow", K(ret), K(length), K(current_absolute_path));
        } else {
          current_absolute_path[length]= '/';
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(data_path_, sizeof(data_path_), "%s%s%s", OB_FILE_PREFIX, current_absolute_path, optarg))) {
          STORAGE_LOG(WARN, "failed to printf file uri", K(ret), K(optarg));
        } else if (OB_FAIL(databuff_printf(input_data_path_, sizeof(input_data_path_), optarg))) {
          STORAGE_LOG(WARN, "failed to copy input_data_path_", K(ret), K(optarg));
        }
      }
      break;
    }
    case 'q': {
      is_quiet_ = true;
      break;
    }
    default: {
      print_usage();
      exit(1);
    }
    }
  }

  return ret;
}

void ObAdminDumpBackupDataExecutor::print_common_header(
    const share::ObBackupCommonHeader &common_header)
{
  PrintHelper::print_dump_title("Common Header");
  PrintHelper::print_dump_line("data_type", common_header.data_type_);
  PrintHelper::print_dump_line("header_version", common_header.header_version_);
  PrintHelper::print_dump_line("compressor_type", common_header.compressor_type_);
  PrintHelper::print_dump_line("data_type", common_header.data_type_);
  PrintHelper::print_dump_line("data_version", common_header.data_version_);
  PrintHelper::print_dump_line("header_length", common_header.header_length_);
  PrintHelper::print_dump_line("header_checksum", common_header.header_checksum_);
  PrintHelper::print_dump_line("data_length_", common_header.data_length_);
  PrintHelper::print_dump_line("data_zlength", common_header.data_zlength_);
  PrintHelper::print_dump_line("data_checksum", common_header.data_checksum_);
  PrintHelper::print_dump_line("align_length", common_header.align_length_);
  PrintHelper::print_end_line();
}

void ObAdminDumpBackupDataExecutor::print_meta_header(
    const share::ObBackupMetaHeader &meta_header)
{
  PrintHelper::print_dump_title("Meta Header");
  PrintHelper::print_dump_line("header_version", meta_header.header_version_);
  PrintHelper::print_dump_line("meta_type", meta_header.meta_type_);
  PrintHelper::print_dump_line("header_checksum", meta_header.header_checksum_);
  PrintHelper::print_dump_line("data_checksum", meta_header.data_checksum_);
  PrintHelper::print_end_line();
}

void ObAdminDumpBackupDataExecutor::print_common_header()
{
  print_common_header(common_header_);
}

void ObAdminDumpBackupDataExecutor::print_backup_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternBackupInfos backup_infos;
  ObArray<ObExternBackupInfo> info_array;
  char backup_set_id_str[64] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(backup_infos.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "cluster backup info failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(backup_infos.get_extern_backup_infos(info_array))) {
      STORAGE_LOG(WARN, "failed to get extern backup infos", K(ret), K(backup_infos));
    } else if (info_array.empty()) {
      //do nothing
    } else {
      const ObExternBackupInfo &info = info_array.at(0);
      int64_t full_backup_set_id = info.full_backup_set_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < info_array.count();) {
        const ObExternBackupInfo &info = info_array.at(i);
        if (full_backup_set_id == info.full_backup_set_id_) {
          PrintHelper::print_dump_title("BackupSet", full_backup_set_id, 1);
        } else {
          full_backup_set_id = info.full_backup_set_id_;
          continue;
        }
        if (OB_FAIL(databuff_printf(backup_set_id_str, 64, "%ld", info.inc_backup_set_id_))) {
         STORAGE_LOG(WARN, "failed to printf inc backup set id", K(ret), K(info));
        } else {
          ++i;
          PrintHelper::print_dump_line(backup_set_id_str, to_cstring(info));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_backup_tenant_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternTenantInfos tenant_infos;
  ObArray<ObExternTenantInfo> tenant_info_array;
  char tenant_id_str[64] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(tenant_infos.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "backup tenant info failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(tenant_infos.get_extern_tenant_infos(tenant_info_array))) {
      STORAGE_LOG(WARN, "failed to get extern tenant infos", K(ret), K(tenant_infos));
    } else if (tenant_info_array.empty()) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_info_array.count(); ++i) {
        const ObExternTenantInfo &tenant_info = tenant_info_array.at(i);
        if (OB_FAIL(databuff_printf(tenant_id_str, 64, "%lu", tenant_info.tenant_id_))) {
         STORAGE_LOG(WARN, "failed to printf inc backup set id", K(ret), K(tenant_info));
        } else {
          PrintHelper::print_dump_line(tenant_id_str, to_cstring(tenant_info));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_clog_backup_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternLogArchiveBackupInfo log_achive_backup_infos;
  ObArray<ObTenantLogArchiveStatus> status_array;
  char tenant_id_str[64] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(log_achive_backup_infos.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "log achive backup infos failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(log_achive_backup_infos.get_log_archive_status(status_array))) {
      STORAGE_LOG(WARN, "failed to get extern log archive status", K(ret), K(log_achive_backup_infos));
    } else if (status_array.empty()) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < status_array.count(); ++i) {
        const ObTenantLogArchiveStatus &status = status_array.at(i);
        if (OB_FAIL(databuff_printf(tenant_id_str, 64, "%lu", status.tenant_id_))) {
         STORAGE_LOG(WARN, "failed to printf inc backup set id", K(ret), K(status));
        } else {
          PrintHelper::print_dump_line(tenant_id_str, to_cstring(status));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_backup_set_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternBackupSetInfos backup_set_infos;
  ObArray<ObExternBackupSetInfo> set_info_array;
  char backup_set_id_str[64] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(backup_set_infos.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "cluster backup set info failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(backup_set_infos.get_extern_backup_set_infos(set_info_array))) {
      STORAGE_LOG(WARN, "failed to get extern backup set infos", K(ret), K(backup_set_infos));
    } else if (set_info_array.empty()) {
      //do nothing
    } else {
      const ObExternBackupSetInfo &info = set_info_array.at(0);
      int64_t full_backup_set_id = info.backup_set_id_;
      PrintHelper::print_dump_title("BackupSet", full_backup_set_id, 1);

      for (int64_t i = 0; OB_SUCC(ret) && i < set_info_array.count(); ++i) {
        const ObExternBackupSetInfo &set_info = set_info_array.at(i);
        if (OB_FAIL(databuff_printf(backup_set_id_str, 64, "%ld", set_info.backup_set_id_))) {
         STORAGE_LOG(WARN, "failed to printf inc backup set id", K(ret), K(set_info));
        } else {
          PrintHelper::print_dump_line(backup_set_id_str, to_cstring(set_info));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_backup_piece_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternalBackupPieceInfo backup_piece_info;
  ObArray<share::ObBackupPieceInfo> piece_info_array;
  char backup_piece_id_str[MAX_BACKUP_ID_STR_LENGTH] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(
      data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(backup_piece_info.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "backup piece info failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(backup_piece_info.get_piece_array(piece_info_array))) {
      STORAGE_LOG(WARN, "failed to get piece array", K(ret), K(backup_piece_info));
    } else if (piece_info_array.empty()) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < piece_info_array.count(); ++i) {
        const ObBackupPieceInfo &piece_info = piece_info_array.at(i);
        if (OB_FAIL(databuff_printf(backup_piece_id_str, MAX_BACKUP_ID_STR_LENGTH, "%ld", piece_info.key_.backup_piece_id_))) {
          STORAGE_LOG(WARN, "failed to printf backup piece id str", K(ret), K(piece_info));
        } else {
          PrintHelper::print_dump_line(backup_piece_id_str, to_cstring(piece_info));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_backup_single_piece_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObBackupPieceInfo piece;
  char backup_piece_id_str[MAX_BACKUP_ID_STR_LENGTH] = "";
  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(
      data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else {
    int64_t pos = 0;
    const ObBackupCommonHeader *common_header =
        reinterpret_cast<const ObBackupCommonHeader*>(buf + pos);
    pos += common_header->header_length_;
    if (OB_FAIL(common_header->check_header_checksum())) {
      STORAGE_LOG(WARN, "failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > file_length - pos) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "need more data then buf len", K(ret), KP(buf), K(file_length),
          K(*common_header));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + pos, common_header->data_zlength_))) {
      STORAGE_LOG(ERROR, "failed to check archive backup single piece", K(ret), K(*common_header));
    } else if (OB_FAIL(piece.deserialize(buf, pos + common_header->data_zlength_, pos))) {
      STORAGE_LOG(ERROR, "failed to deserialize piece", K(ret), K(*common_header));
    } else {
      print_common_header(*common_header);
      if (OB_FAIL(databuff_printf(backup_piece_id_str, MAX_BACKUP_ID_STR_LENGTH, "%ld", piece.key_.backup_piece_id_))) {
        STORAGE_LOG(WARN, "failed to printf backup piece id str", K(ret), K(piece));
      } else {
        PrintHelper::print_dump_line(backup_piece_id_str, to_cstring(piece));
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_backup_set_file_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternBackupSetFileInfos backup_set_infos;
  ObArray<share::ObBackupSetFileInfo> backup_set_file_array;
  char backup_set_id_str[MAX_BACKUP_ID_STR_LENGTH] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(
      data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(backup_set_infos.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "backup piece info failed to read", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (OB_FAIL(backup_set_infos.get_backup_set_file_infos(backup_set_file_array))) {
      STORAGE_LOG(WARN, "failed to get piece array", K(ret), K(backup_set_infos));
    } else if (backup_set_file_array.empty()) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_file_array.count(); ++i) {
        const ObBackupSetFileInfo &file_info = backup_set_file_array.at(i);
        if (OB_FAIL(databuff_printf(backup_set_id_str, MAX_BACKUP_ID_STR_LENGTH, "%ld", file_info.backup_set_id_))) {
          STORAGE_LOG(WARN, "failed to printf backup set id str", K(ret), K(file_info));
        } else {
          PrintHelper::print_dump_line(backup_set_id_str, to_cstring(file_info));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_pg_list()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObExternPGList pg_list;
  ObArray<ObPartitionKey> pkey_array;
  char table_id_str[64] = "";

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(pg_list.read_buf(buf, file_length))) {
    STORAGE_LOG(WARN, "backup pg list read failed", K(ret), K(buf), K(file_length), K(data_path_), K(storage_info_));
  } else {
    print_common_header();
    if (pg_list.get(pkey_array)) {
      STORAGE_LOG(WARN, "failed to get extern pg list", K(ret), K(pg_list));
    } else if (pkey_array.empty()) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pkey_array.count(); ++i) {
        const ObPartitionKey &pkey = pkey_array.at(i);
        if (OB_FAIL(databuff_printf(table_id_str, 64, "%lu", pkey.get_table_id()))) {
         STORAGE_LOG(WARN, "failed to printf table id", K(ret), K(pkey));
        } else {
          PrintHelper::print_dump_line(table_id_str, to_cstring(pkey));
        }
      }
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_meta_index()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObArray<ObBackupMetaIndex> meta_index_array;

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file length should not be zero", K(ret), K(file_length));
  } else {
    ObBufferReader buffer_reader(buf, file_length, 0);
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      meta_index_array.reset();
      ObBackupCommonHeader common_header;
      if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_meta_index(buffer_reader, common_header, meta_index_array))) {
        STORAGE_LOG(WARN, "failed to get meta index", K(ret), K(buffer_reader));
      } else if (OB_FAIL(print_meta_index_(common_header, meta_index_array))) {
        STORAGE_LOG(WARN, "failed to print meta index", K(ret), K(common_header));
      } else {
        STORAGE_LOG(INFO, "meta index", K(buffer_reader), K(meta_index_array), K(common_header));
        if (BACKUP_FILE_END_MARK == common_header.data_type_) {
          LOG_INFO("end print meta index");
          break;
        }
      }
    }
  }
}

int ObAdminDumpBackupDataExecutor::print_meta_index_(
    const share::ObBackupCommonHeader &common_header,
    const common::ObIArray<ObBackupMetaIndex> &meta_index_array)
{
  int ret = OB_SUCCESS;
  print_common_header(common_header);
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array.count(); ++i) {
    const ObBackupMetaIndex &meta_index = meta_index_array.at(i);
    ObMetaIndexKey key(meta_index.table_id_, meta_index.partition_id_, meta_index.meta_type_);
    PrintHelper::print_dump_line(to_cstring(key), to_cstring(meta_index));
  }
  PrintHelper::print_end_line();

  return ret;
}


void ObAdminDumpBackupDataExecutor::print_meta_data()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObBackupMetaHeader meta_header;

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, offset_, data_length_, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_meta_header(buf, data_length_, meta_header))) {
    STORAGE_LOG(WARN, "failed to get meta header", K(ret), K(data_length_));
  } else {
    if (PARTITION_GROUP_META == meta_header.meta_type_) {
      print_partition_group_meta(buf, data_length_);
    } else if (PARTITION_META == meta_header.meta_type_) {
      print_partition_meta(buf, data_length_);
    } else if (SSTABLE_METAS == meta_header.meta_type_) {
      print_sstable_meta(buf, data_length_);
    } else if (TABLE_KEYS == meta_header.meta_type_) {
      print_table_keys(buf, data_length_);
    } else if (PARTITION_GROUP_META_INFO == meta_header.meta_type_) {
      print_partition_group_meta_info(buf, data_length_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is invalid", K(ret), K(meta_header));
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_partition_group_meta(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "print partition group meta is invalid", K(ret), KP(buf), K(size));
  } else {
    ObPartitionGroupMeta pg_meta;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader *meta_header = NULL;
    if (OB_FAIL(buffer_reader.deserialize(buf, size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(size), K(pos));
    } else if (OB_FAIL(buffer_reader.get(meta_header))) {
      STORAGE_LOG(WARN, "read meta header fail", K(ret));
    } else if (OB_ISNULL(meta_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta header is null", K(ret));
    } else if (OB_FAIL(meta_header->check_valid())) {
      STORAGE_LOG(WARN, "meta_header is invalid", K(ret), K(buffer_reader));
    } else if (meta_header->data_length_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
    } else if (OB_FAIL(meta_header->check_data_checksum(
        buffer_reader.current(),
        meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret), K(buffer_reader));
    } else if (OB_FAIL(buffer_reader.read_serialize(pg_meta))) {
      STORAGE_LOG(WARN, "read pg meta fail", K(ret), K(meta_header));
    } else if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(meta_header), K(size));
    } else {
      print_meta_header(*meta_header);
      PrintHelper::print_dump_title("Partition Group Meta");
      PrintHelper::print_dump_line(to_cstring(pg_meta.pg_key_), to_cstring(pg_meta));
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_partition_meta(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "print partition group meta is invalid", K(ret), KP(buf), K(size));
  } else {
    ObPartitionStoreMeta meta;
    int64_t pos = 0;
    ObBufferReader buffer_reader;
    const ObBackupMetaHeader *meta_header = NULL;

    if (OB_FAIL(buffer_reader.deserialize(buf, size, pos))) {
      LOG_WARN("failed to deserialize buffer reader", K(ret), K(size));
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
    } else if (OB_FAIL(meta_header->check_data_checksum(
        buffer_reader.current(),
        meta_header->data_length_))) {
      STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
    } else if (OB_FAIL(buffer_reader.read_serialize(meta))) {
      STORAGE_LOG(WARN, "read partition meta fail", K(ret), K(buffer_reader));
    } else if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(buffer_reader));
    } else {
      print_meta_header(*meta_header);
      PrintHelper::print_dump_title("Partition Meta");
      PrintHelper::print_dump_line(to_cstring(meta.pkey_), to_cstring(meta));
      PrintHelper::print_end_line();
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_sstable_meta(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTableBaseMeta> sstable_meta_array;
  int64_t table_count = 0;
  int64_t pos = 0;
  ObBufferReader buffer_reader;
  const ObBackupMetaHeader *meta_header = NULL;

  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "print partition group meta is invalid", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(buffer_reader.deserialize(buf, size, pos))) {
    LOG_WARN("failed to deserialize buffer reader", K(ret), K(size), K(pos));
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
  } else if (OB_FAIL(meta_header->check_data_checksum(
      buffer_reader.current(),
      meta_header->data_length_))) {
    STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
  } else if (OB_FAIL(buffer_reader.read(table_count))) {
    STORAGE_LOG(WARN, "read sstable count", K(ret), K(buffer_reader));
  } else {
    ObSSTableBaseMeta meta(allocator_);
    for (int i=0; OB_SUCC(ret) && i < table_count; ++i) {
      meta.reset();
      if (OB_FAIL(buffer_reader.read_serialize(meta))) {
        STORAGE_LOG(WARN, "read sstable base meta fail", K(ret), K(buffer_reader));
      } else if (OB_FAIL(sstable_meta_array.push_back(meta))) {
        STORAGE_LOG(WARN, "push back meta fail", K(ret), K(buffer_reader));
      }
    }
    if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(buffer_reader));
    }
  }

  if (OB_SUCC(ret)) {
    print_meta_header(*meta_header);
    PrintHelper::print_dump_title("SSTable Meta");
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_meta_array.count(); ++i) {
      const ObSSTableBaseMeta &meta = sstable_meta_array.at(i);
      PrintHelper::print_dump_line(to_cstring(meta.index_id_), to_cstring(meta));
    }
    PrintHelper::print_end_line();
  }
}

void ObAdminDumpBackupDataExecutor::print_table_keys(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t table_count = 0;
  storage::ObITable::TableKey table_key;
  ObArray<storage::ObITable::TableKey> table_key_array;
  int64_t pos = 0;
  ObBufferReader buffer_reader;
  const ObBackupMetaHeader *meta_header = NULL;

  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "print partition group meta is invalid", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(buffer_reader.deserialize(buf, size, pos))) {
    LOG_WARN("failed to deserialize buffer reader", K(ret), K(size));
  } else if (OB_FAIL(buffer_reader.get(meta_header))) {
    STORAGE_LOG(WARN, "read meta header fail", K(ret));
  } else if (OB_ISNULL(meta_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta header is null", K(ret));
  } else if (OB_FAIL(meta_header->check_valid())) {
    STORAGE_LOG(WARN, "meta_header is invaild", K(ret), K(buffer_reader), K(*meta_header));
  } else if (meta_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "meta_header data_length may be wrong, buffer_reader not enough", K(ret));
  } else if (OB_FAIL(meta_header->check_data_checksum(
      buffer_reader.current(),
      meta_header->data_length_))) {
    STORAGE_LOG(WARN, "meta_header data checksum fail", K(ret));
  } else if (OB_FAIL(buffer_reader.read(table_count))) {
    STORAGE_LOG(WARN, "read table count", K(ret), K(buffer_reader));
  } else {
    for (int i=0; OB_SUCC(ret) && i < table_count; ++i) {
      if (OB_FAIL(buffer_reader.read_serialize(table_key))) {
        STORAGE_LOG(WARN, "read table key fail", K(ret), K(i), K(buffer_reader));
      } else if (OB_FAIL(table_key_array.push_back(table_key))) {
        STORAGE_LOG(WARN, "push back table key fail", K(ret), K(i), K(buffer_reader));
      }
    }
    if (0 != buffer_reader.remain()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(buffer_reader));
    }
  }
  if (OB_SUCC(ret)) {
    print_meta_header(*meta_header);
    PrintHelper::print_dump_title("TableKeys");
    for (int64_t i = 0; OB_SUCC(ret) && i < table_key_array.count(); ++i) {
      const storage::ObITable::TableKey &table_key = table_key_array.at(i);
      PrintHelper::print_dump_line(to_cstring(table_key.table_id_), to_cstring(table_key));
    }
    PrintHelper::print_end_line();
  }
}

void ObAdminDumpBackupDataExecutor::print_partition_group_meta_info(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBufferReader buffer_reader;
  ObBackupPGMetaInfo pg_meta_info;
  const ObBackupMetaHeader *meta_header = NULL;
  if (OB_ISNULL(buf) || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(buf), K(size));
  } else if (OB_FAIL(buffer_reader.deserialize(buf, size, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize buffer reader", K(ret), K(buf), K(size));
  } else if (OB_FAIL(buffer_reader.get(meta_header))) {
    STORAGE_LOG(WARN, "failed to read meta header", K(ret), K(buffer_reader));
  } else if (OB_ISNULL(meta_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta header is null", K(ret));
  } else if (meta_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
  } else if (OB_FAIL(meta_header->check_data_checksum(
      buffer_reader.current(), meta_header->data_length_))) {
    STORAGE_LOG(WARN, "failed to check data checksum", K(ret));
  } else if (OB_FAIL(buffer_reader.read_serialize(pg_meta_info))) {
    STORAGE_LOG(WARN, "failed to read pg meta info", K(ret));
  } else if (0 != buffer_reader.remain()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read meta buff size not expected", K(ret), K(meta_header), K(size));
  } else {
    print_meta_header(*meta_header);
    PrintHelper::print_dump_title("Partition Group Meta Info");
    PrintHelper::print_dump_line(to_cstring(pg_meta_info.pg_meta_.pg_key_), to_cstring(pg_meta_info));
    PrintHelper::print_end_line();
  }
}

// TODO: refine it later, check the macro index is v1 or v2
int ObAdminDumpBackupDataExecutor::get_macro_data_index_version(int64_t &version)
{
  int ret = OB_SUCCESS;
  version = ObBackupMacroIndexVersion::BACKUP_MACRO_INDEX_VERSION_2;
  return ret;
}

void ObAdminDumpBackupDataExecutor::print_macro_data_index()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  int64_t pos = 0;
  int64_t version = 0;
  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file length should not be zero", K(ret), K(file_length));
  } else if (OB_FAIL(get_macro_data_index_version(version))) {
    STORAGE_LOG(WARN, "failed to get macro data index version", K(ret), K(file_length));
  } else {
    if (ObBackupMacroIndexVersion::BACKUP_MACRO_INDEX_VERSION_1 == version) {
      print_macro_data_index_v1(buf, file_length);
    } else if (ObBackupMacroIndexVersion::BACKUP_MACRO_INDEX_VERSION_2 == version) {
      print_macro_data_index_v2(buf, file_length);
    } else {
      // do nothing
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_macro_data_index_v1(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObBufferReader buffer_reader(buf, size, 0);
  int64_t pos = 0;
  ObArray<ObBackupMacroIndex> macro_index_array;
  while (OB_SUCC(ret) && pos < size) {
    macro_index_array.reset();
    ObBackupCommonHeader common_header;
    if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_macro_block_index(buffer_reader, common_header, macro_index_array))) {
      STORAGE_LOG(WARN, "failed to get meta index", K(ret), K(size), K(pos));
    } else if (OB_FAIL(print_macro_data_index_v1_(common_header, macro_index_array))) {
      STORAGE_LOG(WARN, "failed to print meta index", K(ret), K(common_header));
    } else {
      STORAGE_LOG(INFO, "macro block index", "pos", pos, K(buffer_reader), K(macro_index_array), K(common_header));
      if (BACKUP_FILE_END_MARK == common_header.data_type_) {
        STORAGE_LOG(INFO, "end print macro data index");
        break;
      }
    }
  }
}

int ObAdminDumpBackupDataExecutor::print_macro_data_index_v1_(
    const share::ObBackupCommonHeader &common_header,
    const common::ObIArray<ObBackupMacroIndex> &macro_index_array)
{
  int ret = OB_SUCCESS;
  print_common_header(common_header);
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_index_array.count(); ++i) {
    const ObBackupMacroIndex &macro_index = macro_index_array.at(i);
    ObMetaIndexKey key(macro_index.table_id_, macro_index.partition_id_, 0);
    PrintHelper::print_dump_line(to_cstring(key), to_cstring(macro_index));
  }
  PrintHelper::print_end_line();

  return ret;
}

void ObAdminDumpBackupDataExecutor::print_macro_data_index_v2(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObBufferReader buffer_reader(buf, size, 0);
  TableMacroIndexMap index_map;
  static const int64_t BUCKET_NUM = 1000;
  if (OB_FAIL(index_map.create(BUCKET_NUM, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create table macro index map", K(ret)); 
  } else if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_macro_block_index_v2(buffer_reader, allocator_, index_map))) {
    STORAGE_LOG(WARN, "failed to get macro block index", K(ret)); 
  } else if (OB_FAIL(print_macro_data_index_v2_(index_map))) {
    STORAGE_LOG(WARN, "failed to print macro data index", K(ret)); 
  }
}

int ObAdminDumpBackupDataExecutor::print_macro_data_index_v2_(
    const TableMacroIndexMap &index_map)
{
  int ret = OB_SUCCESS;
  TableMacroIndexMap::const_iterator iter;
  iter = index_map.begin();
  for (; OB_SUCC(ret) && iter != index_map.end(); ++iter) {
    const ObITable::TableKey &table_key = iter->first;
    const ObArray<ObBackupTableMacroIndex> *index_list = iter->second;
    PrintHelper::print_dump_line("table_key", to_cstring(table_key));
    if (OB_ISNULL(index_list)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "index list should not be null", K(ret)); 
    } 
    for (int64_t i = 0; OB_SUCC(ret) && i < index_list->count(); ++i) {
      const ObBackupTableMacroIndex &index = index_list->at(i);
      PrintHelper::print_dump_line("macro_index", to_cstring(index));
    }
    PrintHelper::print_end_line();
  }
  return ret;
}

void ObAdminDumpBackupDataExecutor::print_macro_block()
{
  int ret = OB_SUCCESS;
  char *read_buf = NULL;
  ObFullMacroBlockMeta full_macro_meta;
  blocksstable::ObMacroBlockMeta *macro_meta = NULL;
  ObSSTableDataBlockReader macro_reader;
  ObSSTableMacroBlockChecker checker;

  if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator_.alloc(data_length_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc read buf", K(ret), K(data_length_));
  } else if (OB_FAIL(ObRestoreFileUtil::pread_file(data_path_,
      storage_info_,
      offset_,
      data_length_,
      read_buf))) {
    STORAGE_LOG(WARN, "fail to pread macro data buffer", K(ret), K(storage_info_), K(data_path_), K(data_length_), K(offset_));
  } else {
    const int64_t read_align_len = data_length_ + DIO_READ_ALIGN_SIZE;
    ObBufferReader buffer_reader(read_buf, read_align_len, 0);
    const ObBackupCommonHeader *common_header = NULL;
    blocksstable::ObMacroBlockMeta tmp_meta;
    blocksstable::ObBufferReader tmp_macro_meta_reader;
    blocksstable::ObBufferReader tmp_macro_data;
    //TODO: fix print ObBackupMacroData in this function, include below comment lines
    //ObBackupMacroData backup_macro_data(tmp_meta, tmp_macro_data);
    common_header = NULL;
    common::ObObj *endkey = NULL;
    if (OB_ISNULL(endkey = reinterpret_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN,"fail to alloc memory for macro block end key", K(ret));
    } else if (OB_ISNULL(tmp_meta.endkey_ = new(endkey) ObObj[common::OB_MAX_COLUMN_NUMBER])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro meta endkey is null", K(ret));
    } else if (OB_FAIL(buffer_reader.get(common_header))) {
      STORAGE_LOG(WARN, "read macro data common header fail", K(ret));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta index common header is null", K(ret));
    } else if (OB_FAIL(common_header->check_valid())) {
      STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer_reader not enough", K(ret), K(buffer_reader));
  /*  } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(buffer_reader));
    } else if (OB_FAIL(buffer_reader.read_serialize(backup_macro_data))) {
      STORAGE_LOG(WARN, "read macro meta fail", K(ret), K(*common_header), K(data_dir_));
  */} else if (OB_FAIL(tmp_meta.deep_copy(macro_meta, allocator_))) {
      STORAGE_LOG(WARN, "copy macro meta fail", K(ret), K(*common_header), K(data_path_));
    } else if (OB_ISNULL(macro_meta)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro_meta is null", K(ret), K(*common_header), K(data_path_));
    } else {
      STORAGE_LOG(INFO, "start dump macro data", K(*common_header), K(buffer_reader), K(tmp_macro_data),
          //"backup_macro_data need size", backup_macro_data.get_serialize_size(),
          K(*macro_meta));
      if (OB_FAIL(macro_reader.init(tmp_macro_data.data(), tmp_macro_data.capacity()))) {
        STORAGE_LOG(ERROR, "failed to init macro reader", K(ret));
      } else if (OB_FAIL(macro_reader.dump())) {
        STORAGE_LOG(ERROR, "failed dump macro block", K(ret));
//      } else if (OB_FAIL(checker.check(tmp_macro_data.data(), tmp_macro_data.capacity(), *macro_meta,
//          ObMacroBlockCheckLevel::CHECK_LEVEL_AUTO))) {
//        LOG_WARN("failed to check macro block", K(ret), K(tmp_macro_data), K(*macro_meta));
      }
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_tenant_name_info()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObTenantNameSimpleMgr mgr;
  ObTenantNameSimpleMgr::ObTenantNameMeta meta;
  common::ObArray<const ObTenantNameSimpleMgr::ObTenantNameInfo*> infos;

  if (OB_FAIL(ObAdminDumpBackupDataReaderUtil::get_backup_data(data_path_, storage_info_, buf, file_length, allocator_))) {
    STORAGE_LOG(WARN, "failed to get backup data", K(ret), K(data_path_), K(storage_info_));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "buf should not be NULL", K(ret), KP(buf));
  } else if (OB_FAIL(mgr.init())) {
    LOG_WARN("failed to init mgr", K(ret));
  } else if (OB_FAIL(mgr.read_buf(buf, file_length))) {
    LOG_WARN("failed to read mgr", K(ret));
  } else if (OB_FAIL(mgr.get_infos(meta, infos))) {
    LOG_WARN("failed to get infos", K(ret));
  } else {
    print_common_header();
    print_tenant_name_info_meta(meta);
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      print_tenant_name_info(i, *infos.at(i));
    }
  }
}

void ObAdminDumpBackupDataExecutor::print_tenant_name_info_meta(
    const ObTenantNameSimpleMgr::ObTenantNameMeta &meta)
{
  PrintHelper::print_dump_title("tenant name meta");
  PrintHelper::print_dump_line("schema_version", meta.schema_version_);
  PrintHelper::print_dump_line("tenant_count", meta.tenant_count_);
  PrintHelper::print_end_line();
}

void ObAdminDumpBackupDataExecutor::print_tenant_name_info(
    const int64_t idx, const ObTenantNameSimpleMgr::ObTenantNameInfo &info)
{
  PrintHelper::print_dump_title("tenant name info", idx, 1/*level*/);
  PrintHelper::print_dump_line("tenant_name", to_cstring(info.tenant_name_));
  PrintHelper::print_dump_line("is_complete", info.is_complete_);
  for (int64_t i = 0; i < info.id_list_.count(); ++i) {
    const ObTenantNameSimpleMgr::ObTenantIdItem &item = info.id_list_.at(i);
    PrintHelper::print_dump_line("idx", i, 2/*level*/);
    PrintHelper::print_dump_line("tenant_id", item.tenant_id_, 2/*level*/);
    PrintHelper::print_dump_line("timestamp", item.timestamp_, 2/*level*/);
  }
  PrintHelper::print_end_line();
}

void ObAdminDumpBackupDataExecutor::print_usage()
{
  printf("\n");
  printf("Usage: dump_backup command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-d,--data-file-path", "absolute data file path with file prefix");
  printf(HELP_FMT, "-o,--offset", "data file offset");
  printf(HELP_FMT, "-l,--data-length", "data length");
  printf(HELP_FMT, "-f,--file-path", "relative data file path");
#ifdef _WITH_OSS
  printf(HELP_FMT, "-s,--storage-info", "oss should provide storage info");
#endif
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-c,--check_exist", "check file is exist or not");
  printf("samples:\n");
  printf("  dump meta: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/backup_info \n");
  printf("  dump macro: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/macro_block_1.0 -o1024 -l2048\n");
  printf("  dump data with -f: \n");
  printf("\tob_admin dump_backup -f/home/admin/macro_block_1.0 -o1024 -l2048\n");
#ifdef _WITH_OSS
  printf("  dump data with -s: \n");
  printf("\tob_admin dump_backup -d'oss://home/admin/backup_info' -s'host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222'\n");
#endif
}

void ObAdminDumpBackupDataExecutor::print_archive_block_meta()
{
  int ret = OB_SUCCESS;
  ObAdminLogArchiveExecutor executor;

  if (OB_FAIL(executor.dump_single_file(input_data_path_))) {
    LOG_WARN("failed to dump archive block data", K(ret), K(input_data_path_));
  }
}

void ObAdminDumpBackupDataExecutor::print_archive_index_file()
{
  int ret = OB_SUCCESS;
  ObAdminLogArchiveExecutor executor;

  if (OB_FAIL(executor.dump_single_index_file(input_data_path_))) {
    LOG_WARN("failed to dump archive index", K(ret), K(input_data_path_));
  }
}

int ObAdminDumpBackupDataExecutor::check_exist(const char *data_path, const char *storage_info)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false);
  bool exist = false;
  const char *uri_str = "uri";
  const char *is_exist_str = "is_exist";
  size_t data_path_len = strlen(data_path);
  char dir_data_path[common::OB_MAX_URI_LENGTH + 1] = {0};

  if (data_path_len <= 0 || data_path[data_path_len - 1] == '/') {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "uri format not right", K(ret));
  }
  if (OB_FAIL(ret)) {
#ifdef _WITH_OSS
  } else if (OB_FAIL(ObOssEnvIniter::get_instance().global_init())) {
    STORAGE_LOG(WARN, "init oss storage fail", K(ret));
#endif
  } else if (OB_FAIL(util.is_exist(data_path, storage_info, exist))) {
    STORAGE_LOG(WARN, "failed to check is exist", K(ret), K(data_path));
  } else {
    PrintHelper::print_dump_title("check_exist");
    if (exist) {
      PrintHelper::print_dump_line(uri_str, data_path);
      PrintHelper::print_dump_line(is_exist_str, "true");
    } else if (OB_FAIL(databuff_printf(dir_data_path, sizeof(dir_data_path), "%s/", data_path))) {
      STORAGE_LOG(WARN, "failed to printf file uri", K(ret), K(optarg));
    } else {
      bool empty = false;
      if (OB_FAIL(util.is_empty_directory(dir_data_path, storage_info, empty))) {
        STORAGE_LOG(WARN, "failed to check is empty", K(ret), K(data_path));
      } else {
        if (empty) {
          PrintHelper::print_dump_line(uri_str, data_path);
          PrintHelper::print_dump_line(is_exist_str, "false");
        } else {
          PrintHelper::print_dump_line(uri_str, data_path);
          PrintHelper::print_dump_line(is_exist_str, "true");
        }
        PrintHelper::print_end_line();
      }
    }
  }
  return ret;
}

} //namespace tools
} //namespace oceanbase

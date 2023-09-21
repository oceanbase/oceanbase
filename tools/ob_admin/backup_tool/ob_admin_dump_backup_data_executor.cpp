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

#include "ob_admin_dump_backup_data_executor.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/container/ob_array.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

#include <algorithm>
#include <functional>

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::backup;
using namespace oceanbase::blocksstable;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase {
namespace tools {

/* ObAdminDumpBackupDataUtil */

int ObAdminDumpBackupDataUtil::read_archive_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    share::ObIBackupSerializeProvider &serializer)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t file_length = 0;
  char * buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&serializer);
  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.get_file_length(backup_path.ptr(), &storage_info, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get file length.", K(ret), K(backup_path));
    } else {
      STORAGE_LOG(WARN, "file not exist.", K(ret), K(backup_path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file is empty.", K(ret), K(backup_path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(backup_path), K(file_length));
  } else if (OB_FAIL(util.read_single_file(backup_path.ptr(), &storage_info, buf, file_length, read_size))) {
    STORAGE_LOG(WARN, "failed to read file.", K(ret), K(backup_path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match.", K(ret), K(backup_path), K(file_length), K(read_size));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize.", K(ret), K(backup_path), K(file_length));
  } else {
    STORAGE_LOG(INFO, "succeed to read archive file.", K(backup_path), K(serializer));
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_backup_file_header(const common::ObString &backup_path, const common::ObString &storage_info_str,
    backup::ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  file_header.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t header_len = sizeof(ObBackupFileHeader);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info_str));
  } else if (OB_UNLIKELY(file_length <= sizeof(header_len))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file too small", K(ret), K(file_length), K(header_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(header_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(header_len));
  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, 0, header_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info_str), K(file_length), K(header_len));
  } else {
    ObBufferReader buffer_reader(buf, header_len);
    const ObBackupFileHeader *header = NULL;
    if (OB_FAIL(buffer_reader.get(header))) {
      STORAGE_LOG(WARN, "failed to get file header", K(ret));
    } else if (OB_ISNULL(header)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup data file header is null", K(ret));
    } else {
      file_header = *header;
    }
  }

  return ret;
}

int ObAdminDumpBackupDataUtil::read_data_file_trailer(const common::ObString &backup_path,
    const common::ObString &storage_info_str, backup::ObBackupDataFileTrailer &file_trailer)
{
  int ret = OB_SUCCESS;
  file_trailer.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupDataFileTrailer);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info_str));
  } else if (OB_UNLIKELY(file_length <= sizeof(trailer_len))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info_str), K(file_length), K(trailer_len));
  } else {
    ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupDataFileTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {
      STORAGE_LOG(WARN, "failed to get file trailer", K(ret));
    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup data file trailer is null", K(ret));
    } else if (OB_FAIL(trailer->check_valid())) {
      STORAGE_LOG(WARN, "failed to check is valid", K(ret), K(*trailer));
    } else {
      file_trailer = *trailer;
      STORAGE_LOG(INFO, "read file trailer", K(backup_path), K(file_trailer));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_index_file_trailer(const common::ObString &backup_path,
    const common::ObString &storage_info_str, backup::ObBackupMultiLevelIndexTrailer &index_trailer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupMultiLevelIndexTrailer);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info_str));
  } else if (OB_UNLIKELY(file_length <= sizeof(trailer_len))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info_str), K(file_length), K(trailer_len));
  } else {
    ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupMultiLevelIndexTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {
      STORAGE_LOG(WARN, "failed to get file trailer", K(ret));
    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup data file trailer is null", K(ret));
    } else {
      index_trailer = *trailer;
    }
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_tablet_metas_file_trailer(const common::ObString &backup_path,
    const common::ObString &storage_info_str, backup::ObTabletInfoTrailer &tablet_meta_trailer)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&tablet_meta_trailer);
  const int64_t trailer_len = serializer_wrapper.get_serialize_size();
  int64_t pos = 0;
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info_str));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info_str), K(file_length), K(trailer_len));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, trailer_len, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize.", K(ret));
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::pread_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    const int64_t offset, const int64_t read_size, char *buf)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t real_read_size = 0;
  share::ObBackupStorageInfo storage_info;
  if (OB_UNLIKELY(0 == backup_path.length())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(ret), K(backup_path));
  } else if (OB_UNLIKELY(read_size <= 0)) {
    STORAGE_LOG(INFO, "read data len is zero", K(backup_path));
  } else if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.read_part_file(backup_path, &storage_info, buf, read_size, offset, real_read_size))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(offset), K(read_size));
  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "not read enough file", K(ret), K(read_size), K(real_read_size), K(backup_path));
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::get_backup_file_length(
    const common::ObString &backup_path, const common::ObString &storage_info_str, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.get_file_length(backup_path, &storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info));
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::get_common_header(
    blocksstable::ObBufferReader &buffer_reader, const share::ObBackupCommonHeader *&common_header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_reader.get(common_header))) {
    STORAGE_LOG(WARN, "failed to read common header", K(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    STORAGE_LOG(WARN, "common header is not valid", K(ret));
  } else if (common_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer reader not enough", K(ret));
  } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
    STORAGE_LOG(WARN, "failed to check data checksum", K(ret), K(buffer_reader));
  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func2)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret));
    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        IndexType index;
        if (OB_FAIL(buffer_reader.read_serialize(index))) {
          STORAGE_LOG(WARN, "failed to read serialize", K(ret));
        } else if (OB_FAIL(index_list.push_back(index))) {
          STORAGE_LOG(WARN, "failed to push back", K(ret), K(index));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {
        STORAGE_LOG(WARN, "failed to print func 1", K(ret), KPC(common_header));
      } else if (OB_FAIL(print_func2(index_list))) {
        STORAGE_LOG(WARN, "failed to print func 2", K(ret), K(index_list));
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer reader buf not enough", K(ret), KPC(common_header));
      }
    }
  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_blocks(
    blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    int64_t start_pos = buffer_reader.pos();
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret));
    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        IndexType index;
        if (OB_FAIL(buffer_reader.read_serialize(index))) {
          STORAGE_LOG(WARN, "failed to read serialize", K(ret));
        } else if (OB_FAIL(index_list.push_back(index))) {
          STORAGE_LOG(WARN, "failed to push back", K(ret), K(index));
        }
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer reader buf not enough", K(ret), KPC(common_header));
      }
    }
  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func3)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    backup::ObBackupMultiLevelIndexHeader index_header;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret));
    } else if (OB_FAIL(buffer_reader.read_serialize(index_header))) {
      STORAGE_LOG(WARN, "failed to read serialize", K(ret));
    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        IndexType index;
        if (OB_FAIL(buffer_reader.read_serialize(index))) {
          STORAGE_LOG(WARN, "failed to read serialize", K(ret));
        } else if (OB_FAIL(index_list.push_back(index))) {
          STORAGE_LOG(WARN, "failed to push back", K(ret), K(index));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {
        STORAGE_LOG(WARN, "failed to print func 1", K(ret), KPC(common_header));
      } else if (OB_FAIL(print_func2(index_header))) {
        STORAGE_LOG(WARN, "failed to print func 2", K(ret), K(index_header));
      } else if (OB_FAIL(print_func3(index_list))) {
        STORAGE_LOG(WARN, "failed to print func 3", K(ret), K(index_header), K(index_list));
      } else {
        index_list.reset();
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer reader buf not enough", K(ret), KPC(common_header));
      } else {
        STORAGE_LOG(WARN, "failed to advance", KPC(common_header));
      }
    }
  }
  return ret;
}

template <class IndexType, class IndexIndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func3,
    const std::function<int(const common::ObIArray<IndexIndexType> &)> &print_func4)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  ObArray<IndexIndexType> index_index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    backup::ObBackupMultiLevelIndexHeader index_header;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret), K(start_pos), K(buffer_reader));
    } else {
      int64_t data_size = 0;
      const int64_t pos = buffer_reader.pos();
      if (OB_FAIL(buffer_reader.read_serialize(index_header))) {
        STORAGE_LOG(WARN, "failed to read serialize", K(ret));
      } else {
        data_size = common_header->data_zlength_ - (buffer_reader.pos() - pos);
      }
      int64_t end_pos = buffer_reader.pos() + data_size;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          IndexType index;
          if (OB_FAIL(buffer_reader.read_serialize(index))) {
            STORAGE_LOG(WARN, "failed to read serialize", K(ret));
          } else if (OB_FAIL(index_list.push_back(index))) {
            STORAGE_LOG(WARN, "failed to push back", K(ret), K(index));
          }
        } else {
          IndexIndexType index_index;
          if (OB_FAIL(buffer_reader.read_serialize(index_index))) {
            STORAGE_LOG(WARN, "failed to read serialize", K(ret));
          } else if (OB_FAIL(index_index_list.push_back(index_index))) {
            STORAGE_LOG(WARN, "failed to push back", K(ret), K(index_index));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {
        STORAGE_LOG(WARN, "failed to print func 1", K(ret), KPC(common_header));
      } else if (OB_FAIL(print_func2(index_header))) {
        STORAGE_LOG(WARN, "failed to print func 2", K(ret), K(index_header));
      } else {
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          if (OB_FAIL(print_func3(index_list))) {
            STORAGE_LOG(WARN, "failed to print func 3", K(ret), K(index_header), K(index_list));
          } else {
            index_list.reset();
          }
        } else {
          if (OB_FAIL(print_func4(index_index_list))) {
            STORAGE_LOG(WARN, "failed to print func 4", K(ret), K(index_header), K(index_index_list));
          } else {
            index_index_list.reset();
          }
        }
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
        STORAGE_LOG(WARN, "buffer reader buf not enough", K(ret), KPC(common_header));
      } else {
        STORAGE_LOG(WARN, "failed to advance", KPC(common_header));
      }
    }
  }
  return ret;
}

template <typename BackupSmallFileType>
int ObAdminDumpBackupDataUtil::read_backup_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    BackupSmallFileType &file_info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&file_info);

  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.get_file_length(backup_path.ptr(), &storage_info, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get file length.", K(ret), K(backup_path));
    } else {
      STORAGE_LOG(WARN, "file not exist.", K(ret), K(backup_path));
    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file is empty.", K(ret), K(backup_path));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(backup_path), K(file_length));
  } else if (OB_FAIL(util.read_single_file(ObString(backup_path.ptr()), &storage_info, buf, file_length, read_size))) {
    STORAGE_LOG(WARN, "failed to read file.", K(ret), K(backup_path), K(file_length));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read file length not match.", K(ret), K(backup_path), K(file_length), K(read_size));
  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize.", K(ret), K(backup_path), K(file_length));
  } else {
    STORAGE_LOG(INFO, "succeed to read single file.", K(backup_path), K(file_info));
  }
  return ret;
}

/* ObAdminDumpBackupDataExecutor */

ObAdminDumpBackupDataExecutor::ObAdminDumpBackupDataExecutor() : offset_(), length_(), file_type_(), is_quiet_(false),
    allocator_()
{
  MEMSET(backup_path_, 0, common::OB_MAX_URI_LENGTH);
  MEMSET(storage_info_, 0, common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
}

ObAdminDumpBackupDataExecutor::~ObAdminDumpBackupDataExecutor()
{}

int ObAdminDumpBackupDataExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
  lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);
  int64_t data_type = 0;
  ObBackupDestType::TYPE path_type;
#ifdef OB_BUILD_TDE_SECURITY
  // The root_key is set to ensure the successful parsing of backup_dest, because there is encryption and decryption of access_key
  share::ObMasterKeyGetter::instance().init(NULL);
  ObMasterKeyGetter::instance().set_root_key(OB_SYS_TENANT_ID, obrpc::RootKeyType::DEFAULT, ObString());
#endif
  if (OB_FAIL(parse_cmd_(argc, argv))) {
    STORAGE_LOG(WARN, "failed to parse cmd", K(ret), K(argc), K(argv));
  } else if (is_quiet_) {
    OB_LOGGER.set_log_level("ERROR");
  } else {
    OB_LOGGER.set_log_level("INFO");
  }

  if (OB_FAIL(ret)) {
  } else if (check_exist_) {
    // ob_admin dump_backup -d'xxxxx' -c
    if (OB_FAIL(do_check_exist_())) {
      STORAGE_LOG(WARN, "fail to do check exist", K(ret));
    }
  } else if (OB_FAIL(check_tenant_backup_path_type_(backup_path_, storage_info_, path_type))) {
    STORAGE_LOG(WARN, "fail to check tenant backup path type", K(ret));
  } else if (path_type == ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA) {
    // if backup path is tenant backup path. dump the tenant backup infos of the latest backup set dir.
    if (OB_FAIL(dump_tenant_backup_path_())) {
      STORAGE_LOG(WARN, "fail to dump tenant backup path");
    }
  } else if (path_type == ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG) {
    // if backup path is tenant archive path. dump the archive round info.
    if (OB_FAIL(dump_tenant_archive_path_())) {
      STORAGE_LOG(WARN, "fail to dump tenant archive path");
    }
  } else if (OB_FAIL(check_file_exist_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "failed to check exist", K(ret), K_(backup_path), K_(storage_info));
  } else if (OB_FAIL(get_backup_file_type_())) {
    STORAGE_LOG(WARN, "failed to get backup file type", K(ret), K_(backup_path), K_(storage_info));
  } else if (OB_FAIL(do_execute_())) {
    STORAGE_LOG(WARN, "failed to do execute", K(ret), K_(backup_path), K_(storage_info));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "h:d:s:o:l:qc";
  struct option longopts[] = {{"help", 0, NULL, 'h'},
      {"backup_path", 1, NULL, 'd'},
      {"storage_info", 1, NULL, 's'},
      {"file_uri", 1, NULL, 'f'},
      {"offset", 1, NULL, 'o'},
      {"quiet", 0, NULL, 'q' },
      {"check_exist", 0, NULL, 'c'},
      {"length", 1, NULL, 'l'},
      {NULL, 0, NULL, 0}};
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage_();
        exit(1);
      }
      case 'd': {
        if (OB_FAIL(databuff_printf(backup_path_, sizeof(backup_path_), "%s", optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        }
        break;
      }
      case 's': {
        if (OB_FAIL(databuff_printf(storage_info_, sizeof(storage_info_), "%s", optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        }
        break;
      }
      case 'f': {
        if (OB_FAIL(get_backup_file_path_())) {
          STORAGE_LOG(WARN, "failed to get backup file path", K(ret));
        }
        break;
      }
      case 'o': {
        offset_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'q': {
        is_quiet_ = true;
        break;
      }
      case 'c': {
        check_exist_ = true;
        break;
      }
      case 'l': {
        length_ = strtoll(optarg, NULL, 10);
        break;
      }
      default: {
        print_usage_();
        exit(1);
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::do_check_exist_()
{
  int ret = OB_SUCCESS;
  bool is_exist = true; ;
  if (OB_FAIL(check_file_exist_(backup_path_, storage_info_))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to check file exist", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_exist) {
    if (OB_FAIL(check_dir_exist_(backup_path_, storage_info_, is_exist))) {
      STORAGE_LOG(WARN, "fail to check dir exist", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dump_check_exist_result_(backup_path_, storage_info_, is_exist))) {
      STORAGE_LOG(WARN, "fail to dump check exist result", K(ret));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_dir_exist_(
    const char *backup_path, const char *storage_info_str, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  bool is_empty_dir = true;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path, storage_info_str))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.is_empty_directory(backup_path, &storage_info, is_empty_dir))) {
    STORAGE_LOG(WARN, "failed to check dir exist", K(ret), K(backup_path), K(storage_info));
  } else if (!is_empty_dir) {
    is_exist = true;
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_file_exist_(const char *backup_path, const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path, storage_info_str))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_str));
  } else if (OB_FAIL(util.is_exist(backup_path, &storage_info, exist))) {
    STORAGE_LOG(WARN, "failed to check file exist", K(ret), K(backup_path), K(storage_info));
  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    STORAGE_LOG(WARN, "index file do not exist", K(ret), K(backup_path));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::do_execute_()
{
  int ret = OB_SUCCESS;
  switch (file_type_) {
    case backup::ObBackupFileType::BACKUP_DATA_FILE: {
      if (OB_FAIL(print_backup_data_file_())) {
        STORAGE_LOG(WARN, "failed to print backup data file", K(ret));
      }
      break;
    }
    case backup::ObBackupFileType::BACKUP_MACRO_RANGE_INDEX_FILE: {
      if (OB_FAIL(print_macro_range_index_file())) {
        STORAGE_LOG(WARN, "failed to print macro range index file", K(ret));
      }
      break;
    }
    case backup::ObBackupFileType::BACKUP_META_INDEX_FILE: {
      if (OB_FAIL(print_meta_index_file())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_LS_INFO: {
      if (OB_FAIL(print_ls_attr_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TABLET_TO_LS_INFO: {
      if (OB_FAIL(print_tablet_to_ls_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_DELETED_TABLET_INFO: {
      if (OB_FAIL(print_deleted_tablet_info_())) {
        STORAGE_LOG(WARN, "failed to print deleted tablet info", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_LOCALITY_INFO: {
      if (OB_FAIL(print_tenant_locality_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_SET_INFO: {
      if (OB_FAIL(print_backup_set_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_DIAGNOSE_INFO: {
      if (OB_FAIL(print_tenant_diagnose_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_DATA_PLACEHOLDER: {
      if (OB_FAIL(print_place_holder_info_())) {
        STORAGE_LOG(WARN, "failed to print meta index file", K(ret));
      }
      break;
    }
    // archive
    case share::ObBackupFileType::BACKUP_ARCHIVE_ROUND_START_INFO: {
      if (OB_FAIL(print_archive_round_start_file_())) {
        STORAGE_LOG(WARN, "failed to print archive round start file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_ROUND_END_INFO: {
      if (OB_FAIL(print_archive_round_end_file_())) {
        STORAGE_LOG(WARN, "failed to print archive round end file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_PIECE_START_INFO: {
      if (OB_FAIL(print_archive_piece_start_file_())) {
        STORAGE_LOG(WARN, "failed to print archive piece start file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_PIECE_END_INFO: {
      if (OB_FAIL(print_archive_piece_end_file_())) {
        STORAGE_LOG(WARN, "failed to print archive piece end file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_SINGLE_PIECE_INFO: {
      if (OB_FAIL(print_archive_single_piece_file_())) {
        STORAGE_LOG(WARN, "failed to print archive single piece file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_INNER_PLACEHOLDER_INFO: {
      if (OB_FAIL(print_archive_piece_inner_placeholder_file_())) {
        STORAGE_LOG(WARN, "failed to print archive inner piece placeholder file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_SINGLE_LS_FILE_LIST_INFO: {
      if (OB_FAIL(print_archive_single_ls_info_file_())) {
        STORAGE_LOG(WARN, "failed to print archive single ls info file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_FILE_LIST_INFO: {
      if (OB_FAIL(print_archive_piece_list_info_file_())) {
        STORAGE_LOG(WARN, "failed to print archive piece list info file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_FORMAT_FILE: {
      if (OB_FAIL(print_backup_format_file_())) {
        STORAGE_LOG(WARN, "failed to print backup format file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_SET_INFOS: {
      if (OB_FAIL(print_tenant_backup_set_infos_())) {
        STORAGE_LOG(WARN, "failed to print backup format file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_LS_META_INFOS_FILE: {
      if (OB_FAIL(print_backup_ls_meta_infos_file_())) {
        STORAGE_LOG(WARN, "failed to print backup ls meta infos file", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_ARCHIVE_PIECE_INFOS: {
      if (OB_FAIL(print_tenant_archive_piece_infos_file_())) {
        STORAGE_LOG(WARN, "failed to print tenant archive piece infos", K(ret));
      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TABLET_METAS_INFO: {
      if (OB_FAIL(print_ls_tablet_meta_tablets_())) {
        STORAGE_LOG(WARN, "failed to print ls tablet meta tablets", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "invalid type", K(ret), K_(file_type));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_file_path_()
{
  int ret = OB_SUCCESS;
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
      current_absolute_path[length] = '/';
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(
            backup_path_, sizeof(backup_path_), "%s%s%s", OB_FILE_PREFIX, current_absolute_path, optarg))) {
      STORAGE_LOG(WARN, "failed to printf file uri", K(ret), K(optarg));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_file_type_()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  ObArenaAllocator allocator;
  const int64_t type_len = sizeof(uint16_t);
  if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info));
  } else if (OB_UNLIKELY(file_length <= sizeof(type_len))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file too small", K(ret), K(file_length), K(type_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(type_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(type_len));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, 0, type_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info), K(file_length), K(type_len));
  } else {
    uint16_t *type = reinterpret_cast<uint16_t*>(buf);
    file_type_ = *type;
    STORAGE_LOG(INFO, "get file type", K(file_type_));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  printf("Usage: dump_backup command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-d,--backup-file-path", "absolute backup file path with file prefix");
  printf(HELP_FMT, "-s,--storage-info", "oss/cos should provide storage info");
  printf(HELP_FMT, "-f,--file-path", "relative data file path");
  printf(HELP_FMT, "-o,--offset", "data offset");
  printf(HELP_FMT, "-l,--length", "data length");
  printf(HELP_FMT, "-c,--check-exist", "check file is exist or not");
  printf("samples:\n");
  printf("  dump meta: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/backup_info \n");
  printf("  dump macro: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/macro_block_1.0 -o1024 -l2048\n");
  printf("  dump data with -f: \n");
  printf("\tob_admin dump_backup -f/home/admin/macro_block_1.0 -o1024 -l2048\n");
  printf("  dump data with -s: \n");
  printf("\tob_admin dump_backup -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222'\n");
  printf("\tob_admin dump_backup -d'cos://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&appid=333'\n");
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_tenant_backup_path_(const char *data_path, const char *storage_info_str, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObBackupPathString path;
  share::ObBackupDest backup_dest;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc desc;
  is_exist = false;
  if (OB_FAIL(backup_dest.set(data_path, storage_info_str))) {
    STORAGE_LOG(WARN, "fail to set backup dest", K(ret));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(path.ptr(), path.capacity()))) {
    STORAGE_LOG(WARN, "fail to get backup dest str", K(ret));
  } else if (OB_FAIL(backup_store.init(path.ptr()))) {
    STORAGE_LOG(WARN, "fail to init backup store", K(ret));
  } else if (OB_FAIL(backup_store.is_format_file_exist(is_exist))) {
    STORAGE_LOG(WARN, "fail to check format file exist", K(ret));
  } else if (is_exist) {
    if (OB_FAIL(backup_store.read_format_file(desc))) {
      STORAGE_LOG(WARN, "fail to read format file", K(ret));
    } else if (desc.dest_type_ != ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA) {
      is_exist = false;
    }
  }

  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_backup_path_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  storage::ObBackupSetFilter op;
  share::ObBackupPath path;
  share::ObBackupStorageInfo storage_info;
  storage::ObTenantBackupSetInfosDesc tenant_backup_set_infos;
  ObSArray<share::ObBackupSetDesc> backup_set_array;
  ObArray<share::ObBackupSetFileDesc> target_backup_set;
  if (OB_FAIL(get_backup_set_placeholder_dir_path(path))) {
    STORAGE_LOG(WARN, "fail to get backup set placeholder dir path", K(ret));
  } else if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(storage_info_));
  } else if (OB_FAIL(util.list_files(path.get_ptr(), &storage_info, op))) {
    STORAGE_LOG(WARN, "fail to list files", K(ret));
  } else if (OB_FAIL(op.get_backup_set_array(backup_set_array))) {
    STORAGE_LOG(WARN, "fail to get backup set names", K(ret));
  } else if (!backup_set_array.empty()) {
    for (int64_t i = backup_set_array.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      path.reset();
      const share::ObBackupSetDesc &backup_set_dir = backup_set_array.at(i);
      if (OB_FAIL(get_tenant_backup_set_infos_path_(backup_set_dir, path))) {
        STORAGE_LOG(WARN, "fail to get backup set infos path");
      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(path.get_obstr(), ObString(storage_info_), tenant_backup_set_infos))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          STORAGE_LOG(WARN, "backup set file is not exist", K(ret), K(path));
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "fail to read backup info file", K(ret));
        }
      } else if (OB_FAIL(filter_backup_set_(tenant_backup_set_infos, backup_set_array, target_backup_set))) {
        STORAGE_LOG(WARN, "fail to filter backup set", K(ret));
      } else if (OB_FAIL(inner_print_common_header_(path.get_obstr(), ObString(storage_info_)))) {
        STORAGE_LOG(WARN, "fail to print common header", K(ret));
      } else if (OB_FAIL(dump_tenant_backup_set_infos_(target_backup_set))) {
        STORAGE_LOG(WARN, "fail to dump tenant backup set infos", K(ret));
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_data_file_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupDataFileTrailer file_trailer;
  ObArray<backup::ObBackupMetaIndex> meta_index_list;
  if (OB_FAIL(print_backup_file_header_())) {
    STORAGE_LOG(WARN, "failed to print backup file header", K(ret), K(backup_path), K(storage_info));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_data_file_trailer(backup_path, storage_info, file_trailer))) {
    STORAGE_LOG(WARN, "failed to read file trailer", K(ret), K(backup_path), K(storage_info));
  } else {
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      const int64_t offset = file_trailer.macro_index_offset_;
      const int64_t length = file_trailer.macro_index_length_;
      ObArray<backup::ObBackupMacroBlockIndex> macro_index_list;
      if (0 == length) {
        // do nothing
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
        STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
      } else {
        ObBufferReader buffer_reader(buf, length);
        if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMacroBlockIndex>(
                buffer_reader, macro_index_list))) {
          STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < macro_index_list.count(); ++i) {
            const backup::ObBackupMacroBlockIndex &index = macro_index_list.at(i);
            if (OB_FAIL(inner_print_macro_block_(index.offset_, index.length_, i))) {
              STORAGE_LOG(WARN, "failed to inner print macro block", K(ret), K(index));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      const int64_t offset = file_trailer.meta_index_offset_;
      const int64_t length = file_trailer.meta_index_length_;
      ObArray<backup::ObBackupMetaIndex> meta_index_list;
      if (0 == length) {
        // do nothing
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
        STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
      } else {
        ObBufferReader buffer_reader(buf, length);
        if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMetaIndex>(
                buffer_reader, meta_index_list))) {
          STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_list.count(); ++i) {
            const backup::ObBackupMetaIndex &index = meta_index_list.at(i);
            if (backup::BACKUP_SSTABLE_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_sstable_metas_(index.offset_, index.length_))) {
                STORAGE_LOG(WARN, "failed to inner print sstable metas", K(ret), K(index));
              }
            } else if (backup::BACKUP_TABLET_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_tablet_meta_(index.offset_, index.length_))) {
                STORAGE_LOG(WARN, "failed to inner print tablet meta", K(ret), K(index));
              }
            } else if (backup::BACKUP_MACRO_BLOCK_ID_MAPPING_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_backup_macro_block_id_mapping_metas_(index.offset_, index.length_))) {
                STORAGE_LOG(WARN, "failed to inner print backup macro block id mapping meta", K(ret), K(index));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(inner_print_macro_block_index_list_(
              file_trailer.macro_index_offset_, file_trailer.macro_index_length_))) {
        STORAGE_LOG(WARN, "failed to inner print macro block index list", K(ret), K(backup_path), K(storage_info));
      } else if (OB_FAIL(
                     inner_print_meta_index_list_(file_trailer.meta_index_offset_, file_trailer.meta_index_length_))) {
        STORAGE_LOG(WARN, "failed to inner print meta index list", K(ret), K(backup_path), K(storage_info));
      } else if (OB_FAIL(dump_data_file_trailer_(file_trailer))) {
        STORAGE_LOG(WARN, "failed to print data file trailer", K(ret), K(file_trailer));
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_range_index_file()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  const int64_t offset = 0;
  int64_t length = 0;
  char *buf = NULL;
  backup::ObBackupMultiLevelIndexTrailer index_trailer;
  if (OB_FAIL(print_backup_file_header_())) {
    STORAGE_LOG(WARN, "failed to print backup file header", K(ret), K(backup_path), K(storage_info));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, length))) {
    STORAGE_LOG(WARN, "failed to get backup file length", K(ret), K(backup_path), K(storage_info));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, index_trailer))) {
    STORAGE_LOG(WARN, "failed to read index file trailer", K(ret), K(backup_path));
  } else {
    const int64_t header_size = DIO_READ_ALIGN_SIZE;
    const int64_t trailer_size = sizeof(backup::ObBackupMultiLevelIndexTrailer);
    buf += header_size;
    ObBufferReader buffer_reader(buf, length - header_size - trailer_size);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_, this, std::placeholders::_1);
    auto print_func4 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_, this, std::placeholders::_1);
    auto ParseFunc = ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndex,
        backup::ObBackupMacroRangeIndexIndex>;
    if (OB_FAIL(ParseFunc(buffer_reader, print_func1, print_func2, print_func3, print_func4))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_index_file_trailer_(index_trailer))) {
      STORAGE_LOG(WARN, "failed to print index file trailer", K(ret), K(index_trailer));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_file()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  const int64_t offset = 0;
  int64_t length = 0;
  char *buf = NULL;
  backup::ObBackupMultiLevelIndexTrailer index_trailer;
  if (OB_FAIL(print_backup_file_header_())) {
    STORAGE_LOG(WARN, "failed to print backup file header", K(ret), K(backup_path), K(storage_info));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, length))) {
    STORAGE_LOG(WARN, "failed to get backup file length", K(ret), K(backup_path), K(storage_info));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, index_trailer))) {
    STORAGE_LOG(WARN, "failed to read index file trailer", K(ret), K(backup_path));
  } else {
    const int64_t header_size = DIO_READ_ALIGN_SIZE;
    const int64_t trailer_size = sizeof(backup::ObBackupMultiLevelIndexTrailer);
    buf += header_size;
    ObBufferReader buffer_reader(buf, length - header_size - trailer_size);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 = std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_list_, this, std::placeholders::_1);
    auto print_func4 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_, this, std::placeholders::_1);
    auto ParseFunc = ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMetaIndex,
        backup::ObBackupMetaIndexIndex>;
    if (OB_FAIL(ParseFunc(buffer_reader, print_func1, print_func2, print_func3, print_func4))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_index_file_trailer_(index_trailer))) {
      STORAGE_LOG(WARN, "failed to print index file trailer", K(ret), K(index_trailer));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_file_header_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupFileHeader file_header;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_file_header(backup_path, storage_info, file_header))) {
    STORAGE_LOG(WARN, "failed to printf file uri", K(ret), K(optarg));
  } else if (OB_FAIL(dump_backup_file_header_(file_header))) {
    STORAGE_LOG(WARN, "failed to dump backup file header", K(ret), K(file_header));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_block_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_macro_block_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print macro block", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_meta_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_tablet_meta_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print tablet meta metas", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_sstable_metas_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_sstable_metas_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print sstable metas", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_block_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_macro_block_index_list_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print macro block index list", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_meta_index_list_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print meta index list", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_data_file_trailer_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupDataFileTrailer file_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_data_file_trailer(backup_path, storage_info, file_trailer))) {
    STORAGE_LOG(WARN, "failed to read file trailer", K(ret), K(backup_path), K(storage_info));
  } else if (OB_FAIL(dump_data_file_trailer_(file_trailer))) {
    STORAGE_LOG(WARN, "failed to print data file trailer", K(ret), K(file_trailer));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_index_file_trailer_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupMultiLevelIndexTrailer file_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, file_trailer))) {
    STORAGE_LOG(WARN, "failed to read file trailer", K(ret), K(backup_path), K(storage_info));
  } else if (OB_FAIL(dump_index_file_trailer_(file_trailer))) {
    STORAGE_LOG(WARN, "failed to print index file trailer", K(ret), K(file_trailer));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_range_index_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_macro_range_index_index_list_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print macro range index index list", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(inner_print_meta_index_index_list_(offset, length))) {
    STORAGE_LOG(WARN, "failed to inner print meta index index list", K(ret), K(offset), K(length));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_ls_attr_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataLSAttrDesc ls_attr_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), ls_attr_desc))) {
    STORAGE_LOG(WARN, "fail to read ls attr info", K(ret), K(backup_path_), K(storage_info_));
  } else {
    ARRAY_FOREACH_X(ls_attr_desc.ls_attr_array_, i , cnt, OB_SUCC(ret)) {
      const share::ObLSAttr ls_attr = ls_attr_desc.ls_attr_array_.at(i);
      if (OB_FAIL(dump_ls_attr_info_(ls_attr))) {
        STORAGE_LOG(WARN, "fail to dump ls attr info", K(ret), K(ls_attr));
      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_to_ls_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataTabletToLSDesc tablet_to_ls_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), tablet_to_ls_desc))) {
    STORAGE_LOG(WARN, "fail to read tablet to ls info", K(ret), K(backup_path_), K(storage_info_));
  } else {
    ARRAY_FOREACH_X(tablet_to_ls_desc.tablet_to_ls_, i , cnt, OB_SUCC(ret)) {
      const storage::ObBackupDataTabletToLSInfo tablet_to_ls = tablet_to_ls_desc.tablet_to_ls_.at(i);
      if (OB_FAIL(dump_tablet_to_ls_info_(tablet_to_ls))) {
        STORAGE_LOG(WARN, "fail to dump ls attr info", K(ret), K(tablet_to_ls));
      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_deleted_tablet_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDeletedTabletToLSDesc deleted_tablet_to_ls;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), deleted_tablet_to_ls))) {
    STORAGE_LOG(WARN, "fail to read tablet to ls info", K(ret), K(backup_path_), K(storage_info_));
  } else {
    ARRAY_FOREACH_X(deleted_tablet_to_ls.deleted_tablet_to_ls_, i , cnt, OB_SUCC(ret)) {
      const storage::ObBackupDataTabletToLSInfo tablet_to_ls = deleted_tablet_to_ls.deleted_tablet_to_ls_.at(i);
      if (OB_FAIL(dump_tablet_to_ls_info_(tablet_to_ls))) {
        STORAGE_LOG(WARN, "fail to dump ls attr info", K(ret), K(tablet_to_ls));
      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_locality_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternTenantLocalityInfoDesc locality_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), locality_desc))) {
    STORAGE_LOG(WARN, "fail to read locality info", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_tenant_locality_info_(locality_desc))) {
    STORAGE_LOG(WARN, "fail to dump tenant locality info", K(ret), K(locality_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_diagnose_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternTenantDiagnoseInfoDesc diagnose_info;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), diagnose_info))) {
    STORAGE_LOG(WARN, "fail to read locality info", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_tenant_diagnose_info_(diagnose_info))) {
    STORAGE_LOG(WARN, "fail to dump tenant locality info", K(ret), K(diagnose_info));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_set_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternBackupSetInfoDesc backup_set_info_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), backup_set_info_desc))) {
    STORAGE_LOG(WARN, "fail to read locality info", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_backup_set_info(backup_set_info_desc.backup_set_file_))) {
    STORAGE_LOG(WARN, "fail to dump tenant locality info", K(ret), K(backup_set_info_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_place_holder_info_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_round_start_file_()
{
  int ret = OB_SUCCESS;
  share::ObRoundStartDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive round start file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_round_start_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive round start file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_round_end_file_()
{
  int ret = OB_SUCCESS;
  share::ObRoundEndDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive round end file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_round_end_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive round end file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_start_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceStartDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece start file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_piece_start_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece start file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_end_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceEndDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece end file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_piece_end_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece end file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_single_piece_file_()
{
  int ret = OB_SUCCESS;
  share::ObSinglePieceDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive single piece file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_single_piece_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive single piece file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_inner_placeholder_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceInnerPlaceholderDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece inner placeholder file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_piece_inner_placeholder_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece inner placeholder file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_single_ls_info_file_()
{
  int ret = OB_SUCCESS;
  share::ObSingleLSInfoDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive single ls info file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_single_ls_info_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive single ls info file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_list_info_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceInfoDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece info file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_archive_piece_list_info_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece info file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_archive_piece_infos_file_()
{
  int ret = OB_SUCCESS;
  share::ObTenantArchivePieceInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece info file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_tenant_archive_piece_infos_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump tenant archive piece infos file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_ls_tablet_meta_tablets_()
{
  int ret = OB_SUCCESS;
  backup::ObTabletInfoTrailer tablet_meta_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_tablet_metas_file_trailer(backup_path_, storage_info_, tablet_meta_trailer))) {
    STORAGE_LOG(WARN, "failed to read tablet metas file trailer", K(ret));
  } else {
    common::ObArenaAllocator allocator;
    const int64_t DEFAULT_BUF_LEN = 2 * 1024 * 1024; // 2M
    int64_t cur_buf_offset = tablet_meta_trailer.offset_;
    int64_t cur_total_len = 0;
    char *buf = nullptr;
    while (OB_SUCC(ret) && cur_buf_offset < tablet_meta_trailer.length_) {
      const uint64_t buf_len = tablet_meta_trailer.length_ - cur_buf_offset < DEFAULT_BUF_LEN ?
                               tablet_meta_trailer.length_ - cur_buf_offset : DEFAULT_BUF_LEN;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc read buf", K(ret), K(buf_len));
      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path_, storage_info_, cur_buf_offset, buf_len, buf))) {
        STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path_), K(storage_info_), K(cur_buf_offset), K(buf_len));
      } else {
        backup::ObBackupTabletMeta tablet_meta;
        blocksstable::ObBufferReader buffer_reader(buf, buf_len);
        while (OB_SUCC(ret)) {
          tablet_meta.reset();
          int64_t pos = 0;
          const ObBackupCommonHeader *common_header = nullptr;
          if (buffer_reader.remain() == 0) {
            cur_total_len = buffer_reader.capacity();
            STORAGE_LOG(INFO, "read buf finish", K(buffer_reader));
            break;
          } else if (OB_FAIL(buffer_reader.get(common_header))) {
            STORAGE_LOG(WARN, "failed to get common_header", K(ret), K(backup_path_), K(buffer_reader));
          } else if (OB_ISNULL(common_header)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "common header is null", K(ret), K(backup_path_), K(buffer_reader));
          } else if (OB_FAIL(common_header->check_valid())) {
            STORAGE_LOG(WARN, "common_header is not valid", K(ret), K(backup_path_), K(buffer_reader));
          } else if (common_header->data_zlength_ > buffer_reader.remain()) {
            cur_total_len = buffer_reader.pos() - sizeof(ObBackupCommonHeader);
            STORAGE_LOG(INFO, "buf not enough, wait later", K(cur_total_len), K(buffer_reader));
            break;
          } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
            STORAGE_LOG(WARN, "failed to check data checksum", K(ret), K(*common_header), K(backup_path_), K(buffer_reader));
          } else if (OB_FAIL(tablet_meta.tablet_meta_.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
            STORAGE_LOG(WARN, "failed to read data_header", K(ret), K(*common_header), K(backup_path_), K(buffer_reader));
          } else if (OB_FAIL(buffer_reader.advance(common_header->data_length_ + common_header->align_length_))) {
            STORAGE_LOG(WARN, "failed to advance buffer", K(ret));
          } else {
            tablet_meta.tablet_id_ = tablet_meta.tablet_meta_.tablet_id_;
            if (OB_FAIL(dump_common_header_(*common_header))) {
              STORAGE_LOG(WARN, "failed to dump common header", K(ret));
            } else if (OB_FAIL(dump_backup_tablet_meta_(tablet_meta))) {
              STORAGE_LOG(WARN, "failed to dump backup tablet meta", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          cur_buf_offset += cur_total_len;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(dump_tablet_trailer_(tablet_meta_trailer))) {
      STORAGE_LOG(WARN, "failed to inner print tablet trailer", K(ret));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupFormatDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece info file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_backup_format_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece info file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_backup_set_infos_()
{
 int ret = OB_SUCCESS;
  ObBackupDest backup_tenant_dest;
  ObBackupSetFileDesc latest_file_desc;
  storage::ObTenantBackupSetInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece info file", K(ret), K(backup_path_), K(storage_info_));
  } else if(!file_desc.backup_set_infos_.empty()) {
    latest_file_desc = file_desc.backup_set_infos_.at(file_desc.backup_set_infos_.count() - 1);
    if (OB_FAIL(backup_tenant_dest.set(latest_file_desc.backup_path_))) {
      STORAGE_LOG(WARN, "fail to set backup tenant dest", K(ret), K(latest_file_desc), K(storage_info_));
    } else if (OB_FAIL(dump_tenant_backup_set_infos_(file_desc.backup_set_infos_))) {
        STORAGE_LOG(WARN, "fail to dump archive piece info file", K(ret), K(file_desc));
      }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_ls_meta_infos_file_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupLSMetaInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to inner print common header", K(ret));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {
    STORAGE_LOG(WARN, "fail to read archive piece info file", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(dump_backup_ls_meta_infos_file_(file_desc))) {
    STORAGE_LOG(WARN, "fail to dump archive piece info file", K(ret), K(file_desc));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_block_(
    const int64_t offset, const int64_t length, const int64_t idx)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_common_header_(*common_header))) {
      STORAGE_LOG(WARN, "failed to print common header", K(ret), KPC(common_header));
    } else {
      const int64_t data_length = common_header->data_zlength_;
      PrintHelper::print_dump_title("Macro Block");
      PrintHelper::print_dump_line("Idx", idx);
      PrintHelper::print_dump_line("MacroBlockLength", data_length);
      PrintHelper::print_end_line();
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_tablet_meta_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    backup::ObBackupTabletMeta tablet_meta;
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_common_header_(*common_header))) {
      STORAGE_LOG(WARN, "failed to print common header", K(ret), KPC(common_header));
    } else if (OB_FAIL(tablet_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
      STORAGE_LOG(WARN, "failed to read data_header", K(ret), K(*common_header), K(backup_path));
    } else if (OB_FAIL(dump_backup_tablet_meta_(tablet_meta))) {
      STORAGE_LOG(WARN, "failed to print backup tablet meta", K(ret), K(tablet_meta));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_sstable_metas_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_common_header_(*common_header))) {
      STORAGE_LOG(WARN, "failed to print common header", K(ret), KPC(common_header));
    } else {
      int64_t total_pos = 0;
      char *tmp_buf = buffer_reader.current();
      const int64_t total_data_size = common_header->data_zlength_;
      ObBackupSSTableMeta sstable_meta;
      while (OB_SUCC(ret) && total_data_size - total_pos > 0) {
        int64_t pos = 0;
        sstable_meta.reset();
        if (OB_FAIL(sstable_meta.deserialize(tmp_buf + total_pos, total_data_size - total_pos, pos))) {
          STORAGE_LOG(WARN, "failed to deserialize", K(ret), K(total_pos), K(total_data_size), K(*common_header));
        } else if (OB_FAIL(dump_backup_sstable_meta_(sstable_meta))) {
          STORAGE_LOG(WARN, "failed to push back", K(ret), K(sstable_meta));
        } else {
          total_pos += pos;
        }
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_backup_macro_block_id_mapping_metas_(
    const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    backup::ObBackupMacroBlockIDMappingsMeta mapping_meta;
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {
      STORAGE_LOG(WARN, "failed to get common header", K(ret), K(backup_path));
    } else if (OB_FAIL(dump_common_header_(*common_header))) {
      STORAGE_LOG(WARN, "failed to print common header", K(ret), KPC(common_header));
    } else if (OB_FAIL(mapping_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
      STORAGE_LOG(WARN, "failed to read data_header", K(ret), K(*common_header), K(backup_path));
    } else if (OB_FAIL(dump_backup_macro_block_id_mapping_meta_(mapping_meta))) {
      STORAGE_LOG(WARN, "failed to print backup tablet meta", K(ret), K(mapping_meta));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_block_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (0 == length) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_block_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMacroBlockIndex>(
            buffer_reader, print_func1, print_func2))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_meta_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (0 == length) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 = std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMetaIndex>(
            buffer_reader, print_func1, print_func2))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_range_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_range_index_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(backup_path));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndexIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_meta_index_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(length));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path));
  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMetaIndexIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {
      STORAGE_LOG(WARN, "failed to parse from index block", K(ret), K(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_common_header_(const char *data_path, const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  common::ObString tmp_data_path(data_path);
  common::ObString tmp_storage_info(storage_info_str);
  if (OB_FAIL(inner_print_common_header_(tmp_data_path, tmp_storage_info))) {
    STORAGE_LOG(WARN, "failed to common header", K(ret), K(tmp_data_path), K(tmp_storage_info));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_common_header_(const common::ObString &backup_path,
    const common::ObString &storage_info)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  share::ObBackupCommonHeader *header = nullptr;
  ObArenaAllocator allocator;
  const int64_t header_len = sizeof(share::ObBackupCommonHeader);

  if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, file_length))) {
    STORAGE_LOG(WARN, "failed to get file length", K(ret), K(backup_path), K(storage_info));
  } else if (OB_UNLIKELY(file_length <= sizeof(header_len))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup file too small", K(ret), K(file_length), K(header_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(header_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(header_len));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, 0, header_len, buf))) {
    STORAGE_LOG(WARN, "failed to pread file", K(ret), K(backup_path), K(storage_info), K(file_length), K(header_len));
  } else if (OB_FALSE_IT(header = reinterpret_cast<share::ObBackupCommonHeader *>(buf))) {
  } else if (OB_FAIL(header->check_valid())) {
    STORAGE_LOG(WARN, "fail to check common header valid");
  } else if (OB_FAIL(dump_common_header_(*header))) {
    STORAGE_LOG(WARN, "fail to dump common header", KPC(header));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_file_header_(const backup::ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup File Header");
  PrintHelper::print_dump_line("magic", file_header.magic_);
  PrintHelper::print_dump_line("version", file_header.version_);
  PrintHelper::print_dump_line("file_type", file_header.file_type_);
  PrintHelper::print_dump_line("compressor_type", file_header.reserved_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_common_header_(const share::ObBackupCommonHeader &common_header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Common Header");
  PrintHelper::print_dump_line("data_type", common_header.data_type_);
  PrintHelper::print_dump_line("header_version", common_header.header_version_);
  PrintHelper::print_dump_line("data_version", common_header.data_version_);
  PrintHelper::print_dump_line("compressor_type", common_header.compressor_type_);
  PrintHelper::print_dump_line("header_length", common_header.header_length_);
  PrintHelper::print_dump_line("header_checksum", common_header.header_checksum_);
  PrintHelper::print_dump_line("data_length", common_header.data_length_);
  PrintHelper::print_dump_line("data_zlength", common_header.data_zlength_);
  PrintHelper::print_dump_line("data_checksum", common_header.data_checksum_);
  PrintHelper::print_dump_line("align_length", common_header.align_length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_data_file_trailer_(const backup::ObBackupDataFileTrailer &trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Data File Trailer");
  PrintHelper::print_dump_line("data_type", trailer.data_type_);
  PrintHelper::print_dump_line("data_version", trailer.data_version_);
  PrintHelper::print_dump_line("macro_block_count", trailer.macro_block_count_);
  PrintHelper::print_dump_line("meta_count", trailer.meta_count_);
  PrintHelper::print_dump_line("macro_index_offset", trailer.macro_index_offset_);
  PrintHelper::print_dump_line("macro_index_length", trailer.macro_index_length_);
  PrintHelper::print_dump_line("meta_index_offset", trailer.meta_index_offset_);
  PrintHelper::print_dump_line("meta_index_length", trailer.meta_index_length_);
  PrintHelper::print_dump_line("offset", trailer.offset_);
  PrintHelper::print_dump_line("length", trailer.length_);
  PrintHelper::print_dump_line("data_accumulate_checksum", trailer.data_accumulate_checksum_);
  PrintHelper::print_dump_line("trailer_checksum", trailer.trailer_checksum_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tablet_trailer_(const backup::ObTabletInfoTrailer &tablet_meta_trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Tablet Meta Trailer");
  PrintHelper::print_dump_line("file_id", tablet_meta_trailer.file_id_);
  PrintHelper::print_dump_line("tablet_count", tablet_meta_trailer.tablet_cnt_);
  PrintHelper::print_dump_line("offset", tablet_meta_trailer.offset_);
  PrintHelper::print_dump_line("length", tablet_meta_trailer.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_index_file_trailer_(const backup::ObBackupMultiLevelIndexTrailer &trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Index File Trailer");
  PrintHelper::print_dump_line("file_type", trailer.file_type_);
  PrintHelper::print_dump_line("tree_height", trailer.tree_height_);
  PrintHelper::print_dump_line("last_block_offset", trailer.last_block_offset_);
  PrintHelper::print_dump_line("last_block_length", trailer.last_block_length_);
  PrintHelper::print_dump_line("checksum", trailer.checksum_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_(const backup::ObBackupMultiLevelIndexHeader &header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Multi Level Index Header");
  PrintHelper::print_dump_line("magic", header.magic_);
  PrintHelper::print_dump_line("backup_type", header.backup_type_);
  PrintHelper::print_dump_line("index_level", header.index_level_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_(const backup::ObBackupMacroBlockIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Block Index");
  PrintHelper::print_dump_line("logic_id", to_cstring(index.logic_id_));
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_list_(
    const common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroBlockIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_block_index_(index))) {
      STORAGE_LOG(WARN, "failed to print macro block index", K(ret), K_(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_(const backup::ObBackupMacroRangeIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Range Index");
  PrintHelper::print_dump_line("start_key:tablet_id", index.start_key_.tablet_id_);
  PrintHelper::print_dump_line("start_key:logic_version", index.start_key_.logic_version_);
  PrintHelper::print_dump_line("start_key:data_seq", index.start_key_.data_seq_);
  PrintHelper::print_dump_line("end_key:tablet_id", index.end_key_.tablet_id_);
  PrintHelper::print_dump_line("end_key:logic_version", index.end_key_.logic_version_);
  PrintHelper::print_dump_line("end_key:data_seq", index.end_key_.data_seq_);
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_(
    const common::ObIArray<backup::ObBackupMacroRangeIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroRangeIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_range_index_(index))) {
      STORAGE_LOG(WARN, "failed to print macro range index", K(ret), K(index));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_(const backup::ObBackupMacroRangeIndexIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Range Index Index");
  PrintHelper::print_dump_line("end_key:start_key", to_cstring(index.end_key_.start_key_));
  PrintHelper::print_dump_line("end_key:end_key", to_cstring(index.end_key_.end_key_));
  PrintHelper::print_dump_line("end_key:backup_set_id", index.end_key_.backup_set_id_);
  PrintHelper::print_dump_line("end_key:ls_id", index.end_key_.ls_id_.id());
  PrintHelper::print_dump_line("end_key:turn_id", index.end_key_.turn_id_);
  PrintHelper::print_dump_line("end_key:retry_id", index.end_key_.retry_id_);
  PrintHelper::print_dump_line("end_key:file_id", index.end_key_.file_id_);
  PrintHelper::print_dump_line("end_key:offset", index.end_key_.offset_);
  PrintHelper::print_dump_line("end_key:length", index.end_key_.length_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_(
    const common::ObIArray<backup::ObBackupMacroRangeIndexIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroRangeIndexIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_range_index_index_(index))) {
      STORAGE_LOG(WARN, "failed to print macro range index index", K(ret), K(index));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_(const backup::ObBackupMetaIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Meta Index");
  PrintHelper::print_dump_line("meta_key", to_cstring(index.meta_key_));
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_list_(const common::ObIArray<backup::ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMetaIndex &index = index_list.at(i);
    if (OB_FAIL(dump_meta_index_(index))) {
      STORAGE_LOG(WARN, "failed to print meta index", K(ret), K_(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_index_(const backup::ObBackupMetaIndexIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Meta Index Index");
  PrintHelper::print_dump_line("tablet_id", index.end_key_.meta_key_.tablet_id_.id());
  PrintHelper::print_dump_line("meta_type", index.end_key_.meta_key_.meta_type_);
  PrintHelper::print_dump_line("backup_set_id", index.end_key_.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.end_key_.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.end_key_.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.end_key_.retry_id_);
  PrintHelper::print_dump_line("file_id", index.end_key_.file_id_);
  PrintHelper::print_dump_line("offset", index.end_key_.offset_);
  PrintHelper::print_dump_line("length", index.end_key_.length_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_(
    const common::ObIArray<backup::ObBackupMetaIndexIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMetaIndexIndex &index = index_list.at(i);
    if (OB_FAIL(dump_meta_index_index_(index))) {
      STORAGE_LOG(WARN, "failed to print meta index index", K(ret), K_(backup_path));
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_tablet_meta_(const backup::ObBackupTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Tablet Meta");
  PrintHelper::print_dump_line("tablet_id", tablet_meta.tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:ls_id", tablet_meta.tablet_meta_.ls_id_.id());
  PrintHelper::print_dump_line("tablet_meta:tablet_id", tablet_meta.tablet_meta_.tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:data_tablet_id", tablet_meta.tablet_meta_.data_tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:ref_tablet_id", tablet_meta.tablet_meta_.ref_tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:clog_checkpoint_scn", tablet_meta.tablet_meta_.clog_checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("tablet_meta:ddl_checkpoint_scn", tablet_meta.tablet_meta_.ddl_checkpoint_scn_.get_val_for_logservice());
  // TODO yq: print migration param tablet status
  // PrintHelper::print_dump_line("tablet_meta:tablet_status", tablet_meta.tablet_meta_.tx_data_.tablet_status_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_sstable_meta_(const backup::ObBackupSSTableMeta &sstable_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup SSTable Meta");
  PrintHelper::print_dump_line("tablet_id", sstable_meta.tablet_id_.id());
  PrintHelper::print_dump_line("table_key", to_cstring(sstable_meta.sstable_meta_.table_key_));
  PrintHelper::print_dump_line("sstable_meta:row_count", sstable_meta.sstable_meta_.basic_meta_.row_count_);
  PrintHelper::print_dump_line("sstable_meta:occupy_size", sstable_meta.sstable_meta_.basic_meta_.occupy_size_);
  PrintHelper::print_dump_line("sstable_meta:original_size", sstable_meta.sstable_meta_.basic_meta_.original_size_);
  PrintHelper::print_dump_line("sstable_meta:data_checksum", sstable_meta.sstable_meta_.basic_meta_.data_checksum_);
  PrintHelper::print_dump_line("sstable_meta:index_type", sstable_meta.sstable_meta_.basic_meta_.index_type_);
  PrintHelper::print_dump_line("sstable_meta:rowkey_column_count", sstable_meta.sstable_meta_.basic_meta_.rowkey_column_count_);
  PrintHelper::print_dump_line("sstable_meta:column_cnt", sstable_meta.sstable_meta_.basic_meta_.column_cnt_);
  PrintHelper::print_dump_line("sstable_meta:data_macro_block_count", sstable_meta.sstable_meta_.basic_meta_.data_macro_block_count_);
  PrintHelper::print_dump_list_start("logic_id_array");
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_meta.logic_id_list_.count(); ++i) {
    const blocksstable::ObLogicMacroBlockId &macro_id = sstable_meta.logic_id_list_.at(i);
    PrintHelper::print_dump_list_value(to_cstring(macro_id), i == sstable_meta.logic_id_list_.count() - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_macro_block_id_mapping_meta_(
    const backup::ObBackupMacroBlockIDMappingsMeta &mapping_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup SSTable Macro Block ID Mapping Meta");
  PrintHelper::print_dump_line("version", mapping_meta.version_);
  PrintHelper::print_dump_line("sstable_count", mapping_meta.sstable_count_);
  for (int64_t i = 0; OB_SUCC(ret) && i < mapping_meta.sstable_count_; ++i) {
    const ObBackupMacroBlockIDMapping &item = mapping_meta.id_map_list_[i];
    const ObITable::TableKey &table_key = item.table_key_;
    int64_t num_of_entries = item.id_pair_list_.count();
    PrintHelper::print_dump_line("table_key", to_cstring(table_key));
    PrintHelper::print_dump_line("num_of_entries", num_of_entries);
    for (int64_t j = 0; OB_SUCC(ret) && j < num_of_entries; ++j) {
      const ObBackupMacroBlockIDPair &pair = item.id_pair_list_.at(j);
      const blocksstable::ObLogicMacroBlockId &logic_id = pair.logic_id_;
      const ObBackupPhysicalID &physical_id = pair.physical_id_;
      PrintHelper::print_dump_line("logic_id", to_cstring(logic_id));
      PrintHelper::print_dump_line("physical_id", to_cstring(physical_id));
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_ls_attr_info_(const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("ls_attr info");
  PrintHelper::print_dump_line("ls_id", ls_attr.get_ls_id().id());
  PrintHelper::print_dump_line("ls_group_id", ls_attr.get_ls_group_id());
  PrintHelper::print_dump_line("flag", ls_attr.get_ls_flag().get_flag_value());
  PrintHelper::print_dump_line("status", ls_attr.get_ls_status());
  PrintHelper::print_dump_line("operation_type", ls_attr.get_ls_operation_type());
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tablet_to_ls_info_(const storage::ObBackupDataTabletToLSInfo &tablet_to_ls_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("tablet_to_ls info");
  PrintHelper::print_dump_line("ls_id", tablet_to_ls_info.ls_id_.id());
  PrintHelper::print_dump_list_start("tablet_id");
  ARRAY_FOREACH_X(tablet_to_ls_info.tablet_id_list_, i , cnt, OB_SUCC(ret)) {
    const common::ObTabletID &tablet_id = tablet_to_ls_info.tablet_id_list_.at(i);
    PrintHelper::print_dump_list_value(to_cstring(tablet_id.id()), i == cnt - 1);
  }
  PrintHelper::print_dump_list_end();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_locality_info_(const storage::ObExternTenantLocalityInfoDesc &locality_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("locality info");
  PrintHelper::print_dump_line("tenant_id", locality_info.tenant_id_);
  PrintHelper::print_dump_line("tenant_name", locality_info.tenant_name_.ptr());
  PrintHelper::print_dump_line("backup_set_id", locality_info.backup_set_id_);
  PrintHelper::print_dump_line("cluster_id", locality_info.cluster_id_);
  PrintHelper::print_dump_line("cluster_name", locality_info.cluster_name_.ptr());
  PrintHelper::print_dump_line("compat_mode", static_cast<int64_t>(locality_info.compat_mode_));
  PrintHelper::print_dump_line("locality", locality_info.locality_.ptr());
  PrintHelper::print_dump_line("primary_zone", locality_info.primary_zone_.ptr());
  PrintHelper::print_dump_line("sys_time_zone", locality_info.sys_time_zone_.ptr());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_diagnose_info_(const storage::ObExternTenantDiagnoseInfoDesc &diagnose_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("diagnose info");
  if (OB_FAIL(dump_tenant_locality_info_(diagnose_info.tenant_locality_info_))) {
    STORAGE_LOG(WARN, "fail to dump tenant locality info", K(ret), K(diagnose_info));
  } else if (OB_FAIL(dump_backup_set_info(diagnose_info.backup_set_file_))) {
    STORAGE_LOG(WARN, "fail to dump backup set info", K(ret), K(diagnose_info));
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_backup_set_infos_(const ObIArray<oceanbase::share::ObBackupSetFileDesc> &backup_set_infos)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataStore store;
  ObTimeZoneInfoWrap time_zone_wrap;
  ObBackupDest backup_set_dest;
  if (OB_FAIL(backup_set_dest.set(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to set backup set dest", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    STORAGE_LOG(WARN, "fail to init backup data store", K(ret), K(backup_path_), K(storage_info_));
  } else if (OB_FAIL(store.get_backup_sys_time_zone_wrap(time_zone_wrap))) {
    STORAGE_LOG(WARN, "fail to get backup sys time zone wrap", K(ret), K(store));
  } else {
    PrintHelper::print_dump_title("tenant backup set infos");
    ARRAY_FOREACH_X(backup_set_infos, i , cnt, OB_SUCC(ret)) {
      const share::ObBackupSetFileDesc &backup_set_desc = backup_set_infos.at(i);
      int64_t pos = 0;
      char buf[OB_MAX_CHAR_LEN] = { 0 };
      char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
      char min_restore_scn_str_buf[OB_MAX_TIME_STR_LENGTH] = { 0 };
      if (OB_FAIL(ObTimeConverter::scn_to_str(backup_set_desc.min_restore_scn_.get_val_for_inner_table_field(),
                                              time_zone_wrap.get_time_zone_info(),
                                              min_restore_scn_str_buf,
                                              OB_MAX_TIME_STR_LENGTH,
                                              pos))) {
        STORAGE_LOG(WARN, "fail to convert scn to str", K(ret), K(time_zone_wrap), K(backup_set_desc));
      } else if (OB_FAIL(databuff_printf(buf, OB_MAX_CHAR_LEN, "%ld", i+1))) {
        STORAGE_LOG(WARN, "fail to printf buf", K(ret), K(i));
      } else if (OB_FALSE_IT(backup_set_desc.to_string(min_restore_scn_str_buf, str_buf, OB_MAX_TEXT_LENGTH))) {
      } else {
        PrintHelper::print_dump_line(buf, str_buf);
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_ls_meta_infos_file_(const storage::ObBackupLSMetaInfosDesc &ls_meta_infos)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("ls meta infos");
  ARRAY_FOREACH_X(ls_meta_infos.ls_meta_packages_, i , cnt, OB_SUCC(ret)) {
    const storage::ObLSMetaPackage &meta_package = ls_meta_infos.ls_meta_packages_.at(i);
    char buf[OB_MAX_CHAR_LEN] = { 0 };
    char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
    if (OB_FAIL(databuff_printf(buf, OB_MAX_CHAR_LEN, "%ld", i+1))) {
      STORAGE_LOG(WARN, "fail to printf buf", K(ret), K(i));
    } else if (OB_FALSE_IT(meta_package.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
    } else {
      PrintHelper::print_dump_line(buf, str_buf);
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_set_info(const share::ObBackupSetFileDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  char tenant_version_display[OB_CLUSTER_VERSION_LENGTH] = "";
  char cluster_version_display[OB_CLUSTER_VERSION_LENGTH] = "";
  int64_t pos =  ObClusterVersion::print_version_str(
    tenant_version_display, OB_CLUSTER_VERSION_LENGTH, backup_set_info.tenant_compatible_);
  pos =  ObClusterVersion::print_version_str(
    cluster_version_display, OB_CLUSTER_VERSION_LENGTH, backup_set_info.cluster_version_);

  PrintHelper::print_dump_title("backup set info");
  PrintHelper::print_dump_line("tenant_id", backup_set_info.tenant_id_);
  PrintHelper::print_dump_line("backup_set_id", backup_set_info.backup_set_id_);
  PrintHelper::print_dump_line("dest_id", backup_set_info.dest_id_);
  PrintHelper::print_dump_line("incarnation", backup_set_info.incarnation_);
  PrintHelper::print_dump_line("backup_type", backup_set_info.backup_type_.get_backup_type_str());
  PrintHelper::print_dump_line("backup_date", backup_set_info.date_);
  PrintHelper::print_dump_line("prev_full_backup_set_id", backup_set_info.prev_full_backup_set_id_);
  PrintHelper::print_dump_line("prev_inc_backup_set_id", backup_set_info.prev_inc_backup_set_id_);
  PrintHelper::print_dump_line("input_bytes", backup_set_info.stats_.input_bytes_);
  PrintHelper::print_dump_line("output_bytes", backup_set_info.stats_.output_bytes_);
  PrintHelper::print_dump_line("tablet_count", backup_set_info.stats_.tablet_count_);
  PrintHelper::print_dump_line("finish tablet count", backup_set_info.stats_.finish_tablet_count_);
  PrintHelper::print_dump_line("macro block count", backup_set_info.stats_.macro_block_count_);
  PrintHelper::print_dump_line("finish macro block count", backup_set_info.stats_.finish_macro_block_count_);
  PrintHelper::print_dump_line("extra_meta_bytes", backup_set_info.stats_.extra_bytes_);
  PrintHelper::print_dump_line("finish file count", backup_set_info.stats_.finish_file_count_);
  PrintHelper::print_dump_line("start_time", backup_set_info.start_time_);
  PrintHelper::print_dump_line("end_time", backup_set_info.end_time_);
  PrintHelper::print_dump_line("status", backup_set_info.status_);
  PrintHelper::print_dump_line("result", backup_set_info.result_);
  PrintHelper::print_dump_line("encryption_mode", backup_set_info.encryption_mode_);
  PrintHelper::print_dump_line("passwd", backup_set_info.passwd_.ptr());
  PrintHelper::print_dump_line("file_status", backup_set_info.file_status_);
  PrintHelper::print_dump_line("backup_path", backup_set_info.backup_path_.ptr());
  PrintHelper::print_dump_line("start_replay_scn", backup_set_info.start_replay_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("min_restore_scn", backup_set_info.min_restore_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("tenant_compatible", tenant_version_display);
  PrintHelper::print_dump_line("cluster_version", cluster_version_display);
  PrintHelper::print_dump_line("backup_compatible", backup_set_info.backup_compatible_);
  PrintHelper::print_dump_line("meta_turn_id", backup_set_info.meta_turn_id_);
  PrintHelper::print_dump_line("data_turn_id", backup_set_info.data_turn_id_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_round_start_file_(const share::ObRoundStartDesc &round_start_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive round start info");
  PrintHelper::print_dump_line("dest_id", round_start_file.dest_id_);
  PrintHelper::print_dump_line("round_id", round_start_file.round_id_);
  PrintHelper::print_dump_line("start_scn", round_start_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("base_piece_id", round_start_file.base_piece_id_);
  PrintHelper::print_dump_line("piece_switch_interval", round_start_file.piece_switch_interval_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_round_end_file_(const share::ObRoundEndDesc round_end_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive round end info");
  PrintHelper::print_dump_line("dest_id", round_end_file.dest_id_);
  PrintHelper::print_dump_line("round_id", round_end_file.round_id_);
  PrintHelper::print_dump_line("start_scn", round_end_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", round_end_file.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("base_piece_id", round_end_file.base_piece_id_);
  PrintHelper::print_dump_line("piece_switch_interval", round_end_file.piece_switch_interval_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_start_file_(const share::ObPieceStartDesc &piece_start_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece start info");
  PrintHelper::print_dump_line("dest_id", piece_start_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_start_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_start_file.piece_id_);
  PrintHelper::print_dump_line("start_scn", piece_start_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_end_file_(const share::ObPieceEndDesc &piece_end_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece end info");
  PrintHelper::print_dump_line("dest_id", piece_end_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_end_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_end_file.piece_id_);
  PrintHelper::print_dump_line("end_scn", piece_end_file.end_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_archive_piece_infos_file_(const share::ObTenantArchivePieceInfosDesc &piece_infos_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece infos");
  PrintHelper::print_dump_line("dest_id", piece_infos_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_infos_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_infos_file.piece_id_);
  PrintHelper::print_dump_line("incarnation", piece_infos_file.incarnation_);
  PrintHelper::print_dump_line("dest_no", piece_infos_file.dest_no_);
  PrintHelper::print_dump_line("compatible", static_cast<int64_t>(piece_infos_file.compatible_.version_));
  PrintHelper::print_dump_line("start_scn", piece_infos_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("end_scn", piece_infos_file.end_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("path", piece_infos_file.path_.ptr());
  ARRAY_FOREACH_X(piece_infos_file.his_frozen_pieces_, i , cnt, OB_SUCC(ret)) {
    const ObTenantArchivePieceAttr &piece = piece_infos_file.his_frozen_pieces_.at(i);
    char buf[OB_MAX_CHAR_LEN] = { 0 };
    char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
    if (OB_FAIL(databuff_printf(buf, OB_MAX_CHAR_LEN, "%ld", i+1))) {
      STORAGE_LOG(WARN, "fail to printf buf", K(ret), K(i));
    } else if (OB_FALSE_IT(piece.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
    } else {
      PrintHelper::print_dump_line(buf, str_buf);
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_one_piece_(const share::ObTenantArchivePieceAttr &piece)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_line("tenant_id", piece.key_.tenant_id_);
  PrintHelper::print_dump_line("dest_id", piece.key_.dest_id_);
  PrintHelper::print_dump_line("round_id", piece.key_.round_id_);
  PrintHelper::print_dump_line("piece_id", piece.key_.piece_id_);
  PrintHelper::print_dump_line("incarnation", piece.incarnation_);
  PrintHelper::print_dump_line("dest_no", piece.dest_no_);
  PrintHelper::print_dump_line("file_count", piece.file_count_);
  PrintHelper::print_dump_line("start_scn", piece.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", piece.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("max_scn", piece.max_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("end_scn", piece.end_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("compatible", static_cast<int64_t>(piece.compatible_.version_));
  PrintHelper::print_dump_line("input_bytes", piece.input_bytes_);
  PrintHelper::print_dump_line("output_bytes", piece.output_bytes_);
  PrintHelper::print_dump_line("status", piece.status_.to_status_str());
  PrintHelper::print_dump_line("file_status", ObBackupFileStatus::get_str(piece.file_status_));
  PrintHelper::print_dump_line("cp_file_id", piece.cp_file_id_);
  PrintHelper::print_dump_line("cp_file_offset", piece.cp_file_offset_);
  PrintHelper::print_dump_line("path", piece.path_.ptr());
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_single_piece_file_(const share::ObSinglePieceDesc &piece_single_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive single piece info");
  dump_one_piece_(piece_single_file.piece_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_inner_placeholder_file_(const share::ObPieceInnerPlaceholderDesc &piece_inner_placeholder)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece inner placeholder info");
  PrintHelper::print_dump_line("dest_id", piece_inner_placeholder.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_inner_placeholder.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_inner_placeholder.piece_id_);
  PrintHelper::print_dump_line("start_scn", piece_inner_placeholder.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", piece_inner_placeholder.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_single_ls_info_file_(const share::ObSingleLSInfoDesc &single_ls_info_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive single ls info");
  PrintHelper::print_dump_line("dest_id", single_ls_info_file.dest_id_);
  PrintHelper::print_dump_line("round_id", single_ls_info_file.round_id_);
  PrintHelper::print_dump_line("piece_id", single_ls_info_file.piece_id_);
  PrintHelper::print_dump_line("ls_id", single_ls_info_file.ls_id_.id());
  PrintHelper::print_dump_line("checkpoint_scn", single_ls_info_file.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("max_lsn", single_ls_info_file.max_lsn_);
  ARRAY_FOREACH_X(single_ls_info_file.filelist_, i , cnt, OB_SUCC(ret)) {
    const ObSingleLSInfoDesc::OneFile &one_file = single_ls_info_file.filelist_.at(i);
    PrintHelper::print_dump_title("archive file", i, 1);
    PrintHelper::print_dump_line("file id", one_file.file_id_);
    PrintHelper::print_dump_line("bytes", one_file.size_bytes_);
    PrintHelper::print_end_line();
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_list_info_file_(const share::ObPieceInfoDesc &piece_info_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece list info");
  PrintHelper::print_dump_line("dest_id", piece_info_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_info_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_info_file.piece_id_);
  ARRAY_FOREACH_X(piece_info_file.filelist_, i , cnt, OB_SUCC(ret)) {
    const share::ObSingleLSInfoDesc &one_file = piece_info_file.filelist_.at(i);
    if (OB_FAIL(dump_archive_single_ls_info_file_(one_file))) {
      STORAGE_LOG(WARN, "fail to dump archive single ls info file", K(ret));
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_format_file_(const share::ObBackupFormatDesc &format_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("backup format info");
  PrintHelper::print_dump_line("cluster_name", format_file.cluster_name_.ptr());
  PrintHelper::print_dump_line("cluster_id", format_file.cluster_id_);
  PrintHelper::print_dump_line("tenant_name", format_file.tenant_name_.ptr());
  PrintHelper::print_dump_line("tenant_id", format_file.tenant_id_);
  PrintHelper::print_dump_line("path", format_file.path_.ptr());
  PrintHelper::print_dump_line("incarnation", format_file.incarnation_);
  PrintHelper::print_dump_line("dest_id", format_file.dest_id_);
  PrintHelper::print_dump_line("dest_type", format_file.dest_type_);
  PrintHelper::print_dump_line("ts", format_file.ts_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_check_exist_result_(
    const char *data_path, const char *storage_info_str, const bool is_exist)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("check_exist");
  PrintHelper::print_dump_line("uri", data_path);
  PrintHelper::print_dump_line("is_exist", is_exist ? "true" : "false");
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_autoinc_seq_(const share::ObTabletAutoincSeq &autoinc_seq)
{
  int ret = OB_SUCCESS;
  const share::ObTabletAutoincInterval *intervals = autoinc_seq.get_intervals();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_seq.get_intervals_count(); ++i) {
    const share::ObTabletAutoincInterval &interval = intervals[i];
    PrintHelper::print_dump_line("tablet_meta:autoinc_seq:start", interval.start_);
    PrintHelper::print_dump_line("tablet_meta:autoinc_seq:end", interval.end_);
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_tenant_backup_set_infos_path_(
    const share::ObBackupSetDesc &backup_set_dir_name,
    share::ObBackupPath &target_path)
{
  int ret = OB_SUCCESS;
  target_path.reset();
  if (OB_FAIL(target_path.init(backup_path_))) {
    STORAGE_LOG(WARN, "fail to init tmp_path", K(ret));
  } else if (OB_FAIL(target_path.join_backup_set(backup_set_dir_name))) {
    STORAGE_LOG(WARN, "fail to join backup set dir name", K(ret), K(backup_set_dir_name));
  } else if (OB_FAIL(target_path.join(OB_STR_TENANT_BACKUP_SET_INFOS, ObBackupFileSuffix::BACKUP))) {
    STORAGE_LOG(WARN, "fail to join tenant backup set infos", K(ret), K(backup_set_dir_name));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_set_placeholder_dir_path(
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (OB_FAIL(dest.set(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "fail to set backup dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(dest, path))) {
    STORAGE_LOG(WARN, "fail to get backup place holder dir path", K(ret), K(path));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::filter_backup_set_(
    const storage::ObTenantBackupSetInfosDesc &tenant_backup_set_infos,
    const ObSArray<share::ObBackupSetDesc> &placeholder_infos,
    ObIArray<share::ObBackupSetFileDesc> &target_backup_set)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tenant_backup_set_infos.backup_set_infos_, i, cnt, OB_SUCC(ret)) {
    const share::ObBackupSetFileDesc &backup_set_info = tenant_backup_set_infos.backup_set_infos_.at(i);
    share::ObBackupSetDesc backup_set_dir_name;
    backup_set_dir_name.backup_set_id_ = backup_set_info.backup_set_id_;
    backup_set_dir_name.backup_type_.type_ = backup_set_info.backup_type_.type_;
    ARRAY_FOREACH_X(placeholder_infos, j, cnt, OB_SUCC(ret)) {
      const share::ObBackupSetDesc &tmp_name = placeholder_infos.at(j);
      if (tmp_name == backup_set_dir_name) {
        if (OB_FAIL(target_backup_set.push_back(backup_set_info))) {
          STORAGE_LOG(WARN, "fail to get backup set dir name", K(ret), K(backup_set_info));
        }
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::read_locality_info_file(const char *tenant_backup_path, share::ObBackupSetDesc latest_backup_set_desc, storage::ObExternTenantLocalityInfoDesc &locality_info) {
  int ret = OB_SUCCESS;
  share::ObBackupDest tenant_backup_dest;
  share::ObBackupPath locality_info_path;
  if (OB_FAIL(tenant_backup_dest.set(tenant_backup_path, storage_info_))) {
    STORAGE_LOG(WARN, "fail to set tenant backup set dest", K(ret), K(tenant_backup_path));
  } else if (OB_FAIL(ObBackupPathUtil::get_locality_info_path(tenant_backup_dest, latest_backup_set_desc, locality_info_path))) {
    STORAGE_LOG(WARN, "fail to get locality info path", K(ret), K(tenant_backup_dest), K(latest_backup_set_desc));
  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(locality_info_path.get_obstr(), ObString(storage_info_), locality_info))) {
    STORAGE_LOG(WARN, "fail to read locality info file", K(ret), K(locality_info_path), K(storage_info_));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_tenant_backup_path_type_(const char *data_path, const char *storage_info_str, ObBackupDestType::TYPE &type)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc desc;
  bool is_exist = false;
  if (OB_FAIL(backup_dest.set(data_path, storage_info_str))) {
    STORAGE_LOG(WARN, "fail to set backup dest", K(ret));
  } else if (OB_FAIL(backup_store.init(backup_dest))) {
    STORAGE_LOG(WARN, "fail to init backup store", K(ret));
  } else if (OB_FAIL(backup_store.is_format_file_exist(is_exist))) {
    STORAGE_LOG(WARN, "fail to check format file exist", K(ret));
  } else if (!is_exist) {
    type = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  } else if (OB_FAIL(backup_store.read_format_file(desc))) {
    STORAGE_LOG(WARN, "fail to read format file", K(ret));
  } else {
    type = static_cast<ObBackupDestType::TYPE>(desc.dest_type_);
  }

  return ret;
}

int ObAdminDumpBackupDataExecutor::build_rounds_info_(
    const share::ObPieceKey &first_piece,
    const common::ObIArray<share::ObTenantArchivePieceAttr> &pieces,
    common::ObIArray<share::ObTenantArchiveRoundAttr> &rounds)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr round;
  for (int64_t i = 0; OB_SUCC(ret) && i < pieces.count(); i++) {
    const ObTenantArchivePieceAttr &current_piece = pieces.at(i);
    if (current_piece.key_.piece_id_ < first_piece.piece_id_) {
      // May be it is cleaned.
      continue;
    }

    if (current_piece.key_.dest_id_ != first_piece.dest_id_) {
      // This piece is archived to other path.
      continue;
    }

    if (round.round_id_ != current_piece.key_.round_id_) {
      if (round.is_valid() && OB_FAIL(rounds.push_back(round))) {
        STORAGE_LOG(WARN, "failed to push backup round", K(ret));
      } else {
        round.key_.tenant_id_ = current_piece.key_.tenant_id_;
        round.key_.dest_no_ = current_piece.dest_no_;
        round.incarnation_ = current_piece.incarnation_;
        round.dest_id_ = current_piece.key_.dest_id_;
        round.round_id_ = current_piece.key_.round_id_;

        round.start_scn_ = current_piece.start_scn_;
        round.checkpoint_scn_ = current_piece.checkpoint_scn_;
        round.max_scn_ = current_piece.max_scn_;
        round.compatible_ = current_piece.compatible_;
        round.base_piece_id_ = current_piece.key_.piece_id_;
        round.used_piece_id_ = current_piece.key_.piece_id_;
        round.piece_switch_interval_ = current_piece.end_scn_.convert_to_ts() - current_piece.start_scn_.convert_to_ts();
        round.path_ = current_piece.path_;

        if (current_piece.is_active()) {
          round.state_.set_doing();
          round.active_input_bytes_ = current_piece.input_bytes_;
          round.active_output_bytes_ = current_piece.output_bytes_;
          round.frozen_input_bytes_ = 0;
          round.frozen_output_bytes_ = 0;
        } else {
          round.state_.set_stop();
          round.active_input_bytes_ = 0;
          round.active_output_bytes_ = 0;
          round.frozen_input_bytes_ = current_piece.input_bytes_;
          round.frozen_output_bytes_ = current_piece.output_bytes_;
        }
      }
    } else {
      round.used_piece_id_ = current_piece.key_.piece_id_;
      round.checkpoint_scn_ = current_piece.checkpoint_scn_;
      round.max_scn_ = current_piece.max_scn_;
      if (current_piece.is_active()) {
        round.state_.set_doing();
        round.active_input_bytes_ = current_piece.input_bytes_;
        round.active_output_bytes_ = current_piece.output_bytes_;
      } else {
        round.state_.set_stop();
        round.frozen_input_bytes_ = current_piece.input_bytes_;
        round.frozen_output_bytes_ = current_piece.output_bytes_;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!round.is_valid()) {
    STORAGE_LOG(INFO, "no round");
  } else if (OB_FAIL(rounds.push_back(round))) {
    STORAGE_LOG(WARN, "failed to push backup round", K(ret));
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_archive_path_()
{
  int ret = OB_SUCCESS;
  ObArchiveStore store;
  ObBackupDest backup_dest;
  ObArray<ObPieceKey> piece_keys;
  ObArray<ObTenantArchiveRoundAttr> rounds;
  if (OB_FAIL(backup_dest.set(backup_path_, storage_info_))) {
    STORAGE_LOG(WARN, "failed to set backup dest", K(ret), K_(storage_info), K_(backup_path));
  } else if (OB_FAIL(store.init(backup_dest))) {
     STORAGE_LOG(WARN, "failed to init archive store", K(ret));
  } else if (OB_FAIL(store.get_all_piece_keys(piece_keys))) {
    STORAGE_LOG(WARN, "failed to get all piece keys", K(ret));
  } else {
    bool is_empty_piece = true;
    ObExternPieceWholeInfo piece_whole_info;
    for (int64_t i = piece_keys.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const ObPieceKey &key = piece_keys.at(i);
      if (OB_FAIL(store.get_whole_piece_info(key.dest_id_, key.round_id_, key.piece_id_, is_empty_piece, piece_whole_info))) {
        STORAGE_LOG(WARN, "failed to get whole piece info", K(ret), K(key));
      } else if (!is_empty_piece) {
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_empty_piece) {
    } else if (OB_FAIL(piece_whole_info.his_frozen_pieces_.push_back(piece_whole_info.current_piece_))) {
      STORAGE_LOG(WARN, "failed to push backup current piece", K(ret));
    } else if (OB_FAIL(build_rounds_info_(piece_keys.at(0), piece_whole_info.his_frozen_pieces_, rounds))) {
      STORAGE_LOG(WARN, "failed to build rounds info", K(ret), K(piece_keys), K(piece_whole_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    PrintHelper::print_dump_title("tenant archive round infos");
    ARRAY_FOREACH_X(rounds, i , cnt, OB_SUCC(ret)) {
      const ObTenantArchiveRoundAttr &round = rounds.at(i);
      char buf[OB_MAX_CHAR_LEN] = { 0 };
      char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
      if (OB_FAIL(databuff_printf(buf, OB_MAX_CHAR_LEN, "%ld", i+1))) {
        STORAGE_LOG(WARN, "fail to printf buf", K(ret), K(i));
      } else if (OB_FALSE_IT(round.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
      } else {
        PrintHelper::print_dump_line(buf, str_buf);
      }
    }
    PrintHelper::print_end_line();
  }

  return ret;
}

}  // namespace tools
}  // namespace oceanbase

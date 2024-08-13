/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_ADMIN_BACKUP_VALIDATION_UTIL_H_
#define OB_ADMIN_BACKUP_VALIDATION_UTIL_H_
#include "../ob_admin_executor.h"
#include "ob_admin_backup_validation_ctx.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_serialize_provider.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase
{
namespace tools
{
class ObAdminBackupValidationUtil
{
public:
  // static functions
  // read helper
  static int convert_comma_separated_string_to_array(const char *str,
                                                     common::ObArray<common::ObString> &str_array);

  static int check_dir_exist(const char *full_path, const share::ObBackupStorageInfo *storage_info,
                             bool &is_empty_dir);
  static int check_file_exist(const char *full_path, const share::ObBackupStorageInfo *storage_info,
                              bool &exist);

  static int get_file_length(const char *full_path, const share::ObBackupStorageInfo *storage_info,
                             int64_t &file_length);

  static int get_backup_common_header(blocksstable::ObBufferReader &buffer_reader,
                                      share::ObBackupCommonHeader &header);

  // notice: the pos will be advanced
  // by default, it will go the sizeof(BackupStructType), this is also the
  // typical read_size but under typical case, the data will be aligned to
  // DIO_READ_ALIGN_SIZE
  template <typename BackupStructType>
  static int read_backup_struct(const share::ObBackupPath &file_path, int64_t &pos,
                                const share::ObBackupStorageInfo &storage_info,
                                BackupStructType &backup_struct,
                                ObAdminBackupValidationCtx *ctx = nullptr,
                                int64_t advance_size = sizeof(BackupStructType))
  {
    int ret = OB_SUCCESS;
    common::ObBackupIoAdapter util;
    int64_t file_length = 0;
    int64_t read_size = 0;

    if (OB_FAIL(util.get_file_length(file_path.get_ptr(), &storage_info, file_length))) {
      STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
    } else if (file_length - pos < sizeof(backup_struct)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file not enough", K(ret), K(file_path), K(file_length));
    } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(sizeof(BackupStructType)))) {
    } else if (OB_FAIL(util.read_part_file(file_path.get_obstr(), &storage_info,
                                           reinterpret_cast<char *>(&backup_struct),
                                           sizeof(backup_struct), pos, read_size))) {
      STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
    } else if (read_size != sizeof(BackupStructType)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read length not match", K(ret), K(file_path), K(file_length),
                  K(read_size));
    } else if (OB_FAIL(backup_struct.check_valid())) {
      STORAGE_LOG(WARN, "backup struct is not valid", K(ret), K(file_path));
    } else if (FALSE_IT(pos += advance_size)) {
      // do nothing
    } else {
      STORAGE_LOG(INFO, "succeed to read backup struct", K(file_path));
    }
    if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
      ctx->go_abort(file_path.get_ptr(), "File not exist or physical corrputed");
    }
    return ret;
  }

  template <typename BackupSingleInfoFileType>
  static int read_backup_info_file(const share::ObBackupPath &file_path,
                                   const share::ObBackupStorageInfo &storage_info,
                                   BackupSingleInfoFileType &file_info,
                                   ObAdminBackupValidationCtx *ctx = nullptr)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    int64_t file_length = 0;
    char *buf = nullptr;
    int64_t read_size = 0;
    common::ObBackupIoAdapter util;
    common::ObArenaAllocator allocator;
    share::ObBackupSerializeHeaderWrapper serializer_wrapper(&file_info);

    if (OB_FAIL(util.get_file_length(file_path.get_ptr(), &storage_info, file_length))) {
      STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
    } else if (0 == file_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file is empty", K(ret), K(file_path));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(file_length));
    } else if (OB_NOT_NULL(ctx) && OB_FAIL(ctx->limit_and_sleep(file_length))) {
    } else if (OB_FAIL(util.read_single_file(file_path.get_obstr(), &storage_info, buf, file_length,
                                             read_size))) {
      STORAGE_LOG(WARN, "failed to read file", K(ret), K(file_path), K(file_length));
    } else if (file_length != read_size) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read file length not match", K(ret), K(file_path), K(file_length),
                  K(read_size));
    } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {
      // in deserialize, will automactically check the crc in the common header
      STORAGE_LOG(WARN, "failed to deserialize", K(ret), K(file_path), K(file_length));
    } else if (!serializer_wrapper.is_valid()) {
      STORAGE_LOG(WARN, "file_info is not valid", K(ret), K(file_path), K(file_length));
    } else {
      STORAGE_LOG(INFO, "succeed to read single file", K(file_path));
    }
    if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
      ctx->go_abort(file_path.get_ptr(), "File not exist or physical corrputed");
    }
    return ret;
  }
  template <class IndexType, class IndexIndexType>
  static int read_backup_index_file(const share::ObBackupPath &file_path,
                                    const share::ObBackupStorageInfo &storage_info,
                                    ObArray<IndexType> &index_list,
                                    ObArray<IndexIndexType> &index_index_list,
                                    ObAdminBackupValidationCtx *ctx = nullptr)
  {
    int ret = OB_SUCCESS;
    backup::ObBackupFileHeader file_header;
    backup::ObBackupMultiLevelIndexTrailer index_trailer;
    backup::ObBackupFileType file_type;
    common::ObBackupIoAdapter util;
    ObArenaAllocator tmp_allocator;
    int64_t pos = 0;
    int64_t file_length = -1;
    int64_t data_length = -1;
    int64_t read_size = -1;
    char *buf = nullptr;
    if (OB_FAIL(util.get_file_length(file_path.get_ptr(), &storage_info, file_length))) {
      STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
    } else if (OB_FAIL(read_backup_struct(file_path, pos, storage_info, file_header, ctx,
                                          DIO_ALIGN_SIZE /*file header aligned*/))) {
      STORAGE_LOG(WARN, "failed to read backup file header", K(ret), K(file_path));
    } else if (FALSE_IT(data_length = file_length - DIO_ALIGN_SIZE /*file header aligned*/
                                      - sizeof(backup::ObBackupMultiLevelIndexTrailer))
               || data_length <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file length should not be negative", K(ret), K(file_path), K(file_length));
    } else if (OB_ISNULL(buf = static_cast<char *>(tmp_allocator.alloc(data_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(data_length));
    } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(data_length))) {
    } else if (OB_FAIL(util.read_part_file(file_path.get_obstr(), &storage_info, buf, data_length,
                                           pos, read_size))) {
      STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
    } else if (read_size != data_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
    } else if (FALSE_IT(pos += read_size)) {
    } else if (OB_FAIL(read_backup_struct(file_path, pos, storage_info, index_trailer, ctx))) {
      STORAGE_LOG(WARN, "failed to read backup file trailer", K(ret), K(file_path));
    } else if (pos != file_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file struct is not correct", K(ret), K(file_path));
    }
    blocksstable::ObBufferReader buffer_reader(buf, data_length, 0);
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      // in each loop, read the common header first
      // then ObBackupMultiLevelIndexHeader
      // then some IndexIndexType/IndexType
      share::ObBackupCommonHeader common_header;
      backup::ObBackupMultiLevelIndexHeader index_header;
      int64_t end_pos;
      if (OB_FAIL(get_backup_common_header(buffer_reader, common_header))) {
        STORAGE_LOG(WARN, "failed to read backup common header", K(ret), K(file_path));
      } else if (FALSE_IT(end_pos = buffer_reader.pos() + common_header.data_length_)) {
      } else if (OB_FAIL(buffer_reader.read_serialize(index_header))) {
        STORAGE_LOG(WARN, "failed to read backup index header", K(ret), K(file_path));
      } else {
        while (OB_SUCC(ret) && buffer_reader.pos() < end_pos) {
          if (backup::OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
            IndexType index;
            if (OB_FAIL(buffer_reader.read_serialize(index))) {
              STORAGE_LOG(WARN, "failed to read backup index", K(ret), K(file_path));
            } else if (OB_FAIL(index_list.push_back(index))) {
              STORAGE_LOG(WARN, "failed to push back", K(ret), K(index));
            }
          } else {
            IndexIndexType index_index;
            if (OB_FAIL(buffer_reader.read_serialize(index_index))) {
              STORAGE_LOG(WARN, "failed to read backup index index", K(ret), K(file_path));
            } else if (OB_FAIL(index_index_list.push_back(index_index))) {
              STORAGE_LOG(WARN, "failed to push back", K(ret), K(index_index));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(buffer_reader.advance(common_header.align_length_))) {
          STORAGE_LOG(WARN, "failed to advance align length", K(ret), K(file_path));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // cross check the trailer and index read
      if (index_trailer.is_empty_index() && !index_list.empty()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "index_trailer is empty but index_list is not", K(ret), K(file_path));
      } else if (!index_index_list.empty() && index_list.empty()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "index_index_list is not empty but index_list is", K(ret), K(file_path));
      } else if (index_trailer.last_block_offset_
                 != index_index_list.at(index_index_list.size() - 1).offset_
                        + index_index_list.at(index_index_list.size() - 1).length_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "index_trailer does not match index_index_list", K(ret), K(file_path));
      } else {
        STORAGE_LOG(INFO, "succeed to read backup index file", K(file_path));
      }
    }
    if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
      ctx->go_abort(file_path.get_ptr(), "File not exist or physical corrputed");
    }
    return ret;
  }
  static int get_tenant_meta_index_retry_id(const share::ObBackupDest &backup_set_dest,
                                            const share::ObBackupDataType &backup_data_type,
                                            const backup::ObBackupFileType &backup_file_type,
                                            const int64_t turn_id, int64_t &retry_id);

  static int read_tablet_meta(const share::ObBackupDest &backup_set_dest,
                              const backup::ObBackupMetaIndex &meta_index,
                              const share::ObBackupDataType &backup_data_type,
                              backup::ObBackupTabletMeta &tablet_meta,
                              ObAdminBackupValidationCtx *ctx = nullptr);
  static int read_sstable_metas(const share::ObBackupDest &backup_set_dest,
                                const backup::ObBackupMetaIndex &meta_index,
                                const share::ObBackupDataType &backup_data_type,
                                common::ObArray<backup::ObBackupSSTableMeta> &sstable_metas,
                                ObAdminBackupValidationCtx *ctx = nullptr);
  static int
  read_macro_block_id_mappings_meta(const share::ObBackupDest &backup_set_dest,
                                    const backup::ObBackupMetaIndex &meta_index,
                                    const share::ObBackupDataType &backup_data_type,
                                    backup::ObBackupMacroBlockIDMappingsMeta &id_mappings_meta,
                                    ObAdminBackupValidationCtx *ctx = nullptr);

  static int read_macro_block_data(const share::ObBackupDest &backup_set_dest,
                                   const backup::ObBackupMacroBlockIndex &macro_index,
                                   const share::ObBackupDataType &backup_data_type,
                                   blocksstable::ObBufferReader &data_buffer,
                                   ObAdminBackupValidationCtx *ctx = nullptr,
                                   const int64_t align_size = DIO_READ_ALIGN_SIZE);

private:
  static int get_tenant_index_file_name_prefix_(const share::ObBackupDataType &backup_data_type,
                                                const backup::ObBackupFileType &backup_file_type,
                                                const char *&file_name);
};
}; // namespace tools
}; // namespace oceanbase
#endif
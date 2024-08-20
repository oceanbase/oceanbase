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
#include "logservice/archiveservice/ob_archive_define.h"
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

  template <typename BackupStructType, typename Enable = void> struct backup_sturct_reader;
  template <typename BackupStructType>
  struct backup_sturct_reader<BackupStructType,
                              typename std::enable_if<!std::is_same<
                                  BackupStructType, archive::ObArchiveFileHeader>::value>::type>
  {
    // notice: the pos will be advanced
    // by default, it will go the sizeof(BackupStructType), this is also the
    // typical read_size but under typical case, the data will be aligned to
    // DIO_READ_ALIGN_SIZE
    // THIS IS FOR PLAIN DATA
    static int read_backup_struct(const share::ObBackupPath &file_path,
                                  const share::ObBackupStorageInfo &storage_info,
                                  BackupStructType &backup_struct, int64_t &pos,
                                  ObAdminBackupValidationCtx *ctx = nullptr,
                                  int64_t advance_size = sizeof(BackupStructType))
    {
      int ret = OB_SUCCESS;
      common::ObBackupIoAdapter util;
      int64_t file_length = 0;
      int64_t read_size = 0;
      char *buf = nullptr;
      common::ObArenaAllocator allocator("ObAdmBakVal");
      if (file_path.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "file_path is empty", K(ret));
      } else if (!storage_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "storage_info is invalid", K(ret), K(storage_info));
      } else if (OB_FAIL(util.adaptively_get_file_length(file_path.get_ptr(), &storage_info,
                                                         file_length))) {
        STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
      } else if (file_length - pos < sizeof(backup_struct)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "file not enough", K(ret), K(file_path), K(file_length));
      } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(sizeof(BackupStructType)))) {
        STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(sizeof(BackupStructType)));
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(sizeof(backup_struct))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(sizeof(backup_struct)));
      } else if (OB_FAIL(util.adaptively_read_part_file(file_path.get_obstr(), &storage_info, buf,
                                                        sizeof(backup_struct), pos, read_size))) {
        STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
      } else if (read_size != sizeof(BackupStructType)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read length not match", K(ret), K(file_path), K(file_length),
                    K(read_size));
      } else {
        blocksstable::ObBufferReader buffer_reader(buf, sizeof(backup_struct));
        const BackupStructType *tmp_struct = nullptr;
        if (OB_FAIL(buffer_reader.get(tmp_struct))) {
          STORAGE_LOG(WARN, "failed to get tmp struct", K(ret));
        } else if (OB_ISNULL(tmp_struct)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup data tmp struct is null", K(ret));
        } else if (OB_FAIL(tmp_struct->check_valid())) {
          STORAGE_LOG(WARN, "failed to check is valid", K(ret), K(*tmp_struct));
        } else {
          backup_struct = *tmp_struct;
          pos += advance_size;
          STORAGE_LOG(INFO, "succeed to read backup struct", K(file_path));
        }
      }
      if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
        ctx->go_abort(file_path.get_ptr(), "File not exist or physical corrputed");
      }
      return ret;
    }
  };

  template <typename BackupStructType>
  struct backup_sturct_reader<BackupStructType,
                              typename std::enable_if<std::is_same<
                                  BackupStructType, archive::ObArchiveFileHeader>::value>::type>
  {
    // notice: the pos will be advanced
    // by default, it will go the sizeof(BackupStructType), this is also the
    // typical read_size but under typical case, the data will be aligned to
    // DIO_READ_ALIGN_SIZE
    // THIS IS FOR SERIALIZABLE DATA
    static int read_backup_struct(const share::ObBackupPath &file_path,
                                  const share::ObBackupStorageInfo &storage_info,
                                  BackupStructType &backup_struct, int64_t &pos,
                                  ObAdminBackupValidationCtx *ctx = nullptr,
                                  int64_t advance_size = sizeof(BackupStructType))
    {
      int ret = OB_SUCCESS;
      common::ObBackupIoAdapter util;
      int64_t file_length = 0;
      int64_t read_size = 0;
      char *buf = nullptr;
      common::ObArenaAllocator allocator("ObAdmBakVal");
      if (file_path.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "file_path is empty", K(ret));
      } else if (!storage_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "storage_info is invalid", K(ret), K(storage_info));
      } else if (OB_FAIL(util.adaptively_get_file_length(file_path.get_ptr(), &storage_info,
                                                         file_length))) {
        STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
      } else if (file_length - pos < sizeof(backup_struct)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "file not enough", K(ret), K(file_path), K(file_length));
      } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(sizeof(BackupStructType)))) {
        STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(sizeof(BackupStructType)));
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(sizeof(backup_struct))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(sizeof(backup_struct)));
      } else if (OB_FAIL(util.adaptively_read_part_file(file_path.get_obstr(), &storage_info, buf,
                                                        sizeof(backup_struct), pos, read_size))) {
        STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
      } else if (read_size != sizeof(BackupStructType)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read length not match", K(ret), K(file_path), K(file_length),
                    K(read_size));
      } else {
        if (OB_FAIL(backup_struct.deserialize(buf, sizeof(backup_struct), pos))) {
          STORAGE_LOG(WARN, "failed to deserialize", K(ret), K(file_path));
        } else if (!backup_struct.is_valid()) {
          STORAGE_LOG(WARN, "failed to check is valid", K(ret), K(backup_struct));
        } else {
          STORAGE_LOG(INFO, "succeed to read backup struct", K(file_path));
        }
      }
      if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
        ctx->go_abort(file_path.get_ptr(), "File not exist or physical corrputed");
      }
      return ret;
    }
  };

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
    common::ObArenaAllocator allocator("ObAdmBakVal");
    share::ObBackupSerializeHeaderWrapper serializer_wrapper(&file_info);

    if (file_path.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "file_path is empty", K(ret));
    } else if (!storage_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "storage_info is invalid", K(ret), K(storage_info));
    } else if (OB_FAIL(util.adaptively_get_file_length(file_path.get_ptr(), &storage_info,
                                                       file_length))) {
      STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
    } else if (0 == file_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file is empty", K(ret), K(file_path));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(file_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(file_length));
    } else if (OB_NOT_NULL(ctx) && OB_FAIL(ctx->limit_and_sleep(file_length))) {
      STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(file_length));
    } else if (OB_FAIL(util.adaptively_read_single_file(file_path.get_obstr(), &storage_info, buf,
                                                        file_length, read_size))) {
      STORAGE_LOG(WARN, "failed to read file", K(ret), K(file_path), K(file_length));
    } else if (file_length != read_size) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read file length not match", K(ret), K(file_path), K(file_length),
                  K(read_size));
    } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {
      // in deserialize, will automactically check the crc in the common header
      STORAGE_LOG(WARN, "failed to deserialize", K(ret), K(file_path), K(file_length));
    } else if (!serializer_wrapper.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
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
    ObArenaAllocator tmp_allocator("ObAdmBakVal");
    int64_t pos = 0;
    int64_t file_length = -1;
    int64_t data_length = -1;
    int64_t read_size = -1;
    char *buf = nullptr;
    if (file_path.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "file_path is empty", K(ret));
    } else if (!storage_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "storage_info is invalid", K(ret), K(storage_info));
    } else if (OB_FAIL(util.adaptively_get_file_length(file_path.get_ptr(), &storage_info,
                                                       file_length))) {
      STORAGE_LOG(WARN, "failed to get file length", K(ret), K(file_path));
    } else if (OB_FAIL(backup_sturct_reader<typeof(file_header)>::read_backup_struct(
                   file_path, storage_info, file_header, pos, ctx,
                   DIO_ALIGN_SIZE /*file header aligned*/))) {
      STORAGE_LOG(WARN, "failed to read backup file header", K(ret), K(file_path));
    } else if (OB_FAIL(file_header.check_valid())) {
      STORAGE_LOG(WARN, "file_header is not valid", K(ret), K(file_path));
    } else if (FALSE_IT(data_length = file_length - DIO_ALIGN_SIZE /*file header aligned*/
                                      - sizeof(backup::ObBackupMultiLevelIndexTrailer))
               || data_length <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file length should not be negative", K(ret), K(file_path), K(file_length));
    } else if (OB_ISNULL(buf = static_cast<char *>(tmp_allocator.alloc(data_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(data_length));
    } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(data_length))) {
      STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(data_length));
    } else if (OB_FAIL(util.adaptively_read_part_file(file_path.get_obstr(), &storage_info, buf,
                                                      data_length, pos, read_size))) {
      STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
    } else if (read_size != data_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to pread file", K(ret), K(file_path));
    } else if (FALSE_IT(pos += read_size)) {
    } else if (OB_FAIL(backup_sturct_reader<typeof(index_trailer)>::read_backup_struct(
                   file_path, storage_info, index_trailer, pos, ctx))) {
      STORAGE_LOG(WARN, "failed to read backup file trailer", K(ret), K(file_path));
    } else if (pos != file_length) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file struct is not correct", K(ret), K(file_path));
    } else if (OB_FAIL(index_trailer.check_valid())) {
      STORAGE_LOG(WARN, "index_trailer is not valid", K(ret), K(file_path));
    } else {
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
          STORAGE_LOG(WARN, "index_index_list is not empty but index_list is", K(ret),
                      K(file_path));
        } else if (index_trailer.last_block_offset_
                   != index_index_list.at(index_index_list.size() - 1).offset_
                          + index_index_list.at(index_index_list.size() - 1).length_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "index_trailer does not match index_index_list", K(ret), K(file_path));
        } else {
          STORAGE_LOG(INFO, "succeed to read backup index file", K(file_path));
        }
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

  static int get_ls_ids_from_active_piece(const share::ObBackupDest &backup_piece_dest,
                                          common::ObArray<share::ObLSID> &ls_ids,
                                          ObAdminBackupValidationCtx *ctx = nullptr);

  static int get_piece_ls_start_lsn(const share::ObBackupDest &backup_piece_dest,
                                    const share::ObLSID &ls_id, uint64_t &start_lsn,
                                    ObAdminBackupValidationCtx *ctx = nullptr);

private:
  static int get_tenant_index_file_name_prefix_(const share::ObBackupDataType &backup_data_type,
                                                const backup::ObBackupFileType &backup_file_type,
                                                const char *&file_name);

  class ObLSStartFileGetterOp final : public ObBaseDirEntryOperator
  {
  public:
    ObLSStartFileGetterOp();
    ~ObLSStartFileGetterOp();
    int func(const dirent *entry) override;
    int get_start_file_path(share::ObBackupPath &piece_ls_dir);
    TO_STRING_KV(K_(min_file_id));

  private:
    int64_t min_file_id_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObLSStartFileGetterOp);
  };
};

}; // namespace tools
}; // namespace oceanbase
#endif
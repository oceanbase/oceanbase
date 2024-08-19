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

#include "ob_admin_backup_validation_util.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_restore_util.h"

namespace oceanbase
{
namespace tools
{
// static functions
// read helper
int ObAdminBackupValidationUtil::convert_comma_separated_string_to_array(
    const char *str, common::ObArray<common::ObString> &str_array)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(str) || 0 == STRLEN(str)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(str));
  } else {
    int start_pos = 0;
    int end_pos = STRLEN(str) - 1;
    while (OB_SUCC(ret) && start_pos <= end_pos) {
      int item_start_pos = start_pos;
      // Find the position of the comma or the end of the content part
      while (start_pos <= end_pos && str[start_pos] != ',') {
        start_pos++;
      }
      int item_end_pos = start_pos - 1;
      if (item_start_pos <= item_end_pos) {
        common::ObString item(item_end_pos - item_start_pos + 1, str + item_start_pos);
        if (OB_FAIL(str_array.push_back(item))) {
          STORAGE_LOG(WARN, "failed to push back item", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // Move to the next item, skip the comma
        start_pos++;
      }
    }
  }

  return ret;
}

int ObAdminBackupValidationUtil::check_dir_exist(const char *full_path,
                                                 const share::ObBackupStorageInfo *storage_info,
                                                 bool &is_empty_dir)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  return util.is_empty_directory(full_path, storage_info, is_empty_dir);
}
int ObAdminBackupValidationUtil::check_file_exist(const char *full_path,
                                                  const share::ObBackupStorageInfo *storage_info,
                                                  bool &exist)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  return util.adaptively_is_exist(full_path, storage_info, exist);
}
int ObAdminBackupValidationUtil::get_file_length(const char *full_path,
                                                 const share::ObBackupStorageInfo *storage_info,
                                                 int64_t &file_length)
{
  common::ObBackupIoAdapter util;
  return util.adaptively_get_file_length(full_path, storage_info, file_length);
}
int ObAdminBackupValidationUtil::get_backup_common_header(
    blocksstable::ObBufferReader &buffer_reader, share::ObBackupCommonHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_reader.read(header))) {
    STORAGE_LOG(WARN, "failed to read common header", K(ret));
  } else if (OB_FAIL(header.check_valid())) {
    STORAGE_LOG(WARN, "common header is not valid", K(ret));
  } else if (common::ObCompressorType::NONE_COMPRESSOR != header.compressor_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "compression is currently not supported", K(ret));
  } else if (header.data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer reader not enough", K(ret));
  } else if (OB_FAIL(header.check_data_checksum(buffer_reader.current(), header.data_length_))) {
    STORAGE_LOG(WARN, "failed to check data checksum", K(ret), K(buffer_reader));
  }
  return ret;
}

int ObAdminBackupValidationUtil::get_tenant_meta_index_retry_id(
    const share::ObBackupDest &backup_set_dest, const share::ObBackupDataType &backup_data_type,
    const backup::ObBackupFileType &backup_file_type, const int64_t turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = 0;
  ObArray<int64_t> id_list;
  const char *file_name_prefix = nullptr;
  share::ObBackupPath full_path;
  common::ObBackupIoAdapter util;
  if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(backup_set_dest));
  } else if (OB_FAIL(get_tenant_index_file_name_prefix_(backup_data_type, backup_file_type,
                                                        file_name_prefix))) {
    STORAGE_LOG(WARN, "failed to get tenant index file name prefix", K(ret), K(backup_data_type),
                K(backup_file_type));
  } else {
    backup::ObBackupDataFileRangeOp file_range_op(file_name_prefix);
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_info_data_info_dir_path(
            backup_set_dest, backup_data_type, turn_id, full_path))) {
    } else if (OB_FAIL(util.adaptively_list_files(
                   full_path.get_obstr(), backup_set_dest.get_storage_info(), file_range_op))) {
      STORAGE_LOG(WARN, "failed to list files", K(ret), K(full_path), K(file_name_prefix));
    } else if (OB_FAIL(file_range_op.get_file_list(id_list))) {
      STORAGE_LOG(WARN, "failed to get file list", K(ret), K(full_path), K(file_name_prefix));
    } else if (id_list.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "id_list is empty", K(ret), K(full_path), K(file_name_prefix));
    } else {
      int64_t largest_retry_id = -1;
      FOREACH_CNT_X(tmp_id, id_list, OB_SUCC(ret))
      {
        if (OB_ISNULL(tmp_id)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tmp_id is null", K(ret));
        } else if (*tmp_id > largest_retry_id) {
          largest_retry_id = *tmp_id;
        }
      }
      if (OB_SUCC(ret)) {
        if (-1 == largest_retry_id) {
          ret = OB_ENTRY_NOT_EXIST;
          STORAGE_LOG(WARN, "largest_retry_id should not be -1", K(ret), K(full_path),
                      K(file_name_prefix));
        } else {
          retry_id = largest_retry_id;
          STORAGE_LOG(INFO, "succeed to get max tenant index retry id", K(full_path),
                      K(largest_retry_id));
        }
      }
    }
  }
  return ret;
}

int ObAdminBackupValidationUtil::get_tenant_index_file_name_prefix_(
    const share::ObBackupDataType &backup_data_type,
    const backup::ObBackupFileType &backup_file_type, const char *&file_name_prefix)
{
  int ret = OB_SUCCESS;
  file_name_prefix = nullptr;
  if (backup_data_type.is_minor_backup()) {
    switch (backup_file_type) {
    case backup::ObBackupFileType::BACKUP_MACRO_RANGE_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MINOR_MACRO_INDEX;
      break;
    case backup::ObBackupFileType::BACKUP_META_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MINOR_META_INDEX;
      break;
    case backup::ObBackupFileType::BACKUP_SEC_META_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MINOR_SEC_META_INDEX;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup file type not correct", K(ret), K(backup_file_type));
    }
  } else if (backup_data_type.is_major_backup()) {
    switch (backup_file_type) {
    case backup::ObBackupFileType::BACKUP_MACRO_RANGE_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MAJOR_MACRO_INDEX;
      break;
    case backup::ObBackupFileType::BACKUP_META_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MAJOR_META_INDEX;
      break;
    case backup::ObBackupFileType::BACKUP_SEC_META_INDEX_FILE:
      file_name_prefix = share::OB_STR_TENANT_MAJOR_SEC_META_INDEX;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup file type not correct", K(ret), K(backup_file_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data type not correct", K(ret), K(backup_data_type));
  }
  return ret;
}

int ObAdminBackupValidationUtil::read_tablet_meta(const share::ObBackupDest &backup_set_dest,
                                                  const backup::ObBackupMetaIndex &meta_index,
                                                  const share::ObBackupDataType &backup_data_type,
                                                  backup::ObBackupTabletMeta &tablet_meta,
                                                  ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath full_path;

  if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(backup_set_dest));
  } else if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(meta_index));
  } else if (backup::BACKUP_TABLET_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta type do not match", K(meta_index));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(
                 backup_set_dest, meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
                 meta_index.retry_id_, meta_index.file_id_, full_path))) {
    STORAGE_LOG(WARN, "failed to get macro block backup path", K(ret), K(backup_set_dest),
                K(meta_index));
  } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(meta_index.length_))) {
    STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(meta_index.length_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_tablet_meta(
                 full_path.get_obstr(), backup_set_dest.get_storage_info(), backup_data_type,
                 meta_index, tablet_meta))) {
    STORAGE_LOG(WARN, "failed to read tablet meta", K(ret), K(full_path));
  } else {
    STORAGE_LOG(DEBUG, "succeed to read tablet meta", K(ret), K(full_path),
                K(meta_index.meta_key_.tablet_id_));
  }

  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(full_path.get_ptr(), "File not exist or physical corrputed");
  }
  return ret;
}

int ObAdminBackupValidationUtil::read_sstable_metas(
    const share::ObBackupDest &backup_set_dest, const backup::ObBackupMetaIndex &meta_index,
    const share::ObBackupDataType &backup_data_type,
    common::ObArray<backup::ObBackupSSTableMeta> &sstable_metas, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath full_path;

  if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(backup_set_dest));
  } else if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(meta_index));
  } else if (backup::BACKUP_SSTABLE_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta type do not match", K(meta_index));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(
                 backup_set_dest, meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
                 meta_index.retry_id_, meta_index.file_id_, full_path))) {
    STORAGE_LOG(WARN, "failed to get macro block backup path", K(ret), K(backup_set_dest),
                K(meta_index));
  } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(meta_index.length_))) {
    STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(meta_index.length_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_sstable_metas(
                 full_path.get_obstr(), backup_set_dest.get_storage_info(), meta_index,
                 sstable_metas))) {
    STORAGE_LOG(WARN, "failed to read sstable metas", K(ret), K(full_path));
  } else {
    STORAGE_LOG(DEBUG, "succeed to read sstable meta", K(ret), K(full_path),
                K(meta_index.meta_key_.tablet_id_), K(sstable_metas.count()));
  }
  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(full_path.get_ptr(), "File not exist or physical corrputed");
  }
  return ret;
}

int ObAdminBackupValidationUtil::read_macro_block_id_mappings_meta(
    const share::ObBackupDest &backup_set_dest, const backup::ObBackupMetaIndex &meta_index,
    const share::ObBackupDataType &backup_data_type,
    backup::ObBackupMacroBlockIDMappingsMeta &id_mappings_meta, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath full_path;

  if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(backup_set_dest));
  } else if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(meta_index));
  } else if (backup::BACKUP_MACRO_BLOCK_ID_MAPPING_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta type do not match", K(meta_index));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(
                 backup_set_dest, meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
                 meta_index.retry_id_, meta_index.file_id_, full_path))) {
    STORAGE_LOG(WARN, "failed to get macro block backup path", K(ret), K(backup_set_dest),
                K(meta_index));
  } else if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(meta_index.length_))) {
    STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(meta_index.length_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_id_mapping_metas(
                 full_path.get_obstr(), backup_set_dest.get_storage_info(), meta_index,
                 id_mappings_meta))) {
    STORAGE_LOG(WARN, "failed to read sstable metas", K(ret), K(full_path));
  } else {
    STORAGE_LOG(DEBUG, "succeed to read id_mappings_meta", K(ret), K(full_path),
                K(meta_index.meta_key_.tablet_id_));
  }
  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(full_path.get_ptr(), "File not exist or physical corrputed");
  }
  return ret;
}

int ObAdminBackupValidationUtil::read_macro_block_data(
    const share::ObBackupDest &backup_set_dest, const backup::ObBackupMacroBlockIndex &macro_index,
    const share::ObBackupDataType &backup_data_type, blocksstable::ObBufferReader &data_buffer,
    ObAdminBackupValidationCtx *ctx, const int64_t align_size)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  share::ObBackupPath full_path;
  common::ObBackupIoAdapter util;
  common::ObArenaAllocator tmp_allocator("ObAdmBakVal");
  blocksstable::ObBufferReader buffer_reader;
  int64_t read_size = -1;

  if (!backup_set_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(backup_set_dest));
  } else if (align_size <= 0 || !common::is_io_aligned(macro_index.length_)
             || !common::is_io_aligned(align_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), K(macro_index), K(align_size));
  } else if (!macro_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(macro_index));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(
                 backup_set_dest, macro_index.ls_id_, backup_data_type, macro_index.turn_id_,
                 macro_index.retry_id_, macro_index.file_id_, full_path))) {
    STORAGE_LOG(WARN, "failed to get macro block backup path", K(ret), K(backup_set_dest),
                K(macro_index));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(tmp_allocator.alloc(macro_index.length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buffer", K(ret), K(macro_index));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, macro_index.length_);
    if (ctx != nullptr && OB_FAIL(ctx->limit_and_sleep(macro_index.length_))) {
      STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(macro_index.length_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data(
                   full_path.get_obstr(), backup_set_dest.get_storage_info(), macro_index,
                   DIO_READ_ALIGN_SIZE /*align_size*/, buffer_reader, data_buffer))) {
      STORAGE_LOG(WARN, "failed to read macro blocks", K(ret), K(full_path));
    } else {
      STORAGE_LOG(DEBUG, "succeed to read macro data", K(ret), K(full_path),
                  K(macro_index.logic_id_));
    }
  }
  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(full_path.get_ptr(), "File not exist or physical corrputed");
  }
  return ret;
}

int ObAdminBackupValidationUtil::get_ls_ids_from_active_piece(
    const share::ObBackupDest &backup_piece_dest, common::ObArray<share::ObLSID> &ls_ids,
    ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  char logstream_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = {0};
  if (OB_FAIL(databuff_printf(logstream_prefix, sizeof(logstream_prefix), "%s", "logstream_"))) {
    STORAGE_LOG(WARN, "failed to set log stream prefix", K(ret));
  } else if (OB_FAIL(prefix_op.init(logstream_prefix, strlen(logstream_prefix)))) {
    STORAGE_LOG(WARN, "failed to init dir prefix", K(ret), K(logstream_prefix));
  } else if (OB_FAIL(util.list_directories(backup_piece_dest.get_root_path(),
                                           backup_piece_dest.get_storage_info(), prefix_op))) {
    STORAGE_LOG(WARN, "failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObLSID ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      int64_t id_val = 0;
      tmp_entry = d_entrys.at(i);
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "file name is null", K(ret));
      } else if (sscanf(tmp_entry.name_, "logstream_%ld", &id_val) != 1) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "file name is null", K(ret));
      } else if (FALSE_IT(ls_id = id_val)) {
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        STORAGE_LOG(WARN, "failed to push back ls_ids", K(ret));
      }
    }
  }
  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(backup_piece_dest.get_root_path().ptr(), "File not exist or physical corrputed");
  }
  return ret;
}

int ObAdminBackupValidationUtil::get_piece_ls_start_lsn(
    const share::ObBackupDest &backup_piece_dest, const share::ObLSID &ls_id, uint64_t &start_lsn,
    ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  class ObLSStartFileGetterOp final : public ObBaseDirEntryOperator
  {
  public:
    ObLSStartFileGetterOp() : min_file_id_(INT64_MAX) {}
    ~ObLSStartFileGetterOp() {}
    int func(const dirent *entry) override
    {
      // The format of archive file name is like '100.obarc'.
      int ret = OB_SUCCESS;
      int64_t file_id_ = INT64_MAX;
      char *endptr = nullptr;
      const char *filename = nullptr;
      if (OB_ISNULL(entry)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid entry", K(ret));
      } else if (OB_FALSE_IT(filename = entry->d_name)) {
      } else if (STRLEN(filename) <= STRLEN(OB_ARCHIVE_SUFFIX)) {
        STORAGE_LOG(WARN, "ignore invalid file name", KCSTRING(filename));
      } else if (OB_FAIL(ob_strtoll(filename, endptr, file_id_))) {
        STORAGE_LOG(WARN, "ignore invalid file name", K(ret), KCSTRING(filename));
        ret = OB_SUCCESS;
      } else if (OB_ISNULL(endptr) || 0 != STRCMP(endptr, OB_ARCHIVE_SUFFIX)) {
        STORAGE_LOG(WARN, "ignore invalid file name", KCSTRING(filename));
      } else if (file_id_ < min_file_id_) {
        min_file_id_ = file_id_;
      }
      return ret;
    }
    int get_start_file_path(share::ObBackupPath &piece_ls_dir)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(piece_ls_dir.join(min_file_id_, ObBackupFileSuffix::ARCHIVE))) {
        STORAGE_LOG(WARN, "failed to join file id", K(ret), K(min_file_id_));
      }
      return ret;
    }
    TO_STRING_KV(K_(min_file_id));

  private:
    int64_t min_file_id_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObLSStartFileGetterOp);
  };

  share::ObBackupPath full_path;
  ObBackupIoAdapter util;
  ObLSStartFileGetterOp op;
  archive::ObArchiveFileHeader file_header;
  int64_t pos = 0;
  if (OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(backup_piece_dest, ls_id, full_path))) {
    STORAGE_LOG(WARN, "failed to get piece ls log dir path", K(ret), K(backup_piece_dest),
                K(ls_id));
  } else if (OB_FAIL(util.adaptively_list_files(full_path.get_ptr(),
                                                backup_piece_dest.get_storage_info(), op))) {
    STORAGE_LOG(WARN, "failed to list files", K(ret), K(full_path));
  } else if (OB_FAIL(op.get_start_file_path(full_path))) {
    STORAGE_LOG(WARN, "failed to get start file path", K(ret), K(full_path));
  } else if (OB_FAIL(backup_sturct_reader<typeof(file_header)>::read_backup_struct(
                 full_path, *backup_piece_dest.get_storage_info(), file_header, pos, ctx))) {
    STORAGE_LOG(WARN, "failed to read backup struct", K(ret), K(full_path));
  } else if (!file_header.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid file header", K(ret), K(file_header));
  } else {
    start_lsn = file_header.start_lsn_;
    STORAGE_LOG(DEBUG, "succeed to get piece ls start lsn", K(ret), K(start_lsn), K(full_path));
  }
  if (OB_NOT_NULL(ctx) && OB_FAIL(ret)) {
    ctx->go_abort(full_path.get_ptr(), "File not exist or physical corrputed");
  }
  return ret;
}
}; // namespace tools
}; // namespace oceanbase
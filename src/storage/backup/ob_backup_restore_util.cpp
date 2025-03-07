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
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_meta_cache.h"

using namespace oceanbase::share;
namespace oceanbase {
namespace backup {

int ObLSBackupRestoreUtil::read_tablet_meta(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
    const ObBackupMetaIndex &meta_index, ObBackupTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(meta_index));
  } else if (BACKUP_TABLET_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type do not match", K(meta_index));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(meta_index.length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(pread_file(path, storage_info, mod, meta_index.offset_, meta_index.length_, buf))) {
    LOG_WARN("failed to pread buffer", K(ret), K(path), K(meta_index));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, meta_index.length_);
    const ObBackupCommonHeader *common_header = NULL;
    int64_t pos = 0;
    if (OB_FAIL(buffer_reader.get(common_header))) {
      LOG_WARN("failed to get common_header", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common_header is not valid", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer_reader not enough", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(tablet_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
      LOG_WARN("failed to read data_header", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else {
      FLOG_INFO("read tablet meta", K(path), K(meta_index), K(tablet_meta));
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::read_sstable_metas(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
    const ObBackupMetaIndex &meta_index, ObBackupMetaKVCache *kv_cache, common::ObIArray<ObBackupSSTableMeta> &sstable_metas)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  ObBackupMetaCacheReader cache_reader;
  ObKVCacheHandle cache_handle;
  blocksstable::ObBufferReader buffer_reader;
  if (!meta_index.is_valid() || OB_ISNULL(kv_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(meta_index), KP(kv_cache));
  } else if (BACKUP_SSTABLE_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type do not match", K(meta_index));
  } else if (OB_FAIL(cache_reader.init(MTL_ID(), path, storage_info, mod, *kv_cache))) {
    LOG_WARN("failed to init cache reader", K(ret), "tenant_id", MTL_ID(), K(path));
  } else if (OB_FAIL(cache_reader.fetch_block(meta_index, allocator, cache_handle, buffer_reader))) {
    LOG_WARN("failedto fetch block", K(ret), K(meta_index));
  } else {
    const ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(buffer_reader.get(common_header))) {
      LOG_WARN("failed to get common_header", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common_header is not valid", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer_reader not enough", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else {
      int64_t total_pos = 0;
      char *tmp_buf = buffer_reader.current();
      const int64_t total_data_size = common_header->data_zlength_;
      ObBackupSSTableMeta sstable_meta;
      while (OB_SUCC(ret) && total_data_size - total_pos > 0) {
        int64_t pos = 0;
        sstable_meta.reset();
        if (OB_FAIL(sstable_meta.deserialize(tmp_buf + total_pos, total_data_size - total_pos, pos))) {
          LOG_WARN("failed to deserialize", K(ret), K(total_pos), K(total_data_size), K(*common_header));
        } else if (OB_FAIL(sstable_metas.push_back(sstable_meta))) {
          LOG_WARN("failed to push back", K(ret), K(sstable_meta));
        } else {
          total_pos += pos;
        }
      }
      if (OB_SUCC(ret)) {
        FLOG_INFO("read sstable metas", K(path), K(meta_index), K(sstable_metas));
      }
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::read_macro_block_id_mapping_metas(const common::ObString &path,
    const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod, const ObBackupMetaIndex &meta_index,
    ObBackupMacroBlockIDMappingsMeta &id_mappings_meta)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(meta_index));
  } else if (BACKUP_MACRO_BLOCK_ID_MAPPING_META != meta_index.meta_key_.meta_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type do not match", K(meta_index));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(meta_index.length_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(meta_index));
  } else if (OB_FAIL(pread_file(path, storage_info, mod, meta_index.offset_, meta_index.length_, buf))) {
    LOG_WARN("failed to pread buffer", K(ret), K(path), K(meta_index));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, meta_index.length_);
    const ObBackupCommonHeader *common_header = NULL;
    int64_t pos = 0;
    if (OB_FAIL(buffer_reader.get(common_header))) {
      LOG_WARN("failed to get common_header", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common_header is not valid", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (common_header->data_zlength_ > buffer_reader.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer_reader not enough", K(ret), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else if (OB_FAIL(id_mappings_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {
      LOG_WARN("failed to read data_header", K(ret), K(*common_header), K(path), K(meta_index), K(buffer_reader));
    } else {
      for (int64_t i = 0; i < id_mappings_meta.sstable_count_; ++i) {
        const ObBackupMacroBlockIDMapping *mapping = id_mappings_meta.id_map_list_[i];
        LOG_DEBUG("read macro block id mapping metas", K(path), K(meta_index), KPC(mapping));
      }
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::read_macro_block_data(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
    const ObBackupMacroBlockIndex &macro_index, const int64_t align_size, blocksstable::ObBufferReader &read_buffer,
    blocksstable::ObBufferReader &data_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = read_buffer.current();
  if (path.empty() || !macro_index.is_valid() || !read_buffer.is_valid() || !data_buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(path), K(macro_index), K(read_buffer), K(data_buffer));
  } else if (align_size <= 0
             || !common::is_io_aligned(macro_index.length_, DIO_ALIGN_SIZE)
             || !common::is_io_aligned(align_size, DIO_ALIGN_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_index), K(align_size));
  } else if (read_buffer.remain() < macro_index.length_) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("read buffer not enough", K(ret), K(path), K(macro_index), K(read_buffer), K(data_buffer));
  } else if (OB_FAIL(pread_file(path, storage_info, mod, macro_index.offset_, macro_index.length_, buf))) {
    LOG_WARN("failed to pread buffer", K(ret), K(path), K(macro_index));
  } else {
    const ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(read_buffer.get(common_header))) {
      LOG_WARN("failed to get common_header", K(ret), K(path), K(macro_index), K(read_buffer));
    } else if (OB_ISNULL(common_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is null", K(ret), K(path), K(macro_index), K(read_buffer));
    } else if (OB_FAIL(common_header->check_valid())) {
      LOG_WARN("common_header is not valid", K(ret), K(path), K(macro_index), K(read_buffer));
    } else if (common_header->data_zlength_ > read_buffer.remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("read_buffer not enough", K(ret), K(path), K(macro_index), K(read_buffer));
    } else if (OB_FAIL(common_header->check_data_checksum(read_buffer.current(), common_header->data_zlength_))) {
      LOG_WARN("failed to check data checksum", K(ret), K(*common_header), K(path), K(macro_index), K(read_buffer));
    } else {
      char *dest = data_buffer.current();
      const int64_t macro_block_size = macro_index.length_ - common_header->header_length_;
      if (OB_FAIL(data_buffer.set_pos(macro_index.length_/* IO size should be aligned */))) {
        LOG_WARN("failed to set pos", K(ret), K(*common_header), K(path), K(macro_index), K(read_buffer), K(data_buffer), K(macro_block_size));
      } else {
        MEMCPY(dest, read_buffer.current(), macro_block_size);
        LOG_DEBUG("read macro block data", K(path), K(macro_index), K(data_buffer));
      }
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::read_ddl_sstable_other_block_id_list_in_ss_mode(
    const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, common::ObIArray<ObBackupLinkedItem> &link_item_list)
{
  int ret = OB_SUCCESS;
  link_item_list.reset();
  ObBackupLinkedBlockItemReader reader;
  bool has_any_block = true;
  if (OB_FAIL(prepare_ddl_sstable_other_block_id_reader_(
      backup_set_dest, path, storage_info, mod, meta_index, table_key, reader, has_any_block))) {
    LOG_WARN("failed to prepare ddl sstable other block id reader", K(ret));
  } else if (!has_any_block) {
    // do nothing
  } else {
    ObBackupLinkedItem link_item;
    while (OB_SUCC(ret)) {
      link_item.reset();
      if (OB_FAIL(reader.get_next_item(link_item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", K(ret));
        }
      } else if (OB_FAIL(link_item_list.push_back(link_item))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::read_ddl_sstable_other_block_id_list_in_ss_mode_with_batch(
    const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, const blocksstable::MacroBlockId &start_macro_id,
    const int64_t count, common::ObIArray<ObBackupLinkedItem> &link_item_list)
{
  int ret = OB_SUCCESS;
  link_item_list.reset();
  ObBackupLinkedBlockItemReader reader;
  bool has_any_block = true;
  if (OB_FAIL(prepare_ddl_sstable_other_block_id_reader_(
      backup_set_dest, path, storage_info, mod, meta_index, table_key, reader, has_any_block))) {
    LOG_WARN("failed to prepare ddl sstable other block id reader", K(ret));
  } else if (!has_any_block) {
    // do nothing
  } else {
    ObBackupLinkedItem link_item;
    int64_t cur_count = 0;
    bool has_meet = false;
    while (OB_SUCC(ret) && cur_count < count) {
      link_item.reset();
      if (OB_FAIL(reader.get_next_item(link_item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", K(ret));
        }
      } else if (link_item.macro_id_ == start_macro_id) {
        has_meet = true;
      }
      if (OB_SUCC(ret) && has_meet) {
        if (OB_FAIL(link_item_list.push_back(link_item))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          cur_count++;
        }
      }
    }
  }
  return ret;
}

int ObLSBackupRestoreUtil::pread_file(const ObString &path, const share::ObBackupStorageInfo *storage_info,
    const ObStorageIdMod &mod, const int64_t offset, const int64_t read_size, char *buf)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t real_read_size = 0;

  if (OB_UNLIKELY(0 == path.length() || !mod.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(path), K(mod), K(ret));
  } else if (OB_UNLIKELY(read_size <= 0)) {  // no data need read
    STORAGE_LOG(INFO, "read data len is zero", K(path), K(read_size));
  } else if (OB_FAIL(util.read_part_file(path,
                                         storage_info,
                                         buf,
                                         read_size,
                                         offset,
                                         real_read_size,
                                         mod))) {
    STORAGE_LOG(WARN, "fail to pread file", K(ret), K(path), K(offset));
  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "not read enough file buffer", K(ret), K(read_size), K(real_read_size), K(path));
  }
  return ret;
}

int ObLSBackupRestoreUtil::prepare_ddl_sstable_other_block_id_reader_(
    const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, ObBackupLinkedBlockItemReader &reader, bool &has_any_block)
{
  int ret = OB_SUCCESS;
  has_any_block = true;
  ObArray<ObBackupSSTableMeta> backup_sstable_metas;
  if (!meta_index.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(path), K(meta_index), K(table_key));
  } else if (OB_FAIL(read_sstable_metas(path, storage_info, mod, meta_index, &OB_BACKUP_META_CACHE, backup_sstable_metas))) {
    LOG_WARN("failed to read sstable metas", K(ret), K(path), K(meta_index));
  } else {
    ObBackupSSTableMeta *sstable_meta_ptr = NULL;
    ARRAY_FOREACH_X(backup_sstable_metas, idx, cnt, OB_SUCC(ret)) {
      const ObBackupSSTableMeta &meta = backup_sstable_metas.at(idx);
      if (meta.sstable_meta_.table_key_ == table_key) {
        sstable_meta_ptr = &backup_sstable_metas.at(idx);
        break;
      }
    }
    if (OB_ISNULL(sstable_meta_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable meta ptr is not valid", K(ret));
    } else if (0 == sstable_meta_ptr->total_other_block_count_) {
      has_any_block = false;
    } else {
      const ObBackupLinkedBlockAddr &entry_block_id = sstable_meta_ptr->entry_block_addr_for_other_block_;
      const int64_t total_block_count = sstable_meta_ptr->total_other_block_count_;
      ObMemAttr mem_attr(MTL_ID(), "ResOtherBlk");
      if (OB_FAIL(reader.init(backup_set_dest, entry_block_id, total_block_count,
          meta_index.meta_key_.tablet_id_, sstable_meta_ptr->sstable_meta_.table_key_, mem_attr, mod))) {
        LOG_WARN("failed to init backup linked block item reader", K(ret), K(backup_set_dest), K(entry_block_id), K(total_block_count));
      }
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase

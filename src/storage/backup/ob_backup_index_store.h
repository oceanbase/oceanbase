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

#ifndef STORAGE_LOG_STREAM_BACKUP_STORE_H_
#define STORAGE_LOG_STREAM_BACKUP_STORE_H_

#include "common/ob_tablet_id.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_index_cache.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_index_compressor.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "common/storage/ob_device_common.h"

namespace oceanbase {
namespace backup {

struct ObBackupIndexStoreParam {
  ObBackupIndexStoreParam();
  bool is_valid() const;
  TO_STRING_KV(K_(index_level), K_(tenant_id), K_(backup_set_id), K_(ls_id), K_(is_tenant_level), K_(backup_data_type),
      K_(turn_id), K_(retry_id), K_(dest_id));
  ObBackupIndexLevel index_level_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  bool is_tenant_level_;
  share::ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t dest_id_;
};

class ObIBackupIndexStore {
public:
  ObIBackupIndexStore();
  virtual ~ObIBackupIndexStore();
  bool is_inited() const
  {
    return is_inited_;
  }
  virtual int get_backup_file_path(share::ObBackupPath &backup_path) const = 0;
  virtual int get_backup_index_cache_key(const ObBackupFileType &backup_file_type, const int64_t offset,
      const int64_t length, ObBackupIndexCacheKey &cache_key) const = 0;

  TO_STRING_KV(K_(mode), K_(index_level), K_(backup_dest), K_(tenant_id), K_(ls_id), K_(turn_id), K_(retry_id));

protected:
  int pread_file_(const common::ObString &backup_path, const share::ObBackupStorageInfo *storage_info,
      const int64_t offset, const int64_t read_size, char *buf);
  int decode_headers_(blocksstable::ObBufferReader &buffer_reader, share::ObBackupCommonHeader &common_header,
      ObBackupMultiLevelIndexHeader &index_header, int64_t &data_size, int64_t &data_zlength, ObCompressorType &compressor_type);
  int decode_common_header_(blocksstable::ObBufferReader &buffer_reader, share::ObBackupCommonHeader &header);
  int decode_multi_level_index_header_(
      blocksstable::ObBufferReader &buffer_reader, ObBackupMultiLevelIndexHeader &header);
  template <typename IndexType>
  int decode_index_from_block_(const int64_t data_zlength, const int64_t original_size, const ObCompressorType &compressor_type,
      blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list);
  int fetch_block_(const ObBackupFileType &backup_file_type, const int64_t offset, const int64_t length,
      common::ObIAllocator &allocator, ObKVCacheHandle &handle, blocksstable::ObBufferReader &buffer_reader);
  int do_on_cache_miss_(const ObBackupFileType &backup_file_type, const int64_t offset, const int64_t length,
      common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader);
  int put_block_to_cache_(const ObBackupFileType &backup_file_type, const int64_t offset, const int64_t length,
      const blocksstable::ObBufferReader &buffer);
  int read_file_trailer_(const common::ObString &backup_path, const share::ObBackupStorageInfo *storage_info,
      const common::ObStorageIdMod &mod);
  int fetch_index_block_from_dest_(const common::ObString &backup_path, const share::ObBackupStorageInfo *storage_info,
      const int64_t offset, const int64_t length, common::ObIAllocator &allocator,
      blocksstable::ObBufferReader &buffer_reader);

protected:
  static const int64_t OB_DEFAULT_BACKUP_INDEX_FILE_ID = 0;
  bool is_inited_;
  bool is_tenant_level_;
  ObBackupRestoreMode mode_;
  ObBackupIndexLevel index_level_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObStorageIdMod mod_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObBackupDataType backup_data_type_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupMultiLevelIndexTrailer trailer_;
  ObBackupIndexBlockCompressor compressor_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupIndexStore);
};

class ObBackupMetaIndexStore : public ObIBackupIndexStore {
public:
  ObBackupMetaIndexStore();
  virtual ~ObBackupMetaIndexStore();
  // for backup mode
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
      const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc, const bool is_sec_meta,
      ObBackupIndexKVCache &index_kv_cache);
  int get_backup_meta_index(
      const common::ObTabletID &tablet_id, const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index);
  void reset();

public:
  virtual int get_backup_file_path(share::ObBackupPath &backup_path) const override;
  virtual int get_backup_index_cache_key(const ObBackupFileType &backup_file_type, const int64_t offset,
      const int64_t length, ObBackupIndexCacheKey &cache_key) const override;

private:
  int get_tablet_meta_index_(const ObBackupMetaKey &meta_key, ObBackupMetaIndex &meta_index);
  int decode_meta_index_from_buffer_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMetaIndex> &index_list);
  int decode_meta_index_index_from_buffer_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMetaIndexIndex> &index_index_list);
  int find_index_lower_bound_(
      const ObBackupMetaKey &meta_key, const common::ObArray<ObBackupMetaIndex> &index_list, ObBackupMetaIndex &index);
  int find_index_index_lower_bound_(const ObBackupMetaKey &meta_key,
      const common::ObArray<ObBackupMetaIndexIndex> &index_index_list, ObBackupMetaIndexIndex &index_index);

protected:
  bool is_sec_meta_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexStore);
};

class ObBackupMacroBlockIndexStore : public ObIBackupIndexStore {
public:
  ObBackupMacroBlockIndexStore();
  virtual ~ObBackupMacroBlockIndexStore();
  // for backup mode
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
      const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy);
  // for restore mode
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
      const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      ObBackupIndexKVCache &index_kv_cache);
  int get_macro_block_index(const blocksstable::ObLogicMacroBlockId &macro_id, ObBackupMacroBlockIndex &macro_index);
  int get_macro_range_index(const blocksstable::ObLogicMacroBlockId &macro_id, ObBackupMacroRangeIndex &range_index);
  void reset();

public:
  virtual int get_backup_file_path(share::ObBackupPath &backup_path) const override;
  virtual int get_backup_index_cache_key(const ObBackupFileType &backup_file_type, const int64_t offset,
      const int64_t length, ObBackupIndexCacheKey &cache_key) const override;

private:
  int inner_get_macro_block_range_index_(
      const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroRangeIndex &range_index);
  int decode_range_index_from_block_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMacroRangeIndex> &range_index);
  int decode_range_index_index_from_block_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMacroRangeIndexIndex> &range_index_index);
  int find_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_macro_block_id,
      const common::ObArray<ObBackupMacroRangeIndex> &index_list, ObBackupMacroRangeIndex &index);
  int find_index_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_macro_block_id,
      const common::ObArray<ObBackupMacroRangeIndexIndex> &index_index_list, ObBackupMacroRangeIndexIndex &index_index);
  int get_macro_block_backup_path_(const ObBackupMacroRangeIndex &range_index, share::ObBackupPath &backup_path);
  int get_backup_set_desc_(const ObBackupMacroRangeIndex &range_index, share::ObBackupSetDesc &backup_set_desc);
  int get_macro_block_index_(const blocksstable::ObLogicMacroBlockId &macro_id, const ObBackupMacroRangeIndex &range_index,
      ObBackupMacroBlockIndex &macro_index);
  virtual int fill_backup_set_descs_(const uint64_t tenant_id, const int64_t backup_set_id, common::ObMySQLProxy &sql_proxy);

private:
  common::ObArray<share::ObBackupSetDesc> backup_set_desc_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroBlockIndexStore);
};

// index store used by restore
class ObRestoreMetaIndexStore final : public ObBackupMetaIndexStore
{
public:
  ObRestoreMetaIndexStore(): ObBackupMetaIndexStore(), data_version_(0) {}
  virtual ~ObRestoreMetaIndexStore() {}
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc, const bool is_sec_meta,
    const uint64_t data_version, ObBackupIndexKVCache &index_kv_cache);
  virtual int get_backup_file_path(share::ObBackupPath &backup_path) const override;
private:
  uint64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreMetaIndexStore);
};

class ObIBackupIndexStoreWrapper {
public:
  ObIBackupIndexStoreWrapper() {}
  virtual ~ObIBackupIndexStoreWrapper() {}

public:
  virtual int check_tenant_compatible(const int64_t tenant_compatible) = 0;
  virtual int get_index_store_array_size(int64_t &array_size) = 0;
  virtual int get_tenant_meta_index_turn_id(const share::ObBackupSetFileDesc &backup_set_info,
    const share::ObBackupDataType &backup_data_type, int64_t &turn_id) = 0;
  virtual int get_tenant_meta_index_retry_id(const share::ObBackupDest &backup_dest, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t is_sec_meta, int64_t &retry_id);
  virtual int get_idx(const share::ObBackupDataType &backup_data_type, int64_t &idx) = 0;
  virtual int get_type_by_idx(const int64_t idx, share::ObBackupDataType &backup_data_type) = 0;
  virtual int get_index_store(const share::ObBackupDataType &backup_data_type, ObRestoreMetaIndexStore *&index_store) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIBackupIndexStoreWrapper);
};

// for tenant compatible less then 4.3.2
class ObBackupMetaIndexStoreWrapperV1 : public ObIBackupIndexStoreWrapper
{
public:
  ObBackupMetaIndexStoreWrapperV1() : ObIBackupIndexStoreWrapper(), store_list_() {}
  virtual ~ObBackupMetaIndexStoreWrapperV1() {}

public:
  virtual int check_tenant_compatible(const int64_t tenant_compatible) override;
  virtual int get_index_store_array_size(int64_t &array_size) override;
  virtual int get_tenant_meta_index_turn_id(const share::ObBackupSetFileDesc &backup_set_info,
     const share::ObBackupDataType &backup_data_type, int64_t &turn_id) override;
  virtual int get_idx(const share::ObBackupDataType &backup_data_type, int64_t &idx) override;
  virtual int get_type_by_idx(const int64_t idx, share::ObBackupDataType &backup_data_type) override;
  virtual int get_index_store(const share::ObBackupDataType &backup_data_type, ObRestoreMetaIndexStore *&index_store) override;

private:
  static const int64_t ARRAY_SIZE = 3;
  ObRestoreMetaIndexStore store_list_[ARRAY_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexStoreWrapperV1);
};

// for tenant compatible greater or equal to 4.3.2
class ObBackupMetaIndexStoreWrapperV2 : public ObIBackupIndexStoreWrapper
{
public:
  ObBackupMetaIndexStoreWrapperV2() : ObIBackupIndexStoreWrapper(), store_list_() {}
  virtual ~ObBackupMetaIndexStoreWrapperV2() {}

public:
  virtual int check_tenant_compatible(const int64_t tenant_compatible) override;
  virtual int get_index_store_array_size(int64_t &array_size) override;
  virtual int get_tenant_meta_index_turn_id(const share::ObBackupSetFileDesc &backup_set_info,
    const share::ObBackupDataType &backup_data_type, int64_t &turn_id) override;
  virtual int get_idx(const share::ObBackupDataType &backup_data_type, int64_t &idx) override;
  virtual int get_type_by_idx(const int64_t idx, share::ObBackupDataType &backup_data_type) override;
  virtual int get_index_store(const share::ObBackupDataType &backup_data_type, ObRestoreMetaIndexStore *&index_store) override;

private:
  static const int64_t ARRAY_SIZE = 2;
  ObRestoreMetaIndexStore store_list_[ARRAY_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexStoreWrapperV2);
};

class ObBackupMetaIndexStoreWrapper final {
public:
  ObBackupMetaIndexStoreWrapper();
  virtual ~ObBackupMetaIndexStoreWrapper();
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
      const share::ObBackupDest &backup_dest, const share::ObBackupSetFileDesc &backup_set_info, const bool is_sec_meta,
      const bool init_sys_tablet_index_store, ObBackupIndexKVCache &index_kv_cache);

  int get_backup_meta_index(const share::ObBackupDataType &backup_data_type, const common::ObTabletID &tablet_id,
      const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index);
  int get_backup_meta_index_store(const share::ObBackupDataType &backup_data_type,
      ObRestoreMetaIndexStore *&index_store);
  TO_STRING_KV(K_(is_sec_meta));

private:
  int inner_init_(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetFileDesc &backup_set_info, const bool is_sec_meta,
    const bool init_sys_tablet_index_store, ObBackupIndexKVCache &index_kv_cache);
  int get_index_store_(const share::ObBackupDataType &type, ObRestoreMetaIndexStore *&index_store);
  int decide_wrapper_(const uint64_t tenant_id, const int64_t tenant_compatible, ObIBackupIndexStoreWrapper *&wrapper);

private:
  bool is_inited_;
  bool is_sec_meta_;
  ObIBackupIndexStoreWrapper *index_wrapper_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaIndexStoreWrapper);
};

// This class allows users to efficiently retrieve the macro block index stored in the macro_block_index file.
// By providing a logic ID, it utilizes binary search to locate the corresponding index,
// enabling users to quickly reuse the desired macro block.
class ObBackupOrderedMacroBlockIndexStore final : public ObIBackupIndexStore
{
  struct MacroBlockIndexCompare {
    bool operator()(const ObBackupMacroBlockIndex &left, const blocksstable::ObLogicMacroBlockId &right) const
    {
      return left.logic_id_ < right;
    }
  };
  struct MacroBlockIndexIndexCompare {
    bool operator()(const ObBackupMacroBlockIndexIndex &left, const blocksstable::ObLogicMacroBlockId &right) const
    {
      return left.end_key_.logic_id_ < right;
    }
  };
public:
  ObBackupOrderedMacroBlockIndexStore();
  ~ObBackupOrderedMacroBlockIndexStore();
  int init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
      const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_info,
      ObBackupIndexKVCache &index_kv_cache);
  int get_macro_block_index(
      const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroBlockIndex &macro_index);

private:
  virtual int get_backup_file_path(share::ObBackupPath &backup_path) const override;
  virtual int get_backup_index_cache_key(const ObBackupFileType &backup_file_type, const int64_t offset,
      const int64_t length, ObBackupIndexCacheKey &cache_key) const override;

private:
  int decode_indexes_from_block_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMacroBlockIndex> &index_list);
  int decode_index_indexes_from_block_(const int64_t data_zlength, const int64_t original_size,
      const ObCompressorType &compressor_type, blocksstable::ObBufferReader &buffer_reader,
      common::ObArray<ObBackupMacroBlockIndexIndex> &index_index_list);
  int find_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_id,
      const common::ObArray<ObBackupMacroBlockIndex> &index_list, ObBackupMacroBlockIndex &macro_index);
  int find_index_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_id,
      const common::ObArray<ObBackupMacroBlockIndexIndex> &index_list, ObBackupMacroBlockIndexIndex &macro_index_index);

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupOrderedMacroBlockIndexStore);
};

class ObBackupTenantIndexRetryIDGetter final
{
public:
  ObBackupTenantIndexRetryIDGetter();
  ~ObBackupTenantIndexRetryIDGetter();
  // for backup
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id,
      const bool is_restore, const bool is_macro_index, const bool is_sec_meta);
  // for restore
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const bool is_restore,
      const bool is_macro_index, const bool is_sec_meta);
  int get_max_retry_id(int64_t &retry_id);
  int get_max_retry_id_v_4_1_x(int64_t &retry_id);

private:
  int get_ls_info_data_info_dir_path_(share::ObBackupPath &backup_path);
  int get_tenant_index_file_name_(const char *&file_name);
  int list_files_(const share::ObBackupPath &backup_path, const share::ObBackupStorageInfo *storage_info,
      const char *file_name_prefix, common::ObArray<int64_t> &id_list);
  int find_largest_id_(const common::ObIArray<int64_t> &id_list, int64_t &largest_id);

private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  bool is_restore_;
  bool is_macro_index_;
  bool is_sec_meta_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTenantIndexRetryIDGetter);
};

}  // namespace backup
}  // namespace oceanbase

#endif

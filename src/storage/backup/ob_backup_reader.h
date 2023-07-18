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

#ifndef STORAGE_LOG_STREAM_BACKUP_READER_H_
#define STORAGE_LOG_STREAM_BACKUP_READER_H_

#include "common/ob_tablet_id.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase {
namespace backup {

enum ObLSTabletIdReaderType {
  FAKE_LS_TABLET_ID_READER = 0,
  LS_TABLET_ID_READER = 1,
  MAX_LS_TABLET_ID_READER,
};

class ObILSTabletIdReader {
public:
  ObILSTabletIdReader() = default;
  virtual ~ObILSTabletIdReader() = default;
  virtual int init(const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id) = 0;
  virtual int get_tablet_id_list(
      share::ObBackupDataType &backup_data_type, const int64_t turn_id,
      const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &tablet_id_list) = 0;
  virtual ObLSTabletIdReaderType get_type() const = 0;

protected:
  static int get_sys_tablet_list_(const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &tablet_id_list);

private:
  DISALLOW_COPY_AND_ASSIGN(ObILSTabletIdReader);
};

class ObLSTabletIdReader : public ObILSTabletIdReader {
public:
  ObLSTabletIdReader();
  virtual ~ObLSTabletIdReader();
  int init(const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id);
  virtual int get_tablet_id_list(
      share::ObBackupDataType &backup_data_type, const int64_t turn_id,
      const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &tablet_id_list) override;
  virtual ObLSTabletIdReaderType get_type() const override
  {
    return LS_TABLET_ID_READER;
  }

private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  storage::ObBackupDataStore store_;
  DISALLOW_COPY_AND_ASSIGN(ObLSTabletIdReader);
};

enum ObTabletLogicIdReaderType {
  FAKE_TABLET_LOGIC_ID_READER = 0,
  TABLET_LOGIC_ID_READER = 1,
  MAX_TABLET_LOGIC_ID_READER,
};

class ObITabletLogicMacroIdReader {
public:
  ObITabletLogicMacroIdReader();
  virtual ~ObITabletLogicMacroIdReader();
  virtual int init(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable,
      const int64_t batch_size) = 0;
  virtual int get_next_batch(common::ObIArray<ObBackupMacroBlockId> &id_array) = 0;
  virtual ObTabletLogicIdReaderType get_type() const = 0;

protected:
  bool is_inited_;
  int64_t batch_size_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  const blocksstable::ObSSTable *sstable_;
  const storage::ObTabletHandle *tablet_handle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObITabletLogicMacroIdReader);
};

class ObTabletLogicMacroIdReader : public ObITabletLogicMacroIdReader {
public:
  ObTabletLogicMacroIdReader();
  virtual ~ObTabletLogicMacroIdReader();
  virtual int init(const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, const blocksstable::ObSSTable &sstable,
      const int64_t batch_size) override;
  virtual int get_next_batch(common::ObIArray<ObBackupMacroBlockId> &id_array) override;
  virtual ObTabletLogicIdReaderType get_type() const override
  {
    return TABLET_LOGIC_ID_READER;
  }

private:
  typedef common::hash::ObCuckooHashMap<blocksstable::ObLogicMacroBlockId, blocksstable::MacroBlockId> MacroBlockIdMap;
  typedef MacroBlockIdMap::const_iterator MacroBlockIDIterator;
  static const int64_t MACRO_BLOCK_BATCH_SIZE = 128;

private:
  common::ObSpinLock lock_;
  ObArenaAllocator allocator_;
  blocksstable::ObDatumRange datum_range_;
  blocksstable::ObSSTableSecMetaIterator meta_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletLogicMacroIdReader);
};

enum ObMacroBlockReaderType {
  FAKE_MACRO_BLOCK_READER = 0,
  LOCAL_MACRO_BLOCK_READER = 1,
  MAX_MACRO_BLOCK_READER,
};

class ObIMacroBlockBackupReader {
public:
  ObIMacroBlockBackupReader();
  virtual ~ObIMacroBlockBackupReader();
  virtual int init(const ObBackupMacroBlockId &macro_id) = 0;
  virtual int get_macro_block_data(
      blocksstable::ObBufferReader &buffer_reader, blocksstable::ObLogicMacroBlockId &logic_id) = 0;
  virtual void reset() = 0;
  virtual ObMacroBlockReaderType get_type() const = 0;
  TO_STRING_KV(K_(logic_id));

protected:
  bool is_inited_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  blocksstable::ObBlockInfo block_info_;
  DISALLOW_COPY_AND_ASSIGN(ObIMacroBlockBackupReader);
};

class ObMacroBlockBackupReader : public ObIMacroBlockBackupReader {
public:
  ObMacroBlockBackupReader();
  virtual ~ObMacroBlockBackupReader();
  int init(const ObBackupMacroBlockId &macro_id);
  virtual int get_macro_block_data(
      blocksstable::ObBufferReader &buffer_reader, blocksstable::ObLogicMacroBlockId &logic_id) override;
  virtual void reset() override;
  virtual ObMacroBlockReaderType get_type() const override
  {
    return LOCAL_MACRO_BLOCK_READER;
  }
  TO_STRING_KV(K_(logic_id), K_(block_info));

private:
  int process_();
  int get_macro_read_info_(const blocksstable::ObLogicMacroBlockId &logic_id, blocksstable::ObMacroBlockReadInfo &read_info);
  int get_macro_block_size_(const blocksstable::ObBufferReader &buffer, int64_t &size);

private:
  bool is_data_ready_;
  int64_t result_code_;
  blocksstable::ObMacroBlockHandle macro_handle_;
  blocksstable::ObBufferReader buffer_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockBackupReader);
};

class ObMultiMacroBlockBackupReader {
public:
  ObMultiMacroBlockBackupReader();
  virtual ~ObMultiMacroBlockBackupReader();
  int init(const uint64_t tenant_id, const ObMacroBlockReaderType &reader_type, const common::ObIArray<ObBackupMacroBlockId> &list);
  int get_next_macro_block(blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id);
  void reset();

private:
  int alloc_macro_block_reader_(const uint64_t tenant_id, const ObMacroBlockReaderType &reader_type, ObIMacroBlockBackupReader *&reader);
  int prepare_macro_block_reader_(const int64_t idx);
  void reset_prev_macro_block_reader_(const int64_t idx);
  int fetch_macro_block_with_retry_(blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id);
  int fetch_macro_block_(blocksstable::ObBufferReader &data, blocksstable::ObLogicMacroBlockId &logic_id);

private:
  static const int64_t FETCH_MACRO_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000L;  // 1s

private:
  bool is_inited_;
  int64_t read_size_;
  int64_t reader_idx_;
  ObMacroBlockReaderType reader_type_;
  common::ObArray<ObBackupMacroBlockId> macro_list_;
  common::ObArray<ObIMacroBlockBackupReader *> readers_;
  DISALLOW_COPY_AND_ASSIGN(ObMultiMacroBlockBackupReader);
};

enum ObTabletMetaReaderType {
  TABLET_META_READER = 0,
  SSTABLE_META_READER = 1,
  MAX_META_READER,
};

class ObITabletMetaBackupReader {
public:
  ObITabletMetaBackupReader();
  virtual ~ObITabletMetaBackupReader();
  virtual int init(const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type,
      storage::ObTabletHandle &tablet_handle) = 0;
  virtual int get_meta_data(blocksstable::ObBufferReader &buffer_reader) = 0;
  virtual ObTabletMetaReaderType get_type() const = 0;

protected:
  bool is_inited_;
  common::ObTabletID tablet_id_;
  share::ObBackupDataType backup_data_type_;
  storage::ObTabletHandle *tablet_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObITabletMetaBackupReader);
};

class ObTabletMetaBackupReader : public ObITabletMetaBackupReader {
public:
  ObTabletMetaBackupReader();
  virtual ~ObTabletMetaBackupReader();
  virtual int init(const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type,
      storage::ObTabletHandle &tablet_handle) override;
  virtual int get_meta_data(blocksstable::ObBufferReader &buffer_reader) override;
  virtual ObTabletMetaReaderType get_type() const
  {
    return TABLET_META_READER;
  }

private:
  blocksstable::ObSelfBufferWriter buffer_writer_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletMetaBackupReader);
};

class ObSSTableMetaBackupReader : public ObITabletMetaBackupReader {
public:
  ObSSTableMetaBackupReader();
  virtual ~ObSSTableMetaBackupReader();
  virtual int init(const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type,
      storage::ObTabletHandle &tablet_handle) override;
  virtual int get_meta_data(blocksstable::ObBufferReader &buffer_reader) override;
  virtual ObTabletMetaReaderType get_type() const
  {
    return SSTABLE_META_READER;
  }

private:
  int get_macro_block_id_list_(const blocksstable::ObSSTable &sstable, ObBackupSSTableMeta &sstable_meta);

private:
  common::ObArray<storage::ObITable *> sstable_array_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMetaBackupReader);
};

class ObBackupTabletCtx;

class ObTabletPhysicalIDMetaBackupReader {
public:
  ObTabletPhysicalIDMetaBackupReader();
  virtual ~ObTabletPhysicalIDMetaBackupReader();
  int init(const common::ObTabletID &tablet_id, ObBackupTabletCtx *ctx);
  int get_meta_data(blocksstable::ObBufferReader &buffer_reader);

private:
  int check_ctx_completed_();

private:
  bool is_inited_;
  ObBackupTabletCtx *ctx_;
  common::ObTabletID tablet_id_;
  blocksstable::ObSelfBufferWriter buffer_writer_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletPhysicalIDMetaBackupReader);
};

}  // namespace backup
}  // namespace oceanbase

#endif

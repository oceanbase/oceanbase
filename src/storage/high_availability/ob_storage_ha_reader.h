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

#ifndef OCEABASE_STORAGE_HA_MACRO_BLOCK_READER_
#define OCEABASE_STORAGE_HA_MACRO_BLOCK_READER_

#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/function/ob_function.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "ob_storage_ha_struct.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "ob_storage_restore_struct.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/backup/ob_backup_data_store.h"

namespace oceanbase
{
namespace storage
{
class ObICopyMacroBlockReader
{
public:
  enum Type {
    MACRO_BLOCK_OB_READER = 0,
    MACRO_BLOCK_RESTORE_READER = 1,
    REMOTE_SSTABLE_MACRO_BLOCK_RESTORE_READER = 2,
    DDL_MACRO_BLOCK_RESTORE_READER = 3,
    MAX_READER_TYPE
  };
  struct CopyMacroBlockReadData final
  {
  public:
    CopyMacroBlockReadData();
    ~CopyMacroBlockReadData();
    void reset();
    bool is_valid() const;
    int set_macro_meta(const blocksstable::ObDataMacroBlockMeta& macro_meta, const bool &is_reuse_macro_block);
    int set_macro_data(const blocksstable::ObBufferReader& macro_data, const bool &is_reuse_macro_block);
    void set_macro_block_id(const blocksstable::MacroBlockId &macro_block_id);
    bool is_reuse_macro_block() const { return is_reuse_macro_block_; }
    bool is_macro_data() const { return data_type_ == ObCopyMacroBlockDataType::MACRO_DATA; }
    bool is_macro_meta() const { return data_type_ == ObCopyMacroBlockDataType::MACRO_META_ROW; }
  public:
    TO_STRING_KV(K_(data_type), K_(is_reuse_macro_block), K_(macro_data), KPC_(macro_meta));
    ObCopyMacroBlockDataType data_type_;
    bool is_reuse_macro_block_;
    blocksstable::ObBufferReader macro_data_;
    blocksstable::ObDataMacroBlockMeta *macro_meta_;
    blocksstable::MacroBlockId macro_block_id_;
    common::ObArenaAllocator allocator_;
  };
  // macro block list is set in the init func
  ObICopyMacroBlockReader() {}
  virtual ~ObICopyMacroBlockReader() {}
  virtual int get_next_macro_block(CopyMacroBlockReadData &read_data) = 0;
  virtual Type get_type() const = 0;
  virtual int64_t get_data_size() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopyMacroBlockReader);
};

struct ObCopyMacroBlockReaderInitParam final
{
  ObCopyMacroBlockReaderInitParam();
  ~ObCopyMacroBlockReaderInitParam();
  void reset();
  bool is_valid() const;
  int assign(const ObCopyMacroBlockReaderInitParam &param);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(table_key), KPC_(copy_macro_range_info), K_(src_info),
      K_(is_leader_restore), K_(restore_action), KP_(bandwidth_throttle), KP_(svr_rpc_proxy),
      KP_(restore_base_info), KP_(meta_index_store), KP_(second_meta_index_store),
      KP_(restore_macro_block_id_mgr), KP_(macro_block_reuse_mgr));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  const ObCopyMacroRangeInfo *copy_macro_range_info_;
  ObStorageHASrcInfo src_info_;
  bool is_leader_restore_;
  ObTabletRestoreAction::ACTION restore_action_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  share::SCN backfill_tx_scn_;
  int64_t data_version_; // max snapshot version of local major sstable (use for macro block reuse)
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockReaderInitParam);
};

class ObCopyMacroBlockObReader: public ObICopyMacroBlockReader
{
public:
  ObCopyMacroBlockObReader();
  virtual ~ObCopyMacroBlockObReader();
  int init(const ObCopyMacroBlockReaderInitParam &param);
  virtual int get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_info);
  virtual Type get_type() const { return MACRO_BLOCK_OB_READER; }
  virtual int64_t get_data_size() const { return data_size_; }

private:
  int alloc_buffers();
  int fetch_next_buffer_if_need();
  int fetch_next_buffer();
  int alloc_from_memctx_first(char* &buf);
  int get_read_info_(const ObCopyMacroBlockHeader &header, const ObBufferReader &data, CopyMacroBlockReadData &read_info);
private:
  bool is_inited_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_HA_FETCH_MACRO_BLOCK> handle_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  blocksstable::ObBufferReader data_buffer_; // Data used to assemble macroblocks
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  common::ObMacroBlockSizeMemoryContext macro_block_mem_context_;
  int64_t last_send_time_;
  int64_t data_size_;
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
  ObITable::TableKey table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockObReader);
};

class ObCopyMacroBlockRestoreReader: public ObICopyMacroBlockReader
{
public:
  ObCopyMacroBlockRestoreReader();
  virtual ~ObCopyMacroBlockRestoreReader();
  int init(const ObCopyMacroBlockReaderInitParam &param);
  virtual int get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_info);
  virtual Type get_type() const { return MACRO_BLOCK_RESTORE_READER; }
  virtual int64_t get_data_size() const { return data_size_; }

private:
  int alloc_buffers();
  void reset_buffers_();
  int convert_logical_id_to_shared_macro_id_(
      const ObLogicMacroBlockId &logic_block_id,
      MacroBlockId &macro_block_id);
  int fetch_macro_block_index_(
      const int64_t block_id_idx,
      const share::ObBackupDataType &backup_data_type,
      blocksstable::ObLogicMacroBlockId &logic_block_id,
      backup::ObBackupMacroBlockIndex &macro_index);
  int get_macro_block_backup_path_(
      const share::ObBackupDest &backup_set_dest,
      const backup::ObBackupMacroBlockIndex &macro_index,
      const share::ObBackupDataType backup_data_type,
      ObBackupPath &backup_path);

private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  ObTabletHandle tablet_handle_;
  const ObCopyMacroRangeInfo *copy_macro_range_info_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr_;
  blocksstable::ObBufferReader data_buffer_; // Data used to assemble macroblocks
  blocksstable::ObBufferReader read_buffer_; // Buffer used to read macro data
  common::ObArenaAllocator allocator_;
  int64_t macro_block_index_;
  int64_t macro_block_count_;
  int64_t data_size_;
  ObDatumRange datum_range_;
  backup::ObBackupSSTableSecMetaIterator *sec_meta_iterator_;
  ObTabletRestoreAction::ACTION restore_action_;
  ObSelfBufferWriter meta_row_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockRestoreReader);
};

class ObCopyDDLMacroBlockRestoreReader: public ObICopyMacroBlockReader
{
public:
  ObCopyDDLMacroBlockRestoreReader();
  virtual ~ObCopyDDLMacroBlockRestoreReader();
  int init(const ObCopyMacroBlockReaderInitParam &param);
  virtual int get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data);
  virtual Type get_type() const { return DDL_MACRO_BLOCK_RESTORE_READER; }
  virtual int64_t get_data_size() const { return data_size_; }

private:
  int alloc_buffers();
  void reset_buffers_();
  int prepare_link_item_();

private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  const ObCopyMacroRangeInfo *copy_macro_range_info_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  blocksstable::ObBufferReader data_buffer_; // Data used to assemble macroblocks
  blocksstable::ObBufferReader read_buffer_; // Buffer used to read macro data
  common::ObArenaAllocator allocator_;
  int64_t macro_block_index_;
  int64_t macro_block_count_;
  int64_t data_size_;
  ObArray<backup::ObBackupLinkedItem> link_item_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyDDLMacroBlockRestoreReader);
};


struct ObCopyMacroBlockHandle final
{
  ObCopyMacroBlockHandle();
  ~ObCopyMacroBlockHandle() = default;
  void reset();
  bool is_valid() const;
  int set_macro_meta(const blocksstable::ObDataMacroBlockMeta &macro_meta);

  bool is_reuse_macro_block_;
  blocksstable::ObStorageObjectHandle read_handle_;
  common::ObArenaAllocator allocator_;
  blocksstable::ObDataMacroBlockMeta *macro_meta_;

  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockHandle);
};

class ObCopyMacroBlockObProducer
{
public:
  ObCopyMacroBlockObProducer();
  virtual ~ObCopyMacroBlockObProducer();

  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObITable::TableKey &table_key,
      const ObCopyMacroRangeInfo &copy_macro_range_info,
      const int64_t data_version,
      const share::SCN backfill_tx_scn);
  int get_next_macro_block(
      blocksstable::ObBufferReader &data,
      ObCopyMacroBlockHeader &copy_macro_block_header);

private:
  int prefetch_();

private:
  static const int64_t MAX_PREFETCH_MACRO_BLOCK_NUM = 2;
  static const int64_t MACRO_META_RESERVE_TIME = 60 * 1000 * 1000LL; // 1minutes

  bool is_inited_;
  ObCopyMacroRangeInfo copy_macro_range_info_;
  int64_t data_version_;
  int64_t macro_idx_;
  ObCopyMacroBlockHandle copy_macro_block_handle_[MAX_PREFETCH_MACRO_BLOCK_NUM];
  int64_t handle_idx_;
  int64_t prefetch_meta_time_;
  common::ObArenaAllocator tablet_allocator_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 sstable_handle_;
  const ObSSTable *sstable_;
  ObDatumRange datum_range_;
  common::ObArenaAllocator allocator_;
  ObSSTableSecMetaIterator second_meta_iterator_;
  common::ObArenaAllocator io_allocator_;
  char *io_buf_[MAX_PREFETCH_MACRO_BLOCK_NUM];
  ObSelfBufferWriter meta_row_buf_; // buffer for macro meta row (ObDatumRow)
  DISALLOW_COPY_AND_ASSIGN(ObCopyMacroBlockObProducer);
};

class ObICopyTabletInfoReader
{
public:
  enum Type {
    TABLET_INFO_OB_READER = 0,
    TABLET_INFO_RESTORE_READER = 1,
    MAX,
  };
  ObICopyTabletInfoReader() {}
  virtual ~ObICopyTabletInfoReader() {}
  virtual int fetch_tablet_info(
      obrpc::ObCopyTabletInfo &tablet_info) = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopyTabletInfoReader);
};

class ObCopyTabletInfoObReader : public ObICopyTabletInfoReader
{
public:
  ObCopyTabletInfoObReader();
  virtual ~ObCopyTabletInfoObReader();
  int init(
      const ObStorageHASrcInfo &src_info,
      const obrpc::ObCopyTabletInfoArg &rpc_arg,
      obrpc::ObStorageRpcProxy &srv_rpc_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  virtual int fetch_tablet_info(obrpc::ObCopyTabletInfo &tablet_info);
  virtual Type get_type() const { return TABLET_INFO_OB_READER; }
private:
  static const int64_t FETCH_TABLET_INFO_TIMEOUT = 60 * 1000 * 1000; //60s
  bool is_inited_;
  ObStorageStreamRpcReader<obrpc::OB_HA_FETCH_TABLET_INFO> rpc_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyTabletInfoObReader);
};

class ObCopyTabletInfoRestoreReader : public ObICopyTabletInfoReader
{
public:
  ObCopyTabletInfoRestoreReader();
  virtual ~ObCopyTabletInfoRestoreReader();
  int init(
      const ObRestoreBaseInfo &restore_base_info,
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store);
  virtual int fetch_tablet_info(obrpc::ObCopyTabletInfo &tablet_info);
  virtual Type get_type() const { return TABLET_INFO_RESTORE_READER; }
private:
  int get_macro_block_backup_path_(
      const backup::ObBackupMetaIndex &tablet_meta_index,
      const share::ObBackupDataType data_type,
      ObBackupPath &tablet_meta_backup_path);
private:
  bool is_inited_;
  const ObRestoreBaseInfo *restore_base_info_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  int64_t tablet_id_index_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyTabletInfoRestoreReader);
};

class ObCopyTabletInfoObProducer
{
public:
  ObCopyTabletInfoObProducer();
  virtual ~ObCopyTabletInfoObProducer();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_id_array);

  int get_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info);
private:
  bool is_inited_;
  ObArray<common::ObTabletID> tablet_id_array_;
  int64_t tablet_index_;
  ObLSHandle ls_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyTabletInfoObProducer);
};

class ObCopyRemoteSSTableInfoObProducer final
{
public:
  ObCopyRemoteSSTableInfoObProducer();
  ~ObCopyRemoteSSTableInfoObProducer() {}

  int init (
      const common::ObTabletID tablet_id,
      ObLS *ls);
  int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_copy_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);

  void reset();

private:
  int check_need_copy_sstable_(
      blocksstable::ObSSTable *sstable,
      bool &need_copy_sstable);
  int get_copy_sstable_count_(int64_t &sstable_count);
  int get_tablet_meta_(ObMigrationTabletParam &tablet_meta);

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator iter_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyRemoteSSTableInfoObProducer);
};

class ObICopySSTableInfoReader
{
public:
  enum Type {
    COPY_SSTABLE_INFO_OB_READER = 0,
    COPY_SSTABLE_INFO_RESTORE_READER = 1,
    MAX_TYPE
  };
  ObICopySSTableInfoReader() {}
  virtual ~ObICopySSTableInfoReader() {}
  virtual int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info) = 0;
  virtual int get_next_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header) = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopySSTableInfoReader);
};

class ObCopySSTableInfoObReader : public ObICopySSTableInfoReader
{
public:
  ObCopySSTableInfoObReader();
  virtual ~ObCopySSTableInfoObReader() {}

  int init(
      const ObStorageHASrcInfo &src_info,
      const obrpc::ObCopyTabletsSSTableInfoArg &rpc_arg,
      obrpc::ObStorageRpcProxy &srv_rpc_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  virtual int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  virtual int get_next_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);

  virtual Type get_type() const { return COPY_SSTABLE_INFO_OB_READER; }

private:
  int fetch_sstable_meta_(obrpc::ObCopyTabletSSTableInfo &sstable_info);

private:
  static const int64_t FETCH_TABLET_SSTABLE_INFO_TIMEOUT = 60 * 1000 * 1000; //60s
  bool is_inited_;
  ObStorageStreamRpcReader<obrpc::OB_HA_FETCH_SSTABLE_INFO> rpc_reader_;
  common::ObArenaAllocator allocator_;
  bool is_sstable_iter_end_;
  int64_t sstable_index_;
  int64_t sstable_count_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableInfoObReader);
};

class ObCopySSTableInfoRestoreReader : public ObICopySSTableInfoReader
{
public:
  ObCopySSTableInfoRestoreReader();
  virtual ~ObCopySSTableInfoRestoreReader() {}

  int init(
      const share::ObLSID &ls_id,
      const ObRestoreBaseInfo &restore_base_info,
      const ObTabletRestoreAction::ACTION &restore_action,
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store);
  virtual int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  virtual int get_next_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);
  virtual Type get_type() const { return COPY_SSTABLE_INFO_RESTORE_READER; }

private:
  int get_tablet_sstable_header_from_backup_(
      const common::ObTabletID &tablet_id,
      obrpc::ObCopyTabletSSTableHeader &copy_header);
  int get_tablet_sstable_header_from_local_(
      const common::ObTabletID &tablet_id,
      obrpc::ObCopyTabletSSTableHeader &copy_header);
  int get_next_sstable_info_from_backup_(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_next_sstable_info_from_local_(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int fetch_sstable_meta_(
      const backup::ObBackupSSTableMeta &backup_sstable_meta,
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_backup_sstable_metas_(
      const common::ObTabletID &tablet_id);
  int inner_get_backup_sstable_metas_(
      const common::ObTabletID &tablet_id,
      const share::ObBackupDataType data_type,
      common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array);
  int filter_backup_sstable_meta_on_data_type_(
      const share::ObBackupDataType data_type,
      common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array);
  int get_macro_block_backup_path_(
      const backup::ObBackupMetaIndex &sstable_meta_index,
      const share::ObBackupDataType data_type,
      ObBackupPath &tablet_meta_backup_path);
  int set_backup_sstable_meta_array_(
      const common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array);
  int get_backup_tablet_meta_(
      const common::ObTabletID &tablet_id,
      obrpc::ObCopyTabletSSTableHeader &copy_header);
  int fetch_backup_tablet_meta_index_(
      const common::ObTabletID &tablet_id,
      const share::ObBackupDataType &backup_data_type,
      backup::ObBackupMetaIndex &meta_index);
  int get_backup_tablet_meta_backup_path_(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupDataType &backup_data_type,
      const backup::ObBackupMetaIndex &meta_index,
      share::ObBackupPath &backup_path);
  int read_backup_tablet_meta_(
      const share::ObBackupPath &backup_path,
      const share::ObBackupStorageInfo *storage_info,
      const share::ObBackupDataType &backup_data_type,
      const backup::ObBackupMetaIndex &meta_index,
      backup::ObBackupTabletMeta &tablet_meta);
  int compare_storage_schema_(
      const common::ObTabletID &tablet_id,
      const ObTabletHandle &old_tablet_handle,
      const backup::ObBackupTabletMeta &tablet_meta,
      bool &need_update);

private:
  bool is_inited_;
  const ObRestoreBaseInfo *restore_base_info_;
  ObTabletRestoreAction::ACTION restore_action_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  int64_t tablet_index_;
  int64_t sstable_count_;
  int64_t sstable_index_;
  bool is_sstable_iter_end_;
  common::ObArray<backup::ObBackupSSTableMeta> backup_sstable_meta_array_;
  common::ObArenaAllocator allocator_;
  share::ObLSID ls_id_;
  ObLSHandle ls_handle_;
  ObCopyRemoteSSTableInfoObProducer remote_sstable_producer_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableInfoRestoreReader);
};

class ObCopyTabletsSSTableInfoObProducer
{
public:
  ObCopyTabletsSSTableInfoObProducer();
  virtual ~ObCopyTabletsSSTableInfoObProducer();

  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObIArray<obrpc::ObCopyTabletSSTableInfoArg> &tablet_sstable_info_array);
  int get_next_tablet_sstable_info(
      obrpc::ObCopyTabletSSTableInfoArg &arg);
private:
  bool is_inited_;
  ObLSHandle ls_handle_;
  common::ObArray<obrpc::ObCopyTabletSSTableInfoArg> tablet_sstable_info_array_;
  int64_t tablet_index_;
};

class ObCopySSTableInfoObProducer
{
public:
  ObCopySSTableInfoObProducer();
  virtual ~ObCopySSTableInfoObProducer() {}

  int init (
      const obrpc::ObCopyTabletSSTableInfoArg &tablet_sstable_info,
      ObLS *ls);
  int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_copy_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);
private:
  int check_need_copy_sstable_(
      blocksstable::ObSSTable *sstable,
      bool &need_copy_sstable);
  int get_copy_sstable_count_(int64_t &sstable_count);
  int get_tablet_meta_(ObMigrationTabletParam &tablet_meta);
  int fake_deleted_tablet_meta_(ObMigrationTabletParam &tablet_meta);

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  obrpc::ObCopyTabletSSTableInfoArg tablet_sstable_info_;
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator iter_;
  storage::ObCopyTabletStatus::STATUS status_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableInfoObProducer);
};

class ObICopySSTableMacroInfoReader
{
public:
  enum Type {
    COPY_SSTABLE_MACRO_INFO_OB_READER = 0,
    COPY_SSTABLE_MACRO_INFO_RESTORE_READER = 1,
    MAX_TYPE
  };
  ObICopySSTableMacroInfoReader() {}
  virtual ~ObICopySSTableMacroInfoReader() {}
  virtual int get_next_sstable_range_info(
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info) = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopySSTableMacroInfoReader);
};

class ObCopySSTableMacroObReader : public ObICopySSTableMacroInfoReader
{
public:
  ObCopySSTableMacroObReader();
  virtual ~ObCopySSTableMacroObReader() {}

  int init(
      const ObStorageHASrcInfo &src_info,
      const obrpc::ObCopySSTableMacroRangeInfoArg &rpc_arg,
      obrpc::ObStorageRpcProxy &srv_rpc_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);

  virtual int get_next_sstable_range_info(
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  virtual Type get_type() const { return COPY_SSTABLE_MACRO_INFO_OB_READER; }

private:
  int fetch_sstable_macro_range_header_(obrpc::ObCopySSTableMacroRangeInfoHeader &header);
  int fetch_sstable_macro_range_(
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
      common::ObIArray<ObCopyMacroRangeInfo> &macro_range_info_array);

private:
  static const int64_t FETCH_SSTABLE_MACRO_INFO_TIMEOUT = 60 * 1000 * 1000; //60s
  bool is_inited_;
  ObStorageStreamRpcReader<obrpc::OB_HA_FETCH_SSTABLE_MACRO_INFO> rpc_reader_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroObReader);
};


class ObCopySSTableMacroObProducer
{
public:
  ObCopySSTableMacroObProducer();
  virtual ~ObCopySSTableMacroObProducer() {}

  int init(
      const uint64_t tenant_id,
      const share::ObLSID & ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObIArray<ObITable::TableKey> &copy_table_key_array,
      const int64_t macro_range_max_marco_count);

  int get_next_sstable_macro_range_info(obrpc::ObCopySSTableMacroRangeInfoHeader &header);
private:
  int get_next_sstable_macro_range_info_(
      obrpc::ObCopySSTableMacroRangeInfoHeader &header);
private:
  bool is_inited_;
  common::ObArray<ObITable::TableKey> copy_table_key_array_;
  int64_t sstable_index_;
  bool is_sstable_iter_init_;
  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_;
  int64_t macro_range_max_marco_count_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroObProducer);
};

class ObICopySSTableMacroRangeObProducer
{
public:
  enum Type {
    COPY_SSTABLE_MACRO_RANGE_INFO_OB_PRODUCER = 0,
    COPY_DDL_SSTABLE_MACRO_RANGE_INFO_OB_PRODUCER = 1,
    MAX_TYPE
  };
  ObICopySSTableMacroRangeObProducer() {}
  virtual ~ObICopySSTableMacroRangeObProducer() {}
  virtual int get_next_macro_range_info(
      ObCopyMacroRangeInfo &macro_range_info) = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopySSTableMacroRangeObProducer);
};

class ObCopySSTableMacroRestoreReader : public ObICopySSTableMacroInfoReader
{
public:
  ObCopySSTableMacroRestoreReader();
  virtual ~ObCopySSTableMacroRestoreReader() {}

  int init(
      const obrpc::ObCopySSTableMacroRangeInfoArg &rpc_arg,
      const ObRestoreBaseInfo &restore_base_info,
      const ObTabletRestoreAction::ACTION &restore_action,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
      backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store);

  virtual int get_next_sstable_range_info(
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  virtual Type get_type() const { return COPY_SSTABLE_MACRO_INFO_RESTORE_READER; }

private:
  int get_next_sstable_range_info_from_local_(
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  int fetch_sstable_macro_range_from_local_(
    const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
    common::ObIArray<ObCopyMacroRangeInfo> &macro_range_info_array);

  int get_next_sstable_range_info_from_backup_(
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  int get_next_sstable_range_info_(
      const ObITable::TableKey &table_key,
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  int build_sstable_range_info_(
      const common::ObTabletID &tablet_id,
      const storage::ObTabletHandle &tablet_handle,
      const ObITable::TableKey &table_key,
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  int build_sstable_range_info_using_iterator_(
      const common::ObTabletID &tablet_id,
      const storage::ObTabletHandle &tablet_handle,
      const ObITable::TableKey &table_key,
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);

  int fetch_sstable_macro_range_header_(obrpc::ObCopySSTableMacroRangeInfoHeader &header);
  int fetch_sstable_macro_range_(
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
      common::ObIArray<ObCopyMacroRangeInfo> &macro_range_info_array);
  int get_next_shared_ddl_sstable_range_info_(
      const ObITable::TableKey &table_key,
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);
  int build_shared_ddl_sstable_range_info_(
      const ObITable::TableKey &table_key,
      const common::ObIArray<backup::ObBackupLinkedItem> &link_item,
      ObCopySSTableMacroRangeInfo &sstable_macro_range_info);

private:
  static const int64_t FETCH_SSTABLE_MACRO_INFO_TIMEOUT = 60 * 1000 * 1000; //60s
  bool is_inited_;
  obrpc::ObCopySSTableMacroRangeInfoArg rpc_arg_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  int64_t sstable_index_;
  ObTabletRestoreAction::ACTION restore_action_;
  ObCopySSTableMacroObProducer sstable_macro_producer_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroRestoreReader);
};

class ObCopySSTableMacroRangeObProducer : public ObICopySSTableMacroRangeObProducer
{
public:
  ObCopySSTableMacroRangeObProducer();
  virtual ~ObCopySSTableMacroRangeObProducer() { second_meta_iterator_.reset(); }
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
      const int64_t macro_range_max_marco_count);
  virtual int get_next_macro_range_info(ObCopyMacroRangeInfo &macro_range_info);
  virtual Type get_type() const { return COPY_SSTABLE_MACRO_RANGE_INFO_OB_PRODUCER; }

private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  int64_t macro_range_count_;
  int64_t macro_range_index_;
  int64_t macro_range_max_marco_count_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
  ObDatumRange datum_range_;
  common::ObArenaAllocator allocator_;
  ObSSTableSecMetaIterator second_meta_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTableMacroRangeObProducer);
};

class ObDDLCopySSTableMacroRangeObProducer : public ObICopySSTableMacroRangeObProducer
{
public:
  ObDDLCopySSTableMacroRangeObProducer();
  virtual ~ObDDLCopySSTableMacroRangeObProducer() { iterator_.reset(); }
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
      const int64_t macro_range_max_marco_count);
  virtual int get_next_macro_range_info(ObCopyMacroRangeInfo &macro_range_info);
  virtual Type get_type() const { return COPY_DDL_SSTABLE_MACRO_RANGE_INFO_OB_PRODUCER; }

public:
  static const int64_t MACRO_RANGE_MAX_MACRO_COUNT = 128;
  static const int64_t SINGLE_MACRO_ID_FIXED_LENGTH = sizeof(MacroBlockId);
  static const int64_t MAX_BUF_SIZE = OB_MAX_ROWKEY_COLUMN_NUMBER * SINGLE_MACRO_ID_FIXED_LENGTH;
private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  int64_t macro_range_count_;
  int64_t macro_range_index_;
  int64_t macro_range_max_marco_count_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
  ObSSTableMetaHandle meta_handle_;
  ObMacroIdIterator iterator_;
  char buf_[MAX_BUF_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObDDLCopySSTableMacroRangeObProducer);
};

class ObICopyLSViewInfoReader
{
public:
  enum Type {
    COPY_LS_ALL_VIEW_OB_READER = 0,
    COPY_LS_ALL_VIEW_RESTORE_READER = 1,
    MAX_TYPE
  };
  ObICopyLSViewInfoReader() {}
  virtual ~ObICopyLSViewInfoReader() {}
  virtual int get_ls_meta(
      ObLSMetaPackage &ls_meta) = 0;
  virtual int get_next_tablet_info(
      obrpc::ObCopyTabletInfo &tablet_info) = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopyLSViewInfoReader);
};


class ObCopyLSViewInfoObReader final : public ObICopyLSViewInfoReader
{
public:
  ObCopyLSViewInfoObReader();
  virtual ~ObCopyLSViewInfoObReader() {}

  int init(
      const ObStorageHASrcInfo &src_info,
      const obrpc::ObCopyLSViewArg &rpc_arg,
      obrpc::ObStorageRpcProxy &srv_rpc_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);

  Type get_type() const override
  {
    return COPY_LS_ALL_VIEW_OB_READER;
  }

  int get_ls_meta(
      ObLSMetaPackage &ls_meta) override;

  int get_next_tablet_info(
      obrpc::ObCopyTabletInfo &tablet_info) override;

private:
  static const int64_t FETCH_LS_VIEW_INFO_TIMEOUT = 60 * 1000 * 1000; // 1min
  bool is_inited_;
  ObLSMetaPackage ls_meta_;
  ObStorageStreamRpcReader<obrpc::OB_HA_FETCH_LS_VIEW> rpc_reader_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyLSViewInfoObReader);
};


class ObCopyLSViewInfoRestoreReader final : public ObICopyLSViewInfoReader
{
public:
  ObCopyLSViewInfoRestoreReader();
  virtual ~ObCopyLSViewInfoRestoreReader() {}
  int init(
      const share::ObLSID &ls_id,
      const ObRestoreBaseInfo &restore_base_info,
      backup::ObBackupMetaIndexStoreWrapper *meta_index_store);

  Type get_type() const override
  {
    return COPY_LS_ALL_VIEW_RESTORE_READER;
  }

  int get_ls_meta(
      ObLSMetaPackage &ls_meta) override;

  int get_next_tablet_info(
      obrpc::ObCopyTabletInfo &tablet_info) override;
private:
  int init_for_4_1_x_(const share::ObLSID &ls_id,
      const ObRestoreBaseInfo &restore_base_info,
      backup::ObBackupMetaIndexStoreWrapper &meta_index_store);

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObExternTabletMetaReader reader_;
  ObCopyTabletInfoRestoreReader reader_41x_; // only used by 4.1

  DISALLOW_COPY_AND_ASSIGN(ObCopyLSViewInfoRestoreReader);
};

class ObCopyRemoteSSTableMacroBlockRestoreReader: public ObICopyMacroBlockReader
{
public:
  ObCopyRemoteSSTableMacroBlockRestoreReader();
  virtual ~ObCopyRemoteSSTableMacroBlockRestoreReader();
  int init(const ObCopyMacroBlockReaderInitParam &param);
  virtual int get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data);
  virtual Type get_type() const { return REMOTE_SSTABLE_MACRO_BLOCK_RESTORE_READER; }
  virtual int64_t get_data_size() const { return data_size_; }

private:
  int alloc_buffers_();
  int fetch_macro_block_index_(
      const int64_t block_id_idx,
      const share::ObBackupDataType &backup_data_type,
      blocksstable::ObLogicMacroBlockId &logic_block_id,
      backup::ObBackupMacroBlockIndex &macro_index);

  int read_backup_macro_block_data_(
      blocksstable::ObDataMacroBlockMeta &macro_meta,
      ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data);
  int get_backup_macro_block_index_(
      const ObRestoreMacroBlockId &macro_id,
      backup::ObBackupMacroBlockIndex &macro_index);
  int do_read_backup_macro_block_data_(
      const backup::ObBackupMacroBlockIndex &macro_index,
      blocksstable::ObBufferReader &data_buffer);
  int read_local_macro_block_data_(
      blocksstable::ObDataMacroBlockMeta &macro_meta,
      ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data);

private:
  bool is_inited_;
  ObITable::TableKey table_key_;
  int64_t data_version_;
  const ObCopyMacroRangeInfo *copy_macro_range_info_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;


  blocksstable::ObBufferReader backup_macro_data_buffer_;
  blocksstable::ObBufferReader backup_macro_read_buffer_;
  char *local_macro_data_buffer_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 sstable_handle_;
  const ObSSTable *sstable_;
  ObSSTableSecMetaIterator second_meta_iterator_;
  ObDatumRange datum_range_;
  int64_t macro_block_count_;
  int64_t data_size_;
  ObSelfBufferWriter meta_row_buf_;
  ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyRemoteSSTableMacroBlockRestoreReader);
};

class ObRebuildTabletSSTableInfoObReader
{
public:
  ObRebuildTabletSSTableInfoObReader();
  virtual ~ObRebuildTabletSSTableInfoObReader() {}

  int init(
      const ObStorageHASrcInfo &src_info,
      const obrpc::ObRebuildTabletSSTableInfoArg &rpc_arg,
      obrpc::ObStorageRpcProxy &srv_rpc_proxy,
      common::ObInOutBandwidthThrottle &bandwidth_throttle);
  int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_next_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);
private:
  int fetch_sstable_meta_(obrpc::ObCopyTabletSSTableInfo &sstable_info);

private:
  static const int64_t FETCH_TABLET_SSTABLE_INFO_TIMEOUT = 60 * 1000 * 1000; //60s
  bool is_inited_;
  ObStorageStreamRpcReader<obrpc::OB_HA_REBUILD_TABLET_SSTABLE_INFO> rpc_reader_;
  common::ObArenaAllocator allocator_;
  bool is_sstable_iter_end_;
  int64_t sstable_index_;
  int64_t sstable_count_;
  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletSSTableInfoObReader);
};

class ObRebuildTabletSSTableProducer
{
public:
  ObRebuildTabletSSTableProducer();
  virtual ~ObRebuildTabletSSTableProducer() {}

  int init (
      const obrpc::ObRebuildTabletSSTableInfoArg &tablet_sstable_info,
      ObLS *ls);
  int get_next_sstable_info(
      obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_copy_tablet_sstable_header(
      obrpc::ObCopyTabletSSTableHeader &copy_header);
private:
  int get_copy_sstable_info_(const obrpc::ObRebuildTabletSSTableInfoArg &tablet_sstable_info);
  int get_tablet_meta_(ObMigrationTabletParam &tablet_meta);
  int fake_deleted_tablet_meta_(ObMigrationTabletParam &tablet_meta);

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  obrpc::ObRebuildTabletSSTableInfoArg tablet_sstable_info_;
  ObTabletHandle tablet_handle_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper_;
  storage::ObCopyTabletStatus::STATUS status_;
  int64_t sstable_count_;
  int64_t sstable_index_;
  ObITable::TableKey major_table_key_;
  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletSSTableProducer);
};


}
}
#endif

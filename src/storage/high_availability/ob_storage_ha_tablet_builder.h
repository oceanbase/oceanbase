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

#ifndef OCEABASE_STORAGE_HA_TABLET_BUILDER
#define OCEABASE_STORAGE_HA_TABLET_BUILDER

#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_storage_restore_struct.h"
#include "ob_storage_ha_reader.h"

namespace oceanbase
{
namespace storage
{

class ObStorageHATableInfoMgr;
struct ObStorageHATabletsBuilderParam final
{
  ObStorageHATabletsBuilderParam();
  ~ObStorageHATabletsBuilderParam() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObStorageHATabletsBuilderParam &param);

  TO_STRING_KV(K_(tenant_id), KPC_(ls), K_(tablet_id_array), K_(src_info), K_(local_rebuild_seq),
      K_(need_check_seq), K_(is_leader_restore), K_(need_keep_old_tablet), KP_(ha_table_info_mgr),
      K_(restore_action), KP_(bandwidth_throttle), KP_(svr_rpc_proxy), KP_(storage_rpc));

  uint64_t tenant_id_;
  ObLS *ls_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  ObStorageHASrcInfo src_info_;
  int64_t local_rebuild_seq_;
  bool need_check_seq_;
  bool is_leader_restore_;
  bool need_keep_old_tablet_;

  ObStorageHATableInfoMgr *ha_table_info_mgr_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  const ObRestoreBaseInfo *restore_base_info_;
  ObTabletRestoreAction::ACTION restore_action_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHATabletsBuilderParam);
};

class ObStorageHATabletsBuilder
{
public:
  typedef hash::ObHashMap<common::ObTabletID, ObCopyTabletSimpleInfo> CopyTabletSimpleInfoMap;
  ObStorageHATabletsBuilder();
  virtual ~ObStorageHATabletsBuilder();
  int init(const ObStorageHATabletsBuilderParam &param);
  int create_or_update_tablets();
  int build_tablets_sstable_info();
  int update_local_tablets();
  const CopyTabletSimpleInfoMap &get_tablets_simple_info_map() { return tablet_simple_info_map_ ; }

private:
  int get_tablet_info_reader_(ObICopyTabletInfoReader *&reader);
  int get_tablet_info_restore_reader_(ObICopyTabletInfoReader *&reader);
  int get_tablet_info_ob_reader_(ObICopyTabletInfoReader *&reader);
  void free_tablet_info_reader_(ObICopyTabletInfoReader *&reader);
  int create_or_update_tablet_(
      const obrpc::ObCopyTabletInfo &tablet_info,
      ObLS *ls);
  int get_tablets_sstable_reader_(ObICopySSTableInfoReader *&reader);
  int build_tablets_sstable_info_(
      const obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_tablets_sstable_restore_reader_(ObICopySSTableInfoReader *&reader);
  int get_tablets_sstable_ob_reader_(ObICopySSTableInfoReader *&reader);
  void free_sstable_info_reader_(ObICopySSTableInfoReader *&reader);

  int build_copy_tablets_sstable_info_arg_(obrpc::ObCopyTabletsSSTableInfoArg &arg);
  int build_copy_tablet_sstable_info_arg_(
      const common::ObTabletID &tablet_id,
      obrpc::ObCopyTabletSSTableInfoArg &arg);
  int get_major_sstable_max_snapshot_(
      const ObSSTableArray &major_sstable_array,
      int64_t &max_snapshot_version);
  int get_remote_logical_minor_log_ts_range_(
      const ObSSTableArray &minor_sstable_array,
      ObLogTsRange &log_ts_range);
  int get_need_copy_ddl_sstable_range_(
      const ObTablet *tablet,
      const ObSSTableArray &ddl_sstable_array,
      ObLogTsRange &log_ts_range);
  int get_ddl_sstable_min_start_log_ts_(
      const ObSSTableArray &ddl_sstable_array,
      int64_t &min_start_log_ts);
  int hold_local_reuse_sstable_(
      const common::ObTabletID &tablet_id,
      ObTablesHandleArray &tables_handle);
  int hold_local_complete_tablet_sstable_(
      ObTablet *tablet,
      ObTablesHandleArray &tables_handle);
  int remove_uncomplete_tablet_(
      const common::ObTabletID &tablet_id);
  int create_tablet_remote_logical_sstable_(
      const common::ObTabletID &tablet_id,
      ObTablesHandleArray &tables_handle);
  int create_remote_logical_sstable_(
      const common::ObTabletID &tablet_id,
      const int64_t start_log_ts,
      const int64_t end_log_ts,
      ObTablet *tablet,
      ObTableHandleV2 &table_handle);
  int build_remote_logical_sstable_param_(
      const int64_t start_log_ts,
      const int64_t end_log_ts,
      const ObStorageSchema &table_schema,
      const common::ObTabletID &tablet_id,
      ObTabletCreateSSTableParam &param);
  int update_local_tablet_(
      const obrpc::ObCopyTabletInfo &tablet_info,
      ObLS *ls);

  int create_tablet_remote_logical_sstable_(
      ObTablet *tablet,
      ObTablesHandleArray &tables_handle);

private:
  bool is_inited_;
  ObStorageHATabletsBuilderParam param_;
  CopyTabletSimpleInfoMap tablet_simple_info_map_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHATabletsBuilder);
};

class ObStorageHATableInfoMgr
{
public:
  ObStorageHATableInfoMgr();
  virtual ~ObStorageHATableInfoMgr();
  int init();
  int add_table_info(
      const common::ObTabletID &tablet_id,
      const obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_table_info(
      const common::ObTabletID &tablet_id,
      const ObITable::TableKey &table_key,
      const blocksstable::ObMigrationSSTableParam *&copy_table_info);
  int get_table_keys(
      const common::ObTabletID &tablet_id,
      common::ObIArray<ObITable::TableKey> &table_keys);
  int remove_tablet_table_info(const common::ObTabletID &tablet_id);
  int init_tablet_info(const obrpc::ObCopyTabletSSTableHeader &copy_header);
  int check_copy_tablet_exist(const common::ObTabletID &tablet_id, bool &is_exist);
  int check_tablet_table_info_exist(
      const common::ObTabletID &tablet_id, bool &is_exist);
  int get_tablet_meta(
      const common::ObTabletID &tablet_id,
      const ObMigrationTabletParam *&tablet_meta);
  void reuse();

public:
  class ObStorageHATabletTableInfoMgr
  {
  public:
    ObStorageHATabletTableInfoMgr();
    virtual ~ObStorageHATabletTableInfoMgr();
    int init(const common::ObTabletID &tablet_id,
        const storage::ObCopyTabletStatus::STATUS &status,
        const ObMigrationTabletParam &tablet_meta);
    int add_copy_table_info(const blocksstable::ObMigrationSSTableParam &copy_table_info);
    int get_copy_table_info(
        const ObITable::TableKey &table_key,
        const blocksstable::ObMigrationSSTableParam *&copy_table_info);
    int get_table_keys(
        common::ObIArray<ObITable::TableKey> &table_keys);
    int check_copy_tablet_exist(bool &is_exist);
    int get_tablet_meta(const ObMigrationTabletParam *&tablet_meta);
  private:
    bool is_inited_;
    common::ObTabletID tablet_id_;
    storage::ObCopyTabletStatus::STATUS status_;
    common::ObArray<blocksstable::ObMigrationSSTableParam> copy_table_info_array_;
    ObMigrationTabletParam tablet_meta_;
    DISALLOW_COPY_AND_ASSIGN(ObStorageHATabletTableInfoMgr);
  };

private:
  static const int64_t MAX_BUCEKT_NUM = 4096;
  typedef hash::ObHashMap<common::ObTabletID, ObStorageHATabletTableInfoMgr *> TabletTableInfoMgr;
  bool is_inited_;
  common::SpinRWLock lock_;
  TabletTableInfoMgr table_info_mgr_map_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHATableInfoMgr);
};

struct ObStorageHACopySSTableParam final
{
  ObStorageHACopySSTableParam();
  ~ObStorageHACopySSTableParam() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObStorageHACopySSTableParam &param);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(copy_table_key_array),
      K_(src_info), K_(local_rebuild_seq), K_(need_check_seq),
      KP_(bandwidth_throttle), KP_(svr_rpc_proxy), KP_(storage_rpc));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObArray<ObITable::TableKey> copy_table_key_array_;

  ObStorageHASrcInfo src_info_;
  int64_t local_rebuild_seq_;
  bool need_check_seq_;
  bool is_leader_restore_;

  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHACopySSTableParam);
};

class ObStorageHACopySSTableInfoMgr
{
public:
  ObStorageHACopySSTableInfoMgr();
  virtual ~ObStorageHACopySSTableInfoMgr();
  int init(const ObStorageHACopySSTableParam &param);

  int get_copy_sstable_maro_range_info(
      const ObITable::TableKey &copy_table_key,
      ObCopySSTableMacroRangeInfo &copy_sstable_macro_range_info);
private:
  int build_sstable_macro_range_info_map_();
  int get_sstable_macro_range_info_reader_(ObICopySSTableMacroInfoReader *&reader);
  int get_sstable_macro_range_info_ob_reader_(ObICopySSTableMacroInfoReader *&reader);
  int get_sstable_macro_range_info_restore_reader_(ObICopySSTableMacroInfoReader *&reader);
  void free_sstable_macro_range_info_reader_(ObICopySSTableMacroInfoReader *&reader);

private:
  static const int64_t MACRO_RANGE_MAX_MACRO_COUNT = 1024;
  typedef hash::ObHashMap<ObITable::TableKey, ObCopySSTableMacroRangeInfo *> CopySSTableMacroRangeInfoMap;
  bool is_inited_;
  ObStorageHACopySSTableParam param_;
  ObArenaAllocator allocator_;
  CopySSTableMacroRangeInfoMap macro_range_info_map_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHACopySSTableInfoMgr);
};




}
}
#endif

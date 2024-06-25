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

namespace share
{
class SCN;
}
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
  // Create all tablets with remote tablet meta.
  int create_or_update_tablets();
  int create_all_tablets(
      const bool need_check_tablet_limit,
      ObICopyLSViewInfoReader *reader,
      common::ObIArray<common::ObTabletID> &sys_tablet_id_list,
      common::ObIArray<common::ObTabletID> &data_tablet_id_list,
      CopyTabletSimpleInfoMap &simple_info_map);
  // Restore PENDING tablets meta. PENDING tablets will be exist at restore phase RESTORE_SYS_TABLETS,
  // RESTORE_TO_CONSISTENT_SCN, or QUICK_RESTORE. Leader gets the meta from backup, follower gets it from leader.
  // If that tablet meta identified uniquely by transfer sequence exists, replace and update the restore status to EMPTY.
  // Otherwise, just update it to UNDEFINED.
  int update_pending_tablets_with_remote();
  int build_tablets_sstable_info();
  int create_all_tablets_with_4_1_rpc(
      CopyTabletSimpleInfoMap &simple_info_map);
private:
  int get_tablet_info_reader_(ObICopyTabletInfoReader *&reader);
  int get_tablet_info_restore_reader_(ObICopyTabletInfoReader *&reader);
  int get_tablet_info_ob_reader_(ObICopyTabletInfoReader *&reader);
  void free_tablet_info_reader_(ObICopyTabletInfoReader *&reader);
  int create_or_update_tablet_(
      const obrpc::ObCopyTabletInfo &tablet_info,
      const bool need_check_tablet_limit,
      ObLS *ls);
  int get_tablets_sstable_reader_(
      const common::ObIArray<ObTabletHandle> &tablet_handle_array,
      ObICopySSTableInfoReader *&reader);
  int build_tablets_sstable_info_(
      const obrpc::ObCopyTabletSSTableInfo &sstable_info);
  int get_tablets_sstable_restore_reader_(
      const common::ObIArray<ObTabletHandle> &tablet_handle_array,
      ObICopySSTableInfoReader *&reader);
  int get_tablets_sstable_ob_reader_(
      const common::ObIArray<ObTabletHandle> &tablet_handle_array,
      ObICopySSTableInfoReader *&reader);
  void free_sstable_info_reader_(ObICopySSTableInfoReader *&reader);

  int build_copy_tablets_sstable_info_arg_(
      const common::ObIArray<ObTabletHandle> &tablet_handle_array,
      obrpc::ObCopyTabletsSSTableInfoArg &arg);
  int build_copy_tablet_sstable_info_arg_(
      const ObTabletHandle &tablet_handle,
      obrpc::ObCopyTabletSSTableInfoArg &arg);
  int get_major_sstable_max_snapshot_(
      const ObSSTableArray &major_sstable_array,
      int64_t &max_snapshot_version);
  int get_minor_scn_range_(
      const ObSSTableArray &minor_sstable_array,
      ObTablet *tablet,
      share::ObScnRange &scn_range);
  int get_need_copy_ddl_sstable_range_(
      const ObTablet *tablet,
      const ObSSTableArray &ddl_sstable_array,
      share::ObScnRange &scn_range);
  int get_ddl_sstable_min_start_scn_(
      const ObSSTableArray &ddl_sstable_array,
      share::SCN &max_start_scn);
  int hold_local_reuse_sstable_(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &local_tablet_hdl,
      ObTablesHandleArray &tables_handle,
      ObStorageSchema &storage_schema,
      common::ObIAllocator &allocator);
  int hold_local_complete_tablet_sstable_(
      ObTablet *tablet,
      ObTablesHandleArray &tables_handle);
  int remove_uncomplete_tablet_(
      const common::ObTabletID &tablet_id);
  int modified_tablet_info_(
      obrpc::ObCopyTabletInfo &tablet_info);

  int create_tablet_with_major_sstables_(
      ObLS *ls,
      const obrpc::ObCopyTabletInfo &tablet_info,
      const ObTablesHandleArray &major_tables,
      const ObStorageSchema &storage_schema);
  int hold_local_tablet_(
      common::ObIArray<ObTabletHandle> &tablet_handle_array);
private:
  bool is_inited_;
  ObStorageHATabletsBuilderParam param_;
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
    common::ObArenaAllocator allocator_;
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
  int check_src_tablet_exist(bool &is_exist);
private:
  int build_sstable_macro_range_info_map_();
  int get_sstable_macro_range_info_reader_(ObICopySSTableMacroInfoReader *&reader);
  int get_sstable_macro_range_info_ob_reader_(ObICopySSTableMacroInfoReader *&reader);
  int get_sstable_macro_range_info_restore_reader_(ObICopySSTableMacroInfoReader *&reader);
  void free_sstable_macro_range_info_reader_(ObICopySSTableMacroInfoReader *&reader);

private:
  static const int64_t MACRO_RANGE_MAX_MACRO_COUNT = 128;
  typedef hash::ObHashMap<ObITable::TableKey, ObCopySSTableMacroRangeInfo *> CopySSTableMacroRangeInfoMap;
  bool is_inited_;
  ObStorageHACopySSTableParam param_;
  ObArenaAllocator allocator_;
  CopySSTableMacroRangeInfoMap macro_range_info_map_;
  storage::ObCopyTabletStatus::STATUS status_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHACopySSTableInfoMgr);
};

class ObStorageHATabletBuilderUtil
{
public:
  static int build_tablet_with_major_tables(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const ObTablesHandleArray &major_tables,
      const ObStorageSchema &storage_schema);
  static int build_table_with_minor_tables(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const ObMigrationTabletParam *src_tablet_meta,
      const ObTablesHandleArray &mds_tables,
      const ObTablesHandleArray &minor_tables,
      const ObTablesHandleArray &ddl_tables,
      const ObTabletRestoreAction::ACTION &restore_action);
  static int check_remote_logical_sstable_exist(
      ObTablet *tablet,
      bool &is_exist);
private:
  static int build_tablet_for_row_store_(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const ObTablesHandleArray &major_tables,
      const ObStorageSchema &storage_schema);
  // for column store
  static int build_tablet_for_column_store_(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const ObTablesHandleArray &major_tables,
      const ObStorageSchema &storage_schema);

  static int get_tablet_(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      ObTabletHandle &tablet_handle);
  static int calc_multi_version_start_with_major_(
      const ObTablesHandleArray &major_tables,
      ObTablet *tablet,
      int64_t &multi_version_start);
  static int inner_update_tablet_table_store_with_major_(
      const int64_t multi_version_start,
      const ObTableHandleV2 &table_handle,
      ObLS *ls,
      ObTablet *tablet,
      const ObStorageSchema &storage_schema,
      const int64_t transfer_seq);
  static int inner_update_tablet_table_store_with_minor_(
      ObLS *ls,
      ObTablet *tablet,
      const bool &need_tablet_meta_merge,
      const ObMigrationTabletParam *src_tablet_meta,
      const ObTablesHandleArray &tables_handle);
  static int check_need_merge_tablet_meta_(
      const ObMigrationTabletParam *src_tablet_meta,
      ObTablet *tablet,
      bool &need_merge);
  static int assemble_column_oriented_sstable_(
      const ObTablesHandleArray &mixed_tables,
      ObTablesHandleArray &co_tables);
  static int get_column_store_tables_(
      const ObTablesHandleArray &major_tables,
      common::ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &column_store_tables,
      int64_t &co_table_cnt);
  static int build_tablet_with_co_tables_(
      ObLS *ls,
      ObTablet *tablet,
      const ObStorageSchema &storage_schema,
      const int64_t multi_version_start,
      const ObTablesHandleArray &co_tables);
  static int append_sstable_array_(ObTablesHandleArray &dest_array, const ObTablesHandleArray &src_array);
};




}
}
#endif

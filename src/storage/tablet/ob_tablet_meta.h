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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_META
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_META

#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ddl_common.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/ddl/ob_tablet_barrier_log.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_full_memory_mds_data.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/high_availability/ob_tablet_ha_status.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/high_availability/ob_tablet_transfer_info.h"
#include "storage/tablet/ob_tablet_space_usage.h"
#include "storage/blocksstable/ob_major_checksum_info.h"
#include "storage/column_store/ob_column_store_replica_ddl_helper.h"
namespace oceanbase
{
namespace storage
{
struct ObMigrationTabletParam;
class ObTabletMeta final
{
  friend class ObTablet;
public:
  static const share::SCN INIT_CLOG_CHECKPOINT_SCN;
  static const share::SCN INVALID_CREATE_SCN;
  static const share::SCN INIT_CREATE_SCN;

public:
  ObTabletMeta();
  ObTabletMeta(const ObTabletMeta &other) = delete;
  ObTabletMeta &operator=(const ObTabletMeta &other) = delete;
  ~ObTabletMeta();
public:
  // first init func
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const share::SCN create_scn,
      const int64_t snapshot_version,
      const lib::Worker::CompatMode compat_mode,
      const ObTabletTableStoreFlag &table_store_flag,
      const int64_t create_schema_version,
      const share::SCN &clog_checkpoint_scn,
      const share::SCN &mds_checkpoint_scn,
      const share::ObSplitTabletInfo &split_info,
      const bool micro_index_clustered,
      const bool has_cs_replica,
      const bool need_generate_cs_replica_cg_array,
      const bool has_truncate_info,
      const uint64_t ddl_data_format_version);
  int init(
      const ObTabletMeta &old_tablet_meta,
      const int64_t snapshot_version,
      const int64_t multi_version_start,
      const int64_t max_sync_storage_schema_version,
      const share::ObSplitTabletInfo &split_info,
      const share::SCN clog_checkpoint_scn = share::SCN::min_scn(),
      const ObDDLTableStoreParam &ddl_info = ObDDLTableStoreParam(),
      const bool has_truncate_info = false);
  int init(
      const ObTabletMeta &old_tablet_meta,
      const share::SCN &flush_scn);
  int init(
      const ObMigrationTabletParam &param,
      const bool is_transfer);
  int init(
      const ObTabletMeta &old_tablet_meta,
      const ObMigrationTabletParam *tablet_meta);
#ifdef OB_BUILD_SHARED_STORAGE
  int init_for_share_storage(const ObTabletMeta &old_tablet_meta);
  int update_for_share_storage(const ObTabletMeta &new_tablet_meta);
  share::SCN get_acquire_scn() const;
#endif

  int assign(const ObTabletMeta &other);
  void reset();
  bool is_valid() const;

  // serialize & deserialize
  int serialize(const uint64_t data_version, char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int64_t get_serialize_size(const uint64_t data_version) const;
  int reset_transfer_table();
  bool has_transfer_table() const;
  share::SCN get_ddl_sstable_start_scn() const;
  // Return the max replayed scn which is the max scn among clog_checkpoint_scn,
  // mds_checkpoint_scn and ddl_checkpoint_scn.
  // Note, if a new type of checkpoint scn is added, donot forget to modify the returned scn.
  share::SCN get_max_replayed_scn() const;
  // for column store replica
  bool is_cs_replica_global_visible_when_ddl() const;
  bool is_cs_replica_global_visible_and_replay_row_store() const;
  bool is_cs_replica_global_visible_and_replay_column_store() const;
public:
  static int deserialize_id(
      const char *buf,
      const int64_t len,
      int64_t &pos,
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id);
  static int init_report_info(
      const blocksstable::ObSSTable *sstable,
      const int64_t report_version,
      ObTabletReportStatus &report_status);
  static int init_report_info(
      const blocksstable::ObMajorChecksumInfo &major_ckm_info,
      const int64_t report_version,
      ObTabletReportStatus &report_status);
  static int update_meta_last_persisted_committed_tablet_status(
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const share::SCN &create_commit_scn,
    ObTabletCreateDeleteMdsUserData &last_persisted_committed_tablet_status);
public:
  TO_STRING_KV(K_(version),
               K_(ls_id),
               K_(tablet_id),
               K_(data_tablet_id),
               K_(ref_tablet_id),
               K_(create_scn),
               K_(start_scn),
               K_(clog_checkpoint_scn),
               K_(ddl_checkpoint_scn),
               K_(snapshot_version),
               K_(multi_version_start),
               K_(compat_mode),
               K_(ha_status),
               K_(report_status),
               K_(table_store_flag),
               K_(ddl_start_scn),
               K_(ddl_snapshot_version),
               K_(max_sync_storage_schema_version),
               K_(max_serialized_medium_scn),
               K_(ddl_execution_id),
               K_(ddl_data_format_version),
               K_(ddl_commit_scn),
               K_(mds_checkpoint_scn),
               K_(min_ss_tablet_version),
               K_(transfer_info),
               K_(extra_medium_info),
               K_(last_persisted_committed_tablet_status),
               K_(create_schema_version),
               K_(space_usage),
               K_(is_empty_shell),
               K_(micro_index_clustered),
               K_(ddl_replay_status),
               K_(split_info),
               K_(has_truncate_info),
               K_(inc_major_snapshot));

public:
  int32_t version_; // alignment: 4B, size: 4B
  int32_t length_; // alignment: 4B, size: 4B
  share::ObLSID ls_id_; // alignment: 8B, size: 8B
  common::ObTabletID tablet_id_; // alignment: 8B, size: 8B
  common::ObTabletID data_tablet_id_; // alignment: 8B, size: 8B
  common::ObTabletID ref_tablet_id_; // alignment: 8B, size: 8B
  share::SCN create_scn_; // alignment: 8B, size: 8B create_tablet_scn, not create_tablet_version_scn
  share::SCN start_scn_; // alignment: 8B, size: 8B
  share::SCN clog_checkpoint_scn_; // may less than last_minor->end_log_ts. alignment: 8B, size: 8B
  share::SCN ddl_checkpoint_scn_; // alignment: 8B, size: 8B
  int64_t snapshot_version_; // alignment: 8B, size: 8B
  int64_t multi_version_start_; // alignment: 8B, size: 8B
  ObTabletHAStatus ha_status_; // alignment: 8B, size: 8B
  ObTabletReportStatus report_status_; // alignment: 8B, size: 40B
  ObTabletTableStoreFlag table_store_flag_; // alignment: 8B, size: 8B
  share::SCN ddl_start_scn_; // alignment: 8B, size: 8B
  int64_t ddl_snapshot_version_; // alignment: 8B, size: 8B
  // max_sync_storage_schema_version_ = MIN(serialized_schema_version, sync_schema_version)
  // serialized_schema_version > sync_schema_version when major update storage schema
  // sync_schema_version > serialized_schema_version when replay schema clog but not mini merge yet
  // max_sync_storage_schema_version will be inaccurate after 4.2
  int64_t max_sync_storage_schema_version_; // alignment: 8B, size: 8B
  int64_t ddl_execution_id_; // alignment: 8B, size: 8B
  int64_t ddl_data_format_version_; // alignment: 8B, size: 8B
  int64_t max_serialized_medium_scn_; // abandon after 4.2 // alignment: 8B, size: 8B
  share::SCN ddl_commit_scn_; // alignment: 8B, size: 8B
  share::SCN mds_checkpoint_scn_; // alignment: 8B, size: 8B
  share::SCN min_ss_tablet_version_; // alignment: 8B, size: 8B
  ObTabletTransferInfo transfer_info_; // alignment: 8B, size: 32B
  compaction::ObExtraMediumInfo extra_medium_info_;
  ObTabletCreateDeleteMdsUserData last_persisted_committed_tablet_status_; // quick access for tablet status in sstables
  ObTabletSpaceUsage space_usage_; // calculated by tablet persist, ObMigrationTabletParam doesn't need it
                                   // alignment: 8B, size: 48B
  int64_t create_schema_version_; // add after 4.2, record schema_version when first create tablet. NEED COMPAT
                                  // alignment: 8B, size: 8B
  // Add after 4.3.3, is used to decide storage type for replaying ddl redo log and create ddl dump sstable in C-Replica.
  // ddl_replay_status only make sense when table schema is row store, originally column store table do not need be processed in C-Replica.
  // The are some conditions the ddl_replay_status used:
  //   1. When offline ddl is concurrent with adding C-Replica, it may write row store clog, but storage schema in C-Replica is columnar.
  //      In this condition, need replay row store ddl clog in C-Replica.
  //   2. When there are with_cs_replica ddl redo clogs, but tablet in C-Replica is migrated from a F/R-Replica and has replayed some row store redo log.
  //      In this condition, need replay row store ddl clog in C-Replica continuely.
  // So we need persist a field in tablet when creating user tablet to decide table_type when restart from a checkpoint, or migrating, etc.
  // - CS_REPLICA_REPLAY_NONE: initial state, tablet not doing offline ddl. only take this type for inital.
  // - CS_REPLICA_INVISILE/CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN/CS_REPLICA_VISIBLE_AND_REPLAY_ROW: tablet is doing offline ddl.
  // - CS_REPLICA_REPLAY_ROW_STORE_FINISH/CS_REPLICA_REPLAY_COLUMN_FINISH: tablet finish offline ddl, set when ddl merge task create major sstable.
  //
  // + -------------------- + -------------------------------- + ------------------------------------ +
  // | C-Replica is visible | ls (the tablet in) is C-Replica  | when tablet created                  |
  // + -------------------- + -------------------------------- + ------------------------------------ +
  // | FALSE                | TRUE/FALSE                       | CS_REPLICA_INVISILE                  |
  // | TRUE                 | FALSE                            | CS_REPLICA_VISIBLE_AND_REPLAY_ROW    |
  // | TRUE                 | TRUE                             | CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN |
  // + -------------------- + --------------------------------- + ----------------------------------- +
  ObCSReplicaDDLReplayStatus ddl_replay_status_;
  //ATTENTION : Add a new variable need consider ObMigrationTabletParam
  // and tablet meta init interface for migration.
  // yuque :
  lib::Worker::CompatMode compat_mode_; // alignment: 1B, size: 4B
  bool has_next_tablet_; // alignment: 1B, size: 2B (abandoned, don't use this field)
  bool is_empty_shell_; // alignment: 1B, size: 2B
  bool micro_index_clustered_; // alignment: 1B, size: 2B
  share::ObSplitTabletInfo split_info_; // alignment: 8B, size: 16B
  bool has_truncate_info_; // be True after first major with truncate info
  int64_t inc_major_snapshot_; // recording the latest inc major merge snapshot
private:
  void update_extra_medium_info(
      const compaction::ObMergeType merge_type,
      const int64_t finish_medium_scn,
      const bool need_wait_check_flag);
  void update_extra_medium_info(
      const compaction::ObExtraMediumInfo &src_addr_extra_info,
      const compaction::ObExtraMediumInfo &src_data_extra_info,
      const int64_t finish_medium_scn);
  int inner_check_(
      const ObTabletMeta &old_tablet_meta,
      const ObMigrationTabletParam *tablet_meta);
  inline void set_space_usage_ (const ObTabletSpaceUsage &space_usage) { space_usage_ = space_usage; }
  inline void set_min_ss_tablet_version_(const share::SCN &min_ss_tablet_version)
  {
    min_ss_tablet_version_ = min_ss_tablet_version;
  }
private:
  static const int32_t TABLET_META_VERSION = 1;
private:
  bool is_inited_;
};

// decouple tablet meta from migration parameter
struct ObMigrationTabletParam final
{
public:
  ObMigrationTabletParam();
  ~ObMigrationTabletParam() { reset(); }
  ObMigrationTabletParam(const ObMigrationTabletParam &) = delete;
  ObMigrationTabletParam &operator=(const ObMigrationTabletParam &) = delete;
public:
  bool is_valid() const;
  bool is_empty_shell() const;
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;
  void reset();
  int assign(const ObMigrationTabletParam &param);
  int build_deleted_tablet_info(const share::ObLSID &ls_id, const ObTabletID &tablet_id);
  int get_tablet_status_for_transfer(ObTabletCreateDeleteMdsUserData &user_data) const;
  const share::ObLSID &get_transfer_src_ls_id() const { return transfer_info_.ls_id_; }
  const share::ObLSID &get_transfer_dest_ls_id() const { return ls_id_; }

  // Return the max tablet checkpoint scn which is the max scn among clog_checkpoint_scn,
  // mds_checkpoint_scn and ddl_checkpoint_scn.
  // Note, if a new type of checkpoint scn is added, donot forget to modify the returned scn.
  share::SCN get_max_tablet_checkpoint_scn() const;

  // used for restore PENDING tablet, the placeholder tablet doesn't have storage schema to use
  static int construct_placeholder_storage_schema_and_medium(
      common::ObArenaAllocator &allocator,
      ObStorageSchema &storage_schema,
      compaction::ObMediumCompactionInfoList &medium_info_list,
      ObTabletFullMemoryMdsData &full_memory_mds_data);

  TO_STRING_KV(K_(magic_number),
               K_(version),
               K_(is_empty_shell),
               K_(ls_id),
               K_(tablet_id),
               K_(data_tablet_id),
               K_(ref_tablet_id),
               K_(create_scn),
               K_(start_scn),
               K_(clog_checkpoint_scn),
               K_(ddl_checkpoint_scn),
               K_(ddl_snapshot_version),
               K_(ddl_start_scn),
               K_(snapshot_version),
               K_(multi_version_start),
               K_(compat_mode),
               K_(ha_status),
               K_(report_status),
               K_(storage_schema),
               K_(medium_info_list),
               K_(extra_medium_info),
               K_(last_persisted_committed_tablet_status),
               K_(table_store_flag),
               K_(max_sync_storage_schema_version),
               K_(ddl_execution_id),
               K_(ddl_data_format_version),
               K_(max_serialized_medium_scn),
               K_(ddl_commit_scn),
               K_(mds_checkpoint_scn),
               K_(mds_data),
               K_(transfer_info),
               K_(create_schema_version),
               K_(micro_index_clustered),
               K_(major_ckm_info),
               K_(ddl_replay_status),
               K_(is_storage_schema_cs_replica),
               K_(split_info),
               K_(has_truncate_info),
               K_(min_ss_tablet_version),
               K_(inc_major_snapshot));
private:
  int deserialize_v2_v3(const char *buf, const int64_t len, int64_t &pos);
  int deserialize_v1(const char *buf, const int64_t len, int64_t &pos);

  // magic_number_ is added to support upgrade from old format(without version and length compatibility)
  // The old format first member is ls_id_(also 8 bytes long), which is not possible be a negative number.
  const static int64_t MAGIC_NUM = -20230111;
public:
  const static int64_t PARAM_VERSION = 1;
  const static int64_t PARAM_VERSION_V2 = 2;
  const static int64_t PARAM_VERSION_V3 = 3;
  int64_t magic_number_;
  int64_t version_;
  bool is_empty_shell_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID data_tablet_id_;
  common::ObTabletID ref_tablet_id_;
  share::SCN create_scn_;
  share::SCN start_scn_;              // for migration
  share::SCN clog_checkpoint_scn_;
  share::SCN ddl_checkpoint_scn_;
  int64_t snapshot_version_;
  int64_t multi_version_start_;
  lib::Worker::CompatMode compat_mode_;
  ObTabletHAStatus ha_status_;
  ObTabletReportStatus report_status_;
  ObStorageSchema storage_schema_; // not valid for empty shell
  compaction::ObMediumCompactionInfoList medium_info_list_; // not valid for empty shell
  compaction::ObExtraMediumInfo extra_medium_info_;
  // Record source tablet last_persisted_committed_tablet_status_ for:
  // 1. create migration shell tablet in case there's no valid status to get_tablet
  // 2. create transfer shell tablet for mds table data in case there's no valid status to get_tablet
  // In other scenes, latest status on destination tablet meta should be get from sstables. The one
  // on param has no usage.
  ObTabletCreateDeleteMdsUserData last_persisted_committed_tablet_status_;
  ObTabletTableStoreFlag table_store_flag_;
  share::SCN ddl_start_scn_;
  int64_t ddl_snapshot_version_;
  // max_sync_version may less than storage_schema.schema_version_ when major update schema
  int64_t max_sync_storage_schema_version_;
  int64_t ddl_execution_id_;
  int64_t ddl_data_format_version_;
  int64_t max_serialized_medium_scn_;
  share::SCN ddl_commit_scn_;
  share::SCN mds_checkpoint_scn_;
  ObTabletFullMemoryMdsData mds_data_;
  ObTabletTransferInfo transfer_info_;
  int64_t create_schema_version_;
  bool micro_index_clustered_;
  blocksstable::ObMajorChecksumInfo major_ckm_info_; // from table store
  ObCSReplicaDDLReplayStatus ddl_replay_status_;
  bool is_storage_schema_cs_replica_;
  share::ObSplitTabletInfo split_info_;
  // [since 4.3.5 bp2] be True after first major with truncate info
  // will never be false even after truncate info recycled
  bool has_truncate_info_;

  share::SCN min_ss_tablet_version_;
  int64_t inc_major_snapshot_; // recording the latest inc major merge snapshot
  // Add new serialization member before this line, below members won't serialize
  common::ObArenaAllocator allocator_; // for storage schema
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_TABLET_META

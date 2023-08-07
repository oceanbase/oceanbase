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
      const int64_t create_schema_version);
  int init(
      const ObTabletMeta &old_tablet_meta,
      const int64_t snapshot_version,
      const int64_t multi_version_start,
      const int64_t max_sync_storage_schema_version,
      const share::SCN clog_checkpoint_scn = share::SCN::min_scn(),
      const ObDDLTableStoreParam &ddl_info = ObDDLTableStoreParam());
  int init(
      const ObTabletMeta &old_tablet_meta,
      const share::SCN &flush_scn);
  int init(
      const ObMigrationTabletParam &param);
  int init(
      const ObTabletMeta &old_tablet_meta,
      const ObMigrationTabletParam *tablet_meta);
  int init(
      common::ObIAllocator &allocator,
      const ObTabletMeta &old_tablet_meta);

  void reset();
  bool is_valid() const;

  int assign(const ObTabletMeta &other);
  // serialize & deserialize
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int reset_transfer_table();
  bool has_transfer_table() const;
  share::SCN get_ddl_sstable_start_scn() const;
  // Return the max replayed scn which is the max scn among clog_checkpoint_scn,
  // mds_checkpoint_scn and ddl_checkpoint_scn.
  // Note, if a new type of checkpoint scn is added, donot forget to modify the returned scn.
  share::SCN get_max_replayed_scn() const;
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
public:
  TO_STRING_KV(K_(version),
               K_(ls_id),
               K_(tablet_id),
               K_(data_tablet_id),
               K_(ref_tablet_id),
               K_(has_next_tablet),
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
               K_(transfer_info),
               K_(create_schema_version));

public:
  int32_t version_;
  int32_t length_;
  share::ObLSID ls_id_; // alignment: 8B, size: 8B
  common::ObTabletID tablet_id_; // alignment: 8B, size: 8B
  common::ObTabletID data_tablet_id_;
  common::ObTabletID ref_tablet_id_;
  share::SCN create_scn_; // alignment: 8B, size: 8B
  share::SCN start_scn_;
  share::SCN clog_checkpoint_scn_; // may less than last_minor->end_log_ts
  share::SCN ddl_checkpoint_scn_;
  // snapshot_version of last minor
  int64_t snapshot_version_;
  int64_t multi_version_start_;
  ObTabletHAStatus ha_status_;
  ObTabletReportStatus report_status_; // alignment: 8B, size: 32B
  ObTabletTableStoreFlag table_store_flag_;
  share::SCN ddl_start_scn_;
  int64_t ddl_snapshot_version_;
  // max_sync_storage_schema_version_ = MIN(serialized_schema_version, sync_schema_version)
  // serialized_schema_version > sync_schema_version when major update storage schema
  // sync_schema_version > serialized_schema_version when replay schema clog but not mini merge yet
  // max_sync_storage_schema_version will be inaccurate after 4.2
  int64_t max_sync_storage_schema_version_;
  int64_t ddl_execution_id_;
  int64_t ddl_data_format_version_;
  int64_t max_serialized_medium_scn_; // abandon after 4.2
  share::SCN ddl_commit_scn_;
  share::SCN mds_checkpoint_scn_;
  ObTabletTransferInfo transfer_info_; // alignment: 8B, size: 32B
  int64_t create_schema_version_; // add after 4.2, record schema_version when first create tablet. NEED COMPAT
  //ATTENTION : Add a new variable need consider ObMigrationTabletParam
  // and tablet meta init interface for migration.
  // yuque :
  lib::Worker::CompatMode compat_mode_; // alignment: 1B, size: 4B
  bool has_next_tablet_;

private:
  int inner_check_(
      const ObTabletMeta &old_tablet_meta,
      const ObMigrationTabletParam *tablet_meta);
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
               K_(table_store_flag),
               K_(max_sync_storage_schema_version),
               K_(ddl_execution_id),
               K_(ddl_data_format_version),
               K_(max_serialized_medium_scn),
               K_(ddl_commit_scn),
               K_(mds_checkpoint_scn),
               K_(mds_data),
               K_(transfer_info),
               K_(create_schema_version));
private:
  int deserialize_v2(const char *buf, const int64_t len, int64_t &pos);
  int deserialize_v1(const char *buf, const int64_t len, int64_t &pos);

  // magic_number_ is added to support upgrade from old format(without version and length compatibility)
  // The old format first member is ls_id_(also 8 bytes long), which is not possible be a negative number.
  const static int64_t MAGIC_NUM = -20230111;
  const static int64_t PARAM_VERSION = 1;
  const static int64_t PARAM_VERSION_V2 = 2;

public:
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

  // Add new serialization member before this line, below members won't serialize
  common::ObArenaAllocator allocator_; // for storage schema
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_TABLET_META

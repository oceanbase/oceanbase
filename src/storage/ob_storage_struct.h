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

#ifndef SRC_STORAGE_OB_STORAGE_STRUCT_H_
#define SRC_STORAGE_OB_STORAGE_STRUCT_H_

#include "blocksstable/ob_block_sstable_struct.h"
#include "lib/ob_replica_define.h"
#include "common/ob_store_range.h"
#include "common/ob_member_list.h"
#include "share/scn.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/high_availability/ob_tablet_ha_status.h"

namespace oceanbase
{

namespace transaction
{
class ObLSTxCtxMgr;
}

namespace storage
{
class ObStorageSchema;
struct ObMigrationTabletParam;

typedef common::ObSEArray<common::ObStoreRowkey, common::OB_DEFAULT_MULTI_GET_ROWKEY_NUM> GetRowkeyArray;
typedef common::ObSEArray<common::ObStoreRange, common::OB_DEFAULT_MULTI_GET_ROWKEY_NUM> ScanRangeArray;

static const int64_t EXIST_READ_SNAPSHOT_VERSION = share::OB_MAX_SCN_TS_NS - 1;
static const int64_t MERGE_READ_SNAPSHOT_VERSION = share::OB_MAX_SCN_TS_NS - 2;
// static const int64_t MV_LEFT_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 3;
// static const int64_t MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 4;
// static const int64_t MV_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 5;
// static const int64_t BUILD_INDEX_READ_SNAPSHOT_VERSION = INT64_MAX - 6;
// static const int64_t WARM_UP_READ_SNAPSHOT_VERSION = INT64_MAX - 7;
static const int64_t GET_BATCH_ROWS_READ_SNAPSHOT_VERSION = share::OB_MAX_SCN_TS_NS - 8;
// static const int64_t GET_SCAN_COST_READ_SNAPSHOT_VERSION = INT64_MAX - 9;

#ifdef ERRSIM
struct ObErrsimBackfillPointType final
{
  OB_UNIS_VERSION(1);
public:
  enum TYPE
  {
    ERRSIM_POINT_NONE = 0,
    ERRSIM_START_BACKFILL_BEFORE = 1,
    ERRSIM_REPLACE_SWAP_BEFORE = 2,
    ERRSIM_REPLACE_AFTER = 3,
    ERRSIM_MODULE_MAX
  };
  ObErrsimBackfillPointType() : type_(ERRSIM_POINT_NONE) {}
  explicit ObErrsimBackfillPointType(const ObErrsimBackfillPointType::TYPE &type) : type_(type) {}
  ~ObErrsimBackfillPointType() = default;
  void reset();
  bool is_valid() const;
  bool operator == (const ObErrsimBackfillPointType &other) const;
  int hash(uint64_t &hash_val) const;
  int64_t hash() const;
  TO_STRING_KV(K_(type));
  TYPE type_;
};

class ObErrsimTransferBackfillPoint final
{
public:
  ObErrsimTransferBackfillPoint();
  virtual ~ObErrsimTransferBackfillPoint();
  bool is_valid() const;
  void reset();
  int set_point_type(const ObErrsimBackfillPointType &point_type);
  int set_point_start_time(int64_t start_time);
  bool is_errsim_point(const ObErrsimBackfillPointType &point_type) const;
  int64_t get_point_start_time() { return point_start_time_; }
  TO_STRING_KV(K_(point_type), K_(point_start_time));
private:
  ObErrsimBackfillPointType point_type_;
  int64_t point_start_time_;
};
#endif

enum ObMigrateStatus
{
  OB_MIGRATE_STATUS_NONE = 0,
  OB_MIGRATE_STATUS_ADD = 1,
  OB_MIGRATE_STATUS_ADD_FAIL = 2,
  OB_MIGRATE_STATUS_MIGRATE = 3,
  OB_MIGRATE_STATUS_MIGRATE_FAIL = 4,
  OB_MIGRATE_STATUS_REBUILD = 5,
//  OB_MIGRATE_STATUS_REBUILD_FAIL = 6, not used yet
  OB_MIGRATE_STATUS_CHANGE = 7,
  OB_MIGRATE_STATUS_RESTORE = 8,
  OB_MIGRATE_STATUS_RESTORE_FAIL = 9,
  OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX = 10,
  OB_MIGRATE_STATUS_COPY_LOCAL_INDEX = 11,
  OB_MIGRATE_STATUS_HOLD = 12,
  OB_MIGRATE_STATUS_RESTORE_FOLLOWER = 13,
  OB_MIGRATE_STATUS_RESTORE_STANDBY = 14,
  OB_MIGRATE_STATUS_RECREATED = 15,
  OB_MIGRATE_STATUS_LINK_MAJOR = 16,
  OB_MIGRATE_STATUS_MAX,
};

inline bool is_migrate_status_in_service(const ObMigrateStatus migrate_status)
{
  return OB_MIGRATE_STATUS_NONE == migrate_status
      ||  OB_MIGRATE_STATUS_REBUILD == migrate_status
      ||  OB_MIGRATE_STATUS_CHANGE == migrate_status
      ||  OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX == migrate_status
      ||  OB_MIGRATE_STATUS_COPY_LOCAL_INDEX == migrate_status;
}

enum ObReplicaOpType
{
  ADD_REPLICA_OP = 1,
  MIGRATE_REPLICA_OP = 2,
  REBUILD_REPLICA_OP = 3,
  CHANGE_REPLICA_OP = 4,
  REMOVE_REPLICA_OP = 5,
  RESTORE_REPLICA_OP = 6,
  COPY_GLOBAL_INDEX_OP = 7,
  COPY_LOCAL_INDEX_OP = 8,
  RESTORE_FOLLOWER_REPLICA_OP = 9,
  BACKUP_REPLICA_OP = 10,
  RESTORE_STANDBY_OP = 11,
  VALIDATE_BACKUP_OP = 12,
  FAST_MIGRATE_REPLICA_OP = 13,
  LINK_SHARE_MAJOR_OP = 14, //share major only for read-only replica in ofs-mode.
  BACKUP_BACKUPSET_OP = 15,
  BACKUP_ARCHIVELOG_OP = 16,
  UNKNOWN_REPLICA_OP,
};

inline bool is_replica_op_valid(const ObReplicaOpType replica_op)
{
  return replica_op >= ADD_REPLICA_OP && replica_op < UNKNOWN_REPLICA_OP;
}

inline bool need_copy_split_state(const ObReplicaOpType replica_op)
{
  return COPY_GLOBAL_INDEX_OP != replica_op && COPY_LOCAL_INDEX_OP != replica_op;
}

inline bool need_migrate_trans_table(const ObReplicaOpType replica_op)
{
  return REBUILD_REPLICA_OP == replica_op
      || CHANGE_REPLICA_OP == replica_op
      || ADD_REPLICA_OP == replica_op
      || MIGRATE_REPLICA_OP == replica_op
      || FAST_MIGRATE_REPLICA_OP == replica_op
      || RESTORE_REPLICA_OP == replica_op
      || RESTORE_FOLLOWER_REPLICA_OP == replica_op
      || RESTORE_STANDBY_OP == replica_op;
}

struct ObTabletReportStatus
{
  ObTabletReportStatus()
    : merge_snapshot_version_(0), cur_report_version_(0), data_checksum_(0), row_count_(0)
  {
  }
  ~ObTabletReportStatus() { };
  void reset()
  {
    merge_snapshot_version_ = 0;
    cur_report_version_ = 0;
    data_checksum_ = 0;
    row_count_ = 0;
  }
  bool need_report() const { return merge_snapshot_version_ > cur_report_version_; }
  TO_STRING_KV(K_(merge_snapshot_version), K_(cur_report_version), K_(data_checksum), K_(row_count));
  int64_t merge_snapshot_version_;
  int64_t cur_report_version_;
  int64_t data_checksum_;
  int64_t row_count_;
  OB_UNIS_VERSION(1);
};


struct ObReportStatus
{
  ObReportStatus()
    : data_version_(0), row_count_(0), row_checksum_(0), data_checksum_(0), data_size_(0),
      required_size_(0), snapshot_version_(0)
  {
  }
  TO_STRING_KV(K_(data_version), K_(row_count), K_(row_checksum),
      K_(data_checksum), K_(data_size), K_(required_size), K_(snapshot_version));
  void reset()
  {
    data_version_ = 0;
    row_count_ = 0;
    row_checksum_ = 0;
    data_checksum_ = 0;
    data_size_ = 0;
    required_size_ = 0;
    snapshot_version_ = 0;
  }
  int64_t data_version_;
  int64_t row_count_;
  int64_t row_checksum_;
  int64_t data_checksum_;
  int64_t data_size_;
  int64_t required_size_;
  int64_t snapshot_version_;
  OB_UNIS_VERSION(1);
};

struct ObPGReportStatus
{
  ObPGReportStatus() { reset(); }
  void reset()
  {
    data_version_ = 0;
    data_size_ = 0;
    required_size_ = 0;
    snapshot_version_ = 0;
  }
  TO_STRING_KV(K_(data_version), K_(data_size), K_(required_size),
    K_(snapshot_version));
  int64_t data_version_;
  int64_t data_size_;
  int64_t required_size_;
  int64_t snapshot_version_; //major frozen ts
  OB_UNIS_VERSION(1);
};

OB_INLINE bool is_valid_migrate_status(const ObMigrateStatus &status);

enum ObPartitionBarrierLogStateEnum
{
  BARRIER_LOG_INIT = 0,
  BARRIER_LOG_WRITTING,
  BARRIER_SOURCE_LOG_WRITTEN,
  BARRIER_DEST_LOG_WRITTEN
};

struct ObPartitionBarrierLogState final
{
public:
  ObPartitionBarrierLogState();
  ~ObPartitionBarrierLogState() = default;
  ObPartitionBarrierLogStateEnum &get_state() { return state_; }
  int64_t get_log_id() { return log_id_; }
  share::SCN get_scn() { return scn_; }
  int64_t get_schema_version() { return schema_version_; }
  void set_log_info(const ObPartitionBarrierLogStateEnum state, const int64_t log_id, const share::SCN &scn, const int64_t schema_version);
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(state));
private:
  ObPartitionBarrierLogStateEnum to_persistent_state() const;
private:
  ObPartitionBarrierLogStateEnum state_;
  int64_t log_id_;
  share::SCN scn_;
  int64_t schema_version_;
};

struct ObGetMergeTablesParam
{
  ObMergeType merge_type_;
  int64_t merge_version_;
  ObGetMergeTablesParam();
  bool is_valid() const;
  OB_INLINE bool is_major_valid() const
  {
    return storage::is_major_merge_type(merge_type_) && merge_version_ > 0;
  }
  TO_STRING_KV(K_(merge_type), K_(merge_version));
};

struct ObGetMergeTablesResult
{
  common::ObVersionRange version_range_;
  ObTablesHandleArray handle_;
  int64_t merge_version_;
  int64_t create_snapshot_version_;
  ObMergeType suggest_merge_type_;
  bool update_tablet_directly_;
  bool schedule_major_;
  share::ObScnRange scn_range_;
  int64_t read_base_version_;

  static const int64_t INVALID_INT_VALUE = -1;

  ObGetMergeTablesResult();
  bool is_valid() const;
  void reset_handle_and_range();
  void reset();
  int assign(const ObGetMergeTablesResult &src);
  int copy_basic_info(const ObGetMergeTablesResult &src);
  TO_STRING_KV(K_(version_range), K_(scn_range), K_(merge_version),
      K_(create_snapshot_version), K_(suggest_merge_type), K_(handle),
      K_(update_tablet_directly), K_(schedule_major), K_(read_base_version));
};

OB_INLINE bool is_valid_migrate_status(const ObMigrateStatus &status)
{
  return status >= OB_MIGRATE_STATUS_NONE && status < OB_MIGRATE_STATUS_MAX;
}

struct ObDDLTableStoreParam final
{
public:
  ObDDLTableStoreParam();
  ~ObDDLTableStoreParam() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(keep_old_ddl_sstable), K_(ddl_start_scn), K_(ddl_commit_scn), K_(ddl_checkpoint_scn),
      K_(ddl_snapshot_version), K_(ddl_execution_id), K_(data_format_version));
public:
  bool keep_old_ddl_sstable_;
  share::SCN ddl_start_scn_;
  share::SCN ddl_commit_scn_;
  share::SCN ddl_checkpoint_scn_;
  int64_t ddl_snapshot_version_;
  int64_t ddl_execution_id_;
  int64_t data_format_version_;
};

struct ObUpdateTableStoreParam
{
  ObUpdateTableStoreParam(
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq);
  ObUpdateTableStoreParam(
    const blocksstable::ObSSTable *sstable,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObStorageSchema *storage_schema,
    const int64_t rebuild_seq,
    const bool need_check_transfer_seq,
    const int64_t transfer_seq,
    const bool need_report = false,
    const share::SCN clog_checkpoint_scn = share::SCN::min_scn(),
    const bool need_check_sstable = false,
    const bool allow_duplicate_sstable = false,
    const ObMergeType merge_type = MERGE_TYPE_MAX);

  ObUpdateTableStoreParam( // for ddl merge task only
    const blocksstable::ObSSTable *sstable,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const int64_t rebuild_seq,
    const ObStorageSchema *storage_schema,
    const bool update_with_major_flag,
    const ObMergeType merge_type,
    const bool need_report);

  bool is_valid() const;
  TO_STRING_KV(KP_(sstable), K_(snapshot_version), K_(clog_checkpoint_scn), K_(multi_version_start),
               K_(need_report), KPC_(storage_schema), K_(rebuild_seq), K_(update_with_major_flag),
               K_(need_check_sstable), K_(ddl_info), K_(allow_duplicate_sstable),
               "merge_type", merge_type_to_str(merge_type_),
               K_(need_check_transfer_seq), K_(transfer_seq));

  const blocksstable::ObSSTable *sstable_;
  int64_t snapshot_version_;
  share::SCN clog_checkpoint_scn_;
  int64_t multi_version_start_;
  bool need_report_;
  const ObStorageSchema *storage_schema_;
  int64_t rebuild_seq_;
  bool update_with_major_flag_;
  bool need_check_sstable_;
  ObDDLTableStoreParam ddl_info_;
  bool allow_duplicate_sstable_;
  bool need_check_transfer_seq_;
  int64_t transfer_seq_;
  ObMergeType merge_type_; // set merge_type only when update tablet in compaction
};

struct ObBatchUpdateTableStoreParam final
{
  ObBatchUpdateTableStoreParam();
  ~ObBatchUpdateTableStoreParam() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObBatchUpdateTableStoreParam &param);
  int get_max_clog_checkpoint_scn(share::SCN &clog_checkpoint_scn) const;

  TO_STRING_KV(K_(tables_handle), K_(rebuild_seq), K_(update_logical_minor_sstable), K_(is_transfer_replace),
      K_(start_scn), KP_(tablet_meta), K_(update_ddl_sstable), K_(restore_status));

  ObTablesHandleArray tables_handle_;
#ifdef ERRSIM
  ObErrsimTransferBackfillPoint errsim_point_info_;
#endif
  int64_t rebuild_seq_;
  bool update_logical_minor_sstable_;
  bool is_transfer_replace_;
  share::SCN start_scn_;
  const ObMigrationTabletParam *tablet_meta_;
  bool update_ddl_sstable_;
  ObTabletRestoreStatus::STATUS restore_status_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchUpdateTableStoreParam);
};

struct ObPartitionReadableInfo
{
  int64_t min_log_service_ts_;
  int64_t min_trans_service_ts_;
  int64_t min_replay_engine_ts_;

  int64_t generated_ts_;
  int64_t max_readable_ts_;
  bool force_;

  ObPartitionReadableInfo();
  ~ObPartitionReadableInfo();

  bool is_valid() const;
  void calc_readable_ts();
  void reset();

  TO_STRING_KV(K(min_log_service_ts_),
               K(min_trans_service_ts_),
               K(min_replay_engine_ts_),
               K(generated_ts_),
               K(max_readable_ts_));
};

struct ObCreateSSTableParamExtraInfo
{
public:
  ObCreateSSTableParamExtraInfo()
    : column_default_checksum_(nullptr),
      column_cnt_(0)
  {
  }
  ~ObCreateSSTableParamExtraInfo() {}
  void reset()
  {
    column_default_checksum_ = nullptr;
    column_cnt_ = 0;
  }
  int assign(const ObCreateSSTableParamExtraInfo &extra_info);

  TO_STRING_KV(K_(column_default_checksum), K_(column_cnt));

  int64_t *column_default_checksum_;
  uint64_t column_cnt_;
};

struct ObTransTableStatus
{
public:
  ObTransTableStatus()
    : end_log_ts_(0),
      row_count_(0)
      {
      }
  int64_t end_log_ts_;
  int64_t row_count_;
};

struct ObMigrateRemoteTableInfo
{
  ObMigrateRemoteTableInfo() { reset(); }
  void reset()
  {
    remote_min_major_version_ = INT64_MAX;
    remote_min_start_log_ts_ = INT64_MAX;
    remote_min_base_version_ = INT64_MAX;
    remote_max_end_log_ts_ = 0;
    remote_max_snapshot_version_ = 0;
    need_reuse_local_minor_ = true;
    meta_merge_end_log_ts_ = 0;
  }
  bool has_major() const { return remote_min_major_version_ != INT64_MAX; }
  int64_t remote_min_major_version_;
  int64_t remote_min_start_log_ts_;
  int64_t remote_min_base_version_;
  int64_t remote_max_end_log_ts_;
  int64_t remote_max_snapshot_version_;
  bool need_reuse_local_minor_;
  bool meta_merge_end_log_ts_;
  TO_STRING_KV(
      K_(remote_min_major_version),
      K_(remote_min_start_log_ts),
      K_(remote_min_base_version),
      K_(remote_max_end_log_ts),
      K_(remote_max_snapshot_version),
      K_(need_reuse_local_minor),
      K_(meta_merge_end_log_ts));
};

class ObRebuildListener
{
public:
  // the upper layer need guarantee the life cycle of the
  // partition ctx mgr pointer should be safe before destruction
  ObRebuildListener(transaction::ObLSTxCtxMgr &mgr);
  ~ObRebuildListener();
  // whether the partition is in rebuild
  bool on_partition_rebuild();
private:
  transaction::ObLSTxCtxMgr& ls_tx_ctx_mgr_;
};


class ObBackupRestoreTableSchemaChecker
{
public:
  static int check_backup_restore_need_skip_table(
      const share::schema::ObTableSchema *table_schema,
      bool &need_skip,
      const bool is_restore_point = false);
};


class ObRestoreFakeMemberListHelper
{
public:
  static int fake_restore_member_list(
      const int64_t replica_cnt,
      common::ObMemberList &fake_member_list);
};

}//storage
}//oceanbase


#endif /* SRC_STORAGE_OB_STORAGE_STRUCT_H_ */

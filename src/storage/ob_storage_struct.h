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
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_table.h"
#include "storage/ob_partition_split.h"
#include "storage/ob_saved_storage_info_v2.h"

namespace oceanbase {
namespace obrpc {
struct ObCreatePartitionArg;
}

namespace blocksstable {
class ObStorageFileHandle;
}

namespace transaction {
class ObPartitionTransCtxMgr;
}

namespace storage {
class ObBaseFileMgr;
typedef common::ObSEArray<common::ObExtStoreRowkey, common::OB_DEFAULT_MULTI_GET_ROWKEY_NUM> GetRowkeyArray;
typedef common::ObSEArray<common::ObExtStoreRange, common::OB_DEFAULT_MULTI_GET_ROWKEY_NUM> ScanRangeArray;

static const int64_t EXIST_READ_SNAPSHOT_VERSION = INT64_MAX - 1;
static const int64_t MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 2;
static const int64_t MV_LEFT_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 3;
static const int64_t MV_RIGHT_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 4;
static const int64_t MV_MERGE_READ_SNAPSHOT_VERSION = INT64_MAX - 5;
static const int64_t BUILD_INDEX_READ_SNAPSHOT_VERSION = INT64_MAX - 6;
static const int64_t WARM_UP_READ_SNAPSHOT_VERSION = INT64_MAX - 7;
static const int64_t GET_BATCH_ROWS_READ_SNAPSHOT_VERSION = INT64_MAX - 8;
static const int64_t GET_SCAN_COST_READ_SNAPSHOT_VERSION = INT64_MAX - 9;

enum ObMigrateStatus {
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
  OB_MIGRATE_STATUS_LINK_MAJOR = 15,
  OB_MIGRATE_STATUS_MAX,
};

enum ObReplicaOpType {
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
  LINK_SHARE_MAJOR_OP = 14,  // share major only for read-only replica in ofs-mode.
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
  return REBUILD_REPLICA_OP == replica_op || CHANGE_REPLICA_OP == replica_op || ADD_REPLICA_OP == replica_op ||
         MIGRATE_REPLICA_OP == replica_op || FAST_MIGRATE_REPLICA_OP == replica_op ||
         RESTORE_REPLICA_OP == replica_op || RESTORE_FOLLOWER_REPLICA_OP == replica_op ||
         RESTORE_STANDBY_OP == replica_op;
}

struct ObMigrateStatusHelper {
public:
  static int trans_replica_op(const ObReplicaOpType& op_type, ObMigrateStatus& migrate_status);
  static int trans_fail_status(const ObMigrateStatus& cur_status, ObMigrateStatus& fail_status);
  static int trans_reboot_status(const ObMigrateStatus& cur_status, ObMigrateStatus& reboot_status);
  static OB_INLINE bool check_can_election(const ObMigrateStatus& cur_status);
  static OB_INLINE bool check_allow_gc(const ObMigrateStatus& cur_status);
  static OB_INLINE bool check_can_migrate_out(const ObMigrateStatus& cur_status);
};

struct ObReportStatus {
  ObReportStatus()
      : data_version_(0),
        row_count_(0),
        row_checksum_(0),
        data_checksum_(0),
        data_size_(0),
        required_size_(0),
        snapshot_version_(0)
  {}
  TO_STRING_KV(K_(data_version), K_(row_count), K_(row_checksum), K_(data_checksum), K_(data_size), K_(required_size),
      K_(snapshot_version));
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

struct ObPGReportStatus {
  ObPGReportStatus()
  {
    reset();
  }
  void reset()
  {
    data_version_ = 0;
    data_size_ = 0;
    required_size_ = 0;
    snapshot_version_ = 0;
  }
  TO_STRING_KV(K_(data_version), K_(data_size), K_(required_size), K_(snapshot_version));
  int64_t data_version_;
  int64_t data_size_;
  int64_t required_size_;
  int64_t snapshot_version_;  // major frozen ts
  OB_UNIS_VERSION(1);
};

OB_INLINE bool is_valid_migrate_status(const ObMigrateStatus& status);

// not used anymore
struct ObPartitionStoreMeta {
  static const int64_t PARTITION_STORE_META_VERSION = 1;
  common::ObPartitionKey pkey_;
  int16_t is_restore_;
  ObMigrateStatus migrate_status_;
  int64_t migrate_timestamp_;  // not used anymore
  common::ObReplicaType replica_type_;
  int64_t saved_split_state_;
  ObSavedStorageInfoV2 storage_info_;

  ObReportStatus report_status_;
  int64_t multi_version_start_;
  uint64_t data_table_id_;
  ObPartitionSplitInfo split_info_;
  common::ObReplicaProperty replica_property_;
  int64_t create_timestamp_;

  ObPartitionStoreMeta();
  virtual ~ObPartitionStoreMeta();
  bool is_valid() const;
  bool inner_is_valid() const;
  void reset();
  int deep_copy(const ObPartitionStoreMeta& meta);

  TO_STRING_KV(K_(pkey), K_(is_restore), K_(replica_type), K_(saved_split_state), K_(migrate_status),
      K_(migrate_timestamp), K_(storage_info), K_(report_status), K_(multi_version_start), K_(data_table_id),
      K_(split_info), K_(replica_property));
  OB_UNIS_VERSION_V(PARTITION_STORE_META_VERSION);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionStoreMeta);
};

struct ObPGPartitionStoreMeta {
  static const int64_t PARTITION_STORE_META_VERSION_V2 = 2;
  common::ObPartitionKey pkey_;
  ObReportStatus report_status_;  // could be reset during backup and restore
  int64_t multi_version_start_;
  uint64_t data_table_id_;
  int64_t create_schema_version_;
  ObReplicaType replica_type_;  // TODO remove it later
  int64_t create_timestamp_;

  ObPGPartitionStoreMeta();
  virtual ~ObPGPartitionStoreMeta();
  bool is_valid() const;
  void reset();
  int assign(const ObPGPartitionStoreMeta& meta)
  {
    return deep_copy(meta);
  }
  int deep_copy(const ObPGPartitionStoreMeta& meta);
  int copy_from_old_meta(const ObPartitionStoreMeta& old_meta);

  TO_STRING_KV(K_(pkey), K_(report_status), K_(multi_version_start), K_(data_table_id), K_(create_schema_version),
      K_(replica_type), K_(create_timestamp));
  OB_UNIS_VERSION(PARTITION_STORE_META_VERSION_V2);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGPartitionStoreMeta);
};

struct ObPartitionGroupMeta {
  static const int64_t PARTITION_GROUP_META_VERSION = 1;
  common::ObPGKey pg_key_;
  int16_t is_restore_;
  ObMigrateStatus migrate_status_;
  int64_t migrate_timestamp_;  // not used anymore
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;
  int64_t saved_split_state_;
  ObSavedStorageInfoV2 storage_info_;

  ObPGReportStatus report_status_;
  // uint64_t data_table_id_;
  ObPartitionSplitInfo split_info_;
  common::ObPartitionArray partitions_;

  int64_t create_schema_version_;
  // increment one on add partition to pg and remove partition to pg
  int64_t ddl_seq_num_;
  int64_t create_timestamp_;
  int64_t create_frozen_version_;
  uint64_t last_restore_log_id_;
  int64_t restore_snapshot_version_;

  ObPartitionGroupMeta();
  virtual ~ObPartitionGroupMeta();
  bool is_valid() const;
  void reset();
  int deep_copy(const ObPartitionGroupMeta& meta);
  int copy_from_store_meta(const ObPartitionStoreMeta& meta);

  //

  int get_recover_info_for_flashback(const int64_t snapshot_version, ObRecoverPoint& point);

  //

  int clear_recover_points_for_physical_flashback(const int64_t version);
  int64_t get_migrate_replay_log_ts() const;

  TO_STRING_KV(K_(pg_key), K_(is_restore), K_(replica_type), K_(replica_property), K_(saved_split_state),
      K_(migrate_status), K_(migrate_timestamp), K_(storage_info), K_(report_status), K_(create_schema_version),
      K_(split_info), K_(partitions), K_(ddl_seq_num), K_(create_timestamp), K_(create_frozen_version),
      K_(last_restore_log_id), K_(restore_snapshot_version));

  OB_UNIS_VERSION_V(PARTITION_GROUP_META_VERSION);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupMeta);
};

struct ObGetMergeTablesParam {
  ObMergeType merge_type_;
  uint64_t index_id_;
  common::ObVersion merge_version_;
  int64_t trans_table_end_log_ts_;
  int64_t trans_table_timestamp_;

  ObGetMergeTablesParam();
  bool is_valid() const;
  OB_INLINE bool is_major_merge() const
  {
    return MAJOR_MERGE == merge_type_;
  }
  OB_INLINE bool is_major_valid() const
  {
    return is_major_merge() && OB_INVALID_ID != index_id_ && merge_version_.major_ > 0 && merge_version_.minor_ == 0;
  }
  TO_STRING_KV(K_(merge_type), K_(index_id), K_(merge_version), K_(trans_table_end_log_ts), K_(trans_table_timestamp));
};

struct ObGetMergeTablesResult {
  common::ObVersionRange version_range_;
  ObTablesHandle handle_;
  ObTableHandle base_handle_;  // previous base table, minor merge is not set it now.
  common::ObVersion merge_version_;
  int64_t base_schema_version_;
  int64_t schema_version_;
  int64_t create_snapshot_version_;
  int64_t checksum_method_;
  ObMergeType suggest_merge_type_;
  bool create_sstable_for_large_snapshot_;
  common::ObLogTsRange log_ts_range_;
  int64_t dump_memtable_timestamp_;
  int64_t read_base_version_;

  ObGetMergeTablesResult();
  bool is_valid() const;
  void reset();
  int deep_copy(const ObGetMergeTablesResult& src);
  TO_STRING_KV(K_(version_range), K_(merge_version), K_(base_schema_version), K_(schema_version),
      K_(create_snapshot_version), K_(checksum_method), K_(suggest_merge_type), K_(handle), K_(base_handle),
      K_(create_sstable_for_large_snapshot), K_(log_ts_range), K_(dump_memtable_timestamp), K_(read_base_version));
};
OB_INLINE bool is_valid_migrate_status(const ObMigrateStatus& status)
{
  return status >= OB_MIGRATE_STATUS_NONE && status < OB_MIGRATE_STATUS_MAX;
}

struct AddTableParam {
  AddTableParam();
  bool is_valid() const;
  TO_STRING_KV(KP_(table), K_(max_kept_major_version_number), K_(multi_version_start), K_(in_slog_trans),
      K_(need_prewarm), K_(is_daily_merge), K_(backup_snapshot_version), KP_(complement_minor_sstable),
      K_(schema_version));

  storage::ObSSTable* table_;
  int64_t max_kept_major_version_number_;
  int64_t multi_version_start_;
  bool in_slog_trans_;
  bool need_prewarm_;
  bool is_daily_merge_;
  storage::ObSSTable* complement_minor_sstable_;
  int64_t backup_snapshot_version_;
  int64_t schema_version_;
};

struct ObPartitionReadableInfo {
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

  TO_STRING_KV(K(min_log_service_ts_), K(min_trans_service_ts_), K(min_replay_engine_ts_), K(generated_ts_),
      K(max_readable_ts_));
};

struct ObCreatePGParam {
public:
  ObCreatePGParam();
  virtual ~ObCreatePGParam()
  {}
  bool is_valid() const;
  int assign(const ObCreatePGParam& param);
  void reset();
  int set_storage_info(const ObSavedStorageInfoV2& info);
  int set_split_info(const ObPartitionSplitInfo& split_info);
  TO_STRING_KV(K_(info), K_(is_restore), K_(replica_type), K_(replica_property), K_(data_version), K_(write_slog),
      K_(split_info), K_(split_state), K_(create_frozen_version), K_(last_restore_log_id), K_(restore_snapshot_version),
      K_(migrate_status));

  ObSavedStorageInfoV2 info_;
  int64_t is_restore_;  // ObReplicaRestoreStatus
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;
  int64_t data_version_;
  bool write_slog_;
  ObPartitionSplitInfo split_info_;
  int64_t split_state_;
  int64_t create_timestamp_;
  blocksstable::ObStorageFileHandle* file_handle_;
  ObBaseFileMgr* file_mgr_;
  int64_t create_frozen_version_;
  uint64_t last_restore_log_id_;
  int64_t restore_snapshot_version_;
  ObMigrateStatus migrate_status_;
  DISALLOW_COPY_AND_ASSIGN(ObCreatePGParam);
};

bool ObMigrateStatusHelper::check_can_election(const ObMigrateStatus& cur_status)
{
  bool can_election = true;

  if (OB_MIGRATE_STATUS_ADD == cur_status || OB_MIGRATE_STATUS_ADD_FAIL == cur_status ||
      OB_MIGRATE_STATUS_MIGRATE == cur_status || OB_MIGRATE_STATUS_MIGRATE_FAIL == cur_status) {
    can_election = false;
  }

  return can_election;
}

bool ObMigrateStatusHelper::check_allow_gc(const ObMigrateStatus& cur_status)
{
  bool allow_gc = true;

  if (OB_MIGRATE_STATUS_ADD == cur_status || OB_MIGRATE_STATUS_MIGRATE == cur_status ||
      OB_MIGRATE_STATUS_REBUILD == cur_status || OB_MIGRATE_STATUS_CHANGE == cur_status ||
      OB_MIGRATE_STATUS_RESTORE == cur_status || OB_MIGRATE_STATUS_COPY_GLOBAL_INDEX == cur_status ||
      OB_MIGRATE_STATUS_COPY_LOCAL_INDEX == cur_status || OB_MIGRATE_STATUS_HOLD == cur_status ||
      OB_MIGRATE_STATUS_RESTORE_FOLLOWER == cur_status || OB_MIGRATE_STATUS_RESTORE_STANDBY == cur_status) {
    allow_gc = false;
  }

  return allow_gc;
}

bool ObMigrateStatusHelper::check_can_migrate_out(const ObMigrateStatus& cur_status)
{
  bool can_migrate_out = true;
  if (OB_MIGRATE_STATUS_NONE != cur_status) {
    can_migrate_out = false;
  }
  return can_migrate_out;
}

class ObCreatePartitionMeta {

public:
  ObCreatePartitionMeta()
  {
    reset();
  }
  ~ObCreatePartitionMeta()
  {
    reset();
  }
  void reset();
  bool is_valid() const
  {
    return is_valid_;
  }
  int extract_from(const share::schema::ObTableSchema& table_schema);
  int replace_tenant_id(const uint64_t tenant_id);
  int assign(const ObCreatePartitionMeta& other);
  const common::ObIArray<share::schema::ObColDesc>& get_store_column_ids() const
  {
    return column_ids_;
  }
  //  inline int64_t get_column_count() const { return column_cnt_; }
  inline int64_t get_rowkey_column_num() const
  {
    return rowkey_column_num_;
  }
  int64_t get_progressive_merge_round() const
  {
    return progressive_merge_round_;
  }
  bool is_global_index_table() const
  {
    return is_global_index_table_;
  }
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline uint64_t get_data_table_id() const
  {
    return data_table_id_;
  }
  inline share::schema::ObTableType get_table_type() const
  {
    return table_type_;
  }
  inline share::schema::ObIndexType get_index_type() const
  {
    return index_type_;
  }
  inline share::schema::ObTableMode get_table_mode() const
  {
    return table_mode_;
  }
  bool is_storage_index_table() const
  {
    return share::schema::ObTableSchema::is_storage_index_table(table_type_);
  }
  const share::schema::ObColumnSchemaV2* get_column_schema(const uint64_t column_id) const;
  const share::schema::IdHashArray* get_id_hash_array() const
  {
    return id_hash_array_;
  };
  TO_STRING_KV(K_(is_valid), K_(table_id), K_(data_table_id), K_(progressive_merge_round), K_(is_global_index_table),
      K_(table_type), K_(table_mode), K_(index_type), K_(rowkey_column_num), K_(column_ids), KP(id_hash_array_));

private:
  //!!!! attention:only after successfully extracting from a ObTableSchema instance, can is_valid_ be true

  bool is_valid_;
  uint64_t table_id_;
  uint64_t data_table_id_;
  int64_t progressive_merge_round_;
  bool is_global_index_table_;
  share::schema::ObTableType table_type_;
  share::schema::ObTableMode table_mode_;
  share::schema::ObIndexType index_type_;
  int64_t rowkey_column_num_;
  common::ObSArray<share::schema::ObColDesc> column_ids_;
  const share::schema::IdHashArray* id_hash_array_;
};

class ObCreatePartitionParam {

public:
  ObCreatePartitionParam()
  {
    reset();
  }
  ~ObCreatePartitionParam()
  {
    reset();
  }
  void reset();
  int extract_from(const obrpc::ObCreatePartitionArg& arg);
  int replace_tenant_id(const uint64_t tenant_id);
  bool need_create_sstable() const
  {
    return need_create_sstable_;
  }
  //!!!! attention:only after successfully extracting from a ObCreatePartitionArg instance, can is_valid_ be true
  bool is_valid() const
  {
    return is_valid_;
  }
  TO_STRING_KV(K_(is_valid), K_(member_list), K_(partition_key), K_(need_create_sstable), K_(schema_version),
      K_(memstore_version), K_(lease_start), K_(replica_type), K_(restore), K_(frozen_timestamp), K_(pg_key),
      K_(schemas));

public:
  //!!!! attention:only after successfully extracting from a ObTableSchema instance, can is_valid_ be true

  bool is_valid_;
  common::ObMemberList member_list_;
  common::ObPartitionKey partition_key_;
  bool need_create_sstable_;
  int64_t schema_version_;
  int64_t memstore_version_;
  int64_t lease_start_;
  common::ObReplicaType replica_type_;
  int64_t restore_;
  int64_t frozen_timestamp_;
  common::ObPGKey pg_key_;
  common::ObSArray<ObCreatePartitionMeta> schemas_;
};

struct ObTransTableStatus {
public:
  ObTransTableStatus() : end_log_ts_(0), row_count_(0)
  {}
  int64_t end_log_ts_;
  int64_t row_count_;
};

struct ObMigrateRemoteTableInfo {
  ObMigrateRemoteTableInfo()
  {
    reset();
  }
  void reset()
  {
    remote_min_major_version_ = INT64_MAX;
    remote_min_start_log_ts_ = INT64_MAX;
    remote_min_base_version_ = INT64_MAX;
    remote_max_end_log_ts_ = 0;
    remote_max_snapshot_version_ = 0;
    need_reuse_local_minor_ = true;
    buffer_minor_end_log_ts_ = 0;
  }
  bool has_major() const
  {
    return remote_min_major_version_ != INT64_MAX;
  }
  int64_t remote_min_major_version_;
  int64_t remote_min_start_log_ts_;
  int64_t remote_min_base_version_;
  int64_t remote_max_end_log_ts_;
  int64_t remote_max_snapshot_version_;
  bool need_reuse_local_minor_;
  bool buffer_minor_end_log_ts_;
  TO_STRING_KV(K_(remote_min_major_version), K_(remote_min_start_log_ts), K_(remote_min_base_version),
      K_(remote_max_end_log_ts), K_(remote_max_snapshot_version), K_(need_reuse_local_minor),
      K_(buffer_minor_end_log_ts));
};

class ObRecoveryPointSchemaFilter final {
public:
  ObRecoveryPointSchemaFilter();
  virtual ~ObRecoveryPointSchemaFilter();
  bool is_inited() const;
  int init(const int64_t tenant_id, const int64_t tenant_recovery_point_schema_version,
      const int64_t tenant_current_schema_version);
  // check pg/partition exist
  int check_partition_exist(const common::ObPartitionKey pkey, bool& is_exist);
  int check_table_exist(const uint64_t table_id, bool& is_exist);
  int do_filter_tables(common::ObIArray<uint64_t>& table_ids);
  int do_filter_pg_partitions(const common::ObPartitionKey& pg_key, common::ObPartitionArray& partitions);
  int check_if_table_miss_by_schema(
      const common::ObPartitionKey& pgkey, const common::hash::ObHashSet<uint64_t>& table_ids);

  TO_STRING_KV(
      K_(is_inited), K_(tenant_id), K_(tenant_recovery_point_schema_version), K_(tenant_current_schema_version));

private:
  int get_table_ids_in_pg_(const common::ObPartitionKey& pgkey, common::ObIArray<uint64_t>& table_ids,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_partition_exist_(const common::ObPartitionKey pkey, const bool check_dropped_partition,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& is_exist);
  int check_table_exist_(const uint64_t table_id, share::schema::ObSchemaGetterGuard& schema_guard, bool& is_exist);
  int check_if_table_miss_by_schema_(const common::ObPartitionKey& pgkey,
      const common::hash::ObHashSet<uint64_t>& table_ids, share::schema::ObSchemaGetterGuard& schema_guard);

private:
  bool is_inited_;
  int64_t tenant_id_;
  int64_t tenant_recovery_point_schema_version_;
  int64_t tenant_current_schema_version_;
  share::schema::ObSchemaGetterGuard recovery_point_schema_guard_;
  share::schema::ObSchemaGetterGuard current_schema_guard_;
  bool is_schema_version_same_;
  DISALLOW_COPY_AND_ASSIGN(ObRecoveryPointSchemaFilter);
};

class ObBackupRestoreTableSchemaChecker {
public:
  static int check_backup_restore_need_skip_table(const share::schema::ObTableSchema* table_schema, bool& need_skip);
};

class ObRebuildListener {
public:
  // the upper layer need guarantee the life cycle of the
  // partition ctx mgr pointer should be safe before destruction
  ObRebuildListener(transaction::ObPartitionTransCtxMgr& mgr);
  ~ObRebuildListener();
  // whether the partition is in rebuild
  bool on_partition_rebuild();

private:
  transaction::ObPartitionTransCtxMgr& ob_partition_ctx_mgr_;
};

class ObRestoreFakeMemberListHelper {
public:
  static int fake_restore_member_list(const int64_t replica_cnt, common::ObMemberList& fake_member_list);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_STORAGE_STRUCT_H_ */

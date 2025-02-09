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

#ifndef OCEANBASE_SHARE_OB_DDL_COMMON_H
#define OCEANBASE_SHARE_OB_DDL_COMMON_H

#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_service.h"
#include "share/location_cache/ob_location_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/tablet/ob_tablet_common.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
struct ObPartitionSplitArg;
struct ObAlterTableArg;
struct ObDropDatabaseArg;
struct ObDropTableArg;
struct ObDropIndexArg;
struct ObRebuildIndexArg;
struct ObTruncateTableArg;
struct ObCreateIndexArg;
struct ObIndexArg;
}
namespace sql
{
class ObPhysicalPlan;
class ObOpSpec;
}
namespace blocksstable
{
  class MacroBlockId;
}
namespace storage
{
class ObTabletHandle;
class ObLSHandle;
struct ObStorageColumnGroupSchema;
class ObCOSSTableV2;
}
namespace rootserver
{
class ObDDLTask;
class ObDDLWaitTransEndCtx;
}
namespace share
{
class ObLocationService;
enum ObDDLType
{
  DDL_INVALID = 0,

  ///< @note add new normal long running ddl type before this line
  DDL_CHECK_CONSTRAINT = 1,
  DDL_FOREIGN_KEY_CONSTRAINT = 2,
  DDL_ADD_NOT_NULL_COLUMN = 3,
  DDL_MODIFY_AUTO_INCREMENT = 4,
  DDL_CREATE_INDEX = 5,
  DDL_DROP_INDEX = 6,
  DDL_CREATE_FTS_INDEX = 7,
  DDL_CREATE_MLOG = 8,
  DDL_DROP_MLOG = 9,
  DDL_CREATE_PARTITIONED_LOCAL_INDEX = 10,
  DDL_DROP_LOB = 11,
  DDL_DROP_FTS_INDEX = 12,
  DDL_DROP_MULVALUE_INDEX = 13,
  DDL_DROP_VEC_INDEX = 14,
  DDL_CREATE_VEC_INDEX = 15,
  DDL_CREATE_MULTIVALUE_INDEX = 16,
  DDL_REBUILD_INDEX = 17,
  DDL_CREATE_VEC_IVFFLAT_INDEX = 18,
  DDL_CREATE_VEC_IVFSQ8_INDEX = 19,
  DDL_CREATE_VEC_IVFPQ_INDEX = 20,
  DDL_DROP_VEC_IVFFLAT_INDEX = 21,
  DDL_DROP_VEC_IVFSQ8_INDEX = 22,
  DDL_DROP_VEC_IVFPQ_INDEX = 23,

  ///< @note tablet split.
  DDL_AUTO_SPLIT_BY_RANGE = 100,
  DDL_AUTO_SPLIT_NON_RANGE = 101,
  DDL_MANUAL_SPLIT_BY_RANGE = 102,
  DDL_MANUAL_SPLIT_NON_RANGE = 103,
  ///< @note Drop schema, and refuse concurrent trans.  
  DDL_DROP_SCHEMA_AVOID_CONCURRENT_TRANS = 500,
  DDL_DROP_DATABASE = 501,
  DDL_DROP_TABLE = 502,
  DDL_TRUNCATE_TABLE = 503,
  DDL_DROP_PARTITION = 504,
  DDL_DROP_SUB_PARTITION = 505,
  DDL_TRUNCATE_PARTITION = 506,
  DDL_TRUNCATE_SUB_PARTITION = 507,
  DDL_RENAME_PARTITION = 508,
  DDL_RENAME_SUB_PARTITION = 509,
  ///< @note add new double table long running ddl type before this line  
  DDL_DOUBLE_TABLE_OFFLINE = 1000,
  DDL_MODIFY_COLUMN = 1001, // only modify columns
  DDL_ADD_PRIMARY_KEY = 1002,
  DDL_DROP_PRIMARY_KEY = 1003,
  DDL_ALTER_PRIMARY_KEY = 1004,
  DDL_ALTER_PARTITION_BY = 1005,
  DDL_DROP_COLUMN = 1006, // only drop columns
  DDL_CONVERT_TO_CHARACTER = 1007,
  DDL_ADD_COLUMN_OFFLINE = 1008, // only add columns
  DDL_COLUMN_REDEFINITION = 1009, // only add/drop columns
  DDL_TABLE_REDEFINITION = 1010,
  DDL_DIRECT_LOAD = 1011, // load data
  DDL_DIRECT_LOAD_INSERT = 1012, // insert into select
  DDL_TABLE_RESTORE = 1013, // table restore
  DDL_MVIEW_COMPLETE_REFRESH = 1014,
  DDL_CREATE_MVIEW = 1015,
  DDL_ALTER_COLUMN_GROUP = 1016, // alter table add/drop column group
  DDL_MODIFY_AUTO_INCREMENT_WITH_REDEFINITION = 1017,
  DDL_PARTITION_SPLIT_RECOVERY_TABLE_REDEFINITION = 1018,

  // @note new normal ddl type to be defined here !!!
  DDL_NORMAL_TYPE = 10001,
  DDL_ADD_COLUMN_ONLINE = 10002, // only add trailing columns
  DDL_CHANGE_COLUMN_NAME = 10003,
  DDL_DROP_COLUMN_INSTANT = 10004,
  DDL_ALTER_PARTITION_AUTO_SPLIT_ATTRIBUTE = 10005, // auto table auto partition // online
  DDL_ADD_COLUMN_INSTANT = 10006, // add after/before column
  DDL_COMPOUND_INSTANT = 10007,
  DDL_ALTER_COLUMN_GROUP_DELAYED = 10008,
  ///< @note add new normal ddl type before this line
  DDL_MAX
};
const char *get_ddl_type(ObDDLType ddl_type);

enum ObDDLTaskType
{
  INVALID_TASK = 0,
  REBUILD_INDEX_TASK = 1,
  REBUILD_CONSTRAINT_TASK = 2,
  REBUILD_FOREIGN_KEY_TASK = 3,
  MAKE_DDL_TAKE_EFFECT_TASK = 4,
  CLEANUP_GARBAGE_TASK = 5,
  MODIFY_FOREIGN_KEY_STATE_TASK = 6,
  // used in rollback_failed_add_not_null_columns() in ob_constraint_task.cpp.
  DELETE_COLUMN_FROM_SCHEMA = 7,
  // remap all index tables to hidden table and take effect through one rpc, applied in drop column for 4.0.
  REMAP_INDEXES_AND_TAKE_EFFECT_TASK = 8,
  UPDATE_AUTOINC_SCHEMA = 9,
  CANCEL_DDL_TASK = 10,
  MODIFY_NOT_NULL_COLUMN_STATE_TASK = 11,
  MAKE_RECOVER_RESTORE_TABLE_TASK_TAKE_EFFECT = 12,
  PARTITION_SPLIT_RECOVERY_TASK = 13,
  PARTITION_SPLIT_RECOVERY_CLEANUP_GARBAGE_TASK = 14,
  SWITCH_VEC_INDEX_NAME_TASK = 15,
};

enum ObDDLTaskStatus { // FARM COMPAT WHITELIST
  PREPARE = 0,
  OBTAIN_SNAPSHOT = 1,
  WAIT_TRANS_END = 2,
  REDEFINITION = 3,
  VALIDATE_CHECKSUM = 4,
  COPY_TABLE_DEPENDENT_OBJECTS = 5,
  TAKE_EFFECT = 6,
  CHECK_CONSTRAINT_VALID = 7,
  SET_CONSTRAINT_VALIDATE = 8,
  MODIFY_AUTOINC = 9,
  SET_WRITE_ONLY = 10, // disused, just for compatibility.
  WAIT_TRANS_END_FOR_WRITE_ONLY = 11,
  SET_UNUSABLE = 12,
  WAIT_TRANS_END_FOR_UNUSABLE = 13,
  DROP_SCHEMA = 14,
  CHECK_TABLE_EMPTY = 15,
  WAIT_CHILD_TASK_FINISH = 16,
  REPENDING = 17,
  START_REFRESH_MVIEW_TASK = 18,
  WAIT_FROZE_END = 19,
  WAIT_COMPACTION_END = 20,
  WAIT_DATA_TABLE_SPLIT_END = 21,
  WAIT_LOCAL_INDEX_SPLIT_END = 22,
  WAIT_LOB_TABLE_SPLIT_END = 23,
  WAIT_PARTITION_SPLIT_RECOVERY_TASK_FINISH = 24,
  GENERATE_ROWKEY_DOC_SCHEMA = 25,
  WAIT_ROWKEY_DOC_TABLE_COMPLEMENT = 26,
  GENERATE_DOC_AUX_SCHEMA = 27,
  WAIT_AUX_TABLE_COMPLEMENT = 28,
  GENERATE_ROWKEY_VID_SCHEMA = 29,
  WAIT_ROWKEY_VID_TABLE_COMPLEMENT = 30,
  GENERATE_VEC_AUX_SCHEMA = 31,
  WAIT_VEC_AUX_TABLE_COMPLEMENT = 32,
  GENERATE_VID_ROWKEY_SCHEMA = 33,
  WAIT_VID_ROWKEY_TABLE_COMPLEMENT = 34,
  REBUILD_SCHEMA = 35,
  SWITCH_INDEX_NAME = 36,
  WRITE_SPLIT_START_LOG = 37,
  DROP_AUX_INDEX_TABLE = 38,
  DROP_LOB_META_ROW = 39,
  GENERATE_SQ_META_TABLE_SCHEMA = 40,
  WAIT_SQ_META_TABLE_COMPLEMENT = 41,
  GENERATE_CENTROID_TABLE_SCHEMA = 42,
  WAIT_CENTROID_TABLE_COMPLEMENT = 43,
  GENERATE_PQ_CENTROID_TABLE_SCHEMA = 44,
  WAIT_PQ_CENTROID_TABLE_COMPLEMENT = 45,
  LOAD_DICTIONARY = 46,

  FAIL = 99,
  SUCCESS = 100
};

const char *const temp_store_format_options[] =
{
  "auto",
  "zstd",
  "lz4",
  "none",
};

enum SortCompactLevel
{
  SORT_DEFAULT_LEVEL = 0,
  SORT_COMPACT_LEVEL = 1,
  SORT_ENCODE_LEVEL = 2,
  SORT_COMPRESSION_LEVEL = 3,
  SORT_COMPRESSION_COMPACT_LEVEL = 4,
  SORT_COMPRESSION_ENCODE_LEVEL = 5
};

enum ObSplitSSTableType
{
  SPLIT_BOTH = 0, // Major and Minor
  SPLIT_MAJOR = 1,
  SPLIT_MINOR = 2
};

static const char* ddl_task_status_to_str(const ObDDLTaskStatus &task_status) {
  const char *str = nullptr;
  switch(task_status) {
    case share::ObDDLTaskStatus::PREPARE:
      str = "PREPARE";
      break;
    case share::ObDDLTaskStatus::OBTAIN_SNAPSHOT:
      str = "OBTAIN_SNAPSHOT";
      break;
    case share::ObDDLTaskStatus::WAIT_TRANS_END:
      str = "WAIT_TRANS_END";
      break;
    case share::ObDDLTaskStatus::REDEFINITION:
      str = "REDEFINITION";
      break;
    case share::ObDDLTaskStatus::VALIDATE_CHECKSUM:
      str = "VALIDATE_CHECKSUM";
      break;
    case share::ObDDLTaskStatus::COPY_TABLE_DEPENDENT_OBJECTS:
      str = "COPY_TABLE_DEPENDENT_OBJECTS";
      break;
    case share::ObDDLTaskStatus::TAKE_EFFECT:
      str = "TAKE_EFFECT";
      break;
    case share::ObDDLTaskStatus::CHECK_CONSTRAINT_VALID:
      str = "CHECK_CONSTRAINT_VALID";
      break;
    case share::ObDDLTaskStatus::SET_CONSTRAINT_VALIDATE:
      str = "SET_CONSTRAINT_VALIDATE";
      break;
    case share::ObDDLTaskStatus::MODIFY_AUTOINC:
      str = "MODIFY_AUTOINC";
      break;
    case share::ObDDLTaskStatus::SET_WRITE_ONLY:
      str = "SET_WRITE_ONLY";
      break;
    case share::ObDDLTaskStatus::WAIT_TRANS_END_FOR_WRITE_ONLY:
      str = "WAIT_TRANS_END_FOR_WRITE_ONLY";
      break;
    case share::ObDDLTaskStatus::SET_UNUSABLE:
      str = "SET_UNUSABLE";
      break;
    case share::ObDDLTaskStatus::WAIT_TRANS_END_FOR_UNUSABLE:
      str = "WAIT_TRANS_END_FOR_UNUSABLE";
      break;
    case share::ObDDLTaskStatus::DROP_SCHEMA:
      str = "DROP_SCHEMA";
      break;
    case ObDDLTaskStatus::CHECK_TABLE_EMPTY:
      str = "CHECK_TABLE_EMPTY";
      break;
    case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
      str = "WAIT_CHILD_TASK_FINISH";
      break;
    case ObDDLTaskStatus::REPENDING:
      str = "REPENDING";
      break;
    case ObDDLTaskStatus::START_REFRESH_MVIEW_TASK:
      str = "START_REFRESH_MVIEW_TASK";
      break;
    case ObDDLTaskStatus::WAIT_FROZE_END:
      str = "WAIT_FROZE_END";
      break;
    case ObDDLTaskStatus::WAIT_COMPACTION_END:
      str = "WAIT_COMPACTION_END";
      break;
    case ObDDLTaskStatus::WAIT_DATA_TABLE_SPLIT_END:
      str = "WAIT_DATA_TABLE_SPLIT_END";
      break;
    case ObDDLTaskStatus::WAIT_LOCAL_INDEX_SPLIT_END:
      str = "WAIT_LOCAL_INDEX_SPLIT_END";
      break;
    case ObDDLTaskStatus::WAIT_LOB_TABLE_SPLIT_END:
      str = "WAIT_LOB_TABLE_SPLIT_END";
      break;
    case ObDDLTaskStatus::WAIT_PARTITION_SPLIT_RECOVERY_TASK_FINISH:
      str = "WAIT_PARTITION_SPLIT_RECOVERY_TASK_FINISH";
      break;
    case ObDDLTaskStatus::GENERATE_ROWKEY_DOC_SCHEMA:
      str = "GENERATE_ROWKEY_DOC_SCHEMA";
      break;
    case ObDDLTaskStatus::LOAD_DICTIONARY:
      str = "LOAD_DICTIONARY";
      break;
    case ObDDLTaskStatus::GENERATE_DOC_AUX_SCHEMA:
      str = "GENERATE_DOC_AUX_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_ROWKEY_DOC_TABLE_COMPLEMENT:
      str = "WAIT_ROWKEY_DOC_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::WAIT_AUX_TABLE_COMPLEMENT:
      str = "WAIT_AUX_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::GENERATE_ROWKEY_VID_SCHEMA:
      str = "GENERATE_ROWKEY_VID_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_ROWKEY_VID_TABLE_COMPLEMENT:
      str = "WAIT_ROWKEY_VID_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::GENERATE_VEC_AUX_SCHEMA:
      str = "GENERATE_VEC_AUX_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_VEC_AUX_TABLE_COMPLEMENT:
      str = "WAIT_VEC_AUX_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::GENERATE_VID_ROWKEY_SCHEMA:
      str = "GENERATE_VID_ROWKEY_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_VID_ROWKEY_TABLE_COMPLEMENT:
      str = "WAIT_VID_ROWKEY_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::REBUILD_SCHEMA:
      str = "REBUILD_SCHEMA";
      break;
    case ObDDLTaskStatus::SWITCH_INDEX_NAME:
      str = "SWITCH_INDEX_NAME";
      break;
    case ObDDLTaskStatus::WRITE_SPLIT_START_LOG:
      str = "WRITE_SPLIT_START_LOG";
      break;
    case ObDDLTaskStatus::DROP_AUX_INDEX_TABLE:
      str = "DROP_AUX_INDEX_TABLE";
      break;
    case ObDDLTaskStatus::DROP_LOB_META_ROW:
      str = "DROP_LOB_META_ROW";
      break;
    case ObDDLTaskStatus::GENERATE_SQ_META_TABLE_SCHEMA:
      str = "GENERATE_SQ_META_TABLE_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_SQ_META_TABLE_COMPLEMENT:
      str = "WAIT_SQ_META_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::GENERATE_CENTROID_TABLE_SCHEMA:
      str = "GENERATE_CENTROID_TABLE_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_CENTROID_TABLE_COMPLEMENT:
      str = "WAIT_CENTROID_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::GENERATE_PQ_CENTROID_TABLE_SCHEMA:
      str = "GENERATE_PQ_CENTROID_TABLE_SCHEMA";
      break;
    case ObDDLTaskStatus::WAIT_PQ_CENTROID_TABLE_COMPLEMENT:
      str = "WAIT_PQ_CENTROID_TABLE_COMPLEMENT";
      break;
    case ObDDLTaskStatus::FAIL:
      str = "FAIL";
      break;
    case ObDDLTaskStatus::SUCCESS:
      str = "SUCCESS";
      break;
  }
  return str;
}

static bool is_partition_split_recovery_table_redefinition(const ObDDLType ddl_type) {
  return ddl_type == DDL_PARTITION_SPLIT_RECOVERY_TABLE_REDEFINITION;
}

static bool is_tablet_split(const ObDDLType ddl_type) {
  return ddl_type >= DDL_AUTO_SPLIT_BY_RANGE && ddl_type <= DDL_MANUAL_SPLIT_NON_RANGE;
}

static bool is_range_split(const ObDDLType ddl_type) {
  return DDL_AUTO_SPLIT_BY_RANGE == ddl_type || DDL_MANUAL_SPLIT_BY_RANGE == ddl_type;
}

static bool is_auto_split(const ObDDLType ddl_type) {
  return DDL_AUTO_SPLIT_BY_RANGE == ddl_type || DDL_AUTO_SPLIT_NON_RANGE == ddl_type;
}

static inline bool is_simple_table_long_running_ddl(const ObDDLType type)
{
  return type > DDL_INVALID && type < DDL_DROP_SCHEMA_AVOID_CONCURRENT_TRANS;
}

static inline bool is_drop_schema_block_concurrent_trans(const ObDDLType type)
{
  return type > DDL_DROP_SCHEMA_AVOID_CONCURRENT_TRANS && type < DDL_DOUBLE_TABLE_OFFLINE;
}

static inline bool is_double_table_long_running_ddl(const ObDDLType type)
{
  return type > DDL_DOUBLE_TABLE_OFFLINE && type < DDL_NORMAL_TYPE;
}

static inline bool is_long_running_ddl(const ObDDLType type)
{
  return is_simple_table_long_running_ddl(type) || is_double_table_long_running_ddl(type);
}

static inline bool is_direct_load_task(const ObDDLType type)
{
  return DDL_DIRECT_LOAD == type || DDL_DIRECT_LOAD_INSERT == type;
}

static inline bool is_complement_data_relying_on_dag(const ObDDLType type)
{
  return DDL_DROP_COLUMN == type
      || DDL_ADD_COLUMN_OFFLINE == type
      || DDL_COLUMN_REDEFINITION == type
      || DDL_TABLE_RESTORE == type;
}

static inline bool is_delete_lob_meta_row_relying_on_dag(const ObDDLType type)
{
  return DDL_DROP_VEC_INDEX == type;
}

static inline bool is_invalid_ddl_type(const ObDDLType type)
{
  return DDL_INVALID == type;
}

static inline bool is_create_index(const ObDDLType type)
{
 return ObDDLType::DDL_CREATE_INDEX == type || ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX == type;
}
// ddl stmt or rs ddl trans has rollbacked and can retry
static inline bool is_ddl_stmt_packet_retry_err(const int ret)
{
  return OB_EAGAIN == ret || OB_SNAPSHOT_DISCARDED == ret || OB_ERR_PARALLEL_DDL_CONFLICT == ret
      || OB_TRANS_KILLED == ret || OB_TRANS_ROLLBACKED == ret // table lock doesn't support leader switch
      || OB_PARTITION_IS_BLOCKED == ret // when LS is block_tx by a transfer task
      || OB_TRANS_NEED_ROLLBACK == ret // transaction killed by leader switch
      ;
}

static inline bool is_direct_load_retry_err(const int ret)
{
  return is_ddl_stmt_packet_retry_err(ret) || ret == OB_TABLET_NOT_EXIST || ret == OB_LS_NOT_EXIST
    || ret == OB_NOT_MASTER
    || ret == OB_TASK_EXPIRED
    || ret == OB_REPLICA_NOT_READABLE
    || ret == OB_TRANS_CTX_NOT_EXIST
    || ret == OB_SCHEMA_ERROR
    || ret == OB_SCHEMA_EAGAIN
    || ret == OB_SCHEMA_NOT_UPTODATE
    || ret == OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH
    || ret == OB_ERR_REMOTE_SCHEMA_NOT_FULL
    || ret == OB_SQL_RETRY_SPM
    ;
}

static inline bool is_supported_pre_split_ddl_type(const ObDDLType type)
{
  return DDL_MODIFY_COLUMN == type
      || DDL_ADD_PRIMARY_KEY == type
      || DDL_DROP_PRIMARY_KEY == type
      || DDL_ALTER_PRIMARY_KEY == type
      || DDL_CONVERT_TO_CHARACTER == type
      || DDL_TABLE_REDEFINITION == type
      || DDL_ALTER_PARTITION_BY == type;
}

static inline bool is_replica_build_ddl_task_status(const ObDDLTaskStatus &task_status)
{
  return ObDDLTaskStatus::REPENDING == task_status || ObDDLTaskStatus::REDEFINITION == task_status;
}

static inline ObDDLType get_create_index_type(const int64_t data_format_version, const share::schema::ObTableSchema &index_schema)
{
  return ((DATA_VERSION_4_2_2_0 <= data_format_version && data_format_version < DATA_VERSION_4_3_0_0) || data_format_version >= DATA_VERSION_4_3_2_0)
    && index_schema.is_storage_local_index_table() && index_schema.is_partitioned_table() ? ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX : ObDDLType::DDL_CREATE_INDEX;
}

enum ObCheckExistedDDLMode
{
  INVALID_DDL_MODE          = 0,
  ALL_LONG_RUNNING_DDL      = 1,
  SIMPLE_TABLE_RUNNING_DDL  = 2,
  DOUBLE_TABLE_RUNNING_DDL  = 3
};

struct ObColumnNameInfo final
{
public:
  ObColumnNameInfo()
    : column_name_(), is_shadow_column_(false), is_enum_set_need_cast_(false)
  {}
  ObColumnNameInfo(const ObString &column_name, const bool is_shadow_column, const bool is_enum_set_need_cast = false)
    : column_name_(column_name), is_shadow_column_(is_shadow_column), is_enum_set_need_cast_(is_enum_set_need_cast)
  {}
  ~ObColumnNameInfo() = default;
  TO_STRING_KV(K_(column_name), K_(is_shadow_column), K_(is_enum_set_need_cast));
public:
  ObString column_name_;
  bool is_shadow_column_;
  bool is_enum_set_need_cast_;
};

class ObColumnNameMap final {
public:
  ObColumnNameMap() {}
  ~ObColumnNameMap() {}
  int init(const schema::ObTableSchema &orig_table_schema,
           const schema::ObTableSchema &new_table_schema,
           const schema::AlterTableSchema &alter_table_arg);
  int init_xml_hidden_column_name_map(
      const schema::ObTableSchema &orig_table_schema,
      const schema::ObTableSchema &new_table_schema);
  int assign(const ObColumnNameMap &other);
  int set(const ObString &orig_column_name, const ObString &new_column_name);
  int get(const ObString &orig_column_name, ObString &new_column_name) const;
  int get_orig_column_name(const ObString &new_column_name, ObString &orig_column_name) const;
  int get_changed_names(ObIArray<std::pair<ObString, ObString>> &changed_names) const;
  DECLARE_TO_STRING;

private:
  ObArenaAllocator allocator_;
  lib::Worker::CompatMode compat_mode_;
  common::hash::ObHashMap<schema::ObColumnNameHashWrapper, ObString> col_name_map_;

  DISALLOW_COPY_AND_ASSIGN(ObColumnNameMap);
};

enum class RedefinitionState
{
  BEFORESCAN = 0,
  SCAN,
  INMEM_SORT,
  MERGE_SORT,
  INSERT,
  DDL_DIAGNOSE_V1
};

struct ScanMonitorNodeInfo final
{
public:
  ScanMonitorNodeInfo():
    tenant_id_(OB_INVALID_ID), task_id_(0), execution_id_(0), first_change_time_(0), last_change_time_(0), last_refresh_time_(0), output_rows_(0)
  {}
  ~ScanMonitorNodeInfo() = default;
  TO_STRING_KV(K(tenant_id_), K(task_id_), K(execution_id_), K(first_change_time_), K(last_change_time_), K(last_refresh_time_), K(output_rows_));
public:
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t first_change_time_;
  int64_t last_change_time_;
  int64_t last_refresh_time_;
  int64_t output_rows_;
};

struct SortMonitorNodeInfo final
{
public:
  SortMonitorNodeInfo():
    tenant_id_(OB_INVALID_ID), task_id_(0), execution_id_(0), thread_id_(0), row_count_id_(0), first_change_time_(0), last_change_time_(0),
    output_rows_(0), row_sorted_(0), dump_size_(0), row_count_(0), sort_expected_round_count_(0), merge_sort_start_time_(0), compress_type_(0)
  {}
  ~SortMonitorNodeInfo() = default;
  TO_STRING_KV(K(tenant_id_), K(task_id_), K(execution_id_), K(thread_id_), K(row_count_id_),
  K(first_change_time_), K(last_change_time_), K(output_rows_), K(row_sorted_), K(dump_size_),
  K(row_count_), K(sort_expected_round_count_), K(merge_sort_start_time_), K(compress_type_));

public:
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t thread_id_;
  int16_t row_count_id_;
  int64_t first_change_time_;
  int64_t last_change_time_;
  int64_t output_rows_;
  int64_t row_sorted_;
  int64_t dump_size_;
  int64_t row_count_;
  int64_t sort_expected_round_count_;
  int64_t merge_sort_start_time_;
  int64_t compress_type_;
};

struct InsertMonitorNodeInfo final
{
public:
  InsertMonitorNodeInfo():
    tenant_id_(OB_INVALID_ID), task_id_(0), execution_id_(0), thread_id_(0), last_refresh_time_(0), cg_row_inserted_(0), sstable_row_inserted_(0)
  {}
  ~InsertMonitorNodeInfo() = default;
  TO_STRING_KV(K(tenant_id_), K(task_id_), K(execution_id_), K(thread_id_), K(last_refresh_time_), K(cg_row_inserted_), K(sstable_row_inserted_));

public:
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t thread_id_;
  int64_t last_refresh_time_;
  int64_t cg_row_inserted_;
  int64_t sstable_row_inserted_;
};

struct ObSqlMonitorStats final
{
  ObSqlMonitorStats():
    is_inited_(false), tenant_id_(OB_INVALID_ID), task_id_(0), ddl_type_(ObDDLType::DDL_INVALID), execution_id_(-1), is_empty_(true)
  {}
  ~ObSqlMonitorStats() = default;
  TO_STRING_KV(K(tenant_id_), K(task_id_), K(execution_id_), K(ddl_type_), K(is_empty_), K(scan_node_), K(sort_node_), K(insert_node_));

public:
  int init(const uint64_t tenant_id, const int64_t task_id, const ObDDLType ddl_type);
  int clean_invalid_data(const int64_t execution_id);
  void reuse()
  {
    is_empty_ = true;
    execution_id_ = -1;
    scan_node_.reset();
    sort_node_.reset();
    insert_node_.reset();
  }

public:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t task_id_;
  ObDDLType ddl_type_;
  int64_t execution_id_;
  bool is_empty_;
  ObSEArray<ScanMonitorNodeInfo, 10> scan_node_;
  ObSEArray<SortMonitorNodeInfo, 10> sort_node_;
  ObSEArray<InsertMonitorNodeInfo, 10> insert_node_;
};

class ObSqlMonitorStatsCollector final
{
public:
  ObSqlMonitorStatsCollector()
    :sql_proxy_(nullptr), scan_task_id_(), scan_tenant_id_(), is_inited_(false),
     scan_res_(), sort_res_(), insert_res_(), scan_index_id_(0), sort_index_id_(0), insert_index_id_(0),
     tenant_id_(OB_INVALID_ID), task_id_(0), execution_id_(0), ddl_type_(DDL_INVALID)
  {}
  int init(ObMySQLProxy *sql_proxy);
  int get_next_sql_plan_monitor_stat(ObSqlMonitorStats &sql_monitor_stats);

private:
  int get_scan_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result);
  int get_sort_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result);
  int get_insert_monitor_stats_batch(sqlclient::ObMySQLResult *scan_result);

  int get_next_scanned_stats(ObSqlMonitorStats &sql_monitor_stats);
  int get_next_sorted_stats(ObSqlMonitorStats &sql_monitor_stats);
  int get_next_inserted_stats(ObSqlMonitorStats &sql_monitor_stats);

  bool inline next_ddl_monitor_node(const uint64_t tenant_id, const int64_t task_id)
  {
    return task_id < task_id_ || (task_id == task_id_ && tenant_id < tenant_id_);
  }

  bool inline previous_ddl_monitor_node(const uint64_t tenant_id, const int64_t task_id)
  {
    return task_id > task_id_ || tenant_id > tenant_id_;
  }

  bool inline outdated_monitor_node(const int64_t execution_id)
  {
    return execution_id < execution_id_ && ddl_type_ != ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX;
  }
  bool inline not_local_index_case()
  {
    return ddl_type_ != ObDDLType::DDL_CREATE_PARTITIONED_LOCAL_INDEX;
  }

public:
  ObMySQLProxy *sql_proxy_;
  ObSEArray<int64_t, 100> scan_task_id_;
  ObSEArray<uint64_t, 100> scan_tenant_id_;
private:
  bool is_inited_;
  ObSEArray<ScanMonitorNodeInfo, 100> scan_res_;
  ObSEArray<SortMonitorNodeInfo, 100> sort_res_;
  ObSEArray<InsertMonitorNodeInfo, 100> insert_res_;
  uint64_t scan_index_id_;
  uint64_t sort_index_id_;
  uint64_t insert_index_id_;
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t execution_id_;
  ObDDLType ddl_type_;
};

struct ObDDLTaskStatInfo final
{
public:
  ObDDLTaskStatInfo();
  ~ObDDLTaskStatInfo() = default;
  int init(const char *&ddl_type_str, const uint64_t table_id);
  TO_STRING_KV(K_(start_time), K_(finish_time), K_(time_remaining), K_(percentage),
               K_(op_name), K_(target), K_(message));
public:
  int64_t start_time_;
  int64_t finish_time_;
  int64_t time_remaining_;
  int64_t percentage_;
  char op_name_[common::MAX_LONG_OPS_NAME_LENGTH];
  char target_[common::MAX_LONG_OPS_TARGET_LENGTH];
  char message_[common::MAX_LONG_OPS_MESSAGE_LENGTH];
};

class ObDDLDiagnoseInfo final
{
public:
  ObDDLDiagnoseInfo()
  {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    task_id_ = 0;
    ddl_type_ = ObDDLType::DDL_INVALID;

    scan_thread_num_ = 0;
    row_scanned_ = 0;
    max_row_scan_ = 0;
    min_row_scan_= INT64_MAX;
    scan_spend_time_ = 0;

    inmem_sort_thread_num_ = 0;
    row_sorted_ = 0;
    inmem_sort_remain_time_ = 0;
    inmem_sort_progress_ = 1;
    inmem_sort_spend_time_ = 0;
    inmem_sort_slowest_thread_id_ = 0;

    merge_sort_thread_num_ = 0;
    row_merge_sorted_ = 0;
    expected_round_ = 0;
    merge_sort_remain_time_= 0;
    merge_sort_progress_ = 1;
    dump_size_ = 0;
    compress_type_ = 0;
    merge_sort_spend_time_ = 0;
    merge_sort_slowest_thread_id_ = 0;

    row_inserted_cg_ = 0;
    row_inserted_file_ = 0;
    insert_thread_num_ = 0;
    finish_thread_num_ = 0;
    insert_progress_ = 1;
    insert_remain_time_ = 0;
    insert_slowest_thread_id_ = 0;

    state_ = RedefinitionState::BEFORESCAN;
    is_empty_ = true;
    finish_ddl_ = false;
    create_local_index_batch_ = false;
    parallelism_ = 0;
    real_parallelism_ = 0;
    execution_id_ = 0;
    slowest_thread_id_ = 0;
    row_max_ = 0;
    row_max_thread_ = 0;
    row_min_ = 0;
    row_min_thread_ = 0;
    scan_start_time_ = 0;
    scan_end_time_ = 0;
    sort_end_time_ = 0;
    insert_end_time_ = 0;
    pos_ = 0;
    thread_index_ = 0;
  }

  ~ObDDLDiagnoseInfo() = default;
  int init(const uint64_t tenant_id, const int64_t task_id, const ObDDLType ddl_type, const int64_t execution_id);

  void inline reuse()
  {
    scan_thread_num_ = 0;
    row_scanned_ = 0;
    max_row_scan_ = 0;
    min_row_scan_= INT64_MAX;
    scan_spend_time_ = 0;

    inmem_sort_thread_num_ = 0;
    row_sorted_ = 0;
    inmem_sort_remain_time_ = 0;
    inmem_sort_progress_ = 1;
    inmem_sort_spend_time_ = 0;
    inmem_sort_slowest_thread_id_ = 0;

    merge_sort_thread_num_ = 0;
    row_merge_sorted_ = 0;
    expected_round_ = 0;
    merge_sort_remain_time_= 0;
    merge_sort_progress_ = 1;
    dump_size_ = 0;
    compress_type_ = 0;
    merge_sort_spend_time_ = 0;
    merge_sort_slowest_thread_id_ = 0;

    row_inserted_cg_ = 0;
    row_inserted_file_ = 0;
    insert_thread_num_ = 0;
    insert_progress_ = 1;
    insert_remain_time_ = 0;
    insert_slowest_thread_id_ = 0;

    state_ = RedefinitionState::BEFORESCAN;
    finish_thread_num_ = 0;
    is_empty_ = true;
    parallelism_ = 0;
    real_parallelism_ = 0;
    execution_id_ = 0;
    slowest_thread_id_ = 0;
    row_max_ = 0;
    row_max_thread_ = 0;
    row_min_ = 0;
    row_min_thread_ = 0;
    scan_start_time_ = 0;
    scan_end_time_ = 0;
    sort_end_time_ = 0;
    insert_end_time_ = 0;
    diagnose_message_[0] = '\0';
    pos_ = 0;
    thread_index_ = 0;
  }

  int diagnose(const ObSqlMonitorStats &sql_monitor_stats);
  int process_sql_monitor_and_generate_longops_message(const ObSqlMonitorStats &sql_monitor_stats, const int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos);
  const inline char *get_diagnose_info()
  {
    return diagnose_message_;
  }
  TO_STRING_KV(K(tenant_id_), K(task_id_),
  K(scan_thread_num_), K(row_scanned_), K(max_row_scan_), K(min_row_scan_), K(scan_start_time_), K(scan_end_time_), K(scan_spend_time_),
  K(inmem_sort_thread_num_), K(row_sorted_), K(inmem_sort_remain_time_), K(inmem_sort_progress_),
  K(merge_sort_thread_num_), K(row_merge_sorted_), K(expected_round_), K(merge_sort_remain_time_), K(merge_sort_progress_),
  K(dump_size_), K(compress_type_),
  K(row_inserted_cg_), K(row_inserted_file_), K(insert_thread_num_), K(insert_progress_), K(insert_remain_time_),
  K(state_), K(parallelism_), K(real_parallelism_), K(execution_id_), K(finish_thread_num_),
  K(min_inmem_sort_row_), K(min_merge_sort_row_), K(min_insert_row_));

private:
  int calculate_sql_plan_monitor_node_info(const ObSqlMonitorStats &sql_monitor_stats);
  int calculate_scan_monitor_node_info(const ObSqlMonitorStats &sql_monitor_stats);
  int calculate_sort_and_insert_info(const ObSqlMonitorStats &sql_monitor_stats);
  int calculate_inmem_sort_info(
      const int64_t row_sorted,
      const int64_t row_count,
      const int64_t first_change_time,
      const int64_t thread_id);
  int calculate_merge_sort_info(
      const int64_t row_count,
      const int64_t row_sorted,
      const SortMonitorNodeInfo &sort_info);
  int calculate_insert_info(
      const int64_t row_count,
      const SortMonitorNodeInfo &sort_info,
      const ObSqlMonitorStats &sql_monitor_stats);
  int local_index_diagnose();
  int finish_ddl_diagnose();
  int running_ddl_diagnose();
  int check_diagnose_case();
  int diagnose_stats_analysis();
  int generate_session_longops_message(const int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos);
  int generate_session_longops_message_v1(int64_t target_cg_cnt, ObDDLTaskStatInfo &stat_info, int64_t &pos);
  bool inline is_skip_case() // finish ddl without sql plan monitor node
  {
    return finish_ddl_ && is_empty_;
  }
  bool inline is_data_skew()
  {
    if (row_min_ > 0) {
      if (static_cast<double>(row_max_ - row_min_) / row_min_ > DATA_SKEW_RATE) {
        return true;
      }
    }
    return false;
  }
  bool inline is_thread_without_data()
  {
    return real_parallelism_ < parallelism_;
  }

static constexpr double DATA_SKEW_RATE = 1.00;

private:
  // ddl info
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t task_id_;
  ObDDLType ddl_type_;

  // scan
  int64_t scan_thread_num_;
  int64_t row_scanned_;
  int64_t max_row_scan_;
  int64_t min_row_scan_;
  double scan_spend_time_;

  // inmem_sort
  int64_t inmem_sort_thread_num_;
  int64_t row_sorted_;
  double inmem_sort_remain_time_;
  double inmem_sort_progress_;
  int64_t min_inmem_sort_row_;
  double inmem_sort_spend_time_;
  int64_t inmem_sort_slowest_thread_id_;

  // merge_sort
  int64_t merge_sort_thread_num_;
  int64_t row_merge_sorted_;
  int64_t expected_round_;
  double merge_sort_progress_;
  double merge_sort_remain_time_;
  int64_t min_merge_sort_row_;
  int64_t compress_type_;
  int64_t dump_size_;
  double merge_sort_spend_time_;
  int64_t merge_sort_slowest_thread_id_;

  //insert
  int64_t insert_thread_num_;
  int64_t row_inserted_cg_;
  int64_t row_inserted_file_;
  double insert_progress_;
  double insert_remain_time_;
  int64_t min_insert_row_;
  double insert_spend_time_;
  int64_t insert_slowest_thread_id_ = 0;

  // analysis data
  bool is_empty_;
  bool finish_ddl_;
  bool create_local_index_batch_;
  RedefinitionState state_;
  int64_t parallelism_;
  int64_t real_parallelism_ ;
  int64_t execution_id_;
  int64_t finish_thread_num_;


  int64_t slowest_thread_id_;
  uint64_t row_max_;
  uint64_t row_max_thread_;
  uint64_t row_min_;
  uint64_t row_min_thread_;

  int64_t scan_start_time_;
  int64_t scan_end_time_;
  int64_t sort_end_time_;
  int64_t insert_end_time_;
  uint64_t thread_index_;
  char diagnose_message_[common::OB_DIAGNOSE_INFO_LENGTH];
  int64_t pos_;
};

class ObDDLUtil
{
public:
  struct ObReplicaKey final
  {
  public:
    ObReplicaKey(): partition_id_(common::OB_INVALID_ID), addr_()
    {}
    ObReplicaKey(const int64_t partition_id, common::ObAddr addr): partition_id_(partition_id), addr_(addr)
    {}
    ~ObReplicaKey() = default;
    uint64_t hash() const
    {
      uint64_t hash_val = addr_.hash();
      hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
      return hash_val;
    }
    bool operator ==(const ObReplicaKey &other) const
    {
      return partition_id_ == other.partition_id_ && addr_ == other.addr_;
    }

    TO_STRING_KV(K_(partition_id), K_(addr));
  public:
    int64_t partition_id_;
    common::ObAddr addr_;
  };

  // get all tablets of a table by table_id
  static int get_tablets(
      const uint64_t tenant_id,
      const int64_t table_id,
      common::ObIArray<common::ObTabletID> &tablet_ids);

  static int get_tablet_count(const uint64_t tenant_id,
                              const int64_t table_id,
                              int64_t &tablet_count);
  static int get_all_indexes_tablets_count(
      schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      int64_t &all_tablet_count);

  // check if the major sstable of a table are exist in all needed replicas
  static int check_major_sstable_complete(
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      const int64_t snapshot_version,
      bool &is_complete);

  static int generate_spatial_index_column_names(const share::schema::ObTableSchema &dest_table_schema,
                                                 const share::schema::ObTableSchema &source_table_schema,
                                                 ObArray<ObColumnNameInfo> &insert_column_names,
                                                 ObArray<ObColumnNameInfo> &column_names,
                                                 ObArray<int64_t> &select_column_ids);
  static int append_multivalue_extra_column(const share::schema::ObTableSchema &dest_table_schema,
                                            const share::schema::ObTableSchema &source_table_schema,
                                            ObArray<ObColumnNameInfo> &column_names,
                                            ObArray<int64_t> &select_column_ids);
  static int generate_build_replica_sql(
      const uint64_t tenant_id,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int64_t task_id,
      const int64_t parallelism,
      const bool use_heap_table_ddl_plan,
      const bool use_schema_version_hint_for_src_table,
      const ObColumnNameMap *col_name_map,
      const ObString &partition_names,
      ObSqlString &sql_string);

  static int generate_build_mview_replica_sql(
      const uint64_t tenant_id,
      const int64_t mview_table_id,
      const int64_t container_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int64_t task_id,
      const int64_t parallelism,
      const bool use_schema_version_hint_for_src_table,
      const common::ObIArray<share::schema::ObBasedSchemaObjectInfo> &based_schema_object_infos,
      ObSqlString &sql_string);

  static int get_tablet_leader_addr(
      share::ObLocationService *location_service,
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout,
      share::ObLSID &ls_id,
      common::ObAddr &leader_addr);

  static int refresh_alter_table_arg(
      const uint64_t tenant_id,
      const int64_t orig_table_id,
      const uint64_t foreign_key_id,
      obrpc::ObAlterTableArg &alter_table_arg);

  static int generate_ddl_schema_hint_str(
      const ObString &table_name,
      const int64_t schema_version,
      const bool is_oracle_mode,
      ObSqlString &sql_string);

  static int generate_mview_ddl_schema_hint_str(
      const uint64_t tenant_id,
      const uint64_t mview_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::schema::ObBasedSchemaObjectInfo> &based_schema_object_infos,
      const bool is_oracle_mode,
      ObSqlString &sql_string);

  static int generate_order_by_str_for_mview(const schema::ObTableSchema &container_table_schema,
                                             ObSqlString &rowkey_column_sql_string);

  static int ddl_get_tablet(
      const storage::ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      storage::ObTabletHandle &tablet_handle,
      const storage::ObMDSGetTabletMode mode = storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK);

  static int clear_ddl_checksum(sql::ObPhysicalPlan *phy_plan);

  static bool is_table_lock_retry_ret_code(int ret)
  {
    return OB_TRY_LOCK_ROW_CONFLICT == ret || OB_NOT_MASTER == ret || OB_TIMEOUT == ret
           || OB_EAGAIN == ret || OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_TRANS_CTX_NOT_EXIST == ret;
  }

  static bool need_remote_write(const int ret_code);

  static int check_can_convert_character(const ObObjMeta &obj_meta)
  {
    return (obj_meta.is_string_type() || obj_meta.is_enum_or_set())
              && CS_TYPE_BINARY != obj_meta.get_collation_type();
  }

  static int get_sys_ls_leader_addr(
    const uint64_t cluster_id,
    const uint64_t tenant_id,
    common::ObAddr &leader_addr);

  static int get_tablet_paxos_member_list(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    common::ObIArray<common::ObAddr> &paxos_server_list,
    int64_t &paxos_member_count);

  static int get_tablet_replica_location(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    ObLSID &ls_id,
    ObLSLocation &location);
  static int get_split_replicas_addrs(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIArray<ObAddr> &member_addrs_array,
    ObIArray<ObAddr> &learner_addrs_array);
  static int get_split_replicas_addrs(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIArray<ObAddr> &replica_addr_array);
  static int construct_ls_tablet_id_map(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &ls_tablet_id_map);
  static int get_index_table_batch_partition_names(
    const uint64_t &tenant_id,
    const int64_t &data_table_id,
    const int64_t &index_table_id,
    const ObIArray<ObTabletID> &tablets,
    common::ObIAllocator &allocator,
    ObIArray<ObString> &partition_names);
  static int get_tablet_data_size(
    const uint64_t &tenant_id,
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    int64_t &data_size);
  static int get_tablet_data_row_cnt(
    const uint64_t &tenant_id,
    const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    int64_t &data_row_cnt);
  static int get_ls_host_left_disk_space(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &leader_addr,
    uint64_t &left_space_size);
  static int generate_partition_names(
    const ObIArray<ObString> &partition_names_array,
    const bool is_oracle_mode,
    common::ObIAllocator &allocator,
    ObString &partition_names);
  static int check_target_partition_is_running(
   const ObString &running_sql_info,
   const ObString &partition_name,
   common::ObIAllocator &allocator,
   bool &is_running_status);
  static int get_tablet_leader(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    common::ObAddr &leader_addr);
  static int check_table_exist(
     const uint64_t tenant_id,
     const uint64_t table_id,
     share::schema::ObSchemaGetterGuard &schema_guard);
  static int get_ddl_rpc_timeout(const int64_t tablet_count, int64_t &ddl_rpc_timeout_us);
  static int get_ddl_rpc_timeout(const int64_t tenant_id, const int64_t table_id, int64_t &ddl_rpc_timeout_us);
  static int get_ddl_tx_timeout(const int64_t tablet_count, int64_t &ddl_tx_timeout_us);
  static int get_ddl_tx_timeout(const int64_t tenant_id, const int64_t table_id, int64_t &ddl_tx_timeout_us);
  static void get_ddl_rpc_timeout_for_database(const int64_t tenant_id, const int64_t database_id, int64_t &ddl_rpc_timeout_us);
  static int64_t get_default_ddl_rpc_timeout();
  static int64_t get_default_ddl_tx_timeout();

  static int get_task_tablet_slice_count(const int64_t tenant_id, const int64_t task_id, bool &is_partition_table, common::hash::ObHashMap<int64_t, int64_t> &tablet_slice_count_map);

  static int get_data_information(
     const uint64_t tenant_id,
     const uint64_t task_id,
     uint64_t &data_format_version,
     int64_t &snapshot_version,
     share::ObDDLTaskStatus &task_status);

  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObPartitionSplitArg &split_arg);
  static int get_data_information(
     const uint64_t tenant_id,
     const uint64_t task_id,
     uint64_t &data_format_version,
     int64_t &snapshot_version,
     share::ObDDLTaskStatus &task_status,
     uint64_t &target_object_id,
     int64_t &schema_version,
     bool &is_no_logging);

  static int replace_user_tenant_id(
    const ObDDLType &ddl_type,
    const uint64_t tenant_id,
    obrpc::ObAlterTableArg &alter_table_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObDropDatabaseArg &drop_db_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObDropTableArg &drop_table_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObDropIndexArg &drop_index_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObRebuildIndexArg &rebuild_index_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObTruncateTableArg &trucnate_table_arg);
  static int replace_user_tenant_id(const uint64_t tenant_id, obrpc::ObCreateIndexArg &create_index_arg);

  static int generate_column_name_str(
    const common::ObIArray<ObColumnNameInfo> &column_names,
    const bool is_oracle_mode,
    const bool with_origin_name,
    const bool with_alias_name,
    const bool use_heap_table_ddl_plan,
    ObSqlString &column_name_str);
  static int generate_column_name_str(
      const ObColumnNameInfo &column_name_info,
      const bool is_oracle_mode,
      const bool with_origin_name,
      const bool with_alias_name,
      const bool with_comma,
      ObSqlString &sql_string);
  static int reshape_ddl_column_obj(
      common::ObDatum &datum,
      const ObObjMeta &obj_meta);
  static int64_t calc_inner_sql_execute_timeout()
  {
    return max(OB_MAX_DDL_SINGLE_REPLICA_BUILD_TIMEOUT, GCONF._ob_ddl_timeout);
  }

  /**
   * NOTICE: The interface is designed for Offline DDL operation only.
   * The caller can not obtain the schema via the hold_buf_src_tenant_schema_guard whose
   * validity is limited by whether src_tenant_id and dst_tenant_id are the same.
   *
   * 1. This interface will provide the same tenant schema guard when src_tenant_id = dst_tenant_id,
   *    to avoid using two different versions of the guard caused by the parallel ddl under the tenant.
   * 2. This interface will provide corresponding tenant schema guard when src_tenant_id != dst_tenant_id.
   *
   * @param [in] src_tenant_id
   * @param [in] dst_tenant_id
   * @param [in] hold_buf_src_tenant_schema_guard: hold buf, invalid when src_tenant_id = dst_tenant_id.
   * @param [in] hold_buf_dst_tenant_schema_guard: hold buf.
   * @param [out] src_tenant_schema_guard:
   *    pointer to the hold_buf_dst_tenant_schema_guard if src_tenant_id = dst_tenant_id,
   *    pointer to the hold_buf_src_tenant_schema_guard if src_tenant_id != dst_tenant_id,
   *    is always not nullptr if the interface return OB_SUCC.
   * @param [out] dst_tenant_schema_guard:
   *    pointer to the hold_buf_dst_tenant_schema_guard,
   *    is always not nullptr if the interface return OB_SUCC.
  */
  static int get_tenant_schema_guard(
      const uint64_t src_tenant_id,
      const uint64_t dst_tenant_id,
      share::schema::ObSchemaGetterGuard &hold_buf_src_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard &hold_buf_dst_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard *&src_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard *&dst_tenant_schema_guard);
  static int get_tablet_physical_row_cnt(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool calc_sstable,
      const bool calc_memtable,
      int64_t &physical_row_count /*OUT*/);
  static int check_table_empty_in_oracle_mode(
      const uint64_t tenant_id,
      const uint64_t table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &is_table_empty);
  static int check_tenant_status_normal(
      ObISQLClient *proxy,
      const uint64_t check_tenant_id);
  static int check_schema_version_refreshed(
      const uint64_t tenant_id,
      const int64_t target_schema_version);
  static bool reach_time_interval(const int64_t i, volatile int64_t &last_time);
  static int is_major_exist(const ObLSID &ls_id, const common::ObTabletID &tablet_id, bool &is_exist);
#ifdef OB_BUILD_SHARED_STORAGE
  static int upload_block_for_ss(const char* buf, const int64_t len, const blocksstable::MacroBlockId &macro_block_id);
  static int update_tablet_gc_info(const ObTabletID &tablet_id, const int64_t pre_snapshot_version, const int64_t new_snapshot_version);
#endif
  static int set_tablet_autoinc_seq(const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t seq_value);
  static int check_table_compaction_checksum_error(
      const uint64_t tenant_id,
      const uint64_t table_id);
  static int get_temp_store_compress_type(const ObCompressorType schema_compr_type,
                                          const int64_t parallel,
                                          ObCompressorType &compr_type);
  static int get_temp_store_compress_type(const share::schema::ObTableSchema *table_schema,
                                          const int64_t parallel,
                                          ObCompressorType &compr_type);
  static inline bool is_verifying_checksum_error_needed(share::ObDDLType type)
  {
    bool res = false;
    switch (type) {
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_DROP_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_DROP_COLUMN:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
      case DDL_TABLE_REDEFINITION:
      case DDL_DIRECT_LOAD:
      case DDL_DIRECT_LOAD_INSERT:
      case DDL_MVIEW_COMPLETE_REFRESH:
      case DDL_CREATE_MVIEW:
      case DDL_ALTER_COLUMN_GROUP:
      case DDL_CREATE_INDEX:
      case DDL_CREATE_MLOG:
      case DDL_CREATE_FTS_INDEX:
      case DDL_CREATE_VEC_INDEX:
      case DDL_CREATE_PARTITIONED_LOCAL_INDEX:
      case DDL_AUTO_SPLIT_BY_RANGE:
      case DDL_AUTO_SPLIT_NON_RANGE:
      case DDL_MANUAL_SPLIT_BY_RANGE:
      case DDL_MANUAL_SPLIT_NON_RANGE:
      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        res = true;
        break;
      default:
        res = false;
    }
    return res;
  }
  static int get_global_index_table_ids(const schema::ObTableSchema &table_schema, ObIArray<uint64_t> &global_index_table_ids, share::schema::ObSchemaGetterGuard &schema_guard);
  static bool use_idempotent_mode(const int64_t data_format_version);
  static int init_macro_block_seq(const int64_t parallel_idx, blocksstable::ObMacroDataSeq &start_seq);
  static bool is_mview_not_retryable(const int64_t data_format_version, const share::ObDDLType task_type);
  static int64_t get_real_parallelism(const int64_t parallelism, const bool is_mv_refresh);
  static int obtain_snapshot(
      const share::ObDDLTaskStatus next_task_status,
      const uint64_t table_id,
      const uint64_t target_table_id,
      int64_t &snapshot_version,
      rootserver::ObDDLTask* task,
      const common::ObIArray<common::ObTabletID> *extra_mv_tablet_ids = NULL);
  static int release_snapshot(
      rootserver::ObDDLTask* task,
      const uint64_t table_id,
      const uint64_t target_table_id,
      const int64_t snapshot_version);
  static int check_and_cancel_single_replica_dag(
      rootserver::ObDDLTask* task,
      const uint64_t table_id,
      const uint64_t target_table_id,
      common::hash::ObHashMap<common::ObTabletID, common::ObTabletID>& check_dag_exit_tablets_map,
      int64_t &check_dag_exit_retry_cnt,
      bool is_complement_data_dag,
      bool &all_dag_exit);
  static int get_no_logging_param(const int64_t tenant_id, bool &is_no_logging);
  static int batch_check_tablet_checksum(
      const uint64_t tenant_id,
      const int64_t start_idx,
      const int64_t end_idx,
      const ObIArray<ObTabletID> &tablet_ids);
  static int hold_snapshot(
      common::ObMySQLTransaction &trans,
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      const int64_t snapshot);
  static int check_need_acquire_lob_snapshot(
      const ObTableSchema *data_table_schema,
      const ObTableSchema *index_table_schema,
      bool &need_acquire);
  static int obtain_snapshot(
      common::ObMySQLTransaction &trans,
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      int64_t &new_fetched_snapshot);
  static int calc_snapshot_with_gts(
      int64_t &snapshot,
      const uint64_t tenant_id,
      const int64_t ddl_task_id = 0,
      const int64_t trans_end_snapshot = 0,
      const int64_t index_snapshot_version_diff = 0);
private:
  static int hold_snapshot(
      common::ObMySQLTransaction &trans,
      rootserver::ObDDLTask* task,
      const uint64_t table_id,
      const uint64_t target_table_id,
      rootserver::ObRootService *root_service,
      const int64_t snapshot_version,
      const common::ObIArray<common::ObTabletID> *extra_mv_tablet_ids);

  static int check_table_column_checksum_error(
      const uint64_t tenant_id,
      const int64_t table_id);

  static int check_tablet_checksum_error(
      const uint64_t tenant_id,
      const int64_t table_id);

  static int generate_order_by_str(
      const ObIArray<int64_t> &select_column_ids,
      const ObIArray<int64_t> &order_column_ids,
      ObSqlString &sql_string);
  static int find_table_scan_table_id(
      const sql::ObOpSpec *spec,
      uint64_t &table_id);

public:
  const static int64_t MAX_BATCH_COUNT = 128;
};

class ObCODDLUtil
{
public:
  static int need_column_group_store(const storage::ObStorageSchema &table_schema, bool &need_column_group);
  static int need_column_group_store(const schema::ObTableSchema &table_schema, bool &need_column_group);

  static int get_base_cg_idx(
      const storage::ObStorageSchema *storage_schema,
      int64_t &base_cg_idx);

  static int get_column_checksums(
      const storage::ObCOSSTableV2 *co_sstable,
      const storage::ObStorageSchema *storage_schema,
      ObIArray<int64_t> &column_checksums);

  static int is_rowkey_based_co_sstable(
      const storage::ObCOSSTableV2 *co_sstable,
      const storage::ObStorageSchema *storage_schema,
      bool &is_rowkey_based);
};



class ObCheckTabletDataComplementOp
{
public:

  static int check_and_wait_old_complement_task(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const int64_t ddl_task_id,
      const int64_t execution_id,
      const common::ObAddr &inner_sql_exec_addr,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t schema_version,
      const int64_t scn,
      bool &need_exec_new_inner_sql);
  static int check_finish_report_checksum(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const int64_t execution_id,
      const uint64_t ddl_task_id);
  static int check_tablet_checksum_update_status(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const uint64_t ddl_task_id,
      const int64_t execution_id,
      const ObIArray<ObTabletID> &tablet_ids,
      bool &tablet_checksum_status);

private:

  static int check_all_tablet_sstable_status(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const uint64_t ddl_task_id,
      bool &is_all_sstable_build_finished);

  static int check_task_inner_sql_session_status(
      const common::ObAddr &inner_sql_exec_addr,
      const common::ObCurTraceId::TraceId &trace_id,
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t scn,
      bool &is_old_task_session_exist);

  static int do_check_tablets_merge_status(
      const uint64_t tenant_id,
      const int64_t snapshot_version,
      const ObIArray<ObTabletID> &tablet_ids,
      const ObLSID &ls_id,
      hash::ObHashMap<ObAddr, ObArray<ObTabletID>> &ip_tablets_map,
      hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map,
      int64_t &tablet_commit_count);

  static int check_tablet_merge_status(
      const uint64_t tenant_id,
      const ObIArray<common::ObTabletID> &tablet_ids,
      const int64_t snapshot_version,
      bool &is_all_tablets_commited);

  static int update_replica_merge_status(
      const ObTabletID &tablet_id,
      const bool merge_status,
      hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map);


  static int calculate_build_finish(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      hash::ObHashMap<ObTabletID, int32_t> &tablets_commited_map,
      int64_t &commit_succ_count);

  static int construct_ls_tablet_map(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      hash::ObHashMap<ObLSID, ObArray<ObTabletID>> &ls_tablets_map);

  static int construct_tablet_ip_map(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      hash::ObHashMap<ObAddr, ObArray<ObTabletID>> &ip_tablets_map);
};

class ObSplitUtil
{
public:
  static int deserializ_parallel_datum_rowkey(
      common::ObIAllocator &rowkey_allocator,
      const char *buf, const int64_t data_len, int64_t &pos,
      ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list);
};

class ObSplitTabletInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObSplitTabletInfo() : split_info_(0), split_src_tablet_id_() { }
  ~ObSplitTabletInfo() { reset(); }
  void reset() { split_info_ = 0; split_src_tablet_id_.reset(); }
  void set_data_incomplete(const bool is_data_incomplete) { is_data_incomplete_ = is_data_incomplete; }
  void set_split_src_tablet_id(const ObTabletID &split_src_tablet_id) { split_src_tablet_id_ = split_src_tablet_id; }
  bool is_data_incomplete() const { return is_data_incomplete_; }
  const ObTabletID &get_split_src_tablet_id() const { return split_src_tablet_id_; }
  TO_STRING_KV(K_(split_info), K_(split_src_tablet_id));
private:
  union {
    uint32_t split_info_;
    struct {
      uint32_t is_data_incomplete_: 1; // whether the data of split dest tablet is complete.
      uint32_t reserved: 31;
    };
  };
  ObTabletID split_src_tablet_id_;
};

typedef common::ObCurTraceId::TraceId DDLTraceId;
class ObDDLEventInfo final
{
public:
  ObDDLEventInfo();
  ObDDLEventInfo(const int32_t sub_id);
  ~ObDDLEventInfo() = default;
  void record_in_guard();
  void copy_event(const ObDDLEventInfo &other);
  void init_sub_trace_id(const int32_t sub_id);
  void set_inner_sql_id(const int64_t inner_sql_id);
  const DDLTraceId &get_trace_id() const { return trace_id_; }
  const DDLTraceId &get_parent_trace_id() const { return parent_trace_id_; }
  int set_trace_id(const DDLTraceId &trace_id) { return trace_id_.set(trace_id.get()); }
  void reset();
  TO_STRING_KV(K(addr_), K(event_ts_), K(sub_id_), K(trace_id_), K(parent_trace_id_));

public:
  ObAddr addr_;
  int32_t sub_id_;
  int64_t event_ts_;
  DDLTraceId parent_trace_id_;
  DDLTraceId trace_id_;
};


}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_DDL_COMMON_H

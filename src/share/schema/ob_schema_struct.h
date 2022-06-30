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

#ifndef _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H

#include <stdint.h>
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_placement_hashset.h"
#include "lib/net/ob_addr.h"
#include "common/ob_range.h"
#include "common/row/ob_row_util.h"
#include "share/ob_replica_info.h"
#include "share/ob_duplicate_scope_define.h"
#include "share/sequence/ob_sequence_option.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/schema/ob_priv_type.h"
#include "share/ob_priv_common.h"

#ifdef __cplusplus
extern "C" {
#endif
struct _ParseNode;
typedef struct _ParseNode ParseNode;
#ifdef __cplusplus
}
#endif

namespace oceanbase {
namespace common {
class ObIAllocator;
class ObSqlString;
class ObString;
class ObDataTypeCastParams;
class ObPartitionKey;
}  // namespace common
namespace sql {
class ObSQLSessionInfo;
class ObPartitionExecutorUtils;
}  // namespace sql
namespace rootserver {
class ObRandomZoneSelector;
class ObReplicaAddr;
}  // namespace rootserver
namespace share {
namespace schema {
typedef common::Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false>
    ParamStore;
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
class ObTableSchema;
class ObColumnSchemaV2;
#define ASSIGN_STRING(dst, src, field, buffer, skip)           \
  (dst)->field.assign(buffer + offset, (src)->field.length()); \
  offset += (src)->field.length() + (skip);

#define ASSIGN_CONST_STRING(dst, src, field, buffer, skip)                               \
  (const_cast<ObString &>((dst)->field)).assign(buffer + offset, (src)->field.length()); \
  offset += (src)->field.length() + (skip);

// match the default now func
#define IS_DEFAULT_NOW_STR(data_type, def_str) \
  ((ObDateTimeType == data_type || ObTimestampType == data_type) && (def_str == N_UPPERCASE_CUR_TIMESTAMP))

#define IS_DEFAULT_NOW_OBJ(def_obj) \
  (ObExtendType == def_obj.get_type() && ObActionFlag::OP_DEFAULT_NOW_FLAG == def_obj.get_ext())

#define OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN 60

// the lower 32-bit flag need be store in __all_column
static const uint64_t OB_MIN_ID = 0;  // used for lower_bound
#define NON_CASCADE_FLAG INT64_C(0)
#define VIRTUAL_GENERATED_COLUMN_FLAG (INT64_C(1) << 0)
#define STORED_GENERATED_COLUMN_FLAG (INT64_C(1) << 1)
#define CTE_GENERATED_COLUMN_FLAG (INT64_C(1) << 2)
#define DEFAULT_EXPR_V2_COLUMN_FLAG (INT64_C(1) << 3)
#define PRIMARY_VP_COLUMN_FLAG (INT64_C(1) << 4)
#define RESERVED_COLUMN_FLAG (INT64_C(1) << 5)
#define INVISIBLE_COLUMN_FLAG (INT64_C(1) << 6)
// The logic of the new table without a primary key changes the column (partition key) to the primary key
#define HEAP_ALTER_ROWKEY_FLAG (INT64_C(1) << 8)
#define PAD_WHEN_CALC_GENERATED_COLUMN_FLAG (INT64_C(1) << 19)
// the high 32-bit flag isn't stored in __all_column
#define GENERATED_DEPS_CASCADE_FLAG (INT64_C(1) << 32)
#define GENERATED_CTXCAT_CASCADE_FLAG (INT64_C(1) << 33)
#define TABLE_PART_KEY_COLUMN_FLAG (INT64_C(1) << 34)

#define STORED_COLUMN_FLAGS_MASK 0xFFFFFFFF
//-------enum defenition
enum ObTableLoadType {
  TABLE_LOAD_TYPE_IN_DISK = 0,
  TABLE_LOAD_TYPE_IN_RAM = 1,
  TABLE_LOAD_TYPE_MAX = 2,
};
// the defination type of table
enum ObTableDefType {
  TABLE_DEF_TYPE_INTERNAL = 0,
  TABLE_DEF_TYPE_USER = 1,
  TABLE_DEF_TYPE_MAX = 2,
};
// level of patition
enum ObPartitionLevel {
  PARTITION_LEVEL_ZERO = 0,  // means non-partitioned table
  PARTITION_LEVEL_ONE = 1,
  PARTITION_LEVEL_TWO = 2,
  PARTITION_LEVEL_MAX,
};
// type of hash name to generate
enum ObHashNameType { FIRST_PART = 0, TEMPLATE_SUB_PART = 1, INDIVIDUAL_SUB_PART = 2 };

OB_INLINE int64_t generate_phy_part_id(int64_t part_idx, int64_t sub_part_idx, ObPartitionLevel part_level)
{
  return (PARTITION_LEVEL_TWO == part_level) ? common::generate_phy_part_id(part_idx, sub_part_idx) : part_idx;
}

enum ObPartitionFuncType {
  // TODO add other type
  PARTITION_FUNC_TYPE_HASH = 0,
  PARTITION_FUNC_TYPE_KEY,
  PARTITION_FUNC_TYPE_KEY_IMPLICIT,
  PARTITION_FUNC_TYPE_RANGE,
  PARTITION_FUNC_TYPE_RANGE_COLUMNS,
  PARTITION_FUNC_TYPE_LIST,
  PARTITION_FUNC_TYPE_KEY_V2,
  PARTITION_FUNC_TYPE_LIST_COLUMNS,
  PARTITION_FUNC_TYPE_HASH_V2,
  PARTITION_FUNC_TYPE_KEY_V3,
  PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2,
  PARTITION_FUNC_TYPE_MAX,
};

int get_part_type_str(ObPartitionFuncType type, common::ObString &str, bool can_change = true);

inline bool is_hash_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_HASH == part_type || PARTITION_FUNC_TYPE_HASH_V2 == part_type;
}

inline bool is_hash_like_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_HASH == part_type || PARTITION_FUNC_TYPE_HASH_V2 == part_type ||
         PARTITION_FUNC_TYPE_KEY == part_type || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type ||
         PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_type || PARTITION_FUNC_TYPE_KEY_V2 == part_type ||
         PARTITION_FUNC_TYPE_KEY_V3 == part_type;
}

inline bool is_key_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_KEY == part_type || PARTITION_FUNC_TYPE_KEY_V2 == part_type ||
         PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type || PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_type ||
         PARTITION_FUNC_TYPE_KEY_V3 == part_type;
}

inline bool is_range_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type;
}

inline bool is_list_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_LIST == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type;
}

int is_sys_table_name(uint64_t database_id, const common::ObString &table_name, bool &is_sys_table);

// adding new table type, take care ObRootUtils::is_balance_target_schema() interface
// This structure indicates whether the tableSchema of this type is the object of load balancing,
// and the judgment is based on whether the table schema has physical partitions of entities,
// and only non-system tables are load-balanced.
enum ObTableType {
  SYSTEM_TABLE = 0,
  SYSTEM_VIEW,
  VIRTUAL_TABLE,
  USER_TABLE,
  USER_VIEW,
  USER_INDEX,          // urgly, compatible with uniform process in ddl_service
                       // will add index for sys table???
  TMP_TABLE,           // Temporary table in mysql compatibility mode
  MATERIALIZED_VIEW,   // Must be put behind, otherwise compatibility will fail
  TMP_TABLE_ORA_SESS,  // Temporary table in oracle compatibility mode, session level
  TMP_TABLE_ORA_TRX,   // Temporary tables in oracle compatibility mode, transaction level
  TMP_TABLE_ALL,       // All types of temporary tables, only used for alter system statements
  AUX_VERTIAL_PARTITION_TABLE,
  MAX_TABLE_TYPE
};

// ObTableType=>const char* ; used for show tables
const char *ob_table_type_str(ObTableType type);
const char *ob_mysql_table_type_str(ObTableType type);

ObTableType get_inner_table_type_by_id(const uint64_t tid);

enum ObIndexType {
  INDEX_TYPE_IS_NOT = 0,  // is not index table
  INDEX_TYPE_NORMAL_LOCAL = 1,
  INDEX_TYPE_UNIQUE_LOCAL = 2,
  INDEX_TYPE_NORMAL_GLOBAL = 3,
  INDEX_TYPE_UNIQUE_GLOBAL = 4,
  INDEX_TYPE_PRIMARY = 5,
  INDEX_TYPE_DOMAIN_CTXCAT = 6,
  /* create table t1(c1 int primary key, c2 int);
   * create index i1 on t1(c2)
   * i1 is a global index.
   * But we regard i1 as a local index for better access performance.
   * Since it is non-partitioned, it's safe to do so.
   */
  INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE = 7,
  INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE = 8,

  INDEX_TYPE_MAX = 9,
};

// using type for index
enum ObIndexUsingType {
  USING_BTREE = 0,
  USING_HASH,
  USING_TYPE_MAX,
};

enum ViewCheckOption {
  VIEW_CHECK_OPTION_NONE = 0,
  VIEW_CHECK_OPTION_LOCAL = 1,
  VIEW_CHECK_OPTION_CASCADED = 2,
  VIEW_CHECK_OPTION_MAX = 3,
};

const char *ob_view_check_option_str(const ViewCheckOption option);
enum ObIndexStatus {
  // this is used in index virtual table:__index_process_info:means the table may be deleted when you get it
  INDEX_STATUS_NOT_FOUND = 0,
  INDEX_STATUS_UNAVAILABLE = 1,
  INDEX_STATUS_AVAILABLE = 2,
  INDEX_STATUS_UNIQUE_CHECKING = 3,    // not used anymore
  INDEX_STATUS_UNIQUE_INELIGIBLE = 4,  // not used anymore
  INDEX_STATUS_INDEX_ERROR = 5,
  INDEX_STATUS_RESTORE_INDEX_ERROR = 6,
  INDEX_STATUS_UNUSABLE = 7,
  INDEX_STATUS_MAX = 8,
};

bool can_rereplicate_index_status(const ObIndexStatus &idst);

enum ObPartitionStatus {
  PARTITION_STATUS_INVALID = -1,
  PARTITION_STATUS_ACTIVE = 0,
  PARTITION_STATUS_LOGICAL_SPLITTING = 1,
  PARTITION_STATUS_MERGE = 2,
  PARTITION_STATUS_PHYSICAL_SPLITTING = 3,
  PARTITION_STATUS_MAX,
};

enum ObAlterColumnMode {
  ALTER_COLUMN_MODE_FORBIDDEN = 0,
  ALTER_COLUMN_MODE_ALLOWED = 1,
  ALTER_COLUMN_MODE_FORBIDDEN_WHEN_MAJOR = 2,
};

struct ObRefreshSchemaStatus {
public:
  ObRefreshSchemaStatus()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        snapshot_timestamp_(common::OB_INVALID_TIMESTAMP),
        readable_schema_version_(common::OB_INVALID_VERSION),
        created_schema_version_(common::OB_INVALID_VERSION)
  {}

  ObRefreshSchemaStatus(const uint64_t tenant_id, const int64_t snapshot_timestamp,
      const int64_t readable_schema_version, const int64_t created_schema_version)
      : tenant_id_(tenant_id),
        snapshot_timestamp_(snapshot_timestamp),
        readable_schema_version_(readable_schema_version),
        created_schema_version_(created_schema_version)
  {}

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    snapshot_timestamp_ = common::OB_INVALID_TIMESTAMP;
    readable_schema_version_ = common::OB_INVALID_VERSION;
    created_schema_version_ = common::OB_INVALID_VERSION;
  }

  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }

  bool operator==(const ObRefreshSchemaStatus &other) const
  {
    return ((this == &other) ||
            (this->tenant_id_ == other.tenant_id_ && this->snapshot_timestamp_ == other.snapshot_timestamp_ &&
                this->readable_schema_version_ == other.readable_schema_version_ &&
                this->created_schema_version_ == other.created_schema_version_));
  }

  TO_STRING_KV(K_(tenant_id), K_(snapshot_timestamp), K_(readable_schema_version), K_(created_schema_version));

public:
  // tenant_id_ is OB_INVALID_TENANT_ID which means non-split mode, effectively means split mode
  uint64_t tenant_id_;
  // snapshot_timestamp_ > 0 Indicates that a weakly consistent read is required, and is used in standalone cluster mode
  int64_t snapshot_timestamp_;
  int64_t readable_schema_version_;
  int64_t created_schema_version_;
};

struct ObRefreshSchemaInfo {
  OB_UNIS_VERSION(1);

public:
  ObRefreshSchemaInfo()
      : schema_version_(common::OB_INVALID_VERSION),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        sequence_id_(common::OB_INVALID_ID),
        split_schema_version_(common::OB_INVALID_VERSION)
  {}
  ObRefreshSchemaInfo(const ObRefreshSchemaInfo &other);
  virtual ~ObRefreshSchemaInfo()
  {}
  int assign(const ObRefreshSchemaInfo &other);
  void reset();
  bool is_valid() const;
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  void set_sequence_id(const uint64_t sequence_id)
  {
    sequence_id_ = sequence_id;
  }
  void set_split_schema_version(const int64_t split_schema_version)
  {
    split_schema_version_ = split_schema_version;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  uint64_t get_sequence_id() const
  {
    return sequence_id_;
  }
  int64_t get_split_schema_version() const
  {
    return split_schema_version_;
  }
  TO_STRING_KV(K_(schema_version), K_(tenant_id), K_(sequence_id), K_(split_schema_version));

private:
  int64_t schema_version_;
  uint64_t tenant_id_;
  uint64_t sequence_id_;
  int64_t split_schema_version_;
};

class ObDropTenantInfo {
public:
  ObDropTenantInfo() : tenant_id_(common::OB_INVALID_TENANT_ID), schema_version_(common::OB_INVALID_VERSION)
  {}
  virtual ~ObDropTenantInfo(){};
  void reset();
  bool is_valid() const;
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  TO_STRING_KV(K_(tenant_id), K_(schema_version));

private:
  uint64_t tenant_id_;
  int64_t schema_version_;
};

struct ObIndexTableStat {
  ObIndexTableStat()
      : index_id_(common::OB_INVALID_ID), index_status_(share::schema::INDEX_STATUS_UNAVAILABLE), is_drop_schema_(false)
  {}
  ObIndexTableStat(const uint64_t index_id, const share::schema::ObIndexStatus index_status, const bool is_drop_schema)
      : index_id_(index_id), index_status_(index_status), is_drop_schema_(is_drop_schema)
  {}
  TO_STRING_KV(K_(index_id), K_(index_status), K_(is_drop_schema));
  uint64_t index_id_;
  share::schema::ObIndexStatus index_status_;
  bool is_drop_schema_;
};

inline bool is_final_invalid_index_status(const ObIndexStatus index_status, const bool is_drop_schema)
{
  bool ret_bool = !(index_status >= INDEX_STATUS_UNAVAILABLE && index_status <= INDEX_STATUS_UNIQUE_CHECKING);

  if (!ret_bool) {
    ret_bool = is_drop_schema;
  }
  return ret_bool;
}

inline bool is_final_index_status(const ObIndexStatus index_status, const bool is_dropped_schema)
{
  return INDEX_STATUS_AVAILABLE == index_status || INDEX_STATUS_INDEX_ERROR == index_status ||
         INDEX_STATUS_RESTORE_INDEX_ERROR == index_status || INDEX_STATUS_UNUSABLE == index_status || is_dropped_schema;
}

inline bool is_error_index_status(const ObIndexStatus index_status, const bool is_dropped_schema)
{
  return INDEX_STATUS_INDEX_ERROR == index_status || INDEX_STATUS_RESTORE_INDEX_ERROR == index_status ||
         INDEX_STATUS_UNUSABLE == index_status || is_dropped_schema;
}

inline bool is_available_index_status(const ObIndexStatus index_status)
{
  return INDEX_STATUS_AVAILABLE == index_status;
}

const char *ob_index_status_str(ObIndexStatus status);

struct ObTenantTableId {
  ObTenantTableId() : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID)
  {}
  ObTenantTableId(const uint64_t tenant_id, const uint64_t table_id) : tenant_id_(tenant_id), table_id_(table_id)
  {}
  bool operator==(const ObTenantTableId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (table_id_ == rv.table_id_);
  }
  ObTenantTableId &operator=(const ObTenantTableId &tenant_table_id);
  int64_t hash() const
  {
    return table_id_;
  }
  bool operator<(const ObTenantTableId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = table_id_ < rv.table_id_;
    }
    return res;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != table_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(table_id));

  uint64_t tenant_id_;
  uint64_t table_id_;
};

struct ObTenantDatabaseId {
  ObTenantDatabaseId() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID)
  {}
  ObTenantDatabaseId(const uint64_t tenant_id, const uint64_t database_id)
      : tenant_id_(tenant_id), database_id_(database_id)
  {}
  bool operator==(const ObTenantDatabaseId &rv) const
  {
    return ((tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_));
  }
  int64_t hash() const
  {
    return database_id_;
  }
  bool operator<(const ObTenantDatabaseId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = database_id_ < rv.database_id_;
    }
    return res;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != database_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(database_id));

  uint64_t tenant_id_;
  uint64_t database_id_;
};

struct ObTenantTablegroupId {
  ObTenantTablegroupId() : tenant_id_(common::OB_INVALID_ID), tablegroup_id_(common::OB_INVALID_ID)
  {}
  ObTenantTablegroupId(const uint64_t tenant_id, const uint64_t tablegroup_id)
      : tenant_id_(tenant_id), tablegroup_id_(tablegroup_id)
  {}
  bool operator==(const ObTenantTablegroupId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (tablegroup_id_ == rv.tablegroup_id_);
  }
  int64_t hash() const
  {
    return tablegroup_id_;
  }
  bool operator<(const ObTenantTablegroupId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = tablegroup_id_ < rv.tablegroup_id_;
    }
    return res;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != tablegroup_id_);
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    tablegroup_id_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(tenant_id), K_(tablegroup_id));

  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
};

typedef enum {
  TENANT_SCHEMA = 0,
  OUTLINE_SCHEMA = 1,
  USER_SCHEMA = 2,
  DATABASE_SCHEMA = 3,
  TABLEGROUP_SCHEMA = 4,
  TABLE_SCHEMA = 5,
  DATABASE_PRIV = 6,
  TABLE_PRIV = 7,
  ROUTINE_SCHEMA = 8,
  SYNONYM_SCHEMA = 9,
  PLAN_BASELINE_SCHEMA = 10,
  PACKAGE_SCHEMA = 12,
  UDF_SCHEMA = 13,
  SEQUENCE_SCHEMA = 14,
  SYS_VARIABLE_SCHEMA = 15,
  UDT_SCHEMA = 16,
  // At present, only liboblog constructs simple table schema in real time in lazy mode, ob does not use it temporarily
  TABLE_SIMPLE_SCHEMA = 17,
  TABLESPACE_SCHEMA = 18,
  TRIGGER_SCHEMA = 19,
  KEYSTORE_SCHEMA = 20,
  PROFILE_SCHEMA = 25,
  AUDIT_SCHEMA = 26,  // UNUSED
  SYS_PRIV = 27,
  OBJ_PRIV = 28,
  DBLINK_SCHEMA = 29,
  LINK_TABLE_SCHEMA = 30,
  ///<<< add schema type before this line
  OB_MAX_SCHEMA
} ObSchemaType;

// The user tenant system table schema is taken from the system tenant, and the schema_id needs to be changed
int need_change_schema_id(const ObSchemaType schema_type, const uint64_t schema_id, bool &need_change);

const char *schema_type_str(const ObSchemaType schema_type);

bool is_normal_schema(const ObSchemaType schema_type);

struct ObSchemaStatisticsInfo {
  ObSchemaStatisticsInfo(ObSchemaType schema_type, int64_t size = 0, int64_t count = 0)
      : schema_type_(schema_type), size_(size), count_(count)
  {}
  ObSchemaStatisticsInfo() : schema_type_(OB_MAX_SCHEMA), size_(0), count_(0)
  {}
  ~ObSchemaStatisticsInfo()
  {}
  bool is_valid() const
  {
    return OB_MAX_SCHEMA != schema_type_;
  }
  void reset()
  {
    schema_type_ = OB_MAX_SCHEMA;
    size_ = 0;
    count_ = 0;
  }
  TO_STRING_KV(K_(schema_type), K_(size), K_(count));
  ObSchemaType schema_type_;
  int64_t size_;
  int64_t count_;
};

class ObPartition;
class PartIdPartitionArrayCmp {
public:
  PartIdPartitionArrayCmp() : ret_(common::OB_SUCCESS)
  {}
  bool operator()(const share::schema::ObPartition *left, const share::schema::ObPartition *right);

public:
  int get_ret() const
  {
    return ret_;
  }

private:
  int ret_;
};

class ObSubPartition;
class SubPartIdPartitionArrayCmp {
public:
  SubPartIdPartitionArrayCmp() : ret_(common::OB_SUCCESS)
  {}
  bool operator()(const share::schema::ObSubPartition *left, const share::schema::ObSubPartition *right);

public:
  int get_ret() const
  {
    return ret_;
  }

private:
  int ret_;
};

struct ObSimpleTableSchema {
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  uint64_t data_table_id_;
  common::ObString table_name_;
  int64_t schema_version_;
  ObTableType table_type_;
  ObSimpleTableSchema()
      : tenant_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        tablegroup_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        data_table_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        table_type_(MAX_TABLE_TYPE)
  {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    tablegroup_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    data_table_id_ = common::OB_INVALID_ID;
    table_name_.reset();
    schema_version_ = common::OB_INVALID_VERSION;
    table_type_ = MAX_TABLE_TYPE;
  }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(tablegroup_id), K_(table_id), K_(data_table_id), K_(table_name),
      K_(schema_version), K_(table_type));
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != database_id_ &&
            common::OB_INVALID_ID != tablegroup_id_ && common::OB_INVALID_ID != table_id_ &&
            common::OB_INVALID_ID != data_table_id_ && !table_name_.empty() && schema_version_ >= 0 &&
            table_type_ != MAX_TABLE_TYPE);
  }
};

enum TableStatus {
  TABLE_STATUS_INVALID = -1,
  // The current schema version does not exist, it is a future table
  TABLE_NOT_CREATE,
  TABLE_EXIST,    // table exist
  TABLE_DELETED,  // table is deleted
};

enum TenantStatus {
  TENANT_STATUS_INVALID = -1,
  TENANT_NOT_CREATE,
  TENANT_EXIST,
  TENANT_DELETED,
};

inline const char *print_table_status(const TableStatus status_no)
{
  const char *status_str = "UNKNOWN";
  switch (status_no) {
    case TABLE_NOT_CREATE:
      status_str = "TABLE_NOT_CREATE";
      break;
    case TABLE_EXIST:
      status_str = "TABLE_EXIST";
      break;
    case TABLE_DELETED:
      status_str = "TABLE_DELETED";
      break;
    default:
      status_str = "UNKNOWN";
      break;
  }
  return status_str;
}

enum PartitionStatus {
  PART_STATUS_INVALID = -1,
  // The current schema version has not been created, it is a future partition
  PART_NOT_CREATE,
  PART_EXIST,    // schema exist
  PART_DELETED,  // partition is deleted
};

inline const char *print_part_status(const PartitionStatus status)
{
  const char *str = "UNKNOWN";
  switch (status) {
    case PART_STATUS_INVALID:
      str = "PART_STATUS_INVALID";
      break;
    case PART_NOT_CREATE:
      str = "PART_NOT_CREATE";
      break;
    case PART_EXIST:
      str = "PART_EXIST";
      break;
    case PART_DELETED:
      str = "PART_DELETED";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

enum ObDependencyTableType {
  DEPENDENCY_INVALID = 0,
  DEPENDENCY_TABLE = 1,
  DEPENDENCY_VIEW = 2,
  DEPENDENCY_SYNONYM = 3,
  DEPENDENCY_PROCEDURE = 4,
  DEPENDENCY_OUTLINE = 5,
  DEPENDENCY_FUNCTION = 6,
  DEPENDENCY_PACKAGE = 7,
  DEPENDENCY_PACKAGE_BODY = 8,
  DEPENDENCY_SEQUENCE = 9,
  DEPENDENCY_TYPE = 10,
  DEPENDENCY_TABLESPACE = 15,
  DEPENDENCY_UDT = 16,
  DEPENDENCY_UDT_BODY = 17
};

enum class ObObjectType {
  INVALID = 0,
  TABLE,
  SEQUENCE,
  PACKAGE,
  TYPE,
  PACKAGE_BODY,
  TYPE_BODY,
  TRIGGER,
  VIEW,
  FUNCTION,
  DIRECTORY,
  INDEX,
  PROCEDURE,
  SYNONYM,
  MAX_TYPE,
};
struct ObSchemaObjVersion {
  ObSchemaObjVersion()
      : object_id_(common::OB_INVALID_ID),
        version_(0),
        // The default is table, which is compatible with the current logic
        object_type_(DEPENDENCY_TABLE),
        is_db_explicit_(false),
        is_existed_(true)
  {}

  ObSchemaObjVersion(int64_t object_id, int64_t version, ObDependencyTableType object_type)
      : object_id_(object_id), version_(version), object_type_(object_type), is_db_explicit_(false), is_existed_(true)
  {}

  inline void reset()
  {
    object_id_ = common::OB_INVALID_ID;
    version_ = 0;
    // The default is table, which is compatible with the current logic
    object_type_ = DEPENDENCY_TABLE;
    is_db_explicit_ = false;
    is_existed_ = true;
  }
  inline int64_t get_object_id() const
  {
    return object_id_;
  }
  inline int64_t get_version() const
  {
    return version_;
  }
  inline ObDependencyTableType get_type() const
  {
    return object_type_;
  }
  inline bool operator==(const ObSchemaObjVersion &other) const
  {
    return object_id_ == other.object_id_ && version_ == other.version_ && object_type_ == other.object_type_ &&
           is_db_explicit_ == other.is_db_explicit_ && is_existed_ == other.is_existed_;
  }
  inline bool operator!=(const ObSchemaObjVersion &other) const
  {
    return !operator==(other);
  }
  ObSchemaType get_schema_type() const
  {
    ObSchemaType ret_type = OB_MAX_SCHEMA;
    switch (object_type_) {
      case DEPENDENCY_TABLE:
      case DEPENDENCY_VIEW:
        ret_type = TABLE_SCHEMA;
        break;
      case DEPENDENCY_SYNONYM:
        ret_type = SYNONYM_SCHEMA;
        break;
      case DEPENDENCY_PROCEDURE:
      case DEPENDENCY_FUNCTION:
        ret_type = ROUTINE_SCHEMA;
        break;
      case DEPENDENCY_PACKAGE:
      case DEPENDENCY_PACKAGE_BODY:
        ret_type = PACKAGE_SCHEMA;
        break;
      case DEPENDENCY_OUTLINE:
        ret_type = OUTLINE_SCHEMA;
        break;
      case DEPENDENCY_SEQUENCE:
        ret_type = SEQUENCE_SCHEMA;
        break;
      case DEPENDENCY_TABLESPACE:
        ret_type = TABLESPACE_SCHEMA;
        break;
      case DEPENDENCY_TYPE:
        ret_type = UDT_SCHEMA;
        break;
      default:
        break;
    }
    return ret_type;
  }
  ObObjectType get_schema_object_type() const
  {
    ObObjectType ret_type = ObObjectType::MAX_TYPE;
    switch (object_type_) {
      case DEPENDENCY_TABLE:
        ret_type = ObObjectType::TABLE;
        break;
      case DEPENDENCY_VIEW:
        ret_type = ObObjectType::VIEW;
        break;
      case DEPENDENCY_PROCEDURE:
        ret_type = ObObjectType::PROCEDURE;
        break;
      case DEPENDENCY_FUNCTION:
        ret_type = ObObjectType::FUNCTION;
        break;
      case DEPENDENCY_PACKAGE:
        ret_type = ObObjectType::PACKAGE;
        break;
      case DEPENDENCY_PACKAGE_BODY:
        ret_type = ObObjectType::PACKAGE_BODY;
        break;
      case DEPENDENCY_SEQUENCE:
        ret_type = ObObjectType::SEQUENCE;
        break;
      case DEPENDENCY_TYPE:
        ret_type = ObObjectType::TYPE;
        break;
      default:
        break;
    }
    return ret_type;
  }
  inline bool is_valid() const
  {
    return common::OB_INVALID_ID != object_id_;
  }
  inline bool is_base_table() const
  {
    return DEPENDENCY_TABLE == object_type_;
  }
  inline bool is_synonym() const
  {
    return DEPENDENCY_SYNONYM == object_type_;
  }
  inline bool is_procedure() const
  {
    return DEPENDENCY_PROCEDURE == object_type_;
  }
  inline bool is_db_explicit() const
  {
    return is_db_explicit_;
  }
  inline bool is_existed() const
  {
    return is_existed_;
  }

  int64_t object_id_;
  int64_t version_;
  ObDependencyTableType object_type_;
  bool is_db_explicit_;
  bool is_existed_;

  TO_STRING_KV(N_TID, object_id_, N_SCHEMA_VERSION, version_, K_(object_type), K_(is_db_explicit), K_(is_existed));
  OB_UNIS_VERSION(1);
};

struct ObSysParam {
  ObSysParam();
  ~ObSysParam();

  int init(const uint64_t tenant_id, const common::ObZone &zone, const common::ObString &name, const int64_t data_type,
      const common::ObString &value, const common::ObString &min_val, const common::ObString &max_val,
      const common::ObString &info, const int64_t flags);
  void reset();
  inline bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  uint64_t tenant_id_;
  common::ObZone zone_;
  char name_[common::OB_MAX_SYS_PARAM_NAME_LENGTH];
  int64_t data_type_;
  char value_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char min_val_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char max_val_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char info_[common::OB_MAX_SYS_PARAM_INFO_LENGTH];
  int64_t flags_;
};

bool ObSysParam::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_;
}
typedef common::ObFixedBitSet<common::OB_MAX_USER_DEFINED_COLUMNS_COUNT> ColumnReferenceSet;

// this is used for schema, and zone in this is a ObString,
// Caution: zone_ here doesn't have buffer, users should manange
//          the buffer memory all by themselves.
struct ObZoneScore {
  ObZoneScore(common::ObString &zone, int64_t score) : zone_(zone), score_(score)
  {}
  ObZoneScore() : zone_(), score_(INT64_MAX)
  {}
  virtual ~ObZoneScore()
  {}
  bool operator<(const ObZoneScore &that)
  {
    return score_ < that.score_;
  }
  void reset()
  {
    zone_.reset();
    score_ = INT64_MAX;
  }
  TO_STRING_KV(K(zone_), K(score_));

  common::ObString zone_;
  int64_t score_;
};
// ObZoneRegion is used to construct the primary zone array of TableSchema/DataBaseSchema/TenantSchema.
// It is only an intermediate variable during the construction process. ObZoneRegion will not be saved
// in the schema eventually. This structure saves the zone and the region in which the zone is located.
struct ObZoneRegion {
  ObZoneRegion() : zone_(), region_()
  {}
  ObZoneRegion(const ObZoneRegion &that) : zone_(that.zone_), region_(that.region_)
  {}
  ObZoneRegion(const common::ObZone &zone, const common::ObRegion &region) : zone_(zone), region_(region)
  {}
  virtual ~ObZoneRegion()
  {}
  void reset()
  {
    zone_.reset();
    region_.reset();
  }
  int assign(const ObZoneRegion &that)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(zone_.assign(that.zone_.ptr()))) {
      SHARE_LOG(WARN, "fail to assign zone", K(ret));
    } else if (OB_FAIL(region_.assign(that.region_.ptr()))) {
      SHARE_LOG(WARN, "fail to assign region", K(ret));
    } else {
    }
    return ret;
  }
  TO_STRING_KV(K(zone_), K(region_));

  common::ObZone zone_;
  common::ObRegion region_;
};

typedef common::ObIArray<share::ObZoneReplicaAttrSet> ZoneLocalityIArray;
typedef common::ObArrayHelper<share::SchemaZoneReplicaAttrSet> ZoneLocalityArray;

class ObCompareNameWithTenantID {
public:
  ObCompareNameWithTenantID()
      : tenant_id_(common::OB_INVALID_ID),
        name_case_mode_(common::OB_NAME_CASE_INVALID),
        database_id_(common::OB_INVALID_ID)
  {}
  ObCompareNameWithTenantID(uint64_t tenant_id)
      : tenant_id_(tenant_id), name_case_mode_(common::OB_NAME_CASE_INVALID), database_id_(common::OB_INVALID_ID)
  {}
  ObCompareNameWithTenantID(uint64_t tenant_id, common::ObNameCaseMode mode)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_id_(common::OB_INVALID_ID)
  {}
  ObCompareNameWithTenantID(uint64_t tenant_id, common::ObNameCaseMode mode, uint64_t database_id)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_id_(database_id)
  {}
  ~ObCompareNameWithTenantID()
  {}
  int compare(const common::ObString &str1, const common::ObString &str2);

private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  uint64_t database_id_;
};

class ObSchema {
public:
  friend class ObLocality;
  friend class ObPrimaryZone;
  ObSchema();
  // explicit ObSchema(common::ObDataBuffer &buffer);
  explicit ObSchema(common::ObIAllocator *allocator);
  virtual ~ObSchema();
  virtual void reset();
  virtual bool is_valid() const
  {
    return common::OB_SUCCESS == error_ret_;
  }
  virtual int zone_array2str(
      const common::ObIArray<common::ObZone> &zone_list, char *str, const int64_t buf_size) const;
  virtual int string_array2str(
      const common::ObIArray<common::ObString> &string_array, char *buf, const int64_t buf_size) const;
  virtual int str2string_array(const char *str, common::ObIArray<common::ObString> &string_array) const;

  virtual int64_t get_convert_size() const
  {
    return 0;
  };
  template <typename DSTSCHEMA>
  static int set_charset_and_collation_options(
      common::ObCharsetType src_charset_type, common::ObCollationType src_collation_type, DSTSCHEMA &dst);
  static common::ObCollationType get_cs_type_with_cmp_mode(const common::ObNameCaseMode mode);

  ObSchema *get_buffer() const
  {
    return buffer_;
  }
  void set_buffer(ObSchema *buffer)
  {
    buffer_ = buffer;
  }
  common::ObIAllocator *get_allocator();
  void reset_allocator();
  int get_assign_ret()
  {
    return error_ret_;
  }
  inline int get_err_ret() const
  {
    return error_ret_;
  }

protected:
  static const int64_t STRING_ARRAY_EXTEND_CNT = 7;
  void *alloc(const int64_t size);
  void free(void *ptr);
  int deep_copy_str(const char *src, common::ObString &dest);
  int deep_copy_str(const common::ObString &src, common::ObString &dest);
  int deep_copy_obj(const common::ObObj &src, common::ObObj &dest);
  int deep_copy_string_array(
      const common::ObIArray<common::ObString> &src, common::ObArrayHelper<common::ObString> &dst);
  int add_string_to_array(const common::ObString &str, common::ObArrayHelper<common::ObString> &str_array);
  int serialize_string_array(
      char *buf, const int64_t buf_len, int64_t &pos, const common::ObArrayHelper<common::ObString> &str_array) const;
  int deserialize_string_array(
      const char *buf, const int64_t data_len, int64_t &pos, common::ObArrayHelper<common::ObString> &str_array);
  int64_t get_string_array_serialize_size(const common::ObArrayHelper<common::ObString> &str_array) const;
  void reset_string(common::ObString &str);
  void reset_string_array(common::ObArrayHelper<common::ObString> &str_array);
  const char *extract_str(const common::ObString &str) const;
  // buffer is the memory used to store schema item, if not same with this pointer,
  // it means that this schema item has already been rewrote to buffer when rewriting
  // other schema manager, and when we want to rewrite this schema item in current schema
  // manager, we just set pointer of schema item in schema manager to buffer
  ObSchema *buffer_;
  int error_ret_;
  bool is_inner_allocator_;
  common::ObIAllocator *allocator_;
};

template <typename DSTSCHEMA>
int ObSchema::set_charset_and_collation_options(
    common::ObCharsetType src_charset_type, common::ObCollationType src_collation_type, DSTSCHEMA &dst)
{
  int ret = common::OB_SUCCESS;
  if (dst.get_charset_type() == common::CHARSET_INVALID && dst.get_collation_type() == common::CS_TYPE_INVALID) {
    // use upper layer schema's charset and collation type
    if (src_charset_type == common::CHARSET_INVALID || src_collation_type == common::CS_TYPE_INVALID) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "charset type or collation type is invalid ", K(ret));
    } else {
      dst.set_charset_type(src_charset_type);
      dst.set_collation_type(src_collation_type);
    }
  } else {
    common::ObCharsetType charset_type = dst.get_charset_type();
    common::ObCollationType collation_type = dst.get_collation_type();
    if (OB_FAIL(common::ObCharset::check_and_fill_info(charset_type, collation_type))) {
      SHARE_SCHEMA_LOG(WARN, "fail to check charset collation", K(charset_type), K(collation_type), K(ret));
    } else {
      dst.set_charset_type(charset_type);
      dst.set_collation_type(collation_type);
    }
  }
  if (common::OB_SUCCESS == ret &&
      !common::ObCharset::is_valid_collation(dst.get_charset_type(), dst.get_collation_type())) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid collation!", K(dst.get_charset_type()), K(dst.get_collation_type()), K(ret));
  }
  return ret;
}

class ObLocality {
  OB_UNIS_VERSION(1);

public:
  explicit ObLocality(ObSchema *schema) : schema_(schema)
  {}
  int assign(const ObLocality &other);
  int set_locality_str(const common::ObString &locality);
  int set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet> &src);
  int set_zone_replica_attr_array(const common::ObIArray<share::SchemaZoneReplicaAttrSet> &src);
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray &schema_replica_set, const common::ObIArray<ReplicaAttr> &src);
  void reset_zone_replica_attr_array();
  int64_t get_convert_size() const;
  inline const common::ObString &get_locality_str() const
  {
    return locality_str_;
  }
  void reset();
  TO_STRING_KV(K_(locality_str), K_(zone_replica_attr_array));

public:
  common::ObString locality_str_;
  ZoneLocalityArray zone_replica_attr_array_;
  ObSchema *schema_;
};

class ObPrimaryZone {
  OB_UNIS_VERSION(1);

public:
  explicit ObPrimaryZone(ObSchema *schema) : schema_(schema)
  {}
  int assign(const ObPrimaryZone &other);
  inline const common::ObString &get_primary_zone() const
  {
    return primary_zone_str_;
  }
  inline const common::ObIArray<ObZoneScore> &get_primary_zone_array() const
  {
    return primary_zone_array_;
  }
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);
  int set_primary_zone(const common::ObString &zone);
  int64_t get_convert_size() const;
  void reset();
  TO_STRING_KV(K_(primary_zone_str), K_(primary_zone_array));

public:
  common::ObString primary_zone_str_;
  common::ObSEArray<ObZoneScore, common::MAX_ZONE_NUM, common::ObNullAllocator> primary_zone_array_;
  ObSchema *schema_;
};

class ObSysVarSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObSysVarSchema() : ObSchema()
  {
    reset();
  }
  ~ObSysVarSchema()
  {}
  explicit ObSysVarSchema(common::ObIAllocator *allocator);
  explicit ObSysVarSchema(const ObSysVarSchema &src_schema);
  ObSysVarSchema &operator=(const ObSysVarSchema &src_schema);
  int assign(const ObSysVarSchema &src_schema);
  virtual bool is_valid() const
  {
    return ObSchema::is_valid() && tenant_id_ != common::OB_INVALID_ID && !name_.empty();
  }
  void reset();
  int64_t get_convert_size() const;
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  const common::ObString &get_name() const
  {
    return name_;
  }
  int set_name(const common::ObString &name)
  {
    return deep_copy_str(name, name_);
  }
  common::ObObjType get_data_type() const
  {
    return data_type_;
  }
  void set_data_type(common::ObObjType data_type)
  {
    data_type_ = data_type;
  }
  const common::ObString &get_value() const
  {
    return value_;
  }
  int get_value(
      common::ObIAllocator *allocator, const common::ObDataTypeCastParams &dtc_params, common::ObObj &value) const;
  inline int get_value(
      common::ObIAllocator *allocator, const common::ObTimeZoneInfo *tz_info, common::ObObj &value) const
  {
    const common::ObDataTypeCastParams dtc_params(tz_info);
    return get_value(allocator, dtc_params, value);
  }
  int set_value(const common::ObString &value)
  {
    return deep_copy_str(value, value_);
  }
  const common::ObString &get_min_val() const
  {
    return min_val_;
  }
  int set_min_val(const common::ObString &min_val)
  {
    return deep_copy_str(min_val, min_val_);
  }
  const common::ObString &get_max_val() const
  {
    return max_val_;
  }
  int set_max_val(const common::ObString &max_val)
  {
    return deep_copy_str(max_val, max_val_);
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  void set_schema_version(int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  int64_t get_flags() const
  {
    return flags_;
  }
  void set_flags(int64_t flags)
  {
    flags_ = flags;
  }
  int set_info(const common::ObString &info)
  {
    return deep_copy_str(info, info_);
  }
  const common::ObString &get_info() const
  {
    return info_;
  }
  int set_zone(const common::ObZone &zone)
  {
    return zone_.assign(zone);
  }
  int set_zone(const common::ObString &zone)
  {
    return zone_.assign(zone);
  }
  const common::ObZone &get_zone() const
  {
    return zone_;
  }
  bool is_invisible() const
  {
    return 0 != (flags_ & ObSysVarFlag::INVISIBLE);
  }
  bool is_global() const
  {
    return 0 != (flags_ & ObSysVarFlag::GLOBAL_SCOPE);
  }
  bool is_query_sensitive() const
  {
    return 0 != (flags_ & ObSysVarFlag::QUERY_SENSITIVE);
  }
  bool is_oracle_only() const
  {
    return 0 != (flags_ & ObSysVarFlag::ORACLE_ONLY);
  }
  bool is_mysql_only() const
  {
    return 0 != (flags_ & ObSysVarFlag::MYSQL_ONLY);
  }
  bool is_read_only() const
  {
    return 0 != (flags_ & ObSysVarFlag::READONLY);
  }
  TO_STRING_KV(K_(tenant_id), K_(name), K_(data_type), K_(value), K_(min_val), K_(max_val), K_(info), K_(zone),
      K_(schema_version), K_(flags));

private:
  uint64_t tenant_id_;
  common::ObString name_;
  common::ObObjType data_type_;
  common::ObString value_;
  common::ObString min_val_;
  common::ObString max_val_;
  common::ObString info_;
  common::ObZone zone_;
  int64_t schema_version_;
  int64_t flags_;
};

class ObSysVariableSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  // base methods
  ObSysVariableSchema();
  explicit ObSysVariableSchema(common::ObIAllocator *allocator);
  virtual ~ObSysVariableSchema();
  ObSysVariableSchema(const ObSysVariableSchema &src_schema);
  ObSysVariableSchema &operator=(const ObSysVariableSchema &src_schema);
  // set methods
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  // get methods
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  void reset_sysvars()
  {
    memset(sysvar_array_, 0, sizeof(sysvar_array_));
  }
  int64_t get_convert_size() const;
  int add_sysvar_schema(const share::schema::ObSysVarSchema &sysvar_schema);
  int load_default_system_variable(bool is_sys_tenant);
  int64_t get_sysvar_count() const
  {
    return ObSysVarFactory::ALL_SYS_VARS_COUNT;
  }
  int64_t get_real_sysvar_count() const;
  int get_sysvar_schema(const common::ObString &sysvar_name, const ObSysVarSchema *&sysvar_schema) const;
  int get_sysvar_schema(ObSysVarClassType var_type, const ObSysVarSchema *&sysvar_schema) const;
  const ObSysVarSchema *get_sysvar_schema(int64_t idx) const;
  ObSysVarSchema *get_sysvar_schema(int64_t idx);
  bool is_read_only() const
  {
    return read_only_;
  }
  common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  void set_name_case_mode(const common::ObNameCaseMode mode)
  {
    name_case_mode_ = mode;
  }
  int get_oracle_mode(bool &is_oracle_mode) const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version), "sysvars",
      common::ObArrayWrap<ObSysVarSchema *>(sysvar_array_, ObSysVarFactory::ALL_SYS_VARS_COUNT), K_(read_only),
      K_(name_case_mode));

private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  ObSysVarSchema *sysvar_array_[ObSysVarFactory::ALL_SYS_VARS_COUNT];
  bool read_only_;
  common::ObNameCaseMode name_case_mode_;
};

enum ObTenantStatus {
  TENANT_STATUS_NORMAL = 0,
  TENANT_STATUS_CREATING = 1,
  TENANT_STATUS_DROPPING = 2,
  TENANT_STATUS_RESTORE = 3,
  TENANT_STATUS_MAX
};

const char *ob_tenant_status_str(const ObTenantStatus);

int get_tenant_status(const common::ObString &str, ObTenantStatus &status);

class ObTenantSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  // base methods
  ObTenantSchema();
  explicit ObTenantSchema(common::ObIAllocator *allocator);
  virtual ~ObTenantSchema();
  ObTenantSchema(const ObTenantSchema &src_schema);
  ObTenantSchema &operator=(const ObTenantSchema &src_schema);
  int assign(const ObTenantSchema &src_schema);
  // for sorted vector
  static bool cmp(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() < rhs->get_tenant_id() : false;
  }
  static bool equal(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() == rhs->get_tenant_id() : false;
  }
  static bool cmp_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  {
    return NULL != lhs ? lhs->get_tenant_id() < tenant_id : false;
  }
  static bool equal_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  {
    return NULL != lhs ? lhs->get_tenant_id() == tenant_id : false;
  }
  // set methods
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int set_tenant_name(const char *tenant_name)
  {
    return deep_copy_str(tenant_name, tenant_name_);
  }
  inline int set_comment(const char *comment)
  {
    return deep_copy_str(comment, comment_);
  }
  inline int set_locality(const char *locality)
  {
    return deep_copy_str(locality, locality_str_);
  }
  inline int set_previous_locality(const char *previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_str_);
  }
  inline int set_tenant_name(const common::ObString &tenant_name)
  {
    return deep_copy_str(tenant_name, tenant_name_);
  }
  inline int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  int set_zone_list(const common::ObIArray<common::ObZone> &zone_list);
  inline int set_primary_zone(const common::ObString &zone);
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);
  inline int add_zone(const common::ObString &zone);
  inline void set_locked(const bool locked)
  {
    locked_ = locked;
  }
  inline void set_read_only(const bool read_only)
  {
    read_only_ = read_only;
  }
  inline void set_rewrite_merge_version(const int64_t version)
  {
    rewrite_merge_version_ = version;
  }
  inline int set_comment(const common::ObString &comment)
  {
    return deep_copy_str(comment, comment_);
  }
  inline int set_locality(const common::ObString &locality)
  {
    return deep_copy_str(locality, locality_str_);
  }
  inline int set_previous_locality(const common::ObString &previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_str_);
  }
  inline void set_charset_type(const common::ObCharsetType type)
  {
    charset_type_ = type;
  }
  inline void set_collation_type(const common::ObCollationType type)
  {
    collation_type_ = type;
  }
  inline void set_name_case_mode(const common::ObNameCaseMode mode)
  {
    name_case_mode_ = mode;
  }
  int set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet> &src);
  int set_zone_replica_attr_array(const common::ObIArray<share::SchemaZoneReplicaAttrSet> &src);
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray &schema_replica_set, const common::ObIArray<ReplicaAttr> &src);
  void reset_zone_replica_attr_array();
  inline void set_storage_format_version(const int64_t storage_format_version);
  inline void set_storage_format_work_version(const int64_t storage_format_work_version);
  void set_default_tablegroup_id(const uint64_t tablegroup_id)
  {
    default_tablegroup_id_ = tablegroup_id;
  }
  int set_default_tablegroup_name(const common::ObString &tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, default_tablegroup_name_);
  }
  inline void set_compatibility_mode(const common::ObCompatibilityMode compatibility_mode)
  {
    compatibility_mode_ = compatibility_mode;
  }

  // get methods
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const char *get_tenant_name() const
  {
    return extract_str(tenant_name_);
  }
  inline const char *get_comment() const
  {
    return extract_str(comment_);
  }
  inline const char *get_locality() const
  {
    return extract_str(locality_str_);
  }
  inline const char *get_previous_locality() const
  {
    return extract_str(previous_locality_str_);
  }
  inline const common::ObString &get_tenant_name_str() const
  {
    return tenant_name_;
  }
  inline int64_t get_logonly_replica_num() const
  {
    return logonly_replica_num_;
  }
  inline const common::ObString &get_primary_zone() const
  {
    return primary_zone_;
  }
  inline bool get_locked() const
  {
    return locked_;
  }
  inline bool is_read_only() const
  {
    return read_only_;
  }
  inline int64_t get_rewrite_merge_version() const
  {
    return rewrite_merge_version_;
  }
  inline const common::ObString &get_comment_str() const
  {
    return comment_;
  }
  inline const common::ObString &get_locality_str() const
  {
    return locality_str_;
  }
  inline const common::ObString &get_previous_locality_str() const
  {
    return previous_locality_str_;
  }
  inline common::ObCharsetType get_charset_type() const
  {
    return charset_type_;
  }
  inline common::ObCollationType get_collation_type() const
  {
    return collation_type_;
  }
  int get_raw_first_primary_zone(
      const rootserver::ObRandomZoneSelector &random_selector, common::ObZone &first_primary_zone) const;
  int get_first_primary_zone(const rootserver::ObRandomZoneSelector &random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs, common::ObZone &first_primary_zone) const;
  int get_primary_zone_score(const common::ObZone &zone, int64_t &zone_score) const;
  inline const common::ObIArray<ObZoneScore> &get_primary_zone_array() const
  {
    return primary_zone_array_;
  }
  int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard, share::schema::ObPrimaryZone &primary_zone) const;
  int get_zone_replica_attr_array(ZoneLocalityIArray &locality) const;
  int get_zone_replica_attr_array_inherit(ObSchemaGetterGuard &schema_guard, ZoneLocalityIArray &locality) const;
  int64_t get_full_replica_num() const;
  int get_paxos_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const;
  int get_zone_list(common::ObIArray<common::ObZone> &zone_list) const;
  int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard, common::ObIArray<common::ObZone> &zone_list) const;
  inline int64_t get_storage_format_version() const
  {
    return storage_format_version_;
  }
  inline int64_t get_storage_format_work_version() const
  {
    return storage_format_work_version_;
  }
  inline uint64_t get_default_tablegroup_id() const
  {
    return default_tablegroup_id_;
  }
  inline const common::ObString &get_default_tablegroup_name() const
  {
    return default_tablegroup_name_;
  }
  inline common::ObCompatibilityMode get_compatibility_mode() const
  {
    return compatibility_mode_;
  }
  inline void set_drop_tenant_time(const int64_t drop_tenant_time)
  {
    drop_tenant_time_ = drop_tenant_time;
  }
  inline int64_t get_drop_tenant_time() const
  {
    return drop_tenant_time_;
  }
  inline bool is_dropping() const
  {
    return TENANT_STATUS_DROPPING == status_;
  }
  inline bool is_in_recyclebin() const
  {
    return in_recyclebin_;
  }
  inline void set_in_recyclebin(const bool in_recyclebin)
  {
    in_recyclebin_ = in_recyclebin;
  }
  inline bool is_creating() const
  {
    return TENANT_STATUS_CREATING == status_;
  }
  inline bool is_restore() const
  {
    return TENANT_STATUS_RESTORE == status_;
  }
  inline bool is_normal() const
  {
    return TENANT_STATUS_NORMAL == status_;
  }
  inline void set_status(const ObTenantStatus status)
  {
    status_ = status;
  }
  inline ObTenantStatus get_status() const
  {
    return status_;
  }
  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  void reset_sysvars()
  {
    memset(sysvar_array_, 0, sizeof(sysvar_array_));
  }
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version), K_(tenant_name), K_(replica_num), K_(zone_list), K_(primary_zone),
      K_(charset_type), K_(locked), K_(comment), K_(name_case_mode), K_(read_only), K_(rewrite_merge_version),
      K_(locality_str), K_(logonly_replica_num), K_(zone_replica_attr_array), K_(primary_zone_array),
      K_(storage_format_version), K_(storage_format_work_version), K_(previous_locality_str), "sysvars",
      common::ObArrayWrap<ObSysVarSchema *>(sysvar_array_, ObSysVarFactory::ALL_SYS_VARS_COUNT),
      K_(default_tablegroup_id), K_(default_tablegroup_name), K_(compatibility_mode), K_(drop_tenant_time), K_(status),
      K_(in_recyclebin));

private:
  int add_sysvar_schema(const share::schema::ObSysVarSchema &sysvar_schema);
  int64_t get_sysvar_count() const
  {
    return ObSysVarFactory::ALL_SYS_VARS_COUNT;
  }
  const ObSysVarSchema *get_sysvar_schema(int64_t idx) const;

private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObString tenant_name_;
  int64_t replica_num_;
  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;
  bool locked_;
  bool read_only_;  // After the schema is split, the value of the system variable shall prevail
  int64_t rewrite_merge_version_;
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  common::ObNameCaseMode name_case_mode_;  // deprecated
  common::ObString comment_;
  common::ObString locality_str_;
  int64_t logonly_replica_num_;
  ZoneLocalityArray zone_replica_attr_array_;
  // primary_zone_ is primary zone list,
  // The following is the parsed array of a single zone, which has been sorted according to priority
  common::ObSEArray<ObZoneScore, common::MAX_ZONE_NUM, common::ObNullAllocator> primary_zone_array_;
  common::ObString previous_locality_str_;
  int64_t storage_format_version_;
  int64_t storage_format_work_version_;
  ObSysVarSchema *sysvar_array_[ObSysVarFactory::ALL_SYS_VARS_COUNT];  // deprecated
  uint64_t default_tablegroup_id_;
  common::ObString default_tablegroup_name_;
  common::ObCompatibilityMode compatibility_mode_;
  int64_t drop_tenant_time_;
  ObTenantStatus status_;
  bool in_recyclebin_;
};

inline int ObTenantSchema::set_zone_list(const common::ObIArray<common::ObString> &zone_list)
{
  return deep_copy_string_array(zone_list, zone_list_);
}

inline int ObTenantSchema::set_primary_zone(const common::ObString &zone)
{
  return deep_copy_str(zone, primary_zone_);
}

inline int ObTenantSchema::add_zone(const common::ObString &zone)
{
  return add_string_to_array(zone, zone_list_);
}

inline void ObTenantSchema::set_storage_format_version(const int64_t storage_format_version)
{
  storage_format_version_ = storage_format_version;
}

inline void ObTenantSchema::set_storage_format_work_version(const int64_t storage_format_work_version)
{
  storage_format_work_version_ = storage_format_work_version;
}

class ObDatabaseSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  // base methods
  ObDatabaseSchema();
  explicit ObDatabaseSchema(common::ObIAllocator *allocator);
  virtual ~ObDatabaseSchema();
  ObDatabaseSchema(const ObDatabaseSchema &src_schema);
  ObDatabaseSchema &operator=(const ObDatabaseSchema &src_schema);
  int assign(const ObDatabaseSchema &src_schema);
  // set methods
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_database_id(const uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  int set_database_name(const char *database_name)
  {
    return deep_copy_str(database_name, database_name_);
  }
  int set_database_name(const common::ObString &database_name)
  {
    return deep_copy_str(database_name, database_name_);
  }
  inline int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  int set_zone_list(const common::ObIArray<common::ObZone> &zone_list);
  inline int set_primary_zone(const common::ObString &zone);
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);
  inline int add_zone(const common::ObString &zone);
  int set_comment(const char *comment)
  {
    return deep_copy_str(comment, comment_);
  }
  int set_comment(const common::ObString &comment)
  {
    return deep_copy_str(comment, comment_);
  }
  inline void set_charset_type(const common::ObCharsetType type)
  {
    charset_type_ = type;
  }
  inline void set_collation_type(const common::ObCollationType type)
  {
    collation_type_ = type;
  }
  inline void set_name_case_mode(const common::ObNameCaseMode mode)
  {
    name_case_mode_ = mode;
  }
  inline void set_read_only(const bool read_only)
  {
    read_only_ = read_only;
  }
  void set_default_tablegroup_id(const uint64_t tablegroup_id)
  {
    default_tablegroup_id_ = tablegroup_id;
  }
  int set_default_tablegroup_name(const common::ObString &tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, default_tablegroup_name_);
  }
  inline void set_in_recyclebin(const bool in_recyclebin)
  {
    in_recyclebin_ = in_recyclebin;
  }
  inline bool is_hidden() const
  {
    return common::OB_RECYCLEBIN_SCHEMA_ID == common::extract_pure_id(database_id_) ||
           common::OB_PUBLIC_SCHEMA_ID == common::extract_pure_id(database_id_);
  }

  // get methods
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const char *get_database_name() const
  {
    return extract_str(database_name_);
  }
  inline const common::ObString &get_database_name_str() const
  {
    return database_name_;
  }
  inline const common::ObString &get_primary_zone() const
  {
    return primary_zone_;
  }
  inline const char *get_comment() const
  {
    return extract_str(comment_);
  }
  inline const common::ObString &get_comment_str() const
  {
    return comment_;
  }
  inline common::ObCharsetType get_charset_type() const
  {
    return charset_type_;
  }
  inline common::ObCollationType get_collation_type() const
  {
    return collation_type_;
  }
  inline common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  inline bool is_read_only() const
  {
    return read_only_;
  }
  inline uint64_t get_default_tablegroup_id() const
  {
    return default_tablegroup_id_;
  }
  inline const common::ObString &get_default_tablegroup_name() const
  {
    return default_tablegroup_name_;
  }
  inline bool is_in_recyclebin() const
  {
    return in_recyclebin_;
  }
  inline bool is_or_in_recyclebin() const
  {
    return in_recyclebin_ || database_id_ == common::combine_id(tenant_id_, common::OB_RECYCLEBIN_SCHEMA_ID);
  }
  inline const common::ObString &get_locality_str() const
  {
    static const common::ObString dummy;
    return dummy;
  }
  int get_raw_first_primary_zone(
      const rootserver::ObRandomZoneSelector &random_selector, common::ObZone &first_primary_zone) const;
  int get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard &schema_guard,
      const rootserver::ObRandomZoneSelector &random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs, common::ObZone &first_primary_zone) const;
  int get_primary_zone_score(const common::ObZone &zone, int64_t &zone_score) const;
  inline const common::ObIArray<ObZoneScore> &get_primary_zone_array() const
  {
    return primary_zone_array_;
  }
  int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard, share::schema::ObPrimaryZone &primary_zone) const;
  int get_paxos_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const;
  int get_zone_replica_attr_array_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard, ZoneLocalityIArray &locality) const;
  // In the current implementation, the zone_list of the Database is directly read from the corresponding tenant.
  int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard, common::ObIArray<common::ObZone> &zone_list) const;
  // other methods
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual void reset();
  void set_drop_schema_version(const int64_t schema_version)
  {
    drop_schema_version_ = schema_version;
  }
  int64_t get_drop_schema_version() const
  {
    return drop_schema_version_;
  }
  bool is_dropped_schema() const
  {
    return drop_schema_version_ > 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(schema_version), K_(database_name), K_(replica_num), K_(zone_list),
      K_(primary_zone), K_(charset_type), K_(collation_type), K_(name_case_mode), K_(comment), K_(read_only),
      K_(default_tablegroup_id), K_(default_tablegroup_name), K_(in_recyclebin), K_(primary_zone_array),
      K_(drop_schema_version));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_version_;
  common::ObString database_name_;
  int64_t replica_num_;
  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;
  common::ObCharsetType charset_type_;      // default:utf8mb4
  common::ObCollationType collation_type_;  // default:utf8mb4_general_ci
  common::ObNameCaseMode name_case_mode_;   // default:OB_NAME_CASE_INVALID
  common::ObString comment_;
  bool read_only_;
  uint64_t default_tablegroup_id_;
  common::ObString default_tablegroup_name_;
  bool in_recyclebin_;
  // primary_zone_ is primary zone list,
  // The following is the parsed array of a single zone, which has been sorted according to priority
  common::ObSEArray<ObZoneScore, common::MAX_ZONE_NUM, common::ObNullAllocator> primary_zone_array_;
  int64_t drop_schema_version_;
};

inline int ObDatabaseSchema::set_zone_list(const common::ObIArray<common::ObString> &zone_list)
{
  return deep_copy_string_array(zone_list, zone_list_);
}

inline int ObDatabaseSchema::set_primary_zone(const common::ObString &zone)
{
  return deep_copy_str(zone, primary_zone_);
}

inline int ObDatabaseSchema::add_zone(const common::ObString &zone)
{
  return add_string_to_array(zone, zone_list_);
}

class ObPartitionOption : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObPartitionOption();
  explicit ObPartitionOption(common::ObIAllocator *allocator);
  virtual ~ObPartitionOption();
  ObPartitionOption(const ObPartitionOption &expr);
  ObPartitionOption &operator=(const ObPartitionOption &expr);
  bool operator==(const ObPartitionOption &expr) const;
  bool operator!=(const ObPartitionOption &expr) const;

  inline bool is_range_part() const
  {
    return share::schema::is_range_part(part_func_type_);
  }
  inline bool is_hash_part() const
  {
    return share::schema::is_hash_part(part_func_type_);
  }
  inline bool is_hash_like_part() const
  {
    return share::schema::is_hash_like_part(part_func_type_);
  }
  inline bool is_key_part() const
  {
    return share::schema::is_key_part(part_func_type_);
  }
  inline bool is_list_part() const
  {
    return share::schema::is_list_part(part_func_type_);
  }
  inline bool is_auto_range_part() const
  {
    return auto_part_ && is_range_part();
  }

  // set methods
  int set_part_expr(const common::ObString &expr)
  {
    return deep_copy_str(expr, part_func_expr_);
  }
  inline void set_part_num(const int64_t part_num)
  {
    part_num_ = part_num;
  }
  inline void set_part_func_type(const ObPartitionFuncType func_type)
  {
    part_func_type_ = func_type;
  }
  inline void set_sub_part_func_type(const ObPartitionFuncType func_type)
  {
    part_func_type_ = func_type;
  }
  inline void set_max_used_part_id(const int64_t max_used_part_id)
  {
    max_used_part_id_ = max_used_part_id;
  }
  inline void set_partition_cnt_within_partition_table(const int64_t partition_cnt_within_partition_table)
  {
    partition_cnt_within_partition_table_ = partition_cnt_within_partition_table;
  }
  inline void set_auto_part(const bool auto_part)
  {
    auto_part_ = auto_part;
  }
  inline void set_auto_part_size(const int64_t auto_part_size)
  {
    auto_part_size_ = auto_part_size;
  }

  // get methods
  inline const common::ObString &get_part_func_expr_str() const
  {
    return part_func_expr_;
  }
  inline const char *get_part_func_expr() const
  {
    return extract_str(part_func_expr_);
  }
  inline int64_t get_part_num() const
  {
    return part_num_;
  }
  inline int64_t get_part_space() const
  {
    return part_space_;
  }
  inline ObPartitionFuncType get_part_func_type() const
  {
    return part_func_type_;
  }
  inline ObPartitionFuncType get_sub_part_func_type() const
  {
    return part_func_type_;
  }
  inline int64_t get_max_used_part_id() const
  {
    return max_used_part_id_;
  }
  inline int64_t get_partition_cnt_within_partition_table() const
  {
    return partition_cnt_within_partition_table_;
  }
  bool is_columns() const
  {
    return is_columns_;
  }
  const common::ObString &get_intervel_start_str() const
  {
    return interval_start_;
  }
  const common::ObString &get_part_intervel_str() const
  {
    return part_interval_;
  }
  inline int64_t get_auto_part_size() const
  {
    return auto_part_size_;
  }

  // other methods
  virtual void reset();
  int64_t assign(const ObPartitionOption &src_part);
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  TO_STRING_KV(K_(part_func_type), K_(part_func_expr), K_(part_num), K_(partition_cnt_within_partition_table),
      K_(max_used_part_id), K_(auto_part), K_(auto_part_size));

private:
  ObPartitionFuncType part_func_type_;
  common::ObString part_func_expr_;
  int64_t part_num_;
  int32_t part_space_;               // Partition id space, see document for details
  bool is_columns_;                  // Whether it is the range columns/list columns partition
  common::ObString interval_start_;  // interval start value

  common::ObString part_interval_;  // interval partition step
  int64_t max_used_part_id_;
  int64_t partition_cnt_within_partition_table_;
  bool auto_part_;          // Whether it is automatic partition
  int64_t auto_part_size_;  // Automatic partition size, 0 is auto
};

// For any questions about the role of this structure, please contact @jiage
class ObSchemaAllocator : public common::ObIAllocator {
public:
  ObSchemaAllocator() : allocator_(NULL)
  {}
  ObSchemaAllocator(common::ObIAllocator &allocator) : allocator_(&allocator)
  {}

  virtual void* alloc(const int64_t sz) override
  {
    return alloc(sz, common::default_memattr);
  }

  virtual void* alloc(const int64_t sz, const common::ObMemAttr& attr) override
  {
    void *ret = NULL;
    if (allocator_) {
      ret = allocator_->alloc(sz, attr);
    }
    return ret;
  }

  virtual void free(void *p) override
  {
    allocator_->free(p);
  }
  virtual ~ObSchemaAllocator(){};

private:
  common::ObIAllocator *allocator_;
};

// May need to add a schema_version field, this mark when the table was created
// If it is at the table level, need not to add it, just use the table level.
// This level is added. Indicates that the partition level is considered.
// Then it is to modify the subpartition of a single primary partition without affecting other things
struct InnerPartListVectorCmp {
public:
  InnerPartListVectorCmp() : ret_(common::OB_SUCCESS)
  {}

public:
  bool operator()(const common::ObNewRow &left, const common::ObNewRow &right)
  {
    bool bool_ret = false;
    int cmp = 0;
    if (common::OB_SUCCESS != ret_) {
      // failed before
    } else if (common::OB_SUCCESS != (ret_ = common::ObRowUtil::compare_row(left, right, cmp))) {
      SHARE_SCHEMA_LOG(ERROR, "l or r is invalid", K(ret_));
    } else {
      bool_ret = (cmp < 0);
    }
    return bool_ret;
  }
  int get_ret() const
  {
    return ret_;
  }

private:
  int ret_;
};

class ObPartitionUtils;
struct ObBasePartition : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  friend class ObPartitionUtils;
  friend class sql::ObPartitionExecutorUtils;
  ObBasePartition();
  explicit ObBasePartition(common::ObIAllocator *allocator);
  virtual void reset();
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }

  void set_table_id(const uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }

  void set_part_id(const int64_t part_id)
  {
    part_id_ = part_id;
  }
  int64_t get_part_id() const
  {
    return part_id_;
  }

  void set_part_idx(const int64_t part_idx)
  {
    part_idx_ = part_idx;
  }
  int64_t get_part_idx() const
  {
    return part_idx_;
  }

  void set_schema_version(int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }

  int set_part_name(common::ObString &part_name)
  {
    return deep_copy_str(part_name, name_);
  }
  const common::ObString &get_part_name() const
  {
    return name_;
  }

  void set_tablespace_id(const int64_t tablespace_id)
  {
    tablespace_id_ = tablespace_id;
  }
  int64_t get_tablespace_id() const
  {
    return tablespace_id_;
  }

  int assign(const ObBasePartition &src_part);

  // This interface is not strictly semantically less than, please note
  static bool less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool list_part_func_layout(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool range_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool hash_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  int add_list_row(const common::ObNewRow &row)
  {
    return list_row_values_.push_back(row);
  }
  int set_high_bound_val(const common::ObRowkey &high_bound_val);
  int set_high_bound_val_with_hex_str(const common::ObString &high_bound_val_hex);
  int set_list_vector_values_with_hex_str(const common::ObString &list_vector_vals_hex);
  const common::ObRowkey &get_high_bound_val() const
  {
    return high_bound_val_;
  }
  virtual int64_t get_deep_copy_size() const;

  const common::ObIArray<common::ObNewRow> &get_list_row_values() const
  {
    return list_row_values_;
  }

  bool same_base_partition(const ObBasePartition &other) const
  {
    return part_id_ == other.part_id_ && high_bound_val_ == other.high_bound_val_ && status_ == other.status_;
  }
  bool allow_ddl_operator() const
  {
    bool ret = true;
    if (1 == source_part_ids_.count() && -1 != source_part_ids_.at(0)) {
      ret = false;
    }
    return ret;
  }
  ObPartitionStatus get_status() const
  {
    return status_;
  }
  const common::ObIArray<int64_t> &get_source_part_ids() const
  {
    return source_part_ids_;
  }
  common::ObIArray<int64_t> &get_source_part_ids()
  {
    return source_part_ids_;
  }
  int set_source_part_id(common::ObString &partition_str);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(table_id), K_(part_id), K_(name), K_(high_bound_val), K_(list_row_values),
      K_(part_idx), K_(is_empty_partition_name));
  int get_source_part_ids_str(char *str, const int64_t buf_len) const;
  void set_is_empty_partition_name(bool is_empty)
  {
    is_empty_partition_name_ = is_empty;
  }
  bool is_empty_partition_name() const
  {
    return is_empty_partition_name_;
  }

protected:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t part_id_;
  int64_t schema_version_;
  common::ObString name_;
  common::ObRowkey high_bound_val_;
  ObSchemaAllocator schema_allocator_;
  common::ObSEArray<common::ObNewRow, 8> list_row_values_;
  enum ObPartitionStatus status_;
  int64_t spare1_;  // TODO Consider whether it is really useful
  int64_t spare2_;
  int64_t spare3_;
  /**
   * @warning: The projector can only be used by the less than interface for partition comparison calculations,
   *  and it is not allowed to be used in other places
   */
  int32_t *projector_;
  int64_t projector_size_;
  common::ObSEArray<int64_t, 2> source_part_ids_;
  int64_t part_idx_;  // Valid for hash partition
  // The partition management operation of tablegroup, because after adding pg,
  // the operation of tablegroup needs to be processed first
  // Under Oracle tenants, partition_name is allowed to be empty. There may be a partition name conflict
  // when filling the tablegroup first and directly copying it to the table. Therefore, add a variable
  // when copying to mark this as an empty partition name and do not copy the partition name.
  bool is_empty_partition_name_;
  int64_t tablespace_id_;
};

struct ObPartition : public ObBasePartition {
  OB_UNIS_VERSION(1);

public:
  ObPartition();
  explicit ObPartition(common::ObIAllocator *allocator);
  int assign(const ObPartition &src_part);

  void reset();
  virtual int64_t get_convert_size() const;
  virtual int clone(common::ObIAllocator &allocator, ObPartition *&dst) const;

  void set_sub_part_num(const int64_t sub_part_num)
  {
    sub_part_num_ = sub_part_num;
  }
  int64_t get_sub_part_num() const
  {
    return sub_part_num_;
  }
  int32_t get_sub_part_space() const
  {
    return sub_part_space_;
  }
  void set_mapping_pg_part_id(const int64_t mapping_pg_part_id)
  {
    mapping_pg_part_id_ = mapping_pg_part_id;
  }
  int64_t get_mapping_pg_part_id() const
  {
    return mapping_pg_part_id_;
  }
  common::ObObj get_sub_interval_start() const
  {
    return sub_interval_start_;
  }
  common::ObObj get_sub_part_interval() const
  {
    return sub_part_interval_;
  }
  void set_drop_schema_version(const int64_t schema_version)
  {
    drop_schema_version_ = schema_version;
  }
  int64_t get_drop_schema_version() const
  {
    return drop_schema_version_;
  }
  bool is_dropped_partition() const
  {
    return drop_schema_version_ > 0;
  }
  void set_max_used_sub_part_id(const int64_t max_used_sub_part_id)
  {
    max_used_sub_part_id_ = max_used_sub_part_id;
  }
  int64_t get_max_used_sub_part_id() const
  {
    return max_used_sub_part_id_;
  }
  ObSubPartition **get_subpart_array() const
  {
    return subpartition_array_;
  }
  int64_t get_subpartition_num() const
  {
    return subpartition_num_;
  }
  ObSubPartition **get_sorted_part_id_subpartition_array() const
  {
    return sorted_part_id_subpartition_array_;
  }
  bool same_partition(const ObPartition &other) const
  {
    return same_base_partition(other) && sub_part_num_ == other.sub_part_num_ &&
           sub_part_space_ == other.sub_part_space_ && sub_interval_start_ == other.sub_interval_start_ &&
           sub_part_interval_ == other.sub_part_interval_;
  }
  int add_partition(const ObSubPartition &subpartition);
  int generate_mapping_pg_subpartition_array();
  ObSubPartition **get_dropped_subpart_array() const
  {
    return dropped_subpartition_array_;
  }
  int64_t get_dropped_subpartition_num() const
  {
    return dropped_subpartition_num_;
  }
  void reset_dropped_subpartition()
  {
    dropped_subpartition_num_ = 0;
    dropped_subpartition_array_ = NULL;
  }
  INHERIT_TO_STRING_KV("BasePartition", ObBasePartition, K_(mapping_pg_part_id), K_(source_part_ids),
      K_(drop_schema_version), K_(max_used_sub_part_id), "subpartition_array",
      common::ObArrayWrap<ObSubPartition *>(subpartition_array_, subpartition_num_), K_(subpartition_array_capacity),
      "dropped_subpartition_array",
      common::ObArrayWrap<ObSubPartition *>(dropped_subpartition_array_, dropped_subpartition_num_),
      K_(dropped_subpartition_array_capacity));

private:
  int inner_add_partition(
      const ObSubPartition &part, ObSubPartition **&part_array, int64_t &part_array_capacity, int64_t &part_num);

protected:
  static const int64_t DEFAULT_ARRAY_CAPACITY = 128;

private:
  // The 14x branch gz_commmon cluster once supported ob_admin to increase the function of subpartition
  // However, this function does not modify sub_part_num, so it is not assumed that the sub_part_num column value
  // of __all_part_history of the templated subpartition table is correct, which is corrected when the schema is
  // refreshed
  int64_t sub_part_num_;
  int32_t sub_part_space_;
  common::ObObj sub_interval_start_;
  common::ObObj sub_part_interval_;
  int64_t mapping_pg_part_id_;
  int64_t drop_schema_version_;
  // The following new members are only valid for non-templated subpartitions
  // Ensure that sub_part_id is not reused when adding or deleting subpartitions
  int64_t max_used_sub_part_id_;
  // nontemplate equal sub_part_num_
  int64_t subpartition_num_;
  // The array size of subpartition_array is not necessarily equal to subpartition_num_
  int64_t subpartition_array_capacity_;
  ObSubPartition **subpartition_array_;
  // Generated when the schema is refreshed, only used by the table, used to quickly index pg_key, not serialized
  ObSubPartition **sorted_part_id_subpartition_array_;
  /* delay delete subpartition */
  int64_t dropped_subpartition_num_;
  int64_t dropped_subpartition_array_capacity_;
  ObSubPartition **dropped_subpartition_array_;
};

struct ObSubPartition : public ObBasePartition {
  OB_UNIS_VERSION(1);

public:
  // For template, because it does not belong to any part, we set part_id to -1
  static const int64_t TEMPLATE_PART_ID = -1;

public:
  ObSubPartition();
  explicit ObSubPartition(common::ObIAllocator *allocator);
  int assign(const ObSubPartition &src_part);
  virtual void reset();
  virtual int clone(common::ObIAllocator &allocator, ObSubPartition *&dst) const;

  void set_sub_part_id(const int64_t subpart_id)
  {
    subpart_id_ = subpart_id;
  }
  int64_t get_sub_part_id() const
  {
    return subpart_id_;
  }
  void set_sub_part_idx(const int64_t subpart_idx)
  {
    subpart_idx_ = subpart_idx;
  }
  int64_t get_sub_part_idx() const
  {
    return subpart_idx_;
  }
  void set_mapping_pg_sub_part_id(const int64_t mapping_pg_sub_part_id)
  {
    mapping_pg_sub_part_id_ = mapping_pg_sub_part_id;
  }
  int64_t get_mapping_pg_sub_part_id() const
  {
    return mapping_pg_sub_part_id_;
  }
  void set_drop_schema_version(const int64_t schema_version)
  {
    drop_schema_version_ = schema_version;
  }
  int64_t get_drop_schema_version() const
  {
    return drop_schema_version_;
  }
  bool is_dropped_partition() const
  {
    return drop_schema_version_ > 0;
  }

  // first compare part_idx,then compare high_bound_val
  static bool less_than(const ObSubPartition *lhs, const ObSubPartition *rhs);
  bool same_sub_partition(const ObSubPartition &other) const
  {
    return same_base_partition(other) && subpart_id_ == other.subpart_id_;
  }
  virtual int64_t get_convert_size() const;
  bool key_match(const ObSubPartition &other) const;
  static bool hash_like_func_less_than(const ObSubPartition *lhs, const ObSubPartition *rhs);
  INHERIT_TO_STRING_KV("BasePartition", ObBasePartition, K_(subpart_id), K_(subpart_idx), K_(mapping_pg_sub_part_id),
      K_(drop_schema_version));

private:
  int64_t subpart_id_;
  int64_t subpart_idx_;             // Valid for hash partition;
  int64_t mapping_pg_sub_part_id_;  // nontemplate table, is partition_id of pg.
                                    // template table, is sub_part_id of pg of template
  int64_t drop_schema_version_;
};

class ObPartitionSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  // base methods
  ObPartitionSchema();
  explicit ObPartitionSchema(common::ObIAllocator *allocator);
  virtual ~ObPartitionSchema();
  ObPartitionSchema(const ObPartitionSchema &src_schema);
  ObPartitionSchema &operator=(const ObPartitionSchema &src_schema);
  // partition related

  virtual const char *get_entity_name() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual void set_tenant_id(const uint64_t tenant_id) = 0;
  virtual uint64_t get_table_id() const = 0;
  virtual void set_table_id(const uint64_t table_id) = 0;
  virtual int64_t get_schema_version() const = 0;
  virtual void set_schema_version(const int64_t schema_version) = 0;
  virtual bool is_user_partition_table() const = 0;
  virtual bool is_user_subpartition_table() const = 0;
  virtual ObPartitionLevel get_part_level() const
  {
    return part_level_;
  }
  virtual int64_t get_partition_cnt() const = 0;
  virtual bool has_self_partition() const = 0;
  virtual int get_primary_zone_inherit(ObSchemaGetterGuard &schema_guard, ObPrimaryZone &primary_zone) const = 0;
  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &schema_guard, ZoneLocalityIArray &locality) const = 0;
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard, common::ObIArray<common::ObZone> &zone_list) const = 0;
  virtual int get_locality_str_inherit(
      share::schema::ObSchemaGetterGuard &guard, const common::ObString *&locality_str) const = 0;
  virtual int get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard &schema_guard,
      const rootserver::ObRandomZoneSelector &random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs, common::ObZone &first_primary_zone) const = 0;
  virtual int check_is_duplicated(share::schema::ObSchemaGetterGuard &guard, bool &is_duplicated) const = 0;
  virtual uint64_t get_tablegroup_id() const = 0;
  virtual void set_tablegroup_id(const uint64_t tg_id) = 0;
  virtual const common::ObString &get_locality_str() const = 0;
  virtual common::ObString &get_locality_str() = 0;
  virtual const common::ObString &get_previous_locality_str() const = 0;
  virtual int get_paxos_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const = 0;
  virtual share::ObDuplicateScope get_duplicate_scope() const = 0;
  virtual int check_has_own_not_f_replica(bool &has_not_f_replica) const = 0;
  virtual void set_duplicate_scope(const share::ObDuplicateScope duplicate_scope) = 0;
  virtual void set_duplicate_scope(const int64_t duplicate_scope) = 0;
  inline virtual int64_t get_part_func_expr_num() const
  {
    return 0;
  }
  inline virtual void set_part_func_expr_num(const int64_t part_func_expr_num)
  {
    UNUSED(part_func_expr_num);
  }
  inline virtual int64_t get_sub_part_func_expr_num() const
  {
    return 0;
  }
  inline virtual void set_sub_part_func_expr_num(const int64_t sub_part_func_expr_num)
  {
    UNUSED(sub_part_func_expr_num);
  }
  // Define the copy distribution method of the partition table, including:
  // 0. Disperse in any way
  // 1. Prioritize the first-partition division
  // 2. Prioritize the second-partition division
  int64_t distribute_by() const;

  inline void set_part_num(const int64_t part_num)
  {
    part_num_ = part_num;
    part_option_.set_part_num(part_num);
  }
  // Only the templated secondary partition is valid, pay attention!!
  inline void set_def_sub_part_num(const int64_t def_subpart_num)
  {
    def_subpart_num_ = def_subpart_num;
    sub_part_option_.set_part_num(def_subpart_num);
  }
  inline void set_part_level(const ObPartitionLevel part_level)
  {
    part_level_ = part_level;
  }
  virtual bool is_sub_part_template() const
  {
    return is_sub_part_template_;
  }
  void set_is_sub_part_template(bool is_sub_part_template)
  {
    is_sub_part_template_ = is_sub_part_template;
  }
  inline int64_t get_first_part_num() const
  {
    return part_option_.get_part_num();
  }
  // Only the templated secondary partition is valid, pay attention!!
  int64_t get_def_sub_part_num() const;
  int get_first_individual_sub_part_num(int64_t &sub_part_num) const;
  // SQL view, unable to see the delayed deleted partition
  int64_t get_all_part_num() const;
  int get_all_partition_num(bool check_dropped_partition, int64_t &part_num) const;

  int check_part_name(const ObPartition &partition);
  int check_part_id(const ObPartition &partition);
  int add_partition(const ObPartition &partition);
  int add_def_subpartition(const ObSubPartition &subpartition);

  inline bool is_hash_part() const
  {
    return part_option_.is_hash_part();
  }
  inline bool is_hash_subpart() const
  {
    return sub_part_option_.is_hash_part();
  }
  inline bool is_key_part() const
  {
    return part_option_.is_key_part();
  }
  inline bool is_key_subpart() const
  {
    return sub_part_option_.is_key_part();
  }
  inline bool is_range_part() const
  {
    return part_option_.is_range_part();
  }
  inline bool is_range_subpart() const
  {
    return sub_part_option_.is_range_part();
  }

  inline bool is_hash_like_part() const
  {
    return part_option_.is_hash_like_part();
  }
  inline bool is_hash_like_subpart() const
  {
    return sub_part_option_.is_hash_like_part();
  }
  inline bool is_list_part() const
  {
    return part_option_.is_list_part();
  }
  inline bool is_list_subpart() const
  {
    return sub_part_option_.is_list_part();
  }
  inline bool is_auto_partitioned_table() const
  {
    return part_option_.is_auto_range_part();
  }

  inline const ObPartitionOption &get_part_option() const
  {
    return part_option_;
  }
  inline ObPartitionOption &get_part_option()
  {
    return part_option_;
  }
  inline const ObPartitionOption &get_sub_part_option() const
  {
    return sub_part_option_;
  }
  inline ObPartitionOption &get_sub_part_option()
  {
    return sub_part_option_;
  }
  inline int64_t get_auto_part_size() const
  {
    return part_option_.get_auto_part_size();
  }

  int serialize_partitions(char *buf, const int64_t data_len, int64_t &pos) const;
  int serialize_dropped_partitions(char *buf, const int64_t data_len, int64_t &pos) const;
  int serialize_def_subpartitions(char *buf, const int64_t data_len, int64_t &pos) const;
  int deserialize_partitions(const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_def_subpartitions(const char *buf, const int64_t data_len, int64_t &pos);

  int get_partition_by_part_id(
      const int64_t part_id, const bool check_dropped_partition, const ObPartition *&partition) const;
  // both part_id and subpart_id is not OB_TWO_PART_MASK
  int get_subpartition(const int64_t part_id, const int64_t subpart_id, const ObSubPartition *&subpartition) const;

  /** generate part name of hash partition, for resolver only
   * @name_type: Type of generated hash partition name:
   *                                FIRST_PART generate partition name
   *                                TEMPLATE_SUB_PART generate subpartition name
   *                                INDIVIDUAL_SUB_PART generate subpartition name of nontemplate
   * @need_upper_case: Does the generated partition name need to be capitalized?
   * @partition: nantemplate table, the partition name needs to be spelled with the partition name,
   *    and partition indicates the partition where the current subpartition is located
   */
  int gen_hash_part_name(const int64_t part_id, const ObHashNameType name_type, const bool need_upper_case, char *buf,
      const int64_t buf_size, int64_t *pos, const ObPartition *partition = NULL) const;
  // Get the partition name corresponding to partition_id (non-partition/partition/subpartition)
  int get_partition_name(const int64_t partition_id, char *buf, const int64_t buf_size, int64_t *pos /* = NULL*/) const;
  // get partition name
  int get_part_name(const int64_t part_id, char *buf, const int64_t buf_size, int64_t *pos /* = NULL*/) const;
  // is_def_subpart refers to the generation of __all_def_sub_part or the part_name in the __all_sub_part table
  int get_subpart_name(const int64_t part_id, const int64_t subpart_id, const bool is_def_subpart, char *buf,
      const int64_t buf_size, int64_t *pos /* = NULL*/) const;

  inline ObPartition **get_part_array() const
  {
    return partition_array_;
  }
  inline int64_t get_partition_capacity() const
  {
    return partition_array_capacity_;
  }
  inline int64_t get_partition_num() const
  {
    return partition_num_;
  }
  // The following interfaces can only be used for templated subpartition tables
  inline ObSubPartition **get_def_subpart_array() const
  {
    return def_subpartition_array_;
  }
  inline int64_t get_def_subpartition_capacity() const
  {
    return def_subpartition_array_capacity_;
  }
  inline int64_t get_def_subpartition_num() const
  {
    return def_subpartition_num_;
  }
  /* ----------------------------------*/
  inline void set_partition_schema_version(const int64_t schema_version)
  {
    partition_schema_version_ = schema_version;
  }
  inline int64_t get_partition_schema_version() const
  {
    return partition_schema_version_;
  }
  // -----dropped partition related -----
  void set_drop_schema_version(const int64_t schema_version)
  {
    drop_schema_version_ = schema_version;
  }
  int64_t get_drop_schema_version() const
  {
    return drop_schema_version_;
  }
  bool is_dropped_schema() const
  {
    return drop_schema_version_ > 0;
  }
  int64_t get_dropped_partition_num() const
  {
    return dropped_partition_num_;
  }
  int clear_dropped_partition();
  void reset_dropped_partition()
  {
    dropped_partition_num_ = 0;
    dropped_partition_array_ = NULL;
  }
  // The partition information needs to be reorganized when truncate the subpartition
  void reset_partition_for_trun_subpart()
  {
    partition_array_capacity_ = 0;
    part_num_ = 0;
    partition_num_ = 0;
    partition_array_ = NULL;
  }
  // This interface is only for iterators, and the corresponding iterator should be used to access the deleted
  // partition, or the schema_guard/schema_service corresponding interface should be used
  ObPartition **get_dropped_part_array() const
  {
    return dropped_partition_array_;
  }
  // -------------------------------------
  int get_part_id_by_name(const common::ObString partition_name, int64_t &part_id) const;
  // FIXME:() Refresh the schema, do not assume that the partition_array is in order, and can be optimized later
  int find_partition_by_part_id(
      const int64_t part_id, const bool check_dropped_partition, const ObPartition *&partition) const;
  int get_subpart_info(
      const int64_t part_id, ObSubPartition **&subpart_array, int64_t &subpart_num, int64_t &subpartition_num) const;
  //-------index partition-------
  int get_part_id_by_idx(const int64_t part_idx, int64_t &part_id) const;
  int get_hash_subpart_id_by_idx(const int64_t part_id, const int64_t subpart_idx, int64_t &subpart_id) const;

  int get_hash_part_idx_by_id(const int64_t part_id, int64_t &part_idx) const;
  int get_hash_subpart_idx_by_id(const int64_t part_id, const int64_t subpart_id, int64_t &subpart_idx) const;

  int convert_partition_idx_to_id(const int64_t partition_idx, int64_t &partition_id) const;
  int convert_partition_id_to_idx(const int64_t partition_id, int64_t &partition_idx) const;
  //----------------------
  // Get the offset of the partition in partition_array, just take the offset of the partition,
  // not including the subpartition
  int get_partition_index_by_id(
      const int64_t part_id, const bool check_dropped_partition, int64_t &partition_index) const;
  // Binary search range partition, only applicable to the partition that has not been split
  int get_partition_index_binary_search(
      const int64_t part_id, const bool check_dropped_partition, int64_t &partition_index) const;
  // Iterate all the partitions to get the offset
  int get_partition_index_loop(
      const int64_t part_id, const bool check_dropped_partition, int64_t &partition_index) const;

  inline void set_partition_status(const ObPartitionStatus partition_status)
  {
    partition_status_ = partition_status;
  }
  inline ObPartitionStatus get_partition_status() const
  {
    return partition_status_;
  }
  bool is_in_splitting() const
  {
    return partition_status_ == PARTITION_STATUS_LOGICAL_SPLITTING ||
           partition_status_ == PARTITION_STATUS_PHYSICAL_SPLITTING;
  }
  bool is_in_logical_split() const
  {
    return partition_status_ == PARTITION_STATUS_LOGICAL_SPLITTING;
  }
  bool is_in_physical_split() const
  {
    return partition_status_ == PARTITION_STATUS_PHYSICAL_SPLITTING;
  }
  // other methods
  virtual void reset();
  virtual bool is_valid() const;
  virtual void set_min_partition_id(uint64_t partition_id)
  {
    UNUSED(partition_id);
  }
  virtual uint64_t get_min_partition_id() const
  {
    return 0;
  }
  virtual int get_split_source_partition_key(
      const common::ObPartitionKey &dst_part_key, common::ObPartitionKey &source_part_key) const;
  DECLARE_VIRTUAL_TO_STRING;
  int try_assign_part_array(const share::schema::ObPartitionSchema &that);
  int try_assign_def_subpart_array(const share::schema::ObPartitionSchema &that);

protected:
  int inner_add_partition(const ObPartition &part);
  int inner_add_partition(const ObSubPartition &part);
  template <class T>
  int inner_add_partition(const T &part, T **&part_array, int64_t &part_array_capacity, int64_t &part_num);
  int get_subpart_name(const int64_t part_id, const int64_t subpart_id, char *buf, const int64_t buf_size,
      int64_t *pos /* = NULL*/) const;
  int get_def_subpart_name(const int64_t subpart_id, char *buf, const int64_t buf_size, int64_t *pos /* = NULL*/) const;

protected:
  static const int64_t DEFAULT_ARRAY_CAPACITY = 128;

protected:
  int64_t part_num_;
  // Redundant value, derived from sub_part_option_, valid when is_sub_part_template = true
  int64_t def_subpart_num_;
  ObPartitionLevel part_level_;
  ObPartitionOption part_option_;
  // The part_num_ is only valid when is_sub_part_template = true
  ObPartitionOption sub_part_option_;
  ObPartition **partition_array_;
  int64_t partition_array_capacity_;  // The array size of partition_array is not necessarily equal to partition_num;
  // Equal to part_num; historical reasons cause the two values to have the same meaning;
  // need to be modified together with part_num_
  int64_t partition_num_;
  /* template table, when is_sub_part_template = true, valid */
  ObSubPartition **def_subpartition_array_;
  // The array size of subpartition_array, not necessarily equal to subpartition_num
  int64_t def_subpartition_array_capacity_;
  int64_t def_subpartition_num_;  // equal subpart_num
  /* template subpartition define end*/
  // Record the split schema, initialized to 0, not cleared after splitting, the bottom layer needs to be used
  int64_t partition_schema_version_;
  ObPartitionStatus partition_status_;
  /* delay delete table/tablegroup/partition start*/
  int64_t drop_schema_version_;
  // delay delete partition, Not visible to SQL
  ObPartition **dropped_partition_array_;
  int64_t dropped_partition_array_capacity_;
  int64_t dropped_partition_num_;
  /* delay delete table/tablegroup/partition end*/
  // It is meaningful when the subpartition table is used to mark whether the subpartition is a templated definition,
  // and it is compatible with
  bool is_sub_part_template_;
};

class ObTablegroupSchema : public ObPartitionSchema {
  OB_UNIS_VERSION(1);

public:
  // base methods
  ObTablegroupSchema();
  explicit ObTablegroupSchema(common::ObIAllocator *allocator);
  virtual ~ObTablegroupSchema();
  ObTablegroupSchema(const ObTablegroupSchema &src_schema);
  ObTablegroupSchema &operator=(const ObTablegroupSchema &src_schema);
  int assign(const ObTablegroupSchema &src_schema);
  // set methods
  inline void set_binding(const bool binding)
  {
    binding_ = binding;
  }
  inline void set_tenant_id(const uint64_t tenant_id) override
  {
    tenant_id_ = tenant_id;
  }
  inline void set_schema_version(const int64_t schema_version) override
  {
    schema_version_ = schema_version;
  }
  virtual void set_tablegroup_id(const uint64_t tablegroup_id) override
  {
    tablegroup_id_ = tablegroup_id;
  }
  inline int set_tablegroup_name(const char *name)
  {
    return deep_copy_str(name, tablegroup_name_);
  }
  inline int set_comment(const char *comment)
  {
    return deep_copy_str(comment, comment_);
  }
  inline int set_tablegroup_name(const common::ObString &name)
  {
    return deep_copy_str(name, tablegroup_name_);
  }
  inline int set_table_name(const common::ObString &name)
  {
    return deep_copy_str(name, tablegroup_name_);
  }
  inline int set_comment(const common::ObString &comment)
  {
    return deep_copy_str(comment, comment_);
  }
  inline int set_locality(const common::ObString &locality)
  {
    return deep_copy_str(locality, locality_info_.locality_str_);
  }
  inline int set_primary_zone(const common::ObString &primary_zone)
  {
    return deep_copy_str(primary_zone, primary_zone_info_.primary_zone_str_);
  }
  inline int set_previous_locality(const common::ObString &previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_);
  }
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);

  int set_zone_replica_attr_array(const common::ObIArray<share::ObZoneReplicaAttrSet> &src);
  int set_zone_replica_attr_array(const common::ObIArray<share::SchemaZoneReplicaAttrSet> &src);
  inline int set_split_partition(const common::ObString &split_partition)
  {
    return deep_copy_str(split_partition, split_partition_name_);
  }
  inline int set_split_rowkey(const common::ObRowkey &rowkey)
  {
    return rowkey.deep_copy(split_high_bound_val_, *get_allocator());
  }
  inline int set_split_list_value(common::ObRowkey &list_values)
  {
    return list_values.deep_copy(split_list_row_values_, *get_allocator());
  }
  // get methods
  inline bool get_binding() const
  {
    return binding_;
  }
  inline uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  virtual inline int64_t get_schema_version() const override
  {
    return schema_version_;
  }
  virtual uint64_t get_tablegroup_id() const override
  {
    return tablegroup_id_;
  }
  inline const char *get_tablegroup_name_str() const
  {
    return extract_str(tablegroup_name_);
  }
  inline const char *get_comment() const
  {
    return extract_str(comment_);
  }
  inline const common::ObString &get_tablegroup_name() const
  {
    return tablegroup_name_;
  }
  inline const common::ObString &get_table_name() const
  {
    return tablegroup_name_;
  }
  virtual const char *get_entity_name() const override
  {
    return extract_str(tablegroup_name_);
  }
  inline const common::ObString &get_comment_str() const
  {
    return comment_;
  }
  inline const ObLocality &get_locality_info() const
  {
    return locality_info_;
  }
  virtual common::ObString &get_locality_str() override
  {
    return locality_info_.locality_str_;
  }
  virtual const common::ObString &get_locality_str() const override
  {
    return locality_info_.locality_str_;
  }
  inline const char *get_locality() const
  {
    return extract_str(locality_info_.locality_str_);
  }
  inline const char *get_previous_locality() const
  {
    return extract_str(previous_locality_);
  }
  virtual const common::ObString &get_previous_locality_str() const override
  {
    return previous_locality_;
  }
  inline const ObPrimaryZone &get_primary_zone_info() const
  {
    return primary_zone_info_;
  }
  inline const common::ObString &get_primary_zone() const
  {
    return primary_zone_info_.primary_zone_str_;
  }
  int set_zone_list(const common::ObIArray<common::ObZone> &zone_list)
  {
    UNUSED(zone_list);
    return common::OB_SUCCESS;
  }
  inline const common::ObString &get_split_partition_name() const
  {
    return split_partition_name_;
  }
  inline const common::ObRowkey &get_split_rowkey() const
  {
    return split_high_bound_val_;
  }
  inline const common::ObRowkey &get_split_list_row_values() const
  {
    return split_list_row_values_;
  }
  void reset_locality_options();
  // In the current implementation, if the locality of the zone_list of the tablegroup is empty,
  // it will be read from the tenant, otherwise it will be parsed from the locality
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard, common::ObIArray<common::ObZone> &zone_list) const override;
  virtual int get_zone_replica_attr_array(ZoneLocalityIArray &locality) const;
  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &schema_guard, ZoneLocalityIArray &locality) const override;
  virtual int get_locality_str_inherit(
      ObSchemaGetterGuard &schema_guard, const common::ObString *&locality_str) const override;
  virtual int get_first_primary_zone_inherit(share::schema::ObSchemaGetterGuard &schema_guard,
      const rootserver::ObRandomZoneSelector &random_selector,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
      common::ObZone &first_primary_zone) const override;
  virtual int check_is_duplicated(share::schema::ObSchemaGetterGuard &guard, bool &is_duplicated) const override;
  int get_primary_zone_score(const common::ObZone &zone, int64_t &zone_score) const;
  inline const common::ObIArray<ObZoneScore> &get_primary_zone_array() const
  {
    return primary_zone_info_.primary_zone_array_;
  }
  virtual int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard, share::schema::ObPrimaryZone &primary_zone) const override;
  int fill_additional_options();
  int check_in_locality_modification(
      share::schema::ObSchemaGetterGuard &schema_guard, bool &in_locality_modification) const;
  int check_is_readonly_at_all(share::schema::ObSchemaGetterGuard &guard, const common::ObZone &zone,
      const common::ObRegion &region, bool &readonly_at_all) const;
  int get_full_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const;
  virtual int get_paxos_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const override;
  int get_all_replica_num(share::schema::ObSchemaGetterGuard &schema_guard, int64_t &num) const;
  // partition related
  virtual share::ObDuplicateScope get_duplicate_scope() const override
  {
    return share::ObDuplicateScope::DUPLICATE_SCOPE_NONE;
  }
  virtual void set_duplicate_scope(const share::ObDuplicateScope duplicate_scope) override
  {
    UNUSED(duplicate_scope);
  }
  virtual void set_duplicate_scope(const int64_t duplicate_scope) override
  {
    UNUSED(duplicate_scope);
  }
  inline virtual bool is_user_partition_table() const override
  {
    return PARTITION_LEVEL_ONE == get_part_level() || PARTITION_LEVEL_TWO == get_part_level();
  }
  inline virtual bool is_user_subpartition_table() const override
  {
    return PARTITION_LEVEL_TWO == get_part_level();
  }
  inline virtual uint64_t get_table_id() const override
  {
    return tablegroup_id_;
  }  // for partition schema used
  inline virtual uint64_t get_database_id() const
  {
    return 0;
  }
  inline virtual void set_database_id(const uint64_t database_id)
  {
    UNUSED(database_id);
  }
  inline virtual void set_table_id(const uint64_t tablegroup_id) override
  {
    tablegroup_id_ = tablegroup_id;
  }
  inline int64_t get_part_func_expr_num() const override
  {
    return part_func_expr_num_;
  }
  inline void set_part_func_expr_num(const int64_t part_func_expr_num) override
  {
    part_func_expr_num_ = part_func_expr_num;
  }
  inline int64_t get_sub_part_func_expr_num() const override
  {
    return sub_part_func_expr_num_;
  }
  inline void set_sub_part_func_expr_num(const int64_t sub_part_func_expr_num) override
  {
    sub_part_func_expr_num_ = sub_part_func_expr_num;
  }
  virtual int64_t get_partition_cnt() const override
  {
    return 0;
  }
  virtual int calc_part_func_expr_num(int64_t &part_func_expr_num) const;
  virtual int calc_subpart_func_expr_num(int64_t &subpart_func_expr_num) const;
  // other methods
  virtual void reset() override;
  int64_t get_convert_size() const override;
  virtual bool has_self_partition() const override
  {
    return get_binding();
  }
  virtual bool is_valid() const override;
  bool is_mock_global_index_invalid() const
  {
    return is_mock_global_index_invalid_;
  }
  bool is_global_index_table() const
  {
    return false;
  }
  void set_mock_global_index_invalid(const bool is_invalid)
  {
    is_mock_global_index_invalid_ = is_invalid;
  }
  bool can_read_index() const
  {
    return true;
  }
  inline bool can_rereplicate_global_index_table() const
  {
    return true;
  }
  virtual int check_has_own_not_f_replica(bool &has_not_f_replica) const override;

  DECLARE_VIRTUAL_TO_STRING;

private:
  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
  int64_t schema_version_;
  common::ObString tablegroup_name_;
  common::ObString comment_;
  // 2.0 add
  ObLocality locality_info_;
  ObPrimaryZone primary_zone_info_;
  int64_t part_func_expr_num_;
  int64_t sub_part_func_expr_num_;
  common::ObString previous_locality_;
  common::ObString split_partition_name_;
  common::ObRowkey split_high_bound_val_;
  common::ObRowkey split_list_row_values_;
  // Indicates whether the tablegroup has physical physical partitions
  // true: With physical physical partition
  // false: Without physical physical partition
  bool binding_;
  bool is_mock_global_index_invalid_;  // Standalone cluster use; no serialization required
};

class ObPartitionUtils {
public:
  /// Get part ids through range of partition_column

  // Get hash(FUNC_HASH/FUNC_KEY) part type ids
  static int get_hash_part_ids(const ObTableSchema &table_schema, const common::ObNewRange &range,
      const int64_t part_num, common::ObIArray<int64_t> &part_ids);
  // @param: get_part_index Indicates whether this function is called to get whether it is part_index or part_id
  // - true  Indicates that part_index is to be obtained, and part_index indicates the position of partition
  //  in the partition array.
  // - false Indicates that the part to be obtained is part_id, and part_id indicates the logical id of the primary
  // partition
  static int get_hash_part_ids(const ObTableSchema &table_schema, const common::ObNewRow &row, int64_t part_num,
      common::ObIArray<int64_t> &part_ids, bool get_part_index = false);
  static int get_hash_subpart_ids(const ObTableSchema &table_schema, const int64_t part_id,
      const common::ObNewRange &range, const int64_t subpart_num, common::ObIArray<int64_t> &subpart_ids);
  // @param: get_subpart_index  Indicates whether this function is called to get whether it is subpart_index or
  // subpart_id
  // - true  Indicates that subpart_index is to be obtained, and subpart_index indicates the position of subpartition
  //  in the subpartition array.
  // - false Indicates that the subpart to be obtained is subpart_id, and subpart_id indicates the logical id of
  //  the primary subpartition
  static int get_hash_subpart_ids(const ObTableSchema &table_schema, const int64_t part_id, const common::ObNewRow &row,
      int64_t subpart_num, common::ObIArray<int64_t> &subpart_ids, bool get_subpart_index = false);

  // Get level-one range part_ids
  static int get_range_part_ids(const common::ObNewRange &range, const bool reverse,
      ObPartition *const *partition_array, const int64_t partition_num, common::ObIArray<int64_t> &part_ids);
  // The meaning of get_part_index can refer to get_hash_part_ids()
  static int get_range_part_ids(const common::ObNewRow &row, ObPartition *const *partition_array,
      const int64_t partition_num, common::ObIArray<int64_t> &part_ids, bool get_part_index = false);

  // Get level-one range part_ids
  static int get_list_part_ids(const common::ObNewRange &range, const bool reverse, ObPartition *const *partition_array,
      const int64_t partition_num, common::ObIArray<int64_t> &part_ids);

  // The meaning of get_part_index can refer to get_hash_part_ids()
  static int get_list_part_ids(const common::ObNewRow &row, ObPartition *const *partition_array,
      const int64_t partition_num, common::ObIArray<int64_t> &part_ids, bool get_part_index = false);

  // Get level-two range part_ids
  static int get_range_part_ids(const int64_t part_id, const common::ObNewRange &range, const bool reverse,
      ObSubPartition *const *partition_array, const int64_t partition_num, common::ObIArray<int64_t> &part_ids);
  // The meaning of get_subpart_index can refer to get_hash_subpart_ids()
  static int get_range_part_ids(const int64_t part_id, const common::ObNewRow &row,
      ObSubPartition *const *partition_array, const int64_t partition_num, common::ObIArray<int64_t> &part_ids,
      bool get_subpart_index = false);

  // Get level-two range part_ids
  static int get_list_part_ids(const int64_t part_id, const common::ObNewRange &range, const bool reverse,
      ObSubPartition *const *partition_array, const int64_t partition_num, common::ObIArray<int64_t> &part_ids);

  // The meaning of get_subpart_index can refer to get_hash_subpart_ids()
  static int get_list_part_ids(const int64_t part_id, const common::ObNewRow &row,
      ObSubPartition *const *partition_array, const int64_t partition_num, common::ObIArray<int64_t> &part_ids,
      bool get_subpart_index = false);

  // According to the given hash value val and partition number part_num,
  // distinguish between oracle and mysql modes to calculate which partition this fold falls on
  // This interface is called at get_hash_part_idxs, get_hash_subpart_ids, etc.
  static int calc_hash_part_idx(const uint64_t val, const int64_t part_num, int64_t &partition_idx);
  template <typename T>
  static int get_range_part_idx(
      const common::ObObj &obj, T *const *part_array, const int64_t part_num, int64_t &part_idx);

  template <typename T>
  static int get_range_part_bounds(T *const *part_array, const int64_t part_num, const int64_t part_idx,
      common::ObObj &lower_bound, common::ObObj &upper_bound);

  // Get all part idxs of part num
  static int get_all_part(const ObTableSchema &table_schema, int64_t part_num, common::ObIArray<int64_t> &part_ids);

  // Convert rowkey to sql literal for show
  static int convert_rowkey_to_sql_literal(const common::ObRowkey &rowkey, char *buf, const int64_t buf_len,
      int64_t &pos, bool print_collation, const common::ObTimeZoneInfo *tz_info);
  // Used to display the defined value of the LIST partition
  static int convert_rows_to_sql_literal(const common::ObIArray<common::ObNewRow> &rows, char *buf,
      const int64_t buf_len, int64_t &pos, bool print_collation, const common::ObTimeZoneInfo *tz_info);
  static int print_oracle_datetime_literal(const common::ObObj &tmp_obj, char *buf, const int64_t buf_len, int64_t &pos,
      const common::ObTimeZoneInfo *tz_info);

  // Convert rowkey's serialize buff to hex.For record all rowkey info.
  static int convert_rowkey_to_hex(const common::ObRowkey &rowkey, char *buf, const int64_t buf_len, int64_t &pos);
  static int convert_rows_to_hex(
      const common::ObIArray<common::ObNewRow> &rows, char *buf, const int64_t buf_len, int64_t &pos);

  // Get partition start with start_part rowkey, return start_pos
  template <typename T>
  static int get_start(
      const T *const *partition_array, const int64_t partition_num, const T &start_part, int64_t &start_pos);

  // Get partition end with end_part rowkey, return end_pos
  template <typename T>
  static int get_end(const T *const *partition_array, const int64_t partition_num,
      const common::ObBorderFlag &border_flag, const T &end_part, int64_t &end_pos);

  // check if partition value equal
  template <typename PARTITION>
  static int check_partition_value(
      const PARTITION &l_part, const PARTITION &r_part, const ObPartitionFuncType part_type, bool &is_equal);

  static bool is_types_equal_for_partition_check(const common::ObObjType &typ1, const common::ObObjType &type2);

private:
  // Get level-one range part_ids
  // The meaning of get_part_index can refer to get_hash_part_ids()
  static int get_range_part_ids(ObPartition &start, ObPartition &end, const common::ObBorderFlag &border_flag,
      const bool reverse, ObPartition *const *partition_array, const int64_t partition_num,
      common::ObIArray<int64_t> &part_ids, bool get_part_index = false);
  // Get level-two range part_ids
  // The meaning of get_subpart_index can refer to get_hash_subpart_ids()
  static int get_range_part_ids(ObSubPartition &start_bound, ObSubPartition &end_bound,
      const common::ObBorderFlag &border_flag, const bool reverse, ObSubPartition *const *partition_array,
      const int64_t partition_num, common::ObIArray<int64_t> &part_ids, bool get_subpart_index = false);
};

template <typename T>
int ObPartitionUtils::get_range_part_idx(
    const common::ObObj &obj, T *const *part_array, const int64_t part_num, int64_t &part_idx)
{
  int ret = common::OB_SUCCESS;
  if (obj.is_null()) {
    part_idx = 0;
  } else {
    common::ObObj buf_obj = obj;
    common::ObRowkey row_key(&buf_obj, 1);
    T end_key;
    end_key.high_bound_val_ = row_key;
    common::ObBorderFlag border_flag;
    border_flag.set_inclusive_start();
    border_flag.set_inclusive_end();
    if (part_num < 1) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "Unexpected partition num", K(part_num), K(ret));
    } else if (OB_FAIL(get_end(part_array, part_num, border_flag, end_key, part_idx))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to get end partition idx", K(end_key), K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObPartitionUtils::get_range_part_bounds(T *const *part_array, const int64_t part_num, const int64_t part_idx,
    common::ObObj &lower_bound, common::ObObj &upper_bound)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!(0 <= part_idx && part_idx < part_num))) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid part_num or part_idx", K(part_num), K(part_idx), K(ret));
  } else if (OB_ISNULL(part_array[part_idx])) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "part_array[part_idx] is null", K(part_idx), K(ret));
  } else if (part_array[part_idx]->get_high_bound_val().get_obj_cnt() > 1) {
    ret = common::OB_NOT_IMPLEMENT;
    SHARE_SCHEMA_LOG(WARN, "range columns count should be 1 now", K(ret));
  } else {
    if (part_idx > 0) {
      if (OB_ISNULL(part_array[part_idx - 1])) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "part_array[part_idx - 1] is null", K(part_idx - 1), K(ret));
      } else {
        lower_bound = part_array[part_idx - 1]->get_high_bound_val().get_obj_ptr()[0];
      }
    } else {
      lower_bound.set_min_value();
    }
    if (part_idx < part_num - 1) {
      upper_bound = part_array[part_idx]->get_high_bound_val().get_obj_ptr()[0];
    } else {
      upper_bound.set_max_value();
    }
  }
  return ret;
}

template <typename T>
int ObPartitionUtils::get_start(
    const T *const *partition_array, const int64_t partition_num, const T &start_part, int64_t &start_pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(partition_array)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Partition array should not be NULL", K(ret));
  } else {
    const T *const *result =
        std::upper_bound(partition_array, partition_array + partition_num, &start_part, T::less_than);
    start_pos = result - partition_array;
    if (start_pos < 0) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "Start pos error", K(partition_num), K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObPartitionUtils::get_end(const T *const *partition_array, const int64_t partition_num,
    const common::ObBorderFlag &border_flag, const T &end_part, int64_t &end_pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(partition_array)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Input partition array should not be NULL", K(ret));
  } else {
    const T *const *result =
        std::lower_bound(partition_array, partition_array + partition_num, &end_part, T::less_than);
    int64_t pos = result - partition_array;
    if (pos >= partition_num) {
      end_pos = partition_num - 1;
    } else if (OB_ISNULL(result) || OB_ISNULL(*result)) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "result should not be NULL", K(ret), K(result));
    } else {
      common::ObNewRow lrow;
      lrow.cells_ = const_cast<common::ObObj *>((*result)->high_bound_val_.get_obj_ptr());
      lrow.count_ = (*result)->high_bound_val_.get_obj_cnt();
      lrow.projector_ = (*result)->projector_;
      lrow.projector_size_ = (*result)->projector_size_;
      common::ObNewRow rrow;
      rrow.cells_ = const_cast<common::ObObj *>(end_part.high_bound_val_.get_obj_ptr());
      rrow.count_ = end_part.high_bound_val_.get_obj_cnt();
      rrow.projector_ = end_part.projector_;
      rrow.projector_size_ = end_part.projector_size_;
      int cmp = 0;
      if (common::OB_SUCCESS != common::ObRowUtil::compare_row(lrow, rrow, cmp)) {
        SHARE_SCHEMA_LOG(ERROR, "lhs or rhs is invalid");
      }
      if (0 == cmp) {
        if (pos == partition_num - 1) {
          end_pos = partition_num - 1;
        } else {
          if (border_flag.inclusive_end()) {
            end_pos = pos + 1;
          } else {
            end_pos = pos;
          }
        }
      } else {
        end_pos = pos;
      }
    }
  }
  return ret;
}

// TODO Put the partition description information together
// struct ObPartitionDesc
//{
//  OB_UNIS_VERSION(1);
//  int get_part(const common::ObNewRange &range,
//               bool reverse,
//               common::ObIArray<int64_t> &part_ids);
//
//  int get_sub_part(uint64_t table_id,
//                   int64_t part_id,
//                   const common::ObNewRange &range,
//                   bool reverse,
//                   common::ObIArray<int64_t> &part_ids);
//  //first partition
//  ObPartition **partition_array_;
//  int64_t partition_array_capacity_;
//  int64_t partition_num_;
//  //second partition
//  ObSubPartition **subpartition_array_;
//  int64_t subpartition_array_capacity_;
//  int64_t subpartition_num_;
//};

class ObViewSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObViewSchema();
  explicit ObViewSchema(common::ObIAllocator *allocator);
  virtual ~ObViewSchema();
  ObViewSchema(const ObViewSchema &src_schema);
  ObViewSchema &operator=(const ObViewSchema &src_schema);
  bool operator==(const ObViewSchema &other) const;
  bool operator!=(const ObViewSchema &other) const;

  inline int set_view_definition(const char *view_definition)
  {
    return deep_copy_str(view_definition, view_definition_);
  }
  inline int set_view_definition(const common::ObString &view_definition)
  {
    return deep_copy_str(view_definition, view_definition_);
  }
  inline void set_view_check_option(const ViewCheckOption option)
  {
    view_check_option_ = option;
  }
  inline void set_view_is_updatable(const bool is_updatable)
  {
    view_is_updatable_ = is_updatable;
  }
  inline void set_materialized(const bool materialized)
  {
    materialized_ = materialized;
  }
  inline void set_character_set_client(const common::ObCharsetType character_set_client)
  {
    character_set_client_ = character_set_client;
  }
  inline void set_collation_connection(const common::ObCollationType collation_connection)
  {
    collation_connection_ = collation_connection;
  }

  // view_definition_ is defined using utf8, for sql resolve please use
  // ObSQLUtils::generate_view_definition_for_resolve
  inline const common::ObString &get_view_definition_str() const
  {
    return view_definition_;
  }
  inline const char *get_view_definition() const
  {
    return extract_str(view_definition_);
  }
  inline ViewCheckOption get_view_check_option() const
  {
    return view_check_option_;
  }
  inline bool get_view_is_updatable() const
  {
    return view_is_updatable_;
  }
  inline bool get_materialized() const
  {
    return materialized_;
  }
  inline common::ObCharsetType get_character_set_client() const
  {
    return character_set_client_;
  }
  inline common::ObCollationType get_collation_connection() const
  {
    return collation_connection_;
  }

  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual void reset();

  TO_STRING_KV(N_VIEW_DEFINITION, view_definition_, N_CHECK_OPTION, ob_view_check_option_str(view_check_option_),
      N_IS_UPDATABLE, STR_BOOL(view_is_updatable_), N_IS_MATERIALIZED, STR_BOOL(materialized_),
      K_(character_set_client), K_(collation_connection));

private:
  common::ObString view_definition_;
  ViewCheckOption view_check_option_;
  bool view_is_updatable_;
  bool materialized_;
  common::ObCharsetType character_set_client_;
  common::ObCollationType collation_connection_;
};

class ObColumnSchemaHashWrapper {
public:
  ObColumnSchemaHashWrapper()
  {}
  explicit ObColumnSchemaHashWrapper(const common::ObString &str) : column_name_(str)
  {}
  ~ObColumnSchemaHashWrapper()
  {}
  void set_name(const common::ObString &str)
  {
    column_name_ = str;
  }
  inline bool operator==(const ObColumnSchemaHashWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_));
  }
  inline uint64_t hash() const;
  common::ObString column_name_;
};
class ObColumnSchemaWrapper {
public:
  ObColumnSchemaWrapper() : column_name_(), prefix_len_(0)
  {}
  explicit ObColumnSchemaWrapper(const common::ObString &str, int32_t prefix_len)
      : column_name_(str), prefix_len_(prefix_len)
  {}
  ~ObColumnSchemaWrapper()
  {}
  void set_name(const common::ObString &str)
  {
    column_name_ = str;
  }
  inline bool name_equal(const ObColumnSchemaWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_));
  }
  inline bool all_equal(const ObColumnSchemaWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_)) && prefix_len_ == other.prefix_len_;
  }
  TO_STRING_KV(K_(column_name), K_(prefix_len));

  common::ObString column_name_;
  int32_t prefix_len_;
};
typedef ObColumnSchemaHashWrapper ObColumnNameHashWrapper;
typedef ObColumnSchemaHashWrapper ObIndexNameHashWrapper;
typedef ObColumnSchemaHashWrapper ObPartitionNameHashWrapper;
typedef ObColumnSchemaHashWrapper ObForeignKeyNameHashWrapper;
typedef ObColumnSchemaWrapper ObColumnNameWrapper;

inline uint64_t ObColumnSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  // case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, column_name_, hash_ret);
  return hash_ret;
}
class ObIndexSchemaHashWrapper {
public:
  ObIndexSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID), data_table_id_(common::OB_INVALID_ID)
  {}
  ObIndexSchemaHashWrapper(
      uint64_t tenant_id, const uint64_t database_id, const uint64_t data_table_id, const common::ObString &index_name)
      : tenant_id_(tenant_id), database_id_(database_id), data_table_id_(data_table_id), index_name_(index_name)
  {}
  ~ObIndexSchemaHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObIndexSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline uint64_t get_data_table_id() const
  {
    return data_table_id_;
  }
  inline const common::ObString &get_index_name() const
  {
    return index_name_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t data_table_id_;  // only for mysql mode
  common::ObString index_name_;
};

inline uint64_t ObIndexSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&data_table_id_, sizeof(uint64_t), hash_ret);
  // case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, index_name_, hash_ret);
  return hash_ret;
}

inline bool ObIndexSchemaHashWrapper::operator==(const ObIndexSchemaHashWrapper &rv) const
{
  // mysql case insensitive
  // oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (data_table_id_ == rv.data_table_id_) &&
         (0 == name_cmp.compare(index_name_, rv.index_name_));
}

class ObTableSchemaHashWrapper {
public:
  ObTableSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        name_case_mode_(common::OB_NAME_CASE_INVALID)
  {}
  ObTableSchemaHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const uint64_t session_id,
      const common::ObNameCaseMode mode, const common::ObString &table_name)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        session_id_(session_id),
        name_case_mode_(mode),
        table_name_(table_name)
  {}
  ~ObTableSchemaHashWrapper()
  {}
  inline uint64_t hash() const;
  bool operator==(const ObTableSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline uint64_t get_session_id() const
  {
    return session_id_;
  }
  inline const common::ObString &get_table_name() const
  {
    return table_name_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t session_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString table_name_;
};

inline uint64_t ObTableSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret);
  return hash_ret;
}

// See ObSchemaMgr::get_table_schema comment for session visibility judgment
inline bool ObTableSchemaHashWrapper::operator==(const ObTableSchemaHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_, database_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) &&
         (name_case_mode_ == rv.name_case_mode_) &&
         (session_id_ == rv.session_id_ || common::OB_INVALID_ID == rv.session_id_) &&
         (0 == name_cmp.compare(table_name_, rv.table_name_));
}

class ObDatabaseSchemaHashWrapper {
public:
  ObDatabaseSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID), name_case_mode_(common::OB_NAME_CASE_INVALID)
  {}
  ObDatabaseSchemaHashWrapper(
      const uint64_t tenant_id, const common::ObNameCaseMode mode, const common::ObString &database_name)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_name_(database_name)
  {}
  ~ObDatabaseSchemaHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObDatabaseSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  inline const common::ObString &get_database_name() const
  {
    return database_name_;
  }

private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString database_name_;
};

inline uint64_t ObDatabaseSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::ObCharset::hash(cs_type, database_name_, hash_ret);
  return hash_ret;
}

inline bool ObDatabaseSchemaHashWrapper::operator==(const ObDatabaseSchemaHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
  return (tenant_id_ == rv.tenant_id_) && (name_case_mode_ == rv.name_case_mode_) &&
         (0 == name_cmp.compare(database_name_, rv.database_name_));
}

class ObTablegroupSchemaHashWrapper {
public:
  ObTablegroupSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID)
  {}
  ObTablegroupSchemaHashWrapper(uint64_t tenant_id, const common::ObString &tablegroup_name)
      : tenant_id_(tenant_id), tablegroup_name_(tablegroup_name)
  {}
  ~ObTablegroupSchemaHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObTablegroupSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline const common::ObString &get_tablegroup_name() const
  {
    return tablegroup_name_;
  }

private:
  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
};

class ObForeignKeyInfoHashWrapper {
public:
  ObForeignKeyInfoHashWrapper()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    foreign_key_name_.assign_ptr("", 0);
  }
  ObForeignKeyInfoHashWrapper(uint64_t tenant_id, const uint64_t database_id, const common::ObString &foreign_key_name)
      : tenant_id_(tenant_id), database_id_(database_id), foreign_key_name_(foreign_key_name)
  {}
  ~ObForeignKeyInfoHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObForeignKeyInfoHashWrapper &rv) const;
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline const common::ObString &get_foreign_key_name() const
  {
    return foreign_key_name_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString foreign_key_name_;
};

inline uint64_t ObForeignKeyInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  // case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, foreign_key_name_, hash_ret);
  return hash_ret;
}

inline bool ObForeignKeyInfoHashWrapper::operator==(const ObForeignKeyInfoHashWrapper &rv) const
{
  // mysql case insensitive
  // oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) &&
         (0 == name_cmp.compare(foreign_key_name_, rv.foreign_key_name_));
}

class ObConstraintInfoHashWrapper {
public:
  ObConstraintInfoHashWrapper()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    constraint_name_.assign_ptr("", 0);
  }
  ObConstraintInfoHashWrapper(uint64_t tenant_id, const uint64_t database_id, const common::ObString &constraint_name)
      : tenant_id_(tenant_id), database_id_(database_id), constraint_name_(constraint_name)
  {}
  ~ObConstraintInfoHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObConstraintInfoHashWrapper &rv) const;
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline const common::ObString &get_constraint_name() const
  {
    return constraint_name_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString constraint_name_;
};

inline uint64_t ObConstraintInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  // case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, constraint_name_, hash_ret);
  return hash_ret;
}

inline bool ObConstraintInfoHashWrapper::operator==(const ObConstraintInfoHashWrapper &rv) const
{
  // mysql case insensitive
  // oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) &&
         (0 == name_cmp.compare(constraint_name_, rv.constraint_name_));
}

inline uint64_t ObTablegroupSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(tablegroup_name_.ptr(), tablegroup_name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObTablegroupSchemaHashWrapper::operator==(const ObTablegroupSchemaHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (tablegroup_name_ == rv.tablegroup_name_);
}

struct ObTenantPlanBaselineId {
  OB_UNIS_VERSION(1);

public:
  ObTenantPlanBaselineId() : tenant_id_(common::OB_INVALID_ID), plan_baseline_id_(common::OB_INVALID_ID)
  {}
  ObTenantPlanBaselineId(const uint64_t tenant_id, const uint64_t plan_baseline_id)
      : tenant_id_(tenant_id), plan_baseline_id_(plan_baseline_id)
  {}
  bool operator==(const ObTenantPlanBaselineId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (plan_baseline_id_ == rhs.plan_baseline_id_);
  }
  bool operator!=(const ObTenantPlanBaselineId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantPlanBaselineId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = plan_baseline_id_ < rhs.plan_baseline_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&plan_baseline_id_, sizeof(plan_baseline_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (plan_baseline_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(plan_baseline_id));
  uint64_t tenant_id_;
  uint64_t plan_baseline_id_;
};

struct ObTenantOutlineId {
  OB_UNIS_VERSION(1);

public:
  ObTenantOutlineId() : tenant_id_(common::OB_INVALID_ID), outline_id_(common::OB_INVALID_ID)
  {}
  ObTenantOutlineId(const uint64_t tenant_id, const uint64_t outline_id)
      : tenant_id_(tenant_id), outline_id_(outline_id)
  {}
  bool operator==(const ObTenantOutlineId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (outline_id_ == rhs.outline_id_);
  }
  bool operator!=(const ObTenantOutlineId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantOutlineId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = outline_id_ < rhs.outline_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&outline_id_, sizeof(outline_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (outline_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(outline_id));
  uint64_t tenant_id_;
  uint64_t outline_id_;
};

// For managing privilege
struct ObTenantUserId {
  OB_UNIS_VERSION(1);

public:
  ObTenantUserId() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTenantUserId(const uint64_t tenant_id, const uint64_t user_id) : tenant_id_(tenant_id), user_id_(user_id)
  {}
  bool operator==(const ObTenantUserId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_);
  }
  bool operator!=(const ObTenantUserId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantUserId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id));
  uint64_t tenant_id_;
  uint64_t user_id_;
};

// For managing privilege
struct ObTenantUrObjId {
  OB_UNIS_VERSION(1);

public:
  ObTenantUrObjId()
      : tenant_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID)
  {}
  ObTenantUrObjId(const uint64_t tenant_id, const uint64_t grantee_id, const uint64_t obj_id, const uint64_t obj_type,
      const uint64_t col_id)
      : tenant_id_(tenant_id), grantee_id_(grantee_id), obj_id_(obj_id), obj_type_(obj_type), col_id_(col_id)
  {}
  bool operator==(const ObTenantUrObjId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (grantee_id_ == rhs.grantee_id_) && (obj_id_ == rhs.obj_id_) &&
           (obj_type_ == rhs.obj_type_) && (col_id_ == rhs.col_id_);
  }
  bool operator!=(const ObTenantUrObjId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantUrObjId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = grantee_id_ < rhs.grantee_id_;
      if (false == bret && grantee_id_ == rhs.grantee_id_) {
        bret = obj_id_ < rhs.obj_id_;
        if (false == bret && obj_id_ == rhs.obj_id_) {
          bret = obj_type_ < rhs.obj_type_;
          if (false == bret && obj_type_ == rhs.obj_type_) {
            bret = col_id_ < rhs.col_id_;
          }
        }
      }
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&grantee_id_, sizeof(grantee_id_), hash_ret);
    hash_ret = common::murmurhash(&obj_id_, sizeof(obj_id_), hash_ret);
    hash_ret = common::murmurhash(&obj_type_, sizeof(obj_type_), hash_ret);
    hash_ret = common::murmurhash(&col_id_, sizeof(col_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (grantee_id_ != common::OB_INVALID_ID) &&
           (obj_id_ != common::OB_INVALID_ID) && (obj_type_ != common::OB_INVALID_ID) &&
           (col_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(grantee_id), K_(obj_id), K_(obj_type), K_(col_id));
  uint64_t tenant_id_;
  uint64_t grantee_id_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t col_id_;
};

class ObPrintPrivSet {
public:
  explicit ObPrintPrivSet(ObPrivSet priv_set) : priv_set_(priv_set)
  {}

  DECLARE_TO_STRING;

private:
  ObPrivSet priv_set_;
};

class ObPrintPackedPrivArray {
public:
  explicit ObPrintPackedPrivArray(const ObPackedPrivArray &packed_priv_array) : packed_priv_array_(packed_priv_array)
  {}

  DECLARE_TO_STRING;

private:
  const ObPackedPrivArray &packed_priv_array_;
};

class ObPriv {
  OB_UNIS_VERSION_V(1);

public:
  ObPriv()
      : tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        schema_version_(1),
        priv_set_(0),
        priv_array_()
  {}
  ObPriv(common::ObIAllocator *allocator)
      : tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        schema_version_(1),
        priv_set_(0),
        priv_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(*allocator))
  {}
  ObPriv(const uint64_t tenant_id, const uint64_t user_id, const int64_t schema_version, const ObPrivSet priv_set)
      : tenant_id_(tenant_id), user_id_(user_id), schema_version_(schema_version), priv_set_(priv_set), priv_array_()
  {}

  virtual ~ObPriv()
  {}
  ObPriv &operator=(const ObPriv &other);
  static bool cmp_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  {
    return (lhs->get_tenant_user_id() < tenant_user_id);
  }
  static bool equal_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  {
    return (lhs->get_tenant_user_id() == tenant_user_id);
  }
  static bool cmp_tenant_id(const ObPriv *lhs, const uint64_t tenant_id)
  {
    return (lhs->get_tenant_id() < tenant_id);
  }
  ObTenantUserId get_tenant_user_id() const
  {
    return ObTenantUserId(tenant_id_, user_id_);
  }

  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_user_id(const uint64_t user_id)
  {
    user_id_ = user_id;
  }
  inline void set_schema_version(const uint64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline void set_priv(const ObPrivType priv)
  {
    priv_set_ |= priv;
  }
  inline void set_priv_set(const ObPrivSet priv_set)
  {
    priv_set_ = priv_set;
  }
  inline void reset_priv_set()
  {
    priv_set_ = 0;
  }
  inline void set_obj_privs(const ObPackedObjPriv obj_privs)
  {
    priv_set_ = obj_privs;
  }
  int set_priv_array(const ObPackedPrivArray &other)
  {
    return priv_array_.assign(other);
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  inline uint64_t get_user_id() const
  {
    return user_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline ObPrivSet get_priv_set() const
  {
    return priv_set_;
  }
  inline ObPrivType get_priv(const ObPrivType priv) const
  {
    return priv_set_ & priv;
  }
  inline const ObPackedPrivArray &get_priv_array() const
  {
    return priv_array_;
  }
  inline ObPackedObjPriv get_obj_privs() const
  {
    return priv_set_;
  }
  virtual void reset();
  int64_t get_convert_size() const;
  virtual bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != user_id_ && schema_version_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(schema_version), "privileges", ObPrintPrivSet(priv_set_));

protected:
  uint64_t tenant_id_;
  uint64_t user_id_;
  int64_t schema_version_;
  ObPrivSet priv_set_;
  // ObPrivSet ora_sys_priv_set_;
  ObPackedPrivArray priv_array_;
};

// Not used now
class ObUserInfoHashWrapper {
public:
  ObUserInfoHashWrapper() : tenant_id_(common::OB_INVALID_ID)
  {}
  ObUserInfoHashWrapper(uint64_t tenant_id, const common::ObString &user_name)
      : tenant_id_(tenant_id), user_name_(user_name)
  {}
  ~ObUserInfoHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObUserInfoHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline const common::ObString &get_user_name() const
  {
    return user_name_;
  }

private:
  uint64_t tenant_id_;
  common::ObString user_name_;
};

inline uint64_t ObUserInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(user_name_.ptr(), user_name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObUserInfoHashWrapper::operator==(const ObUserInfoHashWrapper &other) const
{
  return (tenant_id_ == other.tenant_id_) && (user_name_ == other.user_name_);
}

enum class ObSSLType : int {
  SSL_TYPE_NOT_SPECIFIED = 0,
  SSL_TYPE_NONE,
  SSL_TYPE_ANY,
  SSL_TYPE_X509,
  SSL_TYPE_SPECIFIED,
  SSL_TYPE_MAX
};

common::ObString get_ssl_type_string(const ObSSLType ssl_type);
ObSSLType get_ssl_type_from_string(const common::ObString &ssl_type_str);

enum class ObSSLSpecifiedType : int {
  SSL_SPEC_TYPE_CIPHER = 0,
  SSL_SPEC_TYPE_ISSUER,
  SSL_SPEC_TYPE_SUBJECT,
  SSL_SPEC_TYPE_MAX
};

const char *get_ssl_spec_type_str(const ObSSLSpecifiedType ssl_spec_type);

enum ObUserType {
  OB_USER = 0,
  OB_ROLE,
  OB_TYPE_MAX,
};

#define ADMIN_OPTION_SHIFT 0
#define DISABLE_FLAG_SHIFT 1

class ObUserInfo : public ObSchema, public ObPriv {
  OB_UNIS_VERSION(1);

public:
  ObUserInfo()
      : ObSchema(),
        ObPriv(),
        user_name_(),
        host_name_(),
        passwd_(),
        info_(),
        locked_(false),
        ssl_type_(ObSSLType::SSL_TYPE_NOT_SPECIFIED),
        ssl_cipher_(),
        x509_issuer_(),
        x509_subject_(),
        type_(OB_USER),
        grantee_id_array_(),
        role_id_array_(),
        profile_id_(common::OB_INVALID_ID),
        password_last_changed_timestamp_(common::OB_INVALID_TIMESTAMP),
        role_id_option_array_(),
        max_connections_(0),
        max_user_connections_(0)
  {}
  explicit ObUserInfo(common::ObIAllocator *allocator);
  virtual ~ObUserInfo();
  ObUserInfo(const ObUserInfo &other);
  ObUserInfo &operator=(const ObUserInfo &other);
  static bool cmp(const ObUserInfo *lhs, const ObUserInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() < rhs->get_tenant_user_id() : false;
  }
  static bool equal(const ObUserInfo *lhs, const ObUserInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() == rhs->get_tenant_user_id() : false;
  }

  // set methods
  inline int set_user_name(const char *user_name)
  {
    return deep_copy_str(user_name, user_name_);
  }
  inline int set_user_name(const common::ObString &user_name)
  {
    return deep_copy_str(user_name, user_name_);
  }
  inline int set_host(const char *host_name)
  {
    return deep_copy_str(host_name, host_name_);
  }
  inline int set_host(const common::ObString &host_name)
  {
    return deep_copy_str(host_name, host_name_);
  }
  inline int set_passwd(const char *passwd)
  {
    return deep_copy_str(passwd, passwd_);
  }
  inline int set_passwd(const common::ObString &passwd)
  {
    return deep_copy_str(passwd, passwd_);
  }
  inline int set_info(const char *info)
  {
    return deep_copy_str(info, info_);
  }
  inline int set_info(const common::ObString &info)
  {
    return deep_copy_str(info, info_);
  }
  inline void set_is_locked(const bool locked)
  {
    locked_ = locked;
  }
  inline void set_ssl_type(const ObSSLType ssl_type)
  {
    ssl_type_ = ssl_type;
  }
  inline int set_ssl_cipher(const char *ssl_cipher)
  {
    return deep_copy_str(ssl_cipher, ssl_cipher_);
  }
  inline int set_ssl_cipher(const common::ObString &ssl_cipher)
  {
    return deep_copy_str(ssl_cipher, ssl_cipher_);
  }
  inline int set_x509_issuer(const char *x509_issuer)
  {
    return deep_copy_str(x509_issuer, x509_issuer_);
  }
  inline int set_x509_issuer(const common::ObString &x509_issuer)
  {
    return deep_copy_str(x509_issuer, x509_issuer_);
  }
  inline int set_x509_subject(const char *x509_subject)
  {
    return deep_copy_str(x509_subject, x509_subject_);
  }
  inline int set_x509_subject(const common::ObString &x509_subject)
  {
    return deep_copy_str(x509_subject, x509_subject_);
  }
  inline void set_type(const uint64_t type)
  {
    type_ = type;
  }
  inline void set_profile_id(const uint64_t profile_id)
  {
    profile_id_ = profile_id;
  }
  inline void set_password_last_changed(int64_t ts)
  {
    password_last_changed_timestamp_ = ts;
  }
  inline void set_max_connections(uint64_t max_connections)
  {
    max_connections_ = max_connections;
  }
  inline void set_max_user_connections(uint64_t max_user_connections)
  {
    max_user_connections_ = max_user_connections;
  }
  // get methods
  inline const char *get_user_name() const
  {
    return extract_str(user_name_);
  }
  inline const common::ObString &get_user_name_str() const
  {
    return user_name_;
  }
  inline const char *get_host_name() const
  {
    return extract_str(host_name_);
  }
  inline const common::ObString &get_host_name_str() const
  {
    return host_name_;
  }
  inline const char *get_passwd() const
  {
    return extract_str(passwd_);
  }
  inline const common::ObString &get_passwd_str() const
  {
    return passwd_;
  }
  inline const char *get_info() const
  {
    return extract_str(info_);
  }
  inline const common::ObString &get_info_str() const
  {
    return info_;
  }
  inline bool get_is_locked() const
  {
    return locked_;
  }
  inline ObSSLType get_ssl_type() const
  {
    return ssl_type_;
  }
  inline const common::ObString get_ssl_type_str() const
  {
    return get_ssl_type_string(ssl_type_);
  }
  inline const char *get_ssl_cipher() const
  {
    return extract_str(ssl_cipher_);
  }
  inline const common::ObString &get_ssl_cipher_str() const
  {
    return ssl_cipher_;
  }
  inline const char *get_x509_issuer() const
  {
    return extract_str(x509_issuer_);
  }
  inline const common::ObString &get_x509_issuer_str() const
  {
    return x509_issuer_;
  }
  inline const char *get_x509_subject() const
  {
    return extract_str(x509_subject_);
  }
  inline const common::ObString &get_x509_subject_str() const
  {
    return x509_subject_;
  }
  inline uint64_t get_profile_id() const
  {
    return profile_id_;
  }
  inline int64_t get_password_last_changed() const
  {
    return password_last_changed_timestamp_;
  }
  inline uint64_t get_max_connections() const
  {
    return max_connections_;
  }
  inline uint64_t get_max_user_connections() const
  {
    return max_user_connections_;
  }
  // role
  inline bool is_role() const
  {
    return OB_ROLE == type_;
  }
  inline int get_grantee_count() const
  {
    return grantee_id_array_.count();
  }
  inline int get_role_count() const
  {
    return role_id_array_.count();
  }
  const common::ObSEArray<uint64_t, 8> &get_grantee_id_array() const
  {
    return grantee_id_array_;
  }
  const common::ObSEArray<uint64_t, 8> &get_role_id_array() const
  {
    return role_id_array_;
  }
  const common::ObSEArray<uint64_t, 8> &get_role_id_option_array() const
  {
    return role_id_option_array_;
  }
  int add_grantee_id(const uint64_t id)
  {
    return grantee_id_array_.push_back(id);
  }
  int add_role_id(const uint64_t id, const uint64_t admin_option = NO_OPTION, const uint64_t disable_flag = 0);
  void set_admin_option(uint64_t &option, const uint64_t admin_option)
  {
    option |= (admin_option << ADMIN_OPTION_SHIFT);
  }
  void set_disable_flag(uint64_t &option, const uint64_t disable_flag)
  {
    option |= (disable_flag << DISABLE_FLAG_SHIFT);
  }
  int get_nth_role_option(uint64_t nth, uint64_t &option) const;
  uint64_t get_admin_option(uint64_t option) const
  {
    return 1 & (option >> ADMIN_OPTION_SHIFT);
  }
  uint64_t get_disable_option(uint64_t option) const
  {
    return 1 & (option >> DISABLE_FLAG_SHIFT);
  }

  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(user_name), K_(host_name), "privileges", ObPrintPrivSet(priv_set_),
      K_(info), K_(locked), K_(ssl_type), K_(ssl_cipher), K_(x509_issuer), K_(x509_subject), K_(type),
      K_(grantee_id_array), K_(role_id_array), K_(profile_id));
  bool role_exists(const uint64_t role_id, const uint64_t option) const;
  int get_seq_by_role_id(uint64_t role_id, uint64_t &seq) const;

private:
  common::ObString user_name_;
  common::ObString host_name_;
  common::ObString passwd_;
  common::ObString info_;
  bool locked_;
  ObSSLType ssl_type_;
  common::ObString ssl_cipher_;
  common::ObString x509_issuer_;
  common::ObString x509_subject_;

  int type_;
  common::ObSEArray<uint64_t, 8> grantee_id_array_;  // Record role granted to user
  common::ObSEArray<uint64_t, 8> role_id_array_;     // Record which roles the user/role has
  uint64_t profile_id_;
  int64_t password_last_changed_timestamp_;
  common::ObSEArray<uint64_t, 8> role_id_option_array_;  // Record which roles the user/role has
  uint64_t max_connections_;
  uint64_t max_user_connections_;
};

struct ObDBPrivSortKey {
  ObDBPrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID), sort_(0)
  {}
  ObDBPrivSortKey(const uint64_t tenant_id, const uint64_t user_id, const uint64_t sort_value)
      : tenant_id_(tenant_id), user_id_(user_id), sort_(sort_value)
  {}
  bool operator==(const ObDBPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_) && (sort_ == rhs.sort_);
  }
  bool operator!=(const ObDBPrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObDBPrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = sort_ > rhs.sort_;  // sort values of 'sort_' from big to small
      }
    }
    return bret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K(sort_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t sort_;
};

struct ObOriginalDBKey {
  ObOriginalDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObOriginalDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObOriginalDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_) && (db_ == rhs.db_);
  }
  bool operator!=(const ObOriginalDBKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObOriginalDBKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
    }
    return bret;
  }
  // Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(db_.ptr(), db_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
};

struct ObSysPrivKey {
  ObSysPrivKey() : tenant_id_(common::OB_INVALID_ID), grantee_id_(common::OB_INVALID_ID)
  {}
  ObSysPrivKey(const uint64_t tenant_id, const uint64_t user_id) : tenant_id_(tenant_id), grantee_id_(user_id)
  {}
  bool operator==(const ObSysPrivKey &rhs) const
  {
    return ((tenant_id_ == rhs.tenant_id_) && (grantee_id_ == rhs.grantee_id_));
  }
  bool operator!=(const ObSysPrivKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObSysPrivKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = grantee_id_ < rhs.grantee_id_;
    }
    return bret;
  }
  // Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&grantee_id_, sizeof(grantee_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (grantee_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(grantee_id));
  uint64_t tenant_id_;
  uint64_t grantee_id_;
};

class ObDBPriv : public ObSchema, public ObPriv {
  OB_UNIS_VERSION(1);

public:
  ObDBPriv() : ObPriv(), db_(), sort_(0)
  {}
  explicit ObDBPriv(common::ObIAllocator *allocator) : ObSchema(allocator), ObPriv(allocator), db_(), sort_(0)
  {}
  virtual ~ObDBPriv()
  {}
  ObDBPriv(const ObDBPriv &other) : ObSchema(), ObPriv()
  {
    *this = other;
  }
  ObDBPriv &operator=(const ObDBPriv &other);

  static bool cmp(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? (lhs->get_sort_key() < rhs->get_sort_key()) : false;
  }
  static bool cmp_sort_key(const ObDBPriv *lhs, const ObDBPrivSortKey &sort_key)
  {
    return NULL != lhs ? lhs->get_sort_key() < sort_key : false;
  }
  ObDBPrivSortKey get_sort_key() const
  {
    return ObDBPrivSortKey(tenant_id_, user_id_, sort_);
  }
  static bool equal(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false;
  }  // point check
  ObOriginalDBKey get_original_key() const
  {
    return ObOriginalDBKey(tenant_id_, user_id_, db_);
  }

  // set methods
  inline int set_database_name(const char *db)
  {
    return deep_copy_str(db, db_);
  }
  inline int set_database_name(const common::ObString &db)
  {
    return deep_copy_str(db, db_);
  }
  inline void set_sort(const uint64_t sort)
  {
    sort_ = sort;
  }

  // get methods
  inline const char *get_database_name() const
  {
    return extract_str(db_);
  }
  inline const common::ObString &get_database_name_str() const
  {
    return db_;
  }
  inline uint64_t get_sort() const
  {
    return sort_;
  }
  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), "privileges", ObPrintPrivSet(priv_set_));

private:
  common::ObString db_;
  uint64_t sort_;
};

// In order to find in table_privs_ whether a table is authorized under a certain db
struct ObTablePrivDBKey {
  ObTablePrivDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObTablePrivDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_) && (db_ == rhs.db_);
  }
  bool operator!=(const ObTablePrivDBKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTablePrivDBKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = db_ < rhs.db_;
      }
    }
    return bret;
  }
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
};

struct ObTablePrivSortKey {
  ObTablePrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivSortKey(
      const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db, const common::ObString &table)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db), table_(table)
  {}
  bool operator==(const ObTablePrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_) && (db_ == rhs.db_) && (table_ == rhs.table_);
  }
  bool operator!=(const ObTablePrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTablePrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = db_ < rhs.db_;
        if (false == bret && db_ == rhs.db_) {
          bret = table_ < rhs.table_;
        }
      }
    }
    return bret;
  }
  // Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(db_.ptr(), db_.length(), hash_ret);
    hash_ret = common::murmurhash(table_.ptr(), table_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
};

struct ObObjPrivSortKey {
  ObObjPrivSortKey()
      : tenant_id_(common::OB_INVALID_ID),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID)
  {}
  ObObjPrivSortKey(const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_type, const uint64_t col_id,
      const uint64_t grantor_id, const uint64_t grantee_id)
      : tenant_id_(tenant_id),
        obj_id_(obj_id),
        obj_type_(obj_type),
        col_id_(col_id),
        grantor_id_(grantor_id),
        grantee_id_(grantee_id)
  {}
  bool operator==(const ObObjPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (obj_id_ == rhs.obj_id_) && (obj_type_ == rhs.obj_type_) &&
           (col_id_ == rhs.col_id_) && (grantor_id_ == rhs.grantor_id_) && (grantee_id_ == rhs.grantee_id_);
  }
  bool operator!=(const ObObjPrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObObjPrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = grantee_id_ < rhs.grantee_id_;
      if (false == bret && grantee_id_ == rhs.grantee_id_) {
        bret = obj_id_ < rhs.obj_id_;
        if (false == bret && obj_id_ == rhs.obj_id_) {
          bret = obj_type_ < rhs.obj_type_;
          if (false == bret && obj_type_ == rhs.obj_type_) {
            bret = col_id_ < rhs.col_id_;
            if (false == bret && col_id_ == rhs.col_id_) {
              bret = grantor_id_ < rhs.grantor_id_;
            }
          }
        }
      }
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&grantee_id_, sizeof(grantee_id_), hash_ret);
    hash_ret = common::murmurhash(&obj_id_, sizeof(obj_id_), hash_ret);
    hash_ret = common::murmurhash(&obj_type_, sizeof(obj_type_), hash_ret);
    hash_ret = common::murmurhash(&col_id_, sizeof(col_id_), hash_ret);
    hash_ret = common::murmurhash(&grantor_id_, sizeof(grantor_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (obj_id_ != common::OB_INVALID_ID) &&
           (obj_type_ != common::OB_INVALID_ID) && (grantor_id_ != common::OB_INVALID_ID) &&
           (grantee_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(obj_id), K_(obj_type), K_(col_id), K_(grantor_id), K_(grantee_id));
  uint64_t tenant_id_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t col_id_;
  uint64_t grantor_id_;
  uint64_t grantee_id_;
};

typedef common::ObSEArray<share::schema::ObObjPrivSortKey, 4> ObObjPrivSortKeyArray;

class ObTablePriv : public ObSchema, public ObPriv {
  OB_UNIS_VERSION(1);

public:
  // constructor and destructor
  ObTablePriv() : ObSchema(), ObPriv()
  {}
  explicit ObTablePriv(common::ObIAllocator *allocator) : ObSchema(allocator), ObPriv(allocator)
  {}
  ObTablePriv(const ObTablePriv &other) : ObSchema(), ObPriv()
  {
    *this = other;
  }
  virtual ~ObTablePriv()
  {}

  // operator=
  ObTablePriv &operator=(const ObTablePriv &other);

  // for sort
  ObTablePrivSortKey get_sort_key() const
  {
    return ObTablePrivSortKey(tenant_id_, user_id_, db_, table_);
  }
  static bool cmp(const ObTablePriv *lhs, const ObTablePriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false;
  }
  static bool cmp_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  {
    return NULL != lhs ? lhs->get_sort_key() < sort_key : false;
  }
  static bool equal(const ObTablePriv *lhs, const ObTablePriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false;
  }
  static bool equal_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  {
    return NULL != lhs ? lhs->get_sort_key() == sort_key : false;
  }

  ObTablePrivDBKey get_db_key() const
  {
    return ObTablePrivDBKey(tenant_id_, user_id_, db_);
  }
  static bool cmp_db_key(const ObTablePriv *lhs, const ObTablePrivDBKey &db_key)
  {
    return lhs->get_db_key() < db_key;
  }

  // set methods
  inline int set_database_name(const char *db)
  {
    return deep_copy_str(db, db_);
  }
  inline int set_database_name(const common::ObString &db)
  {
    return deep_copy_str(db, db_);
  }
  inline int set_table_name(const char *table)
  {
    return deep_copy_str(table, table_);
  }
  inline int set_table_name(const common::ObString &table)
  {
    return deep_copy_str(table, table_);
  }

  // get methods
  inline const char *get_database_name() const
  {
    return extract_str(db_);
  }
  inline const common::ObString &get_database_name_str() const
  {
    return db_;
  }
  inline const char *get_table_name() const
  {
    return extract_str(table_);
  }
  inline const common::ObString &get_table_name_str() const
  {
    return table_;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table), "privileges", ObPrintPrivSet(priv_set_));
  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  common::ObString db_;
  common::ObString table_;
};

class ObObjPriv : public ObSchema, public ObPriv {
  OB_UNIS_VERSION(1);

public:
  // constructor and destructor
  ObObjPriv() : ObSchema(), ObPriv()
  {}
  explicit ObObjPriv(common::ObIAllocator *allocator) : ObSchema(allocator), ObPriv()
  {}
  ObObjPriv(const ObObjPriv &other) : ObSchema(), ObPriv()
  {
    *this = other;
  }
  virtual ~ObObjPriv()
  {}

  // operator=
  ObObjPriv &operator=(const ObObjPriv &other);

  // for sort
  ObObjPrivSortKey get_sort_key() const
  {
    return ObObjPrivSortKey(tenant_id_, obj_id_, obj_type_, col_id_, grantor_id_, grantee_id_);
  }

  ObTenantUrObjId get_tenant_ur_obj_id() const
  {
    return ObTenantUrObjId(tenant_id_, grantee_id_, obj_id_, obj_type_, col_id_);
  }

  static bool cmp_tenant_ur_obj_id(const ObObjPriv *lhs, const ObTenantUrObjId &rhs)
  {
    return (lhs->get_tenant_ur_obj_id() < rhs);
  }
  static bool cmp(const ObObjPriv *lhs, const ObObjPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false;
  }
  static bool cmp_sort_key(const ObObjPriv *lhs, const ObObjPrivSortKey &sort_key)
  {
    return NULL != lhs ? lhs->get_sort_key() < sort_key : false;
  }
  static bool equal(const ObObjPriv *lhs, const ObObjPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false;
  }
  static bool equal_sort_key(const ObObjPriv *lhs, const ObObjPrivSortKey &sort_key)
  {
    return NULL != lhs ? lhs->get_sort_key() == sort_key : false;
  }

  // ObTablePrivDBKey get_db_key() const
  // { return ObTablePrivDBKey(tenant_id_, user_id_, db_); }
  // static bool cmp_db_key(const ObTablePriv *lhs, const ObTablePrivDBKey &db_key)
  // { return lhs->get_db_key() < db_key; }

  // set methods
  inline void set_obj_id(const uint64_t obj_id)
  {
    obj_id_ = obj_id;
  }
  inline void set_col_id(const uint64_t col_id)
  {
    col_id_ = col_id;
  }
  inline void set_grantor_id(const uint64_t grantor_id)
  {
    grantor_id_ = grantor_id;
  }
  inline void set_grantee_id(const uint64_t grantee_id)
  {
    grantee_id_ = grantee_id;
  }
  inline void set_objtype(const uint64_t objtype)
  {
    obj_type_ = objtype;
  }
  // get methods
  inline uint64_t get_objtype() const
  {
    return obj_type_;
  }
  inline uint64_t get_obj_id() const
  {
    return obj_id_;
  }
  inline uint64_t get_col_id() const
  {
    return col_id_;
  }
  inline uint64_t get_grantee_id() const
  {
    return grantee_id_;
  }
  inline uint64_t get_grantor_id() const
  {
    return grantor_id_;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(obj_id), K_(obj_type), K_(col_id), "privileges",
      ObPrintPrivSet(priv_set_), K_(grantor_id), K_(grantee_id));
  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t col_id_;
  uint64_t grantor_id_;
  uint64_t grantee_id_;
};

enum ObPrivLevel {
  OB_PRIV_INVALID_LEVEL,
  OB_PRIV_USER_LEVEL,
  OB_PRIV_DB_LEVEL,
  OB_PRIV_TABLE_LEVEL,
  OB_PRIV_DB_ACCESS_LEVEL,
  OB_PRIV_SYS_ORACLE_LEVEL, /* oracle-mode system privilege */
  OB_PRIV_OBJ_ORACLE_LEVEL, /* oracle-mode object privilege */
  OB_PRIV_MAX_LEVEL,
};

const char *ob_priv_level_str(const ObPrivLevel grant_level);

struct ObNeedPriv {
  ObNeedPriv(const common::ObString &db, const common::ObString &table, ObPrivLevel priv_level, ObPrivSet priv_set,
      const bool is_sys_table)
      : db_(db), table_(table), priv_level_(priv_level), priv_set_(priv_set), is_sys_table_(is_sys_table)
  {}
  ObNeedPriv() : db_(), table_(), priv_level_(OB_PRIV_INVALID_LEVEL), priv_set_(0), is_sys_table_(false)
  {}
  int deep_copy(const ObNeedPriv &other, common::ObIAllocator &allocator);
  common::ObString db_;
  common::ObString table_;
  ObPrivLevel priv_level_;
  ObPrivSet priv_set_;
  bool is_sys_table_;  // May be used to represent the table of schema metadata
  TO_STRING_KV(K_(db), K_(table), K_(priv_set), K_(priv_level), K_(is_sys_table));
};

struct ObStmtNeedPrivs {
  typedef common::ObFixedArray<ObNeedPriv, common::ObIAllocator> NeedPrivs;
  explicit ObStmtNeedPrivs(common::ObIAllocator &alloc) : need_privs_(alloc)
  {}
  ObStmtNeedPrivs() : need_privs_()
  {}
  ~ObStmtNeedPrivs()
  {
    reset();
  }
  void reset()
  {
    need_privs_.reset();
  }
  int deep_copy(const ObStmtNeedPrivs &other, common::ObIAllocator &allocator);
  NeedPrivs need_privs_;
  TO_STRING_KV(K_(need_privs));
};

#define CHECK_FLAG_NORMAL 0X0
#define CHECK_FLAG_DIRECT 0X1
#define CHECK_FLAG_WITH_GRANT_OPTION 0x2
/* flag Can proceed or */

// Define the permissions required in oracle mode.
// Record a set of system permissions, or an object permission
struct ObOraNeedPriv {
  ObOraNeedPriv()
      : db_name_(),
        grantee_id_(common::OB_INVALID_ID),
        obj_id_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        check_flag_(false),
        obj_privs_(0),
        owner_id_(common::OB_INVALID_ID)
  {}
  ObOraNeedPriv(const common::ObString &db_name, uint64_t grantee_id, uint64_t obj_id, uint64_t col_id,
      uint64_t obj_type, uint32_t check_flag, ObPackedObjPriv obj_privs, uint64_t owner_id)
      : db_name_(db_name),
        grantee_id_(grantee_id),
        obj_id_(obj_id),
        col_id_(col_id),
        obj_type_(obj_type),
        check_flag_(check_flag),
        obj_privs_(obj_privs),
        owner_id_(owner_id)
  {}
  ~ObOraNeedPriv()
  {}
  int deep_copy(const ObOraNeedPriv &other, common::ObIAllocator &allocator);
  bool same_obj(const ObOraNeedPriv &other);
  // ObOraNeedPriv& operator=(const ObOraNeedPriv &other);
  common::ObString db_name_;
  uint64_t grantee_id_;
  uint64_t obj_id_;
  uint64_t col_id_;
  uint64_t obj_type_;
  uint64_t check_flag_;
  ObPackedObjPriv obj_privs_;
  uint64_t owner_id_;
  TO_STRING_KV(
      K_(db_name), K_(grantee_id), K_(obj_id), K_(col_id), K_(obj_type), K_(check_flag), K_(obj_privs), K_(owner_id));
};

// Define the permissions required for a statement in oracle mode. Use an array to record,
// the system permissions and multiple object permissions are recorded in the array
struct ObStmtOraNeedPrivs {
  typedef common::ObFixedArray<ObOraNeedPriv, common::ObIAllocator> OraNeedPrivs;
  explicit ObStmtOraNeedPrivs(common::ObIAllocator &alloc) : need_privs_(alloc)
  {}
  ObStmtOraNeedPrivs() : need_privs_()
  {}
  ~ObStmtOraNeedPrivs()
  {
    reset();
  }
  void reset()
  {
    need_privs_.reset();
  }
  int deep_copy(const ObStmtOraNeedPrivs &other, common::ObIAllocator &allocator);
  OraNeedPrivs need_privs_;
  TO_STRING_KV(K_(need_privs));
};

struct ObSessionPrivInfo {
  ObSessionPrivInfo()
      : tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        user_name_(),
        host_name_(),
        db_(),
        user_priv_set_(0),
        db_priv_set_(0),
        effective_tenant_id_(common::OB_INVALID_ID),
        enable_role_id_array_()
  {}
  ObSessionPrivInfo(const uint64_t tenant_id, const uint64_t effective_tenant_id, const uint64_t user_id,
      const common::ObString &db, const ObPrivSet user_priv_set, const ObPrivSet db_priv_set)
      : tenant_id_(tenant_id),
        user_id_(user_id),
        user_name_(),
        host_name_(),
        db_(db),
        user_priv_set_(user_priv_set),
        db_priv_set_(db_priv_set),
        effective_tenant_id_(effective_tenant_id),
        enable_role_id_array_()
  {}

  virtual ~ObSessionPrivInfo()
  {}

  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    effective_tenant_id_ = common::OB_INVALID_ID;
    user_id_ = common::OB_INVALID_ID;
    user_name_.reset();
    host_name_.reset();
    db_.reset();
    user_priv_set_ = 0;
    db_priv_set_ = 0;
    enable_role_id_array_.reset();
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  bool is_tenant_changed() const
  {
    return common::OB_INVALID_ID != effective_tenant_id_ && tenant_id_ != effective_tenant_id_;
  }
  void set_effective_tenant_id(uint64_t effective_tenant_id)
  {
    effective_tenant_id_ = effective_tenant_id;
  }
  uint64_t get_effective_tenant_id()
  {
    return effective_tenant_id_;
  }
  virtual TO_STRING_KV(K_(tenant_id), K_(effective_tenant_id), K_(user_id), K_(user_name), K_(host_name), K_(db),
      K_(user_priv_set), K_(db_priv_set));

  uint64_t tenant_id_;  // for privilege.Current login tenant. if normal tenant access
                        // sys tenant's object should use other method for priv checking.
  uint64_t user_id_;
  common::ObString user_name_;
  common::ObString host_name_;
  common::ObString db_;  // db name in current session
  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;  // user's db_priv_set of db
  // Only used for privilege check to determine whether there are currently tenants, otherwise the value is illegal
  uint64_t effective_tenant_id_;
  common::ObSEArray<uint64_t, 8> enable_role_id_array_;
};

struct ObUserLoginInfo {
  ObUserLoginInfo()
  {}
  ObUserLoginInfo(const common::ObString &tenant_name, const common::ObString &user_name,
      const common::ObString &client_ip, const common::ObString &passwd, const common::ObString &db)
      : tenant_name_(tenant_name),
        user_name_(user_name),
        client_ip_(client_ip),
        passwd_(passwd),
        db_(db),
        scramble_str_()
  {}

  ObUserLoginInfo(const common::ObString &tenant_name, const common::ObString &user_name,
      const common::ObString &client_ip, const common::ObString &passwd, const common::ObString &db,
      const common::ObString &scramble_str)
      : tenant_name_(tenant_name),
        user_name_(user_name),
        client_ip_(client_ip),
        passwd_(passwd),
        db_(db),
        scramble_str_(scramble_str)
  {}

  TO_STRING_KV(K_(tenant_name), K_(user_name), K_(client_ip), K_(db), K_(scramble_str));
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString client_ip_;  // client ip for current user
  common::ObString passwd_;
  common::ObString db_;
  common::ObString scramble_str_;
};

// oracle compatible: define u/r system permissions
class ObSysPriv : public ObSchema, public ObPriv {
  OB_UNIS_VERSION(1);

public:
  ObSysPriv() : ObPriv(), grantee_id_(common::OB_INVALID_ID)
  {}
  explicit ObSysPriv(common::ObIAllocator *allocator) : ObSchema(allocator), ObPriv(allocator)
  {}
  virtual ~ObSysPriv()
  {}
  ObSysPriv(const ObSysPriv &other) : ObSchema(), ObPriv(), grantee_id_(common::OB_INVALID_ID)
  {
    *this = other;
  }
  ObSysPriv &operator=(const ObSysPriv &other);

  static bool cmp_tenant_grantee_id(const ObSysPriv *lhs, const ObTenantUserId &tenant_grantee_id)
  {
    return (lhs->get_tenant_grantee_id() < tenant_grantee_id);
  }
  static bool equal_tenant_grantee_id(const ObSysPriv *lhs, const ObTenantUserId &tenant_grantee_id)
  {
    return (lhs->get_tenant_grantee_id() == tenant_grantee_id);
  }
  static bool cmp_tenant_id(const ObPriv *lhs, const uint64_t tenant_id)
  {
    return (lhs->get_tenant_id() < tenant_id);
  }
  ObTenantUserId get_tenant_grantee_id() const
  {
    return ObTenantUserId(tenant_id_, grantee_id_);
  }
  void set_grantee_id(uint64_t grantee_id)
  {
    grantee_id_ = grantee_id;
  }
  uint64_t get_grantee_id() const
  {
    return grantee_id_;
  }
  void set_revoke()
  {
    user_id_ = 234;
  }
  bool is_revoke()
  {
    return user_id_ == 234;
  }

  static bool cmp(const ObSysPriv *lhs, const ObSysPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? (lhs->get_key() < rhs->get_key()) : false;
  }
  static bool cmp_key(const ObSysPriv *lhs, const ObSysPrivKey &key)
  {
    return NULL != lhs ? lhs->get_key() < key : false;
  }
  ObSysPrivKey get_key() const
  {
    return ObSysPrivKey(tenant_id_, grantee_id_);
  }
  static bool equal(const ObSysPriv *lhs, const ObSysPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_key() == rhs->get_key() : false;
  }  // point check
  // ObSysPrivKey get_original_key() const
  //{ return ObSysPrivKey(tenant_id_, user_id_); }

  // other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(grantee_id), "privileges", ObPrintPrivSet(priv_set_), "packedprivarray",
      ObPrintPackedPrivArray(priv_array_));

private:
  uint64_t grantee_id_;
};

int get_int_value(const common::ObString &str, int64_t &value);

class ObHostnameStuct {
public:
  ObHostnameStuct()
  {}
  ~ObHostnameStuct()
  {}
  static const uint32_t FAKE_PORT = 0;
  static const int MAX_IP_BITS = 128;
  static int get_int_value(const common::ObString &str, int64_t &value);
  static bool calc_ip(const common::ObString &host_ip, common::ObAddr &addr);
  static bool calc_ip_mask(const common::ObString &host_ip_mask, common::ObAddr &mask);
  static bool is_ip_match(const common::ObString &client_ip, common::ObString host_name);
  static bool is_wild_match(const common::ObString &client_ip, const common::ObString &host_name);
  static bool is_in_white_list(const common::ObString &client_ip, common::ObString &ip_white_list);
};

enum ObHintFormat {
  HINT_NORMAL,
  HINT_LOCAL,
};

class ObFixedParam {
public:
  ObFixedParam() : offset_(common::OB_INVALID_INDEX), value_()
  {}
  virtual ~ObFixedParam()
  {}
  bool has_equal_value(const common::ObObj &other_value) const;
  bool is_equal(const ObFixedParam &other_param) const;
  VIRTUAL_TO_STRING_KV(K(offset_), K(value_));
  int64_t offset_;
  common::ObObj value_;
};

class ObMaxConcurrentParam {
  OB_UNIS_VERSION(1);

public:
  static const int64_t UNLIMITED = -1;
  typedef common::ObArray<ObFixedParam, common::ObWrapperAllocator> FixParamStore;
  explicit ObMaxConcurrentParam(common::ObIAllocator *allocator, const common::ObMemAttr &attr = common::ObMemAttr());
  virtual ~ObMaxConcurrentParam();
  int destroy();
  int64_t get_convert_size() const;
  int assign(const ObMaxConcurrentParam &max_concurrent_param);
  int64_t get_concurrent_num() const
  {
    return concurrent_num_;
  }
  int match_fixed_param(const ParamStore &const_param_store, bool &is_match) const;
  bool is_concurrent_limit_param() const
  {
    return concurrent_num_ != UNLIMITED;
  }
  bool is_outline_content_param() const
  {
    return !outline_content_.empty();
  }
  int get_fixed_param_with_offset(int64_t offset, ObFixedParam &fixed_param, bool &is_found) const;
  int same_param_as(const ObMaxConcurrentParam &other, bool &is_same) const;
  void set_mem_attr(const common::ObMemAttr &attr)
  {
    mem_attr_ = attr;
  }
  VIRTUAL_TO_STRING_KV(K_(concurrent_num), K_(outline_content), K_(fixed_param_store));

private:
  int deep_copy_outline_content(const common::ObString &src);
  int deep_copy_param_value(const common::ObObj &src, common::ObObj &dest);

public:
  common::ObIAllocator *allocator_;
  int64_t concurrent_num_;
  common::ObString outline_content_;
  common::ObMemAttr mem_attr_;
  FixParamStore fixed_param_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMaxConcurrentParam);
};

// used for outline manager
class ObOutlineParamsWrapper {
  typedef common::ObArray<ObMaxConcurrentParam *, common::ObWrapperAllocatorWithAttr> OutlineParamsArray;
  OB_UNIS_VERSION(1);

public:
  ObOutlineParamsWrapper();
  explicit ObOutlineParamsWrapper(common::ObIAllocator *allocator);
  ~ObOutlineParamsWrapper();
  int destroy();
  int assign(const ObOutlineParamsWrapper &src);
  int set_allocator(common::ObIAllocator *allocator, const common::ObMemAttr &attr = common::ObMemAttr());
  OutlineParamsArray &get_outline_params()
  {
    return outline_params_;
  }
  const OutlineParamsArray &get_outline_params() const
  {
    return outline_params_;
  }
  ObMaxConcurrentParam *get_outline_param(int64_t index) const;
  int64_t get_convert_size() const;
  bool is_empty() const
  {
    return 0 == outline_params_.count();
  }
  int add_param(const ObMaxConcurrentParam &param);
  int has_param(const ObMaxConcurrentParam &param, bool &has_param) const;
  int has_concurrent_limit_param(bool &has) const;
  int64_t get_param_count() const
  {
    return outline_params_.count();
  }
  void reset_allocator()
  {
    allocator_ = NULL;
    mem_attr_ = common::ObMemAttr();
  }
  void set_mem_attr(const common::ObMemAttr &attr)
  {
    mem_attr_ = attr;
  };
  TO_STRING_KV(K_(outline_params));

private:
  common::ObIAllocator *allocator_;
  OutlineParamsArray outline_params_;
  common::ObMemAttr mem_attr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOutlineParamsWrapper);
};

struct BaselineKey {
  OB_UNIS_VERSION(1);

public:
  BaselineKey() : tenant_id_(common::OB_INVALID_ID), db_id_(common::OB_INVALID_ID)
  {}

  TO_STRING_KV(K_(tenant_id), K_(db_id), K_(constructed_sql), K_(params_info_str));

  inline uint64_t hash(uint64_t seed = 0) const
  {
    uint64_t hash_val = seed;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&db_id_, sizeof(db_id_), hash_val);
    hash_val = constructed_sql_.hash(hash_val);
    hash_val = params_info_str_.hash(hash_val);
    return hash_val;
  }

  inline bool operator==(const BaselineKey &other_key) const
  {
    return tenant_id_ == other_key.tenant_id_ && db_id_ == other_key.db_id_ &&
           constructed_sql_ == other_key.constructed_sql_ && params_info_str_ == other_key.params_info_str_;
  }

  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    db_id_ = common::OB_INVALID_ID;
    constructed_sql_.reset();
    params_info_str_.reset();
  }

  uint64_t tenant_id_;                // tenant id  -- ob sql layer obtain
  uint64_t db_id_;                    // database id -- ob sql layer obtain
  common::ObString constructed_sql_;  // Parameterized text string
  common::ObString params_info_str_;  // ObPlanSet obtain
};

class ObPlanBaselineInfo : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObPlanBaselineInfo();
  explicit ObPlanBaselineInfo(common::ObIAllocator *allocator);
  virtual ~ObPlanBaselineInfo();

  ObPlanBaselineInfo(const ObPlanBaselineInfo &src_schema);
  ObPlanBaselineInfo &operator=(const ObPlanBaselineInfo &src_schema);
  void reset();
  int64_t get_convert_size() const;
  static bool cmp(const ObPlanBaselineInfo *lhs, const ObPlanBaselineInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_plan_baseline_id() < rhs->get_plan_baseline_id() : false;
  }
  static bool equal(const ObPlanBaselineInfo *lhs, const ObPlanBaselineInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_plan_baseline_id() == rhs->get_plan_baseline_id() : false;
  }

  inline uint64_t get_tenant_id() const
  {
    return key_.tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return key_.db_id_;
  }
  inline uint64_t get_plan_baseline_id() const
  {
    return plan_baseline_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline uint64_t get_plan_hash_value() const
  {
    return plan_hash_value_;
  }
  inline bool get_fixed() const
  {
    return fixed_;
  }
  inline bool get_enabled() const
  {
    return enabled_;
  }
  inline bool get_executions() const
  {
    return executions_;
  }
  inline bool get_cpu_time() const
  {
    return cpu_time_;
  }
  inline bool get_hints_all_worked() const
  {
    return hints_all_worked_;
  }

  inline void set_tenant_id(const uint64_t id)
  {
    key_.tenant_id_ = id;
  }
  inline void set_database_id(const uint64_t id)
  {
    key_.db_id_ = id;
  }
  inline void set_plan_baseline_id(uint64_t id)
  {
    plan_baseline_id_ = id;
  }
  inline void set_schema_version(int64_t version)
  {
    schema_version_ = version;
  }
  inline void set_plan_hash_value(uint64_t v)
  {
    plan_hash_value_ = v;
  }
  inline void set_fixed(bool v)
  {
    fixed_ = v;
  }
  inline void set_enabled(bool v)
  {
    enabled_ = v;
  }
  inline void set_executions(bool v)
  {
    executions_ = v;
  }
  inline void set_cpu_time(bool v)
  {
    cpu_time_ = v;
  }
  inline void set_hints_all_worked(bool hints_all_worked)
  {
    hints_all_worked_ = hints_all_worked;
  }

  // int set_plan_hash_value(const common::number::ObNumber &num);

  int set_sql_id(const char *sql_id)
  {
    return deep_copy_str(sql_id, sql_id_);
  }
  int set_sql_id(const common::ObString &sql_id)
  {
    return deep_copy_str(sql_id, sql_id_);
  }

  inline const char *get_sql_id() const
  {
    return extract_str(sql_id_);
  }
  inline const common::ObString &get_sql_id_str() const
  {
    return sql_id_;
  }

  int set_hints_info(const char *hints_info)
  {
    return deep_copy_str(hints_info, hints_info_);
  }
  int set_hints_info(const common::ObString &hints_info)
  {
    return deep_copy_str(hints_info, hints_info_);
  }

  inline const char *get_hints_info() const
  {
    return extract_str(hints_info_);
  }
  inline const common::ObString &get_hints_info_str() const
  {
    return hints_info_;
  }

  int set_outline_data(const char *outline_data)
  {
    return deep_copy_str(outline_data, outline_data_);
  }
  int set_outline_data(const common::ObString &outline_data)
  {
    return deep_copy_str(outline_data, outline_data_);
  }

  inline const char *get_outline_data() const
  {
    return extract_str(outline_data_);
  }
  inline const common::ObString &get_outline_data_str() const
  {
    return outline_data_;
  }

  int set_sql_text(const char *constructed_sql)
  {
    return deep_copy_str(constructed_sql, key_.constructed_sql_);
  }
  int set_sql_text(const common::ObString &constructed_sql)
  {
    return deep_copy_str(constructed_sql, key_.constructed_sql_);
  }

  inline const char *get_sql_text() const
  {
    return extract_str(key_.constructed_sql_);
  }
  inline const common::ObString &get_sql_text_str() const
  {
    return key_.constructed_sql_;
  }

  int set_params_info(const char *params_info)
  {
    return deep_copy_str(params_info, key_.params_info_str_);
  }
  int set_params_info(const common::ObString &params_info)
  {
    return deep_copy_str(params_info, key_.params_info_str_);
  }

  inline const char *get_params_info() const
  {
    return extract_str(key_.params_info_str_);
  }
  inline const common::ObString &get_params_info_str() const
  {
    return key_.params_info_str_;
  }

  VIRTUAL_TO_STRING_KV(K_(key), K_(plan_baseline_id), K_(schema_version), K_(outline_data), K_(plan_hash_value),
      K_(fixed), K_(enabled), K_(executions), K_(cpu_time), K_(hints_info), K_(hints_all_worked));

public:
  BaselineKey key_;
  uint64_t plan_baseline_id_;
  int64_t schema_version_;         // the last modify timestamp of this version
  common::ObString outline_data_;  // Hint collection of fixed plan
  common::ObString sql_id_;        // sql id //The last byte stores'\0'
  uint64_t plan_hash_value_;
  bool fixed_;
  bool enabled_;
  int64_t executions_;  // The total number of executions in the evolution process
  int64_t cpu_time_;    // The total CPU time consumed during the evolution process
  common::ObString hints_info_;
  bool hints_all_worked_;
};

class ObOutlineInfo : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObOutlineInfo();
  explicit ObOutlineInfo(common::ObIAllocator *allocator);
  virtual ~ObOutlineInfo();
  ObOutlineInfo(const ObOutlineInfo &src_schema);
  ObOutlineInfo &operator=(const ObOutlineInfo &src_schema);
  void reset();
  bool is_valid() const;
  bool is_valid_for_replace() const;
  int64_t get_convert_size() const;
  static bool cmp(const ObOutlineInfo *lhs, const ObOutlineInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() < rhs->get_outline_id() : false;
  }
  static bool equal(const ObOutlineInfo *lhs, const ObOutlineInfo *rhs)
  {
    return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() == rhs->get_outline_id() : false;
  }

  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_database_id(const uint64_t id)
  {
    database_id_ = id;
  }
  inline void set_outline_id(uint64_t id)
  {
    outline_id_ = id;
  }
  inline void set_schema_version(int64_t version)
  {
    schema_version_ = version;
  }
  int set_name(const char *name)
  {
    return deep_copy_str(name, name_);
  }
  int set_name(const common::ObString &name)
  {
    return deep_copy_str(name, name_);
  }
  int set_signature(const char *sig)
  {
    return deep_copy_str(sig, signature_);
  }
  int set_signature(const common::ObString &sig)
  {
    return deep_copy_str(sig, signature_);
  }
  int set_sql_id(const char *sql_id)
  {
    return deep_copy_str(sql_id, sql_id_);
  }
  int set_sql_id(const common::ObString &sql_id)
  {
    return deep_copy_str(sql_id, sql_id_);
  }
  int set_outline_params(const common::ObString &outline_params_str);
  int set_outline_content(const char *content)
  {
    return deep_copy_str(content, outline_content_);
  }
  int set_outline_content(const common::ObString &content)
  {
    return deep_copy_str(content, outline_content_);
  }
  int set_sql_text(const char *sql)
  {
    return deep_copy_str(sql, sql_text_);
  }
  int set_sql_text(const common::ObString &sql)
  {
    return deep_copy_str(sql, sql_text_);
  }
  int set_outline_target(const char *target)
  {
    return deep_copy_str(target, outline_target_);
  }
  int set_outline_target(const common::ObString &target)
  {
    return deep_copy_str(target, outline_target_);
  }
  int set_owner(const char *owner)
  {
    return deep_copy_str(owner, owner_);
  }
  int set_owner(const common::ObString &owner)
  {
    return deep_copy_str(owner, owner_);
  }
  void set_owner_id(const uint64_t owner_id)
  {
    owner_id_ = owner_id;
  }
  void set_used(const bool used)
  {
    used_ = used;
  }
  int set_version(const char *version)
  {
    return deep_copy_str(version, version_);
  }
  int set_version(const common::ObString &version)
  {
    return deep_copy_str(version, version_);
  }
  void set_compatible(const bool compatible)
  {
    compatible_ = compatible;
  }
  void set_enabled(const bool enabled)
  {
    enabled_ = enabled;
  }
  void set_format(const ObHintFormat hint_format)
  {
    format_ = hint_format;
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_owner_id() const
  {
    return owner_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline uint64_t get_outline_id() const
  {
    return outline_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline bool is_used() const
  {
    return used_;
  }
  inline bool is_enabled() const
  {
    return enabled_;
  }
  inline bool is_compatible() const
  {
    return compatible_;
  }
  inline ObHintFormat get_hint_format() const
  {
    return format_;
  }
  inline const char *get_name() const
  {
    return extract_str(name_);
  }
  inline const common::ObString &get_name_str() const
  {
    return name_;
  }
  inline const char *get_signature() const
  {
    return extract_str(signature_);
  }
  inline const char *get_sql_id() const
  {
    return extract_str(sql_id_);
  }
  inline const common::ObString &get_sql_id_str() const
  {
    return sql_id_;
  }
  inline const common::ObString &get_signature_str() const
  {
    return signature_;
  }
  inline const char *get_outline_content() const
  {
    return extract_str(outline_content_);
  }
  inline const common::ObString &get_outline_content_str() const
  {
    return outline_content_;
  }
  inline const char *get_sql_text() const
  {
    return extract_str(sql_text_);
  }
  inline const common::ObString &get_sql_text_str() const
  {
    return sql_text_;
  }
  inline common::ObString &get_sql_text_str()
  {
    return sql_text_;
  }
  inline const char *get_outline_target() const
  {
    return extract_str(outline_target_);
  }
  inline const common::ObString &get_outline_target_str() const
  {
    return outline_target_;
  }
  inline common::ObString &get_outline_target_str()
  {
    return outline_target_;
  }
  inline const char *get_owner() const
  {
    return extract_str(owner_);
  }
  inline const common::ObString &get_owner_str() const
  {
    return owner_;
  }
  inline const char *get_version() const
  {
    return extract_str(version_);
  }
  inline const common::ObString &get_version_str() const
  {
    return version_;
  }
  int get_visible_signature(common::ObString &visiable_signature) const;
  int get_outline_sql(
      common::ObIAllocator &allocator, const sql::ObSQLSessionInfo &session, common::ObString &outline_sql) const;
  int get_hex_str_from_outline_params(common::ObString &hex_str, common::ObIAllocator &allocator) const;
  const ObOutlineParamsWrapper &get_outline_params_wrapper() const
  {
    return outline_params_wrapper_;
  }
  ObOutlineParamsWrapper &get_outline_params_wrapper()
  {
    return outline_params_wrapper_;
  }
  bool has_outline_params() const
  {
    return outline_params_wrapper_.get_outline_params().count() > 0;
  }
  int has_concurrent_limit_param(bool &has) const;
  int gen_valid_allocator();
  int add_param(const ObMaxConcurrentParam &src_param);
  static int gen_limit_sql(const common::ObString &visible_signature, const ObMaxConcurrentParam *param,
      const sql::ObSQLSessionInfo &session, common::ObIAllocator &allocator, common::ObString &limit_sql);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(database_id), K_(outline_id), K_(schema_version), K_(name), K_(signature),
      K_(sql_id), K_(outline_content), K_(sql_text), K_(owner_id), K_(owner), K_(used), K_(compatible), K_(enabled),
      K_(format), K_(outline_params_wrapper), K_(outline_target));
  static bool is_sql_id_valid(const common::ObString &sql_id);

private:
  static int replace_question_mark(const common::ObString &not_param_sql, const ObMaxConcurrentParam &concurrent_param,
      int64_t start_pos, int64_t cur_pos, int64_t &question_mark_offset, common::ObSqlString &string_helper);
  static int replace_not_param(const common::ObString &not_param_sql, const ParseNode &node, int64_t start_pos,
      int64_t cur_pos, common::ObSqlString &string_helper);

protected:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t outline_id_;
  int64_t schema_version_;      // the last modify timestamp of this version
  common::ObString name_;       // outline name
  common::ObString signature_;  // SQL after constant parameterization
  common::ObString sql_id_;     // Used to directly determine sql_id
  common::ObString outline_content_;
  common::ObString sql_text_;
  common::ObString outline_target_;
  common::ObString owner_;
  bool used_;
  common::ObString version_;
  bool compatible_;
  bool enabled_;
  ObHintFormat format_;
  uint64_t owner_id_;
  ObOutlineParamsWrapper outline_params_wrapper_;
};

class ObDbLinkBaseInfo : public ObSchema {
public:
  ObDbLinkBaseInfo() : ObSchema()
  {
    reset();
  }
  explicit ObDbLinkBaseInfo(common::ObIAllocator *allocator) : ObSchema(allocator)
  {
    reset();
  }
  virtual ~ObDbLinkBaseInfo()
  {}

  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_owner_id(const uint64_t id)
  {
    owner_id_ = id;
  }
  inline void set_dblink_id(const uint64_t id)
  {
    dblink_id_ = id;
  }
  inline void set_schema_version(const int64_t version)
  {
    schema_version_ = version;
  }
  inline int set_dblink_name(const common::ObString &name)
  {
    return deep_copy_str(name, dblink_name_);
  }
  inline int set_cluster_name(const common::ObString &name)
  {
    return deep_copy_str(name, cluster_name_);
  }
  inline int set_tenant_name(const common::ObString &name)
  {
    return deep_copy_str(name, tenant_name_);
  }
  inline int set_user_name(const common::ObString &name)
  {
    return deep_copy_str(name, user_name_);
  }
  inline int set_password(const common::ObString &password)
  {
    return deep_copy_str(password, password_);
  }
  inline void set_host_addr(const common::ObAddr &addr)
  {
    host_addr_ = addr;
  }
  inline int set_host_ip(const common::ObString &host_ip)
  {
    host_addr_.set_ip_addr(host_ip, 0);
    return common::OB_SUCCESS;
  }
  inline void set_host_port(const int32_t host_port)
  {
    host_addr_.set_port(host_port);
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_owner_id() const
  {
    return owner_id_;
  }
  inline uint64_t get_dblink_id() const
  {
    return dblink_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const common::ObString &get_dblink_name() const
  {
    return dblink_name_;
  }
  inline const common::ObString &get_cluster_name() const
  {
    return cluster_name_;
  }
  inline const common::ObString &get_tenant_name() const
  {
    return tenant_name_;
  }
  inline const common::ObString &get_user_name() const
  {
    return user_name_;
  }
  inline const common::ObString &get_password() const
  {
    return password_;
  }
  inline const common::ObAddr &get_host_addr() const
  {
    return host_addr_;
  }
  inline uint32_t get_host_ip() const
  {
    return host_addr_.get_ipv4();
  }
  inline int32_t get_host_port() const
  {
    return host_addr_.get_port();
  }

  void reset();
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(owner_id), K_(dblink_id), K_(dblink_name), K_(cluster_name), K_(tenant_name),
      K_(user_name), K_(host_addr));

protected:
  uint64_t tenant_id_;
  uint64_t owner_id_;
  uint64_t dblink_id_;
  int64_t schema_version_;
  common::ObString dblink_name_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString password_;  // only encrypt when write to sys table.
  common::ObAddr host_addr_;
};

struct ObTenantDbLinkId {
public:
  ObTenantDbLinkId() : tenant_id_(common::OB_INVALID_ID), dblink_id_(common::OB_INVALID_ID)
  {}
  ObTenantDbLinkId(uint64_t tenant_id, uint64_t dblink_id) : tenant_id_(tenant_id), dblink_id_(dblink_id)
  {}
  ObTenantDbLinkId(const ObTenantDbLinkId &other) : tenant_id_(other.tenant_id_), dblink_id_(other.dblink_id_)
  {}
  ObTenantDbLinkId &operator=(const ObTenantDbLinkId &other)
  {
    tenant_id_ = other.tenant_id_;
    dblink_id_ = other.dblink_id_;
    return *this;
  }
  bool operator==(const ObTenantDbLinkId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (dblink_id_ == rhs.dblink_id_);
  }
  bool operator!=(const ObTenantDbLinkId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantDbLinkId &rhs) const
  {
    return (tenant_id_ < rhs.tenant_id_) || (tenant_id_ == rhs.tenant_id_ && dblink_id_ < rhs.dblink_id_);
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    hash_ret = common::murmurhash(&dblink_id_, sizeof(dblink_id_), hash_ret);
    return hash_ret;
  }
  inline bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (dblink_id_ != common::OB_INVALID_ID);
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_dblink_id() const
  {
    return dblink_id_;
  }
  TO_STRING_KV(K_(tenant_id), K_(dblink_id));

private:
  uint64_t tenant_id_;
  uint64_t dblink_id_;
};

class ObDbLinkInfo : public ObDbLinkBaseInfo {
  OB_UNIS_VERSION(1);

public:
  ObDbLinkInfo() : ObDbLinkBaseInfo()
  {}
  explicit ObDbLinkInfo(common::ObIAllocator *allocator) : ObDbLinkBaseInfo(allocator)
  {}
  virtual ~ObDbLinkInfo()
  {}
};

class ObDbLinkSchema : public ObDbLinkBaseInfo {
public:
  ObDbLinkSchema() : ObDbLinkBaseInfo()
  {}
  explicit ObDbLinkSchema(common::ObIAllocator *allocator) : ObDbLinkBaseInfo(allocator)
  {}
  ObDbLinkSchema(const ObDbLinkSchema &src_schema);
  virtual ~ObDbLinkSchema()
  {}
  ObDbLinkSchema &operator=(const ObDbLinkSchema &other);
  bool operator==(const ObDbLinkSchema &other) const;

  inline ObTenantDbLinkId get_tenant_dblink_id() const
  {
    return ObTenantDbLinkId(tenant_id_, dblink_id_);
  }
  inline int64_t get_convert_size() const
  {
    return sizeof(ObDbLinkSchema) + dblink_name_.length() + 1 + cluster_name_.length() + 1 + tenant_name_.length() + 1 +
           user_name_.length() + 1 + password_.length() + 1;
  }
};

class ObSynonymInfo : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObSynonymInfo();
  explicit ObSynonymInfo(common::ObIAllocator *allocator);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(database_id), K_(synonym_id), K_(schema_version));
  virtual ~ObSynonymInfo();
  ObSynonymInfo &operator=(const ObSynonymInfo &src_schema);
  ObSynonymInfo(const ObSynonymInfo &src_schema);
  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_database_id(const uint64_t id)
  {
    database_id_ = id;
  }
  inline void set_object_database_id(const uint64_t id)
  {
    object_db_id_ = id;
  }
  inline void set_synonym_id(uint64_t id)
  {
    synonym_id_ = id;
  }
  inline void set_schema_version(int64_t version)
  {
    schema_version_ = version;
  }
  int64_t get_convert_size() const;
  int set_synonym_name(const char *name)
  {
    return deep_copy_str(name, name_);
  }
  int set_synonym_name(const common::ObString &name)
  {
    return deep_copy_str(name, name_);
  }
  int set_object_name(const char *name)
  {
    return deep_copy_str(name, object_name_);
  }
  int set_object_name(const common::ObString &name)
  {
    return deep_copy_str(name, object_name_);
  }
  int set_version(const char *version)
  {
    return deep_copy_str(version, version_);
  }
  int set_version(const common::ObString &version)
  {
    return deep_copy_str(version, version_);
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline uint64_t get_synonym_id() const
  {
    return synonym_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const char *get_synonym_name() const
  {
    return extract_str(name_);
  }
  inline const common::ObString &get_synonym_name_str() const
  {
    return name_;
  }
  inline const char *get_object_name() const
  {
    return extract_str(object_name_);
  }
  inline const common::ObString &get_object_name_str() const
  {
    return object_name_;
  }
  inline const char *get_version() const
  {
    return extract_str(version_);
  }
  inline const common::ObString &get_version_str() const
  {
    return version_;
  }
  inline uint64_t get_object_database_id() const
  {
    return object_db_id_;
  }
  void reset();

private:
  // void *alloc(int64_t size);
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t synonym_id_;
  int64_t schema_version_;  // the last modify timestamp of this version
  common::ObString name_;   // synonym name
  common::ObString version_;
  common::ObString object_name_;
  uint64_t object_db_id_;
  // common::ObArenaAllocator allocator_;
};

struct ObTenantUDFId {
  OB_UNIS_VERSION(1);

public:
  ObTenantUDFId() : tenant_id_(common::OB_INVALID_ID), udf_name_()
  {}
  ObTenantUDFId(const uint64_t tenant_id, const common::ObString &name) : tenant_id_(tenant_id), udf_name_(name)
  {}
  bool operator==(const ObTenantUDFId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (udf_name_ == rhs.udf_name_);
  }
  bool operator!=(const ObTenantUDFId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantUDFId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = udf_name_ < rhs.udf_name_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(udf_name_.ptr(), udf_name_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (udf_name_.length() != 0);
  }
  TO_STRING_KV(K_(tenant_id), K_(udf_name));
  uint64_t tenant_id_;
  common::ObString udf_name_;
};

struct ObTenantSynonymId {
  OB_UNIS_VERSION(1);

public:
  ObTenantSynonymId() : tenant_id_(common::OB_INVALID_ID), synonym_id_(common::OB_INVALID_ID)
  {}
  ObTenantSynonymId(const uint64_t tenant_id, const uint64_t synonym_id)
      : tenant_id_(tenant_id), synonym_id_(synonym_id)
  {}
  bool operator==(const ObTenantSynonymId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (synonym_id_ == rhs.synonym_id_);
  }
  bool operator!=(const ObTenantSynonymId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantSynonymId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = synonym_id_ < rhs.synonym_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&synonym_id_, sizeof(synonym_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (synonym_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(synonym_id));
  uint64_t tenant_id_;
  uint64_t synonym_id_;
};

struct ObTenantSequenceId {
  OB_UNIS_VERSION(1);

public:
  ObTenantSequenceId() : tenant_id_(common::OB_INVALID_ID), sequence_id_(common::OB_INVALID_ID)
  {}
  ObTenantSequenceId(const uint64_t tenant_id, const uint64_t sequence_id)
      : tenant_id_(tenant_id), sequence_id_(sequence_id)
  {}
  bool operator==(const ObTenantSequenceId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (sequence_id_ == rhs.sequence_id_);
  }
  bool operator!=(const ObTenantSequenceId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantSequenceId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = sequence_id_ < rhs.sequence_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&sequence_id_, sizeof(sequence_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (sequence_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(sequence_id));
  uint64_t tenant_id_;
  uint64_t sequence_id_;
};

class ObAlterOutlineInfo : public ObOutlineInfo {
  OB_UNIS_VERSION(1);

public:
  ObAlterOutlineInfo() : ObOutlineInfo(), alter_option_bitset_()
  {}
  virtual ~ObAlterOutlineInfo()
  {}
  void reset()
  {
    ObOutlineInfo::reset();
    alter_option_bitset_.reset();
  }
  const common::ObBitSet<> &get_alter_option_bitset() const
  {
    return alter_option_bitset_;
  }
  common::ObBitSet<> &get_alter_option_bitset()
  {
    return alter_option_bitset_;
  }
  INHERIT_TO_STRING_KV("ObOutlineInfo", ObOutlineInfo, K_(alter_option_bitset));

private:
  common::ObBitSet<> alter_option_bitset_;
};

class ObOutlineNameHashWrapper {
public:
  ObOutlineNameHashWrapper() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID), name_()
  {}
  ObOutlineNameHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const common::ObString &name_)
      : tenant_id_(tenant_id), database_id_(database_id), name_(name_)
  {}
  ~ObOutlineNameHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObOutlineNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_database_id(uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline void set_name(const common::ObString &name)
  {
    name_ = name;
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline const common::ObString &get_name() const
  {
    return name_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString name_;
};

inline uint64_t ObOutlineNameHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(name_.ptr(), name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineNameHashWrapper::operator==(const ObOutlineNameHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (name_ == rv.name_);
}

class ObOutlineSignatureHashWrapper {
public:
  ObOutlineSignatureHashWrapper() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID), signature_()
  {}
  ObOutlineSignatureHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const common::ObString &signature)
      : tenant_id_(tenant_id), database_id_(database_id), signature_(signature)
  {}
  ~ObOutlineSignatureHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObOutlineSignatureHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_database_id(uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline void set_signature(const common::ObString &signature)
  {
    signature_ = signature;
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline const common::ObString &get_signature() const
  {
    return signature_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString signature_;
};

class ObOutlineSqlIdHashWrapper {
public:
  ObOutlineSqlIdHashWrapper() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID), sql_id_()
  {}
  ObOutlineSqlIdHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const common::ObString &sql_id)
      : tenant_id_(tenant_id), database_id_(database_id), sql_id_(sql_id)
  {}
  ~ObOutlineSqlIdHashWrapper()
  {}
  inline uint64_t hash() const;
  inline bool operator==(const ObOutlineSqlIdHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_database_id(uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline void set_sql_id(const common::ObString &sql_id)
  {
    sql_id_ = sql_id;
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline const common::ObString &get_sql_id() const
  {
    return sql_id_;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString sql_id_;
};

inline uint64_t ObOutlineSqlIdHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(sql_id_.ptr(), sql_id_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineSqlIdHashWrapper::operator==(const ObOutlineSqlIdHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (sql_id_ == rv.sql_id_);
}

inline uint64_t ObOutlineSignatureHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(signature_.ptr(), signature_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineSignatureHashWrapper::operator==(const ObOutlineSignatureHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (signature_ == rv.signature_);
}

class ObSysTableChecker {
private:
  ObSysTableChecker();
  ~ObSysTableChecker();

  int init_tenant_space_table_id_map();
  int init_sys_table_name_map();
  int check_tenant_space_table_id(const uint64_t table_id, bool &is_tenant_space_table);
  int check_sys_table_name(const uint64_t database_id, const common::ObString &table_name, bool &is_tenant_space_table);
  int ob_write_string(const common::ObString &src, common::ObString &dst);

public:
  static ObSysTableChecker &instance();
  int init();
  int destroy();

  int64_t get_tenant_space_sys_table_num()
  {
    return tenant_space_sys_table_num_;
  }

  static int is_tenant_space_table_id(const uint64_t table_id, bool &is_tenant_space_table);
  static int is_sys_table_name(
      const uint64_t database_id, const common::ObString &table_name, bool &is_tenant_space_table);
  static bool is_tenant_table_in_version_2200(const uint64_t table_id);
  static bool is_cluster_private_tenant_table(const uint64_t table_id);
  static bool is_backup_private_tenant_table(const uint64_t table_id);
  static int is_tenant_table_need_weak_read(const uint64_t table_id, bool &need_weak_read);
  static bool is_rs_restart_related_table_id(const uint64_t table_id);
  static bool is_rs_restart_related_partition(const uint64_t table_id, const int64_t partition_id);

public:
  class TableNameWrapper {
  public:
    TableNameWrapper()
        : database_id_(common::OB_INVALID_ID), name_case_mode_(common::OB_NAME_CASE_INVALID), table_name_()
    {}
    TableNameWrapper(const uint64_t database_id, const common::ObNameCaseMode mode, const common::ObString &table_name)
        : database_id_(database_id), name_case_mode_(mode), table_name_(table_name)
    {}
    ~TableNameWrapper()
    {}
    inline uint64_t hash() const;
    bool operator==(const TableNameWrapper &rv) const;
    TO_STRING_KV(K_(database_id), K_(name_case_mode), K_(table_name));

  private:
    uint64_t database_id_;  // pure_database_id
    common::ObNameCaseMode name_case_mode_;
    common::ObString table_name_;
  };
  typedef common::ObSEArray<TableNameWrapper, 2> TableNameWrapperArray;

private:
  static const int64_t TABLE_BUCKET_NUM = 300;

private:
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> tenant_space_table_id_map_;
  common::hash::ObHashMap<uint64_t, TableNameWrapperArray *, common::hash::NoPthreadDefendMode> sys_table_name_map_;
  int64_t tenant_space_sys_table_num_;  // Number of tenant-level system tables (including system table indexes)
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTableChecker);
};

class ObTableSchema;
class ObRecycleObject : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  enum RecycleObjType {
    INVALID = 0,
    TABLE,
    INDEX,
    VIEW,
    DATABASE,
    RESERVED_OBJ_TYPE,
    TRIGGER,
    TENANT,
  };
  ObRecycleObject(common::ObIAllocator *allocator);
  ObRecycleObject()
      : ObSchema(),
        tenant_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        tablegroup_id_(common::OB_INVALID_ID),
        object_name_(),
        original_name_(),
        type_(INVALID),
        tablegroup_name_(),
        database_name_()
  {}
  ObRecycleObject(const ObRecycleObject &recycle_obj);
  ObRecycleObject &operator=(const ObRecycleObject &recycle_obj);
  virtual ~ObRecycleObject()
  {}
  inline bool is_valid() const;
  virtual void reset();

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  uint64_t get_database_id() const
  {
    return database_id_;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  uint64_t get_tablegroup_id() const
  {
    return tablegroup_id_;
  }
  const common::ObString &get_object_name() const
  {
    return object_name_;
  }
  const common::ObString &get_original_name() const
  {
    return original_name_;
  }
  RecycleObjType get_type() const
  {
    return type_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_database_id(const uint64_t db_id)
  {
    database_id_ = db_id;
  }
  void set_table_id(const uint64_t table_id)
  {
    table_id_ = table_id;
  }
  void set_tablegroup_id(const uint64_t tablegroup_id)
  {
    tablegroup_id_ = tablegroup_id;
  }
  int set_object_name(const common::ObString &object_name)
  {
    return deep_copy_str(object_name, object_name_);
  }
  int set_original_name(const common::ObString &original_name)
  {
    return deep_copy_str(original_name, original_name_);
  }
  void set_type(const RecycleObjType type)
  {
    type_ = type;
  }
  int set_type_by_table_schema(const ObSimpleTableSchemaV2 &table_schema);
  static RecycleObjType get_type_by_table_schema(const ObSimpleTableSchemaV2 &table_schema);
  // for backup
  int set_tablegroup_name(const common::ObString &tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, tablegroup_name_);
  }
  const common::ObString &get_tablegroup_name() const
  {
    return tablegroup_name_;
  }
  int set_database_name(const common::ObString &database_name)
  {
    return deep_copy_str(database_name, database_name_);
  }
  const common::ObString &get_database_name() const
  {
    return database_name_;
  }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_id), K_(tablegroup_id), K_(object_name), K_(original_name),
      K_(type), K_(tablegroup_name), K_(database_name));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  uint64_t tablegroup_id_;
  common::ObString object_name_;
  common::ObString original_name_;
  RecycleObjType type_;
  // for backup
  common::ObString tablegroup_name_;
  common::ObString database_name_;
};

inline bool ObRecycleObject::is_valid() const
{
  return INVALID != type_ && !object_name_.empty() && !original_name_.empty();
}

typedef common::hash::ObPlacementHashSet<uint64_t, common::OB_MAX_TABLE_NUM_PER_STMT> DropTableIdHashSet;
// Used to count vertical partition columns
typedef common::hash::ObPlacementHashSet<common::ObString, common::OB_MAX_USER_DEFINED_COLUMNS_COUNT>
    VPColumnNameHashSet;

struct ObBasedSchemaObjectInfo {
  OB_UNIS_VERSION(1);

public:
  ObBasedSchemaObjectInfo()
      : schema_id_(common::OB_INVALID_ID), schema_type_(OB_MAX_SCHEMA), schema_version_(common::OB_INVALID_VERSION)
  {}
  ObBasedSchemaObjectInfo(const uint64_t schema_id, const ObSchemaType schema_type, const int64_t schema_version)
      : schema_id_(schema_id), schema_type_(schema_type), schema_version_(schema_version)
  {}
  bool operator==(const ObBasedSchemaObjectInfo &other) const
  {
    return (schema_id_ == other.schema_id_ && schema_type_ == other.schema_type_ &&
            schema_version_ == other.schema_version_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    return convert_size;
  }
  TO_STRING_KV(K_(schema_id), K_(schema_type), K_(schema_version));
  uint64_t schema_id_;
  ObSchemaType schema_type_;
  int64_t schema_version_;
};

struct ObAuxTableMetaInfo {
  OB_UNIS_VERSION(1);

public:
  ObAuxTableMetaInfo()
      : table_id_(common::OB_INVALID_ID), table_type_(MAX_TABLE_TYPE), drop_schema_version_(common::OB_INVALID_VERSION)
  {}
  ObAuxTableMetaInfo(const uint64_t table_id, const ObTableType table_type, const int64_t drop_schema_version)
      : table_id_(table_id), table_type_(table_type), drop_schema_version_(drop_schema_version)
  {}
  bool operator==(const ObAuxTableMetaInfo &other) const
  {
    return (table_id_ == other.table_id_ && table_type_ == other.table_type_ &&
            drop_schema_version_ == other.drop_schema_version_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    return convert_size;
  }
  bool is_dropped_schema() const
  {
    return drop_schema_version_ > 0;
  }
  TO_STRING_KV(K_(table_id), K_(table_type), K_(drop_schema_version));
  uint64_t table_id_;
  ObTableType table_type_;
  int64_t drop_schema_version_;
};

enum ObConstraintType {
  CONSTRAINT_TYPE_INVALID = 0,
  CONSTRAINT_TYPE_PRIMARY_KEY = 1,
  CONSTRAINT_TYPE_UNIQUE_KEY = 2,
  CONSTRAINT_TYPE_CHECK = 3,
  CONSTRAINT_TYPE_MAX,
};

enum ObReferenceAction {
  ACTION_INVALID = 0,
  ACTION_RESTRICT,
  ACTION_CASCADE,
  ACTION_SET_NULL,
  ACTION_NO_ACTION,
  ACTION_SET_DEFAULT,  // no use now
  ACTION_CHECK_EXIST,  // not a valid option, just used for construct dml stmt.
  ACTION_MAX
};

class ObForeignKeyInfo {
  OB_UNIS_VERSION(1);

public:
  ObForeignKeyInfo()
      : table_id_(common::OB_INVALID_ID),
        foreign_key_id_(common::OB_INVALID_ID),
        child_table_id_(common::OB_INVALID_ID),
        parent_table_id_(common::OB_INVALID_ID),
        child_column_ids_(),
        parent_column_ids_(),
        update_action_(ACTION_INVALID),
        delete_action_(ACTION_INVALID),
        foreign_key_name_(),
        enable_flag_(true),
        is_modify_enable_flag_(false),
        validate_flag_(true),
        is_modify_validate_flag_(false),
        rely_flag_(false),
        is_modify_rely_flag_(false),
        is_modify_fk_state_(false),
        ref_cst_type_(CONSTRAINT_TYPE_INVALID),
        ref_cst_id_(common::OB_INVALID_ID)
  {}
  ObForeignKeyInfo(common::ObIAllocator *allocator);
  virtual ~ObForeignKeyInfo()
  {}

public:
  inline void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  inline void set_foreign_key_id(uint64_t foreign_key_id)
  {
    foreign_key_id_ = foreign_key_id;
  }
  inline void set_child_table_id(uint64_t child_table_id)
  {
    child_table_id_ = child_table_id;
  }
  inline void set_parent_table_id(uint64_t parent_table_id)
  {
    parent_table_id_ = parent_table_id;
  }
  inline void set_update_action(uint64_t update_action)
  {
    update_action_ = static_cast<ObReferenceAction>(update_action);
  }
  inline void set_delete_action(uint64_t delete_action)
  {
    delete_action_ = static_cast<ObReferenceAction>(delete_action);
  }
  inline int set_foreign_key_name(const common::ObString &foreign_key_name)
  {
    foreign_key_name_ = foreign_key_name;
    return common::OB_SUCCESS;
  }
  inline void set_enable_flag(const bool enable_flag)
  {
    enable_flag_ = enable_flag;
  }
  inline void set_is_modify_enable_flag(const bool is_modify_enable_flag)
  {
    is_modify_enable_flag_ = is_modify_enable_flag;
  }
  inline void set_validate_flag(const bool validate_flag)
  {
    validate_flag_ = validate_flag;
  }
  inline void set_is_modify_validate_flag(const bool is_modify_validate_flag)
  {
    is_modify_validate_flag_ = is_modify_validate_flag;
  }
  inline void set_rely_flag(const bool rely_flag)
  {
    rely_flag_ = rely_flag;
  }
  inline void set_is_modify_rely_flag(const bool is_modify_rely_flag)
  {
    is_modify_rely_flag_ = is_modify_rely_flag;
  }
  inline void set_is_modify_fk_state(const bool is_modify_fk_state)
  {
    is_modify_fk_state_ = is_modify_fk_state;
  }
  inline void set_ref_cst_type(ObConstraintType ref_cst_type)
  {
    ref_cst_type_ = ref_cst_type;
  }
  inline void set_ref_cst_id(uint64_t ref_cst_id)
  {
    ref_cst_id_ = ref_cst_id;
  }
  inline const char *get_update_action_str() const
  {
    return get_action_str(update_action_);
  }
  inline const char *get_delete_action_str() const
  {
    return get_action_str(delete_action_);
  }
  inline const char *get_action_str(ObReferenceAction action) const
  {
    const char *ret = NULL;
    if (ACTION_RESTRICT <= action && action <= ACTION_SET_DEFAULT) {
      ret = reference_action_str_[action];
    }
    return ret;
  }
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    foreign_key_id_ = common::OB_INVALID_ID;
    child_table_id_ = common::OB_INVALID_ID;
    parent_table_id_ = common::OB_INVALID_ID;
    child_column_ids_.reset();
    parent_column_ids_.reset();
    update_action_ = ACTION_INVALID;
    delete_action_ = ACTION_INVALID;
    foreign_key_name_.reset();
    enable_flag_ = true;
    is_modify_enable_flag_ = false;
    validate_flag_ = true;
    is_modify_validate_flag_ = false;
    rely_flag_ = false;
    is_modify_rely_flag_ = false;
    is_modify_fk_state_ = false;
    ref_cst_type_ = CONSTRAINT_TYPE_INVALID;
    ref_cst_id_ = common::OB_INVALID_ID;
  }
  int assign(const ObForeignKeyInfo &other);
  inline int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    convert_size += child_column_ids_.get_data_size();
    convert_size += parent_column_ids_.get_data_size();
    convert_size += foreign_key_name_.length() + 1;
    return convert_size;
  }
  TO_STRING_KV(K_(table_id), K_(foreign_key_id), K_(child_table_id), K_(parent_table_id), K_(child_column_ids),
      K_(parent_column_ids), K_(update_action), K_(delete_action), K_(foreign_key_name), K_(enable_flag),
      K_(is_modify_enable_flag), K_(validate_flag), K_(is_modify_validate_flag), K_(rely_flag), K_(is_modify_rely_flag),
      K_(is_modify_fk_state), K_(ref_cst_type), K_(ref_cst_id));

public:
  uint64_t table_id_;  // table_id is not in __all_foreign_key.
  uint64_t foreign_key_id_;
  uint64_t child_table_id_;
  uint64_t parent_table_id_;
  common::ObSEArray<uint64_t, 8> child_column_ids_;
  common::ObSEArray<uint64_t, 8> parent_column_ids_;
  ObReferenceAction update_action_;
  ObReferenceAction delete_action_;
  common::ObString foreign_key_name_;
  bool enable_flag_;
  bool is_modify_enable_flag_;
  bool validate_flag_;
  bool is_modify_validate_flag_;
  bool rely_flag_;
  bool is_modify_rely_flag_;
  bool is_modify_fk_state_;
  ObConstraintType ref_cst_type_;
  uint64_t ref_cst_id_;

private:
  static const char *reference_action_str_[ACTION_MAX + 1];
};

struct ObSimpleForeignKeyInfo {
  OB_UNIS_VERSION(1);

public:
  ObSimpleForeignKeyInfo()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    foreign_key_name_.assign_ptr("", 0);
    foreign_key_id_ = common::OB_INVALID_ID;
  }
  ObSimpleForeignKeyInfo(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id,
      const common::ObString &foreign_key_name, const uint64_t foreign_key_id)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        table_id_(table_id),
        foreign_key_name_(foreign_key_name),
        foreign_key_id_(foreign_key_id)
  {}
  bool operator==(const ObSimpleForeignKeyInfo &other) const
  {
    return (tenant_id_ == other.tenant_id_ && database_id_ == other.database_id_ && table_id_ == other.table_id_ &&
            foreign_key_name_ == other.foreign_key_name_ && foreign_key_id_ == other.foreign_key_id_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    convert_size += foreign_key_name_.length() + 1;
    return convert_size;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    foreign_key_name_.assign_ptr("", 0);
    foreign_key_id_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_id), K_(foreign_key_name), K_(foreign_key_id));

  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  common::ObString foreign_key_name_;
  uint64_t foreign_key_id_;
};

struct ObSimpleConstraintInfo {
  OB_UNIS_VERSION(1);

public:
  ObSimpleConstraintInfo()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    constraint_name_.assign_ptr("", 0);
    constraint_id_ = common::OB_INVALID_ID;
  }
  ObSimpleConstraintInfo(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id,
      const common::ObString &constraint_name, const uint64_t constraint_id)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        table_id_(table_id),
        constraint_name_(constraint_name),
        constraint_id_(constraint_id)
  {}
  bool operator==(const ObSimpleConstraintInfo &other) const
  {
    return (tenant_id_ == other.tenant_id_ && database_id_ == other.database_id_ && table_id_ == other.table_id_ &&
            constraint_name_ == other.constraint_name_ && constraint_id_ == other.constraint_id_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    convert_size += constraint_name_.length() + 1;
    return convert_size;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    constraint_name_.assign_ptr("", 0);
    constraint_id_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_id), K_(constraint_name), K_(constraint_id));

  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  common::ObString constraint_name_;
  uint64_t constraint_id_;
};

template <typename PARTITION>
int ObPartitionUtils::check_partition_value(
    const PARTITION &l_part, const PARTITION &r_part, const ObPartitionFuncType part_type, bool &is_equal)
{
  int ret = common::OB_SUCCESS;
  is_equal = true;
  if (is_hash_like_part(part_type)) {
    is_equal = true;
  } else if (is_range_part(part_type)) {
    if (l_part.get_high_bound_val().get_obj_cnt() != r_part.get_high_bound_val().get_obj_cnt()) {
      is_equal = false;
      SHARE_SCHEMA_LOG(DEBUG,
          "fail to check partition value, value count not equal",
          "left",
          l_part.get_high_bound_val(),
          "right",
          r_part.get_high_bound_val());
    } else {
      for (int64_t i = 0; i < l_part.get_high_bound_val().get_obj_cnt() && is_equal; i++) {
        const common::ObObjMeta meta1 = l_part.get_high_bound_val().get_obj_ptr()[i].get_meta();
        const common::ObObjMeta meta2 = r_part.get_high_bound_val().get_obj_ptr()[i].get_meta();
        if (meta1.get_collation_level() == meta2.get_collation_level() &&
            meta1.get_collation_type() == meta2.get_collation_type()) {
          is_equal = is_types_equal_for_partition_check(meta1.get_type(), meta2.get_type());
        } else {
          is_equal = false;
        }
        if (false == is_equal) {
          SHARE_SCHEMA_LOG(DEBUG,
              "fail to check partition values, value meta not equal",
              "left",
              l_part.get_high_bound_val().get_obj_ptr()[i],
              "right",
              r_part.get_high_bound_val().get_obj_ptr()[i],
              K(lib::is_oracle_mode()));
        } else if (0 !=
                   l_part.get_high_bound_val().get_obj_ptr()[i].compare(r_part.get_high_bound_val().get_obj_ptr()[i],
                       r_part.get_high_bound_val().get_obj_ptr()[i].get_collation_type())) {
          is_equal = false;
          SHARE_SCHEMA_LOG(DEBUG,
              "fail to check partition values, value not equal",
              "left",
              l_part.get_high_bound_val().get_obj_ptr()[i],
              "right",
              r_part.get_high_bound_val().get_obj_ptr()[i]);
        }
      }
    }
  } else if (is_list_part(part_type)) {
    const common::ObIArray<common::ObNewRow> &l_list_values = l_part.get_list_row_values();
    const common::ObIArray<common::ObNewRow> &r_list_values = r_part.get_list_row_values();
    if (l_list_values.count() != r_list_values.count()) {
      is_equal = false;
    } else {
      for (int64_t i = 0; i < l_list_values.count() && is_equal; i++) {
        const common::ObNewRow &l_rowkey = l_list_values.at(i);
        bool find_equal_item = false;
        for (int64_t j = 0; j < r_list_values.count() && !find_equal_item && is_equal; j++) {
          const common::ObNewRow &r_rowkey = r_list_values.at(j);
          // First check that the count and meta information are consistent;
          if (l_rowkey.get_count() != r_rowkey.get_count()) {
            is_equal = false;
            SHARE_SCHEMA_LOG(
                DEBUG, "fail to check partition value, value count not equal", "left", l_rowkey, "right", r_rowkey);
          } else {
            for (int64_t z = 0; z < l_rowkey.get_count() && is_equal; z++) {
              const common::ObObjMeta meta1 = l_rowkey.get_cell(z).get_meta();
              const common::ObObjMeta meta2 = r_rowkey.get_cell(z).get_meta();
              if (meta1.get_collation_level() == meta2.get_collation_level() &&
                  meta1.get_collation_type() == meta2.get_collation_type()) {
                is_equal = is_types_equal_for_partition_check(meta1.get_type(), meta2.get_type());
              } else {
                is_equal = false;
              }
              if (false == is_equal) {
                SHARE_SCHEMA_LOG(DEBUG,
                    "fail to check partition values, value meta not equal",
                    "left",
                    l_rowkey.get_cell(z),
                    "right",
                    r_rowkey.get_cell(z));
              }
            }
          }
          // Check whether the value of rowkey is the same;
          if (OB_SUCC(ret) && is_equal && l_rowkey == r_rowkey) {
            find_equal_item = true;
          }
        }  // end for (int64_t j = 0
        if (!find_equal_item) {
          is_equal = false;
        }
      }  // end for (int64_t i = 0;
    }
  } else {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid part_type", K(ret), K(part_type));
  }
  return ret;
}

class ObSequenceSchema : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObSequenceSchema();
  explicit ObSequenceSchema(common::ObIAllocator *allocator);
  virtual ~ObSequenceSchema();
  ObSequenceSchema &operator=(const ObSequenceSchema &src_schema);
  int assign(const ObSequenceSchema &src_schema);
  ObSequenceSchema(const ObSequenceSchema &src_schema);
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_database_id(const uint64_t id)
  {
    database_id_ = id;
  }
  inline void set_sequence_id(uint64_t id)
  {
    sequence_id_ = id;
  }
  inline void set_schema_version(int64_t version)
  {
    schema_version_ = version;
  }
  inline int set_sequence_name(const char *name)
  {
    return deep_copy_str(name, name_);
  }
  inline int set_sequence_name(const common::ObString &name)
  {
    return deep_copy_str(name, name_);
  }
  inline int set_name(const common::ObString &name)
  {
    return set_sequence_name(name);
  }
  // inline void set_max_value(int64_t val) { option_.set_max_value(val); }
  // inline void set_min_value(int64_t val) { option_.set_min_value(val); }
  // inline void set_increment_by(int64_t val) { option_.set_increment_by(val); }
  // inline void set_start_with(int64_t val) { option_.set_start_with(val); }
  // inline void set_cache_size(int64_t val) { option_.set_cache_size(val); }
  inline void set_cycle_flag(bool cycle)
  {
    option_.set_cycle_flag(cycle);
  }
  inline void set_order_flag(bool order)
  {
    option_.set_order_flag(order);
  }

  // Temporary compatibility code, in order to support max_value etc. as Number type
  // int set_max_value(const common::ObString &str);
  // int set_min_value(const common::ObString &str);
  // int set_increment_by(const common::ObString &str);
  // int set_start_with(const common::ObString &str);
  // int set_cache_size(const common::ObString &str);

  int set_max_value(const common::number::ObNumber &num)
  {
    return option_.set_max_value(num);
  }
  int set_min_value(const common::number::ObNumber &num)
  {
    return option_.set_min_value(num);
  }
  int set_increment_by(const common::number::ObNumber &num)
  {
    return option_.set_increment_by(num);
  }
  int set_start_with(const common::number::ObNumber &num)
  {
    return option_.set_start_with(num);
  }
  int set_cache_size(const common::number::ObNumber &num)
  {
    return option_.set_cache_size(num);
  }

  inline const common::number::ObNumber &get_min_value() const
  {
    return option_.get_min_value();
  }
  inline const common::number::ObNumber &get_max_value() const
  {
    return option_.get_max_value();
  }
  inline const common::number::ObNumber &get_increment_by() const
  {
    return option_.get_increment_by();
  }
  inline const common::number::ObNumber &get_start_with() const
  {
    return option_.get_start_with();
  }
  inline const common::number::ObNumber &get_cache_size() const
  {
    return option_.get_cache_size();
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline uint64_t get_sequence_id() const
  {
    return sequence_id_;
  }
  // inline int64_t get_min_value() const { return option_.get_min_value(); }
  // inline int64_t get_max_value() const { return option_.get_max_value(); }
  // inline int64_t get_increment_by() const { return option_.get_increment_by(); }
  // inline int64_t get_start_with() const { return option_.get_start_with(); }
  // inline int64_t get_cache_size() const { return option_.get_cache_size(); }
  inline bool get_cycle_flag() const
  {
    return option_.get_cycle_flag();
  }
  inline bool get_order_flag() const
  {
    return option_.get_order_flag();
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const common::ObString &get_sequence_name() const
  {
    return name_;
  }
  inline const char *get_sequence_name_str() const
  {
    return extract_str(name_);
  }
  inline share::ObSequenceOption &get_sequence_option()
  {
    return option_;
  }
  inline const share::ObSequenceOption &get_sequence_option() const
  {
    return option_;
  }
  inline ObTenantSequenceId get_tenant_sequence_id() const
  {
    return ObTenantSequenceId(tenant_id_, sequence_id_);
  }

  void reset();

  VIRTUAL_TO_STRING_KV(K_(name), K_(tenant_id), K_(database_id), K_(sequence_id), K_(schema_version), K_(option));

private:
  // void *alloc(int64_t size);
  // int get_value(const common::ObString &str, int64_t &val);
  // int get_value(const common::number::ObNumber &num, int64_t &val);
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t sequence_id_;
  int64_t schema_version_;  // the last modify timestamp of this version
  common::ObString name_;   // sequence name
  share::ObSequenceOption option_;
  // common::ObArenaAllocator allocator_;
};

typedef ObSequenceSchema ObSequenceInfo;

class ObTenantCommonSchemaId {
  OB_UNIS_VERSION(1);

public:
  ObTenantCommonSchemaId() : tenant_id_(common::OB_INVALID_TENANT_ID), schema_id_(common::OB_INVALID_ID)
  {}
  ObTenantCommonSchemaId(const uint64_t tenant_id, const uint64_t schema_id)
      : tenant_id_(tenant_id), schema_id_(schema_id)
  {}
  bool operator==(const ObTenantCommonSchemaId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (schema_id_ == rhs.schema_id_);
  }
  bool operator!=(const ObTenantCommonSchemaId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantCommonSchemaId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = schema_id_ < rhs.schema_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&schema_id_, sizeof(schema_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_TENANT_ID) && (schema_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(schema_id));
  uint64_t tenant_id_;
  uint64_t schema_id_;
};

class ObTenantProfileId : public ObTenantCommonSchemaId {
  OB_UNIS_VERSION(1);

public:
  ObTenantProfileId() : ObTenantCommonSchemaId()
  {}
  ObTenantProfileId(const uint64_t tenant_id, const uint64_t profile_id) : ObTenantCommonSchemaId(tenant_id, profile_id)
  {}
};

class ObProfileSchema : public ObSchema {  // simple schema
  OB_UNIS_VERSION(1);

public:
  static const int64_t INVALID_VALUE = -1;

  enum PARAM_TYPES {
    FAILED_LOGIN_ATTEMPTS = 0,
    PASSWORD_LOCK_TIME,
    PASSWORD_VERIFY_FUNCTION,
    PASSWORD_LIFE_TIME,
    PASSWORD_GRACE_TIME,
    /*
    PASSWORD_REUSE_TIME,
    PASSWORD_REUSE_MAX,
    SESSIONS_PER_USER,
    CPU_PER_SESSION,
    CPU_PER_CALL,
    CONNECT_TIME,
    IDLE_TIME,
    LOGICAL_READS_PER_SESSION,
    LOGICAL_READS_PER_CALL,
    PRIVATE_SGA,
    */
    MAX_PARAMS
  };

  static const int64_t DEFAULT_PARAM_VALUES[MAX_PARAMS];
  static const int64_t INVALID_PARAM_VALUES[MAX_PARAMS];
  static const char *PARAM_VALUE_NAMES[MAX_PARAMS];

  ObProfileSchema();
  explicit ObProfileSchema(common::ObIAllocator *allocator);
  virtual ~ObProfileSchema();

  TO_STRING_KV(K_(tenant_id), K_(profile_id), K_(schema_version), K_(profile_name), K_(failed_login_attempts),
      K_(password_lock_time), K_(password_life_time), K_(password_grace_time));

  bool is_valid() const;
  void reset();
  int64_t get_convert_size() const;

  ObProfileSchema &operator=(const ObProfileSchema &src_schema);
  ObProfileSchema(const ObProfileSchema &src_schema);

  inline void set_tenant_id(const uint64_t id)
  {
    tenant_id_ = id;
  }
  inline void set_profile_id(uint64_t id)
  {
    profile_id_ = id;
  }
  inline void set_schema_version(int64_t version)
  {
    schema_version_ = version;
  }
  inline int set_profile_name(const char *name)
  {
    return deep_copy_str(name, profile_name_);
  }
  inline int set_profile_name(const common::ObString &name)
  {
    return deep_copy_str(name, profile_name_);
  }
  inline void set_failed_login_attempts(const int64_t value)
  {
    failed_login_attempts_ = value;
  }
  inline void set_password_lock_time(const int64_t value)
  {
    password_lock_time_ = value;
  }
  inline void set_password_life_time(const int64_t value)
  {
    password_life_time_ = value;
  }
  inline void set_password_grace_time(const int64_t value)
  {
    password_grace_time_ = value;
  }

  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline uint64_t get_profile_id() const
  {
    return profile_id_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline const char *get_profile_name() const
  {
    return extract_str(profile_name_);
  }
  inline const common::ObString &get_profile_name_str() const
  {
    return profile_name_;
  }
  inline int64_t get_failed_login_attempts() const
  {
    return failed_login_attempts_;
  }
  inline int64_t get_password_lock_time() const
  {
    return password_lock_time_;
  }
  inline int64_t get_password_life_time() const
  {
    return password_life_time_;
  }
  inline int64_t get_password_grace_time() const
  {
    return password_grace_time_;
  }

  inline ObTenantProfileId get_tenant_profile_id() const
  {
    return ObTenantProfileId(tenant_id_, profile_id_);
  }

  int set_value(const int64_t type, const int64_t value);
  int set_default_value(const int64_t type);
  int set_default_values();
  int set_invalid_values();
  static int get_default_value(const int64_t type, int64_t &value);

  inline const char *get_password_verify_function() const
  {
    return extract_str(password_verify_function_);
  }
  inline const common::ObString &get_password_verify_function_str() const
  {
    return password_verify_function_;
  }
  inline int set_password_verify_function(const char *name)
  {
    return deep_copy_str(name, password_verify_function_);
  }
  inline int set_password_verify_function(const common::ObString &name)
  {
    return deep_copy_str(name, password_verify_function_);
  }

private:
  uint64_t tenant_id_;
  uint64_t profile_id_;
  int64_t schema_version_;
  common::ObString profile_name_;
  int64_t failed_login_attempts_;
  int64_t password_lock_time_;
  common::ObString password_verify_function_;
  int64_t password_life_time_;
  int64_t password_grace_time_;
};

common::ObIAllocator *&schema_stack_allocator();

struct ObObjectStruct {
  ObObjectStruct() : type_(ObObjectType::INVALID), id_(common::OB_INVALID_ID){};
  ObObjectStruct(const ObObjectType type, const uint64_t id) : type_(type), id_(id){};
  ~ObObjectStruct(){};

  TO_STRING_KV(K_(type), K_(id));

  ObObjectType type_;
  uint64_t id_;
};

//
class IObErrorInfo {
public:
  IObErrorInfo()
  {}
  virtual ~IObErrorInfo() = 0;
  virtual uint64_t get_object_id() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual uint64_t get_database_id() const = 0;
  virtual int64_t get_schema_version() const = 0;
  virtual ObObjectType get_object_type() const = 0;
  static constexpr uint64_t TYPE_MASK = (0x7) << (sizeof(uint64_t) - 3);
  static constexpr uint64_t VAL_MASK = (-1) >> 3;
  void inline gen_mask(uint64_t type, uint64_t &id)
  {
    id = (TYPE_MASK & type) | (VAL_MASK & id);
  }
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */

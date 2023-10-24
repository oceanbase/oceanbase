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
#include "common/ob_tablet_id.h"
#include "common/row/ob_row_util.h"
#include "share/ob_arbitration_service_status.h" // for ObArbitrationServieStatus
#include "share/ob_replica_info.h"
#include "share/ob_duplicate_scope_define.h"
#include "share/sequence/ob_sequence_option.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/schema/ob_priv_type.h"
#include "share/ob_priv_common.h"
#include "lib/worker.h"
#include "objit/common/ob_item_type.h"
#include "share/ob_share_util.h"          // ObIDGenerator
#include "share/cache/ob_kv_storecache.h" // ObKVCacheHandle
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/string/ob_sql_string.h"

#ifdef __cplusplus
extern "C" {
#endif
struct _ParseNode;
typedef struct _ParseNode ParseNode;
#ifdef __cplusplus
}
#endif

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObSqlString;
class ObString;
class ObDataTypeCastParams;
class ObKVCacheHandle;
}
namespace sql
{
class ObSQLSessionInfo;
class ObPartitionExecutorUtils;
}
namespace rootserver
{
class ObRandomZoneSelector;
class ObReplicaAddr;
}
namespace share
{
namespace schema
{
#define ARRAY_NEW_CONSTRUCT(TYPE, ARRAY) \
  for (int64_t i = 0; i < ARRAY.count() && OB_SUCC(ret); ++i) { \
      TYPE *this_set = &ARRAY.at(i);\
      if (nullptr == (this_set = new (this_set) TYPE())) {\
        ret = OB_ERR_UNEXPECTED;\
        LOG_WARN("placement new return nullptr", K(ret));\
      }\
    }\

typedef common::ParamStore ParamStore;
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
class ObTableSchema;
class ObColumnSchemaV2;
#define ASSIGN_STRING(dst, src, field, buffer, skip)\
  (dst)->field.assign(buffer + offset, (src)->field.length());\
  offset += (src)->field.length() + (skip);

#define ASSIGN_CONST_STRING(dst, src, field, buffer, skip)\
  (const_cast<ObString &>((dst)->field)).assign(buffer + offset, (src)->field.length());\
  offset += (src)->field.length() + (skip);

//match the default now func
#define IS_DEFAULT_NOW_STR(data_type, def_str) \
    ((ObDateTimeType == data_type || ObTimestampType == data_type) && (def_str == N_UPPERCASE_CUR_TIMESTAMP))

#define IS_DEFAULT_NOW_OBJ(def_obj) \
  (ObExtendType == def_obj.get_type() && ObActionFlag::OP_DEFAULT_NOW_FLAG == def_obj.get_ext())

#define OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN 60

//the lower 32-bit flag need be store in __all_column
static const uint64_t OB_MIN_ID  = 0;//used for lower_bound
#define NON_CASCADE_FLAG INT64_C(0)
#define VIRTUAL_GENERATED_COLUMN_FLAG (INT64_C(1) << 0)
#define STORED_GENERATED_COLUMN_FLAG (INT64_C(1) << 1)
#define CTE_GENERATED_COLUMN_FLAG (INT64_C(1) << 2)
#define DEFAULT_EXPR_V2_COLUMN_FLAG (INT64_C(1) << 3)
#define PRIMARY_VP_COLUMN_FLAG (INT64_C(1) << 4)
#define AUX_VP_COLUMN_FLAG (INT64_C(1) << 5)
#define INVISIBLE_COLUMN_FLAG (INT64_C(1) << 6)
#define LABEL_SE_COLUMN_FLAG (INT64_C(1) << 7)	 //column which maintaining labels is for label security policy,
                                                 //and the flag also marks the corresponding policy is enabled on this table
// The logic of the new table without a primary key changes the column (partition key) to the primary key
#define HEAP_ALTER_ROWKEY_FLAG (INT64_C(1) << 8)
#define ALWAYS_IDENTITY_COLUMN_FLAG (INT64_C(1) << 9)
#define DEFAULT_IDENTITY_COLUMN_FLAG (INT64_C(1) << 10)
#define DEFAULT_ON_NULL_IDENTITY_COLUMN_FLAG (INT64_C(1) << 11)
#define HEAP_TABLE_SORT_ROWKEY_FLAG (INT64_C(1) << 12) // 1:sortkey in new no pk table
#define COLUMN_NOT_NULL_CONSTRAINT_FLAG (INT64_C(1) << 13)
#define NOT_NULL_ENABLE_FLAG (INT64_C(1) << 14)
#define NOT_NULL_VALIDATE_FLAG (INT64_C(1) << 15)
#define NOT_NULL_RELY_FLAG (INT64_C(1) << 16)
#define USER_SPECIFIED_STORING_COLUMN_FLAG (INT64_C(1) << 17) // whether the storing column in index table is specified by user.
#define PAD_WHEN_CALC_GENERATED_COLUMN_FLAG (INT64_C(1) << 19)
#define GENERATED_COLUMN_UDF_EXPR (INT64_C(1) << 20)

//the high 32-bit flag isn't stored in __all_column
#define GENERATED_DEPS_CASCADE_FLAG (INT64_C(1) << 32)
#define GENERATED_CTXCAT_CASCADE_FLAG (INT64_C(1) << 33)
#define TABLE_PART_KEY_COLUMN_FLAG (INT64_C(1) << 34)
#define TABLE_ALIAS_NAME_FLAG (INT64_C(1) << 35)
/* create table t1(c1 int, c2 as (c1+1)) partition by hash(c2) partitions 2
   c1 and c2 has flag TABLE_PART_KEY_COLUMN_ORG_FLAG */
#define TABLE_PART_KEY_COLUMN_ORG_FLAG (INT64_C(1) << 37) //column is part key, or column is depened by part key(gc col)
#define SPATIAL_INDEX_GENERATED_COLUMN_FLAG (INT64_C(1) << 38) // for spatial index
#define SPATIAL_COLUMN_SRID_MASK (0xffffffffffffffe0L)

#define STORED_COLUMN_FLAGS_MASK 0xFFFFFFFF

// table_flags stored in __all_table.table_flag
#define CASCADE_RLS_OBJECT_FLAG (INT64_C(1) << 0)

// schema array size
static const int64_t SCHEMA_SMALL_MALLOC_BLOCK_SIZE = 64;
static const int64_t SCHEMA_MALLOC_BLOCK_SIZE = 128;
static const int64_t SCHEMA_MID_MALLOC_BLOCK_SIZE = 256;
static const int64_t SCHEMA_BIG_MALLOC_BLOCK_SIZE = 1024;

static const char* PUBLISH_SCHEMA_MODE_BEST_EFFORT = "BEST_EFFORT";
static const char* PUBLISH_SCHEMA_MODE_ASYNC   = "ASYNC";

//-------enum defenition
enum ObTableLoadType
{
  TABLE_LOAD_TYPE_IN_DISK = 0,
  TABLE_LOAD_TYPE_IN_RAM = 1,
  TABLE_LOAD_TYPE_MAX = 2,
};
//the defination type of table
enum ObTableDefType
{
  TABLE_DEF_TYPE_INTERNAL = 0,
  TABLE_DEF_TYPE_USER = 1,
  TABLE_DEF_TYPE_MAX = 2,
};
//level of patition
enum ObPartitionLevel
{
  PARTITION_LEVEL_ZERO = 0,//means non-partitioned table
  PARTITION_LEVEL_ONE = 1,
  PARTITION_LEVEL_TWO = 2,
  PARTITION_LEVEL_MAX,
};
// type of hash name to generate
enum ObHashNameType
{
  FIRST_PART = 0,
  TEMPLATE_SUB_PART = 1,
  INDIVIDUAL_SUB_PART = 2
};

enum ObPartitionFuncType
{
  //TODO add other type
  PARTITION_FUNC_TYPE_HASH = 0,
  PARTITION_FUNC_TYPE_KEY,
  PARTITION_FUNC_TYPE_KEY_IMPLICIT,
  PARTITION_FUNC_TYPE_RANGE,
  PARTITION_FUNC_TYPE_RANGE_COLUMNS,
  PARTITION_FUNC_TYPE_LIST,
  PARTITION_FUNC_TYPE_LIST_COLUMNS,
  PARTITION_FUNC_TYPE_INTERVAL,
  PARTITION_FUNC_TYPE_MAX,
};

enum ObObjectStatus : int64_t
{
  INVALID = 0,
  VALID = 1,
  NA = 2, /*The use case is unknown*/
};

int get_part_type_str(const bool is_oracle_mode, ObPartitionFuncType type, common::ObString &str);

inline bool is_hash_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_HASH == part_type;
}

inline bool is_hash_like_part(const ObPartitionFuncType part_type) {
  return PARTITION_FUNC_TYPE_HASH == part_type
         || PARTITION_FUNC_TYPE_KEY == part_type
         || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type;
}

inline bool is_key_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_KEY == part_type
         || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type;
}

inline bool is_range_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_RANGE == part_type
      || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type
      || PARTITION_FUNC_TYPE_INTERVAL == part_type;
}

inline bool is_interval_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_INTERVAL == part_type;
}

inline bool is_list_part(const ObPartitionFuncType part_type)
{
  return PARTITION_FUNC_TYPE_LIST == part_type
      || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type;
}

int is_sys_table_name(uint64_t database_id, const common::ObString &table_name, bool &is_sys_table);


// adding new table type, take care ObRootUtils::is_balance_target_schema() interface
// This structure indicates whether the tableSchema of this type is the object of load balancing,
// and the judgment is based on whether the table schema has physical partitions of entities,
// and only non-system tables are load-balanced.
enum ObTableType
{
  SYSTEM_TABLE   = 0,
  SYSTEM_VIEW    = 1,
  VIRTUAL_TABLE  = 2,
  USER_TABLE     = 3,
  USER_VIEW      = 4,
  USER_INDEX     = 5,      // urgly, compatible with uniform process in ddl_service
                           // will add index for sys table???
  TMP_TABLE      = 6,      // Temporary table in mysql compatibility mode
  MATERIALIZED_VIEW  = 7,  // Must be put behind, otherwise compatibility will fail
  TMP_TABLE_ORA_SESS = 8,  // Temporary table in oracle compatibility mode, session level
  TMP_TABLE_ORA_TRX  = 9,  // Temporary tables in oracle compatibility mode, transaction level
  TMP_TABLE_ALL      = 10, // All types of temporary tables, only used for alter system statements
  AUX_VERTIAL_PARTITION_TABLE = 11,
  AUX_LOB_PIECE  = 12,
  AUX_LOB_META   = 13,
  EXTERNAL_TABLE = 14,
  MAX_TABLE_TYPE
};

//ObTableType=>const char* ; used for show tables
const char *ob_table_type_str(ObTableType type);
const char *ob_mysql_table_type_str(ObTableType type);

ObTableType get_inner_table_type_by_id(const uint64_t tid);

bool is_mysql_tmp_table(const ObTableType table_type);
bool is_view_table(const ObTableType table_type);
bool is_index_table(const ObTableType table_type);
bool is_aux_lob_meta_table(const ObTableType table_type);
bool is_aux_lob_piece_table(const ObTableType table_type);
bool is_aux_lob_table(const ObTableType table_type);

enum ObIndexType
{
  INDEX_TYPE_IS_NOT = 0,//is not index table
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
  // INDEX_TYPE_MAX = 9 in 4.0
  // new index types for gis
  INDEX_TYPE_SPATIAL_LOCAL = 10,
  INDEX_TYPE_SPATIAL_GLOBAL = 11,
  INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE = 12,

  INDEX_TYPE_MAX = 13,
};

// using type for index
enum ObIndexUsingType
{
  USING_BTREE = 0,
  USING_HASH,
  USING_TYPE_MAX,
};

enum ViewCheckOption
{
  VIEW_CHECK_OPTION_NONE = 0,
  VIEW_CHECK_OPTION_LOCAL = 1,
  VIEW_CHECK_OPTION_CASCADED = 2,
  VIEW_CHECK_OPTION_MAX = 3,
};

const char *ob_view_check_option_str(const ViewCheckOption option);
enum ObIndexStatus
{
  //this is used in index virtual table:__index_process_info:means the table may be deleted when you get it
  INDEX_STATUS_NOT_FOUND = 0,
  INDEX_STATUS_UNAVAILABLE = 1,
  INDEX_STATUS_AVAILABLE = 2,
  INDEX_STATUS_UNIQUE_CHECKING = 3, // not used anymore
  INDEX_STATUS_UNIQUE_INELIGIBLE = 4, // not used anymore
  INDEX_STATUS_INDEX_ERROR = 5,
  INDEX_STATUS_RESTORE_INDEX_ERROR = 6,
  INDEX_STATUS_UNUSABLE = 7,
  INDEX_STATUS_MAX = 8,
};

enum PartitionType
{
  PARTITION_TYPE_NORMAL = 0,              // normal partition
  PARTITION_TYPE_SPLIT_SOURCE = 1,        // hidden partition, source partition for partition split
  PARTITION_TYPE_SPLIT_DESTINATION = 2,   // hidden partition, destination partition for partition split
  PARTITION_TYPE_MERGE_SOURCE = 3,        // hidden partition, source partition for partition merge
  PARTITION_TYPE_MERGE_DESTINATION = 4,   // hidden partition, destination partition for partition merge
  PARTITION_TYPE_MAX,
};

bool is_normal_partition(const PartitionType partition_type);

bool is_hidden_partition(const PartitionType partition_type);

#define CHECK_PARTITION_NORMAL_FLAG  (INT64_C(1) << 0)
#define CHECK_PARTITION_HIDDEN_FLAG  (INT64_C(1) << 1)

enum ObCheckPartitionMode
{
  CHECK_PARTITION_MODE_NORMAL = (CHECK_PARTITION_NORMAL_FLAG),
  CHECK_PARTITION_MODE_ALL = (CHECK_PARTITION_HIDDEN_FLAG | CHECK_PARTITION_NORMAL_FLAG),
};

bool check_normal_partition(const ObCheckPartitionMode check_partition_mode);

bool check_hidden_partition(const ObCheckPartitionMode check_partition_mode);

enum ObPartitionStatus
{
  PARTITION_STATUS_INVALID = -1,
  PARTITION_STATUS_ACTIVE = 0,
  PARTITION_STATUS_LOGICAL_SPLITTING = 1,   // deprecated
  PARTITION_STATUS_MERGE = 2,
  PARTITION_STATUS_PHYSICAL_SPLITTING = 3,  // deprecated
  PARTITION_STATUS_SPLIT = 4,
  PARTITION_STATUS_MAX,
};

enum ObAlterColumnMode
{
  ALTER_COLUMN_MODE_FORBIDDEN = 0,
  ALTER_COLUMN_MODE_ALLOWED = 1,
  ALTER_COLUMN_MODE_FORBIDDEN_WHEN_MAJOR = 2,
};


lib::Worker::CompatMode get_worker_compat_mode(const ObCompatibilityMode &mode);

struct ObRefreshSchemaStatus
{
public:
  ObRefreshSchemaStatus() : tenant_id_(common::OB_INVALID_TENANT_ID),
                            snapshot_timestamp_(common::OB_INVALID_TIMESTAMP),
                            readable_schema_version_(common::OB_INVALID_VERSION)
  {}


  ObRefreshSchemaStatus(const uint64_t tenant_id,
                        const int64_t snapshot_timestamp,
                        const int64_t readable_schema_version)
      : tenant_id_(tenant_id),
        snapshot_timestamp_(snapshot_timestamp),
        readable_schema_version_(readable_schema_version)
  {}

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    snapshot_timestamp_ = common::OB_INVALID_TIMESTAMP;
    readable_schema_version_ = common::OB_INVALID_VERSION;
  }

  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }

  bool operator ==(const ObRefreshSchemaStatus &other) const
  {
    return ((this == &other)
        || (this->tenant_id_ == other.tenant_id_
          && this->snapshot_timestamp_ == other.snapshot_timestamp_
          && this->readable_schema_version_ == other.readable_schema_version_));
  }

  TO_STRING_KV(K_(tenant_id), K_(snapshot_timestamp), K_(readable_schema_version));
public:
  // tenant_id_ is OB_INVALID_TENANT_ID which means non-split mode, effectively means split mode
  uint64_t tenant_id_;
  // snapshot_timestamp_ > 0 Indicates that a weakly consistent read is required, and is used in standalone cluster mode
  int64_t snapshot_timestamp_;
  int64_t readable_schema_version_;

};

class ObSchemaIdVersion
{
public:
  ObSchemaIdVersion()
    : schema_id_(common::OB_INVALID_ID),
      schema_version_(common::OB_INVALID_VERSION)
  {}
  ~ObSchemaIdVersion() {}
  int init(const uint64_t schema_id, const int64_t schema_version);
  void reset();
  bool is_valid() const;
  uint64_t get_schema_id() const { return schema_id_; }
  int64_t get_schema_version() const { return schema_version_; }
  TO_STRING_KV(K_(schema_id), K_(schema_version));
private:
  uint64_t schema_id_;
  int64_t schema_version_;
};

class ObSchemaVersionGenerator : public ObIDGenerator
{
public:
  // when refresh schema, if new ddl operations are as following:
  // (ALTER USER TABLE, v1), (ALTER SYS TABLE, v2),
  // if we replay new ddl operation one by one, when we execute sql to read sys table
  // to fetch user table schema, leader partition server find sys table version not match,
  // read new user table schema will fail, so we need first refresh sys table schema,
  // then publish, then refresh new user table schemas and publish,
  // but what version we used to publish sys table schema when we haven't refresh use table,
  // we use a temporary version which means it don't contain all schema item whose version
  // is small than temporary version. now we have temporary core versin for core table,
  // temporary system version for system table, we set SCHEMA_VERSION_INC_STEP to 8 so that
  // it is enough when we add more temporary version
  static const int64_t SCHEMA_VERSION_INC_STEP = 8;
public:
  ObSchemaVersionGenerator()
    : ObIDGenerator(SCHEMA_VERSION_INC_STEP) {}
  virtual ~ObSchemaVersionGenerator() {}

  int init(const int64_t start_version, const int64_t end_version);
  int next_version(int64_t &current_version);
  int get_start_version(int64_t &start_version) const;
  int get_current_version(int64_t &current_version) const;
  int get_end_version(int64_t &end_version) const;
  int get_version_cnt(int64_t &version_cnt) const;
};
typedef ObSchemaVersionGenerator TSISchemaVersionGenerator;

struct ObRefreshSchemaInfo
{
  OB_UNIS_VERSION(1);
public:
  ObRefreshSchemaInfo()
    : schema_version_(common::OB_INVALID_VERSION),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      sequence_id_(common::OB_INVALID_ID)
  {}
  ObRefreshSchemaInfo(const ObRefreshSchemaInfo &other);
  virtual ~ObRefreshSchemaInfo() {}
  int assign(const ObRefreshSchemaInfo &other);
  void reset();
  bool is_valid() const;
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  void set_sequence_id(const uint64_t sequence_id) { sequence_id_ = sequence_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_schema_version() const { return schema_version_; }
  uint64_t get_sequence_id() const { return sequence_id_; }
  TO_STRING_KV(K_(schema_version), K_(tenant_id), K_(sequence_id));
private:
  int64_t schema_version_;
  uint64_t tenant_id_;
  uint64_t sequence_id_;
};

class ObDropTenantInfo
{
public:
  ObDropTenantInfo() :
      tenant_id_(common::OB_INVALID_TENANT_ID),
      schema_version_(common::OB_INVALID_VERSION) {}
  virtual ~ObDropTenantInfo() {};
  void reset();
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_schema_version() const {return schema_version_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  TO_STRING_KV(K_(tenant_id), K_(schema_version));
private:
  uint64_t tenant_id_;
  int64_t schema_version_;
};

struct ObIndexTableStat
{
  ObIndexTableStat()
    : index_id_(common::OB_INVALID_ID), index_status_(share::schema::INDEX_STATUS_UNAVAILABLE),
      is_drop_schema_(false)
  {}
  ObIndexTableStat(const uint64_t index_id, const share::schema::ObIndexStatus index_status,
      const bool is_drop_schema)
    : index_id_(index_id), index_status_(index_status), is_drop_schema_(is_drop_schema)
  {}
  TO_STRING_KV(K_(index_id), K_(index_status), K_(is_drop_schema));
  uint64_t index_id_;
  share::schema::ObIndexStatus index_status_;
  bool is_drop_schema_;
};

inline bool is_final_invalid_index_status(const ObIndexStatus index_status)
{
  bool ret_bool = !(index_status >= INDEX_STATUS_UNAVAILABLE
           && index_status <= INDEX_STATUS_UNIQUE_CHECKING);
  return ret_bool;
}

inline bool is_final_index_status(const ObIndexStatus index_status)
{
  return INDEX_STATUS_AVAILABLE == index_status
         || INDEX_STATUS_INDEX_ERROR == index_status
         || INDEX_STATUS_RESTORE_INDEX_ERROR == index_status
         || INDEX_STATUS_UNUSABLE == index_status;
}

inline bool is_error_index_status(const ObIndexStatus index_status)
{
  return INDEX_STATUS_INDEX_ERROR == index_status
         || INDEX_STATUS_RESTORE_INDEX_ERROR == index_status
         || INDEX_STATUS_UNUSABLE == index_status;
}

inline bool is_available_index_status(const ObIndexStatus index_status)
{
  return INDEX_STATUS_AVAILABLE == index_status;
}

const char *ob_index_status_str(ObIndexStatus status);

inline bool is_index_local_storage(ObIndexType index_type)
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type
           || INDEX_TYPE_UNIQUE_LOCAL == index_type
           || INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type
           || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type
           || INDEX_TYPE_PRIMARY == index_type
           || INDEX_TYPE_DOMAIN_CTXCAT == index_type
           || INDEX_TYPE_SPATIAL_LOCAL == index_type
           || INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE == index_type;
}

inline bool is_related_table(
    const ObTableType &table_type,
    const ObIndexType &index_type)
{
  return is_index_local_storage(index_type)
      || is_aux_lob_table(table_type);
}

inline bool index_has_tablet(const ObIndexType &index_type)
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type
        || INDEX_TYPE_UNIQUE_LOCAL == index_type
        || INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type
        || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type
        || INDEX_TYPE_NORMAL_GLOBAL == index_type
        || INDEX_TYPE_UNIQUE_GLOBAL == index_type
        || INDEX_TYPE_SPATIAL_LOCAL == index_type
        || INDEX_TYPE_SPATIAL_GLOBAL == index_type
        || INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE == index_type;
}

struct ObTenantTableId
{
  ObTenantTableId() : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID)
  {}
  ObTenantTableId(const uint64_t tenant_id, const uint64_t table_id)
      : tenant_id_(tenant_id),
        table_id_(table_id)
  {}
  bool operator ==(const ObTenantTableId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (table_id_ == rv.table_id_);
  }
  ObTenantTableId &operator = (const ObTenantTableId &tenant_table_id);
  int64_t hash() const { return table_id_; }
  bool operator <(const ObTenantTableId &rv) const
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

struct ObTenantDatabaseId
{
  ObTenantDatabaseId() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID)
  {}
  ObTenantDatabaseId(const uint64_t tenant_id, const uint64_t database_id)
      : tenant_id_(tenant_id),
        database_id_(database_id)
  {}
  bool operator ==(const ObTenantDatabaseId &rv) const
  {
    return ((tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_));
  }
  int64_t hash() const { return database_id_; }
  bool operator <(const ObTenantDatabaseId &rv) const
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

struct ObTenantTablegroupId
{
  ObTenantTablegroupId() : tenant_id_(common::OB_INVALID_ID), tablegroup_id_(common::OB_INVALID_ID)
  {}
  ObTenantTablegroupId(const uint64_t tenant_id, const uint64_t tablegroup_id)
      : tenant_id_(tenant_id),
        tablegroup_id_(tablegroup_id)
  {}
  bool operator ==(const ObTenantTablegroupId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (tablegroup_id_ == rv.tablegroup_id_);
  }
  int64_t hash() const { return tablegroup_id_; }
  bool operator <(const ObTenantTablegroupId &rv) const
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
  // PLAN_BASELINE_SCHEMA = 10, unused anymore
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
  LABEL_SE_POLICY_SCHEMA = 21,
  LABEL_SE_COMPONENT_SCHEMA = 22,
  LABEL_SE_LABEL_SCHEMA = 23,
  LABEL_SE_USER_LEVEL_SCHEMA = 24,
  PROFILE_SCHEMA = 25,
  AUDIT_SCHEMA = 26,
  SYS_PRIV = 27,
  OBJ_PRIV = 28,
  DBLINK_SCHEMA = 29,
  LINK_TABLE_SCHEMA = 30,
  FK_SCHEMA = 31,
  DIRECTORY_SCHEMA = 32,
  CONTEXT_SCHEMA = 33,
  VIEW_SCHEMA = 34,
  MOCK_FK_PARENT_TABLE_SCHEMA = 35,
  RLS_POLICY_SCHEMA = 36,
  RLS_GROUP_SCHEMA = 37,
  RLS_CONTEXT_SCHEMA = 38,
  CONSTRAINT_SCHEMA = 39,   // not dependent schema
  FOREIGN_KEY_SCHEMA = 40,  // not dependent schema
  ROUTINE_PRIV = 41,
  ///<<< add schema type before this line
  OB_MAX_SCHEMA
} ObSchemaType;

const char *schema_type_str(const ObSchemaType schema_type);

bool is_normal_schema(const ObSchemaType schema_type);

struct ObSchemaStatisticsInfo
{
  ObSchemaStatisticsInfo(ObSchemaType schema_type, int64_t size = 0, int64_t count = 0)
    : schema_type_(schema_type), size_(size), count_(count) {}
  ObSchemaStatisticsInfo()
    : schema_type_(OB_MAX_SCHEMA), size_(0), count_(0) {}
  ~ObSchemaStatisticsInfo() {}
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

struct ObSimpleTableSchema
{
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
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(tablegroup_id),
               K_(table_id),
               K_(data_table_id),
               K_(table_name),
               K_(schema_version),
               K_(table_type));
   bool is_valid() const
   {
     return (common::OB_INVALID_ID != tenant_id_ &&
             common::OB_INVALID_ID != database_id_ &&
             common::OB_INVALID_ID != tablegroup_id_ &&
             common::OB_INVALID_ID != table_id_ &&
             common::OB_INVALID_ID != data_table_id_ &&
             !table_name_.empty() &&
             schema_version_ >= 0 &&
             table_type_ != MAX_TABLE_TYPE);
   }
};

enum TableStatus {
  TABLE_STATUS_INVALID = -1,
  // The current schema version does not exist, it is a future table
  TABLE_NOT_CREATE,
  TABLE_EXIST,                // table exist
  TABLE_DELETED,              // table is deleted
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
    case TABLE_NOT_CREATE :
      status_str = "TABLE_NOT_CREATE";
      break;
    case TABLE_EXIST :
      status_str = "TABLE_EXIST";
      break;
    case TABLE_DELETED :
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
  PART_EXIST,                 // schema exist
  PART_DELETED,               // partition is deleted
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

enum ObDependencyTableType
{
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
  DEPENDENCY_LABEL_SE_POLICY = 11,
  DEPENDENCY_LABEL_SE_COMPONENT = 12,
  DEPENDENCY_LABEL_SE_LABEL = 13,
  DEPENDENCY_LABEL_SE_USER_LEVEL = 14,
  DEPENDENCY_TABLESPACE = 15,
  DEPENDENCY_TYPE_BODY = 16,
  DEPENDENCY_TRIGGER = 17
};

enum class ObObjectType {
  INVALID         = 0,
  TABLE           = 1,
  SEQUENCE        = 2,
  PACKAGE         = 3,
  TYPE            = 4,
  PACKAGE_BODY    = 5,
  TYPE_BODY       = 6,
  TRIGGER         = 7,
  VIEW            = 8,
  FUNCTION        = 9,
  DIRECTORY       = 10,
  INDEX           = 11,
  PROCEDURE       = 12,
  SYNONYM         = 13,
  SYS_PACKAGE     = 14,
  SYS_PACKAGE_ONLY_OBJ_PRIV = 15,
  CONTEXT         = 16,
  MAX_TYPE,
};
struct ObSchemaObjVersion
{
  ObSchemaObjVersion()
    : object_id_(common::OB_INVALID_ID),
      version_(0),
      // The default is table, which is compatible with the current logic
      object_type_(DEPENDENCY_TABLE),
      is_db_explicit_(false),
      is_existed_(true)
  {
  }

  ObSchemaObjVersion(int64_t object_id, int64_t version, ObDependencyTableType object_type)
      : object_id_(object_id),
        version_(version),
        object_type_(object_type),
        is_db_explicit_(false),
        is_existed_(true)
    {
    }

  inline void reset()
  {
    object_id_ = common::OB_INVALID_ID;
    version_ = 0;
    // The default is table, which is compatible with the current logic
    object_type_ = DEPENDENCY_TABLE;
    is_db_explicit_ = false;
    is_existed_ = true;
  }
  inline int64_t get_object_id() const { return object_id_; }
  inline int64_t get_version() const { return version_; }
  inline ObDependencyTableType get_type() const { return object_type_; }
  inline bool operator ==(const ObSchemaObjVersion &other) const
  { return object_id_ == other.object_id_
           && version_ == other.version_
           && object_type_ == other.object_type_
           && is_db_explicit_ == other.is_db_explicit_
           && is_existed_ == other.is_existed_; }
  inline bool operator !=(const ObSchemaObjVersion &other) const { return !operator ==(other); }
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
      case DEPENDENCY_TYPE_BODY:
        ret_type = UDT_SCHEMA;
        break;
      case DEPENDENCY_TRIGGER:
        ret_type = TRIGGER_SCHEMA;
        break;
      default:
        break;
    }
    return ret_type;
  }
  static ObObjectType get_schema_object_type(ObDependencyTableType object_type)
  {
    ObObjectType ret_type = ObObjectType::MAX_TYPE;
    switch (object_type) {
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
      case DEPENDENCY_SYNONYM:
        ret_type = ObObjectType::SYNONYM;
        break;
      default:
        break;
    }
    return ret_type;
  }
  inline bool is_valid() const { return common::OB_INVALID_ID != object_id_; }
  inline bool is_base_table() const { return DEPENDENCY_TABLE == object_type_; }
  inline bool is_synonym() const { return DEPENDENCY_SYNONYM == object_type_; }
  inline bool is_procedure() const { return DEPENDENCY_PROCEDURE == object_type_; }
  inline bool is_db_explicit() const { return is_db_explicit_; }
  inline bool is_existed() const { return is_existed_; }

  int64_t object_id_;
  int64_t version_;
  ObDependencyTableType object_type_;
  bool is_db_explicit_;
  bool is_existed_;

  TO_STRING_KV(N_TID, object_id_,
               N_SCHEMA_VERSION, version_,
               K_(object_type),
               K_(is_db_explicit),
               K_(is_existed));
  OB_UNIS_VERSION(1);
};



struct ObSysParam
{
  ObSysParam();
  ~ObSysParam();

  int init(const uint64_t tenant_id,
           const common::ObZone &zone,
           const common::ObString &name,
           const int64_t data_type,
           const common::ObString &value,
           const common::ObString &min_val,
           const common::ObString &max_val,
           const common::ObString &info,
           const int64_t flags);
  int init(const uint64_t tenant_id,
           const common::ObString &name,
           const int64_t data_type,
           const common::ObString &value,
           const common::ObString &min_val,
           const common::ObString &max_val,
           const common::ObString &info,
           const int64_t flags);
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
struct ObZoneScore
{
  ObZoneScore(common::ObString &zone, int64_t score) : zone_(zone), score_(score) {}
  ObZoneScore() : zone_(), score_(INT64_MAX) {}
  virtual ~ObZoneScore() {}
  bool operator<(const ObZoneScore &that) {
    return score_ < that.score_;
  }
  void reset() { zone_.reset(); score_ = INT64_MAX; }
  TO_STRING_KV(K(zone_), K(score_));

  common::ObString zone_;
  int64_t score_;
};
// ObZoneRegion is used to construct the primary zone array of TableSchema/DataBaseSchema/TenantSchema.
// It is only an intermediate variable during the construction process. ObZoneRegion will not be saved
// in the schema eventually. This structure saves the zone and the region in which the zone is located.
struct ObZoneRegion
{
public:
  enum CheckZoneType
  {
    CZY_ENCRYPTION = 0,
    CZY_NO_ENCRYPTION,
    CZY_NO_NEED_TO_CHECK,
    CZY_MAX,
  };
public:
  ObZoneRegion()
    : zone_(),
      region_(),
      check_zone_type_(CZY_MAX) {}
  ObZoneRegion(const ObZoneRegion &that)
    : zone_(that.zone_),
      region_(that.region_),
      check_zone_type_(that.check_zone_type_) {}
  ObZoneRegion(const common::ObZone &zone, const common::ObRegion &region)
    : zone_(zone),
      region_(region),
      check_zone_type_(CZY_NO_ENCRYPTION) {}
  ObZoneRegion(const common::ObZone &zone,
               const common::ObRegion &region,
               const CheckZoneType check_zone_type)
    : zone_(zone),
      region_(region),
      check_zone_type_(check_zone_type) {}
  virtual ~ObZoneRegion() {}
  void reset() { zone_.reset(); region_.reset(); check_zone_type_ = CZY_MAX; }
  int assign(const ObZoneRegion &that);
  int set_check_zone_type(const int64_t zone_type);
  TO_STRING_KV(K(zone_), K(region_), K(check_zone_type_));

  common::ObZone zone_;
  common::ObRegion region_;
  CheckZoneType check_zone_type_;
};

typedef common::ObIArray<share::ObZoneReplicaAttrSet> ZoneLocalityIArray;
typedef common::ObArrayHelper<share::SchemaZoneReplicaAttrSet> ZoneLocalityArray;

class ObCompareNameWithTenantID
{
public:
  ObCompareNameWithTenantID()
     : tenant_id_(common::OB_INVALID_ID),
       name_case_mode_(common::OB_NAME_CASE_INVALID),
       database_id_(common::OB_INVALID_ID)
  {
  }
  ObCompareNameWithTenantID(uint64_t tenant_id)
      : tenant_id_(tenant_id), name_case_mode_(common::OB_NAME_CASE_INVALID),
        database_id_(common::OB_INVALID_ID)
  {
  }
  ObCompareNameWithTenantID(uint64_t tenant_id, common::ObNameCaseMode mode)
      : tenant_id_(tenant_id), name_case_mode_(mode),
        database_id_(common::OB_INVALID_ID)
  {
  }
  ObCompareNameWithTenantID(uint64_t tenant_id,
                            common::ObNameCaseMode mode,
                            uint64_t database_id)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_id_(database_id)
  {
  }
  ~ObCompareNameWithTenantID() {}
  int compare(const common::ObString &str1, const common::ObString &str2);
private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  uint64_t database_id_;
};

typedef common::ObArray<ObZoneScore> ObPrimaryZoneArray;

class ObSchema
{
public:
  friend class ObLocality;
  friend class ObPrimaryZone;
  ObSchema();
  //explicit ObSchema(common::ObDataBuffer &buffer);
  explicit ObSchema(common::ObIAllocator *allocator);
  virtual ~ObSchema();
  virtual void reset();
  virtual bool is_valid() const { return common::OB_SUCCESS == error_ret_; }
  virtual int zone_array2str(const common::ObIArray<common::ObZone> &zone_list,
                             char *str, const int64_t buf_size) const;
  virtual int set_primary_zone_array(const common::ObIArray<ObZoneScore> &src,
                                     common::ObIArray<ObZoneScore> &dst);
  virtual int string_array2str(const common::ObIArray<common::ObString> &string_array,
                               char *buf, const int64_t buf_size) const;
  virtual int str2string_array(const char *str,
                               common::ObIArray<common::ObString> &string_array) const;

  virtual int64_t get_convert_size() const { return 0;};
  template<typename DSTSCHEMA>
  static int set_charset_and_collation_options(common::ObCharsetType src_charset_type,
                                               common::ObCollationType src_collation_type,
                                               DSTSCHEMA &dst);
  static common::ObCollationType get_cs_type_with_cmp_mode(const common::ObNameCaseMode mode);

  ObSchema *get_buffer() const { return buffer_; }
  void set_buffer(ObSchema *buffer) { buffer_ = buffer; }
  common::ObIAllocator *get_allocator();
  void reset_allocator();
  int get_assign_ret() {return error_ret_;}
  inline int get_err_ret() const { return error_ret_; }
protected:
  static const int64_t STRING_ARRAY_EXTEND_CNT = 7;
  void *alloc(const int64_t size);
  void free(void *ptr);
  int deep_copy_str(const char *src, common::ObString &dest);
  int deep_copy_str(const common::ObString &src, common::ObString &dest);
  int deep_copy_obj(const common::ObObj &src, common::ObObj &dest);
  int deep_copy_string_array(const common::ObIArray<common::ObString> &src,
                             common::ObArrayHelper<common::ObString> &dst);
  int add_string_to_array(const common::ObString &str,
                          common::ObArrayHelper<common::ObString> &str_array);
  int serialize_string_array(char *buf, const int64_t buf_len, int64_t &pos,
                             const common::ObArrayHelper<common::ObString> &str_array) const;
  int deserialize_string_array(const char *buf, const int64_t data_len, int64_t &pos,
                               common::ObArrayHelper<common::ObString> &str_array);
  int64_t get_string_array_serialize_size(
      const common::ObArrayHelper<common::ObString> &str_array) const;
  void reset_string(common::ObString &str);
  void reset_string_array(common::ObArrayHelper<common::ObString> &str_array);
  const char *extract_str(const common::ObString &str) const;
  template <class T>
  int preserve_array(T** &array, int64_t &array_capacity, const int64_t &preserved_capacity);
  // buffer is the memory used to store schema item, if not same with this pointer,
  // it means that this schema item has already been rewrote to buffer when rewriting
  // other schema manager, and when we want to rewrite this schema item in current schema
  // manager, we just set pointer of schema item in schema manager to buffer
  ObSchema *buffer_;
  int error_ret_;
  bool is_inner_allocator_;
  common::ObIAllocator *allocator_;
};

template<typename DSTSCHEMA>
int ObSchema::set_charset_and_collation_options(common::ObCharsetType src_charset_type,
                                                common::ObCollationType src_collation_type,
                                                DSTSCHEMA &dst)
{
  int ret = common::OB_SUCCESS;
  if (dst.get_charset_type() == common::CHARSET_INVALID
      && dst.get_collation_type() == common::CS_TYPE_INVALID) {
    //use upper layer schema's charset and collation type
    if (src_charset_type == common::CHARSET_INVALID
        || src_collation_type == common::CS_TYPE_INVALID) {
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
      SHARE_SCHEMA_LOG(WARN, "fail to check charset collation",
                       K(charset_type), K(collation_type), K(ret));
    } else {
      dst.set_charset_type(charset_type);
      dst.set_collation_type(collation_type);
    }
  }
  if (common::OB_SUCCESS == ret &&
      !common::ObCharset::is_valid_collation(dst.get_charset_type(),
                                             dst.get_collation_type())) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid collation!", K(dst.get_charset_type()),
                     K(dst.get_collation_type()), K(ret));
  }
  return ret;
}

struct SchemaObj
{
  SchemaObj()
  : schema_type_(OB_MAX_SCHEMA),
    tenant_id_(common::OB_INVALID_ID),
    schema_id_(common::OB_INVALID_ID),
    schema_(NULL),
    handle_()
  {}
  ObSchemaType schema_type_;
  uint64_t tenant_id_;
  uint64_t schema_id_;
  ObSchema *schema_;
  common::ObKVCacheHandle handle_;
  TO_STRING_KV(K_(schema_type), K_(tenant_id), K_(schema_id), KP_(schema));
};

class ObLocality
{
  OB_UNIS_VERSION(1);
public:
  explicit ObLocality(ObSchema *schema) : schema_(schema) {}
  int assign(const ObLocality &other);
  int set_locality_str(const common::ObString &locality);
  int set_zone_replica_attr_array(
      const common::ObIArray<share::ObZoneReplicaAttrSet> &src);
  int set_zone_replica_attr_array(
      const common::ObIArray<share::SchemaZoneReplicaAttrSet> &src);
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray &schema_replica_set,
      const common::ObIArray<ReplicaAttr> &src);
  void reset_zone_replica_attr_array();
  int64_t get_convert_size() const;
  inline const common::ObString &get_locality_str() const { return locality_str_; }
  void reset();
  TO_STRING_KV(K_(locality_str), K_(zone_replica_attr_array));
public:
  common::ObString locality_str_;
  ZoneLocalityArray zone_replica_attr_array_;
  ObSchema *schema_;
};

class ObPrimaryZone
{
  OB_UNIS_VERSION(1);
public:
  ObPrimaryZone();
  ObPrimaryZone(common::ObIAllocator &alloc);
  int assign(const ObPrimaryZone &other);
  inline const common::ObString &get_primary_zone() const { return primary_zone_str_; }
  inline const common::ObIArray<ObZoneScore> &get_primary_zone_array() const {
    return primary_zone_array_;
  }
  void set_allocator(common::ObIAllocator *allocator);
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);
  int set_primary_zone(const common::ObString &zone);
  int64_t get_convert_size() const;
  void reset();
  TO_STRING_KV(K_(primary_zone_str), K_(primary_zone_array));
public:
  common::ObIAllocator *allocator_;
  common::ObString primary_zone_str_;
  ObPrimaryZoneArray primary_zone_array_;
};

class ObSysVarSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSysVarSchema() : ObSchema() { reset(); }
  ~ObSysVarSchema() {}
  explicit ObSysVarSchema(common::ObIAllocator *allocator);
  explicit ObSysVarSchema(const ObSysVarSchema &src_schema);
  ObSysVarSchema &operator=(const ObSysVarSchema &src_schema);
  int assign(const ObSysVarSchema &other);
  virtual bool is_valid() const { return ObSchema::is_valid() && tenant_id_ != common::OB_INVALID_ID && !name_.empty(); }
  void reset();
  int64_t get_convert_size() const;
  bool is_equal_except_value(const ObSysVarSchema &other) const;
  bool is_equal_for_add(const ObSysVarSchema &other) const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  const common::ObString &get_name() const { return name_; }
  int set_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  common::ObObjType get_data_type() const { return data_type_; }
  void set_data_type(common::ObObjType data_type) { data_type_ = data_type; }
  const common::ObString &get_value() const { return value_; }
  int get_value(common::ObIAllocator *allocator, const common::ObDataTypeCastParams &dtc_params, common::ObObj &value) const;
  inline int get_value(common::ObIAllocator *allocator, const common::ObTimeZoneInfo *tz_info, common::ObObj &value) const
  {
    const common::ObDataTypeCastParams dtc_params(tz_info);
    return get_value(allocator, dtc_params, value);
  }
  int set_value(const common::ObString &value) { return deep_copy_str(value, value_); }
  const common::ObString &get_min_val() const { return min_val_; }
  int set_min_val(const common::ObString &min_val) { return deep_copy_str(min_val, min_val_); }
  const common::ObString &get_max_val() const { return max_val_; }
  int set_max_val(const common::ObString &max_val) { return deep_copy_str(max_val, max_val_); }
  int64_t get_schema_version() const { return schema_version_; }
  void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  int64_t get_flags() const { return flags_; }
  void set_flags(int64_t flags) { flags_ = flags; }
  int set_info(const common::ObString &info) { return deep_copy_str(info, info_); }
  const common::ObString &get_info() const { return info_; }
  int set_zone(const common::ObZone &zone) { return zone_.assign(zone); }
  int set_zone(const common::ObString &zone) { return zone_.assign(zone); }
  const common::ObZone &get_zone() const { return zone_; }
  bool is_invisible() const { return 0 != (flags_ & ObSysVarFlag::INVISIBLE); }
  bool is_global() const { return 0 != (flags_ & ObSysVarFlag::GLOBAL_SCOPE); }
  bool is_query_sensitive() const { return 0 != (flags_ & ObSysVarFlag::QUERY_SENSITIVE); }
  bool is_oracle_only() const { return 0 != (flags_ & ObSysVarFlag::ORACLE_ONLY); }
  bool is_mysql_only() const { return 0 != (flags_ & ObSysVarFlag::MYSQL_ONLY); }
  bool is_read_only() const { return 0 != (flags_ & ObSysVarFlag::READONLY); }
  TO_STRING_KV(K_(tenant_id),
               K_(name),
               K_(data_type),
               K_(value),
               K_(min_val),
               K_(max_val),
               K_(info),
               K_(zone),
               K_(schema_version),
               K_(flags));
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

class ObSysVariableSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  //base methods
  ObSysVariableSchema();
  explicit ObSysVariableSchema(common::ObIAllocator *allocator);
  virtual ~ObSysVariableSchema();
  ObSysVariableSchema(const ObSysVariableSchema &src_schema);
  ObSysVariableSchema &operator=(const ObSysVariableSchema &src_schema);
  int assign(const ObSysVariableSchema &other);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id)  { tenant_id_ = tenant_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  void reset_sysvars() { memset(sysvar_array_, 0, sizeof(sysvar_array_)); }
  int64_t get_convert_size() const;
  int add_sysvar_schema(const share::schema::ObSysVarSchema &sysvar_schema);
  int load_default_system_variable(bool is_sys_tenant);
  int64_t get_sysvar_count() const { return ObSysVarFactory::ALL_SYS_VARS_COUNT; }
  int64_t get_real_sysvar_count() const;
  int get_sysvar_schema(const common::ObString &sysvar_name, const ObSysVarSchema *&sysvar_schema) const;
  int get_sysvar_schema(ObSysVarClassType var_type, const ObSysVarSchema *&sysvar_schema) const;
  const ObSysVarSchema *get_sysvar_schema(int64_t idx) const;
  ObSysVarSchema *get_sysvar_schema(int64_t idx);
  bool is_read_only() const { return read_only_; }
  common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  void set_name_case_mode(const common::ObNameCaseMode mode) { name_case_mode_ = mode; }
  int get_oracle_mode(bool &is_oracle_mode) const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version),
               "sysvars", common::ObArrayWrap<ObSysVarSchema *>(sysvar_array_, ObSysVarFactory::ALL_SYS_VARS_COUNT),
               K_(read_only), K_(name_case_mode));
private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  ObSysVarSchema *sysvar_array_[ObSysVarFactory::ALL_SYS_VARS_COUNT];
  bool read_only_;
  common::ObNameCaseMode name_case_mode_;
};

enum ObTenantStatus
{
  TENANT_STATUS_NORMAL = 0,
  TENANT_STATUS_CREATING = 1,
  TENANT_STATUS_DROPPING = 2,
  TENANT_STATUS_RESTORE = 3,
  TENANT_STATUS_CREATING_STANDBY = 4,
  TENANT_STATUS_MAX
};

const char *ob_tenant_status_str(const ObTenantStatus);

int get_tenant_status(const common::ObString &str, ObTenantStatus &status);

bool is_tenant_restore(ObTenantStatus &status);
bool is_tenant_normal(ObTenantStatus &status);
bool is_creating_standby_tenant_status(ObTenantStatus &status);
class ObTenantSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  //base methods
  ObTenantSchema();
  explicit ObTenantSchema(common::ObIAllocator *allocator);
  virtual ~ObTenantSchema();
  ObTenantSchema(const ObTenantSchema &src_schema);
  ObTenantSchema &operator=(const ObTenantSchema &src_schema);
  int assign(const ObTenantSchema &src_schema);
  //for sorted vector
  static bool cmp(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() < rhs->get_tenant_id() : false; }
  static bool equal(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() == rhs->get_tenant_id() : false; }
  static bool cmp_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  { return NULL != lhs ? lhs->get_tenant_id() < tenant_id : false; }
  static bool equal_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  { return NULL != lhs ? lhs->get_tenant_id() == tenant_id : false; }
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id)  { tenant_id_ = tenant_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int set_tenant_name(const char *tenant_name) { return deep_copy_str(tenant_name, tenant_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_locality(const char *locality) { return deep_copy_str(locality, locality_str_); }
  inline int set_previous_locality(const char *previous_locality) { return deep_copy_str(previous_locality, previous_locality_str_); }
  inline int set_tenant_name(const common::ObString &tenant_name) { return deep_copy_str(tenant_name, tenant_name_); }
  inline int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  int set_zone_list(const common::ObIArray<common::ObZone> &zone_list);
  inline int set_primary_zone(const common::ObString &zone);
  int set_primary_zone_array(const common::ObIArray<ObZoneScore> &primary_zone_array);
  inline int add_zone(const common::ObString &zone);
  inline void set_locked(const bool locked) { locked_ = locked; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline int set_locality(const common::ObString &locality) { return deep_copy_str(locality, locality_str_); }
  inline int set_previous_locality(const common::ObString &previous_locality) { return deep_copy_str(previous_locality, previous_locality_str_); }
  inline void set_arbitration_service_status(const ObArbitrationServiceStatus status) { arbitration_service_status_ = status; }
  inline int set_arbitration_service_status_from_string(const common::ObString &status) { return arbitration_service_status_.parse_from_string(status); }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) { collation_type_ = type; }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) { name_case_mode_ = mode; }
  int set_zone_replica_attr_array(
      const common::ObIArray<share::ObZoneReplicaAttrSet> &src);
  int set_zone_replica_attr_array(
      const common::ObIArray<share::SchemaZoneReplicaAttrSet> &src);
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray &schema_replica_set,
      const common::ObIArray<ReplicaAttr> &src);
  void reset_zone_replica_attr_array();
  inline void set_storage_format_version(const int64_t storage_format_version);
  inline void set_storage_format_work_version(const int64_t storage_format_work_version);
  void set_default_tablegroup_id(const uint64_t tablegroup_id) { default_tablegroup_id_ = tablegroup_id; }
  int set_default_tablegroup_name(const common::ObString &tablegroup_name) { return deep_copy_str(tablegroup_name, default_tablegroup_name_); }
  inline void set_compatibility_mode(const common::ObCompatibilityMode compatibility_mode) { compatibility_mode_ = compatibility_mode; }

  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_tenant_name() const { return extract_str(tenant_name_); }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const char *get_locality() const { return extract_str(locality_str_); }
  inline const char *get_previous_locality() const { return extract_str(previous_locality_str_); }
  inline const common::ObString &get_tenant_name_str() const { return tenant_name_; }
  inline const common::ObString &get_primary_zone() const { return primary_zone_; }
  inline bool get_locked() const { return locked_; }
  inline bool is_read_only() const { return read_only_; }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline const common::ObString &get_locality_str() const { return locality_str_; }
  inline const common::ObString &get_previous_locality_str() const { return previous_locality_str_; }
  inline const ObArbitrationServiceStatus &get_arbitration_service_status() const { return arbitration_service_status_; }
  inline const char* get_arbitration_service_status_str() const { return arbitration_service_status_.get_status_str(); }

  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  int get_primary_zone_score(
      const common::ObZone &zone,
      int64_t &zone_score) const;
  inline const ObPrimaryZoneArray &get_primary_zone_array() const {
    return primary_zone_array_;
  }
  int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObPrimaryZone &primary_zone) const;
  int get_zone_replica_attr_array(
      ZoneLocalityIArray &locality) const;
  int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &schema_guard,
      ZoneLocalityIArray &locality) const;
  int get_paxos_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const;
  int64_t get_all_replica_num() const;
  int64_t get_full_replica_num() const;

  int get_zone_list(
      common::ObIArray<common::ObZone> &zone_list) const;
  int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObZone> &zone_list) const;
  inline uint64_t get_default_tablegroup_id() const { return default_tablegroup_id_; }
  inline const common::ObString &get_default_tablegroup_name() const { return default_tablegroup_name_; }
  inline common::ObCompatibilityMode get_compatibility_mode() const { return compatibility_mode_; }
  inline bool is_oracle_tenant() const
  {
    return common::ObCompatibilityMode::ORACLE_MODE == compatibility_mode_;
  }
  inline bool is_mysql_tenant() const
  {
    return common::ObCompatibilityMode::MYSQL_MODE == compatibility_mode_;
  }
  inline void set_drop_tenant_time(const int64_t drop_tenant_time) { drop_tenant_time_ = drop_tenant_time; }
  inline int64_t get_drop_tenant_time() const { return drop_tenant_time_; }
  inline bool is_dropping() const { return TENANT_STATUS_DROPPING == status_; }
  inline bool is_in_recyclebin() const { return in_recyclebin_; }
  inline void set_in_recyclebin(const bool in_recyclebin) { in_recyclebin_ = in_recyclebin; }
  inline bool is_creating() const { return TENANT_STATUS_CREATING == status_; }
  inline bool is_restore() const { return TENANT_STATUS_RESTORE == status_
                                          || TENANT_STATUS_CREATING_STANDBY == status_; }
  inline bool is_normal() const { return TENANT_STATUS_NORMAL == status_; }
  inline bool is_restore_tenant_status() const { return TENANT_STATUS_RESTORE == status_; }
  inline bool is_creating_standby_tenant_status() const { return TENANT_STATUS_CREATING_STANDBY == status_; }
  inline void set_status(const ObTenantStatus status) { status_ = status; }
  inline ObTenantStatus get_status() const { return status_; }
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  void reset_physical_location_info();
  //standby no need sync alter tenant attribute, so reset those while create tenant
  void reset_alter_tenant_attributes();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version), K_(tenant_name), K_(zone_list),
               K_(primary_zone), K_(charset_type), K_(locked), K_(comment), K_(name_case_mode),
               K_(read_only), K_(locality_str),
               K_(zone_replica_attr_array), K_(primary_zone_array), K_(previous_locality_str),
               K_(default_tablegroup_id), K_(default_tablegroup_name), K_(compatibility_mode), K_(drop_tenant_time),
               K_(status), K_(in_recyclebin), K_(arbitration_service_status));
private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObString tenant_name_;
  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;
  bool locked_;
  bool read_only_;  // After the schema is split, the value of the system variable shall prevail
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  common::ObNameCaseMode name_case_mode_;  //deprecated
  common::ObString comment_;
  common::ObString locality_str_;
  ZoneLocalityArray zone_replica_attr_array_;
  // primary_zone_ is primary zone list,
  // The following is the parsed array of a single zone, which has been sorted according to priority
  ObPrimaryZoneArray primary_zone_array_;
  common::ObString previous_locality_str_;
  uint64_t default_tablegroup_id_;
  common::ObString default_tablegroup_name_;
  common::ObCompatibilityMode compatibility_mode_;//创建后不可修改
  int64_t drop_tenant_time_;
  ObTenantStatus status_;
  bool in_recyclebin_;
  ObArbitrationServiceStatus arbitration_service_status_;
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
/*
TODO: remove interfaces
int ObDatabaseSchema::get_first_primary_zone_inherit()
int ObDatabaseSchema::get_zone_list()
int ObDatabaseSchema::get_zone_replica_attr_array_inherit()
int ObDatabaseSchema::get_primary_zone_inherit()
int ObDatabaseSchema::get_paxos_replica_num()
*/
class ObDatabaseSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObDatabaseSchema();
  explicit ObDatabaseSchema(common::ObIAllocator *allocator);
  virtual ~ObDatabaseSchema();
  ObDatabaseSchema(const ObDatabaseSchema &src_schema);
  ObDatabaseSchema &operator=(const ObDatabaseSchema &src_schema);
  int assign(const ObDatabaseSchema &src_schema);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  int set_database_name(const char *database_name) { return deep_copy_str(database_name, database_name_); }
  int set_database_name(const common::ObString &database_name) { return deep_copy_str(database_name, database_name_); }
  int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) {collation_type_ = type; }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) {name_case_mode_ = mode; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  void set_default_tablegroup_id(const uint64_t tablegroup_id) { default_tablegroup_id_ = tablegroup_id; }
  int set_default_tablegroup_name(const common::ObString &tablegroup_name) { return deep_copy_str(tablegroup_name, default_tablegroup_name_); }
  inline void set_in_recyclebin(const bool in_recyclebin) { in_recyclebin_ = in_recyclebin; }
  inline bool is_hidden() const
  {
    return is_recyclebin_database_id(database_id_)
           || is_public_database_id(database_id_);
  }

  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_database_name() const { return extract_str(database_name_); }
  inline const common::ObString &get_database_name_str() const { return database_name_; }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline bool is_read_only() const { return read_only_; }
  inline uint64_t get_default_tablegroup_id() const { return default_tablegroup_id_; }
  inline const common::ObString &get_default_tablegroup_name() const { return default_tablegroup_name_; }
  inline bool is_in_recyclebin() const { return in_recyclebin_; }
  inline bool is_or_in_recyclebin() const
  { return in_recyclebin_ || is_recyclebin_database_id(database_id_); }
  int get_first_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
      common::ObZone &first_primary_zone) const;
  int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObPrimaryZone &primary_zone) const;
  int get_paxos_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const;
  int get_zone_replica_attr_array_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      ZoneLocalityIArray &locality) const;
  // In the current implementation, the zone_list of the Database is directly read from the corresponding tenant.
  int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObZone> &zone_list) const;
  //other methods
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual void reset();
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(schema_version), K_(database_name),
    K_(charset_type), K_(collation_type), K_(name_case_mode), K_(comment), K_(read_only),
    K_(default_tablegroup_id), K_(default_tablegroup_name), K_(in_recyclebin));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_version_;
  common::ObString database_name_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  common::ObNameCaseMode name_case_mode_;//default:OB_NAME_CASE_INVALID
  common::ObString comment_;
  bool read_only_;
  uint64_t default_tablegroup_id_;
  common::ObString default_tablegroup_name_;
  bool in_recyclebin_;
};

class ObPartitionOption : public ObSchema
{
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
  { return share::schema::is_range_part(part_func_type_); }
  inline bool is_interval_part() const
  { return share::schema::is_interval_part(part_func_type_); }
  inline bool is_hash_part() const
  { return share::schema::is_hash_part(part_func_type_); }
  inline bool is_hash_like_part() const
  { return share::schema::is_hash_like_part(part_func_type_); }
  inline bool is_key_part() const
  { return share::schema::is_key_part(part_func_type_); }
  inline bool is_list_part() const
  { return share::schema::is_list_part(part_func_type_); }
  inline bool is_auto_range_part() const
  {
    return auto_part_ && is_range_part();
  }

  //set methods
  int set_part_expr(const common::ObString &expr) { return deep_copy_str(expr, part_func_expr_); }
  inline void set_part_num(const int64_t part_num) { part_num_ = part_num; }
  inline void set_part_func_type(const ObPartitionFuncType func_type) { part_func_type_ = func_type; }
  inline void set_sub_part_func_type(const ObPartitionFuncType func_type) { part_func_type_ = func_type; }
  inline void set_auto_part(const bool auto_part) {
    auto_part_ = auto_part;
  }
  inline void set_auto_part_size(const int64_t auto_part_size) {
    auto_part_size_ = auto_part_size;
  }

  //get methods
  inline const common::ObString &get_part_func_expr_str() const { return part_func_expr_; }
  inline const char *get_part_func_expr() const { return extract_str(part_func_expr_); }
  inline int64_t get_part_num() const { return part_num_; }
  inline ObPartitionFuncType get_part_func_type() const { return part_func_type_; }
  inline ObPartitionFuncType get_sub_part_func_type() const { return part_func_type_; }
  const common::ObString &get_intervel_start_str() const { return interval_start_; }
  const common::ObString &get_part_intervel_str() const { return part_interval_; }
  inline int64_t get_auto_part_size() const {
    return auto_part_size_;
  }

  //other methods
  virtual void reset();
  void reuse();
  int64_t assign(const ObPartitionOption & src_part);
  int64_t get_convert_size() const ;
  virtual bool is_valid() const;
  TO_STRING_KV(K_(part_func_type), K_(part_func_expr), K_(part_num),
               K_(auto_part), K_(auto_part_size));
private:
  ObPartitionFuncType part_func_type_;
  common::ObString part_func_expr_;
  // When ObPartOption is ObSubPartOption, it means subpartition num in template subpartition definition.
  int64_t part_num_;
  common::ObString interval_start_; //interval start value
  common::ObString part_interval_; // interval partition step
  bool auto_part_;// Whether it is automatic partition
  int64_t auto_part_size_;// Automatic partition size, 0 is auto
};

// For any questions about the role of this structure, please contact @jiage
class ObSchemaAllocator : public common::ObIAllocator
{
public:
  ObSchemaAllocator()
    : allocator_(NULL)
  {}
  ObSchemaAllocator(common::ObIAllocator &allocator)
    : allocator_(&allocator)
  {}

  virtual void* alloc(const int64_t sz) override
  {
    return NULL == allocator_ ? NULL : allocator_->alloc(sz);
  }

  virtual void* alloc(const int64_t sz, const common::ObMemAttr &attr) override
  {
    return NULL == allocator_ ? NULL : allocator_->alloc(sz, attr);
  }

  virtual void free(void *p) override
  {
    if (allocator_) {
      allocator_->free(p);
      p = NULL;
    }
  }
  virtual ~ObSchemaAllocator() {};
private:
  common::ObIAllocator *allocator_;
};

// May need to add a schema_version field, this mark when the table was created
// If it is at the table level, need not to add it, just use the table level.
// This level is added. Indicates that the partition level is considered.
// Then it is to modify the subpartition of a single primary partition without affecting other things
struct InnerPartListVectorCmp
{
public:
  InnerPartListVectorCmp() : ret_(common::OB_SUCCESS) {}
public:
  bool operator()(const common::ObNewRow &left, const common::ObNewRow &right)
  {
    bool bool_ret = false;
    int cmp = 0;
    if (common::OB_SUCCESS != ret_) {
      // failed before
    } else if (common::OB_SUCCESS != (ret_ = common::ObRowUtil::compare_row(left, right, cmp))) {
      SHARE_SCHEMA_LOG_RET(ERROR, ret_, "l or r is invalid", K(ret_));
    } else {
      bool_ret = (cmp < 0);
    }
    return bool_ret;
  }
  int get_ret() const { return ret_; }
private:
  int ret_;
};

class IRelatedTabletMap
{
public:
  virtual int add_related_tablet_id(common::ObTabletID src_tablet_id,
                                    common::ObTableID related_table_id,
                                    common::ObTabletID related_tablet_id,
                                    common::ObObjectID related_part_id,
                                    common::ObObjectID related_first_level_part_id) = 0;
};

class ObSchemaGetterGuard;
struct RelatedTableInfo
{
  RelatedTableInfo()
    : related_map_(nullptr),
      related_tids_(nullptr),
      guard_(nullptr)
  { }
  inline bool is_valid() const
  {
    return OB_NOT_NULL(related_map_) && OB_NOT_NULL(related_tids_) && OB_NOT_NULL(guard_);
  }
  IRelatedTabletMap *related_map_;
  const common::ObIArrayWrap<common::ObTableID> *related_tids_;
  share::schema::ObSchemaGetterGuard *guard_;
};

struct PartitionIndex
{
public:
  PartitionIndex()
   : part_idx_(common::OB_INVALID_INDEX),
     subpart_idx_(common::OB_INVALID_INDEX) {}
  PartitionIndex(const int64_t part_idx,
                 const int64_t subpart_idx)
   : part_idx_(part_idx),
     subpart_idx_(subpart_idx) {}
  ~PartitionIndex() {}

  inline void init(const int64_t part_idx, const int64_t subpart_idx)
  {
    part_idx_ = part_idx;
    subpart_idx_ = subpart_idx;
  }
  inline void reset()
  {
    part_idx_ = common::OB_INVALID_INDEX;
    subpart_idx_ = common::OB_INVALID_INDEX;
  }
  inline bool is_valid() const { return part_idx_ >= 0; }
  uint64_t get_part_idx() const { return part_idx_; }
  uint64_t get_subpart_idx() const { return subpart_idx_; }
  TO_STRING_KV(K_(part_idx), K_(subpart_idx));
private:
  int64_t part_idx_;
  int64_t subpart_idx_;
};


class ObPartitionUtils;
class ObBasePartition : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  friend class ObPartitionUtils;
  friend class sql::ObPartitionExecutorUtils;
  ObBasePartition();
  explicit ObBasePartition(common::ObIAllocator *allocator);
  virtual void reset();
  void set_tenant_id(const uint64_t tenant_id)
  { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const
  { return tenant_id_; }

  void set_table_id(const uint64_t table_id)
  { table_id_ = table_id ; }
  uint64_t get_table_id() const
  { return table_id_; }

  void set_part_id(const int64_t part_id)
  { part_id_ = part_id; }
  int64_t get_part_id() const
  { return part_id_; }

  void set_tablet_id(const ObTabletID &tablet_id)
  { tablet_id_ = tablet_id; }
  void set_tablet_id(const int64_t tablet_id)
  { tablet_id_ = tablet_id; }
  ObTabletID get_tablet_id() const
  { return tablet_id_; }
  virtual ObObjectID get_object_id() const = 0;

  void set_part_idx(const int64_t part_idx)
  { part_idx_ = part_idx; }
  int64_t get_part_idx() const
  { return part_idx_; }

  void set_schema_version(int64_t schema_version)
  { schema_version_ = schema_version; }
  int64_t get_schema_version() const
  { return schema_version_; }

  int set_part_name(const common::ObString &part_name)
  { return deep_copy_str(part_name, name_); }
  const common::ObString &get_part_name() const
  { return name_; }

  void set_tablespace_id(const int64_t tablespace_id)
  { tablespace_id_ = tablespace_id; }
  int64_t get_tablespace_id() const
  { return tablespace_id_; }

  int assign(const ObBasePartition & src_part);

  // This interface is not strictly semantically less than, please note
  static bool less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool list_part_func_layout(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool range_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  static bool hash_like_func_less_than(const ObBasePartition *lhs, const ObBasePartition *rhs);
  int add_list_row(const common::ObNewRow &row) {
    return list_row_values_.push_back(row);
  }
  int set_low_bound_val(const common::ObRowkey &high_bound_val);
  const common::ObRowkey &get_low_bound_val() const
  { return low_bound_val_; }

  int set_high_bound_val(const common::ObRowkey &high_bound_val);
  int set_high_bound_val_with_hex_str(const common::ObString &high_bound_val_hex);
  int set_list_vector_values_with_hex_str(const common::ObString &list_vector_vals_hex);
  const common::ObRowkey &get_high_bound_val() const
  { return high_bound_val_; }
  virtual int64_t get_deep_copy_size() const;

  const common::ObIArray<common::ObNewRow>& get_list_row_values() const {
    return list_row_values_;
  }
  void reset_high_bound_val() { high_bound_val_.reset(); }
  void reset_list_row_values() { list_row_values_.reset(); }

  bool same_base_partition(const ObBasePartition &other) const
  {
    return part_idx_ == other.part_idx_
        && high_bound_val_ == other.high_bound_val_
        && status_ == other.status_;
  }
  ObPartitionStatus get_status() const { return status_; }
  void set_is_empty_partition_name(bool is_empty) { is_empty_partition_name_ = is_empty; }
  bool is_empty_partition_name() const { return is_empty_partition_name_; }

  void set_partition_type(const PartitionType &type) { partition_type_ = type; }
  PartitionType get_partition_type() const  { return partition_type_; }
  virtual bool is_normal_partition() const = 0;
  virtual bool is_hidden_partition() const { return share::schema::is_hidden_partition(partition_type_); }

  // convert character set.
  int convert_character_for_range_columns_part(const ObCollationType &to_collation);
  int convert_character_for_list_columns_part(const ObCollationType &to_collation);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(table_id), K_(part_id), K_(name), K_(low_bound_val),
                       K_(high_bound_val), K_(list_row_values), K_(part_idx),
                       K_(is_empty_partition_name), K_(tablet_id));
protected:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t part_id_;
  int64_t schema_version_;
  common::ObString name_;
  common::ObRowkey high_bound_val_;
  ObSchemaAllocator schema_allocator_;
  common::ObSEArray<common::ObNewRow, 2> list_row_values_;
  enum ObPartitionStatus status_;
  /**
   * @warning: The projector can only be used by the less than interface for partition comparison calculations,
   *  and it is not allowed to be used in other places
   */
  int32_t *projector_;
  int64_t projector_size_;
  int64_t part_idx_;
  // The partition management operation of tablegroup, because after adding pg,
  // the operation of tablegroup needs to be processed first
  // Under Oracle tenants, partition_name is allowed to be empty. There may be a partition name conflict
  // when filling the tablegroup first and directly copying it to the table. Therefore, add a variable
  // when copying to mark this as an empty partition name and do not copy the partition name.
  bool is_empty_partition_name_;
  int64_t tablespace_id_;
  PartitionType partition_type_;
  common::ObRowkey low_bound_val_;
  ObTabletID tablet_id_;
};

class ObSubPartition;
class ObPartition : public ObBasePartition
{
  OB_UNIS_VERSION(1);
public:
  ObPartition();
  explicit ObPartition(common::ObIAllocator *allocator);
  int assign(const ObPartition & src_part);

  void reset();
  virtual int64_t get_convert_size() const;
  virtual int clone(common::ObIAllocator &allocator, ObPartition *&dst) const;
  virtual ObObjectID get_object_id() const override { return static_cast<ObObjectID>(part_id_); }
  int get_max_sub_part_idx(int64_t &sub_part_idx) const;

  void set_sub_part_num(const int64_t sub_part_num) { sub_part_num_ = sub_part_num; }
  int64_t get_sub_part_num() const { return sub_part_num_; }
  common::ObObj get_sub_interval_start() const { return sub_interval_start_; }
  common::ObObj get_sub_part_interval() const { return sub_part_interval_; }
  virtual bool is_normal_partition() const override
  {
    return !is_hidden_partition();
  }
  ObSubPartition **get_subpart_array() const { return subpartition_array_; }
  int64_t get_subpartition_num() const { return subpartition_num_; }
  bool same_partition(const ObPartition &other) const
  {
    return same_base_partition(other)
        && sub_part_num_ == other.sub_part_num_
        && sub_interval_start_ == other.sub_interval_start_
        && sub_part_interval_ == other.sub_part_interval_;
  }
  int add_partition(const ObSubPartition &subpartition);
  ObSubPartition **get_hidden_subpart_array() const { return hidden_subpartition_array_; }
  int64_t get_hidden_subpartition_num() const { return hidden_subpartition_num_; }
  int preserve_subpartition(const int64_t &capacity);
  int get_normal_subpartition_by_subpartition_index(const int64_t subpartition_index,
                                                const ObSubPartition *&partition) const;
  int get_normal_subpartition_index_by_id(const int64_t subpart_id,
                                      int64_t &subpartition_index) const;

  INHERIT_TO_STRING_KV(
    "BasePartition", ObBasePartition,
    "subpartition_array",
    common::ObArrayWrap<ObSubPartition *>(subpartition_array_, subpartition_num_),
    K_(subpartition_array_capacity),
    "hidden_subpartition_array",
    common::ObArrayWrap<ObSubPartition *>(hidden_subpartition_array_, hidden_subpartition_num_),
    K_(hidden_subpartition_array_capacity));
private:
  int inner_add_partition(
      const ObSubPartition &part,
      ObSubPartition **&part_array,
      int64_t &part_array_capacity,
      int64_t &part_num);
  int deserialize_subpartition_array(const char *buf, const int64_t data_len, int64_t &pos);
protected:
  static const int64_t DEFAULT_ARRAY_CAPACITY = 128;
private:
  int64_t sub_part_num_;
  common::ObObj sub_interval_start_;
  common::ObObj sub_part_interval_;
  int64_t subpartition_num_;
  int64_t subpartition_array_capacity_;
  ObSubPartition **subpartition_array_;
  /* hidden subpartition */
  int64_t hidden_subpartition_num_;
  int64_t hidden_subpartition_array_capacity_;
  ObSubPartition **hidden_subpartition_array_;
};

class ObSubPartition : public ObBasePartition
{
  OB_UNIS_VERSION(1);
public:
  // For template, because it does not belong to any part, we set part_id to -1
  static const int64_t TEMPLATE_PART_ID = -1;
public:
  ObSubPartition();
  explicit ObSubPartition(common::ObIAllocator *allocator);
  int assign(const ObSubPartition & src_part);
  virtual void reset();
  virtual int clone(common::ObIAllocator &allocator, ObSubPartition *&dst) const;

  virtual ObObjectID get_object_id() const override { return static_cast<ObObjectID>(subpart_id_); }

  void set_sub_part_id(const int64_t subpart_id)
  { subpart_id_ = subpart_id; }
  int64_t get_sub_part_id() const
  { return subpart_id_; }
  void set_sub_part_idx(const int64_t subpart_idx)
  { subpart_idx_ = subpart_idx; }
  int64_t get_sub_part_idx() const
  { return subpart_idx_; }
  virtual bool is_normal_partition() const override
  {
    return !is_hidden_partition();
  }
  //first compare part_idx,then compare high_bound_val
  static bool less_than(const ObSubPartition *lhs, const ObSubPartition *rhs);
  bool same_sub_partition(const ObSubPartition &other) const
  {
    return same_base_partition(other) && subpart_idx_ == other.subpart_idx_;
  }
  virtual int64_t get_convert_size() const;
  bool key_match(const ObSubPartition &other) const;
  static bool hash_like_func_less_than(const ObSubPartition *lhs, const ObSubPartition *rhs);
  INHERIT_TO_STRING_KV("BasePartition", ObBasePartition,
                  K_(subpart_id),
                  K_(subpart_idx));
private:
  int64_t subpart_id_;
  int64_t subpart_idx_;
};

class ObPartitionSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  constexpr const static char * const MYSQL_NON_PARTITIONED_TABLE_PART_NAME = "p0";
  constexpr const static char * const ORACLE_NON_PARTITIONED_TABLE_PART_NAME = "P0";

  const static int64_t SUBPART_TEMPLATE_DEF_EXIST_SHIFT = 0;
  const static int64_t SUBPART_TEMPLATE_DEF_VALID_SHIFT = 1;
  const static int64_t SUBPART_TEMPLATE_DEF_EXIST_MASK = 0x1 << SUBPART_TEMPLATE_DEF_EXIST_SHIFT;
  const static int64_t SUBPART_TEMPLATE_DEF_VALID_MASK = 0x1 << SUBPART_TEMPLATE_DEF_VALID_SHIFT;


public:
  //base methods
  ObPartitionSchema();
  explicit ObPartitionSchema(common::ObIAllocator *allocator);
  virtual ~ObPartitionSchema();
  ObPartitionSchema(const ObPartitionSchema &src_schema);
  ObPartitionSchema &operator=(const ObPartitionSchema &src_schema);
  void reuse_partition_schema();
  int assign_partition_schema(const ObPartitionSchema &src_schema);
  //partition related

  virtual const char *get_entity_name() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual void set_tenant_id(const uint64_t tenant_id) = 0;
  virtual uint64_t get_table_id() const = 0;
  virtual void set_table_id(const uint64_t table_id) = 0;
  virtual ObObjectID get_object_id() const = 0;
  virtual ObTabletID get_tablet_id() const = 0;
  virtual bool has_tablet() const = 0;
  virtual int64_t get_schema_version() const = 0;
  virtual void set_schema_version(const int64_t schema_version) = 0;
  virtual bool is_user_partition_table() const = 0;
  virtual bool is_user_subpartition_table() const = 0;
  virtual ObPartitionLevel get_part_level() const { return part_level_; }
  virtual bool has_self_partition() const = 0;
  virtual int get_primary_zone_inherit(
      ObSchemaGetterGuard &schema_guard,
      ObPrimaryZone &primary_zone) const = 0;
  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &schema_guard,
      ZoneLocalityIArray &locality) const = 0;
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObZone> &zone_list) const = 0;
  virtual int get_locality_str_inherit(
      share::schema::ObSchemaGetterGuard &guard,
      const common::ObString *&locality_str) const = 0;
  virtual int get_first_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
      common::ObZone &first_primary_zone) const = 0;
  virtual int check_is_duplicated(
      share::schema::ObSchemaGetterGuard &guard,
      bool &is_duplicated) const = 0;
  virtual uint64_t get_tablegroup_id() const = 0;
  virtual void set_tablegroup_id(const uint64_t tg_id) = 0;
  virtual int get_paxos_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const = 0;
  virtual share::ObDuplicateScope get_duplicate_scope() const = 0;
  virtual void set_duplicate_scope(const share::ObDuplicateScope duplicate_scope) = 0;
  virtual void set_duplicate_scope(const int64_t duplicate_scope) = 0;
  inline virtual int64_t get_part_func_expr_num() const { return 0; }
  inline virtual void set_part_func_expr_num(const int64_t part_func_expr_num) { UNUSED(part_func_expr_num); }
  inline virtual int64_t get_sub_part_func_expr_num() const { return 0; }
  inline virtual void set_sub_part_func_expr_num(const int64_t sub_part_func_expr_num) { UNUSED(sub_part_func_expr_num); }

  inline void set_part_num(const int64_t part_num)
  {
    part_option_.set_part_num(part_num);
  }
  // Only the templated secondary partition is valid, pay attention!!
  inline void set_def_sub_part_num(const int64_t def_subpart_num)
  {
    sub_part_option_.set_part_num(def_subpart_num);
  }
  inline void set_part_level(const ObPartitionLevel part_level) { part_level_ = part_level; }

  inline bool has_sub_part_template_def() const
  {
    return PARTITION_LEVEL_TWO == part_level_
           && sub_part_template_flags_ > 0;
  }
  inline bool sub_part_template_def_valid() const
  {
    return PARTITION_LEVEL_TWO == part_level_
           && sub_part_template_flag_exist(SUBPART_TEMPLATE_DEF_VALID_MASK);
  }
  inline bool sub_part_template_def_exist() const
  {
    return PARTITION_LEVEL_TWO == part_level_
           && sub_part_template_flag_exist(SUBPART_TEMPLATE_DEF_EXIST_MASK);
  }
  inline void set_sub_part_template_def_valid()
  {
    add_sub_part_template_flag(SUBPART_TEMPLATE_DEF_EXIST_MASK);
    add_sub_part_template_flag(SUBPART_TEMPLATE_DEF_VALID_MASK);
  }
  inline void unset_sub_part_template_def_valid()
  {
    del_sub_part_template_flag(SUBPART_TEMPLATE_DEF_VALID_MASK);
  }
  inline int64_t get_sub_part_template_flags() const
  {
    return sub_part_template_flags_;
  }
  inline void set_sub_part_template_flags(int64_t sub_part_template_flags)
  {
    sub_part_template_flags_ = sub_part_template_flags;
  }
  inline int64_t get_first_part_num() const { return part_option_.get_part_num(); }
  // Only the templated secondary partition is valid, pay attention!!
  int64_t get_def_sub_part_num() const;
  int64_t get_all_part_num() const;
  int64_t get_first_part_num(const ObCheckPartitionMode check_partition_mode) const;
  int get_all_partition_num(const ObCheckPartitionMode check_partition_mode, int64_t &part_num) const;

  int check_part_name(const ObPartition &partition);
  int add_partition(const ObPartition &partition);
  int add_def_subpartition(const ObSubPartition &subpartition);

  inline bool is_hash_part() const { return part_option_.is_hash_part(); }
  inline bool is_hash_subpart() const { return sub_part_option_.is_hash_part(); }
  inline bool is_key_part() const { return part_option_.is_key_part(); }
  inline bool is_key_subpart() const { return sub_part_option_.is_key_part(); }
  inline bool is_range_part() const { return part_option_.is_range_part(); }
  inline bool is_interval_part() const { return part_option_.is_interval_part(); }
  inline bool is_range_subpart() const { return sub_part_option_.is_range_part(); }

  inline bool is_hash_like_part() const { return part_option_.is_hash_like_part(); }
  inline bool is_hash_like_subpart() const { return sub_part_option_.is_hash_like_part(); }
  inline bool is_list_part() const { return part_option_.is_list_part(); }
  inline bool is_list_subpart() const { return sub_part_option_.is_list_part(); }

  inline bool is_auto_partitioned_table() const { return part_option_.is_auto_range_part();}

  inline const ObPartitionOption &get_part_option() const { return part_option_; }
  inline ObPartitionOption &get_part_option() { return part_option_; }
  inline const ObPartitionOption &get_sub_part_option() const { return sub_part_option_; }
  inline ObPartitionOption &get_sub_part_option() { return sub_part_option_; }
  inline int64_t get_auto_part_size() const { return part_option_.get_auto_part_size(); }

  // deal with partition schema from ddl resolver
  int try_generate_hash_part();
  int try_generate_hash_subpart(bool &generated);
  int try_generate_subpart_by_template(bool &generated);
  int try_init_partition_idx();

  int serialize_partitions(char *buf, const int64_t data_len, int64_t &pos) const;
  int serialize_def_subpartitions(char *buf, const int64_t data_len, int64_t &pos) const;
  int deserialize_partitions(const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_def_subpartitions(const char *buf, const int64_t data_len, int64_t &pos);

  int get_partition_by_part_id(const int64_t part_id,
                               const ObCheckPartitionMode check_partition_mode,
                               const ObPartition *&partition) const;

  // for non-partitioned table
  int get_tablet_and_object_id(
      common::ObTabletID &tablet_id,
      common::ObObjectID &object_id) const;

  /**
   * first_level_part_id represent the first level part id of subpartition,
   * otherwise its value is OB_INVALID_ID
   * e.g.
   *  PARTITION_LEVEL_ZERO
   *    - object_id = table_id
   *    - first_level_part_id = OB_INVALID_ID
   *  PARTITION_LEVEL_ONE
   *    - object_id = part_id
   *    - first_level_part_id = OB_INVALID_ID
   * PARTITION_LEVEL_TWO
   *    - object_id = sub_part_id
   *    - first_level_part_id = part_id
  */
  int get_tablet_and_object_id_by_index(
      const int64_t part_idx,
      const int64_t subpart_idx,
      ObTabletID &tablet_id,
      ObObjectID &object_id,
      ObObjectID &first_level_part_id) const;

  /** generate part name of hash partition, for resolver only
   * @name_type: Type of generated hash partition name:
   *                                FIRST_PART generate partition name
   *                                TEMPLATE_SUB_PART generate subpartition name
   *                                INDIVIDUAL_SUB_PART generate subpartition name of nontemplate
   * @need_upper_case: Does the generated partition name need to be capitalized?
   * @partition: nantemplate table, the partition name needs to be spelled with the partition name,
   *    and partition indicates the partition where the current subpartition is located
   */
  static int gen_hash_part_name(const int64_t part_idx,
                                const ObHashNameType name_type,
                                const bool need_upper_case,
                                char* buf,
                                const int64_t buf_size,
                                int64_t *pos,
                                const ObPartition *partition = NULL);
  inline ObPartition **get_part_array() const { return partition_array_; }
  inline int64_t get_partition_capacity() const { return partition_array_capacity_; }
  inline int64_t get_partition_num() const { return partition_num_; }
  // The following interfaces can only be used for templated subpartition tables
  inline ObSubPartition **get_def_subpart_array() const { return def_subpartition_array_; }
  inline int64_t get_def_subpartition_capacity() const { return def_subpartition_array_capacity_; }
  inline int64_t get_def_subpartition_num() const { return def_subpartition_num_; }
  /* ----------------------------------*/
  inline void set_partition_schema_version(const int64_t schema_version) { partition_schema_version_ = schema_version; }
  inline int64_t get_partition_schema_version() const { return partition_schema_version_; }
  virtual bool is_hidden_schema() const = 0;
  virtual bool is_normal_schema() const = 0;

  void reset_def_subpartition() {
    def_subpartition_num_ = 0;
    def_subpartition_array_capacity_ = 0;
    def_subpartition_array_ = NULL;
  }
  // The partition information needs to be reorganized when truncate the subpartition
  void reset_partition_array() {
    partition_array_capacity_ = 0;
    partition_num_ = 0;
    partition_array_ = NULL;
  }

  int64_t get_hidden_partition_num() const { return hidden_partition_num_; }
  ObPartition **get_hidden_part_array() const { return hidden_partition_array_; }
  //----------------------
  // Get the offset of the partition in partition_array, just take the offset of the partition,
  // not including the subpartition
  int get_partition_index_by_id(const int64_t part_id,
                                const ObCheckPartitionMode check_partition_mode,
                                int64_t &partition_index) const;

  int get_partition_by_partition_index(
      const int64_t partition_index,
      const ObCheckPartitionMode check_partition_mode,
      const share::schema::ObPartition *&partition) const;

  inline void set_partition_status(const ObPartitionStatus partition_status) { partition_status_ = partition_status; }
  inline ObPartitionStatus get_partition_status() const { return partition_status_; }
  bool is_in_splitting() const { return  partition_status_ == PARTITION_STATUS_LOGICAL_SPLITTING
                                         || partition_status_ == PARTITION_STATUS_PHYSICAL_SPLITTING; }
  bool is_in_logical_split () const { return partition_status_ == PARTITION_STATUS_LOGICAL_SPLITTING; }
  bool is_in_physical_split() const { return partition_status_ == PARTITION_STATUS_PHYSICAL_SPLITTING; }
  //other methods
  virtual void reset();
  virtual bool is_valid() const;
  DECLARE_VIRTUAL_TO_STRING;
  int try_assign_part_array(const share::schema::ObPartitionSchema &that);
  int try_assign_def_subpart_array(const share::schema::ObPartitionSchema &that);

  int set_transition_point(const common::ObRowkey &transition_point);
  int get_transition_point_str(char *buf, const int64_t buf_size, int64_t &len);
  int set_transition_point_with_hex_str(const common::ObString &transition_point_hex);
  const common::ObRowkey &get_transition_point() const
  { return transition_point_; }

  int set_interval_range(const common::ObRowkey &interval_range);
  int get_interval_range_str(char *buf, const int64_t buf_size, int64_t &len);
  int set_interval_range_with_hex_str(const common::ObString &interval_range_hex);
  const common::ObRowkey &get_interval_range() const
  { return interval_range_; }

  // for interval partitioned table, calc range partition number
  // note interval parted table, range part number is not same as all part number, because table
  // may have other interval partitions.
  int get_interval_parted_range_part_num(uint64_t &part_num) const;

  virtual int check_if_oracle_compat_mode(bool &is_oracle_mode) const = 0;
  // only used for virtual table
  int mock_list_partition_array();
  // only used for generate part_name
  int get_max_part_id(int64_t &part_id) const;
  int get_max_part_idx(int64_t &part_idx) const;
  //@param[in] name: the partition name which you want to get partition by
  //@param[out] part: the partition get by the name, when this function could not find the partition
  //            by the name, this param would be nullptr
  //@param[ret] when this function traversal all the partition but could not find the partition by name,
  //            the ret would be OB_UNKNOWN_PARTITION.
  //note this function would only check partition
  int get_partition_by_name(const ObString &name, const ObPartition *&part) const;
  //@param[in] name: the subpartition name which you want to get subpartition by
  //@param[out] part: the partition that the subpartition get by the name belongs to, when this function could not
  //            find the subpartition by the name, this param would be nullptr
  //            subpart: the subpartition get by the name, when this function could not find the subpartition
  //            by the name, this param would be nullptr
  //@param[ret] when this function traversal all the subpartition but could not find the subpartition by name,
  //            the ret would be OB_UNKNOWN_SUBPARTITION.
  //note this function would only check subpartition
  int get_subpartition_by_name(const ObString &name, const ObPartition *&part, const ObSubPartition *&subpart) const;
  //@param[in] name: the partition or subpartition name you want to check
  //           whether there is already a partition or subpartition have the same name
  //@param[ret] when this function traversal all partitions and subpartitions and find the name is duplicate with
  //            existed (sub)partition it will return OB_DUPLICATE_OBJECT_EXIST
  //note this function would check both partitions and subpartitions
  int check_partition_duplicate_with_name(const ObString &name) const;

protected:
  int inner_add_partition(const ObPartition &part);
  template<class T>
  int inner_add_partition(const T &part, T **&part_array,
                          int64_t &part_array_capacity,
                          int64_t &part_num);
  int get_subpart_info(
      const int64_t part_id,
      ObSubPartition **&subpart_array,
      int64_t &subpart_num,
      int64_t &subpartition_num) const;

  // Iterate all the partitions to get the offset
  int get_partition_index_loop(const int64_t part_id,
                               const ObCheckPartitionMode check_partition_mode,
                               int64_t &partition_index) const;

  inline void add_sub_part_template_flag(const int64_t flag)
  {
    sub_part_template_flags_ = sub_part_template_flags_ | flag;
  }
  inline void del_sub_part_template_flag(const int64_t flag)
  {
    sub_part_template_flags_ = sub_part_template_flags_ & (~flag);
  }
  inline bool sub_part_template_flag_exist(const int64_t flag) const
  {
    return (sub_part_template_flags_ & flag) > 0;
  }
protected:
  static const int64_t DEFAULT_ARRAY_CAPACITY = 128;
protected:
  ObPartitionLevel part_level_;
  ObPartitionOption part_option_;
  // The part_num_ is only valid when has_sub_part_template_def()
  ObPartitionOption sub_part_option_;
  ObPartition **partition_array_;
  int64_t partition_array_capacity_; // The array size of partition_array is not necessarily equal to partition_num;
  // Equal to part_num; historical reasons cause the two values to have the same meaning;
  // need to be modified together with part_num_
  int64_t partition_num_;
  /* template table, only used for ddl or schema printer */
  ObSubPartition **def_subpartition_array_;
  // The array size of subpartition_array, not necessarily equal to subpartition_num
  int64_t def_subpartition_array_capacity_;
  int64_t def_subpartition_num_; // equal subpart_num
  /* template subpartition define end*/
  // Record the split schema, initialized to 0, not cleared after splitting, the bottom layer needs to be used
  int64_t partition_schema_version_;
  ObPartitionStatus partition_status_;
  /*
   * Here is the different values for sub_part_template_flags_:
   * 1) 0: sub_part_template is not defined.
   * 2) sub_part_template_def_valid(exist & valid):
   *     - if sub_part_template id defined and partition related ddl(except truncate) is executed.
   *     - only used for schema_printer
   * 3) sub_part_template_def_exist(exist):
   *     - sub_part_template_def maybe not valid after partition related ddl is executed.
   *
   * For now, set/unset sub_part_template_def is not supported yet.
   */
  int64_t sub_part_template_flags_;
  /* hidden partition */
  ObPartition **hidden_partition_array_;
  int64_t hidden_partition_array_capacity_;
  int64_t hidden_partition_num_;
  common::ObRowkey transition_point_;
  common::ObRowkey interval_range_;
};
/*TODO: Delete the following interfaces in ObTablegroupSchema and ObDatabaseSchema
int ObTablegroupSchema::get_first_primary_zone_inherit()
int ObTablegroupSchema::get_zone_list()
int ObTablegroupSchema::get_zone_replica_attr_array_inherit()
int ObTablegroupSchema::get_locality_str_inherit()
int ObTablegroupSchema::get_primary_zone_inherit()
int ObTablegroupSchema::check_is_readonly_at_all()
int ObTablegroupSchema::check_is_readonly_at_all()
int ObTablegroupSchema::get_full_replica_num()
int ObTablegroupSchema::get_paxos_replica_num()
int ObTablegroupSchema::get_all_replica_num()
*/
class ObTablegroupSchema : public ObPartitionSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObTablegroupSchema();
  explicit ObTablegroupSchema(common::ObIAllocator *allocator);
  virtual ~ObTablegroupSchema();
  ObTablegroupSchema(const ObTablegroupSchema &src_schema);
  ObTablegroupSchema &operator=(const ObTablegroupSchema &src_schema);
  int assign(const ObTablegroupSchema &src_schema);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id) override { tenant_id_ = tenant_id; }
  inline void set_schema_version(const int64_t schema_version) override { schema_version_ = schema_version; }
  virtual void set_tablegroup_id(const uint64_t tablegroup_id) override { tablegroup_id_ = tablegroup_id; }
  inline int set_tablegroup_name(const char *name) { return deep_copy_str(name, tablegroup_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_tablegroup_name(const common::ObString &name) { return deep_copy_str(name, tablegroup_name_); }
  inline int set_table_name(const common::ObString &name) { return deep_copy_str(name, tablegroup_name_); }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline int set_sharding(const common::ObString &sharding) { return deep_copy_str(sharding, sharding_); }

  inline int set_split_partition(const common::ObString &split_partition) { return deep_copy_str(split_partition, split_partition_name_); }
  inline int set_split_rowkey(const common::ObRowkey &rowkey)
  { return rowkey.deep_copy(split_high_bound_val_, *get_allocator()); }
  inline int set_split_list_value(common::ObRowkey &list_values) {
    return list_values.deep_copy(split_list_row_values_, *get_allocator());
  }
  //get methods
  inline uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual inline int64_t get_schema_version() const override { return schema_version_; }
  virtual uint64_t get_tablegroup_id() const override { return tablegroup_id_; }
  inline const char *get_tablegroup_name_str() const { return extract_str(tablegroup_name_); }
  inline const char *get_comment() const { return  extract_str(comment_); }
  inline const common::ObString &get_sharding() const { return sharding_; }
  inline const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
  inline const common::ObString &get_table_name() const { return tablegroup_name_; }
  virtual const char *get_entity_name() const override { return extract_str(tablegroup_name_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline const common::ObString &get_split_partition_name() const { return split_partition_name_; }
  inline const common::ObRowkey &get_split_rowkey() const { return split_high_bound_val_; }
  inline const common::ObRowkey& get_split_list_row_values() const {
    return split_list_row_values_;
  }

  // In the current implementation, if the locality of the zone_list of the tablegroup is empty,
  // it will be read from the tenant, otherwise it will be parsed from the locality
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObZone> &zone_list) const override;

  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &schema_guard,
      ZoneLocalityIArray &locality) const override;
  virtual int get_locality_str_inherit(
      ObSchemaGetterGuard &schema_guard,
      const common::ObString *&locality_str) const override;
  virtual int get_first_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
      common::ObZone &first_primary_zone) const override;
  virtual int check_is_duplicated(
      share::schema::ObSchemaGetterGuard &guard,
      bool &is_duplicated) const override;
  virtual int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObPrimaryZone &primary_zone) const override;
  int check_is_readonly_at_all(
      share::schema::ObSchemaGetterGuard &guard,
      const common::ObZone &zone,
      const common::ObRegion &region,
      bool &readonly_at_all) const;
  int get_full_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const;
  virtual int get_paxos_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const override;
  int get_all_replica_num(
      share::schema::ObSchemaGetterGuard &schema_guard,
      int64_t &num) const;
  //partition related
  virtual share::ObDuplicateScope get_duplicate_scope() const override { return share::ObDuplicateScope::DUPLICATE_SCOPE_NONE; }
  virtual void set_duplicate_scope(const share::ObDuplicateScope duplicate_scope) override { UNUSED(duplicate_scope); }
  virtual void set_duplicate_scope(const int64_t duplicate_scope) override { UNUSED(duplicate_scope); }
  inline virtual bool is_user_partition_table() const override
  {
    return PARTITION_LEVEL_ONE == get_part_level()
           || PARTITION_LEVEL_TWO == get_part_level();
  }
  inline virtual bool is_user_subpartition_table() const override
  {
    return PARTITION_LEVEL_TWO == get_part_level();
  }
  inline virtual uint64_t get_table_id() const override { return tablegroup_id_; } // for partition schema used
  virtual ObObjectID get_object_id() const override;
  virtual ObTabletID get_tablet_id() const override;
  virtual bool has_tablet() const override { return false; }
  inline virtual uint64_t get_database_id() const { return 0; }
  inline virtual void set_database_id(const uint64_t database_id) { UNUSED(database_id); }
  inline virtual void set_table_id(const uint64_t tablegroup_id) override { tablegroup_id_ = tablegroup_id; }
  inline int64_t get_part_func_expr_num() const { return part_func_expr_num_; }
  inline void set_part_func_expr_num(const int64_t part_func_expr_num) { part_func_expr_num_ = part_func_expr_num; }
  inline int64_t get_sub_part_func_expr_num() const { return sub_part_func_expr_num_; }
  inline void set_sub_part_func_expr_num(const int64_t sub_part_func_expr_num) { sub_part_func_expr_num_ = sub_part_func_expr_num; }
  virtual int calc_part_func_expr_num(int64_t &part_func_expr_num) const;
  virtual int calc_subpart_func_expr_num(int64_t &subpart_func_expr_num) const;
  //other methods
  virtual void reset();
  int64_t get_convert_size() const;
  virtual bool has_self_partition() const override { return false; }
  virtual bool is_valid() const;
  bool is_global_index_table() const { return false; }
  bool can_read_index() const { return true; }
  virtual bool is_hidden_schema() const override { return false; }
  virtual bool is_normal_schema() const override { return !is_hidden_schema(); }
  virtual int check_if_oracle_compat_mode(bool &is_oracle_mode) const;
  inline int64_t get_truncate_version() { return 0; }

  DECLARE_VIRTUAL_TO_STRING;
private:
  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
  int64_t schema_version_;
  common::ObString tablegroup_name_;
  common::ObString comment_;
  common::ObString sharding_;
  //2.0 add
  int64_t part_func_expr_num_;
  int64_t sub_part_func_expr_num_;
  common::ObString split_partition_name_;
  common::ObRowkey split_high_bound_val_;
  common::ObRowkey split_list_row_values_;
};

class ObPartitionUtils
{
public:
  // According to the given hash value val and partition number part_num,
  // distinguish between oracle and mysql modes to calculate which partition this fold falls on
  // This interface is called at get_hash_part_idxs, get_hash_subpart_ids, etc.
  static int calc_hash_part_idx(const uint64_t val,
                                const int64_t part_num,
                                int64_t &partition_idx);

  //Convert rowkey to sql literal for show
  static int convert_rowkey_to_sql_literal(
             const bool is_oracle_mode,
             const common::ObRowkey &rowkey,
             char *buf,
             const int64_t buf_len,
             int64_t &pos,
             bool print_collation,
             const common::ObTimeZoneInfo *tz_info);

  // Used to display the defined value of the LIST partition
  static int convert_rows_to_sql_literal(
             const bool is_oracle_mode,
             const common::ObIArray<common::ObNewRow>& rows,
             char *buf,
             const int64_t buf_len,
             int64_t &pos,
             bool print_collation,
             const common::ObTimeZoneInfo *tz_info);

  //Convert rowkey's serialize buff to hex.For record all rowkey info.
  static int convert_rowkey_to_hex(const common::ObRowkey &rowkey,
                                   char *buf,
                                   const int64_t buf_len,
                                   int64_t &pos);

  static int convert_rows_to_hex(const common::ObIArray<common::ObNewRow>& rows,
                                 char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos);

  // check if partition value equal
  template <typename PARTITION>
  static int check_partition_value(
             const bool is_oracle_mode,
             const PARTITION &l_part,
             const PARTITION &r_part,
             const ObPartitionFuncType part_type,
             bool &is_equal,
             ObSqlString *user_error = NULL);

  static bool is_types_equal_for_partition_check(
              const bool is_oracle_mode,
              const common::ObObjType &typ1,
              const common::ObObjType &type2);

  static int set_low_bound_val_by_interval_range_by_innersql(
      const bool is_oracle_mode,
      ObPartition &p,
      const ObRowkey &interval_range);

  static int check_interval_partition_table(const ObRowkey &transition_point,
                                            const ObRowkey &interval_range);

  /* --- calc tablet_ids/part_ids/sub_part_ids by partition columns --- */

  // for non-partitioned table
  // param[@in]:
  // - table_schema: should be data table/local index/global_index
  // - guard: related_table and guard should be both null or not.
  // - related_table: related_tids_ can be the following possbilities:
  //                  1. data table: if table_schema is local index.
  //                  2. local indexes: if table_schema is data schema.
  // paramp[@out]:
  // - tablet_id: is valid if return success
  // - object_id: is valid if return success
  // - related_table: related_map_ will be set if related_tids_ is not empty.
  static int get_tablet_and_object_id(
         const share::schema::ObTableSchema &table_schema,
         common::ObTabletID &tablet_id,
         common::ObObjectID &object_id,
         RelatedTableInfo *related_table = NULL);

  // for partitioned table
  // param[@in]:
  // - range: when range is not single key, all part ids in partition_array will be returned
  //          if partitioned table is hash_like_part() or list_part().
  // - table_schema: should be data table/local index/global_index
  // - guard: related_table and guard should be both null or not.
  // - related_table: related_tids_ can be the following possbilities:
  //                  1. data table: if table_schema is local index.
  //                  2. local indexes: if table_schema is data schema.
  // param[@out]:
  // - tablet_ids: tablet_ids is empty if table is secondary-partitioned table.
  //               Otherwise, it's one-to-one correspondence with between tablet_ids and part_ids.
  // - part_ids: object ids for partitions.
  // - related_table:
  //   1. related_map_ will be set if related_tids_ is not empty.
  //   2. If dealing with first part in composited-partitioned table, related_map_ will be empty.
  static int get_tablet_and_part_id(
      const share::schema::ObTableSchema &table_schema,
      const common::ObNewRange &range,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<common::ObObjectID> &part_ids,
      RelatedTableInfo *related_table = NULL);

  // for partitioned table
  // param[@in]:
  // - row
  // - table_schema: should be data table/local index/global_index
  // - guard: related_table and guard should be both null or not.
  // - related_table: related_tids_ can be the following possbilities:
  //                  1. data table: if table_schema is local index.
  //                  2. local indexes: if table_schema is data schema.
  // param[@out]:
  // - tablet_id: tablet_id is invalid if table is secondary-partitioned table.
  // - part_id: object id for partition.
  // - related_table:
  //   1. related_map_ will be set if related_tids_ is not empty.
  //   2. If dealing with first part in composited-partitioned table, related_map_ will be empty.
  static int get_tablet_and_part_id(
      const share::schema::ObTableSchema &table_schema,
      const common::ObNewRow &row,
      common::ObTabletID &tablet_id,
      common::ObObjectID &part_id,
      RelatedTableInfo *related_table = NULL);

  // for secondary-partitioned table
  // param[@in]:
  // - part_id: object_id for partition, error will occur if first part doesn't exist.
  // - range: when range is not single key, all subpart ids in subpartition_array will be returned
  //          if secondary-partitioned table is hash_like_subpart() or list_subpart().
  // - table_schema: should be data table/local index/global_index
  // - guard: related_table and guard should be both null or not.
  // - related_table: related_tids_ can be the following possbilities:
  //                  1. data table: if table_schema is local index.
  //                  2. local indexes: if table_schema is data schema.
  // param[@out]:
  // - tablet_ids: It's one-to-one correspondence with between tablet_ids and subpart_ids.
  // - subpart_ids: object ids for subpartitions.
  // - related_table:
  //   1. related_map_ will be set if related_tids_ is not empty.
  //   2. If dealing with first part in composited-partitioned table, related_map_ will be empty.
  static int get_tablet_and_subpart_id(
      const share::schema::ObTableSchema &table_schema,
      const common::ObPartID &part_id,
      const common::ObNewRange &range,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<common::ObObjectID> &subpart_ids,
      RelatedTableInfo *related_table = NULL);

  // for secondary-partitioned table
  // param[@in]:
  // - row
  // - part_id: object_id for partition, error will occur if first part doesn't exist.
  // - table_schema: should be data table/local index/global_index
  // - guard: related_table and guard should be both null or not.
  // - related_table: related_tids_ can be the following possbilities:
  //                  1. data table: if table_schema is local index.
  //                  2. local indexes: if table_schema is data schema.
  // param[@out]:
  // - tablet_id: tablet_id is correspond with subpart_id.
  // - subpart_id: object id for subpartition.
  // - related_table:
  //   1. related_map_ will be set if related_tids_ is not empty.
  //   2. If dealing with first part in composited-partitioned table, related_map_ will be empty.
  static int get_tablet_and_subpart_id(
      const share::schema::ObTableSchema &table_schema,
      const common::ObPartID &part_id,
      const common::ObNewRow &row,
      common::ObTabletID &tablet_id,
      common::ObObjectID &subpart_id,
      RelatedTableInfo *related_table = NULL);

  /* ----------------------------------------------------------------- */

private:

  /* --- Calc tablet_id and part_id/subpart_id by partition_colums --- */
  static int check_param_valid_(
         const share::schema::ObTableSchema &table_schema,
         RelatedTableInfo *related_table);

  static int fill_tablet_and_object_ids_(
      const bool fill_tablet_id,
      const int64_t part_idx,
      const common::ObIArray<PartitionIndex> &partition_indexes,
      const share::schema::ObTableSchema &table_schema,
      RelatedTableInfo *related_table,
      common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<common::ObObjectID> &object_ids);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, empty tablet_ids may be returned.
  // - range: if range is not single key, all part_ids/tablet_ids in partition_array may be returned.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_hash_tablet_and_part_id_(
      const common::ObNewRange &range,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, empty tablet_ids may be returned.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_range_tablet_and_part_id_(
      const common::ObNewRange &range,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, empty tablet_ids may be returned.
  // - range: if range is not single key, all part_ids/tablet_ids in partition_array may be returned.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_list_tablet_and_part_id_(
      const common::ObNewRange &range,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, invalid tablet_id.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_hash_tablet_and_part_id_(
      const common::ObNewRow &row,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, invalid tablet_id.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_range_tablet_and_part_id_(
      const common::ObNewRow &row,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - fill_tablet_id: if fill_tablet_id is false, invalid tablet_id.
  //
  // param[@out]:
  // - indexes: partition offset in partition_array
  static int get_list_tablet_and_part_id_(
      const common::ObNewRow &row,
      ObPartition * const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  // - range: if range is not single key, all subpart_ids/tablet_ids in subpartition_array may be returned.
  //
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_hash_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRange &range,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  //
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_range_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRange &range,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  // - range: if range is not single key, all subpart_ids/tablet_ids in subpartition_array may be returned.
  //
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_list_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRange &range,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_hash_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRow &row,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_range_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRow &row,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  // param[@in]:
  // - part_id: object_id for partition, which is match with part_id in subpartition_array.
  // param[@out]:
  // - indexes: subpartition offset in subpartition_array
  static int get_list_tablet_and_subpart_id_(
      const common::ObPartID &part_id,
      const common::ObNewRow &row,
      ObSubPartition * const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  static int get_all_tablet_and_part_id_(
      ObPartition* const* partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  static int get_all_tablet_and_subpart_id_(
      const ObPartID &part_id,
      ObSubPartition* const* subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  static int get_range_tablet_and_part_id_(
      const ObPartition &start_bound,
      const ObPartition &end_bound,
      const common::ObBorderFlag &border_flag,
      ObPartition * const *partition_array,
      const int64_t partition_num,
      common::ObIArray<PartitionIndex> &indexes);

  static int get_range_tablet_and_subpart_id_(
      const ObSubPartition &start_bound,
      const ObSubPartition &end_bound,
      const common::ObBorderFlag &border_flag,
      const common::ObPartID &part_id,
      ObSubPartition * const *subpartition_array,
      const int64_t subpartition_num,
      common::ObIArray<PartitionIndex> &indexes);

  //Get partition start with start_part rowkey, return start_pos
  template <typename T>
  static int get_start_(const T *const *partition_array,
                        const int64_t partition_num,
                        const T &start_part,
                        int64_t &start_pos);


  //Get partition end with end_part rowkey, return end_pos
  template <typename T>
  static int get_end_(const T *const*partition_array,
                      const int64_t partition_num,
                      const common::ObBorderFlag &border_flag,
                      const T &end_part,
                      int64_t &end_pos);
  /* ----------------------------------------------------------------- */


  static int print_oracle_datetime_literal(const common::ObObj &tmp_obj,
                                           char *buf,
                                           const int64_t buf_len,
                                           int64_t &pos,
                                           const common::ObTimeZoneInfo *tz_info);
private:
  static const uint64_t DEFAULT_PARTITION_INDEX_NUM = 4;
};

template <typename T>
int ObPartitionUtils::get_start_(
    const T *const *partition_array,
    const int64_t partition_num,
    const T &start_part,
    int64_t &start_pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(partition_array)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Partition array should not be NULL", K(ret));
  } else {
    const T * const*result = std::upper_bound(partition_array,
                                              partition_array + partition_num,
                                              &start_part,
                                              T::less_than);
    start_pos = result - partition_array;
    if (start_pos < 0) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "Start pos error", K(partition_num), K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObPartitionUtils::get_end_(
    const T *const*partition_array,
    const int64_t partition_num,
    const common::ObBorderFlag &border_flag,
    const T &end_part,
    int64_t &end_pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(partition_array)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "Input partition array should not be NULL", K(ret));
  } else {
    const T *const*result = std::lower_bound(partition_array,
                                             partition_array + partition_num,
                                             &end_part,
                                             T::less_than);
    int64_t pos = result - partition_array;
    if (pos >= partition_num) {
      end_pos = partition_num - 1;
    } else if (OB_ISNULL(result) || OB_ISNULL(*result)) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "result should not be NULL", K(ret), K(result));
    } else {
      common::ObNewRow lrow;
      lrow.cells_ = const_cast<common::ObObj*>((*result)->high_bound_val_.get_obj_ptr());
      lrow.count_ = (*result)->high_bound_val_.get_obj_cnt();
      lrow.projector_ = (*result)->projector_;
      lrow.projector_size_ = (*result)->projector_size_;
      common::ObNewRow rrow;
      rrow.cells_ = const_cast<common::ObObj*>(end_part.high_bound_val_.get_obj_ptr());
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
  if (0 < partition_array[end_pos]->low_bound_val_.get_obj_cnt()) {
    ObNewRow lrow;
    lrow.cells_ = const_cast<ObObj*>(end_part.high_bound_val_.get_obj_ptr());
    lrow.count_ = end_part.high_bound_val_.get_obj_cnt();
    lrow.projector_ = end_part.projector_;
    lrow.projector_size_ = end_part.projector_size_;
    ObNewRow rrow;
    rrow.cells_ = const_cast<ObObj*>(partition_array[end_pos]->low_bound_val_.get_obj_ptr());
    rrow.count_ = partition_array[end_pos]->low_bound_val_.get_obj_cnt();
    rrow.projector_ = partition_array[end_pos]->projector_;
    rrow.projector_size_ = partition_array[end_pos]->projector_size_;
    int cmp = 0;
    if (OB_SUCCESS != ObRowUtil::compare_row(lrow, rrow, cmp)) {
      SHARE_SCHEMA_LOG(ERROR, "lhs or rhs is invalid", K(lrow), K(rrow), K(end_part),
                       KPC(partition_array[end_pos]));
    } else if (cmp < 0) {
      end_pos--;
    }
  }
  return ret;
}

class ObViewSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObViewSchema();
  explicit ObViewSchema(common::ObIAllocator *allocator);
  virtual ~ObViewSchema();
  ObViewSchema(const ObViewSchema &src_schema);
  ObViewSchema &operator=(const ObViewSchema &src_schema);
  bool operator==(const ObViewSchema &other) const;
  bool operator!=(const ObViewSchema &other) const;

  inline int set_view_definition(const char *view_definition) { return deep_copy_str(view_definition, view_definition_); }
  inline int set_view_definition(const common::ObString &view_definition) { return deep_copy_str(view_definition, view_definition_); }
  inline void set_view_check_option(const ViewCheckOption option) { view_check_option_ = option; }
  inline void set_view_is_updatable(const bool is_updatable) { view_is_updatable_ = is_updatable; }
  inline void set_materialized(const bool materialized) { materialized_ = materialized; }
  inline void set_character_set_client(const common::ObCharsetType character_set_client) {
    character_set_client_ = character_set_client;
  }
  inline void set_collation_connection(const common::ObCollationType collation_connection) {
    collation_connection_ = collation_connection;
  }

  //view_definition_ is defined using utf8, for sql resolve please use
  // ObSQLUtils::generate_view_definition_for_resolve
  inline const common::ObString &get_view_definition_str() const { return view_definition_; }
  inline const char *get_view_definition() const { return extract_str(view_definition_); }
  inline ViewCheckOption get_view_check_option() const { return view_check_option_; }
  inline bool get_view_is_updatable() const { return view_is_updatable_; }
  inline bool get_materialized() const { return materialized_; }
  inline common::ObCharsetType get_character_set_client() const { return character_set_client_; }
  inline common::ObCollationType get_collation_connection() const { return collation_connection_; }

  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual  void reset();

  TO_STRING_KV(N_VIEW_DEFINITION, view_definition_,
               N_CHECK_OPTION, ob_view_check_option_str(view_check_option_),
               N_IS_UPDATABLE, STR_BOOL(view_is_updatable_),
               N_IS_MATERIALIZED, STR_BOOL(materialized_),
               K_(character_set_client), K_(collation_connection));
private:
  common::ObString view_definition_;
  ViewCheckOption view_check_option_;
  bool view_is_updatable_;
  bool materialized_;
  common::ObCharsetType character_set_client_;
  common::ObCollationType collation_connection_;
};

class ObColumnSchemaHashWrapper
{
public:
  ObColumnSchemaHashWrapper() {}
  explicit ObColumnSchemaHashWrapper(const common::ObString &str) : column_name_(str) {}
  ~ObColumnSchemaHashWrapper(){}
  void set_name(const common::ObString &str) { column_name_ = str; }
  inline bool operator==(const ObColumnSchemaHashWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_));
  }
  inline uint64_t hash() const;
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  common::ObString column_name_;
};
class ObColumnSchemaWrapper
{
public:
  ObColumnSchemaWrapper() : column_name_(), prefix_len_(0) {}
  explicit ObColumnSchemaWrapper(const common::ObString &str, int32_t prefix_len)
      : column_name_(str), prefix_len_(prefix_len) {}
  ~ObColumnSchemaWrapper(){}
  void set_name(const common::ObString &str) { column_name_ = str; }
  inline bool name_equal(const ObColumnSchemaWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_));
  }
  inline bool all_equal(const ObColumnSchemaWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp;
    return (0 == name_cmp.compare(column_name_, other.column_name_))
           && prefix_len_ == other.prefix_len_;
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
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, column_name_, hash_ret);
  return hash_ret;
}

// 1. table is in recyclebin:
//    - pure_data_table_id is invalid
//    - index_name is table_name
// 2. table is in mysql mode(include sys table):
//    - pure_data_table_id is valid
//    - index_name is original_index_name
// 3. table is in oracle mode(include some inner table):
//    - pure_data_table_id is invalid
//    - index_name is original_index_name
class ObIndexSchemaHashWrapper
{
public :
  ObIndexSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        pure_data_table_id_(common::OB_INVALID_ID)
  {
  }
  ObIndexSchemaHashWrapper(uint64_t tenant_id, const uint64_t database_id,
                           const uint64_t data_table_id, const common::ObString &index_name)
      : tenant_id_(tenant_id), database_id_(database_id),
        pure_data_table_id_(data_table_id), index_name_(index_name)
  {
    pure_data_table_id_ = data_table_id;
  }
  ~ObIndexSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObIndexSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_index_name() const { return index_name_; }
  TO_STRING_KV(K_(index_name));
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t pure_data_table_id_; // only for mysql mode
  common::ObString index_name_;
};

inline uint64_t ObIndexSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&pure_data_table_id_, sizeof(uint64_t), hash_ret);
  //case insensitive
  hash_ret = common::ObCharset::hash(
             common::CS_TYPE_UTF8MB4_GENERAL_CI, index_name_, hash_ret, true, NULL);
  return hash_ret;
}

inline bool ObIndexSchemaHashWrapper::operator ==(const ObIndexSchemaHashWrapper &rv) const
{
  //mysql case insensitive
  //oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_)
         && (database_id_ == rv.database_id_)
         && (pure_data_table_id_ == rv.pure_data_table_id_)
         && (0 == name_cmp.compare(index_name_, rv.index_name_));
}

class ObTableSchemaHashWrapper
{
public :
  ObTableSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID), session_id_(common::OB_INVALID_ID),
      name_case_mode_(common::OB_NAME_CASE_INVALID)
  {
  }
  ObTableSchemaHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const uint64_t session_id, const common::ObNameCaseMode mode,
                           const common::ObString &table_name)
      : tenant_id_(tenant_id), database_id_(database_id), session_id_(session_id), name_case_mode_(mode), table_name_(table_name)
  {
  }
  ~ObTableSchemaHashWrapper() {}
  inline uint64_t hash() const;
  bool operator ==(const ObTableSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_session_id() const { return session_id_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(session_id), K_(table_name));
private :
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
  hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret, true, NULL);
  return hash_ret;
}

// See ObSchemaMgr::get_table_schema comment for session visibility judgment
inline bool ObTableSchemaHashWrapper::operator ==(const ObTableSchemaHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_, database_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
      && (name_case_mode_ == rv.name_case_mode_)
      && (session_id_ == rv.session_id_ || common::OB_INVALID_ID == rv.session_id_)
      && (0 == name_cmp.compare(table_name_ ,rv.table_name_));
}

class ObAuxVPSchemaHashWrapper
{
public :
  ObAuxVPSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID)
  {
  }
  ObAuxVPSchemaHashWrapper(uint64_t tenant_id, const uint64_t database_id,
                           const common::ObString &aux_vp_name)
      : tenant_id_(tenant_id), database_id_(database_id), aux_vp_name_(aux_vp_name)
  {
  }
  ~ObAuxVPSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObAuxVPSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_aux_vp_name() const { return aux_vp_name_; }
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString aux_vp_name_;
};

inline uint64_t ObAuxVPSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, aux_vp_name_, hash_ret);
  return hash_ret;
}

inline bool ObAuxVPSchemaHashWrapper::operator ==(const ObAuxVPSchemaHashWrapper &rv) const
{
  //mysql case insensitive
  //oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
         && (0 == name_cmp.compare(aux_vp_name_, rv.aux_vp_name_));
}

class ObDatabaseSchemaHashWrapper
{
public :
  ObDatabaseSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID), name_case_mode_(common::OB_NAME_CASE_INVALID)
  {
  }
  ObDatabaseSchemaHashWrapper(const uint64_t tenant_id, const common::ObNameCaseMode mode,
                              const common::ObString &database_name)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_name_(database_name)
  {
  }
  ~ObDatabaseSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObDatabaseSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline const common::ObString &get_database_name() const { return database_name_; }
private :
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

inline bool ObDatabaseSchemaHashWrapper::operator ==(const ObDatabaseSchemaHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
  return (tenant_id_ == rv.tenant_id_)
      && (name_case_mode_ == rv.name_case_mode_)
      && (0 == name_cmp.compare(database_name_ ,rv.database_name_));
}

class ObTablegroupSchemaHashWrapper
{
public :
  ObTablegroupSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID)
  {
  }
  ObTablegroupSchemaHashWrapper(uint64_t tenant_id, const common::ObString &tablegroup_name)
      : tenant_id_(tenant_id), tablegroup_name_(tablegroup_name)
  {
  }
  ~ObTablegroupSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObTablegroupSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
private :
  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
};

class ObForeignKeyInfoHashWrapper
{
public :
  ObForeignKeyInfoHashWrapper()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    foreign_key_name_.assign_ptr("", 0);
  }
  ObForeignKeyInfoHashWrapper(uint64_t tenant_id, const uint64_t database_id,
                              const common::ObString &foreign_key_name)
      : tenant_id_(tenant_id), database_id_(database_id), foreign_key_name_(foreign_key_name)
  {
  }
  ~ObForeignKeyInfoHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObForeignKeyInfoHashWrapper &rv) const;
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_foreign_key_name() const { return foreign_key_name_; }
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString foreign_key_name_;
};

inline uint64_t ObForeignKeyInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, foreign_key_name_, hash_ret);
  return hash_ret;
}

inline bool ObForeignKeyInfoHashWrapper::operator ==(const ObForeignKeyInfoHashWrapper &rv) const
{
  //mysql case insensitive
  //oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
         && (0 == name_cmp.compare(foreign_key_name_, rv.foreign_key_name_));
}

class ObConstraintInfoHashWrapper
{
public :
  ObConstraintInfoHashWrapper()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    constraint_name_.assign_ptr("", 0);
  }
  ObConstraintInfoHashWrapper(uint64_t tenant_id, const uint64_t database_id,
                              const common::ObString &constraint_name)
      : tenant_id_(tenant_id), database_id_(database_id), constraint_name_(constraint_name)
  {
  }
  ~ObConstraintInfoHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObConstraintInfoHashWrapper &rv) const;
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_constraint_name() const { return constraint_name_; }
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString constraint_name_;
};

inline uint64_t ObConstraintInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, constraint_name_, hash_ret);
  return hash_ret;
}

inline bool ObConstraintInfoHashWrapper::operator ==(const ObConstraintInfoHashWrapper &rv) const
{
  //mysql case insensitive
  //oracle case sensitive
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
         && (0 == name_cmp.compare(constraint_name_, rv.constraint_name_));
}

inline uint64_t ObTablegroupSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(tablegroup_name_.ptr(), tablegroup_name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObTablegroupSchemaHashWrapper::operator ==(const ObTablegroupSchemaHashWrapper &rv)
const
{
  return (tenant_id_ == rv.tenant_id_) && (tablegroup_name_ == rv.tablegroup_name_);
}

struct ObTenantOutlineId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantOutlineId()
      : tenant_id_(common::OB_INVALID_ID), outline_id_(common::OB_INVALID_ID)
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


//For managing privilege
struct ObTenantUserId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantUserId()
      : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTenantUserId(const uint64_t tenant_id, const uint64_t user_id)
      : tenant_id_(tenant_id), user_id_(user_id)
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


//For managing privilege
struct ObTenantUrObjId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantUrObjId()
    : tenant_id_(common::OB_INVALID_ID),
      grantee_id_(common::OB_INVALID_ID),
      obj_id_(common::OB_INVALID_ID),
      obj_type_(common::OB_INVALID_ID),
      col_id_(common::OB_INVALID_ID)
  {}
  ObTenantUrObjId(const uint64_t tenant_id, const uint64_t grantee_id,
                  const uint64_t obj_id, const uint64_t obj_type,
                  const uint64_t col_id)
    : tenant_id_(tenant_id), grantee_id_(grantee_id),
      obj_id_(obj_id), obj_type_(obj_type),
      col_id_(col_id)
  {}
  bool operator==(const ObTenantUrObjId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
            && (grantee_id_ == rhs.grantee_id_)
            && (obj_id_ == rhs.obj_id_ )
            && (obj_type_ == rhs.obj_type_)
            && (col_id_ == rhs.col_id_);
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
    return (tenant_id_ != common::OB_INVALID_ID)
            && (grantee_id_ != common::OB_INVALID_ID)
            && (obj_id_ != common::OB_INVALID_ID)
            && (obj_type_ != common::OB_INVALID_ID)
            && (col_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(grantee_id), K_(obj_id), K_(obj_type), K_(col_id));
  uint64_t tenant_id_;
  uint64_t grantee_id_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t col_id_;
};

class ObPrintPrivSet
{
public:
  explicit ObPrintPrivSet(ObPrivSet priv_set) : priv_set_(priv_set)
  {}

  DECLARE_TO_STRING;
private:
  ObPrivSet priv_set_;
};

class ObPrintPackedPrivArray
{
public:
  explicit ObPrintPackedPrivArray(const ObPackedPrivArray &packed_priv_array) :
      packed_priv_array_(packed_priv_array)
  {}

  DECLARE_TO_STRING;
private:
  const ObPackedPrivArray &packed_priv_array_;
};

class ObPriv
{
  OB_UNIS_VERSION_V(1);

public:
  ObPriv()
      : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
        schema_version_(1), priv_set_(0), priv_array_()
  { }
  ObPriv(common::ObIAllocator *allocator)
      : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
        schema_version_(1), priv_set_(0),
        priv_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(*allocator))
  { }
  ObPriv(const uint64_t tenant_id, const uint64_t user_id,
         const int64_t schema_version, const ObPrivSet priv_set)
      : tenant_id_(tenant_id), user_id_(user_id),
        schema_version_(schema_version), priv_set_(priv_set), priv_array_()
  { }

  virtual ~ObPriv() { }
  ObPriv& operator=(const ObPriv &other);
  static bool cmp_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  { return (lhs->get_tenant_user_id() < tenant_user_id); }
  static bool equal_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  { return (lhs->get_tenant_user_id() == tenant_user_id); }
  static bool cmp_tenant_id(const ObPriv *lhs, const uint64_t tenant_id)
  { return (lhs->get_tenant_id() < tenant_id); }
  ObTenantUserId get_tenant_user_id() const
  { return ObTenantUserId(tenant_id_, user_id_); }

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_user_id(const uint64_t user_id) { user_id_ = user_id; }
  inline void set_schema_version(const uint64_t schema_version) { schema_version_ = schema_version;}
  inline void set_priv(const ObPrivType priv) { priv_set_ |= priv; }
  inline void set_priv_set(const ObPrivSet priv_set) { priv_set_ = priv_set; }
  inline void reset_priv_set() { priv_set_ = 0; }
  inline void set_obj_privs(const ObPackedObjPriv obj_privs) { priv_set_ = obj_privs; }
  int set_priv_array(const ObPackedPrivArray &other)
  { return priv_array_.assign(other); }

  inline uint64_t get_tenant_id() const { return tenant_id_; };
  inline uint64_t get_user_id() const { return user_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline ObPrivSet get_priv_set() const { return priv_set_; }
  inline ObPrivType get_priv(const ObPrivType priv) const { return priv_set_ & priv; }
  inline const ObPackedPrivArray &get_priv_array() const {return priv_array_;}
  inline ObPackedObjPriv get_obj_privs() const {return priv_set_; }
  virtual void reset();
  int64_t get_convert_size() const;
  virtual bool is_valid() const
  { return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != user_id_
        && schema_version_ > 0; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(schema_version),
              "privileges", ObPrintPrivSet(priv_set_));
protected:
  uint64_t tenant_id_;
  uint64_t user_id_;
  int64_t schema_version_;
  ObPrivSet priv_set_;
  //ObPrivSet ora_sys_priv_set_;
  ObPackedPrivArray priv_array_;
};

// Not used now
class ObUserInfoHashWrapper
{
public :
  ObUserInfoHashWrapper()
      : tenant_id_(common::OB_INVALID_ID)
  {}
  ObUserInfoHashWrapper(uint64_t tenant_id, const common::ObString &user_name)
      : tenant_id_(tenant_id),user_name_(user_name)
  {
  }
  ~ObUserInfoHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObUserInfoHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_user_name() const { return user_name_; }
private :
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

inline bool ObUserInfoHashWrapper::operator ==(const ObUserInfoHashWrapper &other) const
{
  return (tenant_id_ == other.tenant_id_) && (user_name_ == other.user_name_);
}

enum class ObSSLType : int
{
  SSL_TYPE_NOT_SPECIFIED = 0,
  SSL_TYPE_NONE,
  SSL_TYPE_ANY,
  SSL_TYPE_X509,
  SSL_TYPE_SPECIFIED,
  SSL_TYPE_MAX
};

common::ObString get_ssl_type_string(const ObSSLType ssl_type);
ObSSLType get_ssl_type_from_string(const common::ObString &ssl_type_str);

enum class ObSSLSpecifiedType : int
{
  SSL_SPEC_TYPE_CIPHER = 0,
  SSL_SPEC_TYPE_ISSUER,
  SSL_SPEC_TYPE_SUBJECT,
  SSL_SPEC_TYPE_MAX
};

const char *get_ssl_spec_type_str(const ObSSLSpecifiedType ssl_spec_type);

enum ObUserType
{
  OB_USER = 0,
  OB_ROLE,
  OB_TYPE_MAX,
};

#define ADMIN_OPTION_SHIFT 0
#define DISABLE_FLAG_SHIFT 1

class ObUserInfo : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  ObUserInfo()
    :ObSchema(), ObPriv(),
     user_name_(), host_name_(), passwd_(), info_(), locked_(false),
     ssl_type_(ObSSLType::SSL_TYPE_NOT_SPECIFIED), ssl_cipher_(), x509_issuer_(), x509_subject_(),
     type_(OB_USER), grantee_id_array_(), role_id_array_(), profile_id_(common::OB_INVALID_ID), password_last_changed_timestamp_(common::OB_INVALID_TIMESTAMP),
     role_id_option_array_(),
     max_connections_(0),
     max_user_connections_(0)
  { }
  explicit ObUserInfo(common::ObIAllocator *allocator);
  virtual ~ObUserInfo();
  ObUserInfo(const ObUserInfo &other);
  ObUserInfo& operator=(const ObUserInfo &other);
  static bool cmp(const ObUserInfo *lhs, const ObUserInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() < rhs->get_tenant_user_id() : false; }
  static bool equal(const ObUserInfo *lhs, const ObUserInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() == rhs->get_tenant_user_id() : false; }

  //set methods
  inline int set_user_name(const char *user_name) { return deep_copy_str(user_name, user_name_); }
  inline int set_user_name(const common::ObString &user_name)  { return deep_copy_str(user_name, user_name_); }
  inline int set_host(const char *host_name) { return deep_copy_str(host_name, host_name_); }
  inline int set_host(const common::ObString &host_name) { return deep_copy_str(host_name, host_name_); }
  inline int set_passwd(const char *passwd) { return deep_copy_str(passwd, passwd_); }
  inline int set_passwd(const common::ObString &passwd) { return deep_copy_str(passwd, passwd_); }
  inline int set_info(const char *info) { return deep_copy_str(info, info_); }
  inline int set_info(const common::ObString &info) { return deep_copy_str(info, info_); }
  inline void set_is_locked(const bool locked) { locked_ = locked; }
  inline void set_ssl_type(const ObSSLType ssl_type) { ssl_type_ = ssl_type; }
  inline int set_ssl_cipher(const char *ssl_cipher) { return deep_copy_str(ssl_cipher, ssl_cipher_); }
  inline int set_ssl_cipher(const common::ObString &ssl_cipher) { return deep_copy_str(ssl_cipher, ssl_cipher_); }
  inline int set_x509_issuer(const char *x509_issuer) { return deep_copy_str(x509_issuer, x509_issuer_); }
  inline int set_x509_issuer(const common::ObString &x509_issuer) { return deep_copy_str(x509_issuer, x509_issuer_); }
  inline int set_x509_subject(const char *x509_subject) { return deep_copy_str(x509_subject, x509_subject_); }
  inline int set_x509_subject(const common::ObString &x509_subject) { return deep_copy_str(x509_subject, x509_subject_); }
  inline void set_type(const int32_t type) { type_ = type; }
  inline void set_profile_id(const uint64_t profile_id) { profile_id_ = profile_id; }
  inline void set_password_last_changed(int64_t ts) { password_last_changed_timestamp_ = ts; }
  inline void set_max_connections(uint64_t max_connections) { max_connections_ = max_connections; }
  inline void set_max_user_connections(uint64_t max_user_connections) { max_user_connections_ = max_user_connections; }
  //get methods
  inline const char* get_user_name() const { return extract_str(user_name_); }
  inline const common::ObString& get_user_name_str() const { return user_name_; }
  inline const char* get_host_name() const { return extract_str(host_name_); }
  inline const common::ObString& get_host_name_str() const { return host_name_; }
  inline const char* get_passwd() const { return extract_str(passwd_); }
  inline const common::ObString& get_passwd_str() const { return passwd_; }
  inline const char* get_info() const { return extract_str(info_); }
  inline const common::ObString& get_info_str() const { return info_; }
  inline bool get_is_locked() const { return locked_; }
  inline ObSSLType get_ssl_type() const { return ssl_type_; }
  inline const common::ObString get_ssl_type_str() const { return get_ssl_type_string(ssl_type_); }
  inline const char* get_ssl_cipher() const { return extract_str(ssl_cipher_); }
  inline const common::ObString& get_ssl_cipher_str() const { return ssl_cipher_; }
  inline const char* get_x509_issuer() const { return extract_str(x509_issuer_); }
  inline const common::ObString& get_x509_issuer_str() const { return x509_issuer_; }
  inline const char* get_x509_subject() const { return extract_str(x509_subject_); }
  inline const common::ObString& get_x509_subject_str() const { return x509_subject_; }
  inline uint64_t get_profile_id() const { return profile_id_; }
  inline int64_t get_password_last_changed() const { return password_last_changed_timestamp_; }
  inline uint64_t get_max_connections() const { return max_connections_; }
  inline uint64_t get_max_user_connections() const { return max_user_connections_; }
  // role
  inline bool is_role() const { return OB_ROLE == type_; }
  inline int64_t get_role_count() const { return role_id_array_.count(); }
  const common::ObSEArray<uint64_t, 8>& get_grantee_id_array() const { return grantee_id_array_; }
  const common::ObSEArray<uint64_t, 8>& get_role_id_array() const { return role_id_array_; }
  const common::ObSEArray<uint64_t, 8>& get_role_id_option_array() const { return role_id_option_array_; }
  int add_grantee_id(const uint64_t id) { return grantee_id_array_.push_back(id); }
  int add_role_id(const uint64_t id,
                  const uint64_t admin_option = NO_OPTION,
                  const uint64_t disable_flag = 0);
  void set_admin_option(uint64_t &option, const uint64_t admin_option)
  { option |= (admin_option << ADMIN_OPTION_SHIFT); }
  void set_disable_flag(uint64_t &option, const uint64_t disable_flag)
  { option |= (disable_flag << DISABLE_FLAG_SHIFT); }
  int get_nth_role_option(uint64_t nth, uint64_t &option) const;
  uint64_t get_admin_option(uint64_t option) const { return 1 & (option >> ADMIN_OPTION_SHIFT); }
  uint64_t get_disable_option(uint64_t option) const { return 1 & (option >> DISABLE_FLAG_SHIFT); }

  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(user_name), K_(host_name),
               "privileges", ObPrintPrivSet(priv_set_),
               K_(info), K_(locked),
               K_(ssl_type), K_(ssl_cipher), K_(x509_issuer), K_(x509_subject),
               K_(type), K_(grantee_id_array), K_(role_id_array),
               K_(profile_id)
              );
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
  common::ObSEArray<uint64_t, 8> grantee_id_array_; // Record role granted to user
  common::ObSEArray<uint64_t, 8> role_id_array_; // Record which roles the user/role has
  uint64_t profile_id_;
  int64_t password_last_changed_timestamp_;
  common::ObSEArray<uint64_t, 8> role_id_option_array_; // Record which roles the user/role has
  uint64_t max_connections_;
  uint64_t max_user_connections_;
};

struct ObDBPrivSortKey
{
  ObDBPrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID), sort_(0)
  {}
  ObDBPrivSortKey(const uint64_t tenant_id, const uint64_t user_id, const uint64_t sort_value)
      : tenant_id_(tenant_id), user_id_(user_id), sort_(sort_value)
  {}
  bool operator==(const ObDBPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (sort_ == rhs.sort_);
  }
  bool operator!=(const ObDBPrivSortKey &rhs) const
  { return !(*this == rhs); }
  bool operator<(const ObDBPrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = sort_ > rhs.sort_;//sort values of 'sort_' from big to small
      }
    }
    return bret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K(sort_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t sort_;
};

struct ObOriginalDBKey
{
  ObOriginalDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObOriginalDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObOriginalDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_);
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
  //Not used yet.
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

  int deep_copy(const ObOriginalDBKey &src, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    tenant_id_ = src.tenant_id_;
    user_id_ = src.user_id_;
    if (OB_FAIL(common::ob_write_string(allocator, src.db_, db_))) {
      SHARE_SCHEMA_LOG(WARN,"failed to deep copy db", KR(ret), K(src.db_));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
};

struct ObSysPrivKey
{
  ObSysPrivKey() : tenant_id_(common::OB_INVALID_ID), grantee_id_(common::OB_INVALID_ID)
  {}
  ObSysPrivKey(const uint64_t tenant_id, const uint64_t user_id)
      : tenant_id_(tenant_id), grantee_id_(user_id)
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
  //Not used yet.
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

class ObDBPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  ObDBPriv()
    : ObPriv(), db_(), sort_(0)
  {}
  explicit ObDBPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(allocator), db_(), sort_(0)
  {}
  virtual ~ObDBPriv()
  {}
  ObDBPriv(const ObDBPriv &other)
      : ObSchema(), ObPriv()
  { *this = other; }
  ObDBPriv& operator=(const ObDBPriv &other);

  static bool cmp(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      (lhs->get_sort_key() < rhs->get_sort_key()) : false;
  }
  static bool cmp_sort_key(const ObDBPriv *lhs, const ObDBPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  ObDBPrivSortKey get_sort_key() const
  { return ObDBPrivSortKey(tenant_id_, user_id_, sort_); }
  static bool equal(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      lhs->get_sort_key() == rhs->get_sort_key(): false;
  } // point check
  ObOriginalDBKey get_original_key() const
  { return ObOriginalDBKey(tenant_id_, user_id_, db_); }

  //set methods
  inline int set_database_name(const char *db) { return deep_copy_str(db, db_); }
  inline int set_database_name(const common::ObString &db) { return deep_copy_str(db, db_); }
  inline void set_sort(const uint64_t sort) { sort_ = sort; }

  //get methods
  inline const char* get_database_name() const { return extract_str(db_); }
  inline const common::ObString& get_database_name_str() const { return db_; }
  inline uint64_t get_sort() const { return sort_; }
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), "privileges", ObPrintPrivSet(priv_set_));
private:
  common::ObString db_;
  uint64_t sort_;
};

// In order to find in table_privs_ whether a table is authorized under a certain db
struct ObTablePrivDBKey
{
  ObTablePrivDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObTablePrivDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_);
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

struct ObTablePrivSortKey
{
  ObTablePrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivSortKey(const uint64_t tenant_id, const uint64_t user_id,
                     const common::ObString &db, const common::ObString &table)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db), table_(table)
  {}
  bool operator==(const ObTablePrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_) && (table_ == rhs.table_);
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
  //Not used yet.
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

  int deep_copy(const ObTablePrivSortKey &src, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    tenant_id_ = src.tenant_id_;
    user_id_ = src.user_id_;
    if (OB_FAIL(common::ob_write_string(allocator, src.db_, db_))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy db", KR(ret), K(src.db_));
    } else if (OB_FAIL(common::ob_write_string(allocator, src.table_, table_))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy table", KR(ret), K(src.table_));
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
};

struct ObObjPrivSortKey
{
  ObObjPrivSortKey() : tenant_id_(common::OB_INVALID_ID),
                       obj_id_(common::OB_INVALID_ID),
                       obj_type_(common::OB_INVALID_ID),
                       col_id_(common::OB_INVALID_ID),
                       grantor_id_(common::OB_INVALID_ID),
                       grantee_id_(common::OB_INVALID_ID)
  {}
  ObObjPrivSortKey(const uint64_t tenant_id,
                   const uint64_t obj_id,
                   const uint64_t obj_type,
                   const uint64_t col_id,
                   const uint64_t grantor_id,
                   const uint64_t grantee_id)
      : tenant_id_(tenant_id), obj_id_(obj_id), obj_type_(obj_type),
        col_id_(col_id), grantor_id_(grantor_id), grantee_id_(grantee_id)
  {}
  bool operator==(const ObObjPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
           && (obj_id_ == rhs.obj_id_) && (obj_type_ == rhs.obj_type_)
           && (col_id_ == rhs.col_id_) && (grantor_id_ == rhs.grantor_id_)
           && (grantee_id_ == rhs.grantee_id_);
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
    return (tenant_id_ != common::OB_INVALID_ID)
           && (obj_id_ != common::OB_INVALID_ID) && (obj_type_ != common::OB_INVALID_ID)
           && (grantor_id_ != common::OB_INVALID_ID) && (grantee_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(obj_id), K_(obj_type),
               K_(col_id), K_(grantor_id), K_(grantee_id));
  uint64_t tenant_id_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t col_id_;
  uint64_t grantor_id_;
  uint64_t grantee_id_;
};

typedef common::ObSEArray<share::schema::ObObjPrivSortKey, 4> ObObjPrivSortKeyArray;

class ObTablePriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  //constructor and destructor
  ObTablePriv()
      : ObSchema(), ObPriv()
  { }
  explicit ObTablePriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(allocator)
  { }
  ObTablePriv(const ObTablePriv &other)
      : ObSchema(), ObPriv()
  { *this = other; }
  virtual ~ObTablePriv() { }

  //operator=
  ObTablePriv& operator=(const ObTablePriv &other);

  //for sort
  ObTablePrivSortKey get_sort_key() const
  { return ObTablePrivSortKey(tenant_id_, user_id_, db_, table_); }
  static bool cmp(const ObTablePriv *lhs, const ObTablePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObTablePriv *lhs, const ObTablePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  ObTablePrivDBKey get_db_key() const
  { return ObTablePrivDBKey(tenant_id_, user_id_, db_); }
  static bool cmp_db_key(const ObTablePriv *lhs, const ObTablePrivDBKey &db_key)
  { return lhs->get_db_key() < db_key; }

  //set methods
  inline int set_database_name(const char *db) { return deep_copy_str(db, db_); }
  inline int set_database_name(const common::ObString &db) { return deep_copy_str(db, db_); }
  inline int set_table_name(const char *table) { return deep_copy_str(table, table_); }
  inline int set_table_name(const common::ObString &table) { return deep_copy_str(table, table_); }

  //get methods
  inline const char* get_database_name() const { return extract_str(db_); }
  inline const common::ObString& get_database_name_str() const { return db_; }
  inline const char* get_table_name() const { return extract_str(table_); }
  inline const common::ObString& get_table_name_str() const { return table_; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table),
               "privileges", ObPrintPrivSet(priv_set_));
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  common::ObString db_;
  common::ObString table_;
};

class ObObjPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  //constructor and destructor
  ObObjPriv()
      : ObSchema(), ObPriv(),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID)
  { }
  explicit ObObjPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID)
  { }
  ObObjPriv(const ObObjPriv &other)
      : ObSchema(), ObPriv(),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID)
  { *this = other; }
  virtual ~ObObjPriv() { }

  //operator=
  ObObjPriv& operator=(const ObObjPriv &other);

  //for sort
  ObObjPrivSortKey get_sort_key() const
  { return ObObjPrivSortKey(tenant_id_, obj_id_, obj_type_, col_id_,
                            grantor_id_, grantee_id_); }

  ObTenantUrObjId get_tenant_ur_obj_id() const
  { return ObTenantUrObjId(tenant_id_, grantee_id_, obj_id_, obj_type_, col_id_); }

  static bool cmp_tenant_ur_obj_id(const ObObjPriv *lhs, const ObTenantUrObjId &rhs)
  { return (lhs->get_tenant_ur_obj_id() < rhs); }
  static bool cmp(const ObObjPriv *lhs, const ObObjPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObObjPriv *lhs, const ObObjPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObObjPriv *lhs, const ObObjPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObObjPriv *lhs, const ObObjPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  // ObTablePrivDBKey get_db_key() const
  // { return ObTablePrivDBKey(tenant_id_, user_id_, db_); }
  // static bool cmp_db_key(const ObTablePriv *lhs, const ObTablePrivDBKey &db_key)
  // { return lhs->get_db_key() < db_key; }

  //set methods
  inline void set_obj_id(const uint64_t obj_id) { obj_id_ = obj_id; }
  inline void set_col_id(const uint64_t col_id) { col_id_ = col_id; }
  inline void set_grantor_id(const uint64_t grantor_id) { grantor_id_ = grantor_id; }
  inline void set_grantee_id(const uint64_t grantee_id) { grantee_id_ = grantee_id; }
  inline void set_objtype(const uint64_t objtype) { obj_type_ = objtype; }
  //get methods
  inline uint64_t get_objtype() const { return obj_type_; }
  inline uint64_t get_obj_id() const { return obj_id_; }
  inline uint64_t get_col_id() const { return col_id_; }
  inline uint64_t get_grantee_id() const { return grantee_id_; }
  inline uint64_t get_grantor_id() const { return grantor_id_; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(obj_id), K_(obj_type), K_(col_id),
               "privileges", ObPrintPrivSet(priv_set_), K_(grantor_id), K_(grantee_id));
  //other methods
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

enum ObPrivLevel
{
  OB_PRIV_INVALID_LEVEL,
  OB_PRIV_USER_LEVEL,
  OB_PRIV_DB_LEVEL,
  OB_PRIV_TABLE_LEVEL,
  OB_PRIV_DB_ACCESS_LEVEL,
  OB_PRIV_SYS_ORACLE_LEVEL,   /* oracle-mode system privilege */
  OB_PRIV_OBJ_ORACLE_LEVEL,   /* oracle-mode object privilege */
  OB_PRIV_MAX_LEVEL,
};

enum ObPrivCheckType
{
  OB_PRIV_CHECK_ALL,
  OB_PRIV_CHECK_ANY,
  OB_PRIV_CHECK_OTHER,
};

const char *ob_priv_level_str(const ObPrivLevel grant_level);

struct ObNeedPriv
{
  ObNeedPriv(const common::ObString &db,
             const common::ObString &table,
             ObPrivLevel priv_level,
             ObPrivSet priv_set,
             const bool is_sys_table,
             const bool is_for_update = false,
             ObPrivCheckType priv_check_type = OB_PRIV_CHECK_ALL)
      : db_(db), table_(table), priv_level_(priv_level), priv_set_(priv_set),
        is_sys_table_(is_sys_table), is_for_update_(is_for_update), priv_check_type_(priv_check_type)
  { }
  ObNeedPriv()
      : db_(), table_(), priv_level_(OB_PRIV_INVALID_LEVEL), priv_set_(0), is_sys_table_(false),
        is_for_update_(false), priv_check_type_(OB_PRIV_CHECK_ALL)
  { }
  int deep_copy(const ObNeedPriv &other, common::ObIAllocator &allocator);
  common::ObString db_;
  common::ObString table_;
  ObPrivLevel priv_level_;
  ObPrivSet priv_set_;
  bool is_sys_table_; // May be used to represent the table of schema metadata
  bool is_for_update_;
  ObPrivCheckType priv_check_type_;
  TO_STRING_KV(K_(db), K_(table), K_(priv_set), K_(priv_level), K_(is_sys_table), K_(is_for_update),
               K_(priv_check_type));
};

struct ObStmtNeedPrivs
{
  typedef common::ObFixedArray<ObNeedPriv, common::ObIAllocator> NeedPrivs;
  explicit ObStmtNeedPrivs(common::ObIAllocator &alloc) : need_privs_(alloc)
  {}
  ObStmtNeedPrivs() : need_privs_()
  {}
  ~ObStmtNeedPrivs()
  { reset(); }
  void reset()
  { need_privs_.reset(); }
  int deep_copy(const ObStmtNeedPrivs &other, common::ObIAllocator &allocator);
  NeedPrivs need_privs_;
  TO_STRING_KV(K_(need_privs));
};

#define CHECK_FLAG_NORMAL             0X0
#define CHECK_FLAG_DIRECT             0X1
#define CHECK_FLAG_WITH_GRANT_OPTION  0x2
/* flag Can proceed or */

// Define the permissions required in oracle mode.
// Record a set of system permissions, or an object permission
struct ObOraNeedPriv
{
  explicit ObOraNeedPriv(common::ObIAllocator &allocator);
  ObOraNeedPriv() :
        db_name_(),
        grantee_id_(common::OB_INVALID_ID),
        obj_id_(common::OB_INVALID_ID),
        obj_level_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        check_flag_(false),
        obj_privs_(0),
        owner_id_(common::OB_INVALID_ID),
        col_id_array_()
  { }
  ObOraNeedPriv(
      const common::ObString &db_name,
      uint64_t grantee_id,
      uint64_t obj_id,
      uint64_t obj_level,
      uint64_t obj_type,
      uint32_t check_flag,
      ObPackedObjPriv obj_privs,
      uint64_t owner_id)
      : db_name_(db_name), grantee_id_(grantee_id), obj_id_(obj_id),
        obj_level_(obj_level), obj_type_(obj_type), check_flag_(check_flag),
        obj_privs_(obj_privs), owner_id_(owner_id), col_id_array_()
  { }
  ~ObOraNeedPriv()
  { }
  int deep_copy(const ObOraNeedPriv &other, common::ObIAllocator &allocator);
  bool same_obj(const ObOraNeedPriv &other);
  bool same_col_id_array(const ObOraNeedPriv &other);
  //ObOraNeedPriv& operator=(const ObOraNeedPriv &other);
  common::ObString db_name_;
  uint64_t grantee_id_;
  uint64_t obj_id_;
  uint64_t obj_level_; // obj_level is used to mark whether the object of the object permissions is a table or a column
                    // OBJ_LEVEL_FOR_COL_PRIV: need_priv means one or more columns
                    // OBJ_LEVEL_FOR_TAB_PRIV: need_priv means a table
  uint64_t obj_type_;
  uint64_t check_flag_;
  ObPackedObjPriv obj_privs_;
  uint64_t owner_id_;
  // When col_id_ is OBJ_LEVEL_FOR_COL_PRIV, col_id_array_ indicates the column to be checked
  common::ObArray<uint64_t> col_id_array_;
  TO_STRING_KV(K_(db_name), K_(grantee_id), K_(obj_id), K_(obj_level), K_(obj_type),
               K_(check_flag), K_(obj_privs), K_(owner_id), K_(col_id_array));
};

// Define the permissions required for a statement in oracle mode. Use an array to record,
// the system permissions and multiple object permissions are recorded in the array
struct ObStmtOraNeedPrivs
{
  typedef common::ObFixedArray<ObOraNeedPriv, common::ObIAllocator> OraNeedPrivs;
  explicit ObStmtOraNeedPrivs(common::ObIAllocator &alloc) : need_privs_(alloc)
  {}
  ObStmtOraNeedPrivs() : need_privs_()
  {}
  ~ObStmtOraNeedPrivs()
  { reset(); }
  void reset()
  { need_privs_.reset(); }
  int deep_copy(const ObStmtOraNeedPrivs &other, common::ObIAllocator &allocator);
  OraNeedPrivs need_privs_;
  TO_STRING_KV(K_(need_privs));
};

struct ObSessionPrivInfo
{
  ObSessionPrivInfo() :
      tenant_id_(common::OB_INVALID_ID),
      user_id_(common::OB_INVALID_ID),
      user_name_(),
      host_name_(),
      db_(),
      user_priv_set_(0),
      db_priv_set_(0),
      effective_tenant_id_(common::OB_INVALID_ID),
      enable_role_id_array_()
  {}
  ObSessionPrivInfo(const uint64_t tenant_id,
                    const uint64_t effective_tenant_id,
                    const uint64_t user_id,
                    const common::ObString &db,
                    const ObPrivSet user_priv_set,
                    const ObPrivSet db_priv_set)
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

  virtual ~ObSessionPrivInfo() {}

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
  bool is_tenant_changed() const { return common::OB_INVALID_ID != effective_tenant_id_
                                          && tenant_id_ != effective_tenant_id_; }
  void set_effective_tenant_id(uint64_t effective_tenant_id) { effective_tenant_id_ = effective_tenant_id; }
  uint64_t get_effective_tenant_id() { return effective_tenant_id_; }
  virtual TO_STRING_KV(K_(tenant_id), K_(effective_tenant_id), K_(user_id), K_(user_name), K_(host_name),
                       K_(db), K_(user_priv_set), K_(db_priv_set));

  uint64_t tenant_id_; //for privilege.Current login tenant. if normal tenant access
                       //sys tenant's object should use other method for priv checking.
  uint64_t user_id_;
  common::ObString user_name_;
  common::ObString host_name_;
  common::ObString db_;              //db name in current session
  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;    //user's db_priv_set of db
  // Only used for privilege check to determine whether there are currently tenants, otherwise the value is illegal
  uint64_t effective_tenant_id_;
  common::ObSEArray<uint64_t, 8> enable_role_id_array_;
};

struct ObUserLoginInfo
{
  ObUserLoginInfo() {}
  ObUserLoginInfo(const common::ObString &tenant_name,
                  const common::ObString &user_name,
                  const common::ObString &client_ip,
                  const common::ObString &passwd,
                  const common::ObString &db)
      : tenant_name_(tenant_name), user_name_(user_name), client_ip_(client_ip),
        passwd_(passwd), db_(db), scramble_str_()
  {}

  ObUserLoginInfo(const common::ObString &tenant_name,
                  const common::ObString &user_name,
                  const common::ObString &client_ip,
                  const common::ObString &passwd,
                  const common::ObString &db,
                  const common::ObString &scramble_str)
      : tenant_name_(tenant_name), user_name_(user_name), client_ip_(client_ip),
        passwd_(passwd), db_(db), scramble_str_(scramble_str)
  {}

  TO_STRING_KV(K_(tenant_name), K_(user_name), K_(client_ip), K_(db), K_(scramble_str));
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString client_ip_;//client ip for current user
  common::ObString passwd_;
  common::ObString db_;
  common::ObString scramble_str_;
};

// oracle compatible: define u/r system permissions
class ObSysPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  ObSysPriv(): ObSchema(), ObPriv(), grantee_id_(common::OB_INVALID_ID) {}
  explicit ObSysPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(allocator), grantee_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObSysPriv()
  {}
  ObSysPriv(const ObSysPriv &other)
      : ObSchema(), ObPriv(), grantee_id_(common::OB_INVALID_ID)
  { *this = other; }
  ObSysPriv& operator=(const ObSysPriv &other);

  static bool cmp_tenant_grantee_id(
      const ObSysPriv *lhs,
      const ObTenantUserId &tenant_grantee_id)
  { return (lhs->get_tenant_grantee_id() < tenant_grantee_id); }
  static bool equal_tenant_grantee_id(
      const ObSysPriv *lhs,
      const ObTenantUserId &tenant_grantee_id)
  { return (lhs->get_tenant_grantee_id() == tenant_grantee_id); }
  static bool cmp_tenant_id(
      const ObPriv *lhs,
      const uint64_t tenant_id)
  { return (lhs->get_tenant_id() < tenant_id); }
  ObTenantUserId get_tenant_grantee_id() const
  { return ObTenantUserId(tenant_id_, grantee_id_); }
  void set_grantee_id(uint64_t grantee_id) { grantee_id_ = grantee_id; }
  uint64_t get_grantee_id() const { return grantee_id_; }
  void set_revoke() { user_id_ = 234; }
  bool is_revoke() { return user_id_ == 234; }

  static bool cmp(const ObSysPriv *lhs, const ObSysPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      (lhs->get_key() < rhs->get_key()) : false;
  }
  static bool cmp_key(const ObSysPriv *lhs, const ObSysPrivKey &key)
  { return NULL != lhs ? lhs->get_key() < key : false; }
  ObSysPrivKey get_key() const
  { return ObSysPrivKey(tenant_id_, grantee_id_); }
  static bool equal(const ObSysPriv *lhs, const ObSysPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      lhs->get_key() == rhs->get_key(): false;
  } // point check
  //ObSysPrivKey get_original_key() const
  //{ return ObSysPrivKey(tenant_id_, user_id_); }


  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(grantee_id),
               "privileges", ObPrintPrivSet(priv_set_),
               "packedprivarray", ObPrintPackedPrivArray(priv_array_));
private:
  uint64_t grantee_id_;
};

int get_int_value(const common::ObString &str, int64_t &value);

class ObHostnameStuct
{
public:
  ObHostnameStuct() {}
  ~ObHostnameStuct() {}
  static const uint32_t FAKE_PORT = 0;
  static const int MAX_IP_BITS = 128;
  static int get_int_value(const common::ObString &str, int64_t &value);
  static bool calc_ip(const common::ObString &host_ip, common::ObAddr &addr);
  static bool calc_ip_mask(const common::ObString &host_ip_mask, common::ObAddr &mask);
  static bool is_ip_match(const common::ObString &client_ip, common::ObString host_name);
  static bool is_wild_match(const common::ObString &client_ip, const common::ObString &host_name);
  static bool is_in_white_list(const common::ObString &client_ip, common::ObString &ip_white_list);
};

enum ObHintFormat
{
  HINT_NORMAL,
  HINT_LOCAL,
};

class ObFixedParam
{
public:
  ObFixedParam() : offset_(common::OB_INVALID_INDEX), value_() { }
  virtual ~ObFixedParam() {}
  bool has_equal_value(const common::ObObj &other_value) const;
  bool is_equal(const ObFixedParam &other_param) const;
  VIRTUAL_TO_STRING_KV(K(offset_), K(value_));
  int64_t offset_;
  common::ObObj value_;
};

class ObMaxConcurrentParam
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t UNLIMITED = -1;
  typedef common::ObArray<ObFixedParam, common::ObWrapperAllocator> FixParamStore;
  explicit ObMaxConcurrentParam(common::ObIAllocator *allocator,
                                const common::ObMemAttr &attr = common::ObMemAttr());
  virtual ~ObMaxConcurrentParam();
  int destroy();
  int64_t get_convert_size() const;
  int assign(const ObMaxConcurrentParam &max_concurrent_param);
  int64_t get_concurrent_num() const { return concurrent_num_;  }
  const common::ObString &get_sql_text() const { return sql_text_; }
  int match_fixed_param(const ParamStore &const_param_store, bool &is_match) const;
  bool is_concurrent_limit_param() const {return concurrent_num_ != UNLIMITED;}
  bool is_outline_content_param() const { return !outline_content_.empty();}
  int get_fixed_param_with_offset(int64_t offset, ObFixedParam &fixed_param, bool &is_found) const ;
  int same_param_as(const ObMaxConcurrentParam &other, bool &is_same) const;
  void set_mem_attr(const common::ObMemAttr &attr) { mem_attr_ = attr; }
  VIRTUAL_TO_STRING_KV(K_(concurrent_num), K_(outline_content), K_(fixed_param_store), K_(sql_text));
private:
  int deep_copy_outline_content(const common::ObString &src);
  int deep_copy_param_value(const common::ObObj &src, common::ObObj &dest);
  int deep_copy_sql_text(const common::ObString &src);
public:
  common::ObIAllocator *allocator_;
  int64_t concurrent_num_;
  common::ObString outline_content_;
  common::ObMemAttr mem_attr_;
  FixParamStore fixed_param_store_;
  common::ObString sql_text_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMaxConcurrentParam);
};

//used for outline manager
class ObOutlineParamsWrapper
{
  typedef common::ObArray<ObMaxConcurrentParam*, common::ObWrapperAllocatorWithAttr> OutlineParamsArray;
  OB_UNIS_VERSION(1);
public:
  ObOutlineParamsWrapper();
  explicit ObOutlineParamsWrapper(common::ObIAllocator *allocator);
  ~ObOutlineParamsWrapper();
  int destroy();
  int assign(const ObOutlineParamsWrapper &src);
  int set_allocator(common::ObIAllocator *allocator,
                    const common::ObMemAttr &attr = common::ObMemAttr());
  OutlineParamsArray &get_outline_params() { return outline_params_; }
  const OutlineParamsArray &get_outline_params() const { return outline_params_; }
  ObMaxConcurrentParam *get_outline_param(int64_t index) const;
  int64_t get_convert_size() const;
  bool is_empty() const { return 0 == outline_params_.count(); }
  int add_param(const ObMaxConcurrentParam& param);
  int has_param(const ObMaxConcurrentParam& param, bool &has_param) const;
  int has_concurrent_limit_param(bool &has) const;
  int64_t get_param_count() const {return outline_params_.count();}
  void reset_allocator() { allocator_ = NULL; mem_attr_ = common::ObMemAttr(); }
  void set_mem_attr(const common::ObMemAttr &attr) { mem_attr_ = attr; };
  TO_STRING_KV(K_(outline_params));
private:
  common::ObIAllocator *allocator_;
  OutlineParamsArray outline_params_;
  common::ObMemAttr mem_attr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOutlineParamsWrapper);
};

class ObOutlineInfo: public ObSchema
{
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
  { return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() < rhs->get_outline_id() : false; }
  static bool equal(const ObOutlineInfo *lhs, const ObOutlineInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() == rhs->get_outline_id() : false; }

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_outline_id(uint64_t id) { outline_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  int set_name(const char *name) { return deep_copy_str(name, name_); }
  int set_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  int set_signature(const char *sig) { return deep_copy_str(sig, signature_); }
  int set_signature(const common::ObString &sig) { return deep_copy_str(sig, signature_); }
  int set_sql_id(const char *sql_id) { return deep_copy_str(sql_id, sql_id_); }
  int set_sql_id(const common::ObString &sql_id) { return deep_copy_str(sql_id, sql_id_); }
  int set_outline_params(const common::ObString &outline_params_str);
  int set_outline_content(const char *content) { return deep_copy_str(content, outline_content_); }
  int set_outline_content(const common::ObString &content) { return deep_copy_str(content, outline_content_); }
  int set_sql_text(const char *sql) { return deep_copy_str(sql, sql_text_); }
  int set_sql_text(const common::ObString &sql) { return deep_copy_str(sql, sql_text_); }
  int set_outline_target(const char *target) { return deep_copy_str(target, outline_target_); }
  int set_outline_target(const common::ObString &target) { return deep_copy_str(target, outline_target_); }
  int set_owner(const char *owner) { return deep_copy_str(owner, owner_); }
  int set_owner(const common::ObString &owner) { return deep_copy_str(owner, owner_); }
  void set_owner_id(const uint64_t owner_id) { owner_id_ = owner_id; }
  void set_used(const bool used) {used_ = used;}
  int set_version(const char *version) { return deep_copy_str(version, version_); }
  int set_version(const common::ObString &version) { return deep_copy_str(version, version_); }
  void set_compatible(const bool compatible) { compatible_ = compatible;}
  void set_enabled(const bool enabled) { enabled_ = enabled;}
  void set_format(const ObHintFormat hint_format) { format_ = hint_format;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_owner_id() const { return owner_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_outline_id() const { return outline_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline bool is_used() const { return used_; }
  inline bool is_enabled() const { return enabled_; }
  inline bool is_compatible() const { return compatible_; }
  inline ObHintFormat get_hint_format() const { return format_; }
  inline const char *get_name() const { return extract_str(name_); }
  inline const common::ObString &get_name_str() const { return name_; }
  inline const char *get_signature() const { return extract_str(signature_); }
  inline const char *get_sql_id() const { return extract_str(sql_id_); }
  inline const common::ObString &get_sql_id_str() const { return sql_id_; }
  inline const common::ObString &get_signature_str() const { return signature_; }
  inline const char *get_outline_content() const { return extract_str(outline_content_); }
  inline const common::ObString &get_outline_content_str() const { return outline_content_; }
  inline const char *get_sql_text() const { return extract_str(sql_text_); }
  inline const common::ObString &get_sql_text_str() const { return sql_text_; }
  inline common::ObString &get_sql_text_str() { return sql_text_; }
  inline const char *get_outline_target() const { return extract_str(outline_target_); }
  inline const common::ObString &get_outline_target_str() const { return outline_target_; }
  inline common::ObString &get_outline_target_str() { return outline_target_; }
  inline const char *get_owner() const { return extract_str(owner_); }
  inline const common::ObString &get_owner_str() const { return owner_; }
  inline const char *get_version() const { return extract_str(version_); }
  inline const common::ObString &get_version_str() const { return version_; }
  int get_visible_signature(common::ObString &visiable_signature) const;
  int get_outline_sql(common::ObIAllocator &allocator, const sql::ObSQLSessionInfo &session, common::ObString &outline_sql) const;
  int get_hex_str_from_outline_params(common::ObString &hex_str, common::ObIAllocator &allocator) const;
  const ObOutlineParamsWrapper &get_outline_params_wrapper() const { return outline_params_wrapper_; }
  ObOutlineParamsWrapper &get_outline_params_wrapper() { return outline_params_wrapper_; }
  bool has_outline_params() const { return outline_params_wrapper_.get_outline_params().count() > 0; }
  int has_concurrent_limit_param(bool &has) const;
  int gen_valid_allocator();
  int add_param(const ObMaxConcurrentParam& src_param);
  static int gen_limit_sql(const common::ObString &visible_signature,
                           const ObMaxConcurrentParam *param,
                           const sql::ObSQLSessionInfo &session,
                           common::ObIAllocator &allocator,
                           common::ObString &limit_sql);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(database_id), K_(outline_id), K_(schema_version),
                       K_(name), K_(signature), K_(sql_id), K_(outline_content), K_(sql_text),
                       K_(owner_id), K_(owner), K_(used), K_(compatible),
                       K_(enabled), K_(format), K_(outline_params_wrapper), K_(outline_target));
  static bool is_sql_id_valid(const common::ObString &sql_id);
private:
  static int replace_question_mark(const common::ObString &not_param_sql,
                                   const ObMaxConcurrentParam &concurrent_param,
                                   int64_t start_pos,
                                   int64_t cur_pos,
                                   int64_t &question_mark_offset,
                                   common::ObSqlString &string_helper);
  static int replace_not_param(const common::ObString &not_param_sql,
                               const ParseNode &node,
                               int64_t start_pos,
                               int64_t cur_pos,
                               common::ObSqlString &string_helper);

protected:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t outline_id_;
  int64_t schema_version_; //the last modify timestamp of this version
  common::ObString name_;//outline name
  common::ObString signature_;// SQL after constant parameterization
  common::ObString sql_id_; // Used to directly determine sql_id
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

class ObDbLinkBaseInfo : public ObSchema
{
public:
  ObDbLinkBaseInfo() : ObSchema() { reset(); }
  explicit ObDbLinkBaseInfo(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  virtual ~ObDbLinkBaseInfo() {}

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_owner_id(const uint64_t id) { owner_id_ = id; }
  inline void set_dblink_id(const uint64_t id) { dblink_id_ = id; }
  inline void set_schema_version(const int64_t version) { schema_version_ = version; }
  inline int set_dblink_name(const common::ObString &name) { return deep_copy_str(name, dblink_name_); }
  inline int set_cluster_name(const common::ObString &name) { return deep_copy_str(name, cluster_name_); }
  inline int set_tenant_name(const common::ObString &name) { return deep_copy_str(name, tenant_name_); }
  inline int set_user_name(const common::ObString &name) { return deep_copy_str(name, user_name_); }
  inline int set_password(const common::ObString &password) { return deep_copy_str(password, password_); }
  inline int set_encrypted_password(const common::ObString &encrypted_password) { return deep_copy_str(encrypted_password, encrypted_password_); }
  int do_encrypt_password();
  int set_plain_password(const common::ObString &plain_password) { return deep_copy_str(plain_password, plain_password_); }
  inline void set_host_addr(const common::ObAddr &addr) { host_addr_ = addr; }
  int set_host_ip(const common::ObString &host_ip);
  inline void set_host_port(const int32_t host_port) { host_addr_.set_port(host_port); }
  inline int set_database_name(const common::ObString &name) { return deep_copy_str(name, database_name_); }
  inline int set_reverse_cluster_name(const common::ObString &name) { return deep_copy_str(name, reverse_cluster_name_); }
  inline int set_reverse_tenant_name(const common::ObString &name) { return deep_copy_str(name, reverse_tenant_name_); }
  inline int set_reverse_user_name(const common::ObString &name) { return deep_copy_str(name, reverse_user_name_); }
  inline int set_reverse_password(const common::ObString &password) { return deep_copy_str(password, reverse_password_); }
  int do_encrypt_reverse_password();
  inline int set_plain_reverse_password(const common::ObString &password) { return deep_copy_str(password, plain_reverse_password_); }
  inline void set_reverse_host_addr(const common::ObAddr &addr) { reverse_host_addr_ = addr; }
  inline int set_reverse_host_ip(const common::ObString &host_ip) { reverse_host_addr_.set_ip_addr(host_ip, 0); return common::OB_SUCCESS; }
  inline void set_reverse_host_port(const int32_t host_port) { reverse_host_addr_.set_port(host_port); }
  inline int set_authusr(const common::ObString &str) { return deep_copy_str(str, authusr_); }
  inline int set_authpwd(const common::ObString &str) { return deep_copy_str(str, authpwd_); }
  inline int set_passwordx(const common::ObString &str) { return deep_copy_str(str, passwordx_); }
  inline int set_authpwdx(const common::ObString &str) { return deep_copy_str(str, authpwdx_); }
  inline const common::ObString &get_authusr() const { return authusr_; }
  inline const common::ObString &get_authpwd() const { return authpwd_; }
  inline const common::ObString &get_passwordx() const { return passwordx_; }
  inline const common::ObString &get_authpwdx() const { return authpwdx_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_owner_id() const { return owner_id_; }
  inline uint64_t get_dblink_id() const { return dblink_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const common::ObString &get_dblink_name() const { return dblink_name_; }
  inline const common::ObString &get_cluster_name() const { return cluster_name_; }
  inline const common::ObString &get_tenant_name() const { return tenant_name_; }
  inline const common::ObString &get_user_name() const { return user_name_; }
  inline const common::ObString &get_password() const { return password_; }
  inline const common::ObString &get_encrypted_password() const { return  encrypted_password_; }
  int do_decrypt_password();
  inline const common::ObString &get_plain_password() const { return plain_password_; }
  inline const common::ObAddr &get_host_addr() const { return host_addr_; }
  inline int32_t get_host_port() const { return host_addr_.get_port(); }
  inline const common::ObString &get_database_name() const { return database_name_; }
  inline const common::ObString &get_reverse_cluster_name() const { return reverse_cluster_name_; }
  inline const common::ObString &get_reverse_tenant_name() const { return reverse_tenant_name_; }
  inline const common::ObString &get_reverse_user_name() const { return reverse_user_name_; }
  inline const common::ObString &get_reverse_password() const { return reverse_password_; }
  int do_decrypt_reverse_password();
  inline const common::ObString &get_plain_reverse_password() const { return plain_reverse_password_; }
  inline const common::ObAddr &get_reverse_host_addr() const { return reverse_host_addr_; }
  inline int32_t get_reverse_host_port() const { return reverse_host_addr_.get_port(); }


  inline int64_t get_driver_proto() const { return driver_proto_; }
  inline void set_driver_proto(int64_t proto) { driver_proto_ = proto; }
  inline bool get_if_not_exist() const { return if_not_exist_; }
  inline void set_if_not_exist(bool value) { if_not_exist_ = value; }
  inline int64_t get_flag() const { return flag_; }
  inline void set_flag(int64_t flag) { flag_ = flag; }
  inline const common::ObString &get_service_name() const { return service_name_; }
  inline const common::ObString &get_conn_string() const { return conn_string_; }
  inline int set_service_name(const common::ObString &str) { return deep_copy_str(str, service_name_); }
  inline int set_conn_string(const common::ObString &str) { return deep_copy_str(str, conn_string_); }
  int get_ora_conn_string(common::ObString &conn_str);

  void reset();
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(owner_id), K_(if_not_exist), K_(dblink_id), K_(dblink_name),
                       K_(cluster_name), K_(tenant_name), K_(database_name), K_(user_name),
                       K_(host_addr),
                       K_(driver_proto), K_(flag), K_(service_name), K_(conn_string),
                       K_(authusr), K_(encrypted_password),
                       K_(reverse_cluster_name),
                       K_(reverse_tenant_name), K_(reverse_user_name),
                       K_(reverse_password), K_(plain_reverse_password),
                       K_(reverse_host_addr));
private:
  int dblink_encrypt(common::ObString &src, common::ObString &dst);
  int dblink_decrypt(common::ObString &src, common::ObString &dst);
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
  int64_t driver_proto_; // type of dblink, ob2ob, ob2oracle
  int64_t flag_; // flag of dblink;
  common::ObString service_name_; // oracle instance name, ex: SID=ORCL, 128
  common::ObString conn_string_; // full connection string.  32767 OB_MAX_ORACLE_VARCHAR_LENGTH
  common::ObString authusr_; // 128
  common::ObString authpwd_; // 128
  common::ObString passwordx_; // 128
  common::ObString authpwdx_; //128
  common::ObString plain_password_;
  common::ObString encrypted_password_;
  common::ObString reverse_cluster_name_;
  common::ObString reverse_tenant_name_; // used for reverse dblink
  common::ObString reverse_user_name_; // used for reverse dblink
  common::ObString reverse_password_;  // used for reverse dblink
  common::ObString plain_reverse_password_; // used for reverse dblink
  common::ObAddr reverse_host_addr_;  // used for reverse dblink
  common::ObString database_name_; // used for mysql dblink
  bool if_not_exist_; // used for mysql dblink
};

struct ObTenantDbLinkId
{
public:
  ObTenantDbLinkId()
    : tenant_id_(common::OB_INVALID_ID),
      dblink_id_(common::OB_INVALID_ID)
  {}
  ObTenantDbLinkId(uint64_t tenant_id, uint64_t dblink_id)
    : tenant_id_(tenant_id),
      dblink_id_(dblink_id)
  {}
  ObTenantDbLinkId(const ObTenantDbLinkId &other)
    : tenant_id_(other.tenant_id_),
      dblink_id_(other.dblink_id_)
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
    return (tenant_id_ < rhs.tenant_id_) ||
           (tenant_id_ == rhs.tenant_id_ && dblink_id_ < rhs.dblink_id_);
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
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_dblink_id() const { return dblink_id_; }
  TO_STRING_KV(K_(tenant_id), K_(dblink_id));
private:
  uint64_t tenant_id_;
  uint64_t dblink_id_;
};

class ObDbLinkInfo : public ObDbLinkBaseInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDbLinkInfo() : ObDbLinkBaseInfo() {}
  explicit ObDbLinkInfo(common::ObIAllocator *allocator) : ObDbLinkBaseInfo(allocator) {}
  virtual ~ObDbLinkInfo() {}
};

class ObDbLinkSchema : public ObDbLinkBaseInfo
{
public:
  ObDbLinkSchema() : ObDbLinkBaseInfo() {}
  explicit ObDbLinkSchema(common::ObIAllocator *allocator) : ObDbLinkBaseInfo(allocator) {}
  ObDbLinkSchema(const ObDbLinkSchema &src_schema);
  virtual ~ObDbLinkSchema() {}
  ObDbLinkSchema &operator=(const ObDbLinkSchema &other);
  int assign(const ObDbLinkSchema &other)
  {
    *this = other;
    return get_err_ret();
  }
  bool operator==(const ObDbLinkSchema &other) const;

  inline ObTenantDbLinkId get_tenant_dblink_id() const { return ObTenantDbLinkId(tenant_id_, dblink_id_); }
  inline int64_t get_convert_size() const
  {
    return sizeof(ObDbLinkSchema)
         + dblink_name_.length() + 1
         + cluster_name_.length() + 1
         + tenant_name_.length() + 1
         + user_name_.length() + 1
         + password_.length() + 1
         + service_name_.length() + 1
         + conn_string_.length() + 1
         + authusr_.length() + 1
         + authpwd_.length() + 1
         + passwordx_.length() + 1
         + authpwdx_.length() + 1
         + plain_password_.length() + 1
         + encrypted_password_.length() + 1
         + reverse_cluster_name_.length() + 1
         + reverse_tenant_name_.length() + 1
         + reverse_user_name_.length() + 1
         + reverse_password_.length() + 1
         + plain_reverse_password_.length() + 1
         + database_name_.length() + 1;
  }
};

class ObSynonymInfo: public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSynonymInfo();
  explicit ObSynonymInfo(common::ObIAllocator *allocator);
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(database_id), K_(synonym_id), K_(schema_version), K_(status));
  virtual ~ObSynonymInfo();
  ObSynonymInfo &operator=(const ObSynonymInfo &src_schema);
  ObSynonymInfo(const ObSynonymInfo &src_schema);
  int assign(const ObSynonymInfo &src_schema);
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_object_database_id(const uint64_t id) { object_db_id_ = id; }
  inline void set_synonym_id(uint64_t id) { synonym_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  int64_t get_convert_size() const;
  int set_synonym_name(const char *name) { return deep_copy_str(name, name_); }
  int set_synonym_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  int set_object_name(const char *name) { return deep_copy_str(name, object_name_); }
  int set_object_name(const common::ObString &name) { return deep_copy_str(name, object_name_); }
  int set_version(const char *version) { return deep_copy_str(version, version_); }
  int set_version(const common::ObString &version) { return deep_copy_str(version, version_); }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_synonym_id() const { return synonym_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_synonym_name() const { return extract_str(name_); }
  inline const common::ObString &get_synonym_name_str() const { return name_; }
  inline const char *get_object_name() const { return extract_str(object_name_); }
  inline const common::ObString &get_object_name_str() const { return object_name_; }
  inline const char *get_version() const { return extract_str(version_); }
  inline const common::ObString &get_version_str() const { return version_; }
  inline uint64_t get_object_database_id() const { return object_db_id_; }
  inline ObObjectStatus get_status() const { return status_; }
  inline void set_status(const ObObjectStatus status) { status_ = status; }
  inline void set_status(const int64_t status) { status_ = static_cast<ObObjectStatus> (status); }
  void reset();
private:
  //void *alloc(int64_t size);
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t synonym_id_;
  int64_t schema_version_; //the last modify timestamp of this version
  common::ObString name_;//synonym name
  common::ObString version_;
  common::ObString object_name_;
  uint64_t object_db_id_;
  ObObjectStatus status_;
  //common::ObArenaAllocator allocator_;
};

struct ObTenantUDFId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantUDFId()
      : tenant_id_(common::OB_INVALID_ID), udf_name_()
  {}
  ObTenantUDFId(const uint64_t tenant_id, const common::ObString &name)
      : tenant_id_(tenant_id), udf_name_(name)
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
    return (tenant_id_ != common::OB_INVALID_ID) && (udf_name_.length() !=0);
  }
  TO_STRING_KV(K_(tenant_id), K_(udf_name));
  uint64_t tenant_id_;
  common::ObString udf_name_;
};


struct ObTenantSynonymId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantSynonymId()
      : tenant_id_(common::OB_INVALID_ID), synonym_id_(common::OB_INVALID_ID)
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

struct ObTenantSequenceId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantSequenceId()
      : tenant_id_(common::OB_INVALID_ID), sequence_id_(common::OB_INVALID_ID)
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

struct ObTenantKeystoreId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantKeystoreId()
      : tenant_id_(common::OB_INVALID_ID), keystore_id_(common::OB_INVALID_ID)
  {}
  ObTenantKeystoreId(const uint64_t tenant_id, const uint64_t keystore_id)
      : tenant_id_(tenant_id), keystore_id_(keystore_id)
  {}
  bool operator==(const ObTenantKeystoreId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (keystore_id_ == rhs.keystore_id_);
  }
  bool operator!=(const ObTenantKeystoreId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantKeystoreId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = keystore_id_ < rhs.keystore_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&keystore_id_, sizeof(keystore_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (keystore_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(keystore_id));
  uint64_t tenant_id_;
  uint64_t keystore_id_;
};

struct ObTenantLabelSePolicyId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantLabelSePolicyId()
      : tenant_id_(common::OB_INVALID_TENANT_ID), label_se_policy_id_(common::OB_INVALID_ID)
  {}
  ObTenantLabelSePolicyId(const uint64_t tenant_id, const uint64_t label_se_policy_id)
      : tenant_id_(tenant_id), label_se_policy_id_(label_se_policy_id)
  {}
  bool operator==(const ObTenantLabelSePolicyId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (label_se_policy_id_ == rhs.label_se_policy_id_);
  }
  bool operator!=(const ObTenantLabelSePolicyId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantLabelSePolicyId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = label_se_policy_id_ < rhs.label_se_policy_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&label_se_policy_id_, sizeof(label_se_policy_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_TENANT_ID) && (label_se_policy_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(label_se_policy_id));
  uint64_t tenant_id_;
  uint64_t label_se_policy_id_;
};

struct ObTenantLabelSeComponentId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantLabelSeComponentId()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        label_se_component_id_(common::OB_INVALID_ID)
  {}
  ObTenantLabelSeComponentId(const uint64_t tenant_id, const uint64_t label_se_component_id)
      : tenant_id_(tenant_id), label_se_component_id_(label_se_component_id)
  {}
  ObTenantLabelSeComponentId(const ObTenantLabelSeComponentId &other)
  {
    *this = other;
  }
  bool operator==(const ObTenantLabelSeComponentId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
        && (label_se_component_id_ == rhs.label_se_component_id_);
  }
  bool operator!=(const ObTenantLabelSeComponentId &rhs) const
  {
    return !(*this == rhs);
  }
  void operator =(const ObTenantLabelSeComponentId &rhs)
  {
    tenant_id_ = rhs.tenant_id_;
    label_se_component_id_ = rhs.label_se_component_id_;
  }
  bool operator<(const ObTenantLabelSeComponentId &rhs) const
  {
    bool bret = false;
    if (tenant_id_ != rhs.tenant_id_) {
      bret = tenant_id_ < rhs.tenant_id_;
    } else {
      bret = label_se_component_id_ < rhs.label_se_component_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&label_se_component_id_, sizeof(label_se_component_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_TENANT_ID)
        && (label_se_component_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(label_se_component_id));
  uint64_t tenant_id_;
  uint64_t label_se_component_id_;
};

struct ObTenantLabelSeLabelId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantLabelSeLabelId()
      : tenant_id_(common::OB_INVALID_TENANT_ID), label_se_label_id_(common::OB_INVALID_ID)
  {}
  ObTenantLabelSeLabelId(const uint64_t tenant_id, const uint64_t label_se_label_id)
      : tenant_id_(tenant_id), label_se_label_id_(label_se_label_id)
  {}
  bool operator==(const ObTenantLabelSeLabelId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
        && (label_se_label_id_ == rhs.label_se_label_id_);
  }
  bool operator!=(const ObTenantLabelSeLabelId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantLabelSeLabelId &rhs) const
  {
    bool bret = false;
    if (tenant_id_ != rhs.tenant_id_) {
      bret = tenant_id_ < rhs.tenant_id_;
    } else {
      bret = label_se_label_id_ < rhs.label_se_label_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&label_se_label_id_, sizeof(label_se_label_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_TENANT_ID)
        && (label_se_label_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(label_se_label_id));
  uint64_t tenant_id_;
  uint64_t label_se_label_id_;
};

struct ObTenantLabelSeUserLevelId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantLabelSeUserLevelId()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        label_se_user_level_id_(common::OB_INVALID_ID)
  {}
  ObTenantLabelSeUserLevelId(const uint64_t tenant_id, const uint64_t label_se_user_level_id)
      : tenant_id_(tenant_id), label_se_user_level_id_(label_se_user_level_id)
  {}
  bool operator==(const ObTenantLabelSeUserLevelId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
        && (label_se_user_level_id_ == rhs.label_se_user_level_id_);
  }
  bool operator!=(const ObTenantLabelSeUserLevelId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantLabelSeUserLevelId &rhs) const
  {
    bool bret = false;
    if (tenant_id_ != rhs.tenant_id_) {
      bret = tenant_id_ < rhs.tenant_id_;
    } else {
      bret = label_se_user_level_id_ < rhs.label_se_user_level_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&label_se_user_level_id_, sizeof(label_se_user_level_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_TENANT_ID)
        && (label_se_user_level_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(label_se_user_level_id));
  uint64_t tenant_id_;
  uint64_t label_se_user_level_id_;
};

class ObAlterOutlineInfo : public ObOutlineInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAlterOutlineInfo() : ObOutlineInfo(), alter_option_bitset_() {}
  virtual ~ObAlterOutlineInfo() {}
  void reset()
  {
    ObOutlineInfo::reset();
    alter_option_bitset_.reset();
  }
  const common::ObBitSet<> &get_alter_option_bitset() const { return alter_option_bitset_; }
  common::ObBitSet<> &get_alter_option_bitset() { return alter_option_bitset_; }
  INHERIT_TO_STRING_KV("ObOutlineInfo", ObOutlineInfo, K_(alter_option_bitset));
private:
  common::ObBitSet<> alter_option_bitset_;
};

class ObOutlineNameHashWrapper
{
public:
  ObOutlineNameHashWrapper() : tenant_id_(common::OB_INVALID_ID),
                               database_id_(common::OB_INVALID_ID),
                               name_() {}
  ObOutlineNameHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                           const common::ObString &name_)
      : tenant_id_(tenant_id), database_id_(database_id), name_(name_)
  {}
  ~ObOutlineNameHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObOutlineNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_name(const common::ObString &name) { name_ = name;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_name() const { return name_; }
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

inline bool ObOutlineNameHashWrapper::operator ==(const ObOutlineNameHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (name_ == rv.name_);
}

class ObOutlineSignatureHashWrapper
{
public:
  ObOutlineSignatureHashWrapper() : tenant_id_(common::OB_INVALID_ID),
                                    database_id_(common::OB_INVALID_ID),
                                    signature_() {}
  ObOutlineSignatureHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                                const common::ObString &signature)
      : tenant_id_(tenant_id), database_id_(database_id), signature_(signature)
  {}
  ~ObOutlineSignatureHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObOutlineSignatureHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_signature(const common::ObString &signature) { signature_ = signature;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_signature() const { return signature_; }
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString signature_;
};

class ObOutlineSqlIdHashWrapper
{
public:
  ObOutlineSqlIdHashWrapper() : tenant_id_(common::OB_INVALID_ID),
                                    database_id_(common::OB_INVALID_ID),
                                    sql_id_() {}
  ObOutlineSqlIdHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                                const common::ObString &sql_id)
      : tenant_id_(tenant_id), database_id_(database_id), sql_id_(sql_id)
  {}
  ~ObOutlineSqlIdHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObOutlineSqlIdHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_sql_id(const common::ObString &sql_id) { sql_id_ = sql_id;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_sql_id() const { return sql_id_; }
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

inline bool ObOutlineSqlIdHashWrapper::operator ==(const ObOutlineSqlIdHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
      && (sql_id_ == rv.sql_id_);
}

inline uint64_t ObOutlineSignatureHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(signature_.ptr(), signature_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineSignatureHashWrapper::operator ==(const ObOutlineSignatureHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
      && (signature_ == rv.signature_);
}

class ObSysTableChecker
{
private:
  ObSysTableChecker();
  ~ObSysTableChecker();

  int init_tenant_space_table_id_map();
  int init_sys_table_name_map();
  int check_tenant_space_table_id(
      const uint64_t table_id,
      bool &is_tenant_space_table);
  int check_sys_table_name(
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &table_name,
      bool &is_tenant_space_table);
  int check_inner_table_exist(
      const uint64_t tenant_id,
      const share::schema::ObSimpleTableSchemaV2 &table,
      bool &exist);
  int ob_write_string(
      const common::ObString &src,
      common::ObString &dst);
  static int append_table_(
             const uint64_t tenant_id,
             share::schema::ObTableSchema &index_schema,
             common::ObIArray<share::schema::ObTableSchema> &tables);
public:
  static ObSysTableChecker &instance();
  int init();
  int destroy();

  int64_t get_tenant_space_sys_table_num() { return tenant_space_sys_table_num_; }

  static int is_sys_table_name(
             const uint64_t tenant_id,
             const uint64_t database_id,
             const common::ObString &table_name,
             bool &is_tenant_space_table);

  static int is_tenant_space_table_id(
             const uint64_t table_id,
             bool &is_tenant_space_table);
  static int is_inner_table_exist(
             const uint64_t tenant_id,
             const share::schema::ObSimpleTableSchemaV2 &table,
             bool &exist);

  static bool is_cluster_private_tenant_table(const uint64_t table_id);

  static bool is_sys_table_index_tid(const int64_t index_id);
  static bool is_sys_table_has_index(const int64_t table_id);
  static int fill_sys_index_infos(share::schema::ObTableSchema &table);
  static int get_sys_table_index_tids(const int64_t table_id, common::ObIArray<uint64_t> &index_tids);
  static int append_sys_table_index_schemas(
             const uint64_t tenant_id,
             const uint64_t data_table_id,
             common::ObIArray<share::schema::ObTableSchema> &tables);
  static int add_sys_table_index_ids(
             const uint64_t tenant_id,
             common::ObIArray<uint64_t> &table_ids);
public:
  class TableNameWrapper
  {
  public:
    TableNameWrapper()
        : database_id_(common::OB_INVALID_ID),
          name_case_mode_(common::OB_NAME_CASE_INVALID),
          table_name_()
    {
    }
    TableNameWrapper(const uint64_t database_id,
                     const common::ObNameCaseMode mode,
                     const common::ObString &table_name)
        : database_id_(database_id), name_case_mode_(mode), table_name_(table_name)
    {
    }
    ~TableNameWrapper() {}
    inline uint64_t hash() const;
    bool operator ==(const TableNameWrapper &rv) const;
    TO_STRING_KV(K_(database_id), K_(name_case_mode), K_(table_name));
  private:
    uint64_t database_id_; //pure_database_id
    common::ObNameCaseMode name_case_mode_;
    common::ObString table_name_;
  };
  typedef common::ObSEArray<TableNameWrapper, 2> TableNameWrapperArray;
private:
  static const int64_t TABLE_BUCKET_NUM = 300;
private:
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> tenant_space_table_id_map_;
  common::hash::ObHashMap<uint64_t, TableNameWrapperArray*, common::hash::NoPthreadDefendMode> sys_table_name_map_;
  int64_t tenant_space_sys_table_num_;  // Number of tenant-level system tables (including system table indexes)
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTableChecker);
};

class ObTableSchema;
class ObRecycleObject : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  enum RecycleObjType{
    INVALID  = 0,
    TABLE    = 1,
    INDEX    = 2,
    VIEW     = 3,
    DATABASE = 4,
    AUX_VP   = 5,
    TRIGGER  = 6,
    TENANT   = 7,
    AUX_LOB_META = 8,
    AUX_LOB_PIECE = 9,
  };
  ObRecycleObject(common::ObIAllocator *allocator);
  ObRecycleObject() : ObSchema(), tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      tablegroup_id_(common::OB_INVALID_ID),
      object_name_(), original_name_(), type_(INVALID),
      tablegroup_name_(), database_name_() {}
  ObRecycleObject(const ObRecycleObject &recycle_obj);
  ObRecycleObject &operator=(const ObRecycleObject &recycle_obj);
  virtual ~ObRecycleObject() {}
  inline bool is_valid() const;
  virtual void reset();

  uint64_t get_tenant_id() const { return tenant_id_;}
  uint64_t get_database_id() const { return database_id_; }
  uint64_t get_table_id() const  { return table_id_; }
  uint64_t get_tablegroup_id() const { return tablegroup_id_; }
  const common::ObString &get_object_name() const { return object_name_; }
  const common::ObString &get_original_name() const { return original_name_; }
  RecycleObjType get_type() const { return type_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_database_id(const uint64_t db_id) { database_id_ = db_id; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_tablegroup_id(const uint64_t tablegroup_id) { tablegroup_id_ = tablegroup_id; }
  int set_object_name(const common::ObString &object_name)
    { return deep_copy_str(object_name, object_name_);}
  int set_original_name(const common::ObString &original_name)
    { return deep_copy_str(original_name, original_name_);}
  void set_type(const RecycleObjType type) { type_ = type; }
  int set_type_by_table_schema(const ObSimpleTableSchemaV2 &table_schema);
  static RecycleObjType get_type_by_table_schema(const ObSimpleTableSchemaV2 &table_schema);
  //for backup
  int set_tablegroup_name(const common::ObString &tablegroup_name)
    { return deep_copy_str(tablegroup_name, tablegroup_name_); }
  const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
  int set_database_name(const common::ObString &database_name)
  { return deep_copy_str(database_name, database_name_); }
  const common::ObString &get_database_name() const { return database_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_id), K_(tablegroup_id),
               K_(object_name), K_(original_name), K_(type), K_(tablegroup_name), K_(database_name));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  uint64_t tablegroup_id_;
  common::ObString object_name_;
  common::ObString original_name_;
  RecycleObjType type_;
  //for backup
  common::ObString tablegroup_name_;
  common::ObString database_name_;
};

inline bool ObRecycleObject::is_valid() const
{
  return INVALID != type_ && !object_name_.empty() && !original_name_.empty();
}

typedef common::hash::ObPlacementHashSet<uint64_t, common::OB_MAX_TABLE_NUM_PER_STMT> DropTableIdHashSet;
// Used to count vertical partition columns
typedef common::hash::ObPlacementHashSet<common::ObString, common::OB_MAX_USER_DEFINED_COLUMNS_COUNT> VPColumnNameHashSet;

struct ObBasedSchemaObjectInfo
{
  OB_UNIS_VERSION(1);
public:
  ObBasedSchemaObjectInfo()
    : schema_id_(common::OB_INVALID_ID),
      schema_type_(OB_MAX_SCHEMA),
      schema_version_(common::OB_INVALID_VERSION),
      schema_tenant_id_(OB_INVALID_TENANT_ID)
  {}
  ObBasedSchemaObjectInfo(
      const uint64_t schema_id,
      const ObSchemaType schema_type,
      const int64_t schema_version,
      const uint64_t schema_tenant_id = OB_INVALID_TENANT_ID)
      : schema_id_(schema_id),
        schema_type_(schema_type),
        schema_version_(schema_version),
        schema_tenant_id_(schema_tenant_id)
  {}
  bool operator ==(const ObBasedSchemaObjectInfo &other) const {
    return (schema_id_ == other.schema_id_
            && schema_type_ == other.schema_type_
            && schema_version_ == other.schema_version_
            && schema_tenant_id_ == other.schema_tenant_id_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    return convert_size;
  }
  TO_STRING_KV(K_(schema_id), K_(schema_type), K_(schema_version), K_(schema_tenant_id));
  uint64_t schema_id_;
  ObSchemaType schema_type_;
  int64_t schema_version_;
  uint64_t schema_tenant_id_;
};


struct ObAuxTableMetaInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAuxTableMetaInfo()
    : table_id_(common::OB_INVALID_ID),
      table_type_(MAX_TABLE_TYPE),
      index_type_(INDEX_TYPE_MAX)
  {}
  ObAuxTableMetaInfo(
      const uint64_t table_id,
      const ObTableType table_type,
      const ObIndexType index_type)
      : table_id_(table_id),
        table_type_(table_type),
        index_type_(index_type)
  {}
  bool operator ==(const ObAuxTableMetaInfo &other) const {
    return (table_id_ == other.table_id_
            && table_type_ == other.table_type_
            && index_type_ == other.index_type_);
  }
  int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    return convert_size;
  }
  TO_STRING_KV(K_(table_id), K_(table_type), K_(index_type));
  uint64_t table_id_;
  ObTableType table_type_;
  ObIndexType index_type_;
};

enum ObConstraintType
{
  CONSTRAINT_TYPE_INVALID = 0,
  CONSTRAINT_TYPE_PRIMARY_KEY = 1,
  CONSTRAINT_TYPE_UNIQUE_KEY = 2,
  CONSTRAINT_TYPE_CHECK = 3,
  CONSTRAINT_TYPE_NOT_NULL = 4,
  CONSTRAINT_TYPE_MAX,
};

enum ObNameGeneratedType
{
  GENERATED_TYPE_UNKNOWN = 0,
  GENERATED_TYPE_USER = 1,
  GENERATED_TYPE_SYSTEM = 2
};

enum ObReferenceAction
{
  ACTION_INVALID = 0,
  ACTION_RESTRICT,
  ACTION_CASCADE,
  ACTION_SET_NULL,
  ACTION_NO_ACTION,
  ACTION_SET_DEFAULT,   // no use now
  ACTION_CHECK_EXIST,   // not a valid option, just used for construct dml stmt.
  ACTION_MAX
};

enum ObCstFkValidateFlag
{
  CST_FK_NO_VALIDATE = 0,
  CST_FK_VALIDATED,
  CST_FK_VALIDATING,
  CST_FK_MAX,
};

class ObForeignKeyInfo
{
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
      validate_flag_(CST_FK_VALIDATED),
      is_modify_validate_flag_(false),
      rely_flag_(false),
      is_modify_rely_flag_(false),
      is_modify_fk_state_(false),
      ref_cst_type_(CONSTRAINT_TYPE_INVALID),
      ref_cst_id_(common::OB_INVALID_ID),
      is_modify_fk_name_flag_(false),
      is_parent_table_mock_(false),
      name_generated_type_(GENERATED_TYPE_UNKNOWN)
  {}
  ObForeignKeyInfo(common::ObIAllocator *allocator);
  virtual ~ObForeignKeyInfo() {}
public:
  inline void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  inline void set_foreign_key_id(uint64_t foreign_key_id) { foreign_key_id_ = foreign_key_id; }
  inline void set_child_table_id(uint64_t child_table_id) { child_table_id_ = child_table_id; }
  inline void set_parent_table_id(uint64_t parent_table_id) { parent_table_id_ = parent_table_id; }
  inline void set_update_action(uint64_t update_action) { update_action_ = static_cast<ObReferenceAction>(update_action); }
  inline void set_delete_action(uint64_t delete_action) { delete_action_ = static_cast<ObReferenceAction>(delete_action); }
  inline int set_foreign_key_name(const common::ObString &foreign_key_name) { foreign_key_name_ = foreign_key_name; return common::OB_SUCCESS; }
  inline void set_enable_flag(const bool enable_flag) { enable_flag_ = enable_flag; }
  inline void set_is_modify_enable_flag(const bool is_modify_enable_flag) { is_modify_enable_flag_ = is_modify_enable_flag; }
  inline void set_validate_flag(const ObCstFkValidateFlag validate_flag) { validate_flag_ = validate_flag; }
  inline void set_is_modify_validate_flag(const bool is_modify_validate_flag) { is_modify_validate_flag_ = is_modify_validate_flag; }
  inline void set_rely_flag(const bool rely_flag) { rely_flag_ = rely_flag; }
  inline void set_is_modify_rely_flag(const bool is_modify_rely_flag) { is_modify_rely_flag_ = is_modify_rely_flag; }
  inline void set_is_modify_fk_state(const bool is_modify_fk_state) { is_modify_fk_state_ = is_modify_fk_state; }
  inline void set_is_modify_fk_name_flag(const bool is_modify_fk_name_flag) { is_modify_fk_name_flag_ = is_modify_fk_name_flag; }
  inline void set_is_parent_table_mock(const bool is_parent_table_mock) { is_parent_table_mock_ = is_parent_table_mock; }
  inline void set_ref_cst_type(ObConstraintType ref_cst_type) { ref_cst_type_ = ref_cst_type; }
  inline void set_ref_cst_id(uint64_t ref_cst_id) { ref_cst_id_ = ref_cst_id; }
  inline bool is_no_validate() const { return CST_FK_NO_VALIDATE == validate_flag_; }
  inline bool is_validated() const { return CST_FK_VALIDATED == validate_flag_; }
  inline bool is_validating() const { return CST_FK_VALIDATING == validate_flag_; }
  inline const char *get_update_action_str() const { return get_action_str(update_action_); }
  inline const char *get_delete_action_str() const { return get_action_str(delete_action_); }
  inline const char *get_action_str(ObReferenceAction action) const
  {
    const char *ret = NULL;
    if (ACTION_RESTRICT <= action && action <= ACTION_SET_DEFAULT) {
      ret = reference_action_str_[action];
    }
    return ret;
  }

  int get_child_column_id(const uint64_t parent_column_id, uint64_t &child_column_id) const;
  int get_parent_column_id(const uint64_t child_column_id, uint64_t &parent_column_id) const;
  inline void set_name_generated_type(const ObNameGeneratedType is_sys_generated) {
    name_generated_type_ = is_sys_generated;
  }
  inline ObNameGeneratedType get_name_generated_type() const { return name_generated_type_; }
  bool is_sys_generated_name(bool check_unknown) const;
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
    validate_flag_ = CST_FK_VALIDATED;
    is_modify_validate_flag_ = false;
    rely_flag_ = false;
    is_modify_rely_flag_ = false;
    is_modify_fk_state_ = false;
    ref_cst_type_ = CONSTRAINT_TYPE_INVALID;
    ref_cst_id_ = common::OB_INVALID_ID;
    is_modify_fk_name_flag_ = false;
    is_parent_table_mock_ = false;
    name_generated_type_ = GENERATED_TYPE_UNKNOWN;
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
  TO_STRING_KV(K_(table_id), K_(foreign_key_id),
               K_(child_table_id), K_(parent_table_id),
               K_(child_column_ids), K_(parent_column_ids),
               K_(update_action), K_(delete_action),
               K_(foreign_key_name), K_(enable_flag), K_(is_modify_enable_flag),
               K_(validate_flag), K_(is_modify_validate_flag),
               K_(rely_flag), K_(is_modify_rely_flag), K_(is_modify_fk_state),
               K_(ref_cst_type), K_(ref_cst_id), K_(is_modify_fk_name_flag), K_(is_parent_table_mock),
               K_(name_generated_type));

public:
  uint64_t table_id_;   // table_id is not in __all_foreign_key.
  uint64_t foreign_key_id_;
  uint64_t child_table_id_;
  uint64_t parent_table_id_;
  common::ObSEArray<uint64_t, 4> child_column_ids_;
  common::ObSEArray<uint64_t, 4> parent_column_ids_;
  ObReferenceAction update_action_;
  ObReferenceAction delete_action_;
  common::ObString foreign_key_name_;
  bool enable_flag_;
  bool is_modify_enable_flag_;
  ObCstFkValidateFlag validate_flag_;
  bool is_modify_validate_flag_;
  bool rely_flag_;
  bool is_modify_rely_flag_;
  bool is_modify_fk_state_;
  ObConstraintType ref_cst_type_;
  uint64_t ref_cst_id_;
  bool is_modify_fk_name_flag_;
  bool is_parent_table_mock_;
  ObNameGeneratedType name_generated_type_;
private:
  static const char *reference_action_str_[ACTION_MAX + 1];
};

struct ObSimpleForeignKeyInfo
{
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
  ObSimpleForeignKeyInfo(const uint64_t tenant_id, const uint64_t database_id,
                         const uint64_t table_id, const common::ObString &foreign_key_name,
                         const uint64_t foreign_key_id)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        table_id_(table_id),
        foreign_key_name_(foreign_key_name),
        foreign_key_id_(foreign_key_id)
  {}
  bool operator ==(const ObSimpleForeignKeyInfo &other) const {
    return (tenant_id_ == other.tenant_id_
        && database_id_ == other.database_id_
        && table_id_ == other.table_id_
        && foreign_key_name_ == other.foreign_key_name_
        && foreign_key_id_ == other.foreign_key_id_);
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
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(table_id),
              K_(foreign_key_name), K_(foreign_key_id));

  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  common::ObString foreign_key_name_;
  uint64_t foreign_key_id_;
};

struct ObSimpleConstraintInfo
{
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
  ObSimpleConstraintInfo(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id, const common::ObString &constraint_name, const uint64_t constraint_id)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        table_id_(table_id),
        constraint_name_(constraint_name),
        constraint_id_(constraint_id)
  {}
  bool operator ==(const ObSimpleConstraintInfo &other) const {
    return (tenant_id_ == other.tenant_id_
        && database_id_ == other.database_id_
        && table_id_ == other.table_id_
        && constraint_name_ == other.constraint_name_
        && constraint_id_ == other.constraint_id_);
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

#define ASSIGN_PARTITION_ERROR(ERROR_STRING, USER_ERROR) { \
  if (OB_SUCC(ret)) {\
    if (OB_NOT_NULL(ERROR_STRING)) { \
      if (OB_FAIL(ERROR_STRING->assign(USER_ERROR))) { \
        SHARE_SCHEMA_LOG(WARN, "fail to append user error", KR(ret));\
      }\
    }\
  }\
}\

template<typename PARTITION>
int ObPartitionUtils::check_partition_value(
    const bool is_oracle_mode,
    const PARTITION &l_part,
    const PARTITION &r_part,
    const ObPartitionFuncType part_type,
    bool &is_equal,
    ObSqlString *user_error)
{
  int ret = common::OB_SUCCESS;
  is_equal = true;
  if (is_hash_like_part(part_type)) {
    is_equal = true;
  } else if (is_range_part(part_type)) {
    if (l_part.get_high_bound_val().get_obj_cnt() != r_part.get_high_bound_val().get_obj_cnt()) {
      is_equal = false;
      ASSIGN_PARTITION_ERROR(user_error, "range_part partition value count not equal");
      SHARE_SCHEMA_LOG(TRACE, "fail to check partition value, value count not equal",
                       "left", l_part.get_high_bound_val(),
                       "right", r_part.get_high_bound_val());
    } else {
      for (int64_t i = 0; i < l_part.get_high_bound_val().get_obj_cnt() && is_equal; i++) {
        const common::ObObjMeta meta1 = l_part.get_high_bound_val().get_obj_ptr()[i].get_meta();
        const common::ObObjMeta meta2 = r_part.get_high_bound_val().get_obj_ptr()[i].get_meta();
        // The obj comparison function does not require the same cs_level
        if (meta1.get_collation_type() == meta2.get_collation_type()) {
          is_equal = is_types_equal_for_partition_check(is_oracle_mode, meta1.get_type(), meta2.get_type());
          if (!is_equal) {
            ASSIGN_PARTITION_ERROR(user_error, "range_part partition meta type not equal");
            SHARE_SCHEMA_LOG(TRACE, "fail to check partition values, value meta not equal",
                           "left", l_part.get_high_bound_val().get_obj_ptr()[i],
                           "right", r_part.get_high_bound_val().get_obj_ptr()[i], K(is_oracle_mode));
          }
        } else {
          is_equal = false;
          ASSIGN_PARTITION_ERROR(user_error, "range_part partition collation type not matched");
          SHARE_SCHEMA_LOG(TRACE, "collation type not matched", "left", meta1.get_collation_type(),
                           "right", meta2.get_collation_type());
        }
        if (!is_equal) {
          //do nothing
        } else if (0 != l_part.get_high_bound_val().get_obj_ptr()[i].compare(r_part.get_high_bound_val().get_obj_ptr()[i],
                                                                      r_part.get_high_bound_val().get_obj_ptr()[i].get_collation_type())) {
          is_equal = false;
          ASSIGN_PARTITION_ERROR(user_error, "range_part partition value not equal");
          SHARE_SCHEMA_LOG(TRACE, "fail to check partition values, value not equal",
                           "left", l_part.get_high_bound_val().get_obj_ptr()[i],
                           "right", r_part.get_high_bound_val().get_obj_ptr()[i]);
        }
      }
    }
  } else if (is_list_part(part_type)) {
    const common::ObIArray<common::ObNewRow> &l_list_values = l_part.get_list_row_values();
    const common::ObIArray<common::ObNewRow> &r_list_values = r_part.get_list_row_values();
    if (l_list_values.count() != r_list_values.count()) {
      is_equal = false;
      ASSIGN_PARTITION_ERROR(user_error, "list_part partition value count not equal");
      SHARE_SCHEMA_LOG(TRACE, "fail to check list_part partition value, value count not equal",
                       "left", l_list_values,
                       "right", r_list_values);
    } else {
      for (int64_t i = 0; i < l_list_values.count() && is_equal; i++) {
        const common::ObNewRow &l_rowkey = l_list_values.at(i);
        bool find_equal_item = false;
        for (int64_t j = 0; j < r_list_values.count() && !find_equal_item && is_equal; j++) {
          const common::ObNewRow &r_rowkey = r_list_values.at(j);
          // First check that the count and meta information are consistent;
          if (l_rowkey.get_count() != r_rowkey.get_count()) {
            is_equal = false;
            ASSIGN_PARTITION_ERROR(user_error, "list_part partition value count not equal");
            SHARE_SCHEMA_LOG(TRACE, "fail to check partition value, value count not equal",
                            "left", l_rowkey, "right", r_rowkey);
          } else {
            for (int64_t z = 0; z < l_rowkey.get_count() && is_equal; z++) {
              const common::ObObjMeta meta1 = l_rowkey.get_cell(z).get_meta();
              const common::ObObjMeta meta2 = r_rowkey.get_cell(z).get_meta();
              // The obj comparison function does not require the same cs_level
              if (meta1.get_collation_type() == meta2.get_collation_type()) {
                is_equal = is_types_equal_for_partition_check(is_oracle_mode, meta1.get_type(), meta2.get_type());
                if (!is_equal) {
                  ASSIGN_PARTITION_ERROR(user_error, "list_part partition meta type not equal");
                  SHARE_SCHEMA_LOG(TRACE, "fail to check partition values, value meta not equal",
                                 "left", l_rowkey.get_cell(z), "right", r_rowkey.get_cell(z));
                }
              } else {
                is_equal = false;
                ASSIGN_PARTITION_ERROR(user_error, "list_part partition collation type not matched");
                SHARE_SCHEMA_LOG(TRACE,"collation type not matched", "left", meta1.get_collation_type(),
                                 "right", meta2.get_collation_type());
              }
            }
          }
          // Check whether the value of rowkey is the same;
          if (OB_SUCC(ret) && is_equal && l_rowkey == r_rowkey) {
            find_equal_item = true;
          }
        } //end for (int64_t j = 0
        if (!find_equal_item) {
          is_equal = false;
          ASSIGN_PARTITION_ERROR(user_error, "list_part partition value not equal");
          SHARE_SCHEMA_LOG(TRACE,"list_part partition value not equal");
        }
      } //end for (int64_t i = 0;
    }
  } else {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid part_type", K(ret), K(part_type));
  }
  return ret;
}

// Equivalent to simple schema
class ObKeystoreSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObKeystoreSchema();
  explicit ObKeystoreSchema(common::ObIAllocator *allocator);
  virtual ~ObKeystoreSchema();
  ObKeystoreSchema &operator=(const ObKeystoreSchema &src_schema);
  ObKeystoreSchema(const ObKeystoreSchema &src_schema);
  int64_t get_convert_size() const;
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_keystore_id(uint64_t id) { keystore_id_ = id; }
  inline void set_master_key_id(uint64_t id) { master_key_id_ = id; }
  inline void set_status(uint64_t status) { status_ = status; }
  inline int set_keystore_name(const char *name) { return deep_copy_str(name, keystore_name_); }
  inline int set_keystore_name(const common::ObString &name) { return deep_copy_str(name, keystore_name_); }
  inline int set_password(const char *name) { return deep_copy_str(name, password_); }
  inline int set_password(const common::ObString &name) { return deep_copy_str(name, password_); }
  inline int set_master_key(const char *key) { return deep_copy_str(key, master_key_); }
  inline int set_master_key(const common::ObString &key) { return deep_copy_str(key, master_key_); }
  inline int set_encrypted_key(const char *key) { return deep_copy_str(key, encrypted_key_); }
  inline int set_encrypted_key(const common::ObString &key) { return deep_copy_str(key, encrypted_key_); }
  // inline ObTenantKeystoreId get_tenant_keystore_id() const { return ObTenantKeystoreId(tenant_id_, keystore_id_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_keystore_id() const { return keystore_id_; }
  inline int64_t get_master_key_id() const { return master_key_id_; }
  inline uint64_t get_status() const { return status_; }
  inline const common::ObString &get_keystore_name() const { return keystore_name_; }
  inline const char *get_keystore_name_str() const { return extract_str(keystore_name_); }
  inline const common::ObString &get_password() const { return password_; }
  inline const char *get_password_str() const { return extract_str(password_); }
  inline const common::ObString &get_master_key() const { return master_key_; }
  inline const char *get_master_key_str() const { return extract_str(master_key_); }
  inline const common::ObString &get_encrypted_key() const { return encrypted_key_; }
  inline const char *get_encrypted_key_str() const { return extract_str(encrypted_key_); }
  int64_t get_schema_version() const { return schema_version_; }
  void reset();
  inline ObTenantKeystoreId get_tenant_keystore_id() const
  { return ObTenantKeystoreId(tenant_id_, keystore_id_); }
  VIRTUAL_TO_STRING_KV(K_(keystore_name),
                       K_(tenant_id),
                       K_(keystore_id),
                       K_(schema_version),
                       K_(status),
                       K_(master_key_id),
                       K_(master_key),
                       K_(encrypted_key));
private:
  // void *alloc(int64_t size);
private:
  uint64_t tenant_id_;
  uint64_t keystore_id_;
  int64_t schema_version_;
  common::ObString keystore_name_;
  // Option supported by keystore
  common::ObString password_;
  // 1->open and include master key
// 2->open and don't include a master key
  // 0->close
  int64_t status_;
  uint64_t master_key_id_;
  common::ObString master_key_;
  common::ObString encrypted_key_;
};

static const char* IDENTITY_COLUMN_SEQUENCE_OBJECT_NAME_PREFIX = "ISEQ$$_";
class ObSequenceSchema: public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSequenceSchema();
  explicit ObSequenceSchema(common::ObIAllocator *allocator);
  virtual ~ObSequenceSchema();
  ObSequenceSchema &operator=(const ObSequenceSchema &src_schema);
  int assign(const ObSequenceSchema &src_schema);
  ObSequenceSchema(const ObSequenceSchema &src_schema);
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_sequence_id(uint64_t id) { sequence_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline void set_is_system_generated(bool is_system_generated) { is_system_generated_ = is_system_generated; }
  inline int set_sequence_name(const char *name) { return deep_copy_str(name, name_); }
  inline int set_sequence_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  inline int set_name(const common::ObString &name) { return set_sequence_name(name); }
  // inline void set_max_value(int64_t val) { option_.set_max_value(val); }
  // inline void set_min_value(int64_t val) { option_.set_min_value(val); }
  // inline void set_increment_by(int64_t val) { option_.set_increment_by(val); }
  // inline void set_start_with(int64_t val) { option_.set_start_with(val); }
  // inline void set_cache_size(int64_t val) { option_.set_cache_size(val); }
  inline void set_cycle_flag(bool cycle) { option_.set_cycle_flag(cycle); }
  inline void set_order_flag(bool order) { option_.set_order_flag(order); }

  // Temporary compatibility code, in order to support max_value etc. as Number type
  // int set_max_value(const common::ObString &str);
  // int set_min_value(const common::ObString &str);
  // int set_increment_by(const common::ObString &str);
  // int set_start_with(const common::ObString &str);
  // int set_cache_size(const common::ObString &str);

  int set_max_value(const common::number::ObNumber &num) { return option_.set_max_value(num); }
  int set_min_value(const common::number::ObNumber &num) { return option_.set_min_value(num); }
  int set_increment_by(const common::number::ObNumber &num) { return option_.set_increment_by(num); }
  int set_start_with(const common::number::ObNumber &num) { return option_.set_start_with(num); }
  int set_cache_size(const common::number::ObNumber &num) { return option_.set_cache_size(num); }

  inline const common::number::ObNumber &get_min_value() const { return option_.get_min_value(); }
  inline const common::number::ObNumber &get_max_value() const { return option_.get_max_value(); }
  inline const common::number::ObNumber &get_increment_by() const { return option_.get_increment_by(); }
  inline const common::number::ObNumber &get_start_with() const { return option_.get_start_with(); }
  inline const common::number::ObNumber &get_cache_size() const { return option_.get_cache_size(); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_sequence_id() const { return sequence_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline bool get_is_system_generated() const { return is_system_generated_; }
  //inline int64_t get_min_value() const { return option_.get_min_value(); }
  //inline int64_t get_max_value() const { return option_.get_max_value(); }
  //inline int64_t get_increment_by() const { return option_.get_increment_by(); }
  //inline int64_t get_start_with() const { return option_.get_start_with(); }
  //inline int64_t get_cache_size() const { return option_.get_cache_size(); }
  inline bool get_cycle_flag() const { return option_.get_cycle_flag(); }
  inline bool get_order_flag() const { return option_.get_order_flag(); }
  inline const common::ObString &get_sequence_name() const { return name_; }
  inline const char *get_sequence_name_str() const { return extract_str(name_); }
  inline share::ObSequenceOption &get_sequence_option() { return option_; }
  inline const share::ObSequenceOption &get_sequence_option() const { return option_; }
  inline ObTenantSequenceId get_tenant_sequence_id() const
  { return ObTenantSequenceId(tenant_id_, sequence_id_); }
  inline uint64_t get_dblink_id() const { return dblink_id_; }
  inline void set_dblink_id(uint64_t id) { dblink_id_ = id; }

  void reset();

  VIRTUAL_TO_STRING_KV(K_(name),
                       K_(tenant_id),
                       K_(database_id),
                       K_(sequence_id),
                       K_(schema_version),
                       K_(option),
                       K_(is_system_generated),
                       K_(dblink_id));
private:
  //void *alloc(int64_t size);
  // int get_value(const common::ObString &str, int64_t &val);
  // int get_value(const common::number::ObNumber &num, int64_t &val);
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t sequence_id_;
  int64_t schema_version_; //the last modify timestamp of this version
  common::ObString name_;//sequence name
  share::ObSequenceOption option_;
  bool is_system_generated_;
  //common::ObArenaAllocator allocator_;
  uint64_t dblink_id_;
};

typedef ObSequenceSchema ObSequenceInfo;

struct ObTenantTablespaceId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantTablespaceId()
      : tenant_id_(common::OB_INVALID_TENANT_ID), tablespace_id_(common::OB_INVALID_ID)
  {}
  ObTenantTablespaceId(const uint64_t tenant_id, const uint64_t tablespace_id)
      : tenant_id_(tenant_id), tablespace_id_(tablespace_id)
  {}
  bool operator==(const ObTenantTablespaceId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (tablespace_id_ == rhs.tablespace_id_);
  }
  bool operator!=(const ObTenantTablespaceId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantTablespaceId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = tablespace_id_ < rhs.tablespace_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&tablespace_id_, sizeof(tablespace_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (tablespace_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(tablespace_id));
  uint64_t tenant_id_;
  uint64_t tablespace_id_;
};

class ObTablespaceSchema: public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObTablespaceSchema();
  explicit ObTablespaceSchema(common::ObIAllocator *allocator);
  virtual ~ObTablespaceSchema();
  ObTablespaceSchema &operator=(const ObTablespaceSchema &src_schema);
  ObTablespaceSchema(const ObTablespaceSchema &src_schema);
  int assign(const ObTablespaceSchema &src_schema)
  {
    *this = src_schema;
    return get_err_ret();
  }
  int64_t get_convert_size() const;

  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_tablespace_id(uint64_t id) { tablespace_id_ = id; }
  inline int set_tablespace_name(const char *name) { return deep_copy_str(name, tablespace_name_); }
  inline int set_tablespace_name(const common::ObString &name) { return deep_copy_str(name, tablespace_name_); }
  inline int set_encryption_name(const char *name) { return deep_copy_str(name, encryption_name_); }
  inline int set_encryption_name(const common::ObString &name) { return deep_copy_str(name, encryption_name_); }
  inline int set_encrypt_key(const char *key) { return deep_copy_str(key, encrypt_key_); }
  inline int set_encrypt_key(const common::ObString &key) { return deep_copy_str(key, encrypt_key_); }
  inline void set_master_key_id(uint64_t id) { master_key_id_ = id; }
  // inline ObTenantTablespaceId get_tenant_tablespace_id() const { return ObTenantTablespaceId(tenant_id_, tablespace_id_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_tablespace_id() const { return tablespace_id_; }
  inline const common::ObString &get_tablespace_name() const { return tablespace_name_; }
  inline const char *get_tablespace_name_str() const { return extract_str(tablespace_name_); }
  inline const common::ObString &get_encryption_name() const { return encryption_name_; }
  inline const char *get_encryption_name_str() const { return extract_str(encryption_name_); }
  inline const common::ObString &get_encrypt_key() const { return encrypt_key_; }
  inline const char *get_encrypt_key_str() const { return extract_str(encrypt_key_); }
  int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_master_key_id() const { return master_key_id_; }
  inline ObTenantTablespaceId get_tenant_tablespace_id() const
  { return ObTenantTablespaceId(tenant_id_, tablespace_id_); }
  inline bool need_encrypt() { return encrypt_key_.length() > 0; }
  void reset();
  VIRTUAL_TO_STRING_KV(K_(tablespace_name),
                       K_(tenant_id),
                       K_(tablespace_id),
                       K_(schema_version),
                       K_(encryption_name),
                       K_(encrypt_key),
                       K_(master_key_id));
private:
  // void *alloc(int64_t size);
private:
  uint64_t tenant_id_;
  uint64_t tablespace_id_;
  int64_t schema_version_;
  common::ObString tablespace_name_;

  // Options supported by tablespace
  common::ObString encryption_name_;
  common::ObString encrypt_key_;
  uint64_t master_key_id_;

};

class ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantCommonSchemaId()
      : tenant_id_(common::OB_INVALID_TENANT_ID), schema_id_(common::OB_INVALID_ID)
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

class ObTenantProfileId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantProfileId() : ObTenantCommonSchemaId() {}
  ObTenantProfileId(const uint64_t tenant_id, const uint64_t profile_id)
    : ObTenantCommonSchemaId(tenant_id, profile_id) {}
};

class ObProfileSchema: public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  static const int64_t INVALID_VALUE = -1;
  static const int64_t DEFAULT_VALUE = -2;
  static const int64_t UNLIMITED_VALUE = INT64_MAX;


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

  TO_STRING_KV(K_(tenant_id), K_(profile_id), K_(schema_version), K_(profile_name),
               K_(failed_login_attempts), K_(password_lock_time), K_(password_life_time),
               K_(password_grace_time));

  bool is_valid() const;
  void reset();
  int64_t get_convert_size() const;

  ObProfileSchema &operator=(const ObProfileSchema &src_schema);
  ObProfileSchema(const ObProfileSchema &src_schema);

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_profile_id(uint64_t id) { profile_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline int set_profile_name(const char *name) { return deep_copy_str(name, profile_name_); }
  inline int set_profile_name(const common::ObString &name) { return deep_copy_str(name, profile_name_); }
  inline void set_failed_login_attempts(const int64_t value) { failed_login_attempts_ = value; }
  inline void set_password_lock_time(const int64_t value) { password_lock_time_ = value; }
  inline void set_password_life_time(const int64_t value) { password_life_time_ = value; }
  inline void set_password_grace_time(const int64_t value) { password_grace_time_ = value; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_profile_id() const { return profile_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_profile_name() const { return extract_str(profile_name_); }
  inline const common::ObString &get_profile_name_str() const { return profile_name_; }
  inline int64_t get_failed_login_attempts() const { return failed_login_attempts_; }
  inline int64_t get_password_lock_time() const { return password_lock_time_; }
  inline int64_t get_password_life_time() const { return password_life_time_; }
  inline int64_t get_password_grace_time() const { return password_grace_time_; }

  inline ObTenantProfileId get_tenant_profile_id() const { return ObTenantProfileId(tenant_id_, profile_id_); }

  int set_value(const int64_t type, const int64_t value);
  int set_default_value(const int64_t type);
  int set_default_values();
  int set_default_values_v2();
  int set_invalid_values();
  static int get_default_value(const int64_t type, int64_t &value);

  inline const char *get_password_verify_function() const { return extract_str(password_verify_function_); }
  inline const common::ObString &get_password_verify_function_str() const { return password_verify_function_; }
  inline int set_password_verify_function(const char *name) { return deep_copy_str(name, password_verify_function_); }
  inline int set_password_verify_function(const common::ObString &name) { return deep_copy_str(name, password_verify_function_); }
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
class ObSchemaStackAllocatorGuard
{
public:
  ObSchemaStackAllocatorGuard() = delete;
  explicit ObSchemaStackAllocatorGuard(ObIAllocator *allocator)
  {
    schema_stack_allocator() = allocator;
  }
  ~ObSchemaStackAllocatorGuard()
  {
    schema_stack_allocator() = NULL;
  }
};

class ObLabelSePolicySchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  enum class PolicyStatus {
    ERROR = -1,
    ENABLED = 0,
    DISABLED = 1,
  };
  enum class PolicyOptions {  //move to new file
    INVALID_OPTION = -1,
    //The Label Management Enforcement Options
    LABEL_DEFAULT,
    LABEL_UPDATE,
    //The Access Control Enforcement Options
    READ_CONTROL,       //controls SELECT, UPDATE, and DELETE
    WRITE_CONTROL,      //controls INSERT, UPDATE, and DELETE
    INSERT_CONTROL,
    DELETE_CONTROL,
    UPDATE_CONTROL,
    CHECK_CONTROL,      //controls INSERT, UPDATE
                        /* Applies READ_CONTROL policy enforcement to
                         * INSERT and UPDATE statements to assure that
                         * the new row label is read-accessible.
                         * */
    //The Overriding Enforcement Options
    ALL_CONTROL,
    NO_CONTROL,
    TOTAL_OPTIONS
  };
  static_assert(static_cast<int64_t>(PolicyOptions::TOTAL_OPTIONS) < sizeof(int64_t) * 8, "too many ols policy options");

  ObLabelSePolicySchema();
  explicit ObLabelSePolicySchema(common::ObIAllocator *allocator);
  ObLabelSePolicySchema(const ObLabelSePolicySchema &src_schema);
  virtual ~ObLabelSePolicySchema();
  ObLabelSePolicySchema &operator =(const ObLabelSePolicySchema &other);
  int assign(const ObLabelSePolicySchema &other)
  {
    *this = other;
    return get_err_ret();
  }
  bool operator ==(const ObLabelSePolicySchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(label_se_policy_id),
               K_(schema_version),
               K_(policy_name),
               K_(column_name),
               K_(default_options),
               K_(flag));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_label_se_policy_id(const uint64_t label_se_policy_id) { label_se_policy_id_ = label_se_policy_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int set_policy_name(const common::ObString &policy_name) { return deep_copy_str(policy_name, policy_name_); }
  inline int set_column_name(const common::ObString &column_name) { return deep_copy_str(column_name, column_name_); }
  inline void set_default_options(const int64_t default_option) { default_options_ = default_option; }
  inline void set_flag(int64_t flag) { flag_ = flag; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_label_se_policy_id() const { return label_se_policy_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_policy_name() const { return extract_str(policy_name_); }
  inline const char *get_column_name() const { return extract_str(column_name_); }
  inline const common::ObString &get_policy_name_str() const { return policy_name_; }
  inline const common::ObString &get_column_name_str() const { return column_name_; }
  inline int64_t get_default_options() const { return default_options_; }
  inline int64_t get_flag() const { return flag_; }
  inline ObTenantLabelSePolicyId get_tenant_label_se_policy_id() const { return ObTenantLabelSePolicyId(tenant_id_, label_se_policy_id_); }

private:
  uint64_t tenant_id_;
  uint64_t label_se_policy_id_;
  int64_t schema_version_;
  common::ObString policy_name_;
  common::ObString column_name_;
  int64_t default_options_;
  int64_t flag_;
};

class ObLabelSeComponentSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  enum class CompType {
    INVALID = -1,
    LEVEL,
    COMPARTMENT,
    GROUP,
  };
  ObLabelSeComponentSchema();
  explicit ObLabelSeComponentSchema(common::ObIAllocator *allocator);
  ObLabelSeComponentSchema(const ObLabelSeComponentSchema &src_schema);
  virtual ~ObLabelSeComponentSchema();
  ObLabelSeComponentSchema &operator =(const ObLabelSeComponentSchema &other);
  int assign(const ObLabelSeComponentSchema &other)
  {
    *this = other;
    return get_err_ret();
  }
  bool operator ==(const ObLabelSeComponentSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(label_se_policy_id),
               K_(label_se_component_id),
               K_(comp_type),
               K_(schema_version),
               K_(short_name),
               K_(long_name),
               K_(parent_name));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_label_se_policy_id(const uint64_t label_se_policy_id) { label_se_policy_id_ = label_se_policy_id; }
  inline void set_label_se_component_id(const uint64_t label_se_component_id) { label_se_component_id_ = label_se_component_id; }
  inline void set_comp_type(const int64_t comp_type) { comp_type_ = comp_type; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_comp_num(const int64_t comp_num) { comp_num_ = comp_num; }
  inline int set_short_name(const common::ObString &short_name) { return deep_copy_str(short_name, short_name_); }
  inline int set_long_name(const common::ObString &long_name) { return deep_copy_str(long_name, long_name_); }
  inline int set_parent_name(const common::ObString &parent_name) { return deep_copy_str(parent_name, parent_name_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_label_se_policy_id() const { return label_se_policy_id_; }
  inline uint64_t get_label_se_component_id() const { return label_se_component_id_; }
  inline int64_t get_comp_type() const { return comp_type_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int64_t get_comp_num() const { return comp_num_; }
  inline const char *get_long_name() const { return extract_str(long_name_); }
  inline const char *get_short_name() const { return extract_str(short_name_); }
  inline const char *get_parent_name() const { return extract_str(parent_name_); }
  inline const common::ObString &get_long_name_str() const { return long_name_; }
  inline const common::ObString &get_short_name_str() const { return short_name_; }
  inline const common::ObString &get_parent_name_str() const { return parent_name_; }
  inline ObTenantLabelSeComponentId get_tenant_label_se_component_id() const
  { return ObTenantLabelSeComponentId(tenant_id_, label_se_component_id_); }
  inline ObTenantLabelSePolicyId get_tenant_label_se_policy_id() const
  { return ObTenantLabelSePolicyId(tenant_id_, label_se_policy_id_); }

  inline bool is_valid_comp_num() const { return comp_num_ >= 0 && comp_num_ <= 9999; }

private:
  uint64_t tenant_id_;
  uint64_t label_se_policy_id_;
  uint64_t label_se_component_id_;
  int64_t comp_type_; //level compartment group
  int64_t schema_version_;
  int64_t comp_num_; //[0-9999]
  common::ObString short_name_;
  common::ObString long_name_;
  common::ObString parent_name_; //only for group
};


class ObLabelSeLabelSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  enum class LabelFlag {
    INVALID = -1,
    USER_LABEL,
    USER_DATA_LABEL,
  };

  ObLabelSeLabelSchema();
  explicit ObLabelSeLabelSchema(common::ObIAllocator *allocator);
  ObLabelSeLabelSchema(const ObLabelSeLabelSchema &src_schema);
  virtual ~ObLabelSeLabelSchema();
  ObLabelSeLabelSchema &operator =(const ObLabelSeLabelSchema &other);
  int assign(const ObLabelSeLabelSchema &other)
  {
    *this = other;
    return get_err_ret();
  }
  bool operator ==(const ObLabelSeLabelSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(label_se_label_id),
               K_(schema_version),
               K_(label_se_policy_id),
               K_(label_tag),
               K_(label),
               K_(flag));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_label_se_label_id(const uint64_t label_se_label_id) { label_se_label_id_ = label_se_label_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_label_se_policy_id(const uint64_t label_se_policy_id) { label_se_policy_id_ = label_se_policy_id; }
  inline void set_label_tag(const int64_t label_tag) { label_tag_ = label_tag; }
  inline int set_label(const common::ObString &label) { return deep_copy_str(label, label_); }
  inline void set_flag(int64_t flag) { flag_ = flag; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_label_se_label_id() const { return label_se_label_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_label_se_policy_id() const { return label_se_policy_id_; }
  inline int64_t get_label_tag() const { return label_tag_; }
  inline const char *get_label() const { return extract_str(label_); }
  inline const common::ObString &get_label_str() const { return label_; }
  inline int64_t get_flag() const { return flag_; }
  inline ObTenantLabelSeLabelId get_tenant_label_se_label_id() const
  { return ObTenantLabelSeLabelId(tenant_id_, label_se_label_id_); }

private:
  uint64_t tenant_id_;
  uint64_t label_se_label_id_;
  int64_t schema_version_;
  uint64_t label_se_policy_id_;
  int64_t label_tag_;
  common::ObString label_;
  int64_t flag_;
};

class ObLabelSeUserLevelSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  ObLabelSeUserLevelSchema();
  explicit ObLabelSeUserLevelSchema(common::ObIAllocator *allocator);
  ObLabelSeUserLevelSchema(const ObLabelSeUserLevelSchema &src_schema);
  virtual ~ObLabelSeUserLevelSchema();
  ObLabelSeUserLevelSchema &operator =(const ObLabelSeUserLevelSchema &other);
  int assign(const ObLabelSeUserLevelSchema &other)
  {
    *this = other;
    return get_err_ret();
  }
  bool operator ==(const ObLabelSeUserLevelSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(label_se_user_level_id),
               K_(user_id),
               K_(schema_version),
               K_(label_se_policy_id),
               K_(maximum_level),
               K_(minimum_level),
               K_(default_level),
               K_(row_level));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_label_se_user_level_id(const uint64_t label_se_user_level_id) { label_se_user_level_id_ = label_se_user_level_id; }
  inline void set_user_id(const uint64_t user_id) { user_id_ = user_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_label_se_policy_id(const uint64_t label_se_policy_id) { label_se_policy_id_ = label_se_policy_id; }
  inline void set_maximum_level(const int64_t level) { maximum_level_ = level; }
  inline void set_minimum_level(const int64_t level) { minimum_level_ = level; }
  inline void set_default_level(const int64_t level) { default_level_ = level; }
  inline void set_row_level(const int64_t level) { row_level_ = level; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_label_se_user_level_id() const { return label_se_user_level_id_; }
  inline uint64_t get_user_id() const { return user_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_label_se_policy_id() const { return label_se_policy_id_; }
  inline int64_t get_maximum_level() const { return maximum_level_; }
  inline int64_t get_minimum_level() const { return minimum_level_; }
  inline int64_t get_default_level() const { return default_level_; }
  inline int64_t get_row_level() const { return row_level_; }

  inline ObTenantLabelSeUserLevelId get_tenant_label_se_user_level_id() const
  { return ObTenantLabelSeUserLevelId(tenant_id_, label_se_user_level_id_); }

private:
  uint64_t tenant_id_;
  uint64_t label_se_user_level_id_;
  uint64_t user_id_;
  int64_t schema_version_;
  uint64_t label_se_policy_id_;
  int64_t maximum_level_;
  int64_t minimum_level_;
  int64_t default_level_;
  int64_t row_level_; //for insert
};

class ObTenantDirectoryId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantDirectoryId() : ObTenantCommonSchemaId() {}
  ObTenantDirectoryId(const uint64_t tenant_id, const uint64_t directory_id)
    : ObTenantCommonSchemaId(tenant_id, directory_id) {}
};

class ObDirectorySchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObDirectorySchema();
  explicit ObDirectorySchema(common::ObIAllocator *allocator);
  virtual ~ObDirectorySchema();

  explicit ObDirectorySchema(const ObDirectorySchema &other);
  ObDirectorySchema &operator=(const ObDirectorySchema &other);

  int assign(const ObDirectorySchema &other);

  virtual bool is_valid() const;
  virtual void reset();

  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  inline void set_directory_id(const uint64_t directory_id) { directory_id_ = directory_id; }
  inline int set_directory_name(const char *name) { return deep_copy_str(name, directory_name_); }
  inline int set_directory_name(const common::ObString &name) { return deep_copy_str(name, directory_name_); }
  inline int set_directory_path(const char *path) { return deep_copy_str(path, directory_path_); }
  inline int set_directory_path(const common::ObString &path) { return deep_copy_str(path, directory_path_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_directory_id() const { return directory_id_; }
  inline const char *get_directory_name() const { return extract_str(directory_name_); }
  inline const common::ObString &get_directory_name_str() const { return directory_name_; }
  inline const char *get_directory_path() const { return extract_str(directory_path_); }
  inline const common::ObString &get_directory_path_str() const { return directory_path_; }

  inline ObTenantDirectoryId get_tenant_directory_id() const { return ObTenantDirectoryId(tenant_id_, directory_id_); }

  TO_STRING_KV(K_(tenant_id), K_(directory_id), K_(schema_version),
               K_(directory_name), K_(directory_path));
private:
  uint64_t tenant_id_;
  uint64_t directory_id_;
  int64_t schema_version_;
  common::ObString directory_name_;
  common::ObString directory_path_;
};


// Add or delete an audit rule, corresponding to the audit/noaudit syntax
enum ObSAuditModifyType : uint64_t
{
  AUDIT_MT_INVALID = 0,
  AUDIT_MT_ADD,
  AUDIT_MT_DEL,
};

// Object audit or statement audit
enum ObSAuditType : uint64_t
{
  AUDIT_INVALID = 0,
  AUDIT_STMT_ALL_USER,
  AUDIT_STMT,
  AUDIT_OBJ_DEFAULT,
  AUDIT_TABLE,
  AUDIT_SEQUENCE,
  AUDIT_PACKAGE,
  AUDIT_PROCEDURE,
  AUDIT_MAX,
};

enum ObSAuditOperTimingType : uint64_t
{
  AUDIT_OT_INVALID = 0,
  AUDIT_OT_NOT_SET,
  AUDIT_OT_SESSION,
  AUDIT_OT_ACCESS,
};

enum ObSAuditOperByType : uint64_t
{
  AUDIT_BY_SESSION = 0,
  AUDIT_BY_ACCESS,
};

enum ObSAuditOperWhenType : uint64_t
{
  AUDIT_WHEN_NOT_SET = 0,
  AUDIT_WHEN_FAILURE,
  AUDIT_WHEN_SUCCESS,
};




// Statement types in statement audit
enum ObSAuditOperationType : uint64_t
{
  // NOTICE: ALWAYS append new enum value at the last
  // NEVER insert in the middle part to avoid compatibility problem
  AUDIT_OP_INVALID = 0,
  //stmt
  AUDIT_OP_ALL_STMTS,
  AUDIT_OP_ALTER_SYSTEM,
  AUDIT_OP_CLUSTER,
  AUDIT_OP_CONTEXT,
  AUDIT_OP_DBLINK,
  AUDIT_OP_INDEX_TABLE,
  AUDIT_OP_MATERIALIZED_VIEW,
  AUDIT_OP_NOT_EXIST,
  AUDIT_OP_OUTLINE,
  AUDIT_OP_PROCEDURE,
  AUDIT_OP_PROFILE,
  AUDIT_OP_PUBLIC_DBLINK,
  AUDIT_OP_PUBLIC_SYNONYM,
  AUDIT_OP_ROLE,
  AUDIT_OP_SEQUENCE,
  AUDIT_OP_SESSION,
  AUDIT_OP_SYNONYM,
  AUDIT_OP_SYSTEM_AUDIT,
  AUDIT_OP_SYSTEM_GRANT,
  AUDIT_OP_TABLE,
  AUDIT_OP_TABLESPACE,
  AUDIT_OP_TRIGGER,
  AUDIT_OP_TYPE,
  AUDIT_OP_USER,
  AUDIT_OP_VIEW,

  AUDIT_OP_ALTER_SEQUENCE,
  AUDIT_OP_ALTER_TABLE,
  AUDIT_OP_COMMENT_TABLE,
  AUDIT_OP_DELETE_TABLE,
  AUDIT_OP_EXECUTE_PROCEDURE,
  AUDIT_OP_GRANT_PROCEDURE,
  AUDIT_OP_GRANT_SEQUENCE,
  AUDIT_OP_GRANT_TABLE,
  AUDIT_OP_GRANT_TYPE,
  AUDIT_OP_INSERT_TABLE,
  AUDIT_OP_SELECT_SEQUENCE,
  AUDIT_OP_SELECT_TABLE,
  AUDIT_OP_UPDATE_TABLE,

  //object
  AUDIT_OP_ALTER,
  AUDIT_OP_AUDIT,
  AUDIT_OP_COMMENT,
  AUDIT_OP_DELETE,
  AUDIT_OP_GRANT,
  AUDIT_OP_INDEX,
  AUDIT_OP_INSERT,
  AUDIT_OP_LOCK,
  AUDIT_OP_RENAME,
  AUDIT_OP_SELECT,
  AUDIT_OP_UPDATE,
  AUDIT_OP_REF,
  AUDIT_OP_EXECUTE,
  AUDIT_OP_CREATE,
  AUDIT_OP_READ,
  AUDIT_OP_WRITE,
  AUDIT_OP_FLASHBACK,

  AUDIT_OP_DIRECTORY,

  //privilege ....
  AUDIT_OP_MAX
};
const char *get_audit_operation_type_str(const ObSAuditOperationType type);

int get_operation_type_from_item_type(const bool is_stmt_audit,
                                      const int32_t item_type,
                                      ObSAuditOperationType &operation_type,
                                      bool &is_ddl);
struct ObTenantAuditKey
{
  OB_UNIS_VERSION(1);
public:
  ObTenantAuditKey() : tenant_id_(common::OB_INVALID_ID),
                      audit_id_(common::OB_INVALID_ID)
  {}
  ObTenantAuditKey(const uint64_t tenant_id, const uint64_t audit_id)
      : tenant_id_(tenant_id), audit_id_(audit_id)
  {}
  inline bool operator==(const ObTenantAuditKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_
            && audit_id_ == rhs.audit_id_);
  }
  inline bool operator!=(const ObTenantAuditKey &rhs) const
  {
    return !(*this == rhs);
  }
  inline bool operator<(const ObTenantAuditKey &rhs) const
  {
    bool bret = false;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = audit_id_ < rhs.audit_id_;
    } else {
      bret = tenant_id_ < rhs.tenant_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&audit_id_, sizeof(audit_id_), hash_ret);
    return hash_ret;
  }
  inline bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID
            && audit_id_ != common::OB_INVALID_ID);
  }
  inline void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    audit_id_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(tenant_id), K_(audit_id));
  uint64_t tenant_id_;
  uint64_t audit_id_;
};

class ObSAuditSchema: public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSAuditSchema() : ObSchema() { reset(); };
  explicit ObSAuditSchema(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  virtual ~ObSAuditSchema() {}
  ObSAuditSchema &operator=(const ObSAuditSchema &src_schema)
  {
    if (this != &src_schema) {
      reset();
      error_ret_ = src_schema.error_ret_;
      schema_version_ = src_schema.schema_version_;
      audit_key_ = src_schema.audit_key_;
      audit_type_ = src_schema.audit_type_;
      owner_id_ = src_schema.owner_id_;
      operation_type_ = src_schema.operation_type_;
      in_success_ = src_schema.in_success_;
      in_failure_ = src_schema.in_failure_;
    }
    return *this;
  }

  int assign(const ObSAuditSchema &src_schema)
  {
    *this = src_schema;
    return get_err_ret();
  }
  ObSAuditSchema(const ObSAuditSchema &src_schema) : ObSchema()
  {
    reset();
    *this = src_schema;
  }

  int64_t get_convert_size() const { return static_cast<int64_t>(sizeof(ObSAuditSchema)); }

  void reset()
  {
    ObSchema::reset();
    schema_version_ = common::OB_INVALID_VERSION;
    audit_key_.reset();
    audit_type_ = AUDIT_INVALID;
    owner_id_ = common::OB_INVALID_ID;
    operation_type_ = AUDIT_OP_INVALID;
    in_success_ = AUDIT_OT_NOT_SET;
    in_failure_ = AUDIT_OT_NOT_SET;
  }
  bool is_valid() const
  {
    return (audit_key_.is_valid()
            && (AUDIT_INVALID < audit_type_ && audit_type_ < AUDIT_MAX)
            && owner_id_ != common::OB_INVALID_ID
            && (AUDIT_OP_INVALID < operation_type_ && operation_type_ < AUDIT_OP_MAX)
            && ((AUDIT_OT_NOT_SET <= in_success_ && in_success_ <= AUDIT_OT_ACCESS)
                || (AUDIT_OT_NOT_SET <= in_failure_ && in_failure_ <= AUDIT_OT_ACCESS)));
  }
  inline void set_tenant_id(const uint64_t id) { audit_key_.tenant_id_ = id; }
  inline void set_audit_id(const uint64_t id) { audit_key_.audit_id_ = id; }
  inline void set_schema_version(const int64_t version) { schema_version_ = version; }
  inline void set_audit_type(const ObSAuditType type) { audit_type_ = type; }
  inline void set_audit_type(const uint64_t type) { audit_type_ = static_cast<ObSAuditType>(type); }
  inline void set_owner_id(const uint64_t value) { owner_id_ = value; }
  inline void set_operation_type(const uint64_t value) { operation_type_ = static_cast<ObSAuditOperationType>(value); }
  inline void set_operation_type(const ObSAuditOperationType value) { operation_type_ = value; }
  inline void set_in_success(const uint64_t value) { in_success_ = static_cast<ObSAuditOperTimingType>(value); }
  inline void set_in_success(const ObSAuditOperTimingType value) { in_success_ = value; }
  inline void set_in_failure(const uint64_t value) { in_failure_ = static_cast<ObSAuditOperTimingType>(value); }
  inline void set_in_failure(const ObSAuditOperTimingType value) { in_failure_ = value; }

  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_tenant_id() const { return audit_key_.tenant_id_; }
  inline uint64_t get_audit_id() const { return audit_key_.audit_id_; }
  inline ObSAuditType get_audit_type() const { return audit_type_; }
  inline uint64_t get_owner_id() const { return owner_id_; }
  inline ObSAuditOperationType get_operation_type() const { return operation_type_; }
  inline ObSAuditOperTimingType get_in_success() const { return in_success_; }
  inline ObSAuditOperTimingType get_in_failure() const { return in_failure_; }
  inline ObTenantAuditKey get_audit_key() const { return audit_key_; }

  OB_INLINE static bool is_access_operation_for_default(const ObSAuditOperationType type)
  {
    return !(AUDIT_OP_LOCK == type
             || AUDIT_OP_REF == type
             || (AUDIT_OP_CREATE <= type && type <= AUDIT_OP_WRITE));
  }
  OB_INLINE static bool is_access_operation_for_table(const ObSAuditOperationType type)
  {
    return ((AUDIT_OP_ALTER <= type && type <= AUDIT_OP_UPDATE)
            || AUDIT_OP_FLASHBACK == type);
  }
  OB_INLINE static bool is_access_operation_for_sequence(const ObSAuditOperationType type)
  {
    return (AUDIT_OP_ALTER == type
            || AUDIT_OP_AUDIT == type
            || AUDIT_OP_GRANT == type
            || AUDIT_OP_SELECT == type);
  }
  OB_INLINE static bool is_access_operation_for_package(const ObSAuditOperationType type)
  {
    return (AUDIT_OP_AUDIT == type
            || AUDIT_OP_GRANT == type
            || AUDIT_OP_EXECUTE == type);
  }
  OB_INLINE static bool is_access_operation_for_procedure(const ObSAuditOperationType type)
  {
    return (AUDIT_OP_AUDIT == type
            || AUDIT_OP_GRANT == type
            || AUDIT_OP_EXECUTE == type);
  }
  OB_INLINE bool is_access_operation_for_table() const
  {
    return is_access_operation_for_table(operation_type_);
  }
  OB_INLINE bool is_access_operation_for_sequence() const
  {
    return is_access_operation_for_sequence(operation_type_);
  }
  OB_INLINE bool is_access_operation_for_package() const
  {
    return is_access_operation_for_package(operation_type_);
  }
  OB_INLINE bool is_access_operation_for_procedure() const
  {
    return is_access_operation_for_procedure(operation_type_);
  }
  OB_INLINE static bool is_access_operation_for_stmt(const ObSAuditOperationType type)
  {
    return !(AUDIT_OP_CONTEXT == type
             || AUDIT_OP_DBLINK == type
             || AUDIT_OP_MATERIALIZED_VIEW == type
             || AUDIT_OP_PUBLIC_DBLINK == type);
  }
  OB_INLINE bool is_access_operation_for_stmt() const
  {
    return is_access_operation_for_stmt(operation_type_);
  }

  //TODO::it is not enough
  inline bool is_access_audit(const int return_code) const
  {
    return ((common::OB_SUCCESS == return_code && AUDIT_OT_NOT_SET != in_success_)
            || (common::OB_SUCCESS != return_code && AUDIT_OT_NOT_SET != in_failure_));
  }
  inline ObTenantAuditKey get_tenant_audit_id() const
  {
    return audit_key_;
  }

  VIRTUAL_TO_STRING_KV(K_(schema_version),
                       K_(audit_key),
                       K_(audit_type),
                       K_(owner_id),
                       K_(operation_type),
                       K_(in_success),
                       K_(in_failure));
private:
//audit_type:     stmt,        obj,        privilege
//owner_id:       user_id,     obj_id,     user_id
//operation_type: stmt_type,   oper_type,  priv_type

  int64_t schema_version_; //the last modify timestamp of this version
  ObTenantAuditKey audit_key_;
  ObSAuditType audit_type_;
  uint64_t owner_id_;
  ObSAuditOperationType operation_type_;
  ObSAuditOperTimingType in_success_;
  ObSAuditOperTimingType in_failure_;
};

struct ObObjectStruct
{
  ObObjectStruct() : type_(ObObjectType::INVALID), id_(common::OB_INVALID_ID) {};
  ObObjectStruct(const ObObjectType type, const uint64_t id)
    : type_(type), id_(id) {};
  ~ObObjectStruct() {};

  TO_STRING_KV(K_(type), K_(id));

  ObObjectType type_;
  uint64_t id_;
};

//
class IObErrorInfo {
public:
  IObErrorInfo() {}
  virtual ~IObErrorInfo() = 0;
  virtual uint64_t get_object_id() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual uint64_t get_database_id() const = 0;
  virtual int64_t get_schema_version() const = 0;
  virtual ObObjectType get_object_type() const = 0;
  static constexpr uint64_t TYPE_MASK = (0x7) << (sizeof(uint64_t) - 3);
  static constexpr uint64_t VAL_MASK = (-1) >> 3;
};

struct ObContextKey
{
  ObContextKey(const uint64_t tenant_id, const uint64_t context_id)
      : tenant_id_(tenant_id), context_id_(context_id)
  {}
  bool operator==(const ObContextKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (context_id_ == rhs.context_id_);
  }
  bool operator!=(const ObContextKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObContextKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = context_id_ < rhs.context_id_;
    }
    return bret;
  }
  //Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&context_id_, sizeof(context_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (context_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(context_id));
  uint64_t tenant_id_;
  uint64_t context_id_;
};

class ObContextSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObContextSchema();
  explicit ObContextSchema(common::ObIAllocator *allocator);
  virtual ~ObContextSchema();
  ObContextSchema &operator=(const ObContextSchema &src_schema);
  int assign(const ObContextSchema &src_schema);
  ObContextSchema(const ObContextSchema &src_schema);

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_context_id(const uint64_t id) { context_id_ = id; }
  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_origin_con_id(int64_t id) { origin_con_id_ = id; }
  inline void set_is_tracking(bool is_tracking) { tracking_ = is_tracking; }
  inline int set_namespace(const char *ctx_namespace) { return deep_copy_str(ctx_namespace, namespace_); }
  inline int set_namespace(const common::ObString &ctx_namespace) { return deep_copy_str(ctx_namespace, namespace_); }
  inline int set_schema_name(const common::ObString &schema_name) { return deep_copy_str(schema_name, schema_name_); }
  inline int set_trusted_package(const char *trusted_package) { return deep_copy_str(trusted_package, trusted_package_); }
  inline int set_trusted_package(const common::ObString &trusted_package) { return deep_copy_str(trusted_package, trusted_package_); }
  inline void set_context_type(const ObContextType &type) { type_ = type; }
  inline void set_context_type(const int64_t type) { type_ = static_cast<ObContextType> (type); }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_context_id() const { return context_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int64_t get_origin_con_id() const { return origin_con_id_; }
  inline bool get_tracking() const { return tracking_; }
  inline ObContextType get_context_type() const { return type_; }

  inline const common::ObString &get_namespace() const { return namespace_; }
  inline const char *get_namespace_str() const { return extract_str(namespace_); }
  inline const common::ObString &get_schema_name() const { return schema_name_; }
  inline const char *get_schema_name_str() const { return extract_str(schema_name_); }
  inline const common::ObString &get_trusted_package() const { return trusted_package_; }
  inline const char *get_trusted_package_str() const { return extract_str(trusted_package_); }
  inline ObContextKey get_context_key() const
  { return ObContextKey(tenant_id_, context_id_); }

  void reset();

  VIRTUAL_TO_STRING_KV(K_(tenant_id),
                       K_(context_id),
                       K_(schema_version),
                       K(namespace_),
                       K_(schema_name),
                       K_(trusted_package),
                       K_(type),
                       K_(origin_con_id),
                       K_(tracking));
private:
  uint64_t tenant_id_;
  uint64_t context_id_;
  int64_t schema_version_;
  common::ObString namespace_;//ctx namespace
  common::ObString schema_name_; // database name
  common::ObString trusted_package_;
  ObContextType type_;
  int64_t origin_con_id_;
  bool tracking_;
};

struct ObMockFKParentTableKey
{
  ObMockFKParentTableKey(const uint64_t tenant_id, const uint64_t mock_fk_parent_table_id)
      : tenant_id_(tenant_id), mock_fk_parent_table_id_(mock_fk_parent_table_id) {}
  bool operator==(const ObMockFKParentTableKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (mock_fk_parent_table_id_ == rhs.mock_fk_parent_table_id_);
  }
  bool operator!=(const ObMockFKParentTableKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObMockFKParentTableKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = mock_fk_parent_table_id_ < rhs.mock_fk_parent_table_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&mock_fk_parent_table_id_, sizeof(mock_fk_parent_table_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (mock_fk_parent_table_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(mock_fk_parent_table_id));
  uint64_t tenant_id_;
  uint64_t mock_fk_parent_table_id_;
};

enum ObMockFKParentTableOperationType
{
  MOCK_FK_PARENT_TABLE_OP_INVALID = 0,
  MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_DROP_PARENT_TABLE = 1,
  MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE = 2,
  MOCK_FK_PARENT_TABLE_OP_DROP_TABLE = 3,
  MOCK_FK_PARENT_TABLE_OP_ADD_COLUMN = 4,
  MOCK_FK_PARENT_TABLE_OP_DROP_COLUMN = 5,
  MOCK_FK_PARENT_TABLE_OP_REPLACED_BY_REAL_PREANT_TABLE = 6,
  MOCK_FK_PARENT_TABLE_OP_UPDATE_SCHEMA_VERSION = 7,
  MOCK_FK_PARENT_TABLE_OP_TYPE_MAX = 1000,
};

// <column_id, column_name>
typedef common::ObArray<std::pair<uint64_t, common::ObString> > ObMockFKParentTableColumnArray;

class ObSimpleMockFKParentTableSchema : public ObSchema
{
public:
  ObSimpleMockFKParentTableSchema();
  explicit ObSimpleMockFKParentTableSchema(common::ObIAllocator *allocator);
  virtual ~ObSimpleMockFKParentTableSchema();
  ObSimpleMockFKParentTableSchema(const ObSimpleMockFKParentTableSchema &src_schema);
  ObSimpleMockFKParentTableSchema &operator=(const ObSimpleMockFKParentTableSchema &src_schema);
  int assign(const ObSimpleMockFKParentTableSchema &src_schema);
  void reset();

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }

  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }

  inline void set_mock_fk_parent_table_id(const uint64_t mock_fk_parent_table_id) { mock_fk_parent_table_id_ = mock_fk_parent_table_id; }
  inline uint64_t get_mock_fk_parent_table_id() const { return mock_fk_parent_table_id_; }
  inline void set_table_id(const uint64_t mock_fk_parent_table_id) { mock_fk_parent_table_id_ = mock_fk_parent_table_id; }
  inline uint64_t get_table_id() const { return mock_fk_parent_table_id_; }

  inline int set_mock_fk_parent_table_name(const common::ObString &mock_fk_parent_table_name) { return deep_copy_str(mock_fk_parent_table_name, mock_fk_parent_table_name_); }
  inline const common::ObString &get_mock_fk_parent_table_name() const { return mock_fk_parent_table_name_; }
  inline int set_table_name(const common::ObString &mock_fk_parent_table_name) { return deep_copy_str(mock_fk_parent_table_name, mock_fk_parent_table_name_); }
  inline const common::ObString &get_table_name_str() const { return mock_fk_parent_table_name_; }

  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }

  int check_if_oracle_compat_mode(bool &is_oracle_mode) const { is_oracle_mode = false; return OB_SUCCESS; };

  inline ObMockFKParentTableKey get_mock_parent_table_key() const
  { return ObMockFKParentTableKey(tenant_id_, mock_fk_parent_table_id_); }
  int64_t get_convert_size() const;

  VIRTUAL_TO_STRING_KV(K_(tenant_id),
                       K_(database_id),
                       K_(mock_fk_parent_table_id),
                       K_(mock_fk_parent_table_name),
                       K_(schema_version));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t mock_fk_parent_table_id_;
  common::ObString mock_fk_parent_table_name_;
  int64_t schema_version_;
};

class ObMockFKParentTableSchema : public ObSimpleMockFKParentTableSchema
{
public:
  ObMockFKParentTableSchema();
  explicit ObMockFKParentTableSchema(common::ObIAllocator *allocator);
  virtual ~ObMockFKParentTableSchema();
  ObMockFKParentTableSchema &operator=(const ObMockFKParentTableSchema &src_schema);
  int assign(const ObMockFKParentTableSchema &src_schema);
  ObMockFKParentTableSchema(const ObMockFKParentTableSchema &src_schema);
  void reset();

  // foreign_key_infos_
  int set_foreign_key_infos(const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos_);
  int add_foreign_key_info(const ObForeignKeyInfo &foreign_key_info);
  inline const common::ObIArray<ObForeignKeyInfo> &get_foreign_key_infos() const { return foreign_key_infos_; }
  inline common::ObIArray<ObForeignKeyInfo> &get_foreign_key_infos() { return foreign_key_infos_; }
  inline void reset_foreign_key_infos() { foreign_key_infos_.reset(); }

  // column_array_
  int set_column_array(const ObMockFKParentTableColumnArray &other);
  int add_column_info_to_column_array(const std::pair<uint64_t, common::ObString> &column_info);
  void reset_column_array() { column_array_.reset(); }
  inline const ObMockFKParentTableColumnArray &get_column_array() const {return column_array_;}
  void get_column_name_by_column_id(const uint64_t column_id, common::ObString &column_name, bool &is_column_exist) const;
  void get_column_id_by_column_name(const common::ObString column_name, uint64_t &column_id, bool &is_column_exist) const;
  int reconstruct_column_array_by_foreign_key_infos(const ObMockFKParentTableSchema* orig_mock_fk_parent_table_ptr);

  inline void set_operation_type(const ObMockFKParentTableOperationType operation_type) { operation_type_ = operation_type; }
  inline ObMockFKParentTableOperationType get_operation_type() const { return operation_type_; }

  int64_t get_convert_size() const;

  VIRTUAL_TO_STRING_KV(
      K(get_tenant_id()),
      K(get_database_id()),
      K(get_mock_fk_parent_table_id()),
      K(get_mock_fk_parent_table_name()),
      K(get_schema_version()),
      K_(foreign_key_infos),
      K_(column_array),
      K_(operation_type));
private:
  common::ObArray<ObForeignKeyInfo> foreign_key_infos_;
  ObMockFKParentTableColumnArray column_array_;
  ObMockFKParentTableOperationType operation_type_;
};

class ObTenantRlsPolicyId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantRlsPolicyId() : ObTenantCommonSchemaId() {}
  ObTenantRlsPolicyId(const uint64_t tenant_id, const uint64_t rls_policy_id)
    : ObTenantCommonSchemaId(tenant_id, rls_policy_id) {}
};

class ObTenantRlsGroupId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantRlsGroupId() : ObTenantCommonSchemaId() {}
  ObTenantRlsGroupId(const uint64_t tenant_id, const uint64_t rls_group_id)
    : ObTenantCommonSchemaId(tenant_id, rls_group_id) {}
};

class ObTenantRlsContextId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantRlsContextId() : ObTenantCommonSchemaId() {}
  ObTenantRlsContextId(const uint64_t tenant_id, const uint64_t rls_context_id)
    : ObTenantCommonSchemaId(tenant_id, rls_context_id) {}
};

#define RLS_POLICY_SELECT_FLAG (1 << 0)
#define RLS_POLICY_INSERT_FLAG (1 << 1)
#define RLS_POLICY_UPDATE_FLAG (1 << 2)
#define RLS_POLICY_DELETE_FLAG (1 << 3)
#define RLS_POLICY_STATIC_FLAG (1 << 4)
#define RLS_POLICY_SYNONYM_FLAG (1 << 5)
#define RLS_POLICY_SHARE_STATIC_FLAG (1 << 6)
#define RLS_POLICY_CONTEXT_FLAG (1 << 7)
#define RLS_POLICY_SHARE_CONTEXT_FLAG (1 << 8)
#define RLS_POLICY_LONG_PREDICATE_FLAG (1 << 9)
#define RLS_POLICY_RELEVANT_COLUMN_FLAG (1 << 10)
#define RLS_POLICY_INDEX_FLAG (1 << 11)
#define RLS_POLICY_SEC_ALL_ROWS_FLAG (1 << 12)

class ObRlsSecColumnSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  ObRlsSecColumnSchema();
  explicit ObRlsSecColumnSchema(common::ObIAllocator *allocator);
  ObRlsSecColumnSchema(const ObRlsSecColumnSchema &src_schema);
  virtual ~ObRlsSecColumnSchema();
  ObRlsSecColumnSchema &operator =(const ObRlsSecColumnSchema &other);
  int assign(const ObRlsSecColumnSchema &other);
  bool operator ==(const ObRlsSecColumnSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(rls_policy_id),
               K_(column_id),
               K_(schema_version));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_rls_policy_id(const uint64_t rls_policy_id) { rls_policy_id_ = rls_policy_id; }
  inline void set_column_id(const uint64_t column_id) { column_id_ = column_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_rls_policy_id() const { return rls_policy_id_; }
  inline uint64_t get_column_id() const { return column_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_operation_table_id() const { return rls_policy_id_; }
private:
  uint64_t tenant_id_;
  uint64_t rls_policy_id_;
  uint64_t column_id_;
  int64_t schema_version_;
};

class ObRlsPolicySchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  ObRlsPolicySchema();
  explicit ObRlsPolicySchema(common::ObIAllocator *allocator);
  ObRlsPolicySchema(const ObRlsPolicySchema &src_schema);
  virtual ~ObRlsPolicySchema();
  ObRlsPolicySchema &operator =(const ObRlsPolicySchema &other);
  int assign(const ObRlsPolicySchema &other);
  bool operator ==(const ObRlsPolicySchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(rls_policy_id),
               K_(schema_version),
               K_(table_id),
               K_(rls_group_id),
               K_(stmt_type),
               K_(check_opt),
               K_(enable_flag),
               K_(policy_name),
               K_(policy_function_schema),
               K_(policy_package_name),
               K_(policy_function_name),
               "sec_columns", ObArrayWrap<ObRlsSecColumnSchema*>(sec_column_array_, column_cnt_));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_rls_policy_id(const uint64_t rls_policy_id) { rls_policy_id_ = rls_policy_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  inline void set_rls_group_id(const uint64_t rls_group_id) { rls_group_id_ = rls_group_id; }
  inline void set_stmt_type(const int64_t stmt_type) { stmt_type_ = stmt_type; }
  inline void set_check_opt(bool check_opt) { check_opt_ = check_opt; }
  inline void set_enable_flag(bool enable_flag) { enable_flag_ = enable_flag; }
  inline int set_policy_name(const common::ObString &policy_name) { return deep_copy_str(policy_name, policy_name_); }
  inline int set_policy_function_schema(const common::ObString &policy_function_schema) { return deep_copy_str(policy_function_schema, policy_function_schema_); }
  inline int set_policy_package_name(const common::ObString &policy_package_name) { return deep_copy_str(policy_package_name, policy_package_name_); }
  inline int set_policy_function_name(const common::ObString &policy_function_name) { return deep_copy_str(policy_function_name, policy_function_name_); }
  int add_sec_column(const ObRlsSecColumnSchema &sec_column);
  int set_ids_cascade();
  int rebuild_with_table_schema(const ObRlsPolicySchema &src_schema,
                                const share::schema::ObTableSchema &table_schema);

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_rls_policy_id() const { return rls_policy_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline uint64_t get_rls_group_id() const { return rls_group_id_; }
  inline int64_t get_stmt_type() const { return stmt_type_; }
  inline bool get_check_opt() const { return check_opt_; }
  inline bool get_enable_flag() const { return enable_flag_; }
  inline const common::ObString &get_policy_name() const { return policy_name_; }
  inline const common::ObString &get_policy_function_schema() const { return policy_function_schema_; }
  inline const common::ObString &get_policy_package_name() const { return policy_package_name_; }
  inline const common::ObString &get_policy_function_name() const { return policy_function_name_; }
  inline int64_t get_sec_column_count() const { return column_cnt_; }
  const ObRlsSecColumnSchema* get_sec_column_by_idx(const int64_t idx) const;
  inline ObTenantRlsPolicyId get_sort_key() const { return ObTenantRlsPolicyId(tenant_id_, rls_policy_id_); }
  inline uint64_t get_operation_table_id() const { return rls_policy_id_; }
  inline bool is_column_level_policy() const { return column_cnt_ > 0; }

  inline bool is_context_sensitive() const
  { return stmt_type_ & (RLS_POLICY_CONTEXT_FLAG | RLS_POLICY_SHARE_CONTEXT_FLAG); };
  inline bool has_stmt_type_flag(const int64_t flag) const { return flag == (stmt_type_ & flag); };
  inline void add_stmt_type_flag(const int64_t flag) { stmt_type_ |= flag; }
  inline void del_stmt_type_flag(const int64_t flag) { stmt_type_ &= ~flag; }
  inline bool has_partial_stmt_type_flag(const int64_t flag) const { return stmt_type_ & flag; };
private:
  static const int64_t DEFAULT_ARRAY_CAPACITY = 16;
  uint64_t tenant_id_;
  uint64_t rls_policy_id_;
  int64_t schema_version_;
  uint64_t table_id_;
  uint64_t rls_group_id_;
  int64_t stmt_type_;
  bool check_opt_;
  bool enable_flag_;
  common::ObString policy_name_;
  common::ObString policy_function_schema_;
  common::ObString policy_package_name_;
  common::ObString policy_function_name_;
  ObRlsSecColumnSchema **sec_column_array_;
  int64_t column_array_capacity_;
  int64_t column_cnt_;
};

class ObRlsGroupSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  ObRlsGroupSchema();
  explicit ObRlsGroupSchema(common::ObIAllocator *allocator);
  ObRlsGroupSchema(const ObRlsGroupSchema &src_schema);
  virtual ~ObRlsGroupSchema();
  ObRlsGroupSchema &operator =(const ObRlsGroupSchema &other);
  int assign(const ObRlsGroupSchema &other);
  bool operator ==(const ObRlsGroupSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(rls_group_id),
               K_(schema_version),
               K_(table_id),
               K_(policy_group_name));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_rls_group_id(const uint64_t rls_group_id) { rls_group_id_ = rls_group_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  inline int set_policy_group_name(const common::ObString &policy_group_name) { return deep_copy_str(policy_group_name, policy_group_name_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_rls_group_id() const { return rls_group_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline const common::ObString &get_policy_group_name() const { return policy_group_name_; }
  inline ObTenantRlsGroupId get_sort_key() const { return ObTenantRlsGroupId(tenant_id_, rls_group_id_); }
  inline uint64_t get_operation_table_id() const { return rls_group_id_; }
private:
  uint64_t tenant_id_;
  uint64_t rls_group_id_;
  int64_t schema_version_;
  uint64_t table_id_;
  common::ObString policy_group_name_;
};

class ObRlsContextSchema : public ObSchema
{//simple schema
  OB_UNIS_VERSION(1);
public:
  ObRlsContextSchema();
  explicit ObRlsContextSchema(common::ObIAllocator *allocator);
  ObRlsContextSchema(const ObRlsContextSchema &src_schema);
  virtual ~ObRlsContextSchema();
  ObRlsContextSchema &operator =(const ObRlsContextSchema &other);
  int assign(const ObRlsContextSchema &other);
  bool operator ==(const ObRlsContextSchema &other) const;
  TO_STRING_KV(K_(tenant_id),
               K_(rls_context_id),
               K_(schema_version),
               K_(table_id),
               K_(context_name),
               K_(attribute));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_rls_context_id(const uint64_t rls_context_id) { rls_context_id_ = rls_context_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  inline int set_context_name(const common::ObString &context_name) { return deep_copy_str(context_name, context_name_); }
  inline int set_attribute(const common::ObString &attribute) { return deep_copy_str(attribute, attribute_); }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_rls_context_id() const { return rls_context_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline const common::ObString &get_context_name() const { return context_name_; }
  inline const common::ObString &get_attribute() const { return attribute_; }
  inline ObTenantRlsContextId get_sort_key() const { return ObTenantRlsContextId(tenant_id_, rls_context_id_); }
  inline uint64_t get_operation_table_id() const { return rls_context_id_; }
private:
  uint64_t tenant_id_;
  uint64_t rls_context_id_;
  int64_t schema_version_;
  uint64_t table_id_;
  common::ObString context_name_;
  common::ObString attribute_;
};

class ObTableLatestSchemaVersion
{
public:
  ObTableLatestSchemaVersion();
  virtual ~ObTableLatestSchemaVersion() {}
  void reset();
  int init(const uint64_t table_id, const int64_t schema_version, const bool is_deleted);
  bool is_valid() const;
  int assign(const ObTableLatestSchemaVersion &other);
  bool is_deleted() const { return is_deleted_; }
  uint64_t get_table_id() const { return table_id_; }
  int64_t get_schema_version() const { return schema_version_; }

  VIRTUAL_TO_STRING_KV(K_(table_id), K_(schema_version), K_(is_deleted));
private:
  uint64_t table_id_;
  int64_t schema_version_;
  bool is_deleted_;
};

class ObIndexNameInfo
{
public:
  ObIndexNameInfo();
  ~ObIndexNameInfo() {}
  void reset();

  int init(common::ObIAllocator &allocator,
           const share::schema::ObSimpleTableSchemaV2 &index_schema);
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_database_id() const { return database_id_; }
  uint64_t get_data_table_id() const { return data_table_id_; }
  uint64_t get_index_id() const { return index_id_; }
  const ObString& get_index_name() const { return index_name_; }
  const ObString& get_original_index_name() const { return original_index_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id),
               K_(data_table_id), K_(index_id),
               K_(index_name), K_(original_index_name));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t data_table_id_;
  uint64_t index_id_;
  ObString index_name_;
  ObString original_index_name_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexNameInfo);
};

template<class K, class V>
struct GetIndexNameKey
{
  void operator()(const K &k, const V &v)
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template<>
struct GetIndexNameKey<ObIndexSchemaHashWrapper, ObIndexNameInfo*>
{
  ObIndexSchemaHashWrapper operator()(const ObIndexNameInfo *index_name_info) const;
};

typedef common::hash::ObPointerHashMap<ObIndexSchemaHashWrapper, ObIndexNameInfo*, GetIndexNameKey, 1024> ObIndexNameMap;

}//namespace schema
}//namespace share
}//namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */

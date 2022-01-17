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

#ifndef OCEANBASE_SERVER_SCHEMA_SERVICE_H_
#define OCEANBASE_SERVER_SCHEMA_SERVICE_H_

#include "lib/allocator/ob_mod_define.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_iteratable_hashmap.h"  //ObIteratableHashMap
#include "lib/hash/ob_hashmap.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_array_wrap.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_mem_mgr.h"

namespace oceanbase {
namespace common {
class ObInnerTableBackupGuard;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObCommonConfig;
class ObKVCacheHandle;
class ObTimeoutCtx;
}  // namespace common
namespace share {
typedef int (*schema_create_func)(share::schema::ObTableSchema& table_schema);
namespace schema {
class ObSchemaMgr;

enum NewVersionType { CUR_NEW_VERSION = 0, GEN_NEW_VERSION = 1 };

struct SchemaKey {
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  uint64_t outline_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  int64_t schema_version_;
  uint64_t synonym_id_;
  uint64_t sequence_id_;
  common::ObString sequence_name_;
  uint64_t udf_id_;
  common::ObString udf_name_;
  uint64_t profile_id_;
  uint64_t grantee_id_;
  uint64_t grantor_id_;
  uint64_t col_id_;
  uint64_t obj_type_;
  uint64_t dblink_id_;

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(database_id), K_(tablegroup_id), K_(table_id), K_(outline_id),
      K_(database_name), K_(table_name), K_(schema_version), K_(synonym_id), K_(sequence_id), K_(sequence_name),
      K_(udf_id), K_(udf_name), K_(profile_id), K_(grantee_id), K_(grantor_id), K_(col_id), K_(obj_type),
      K_(dblink_id));

  SchemaKey()
      : tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        tablegroup_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        outline_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        synonym_id_(common::OB_INVALID_ID),
        sequence_id_(common::OB_INVALID_ID),
        sequence_name_(),
        udf_id_(common::OB_INVALID_ID),
        udf_name_(),
        profile_id_(common::OB_INVALID_ID),
        grantee_id_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        col_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        dblink_id_(common::OB_INVALID_ID)
  {}
  static bool cmp_with_tenant_id(const SchemaKey& a, const SchemaKey& b)
  {
    return a.tenant_id_ < b.tenant_id_;
  }
  uint64_t get_tenant_key() const
  {
    return tenant_id_;
  }
  ObTenantUserId get_user_key() const
  {
    return ObTenantUserId(tenant_id_, user_id_);
  }
  ObTenantDatabaseId get_database_key() const
  {
    return ObTenantDatabaseId(tenant_id_, database_id_);
  }
  ObTenantTablegroupId get_tablegroup_key() const
  {
    return ObTenantTablegroupId(tenant_id_, tablegroup_id_);
  }
  ObTenantTableId get_table_key() const
  {
    return ObTenantTableId(tenant_id_, table_id_);
  }
  ObTenantOutlineId get_outline_key() const
  {
    return ObTenantOutlineId(tenant_id_, outline_id_);
  }
  ObTenantSynonymId get_synonym_key() const
  {
    return ObTenantSynonymId(tenant_id_, synonym_id_);
  }
  ObTenantSequenceId get_sequence_key() const
  {
    return ObTenantSequenceId(tenant_id_, sequence_id_);
  }
  ObOriginalDBKey get_db_priv_key() const
  {
    return ObOriginalDBKey(tenant_id_, user_id_, database_name_);
  }
  ObTablePrivSortKey get_table_priv_key() const
  {
    return ObTablePrivSortKey(tenant_id_, user_id_, database_name_, table_name_);
  }
  ObTenantUDFId get_udf_key() const
  {
    return ObTenantUDFId(tenant_id_, udf_name_);
  }
  uint64_t get_sys_variable_key() const
  {
    return tenant_id_;
  }
  ObTenantProfileId get_profile_key() const
  {
    return ObTenantProfileId(tenant_id_, profile_id_);
  }
  ObSysPrivKey get_sys_priv_key() const
  {
    return ObSysPrivKey(tenant_id_, grantee_id_);
  }
  ObObjPrivSortKey get_obj_priv_key() const
  {
    return ObObjPrivSortKey(tenant_id_, table_id_, obj_type_, col_id_, grantor_id_, grantee_id_);
  }
  ObTenantDbLinkId get_dblink_key() const
  {
    return ObTenantDbLinkId(tenant_id_, dblink_id_);
  }
};

struct VersionHisKey {
  VersionHisKey(const ObSchemaType schema_type = OB_MAX_SCHEMA, const uint64_t schema_id = common::OB_INVALID_ID)
      : schema_type_(schema_type), schema_id_(schema_id)
  {}
  inline bool operator==(const VersionHisKey& other) const
  {
    return schema_type_ == other.schema_type_ && schema_id_ == other.schema_id_;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_code = 0;
    hash_code = common::murmurhash(&schema_type_, sizeof(schema_type_), hash_code);
    hash_code = common::murmurhash(&schema_id_, sizeof(schema_id_), hash_code);
    return hash_code;
  }
  inline bool is_valid() const
  {
    return OB_MAX_SCHEMA != schema_type_ && common::OB_INVALID_ID != schema_id_;
  }
  ObSchemaType schema_type_;
  int64_t schema_id_;
  TO_STRING_KV(K(schema_type_), K(schema_id_));
};
const static int MAX_CACHED_VERSION_CNT = 16;
struct VersionHisVal {
  VersionHisVal()
      : snapshot_version_(common::OB_INVALID_VERSION),
        is_deleted_(false),
        valid_cnt_(0),
        min_version_(common::OB_INVALID_VERSION)
  {}
  void reset()
  {
    snapshot_version_ = common::OB_INVALID_VERSION;
    is_deleted_ = false;
    valid_cnt_ = 0;
    min_version_ = common::OB_INVALID_VERSION;
  }
  int64_t snapshot_version_;
  bool is_deleted_;
  int64_t versions_[MAX_CACHED_VERSION_CNT];
  int valid_cnt_;
  int64_t min_version_;
  TO_STRING_KV(K(snapshot_version_), K(is_deleted_), "versions", common::ObArrayWrap<int64_t>(versions_, valid_cnt_),
      K(min_version_));
};

class ObMaxSchemaVersionFetcher {
public:
  ObMaxSchemaVersionFetcher() : max_schema_version_(common::OB_INVALID_VERSION)
  {}
  virtual ~ObMaxSchemaVersionFetcher()
  {}

  int operator()(common::hash::HashMapPair<uint64_t, ObSchemaMgr*>& entry);
  int64_t get_max_schema_version()
  {
    return max_schema_version_;
  }

private:
  int64_t max_schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObMaxSchemaVersionFetcher);
};

class ObSchemaVersionGetter {
public:
  ObSchemaVersionGetter() : schema_version_(common::OB_INVALID_VERSION)
  {}
  virtual ~ObSchemaVersionGetter()
  {}

  int operator()(common::hash::HashMapPair<uint64_t, ObSchemaMgr*>& entry);
  int64_t get_schema_version()
  {
    return schema_version_;
  }

private:
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaVersionGetter);
};

class ObSchemaGetterGuard;

class ObServerSchemaService {
public:
#define ALLOW_NEXT_LOG() ObTaskController::get().allow_next_syslog();
#define SCHEMA_KEY_FUNC(SCHEMA)                                                                \
  struct SCHEMA##_key_hash_func {                                                              \
    uint64_t operator()(const SchemaKey& schema_key) const                                     \
    {                                                                                          \
      return common::murmurhash(&schema_key.SCHEMA##_id_, sizeof(schema_key.SCHEMA##_id_), 0); \
    }                                                                                          \
  };                                                                                           \
  struct SCHEMA##_key_equal_to {                                                               \
    bool operator()(const SchemaKey& a, const SchemaKey& b) const                              \
    {                                                                                          \
      return a.SCHEMA##_id_ == b.SCHEMA##_id_;                                                 \
    }                                                                                          \
  };
  SCHEMA_KEY_FUNC(tenant);
  SCHEMA_KEY_FUNC(user);
  SCHEMA_KEY_FUNC(tablegroup);
  SCHEMA_KEY_FUNC(database);
  SCHEMA_KEY_FUNC(table);
  SCHEMA_KEY_FUNC(outline);
  SCHEMA_KEY_FUNC(synonym);
  SCHEMA_KEY_FUNC(sequence);
  SCHEMA_KEY_FUNC(profile);
  SCHEMA_KEY_FUNC(dblink);
#undef SCHEMA_KEY_FUNC

  struct udf_key_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      return common::murmurhash(schema_key.udf_name_.ptr(), schema_key.udf_name_.length(), 0);
    }
  };

  struct udf_key_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.udf_name_ == b.udf_name_;
    }
  };

  struct db_priv_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      uint64_t hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.user_id_, sizeof(schema_key.user_id_), hash_code);
      hash_code = common::murmurhash(schema_key.database_name_.ptr(), schema_key.database_name_.length(), hash_code);
      return hash_code;
    }
  };
  struct db_priv_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.tenant_id_ == b.tenant_id_ && a.user_id_ == b.user_id_ && a.database_name_ == b.database_name_;
    }
  };
  struct table_priv_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      uint64_t hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.user_id_, sizeof(schema_key.user_id_), hash_code);
      hash_code = common::murmurhash(schema_key.database_name_.ptr(), schema_key.database_name_.length(), hash_code);
      hash_code = common::murmurhash(schema_key.table_name_.ptr(), schema_key.table_name_.length(), hash_code);
      return hash_code;
    }
  };
  struct table_priv_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.tenant_id_ == b.tenant_id_ && a.user_id_ == b.user_id_ && a.database_name_ == b.database_name_ &&
             a.table_name_ == b.table_name_;
    }
  };
  struct obj_priv_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      uint64_t hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.table_id_, sizeof(schema_key.table_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.obj_type_, sizeof(schema_key.obj_type_), hash_code);
      hash_code = common::murmurhash(&schema_key.col_id_, sizeof(schema_key.col_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.grantor_id_, sizeof(schema_key.grantor_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.grantee_id_, sizeof(schema_key.grantee_id_), hash_code);
      return hash_code;
    }
  };
  struct obj_priv_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.tenant_id_ == b.tenant_id_ && a.table_id_ == b.table_id_ && a.obj_type_ == b.obj_type_ &&
             a.col_id_ == b.col_id_ && a.grantor_id_ == b.grantor_id_ && a.grantee_id_ == b.grantee_id_;
    }
  };
  struct sys_variable_key_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      return common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), 0);
    }
  };

  struct sys_variable_key_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.tenant_id_ == b.tenant_id_;
    }
  };
  struct sys_priv_hash_func {
    uint64_t operator()(const SchemaKey& schema_key) const
    {
      uint64_t hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), hash_code);
      hash_code = common::murmurhash(&schema_key.grantee_id_, sizeof(schema_key.grantee_id_), hash_code);
      return hash_code;
    }
  };
  struct sys_priv_equal_to {
    bool operator()(const SchemaKey& a, const SchemaKey& b) const
    {
      return a.tenant_id_ == b.tenant_id_ && a.grantee_id_ == b.grantee_id_;
    }
  };
#define SCHEMA_KEYS_DEF(SCHEMA, SCHEMA_KEYS)                                                                 \
  typedef common::hash::                                                                                     \
      ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, SCHEMA##_key_hash_func, SCHEMA##_key_equal_to> \
          SCHEMA_KEYS;
  SCHEMA_KEYS_DEF(tenant, TenantKeys);
  SCHEMA_KEYS_DEF(user, UserKeys);
  SCHEMA_KEYS_DEF(database, DatabaseKeys);
  SCHEMA_KEYS_DEF(tablegroup, TablegroupKeys);
  SCHEMA_KEYS_DEF(table, TableKeys);
  SCHEMA_KEYS_DEF(outline, OutlineKeys);
  SCHEMA_KEYS_DEF(synonym, SynonymKeys);
  SCHEMA_KEYS_DEF(udf, UdfKeys);
  SCHEMA_KEYS_DEF(sequence, SequenceKeys);
  SCHEMA_KEYS_DEF(sys_variable, SysVariableKeys);
  SCHEMA_KEYS_DEF(profile, ProfileKeys);
  SCHEMA_KEYS_DEF(dblink, DbLinkKeys);
#undef SCHEMA_KEYS_DEF
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, db_priv_hash_func, db_priv_equal_to>
      DBPrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, table_priv_hash_func,
      table_priv_equal_to>
      TablePrivKeys;

  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, sys_priv_hash_func, sys_priv_equal_to>
      SysPrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, obj_priv_hash_func, obj_priv_equal_to>
      ObjPrivKeys;

  struct AllSchemaKeys {
    // tenant
    TenantKeys new_tenant_keys_;
    TenantKeys alter_tenant_keys_;
    TenantKeys del_tenant_keys_;
    // user
    UserKeys new_user_keys_;
    UserKeys del_user_keys_;
    // database
    DatabaseKeys new_database_keys_;
    DatabaseKeys del_database_keys_;
    // tablegroup
    TablegroupKeys new_tablegroup_keys_;
    TablegroupKeys del_tablegroup_keys_;
    // table
    TableKeys new_table_keys_;
    TableKeys del_table_keys_;
    // outline
    OutlineKeys new_outline_keys_;
    OutlineKeys del_outline_keys_;
    // db_priv
    DBPrivKeys new_db_priv_keys_;
    DBPrivKeys del_db_priv_keys_;
    // table_priv
    TablePrivKeys new_table_priv_keys_;
    TablePrivKeys del_table_priv_keys_;
    // for core and sys schema
    common::ObArray<uint64_t> core_table_ids_;
    common::ObArray<uint64_t> sys_table_ids_;
    // synonym
    SynonymKeys new_synonym_keys_;
    SynonymKeys del_synonym_keys_;
    // udf
    UdfKeys new_udf_keys_;
    UdfKeys del_udf_keys_;
    // sequence
    SequenceKeys new_sequence_keys_;
    SequenceKeys del_sequence_keys_;
    // sys_variable
    SysVariableKeys new_sys_variable_keys_;
    SysVariableKeys del_sys_variable_keys_;
    // drop tenant info
    TenantKeys add_drop_tenant_keys_;
    TenantKeys del_drop_tenant_keys_;

    // profile
    ProfileKeys new_profile_keys_;
    ProfileKeys del_profile_keys_;

    // sys_priv
    SysPrivKeys new_sys_priv_keys_;
    SysPrivKeys del_sys_priv_keys_;

    // obj_priv
    ObjPrivKeys new_obj_priv_keys_;
    ObjPrivKeys del_obj_priv_keys_;

    // dblink
    DbLinkKeys new_dblink_keys_;
    DbLinkKeys del_dblink_keys_;

    void reset();
    int create(int64_t bucket_size);
  };

  struct AllSimpleIncrementSchema {
    common::ObArray<ObSimpleTenantSchema> simple_tenant_schemas_;  // new tenant
    common::ObArray<ObSimpleTenantSchema> alter_tenant_schemas_;
    common::ObArray<ObSimpleDatabaseSchema> simple_database_schemas_;
    common::ObArray<ObSimpleTableSchemaV2> simple_table_schemas_;
    common::ObArray<ObSimpleTablegroupSchema> simple_tablegroup_schemas_;
    common::ObArray<ObSimpleOutlineSchema> simple_outline_schemas_;
    common::ObArray<ObSimpleSynonymSchema> simple_synonym_schemas_;
    common::ObArray<ObSimpleUDFSchema> simple_udf_schemas_;
    common::ObArray<ObSequenceSchema> simple_sequence_schemas_;
    common::ObArray<ObSimpleUserSchema> simple_user_schemas_;
    common::ObArray<ObDbLinkSchema> simple_dblink_schemas_;
    common::ObArray<ObDBPriv> simple_db_priv_schemas_;
    common::ObArray<ObTablePriv> simple_table_priv_schemas_;
    common::ObArray<ObSimpleSysVariableSchema> simple_sys_variable_schemas_;
    common::ObArray<ObProfileSchema> simple_profile_schemas_;
    common::ObArray<ObSysPriv> simple_sys_priv_schemas_;
    common::ObArray<ObObjPriv> simple_obj_priv_schemas_;

    common::ObArray<ObTableSchema> core_tables_;
    common::ObArray<ObTableSchema*> sys_tables_;
    common::ObArenaAllocator allocator_;
  };

public:
  int init(common::ObMySQLProxy* sql_proxy, common::ObDbLinkProxy* dblink_proxy, const common::ObCommonConfig* config);
  explicit ObServerSchemaService(bool enable_backup = false);
  virtual ~ObServerSchemaService();
  // get full schema instead of using patch, we call this automatically and do not expect user to
  // call this(if you really need this , use friend class, such as chunkserver)
  // construct core schema from hard code
  int fill_all_core_table_schema(ObSchemaMgr& schema_mgr_for_cache);
  virtual int get_tenant_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  int64_t get_table_count() const;
  // the schema service should be thread safe
  ObSchemaService* get_schema_service(void) const;
  void dump_schema_manager() const;

  //----restore schema related, do not use it now-----//
  inline void set_guard(common::ObInnerTableBackupGuard* guard);

  virtual bool is_sys_full_schema() const
  {
    return increment_basepoint_set_;
  }
  virtual bool is_tenant_full_schema(const uint64_t tenant_id) const = 0;

  // schema_cache
  bool fetch_incremental_schemas()
  {
    return true;
  }
  ObSchema fetch_schema()
  {
    return ObSchema();
  }

  // public utils
  virtual int get_schema_version_in_inner_table(common::ObISQLClient& sql_client,
      const share::schema::ObRefreshSchemaStatus& schema_status, int64_t& target_version);

  int query_tenant_status(const uint64_t tenant_id, TenantStatus& tenant_status);
  int query_table_status(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, const uint64_t table_id, const bool is_pg, TableStatus& status);
  int query_partition_status_from_sys_table(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const common::ObPartitionKey& pkey, const bool is_sub_part_template, PartitionStatus& status);
  int construct_schema_version_history(const ObRefreshSchemaStatus& schema_status, const int64_t snapshot_version,
      const VersionHisKey& key, VersionHisVal& val);
  int get_tenant_all_increment_schema_ids(const uint64_t tenant_id, const int64_t base_version,
      const int64_t new_schema_version, const ObSchemaOperationCategory& schema_category,
      common::ObIArray<uint64_t>& schema_ids);

  int get_baseline_schema_version(int64_t& baseline_schema_version);
  int update_baseline_schema_version(const int64_t baseline_schema_version);

  int get_refresh_schema_info(ObRefreshSchemaInfo& schema_info);

  // for slave cluster rs used only, get increment schema operation
  virtual int get_increment_schema_operations(const ObRefreshSchemaStatus& schema_status, const int64_t base_version,
      ObSchemaService::SchemaOperationSetWithAlloc& schema_operations);

  int get_mock_schema_info(
      const uint64_t schema_id, const ObSchemaType schema_type, ObMockSchemaInfo& mock_schema_info);
  int set_timeout_ctx(common::ObTimeoutCtx& ctx);

  int check_tenant_can_use_new_table(const uint64_t tenant_id, bool& can) const;

protected:
  bool check_inner_stat() const;
  int check_stop() const;
  virtual int fallback_schema_mgr(
      const ObRefreshSchemaStatus& schema_status, ObSchemaMgr& schema_mgr, const int64_t schema_version);
  // refresh schema and update schema_manager_for_cache_
  // leave the job of schema object copy to get_schema func
  int refresh_schema(int64_t expected_version);
  int refresh_schema(const ObRefreshSchemaStatus& schema_status);
  int init_basic_schema();
  int init_all_core_schema();
  virtual int publish_schema() = 0;

  virtual int publish_schema(const uint64_t tenant_id) = 0;
  virtual int init_multi_version_schema_struct(const uint64_t tenant_id) = 0;

  int init_schema_struct(const uint64_t tenant_id);
  int init_sys_basic_schema();
  int init_tenant_basic_schema(const uint64_t tenant_id);

  int destroy_schema_struct(uint64_t tenant_id);

private:
  virtual int destroy();

  // stats table instances in schem mgrs
  class ObTable {
  public:
    ObTable() : version_(0), combined_id_(0){};
    virtual ~ObTable(){};
    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&version_, sizeof(uint64_t), 0);
      hash_val = common::murmurhash(&combined_id_, sizeof(uint64_t), hash_val);
      return hash_val;
    }

    bool operator==(const ObTable& tbl) const
    {
      bool equal = false;
      if (version_ == tbl.version_ && combined_id_ == tbl.combined_id_) {
        equal = true;
      }
      return equal;
    }

    int64_t version_;
    int64_t combined_id_;
  };
  // not used now
  struct AllIncrementSchema {
    common::ObArenaAllocator allocator_;
    common::ObArray<ObTableSchema*> table_schema_;
    common::ObArray<ObDatabaseSchema> db_schema_;
    common::ObArray<ObTablegroupSchema> tg_schema_;
    common::ObArray<uint64_t> max_used_tids_;  // max used table_id of each tenant
    // For managing privileges
    common::ObArray<ObTenantSchema> tenant_info_array_;
    common::ObArray<ObUserInfo> user_info_array_;
    common::ObArray<ObDBPriv> db_priv_array_;
    common::ObArray<ObTablePriv> table_priv_array_;

    AllIncrementSchema(const char* label = common::ObModIds::OB_SCHEMA) : allocator_(label)
    {}
    void reset();
  };

  enum RefreshSchemaType {
    RST_FULL_SCHEMA_IDS = 0,
    RST_FULL_SCHEMA_ALL,
    RST_INCREMENT_SCHEMA_IDS,
    RST_INCREMENT_SCHEMA_ALL
  };

  int refresh_increment_schema(int64_t expected_version);
  int refresh_full_schema(int64_t expected_version);

  int refresh_sys_increment_schema(const ObRefreshSchemaStatus& schema_status);
  int refresh_tenant_increment_schema(const ObRefreshSchemaStatus& schema_status);

  int refresh_sys_full_schema(const ObRefreshSchemaStatus& schema_status);
  int refresh_tenant_full_schema(const ObRefreshSchemaStatus& schema_status);
  int refresh_tenant_full_normal_schema(
      common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status, const int64_t schema_version);

#define GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(SCHEMA) \
  int get_increment_##SCHEMA##_keys(                  \
      const ObSchemaMgr& schema_guard, const ObSchemaOperation& schema_operation, AllSchemaKeys& schema_ids);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(tenant);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(user);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(database);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(tablegroup);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(table);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(outline);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(db_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(table_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(synonym);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(udf);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sequence);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sys_variable);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(profile);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sys_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(obj_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(dblink);
#undef GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE

  // replay log
  int replay_log(const ObSchemaMgr& schema_mgr, const ObSchemaService::SchemaOperationSetWithAlloc& schema_operations,
      AllSchemaKeys& schema_keys);
  int replay_log_reversely(const ObSchemaMgr& schema_mgr,
      const ObSchemaService::SchemaOperationSetWithAlloc& schema_operations, AllSchemaKeys& schema_keys);
  int update_schema_mgr(
      common::ObISQLClient& sql_client, ObSchemaMgr& schema_mgr, const int64_t schema_version, AllSchemaKeys& all_keys);
  int update_schema_mgr(common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
      ObSchemaMgr& schema_mgr, const int64_t schema_version, AllSchemaKeys& all_keys);
  int add_tenant_schemas_to_cache(const TenantKeys& tenant_keys, common::ObISQLClient& sql_client);
  int add_sys_variable_schemas_to_cache(const SysVariableKeys& sys_variable_keys, common::ObISQLClient& sql_client);
  int fetch_increment_schemas(const AllSchemaKeys& all_keys, const int64_t schema_version,
      common::ObISQLClient& sql_client, AllSimpleIncrementSchema& simple_incre_schemas);
  int fetch_increment_schemas(const ObRefreshSchemaStatus& schema_status, const AllSchemaKeys& all_keys,
      const int64_t schema_version, common::ObISQLClient& sql_client, AllSimpleIncrementSchema& simple_incre_schemas);
  int apply_increment_schema_to_cache(
      const AllSchemaKeys& all_keys, const AllSimpleIncrementSchema& simple_incre_schemas, ObSchemaMgr& schema_mgr);
  int update_core_and_sys_schemas_in_cache(const ObSchemaMgr& schema_mgr, common::ObIArray<ObTableSchema>& core_tables,
      common::ObIArray<ObTableSchema*>& sys_tables);

  int process_tenant_space_table(ObSchemaMgr& schema_mgr, const common::ObIArray<uint64_t>& new_tenant_ids,
      const common::ObIArray<uint64_t>* del_table_ids = NULL, const common::ObIArray<uint64_t>* new_table_ids = NULL);
  int try_fetch_publish_core_schemas(const ObRefreshSchemaStatus& schema_status, const int64_t core_schema_version,
      const int64_t publish_version, common::ObISQLClient& sql_client, bool& core_schema_change);
  int try_fetch_publish_sys_schemas(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const int64_t publish_version, common::ObISQLClient& sql_client, bool& sys_schema_change);
  int refresh_full_normal_schema_v2(common::ObISQLClient& sql_client, const int64_t schema_version);

  int check_core_or_sys_schema_change(common::ObISQLClient& sql_client, const int64_t core_schema_version,
      const int64_t schema_version, bool& core_schema_change, bool& sys_schema_change);
  virtual int check_sys_schema_change(common::ObISQLClient& sql_client, const int64_t schema_version,
      const int64_t new_schema_version, bool& sys_schema_change);
  int get_table_ids(const schema_create_func* schema_creators, common::ObIArray<uint64_t>& table_ids) const;

  virtual int determine_refresh_type(void);  // add virtual for test
  bool next_refresh_type_is_full(void);
  int add_index_tids(const ObSchemaMgr& schema_mgr, ObTableSchema& table);
  int extract_core_and_sys_table_ids(
      const TableKeys& keys, common::ObIArray<uint64_t>& core_table_ids, common::ObIArray<uint64_t>& sys_table_ids);
  int renew_core_and_sys_table_schemas(common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      common::ObArray<ObTableSchema>& core_tables, common::ObArray<ObTableSchema*>& sys_tables,
      const common::ObIArray<uint64_t>* core_table_ids = NULL, const common::ObIArray<uint64_t>* sys_table_ids = NULL);
  int reload_mock_schema_info(const ObSchemaMgr& schema_mgr);
  int try_update_split_schema_version_v2(
      const share::schema::ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id);

protected:
  int update_table_columns(const uint64_t tenant_id, ObTableSchema& table_schema);
  virtual int update_schema_cache(common::ObIArray<ObTableSchema*>& schema_array, const bool is_force = false) = 0;
  virtual int update_schema_cache(common::ObIArray<ObTableSchema>& schema_array, const bool is_forece = false) = 0;
  virtual int update_schema_cache(const common::ObIArray<ObTenantSchema>& schema_array) = 0;
  virtual int update_schema_cache(const ObSysVariableSchema& schema) = 0;
  int add_tenant_schema_to_cache(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const int64_t schema_version);
  int add_sys_variable_schema_to_cache(common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, const int64_t schema_version);
  int convert_to_simple_schema(
      const common::ObIArray<ObTableSchema>& schemas, common::ObIArray<ObSimpleTableSchemaV2>& simple_schemas);
  int convert_to_simple_schema(
      const common::ObIArray<ObTableSchema*>& schemas, common::ObIArray<ObSimpleTableSchemaV2>& simple_schemas);
  int convert_to_simple_schema(const ObTableSchema& schema, ObSimpleTableSchemaV2& simple_schema);

protected:
  // core table count
  const static int64_t MIN_TABLE_COUNT = 1;
  static const int64_t REFRESH_SCHEMA_INTERVAL_US = 100 * 1000;  // 100ms
  static const int64_t MIN_SWITCH_ALLOC_CNT = 64;
  static const int64_t DEFAULT_FETCH_SCHEMA_TIMEOUT_US = 2 * 1000 * 1000;  // 2s
  static const int64_t MAX_FETCH_SCHEMA_TIMEOUT_US = 60 * 1000 * 1000;     // 60s
  bool all_core_schema_init_;
  bool all_root_schema_init_;
  bool core_schema_init_;
  bool sys_schema_init_;
  bool sys_tenant_got_;
  bool backup_succ_;              // backup success or not
  bool increment_basepoint_set_;  // has refreshed full schema as increment_basepoint or not
  common::SpinRWLock schema_manager_rwlock_;
  ObSchemaMgr* schema_mgr_for_cache_;
  ObSchemaMemMgr mem_mgr_;
  ObSchemaMemMgr mem_mgr_for_liboblog_;
  mutable lib::ObMutex mem_mgr_for_liboblog_mutex_;
  ObSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  const common::ObCommonConfig* config_;
  int64_t refresh_times_;
  char schema_file_path_[common::OB_MAX_FILE_NAME_LENGTH];
  bool enable_backup_;  // for liboblog which doesn't need
  common::ObInnerTableBackupGuard* guard_;
  bool load_sys_tenant_;
  // stats schema info for duplicated memory allocation
  common::hash::ObIteratableHashMap<ObTable, int64_t> table_stat_hash_;
  lib::ObMutex table_stat_lock_;
  const static int VERSION_HIS_MAP_BUCKET_NUM = 64 * 1024;
  common::hash::ObHashMap<VersionHisKey, VersionHisVal, common::hash::ReadWriteDefendMode> version_his_map_;

private:
  static const int64_t BUCKET_SIZE = 128;
  int64_t core_schema_version_;
  int64_t schema_version_;
  // schema_version after bootstrap succeed, it's the min schema_version can be fallbacked
  int64_t baseline_schema_version_;
  RefreshSchemaType refresh_schema_type_;

protected:
  // new schema management by tenant, need protected by lock
  const static int TENANT_MAP_BUCKET_NUM = 1024;
  const static int MOCK_SCHEMA_BUCKET_NUM = 100;
  common::hash::ObHashMap<uint64_t, bool, common::hash::ReadWriteDefendMode> refresh_full_schema_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMgr*, common::hash::ReadWriteDefendMode> schema_mgr_for_cache_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMemMgr*, common::hash::ReadWriteDefendMode> mem_mgr_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMemMgr*, common::hash::ReadWriteDefendMode> mem_mgr_for_liboblog_map_;
  common::hash::ObHashMap<uint64_t, ObMockSchemaInfo, common::hash::ReadWriteDefendMode> mock_schema_info_map_;
  common::hash::ObHashMap<uint64_t, int64_t, common::hash::ReadWriteDefendMode> schema_split_version_v2_map_;
};

inline void ObServerSchemaService::set_guard(common::ObInnerTableBackupGuard* guard)
{
  guard_ = guard;
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OCEANBASE_SERVER_SCHEMA_SERVICE_H_

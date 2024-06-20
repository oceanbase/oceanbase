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
#include "lib/hash/ob_iteratable_hashmap.h" //ObIteratableHashMap
#include "lib/hash/ob_hashmap.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_array_wrap.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_mem_mgr.h"
#include "share/schema/ob_dblink_mgr.h"
#include "share/schema/ob_directory_mgr.h"
#include "share/schema/ob_keystore_mgr.h"
#include "share/schema/ob_label_se_policy_mgr.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_package_mgr.h"
#include "share/schema/ob_priv_mgr.h"
#include "share/schema/ob_profile_mgr.h"
#include "share/schema/ob_routine_mgr.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_security_audit_mgr.h"
#include "share/schema/ob_sequence_mgr.h"
#include "share/schema/ob_synonym_mgr.h"
#include "share/schema/ob_sys_variable_mgr.h"
#include "share/schema/ob_tablespace_mgr.h"
#include "share/schema/ob_trigger_mgr.h"
#include "share/schema/ob_udf_mgr.h"
#include "share/schema/ob_udt_mgr.h"
#include "share/schema/ob_context_mgr.h"
#include "share/schema/ob_mock_fk_parent_table_mgr.h"

namespace oceanbase
{
namespace common
{
class ObInnerTableBackupGuard;
class ObMySQLTransactionaction;
class ObMySQLProxy;
class ObCommonConfig;
class ObKVCacheHandle;
class ObTimeoutCtx;
}
namespace share
{
typedef int (*schema_create_func)(share::schema::ObTableSchema &table_schema);
namespace schema
{
class ObSchemaMgr;

enum NewVersionType {
  CUR_NEW_VERSION = 0,
  GEN_NEW_VERSION = 1
};

struct SchemaKey
{
  uint64_t tenant_id_;
  union {
    uint64_t user_id_;
    uint64_t grantee_id_;
    uint64_t client_user_id_;
  };
  union {
    uint64_t database_id_;
    uint64_t grantor_id_;
    uint64_t proxy_user_id_;
  };
  common::ObString database_name_;
  uint64_t tablegroup_id_;
  union {
    uint64_t table_id_;
    uint64_t outline_id_;
    uint64_t synonym_id_;
    uint64_t routine_id_;
    uint64_t package_id_;
    uint64_t udt_id_;
    uint64_t sequence_id_;
    uint64_t keystore_id_;
    uint64_t label_se_policy_id_;
    uint64_t label_se_component_id_;
    uint64_t label_se_label_id_;
    uint64_t label_se_user_level_id_;
    uint64_t tablespace_id_;
    uint64_t trigger_id_;
    uint64_t profile_id_;
    uint64_t audit_id_;
    uint64_t dblink_id_;
    uint64_t directory_id_;
    uint64_t context_id_;
    uint64_t mock_fk_parent_table_id_;
    uint64_t rls_policy_id_;
    uint64_t rls_group_id_;
    uint64_t rls_context_id_;
    uint64_t routine_type_;
    uint64_t column_priv_id_;
  };
  union {
    common::ObString table_name_;
    common::ObString routine_name_;
    common::ObString udf_name_;
    common::ObString sequence_name_;
    common::ObString keystore_name_;
    common::ObString tablespace_name_;
    common::ObString context_namespace_;
    common::ObString mock_fk_parent_table_namespace_;
  };
  int64_t schema_version_;
  uint64_t col_id_;
  uint64_t obj_type_;

  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(database_id),
               K_(tablegroup_id),
               K_(table_id),
               K_(outline_id),
               K_(routine_name),
               K_(routine_id),
               K_(database_name),
               K_(table_name),
               K_(schema_version),
               K_(synonym_id),
               K_(package_id),
               K_(trigger_id),
               K_(sequence_id),
               K_(sequence_name),
               K_(udf_name),
               K_(udt_id),
               K_(keystore_id),
               K_(keystore_name),
               K_(label_se_policy_id),
               K_(label_se_component_id),
               K_(label_se_label_id),
               K_(label_se_user_level_id),
               K_(tablespace_id),
               K_(tablespace_name),
               K_(profile_id),
               K_(audit_id),
               K_(grantee_id),
               K_(grantor_id),
               K_(col_id),
               K_(obj_type),
               K_(dblink_id),
               K_(directory_id),
               K_(context_id),
               K_(mock_fk_parent_table_id),
               K_(rls_policy_id),
               K_(rls_group_id),
               K_(rls_context_id),
               K_(routine_type),
               K_(column_priv_id),
               K_(client_user_id),
               K_(proxy_user_id));

  SchemaKey()
    : tenant_id_(common::OB_INVALID_ID),
      user_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      database_name_(),
      tablegroup_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      table_name_(),
      schema_version_(common::OB_INVALID_VERSION),
      col_id_(common::OB_INVALID_ID),
      obj_type_(common::OB_INVALID_ID)
  {}
  static bool cmp_with_tenant_id(const SchemaKey &a, const SchemaKey &b)
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
  ObTenantRoutineId get_routine_key() const
  {
    return ObTenantRoutineId(tenant_id_, routine_id_);
  }
  ObTenantSynonymId get_synonym_key() const
  {
    return ObTenantSynonymId(tenant_id_, synonym_id_);
  }
  ObTenantPackageId get_package_key() const
  {
    return ObTenantPackageId(tenant_id_, package_id_);
  }
  ObTenantTriggerId get_trigger_key() const
  {
    return ObTenantTriggerId(tenant_id_, trigger_id_);
  }
  ObTenantSequenceId get_sequence_key() const
  {
    return ObTenantSequenceId(tenant_id_, sequence_id_);
  }
  ObTenantKeystoreId get_keystore_key() const
  {
    return ObTenantKeystoreId(tenant_id_, keystore_id_);
  }
  ObOriginalDBKey get_db_priv_key() const
  {
    return ObOriginalDBKey(tenant_id_, user_id_, database_name_);
  }
  ObTablePrivSortKey get_table_priv_key() const
  {
    return ObTablePrivSortKey(tenant_id_, user_id_, database_name_, table_name_);
  }
  ObRoutinePrivSortKey get_routine_priv_key() const
  {
    return ObRoutinePrivSortKey(tenant_id_, user_id_, database_name_, routine_name_, obj_type_);
  }
  ObTenantUDFId get_udf_key() const
  {
    return ObTenantUDFId(tenant_id_, udf_name_);
  }
  ObTenantUDTId get_udt_key() const
  {
    return ObTenantUDTId(tenant_id_, udt_id_);
  }
  uint64_t get_sys_variable_key() const
  {
    return tenant_id_;
  }
  ObTenantLabelSePolicyId get_label_se_policy_key() const
  {
    return ObTenantLabelSePolicyId(tenant_id_, label_se_policy_id_);
  }
  ObTenantLabelSeComponentId get_label_se_component_key() const
  {
    return ObTenantLabelSeComponentId(tenant_id_, label_se_component_id_);
  }
  ObTenantLabelSeLabelId get_label_se_label_key() const
  {
    return ObTenantLabelSeLabelId(tenant_id_, label_se_label_id_);
  }
  ObTenantLabelSeUserLevelId get_label_se_user_level_key() const
  {
    return ObTenantLabelSeUserLevelId(tenant_id_, label_se_user_level_id_);
  }
  ObTenantTablespaceId get_tablespace_key() const
  {
    return ObTenantTablespaceId(tenant_id_, tablespace_id_);
  }
  ObTenantProfileId get_profile_key() const
  {
    return ObTenantProfileId(tenant_id_, profile_id_);
  }
  ObTenantAuditKey get_audit_key() const
  {
    return ObTenantAuditKey(tenant_id_, audit_id_);
  }
  ObSysPrivKey get_sys_priv_key() const
  {
    return ObSysPrivKey(tenant_id_, grantee_id_);
  }
  ObObjPrivSortKey get_obj_priv_key() const
  {
    return ObObjPrivSortKey(tenant_id_, 
                            table_id_, 
                            obj_type_,
                            col_id_,
                            grantor_id_,
                            grantee_id_);
  }
  ObTenantDbLinkId get_dblink_key() const
  {
    return ObTenantDbLinkId(tenant_id_, dblink_id_);
  }
  ObTenantDirectoryId get_directory_key() const
  {
    return ObTenantDirectoryId(tenant_id_, directory_id_);
  }
  ObContextKey get_context_key() const
  {
    return ObContextKey(tenant_id_, context_id_);
  }
  ObMockFKParentTableKey get_mock_fk_parent_table_key() const
  {
    return ObMockFKParentTableKey(tenant_id_, mock_fk_parent_table_id_);
  }
  ObTenantRlsPolicyId get_rls_policy_key() const
  {
    return ObTenantRlsPolicyId(tenant_id_, rls_policy_id_);
  }
  ObTenantRlsGroupId get_rls_group_key() const
  {
    return ObTenantRlsGroupId(tenant_id_, rls_group_id_);
  }
  ObTenantRlsContextId get_rls_context_key() const
  {
    return ObTenantRlsContextId(tenant_id_, rls_context_id_);
  }
  ObColumnPrivIdKey get_column_priv_key() const
  {
    return ObColumnPrivIdKey(tenant_id_, column_priv_id_);
  }
};

struct VersionHisKey
{
  VersionHisKey()
    : schema_type_(OB_MAX_SCHEMA),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      schema_id_(common::OB_INVALID_ID)
  {}
  // tenant_id should be OB_SYS_TENANT_ID when schema_type is TENANT_SCHEMA.
  VersionHisKey(const ObSchemaType schema_type,
                const uint64_t tenant_id,
                const uint64_t schema_id)
    : schema_type_(schema_type),
      tenant_id_(tenant_id),
      schema_id_(schema_id)
  {}
  inline bool operator==(const VersionHisKey &other) const
  {
    return schema_type_ == other.schema_type_
           && tenant_id_ == other.tenant_id_
           && schema_id_ == other.schema_id_;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_code = 0;
    hash_code = common::murmurhash(&schema_type_, sizeof(schema_type_), hash_code);
    hash_code = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
    hash_code = common::murmurhash(&schema_id_, sizeof(schema_id_), hash_code);
    return hash_code;
  }
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  inline bool is_valid() const
  {
    return OB_MAX_SCHEMA != schema_type_
           && common::OB_INVALID_TENANT_ID != tenant_id_
           && common::OB_INVALID_ID != schema_id_;
  }
  ObSchemaType schema_type_;
  uint64_t tenant_id_;
  int64_t schema_id_;
  TO_STRING_KV(K_(schema_type), K_(tenant_id), K_(schema_id));
};
const static int MAX_CACHED_VERSION_CNT = 16;
struct VersionHisVal
{
  VersionHisVal()
    : snapshot_version_(common::OB_INVALID_VERSION), is_deleted_(false),
      valid_cnt_(0), min_version_(common::OB_INVALID_VERSION)
  {
    MEMSET(versions_, common::OB_INVALID_VERSION, MAX_CACHED_VERSION_CNT);
  }
  void reset() {
    snapshot_version_ = common::OB_INVALID_VERSION;
    is_deleted_ = false;
    valid_cnt_ = 0;
    min_version_ = common::OB_INVALID_VERSION;
    MEMSET(versions_, common::OB_INVALID_VERSION, MAX_CACHED_VERSION_CNT);
  }
  int64_t snapshot_version_;
  bool is_deleted_;
  int64_t versions_[MAX_CACHED_VERSION_CNT];
  int valid_cnt_;
  int64_t min_version_;
  TO_STRING_KV(K(snapshot_version_), K(is_deleted_),
               "versions", common::ObArrayWrap<int64_t>(versions_, valid_cnt_),
               K(min_version_));
};

class ObMaxSchemaVersionFetcher
{
public:
  ObMaxSchemaVersionFetcher()
    : max_schema_version_(common::OB_INVALID_VERSION) {}
  virtual ~ObMaxSchemaVersionFetcher() {}

  int operator() (common::hash::HashMapPair<uint64_t, ObSchemaMgr *> &entry);
  int64_t get_max_schema_version() { return max_schema_version_; }
private:
  int64_t max_schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObMaxSchemaVersionFetcher);
};

class ObSchemaVersionGetter
{
public:
  ObSchemaVersionGetter()
    : schema_version_(common::OB_INVALID_VERSION) {}
  virtual ~ObSchemaVersionGetter() {}

  int operator() (common::hash::HashMapPair<uint64_t, ObSchemaMgr *> &entry);
  int64_t get_schema_version() { return schema_version_; }
private:
  int64_t schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaVersionGetter);
};

class ObSchemaGetterGuard;

class ObServerSchemaService
{
public:
  #define ALLOW_NEXT_LOG() share::ObTaskController::get().allow_next_syslog();
  #define SCHEMA_KEY_FUNC(SCHEMA)   \
    struct SCHEMA##_key_hash_func   \
    {                               \
      int operator()(const SchemaKey &schema_key, uint64_t &hash_val) const \
      {                             \
        hash_val = common::murmurhash(&schema_key.SCHEMA##_id_, sizeof(schema_key.SCHEMA##_id_), 0); \
        return OB_SUCCESS;          \
      }                             \
    };                              \
    struct SCHEMA##_key_equal_to    \
    {                               \
      bool operator()(const SchemaKey &a, const SchemaKey &b) const \
      {                             \
        return a.SCHEMA##_id_ == b.SCHEMA##_id_; \
      }                             \
    };
  SCHEMA_KEY_FUNC(tenant);
  SCHEMA_KEY_FUNC(user);
  SCHEMA_KEY_FUNC(tablegroup);
  SCHEMA_KEY_FUNC(database);
  SCHEMA_KEY_FUNC(table);
  SCHEMA_KEY_FUNC(outline);
  SCHEMA_KEY_FUNC(synonym);
  SCHEMA_KEY_FUNC(package);
  SCHEMA_KEY_FUNC(routine);
  SCHEMA_KEY_FUNC(trigger);
  SCHEMA_KEY_FUNC(udt);
  SCHEMA_KEY_FUNC(sequence);
  SCHEMA_KEY_FUNC(keystore);
  SCHEMA_KEY_FUNC(label_se_policy);
  SCHEMA_KEY_FUNC(label_se_component);
  SCHEMA_KEY_FUNC(label_se_label);
  SCHEMA_KEY_FUNC(label_se_user_level);
  SCHEMA_KEY_FUNC(tablespace);
  SCHEMA_KEY_FUNC(profile);
  SCHEMA_KEY_FUNC(audit);
  SCHEMA_KEY_FUNC(dblink);
  SCHEMA_KEY_FUNC(directory);
  SCHEMA_KEY_FUNC(rls_policy);
  SCHEMA_KEY_FUNC(rls_group);
  SCHEMA_KEY_FUNC(rls_context);
  #undef SCHEMA_KEY_FUNC

  struct udf_key_hash_func {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const {
      hash_code = common::murmurhash(schema_key.udf_name_.ptr(), schema_key.udf_name_.length(), 0);
      return OB_SUCCESS;
    }
  };

  struct udf_key_equal_to {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const {
      return a.udf_name_ == b.udf_name_;
    }
  };

  struct db_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.user_id_,
                                     sizeof(schema_key.user_id_),
                                     hash_code);
      hash_code = common::murmurhash(schema_key.database_name_.ptr(),
                                     schema_key.database_name_.length(),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct db_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ &&
          a.user_id_ == b.user_id_ &&
          a.database_name_ == b.database_name_;
    }
  };
  struct table_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.user_id_,
                                     sizeof(schema_key.user_id_),
                                     hash_code);
      hash_code = common::murmurhash(schema_key.database_name_.ptr(),
                                     schema_key.database_name_.length(),
                                     hash_code);
      hash_code = common::murmurhash(schema_key.table_name_.ptr(),
                                     schema_key.table_name_.length(),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct table_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ &&
          a.user_id_ == b.user_id_ &&
          a.database_name_ == b.database_name_ &&
          a.table_name_ == b.table_name_;
    }
  };

  struct routine_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.user_id_,
                                     sizeof(schema_key.user_id_),
                                     hash_code);
      hash_code = common::murmurhash(schema_key.database_name_.ptr(),
                                     schema_key.database_name_.length(),
                                     hash_code);
      hash_code = common::ObCharset::hash(cs_type, schema_key.routine_name_, hash_code);
      hash_code = common::murmurhash(&schema_key.obj_type_,
                                     sizeof(schema_key.obj_type_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };

  //In dcl resolver, ObSQLUtils::cvt_db_name_to_org will make db_name and table_name string user wrotten in the sql the same as the string in the schema.
  //So in the schema stage, db name can directly binary compare with each other without considering the collation.
  struct routine_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      ObCompareNameWithTenantID name_cmp(a.tenant_id_);
      return a.tenant_id_ == b.tenant_id_ &&
          a.user_id_ == b.user_id_ &&
          a.database_name_ == b.database_name_ &&
          0 == name_cmp.compare(a.routine_name_, b.routine_name_) &&
          a.obj_type_ == b.obj_type_;
    }
  };

  struct column_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.column_priv_id_,
                                     sizeof(schema_key.column_priv_id_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };

  struct column_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ &&
             a.column_priv_id_ == b.column_priv_id_;
    }
  };

  struct obj_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.table_id_,
                                     sizeof(schema_key.table_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.obj_type_,
                                     sizeof(schema_key.obj_type_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.col_id_,
                                     sizeof(schema_key.col_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.grantor_id_,
                                     sizeof(schema_key.grantor_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.grantee_id_,
                                     sizeof(schema_key.grantee_id_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct obj_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ 
          && a.table_id_ == b.table_id_ 
          && a.obj_type_ == b.obj_type_ 
          && a.col_id_ == b.col_id_ 
          && a.grantor_id_ == b.grantor_id_
          && a.grantee_id_ == b.grantee_id_ 
          ;
    }
  };
  struct sys_variable_key_hash_func {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const {
      hash_code = common::murmurhash(&schema_key.tenant_id_, sizeof(schema_key.tenant_id_), 0);
      return OB_SUCCESS;
    }
  };

  struct sys_variable_key_equal_to {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const {
      return a.tenant_id_ == b.tenant_id_;
    }
  };
  struct sys_priv_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.grantee_id_,
                                     sizeof(schema_key.grantee_id_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct sys_priv_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ &&
          a.grantee_id_ == b.grantee_id_ ;
    }
  };
  struct context_key_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.context_id_,
                                     sizeof(schema_key.context_id_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct context_key_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_ &&
          a.context_id_ == b.context_id_ ;
    }
  };
  struct mock_fk_parent_table_key_hash_func
  {
    int operator()(const SchemaKey &schema_key, uint64_t &hash_code) const
    {
      hash_code = 0;
      hash_code = common::murmurhash(&schema_key.tenant_id_,
                                     sizeof(schema_key.tenant_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.database_id_,
                                     sizeof(schema_key.database_id_),
                                     hash_code);
      hash_code = common::murmurhash(&schema_key.mock_fk_parent_table_id_,
                                     sizeof(schema_key.mock_fk_parent_table_id_),
                                     hash_code);
      return OB_SUCCESS;
    }
  };
  struct mock_fk_parent_table_key_equal_to
  {
    bool operator()(const SchemaKey &a, const SchemaKey &b) const
    {
      return a.tenant_id_ == b.tenant_id_
             && a.database_id_ == b.database_id_
             && a.mock_fk_parent_table_id_ == b.mock_fk_parent_table_id_;
    }
  };
  #define SCHEMA_KEYS_DEF(SCHEMA, SCHEMA_KEYS)                               \
    typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode, \
        SCHEMA##_key_hash_func, SCHEMA##_key_equal_to> SCHEMA_KEYS;
  SCHEMA_KEYS_DEF(tenant, TenantKeys);
  SCHEMA_KEYS_DEF(user, UserKeys);
  SCHEMA_KEYS_DEF(database, DatabaseKeys);
  SCHEMA_KEYS_DEF(tablegroup, TablegroupKeys);
  SCHEMA_KEYS_DEF(table, TableKeys);
  SCHEMA_KEYS_DEF(outline, OutlineKeys);
  SCHEMA_KEYS_DEF(routine, RoutineKeys);
  SCHEMA_KEYS_DEF(synonym, SynonymKeys);
  SCHEMA_KEYS_DEF(package, PackageKeys);
  SCHEMA_KEYS_DEF(trigger, TriggerKeys);
  SCHEMA_KEYS_DEF(udf, UdfKeys);
  SCHEMA_KEYS_DEF(udt, UDTKeys);
  SCHEMA_KEYS_DEF(sequence, SequenceKeys);
  SCHEMA_KEYS_DEF(sys_variable, SysVariableKeys);
  SCHEMA_KEYS_DEF(keystore, KeystoreKeys);
  SCHEMA_KEYS_DEF(label_se_policy, LabelSePolicyKeys);
  SCHEMA_KEYS_DEF(label_se_component, LabelSeComponentKeys);
  SCHEMA_KEYS_DEF(label_se_label, LabelSeLabelKeys);
  SCHEMA_KEYS_DEF(label_se_user_level, LabelSeUserLevelKeys);
  SCHEMA_KEYS_DEF(tablespace, TablespaceKeys);
  SCHEMA_KEYS_DEF(profile, ProfileKeys);
  SCHEMA_KEYS_DEF(audit, AuditKeys);
  SCHEMA_KEYS_DEF(dblink, DbLinkKeys);
  SCHEMA_KEYS_DEF(directory, DirectoryKeys);
  SCHEMA_KEYS_DEF(context, ContextKeys);
  SCHEMA_KEYS_DEF(mock_fk_parent_table, MockFKParentTableKeys);
  SCHEMA_KEYS_DEF(rls_policy, RlsPolicyKeys);
  SCHEMA_KEYS_DEF(rls_group, RlsGroupKeys);
  SCHEMA_KEYS_DEF(rls_context, RlsContextKeys);

  #undef SCHEMA_KEYS_DEF
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      db_priv_hash_func, db_priv_equal_to> DBPrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      table_priv_hash_func, table_priv_equal_to> TablePrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      routine_priv_hash_func, routine_priv_equal_to> RoutinePrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      column_priv_hash_func, column_priv_equal_to> ColumnPrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      sys_priv_hash_func, sys_priv_equal_to> SysPrivKeys;
  typedef common::hash::ObHashSet<SchemaKey, common::hash::NoPthreadDefendMode,
      obj_priv_hash_func, obj_priv_equal_to> ObjPrivKeys;

  struct AllSchemaKeys
  {
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
    // routine
    RoutineKeys new_routine_keys_;
    RoutineKeys del_routine_keys_;
    // package
    PackageKeys new_package_keys_;
    PackageKeys del_package_keys_;
    // trigger
    TriggerKeys new_trigger_keys_;
    TriggerKeys del_trigger_keys_;
    // db_priv
    DBPrivKeys new_db_priv_keys_;
    DBPrivKeys del_db_priv_keys_;
    // table_priv
    TablePrivKeys new_table_priv_keys_;
    TablePrivKeys del_table_priv_keys_;
    // routine_priv
    RoutinePrivKeys new_routine_priv_keys_;
    RoutinePrivKeys del_routine_priv_keys_;
    // column_priv
    ColumnPrivKeys new_column_priv_keys_;
    ColumnPrivKeys del_column_priv_keys_;
    // virtual table or sys view
    common::ObArray<uint64_t> non_sys_table_ids_;
    // synonym
    SynonymKeys new_synonym_keys_;
    SynonymKeys del_synonym_keys_;
    // udf
    UdfKeys new_udf_keys_;
    UdfKeys del_udf_keys_;
    // udt
    UDTKeys new_udt_keys_;
    UDTKeys del_udt_keys_;
    // sequence
    SequenceKeys new_sequence_keys_;
    SequenceKeys del_sequence_keys_;
    // sys_variable
    SysVariableKeys new_sys_variable_keys_;
    SysVariableKeys del_sys_variable_keys_;
    //drop tenant info
    TenantKeys add_drop_tenant_keys_;
    TenantKeys del_drop_tenant_keys_;
    //keystore
    KeystoreKeys new_keystore_keys_;
    KeystoreKeys del_keystore_keys_;
    //label_se_policy
    LabelSePolicyKeys new_label_se_policy_keys_;
    LabelSePolicyKeys del_label_se_policy_keys_;
    LabelSeComponentKeys new_label_se_component_keys_;
    LabelSeComponentKeys del_label_se_component_keys_;
    LabelSeLabelKeys new_label_se_label_keys_;
    LabelSeLabelKeys del_label_se_label_keys_;
    LabelSeUserLevelKeys new_label_se_user_level_keys_;
    LabelSeUserLevelKeys del_label_se_user_level_keys_;

    // tablespace
    TablespaceKeys new_tablespace_keys_;
    TablespaceKeys del_tablespace_keys_;
    //profile
    ProfileKeys new_profile_keys_;
    ProfileKeys del_profile_keys_;

    // audit
    AuditKeys new_audit_keys_;
    AuditKeys del_audit_keys_;

    // sys_priv
    SysPrivKeys new_sys_priv_keys_;
    SysPrivKeys del_sys_priv_keys_;

    // obj_priv
    ObjPrivKeys new_obj_priv_keys_;
    ObjPrivKeys del_obj_priv_keys_;

    // dblink
    DbLinkKeys new_dblink_keys_;
    DbLinkKeys del_dblink_keys_;

    // directory
    DirectoryKeys new_directory_keys_;
    DirectoryKeys del_directory_keys_;

    // context
    ContextKeys new_context_keys_;
    ContextKeys del_context_keys_;

    // mock_fk_parent_table
    MockFKParentTableKeys new_mock_fk_parent_table_keys_;
    MockFKParentTableKeys del_mock_fk_parent_table_keys_;

    // row_level_security
    RlsPolicyKeys new_rls_policy_keys_;
    RlsPolicyKeys del_rls_policy_keys_;
    RlsGroupKeys new_rls_group_keys_;
    RlsGroupKeys del_rls_group_keys_;
    RlsContextKeys new_rls_context_keys_;
    RlsContextKeys del_rls_context_keys_;

    void reset();
    int create(int64_t bucket_size);

    bool need_fetch_schemas_for_data_dict() const {
      return new_tenant_keys_.size() > 0
             || alter_tenant_keys_.size() > 0
             || new_table_keys_.size() > 0
             || new_database_keys_.size() > 0;
    }
  };

  struct AllSimpleIncrementSchema
  {
    common::ObArray<ObSimpleTenantSchema> simple_tenant_schemas_; //new tenant
    common::ObArray<ObSimpleTenantSchema> alter_tenant_schemas_;
    common::ObArray<ObSimpleDatabaseSchema> simple_database_schemas_;
    common::ObArray<ObSimpleTableSchemaV2 *> simple_table_schemas_;
    common::ObArray<ObSimpleTablegroupSchema> simple_tablegroup_schemas_;
    common::ObArray<ObSimpleOutlineSchema> simple_outline_schemas_;
    common::ObArray<ObSimpleRoutineSchema> simple_routine_schemas_;
    common::ObArray<ObSimpleSynonymSchema> simple_synonym_schemas_;
    common::ObArray<ObSimplePackageSchema> simple_package_schemas_;
    common::ObArray<ObSimpleTriggerSchema> simple_trigger_schemas_;
    common::ObArray<ObSimpleUDFSchema> simple_udf_schemas_;
    common::ObArray<ObSequenceSchema> simple_sequence_schemas_;
    common::ObArray<ObSimpleUserSchema> simple_user_schemas_;
    common::ObArray<ObDbLinkSchema> simple_dblink_schemas_;
    common::ObArray<ObDBPriv> simple_db_priv_schemas_;
    common::ObArray<ObTablePriv> simple_table_priv_schemas_;
    common::ObArray<ObRoutinePriv> simple_routine_priv_schemas_;
    common::ObArray<ObColumnPriv> simple_column_priv_schemas_;
    common::ObArray<ObSimpleUDTSchema> simple_udt_schemas_;
    common::ObArray<ObSimpleSysVariableSchema> simple_sys_variable_schemas_;
    common::ObArray<ObKeystoreSchema> simple_keystore_schemas_;
    common::ObArray<ObLabelSePolicySchema> simple_label_se_policy_schemas_;
    common::ObArray<ObLabelSeComponentSchema> simple_label_se_component_schemas_;
    common::ObArray<ObLabelSeLabelSchema> simple_label_se_label_schemas_;
    common::ObArray<ObLabelSeUserLevelSchema> simple_label_se_user_level_schemas_;
    common::ObArray<ObTablespaceSchema> simple_tablespace_schemas_;
    common::ObArray<ObProfileSchema> simple_profile_schemas_;
    common::ObArray<ObSAuditSchema> simple_audit_schemas_;
    common::ObArray<ObSysPriv> simple_sys_priv_schemas_;
    common::ObArray<ObObjPriv> simple_obj_priv_schemas_;
    common::ObArray<ObDirectorySchema> simple_directory_schemas_;
    common::ObArray<ObContextSchema> simple_context_schemas_;
    common::ObArray<ObSimpleMockFKParentTableSchema> simple_mock_fk_parent_table_schemas_;
    common::ObArray<ObRlsPolicySchema> simple_rls_policy_schemas_;
    common::ObArray<ObRlsGroupSchema> simple_rls_group_schemas_;
    common::ObArray<ObRlsContextSchema> simple_rls_context_schemas_;
    common::ObArray<ObTableSchema *> non_sys_tables_;
    common::ObArenaAllocator allocator_;
  };

public:
  int init(common::ObMySQLProxy *sql_proxy,
           common::ObDbLinkProxy *dblink_proxy,
           const common::ObCommonConfig *config);
  explicit ObServerSchemaService();
  virtual ~ObServerSchemaService();
  //get full schema instead of using patch, we call this automatically and do not expect user to
  //call this(if you really need this , use friend class, such as chunkserver)
  //construct core schema from hard code
  int fill_all_core_table_schema(const uint64_t tenant_id, ObSchemaMgr &schema_mgr_for_cache);
  virtual int get_tenant_schema_version(const uint64_t tenant_id, int64_t &schema_version);
  int64_t get_table_count() const;
  //the schema service should be thread safe
  ObSchemaService *get_schema_service(void) const;
  common::ObMySQLProxy *get_sql_proxy(void) const { return sql_proxy_; }
  void dump_schema_manager() const;

  // public utils
  virtual int get_schema_version_in_inner_table(
    common::ObISQLClient &sql_client,
    const share::schema::ObRefreshSchemaStatus &schema_status,
    int64_t &target_version);

  int query_tenant_status(const uint64_t tenant_id, TenantStatus &tenant_status);
  int construct_schema_version_history(const ObRefreshSchemaStatus &schema_status,
                                       const int64_t snapshot_version,
                                       const VersionHisKey &key,
                                       VersionHisVal &val);

  int get_refresh_schema_info(ObRefreshSchemaInfo &schema_info);

  // Fetch increments schemas in DDL trans. This interface won't return the following increment schemas:
  // 1. schema which is dropped in DDL trans.
  // 2. changed inner tables.
  int get_increment_schemas_for_data_dict(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t start_version,
      common::ObIAllocator &allocator,
      common::ObIArray<const ObTenantSchema *> &tenant_schemas,
      common::ObIArray<const ObDatabaseSchema *> &database_schemas,
      common::ObIArray<const ObTableSchema *> &table_schemas);

protected:
  bool check_inner_stat() const;
  int check_stop() const;
  virtual int fallback_schema_mgr(const ObRefreshSchemaStatus &schema_status,
                                  ObSchemaMgr &schema_mgr,
                                  const int64_t schema_version);
  //refresh schema and update schema_manager_for_cache_
  //leave the job of schema object copy to get_schema func
  int refresh_schema(const ObRefreshSchemaStatus &schema_status);

  virtual int publish_schema(const uint64_t tenant_id) = 0;
  virtual int init_multi_version_schema_struct(const uint64_t tenant_id) = 0;

  int init_schema_struct(const uint64_t tenant_id);
  int init_tenant_basic_schema(const uint64_t tenant_id);

  int destroy_schema_struct(uint64_t tenant_id);

  bool need_construct_aux_infos_(const ObTableSchema &table_schema);
  int construct_aux_infos_(
      common::ObISQLClient &sql_client,
      const share::schema::ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      ObTableSchema &table_schema);
private:
  virtual int destroy();

  // stats table instances in schem mgrs
  class ObTable
  {
    public:
    ObTable() : version_(0), combined_id_(0) {};
    virtual ~ObTable() {};
    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&version_, sizeof(uint64_t), 0);
      hash_val = common::murmurhash(&combined_id_, sizeof(uint64_t), hash_val);
      return hash_val;
    }

    bool operator== (const ObTable &tbl) const
    {
      bool equal = false;
      if (version_ == tbl.version_ &&
          combined_id_ == tbl.combined_id_) {
        equal = true;
      }
      return equal;
    }

    int64_t version_;
    int64_t combined_id_;
  };

  enum RefreshSchemaType
  {
    RST_FULL_SCHEMA_IDS = 0,
    RST_FULL_SCHEMA_ALL,
    RST_INCREMENT_SCHEMA_IDS,
    RST_INCREMENT_SCHEMA_ALL
  };

  int refresh_increment_schema(const ObRefreshSchemaStatus &schema_status);
  int refresh_full_schema(const ObRefreshSchemaStatus &schema_status);
  int refresh_tenant_full_normal_schema(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const int64_t schema_version);

#define GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(SCHEMA)               \
  int get_increment_##SCHEMA##_keys(const ObSchemaMgr &schema_guard,  \
                                    const ObSchemaOperation &schema_operation, \
                                    AllSchemaKeys &schema_ids);      \
  int get_increment_##SCHEMA##_keys_reversely(const ObSchemaMgr &schema_guard,  \
                                              const ObSchemaOperation &schema_operation, \
                                              AllSchemaKeys &schema_ids);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(tenant);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(user);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(database);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(tablegroup);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(table);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(outline);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(db_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(table_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(routine_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(column_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(routine);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(synonym);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(package);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(trigger);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(udf);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(udt);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sequence);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sys_variable);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(keystore);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(label_se_policy);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(label_se_component);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(label_se_label);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(label_se_user_level);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(tablespace);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(profile);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(audit);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(sys_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(obj_priv);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(dblink);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(directory);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(context);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(mock_fk_parent_table);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(rls_policy);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(rls_group);
  GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE(rls_context);
#undef GET_INCREMENT_SCHEMA_KEY_FUNC_DECLARE


#define APPLY_SCHEMA_TO_CACHE(SCHEMA, SCHEMA_MGR) \
  int apply_##SCHEMA##_schema_to_cache( \
      const uint64_t tenant_id, \
      const AllSchemaKeys &all_keys, \
      const AllSimpleIncrementSchema &simple_incre_schemas, \
      SCHEMA_MGR &schema_mgr);
  APPLY_SCHEMA_TO_CACHE(tenant, ObSchemaMgr);
  APPLY_SCHEMA_TO_CACHE(sys_variable, ObSysVariableMgr);
  APPLY_SCHEMA_TO_CACHE(user, ObSchemaMgr);
  APPLY_SCHEMA_TO_CACHE(database, ObSchemaMgr);
  APPLY_SCHEMA_TO_CACHE(tablegroup, ObSchemaMgr);
  APPLY_SCHEMA_TO_CACHE(table, ObSchemaMgr);
  APPLY_SCHEMA_TO_CACHE(outline, ObOutlineMgr);
  APPLY_SCHEMA_TO_CACHE(routine, ObRoutineMgr);
  APPLY_SCHEMA_TO_CACHE(package, ObPackageMgr);
  APPLY_SCHEMA_TO_CACHE(trigger, ObTriggerMgr);
  APPLY_SCHEMA_TO_CACHE(db_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(table_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(routine_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(column_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(synonym, ObSynonymMgr);
  APPLY_SCHEMA_TO_CACHE(udf, ObUDFMgr);
  APPLY_SCHEMA_TO_CACHE(udt, ObUDTMgr);
  APPLY_SCHEMA_TO_CACHE(sequence, ObSequenceMgr);
  APPLY_SCHEMA_TO_CACHE(keystore, ObKeystoreMgr);
  APPLY_SCHEMA_TO_CACHE(label_se_policy, ObLabelSePolicyMgr);
  APPLY_SCHEMA_TO_CACHE(label_se_component, ObLabelSeCompMgr);
  APPLY_SCHEMA_TO_CACHE(label_se_label, ObLabelSeLabelMgr);
  APPLY_SCHEMA_TO_CACHE(label_se_user_level, ObLabelSeUserLevelMgr);
  APPLY_SCHEMA_TO_CACHE(tablespace, ObTablespaceMgr);
  APPLY_SCHEMA_TO_CACHE(profile, ObProfileMgr);
  APPLY_SCHEMA_TO_CACHE(audit, ObSAuditMgr);
  APPLY_SCHEMA_TO_CACHE(sys_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(obj_priv, ObPrivMgr);
  APPLY_SCHEMA_TO_CACHE(dblink, ObDbLinkMgr);
  APPLY_SCHEMA_TO_CACHE(directory, ObDirectoryMgr);
  APPLY_SCHEMA_TO_CACHE(context, ObContextMgr);
  APPLY_SCHEMA_TO_CACHE(mock_fk_parent_table, ObMockFKParentTableMgr);
  APPLY_SCHEMA_TO_CACHE(rls_policy, ObRlsPolicyMgr);
  APPLY_SCHEMA_TO_CACHE(rls_group, ObRlsGroupMgr);
  APPLY_SCHEMA_TO_CACHE(rls_context, ObRlsContextMgr);
#undef APPLY_SCHEMA_TO_CACHE

  // replay log
  int replay_log(
      const ObSchemaMgr &schema_mgr,
      const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
      AllSchemaKeys &schema_keys);
  int replay_log_reversely(
      const ObSchemaMgr &schema_mgr,
      const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
      AllSchemaKeys &schema_keys);
  int update_schema_mgr(common::ObISQLClient &sql_client,
                        const ObRefreshSchemaStatus &schema_status,
                        ObSchemaMgr &schema_mgr,
                        const int64_t schema_version,
                        AllSchemaKeys &all_keys);
  int add_tenant_schemas_to_cache(const TenantKeys &tenant_keys,
                                  common::ObISQLClient &sql_client);
  int add_sys_variable_schemas_to_cache(
      const SysVariableKeys &sys_variable_keys,
      common::ObISQLClient &sql_client);
  int fetch_increment_schemas(const AllSchemaKeys &all_keys,
                              const int64_t schema_version,
                              common::ObISQLClient &sql_client,
                              AllSimpleIncrementSchema &simple_incre_schemas);
  int fetch_increment_schemas(const ObRefreshSchemaStatus &schema_status,
                              const AllSchemaKeys &all_keys,
                              const int64_t schema_version,
                              common::ObISQLClient &sql_client,
                              AllSimpleIncrementSchema &simple_incre_schemas);
  int apply_increment_schema_to_cache(const AllSchemaKeys &all_keys,
                                      const AllSimpleIncrementSchema &simple_incre_schemas,
                                      ObSchemaMgr &schema_mgr);
  int update_non_sys_schemas_in_cache_(const ObSchemaMgr &schema_mgr,
                                       common::ObIArray<ObTableSchema *> &non_sys_tables);

  int try_fetch_publish_core_schemas(const ObRefreshSchemaStatus &schema_status,
                                     const int64_t core_schema_version,
                                     const int64_t publish_version,
                                     common::ObISQLClient &sql_client,
                                     bool &core_schema_change);
  int try_fetch_publish_sys_schemas(const ObRefreshSchemaStatus &schema_status,
                                    const int64_t schema_version,
                                    const int64_t publish_version,
                                    common::ObISQLClient &sql_client,
                                    bool &sys_schema_change);

  int check_core_or_sys_schema_change(common::ObISQLClient &sql_client,
                                      const ObRefreshSchemaStatus &schema_status,
                                      const int64_t core_schema_version,
                                      const int64_t schema_version,
                                      bool &core_schema_change,
                                      bool &sys_schema_change);
  int check_core_schema_change_(
      ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const int64_t core_schema_version,
      bool &core_schema_change);

  virtual int check_sys_schema_change(common::ObISQLClient &sql_client,
                                      const ObRefreshSchemaStatus &schema_status,
                                      const int64_t schema_version,
                                      const int64_t new_schema_version,
                                      bool &sys_schema_change);
  int get_sys_table_ids(
      const uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const;
  int get_table_ids(const uint64_t tenant_id,
                    const schema_create_func *schema_creators,
                    common::ObIArray<uint64_t> &table_ids) const;
  int add_sys_table_index_ids(const uint64_t tenant_id,
                              common::ObIArray<uint64_t> &table_ids) const;
  int add_sys_table_lob_aux_ids(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &table_ids) const;
  int add_index_tids(const ObSchemaMgr &schema_mgr,
                     ObTableSchema &table);
  int extract_non_sys_table_ids_(const TableKeys &keys, common::ObIArray<uint64_t> &non_sys_table_ids);
protected:
  virtual int update_schema_cache(common::ObIArray<ObTableSchema*> &schema_array) = 0;
  virtual int update_schema_cache(common::ObIArray<ObTableSchema> &schema_array) = 0;
  virtual int update_schema_cache(const common::ObIArray<ObTenantSchema> &schema_array) = 0;
  virtual int update_schema_cache(const ObSysVariableSchema &schema) = 0;
  virtual int add_aux_schema_from_mgr(const ObSchemaMgr &mgr,
                                      ObTableSchema &table_schema,
                                      const ObTableType table_type) = 0;
  int add_tenant_schema_to_cache(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const int64_t schema_version);
  int add_sys_variable_schema_to_cache(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const int64_t schema_version);
  int convert_to_simple_schema(
      common::ObIAllocator &allocator,
      const common::ObIArray<ObTableSchema> &schemas,
      common::ObIArray<ObSimpleTableSchemaV2 *> &simple_schemas);
  int convert_to_simple_schema(
      common::ObIAllocator &allocator,
      const common::ObIArray<ObTableSchema *> &schemas,
      common::ObIArray<ObSimpleTableSchemaV2 *> &simple_schemas);
  int convert_to_simple_schema(
      const ObTableSchema &schema,
      ObSimpleTableSchemaV2 &simple_schema);

  template<typename SchemaKeys>
  int del_operation(const uint64_t tenant_id, SchemaKeys &keys);

  template<typename SchemaKeys>
  int convert_schema_keys_to_array(const SchemaKeys &key_set,
                                   common::ObIArray<SchemaKey> &key_array);

  int del_tenant_operation(const uint64_t tenant_id,
                           AllSchemaKeys &schema_keys,
                           const bool new_flag);

  /*-- data dict related --*/
  int get_increment_schema_keys_for_data_dict_(
      const ObSchemaService::SchemaOperationSetWithAlloc &schema_operations,
      AllSchemaKeys &schema_keys);
  int fetch_increment_schemas_for_data_dict_(
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const AllSchemaKeys &schema_keys,
      common::ObIArray<const ObTenantSchema *> &tenant_schemas,
      common::ObIArray<const ObDatabaseSchema *> &database_schemas,
      common::ObIArray<const ObTableSchema *> &table_schemas);
  int fetch_increment_tenant_schemas_for_data_dict_(
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const AllSchemaKeys &schema_keys,
      common::ObIArray<const ObTenantSchema *> &tenant_schemas);
  int fetch_increment_database_schemas_for_data_dict_(
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const AllSchemaKeys &schema_keys,
      common::ObIArray<const ObDatabaseSchema *> &database_schemas);
  int fetch_increment_table_schemas_for_data_dict_(
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const AllSchemaKeys &schema_keys,
      common::ObIArray<const ObTableSchema *> &table_schemas);
protected:
  // core table count
  const static int64_t MIN_TABLE_COUNT = 1;
  static const int64_t REFRESH_SCHEMA_INTERVAL_US = 100 * 1000; // 100ms
  static const int64_t MIN_SWITCH_ALLOC_CNT = 64;
  static const int64_t DEFAULT_FETCH_SCHEMA_TIMEOUT_US = 2 * 1000 * 1000; // 2s
  static const int64_t MAX_FETCH_SCHEMA_TIMEOUT_US = 60 * 1000 * 1000; // 60s
  common::SpinRWLock schema_manager_rwlock_;
  mutable lib::ObMutex mem_mgr_for_liboblog_mutex_;
  ObSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  const common::ObCommonConfig *config_;
  const static int VERSION_HIS_MAP_BUCKET_NUM = 64 * 1024;
  common::hash::ObHashMap<VersionHisKey, VersionHisVal, common::hash::ReadWriteDefendMode> version_his_map_;

private:
  static const int64_t BUCKET_SIZE = 128;
  // schema_version after bootstrap succeed, it's the min schema_version can be fallbacked

protected:
  // new schema management by tenant, need protected by lock
  const static int TENANT_MAP_BUCKET_NUM = 1024;
  common::hash::ObHashMap<uint64_t, bool, common::hash::ReadWriteDefendMode> refresh_full_schema_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMgr*, common::hash::ReadWriteDefendMode> schema_mgr_for_cache_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMemMgr*, common::hash::ReadWriteDefendMode> mem_mgr_map_;
  common::hash::ObHashMap<uint64_t, ObSchemaMemMgr*, common::hash::ReadWriteDefendMode> mem_mgr_for_liboblog_map_;
};

template<typename SchemaKeys>
int ObServerSchemaService::del_operation(const uint64_t tenant_id, SchemaKeys &keys)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_INVALID_ID == tenant_id) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  }
  common::ObArray<SchemaKey> to_remove;
  for (typename SchemaKeys::const_iterator it = keys.begin();
       OB_SUCC(ret) && it != keys.end(); ++it) {
    if ((it->first).tenant_id_ == tenant_id) {
      if (OB_FAIL(to_remove.push_back(it->first))) {
        SHARE_SCHEMA_LOG(WARN, "push_back failed", KR(ret));
      }
    }
  }
  FOREACH_CNT_X(v, to_remove, OB_SUCC(ret)) {
    int64_t hash_ret = keys.erase_refactored(*v);
    if (common::OB_SUCCESS != hash_ret && common::OB_HASH_NOT_EXIST != hash_ret) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "erase failed", "value", *v, KR(ret));
    }
  }
  return ret;
}

template<typename SchemaKeys>
int ObServerSchemaService::convert_schema_keys_to_array(
    const SchemaKeys &key_set,
    common::ObIArray<SchemaKey> &key_array)
{
  int ret = common::OB_SUCCESS;
  key_array.reset();
  for (typename SchemaKeys::const_iterator it = key_set.begin();
       OB_SUCC(ret) && it != key_set.end(); it++) {
    const SchemaKey &key = it->first;
    if (OB_FAIL(key_array.push_back(key))) {
      SHARE_SCHEMA_LOG(WARN, "fail to push back schema key", KR(ret), K(key));
    }
  }
  return ret;
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OCEANBASE_SERVER_SCHEMA_SERVICE_H_

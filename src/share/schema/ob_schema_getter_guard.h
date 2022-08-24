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

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/part/ob_part_mgr.h"
#include "share/schema/ob_schema_mgr_cache.h"

namespace oceanbase {
namespace common {
class ObString;
class ObKVCacheHandle;
template <class T>
class ObIArray;
}  // namespace common
namespace sql {
class ObSQLSessionInfo;
}
namespace share {
class ObWorker;
namespace schema {
class ObTenantSchema;
class ObUserInfo;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
class ObSessionPrivInfo;
class ObUserLoginInfo;
class ObStmtNeedPrivs;
class ObNeedPriv;
class ObDBPriv;
class ObTablePriv;
class IdVersion;
class SchemaName;
class ObSimpleTenantSchema;
class ObSimpleTablegroupSchema;
class ObSimpleDatabaseSchema;
class ObSimpleSysVariableSchema;
class ObSimpleSynonymSchema;
class ObUDF;
class ObMockSchemaInfo;
class ObSchemaGetterGuardHelper {
public:
  ObSchemaGetterGuardHelper(common::ObIAllocator& alloc)
      : is_inited_(false), alloc_(alloc), full_schemas_(), simple_schemas_()
  {}
  virtual ~ObSchemaGetterGuardHelper()
  {}

  int init();
  void reset();
  int get_table_schema(ObSchemaGetterGuard& guard, const ObTableSchema* orig_table, const ObTableSchema*& table_schema);
  int get_table_schema(
      ObSchemaGetterGuard& guard, const ObSimpleTableSchemaV2* orig_table, const ObSimpleTableSchemaV2*& table_schema);
  int get_tablegroup_schema(ObSchemaGetterGuard& guard, const ObTablegroupSchema* orig_tablegroup,
      const ObTablegroupSchema*& tablegroup_schema);
  int get_tablegroup_schema(ObSchemaGetterGuard& guard, const ObSimpleTablegroupSchema* orig_tablegroup,
      const ObSimpleTablegroupSchema*& tablegroup_schema);

private:
  const static int64_t DEFAULT_SCHEMA_NUM = 100;
  int64_t is_inited_;
  common::ObIAllocator& alloc_;
  common::hash::ObHashMap<uint64_t, const ObSchema*, common::hash::NoPthreadDefendMode> full_schemas_;
  common::hash::ObHashMap<uint64_t, const ObSchema*, common::hash::NoPthreadDefendMode> simple_schemas_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaGetterGuardHelper);
};

class ObSchemaMgrInfo {
public:
  ObSchemaMgrInfo()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        snapshot_version_(common::OB_INVALID_VERSION),
        schema_mgr_(NULL),
        mgr_handle_(),
        schema_status_()
  {}
  ObSchemaMgrInfo(const uint64_t tenant_id, const int64_t snapshot_version, const ObSchemaMgr*& schema_mgr,
      const ObSchemaMgrHandle& mgr_handle, const ObRefreshSchemaStatus& schema_status)
      : tenant_id_(tenant_id), snapshot_version_(snapshot_version), schema_mgr_(schema_mgr), mgr_handle_(mgr_handle)
  {
    schema_status_ = schema_status;
  }
  ObSchemaMgrInfo& operator=(const ObSchemaMgrInfo& other);
  explicit ObSchemaMgrInfo(const ObSchemaMgrInfo& other);
  virtual ~ObSchemaMgrInfo();
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  void set_schema_mgr(const ObSchemaMgr* schema_mgr)
  {
    schema_mgr_ = schema_mgr;
  }
  const ObSchemaMgr* get_schema_mgr() const
  {
    return schema_mgr_;
  }
  ObRefreshSchemaStatus get_schema_status() const
  {
    return schema_status_;
  }
  ObSchemaMgrHandle& get_schema_mgr_handle()
  {
    return mgr_handle_;
  }
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(snapshot_version), KP_(schema_mgr), K_(schema_status));

private:
  uint64_t tenant_id_;
  int64_t snapshot_version_;
  const ObSchemaMgr* schema_mgr_;
  ObSchemaMgrHandle mgr_handle_;
  ObRefreshSchemaStatus schema_status_;
};

class ObSchemaGetterGuard : public common::ObPartMgr {
  friend class ObMultiVersionSchemaService;
  friend class MockSchemaService;
  struct SchemaObj {
    SchemaObj(const uint64_t schema_id = common::OB_INVALID_ID)
        : schema_type_(OB_MAX_SCHEMA), schema_id_(schema_id), schema_(NULL)
    {}
    ObSchemaType schema_type_;
    uint64_t schema_id_;
    ObSchema* schema_;
    common::ObKVCacheHandle handle_;
    TO_STRING_KV(K_(schema_type), K_(schema_id), KP_(schema));
  };
  const static int DEFAULT_RESERVE_SIZE = 2;
  const static int DEFAULT_TENANT_NUM = 2;
  typedef common::ObSEArray<SchemaObj, DEFAULT_RESERVE_SIZE> SchemaObjs;
  typedef common::ObSEArray<ObSchemaMgrInfo, DEFAULT_TENANT_NUM> SchemaMgrInfos;

public:
  enum CheckTableType {
    ALL_TYPES = 0,
    TEMP_TABLE_TYPE = 1,
    NON_TEMP_TABLE_TYPE = 2,
  };

  enum SchemaGuardType {
    INVALID_SCHEMA_GUARD_TYPE = 0,
    SCHEMA_GUARD = 1,
    TENANT_SCHEMA_GUARD = 2,
    TABLE_SCHEMA_GUARD = 3
  };

  ObSchemaGetterGuard();
  virtual ~ObSchemaGetterGuard();
  int reset();
  void dump();
  OB_INLINE bool is_inited() const
  {
    return is_inited_;
  }

  int get_schema_version(const uint64_t tenant_id, int64_t& schema_version) const;

  /*
   * with_mv: if index_tid_array contains ematerialized view.
   * with_global_index: if index_tid_array contains global index.
   * with_domain_index: if index_tid_array contains domain index.
   */
  int get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool with_mv,
      bool with_global_index = true, bool with_domain_index = true);
  int check_has_local_unique_index(uint64_t table_id, bool& has_local_unique_index);

  bool is_tenant_schema_valid(const int64_t tenant_id) const;
  /*
       interface for simple schema
       */
  int get_table_schema(const uint64_t table_id, const ObSimpleTableSchemaV2*& table_schema);
  int get_table_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_simple_table_schema(uint64_t tenant_id, uint64_t database_id, const common::ObString& table_name,
      const bool is_index, const ObSimpleTableSchemaV2*& simple_table_schema);
  int get_table_schemas(common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas(common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_tablegroup_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObTablegroupSchema*>& tablegroup_schemas);

  // TODO: impl them
  int get_user_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObUserInfo*>& user_schemas);
  int get_database_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObDatabaseSchema*>& database_schemas);
  int get_tablegroup_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTablegroupSchema*>& tablegroup_schemas);
  int get_table_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_outline_infos_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObOutlineInfo*>& outline_infos);
  int get_inner_table_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_user_table_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);

  int get_inner_table_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_user_table_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);

  int get_synonym_infos_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObSynonymInfo*>& synonym_info);
  int get_table_schemas_in_database(
      const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_database(const uint64_t tenant_id, const uint64_t database_id,
      common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas_in_tablegroup(
      const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id, const uint64_t tablegroup_id,
      common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const;
  int get_available_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const;
  int get_tablegroup_ids_in_tenant(const uint64_t tenant_id, common::ObIArray<uint64_t>& tablegroup_id_array);
  int get_table_ids_in_tenant(const uint64_t tenant_id, common::ObIArray<uint64_t>& table_ids);
  int get_table_ids_in_database(
      const uint64_t tenant_id, const uint64_t dataspace_id, common::ObIArray<uint64_t>& table_id_array);
  int get_table_ids_in_tablegroup(
      const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObIArray<uint64_t>& table_id_array);
  int batch_get_next_table(
      const ObTenantTableId& tenant_table_id, const int64_t get_size, common::ObIArray<ObTenantTableId>& table_array);
  int get_outline_infos_in_database(
      const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObOutlineInfo*>& outline_infos);
  int get_synonym_infos_in_database(
      const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObSynonymInfo*>& synonym_infos);

  int get_sequence_infos_in_database(
      const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObSequenceSchema*>& sequence_infos);
  int get_profile_infos_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObProfileSchema*>& profile_infos);
  /*
     get_id
  */
  int get_tenant_id(const common::ObString& tenant_name, uint64_t& tenant_id);
  int get_user_id(uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      uint64_t& user_id, const bool is_role = false);
  int get_database_id(uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id);
  int get_tablegroup_id(uint64_t tenant_id, const common::ObString& tablegroup_name, uint64_t& tablegroup_id);
  int get_table_id(uint64_t tenant_id, uint64_t database_id, const common::ObString& table_name, const bool is_index,
      const CheckTableType check_type,  // if temporary table is visable
      uint64_t& table_id);
  int get_table_id(uint64_t tenant_id, const common::ObString& database_name, const common::ObString& table_name,
      const bool is_index,
      const CheckTableType check_type,  // if temporary table is visable
      uint64_t& table_id);
  int get_foreign_key_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& foreign_key_name,
      uint64_t& foreign_key_id);
  int get_foreign_key_info(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& foreign_key_name, ObSimpleForeignKeyInfo& foreign_key_info);
  int get_constraint_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& constraint_name,
      uint64_t& constraint_id);
  int get_constraint_info(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& constraint_name,
      ObSimpleConstraintInfo& constraint_info) const;
  int get_tenant_name_case_mode(const uint64_t tenant_id, common::ObNameCaseMode& mode);
  int get_tenant_compat_mode(const uint64_t tenant_id, share::ObWorker::CompatMode& compat_mode);
  int get_tenant_read_only(const uint64_t tenant_id, bool& read_only);
  int get_tenant_meta_reserved_memory_percentage(
      const uint64_t tenant_id, common::ObIAllocator& allocator, int64_t& percentage);
  int get_mock_schema_info(
      const uint64_t schema_id, const ObSchemaType schema_type, ObMockSchemaInfo& mock_schema_info);
  /*
     get_schema
  */
  // basic interface
  int get_tenant_info(uint64_t tenant_id, const ObTenantSchema*& tenant_info);
  int get_tenant_info(uint64_t tenant_id, const ObSimpleTenantSchema*& tenant_info);
  int get_user_info(uint64_t user_id, const ObUserInfo*& user_info);
  int get_database_schema(uint64_t database_id, const ObDatabaseSchema*& database_schema);
  int get_database_schema(uint64_t database_id, const ObSimpleDatabaseSchema*& database_schema);
  int get_tablegroup_schema(uint64_t tablegroup_id, const ObTablegroupSchema*& tablegourp_schema);
  int get_tablegroup_schema(uint64_t tablegroup_id, const ObSimpleTablegroupSchema*& tablegroup_schema);
  int get_table_schema(uint64_t table_id, const ObTableSchema*& table_schema);
  int get_index_status(
      const uint64_t data_table_id, const bool with_global_index, common::ObIArray<ObIndexTableStat>& index_status);
  int get_all_aux_table_status(
      const uint64_t data_table_id, const bool with_global_index, common::ObIArray<ObIndexTableStat>& index_status);
  int get_table_schema(uint64_t tenant_id, uint64_t database_id, const common::ObString& table_name,
      const bool is_index, const ObTableSchema*& table_schema);
  int get_table_schema(uint64_t tenant_id, const common::ObString& database_name, const common::ObString& table_name,
      const bool is_index, const ObTableSchema*& table_schema);

  int get_sys_variable_schema(const common::ObString& tenant_name, const ObSysVariableSchema*& sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id, const ObSysVariableSchema*& sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id, const ObSimpleSysVariableSchema*& sys_variable_schema);
  int get_tenant_system_variable(
      uint64_t tenant_id, const common::ObString& var_name, const ObSysVarSchema*& var_schema);
  int get_tenant_system_variable(uint64_t tenant_id, ObSysVarClassType var_id, const ObSysVarSchema*& var_schema);
  int get_timestamp_service_type(const uint64_t tenant_id, int64_t& ts_type);
  int get_tenant_info(const common::ObString& tenant_name, const ObTenantSchema*& tenant_schema);
  int get_user_info(uint64_t tenant_id, uint64_t user_id, const ObUserInfo*& user_info);
  int get_user_info(uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      const ObUserInfo*& user_info);
  int get_user_info(
      uint64_t tenant_id, const common::ObString& user_name, common::ObIArray<const ObUserInfo*>& users_info);
  int get_database_schema(uint64_t tenant_id, uint64_t database_id, const ObDatabaseSchema*& database_schema);
  int get_database_schema(
      uint64_t tenant_id, const common::ObString& database_name, const ObDatabaseSchema*& database_schema);
  int get_column_schema(uint64_t table_id, uint64_t column_id, const ObColumnSchemaV2*& column_schema);
  int get_column_schema(uint64_t table_id, const common::ObString& column_name, const ObColumnSchemaV2*& column_schema);

  // for resolver
  int get_can_write_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool only_global = false);

  int column_is_key(uint64_t table_id, uint64_t column_id, bool& is_key);

  // for readonly
  int verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs& stmt_need_privs);
  // TODO: move to private
  int verify_db_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv);
  int verify_table_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv);
  // for privilege
  int add_role_id_recursively(uint64_t role_id, ObSessionPrivInfo& s_priv);
  int check_user_access(
      const ObUserLoginInfo& login_info, ObSessionPrivInfo& s_priv, SSL* ssl_st, const ObUserInfo*& sel_user_info);
  int check_ssl_access(const ObUserInfo& user_info, SSL* ssl_st);
  int check_ssl_invited_cn(const uint64_t tenant_id, SSL* ssl_st);
  int check_db_access(ObSessionPrivInfo& s_priv, const common::ObString& database_name);
  int check_db_show(const ObSessionPrivInfo& session_priv, const common::ObString& db, bool& allow_show);
  int check_table_show(const ObSessionPrivInfo& session_priv, const common::ObString& db, const common::ObString& table,
      bool& allow_show);

  int check_ora_priv(const uint64_t tenant_id, const uint64_t uid, const ObStmtOraNeedPrivs& stmt_need_privs,
      const common::ObIArray<uint64_t>& role_id_array);
  int check_priv(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs);
  int check_priv_or(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs);
  int check_db_priv(const ObSessionPrivInfo& session_priv, const common::ObString& db, const ObPrivSet need_priv,
      ObPrivSet& user_db_priv_set);
  int check_db_priv(const ObSessionPrivInfo& session_priv, const common::ObString& db, const ObPrivSet need_priv);
  int check_user_priv(const ObSessionPrivInfo& session_priv, const ObPrivSet priv_set);
  int check_db_access(const ObSessionPrivInfo& session_priv, const common::ObString& db, ObPrivSet& db_priv_set,
      bool print_warn = true);
  int check_single_obj_priv(const uint64_t tenant_id, const uint64_t uid, const ObOraNeedPriv& need_priv,
      const common::ObIArray<uint64_t>& role_id_array);
  int check_single_table_priv(const ObSessionPrivInfo& session_priv, const ObNeedPriv& table_need_priv);
  int get_user_infos_with_tenant_id(const uint64_t tenant_id, common::ObIArray<const ObUserInfo*>& user_infos);
  int get_db_priv_with_tenant_id(const uint64_t tenant_id, common::ObIArray<const ObDBPriv*>& db_privs);
  int get_db_priv_with_user_id(
      const uint64_t tenant_id, const uint64_t user_id, common::ObIArray<const ObDBPriv*>& db_privs);
  int get_table_priv_with_tenant_id(const uint64_t tenant_id, common::ObIArray<const ObTablePriv*>& table_privs);
  int get_table_priv_with_user_id(
      const uint64_t tenant_id, const uint64_t user_id, common::ObIArray<const ObTablePriv*>& table_privs);
  int get_obj_priv_with_grantee_id(
      const uint64_t tenant_id, const uint64_t grnatee_id, common::ObIArray<const ObObjPriv*>& obj_privs);
  int get_obj_priv_with_grantor_id(const uint64_t tenant_id, const uint64_t grantor_id,
      common::ObIArray<const ObObjPriv*>& obj_privs, bool reset_flag);
  int get_obj_priv_with_obj_id(const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_type,
      common::ObIArray<const ObObjPriv*>& obj_privs, bool reset_flag);
  int get_obj_privs_in_ur_and_obj(
      const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, ObPackedObjPriv& obj_privs);
  int get_obj_privs_in_grantor_ur_obj_id(
      const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, common::ObIArray<const ObObjPriv*>& obj_privs);
  int get_obj_privs_in_grantor_obj_id(
      const uint64_t tenant_id, const ObObjPrivSortKey& obj_key, common::ObIArray<const ObObjPriv*>& obj_privs);
  int get_db_priv_set(
      const uint64_t tenant_id, const uint64_t user_id, const common::ObString& db, ObPrivSet& priv_set);
  // for compatible
  int get_db_priv_set(const ObOriginalDBKey& db_priv_key, ObPrivSet& priv_set, bool is_pattern = false);
  int get_table_priv_set(const ObTablePrivSortKey& table_priv_key, ObPrivSet& priv_set);
  int get_obj_privs(const ObObjPrivSortKey& obj_priv_key, ObPackedObjPriv& obj_privs);
  // TODO@: ObDDLOperator::drop_tablegroup
  int check_database_exists_in_tablegroup(const uint64_t tenant_id, const uint64_t tablegroup_id, bool& not_empty);

  // : just return pointer to save my life.
  const ObUserInfo* get_user_info(const uint64_t user_id);
  // const ObDatabaseSchema *get_database_schema(const uint64_t database_id);
  const ObTablegroupSchema* get_tablegroup_schema(const uint64_t tablegroup_id);
  // const ObTableSchema *get_table_schema(const uint64_t table_id);
  const ObColumnSchemaV2* get_column_schema(const uint64_t table_id, const uint64_t column_id);
  const ObTenantSchema* get_tenant_info(const common::ObString& tenant_name);

  // check exist, for root_service/ddl_service/ddl_operator
  int check_database_exist(
      const uint64_t tenant_id, const common::ObString& database_name, bool& is_exist, uint64_t* database_id = NULL);
  int check_database_in_recyclebin(const uint64_t database_id, bool& in_recyclebin);
  int check_database_exist(const uint64_t database_id, bool& is_exist);
  int check_tablegroup_exist(const uint64_t tenant_id, const common::ObString& tablegroup_name, bool& is_exist,
      uint64_t* tablegroup_id = NULL);
  int check_tablegroup_exist(const uint64_t tablegroup_id, bool& is_exist);
  int check_oracle_object_exist(const uint64_t tenant_id, const uint64_t db_id, const common::ObString& object_name,
      const ObSchemaType& schema_type, const bool is_or_replace, common::ObIArray<ObSchemaType>& conflict_schema_types);
  int check_table_exist(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index,
      const CheckTableType check_type,  // if temporary table is visable
      bool& is_exist, uint64_t* table_id = NULL);
  int check_table_exist(const uint64_t table_id, bool& is_exist);
  int check_partition_can_remove(
      const uint64_t table_id, const int64_t phy_partition_id, const bool check_dropped_partition, bool& can);
  int check_partition_exist(
      const uint64_t table_id, const int64_t phy_partition_id, const bool check_dropped_partition, bool& is_exist);
  int check_tenant_exist(const uint64_t tenant_id, bool& is_exist);
  int check_outline_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& outline_name, uint64_t& outline_id, bool& exist);
  int check_outline_exist_with_sql(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& paramlized_sql, bool& exist);
  int check_outline_exist_with_sql_id(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& sql_id, bool& exist);
  int get_outline_info_with_name(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& name,
      const ObOutlineInfo*& outline_info);
  int get_outline_info_with_name(const uint64_t tenant_id, const common::ObString& db_name,
      const common::ObString& outline_name, const ObOutlineInfo*& outline_info);
  int get_outline_info_with_signature(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& signature, const ObOutlineInfo*& outline_info);

  int get_synonym_info_version(uint64_t synonym_id, int64_t& synonym_version);
  int get_synonym_info(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& name,
      const ObSynonymInfo*& synonym_info);
  int get_synonym_info(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& name,
      const ObSimpleSynonymSchema*& synonym_info);
  int get_simple_synonym_info(
      const uint64_t tenant_id, const uint64_t synonym_id, const ObSimpleSynonymSchema*& synonym_info);
  int check_synonym_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& synonym_name, bool& exist, uint64_t& synonym_id);
  int get_object_with_synonym(const uint64_t tenant_id, const uint64_t syn_database_id,
      const common::ObString& syn_name, uint64_t& obj_database_id, uint64_t& synonym_id,
      common::ObString& obj_table_name, bool& do_exist) const;
  int get_outline_info_with_sql_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& sql_id,
      const ObOutlineInfo*& outline_info);
  // about user define function
  int check_udf_exist_with_name(const uint64_t tenant_id, const common::ObString& name, bool& exist);
  int get_udf_info(
      const uint64_t tenant_id, const common::ObString& name, const share::schema::ObUDF*& udf_info, bool& exist);

  int check_sequence_exist_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& sequence_name, bool& exist, uint64_t& sequence_id);
  int get_sequence_schema(const uint64_t tenant_id, const uint64_t sequence_id, const ObSequenceSchema*& schema);
  int get_sequence_schema_with_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& sequence_name, const ObSequenceSchema*& sequence_schema);
  // end user define function

  int get_profile_schema_by_name(
      const uint64_t tenant_id, const common::ObString& name, const ObProfileSchema*& schema);

  int get_profile_schema_by_id(const uint64_t tenant_id, const uint64_t profile_id, const ObProfileSchema*& schema);

  int get_user_profile_failed_login_limits(
      const uint64_t user_id, int64_t& failed_login_limit_num, int64_t& failed_login_limit_time);
  int get_user_password_expire_times(
      const uint64_t user_id, int64_t& password_last_change, int64_t& password_life_time, int64_t& password_grace_time);
  // dblink function begin
  int check_dblink_exist(const uint64_t tenant_id, const common::ObString& dblink_name, bool& exist) const;
  int get_dblink_id(const uint64_t tenant_id, const common::ObString& dblink_name, uint64_t& dblink_id) const;
  int get_dblink_user(const uint64_t tenant_id, const common::ObString& dblink_name, common::ObString& dblink_user,
      common::ObIAllocator& allocator);
  int get_dblink_schema(const uint64_t tenant_id, const common::ObString& dblink_name,
      const share::schema::ObDbLinkSchema*& dblink_schema) const;
  int get_dblink_schema(const uint64_t dblink_id, const share::schema::ObDbLinkSchema*& dblink_schema);
  int get_link_table_schema(uint64_t dblink_id, const common::ObString& database_name,
      const common::ObString& table_name, common::ObIAllocator& allocator, ObTableSchema*& table_schema);
  // dblink function end

  int check_user_exist(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      bool& is_exist, uint64_t* user_id = NULL);
  int check_user_exist(const uint64_t tenant_id, const uint64_t user_id, bool& is_exist);

  int query_table_status(const uint64_t table_id, TableStatus& table_status);

  // For liboblog only
  int query_partition_status(const common::ObPartitionKey& pkey, PartitionStatus& part_status);

  int get_table_schema_version(const uint64_t table_id, int64_t& table_version);

  template <typename SchemaType>
  int check_flashback_object_exist(
      const SchemaType& object_schema, const common::ObString& object_name, bool& object_exist);

  virtual int get_part(const uint64_t table_id, const share::schema::ObPartitionLevel part_level, const int64_t part_id,
      const common::ObNewRange& range, const bool reverse, common::ObIArray<int64_t>& part_ids) override;
  virtual int get_part(const uint64_t table_id, const share::schema::ObPartitionLevel part_level, const int64_t part_id,
      const common::ObNewRow& row, common::ObIArray<int64_t>& part_ids) override;
  int get_schema_count(int64_t& schema_count);
  int get_schema_count(const uint64_t tenant_id, int64_t& schema_count);
  int get_schema_size(const uint64_t tenant_id, int64_t& schema_count);
  int get_schema_version_v2(const ObSchemaType schema_type, const uint64_t schema_id, int64_t& schema_version);
  int get_idx_schema_by_origin_idx_name(
      uint64_t tenant_id, uint64_t database_id, const common::ObString& index_name, const ObTableSchema*& table_schema);

  int get_tenant_unavailable_index(const uint64_t tenant_id, common::ObIArray<uint64_t>& table_ids);
  int check_unavailable_index_exist(const uint64_t tenant_id, bool& exist);
  int check_restore_error_index_exist(const uint64_t tenant_id, bool& exist);

  inline uint64_t get_session_id() const
  {
    return session_id_;
  }
  inline void set_session_id(const uint64_t id)
  {
    session_id_ = id;
  }

  int get_partition_cnt(uint64_t table_id, int64_t& part_cnt);

  bool is_tenant_schema_guard() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }

  int get_tenant_mv_ids(const uint64_t tenant_id, common::ObArray<uint64_t>& mv_ids) const;
  int get_all_mv_ids(common::ObArray<uint64_t>& mv_ids) const;

  SchemaGuardType get_schema_guard_type() const
  {
    return schema_guard_type_;
  }

  bool is_standby_cluster()
  {
    return is_standby_cluster_;
  }
  bool is_schema_splited() const
  {
    return 0 != schema_mgr_infos_.count();
  }
  int check_formal_guard() const;
  int is_lazy_mode(const uint64_t tenant_id, bool& is_lazy) const;

  int check_tenant_is_restore(const uint64_t tenant_id, bool& is_restore);
  int check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool& is_dropped);

  int get_sys_priv_with_tenant_id(const uint64_t tenant_id, common::ObIArray<const ObSysPriv*>& sys_privs);
  int get_sys_priv_with_grantee_id(const uint64_t tenant_id, const uint64_t grantee_id, ObSysPriv*& sys_priv);
  int check_global_index_exist(const uint64_t tenant_id, const uint64_t table_id, bool &exist);

public:
  // for optimize
  // ------- local cache : id2schema  ---------
  struct IdSchemaWrapper {
    ObSchemaType schema_type_;
    uint64_t schema_id_;
    const ObSchema* schema_;
    TO_STRING_KV(K_(schema_type), K_(schema_id), KP_(schema));
  };

  template <typename T>
  int get_from_local_cache(const ObSchemaType schema_type, const uint64_t schema_id, const T*& schema);

private:
  int get_outline_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObOutlineInfo*>& outline_schemas);
  int get_synonym_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObSynonymInfo*>& synonym_schemas);

  // TODO: add this to all member functions
  bool check_inner_stat() const;
  template <typename T>
  int get_schema(
      const ObSchemaType schema_type, const uint64_t schema_id, const int64_t schema_version, const T*& schema);
  template <typename T>
  int get_schema(const SchemaName& schema_name, const T*& schema);
  template <typename T>
  int get_schema(const ObSchemaType schema_type, const uint64_t schema_id, const T*& schema);
  template <typename T>
  int get_schema_v2(const ObSchemaType schema_type, const uint64_t schema_id, const T*& schema,
      int64_t specified_version = common::OB_INVALID_VERSION);

  int get_simple_table_schema_inner(uint64_t tenant_id, uint64_t database_id, const common::ObString& table_name,
      const bool is_index, const ObSimpleTableSchemaV2*& simple_table_schema);

  int get_table_schema_inner(const uint64_t table_id, const ObSimpleTableSchemaV2*& table_schema);
  int get_inner_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_user_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas_inner(common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas_inner(common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_inner_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_user_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_tenant_inner(
      const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_database_inner(
      const uint64_t tenant_id, const uint64_t database_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_database_inner(const uint64_t tenant_id, const uint64_t database_id,
      common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schemas_in_tablegroup_inner(
      const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObIArray<const ObTableSchema*>& table_schemas);
  int get_table_schemas_in_tablegroup_inner(const uint64_t tenant_id, const uint64_t tablegroup_id,
      common::ObIArray<const ObSimpleTableSchemaV2*>& table_schemas);
  int get_table_schema_inner(uint64_t table_id, const ObTableSchema*& table_schema);
  int get_table_schema_inner(uint64_t tenant_id, uint64_t database_id, const common::ObString& table_name,
      const bool is_index, const ObTableSchema*& table_schema);
  int get_table_schema_inner(uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, const bool is_index, const ObTableSchema*& table_schema);
  int get_index_status_inner(const ObTableSchema& data_table_schema, const bool with_global_index,
      common::ObIArray<ObIndexTableStat>& index_status);

private:
  int add_handle(const common::ObKVCacheHandle& handle);
  int init(const bool is_standby_cluster);
  int fast_reset()
  {
    return is_inited_ ? reset() : common::OB_SUCCESS;
  }
  template <class PartitionKeyIter>
  int query_partition_status_from_table_schema_(const common::ObPartitionKey& pkey, PartitionKeyIter& pkey_iter,
      const int64_t schema_version, const bool is_sub_part_template, PartitionStatus& part_status);
  int check_tenant_schema_guard(const uint64_t tenant_id) const;
  int get_schema_mgr(const uint64_t tenant_id, const ObSchemaMgr*& schema_mgr) const;
  int get_schema_mgr_info(const uint64_t tenant_id, const ObSchemaMgrInfo*& schema_mgr_info) const;
  int check_lazy_guard(const uint64_t tenant_id, const ObSchemaMgr*& mgr) const;
  int get_schema_status(const uint64_t tenant_id, ObRefreshSchemaStatus& schema_status);

  static bool compair_schema_mgr_info_with_tenant_id(const ObSchemaMgrInfo& schema_mgr, const uint64_t& tenant_id);
  bool check_fetch_table_id(const uint64_t table_id) const;
  int check_ora_conn_access(const uint64_t tenant_id, const uint64_t user_id, bool print_warn,
      const common::ObIArray<uint64_t>& role_id_array);
  int try_mock_rowid(const ObTableSchema* orig_table, const ObTableSchema*& final_table);

private:
  ObMultiVersionSchemaService* schema_service_;
  int64_t snapshot_version_;
  uint64_t session_id_;  // 0: default value (session_id_ is useless)
                         // OB_INVALID_ID: inner session
                         // other: session id from SQL
                         // it's use to control if table is visable in some sessions

  // mgr_ is null means ObSchemaGetterGuard is running in lazy mode.
  const ObSchemaMgr* mgr_;
  ObSchemaMgrHandle mgr_handle_;

  static const int MAX_ID_SCHEMAS = 32;

  // manage memory of specail tenant schema and mock schemas
  common::ObArenaAllocator mock_allocator_;
  ObTenantSchema* mock_gts_schema_;
  ObSimpleTenantSchema* mock_simple_gts_schema_;
  // manage mock schema objects in standby cluster
  ObSchemaGetterGuardHelper schema_helper_;

  // tenant_id_ is valid means it's tenant schema guard
  uint64_t tenant_id_;
  // manage memory of schema_mgr_infos_ and schema_objs_
  common::ObArenaAllocator local_allocator_;
  SchemaMgrInfos schema_mgr_infos_;
  // for new lazy logic
  SchemaObjs schema_objs_;

  SchemaGuardType schema_guard_type_;
  bool is_standby_cluster_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaGetterGuard);
};
}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_

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

#ifndef _OB_OCEANBASE_SCHEMA_SCHEMA_SERVICE_SQL_IMPL
#define _OB_OCEANBASE_SCHEMA_SCHEMA_SERVICE_SQL_IMPL

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "common/ob_string_buf.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_tenant_sql_service.h"
#include "share/schema/ob_database_sql_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_tablegroup_sql_service.h"
#include "share/schema/ob_user_sql_service.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/schema/ob_outline_sql_service.h"
#include "share/schema/ob_synonym_sql_service.h"
#include "share/schema/ob_udf_sql_service.h"
#include "share/schema/ob_sequence_sql_service.h"
#include "share/schema/ob_sys_variable_sql_service.h"
#include "share/schema/ob_profile_sql_service.h"
#include "share/schema/ob_dblink_sql_service.h"

namespace oceanbase {
namespace common {
class ObSqlString;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObCommonConfig;
}  // namespace common
namespace share {
class ObDMLSqlSplicer;
namespace schema {
// new cache
class ObSimpleTenantSchema;
class ObSimpleUserSchema;
class ObSimpleDatabaseSchema;
class ObSimpleTablegroupSchema;
class ObSimpleTableSchemaV2;
class VersionHisKey;
class VersionHisVal;
class ObSimpleSysVariableSchema;

class ObSchemaServiceSQLImpl : public ObSchemaService {
private:
  static bool cmp_table_id(const ObTableSchema* a, const ObTableSchema* b)
  {
    return a->get_table_id() < b->get_table_id();
  }

public:
  const static int64_t MAX_IN_QUERY_PER_TIME = 100L;  // FIXME@:change from 1000 to 100 for debugging
  const static int TENANT_MAP_BUCKET_NUM = 1024;
  const static int64_t MAX_BATCH_PART_NUM = 5000;

  ObSchemaServiceSQLImpl();
  virtual ~ObSchemaServiceSQLImpl();
  virtual int init(common::ObMySQLProxy* sql_proxy, common::ObDbLinkProxy* dblink_proxy,
      const share::schema::ObServerSchemaService* schema_service) override;
  virtual void set_common_config(const common::ObCommonConfig* config) override
  {
    config_ = config;
  }

#define GET_DDL_SQL_SERVICE_FUNC(SCHEMA_TYPE, SCHEMA)                \
  Ob##SCHEMA_TYPE##SqlService& get_##SCHEMA##_sql_service() override \
  {                                                                  \
    return SCHEMA##_service_;                                        \
  };
  GET_DDL_SQL_SERVICE_FUNC(Tenant, tenant);
  GET_DDL_SQL_SERVICE_FUNC(Database, database);
  GET_DDL_SQL_SERVICE_FUNC(Table, table);
  GET_DDL_SQL_SERVICE_FUNC(Tablegroup, tablegroup);
  GET_DDL_SQL_SERVICE_FUNC(User, user);
  GET_DDL_SQL_SERVICE_FUNC(Priv, priv);
  GET_DDL_SQL_SERVICE_FUNC(Outline, outline);
  GET_DDL_SQL_SERVICE_FUNC(Synonym, synonym);
  GET_DDL_SQL_SERVICE_FUNC(UDF, udf);
  GET_DDL_SQL_SERVICE_FUNC(Sequence, sequence);
  GET_DDL_SQL_SERVICE_FUNC(SysVariable, sys_variable);
  GET_DDL_SQL_SERVICE_FUNC(Profile, profile);
  GET_DDL_SQL_SERVICE_FUNC(DbLink, dblink);

  /* sequence_id related */
  virtual int init_sequence_id(const int64_t rootservice_epoch) override;
  virtual int inc_sequence_id() override;
  virtual uint64_t get_sequence_id() override
  {
    return schema_info_.get_sequence_id();
  };

  virtual int64_t get_last_operation_schema_version() const override
  {
    return last_operation_schema_version_;
  };
  virtual uint64_t get_last_operation_tenant_id() const override
  {
    return last_operation_tenant_id_;
  };
  virtual int set_last_operation_info(const uint64_t tenant_id, const int64_t schema_version) override;

  virtual int get_refresh_schema_info(ObRefreshSchemaInfo& schema_info) override;
  // enable refresh schema info
  virtual int set_refresh_schema_info(const ObRefreshSchemaInfo& schema_info) override;

  virtual bool is_sync_primary_ddl() const override
  {
    return 0 < primary_schema_versions_.count();
  }
  // get schema of __all_core_table
  virtual int get_all_core_table_schema(ObTableSchema& table_schema) override;
  virtual void set_in_bootstrap(const bool is_in_bootstrap) override
  {
    is_in_bootstrap_ = is_in_bootstrap;
  }
  virtual bool is_in_bootstrap() const override
  {
    return is_in_bootstrap_;
  }
  // get schemas of core tables, need get from kv table
  int get_core_table_schemas(common::ObISQLClient& sql_client, common::ObArray<ObTableSchema>& core_schemas) override;

  int get_sys_table_schemas(const common::ObIArray<uint64_t>& table_ids, common::ObISQLClient& sql_client,
      common::ObIAllocator& allocator, common::ObArray<ObTableSchema*>& sys_schemas) override;

  /**
   * for schema fetcher
   */
  virtual int get_table_schema(const ObRefreshSchemaStatus& schema_status, const uint64_t table_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      ObTableSchema*& table_schema) override;

  virtual int get_batch_table_schema(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      common::ObArray<uint64_t>& table_ids, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      common::ObArray<ObTableSchema*>& table_schema_array) override;

  /**
   * for refresh full schema
   */
  virtual int get_all_tenants(common::ObISQLClient& sql_client, const int64_t schema_version,
      common::ObIArray<ObSimpleTenantSchema>& schema_array) override;
  virtual int get_sys_variable(common::ObISQLClient& client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, const int64_t schema_version, ObSimpleSysVariableSchema& schema) override;
#define GET_ALL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)            \
  virtual int get_all_##SCHEMA##s(common::ObISQLClient& sql_client, \
      const ObRefreshSchemaStatus& schema_status,                   \
      const int64_t schema_version,                                 \
      const uint64_t tenant_id,                                     \
      common::ObIArray<SCHEMA_TYPE>& schema_array) override;
  GET_ALL_SCHEMA_FUNC_DECLARE(user, ObSimpleUserSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  GET_ALL_SCHEMA_FUNC_DECLARE(db_priv, ObDBPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(table_priv, ObTablePriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(sys_priv, ObSysPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(obj_priv, ObObjPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(dblink, ObDbLinkSchema);

  // get tenant increment schema operation between (base_version, new_schema_version]
  virtual int get_increment_schema_operations(const ObRefreshSchemaStatus& schema_status, const int64_t base_version,
      const int64_t new_schema_version, common::ObISQLClient& sql_client,
      SchemaOperationSetWithAlloc& schema_operations) override;

  // get tenant increment schema operation (base_version, ] with limit, for rs used only
  virtual int get_increment_schema_operations(const ObRefreshSchemaStatus& schema_status, const int64_t base_version,
      common::ObISQLClient& sql_client, SchemaOperationSetWithAlloc& schema_operations) override;

  virtual int check_sys_schema_change(common::ObISQLClient& sql_client, const common::ObIArray<uint64_t>& sys_table_ids,
      const int64_t schema_version, const int64_t new_schema_version, bool& sys_schema_change) override;

  virtual int get_tenant_schema(const int64_t schema_version, common::ObISQLClient& client, const uint64_t tenant_id,
      ObTenantSchema& tenant_schema) override;

  // get table schema of a single table
  virtual int get_table_schema_from_inner_table(const ObRefreshSchemaStatus& schema_status, const uint64_t table_id,
      common::ObISQLClient& sql_client, ObTableSchema& table_schema) override;

  virtual int fetch_new_tenant_id(uint64_t& new_tenant_id) override;
  virtual int fetch_new_table_id(const uint64_t tenant_id, uint64_t& new_table_id) override;
  virtual int fetch_new_database_id(const uint64_t tenant_id, uint64_t& new_database_id) override;
  virtual int fetch_new_tablegroup_id(const uint64_t tenant_id, uint64_t& new_tablegroup_id) override;
  virtual int fetch_new_user_id(const uint64_t tenant_id, uint64_t& new_user_id) override;
  virtual int fetch_new_outline_id(const uint64_t tenant_id, uint64_t& new_outline_id) override;
  virtual int fetch_new_synonym_id(const uint64_t tenant_id, uint64_t& new_synonym_id) override;
  virtual int fetch_new_udf_id(const uint64_t tenant_id, uint64_t& new_udf_id) override;
  virtual int fetch_new_constraint_id(const uint64_t tenant_id, uint64_t& new_constraint_id) override;
  virtual int fetch_new_sequence_id(const uint64_t tenant_id, uint64_t& new_sequence_id) override;
  virtual int fetch_new_dblink_id(const uint64_t tenant_id, uint64_t& new_dblink_id) override;

  virtual int fetch_new_profile_id(const uint64_t tenant_id, uint64_t& new_profile_id) override;

  //  virtual int insert_sys_param(const ObSysParam &sys_param,
  //                               common::ObISQLClient *sql_client);

  virtual int delete_partition_table(const uint64_t partition_table_id, const uint64_t tenant_id,
      const int64_t partition_idx, common::ObISQLClient& sql_client) override;

  virtual int get_tablegroup_schema(const ObRefreshSchemaStatus& schema_status, const uint64_t tablegroup_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      ObTablegroupSchema*& tablegroup_schema) override;

  virtual int get_batch_tenants(common::ObISQLClient& client, const int64_t schema_version,
      common::ObArray<SchemaKey>& schema_keys, common::ObIArray<ObSimpleTenantSchema>& schema_array) override;

#define GET_BATCH_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                     \
  virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus& schema_status, \
      common::ObISQLClient& client,                                             \
      const int64_t schema_version,                                             \
      common::ObArray<SchemaKey>& schema_keys,                                  \
      common::ObIArray<SCHEMA_TYPE>& schema_array) override;
  GET_BATCH_SCHEMAS_FUNC_DECLARE(user, ObSimpleUserSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(db_priv, ObDBPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(table_priv, ObTablePriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sys_variable, ObSimpleSysVariableSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(obj_priv, ObObjPriv);

  // get table schema of a table id list by schema version
  GET_BATCH_SCHEMAS_FUNC_DECLARE(dblink, ObDbLinkSchema);

  // batch will split big query into batch query, each time MAX_IN_QUERY_PER_TIME
  // get_batch_xxx_schema will call fetch_all_xxx_schema
#define GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                 \
  virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus& schema_status, \
      const int64_t schema_version,                                             \
      common::ObArray<uint64_t>& SCHEMA##_ids,                                  \
      common::ObISQLClient& sql_client,                                         \
      common::ObIArray<SCHEMA_TYPE>& SCHEMA##_schema_array) override;
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(database, ObDatabaseSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(tablegroup, ObTablegroupSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(outline, ObOutlineInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(synonym, ObSynonymInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(udf, ObUDF);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);

  virtual int get_batch_users(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      common::ObArray<uint64_t>& tenant_user_ids, common::ObISQLClient& sql_client,
      common::ObArray<ObUserInfo>& user_info_array) override;

  virtual int get_batch_tenants(common::ObISQLClient& client, const int64_t schema_version,
      common::ObArray<uint64_t>& tenant_ids, common::ObIArray<ObTenantSchema>& schema_array) override;

#define FETCH_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE) \
  int fetch_##SCHEMA##s(common::ObISQLClient& client,   \
      const ObRefreshSchemaStatus& schema_status,       \
      const int64_t schema_version,                     \
      const uint64_t tenant_id,                         \
      common::ObIArray<SCHEMA_TYPE>& schema_array,      \
      const SchemaKey* schema_keys = NULL,              \
      const int64_t schema_key_size = 0);
  FETCH_SCHEMAS_FUNC_DECLARE(user, ObSimpleUserSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  FETCH_SCHEMAS_FUNC_DECLARE(db_priv, ObDBPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(table_priv, ObTablePriv);
  FETCH_SCHEMAS_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(dblink, ObDbLinkSchema);

  FETCH_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(obj_priv, ObObjPriv);

  int fetch_tenants(common::ObISQLClient& client, const int64_t schema_version,
      common::ObIArray<ObSimpleTenantSchema>& schema_array, const SchemaKey* schema_keys = NULL,
      const int64_t schema_key_size = 0);

/**
 * fetch full schema
 */
#define FETCH_FULL_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                \
  int fetch_all_##SCHEMA##_info(const ObRefreshSchemaStatus& schema_status, \
      const int64_t schema_version,                                         \
      const uint64_t tenant_id,                                             \
      common::ObISQLClient& sql_client,                                     \
      common::ObIArray<SCHEMA_TYPE>& SCHEMA##_schema_array,                 \
      const uint64_t* SCHEMA##_ids = NULL,                                  \
      const int64_t SCHEMA##_ids_size = 0);

  FETCH_FULL_SCHEMAS_FUNC_DECLARE(database, ObDatabaseSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(tablegroup, ObTablegroupSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(outline, ObOutlineInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(synonym, ObSynonymInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udf, ObUDF);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);

  int fetch_all_user_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<ObUserInfo>& user_array,
      const uint64_t* user_keys = NULL, const int64_t users_size = 0);

  int fetch_role_grantee_map_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, ObISQLClient& sql_client, ObArray<ObUserInfo>& user_array, const bool is_fetch_role,
      const uint64_t* user_keys /* = NULL */, const int64_t users_size /* = 0 */);

  virtual int get_core_version(
      common::ObISQLClient& sql_client, const int64_t frozen_version, int64_t& core_schema_version) override;
  virtual int get_baseline_schema_version(
      common::ObISQLClient& sql_client, const int64_t frozen_version, int64_t& baseline_schema_version) override;

  virtual int fetch_schema_version(
      const ObRefreshSchemaStatus& schema_status, common::ObISQLClient& sql_client, int64_t& schema_version) override;

  virtual void set_refreshed_schema_version(const int64_t schema_version) override;
  virtual int gen_new_schema_version(
      const uint64_t tenant_id, const int64_t refreshed_schema_version, int64_t& schema_version) override;

  virtual int get_new_schema_version(uint64_t tenant_id, int64_t& schema_version) override;

  virtual int get_ori_schema_version(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, int64_t& last_schema_version) override;

  /**
   * for recycle bin
   */
  virtual int insert_recyclebin_object(const ObRecycleObject& recycle_obj, common::ObISQLClient& sql_client) override;

  virtual int fetch_recycle_object(const uint64_t tenant_id, const common::ObString& object_name,
      const ObRecycleObject::RecycleObjType recycle_obj_type, common::ObISQLClient& sql_client,
      common::ObIArray<ObRecycleObject>& recycle_objs) override;

  virtual int delete_recycle_object(
      const uint64_t tenant_id, const ObRecycleObject& recycle_object, common::ObISQLClient& sql_client) override;

  virtual int fetch_expire_recycle_objects(const uint64_t tenant_id, const int64_t expire_time,
      common::ObISQLClient& sql_client, common::ObIArray<ObRecycleObject>& recycle_objs) override;

  virtual int fetch_recycle_objects_of_db(const uint64_t tenant_id, const uint64_t database_id,
      common::ObISQLClient& sql_client, common::ObIArray<ObRecycleObject>& recycle_objs) override;

  virtual int query_table_status(const ObRefreshSchemaStatus& schema_status, common::ObISQLClient& sql_client,
      const int64_t schema_version, const uint64_t tenant_id, const uint64_t table_id, const bool is_pg,
      TableStatus& table_status) override;
  // for backup
  virtual int construct_recycle_table_object(
      common::ObISQLClient& sql_client, const ObSimpleTableSchemaV2& table, ObRecycleObject& recycle_object) override;

  virtual int construct_recycle_database_object(
      common::ObISQLClient& sql_client, const ObDatabaseSchema& database, ObRecycleObject& recycle_object) override;

  // for liboblog
  virtual int query_partition_status_from_sys_table(const ObRefreshSchemaStatus& schema_status,
      common::ObISQLClient& sql_client, const int64_t schema_version, const common::ObPartitionKey& pkey,
      const bool is_sub_part_template, PartitionStatus& table_status) override;
  virtual int fetch_aux_tables(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      common::ObIArray<ObAuxTableMetaInfo>& aux_tables) override;

  // link table.
  virtual int get_link_table_schema(const ObDbLinkSchema& dblink_schema, const common::ObString& database_name,
      const common::ObString& table_name, common::ObIAllocator& allocator, ObTableSchema*& table_schema) override;

  static int check_ddl_id_exist(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const common::ObString& ddl_id_str, bool& is_exists);
  virtual int construct_schema_version_history(const ObRefreshSchemaStatus& schema_status,
      common::ObISQLClient& sql_client, const int64_t snapshot_version, const VersionHisKey& version_his_key,
      VersionHisVal& version_his_val) override;

  static int fetch_tenant_compat_mode(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, common::ObCompatibilityMode& mode);
  static int sort_partition_array(ObPartitionSchema& partition_schema);
  static int sort_subpartition_array(ObPartitionSchema& partition_schema);

  static int gen_bootstrap_schema_version(const int64_t tenant_id, const int64_t refreshed_schema_version,
      int64_t& gen_schema_version, int64_t& schema_version);
  virtual int get_mock_schema_infos(common::ObISQLClient& sql_client, common::ObIArray<uint64_t>& tenant_ids,
      common::hash::ObHashMap<uint64_t, ObMockSchemaInfo>& new_mock_schema_infos) override;

  virtual int get_drop_tenant_infos(common::ObISQLClient& sql_client, int64_t schema_version,
      common::ObIArray<ObDropTenantInfo>& drop_tenant_infos) override;

  virtual int query_tenant_status(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, TenantStatus& tenant_status) override;
  virtual int get_schema_version_by_timestamp(common::ObISQLClient& sql_client,
      const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, int64_t timestamp,
      int64_t& schema_version) override;
  virtual int get_first_trans_end_schema_version(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, int64_t& schema_version) override;
  virtual int load_split_schema_version(common::ObISQLClient& sql_client, int64_t& split_schema_version) override;
  virtual int get_split_schema_version_v2(common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, int64_t& split_schema_version) override;

private:
  bool check_inner_stat();
  int fetch_new_schema_id(const uint64_t tenant_id, const share::ObMaxIdType max_id_type, uint64_t& new_schema_id);

  int get_core_table_priorities(common::ObISQLClient& sql_client, common::ObArray<ObTableSchema>& core_schemas);
  int get_core_table_columns(common::ObISQLClient& sql_client, common::ObArray<ObTableSchema>& core_schemaas);

  // get schemas of sys tables and user tables, read from schema related core tables
  int get_not_core_table_schemas(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const common::ObArray<uint64_t>& table_ids, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      common::ObArray<ObTableSchema*>& not_core_schemas);

  template <typename T>
  int fetch_all_table_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version, uint64_t tenant_id,
      common::ObISQLClient& sql_client, common::ObIAllocator& allocator, common::ObIArray<T>& table_schema_array,
      const uint64_t* table_ids = NULL, const int64_t table_ids_size = 0);
  int fetch_all_column_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<ObTableSchema*>& table_schema_array,
      const uint64_t* table_ids = NULL, const int64_t table_ids_size = 0);
  int fetch_all_constraint_info_ignore_inner_table(const ObRefreshSchemaStatus& schema_status,
      const int64_t schema_version, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObArray<ObTableSchema*>& table_schema_array, const uint64_t* table_ids = NULL,
      const int64_t table_ids_size = 0);
  int fetch_all_constraint_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<ObTableSchema*>& table_schema_array,
      const uint64_t* table_ids = NULL, const int64_t table_ids_size = 0);

  template <typename T>
  int gen_batch_fetch_array(common::ObArray<T*>& table_schema_array, const uint64_t* table_ids /* = NULL */,
      const int64_t table_ids_size /*= 0 */, common::ObIArray<uint64_t>& part_tables,
      common::ObIArray<uint64_t>& def_subpart_tables, common::ObIArray<uint64_t>& subpart_tables,
      common::ObIArray<int64_t>& part_idxs, common::ObIArray<int64_t>& def_subpart_idxs,
      common::ObIArray<int64_t>& subpart_idxs);

  template <typename T>
  int fetch_all_partition_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<T*>& table_schema_array,
      const uint64_t* table_ids /* = NULL */, const int64_t table_ids_size /*= 0 */);

  int fetch_all_tenant_info(const int64_t schema_version, common::ObISQLClient& client,
      common::ObIArray<ObTenantSchema>& tenant_schema_array, const uint64_t* tenant_ids = NULL,
      const int64_t tenant_ids_size = 0);

  int get_sys_variable_schema(common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, const int64_t schema_version,
      share::schema::ObSysVariableSchema& sys_variable_schema) override;

  template <typename T>
  int retrieve_schema_version(T& result, int64_t& schema_version);
  // retrieve core table and sys table don't read history table, set check_deleted to false
  // to filter is_deleted column

  int sql_append_pure_ids(const ObRefreshSchemaStatus& schema_status, const uint64_t* ids, const int64_t ids_size,
      common::ObSqlString& sql);

  //-------------------------- for new schema_cache ------------------------------

  int get_not_core_table_schema(const ObRefreshSchemaStatus& schema_status, const uint64_t table_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      ObTableSchema*& table_schema);

  int fetch_table_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, const uint64_t table_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, common::ObIAllocator& allocator,
      ObTableSchema*& table_schema);
  int fetch_column_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, const uint64_t table_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, ObTableSchema*& table_schema);
  int fetch_constraint_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      ObTableSchema*& table_schema);
  int fetch_constraint_column_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t schema_version, common::ObISQLClient& sql_client, ObConstraint*& cst);
  template <typename T>
  int fetch_all_part_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<T*>& range_part_tables,
      const uint64_t* table_ids /* = NULL */, const int64_t table_ids_size /*= 0 */);
  template <typename T>
  int fetch_all_def_subpart_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<T*>& range_subpart_tables,
      const uint64_t* table_ids /* = NULL */, const int64_t table_ids_size /*= 0 */);

  template <typename T>
  int fetch_all_subpart_info(const ObRefreshSchemaStatus& schema_status, const int64_t schema_version,
      const uint64_t tenant_id, common::ObISQLClient& sql_client, common::ObArray<T*>& range_subpart_tables,
      const uint64_t* table_ids /* = NULL */, const int64_t table_ids_size /*= 0 */);

  int fetch_partition_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      ObTableSchema*& table_schema);

  int fetch_foreign_key_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      ObTableSchema& table_schema);
  int fetch_foreign_key_column_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, ObForeignKeyInfo& foreign_key_info);

  int fetch_foreign_key_array_for_simple_table_schemas(const ObRefreshSchemaStatus& schema_status,
      const int64_t schema_version, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObArray<ObSimpleTableSchemaV2*>& table_schema_array, const uint64_t* table_ids,
      const int64_t table_ids_size);

  int fetch_constraint_array_for_simple_table_schemas(const ObRefreshSchemaStatus& schema_status,
      const int64_t schema_version, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObArray<ObSimpleTableSchemaV2*>& table_schema_array, const uint64_t* table_ids,
      const int64_t table_ids_size);

  // whether we can see the expected version or not
  // @return OB_SCHEMA_EAGAIN when not readable
  virtual int can_read_schema_version(const ObRefreshSchemaStatus& schema_status, int64_t expected_version) override;

  int fetch_tablegroup_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t tablegroup_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      common::ObIAllocator& allocator, ObTablegroupSchema*& tablegroup_schema);

  int fetch_partition_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t tablegroup_id, const int64_t schema_version, common::ObISQLClient& sql_client,
      ObTablegroupSchema*& tablegroup_schema);

  template <typename T>
  int fetch_link_table_info(const ObDbLinkSchema& dblink_schema, const common::ObString& database_name,
      const common::ObString& table_name, common::ObDbLinkProxy& dblink_client, ObIAllocator& alloctor,
      T*& table_schema);
  int fetch_link_column_info(const ObDbLinkSchema& dblink_schema, const common::ObString& database_name,
      const common::ObString& table_name, common::ObDbLinkProxy& dblink_client, ObTableSchema& table_schema);
  int try_mock_link_table_pkey(ObTableSchema& table_schema);

  template <typename SCHEMA>
  int fetch_part_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id, const uint64_t schema_id,
      const int64_t schema_version, common::ObISQLClient& sql_client, SCHEMA*& schema);

  template <typename SCHEMA>
  int fetch_sub_part_info(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      const uint64_t schema_id, const int64_t schema_version, common::ObISQLClient& sql_client, SCHEMA*& schema);

  template <typename SCHEMA>
  int sort_table_partition_info(SCHEMA& table_schema);

  int sort_tablegroup_partition_info(ObTablegroupSchema& tablegroup_schema);

  template <typename SCHEMA>
  int try_mock_partition_array(SCHEMA& table_schema);

  template <typename SCHEMA>
  int try_mock_def_subpartition_array(SCHEMA& table_schema);

  int fetch_temp_table_schemas(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      common::ObISQLClient& sql_client, common::ObIArray<ObTableSchema*>& table_schema_array);

  int fetch_temp_table_schema(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      common::ObISQLClient& sql_client, ObTableSchema& table_schema);

  int fetch_sys_variable(common::ObISQLClient& client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, const int64_t schema_version, ObSimpleSysVariableSchema& sys_variable);

  int fetch_sys_variable_version(common::ObISQLClient& sql_client, const ObRefreshSchemaStatus& schema_status,
      const uint64_t tenant_id, const int64_t schema_version, int64_t& fetch_schema_version);

  int get_tenant_system_variable(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id,
      common::ObIAllocator& cal_buf, common::ObISQLClient& sql_client, int64_t schema_version,
      common::ObString& var_name, common::ObObj& var_value);

  static uint64_t fill_exec_tenant_id(const ObRefreshSchemaStatus& schema_status);
  static uint64_t fill_extract_tenant_id(const ObRefreshSchemaStatus& schema_status, const uint64_t tenant_id);
  static uint64_t fill_extract_schema_id(const ObRefreshSchemaStatus& schema_status, const uint64_t schema_id);
  int gen_not_schema_split_version(int64_t& schema_version);
  int gen_leader_sys_schema_version(const int64_t tenant_id, int64_t& schema_version);
  int gen_leader_normal_schema_version(
      const uint64_t tenant_id, const int64_t refreshed_schema_version, int64_t& schema_version);

private:
  common::ObMySQLProxy* mysql_proxy_;
  common::ObDbLinkProxy* dblink_proxy_;
  lib::ObMutex mutex_;
  // record last schema version of log operation while execute ddl
  int64_t last_operation_schema_version_;
  ObTenantSqlService tenant_service_;
  ObDatabaseSqlService database_service_;
  ObTableSqlService table_service_;
  ObTablegroupSqlService tablegroup_service_;
  ObUserSqlService user_service_;
  ObPrivSqlService priv_service_;
  ObOutlineSqlService outline_service_;
  // ObZoneSqlService zone_service_;
  // to make sure the generated schema version in increased globally
  // ex. RS switched, the new RS must , refresh_schema first from inner
  // table, then update refreshed version, even the new RS local time
  // is before normal time, the new generated schema version is stil
  // larger than refresh schema version
  //
  // to avoid the effect of machine time changed by admin, refreshed
  // schema version must be updated  after local schema refreshed
  int64_t refreshed_schema_version_;
  int64_t gen_schema_version_;
  const common::ObCommonConfig* config_;
  bool is_inited_;
  ObSynonymSqlService synonym_service_;
  ObUDFSqlService udf_service_;
  ObSequenceSqlService sequence_service_;
  ObProfileSqlService profile_service_;

  common::SpinRWLock rw_lock_;
  uint64_t last_operation_tenant_id_;
  uint64_t sequence_id_;
  ObRefreshSchemaInfo schema_info_;

  ObSysVariableSqlService sys_variable_service_;
  ObDbLinkSqlService dblink_service_;
  // When cluster is in boostrap, we generate new schema version by a certain step.
  bool is_in_bootstrap_;
  /*
   * When synchronize ddl of primary cluster's system tenant in standby cluster,
   * we generate new schema versions by existed schema version from primary cluster.
   * primary_schema_versions_ is used to store schema versions from primary cluster,
   * which may be reset before or after ddl execution.
   */
  common::ObSEArray<int64_t, 5> primary_schema_versions_;
  // record used schema version's index in primary_schema_versions_
  int64_t primary_schema_used_index_;
  common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> gen_schema_version_map_;
  const ObServerSchemaService* schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaServiceSQLImpl);
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_SERVICE_SQL_IMPL_H */

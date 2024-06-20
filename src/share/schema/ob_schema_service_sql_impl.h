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
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_tenant_sql_service.h"
#include "share/schema/ob_database_sql_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_tablegroup_sql_service.h"
#include "share/schema/ob_user_sql_service.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/schema/ob_outline_sql_service.h"
#include "share/schema/ob_synonym_sql_service.h"
#include "share/schema/ob_routine_sql_service.h"
#include "share/schema/ob_trigger_sql_service.h"
#include "share/schema/ob_udf_sql_service.h"
#include "share/schema/ob_udt_sql_service.h"
#include "share/schema/ob_sequence_sql_service.h"
#include "share/schema/ob_sys_variable_sql_service.h"
#include "share/schema/ob_keystore_sql_service.h"
#include "share/schema/ob_label_se_policy_sql_service.h"
#include "share/schema/ob_tablespace_sql_service.h"
#include "share/schema/ob_profile_sql_service.h"
#include "share/schema/ob_security_audit_sql_service.h"
#include "share/schema/ob_dblink_sql_service.h"
#include "share/schema/ob_directory_sql_service.h"
#include "share/schema/ob_context_sql_service.h"
#include "share/schema/ob_rls_sql_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "sql/dblink/ob_dblink_utils.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObCommonConfig;
}
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
// new cache
class ObSimpleTenantSchema;
class ObSimpleUserSchema;
class ObSimpleDatabaseSchema;
class ObSimpleTablegroupSchema;
class ObSimpleTableSchemaV2;
class ObSimpleRoutineSchema;
class ObSimplePackageSchema;
class ObSimpleTriggerSchema;
class ObSimpleUDTSchema;
class ObSimpleSysVariableSchema;
class ObKeystoreSchema;
class ObTablespaceSchema;
class ObSimpleMockFKParentTableSchema;
class ObMockFKParentTableSchema;

class ObSchemaServiceSQLImpl : public ObSchemaService
{
private:
  static bool cmp_table_id(const ObTableSchema *a, const ObTableSchema *b)
  {
    return a->get_table_id() < b->get_table_id();
  }
public:
  const static int64_t MAX_IN_QUERY_PER_TIME = 100L; //FIXME@xiyu:change from 1000 to 100 for debugging
  const static int TENANT_MAP_BUCKET_NUM = 1024;
  const static int64_t MAX_BATCH_PART_NUM = 5000;

  struct TableTrunc {
    TableTrunc() : table_id_(OB_INVALID_ID), truncate_version_(OB_INVALID_VERSION) {}
    TableTrunc(uint64_t table_id, int64_t truncate_version) : table_id_(table_id), truncate_version_(truncate_version) {}
    uint64_t table_id_;
    int64_t truncate_version_;
    TO_STRING_KV(K_(table_id), K_(truncate_version));
  };

  ObSchemaServiceSQLImpl();
  virtual ~ObSchemaServiceSQLImpl();
  virtual int init(common::ObMySQLProxy *sql_proxy,
                   common::ObDbLinkProxy *dblink_proxy,
                   const share::schema::ObServerSchemaService *schema_service);
  virtual void set_common_config(const common::ObCommonConfig *config) { config_ = config; }

#define GET_DDL_SQL_SERVICE_FUNC(SCHEMA_TYPE, SCHEMA)            \
  Ob##SCHEMA_TYPE##SqlService &get_##SCHEMA##_sql_service() {    \
    return SCHEMA##_service_;                                    \
  }
  GET_DDL_SQL_SERVICE_FUNC(Tenant, tenant)
  GET_DDL_SQL_SERVICE_FUNC(Database, database)
  GET_DDL_SQL_SERVICE_FUNC(Table, table)
  GET_DDL_SQL_SERVICE_FUNC(Tablegroup, tablegroup)
  GET_DDL_SQL_SERVICE_FUNC(User, user)
  GET_DDL_SQL_SERVICE_FUNC(Priv, priv)
  GET_DDL_SQL_SERVICE_FUNC(Outline, outline)
  GET_DDL_SQL_SERVICE_FUNC(Routine, routine)
  GET_DDL_SQL_SERVICE_FUNC(Trigger, trigger)
  GET_DDL_SQL_SERVICE_FUNC(Synonym, synonym)
  GET_DDL_SQL_SERVICE_FUNC(UDF, udf)
  GET_DDL_SQL_SERVICE_FUNC(UDT, udt)
  GET_DDL_SQL_SERVICE_FUNC(Sequence, sequence)
  GET_DDL_SQL_SERVICE_FUNC(SysVariable, sys_variable)
  GET_DDL_SQL_SERVICE_FUNC(Keystore, keystore)
  GET_DDL_SQL_SERVICE_FUNC(LabelSePolicy, label_se_policy)
  GET_DDL_SQL_SERVICE_FUNC(Tablespace, tablespace)
  GET_DDL_SQL_SERVICE_FUNC(Profile, profile)
  GET_DDL_SQL_SERVICE_FUNC(Audit, audit)
  GET_DDL_SQL_SERVICE_FUNC(DbLink, dblink)
  GET_DDL_SQL_SERVICE_FUNC(Directory, directory)
  GET_DDL_SQL_SERVICE_FUNC(Context, context)
  GET_DDL_SQL_SERVICE_FUNC(Rls, rls)

  /* sequence_id related */
  virtual int init_sequence_id(const int64_t rootservice_epoch);
  virtual int inc_sequence_id();
  virtual uint64_t get_sequence_id() { SpinRLockGuard guard(rw_lock_); return sequence_id_;}

  virtual int get_refresh_schema_info(ObRefreshSchemaInfo &schema_info);
  //enable refresh schema info
  virtual int set_refresh_schema_info(const ObRefreshSchemaInfo &schema_info);

  // get schema of __all_core_table
  virtual int get_all_core_table_schema(ObTableSchema &table_schema);
  virtual void set_cluster_schema_status(const ObClusterSchemaStatus &status) override
  { ATOMIC_SET(&cluster_schema_status_, status); }
  virtual ObClusterSchemaStatus get_cluster_schema_status() const override
  { return ATOMIC_LOAD(&cluster_schema_status_); }
  // get schemas of core tables, need get from kv table
  int get_core_table_schemas(common::ObISQLClient &sql_client,
                             const ObRefreshSchemaStatus &schema_status,
                             common::ObArray<ObTableSchema> &core_schemas);

  int get_sys_table_schemas(common::ObISQLClient &sql_client,
                            const ObRefreshSchemaStatus &schema_status,
                            const common::ObIArray<uint64_t> &table_ids,
                            common::ObIAllocator &allocator,
                            common::ObArray<ObTableSchema *> &sys_schemas);

  /**
   * for schema fetcher
   */
  virtual int get_table_schema(const ObRefreshSchemaStatus &schema_status,
                               const uint64_t table_id,
                               const int64_t schema_version,
                               common::ObISQLClient &sql_client,
                               common::ObIAllocator &allocator,
                               ObTableSchema *&table_schema);
  virtual int get_batch_table_schema(const ObRefreshSchemaStatus &schema_status,
                                     const int64_t schema_version,
                                     common::ObArray<uint64_t> &table_ids,
                                     common::ObISQLClient &sql_client,
                                     common::ObIAllocator &allocator,
                                     common::ObArray<ObTableSchema *> &table_schema_array);

  /**
   * for refresh full schema
   */
  virtual int get_all_tenants(common::ObISQLClient &sql_client,
                              const int64_t schema_version,
                              common::ObIArray<ObSimpleTenantSchema> &schema_array);
  virtual int get_sys_variable(common::ObISQLClient &client,
                               const ObRefreshSchemaStatus &schema_status,
                               const uint64_t tenant_id,
                               const int64_t schema_version,
                               ObSimpleSysVariableSchema &schema);
  #define GET_ALL_SCHEMA_WITH_ALLOCATOR_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE) \
    virtual int get_all_##SCHEMA##s(common::ObISQLClient &sql_client, \
                                    common::ObIAllocator &allocator,  \
                                    const ObRefreshSchemaStatus &schema_status, \
                                    const int64_t schema_version,      \
                                    const uint64_t tenant_id,           \
                                    common::ObIArray<SCHEMA_TYPE *> &schema_array);
  #define GET_ALL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                 \
    virtual int get_all_##SCHEMA##s(common::ObISQLClient &sql_client, \
                                    const ObRefreshSchemaStatus &schema_status, \
                                    const int64_t schema_version,      \
                                    const uint64_t tenant_id,           \
                                    common::ObIArray<SCHEMA_TYPE> &schema_array);
  GET_ALL_SCHEMA_FUNC_DECLARE(user, ObSimpleUserSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  GET_ALL_SCHEMA_WITH_ALLOCATOR_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  GET_ALL_SCHEMA_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(db_priv, ObDBPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(table_priv, ObTablePriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(routine_priv, ObRoutinePriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(column_priv, ObColumnPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(package, ObSimplePackageSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(trigger, ObSimpleTriggerSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(keystore, ObKeystoreSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(audit, ObSAuditSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(sys_priv, ObSysPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(obj_priv, ObObjPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE(dblink, ObDbLinkSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(directory, ObDirectorySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(context, ObContextSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(rls_policy, ObRlsPolicySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(rls_group, ObRlsGroupSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE(rls_context, ObRlsContextSchema);

  //get tenant increment schema operation between (base_version, new_schema_version]
  virtual int get_increment_schema_operations(const ObRefreshSchemaStatus &schema_status,
                                              const int64_t base_version,
                                              const int64_t new_schema_version,
                                              common::ObISQLClient &sql_client,
                                              SchemaOperationSetWithAlloc &schema_operations);

  virtual int check_sys_schema_change(
              common::ObISQLClient &sql_client,
              const ObRefreshSchemaStatus &schema_status,
              const common::ObIArray<uint64_t> &sys_table_ids,
              const int64_t schema_version,
              const int64_t new_schema_version,
              bool &sys_schema_change);


  //get table schema of a single table
  virtual int get_table_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                                const uint64_t table_id,
                                                common::ObISQLClient &sql_client,
                                                ObTableSchema &table_schema);
  virtual int get_db_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                             const uint64_t &database_id,
                                             ObIArray<ObDatabaseSchema> &database_schema,
                                             ObISQLClient &sql_client);
  virtual int get_full_table_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                                     const int64_t &table_id,
                                                     ObTableSchema &table_schema,
                                                     ObArenaAllocator &allocator,
                                                     ObMySQLTransaction &trans);
  // get mock fk parent table schema of a single mock fk parent table
  virtual int get_mock_fk_parent_table_schema_from_inner_table(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t table_id,
      common::ObISQLClient &sql_client,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);

  virtual int fetch_new_object_ids(
              const uint64_t tenant_id,
              const int64_t object_cnt,
              uint64_t &max_object_id) override;
  virtual int fetch_new_partition_ids(
              const uint64_t tenant_id,
              const int64_t partition_num,
              uint64_t &max_partition_id) override;
  virtual int fetch_new_tablet_ids(
              const uint64_t tenant_id,
              const bool gen_normal_tablet,
              const uint64_t size,
              uint64_t &min_tablet_id) override;
  virtual int fetch_new_tenant_id(uint64_t &new_tenant_id);
  virtual int fetch_new_table_id(const uint64_t tenant_id, uint64_t &new_table_id);
  virtual int fetch_new_database_id(const uint64_t tenant_id, uint64_t &new_database_id);
  virtual int fetch_new_tablegroup_id(const uint64_t tenant_id, uint64_t &new_tablegroup_id);
  virtual int fetch_new_user_id(const uint64_t tenant_id, uint64_t &new_user_id);
  virtual int fetch_new_outline_id(const uint64_t tenant_id, uint64_t &new_outline_id);
  virtual int fetch_new_synonym_id(const uint64_t tenant_id, uint64_t &new_synonym_id);
  virtual int fetch_new_udf_id(const uint64_t tenant_id, uint64_t &new_udf_id);
  virtual int fetch_new_constraint_id(const uint64_t tenant_id, uint64_t &new_constraint_id);
  virtual int fetch_new_sequence_id(const uint64_t tenant_id, uint64_t &new_sequence_id);
  virtual int fetch_new_udt_id(const uint64_t tenant_id, uint64_t &new_udt_id);
  virtual int fetch_new_routine_id(const uint64_t tenant_id, uint64_t &new_routine_id);
  virtual int fetch_new_package_id(const uint64_t tenant_id, uint64_t &new_package_id);
  virtual int fetch_new_dblink_id(const uint64_t tenant_id, uint64_t &new_dblink_id);
  virtual int fetch_new_sys_pl_object_id(const uint64_t tenant_id, uint64_t &new_object_id);

  virtual int fetch_new_keystore_id(const uint64_t tenant_id, uint64_t &new_keystore_id);
  virtual int fetch_new_master_key_id(const uint64_t tenant_id, uint64_t &new_master_key_id);
  virtual int fetch_new_label_se_policy_id(const uint64_t tenant_id, uint64_t &label_se_policy_id);
  virtual int fetch_new_label_se_component_id(const uint64_t tenant_id, uint64_t &label_se_component_id);
  virtual int fetch_new_label_se_label_id(const uint64_t tenant_id, uint64_t &label_se_label_id);
  virtual int fetch_new_label_se_user_level_id(const uint64_t tenant_id, uint64_t &label_se_user_level_id);
  virtual int fetch_new_tablespace_id(const uint64_t tenant_id, uint64_t &new_tablespace_id);
  virtual int fetch_new_trigger_id(const uint64_t tenant_id, uint64_t &new_trigger_id);
  virtual int fetch_new_profile_id(const uint64_t tenant_id, uint64_t &new_profile_id);
  virtual int fetch_new_audit_id(const uint64_t tenant_id, uint64_t &new_audit_id);
  virtual int fetch_new_directory_id(const uint64_t tenant_id, uint64_t &new_directory_id);
  virtual int fetch_new_context_id(const uint64_t tenant_id, uint64_t &new_context_id);
  virtual int fetch_new_rls_policy_id(const uint64_t tenant_id, uint64_t &new_rls_policy_id);
  virtual int fetch_new_rls_group_id(const uint64_t tenant_id, uint64_t &new_rls_group_id);
  virtual int fetch_new_rls_context_id(const uint64_t tenant_id, uint64_t &new_rls_context_id);
  virtual int fetch_new_priv_id(const uint64_t tenant_id, uint64_t &new_priv_id);
//  virtual int insert_sys_param(const ObSysParam &sys_param,
//                               common::ObISQLClient *sql_client);

  virtual int get_tablegroup_schema(const ObRefreshSchemaStatus &schema_status,
                                    const uint64_t tablegroup_id,
                                    const int64_t schema_version,
                                    common::ObISQLClient &sql_client,
                                    common::ObIAllocator &allocator,
                                    ObTablegroupSchema *&tablegroup_schema) override;

  virtual int get_batch_tenants(common::ObISQLClient &client,
                                const int64_t schema_version,
                                common::ObArray<SchemaKey> &schema_keys,
                                common::ObIArray<ObSimpleTenantSchema> &schema_array);

#define GET_BATCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE) \
    virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status,\
                                      common::ObISQLClient &client,     \
                                      common::ObIAllocator &allocator,  \
                                      const int64_t schema_version,     \
                                      common::ObArray<SchemaKey> &schema_keys, \
                                      common::ObIArray<SCHEMA_TYPE *> &schema_array);
#define GET_BATCH_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)           \
    virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status,\
                                      common::ObISQLClient &client,     \
                                      const int64_t schema_version,     \
                                      common::ObArray<SchemaKey> &schema_keys, \
                                      common::ObIArray<SCHEMA_TYPE> &schema_array);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(user, ObSimpleUserSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  GET_BATCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(db_priv, ObDBPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(table_priv, ObTablePriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(routine_priv, ObRoutinePriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(column_priv, ObColumnPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(package, ObSimplePackageSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(trigger, ObSimpleTriggerSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sys_variable, ObSimpleSysVariableSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(keystore, ObKeystoreSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(audit, ObSAuditSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(obj_priv, ObObjPriv);

  //get table schema of a table id list by schema version
  GET_BATCH_SCHEMAS_FUNC_DECLARE(dblink, ObDbLinkSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(directory, ObDirectorySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(context, ObContextSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(rls_policy, ObRlsPolicySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(rls_group, ObRlsGroupSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE(rls_context, ObRlsContextSchema);

  //batch will split big query into batch query, each time MAX_IN_QUERY_PER_TIME
  //get_batch_xxx_schema will call fetch_all_xxx_schema
#define GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                                        \
  virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status,\
                                    const int64_t schema_version,                           \
                                    common::ObArray<uint64_t> &SCHEMA##_ids,               \
                                    common::ObISQLClient &sql_client,                       \
                                    common::ObIArray<SCHEMA_TYPE> &SCHEMA##_schema_array);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(database, ObDatabaseSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(outline, ObOutlineInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(routine, ObRoutineInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(synonym, ObSynonymInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(package, ObPackageInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(trigger, ObTriggerInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(udf, ObUDF);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(udt, ObUDTTypeInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(sequence, ObSequenceSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(keystore, ObKeystoreSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(profile, ObProfileSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(audit, ObSAuditSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE(mock_fk_parent_table, ObMockFKParentTableSchema);

  virtual int get_batch_users(const ObRefreshSchemaStatus &schema_status,
                              const int64_t schema_version,
                              common::ObArray<uint64_t> &tenant_user_ids,
                              common::ObISQLClient &sql_client,
                              common::ObArray<ObUserInfo> &user_info_array);

  virtual int get_batch_tenants(common::ObISQLClient &client,
                                const int64_t schema_version,
                                common::ObArray<uint64_t> &tenant_ids,
                                common::ObIArray<ObTenantSchema> &schema_array);

#define FETCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)     \
    int fetch_##SCHEMA##s(common::ObISQLClient &client,     \
                          common::ObIAllocator &allocator,  \
                          const ObRefreshSchemaStatus &schema_status,\
                          const int64_t schema_version,     \
                          const uint64_t tenant_id,         \
                          common::ObIArray<SCHEMA_TYPE *> &schema_array, \
                          const SchemaKey *schema_keys = NULL,         \
                          const int64_t schema_key_size = 0);
#define FETCH_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)   \
    int fetch_##SCHEMA##s(common::ObISQLClient &client,     \
                          const ObRefreshSchemaStatus &schema_status,\
                          const int64_t schema_version,     \
                          const uint64_t tenant_id,         \
                          common::ObIArray<SCHEMA_TYPE> &schema_array, \
                          const SchemaKey *schema_keys = NULL,         \
                          const int64_t schema_key_size = 0);
  FETCH_SCHEMAS_FUNC_DECLARE(user, ObSimpleUserSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  FETCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE(table, ObSimpleTableSchemaV2);
  FETCH_SCHEMAS_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(db_priv, ObDBPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(table_priv, ObTablePriv);
  FETCH_SCHEMAS_FUNC_DECLARE(routine_priv, ObRoutinePriv);
  FETCH_SCHEMAS_FUNC_DECLARE(column_priv, ObColumnPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(package, ObSimplePackageSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(trigger, ObSimpleTriggerSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(udf, ObSimpleUDFSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(dblink, ObDbLinkSchema);

  FETCH_SCHEMAS_FUNC_DECLARE(keystore, ObKeystoreSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  FETCH_SCHEMAS_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(audit, ObSAuditSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(obj_priv, ObObjPriv);
  FETCH_SCHEMAS_FUNC_DECLARE(directory, ObDirectorySchema);
  FETCH_SCHEMAS_FUNC_DECLARE(context, ObContextSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(rls_policy, ObRlsPolicySchema);
  FETCH_SCHEMAS_FUNC_DECLARE(rls_group, ObRlsGroupSchema);
  FETCH_SCHEMAS_FUNC_DECLARE(rls_context, ObRlsContextSchema);

  int fetch_mock_fk_parent_table_column_info(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const int64_t schema_version,
      common::ObISQLClient &sql_client,
      ObMockFKParentTableSchema &mock_fk_parent_table);

  int fetch_tenants(common::ObISQLClient &client,
                    const int64_t schema_version,
                    common::ObIArray<ObSimpleTenantSchema> &schema_array,
                    const SchemaKey *schema_keys = NULL,
                    const int64_t schema_key_size = 0);

  /**
   * fetch full schema
   */
  #define FETCH_FULL_SCHEMAS_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                         \
    int fetch_all_##SCHEMA##_info(const ObRefreshSchemaStatus &schema_status, \
                                  const int64_t schema_version,                        \
                                  const uint64_t tenant_id,                            \
                                  common::ObISQLClient &sql_client,                    \
                                  common::ObIArray<SCHEMA_TYPE> &SCHEMA##_schema_array, \
                                  const uint64_t *SCHEMA##_ids = NULL,                 \
                                  const int64_t SCHEMA##_ids_size = 0);

  FETCH_FULL_SCHEMAS_FUNC_DECLARE(database, ObDatabaseSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(outline, ObOutlineInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(routine, ObRoutineInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(package, ObPackageInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(trigger, ObTriggerInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(routine_param, ObRoutineInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(synonym, ObSynonymInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udf, ObUDF);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udt, ObUDTTypeInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udt_attr, ObUDTTypeInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udt_coll, ObUDTTypeInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(udt_object, ObUDTTypeInfo);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(sequence, ObSequenceSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(keystore, ObKeystoreSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(label_se_policy, ObLabelSePolicySchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(label_se_component, ObLabelSeComponentSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(label_se_label, ObLabelSeLabelSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(label_se_user_level, ObLabelSeUserLevelSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(tablespace, ObTablespaceSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(profile, ObProfileSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(audit, ObSAuditSchema);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(sys_priv, ObSysPriv);
  FETCH_FULL_SCHEMAS_FUNC_DECLARE(mock_fk_parent_table, ObMockFKParentTableSchema);

  int fetch_all_user_info(const ObRefreshSchemaStatus &schema_status,
                          const int64_t schema_version,
                          const uint64_t tenant_id,
                          common::ObISQLClient &sql_client,
                          common::ObArray<ObUserInfo> &user_array,
                          const uint64_t *user_keys = NULL,
                          const int64_t users_size = 0);

  int fetch_role_grantee_map_info(
      const ObRefreshSchemaStatus &schema_status,
      const int64_t schema_version,
      const uint64_t tenant_id,
      ObISQLClient &sql_client,
      ObArray<ObUserInfo> &user_array,
      const bool is_fetch_role,
      const uint64_t *user_keys /* = NULL */,
      const int64_t users_size /* = 0 */);

 int fetch_proxy_user_info(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t schema_version,
    const uint64_t tenant_id,
    ObISQLClient &sql_client,
    ObArray<ObUserInfo> &user_array,
    const bool is_fetch_proxy,
    const uint64_t *user_keys /* = NULL */,
    const int64_t users_size /* = 0 */);

 int fetch_proxy_role_info(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t schema_version,
    const uint64_t tenant_id,
    ObISQLClient &sql_client,
    ObArray<ObUserInfo> &user_array,
    const bool is_fetch_proxy,
    const uint64_t *user_keys /* = NULL */,
    const int64_t users_size /* = 0 */);

  virtual int get_core_version(common::ObISQLClient &sql_client,
                               const ObRefreshSchemaStatus &schema_status,
                               int64_t &core_schema_version);
  virtual int get_baseline_schema_version(
              common::ObISQLClient &sql_client,
              const ObRefreshSchemaStatus &schema_status,
              int64_t &baseline_schema_version);

  virtual int fetch_schema_version(const ObRefreshSchemaStatus &schema_status,
                                   common::ObISQLClient &sql_client,
                                   int64_t &schema_version);

  virtual void set_refreshed_schema_version(const int64_t schema_version);
  virtual int gen_new_schema_version(const uint64_t tenant_id,
                                     const int64_t refreshed_schema_version,
                                     int64_t &schema_version);

  // gen schema versions in [start_version, end_version] with specified schema version cnt.
  // @param[out]:
  // - schema_version: end_version
  virtual int gen_batch_new_schema_versions(
              const uint64_t tenant_id,
              const int64_t refreshed_schema_version,
              const int64_t version_cnt,
              int64_t &schema_version);


  virtual int get_new_schema_version(uint64_t tenant_id, int64_t &schema_version);

  virtual int get_ori_schema_version(const ObRefreshSchemaStatus &schema_status, const uint64_t tenant_id, const uint64_t table_id, int64_t &last_schema_version);

  /**
   * for recycle bin
   */
  virtual int insert_recyclebin_object(
      const ObRecycleObject &recycle_obj,
      common::ObISQLClient &sql_client);

  virtual int fetch_recycle_object(
      const uint64_t tenant_id,
      const common::ObString &object_name,
      const ObRecycleObject::RecycleObjType recycle_obj_type,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs);

  virtual int delete_recycle_object(
      const uint64_t tenant_id,
      const ObRecycleObject &recycle_object,
      common::ObISQLClient &sql_client);

  virtual int fetch_expire_recycle_objects(
      const uint64_t tenant_id,
      const int64_t expire_time,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs);

  virtual int fetch_recycle_objects_of_db(
      const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs);

  // for backup
  virtual int construct_recycle_table_object(
      common::ObISQLClient &sql_client,
      const ObSimpleTableSchemaV2 &table,
      ObRecycleObject &recycle_object);

  virtual int construct_recycle_database_object(
      common::ObISQLClient &sql_client,
      const ObDatabaseSchema &database,
      ObRecycleObject &recycle_object);

  virtual int fetch_aux_tables(const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t schema_version,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObAuxTableMetaInfo> &aux_tables);

  // link table.
  virtual int get_link_table_schema(const ObDbLinkSchema *dblink_schema,
                                    const common::ObString &database_name,
                                    const common::ObString &table_name,
                                    common::ObIAllocator &allocator,
                                    ObTableSchema *&table_schema,
                                    sql::ObSQLSessionInfo *session_info,
                                    const ObString &dblink_name,
                                    bool is_reverse_link,
                                    uint64_t *current_scn);

  static int check_ddl_id_exist(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObString &ddl_id_str,
      bool &is_exists);
  virtual int construct_schema_version_history(
      const ObRefreshSchemaStatus &schema_status,
      common::ObISQLClient &sql_client,
      const int64_t snapshot_version,
      const VersionHisKey &version_his_key,
      VersionHisVal &version_his_val);

  static int fetch_tenant_compat_mode(common::ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      common::ObCompatibilityMode &mode);

#ifdef OB_BUILD_TDE_SECURITY
  static int fetch_master_key(common::ObISQLClient &sql_client,
                              const uint64_t tenant_id,
                              const uint64_t master_key_id,
                              share::ObMasterKey *key,
                              common::ObString &encrypt_out);
#endif

  virtual int get_drop_tenant_infos(
      common::ObISQLClient &sql_client,
      int64_t schema_version,
      common::ObIArray<ObDropTenantInfo> &drop_tenant_infos);

  virtual int query_tenant_status(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      TenantStatus &tenant_status);
  virtual int get_schema_version_by_timestamp(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      int64_t timestamp,
      int64_t &schema_version);
  virtual int get_first_trans_end_schema_version(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      int64_t &schema_version);

  static int sort_table_partition_info_v2(ObTableSchema &table_schema);

  // Get latest schema version from inner table for each table_id.
  // The count of table_schema_versions may be less than the count of table_ids
  // when table is deleted and the schema history is recycled.
  virtual int get_table_latest_schema_versions(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &table_ids,
      common::ObIArray<ObTableLatestSchemaVersion> &table_schema_versions);

  /*----------- interfaces for latest schema start -----------*/
  virtual int get_tablegroup_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObString &tablegroup_name,
              uint64_t &tablegroup_id) override;

  virtual int get_database_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObString &database_name,
              uint64_t &database_id) override;

  virtual int get_table_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t session_id,
              const ObString &table_name,
              uint64_t &table_id,
              ObTableType &table_type,
              int64_t &schema_version) override;

  // index_name comparsion:
  // 1. oracle & !is_mysql_sys_database : case sensitive
  // 2. mysql || is_mysql_sys_database : case insensitive
  //
  // @param[int]:
  // - index_name : should be a "full" index name, can't be a original index name
  // @param[out]:
  // - index_id : OB_INVALID_ID means that index doesn't exist
  virtual int get_index_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &index_name,
              uint64_t &index_id) override;

  virtual int get_mock_fk_parent_table_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &table_name,
              uint64_t &mock_fk_parent_table_id) override;

  virtual int get_synonym_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &synonym_name,
              uint64_t &synonym_id) override;

  virtual int get_constraint_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &constraint_name,
              uint64_t &constraint_id) override;

  virtual int get_foreign_key_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &foreign_key_name,
              uint64_t &foreign_key_id) override;

  virtual int get_sequence_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &sequence_name,
              uint64_t &sequence_id,
              bool &is_system_generated) override;

  virtual int get_package_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &package_name,
              const ObPackageType package_type,
              const int64_t compatible_mode,
              uint64_t &package_id) override;

  virtual int get_routine_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t package_id,
              const uint64_t overload,
              const ObString &routine_name,
              common::ObIArray<std::pair<uint64_t, share::schema::ObRoutineType>> &routine_pairs) override;

  virtual int get_udt_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t package_id,
              const ObString &udt_name,
              uint64_t &udt_id) override;

  virtual int get_table_schema_versions(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const common::ObIArray<uint64_t> &table_ids,
              common::ObIArray<ObSchemaIdVersion> &versions) override;

  virtual int get_mock_fk_parent_table_schema_versions(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const common::ObIArray<uint64_t> &table_ids,
              common::ObIArray<ObSchemaIdVersion> &versions) override;

  virtual int get_audits_in_owner(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObSAuditType audit_type,
              const uint64_t owner_id,
              common::ObIArray<ObSAuditSchema> &audit_schemas) override;

  /*----------- interfaces for latest schema end -------------*/

private:
  bool check_inner_stat();
  int fetch_new_normal_rowid_table_tablet_ids_(
      const uint64_t tenant_id,
      const uint64_t size,
      uint64_t &min_tablet_id);
  int fetch_new_extended_rowid_table_tablet_ids_(
      const uint64_t tenant_id,
      const uint64_t size,
      uint64_t &min_tablet_id);
  int fetch_new_schema_id_(
      const uint64_t tenant_id,
      const share::ObMaxIdType max_id_type,
      uint64_t &new_schema_id);
  int fetch_new_tablet_ids_(
      const uint64_t tenant_id,
      const share::ObMaxIdType max_id_type,
      const uint64_t size,
      uint64_t &min_tablet_id);

  int get_core_table_priorities(common::ObISQLClient &sql_client,
                                const ObRefreshSchemaStatus &schema_status,
                                common::ObArray<ObTableSchema> &core_schemas);
  int get_core_table_columns(common::ObISQLClient &sql_client,
                             const ObRefreshSchemaStatus &schema_status,
                             common::ObArray<ObTableSchema> &core_schemas);

  // get schemas of sys tables and user tables, read from schema related core tables
  int get_not_core_table_schemas(const ObRefreshSchemaStatus &schema_status,
              const int64_t schema_version,
              const common::ObArray<uint64_t> &table_ids,
              common::ObISQLClient &sql_client,
              common::ObIAllocator &allocator,
              common::ObArray<ObTableSchema *> &not_core_schemas);

  template <typename T>
  int fetch_all_table_info(const ObRefreshSchemaStatus &schema_status,
                           const int64_t schema_version,
                           uint64_t tenant_id,
                           common::ObISQLClient &sql_client,
                           common::ObIAllocator &allocator,
                           common::ObIArray<T> &table_schema_array,
                           const uint64_t *table_ids = NULL,
                           const int64_t table_ids_size = 0);
  int fetch_all_column_info(const ObRefreshSchemaStatus &schema_status,
                            const int64_t schema_version,
                            const uint64_t tenant_id,
                            common::ObISQLClient &sql_client,
                            common::ObArray<ObTableSchema *> &table_schema_array,
                            const uint64_t *table_ids = NULL,
                            const int64_t table_ids_size = 0);
  int fetch_all_column_group_info(const ObRefreshSchemaStatus &schema_status,
                                  const int64_t schema_version,
                                  const uint64_t tenant_id,
                                  common::ObISQLClient &sql_client,
                                  common::ObArray<ObTableSchema *> &table_schema_array,
                                  const uint64_t *table_ids,
                                  const int64_t table_ids_size);
  int fetch_all_column_group_schema(const ObRefreshSchemaStatus &schema_status,
                                    const int64_t schema_version,
                                    const uint64_t tenant_id,
                                    common::ObISQLClient &sql_client,
                                    const bool is_history,
                                    common::ObArray<ObTableSchema *> &table_schema_array,
                                    const uint64_t *table_ids,
                                    const int64_t table_ids_size);
  int fetch_all_column_group_mapping(const ObRefreshSchemaStatus &schema_status,
                                     const int64_t schema_version,
                                     const uint64_t tenant_id,
                                     common::ObISQLClient &sql_client,
                                     const bool is_history,
                                     ObArray<ObTableSchema *> &table_schema_array,
                                     const uint64_t *table_ids,
                                     const int64_t table_ids_size);
  int fetch_all_constraint_info_ignore_inner_table(const ObRefreshSchemaStatus &schema_status,
                                                   const int64_t schema_version,
                                                   const uint64_t tenant_id,
                                                   common::ObISQLClient &sql_client,
                                                   common::ObArray<ObTableSchema *> &table_schema_array,
                                                   const uint64_t *table_ids = NULL,
                                                   const int64_t table_ids_size = 0);
  int fetch_all_constraint_info(const ObRefreshSchemaStatus &schema_status,
                                const int64_t schema_version,
                            const uint64_t tenant_id,
                            common::ObISQLClient &sql_client,
                            common::ObArray<ObTableSchema *> &table_schema_array,
                            const uint64_t *table_ids = NULL,
                            const int64_t table_ids_size = 0);

  template<typename T>
  int gen_batch_fetch_array(
      common::ObArray<T *> &table_schema_array,
      const uint64_t *table_ids /* = NULL */,
      const int64_t table_ids_size /*= 0 */,
      common::ObIArray<TableTrunc> &part_tables,
      common::ObIArray<TableTrunc> &subpart_tables,
      common::ObIArray<uint64_t> &def_subpart_tables,
      common::ObIArray<int64_t> &part_idxs,
      common::ObIArray<int64_t> &def_subpart_idxs,
      common::ObIArray<int64_t> &subpart_idxs);

  template<typename T>
  int fetch_all_partition_info(const ObRefreshSchemaStatus &schema_status,
                               const int64_t schema_version,
                               const uint64_t tenant_id,
                               common::ObISQLClient &sql_client,
                               common::ObArray<T *> &table_schema_array,
                               const uint64_t *table_ids /* = NULL */,
                               const int64_t table_ids_size /*= 0 */);

  int fetch_all_tenant_info(const int64_t schema_version,
                            common::ObISQLClient &client,
                            common::ObIArray<ObTenantSchema> &tenant_schema_array,
                            const uint64_t *tenant_ids = NULL,
                            const int64_t tenant_ids_size = 0);

  int get_sys_variable_schema(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const int64_t schema_version,
      share::schema::ObSysVariableSchema &sys_variable_schema);

  template<typename T>
  int retrieve_schema_version(T &result, int64_t &schema_version);
  // retrieve core table and sys table don't read history table, set check_deleted to false
  // to filter is_deleted column

  int sql_append_pure_ids(const ObRefreshSchemaStatus &schema_status,
                          const TableTrunc *ids,
                          const int64_t ids_size,
                          common::ObSqlString &sql);
  int sql_append_pure_ids(const ObRefreshSchemaStatus &schema_status,
                          const uint64_t *ids,
                          const int64_t ids_size,
                          common::ObSqlString &sql);
  int sql_append_ids_and_truncate_version(const ObRefreshSchemaStatus &schema_status,
                                          const TableTrunc *ids,
                                          const int64_t ids_size,
                                          const int64_t schema_version,
                                          common::ObSqlString &sql);

  //-------------------------- for new schema_cache ------------------------------

  int get_not_core_table_schema(const ObRefreshSchemaStatus &schema_status,
                                const uint64_t table_id,
                                const int64_t schema_version,
                                common::ObISQLClient &sql_client,
                                common::ObIAllocator &allocator,
                                ObTableSchema *&table_schema);

  int fetch_table_info(const ObRefreshSchemaStatus &schema_status,
                       const uint64_t tenant_id,
                       const uint64_t table_id,
                       const int64_t schema_version,
                       common::ObISQLClient &sql_client,
                       common::ObIAllocator &allocator,
                       ObTableSchema *&table_schema);
  int fetch_column_info(const ObRefreshSchemaStatus &schema_status,
                        const uint64_t tenant_id,
                        const uint64_t table_id,
                        const int64_t schema_version,
                        common::ObISQLClient &sql_client,
                        ObTableSchema *&table_schema);
  int fetch_constraint_info(const ObRefreshSchemaStatus &schema_status,
                        const uint64_t tenant_id,
                        const uint64_t table_id,
                        const int64_t schema_version,
                        common::ObISQLClient &sql_client,
                        ObTableSchema *&table_schema);
  int fetch_constraint_column_info(const ObRefreshSchemaStatus &schema_status,
                                   const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   const int64_t schema_version,
                                   common::ObISQLClient &sql_client,
                                   ObConstraint *&cst);
  template<typename T>
  int fetch_all_encrypt_info(const ObRefreshSchemaStatus &schema_status,
                        const int64_t schema_version,
                        const uint64_t tenant_id,
                        common::ObISQLClient &sql_client,
                        common::ObArray<T *> &table_schema_array,
                        const int64_t array_size);
  template<typename T>
  int fetch_all_part_info(const ObRefreshSchemaStatus &schema_status,
                          const int64_t schema_version,
                          const uint64_t tenant_id,
                          common::ObISQLClient &sql_client,
                          common::ObArray<T *> &range_part_tables,
                          const TableTrunc *table_ids /* = NULL */,
                          const int64_t table_ids_size /*= 0 */);
  template<typename T>
  int fetch_all_def_subpart_info(const ObRefreshSchemaStatus &schema_status,
                                 const int64_t schema_version,
                                 const uint64_t tenant_id,
                                 common::ObISQLClient &sql_client,
                                 common::ObArray<T *> &range_subpart_tables,
                                 const uint64_t *table_ids /* = NULL */,
                                 const int64_t table_ids_size /*= 0 */);

  template<typename T>
  int fetch_all_subpart_info(const ObRefreshSchemaStatus &schema_status,
                             const int64_t schema_version,
                             const uint64_t tenant_id,
                             common::ObISQLClient &sql_client,
                             common::ObArray<T *> &range_subpart_tables,
                             const TableTrunc *table_ids /* = NULL */,
                             const int64_t table_ids_size /*= 0 */);

  int fetch_partition_info(const ObRefreshSchemaStatus &schema_status,
                           const uint64_t tenant_id,
                           const uint64_t table_id,
                           const int64_t schema_version,
                           common::ObISQLClient &sql_client,
                           ObTableSchema *&table_schema);

  int fetch_column_group_info(const ObRefreshSchemaStatus &schema_status,
                              const uint64_t tenant_id,
                              const uint64_t table_id,
                              const int64_t schema_version,
                              common::ObISQLClient &sql_client,
                              ObTableSchema *&table_schema);

  template<typename TABLE_SCHEMA>
  int fetch_foreign_key_info(const ObRefreshSchemaStatus &schema_status,
                             const uint64_t tenant_id,
                             const uint64_t table_id,
                             const int64_t schema_version,
                             common::ObISQLClient &sql_client,
                             TABLE_SCHEMA &table_schema);
  int fetch_foreign_key_column_info(const ObRefreshSchemaStatus &schema_status,
                                    const uint64_t tenant_id,
                                    const int64_t schema_version,
                                    common::ObISQLClient &sql_client,
                                    ObForeignKeyInfo &foreign_key_info);

  int fetch_foreign_key_array_for_simple_table_schemas(const ObRefreshSchemaStatus &schema_status,
                                                       const int64_t schema_version,
                                                       const uint64_t tenant_id,
                                                       common::ObISQLClient &sql_client,
                                                       common::ObArray<ObSimpleTableSchemaV2 *> &table_schema_array,
                                                       const uint64_t *table_ids,
                                                       const int64_t table_ids_size);

  int fetch_constraint_array_for_simple_table_schemas(const ObRefreshSchemaStatus &schema_status,
                                                      const int64_t schema_version,
                                                      const uint64_t tenant_id,
                                                      common::ObISQLClient &sql_client,
                                                      common::ObArray<ObSimpleTableSchemaV2 *> &table_schema_array,
                                                      const uint64_t *table_ids,
                                                      const int64_t table_ids_size);

  int fetch_trigger_list(const ObRefreshSchemaStatus &schema_status,
                         const uint64_t tenant_id,
                         const uint64_t table_id,
                         const int64_t schema_version,
                         common::ObISQLClient &sql_client,
                         ObTableSchema &table_schema);

  int fetch_rls_object_list(const ObRefreshSchemaStatus &schema_status,
                            const uint64_t tenant_id,
                            const uint64_t table_id,
                            const int64_t schema_version,
                            common::ObISQLClient &sql_client,
                            ObTableSchema &table_schema);

  int fetch_rls_columns(const ObRefreshSchemaStatus &schema_status,
                        const int64_t schema_version,
                        const uint64_t tenant_id,
                        common::ObISQLClient &sql_client,
                        common::ObArray<ObRlsPolicySchema *> &rls_policy_array,
                        const uint64_t *table_ids,
                        const int64_t table_ids_size);

  // whether we can see the expected version or not
  // @return OB_SCHEMA_EAGAIN when not readable
  virtual int can_read_schema_version(const ObRefreshSchemaStatus &schema_status, int64_t expected_version) override;



  int fetch_tablegroup_info(const ObRefreshSchemaStatus &schema_status,
                            const uint64_t tenant_id,
                            const uint64_t tablegroup_id,
                            const int64_t schema_version,
                            common::ObISQLClient &sql_client,
                            common::ObIAllocator &allocator,
                            ObTablegroupSchema *&tablegroup_schema);

  int fetch_partition_info(const ObRefreshSchemaStatus &schema_status,
                           const uint64_t tenant_id,
                           const uint64_t tablegroup_id,
                           const int64_t schema_version,
                           common::ObISQLClient &sql_client,
                           ObTablegroupSchema *&tablegroup_schema);

  template<typename T>
  int fetch_link_table_info(common::sqlclient::dblink_param_ctx &param_ctx,
                            sql::DblinkGetConnType conn_type,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            ObIAllocator &alloctor,
                            T *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const ObString &dblink_name,
                            sql::ObReverseLink *reverse_link,
                            uint64_t *current_scn);
  template<typename T>
  int generate_link_table_schema(const common::sqlclient::dblink_param_ctx &param_ctx,
                                 sql::DblinkGetConnType conn_type,
                                 const ObString &database_name,
                                 const ObString &table_name,
                                 ObIAllocator &allocator,
                                 T *&table_schema,
                                 const sql::ObSQLSessionInfo *session_info,
                                 common::sqlclient::ObISQLConnection *dblink_conn,
                                 const common::sqlclient::ObMySQLResult *col_meta_result);
  int fetch_link_current_scn(const common::sqlclient::dblink_param_ctx &param_ctx,
                             sql::DblinkGetConnType conn_type,
                             ObIAllocator &allocator,
                             common::sqlclient::ObISQLConnection *dblink_conn,
                             sql::ObReverseLink *reverse_link,
                             uint64_t &current_scn);
  int try_mock_link_table_column(ObTableSchema &table_schema);

  template<typename SCHEMA>
  int fetch_part_info(const ObRefreshSchemaStatus &schema_status,
                      const uint64_t tenant_id,
                      const uint64_t schema_id,
                      const int64_t schema_version,
                      common::ObISQLClient &sql_client,
                      SCHEMA *&schema);

  template<typename SCHEMA>
  int fetch_sub_part_info(const ObRefreshSchemaStatus &schema_status,
                          const uint64_t tenant_id,
                          const uint64_t schema_id,
                          const int64_t schema_version,
                          common::ObISQLClient &sql_client,
                          SCHEMA *&schema);

  template<typename SCHEMA>
  int sort_tables_partition_info(const common::ObIArray<SCHEMA *> &table_schema_array);
  template<typename SCHEMA>
  int sort_table_partition_info(SCHEMA &table_schema);
  int sort_tablegroup_partition_info(ObTablegroupSchema &tablegroup_schema);

  static int sort_partition_array(ObPartitionSchema &partition_schema);
  static int sort_subpartition_array(ObPartitionSchema &partition_schema);

  template<typename SCHEMA>
  int try_mock_partition_array(SCHEMA &table_schema);
  template<typename SCHEMA>
  int try_mock_default_column_group(SCHEMA &table_schema);

  int fetch_temp_table_schemas(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObTableSchema*> &table_schema_array);

  int fetch_temp_table_schema(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      common::ObISQLClient &sql_client,
      ObTableSchema &table_schema);

  int fetch_sys_variable(
      common::ObISQLClient &client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const int64_t schema_version,
      ObSimpleSysVariableSchema &sys_variable);

  int fetch_sys_variable_version(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const int64_t schema_version,
      int64_t &fetch_schema_version);

  int get_tenant_system_variable(const ObRefreshSchemaStatus &schema_status,
                                 const uint64_t tenant_id,
                                 common::ObIAllocator &cal_buf,
                                 common::ObISQLClient &sql_client,
                                 int64_t schema_version,
                                 common::ObString &var_name,
                                 common::ObObj &var_value);

  static uint64_t fill_exec_tenant_id(const ObRefreshSchemaStatus &schema_status);
  static uint64_t fill_extract_tenant_id(const ObRefreshSchemaStatus &schema_status, const uint64_t tenant_id);
  static uint64_t fill_extract_schema_id(const ObRefreshSchemaStatus &schema_status, const uint64_t schema_id);

  int gen_tenant_new_schema_version_(
      const uint64_t tenant_id,
      const int64_t refreshed_schema_version,
      const int64_t version_cnt,
      int64_t &schema_version);
  int gen_new_schema_version_(
      const int64_t refreshed_schema_version,
      const int64_t gen_schema_version,
      const int64_t version_cnt,
      int64_t &schema_version);

  int retrieve_schema_id_with_name_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObSqlString &sql,
      const char* id_col_name,
      const char* name_col_name,
      const ObString &schema_name,
      const bool case_compare,
      const bool compare_with_collation,
      uint64_t &schema_id);

  bool schema_name_is_equal_(
       const ObString &src,
       const ObString &dst,
       const bool case_compare,
       const bool compare_with_collation);

  int fetch_table_latest_schema_versions_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &table_ids,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObIArray<ObTableLatestSchemaVersion> &table_schema_versions);

  int set_refresh_full_schema_timeout_ctx_(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const char* tname,
      ObTimeoutCtx &ctx);

  int calc_refresh_full_schema_timeout_ctx_(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const char* tname,
      int64_t &timeout,
      int64_t &row_cnt);

  bool in_parallel_ddl_thread_();
  bool need_column_group(const ObTableSchema &table_schema);
private:
  common::ObMySQLProxy *mysql_proxy_;
  common::ObDbLinkProxy *dblink_proxy_;
  // record last schema version of log operation while execute ddl
  int64_t last_operation_schema_version_;
  ObTenantSqlService tenant_service_;
  ObDatabaseSqlService database_service_;
  ObTableSqlService table_service_;
  ObTablegroupSqlService tablegroup_service_;
  ObUserSqlService user_service_;
  ObPrivSqlService priv_service_;
  ObOutlineSqlService outline_service_;
  //ObZoneSqlService zone_service_;
  ObRoutineSqlService routine_service_;
  ObTriggerSqlService trigger_service_;
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
  const common::ObCommonConfig *config_;
  bool is_inited_;
  ObSynonymSqlService synonym_service_;
  ObUDFSqlService udf_service_;
  ObUDTSqlService udt_service_;
  ObSequenceSqlService sequence_service_;
  ObKeystoreSqlService keystore_service_;
  ObLabelSePolicySqlService label_se_policy_service_;
  ObTablespaceSqlService tablespace_service_;;
  ObProfileSqlService profile_service_;
  ObAuditSqlService audit_service_;

  common::SpinRWLock rw_lock_;
  uint64_t last_operation_tenant_id_;
  uint64_t sequence_id_;
  ObRefreshSchemaInfo schema_info_;

  ObSysVariableSqlService sys_variable_service_;
  ObDbLinkSqlService dblink_service_;
  ObDirectorySqlService directory_service_;
  ObContextSqlService context_service_;
  ObRlsSqlService rls_service_;

  ObClusterSchemaStatus cluster_schema_status_;
  common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> gen_schema_version_map_;
  const ObServerSchemaService *schema_service_;

  lib::ObMutex object_ids_mutex_;
  lib::ObMutex normal_tablet_ids_mutex_;
  lib::ObMutex extended_tablet_ids_mutex_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaServiceSQLImpl);
};

}//namespace schema
}//namespace share
}//namespace oceanbase
#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_SERVICE_SQL_IMPL_H */

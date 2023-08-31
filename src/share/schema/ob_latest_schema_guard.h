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

#ifndef OB_OCEANBASE_SCHEMA_OB_LATEST_SCHEMA_GUARD_H_
#define OB_OCEANBASE_SCHEMA_OB_LATEST_SCHEMA_GUARD_H_
#include "lib/allocator/page_arena.h"       //ObArenaAllocator
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_udt_info.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaService;
struct SchemaObj;

// NOTICE:
// 1. Not thread safety.
// 2. Don't assume that objects are fetched under the same schema version.
// 3. Objects will be cached in guard when fetch object by id in the first time.
// 4. Should be used after related objects are locked by name/id.
class ObLatestSchemaGuard
{
const static int DEFAULT_RESERVE_SIZE = 32;
typedef common::ObSEArray<SchemaObj, DEFAULT_RESERVE_SIZE> SchemaObjs;
public:
  ObLatestSchemaGuard() = delete;
  ObLatestSchemaGuard(share::schema::ObMultiVersionSchemaService *schema_service,
                      const uint64_t tenant_id);
  ~ObLatestSchemaGuard();
public:
  /* -------------- interfaces without cache ---------------*/

  // 1. won't cache tablegroup_id by name.
  //
  // @param[in]:
  // - tablegroup_name : string comparsion is case sensitive in mysql/oracle tenant.
  // @param[out]:
  // - tablegroup_id : OB_INVALID_ID means tablegroup not exist
  int get_tablegroup_id(const ObString &tablegroup_name,
                        uint64_t &tablegroup_id);

  // 1. won't cache database_id by name.
  //
  // @param[in]:
  // - datasbase_name:
  // 1) If database name is "oceanbase", string comparsion is case insensitive.
  // 2) string comparsion is case sensitive in oracle tenant.
  // 3) string comparsion is case insensitive in mysql tenant.
  // @param[out]:
  // - database_id: OB_INVALID_ID means databse not exist
  int get_database_id(
      const common::ObString &database_name,
      uint64_t &database_id);

  // ATTENTION!!!!:
  //
  // 1. hidden/lob meta/lob piece tables are not visible in user namespace, so we can't get related objects from this interface.
  //
  // 2.TODO(yanmu.ztl): This interface doesn't support to get index id by index name.
  //
  // 3. we will match table name with the following priorities:
  // (rules with smaller sequence numbers have higher priority)
  // - 3.1. if session_id > 0, match table with specified session_id. (mysql tmp table or ctas table)
  // - 3.2. match table with session_id = 0.
  // - 3.3. if table name is inner table name (comparsion insensitive), match related inner table.
  // - 3.4. string comparsion is case sensitive in oracle tenant and is case insensitive in mysql tenant.
  //
  // 4. mock parent table is not visible in this interface.
  //
  // 5. won't cache database_id by name.
  //
  // @param[in]:
  // - database_id
  // - session_id
  // - table_name
  // @param[out]:
  // - table_id: OB_INVALID_ID means table not exist
  // - table_type
  // - schema_version
  int get_table_id(
      const uint64_t database_id,
      const uint64_t session_id,
      const ObString &table_name,
      uint64_t &table_id,
      ObTableType &table_type,
      int64_t &schema_version);

  // check if table is a mock parent table
  // 1. table name comparsion is case sensitive.
  // 2. won't cache mock_fk_parent_table_id by name.
  // 3. TODO(yanmu.ztl): May has poor performance when tenant has many mock parent tables
  //                     because related table is lack of index on name.
  // @param[in]:
  // - database_id
  // - table_name
  // @param[out]:
  // - mock_fk_parent_table_id : OB_INVALID_ID means databse not exist
  int get_mock_fk_parent_table_id(
      const uint64_t database_id,
      const ObString &table_name,
      uint64_t &mock_fk_parent_table_id);

  // 1. synonym_name comparsion is case sensitive.
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - synonym_name
  // @param[out]:
  // - synonym_id : OB_INVALID_ID means synonym not exist
  int get_synonym_id(const uint64_t database_id,
                     const ObString &synonym_name,
                     uint64_t &synonym_id);

  // 1. constraint name comparsion:
  // - case sensitive: oracle
  // - case insensitive: mysql
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - constraint_name
  // @param[out]:
  // - : OB_INVALID_ID means constraint not exist
  int get_constraint_id(const uint64_t database_id,
                        const ObString &constraint_name,
                        uint64_t &constraint_id);

  // 1. foreign key name comparsion:
  // - case sensitive: oracle
  // - case insensitive: mysql
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - foreign_key_name
  // @param[out]:
  // - : OB_INVALID_ID means foreign key not exist
  int get_foreign_key_id(const uint64_t database_id,
                         const ObString &foreign_key_name,
                         uint64_t &foreign_key_id);

  // 1. sequence name comparsion: case sensitive
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - sequence_name
  // @param[out]:
  // - is_system_generated : true means it's a inner sequence object
  // - sequence_id : OB_INVALID_ID means constraint not exist
  int get_sequence_id(const uint64_t database_id,
                      const ObString &sequence_name,
                      uint64_t &sequence_id,
                      bool &is_system_generated);

  // 1. package name comparsion:
  // - case sensitive: oracle
  // - case insensitive: mysql
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - package_name
  // - package_type
  // - compatible_mode
  // @param[out]:
  // - package_id : OB_INVALID_ID means package not exist
  int get_package_id(const uint64_t database_id,
                     const ObString &package_name,
                     const ObPackageType package_type,
                     const int64_t compatible_mode,
                     uint64_t &package_id);

  // 1. routine name comparsion:
  // - case sensitive: oracle
  // - case insensitive: mysql
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - package_id: can be OB_INVALID_ID
  // - overload: can be 0
  // - routine_name
  // @param[out]:
  // - routine_pairs : empty means routine not exist
  int get_routine_id(
      const uint64_t database_id,
      const uint64_t package_id,
      const uint64_t overload,
      const ObString &routine_name,
      common::ObIArray<std::pair<uint64_t, share::schema::ObRoutineType>> &routine_pairs);

  // 1. udt name comparsion:
  // - case sensitive: oracle
  // - case insensitive: mysql
  // 2. won't cache id by name.
  //
  // @param[in]:
  // - database_id
  // - package_id: can be OB_INVALID_ID
  // - type_code
  // - udt_name
  // @param[out]:
  // - exist
  int check_udt_exist(
      const uint64_t database_id,
      const uint64_t package_id,
      const ObUDTTypeCode type_code,
      const ObString &udt_name,
      bool &exist);

  // 1. won't cache versions.
  // @param[in]:
  // - table_ids
  // @param[out]:
  // - versions
  int get_table_schema_versions(
      const common::ObIArray<uint64_t> &table_ids,
      common::ObIArray<ObSchemaIdVersion> &versions);

  // 1. won't cache versions.
  // @param[in]:
  // - table_ids
  // @param[out]:
  // - versions
  int get_mock_fk_parent_table_schema_versions(
      const common::ObIArray<uint64_t> &table_ids,
      common::ObIArray<ObSchemaIdVersion> &versions);

  // 1. won't cache.
  // 2. return audits which is AUDIT_OBJ_DEFAULT and owner is OB_AUDIT_MOCK_USER_ID.
  // @param[in]:
  // - allocator
  // @param[out]:
  // - audit_schemas
  int get_default_audit_schemas(
      common::ObIArray<ObSAuditSchema> &audit_schemas);

  // 1. won't cache
  //
  // https://docs.oracle.com/cd/E18283_01/server.112/e17118/sql_elements008.htm
  // Within a namespace, no two objects can have the same name.
  // In oracle mode, the following schema objects share one namespace:
  // Tables(create, rename, flashback)
  // Views(create, create or replace, rename, flashback)
  // Sequences(create, rename)
  // Private synonyms(create, create or replace, rename)
  // Stand-alone procedures(create, create or replace)
  // Stand-alone stored functions(create, create or replace)
  // Packages(create, create or replace)
  // Materialized views (OB oracle mode is not supported now)
  // User-defined types(create, create or replace)

  // This function is used to check object name is duplicate in other different schemas in oracle mode.
  // This function should be as a supplement to the original oracle detection logic of duplicate object name.
  // @param [in] database_id
  // @param [in] session_id : for temporary table
  // @param [in] object_name
  // @param [in] schema_type : schema type of object to be checked
  // @param [in] routine_type : If schema_type is ROUTINE_SCHEMA, routine_type is used to
  //                            distinguish whether object is procedure or function.
  // @param [in] is_or_replace : distinguish whether create schema with create_or_replace option
  //
  // @return : return OB_ERR_EXIST_OBJECT when object name conflicts
  int check_oracle_object_exist(
      const uint64_t database_id,
      const uint64_t session_id,
      const ObString &object_name,
      const ObSchemaType &schema_type,
      const ObRoutineType &routine_type,
      const bool is_or_replace);

  /* -------------- interfaces without cache end ---------------*/

  /* -------------- interfaces with cache ---------------*/

  // 1. will cache schema in guard
  // @param[in]:
  // - table_id
  // @param[out]:
  // - table_schema: return NULL if table not exist
  int get_table_schema(
      const uint64_t table_id,
      const ObTableSchema *&table_schema);

  // 1. will cache schema in guard
  // @param[in]:
  // - mock_fk_parent_table_id
  // @param[out]:
  // - mock_fk_parent_table_schema: return NULL if mock fk parent table not exist
  int get_mock_fk_parent_table_schema(
      const uint64_t mock_fk_parent_table_id,
      const ObMockFKParentTableSchema *&mock_fk_parent_table_schema);

  // 1. will cache tablegroup schema in guard
  // @param[in]:
  // - tablegroup_id
  // @param[out]:
  // - tablegroup_schema: return NULL if tablegroup not exist
  int get_tablegroup_schema(
      const uint64_t tablegroup_id,
      const ObTablegroupSchema *&tablegroup_schema);

  // 1. will cache database schema in guard
  // @param[in]:
  // - database_id
  // @param[out]:
  // - database_schema: return NULL if database not exist
  int get_database_schema(
      const uint64_t database_id,
      const ObDatabaseSchema *&database_schema);

  // 1. will cache tenant schema in guard
  // @param[in]:
  // - tenant_id
  // @param[out]:
  // - tenant_schema: return NULL if tenant not exist
  int get_tenant_schema(
      const uint64_t tenant_id,
      const ObTenantSchema *&tenant_schema);

  // 1. will cache tablespace schema in guard
  // @param[in]:
  // - tablespace_id
  // @param[out]:
  // - tablespace_schema: return NULL if tablespace not exist
  int get_tablespace_schema(
      const uint64_t tablespace_id,
      const ObTablespaceSchema *&tablespace_schema);

  // 1. will cache udt schema in guard
  // @param[in]:
  // - udt_id
  // @param[out]:
  // - udt_info: return NULL if udt not exist
  int get_udt_info(
      const uint64_t udt_id,
      const ObUDTTypeInfo *udt_info);
  /* -------------- interfaces with cache end ---------------*/
private:
  int check_inner_stat_();
  int check_and_get_service_(
      ObSchemaService *&schema_service_impl,
      common::ObMySQLProxy *&sql_proxy);

  // For TENANT_SCHEMA, tenant_id should be OB_SYS_TENANT_ID;
  // For SYS_VARIABLE_SCHEMA, tenant_id should be equal with schema_id;
  template<typename T>
  int get_schema_(
      const ObSchemaType schema_type,
      const uint64_t tenant_id,
      const uint64_t schema_id,
      const T *&schema);

  template<typename T>
  int get_from_local_cache_(
      const ObSchemaType schema_type,
      const uint64_t tenant_id,
      const uint64_t schema_id,
      const T *&schema);

  template<typename T>
  int put_to_local_cache_(
      const ObSchemaType schema_type,
      const uint64_t tenant_id,
      const uint64_t schema_id,
      const T *&schema);
private:
  ObMultiVersionSchemaService *schema_service_;
  uint64_t tenant_id_;
  common::ObArenaAllocator local_allocator_;
  SchemaObjs schema_objs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatestSchemaGuard);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_LATEST_SCHEMA_GUARD_H_

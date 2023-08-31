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
#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_latest_schema_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObLatestSchemaGuard::ObLatestSchemaGuard(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id)
  : schema_service_(schema_service),
    tenant_id_(tenant_id),
    local_allocator_("LastestSchGuard"),
    schema_objs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_))
{
}

ObLatestSchemaGuard::~ObLatestSchemaGuard()
{
}

int ObLatestSchemaGuard::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id_, schema_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObLatestSchemaGuard::check_and_get_service_(
    ObSchemaService *&schema_service_impl,
    ObMySQLProxy *&sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy = schema_service_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  }
  return ret;
}

template<typename T>
int ObLatestSchemaGuard::get_schema_(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema)
{
  int ret = OB_SUCCESS;
  const ObSchema *base_schema = NULL;
  schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(TENANT_SCHEMA == schema_type && !is_sys_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id for TENANT_SCHEMA", KR(ret), K(tenant_id), K(schema_id));
  } else if (OB_UNLIKELY(SYS_VARIABLE_SCHEMA == schema_type && tenant_id != schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and schema_id not match for TENANT_SCHEMA",
             KR(ret), K(tenant_id), K(schema_id));
  } else if (OB_UNLIKELY(!is_normal_schema(schema_type)
             || OB_INVALID_ID == schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
  } else if (OB_FAIL(get_from_local_cache_(schema_type, tenant_id, schema_id, schema))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get schema from cache", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else if (OB_FAIL(schema_service_->get_latest_schema(
               local_allocator_, schema_type, tenant_id, schema_id, base_schema))) {
      LOG_WARN("fail to get latest schema", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else if (OB_ISNULL(base_schema)) {
      // schema not exist
    } else if (OB_FAIL(put_to_local_cache_(schema_type, tenant_id, schema_id, base_schema))) {
      LOG_WARN("fail to put to local cache", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else {
      schema = static_cast<const T*>(base_schema);
    }
  }
  return ret;
}

template<typename T>
int ObLatestSchemaGuard::get_from_local_cache_(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == schema_id
      || OB_INVALID_TENANT_ID == tenant_id
      || !is_normal_schema(schema_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
  } else {
    const ObSchema *tmp_schema = NULL;
    bool found = false;
    FOREACH_CNT_X(id_schema, schema_objs_, !found) {
      if (id_schema->schema_type_ == schema_type
          && id_schema->tenant_id_ == tenant_id
          && id_schema->schema_id_ == schema_id) {
        tmp_schema = id_schema->schema_;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("local cache miss [id to schema]", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp schema is NULL", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
    } else {
      schema = static_cast<const T *>(tmp_schema);
      LOG_TRACE("schema cache hit", K(schema_type), K(tenant_id), K(schema_id));
    }
  }
  return ret;
}

template<typename T>
int ObLatestSchemaGuard::put_to_local_cache_(
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const T *&schema)
{
  int ret = OB_SUCCESS;
  SchemaObj schema_obj;
  schema_obj.schema_type_ = schema_type;
  schema_obj.tenant_id_ = tenant_id;
  schema_obj.schema_id_ = schema_id;
  schema_obj.schema_ = const_cast<ObSchema*>(schema);
  if (OB_FAIL(schema_objs_.push_back(schema_obj))) {
    LOG_WARN("add schema object failed", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_tablegroup_id(
    const common::ObString &tablegroup_name,
    uint64_t &tablegroup_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  tablegroup_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(tablegroup_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_name is empty", KR(ret), K_(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(schema_service_impl->get_tablegroup_id(
             *sql_proxy, tenant_id_, tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", KR(ret), K_(tenant_id), K(tablegroup_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)) {
    LOG_INFO("tablegroup not exist", KR(ret), K_(tenant_id), K(tablegroup_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_database_id(
    const common::ObString &database_name,
    uint64_t &database_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  database_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_name is empty", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_FAIL(schema_service_impl->get_database_id(
             *sql_proxy, tenant_id_, database_name, database_id))) {
    LOG_WARN("fail to get database id", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
    LOG_INFO("database not exist", KR(ret), K_(tenant_id), K(database_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_table_id(
    const uint64_t database_id,
    const uint64_t session_id,
    const ObString &table_name,
    uint64_t &table_id,
    ObTableType &table_type,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  table_id = OB_INVALID_ID;
  table_type = ObTableType::MAX_TABLE_TYPE;
  schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/table_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(schema_service_impl->get_table_id(
             *sql_proxy, tenant_id_, database_id, session_id,
             table_name, table_id, table_type, schema_version))) {
    LOG_WARN("fail to get database id", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_INFO("table not exist", KR(ret), K_(tenant_id),  K(database_id), K(session_id), K(table_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_mock_fk_parent_table_id(
    const uint64_t database_id,
    const ObString &table_name,
    uint64_t &mock_fk_parent_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/table_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(schema_service_impl->get_mock_fk_parent_table_id(
             *sql_proxy, tenant_id_, database_id, table_name, mock_fk_parent_table_id))) {
    LOG_WARN("fail to get mock parent table id", KR(ret), K_(tenant_id), K(database_id), K(table_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == mock_fk_parent_table_id)) {
    LOG_INFO("mock parent table not exist", KR(ret), K_(tenant_id), K(database_id), K(table_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_synonym_id(
    const uint64_t database_id,
    const ObString &synonym_name,
    uint64_t &synonym_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  synonym_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || synonym_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/synonym_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_FAIL(schema_service_impl->get_synonym_id(
             *sql_proxy, tenant_id_, database_id, synonym_name, synonym_id))) {
    LOG_WARN("fail to get synonym id", KR(ret), K_(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == synonym_id)) {
    LOG_INFO("synonym not exist", KR(ret), K_(tenant_id), K(database_id), K(synonym_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_constraint_id(
    const uint64_t database_id,
    const ObString &constraint_name,
    uint64_t &constraint_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  constraint_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || constraint_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/constraint_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(schema_service_impl->get_constraint_id(
             *sql_proxy, tenant_id_, database_id, constraint_name, constraint_id))) {
    LOG_WARN("fail to get constraint id", KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == constraint_id)) {
    LOG_INFO("constraint not exist", KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_foreign_key_id(
    const uint64_t database_id,
    const ObString &foreign_key_name,
    uint64_t &foreign_key_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  foreign_key_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || foreign_key_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/foreign_key_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(schema_service_impl->get_foreign_key_id(
             *sql_proxy, tenant_id_, database_id, foreign_key_name, foreign_key_id))) {
    LOG_WARN("fail to get foreign_key id", KR(ret), K_(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == foreign_key_id)) {
    LOG_INFO("foreign_key not exist", KR(ret), K_(tenant_id), K(database_id), K(foreign_key_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_sequence_id(
    const uint64_t database_id,
    const ObString &sequence_name,
    uint64_t &sequence_id,
    bool &is_system_generated)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  sequence_id = OB_INVALID_ID;
  is_system_generated = false;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || sequence_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/sequence_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(schema_service_impl->get_sequence_id(
             *sql_proxy, tenant_id_, database_id,
             sequence_name, sequence_id, is_system_generated))) {
    LOG_WARN("fail to get sequence id", KR(ret), K_(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == sequence_id)) {
    LOG_INFO("sequence not exist", KR(ret), K_(tenant_id), K(database_id), K(sequence_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_package_id(
    const uint64_t database_id,
    const ObString &package_name,
    const ObPackageType package_type,
    const int64_t compatible_mode,
    uint64_t &package_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  package_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || package_name.empty()
             || INVALID_PACKAGE_TYPE == package_type
             || compatible_mode < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/package_name/package_type/compatible_mode is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(package_name),
             K(package_type), K(compatible_mode));
  } else if (OB_FAIL(schema_service_impl->get_package_id(
             *sql_proxy, tenant_id_, database_id, package_name,
             package_type, compatible_mode, package_id))) {
    LOG_WARN("fail to get package id", KR(ret), K_(tenant_id),
             K(database_id), K(package_name), K(compatible_mode));
  } else if (OB_UNLIKELY(OB_INVALID_ID == package_id)) {
    LOG_INFO("package not exist", KR(ret), K_(tenant_id), K(database_id),
             K(package_name), K(package_type), K(compatible_mode));
  }

  return ret;
}

int ObLatestSchemaGuard::get_routine_id(
    const uint64_t database_id,
    const uint64_t package_id,
    const uint64_t overload,
    const ObString &routine_name,
    common::ObIArray<std::pair<uint64_t, share::schema::ObRoutineType>> &routine_pairs)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  routine_pairs.reset();
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || routine_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/routine_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(routine_name));
  } else if (OB_FAIL(schema_service_impl->get_routine_id(
             *sql_proxy, tenant_id_, database_id, package_id,
             overload, routine_name, routine_pairs))) {
    LOG_WARN("fail to get routine id", KR(ret), K_(tenant_id),
             K(database_id), K(package_id), K(overload), K(routine_name));
  } else if (OB_UNLIKELY(routine_pairs.empty())) {
    LOG_INFO("routine not exist", KR(ret), K_(tenant_id), K(database_id),
             K(package_id), K(routine_name), K(overload), K(routine_name));
  }
  return ret;
}

int ObLatestSchemaGuard::check_udt_exist(
    const uint64_t database_id,
    const uint64_t package_id,
    const ObUDTTypeCode type_code,
    const ObString &udt_name,
    bool &exist)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  exist = false;
  uint64_t udt_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || UDT_INVALID_TYPE_CODE == type_code
             || udt_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/type_code/udt_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(type_code), K(udt_name));
  } else if (OB_FAIL(schema_service_impl->get_udt_id(
             *sql_proxy, tenant_id_, database_id, package_id, udt_name, udt_id))) {
    LOG_WARN("fail to get udt id", KR(ret), K_(tenant_id), K(database_id),
             K(package_id), K(udt_name));
  } else if (OB_UNLIKELY(OB_INVALID_ID == udt_id)) {
    exist = false;
  } else if (UDT_TYPE_OBJECT_BODY != type_code) {
    exist = true;
  } else {
    const ObUDTTypeInfo *udt_info = NULL;
    if (OB_FAIL(get_udt_info(udt_id, udt_info))) {
      LOG_WARN("fail to get udt", KR(ret), K_(tenant_id), K(udt_id));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt not exist", KR(ret), K_(tenant_id), K(udt_id));
    } else if (2 == udt_info->get_object_type_infos().count()) {
      if (udt_info->is_object_type_legal()) {
         exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("illegal object type which has object body",
                 KR(ret), KPC(udt_info), K(udt_name), K(type_code));
      }
    } else {
      exist = false;
    }
  }
  if (OB_SUCC(ret) && !exist) {
    LOG_INFO("udt not exist", KR(ret), K_(tenant_id),
             K(package_id), K(type_code), K(udt_name));
  }
  return ret;
}

int ObLatestSchemaGuard::get_table_schema_versions(
    const common::ObIArray<uint64_t> &table_ids,
    common::ObIArray<ObSchemaIdVersion> &versions)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(table_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ids is empty", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_schema_versions(
             *sql_proxy, tenant_id_, table_ids, versions))) {
    LOG_WARN("fail to get table schema versions", KR(ret), K_(tenant_id), K(table_ids));
  }
  return ret;
}

int ObLatestSchemaGuard::get_mock_fk_parent_table_schema_versions(
    const common::ObIArray<uint64_t> &table_ids,
    common::ObIArray<ObSchemaIdVersion> &versions)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(table_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ids is empty", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_mock_fk_parent_table_schema_versions(
             *sql_proxy, tenant_id_, table_ids, versions))) {
    LOG_WARN("fail to get mock fk parent table schema versions", KR(ret), K_(tenant_id), K(table_ids));
  }
  return ret;
}

int ObLatestSchemaGuard::get_default_audit_schemas(
    common::ObIArray<ObSAuditSchema> &audit_schemas)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObMySQLProxy *sql_proxy = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_proxy))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_audits_in_owner(
             *sql_proxy, tenant_id_, AUDIT_OBJ_DEFAULT, OB_AUDIT_MOCK_USER_ID,
             audit_schemas))) {
    LOG_WARN("fail to get audits in owner", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLatestSchemaGuard::check_oracle_object_exist(
    const uint64_t database_id,
    const uint64_t session_id,
    const ObString &object_name,
    const ObSchemaType &schema_type,
    const ObRoutineType &routine_type,
    const bool is_or_replace)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(
      OB_INVALID_TENANT_ID == tenant_id_
      || OB_INVALID_ID == database_id
      || object_name.empty()
      || OB_MAX_SCHEMA == schema_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K_(tenant_id),
             K(database_id), K(object_name), K(schema_type));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(tenant_id));
  } else if (!is_oracle_mode) {
    // skip
  } else {
    // table
    if (OB_SUCC(ret)) {
      uint64_t table_id = OB_INVALID_ID;
      ObTableType table_type = MAX_TABLE_TYPE;
      int64_t schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(get_table_id(database_id, session_id, object_name,
                               table_id, table_type, schema_version))) {
        LOG_WARN("fail to get table id", KR(ret), K(database_id), K(session_id), K(object_name));
      } else if (OB_INVALID_ID != table_id) {
        if (TABLE_SCHEMA == schema_type && is_view_table(table_type) && is_or_replace) {
          // create or replace view
        } else {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by table in oracle mode", KR(ret), K(table_id));
        }
      }
    }
    // sequence
    if (OB_SUCC(ret)) {
      uint64_t sequence_id = OB_INVALID_ID;
      bool is_system_generated = false;
      if (OB_FAIL(get_sequence_id(database_id, object_name, sequence_id, is_system_generated))) {
        LOG_WARN("fail to get sequence id", KR(ret), K(database_id), K(object_name));
      } else if (OB_INVALID_ID != sequence_id) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by sequence in oracle mode", KR(ret), K(sequence_id));
      }
    }
    // synonym
    if (OB_SUCC(ret)) {
      uint64_t synonym_id = OB_INVALID_ID;
      if (OB_FAIL(get_synonym_id(database_id, object_name, synonym_id))) {
        LOG_WARN("fail to get synonym id", KR(ret), K(database_id), K(object_name));
      } else if (OB_INVALID_ID != synonym_id) {
        if (SYNONYM_SCHEMA == schema_type && is_or_replace) {
          // create or replace synonym
        } else {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by synonym in oracle mode", KR(ret), K(synonym_id));
        }
      }
    }
    // package
    if (OB_SUCC(ret)) {
      uint64_t package_id = OB_INVALID_ID;
      if (OB_FAIL(get_package_id(database_id, object_name,
          PACKAGE_TYPE, COMPATIBLE_ORACLE_MODE, package_id))) {
        LOG_WARN("fail to get package id", KR(ret), K(database_id), K(object_name));
      } else if (OB_INVALID_ID != package_id) {
        if (PACKAGE_SCHEMA == schema_type && is_or_replace) {
          // create or replace package
        } else {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by package in oracle mode", KR(ret), K(package_id));
        }
      }
    }
    // standalone procedure/function
    if (OB_SUCC(ret)) {
      ObArray<std::pair<uint64_t, ObRoutineType>> routine_pairs;
      const uint64_t package_id = OB_INVALID_ID;
      const uint64_t overload = 0;
      if (OB_FAIL(get_routine_id(database_id, package_id, overload, object_name, routine_pairs))) {
        LOG_WARN("fail to get routine id", KR(ret), K(database_id), K(package_id), K(overload), K(object_name));
      } else if (!routine_pairs.empty()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < routine_pairs.count(); i++) {
          std::pair<uint64_t, ObRoutineType> &routine_pair = routine_pairs.at(i);
          if (ROUTINE_SCHEMA == schema_type && is_or_replace) {
            if (ROUTINE_PROCEDURE_TYPE == routine_type
                && ROUTINE_PROCEDURE_TYPE == routine_pair.second) {
              // create or replace standalone procedure
            } else if (ROUTINE_PROCEDURE_TYPE != routine_type
                       && ROUTINE_FUNCTION_TYPE == routine_pair.second) {
              // create or replace standalone function
            } else {
              ret = OB_ERR_EXIST_OBJECT;
            }
          } else {
            ret = OB_ERR_EXIST_OBJECT;
          }
          if (OB_ERR_EXIST_OBJECT == ret) {
            LOG_WARN("Name is already used by routine in oracle mode", KR(ret),
                     "routine_id", routine_pair.first, "routine_type", routine_pair.second);
          }
        } // end for
      }
    }
    // udt
    if (OB_SUCC(ret)) {
      const uint64_t package_id = OB_INVALID_ID;
      const ObUDTTypeCode type_code = ObUDTTypeCode::UDT_TYPE_OBJECT;
      bool exist = false;
      if (OB_FAIL(check_udt_exist(database_id, package_id, type_code, object_name, exist))) {
        LOG_WARN("fail to check udt exist", KR(ret), K(database_id), K(package_id), K(type_code));
      } else if (exist) {
        if (UDT_SCHEMA == schema_type && is_or_replace) {
          // create or replace udt
        } else {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by udt in oracle mode", KR(ret), K(object_name));
        }
      }
    }
  }
  return ret;
}

int ObLatestSchemaGuard::get_table_schema(
    const uint64_t table_id,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_schema_(TABLE_SCHEMA,
             tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get table table", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    LOG_INFO("table not exist", KR(ret), K_(tenant_id), K(table_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_mock_fk_parent_table_schema(
    const uint64_t mock_fk_parent_table_id,
    const ObMockFKParentTableSchema *&mock_fk_parent_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_schema_(MOCK_FK_PARENT_TABLE_SCHEMA,
             tenant_id_, mock_fk_parent_table_id, mock_fk_parent_table_schema))) {
    LOG_WARN("fail to get mock fk parent table", KR(ret), K_(tenant_id), K(mock_fk_parent_table_id));
  } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
    LOG_INFO("mock fk parent table not exist", KR(ret), K_(tenant_id), K(mock_fk_parent_table_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_tablegroup_schema(
    const uint64_t tablegroup_id,
    const ObTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  tablegroup_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", KR(ret), K_(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(get_schema_(TABLEGROUP_SCHEMA,
             tenant_id_, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup", KR(ret), K_(tenant_id), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    LOG_INFO("tablegroup not exist", KR(ret), K_(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_database_schema(
    const uint64_t database_id,
    const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id is invalid", KR(ret), K_(tenant_id), K(database_id));
  } else if (OB_FAIL(get_schema_(DATABASE_SCHEMA,
             tenant_id_, database_id, database_schema))) {
    LOG_WARN("fail to get database", KR(ret), K_(tenant_id), K(database_id));
  } else if (OB_ISNULL(database_schema)) {
    LOG_INFO("database not exist", KR(ret), K_(tenant_id), K(database_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_tenant_schema(
    const uint64_t tenant_id,
    const ObTenantSchema *&tenant_schema)
{
  int ret = OB_SUCCESS;
  tenant_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K_(tenant_id), K(tenant_id));
  } else if (OB_FAIL(get_schema_(TENANT_SCHEMA,
             OB_SYS_TENANT_ID, tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant", KR(ret), K_(tenant_id), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    LOG_INFO("tenant not exist", KR(ret), K_(tenant_id), K(tenant_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_tablespace_schema(
    const uint64_t tablespace_id,
    const ObTablespaceSchema *&tablespace_schema)
{
  int ret = OB_SUCCESS;
  tablespace_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tablespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablespace_id is invalid", KR(ret), K_(tenant_id), K(tablespace_id));
  } else if (OB_FAIL(get_schema_(TABLESPACE_SCHEMA,
             tenant_id_, tablespace_id, tablespace_schema))) {
    LOG_WARN("fail to get tablespace", KR(ret), K_(tenant_id), K(tablespace_id));
  } else if (OB_ISNULL(tablespace_schema)) {
    LOG_INFO("tablespace not exist", KR(ret), K_(tenant_id), K(tablespace_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_udt_info(
    const uint64_t udt_id,
    const ObUDTTypeInfo *udt_info)
{
  int ret = OB_SUCCESS;
  udt_info = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == udt_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("udt_id is invalid", KR(ret), K_(tenant_id), K(udt_id));
  } else if (OB_FAIL(get_schema_(UDT_SCHEMA, tenant_id_, udt_id, udt_info))) {
    LOG_WARN("fail to get udt", KR(ret), K_(tenant_id), K(udt_id));
  } else if (OB_ISNULL(udt_info)) {
    LOG_INFO("udt not exist", KR(ret), K_(tenant_id), K(udt_info));
  }
  return ret;
}

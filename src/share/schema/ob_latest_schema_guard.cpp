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
#include "share/schema/ob_schema_utils.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObLatestSchemaGuard::ObLatestSchemaGuard(
  ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id,
  ObISQLClient *sql_client)
  : schema_service_(schema_service),
    tenant_id_(tenant_id),
    local_allocator_("LastestSchGuard"),
    schema_objs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(local_allocator_)),
    sql_client_(sql_client)
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
    ObISQLClient *&sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service impl is null", KR(ret), K_(tenant_id));
  } else if (OB_NOT_NULL(sql_client_)) {
    sql_client = sql_client_;
  } else if (OB_ISNULL(sql_client = schema_service_->get_sql_proxy())) {
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
  ObISQLClient *sql_client = NULL;
  tablegroup_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(tablegroup_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_name is empty", KR(ret), K_(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(schema_service_impl->get_tablegroup_id(
             *sql_client, tenant_id_, tablegroup_name, tablegroup_id))) {
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
  ObISQLClient *sql_client = NULL;
  database_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_name is empty", KR(ret), K_(tenant_id), K(database_name));
  } else if (OB_FAIL(schema_service_impl->get_database_id(
             *sql_client, tenant_id_, database_name, database_id))) {
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
  ObISQLClient *sql_client = NULL;
  table_id = OB_INVALID_ID;
  table_type = ObTableType::MAX_TABLE_TYPE;
  schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/table_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(schema_service_impl->get_table_id(
             *sql_client, tenant_id_, database_id, session_id,
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
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/table_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(table_name));
  } else if (OB_FAIL(schema_service_impl->get_mock_fk_parent_table_id(
             *sql_client, tenant_id_, database_id, table_name, mock_fk_parent_table_id))) {
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
  ObISQLClient *sql_client = NULL;
  synonym_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || synonym_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/synonym_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(synonym_name));
  } else if (OB_FAIL(schema_service_impl->get_synonym_id(
             *sql_client, tenant_id_, database_id, synonym_name, synonym_id))) {
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
  ObISQLClient *sql_client = NULL;
  constraint_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || constraint_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/constraint_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
  } else if (OB_FAIL(schema_service_impl->get_constraint_id(
             *sql_client, tenant_id_, database_id, constraint_name, constraint_id))) {
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
  ObISQLClient *sql_client = NULL;
  foreign_key_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || foreign_key_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/foreign_key_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(foreign_key_name));
  } else if (OB_FAIL(schema_service_impl->get_foreign_key_id(
             *sql_client, tenant_id_, database_id, foreign_key_name, foreign_key_id))) {
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
  ObISQLClient *sql_client = NULL;
  sequence_id = OB_INVALID_ID;
  is_system_generated = false;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || sequence_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/sequence_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(sequence_name));
  } else if (OB_FAIL(schema_service_impl->get_sequence_id(
             *sql_client, tenant_id_, database_id,
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
  ObISQLClient *sql_client = NULL;
  package_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
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
             *sql_client, tenant_id_, database_id, package_name,
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
  ObISQLClient *sql_client = NULL;
  routine_pairs.reset();
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || routine_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/routine_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(routine_name));
  } else if (OB_FAIL(schema_service_impl->get_routine_id(
             *sql_client, tenant_id_, database_id, package_id,
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
  ObISQLClient *sql_client = NULL;
  exist = false;
  uint64_t udt_id = OB_INVALID_ID;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
             || UDT_INVALID_TYPE_CODE == type_code
             || udt_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_id/type_code/udt_name is invalid",
             KR(ret), K_(tenant_id), K(database_id), K(type_code), K(udt_name));
  } else if (OB_FAIL(schema_service_impl->get_udt_id(
             *sql_client, tenant_id_, database_id, package_id, udt_name, udt_id))) {
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


int ObLatestSchemaGuard::get_default_audit_schemas(
    common::ObIArray<ObSAuditSchema> &audit_schemas)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_audits_in_owner(
             *sql_client, tenant_id_, AUDIT_OBJ_DEFAULT, OB_AUDIT_MOCK_USER_ID,
             audit_schemas))) {
    LOG_WARN("fail to get audits in owner", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_audit_schemas_in_owner(
    const oceanbase::share::schema::ObSAuditType audit_type,
    const uint64_t object_id,
    common::ObIArray<ObSAuditSchema> &audit_schemas)
{
  int ret = OB_SUCCESS;
  audit_schemas.reset();
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_audits_in_owner(
             *sql_client, tenant_id_, audit_type, object_id,
             audit_schemas))) {
    LOG_WARN("fail to get audits in owner", KR(ret), K_(tenant_id), K(audit_type), K(object_id));
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

int ObLatestSchemaGuard::get_tablegroup_schema_(
    ObISQLClient &sql_client,
    const uint64_t tablegroup_id,
    const ObTablegroupSchema *&tablegroup_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", KR(ret), K(tablegroup_id));
  } else if (OB_FAIL(get_from_local_cache_(TABLEGROUP_SCHEMA, tenant_id_,
      tablegroup_id, tablegroup_schema))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get schema from cache", KR(ret), K_(tenant_id), K(tablegroup_id));
    } else {
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = tenant_id_;
      const int64_t schema_version = INT64_MAX;
      const ObSchema *base_schema = NULL;
      ObTablegroupSchema *tmp_tablegroup_schema = NULL;
      if (OB_FAIL(schema_service_->get_schema_service()->get_tablegroup_schema(schema_status,
          tablegroup_id, schema_version, sql_client, local_allocator_, tmp_tablegroup_schema))) {
        LOG_WARN("fail to get latest schema", KR(ret), K_(tenant_id), K(tablegroup_id));
      } else if (OB_ISNULL(tmp_tablegroup_schema)) {
        // schema not exist
      } else if (FALSE_IT(tablegroup_schema = tmp_tablegroup_schema))  {
      } else if (FALSE_IT(base_schema = tmp_tablegroup_schema))  {
      } else if (OB_FAIL(put_to_local_cache_(TABLEGROUP_SCHEMA, tenant_id_,
          tablegroup_id, base_schema))) {
        LOG_WARN("fail to put to local cache", KR(ret), K_(tenant_id), K(tablegroup_id));
      }
    }
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
  } else if (OB_NOT_NULL(sql_client_)) {
    // 'sql_client_ not null' means a transcation (ddl transaction is child class of ObISQLClient) is passed in
    // and we should use this sql_client to get visible tablegroup schema in current transaction
    if (OB_FAIL(get_tablegroup_schema_(*sql_client_, tablegroup_id, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup", KR(ret), K(tablegroup_id));
    }
  } else if (OB_FAIL(get_schema_(TABLEGROUP_SCHEMA,
             tenant_id_, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup", KR(ret), K_(tenant_id), K(tablegroup_id));
  }

  if (OB_SUCC(ret) && OB_ISNULL(tablegroup_schema)) {
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
    const ObUDTTypeInfo *&udt_info)
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

int ObLatestSchemaGuard::get_coded_index_name_info_mysql(
    common::ObIAllocator &allocator,
    const uint64_t database_id,
    const uint64_t data_table_id,
    const ObString &index_name,
    const bool is_built_in,
    ObIndexSchemaInfo &index_info)
{
  int ret = OB_SUCCESS;
  ObISQLClient *sql_client = NULL;
  ObSchemaService *schema_service_impl = nullptr;
  bool is_oracle_mode = false;
  ObArray<ObIndexSchemaInfo> index_infos;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == database_id
                  || OB_INVALID_ID == data_table_id
                  || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should use in mysql mode", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_index_infos(allocator, *sql_client,
      tenant_id_, database_id, data_table_id, index_infos))) {
    LOG_WARN("fail to get table index name in mysql", KR(ret), K_(tenant_id),
      K(data_table_id), K(data_table_id));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i)
  {
    if (schema_service_impl->schema_name_is_equal(index_name,
                                                  index_infos.at(i).get_index_name(),
                                                  true/*case_compare*/,
                                                  true/*collation*/)) {
      if (is_built_in == schema::is_built_in_index(index_infos.at(i).get_index_type())) {
        if (OB_FAIL(index_info.assign(index_infos.at(i)))) {
          LOG_WARN("fail to assign index info", KR(ret));
        }
        break;
      }
    }
  }
  return ret;
}

#ifndef GET_OBJ_SCHEMA_VERSIONS
#define GET_OBJ_SCHEMA_VERSIONS(OBJECT_NAME) \
  int ObLatestSchemaGuard::get_##OBJECT_NAME##_schema_versions(const common::ObIArray<uint64_t> &obj_ids, \
                                                               common::ObIArray<ObSchemaIdVersion> &versions) \
    { \
      int ret = OB_SUCCESS; \
      ObISQLClient *sql_client = nullptr; \
      ObSchemaService *schema_service_impl = nullptr; \
      if (OB_FAIL(check_inner_stat_())) { \
        LOG_WARN("fail to check inner stat", KR(ret)); \
      } else if (OB_UNLIKELY(obj_ids.count() <= 0)) { \
        ret = OB_INVALID_ARGUMENT; \
        LOG_WARN("obj_ids is empty", KR(ret)); \
      } else if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) { \
        LOG_WARN("fail to check and get service", KR(ret)); \
      } else if (OB_FAIL(schema_service_impl->get_##OBJECT_NAME##_schema_versions(*sql_client, tenant_id_, obj_ids, versions))) { \
        LOG_WARN("fail to get obj schema versions", KR(ret), K_(tenant_id), K(obj_ids)); \
      } \
      return ret; \
    }

  GET_OBJ_SCHEMA_VERSIONS(table);
  GET_OBJ_SCHEMA_VERSIONS(mock_fk_parent_table);
  GET_OBJ_SCHEMA_VERSIONS(routine);
  GET_OBJ_SCHEMA_VERSIONS(synonym);
  GET_OBJ_SCHEMA_VERSIONS(package);
  GET_OBJ_SCHEMA_VERSIONS(type);
  GET_OBJ_SCHEMA_VERSIONS(sequence);
#undef GET_OBJ_SCHEMA_VERSIONS
#endif
int ObLatestSchemaGuard::get_obj_privs(const uint64_t obj_id,
                                       const ObObjectType obj_type,
                                       common::ObIArray<ObObjPriv> &obj_privs)
{
  int ret = OB_SUCCESS;
  ObISQLClient *sql_client = NULL;
  ObSchemaService *schema_service_impl = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == obj_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(obj_id));
  } else if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_obj_priv_with_obj_id(*sql_client, tenant_id_,
             obj_id, static_cast<uint64_t>(obj_type), obj_privs))) {
    LOG_WARN("fail to get obj priv", KR(ret), K_(tenant_id), K(obj_id));
  }
  return ret;
}


#ifndef GET_RLS_SCHEMA
#define GET_RLS_SCHEMA(SCHEMA, SCHEMA_TYPE) \
  int ObLatestSchemaGuard::get_##SCHEMA##s(const uint64_t rls_id, \
                                           const SCHEMA_TYPE *&rls_schema) \
  { \
    int ret = OB_SUCCESS; \
    rls_schema = nullptr; \
    ObISQLClient *sql_client = nullptr; \
    ObSchemaService *schema_service_impl = nullptr; \
    if (OB_FAIL(check_inner_stat_())) { \
      LOG_WARN("fail to check inner stat", KR(ret)); \
    } else if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) { \
      LOG_WARN("fail to check and get service", KR(ret)); \
    } else if (OB_UNLIKELY(OB_INVALID_ID == rls_id)) { \
      ret = OB_INVALID_ARGUMENT; \
      LOG_WARN("rls id is invalid", KR(ret), K(rls_id)); \
    } else { \
      ObArray<SchemaKey> rls_keys; \
      SchemaKey rls_key; \
      rls_key.tenant_id_ = tenant_id_; \
      rls_key.SCHEMA##_id_ = rls_id; \
      if (OB_FAIL(rls_keys.push_back(rls_key))) { \
        LOG_WARN("fail to push back rls key", KR(ret), K(rls_key)); \
      } \
      ObRefreshSchemaStatus schema_status; \
      schema_status.tenant_id_ = tenant_id_; \
      const int64_t schema_version = INT64_MAX; \
      common::ObArray<SCHEMA_TYPE> object_schema_array;\
      SCHEMA_TYPE *tmp_object_schema = nullptr;\
      if (FAILEDx(schema_service_impl->get_batch_##SCHEMA##s(schema_status, *sql_client, \
                                                             schema_version, rls_keys, object_schema_array))) { \
        LOG_WARN("fail to get batch rls", KR(ret), K(schema_status), K(rls_keys)); \
      } else if (OB_UNLIKELY(1 != object_schema_array.count())) { \
        if (0 == object_schema_array.count()) { \
          /* the action of not exist is up to user */ \
        } else { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("unexpected schema count", KR(ret), K(object_schema_array)); \
        } \
      } else if (OB_FAIL(ObSchemaUtils::alloc_schema(local_allocator_, \
                                                     object_schema_array.at(0), \
                                                     tmp_object_schema))) { \
        LOG_WARN("fail to alloc new var", KR(ret), K(rls_keys)); \
      } else if (OB_ISNULL(tmp_object_schema)) { \
        ret = OB_ERR_UNEXPECTED; \
        LOG_WARN(#SCHEMA_TYPE "is NULL", KR(ret)); \
      } else { \
        rls_schema = tmp_object_schema; \
      } \
    } \
    return ret; \
  } \

  GET_RLS_SCHEMA(rls_policy, ObRlsPolicySchema);
  GET_RLS_SCHEMA(rls_group, ObRlsGroupSchema);
  GET_RLS_SCHEMA(rls_context, ObRlsContextSchema);
#undef GET_RLS_SCHEMA
#endif

int ObLatestSchemaGuard::get_sensitive_rule_schemas_by_table(
  const ObTableSchema &table_schema,
  ObIArray<ObSensitiveRuleSchema *> &schemas)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = INT64_MAX;
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id_;
  ObSEArray<uint64_t, 8> column_ids;
  ObArray<SchemaKey> sensitive_column_schema_keys;
  ObArray<ObSensitiveColumnSchema> sensitive_column_schema_array;
  ObArray<SchemaKey> sensitive_rule_schema_keys;
  ObArray<ObSensitiveRuleSchema> sensitive_rule_schema_array;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("get column ids failed", KR(ret), K(table_schema));
  }
  // get sensitive columns from table
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    SchemaKey sensitive_column_schema_key;
    sensitive_column_schema_key.tenant_id_ = tenant_id_;
    sensitive_column_schema_key.table_id_ = table_schema.get_table_id();
    sensitive_column_schema_key.col_id_ = column_ids.at(i);
    if (OB_FAIL(sensitive_column_schema_keys.push_back(sensitive_column_schema_key))) {
      LOG_WARN("fail to push back sensitive column schema key", KR(ret), K(sensitive_column_schema_key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (sensitive_column_schema_keys.count() == 0) {
  } else if (FAILEDx(schema_service_impl->get_batch_sensitive_columns(schema_status,
                                                                      *sql_client,
                                                                      schema_version,
                                                                      sensitive_column_schema_keys,
                                                                      sensitive_column_schema_array))) {
    LOG_WARN("fail to get batch sensitive column", KR(ret), K(schema_status), K(sensitive_column_schema_keys));
  }
  // get corresponding sensitive rules of sensitive columns
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_column_schema_array.count(); ++i) {
    SchemaKey sensitive_rule_schema_key;
    sensitive_rule_schema_key.tenant_id_ = tenant_id_;
    sensitive_rule_schema_key.sensitive_rule_id_ = sensitive_column_schema_array.at(i).get_sensitive_rule_id();
    if (OB_FAIL(sensitive_rule_schema_keys.push_back(sensitive_rule_schema_key))) {
      LOG_WARN("fail to push back sensitive rule schema key", KR(ret), K(sensitive_rule_schema_key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (sensitive_rule_schema_keys.count() == 0) { // no sensitive rule, do nothing
  } else if (FAILEDx(schema_service_impl->get_batch_sensitive_rules(schema_status,
                                                                    *sql_client,
                                                                    schema_version,
                                                                    sensitive_rule_schema_keys,
                                                                    sensitive_rule_schema_array))) {
    LOG_WARN("fail to get batch sensitive rule", KR(ret), K(schema_status), K(sensitive_rule_schema_keys));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rule_schema_array.count(); ++i) {
    ObSensitiveRuleSchema *rule_schema = NULL;
    if (OB_FAIL(ObSchemaUtils::alloc_schema(local_allocator_, sensitive_rule_schema_array.at(i), rule_schema))) {
      LOG_WARN("fail to alloc new var", KR(ret));
    } else if (OB_ISNULL(rule_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rule_schema is NULL", KR(ret));
    } else if (OB_FAIL(schemas.push_back(rule_schema))) {
      LOG_WARN("push back schema failed", KR(ret), K(rule_schema));
    }
  }
  return ret;
}

int ObLatestSchemaGuard::get_sequence_schema(const uint64_t sequence_id,
                                             const ObSequenceSchema *&sequence_schema)
{
  int ret = OB_SUCCESS;
  sequence_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == sequence_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sequence_id is invalid", KR(ret), K_(tenant_id), K(sequence_id));
  } else if (OB_FAIL(get_schema_(SEQUENCE_SCHEMA, tenant_id_, sequence_id, sequence_schema))) {
    LOG_WARN("fail to get sequence", KR(ret), K_(tenant_id), K(sequence_id));
  } else if (OB_ISNULL(sequence_schema)) {
    LOG_INFO("sequence not exist", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_trigger_info(const uint64_t trigger_id,
                                          const ObTriggerInfo *&trigger_info)
{
  int ret = OB_SUCCESS;
  trigger_info = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == trigger_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trigger_id is invalid", KR(ret), K_(tenant_id), K(trigger_id));
  } else if (OB_FAIL(get_schema_(TRIGGER_SCHEMA, tenant_id_, trigger_id, trigger_info))) {
    LOG_WARN("fail to get trigger", KR(ret), K_(tenant_id), K(trigger_id));
  } else if (OB_ISNULL(trigger_info)) {
    LOG_INFO("trigger not exist", KR(ret), K_(tenant_id), K(trigger_info));
  }
  return ret;
}

int ObLatestSchemaGuard::get_table_schemas_in_tablegroup(
    const uint64_t tablegroup_id,
    ObIArray<const ObTableSchema *> &table_schemas)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_schemas_in_tablegroup(local_allocator_,
      *sql_client, tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("failed to get table schemas in tablegroup", KR(ret), K_(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObLatestSchemaGuard::check_database_exists_in_tablegroup(
    const uint64_t tablegroup_id,
    bool &exists)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->check_database_exists_in_tablegroup(*sql_client,
      tenant_id_, tablegroup_id, exists))) {
    LOG_WARN("failed to check database exists in tablegroup", KR(ret), K_(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_table_id_and_table_name_in_tablegroup(
    const uint64_t tablegroup_id,
    ObIArray<ObString> &table_names,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = NULL;
  ObISQLClient *sql_client = NULL;
  if (OB_FAIL(check_and_get_service_(schema_service_impl, sql_client))) {
    LOG_WARN("fail to check and get service", KR(ret));
  } else if (OB_FAIL(schema_service_impl->get_table_id_and_table_name_in_tablegroup(local_allocator_, *sql_client,
      tenant_id_, tablegroup_id, table_names, table_ids))) {
    LOG_WARN("fail to get table names and ids in tablegroup", KR(ret), K_(tenant_id), K(tablegroup_id));
  }
  return ret;
}

int ObLatestSchemaGuard::get_sys_variable_schema(const ObSysVariableSchema *&sys_variable_schema)
{
  int ret = OB_SUCCESS;
  sys_variable_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_schema_(SYS_VARIABLE_SCHEMA, tenant_id_, tenant_id_/*schema_id*/, sys_variable_schema))) {
    LOG_WARN("fail to get tenant system variable", KR(ret), K_(tenant_id), KPC(sys_variable_schema));
  } else if (OB_ISNULL(sys_variable_schema)) {
    LOG_INFO("sys_variable_schema is null", KR(ret), K_(tenant_id));
  }
  return ret;
}

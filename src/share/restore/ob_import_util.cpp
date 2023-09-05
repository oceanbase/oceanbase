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
#define USING_LOG_PREFIX SHARE
#include "ob_import_util.h"

using namespace oceanbase;
using namespace share;

bool ObImportTableUtil::can_retrieable_err(const int err_code)
{
  bool bret = true;
  switch(err_code) {
    case OB_NOT_INIT :
    case OB_INIT_TWICE:
    case OB_INVALID_ARGUMENT :
    case OB_ERR_UNEXPECTED :
    case OB_ERR_SYS :
    case OB_CANCELED :
    case OB_NOT_SUPPORTED :
    case OB_TENANT_HAS_BEEN_DROPPED :
    case OB_TENANT_NOT_EXIST:
    case OB_ERR_INVALID_TENANT_NAME:
    case OB_HASH_EXIST:
    case OB_HASH_NOT_EXIST:
    case OB_ENTRY_EXIST:
    case OB_ENTRY_NOT_EXIST:
    case OB_VERSION_NOT_MATCH:
    case OB_STATE_NOT_MATCH:
    case OB_TABLE_NOT_EXIST:
    case OB_ERR_BAD_DATABASE:
    case OB_LS_RESTORE_FAILED:
    case OB_OP_NOT_ALLOW:
    case OB_TABLEGROUP_NOT_EXIST:
    case OB_TABLESPACE_NOT_EXIST:
    case OB_ERR_TABLE_EXIST:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}

int ObImportTableUtil::get_tenant_schema_guard(share::schema::ObMultiVersionSchemaService &schema_service, uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  schema::ObTenantStatus status;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("faield to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tenant_status(tenant_id, status))) {
    LOG_WARN("failed to get tenant status", K(ret));
  } else if (schema::ObTenantStatus::TENANT_STATUS_NORMAL != status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant status is not normal", K(ret), K(tenant_id));
  }
  return ret;
}

int ObImportTableUtil::check_database_schema_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &db_name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("faield to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.check_database_exist(tenant_id, db_name, is_exist))) {
    LOG_WARN("failed to check database exist", K(ret), K(tenant_id), K(db_name));
  }
  return ret;
}

int ObImportTableUtil::check_table_schema_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &db_name, const ObString &table_name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  uint64_t database_id = OB_INVALID_ID;
  is_exist = false;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("faield to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_database_id(tenant_id, db_name, database_id))) {
    LOG_WARN("failed to get database id", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == database_id) {
    LOG_WARN("database not exist", K(tenant_id), K(db_name));
  } else if (OB_FAIL(guard.check_table_exist(tenant_id, database_id, table_name, false,
      schema::ObSchemaGetterGuard::CheckTableType::ALL_NON_HIDDEN_TYPES, is_exist))) {
    LOG_WARN("failed to check table exist", K(ret));
  }
  return ret;
}

int ObImportTableUtil::check_tablegroup_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &tablegroup, bool &is_exist)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  uint64_t table_group_id = OB_INVALID_ID;
  is_exist = false;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("faield to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tablegroup_id(tenant_id, tablegroup, table_group_id))) {
    LOG_WARN("failed to get tablegroup id", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == table_group_id) {
    LOG_INFO("tablegroup not exist", K(tenant_id), K(tablegroup));
  } else {
    is_exist = true;
  }
  return ret;
}

int ObImportTableUtil::check_tablespace_exist(share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, const ObString &tablespace, bool &is_exist)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  const schema::ObTablespaceSchema *schema = nullptr;
  is_exist = false;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("faield to get tenant schema guard", K(ret));
  } else if (OB_FAIL(guard.get_tablespace_schema_with_name(tenant_id, tablespace, schema))) {
    LOG_WARN("failed to get tablespace id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema)) {
    LOG_INFO("tablespace not exist", K(tenant_id), K(tablespace));
  } else {
    is_exist = true;
  }
  return ret;
}

int ObImportTableUtil::get_tenant_name_case_mode(const uint64_t tenant_id, ObNameCaseMode &name_case_mode)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard guard;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service must not be null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_name_case_mode(tenant_id, name_case_mode))) {
    LOG_WARN("faield to get tenant schema guard", K(ret), K(tenant_id));
  }
  return ret;
}
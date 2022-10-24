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

namespace oceanbase
{
namespace share
{
namespace schema
{
inline int MockObMultiVersionSchemaService::init()
{
  int ret = common::OB_SUCCESS;
  common::ObKVGlobalCache::get_instance().init();
  if (OB_FAIL(init_maps())) {
     _OB_LOG(WARN, "fail to init maps, ret[%d]", ret);
  } else if (OB_FAIL(schema_cache_.init())) {
    _OB_LOG(WARN, "schema_cache_ init fail, ret[%d]", ret);
  } else if (OB_FAIL(init_sys_tenant_user_schema())) {
    _OB_LOG(WARN, "init sys tenant user schema fail, ret[%d]", ret);
  } else {
    //increment_basepoint_set_ = true;
  }
  return ret;
}

inline int MockObMultiVersionSchemaService::get_schema_guard(ObSchemaGetterGuard &guard,
    int64_t snapshot_version) {
  int ret = common::OB_SUCCESS;
  guard.reset();
  guard.schema_service_ = this;

  if (common::OB_INVALID_VERSION == snapshot_version) {
    //int64_t latest_local_version = get_latest_local_version();
    //if (ObSchemaService::is_core_temp_version(latest_local_version)) {
    //  guard.snapshot_version_ = latest_local_version + 2;
    //} else if (ObSchemaService::is_sys_temp_version(latest_local_version)) {
    //  guard.snapshot_version_ = latest_local_version + 1;
    //} else {
    //  guard.snapshot_version_ = latest_local_version;
    //}
    guard.snapshot_version_ = INT64_MAX;
  } else if (INT64_MAX == snapshot_version) {
    guard.snapshot_version_ = snapshot_version;
  } else {
    guard.snapshot_version_ = snapshot_version;
  }
  //if (OB_FAIL(ret)) {
  //} else if (OB_FAIL(guard.id_schema_map_.destroy())) {
  //  _OB_LOG(WARN, "destroy failed, ret %d", ret);
  //} else if (OB_FAIL(guard.id_schema_map_.create(ObSchemaGetterGuard::OB_SCHEMA_NAME_MAP_BUCKET_NUM_GUARD,
  //    common::ObModIds::OB_SCHEMA_TENANT_NAME_SCHEMA_MAP))) {
  //  _OB_LOG(WARN, "create id_schema_map failed,ret %d", ret);
  //}
  return ret;
}
inline int MockObMultiVersionSchemaService::add_table_schema(ObTableSchema &table_schema, int64_t schema_version)
{
  int ret = common::OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable = NULL;
  if (OB_FAIL(get_schema_guard(schema_guard, INT64_MAX))) {
    _OB_LOG(WARN, "get schema guard fail, ret %d", ret);
  } else if (OB_FAIL(schema_guard.get_tenant_info(common::OB_SYS_TENANT_ID, tenant_schema))) {
    OB_LOG(WARN, "get tenant info failed", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = common::OB_TENANT_NOT_EXIST;
    _OB_LOG(WARN, "tenant schema is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(common::OB_SYS_TENANT_ID, sys_variable))) {
    OB_LOG(WARN, "get tenant info failed", K(ret));
  } else if (OB_ISNULL(sys_variable)) {
    ret = common::OB_TENANT_NOT_EXIST;
    _OB_LOG(WARN, "sys variable schema is null", K(ret));
  } else {
    common::ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
    if (local_mode <= common::OB_NAME_CASE_INVALID || local_mode >= common::OB_NAME_CASE_MAX)   {
      ret = common::OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "invalid tenant mod, ret %d", ret);
    } else {
      common::ObArray<SchemaUpdateInfo> update_infos;
      SchemaUpdateInfo schema_update_info;
      schema_update_info.schema_type_ = TABLE_SCHEMA;
      schema_update_info.update_type_ = SCHEMA_UPDATE_TYPE_ADD;
      schema_update_info.tenant_id_ = table_schema.get_tenant_id();
      schema_update_info.database_id_ = table_schema.get_database_id();
      schema_update_info.table_id_ = table_schema.get_table_id();
      schema_update_info.table_name_ = table_schema.get_table_name();
      schema_update_info.name_case_mode_ = local_mode;
      schema_update_info.schema_version_ = schema_version;
      table_schema.set_schema_version(schema_version);
      table_schema.set_name_case_mode(local_mode);
      if (OB_FAIL(update_infos.push_back(schema_update_info))) {
        _OB_LOG(WARN, "update_infos push back fail, ret %d", ret);
      } else if (OB_FAIL(schema_cache_.put_schema(TABLE_SCHEMA,table_schema.get_table_id(),
          schema_version, table_schema))) {
        _OB_LOG(WARN, "put schema fail, ret %d", ret);
      } else if (OB_FAIL(update_name_maps(update_infos))) {
        _OB_LOG(WARN, "update name maps fail, ret %d", ret);
      } else if (OB_FAIL(get_schema_guard(schema_guard, INT64_MAX))) {
        _OB_LOG(WARN, "get schema guard fail, ret %d", ret);
      }
    }
  }
  return ret;
}
inline int MockObMultiVersionSchemaService::add_database_schema(ObDatabaseSchema &database_schema, int64_t schema_version)
{
  int ret = common::OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable = NULL;
  if (OB_FAIL(get_schema_guard(schema_guard, INT64_MAX))) {
    _OB_LOG(WARN, "get schema guard fail, ret %d", ret);
  } else if (OB_FAIL(schema_guard.get_tenant_info(common::OB_SYS_TENANT_ID, tenant_schema))) {
    OB_LOG(WARN, "get tenant info failed", K(database_schema), K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = common::OB_TENANT_NOT_EXIST;
    _OB_LOG(WARN, "tenant schema is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(common::OB_SYS_TENANT_ID, sys_variable))) {
    OB_LOG(WARN, "get tenant info failed", K(ret));
  } else if (OB_ISNULL(sys_variable)) {
    ret = common::OB_TENANT_NOT_EXIST;
    _OB_LOG(WARN, "sys variable schema is null", K(ret));
  } else {
    common::ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
    if (local_mode <= common::OB_NAME_CASE_INVALID || local_mode >= common::OB_NAME_CASE_MAX)   {
      ret = common::OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "invalid tenant mod, ret %d", ret);
    } else {
      common::ObArray<SchemaUpdateInfo> update_infos;
      SchemaUpdateInfo schema_update_info;
      schema_update_info.schema_type_ = DATABASE_SCHEMA;
      schema_update_info.update_type_ = SCHEMA_UPDATE_TYPE_ADD;
      schema_update_info.tenant_id_ = database_schema.get_tenant_id();
      schema_update_info.database_id_ = database_schema.get_database_id();
      schema_update_info.database_name_ = database_schema.get_database_name();
      schema_update_info.name_case_mode_ = local_mode;
      schema_update_info.schema_version_ = schema_version;
      database_schema.set_schema_version(schema_version);
      database_schema.set_name_case_mode(local_mode);
      if (OB_FAIL(update_infos.push_back(schema_update_info))) {
        _OB_LOG(WARN, "update_infos push back fail, ret %d", ret);
      } else if (OB_FAIL(schema_cache_.put_schema(DATABASE_SCHEMA,database_schema.get_database_id(),
         schema_version, database_schema))) {
        _OB_LOG(WARN, "put schema fail, ret %d", ret);
      } else if (OB_FAIL(update_name_maps(update_infos))) {
        _OB_LOG(WARN, "update name maps fail, ret %d", ret);
      } else if (OB_FAIL(get_schema_guard(schema_guard, INT64_MAX))) {
        _OB_LOG(WARN, "get schema guard fail, ret %d", ret);
      }
    }
  }
  return ret;
}
} //end namespace schema
} //end namespace share
} //end namespace oceanbase

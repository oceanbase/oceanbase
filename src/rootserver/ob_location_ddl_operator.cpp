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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_location_ddl_operator.h"
#include "rootserver/ob_ddl_operator.h"
#include "src/share/schema/ob_priv_sql_service.h"
#include "rootserver/ob_objpriv_mysql_ddl_operator.h"

namespace oceanbase
{
namespace rootserver
{
int ObLocationDDLOperator::create_location(const ObString &ddl_str,
                    const uint64_t user_id,
                    share::schema::ObLocationSchema &schema,
                    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  uint64_t new_location_id = OB_INVALID_ID;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_location_id(tenant_id, new_location_id))) {
    LOG_WARN("failed to fetch new_location_id", K(tenant_id), K(ret));
  } else if (FALSE_IT(schema.set_location_id(new_location_id))) {
    // do nothing
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to gen new_schema_version", K(tenant_id), K(ret));
  } else if (FALSE_IT(schema.set_schema_version(schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_location_sql_service().apply_new_schema(
              schema, trans, ObSchemaOperationType::OB_DDL_CREATE_LOCATION, ddl_str))) {
    LOG_WARN("failed to create location", K(schema.get_location_name()), K(ret));
  } else {
    ObTablePrivSortKey table_priv_key;
    table_priv_key.tenant_id_ = tenant_id;
    table_priv_key.user_id_ = user_id;

    ObPrivSet priv_set;
    priv_set = OB_PRIV_READ | OB_PRIV_WRITE | OB_PRIV_GRANT;

    ObObjPrivSortKey obj_priv_key;
    obj_priv_key.tenant_id_ = tenant_id;
    obj_priv_key.obj_id_ = new_location_id;
    obj_priv_key.obj_type_ = static_cast<uint64_t>(ObObjectType::LOCATION);
    obj_priv_key.col_id_ = OB_COMPACT_COLUMN_INVALID_ID;
    obj_priv_key.grantor_id_ = OB_ORA_SYS_USER_ID;
    obj_priv_key.grantee_id_ = user_id;

    share::ObRawObjPrivArray priv_array;
    priv_array.push_back(OBJ_PRIV_ID_READ);
    priv_array.push_back(OBJ_PRIV_ID_WRITE);

    ObObjMysqlPrivSortKey obj_mysql_priv_key;
    obj_mysql_priv_key.tenant_id_ = tenant_id;
    obj_mysql_priv_key.user_id_ = user_id;
    obj_mysql_priv_key.object_name_ = schema.get_location_name();
    obj_mysql_priv_key.object_type_ = static_cast<uint64_t>(ObObjectType::LOCATION);

    ObDDLOperator ddl_operator(schema_service_, sql_proxy_);
    ObObjPrivMysqlDDLOperator mysql_ddl_operator(schema_service_, sql_proxy_);
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
    } else if (lib::Worker::CompatMode::ORACLE == compat_mode
               && OB_FAIL(ddl_operator.grant_table(table_priv_key, priv_set, NULL, trans, priv_array, 0, obj_priv_key))) {
      LOG_WARN("fail to grant table, oracle mode", K(ret), K(table_priv_key), K(priv_set), K(obj_priv_key));
    } else if (lib::Worker::CompatMode::MYSQL == compat_mode
               && OB_FAIL(mysql_ddl_operator.grant_object(obj_mysql_priv_key, priv_set, trans, 0, false))) {
      LOG_WARN("fail to grant table, mysql mode", K(ret), K(obj_mysql_priv_key), K(priv_set));
    }
  }
  return ret;
}

int ObLocationDDLOperator::alter_location(const ObString &ddl_str,
                                  share::schema::ObLocationSchema &schema,
                                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service should not be null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema.set_schema_version(new_schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_location_sql_service().apply_new_schema(
    schema, trans, ObSchemaOperationType::OB_DDL_ALTER_LOCATION, ddl_str))) {
    LOG_WARN("failed to alter location", K(schema), K(ret));
  }
  return ret;
}

int ObLocationDDLOperator::drop_location(const ObString &ddl_str,
                  share::schema::ObLocationSchema &schema,
                  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t location_id = schema.get_location_id();
  const uint64_t location_type = static_cast<uint64_t>(ObObjectType::LOCATION);
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema service must not null", K(ret));
  } else {
    ObArray<const ObSimpleTableSchemaV2 *> tables;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
      LOG_WARN("get full tables failed", K(tenant_id), KT(location_id), K(ret));
    }
    bool is_stop = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count() && !is_stop; ++i) {
      const ObSimpleTableSchemaV2 *table_schema = tables.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", KR(ret), K(i), K(tables.count()));
      } else if (ObTableType::EXTERNAL_TABLE == table_schema->get_table_type()) {
        const ObTableSchema *full_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_schema->get_table_id(), full_schema))) {
          LOG_WARN("get table schema failed");
        } else if (OB_ISNULL(full_schema)){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_schema is null", KR(ret), K(i));
        } else if (full_schema->get_external_location_id() == location_id) {
          is_stop = true;
        }
      }
    }
    if (is_stop) {
      ret = OB_LOCATION_IN_USE;
    }
  }

  ObDDLOperator ddl_operator(schema_service_, sql_proxy_);
  ObObjPrivMysqlDDLOperator mysql_ddl_operator(schema_service_, sql_proxy_);
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_FAIL(ret)) {
    LOG_WARN("location in use", K(ret));
  } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
  } else if (lib::Worker::CompatMode::ORACLE == compat_mode
             && OB_FAIL(ddl_operator.drop_obj_privs(tenant_id, location_id, location_type,
                                                    trans, schema_service_, schema_guard))) {
    LOG_WARN("failed to drop obj privs for location", K(ret),
              K(tenant_id), K(location_id), K(location_type));
  } else if (lib::Worker::CompatMode::MYSQL == compat_mode
             && OB_FAIL(mysql_ddl_operator.drop_obj_mysql_privs(tenant_id, schema.get_location_name(), location_type,
                                                          trans, schema_service_, schema_guard))) {
    LOG_WARN("failed to drop obj privs for location", K(ret),
              K(tenant_id), K(location_id), K(location_type));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema_version", K(ret), K(tenant_id));
  } else if (FALSE_IT(schema.set_schema_version(new_schema_version))) {
    // do nothing
  } else if (OB_FAIL(schema_service->get_location_sql_service().apply_new_schema(
    schema, trans, ObSchemaOperationType::OB_DDL_DROP_LOCATION, ddl_str))) {
    LOG_WARN("failed to drop location", K(schema), K(ret));
  }
  return ret;
}

}
}
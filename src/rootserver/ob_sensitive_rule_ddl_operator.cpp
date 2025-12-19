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
#include "rootserver/ob_sensitive_rule_ddl_operator.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/schema/ob_sensitive_rule_schema_struct.h"
#include "share/schema/ob_table_sql_service.h"

namespace oceanbase
{
namespace rootserver
{

int ObSensitiveRuleDDLOperator::handle_sensitive_rule_function(ObSensitiveRuleSchema &schema,
                                                               ObMySQLTransaction &trans,
                                                               const share::schema::ObSchemaOperationType ddl_type,
                                                               const ObString &ddl_stmt_str,
                                                               const uint64_t tenant_id,
                                                               ObSchemaGetterGuard &schema_guard,
                                                               uint64_t user_id)
{
  int ret = OB_SUCCESS;
  const ObSensitiveRuleSchema *exist_schema = NULL;
  ObSensitiveRuleSchema new_schema;
  if (OB_FAIL(schema_guard.get_sensitive_rule_schema_by_name(tenant_id,
                                                             schema.get_sensitive_rule_name_str(),
                                                             exist_schema))) {
    LOG_WARN("failed to check if schema exists", K(ret));
  } else if (OB_FAIL(handle_sensitive_rule_function_inner(schema, exist_schema, trans, ddl_type,
                                                          ddl_stmt_str, tenant_id, new_schema))) {
    LOG_WARN("failed to handle sensitive rule function inner", K(ret));
  } else if (OB_FAIL(grant_or_revoke_after_ddl(new_schema, trans, ddl_type, schema_guard, user_id))) {
    LOG_WARN("grant or revoke sensitive rule failed", K(ret));
  } else if (OB_FAIL(update_table_schema(new_schema, trans, schema_guard,tenant_id))) {
    LOG_WARN("fail to update table schema", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSensitiveRuleDDLOperator::handle_sensitive_rule_function_inner(ObSensitiveRuleSchema &schema,
                                                                     const ObSensitiveRuleSchema *exist_schema,
                                                                     ObMySQLTransaction &trans,
                                                                     const share::schema::ObSchemaOperationType ddl_type,
                                                                     const ObString &ddl_stmt_str,
                                                                     const uint64_t tenant_id,
                                                                     ObSensitiveRuleSchema &new_schema)
{
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t sensitive_rule_id = OB_INVALID_ID;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema version", K(ret), K(tenant_id));
  } else {
    switch (ddl_type) {
      case OB_DDL_CREATE_SENSITIVE_RULE:
        if (OB_NOT_NULL(exist_schema)) {
          ret = OB_SENSITIVE_RULE_EXIST;
          LOG_WARN("sensitive rule already exist", K(ret), K(schema));
        } else if (OB_FAIL(schema_sql_service->fetch_new_sensitive_rule_id(tenant_id, sensitive_rule_id))) {
          LOG_WARN("failed to fetch new sensitive rule id", K(ret), K(tenant_id));
        } else if (OB_FAIL(new_schema.assign(schema))) {
          LOG_WARN("failed to assign schema", K(ret));
        } else if (FALSE_IT(new_schema.set_schema_version(new_schema_version))){
        } else {
          new_schema.set_sensitive_rule_id(sensitive_rule_id);
        }
        break;
      case OB_DDL_DROP_SENSITIVE_RULE:
      case OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN:
      case OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN:
      case OB_DDL_ALTER_SENSITIVE_RULE_ENABLE:
      case OB_DDL_ALTER_SENSITIVE_RULE_DISABLE:
      case OB_DDL_ALTER_SENSITIVE_RULE_SET_PROTECTION_SPEC:{
        if (OB_ISNULL(exist_schema)) {
          ret = OB_SENSITIVE_RULE_NOT_EXIST;
          LOG_WARN("sensitive rule not exist", K(ret), K(schema));
        } else if (OB_FAIL(new_schema.assign(*exist_schema))) {
          LOG_WARN("failed to assign schema", K(ret));
        } else if (FALSE_IT(new_schema.set_schema_version(new_schema_version))){
        }
        // set new schema properties according to ddl type
        if (OB_SUCC(ret)) {
          if (OB_DDL_ALTER_SENSITIVE_RULE_ENABLE == ddl_type
              || OB_DDL_ALTER_SENSITIVE_RULE_DISABLE == ddl_type) {
            new_schema.set_enabled(ddl_type == OB_DDL_ALTER_SENSITIVE_RULE_ENABLE);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_DDL_ALTER_SENSITIVE_RULE_SET_PROTECTION_SPEC == ddl_type) {
            new_schema.set_protection_policy(schema.get_protection_policy());
            if (OB_FAIL(new_schema.set_method(schema.get_method_str()))) {
              LOG_WARN("set method failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN == ddl_type
              || OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN == ddl_type) {
            if (OB_FAIL(new_schema.set_sensitive_field_items(schema.get_sensitive_field_items()))) {
              LOG_WARN("set sensitive field items failed", K(ret));
            }
          }
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support ddl type", K(ret), K(ddl_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this sensitive_rule ddl type");
        break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_sql_service->get_sensitive_rule_sql_service().apply_new_schema(
                     new_schema, trans, ddl_type, ddl_stmt_str))) {
    LOG_WARN("apply new sensitive rule schema failed", K(ret), K(ddl_stmt_str));
  }
  return ret;
}

int ObSensitiveRuleDDLOperator::update_table_schema(ObSensitiveRuleSchema &schema,
                                                    ObMySQLTransaction &trans,
                                                    ObSchemaGetterGuard &schema_guard,
                                                    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t last_table_id = OB_INVALID_ID;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_sensitive_field_items().count(); ++i) {
    const ObSensitiveFieldItem &item = schema.get_sensitive_field_items().at(i);
    const ObTableSchema *table_schema = NULL;
    if (item.table_id_ == last_table_id) {  // table already updated
    } else if (FALSE_IT(last_table_id = item.table_id_)) {
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, item.table_id_, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(item.table_id_));
    } else if (NULL == table_schema) {
      // do nothing, the table may be dropped, or is hidden table in offline ddl
      LOG_INFO("skip update table schema version, table does not exist", K(tenant_id), K(item.table_id_));
    } else if (!table_schema->check_can_do_ddl()) {
      // Skip schema version increment if table is in offline DDL state
      LOG_INFO("skip update table schema version, table can not do ddl", K(tenant_id), K(item.table_id_));
    } else if (OB_FAIL(schema_service->get_table_sql_service().update_data_table_schema_version(
               trans, tenant_id, item.table_id_, table_schema->get_in_offline_ddl_white_list()))) {
      LOG_WARN("fail to update table schema version", K(ret), K(tenant_id), K(item.table_id_));
    }
  }
  return ret;
}

int ObSensitiveRuleDDLOperator::grant_or_revoke_after_ddl(ObSensitiveRuleSchema &schema,
                                                          ObMySQLTransaction &trans,
                                                          const share::schema::ObSchemaOperationType ddl_type,
                                                          ObSchemaGetterGuard &schema_guard,
                                                          uint64_t user_id)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  ObSchemaService *schema_sql_service = NULL;
  ObDDLOperator ddl_operator(schema_service_, sql_proxy_);
  if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  } else if (OB_DDL_CREATE_SENSITIVE_RULE == ddl_type) {
    if (lib::Worker::CompatMode::MYSQL == compat_mode) {
      ObSensitiveRulePrivSortKey sensitive_rule_priv_key(tenant_id,
                                                         user_id,
                                                         schema.get_sensitive_rule_name_str());
      ObPrivSet priv_set = OB_PRIV_PLAINACCESS;
      OZ(grant_revoke_sensitive_rule(sensitive_rule_priv_key, priv_set, true, ObString(), trans));
    } else {
      int64_t new_schema_version_ora = OB_INVALID_VERSION;
      ObObjPrivSortKey obj_priv_key(tenant_id,
                                    schema.get_sensitive_rule_id(),
                                    static_cast<uint64_t>(ObObjectType::SENSITIVE_RULE),
                                    OBJ_LEVEL_FOR_TAB_PRIV,
                                    OB_ORA_SYS_USER_ID,
                                    user_id);
      share::ObRawObjPrivArray new_obj_priv_array;
      share::ObRawObjPrivArray obj_priv_array;
      OZ(obj_priv_array.push_back(OBJ_PRIV_ID_PLAINACCESS));
      OZ(ddl_operator.set_need_flush_ora(schema_guard, obj_priv_key, 0, obj_priv_array, new_obj_priv_array));
      if (new_obj_priv_array.count() > 0) {
        OZ(schema_service_.gen_new_schema_version(tenant_id, new_schema_version_ora));
        OZ(schema_sql_service->get_priv_sql_service().grant_table_ora_only(NULL,
                                                                           trans,
                                                                           new_obj_priv_array,
                                                                           0,
                                                                           obj_priv_key,
                                                                           new_schema_version_ora,
                                                                           false,
                                                                           false));
      }
    }
  } else if (OB_DDL_DROP_SENSITIVE_RULE == ddl_type) {
    if (lib::Worker::CompatMode::ORACLE == compat_mode) {
      OZ(ddl_operator.drop_obj_privs(tenant_id, schema.get_sensitive_rule_id(),
                                     static_cast<uint64_t>(ObObjectType::SENSITIVE_RULE), trans,
                                     schema_service_, schema_guard));
    }
  }
  return ret;
}

int ObSensitiveRuleDDLOperator::grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                                            const ObPrivSet priv_set,
                                                            const bool grant,
                                                            const common::ObString &ddl_stmt_str,
                                                            common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaService *schema_sql_service = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObPrivSet new_priv = priv_set;
  bool need_flush = true;
  ObPrivSet sensitive_rule_priv_set = OB_PRIV_SET_EMPTY;
  uint64_t tenant_id = sensitive_rule_priv_key.tenant_id_;
  uint64_t user_id = sensitive_rule_priv_key.user_id_;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or user_id", K(tenant_id), K(user_id), K(ret));
  } else if (OB_ISNULL(schema_sql_service = schema_service_.get_schema_service())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_sql_service must not null", K(ret));
  } else if (0 == priv_set) {
    //do nothing
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_sensitive_rule_priv_set(sensitive_rule_priv_key,
                                                              sensitive_rule_priv_set))) {
    LOG_WARN("get sensitive_rule priv set failed", K(ret));
  } else if (!grant && OB_PRIV_SET_EMPTY == sensitive_rule_priv_set) {
    ret = OB_ERR_NO_GRANT;
    LOG_WARN("no such grant to revoke", K(ret));
  } else {
    new_priv = grant ? priv_set | sensitive_rule_priv_set
                     : (~priv_set) & sensitive_rule_priv_set;
    need_flush = (new_priv != sensitive_rule_priv_set);
    if (need_flush) {
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("failed to gen new schema_version", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_sql_service->get_sensitive_rule_sql_service()
                              .grant_revoke_sensitive_rule(sensitive_rule_priv_key,
                                                           new_priv,
                                                           new_schema_version,
                                                           ddl_stmt_str, trans))) {
        LOG_WARN("apply sensitive_rule failed", K(ret));
      }
    }
  }
  return ret;
}

// used in drop table / drop column
int ObSensitiveRuleDDLOperator::drop_sensitive_column_cascades(const ObTableSchema &table_schema,
                                                               const ObIArray<uint64_t> &drop_column_ids,
                                                               ObMySQLTransaction &trans,
                                                               ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t table_id = table_schema.get_table_id();
  ObString empty_str;
  ObSEArray<const ObSensitiveRuleSchema *, 4> sensitive_rules;
  if (OB_FAIL(schema_guard.get_sensitive_rule_schemas_by_table(table_schema, sensitive_rules))) {
    LOG_WARN("get sensitive rule schemas failed", KR(ret), K(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rules.count(); i++) {
    const ObSensitiveRuleSchema *sensitive_rule = sensitive_rules.at(i);
    ObSensitiveRuleSchema drop_schema;
    if (OB_FAIL(build_drop_sensitive_column_schema(sensitive_rule, table_schema,
                                                   drop_column_ids, drop_schema))) {
      LOG_WARN("build drop sensitive column schema failed", KR(ret), K(i));
    } else if(OB_FAIL(handle_sensitive_rule_function(drop_schema, trans,
                                                     OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN,
                                                     empty_str, tenant_id,
                                                     schema_guard, OB_INVALID_ID))) {
      LOG_WARN("failed to drop sensitive column cascades", KR(ret), K(drop_schema));
    }
  }

  return ret;
}

// for non-parallel drop table
int ObSensitiveRuleDDLOperator::drop_sensitive_column_in_drop_table(const ObTableSchema &table_schema,
                                                                    ObMySQLTransaction &trans,
                                                                    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> column_ids;
  if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("get column ids failed", KR(ret), K(table_schema));
  } else if (OB_FAIL(drop_sensitive_column_cascades(table_schema, column_ids, trans, schema_guard))) {
    LOG_WARN("drop sensitive column cascades failed", KR(ret), K(table_schema));
  }
  return ret;
}

// for parallel drop table
int ObSensitiveRuleDDLOperator::drop_sensitive_column_in_drop_table(const ObTableSchema &table_schema,
                                                                    ObMySQLTransaction &trans,
                                                                    ObIArray<ObSensitiveRuleSchema *> &sensitive_rules)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  ObString empty_str;
  ObSEArray<uint64_t, 8> drop_column_ids;
  ObSensitiveRuleSchema new_schema; // dummy
  if (OB_FAIL(table_schema.get_column_ids(drop_column_ids))) {
    LOG_WARN("get column ids failed", KR(ret), K(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rules.count(); i++) {
    const ObSensitiveRuleSchema *sensitive_rule = sensitive_rules.at(i);
    ObSensitiveRuleSchema drop_schema;
    if (OB_FAIL(build_drop_sensitive_column_schema(sensitive_rule, table_schema,
                                                   drop_column_ids, drop_schema))) {
      LOG_WARN("build drop sensitive column schema failed", KR(ret), K(i));
    } else if (OB_FAIL(handle_sensitive_rule_function_inner(drop_schema, sensitive_rule, trans,
                                                            OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN,
                                                            empty_str, tenant_id, new_schema))) {
      LOG_WARN("failed to drop sensitive column in drop table", KR(ret), K(drop_schema));
    }
  }
  return ret;
}

int ObSensitiveRuleDDLOperator::build_drop_sensitive_column_schema(
  const ObSensitiveRuleSchema *sensitive_rule,
  const ObTableSchema &table_schema,
  const ObIArray<uint64_t> &drop_column_ids,
  ObSensitiveRuleSchema &drop_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sensitive_rule)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sensitive rule", KR(ret));
  } else if (OB_FAIL(drop_schema.assign(*sensitive_rule))) {
    LOG_WARN("assign sensitive rule failed", KR(ret));
  } else if (FALSE_IT(drop_schema.reset_sensitive_field_items())) {
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < drop_column_ids.count(); ++j) {
    ObSensitiveFieldItem drop_item(table_schema.get_table_id(), drop_column_ids.at(j));
    if (!has_exist_in_array(sensitive_rule->get_sensitive_field_items(), drop_item)) {
    } else if (OB_FAIL(drop_schema.add_sensitive_field_item(drop_item))) {
      LOG_WARN("remove sensitive field item failed", KR(ret), K(drop_item));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
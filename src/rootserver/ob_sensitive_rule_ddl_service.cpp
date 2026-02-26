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
#include "rootserver/ob_sensitive_rule_ddl_service.h"
#include "rootserver/ob_sensitive_rule_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_sql_generator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
int ObSensitiveRuleDDLService::sensitive_field_validation_check(const ObSensitiveRuleSchema &schema,
                                                                ObSchemaGetterGuard &schema_guard,
                                                                bool should_exist)
{
  int ret = OB_SUCCESS;
  const ObSensitiveRuleSchema *exist_schema = NULL;
  uint64_t tenant_id = schema.get_tenant_id();
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_sensitive_field_items().count(); ++i) {
    ObSensitiveFieldItem item = schema.get_sensitive_field_items().at(i);
    const ObTableSchema *table_schema = NULL;
    const ObColumnSchemaV2 *column_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, item.table_id_, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(item.table_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(item));
    } else if (!(table_schema->is_user_table() || table_schema->is_user_view())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table is not user table or user view", K(ret), K(item));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "Can not apply sensitive rule to non-user table or view");
    } else if (OB_FAIL(schema_guard.get_column_schema(tenant_id, item.table_id_, item.column_id_, column_schema))) {
      LOG_WARN("fail to get column schema", K(ret), K(tenant_id), K(item.table_id_), K(item.column_id_));
    } else if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret), K(item));
    } else if (OB_FAIL(schema_guard.get_sensitive_rule_schema_by_column(tenant_id,
                                                                        item.table_id_,
                                                                        item.column_id_,
                                                                        exist_schema))) {
      LOG_WARN("failed to check if sensitive column exists", K(ret));
    } else if (should_exist
               && (NULL == exist_schema
                   || exist_schema->get_sensitive_rule_id() != schema.get_sensitive_rule_id())) {
      ret = OB_SENSITIVE_COLUMN_NOT_EXIST;
      LOG_WARN("sensitive column not exist", K(ret), K(item));
      LOG_USER_ERROR(OB_SENSITIVE_COLUMN_NOT_EXIST, table_schema->get_table_name_str().length(),
                                                    table_schema->get_table_name_str().ptr(),
                                                    column_schema->get_column_name_str().length(),
                                                    column_schema->get_column_name_str().ptr());
    } else if (!should_exist && NULL != exist_schema) {
      ret = OB_SENSITIVE_COLUMN_EXIST;
      LOG_WARN("sensitive column already exist", K(ret), K(item));
      LOG_USER_ERROR(OB_SENSITIVE_COLUMN_EXIST, table_schema->get_table_name_str().length(),
                                                table_schema->get_table_name_str().ptr(),
                                                column_schema->get_column_name_str().length(),
                                                column_schema->get_column_name_str().ptr());
    } else { /* do nothing */ }
  }
  return ret;
}

int ObSensitiveRuleDDLService::ddl_validation_check(ObSchemaOperationType ddl_type,
                                                    ObSensitiveRuleSchema &new_schema,
                                                    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_schema.get_tenant_id();
  bool is_schema_exist = false;
  const ObSensitiveRuleSchema *old_schema = NULL;
  if (OB_FAIL(schema_guard.get_sensitive_rule_schema_by_name(tenant_id,
                                                             new_schema.get_sensitive_rule_name_str(),
                                                             old_schema))) {
    LOG_WARN("failed to check if schema exists", K(ret));
  } else {
    switch (ddl_type) {
      case OB_DDL_CREATE_SENSITIVE_RULE:
        if (OB_NOT_NULL(old_schema)) {
          ret = OB_SENSITIVE_RULE_EXIST;
          LOG_WARN("sensitive rule already exist", K(ret), K(old_schema));
          LOG_USER_ERROR(OB_SENSITIVE_RULE_EXIST, old_schema->get_sensitive_rule_name_str().length(),
                                                  old_schema->get_sensitive_rule_name_str().ptr());
        } else if (OB_FAIL(sensitive_field_validation_check(new_schema, schema_guard, false))) {
          LOG_WARN("check if sensitive column exists failed", K(ret));
        }
        break;
      case OB_DDL_DROP_SENSITIVE_RULE:
      case OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN:
      case OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN:
      case OB_DDL_ALTER_SENSITIVE_RULE_ENABLE:
      case OB_DDL_ALTER_SENSITIVE_RULE_DISABLE:
      case OB_DDL_ALTER_SENSITIVE_RULE_SET_PROTECTION_SPEC:
        if (OB_ISNULL(old_schema)) {
          ret = OB_SENSITIVE_RULE_NOT_EXIST;
          LOG_WARN("sensitive rule not exist", K(ret), K(new_schema));
          LOG_USER_ERROR(OB_SENSITIVE_RULE_NOT_EXIST, new_schema.get_sensitive_rule_name_str().length(),
                                                      new_schema.get_sensitive_rule_name_str().ptr());
        } else if (FALSE_IT(new_schema.set_sensitive_rule_id(old_schema->get_sensitive_rule_id()))) {
        } else if (OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN == ddl_type
                   && OB_FAIL(sensitive_field_validation_check(new_schema, schema_guard, false))) {
          LOG_WARN("check if sensitive column exists failed", K(ret));
        } else if (OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN == ddl_type
                   && OB_FAIL(sensitive_field_validation_check(new_schema, schema_guard, true))) {
          LOG_WARN("check if sensitive column exists failed", K(ret));
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support ddl type", K(ret), K(ddl_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this sensitive_rule ddl type");
        break;
    }
  }
  return ret;
}

int ObSensitiveRuleDDLService::handle_sensitive_rule_ddl(const ObSensitiveRuleDDLArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.schema_.get_tenant_id();
  int64_t refreshed_schema_version = 0;
  uint64_t data_version = 0;
  ObSensitiveRuleSchema schema = arg.schema_; // make a copy
  ObSqlString ddl_stmt_str;
  ObString ddl_sql;
  ObSchemaGetterGuard schema_guard;
  // 1. get the newest schema
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (!((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) ||
               (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
               (data_version >= DATA_VERSION_4_5_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sensitive rule not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("Get schema manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->check_parallel_ddl_conflict(schema_guard, arg))) {
    LOG_WARN("check parallel ddl conflict failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else {
    // 2. start DDL transaction
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObSensitiveRuleDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", K(ret), K(refreshed_schema_version), K(tenant_id));
    // 3. check resolver result
    } else if (OB_FAIL(ddl_validation_check(arg.ddl_type_, schema, schema_guard))) {
      LOG_WARN("ddl validation check failed", K(ret));
    // 4. call ObDDLOperator related function & update related table schema
    } else if (OB_FAIL(ddl_operator.handle_sensitive_rule_function(schema,
                                                                   trans,
                                                                   arg.ddl_type_,
                                                                   arg.ddl_stmt_str_,
                                                                   tenant_id,
                                                                   schema_guard,
                                                                   arg.user_id_))) {
      LOG_WARN("handle sensitive rule function failed", K(ret));
    }

    // 5. end DDL transaction
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
    // 6. refresh (publish) schema
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSensitiveRuleDDLService::revoke_sensitive_rule(const ObRevokeSensitiveRuleArg &arg)
{
  int ret = OB_SUCCESS;
  ObSqlString ddl_stmt_str;
  ObString ddl_sql;
  ObSensitiveRulePrivSortKey sensitive_rule_priv_key(arg.tenant_id_, arg.user_id_, arg.sensitive_rule_);
  ObSchemaGetterGuard schema_guard;
  const ObUserInfo *user_info = NULL;
  ObNeedPriv need_priv;
  need_priv.priv_set_ = arg.priv_set_;
  need_priv.priv_level_ = OB_PRIV_SENSITIVE_RULE_LEVEL;
  need_priv.sensitive_rule_ = arg.sensitive_rule_;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_,
                                                                                       schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(ddl_service_->check_parallel_ddl_conflict(schema_guard, arg))) {
    LOG_WARN("check parallel ddl conflict failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_user_info(arg.tenant_id_, arg.user_id_, user_info))) {
    LOG_WARN("get_user_info failed", K(arg), K(ret));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", K(ret), K(arg));
  } else if (OB_FAIL(grant_revoke_sensitive_rule(sensitive_rule_priv_key,
                                                 user_info->get_user_name_str(),
                                                 user_info->get_host_name_str(),
                                                 need_priv,
                                                 false,
                                                 schema_guard))) {
    LOG_WARN("grant_revoke_sensitive_rule failed", K(ret), K(arg.tenant_id_), K(arg.user_id_), K(ddl_sql));
  }
  return ret;
}

int ObSensitiveRuleDDLService::grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                                           const ObString &user_name,
                                                           const ObString &host_name,
                                                           const ObNeedPriv &need_priv,
                                                           const bool grant,
                                                           share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool is_ora_mode = false;
  int64_t refreshed_schema_version = 0;
  uint64_t tenant_id = sensitive_rule_priv_key.tenant_id_;
  uint64_t user_id = sensitive_rule_priv_key.user_id_;
  ObSqlString ddl_stmt_sql_str;
  ObString ddl_stmt_str;
  const ObPrivSet priv_set = need_priv.priv_set_;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant id is invalid", K(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_ora_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if ((lib::is_mysql_mode() && !((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) ||
                                        (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                        (data_version >= DATA_VERSION_4_5_1_0)))
             || (lib::is_oracle_mode() && !((data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0) ||
                                            (data_version >= DATA_VERSION_4_5_1_0)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sensitive rule not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sensitive rule");
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObDDLSqlGenerator::gen_sensitive_rule_priv_sql(ObAccountArg(user_name, host_name),
                                                                    need_priv, grant, ddl_stmt_sql_str))) {
    LOG_WARN("gen_sensitive_rule_priv sql failed", K(ret), K(user_name), K(host_name));
  } else {
    ddl_stmt_str = ddl_stmt_sql_str.string();
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObSensitiveRuleDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", K(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(ddl_operator.grant_revoke_sensitive_rule(sensitive_rule_priv_key, priv_set,
                                                                grant, ddl_stmt_str, trans))) {
      LOG_WARN("failed to grant revoke sensitive rule", K(ret), K(tenant_id), K(user_id), K(priv_set), K(grant));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSensitiveRuleDDLService::drop_sensitive_column_caused_by_drop_column_online(
  share::schema::ObSchemaGetterGuard &schema_guard,
  const share::schema::ObTableSchema &origin_table_schema,
  const common::ObIArray<uint64_t> &drop_cols_id_arr,
  common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else {
    ObSensitiveRuleDDLOperator sensitive_rule_ddl_operator(ddl_service_->get_schema_service(),
                                                           ddl_service_->get_sql_proxy());
    if (OB_FAIL(sensitive_rule_ddl_operator.drop_sensitive_column_cascades(origin_table_schema,
                                                                           drop_cols_id_arr,
                                                                           trans, schema_guard))) {
      LOG_WARN("drop sensitive column in drop table failed", KR(ret), K(origin_table_schema));
    }
  }
  return ret;
}

int ObSensitiveRuleDDLService::rebuild_hidden_table_sensitive_columns(
  const ObTableSchema &orig_table_schema,
  const ObTableSchema &hidden_table_schema,
  const AlterTableSchema &alter_table_schema,
  ObSchemaGetterGuard &schema_guard,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObString empty_ddl_stmt;
  const uint64_t tenant_id = orig_table_schema.get_tenant_id();
  ObSEArray<const ObSensitiveRuleSchema *, 4> sensitive_rules;
  ObColumnNameMap col_name_map;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_sensitive_rule_schemas_by_table(orig_table_schema, sensitive_rules))) {
    LOG_WARN("get sensitive rule schemas failed", KR(ret), K(orig_table_schema));
  } else if (OB_FAIL(col_name_map.init(orig_table_schema, hidden_table_schema, alter_table_schema))) {
    LOG_WARN("failed to init column name map", K(ret));
  } else {
    ObSensitiveRuleDDLOperator sensitive_rule_ddl_operator(ddl_service_->get_schema_service(),
                                                           ddl_service_->get_sql_proxy());
    for (int64_t i = 0; OB_SUCC(ret) && i < sensitive_rules.count(); i++) {
      const ObSensitiveRuleSchema *sensitive_rule = sensitive_rules.at(i);
      ObSensitiveRuleSchema rebuild_schema;
      if (OB_ISNULL(sensitive_rule)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sensitive rule is null", KR(ret));
      } else if (OB_FAIL(rebuild_schema.assign(*sensitive_rule))) {
        LOG_WARN("assign sensitive rule schema failed", KR(ret));
      } else if (FALSE_IT(rebuild_schema.reset_sensitive_field_items())) {
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < orig_table_schema.get_column_count(); ++j) {
        const ObColumnSchemaV2 *orig_col_schema = orig_table_schema.get_column_schema_by_idx(j);
        const ObColumnSchemaV2 *hidden_col_schema = NULL;
        ObString hidden_col_name;
        if (OB_ISNULL(orig_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orig col schema is null", KR(ret));
        } else {
          ObSensitiveFieldItem orig_item(orig_col_schema->get_table_id(), orig_col_schema->get_column_id());
          if (!has_exist_in_array(sensitive_rule->get_sensitive_field_items(), orig_item)) {
            // not sensitive column, do nothing
          } else if (OB_FAIL(col_name_map.get(orig_col_schema->get_column_name_str(), hidden_col_name))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              // the column is dropped
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("get hidden col schema failed", KR(ret));
            }
          } else if (OB_ISNULL(hidden_col_schema = hidden_table_schema.get_column_schema(hidden_col_name))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hidden col schema is null", KR(ret));
          } else if (OB_FAIL(rebuild_schema.add_sensitive_field_item(hidden_table_schema.get_table_id(),
                                                                     hidden_col_schema->get_column_id()))) {
            LOG_WARN("add sensitive field item failed",
                     KR(ret), K(hidden_table_schema.get_table_id()), K(hidden_col_schema->get_column_id()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if(OB_FAIL(sensitive_rule_ddl_operator.handle_sensitive_rule_function(
                                                      rebuild_schema, trans,
                                                      OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN,
                                                      empty_ddl_stmt, tenant_id,
                                                      schema_guard, OB_INVALID_ID))) {
        LOG_WARN("failed to rebuild column for sensitive rule after creating hidden table",
                  KR(ret), K(rebuild_schema));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
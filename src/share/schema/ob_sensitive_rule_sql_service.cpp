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
#include "share/schema/ob_sensitive_rule_sql_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_routine_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObSensitiveRuleSqlService::apply_new_schema(const ObSensitiveRuleSchema &schema,
                                                ObISQLClient &sql_client,
                                                const ObSchemaOperationType ddl_type,
                                                const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  switch (ddl_type) {
    case OB_DDL_CREATE_SENSITIVE_RULE:
      if (OB_FAIL(add_schema(sql_client, schema))) {
        LOG_WARN("failed to apply new sensitive rule schema", K(ret));
      } else if (OB_FAIL(add_column_schema(sql_client, schema))) {
        LOG_WARN("failed to add column schema", K(ret));
      }
      break;
    case OB_DDL_DROP_SENSITIVE_RULE:
      if (OB_FAIL(drop_column_schema(sql_client, schema))) {
        LOG_WARN("failed to drop column schema", K(ret));
      } else if (OB_FAIL(drop_schema(sql_client, schema))) {
        LOG_WARN("failed to drop sensitive rule schema", K(ret));
      }
      break;
    case OB_DDL_ALTER_SENSITIVE_RULE_ADD_COLUMN:
      if (OB_FAIL(add_column_schema(sql_client, schema))) {
        LOG_WARN("failed to add column schema", K(ret));
      } else if (OB_FAIL(alter_schema(sql_client, schema))) {
        LOG_WARN("failed to alter schema");
      }
      break;
    case OB_DDL_ALTER_SENSITIVE_RULE_DROP_COLUMN:
      if (OB_FAIL(drop_column_schema(sql_client, schema))) {
        LOG_WARN("failed to drop column schema", K(ret));
      } else if (OB_FAIL(alter_schema(sql_client, schema))) {
        LOG_WARN("failed to alter schema");
      }
      break;
    case OB_DDL_ALTER_SENSITIVE_RULE_ENABLE:
    case OB_DDL_ALTER_SENSITIVE_RULE_DISABLE:
    case OB_DDL_ALTER_SENSITIVE_RULE_SET_PROTECTION_SPEC:
      if (OB_FAIL(alter_schema(sql_client, schema))) {
        LOG_WARN("failed to alter sensitive rule schema", K(ret));
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      break;
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation operation;
    operation.tenant_id_ = schema.get_tenant_id();
    operation.sensitive_rule_id_ = schema.get_sensitive_rule_id();
    operation.op_type_ = ddl_type;
    operation.schema_version_ = schema.get_schema_version();
    operation.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObSensitiveRuleSqlService::add_schema(ObISQLClient &sql_client,
                                          const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  // INSERT INTO __all_sensitive_rule, __all_sensitive_rule_history
  const char *tname[] = {OB_ALL_SENSITIVE_RULE_TNAME, OB_ALL_SENSITIVE_RULE_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(gen_insert_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (THE_HISTORY_TABLE_IDX == i) {
      SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
      SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                      static_cast<int32_t>(values.length()),
                                      values.ptr()))) {
      LOG_WARN("append sql failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
    values.reset();
  }

  return ret;
}

int ObSensitiveRuleSqlService::drop_schema(ObISQLClient &sql_client,
                                           const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  // delete from __all_sensitive_rule
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND sensitive_rule_id = %lu",
                                    OB_ALL_SENSITIVE_RULE_TNAME,
                                    ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                    ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_sensitive_rule_id())))) {
    LOG_WARN("fail to assign sql format", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
  }
  // insert into __all_sensitive_rule_history
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", OB_ALL_SENSITIVE_RULE_HISTORY_TNAME))) {
    LOG_WARN("fail to assign sql format", K(ret));
  } else if (OB_FAIL(gen_insert_sql(sql, values, schema))) {
    LOG_WARN("fail to gen sql", K(ret));
  } else {
    SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
    SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                    static_cast<int32_t>(values.length()),
                                    values.ptr()))) {
    LOG_WARN("append sql failed, ", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
  }
  values.reset();
  return ret;
}

int ObSensitiveRuleSqlService::alter_schema(ObISQLClient &sql_client,
                                            const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_SENSITIVE_RULE_TNAME, OB_ALL_SENSITIVE_RULE_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               THE_SYS_TABLE_IDX == i ? "REPLACE" : "INSERT",
                               tname[i]))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(gen_insert_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (THE_HISTORY_TABLE_IDX == i) {
      SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
      SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                      static_cast<int32_t>(values.length()),
                                      values.ptr()))) {
      LOG_WARN("append sql failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
    values.reset();
  }
  return ret;
}

int ObSensitiveRuleSqlService::gen_insert_sql(common::ObSqlString &sql,
                                              common::ObSqlString &values,
                                              const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, schema.get_tenant_id()),
                       "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_sensitive_rule_id()),
                       "sensitive_rule_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_protection_policy(), "protection_policy", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_enabled(), "enabled", "%u");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values,
                                  schema.get_sensitive_rule_name_str().ptr(),
                                  schema.get_sensitive_rule_name_str().length(),
                                  "sensitive_rule_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values,
                                  schema.get_method_str().ptr(),
                                  schema.get_method_str().length(),
                                  "method");
  return ret;
}

int ObSensitiveRuleSqlService::add_column_schema(ObISQLClient &sql_client,
                                                 const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_SENSITIVE_COLUMN_TNAME, OB_ALL_SENSITIVE_COLUMN_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    for (int64_t j = 0; j < schema.get_sensitive_field_items().count(); ++j) {
      ObSensitiveFieldItem item = schema.get_sensitive_field_items().at(j);
      if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
        LOG_WARN("fail to assign sql format", K(ret));
      } else {
        SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                             "tenant_id", "%lu");
        SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_sensitive_rule_id()),
                             "sensitive_rule_id", "%lu");
        SQL_COL_APPEND_VALUE(sql, values, item.table_id_, "table_id", "%lu");
        SQL_COL_APPEND_VALUE(sql, values, item.column_id_, "column_id", "%lu");
      }
      if (OB_FAIL(ret)) {
      } else if (THE_HISTORY_TABLE_IDX == i) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                        static_cast<int32_t>(values.length()),
                                        values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
      values.reset();
    }
  }
  return ret;
}

int ObSensitiveRuleSqlService::drop_column_schema(ObISQLClient &sql_client,
                                                  const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_sensitive_field_items().count(); ++i) {
    // delete from __all_sensitive_column
    ObSensitiveFieldItem item = schema.get_sensitive_field_items().at(i);
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu "
                               "AND sensitive_rule_id = %lu AND table_id = %lu AND column_id = %lu",
                               OB_ALL_SENSITIVE_COLUMN_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_sensitive_rule_id()),
                               item.table_id_, item.column_id_))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
    // insert into __all_sensitive_column_history
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", OB_ALL_SENSITIVE_COLUMN_HISTORY_TNAME))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                           "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_sensitive_rule_id()),
                          "sensitive_rule_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, item.table_id_, "table_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, item.column_id_, "column_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
      SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                      static_cast<int32_t>(values.length()),
                                      values.ptr()))) {
      LOG_WARN("append sql failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
    values.reset();
  }
  return ret;
}

int ObSensitiveRuleSqlService::gen_sensitive_rule_priv_dml(const uint64_t exec_tenant_id,
                                                           const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                                           const ObPrivSet &priv_set,
                                                           share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, sensitive_rule_priv_key.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, sensitive_rule_priv_key.user_id_)))
      || OB_FAIL(dml.add_pk_column("sensitive_rule_name", ObHexEscapeSqlStr(
                                                          sensitive_rule_priv_key.sensitive_rule_))
      || OB_FAIL(dml.add_column("priv_set", priv_set)
      || OB_FAIL(dml.add_gmt_modified())))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObSensitiveRuleSqlService::grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                                           const ObPrivSet priv_set,
                                                           const int64_t new_schema_version,
                                                           const ObString &ddl_stmt_str,
                                                           ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = sensitive_rule_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  if (!sensitive_rule_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sensitive_rule_priv_key));
  } else if (OB_FAIL(gen_sensitive_rule_priv_dml(exec_tenant_id, sensitive_rule_priv_key, priv_set, dml))) {
    LOG_WARN("fail to gen sensitive rule priv dml", K(ret), K(sensitive_rule_priv_key), K(priv_set));
  }
  // insert into __all_sensitive_rule_privilege
  if (OB_SUCC(ret)) {
    if (is_deleted && OB_FAIL(exec.exec_delete(OB_ALL_SENSITIVE_RULE_PRIVILEGE_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec delete", K(ret));
    } else if (!is_deleted && OB_FAIL(exec.exec_replace(OB_ALL_SENSITIVE_RULE_PRIVILEGE_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec replace", K(ret));
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(ret));
    }
  }
  // insert into __all_sensitive_rule_privilege_history
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
        || OB_FAIL(dml.add_pk_column("is_deleted", is_deleted))) {
      LOG_WARN("fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_SENSITIVE_RULE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec replace", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expected to be one", K(affected_rows), K(ret));
    }
  }
  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation priv_op;
    priv_op.tenant_id_ = sensitive_rule_priv_key.tenant_id_;
    priv_op.user_id_ = sensitive_rule_priv_key.user_id_;
    priv_op.sensitive_rule_name_ = sensitive_rule_priv_key.sensitive_rule_;
    priv_op.op_type_ = is_deleted ? OB_DDL_DEL_SENSITIVE_RULE_PRIV : OB_DDL_GRANT_REVOKE_SENSITIVE_RULE;
    priv_op.schema_version_ = new_schema_version;
    priv_op.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(priv_op, sql_client))) {
      LOG_WARN("fail to log operation", K(ret));
    }
  }
  return ret;
}

} // end namespace schema
} // end namespace share
} // end namespace oceanbase
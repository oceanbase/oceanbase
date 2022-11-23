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

#include "share/schema/ob_label_se_policy_sql_service.h"

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "ob_routine_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

template<>
void ObLabelSePolicySqlService::fill_schema_operation(const ObLabelSePolicySchema &schema,
                                                      ObSchemaOperationType ddl_type,
                                                      ObSchemaOperation &operation,
                                                      const ObString &ddl_stmt_str)
{
  operation.tenant_id_ = schema.get_tenant_id();
  operation.table_id_ = schema.get_label_se_policy_id();
  operation.op_type_ = ddl_type;
  operation.schema_version_ = schema.get_schema_version();
  operation.ddl_stmt_str_ = ddl_stmt_str;
}

template<>
void ObLabelSePolicySqlService::fill_schema_operation(const ObLabelSeComponentSchema &schema,
                                                      ObSchemaOperationType ddl_type,
                                                      ObSchemaOperation &operation,
                                                      const ObString &ddl_stmt_str)
{
  operation.tenant_id_ = schema.get_tenant_id();
  operation.table_id_ = schema.get_label_se_component_id();
  operation.op_type_ = ddl_type;
  operation.schema_version_ = schema.get_schema_version();
  operation.ddl_stmt_str_ = ddl_stmt_str;
}

template<>
void ObLabelSePolicySqlService::fill_schema_operation(const ObLabelSeLabelSchema &schema,
                                                      ObSchemaOperationType ddl_type,
                                                      ObSchemaOperation &operation,
                                                      const ObString &ddl_stmt_str)
{
  operation.tenant_id_ = schema.get_tenant_id();
  operation.table_id_ = schema.get_label_se_label_id();
  operation.op_type_ = ddl_type;
  operation.schema_version_ = schema.get_schema_version();
  operation.ddl_stmt_str_ = ddl_stmt_str;
}

template<>
void ObLabelSePolicySqlService::fill_schema_operation(const ObLabelSeUserLevelSchema &schema,
                                                      ObSchemaOperationType ddl_type,
                                                      ObSchemaOperation &operation,
                                                      const ObString &ddl_stmt_str)
{
  operation.tenant_id_ = schema.get_tenant_id();
  operation.table_id_ = schema.get_label_se_user_level_id();
  operation.op_type_ = ddl_type;
  operation.schema_version_ = schema.get_schema_version();
  operation.ddl_stmt_str_ = ddl_stmt_str;
}

template<>
int ObLabelSePolicySqlService::gen_sql(ObSqlString &sql,
                                       ObSqlString &values,
                                       const ObLabelSePolicySchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(schema.get_tenant_id());
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                         exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_policy_id()), "label_se_policy_id", "%lu");

  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_name_str().ptr(),
                                  schema.get_policy_name_str().length(), "policy_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_column_name_str().ptr(),
                                  schema.get_column_name_str().length(), "column_name");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_default_options(), "default_options", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_flag(), "flag", "%ld");
  return ret;
}

template<>
int ObLabelSePolicySqlService::add_schema(common::ObISQLClient &sql_client,
                                          const ObLabelSePolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_POLICY_TNAME, OB_ALL_TENANT_OLS_POLICY_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      LOG_WARN("append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::alter_schema(common::ObISQLClient &sql_client,
                                            const ObLabelSePolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_POLICY_TNAME, OB_ALL_TENANT_OLS_POLICY_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {

    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i == THE_SYS_TABLE_IDX) ? "REPLACE" : "INSERT",
                               tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}


template<>
int ObLabelSePolicySqlService::drop_schema(common::ObISQLClient &sql_client,
                                           const ObLabelSePolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_POLICY_TNAME, OB_ALL_TENANT_OLS_POLICY_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND label_se_policy_id = %lu",
                               tname[THE_SYS_TABLE_IDX],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_label_se_policy_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
  }

  for (int64_t i = THE_HISTORY_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::gen_sql(ObSqlString &sql,
                                       ObSqlString &values,
                                       const ObLabelSeComponentSchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(schema.get_tenant_id());
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                         exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_component_id()), "label_se_component_id", "%lu");

  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_policy_id()), "label_se_policy_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_comp_type(), "comp_type", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_comp_num(), "comp_num", "%ld");

  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_short_name_str().ptr(),
                                  schema.get_short_name_str().length(), "short_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_long_name_str().ptr(),
                                  schema.get_long_name_str().length(), "long_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_parent_name_str().ptr(),
                                  schema.get_parent_name_str().length(), "parent_name");
  return ret;
}

template<>
int ObLabelSePolicySqlService::add_schema(common::ObISQLClient &sql_client,
                                          const ObLabelSeComponentSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_COMPONENT_TNAME, OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::alter_schema(common::ObISQLClient &sql_client,
                                            const ObLabelSeComponentSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_COMPONENT_TNAME, OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i != THE_HISTORY_TABLE_IDX) ? "REPLACE" : "INSERT",
                               tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}


template<>
int ObLabelSePolicySqlService::drop_schema(common::ObISQLClient &sql_client,
                                           const ObLabelSeComponentSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_COMPONENT_TNAME, OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND label_se_component_id = %lu",
                               tname[THE_SYS_TABLE_IDX],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_label_se_component_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
    }
  }

  for (int64_t i = THE_HISTORY_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::gen_sql(ObSqlString &sql,
                                       ObSqlString &values,
                                       const ObLabelSeLabelSchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(schema.get_tenant_id());
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                         exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_label_id()), "label_se_label_id", "%lu");

  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_policy_id()), "label_se_policy_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_label_tag(), "label_tag", "%ld");

  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_label_str().ptr(),
                                  schema.get_label_str().length(), "label");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_flag(), "flag", "%ld");
  return ret;
}

template<>
int ObLabelSePolicySqlService::add_schema(common::ObISQLClient &sql_client,
                                          const ObLabelSeLabelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_LABEL_TNAME, OB_ALL_TENANT_OLS_LABEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}


template<>
int ObLabelSePolicySqlService::alter_schema(common::ObISQLClient &sql_client,
                                            const ObLabelSeLabelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_LABEL_TNAME, OB_ALL_TENANT_OLS_LABEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i != THE_HISTORY_TABLE_IDX) ? "REPLACE" : "INSERT",
                               tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}


template<>
int ObLabelSePolicySqlService::drop_schema(common::ObISQLClient &sql_client,
                                           const ObLabelSeLabelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_LABEL_TNAME, OB_ALL_TENANT_OLS_LABEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND label_se_label_id = %lu",
                               tname[THE_SYS_TABLE_IDX],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_label_se_label_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
    }
  }

  for (int64_t i = THE_HISTORY_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::gen_sql(ObSqlString &sql,
                                       ObSqlString &values,
                                       const ObLabelSeUserLevelSchema &schema)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(schema.get_tenant_id());
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                         exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_user_level_id()), "label_se_user_level_id", "%lu");

  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_user_id()), "user_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                         exec_tenant_id, schema.get_label_se_policy_id()), "label_se_policy_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_maximum_level(), "maximum_level", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_minimum_level(), "minimum_level", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_default_level(), "default_level", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_row_level(), "row_level", "%ld");
  return ret;
}

template<>
int ObLabelSePolicySqlService::add_schema(common::ObISQLClient &sql_client,
                                          const ObLabelSeUserLevelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_USER_LEVEL_TNAME, OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }
  return ret;
}

template<>
int ObLabelSePolicySqlService::alter_schema(common::ObISQLClient &sql_client,
                                            const ObLabelSeUserLevelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_USER_LEVEL_TNAME, OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i != THE_HISTORY_TABLE_IDX) ? "REPLACE" : "INSERT",
                               tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }

  return ret;
}


template<>
int ObLabelSePolicySqlService::drop_schema(common::ObISQLClient &sql_client,
                                           const ObLabelSeUserLevelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_TENANT_OLS_USER_LEVEL_TNAME, OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND label_se_user_level_id = %lu",
                               tname[THE_SYS_TABLE_IDX],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_label_se_user_level_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
      }
    }
  }

  for (int64_t i = THE_HISTORY_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(tname);
       ++i) {

    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("gen sql failed", K(ret));
    }

    if (OB_SUCC(ret)) {

      if (i == THE_HISTORY_TABLE_IDX) {
        SQL_COL_APPEND_VALUE(sql, values, "true", "is_deleted", "%s");
        SQL_COL_APPEND_VALUE(sql, values, schema.get_schema_version(), "schema_version", "%ld");
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
        }
      }
    }
    values.reset();
  }

  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase



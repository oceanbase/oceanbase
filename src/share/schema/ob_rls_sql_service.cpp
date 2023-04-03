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
#include "share/schema/ob_rls_sql_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
const char *ObRlsSqlService::RLS_POLICY_TABLES[2] = {OB_ALL_RLS_POLICY_TNAME,
                                                     OB_ALL_RLS_POLICY_HISTORY_TNAME};
const char *ObRlsSqlService::RLS_GROUP_TABLES[2] = {OB_ALL_RLS_GROUP_TNAME,
                                                    OB_ALL_RLS_GROUP_HISTORY_TNAME};
const char *ObRlsSqlService::RLS_CONTEXT_TABLES[2] = {OB_ALL_RLS_CONTEXT_TNAME,
                                                      OB_ALL_RLS_CONTEXT_HISTORY_TNAME};
const char *ObRlsSqlService::RLS_SEC_COLUMN_TABLES[2] = {OB_ALL_RLS_SECURITY_COLUMN_TNAME,
                                                         OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TNAME};
const char *ObRlsSqlService::RLS_CS_ATTRIBUTE_TABLES[2] = {OB_ALL_RLS_ATTRIBUTE_TNAME,
                                                           OB_ALL_RLS_ATTRIBUTE_HISTORY_TNAME};

template<>
int ObRlsSqlService::gen_sql(ObSqlString &sql, ObSqlString &values, const ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_rls_policy_id()), "rls_policy_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_table_id()), "table_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_rls_group_id()), "rls_group_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_stmt_type(), "stmt_type", "%ld");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_check_opt(), "check_opt", "%d");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_enable_flag(), "enable_flag", "%d");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_name().ptr(),
                                  schema.get_policy_name().length(), "policy_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_function_schema().ptr(),
                                  schema.get_policy_function_schema().length(),
                                  "policy_function_schema");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_package_name().ptr(),
                                  schema.get_policy_package_name().length(),
                                  "policy_package_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_function_name().ptr(),
                                  schema.get_policy_function_name().length(),
                                  "policy_function_name");
  return ret;
}

template<>
int ObRlsSqlService::gen_sql(ObSqlString &sql, ObSqlString &values, const ObRlsGroupSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_rls_group_id()), "rls_group_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_table_id()), "table_id", "%lu");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_policy_group_name().ptr(),
                                  schema.get_policy_group_name().length(), "policy_group_name");
  return ret;
}

template<>
int ObRlsSqlService::gen_sql(ObSqlString &sql, ObSqlString &values, const ObRlsContextSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_rls_context_id()), "rls_context_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_table_id()), "table_id", "%lu");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_context_name().ptr(),
                                  schema.get_context_name().length(), "context_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_attribute().ptr(),
                                  schema.get_attribute().length(), "attribute");
  return ret;
}

template<>
int ObRlsSqlService::gen_sql(ObSqlString &sql, ObSqlString &values, const ObRlsSecColumnSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_rls_policy_id()), "rls_policy_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, schema.get_column_id(), "column_id", "%lu");
  return ret;
}

template<>
int ObRlsSqlService::add_schema(ObISQLClient &sql_client,
                                const ObRlsPolicySchema &schema,
                                const int64_t new_schema_version)
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
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_POLICY_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_POLICY_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
    values.reset();
  }
  return ret;
}

int ObRlsSqlService::alter_schema(ObISQLClient &sql_client,
                                  const ObRlsPolicySchema &schema,
                                  const int64_t new_schema_version)
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
  for (int64_t i = THE_SYS_TABLE_IDX;
       OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_POLICY_TABLES);
       ++i) {
    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i == THE_HISTORY_TABLE_IDX) ? "INSERT" : "REPLACE",
                               RLS_POLICY_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
    values.reset();
  }
  return ret;
}

template<>
int ObRlsSqlService::drop_schema(ObISQLClient &sql_client,
                                 const ObRlsPolicySchema &schema,
                                 const int64_t new_schema_version)
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
   if (OB_SUCC(ret)) {
     if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND rls_policy_id = %lu",
                                RLS_POLICY_TABLES[THE_SYS_TABLE_IDX],
                                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_rls_policy_id())))) {
       LOG_WARN("fail to assign sql format", K(ret));
     } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
       LOG_WARN("fail to execute sql", K(sql), K(ret));
     } else if (!is_single_row(affected_rows)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
     }
   }
   for (int64_t i = THE_HISTORY_TABLE_IDX;
        OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_POLICY_TABLES);
        ++i) {
     if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_POLICY_TABLES[i]))) {
       STORAGE_LOG(WARN, "append table name failed", K(ret));
     } else if (OB_FAIL(gen_sql(sql, values, schema))) {
       LOG_WARN("fail to gen sql", K(ret));
     } else if (i == THE_HISTORY_TABLE_IDX) {
       SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
       SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
     values.reset();
   }
   return ret;
}

template<>
int ObRlsSqlService::add_schema(ObISQLClient &sql_client,
                                const ObRlsGroupSchema &schema,
                                const int64_t new_schema_version)
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
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_GROUP_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_GROUP_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
    values.reset();
  }
  return ret;
}

template<>
int ObRlsSqlService::drop_schema(ObISQLClient &sql_client,
                                 const ObRlsGroupSchema &schema,
                                 const int64_t new_schema_version)
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
   if (OB_SUCC(ret)) {
     if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND rls_group_id = %lu",
                                RLS_GROUP_TABLES[THE_SYS_TABLE_IDX],
                                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_rls_group_id())))) {
       LOG_WARN("fail to assign sql format", K(ret));
     } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
       LOG_WARN("fail to execute sql", K(sql), K(ret));
     } else if (!is_single_row(affected_rows)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
     }
   }
   for (int64_t i = THE_HISTORY_TABLE_IDX;
        OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_GROUP_TABLES);
        ++i) {
     if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_GROUP_TABLES[i]))) {
       STORAGE_LOG(WARN, "append table name failed", K(ret));
     } else if (OB_FAIL(gen_sql(sql, values, schema))) {
       LOG_WARN("fail to gen sql", K(ret));
     } else if (i == THE_HISTORY_TABLE_IDX) {
       SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
       SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
     values.reset();
   }
   return ret;
}

template<>
int ObRlsSqlService::add_schema(ObISQLClient &sql_client,
                                const ObRlsContextSchema &schema,
                                const int64_t new_schema_version)
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
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_CONTEXT_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_CONTEXT_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
    values.reset();
  }
  return ret;
}

template<>
int ObRlsSqlService::drop_schema(ObISQLClient &sql_client,
                                 const ObRlsContextSchema &schema,
                                 const int64_t new_schema_version)
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
   if (OB_SUCC(ret)) {
     if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND rls_context_id = %lu",
                                RLS_CONTEXT_TABLES[THE_SYS_TABLE_IDX],
                                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_rls_context_id())))) {
       LOG_WARN("fail to assign sql format", K(ret));
     } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
       LOG_WARN("fail to execute sql", K(sql), K(ret));
     } else if (!is_single_row(affected_rows)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
     }
   }
   for (int64_t i = THE_HISTORY_TABLE_IDX;
        OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_CONTEXT_TABLES);
        ++i) {
     if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_CONTEXT_TABLES[i]))) {
       STORAGE_LOG(WARN, "append table name failed", K(ret));
     } else if (OB_FAIL(gen_sql(sql, values, schema))) {
       LOG_WARN("fail to gen sql", K(ret));
     } else if (i == THE_HISTORY_TABLE_IDX) {
       SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
       SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
     values.reset();
   }
   return ret;
}

template<>
int ObRlsSqlService::add_schema(ObISQLClient &sql_client,
                                const ObRlsSecColumnSchema &schema,
                                const int64_t new_schema_version)
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
  for (int64_t i = THE_SYS_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_SEC_COLUMN_TABLES); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_SEC_COLUMN_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
      SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
    values.reset();
  }
  return ret;
}

template<>
int ObRlsSqlService::drop_schema(ObISQLClient &sql_client,
                                 const ObRlsSecColumnSchema &schema,
                                 const int64_t new_schema_version)
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
   if (OB_SUCC(ret)) {
     if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND rls_policy_id = %lu AND column_id = %lu",
                                RLS_SEC_COLUMN_TABLES[THE_SYS_TABLE_IDX],
                                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_rls_policy_id()),
                                schema.get_column_id()))) {
       LOG_WARN("fail to assign sql format", K(ret));
     } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
       LOG_WARN("fail to execute sql", K(sql), K(ret));
     } else if (!is_single_row(affected_rows)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
     }
   }
   for (int64_t i = THE_HISTORY_TABLE_IDX;
        OB_SUCC(ret) && i < ARRAYSIZEOF(RLS_SEC_COLUMN_TABLES);
        ++i) {
     if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", RLS_SEC_COLUMN_TABLES[i]))) {
       STORAGE_LOG(WARN, "append table name failed", K(ret));
     } else if (OB_FAIL(gen_sql(sql, values, schema))) {
       LOG_WARN("fail to gen sql", K(ret));
     } else if (i == THE_HISTORY_TABLE_IDX) {
       SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
       SQL_COL_APPEND_VALUE(sql, values, new_schema_version, "schema_version", "%ld");
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
     values.reset();
   }
   return ret;
}

template<>
int ObRlsSqlService::apply_new_schema(const ObRlsPolicySchema &schema,
                                      const int64_t new_schema_version,
                                      ObISQLClient &sql_client,
                                      ObSchemaOperationType ddl_type,
                                      const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  switch (ddl_type) {
  case OB_DDL_CREATE_RLS_POLICY:
    ret = add_schema_cascade(sql_client, schema, new_schema_version);
    break;
  case OB_DDL_ALTER_RLS_POLICY:
    ret = alter_schema(sql_client, schema, new_schema_version);
    break;
  case OB_DDL_DROP_RLS_POLICY:
    ret = drop_schema_cascade(sql_client, schema, new_schema_version);
    break;
  default:
    ret = OB_NOT_SUPPORTED;
    SHARE_SCHEMA_LOG(WARN, "not support ddl type", K(ret), K(ddl_type));
  }

  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "exec sql on inner table failed", K(ret), K(ddl_type));
  } else {
    ObSchemaOperation operation;
    fill_schema_operation(schema, new_schema_version, ddl_type, operation, ddl_stmt_str);
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObRlsSqlService::add_schema_cascade(ObISQLClient &sql_client,
                                        const ObRlsPolicySchema &schema,
                                        const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  OZ (add_schema(sql_client, schema, new_schema_version));
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_sec_column_count(); ++i) {
    const ObRlsSecColumnSchema *sec_column = schema.get_sec_column_by_idx(i);
    CK (OB_NOT_NULL(sec_column));
    OZ (add_schema(sql_client, *sec_column, new_schema_version));
  }
  return ret;
}

int ObRlsSqlService::drop_schema_cascade(ObISQLClient &sql_client,
                                         const ObRlsPolicySchema &schema,
                                         const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  OZ (drop_schema(sql_client, schema, new_schema_version));
  for (int64_t i = 0; OB_SUCC(ret) && i < schema.get_sec_column_count(); ++i) {
    const ObRlsSecColumnSchema *sec_column = schema.get_sec_column_by_idx(i);
    CK (OB_NOT_NULL(sec_column));
    OZ (drop_schema(sql_client, *sec_column, new_schema_version));
  }
  return ret;
}

int ObRlsSqlService::drop_rls_sec_column(const ObRlsPolicySchema &policy_schema,
                                         const ObRlsSecColumnSchema &column_schema,
                                         const int64_t new_schema_version,
                                         ObISQLClient &sql_client,
                                         const common::ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  OZ (drop_schema(sql_client, column_schema, new_schema_version));
  OZ (alter_schema(sql_client, policy_schema, new_schema_version));
  if (OB_SUCC(ret)) {
    ObSchemaOperation operation;
    ObSchemaOperationType ddl_type = OB_DDL_ALTER_RLS_POLICY;
    fill_schema_operation(policy_schema, new_schema_version, ddl_type, operation, ddl_stmt_str);
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

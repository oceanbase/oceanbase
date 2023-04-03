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
#include "share/schema/ob_directory_sql_service.h"
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
const char *ObDirectorySqlService::DIRECTORY_TABLES[2] = {OB_ALL_TENANT_DIRECTORY_TNAME,
                                                          OB_ALL_TENANT_DIRECTORY_HISTORY_TNAME};

ObDirectorySqlService::ObDirectorySqlService(ObSchemaService &schema_service)
  : ObDDLSqlService(schema_service)
{
}

ObDirectorySqlService::~ObDirectorySqlService()
{
}

int ObDirectorySqlService::apply_new_schema(const ObDirectorySchema &schema,
                                            ObISQLClient &sql_client,
                                            ObSchemaOperationType ddl_type,
                                            const common::ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  switch (ddl_type) {
  case OB_DDL_CREATE_DIRECTORY:
    ret = add_schema(sql_client, schema);
    break;
  case OB_DDL_ALTER_DIRECTORY:
    ret = alter_schema(sql_client, schema);
    break;
  case OB_DDL_DROP_DIRECTORY:
    ret = drop_schema(sql_client, schema);
    break;
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported ddl type", K(ret), K(ddl_type));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to exec ddl sql", K(ret), K(schema), K(ddl_type));
  } else {
    ObSchemaOperation operation;
    operation.tenant_id_ = schema.get_tenant_id();
    operation.table_id_ = schema.get_directory_id();
    operation.op_type_ = ddl_type;
    operation.schema_version_ = schema.get_schema_version();
    operation.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObDirectorySqlService::add_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema)
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
       OB_SUCC(ret) && i < ARRAYSIZEOF(DIRECTORY_TABLES);
       ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", DIRECTORY_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
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
    values.reset();
  }
  return ret;
}

int ObDirectorySqlService::alter_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema)
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
       OB_SUCC(ret) && i < ARRAYSIZEOF(DIRECTORY_TABLES);
       ++i) {
    if (OB_FAIL(sql.assign_fmt("%s INTO %s(",
                               (i == THE_HISTORY_TABLE_IDX) ? "INSERT" : "REPLACE",
                               DIRECTORY_TABLES[i]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
      SQL_COL_APPEND_VALUE(sql, values, 0, "is_deleted", "%d");
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
    values.reset();
  }
  return ret;
}

int ObDirectorySqlService::drop_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema)
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
     if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND directory_id = %lu",
                                DIRECTORY_TABLES[THE_SYS_TABLE_IDX],
                                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_directory_id())))) {
       LOG_WARN("fail to assign sql format", K(ret));
     } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
       LOG_WARN("fail to execute sql", K(sql), K(ret));
     } else if (!is_single_row(affected_rows)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
     }
   }
   for (int64_t i = THE_HISTORY_TABLE_IDX;
        OB_SUCC(ret) && i < ARRAYSIZEOF(DIRECTORY_TABLES);
        ++i) {
     if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", DIRECTORY_TABLES[i]))) {
       STORAGE_LOG(WARN, "append table name failed", K(ret));
     } else if (OB_FAIL(gen_sql(sql, values, schema))) {
       LOG_WARN("fail to gen sql", K(ret));
     } else if (i == THE_HISTORY_TABLE_IDX) {
       SQL_COL_APPEND_VALUE(sql, values, 1, "is_deleted", "%d");
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
     values.reset();
   }
   return ret;
}

int ObDirectorySqlService::gen_sql(common::ObSqlString &sql, common::ObSqlString &values, const ObDirectorySchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_directory_id()), "directory_id", "%lu");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_directory_name_str().ptr(),
                                  schema.get_directory_name_str().length(), "directory_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_directory_path_str().ptr(),
                                  schema.get_directory_path_str().length(), "directory_path");
  return ret;
}
} // namespace schema
} // namespace share
} // namespace oceanbase

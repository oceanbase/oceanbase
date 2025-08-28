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
#include "share/schema/ob_catalog_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "ob_routine_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObCatalogSqlService::apply_new_schema(const ObCatalogSchema &schema,
                                          ObISQLClient &sql_client,
                                          const ObSchemaOperationType ddl_type,
                                          const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  switch (ddl_type) {
  case OB_DDL_CREATE_CATALOG:
    ret = add_schema(sql_client, schema);
    break;
  case OB_DDL_ALTER_CATALOG:
    ret = alter_schema(sql_client, schema);
    break;
  case OB_DDL_DROP_CATALOG:
    ret = drop_schema(sql_client, schema);
    break;
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ddl not support", K(ret), K(ddl_type));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to exec ddl sql", K(ret), K(schema));
  } else {
    ObSchemaOperation operation;

    operation.tenant_id_ = schema.get_tenant_id();
    operation.catalog_id_ = schema.get_catalog_id();
    operation.op_type_ = ddl_type;
    operation.schema_version_ = schema.get_schema_version();
    operation.ddl_stmt_str_ = ddl_stmt_str;

    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObCatalogSqlService::gen_sql(ObSqlString &sql,
                                 ObSqlString &values,
                                 const ObCatalogSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                       exec_tenant_id, schema.get_tenant_id()), "tenant_id", "%lu");
  SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                       exec_tenant_id, schema.get_catalog_id()), "catalog_id", "%lu");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_catalog_name_str().ptr(),
                                  schema.get_catalog_name_str().length(), "catalog_name");
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, schema.get_catalog_properties_str().ptr(),
                                  schema.get_catalog_properties_str().length(), "catalog_properties");
  return ret;
}

int ObCatalogSqlService::add_schema(ObISQLClient &sql_client,
                                    const ObCatalogSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_CATALOG_TNAME, OB_ALL_CATALOG_HISTORY_TNAME};
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
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
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
    values.reset();
  }
  return ret;
}

int ObCatalogSqlService::alter_schema(ObISQLClient &sql_client,
                                      const ObCatalogSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_CATALOG_TNAME, OB_ALL_CATALOG_HISTORY_TNAME};
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
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
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
    values.reset();
  }
  return ret;
}

int ObCatalogSqlService::drop_schema(ObISQLClient &sql_client,
                                     const ObCatalogSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;
  const char *tname[] = {OB_ALL_CATALOG_TNAME, OB_ALL_CATALOG_HISTORY_TNAME};
  enum {THE_SYS_TABLE_IDX = 0, THE_HISTORY_TABLE_IDX = 1};
  uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input argument", K(ret), K(schema));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND catalog_id = %lu",
                              tname[THE_SYS_TABLE_IDX],
                              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema.get_catalog_id())))) {
      LOG_WARN("fail to assign sql format", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
    }
  }
  for (int64_t i = THE_HISTORY_TABLE_IDX; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); ++i) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(", tname[THE_HISTORY_TABLE_IDX]))) {
      STORAGE_LOG(WARN, "append table name failed", K(ret));
    } else if (OB_FAIL(gen_sql(sql, values, schema))) {
      LOG_WARN("fail to gen sql", K(ret));
    } else if (i == THE_HISTORY_TABLE_IDX) {
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
    values.reset();
  }

  return ret;
}

int ObCatalogSqlService::gen_catalog_priv_dml(const uint64_t exec_tenant_id,
                                              const ObCatalogPrivSortKey &catalog_priv_key,
                                              const ObPrivSet &priv_set,
                                              share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, catalog_priv_key.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, catalog_priv_key.user_id_)))
      || OB_FAIL(dml.add_pk_column("catalog_name", ObHexEscapeSqlStr(catalog_priv_key.catalog_)))
      || OB_FAIL(dml.add_column("priv_set", priv_set))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } 
  return ret;
}

int ObCatalogSqlService::grant_revoke_catalog(const ObCatalogPrivSortKey &catalog_priv_key,
                                              const ObPrivSet priv_set,
                                              const int64_t new_schema_version,
                                              const ObString &ddl_stmt_str,
                                              ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = catalog_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  if (!catalog_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(catalog_priv_key), K(ret));
  } else if (OB_FAIL(gen_catalog_priv_dml(exec_tenant_id, catalog_priv_key, priv_set, dml))) {
    LOG_WARN("gen_catalog_priv_dml failed", K(catalog_priv_key), K(priv_set), K(ret));
  }
  // insert into __all_catalog_privilege
  if (OB_SUCC(ret)) {
    if (is_deleted) {
      if (OB_FAIL(exec.exec_delete(OB_ALL_CATALOG_PRIVILEGE_TNAME, dml, affected_rows))) {
        LOG_WARN("exec_delete failed", K(ret));
      }
    } else {
      if (OB_FAIL(exec.exec_replace(OB_ALL_CATALOG_PRIVILEGE_TNAME, dml, affected_rows))) {
        LOG_WARN("exec_replace failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one or two", K(affected_rows), K(ret));
    }
  }
  // insert into __all_catalog_privilege_history
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_CATALOG_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("exec_replace failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
    }
  }
  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation priv_operation;
    priv_operation.tenant_id_ = catalog_priv_key.tenant_id_;
    priv_operation.user_id_ = catalog_priv_key.user_id_;
    priv_operation.catalog_name_ = catalog_priv_key.catalog_;
    priv_operation.op_type_ = (is_deleted ? OB_DDL_DEL_CATALOG_PRIV : OB_DDL_GRANT_REVOKE_CATALOG);
    priv_operation.schema_version_ = new_schema_version;
    priv_operation.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(priv_operation, sql_client))) {
      LOG_WARN("failed to log operation", K(ret));
    }
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

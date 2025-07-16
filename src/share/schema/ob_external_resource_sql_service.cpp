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

#include "ob_external_resource_sql_service.h"

#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_external_resource_mgr.h"

namespace oceanbase
{

namespace share
{

namespace schema
{
int ObExternalResourceSqlService::create_external_resource(const ObSimpleExternalResourceSchema &new_schema,
                                                           const ObString &content,
                                                           const ObString &comment,
                                                           const ObString &ddl_stmt,
                                                           common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer sql;
  ObSqlString buffer;

  const uint64_t tenant_id = new_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;

  if (!new_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_pk_column("resource_id", new_schema.get_resource_id()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("database_id", new_schema.get_database_id()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(new_schema.get_name())))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("type", new_schema.get_type()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("content", ObHexEscapeSqlStr(content)))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("comment", ObHexEscapeSqlStr(comment)))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_EXTERNAL_RESOURCE_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else if (OB_FAIL(sql.add_pk_column("schema_version", new_schema.get_schema_version()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FAIL(sql.add_column("is_deleted", 0))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(content), K(comment));
  } else if (OB_FALSE_IT(buffer.reuse())) {
    // unreachable
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_schema.get_tenant_id();
    opt.database_id_ = new_schema.get_database_id();
    opt.external_resource_id_ = new_schema.get_resource_id();
    opt.op_type_ = OB_DDL_CREATE_EXTERNAL_RESOURCE;
    opt.schema_version_ = new_schema.get_schema_version();
    opt.external_resource_name_ = new_schema.get_name();
    opt.ddl_stmt_str_ = ddl_stmt;

    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObExternalResourceSqlService::drop_external_resource(const ObSimpleExternalResourceSchema &schema,
                                                         const ObString &ddl_stmt,
                                                         common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;

  ObDMLSqlSplicer sql;
  ObSqlString buffer;

  const uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;

  if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("failed to add column", K(ret), K(schema));
  } else if (OB_FAIL(sql.add_pk_column("resource_id", schema.get_resource_id()))) {
    LOG_WARN("failed to add column", K(ret), K(schema));
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_EXTERNAL_RESOURCE_TNAME, buffer))) {
    LOG_WARN("failed to splice_delete_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    buffer.reuse();

    if (OB_FAIL(buffer.append_fmt(
            "INSERT INTO %s(tenant_id, resource_id, schema_version, database_id, name, type, content, comment, is_deleted) "
                    "SELECT tenant_id, resource_id, %ld, database_id, name, type, content, comment, 1 FROM %s WHERE tenant_id=0 AND resource_id=%lu",
            OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
            schema.get_schema_version(),
            OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
            schema.get_resource_id()))) {
      LOG_WARN("failed to append_fmt", K(ret), K(buffer));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
      LOG_WARN("failed to execute write", K(ret), K(buffer));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = schema.get_tenant_id();
    opt.database_id_ = schema.get_database_id();
    opt.external_resource_id_ = schema.get_resource_id();
    opt.op_type_ = OB_DDL_DROP_EXTERNAL_RESOURCE;
    opt.schema_version_ = schema.get_schema_version();
    opt.external_resource_name_ = schema.get_name();
    opt.ddl_stmt_str_ = ddl_stmt;

    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
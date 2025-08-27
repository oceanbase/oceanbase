/**
 * Copyright (c) 2025 OceanBase
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

#include "share/schema/ob_ai_model_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_ai_model_mgr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
int ObAiModelSqlService::create_ai_model(const ObAiModelSchema &new_schema,
                                         const ObString &ddl_stmt,
                                         common::ObISQLClient &sql_client)
{
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = new_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (!new_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_pk_column("model_id", new_schema.get_model_id()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema), K(tenant_id));
  } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(new_schema.get_name())))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_column("type", static_cast<int64_t>(new_schema.get_type())))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(new_schema.get_model_name())))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else if (OB_FAIL(sql.add_pk_column("schema_version", new_schema.get_schema_version()))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_column("is_deleted", 0))) {
    LOG_WARN("failed to add column", K(ret), K(new_schema));
  } else if (FALSE_IT(buffer.reuse())) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_HISTORY_TNAME, buffer))) {
    LOG_WARN("failed to splice_insert_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = new_schema.get_tenant_id();
    opt.ai_model_id_ = new_schema.get_model_id();
    opt.op_type_ = OB_DDL_CREATE_AI_MODEL;
    opt.schema_version_ = new_schema.get_schema_version();
    opt.ai_model_name_ = new_schema.get_name();
    opt.ddl_stmt_str_ = ddl_stmt;

    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObAiModelSqlService::drop_ai_model(const ObAiModelSchema &schema,
                                       const int64_t new_schema_version,
                                       const ObString &ddl_stmt,
                                       common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.4.1.0 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.4.1.0");
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("failed to add column", K(ret), K(schema));
  } else if (OB_FAIL(sql.add_pk_column("model_id", schema.get_model_id()))) {
    LOG_WARN("failed to add column", K(ret), K(schema));
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_AI_MODEL_TNAME, buffer))) {
    LOG_WARN("failed to splice_delete_sql", K(ret));
  } else if (OB_FAIL(sql_client.write(schema.get_tenant_id(), buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    buffer.reuse();
    sql.reuse();
    if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
      LOG_WARN("failed to add column", K(ret), K(schema));
    } else if (OB_FAIL(sql.add_pk_column("model_id", schema.get_model_id()))) {
      LOG_WARN("failed to add column", K(ret), K(schema), K(tenant_id));
    } else if (OB_FAIL(sql.add_pk_column("schema_version", new_schema_version))) {
      LOG_WARN("failed to add column", K(ret), K(new_schema_version));
    } else if (OB_FAIL(sql.add_column("is_deleted", 1))) {
      LOG_WARN("failed to add column", K(ret), K(schema));
    } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(schema.get_name())))) {
      LOG_WARN("failed to add column", K(ret), K(schema));
    } else if (OB_FAIL(sql.add_column("type", static_cast<int64_t>(schema.get_type())))) {
      LOG_WARN("failed to add column", K(ret), K(schema));
    } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(schema.get_model_name())))) {
      LOG_WARN("failed to add column", K(ret), K(schema));
    } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_HISTORY_TNAME, buffer))) {
      LOG_WARN("failed to splice_insert_sql", K(ret));
    } else if (OB_FAIL(sql_client.write(schema.get_tenant_id(), buffer.ptr(), affected_rows))) {
      LOG_WARN("failed to execute write", K(ret), K(buffer));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = schema.get_tenant_id();
    opt.ai_model_id_ = schema.get_model_id();
    opt.op_type_ = OB_DDL_DROP_AI_MODEL;
    opt.schema_version_ = new_schema_version;
    opt.ai_model_name_ = schema.get_name();
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

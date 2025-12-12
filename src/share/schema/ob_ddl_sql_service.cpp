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
#include "ob_ddl_sql_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
// sql_exec_tenant_id is valid, indicating that the __all_ddl_operation of the corresponding tenant needs to be written
int ObDDLSqlService::log_operation(
  ObSchemaOperation &schema_operation,
  common::ObISQLClient &sql_client,
  const int64_t sql_exec_tenant_id /*= OB_INVALID_TENANT_ID*/,
  common::ObSqlString *public_sql_string /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_sql_string;
  ObSqlString *sql_string = (NULL != public_sql_string ? public_sql_string : &tmp_sql_string);
  ObDMLSqlSplicer ddl_operation_dml;
  ObDMLSqlSplicer ddl_id_dml;
  int64_t affected_rows = 0;
  uint64_t tenant_id = schema_operation.tenant_id_;
  uint64_t sql_tenant_id = tenant_id;
  if (OB_INVALID_TENANT_ID != sql_exec_tenant_id) {
    // The following two scenarios need to write the __all_ddl_operation of the corresponding tenant
    // according to the incoming tenant_id:
    // 1. cluster level ddl.
    sql_tenant_id = sql_exec_tenant_id;
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    sql_tenant_id = OB_SYS_TENANT_ID;
  }
  if (OB_ISNULL(sql_string)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(sql_string));
  } else if (FALSE_IT(sql_string->reuse())) {
  } else if (OB_FAIL(log_operation_dml(schema_operation, ddl_operation_dml, ddl_id_dml, sql_tenant_id))) {
    LOG_WARN("failed to get log_operation_dml", KR(ret), K(schema_operation), K(sql_tenant_id));
  } else if (OB_FAIL(ddl_operation_dml.splice_insert_sql(OB_ALL_DDL_OPERATION_TNAME, *sql_string))) {
    LOG_WARN("failed to splice insert sql", KR(ret));
  } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql_string->ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(sql_tenant_id), K(*sql_string));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expect to 1, ", KR(ret), K(affected_rows));
  } else if (ddl_id_dml.empty()) {
    // __all_ddl_id insert check is done in log_operation_dml
  } else if (FALSE_IT(sql_string->reset())) {
  } else if (OB_FAIL(ddl_id_dml.splice_insert_sql(OB_ALL_DDL_ID_TNAME, *sql_string))) {
    LOG_WARN("failed to splice insert sql", KR(ret));
  } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql_string->ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(sql_tenant_id), K(*sql_string));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expect to 1, ", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObDDLSqlService::log_operation_dml(
    const ObSchemaOperation &schema_operation,
    share::ObDMLSqlSplicer &ddl_operation_dml,
    share::ObDMLSqlSplicer &ddl_id_dml,
    const int64_t sql_tenant_id)
{
  int ret = OB_SUCCESS;
  auto *tsi_value = GET_TSI(TSIDDLVar);
  auto *tsi_oper = GET_TSI(TSILastOper);
  ObString *ddl_id_str = NULL;
  uint64_t tenant_id = schema_operation.tenant_id_;
  uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
  if (OB_UNLIKELY(!schema_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_operation is invalid", K(schema_operation), K(ret));
  } else if (OB_ISNULL(tsi_oper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get TSILatOper", KR(ret), K(schema_operation));
  } else if (OB_ISNULL(tsi_value)) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get TSIDDLVar", K(tmp_ret), K(schema_operation));
  } else {
    exec_tenant_id = tsi_value->exec_tenant_id_;
    ddl_id_str = tsi_value->ddl_id_str_;
    tsi_oper->last_operation_tenant_id_ = sql_tenant_id;
    tsi_oper->last_operation_schema_version_ = schema_operation.schema_version_;
  }
  // __all_ddl_operation
  // __all_ddl_id
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_ddl_operation_dml(schema_operation, exec_tenant_id, sql_tenant_id, ddl_operation_dml))) {
    LOG_WARN("failed to gen ddl operation dml", KR(ret), K(schema_operation), K(exec_tenant_id), K(sql_tenant_id));
  } else if (OB_ISNULL(ddl_id_str) || schema_operation.ddl_stmt_str_.empty()) {
      // do-nothing, only record ddl_id into __all_ddl_id if ddl_stmt is not empty
  } else if (OB_FAIL(gen_ddl_id_dml(schema_operation, exec_tenant_id, ddl_id_str, ddl_id_dml))) {
    LOG_WARN("failed to gen ddl id dml", KR(ret), K(schema_operation), K(exec_tenant_id), K(ddl_id_str));
  } else {
    tsi_value->ddl_id_str_ = NULL;
  }
  return ret;
}

int ObDDLSqlService::gen_ddl_operation_dml(
    const ObSchemaOperation &schema_operation,
    const uint64_t exec_tenant_id,
    const uint64_t sql_tenant_id,
    share::ObDMLSqlSplicer &ddl_operation_dml)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!schema_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_operation is invalid", K(schema_operation), K(ret));
  } else if (OB_FAIL(ddl_operation_dml.add_column("schema_version", schema_operation.schema_version_))) {
    LOG_WARN("failed to add column schema_version", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("tenant_id",
          is_tenant_operation(schema_operation.op_type_) ? schema_operation.tenant_id_ : OB_INVALID_TENANT_ID))) {
    LOG_WARN("failed to add column tenant_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("exec_tenant_id", static_cast<int64_t>(exec_tenant_id)))) {
    LOG_WARN("failed to add column exec_tenant_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("user_id",
          fill_schema_id(sql_tenant_id, schema_operation.user_id_)))) {
    LOG_WARN("failed to add column user_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("database_id",
          fill_schema_id(sql_tenant_id, schema_operation.database_id_)))) {
    LOG_WARN("failed to add column database_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("database_name",
          ObHexEscapeSqlStr(schema_operation.database_name_?:"")))) {
    LOG_WARN("failed to add column database_name", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("tablegroup_id",
          fill_schema_id(sql_tenant_id, schema_operation.tablegroup_id_)))) {
    LOG_WARN("failed to add column tablegroup_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("table_id",
          fill_schema_id(sql_tenant_id, schema_operation.table_id_)))) {
    LOG_WARN("failed to add column table_id", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("table_name",
          ObHexEscapeSqlStr(schema_operation.table_name_?:"")))) {
    LOG_WARN("failed to add column table_name", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("operation_type", schema_operation.op_type_))) {
    LOG_WARN("failed to add column operation_type", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_column("ddl_stmt_str",
          ObHexEscapeSqlStr(schema_operation.ddl_stmt_str_?:"")))) {
    LOG_WARN("failed to add column ddl_stmt_str", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.add_gmt_modified())) {
    LOG_WARN("failed to add column gmt_modified", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_operation_dml.finish_row())) {
    LOG_WARN("failed to finish ddl_operation_dml", KR(ret));
  }
  return ret;
}

int ObDDLSqlService::gen_ddl_id_dml(
    const ObSchemaOperation &schema_operation,
    const uint64_t exec_tenant_id,
    const ObString *ddl_id_str,
    share::ObDMLSqlSplicer &ddl_id_dml)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!schema_operation.is_valid()) || OB_ISNULL(ddl_id_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_operation is invalid", K(ret), K(schema_operation), KP(ddl_id_str));
  } else if (OB_FAIL(ddl_id_dml.add_column("tenant_id", exec_tenant_id))) {
    LOG_WARN("failed to add column tenant_id", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(ddl_id_dml.add_column("ddl_id_str", ObHexEscapeSqlStr(*ddl_id_str)))) {
    LOG_WARN("failed to add column ddl_id_str", KR(ret), K(*ddl_id_str));
  } else if (OB_FAIL(ddl_id_dml.add_column("ddl_stmt_str", ObHexEscapeSqlStr(schema_operation.ddl_stmt_str_)))) {
    LOG_WARN("failed to add column ddl_stmt_str", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_id_dml.add_gmt_modified())) {
    LOG_WARN("failed to add column gmt_modified", KR(ret), K(schema_operation));
  } else if (OB_FAIL(ddl_id_dml.finish_row())) {
    LOG_WARN("failed to finish ddl_operation_dml", KR(ret));
  }
  return ret;
}

int ObDDLSqlService::log_nop_operation(const ObSchemaOperation &schema_operation,
                                       const int64_t new_schema_version,
                                       const common::ObString &ddl_sql_str,
                                       common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSchemaOperation ddl_schema_op;
  ddl_schema_op = schema_operation;
  ddl_schema_op.ddl_stmt_str_ = ddl_sql_str;

  if (OB_INVALID_VERSION == new_schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema version", K(ret), K(new_schema_version), K(ddl_schema_op));
  } else {
    ddl_schema_op.schema_version_ = new_schema_version;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_VERSION == ddl_schema_op.schema_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema version", K(ret), K(ddl_schema_op));
  } else if (OB_FAIL(log_operation(ddl_schema_op, sql_client))) {
    LOG_WARN("failed to log ddl operator", K(ret), K(ddl_schema_op));
  }
  return ret;
}

uint64_t ObDDLSqlService::fill_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id)
{
  UNUSED(exec_tenant_id);
  return schema_id;
}
} //end of schema
} //end of share
} //end of oceanbase

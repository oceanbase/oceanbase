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
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
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
  ObString ddl_str_hex;
  ObSqlString hex_sql_string;
  ObSqlString tmp_sql_string;
  ObSqlString hex_database_string;
  ObSqlString hex_table_string;
  ObSqlString *sql_string = (NULL != public_sql_string ? public_sql_string : &tmp_sql_string);
  sql_string->reuse();
  auto *tsi_value = GET_TSI(TSIDDLVar);
  auto *tsi_oper = GET_TSI(TSILastOper);
  ObString *ddl_id_str = NULL;
  uint64_t tenant_id = schema_operation.tenant_id_;
  uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
  uint64_t sql_tenant_id = tenant_id;
  if (OB_INVALID_TENANT_ID != sql_exec_tenant_id) {
    // The following two scenarios need to write the __all_ddl_operation of the corresponding tenant
    // according to the incoming tenant_id:
    // 1. cluster level ddl.
    sql_tenant_id = sql_exec_tenant_id;
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    sql_tenant_id = OB_SYS_TENANT_ID;
  }
  if (OB_UNLIKELY(!schema_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_operation is invalid", K(schema_operation), K(ret));
  } else if (OB_ISNULL(tsi_oper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get TSILatOper", KR(ret), K(schema_operation));
  } else if (OB_ISNULL(tsi_value)) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get TSIDDLVar", K(tmp_ret), K(schema_operation));
  } else if (OB_FAIL(sql_append_hex_escape_str(schema_operation.database_name_, hex_database_string))) {
    LOG_WARN("sql_append_hex_escape_str failed", K(ret), K(schema_operation.database_name_));
  } else if (OB_FAIL(sql_append_hex_escape_str(schema_operation.table_name_, hex_table_string))) {
    LOG_WARN("sql_append_hex_escape_str failed", K(ret), K(schema_operation.table_name_));
  } else if (OB_FAIL(sql_append_hex_escape_str(schema_operation.ddl_stmt_str_, hex_sql_string))) {
    LOG_WARN("sql_append_hex_escape_str failed", K(schema_operation.ddl_stmt_str_));
  } else {
    exec_tenant_id = tsi_value->exec_tenant_id_;
    ddl_id_str = tsi_value->ddl_id_str_;
    ddl_str_hex = hex_sql_string.string();
    tsi_oper->last_operation_tenant_id_ = sql_tenant_id;
    tsi_oper->last_operation_schema_version_ = schema_operation.schema_version_;
  }

  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_string->append_fmt("INSERT INTO %s (SCHEMA_VERSION, TENANT_ID, EXEC_TENANT_ID, USER_ID, DATABASE_ID, "
                "DATABASE_NAME, TABLEGROUP_ID, TABLE_ID, TABLE_NAME, OPERATION_TYPE, DDL_STMT_STR, gmt_modified) "
                "values (%ld, %lu, %lu, %lu, %lu, %.*s, %ld, %lu, %.*s, %d, %.*s, now(6))",
                OB_ALL_DDL_OPERATION_TNAME,
                schema_operation.schema_version_,
                is_tenant_operation(schema_operation.op_type_) ? schema_operation.tenant_id_ : OB_INVALID_TENANT_ID,
                static_cast<int64_t>(exec_tenant_id), // not used after schema splited
                fill_schema_id(sql_tenant_id, schema_operation.user_id_),
                fill_schema_id(sql_tenant_id, schema_operation.database_id_),
                hex_database_string.string().length(),
                hex_database_string.string().ptr(),
                fill_schema_id(sql_tenant_id, schema_operation.tablegroup_id_),
                fill_schema_id(sql_tenant_id, schema_operation.table_id_),
                hex_table_string.string().length(),
                hex_table_string.string().ptr(),
                schema_operation.op_type_,
                ddl_str_hex.length(),
                ddl_str_hex.ptr()))) {
      LOG_WARN("sql string append format string failed, ", K(ret));
    } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql_string->ptr(), affected_rows))) {
      LOG_WARN("execute sql failed,  ", "sql", sql_string->ptr(), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ddl_id_str) || schema_operation.ddl_stmt_str_.empty()) {
      // do-nothing, only record ddl_id into __all_ddl_id if ddl_stmt is not empty
    } else {
      int64_t affected_rows = 0;
      sql_string->reuse();
      if (OB_FAIL(sql_string->append_fmt("INSERT INTO %s (TENANT_ID, DDL_ID_STR, DDL_STMT_STR, gmt_modified) "
          "values(%lu, ",
          OB_ALL_DDL_ID_TNAME, exec_tenant_id))) {
        LOG_WARN("sql string append format string failed, ", K(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(*ddl_id_str, *sql_string))) {
        LOG_WARN("sql_append_hex_escape_str failed", K(*ddl_id_str), K(ret));
      } else if (OB_FAIL(sql_string->append_fmt(", %.*s, now(6))", ddl_str_hex.length(), ddl_str_hex.ptr()))) {
        LOG_WARN("sql string append tailer failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(sql_tenant_id, sql_string->ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", sql_string->ptr(), K(ret));
      } else if (1 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1, ", K(affected_rows), K(ret));
      } else {
        tsi_value->ddl_id_str_ = NULL;
      }
    }
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

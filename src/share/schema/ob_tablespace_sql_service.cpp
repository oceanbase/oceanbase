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
#include "ob_tablespace_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObTablespaceSqlService::create_tablespace(const ObTablespaceSchema &tablespace_schema,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!tablespace_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "tablespace_schema is invalid", K(tablespace_schema.get_tablespace_name_str()), K(ret));
  } else {
    if (OB_FAIL(add_tablespace(sql_client, tablespace_schema))) {
      LOG_WARN("failed to add tablespace", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = tablespace_schema.get_tenant_id();
      // opt.database_id_ = 0;
      // opt.tablespace_id_ = tablespace_schema.get_tablespace_id();
      opt.table_id_ = tablespace_schema.get_tablespace_id();
      opt.table_name_ = tablespace_schema.get_tablespace_name();
      opt.op_type_ = OB_DDL_CREATE_TABLESPACE;
      opt.schema_version_ = tablespace_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObTablespaceSqlService::add_tablespace(common::ObISQLClient &sql_client,
                                       const ObTablespaceSchema &tablespace_schema,
                                       const bool only_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  const char *tname[] = {OB_ALL_TENANT_TABLESPACE_TNAME, OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME};
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_TENANT_TABLESPACE_TNAME)) {
      continue;
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                                        exec_tenant_id, tablespace_schema.get_tenant_id()), "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                        exec_tenant_id, tablespace_schema.get_tablespace_id()), "tablespace_id", "%lu");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, tablespace_schema.get_tablespace_name().ptr(),
                                      tablespace_schema.get_tablespace_name().length(), "tablespace_name");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, tablespace_schema.get_encryption_name().ptr(),
                                      tablespace_schema.get_encryption_name().length(), "encryption_name");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, tablespace_schema.get_encrypt_key().ptr(),
                                      tablespace_schema.get_encrypt_key().length(), "encrypt_key");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                        exec_tenant_id, tablespace_schema.get_master_key_id()), "master_key_id", "%lu");
      if (0 == STRCMP(tname[i], OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME)) {
        SQL_COL_APPEND_VALUE(sql, values, tablespace_schema.get_schema_version(), "schema_version", "%ld");
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else {
          if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(affected_rows), K(sql), K(ret));
          }
        }
      }
    }
    values.reset();
  }
  return ret;
}

int ObTablespaceSqlService::alter_tablespace(const ObTablespaceSchema &tablespace_schema,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!tablespace_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "tablespace_schema is invalid", 
        K(tablespace_schema.get_tablespace_name_str()), K(ret));
  } else {
    if (OB_FAIL(update_tablespace(sql_client, tablespace_schema))) {
      LOG_WARN("failed to update tablespace", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = tablespace_schema.get_tenant_id();
      opt.table_id_ = tablespace_schema.get_tablespace_id();
      opt.table_name_ = tablespace_schema.get_tablespace_name();
      opt.op_type_ = OB_DDL_ALTER_TABLESPACE;
      opt.schema_version_ = tablespace_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObTablespaceSqlService::update_tablespace(common::ObISQLClient &sql_client, 
  const ObTablespaceSchema &tablespace_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = tablespace_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t tablespace_id = tablespace_schema.get_tablespace_id();
  if (OB_SUCC(ret)) {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("tablespace_id", 
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablespace_id)))
          || OB_FAIL(dml.add_column("tablespace_name", 
                ObHexEscapeSqlStr(tablespace_schema.get_tablespace_name())))
          || OB_FAIL(dml.add_column("encryption_name", 
                ObHexEscapeSqlStr( tablespace_schema.get_encryption_name())))
          || OB_FAIL(dml.add_column("encrypt_key", 
                ObHexEscapeSqlStr(tablespace_schema.get_encrypt_key())))
          || OB_FAIL(dml.add_column("master_key_id", 
                    ObSchemaUtils::get_extract_schema_id(
                    exec_tenant_id, tablespace_schema.get_master_key_id())))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
    } else if (FAILEDx(exec.exec_update(OB_ALL_TENANT_TABLESPACE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    } else {/*do nothing*/}
  }
  // add to __all_tenant_tablespace_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_tablespace(sql_client, tablespace_schema, only_history))) {
      LOG_WARN("add_tablespace failed",
               K(tablespace_schema.get_tablespace_name_str()),
               K(only_history),
               K(ret));
    }
  }
  return ret;
}

int ObTablespaceSqlService::drop_tablespace(const ObTablespaceSchema &tablespace_schema,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  const uint64_t tablespace_id = tablespace_schema.get_tablespace_id();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
        || OB_INVALID_ID == tablespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablespace info",
             K(tablespace_schema.get_tablespace_name_str()), K(ret));
  } else if (OB_FAIL(delete_tablespace(tablespace_schema, sql_client, ddl_stmt_str))) {
    LOG_WARN("failed to delete tablespace", K(tablespace_schema.get_tablespace_name_str()), K(ret));
  } else {/*do nothing*/}
  return ret;
}
  
int ObTablespaceSqlService::delete_tablespace(const ObTablespaceSchema &tablespace_schema,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  UNUSED(only_history);
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t tenant_id = tablespace_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t tablespace_id = tablespace_schema.get_tablespace_id();
  const int64_t new_schema_version = tablespace_schema.get_schema_version();
  const int64_t IS_DELETED = 1;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == tablespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablespace info", K(tenant_id), K(tablespace_schema), K(ret));
  } else {
    // insert into __all_tenant_tablespace_history
    if (FAILEDx(sql.assign_fmt(
                "INSERT INTO %s(tenant_id, tablespace_id, schema_version, is_deleted)"
                " VALUES(%lu,%lu,%ld,%ld)",
                OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablespace_id),
                new_schema_version, IS_DELETED))) {
      LOG_WARN("assign insert into all tablespace history fail",
               K(tenant_id), K(tablespace_id), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    }

    // delete from __all_tenant_tablespace
    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND tablespace_id = %lu",
                               OB_ALL_TENANT_TABLESPACE_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, tablespace_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete tablespace schema",
               K(tenant_id),
               K(tablespace_schema),
               K(OB_ALL_TENANT_TABLESPACE_TNAME));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.table_id_ = tablespace_id;
      opt.op_type_ = OB_DDL_DROP_TABLESPACE;
      opt.schema_version_ = new_schema_version;
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase

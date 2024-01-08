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
#include "ob_outline_sql_service.h"
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

int ObOutlineSqlService::insert_outline(const ObOutlineInfo &outline_info,
                                        common::ObISQLClient &sql_client,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!outline_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "outline_info is invalid", K(outline_info), K(ret));
  } else {
    if (OB_FAIL(add_outline(sql_client, outline_info))) {
      LOG_WARN("failed to add outline", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = outline_info.get_tenant_id();
      opt.database_id_ = outline_info.get_database_id();
      opt.outline_id_ = outline_info.get_outline_id();
      opt.table_id_ = outline_info.get_outline_id();
      opt.op_type_ = OB_DDL_CREATE_OUTLINE;
      opt.schema_version_ = outline_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObOutlineSqlService::replace_outline(const ObOutlineInfo &outline_info,
                                         common::ObISQLClient &sql_client,
                                         const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!outline_info.is_valid_for_replace()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outline info is invalid", K(outline_info), K(ret));
  } else {
    ObSqlString sql;
    uint64_t tenant_id = outline_info.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    uint64_t outline_id = outline_info.get_outline_id();
    ObString outline_params_hex_str;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    // modify __all_outline table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(outline_info.get_hex_str_from_outline_params(outline_params_hex_str, allocator))) {
        LOG_WARN("fail to get_hex_str_from_outline_params", K(ret));
      }

      if (OB_SUCC(ret)) {
        ObDMLExecHelper exec(sql_client, exec_tenant_id);
        ObDMLSqlSplicer dml;
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("outline_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_id)))
            || OB_FAIL(dml.add_column("outline_content", ObHexEscapeSqlStr(
                        outline_info.get_outline_content_str().empty() ? ObString::make_string("")
                        : outline_info.get_outline_content_str())))
            || OB_FAIL(dml.add_column("outline_params", ObHexEscapeSqlStr(outline_params_hex_str)))
            || OB_FAIL(dml.add_column("sql_text", outline_info.get_sql_text_str().empty() ? ObString::make_string("") : ObHexEscapeSqlStr(outline_info.get_sql_text_str())))
            || OB_FAIL(dml.add_column("sql_id", outline_info.get_sql_id_str().empty() ? ObString::make_string("") : ObHexEscapeSqlStr(outline_info.get_sql_id_str())))
            || OB_FAIL(dml.add_column("version", ObHexEscapeSqlStr(outline_info.get_version_str())))
            || OB_FAIL(dml.add_column("schema_version", outline_info.get_schema_version()))
            || OB_FAIL(dml.add_column("outline_target", ObHexEscapeSqlStr(
                        outline_info.get_outline_target_str().empty() ? ObString::make_string("")
                        : outline_info.get_outline_target_str())))
            || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        }

        // udpate __all_outline table
        int64_t affected_rows = 0;
        if (FAILEDx(exec.exec_update(OB_ALL_OUTLINE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update sql fail", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
        } else {/*do nothing*/}
      }
    }

    // add to __all_outline_history table
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      if (OB_FAIL(add_outline(sql_client, outline_info, only_history))) {
        LOG_WARN("add_outline failed", K(outline_info), K(only_history), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = outline_info.get_tenant_id();
      opt.database_id_ = outline_info.get_database_id();
      opt.outline_id_ = outline_info.get_outline_id();
      opt.table_id_ = outline_info.get_outline_id();//reuse table_id
      opt.op_type_ = OB_DDL_REPLACE_OUTLINE;
      opt.schema_version_ = outline_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObOutlineSqlService::alter_outline(const ObOutlineInfo &outline_info,
                                       common::ObISQLClient &sql_client,
                                       const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!outline_info.is_valid_for_replace()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outline info is invalid", K(outline_info), K(ret));
  } else {
    ObSqlString sql;
    uint64_t tenant_id = outline_info.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    uint64_t outline_id = outline_info.get_outline_id();
    ObString outline_params_hex_str;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    // modify __all_outline table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(outline_info.get_hex_str_from_outline_params(outline_params_hex_str, allocator))) {
        LOG_WARN("fail to get_hex_str_from_max_concurrent_param", K(ret));
      }

      // outline_content is described by sql_test here.
      if (OB_SUCC(ret)) {
        ObDMLExecHelper exec(sql_client, exec_tenant_id);
        ObDMLSqlSplicer dml;
        if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
            || OB_FAIL(dml.add_pk_column("outline_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_id)))
            || OB_FAIL(dml.add_column("outline_content", ObHexEscapeSqlStr(
                        outline_info.get_outline_content_str().empty() ? ObString::make_string("")
                        : outline_info.get_outline_content_str())))
            || OB_FAIL(dml.add_column("outline_params", ObHexEscapeSqlStr(outline_params_hex_str)))
            || OB_FAIL(dml.add_column("sql_text", ObHexEscapeSqlStr(outline_info.get_sql_text_str())))
            || OB_FAIL(dml.add_column("schema_version", outline_info.get_schema_version()))
            || OB_FAIL(dml.add_gmt_modified())) {
          LOG_WARN("add column failed", K(ret));
        }

        // udpate __all_outline table
        int64_t affected_rows = 0;
        if (FAILEDx(exec.exec_update(OB_ALL_OUTLINE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update sql fail", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
        } else {/*do nothing*/}
      }
    }

    // add to __all_outline_history table
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      if (OB_FAIL(add_outline(sql_client, outline_info, only_history))) {
        LOG_WARN("add_outline failed", K(outline_info), K(only_history), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = outline_info.get_tenant_id();
      opt.database_id_ = outline_info.get_database_id();
      opt.outline_id_ = outline_info.get_outline_id();
      opt.table_id_ = outline_info.get_outline_id();//reuse table_id
      opt.op_type_ = OB_DDL_ALTER_OUTLINE;
      opt.schema_version_ = outline_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObOutlineSqlService::delete_outline(const uint64_t tenant_id,
                                        const uint64_t database_id,
                                        const uint64_t outline_id,
                                        const int64_t new_schema_version,
                                        common::ObISQLClient &sql_client,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const int64_t IS_DELETED = 1;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
                        || OB_INVALID_ID == outline_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outline info in drop outline", K(tenant_id), K(database_id),
             K(outline_id), K(ret));
  } else {
    // insert into __all_table_history
    if (FAILEDx(sql.assign_fmt(
                   "INSERT INTO %s(tenant_id, outline_id,schema_version,is_deleted)"
                   " VALUES(%lu,%lu,%ld,%ld)",
                   OB_ALL_OUTLINE_HISTORY_TNAME,
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_id),
                   new_schema_version, IS_DELETED))) {
      LOG_WARN("assign insert into all outline history fail",
               K(tenant_id), K(outline_id), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    } else {/*do nothing*/}

    // delete from __all_outline
    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND outline_id=%lu",
                               OB_ALL_OUTLINE_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(sql), K(affected_rows), K(ret));
    } else {/*do nothing*/}

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.outline_id_ = outline_id;
      opt.table_id_ = outline_id;
      opt.database_id_ = database_id;
      opt.op_type_ = OB_DDL_DROP_OUTLINE;
      opt.schema_version_ = new_schema_version;
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObOutlineSqlService::add_outline(common::ObISQLClient &sql_client,
                                     const ObOutlineInfo &outline_info,
                                     const bool only_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  const char *tname[] = {OB_ALL_OUTLINE_TNAME, OB_ALL_OUTLINE_HISTORY_TNAME};
  ObString max_outline_params_hex_str;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  uint64_t tenant_id = outline_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(outline_info.get_hex_str_from_outline_params(max_outline_params_hex_str, allocator))) {
    LOG_WARN("fail to get_hex_str_from_outline_params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_OUTLINE_TNAME)) {
      continue;
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, outline_info.get_tenant_id()), "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_info.get_outline_id()), "outline_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, outline_info.get_database_id()), "database_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, outline_info.get_schema_version(), "schema_version", "%ld");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_name(),
                                      outline_info.get_name_str().length(), "name");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_signature(),
                                      outline_info.get_signature_str().length(), "signature");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_sql_id(),
                                      outline_info.get_sql_id_str().length(), "sql_id");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_outline_content(),
                                      outline_info.get_outline_content_str().length(), "outline_content");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_sql_text(),
                                      outline_info.get_sql_text_str().length(), "sql_text");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_owner(),
                                      outline_info.get_owner_str().length(), "owner");
      SQL_COL_APPEND_VALUE(sql, values, outline_info.is_used(), "used", "%d");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_version(),
                                      outline_info.get_version_str().length(), "version");
      SQL_COL_APPEND_VALUE(sql, values, outline_info.is_compatible(), "compatible", "%d");
      SQL_COL_APPEND_VALUE(sql, values, outline_info.is_enabled(), "enabled", "%d");
      SQL_COL_APPEND_VALUE(sql, values, outline_info.get_hint_format(), "format", "%d");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, max_outline_params_hex_str.ptr(),
                                      max_outline_params_hex_str.length(), "outline_params");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, outline_info.get_outline_target(),
                                      outline_info.get_outline_target_str().length(), "outline_target");
      if (0 == STRCMP(tname[i], OB_ALL_OUTLINE_HISTORY_TNAME)) {
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


} //end of schema
} //end of share
} //end of oceanbase

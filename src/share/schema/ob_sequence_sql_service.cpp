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
#include "ob_sequence_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {

int ObSequenceSqlService::insert_sequence(
    const ObSequenceSchema& sequence_schema, common::ObISQLClient* sql_client, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!sequence_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "sequence_schema is invalid", K(sequence_schema.get_sequence_name_str()), K(ret));
  } else {
    if (OB_FAIL(add_sequence(*sql_client, sequence_schema))) {
      LOG_WARN("failed to add sequence", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = sequence_schema.get_tenant_id();
      opt.database_id_ = sequence_schema.get_database_id();
      opt.sequence_id_ = sequence_schema.get_sequence_id();
      opt.table_id_ = sequence_schema.get_sequence_id();
      opt.table_name_ = sequence_schema.get_sequence_name();
      opt.op_type_ = OB_DDL_CREATE_SEQUENCE;
      opt.schema_version_ = sequence_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::replace_sequence(const ObSequenceSchema& sequence_schema, const bool is_rename,
    common::ObISQLClient* sql_client, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSqlString sql;
    uint64_t tenant_id = sequence_schema.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    uint64_t sequence_id = sequence_schema.get_sequence_id();
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    // modify __all_sequence table
    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret)) {
        ObDMLExecHelper exec(*sql_client, exec_tenant_id);
        ObDMLSqlSplicer dml;
        if (!is_rename) {
          if (OB_FAIL(
                  dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
              OB_FAIL(dml.add_pk_column(
                  "sequence_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id))) ||
              OB_FAIL(dml.add_column("min_value", sequence_schema.get_min_value())) ||
              OB_FAIL(dml.add_column("max_value", sequence_schema.get_max_value())) ||
              OB_FAIL(dml.add_column("increment_by", sequence_schema.get_increment_by())) ||
              OB_FAIL(dml.add_column("cache_size", sequence_schema.get_cache_size())) ||
              OB_FAIL(dml.add_column("order_flag", sequence_schema.get_order_flag())) ||
              OB_FAIL(dml.add_column("cycle_flag", sequence_schema.get_cycle_flag())) ||
              OB_FAIL(dml.add_column("schema_version", sequence_schema.get_schema_version())) ||
              OB_FAIL(dml.add_gmt_modified())) {
            LOG_WARN("add column failed", K(ret));
          }
        } else {  // rename sequence
          if (OB_FAIL(
                  dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id))) ||
              OB_FAIL(dml.add_pk_column(
                  "sequence_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id))) ||
              OB_FAIL(dml.add_column("sequence_name", ObHexEscapeSqlStr(sequence_schema.get_sequence_name()))) ||
              OB_FAIL(dml.add_column("schema_version", sequence_schema.get_schema_version())) ||
              OB_FAIL(dml.add_gmt_modified())) {
            LOG_WARN("add column failed", K(ret));
          }
        }
        // udpate __all_sequence_object table
        int64_t affected_rows = 0;
        if (FAILEDx(exec.exec_update(OB_ALL_SEQUENCE_OBJECT_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update sql fail", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
        } else { /*do nothing*/
        }
      }
    }

    // add to __all_sequence_object_history table
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      if (OB_FAIL(add_sequence(*sql_client, sequence_schema, only_history))) {
        LOG_WARN("add_sequence failed", K(sequence_schema.get_sequence_name_str()), K(only_history), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = sequence_schema.get_tenant_id();
      opt.database_id_ = sequence_schema.get_database_id();
      opt.sequence_id_ = sequence_schema.get_sequence_id();
      opt.table_id_ = sequence_schema.get_sequence_id();
      opt.table_name_ = sequence_schema.get_sequence_name();
      opt.op_type_ = OB_DDL_ALTER_SEQUENCE;
      opt.schema_version_ = sequence_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::delete_sequence(const uint64_t tenant_id, const uint64_t database_id,
    const uint64_t sequence_id, const int64_t new_schema_version, common::ObISQLClient* sql_client,
    const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t IS_DELETED = 1;

  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == sequence_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sequence info in drop sequence", K(tenant_id), K(database_id), K(sequence_id), K(ret));
  } else {
    // insert into __all_sequence_object_history
    if (FAILEDx(sql.assign_fmt("INSERT INTO %s(tenant_id, sequence_id,schema_version,is_deleted)"
                               " VALUES(%lu,%lu,%ld,%ld)",
            OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id),
            new_schema_version,
            IS_DELETED))) {
      LOG_WARN("assign insert into all sequence history fail", K(tenant_id), K(sequence_id), K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    }

    // delete from __all_sequence_object
    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND sequence_id=%lu",
            OB_ALL_SEQUENCE_OBJECT_TNAME,
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete sequence schema",
          K(tenant_id),
          K(database_id),
          K(sequence_id),
          K(OB_ALL_SEQUENCE_OBJECT_TNAME));
    }

    if (FAILEDx(sql.assign_fmt(
            "DELETE FROM %s WHERE sequence_id=%lu", OB_ALL_SEQUENCE_VALUE_TNAME, extract_pure_id(sequence_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client->write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 < affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("more than one row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete sequence value",
          K(tenant_id),
          K(database_id),
          K(sequence_id),
          K(OB_ALL_SEQUENCE_VALUE_TNAME));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.sequence_id_ = sequence_id;
      opt.table_id_ = sequence_id;
      opt.database_id_ = database_id;
      opt.op_type_ = OB_DDL_DROP_SEQUENCE;
      opt.schema_version_ = new_schema_version;
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceSqlService::drop_sequence(const ObSequenceSchema& sequence_schema, const int64_t new_schema_version,
    common::ObISQLClient* sql_client, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = sequence_schema.get_tenant_id();
  const uint64_t database_id = sequence_schema.get_database_id();
  const uint64_t sequence_id = sequence_schema.get_sequence_id();
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || OB_INVALID_ID == sequence_id ||
                         new_schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sequence info in drop sequence", K(sequence_schema.get_sequence_name_str()), K(ret));
  } else if (OB_FAIL(
                 delete_sequence(tenant_id, database_id, sequence_id, new_schema_version, sql_client, ddl_stmt_str))) {
    LOG_WARN("failed to delete sequence", K(sequence_schema.get_sequence_name_str()), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSequenceSqlService::add_sequence(
    common::ObISQLClient& sql_client, const ObSequenceSchema& sequence_schema, const bool only_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  const char* tname[] = {OB_ALL_SEQUENCE_OBJECT_TNAME, OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME};
  ObString max_sequence_params_hex_str;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const uint64_t tenant_id = sequence_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_SEQUENCE_OBJECT_TNAME)) {
      continue;
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql,
          values,
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, sequence_schema.get_tenant_id()),
          "tenant_id",
          "%lu");
      SQL_COL_APPEND_VALUE(sql,
          values,
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_schema.get_sequence_id()),
          "sequence_id",
          "%lu");
      SQL_COL_APPEND_VALUE(sql,
          values,
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, sequence_schema.get_database_id()),
          "database_id",
          "%lu");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_schema_version(), "schema_version", "%ld");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql,
          values,
          sequence_schema.get_sequence_name().ptr(),
          sequence_schema.get_sequence_name().length(),
          "sequence_name");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_min_value().format(), "min_value", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_max_value().format(), "max_value", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_increment_by().format(), "increment_by", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_start_with().format(), "start_with", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_cache_size().format(), "cache_size", "%s");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_order_flag(), "order_flag", "%d");
      SQL_COL_APPEND_VALUE(sql, values, sequence_schema.get_cycle_flag(), "cycle_flag", "%d");
      if (0 == STRCMP(tname[i], OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME)) {
        SQL_COL_APPEND_VALUE(sql, values, "false", "is_deleted", "%s");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(
                ", gmt_modified) VALUES (%.*s, now(6))", static_cast<int32_t>(values.length()), values.ptr()))) {
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

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

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
#include "ob_keystore_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_encryption_util.h"
namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObKeystoreSqlService::create_keystore(const ObKeystoreSchema &keystore_schema,
    common::ObISQLClient &sql_client,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!keystore_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "keystore_schema is invalid", K(keystore_schema.get_keystore_name_str()), K(ret));
  } else {
    if (OB_FAIL(add_keystore(sql_client, keystore_schema))) {
      LOG_WARN("failed to add keystore", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = keystore_schema.get_tenant_id();
      opt.table_id_ = keystore_schema.get_keystore_id();
      opt.table_name_ = keystore_schema.get_keystore_name();
      opt.op_type_ = OB_DDL_CREATE_KEYSTORE;
      opt.schema_version_ = keystore_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObKeystoreSqlService::add_keystore(common::ObISQLClient &sql_client,
                                       const ObKeystoreSchema &keystore_schema,
                                       const bool only_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString values;
  const char *tname[] = {OB_ALL_TENANT_KEYSTORE_TNAME, OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME};
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const uint64_t tenant_id = keystore_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_TENANT_KEYSTORE_TNAME)) {
      continue;
    } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname[i]))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(
                                        exec_tenant_id, keystore_schema.get_tenant_id()), "tenant_id", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                                        exec_tenant_id, keystore_schema.get_keystore_id()), "keystore_id", "%lu");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, keystore_schema.get_keystore_name().ptr(),
                                      keystore_schema.get_keystore_name().length(), "keystore_name");
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, keystore_schema.get_password().ptr(),
                                      keystore_schema.get_password().length(), "password");
      SQL_COL_APPEND_VALUE(sql, values, keystore_schema.get_status(), "status", "%lu");
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_schema_id(
                           exec_tenant_id, keystore_schema.get_master_key_id()), "master_key_id", "%lu");
      if (OB_INTERNAL_ENCRYPTED_KEY_LENGTH == keystore_schema.get_encrypted_key().length()) {
        SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, "", 0, "master_key");
      } else {
        SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, keystore_schema.get_master_key().ptr(),
                                        keystore_schema.get_master_key().length(), "master_key");
      }
      SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, keystore_schema.get_encrypted_key().ptr(),
                                      keystore_schema.get_encrypted_key().length(),
                                      "encrypted_key");
      if (0 == STRCMP(tname[i], OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME)) {
        SQL_COL_APPEND_VALUE(sql, values, keystore_schema.get_schema_version(), "schema_version", "%ld");
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

int ObKeystoreSqlService::alter_keystore(const ObKeystoreSchema &keystore_schema,
                                         common::ObISQLClient &sql_client,
                                         const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!keystore_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "keystore_schema is invalid", K(keystore_schema.get_keystore_name_str()), K(ret));
  } else {
    if (OB_FAIL(update_keystore(sql_client, keystore_schema))) {
      LOG_WARN("failed to add keystore", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = keystore_schema.get_tenant_id();
      opt.table_id_ = keystore_schema.get_keystore_id();
      opt.table_name_ = keystore_schema.get_keystore_name();
      opt.op_type_ = OB_DDL_ALTER_KEYSTORE;
      opt.schema_version_ = keystore_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObKeystoreSqlService::update_keystore(common::ObISQLClient &sql_client,
                                          const ObKeystoreSchema &keystore_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t tenant_id = keystore_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t keystore_id = keystore_schema.get_keystore_id();
  uint64_t master_key_id = keystore_schema.get_master_key_id();
  // modify __all_keystore table
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      ObDMLSqlSplicer dml;
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                 exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("keystore_id", ObSchemaUtils::get_extract_schema_id(
                                                      exec_tenant_id, keystore_id)))
          || OB_FAIL(dml.add_column("keystore_name", ObHexEscapeSqlStr(keystore_schema.get_keystore_name())))
          || OB_FAIL(dml.add_column("password", ObHexEscapeSqlStr(keystore_schema.get_password())))
          || OB_FAIL(dml.add_column("status", keystore_schema.get_status()))
          || OB_FAIL(dml.add_column("master_key_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, master_key_id)))
          || OB_FAIL(dml.add_column("master_key", (OB_INTERNAL_ENCRYPTED_KEY_LENGTH == keystore_schema.get_encrypted_key().length()) ?
              ObString::make_string("") :
              ObHexEscapeSqlStr(keystore_schema.get_master_key())))
          || OB_FAIL(dml.add_column("encrypted_key", ObHexEscapeSqlStr(keystore_schema.get_encrypted_key())))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      }

      // udpate __all_keystore_object table
      int64_t affected_rows = 0;
      if (FAILEDx(exec.exec_update(OB_ALL_TENANT_KEYSTORE_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update sql fail", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
      } else {/*do nothing*/}
    }
  }

  // add to __all_keystore_object_history table
  if (OB_SUCC(ret)) {
    const bool only_history = true;
    if (OB_FAIL(add_keystore(sql_client, keystore_schema, only_history))) {
      LOG_WARN("add_keystore failed",
               K(keystore_schema.get_keystore_name_str()),
               K(only_history),
               K(ret));
    }
  }
  return ret;
}



} //end of schema
} //end of share
} //end of oceanbase

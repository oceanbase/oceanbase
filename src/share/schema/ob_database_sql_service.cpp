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
#include "ob_database_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase
{
using namespace common;
namespace rootserver
{
class ObRootService;
}
namespace share
{
namespace schema
{

int ObDatabaseSqlService::insert_database(const ObDatabaseSchema &database_schema,
                                          common::ObISQLClient &sql_client,
                                          const ObString *ddl_stmt_str/*=NULL*/,
                                          const bool is_only_history/*=false*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!database_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database schema is invalid", K(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(database_schema.get_charset_type(),
                                                                    exec_tenant_id))) {
    LOG_WARN("failed to check charset data version valid", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    if (OB_SUCC(ret)) {
      const int64_t INVALID_REPLICA_NUM = -1;
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                 exec_tenant_id, database_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("database_id", ObSchemaUtils::get_extract_schema_id(
                                                      exec_tenant_id, database_schema.get_database_id())))
          || OB_FAIL(dml.add_column("database_name", ObHexEscapeSqlStr(database_schema.get_database_name_str())))
          || OB_FAIL(dml.add_column("collation_type", database_schema.get_collation_type()))
          || OB_FAIL(dml.add_column("comment", database_schema.get_comment()))
          || OB_FAIL(dml.add_column("read_only", database_schema.is_read_only()))
          || OB_FAIL(dml.add_column("default_tablegroup_id", ObSchemaUtils::get_extract_schema_id(
                                                             exec_tenant_id, database_schema.get_default_tablegroup_id())))
          || OB_FAIL(dml.add_column("in_recyclebin", database_schema.is_in_recyclebin()))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      }
    }
    ObDMLExecHelper exec(sql_client, exec_tenant_id);

    // insert into __all_database
    if (OB_FAIL(ret)) {
    } else if (is_only_history) {
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // insert into __all_database_history
    const int64_t is_deleted = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml.add_pk_column("schema_version", database_schema.get_schema_version()))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // log operations
    if (OB_SUCC(ret) && !is_only_history) {
      ObSchemaOperation create_db_op;
      create_db_op.tenant_id_ = database_schema.get_tenant_id();
      create_db_op.database_id_ = database_schema.get_database_id();
      create_db_op.tablegroup_id_ = 0;
      create_db_op.table_id_ = 0;
      create_db_op.op_type_ = OB_DDL_ADD_DATABASE;
      create_db_op.schema_version_ = database_schema.get_schema_version();
      create_db_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(create_db_op, sql_client))) {
        LOG_WARN("log create database ddl operation failed", K(create_db_op), K(ret));
      }
    }
  }
  return ret;
}

int ObDatabaseSqlService::update_database(const ObDatabaseSchema &database_schema,
                                          common::ObISQLClient &sql_client,
                                          const ObSchemaOperationType op_type,
                                          const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!database_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database scheam is invalid", K(ret));
  } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(database_schema.get_charset_type(),
                                                                    exec_tenant_id))) {
    LOG_WARN("failed to check charset data version valid", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    if (OB_SUCC(ret)) {
      const int64_t INVALID_REPLICA_NUM = -1;
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                    exec_tenant_id, database_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("database_id", ObSchemaUtils::get_extract_schema_id(
                                       exec_tenant_id, database_schema.get_database_id())))
          || OB_FAIL(dml.add_column("database_name", ObHexEscapeSqlStr(database_schema.get_database_name_str())))
          || OB_FAIL(dml.add_column(OBJ_GET_K(database_schema, collation_type)))
          || OB_FAIL(dml.add_column("read_only", database_schema.is_read_only()))
          || OB_FAIL(dml.add_column("default_tablegroup_id", ObSchemaUtils::get_extract_schema_id(
                                    exec_tenant_id, database_schema.get_default_tablegroup_id())))
          || OB_FAIL(dml.add_column("in_recyclebin", database_schema.is_in_recyclebin()))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      }
    }
    ObDMLExecHelper exec(sql_client, exec_tenant_id);

    // insert into __all_database
    if (FAILEDx(exec.exec_update(OB_ALL_DATABASE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // insert into __all_database_history
    const int64_t is_deleted = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.add_pk_column(OBJ_GET_K(database_schema, schema_version)))
          || OB_FAIL(dml.add_column("is_deleted", is_deleted))
          || OB_FAIL(dml.add_column("comment", database_schema.get_comment()))) {
        LOG_WARN("add column failed", K(ret));
      }
    }

    if (FAILEDx(exec.exec_replace(OB_ALL_DATABASE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }

    // log operations
    if (OB_SUCC(ret)) {
      ObSchemaOperation alter_db_op;
      alter_db_op.tenant_id_ = database_schema.get_tenant_id();
      alter_db_op.database_id_ = database_schema.get_database_id();
      alter_db_op.tablegroup_id_ = 0;
      alter_db_op.table_id_ = 0;
      alter_db_op.op_type_ = op_type;
      alter_db_op.schema_version_ = database_schema.get_schema_version();
      alter_db_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(alter_db_op, sql_client))) {
        SHARE_SCHEMA_LOG(WARN, "log update database ddl operation failed", K(alter_db_op), K(ret));
      }
    }
  }
  return ret;
}

int ObDatabaseSqlService::delete_database(const ObDatabaseSchema &db_schema,
                                          const int64_t new_schema_version,
                                          common::ObISQLClient &sql_client,
                                          const ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t IS_DELETED = 1;
  const uint64_t tenant_id = db_schema.get_tenant_id();
  const uint64_t database_id = db_schema.get_database_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  // delete from __all_database
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND database_id = %lu",
                            OB_ALL_DATABASE_TNAME,
                            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id)))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
  } else {
    // mark delete in __all_database_history
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, database_id, schema_version, is_deleted) "
                               "VALUES(%lu, %lu, %ld, %ld)",
                               OB_ALL_DATABASE_HISTORY_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id),
                               new_schema_version, IS_DELETED))) {
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows is expected to one", K(affected_rows), K(ret));
    }
  }

  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation delete_db_op;
    delete_db_op.tenant_id_ = tenant_id;
    delete_db_op.database_id_ = database_id;
    delete_db_op.tablegroup_id_ = 0;
    delete_db_op.table_id_ = 0;
    delete_db_op.schema_version_ = new_schema_version;
    delete_db_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    delete_db_op.op_type_ = OB_DDL_DEL_DATABASE;
    if (OB_FAIL(log_operation(delete_db_op, sql_client))) {
      LOG_WARN("log delete database ddl operation failed", K(delete_db_op), K(ret));
    }
  }

  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase

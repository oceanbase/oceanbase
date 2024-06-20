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
#include "ob_priv_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_fast_convert.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_user_sql_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_sql_client_decorator.h"
#include "sql/resolver/ob_schema_checker.h"
#include "share/ob_max_id_fetcher.h" // ObMaxIdFetcher
#include "sql/ob_sql_utils.h"


namespace oceanbase
{
using namespace common;
using namespace sql;
namespace share
{
namespace schema
{

int ObPrivSqlService::grant_database(
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const common::ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!db_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(db_priv_key), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(gen_db_priv_dml(exec_tenant_id, db_priv_key, priv_set, dml))) {
      LOG_WARN("gen_db_priv_dml failed", K(db_priv_key), K(priv_set), K(ret));
    }

    // insert into __all_database_privilege
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(exec.exec_delete(OB_ALL_DATABASE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_delete failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one or two", K(affected_rows), K(ret));
      }
    }

    // insert into __all_database_privilege_history
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
        }
      } else {
        if (OB_FAIL(add_db_priv_history(db_priv_key, priv_set, new_schema_version, sql_client))) {
          LOG_WARN("add_db_priv_history failed", K(db_priv_key), K(new_schema_version), K(ret));
        }
      }
    }

    // log operations
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = db_priv_key.tenant_id_;
      priv_operation.user_id_ = db_priv_key.user_id_;
      priv_operation.database_name_ = db_priv_key.db_;
      priv_operation.op_type_ = (is_deleted ?
          OB_DDL_DEL_DB_PRIV : OB_DDL_GRANT_REVOKE_DB);
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObPrivSqlService::insert_objauth(
    const uint64_t exec_tenant_id,
    const ObObjPrivSortKey &obj_priv_key,
    const int64_t new_schema_version,
    const share::ObRawObjPrivArray &obj_priv_array,
    const bool is_deleted,
    const uint64_t option,
    ObDMLExecHelper &exec,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  share::ObRawObjPriv raw_obj_priv;

  if (obj_priv_array.count() > 0) {
    ARRAY_FOREACH(obj_priv_array, idx) {
      raw_obj_priv = obj_priv_array.at(idx);
      dml.reset();
      // insert into __all_objauth
      OZ (gen_obj_priv_dml_ora(exec_tenant_id, obj_priv_key, raw_obj_priv,
                               option, dml, is_deleted));
      if (is_deleted) {
        OZ (exec.exec_delete(OB_ALL_TENANT_OBJAUTH_TNAME, dml, affected_rows));
      } else {
        OZ (exec.exec_replace(OB_ALL_TENANT_OBJAUTH_TNAME, dml, affected_rows));
      }
      if (OB_FAIL(ret)) {
      } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        if (is_deleted) {
          ret = OB_SEARCH_NOT_FOUND;
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("affected_rows unexpected to be one ", K(affected_rows), K(ret));
      }
      // insert into __all_objauth_history
      if (OB_SUCC(ret)) {
        OZ (dml.add_pk_column("schema_version", new_schema_version));
        OZ (dml.add_column("is_deleted", is_deleted));
        OZ (exec.exec_insert(OB_ALL_TENANT_OBJAUTH_HISTORY_TNAME,
                            dml,
                            affected_rows));
        if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
        }
      }
    }
  }
  return ret;
}

/*
 * Only deal with Authorization operations of single object.
 * obj_priv_key : priv key of obj
 * obj_priv_array + option: new privs of obj
 * new_schema_version_ora : used for new schemas
 */
int ObPrivSqlService::grant_table(
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const share::ObRawObjPrivArray &obj_priv_array,
    const uint64_t option,
    const ObObjPrivSortKey &obj_priv_key,
    const int64_t new_schema_version_ora,
    const bool is_grant,
    bool is_revoke_all_ora)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(table_priv_key), K(ret));
  } else if ((obj_priv_array.count()) > 0 && !obj_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_priv_key), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(gen_table_priv_dml(exec_tenant_id, table_priv_key, priv_set, dml))) {
      LOG_WARN("gen_table_priv_dml failed", K(table_priv_key), K(priv_set), K(ret));
    }
    // insert into __all_table_privilege
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(exec.exec_delete(OB_ALL_TABLE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_delete failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_replace(OB_ALL_TABLE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one ", K(affected_rows), K(ret));
      }
    }

    // insert into __all_table_privilege_history
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_replace(OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
        }
      } else {
        if (OB_FAIL(add_table_priv_history(table_priv_key, priv_set,
                                           new_schema_version, sql_client))) {
          LOG_WARN("add_table_priv_history failed", K(table_priv_key),
            K(priv_set), K(new_schema_version), K(ret));
        }
      }
    }

    // insert into __all_objauth_privileges
    OZ (insert_objauth(exec_tenant_id,
                       obj_priv_key,
                       new_schema_version_ora,
                       obj_priv_array,
                       !is_grant,
                       option,
                       exec,
                       dml),
        exec_tenant_id, obj_priv_array);

    //log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = table_priv_key.tenant_id_;
      priv_operation.user_id_ = table_priv_key.user_id_;
      priv_operation.database_name_ = table_priv_key.db_;
      priv_operation.table_name_ = table_priv_key.table_;
      priv_operation.op_type_ = (is_deleted ?
          OB_DDL_DEL_TABLE_PRIV : OB_DDL_GRANT_REVOKE_TABLE);
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
    // log oracle objauth opertion
    if (OB_SUCC(ret) && obj_priv_array.count() > 0) {
      OZ (log_obj_priv_operation(obj_priv_key,
                                new_schema_version_ora,
                                is_revoke_all_ora ?
                                  OB_DDL_OBJ_PRIV_DELETE : OB_DDL_OBJ_PRIV_GRANT_REVOKE,
                                NULL,
                                sql_client));
    }
  }
  return ret;
}


int ObPrivSqlService::grant_routine(
    const ObRoutinePrivSortKey &routine_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const uint64_t option,
    const bool is_grant)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = routine_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (!ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("grant/revoke routine priv is not suppported when tenant's data version is below 4.3.1.0 or 4.2.2.0", KR(ret));
  } else if (!routine_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(routine_priv_key), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(gen_routine_priv_dml(exec_tenant_id, routine_priv_key, priv_set, dml))) {
      LOG_WARN("gen_routine_priv_dml failed", K(routine_priv_key), K(priv_set), K(ret));
    }
    // insert into __all_routine_privilege
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(exec.exec_delete(OB_ALL_ROUTINE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_delete failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_replace(OB_ALL_ROUTINE_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one ", K(affected_rows), K(ret));
      }
    }

    // insert into __all_routine_privilege_history
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
          || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("exec_replace failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
      }
    }

    //log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = routine_priv_key.tenant_id_;
      priv_operation.user_id_ = routine_priv_key.user_id_;
      priv_operation.database_name_ = routine_priv_key.db_;
      priv_operation.routine_name_ = routine_priv_key.routine_;
      priv_operation.routine_type_ = routine_priv_key.routine_type_;
      priv_operation.op_type_ = (is_deleted ?
          OB_DDL_DEL_ROUTINE_PRIV : OB_DDL_GRANT_ROUTINE_PRIV);
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObPrivSqlService::grant_column(
    const ObColumnPrivSortKey &column_priv_key,
    uint64_t column_priv_id,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const bool is_grant)
{
  int ret = OB_SUCCESS;
  const bool is_deleted = priv_set == 0;
  const uint64_t tenant_id = column_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(tenant_id))) {
    LOG_WARN("grant/revoke column priv is not suppported", KR(ret));
  } else if (!column_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(column_priv_key), K(ret));
  } else if (OB_INVALID_ID == column_priv_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column priv id is invalid", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    uint64_t new_column_priv_id = OB_INVALID_ID;
    if (OB_FAIL(gen_column_priv_dml(exec_tenant_id, column_priv_key, column_priv_id, priv_set, dml))) {
      LOG_WARN("gen_column_priv_dml failed", K(column_priv_key), K(priv_set), K(ret));
    }
    // insert into __all_column_privilege
    if (OB_SUCC(ret)) {
      if (is_deleted) {
        if (OB_FAIL(exec.exec_delete(OB_ALL_COLUMN_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_delete failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_replace(OB_ALL_COLUMN_PRIVILEGE_TNAME, dml, affected_rows))) {
          LOG_WARN("exec_replace failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one ", K(affected_rows), K(ret));
      }
    }

    // insert into __all_column_privilege_history
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
          || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("exec_replace failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
      }
    }
    //log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.tenant_id_ = column_priv_key.tenant_id_;
      priv_operation.column_priv_id_ = column_priv_id;
      priv_operation.op_type_ = is_deleted ?
          OB_DDL_DEL_COLUMN_PRIV : OB_DDL_GRANT_COLUMN_PRIV;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObPrivSqlService::gen_column_priv_dml(
    const uint64_t exec_tenant_id,
    const ObColumnPrivSortKey &column_priv_key,
    const uint64_t priv_id,
    const ObPrivSet &priv_set,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t all_priv = 0;
  if (OB_FAIL(ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(exec_tenant_id))) {
    LOG_WARN("all column priv is not suppported", KR(ret));
  } else {
    if ((priv_set & OB_PRIV_SELECT) != 0) { all_priv |= 1; }
    if ((priv_set & OB_PRIV_INSERT) != 0) { all_priv |= 2; }
    if ((priv_set & OB_PRIV_UPDATE) != 0) { all_priv |= 4; }
    if ((priv_set & OB_PRIV_REFERENCES) != 0) { all_priv |= 8; }
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
        || OB_FAIL(dml.add_pk_column("user_id", column_priv_key.user_id_))
        || OB_FAIL(dml.add_pk_column("priv_id", priv_id))
        || OB_FAIL(dml.add_column("database_name", column_priv_key.db_))
        || OB_FAIL(dml.add_column("table_name", column_priv_key.table_))
        || OB_FAIL(dml.add_column("column_name", column_priv_key.column_))
        || OB_FAIL(dml.add_column("all_priv", all_priv))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  return ret;
}

int ObPrivSqlService::revoke_routine(
    const ObRoutinePrivSortKey &routine_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  return grant_routine(routine_priv_key,
                     priv_set,
                     new_schema_version,
                     ddl_stmt_str,
                     sql_client,
                     NO_OPTION,
                     false);
}


/*
 * This function should by called when grant only oracle related privs.
 * Only deal with Authorization operations of single object.
 * obj_priv_key : priv key of obj
 * obj_priv_array + option: new privs of obj
 * new_schema_version_ora : used for new schemas
 */
int ObPrivSqlService::grant_table_ora_only(
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const share::ObRawObjPrivArray &obj_priv_array,
    const uint64_t option,
    const ObObjPrivSortKey &obj_priv_key,
    const int64_t new_schema_version_ora,
    const bool is_deleted,
    bool is_delete_all)
{
  int ret = OB_SUCCESS;
  /* xinqi to do: set is_delete when executing revoke stmt */
  const uint64_t tenant_id = obj_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!obj_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(obj_priv_key), K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    // insert into __all_objauth_privileges
    OZ (insert_objauth(exec_tenant_id,
                       obj_priv_key,
                       new_schema_version_ora,
                       obj_priv_array,
                       is_deleted,
                       option,
                       exec,
                       dml),
        exec_tenant_id, obj_priv_array);

    // log oracle objauth opertion
    if (OB_SUCC(ret) && obj_priv_array.count() > 0) {
      OZ (log_obj_priv_operation(obj_priv_key,
                                 new_schema_version_ora,
                                 is_delete_all ?
                                   OB_DDL_OBJ_PRIV_DELETE : OB_DDL_OBJ_PRIV_GRANT_REVOKE,
                                 ddl_stmt_str,
                                 sql_client));
    }
  }
  return ret;
}

int ObPrivSqlService::revoke_database(
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  return grant_database(db_priv_key, priv_set, new_schema_version, ddl_stmt_str, sql_client);
}

int ObPrivSqlService::revoke_table(
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet priv_set,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    const int64_t new_schema_version_ora,
    const ObObjPrivSortKey &obj_priv_key,
    const share::ObRawObjPrivArray &obj_priv_array,
    bool is_revoke_all)
{
  return grant_table(table_priv_key,
                     priv_set,
                     new_schema_version,
                     ddl_stmt_str,
                     sql_client,
                     obj_priv_array,
                     NO_OPTION,
                     obj_priv_key,
                     new_schema_version_ora,
                     false,
                     is_revoke_all);
}

int ObPrivSqlService::revoke_table_ora(
    const ObObjPrivSortKey &obj_priv_key,
    const share::ObRawObjPrivArray &obj_priv_array,
    const int64_t new_schema_version,
    const common::ObString *ddl_stmt_str,
    ObISQLClient &sql_client,
    bool is_revoke_all)
{
  return grant_table_ora_only(ddl_stmt_str,
                              sql_client,
                              obj_priv_array,
                              GRANT_OPTION,
                              obj_priv_key,
                              new_schema_version,
                              true,
                              is_revoke_all);
}

int ObPrivSqlService::delete_db_priv(
    const ObOriginalDBKey &org_db_key,
    const int64_t new_schema_version,
    common::ObISQLClient &sql_client,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = org_db_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!org_db_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(org_db_key), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("TENANT_ID", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, org_db_key.tenant_id_)))
        || OB_FAIL(dml.add_pk_column("USER_ID", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, org_db_key.user_id_)))
        || OB_FAIL(dml.add_pk_column("DATABASE_NAME", ObHexEscapeSqlStr(org_db_key.db_)))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // delete from __all_database_privilege
    if (FAILEDx(exec.exec_delete(
                   OB_ALL_DATABASE_PRIVILEGE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      //for mysql, if db name and table name is case sensitive,
      //then for a privilege on t1 and T1 should exist 2 records in the inner table.
      //but the key of the inner table is tenant_id, user_id and database_name
      //the database_name is varchar, and its charset is utf8_general_ci(insensitive).
      //so the record number could only be one.
      //here we bypass now, should fix the bug, then delete this code.
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      bool is_oracle_mode = false;
      if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                tenant_id, is_oracle_mode))) {
        LOG_WARN("fail to check is oracle mode", K(ret));
      } else if (is_oracle_mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_name_case_mode(tenant_id, mode))) {
        LOG_WARN("fail to get tenant name case mode", K(tenant_id), K(ret));
      } else if (mode != OB_ORIGIN_AND_SENSITIVE) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
      } else {
        //by pass
      }
    }

    // mark delete in __all_dtabase_privilege_history
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml.add_pk_column("SCHEMA_VERSION", new_schema_version))
        || OB_FAIL(dml.add_column("IS_DELETED", 1))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = org_db_key.tenant_id_;
      priv_operation.user_id_ = org_db_key.user_id_;
      priv_operation.database_name_ = org_db_key.db_;
      priv_operation.op_type_ = OB_DDL_DEL_DB_PRIV;
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }

  return ret;
}

int ObPrivSqlService::delete_table_priv(
    const ObTablePrivSortKey &table_priv_key,
    const int64_t new_schema_version,
    ObISQLClient &sql_client,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!table_priv_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(table_priv_key), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("TENANT_ID", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table_priv_key.tenant_id_)))
        || OB_FAIL(dml.add_pk_column("USER_ID", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, table_priv_key.user_id_)))
        || OB_FAIL(dml.add_pk_column("DATABASE_NAME", ObHexEscapeSqlStr(table_priv_key.db_)))
        || OB_FAIL(dml.add_pk_column("TABLE_NAME", ObHexEscapeSqlStr(table_priv_key.table_)))
        || OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add column failed", K(ret));
    }

    // delete from __all_table_privilege
    if (FAILEDx(exec.exec_delete(
                   OB_ALL_TABLE_PRIVILEGE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
        //for mysql, if db name and table name is case sensitive,
      //then for a privilege on t1 and T1 should exist 2 records in the inner table.
      //but the key of the inner table is tenant_id, user_id and database_name, table_name
      //the database_name and table_name is varchar, and its charset is utf8_general_ci(insensitive).
      //so the records number could be only exist one.
      //here we bypass now, should fix the bug, then delete this code.
      ObNameCaseMode mode = OB_NAME_CASE_INVALID;
      bool is_oracle_mode = false;
      if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                tenant_id, is_oracle_mode))) {
        LOG_WARN("fail to check is oracle mode", K(ret));
      } else if (is_oracle_mode) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_name_case_mode(tenant_id, mode))) {
        LOG_WARN("fail to get tenant name case mode", K(tenant_id), K(ret));
      } else if (mode != OB_ORIGIN_AND_SENSITIVE) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
      } else {
        // by pass
      }
    }

    // mark delete in __all_table_privilege_history
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml.add_pk_column("SCHEMA_VERSION", new_schema_version))
        || OB_FAIL(dml.add_column("IS_DELETED", 1))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expect to 1", K(affected_rows), K(ret));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation priv_operation;
      priv_operation.schema_version_ = new_schema_version;
      priv_operation.tenant_id_ = table_priv_key.tenant_id_;
      priv_operation.user_id_ = table_priv_key.user_id_;
      priv_operation.database_name_ = table_priv_key.db_;
      priv_operation.table_name_ = table_priv_key.table_;
      priv_operation.op_type_ = OB_DDL_DEL_TABLE_PRIV;
      if (OB_FAIL(log_operation(priv_operation, sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObPrivSqlService::log_obj_priv_operation(
    const ObObjPrivSortKey &obj_priv_key,
    const int64_t new_schema_version,
    const ObSchemaOperationType op_type,
    const ObString *ddl_stmt_str,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSchemaOperation priv_operation;
  priv_operation.schema_version_ = new_schema_version;
  priv_operation.tenant_id_ = obj_priv_key.tenant_id_;
  priv_operation.set_obj_id(obj_priv_key.obj_id_);
  priv_operation.set_grantee_id(obj_priv_key.grantee_id_);
  priv_operation.set_grantor_id(obj_priv_key.grantor_id_);
  priv_operation.set_col_id(obj_priv_key.col_id_);
  ObFastFormatInt ff(obj_priv_key.obj_type_);
  priv_operation.table_name_ = ObString(ff.length(), ff.ptr());
  priv_operation.op_type_ = op_type;
  priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();

  if (OB_FAIL(log_operation(priv_operation, sql_client))) {
    LOG_WARN("Failed to log operation", K(ret));
  }
  return ret;
}

/*
 * remove all the privs of obj granted by grantor/grantee
 * eg:grant select, update on t1 to u1
 */
int ObPrivSqlService::delete_obj_priv(
    const ObObjPriv &obj_priv,
    const int64_t new_schema_version,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = obj_priv.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!obj_priv.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(obj_priv), K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObDMLSqlSplicer dml;
    ObRawObjPrivArray raw_priv_array;
    /* obj_priv map to raw_priv_array */
    OZ (ObPrivPacker::raw_obj_priv_from_pack(obj_priv.get_obj_privs(), raw_priv_array));

    /* insert objauth and objauth_history*/
    OZ (insert_objauth(exec_tenant_id,
                       obj_priv.get_sort_key(),
                       new_schema_version,
                       raw_priv_array,
                       true,
                       NO_OPTION,
                       exec,
                       dml));

    OZ (log_obj_priv_operation(obj_priv.get_sort_key(),
                               new_schema_version,
                               OB_DDL_OBJ_PRIV_DELETE,
                               NULL,
                               sql_client));
  }

  return ret;
}

int ObPrivSqlService::add_db_priv_history(
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet &priv_set,
    const int64_t schema_version,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = db_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  LOG_INFO("for test", K(priv_set));
  if (OB_FAIL(gen_db_priv_dml(exec_tenant_id, db_priv_key, priv_set, dml))) {
    LOG_WARN("gen_db_dml failed", K(db_priv_key), K(ret));
  } else {
    const int64_t is_deleted = 0;
    if (OB_FAIL(dml.add_pk_column("schema_version", schema_version))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    }  else if (OB_FAIL(exec.exec_replace(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME,
            dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObPrivSqlService::add_table_priv_history(
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet &priv_set,
    const int64_t schema_version,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_priv_key.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (OB_FAIL(gen_table_priv_dml(exec_tenant_id, table_priv_key, priv_set, dml))) {
    LOG_WARN("gen_table_priv_dml failed", K(table_priv_key), K(ret));
  } else {
    const int64_t is_deleted = 0;
    if (OB_FAIL(dml.add_pk_column("schema_version", schema_version))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME,
        dml, affected_rows))) {
      LOG_WARN("execute update sql fail", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObPrivSqlService::gen_obj_priv_dml_ora(
    const uint64_t exec_tenant_id,
    const ObObjPrivSortKey &obj_priv_key,
    share::ObRawObjPriv &raw_obj_priv,
    const uint64_t option,
    ObDMLSqlSplicer &dml,
    bool is_deleted)
{
  int ret = OB_SUCCESS;
  OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                              exec_tenant_id, obj_priv_key.tenant_id_)));
  OZ (dml.add_pk_column("obj_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, obj_priv_key.obj_id_)));
  OZ (dml.add_pk_column("objtype", obj_priv_key.obj_type_));
  OZ (dml.add_pk_column("col_id", obj_priv_key.col_id_));
  OZ (dml.add_pk_column("grantor_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, obj_priv_key.grantor_id_)));
  OZ (dml.add_pk_column("grantee_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, obj_priv_key.grantee_id_)));
  OZ (dml.add_pk_column("priv_id", raw_obj_priv));
  if (!is_deleted) {
    OZ (dml.add_column("priv_option", option));
  }
  OZ (dml.add_gmt_modified());
  return ret;
}

int ObPrivSqlService::gen_table_priv_dml(
    const uint64_t exec_tenant_id,
    const ObTablePrivSortKey &table_priv_key,
    const ObPrivSet &priv_set,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, table_priv_key.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, table_priv_key.user_id_)))
      || OB_FAIL(dml.add_pk_column("database_name", ObHexEscapeSqlStr(table_priv_key.db_)))
      || OB_FAIL(dml.add_pk_column("table_name", ObHexEscapeSqlStr(table_priv_key.table_)))
      || OB_FAIL(dml.add_column("PRIV_ALTER", priv_set & OB_PRIV_ALTER ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE", priv_set & OB_PRIV_CREATE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DELETE", priv_set & OB_PRIV_DELETE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DROP", priv_set & OB_PRIV_DROP ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_GRANT_OPTION", priv_set & OB_PRIV_GRANT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INSERT", priv_set & OB_PRIV_INSERT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_UPDATE", priv_set & OB_PRIV_UPDATE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SELECT", priv_set & OB_PRIV_SELECT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INDEX", priv_set & OB_PRIV_INDEX ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_VIEW", priv_set & OB_PRIV_CREATE_VIEW ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SHOW_VIEW", priv_set & OB_PRIV_SHOW_VIEW ? 1 : 0))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObPrivSqlService::gen_routine_priv_dml(
    const uint64_t exec_tenant_id,
    const ObRoutinePrivSortKey &routine_priv_key,
    const ObPrivSet &priv_set,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t all_priv = 0;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(exec_tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(exec_tenant_id));
  } else if (!ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("all routine priv is not suppported when tenant's data version is below 4.3.1.0 or 4.2.2.0", KR(ret));
  } else {
    if ((priv_set & OB_PRIV_EXECUTE) != 0) { all_priv |= 1; }
    if ((priv_set & OB_PRIV_ALTER_ROUTINE) != 0) { all_priv |= 2; }
    if ((priv_set & OB_PRIV_GRANT) != 0) { all_priv |= 4; }
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
        || OB_FAIL(dml.add_pk_column("user_id", routine_priv_key.user_id_))
        || OB_FAIL(dml.add_pk_column("database_name", ObHexEscapeSqlStr(routine_priv_key.db_)))
        || OB_FAIL(dml.add_pk_column("routine_name", ObHexEscapeSqlStr(routine_priv_key.routine_)))
        || OB_FAIL(dml.add_pk_column("routine_type", routine_priv_key.routine_type_))
        || OB_FAIL(dml.add_column("all_priv", all_priv))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  return ret;
}

int ObPrivSqlService::gen_db_priv_dml(
    const uint64_t exec_tenant_id,
    const ObOriginalDBKey &db_priv_key,
    const ObPrivSet &priv_set,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_others = 0;
  priv_others |= (priv_set & OB_PRIV_EXECUTE) != 0 ? 1 : 0;
  priv_others |= (priv_set & OB_PRIV_ALTER_ROUTINE) != 0 ? 2 : 0;
  priv_others |= (priv_set & OB_PRIV_CREATE_ROUTINE) != 0 ? 4 : 0;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(exec_tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, db_priv_key.tenant_id_)))
      || OB_FAIL(dml.add_pk_column("user_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, db_priv_key.user_id_)))
      || OB_FAIL(dml.add_pk_column("database_name", ObHexEscapeSqlStr(db_priv_key.db_)))
      || OB_FAIL(dml.add_column("PRIV_ALTER", priv_set & OB_PRIV_ALTER ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE", priv_set & OB_PRIV_CREATE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DELETE", priv_set & OB_PRIV_DELETE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_DROP", priv_set & OB_PRIV_DROP ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_GRANT_OPTION", priv_set & OB_PRIV_GRANT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INSERT", priv_set & OB_PRIV_INSERT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_UPDATE", priv_set & OB_PRIV_UPDATE ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SELECT", priv_set & OB_PRIV_SELECT ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_INDEX", priv_set & OB_PRIV_INDEX ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_CREATE_VIEW", priv_set & OB_PRIV_CREATE_VIEW ? 1 : 0))
      || OB_FAIL(dml.add_column("PRIV_SHOW_VIEW", priv_set & OB_PRIV_SHOW_VIEW ? 1 : 0))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version)) {
    if (priv_others != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("priv others is not suppported when tenant's data version is below 4.3.1.0 or 4.2.2.0", KR(ret));
    }
  } else if (OB_FAIL(dml.add_column("PRIV_OTHERS", priv_others))) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

/* construct dml for all_tenant_sysauth */
int ObPrivSqlService::gen_grant_sys_priv_dml(
    const uint64_t exec_tenant_id,
    const uint64_t tenant_id,
    const uint64_t grantee_id,
    const uint64_t option,
    const ObRawPriv &raw_priv,
    ObDMLSqlSplicer &dml)
{
  uint64_t pure_grantee_id;

  pure_grantee_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, grantee_id);
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id,
                                             tenant_id)))
      || OB_FAIL(dml.add_pk_column("grantee_id", pure_grantee_id))
      || OB_FAIL(dml.add_pk_column("priv_id", raw_priv))
      || OB_FAIL(dml.add_column("priv_option", option))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObPrivSqlService::alter_user_default_role(
    const ObUserInfo &user_info,
    const int64_t new_schema_version,
    const ObString *ddl_stmt_str,
    ObIArray<uint64_t> &role_id_array,
    ObIArray<uint64_t> &disable_flag_array,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = user_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t user_id = user_info.get_user_id();
  uint64_t role_id = OB_INVALID_ID;

  if (!is_valid_tenant_id(tenant_id)
      || !is_valid_id(user_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(tenant_id), K(user_id), K(ret));
  } else {
    ObDMLSqlSplicer dml;
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);

    for (int64_t i = 0; OB_SUCC(ret) && i < role_id_array.count(); i++) {
      dml.reset();

      role_id = role_id_array.at(i);
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                exec_tenant_id, tenant_id)))
          || OB_FAIL(dml.add_pk_column("grantee_id", ObSchemaUtils::get_extract_schema_id(
                                                  exec_tenant_id, user_id)))
          || OB_FAIL(dml.add_pk_column("role_id", ObSchemaUtils::get_extract_schema_id(
                                                  exec_tenant_id, role_id)))
          || OB_FAIL(dml.add_column("disable_flag",disable_flag_array.at(i)))
          || OB_FAIL(dml.add_gmt_modified())) {
        LOG_WARN("add column failed", K(ret));
      }

      // replace __all_tenant_role_grantee_map table
      OZ (exec.exec_update(OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME, dml, affected_rows));
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update should affect only 1 row", K(role_id), K(affected_rows), K(ret));
      }

      // insert __all_tenant_role_grantee_map history table
      OZ (dml.add_pk_column("schema_version", new_schema_version));
      OZ (dml.add_column("is_deleted", 0));
      OZ (exec.exec_insert(OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME, dml, affected_rows));
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update should affect only 1 row", K(role_id), K(affected_rows), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      if (OB_FAIL((schema_service_.get_user_sql_service()).grant_revoke_user(user_info,
          new_schema_version,
          ddl_stmt_str,
          sql_client,
          false))) {
        LOG_WARN("fail to push back", K(ret), K(user_info));
      }
    }
  }

  return ret;
}


int ObPrivSqlService::grant_revoke_role(
    const uint64_t tenant_id,
    const ObUserInfo &user_info,
    const common::ObIArray<uint64_t> &role_ids,
    // Instead of using role_ids, use specified_role_info directly when is not null.
    const ObUserInfo *specified_role_info,
    const int64_t new_schema_version,
    const common::ObString *ddl_stmt_str,
    common::ObISQLClient &sql_client,
    const bool is_grant,
    ObSchemaGetterGuard &schema_guard,
    const uint64_t option)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t grantee_id = user_info.get_user_id();
  const int64_t is_deleted = is_grant ? 0 : 1;
  ObSqlString sql;
  int64_t affected_rows = 0;
  bool is_oracle_mode = false;

  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get is oracle mode", K(ret));
  }

  if (OB_SUCC(ret) && !is_oracle_mode) {
    OZ (ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(tenant_id));
  }

  // __all_tenant_role_grantee_map
  if (OB_FAIL(ret)) {
  } else if (is_grant) {
    // grant role to grantee
    // grant role to user (reentrantly)
    if (OB_FAIL(sql.append_fmt("REPLACE INTO %s VALUES ", OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME))) {
      LOG_WARN("append table name failed, ", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
        const uint64_t role_id = role_ids.at(i);
        if (0 != i) {
          if (OB_FAIL(sql.append_fmt(", "))) {
            LOG_WARN("append sql failed, ", K(ret));
          }
        }
        // gmt_create, gmt_modified
        if (FAILEDx(sql.append_fmt("(now(6), now(6), %lu, %lu, %lu, %lu, %lu)",
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, user_info.get_tenant_id()),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, grantee_id),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, role_id),
            option,
            static_cast<uint64_t>(is_oracle_mode ? 0 : 1)  /* disable flag */))) {
          LOG_WARN("append sql failed, ", K(ret));
        }
      }
      if (FAILEDx(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", sql.ptr(), K(ret));
      }
    }
  } else {
    // revoke role from grantee
    if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE TENANT_ID = %lu and GRANTEE_ID = %lu and ROLE_ID IN (",
        OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, user_info.get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, grantee_id)))) {
      LOG_WARN("append table name failed, ", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
      const uint64_t role_id = role_ids.at(i);
      if (0 != i) {
        if (OB_FAIL(sql.append_fmt(", "))) {
          LOG_WARN("append sql failed, ", K(ret));
        }
      }
      if (FAILEDx(sql.append_fmt("%lu", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, role_id)))) {
        LOG_WARN("append sql failed, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", sql.ptr(), K(ret));
      } else if (role_ids.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is not expected", K(ret), K(affected_rows), K(role_ids.count()));
      }
    }
  }

  // insert into __all_tenant_role_grantee_map_history
  sql.reset();
  if (FAILEDx(sql.append_fmt("INSERT INTO %s VALUES ", OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME))) {
    LOG_WARN("append table name failed, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
      const uint64_t role_id = role_ids.at(i);
      if (0 != i) {
        if (OB_FAIL(sql.append_fmt(", "))) {
          LOG_WARN("append sql failed, ", K(ret));
        }
      }
      if (FAILEDx(sql.append_fmt("(now(6), now(6), %lu, %lu, %lu, %ld, %ld, %lu, %lu)",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, user_info.get_tenant_id()),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, grantee_id),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, role_id),
          new_schema_version,
          is_deleted,
          option,
          static_cast<uint64_t>(is_oracle_mode ? 0 : 1)/* disable flag */))) {
        LOG_WARN("append sql failed, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed,  ", "sql", sql.ptr(), K(ret));
      } else if (role_ids.count() != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is not expected", K(ret), K(affected_rows), K(role_ids.count()));
      }
    }
  }

  // log operations, update user_info
  if (OB_SUCC(ret)) {
    // If specified_role_info is not NULL, this grant sql is used after "create role" automaticlly,
    // therefore we do not log this sql into log_operation
    // to avoid remote machine redo this "grant" by setting ddl_stmt_str NULL
    if (is_grant && OB_NOT_NULL(specified_role_info)) {
      ddl_stmt_str = NULL;
    }
    if (OB_FAIL((schema_service_.get_user_sql_service()).grant_revoke_user(user_info,
        new_schema_version,
        ddl_stmt_str,
        sql_client,
        false))) {
      LOG_WARN("fail to push back", K(ret), K(user_info));
    }
  }
  // log operations, update role_info
  if (OB_SUCC(ret)) {
    common::ObArray<ObUserInfo> role_infos;
    if (is_grant && OB_NOT_NULL(specified_role_info)) {
      // User role_info offered by ddl_operator
      if (OB_FAIL(role_infos.push_back(*specified_role_info))) {
        LOG_WARN("fail to push back", K(ret), K(*specified_role_info));
      }
    } else {
      // Get role_info from schema
      for (int64_t i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
        const uint64_t role_id = role_ids.at(i);
        const ObUserInfo *role= NULL;
        if (FAILEDx(schema_guard.get_user_info(tenant_id, role_id, role))) {
          LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(role_id));
        } else if (NULL == role) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("role is null", K(ret), K(role_id));
        } else if (OB_FAIL(role_infos.push_back(*role))) {
          LOG_WARN("fail to push back", K(ret), KPC(role));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL((schema_service_.get_user_sql_service()).update_user_schema_version(tenant_id,
          role_infos,
          NULL,
          sql_client))) {
        LOG_WARN("Failed to grant or revoke user", K(exec_tenant_id), K(user_info), K(ret));
      }
    }
  }

  return ret;
}

int ObPrivSqlService::grant_sys_priv_to_ur(
    const uint64_t tenant_id,
    const uint64_t grantee_id,
    const uint64_t option,
    const ObRawPrivArray &priv_array,
    const int64_t new_schema_version,
    const common::ObString *ddl_stmt_str,
    common::ObISQLClient &sql_client,
    const bool is_grant,
    const bool is_revoke_all)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  // grant sys priv to grantee
  // grant sys priv to user (reentrantly)
  ObDMLSqlSplicer dml;
  ARRAY_FOREACH(priv_array, idx) {
    dml.reset();
    /*
     * There are three cases for grant:
     * 1. grant
     *      insert __all_tenant_sysauth
     * 2. grant with option
     *      insert __all_tenant_sysauth
     * 3. add addtional option
     *      replace __all_tenant_sysauth
     * There is only one case for revoke.
    */
    dml.reset();
    OZ (gen_grant_sys_priv_dml(exec_tenant_id, tenant_id, grantee_id, option, priv_array.at(idx), dml));
    if (is_grant) {
      OZ (exec.exec_replace(OB_ALL_TENANT_SYSAUTH_TNAME, dml, affected_rows));
    } else {
      OZ (exec.exec_delete(OB_ALL_TENANT_SYSAUTH_TNAME, dml, affected_rows));
    }
    if (OB_SUCC(ret) && !is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one ", K(exec_tenant_id),
               K(grantee_id), K(priv_array.at(idx)), K(affected_rows), K(ret));
    }
    // insert __all_tenant_sysauth_history
    OZ (dml.add_pk_column("schema_version", new_schema_version));
    OZ (dml.add_column("is_deleted", !is_grant));
    OZ (exec.exec_insert(OB_ALL_TENANT_SYSAUTH_HISTORY_TNAME, dml, affected_rows));
    if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one ",
               K(exec_tenant_id), K(grantee_id), K(affected_rows), K(ret));
    }
  }
  // log operations
  if (OB_SUCC(ret)) {
    ObSchemaOperation priv_operation;
    priv_operation.schema_version_ = new_schema_version;
    priv_operation.tenant_id_ = tenant_id;
    priv_operation.grantee_id_ = grantee_id;
    /* user_id reuse to store grantee_id */
    priv_operation.user_id_ = grantee_id;
    if (!is_revoke_all) {
      priv_operation.op_type_ = OB_DDL_SYS_PRIV_GRANT_REVOKE;
    } else {
      priv_operation.op_type_ = OB_DDL_SYS_PRIV_DELETE;
    }
    priv_operation.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(priv_operation, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObPrivSqlService::grant_proxy(const uint64_t tenant_id,
                                  const uint64_t client_user_id,
                                  const uint64_t proxy_user_id,
                                  const uint64_t flags,
                                  const int64_t new_schema_version,
                                  ObISQLClient &sql_client,
                                  const bool is_grant)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  const bool is_deleted = is_grant ? false : true;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (!ObSQLUtils::is_data_version_ge_423_or_432(tenant_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter user grant connect through is not supported when data version is below 4.2.3 or 4.3.2");
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
      || OB_FAIL(dml.add_pk_column("client_user_id", client_user_id))
      || OB_FAIL(dml.add_pk_column("proxy_user_id", proxy_user_id))
      || OB_FAIL(dml.add_column("flags", flags))
      || OB_FAIL(dml.add_column("credential_type", 0))) {
    LOG_WARN("add column failed", K(ret));
  } else if (is_grant) {
    if (OB_FAIL(exec.exec_replace(OB_ALL_USER_PROXY_INFO_TNAME, dml, affected_rows))) {
      LOG_WARN("exec replace failed", K(ret));
    }
  } else {
    if (OB_FAIL(exec.exec_delete(OB_ALL_USER_PROXY_INFO_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_USER_PROXY_INFO_HISTORY_TNAME, dml, affected_rows))) {
    LOG_WARN("exec_replace failed", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
  }
  return ret;
}

int ObPrivSqlService::grant_proxy_role(const uint64_t tenant_id,
                                  const uint64_t client_user_id,
                                  const uint64_t proxy_user_id,
                                  const uint64_t role_id,
                                  const int64_t new_schema_version,
                                  ObISQLClient &sql_client,
                                  const bool is_grant)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, exec_tenant_id);
  ObDMLSqlSplicer dml;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (!ObSQLUtils::is_data_version_ge_423_or_432(tenant_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter user grant connect through is not supported when data version is below 4.2.3 or 4.3.2");
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0))
      || OB_FAIL(dml.add_pk_column("client_user_id", client_user_id))
      || OB_FAIL(dml.add_pk_column("proxy_user_id", proxy_user_id))
      || OB_FAIL(dml.add_pk_column("role_id", role_id))) {
    LOG_WARN("add column failed", K(ret));
  } else if (is_grant) {
    if (OB_FAIL(exec.exec_replace(OB_ALL_USER_PROXY_ROLE_INFO_TNAME, dml, affected_rows))) {
      LOG_WARN("exec replace failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
    }
  } else {
    if (OB_FAIL(exec.exec_delete(OB_ALL_USER_PROXY_ROLE_INFO_TNAME, dml, affected_rows))) {
      LOG_WARN("exec delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("schema_version", new_schema_version))
            || OB_FAIL(dml.add_column("is_deleted", !is_grant))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TNAME, dml, affected_rows))) {
    LOG_WARN("exec_replace failed", K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expeccted to be one", K(affected_rows), K(ret));
  }
  return ret;
}


} //end of schema
} //end of share
} //end of oceanbase

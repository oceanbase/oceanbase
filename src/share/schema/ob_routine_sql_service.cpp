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
#include "ob_routine_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_package_info.h"
#include "ob_routine_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "pl/ob_pl_stmt.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
int ObRoutineSqlService::create_package(ObPackageInfo &package_info,
                                        ObISQLClient *sql_client,
                                        bool is_replace,
                                        const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!package_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "package_info is invalid", K(package_info), K(ret));
  } else {
    if (OB_FAIL(add_package(*sql_client, package_info, is_replace))) {
      LOG_WARN("add package failed", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = package_info.get_tenant_id();
      opt.database_id_ = package_info.get_database_id();
      opt.table_id_ = package_info.get_package_id();
      opt.op_type_ = is_replace ? OB_DDL_ALTER_PACKAGE : OB_DDL_CREATE_PACKAGE;
      opt.schema_version_ = package_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObRoutineSqlService::drop_package(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const uint64_t package_id,
                                      const int64_t new_schema_version,
                                      ObISQLClient &sql_client,
                                      const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == database_id)
      || OB_UNLIKELY(OB_INVALID_ID == package_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid package info in drop procedure", KR(ret), K(tenant_id), K(database_id), K(package_id));
  } else if (OB_FAIL(del_package(sql_client, tenant_id, package_id, new_schema_version))) {
    LOG_WARN("delete package failed", K(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = database_id;
    opt.table_id_ = package_id;
    opt.op_type_ = OB_DDL_DROP_PACKAGE;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObRoutineSqlService::alter_package(const ObPackageInfo &package_info,
                                       common::ObISQLClient *sql_client,
                                       const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!package_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "package_info is invalid", K(package_info), K(ret));
  } else if (OB_FAIL(add_package(*sql_client, package_info, true))) {
    LOG_WARN("add package failed", K(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = package_info.get_tenant_id();
    opt.database_id_ = package_info.get_database_id();
    opt.table_id_ = package_info.get_package_id();
    opt.op_type_ = OB_DDL_ALTER_PACKAGE;
    opt.schema_version_ = package_info.get_schema_version();
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, *sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObRoutineSqlService::add_package(common::ObISQLClient &sql_client,
                                     const ObPackageInfo &package_info,
                                     bool is_replace,
                                     bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = package_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(gen_package_dml(exec_tenant_id, package_info, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      if (is_replace) {
        if (OB_FAIL(exec.exec_update(OB_ALL_PACKAGE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_PACKAGE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_PACKAGE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
   }
   return ret;
}

int ObRoutineSqlService::create_routine(ObRoutineInfo &routine_info,
                                        ObISQLClient *sql_client,
                                        const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!routine_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "routine_info is invalid", K(routine_info), K(ret));
  } else {
    if (OB_FAIL(add_routine(*sql_client, routine_info))) {
      LOG_WARN("add routine failed", K(ret));
    } else if (OB_FAIL(add_routine_params(*sql_client, routine_info))) {
      LOG_WARN("add routine params failed", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = routine_info.get_tenant_id();
      opt.database_id_ = routine_info.get_database_id();
      opt.table_id_ = routine_info.get_routine_id();
      opt.op_type_ = OB_DDL_CREATE_ROUTINE;
      opt.schema_version_ = routine_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObRoutineSqlService::update_routine(ObRoutineInfo &routine_info,
                                        ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(sql_client));
  OV (routine_info.is_valid(), OB_ERR_UNEXPECTED, K(routine_info));
  OZ (add_routine(*sql_client, routine_info, true));
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = routine_info.get_tenant_id();
    opt.database_id_ = routine_info.get_database_id();
    opt.table_id_ = routine_info.get_routine_id();
    opt.op_type_ = OB_DDL_REPLACE_ROUTINE;
    opt.schema_version_ = routine_info.get_schema_version();
    opt.ddl_stmt_str_ = ObString();
    if (OB_FAIL(log_operation(opt, *sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }

  return ret;
}

int ObRoutineSqlService::replace_routine(ObRoutineInfo &routine_info,
                                         const ObRoutineInfo *old_routine_info,
                                         const int64_t del_param_schema_version,
                                         ObISQLClient *sql_client,
                                         const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(sql_client));
  CK(OB_NOT_NULL(old_routine_info));
  if (OB_SUCC(ret)) {
    LOG_WARN("old_routine_info", K(*old_routine_info), K(old_routine_info->get_routine_params().count()));
    if (!routine_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new routine info is invalid", K(routine_info), K(ret));
    } else if (OB_FAIL(add_routine(*sql_client, routine_info, true))) {
      LOG_WARN("add routine failed", K(routine_info), K(ret));
    } else if (old_routine_info->get_routine_params().count() > 0
               && OB_FAIL(del_routine_params(*sql_client, *old_routine_info, del_param_schema_version))) {
      LOG_WARN("del routine params failed", K(routine_info), K(ret));
    } else if (OB_FAIL(add_routine_params(*sql_client, routine_info))) {
      LOG_WARN("add routine params failed", K(routine_info), K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = routine_info.get_tenant_id();
      opt.database_id_ = routine_info.get_database_id();
      opt.table_id_ = routine_info.get_routine_id();
      opt.op_type_ = OB_DDL_REPLACE_ROUTINE;
      opt.schema_version_ = routine_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObRoutineSqlService::drop_routine(const ObRoutineInfo &routine_info,
                                      const int64_t new_schema_version,
                                      ObISQLClient &sql_client,
                                      const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = routine_info.get_tenant_id();
  uint64_t db_id = routine_info.get_database_id();
  uint64_t routine_id = routine_info.get_routine_id();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == db_id)
      || OB_UNLIKELY(OB_INVALID_ID == routine_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid routine info in drop procedure", K(tenant_id), K(db_id), K(routine_id));
  } else if (OB_FAIL(del_routine(sql_client, routine_info, new_schema_version))) {
    LOG_WARN("delete from __all_routine failed", K(ret));
  } else if (routine_info.get_routine_params().count() > 0 && OB_FAIL(del_routine_params(sql_client, routine_info, new_schema_version))) {
    LOG_WARN("delete from __all_routine_param failed", K(ret));
  } else {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = db_id;
    opt.table_id_ = routine_id;
    opt.op_type_ = OB_DDL_DROP_ROUTINE;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObRoutineSqlService::del_package(ObISQLClient &sql_client,
                                     const uint64_t tenant_id,
                                     const uint64_t package_id,
                                     int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("package_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, package_id)))) {
    LOG_WARN("add pk column to __all_package failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_PACKAGE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret));
    }
  }

  if (OB_SUCCESS == ret) {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, package_id, schema_version, is_deleted)"
        " VALUES(%lu,%lu,%ld,%d)",
        OB_ALL_PACKAGE_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, package_id),
        new_schema_version, 1))) {
      LOG_WARN("assign insert into all package history fail", K(package_id), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    }
  }
  return ret;
}

int ObRoutineSqlService::del_routine(ObISQLClient &sql_client,
                                     const ObRoutineInfo &routine_info,
                                     int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t routine_id = routine_info.get_routine_id();
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, routine_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("routine_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, routine_id)))) {
    LOG_WARN("add pk column to __all_routine failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_ROUTINE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(routine_info), K(new_schema_version));
    }
  }

  if (OB_SUCCESS == ret) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    // insert into __all_routine_history
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, routine_id, schema_version, is_deleted) VALUES(%lu,%lu,%ld,%d)",
        OB_ALL_ROUTINE_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, routine_info.get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, routine_id),
        new_schema_version, 1))) {
      LOG_WARN("assign insert into __all_routine_history fail", K(routine_info), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObRoutineSqlService::del_routine_params(ObISQLClient &sql_client,
                                            const ObRoutineInfo &routine_info,
                                            int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t routine_id = routine_info.get_routine_id();
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, routine_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("routine_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, routine_id)))) {
    LOG_WARN("add pk column to __all_routine_param failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_ROUTINE_PARAM_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (affected_rows < routine_info.get_routine_params().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(routine_info),
                K(affected_rows), K(routine_info.get_routine_params().count()));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    ObSqlString sql;
    int64_t affected_rows = 0;

    if (OB_FAIL(sql.append_fmt("INSERT /*+use_plan_cache(none)*/ INTO %s "
        "(tenant_id, routine_id, sequence, schema_version, is_deleted) VALUES ",
        OB_ALL_ROUTINE_PARAM_HISTORY_TNAME))) {
      LOG_WARN("append_fmt failed", K(ret));
    }
    const ObIArray<ObRoutineParam*> &routine_params = routine_info.get_routine_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < routine_params.count(); ++i) {
      const ObRoutineParam *routine_param = NULL;
      if (OB_ISNULL(routine_param = routine_params.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine param is null");
      } else if (OB_FAIL(sql.append_fmt("%s(%lu, %lu, %lu, %lu, %d)", (0 == i) ? "" : ",",
          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, routine_param->get_tenant_id()),
          ObSchemaUtils::get_extract_schema_id(exec_tenant_id, routine_param->get_routine_id()),
          routine_param->get_sequence(),
          new_schema_version, 1))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (routine_params.count() != affected_rows) {
        LOG_WARN("affected_rows not same with routine_param_count", K(affected_rows),
                 "param_count", routine_params.count(), K(ret));
      }
    }
  }

  return ret;
}

int ObRoutineSqlService::gen_package_dml(
    const uint64_t exec_tenant_id,
    const ObPackageInfo &package_info,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, package_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("package_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, package_info.get_package_id())))
      || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, package_info.get_database_id())))
      || OB_FAIL(dml.add_column("package_name", ObHexEscapeSqlStr(package_info.get_package_name())))
      || OB_FAIL(dml.add_column("schema_version", package_info.get_schema_version()))
      || OB_FAIL(dml.add_column("type", package_info.get_type()))
      || OB_FAIL(dml.add_column("flag", package_info.get_flag()))
      || OB_FAIL(dml.add_column("owner_id", ObSchemaUtils::get_extract_schema_id(
                                            exec_tenant_id, package_info.get_owner_id())))
      || OB_FAIL(dml.add_column("comp_flag", package_info.get_comp_flag()))
      || OB_FAIL(dml.add_column("exec_env", ObHexEscapeSqlStr(package_info.get_exec_env())))
      || OB_FAIL(dml.add_column("source", ObHexEscapeSqlStr(package_info.get_source())))
      || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(package_info.get_comment())))
      || OB_FAIL(dml.add_column("route_sql", ObHexEscapeSqlStr(package_info.get_route_sql())))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObRoutineSqlService::gen_routine_dml(
    const uint64_t exec_tenant_id,
    const ObRoutineInfo &routine_info,
    ObDMLSqlSplicer &dml,
    bool is_replace)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, routine_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("routine_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, routine_info.get_routine_id())))
      || OB_FAIL(dml.add_pk_column("package_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, routine_info.get_package_id())))
      || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, routine_info.get_database_id())))
      || OB_FAIL(dml.add_column("routine_name", ObHexEscapeSqlStr(routine_info.get_routine_name())))
      || OB_FAIL(dml.add_column("overload", routine_info.get_overload()))
      || OB_FAIL(dml.add_column("subprogram_id", routine_info.get_subprogram_id()))
      || OB_FAIL(dml.add_column("schema_version", routine_info.get_schema_version()))
      || OB_FAIL(dml.add_column("routine_type", routine_info.get_routine_type()))
      || OB_FAIL(dml.add_column("flag", routine_info.get_flag()))
      || OB_FAIL(dml.add_column("owner_id", ObSchemaUtils::get_extract_schema_id(
                                            exec_tenant_id, routine_info.get_owner_id())))
      || OB_FAIL(dml.add_column("priv_user", ObHexEscapeSqlStr(routine_info.get_priv_user())))
      || OB_FAIL(dml.add_column("comp_flag", routine_info.get_comp_flag()))
      || OB_FAIL(dml.add_column("exec_env", ObHexEscapeSqlStr(routine_info.get_exec_env())))
      || OB_FAIL(dml.add_column("routine_body", ObHexEscapeSqlStr(routine_info.get_routine_body())))
      || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(routine_info.get_comment())))
      || OB_FAIL(dml.add_column("route_sql", ObHexEscapeSqlStr(routine_info.get_route_sql())))) {
    LOG_WARN("add column failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (is_sys_tenant(pl::get_tenant_id_by_object_id(routine_info.get_type_id()))) {
    if (OB_FAIL(dml.add_column("type_id", routine_info.get_type_id()))) {
      LOG_WARN("add column failed", K(ret));
    }
  } else {
    if (OB_FAIL(dml.add_column("type_id", ObSchemaUtils::get_extract_schema_id(
                               exec_tenant_id, routine_info.get_type_id())))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if ((!is_replace && OB_FAIL(dml.add_gmt_create()))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObRoutineSqlService::gen_routine_param_dml(
    const uint64_t exec_tenant_id,
    const ObRoutineParam &routine_param,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  ObString bin_extended_type_info;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  if (ob_is_enum_or_set_type(routine_param.get_param_type().get_obj_type())) {
    char *extended_type_info_buf = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(extended_type_info_buf = static_cast<char *>(allocator.alloc(OB_MAX_VARBINARY_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for extended type info buf failed", K(ret));
    } else if (OB_FAIL(routine_param.serialize_extended_type_info(extended_type_info_buf, OB_MAX_VARBINARY_LENGTH, pos))) {
      LOG_WARN("fail to serialize_extended_type_info", K(ret));
    } else {
      bin_extended_type_info.assign_ptr(extended_type_info_buf, static_cast<int32_t>(pos));
    }
  }
  ObCharsetType charset_type = CS_TYPE_ANY == routine_param.get_param_type().get_collation_type() ?
                               CHARSET_ANY : ObCharset::charset_type_by_coll(
                                             routine_param.get_param_type().get_collation_type());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, routine_param.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("routine_id", ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, routine_param.get_routine_id())))
      || OB_FAIL(dml.add_pk_column("sequence", routine_param.get_sequence()))
      || OB_FAIL(dml.add_column("subprogram_id", routine_param.get_subprogram_id()))
      || OB_FAIL(dml.add_column("param_position", routine_param.get_param_position()))
      || OB_FAIL(dml.add_column("param_level", routine_param.get_param_level()))
      || OB_FAIL(dml.add_column("param_name", ObHexEscapeSqlStr(routine_param.get_param_name())))
      || OB_FAIL(dml.add_column("schema_version", routine_param.get_schema_version()))
      || OB_FAIL(dml.add_column("param_type", routine_param.get_param_type().get_obj_type()))
      || OB_FAIL(dml.add_column("param_length", routine_param.get_param_type().get_length()))
      || OB_FAIL(dml.add_column("param_precision", routine_param.get_param_type().get_precision()))
      || OB_FAIL(dml.add_column("param_scale", routine_param.get_param_type().get_scale()))
      || OB_FAIL(dml.add_column("param_zero_fill", routine_param.get_param_type().is_zero_fill()))
      || OB_FAIL(dml.add_column("param_charset", charset_type))
      || OB_FAIL(dml.add_column("param_coll_type", routine_param.get_param_type().get_collation_type()))
      || OB_FAIL(dml.add_column("flag", routine_param.get_flag()))
      || OB_FAIL(dml.add_column("default_value", ObHexEscapeSqlStr(routine_param.get_default_value())))) {
    LOG_WARN("add column failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (is_sys_database_id(routine_param.get_type_owner())) {
    if (OB_FAIL(dml.add_column("type_owner", routine_param.get_type_owner()))) {
      LOG_WARN("add column failed", K(ret));
    }
  } else {
    if (OB_FAIL(dml.add_column("type_owner", ObSchemaUtils::get_extract_schema_id(
                                             exec_tenant_id, routine_param.get_type_owner())))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_column("type_name", ObHexEscapeSqlStr(routine_param.get_type_name())))
      || OB_FAIL(dml.add_column("type_subname", ObHexEscapeSqlStr(routine_param.get_type_subname())))
      || OB_FAIL(dml.add_column("extended_type_info", ObHexEscapeSqlStr(bin_extended_type_info)))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObRoutineSqlService::add_routine(ObISQLClient &sql_client,
                                     const ObRoutineInfo &routine_info,
                                     bool is_replace,
                                     bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(gen_routine_dml(exec_tenant_id, routine_info, dml, is_replace))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      if (is_replace) {
        if (OB_FAIL(exec.exec_update(OB_ALL_ROUTINE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_ROUTINE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_ROUTINE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObRoutineSqlService::add_routine_params(ObISQLClient &sql_client,
                                            ObRoutineInfo &routine_info,
                                            bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  ObIArray<ObRoutineParam *> &routine_params = routine_info.get_routine_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_params.count(); ++i) {
    ObRoutineParam *routine_param = routine_params.at(i);
    if (OB_ISNULL(routine_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("routine param is null", K(i));
    } else {
      routine_param->set_routine_id(routine_info.get_routine_id());
      routine_param->set_schema_version(routine_info.get_schema_version());
      if (routine_param->is_self_param()) {
        routine_param->set_type_owner(routine_info.get_database_id());
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(gen_routine_param_dml(exec_tenant_id, *routine_param, dml))) {
      LOG_WARN("gen routine param dml failed", K(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (!only_history) {
        ObDMLExecHelper exec(sql_client, exec_tenant_id);
        if (OB_FAIL(exec.exec_insert(OB_ALL_ROUTINE_PARAM_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t is_deleted = 0;
        if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(exec.exec_insert(OB_ALL_ROUTINE_PARAM_HISTORY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
    }
    dml.reset();
  }
  return ret;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase

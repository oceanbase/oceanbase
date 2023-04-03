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
#include "ob_context_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_unit_getter.h"
#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObContextSqlService::insert_context(const ObContextSchema &context_schema,
                                        common::ObISQLClient *sql_client,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL", K(ret));
  } else if (!context_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "context_schema is invalid", K(context_schema.get_namespace()), K(ret));
  } else {
    if (OB_FAIL(add_context(*sql_client, context_schema))) {
      LOG_WARN("failed to add context", K(ret));
    } else {
      ObSchemaOperation opt;
      opt.tenant_id_ = context_schema.get_tenant_id();
      opt.context_id_ = context_schema.get_context_id();
      opt.context_name_ = context_schema.get_namespace();
      opt.op_type_ = OB_DDL_CREATE_CONTEXT;
      opt.schema_version_ = context_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObContextSqlService::alter_context(const ObContextSchema &context_schema,
                                           common::ObISQLClient *sql_client,
                                           const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL", K(ret));
  } else {
    uint64_t tenant_id = context_schema.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    // modify __all_context table
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(*sql_client, exec_tenant_id);
      ObDMLSqlSplicer dml;
      bool is_history = false;
      // udpate __all_context table
      int64_t affected_rows = 0;
      if (OB_FAIL(format_dml_sql(context_schema, dml, is_history))) {
        LOG_WARN("failed to get dml sql", K(ret));
      } else if (FAILEDx(exec.exec_update(OB_ALL_CONTEXT_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update sql fail", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update should affect only 1 row", K(affected_rows), K(ret));
      } else {/*do nothing*/}
    }

    // add to __all_context_object_history table
    if (OB_SUCC(ret)) {
      const bool only_history = true;
      if (OB_FAIL(add_context(*sql_client, context_schema, only_history))) {
        LOG_WARN("add_context failed",
                 K(context_schema.get_namespace()),
                 K(only_history),
                 K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = context_schema.get_tenant_id();
      opt.context_id_ = context_schema.get_context_id();
      opt.context_name_ = context_schema.get_namespace();
      opt.op_type_ = OB_DDL_ALTER_CONTEXT;
      opt.schema_version_ = context_schema.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(opt), K(ret));
      }
    }
  }
  return ret;
}

int ObContextSqlService::delete_context(const uint64_t tenant_id,
                                        const uint64_t context_id,
                                        const ObString &ctx_namespace,
                                        const int64_t new_schema_version,
                                        const ObContextType &type,
                                        common::ObISQLClient *sql_client,
                                        const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const int64_t IS_DELETED = 1;

  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid context info in drop context", K(tenant_id), K(ret));
  } else {
    // insert into __all_context_history
    if (FAILEDx(sql.assign_fmt(
                "INSERT INTO %s(tenant_id, context_id, namespace, schema_version, is_deleted)"
                " VALUES(%lu,%lu,\'%s\',%ld,%ld)",
                OB_ALL_CONTEXT_HISTORY_TNAME,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, context_id),
                ctx_namespace.ptr(),
                new_schema_version, IS_DELETED))) {
      LOG_WARN("assign insert into all tenant context history fail",
               K(tenant_id), K(context_id), K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    }

    // delete from __all_context
    if (FAILEDx(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND context_id=%lu",
                               OB_ALL_CONTEXT_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id, context_id)))) {
      LOG_WARN("append_fmt failed", K(ret));
    } else if (OB_FAIL(sql_client->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(tenant_id), K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row deleted", K(sql), K(affected_rows), K(ret));
    } else {
      LOG_INFO("success delete context schema",
               K(tenant_id),
               K(context_id),
               K(ctx_namespace),
               K(OB_ALL_CONTEXT_TNAME));
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = tenant_id;
      opt.context_id_ = context_id;
      opt.op_type_ = OB_DDL_DROP_CONTEXT;
      opt.context_name_ = ctx_namespace;
      opt.schema_version_ = new_schema_version;
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}


int ObContextSqlService::drop_context(const ObContextSchema &context_schema,
                                      const int64_t new_schema_version,
                                      common::ObISQLClient *sql_client,
                                      bool &need_clean,
                                      const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = context_schema.get_tenant_id();
  const uint64_t context_id = context_schema.get_context_id();
  const ObString &ctx_namespace = context_schema.get_namespace();
  need_clean = false;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql client is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || OB_INVALID_ID == context_id
                         || ctx_namespace.length() <= 0
                         || new_schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid context info in drop context",
             K(context_schema.get_namespace()), K(ret));
  } else if (OB_FAIL(delete_context(tenant_id, context_id, ctx_namespace,
                                    new_schema_version, context_schema.get_context_type(),
                                    sql_client, ddl_stmt_str))) {
    LOG_WARN("failed to delete context", K(context_schema.get_namespace()), K(ret));
  } else {
    need_clean = (ACCESSED_GLOBALLY == context_schema.get_context_type());
  }
  return ret;
}


int ObContextSqlService::add_context(common::ObISQLClient &sql_client,
                                     const ObContextSchema &context_schema,
                                     const bool only_history)
{
  int ret = OB_SUCCESS;
  const char *tname[] = {OB_ALL_CONTEXT_TNAME, OB_ALL_CONTEXT_HISTORY_TNAME};
  const uint64_t tenant_id = context_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    ObDMLSqlSplicer dml;
    bool is_history = (0 == STRCMP(tname[i], OB_ALL_CONTEXT_HISTORY_TNAME));
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (only_history && 0 == STRCMP(tname[i], OB_ALL_CONTEXT_TNAME)) {
      continue;
    } else if (OB_FAIL(format_dml_sql(context_schema, dml, is_history))) {
      LOG_WARN("failed to format dml sql", K(ret));
    } else if (OB_FAIL(exec.exec_insert(tname[i], dml, affected_rows))) {
      LOG_WARN("failed to exec insert", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObContextSqlService::format_dml_sql(const ObContextSchema &context_schema,
                                        ObDMLSqlSplicer &dml,
                                        bool &is_history)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = context_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("context_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, context_schema.get_context_id())))
      || OB_FAIL(dml.add_column("namespace", context_schema.get_namespace()))
      || OB_FAIL(dml.add_column("schema_version", context_schema.get_schema_version()))
      || OB_FAIL(dml.add_column("database_name", context_schema.get_schema_name()))
      || OB_FAIL(dml.add_column("package", context_schema.get_trusted_package()))
      || OB_FAIL(dml.add_column("type", static_cast<int64_t> (context_schema.get_context_type())))
      || OB_FAIL(dml.add_column("origin_con_id", context_schema.get_origin_con_id()))
      || OB_FAIL(dml.add_column("tracking", static_cast<int64_t> (context_schema.get_tracking())))
      || OB_FAIL(dml.add_gmt_modified())
      || (is_history && OB_FAIL(dml.add_column("is_deleted", 0)))) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}


} //end of schema
} //end of share
} //end of oceanbase

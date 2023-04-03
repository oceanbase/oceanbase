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
#include "ob_sys_variable_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
int ObSysVariableSqlService::replace_sys_variable(
    ObSysVariableSchema &sys_variable_schema,
    ObISQLClient &sql_client,
    const ObSchemaOperationType &operation_type,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!sys_variable_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys variable schema", K(sys_variable_schema), K(ret));
  } else if (sys_variable_schema.get_real_sysvar_count() > 0) {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    ObDMLSqlSplicer dml_for_history;
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema.get_sysvar_count(); ++i) {
      ObSysVarSchema *sysvar_schema = sys_variable_schema.get_sysvar_schema(i);
      if (sysvar_schema != NULL) {
        sysvar_schema->set_schema_version(sys_variable_schema.get_schema_version());
        if (OB_FAIL(gen_sys_variable_dml(dml, *sysvar_schema, false/*is_history*/))) {
          LOG_WARN("fail to gen sys variable dml", K(ret));
        } else if (OB_FAIL(dml.finish_row())) {
          LOG_WARN("fail to finish row", K(ret));
        } else if (OB_FAIL(gen_sys_variable_dml(dml_for_history, *sysvar_schema, true/*is_history*/))) {
          LOG_WARN("fail to gen sys variable dml", K(ret));
        } else if (OB_FAIL(dml_for_history.finish_row())) {
          LOG_WARN("fail to finish row", K(ret));
        }
      }
    }
    // batch replace __all_sys_variable
    if (OB_SUCC(ret)) {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_batch_insert_update_sql(OB_ALL_SYS_VARIABLE_TNAME, sql))) {
        LOG_WARN("splice sql failed", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      }
    }
    // batch insert __all_sys_variable_history
    if (OB_SUCC(ret)) {
      ObSqlString sql;
      if (OB_FAIL(dml_for_history.splice_batch_insert_sql(OB_ALL_SYS_VARIABLE_HISTORY_TNAME, sql))) {
        LOG_WARN("splice sql failed", K(ret));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      }
    }
    // It won't log ddl_operation while create sys tenant.
    if (OB_SUCC(ret) && OB_DDL_MAX_OP != operation_type) {
      // log operation
      ObSchemaOperation op;
      op.tenant_id_ = sys_variable_schema.get_tenant_id();
      op.op_type_ = operation_type;
      op.schema_version_ = sys_variable_schema.get_schema_version();
      op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(op, sql_client))) {
        LOG_WARN("log add sysvar schema ddl operation failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSysVariableSqlService::replace_system_variable(
    const ObSysVarSchema &sysvar_schema,
    const ObSchemaOperationType &op,
    ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_DDL_ADD_TENANT != op && OB_DDL_ALTER_TENANT != op && OB_DDL_ALTER_SYS_VAR != op) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace tenant op", K(op), K(ret));
  } else if (OB_FAIL(replace_system_variable(sysvar_schema, sql_client))) {
    LOG_WARN("replace system variable failed", K(ret));
  } else {
    // log operation
    ObSchemaOperation tenant_op;
    tenant_op.tenant_id_ = sysvar_schema.get_tenant_id();
    tenant_op.op_type_ = op;
    tenant_op.schema_version_ = sysvar_schema.get_schema_version();
    tenant_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(tenant_op, sql_client))) {
      LOG_WARN("log add sysvar schema ddl operation failed", K(ret));
    }
  }
  return ret;
}

int ObSysVariableSqlService::replace_system_variable(
    const ObSysVarSchema &sysvar_schema,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sysvar_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!sysvar_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sysvar_schema is invalid", K(sysvar_schema), K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    // insert into __all_sys_variable
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      bool is_history = false;
      if (OB_FAIL(gen_sys_variable_dml(dml, sysvar_schema, is_history))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_SYS_VARIABLE_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (0 != affected_rows && 1 != affected_rows && 2 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
      } else {
        OB_LOG(INFO, "replace sysvar schema success", K(affected_rows), K(sysvar_schema));
      }
    }

    // insert into __all_sys_variable_history
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      bool is_history = true;
      dml.reset();
      if (OB_FAIL(gen_sys_variable_dml(dml, sysvar_schema, is_history))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_SYS_VARIABLE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (0 != affected_rows && 1 != affected_rows && 2 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObSysVariableSqlService::gen_sys_variable_dml(ObDMLSqlSplicer &dml, const ObSysVarSchema &sysvar_schema, bool is_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sysvar_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
#define FORMAT_STR(str) (str.empty() ? ObString("") : str)
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                exec_tenant_id, sysvar_schema.get_tenant_id())))
            || OB_FAIL(dml.add_pk_column("name", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_name()))))
            || OB_FAIL(dml.add_pk_column("zone", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_zone().str()))))
            || OB_FAIL(dml.add_column("data_type", sysvar_schema.get_data_type()))
            || OB_FAIL(dml.add_column("value", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_value()))))
            || OB_FAIL(dml.add_column("min_val", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_min_val()))))
            || OB_FAIL(dml.add_column("max_val", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_max_val()))))
            || OB_FAIL(dml.add_column("info", ObHexEscapeSqlStr(FORMAT_STR(sysvar_schema.get_info()))))
            || OB_FAIL(dml.add_column("flags", sysvar_schema.get_flags()))
            || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  } else if (is_history) {
    const int64_t is_deleted = 0;
    if (OB_FAIL(dml.add_pk_column("schema_version", sysvar_schema.get_schema_version()))
        || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
      LOG_WARN("add column failed", K(ret));
    }
  }
#undef FORMAT_STR
  return ret;
}
} //end of schema
} //end of share
} //end of oceanbase

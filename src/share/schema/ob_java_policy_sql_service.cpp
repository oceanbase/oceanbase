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

#include "share/schema/ob_java_policy_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_java_policy_mgr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

int ObJavaPolicySqlService::create_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                                               const common::ObString &ddl_stmt_str,
                                               common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = java_policy_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!java_policy_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(java_policy_schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_pk_column("`key`", java_policy_schema.get_key()))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_column("kind", static_cast<int64_t>(java_policy_schema.get_kind())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("grantee", java_policy_schema.get_grantee()))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("type_schema", java_policy_schema.get_type_schema()))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("type_name", ObHexEscapeSqlStr(java_policy_schema.get_type_name())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(java_policy_schema.get_name())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("action", ObHexEscapeSqlStr(java_policy_schema.get_action())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_column("status", static_cast<int64_t>(java_policy_schema.get_status())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_JAVA_POLICY_TNAME, buffer))) {
    LOG_WARN("fail to splice insert sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else if (OB_FAIL(sql.add_pk_column("schema_version", java_policy_schema.get_schema_version()))) {
      LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_column("is_deleted", 0))) {
      LOG_WARN("fail to add column", K(ret));
  } else if (OB_FALSE_IT(buffer.reuse())) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_JAVA_POLICY_HISTORY_TNAME, buffer))) {
      LOG_WARN("fail to splice insert sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
      LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
  } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  }

  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = java_policy_schema.get_type_schema();
    opt.java_policy_id_ = java_policy_schema.get_key();
    opt.op_type_ = OB_DDL_CREATE_JAVA_POLICY;
    opt.schema_version_ = java_policy_schema.get_schema_version();
    opt.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("log operation failed", K(ret));
    }
  }

  return ret;
}

int ObJavaPolicySqlService::drop_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                                             const common::ObString &ddl_stmt_str,
                                             common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = java_policy_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!java_policy_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(java_policy_schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_pk_column("`key`", java_policy_schema.get_key()))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(buffer.append_fmt("INSERT INTO %s(tenant_id, `key`, schema_version, kind, grantee, type_schema, type_name, name, action, status, gmt_create, gmt_modified, is_deleted) "
            "SELECT tenant_id, `key`, %ld, kind, grantee, type_schema, type_name, name, action, status, gmt_create, gmt_modified, 1 FROM %s "
            "WHERE tenant_id = 0 AND `key` = %lu",
            OB_ALL_JAVA_POLICY_HISTORY_TNAME,
            java_policy_schema.get_schema_version(),
            OB_ALL_JAVA_POLICY_TNAME,
            java_policy_schema.get_key()))) {
    LOG_WARN("fail to append fmt", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else if (OB_FALSE_IT(buffer.reuse())) {
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_JAVA_POLICY_TNAME, buffer))) {
    LOG_WARN("fail to splice delete sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  }

  // log operation
  if (OB_SUCC(ret)) {

    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = java_policy_schema.get_type_schema();
    opt.java_policy_id_ = java_policy_schema.get_key();
    opt.op_type_ = OB_DDL_DROP_JAVA_POLICY;
    opt.schema_version_ = java_policy_schema.get_schema_version();
    opt.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("log operation failed", K(ret));
    }
  }

  return ret;
}

int ObJavaPolicySqlService::modify_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                                               const common::ObString &ddl_stmt_str,
                                               common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  const uint64_t tenant_id = java_policy_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (!java_policy_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(java_policy_schema));
  } else if (OB_FAIL(sql.add_pk_column("tenant_id", 0))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_pk_column("`key`", java_policy_schema.get_key()))) {
    LOG_WARN("fail to add pk column", K(ret));
  } else if (OB_FAIL(sql.add_column("status", static_cast<int64_t>(java_policy_schema.get_status())))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(sql.add_gmt_modified())) {
    LOG_WARN("fail to add gmt modified", K(ret));
  } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_JAVA_POLICY_TNAME, buffer))) {
    LOG_WARN("fail to splice update sql", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
    LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    buffer.reuse();
    if (OB_FAIL(buffer.append_fmt("INSERT INTO %s(tenant_id, `key`, schema_version, kind, grantee, type_schema, type_name, name, action, status, gmt_create, gmt_modified, is_deleted) "
            "SELECT tenant_id, `key`, %ld, kind, grantee, type_schema, type_name, name, action, status, gmt_create, gmt_modified, 0 FROM %s "
            "WHERE tenant_id = 0 AND `key` = %lu",
            OB_ALL_JAVA_POLICY_HISTORY_TNAME,
            java_policy_schema.get_schema_version(),
            OB_ALL_JAVA_POLICY_TNAME,
            java_policy_schema.get_key()))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, buffer.ptr(), affected_rows))) {
      LOG_WARN("fail to write", K(ret), K(exec_tenant_id), K(buffer));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
    }
  }

  // log operation
  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = java_policy_schema.get_type_schema();
    opt.java_policy_id_ = java_policy_schema.get_key();
    opt.op_type_ = OB_DDL_MODIFY_JAVA_POLICY;
    opt.schema_version_ = java_policy_schema.get_schema_version();
    opt.ddl_stmt_str_ = ddl_stmt_str;
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("log operation failed", K(ret));
    }
  }

  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

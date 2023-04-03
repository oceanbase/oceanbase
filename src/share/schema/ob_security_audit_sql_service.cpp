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
#include "ob_user_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_security_audit_sql_service.h"
namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObAuditSqlService::add_audit_metainfo(common::ObISQLClient &sql_client,
                                          const ObSAuditSchema &audit_schema,
                                          const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *tname[] = {
      OB_ALL_TENANT_SECURITY_AUDIT_TNAME,
      OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME};
  enum { INSERT_TABLE = 0, INSERT_HISTORY_TABLE, MAX_SQL_COUNT };
  const char *SQL_FORMAT[MAX_SQL_COUNT] = {
      "INSERT INTO %s (tenant_id, audit_id, audit_type, owner_id, operation_type, "
      "in_success, in_failure, gmt_modified) VALUES (%lu, %lu, %lu, %lu, %lu, %lu, %lu, now(6)) ",

      "INSERT INTO %s (tenant_id, audit_id, audit_type, owner_id, operation_type, "
      "in_success, in_failure, is_deleted, schema_version, gmt_modified) VALUES "
      "(%lu, %lu, %lu, %lu, %lu, %lu, %lu, false, %ld, now(6)) "
  };
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(audit_schema.get_tenant_id());

  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    sql.reuse();
    if (OB_FAIL(sql.append_fmt(SQL_FORMAT[i],
                               tname[i],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                    audit_schema.get_tenant_id()),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                    audit_schema.get_audit_id()),
                               audit_schema.get_audit_type(),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                    audit_schema.get_owner_id()),
                               audit_schema.get_operation_type(),
                               audit_schema.get_in_success(),
                               audit_schema.get_in_failure(),
                               new_schema_version))) {
      //the new_schema_version is unused for SQL_FORMAT[0]
      LOG_WARN("append sql failed, ", K(audit_schema), K(ret));
    } else {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret), K(exec_tenant_id), K(audit_schema));
      } else if (OB_UNLIKELY(affected_rows != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(audit_schema), K(ret));
      } else {
        LOG_DEBUG("insert succeed", K(affected_rows),K(audit_schema),K(new_schema_version),K(sql));
      }
    }
  }
  return ret;
}


int ObAuditSqlService::update_audit_metainfo(common::ObISQLClient &sql_client,
                                             const ObSAuditModifyType modify_type,
                                             const ObSAuditSchema &audit_schema,
                                             const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *tname[] = {
      OB_ALL_TENANT_SECURITY_AUDIT_TNAME,
      OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME
  };
  enum { UPDATE_TABLE_TWO = 0, INSERT_HISTORY_TABLE, MAX_SQL_COUNT };
  const char *SQL_FORMAT[MAX_SQL_COUNT] = {
      "UPDATE %s SET in_success = %lu, in_failure = %lu WHERE (tenant_id, audit_id) "
      "IN ((%lu, %lu)) ",

      "INSERT INTO %s (tenant_id, audit_id, audit_type, owner_id, operation_type, "
      "in_success, in_failure, is_deleted, schema_version, gmt_modified) VALUES "
      "(%lu, %lu, %lu, %lu, %lu, %lu, %lu, false, %ld, now(6)) "
  };
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(audit_schema.get_tenant_id());

  if (OB_UNLIKELY(!audit_schema.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("audit_schema is invalid", K(audit_schema), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
      sql.reuse();
      if (i == 0) {
        if (OB_FAIL(sql.append_fmt(SQL_FORMAT[UPDATE_TABLE_TWO],
                                   tname[i],
                                   audit_schema.get_in_success(),
                                   audit_schema.get_in_failure(),
                                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                       audit_schema.get_tenant_id()),
                                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                       audit_schema.get_audit_id())))) {
          LOG_WARN("append sql failed, ", K(audit_schema), K(ret));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(SQL_FORMAT[INSERT_HISTORY_TABLE],
                                   tname[i],
                                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                       audit_schema.get_tenant_id()),
                                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                       audit_schema.get_audit_id()),
                                   audit_schema.get_audit_type(),
                                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                       audit_schema.get_owner_id()),
                                   audit_schema.get_operation_type(),
                                   audit_schema.get_in_success(),
                                   audit_schema.get_in_failure(),
                                   new_schema_version))) {
          LOG_WARN("append sql failed, ", K(audit_schema), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret), K(exec_tenant_id), K(audit_schema));
        } else if (OB_UNLIKELY(affected_rows != 1) && OB_UNLIKELY(affected_rows != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value", K(affected_rows), K(sql), K(audit_schema), K(ret));
        } else {
          LOG_DEBUG("insert succeed", K(affected_rows), K(audit_schema), K(modify_type),
                    K(new_schema_version), K(sql));
        }
      }
    }
  }
  return ret;
}


int ObAuditSqlService::del_audit_metainfo(common::ObISQLClient &sql_client,
                                          const ObSAuditSchema &audit_schema,
                                          const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *tname[] = {
      OB_ALL_TENANT_SECURITY_AUDIT_TNAME,
      OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME
  };
  const char *SQL_FORMAT[] = {
      "DELETE FROM %s WHERE (tenant_id, audit_id) IN ((%lu, %lu));",

      "INSERT INTO %s (tenant_id, audit_id, audit_type, owner_id, operation_type, in_success, "
      "in_failure, is_deleted, schema_version, gmt_modified) VALUES "
      "(%lu, %lu, %lu, %lu, %lu, %lu, %lu, true, %ld, now(6));"
  };
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(audit_schema.get_tenant_id());
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tname); i++) {
    sql.reuse();
    if (OB_FAIL(sql.append_fmt(SQL_FORMAT[i],
                               tname[i],
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                    audit_schema.get_tenant_id()),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                    audit_schema.get_audit_id()),
                               audit_schema.get_audit_type(),
                               ObSchemaUtils::get_extract_schema_id(exec_tenant_id,
                                                                    audit_schema.get_owner_id()),
                               audit_schema.get_operation_type(),
                               audit_schema.get_in_success(),
                               audit_schema.get_in_failure(),
                               new_schema_version))) {
      LOG_WARN("append sql failed, ", K(audit_schema), K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret), K(audit_schema), K(exec_tenant_id));
      } else if (OB_UNLIKELY(affected_rows != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value", K(affected_rows), K(sql), K(audit_schema), K(ret));
      } else {
        LOG_DEBUG("insert succeed", K(affected_rows),K(sql),K(audit_schema),K(new_schema_version));
      }
    }
  }
  return ret;
}

int ObAuditSqlService::handle_audit_metainfo(const ObSAuditSchema &audit_schema,
                                             const ObSAuditModifyType modify_type,
                                             const bool need_update,
                                             const int64_t new_schema_version,
                                             const ObString *ddl_stmt_str,
                                             common::ObISQLClient &sql_client,
                                             common::ObSqlString &public_sql_string)
{
  int ret = OB_SUCCESS;
  ObSchemaOperation priv_operation;
  if (need_update) {
    if (OB_FAIL(update_audit_metainfo(sql_client, modify_type, audit_schema, new_schema_version))){
      LOG_WARN("failed to add_audit_metainfo", K(ret));
    } else {
      priv_operation.op_type_ = OB_DDL_UPDATE_AUDIT ;
    }
  } else if (AUDIT_MT_ADD == modify_type) {
    if (OB_FAIL(add_audit_metainfo(sql_client, audit_schema, new_schema_version))) {
      LOG_WARN("failed to add_audit_metainfo", K(ret));
    } else {
      priv_operation.op_type_ = OB_DDL_ADD_AUDIT;
    }
  } else if (AUDIT_MT_DEL == modify_type) {
    if (OB_FAIL(del_audit_metainfo(sql_client, audit_schema, new_schema_version))) {
      LOG_WARN("failed to del_audit_metainfo", K(ret));
    } else {
      priv_operation.op_type_ = OB_DDL_DEL_AUDIT;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments error", K(modify_type), K(audit_schema), K(ret));
  }

  // log operations
  if (OB_SUCC(ret)) {
    priv_operation.tenant_id_ = audit_schema.get_tenant_id();
    priv_operation.table_id_ = audit_schema.get_audit_id();
    priv_operation.schema_version_ = new_schema_version;
    priv_operation.ddl_stmt_str_ = (NULL != ddl_stmt_str ? *ddl_stmt_str : ObString());
    if (OB_FAIL(log_operation(priv_operation, sql_client,
                              OB_INVALID_TENANT_ID, &public_sql_string))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase

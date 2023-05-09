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
#include "ob_dblink_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObDbLinkSqlService::insert_dblink(const ObDbLinkBaseInfo &dblink_info,
                                      const int64_t is_deleted,
                                      ObISQLClient &sql_client,
                                      const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, dblink_info.get_tenant_id());
  if (!dblink_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink info is invalid", K(ret));
  } else if (OB_FAIL(add_pk_columns(dblink_info.get_tenant_id(),
                                    dblink_info.get_dblink_id(), dml))) {
    LOG_WARN("failed to add pk columns", K(ret), K(dblink_info));
  } else if (OB_FAIL(add_normal_columns(dblink_info, dml))) {
    LOG_WARN("failed to add normal columns", K(ret), K(dblink_info));
  }
  // insert into __all_dblink only when create dblink.
  if (OB_SUCC(ret) && !is_deleted) {
    if (OB_FAIL(exec.exec_insert(OB_ALL_DBLINK_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to execute insert", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }
  }
  // insert into __all_dblink_history always.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_history_columns(dblink_info, is_deleted, dml))) {
      LOG_WARN("failed to add history columns", K(ret), K(dblink_info));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_DBLINK_HISTORY_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to execute insert", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaOperation dblink_op;
    dblink_op.tenant_id_ = dblink_info.get_tenant_id();
    dblink_op.dblink_id_ = dblink_info.get_dblink_id();
    dblink_op.table_id_ = dblink_info.get_dblink_id();
    dblink_op.op_type_ = (is_deleted ? OB_DDL_DROP_DBLINK : OB_DDL_CREATE_DBLINK);
    dblink_op.schema_version_ = dblink_info.get_schema_version();
    dblink_op.ddl_stmt_str_ = !OB_ISNULL(ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(dblink_op, sql_client))) {
      LOG_WARN("failed to log create dblink ddl operation", K(dblink_op), K(ret));
    }
  }
  return ret;
}

int ObDbLinkSqlService::delete_dblink(const uint64_t tenant_id,
                                      const uint64_t dblink_id,
                                      ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObDMLExecHelper exec(sql_client, tenant_id);
  if (OB_FAIL(add_pk_columns(tenant_id, dblink_id, dml))) {
    LOG_WARN("failed to add pk columns", K(ret), K(tenant_id), K(dblink_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_DBLINK_TNAME, dml, affected_rows))) {
    LOG_WARN("failed to execute delete", K(ret));
  }
  LOG_WARN("affected_rows", K(affected_rows), K(ret));
  return ret;
}

int ObDbLinkSqlService::add_pk_columns(const uint64_t tenant_id,
                                       const uint64_t dblink_id,
                                       ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  uint64_t extract_dblink_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, dblink_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id))
   || OB_FAIL(dml.add_pk_column("dblink_id", extract_dblink_id))) {
    LOG_WARN("failed to add pk columns", K(ret));
  }
  return ret;
}

int ObDbLinkSqlService::add_normal_columns(const ObDbLinkBaseInfo &dblink_info,
                                           ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dblink_info.get_tenant_id());
  uint64_t owner_id = dblink_info.get_owner_id();
  uint64_t extract_owner_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, owner_id);
  ObString host_ip;
  ObString reverse_host_ip;
  char ip_buf[MAX_IP_ADDR_LENGTH] = {0};
  char reverse_ip_buf[MAX_IP_ADDR_LENGTH] = {0};
  uint64_t compat_version = 0;
  bool is_oracle_mode = false;
  uint64_t tenant_id = dblink_info.get_tenant_id();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (compat_version < DATA_VERSION_4_2_0_0 && !is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
  } else if (!dblink_info.get_host_addr().ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to ip to string", K(ret), K(dblink_info.get_host_addr()));
  } else if (FALSE_IT(host_ip.assign_ptr(ip_buf, static_cast<int32_t>(STRLEN(ip_buf))))) {
    // nothing.
  } else if (!dblink_info.get_reverse_host_addr().ip_to_string(reverse_ip_buf, sizeof(reverse_ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to reverse_ip to string", K(ret), K(dblink_info.get_reverse_host_addr()));
  } else if (FALSE_IT(reverse_host_ip.assign_ptr(reverse_ip_buf, static_cast<int32_t>(STRLEN(reverse_ip_buf))))) {
    // nothing.
  } else if (OB_FAIL(dml.add_column("dblink_name", ObHexEscapeSqlStr(dblink_info.get_dblink_name())))
          || OB_FAIL(dml.add_column("owner_id", extract_owner_id))
          || OB_FAIL(dml.add_column("host_ip", host_ip))
          || OB_FAIL(dml.add_column("host_port", dblink_info.get_host_port()))
          || OB_FAIL(dml.add_column("cluster_name", dblink_info.get_cluster_name()))
          || OB_FAIL(dml.add_column("tenant_name", dblink_info.get_tenant_name()))
          || OB_FAIL(dml.add_column("user_name", dblink_info.get_user_name()))
          || OB_FAIL(dml.add_column("driver_proto", dblink_info.get_driver_proto()))
          || OB_FAIL(dml.add_column("flag", dblink_info.get_flag()))
          || OB_FAIL(dml.add_column("service_name", dblink_info.get_service_name()))
          || OB_FAIL(dml.add_column("conn_string", dblink_info.get_conn_string()))
          || OB_FAIL(dml.add_column("authusr", dblink_info.get_authusr())) //no use
          || OB_FAIL(dml.add_column("authpwd", dblink_info.get_authpwd())) //no use
          || OB_FAIL(dml.add_column("passwordx", dblink_info.get_passwordx())) //no use
          || OB_FAIL(dml.add_column("authpwdx", dblink_info.get_authpwdx())) //no use
          // oracle store plain text of password in link$, so need not encrypt.
          || OB_FAIL(dml.add_column("password", dblink_info.get_password()))) {
    LOG_WARN("failed to add normal columns", K(ret));
  } else {
    const ObString &encrypted_password = dblink_info.get_encrypted_password();
    const int32_t &reverse_host_port = dblink_info.get_reverse_host_port();
    const ObString &reverse_cluster_name = dblink_info.get_reverse_cluster_name();
    const ObString &reverse_tenant_name = dblink_info.get_reverse_tenant_name();
    const ObString &reverse_user_name = dblink_info.get_reverse_user_name();
    const ObString &reverse_password = dblink_info.get_reverse_password();
    const ObString &password = dblink_info.get_password();
    uint64_t compat_version = 0;
    uint64_t tenant_id = dblink_info.get_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) { //compat_version < DATA_VERSION_4_1_0_0
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else {
      if (compat_version < DATA_VERSION_4_1_0_0) {
        if (!encrypted_password.empty() ||
            0 != reverse_host_port ||
            !reverse_cluster_name.empty() ||
            !reverse_tenant_name.empty() ||
            !reverse_user_name.empty() ||
            !reverse_password.empty()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("some column of dblink_info is not empty when MIN_DATA_VERSION is below DATA_VERSION_4_1_0_0", K(ret),
                                                                                        K(encrypted_password),
                                                                                        K(reverse_host_port),
                                                                                        K(reverse_cluster_name),
                                                                                        K(reverse_tenant_name),
                                                                                        K(reverse_user_name),
                                                                                        K(reverse_password));
        } else if (password.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("password can not be empty when MIN_DATA_VERSION is below DATA_VERSION_4_1_0_0", K(ret), K(password));
        }
      } else if (encrypted_password.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("encrypted_password is invalid when MIN_DATA_VERSION is DATA_VERSION_4_1_0_0 or above", K(ret), K(encrypted_password));
      } else if (!password.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("password need be empty when MIN_DATA_VERSION is DATA_VERSION_4_1_0_0 or above", K(ret), K(password));
      } else if (OB_FAIL(dml.add_column("encrypted_password", encrypted_password))
                || OB_FAIL(dml.add_column("reverse_host_ip", reverse_host_ip))
                || OB_FAIL(dml.add_column("reverse_host_port", dblink_info.get_reverse_host_port()))
                || OB_FAIL(dml.add_column("reverse_cluster_name", dblink_info.get_reverse_cluster_name()))
                || OB_FAIL(dml.add_column("reverse_tenant_name", dblink_info.get_reverse_tenant_name()))
                || OB_FAIL(dml.add_column("reverse_user_name", dblink_info.get_reverse_user_name()))
                || OB_FAIL(dml.add_column("reverse_password", dblink_info.get_reverse_password()))) {
        LOG_WARN("failed to add encrypted_password column", K(ret), K(encrypted_password));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (compat_version < DATA_VERSION_4_2_0_0) {
        if (!dblink_info.get_database_name().empty()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("some column of dblink_info is not empty when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret), K(dblink_info.get_database_name()));
        }
      } else if (OB_FAIL(dml.add_column("database_name", dblink_info.get_database_name()))) {
        LOG_WARN("failed to add normal database_name", K(dblink_info.get_database_name()), K(ret));
      }
    }

  }
  return ret;
}

int ObDbLinkSqlService::add_history_columns(const ObDbLinkBaseInfo &dblink_info,
                                            int64_t is_deleted,
                                            ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("schema_version", dblink_info.get_schema_version()))
   || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
    LOG_WARN("failed to add history columns", K(ret));
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase

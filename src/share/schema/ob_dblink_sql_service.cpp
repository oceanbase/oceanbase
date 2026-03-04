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
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace common::number;
using namespace common::sqlclient;
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
  bool support_domin_name = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (compat_version < DATA_VERSION_4_2_0_0 && !is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
  } else {
    support_domin_name = compat_version >= DATA_VERSION_4_2_5_0 ||
                         (compat_version >= MOCK_DATA_VERSION_4_2_1_8 && compat_version < DATA_VERSION_4_2_2_0);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!support_domin_name) {
    if (!dblink_info.get_host_addr().ip_to_string(ip_buf, sizeof(ip_buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to ip to string", K(ret), K(dblink_info.get_host_addr()));
    } else if (FALSE_IT(host_ip.assign_ptr(ip_buf, static_cast<int32_t>(STRLEN(ip_buf))))) {
    } else if (OB_FAIL(dml.add_column("host_ip", host_ip))) {
      LOG_WARN("failed to add host_ip columns", K(ret));
    } else if (OB_FAIL(dml.add_column("host_port", dblink_info.get_host_addr().get_port()))) {
      LOG_WARN("failed to add host_port columns", K(ret));
    } else if (!dblink_info.get_reverse_host_addr().ip_to_string(reverse_ip_buf, sizeof(reverse_ip_buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to reverse_ip to string", K(ret), K(dblink_info.get_reverse_host_addr()));
    } else if (FALSE_IT(reverse_host_ip.assign_ptr(reverse_ip_buf, static_cast<int32_t>(STRLEN(reverse_ip_buf))))) {
      // nothing.
    } else if (compat_version >= DATA_VERSION_4_1_0_0) {
      if (OB_FAIL(dml.add_column("reverse_host_ip", reverse_host_ip))) {
        LOG_WARN("failed to add reverse_host_ip columns", K(ret));
      } else if (OB_FAIL(dml.add_column("reverse_host_port", dblink_info.get_reverse_host_addr().get_port()))) {
        LOG_WARN("failed to add reverse_host_port columns", K(ret));
      }
    }
  } else {
    if (OB_FAIL(dml.add_column("host_ip", dblink_info.get_host_name()))) {
      LOG_WARN("failed to add host_ip columns", K(ret));
    } else if (OB_FAIL(dml.add_column("host_port", dblink_info.get_host_port()))) {
      LOG_WARN("failed to add host_port columns", K(ret));
    } else if (OB_FAIL(dml.add_column("reverse_host_ip", dblink_info.get_reverse_host_name()))) {
      LOG_WARN("failed to add reverse_host_ip columns", K(ret));
    } else if (OB_FAIL(dml.add_column("reverse_host_port", dblink_info.get_reverse_host_port()))) {
      LOG_WARN("failed to add reverse_host_port columns", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(dml.add_column("dblink_name", ObHexEscapeSqlStr(dblink_info.get_dblink_name())))
          || OB_FAIL(dml.add_column("owner_id", extract_owner_id))
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

int ObDbLinkSqlService::get_link_table_schema(const ObDbLinkSchema *dblink_schema,
                                                  const ObString &database_name,
                                                  const ObString &table_name,
                                                  ObIAllocator &alloctor,
                                                  ObTableSchema *&table_schema,
                                                  sql::ObSQLSessionInfo *session_info,
                                                  const ObString &dblink_name,
                                                  bool is_reverse_link,
                                                  uint64_t *current_scn,
                                                  bool &is_under_oracle12c)
{
  int ret = OB_SUCCESS;
  ObTableSchema *tmp_schema = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t dblink_id = OB_INVALID_ID;
  DblinkDriverProto link_type = DBLINK_DRV_OB;
  dblink_param_ctx param_ctx;
  sql::DblinkGetConnType conn_type = sql::DblinkGetConnType::DBLINK_POOL;
  ObReverseLink *reverse_link = NULL;
  ObString dblink_name_for_meta;
  int64_t sql_request_level = 0;
  if (NULL != dblink_schema) {
    tenant_id = dblink_schema->get_tenant_id();
    dblink_id = dblink_schema->get_dblink_id();
    link_type = static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto());
  }
  if (OB_ISNULL(dblink_proxy_) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(dblink_proxy_), KP(session_info));
  } else if (FALSE_IT(sql_request_level = session_info->get_next_sql_request_level())) {
  } else if (sql_request_level < 1 || sql_request_level > 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_request_level", K(sql_request_level), K(ret));
  } else if (is_reverse_link) { // RM process sql within @! and @xxxx! send by TM
    if (OB_FAIL(session_info->get_dblink_context().get_reverse_link(reverse_link))) {
      LOG_WARN("failed to get reverse link info", KP(reverse_link), K(session_info->get_server_sid()), K(ret));
    } else if (NULL == reverse_link) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(reverse_link), KP(session_info->get_server_sid()));
    } else {
      LOG_DEBUG("link schema, RM process sql within @! and @xxxx! send by TM", K(*reverse_link));
      conn_type = sql::DblinkGetConnType::TEMP_CONN;
      if (OB_FAIL(reverse_link->open(sql_request_level))) {
        LOG_WARN("failed to open reverse_link", K(ret));
      } else {
        tenant_id = session_info->get_effective_tenant_id(); //avoid tenant_id is OB_INVALID_ID
        dblink_name_for_meta.assign_ptr(dblink_name.ptr(), dblink_name.length());
        LOG_DEBUG("succ to open reverse_link when pull table meta", K(dblink_name_for_meta));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObDblinkService::init_dblink_param_ctx(param_ctx,
                                                     session_info,
                                                     alloctor, //useless in oracle mode
                                                     dblink_id,
                                                     link_type,
                                                     DblinkPoolType::DBLINK_POOL_SCHEMA))) {
    LOG_WARN("failed to init dblink param ctx", K(ret), K(param_ctx), K(dblink_id));
  } else {
    // param_ctx.charset_id_ and param_ctx.ncharset_id_, default value is what we need.
    param_ctx.charset_id_ = common::ObNlsCharsetId::CHARSET_AL32UTF8_ID;
    param_ctx.ncharset_id_ = common::ObNlsCharsetId::CHARSET_AL32UTF8_ID;
  }
  // skip to process TM process sql within @xxxx send by RM, cause here can not get DBLINK_INFO hint(still unresolved).
  LOG_DEBUG("get link table schema", K(table_name), K(database_name), KP(dblink_schema), KP(reverse_link), K(is_reverse_link), K(conn_type), K(ret));
  if (OB_FAIL(ret)) {// process normal dblink request
    // do nothing
  } else if (sql::DblinkGetConnType::DBLINK_POOL == conn_type && OB_ISNULL(dblink_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(dblink_schema));
  } else if (sql::DblinkGetConnType::DBLINK_POOL == conn_type &&
      OB_FAIL(dblink_proxy_->create_dblink_pool(param_ctx,
                                                dblink_schema->get_host_name(),
                                                dblink_schema->get_host_port(),
                                                dblink_schema->get_tenant_name(),
                                                dblink_schema->get_user_name(),
                                                dblink_schema->get_plain_password(),
                                                dblink_schema->get_database_name(),
                                                dblink_schema->get_conn_string(),
                                                dblink_schema->get_cluster_name()))) {
    LOG_WARN("create dblink pool failed", K(ret), K(param_ctx));
  } else if (OB_FAIL(fetch_link_table_info(param_ctx,
                                           conn_type,
                                           database_name,
                                           table_name,
                                           alloctor,
                                           tmp_schema,
                                           session_info,
                                           dblink_name_for_meta,
                                           reverse_link,
                                           current_scn,
                                           is_under_oracle12c))) {
    LOG_WARN("fetch link table info failed", K(ret), K(dblink_schema), K(database_name), K(table_name));
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else {
    table_schema = tmp_schema;
  }
  // close reverse_link
  if (NULL != reverse_link && OB_FAIL(ret)) {
    reverse_link->close();
    LOG_DEBUG("close reverse link", KP(reverse_link), K(ret));
  }
  return ret;
}

template<typename T>
int ObDbLinkSqlService::fetch_link_table_info(dblink_param_ctx &param_ctx,
                                                  sql::DblinkGetConnType conn_type,
                                                  const ObString &database_name,
                                                  const ObString &table_name,
                                                  ObIAllocator &allocator,
                                                  T *&table_schema,
                                                  sql::ObSQLSessionInfo *session_info,
                                                  const ObString &dblink_name,
                                                  sql::ObReverseLink *reverse_link,
                                                  uint64_t *current_scn,
                                                  bool &is_under_oracle12c)
{
  int ret = OB_SUCCESS;
  int dblink_read_ret = OB_SUCCESS;
  const char *dblink_read_errmsg = NULL;
  ObMySQLResult *result = NULL;
  ObSqlString sql;
  int64_t sql_request_level = param_ctx.sql_request_level_;
  // convert character set of schema name according to nls_collation
  ObString nls_database_name;
  ObString nls_table_name;
  ObString nls_dblink_name;
  static const char * sql_str_fmt_array[] = { // for Oracle mode dblink
    "/*$BEFPARSEdblink_req_level=1*/ SELECT * FROM \"%.*s\".\"%.*s\"%c%.*s WHERE ROWNUM < 1",
    "/*$BEFPARSEdblink_req_level=2*/ SELECT * FROM \"%.*s\".\"%.*s\"%c%.*s WHERE ROWNUM < 1",
    "/*$BEFPARSEdblink_req_level=3*/ SELECT * FROM \"%.*s\".\"%.*s\"%c%.*s WHERE ROWNUM < 1",
  };
  static const char * sql_str_fmt_array_mysql_mode[] = { // for MySql mode dblink
    "/*$BEFPARSEdblink_req_level=1*/ SELECT * FROM `%.*s`.`%.*s`%c%.*s LIMIT 0",
    "/*$BEFPARSEdblink_req_level=2*/ SELECT * FROM `%.*s`.`%.*s`%c%.*s LIMIT 0",
    "/*$BEFPARSEdblink_req_level=3*/ SELECT * FROM `%.*s`.`%.*s`%c%.*s LIMIT 0",
  };
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObISQLConnection *dblink_conn = NULL;
    if (NULL == session_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info is NULL", K(ret));
    } else if (database_name.empty() || table_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name or database name is empty", K(ret), K(database_name), K(table_name));
    } else if (OB_FAIL(convert_idenfitier_charset(param_ctx.link_type_, allocator, database_name,
                      session_info, nls_database_name))) {
      LOG_WARN("convert charset of database name failed", K(ret));
    } else if (OB_FAIL(convert_idenfitier_charset(param_ctx.link_type_, allocator, table_name,
                      session_info, nls_table_name))) {
      LOG_WARN("convert charset of table name failed", K(ret));
    } else if (OB_FAIL(convert_idenfitier_charset(param_ctx.link_type_, allocator, dblink_name,
                      session_info, nls_dblink_name))) {
      LOG_WARN("convert charset of dblink name failed", K(ret));
    } else if (OB_FAIL(sql.append_fmt(lib::is_oracle_mode() ?
                                      sql_str_fmt_array[sql_request_level - 1] :
                                      sql_str_fmt_array_mysql_mode[sql_request_level - 1],
                                      nls_database_name.length(), nls_database_name.ptr(),
                                      nls_table_name.length(), nls_table_name.ptr(),
                                      nls_dblink_name.empty() ? ' ' : '@',
                                      nls_dblink_name.length(), nls_dblink_name.ptr()))) {
      LOG_WARN("append sql failed", K(ret), K(nls_database_name), K(nls_table_name), K(nls_dblink_name));
    } else if (sql::DblinkGetConnType::TEMP_CONN == conn_type) {
      if (OB_ISNULL(reverse_link)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reverse_link is NULL", K(ret));
      } else if (OB_FAIL(reverse_link->read(sql.ptr(), res))) {
        LOG_WARN("failed to read table meta by reverse_link", K(ret));
      } else {
        LOG_DEBUG("succ to read table meta by reverse_link");
      }
    } else if (OB_FAIL(dblink_proxy_->acquire_dblink(param_ctx,
                                                     dblink_conn))) {
      LOG_WARN("failed to acquire dblink", K(ret), K(param_ctx));
    } else if (OB_FAIL(session_info->get_dblink_context().register_dblink_conn_pool(dblink_conn->get_common_server_pool()))) {
      LOG_WARN("failed to register dblink conn pool to current session", K(ret));
    } else if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn, res, sql.ptr()))) {
      LOG_WARN("read link failed", K(ret), K(param_ctx), K(sql.ptr()));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result. ", K(ret));
    } else if (OB_FAIL(result->set_expected_charset_id(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID),
                                                       static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)))) {
      // dblink will convert the result to AL32UTF8 when pulling schema meta
      LOG_WARN("failed to set expected charset id", K(ret));
    } else if (OB_FAIL(generate_link_table_schema(param_ctx,
                                                  conn_type,
                                                  database_name,
                                                  table_name,
                                                  allocator,
                                                  table_schema,
                                                  session_info,
                                                  dblink_conn,
                                                  result))) {
      LOG_WARN("generate link table schema failed", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr");
    } else if (lib::is_oracle_mode() && OB_FAIL(try_mock_link_table_column(*table_schema))) {
      LOG_WARN("failed to mock link table pkey", K(ret));
    } else {
      table_schema->set_table_organization_mode(ObTableOrganizationMode::TOM_HEAP_ORGANIZED);
    }
    if (OB_SUCC(ret) && DBLINK_DRV_OB == param_ctx.link_type_ && NULL != current_scn) {
      if (OB_FAIL(fetch_link_current_scn(param_ctx,
                                         conn_type,
                                         allocator,
                                         dblink_conn,
                                         reverse_link,
                                         *current_scn))) {
        LOG_WARN("fetch link current scn failed", K(ret));
      }
    }
#ifdef OB_BUILD_DBLINK
    if (DBLINK_DRV_OCI == param_ctx.link_type_ && OB_NOT_NULL(dblink_conn)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn)->free_oci_stmt())) {
        LOG_WARN("failed to close oci result", K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        SMART_VAR(ObMySQLProxy::MySQLResult, version_res) {
          const char* check_version_sql = "select version, product from product_component_version where product LIKE 'Oracle%' and version < '12.1.0.1'";
          ObMySQLResult *version_result = NULL;
          if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn, version_res, check_version_sql))) {
            LOG_WARN("read link failed", K(ret), K(param_ctx), K(sql.ptr()));
          } else if (OB_ISNULL(version_result = version_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get result", K(ret));
          } else if (OB_FAIL(version_result->set_expected_charset_id(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID),
                                                                     static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)))) {
            LOG_WARN("failed to set expected charset id", K(ret));
          } else if (OB_FAIL(version_result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            is_under_oracle12c = true;
          }
          if (OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn)->free_oci_stmt())) {
            LOG_WARN("failed to close oci result", K(tmp_ret));
          }
        }
      }
    }
#endif
    if (NULL != dblink_conn) {
      int tmp_ret = OB_SUCCESS;
      if (DBLINK_DRV_OB == param_ctx.link_type_ &&
          NULL != result &&
          OB_SUCCESS != (tmp_ret = result->close())) {
        LOG_WARN("failed to close result", K(tmp_ret));
      }
      if (OB_SUCCESS != (tmp_ret = dblink_proxy_->release_dblink(param_ctx.link_type_, dblink_conn))) {
        LOG_WARN("failed to relese connection", K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

template<typename T>
int ObDbLinkSqlService::generate_link_table_schema(const dblink_param_ctx &param_ctx,
                                                      sql::DblinkGetConnType conn_type,
                                                      const ObString &database_name,
                                                      const ObString &table_name,
                                                      ObIAllocator &allocator,
                                                      T *&table_schema,
                                                      const sql::ObSQLSessionInfo *session_info,
                                                      common::sqlclient::ObISQLConnection *dblink_conn,
                                                      const ObMySQLResult *col_meta_result)
{
  int ret = OB_SUCCESS;
  const char * desc_sql_str_fmt = "/*$BEFPARSEdblink_req_level=1*/ desc \"%.*s\".\"%.*s\"";
  ObSqlString desc_sql;
  int64_t desc_res_row_idx = -1;
  bool need_desc = param_ctx.sql_request_level_ == 1 &&
                   DBLINK_DRV_OB == param_ctx.link_type_ &&
                   sql::DblinkGetConnType::TEMP_CONN != conn_type;
  bool need_convert_charset = DBLINK_DRV_OB == param_ctx.link_type_ && is_oracle_mode();
  ObMySQLResult *desc_result = NULL;
  CK(OB_NOT_NULL(session_info));
  SMART_VAR(ObMySQLProxy::MySQLResult, desc_res) {
    T tmp_table_schema;
    table_schema = NULL;
    int64_t column_count = col_meta_result->get_column_count();
    ObCollationType nls_collation = session_info->get_nls_collation();
    tmp_table_schema.set_tenant_id(param_ctx.tenant_id_);
    tmp_table_schema.set_table_id(1); //no use
    tmp_table_schema.set_dblink_id(param_ctx.dblink_id_);
    tmp_table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    tmp_table_schema.set_charset_type(ObCharset::charset_type_by_coll(tmp_table_schema.get_collation_type()));
    if (OB_FAIL(tmp_table_schema.set_table_name(table_name))) {
      LOG_WARN("set table name failed", K(ret), K(table_name));
    } else if (OB_FAIL(tmp_table_schema.set_link_database_name(database_name))) {
      LOG_WARN("set database name failed", K(ret), K(database_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObColumnSchemaV2 column_schema;
      int16_t precision = 0;
      int16_t scale = 0;
      int32_t length = 0;
      ObString column_name;
      bool old_max_length = false;
      ObDataType data_type;
      if (OB_FAIL(col_meta_result->get_col_meta(i, old_max_length, column_name, data_type))) {
        LOG_WARN("failed to get column meta", K(i), K(old_max_length), K(ret));
      } else if (need_convert_charset && OB_FAIL(ObCharset::charset_convert(allocator, column_name,
                 nls_collation, CS_TYPE_UTF8MB4_BIN, column_name))) {
        LOG_WARN("convert charset of column name failed", K(ret), K(column_name));
      } else if (OB_FAIL(column_schema.set_column_name(column_name))) {
        LOG_WARN("failed to set column name", K(i), K(column_name), K(ret));
      } else {
        precision = data_type.get_precision();
        scale = data_type.get_scale();
        length = data_type.get_length();
        column_schema.set_table_id(tmp_table_schema.get_table_id());
        column_schema.set_tenant_id(param_ctx.tenant_id_);
        column_schema.set_column_id(i + OB_END_RESERVED_COLUMN_ID_NUM);
        column_schema.set_meta_type(data_type.get_meta_type());
        column_schema.set_charset_type(ObCharset::charset_type_by_coll(column_schema.get_collation_type()));
        column_schema.set_data_precision(precision);
        column_schema.set_data_scale(scale);
        column_schema.set_zero_fill(data_type.is_zero_fill());
        LOG_DEBUG("schema service sql impl get DBLINK schema", K(column_schema), K(length));
        if (need_desc && OB_ISNULL(desc_result) &&
            (ObNCharType == column_schema.get_data_type()
             || ObNVarchar2Type == column_schema.get_data_type())) {
          ObString nls_database_name;
          ObString nls_table_name;
          if (OB_ISNULL(dblink_conn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dblink conn is null",K(ret));
          } else if (OB_FAIL(convert_idenfitier_charset(param_ctx.link_type_, allocator, database_name,
                            session_info, nls_database_name))) {
            LOG_WARN("convert charset of database name failed", K(ret));
          } else if (OB_FAIL(convert_idenfitier_charset(param_ctx.link_type_, allocator, table_name,
                            session_info, nls_table_name))) {
            LOG_WARN("convert charset of table name failed", K(ret));
          } else if (OB_FAIL(desc_sql.append_fmt(desc_sql_str_fmt, nls_database_name.length(),
                        nls_database_name.ptr(), nls_table_name.length(), nls_table_name.ptr()))) {
            LOG_WARN("append desc sql failed", K(ret));
          } else if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn, desc_res, desc_sql.ptr()))) {
            LOG_WARN("read link failed", K(ret), K(param_ctx), K(desc_sql.ptr()));
          } else if (OB_ISNULL(desc_result = desc_res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get result", K(ret));
          } else if (OB_FAIL(desc_result->set_expected_charset_id(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID),
                                                                  static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)))) {
            // dblink will convert the result to AL32UTF8 when pulling schema meta
            LOG_WARN("failed to set expected charset id", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(desc_result)
            &&(ObNCharType == column_schema.get_data_type()
              || ObNVarchar2Type == column_schema.get_data_type())) {
          while (OB_SUCC(ret) && desc_res_row_idx < i) {
            if (OB_FAIL(desc_result->next())) {
              LOG_WARN("failed to get next row", K(ret));
            }
            ++desc_res_row_idx;
          }
          if (desc_res_row_idx == i && OB_SUCC(ret)) {
            const ObTimeZoneInfo *tz_info = TZ_INFO(session_info);
            ObObj value;
            ObString string_value;
            if (OB_ISNULL(tz_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tz info is NULL", K(ret));
            } else if (OB_FAIL(desc_result->get_obj(1, value, tz_info, &allocator))) {
              LOG_WARN("failed to get obj", K(ret));
            } else if (ObVarcharType != value.get_type()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type is invalid", K(value.get_type()), K(ret));
            } else if (OB_FAIL(value.get_varchar(string_value))) {
              LOG_WARN("failed to get varchar value", K(ret));
            } else if (OB_FAIL(ObDblinkService::get_length_from_type_text(string_value, length))) {
              LOG_WARN("failed to get length", K(ret));
            } else {
              LOG_DEBUG("desc table type string", K(string_value), K(length));
            }
          }
        }
        column_schema.set_data_length(length);
      }
      LOG_DEBUG("dblink column schema", K(i), K(column_schema.get_data_precision()),
                                          K(column_schema.get_data_scale()),
                                          K(column_schema.get_data_length()),
                                          K(column_schema.get_data_type()));
      if (OB_SUCC(ret) && OB_FAIL(tmp_table_schema.add_column(column_schema))) {
        LOG_WARN("fail to add link column schema. ", K(i), K(column_schema), K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObSchemaUtils::alloc_schema(allocator, tmp_table_schema, table_schema))) {
      LOG_WARN("failed to alloc table_schema", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(desc_result) && OB_SUCCESS != (tmp_ret = desc_result->close())) {
      LOG_WARN("failed to close desc result", K(tmp_ret));
    } else {
      desc_result = NULL;
    }
  }
  return ret;
}

int ObDbLinkSqlService::fetch_link_current_scn(const dblink_param_ctx &param_ctx,
                                                   sql::DblinkGetConnType conn_type,
                                                   ObIAllocator &allocator,
                                                   common::sqlclient::ObISQLConnection *dblink_conn,
                                                   sql::ObReverseLink *reverse_link,
                                                   uint64_t &current_scn)
{
  int ret = OB_SUCCESS;
  static const char * sql_str_fmt_array[] = {
    "/*$BEFPARSEdblink_req_level=1*/ SELECT current_scn() FROM dual",
    "/*$BEFPARSEdblink_req_level=2*/ SELECT current_scn() FROM dual",
    "/*$BEFPARSEdblink_req_level=3*/ SELECT current_scn() FROM dual",
  };
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObSqlString sql;
    ObMySQLResult *result = NULL;
    int64_t sql_request_level = param_ctx.sql_request_level_;
    if (OB_UNLIKELY(sql_request_level <= 0 || sql_request_level > 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sql req level", K(ret), K(sql_request_level));
    } else if (OB_FAIL(sql.assign(sql_str_fmt_array[sql_request_level - 1]))) {
      LOG_WARN("append sql failed",K(ret));
    } else if (sql::DblinkGetConnType::TEMP_CONN == conn_type) {
      if (OB_ISNULL(reverse_link)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reverse_link is NULL", K(ret));
      } else if (OB_FAIL(reverse_link->read(sql.ptr(), res))) {
        LOG_WARN("failed to read table meta by reverse_link", K(ret));
        ret = OB_SUCCESS;
      } else {
        result = res.get_result();
      }
    } else if (OB_ISNULL(dblink_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("link conn is NULL", K(ret));
    } else if (OB_FAIL(dblink_proxy_->dblink_read(dblink_conn, res, sql.ptr()))) {
      LOG_WARN("read link failed", K(ret), K(param_ctx), K(sql.ptr()));
      ret = OB_SUCCESS;
    } else {
      result = res.get_result();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(result)) {
      ObNumber scn_num;
      ObObj value;
      int64_t col_idx = 0;
      if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(result->get_obj(col_idx, value, NULL /* tz_info */, &allocator))) {
        LOG_WARN("get obj failed", K(ret));
      } else if (value.is_number()) {
        if (OB_FAIL(value.get_number(scn_num))) {
          LOG_WARN("get number failed", K(ret));
        } else if (OB_UNLIKELY(!scn_num.is_valid_uint64(current_scn))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value value", K(ret), K(value));
        }
      } else if (OB_LIKELY(value.is_uint64())) {
        current_scn = value.get_uint64();
      } else if (value.is_int()) {
        current_scn = value.get_int();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value type", K(ret), K(value));
      }
    }
  }
  return ret;
}

int ObDbLinkSqlService::try_mock_link_table_column(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator cs_iter = table_schema.column_begin();
  ObTableSchema::const_column_iterator cs_iter_end = table_schema.column_end();
  uint64_t max_column_id = 0;
  bool need_mock = true;
  for (; OB_SUCC(ret) && cs_iter != cs_iter_end; cs_iter++) {
    const ObColumnSchemaV2 &column_schema = **cs_iter;
    if (need_mock && column_schema.get_rowkey_position() > 0) {
      need_mock = false;
    }
    if (column_schema.get_column_id() > max_column_id) {
      max_column_id = column_schema.get_column_id();
    }
  }
  if (OB_SUCC(ret) && need_mock) {
    // This function should be consistent with ObSchemaRetrieveUtils::retrieve_link_column_schema().
    ObColumnSchemaV2 pkey_column;
    pkey_column.set_table_id(table_schema.get_table_id());
    pkey_column.set_column_id(OB_MOCK_LINK_TABLE_PK_COLUMN_ID);
    pkey_column.set_rowkey_position(1);
    pkey_column.set_data_type(ObUInt64Type);
    pkey_column.set_data_precision(-1);
    pkey_column.set_data_scale(-1);
    pkey_column.set_nullable(0);
    pkey_column.set_collation_type(CS_TYPE_BINARY);
    pkey_column.set_charset_type(CHARSET_BINARY);
    pkey_column.set_column_name(OB_MOCK_LINK_TABLE_PK_COLUMN_NAME);
    pkey_column.set_is_hidden(true);
    if (OB_FAIL(table_schema.add_column(pkey_column))) {
      LOG_WARN("failed to add link table pkey column", K(ret), K(pkey_column));
    } else if (OB_FAIL(table_schema.set_rowkey_info(pkey_column))) {
      LOG_WARN("failed to set rowkey info", K(ret), K(pkey_column));
    }
  }
  return ret;
}

int ObDbLinkSqlService::convert_idenfitier_charset(const DblinkDriverProto driver_proto,
                                                   ObIAllocator &alloc,
                                                   const ObString &in,
                                                   const sql::ObSQLSessionInfo *session_info,
                                                   ObString &out)
{
  int ret = OB_SUCCESS;
  ObCollationType tenant_collation = CS_TYPE_INVALID;
  if (!is_oracle_mode() || driver_proto != DBLINK_DRV_OB) {
    out = in;
  } else if (in.empty()) {
    out = in;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (FALSE_IT(tenant_collation = session_info->get_nls_collation())) {
  } else if (OB_FAIL(ObCharset::charset_convert(alloc, in, CS_TYPE_UTF8MB4_BIN, tenant_collation, out))) {
    LOG_WARN("charset convert failed", K(ret), K(in), K(tenant_collation));
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase

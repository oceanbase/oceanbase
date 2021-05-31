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

#define USING_LOG_PREFIX COMMON_MYSQLP

#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/sql_mode/ob_sql_mode_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

ObCommonSqlProxy::ObCommonSqlProxy() : pool_(NULL)
{}

ObCommonSqlProxy::~ObCommonSqlProxy()
{}

int ObCommonSqlProxy::init(ObISQLConnectionPool* pool)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
  } else {
    pool_ = pool;
  }
  return ret;
}

void ObCommonSqlProxy::operator=(const ObCommonSqlProxy& o)
{
  this->ObISQLClient::operator=(o);
  active_ = o.active_;
  pool_ = o.pool_;
}

int ObCommonSqlProxy::read(ReadResult& result, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  ObISQLConnection* conn = NULL;
  if (OB_FAIL(acquire(conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_FAIL(read(conn, result, tenant_id, sql))) {
    LOG_WARN("read failed", K(ret));
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::read(ObISQLConnection* conn, ReadResult& result, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  const int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  result.reset();
  if (OB_ISNULL(sql) || OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql or null conn", K(ret), K(sql), K(conn));
  } else if (!is_active()) {  // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), K(sql));
  } else {
    if (OB_FAIL(conn->execute_read(tenant_id, sql, result))) {
      LOG_WARN("query failed", K(ret), K(conn), K(start), K(sql));
    }
  }
  LOG_TRACE("execute sql", K(sql), K(ret));
  return ret;
}

int ObCommonSqlProxy::write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection* conn = NULL;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(acquire(conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) {  // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), K(sql));
  } else {
    if (OB_FAIL(conn->execute_write(tenant_id, sql, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(conn), K(start), K(sql));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql", K(sql), K(ret));
  return ret;
}

int ObCommonSqlProxy::write(
    const uint64_t tenant_id, const char* sql, int64_t& affected_rows, int64_t compatibility_mode)
{
  int ret = OB_SUCCESS;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection* conn = NULL;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(acquire(conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) {  // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), K(sql));
  }
  int64_t old_compatibility_mode;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conn->get_session_variable("ob_compatibility_mode", old_compatibility_mode))) {
      LOG_WARN("fail to get inner connection compatibility mode", K(ret));
    } else if (old_compatibility_mode != compatibility_mode &&
               OB_FAIL(conn->set_session_variable("ob_compatibility_mode", compatibility_mode))) {
      LOG_WARN("fail to set inner connection compatibility mode", K(ret), K(compatibility_mode));
    } else {
      if (is_oracle_compatible(static_cast<ObCompatibilityMode>(compatibility_mode))) {
        conn->set_oracle_compat_mode();
      } else {
        conn->set_mysql_compat_mode();
      }
      LOG_TRACE("compatibility mode switch successfully!", K(old_compatibility_mode), K(compatibility_mode));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conn->execute_write(tenant_id, sql, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(conn), K(start), K(sql));
    } else if (old_compatibility_mode != compatibility_mode &&
               OB_FAIL(conn->set_session_variable("ob_compatibility_mode", old_compatibility_mode))) {
      LOG_WARN("fail to recover inner connection sql mode", K(ret));
    } else {
      if (is_oracle_compatible(static_cast<ObCompatibilityMode>(old_compatibility_mode))) {
        conn->set_oracle_compat_mode();
      } else {
        conn->set_mysql_compat_mode();
      }
      LOG_TRACE("compatibility mode switch successfully!", K(compatibility_mode), K(old_compatibility_mode));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql with sql mode", K(sql), K(compatibility_mode), K(ret));
  return ret;
}

int ObCommonSqlProxy::execute(const uint64_t tenant_id, ObIExecutor& executor)
{
  int ret = OB_SUCCESS;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection* conn = NULL;
  if (OB_FAIL(acquire(conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) {  // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret));
  } else {
    if (OB_FAIL(conn->execute(tenant_id, executor))) {
      LOG_WARN("execute failed", K(ret), K(conn), K(start));
    }
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::close(ObISQLConnection* conn, const int succ)
{
  int ret = OB_SUCCESS;
  if (conn != NULL) {
    ret = pool_->release(conn, OB_SUCCESS == succ);
    if (OB_FAIL(ret)) {
      LOG_WARN("release connection failed", K(ret), K(conn));
    }
  }
  return ret;
}

int ObCommonSqlProxy::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (OB_FAIL(pool_->escape(from, from_size, to, to_size, out_size))) {
    LOG_WARN("escape string failed",
        "from",
        ObString(from_size, from),
        K(from_size),
        "to",
        static_cast<void*>(to),
        K(to_size));
  }
  return ret;
}

int ObCommonSqlProxy::add_slashes(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  // In order to facilitate the use of ObString's ptr() and length() functions to pass in, from can be NULL when
  // from_size is 0
  if (OB_UNLIKELY(OB_ISNULL(from) && 0 != from_size) || OB_ISNULL(to) || OB_UNLIKELY(to_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(from), K(to), K(to_size), K(ret));
  } else if (OB_UNLIKELY(to_size < (2 * from_size + 1))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("to size must be 2 times longer than from length + 1", K(from_size), K(to_size), K(ret));
  } else {
    out_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < from_size; ++i) {
      switch (from[i]) {
        case '\0':
          to[out_size++] = '\\';
          to[out_size++] = '0';
          break;
        case '\'':
        case '\"':
        case '\\':
          to[out_size++] = '\\';
          to[out_size++] = from[i];
          break;
        default:
          to[out_size++] = from[i];
          break;
      }
    }
    to[out_size] = '\0';
  }
  return ret;
}

int ObCommonSqlProxy::acquire(sqlclient::ObISQLConnection*& conn)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = OB_INVALID_ID;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (OB_FAIL(pool_->acquire(conn, cluster_id, this))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  }
  return ret;
}

int ObCommonSqlProxy::read(ReadResult& result, const int64_t cluster_id, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  const int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  result.reset();
  ObISQLConnection* conn = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (NULL == sql) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(pool_->acquire(conn, cluster_id, this))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (NULL == conn) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) {  // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), K(sql));
  } else {
    if (OB_FAIL(conn->execute_read(tenant_id, sql, result))) {
      LOG_WARN("query failed", K(ret), K(conn), K(start), K(sql));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql", K(sql), K(ret));
  return ret;
}

int ObDbLinkProxy::init(ObDbLinkConnectionPool* pool)
{
  is_prepare_env = false;
  return ObMySQLProxy::init(pool);
}

int ObDbLinkProxy::create_dblink_pool(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant,
    const ObString& db_user, const ObString& db_pass, const ObString& db_name)
{
  int ret = OB_SUCCESS;
  ObDbLinkConnectionPool* dblink_pool = static_cast<ObDbLinkConnectionPool*>(pool_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (OB_FAIL(dblink_pool->create_dblink_pool(dblink_id, server, db_tenant, db_user, db_pass, db_name))) {
    LOG_WARN(
        "create dblink pool failed", K(ret), K(dblink_id), K(server), K(db_tenant), K(db_user), K(db_pass), K(db_name));
  }
  return ret;
}

int ObDbLinkProxy::acquire_dblink(uint64_t dblink_id, ObMySQLConnection*& dblink_conn)
{
  int ret = OB_SUCCESS;
  ObDbLinkConnectionPool* dblink_pool = static_cast<ObDbLinkConnectionPool*>(pool_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink proxy not inited");
  } else if (OB_FAIL(dblink_pool->acquire_dblink(dblink_id, dblink_conn))) {
    LOG_WARN("acquire dblink failed", K(ret), K(dblink_id));
  } else if (OB_FAIL(prepare_enviroment(dblink_conn, 0))) {
    LOG_WARN("failed to prepare dblink env", K(ret));
  }
  return ret;
}

int ObDbLinkProxy::prepare_enviroment(ObMySQLConnection* dblink_conn, int link_type)
{
  UNUSED(link_type);
  int ret = OB_SUCCESS;
  if (is_prepare_env) {
    // do nothing;
  } else if (OB_ISNULL(dblink_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink is null", K(ret));
  } else {
    if (OB_FAIL(execute_init_sql(dblink_conn))) {
      LOG_WARN("failed to execute init sql", K(ret));
    } else {
      LOG_DEBUG("set session variable nls_date_format");
      is_prepare_env = true;
    }
  }
  return ret;
}

int ObDbLinkProxy::execute_init_sql(ObMySQLConnection* dblink_conn)
{
  int ret = OB_SUCCESS;
  ObMySQLStatement stmt;
  typedef const char* sql_ptr_type;
  static sql_ptr_type sql_ptr[] = {"set nls_date_format='YYYY-MM-DD HH24:MI:SS'",
      "set nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF'",
      "set nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD'"};
  for (int i = 0; OB_SUCC(ret) && i < sizeof(sql_ptr) / sizeof(sql_ptr_type); ++i) {
    if (OB_FAIL(stmt.init(*dblink_conn, sql_ptr[i]))) {
      LOG_WARN("create statement failed", K(ret));
    } else if (OB_FAIL(stmt.execute_update())) {
      LOG_WARN("execute sql failed", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObDbLinkProxy::release_dblink(/*uint64_t dblink_id,*/ ObMySQLConnection* dblink_conn)
{
  int ret = OB_SUCCESS;
  ObDbLinkConnectionPool* dblink_pool = static_cast<ObDbLinkConnectionPool*>(pool_);
  is_prepare_env = false;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink proxy not inited");
  } else if (OB_FAIL(dblink_pool->release_dblink(dblink_conn))) {
    LOG_WARN("release dblink failed", K(ret));
  }
  return ret;
}

int ObDbLinkProxy::dblink_read(const uint64_t dblink_id, ReadResult& result, const char* sql)
{
  int ret = OB_SUCCESS;
  ObMySQLConnection* dblink_conn = NULL;
  if (OB_FAIL(acquire_dblink(dblink_id, dblink_conn))) {
    LOG_WARN("acquire dblink failed", K(ret));
  } else if (OB_FAIL(read(dblink_conn, result, OB_INVALID_TENANT_ID, sql))) {
    LOG_WARN("read from dblink failed", K(ret));
  } else if (OB_FAIL(release_dblink(dblink_conn))) {
    LOG_WARN("release dblink failed", K(ret));
  }
  return ret;
}

int ObDbLinkProxy::dblink_read(ObMySQLConnection* dblink_conn, ReadResult& result, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_conn)) {
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (OB_FAIL(read(dblink_conn, result, OB_INVALID_TENANT_ID, sql))) {
    LOG_WARN("read from dblink failed", K(ret));
  }
  return ret;
}

int ObDbLinkProxy::rollback(ObMySQLConnection* dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_conn)) {
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (OB_FAIL(dblink_conn->rollback())) {
    LOG_WARN("read from dblink failed", K(ret));
  }
  return ret;
}

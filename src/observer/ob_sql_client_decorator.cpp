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
#include "ob_sql_client_decorator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObSQLClientRetry::escape(const char *from, const int64_t from_size,
                             char *to, const int64_t to_size, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->escape(from, from_size, to, to_size, out_size);
  }
  return ret;
}


int ObSQLClientRetry::read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql)
{
  //TODO if need across cluster
  UNUSEDx(res, cluster_id, tenant_id, sql);
  return OB_NOT_SUPPORTED;
}

int ObSQLClientRetry::read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->read(res, tenant_id, sql, group_id);
    if (OB_FAIL(ret)) {
      for (int32_t retry = 0; retry < retry_limit_ && OB_SUCCESS != ret; retry++) {
        LOG_WARN("retry execute query when failed", K(ret), K(retry), K_(retry_limit), K(sql));
        ret = sql_client_->read(res, tenant_id, sql, group_id);
      }
    }
  }
  return ret;
}

int ObSQLClientRetry::write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->write(tenant_id, sql, group_id, affected_rows);
  }
  return ret;
}

sqlclient::ObISQLConnectionPool *ObSQLClientRetry::get_pool()
{
  sqlclient::ObISQLConnectionPool *pool = NULL;
  if (NULL != sql_client_) {
    pool = sql_client_->get_pool();
  }
  return pool;
}

sqlclient::ObISQLConnection *ObSQLClientRetry::get_connection()
{
  sqlclient::ObISQLConnection *conn = NULL;
  if (NULL != sql_client_) {
    conn = sql_client_->get_connection();
  }
  return conn;
}

////////////////////////////////////////////////////////////////
int ObSQLClientRetryWeak::escape(const char *from, const int64_t from_size,
                             char *to, const int64_t to_size, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->escape(from, from_size, to, to_size, out_size);
  }
  return ret;
}

int ObSQLClientRetryWeak::read_without_check_sys_variable(
    sqlclient::ObISQLConnection *conn,
    ReadResult &res,
    const uint64_t tenant_id,
    const char *sql)
{
  int ret = OB_SUCCESS;
  ObString check_sys_variable_name = ObString::make_string("ob_check_sys_variable");
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_FAIL(conn->set_session_variable(check_sys_variable_name, static_cast<int64_t>(check_sys_variable_)))) {
    LOG_WARN("failed to set session variable ob_check_sys_variable", K(ret));
  } else {

    ret = conn->execute_read(tenant_id, sql, res);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to read without check sys variable", K(ret), K(sql), K(tenant_id),
               K_(check_sys_variable), K_(snapshot_timestamp));
    } else {
      LOG_TRACE("read without check sys variable succeeded!", K(ret), K(sql), K(tenant_id),
                K_(check_sys_variable), K_(snapshot_timestamp));
    }

    int check_sys_variable = 1;
    int tmp_ret = conn->set_session_variable(check_sys_variable_name, check_sys_variable);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set session variable ob_check_sys_variable", K(ret));
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObSQLClientRetryWeak::read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql)
{
  //TODO if need across cluster
  UNUSEDx(res, cluster_id, tenant_id, sql);
  return OB_NOT_SUPPORTED;
}

int ObSQLClientRetryWeak::read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    // normal read
    if (check_sys_variable_) {
      ret = sql_client_->read(res, tenant_id, sql, group_id);
    } else {
      sqlclient::ObISQLConnection *conn = sql_client_->get_connection();
      ObSingleConnectionProxy single_conn_proxy;
      if (OB_NOT_NULL(conn)) {
        // for transaction
      } else if (OB_FAIL(single_conn_proxy.connect(tenant_id, group_id, sql_client_))) {
        LOG_WARN("failed to get mysql connect", KR(ret), K(tenant_id));
      } else {
        conn = single_conn_proxy.get_connection();
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(conn)) {
        ret = read_without_check_sys_variable(conn, res, tenant_id, sql);
      }
    }
  }
  return ret;
}

int ObSQLClientRetryWeak::write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->write(tenant_id, sql, group_id, affected_rows);
  }
  return ret;
}

sqlclient::ObISQLConnectionPool *ObSQLClientRetryWeak::get_pool()
{
  sqlclient::ObISQLConnectionPool *pool = NULL;
  if (NULL != sql_client_) {
    pool = sql_client_->get_pool();
  }
  return pool;
}

sqlclient::ObISQLConnection *ObSQLClientRetryWeak::get_connection()
{
  sqlclient::ObISQLConnection *conn = NULL;
  if (NULL != sql_client_) {
    conn = sql_client_->get_connection();
  }
  return conn;
}

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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include <mysql.h>
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_dblink_error_trans.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
ObMySQLStatement::ObMySQLStatement() :
    conn_(NULL),
    result_(*this),
    stmt_(NULL),
    sql_str_(NULL)
{

}

ObMySQLStatement::~ObMySQLStatement()
{
}

MYSQL *ObMySQLStatement::get_stmt_handler()
{
  return stmt_;
}

MYSQL *ObMySQLStatement::get_conn_handler()
{
  return stmt_;
}

ObMySQLConnection *ObMySQLStatement::get_connection()
{
  return conn_;
}

int ObMySQLStatement::init(ObMySQLConnection &conn, const char *sql)
{
  int ret = OB_SUCCESS;
  conn_ = &conn;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql, sql is null", K(ret));
  } else if (OB_ISNULL(stmt_ = conn.get_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql handler is null", K(ret));
  } else {
    sql_str_ = sql;
  }
  return ret;
}

void ObMySQLStatement::close()
{
  // FIXME: we should not call mysql_close()?
  // if mysql_close is called, connection to server is off
  // OB_ASSERT(NULL != stmt_);
  result_.close();
  // mysql_close(stmt_);
}

int ObMySQLStatement::execute_update()
{
  int64_t dummy = -1;
  return execute_update(dummy);
}

int ObMySQLStatement::execute_update(int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  const int CR_SERVER_LOST = 2013;
  if (OB_ISNULL(conn_) || OB_ISNULL(stmt_) || OB_ISNULL(sql_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql stmt", K_(conn), KP_(stmt), KP_(sql_str), K(ret));
  } else if (OB_UNLIKELY(!conn_->usable())) {
    ret = -CR_SERVER_LOST;
    conn_->set_last_error(ret);
    LOG_WARN("conn already failed, should not execute query again!", K(conn_));
  } else {
    int64_t begin = ObTimeUtility::current_monotonic_raw_time();
    if (0 != (tmp_ret = mysql_real_query(stmt_, sql_str_, STRLEN(sql_str_)))) {
      ret = -mysql_errno(stmt_);
      char errmsg[256] = {0};
      const char *srcmsg = mysql_error(stmt_);
      MEMCPY(errmsg, srcmsg, MIN(255, STRLEN(srcmsg)));
      LOG_WARN("fail to query server", "sessid",  conn_->get_sessid(), "server", stmt_->host, "port", stmt_->port,
               "err_msg", errmsg, K(tmp_ret), K(ret), K(sql_str_));
      if (OB_NOT_MASTER == tmp_ret) {
        // conn -> server pool -> connection pool
        conn_->get_root()->get_root()->signal_refresh(); // refresh server pool immediately
      }
      if (OB_INVALID_ID != conn_->get_dblink_id()) {
        LOG_WARN("dblink connection error", K(ret),
                                            KP(conn_),
                                            K(conn_->get_dblink_id()),
                                            K(conn_->get_sessid()),
                                            K(conn_->usable()),
                                            K(conn_->ping()),
                                            K(stmt_->host),
                                            K(stmt_->port),
                                            K(errmsg),
                                            K(STRLEN(sql_str_)),
                                            K(sql_str_));
        TRANSLATE_CLIENT_ERR(ret, errmsg);
      }
      if (is_need_disconnect_error(ret)) {
        conn_->set_usable(false);
      }
    } else {
      affected_rows = mysql_affected_rows(stmt_);
    }
    int64_t end = ObTimeUtility::current_monotonic_raw_time();
    LOG_TRACE("execute stat", "excute time(us)", (end - begin), "SQL:", sql_str_, K(ret));
  }
  return ret;
}


ObMySQLResult *ObMySQLStatement::execute_query(bool enable_use_result)
{
  ObMySQLResult *result = NULL;
  int ret = OB_SUCCESS;
  const int CR_SERVER_LOST = 2013;
  if (OB_ISNULL(conn_) || OB_ISNULL(stmt_) || OB_ISNULL(sql_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql stmt", K_(conn), K_(stmt), KP_(sql_str), K(ret));
  } else if (OB_UNLIKELY(!conn_->usable())) {
    ret = -CR_SERVER_LOST;
    conn_->set_last_error(ret);
    LOG_WARN("conn already failed, should not execute query again", K(conn_));
  } else {
    int64_t begin = ObTimeUtility::current_monotonic_raw_time();
    if (0 != mysql_real_query(stmt_, sql_str_, STRLEN(sql_str_))) {
      ret = -mysql_errno(stmt_);
      char errmsg[256] = {0};
      const char *srcmsg = mysql_error(stmt_);
      MEMCPY(errmsg, srcmsg, MIN(255, STRLEN(srcmsg)));
      const int ERR_LOCK_WAIT_TIMEOUT = -1205;
      if (ERR_LOCK_WAIT_TIMEOUT == ret) {
        LOG_INFO("fail to query server", "sessid", conn_->get_sessid(), "host", stmt_->host, "port", stmt_->port,
               "err_msg", errmsg, K(ret), K(sql_str_));
      } else {
        LOG_WARN("fail to query server", "host", stmt_->host, "port", stmt_->port, K(conn_->get_sessid()),
               "err_msg", errmsg, K(ret), K(STRLEN(sql_str_)), K(sql_str_), K(lbt()));
      }
      if (OB_SUCCESS == ret) {
        ret = OB_ERR_SQL_CLIENT;
        LOG_WARN("can not get errno", K(ret));
      } else if (OB_INVALID_ID != conn_->get_dblink_id()) {
        LOG_WARN("dblink connection error", K(ret),
                                            KP(conn_),
                                            K(conn_->get_dblink_id()),
                                            K(conn_->get_sessid()),
                                            K(conn_->usable()),
                                            K(conn_->ping()),
                                            K(stmt_->host),
                                            K(stmt_->port),
                                            K(errmsg),
                                            K(STRLEN(sql_str_)),
                                            K(sql_str_));
        TRANSLATE_CLIENT_ERR(ret, errmsg);
      }
      if (is_need_disconnect_error(ret)) {
        conn_->set_usable(false);
      }
    } else if (OB_FAIL(result_.init(enable_use_result))) {
      LOG_WARN("fail to init sql result", K(ret));
    } else {
      result = &result_;
    }
    conn_->set_last_error(ret);
    int64_t end = ObTimeUtility::current_monotonic_raw_time();
    LOG_TRACE("execute stat", "time(us)", (end - begin), "SQL", sql_str_, K(ret));
  }
  return result;
}

bool ObMySQLStatement::is_need_disconnect_error(int ret)
{
  bool need_disconnect = false;

  constexpr int CR_UNKNOWN_ERROR = 2000;
  constexpr int CR_SOCKET_CREATE_ERROR = 2001;
  constexpr int CR_CONNECTION_ERROR = 2002;
  constexpr int CR_CONN_HOST_ERROR = 2003;
  constexpr int CR_IPSOCK_ERROR = 2004;
  constexpr int CR_UNKNOWN_HOST = 2005;
  constexpr int CR_SERVER_GONE_ERROR = 2006;
  constexpr int CR_WRONG_HOST_INFO = 2009;
  constexpr int CR_LOCALHOST_CONNECTION = 2010;
  constexpr int CR_TCP_CONNECTION = 2011;
  constexpr int CR_SERVER_HANDSHAKE_ERR = 2012;
  constexpr int CR_SERVER_LOST = 2013;
  constexpr int CR_COMMANDS_OUT_OF_SYNC = 2014;
  int obclient_connection_errnos[] = {
    CR_UNKNOWN_ERROR,
    CR_SOCKET_CREATE_ERROR,
    CR_CONNECTION_ERROR,
    CR_CONN_HOST_ERROR,
    CR_IPSOCK_ERROR,
    CR_UNKNOWN_HOST,
    CR_SERVER_GONE_ERROR,
    CR_WRONG_HOST_INFO,
    CR_LOCALHOST_CONNECTION,
    CR_TCP_CONNECTION,
    CR_SERVER_HANDSHAKE_ERR,
    CR_SERVER_LOST,
    CR_COMMANDS_OUT_OF_SYNC
  };

  ret = abs(ret);
  for (int64_t i = 0; i < sizeof(obclient_connection_errnos) / sizeof(int); ++i) {
    if (ret == obclient_connection_errnos[i]) {
      need_disconnect = true;// need disconnect when there is a connection error
      break;
    }
  }
  return need_disconnect;
}

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

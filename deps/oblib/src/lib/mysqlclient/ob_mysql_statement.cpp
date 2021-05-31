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
#include <mariadb/mysql.h>
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/time/tbtimeutil.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLStatement::ObMySQLStatement() : conn_(NULL), result_(*this), stmt_(NULL), sql_str_(NULL)
{}

ObMySQLStatement::~ObMySQLStatement()
{}

MYSQL* ObMySQLStatement::get_stmt_handler()
{
  return stmt_;
}

MYSQL* ObMySQLStatement::get_conn_handler()
{
  return stmt_;
}

ObMySQLConnection* ObMySQLStatement::get_connection()
{
  return conn_;
}

int ObMySQLStatement::init(ObMySQLConnection& conn, const char* sql)
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

int ObMySQLStatement::execute_update(int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  if (OB_ISNULL(conn_) || OB_ISNULL(stmt_) || OB_ISNULL(sql_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql stmt", K_(conn), K_(stmt), K_(sql_str), K(ret));
  } else {
    int64_t begin = obsys::CTimeUtil::getMonotonicTime();
    if (0 != (tmp_ret = mysql_real_query(stmt_, sql_str_, STRLEN(sql_str_)))) {
      ret = -mysql_errno(stmt_);
      LOG_WARN("fail to query server",
          "server",
          stmt_->host,
          "port",
          stmt_->port,
          "err_msg",
          mysql_error(stmt_),
          K(tmp_ret),
          K(ret));
      if (OB_NOT_MASTER == tmp_ret) {
        // conn -> server pool -> connection pool
        conn_->get_root()->get_root()->signal_refresh();  // refresh server pool immediately
      }
    } else {
      affected_rows = mysql_affected_rows(stmt_);
    }
    int64_t end = obsys::CTimeUtil::getMonotonicTime();
    LOG_TRACE("execute stat", "excute time(us)", (end - begin), "SQL:", sql_str_);
  }
  return ret;
}

ObMySQLResult* ObMySQLStatement::execute_query()
{
  ObMySQLResult* result = NULL;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conn_) || OB_ISNULL(stmt_) || OB_ISNULL(sql_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid mysql stmt", K_(conn), K_(stmt), K_(sql_str), K(ret));
  } else {
    int64_t begin = obsys::CTimeUtil::getMonotonicTime();
    if (0 != mysql_real_query(stmt_, sql_str_, STRLEN(sql_str_))) {
      ret = -mysql_errno(stmt_);
      const int ER_LOCK_WAIT_TIMEOUT = -1205;
      if (ER_LOCK_WAIT_TIMEOUT == ret) {
        LOG_INFO(
            "fail to query server", "host", stmt_->host, "port", stmt_->port, "err_msg", mysql_error(stmt_), K(ret));
      } else {
        LOG_WARN(
            "fail to query server", "host", stmt_->host, "port", stmt_->port, "err_msg", mysql_error(stmt_), K(ret));
      }
      if (OB_SUCCESS == ret) {
        ret = OB_ERR_SQL_CLIENT;
        LOG_WARN("can not get errno", K(ret));
      }
    } else if (OB_FAIL(result_.init())) {
      LOG_WARN("fail to init sql result", K(ret));
    } else {
      result = &result_;
    }
    conn_->set_last_error(ret);
    int64_t end = obsys::CTimeUtil::getMonotonicTime();
    LOG_TRACE("execute stat", "time(us)", (end - begin), "SQL", sql_str_, K(ret));
  }
  return result;
}
}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase

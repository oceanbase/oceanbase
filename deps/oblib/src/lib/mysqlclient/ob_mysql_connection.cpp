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

#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_read_context.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLConnection::ObMySQLConnection()
    : root_(NULL),
      last_error_code_(OB_SUCCESS),
      busy_(false),
      timestamp_(0),
      error_times_(0),
      succ_times_(0),
      connection_version_(0),
      closed_(true),
      timeout_(-1),
      last_trace_id_(0),
      mode_(OCEANBASE_MODE),
      db_name_(NULL),
      tenant_id_(OB_INVALID_ID),
      read_consistency_(-1)
{
  memset(&mysql_, 0, sizeof(MYSQL));
}

ObMySQLConnection::~ObMySQLConnection()
{}

ObServerConnectionPool* ObMySQLConnection::get_root()
{
  return root_;
}

void ObMySQLConnection::init(ObServerConnectionPool* pool)
{
  root_ = pool;
  timestamp_ = 0;
  error_times_ = 0;
  succ_times_ = 0;
  set_last_error(OB_SUCCESS);
}

void ObMySQLConnection::set_timeout(const int64_t timeout)
{
  timeout_ = timeout;
}
const ObAddr& ObMySQLConnection::get_server(void) const
{
  return root_->get_server();
}

void ObMySQLConnection::reset()
{
  root_ = NULL;
  timestamp_ = 0;
  error_times_ = 0;
  succ_times_ = 0;
  set_last_error(OB_SUCCESS);
}

int ObMySQLConnection::create_statement(ObMySQLStatement& stmt, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(switch_tenant(tenant_id))) {
    LOG_WARN("switch tenant failed", K(tenant_id), K(ret));
  } else if (OB_FAIL(reset_read_consistency())) {
    LOG_WARN("fail to set read consistency", K(ret));
  } else if (OB_FAIL(stmt.init(*this, sql))) {
    LOG_WARN("fail to init prepared statement", K(ret));
  }
  return ret;
}

int ObMySQLConnection::prepare_statement(ObMySQLPreparedStatement& stmt, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stmt.init(*this, sql))) {
    LOG_WARN("fail to init prepared statement", K(ret));
  }
  return ret;
}

int ObMySQLConnection::connect(const char* user, const char* pass, const char* db)
{
  int ret = OB_SUCCESS;
  const static int MAX_IP_BUFFER_LEN = 32;
  char host[MAX_IP_BUFFER_LEN];
  host[0] = '\0';
  // https://baike.baidu.com/item/mysql_real_connect/4007597
  // if db is NULL, the default database is used.
  if (OB_ISNULL(user) || OB_ISNULL(pass) /*|| OB_ISNULL(db)*/) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(user), K(pass), K(db), K(ret));
  } else if (!root_->get_server().ip_to_string(host, MAX_IP_BUFFER_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("fail to get host.", K(root_->get_server()), K(ret));
  } else {
    close();
    LOG_INFO("connecting to mysql server", "ip", host, "port", root_->get_server().get_port());
    mysql_init(&mysql_);
    mysql_options(&mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout_);
    mysql_options(&mysql_, MYSQL_OPT_READ_TIMEOUT, &timeout_);
    mysql_options(&mysql_, MYSQL_OPT_WRITE_TIMEOUT, &timeout_);
    int32_t port = root_->get_server().get_port();
    MYSQL* mysql = mysql_real_connect(&mysql_, host, user, pass, db, port, NULL, 0);
    if (OB_ISNULL(mysql)) {
      ret = -mysql_errno(&mysql_);
      LOG_WARN("fail to connect to mysql server", K(host), K(user), K(port), "info", mysql_error(&mysql_), K(ret));
    } else {
      /*Note: mysql_real_connect() incorrectly reset the MYSQL_OPT_RECONNECT option
       * to its default value before MySQL 5.0.19. Therefore, prior to that version,
       * if you want reconnect to be enabled for each connection, you must
       * call mysql_options() with the MYSQL_OPT_RECONNECT option after each call
       * to mysql_real_connect(). This is not necessary as of 5.0.19: Call mysql_options()
       * only before mysql_real_connect() as usual.
       */
      my_bool reconnect = 0;  // in OB, do manual reconnect.
      mysql_options(&mysql_, MYSQL_OPT_RECONNECT, &reconnect);
      closed_ = false;
      db_name_ = db;
      tenant_id_ = OB_SYS_TENANT_ID;
      read_consistency_ = -1;
    }
  }
  return ret;
}

void ObMySQLConnection::close()
{
  if (!closed_) {
    mysql_close(&mysql_);
    closed_ = true;
    memset(&mysql_, 0, sizeof(MYSQL));
  }
}

int ObMySQLConnection::start_transaction(bool with_snap_shot /* = false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else if (with_snap_shot) {
    if (0 != mysql_real_query(&mysql_, "START TRANSACTION WITH CONSISTENT SNAPSHOT", 42)) {
      ret = -mysql_errno(&mysql_);
      LOG_WARN("fail to start transaction", "info", mysql_error(&mysql_), K(ret));
    }
  } else {
    if (0 != mysql_real_query(&mysql_, "START TRANSACTION", 17)) {
      ret = -mysql_errno(&mysql_);
      LOG_WARN("fail to start transaction", "info", mysql_error(&mysql_), K(ret));
    }
  }
  return ret;
}

int ObMySQLConnection::rollback()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    if (0 != mysql_rollback(&mysql_)) {
      ret = -mysql_errno(&mysql_);
      LOG_WARN("fail to rollback", "info", mysql_error(&mysql_), K(ret));
    }
  }
  return ret;
}

int ObMySQLConnection::commit()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    if (0 != mysql_commit(&mysql_)) {
      ret = -mysql_errno(&mysql_);
      LOG_WARN("fail to commit", "info", mysql_error(&mysql_), K(ret));
    }
  }
  return ret;
}

int ObMySQLConnection::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from) || OB_ISNULL(to) || OB_UNLIKELY(to_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(from), K(to), K(to_size), K(ret));
  } else if (OB_UNLIKELY(to_size < (2 * from_size + 1))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("to size must be 2 times longer than from length + 1", K(from_size), K(to_size), K(ret));
  } else if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    out_size = mysql_real_escape_string(&mysql_, to, from, from_size);
  }
  return ret;
}

/* can ONLY call in ObMySQLConnectionPool::acquire()
 * CAN NOT call during a transaction, would cause losing state
 */
int ObMySQLConnection::ping()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    // auto reconnect is disabled.
    if (0 != mysql_ping(&mysql_)) {
      ret = OB_ERR_SQL_CLIENT;
    }
  }
  return ret;
}

int ObMySQLConnection::set_timeout_variable(const int64_t query_timeout, const int64_t trx_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(query_timeout <= 0) || OB_UNLIKELY(trx_timeout <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(query_timeout), K(trx_timeout), K(ret));
  } else {
    SMART_VAR(char[OB_MAX_SQL_LENGTH], sql)
    {
      sql[0] = '\0';
      ObMySQLStatement stmt;
      int64_t affect_rows = 0;
      int64_t w_len = snprintf(sql,
          OB_MAX_SQL_LENGTH,
          "SET SESSION ob_query_timeout = %ld, "
          "SESSION ob_trx_timeout = %ld",
          query_timeout,
          trx_timeout);
      if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len >= OB_MAX_SQL_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fill sql string error", K(ret));
      } else if (OB_FAIL(create_statement(stmt, OB_SYS_TENANT_ID, sql))) {
        LOG_WARN("create statement failed", K(ret));
      } else if (OB_FAIL(stmt.execute_update(affect_rows))) {
        LOG_WARN("execute sql failed", K(get_server()), K(sql), K(ret));
      }
    }
  }
  if (DEBUG_MODE == mode_) {
    // Here in order to pass the single test, ignore the error of setting system variables
    ret = OB_SUCCESS;
  }
  return ret;
}
int ObMySQLConnection::set_trace_id()
{
  int ret = OB_SUCCESS;
  // if already has traceid
  const uint64_t* trace_id_val = ObCurTraceId::get();
  int64_t trace_id = trace_id_val[0];  // @bug @todo fixme, trace id is a string now
  if (trace_id != last_trace_id_) {
    SMART_VAR(char[OB_MAX_SQL_LENGTH], set_trace_sql)
    {
      set_trace_sql[0] = '\0';
      ObMySQLStatement stmt;
      int64_t affect_rows = 0;
      int64_t w_len = snprintf(set_trace_sql, OB_MAX_SQL_LENGTH, "SET @%s = %ld;", OB_TRACE_ID_VAR_NAME, trace_id);
      if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len >= OB_MAX_SQL_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fill sql error", K(ret));
      } else if (OB_FAIL(create_statement(stmt, OB_SYS_TENANT_ID, set_trace_sql))) {
        LOG_WARN("create statement failed", K(ret));
      } else if (OB_FAIL(stmt.execute_update(affect_rows))) {
        LOG_WARN("execute sql failed", K(get_server()), K(set_trace_sql), K(ret));
      }
    }
  }
  last_trace_id_ = trace_id;
  if (DEBUG_MODE == mode_) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMySQLConnection::init_oceanbase_connection()
{
  int ret = OB_SUCCESS;
  if (OCEANBASE_MODE == mode_) {
    ObMySQLStatement stmt;
    const char* sql = "set @@session.autocommit = ON;";
    if (OB_SYS_TENANT_ID != tenant_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant should be sys", K(ret), K_(tenant_id));
    } else if (OB_FAIL(rollback())) {
      LOG_WARN("fail to rollback", K(ret));
    } else if (OB_FAIL(stmt.init(*this, sql))) {
      LOG_WARN("create statement failed", K(ret));
    } else if (OB_FAIL(stmt.execute_update())) {
      LOG_WARN("execute sql failed", K(sql), K(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObMySQLConnection::switch_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (tenant_id_ != tenant_id && OB_INVALID_TENANT_ID != tenant_id) {
    ObMySQLStatement stmt;
    ObSqlString sql;
    if (OCEANBASE_MODE == mode_) {
      if (OB_FAIL(sql.append_fmt("ALTER SYSTEM CHANGE TENANT TENANT_ID = %lu", tenant_id))) {
        LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(tenant_id_));
      }
    } else {
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(sql.append_fmt("USE %s", db_name_))) {
          LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(db_name_), K(tenant_id_));
        }
      } else {
        if (OB_FAIL(sql.append_fmt("USE %s_%lu", db_name_, tenant_id))) {
          LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(db_name_), K(tenant_id_));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt.init(*this, sql.ptr()))) {
      LOG_WARN("create statement failed", K(ret));
    } else if (OB_FAIL(stmt.execute_update())) {
      LOG_WARN("execute sql failed", K(sql), K(ret), K(tenant_id), K(tenant_id_));
    } else {
      tenant_id_ = tenant_id;
    }
  }
  return ret;
}

int ObMySQLConnection::execute_write(
    const uint64_t tenant_id, const char* sql, int64_t& affected_rows, bool is_user_sql)
{
  UNUSED(is_user_sql);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    ObMySQLStatement stmt;
    if (OB_FAIL(create_statement(stmt, tenant_id, sql))) {
      LOG_WARN("create statement failed", K(sql), K(ret));
    } else if (OB_FAIL(stmt.execute_update(affected_rows))) {
      LOG_WARN("statement execute update failed", K(sql), K(ret));
    }
  }
  return ret;
}

int ObMySQLConnection::execute_read(
    const uint64_t tenant_id, const char* sql, ObISQLClient::ReadResult& res, bool is_user_sql, bool is_from_pl)
{
  UNUSED(is_user_sql);
  UNUSED(is_from_pl);
  int ret = OB_SUCCESS;
  ObMySQLReadContext* read_ctx = NULL;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else if (OB_FAIL(res.create_handler(read_ctx))) {
    LOG_ERROR("create result handler failed", K(ret));
  } else if (OB_FAIL(create_statement(read_ctx->stmt_, tenant_id, sql))) {
    LOG_WARN("create statement failed", K(sql), K(ret));
  } else if (OB_ISNULL(read_ctx->result_ = read_ctx->stmt_.execute_query())) {
    ret = get_last_error();
    const int ER_LOCK_WAIT_TIMEOUT = -1205;
    if (ER_LOCK_WAIT_TIMEOUT == ret) {
      LOG_INFO("query failed", K(get_server()), K(sql), K(ret));
    } else {
      LOG_WARN("query failed", K(get_server()), K(sql), K(ret));
    }
  }
  return ret;
}

int ObMySQLConnection::get_session_variable(const ObString& name, int64_t& val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    ObMySQLReadContext read_ctx;
    ObSqlString sql;
    ObMySQLResult* result = NULL;
    if (OB_FAIL(sql.append_fmt("select %.*s from dual", name.length(), name.ptr()))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(create_statement(read_ctx.stmt_, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("create statement failed", K(sql), K(ret));
    } else if (OB_ISNULL(read_ctx.result_ = read_ctx.stmt_.execute_query())) {
      ret = get_last_error();
      LOG_WARN("query failed", K(get_server()), K(sql), K(ret));
    } else if (NULL == (result = read_ctx.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (OB_FAIL(result->get_single_int(0, 0, val))) {
      LOG_WARN("failed to query session value", K(ret), K(sql));
    }
  }
  return ret;
}

int ObMySQLConnection::set_session_variable(const ObString& name, int64_t val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else {
    ObMySQLStatement stmt;
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("set %.*s = %ld", name.length(), name.ptr(), val))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(stmt.init(*this, sql.ptr()))) {
      LOG_WARN("create statement failed", K(ret));
    } else if (OB_FAIL(stmt.execute_update())) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      LOG_DEBUG("set session variable", K(name), K(val));
      if (0 == name.case_compare("ob_read_consistency")) {
        read_consistency_ = val;
        LOG_INFO("set mysql connection' ob_read_consistency", K(val), K(sql));
      }
    }
  }
  if (DEBUG_MODE == mode_) {
    // Here in order to pass the single test, ignore the error of setting system variables
    ret = OB_SUCCESS;
  }
  return ret;
}

// When the main database is in the switching state, the external SQL will be affected by the ob_read_consistency set by
// the user, and the standby database may weakly read the internal table, but an error is reported because multiple
// versions do not exist. In fact, the current code does not require external SQL to weaken consistent reads. For the
// implementation of ObMySqlConnection, it is restricted to use strong consistent reads.
int ObMySQLConnection::reset_read_consistency()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(closed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not established. call connect first", K(ret));
  } else if (3 != read_consistency_) {
    ObString ob_read_consistency = ObString::make_string("ob_read_consistency");
    int64_t val = 3;  // strong
    if (OB_FAIL(set_session_variable(ob_read_consistency, val))) {
      LOG_WARN("fail to set session variable", K(ob_read_consistency), K(val));
    }
  }
  return ret;
}

int ObMySQLConnection::connect_dblink()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is NULL", K(ret));
  } else if (OB_FAIL(connect(root_->get_db_user(), root_->get_db_pass(), root_->get_db_name()))) {
    LOG_WARN("fail to connect", K(ret));
  }
  return ret;
}

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase

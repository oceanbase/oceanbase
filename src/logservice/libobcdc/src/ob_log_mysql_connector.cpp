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
 *
 * This file defines MySQLConnector for OBCDC
 */

#define USING_LOG_PREFIX  OBLOG

#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "ob_log_mysql_connector.h"

#include "lib/string/ob_string.h"   // ObString
#include "share/ob_time_utility2.h" // ObTimeUtility2

#include "ob_log_utils.h"           // _SEC_


using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace libobcdc
{
bool MySQLConnConfig::is_valid() const
{
  bool ret = true;
  if (! svr_.is_valid()
      || NULL == mysql_user_
      || NULL == mysql_password_
      || mysql_connect_timeout_sec_ <= 0
      || mysql_query_timeout_sec_ <= 0) {
    ret = false;
  }
  return ret;
}

int MySQLConnConfig::reset(const ObAddr &svr,
    const char *mysql_user,
    const char *mysql_password,
    const char *mysql_db,
    const int mysql_connect_timeout_sec,
    const int mysql_query_timeout_sec)
{
  int ret = OB_SUCCESS;

  reset();

  if (OB_UNLIKELY(! svr.ip_to_string(ip_buf_, sizeof(ip_buf_)))) {
    LOG_ERROR("ip_to_string fail", K(svr), K(ip_buf_), K(sizeof(ip_buf_)));
    ret = OB_ERR_UNEXPECTED;
  } else {
    svr_ = svr;
    mysql_db_ = mysql_db;
    mysql_connect_timeout_sec_ = mysql_connect_timeout_sec;
    mysql_query_timeout_sec_ = mysql_query_timeout_sec;
    mysql_user_ = mysql_user;
    mysql_password_ = mysql_password;
  }

  return ret;
}

////////////////////////////////////// ObLogMySQLConnector /////////////////////////////////

ObLogMySQLConnector::ObLogMySQLConnector() :
    inited_(false),
    mysql_(NULL),
    svr_()
{
}

ObLogMySQLConnector::~ObLogMySQLConnector()
{
  destroy();
}

int ObLogMySQLConnector::init(const MySQLConnConfig& cfg,
    const bool enable_ssl_client_authentication)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("twice init", KR(ret));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!cfg.is_valid())) {
    LOG_ERROR("invalid config", KR(ret), K(cfg));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_conn_(cfg, enable_ssl_client_authentication))) {
    if (OB_NEED_RETRY != ret) {
      LOG_ERROR("init connector fail", KR(ret), K(cfg), K(enable_ssl_client_authentication));
    }
  } else {
    svr_ = cfg.svr_;
    inited_ = true;
    LOG_INFO("init mysql connector succ", K(this), K(cfg), "is_oracle_mode", is_oracle_mode());
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }

  return ret;
}

void ObLogMySQLConnector::destroy()
{
  inited_ = false;
  destroy_conn_();

  mysql_ = NULL;
  svr_.reset();

  LOG_INFO("destroy mysql connector succ", K(this), K(svr_));
}

int ObLogMySQLConnector::query(MySQLQueryBase& query)
{
  int ret = OB_SUCCESS;
  int err = 0;
  bool done = false;
  const char *sql = NULL;
  unsigned long sql_len = 0;
  MYSQL_RES *res = NULL;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.get_sql(sql, sql_len))) {
    LOG_ERROR("get_sql fail", KR(ret), K(sql), K(sql_len));
  } else if (OB_ISNULL(sql) || sql_len <= 0) {
    LOG_ERROR("invalid sql", K(sql), K(sql_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != (err = mysql_real_query(mysql_, sql, sql_len))) {
    int mysql_error_code = mysql_errno(mysql_);
    const char *err_msg = mysql_error(mysql_);

    LOG_WARN("mysql_real_query fail", K(err), K(mysql_error_code), "mysql_error", err_msg,
        K(svr_), K(sql_len), K(sql), K(svr_), "is_oracle_mode", is_oracle_mode());

    // 1. execution of sql failed, and mysql error: 1054 - Unknown column 'id' in 'field list'
    // This means that libobcdc is connected to a low version of the observer, no replica_type information is available, error code OB_ERR_COLUMN_NOT_FOUND is returned
    //
    // 2. execution of sql failed, and mysql error: 1146 - Table xx doesn't exist
    query.set_mysql_error(mysql_error_code, err_msg);
  } else if (OB_FAIL(query.set_result(mysql_, svr_))) {
    LOG_ERROR("set result fail", KR(ret), K(res), K(mysql_), K(svr_));
  } else {
    done = true;
  }

  if (OB_SUCCESS == ret && !done) {
    ret = OB_NEED_RETRY;
  }
  return ret;
}

int ObLogMySQLConnector::exec(MySQLQueryBase& query)
{
  int ret = OB_SUCCESS;
  int err = 0;
  bool done = false;
  const char *sql = NULL;
  unsigned long sql_len = 0;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(query.get_sql(sql, sql_len))) {
    LOG_ERROR("get_sql fail", KR(ret), K(sql), K(sql_len));
  } else if (OB_ISNULL(sql) || sql_len <= 0) {
    LOG_ERROR("invalid sql", K(sql), K(sql_len));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != (err = mysql_real_query(mysql_, sql, sql_len))) {
    int mysql_error_code = mysql_errno(mysql_);
    const char *err_msg = mysql_error(mysql_);
    LOG_WARN("mysql_real_query fail", K(err), K(mysql_error_code), K(err_msg), K(svr_),
        K(sql_len), K(sql), "is_oracle_mode", is_oracle_mode());

    // Setting error codes in case of errors
    query.set_mysql_error(mysql_error_code, err_msg);
  } else if (OB_FAIL(query.set_result(mysql_, svr_))) {
    LOG_ERROR("set result fail", KR(ret), K(mysql_), K(svr_));
  } else {
    done = true;
  }

  if (OB_SUCCESS == ret && !done) {
    ret = OB_NEED_RETRY;
  }
  return ret;
}

bool ObLogMySQLConnector::is_oracle_mode() const
{
  bool b_ret = false;
#ifdef OB_USE_DRCMSG
  b_ret = OB_NOT_NULL(mysql_) && mysql_->oracle_mode;
#endif
  return b_ret;
}

#define SET_TIMEOUT_SQL "SET SESSION ob_query_timeout = %ld, SESSION ob_trx_timeout = %ld"

int ObLogMySQLConnector::set_timeout_variable_(const int64_t query_timeout, const int64_t trx_timeout)
{
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_SQL_LENGTH], sql) {
    sql[0] = '\0';
    int64_t sql_len = 0;
    int mysql_err = 0;

    if (OB_UNLIKELY(query_timeout <= 0) || OB_UNLIKELY(trx_timeout <= 0)) {
      LOG_ERROR("invalid argument", K(query_timeout), K(trx_timeout));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(databuff_printf(sql, sizeof(sql), sql_len, SET_TIMEOUT_SQL, query_timeout,
        trx_timeout))) {
      LOG_ERROR("build SET_TIMEOUT_SQL fail", KR(ret), K(sql), K(sizeof(sql)), K(query_timeout),
          K(trx_timeout));
    } else if (0 != (mysql_err = mysql_real_query(mysql_, sql, sql_len))) {
      LOG_WARN("mysql_real_query fail", K(mysql_err), K(mysql_error(mysql_)), K(sql_len), K(sql),
          K(svr_), "is_oracle_mode", is_oracle_mode());
      ret = OB_NEED_RETRY;
    } else {
      // success
    }
  }
  return ret;
}

int ObLogMySQLConnector::init_conn_(const MySQLConnConfig &cfg,
    const bool enable_ssl_client_authentication)
{
  int ret = OB_SUCCESS;
  unsigned int connect_timeout = static_cast<unsigned int>(cfg.mysql_connect_timeout_sec_);
  unsigned int read_timeout = connect_timeout;
  unsigned int write_timeout = connect_timeout;
  int64_t query_timeout_us = cfg.mysql_query_timeout_sec_ * _SEC_;
  int64_t trx_timeout_us = query_timeout_us;
  const char *db_name = ""; // connect to ob without db_name at first to get tenant_mode

  if (NULL == (mysql_ = mysql_init(NULL))) {
    LOG_ERROR("mysql_init fail", KR(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != (mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout))) {
    LOG_ERROR("failed to set conn timeout for mysql conn",
        K(mysql_error(mysql_)), K(connect_timeout));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != (mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &read_timeout))) {
    LOG_ERROR("failed to set read timeout for mysql conn",
        K(mysql_error(mysql_)), K(read_timeout));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != (mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout))) {
    LOG_ERROR("failed to set write timeout for mysql conn",
        K(mysql_error(mysql_)), K(write_timeout));
    ret = OB_ERR_UNEXPECTED;
  } else {
#ifdef OB_BUILD_TDE_SECURITY
    if (! enable_ssl_client_authentication) {
      int64_t ssl_enforce = 0;

      if (0 != (mysql_options(mysql_, MYSQL_OPT_SSL_ENFORCE, &ssl_enforce))) {
        LOG_ERROR("failed to set ssl mode for mysql conn",
            K(mysql_error(mysql_)), K(ssl_enforce));
        ret = OB_ERR_UNEXPECTED;
      }
    }
#endif

    // CLIENT_MULTI_STATEMENTS: enable multiple-statement execution and multiple-result
    if (mysql_ != mysql_real_connect(mysql_,
          cfg.get_mysql_addr(),
          cfg.mysql_user_,
          cfg.mysql_password_,
          db_name,
          cfg.get_mysql_port(),
          NULL, CLIENT_MULTI_STATEMENTS)) {
      LOG_WARN("mysql connect failed", "mysql_error", mysql_error(mysql_),
          K(cfg.get_mysql_addr()),  K(cfg.get_mysql_port()), K(cfg));
      ret = OB_NEED_RETRY;
    }
    // Set timeout variables
    else if (OB_FAIL(set_timeout_variable_(query_timeout_us, trx_timeout_us))) {
      LOG_WARN("set_timeout_variable_ fail", KR(ret), K(query_timeout_us), K(trx_timeout_us), K(cfg));
    } else {
      db_name = is_oracle_mode() ? OB_ORA_SYS_SCHEMA_NAME : OB_SYS_DATABASE_NAME;

      // first connect to db with empty db_name to get tenant_mode, then set default db according to tenant_mode
      if (OB_ISNULL(db_name)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid default_db_name, OCEANBASE for mysql tenant and SYS for oracle tenant",
            KR(ret), "is_oracle_mode", is_oracle_mode(), K(cfg));
      } else if (0 != mysql_select_db(mysql_, db_name)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to change default db", KR(ret), "is_oracle_mode", is_oracle_mode(), K(db_name), K(cfg));
      }
      // Connection successful
    }
  }

  if (OB_SUCCESS != ret && NULL != mysql_) {
    mysql_close(mysql_);
    mysql_ = NULL;
  }

  return ret;
}

void ObLogMySQLConnector::destroy_conn_()
{
  if (NULL != mysql_) {
    mysql_close(mysql_);
    mysql_ = NULL;
  }
}

///////////////////////////////////////// MySQLQueryBase ///////////////////////////////////////////

MySQLQueryBase::MySQLQueryBase() :
    inited_(false),
    sql_(NULL),
    sql_len_(0),
    mysql_(NULL),
    svr_(),
    res_(NULL),
    row_(NULL),
    col_lens_(0),
    col_cnt_(0),
    mysql_err_code_(0),
    succ_result_set_cnt_(0)
{
  mysql_err_msg_[0] = '\0';
}

MySQLQueryBase::~MySQLQueryBase()
{
  destroy();
}

int MySQLQueryBase::init(const char *sql, const unsigned long sql_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(sql) || OB_UNLIKELY(sql_len <= 0)) {
    LOG_ERROR("invalid argument", K(sql), K(sql_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_ = sql;
    sql_len_ = sql_len;
    mysql_ = NULL;
    svr_.reset();
    res_ = NULL;
    row_ = NULL;
    col_lens_ = 0;
    col_cnt_ = 0;
    mysql_err_code_ = 0;
    mysql_err_msg_[0] = '\0';
    succ_result_set_cnt_ = 0;

    inited_ = true;
  }
  return ret;
}

void MySQLQueryBase::destroy()
{
  inited_ = false;

  if (NULL != res_) {
    mysql_free_result(res_);
    res_ = NULL;
  }

  sql_ = NULL;
  sql_len_ = 0;
  mysql_ = NULL;
  svr_.reset();
  res_ = NULL;
  row_ = NULL;
  col_lens_ = NULL;
  col_cnt_ = 0;

  mysql_err_code_ = 0;
  mysql_err_msg_[0] = '\0';

  succ_result_set_cnt_ = 0;
}

int MySQLQueryBase::get_sql(const char *&sql, unsigned long &sql_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else {
    sql = sql_;
    sql_len = sql_len_;
  }
  return ret;
}

int MySQLQueryBase::set_result(MYSQL *mysql, const ObAddr &svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mysql)) {
    LOG_ERROR("mysql handle is NULL", K(mysql));
    ret = OB_INVALID_ARGUMENT;
  } else {
    mysql_ = mysql;
    svr_ = svr;
    res_ = NULL;
    col_lens_ = 0;
    row_ = NULL;
    col_cnt_ = 0;
  }
  return ret;
}

int MySQLQueryBase::next_result()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mysql_)) {
    LOG_ERROR("mysql_ is null", K(mysql_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // If the previous result is valid, release the previous result
    if (OB_UNLIKELY(NULL != res_)) {
      mysql_free_result(res_);
      res_ = NULL;
    }

    if (OB_FAIL(iterate_next_result_())) {
      if (OB_NEED_RETRY != ret && OB_ITER_END != ret) {
        LOG_ERROR("iterate_next_result_ fail", KR(ret));
      }
    }
    // Save the result
    // FIXME: Here it is assumed that the statement is a SELECT type statement with a result set.
    // If INSERT/UPDATE/DELETE etc. are to be supported in the future, here it is determined whether the result set should be returned
    else if (OB_ISNULL(res_ = mysql_store_result(mysql_))) {
      int mysql_error_code = mysql_errno(mysql_);
      const char *err_msg = mysql_error(mysql_);
      LOG_WARN("mysql_store_result return NULL", K(mysql_error_code), K(err_msg), K(svr_));
      // Setting error codes
      set_mysql_error(mysql_error_code, err_msg);
      ret = OB_NEED_RETRY;
    } else {
      ++succ_result_set_cnt_;
    }
  }

  return ret;
}

int MySQLQueryBase::iterate_next_result_()
{
  int ret = OB_SUCCESS;
  int status = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else {
    // No need to execute mysql_next_result() when iterating through the result set for the first time
    if (0 == succ_result_set_cnt_) {
      // do nothing
    } else {
      // Iterate over the results of the next statement in the multi-statement
      // This is the stage on the server side where the specific "query" is executed, from syntax checking to successful execution.
      // Any errors may occur, so the effect and handling should be consistent with mysql_real_query()
      //
      // @return -1 = no, end of iteration already
      // @return >0 = error, error occurred
      // @return 0 = yes, there is still a next result set
      status = mysql_next_result(mysql_);

      if (-1 == status) {
        // No more result sets available
        ret = OB_ITER_END;
      } else if (0 == status) {
        // Iteration success
      } else {
        int mysql_error_code = mysql_errno(mysql_);
        const char *err_msg = mysql_error(mysql_);
        LOG_WARN("mysql_next_result fail, need retry", K(status), K(mysql_error_code), K(err_msg),
            K(succ_result_set_cnt_), K(svr_));
        // set error code
        set_mysql_error(mysql_error_code, err_msg);
        // Return need_retry
        ret = OB_NEED_RETRY;
      }
    }
  }

  return ret;
}

int MySQLQueryBase::next_row()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(res_)) {
    LOG_ERROR("invalid mysql result", K(res_));
    ret = OB_INVALID_DATA;
  } else if (OB_ISNULL(row_ = mysql_fetch_row(res_))) {
    if (0 != mysql_errno(mysql_)) {
      LOG_WARN("mysql_fetch_row fail", K(mysql_errno(mysql_)), K(mysql_error(mysql_)), K(svr_));
      ret = OB_NEED_RETRY;
    } else {
      ret = OB_ITER_END;
    }
  } else if (OB_ISNULL(col_lens_ = mysql_fetch_lengths(res_))) {
    LOG_ERROR("mysql_fetch_lengths fail", K(mysql_errno(mysql_)), K(mysql_error(mysql_)), K(svr_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Calculate the number of columns
    col_cnt_ = mysql_num_fields(res_);
  }

  // Uniform setting of mysql error codes
  if (OB_SUCCESS != ret && OB_ITER_END != ret && NULL != mysql_) {
    set_mysql_error(mysql_errno(mysql_), mysql_error(mysql_));
  }

  if (OB_SUCCESS != ret) {
    // The current result iteration is complete, or an error is encountered and the current SQL query result set is released early
    // 1. ret = OB_ITER_END, end of iteration
    // 2. ret = other error code
    if (NULL != res_) {
      mysql_free_result(res_);
      res_ = NULL;
    }
  }

  return ret;
}

int MySQLQueryBase::get_column_index(const char *column_name, int64_t &column_index)
{
  int ret = OB_SUCCESS;
  column_index = -1;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(column_name)) {
    LOG_ERROR("column_name is null");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(res_)) {
    LOG_ERROR("invalid mysql result", K(res_));
    ret = OB_INVALID_DATA;
  } else {
    MYSQL_FIELD *field = NULL;
    bool done = false;
    int column_count = mysql_num_fields(res_);

    for (int i = 0; ! done && i < column_count; i++) {
      // Get the definition of column i
      if (NULL != (field = mysql_fetch_field_direct(res_, i))) {
        if (0 == STRCMP(column_name, field->name)) {
          done = true;
          column_index = i;
        }
      }
    }

    if (!done) {
      column_index = -1;
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
  }

  return ret;
}

int MySQLQueryBase::get_int(const int64_t col_idx,
    int64_t& int_val,
    bool &is_null_value) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_varchar(col_idx, varchar_val, is_null_value))) {
    LOG_ERROR("failed to get varchar", KR(ret), K(col_idx));
  } else if (is_null_value) {
    // null value
  } else if (OB_ISNULL(varchar_val.ptr())) {
    LOG_ERROR("varchar_val is invalid", K(varchar_val), K(col_idx));
    ret = OB_INVALID_DATA;
  } else {
    int64_t ret_val = 0;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtoll(nptr, &end_ptr, 10);
    if (*nptr != '\0' && *end_ptr == '\0') {
      int_val = ret_val;
    } else {
      LOG_ERROR("invalid int value", K(varchar_val));
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int MySQLQueryBase::get_uint(const int64_t col_idx,
    uint64_t& int_val,
    bool &is_null_value) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_varchar(col_idx, varchar_val, is_null_value))) {
    LOG_ERROR("failed to get varchar", KR(ret), K(col_idx));
  } else if (is_null_value) {
    // null value
  } else if (OB_ISNULL(varchar_val.ptr())) {
    LOG_ERROR("varchar_val is invalid", K(varchar_val), K(col_idx));
    ret = OB_INVALID_DATA;
  } else {
    uint64_t ret_val = 0;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtoull(nptr, &end_ptr, 10);
    if (*nptr != '\0' && *end_ptr == '\0') {
      int_val = ret_val;
    } else {
      LOG_ERROR("invalid int value", K(varchar_val));
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int MySQLQueryBase::get_datetime(const int64_t col_idx,
    int64_t& datetime,
    bool &is_null_value) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  int64_t ret_val = 0;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_varchar(col_idx, varchar_val, is_null_value))) {
    LOG_ERROR("failed to get varchar", KR(ret), K(col_idx));
  } else if (is_null_value) {
    // null value
  } else if (OB_ISNULL(varchar_val.ptr())) {
    LOG_ERROR("varchar_val is invalid", K(varchar_val), K(col_idx));
    ret = OB_INVALID_DATA;
  }
  // Convert str to usec.
  else if (OB_FAIL(ObTimeUtility2::str_to_usec(varchar_val, ret_val))) {
    LOG_ERROR("failed to convert str to usec", KR(ret), K(varchar_val));
  } else {
    datetime = ret_val;
  }
  return ret;
}

int MySQLQueryBase::get_varchar(const int64_t col_idx,
    ObString& varchar_val,
    bool &is_null_value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(row_) || OB_ISNULL(col_lens_)) {
    LOG_ERROR("invalid row or col_lens ", K(row_), K(col_lens_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(col_idx < 0) || OB_UNLIKELY(col_cnt_ <= col_idx)) {
    LOG_ERROR("invalid col idx", K(col_idx), K(col_cnt_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    varchar_val.assign(row_[col_idx], static_cast<int32_t>(col_lens_[col_idx]));
    is_null_value = (NULL == row_[col_idx]);
  }
  return ret;
}

void MySQLQueryBase::set_mysql_error(const int err_code, const char *err_msg)
{
  int ret = OB_SUCCESS;

  mysql_err_code_ = err_code;
  mysql_err_msg_[0] = '\0';

  // set error msg
  if (0 != err_code && NULL != err_msg) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(mysql_err_msg_, sizeof(mysql_err_msg_), pos, "%s", err_msg))) {
      LOG_ERROR("databuff_printf err_msg fail", KR(ret), K(mysql_err_msg_), K(sizeof(mysql_err_msg_)),
          K(pos), K(err_msg), K(err_code));
    }
  }
}

}
}

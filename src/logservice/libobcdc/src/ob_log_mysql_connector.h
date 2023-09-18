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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_MYSQL_CONNECTOR_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_MYSQL_CONNECTOR_H_

#include <mysql.h>
#include <errmsg.h>

#include "share/ob_define.h"                // OB_MAX_*
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
#include "lib/net/ob_addr.h"                // ObAddr

namespace oceanbase
{
namespace libobcdc
{
class MySQLQueryBase;
struct MySQLConnConfig;
class IObLogMySQLConnector
{
public:
  virtual ~IObLogMySQLConnector() {}

public:
  /// Execute the query operation: SELECT
  ///
  /// The only two return values are the following, to prevent an error from occurring in the query, which could lead to an unexpected exit
  /// @retval OB_SUCCESS      success
  /// @retval OB_NEED_RETRY   needs to be retried
  virtual int query(MySQLQueryBase &query) = 0;

  /// 执行写入操作：
  /// UPDATE, INSERT, CREATE TABLE, etc.
  virtual int exec(MySQLQueryBase &query) = 0;

  virtual int init(const MySQLConnConfig &cfg, const bool enable_ssl_client_authentication) = 0;
  virtual bool is_inited() const = 0;
  virtual void destroy() = 0;
};

/////////////////////////////////// ObLogMySQLConnector /////////////////////////

class ObLogMySQLConnector : public IObLogMySQLConnector
{
public:
  ObLogMySQLConnector();
  virtual ~ObLogMySQLConnector();
  int init(const MySQLConnConfig &cfg,
      const bool enable_ssl_client_authentication);
  bool is_inited() const { return inited_; }
  void destroy();
  const common::ObAddr &get_server() const { return svr_; }
public:
  int query(MySQLQueryBase &query);
  int exec(MySQLQueryBase &query);
  bool is_oracle_mode() const;
private:
  int init_conn_(const MySQLConnConfig &cfg,
      const bool enable_ssl_client_authentication);
  void destroy_conn_();
  int set_timeout_variable_(const int64_t query_timeout, const int64_t trx_timeout);
public:
  const static char *DEFAULT_DB_NAME_MYSQL_MODE;
  const static char *DEFAULT_DB_NAME_ORACLE_MODE;
private:
  bool              inited_;
  MYSQL             *mysql_;
  common::ObAddr    svr_;
};

struct MySQLConnConfig
{
  common::ObAddr svr_;
  const char *mysql_user_;
  const char *mysql_password_;
  const char *mysql_db_;
  int mysql_connect_timeout_sec_;
  int mysql_query_timeout_sec_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH + 1];

  const char *get_mysql_addr() const { return ip_buf_; }
  int get_mysql_port() const { return svr_.get_port(); }

  bool is_valid() const;
  void reset()
  {
    svr_.reset();
    mysql_user_ = 0;
    mysql_password_ = 0;
    mysql_db_ = 0;
    mysql_connect_timeout_sec_ = 0;
    mysql_query_timeout_sec_ = 0;
    ip_buf_[0] = '\0';
  }

  int reset(const common::ObAddr &svr,
      const char *mysql_user,
      const char *mysql_password,
      const char *mysql_db,
      const int mysql_connect_timeout_sec,
      const int mysql_query_timeout_sec);

  TO_STRING_KV(K_(svr),
               K_(mysql_user),
               K_(mysql_db),
               K_(mysql_connect_timeout_sec),
               K_(mysql_query_timeout_sec));
};

/*
 * MySQL Query Base Class
 * To implement special queries and writes, you can inherit this class and then use the MySQL Connector to execute the queries and writes
 *
 * single/multiple-statement query operations (select).
 * 1. init(), which initialises the SQL
 * 2. pass the class object into the MySQL Connector's query() function to execute the SQL
 * 3. call next_result(), get the result set
 * 4. call next_row() to iterate over the row data
 * 5. use the get_xxx() function to get the corresponding column data when processing each row of data
 *
 * Write operations (update, insert)
 * 1. init(), initialise the SQL
 * 2. pass the class object into the MySQL Connector's exec() function to execute the SQL
 */
class MySQLQueryBase
{
protected:
  static const int64_t DEFAULT_SQL_LENGTH = 1024;

protected:
  MySQLQueryBase();
  virtual ~MySQLQueryBase();

public:
  int init(const char *sql, const unsigned long sql_len);
  void destroy();

  int get_sql(const char *&sql, unsigned long &sql_len);

  int set_result(MYSQL *mysql, const common::ObAddr &svr);

  /*
   * Iterate over the next result set
   * Support multiple-statement execution and multiple-result
   *
   * OB_ITER_END: no more results.
   * OB_NEED_RETRY: on connection failure.
   */
  int next_result();

  /*
   * Get next row till end.
   * OB_ITER_END: no more rows.
   * OB_NEED_RETRY: on connection failure.
   */
  int next_row();

  /*
   * Get index by column name
   * OB_ERR_COLUMN_NOT_FOUND : not found
   */
  int get_column_index(const char *column_name, int64_t &column_index);
  /*
   * Get column data.
   * Read column col_idx of certain type.
   */
  int get_int(const int64_t col_idx,
      int64_t &int_val,
      bool &is_null_value) const;
  int get_uint(const int64_t col_idx,
      uint64_t &int_val,
      bool &is_null_value) const;
  int get_datetime(const int64_t col_idx,
      int64_t &datetime,
      bool &is_null_value) const;
  int get_varchar(const int64_t col_idx,
      common::ObString &varchar_val,
      bool &is_null_value) const;

  // Error handling
  void set_mysql_error(const int err_code, const char *err_msg);
  int get_mysql_err_code() const { return mysql_err_code_; }
  const char *get_mysql_err_msg() const { return mysql_err_msg_; }

  int64_t get_result_count() const { return succ_result_set_cnt_; }

  const common::ObAddr &get_server() const { return svr_; }

private:
  int iterate_next_result_();

protected:
  bool            inited_;
  const char      *sql_;
  unsigned long   sql_len_;
  MYSQL           *mysql_;
  common::ObAddr  svr_;
  MYSQL_RES       *res_;
  MYSQL_ROW       row_;           // single row of data
  unsigned long   *col_lens_;     // Record the value of all columns in a single row
  unsigned int    col_cnt_;       // Record the number of columns
  // Log mysql execution error codes
  int             mysql_err_code_;
  // Logging mysql execution error text messages
  char            mysql_err_msg_[common::OB_MAX_ERROR_MSG_LEN];

  // Number of result sets that have been successfully iterated
  int64_t         succ_result_set_cnt_;
};

}
}

#endif

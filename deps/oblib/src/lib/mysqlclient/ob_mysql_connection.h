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

#ifndef __COMMON_OB_MYSQL_CONNECTION__
#define __COMMON_OB_MYSQL_CONNECTION__

#include <mysql.h>
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/net/ob_addr.h"
namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObServerConnectionPool;
class ObMySQLStatement;
class ObMySQLPreparedStatement;
class ObMySQLConnectionPool;

class ObMySQLConnection : public ObISQLConnection //, ObIDbLinkConnection
{
  friend class ObServerConnectionPool;
public:
  enum
  {
    OB_MYSQL_CONNECTION_ERROR = 1,
    // OB_MYSQL_CONNECTION_WARN = 2,
    OB_MYSQL_CONNECTION_SUCC = 3
  };
  enum Mode
  {
    DEBUG_MODE = 0,
    MYSQL_MODE = 1,
    OCEANBASE_MODE = 2
  };
public:
  ObMySQLConnection();
  ~ObMySQLConnection();
  int connect(const char *user, const char *pass, const char *db,
                                 oceanbase::common::ObAddr &addr, int64_t timeout, bool read_write_no_timeout = false, int64_t sql_req_level = 0);
  int connect(const char *user, const char *pass, const char *db, const bool use_ssl, bool read_write_no_timeout = false, int64_t sql_req_level = 0);
  void close();
  virtual bool is_closed() const;
  // use user provided the statement
  template<typename T>
  int create_statement(T &stmt, const uint64_t tenant_id, const char *sql);
  int prepare_statement(ObMySQLPreparedStatement &stmt, const char *sql);
  int escape(const char *from, const int64_t from_size, char *to,
      const int64_t to_size, int64_t &out_size);
  void init(ObServerConnectionPool *root);
  void reset();
  const common::ObAddr &get_server(void) const;
  ObServerConnectionPool *get_root();
  virtual ObCommonServerConnectionPool *get_common_server_pool() override;
  MYSQL *get_handler();
  void set_last_error(int err_code);
  int get_last_error(void) const;

  virtual int execute_read(const uint64_t tenant_id, const char *sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) override;

  virtual int execute_read(const int64_t cluster_id, const uint64_t tenant_id, const ObString &sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) override;

  virtual int execute_write(const uint64_t tenant_id, const ObString &sql,
      int64_t &affected_rows, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) override;

  virtual int execute_write(const uint64_t tenant_id, const char *sql,
                            int64_t &affected_rows, bool is_user_sql = false,
                            const common::ObAddr *sql_exec_addr = nullptr) override;
  virtual int execute_proc(const uint64_t tenant_id,
                        ObIAllocator &allocator,
                        ParamStore &params,
                        ObString &sql,
                        const share::schema::ObRoutineInfo &routine_info,
                        const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                        const ObTimeZoneInfo *tz_info,
                        ObObj *result) override;
  virtual int start_transaction(const uint64_t &tenant_id, bool with_snap_shot = false) override;
  virtual int rollback() override;
  virtual int commit() override;

  // session environment
  virtual int get_session_variable(const ObString &name, int64_t &val) override;
  virtual int set_session_variable(const ObString &name, int64_t val) override;
  int set_session_variable(const ObString &name, const ObString &val);

  virtual int ping() override;
  int set_trace_id();
  void set_timeout(const int64_t timeout);
  virtual int set_timeout_variable(const int64_t query_timeout, const int64_t trx_timeout);
  virtual bool is_busy(void) const;
  virtual void set_busy(const bool busy);
  void set_connection_version(const int64_t version);
  int64_t connection_version() const;
  virtual void set_timestamp(const int64_t timestamp) { timestamp_ = timestamp; }
  int64_t get_timestamp() const { return timestamp_; }
  void set_mode(const Mode mode) { mode_ = mode; }
  int init_oceanbase_connection();
  void set_read_consistency(const int64_t read_consistency) { read_consistency_ = read_consistency; }
  void set_read_consistency_strong() { set_read_consistency(READ_CONSISTENCY_STRONG); }

  VIRTUAL_TO_STRING_KV(K_(db_name), K_(busy));

  // dblink.
  virtual int connect_dblink(const bool use_ssl, int64_t sql_request_level);


private:
  int switch_tenant(const uint64_t tenant_id);
  int reset_read_consistency();


private:
  const static int64_t READ_CONSISTENCY_STRONG = 3;

  ObServerConnectionPool *root_;  // each connection belongs to ONE pool
  MYSQL mysql_;
  int last_error_code_;
  bool busy_;
  int64_t timestamp_;
  int64_t error_times_;
  int64_t succ_times_;
  int64_t connection_version_;
  bool closed_;
  int64_t timeout_;
  int64_t last_trace_id_;
  Mode mode_;
  const char *db_name_;
  uint64_t tenant_id_;
  int64_t read_consistency_;
  DISALLOW_COPY_AND_ASSIGN(ObMySQLConnection);
};
inline bool ObMySQLConnection::is_busy() const
{
  return busy_;
}
inline void ObMySQLConnection::set_busy(const bool busy)
{
  busy_ = busy;
}
inline bool ObMySQLConnection::is_closed() const
{
  return closed_;
}
inline MYSQL *ObMySQLConnection::get_handler()
{
  return &mysql_;
}
inline void ObMySQLConnection::set_last_error(int err_code)
{
  this->last_error_code_ = err_code;
}
inline int ObMySQLConnection::get_last_error(void) const
{
  return this->last_error_code_;
}
inline void ObMySQLConnection::set_connection_version(const int64_t version)
{
  connection_version_ = version;
}
inline int64_t ObMySQLConnection::connection_version() const
{
  return connection_version_;
}

template<typename T>
int ObMySQLConnection::create_statement(T &stmt, const uint64_t tenant_id, const char *sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(switch_tenant(tenant_id))) {
    _OB_LOG(WARN, "switch tenant failed, tenant_id=%ld, ret=%d", tenant_id, ret);
  } else if (OB_FAIL(reset_read_consistency())) {
    _OB_LOG(WARN, "fail to set read consistency, ret=%d", ret);
  } else if (OB_FAIL(stmt.init(*this, sql))) {
    _OB_LOG(WARN, "fail to init statement, ret=%d", ret);
  }
  return ret;
}
}
}
}

#endif // __COMMON_OB_MYSQL_CONNECTION__

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

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_

#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/timezone/ob_timezone_info.h"

namespace oceanbase
{
namespace sql
{
class ObSql;
struct ObSqlCtx;
class ObResultSet;
}
namespace common
{
class ObIAllocator;
class ObString;

namespace sqlclient
{

class ObCommonServerConnectionPool
{
public:
  ObCommonServerConnectionPool() : free_conn_count_(0), busy_conn_count_(0) {}
  virtual ~ObCommonServerConnectionPool() {}

  //dblink
  virtual int free_dblink_session(uint32_t sessid) = 0;
  TO_STRING_KV(K_(free_conn_count), K_(busy_conn_count));
protected:
  volatile uint64_t free_conn_count_;
  volatile uint64_t busy_conn_count_;
};

class ObISQLResultHandler;

// execute in sql engine
class ObIExecutor
{
public:
  ObIExecutor() {}
  virtual ~ObIExecutor() {}

  // get schema version, return OB_INVALID_VERSION for newest schema.
  virtual int64_t get_schema_version() const { return OB_INVALID_VERSION; }

  virtual int execute(sql::ObSql &engine, sql::ObSqlCtx &ctx, sql::ObResultSet &res) = 0;

  // process result after result open
  virtual int process_result(sql::ObResultSet &res) = 0;

  virtual int64_t to_string(char *, const int64_t) const { return 0; }
};

// SQL client connection interface
class ObISQLConnection
{
public:
  ObISQLConnection()
      :oracle_mode_(false),
       is_init_remote_env_(false)
  {}
  virtual ~ObISQLConnection() {}

  // sql execute interface
  virtual int execute_read(const uint64_t tenant_id, const char *sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      bool is_from_pl = false) = 0;
  virtual int execute_read(const int64_t cluster_id, const uint64_t tenant_id, const ObString &sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      bool is_from_pl = false) = 0;
  virtual int execute_write(const uint64_t tenant_id, const char *sql,
      int64_t &affected_rows, bool is_user_sql = false) = 0;
  virtual int execute_write(const uint64_t tenant_id, const ObString &sql,
      int64_t &affected_rows, bool is_user_sql = false) = 0;

  // transaction interface
  virtual int start_transaction(const uint64_t &tenant_id, bool with_snap_shot = false) = 0;
  virtual int rollback() = 0;
  virtual int commit() = 0;

  // session environment
  virtual int get_session_variable(const ObString &name, int64_t &val) = 0;
  virtual int set_session_variable(const ObString &name, int64_t val) = 0;

  // dblink
  virtual ObCommonServerConnectionPool *get_common_server_pool() = 0;

  virtual int execute(const uint64_t tenant_id, ObIExecutor &executor)
  {
    UNUSED(tenant_id);
    UNUSED(executor);
    return OB_NOT_SUPPORTED;
  }

  void set_mysql_compat_mode() { oracle_mode_ = false; }
  void set_oracle_compat_mode() { oracle_mode_ = true; }
  bool is_oracle_compat_mode() const { return oracle_mode_; }
  virtual int set_ddl_info(const void *ddl_info) { UNUSED(ddl_info); return OB_NOT_SUPPORTED; }
  virtual int set_tz_info_wrap(const ObTimeZoneInfoWrap &tz_info_wrap) { UNUSED(tz_info_wrap); return OB_NOT_SUPPORTED; }
  virtual void set_nls_formats(const ObString *nls_formats) { UNUSED(nls_formats); }
  virtual void set_is_load_data_exec(bool v) { UNUSED(v); }
  virtual void set_force_remote_exec(bool v) { UNUSED(v); }
  virtual int64_t get_cluster_id() const { return common::OB_INVALID_ID; }
  void set_init_remote_env(bool flag) { is_init_remote_env_ = flag;}
  bool get_init_remote_env() const { return is_init_remote_env_; }
protected:
  bool oracle_mode_;
  bool is_init_remote_env_; // for dblink, we have to init remote env with some sql
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_

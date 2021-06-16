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

namespace oceanbase {
namespace sql {
class ObSql;
class ObSqlCtx;
class ObResultSet;
}  // namespace sql
namespace common {
class ObIAllocator;
class ObString;

namespace sqlclient {

class ObISQLResultHandler;

// execute in sql engine
class ObIExecutor {
public:
  ObIExecutor()
  {}
  virtual ~ObIExecutor()
  {}

  // get schema version, return OB_INVALID_VERSION for newest schema.
  virtual int64_t get_schema_version() const
  {
    return OB_INVALID_VERSION;
  }

  virtual int execute(sql::ObSql& engine, sql::ObSqlCtx& ctx, sql::ObResultSet& res) = 0;

  // process result after result open
  virtual int process_result(sql::ObResultSet& res) = 0;

  virtual int64_t to_string(char*, const int64_t) const
  {
    return 0;
  }
};

// SQL client connection interface
class ObISQLConnection {
public:
  ObISQLConnection() : did_no_retry_on_rpc_error_(false), oracle_mode_(false)
  {}
  virtual ~ObISQLConnection()
  {}

  // sql execute interface
  virtual int execute_read(const uint64_t tenant_id, const char* sql, ObISQLClient::ReadResult& res,
      bool is_user_sql = false, bool is_from_pl = false) = 0;
  virtual int execute_write(
      const uint64_t tenant_id, const char* sql, int64_t& affected_rows, bool is_user_sql = false) = 0;

  // transaction interface
  virtual int start_transaction(bool with_snap_shot = false) = 0;
  virtual int rollback() = 0;
  virtual int commit() = 0;

  // session environment
  virtual int get_session_variable(const ObString& name, int64_t& val) = 0;
  virtual int set_session_variable(const ObString& name, int64_t val) = 0;

  virtual int execute(const uint64_t tenant_id, ObIExecutor& executor)
  {
    UNUSED(tenant_id);
    UNUSED(executor);
    return OB_NOT_SUPPORTED;
  }

  void set_no_retry_on_rpc_error(bool did_no_retry)
  {
    did_no_retry_on_rpc_error_ = did_no_retry;
  }
  bool get_no_retry_on_rpc_error() const
  {
    return did_no_retry_on_rpc_error_;
  }

  void set_mysql_compat_mode()
  {
    oracle_mode_ = false;
  }
  void set_oracle_compat_mode()
  {
    oracle_mode_ = true;
  }
  bool is_oracle_compat_mode() const
  {
    return oracle_mode_;
  }
  virtual int64_t get_cluster_id() const
  {
    return common::OB_INVALID_ID;
  }

protected:
  bool did_no_retry_on_rpc_error_;
  bool oracle_mode_;
};

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_

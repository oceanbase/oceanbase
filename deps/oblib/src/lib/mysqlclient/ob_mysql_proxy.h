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

#ifndef OCEANBASE_MYSQL_PROXY_H_
#define OCEANBASE_MYSQL_PROXY_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObISQLConnection;
class ObISQLConnectionPool;
}  // namespace sqlclient

// thread safe sql proxy
// TODO : implement retry logic by general method (macros e.t.)
class ObCommonSqlProxy : public ObISQLClient {
public:
  // FIXME : remove this typedef?
  typedef ReadResult MySQLResult;

  ObCommonSqlProxy();
  virtual ~ObCommonSqlProxy();

  static int add_slashes(const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size);

  // init the connection pool
  virtual int init(sqlclient::ObISQLConnectionPool* pool);

  virtual int escape(
      const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size) override;
  // execute query and return data result
  virtual int read(ReadResult& res, const uint64_t tenant_id, const char* sql) override;
  virtual int read(ReadResult& res, const int64_t cluster_id, const uint64_t tenant_id, const char* sql) override;
  using ObISQLClient::read;
  // execute update sql
  virtual int write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows) override;
  int write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows, int64_t compatibility_mode);
  using ObISQLClient::write;

  bool is_inited() const
  {
    return NULL != pool_;
  }
  sqlclient::ObISQLConnectionPool* get_pool() override
  {
    return pool_;
  }

  // can only use assign() to copy to prevent passing ObCommonSqlProxy by value unintentionally.
  void assign(const ObCommonSqlProxy& proxy)
  {
    *this = proxy;
  }

  // relase the connection
  int close(sqlclient::ObISQLConnection* conn, const int succ);

  int execute(const uint64_t tenant_id, sqlclient::ObIExecutor& executor);

protected:
  int acquire(sqlclient::ObISQLConnection*& conn);
  int read(sqlclient::ObISQLConnection* conn, ReadResult& result, const uint64_t tenant_id, const char* sql);

  sqlclient::ObISQLConnectionPool* pool_;

  DISALLOW_COPY_AND_ASSIGN(ObCommonSqlProxy);
};

class ObMySQLProxy : public ObCommonSqlProxy {
public:
  virtual bool is_oracle_mode() const override
  {
    return false;
  }
};

class ObOracleSqlProxy : public ObCommonSqlProxy {
public:
  virtual bool is_oracle_mode() const override
  {
    return true;
  }

  ObOracleSqlProxy() : ObCommonSqlProxy()
  {}

  explicit ObOracleSqlProxy(ObMySQLProxy& sql_proxy)
  {
    pool_ = sql_proxy.get_pool();
  }
};

class ObDbLinkProxy : public ObMySQLProxy {
public:
  virtual int init(sqlclient::ObDbLinkConnectionPool* pool);
  int create_dblink_pool(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant, const ObString& db_user,
      const ObString& db_pass, const ObString& db_name);
  int acquire_dblink(uint64_t dblink_id, sqlclient::ObMySQLConnection*& dblink_conn);
  int release_dblink(/*uint64_t dblink_id,*/ sqlclient::ObMySQLConnection* dblink_conn);
  int dblink_read(const uint64_t dblink_id, ReadResult& result, const char* sql);
  int dblink_read(sqlclient::ObMySQLConnection* dblink_conn, ReadResult& result, const char* sql);
  int rollback(sqlclient::ObMySQLConnection* dblink_conn);

private:
  int prepare_enviroment(sqlclient::ObMySQLConnection* dblink_conn, int link_type);
  int execute_init_sql(sqlclient::ObMySQLConnection* dblink_conn);
  bool is_prepare_env;
};

// SQLXXX_APPEND macros for appending class member to insert sql
#define SQL_APPEND_COLUMN_NAME(sql, values, column)                                               \
  do {                                                                                            \
    if (OB_SUCC(ret)) {                                                                           \
      if (OB_SUCCESS != (ret = sql.append_fmt("%s%s", (values).empty() ? "" : ", ", (column)))) { \
        _OB_LOG(WARN, "sql append column %s failed, ret %d", (column), ret);                      \
      }                                                                                           \
    }                                                                                             \
  } while (false)

#define SQL_COL_APPEND_VALUE(sql, values, v, column, fmt)                                           \
  do {                                                                                              \
    SQL_APPEND_COLUMN_NAME(sql, values, column);                                                    \
    if (OB_SUCC(ret)) {                                                                             \
      if (OB_SUCCESS != (ret = (values).append_fmt("%s" fmt, (values).empty() ? "" : ", ", (v)))) { \
        _OB_LOG(WARN, "sql append value failed, ret %d, " #v " " fmt, ret, (v));                    \
      }                                                                                             \
    }                                                                                               \
  } while (false)

#define SQL_COL_APPEND_TWO_VALUE(sql, values, v1, v2, column, fmt)                                         \
  do {                                                                                                     \
    SQL_APPEND_COLUMN_NAME(sql, values, column);                                                           \
    if (OB_SUCC(ret)) {                                                                                    \
      if (OB_SUCCESS != (ret = (values).append_fmt("%s" fmt, (values).empty() ? "" : ", ", (v1), (v2)))) { \
        _OB_LOG(WARN, "sql append value failed, ret %d, " #v1 ", " #v2 " " fmt, ret, (v1), (v2));          \
      }                                                                                                    \
    }                                                                                                      \
  } while (false)

#define SQL_COL_APPEND_STR_VALUE(sql, values, v, v_len, column)                                                      \
  do {                                                                                                               \
    SQL_APPEND_COLUMN_NAME(sql, values, column);                                                                     \
    if (OB_SUCC(ret)) {                                                                                              \
      if (OB_SUCCESS !=                                                                                              \
          (ret = (values).append_fmt("%s'%.*s'", (values).empty() ? "" : ", ", static_cast<int32_t>(v_len), (v)))) { \
        _OB_LOG(WARN, "sql append value failed, ret %d, " #v " %.*s", ret, static_cast<int32_t>(v_len), (v));        \
      }                                                                                                              \
    }                                                                                                                \
  } while (false)

#define SQL_COL_APPEND_STRING_VALUE(sql, values, obj, member)                    \
  do {                                                                           \
    if (!((obj).member##_).empty()) {                                            \
      SQL_APPEND_COLUMN_NAME(sql, values, #member);                              \
      if (OB_SUCC(ret)) {                                                        \
        if (OB_SUCCESS != (ret = (values).append_fmt("%s'%.*s'",                 \
                               (values).empty() ? "" : ", ",                     \
                               static_cast<int32_t>(((obj).member##_).length()), \
                               ((obj).member##_).ptr()))) {                      \
          OB_LOG(WARN, "sql append value failed", K(ret));                       \
        }                                                                        \
      }                                                                          \
    }                                                                            \
  } while (false)

#define SQL_COL_APPEND_CSTR_VALUE(sql, values, v, column) SQL_COL_APPEND_STR_VALUE(sql, values, v, strlen(v), column)

#define SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, v_len, column)                                              \
  do {                                                                                                              \
    SQL_APPEND_COLUMN_NAME(sql, values, column);                                                                    \
    if (OB_SUCC(ret)) {                                                                                             \
      if (OB_SUCCESS != (ret = (values).append((values).empty() ? "'" : ", "))) {                                   \
        _OB_LOG(WARN, "sql append ', ' failed, ret %d", ret);                                                       \
      } else if (OB_SUCCESS != (ret = sql_append_hex_escape_str((v), v_len, (values)))) {                           \
        _OB_LOG(                                                                                                    \
            WARN, "sql append escaped value failed, ret %d, " #v " %.*s", ret, static_cast<int32_t>((v_len)), (v)); \
      }                                                                                                             \
    }                                                                                                               \
  } while (false)

#define SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, v, column) \
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, strlen(v), column)

#define SQL_APPEND_VALUE(sql, values, obj, member, fmt) SQL_COL_APPEND_VALUE(sql, values, (obj).member##_, #member, fmt)

#define SQL_APPEND_INT_VALUE(sql, values, obj, member) \
  SQL_COL_APPEND_VALUE(sql, values, static_cast<int64_t>((obj).member##_), #member, "%ld")

#define SQL_APPEND_UINT_VALUE(sql, values, obj, member) \
  SQL_COL_APPEND_VALUE(sql, values, static_cast<uint64_t>((obj).member##_), #member, "%lu")

#define SQL_APPEND_UINT_VALUE_WITH_TENANT_ID(sql, values, obj, member)                              \
  SQL_COL_APPEND_VALUE(sql,                                                                         \
      values,                                                                                       \
      static_cast<uint64_t>(ObSchemaUtils::get_extract_schema_id(exec_tenant_id, (obj).member##_)), \
      #member,                                                                                      \
      "%lu")

#define SQL_APPEND_CSTR_VALUE(sql, values, obj, member) SQL_COL_APPEND_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_STR_VALUE(sql, values, obj, member) \
  SQL_COL_APPEND_STR_VALUE(sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)

#define SQL_APPEND_ESCAPE_CSTR_VALUE(sql, values, obj, member) \
  SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_ESCAPE_STR_VALUE(sql, values, obj, member) \
  SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_MYSQL_PROXY_H_

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
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oracle_oci_connection.h"
#endif

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
class ObDbLinkConnectionPool;
}

struct ObSessionDDLInfo final
{
public:
  ObSessionDDLInfo()
    : ddl_info_(0)
  {}
  ~ObSessionDDLInfo() = default;
  void set_is_ddl(const bool is_ddl) { is_ddl_ = is_ddl; }
  bool is_ddl() const { return is_ddl_; }
  void set_source_table_hidden(const bool is_hidden) { is_source_table_hidden_ = is_hidden; }
  bool is_source_table_hidden() const { return is_source_table_hidden_; }
  void set_dest_table_hidden(const bool is_hidden) { is_dest_table_hidden_ = is_hidden; }
  bool is_dest_table_hidden() const { return is_dest_table_hidden_; }
  void set_heap_table_ddl(const bool flag) { is_heap_table_ddl_ = flag; }
  bool is_heap_table_ddl() const { return is_heap_table_ddl_; }
  void set_ddl_check_default_value(const bool flag) { is_ddl_check_default_value_bit_ = flag; }
  bool is_ddl_check_default_value() const { return is_ddl_check_default_value_bit_; }
  TO_STRING_KV(K_(ddl_info));
  OB_UNIS_VERSION(1);
public:
  static const int64_t IS_DDL_BIT = 1;
  static const int64_t IS_TABLE_HIDDEN_BIT = 1;
  static const int64_t IS_HEAP_TABLE_DDL_BIT = 1;
  static const int64_t IS_DDL_CHECK_DEFAULT_VALUE_BIT = 1;
  static const int64_t RESERVED_BIT = sizeof(int64_t) - IS_DDL_BIT - 2 * IS_TABLE_HIDDEN_BIT - IS_HEAP_TABLE_DDL_BIT - IS_DDL_CHECK_DEFAULT_VALUE_BIT;
  union {
    uint64_t ddl_info_;
    struct {
      uint64_t is_ddl_: IS_DDL_BIT;
      uint64_t is_source_table_hidden_: IS_TABLE_HIDDEN_BIT;
      uint64_t is_dest_table_hidden_: IS_TABLE_HIDDEN_BIT;
      uint64_t is_heap_table_ddl_: IS_HEAP_TABLE_DDL_BIT;
      uint64_t is_ddl_check_default_value_bit_ : IS_DDL_CHECK_DEFAULT_VALUE_BIT;
      uint64_t reserved_bit : RESERVED_BIT;
    };
  };
};

struct ObSessionParam final
{
public:
  ObSessionParam()
    : sql_mode_(nullptr), tz_info_wrap_(nullptr), ddl_info_(), is_load_data_exec_(false), use_external_session_(false), consumer_group_id_(0), nls_formats_{}
  {}
  ~ObSessionParam() = default;
public:
  int64_t *sql_mode_;
  ObTimeZoneInfoWrap *tz_info_wrap_;
  ObSessionDDLInfo ddl_info_;
  bool is_load_data_exec_;
  bool use_external_session_; // need init remote inner sql conn with sess getting from sess mgr
  int64_t consumer_group_id_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
};

// thread safe sql proxy
// TODO baihua: implement retry logic by general method (macros e.t.)
class ObCommonSqlProxy : public ObISQLClient
{
public:
  // FIXME baihua: remove this typedef?
  typedef ReadResult MySQLResult;

  ObCommonSqlProxy();
  virtual ~ObCommonSqlProxy();

  static int add_slashes(const char *from, const int64_t from_size,
                         char *to, const int64_t to_size, int64_t &out_size);

  // init the connection pool
  virtual int init(sqlclient::ObISQLConnectionPool *pool);

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) override;
  // execute query and return data result
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override { return this->read(res, tenant_id, sql, 0/*group_id*/); }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const ObSessionParam *session_param, int64_t user_set_timeout = 0);
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const common::ObAddr *sql_exec_addr);
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) override;
  //only for across cluster
  //cluster_id can not GCONF.cluster_id
  virtual int read(ReadResult &res,
                   const int64_t cluster_id,
                   const uint64_t tenant_id,
                   const char *sql) override;
  using ObISQLClient::read;
  // execute update sql
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override { return this->write(tenant_id, sql, 0/**/, affected_rows); }
  virtual int write(const uint64_t tenant_id, const char *sql, const int32_t group_id, int64_t &affected_rows) override;
  int write(const uint64_t tenant_id, const ObString sql, int64_t &affected_rows, int64_t compatibility_mode,
        const ObSessionParam *session_param = nullptr,
        const common::ObAddr *sql_exec_addr = nullptr);
  using ObISQLClient::write;

  bool is_inited() const { return NULL != pool_; }
  virtual sqlclient::ObISQLConnectionPool *get_pool() override { return pool_; }
  virtual sqlclient::ObISQLConnection *get_connection() override { return NULL; }

  // can only use assign() to copy to prevent passing ObCommonSqlProxy by value unintentionally.
  void assign(const ObCommonSqlProxy &proxy) { *this = proxy; }

  // relase the connection
  int close(sqlclient::ObISQLConnection *conn, const int succ);

  int execute(const uint64_t tenant_id, sqlclient::ObIExecutor &executor);

protected:
  int acquire(sqlclient::ObISQLConnection *&conn) { return this->acquire(OB_INVALID_TENANT_ID, conn, 0); }
  int acquire(const uint64_t tenant_id, sqlclient::ObISQLConnection *&conn, const int32_t group_id);
  int read(sqlclient::ObISQLConnection *conn, ReadResult &result,
           const uint64_t tenant_id, const char *sql, const common::ObAddr *sql_exec_addr = nullptr);

  sqlclient::ObISQLConnectionPool *pool_;

  DISALLOW_COPY_AND_ASSIGN(ObCommonSqlProxy);
};

class ObMySQLProxy : public ObCommonSqlProxy
{
public:
  virtual bool is_oracle_mode() const override { return false; }
};

class ObOracleSqlProxy : public ObCommonSqlProxy
{
public:
  virtual bool is_oracle_mode() const override { return true; }

  ObOracleSqlProxy() : ObCommonSqlProxy()
  {
  }

  explicit ObOracleSqlProxy(ObMySQLProxy &sql_proxy)
  {
    pool_ = sql_proxy.get_pool();
  }
};

class ObDbLinkProxy : public ObCommonSqlProxy
{
public:
  virtual bool is_oracle_mode() const override { return true; }
  virtual int init(sqlclient::ObDbLinkConnectionPool *pool);
  int create_dblink_pool(const sqlclient::dblink_param_ctx &param_ctx,
                         const ObAddr &server,
                         const ObString &db_tenant, const ObString &db_user,
                         const ObString &db_pass, const ObString &db_name,
                         const common::ObString &conn_str,
                         const common::ObString &cluster_str);
  int acquire_dblink(const sqlclient::dblink_param_ctx &param_ctx,
                     sqlclient::ObISQLConnection *&dblink_conn);
  int release_dblink(sqlclient::DblinkDriverProto dblink_type, sqlclient::ObISQLConnection *dblink_conn);
  int dblink_read(sqlclient::ObISQLConnection *dblink_conn, ReadResult &result, const char *sql);
  int dblink_write(sqlclient::ObISQLConnection *dblink_conn, int64_t &affected_rows, const char *sql);
  int dblink_execute_proc(sqlclient::ObISQLConnection *dblink_conn);
  int dblink_execute_proc(const uint64_t tenant_id,
                          sqlclient::ObISQLConnection *dblink_conn,
                          ObIAllocator &allocator,
                          ParamStore &params,
                          ObString &sql,
                          const share::schema::ObRoutineInfo &routine_info,
                          const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                          const ObTimeZoneInfo *tz_info);
  int dblink_prepare(sqlclient::ObISQLConnection *dblink_conn, const char *sql);
  int dblink_bind_basic_type_by_pos(sqlclient::ObISQLConnection *dblink_conn,
                                    uint64_t position,
                                    void *param,
                                    int64_t param_size,
                                    int32_t datatype,
                                    int32_t &indicator);
  int dblink_bind_array_type_by_pos(sqlclient::ObISQLConnection *dblink_conn,
                                    uint64_t position,
                                    void *array,
                                    int32_t *indicators,
                                    int64_t ele_size,
                                    int32_t ele_datatype,
                                    uint64_t array_size,
                                    uint32_t *out_valid_array_size);
  int dblink_get_server_major_version(sqlclient::ObISQLConnection *dblink_conn,
                                      int64_t &major_version);
  int dblink_get_package_udts(common::sqlclient::ObISQLConnection *dblink_conn,
                              ObIAllocator &alloctor,
                              const common::ObString &database_name,
                              const common::ObString &package_name,
                              common::ObIArray<pl::ObUserDefinedType *> &udts,
                              uint64_t dblink_id,
                              uint64_t &next_object_id);
  int rollback(sqlclient::ObISQLConnection *dblink_conn);
  int switch_dblink_conn_pool(sqlclient::DblinkDriverProto type, sqlclient::ObISQLConnectionPool *&dblink_conn_pool);
  int set_dblink_pool_charset(uint64_t dblink_id);
  inline sqlclient::ObDbLinkConnectionPool *get_dblink_conn_pool() { return link_pool_; }
  int clean_dblink_connection(uint64_t tenant_id);
  static int execute_init_sql(const sqlclient::dblink_param_ctx &param_ctx,
                              sqlclient::ObISQLConnection *dblink_conn);
private:
  int prepare_enviroment(const sqlclient::dblink_param_ctx &param_ctx,
                         sqlclient::ObISQLConnection *dblink_conn);
private:
  sqlclient::ObDbLinkConnectionPool *link_pool_;
};

// SQLXXX_APPEND macros for appending class member to insert sql
#define SQL_APPEND_COLUMN_NAME(sql, values, column) \
    do { \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = sql.append_fmt("%s%s", \
            (values).empty() ? "" : ", ", (column)))) { \
          _OB_LOG(WARN, "sql append column %s failed, ret %d", (column), ret); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_VALUE(sql, values, v, column, fmt) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append_fmt( \
            "%s" fmt, (values).empty() ? "" : ", ", (v)))) { \
          _OB_LOG(WARN, "sql append value failed, ret %d, " #v " " fmt, ret, (v)); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_TWO_VALUE(sql, values, v1, v2, column, fmt) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append_fmt( \
            "%s" fmt, (values).empty() ? "" : ", ", (v1), (v2)))) { \
          _OB_LOG(WARN, "sql append value failed, ret %d, " #v1 ", " #v2 " " fmt, \
              ret, (v1), (v2)); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_STR_VALUE(sql, values, v, v_len, column) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append_fmt( \
            "%s'%.*s'", (values).empty() ? "" : ", ", \
            static_cast<int32_t>(v_len), (v)))) { \
          _OB_LOG(WARN, "sql append value failed, ret %d, " #v " %.*s", \
              ret, static_cast<int32_t>(v_len), (v)); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_STRING_VALUE(sql, values, obj, member) \
    do { \
      if (!((obj).member##_).empty()) {\
        SQL_APPEND_COLUMN_NAME(sql, values, #member); \
        if (OB_SUCC(ret)) { \
          if (OB_SUCCESS != (ret = (values).append_fmt( \
              "%s'%.*s'", (values).empty() ? "" : ", ", \
              static_cast<int32_t>(((obj).member##_).length()), ((obj).member##_).ptr()))) { \
            OB_LOG(WARN, "sql append value failed", K(ret)); \
          } \
        } \
      }\
    } while (false)

#define SQL_COL_APPEND_CSTR_VALUE(sql, values, v, column) \
    SQL_COL_APPEND_STR_VALUE(sql, values, v, strlen(v), column)

#define SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, v_len, column) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append((values).empty() ? "'" : ", "))) { \
          _OB_LOG(WARN, "sql append ', ' failed, ret %d", ret); \
        } else if (OB_SUCCESS != (ret = sql_append_hex_escape_str((v), v_len, (values)))) { \
          _OB_LOG(WARN, "sql append escaped value failed, ret %d, " #v " %.*s", \
              ret, static_cast<int32_t>((v_len)), (v)); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, v, column) \
    SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, strlen(v), column)

#define SQL_APPEND_VALUE(sql, values, obj, member, fmt) \
    SQL_COL_APPEND_VALUE(sql, values, (obj).member##_, #member, fmt)

#define SQL_APPEND_INT_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_VALUE(sql, values, static_cast<int64_t>((obj).member##_), #member, "%ld")

#define SQL_APPEND_UINT_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_VALUE(sql, values, static_cast<uint64_t>((obj).member##_), #member, "%lu")

#define SQL_APPEND_UINT_VALUE_WITH_TENANT_ID(sql, values, obj, member) \
    SQL_COL_APPEND_VALUE(sql, values, static_cast<uint64_t>(ObSchemaUtils::get_extract_schema_id(\
        exec_tenant_id, (obj).member##_)), #member, "%lu")

#define SQL_APPEND_CSTR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_STR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_STR_VALUE( \
        sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)

#define SQL_APPEND_ESCAPE_CSTR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_ESCAPE_STR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_ESCAPE_STR_VALUE( \
        sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)


}
}

#endif // OCEANBASE_MYSQL_PROXY_H_

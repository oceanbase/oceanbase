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
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{
class ObSql;
struct ObSqlCtx;
class ObResultSet;
}
namespace share
{
namespace schema
{
class ObRoutineInfo;
}

}

namespace pl
{
class ObUserDefinedType;
}
namespace common
{
class ObIAllocator;
class ObString;

namespace sqlclient
{
class ObISQLConnection;
class ObCommonServerConnectionPool
{
public:
  ObCommonServerConnectionPool() : free_conn_count_(0), busy_conn_count_(0) {}
  virtual ~ObCommonServerConnectionPool() {}

  //dblink
  virtual int free_dblink_session(uint32_t sessid) = 0;
  virtual int release(common::sqlclient::ObISQLConnection *connection, const bool succ) = 0;
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
  ObISQLConnection() :
       oracle_mode_(false),
       is_inited_(false),
       dblink_id_(OB_INVALID_ID),
       dblink_driver_proto_(DBLINK_UNKNOWN),
       sessid_(-1),
       consumer_group_id_(0),
       has_reverse_link_credentials_(false),
       usable_(true),
       last_set_sql_mode_cstr_(NULL),
       last_set_sql_mode_cstr_buf_size_(0),
       last_set_client_charset_cstr_(NULL),
       last_set_connection_charset_cstr_(NULL),
       last_set_results_charset_cstr_(NULL),
       next_conn_(NULL),
       check_priv_(false)
  {}
  virtual ~ObISQLConnection() {
    allocator_.reset();
    last_set_sql_mode_cstr_buf_size_ = 0;
    last_set_sql_mode_cstr_ = NULL;
    last_set_client_charset_cstr_ = NULL;
    last_set_connection_charset_cstr_ = NULL;
    last_set_results_charset_cstr_ = NULL;
    next_conn_ = NULL;
  }

  // sql execute interface
  virtual int execute_read(const uint64_t tenant_id, const char *sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) = 0;
  virtual int execute_read(const int64_t cluster_id, const uint64_t tenant_id, const ObString &sql,
      ObISQLClient::ReadResult &res, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) = 0;
  virtual int execute_write(const uint64_t tenant_id, const char *sql,
      int64_t &affected_rows, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) = 0;
  virtual int execute_write(const uint64_t tenant_id, const ObString &sql,
      int64_t &affected_rows, bool is_user_sql = false,
      const common::ObAddr *sql_exec_addr = nullptr) = 0;
  virtual int execute_proc() { return OB_NOT_SUPPORTED; }
  virtual int execute_proc(const uint64_t tenant_id,
                        ObIAllocator &allocator,
                        ParamStore &params,
                        ObString &sql,
                        const share::schema::ObRoutineInfo &routine_info,
                        const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                        const ObTimeZoneInfo *tz_info,
                        ObObj *result) = 0;
  virtual int prepare(const char *sql) {
    UNUSED(sql);
    return OB_NOT_SUPPORTED;
  }
  virtual int bind_basic_type_by_pos(uint64_t position,
                                     void *param,
                                     int64_t param_size,
                                     int32_t datatype,
                                     int32_t &indicator)
  {
    UNUSEDx(position, param, param_size, datatype);
    return OB_NOT_SUPPORTED;
  }
  virtual int bind_array_type_by_pos(uint64_t position,
                                     void *array,
                                     int32_t *indicators,
                                     int64_t ele_size,
                                     int32_t ele_datatype,
                                     uint64_t array_size,
                                     uint32_t *out_valid_array_size)
  {
    UNUSEDx(position, array, ele_size, ele_datatype, array_size, out_valid_array_size);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_server_major_version(int64_t &major_version) {
    return OB_NOT_SUPPORTED;
  }
  virtual int get_package_udts(ObIAllocator &alloctor,
                               const common::ObString &database_name,
                               const common::ObString &package_name,
                               common::ObIArray<pl::ObUserDefinedType *> &udts,
                               uint64_t dblink_id,
                               uint64_t &next_object_id)
  { return OB_NOT_SUPPORTED; }

  // transaction interface
  virtual int start_transaction(const uint64_t &tenant_id, bool with_snap_shot = false) = 0;
  virtual int rollback() = 0;
  virtual int commit() = 0;

  // session environment
  virtual int get_session_variable(const ObString &name, int64_t &val) = 0;
  virtual int set_session_variable(const ObString &name, int64_t val) = 0;

  virtual int execute(const uint64_t tenant_id, ObIExecutor &executor)
  {
    UNUSED(tenant_id);
    UNUSED(executor);
    return OB_NOT_SUPPORTED;
  }


  // dblink
  virtual ObCommonServerConnectionPool *get_common_server_pool() = 0;
  void set_dblink_id(uint64_t dblink_id) { dblink_id_ = dblink_id; }
  uint64_t get_dblink_id() { return dblink_id_; }
  void set_sessid(uint32_t sessid) { sessid_ = sessid; }
  uint32_t get_sessid() { return sessid_; }
  void set_dblink_driver_proto(DblinkDriverProto dblink_driver_proto) { dblink_driver_proto_ = dblink_driver_proto; }
  DblinkDriverProto get_dblink_driver_proto() { return dblink_driver_proto_; }

  void set_mysql_compat_mode() { oracle_mode_ = false; }
  void set_oracle_compat_mode() { oracle_mode_ = true; }
  bool is_oracle_compat_mode() const { return oracle_mode_; }
  virtual int set_ddl_info(const void *ddl_info) { UNUSED(ddl_info); return OB_NOT_SUPPORTED; }
  virtual int set_tz_info_wrap(const ObTimeZoneInfoWrap &tz_info_wrap) { UNUSED(tz_info_wrap); return OB_NOT_SUPPORTED; }
  virtual void set_nls_formats(const ObString *nls_formats) { UNUSED(nls_formats); }
  virtual void set_is_load_data_exec(bool v) { UNUSED(v); }
  virtual void set_force_remote_exec(bool v) { UNUSED(v); }
  virtual void set_use_external_session(bool v) { UNUSED(v); }
  virtual int64_t get_cluster_id() const { return common::OB_INVALID_ID; }
  void set_session_init_status(bool status) { is_inited_ = status;}
  virtual void set_user_timeout(int64_t user_timeout) { UNUSED(user_timeout); }
  virtual int64_t get_user_timeout() const { return 0; }
  int is_session_inited(const sqlclient::dblink_param_ctx &param_ctx, bool &is_inited)
  {
    int ret = OB_SUCCESS;
    is_inited = false;
    int64_t sql_mode_len = 0;
    const char *sql_mode_cstr = param_ctx.set_sql_mode_cstr_;
    if (lib::is_oracle_mode()) {
      is_inited = is_inited_;
    } else if (FALSE_IT([&]{
                              if (OB_NOT_NULL(sql_mode_cstr)) {
                                is_inited = (0 == ObString(sql_mode_cstr).compare(last_set_sql_mode_cstr_));
                                sql_mode_len = STRLEN(sql_mode_cstr);
                              } else {
                                is_inited = true;
                              }
                           }())) {
    } else if (!is_inited && OB_NOT_NULL(sql_mode_cstr)) {
      if (sql_mode_len >= last_set_sql_mode_cstr_buf_size_) {
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc((sql_mode_len * 2)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          last_set_sql_mode_cstr_ = (char *)buf;
          last_set_sql_mode_cstr_buf_size_ = sql_mode_len * 2;
        }
      }
      if (OB_SUCC(ret)) {
        MEMCPY(last_set_sql_mode_cstr_, sql_mode_cstr, sql_mode_len);
        last_set_sql_mode_cstr_[sql_mode_len] = 0;
      }
    }
    if (param_ctx.set_client_charset_cstr_ != last_set_client_charset_cstr_ ||
        param_ctx.set_connection_charset_cstr_ != last_set_connection_charset_cstr_ ||
        param_ctx.set_results_charset_cstr_ != last_set_results_charset_cstr_ ||
        param_ctx.set_transaction_isolation_cstr_ != last_set_transaction_isolation_cstr_) {
      is_inited = false;
      last_set_client_charset_cstr_ = param_ctx.set_client_charset_cstr_;
      last_set_connection_charset_cstr_ = param_ctx.set_connection_charset_cstr_;
      last_set_results_charset_cstr_ = param_ctx.set_results_charset_cstr_;
      last_set_transaction_isolation_cstr_ = param_ctx.set_transaction_isolation_cstr_;
    }
    return ret;
  }
  void set_group_id(const int64_t v) {consumer_group_id_ = v; }
  int64_t get_group_id() const {return consumer_group_id_; }
  void set_reverse_link_creadentials(bool flag) { has_reverse_link_credentials_ = flag; }
  bool get_reverse_link_creadentials() { return has_reverse_link_credentials_; }
  void set_usable(bool flag) { usable_ = flag; }
  bool usable() { return usable_; }
  virtual int ping() { return OB_SUCCESS; }
  void dblink_rlock() { dblink_lock_.rlock()->lock(); }
  void dblink_unrlock() { dblink_lock_.rlock()->unlock(); }
  void dblink_wlock() { dblink_lock_.wlock()->lock(); }
  void dblink_unwlock() { dblink_lock_.wlock()->unlock(); }
  ObISQLConnection *get_next_conn() { return next_conn_; }
  void set_next_conn(ObISQLConnection *conn) { next_conn_ = conn; }
  void set_check_priv(bool on) { check_priv_ = on; }
  bool is_check_priv() { return check_priv_; }
protected:
  bool oracle_mode_;
  bool is_inited_; // for oracle dblink, we have to init remote env with some sql
  uint64_t dblink_id_; // for dblink, record dblink_id of a connection used by dblink
  DblinkDriverProto dblink_driver_proto_; //for dblink, record DblinkDriverProto of a connection used by dblink
  uint32_t sessid_;
  int64_t consumer_group_id_; //for resource isolation
  bool has_reverse_link_credentials_; // for dblink, mark if this link has credentials set
  bool usable_;  // usable_ = false: connection is unusable, should not execute query again.
  char *last_set_sql_mode_cstr_; // for mysql dblink to set sql mode
  int64_t last_set_sql_mode_cstr_buf_size_;
  const char *last_set_client_charset_cstr_;
  const char *last_set_connection_charset_cstr_;
  const char *last_set_results_charset_cstr_;
  const char *last_set_transaction_isolation_cstr_;
  common::ObArenaAllocator allocator_;
  obsys::ObRWLock dblink_lock_;
  ObISQLConnection *next_conn_; // used in dblink_conn_map_
  bool check_priv_;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_

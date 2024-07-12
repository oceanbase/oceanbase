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

#ifndef OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_H_
#define OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_H_

#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_2d_array.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/monitor/ob_exec_stat.h"
#include "observer/ob_restore_sql_modifier.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "observer/ob_inner_sql_rpc_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/tablelock/ob_table_lock_common.h"   //ObTableLockMode

namespace oceanbase
{
namespace obrpc
{
class ObInnerSqlRpcP;
}
namespace common
{
class ObString;
class ObServerConfig;
namespace sqlclient
{
class ObISQLResultHandler;
}
}

namespace share
{
class ObLSTableOperator;
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObSql;
}
namespace transaction
{
enum class ObTxDataSourceType : int64_t;
struct ObRegisterMdsFlag;
namespace tablelock
{
class ObLockRequest;
class ObLockObjRequest;
class ObLockTableRequest;
class ObLockTabletRequest;
class ObLockPartitionRequest;
class ObUnLockObjRequest;
class ObUnLockTableRequest;
class ObUnLockPartitionRequest;
class ObUnLockTabletRequest;
}
}
namespace observer
{
class ObInnerSQLResult;
class ObInnerSQLConnectionPool;
class ObVTIterCreator;
class ObVirtualTableIteratorFactory;
class ObInnerSQLReadContext;
class ObITimeRecord
{
public:
  virtual int64_t get_send_timestamp() const = 0;
  virtual int64_t get_receive_timestamp() const = 0;
  virtual int64_t get_enqueue_timestamp() const = 0;
  virtual int64_t get_run_timestamp() const = 0;
  virtual int64_t get_process_timestamp() const = 0;
  virtual int64_t get_single_process_timestamp() const = 0;
  virtual int64_t get_exec_start_timestamp() const = 0;
  virtual int64_t get_exec_end_timestamp() const = 0;
};

class ObInnerSQLConnection
    : public common::sqlclient::ObISQLConnection,
      public common::ObDLinkBase<ObInnerSQLConnection>
{
  friend class obrpc::ObInnerSqlRpcP;
public:
  static constexpr const char LABEL[] = "RPInnerSqlConn";
  class SavedValue
  {
  public:
    SavedValue()
    {
      reset();
    }
    inline void reset()
    {
      ref_ctx_ = NULL;
      execute_start_timestamp_ = 0;
      execute_end_timestamp_ = 0;
    }
  public:
    ObInnerSQLReadContext *ref_ctx_;
    int64_t execute_start_timestamp_;
    int64_t execute_end_timestamp_;
  };

  // Worker and session timeout may be altered in sql execution, restore to origin value after execution.
  class TimeoutGuard
  {
  public:
    TimeoutGuard(ObInnerSQLConnection &conn);
    ~TimeoutGuard();
  private:
    ObInnerSQLConnection &conn_;
    int64_t worker_timeout_;
    int64_t query_timeout_;
    int64_t trx_timeout_;
  };

public:
  class ObSqlQueryExecutor;

  ObInnerSQLConnection();
  virtual ~ObInnerSQLConnection();

  int init(ObInnerSQLConnectionPool *pool,
           share::schema::ObMultiVersionSchemaService *schema_service,
           sql::ObSql *ob_sql,
           ObVTIterCreator *vt_iter_creator,
           common::ObServerConfig *config,
           sql::ObSQLSessionInfo *extern_session = NULL,
           ObISQLClient *client_addr = NULL,
           ObRestoreSQLModifier *sql_modifer = NULL,
           const bool use_static_engine = false,
           const bool is_oracle_mode = false,
           const int32_t group_id = 0);
  int destroy(void);
  inline void reset() { destroy(); }
  virtual int execute_read(const uint64_t tenant_id, const char *sql,
                           common::ObISQLClient::ReadResult &res, bool is_user_sql = false,
                           const common::ObAddr *sql_exec_addr = nullptr/* ddl inner sql execution addr */) override;
  virtual int execute_read(const int64_t cluster_id, const uint64_t tenant_id, const ObString &sql,
                           common::ObISQLClient::ReadResult &res, bool is_user_sql = false,
                           const common::ObAddr *sql_exec_addr = nullptr/* ddl inner sql execution addr */) override;
  virtual int execute_write(const uint64_t tenant_id, const char *sql,
                            int64_t &affected_rows, bool is_user_sql = false,
                            const common::ObAddr *sql_exec_addr = nullptr/* ddl inner sql execution addr */) override;
  virtual int execute_write(const uint64_t tenant_id, const ObString &sql,
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
  virtual sqlclient::ObCommonServerConnectionPool *get_common_server_pool() override;
  virtual int rollback() override;
  virtual int commit() override;
  sql::ObSQLSessionInfo &get_session() { return NULL == extern_session_ ? inner_session_ : *extern_session_; }
  const sql::ObSQLSessionInfo &get_session() const { return NULL == extern_session_ ? inner_session_ : *extern_session_; }
  const sql::ObSQLSessionInfo *get_extern_session() const { return extern_session_; }
  // session environment
  virtual int get_session_variable(const ObString &name, int64_t &val) override;
  virtual int set_session_variable(const ObString &name, int64_t val) override;
  inline void set_spi_connection(bool is_spi_conn) { is_spi_conn_ = is_spi_conn; }
  int set_primary_schema_version(const common::ObIArray<int64_t> &primary_schema_versions);

  virtual int set_ddl_info(const void *ddl_info);
  virtual int set_tz_info_wrap(const ObTimeZoneInfoWrap &tz_info_wrap);
  virtual void set_nls_formats(const ObString *nls_formats);
  virtual void set_is_load_data_exec(bool v);
  virtual void set_force_remote_exec(bool v) { force_remote_execute_ = v; }
  virtual void set_use_external_session(bool v) { use_external_session_ = v; }
  bool is_nested_conn();
  virtual void set_user_timeout(int64_t timeout) { user_timeout_ = timeout; }
  virtual int64_t get_user_timeout() const { return user_timeout_; }
  void ref();
  // when ref count decrease to zero, revert connection to connection pool.
  void unref();
  int64_t get_ref() const { return ref_cnt_; }

  ObVTIterCreator *get_vt_iter_creator() const { return vt_iter_creator_; }
  ObInnerSQLReadContext *&get_prev_read_ctx() { return ref_ctx_; }
  void dump_conn_bt_info();
  class RefGuard {
  public:
    explicit RefGuard(ObInnerSQLConnection &conn)
      : conn_(conn)
    {
      conn_.ref();
    }
    ~RefGuard()
    {
      conn_.unref();
    }
    ObInnerSQLConnection &get_conn() { return conn_; }

  private:
    ObInnerSQLConnection &conn_;
  };
public:
  int64_t get_send_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_receive_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_enqueue_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_run_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_process_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_single_process_timestamp() const { return get_session().get_query_start_time(); }
  int64_t get_exec_start_timestamp() const { return execute_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return execute_end_timestamp_; }
  common::ObISQLClient *get_associated_client() const { return associated_client_; }
  bool is_in_trans() const { return is_in_trans_; }
  void set_is_in_trans(const bool is_in_trans) { is_in_trans_ = is_in_trans; }
  bool is_resource_conn() const { return is_resource_conn_; }
  void set_is_resource_conn(const bool is_resource_conn) { is_resource_conn_ = is_resource_conn; }
  void set_resource_conn_id(uint64_t resource_conn_id) { resource_conn_id_ = resource_conn_id; }
  uint64_t get_resource_conn_id() const { return resource_conn_id_; }
  const common::ObAddr &get_resource_svr() const { return resource_svr_; }
  void set_resource_svr(const common::ObAddr &resource_svr) { resource_svr_ = resource_svr; }
  void set_is_idle(bool is_idle) { is_idle_ = is_idle; }
  bool is_idle() const { return is_idle_; }
  void set_force_no_reuse(bool force_no_reuse) { force_no_reuse_ = force_no_reuse; }
  bool is_force_no_reuse() const { return force_no_reuse_; }

  void set_last_query_timestamp(int64_t last_query_timestamp)
  { last_query_timestamp_ = last_query_timestamp; }
  int64_t get_last_query_timestamp() const { return last_query_timestamp_; }
  void reset_resource_conn_info()
  { resource_conn_id_ = OB_INVALID_ID; last_query_timestamp_ = 0; resource_svr_.reset(); }

public:

  sql::ObSql *get_sql_engine() { return ob_sql_; }

  virtual int execute(const uint64_t tenant_id, sqlclient::ObIExecutor &executor) override;

  int forward_request(const uint64_t tenant_id,
                      const int64_t op_type,
                      const ObString &sql,
                      ObInnerSQLResult &res,
                      const int32_t group_id = 0);

public:
  // nested session and sql execute for foreign key.
  int begin_nested_session(sql::ObSQLSessionInfo::StmtSavedValue &saved_session,
                           SavedValue &saved_conn, bool skip_cur_stmt_tables);
  int end_nested_session(sql::ObSQLSessionInfo::StmtSavedValue &saved_session,
                         SavedValue &saved_conn);
  bool is_extern_session() const { return NULL != extern_session_; }
  bool is_inner_session() const { return NULL == extern_session_; }
  bool is_spi_conn() const { return is_spi_conn_; }
  // set timeout to session variable
  int set_session_timeout(int64_t query_timeout, int64_t trx_timeout);

public:// for mds
  int register_multi_data_source(const uint64_t &tenant_id,
                                 const share::ObLSID ls_id,
                                 const transaction::ObTxDataSourceType type,
                                 const char *buf,
                                 const int64_t buf_len,
                                 const transaction::ObRegisterMdsFlag &register_flag = transaction::ObRegisterMdsFlag());

public:
  static int process_record(sql::ObResultSet &result_set,
                            sql::ObSqlCtx &sql_ctx,
                            sql::ObSQLSessionInfo &session,
                            ObITimeRecord &time_record,
                            int last_ret,
                            int64_t execution_id,
                            int64_t ps_stmt_id,
                            ObWaitEventDesc &max_wait_desc,
                            ObWaitEventStat &total_wait_desc,
                            sql::ObExecRecord &exec_record,
                            sql::ObExecTimestamp &exec_timestamp,
                            bool has_tenant_resource,
                            const ObString &ps_sql,
                            bool is_from_pl = false,
                            ObString *pl_exec_params = NULL);
  static int process_audit_record(sql::ObResultSet &result_set,
                                  sql::ObSqlCtx &sql_ctx,
                                  sql::ObSQLSessionInfo &session,
                                  int last_ret,
                                  int64_t execution_id,
                                  int64_t ps_stmt_id,
                                  bool has_tenant_resource,
                                  const ObString &ps_sql,
                                  bool is_from_pl = false);
  static void record_stat(sql::ObSQLSessionInfo &session,
                          const sql::stmt::StmtType type,
                          bool is_from_pl = false);

  static int init_session_info(sql::ObSQLSessionInfo *session,
                               const bool is_extern_session,
                               const bool is_oracle_mode,
                               const bool is_ddl);

  int64_t get_init_timestamp() const { return init_timestamp_; }
  int switch_tenant(const uint64_t tenant_id);
  bool is_local_execute(const int64_t cluster_id, const uint64_t tenant_id);
public:
  static const int64_t LOCK_RETRY_TIME = 1L * 1000 * 1000;
  static const int64_t TOO_MANY_REF_ALERT = 1024;
  static const uint32_t INNER_SQL_SESS_ID = 1;
  static const uint32_t INNER_SQL_PROXY_SESS_ID = 1;
  static const int64_t MAX_BT_SIZE = 20;
  static const int64_t EXTRA_REFRESH_LOCATION_TIME = 1L * 1000 * 1000;
private:
  int init_session(sql::ObSQLSessionInfo* session_info = NULL, const bool is_ddl = false);
  int init_result(ObInnerSQLResult &res,
                  ObVirtualTableIteratorFactory *vt_iter_factory,
                  int64_t retry_cnt,
                  share::schema::ObSchemaGetterGuard &schema_guard,
                  pl::ObPLBlockNS *secondary_namespace,
                  bool is_prepare_protocol = false,
                  bool is_prepare_stage = false,
                  bool is_dynamic_sql = false,
                  bool is_dbms_sql = false,
                  bool is_cursor = false);
  int process_retry(ObInnerSQLResult &res,
                    int do_ret,
                    int64_t abs_timeout_us,
                    bool &need_retry,
                    int64_t retry_cnt);
  template <typename T>
  int process_final(const T &sql,
                    ObInnerSQLResult &res,
                    int do_ret);
  // execute with retry
  int query(sqlclient::ObIExecutor &executor,
            ObInnerSQLResult &res,
            ObVirtualTableIteratorFactory *vt_iter_factory = NULL);
  int do_query(sqlclient::ObIExecutor &executor, ObInnerSQLResult &res);

  // set timeout to session variable
  int set_timeout(int64_t &abs_timeout_us);

  lib::Worker::CompatMode get_compat_mode() const;

  int nonblock_get_leader(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      common::ObAddr &leader);

  int execute_read_inner(const int64_t cluster_id, const uint64_t tenant_id, const ObString &sql,
                         common::ObISQLClient::ReadResult &res, bool is_user_sql = false,
                         const common::ObAddr *sql_exec_addr = nullptr);
  int execute_write_inner(const uint64_t tenant_id, const ObString &sql, int64_t &affected_rows,
      bool is_user_sql = false, const common::ObAddr *sql_exec_addr = nullptr);
  int start_transaction_inner(const uint64_t &tenant_id, bool with_snap_shot = false);
  template <typename T>
  int retry_while_no_tenant_resource(const int64_t cluster_id, const uint64_t &tenant_id, T function);

  int forward_request_(const uint64_t tenant_id,
                       const int64_t op_type,
                       const ObString &sql,
                       ObInnerSQLResult &res,
                       const int32_t group_id = 0);
  int get_session_timeout_for_rpc(int64_t &query_timeout, int64_t &trx_timeout);
private:
  bool inited_;
  observer::ObQueryRetryCtrl retry_ctrl_;
  sql::ObSQLSessionInfo inner_session_;
  sql::ObSQLSessionInfo *extern_session_;   // nested sql and spi both use it, rename to extern.
  bool is_spi_conn_;
  int64_t ref_cnt_;
  ObInnerSQLConnectionPool *pool_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  sql::ObSql *ob_sql_;
  ObVTIterCreator *vt_iter_creator_;
  ObInnerSQLReadContext *ref_ctx_;
  ObRestoreSQLModifier *sql_modifier_;
  int64_t init_timestamp_;
  int64_t tid_;
  int bt_size_;
  void *bt_addrs_[MAX_BT_SIZE];
  int64_t execute_start_timestamp_;
  int64_t execute_end_timestamp_;
  common::ObServerConfig *config_;
  common::ObISQLClient *associated_client_;

  // for using rpc to send sql to the server which has tenant resource
  bool is_in_trans_;
  bool is_resource_conn_;
  bool is_idle_; // for resource_conn_
  common::ObAddr resource_svr_; // server of destination in local rpc call
  uint64_t resource_conn_id_; // resource conn_id of dst srv
  int64_t last_query_timestamp_;
  /*
   This flag is used by ddl to force this inner sql bypass the local-optimized path, always execute it through rpc

   Why do we need this? 
   DDL needs to issue an "insert into select" sql to build the single replica, which will deal with user data.
   However, in the local-optimized path, the sql will be executed using a "fake user tenant session", whose 
   login_tenant_id is actually sys tenant. This leads to unexpected privilege check result since privilege
   check always uses login_tenant_id due to security. To solve this problem, we add this flag to force all
   "insert into select" inner sqls issued by DDL to use remote execution, where observer will create a real
   user session to execute the sql.

   What do I mean by saying "fake user tenant session"?
   a session created by sys tenant then uses tenant switch to behave like user tenant.
  */
  bool force_remote_execute_;
  bool force_no_reuse_;

  // ask the inner sql connection to use external session instead of internal one
  // this enables show session / kill session using sql query command
  bool use_external_session_;
  int32_t group_id_;
  //support set user timeout of stream rpc but not depend on internal_sql_execute_timeout
  int64_t user_timeout_;
  DISABLE_COPY_ASSIGN(ObInnerSQLConnection);
};

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_H_

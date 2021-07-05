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
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase {
namespace common {
class ObString;
class ObServerConfig;
namespace sqlclient {
class ObISQLResultHandler;
}
}  // namespace common

namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace sql {
class ObSql;
}
namespace observer {
class ObInnerSQLResult;
class ObInnerSQLConnectionPool;
class ObVTIterCreator;
class ObVirtualTableIteratorFactory;
class ObInnerSQLReadContext;
class ObITimeRecord {
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

typedef common::Ob2DArray<ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false> ParamStore;

class ObInnerSQLConnection : public common::sqlclient::ObISQLConnection,
                             public common::ObDLinkBase<ObInnerSQLConnection> {
public:
  static constexpr const char LABEL[] = "RPInnerSqlConn";
  class SavedValue {
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
    ObInnerSQLReadContext* ref_ctx_;
    int64_t execute_start_timestamp_;
    int64_t execute_end_timestamp_;
  };

  // Worker and session timeout may be altered in sql execution, restore to origin value after execution.
  class TimeoutGuard {
  public:
    TimeoutGuard(ObInnerSQLConnection& conn);
    ~TimeoutGuard();

  private:
    ObInnerSQLConnection& conn_;
    int64_t worker_timeout_;
    int64_t query_timeout_;
    int64_t trx_timeout_;
  };

public:
  class ObSqlQueryExecutor;

  ObInnerSQLConnection();
  virtual ~ObInnerSQLConnection();

  int init(ObInnerSQLConnectionPool* pool, share::schema::ObMultiVersionSchemaService* schema_service,
      sql::ObSql* ob_sql, ObVTIterCreator* vt_iter_creator,
      const share::ObPartitionTableOperator* partiton_table_operator, common::ObServerConfig* config,
      sql::ObSQLSessionInfo* extern_session = NULL, ObISQLClient* client_addr = NULL,
      ObRestoreSQLModifier* sql_modifer = NULL);
  int destroy(void);
  inline void reset()
  {
    destroy();
  }

  virtual int execute_read(const uint64_t tenant_id, const char* sql, common::ObISQLClient::ReadResult& res,
      bool is_user_sql = false, bool is_from_pl = false) override;
  virtual int execute_write(
      const uint64_t tenant_id, const char* sql, int64_t& affected_rows, bool is_user_sql = false) override;

  virtual int start_transaction(bool with_snap_shot = false) override;
  virtual int rollback() override;
  virtual int commit() override;
  sql::ObSQLSessionInfo& get_session()
  {
    return NULL == extern_session_ ? inner_session_ : *extern_session_;
  }
  const sql::ObSQLSessionInfo& get_session() const
  {
    return NULL == extern_session_ ? inner_session_ : *extern_session_;
  }
  const sql::ObSQLSessionInfo* get_extern_session() const
  {
    return extern_session_;
  }
  // session environment
  virtual int get_session_variable(const ObString& name, int64_t& val) override;
  virtual int set_session_variable(const ObString& name, int64_t val) override;
  inline void set_spi_connection(bool is_spi_conn)
  {
    is_spi_conn_ = is_spi_conn;
  }
  int set_primary_schema_version(const common::ObIArray<int64_t>& primary_schema_versions);

  void ref();
  // when ref count decrease to zero, revert connection to connection pool.
  void unref();
  int64_t get_ref() const
  {
    return ref_cnt_;
  }

  ObVTIterCreator* get_vt_iter_creator() const
  {
    return vt_iter_creator_;
  }
  ObInnerSQLReadContext*& get_prev_read_ctx()
  {
    return ref_ctx_;
  }
  void dump_conn_bt_info();
  class RefGuard {
  public:
    explicit RefGuard(ObInnerSQLConnection& conn) : conn_(conn)
    {
      conn_.ref();
    }
    ~RefGuard()
    {
      conn_.unref();
    }
    ObInnerSQLConnection& get_conn()
    {
      return conn_;
    }

  private:
    ObInnerSQLConnection& conn_;
  };

public:
  int64_t get_send_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_receive_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_enqueue_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_run_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_process_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_single_process_timestamp() const
  {
    return get_session().get_query_start_time();
  }
  int64_t get_exec_start_timestamp() const
  {
    return execute_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return execute_end_timestamp_;
  }
  common::ObISQLClient* get_associated_client() const
  {
    return associated_client_;
  }

public:
  int prepare(const uint64_t tenant_id, const ObString& sql, bool is_dynamic_sql, bool is_dbms_sql, bool is_cursor,
      common::ObISQLClient::ReadResult& res);
  int execute(const uint64_t tenant_id, const ObPsStmtId client_stmt_id, const sql::stmt::StmtType stmt_type,
      ParamStore& params, common::ObISQLClient::ReadResult& res, bool is_from_pl = false, bool is_dynamic = false);

  virtual int execute(const uint64_t tenant_id, sqlclient::ObIExecutor& executor) override;

public:
  // nested session and sql execute for foreign key.
  int begin_nested_session(
      sql::ObSQLSessionInfo::StmtSavedValue& saved_session, SavedValue& saved_conn, bool skip_cur_stmt_tables);
  int end_nested_session(sql::ObSQLSessionInfo::StmtSavedValue& saved_session, SavedValue& saved_conn);
  int set_foreign_key_cascade(bool is_cascade);
  int get_foreign_key_cascade(bool& is_cascade) const;
  int set_foreign_key_check_exist(bool is_check_exist);
  int get_foreign_key_check_exist(bool& is_check_exist) const;
  bool is_extern_session() const
  {
    return NULL != extern_session_;
  }
  bool is_inner_session() const
  {
    return NULL == extern_session_;
  }
  bool is_spi_conn() const
  {
    return is_spi_conn_;
  }

public:
  static int process_record(ObInnerSQLResult& res, sql::ObSQLSessionInfo& session, ObITimeRecord& time_record,
      int last_ret, int64_t execution_id, int64_t ps_stmt_id, int64_t routine_id, ObWaitEventDesc& max_wait_desc,
      ObWaitEventStat& total_wait_desc, sql::ObExecRecord& exec_record, sql::ObExecTimestamp& exec_timestamp,
      bool is_from_pl = false);
  static void record_stat(sql::ObSQLSessionInfo& session, const sql::stmt::StmtType type, bool is_from_pl = false);

public:
  static const int64_t LOCK_RETRY_TIME = 1L * 1000 * 1000;
  static const int64_t TOO_MANY_REF_ALERT = 1024;
  static const uint32_t INNER_SQL_SESS_VERSION = 0;
  static const uint32_t INNER_SQL_SESS_ID = 1;
  static const uint32_t INNER_SQL_PROXY_SESS_ID = 1;
  static const int64_t MAX_BT_SIZE = 20;
  static const int64_t EXTRA_REFRESH_LOCATION_TIME = 1L * 1000 * 1000;

private:
  int init_session(sql::ObSQLSessionInfo* session_info = NULL);
  int init_result(ObInnerSQLResult& res, ObVirtualTableIteratorFactory* vt_iter_factory, int64_t retry_cnt,
      share::schema::ObSchemaGetterGuard& schema_guard, bool is_prepare_protocol = false, bool is_prepare_stage = false,
      bool is_from_pl = false, bool is_dynamic_sql = false, bool is_dbms_sql = false, bool is_cursor = false);
  int process_retry(
      ObInnerSQLResult& res, int& do_ret, int64_t abs_timeout_us, bool& need_retry, int64_t retry_cnt, bool is_from_pl);
  template <typename T>
  int process_final(const T& sql, ObInnerSQLResult& res, int do_ret);
  // execute with retry
  int query(sqlclient::ObIExecutor& executor, ObInnerSQLResult& res,
      ObVirtualTableIteratorFactory* vt_iter_factory = NULL, bool is_from_pl = false);
  int do_query(sqlclient::ObIExecutor& executor, ObInnerSQLResult& res);

  int prepare(const common::ObString& sql, bool is_dynamic_sql, bool is_dbms_sql, bool is_cursor, ObInnerSQLResult& res,
      ObVirtualTableIteratorFactory* vt_iter_factory = NULL);
  int do_prepare(const common::ObString& sql, ObInnerSQLResult& res);
  int execute(ParamStore& params, ObInnerSQLResult& res, ObVirtualTableIteratorFactory* vt_iter_factory = NULL,
      bool is_from_pl = false, bool is_dynamic = false);
  int do_execute(const ParamStore& params, ObInnerSQLResult& res);
  int switch_tenant(const uint64_t tenant_id);

  // set timeout to session variable
  int set_timeout(int64_t& abs_timeout_us, bool is_from_pl);
  int set_session_timeout(int64_t query_timeout, int64_t trx_timeout);
  int process_schema_version(sql::ObResultSet& result_set);
  bool is_ddl_from_primary()
  {
    return 0 < primary_schema_versions_.count();
  }

  share::ObWorker::CompatMode get_compat_mode() const;

private:
  bool inited_;
  sql::ObSQLSessionInfo inner_session_;
  sql::ObSQLSessionInfo* extern_session_;  // nested sql and spi both use it, rename to extern.
  bool is_spi_conn_;
  int64_t ref_cnt_;
  ObInnerSQLConnectionPool* pool_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  sql::ObSql* ob_sql_;
  ObVTIterCreator* vt_iter_creator_;
  ObInnerSQLReadContext* ref_ctx_;
  const share::ObPartitionTableOperator* partition_table_operator_;
  ObRestoreSQLModifier* sql_modifier_;
  int64_t init_timestamp_;
  int64_t tid_;
  int bt_size_;
  void* bt_addrs_[MAX_BT_SIZE];
  int64_t execute_start_timestamp_;
  int64_t execute_end_timestamp_;
  common::ObServerConfig* config_;
  common::ObISQLClient* associated_client_;
  // used in standby cluster
  // save primary cluster's DDL schema version here
  common::ObSEArray<int64_t, 5> primary_schema_versions_;

  DISABLE_COPY_ASSIGN(ObInnerSQLConnection);
};

}  // end of namespace observer
}  // end of namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_INNER_SQL_CONNECTION_H_

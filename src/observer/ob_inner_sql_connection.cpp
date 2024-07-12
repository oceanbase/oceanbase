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

#define USING_LOG_PREFIX SERVER

#include "ob_inner_sql_connection.h"
#include "lib/worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_timeout_ctx.h"
#include "common/ob_smart_call.h"
#include "share/ob_debug_sync.h"
#include "share/ob_time_utility2.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "observer/ob_server.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/mysql/obmp_base.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "ob_inner_sql_connection_pool.h"
#include "ob_inner_sql_read_context.h"
#include "ob_inner_sql_result.h"
#include "storage/tx/ob_trans_define.h"      // ObTransIsolation
#include "storage/tx/ob_trans_service.h"
#include "sql/resolver/ob_schema_checker.h"
#include "observer/ob_inner_sql_rpc_processor.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#ifdef OB_BUILD_AUDIT_SECURITY
#include "sql/monitor/ob_security_audit_utils.h"
#endif
#include "sql/plan_cache/ob_ps_cache.h"
#include "observer/mysql/obmp_stmt_execute.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share;
using namespace share::schema;
using namespace transaction::tablelock;

namespace observer
{

constexpr const char ObInnerSQLConnection::LABEL[];

class ObInnerSQLConnection::ObSqlQueryExecutor : public sqlclient::ObIExecutor
{
public:
  explicit ObSqlQueryExecutor(const ObString &sql) : sql_(sql) {}
  explicit ObSqlQueryExecutor(const char *sql) : sql_(ObString::make_string(sql)) {}

  virtual ~ObSqlQueryExecutor() {}

  virtual int execute(sql::ObSql &engine, sql::ObSqlCtx &ctx, sql::ObResultSet &res)
  {
    int ret = OB_SUCCESS;
    SQL_INFO_GUARD(sql_, ObString(OB_MAX_SQL_ID_LENGTH, ctx.sql_id_));
    // Deep copy sql, because sql may be destroyed before result iteration.
    const int64_t alloc_size = sizeof(ObString) + sql_.length() + 1; // 1 for C terminate char
    void *mem = res.get_mem_pool().alloc(alloc_size);
    if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      ObString *dup_sql = new (mem) ObString(sql_.length(), sql_.length(),
                                             static_cast<char *>(mem) + sizeof(ObString));
      MEMCPY(dup_sql->ptr(), sql_.ptr(), sql_.length());
      dup_sql->ptr()[sql_.length()] = '\0';
      res.get_session().store_query_string(*dup_sql);
      ret = engine.stmt_query(*dup_sql, ctx, res);
    }
    return ret;
  }

  // process result after result open
  virtual int process_result(sql::ObResultSet &) override { return OB_SUCCESS; }

  INHERIT_TO_STRING_KV("ObIExecutor", ObIExecutor, K_(sql));

private:
  ObString sql_;
};

ObInnerSQLConnection::TimeoutGuard::TimeoutGuard(ObInnerSQLConnection &conn)
  : conn_(conn)
{
  int ret = OB_SUCCESS;
  worker_timeout_ = THIS_WORKER.get_timeout_ts();
  if (OB_FAIL(conn_.get_session().get_query_timeout(query_timeout_))
      || OB_FAIL(conn_.get_session().get_tx_timeout(trx_timeout_))) {
    LOG_ERROR("get timeout failed", KR(ret), K(query_timeout_), K(trx_timeout_));
  }
}

ObInnerSQLConnection::TimeoutGuard::~TimeoutGuard()
{
  int ret = OB_SUCCESS;
  if (THIS_WORKER.get_timeout_ts() != worker_timeout_) {
    THIS_WORKER.set_timeout_ts(worker_timeout_);
  }

  int64_t query_timeout = 0;
  int64_t trx_timeout = 0;
  if (OB_FAIL(conn_.get_session().get_query_timeout(query_timeout))
      || OB_FAIL(conn_.get_session().get_tx_timeout(trx_timeout))) {
    LOG_ERROR("get timeout failed", KR(ret), K(query_timeout), K(trx_timeout));
  } else {
    if (query_timeout != query_timeout_ || trx_timeout != trx_timeout_) {
      if (OB_FAIL(conn_.set_session_timeout(query_timeout_, trx_timeout_))) {
        LOG_ERROR("set session timeout failed", K(ret));
      }
    }
  }
}

ObInnerSQLConnection::ObInnerSQLConnection()
    : inited_(false), inner_session_(), extern_session_(NULL),
      is_spi_conn_(false),
      ref_cnt_(0), pool_(NULL), schema_service_(NULL), ob_sql_(NULL), vt_iter_creator_(NULL),
      ref_ctx_(NULL),
      sql_modifier_(NULL),
      init_timestamp_(0),
      tid_(-1),
      bt_size_(0),
      bt_addrs_(),
      execute_start_timestamp_(0),
      execute_end_timestamp_(0),
      config_(NULL),
      associated_client_(NULL),
      is_in_trans_(false),
      is_resource_conn_(false),
      is_idle_(true),
      resource_svr_(),
      resource_conn_id_(OB_INVALID_ID),
      last_query_timestamp_(0),
      force_remote_execute_(false),
      force_no_reuse_(false),
      use_external_session_(false),
      group_id_(0),
      user_timeout_(0)
{
}

ObInnerSQLConnection::~ObInnerSQLConnection()
{
  if (0 < ref_cnt_) {
    LOG_ERROR_RET(OB_ERROR, "connection be referenced while destruct", K_(ref_cnt));
  }
  if (OB_NOT_NULL(inner_session_.get_tx_desc())) {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_SUCC(guard.switch_to(inner_session_.get_tx_desc()->get_tenant_id(), false))) {
      MTL(transaction::ObTransService*)->release_tx(*inner_session_.get_tx_desc());
    }
    {
      ObSQLSessionInfo::LockGuard guard(inner_session_.get_thread_data_lock());
      inner_session_.get_tx_desc() = NULL;
    }
  }
}

int ObInnerSQLConnection::init(ObInnerSQLConnectionPool *pool,
                               ObMultiVersionSchemaService *schema_service,
                               ObSql *ob_sql,
                               ObVTIterCreator *vt_iter_creator,
                               common::ObServerConfig *config,
                               sql::ObSQLSessionInfo *extern_session, /* = NULL */
                               ObISQLClient *client_addr, /* = NULL */
                               ObRestoreSQLModifier *sql_modifier /* = NULL */,
                               const bool use_static_engine /* = false */,
                               const bool is_oracle_mode /* = false */,
                               const int32_t group_id /* = 0*/)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("connection init twice", K(ret));
  } else if ((NULL == pool && NULL == sql_modifier) || NULL == schema_service
      || NULL == ob_sql || NULL == vt_iter_creator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service should not be NULL", K(ret), KP(sql_modifier),
        KP(pool), KP(schema_service), KP(ob_sql), KP(vt_iter_creator));
  } else {
    pool_ = pool;
    schema_service_ = schema_service;
    ob_sql_ = ob_sql;
    vt_iter_creator_ = vt_iter_creator;
    sql_modifier_ = sql_modifier;
    init_timestamp_ = ObTimeUtility::current_time();
    tid_ = GETTID();
    if (NULL == extern_session || 0 != EVENT_CALL(EventTable::EN_INNER_SQL_CONN_LEAK_CHECK)) {
      // Only backtrace internal used connection to avoid performance problems.
      bt_size_ = ob_backtrace(bt_addrs_, MAX_BT_SIZE);
    }
    config_ = config;
    associated_client_ = client_addr;
    if (NULL != client_addr) {
      oracle_mode_ = client_addr->is_oracle_mode();
    } else if (NULL != extern_session) {
      oracle_mode_ = ORACLE_MODE == extern_session->get_compatibility_mode();
    } else {
      oracle_mode_ = is_oracle_mode;
    }
    if (OB_FAIL(init_session(extern_session, use_static_engine))) {
      LOG_WARN("init session failed", K(ret));
    } else {
      group_id_ = group_id;
      inited_ = true;
    }
  }
  return ret;
}

int ObInnerSQLConnection::destroy()
{
  int ret = OB_SUCCESS;
  // uninited connection can be destroy too
  if (inited_) {
    if (0 < ref_cnt_) {
      ret = OB_REF_NUM_NOT_ZERO;
      LOG_ERROR("connection be referenced while destroy", K(ret), K_(ref_cnt));
    }

    // continue execute while error happen.
    inited_ = false;
    ref_cnt_ = 0;
    pool_ = NULL;
    schema_service_ = NULL;
    inner_session_.destroy();
    extern_session_ = NULL;
    config_ = NULL;
    associated_client_ = NULL;
    ref_ctx_ = NULL;
    user_timeout_ = 0;
  }
  return ret;
}

void ObInnerSQLConnection::ref()
{
  if (!inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "not init");
  } else {
    ref_cnt_++;
    if (ref_cnt_ > TOO_MANY_REF_ALERT) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "connection be referenced too many times, this should be rare",
          K_(ref_cnt));
    }
  }
}

void ObInnerSQLConnection::unref()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(lbt()));
  } else if (OB_ISNULL(pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not init conn pool", K(ret));
  } else {
    if (0 == --ref_cnt_) {
      if (OB_FAIL(pool_->revert(this))) {
        LOG_WARN("revert connection failed", K(ret));
      }
    } else {
      // see
      // extern_session_ = NULL;
    }
  }
}

int ObInnerSQLConnection::set_ddl_info(const void *ddl_info)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo &session = get_session();
  if (OB_ISNULL(ddl_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ddl_info));
  } else {
    const ObSessionDDLInfo *tmp_ddl_info = reinterpret_cast<const ObSessionDDLInfo *>(ddl_info);
    session.set_ddl_info(*tmp_ddl_info);
  }
  return ret;
}

void ObInnerSQLConnection::set_nls_formats(const ObString *nls_formats)
{
  get_session().set_nls_formats(nls_formats);
}

int ObInnerSQLConnection::set_tz_info_wrap(const ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo &session = get_session();
  if (OB_FAIL(session.set_tz_info_wrap(tz_info_wrap))) {
    LOG_WARN("fail to set time zone info", K(ret), K(tz_info_wrap));
  }
  return ret;
}

void ObInnerSQLConnection::set_is_load_data_exec(bool v)
{
  get_session().set_load_data_exec_session(v);
}

int ObInnerSQLConnection::init_session_info(
    sql::ObSQLSessionInfo *session,
    const bool is_extern_session,
    const bool is_oracle_mode,
    const bool is_ddl)
{
  int ret = OB_SUCCESS;
  if (NULL == session) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init session info, not pointer", K(ret), KPC(session));
  } else {
    // called in init(), can not check inited_ flag.
    const bool print_info_log = false;
    const bool is_sys_tenant = true;
    ObPCMemPctConf pc_mem_conf;
    session->set_inner_session();
    ObObj mysql_mode;
    ObObj oracle_mode;
    mysql_mode.set_int(0);
    oracle_mode.set_int(1);
    ObObj mysql_sql_mode;
    ObObj oracle_sql_mode;
    mysql_sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
    oracle_sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
    if (OB_FAIL(session->load_default_sys_variable(print_info_log, is_sys_tenant))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(session->update_max_packet_size())) {
      LOG_WARN("fail to update max packet size", K(ret));
    } else if (OB_FAIL(session->init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init tenant", K(ret));
    } else {
      if (!is_extern_session) { // if not exetern session
        if(OB_FAIL(session->switch_tenant(OB_SYS_TENANT_ID))) {
          LOG_WARN("Init sys tenant in session error", K(ret));
        } else if (OB_FAIL(session->set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID))) {
          LOG_WARN("Set sys user in session error", K(ret));
        } else {
          session->set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
          session->set_database_id(OB_SYS_DATABASE_ID);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(session->update_sys_variable(
            SYS_VAR_SQL_MODE, is_oracle_mode ? oracle_sql_mode : mysql_sql_mode))) {
          LOG_WARN("update sys variables failed", K(ret));
        } else if (OB_FAIL(session->update_sys_variable(
            SYS_VAR_OB_COMPATIBILITY_MODE, is_oracle_mode ? oracle_mode : mysql_mode))) {
          LOG_WARN("update sys variables failed", K(ret));
        } else if (OB_FAIL(session->update_sys_variable(
            SYS_VAR_NLS_DATE_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT))) {
          LOG_WARN("update sys variables failed", K(ret));
        } else if (OB_FAIL(session->update_sys_variable(
            SYS_VAR_NLS_TIMESTAMP_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT))) {
          LOG_WARN("update sys variables failed", K(ret));
        } else if (OB_FAIL(session->update_sys_variable(
            SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT))) {
          LOG_WARN("update sys variables failed", K(ret));
        } else {
          ObString database_name(OB_SYS_DATABASE_NAME);
          if (OB_FAIL(session->set_default_database(database_name))) {
            LOG_WARN("fail to set default database", K(ret), K(database_name));
          } else if (OB_FAIL(session->get_pc_mem_conf(pc_mem_conf))) {
            LOG_WARN("fail to get pc mem conf", K(ret));
          } else {
            session->set_database_id(OB_SYS_DATABASE_ID);
            //TODO shengle ?
            session->get_ddl_info().set_is_ddl(is_ddl);
            session->reset_timezone();
            session->init_use_rich_format();
          }
        }
      }
    }
  }
  return ret;
}

int ObInnerSQLConnection::init_session(sql::ObSQLSessionInfo* extern_session, const bool is_ddl)
{
  int ret = OB_SUCCESS;
  if (NULL == extern_session) {
    sql::ObSQLSessionInfo * inner_session = &inner_session_;
    ObArenaAllocator *allocator = NULL;
    const bool is_extern_session = false;
    if (OB_FAIL(inner_session->init(INNER_SQL_SESS_ID, INNER_SQL_PROXY_SESS_ID, allocator))) {
      LOG_WARN("init session failed", K(ret));
    } else if (OB_FAIL(init_session_info(inner_session, is_extern_session, oracle_mode_, is_ddl))) {
      LOG_WARN("fail to init session info", K(ret), KPC(inner_session));
    }
  } else {
    extern_session_ = extern_session;
  }
  return ret;
}

int ObInnerSQLConnection::init_result(ObInnerSQLResult &res,
                                      ObVirtualTableIteratorFactory *vt_iter_factory,
                                      int64_t retry_cnt,
                                      ObSchemaGetterGuard &schema_guard,
                                      pl::ObPLBlockNS *secondary_namespace,
                                      bool is_prepare_protocol,
                                      bool is_prepare_stage,
                                      bool is_dynamic_sql,
                                      bool is_dbms_sql,
                                      bool is_cursor)
{
  int ret = OB_SUCCESS;
  UNUSED(vt_iter_factory);
  sql::ObResultSet &result_set = res.result_set();
  const ObGlobalContext &gctx = ObServer::get_instance().get_gctx();
  result_set.get_exec_context().get_task_exec_ctx().set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  result_set.get_exec_context().get_task_exec_ctx().schema_service_ = gctx.schema_service_;
  result_set.get_exec_context().set_sql_ctx(&res.sql_ctx());
  res.sql_ctx().retry_times_ = retry_cnt;
  res.sql_ctx().session_info_ = &get_session();
  res.sql_ctx().disable_privilege_check_ = is_check_priv()
                                            ? PRIV_CHECK_FLAG_NORMAL : PRIV_CHECK_FLAG_DISABLE;
  res.sql_ctx().secondary_namespace_ = secondary_namespace;
  res.sql_ctx().is_prepare_protocol_ = is_prepare_protocol;
  res.sql_ctx().is_prepare_stage_ = is_prepare_stage;
  res.sql_ctx().is_dynamic_sql_ = is_dynamic_sql;
  res.sql_ctx().is_dbms_sql_ = is_dbms_sql;
  res.sql_ctx().is_cursor_ = is_cursor;
  res.sql_ctx().schema_guard_ = &schema_guard;
  if (OB_FAIL(res.result_set().init())) {
    LOG_WARN("result set init failed", K(ret));
  } else if (is_prepare_protocol
             && NULL == secondary_namespace
             && !is_dynamic_sql) {
    result_set.set_simple_ps_protocol();
  } else { /*do nothing*/ }
  return ret;
}

int ObInnerSQLConnection::process_retry(ObInnerSQLResult &res,
                                        int last_ret,
                                        int64_t abs_timeout_us,
                                        bool &need_retry,
                                        int64_t retry_cnt)
{
  UNUSED(abs_timeout_us);
  UNUSED(retry_cnt);
  int client_ret = OB_SUCCESS;
  bool force_local_retry = true;
  bool is_inner_sql = true;
  retry_ctrl_.test_and_save_retry_state(GCTX, res.sql_ctx(), res.result_set(),
                                        last_ret, client_ret,
                                        force_local_retry, is_inner_sql);
  need_retry = (ObQueryRetryType::RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());
  return client_ret;
}

class ObInnerSQLTimeRecord : public ObITimeRecord
{
public:
  ObInnerSQLTimeRecord(sql::ObSQLSessionInfo &session)
    : session_(session),
      execute_start_timestamp_(0),
      execute_end_timestamp_(0) {}

  int64_t get_send_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_receive_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_enqueue_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_run_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_process_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_single_process_timestamp() const { return session_.get_query_start_time(); }
  int64_t get_exec_start_timestamp() const { return execute_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return execute_end_timestamp_; }

  void set_execute_start_timestamp(int64_t v) { execute_start_timestamp_ = v; }
  void set_execute_end_timestamp(int64_t v) { execute_end_timestamp_ = v; }

private:
  sql::ObSQLSessionInfo &session_;
  int64_t execute_start_timestamp_;
  int64_t execute_end_timestamp_;
};

int ObInnerSQLConnection::process_record(sql::ObResultSet &result_set,
                                         sql::ObSqlCtx &sql_ctx,
                                         sql::ObSQLSessionInfo &session,
                                         ObITimeRecord &time_record,
                                         int last_ret,
                                         int64_t execution_id,
                                         int64_t ps_stmt_id,
                                         ObWaitEventDesc &max_wait_desc,
                                         ObWaitEventStat &total_wait_desc,
                                         ObExecRecord &exec_record,
                                         ObExecTimestamp &exec_timestamp,
                                         bool has_tenant_resource,
                                         const ObString &ps_sql,
                                         bool is_from_pl,
                                         ObString *pl_exec_params)
{
  int ret = OB_SUCCESS;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  ObArenaAllocator alloc;

  // some statistics must be recorded for plan stat, even though sql audit disabled
  bool first_record = (1 == audit_record.try_cnt_);
  ObExecStatUtils::record_exec_timestamp(time_record, first_record, exec_timestamp);
  audit_record.exec_timestamp_ = exec_timestamp;
  audit_record.exec_timestamp_.update_stage_time();
  audit_record.plsql_exec_time_ = session.get_plsql_exec_time();
  if (audit_record.pl_trace_id_.is_invalid() &&
        result_set.is_pl_stmt(result_set.get_stmt_type()) &&
        OB_NOT_NULL(ObCurTraceId::get_trace_id())) {
    audit_record.pl_trace_id_ = *ObCurTraceId::get_trace_id();
  }
  session.update_pure_sql_exec_time(audit_record.exec_timestamp_.elapsed_t_);

  if (enable_perf_event) {
    record_stat(session, result_set.get_stmt_type(), is_from_pl);
    exec_record.max_wait_event_ = max_wait_desc;
    exec_record.wait_time_end_ = total_wait_desc.time_waited_;
    exec_record.wait_count_end_ = total_wait_desc.total_waits_;
    audit_record.exec_record_ = exec_record;
    audit_record.update_event_stage_state();
    if (OB_NOT_NULL(result_set.get_physical_plan())) {
      const int64_t time_cost = ObTimeUtility::current_time() - session.get_query_start_time();
      ObSQLUtils::record_execute_time(result_set.get_physical_plan()->get_plan_type(), time_cost);
    }
  }
  if (enable_sql_audit) {
    ret = process_audit_record(result_set, sql_ctx, session, last_ret, execution_id,
              ps_stmt_id, has_tenant_resource, ps_sql, is_from_pl);
    if (NULL != pl_exec_params) {
      audit_record.params_value_ = pl_exec_params->ptr();
      audit_record.params_value_len_ = pl_exec_params->length();
    }
  }
  ObSQLUtils::handle_audit_record(false, sql::PSCursor == audit_record.exec_timestamp_.exec_type_
                                         ? EXECUTE_PS_EXECUTE :
                                           (is_from_pl ? EXECUTE_PL_EXECUTE : EXECUTE_INNER),
                                  session, sql_ctx.is_sensitive_);

  // 临时allocator 申请的内存，需要在这里 置 NULL
  {
    audit_record.params_value_ = NULL;
    audit_record.params_value_len_ = 0;
  }
  return ret;
}

int ObInnerSQLConnection::process_audit_record(sql::ObResultSet &result_set,
                                               sql::ObSqlCtx &sql_ctx,
                                               sql::ObSQLSessionInfo &session,
                                               int last_ret,
                                               int64_t execution_id,
                                               int64_t ps_stmt_id,
                                               bool has_tenant_resource,
                                               const ObString &ps_sql,
                                               bool is_from_pl)
{
  int ret = OB_SUCCESS;

  if (has_tenant_resource) {
    ObAuditRecordData &audit_record = session.get_raw_audit_record();
    audit_record.try_cnt_++;
    ObPhysicalPlan *plan = result_set.get_physical_plan();
    audit_record.seq_ = 0;  //don't use now
    audit_record.status_ = (0 == last_ret || OB_ITER_END == last_ret)
        ? obmysql::REQUEST_SUCC : last_ret;

    audit_record.client_addr_ = session.get_peer_addr();
    audit_record.user_client_addr_ = session.get_user_client_addr();
    audit_record.user_group_ = THIS_WORKER.get_group_id();
    audit_record.execution_id_ = execution_id;
    audit_record.ps_stmt_id_ = ps_stmt_id;
    audit_record.ps_inner_stmt_id_ = ps_stmt_id;
    if (ps_sql.length() != 0) {
      audit_record.sql_ = const_cast<char *>(ps_sql.ptr());
      audit_record.sql_len_ = min(ps_sql.length(), OB_MAX_SQL_LENGTH);
    }
    MEMCPY(audit_record.sql_id_, sql_ctx.sql_id_, (int32_t)sizeof(audit_record.sql_id_));
    audit_record.affected_rows_ = result_set.get_affected_rows();
    audit_record.return_rows_ = result_set.get_return_rows();
    if (NULL != result_set.get_exec_context().get_task_executor_ctx()) {
      audit_record.partition_cnt_ = result_set.get_exec_context()
                                                    .get_das_ctx()
                                                    .get_related_tablet_cnt();
    }

    if (NULL != result_set.get_physical_plan()) {
      audit_record.plan_type_ = result_set.get_physical_plan()->get_plan_type();
      audit_record.table_scan_ = result_set.get_physical_plan()->contain_table_scan();
      audit_record.plan_id_ = result_set.get_physical_plan()->get_plan_id();
      audit_record.plan_hash_ = result_set.get_physical_plan()->get_plan_hash_value();
      audit_record.partition_hit_ = session.partition_hit().get_bool();
    }

    audit_record.is_executor_rpc_ = false;
    audit_record.is_inner_sql_ = !is_from_pl;
    audit_record.is_hit_plan_cache_ = result_set.get_is_from_plan_cache();
    audit_record.is_multi_stmt_ = false; //是否是multi sql
    audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();

    ObIArray<ObTableRowCount> *table_row_count_list = NULL;
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(result_set.get_exec_context());
    if (NULL != plan_ctx) {
      audit_record.consistency_level_ = plan_ctx->get_consistency_level();
      audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
      table_row_count_list = &(plan_ctx->get_table_row_count_list());
    }

    //update v$sql statistics
    if (OB_SUCC(last_ret) && session.get_local_ob_enable_plan_cache()) {
      if (NULL != plan) {
        if (!(sql_ctx.self_add_plan_) && sql_ctx.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
                                false, // false mean not first update plan stat
                                table_row_count_list);
        } else if (sql_ctx.self_add_plan_ && !sql_ctx.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
                                true,
                                table_row_count_list);
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObInnerSQLConnection::process_final(const T &sql,
                                        ObInnerSQLResult &res,
                                        int last_ret)
{
  int ret = OB_SUCCESS;
  UNUSED(res);
  if (lib::is_diagnose_info_enabled()) {
    int64_t process_time = ObTimeUtility::current_time() - get_session().get_query_start_time();
    if (OB_SUCC(last_ret)) {
      const int64_t now = ObTimeUtility::current_time();
      EVENT_INC(INNER_SQL_CONNECTION_EXECUTE_COUNT);
      EVENT_ADD(INNER_SQL_CONNECTION_EXECUTE_TIME, now - get_session().get_query_start_time());
    }

    if (process_time > 1L * 1000 * 1000) {
      LOG_INFO("slow inner sql", K(last_ret), K(sql), K(process_time));
    }
  }
  return ret;
}

int ObInnerSQLConnection::do_query(sqlclient::ObIExecutor &executor, ObInnerSQLResult &res)
{
  int ret = OB_SUCCESS;
  WITH_CONTEXT(res.mem_context_) {
    // restore有自己的inner_sql_connection，sql_modifier不为null
    bool is_restore = NULL != sql_modifier_;
    res.sql_ctx().is_restore_ = is_restore;
    get_session().set_process_query_time(ObTimeUtility::current_time());
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(ob_sql_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob_sql_ is NULL", K(ret));
    } else if (OB_FAIL(executor.execute(*ob_sql_, res.sql_ctx(), res.result_set()))) {
      LOG_WARN("executor execute failed", K(ret));
    } else {
      ObSQLSessionInfo &session = res.result_set().get_session();
      session.set_expect_group_id(group_id_);
      if (OB_ISNULL(res.sql_ctx().schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null");
      } else if (OB_FAIL(session.update_query_sensitive_system_variable(*(res.sql_ctx().schema_guard_)))) {
        LOG_WARN("update query affacted system variable failed", K(ret));
      } else if (OB_UNLIKELY(is_restore)
                 && OB_FAIL(sql_modifier_->modify(res.result_set()))) {
        LOG_WARN("fail modify sql", K(res.result_set().get_statement_name()), K(ret));
      } else if (OB_FAIL(res.open())) {
        LOG_WARN("result set open failed", K(ret), K(executor));
      }
    }
  }

  return ret;
}

int ObInnerSQLConnection::query(sqlclient::ObIExecutor &executor,
                                ObInnerSQLResult &res,
                                ObVirtualTableIteratorFactory *vt_iter_factory)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard g(get_compat_mode());
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;

  exec_timestamp.exec_type_ = sql::InnerSql;
  const ObGlobalContext &gctx = ObServer::get_instance().get_gctx();
  int64_t start_time = ObTimeUtility::current_time();
  get_session().set_query_start_time(start_time); //FIXME 暂时写成这样
  get_session().set_trans_type(transaction::ObTxClass::SYS);
  int64_t abs_timeout_us = 0;
  int64_t execution_id = 0;
  const uint64_t* trace_id_val = ObCurTraceId::get();
  bool is_trace_id_init = true;
  ObQueryRetryInfo &retry_info = get_session().get_retry_info_for_update();
  if (0 == trace_id_val[0]) {
    is_trace_id_init = false;
    common::ObCurTraceId::init(observer::ObServer::get_instance().get_self());
  }

  // backup && restore worker/session timeout.
  TimeoutGuard timeout_guard(*this);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL != ref_ctx_) {
    ret = OB_REF_NUM_NOT_ZERO;
    LOG_ERROR("connection still be referred by previous sql result, can not execute sql now",
              K(ret), K(executor));
  } else if (OB_FAIL(set_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret));
  } else if (OB_ISNULL(ob_sql_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql engine", K(ret), K(ob_sql_));
  } else if (OB_UNLIKELY(retry_info.is_inited())) {
    if (is_inner_session()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("retry info is inited", K(ret), K(retry_info), K(executor));
    }
  } else if (OB_FAIL(retry_info.init())) {
    LOG_WARN("fail to init retry info", K(ret), K(retry_info), K(executor));
  }

  // switch tenant for MTL tenant ctx
  uint64_t tenant_id = get_session().get_effective_tenant_id();

  if (OB_SUCC(ret)) {
    MTL_SWITCH(tenant_id) {
      execution_id = ob_sql_->get_execution_id();
      bool need_retry = true;
      retry_ctrl_.clear_state_before_each_retry(get_session().get_retry_info_for_update());
      retry_ctrl_.reset_retry_times();
      for (int64_t retry_cnt = 0; need_retry; ++retry_cnt) {
        need_retry = false;
        retry_info.clear_state_before_each_retry();
        res.set_is_read(true);
        if (retry_cnt > 0) { // reset result set
          bool is_user_sql = res.result_set().is_user_sql();
          res.~ObInnerSQLResult();
          new (&res) ObInnerSQLResult(get_session());
          if (OB_FAIL(res.init())) {
            LOG_WARN("fail to init result set", K(ret));
          } else {
            res.result_set().set_user_sql(is_user_sql);
            res.set_is_read(true);
          }
        }
        get_session().get_raw_audit_record().request_memory_used_ = 0;
        observer::ObProcessMallocCallback pmcb(0,
              get_session().get_raw_audit_record().request_memory_used_);
        lib::ObMallocCallbackGuard guard(pmcb);
        int64_t local_tenant_schema_version = -1;
        int64_t local_sys_schema_version = -1;
        ObWaitEventDesc max_wait_desc;
        ObWaitEventStat total_wait_desc;
        ObInnerSQLTimeRecord time_record(get_session());
        const bool enable_perf_event = lib::is_diagnose_info_enabled();
        const bool enable_sql_audit =
          GCONF.enable_sql_audit && get_session().get_local_ob_enable_sql_audit();
        {
          ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
          ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);

          if (enable_perf_event) {
            exec_record.record_start();
          }

          const uint64_t tenant_id = get_session().get_effective_tenant_id();

          if (OB_FAIL(ret)){
            // do nothing
          } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, res.schema_guard_))) {
            LOG_WARN("get schema guard failed", K(ret));
          } else if (OB_FAIL(init_result(res, vt_iter_factory, retry_cnt,
                                         res.schema_guard_, NULL, false, false))) {
            LOG_WARN("failed to init result", K(ret));
          } else if (OB_FAIL(res.schema_guard_.get_schema_version(tenant_id, local_tenant_schema_version))) {
            LOG_WARN("get tenant schema version failed", K(ret), K(ob_sql_));
          } else if (OB_FAIL(res.schema_guard_.get_schema_version(OB_SYS_TENANT_ID, local_sys_schema_version))) {
            LOG_WARN("get sys tenant schema version failed", K(ret), K(ob_sql_));
          } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_1_0_0) {
            if (ObSchemaService::is_formal_version(local_tenant_schema_version)) {
              res.result_set().get_exec_context().get_task_exec_ctx().set_query_tenant_begin_schema_version(local_tenant_schema_version);
            } else {
              LOG_WARN("is not a formal_schema_version", K(local_tenant_schema_version));
            }
            if (ObSchemaService::is_formal_version(local_sys_schema_version)) {
              res.result_set().get_exec_context().get_task_exec_ctx().set_query_sys_begin_schema_version(local_sys_schema_version);
            } else {
              LOG_WARN("is not a formal_schema_version", K(local_sys_schema_version));
            }
          } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0) {
            res.result_set().get_exec_context().get_task_exec_ctx().set_query_tenant_begin_schema_version(local_tenant_schema_version);
            res.result_set().get_exec_context().get_task_exec_ctx().set_query_sys_begin_schema_version(local_sys_schema_version);
          }

          int ret_code = OB_SUCCESS;
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(SMART_CALL(do_query(executor, res)))) {
            ret_code = ret;
            LOG_WARN("execute failed", K(ret), K(tenant_id), K(executor), K(retry_cnt),
                K(local_sys_schema_version), K(local_tenant_schema_version));
            ret = process_retry(res, ret, abs_timeout_us, need_retry, retry_cnt);
            // moved here from ObInnerSQLConnection::do_query() -> ObInnerSQLResult::open().
            int close_ret = res.force_close();
            if (OB_SUCCESS != close_ret) {
              LOG_WARN("failed to close result", K(close_ret), K(ret));
            }
          } else if (retry_cnt > 0) {
            int64_t total_time_cost_us = (ObTimeUtility::current_time() - start_time);
            LOG_INFO("[OK] inner sql execute success after retry!", K(retry_cnt), K(total_time_cost_us));
          }
          get_session().set_session_in_retry(need_retry, ret_code);
          //监控项统计开始
          execute_start_timestamp_ = (res.get_execute_start_ts() > 0)
                                      ? res.get_execute_start_ts()
                                      : ObTimeUtility::current_time();
          //监控项统计结束
          execute_end_timestamp_ = (res.get_execute_end_ts() > 0)
                                    ? res.get_execute_end_ts()
                                    : ObTimeUtility::current_time();

          time_record.set_execute_start_timestamp(execute_start_timestamp_);
          time_record.set_execute_end_timestamp(execute_end_timestamp_);
          if (enable_perf_event) {
            exec_record.record_end();
          }
        }

        if (res.is_inited()) {
          ObString dummy_ps_sql;
          int record_ret = process_record(res.result_set(), res.sql_ctx(), get_session(),
                                time_record, ret, execution_id, OB_INVALID_ID,
                                max_wait_desc, total_wait_desc, exec_record, exec_timestamp,
                                res.has_tenant_resource(), dummy_ps_sql);
          if (OB_SUCCESS != record_ret) {
            LOG_WARN("failed to process record",  K(executor), K(record_ret), K(ret));
          }
        }

        if (res.is_inited()) {
#ifdef OB_BUILD_AUDIT_SECURITY
          (void)ObSecurityAuditUtils::handle_security_audit(res.result_set(),
                                                            res.sql_ctx().schema_guard_,
                                                            res.sql_ctx().cur_stmt_,
                                                            ObString::make_string("inner sql"),
                                                            ret);

#endif
          if (OB_SUCC(ret) && get_session().get_in_transaction()) {
            if (ObStmt::is_dml_write_stmt(res.result_set().get_stmt_type()) ||
                ObStmt::is_savepoint_stmt(res.result_set().get_stmt_type())) {
              get_session().set_has_exec_inner_dml(true);
            }
          }
        }
      }
    }
  }
  if (res.is_inited()) {
    int aret = process_final(executor, res, ret);
    if (OB_SUCCESS != aret) {
      LOG_WARN("failed to process final",  K(executor), K(aret), K(ret));
    }
  }

  if (false == is_trace_id_init) {
    common::ObCurTraceId::reset();
  }
  if (is_inner_session()) {
    retry_info.reset();
  }
  return ret;
}

common::sqlclient::ObCommonServerConnectionPool *ObInnerSQLConnection::get_common_server_pool()
{
  return NULL;
}

template <typename T>
int ObInnerSQLConnection::retry_while_no_tenant_resource(const int64_t cluster_id, const uint64_t &tenant_id, T function)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
  const int64_t max_retry_us = 128 * 1000;
  int64_t retry_us = 2 * 1000;
  bool need_retry = is_in_trans() ? false : true;
  if (get_session().get_ddl_info().is_ddl()) {  // ddl retry in ddl scheduler layer
    need_retry = false;
  }
  // timeout related
  int64_t abs_timeout_us = 0;
  int64_t start_time = ObTimeUtility::current_time();
  get_session().set_query_start_time(start_time);
  TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout.

  if (OB_FAIL(set_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret));
  } else {
    do {
      int64_t now = ObTimeUtility::current_time();
      if (now >= abs_timeout_us) {
        need_retry = false;
        ret = OB_TIMEOUT;
        LOG_WARN("timeout, do not need retry", K(ret), K(abs_timeout_us), K(now));
      } else if (OB_FAIL(function())) {
        if (is_unit_migrate(ret)) {
          LOG_INFO("failed to get newest location and will force renew", K(ret), K(tenant_id), K(ls_id));
          int tmp_ret = GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id);
          if (OB_SUCCESS != tmp_ret) {
            need_retry = false; // nonblock_renew failed
            LOG_WARN("nonblock renew from location cache failed", K(tmp_ret), K(cluster_id), K(tenant_id), K(ls_id));
          } else {
            ob_usleep(retry_us);
            if (retry_us < max_retry_us) {
              retry_us = retry_us * 2;
            }
          }
        } else {
          need_retry = false; // errno is not related to OB_TENANT_NOT_IN_SERVER
          LOG_WARN("retry_while_no_tenant_resource failed", K(ret), K(tenant_id));
        }
      } else {
        need_retry = false; // function is successful
      }
    } while (need_retry);
  }
  return ret;
}

int ObInnerSQLConnection::start_transaction(
    const uint64_t &tenant_id,
    bool with_snap_shot /* = false */)
{
  int ret = OB_SUCCESS;
  auto function = [&]() { return start_transaction_inner(tenant_id, with_snap_shot); };
  if (OB_FAIL(retry_while_no_tenant_resource(GCONF.cluster_id, tenant_id, function))) {
    LOG_WARN("start_transaction failed", K(ret), K(tenant_id), K(with_snap_shot));
  }
  return ret;
}


int ObInnerSQLConnection::start_transaction_inner(
    const uint64_t &tenant_id,
    bool with_snap_shot /* = false */)
{
  int ret = OB_SUCCESS;
  ObString sql;
  bool has_tenant_resource = false;
  if (with_snap_shot) {
    sql = ObString::make_string("START TRANSACTION WITH CONSISTENT SNAPSHOT");
  } else {
    sql = ObString::make_string("START TRANSACTION");
  }
  ObSqlQueryExecutor executor(sql);
  SMART_VAR(ObInnerSQLResult, res, get_session()) {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (GCTX.omt_->is_available_tenant(tenant_id)) {
      has_tenant_resource = true;
      if (OB_FAIL(switch_tenant(tenant_id))) {
        LOG_WARN("set system tenant id failed", K(ret), K(tenant_id));
      }
    } else {
      has_tenant_resource = false;
      LOG_DEBUG("tenant not in server", K(ret), K(tenant_id), K(MYADDR));
    }
    if (OB_SUCC(ret)) {
      if (is_in_trans()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner conn is already in trans", K(ret));
      } else if (OB_FAIL(res.init(has_tenant_resource))) {
        LOG_WARN("init result set", K(ret), K(has_tenant_resource));
      } else if (has_tenant_resource) {
        // local
        if (OB_FAIL(query(executor, res))) {
          LOG_WARN("start transaction failed", K(ret), K(tenant_id), K(with_snap_shot));
        } else if (OB_FAIL(res.close())) {
          LOG_WARN("close result set failed", K(ret), K(tenant_id), K(sql));
        }
        // remote
      } else {
        TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
        common::ObAddr resource_server_addr; // MYADDR
        share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
        int64_t query_timeout = OB_INVALID_TIMESTAMP;
        int64_t trx_timeout = OB_INVALID_TIMESTAMP;
        ObSQLMode sql_mode = 0;
        const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
        bool is_load_data_exec = get_session().is_load_data_exec_session();
        if (is_resource_conn()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn of resource_svr still doesn't has the tenant resource",
                   K(ret), K(MYADDR), K(tenant_id), K(get_resource_conn_id()));
        } else if (OB_INVALID_ID != get_resource_conn_id() || get_resource_svr().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn_id is not invalid or resource_svr is valid in start_transaction",
              K(ret), K(tenant_id), K(get_resource_conn_id()), K(get_resource_svr()));
        } else if (OB_FAIL(nonblock_get_leader(GCONF.cluster_id, tenant_id, ls_id,
                                               resource_server_addr))) {
          LOG_WARN("nonblock get leader failed", KR(ret), K(tenant_id), K(ls_id), K(resource_server_addr));
        } else if (FALSE_IT(set_resource_svr(resource_server_addr))) {
        } else if (OB_FAIL(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
          LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
        } else {
          ObInnerSQLTransmitArg arg (MYADDR, get_resource_svr(), tenant_id, get_resource_conn_id(),
              sql, ObInnerSQLTransmitArg::OPERATION_TYPE_START_TRANSACTION,
              lib::Worker::CompatMode::ORACLE == get_compat_mode(), GCONF.cluster_id,
              THIS_WORKER.get_timeout_ts(), query_timeout, trx_timeout,
              sql_mode, ddl_info, is_load_data_exec, use_external_session_);
          arg.set_nls_formats(get_session().get_local_nls_date_format(),
                              get_session().get_local_nls_timestamp_format(),
                              get_session().get_local_nls_timestamp_tz_format());
          ObInnerSqlRpcStreamHandle *handler = res.remote_result_set().get_stream_handler();
          if (OB_ISNULL(handler)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("handler is null ptr", K(ret));
          } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
            LOG_WARN("fail to set tz info wrap", K(ret));
          } else if (FALSE_IT(handler->get_result()->set_conn_id(OB_INVALID_ID))) {
          } else if (FALSE_IT(handler->get_result()->set_tenant_id(get_session().get_effective_tenant_id()))) {
          } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(resource_server_addr).by(tenant_id).
                             timeout(query_timeout).group_id(group_id_).
                             inner_sql_sync_transmit(
                                 arg, *(handler->get_result()), handler->get_handle()))) {
            LOG_WARN("inner_sql_sync_transmit process failed", K(ret), K(tenant_id));
          } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
            ret = handler->get_result()->get_err_code();
            LOG_WARN("failed to execute inner sql", K(ret));
          } else if (FALSE_IT(set_resource_conn_id(handler->get_result()->get_conn_id()))) {
          } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
          }
        }
        if (OB_FAIL(ret)) {
          reset_resource_conn_info();
        }
      }
      if (OB_SUCC(ret)) {
        set_is_in_trans(true);
      }
    }
  }

  return ret;
}

int ObInnerSQLConnection::register_multi_data_source(const uint64_t &tenant_id,
                                                     const share::ObLSID ls_id,
                                                     const transaction::ObTxDataSourceType type,
                                                     const char *buf,
                                                     const int64_t buf_len,
                                                     const transaction::ObRegisterMdsFlag & register_flag)
{
  int ret = OB_SUCCESS;
  const bool local_execute = is_local_execute(GCONF.cluster_id, tenant_id);
  transaction::ObTxDesc *tx_desc = nullptr;

  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id));
    } else if (local_execute) {
      if (OB_FAIL(switch_tenant(tenant_id))) {
        LOG_WARN("set system tenant id failed", K(ret), K(tenant_id));
      }
    } else {
      LOG_DEBUG("tenant may be not in server", K(ret), K(local_execute), K(tenant_id), K(MYADDR));
    }

    if (OB_SUCC(ret)) {
      if (!is_in_trans()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner conn must be already in trans when register multi source data", K(ret));
      } else if (OB_FAIL(res.init(local_execute))) {
        LOG_WARN("init result set", K(ret), K(local_execute));
      } else if (local_execute) {
        if (OB_ISNULL(tx_desc = get_session().get_tx_desc())) {
          // TODO ADD LOG and check get_session
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid tx_desc", K(ret), K(ls_id), K(type));
        } else {
          MTL_SWITCH(tenant_id)
          {
            if (OB_FAIL(MTL(transaction::ObTransService *)->register_mds_into_tx(*tx_desc,
                                                                                 ls_id,
                                                                                 type,
                                                                                 buf,
                                                                                 buf_len,
                                                                                 0,
                                                                                 register_flag))) {
              LOG_WARN("regiser multi data source failed", K(ret), K(tenant_id), K(type));
            } else if (OB_FAIL(res.close())) {
              LOG_WARN("close result set failed", K(ret), K(tenant_id));
            }
          }
        }
      } else {
        common::ObAddr resource_server_addr; // MYADDR
        int64_t query_timeout = OB_INVALID_TIMESTAMP;
        int64_t trx_timeout = OB_INVALID_TIMESTAMP;
        ObSQLMode sql_mode = 0;
        const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
        bool is_load_data_exec = get_session().is_load_data_exec_session();
        if (is_resource_conn()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn of resource_svr still doesn't has the tenant resource", K(ret),
                   K(MYADDR), K(tenant_id), K(get_resource_conn_id()));
        } else if (OB_INVALID_ID == get_resource_conn_id() || !get_resource_svr().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn_id or resource_svr is invalid", K(ret), K(tenant_id),
                   K(get_resource_conn_id()), K(get_resource_svr()));
        } else if (OB_FAIL(get_session().get_query_timeout(query_timeout))
                   || OB_FAIL(get_session().get_tx_timeout(trx_timeout))) {
          LOG_WARN("get conn timeout failed", KR(ret), K(get_session()));
        } else {
          transaction::ObMDSInnerSQLStr mds_str;
          char *tmp_str = nullptr;
          int64_t pos = 0;
          ObString sql;
          if (OB_FAIL(mds_str.set(buf, buf_len, type, ls_id, register_flag))) {
            LOG_WARN("set multi source data in msd_str error", K(ret), K(type), K(ls_id));
          } else if (OB_ISNULL(tmp_str = static_cast<char *>(
                                   ob_malloc(mds_str.get_serialize_size(), "MulTxDataStr")))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory for sql_str failed", K(ret), K(mds_str.get_serialize_size()));
          } else if (OB_FAIL(mds_str.serialize(tmp_str, mds_str.get_serialize_size(), pos))) {
            LOG_WARN("serialize mds_str failed", K(ret), K(mds_str), K(mds_str.get_serialize_size()));
          } else {
            sql.assign_ptr(tmp_str, mds_str.get_serialize_size());
            ret = forward_request_(tenant_id, ObInnerSQLTransmitArg::OPERATION_TYPE_REGISTER_MDS, sql, res);
          }

          if (OB_NOT_NULL(tmp_str)) {
            ob_free(tmp_str);
          }
        }
      }
    }
  }


  LOG_INFO("register mds in inner_sql_connection",
           KR(ret),
           KP(this),
           K(local_execute),
           K(get_resource_conn_id()),
           K(get_session().get_sessid()),
           KPC(get_session().get_tx_desc()));
  return ret;
}

int ObInnerSQLConnection::forward_request(const uint64_t tenant_id,
                                          const int64_t op_type,
                                          const ObString &sql,
                                          ObInnerSQLResult &res,
                                          const int32_t group_id)
{
  return forward_request_(tenant_id, op_type, sql, res, group_id);
}

int ObInnerSQLConnection::forward_request_(const uint64_t tenant_id,
                                           const int64_t op_type,
                                           const ObString &sql,
                                           ObInnerSQLResult &res,
                                           const int32_t group_id)
{
  int ret = OB_SUCCESS;

  TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
  common::ObAddr resource_server_addr; // MYADDR
  share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
  int64_t query_timeout = OB_INVALID_TIMESTAMP;
  int64_t trx_timeout = OB_INVALID_TIMESTAMP;
  ObSQLMode sql_mode = 0;
  const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
  bool is_load_data_exec = get_session().is_load_data_exec_session();
  int32_t real_group_id = group_id_;
  if (0 != group_id) {
    real_group_id = group_id;
  }
  if (is_resource_conn()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resource_conn of resource_svr still doesn't has the tenant resource", K(ret),
             K(MYADDR), K(tenant_id), K(get_resource_conn_id()));
  } else if (OB_INVALID_ID == get_resource_conn_id() || !get_resource_svr().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resource_conn_id or resource_svr is invalid", K(ret), K(tenant_id),
             K(get_resource_conn_id()), K(get_resource_svr()));
  } else if (OB_FAIL(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
    LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
  } else {
    ObInnerSQLTransmitArg arg(MYADDR, get_resource_svr(), tenant_id, get_resource_conn_id(),
                              sql, (ObInnerSQLTransmitArg::InnerSQLOperationType)op_type,
                              lib::Worker::CompatMode::ORACLE == get_compat_mode(),
                              GCONF.cluster_id, THIS_WORKER.get_timeout_ts(), query_timeout,
                              trx_timeout, sql_mode, ddl_info, is_load_data_exec, use_external_session_);
    arg.set_nls_formats(get_session().get_local_nls_date_format(),
                        get_session().get_local_nls_timestamp_format(),
                        get_session().get_local_nls_timestamp_tz_format());
    ObInnerSqlRpcStreamHandle *handler = res.remote_result_set().get_stream_handler();
    if (OB_ISNULL(handler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("handler is null ptr", K(ret));
    } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
      LOG_WARN("fail to set tz info wrap", K(ret));
      // } else if (FALSE_IT(handler->get_result()->set_conn_id(OB_INVALID_ID))) {
    } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(get_resource_svr())
                       .by(tenant_id)
                       .timeout(query_timeout)
                       .group_id(real_group_id)
                       .inner_sql_sync_transmit(arg,
                                                *(handler->get_result()),
                                                handler->get_handle()))) {
      LOG_WARN("inner_sql_sync_transmit process failed", K(ret), K(tenant_id));
      // } else if (FALSE_IT(set_resource_conn_id(handler->get_result()->get_conn_id()))) {
    } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
      ret = handler->get_result()->get_err_code();
      LOG_WARN("failed to execute inner sql", K(ret));
    } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
    } else if (OB_FAIL(res.close())) {
      LOG_WARN("close result set failed", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObInnerSQLConnection::rollback()
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(inner_rollback);
  ObSqlQueryExecutor executor("ROLLBACK");
  bool has_tenant_resource = is_resource_conn() || OB_INVALID_ID == get_resource_conn_id();
  if (!is_in_trans()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner conn is not in trans", K(ret));
  } else {
    SMART_VAR(ObInnerSQLResult, res, get_session()) {
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("connection not inited", K(ret));
      } else if (OB_FAIL(res.init(has_tenant_resource))) {
        LOG_WARN("init result set", K(ret));
      } else if (has_tenant_resource) {
        if (OB_FAIL(query(executor, res))) {
          LOG_WARN("rollback failed", K(ret));
        } else if (OB_FAIL(res.close())) {
          LOG_WARN("close result set failed", K(ret));
        }
      } else {
        TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
        int64_t query_timeout = OB_INVALID_TIMESTAMP;
        int64_t trx_timeout = OB_INVALID_TIMESTAMP;
        ObSQLMode sql_mode = 0;
        const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
        bool is_load_data_exec = get_session().is_load_data_exec_session();
        if (OB_INVALID_ID == get_resource_conn_id() || !get_resource_svr().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn_id or resource_svr is invalid in rollback",
                    K(ret), K(get_resource_conn_id()), K(get_resource_svr()));
        } else if (OB_FAIL(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
          LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
        } else {
          ObInnerSQLTransmitArg arg (MYADDR, get_resource_svr(),
              get_session().get_effective_tenant_id(), get_resource_conn_id(),
              ObString::make_string("ROLLBACK"), ObInnerSQLTransmitArg::OPERATION_TYPE_ROLLBACK,
              lib::Worker::CompatMode::ORACLE == get_compat_mode(), GCONF.cluster_id,
              THIS_WORKER.get_timeout_ts(), query_timeout, trx_timeout, sql_mode,
              ddl_info, is_load_data_exec, use_external_session_);
          arg.set_nls_formats(get_session().get_local_nls_date_format(),
                              get_session().get_local_nls_timestamp_format(),
                              get_session().get_local_nls_timestamp_tz_format());
          ObInnerSqlRpcStreamHandle *handler = res.remote_result_set().get_stream_handler();
          if (OB_ISNULL(handler)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("handler is null ptr", K(ret));
          } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
            LOG_WARN("fail to set tz info wrap", K(ret));
          } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(get_resource_svr()).by(OB_SYS_TENANT_ID).
                             timeout(query_timeout).
                             group_id(group_id_).
                             inner_sql_sync_transmit(
                                 arg, *(handler->get_result()), handler->get_handle()))) {
            LOG_WARN("inner_sql_sync_transmit process failed",
                K(ret), K(handler->get_result()->get_err_code()));
          } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
            ret = handler->get_result()->get_err_code();
            LOG_WARN("failed to execute inner sql", K(ret));
          } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
          } else if (FALSE_IT(reset_resource_conn_info())) {
          }
        }
      }
    }
  }
  set_is_in_trans(false);
  return ret;
}

int ObInnerSQLConnection::commit()
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(inner_commit);
  DEBUG_SYNC(BEFORE_INNER_SQL_COMMIT);
  ObSqlQueryExecutor executor("COMMIT");
  bool has_tenant_resource = is_resource_conn() || OB_INVALID_ID == get_resource_conn_id();
  if (!is_in_trans()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner conn is not in trans", K(ret));
  } else {
    SMART_VAR(ObInnerSQLResult, res, get_session()) {
      if (!inited_) {
        ret = OB_NOT_INIT;
        LOG_WARN("connection not inited", K(ret));
      } else if (OB_FAIL(res.init(has_tenant_resource))) {
        LOG_WARN("init result set", K(ret));
      } else if (has_tenant_resource) {
        if (OB_FAIL(query(executor, res))) {
          LOG_WARN("commit failed", K(ret));
        } else if (OB_FAIL(res.close())) {
          LOG_WARN("close result set failed", K(ret));
        }
      } else {
        TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
        int64_t query_timeout = OB_INVALID_TIMESTAMP;
        int64_t trx_timeout = OB_INVALID_TIMESTAMP;
        ObSQLMode sql_mode = 0;
        const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
        bool is_load_data_exec = get_session().is_load_data_exec_session();
        if (!get_resource_svr().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_svr is invalid in commit", K(ret), K(get_resource_svr()));
        } else if (OB_FAIL(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
          LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
        } else {
          ObInnerSQLTransmitArg arg (MYADDR, get_resource_svr(),
              get_session().get_effective_tenant_id(), get_resource_conn_id(),
              ObString::make_string("COMMIT"), ObInnerSQLTransmitArg::OPERATION_TYPE_COMMIT,
              lib::Worker::CompatMode::ORACLE == get_compat_mode(), GCONF.cluster_id,
              THIS_WORKER.get_timeout_ts(), query_timeout, trx_timeout, sql_mode,
              ddl_info, is_load_data_exec, use_external_session_);
          arg.set_nls_formats(get_session().get_local_nls_date_format(),
                              get_session().get_local_nls_timestamp_format(),
                              get_session().get_local_nls_timestamp_tz_format());
          ObInnerSqlRpcStreamHandle *handler = res.remote_result_set().get_stream_handler();
          if (OB_ISNULL(handler)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("handler is null ptr", K(ret));
          } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
            LOG_WARN("fail to set tz info wrap", K(ret));
          } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(get_resource_svr()).by(OB_SYS_TENANT_ID).
                     timeout(query_timeout).group_id(group_id_).
                     inner_sql_sync_transmit(
                         arg, *(handler->get_result()), handler->get_handle()))) {
            LOG_WARN("inner_sql_sync_transmit process failed",
                     K(ret), K(handler->get_result()->get_err_code()));
          } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
            ret = handler->get_result()->get_err_code();
            LOG_WARN("failed to execute inner sql", K(ret));
          } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
          } else if (FALSE_IT(reset_resource_conn_info())) {
          }
        }
      }
    }
  }
  set_is_in_trans(false);
  return ret;
}

int ObInnerSQLConnection::execute_write(const uint64_t tenant_id, const char *sql,
  int64_t &affected_rows, bool is_user_sql, const common::ObAddr *sql_exec_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute_write(tenant_id, ObString::make_string(sql), affected_rows, is_user_sql, sql_exec_addr))) {
    LOG_WARN("execute_write failed", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObInnerSQLConnection::execute_write(const uint64_t tenant_id, const ObString &sql,
    int64_t &affected_rows, bool is_user_sql, const common::ObAddr *sql_exec_addr)
{
  int ret = OB_SUCCESS;
  auto function = [&]() { return execute_write_inner(tenant_id, sql, affected_rows, is_user_sql, sql_exec_addr); };
  if (OB_FAIL(retry_while_no_tenant_resource(GCONF.cluster_id, tenant_id, function))) {
    LOG_WARN("execute_write failed", K(ret), K(tenant_id), K(sql), K(is_user_sql));
  }
  return ret;
}

int ObInnerSQLConnection::execute_proc(const uint64_t tenant_id,
                                      ObIAllocator &allocator,
                                      ParamStore &params,
                                      ObString &sql,
                                      const share::schema::ObRoutineInfo &routine_info,
                                      const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                      const ObTimeZoneInfo *tz_info,
                                      ObObj *result)
{
  UNUSEDx(tenant_id, allocator, params, sql, routine_info, udts, tz_info, result);
  int ret = OB_SUCCESS;
  return ret;
}

int ObInnerSQLConnection::execute_write_inner(const uint64_t tenant_id, const ObString &sql,
    int64_t &affected_rows, bool is_user_sql, const common::ObAddr *sql_exec_addr)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(inner_execute_write);
  ObSqlQueryExecutor executor(sql);
  const bool local_execute = is_local_execute(GCONF.cluster_id, tenant_id);
  SMART_VAR(ObInnerSQLResult, res, get_session()) {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (0 == sql.length() || NULL == sql.ptr()  || '\0' == *(sql.ptr())
               || OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(sql), K(tenant_id));
    }
    if (OB_SUCC(ret)) {
      if (local_execute && OB_FAIL(switch_tenant(tenant_id))) {
        LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(res.init(local_execute))) {
        LOG_WARN("init result set", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (local_execute) {
      res.result_set().set_user_sql(is_user_sql);
      if (OB_FAIL(query(executor, res))) {
        LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
      } else if (FALSE_IT(affected_rows = res.result_set().get_affected_rows())) {
      } else if (OB_FAIL(res.close())) {
        LOG_WARN("close result set failed", K(ret), K(tenant_id), K(sql));
      }
      if (get_session().get_ddl_info().is_ddl()) {
        SERVER_EVENT_ADD(
          "ddl", "local execute ddl inner sql",
          "tenant_id", tenant_id,
          "trace_id", *ObCurTraceId::get_trace_id(),
          "ret", ret,
          "affected_rows", affected_rows,
          "start_ts", res.execute_start_ts_,
          "end_ts", res.execute_end_ts_);
      }
    } else if (is_resource_conn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resource_conn of resource_svr still doesn't has the tenant resource",
               K(ret), K(MYADDR), K(tenant_id), K(get_resource_conn_id()));
    } else { // !has_tenant_resource
      TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
      int64_t query_timeout = OB_DEFAULT_SESSION_TIMEOUT;
      int64_t trx_timeout = OB_DEFAULT_SESSION_TIMEOUT;
      int64_t consumer_group_id = get_group_id();
      ObSQLMode sql_mode = 0;
      const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
      bool is_load_data_exec = get_session().is_load_data_exec_session();
      if (is_in_trans()) {
        if (!get_resource_svr().is_valid() || OB_INVALID_ID == get_resource_conn_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("resource_conn_id or resource_svr is invalid",
                   K(ret), K(get_resource_svr()), K(get_resource_conn_id()));
        }
      } else if (!OB_ISNULL(sql_exec_addr)) {  // not in trans
          set_resource_svr(*sql_exec_addr);
          set_resource_conn_id(OB_INVALID_ID);
      } else { // not in trans
        common::ObAddr resource_server_addr;
        share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
        if (OB_FAIL(nonblock_get_leader(
                    GCONF.cluster_id, tenant_id, ls_id, resource_server_addr))) {
          LOG_WARN("nonblock get leader failed", K(ret), K(tenant_id), K(ls_id));
        } else {
          set_resource_svr(resource_server_addr);
          set_resource_conn_id(OB_INVALID_ID);
        }
      }
      if (FAILEDx(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
        LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
      }
      if (OB_SUCC(ret)) {
        ObObj tmp_sql_mode;
        if (OB_FAIL(get_session().get_sys_variable_by_name("sql_mode", tmp_sql_mode))) {
          LOG_WARN("fail to get sql mode", K(ret));
        } else {
          sql_mode = tmp_sql_mode.get_uint64();
        }
      }
      if (OB_SUCC(ret)) {
        get_session().store_query_string(sql);
        ObInnerSQLTransmitArg arg (MYADDR, get_resource_svr(), tenant_id, get_resource_conn_id(),
            sql, ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_WRITE,
            lib::Worker::CompatMode::ORACLE == get_compat_mode(), GCONF.cluster_id,
            THIS_WORKER.get_timeout_ts(), query_timeout, trx_timeout, sql_mode,
            ddl_info, is_load_data_exec, use_external_session_, consumer_group_id);
        arg.set_nls_formats(get_session().get_local_nls_date_format(),
                            get_session().get_local_nls_timestamp_format(),
                            get_session().get_local_nls_timestamp_tz_format());
        ObInnerSqlRpcStreamHandle *handler = res.remote_result_set().get_stream_handler();

        if (OB_ISNULL(handler)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("handler is null ptr", K(ret));
        } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
          LOG_WARN("fail to set tz info wrap", K(ret));
        } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(get_resource_svr()).by(tenant_id).
                           timeout(query_timeout).group_id(group_id_).inner_sql_sync_transmit(
                               arg, *(handler->get_result()), handler->get_handle()))) {
          // complement data for offline ddl may exceed rpc default timeout, thus need to set it to a bigger value for this proxy.
          LOG_WARN("inner_sql_sync_transmit process failed", K(ret), K(tenant_id));
        } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
          ret = handler->get_result()->get_err_code();
          LOG_WARN("failed to execute inner sql", K(ret));
        } else if (FALSE_IT(affected_rows = handler->get_result()->get_affected_rows())) {
        } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
        } else if (get_session().get_in_transaction()) {
          bool dml_or_savepoint =
            ObStmt::is_dml_write_stmt(handler->get_result()->get_stmt_type())
            || ObStmt::is_savepoint_stmt(handler->get_result()->get_stmt_type());
          get_session().set_has_exec_inner_dml(dml_or_savepoint);
        }
        if (get_session().get_ddl_info().is_ddl()) {
          SERVER_EVENT_ADD(
            "ddl", "send ddl inner sql",
            "tenant_id", tenant_id,
            "trace_id", *ObCurTraceId::get_trace_id(),
            "ret", ret,
            "affected_rows", affected_rows,
            "start_ts", res.execute_start_ts_,
            "end_ts", res.execute_end_ts_);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(res.close())) {
            LOG_WARN("close result set failed", K(ret), K(tenant_id), K(sql));
          } else if (!is_in_trans() && FALSE_IT(reset_resource_conn_info())) {
          }
        }
      }
    }
#ifndef NDEBUG
    if (tenant_id < OB_MAX_RESERVED_TENANT_ID) {  //only print log for sys table
      LOG_INFO("execute write sql", K(ret), K(tenant_id), K(affected_rows), K(sql));
    }
#endif
  }

  return ret;
}

// for setting timeout params in inner rpc arg
int ObInnerSQLConnection::get_session_timeout_for_rpc(int64_t &query_timeout, int64_t &trx_timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = 0;
  int64_t start_time = ObTimeUtility::current_time();
  get_session().set_query_start_time(start_time);
  if (OB_FAIL(set_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret));
  } else if (OB_FAIL(get_session().get_query_timeout(query_timeout))
             || OB_FAIL(get_session().get_tx_timeout(trx_timeout))) {
    LOG_WARN("get sessio timeout failed", KR(ret), K(query_timeout), K(trx_timeout));
  }
  return ret;
}

int ObInnerSQLConnection::execute_read(const uint64_t tenant_id,
                                       const char *sql,
                                       ObISQLClient::ReadResult &res,
                                       bool is_user_sql,
                                       const common::ObAddr *sql_exec_addr)
{
  return execute_read(GCONF.cluster_id, tenant_id, sql, res, is_user_sql, sql_exec_addr);
}

int ObInnerSQLConnection::execute_read(const int64_t cluster_id,
                                       const uint64_t tenant_id,
                                       const ObString &sql,
                                       ObISQLClient::ReadResult &res,
                                       bool is_user_sql,
                                       const common::ObAddr *sql_exec_addr)
{

  int ret = OB_SUCCESS;
  auto function = [&]() {
    res.reuse();
    return execute_read_inner(cluster_id, tenant_id, sql, res, is_user_sql, sql_exec_addr);
  };
  if (OB_FAIL(retry_while_no_tenant_resource(cluster_id, tenant_id, function))) {
    LOG_WARN("execute_read failed", K(ret), K(cluster_id), K(tenant_id));
  }
  return ret;
}

bool ObInnerSQLConnection::is_local_execute(const int64_t cluster_id, const uint64_t tenant_id)
{
  bool local_execute = true;

  if (GCONF.cluster_id != cluster_id) {
    local_execute = false;
  } else if (is_resource_conn() || is_extern_session()) {
    local_execute = true;
  } else if (is_in_trans()) {
    // Force to remote execute when inner sql is in trans and other inner sql in the same trans has been remote executed before.
    // Although local obs has tenant resource now. Vice versa.
    local_execute = OB_INVALID_ID == get_resource_conn_id();
  } else if (force_remote_execute_) {
    local_execute = false;
  } else if (!GCTX.omt_->is_available_tenant(tenant_id)) {
    local_execute = false;
  }
  return local_execute;
}

int ObInnerSQLConnection::execute_read_inner(const int64_t cluster_id,
                                             const uint64_t tenant_id,
                                             const ObString &sql,
                                             ObISQLClient::ReadResult &res,
                                             bool is_user_sql,
                                             const common::ObAddr *sql_exec_addr)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(inner_execute_read);
  ObInnerSQLReadContext *read_ctx = NULL;
  const static int64_t ctx_size = sizeof(ObInnerSQLReadContext);
  static_assert(ctx_size <= ObISQLClient::ReadResult::BUF_SIZE, "buffer not enough");
  ObSqlQueryExecutor executor(sql);
  const bool local_execute = is_local_execute(cluster_id, tenant_id);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not inited", K(ret));
  } else if (0 == sql.length() || NULL == sql.ptr()  || '\0' == *(sql.ptr())
             || OB_INVALID_ID == tenant_id
             || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sql), K(tenant_id), K(cluster_id));
  }
  if (OB_FAIL(ret)) {
  } else if (local_execute && OB_FAIL(switch_tenant(tenant_id))) {
    LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(res.create_handler(read_ctx, *this))) {
    LOG_WARN("create result handler failed", K(ret));
  } else if (OB_FAIL(read_ctx->get_result().init(local_execute))) {
    LOG_WARN("init result set", K(ret), K(local_execute));
  } else if (local_execute) {
    read_ctx->get_result().result_set().set_user_sql(is_user_sql);
    if (OB_FAIL(query(executor, read_ctx->get_result(),
                      &read_ctx->get_vt_iter_factory()))) {
      LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
    }
  } else if (is_resource_conn()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resource_conn of resource_svr still doesn't has the tenant resource",
             K(ret), K(MYADDR), K(tenant_id), K(get_resource_conn_id()));
  } else if (!local_execute) {
    TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
    int64_t query_timeout = OB_INVALID_TIMESTAMP;
    int64_t trx_timeout = OB_INVALID_TIMESTAMP;
    ObSQLMode sql_mode = 0;
    const ObSessionDDLInfo &ddl_info = get_session().get_ddl_info();
    bool is_load_data_exec = get_session().is_load_data_exec_session();
    if (is_in_trans()) {
      if (!get_resource_svr().is_valid() || OB_INVALID_ID == get_resource_conn_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resource_conn_id or resource_svr is invalid",
                 K(ret), K(get_resource_svr()), K(get_resource_conn_id()));
      } else if (GCONF.cluster_id != cluster_id) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not acrocc cluster in trans", KR(ret), K(resource_conn_id_));
      }
    } else if (!OB_ISNULL(sql_exec_addr)) {
       set_resource_svr(*sql_exec_addr);
       set_resource_conn_id(OB_INVALID_ID);
       LOG_INFO("set sql exec addr", KR(ret), K(*sql_exec_addr));
    } else {
      common::ObAddr resource_server_addr;
      share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
      if (OB_FAIL(nonblock_get_leader(
                  cluster_id, tenant_id, ls_id, resource_server_addr))) {
        LOG_WARN("nonblock get leader failed", K(ret), K(tenant_id), K(ls_id), K(cluster_id));
      } else {
        set_resource_svr(resource_server_addr);
        set_resource_conn_id(OB_INVALID_ID);
      }
    }
    if (FAILEDx(get_session_timeout_for_rpc(query_timeout, trx_timeout))) {
      LOG_WARN("fail to get_session_timeout_for_rpc", K(ret), K(query_timeout), K(trx_timeout));
    }
    if (OB_SUCC(ret)) {
      ObObj tmp_sql_mode;
      if (OB_FAIL(get_session().get_sys_variable_by_name("sql_mode", tmp_sql_mode))) {
        LOG_WARN("fail to get sql mode", K(ret));
      } else {
        sql_mode = tmp_sql_mode.get_uint64();
      }
    }
    if (OB_SUCC(ret)) {
      get_session().store_query_string(sql);
      ObInnerSQLTransmitArg arg (MYADDR, get_resource_svr(), tenant_id, get_resource_conn_id(),
          sql, ObInnerSQLTransmitArg::OPERATION_TYPE_EXECUTE_READ,
          lib::Worker::CompatMode::ORACLE == get_compat_mode(), GCONF.cluster_id,
          THIS_WORKER.get_timeout_ts(), query_timeout, trx_timeout, sql_mode,
          ddl_info, is_load_data_exec, use_external_session_);
      arg.set_nls_formats(get_session().get_local_nls_date_format(),
                          get_session().get_local_nls_timestamp_format(),
                          get_session().get_local_nls_timestamp_tz_format());
      ObInnerSqlRpcStreamHandle *handler =
          read_ctx->get_result().remote_result_set().get_stream_handler();
      if (OB_ISNULL(handler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("handler is null ptr", K(ret));
      } else if (OB_FAIL(arg.set_tz_info_wrap(get_session().get_tz_info_wrap()))) {
        LOG_WARN("fail to set tz info wrap", K(ret));
      } else if (OB_FAIL(GCTX.inner_sql_rpc_proxy_->to(get_resource_svr()).
            dst_cluster_id(cluster_id).by(tenant_id).timeout(query_timeout).group_id(group_id_).
                         inner_sql_sync_transmit(
                             arg, *(handler->get_result()), handler->get_handle()))) {
        LOG_WARN("inner_sql_sync_transmit process failed", K(ret), K(tenant_id), K(cluster_id));
      } else if (OB_SUCCESS != handler->get_result()->get_err_code()) {
        ret = handler->get_result()->get_err_code();
        LOG_WARN("failed to execute inner sql", K(ret));
      } else if (FALSE_IT(read_ctx->get_result().remote_result_set().set_stmt_type(
                          handler->get_result()->get_stmt_type()))) {
      } else if (OB_FAIL(read_ctx->get_result().open())) {
        LOG_WARN("result set open failed", K(ret));
      } else if (FALSE_IT(read_ctx->get_result().set_is_read(true))) {
      } else if (FALSE_IT(get_session().set_trans_type(transaction::ObTxClass::SYS))) {
      }
    }
    if (!is_in_trans()) {
      reset_resource_conn_info();
    }
  }
  if (OB_SUCC(ret)) {
    ref_ctx_ = read_ctx;
  }
  return ret;
}

int ObInnerSQLConnection::nonblock_get_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const share::ObLSID ls_id,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  TimeoutGuard timeout_guard(*this); // backup && restore worker/session timeout
  int64_t abs_timeout_us = 0;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t old_query_start_time = get_session().get_query_start_time();
  get_session().set_query_start_time(start_time);

  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_FAIL(set_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_tenant_dropped = false;
    int64_t tmp_abs_timeout_us = 0;
    const int64_t retry_interval_us = 200 * 1000; // 200ms
    do {
      ret = OB_SUCCESS;
      tmp_abs_timeout_us = ObTimeUtility::current_time() + GCONF.location_cache_refresh_sql_timeout;
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(THIS_WORKER.get_timeout_ts()));
      } else if (OB_TMP_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(tenant_id,
                                                                             is_tenant_dropped))) {
        LOG_WARN("user tenant has been dropped", KR(ret), K(tenant_id));
      } else if (is_tenant_dropped) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("user tenant has been dropped", KR(ret), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
                  cluster_id, tenant_id, ls_id, leader, tmp_abs_timeout_us, retry_interval_us))) {
        LOG_WARN("get leader with retry until timeout failed",  KR(ret), K(tenant_id), K(ls_id),
            K(leader), K(cluster_id), K(tmp_abs_timeout_us), K(retry_interval_us));
      } else if (OB_UNLIKELY(!leader.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader addr invalid", K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(leader));
      } else {
        LOG_DEBUG("get participants", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
      }
    } while (is_location_service_renew_error(ret));
  }
  get_session().set_query_start_time(old_query_start_time);
  return ret;
}

int ObInnerSQLConnection::execute(
    const uint64_t tenant_id, sqlclient::ObIExecutor &executor)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  FLTSpanGuard(inner_execute);
  SMART_VAR(ObInnerSQLResult, res, get_session()) {
    if (OB_FAIL(res.init())) {
      LOG_WARN("init result set", K(ret));
    } else if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id), K(executor));
    } else if (OB_FAIL(switch_tenant(tenant_id))) {
      LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(query(executor, res))) {
      LOG_WARN("executor execute failed", K(ret), K(tenant_id), K(executor));
    } else {
      lib::CompatModeGuard g(get_compat_mode());
      MTL_SWITCH(tenant_id) {
        WITH_CONTEXT(res.mem_context_) {
          if (OB_FAIL(executor.process_result(res.result_set()))) {
            LOG_WARN("process result failed", K(ret));
          } else {
            if (OB_FAIL(res.close())) {
              LOG_WARN("close result set failed", K(ret), K(tenant_id), K(executor));
            }
          }
        }
      }
    }
    LOG_INFO("execute executor", K(ret), K(tenant_id), K(executor));
  }
  return ret;
}

int ObInnerSQLConnection::switch_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // called in init(), can not check inited_ flag.
  if (OB_INVALID_ID == tenant_id) {
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (is_inner_session()) {
    if (OB_FAIL(get_session().switch_tenant(tenant_id))){
      LOG_WARN("Init sys tenant in session error", K(ret));
    } else if (OB_FAIL(get_session().set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID))) {
      LOG_WARN("Set sys user in session error", K(ret));
    } else {
      get_session().set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
      get_session().set_database_id(OB_SYS_DATABASE_ID);
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObInnerSQLConnection::set_timeout(int64_t &abs_timeout_us)
{
  int ret = OB_SUCCESS;
  const ObTimeoutCtx &ctx = ObTimeoutCtx::get_ctx();
  const int64_t now = ObTimeUtility::current_time();
  int64_t timeout = 0;
  int64_t trx_timeout = 0;
  abs_timeout_us = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", K(ret), K(abs_timeout_us), K(now), K(THIS_WORKER.get_timeout_ts()));
    } else {
      if (THIS_WORKER.get_timeout_remain() < OB_MAX_USER_SPECIFIED_TIMEOUT) {
        timeout = THIS_WORKER.get_timeout_remain();
        abs_timeout_us = THIS_WORKER.get_timeout_ts();
        LOG_DEBUG("set timeout by worker", K(timeout), K(abs_timeout_us));
        trx_timeout = timeout;
        LOG_DEBUG("set timeout according to THIS_WORKER", K(timeout), K(trx_timeout), K(abs_timeout_us));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_timeout_set()) {
      if (ctx.get_abs_timeout() < abs_timeout_us || 0 == abs_timeout_us) {
        abs_timeout_us = ctx.get_abs_timeout();
        timeout = ctx.get_timeout();
        trx_timeout = timeout;
      }
      if (ctx.is_trx_timeout_set()) {
        trx_timeout = ctx.get_trx_timeout_us();
      }
      if (timeout <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(ctx), K(abs_timeout_us));
      }
    }
#if !defined(NDEBUG)
    LOG_DEBUG("set timeout according to time_ctx", K(timeout), K(trx_timeout), K(abs_timeout_us));
#endif
  }

  if (OB_SUCC(ret)) {
    if (0 == abs_timeout_us) {
      timeout = (user_timeout_ > 0) ? user_timeout_ : GCONF.internal_sql_execute_timeout;
      trx_timeout = timeout;
      abs_timeout_us = now + timeout;
    }
  }

  // no need to set session timeout for outer session if no timeout ctx
  if (OB_SUCC(ret)
      && (is_inner_session() || ctx.is_timeout_set() || ctx.is_trx_timeout_set())) {
    if (OB_FAIL(set_session_timeout(timeout, trx_timeout))) {
      LOG_WARN("set session timeout failed", K(timeout), K(trx_timeout), K(ret));
    } else {
      THIS_WORKER.set_timeout_ts(get_session().get_query_start_time() + timeout);
    }
  }
  return ret;
}

int ObInnerSQLConnection::set_session_timeout(int64_t query_timeout, int64_t trx_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    LOG_DEBUG("set query timeout", K(query_timeout));
    ObObj val;
    val.set_int(query_timeout);
    if (OB_FAIL(get_session().update_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, val))) {
      LOG_WARN("set sys variable failed", K(ret), K(OB_SV_QUERY_TIMEOUT), K(val));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("set trx timeout", K(trx_timeout));
    ObObj val;
    val.set_int(trx_timeout);
    if (OB_FAIL(get_session().update_sys_variable(SYS_VAR_OB_TRX_TIMEOUT, val))) {
      LOG_WARN("set sys variable failed", K(ret), K(OB_SV_TRX_TIMEOUT), K(val));
    }
  }

  return ret;
}

void ObInnerSQLConnection::dump_conn_bt_info()
{
  const int64_t BUF_SIZE = (1LL << 10);
  char buf_bt[BUF_SIZE];
  buf_bt[0] = '\0';
  char buf_time[OB_MAX_TIMESTAMP_LENGTH];
  int64_t pos = 0;
  (void)ObTimeUtility2::usec_to_str(init_timestamp_, buf_time, OB_MAX_TIMESTAMP_LENGTH, pos);
  pos = 0;
  parray(buf_bt, BUF_SIZE, (int64_t*)*&bt_addrs_, bt_size_);
  LOG_WARN_RET(OB_SUCCESS, "dump inner sql connection backtrace", "tid", tid_, "init time", buf_time, "backtrace", buf_bt);
}

void ObInnerSQLConnection::record_stat(sql::ObSQLSessionInfo& session,
                                       const stmt::StmtType type,
                                       bool is_from_pl)
{
#define ADD_STMT_STAT(type)                           \
  if (is_from_pl) {                                   \
    EVENT_INC(SQL_##type##_COUNT);                    \
    EVENT_ADD(SQL_##type##_TIME, time_cost);          \
  } else {                                            \
    EVENT_INC(SQL_INNER_##type##_COUNT);              \
    EVENT_ADD(SQL_INNER_##type##_TIME, time_cost);    \
  }

#define ADD_CASE(type)                                \
  case stmt::T_##type:                                \
    ADD_STMT_STAT(type);                              \
    break

  if (lib::is_diagnose_info_enabled()) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t time_cost = now - session.get_query_start_time();
    switch (type) {
      ADD_CASE(SELECT);
      ADD_CASE(INSERT);
      ADD_CASE(REPLACE);
      ADD_CASE(UPDATE);
      ADD_CASE(DELETE);
      default: {
        ADD_STMT_STAT(OTHER);
      }
    }
  }
#undef ADD_STMT_STAT
#undef ADD_CASE
}

int ObInnerSQLConnection::get_session_variable(const ObString &name, int64_t &val)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == name.case_compare("tx_isolation")) {
    // 隔离级别是一个varchar值
    ObObj obj;
    if (OB_FAIL(get_session().get_sys_variable_by_name(name, obj))) {
      LOG_WARN("get tx_isolation system variable value fail", K(ret), K(name));
    } else {
      // varchar转换为int
      val = transaction::ObTransIsolation::get_level(obj.get_string());
    }
  } else {
    ret = get_session().get_sys_variable_by_name(name, val);
  }
  return ret;
}

int ObInnerSQLConnection::set_session_variable(const ObString &name, int64_t val)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == name.case_compare("ob_check_sys_variable")) { // fake system variable
    if (0 == val) {
      LOG_TRACE("disable inner sql check sys variable");
    }
    (void)get_session().set_check_sys_variable(0 != val);
  } else if (0 == name.case_compare("tx_isolation")) {
    // 隔离级别是一个string
    ObObj obj;
    obj.set_varchar(transaction::ObTransIsolation::get_name(val));
    obj.set_collation_type(ObCharset::get_system_collation());
    if (OB_FAIL(get_session().update_sys_variable_by_name(name, obj))) {
      LOG_WARN("update sys variable by name fail", K(ret), K(name), K(obj), K(name), K(val));
    }
  } else if (OB_FAIL(get_session().update_sys_variable_by_name(name, val))) {
    LOG_WARN("failed to update sys variable", K(ret), K(name), K(val));
  } else if (0 == name.case_compare("ob_read_consistency")) {
    LOG_INFO("inner session use weak consitency", K(val), "inner_connection_p", this);
  }
  return ret;
}

lib::Worker::CompatMode ObInnerSQLConnection::get_compat_mode() const
{
  lib::Worker::CompatMode mode;
  if (is_oracle_compat_mode()) {
    mode = lib::Worker::CompatMode::ORACLE;
  } else {
    mode = lib::Worker::CompatMode::MYSQL;
  }
  return mode;
}

// nested session and sql execute for foreign key.

int ObInnerSQLConnection::begin_nested_session(ObSQLSessionInfo::StmtSavedValue &saved_session,
                                               SavedValue &saved_conn, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  if (!is_extern_session()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is not extern session", K(ret));
  } else if (OB_FAIL(extern_session_->begin_nested_session(saved_session, skip_cur_stmt_tables))) {
    LOG_WARN("failed to begin nested session", K(ret));
  } else {
    saved_conn.ref_ctx_ = ref_ctx_;
    saved_conn.execute_start_timestamp_ = execute_start_timestamp_;
    saved_conn.execute_end_timestamp_ = execute_end_timestamp_;
  }
  return ret;
}

int ObInnerSQLConnection::end_nested_session(ObSQLSessionInfo::StmtSavedValue &saved_session,
                                             SavedValue &saved_conn)
{
  int ret = OB_SUCCESS;
  if (!is_extern_session()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is not extern session", K(ret));
  } else if (OB_FAIL(extern_session_->end_nested_session(saved_session))) {
    LOG_WARN("failed to end nested session", K(ret));
  } else {
    ref_ctx_ = saved_conn.ref_ctx_;
    execute_start_timestamp_ = saved_conn.execute_start_timestamp_;
    execute_end_timestamp_ = saved_conn.execute_end_timestamp_;
    saved_conn.reset();
  }
  return ret;
}
} // end of namespace observer
} // end of namespace oceanbase

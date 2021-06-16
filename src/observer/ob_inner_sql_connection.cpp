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
#include "share/ob_worker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "common/ob_timeout_ctx.h"
#include "common/ob_smart_call.h"
#include "share/ob_debug_sync.h"
#include "share/ob_time_utility2.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_server_config.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_partition_location_cache.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "observer/ob_server.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/ob_req_time_service.h"
#include "ob_inner_sql_connection_pool.h"
#include "ob_inner_sql_read_context.h"
#include "ob_inner_sql_result.h"
#include "storage/transaction/ob_trans_define.h"  // ObTransIsolation
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace share;
using namespace share::schema;

namespace observer {

constexpr const char ObInnerSQLConnection::LABEL[];

class ObInnerSQLConnection::ObSqlQueryExecutor : public sqlclient::ObIExecutor {
public:
  explicit ObSqlQueryExecutor(const ObString& sql) : sql_(sql)
  {}
  explicit ObSqlQueryExecutor(const char* sql) : sql_(ObString::make_string(sql))
  {}

  virtual ~ObSqlQueryExecutor()
  {}

  virtual int execute(sql::ObSql& engine, sql::ObSqlCtx& ctx, sql::ObResultSet& res)
  {
    observer::ObReqTimeGuard req_timeinfo_guard;
    res.get_session().store_query_string(sql_);
    return engine.stmt_query(sql_, ctx, res);
  }

  // process result after result open
  virtual int process_result(sql::ObResultSet&) override
  {
    return OB_SUCCESS;
  }

  INHERIT_TO_STRING_KV("ObIExecutor", ObIExecutor, K_(sql));

private:
  ObString sql_;
};

ObInnerSQLConnection::TimeoutGuard::TimeoutGuard(ObInnerSQLConnection& conn) : conn_(conn)
{
  int ret = OB_SUCCESS;
  worker_timeout_ = THIS_WORKER.get_timeout_ts();
  if (OB_FAIL(conn_.get_session().get_query_timeout(query_timeout_)) ||
      OB_FAIL(conn_.get_session().get_tx_timeout(trx_timeout_))) {
    LOG_ERROR("get timeout failed", K(ret));
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
  if (OB_FAIL(conn_.get_session().get_query_timeout(query_timeout)) ||
      OB_FAIL(conn_.get_session().get_tx_timeout(trx_timeout))) {
    LOG_ERROR("get timeout failed", K(ret));
  } else {
    if (query_timeout != query_timeout_ || trx_timeout != trx_timeout_) {
      if (OB_FAIL(conn_.set_session_timeout(query_timeout_, trx_timeout_))) {
        LOG_ERROR("set session timeout failed", K(ret));
      }
    }
  }
}

ObInnerSQLConnection::ObInnerSQLConnection()
    : inited_(false),
      inner_session_(),
      extern_session_(NULL),
      is_spi_conn_(false),
      ref_cnt_(0),
      pool_(NULL),
      schema_service_(NULL),
      ob_sql_(NULL),
      vt_iter_creator_(NULL),
      ref_ctx_(NULL),
      partition_table_operator_(NULL),
      sql_modifier_(NULL),
      init_timestamp_(0),
      tid_(-1),
      bt_size_(0),
      bt_addrs_(),
      execute_start_timestamp_(0),
      execute_end_timestamp_(0),
      config_(NULL),
      associated_client_(NULL),
      primary_schema_versions_()
{}

ObInnerSQLConnection::~ObInnerSQLConnection()
{
  if (0 < ref_cnt_) {
    LOG_ERROR("connection be referenced while destruct", K_(ref_cnt));
  }
}

int ObInnerSQLConnection::init(ObInnerSQLConnectionPool* pool, ObMultiVersionSchemaService* schema_service,
    ObSql* ob_sql, ObVTIterCreator* vt_iter_creator, const share::ObPartitionTableOperator* partition_table_operator,
    common::ObServerConfig* config, sql::ObSQLSessionInfo* extern_session, /* = NULL */
    ObISQLClient* client_addr,                                             /* = NULL */
    ObRestoreSQLModifier* sql_modifier /* = NULL */)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("connection init twice", K(ret));
  } else if ((NULL == pool && NULL == sql_modifier) || NULL == schema_service || NULL == ob_sql ||
             NULL == vt_iter_creator || NULL == partition_table_operator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service should not be NULL",
        K(ret),
        KP(sql_modifier),
        KP(pool),
        KP(schema_service),
        KP(ob_sql),
        KP(vt_iter_creator),
        KP(partition_table_operator));
  } else {
    pool_ = pool;
    schema_service_ = schema_service;
    ob_sql_ = ob_sql;
    vt_iter_creator_ = vt_iter_creator;
    partition_table_operator_ = partition_table_operator;
    sql_modifier_ = sql_modifier;
    init_timestamp_ = ObTimeUtility::current_time();
    tid_ = GETTID();
    if (NULL == extern_session) {
      // Only backtrace internal used connection to avoid performance problems.
      bt_size_ = backtrace(bt_addrs_, MAX_BT_SIZE);
    }
    config_ = config;
    associated_client_ = client_addr;
    primary_schema_versions_.reset();
    if (NULL != client_addr) {
      oracle_mode_ = client_addr->is_oracle_mode();
    } else if (NULL != extern_session) {
      oracle_mode_ = ORACLE_MODE == extern_session->get_compatibility_mode();
    }
    if (OB_FAIL(init_session(extern_session))) {
      LOG_WARN("init session failed", K(ret));
    } else {
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
    primary_schema_versions_.reset();
    ref_ctx_ = NULL;
  }
  return ret;
}

void ObInnerSQLConnection::ref()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    ref_cnt_++;
    if (ref_cnt_ > TOO_MANY_REF_ALERT) {
      LOG_WARN("connection be referenced too many times, this should be rare", K_(ref_cnt));
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
    LOG_ERROR("not init conn pool");
  } else {
    if (0 == --ref_cnt_) {
      if (OB_FAIL(pool_->revert(this))) {
        LOG_WARN("revert connection failed", K(ret));
      }
    } else {
      // extern_session_ = NULL;
    }
  }
}

int ObInnerSQLConnection::init_session(sql::ObSQLSessionInfo* extern_session)
{
  int ret = OB_SUCCESS;
  if (NULL == extern_session) {
    // called in init(), can not check inited_ flag.
    ObArenaAllocator* allocator = NULL;
    const bool print_info_log = false;
    const bool is_sys_tenant = true;
    ObPCMemPctConf pc_mem_conf;
    inner_session_.set_inner_session();
    ObObj mysql_mode;
    ObObj oracle_mode;
    mysql_mode.set_int(0);
    oracle_mode.set_int(1);
    ObObj mysql_sql_mode;
    ObObj oracle_sql_mode;
    mysql_sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
    oracle_sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
    if (OB_FAIL(inner_session_.init(INNER_SQL_SESS_VERSION, INNER_SQL_SESS_ID, INNER_SQL_PROXY_SESS_ID, allocator))) {
      LOG_WARN("init session failed", K(ret));
    } else if (OB_FAIL(inner_session_.load_default_sys_variable(print_info_log, is_sys_tenant))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_max_packet_size())) {
      LOG_WARN("fail to update max packet size", K(ret));
    } else if (OB_FAIL(inner_session_.init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init tenant", K(ret));
    } else if (OB_FAIL(switch_tenant(OB_SYS_TENANT_ID))) {
      LOG_WARN("set system tenant id failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_sys_variable(
                   SYS_VAR_SQL_MODE, oracle_mode_ ? oracle_sql_mode : mysql_sql_mode))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_sys_variable(
                   SYS_VAR_OB_COMPATIBILITY_MODE, oracle_mode_ ? oracle_mode : mysql_mode))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_sys_variable(
                   SYS_VAR_NLS_DATE_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_sys_variable(
                   SYS_VAR_NLS_TIMESTAMP_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_.update_sys_variable(
                   SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else {
      ObString database_name(OB_SYS_DATABASE_NAME);
      if (OB_FAIL(inner_session_.set_default_database(database_name))) {
        LOG_WARN("fail to set default database", K(ret), K(database_name));
      } else if (OB_FAIL(inner_session_.get_pc_mem_conf(pc_mem_conf))) {
        LOG_WARN("fail to get pc mem conf", K(ret));
      } else {
        inner_session_.set_plan_cache_manager(ob_sql_->get_plan_cache_manager());
        inner_session_.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
        inner_session_.set_use_static_typing_engine(false);
      }
    }
  } else {
    extern_session_ = extern_session;
  }
  return ret;
}

int ObInnerSQLConnection::init_result(ObInnerSQLResult& res, ObVirtualTableIteratorFactory* vt_iter_factory,
    int64_t retry_cnt, ObSchemaGetterGuard& schema_guard, bool is_prepare_protocol, bool is_prepare_stage,
    bool is_from_pl, bool is_dynamic_sql, bool is_dbms_sql, bool is_cursor)
{
  int ret = OB_SUCCESS;
  sql::ObResultSet& result_set = res.result_set();
  const ObGlobalContext& gctx = ObServer::get_instance().get_gctx();
  result_set.init_partition_location_cache(gctx.location_cache_, gctx.self_addr_, &schema_guard);
  result_set.get_exec_context().get_task_exec_ctx().set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  result_set.get_exec_context().get_task_exec_ctx().schema_service_ = gctx.schema_service_;
  res.sql_ctx().retry_times_ = retry_cnt;
  res.sql_ctx().merged_version_ = *(gctx.merged_version_);
  res.sql_ctx().vt_iter_factory_ = vt_iter_factory;
  res.sql_ctx().session_info_ = &get_session();
  res.sql_ctx().sql_proxy_ = gctx.sql_proxy_;
  res.sql_ctx().use_plan_cache_ = true;
  res.sql_ctx().disable_privilege_check_ = is_from_pl ? PRIV_CHECK_FLAG_IN_PL : PRIV_CHECK_FLAG_DISABLE;
  res.sql_ctx().partition_table_operator_ = partition_table_operator_;
  res.sql_ctx().partition_location_cache_ = &(result_set.get_partition_location_cache());
  res.sql_ctx().part_mgr_ = gctx.part_mgr_;
  res.sql_ctx().is_prepare_protocol_ = is_prepare_protocol;
  res.sql_ctx().is_prepare_stage_ = is_prepare_stage;
  res.sql_ctx().is_dynamic_sql_ = is_dynamic_sql;
  res.sql_ctx().is_dbms_sql_ = is_dbms_sql;
  res.sql_ctx().is_cursor_ = is_cursor;
  if (FALSE_IT(res.sql_ctx().schema_guard_ = &schema_guard)) {
    // do-nothing
  } else if (NULL == res.sql_ctx().partition_table_operator_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition table operator is NULL!", K(ret));
  } else if (OB_FAIL(res.result_set().init())) {
    LOG_WARN("result set init failed", K(ret));
  } else if (is_prepare_protocol && !is_dynamic_sql) {
    result_set.set_simple_ps_protocol();
  } else { /*do nothing*/
  }
  return ret;
}

int ObInnerSQLConnection::process_retry(
    ObInnerSQLResult& res, int& last_ret, int64_t abs_timeout_us, bool& need_retry, int64_t retry_cnt, bool is_from_pl)
{
  int ret = last_ret;
  sql::ObResultSet& result_set = res.result_set();
  bool is_direct_local_plan_retry = result_set.get_exec_context().get_direct_local_plan() &&
                                    (OB_NOT_MASTER == last_ret || OB_PARTITION_NOT_EXIST == last_ret);
  ObQueryRetryInfo& retry_info = inner_session_.get_retry_info_for_update();
  const bool non_blocking_refresh = false;
  bool repeatable_stmt = (ObStmt::is_dml_stmt(result_set.get_stmt_type()) ||
                          ObStmt::is_ddl_stmt(result_set.get_stmt_type(), result_set.has_global_variable()) ||
                          ObStmt::is_dcl_stmt(result_set.get_stmt_type()));
  int64_t now = ObTimeUtility::current_time();
  if (now >= abs_timeout_us) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout, do not need retry", K(ret), K(last_ret), K(abs_timeout_us), K(now));
    if (retry_info.is_rpc_timeout() || is_transaction_rpc_timeout_err(last_ret)) {
      int err1 = result_set.refresh_location_cache(true);
      if (OB_SUCCESS != err1) {
        LOG_WARN("fail to nonblock refresh location cache", K(ret), K(last_ret), K(err1));
      }
      LOG_WARN("sql rpc timeout, or trans rpc timeout, maybe location is changed, "
               "refresh location cache non blockly",
          K(ret),
          K(last_ret),
          K(retry_info.is_rpc_timeout()));
    }
  } else if (repeatable_stmt && did_no_retry_on_rpc_error_ &&
             (retry_info.is_rpc_timeout() || is_transaction_rpc_timeout_err(last_ret) ||
                 is_server_down_error(last_ret) || is_master_changed_error(last_ret))) {
    ret = OB_TIMEOUT;
    LOG_INFO("YZFDEBUG no retry for rpc error",
        K(ret),
        K(abs_timeout_us),
        K(now),
        K(retry_cnt),
        K(last_ret),
        K(is_direct_local_plan_retry));
    if (!is_direct_local_plan_retry) {
      int err1 = result_set.refresh_location_cache(true);
      if (OB_SUCCESS != err1) {
        LOG_WARN("fail to nonblock refresh location cache", K(ret), K(last_ret), K(err1));
      }
    }
  } else if (repeatable_stmt && is_try_lock_row_err(last_ret) && is_from_pl) {
    if (retry_cnt <= 0 || !get_session().get_pl_can_retry()) {
      need_retry = true;
    } else {
      need_retry = false;
      ret = last_ret;
    }
  } else if (is_schema_error(last_ret) && result_set.is_user_sql()) {
    if ((OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == last_ret) || (OB_SCHEMA_NOT_UPTODATE == last_ret) ||
        (OB_SCHEMA_ERROR == last_ret) || (OB_SCHEMA_EAGAIN == last_ret)) {
      need_retry = true;
      LOG_WARN("schema err, need to retry", K(last_ret));
    } else {
      LOG_WARN("schema err, but not need to retry", K(last_ret));
    }
  } else if (repeatable_stmt &&
             (is_master_changed_error(last_ret) || is_partition_change_error(last_ret) ||
                 is_server_down_error(last_ret) || is_process_timeout_error(last_ret) ||
                 is_get_location_timeout_error(last_ret) || is_try_lock_row_err(last_ret) ||
                 is_has_no_readable_replica_err(last_ret) || is_select_dup_follow_replic_err(last_ret) ||
                 is_trans_stmt_need_retry_error(last_ret) || is_transaction_set_violation_err(last_ret) ||
                 is_snapshot_discarded_err(last_ret))) {
    need_retry = true;
    const uint64_t* trace_id = ObCurTraceId::get();
    bool sql_trigger_by_user_req = (NULL != trace_id && 0 != trace_id[0] && 0 != trace_id[1]);
    if (is_location_leader_not_exist_error(last_ret) || is_has_no_readable_replica_err(last_ret)) {
      // retry_info.reset();
      retry_info.clear();
    }
    if (!is_try_lock_row_err(last_ret)) {
      LOG_INFO("sql execute need retry", K(ret), K(last_ret), K(retry_cnt), K(is_direct_local_plan_retry));
      if (!is_direct_local_plan_retry && OB_FAIL(result_set.refresh_location_cache(non_blocking_refresh))) {
        LOG_WARN("refresh location cache failed", K(ret), K(last_ret));
      }
    }
    if (sql_trigger_by_user_req) {
      const int64_t step = 1000 * 10;  // 10ms
      const int64_t max = 100;         // cost constant time after max retry_cnt
      const int64_t sleep_time_us = (retry_cnt < max ? retry_cnt : max) * step;
      LOG_INFO("sql execute need retry", K(ret), K(last_ret), K(retry_cnt), K(sleep_time_us));
      THIS_WORKER.sched_wait();
      usleep(static_cast<unsigned int>(sleep_time_us));
      THIS_WORKER.sched_run();
    }
  } else if (repeatable_stmt && is_data_not_readable_err(last_ret)) {
    need_retry = true;
    const int64_t step = 1000 * 10;  // 10ms
    const int64_t max = 10;          // cost constant time after max retry_cnt, so max sleep time is 100ms
    const int64_t sleep_time_us = (retry_cnt < max ? retry_cnt : max) * step;
    usleep(static_cast<unsigned int>(sleep_time_us));

    LOG_WARN("has read not readable data, retry in local thread",
        K(need_retry),
        K(ret),
        K(last_ret),
        K(retry_cnt),
        K(sleep_time_us));
  } else if (repeatable_stmt && is_gts_not_ready_err(last_ret)) {
    const int64_t sleep_time_us = 10 * 1000;  // 10ms
    need_retry = true;
    LOG_WARN("gts is not ready, need retry", K(ret), K(last_ret), K(retry_cnt));
    THIS_WORKER.sched_wait();
    usleep(static_cast<unsigned int>(sleep_time_us));
    THIS_WORKER.sched_run();
  } else if (repeatable_stmt && OB_AUTOINC_SERVICE_BUSY == last_ret) {
    const int64_t sleep_time_us = 10 * 1000;  // 10ms
    need_retry = true;
    LOG_WARN("conncurrent update autoinc cache, need retry", K(ret), K(last_ret), K(retry_cnt));
    THIS_WORKER.sched_wait();
    usleep(static_cast<unsigned int>(sleep_time_us));
    THIS_WORKER.sched_run();
  }
  if (get_session().is_nested_session()) {
    /**
     * right now, top session will retry, bug we can do something here like refresh XXX cache.
     * in future, nested session can retry if nested transaction is supported.
     */
    need_retry = false;
    ret = last_ret;
  }
  if (need_retry) {
    LOG_WARN("need retry, set ret to OB_SUCCESS", K(ret), K(last_ret), K(retry_cnt));
    retry_info.set_last_query_retry_err(last_ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

class ObInnerSQLTimeRecord : public ObITimeRecord {
public:
  ObInnerSQLTimeRecord(sql::ObSQLSessionInfo& session)
      : session_(session), execute_start_timestamp_(0), execute_end_timestamp_(0)
  {}

  int64_t get_send_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_receive_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_enqueue_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_run_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_process_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_single_process_timestamp() const
  {
    return session_.get_query_start_time();
  }
  int64_t get_exec_start_timestamp() const
  {
    return execute_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return execute_end_timestamp_;
  }

  void set_execute_start_timestamp(int64_t v)
  {
    execute_start_timestamp_ = v;
  }
  void set_execute_end_timestamp(int64_t v)
  {
    execute_end_timestamp_ = v;
  }

private:
  sql::ObSQLSessionInfo& session_;
  int64_t execute_start_timestamp_;
  int64_t execute_end_timestamp_;
};

int ObInnerSQLConnection::process_record(ObInnerSQLResult& res, sql::ObSQLSessionInfo& session,
    ObITimeRecord& time_record, int last_ret, int64_t execution_id, int64_t ps_stmt_id, int64_t routine_id,
    ObWaitEventDesc& max_wait_desc, ObWaitEventStat& total_wait_desc, ObExecRecord& exec_record,
    ObExecTimestamp& exec_timestamp, bool is_from_pl)
{
  int ret = OB_SUCCESS;

  UNUSED(routine_id);
  sql::ObResultSet& result_set = res.result_set();
  ObAuditRecordData& audit_record = session.get_audit_record();
  ObPhysicalPlan* plan = res.result_set().get_physical_plan();
  audit_record.seq_ = 0;  // don't use now
  audit_record.status_ = (0 == last_ret || OB_ITER_END == last_ret) ? obmysql::REQUEST_SUCC : last_ret;

  audit_record.client_addr_ = session.get_peer_addr();
  audit_record.user_client_addr_ = session.get_user_client_addr();
  audit_record.user_group_ = THIS_WORKER.get_group_id();
  audit_record.execution_id_ = execution_id;
  audit_record.ps_stmt_id_ = ps_stmt_id;
  MEMCPY(audit_record.sql_id_, res.sql_ctx().sql_id_, (int32_t)sizeof(audit_record.sql_id_));
  audit_record.affected_rows_ = res.result_set().get_affected_rows();
  audit_record.return_rows_ = res.result_set().get_return_rows();
  if (NULL != res.result_set().get_exec_context().get_task_executor_ctx()) {
    audit_record.partition_cnt_ = res.result_set().get_exec_context().get_task_executor_ctx()->get_related_part_cnt();
  }

  exec_record.max_wait_event_ = max_wait_desc;
  exec_record.wait_time_end_ = total_wait_desc.time_waited_;
  exec_record.wait_count_end_ = total_wait_desc.total_waits_;

  if (NULL != res.result_set().get_physical_plan()) {
    audit_record.plan_type_ = res.result_set().get_physical_plan()->get_plan_type();
    audit_record.table_scan_ = res.result_set().get_physical_plan()->contain_table_scan();
    audit_record.plan_id_ = res.result_set().get_physical_plan()->get_plan_id();
    audit_record.plan_hash_ = res.result_set().get_physical_plan()->get_plan_hash_value();
  }

  audit_record.is_executor_rpc_ = false;
  audit_record.is_inner_sql_ = true;
  audit_record.is_hit_plan_cache_ = res.result_set().get_is_from_plan_cache();
  audit_record.is_multi_stmt_ = false;  // if multi sql
  audit_record.trans_hash_ = session.get_trans_desc().get_trans_id().hash();

  bool first_record = (0 == audit_record.try_cnt_);
  ObExecStatUtils::record_exec_timestamp(time_record, first_record, exec_timestamp);
  audit_record.exec_timestamp_ = exec_timestamp;
  audit_record.exec_record_ = exec_record;

  ObIArray<ObTableRowCount>* table_row_count_list = NULL;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(res.result_set().get_exec_context());
  if (NULL != plan_ctx) {
    audit_record.consistency_level_ = plan_ctx->get_consistency_level();
    audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
    table_row_count_list = &(plan_ctx->get_table_row_count_list());
  }

  audit_record.update_stage_stat();

  // update v$sql statistics
  if (OB_SUCC(last_ret) && session.get_local_ob_enable_plan_cache()) {
    if (NULL != plan) {
      if (!(res.sql_ctx().self_add_plan_) && res.sql_ctx().plan_cache_hit_) {
        plan->update_plan_stat(audit_record,
            false,  // false mean not first update plan stat
            res.result_set().get_exec_context().get_is_evolution(),
            table_row_count_list);
      } else if (res.sql_ctx().self_add_plan_ && !res.sql_ctx().plan_cache_hit_) {
        plan->update_plan_stat(
            audit_record, true, res.result_set().get_exec_context().get_is_evolution(), table_row_count_list);
      }
    }
  }

  record_stat(session, result_set.get_stmt_type(), is_from_pl);
  ObSQLUtils::handle_audit_record(false, EXECUTE_INNER, session, res.result_set().get_exec_context());
  return ret;
}

template <typename T>
int ObInnerSQLConnection::process_final(const T& sql, ObInnerSQLResult& res, int last_ret)
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

int ObInnerSQLConnection::do_query(sqlclient::ObIExecutor& executor, ObInnerSQLResult& res)
{
  int ret = OB_SUCCESS;
  WITH_CONTEXT(res.mem_context_)
  {
    bool is_restore = NULL != sql_modifier_;
    res.sql_ctx().is_restore_ = is_restore;
    res.sql_ctx().is_ddl_from_primary_ = is_ddl_from_primary();
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(ob_sql_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob_sql_ is NULL", K(ret));
    } else if (OB_FAIL(executor.execute(*ob_sql_, res.sql_ctx(), res.result_set()))) {
      LOG_WARN("executor execute failed", K(ret));
    } else {
      ObSQLSessionInfo& session = res.result_set().get_session();
      if (OB_ISNULL(res.sql_ctx().schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null");
      } else if (OB_FAIL(session.update_query_sensitive_system_variable(*(res.sql_ctx().schema_guard_)))) {
        LOG_WARN("update query affacted system variable failed", K(ret));
      } else if (OB_UNLIKELY(is_restore) && OB_FAIL(sql_modifier_->modify(res.result_set()))) {
        LOG_WARN("fail modify sql", K(res.result_set().get_statement_name()), K(ret));
      } else if (0 < primary_schema_versions_.count() && OB_FAIL(process_schema_version(res.result_set()))) {
        LOG_WARN("failed to procsee schema version", K(ret), K(primary_schema_versions_));
      } else if (OB_FAIL(res.open())) {
        LOG_WARN("result set open failed", K(ret), K(executor));
      }
    }
  }

  return ret;
}

int ObInnerSQLConnection::process_schema_version(sql::ObResultSet& result_set)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 >= primary_schema_versions_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema version is invalid", K(ret), K(primary_schema_versions_));
  } else {
    ObICmd* cmd = const_cast<ObICmd*>(result_set.get_cmd());
    if (OB_ISNULL(cmd)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmd is null", K(ret), K(result_set));
    } else {
      sql::ObDDLStmt* ddl_stmt = static_cast<sql::ObDDLStmt*>(cmd);
      if (OB_ISNULL(ddl_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type error", K(ret), K(cmd));
      } else if (OB_FAIL(ddl_stmt->get_ddl_arg().primary_schema_versions_.assign(primary_schema_versions_))) {
        LOG_WARN("failed to assign ddl_stmt", K(ret), K(primary_schema_versions_), K(ddl_stmt));
      }
    }
  }
  return ret;
}

int ObInnerSQLConnection::query(sqlclient::ObIExecutor& executor, ObInnerSQLResult& res,
    ObVirtualTableIteratorFactory* vt_iter_factory, bool is_from_pl)
{
  int ret = OB_SUCCESS;
  share::CompatModeGuard g(get_compat_mode());
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;

  exec_timestamp.exec_type_ = sql::InnerSql;
  const ObGlobalContext& gctx = ObServer::get_instance().get_gctx();
  int64_t start_time = ObTimeUtility::current_time();
  if (!is_from_pl) {
    get_session().set_query_start_time(start_time);
  }
  get_session().set_trans_type(transaction::ObTransType::TRANS_SYSTEM);
  int64_t abs_timeout_us = 0;
  int64_t execution_id = 0;
  bool is_trace_id_init = true;
  ObQueryRetryInfo& retry_info = inner_session_.get_retry_info_for_update();
  ObAddr host_addr = ObCurTraceId::get_addr();
  if (!host_addr.is_valid()) {
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
    LOG_ERROR("connection still be referred by previous sql result, can not execute sql now", K(ret), K(executor));
  } else if (OB_FAIL(set_timeout(abs_timeout_us, is_from_pl))) {
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
  if (OB_SUCC(ret)) {
    execution_id = ob_sql_->get_execution_id();
    bool need_retry = true;
    for (int64_t retry_cnt = 0; need_retry; ++retry_cnt) {
      need_retry = false;
      retry_info.clear_state_before_each_retry();
      if (retry_cnt > 0) {  // reset result set
        bool is_user_sql = res.result_set().is_user_sql();
        res.~ObInnerSQLResult();
        new (&res) ObInnerSQLResult(get_session());
        if (OB_FAIL(res.init())) {
          LOG_WARN("fail to init result set", K(ret));
        } else {
          res.result_set().set_user_sql(is_user_sql);
        }
      }

      int64_t local_tenant_schema_version = -1;
      int64_t local_sys_schema_version = -1;
      ObWaitEventDesc max_wait_desc;
      ObWaitEventStat total_wait_desc;
      const bool enable_perf_event = lib::is_diagnose_info_enabled();
      const bool enable_sql_audit =
          GCONF.enable_sql_audit && get_session().get_local_ob_enable_sql_audit() && !is_from_pl;
      {
        ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
        ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);

        if (enable_sql_audit) {
          exec_record.record_start();
        }

        const uint64_t tenant_id = get_session().get_effective_tenant_id();
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, res.schema_guard_))) {
          LOG_WARN("get schema guard failed", K(ret));
        } else if (OB_FAIL(init_result(res, vt_iter_factory, retry_cnt, res.schema_guard_, false, false, is_from_pl))) {
          LOG_WARN("failed to init result", K(ret));
        } else if (OB_FAIL(res.schema_guard_.get_schema_version(tenant_id, local_tenant_schema_version))) {
          LOG_WARN("get tenant schema version failed", K(ret), K(ob_sql_));
        } else if (OB_FAIL(res.schema_guard_.get_schema_version(OB_SYS_TENANT_ID, local_sys_schema_version))) {
          LOG_WARN("get sys tenant schema version failed", K(ret), K(ob_sql_));
        } else if (OB_UNLIKELY(is_extern_session())) {
          res.result_set().get_exec_context().get_task_exec_ctx().set_query_tenant_begin_schema_version(
              local_tenant_schema_version);
          res.result_set().get_exec_context().get_task_exec_ctx().set_query_sys_begin_schema_version(
              local_sys_schema_version);
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(SMART_CALL(do_query(executor, res)))) {
          LOG_WARN("execute failed", K(ret), K(executor), K(retry_cnt));
          int tmp_ret = process_retry(res, ret, abs_timeout_us, need_retry, retry_cnt, is_from_pl);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("failed to process retry", K(tmp_ret), K(ret), K(executor), K(retry_cnt));
          }
          ret = tmp_ret;
          // moved here from ObInnerSQLConnection::do_query() -> ObInnerSQLResult::open().
          int close_ret = res.force_close(need_retry);
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("failed to close result", K(close_ret), K(ret));
          }
        }
        execute_start_timestamp_ = res.get_execute_start_ts();
        execute_end_timestamp_ = res.get_execute_end_ts();

        if (enable_sql_audit) {
          exec_record.record_end();
        }
      }

      if (enable_sql_audit && res.is_inited()) {
        ObInnerSQLTimeRecord time_record(get_session());
        time_record.set_execute_start_timestamp(execute_start_timestamp_);
        time_record.set_execute_end_timestamp(execute_end_timestamp_);
        int record_ret = process_record(res,
            get_session(),
            time_record,
            ret,
            execution_id,
            OB_INVALID_ID,
            OB_INVALID_ID,
            max_wait_desc,
            total_wait_desc,
            exec_record,
            exec_timestamp);
        if (OB_SUCCESS != record_ret) {
          LOG_WARN("failed to process record", K(executor), K(record_ret), K(ret));
        }
      }

      if (res.is_inited()) {
        if (get_session().get_in_transaction()) {
          if (ObStmt::is_dml_write_stmt(res.result_set().get_stmt_type())) {
            get_session().set_has_inner_dml_write(true);
          }
        }
      }
    }
  }
  if (res.is_inited()) {
    int aret = process_final(executor, res, ret);
    if (OB_SUCCESS != aret) {
      LOG_WARN("failed to process final", K(executor), K(aret), K(ret));
    }
  }

  if (false == is_trace_id_init) {
    common::ObCurTraceId::reset();
  }

  retry_info.reset();
  return ret;
}

int ObInnerSQLConnection::do_prepare(const common::ObString& sql, ObInnerSQLResult& res)
{
  int ret = OB_SUCCESS;
  WITH_CONTEXT(res.mem_context_)
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (sql.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(sql));
    } else if (OB_FAIL(ob_sql_->stmt_prepare(sql, res.sql_ctx(), res.result_set()))) {
      LOG_WARN("sql execute failed", K(ret), K(sql));
    }
  }
  return ret;
}

int ObInnerSQLConnection::prepare(const ObString& sql, bool is_dynamic_sql, bool is_dbms_sql, bool is_cursor,
    ObInnerSQLResult& res, ObVirtualTableIteratorFactory* vt_iter_factory)
{
  int ret = OB_SUCCESS;
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  int64_t execution_id = 0;
  exec_timestamp.exec_type_ = sql::InnerSql;
  const ObGlobalContext& gctx = ObServer::get_instance().get_gctx();
  int64_t old_query_start_time = get_session().get_query_start_time();
  get_session().set_query_start_time(ObTimeUtility::current_time());
  get_session().set_trans_type(transaction::ObTransType::TRANS_SYSTEM);
  // get_session().store_query_string(sql);
  int64_t abs_timeout_us = 0;
  const uint64_t* trace_id_val = ObCurTraceId::get();
  bool is_trace_id_init = true;
  ObQueryRetryInfo& retry_info = inner_session_.get_retry_info_for_update();
  if (0 == trace_id_val[0]) {
    is_trace_id_init = false;
    common::ObCurTraceId::init(observer::ObServer::get_instance().get_self());
  }

  // backup && restore worker/session timeout.
  TimeoutGuard timeout_guard(*this);

  // %vt_iter_factory may be NULL
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sql));
  } else if (NULL != ref_ctx_) {
    ret = OB_REF_NUM_NOT_ZERO;
    LOG_ERROR("connection still be referred by previous sql result, can not execute sql now", K(ret), K(sql));
  } else if (OB_FAIL(set_timeout(abs_timeout_us, true))) {
    LOG_WARN("set timeout failed", K(ret));
  } else if (OB_ISNULL(ob_sql_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql engine", K(ret), K(ob_sql_));
  } else if (OB_UNLIKELY(retry_info.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("retry info is inited", K(ret), K(retry_info), K(sql));
  } else if (OB_FAIL(retry_info.init())) {
    LOG_WARN("fail to init retry info", K(ret), K(retry_info), K(sql));
  } else {
    execution_id = ob_sql_->get_execution_id();
  }
  if (OB_SUCC(ret)) {
    bool need_retry = true;
    for (int64_t retry_cnt = 0; need_retry; ++retry_cnt) {
      need_retry = false;
      retry_info.clear_state_before_each_retry();
      if (retry_cnt > 0) {  // reset result set
        res.~ObInnerSQLResult();
        new (&res) ObInnerSQLResult(get_session());
        ret = res.init();
      }
      const uint64_t tenant_id = get_session().get_effective_tenant_id();
      if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, res.schema_guard_))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(init_result(res,
                     vt_iter_factory,
                     retry_cnt,
                     res.schema_guard_,
                     true,
                     true,
                     true,
                     is_dynamic_sql,
                     is_dbms_sql,
                     is_cursor))) {
        LOG_WARN("failed to init result", K(ret));
      } else if (OB_FAIL(do_prepare(sql, res))) {
        LOG_WARN("execute sql failed", K(ret), K(sql), K(retry_cnt));
        int tmp_ret = process_retry(res, ret, abs_timeout_us, need_retry, retry_cnt, true);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to process retry", K(tmp_ret), K(ret), K(sql), K(retry_cnt));
        }
        ret = tmp_ret;
      }
    }
  }
  if (res.is_inited()) {
    int aret = process_final(sql, res, ret);
    if (OB_SUCCESS != aret) {
      LOG_WARN("failed to process final", K(sql), K(aret), K(ret));
    }
  }
  if (false == is_trace_id_init) {
    common::ObCurTraceId::reset();
  }
  retry_info.reset();
  get_session().set_query_start_time(old_query_start_time);
  return ret;
}

int ObInnerSQLConnection::do_execute(const ParamStore& params, ObInnerSQLResult& res)
{
  int ret = OB_SUCCESS;
  WITH_CONTEXT(res.mem_context_)
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(ob_sql_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ob_sql_ is NULL", K(ret));
    } else if (OB_FAIL(ob_sql_->stmt_execute(res.result_set().get_statement_id(),
                   res.result_set().get_stmt_type(),
                   params,
                   res.sql_ctx(),
                   res.result_set(),
                   true /* is_inner_sql */))) {
      LOG_WARN("sql execute failed", K(res.result_set().get_statement_id()), K(ret));
    } else {
      ObSQLSessionInfo& session = res.result_set().get_session();
      if (OB_ISNULL(res.sql_ctx().schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null");
      } else if (OB_FAIL(session.update_query_sensitive_system_variable(*(res.sql_ctx().schema_guard_)))) {
        LOG_WARN("update query affacted system variable failed", K(ret));
      } else if (OB_UNLIKELY(NULL != sql_modifier_) && OB_FAIL(sql_modifier_->modify(res.result_set()))) {
        LOG_WARN("fail modify sql", K(res.result_set().get_statement_name()), K(ret));
      } else if (OB_FAIL(res.open())) {
        LOG_WARN("result set open failed", K(res.result_set().get_statement_id()), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObInnerSQLConnection::execute(ParamStore& params, ObInnerSQLResult& res,
    ObVirtualTableIteratorFactory* vt_iter_factory, bool is_from_pl, bool is_dynamic)
{
  int ret = OB_SUCCESS;
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = sql::InnerSql;
  const ObGlobalContext& gctx = ObServer::get_instance().get_gctx();
  const ObString& sql = res.result_set().get_statement_name();
  int64_t start_time = ObTimeUtility::current_time();
  if (!is_from_pl) {
    get_session().set_query_start_time(start_time);
  }
  get_session().set_trans_type(transaction::ObTransType::TRANS_SYSTEM);
  int64_t abs_timeout_us = 0;
  int64_t execution_id = 0;
  uint64_t stmt_id = res.result_set().get_statement_id();
  sql::stmt::StmtType stmt_type = res.result_set().get_stmt_type();
  const uint64_t* trace_id_val = ObCurTraceId::get();
  bool is_trace_id_init = true;
  ObQueryRetryInfo& retry_info = inner_session_.get_retry_info_for_update();
  if (0 == trace_id_val[0]) {
    is_trace_id_init = false;
    common::ObCurTraceId::init(observer::ObServer::get_instance().get_self());
  }

  // backup && restore worker/session timeout.
  TimeoutGuard timeout_guard(*this);

  // %vt_iter_factory may be NULL
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL != ref_ctx_) {
    ret = OB_REF_NUM_NOT_ZERO;
    LOG_ERROR("connection still be referred by previous sql result, can not execute sql now", K(ret), K(sql));
  } else if (OB_FAIL(set_timeout(abs_timeout_us, is_from_pl))) {
    LOG_WARN("set timeout failed", K(ret));
  } else if (OB_ISNULL(ob_sql_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql engine", K(ret), K(ob_sql_));
  } else if (OB_UNLIKELY(retry_info.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("retry info is inited", K(ret), K(retry_info), K(sql));
  } else if (OB_FAIL(retry_info.init())) {
    LOG_WARN("fail to init retry info", K(ret), K(retry_info), K(sql));
  } else {
    execution_id = ob_sql_->get_execution_id();
    bool need_retry = true;
    for (int64_t retry_cnt = 0; need_retry; ++retry_cnt) {
      need_retry = false;
      retry_info.clear_state_before_each_retry();
      if (retry_cnt > 0) {  // reset result set
        res.~ObInnerSQLResult();
        new (&res) ObInnerSQLResult(get_session());
        ret = res.init();
        if (OB_SUCC(ret)) {
          res.result_set().set_ps_protocol();
          res.result_set().set_statement_id(stmt_id);
          res.result_set().set_stmt_type(stmt_type);
        }
      }

      ObWaitEventDesc max_wait_desc;
      ObWaitEventStat total_wait_desc;
      const bool enable_perf_event = lib::is_diagnose_info_enabled();
      const bool enable_sql_audit =
          GCONF.enable_sql_audit && get_session().get_local_ob_enable_sql_audit() && !is_from_pl;
      {
        ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
        ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);

        if (enable_sql_audit) {
          exec_record.record_start();
        }

        const uint64_t tenant_id = get_session().get_effective_tenant_id();
        int ret_code = OB_SUCCESS;
        if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(tenant_id, res.schema_guard_))) {
          LOG_WARN("get schema guard failed", K(ret));
        } else if (OB_FAIL(init_result(
                       res, vt_iter_factory, retry_cnt, res.schema_guard_, false, false, is_from_pl, is_dynamic))) {
          LOG_WARN("failed to init result", K(ret));
        } else if (OB_FAIL(do_execute(params, res))) {
          LOG_WARN("execute sql failed", K(ret), K(sql), K(retry_cnt));
          ret_code = ret;
          int tmp_ret = process_retry(res, ret, abs_timeout_us, need_retry, retry_cnt, is_from_pl);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("failed to process retry", K(tmp_ret), K(ret), K(sql), K(retry_cnt));
          }
          ret = tmp_ret;
          // moved here from ObInnerSQLConnection::do_execute() -> ObInnerSQLResult::open().
          int close_ret = res.force_close(need_retry);
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("failed to close result", K(close_ret), K(ret), K(sql));
          }
        }
        ObSQLSessionInfo& session = res.result_set().get_session();
        session.set_session_in_retry(need_retry, ret_code);
        LOG_DEBUG("after process_retry",
            K(retry_cnt),
            K(ret),
            K(need_retry),
            K(inner_session_),
            KP(&inner_session_),
            K(inner_session_.get_is_in_retry()),
            K(session.get_is_in_retry_for_dup_tbl()),
            K(session),
            K(&session),
            K(session.get_is_in_retry()));

        execute_start_timestamp_ = res.get_execute_start_ts();
        execute_end_timestamp_ = res.get_execute_end_ts();
        if (enable_sql_audit) {
          exec_record.record_end();
        }
      }

      if (enable_sql_audit && res.is_inited()) {
        ObInnerSQLTimeRecord time_record(get_session());
        time_record.set_execute_start_timestamp(execute_start_timestamp_);
        time_record.set_execute_end_timestamp(execute_end_timestamp_);
        int record_ret = process_record(res,
            get_session(),
            time_record,
            ret,
            execution_id,
            OB_INVALID_ID,
            OB_INVALID_ID,
            max_wait_desc,
            total_wait_desc,
            exec_record,
            exec_timestamp);
        if (OB_SUCCESS != record_ret) {
          LOG_WARN("failed to process record", K(sql), K(record_ret), K(ret));
        }
      }

      if (get_session().get_in_transaction()) {
        if (ObStmt::is_dml_write_stmt(stmt_type)) {
          get_session().set_has_inner_dml_write(true);
        }
      }
    }
  }
  if (res.is_inited()) {
    int aret = process_final(sql, res, ret);
    if (OB_SUCCESS != aret) {
      LOG_WARN("failed to process final", K(sql), K(aret), K(ret));
    }
  }

  if (false == is_trace_id_init) {
    common::ObCurTraceId::reset();
  }

  retry_info.reset();

  return ret;
}

int ObInnerSQLConnection::start_transaction(bool with_snap_shot /* = false */)
{
  int ret = OB_SUCCESS;
  ObString sql;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (with_snap_shot) {
    sql = ObString::make_string("START TRANSACTION WITH CONSISTENT SNAPSHOT");
  } else {
    sql = ObString::make_string("START TRANSACTION");
  }
  ObSqlQueryExecutor executor(sql);
  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
    if (OB_FAIL(res.init())) {
      LOG_WARN("init result set", K(ret));
    } else if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (OB_FAIL(query(executor, res))) {
      LOG_WARN("start transaction failed", K(ret), K(with_snap_shot));
    } else if (OB_FAIL(res.close())) {
      LOG_WARN("close result set failed", K(ret));
    }
  }

  return ret;
}

int ObInnerSQLConnection::rollback()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObSqlQueryExecutor executor("ROLLBACK");
  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
    if (OB_FAIL(res.init())) {
      LOG_WARN("init result set", K(ret));
    } else if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (OB_FAIL(query(executor, res))) {
      LOG_WARN("rollback failed", K(ret));
    } else if (OB_FAIL(res.close())) {
      LOG_WARN("close result set failed", K(ret));
    }
  }

  return ret;
}

int ObInnerSQLConnection::commit()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_INNER_SQL_COMMIT);
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObSqlQueryExecutor executor("COMMIT");
  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
    if (OB_FAIL(res.init())) {
      LOG_WARN("init result set", K(ret));
    } else if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (OB_FAIL(query(executor, res))) {
      LOG_WARN("commit failed", K(ret));
    } else if (OB_FAIL(res.close())) {
      LOG_WARN("close result set failed", K(ret));
    }
  }

  return ret;
}

int ObInnerSQLConnection::execute_write(
    const uint64_t tenant_id, const char* sql, int64_t& affected_rows, bool is_user_sql)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObSqlQueryExecutor executor(sql);
  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
    if (OB_FAIL(res.init())) {
      LOG_WARN("init result set", K(ret));
    } else if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("connection not inited", K(ret));
    } else if (NULL == sql || '\0' == *sql || OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(sql), K(tenant_id));
    } else if (OB_FAIL(switch_tenant(tenant_id))) {
      LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
    } else if (FALSE_IT(res.result_set().set_user_sql(is_user_sql))) {
    } else if (OB_FAIL(query(executor, res))) {
      LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
    } else if (FALSE_IT(affected_rows = res.result_set().get_affected_rows())) {
    } else if (OB_FAIL(res.close())) {
      LOG_WARN("close result set failed", K(ret), K(tenant_id), K(sql));
    }
    primary_schema_versions_.reset();
    if (tenant_id < OB_MAX_RESERVED_TENANT_ID) {  // only print log for sys table
      LOG_INFO("execute write sql", K(ret), K(tenant_id), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObInnerSQLConnection::execute_read(
    const uint64_t tenant_id, const char* sql, ObISQLClient::ReadResult& res, bool is_user_sql, bool is_from_pl)
{
  int ret = OB_SUCCESS;
  ObInnerSQLReadContext* read_ctx = NULL;
  observer::ObReqTimeGuard req_timeinfo_guard;
  const static int64_t ctx_size = sizeof(ObInnerSQLReadContext);
  static_assert(ctx_size <= ObISQLClient::ReadResult::BUF_SIZE, "buffer not enough");
  ObSqlQueryExecutor executor(sql);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not inited", K(ret));
  } else if (NULL == sql || '\0' == *sql || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(switch_tenant(tenant_id))) {
    LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(res.create_handler(read_ctx, *this))) {
    LOG_WARN("create result handler failed", K(ret));
  } else if (OB_FAIL(read_ctx->get_result().init())) {
    LOG_WARN("init result set", K(ret));
  } else if (FALSE_IT(read_ctx->get_result().result_set().set_user_sql(is_user_sql))) {
    // nothing.
  } else if (OB_FAIL(query(executor, read_ctx->get_result(), &read_ctx->get_vt_iter_factory(), is_from_pl))) {
    LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
  }
  if (OB_SUCC(ret)) {
    ref_ctx_ = read_ctx;
  }
  return ret;
}

int ObInnerSQLConnection::execute(const uint64_t tenant_id, sqlclient::ObIExecutor& executor)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;
  SMART_VAR(ObInnerSQLResult, res, get_session())
  {
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
      share::CompatModeGuard g(get_compat_mode());
      WITH_CONTEXT(res.mem_context_)
      {
        if (OB_FAIL(executor.process_result(res.result_set()))) {
          LOG_WARN("process result failed", K(ret));
        } else {
          if (OB_FAIL(res.close())) {
            LOG_WARN("close result set failed", K(ret), K(tenant_id), K(executor));
          }
        }
      }
    }
    LOG_INFO("execute executor", K(ret), K(tenant_id), K(executor));
  }
  return ret;
}

int ObInnerSQLConnection::prepare(const uint64_t tenant_id, const ObString& sql, bool is_dynamic_sql, bool is_dbms_sql,
    bool is_cursor, ObISQLClient::ReadResult& res)
{
  int ret = OB_SUCCESS;
  ObInnerSQLReadContext* read_ctx = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not inited", K(ret));
  } else if (NULL == sql || '\0' == *sql || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(switch_tenant(tenant_id))) {
    LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(res.create_handler(read_ctx, *this))) {
    LOG_WARN("create result handler failed", K(ret));
  } else if (OB_FAIL(read_ctx->get_result().init())) {
    LOG_WARN("init result set", K(ret));
  } else if (OB_FAIL(prepare(sql,
                 is_dynamic_sql,
                 is_dbms_sql,
                 is_cursor,
                 read_ctx->get_result(),
                 &read_ctx->get_vt_iter_factory()))) {
    LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
  }
  if (OB_SUCC(ret)) {
    ref_ctx_ = read_ctx;
  }
  return ret;
}

int ObInnerSQLConnection::execute(const uint64_t tenant_id, const ObPsStmtId stmt_id, const stmt::StmtType stmt_type,
    ParamStore& params, ObISQLClient::ReadResult& res, bool is_from_pl, bool is_dynamic)
{
  int ret = OB_SUCCESS;
  ObInnerSQLReadContext* read_ctx = NULL;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObPsStmtInfoGuard ps_guard;
  ObPsStmtInfo* ps_info = NULL;
  ObPsCache* ps_cache = get_session().get_ps_cache();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt_id), K(tenant_id));
  } else if (OB_FAIL(switch_tenant(tenant_id))) {
    LOG_WARN("switch tenant_id failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(res.create_handler(read_ctx, *this))) {
    LOG_WARN("create result handler failed", K(ret));
  } else if (OB_FAIL(read_ctx->get_result().init())) {
    LOG_WARN("init result set", K(ret));
  } else if (OB_ISNULL(ps_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps cache is null", K(ret), K(ps_cache));
  } else if (OB_FAIL(ps_cache->get_stmt_info_guard(stmt_id, ps_guard))) {
    LOG_WARN("get stmt info guard failed", K(ret), K(stmt_id));
  } else if (OB_ISNULL(ps_info = ps_guard.get_stmt_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt info is null", K(ret), K(ps_info), K(stmt_id));
  } else {
    read_ctx->get_result().result_set().set_ps_protocol();
    read_ctx->get_result().result_set().set_statement_id(stmt_id);
    read_ctx->get_result().result_set().set_stmt_type(stmt_type);
    get_session().store_query_string(ps_info->get_ps_sql());

    if (OB_FAIL(execute(params, read_ctx->get_result(), &read_ctx->get_vt_iter_factory(), is_from_pl, is_dynamic))) {
      LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(stmt_id));
    }
  }
  if (OB_SUCC(ret)) {
    ref_ctx_ = read_ctx;
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
    if (OB_FAIL(get_session().switch_tenant(tenant_id))) {
      LOG_WARN("Init sys tenant in session error", K(ret));
    } else if (OB_FAIL(
                   get_session().set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, combine_id(tenant_id, OB_SYS_USER_ID)))) {
      LOG_WARN("Set sys user in session error", K(ret));
    } else {
      get_session().set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
      get_session().set_database_id(combine_id(tenant_id, OB_SYS_DATABASE_ID));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObInnerSQLConnection::set_timeout(int64_t& abs_timeout_us, bool is_from_pl)
{
  int ret = OB_SUCCESS;
  const ObTimeoutCtx& ctx = ObTimeoutCtx::get_ctx();
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
      LOG_WARN("already timeout", K(ret), K(abs_timeout_us), K(THIS_WORKER.get_timeout_ts()));
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
      timeout = GCONF.internal_sql_execute_timeout;
      trx_timeout = timeout;
      abs_timeout_us = now + timeout;
    }
  }

  // no need to set session timeout for outer session if no timeout ctx
  if (OB_SUCC(ret) && (is_inner_session() || ctx.is_timeout_set() || ctx.is_trx_timeout_set() || is_from_pl)) {
    if (!is_from_pl) {
      if (OB_FAIL(set_session_timeout(timeout, trx_timeout))) {
        LOG_WARN("set session timeout failed", K(timeout), K(trx_timeout), K(ret));
      } else {
        THIS_WORKER.set_timeout_ts(get_session().get_query_start_time() + timeout);
      }
    } else {
      int64_t query_timeout;
      OZ(get_session().get_query_timeout(query_timeout));
      OX(abs_timeout_us = get_session().get_query_start_time() > 0
                              ? get_session().get_query_start_time() + query_timeout
                              : ObTimeUtility::current_time() + query_timeout);
      if (OB_SUCC(ret) && THIS_WORKER.get_timeout_ts() > abs_timeout_us) {
        OX(THIS_WORKER.set_timeout_ts(abs_timeout_us));
      }
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
  for (int i = 0; i < bt_size_; ++i) {
    if (OB_UNLIKELY(pos + 1 > BUF_SIZE)) {
      LOG_WARN("buf is not large enough", K(pos), K(BUF_SIZE));
    } else {
      (void)databuff_printf(buf_bt, BUF_SIZE, pos, "%p ", bt_addrs_[i]);
    }
  }
  LOG_WARN("dump inner sql connection backtrace", "tid", tid_, "init time", buf_time, "backtrace", buf_bt);
}

void ObInnerSQLConnection::record_stat(sql::ObSQLSessionInfo& session, const stmt::StmtType type, bool is_from_pl)
{
#define ADD_STMT_STAT(type)                        \
  if (is_from_pl) {                                \
    EVENT_INC(SQL_##type##_COUNT);                 \
    EVENT_ADD(SQL_##type##_TIME, time_cost);       \
  } else {                                         \
    EVENT_INC(SQL_INNER_##type##_COUNT);           \
    EVENT_ADD(SQL_INNER_##type##_TIME, time_cost); \
  }

#define ADD_CASE(type)   \
  case stmt::T_##type:   \
    ADD_STMT_STAT(type); \
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

int ObInnerSQLConnection::get_session_variable(const ObString& name, int64_t& val)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == name.case_compare("ob_read_snapshot_version")) {  // fake system variable
    val = get_session().get_read_snapshot_version();
  } else if (0 == name.case_compare("tx_isolation")) {
    ObObj obj;
    if (OB_FAIL(get_session().get_sys_variable_by_name(name, obj))) {
      LOG_WARN("get tx_isolation system variable value fail", K(ret), K(name));
    } else {
      val = transaction::ObTransIsolation::get_level(obj.get_string());
    }
  } else {
    ret = get_session().get_sys_variable_by_name(name, val);
  }
  return ret;
}

int ObInnerSQLConnection::set_session_variable(const ObString& name, int64_t val)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == name.case_compare("ob_read_snapshot_version")) {  // fake system variable
    if (val > 0) {
      LOG_INFO("inner sql with read snapshot version", K(name), K(val));
    }
    (void)get_session().set_read_snapshot_version(val);
  } else if (0 == name.case_compare("ob_check_sys_variable")) {  // fake system variable
    if (0 == val) {
      LOG_INFO("disable inner sql check sys variable");
    }
    (void)get_session().set_check_sys_variable(0 != val);
  } else if (0 == name.case_compare("tx_isolation")) {
    ObObj obj;
    obj.set_varchar(transaction::ObTransIsolation::get_name(val));
    if (OB_FAIL(get_session().update_sys_variable_by_name(name, obj))) {
      LOG_WARN("update sys variable by name fail", K(ret), K(name), K(obj), K(name), K(val));
    }
  } else if (OB_FAIL(get_session().update_sys_variable_by_name(name, val))) {
    LOG_WARN("failed to update sys variable", K(ret), K(name), K(val));
  } else if (0 == name.case_compare("ob_read_consistency")) {
    LOG_INFO("YZFDEBUG inner session use weak consitency", K(val), "inner_connection_p", this);
  }
  return ret;
}

ObWorker::CompatMode ObInnerSQLConnection::get_compat_mode() const
{
  ObWorker::CompatMode mode;
  if (is_oracle_compat_mode()) {
    mode = ObWorker::CompatMode::ORACLE;
  } else {
    mode = ObWorker::CompatMode::MYSQL;
  }
  return mode;
}

// nested session and sql execute for foreign key.

int ObInnerSQLConnection::begin_nested_session(
    ObSQLSessionInfo::StmtSavedValue& saved_session, SavedValue& saved_conn, bool skip_cur_stmt_tables)
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

int ObInnerSQLConnection::end_nested_session(ObSQLSessionInfo::StmtSavedValue& saved_session, SavedValue& saved_conn)
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

int ObInnerSQLConnection::set_foreign_key_cascade(bool is_cascade)
{
  int ret = OB_SUCCESS;
  OV(is_extern_session());
  OX(extern_session_->set_foreign_key_casecade(is_cascade));
  return ret;
}

int ObInnerSQLConnection::get_foreign_key_cascade(bool& is_cascade) const
{
  int ret = OB_SUCCESS;
  OV(is_extern_session());
  OX(is_cascade = extern_session_->is_foreign_key_cascade());
  return ret;
}

int ObInnerSQLConnection::set_foreign_key_check_exist(bool is_check_exist)
{
  int ret = OB_SUCCESS;
  OV(is_extern_session());
  OX(extern_session_->set_foreign_key_check_exist(is_check_exist));
  return ret;
}

int ObInnerSQLConnection::get_foreign_key_check_exist(bool& is_check_exist) const
{
  int ret = OB_SUCCESS;
  OV(is_extern_session());
  OX(is_check_exist = extern_session_->is_foreign_key_check_exist());
  return ret;
}

int ObInnerSQLConnection::set_primary_schema_version(const common::ObIArray<int64_t>& primary_schema_versions)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(primary_schema_versions_.assign(primary_schema_versions))) {
    LOG_WARN("failed to copy assign schema version", K(ret), K(primary_schema_versions), K(primary_schema_versions_));
  }
  return ret;
}
}  // end of namespace observer
}  // end of namespace oceanbase

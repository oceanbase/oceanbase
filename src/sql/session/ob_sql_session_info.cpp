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

#define USING_LOG_PREFIX SQL_SESSION

#include "sql/session/ob_sql_session_info.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/alloc/alloc_func.h"
#include "lib/string/ob_sql_string.h"
#include "lib/rc/ob_rc.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "io/easy_io.h"
#include "rpc/ob_rpc_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "pl/sys_package/ob_dbms_sql.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "observer/mysql/ob_query_driver.h"
#include "observer/ob_server.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_create_synonym_stmt.h"
#include "sql/resolver/ddl/ob_drop_synonym_stmt.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/string/ob_string.h"
#include "sql/engine/px/ob_px_target_mgr.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_proto_trans_util.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/debug/ob_pl_debugger_manager.h"
#include "pl/sys_package/ob_pl_utl_file.h"
#endif
#include "lib/allocator/ob_mod_define.h"
#include "lib/string/ob_hex_utils_base.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "observer/ob_sql_client_decorator.h"
#include "ob_sess_info_verify.h"
#include "share/schema/ob_schema_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::pl;
using namespace oceanbase::obmysql;

static const int64_t DEFAULT_XA_END_TIMEOUT_SECONDS = 60;/*60s*/

const char *state_str[] =
{
  "INIT",
  "SLEEP",
  "ACTIVE",
  "QUERY_KILLED",
  "SESSION_KILLED",
};

void ObTenantCachedSchemaGuardInfo::reset()
{
  schema_guard_.reset();
  ref_ts_ = 0;
  tenant_id_ = 0;
  schema_version_ = 0;
}

int ObTenantCachedSchemaGuardInfo::refresh_tenant_schema_guard(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(OBSERVER.get_gctx().schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_schema_version(tenant_id, schema_version_))) {
    LOG_WARN("fail get schema version", K(ret), K(tenant_id));
  } else {
    ref_ts_ = ObClockGenerator::getClock();
    tenant_id_ = tenant_id;
  }

  return ret;
}

void ObTenantCachedSchemaGuardInfo::try_revert_schema_guard()
{
  if (schema_guard_.is_inited()) {
    const int64_t MAX_SCHEMA_GUARD_CACHED_TIME = 10 * 1000 * 1000;
    if (ObClockGenerator::getClock() - ref_ts_ > MAX_SCHEMA_GUARD_CACHED_TIME) {
      LOG_DEBUG("revert schema guard success by sql",
               "session_id", schema_guard_.get_session_id(),
               K_(tenant_id),
               K_(schema_version));
      reset();
    }
  }
}

ObSQLSessionInfo::ObSQLSessionInfo(const uint64_t tenant_id) :
      ObVersionProvider(),
      ObBasicSessionInfo(tenant_id),
      is_inited_(false),
      warnings_buf_(),
      show_warnings_buf_(),
      end_trans_cb_(),
      user_priv_set_(),
      db_priv_set_(),
      curr_trans_start_time_(0),
      curr_trans_last_stmt_time_(0),
      sess_create_time_(0),
      last_refresh_temp_table_time_(0),
      has_temp_table_flag_(false),
      has_accessed_session_level_temp_table_(false),
      enable_early_lock_release_(false),
      is_for_trigger_package_(false),
      trans_type_(transaction::ObTxClass::USER),
      version_provider_(NULL),
      config_provider_(NULL),
      request_manager_(NULL),
      flt_span_mgr_(NULL),
      plan_cache_(NULL),
      ps_cache_(NULL),
      found_rows_(1),
      affected_rows_(-1),
      global_sessid_(0),
      read_uncommited_(false),
      trace_recorder_(NULL),
      inner_flag_(false),
      is_max_availability_mode_(false),
      next_client_ps_stmt_id_(0),
      is_remote_session_(false),
      session_type_(INVALID_TYPE),
      curr_session_context_size_(0),
      pl_context_(NULL),
      pl_can_retry_(true),
#ifdef OB_BUILD_ORACLE_PL
      pl_debugger_(NULL),
#endif
#ifdef OB_BUILD_SPM
      select_plan_type_(ObSpmCacheCtx::INVALID_TYPE),
#endif
      pl_attach_session_id_(0),
      pl_query_sender_(NULL),
      pl_ps_protocol_(false),
      is_ob20_protocol_(false),
      is_session_var_sync_(false),
      pl_sync_pkg_vars_(NULL),
      inner_conn_(NULL),
      encrypt_info_(),
      enable_role_array_(),
      in_definer_named_proc_(false),
      priv_user_id_(OB_INVALID_ID),
      xa_end_timeout_seconds_(transaction::ObXADefault::OB_XA_TIMEOUT_SECONDS),
      xa_last_result_(OB_SUCCESS),
      cached_tenant_config_info_(this),
      prelock_(false),
      proxy_version_(0),
      min_proxy_version_ps_(0),
      is_ignore_stmt_(false),
      ddl_info_(),
      is_table_name_hidden_(false),
      piece_cache_(NULL),
      is_load_data_exec_session_(false),
      pl_exact_err_msg_(),
      is_varparams_sql_prepare_(false),
      got_tenant_conn_res_(false),
      got_user_conn_res_(false),
      conn_res_user_id_(OB_INVALID_ID),
      mem_context_(nullptr),
      has_query_executed_(false),
      is_latest_sess_info_(false),
      cur_exec_ctx_(nullptr),
      restore_auto_commit_(false),
      dblink_context_(this),
      sql_req_level_(0),
      expect_group_id_(OB_INVALID_ID),
      group_id_not_expected_(false),
      gtt_session_scope_unique_id_(0),
      gtt_trans_scope_unique_id_(0),
      vid_(OB_INVALID_ID),
      vport_(0),
      in_bytes_(0),
      out_bytes_(0),
      current_dblink_sequence_id_(0),
      client_non_standard_(false)
{
  MEMSET(tenant_buff_, 0, sizeof(share::ObTenantSpaceFetcher));
  MEMSET(vip_buf_, 0, sizeof(vip_buf_));
}

ObSQLSessionInfo::~ObSQLSessionInfo()
{
  plan_cache_ = NULL;
  destroy(false);
}

int ObSQLSessionInfo::init(uint32_t sessid, uint64_t proxy_sessid,
    common::ObIAllocator *bucket_allocator, const ObTZInfoMap *tz_info, int64_t sess_create_time,
    uint64_t tenant_id)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  static const int64_t PS_BUCKET_NUM = 64;
  if (OB_FAIL(ObBasicSessionInfo::init(sessid, proxy_sessid, bucket_allocator, tz_info))) {
    LOG_WARN("fail to init basic session info", K(ret));
  } else if (FALSE_IT(txn_free_route_ctx_.set_sessid(sessid))) {
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(package_state_map_.create(hash::cal_next_prime(4),
                                               ObMemAttr(orig_tenant_id_, "PackStateMap")))) {
    LOG_WARN("create package state map failed", K(ret));
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(sequence_currval_map_.create(hash::cal_next_prime(32),
                                                  ObMemAttr(orig_tenant_id_, "SequenceMap")))) {
    LOG_WARN("create sequence current value map failed", K(ret));
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(dblink_sequence_id_map_.create(hash::cal_next_prime(32),
                                                  ObMemAttr(orig_tenant_id_, "SequenceIdMap")))) {
    LOG_WARN("create dblink sequence id map failed", K(ret));
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(contexts_map_.create(hash::cal_next_prime(32),
                                          ObMemAttr(orig_tenant_id_, "ContextsMap")))) {
    LOG_WARN("create contexts map failed", K(ret));
  } else {
    curr_session_context_size_ = 0;
    if (is_obproxy_mode()) {
      sess_create_time_ = sess_create_time;
    } else {
      sess_create_time_ = ObTimeUtility::current_time();
    }
    const char *sup_proxy_min_version = "1.8.4";
    min_proxy_version_ps_ = 0;
    if (OB_FAIL(ObClusterVersion::get_version(sup_proxy_min_version, min_proxy_version_ps_))) {
      LOG_WARN("failed to get version", K(ret));
    } else {
      is_inited_ = true;
      refresh_temp_tables_sess_active_time();
    }
  }
  return ret;
}

//for test
int ObSQLSessionInfo::test_init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid,
    common::ObIAllocator *bucket_allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(version);
  if (OB_FAIL(ObBasicSessionInfo::test_init(sessid, proxy_sessid, bucket_allocator))) {
    LOG_WARN("fail to init basic session info", K(ret));
  } else if (FALSE_IT(txn_free_route_ctx_.set_sessid(sessid))) {
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSQLSessionInfo::reset(bool skip_sys_var)
{
  if (is_inited_) {
    // ObVersionProvider::reset();
    reset_all_package_changed_info();
    warnings_buf_.reset();
    show_warnings_buf_.reset();
    end_trans_cb_.reset(),
    audit_record_.reset();
    user_priv_set_ = 0;
    db_priv_set_ = 0;
    curr_trans_start_time_ = 0;
    curr_trans_last_stmt_time_ = 0;
    sess_create_time_ = 0;
    last_refresh_temp_table_time_ = 0;
    has_temp_table_flag_ = false;
    has_accessed_session_level_temp_table_ = false;
    is_for_trigger_package_ = false;
    trans_type_ = transaction::ObTxClass::USER;
    version_provider_ = NULL;
    config_provider_ = NULL;
    request_manager_ = NULL;
    flt_span_mgr_ = NULL;
    MEMSET(tenant_buff_, 0, sizeof(share::ObTenantSpaceFetcher));
    ps_cache_ = NULL;
    found_rows_ = 1;
    affected_rows_ = -1;
    global_sessid_ = 0;
    read_uncommited_ = false;
    trace_recorder_ = NULL;
    inner_flag_ = false;
    is_max_availability_mode_ = false;
    enable_early_lock_release_ = false;
    ps_session_info_map_.reuse();
    ps_name_id_map_.reuse();
    next_client_ps_stmt_id_ = 0;
    is_remote_session_ = false;
    session_type_ = INVALID_TYPE;
    package_state_map_.reuse();
    sequence_currval_map_.reuse();
    dblink_sequence_id_map_.reuse();
    curr_session_context_size_ = 0;
    pl_context_ = NULL;
    pl_can_retry_ = true;
#ifdef OB_BUILD_ORACLE_PL
    pl_debugger_ = NULL;
#endif
    pl_attach_session_id_ = 0;
    pl_query_sender_ = NULL;
    pl_ps_protocol_ = false;
    if (pl_cursor_cache_.is_inited()) {
      // when select GV$OPEN_CURSOR, we will add get_thread_data_lock to fetch pl_cursor_map_
      // so we need get_thread_data_lock there
      ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
      pl_cursor_cache_.reset();
    }
    inner_conn_ = NULL;
    session_stat_.reset();
    pl_sync_pkg_vars_ = NULL;
    //encrypt_info_.reset();
    cached_schema_guard_info_.reset();
    encrypt_info_.reset();
    enable_role_array_.reset();
    in_definer_named_proc_ = false;
    priv_user_id_ = OB_INVALID_ID;
    xa_end_timeout_seconds_ = transaction::ObXADefault::OB_XA_TIMEOUT_SECONDS;
    xa_last_result_ = OB_SUCCESS;
    prelock_ = false;
    proxy_version_ = 0;
    min_proxy_version_ps_ = 0;
    if (OB_NOT_NULL(mem_context_)) {
      destroy_contexts_map(contexts_map_, mem_context_->get_malloc_allocator());
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = NULL;
    }
    contexts_map_.reuse();
    cur_exec_ctx_ = nullptr;
    plan_cache_ = NULL;
    client_app_info_.reset();
    has_query_executed_ = false;
    flt_control_info_.reset();
    is_send_control_info_ = false;
    trace_enable_ = false;
    auto_flush_trace_ = false;
    coninfo_set_by_sess_ = false;
    is_ob20_protocol_ = false;
    is_session_var_sync_ = false;
    is_latest_sess_info_ = false;
    int temp_ret = OB_SUCCESS;
    sql_req_level_ = 0;
    optimizer_tracer_.reset();
    expect_group_id_ = OB_INVALID_ID;
    flt_control_info_.reset();
    group_id_not_expected_ = false;
    //call at last time
    dblink_context_.reset(); // need reset before ObBasicSessionInfo::reset(skip_sys_var);
    ObBasicSessionInfo::reset(skip_sys_var);
    txn_free_route_ctx_.reset();
    client_non_standard_ = false;
  }
  gtt_session_scope_unique_id_ = 0;
  gtt_trans_scope_unique_id_ = 0;
  gtt_session_scope_ids_.reset();
  gtt_trans_scope_ids_.reset();
  vid_ = OB_INVALID_ID;
  vport_ = 0;
  in_bytes_ = 0;
  out_bytes_ = 0;
  MEMSET(vip_buf_, 0, sizeof(vip_buf_));
  current_dblink_sequence_id_ = 0;
  dblink_sequence_schemas_.reset();
}

void ObSQLSessionInfo::clean_status()
{
  reset_all_package_changed_info();
  ObBasicSessionInfo::clean_status();
}

bool ObSQLSessionInfo::is_encrypt_tenant()
{
  bool ret = false;
#ifdef OB_BUILD_TDE_SECURITY
  uint64_t cur_time = ObClockGenerator::getClock();
  int64_t tenant_id = get_effective_tenant_id();
  if (cur_time - encrypt_info_.last_modify_time_ > 10 * 1000 * 1000L) {
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    LOG_WARN("Invalid tenant id to init fast freeze checker", K(tenant_id));
    } else {
      encrypt_info_.last_modify_time_ = cur_time;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        ObString method_str(tenant_config->tde_method.get_value());
        if (ObTdeMethodUtil::is_kms(method_str)) {
          encrypt_info_.is_encrypt_ = true;
          ret = true;
        } else {
          encrypt_info_.is_encrypt_ = false;
          ret = false;
        }
      }
    }
  } else {
    ret = encrypt_info_.is_encrypt_;
  }
#endif

  return ret;
}

int ObSQLSessionInfo::is_force_temp_table_inline(bool &force_inline) const
{
  int ret = OB_SUCCESS;
  int64_t with_subquery_policy = 0;
  force_inline = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    int64_t with_subquery_policy = tenant_config->_with_subquery;
    if (2 == with_subquery_policy) {
      force_inline = true;
    }
  }
  return ret;
}

int ObSQLSessionInfo::is_force_temp_table_materialize(bool &force_materialize) const
{
  int ret = OB_SUCCESS;
  int64_t with_subquery_policy = 0;
  force_materialize = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    int64_t with_subquery_policy = tenant_config->_with_subquery;
    if (1 == with_subquery_policy) {
      force_materialize = true;
    }
  }
  return ret;
}

int ObSQLSessionInfo::is_temp_table_transformation_enabled(bool &transformation_enabled) const
{
  int ret = OB_SUCCESS;
  transformation_enabled = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    transformation_enabled = tenant_config->_xsolapi_generate_with_clause;
  }
  return ret;
}

int ObSQLSessionInfo::is_groupby_placement_transformation_enabled(bool &transformation_enabled) const
{
  int ret = OB_SUCCESS;
  transformation_enabled = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    transformation_enabled = tenant_config->_optimizer_group_by_placement;
  }
  return ret;
}

bool ObSQLSessionInfo::is_in_range_optimization_enabled() const
{
  bool bret = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    bret = tenant_config->_enable_in_range_optimization;
  }
  return bret;
}

int ObSQLSessionInfo::is_better_inlist_enabled(bool &enabled) const
{
  int ret = OB_SUCCESS;
  enabled = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    enabled = tenant_config->_optimizer_better_inlist_costing;
  }
  return ret;
}

bool ObSQLSessionInfo::is_index_skip_scan_enabled() const
{
  bool bret = false;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    bret = tenant_config->_optimizer_skip_scan_enabled;
  }
  return bret;
}

bool ObSQLSessionInfo::is_var_assign_use_das_enabled() const
{
  bool bret = true;
  int64_t tenant_id = get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    bret = tenant_config->_enable_var_assign_use_das;
  }
  return bret;
}

void ObSQLSessionInfo::destroy(bool skip_sys_var)
{
  if (is_inited_) {
    int ret = OB_SUCCESS;
    if (rpc::is_io_thread()) {
      LOG_WARN("free session at IO thread", "sessid", get_sessid(), "proxy_sessid", get_proxy_sessid());
      //事务层需要保持在end trans时，既没有阻塞操作，又没有rpc调用。否则会影响server IO性能或产生死锁
    }

    // 反序列化出来的 session 不应该做 end_trans 等清理工作
    // bug:
    if (false == get_is_deserialized()) {
      if (false == ObSchemaService::g_liboblog_mode_) {
        //session断开时调用ObTransService::end_trans回滚事务，
        // 此处stmt_timeout ＝ 当前时间 ＋语句query超时时间,而不是最后一条sql的start_time, 相关bug_id : 7961445
        set_query_start_time(ObTimeUtility::current_time());
        // 这里调用end_trans无需上锁，因为调用reclaim_value时意味着已经没有query并发使用session
        // 调用这个函数之前会调session.set_session_state(SESSION_KILLED)，
        bool need_disconnect = false;
        if (is_in_transaction() && !is_txn_free_route_temp()) {
          transaction::ObTransID tx_id = get_tx_id();
          MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
          // inner session skip check switch tenant, because the inner connection was shared between tenant
          if (OB_SUCC(guard.switch_to(get_effective_tenant_id(), !is_inner()))) {
            if (OB_FAIL(ObSqlTransControl::rollback_trans(this, need_disconnect))) {
              LOG_WARN("fail to rollback transaction", K(get_sessid()),
                       "proxy_sessid", get_proxy_sessid(), K(ret));
            } else if (false == inner_flag_ && false == is_remote_session_) {
              LOG_INFO("end trans successfully",
                       "sessid", get_sessid(),
                       "proxy_sessid", get_proxy_sessid(),
                       "trans id", tx_id);
            }
          } else {
            LOG_WARN("fail to switch tenant", K(get_effective_tenant_id()), K(ret));
          }
        }
      }
    }

    // 临时表在 slave session 析构时不能清理
    if (false == get_is_deserialized()) {
      int temp_ret = drop_temp_tables();
      if (OB_UNLIKELY(OB_SUCCESS != temp_ret)) {
        LOG_WARN("fail to drop temp tables", K(temp_ret));
      }
      refresh_temp_tables_sess_active_time();
    }

    // slave session 上 ps_session_info_map_ 为空，调用 close 也不会有副作用
    if (OB_SUCC(ret)) {
      if (OB_FAIL(close_all_ps_stmt())) {
        LOG_WARN("failed to close all stmt", K(ret));
      }
    }

    //close all cursor
    if (OB_SUCC(ret) && pl_cursor_cache_.is_inited()) {
      if (OB_FAIL(pl_cursor_cache_.close_all(*this))) {
        LOG_WARN("failed to close all cursor", K(ret));
      }
    }

    if (OB_SUCC(ret) && NULL != piece_cache_) {
      if (OB_FAIL((static_cast<observer::ObPieceCache*>(piece_cache_))
                      ->close_all(*this))) {
        LOG_WARN("failed to close all piece", K(ret));
      }
      get_session_allocator().free(piece_cache_);
      piece_cache_ = NULL;
    }

#ifdef OB_BUILD_ORACLE_PL
    if (OB_SUCC(ret)) {
      const int64_t session_id = get_sessid();
      // utl file should close all fd when user session exits,
      // so we should check session type here to avoid fd closing
      // unexpectedly when inner session exists
      if (is_user_session() && OB_FAIL(ObPLUtlFile::close_all(session_id))) {
        LOG_WARN("failed to close all fd in utl file", K(ret), K(session_id));
      }
    }
#endif

#ifdef OB_BUILD_ORACLE_PL
    // pl debug 功能, pl debug不支持分布式调试，但调用也不会有副作用
    reset_pl_debugger_resource();
#endif
    // 非分布式需要的话，分布式也需要，用于清理package的全局变量值
    reset_all_package_state();
    reset(skip_sys_var);
    is_inited_ = false;
    sql_req_level_ = 0;
  }
}

int ObSQLSessionInfo::close_ps_stmt(ObPsStmtId client_stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *ps_sess_info = NULL;
  if (OB_FAIL(get_ps_session_info(client_stmt_id, ps_sess_info))) {
    LOG_WARN("fail to get ps session info", K(client_stmt_id), "session_id", get_sessid(), K(ret));
  } else if (OB_ISNULL(ps_sess_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ps session info is null", K(client_stmt_id), "session_id", get_sessid(), K(ret));
  } else {
    ObPsStmtId inner_stmt_id = ps_sess_info->get_inner_stmt_id();
    ps_sess_info->dec_ref_count();
    if (ps_sess_info->need_erase()) {
      if (OB_ISNULL(ps_cache_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ps cache is null", K(ret));
      } else if (OB_FAIL(ps_cache_->deref_ps_stmt(inner_stmt_id))) {
        LOG_WARN("close ps stmt failed", K(ret), "session_id", get_sessid(), K(ret));
      }
      //无论上面是否成功, 都需要将session info资源释放
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = remove_ps_session_info(client_stmt_id))) {
        ret = tmp_ret;
        LOG_WARN("remove ps session info failed", K(client_stmt_id),
                  "session_id", get_sessid(), K(ret));
      }
      LOG_TRACE("close ps stmt", K(ret), K(client_stmt_id), K(inner_stmt_id), K(lbt()));
    }
  }
  return ret;
}

int ObSQLSessionInfo::close_all_ps_stmt()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps_cache_)) {
    // do nothing, session no ps
  } else if (!ps_session_info_map_.created()) {
    // do nothing, no ps added to map
  } else {
    PsSessionInfoMap::iterator iter = ps_session_info_map_.begin();
    ObPsStmtId inner_stmt_id = OB_INVALID_ID;
    for (; iter != ps_session_info_map_.end(); ++iter) { //ignore ret
      const ObPsStmtId client_stmt_id = iter->first;
      if (OB_FAIL(get_inner_ps_stmt_id(client_stmt_id, inner_stmt_id))) {
        LOG_WARN("get_inner_ps_stmt_id failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
      } else if (OB_FAIL(ps_cache_->deref_ps_stmt(inner_stmt_id))) {
        LOG_WARN("close ps stmt failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
      } else if (OB_ISNULL(iter->second)) {
        // do nothing
      } else {
        iter->second->~ObPsSessionInfo();
        ps_session_info_allocator_.free(iter->second);
        iter->second = NULL;
      }
    }
    ps_session_info_allocator_.reset();
    ps_session_info_map_.reuse();
  }
  return ret;
}

//用于oracle临时表数据的清理, 在session断开(会话级&事务级)和commit时(事务级)调用
int ObSQLSessionInfo::delete_from_oracle_temp_tables(const obrpc::ObDropTableArg &const_drop_table_arg)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  common::ObCommonSqlProxy *user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = NULL;
  //ObSEArray<const ObSimpleTableSchemaV2 *, 512> table_schemas;
  obrpc::ObDropTableArg &drop_table_arg = const_cast<obrpc::ObDropTableArg &>(const_drop_table_arg);
  const share::schema::ObTableType table_type = drop_table_arg.table_type_;
  const uint64_t tenant_id = drop_table_arg.tenant_id_;
  const ObTableSchema *table_schema = NULL;
  user_sql_proxy = &oracle_sql_proxy;

  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              tenant_id,
              schema_guard))) {
    LOG_WARN("get schema guard failed.", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(oracle_sql_proxy.init(sql_proxy->get_pool()))) {
    LOG_WARN("init oracle sql proxy failed", K(ret));
  } else if (TMP_TABLE_ORA_SESS == table_type || TMP_TABLE_ORA_TRX == table_type) {
    ObIArray<uint64_t> &table_ids = table_type == share::schema::TMP_TABLE_ORA_TRX ?
          get_gtt_trans_scope_ids() : get_gtt_session_scope_ids();
    uint64_t unique_id = table_type == share::schema::TMP_TABLE_ORA_TRX ?
          get_gtt_trans_scope_unique_id() : get_gtt_session_scope_unique_id();
    LOG_DEBUG("delete temp table", K(table_ids), K(unique_id));
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(i), table_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        //table may be dropped, ignore
      } else if (tenant_id != table_schema->get_tenant_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_id not match", K(ret), K(tenant_id), "table_id", table_schema->get_table_id());
      } else if (((TMP_TABLE_ORA_SESS == table_type && table_schema->is_oracle_tmp_table())
                  || (TMP_TABLE_ORA_TRX == table_type && table_schema->is_oracle_trx_tmp_table()))
                  && table_schema->is_normal_schema()) {
        database_schema = NULL;
        if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_tenant_id(),
            table_schema->get_database_id(), database_schema))) {
          LOG_WARN("failed to get database schema", K(ret));
        } else if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", K(ret));
        } else if (database_schema->is_in_recyclebin() || table_schema->is_in_recyclebin()) {
          LOG_DEBUG("skip table schema in recyclebin", K(*table_schema));
        } else {
          const int64_t limit = 1000;
          ret = sql.assign_fmt("DELETE FROM \"%.*s\".\"%.*s\" WHERE %s = %ld AND ROWNUM <= %ld",
                                database_schema->get_database_name_str().length(),
                                database_schema->get_database_name_str().ptr(),
                                table_schema->get_table_name_str().length(),
                                table_schema->get_table_name_str().ptr(),
                                OB_HIDDEN_SESSION_ID_COLUMN_NAME, unique_id,
                                limit);

          if (OB_SUCC(ret)) {
            int64_t affect_rows = 0;
            int64_t last_batch_affect_rows = limit;
            int64_t cur_time = ObTimeUtility::current_time();
            int64_t cur_timeout_backup = THIS_WORKER.get_timeout_ts();
            THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
            while (OB_SUCC(ret) && last_batch_affect_rows > 0) {
              if (OB_FAIL(user_sql_proxy->write(tenant_id, sql.ptr(), last_batch_affect_rows))) {
                LOG_WARN("execute sql failed", K(ret), K(sql));
              } else {
                affect_rows += last_batch_affect_rows;
              }
            }
            if (OB_SUCC(ret)) {
              LOG_DEBUG("succeed to delete rows in oracle temporary table", K(sql), K(affect_rows));
              //delete relation temp table stats.
              if (OB_FAIL(ObOptStatManager::get_instance().delete_table_stat(tenant_id,
                                                      table_schema->get_table_id(), affect_rows))) {
                LOG_WARN("failed to delete table stats", K(ret));
              }
            } else {
              LOG_WARN("failed to delete rows in oracle temporary table", K(ret), K(sql));
            }
            LOG_INFO("delete rows in oracle temporary table", K(sql), K(affect_rows),
                     "clean_time", ObTimeUtility::current_time() - cur_time);
            THIS_WORKER.set_timeout_ts(cur_timeout_backup);
          }
        }
      }
    }

    if (TMP_TABLE_ORA_TRX == table_type && !get_is_deserialized()) {
      gtt_trans_scope_ids_.reuse();
      gen_gtt_trans_scope_unique_id();
      if (gtt_session_scope_ids_.count() == 0) {
        if (OB_FAIL(set_session_temp_table_used(false))) {
          LOG_WARN("fail to set session temp table unused", K(ret));
        }
      }
    }
  }
  return ret;
}
//mysql租户: 如果session创建过临时表, 直连模式: session断开时drop temp table;
//oracle租户, commit时为了清空数据也会调用此接口, 但仅清除事务级别的临时表;
//            session断开时则清理掉事务级和会话级的临时表;
//由于oracle临时表仅仅是清理本session数据, 为避免rs拥塞,不发往rs由sql proxy执行
//对于分布式计划, 除非ac=1否则交给master session清理, 反序列化得到的session不做事情
int ObSQLSessionInfo::drop_temp_tables(const bool is_disconn, const bool is_xa_trans)
{
  int ret = OB_SUCCESS;
  bool ac = false;
  bool is_sess_disconn = is_disconn;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  if (OB_FAIL(get_autocommit(ac))) {
    LOG_WARN("get autocommit error", K(ret), K(ac));
  } else if (!(is_inner() && !is_user_session())
             && (get_has_temp_table_flag()
                 || has_accessed_session_level_temp_table()
                 || has_tx_level_temp_table()
                 || is_xa_trans)
             && (!get_is_deserialized() || ac)) {
    bool need_drop_temp_table = false;
    //mysql: 仅直连 & sess 断开时
    //oracle: commit; 或者 直连时的断session;
    if (!is_oracle_mode()) {
      if (false == is_obproxy_mode() && is_sess_disconn) {
        need_drop_temp_table = true;
      }
    } else {
      if (false == is_sess_disconn || false == is_obproxy_mode()) {
        need_drop_temp_table = true;
        //ac=1, 反序列化session断开时只是任务结束, 并不是真的sess断开, 视作trx commit
        if (is_sess_disconn && get_is_deserialized() && ac) {
          is_sess_disconn = false;
        }
      }
    }
    if (need_drop_temp_table) {
      LOG_DEBUG("need_drop_temp_table",
               K(is_oracle_mode()),
               K(get_current_query_string()),
               K(get_login_tenant_id()),
               K(get_effective_tenant_id()),
               K(lbt()));
      obrpc::ObDDLRes res;
      obrpc::ObDropTableArg drop_table_arg;
      drop_table_arg.if_exist_ = true;
      drop_table_arg.to_recyclebin_ = false;
      if (false == is_sess_disconn) {
        drop_table_arg.table_type_ = share::schema::TMP_TABLE_ORA_TRX;
      } else if (is_oracle_mode()) {
        drop_table_arg.table_type_ = share::schema::TMP_TABLE_ORA_SESS;
      } else {
        drop_table_arg.table_type_ = share::schema::TMP_TABLE;
      }
      drop_table_arg.session_id_ = get_sessid_for_table();
      drop_table_arg.tenant_id_ = get_effective_tenant_id();
      drop_table_arg.exec_tenant_id_ = get_effective_tenant_id();
      common_rpc_proxy = GCTX.rs_rpc_proxy_;
      if (OB_ISNULL(common_rpc_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc proxy is null", K(ret));
      } else if (is_oracle_mode() && OB_FAIL(delete_from_oracle_temp_tables(drop_table_arg))) {
        LOG_WARN("failed to delete from oracle temporary table", K(drop_table_arg), K(ret));
      } else if (!is_oracle_mode() && OB_FALSE_IT(drop_table_arg.compat_mode_ = lib::Worker::CompatMode::MYSQL)) {
      } else if (!is_oracle_mode() && OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg, res))) {
        LOG_WARN("failed to drop temporary table", K(drop_table_arg), K(ret));
      } else {
        LOG_INFO("temporary tables dropped due to connection disconnected", K(is_sess_disconn), K(drop_table_arg));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to drop temp tables", K(ret),
             K(get_effective_tenant_id()), K(get_sessid()),
             K(has_accessed_session_level_temp_table()),
             K(is_xa_trans),
             K(lbt()));
  }
  return ret;
}

//清理oracle临时表中数据来源和当前session id相同, 但属于被重用的旧的session数据
int ObSQLSessionInfo::drop_reused_oracle_temp_tables()
{
  int ret = OB_SUCCESS;
  //obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  if (false == get_is_deserialized()
      && !is_inner()
      && !GCTX.is_standby_cluster()) {
    obrpc::ObDropTableArg drop_table_arg;
    drop_table_arg.if_exist_ = true;
    drop_table_arg.to_recyclebin_ = false;
    drop_table_arg.table_type_ = share::schema::TMP_TABLE_ORA_SESS;
    drop_table_arg.session_id_ = get_sessid_for_table();
    drop_table_arg.tenant_id_ = get_effective_tenant_id();
    drop_table_arg.sess_create_time_ = get_sess_create_time();
    //common_rpc_proxy = GCTX.rs_rpc_proxy_;
    if (OB_FAIL(delete_from_oracle_temp_tables(drop_table_arg))) {
    //if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg))) {
      LOG_WARN("failed to drop reused temporary table", K(drop_table_arg), K(ret));
    } else {
      LOG_DEBUG("succeed to delete old rows for oracle temporary table", K(drop_table_arg));
    }
  }
  return ret;
}

//proxy方式下session创建、断开和后台定时task检查:
//如果距离上次更新此session->last_refresh_temp_table_time_ 超过1hr
//则更新session创建的临时表最后活动时间SESSION_ACTIVE_TIME
//oracle临时表依赖附加的__sess_create_time判断重用并清理, 不需要更新
void ObSQLSessionInfo::refresh_temp_tables_sess_active_time()
{
  int ret = OB_SUCCESS;
  const int64_t REFRESH_INTERVAL = 60L * 60L * 1000L * 1000L; // 1hr
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  if (get_has_temp_table_flag() && is_obproxy_mode()
      && !is_oracle_mode()) {
    int64_t now = ObTimeUtility::current_time();
    obrpc::ObAlterTableRes res;
    if (now - get_last_refresh_temp_table_time() >= REFRESH_INTERVAL) {
      SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
        AlterTableSchema *alter_table_schema = &alter_table_arg.alter_table_schema_;
        alter_table_arg.session_id_ = get_sessid_for_table();
        alter_table_schema->alter_type_ = OB_DDL_ALTER_TABLE;
        common_rpc_proxy = GCTX.rs_rpc_proxy_;
        alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
        alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
        alter_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
        alter_table_arg.compat_mode_ = lib::Worker::CompatMode::MYSQL;
        if (OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ACTIVE_TIME))) {
          LOG_WARN("failed to add member SESSION_ACTIVE_TIME for alter table schema", K(ret));
        } else if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(get_tz_info_wrap()))) {
          LOG_WARN("failed to deep copy tz_info_wrap", K(ret));
        } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
          LOG_WARN("failed to alter temporary table session active time", K(alter_table_arg), K(ret), K(is_obproxy_mode()));
        } else {
          LOG_DEBUG("session active time of temporary tables refreshed", K(ret), "last refresh time", get_last_refresh_temp_table_time());
          set_last_refresh_temp_table_time(now);
        }
      }
    } else {
      LOG_DEBUG("no need to refresh session active time of temporary tables", "last refresh time", get_last_refresh_temp_table_time());
    }
  }
}


ObMySQLRequestManager* ObSQLSessionInfo::get_request_manager()
{
  int ret = OB_SUCCESS;
  if (NULL == request_manager_) {
    MTL_SWITCH(get_effective_tenant_id()) {
      request_manager_ = MTL(obmysql::ObMySQLRequestManager*);
    }
  }

  return request_manager_;
}

ObFLTSpanMgr* ObSQLSessionInfo::get_flt_span_manager()
{
  int ret = OB_SUCCESS;
  if (NULL == flt_span_mgr_) {
    MTL_SWITCH(get_priv_tenant_id()) {
      flt_span_mgr_ = MTL(sql::ObFLTSpanMgr*);
    }
  }
  return flt_span_mgr_;
}

void ObSQLSessionInfo::set_show_warnings_buf(int error_code)
{
  // if error message didn't insert into THREAD warning buffer,
  //    insert it into SESSION warning buffer
  // if no error at all,
  //    clear err.
  if (OB_SUCCESS != error_code && strlen(warnings_buf_.get_err_msg()) <= 0) {
    warnings_buf_.set_error(ob_errpkt_strerror(error_code, lib::is_oracle_mode()), error_code);
    warnings_buf_.reset_warning();
  } else if (OB_SUCCESS == error_code) {
    warnings_buf_.reset_err();
  }
  show_warnings_buf_ = warnings_buf_; // show_warnings_buf_用于show warnings
}

void ObSQLSessionInfo::update_show_warnings_buf()
{
  for (int64_t i = 0; i < warnings_buf_.get_readable_warning_count(); i++) {
    const ObWarningBuffer::WarningItem *item = warnings_buf_.get_warning_item(i);
    if (OB_ISNULL(item)) {
    } else if (item->log_level_ == common::ObLogger::UserMsgLevel::USER_WARN) {
      show_warnings_buf_.append_warning(item->msg_, item->code_);
    } else if (item->log_level_ == common::ObLogger::UserMsgLevel::USER_NOTE) {
      show_warnings_buf_.append_note(item->msg_, item->code_);
    }
  }
}

void ObSQLSessionInfo::get_session_priv_info(share::schema::ObSessionPrivInfo &session_priv) const
{
  session_priv.tenant_id_ = get_priv_tenant_id();
  session_priv.user_id_ = get_user_id();
  session_priv.user_name_ = get_user_name();
  session_priv.host_name_ = get_host_name();
  session_priv.db_ = get_database_name();
  session_priv.user_priv_set_ = user_priv_set_;
  session_priv.db_priv_set_ = db_priv_set_;
  session_priv.enable_role_id_array_.assign(enable_role_array_);
}

ObPlanCache *ObSQLSessionInfo::get_plan_cache()
{
  if (OB_NOT_NULL(plan_cache_)) {
    // do nothing
  } else {
    //release old plancache and get new
    ObPCMemPctConf pc_mem_conf;
    if (OB_SUCCESS != get_pc_mem_conf(pc_mem_conf)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fail to get pc mem conf");
      plan_cache_ = NULL;
    } else {
      plan_cache_ = MTL(ObPlanCache*);
      if (OB_ISNULL(plan_cache_)) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to get plan cache");
      } else if (MTL_ID() != get_effective_tenant_id()) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unmatched tenant_id", K(MTL_ID()), K(get_effective_tenant_id()));
      } else if (plan_cache_->is_inited()) {
        // skip update mem conf
      } else if (OB_SUCCESS != plan_cache_->set_mem_conf(pc_mem_conf)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fail to set plan cache memory conf");
      }
    }
  }
  return plan_cache_;
}

ObPsCache *ObSQLSessionInfo::get_ps_cache()
{
  if (OB_NOT_NULL(ps_cache_)) {
    //do nothing
  } else {
    int ret = OB_SUCCESS;
    const uint64_t tenant_id = get_effective_tenant_id();
    ObPCMemPctConf pc_mem_conf;
    ObMemAttr mem_attr;
    mem_attr.label_ = "PsSessionInfo";
    mem_attr.tenant_id_ = tenant_id;
    mem_attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    if (OB_FAIL(get_pc_mem_conf(pc_mem_conf))) {
      LOG_ERROR("failed to get pc mem conf");
      ps_cache_ = NULL;
    } else {
      ps_cache_ = MTL(ObPsCache*);
      if (OB_ISNULL(ps_cache_)) {
        LOG_WARN("failed to get ps cache");
      } else if (MTL_ID() != get_effective_tenant_id()) {
        LOG_ERROR("unmatched tenant_id", K(MTL_ID()), K(get_effective_tenant_id()));
      } else if (!ps_cache_->is_inited() &&
                  OB_FAIL(ps_cache_->init(common::OB_PLAN_CACHE_BUCKET_NUMBER, tenant_id))) {
        LOG_WARN("failed to init ps cache");
      } else {
        ps_session_info_allocator_.set_attr(mem_attr);
      }
    }
  }
  return ps_cache_;
}


//whether the user has the super privilege
bool ObSQLSessionInfo::has_user_super_privilege() const
{
  int ret = false;
  if (OB_PRIV_HAS_ANY(user_priv_set_, OB_PRIV_SUPER)) {
    ret = true;
  }
  return ret;
}

//whether the user has the process privilege
bool ObSQLSessionInfo::has_user_process_privilege() const
{
  int ret = false;
  if (OB_PRIV_HAS_ANY(user_priv_set_, OB_PRIV_PROCESS)) {
    ret = true;
  }
  return ret;
}

//check tenant read_only
int ObSQLSessionInfo::check_global_read_only_privilege(const bool read_only,
                                                       const ObSqlTraits &sql_traits)
{
  int ret = OB_SUCCESS;
  if (!has_user_super_privilege()
      && !is_tenant_changed()
      && read_only) {
    /** session1                session2
     *  insert into xxx;
     *                          set @@global.read_only = 1;
     *  update xxx (should fail)
     *  create (should fail)
     *  ... (all write stmt should fail)
     */
    if (!sql_traits.is_readonly_stmt_) {
      if (sql_traits.is_modify_tenant_stmt_) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER");
        LOG_WARN("Access denied; you need (at least one of)"
                 "the SUPER privilege(s) for this operation");
      } else {
        ret = OB_ERR_OPTION_PREVENTS_STATEMENT;

        LOG_WARN("the server is running with read_only, cannot execute stmt");
      }
    } else {
      /** session1            session2                    session3
       *  begin                                           begin;
       *  insert into xxx;                                (without write stmt)
       *                      set @@global.read_only = 1;
       *  commit; (should fail)                           commit; (should success)
       */
      if (sql_traits.is_commit_stmt_ && is_in_transaction() && !tx_desc_->is_clean()) {
        ret = OB_ERR_OPTION_PREVENTS_STATEMENT;
        LOG_WARN("the server is running with read_only, cannot execute stmt");
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::remove_prepare(const ObString &ps_name)
{
  int ret = OB_SUCCESS;
  ObPsStmtId ps_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!ps_name_id_map_.created())) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("map not created before insert any element", K(ret));
  } else if (OB_FAIL(ps_name_id_map_.erase_refactored(ps_name, &ps_id))) {
    LOG_WARN("ps session info not exist", K(ps_name));
  } else if (OB_INVALID_ID == ps_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSQLSessionInfo::get_prepare_id(const ObString &ps_name, ObPsStmtId &ps_id) const
{
  int ret = OB_SUCCESS;
  ps_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!ps_name_id_map_.created())) {
    ret = OB_HASH_NOT_EXIST;
  } else if (OB_FAIL(ps_name_id_map_.get_refactored(ps_name, ps_id))) {
    LOG_WARN("get ps session info failed", K(ps_name));
  } else if (OB_INVALID_ID == ps_id) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("ps info is null", K(ret), K(ps_name));
  } else { /*do nothing*/ }

  if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_EER_UNKNOWN_STMT_HANDLER;
  }
  return ret;
}

int ObSQLSessionInfo::add_prepare(const ObString &ps_name, ObPsStmtId ps_id)
{
  int ret = OB_SUCCESS;
  ObString stored_name;
  ObPsStmtId exist_ps_id = OB_INVALID_ID;
  if (OB_FAIL(name_pool_.write_string(ps_name, &stored_name))) {
    LOG_WARN("failed to copy name", K(ps_name), K(ps_id), K(ret));
  } else if (OB_FAIL(try_create_ps_name_id_map())) {
    LOG_WARN("fail create ps name id map", K(ret));
  } else if (OB_FAIL(ps_name_id_map_.get_refactored(stored_name, exist_ps_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(ps_name_id_map_.set_refactored(stored_name, ps_id))) {
        LOG_WARN("fail insert ps id to hash map", K(stored_name), K(ps_id), K(ret));
      }
    } else {
      LOG_WARN("fail to search ps name hash id map", K(stored_name), K(ret));
    }
  } else if (ps_id != exist_ps_id) {
    LOG_DEBUG("exist ps id is diff", K(ps_id), K(exist_ps_id), K(ps_name), K(stored_name));
    if (OB_FAIL(remove_prepare(stored_name))) {
      LOG_WARN("failed to remove prepare", K(stored_name), K(ret));
    } else if (OB_FAIL(remove_ps_session_info(exist_ps_id))) {
      LOG_WARN("failed to remove prepare sesion info", K(exist_ps_id), K(stored_name), K(ret));
    } else if (OB_FAIL(ps_name_id_map_.set_refactored(stored_name, ps_id))) {
      LOG_WARN("fail insert ps id to hash map", K(stored_name), K(ps_id), K(ret));
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_ps_session_info(const ObPsStmtId stmt_id,
                                          ObPsSessionInfo *&ps_session_info) const
{
  int ret = OB_SUCCESS;
  ps_session_info = NULL;
  if (OB_UNLIKELY(!ps_session_info_map_.created())) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("map not created before insert any element", K(ret));
  } else if (OB_FAIL(ps_session_info_map_.get_refactored(stmt_id, ps_session_info))) {
    LOG_WARN("get ps session info failed", K(stmt_id), K(get_sessid()));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_EER_UNKNOWN_STMT_HANDLER;
    }
  } else if (OB_ISNULL(ps_session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps session info is null", K(ret), K(stmt_id));
  }
  return ret;
}

int ObSQLSessionInfo::remove_ps_session_info(const ObPsStmtId stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *session_info = NULL;
  LOG_TRACE("remove ps session info", K(ret), K(stmt_id), K(get_sessid()), K(lbt()));
  if (OB_UNLIKELY(!ps_session_info_map_.created())) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("map not created before insert any element", K(ret));
  } else if (OB_FAIL(ps_session_info_map_.erase_refactored(stmt_id, &session_info))) {
    LOG_WARN("ps session info not exist", K(stmt_id));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else {
    LOG_TRACE("remove ps session info", K(ret), K(stmt_id), K(get_sessid()));
    session_info->~ObPsSessionInfo();
    ps_session_info_allocator_.free(session_info);
    session_info = NULL;
  }
  return ret;
}

int ObSQLSessionInfo::prepare_ps_stmt(const ObPsStmtId inner_stmt_id,
                                      const ObPsStmtInfo *stmt_info,
                                      ObPsStmtId &client_stmt_id,
                                      bool &already_exists,
                                      bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *session_info = NULL;
  // 相同sql返回不同stmt id:
  // 1. 有proxy且版本大于等于1.8.4以上 or 直连情况
  // 3. 非内部sql
  const bool is_new_proxy = ((is_obproxy_mode() && proxy_version_ >= min_proxy_version_ps_)
                                      || !is_obproxy_mode());
  if (is_new_proxy && !is_inner_sql) {
    client_stmt_id = ++next_client_ps_stmt_id_;
  } else {
    client_stmt_id = inner_stmt_id;
  }
  already_exists = false;
  if (is_inner_sql) {
    LOG_TRACE("is inner sql no need to add session info",
              K(proxy_version_), K(min_proxy_version_ps_), K(inner_stmt_id),
              K(client_stmt_id), K(next_client_ps_stmt_id_), K(is_new_proxy), K(is_inner_sql));
  } else {
    LOG_TRACE("will add session info", K(proxy_version_), K(min_proxy_version_ps_),
              K(inner_stmt_id), K(client_stmt_id), K(next_client_ps_stmt_id_),
              K(is_new_proxy), K(ret), K(is_inner_sql));

    if (OB_FAIL(try_create_ps_session_info_map())) {
      LOG_WARN("fail create map", K(ret));
    } else {
      ret = ps_session_info_map_.get_refactored(client_stmt_id, session_info);
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(session_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session_info is NULL", K(ret), K(inner_stmt_id), K(client_stmt_id));
      } else {
        already_exists = true;
        session_info->inc_ref_count();
      }
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      char *buf = static_cast<char*>(ps_session_info_allocator_.alloc(sizeof(ObPsSessionInfo)));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_ISNULL(stmt_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt info is null", K(ret), K(stmt_info));
      } else {
        session_info = new (buf) ObPsSessionInfo(orig_tenant_id_, stmt_info->get_num_of_param());
        session_info->set_stmt_id(client_stmt_id);
        session_info->set_stmt_type(stmt_info->get_stmt_type());
        session_info->set_ps_stmt_checksum(stmt_info->get_ps_stmt_checksum());
        session_info->set_inner_stmt_id(inner_stmt_id);
        session_info->set_num_of_returning_into(stmt_info->get_num_of_returning_into());
        LOG_TRACE("add ps session info", K(stmt_info->get_ps_sql()),
                                        K(stmt_info->get_ps_stmt_checksum()),
                                        K(client_stmt_id),
                                        K(inner_stmt_id),
                                        K(get_sessid()),
                                        K(stmt_info->get_num_of_param()),
                                        K(stmt_info->get_num_of_returning_into()));
      }

      if (OB_SUCC(ret)) {
        session_info->inc_ref_count();
        if (OB_FAIL(ps_session_info_map_.set_refactored(client_stmt_id, session_info))) {
          // OB_HASH_EXIST cannot be here, no need to handle
          LOG_WARN("push back ps_session info failed", K(ret), K(client_stmt_id));
        } else {
          LOG_TRACE("add ps session info success", K(client_stmt_id), K(get_sessid()));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(session_info)) {
        session_info->~ObPsSessionInfo();
        ps_session_info_allocator_.free(session_info);
        session_info = NULL;
        buf = NULL;
      }
    } else {
      LOG_WARN("get ps session failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_inner_ps_stmt_id(ObPsStmtId cli_stmt_id, ObPsStmtId &inner_stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *ps_session_info = NULL;
  if (OB_UNLIKELY(!ps_session_info_map_.created())) {
    ret = OB_HASH_NOT_EXIST;
    LOG_WARN("map not created before insert any element", K(ret));
  } else if (OB_FAIL(ps_session_info_map_.get_refactored(cli_stmt_id, ps_session_info))) {
    LOG_WARN("get inner ps stmt id failed", K(ret), K(cli_stmt_id), K(lbt()));
  } else if (OB_ISNULL(ps_session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps session info is null", K(cli_stmt_id), "session_id", get_sessid(), K(ret));
  } else {
    inner_stmt_id = ps_session_info->get_inner_stmt_id();
  }
  return ret;
}

ObPLCursorInfo *ObSQLSessionInfo::get_cursor(int64_t cursor_id)
{
  ObPLCursorInfo *cursor = NULL;
  if (OB_SUCCESS != pl_cursor_cache_.pl_cursor_map_.get_refactored(cursor_id, cursor)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get cursor info failed", K(cursor_id), K(get_sessid()));
  }
  return cursor;
}

ObDbmsCursorInfo *ObSQLSessionInfo::get_dbms_cursor(int64_t cursor_id)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = NULL;
  ObDbmsCursorInfo *dbms_cursor = NULL;
  OV (OB_NOT_NULL(cursor = get_cursor(cursor_id)),
      OB_INVALID_ARGUMENT, cursor_id);
  OV (OB_NOT_NULL(dbms_cursor = dynamic_cast<ObDbmsCursorInfo *>(cursor)),
      OB_INVALID_ARGUMENT, cursor_id);
  return dbms_cursor;
}

int ObSQLSessionInfo::add_cursor(pl::ObPLCursorInfo *cursor)
{
// open_cursors is 0 to indicate a special state, no limit is set
#define NEED_CHECK_SESS_OPEN_CURSORS_LIMIT(v) (0 == v ? false : true)
  int ret = OB_SUCCESS;
  bool add_cursor_success = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(get_effective_tenant_id()));
  CK (tenant_config.is_valid());
  CK (OB_NOT_NULL(cursor));
  if (OB_SUCC(ret)) {
    int64_t open_cursors_limit = tenant_config->open_cursors;
    if (NEED_CHECK_SESS_OPEN_CURSORS_LIMIT(open_cursors_limit)
        && open_cursors_limit <= pl_cursor_cache_.pl_cursor_map_.size()) {
      ret = OB_ERR_OPEN_CURSORS_EXCEEDED;
      LOG_WARN("maximum open cursors exceeded",
                K(ret), K(open_cursors_limit), K(pl_cursor_cache_.pl_cursor_map_.size()));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t id = cursor->get_id();
    if (OB_INVALID_ID == id) {
      // mysql ps模式时，会提前将cursor id设置为 stmt_id
      id = pl_cursor_cache_.gen_cursor_id();
      // ps cursor: proxy will record server ip, other ops of ps cursor will route by record ip.

      // server cursor(not ps cursor) has same logic with temporary table,
      // because we can not sync server cursor status to other server,
      // so after open server cursor, other modify of this cursor need to do on same server.
      bool is_already_set = false;
      OZ (get_session_temp_table_used(is_already_set));
      if (OB_SUCC(ret) && !is_already_set) {
        OZ (set_session_temp_table_used(true));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // when select GV$OPEN_CURSOR, we will add get_thread_data_lock to fetch pl_cursor_map_
      // so we need get_thread_data_lock there
      ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
      if (OB_FAIL(pl_cursor_cache_.pl_cursor_map_.set_refactored(id, cursor))) {
        LOG_WARN("fail insert ps id to hash map", K(id), K(*cursor), K(ret));
      } else {
        cursor->set_id(id);
        add_cursor_success = true;
        if (lib::is_diagnose_info_enabled()) {
          EVENT_INC(SQL_OPEN_CURSORS_CURRENT);
          EVENT_INC(SQL_OPEN_CURSORS_CUMULATIVE);
        }
        LOG_DEBUG("ps cursor: add cursor", K(ret), K(id), K(get_sessid()));
      }
    }
  }
  if (!add_cursor_success && OB_NOT_NULL(cursor)) {
    int64_t id = cursor->get_id();
    int tmp_ret = close_cursor(cursor);
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("close cursor fail when add cursor to sesssion.", K(ret), K(id), K(get_sessid()));
    }
  }
  return ret;
}

int ObSQLSessionInfo::close_cursor(ObPLCursorInfo *&cursor)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cursor)) {
    int64_t id = cursor->get_id();
    OZ (cursor->close(*this));
    cursor->~ObPLCursorInfo();
    get_cursor_allocator().free(cursor);
    cursor = NULL;
    LOG_DEBUG("close cursor", K(ret), K(id), K(get_sessid()));
  } else {
    LOG_DEBUG("close cursor is null", K(get_sessid()));
  }
  return ret;
}

int ObSQLSessionInfo::close_cursor(int64_t cursor_id)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = NULL;
  LOG_INFO("ps cursor : remove cursor", K(ret), K(cursor_id), K(get_sessid()));
  // when select GV$OPEN_CURSOR, we will add get_thread_data_lock to fetch pl_cursor_map_
  // so we need get_thread_data_lock there
  ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
  if (OB_FAIL(pl_cursor_cache_.pl_cursor_map_.erase_refactored(cursor_id, &cursor))) {
    LOG_WARN("cursor info not exist", K(cursor_id));
  } else if (OB_ISNULL(cursor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else {
    LOG_DEBUG("close cursor", K(ret), K(cursor_id), K(get_sessid()));
    OZ (cursor->close(*this));
    cursor->~ObPLCursorInfo();
    get_cursor_allocator().free(cursor);
    cursor = NULL;
    if (lib::is_diagnose_info_enabled()) {
      EVENT_DEC(SQL_OPEN_CURSORS_CURRENT);
    }
  }
  return ret;
}

int ObSQLSessionInfo::print_all_cursor()
{
  int ret = OB_SUCCESS;
  int64_t open_cnt = 0;
  int64_t unexpected_cnt = 0;
  LOG_DEBUG("CURSOR DEBUG: total cursors in cursor map: ",
            K(pl_cursor_cache_.pl_cursor_map_.size()));
  for (CursorCache::CursorMap::iterator iter = pl_cursor_cache_.pl_cursor_map_.begin();  //ignore ret
      iter != pl_cursor_cache_.pl_cursor_map_.end();  ++iter) {
    pl::ObPLCursorInfo *cursor_info = iter->second;
    if (OB_ISNULL(cursor_info)) {
      // do nothing;
    } else {
      if (cursor_info->isopen()) {
        open_cnt++;
        LOG_DEBUG("CURSOR DEBUG: found open cursor", K(*cursor_info),
                                                    K(cursor_info->get_ref_count()));
      } else {
        if (0 != cursor_info->get_ref_count()) {
          unexpected_cnt++;
          LOG_DEBUG("CURSOR DEBUG: found closed cursor", K(*cursor_info),
                                                      K(cursor_info->get_ref_count()));
        }
      }
    }
  }
  LOG_DEBUG("CURSOR DEBUG: may illegal cursors in cursor map: ",  K(open_cnt), K(unexpected_cnt));
  return ret;
}

int ObSQLSessionInfo::init_cursor_cache()
{
  int ret = OB_SUCCESS;
  if (!pl_cursor_cache_.is_inited()) {
    // when select GV$OPEN_CURSOR, we will add get_thread_data_lock to fetch pl_cursor_map_
    // so we need get_thread_data_lock there
    ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
    OZ (pl_cursor_cache_.init(get_effective_tenant_id()),
                              get_effective_tenant_id(),
                              get_proxy_sessid(),
                              get_sessid());
  }
  return ret;
}

int ObSQLSessionInfo::close_dbms_cursor(int64_t cursor_id)
{
  int ret = OB_SUCCESS;
  ObPLCursorInfo *cursor = NULL;
  LOG_INFO("remove dbms cursor", K(ret), K(cursor_id), K(get_sessid()));
  // when select GV$OPEN_CURSOR, we will add get_thread_data_lock to fetch pl_cursor_map_
  // so we need get_thread_data_lock there
  ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
  OZ (pl_cursor_cache_.pl_cursor_map_.erase_refactored(cursor_id, &cursor), cursor_id);
  OV (OB_NOT_NULL(cursor), OB_ERR_UNEXPECTED, cursor_id);
  if (OB_SUCC(ret) && lib::is_diagnose_info_enabled()) {
    EVENT_DEC(SQL_OPEN_CURSORS_CURRENT);
  }
  // dbms cursor应该先执行spi接口关闭，再执行session接口删除。
  OV (!cursor->isopen(), OB_ERR_UNEXPECTED, cursor_id);
  OX (cursor->reset());
  OX (get_cursor_allocator().free(cursor));
  OX (cursor = NULL);
  return ret;
}

int ObSQLSessionInfo::make_cursor(pl::ObPLCursorInfo *&cursor)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(cursor);
#else
  const pl::ObRefCursorType pl_type;
  ObObj param;
  param.set_ext(reinterpret_cast<int64_t>(cursor));
  int64_t param_size = 0;
  ObSchemaGetterGuard dummy_schema_guard;
  OZ (init_cursor_cache());
  OZ (pl_type.init_obj(dummy_schema_guard, get_cursor_allocator(), param, param_size));
  OX (cursor = reinterpret_cast<ObPLCursorInfo*>(param.get_ext()));
  OZ (add_cursor(cursor));
  LOG_DEBUG("cursor alloc, session cursor", K(cursor));
#endif
  return ret;
}

int ObSQLSessionInfo::make_dbms_cursor(pl::ObDbmsCursorInfo *&cursor,
                                       uint64_t id)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (!pl_cursor_cache_.is_inited()) {
    OZ (pl_cursor_cache_.init(get_effective_tenant_id()),
        get_effective_tenant_id(), get_proxy_sessid(), get_sessid());
  }
  OV (OB_NOT_NULL(buf = get_cursor_allocator().alloc(sizeof(ObDbmsCursorInfo))),
      OB_ALLOCATE_MEMORY_FAILED, sizeof(ObDbmsCursorInfo));
  OX (MEMSET(buf, 0, sizeof(ObDbmsCursorInfo)));
  OV (OB_NOT_NULL(cursor = new (buf) ObDbmsCursorInfo(get_cursor_allocator())));
  OZ (cursor->init());
  OX (cursor->set_id(id));
  OZ (add_cursor(cursor));
  /*
   * 一个dbms cursor可以在open之后反复parse，每次都可以切换为不同语句，所以内部不同对象需要使用不同allocator：
   * 1. cursor_id在open_cursor之后就不变化，直到close_cursor，所以生命周期比较长，从session的allocator分配。
   * 2. sql_stmt_等其它属性都是每次parse之后就变化的，所以生命周期较短，从entiry分配内存，并且在每次parse时重新
   *    创建entity。
   * 3. spi_result、spi_cursor也要在每次parse时重置。
   */
  return ret;
}

int ObSQLSessionInfo::check_read_only_privilege(const bool read_only,
                                                const ObSqlTraits &sql_traits)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_global_read_only_privilege(read_only, sql_traits))) {
    LOG_WARN("failed to check global read_only privilege!", K(ret));
  } else if (OB_FAIL(check_tx_read_only_privilege(sql_traits))){
    LOG_WARN("failed to check tx_read_only privilege!", K(ret));
  }
  return ret;
}

//在session中当trace被打开过一次，则分配一个buffer, 该buffer在整个session析构时才会被释放。
ObTraceEventRecorder* ObSQLSessionInfo::get_trace_buf()
{
  if (NULL == trace_recorder_) {
    void *ptr = name_pool_.alloc(sizeof(ObTraceEventRecorder));
    if (NULL == ptr) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc trace recorder");
    } else {
      trace_recorder_ = new (ptr) ObTraceEventRecorder(false, ObLatchIds::SESSION_TRACE_RECORDER_LOCK);
    }
  }
  return trace_recorder_;
}

void ObSQLSessionInfo::clear_trace_buf()
{
  if (NULL != trace_recorder_) {
    trace_recorder_->reset();
  }
}

OB_DEF_SERIALIZE(ObSQLSessionInfo::ApplicationInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, module_name_, action_name_, client_info_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSQLSessionInfo::ApplicationInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, module_name_, action_name_, client_info_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSQLSessionInfo::ApplicationInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, module_name_, action_name_, client_info_);
  return len;
}

OB_DEF_SERIALIZE(ObSQLSessionInfo)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_ENCODE,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSQLSessionInfo)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_DECODE,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_);
  (void)ObSQLUtils::adjust_time_by_ntp_offset(thread_data_.cur_query_start_time_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSQLSessionInfo)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObSQLSessionInfo, ObBasicSessionInfo));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      thread_data_.cur_query_start_time_,
      user_priv_set_,
      db_priv_set_,
      trans_type_,
      global_sessid_,
      inner_flag_,
      is_max_availability_mode_,
      session_type_,
      has_temp_table_flag_,
      enable_early_lock_release_,
      enable_role_array_,
      in_definer_named_proc_,
      priv_user_id_,
      xa_end_timeout_seconds_,
      prelock_,
      proxy_version_,
      min_proxy_version_ps_,
      thread_data_.is_in_retry_,
      ddl_info_,
      gtt_session_scope_unique_id_,
      gtt_trans_scope_unique_id_,
      gtt_session_scope_ids_,
      gtt_trans_scope_ids_);
  return len;
}

int ObSQLSessionInfo::get_collation_type_of_names(
    const ObNameTypeClass type_class,
    ObCollationType &cs_type) const
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  cs_type = CS_TYPE_INVALID;
  if (OB_TABLE_NAME_CLASS == type_class) {
    if (OB_FAIL(get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
      cs_type = CS_TYPE_UTF8MB4_BIN;
    } else if (OB_ORIGIN_AND_INSENSITIVE == case_mode || OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    }
  } else if (OB_COLUMN_NAME_CLASS == type_class) {
    cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (OB_USER_NAME_CLASS == type_class) {
    cs_type = CS_TYPE_UTF8MB4_BIN;
  }
  return ret;
}

int ObSQLSessionInfo::name_case_cmp(
    const ObString &name,
    const ObString &name_other,
    const ObNameTypeClass type_class,
    bool &is_equal) const
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(get_collation_type_of_names(type_class, cs_type))) {
    LOG_WARN("fail to get collation type of name", K(name), K(name_other), K(type_class), K(ret));
  } else {
    if (0 == ObCharset::strcmp(cs_type, name, name_other)) {
      is_equal = true;
    } else {
      is_equal = false;
    }
  }
  return ret;
}

int ObSQLSessionInfo::kill_query()
{
  LOG_INFO("kill query", K(get_sessid()), K(get_proxy_sessid()), K(get_current_query_string()));
  ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
  update_last_active_time();
  set_session_state(QUERY_KILLED);
  return OB_SUCCESS;
}

int ObSQLSessionInfo::set_query_deadlocked()
{
  LOG_INFO("set query deadlocked", K(get_sessid()), K(get_proxy_sessid()), K(get_current_query_string()));
  ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
  update_last_active_time();
  set_session_state(QUERY_DEADLOCKED);
  return OB_SUCCESS;
}

const ObAuditRecordData &ObSQLSessionInfo::get_final_audit_record(
                                           ObExecuteMode mode)
{
  int ret = OB_SUCCESS;
  audit_record_.trace_id_ = *ObCurTraceId::get_trace_id();
  audit_record_.request_type_ = mode;
  audit_record_.session_id_ = get_sessid();
  audit_record_.proxy_session_id_ = get_proxy_sessid();
  audit_record_.tenant_id_ = get_priv_tenant_id();
  audit_record_.user_id_ = get_user_id();
  audit_record_.effective_tenant_id_ = get_effective_tenant_id();
  if (EXECUTE_INNER == mode
      || EXECUTE_LOCAL == mode
      || EXECUTE_PS_PREPARE == mode
      || EXECUTE_PS_EXECUTE == mode
      || EXECUTE_PS_SEND_PIECE == mode
      || EXECUTE_PS_GET_PIECE == mode
      || EXECUTE_PS_SEND_LONG_DATA == mode
      || EXECUTE_PS_FETCH == mode
      || EXECUTE_PL_EXECUTE == mode) {
    audit_record_.tenant_name_ = const_cast<char *>(get_tenant_name().ptr());
    audit_record_.tenant_name_len_ = min(get_tenant_name().length(),
                                         OB_MAX_TENANT_NAME_LENGTH);
    audit_record_.user_name_ = const_cast<char *>(get_user_name().ptr());
    audit_record_.user_name_len_ = min(get_user_name().length(),
                                       OB_MAX_USER_NAME_LENGTH);
    audit_record_.db_name_ = const_cast<char *>(get_database_name().ptr());
    audit_record_.db_name_len_ = min(get_database_name().length(),
                                     OB_MAX_DATABASE_NAME_LENGTH);

    if (EXECUTE_PS_EXECUTE == mode
        || EXECUTE_PS_SEND_PIECE == mode
        || EXECUTE_PS_GET_PIECE == mode
        || EXECUTE_PS_SEND_LONG_DATA == mode
        || EXECUTE_PS_FETCH == mode) {
      //ps模式对应的sql在协议层中设置, session的current_query_中没值
      // do nothing
    } else {
      ObString sql = get_current_query_string();
      audit_record_.sql_ = const_cast<char *>(sql.ptr());
      audit_record_.sql_len_ = min(sql.length(), OB_MAX_SQL_LENGTH);
      audit_record_.sql_cs_type_ = get_local_collation_connection();
    }

    if (OB_FAIL(get_database_id(audit_record_.db_id_))) {
      LOG_WARN("fail to get database id", K(ret));
    }
  } else if (EXECUTE_REMOTE == mode || EXECUTE_DIST == mode) {
    audit_record_.tenant_name_ = NULL;
    audit_record_.tenant_name_len_ = 0;
    audit_record_.user_name_ = NULL;
    audit_record_.user_name_len_ = 0;
    audit_record_.db_name_ = NULL;
    audit_record_.db_name_len_ = 0;
    audit_record_.sql_ = NULL;
    audit_record_.sql_len_ = 0;
    audit_record_.sql_cs_type_ = CS_TYPE_INVALID;
  }

  audit_record_.txn_free_route_flag_ = txn_free_route_ctx_.get_audit_record();
  audit_record_.txn_free_route_version_ = txn_free_route_ctx_.get_global_version();
  trace::UUID trc_uuid = OBTRACE->get_trace_id();
  int64_t pos = 0;
  if (trc_uuid.is_inited()) {
    trc_uuid.tostring(audit_record_.flt_trace_id_, OB_MAX_UUID_STR_LENGTH + 1, pos);
  } else {
    // do nothing
  }
  audit_record_.flt_trace_id_[pos] = '\0';
  return audit_record_;
}

void ObSQLSessionInfo::update_stat_from_exec_record()
{
  session_stat_.total_logical_read_ += (audit_record_.exec_record_.memstore_read_row_count_
                                        + audit_record_.exec_record_.ssstore_read_row_count_);
//  session_stat_.total_logical_write_ += 0;
//  session_stat_.total_physical_read_ += 0;
//  session_stat_.total_lock_count_ += 0;
}

void ObSQLSessionInfo::update_stat_from_exec_timestamp()
{
  session_stat_.total_cpu_time_us_ += audit_record_.exec_timestamp_.executor_t_;
  session_stat_.total_exec_time_us_ += audit_record_.exec_timestamp_.elapsed_t_;
}

void ObSQLSessionInfo::update_alive_time_stat()
{
  session_stat_.total_alive_time_us_ = ObTimeUtility::current_time() - sess_create_time_;;
}

void ObSQLSessionInfo::set_session_type_with_flag()
{
  if (OB_UNLIKELY(INVALID_TYPE == session_type_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "session type is not init, only happen when old server send rpc to new server");
    session_type_ = inner_flag_ ? INNER_SESSION : USER_SESSION;
  }
}

void ObSQLSessionInfo::set_early_lock_release(bool enable)
{
  enable_early_lock_release_ = enable;
  if (enable) {
    SQL_SESSION_LOG(DEBUG, "set early lock release success",
        "sessid", get_sessid(), "proxy_sessid", get_proxy_sessid());
  }
}

ObPLCursorInfo *ObSQLSessionInfo::get_pl_implicit_cursor()
{
  return NULL != pl_context_ ? &(pl_context_->get_cursor_info()) : NULL;
}
const ObPLCursorInfo *ObSQLSessionInfo::get_pl_implicit_cursor() const
{
  return NULL != pl_context_ ? &(pl_context_->get_cursor_info()) : NULL;
}

ObPLSqlCodeInfo *ObSQLSessionInfo::get_pl_sqlcode_info()
{
  return NULL != pl_context_ ? &(pl_context_->get_sqlcode_info()) : NULL;
}
const ObPLSqlCodeInfo *ObSQLSessionInfo::get_pl_sqlcode_info() const
{
  return NULL != pl_context_ ? &(pl_context_->get_sqlcode_info()) : NULL;
}

bool ObSQLSessionInfo::has_pl_implicit_savepoint()
{
  return NULL != pl_context_ ? pl_context_->has_implicit_savepoint() : false;
}

void ObSQLSessionInfo::clear_pl_implicit_savepoint()
{
  if (OB_NOT_NULL(pl_context_)) {
    pl_context_->clear_implicit_savepoint();
  }
}

void ObSQLSessionInfo::set_has_pl_implicit_savepoint(bool v)
{
  if (OB_NOT_NULL(pl_context_)) {
    pl_context_->set_has_implicit_savepoint(v);
  }
}

bool ObSQLSessionInfo::is_pl_debug_on()
{
  bool is_on = false;
#ifdef OB_BUILD_ORACLE_PL
  is_on = pl_debugger_ != NULL && pl_debugger_->is_debug_on();
#endif
  return is_on;
}


#ifdef OB_BUILD_ORACLE_PL
int ObSQLSessionInfo::initialize_pl_debugger()
{
  int ret = OB_SUCCESS;
  ObPDBManager *instance = ObPDBManager::get_instance();
  if (OB_NOT_NULL(instance)) {
    OZ (instance->alloc(pl_debugger_, this));
    CK (OB_NOT_NULL(pl_debugger_));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current observer has not debugger instance!", K(ret));
  }
  return ret;
}

int ObSQLSessionInfo::get_pl_debugger(
  uint32_t id, pl::debugger::ObPLDebugger *& pl_debugger)
{
  int ret = OB_SUCCESS;
  ObPDBManager *instance = ObPDBManager::get_instance();
  if (OB_NOT_NULL(instance)) {
    OZ (instance->get(id, pl_debugger));
    CK (OB_NOT_NULL(pl_debugger));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current observer has not debugger instance!", K(ret));
  }
  return ret;
}

int ObSQLSessionInfo::release_pl_debugger(pl::debugger::ObPLDebugger *pl_debugger)
{
  int ret = OB_SUCCESS;
  ObPDBManager *instance = ObPDBManager::get_instance();
  if (OB_ISNULL(pl_debugger)) {
  } else if (OB_NOT_NULL(instance)) {
    instance->release(pl_debugger);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current observer has not debugger instance!", K(ret));
  }
  return ret;
}

int ObSQLSessionInfo::free_pl_debugger()
{
  int ret = OB_SUCCESS;
  ObPDBManager *instance = ObPDBManager::get_instance();
  if (OB_ISNULL(pl_debugger_)) {
  } else if (OB_NOT_NULL(instance)) {
    instance->free(pl_debugger_);
    pl_debugger_ = NULL;
    LOG_INFO("free current session debugger", K(ret));
  } else {
    LOG_ERROR("current observer has not debugger instance!");
  }
  return ret;
}

void ObSQLSessionInfo::reset_pl_debugger_resource()
{
  free_pl_debugger();
}
#endif

void ObSQLSessionInfo::reset_all_package_changed_info()
{
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      it->second->reset_package_changed_info();
    }
  }
}

void ObSQLSessionInfo::reset_all_package_state()
{
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      it->second->reset(this);
      it->second->~ObPLPackageState();
      get_package_allocator().free(it->second);
      it->second = NULL;
    }
    package_state_map_.clear();
  }
}

int ObSQLSessionInfo::reset_all_package_state_by_dbms_session(bool need_set_sync_var)
{
  /* its called by dbms_session.reset_package()
   * in this mode
   *  1. we also should reset all user variable mocked by package var
   *  2. if the package is a trigger, we should do nothing
   *
   */
  int ret = OB_SUCCESS;
  if (0 == package_state_map_.size()
      || NULL != get_pl_context()
      || false == need_reset_package()) {
    // do nothing
  } else {
    ObSEArray<int64_t, 4> remove_packages;
    if (0 != package_state_map_.size()) {
      FOREACH(it, package_state_map_) {
        if (!share::schema::ObTriggerInfo::is_trigger_package_id(it->second->get_package_id())) {
          ret = ret != OB_SUCCESS ? ret : remove_packages.push_back(it->first);
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < remove_packages.count(); ++i) {
      ObPLPackageState *package_state = NULL;
      bool need_reset = false;
      OZ (package_state_map_.get_refactored(remove_packages.at(i), package_state));
      CK (OB_NOT_NULL(package_state));
      OZ (package_state_map_.erase_refactored(remove_packages.at(i)));
      OX (need_reset = true);
      OZ (package_state->remove_user_variables_for_package_state(*this));
      if (need_reset && NULL != package_state) {
        package_state->reset(this);
        package_state->~ObPLPackageState();
        get_package_allocator().free(package_state);
      }
    }
    if (OB_SUCC(ret) && need_set_sync_var) {
      ObSessionVariable sess_var;
      ObString key("##__OB_RESET_ALL_PACKAGE_BY_DBMS_SESSION_RESET_PACKAGE__");
      sess_var.meta_.set_timestamp();
      sess_var.value_.set_timestamp(ObTimeUtility::current_time());
      if (OB_FAIL(ObBasicSessionInfo::replace_user_variable(key, sess_var))) {
        LOG_WARN("add user var failed", K(ret));
      }
    }
    // wether reset succ or not, set need_reset_package to false
    set_need_reset_package(false);
  }
  return ret;
}

int ObSQLSessionInfo::reset_all_serially_package_state()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> serially_packages;
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      if (it->second->get_serially_reusable()) {
        it->second->reset(this);
        OZ (serially_packages.push_back(it->first));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < serially_packages.count(); ++i) {
    OZ (package_state_map_.erase_refactored(serially_packages.at(i)));
  }
  return ret;
}

bool ObSQLSessionInfo::is_package_state_changed() const
{
  bool b_ret = false;
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      if (it->second->is_package_info_changed()) {
        b_ret = true;
        break;
      }
    }
  }
  return b_ret;
}

bool ObSQLSessionInfo::get_changed_package_state_num() const
{
  int64_t changed_num = 0;
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      if (it->second->is_package_info_changed()) {
        changed_num++;
      }
    }
  }
  return changed_num;
}

int ObSQLSessionInfo::add_changed_package_info(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObPLExecCtx pl_ctx(&allocator, &exec_ctx, NULL, NULL, NULL, NULL);
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      ObPLPackageState *package_state = it->second;
      if (package_state->is_package_info_changed()) {
        ObSEArray<ObString, 4> key;
        ObSEArray<ObObj, 4> value;
        bool is_valid = false;
        if (OB_FAIL(package_state->check_package_state_valid(exec_ctx, is_valid))) {
          LOG_WARN("check package state failed", K(ret), KPC(package_state));
        } else if (!is_valid) {
          LOG_INFO("package state is invalid, ignore this package.", KPC(package_state));
        } else if (OB_FAIL(package_state->convert_changed_info_to_string_kvs(pl_ctx, key, value))) {
          LOG_WARN("convert package state to string kv failed", K(ret));
        } else {
          ObSessionVariable sess_var;
          int tmp_ret = OB_SUCCESS;
          for (int64_t i = 0; OB_SUCC(ret) && i < key.count(); i++) {
            sess_var.value_ = value[i];
            sess_var.meta_ = value[i].get_meta();
            if (OB_FAIL(ObBasicSessionInfo::replace_user_variable(key[i], sess_var))) {
              LOG_WARN("add user var failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::shrink_package_info()
{
  int ret = OB_SUCCESS;
  if (0 != package_state_map_.size()) {
    FOREACH(it, package_state_map_) {
      ObPLPackageState *package_state = it->second;
      if (OB_FAIL(package_state->shrink())) {
        LOG_WARN("package shrink failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::replace_user_variable(
  const common::ObString &name, const ObSessionVariable &value)
{
  return ObBasicSessionInfo::replace_user_variable(name, value);
}

int ObSQLSessionInfo::replace_user_variable(
  ObExecContext &ctx, const common::ObString &name, const ObSessionVariable &value)
{
  int ret = OB_SUCCESS;
  bool is_package_variable = false;
  if (name.prefix_match("pkg.") && name.length() >= 52) {
    is_package_variable = true;
    for (int64_t i = 4; i < name.length(); ++i) {
      if (!((name[i] >= '0' && name[i] <= '9')
            || (name[i] >= 'a' && name[i] <= 'z'))) {
        is_package_variable = false;
      }
    }
  }
  if (0 == name.case_compare("##__OB_RESET_ALL_PACKAGE_BY_DBMS_SESSION_RESET_PACKAGE__")) {
    // "##__OB_RESET_ALL_PACKAGE_BY_DBMS_SESSION_RESET_PACKAGE__"
    // this variable is used to reset_package.
    // if we get a set stmt of OB_RESET_ALL_PACKAGE_BY_DBMS_SESSION_RESET_PACKAGE
    // we should only reset_all_package, do not need set_user_variable
    OZ (reset_all_package_state_by_dbms_session(false));
  } else if (is_package_variable && OB_NOT_NULL(get_pl_engine())) {
    OZ (set_package_variable(ctx, name, value.value_, true));
  } else {
    OZ (ObBasicSessionInfo::replace_user_variable(name, value));
  }
  return ret;
}

int ObSQLSessionInfo::replace_user_variables(const ObSessionValMap &user_var_map)
{
  return ObBasicSessionInfo::replace_user_variables(user_var_map);
}

int ObSQLSessionInfo::replace_user_variables(
  ObExecContext &ctx, const ObSessionValMap &user_var_map)
{
  int ret = OB_SUCCESS;
  OZ (ObBasicSessionInfo::replace_user_variables(user_var_map));
  if (OB_SUCC(ret)
      && OB_NOT_NULL(get_pl_engine())
      && user_var_map.size() > 0) {
    OZ (set_package_variables(ctx, user_var_map));
  }
  return ret;
}

int ObSQLSessionInfo::set_package_variables(
  ObExecContext &ctx, const ObSessionValMap &user_var_map)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("set package variables now!");
  const sql::ObSessionValMap::VarNameValMap &new_map = user_var_map.get_val_map();
  common::hash::ObHashSet<common::ObString> sync_pkg_vars;
  OZ (sync_pkg_vars.create(32));
  OX (pl_sync_pkg_vars_ = &sync_pkg_vars);
  for (sql::ObSessionValMap::VarNameValMap::const_iterator iter = new_map.begin();
       OB_SUCC(ret) && iter != new_map.end(); iter++) {
    const common::ObString &key = iter->first;
    const ObObj &value = iter->second.value_;
    if (!key.prefix_match("pkg.")) {
      // do nothing ...
    } else if (OB_HASH_EXIST == sync_pkg_vars.exist_refactored(key)) {
      // do nothing ...
    } else {
      OZ (set_package_variable(ctx, key, value));
    }
  }
  LOG_DEBUG("set package variables end!!!", K(ret));
  pl_sync_pkg_vars_ = NULL;
  return ret;
}

int ObSQLSessionInfo::set_package_variable(
  ObExecContext &ctx, const common::ObString &key, const common::ObObj &value, bool from_proxy)
{
  int ret = OB_SUCCESS;
  pl::ObPLPackageManager &pl_manager = get_pl_engine()->get_package_manager();
  share::schema::ObSchemaGetterGuard schema_guard;
  pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
  ObPackageVarSetName name;
  ObArenaAllocator allocator;
  bool match = false;
  CK (OB_NOT_NULL(GCTX.schema_service_));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  if (OB_SUCC(ret)) {
    ObPLResolveCtx resolve_ctx(
      ctx.get_allocator(),
      *this,
      schema_guard,
      package_guard,
      *(ctx.get_sql_proxy()),
      false, /*ps*/
      false, /*check mode*/
      false, /*sql scope*/
      NULL, /*param_list*/
      NULL, /*extern_param_info*/
      TgTimingEvent::TG_TIMING_EVENT_INVALID,
      true /*is_sync_pacakge_var*/);
    OZ (package_guard.init());
    OZ (name.decode(allocator, key));
    CK (name.valid());
    OZ (GCTX.schema_service_->get_tenant_schema_guard(get_effective_tenant_id(), schema_guard));
    OZ (pl_manager.check_version(resolve_ctx, name.package_id_, name.state_version_, match));
    if (OB_SUCC(ret) && match) {
      OZ (pl_manager.set_package_var_val(
        resolve_ctx, ctx, name.package_id_, name.var_idx_, value, true, from_proxy));
      LOG_DEBUG("set pacakge variable",
                K(ret), K(key), K(name.package_id_),
                K(name.state_version_.package_version_),
                K(name.state_version_.package_body_version_));
    } else {
      LOG_INFO("set package variable failed", K(ret), K(match), K(name), K(value));
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_sequence_value(uint64_t tenant_id,
                                         uint64_t seq_id,
                                         share::ObSequenceValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id ||
      OB_INVALID_ID == seq_id)) {
    LOG_WARN("invalid args", K(tenant_id), K(seq_id), K(ret));
  } else if (OB_FAIL(sequence_currval_map_.get_refactored(seq_id, value))) {
    LOG_WARN("fail get seq", K(tenant_id), K(seq_id), K(ret));
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_SEQUENCE_NOT_DEFINE;
      LOG_USER_ERROR(OB_ERR_SEQUENCE_NOT_DEFINE);
    }
  } else {
    // ok
  }
  return ret;
}

int ObSQLSessionInfo::set_sequence_value(uint64_t tenant_id,
                                         uint64_t seq_id,
                                         const ObSequenceValue &value)
{
  int ret = OB_SUCCESS;
  const bool overwrite_exits = true;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id ||
      OB_INVALID_ID == seq_id)) {
    LOG_WARN("invalid args", K(tenant_id), K(seq_id), K(ret));
  } else if (OB_FAIL(sequence_currval_map_.set_refactored(seq_id, value, overwrite_exits))) {
    LOG_WARN("fail get seq", K(tenant_id), K(seq_id), K(ret));
  } else {
    sequence_currval_encoder_.is_changed_ = true;
  }
  return ret;
}

int ObSQLSessionInfo::drop_sequence_value_if_exists(uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == seq_id)) {
    LOG_WARN("invalid args", K(seq_id), K(ret));
  } else if (OB_FAIL(sequence_currval_map_.erase_refactored(seq_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("drop sequence value not exists", K(ret), K(seq_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("drop sequence value failed", K(ret), K(seq_id));
    }
  } else {
    sequence_currval_encoder_.is_changed_ = true;
  }
  return ret;
}

int ObSQLSessionInfo::get_dblink_sequence_id(const ObString &sequence_name,
                                            uint64_t dblink_id,
                                            uint64_t &seq_id)const
{
  int ret = OB_SUCCESS;
  ObDBlinkSequenceIdKey key(sequence_name, dblink_id);
  if (OB_UNLIKELY(OB_INVALID_ID == dblink_id ||
      sequence_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(dblink_id), K(seq_id), K(ret));
  } else if (OB_FAIL(dblink_sequence_id_map_.get_refactored(key, seq_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      seq_id = OB_INVALID_ID;
      ret = OB_SUCCESS;
    }
  } else {
    // ok
  }
  LOG_TRACE("get dblink sequence id", K(sequence_name), K(dblink_id), K(seq_id));
  return ret;
}

int ObSQLSessionInfo::get_next_sequence_id(uint64_t &seq_id)
{
  int ret = OB_SUCCESS;
  seq_id = ++current_dblink_sequence_id_;
  sequence_currval_encoder_.is_changed_ = true;
  return ret;
}

int ObSQLSessionInfo::set_dblink_sequence_id(const ObString &sequence_name,
                                            uint64_t dblink_id,
                                            uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  ObString name;
  const bool overwrite_exits = true;
  if (OB_UNLIKELY(OB_INVALID_ID == dblink_id ||
      OB_INVALID_ID == seq_id ||
      sequence_name.empty())) {
    LOG_WARN("invalid args", K(dblink_id), K(seq_id), K(ret));
  } else if (OB_FAIL(ob_write_string(get_session_allocator(), sequence_name, name))) {
    LOG_WARN("failed to write obstring", K(ret));
  } else {
    ObDBlinkSequenceIdKey key(name, dblink_id);
    if (OB_FAIL(dblink_sequence_id_map_.set_refactored(key, seq_id, overwrite_exits))) {
      LOG_WARN("fail get seq", K(dblink_id), K(seq_id), K(ret));
    } else {
      sequence_currval_encoder_.is_changed_ = true;
      LOG_TRACE("set new dblink sequence id", K(name), K(dblink_id), K(seq_id));
    }
  }
  return ret;
}

int ObSQLSessionInfo::drop_dblink_sequence_id_if_exists(const ObString &sequence_name,
                                                        uint64_t dblink_id,
                                                        uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  ObDBlinkSequenceIdKey key(sequence_name, dblink_id);
  if (OB_UNLIKELY(OB_INVALID_ID == dblink_id ||
      OB_INVALID_ID == seq_id ||
      sequence_name.empty())) {
    LOG_WARN("invalid args", K(sequence_name), K(dblink_id), K(seq_id), K(ret));
  } else if (OB_FAIL(drop_sequence_value_if_exists(seq_id))) {
    LOG_WARN("failed to drop sequence value", K(ret));
  } else if (OB_FAIL(dblink_sequence_id_map_.erase_refactored(key))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("drop sequence value not exists", K(ret),  K(dblink_id), K(seq_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("drop sequence value failed", K(ret), K(dblink_id), K(seq_id));
    }
  } else {
    sequence_currval_encoder_.is_changed_ = true;
  }
  return ret;
}

int ObSQLSessionInfo::get_context_values(const ObString &context_name,
                                        const ObString &attribute,
                                        ObString &value,
                                        bool &exist)
{
  int ret = OB_SUCCESS;
  ObInnerContextMap *inner_map = nullptr;
  ObContextUnit *unit = nullptr;
  exist = false;
  if (OB_FAIL(contexts_map_.get_refactored(context_name, inner_map))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to probe hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(inner_map) || OB_ISNULL(inner_map->context_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get correct hash map", K(ret), KP(inner_map));
  } else if (OB_FAIL(inner_map->context_map_->get_refactored(attribute, unit))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to probe hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get correct unit", K(ret));
  } else {
    exist = true;
    value = unit->value_;
  }
  return ret;
}
int ObSQLSessionInfo::set_context_values(const ObString &context_name,
                                          const ObString &attribute,
                                          const ObString &value)
{
  int ret = OB_SUCCESS;
  ObInnerContextMap *inner_map = nullptr;
  ObContextUnit *exist_unit = nullptr;
  const bool overwrite_exits = true;
  int32_t session_context_size = static_cast<int32_t> (GCONF._session_context_size);
  if (OB_FAIL(init_mem_context(get_effective_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mem context", K(ret));
  } else {
    ObIAllocator &malloc_alloc = mem_context_->get_malloc_allocator();
    if (OB_FAIL(contexts_map_.get_refactored(context_name, inner_map))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to probe hash map", K(ret));
      } else if (curr_session_context_size_ >= session_context_size) {
        ret = OB_ERR_SESSION_CONTEXT_EXCEEDED;
        LOG_USER_ERROR(OB_ERR_SESSION_CONTEXT_EXCEEDED);
        LOG_WARN("use too much local context in a session", K(session_context_size),
                                                            K(curr_session_context_size_));
      } else {
        ObInnerContextMap *new_map = nullptr;
        ObContextUnit *new_unit = nullptr;
        if (OB_ISNULL(new_map = static_cast<ObInnerContextMap *>
                              (malloc_alloc.alloc(sizeof(ObInnerContextMap))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc context map", K(ret));
        } else if (FALSE_IT(new (new_map) ObInnerContextMap(malloc_alloc))) {
        } else if (OB_FAIL(new_map->init())) {
          LOG_WARN("failed to init context map", K(ret));
        } else if (OB_FAIL(ob_write_string(malloc_alloc,
                                            context_name,
                                            new_map->context_name_))) {
          LOG_WARN("failed to write name", K(ret));
        } else if (OB_ISNULL(new_unit = static_cast<ObContextUnit *>
                                    (malloc_alloc.alloc(sizeof(ObContextUnit))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc context unit", K(ret));
        } else if (FALSE_IT(new (new_unit) ObContextUnit())) {
        } else if (OB_FAIL(new_unit->deep_copy(attribute, value, malloc_alloc))) {
          LOG_WARN("failed to copy context unit", K(ret));
        } else if (OB_FAIL(new_map->context_map_->set_refactored(new_unit->attribute_, new_unit))) {
          LOG_WARN("failed to insert new unit", K(ret));
        } else if (OB_FAIL(contexts_map_.set_refactored(new_map->context_name_, new_map))) {
          LOG_WARN("failed to insert new map", K(ret));
        } else {
          app_ctx_info_encoder_.is_changed_ = true;
          ++curr_session_context_size_;
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(new_unit)) {
            new_unit->free(malloc_alloc);
            malloc_alloc.free(new_unit);
          }
          if (OB_NOT_NULL(new_map)) {
            new_map->destroy_map();
            malloc_alloc.free(new_map);
          }
        }
      }
    } else if (OB_ISNULL(inner_map) || OB_ISNULL(inner_map->context_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get correct hash map", K(ret), KP(inner_map));
    } else if (OB_FAIL(inner_map->context_map_->get_refactored(attribute, exist_unit))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to probe hash map", K(ret));
      } else if (curr_session_context_size_ >= session_context_size) {
        ret = OB_ERR_SESSION_CONTEXT_EXCEEDED;
        LOG_USER_ERROR(OB_ERR_SESSION_CONTEXT_EXCEEDED);
        LOG_WARN("use too much local context in a session", K(session_context_size),
                                                            K(curr_session_context_size_));
      } else {
        ObContextUnit *new_unit = nullptr;
        if (OB_ISNULL(new_unit = static_cast<ObContextUnit *>
                                    (malloc_alloc.alloc(sizeof(ObContextUnit))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc context unit", K(ret));
        } else if (FALSE_IT(new (new_unit) ObContextUnit())) {
        } else if (OB_FAIL(new_unit->deep_copy(attribute, value, malloc_alloc))) {
          LOG_WARN("failed to construst context unit", K(ret));
        } else if (OB_FAIL(inner_map->context_map_->set_refactored(new_unit->attribute_,
                                                                    new_unit))) {
          LOG_WARN("failed to insert new unit", K(ret));
        } else {
          app_ctx_info_encoder_.is_changed_ = true;
          ++curr_session_context_size_;
        }
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(new_unit)) {
            new_unit->free(malloc_alloc);
            malloc_alloc.free(new_unit);
          }
        }
      }
    } else {
      app_ctx_info_encoder_.is_changed_ = true;
      malloc_alloc.free(exist_unit->value_.ptr());
      exist_unit->value_.reset();
      if (OB_FAIL(ob_write_string(malloc_alloc, value, exist_unit->value_))) {
        LOG_WARN("failed to write value", K(ret));
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::clear_all_context(const ObString &context_name)
{
  int ret = OB_SUCCESS;
  ObInnerContextMap *inner_map = nullptr;
  if (OB_FAIL(init_mem_context(get_effective_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mem context", K(ret));
  } else if (OB_FAIL(contexts_map_.erase_refactored(context_name, &inner_map))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase namespace", K(ret));
    }
  } else if (OB_ISNULL(inner_map) || OB_ISNULL(inner_map->context_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get correct hash map", K(ret), KP(inner_map));
  } else {
    app_ctx_info_encoder_.is_changed_ = true;
    curr_session_context_size_ -= inner_map->context_map_->size();
    inner_map->destroy();
    mem_context_->get_malloc_allocator().free(inner_map);
  }
  return ret;
}
int ObSQLSessionInfo::clear_context(const ObString &context_name,
                                    const ObString &attribute)
{
  int ret = OB_SUCCESS;
  ObInnerContextMap *inner_map = nullptr;
  ObContextUnit *ctx_unit = nullptr;
  if (OB_FAIL(init_mem_context(get_effective_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mem context", K(ret));
  } else if (OB_FAIL(contexts_map_.get_refactored(context_name, inner_map))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase namespace", K(ret));
    }
  } else if (OB_ISNULL(inner_map) || OB_ISNULL(inner_map->context_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get correct hash map", K(ret), KP(inner_map));
  } else if (OB_FAIL(inner_map->context_map_->erase_refactored(attribute, &ctx_unit))) {
     if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase namespace", K(ret));
    }
  } else {
    app_ctx_info_encoder_.is_changed_ = true;
    --curr_session_context_size_;
    ctx_unit->free(mem_context_->get_malloc_allocator());
    mem_context_->get_malloc_allocator().free(ctx_unit);
  }
  return ret;
}

void ObSQLSessionInfo::set_flt_control_info(const FLTControlInfo &con_info)
{
  bool support_show_trace = flt_control_info_.support_show_trace_;
  flt_control_info_ = con_info;
  flt_control_info_.support_show_trace_ = support_show_trace;
  control_info_encoder_.is_changed_ = true;
}

void ObSQLSessionInfo::set_flt_control_info_no_sync(const FLTControlInfo &con_info)
{
  bool support_show_trace = flt_control_info_.support_show_trace_;
  flt_control_info_ = con_info;
  flt_control_info_.support_show_trace_ = support_show_trace;
}

int ObSQLSessionInfo::set_client_id(const common::ObString &client_identifier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicSessionInfo::set_client_identifier(client_identifier))) {
    LOG_WARN("failed to set client id", K(ret));
  } else {
    client_id_info_encoder_.is_changed_ = true;
  }
  return ret;
}

int ObSQLSessionInfo::save_session(StmtSavedValue &saved_value)
{
  int ret = OB_SUCCESS;
  OZ (save_basic_session(saved_value));
  OZ (save_sql_session(saved_value));
  return ret;
}

int ObSQLSessionInfo::save_sql_session(StmtSavedValue &saved_value)
{
  int ret = OB_SUCCESS;
  OX (saved_value.audit_record_.assign(audit_record_));
  OX (audit_record_.reset());
  OX (saved_value.inner_flag_ = inner_flag_);
  OX (saved_value.session_type_ = session_type_);
  OX (saved_value.read_uncommited_ = read_uncommited_);
  OX (saved_value.is_ignore_stmt_ = is_ignore_stmt_);
  OX (inner_flag_ = true);
#ifdef OB_BUILD_SPM
  OX (saved_value.select_plan_type_ = select_plan_type_);
#endif
  return ret;
}

int ObSQLSessionInfo::restore_sql_session(StmtSavedValue &saved_value)
{
  int ret = OB_SUCCESS;
  OX (session_type_ = saved_value.session_type_);
  OX (inner_flag_ = saved_value.inner_flag_);
  OX (read_uncommited_ = saved_value.read_uncommited_);
  OX (is_ignore_stmt_ = saved_value.is_ignore_stmt_);
  OX (audit_record_.assign(saved_value.audit_record_));
#ifdef OB_BUILD_SPM
  OX (select_plan_type_ = saved_value.select_plan_type_);
#endif
  return ret;
}

int ObSQLSessionInfo::restore_session(StmtSavedValue &saved_value)
{
  int ret = OB_SUCCESS;
  OZ (restore_sql_session(saved_value));
  OZ (restore_basic_session(saved_value));
  return ret;
}

int ObSQLSessionInfo::begin_nested_session(StmtSavedValue &saved_value, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  OV (nested_count_ >= 0, OB_ERR_UNEXPECTED, nested_count_);
  OZ (ObBasicSessionInfo::begin_nested_session(saved_value, skip_cur_stmt_tables));
  OZ (save_sql_session(saved_value));
  OX (nested_count_++);
  LOG_DEBUG("begin_nested_session", K(ret), K_(nested_count));
  return ret;
}

int ObSQLSessionInfo::end_nested_session(StmtSavedValue &saved_value)
{
  int ret = OB_SUCCESS;
  OV (nested_count_ > 0, OB_ERR_UNEXPECTED, nested_count_);
  OX (nested_count_--);
  OZ (restore_sql_session(saved_value));
  OZ (ObBasicSessionInfo::end_nested_session(saved_value));
  OX (saved_value.reset());
  return ret;
}

int ObSQLSessionInfo::set_enable_role_array(const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  ret = enable_role_array_.assign(role_id_array);
  return ret;
}

void ObSQLSessionInfo::ObCachedTenantConfigInfo::refresh()
{
  int tmp_ret = OB_SUCCESS;
  const uint64_t effective_tenant_id = session_->get_effective_tenant_id();
  int64_t cur_ts = ObClockGenerator::getClock();
  const bool change_tenant = (saved_tenant_info_ != effective_tenant_id);
  if (change_tenant || cur_ts - last_check_ec_ts_ > 5000000) {
    if (change_tenant) {
      LOG_DEBUG("refresh tenant config where tenant changed",
                  K_(saved_tenant_info), K(effective_tenant_id));
      ATOMIC_STORE(&saved_tenant_info_, effective_tenant_id);
    }
      // 1.是否支持外部一致性
    is_external_consistent_ = transaction::ObTsMgr::get_instance().is_external_consistent(effective_tenant_id);
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(effective_tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      // 2.是否允许 batch_multi_statement
      enable_batched_multi_statement_ = tenant_config->ob_enable_batched_multi_statement;
      // 3.是否允许bloom_filter
      if (tenant_config->_bloom_filter_enabled) {
        enable_bloom_filter_ = true;
      } else {
        enable_bloom_filter_ = false;
      }
      // 4.sort area size
      ATOMIC_STORE(&sort_area_size_, tenant_config->_sort_area_size);
      ATOMIC_STORE(&hash_area_size_, tenant_config->_hash_area_size);
      ATOMIC_STORE(&enable_query_response_time_stats_, tenant_config->query_response_time_stats);
      ATOMIC_STORE(&enable_user_defined_rewrite_rules_, tenant_config->enable_user_defined_rewrite_rules);
      ATOMIC_STORE(&range_optimizer_max_mem_size_, tenant_config->range_optimizer_max_mem_size);
      // 5.allow security audit
      if (OB_SUCCESS != (tmp_ret = ObSecurityAuditUtils::check_allow_audit(*session_, at_type_))) {
        LOG_WARN_RET(tmp_ret, "fail get tenant_config", "ret", tmp_ret,
                                           K(session_->get_priv_tenant_id()),
                                           K(effective_tenant_id),
                                           K(lbt()));
      }
      // 6. enable extended SQL syntax in the MySQL mode
      enable_sql_extension_ = tenant_config->enable_sql_extension;
      px_join_skew_handling_ = tenant_config->_px_join_skew_handling;
      px_join_skew_minfreq_ = tenant_config->_px_join_skew_minfreq;
      // 7. print_sample_ppm_ for flt
      ATOMIC_STORE(&print_sample_ppm_, tenant_config->_print_sample_ppm);
    }
    ATOMIC_STORE(&last_check_ec_ts_, cur_ts);
    session_->update_tenant_config_version(
        (::oceanbase::omt::ObTenantConfigMgr::get_instance()).get_tenant_config_version(effective_tenant_id));
  }
  UNUSED(tmp_ret);
}

int ObSQLSessionInfo::get_tmp_table_size(uint64_t &size) {
  int ret = OB_SUCCESS;
  const ObBasicSysVar *tmp_table_size = get_sys_var(SYS_VAR_TMP_TABLE_SIZE);
  CK (OB_NOT_NULL(tmp_table_size));
  if (OB_SUCC(ret) &&
      tmp_table_size->get_value().get_uint64() != tmp_table_size->get_max_val().get_uint64()) {
    size = tmp_table_size->get_value().get_uint64();
  } else {
    size = OB_INVALID_SIZE;
  }
  return ret;
}
int ObSQLSessionInfo::ps_use_stream_result_set(bool &use_stream) {
  int ret = OB_SUCCESS;
  uint64_t size = 0;
  use_stream = false;
  OZ (get_tmp_table_size(size));
  if (OB_SUCC(ret) && OB_INVALID_SIZE == size) {
    use_stream = true;
#if !defined(NDEBUG)
    LOG_INFO("cursor use stream result.");
#endif
  }
  return ret;
}

void* ObSQLSessionInfo::get_piece_cache(bool need_init) {
  if (NULL == piece_cache_ && need_init) {
    void *buf = get_session_allocator().alloc(sizeof(observer::ObPieceCache));
    if (NULL != buf) {
      MEMSET(buf, 0, sizeof(observer::ObPieceCache));
      piece_cache_ = new (buf) observer::ObPieceCache();
      if (OB_SUCCESS != (static_cast<observer::ObPieceCache*>(piece_cache_))->init(
                            get_effective_tenant_id())) {
        get_session_allocator().free(piece_cache_);
        piece_cache_ = NULL;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "init piece cache fail");
      }
    }
  }
  return piece_cache_;
}




int ObSQLSessionInfo::on_user_connect(share::schema::ObSessionPrivInfo &priv_info,
                                      const ObUserInfo *user_info)
{
  int ret = OB_SUCCESS;
  ObConnectResourceMgr *conn_res_mgr = GCTX.conn_res_mgr_;
  if (get_is_deserialized()) {
    // do nothing
  } else if (OB_ISNULL(conn_res_mgr) || OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connect resource mgr or user info is null", K(ret), K(conn_res_mgr));
  } else {
    const ObPrivSet &priv = priv_info.user_priv_set_;
    const ObString &user_name = priv_info.user_name_;
    const uint64_t tenant_id = priv_info.tenant_id_;
    const uint64_t user_id = priv_info.user_id_;
    uint64_t max_connections_per_hour = user_info->get_max_connections();
    uint64_t max_user_connections = user_info->get_max_user_connections();
    uint64_t max_tenant_connections = 0;
    if (OB_FAIL(get_sys_variable(SYS_VAR_MAX_CONNECTIONS, max_tenant_connections))) {
      LOG_WARN("get system variable SYS_VAR_MAX_CONNECTIONS failed", K(ret));
    } else if (0 == max_user_connections) {
      if (OB_FAIL(get_sys_variable(SYS_VAR_MAX_USER_CONNECTIONS, max_user_connections))) {
        LOG_WARN("get system variable SYS_VAR_MAX_USER_CONNECTIONS failed", K(ret));
      }
    } else {
      ObObj val;
      val.set_uint64(max_user_connections);
      if (OB_FAIL(update_sys_variable(SYS_VAR_MAX_USER_CONNECTIONS, val))) {
        LOG_WARN("set system variable SYS_VAR_MAX_USER_CONNECTIONS failed", K(ret), K(val));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(conn_res_mgr->on_user_connect(
                tenant_id, user_id, priv, user_name,
                max_connections_per_hour,
                max_user_connections,
                max_tenant_connections, *this))) {
      LOG_WARN("create user connection failed", K(ret));
    }
  }
  return ret;
}

int ObSQLSessionInfo::on_user_disconnect()
{
  int ret = OB_SUCCESS;
  ObConnectResourceMgr *conn_res_mgr = GCTX.conn_res_mgr_;
  if (OB_ISNULL(conn_res_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connect resource mgr is null", K(ret));
  } else if (OB_FAIL(conn_res_mgr->on_user_disconnect(*this))) {
    LOG_WARN("user disconnect failed", K(ret));
  }
  return ret;
}

// prepare baseline for the following `calc_txn_free_route` to get the diff
void ObSQLSessionInfo::prep_txn_free_route_baseline(bool reset_audit)
{
#define RESET_TXN_STATE_ENCODER_CHANGED_(x) txn_##x##_info_encoder_.is_changed_ = false
#define RESET_TXN_STATE_ENCODER_CHANGED(x) RESET_TXN_STATE_ENCODER_CHANGED_(x)
  LST_DO(RESET_TXN_STATE_ENCODER_CHANGED, (;), static, dynamic, participants, extra);
#undef RESET_TXN_STATE_ENCODER_CHANGED
#undef RESET_TXN_STATE_ENCODER_CHANGED_
  if (reset_audit) {
    txn_free_route_ctx_.reset_audit_record();
  }
  txn_free_route_ctx_.init_before_handle_request(tx_desc_);
}

void ObSQLSessionInfo::post_sync_session_info()
{
  if (!get_is_in_retry()) {
    // preapre baseline for the following executing stmt/cmd
    prep_txn_free_route_baseline(false);
  }
}

void ObSQLSessionInfo::set_txn_free_route(bool txn_free_route)
{
  txn_free_route_ctx_.reset_audit_record();
  txn_free_route_ctx_.init_before_update_state(txn_free_route);
}

bool ObSQLSessionInfo::can_txn_free_route() const
{
  return txn_free_route_ctx_.can_free_route();
}

int ObSQLSessionInfo::calc_txn_free_route()
{
  int ret = OB_SUCCESS;
  if (!txn_free_route_ctx_.has_calculated()) {
    OZ (ObSqlTransControl::calc_txn_free_route(*this, txn_free_route_ctx_));
    if (OB_SUCC(ret)) {
      txn_static_info_encoder_.is_changed_ = txn_free_route_ctx_.is_static_changed();
      txn_dynamic_info_encoder_.is_changed_ = txn_free_route_ctx_.is_dynamic_changed();
      txn_participants_info_encoder_.is_changed_ = txn_free_route_ctx_.is_parts_changed();
      txn_extra_info_encoder_.is_changed_ = txn_free_route_ctx_.is_extra_changed();
    }
  }
  return ret;
}

void ObSQLSessionInfo::check_txn_free_route_alive()
{
  ObSqlTransControl::check_free_route_tx_alive(*this, txn_free_route_ctx_);
}

void ObSQLSessionInfo::reset_tx_variable(bool reset_next_scope)
{
  ObBasicSessionInfo::reset_tx_variable(reset_next_scope);
  set_early_lock_release(false);
}
void ObSQLSessionInfo::destroy_contexts_map(ObContextsMap &map, common::ObIAllocator &alloc)
{
  for (auto it = map.begin(); it != map.end(); ++it) {
    it->second->destroy();
    alloc.free(it->second);
  }
}

inline int ObSQLSessionInfo::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, ObModIds::OB_SQL_SESSION,
                     common::ObCtxIds::WORK_AREA);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    }
  }
  return ret;
}

bool ObSQLSessionInfo::has_sess_info_modified() const {
  bool is_changed = false;
  for (int64_t i = 0; !is_changed && i < SESSION_SYNC_MAX_TYPE; i++) {
    is_changed |= sess_encoders_[i]->is_changed_;
  }
  return is_changed;
}

int ObSQLSessionInfo::set_module_name(const common::ObString &mod) {
  int ret = OB_SUCCESS;
  MEMSET(module_buf_, 0x00, common::OB_MAX_MOD_NAME_LENGTH);
  MEMCPY(module_buf_, mod.ptr(), min(common::OB_MAX_MOD_NAME_LENGTH, mod.length()));
  client_app_info_.module_name_.assign(&module_buf_[0], min(common::OB_MAX_MOD_NAME_LENGTH, mod.length()));
  return ret;
}

int ObSQLSessionInfo::set_action_name(const common::ObString &act) {
  int ret = OB_SUCCESS;
  MEMSET(action_buf_, 0x00, common::OB_MAX_ACT_NAME_LENGTH);
  MEMCPY(action_buf_, act.ptr(), min(common::OB_MAX_ACT_NAME_LENGTH, act.length()));
  client_app_info_.action_name_.assign(&action_buf_[0], min(common::OB_MAX_ACT_NAME_LENGTH, act.length()));
  return ret;
}

int ObSQLSessionInfo::set_client_info(const common::ObString &client_info) {
  int ret = OB_SUCCESS;
  MEMSET(client_info_buf_, 0x00, common::OB_MAX_CLIENT_INFO_LENGTH);
  MEMCPY(client_info_buf_, client_info.ptr(), min(common::OB_MAX_CLIENT_INFO_LENGTH, client_info.length()));
  client_app_info_.client_info_.assign(&client_info_buf_[0], min(common::OB_MAX_CLIENT_INFO_LENGTH, client_info.length()));
  return ret;
}


int ObSQLSessionInfo::get_sess_encoder(const SessionSyncInfoType sess_sync_info_type, ObSessInfoEncoder* &encoder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sess_sync_info_type >= SESSION_SYNC_MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session sync info type", K(ret), K(sess_sync_info_type));
  } else if (OB_ISNULL(sess_encoders_[sess_sync_info_type])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session sync info encoder", K(ret), K(sess_sync_info_type));
  } else {
    encoder = sess_encoders_[sess_sync_info_type];
  }
  return ret;
}

int ObSQLSessionInfo::update_sess_sync_info(const SessionSyncInfoType sess_sync_info_type,
                                          const char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("deserialize encode buf", KPHEX(buf+pos, length-pos), K(length-pos), K(pos));
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", K(ret), K(buf));
  } else if (sess_sync_info_type < 0  || sess_sync_info_type >= SESSION_SYNC_MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session sync info type", K(ret), K(sess_sync_info_type));
  } else if (OB_ISNULL(sess_encoders_[sess_sync_info_type])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session sync info encoder", K(ret), K(sess_sync_info_type));
  } else if (OB_FAIL(sess_encoders_[sess_sync_info_type]->deserialize(*this, buf, length, pos))) {
    LOG_WARN("failed to deserialize sess sync info", K(ret), K(sess_sync_info_type), KPHEX(buf, length), K(length), K(pos));
  } else {
    // do nothing
    LOG_DEBUG("get app info", K(client_app_info_.module_name_), K(client_app_info_.action_name_), K(client_app_info_.client_info_));
  }
  return ret;
}



int ObErrorSyncSysVarEncoder::serialize(ObSQLSessionInfo &sess, char *buf,
                                const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSysVarClassType, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_var_delta_ids;
  if (OB_FAIL(sess.get_error_sync_sys_vars(sys_var_delta_ids))) {
    LOG_WARN("failed to calc need serialize vars", K(ret));
  } else if (OB_FAIL(sess.serialize_sync_sys_vars(sys_var_delta_ids, buf, length, pos))) {
    LOG_WARN("failed to serialize sys var delta", K(ret), K(sys_var_delta_ids.count()),
                                      KPHEX(buf+pos, length-pos), K(length-pos), K(pos));
  } else {
    LOG_TRACE("success serialize sys var delta", K(ret), K(sys_var_delta_ids),
              "inc sys var ids", sess.sys_var_inc_info_.get_all_sys_var_ids(),
              K(sess.get_sessid()), K(sess.get_proxy_sessid()));
  }
  return ret;
}

int ObErrorSyncSysVarEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf,
                                  const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t deserialize_sys_var_count = 0;
  if (OB_FAIL(sess.deserialize_sync_error_sys_vars(deserialize_sys_var_count, buf, length, pos))) {
    LOG_WARN("failed to deserialize sys var delta", K(ret), K(deserialize_sys_var_count),
                                    KPHEX(buf+pos, length-pos), K(length-pos), K(pos));
  } else {
    LOG_DEBUG("success deserialize sys var delta", K(ret), K(deserialize_sys_var_count));
  }
  return ret;
}

int ObErrorSyncSysVarEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const {
  int ret = OB_SUCCESS;
  ObSEArray<ObSysVarClassType, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_var_delta_ids;
  if (OB_FAIL(sess.get_error_sync_sys_vars(sys_var_delta_ids))) {
    LOG_WARN("failed to calc need serialize vars", K(ret));
  } else if (OB_FAIL(sess.get_sync_sys_vars_size(sys_var_delta_ids, len))) {
    LOG_WARN("failed to serialize size sys var delta", K(ret));
  } else {
    LOG_DEBUG("success serialize size sys var delta", K(ret), K(sys_var_delta_ids.count()), K(len));
  }
  return ret;
}

int ObErrorSyncSysVarEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
    if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_LAST_SCHEMA_VERSION) {
      //need sync sys var
      if (OB_FAIL(sess.get_sys_var(j)->serialize(buf, length, pos))) {
        LOG_WARN("failed to serialize", K(length), K(ret));
      }
    } else {
      // do nothing.
    }
  }
  return ret;
}

int64_t ObErrorSyncSysVarEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t size = 0;
  for (int64_t j = 0; j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
    if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_LAST_SCHEMA_VERSION) {
      // need sync sys var
      size += sess.get_sys_var(j)->get_serialize_size();
    } else {
      // do nothing.
    }
  }
  return size;
}

int ObErrorSyncSysVarEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                          const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObErrorSyncSysVarEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  int64_t pos = 0;
  const char *buf = last_sess_buf;
  int64_t data_len = last_sess_length;
  common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_SESSION,
                                                    OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    sess.get_effective_tenant_id());

  ObBasicSysVar *last_sess_sys_vars = NULL;
  for (int64_t j = 0; OB_SUCC(ret) && j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
    if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_LAST_SCHEMA_VERSION) {
      if (OB_FAIL(ObSessInfoVerify::create_tmp_sys_var(sess, ObSysVariables::get_sys_var_id(j),
          last_sess_sys_vars, allocator))) {
        LOG_WARN("fail to create sys var", K(ret));
      } else if (OB_FAIL(last_sess_sys_vars->deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize", K(ret), K(data_len), K(pos));
      } else if (!sess.get_sys_var(j)->get_value().can_compare(
                last_sess_sys_vars->get_value())) {
        share::ObTaskController::get().allow_next_syslog();
        LOG_WARN("failed to verify sys vars", K(j), K(ret),
                "current_sess_sys_vars", sess.get_sys_var(j)->get_value(),
                "last_sess_sys_vars", last_sess_sys_vars->get_value());
      } else if (sess.get_sys_var(j)->get_value() != last_sess_sys_vars->get_value()) {
        share::ObTaskController::get().allow_next_syslog();
        LOG_WARN("failed to verify sys vars", K(j), K(ret),
                "current_sess_sys_vars", sess.get_sys_var(j)->get_value(),
                "last_sess_sys_vars", last_sess_sys_vars->get_value());
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_mem_ctx_alloc(common::ObIAllocator *&alloc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_mem_context(get_effective_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mem context", K(ret));
  } else {
    alloc = &mem_context_->get_malloc_allocator();
  }
  return ret;
}

int ObSysVarEncoder::serialize(ObSQLSessionInfo &sess, char *buf,
                                const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSysVarClassType, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_var_delta_ids;
  if (OB_FAIL(sess.get_sync_sys_vars(sys_var_delta_ids))) {
    LOG_WARN("failed to calc need serialize vars", K(ret));
  } else if (OB_FAIL(sess.serialize_sync_sys_vars(sys_var_delta_ids, buf, length, pos))) {
    LOG_WARN("failed to serialize sys var delta", K(ret), K(sys_var_delta_ids.count()),
                                      KPHEX(buf+pos, length-pos), K(length-pos), K(pos));
  } else {
    LOG_TRACE("success serialize sys var delta", K(ret), K(sys_var_delta_ids),
              "inc sys var ids", sess.sys_var_inc_info_.get_all_sys_var_ids(),
              K(sess.get_sessid()), K(sess.get_proxy_sessid()));
  }
  return ret;
}

int ObSysVarEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf,
                                  const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t deserialize_sys_var_count = 0;
  if (OB_FAIL(sess.deserialize_sync_sys_vars(deserialize_sys_var_count, buf, length, pos))) {
    LOG_WARN("failed to deserialize sys var delta", K(ret), K(deserialize_sys_var_count),
                                    KPHEX(buf+pos, length-pos), K(length-pos), K(pos));
  } else {
    LOG_DEBUG("success deserialize sys var delta", K(ret), K(deserialize_sys_var_count));
  }
  return ret;
}

int ObSysVarEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const {
  int ret = OB_SUCCESS;
  ObSEArray<ObSysVarClassType, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_var_delta_ids;
  if (OB_FAIL(sess.get_sync_sys_vars(sys_var_delta_ids))) {
    LOG_WARN("failed to calc need serialize vars", K(ret));
  } else if (OB_FAIL(sess.get_sync_sys_vars_size(sys_var_delta_ids, len))) {
    LOG_WARN("failed to serialize size sys var delta", K(ret));
  } else {
    LOG_DEBUG("success serialize size sys var delta", K(ret), K(sys_var_delta_ids.count()), K(len));
  }
  return ret;
}

int ObSysVarEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sess.get_sys_var_cache_inc_data().serialize(buf, length, pos))) {
    LOG_WARN("failed to serialize", K(length), K(ret));
  } else if (OB_FAIL(sess.get_sys_var_in_pc_str().serialize(buf, length, pos))) {
    LOG_WARN("failed to serialize", K(ret), K(length), K(pos));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
      if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_SERVER_UUID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_PROXY_PARTITION_HIT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_STATEMENT_TRACE_ID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_VERSION_COMMENT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK) {
        // no need sync sys var
        continue;
      }
      if (OB_FAIL(sess.get_sys_var(j)->serialize(buf, length, pos))) {
        LOG_WARN("failed to serialize", K(length), K(ret));
      }
    }
  }
  return ret;
}

int64_t ObSysVarEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t size = 0;
  size = sess.get_sys_var_cache_inc_data().get_serialize_size();
  size += sess.get_sys_var_in_pc_str().get_serialize_size();
  for (int64_t j = 0; j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
      if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_SERVER_UUID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_PROXY_PARTITION_HIT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_STATEMENT_TRACE_ID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_VERSION_COMMENT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK) {
      // no need sync sys var
      continue;
    }
    size += sess.get_sys_var(j)->get_serialize_size();
  }
  return size;
}

int ObSysVarEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                          const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObSysVarEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  int64_t pos = 0;
  const char *buf = last_sess_buf;
  int64_t data_len = last_sess_length;
  common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_SESSION,
                                                    OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    sess.get_effective_tenant_id());
  ObBasicSessionInfo::SysVarsCacheData last_sess_sys_var_cache_data;
  ObString last_sess_sys_var_in_pc_str;
  bool is_error = false; // judging the error location
  if (OB_FAIL(last_sess_sys_var_cache_data.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(last_sess_sys_var_in_pc_str.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret), K(data_len), K(pos));
  } else {
    ObBasicSysVar *last_sess_sys_vars = NULL;
    for (int64_t j = 0; OB_SUCC(ret) && j< share::ObSysVarFactory::ALL_SYS_VARS_COUNT; ++j) {
      if (ObSysVariables::get_sys_var_id(j) == SYS_VAR_SERVER_UUID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_PROXY_PARTITION_HIT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_OB_STATEMENT_TRACE_ID ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR_VERSION_COMMENT ||
          ObSysVariables::get_sys_var_id(j) == SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK) {
        // no need sync sys var
        continue;
      }
      if (OB_FAIL(ObSessInfoVerify::create_tmp_sys_var(sess, ObSysVariables::get_sys_var_id(j),
          last_sess_sys_vars, allocator))) {
        LOG_WARN("fail to create sys var", K(ret));
      } else if (OB_FAIL(last_sess_sys_vars->deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize", K(ret), K(data_len), K(pos));
      } else if (!sess.get_sys_var(j)->get_value().can_compare(
                last_sess_sys_vars->get_value())) {
        share::ObTaskController::get().allow_next_syslog();
        LOG_WARN("failed to verify sys vars", K(j), K(ret),
                "current_sess_sys_vars", sess.get_sys_var(j)->get_value(),
                "last_sess_sys_vars", last_sess_sys_vars->get_value());
        is_error = true;
      } else if (sess.get_sys_var(j)->get_value() != last_sess_sys_vars->get_value()) {
        share::ObTaskController::get().allow_next_syslog();
        LOG_WARN("failed to verify sys vars", K(j), K(ret),
                "current_sess_sys_vars", sess.get_sys_var(j)->get_value(),
                "last_sess_sys_vars", last_sess_sys_vars->get_value());
        is_error = true;
      } else {
        // do nothing
      }
    }
    if (OB_FAIL(ret)) {

    } else if (sess.get_sys_var_in_pc_str() != last_sess_sys_var_in_pc_str) {
      share::ObTaskController::get().allow_next_syslog();
      LOG_WARN("failed to verify sys var in pc str", K(ret), "current_sess_sys_var_in_pc_str",
            sess.get_sys_var_in_pc_str(),
            "last_sess_sys_var_in_pc_str", last_sess_sys_var_in_pc_str);
    } else if (!is_error) {
      share::ObTaskController::get().allow_next_syslog();
      LOG_WARN("failed to verify sys vars cache inc data", K(ret),
          "current_sess_sys_var_cache_data", sess.get_sys_var_cache_inc_data(),
          "last_sess_sys_var_cache_data", last_sess_sys_var_cache_data);
    }
  }
  return ret;
}

void ObSQLSessionInfo::gen_gtt_session_scope_unique_id()
{
  static int64_t cur_ts = 0;
  int64_t next_ts = ObSQLUtils::combine_server_id(ObSQLUtils::get_next_ts(cur_ts), GCTX.server_id_);
  gtt_session_scope_unique_id_ = next_ts;
  LOG_DEBUG("check temporary table ssid session scope", K(next_ts), K(get_sessid_for_table()), K(GCTX.server_id_), K(lbt()));
}

void ObSQLSessionInfo::gen_gtt_trans_scope_unique_id()
{
  static int64_t cur_ts = 0;
  int64_t next_ts = ObSQLUtils::combine_server_id(ObSQLUtils::get_next_ts(cur_ts), GCTX.server_id_);
  gtt_trans_scope_unique_id_ = next_ts;
  LOG_DEBUG("check temporary table ssid trans scope", K(next_ts), K(get_sessid_for_table()), K(GCTX.server_id_), K(lbt()));
}

int ObAppInfoEncoder::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t org_pos = pos;
  if (OB_FAIL(sess.get_client_app_info().serialize(buf, length, pos))) {
    LOG_WARN("failed to serialize application info.", K(ret), K(pos), K(length));
  } else {
    LOG_DEBUG("serialize encode buf", KPHEX(buf+org_pos, pos-org_pos), K(pos-org_pos));
    LOG_DEBUG("serialize buf", KPHEX(buf, pos));
  }
  return ret;
}

int ObAppInfoEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("app info buf", KPHEX(buf, length), K(pos), K(length), KPHEX(buf+pos, length-pos));
  sess.get_client_app_info().reset();
  ObSQLSessionInfo::ApplicationInfo app_info;
  if (OB_FAIL(app_info.deserialize(buf, length, pos))) {
    LOG_WARN("failed to deserialize application info.", K(ret), K(pos), K(length));
  } else if (OB_FAIL(sess.set_client_info(app_info.client_info_))) {
    LOG_WARN("failed to set client info", K(ret));
  } else if (OB_FAIL(sess.set_action_name(app_info.action_name_))) {
    LOG_WARN("failed to set action name", K(ret));
  } else if (OB_FAIL(sess.set_module_name(app_info.module_name_))) {
    LOG_WARN("failed to set module name", K(ret));
  } else {
    LOG_TRACE("get encoder app info", K(sess.get_client_app_info().module_name_),
                                      K(sess.get_client_app_info().action_name_),
                                      K(sess.get_client_app_info().client_info_));
  }
  return ret;
}

int ObAppInfoEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const {
  int ret = OB_SUCCESS;
  len = sess.get_client_app_info().get_serialize_size();
  return ret;
}

int ObAppInfoEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize(sess, buf, length, pos))) {
    LOG_WARN("failed to fetch session info.", K(ret), K(pos), K(length));
  }
  return ret;
}

int64_t ObAppInfoEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t len = 0;
  get_serialize_size(sess, len);
  return len;
}

int ObAppInfoEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                          const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObAppInfoEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  ObSQLSessionInfo::ApplicationInfo last_sess_app_info;
  if (OB_FAIL(last_sess_app_info.deserialize(last_sess_buf, last_sess_length, pos))) {
    LOG_WARN("failed to deserialize application info.", K(ret), K(pos), K(last_sess_length));
  } else if (sess.get_client_info() != last_sess_app_info.client_info_) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_WARN("failed to verify client info", K(ret),
      "current_sess_app_info.client_info_", sess.get_client_info(),
      "last_sess_app_info.client_info_", last_sess_app_info.client_info_);
  } else if (sess.get_action_name() != last_sess_app_info.action_name_) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_WARN("failed to verify action name", K(ret),
      "current_sess_app_info.action_name_", sess.get_action_name(),
      "last_sess_app_info.action_name_", last_sess_app_info.action_name_);
  } else if (sess.get_module_name() != last_sess_app_info.module_name_) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_WARN("failed to verify module name", K(ret),
      "current_sess_app_info.module_name_", sess.get_module_name(),
      "last_sess_app_info.module_name_", last_sess_app_info.module_name_);
  } else {
    share::ObTaskController::get().allow_next_syslog();
    LOG_INFO("success to verify app info",
              "current_sess_app_info client info", sess.get_client_info(),
          "last_sess_app_info client info", last_sess_app_info.client_info_,
          "current_sess_app_info action name", sess.get_action_name(),
          "last_sess_app_info action name", last_sess_app_info.action_name_,
          "current_sess_app_info module name", sess.get_module_name(),
          "last_sess_app_info module name", last_sess_app_info.module_name_);
  }
  return ret;
}

int ObAppInfoEncoder::set_client_info(ObSQLSessionInfo* sess, const ObString &client_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sess is NULL", K(ret));
  } else if (OB_FAIL(sess->set_client_info(client_info))) {
    LOG_WARN("failed to set client info", K(ret));
  } else {
    this->is_changed_ = true;
  }
  return ret;
}

int ObAppInfoEncoder::set_module_name(ObSQLSessionInfo* sess, const ObString &mod)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sess is NULL", K(ret));
  } else if (OB_FAIL(sess->set_module_name(mod))) {
    LOG_WARN("failed to set module name", K(ret));
  } else {
    this->is_changed_ = true;
  }
  return ret;
}

int ObAppInfoEncoder::set_action_name(ObSQLSessionInfo* sess, const ObString &act)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sess is NULL", K(ret));
  } else if (OB_FAIL(sess->set_action_name(act))) {
    LOG_WARN("failed to set action name", K(ret));
  } else {
    this->is_changed_ = true;
  }
  return ret;
}

int ObClientIdInfoEncoder::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, sess.get_client_identifier());
  return ret;
}
int ObClientIdInfoEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sess.init_client_identifier())) {
    LOG_WARN("failed to init client identifier", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, sess.get_client_identifier_for_update());
  }
  return ret;
}
int ObClientIdInfoEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ADD_LEN(sess.get_client_identifier());
  return ret;
}

int ObClientIdInfoEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize(sess, buf, length, pos))) {
    LOG_WARN("failed to fetch session info.", K(ret), K(pos), K(length));
  }
  return ret;
}

int64_t ObClientIdInfoEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t len = 0;
  get_serialize_size(sess, len);
  return len;
}

int ObClientIdInfoEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                            const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObClientIdInfoEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  common::ObString last_sess_client_identifier;
  const char *buf = last_sess_buf;
  int64_t pos = 0;
  int64_t data_len = last_sess_length;
  LST_DO_CODE(OB_UNIS_DECODE, last_sess_client_identifier);
  if (sess.get_client_identifier() != last_sess_client_identifier) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_WARN("failed to verify client_identifier", K(ret),
      "current_sess_client_identifier", sess.get_client_identifier(),
      "last_sess_client_identifier", last_sess_client_identifier);
  } else {
    share::ObTaskController::get().allow_next_syslog();
    LOG_INFO("success to verify clientid info",
              "current_sess_client_identifier", sess.get_client_identifier(),
              "last_sess_client_identifier", last_sess_client_identifier);
  }

  return ret;
}

int ObAppCtxInfoEncoder::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObContextsMap &map = sess.get_contexts_map();
  OB_UNIS_ENCODE(map.size());
  int64_t count = 0;
  for (auto it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it, ++count) {
    OB_UNIS_ENCODE(*it->second);
  }
  CK (count == map.size());
  return ret;
}
int ObAppCtxInfoEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t map_size = 0;
  OB_UNIS_DECODE(map_size);
  ObIAllocator *alloc = nullptr;
  OZ (sess.get_mem_ctx_alloc(alloc));
  CK (OB_NOT_NULL(alloc));
  OX (sess.reuse_context_map());
  for (int64_t i = 0; OB_SUCC(ret) && i < map_size; ++i) {
    ObInnerContextMap *inner_map = nullptr;
    if (OB_ISNULL(inner_map = static_cast<ObInnerContextMap *> (alloc->alloc(sizeof(ObInnerContextMap))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc inner context map", K(ret));
    } else {
      new (inner_map) ObInnerContextMap(*alloc);
      OB_UNIS_DECODE(*inner_map);
      OZ (sess.get_contexts_map().set_refactored(inner_map->context_name_, inner_map));
    }
  }
  return ret;
}
int ObAppCtxInfoEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const
{
  int ret = OB_SUCCESS;
  ObContextsMap &map = sess.get_contexts_map();
  OB_UNIS_ADD_LEN(map.size());
  for (auto it = map.begin(); it != map.end(); ++it) {
    OB_UNIS_ADD_LEN(*it->second);
  }
  return ret;
}

int ObAppCtxInfoEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize(sess, buf, length, pos))) {
    LOG_WARN("failed to fetch session info.", K(ret), K(pos), K(length));
  }
  return ret;
}

int64_t ObAppCtxInfoEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t len = 0;
  get_serialize_size(sess, len);
  return len;
}

int ObAppCtxInfoEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                           const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObAppCtxInfoEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  const char *buf = last_sess_buf;
  int64_t pos = 0;
  int64_t data_len = last_sess_length;
  int64_t map_size = 0;
  common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_SESSION,
                                                    OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    sess.get_effective_tenant_id());
  ObContextsMap &map = sess.get_contexts_map();
  OB_UNIS_DECODE(map_size);
  if (map_size != sess.get_contexts_map().size()) {
     LOG_WARN("failed to verify app ctx info", K(ret), "current_map_size", map_size,
               "last_map_size", sess.get_contexts_map().size());
  } else {
    auto it = map.begin();
    for (int64_t i = 0; OB_SUCC(ret) && i < map_size && it != map.end(); ++i, ++it) {
      ObInnerContextMap *last_inner_map = nullptr;
      if (OB_ISNULL(last_inner_map = static_cast<ObInnerContextMap *>
                    (allocator.alloc(sizeof(ObInnerContextMap))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc inner context map", K(ret));
      } else {
        new (last_inner_map) ObInnerContextMap(allocator);
        OB_UNIS_DECODE(*last_inner_map);
        if (*last_inner_map == *it->second) {
          // do nothing
        } else {
          share::ObTaskController::get().allow_next_syslog();
          LOG_WARN("failed to verify app ctx info", K(ret),
          "current_inner_map", it->second,
          "last_inner_map", last_inner_map);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    LOG_TRACE("success to verify app ctx info", K(ret));
  }

  return ret;
}

int ObSequenceCurrvalEncoder::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  //serialize currval map
  ObSequenceCurrvalMap &map = sess.get_sequence_currval_map();
  OB_UNIS_ENCODE(map.size());
  int64_t count = 0;
  for (auto it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it, ++count) {
    OB_UNIS_ENCODE(it->first);
    OB_UNIS_ENCODE(it->second);
  }
  CK (count == map.size());
  OB_UNIS_ENCODE(sess.get_current_dblink_sequence_id());
  //serialize dblink sequence id map
  ObDBlinkSequenceIdMap &id_map = sess.get_dblink_sequence_id_map();
  OB_UNIS_ENCODE(id_map.size());
  count = 0;
  for (auto it = id_map.begin(); OB_SUCC(ret) && it != id_map.end(); ++it, ++count) {
    OB_UNIS_ENCODE(it->first.sequence_name_);
    OB_UNIS_ENCODE(it->first.dblink_id_);
    OB_UNIS_ENCODE(it->second);
  }
  CK (count == id_map.size());
  return ret;
}

int ObSequenceCurrvalEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t map_size = 0;
  int64_t current_id = 0;
  //deserialize currval map
  OB_UNIS_DECODE(map_size);
  ObSequenceCurrvalMap &map = sess.get_sequence_currval_map();
  OX (sess.reuse_all_sequence_value());
  uint64_t seq_id = 0;
  ObSequenceValue seq_val;
  for (int64_t i = 0; OB_SUCC(ret) && i < map_size; ++i) {
    OB_UNIS_DECODE(seq_id);
    OB_UNIS_DECODE(seq_val);
    OZ (map.set_refactored(seq_id, seq_val, true /*overwrite_exits*/));
  }
  OB_UNIS_DECODE(current_id);
  sess.set_current_dblink_sequence_id(current_id);
  //deserialize dblink seuqnce id map
  OB_UNIS_DECODE(map_size);
  ObDBlinkSequenceIdMap &id_map = sess.get_dblink_sequence_id_map();
  ObDBlinkSequenceIdKey key;
  seq_id = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < map_size; ++i) {
    OB_UNIS_DECODE(key.sequence_name_);
    OB_UNIS_DECODE(key.dblink_id_);
    OB_UNIS_DECODE(seq_id);
    OZ (id_map.set_refactored(key, seq_id, true /*overwrite_exits*/));
  }
  return ret;
}

int ObSequenceCurrvalEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const
{
  int ret = OB_SUCCESS;
  ObSequenceCurrvalMap &map = sess.get_sequence_currval_map();
  OB_UNIS_ADD_LEN(map.size());
  for (auto it = map.begin(); it != map.end(); ++it) {
    OB_UNIS_ADD_LEN(it->first);
    OB_UNIS_ADD_LEN(it->second);
  }
  OB_UNIS_ADD_LEN(sess.get_current_dblink_sequence_id());
  ObDBlinkSequenceIdMap &id_map = sess.get_dblink_sequence_id_map();
  OB_UNIS_ADD_LEN(id_map.size());
  for (auto it = id_map.begin(); it != id_map.end(); ++it) {
    OB_UNIS_ADD_LEN(it->first.sequence_name_);
    OB_UNIS_ADD_LEN(it->first.dblink_id_);
    OB_UNIS_ADD_LEN(it->second);
  }
  return ret;
}

int ObSequenceCurrvalEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf,
                                              const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize(sess, buf, length, pos))) {
    LOG_WARN("failed to fetch session info.", K(ret), K(pos), K(length));
  }
  return ret;
}

int64_t ObSequenceCurrvalEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t len = 0;
  get_serialize_size(sess, len);
  return len;
}

int ObSequenceCurrvalEncoder::compare_sess_info(const char* current_sess_buf,
                                                int64_t current_sess_length,
                                                const char* last_sess_buf,
                                                int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  if (current_sess_length != last_sess_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare session info", K(ret), K(current_sess_length), K(last_sess_length),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  } else if (memcmp(current_sess_buf, last_sess_buf, current_sess_length) == 0) {
    LOG_TRACE("success to compare session info", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compare buf session info", K(ret),
      KPHEX(current_sess_buf, current_sess_length), KPHEX(last_sess_buf, last_sess_length));
  }
  return ret;
}

int ObSequenceCurrvalEncoder::display_sess_info(ObSQLSessionInfo &sess,
                                                const char* current_sess_buf,
                                                int64_t current_sess_length,
                                                const char* last_sess_buf,
                                                int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  const char *buf = last_sess_buf;
  int64_t pos = 0;
  int64_t data_len = last_sess_length;
  int64_t map_size = 0;
  int64_t current_id = 0;
  bool found_mismatch = false;
  OB_UNIS_DECODE(map_size);
  ObSequenceCurrvalMap &map = sess.get_sequence_currval_map();
  if (map_size != map.size()) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_WARN("Sequence currval map size mismatch", K(ret), "current_map_size", map.size(),
             "last_map_size", map_size);
  } else {
    uint64_t seq_id = 0;
    ObSequenceValue seq_val_decode;
    ObSequenceValue seq_val_origin;
    for (int64_t i = 0; OB_SUCC(ret) && !found_mismatch && i < map_size; ++i) {
      OB_UNIS_DECODE(seq_id);
      OB_UNIS_DECODE(seq_val_decode);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(map.get_refactored(seq_id, seq_val_origin))) {
          if (ret == OB_HASH_NOT_EXIST) {
            found_mismatch = true;
            share::ObTaskController::get().allow_next_syslog();
            LOG_WARN("Decoded sequence id not found", K(ret), K(i), K(map_size), K(seq_id));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("Fail to get refactored from map", K(ret), K(seq_id));
          }
        } else if (seq_val_decode.val() != seq_val_origin.val()) {
          found_mismatch = true;
          share::ObTaskController::get().allow_next_syslog();
          LOG_WARN("Sequence currval mismatch", K(ret), K(i), K(map_size), K(seq_id),
                   "current_seq_val", seq_val_origin, "last_seq_val", seq_val_decode);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(current_id);
    if (current_id != sess.get_current_dblink_sequence_id()) {
      found_mismatch = true;
      share::ObTaskController::get().allow_next_syslog();
            LOG_WARN("current dblink sequence id mismatch",
                    "current_seq_id", current_id, "last_seq_id", sess.get_current_dblink_sequence_id());
    }
    OB_UNIS_DECODE(map_size);
    ObDBlinkSequenceIdMap &id_map = sess.get_dblink_sequence_id_map();
    if (map_size != id_map.size()) {
      share::ObTaskController::get().allow_next_syslog();
      LOG_WARN("Sequence id map size mismatch", K(ret), "current_map_size", id_map.size(),
              "last_map_size", map_size);
    } else {
      ObDBlinkSequenceIdKey key;
      uint64_t seq_id_decode = 0;
      uint64_t seq_id_origin = 0;
      for (int64_t i = 0; OB_SUCC(ret) && !found_mismatch && i < map_size; ++i) {
        OB_UNIS_DECODE(key.sequence_name_);
        OB_UNIS_DECODE(key.dblink_id_);
        OB_UNIS_DECODE(seq_id_decode);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(id_map.get_refactored(key, seq_id_origin))) {
            if (ret == OB_HASH_NOT_EXIST) {
              found_mismatch = true;
              share::ObTaskController::get().allow_next_syslog();
              LOG_WARN("Decoded sequence id not found", K(ret), K(i), K(map_size), K(seq_id_decode));
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("Fail to get refactored from map", K(ret), K(seq_id_decode));
            }
          } else if (seq_id_decode != seq_id_origin) {
            found_mismatch = true;
            share::ObTaskController::get().allow_next_syslog();
            LOG_WARN("Sequence id mismatch", K(ret), K(i), K(map_size),
                    "current_seq_id", seq_id_decode, "last_seq_id", seq_id_origin);
          }
        }
      }
      if (OB_SUCC(ret) && !found_mismatch) {
        share::ObTaskController::get().allow_next_syslog();
        LOG_WARN("All sequence currval is matched", K(ret), K(map_size));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObInnerContextMap)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(context_map_));
  OB_UNIS_ENCODE(context_name_);
  OB_UNIS_ENCODE(context_map_->size());
  for (auto it = context_map_->begin(); OB_SUCC(ret) && it != context_map_->end(); ++it) {
    OB_UNIS_ENCODE(*it->second);
  }
  return ret;
}
OB_DEF_DESERIALIZE(ObInnerContextMap)
{
  int ret = OB_SUCCESS;
  ObString tmp_context_name;
  int64_t map_size = 0;
  OB_UNIS_DECODE(tmp_context_name);
  OB_UNIS_DECODE(map_size);
  OZ (ob_write_string(alloc_, tmp_context_name, context_name_));
  OZ (init());
  ObContextUnit tmp_unit;
  for (int64_t i = 0; OB_SUCC(ret) && i < map_size; ++i) {
    ObContextUnit *unit = nullptr;
    OB_UNIS_DECODE(tmp_unit);
    CK (OB_NOT_NULL(unit = static_cast<ObContextUnit *> (alloc_.alloc(sizeof(ObContextUnit)))));
    OZ (unit->deep_copy(tmp_unit.attribute_, tmp_unit.value_, alloc_));
    OZ (context_map_->set_refactored(unit->attribute_, unit));
  }
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObInnerContextMap)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(context_name_);
  if (OB_NOT_NULL(context_map_)) {
    OB_UNIS_ADD_LEN(context_map_->size());
    for (auto it = context_map_->begin(); it != context_map_->end(); ++it) {
      OB_UNIS_ADD_LEN(*it->second);
    }
  }
  return len;
}
OB_DEF_SERIALIZE(ObContextUnit)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              attribute_,
              value_);
  return ret;
}
OB_DEF_DESERIALIZE(ObContextUnit)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              attribute_,
              value_);
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObContextUnit)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              attribute_,
              value_);
  return len;
}

int ObControlInfoEncoder::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sess.get_control_info().serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize control info", K(buf_len), K(pos));
  } else if (OB_FAIL(ObProtoTransUtil::store_int1(buf, buf_len, pos, sess.is_coninfo_set_by_sess(), CONINFO_BY_SESS))) {
    LOG_WARN("failed to store control info set by sess", K(sess.is_coninfo_set_by_sess()), K(pos));
  } else {
    LOG_TRACE("serialize control info", K(sess.get_sessid()), K(sess.get_control_info()));
  }
  return ret;
}

int ObControlInfoEncoder::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  FLTControlInfo con;
  FullLinkTraceExtraInfoType extra_type;
  int32_t v_len = 0;
  int16_t id = 0;
  int8_t setby_sess = 0;
  if (OB_FAIL(FLTExtraInfo::resolve_type_and_len(buf, data_len, pos, extra_type, v_len))) {
    LOG_WARN("failed to resolve type and len", K(data_len), K(pos));
  } else if (extra_type != FLT_TYPE_CONTROL_INFO) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra type", K(extra_type), K(ret));
  } else if (OB_FAIL(con.deserialize(buf, pos+v_len, pos))) {
    LOG_WARN("failed to resolve control info", K(v_len), K(pos));
  } else if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, data_len, pos, id, v_len))) {
    LOG_WARN("failed to get extra_info", K(ret), KP(buf));
  } else if (CONINFO_BY_SESS != id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(id));
  } else if (OB_FAIL(ObProtoTransUtil::get_int1(buf, *(const_cast<int64_t *>(&data_len)), pos, static_cast<int64_t>(v_len), setby_sess))) {
    LOG_WARN("failed to resolve set by sess", K(ret));
  } else {
    sess.set_flt_control_info(con);
    sess.set_coninfo_set_by_sess(static_cast<bool>(setby_sess));
    // if control info not changed or control info not set, not need to feedback
    if (con == sess.get_control_info() || !sess.get_control_info().is_valid()) {
      // not need to feedback
      sess.set_send_control_info(true);
      sess.get_control_info_encoder().is_changed_ = false;
    }

    LOG_TRACE("deserialize control info", K(sess.get_sessid()), K(sess.get_control_info()));
  }
  return ret;
}

int ObControlInfoEncoder::get_serialize_size(ObSQLSessionInfo& sess, int64_t &len) const
{
  int ret = OB_SUCCESS;
  len = sess.get_control_info().get_serialize_size() + 6 + sizeof(bool);
  return ret;
}

int ObControlInfoEncoder::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize(sess, buf, length, pos))) {
    LOG_WARN("failed to fetch session info.", K(ret), K(pos), K(length));
  }
  return ret;
}

int64_t ObControlInfoEncoder::get_fetch_sess_info_size(ObSQLSessionInfo& sess)
{
  int64_t len = 0;
  get_serialize_size(sess, len);
  return len;
}

int ObControlInfoEncoder::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length,
                                            const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  // todo The current control info does not meet the synchronization mechanism and cannot be verified
  return ret;
}

int ObControlInfoEncoder::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf,
            int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length)
{
  int ret = OB_SUCCESS;
  UNUSED(current_sess_buf);
  UNUSED(current_sess_length);
  const char *buf = last_sess_buf;
  int64_t pos = 0;
  int64_t data_len = last_sess_length;
  FLTControlInfo last_sess_con;
  FullLinkTraceExtraInfoType extra_type;
  int32_t v_len = 0;
  int16_t id = 0;
  int8_t last_sess_setby_sess = 0;
  if (OB_FAIL(FLTExtraInfo::resolve_type_and_len(buf, data_len, pos, extra_type, v_len))) {
    LOG_WARN("failed to resolve type and len", K(data_len), K(pos));
  } else if (extra_type != FLT_TYPE_CONTROL_INFO) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra type", K(extra_type), K(ret));
  } else if (OB_FAIL(last_sess_con.deserialize(buf, pos+v_len, pos))) {
    LOG_WARN("failed to resolve control info", K(v_len), K(pos));
  } else if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, data_len, pos, id, v_len))) {
    LOG_WARN("failed to get extra_info", K(ret), KP(buf));
  } else if (CONINFO_BY_SESS != id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(id));
  } else if (OB_FAIL(ObProtoTransUtil::get_int1(buf, *(const_cast<int64_t *>(&data_len)),
                                        pos, static_cast<int64_t>(v_len), last_sess_setby_sess))) {
    LOG_WARN("failed to resolve set by sess", K(ret));
  } else {
    if (sess.is_coninfo_set_by_sess() != last_sess_setby_sess) {
      LOG_WARN("failed to verify control info", K(ret),
        "current_coninfo_set_by_sess", sess.is_coninfo_set_by_sess(),
        "last_coninfo_set_by_sess", last_sess_setby_sess);
    } else if (sess.get_control_info().is_equal(last_sess_con)) {
      LOG_INFO("success to verify control info, no need to attention support_show_trace", K(ret),
                "current_coninfo", sess.get_control_info(),
                "last_coninfo", last_sess_con);
    } else {
      LOG_WARN("failed to verify control info, no need to attention support_show_trace", K(ret),
        "current_coninfo", sess.get_control_info(),
        "last_coninfo", last_sess_con);
    }
  }
  return ret;
}

#define SESS_ENCODER_DELEGATE_TO_TXN(CLS, func)                         \
int CLS::serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos)\
{                                                                     \
  return ObSqlTransControl::serialize_txn_##func##_state(sess, buf, length, pos); \
}                                                                     \
int CLS::deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos) \
{                                                                       \
  return ObSqlTransControl::update_txn_##func##_state(sess, buf, length, pos); \
}                                                                       \
int CLS::get_serialize_size(ObSQLSessionInfo &sess, int64_t &len) const          \
{                                                                       \
  int ret = OB_SUCCESS;                                                 \
  len = ObSqlTransControl::get_txn_##func##_state_serialize_size(sess); \
  return ret;                                                           \
}                                                                        \
int CLS::fetch_sess_info(ObSQLSessionInfo &sess, char *buf, const int64_t data_len, int64_t &pos) \
{                                                                       \
  return ObSqlTransControl::fetch_txn_##func##_state(sess, buf, data_len, pos); \
}                                                                       \
int64_t CLS::get_fetch_sess_info_size(ObSQLSessionInfo &sess)         \
{                                                                       \
  return ObSqlTransControl::get_fetch_txn_##func##_state_size(sess); \
}                                                                       \
int CLS::compare_sess_info(const char* current_sess_buf, int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length) \
{                                                                       \
  return ObSqlTransControl::cmp_txn_##func##_state(current_sess_buf, current_sess_length, last_sess_buf, last_sess_length); \
}                                                                       \
int CLS::display_sess_info(ObSQLSessionInfo &sess, const char* current_sess_buf, int64_t current_sess_length, const char* last_sess_buf, int64_t last_sess_length) \
{                                                                       \
  ObSqlTransControl::display_txn_##func##_state(sess, current_sess_buf, current_sess_length, last_sess_buf, last_sess_length); \
  return OB_SUCCESS;                                                    \
}

SESS_ENCODER_DELEGATE_TO_TXN(ObTxnStaticInfoEncoder, static)
SESS_ENCODER_DELEGATE_TO_TXN(ObTxnDynamicInfoEncoder, dynamic)
SESS_ENCODER_DELEGATE_TO_TXN(ObTxnParticipantsInfoEncoder, parts)
SESS_ENCODER_DELEGATE_TO_TXN(ObTxnExtraInfoEncoder, extra)

#undef SESS_ENCODER_DELEGATE_TO_TXN
// in oracle mode, if the user variable is valid, we use it first
int64_t ObSQLSessionInfo::get_xa_end_timeout_seconds() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t ret_val = 0;
  if (is_oracle_mode()) {
    const ObString xa_timeout_var(transaction::ObXADefault::OB_XA_TIMEOUT_NAME);
    const ObObj *xa_timeout_value = get_user_variable_value(xa_timeout_var);
    if (NULL != xa_timeout_value) {
      int64_t tmp_val = 0;
      if (OB_SUCCESS != (tmp_ret = xa_timeout_value->get_number().cast_to_int64(tmp_val))) {
        LOG_WARN_RET(tmp_ret, "failed to get xa timeout", K(tmp_ret), K(tmp_val));
      } else if (0 >= tmp_val) {
        LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid xa timeout", K(tmp_val));
      } else {
        ret_val = tmp_val;
        LOG_INFO("get xa timeout from user variable", K(ret_val), "session_id", get_sessid());
      }
    } else {
      ret_val = xa_end_timeout_seconds_;
      LOG_INFO("get xa timeout from local", K(ret_val), "session_id", get_sessid());
    }
  } else {
    ret_val = xa_end_timeout_seconds_;
  }
  return ret_val;
}

int ObSQLSessionInfo::set_xa_end_timeout_seconds(int64_t seconds)
{
  int ret = OB_SUCCESS;
  if (0 >= seconds) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(seconds));
  } else {
    if (is_oracle_mode()) {
      const ObString xa_timeout_var(transaction::ObXADefault::OB_XA_TIMEOUT_NAME);
      ObSessionVariable sess_var;
      ObArenaAllocator allocator;
      number::ObNumber value;
      if (OB_FAIL(value.from(seconds, allocator))) {
        LOG_WARN("failed to get number", K(ret));
      } else {
        xa_end_timeout_seconds_ = seconds;
        sess_var.value_.set_number(value);
        sess_var.meta_.set_type(sess_var.value_.get_type());
        sess_var.meta_.set_scale(sess_var.value_.get_scale());
        sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
        sess_var.meta_.set_collation_type(sess_var.value_.get_collation_type());
        if (OB_FAIL(replace_user_variable(xa_timeout_var, sess_var))) {
          LOG_WARN("failed to set variable xa timeout", K(ret), K(seconds));
        } else {
          LOG_INFO("set xa timeout in session", K(seconds), "session_id", get_sessid());
        }
      }
    } else {
      xa_end_timeout_seconds_ = seconds;
    }
  }
  return ret;
}

int ObSQLSessionInfo::add_dblink_sequence_schema(ObSequenceSchema *schema)
{
  int ret = OB_SUCCESS;
  ObSequenceSchema *new_sequence_schema = NULL;
  const ObSequenceSchema* exists_schema = NULL;
  if (OB_ISNULL(schema)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sequence schema is null", K(ret));
  } else if (OB_UNLIKELY(!schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(schema));
  } else if (OB_FAIL(get_dblink_sequence_schema(schema->get_sequence_id(),
                                                 exists_schema))) {
    LOG_WARN("failed to get dblink sequence schema", K(ret));
  } else if (NULL != exists_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sequence exists", KPC(schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(get_session_allocator(),
                                                 *schema,
                                                 new_sequence_schema))) {
    LOG_WARN("alloca sequence schema failed", K(ret));
  } else if (OB_ISNULL(new_sequence_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_sequence_schema), K(ret));
  } else if (OB_FAIL(dblink_sequence_schemas_.push_back(new_sequence_schema))) {
    LOG_WARN("failed to push back sequence schema", K(ret));
  }
  return ret;
}

int ObSQLSessionInfo::get_dblink_sequence_schema(int64_t sequence_id, const ObSequenceSchema* &schema)const
{
  int ret = OB_SUCCESS;
  bool find = false;
  schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < dblink_sequence_schemas_.count(); ++i) {
    ObSequenceSchema *seq_schema = dblink_sequence_schemas_.at(i);
    if (OB_ISNULL(seq_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null schema", K(ret));
    } else if (sequence_id == seq_schema->get_sequence_id()) {
      find = true;
      schema = dblink_sequence_schemas_.at(i);
    }
  }
  LOG_TRACE("get dblink sequence schema", K(sequence_id), K(dblink_sequence_schemas_));
  return ret;
}

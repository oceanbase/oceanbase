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
#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "io/easy_io.h"
#include "rpc/ob_rpc_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_server.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_create_synonym_stmt.h"
#include "sql/resolver/ddl/ob_drop_synonym_stmt.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "lib/checksum/ob_crc64.h"
#include "observer/mysql/obmp_stmt_send_long_data.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;

static const int64_t DEFAULT_XA_END_TIMEOUT_SECONDS = 60; /*60s*/

const char* state_str[] = {
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
      LOG_INFO("revert schema guard success by sql",
          "session_id",
          schema_guard_.get_session_id(),
          K_(tenant_id),
          K_(schema_version));
      reset();
    }
  }
}

ObSQLSessionInfo::ObSQLSessionInfo()
    : ObVersionProvider(),
      ObBasicSessionInfo(),
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
      enable_early_lock_release_(false),
      trans_type_(transaction::ObTransType::UNKNOWN),
      version_provider_(NULL),
      config_provider_(NULL),
      plan_cache_manager_(NULL),
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
      pl_can_retry_(true),
      pl_attach_session_id_(0),
      last_plan_id_(OB_INVALID_ID),
      pl_sync_pkg_vars_(NULL),
      inner_conn_(NULL),
      use_static_typing_engine_(false),
      ctx_mem_context_(nullptr),
      enable_role_array_(),
      in_definer_named_proc_(false),
      priv_user_id_(OB_INVALID_ID),
      xa_end_timeout_seconds_(DEFAULT_XA_END_TIMEOUT_SECONDS),
      is_external_consistent_(false),
      enable_batched_multi_statement_(false),
      saved_tenant_info_(0),
      sort_area_size_(128 * 1024 * 1024),
      last_check_ec_ts_(0),
      xa_last_result_(OB_SUCCESS),
      prelock_(false),
      proxy_version_(0),
      min_proxy_version_ps_(0),
      is_ignore_stmt_(false),
      got_conn_res_(false),
      piece_cache_(NULL)
{}

ObSQLSessionInfo::~ObSQLSessionInfo()
{
  if (NULL != plan_cache_) {
    plan_cache_->dec_ref_count();
    plan_cache_ = NULL;
  }
  destroy(false);
}

int ObSQLSessionInfo::init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid,
    common::ObIAllocator* bucket_allocator, const ObTZInfoMap* tz_info, int64_t sess_create_time, uint64_t tenant_id)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  static const int64_t PS_BUCKET_NUM = 64;
  if (OB_FAIL(ObBasicSessionInfo::init(version, sessid, proxy_sessid, bucket_allocator, tz_info))) {
    LOG_WARN("fail to init basic session info", K(ret));
  } else if (!is_acquire_from_pool() && OB_FAIL(ps_session_info_map_.create(hash::cal_next_prime(PS_BUCKET_NUM),
                                            ObModIds::OB_HASH_BUCKET_PS_SESSION_INFO,
                                            ObModIds::OB_HASH_NODE_PS_SESSION_INFO))) {
    LOG_WARN("create ps session info map failed", K(ret));
  } else if (!is_acquire_from_pool() && OB_FAIL(ps_name_id_map_.create(hash::cal_next_prime(PS_BUCKET_NUM),
                                            ObModIds::OB_HASH_BUCKET_PS_SESSION_INFO,
                                            ObModIds::OB_HASH_NODE_PS_SESSION_INFO))) {
    LOG_WARN("create ps name id map failed", K(ret));
  } else if (!is_acquire_from_pool() &&
             OB_FAIL(sequence_currval_map_.create(
                 hash::cal_next_prime(32), ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    LOG_WARN("create sequence current value map failed", K(ret));
  } else {
    if (is_obproxy_mode()) {
      sess_create_time_ = sess_create_time;
    } else {
      sess_create_time_ = ObTimeUtility::current_time();
    }
    const char* sup_proxy_min_version = "1.8.4";
    min_proxy_version_ps_ = 0;
    if (OB_FAIL(ObClusterVersion::get_version(sup_proxy_min_version, min_proxy_version_ps_))) {
      LOG_WARN("failed to get version", K(ret));
    } else {
      is_inited_ = true;
      refresh_temp_tables_sess_active_time();
    }
  }
  get_trans_desc().set_audit_record(&audit_record_);
  return ret;
}
// for test
int ObSQLSessionInfo::test_init(
    uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicSessionInfo::test_init(version, sessid, proxy_sessid, bucket_allocator))) {
    LOG_WARN("fail to init basic session info", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSQLSessionInfo::reset(bool skip_sys_var)
{
  if (is_inited_) {
    // ObVersionProvider::reset();
    warnings_buf_.reset();
    show_warnings_buf_.reset();
    end_trans_cb_.reset(), audit_record_.reset();
    user_priv_set_ = 0;
    db_priv_set_ = 0;
    curr_trans_start_time_ = 0;
    curr_trans_last_stmt_time_ = 0;
    sess_create_time_ = 0;
    last_refresh_temp_table_time_ = 0;
    has_temp_table_flag_ = false;
    trans_type_ = transaction::ObTransType::UNKNOWN;
    version_provider_ = NULL;
    config_provider_ = NULL;
    plan_cache_manager_ = NULL;
    if (NULL != ps_cache_) {
      ps_cache_->dec_ref_count();
      ps_cache_ = NULL;
    }
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
    is_remote_session_ = true;
    session_type_ = INVALID_TYPE;
    sequence_currval_map_.reuse();
    pl_can_retry_ = true;
    pl_attach_session_id_ = 0;
    last_plan_id_ = OB_INVALID_ID;
    inner_conn_ = NULL;
    session_stat_.reset();
    pl_sync_pkg_vars_ = NULL;
    ObBasicSessionInfo::reset(skip_sys_var);
    if (ctx_mem_context_ != nullptr) {
      DESTROY_CONTEXT(ctx_mem_context_);
      ctx_mem_context_ = nullptr;
    }
    cached_schema_guard_info_.reset();
    enable_role_array_.reset();
    in_definer_named_proc_ = false;
    priv_user_id_ = OB_INVALID_ID;
    xa_end_timeout_seconds_ = DEFAULT_XA_END_TIMEOUT_SECONDS;
    is_external_consistent_ = false;
    enable_batched_multi_statement_ = false;
    saved_tenant_info_ = 0;
    sort_area_size_ = 128 * 1024 * 1024;
    last_check_ec_ts_ = 0;
    xa_last_result_ = OB_SUCCESS;
    prelock_ = false;
    proxy_version_ = 0;
    min_proxy_version_ps_ = 0;
  }
}

void ObSQLSessionInfo::clean_status()
{
  ObBasicSessionInfo::clean_status();
}

bool ObSQLSessionInfo::is_encrypt_tenant()
{
  bool ret = false;
  return ret;
}

void ObSQLSessionInfo::destroy(bool skip_sys_var)
{
  if (is_inited_) {
    int ret = OB_SUCCESS;
    if (rpc::is_io_thread()) {
      LOG_WARN("free session at IO thread",
          "sessid",
          get_sessid(),
          "proxy_sessid",
          get_proxy_sessid(),
          "version",
          get_version());
    }

    if (false == get_is_deserialized()) {
      if (false == ObSchemaService::g_liboblog_mode_) {
        set_query_start_time(ObTimeUtility::current_time());
        bool has_called_txs_end_trans = false;
        if (OB_FAIL(ObSqlTransControl::end_trans(GCTX.par_ser_, this, has_called_txs_end_trans))) {
          LOG_WARN("fail to rollback transaction",
              K(get_sessid()),
              "proxy_sessid",
              get_proxy_sessid(),
              K(has_called_txs_end_trans),
              K(ret));
        } else if (false == inner_flag_ && false == is_remote_session_) {
          LOG_INFO("end trans successfully",
              "sessid",
              get_sessid(),
              "proxy_sessid",
              get_proxy_sessid(),
              "version",
              get_version(),
              "trans id",
              get_trans_desc().get_trans_id(),
              K(has_called_txs_end_trans));
        }
      }
    }

    if (false == get_is_deserialized()) {
      drop_temp_tables();
      refresh_temp_tables_sess_active_time();
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(close_all_ps_stmt())) {
        LOG_WARN("failed to close all stmt", K(ret));
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

    reset(skip_sys_var);
    is_inited_ = false;
  }
}

int ObSQLSessionInfo::close_ps_stmt(ObPsStmtId client_stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo* ps_sess_info = NULL;
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
      // release resource of ps stmt in any case.
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = remove_ps_session_info(client_stmt_id))) {
        ret = tmp_ret;
        LOG_WARN("remove ps session info failed", K(client_stmt_id), "session_id", get_sessid(), K(ret));
      }
      LOG_TRACE("close ps stmt", K(ret), K(client_stmt_id), K(inner_stmt_id), K(lbt()));
    }
  }
  return ret;
}

int ObSQLSessionInfo::close_all_ps_stmt()
{
  int ret = OB_SUCCESS;
  PsSessionInfoMap::iterator iter = ps_session_info_map_.begin();
  if (OB_ISNULL(ps_cache_)) {
    // do nothing, session no ps
  } else {
    ObPsStmtId inner_stmt_id = OB_INVALID_ID;
    for (; iter != ps_session_info_map_.end(); ++iter) {  // ignore ret
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

int ObSQLSessionInfo::delete_from_oracle_temp_tables(const obrpc::ObDropTableArg& const_drop_table_arg)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql;
  int64_t affect_rows = 0;
  common::ObMySQLProxy* sql_proxy = GCTX.sql_proxy_;
  common::ObCommonSqlProxy* user_sql_proxy;
  common::ObOracleSqlProxy oracle_sql_proxy;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema* database_schema = NULL;
  ObSEArray<const ObSimpleTableSchemaV2*, 512> table_schemas;
  obrpc::ObDropTableArg& drop_table_arg = const_cast<obrpc::ObDropTableArg&>(const_drop_table_arg);
  const share::schema::ObTableType table_type = drop_table_arg.table_type_;
  const uint64_t tenant_id = drop_table_arg.tenant_id_;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed.", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(oracle_sql_proxy.init(sql_proxy->get_pool()))) {
    LOG_WARN("init oracle sql proxy failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id));
  } else {
    user_sql_proxy = &oracle_sql_proxy;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got invalid schema", K(ret), K(i));
      } else if (tenant_id != table_schema->get_tenant_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_id not match", K(ret), K(tenant_id), "table_id", table_schema->get_table_id());
      } else if ((TMP_TABLE_ORA_SESS == table_type && table_schema->is_oracle_tmp_table()) ||
                 (TMP_TABLE_ORA_TRX == table_type && table_schema->is_oracle_trx_tmp_table())) {
        database_schema = NULL;
        if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
          LOG_WARN("failed to get database schema", K(ret));
        } else if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", K(ret));
        } else if (database_schema->is_in_recyclebin() || table_schema->is_in_recyclebin()) {
          LOG_DEBUG("skip table schema in recyclebin", K(*table_schema));
        } else {
          if (0 == drop_table_arg.sess_create_time_) {
            ret = sql.assign_fmt("DELETE FROM \"%.*s\".\"%.*s\" WHERE %s = %ld",
                database_schema->get_database_name_str().length(),
                database_schema->get_database_name_str().ptr(),
                table_schema->get_table_name_str().length(),
                table_schema->get_table_name_str().ptr(),
                OB_HIDDEN_SESSION_ID_COLUMN_NAME,
                drop_table_arg.session_id_);
          } else {
            ret = sql.assign_fmt("DELETE FROM \"%.*s\".\"%.*s\" WHERE %s = %ld AND %s <> %ld",
                database_schema->get_database_name_str().length(),
                database_schema->get_database_name_str().ptr(),
                table_schema->get_table_name_str().length(),
                table_schema->get_table_name_str().ptr(),
                OB_HIDDEN_SESSION_ID_COLUMN_NAME,
                drop_table_arg.session_id_,
                OB_HIDDEN_SESS_CREATE_TIME_COLUMN_NAME,
                drop_table_arg.sess_create_time_);
          }

          if (OB_SUCC(ret)) {
            // THIS_WORKER.set_timeout_ts(get_query_start_time() + 2 * 1000L * 1000L);
            int64_t cur_timeout_backup = THIS_WORKER.get_timeout_ts();
            THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
            if (OB_FAIL(user_sql_proxy->write(tenant_id, sql.ptr(), affect_rows))) {
              LOG_WARN("execute sql failed", K(ret), K(sql));
            } else {
              LOG_INFO("succeed to delete rows in oracle temporary table", K(sql), K(affect_rows));
            }
            THIS_WORKER.set_timeout_ts(cur_timeout_backup);
          }
        }
      }
    }
  }
  return ret;
}
int ObSQLSessionInfo::drop_temp_tables(const bool is_sess_disconn_const, const bool is_xa_trans)
{
  int ret = OB_SUCCESS;
  bool ac = false;
  bool is_sess_disconn = is_sess_disconn_const;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  if (OB_FAIL(get_autocommit(ac))) {
    LOG_WARN("get autocommit error", K(ret), K(ac));
  } else if ((get_has_temp_table_flag() || get_trans_desc().is_trx_level_temporary_table_involved() || is_xa_trans) &&
             (!get_is_deserialized() || ac)) {
    bool need_drop_temp_table = false;
    if (!is_oracle_mode()) {
      if (false == is_obproxy_mode() && is_sess_disconn) {
        need_drop_temp_table = true;
      }
    } else {
      if (false == is_sess_disconn || false == is_obproxy_mode()) {
        need_drop_temp_table = true;
        if (is_sess_disconn && get_is_deserialized() && ac) {
          is_sess_disconn = false;
        }
      }
    }
    if (need_drop_temp_table) {
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
      drop_table_arg.tenant_id_ = get_login_tenant_id();
      common_rpc_proxy = GCTX.rs_rpc_proxy_;
      if (is_oracle_mode() && OB_FAIL(delete_from_oracle_temp_tables(drop_table_arg))) {
        LOG_WARN("failed to delete from oracle temporary table", K(drop_table_arg), K(ret));
      } else if (!is_oracle_mode() && OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg))) {
        LOG_WARN("failed to drop temporary table", K(drop_table_arg), K(ret));
      } else {
        LOG_DEBUG("temporary tables dropped due to connection disconnected", K(is_sess_disconn), K(drop_table_arg));
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::drop_reused_oracle_temp_tables()
{
  int ret = OB_SUCCESS;
  // obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  if (false == get_is_deserialized() && !GCTX.is_standby_cluster()) {
    obrpc::ObDropTableArg drop_table_arg;
    drop_table_arg.if_exist_ = true;
    drop_table_arg.to_recyclebin_ = false;
    drop_table_arg.table_type_ = share::schema::TMP_TABLE_ORA_SESS;
    drop_table_arg.session_id_ = get_sessid_for_table();
    drop_table_arg.tenant_id_ = get_login_tenant_id();
    drop_table_arg.sess_create_time_ = get_sess_create_time();
    // common_rpc_proxy = GCTX.rs_rpc_proxy_;
    if (OB_FAIL(delete_from_oracle_temp_tables(drop_table_arg))) {
      // if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg))) {
      LOG_WARN("failed to drop reused temporary table", K(drop_table_arg), K(ret));
    } else {
      LOG_DEBUG("succeed to delete old rows for oracle temporary table", K(drop_table_arg));
    }
  }
  return ret;
}

void ObSQLSessionInfo::refresh_temp_tables_sess_active_time()
{
  int ret = OB_SUCCESS;
  const int64_t REFRESH_INTERVAL = 60L * 60L * 1000L * 1000L;  // 1hr
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  if (get_has_temp_table_flag() && is_obproxy_mode() && !is_oracle_mode()) {
    int64_t now = ObTimeUtility::current_time();
    obrpc::ObAlterTableRes res;
    if (now - get_last_refresh_temp_table_time() >= REFRESH_INTERVAL) {
      obrpc::ObAlterTableArg alter_table_arg;
      AlterTableSchema* alter_table_schema = &alter_table_arg.alter_table_schema_;
      alter_table_arg.session_id_ = get_sessid_for_table();
      alter_table_schema->alter_type_ = OB_DDL_ALTER_TABLE;
      common_rpc_proxy = GCTX.rs_rpc_proxy_;
      if (OB_FAIL(alter_table_schema->alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ACTIVE_TIME))) {
        LOG_WARN("failed to add member SESSION_ACTIVE_TIME for alter table schema", K(ret));
      } else if (OB_FAIL(common_rpc_proxy->alter_table(alter_table_arg, res))) {
        LOG_WARN(
            "failed to alter temporary table session active time", K(alter_table_arg), K(ret), K(is_obproxy_mode()));
      } else {
        LOG_DEBUG("session active time of temporary tables refreshed",
            K(ret),
            "last refresh time",
            get_last_refresh_temp_table_time());
        set_last_refresh_temp_table_time(now);
      }
    } else {
      LOG_DEBUG("no need to refresh session active time of temporary tables",
          "last refresh time",
          get_last_refresh_temp_table_time());
    }
  }
}

void ObSQLSessionInfo::set_show_warnings_buf(int error_code)
{
  // if error message didn't insert into THREAD warning buffer,
  //    insert it into SESSION warning buffer
  // if no error at all,
  //    clear err.
  if (OB_SUCCESS != error_code && strlen(warnings_buf_.get_err_msg()) <= 0) {
    warnings_buf_.set_error(ob_errpkt_strerror(error_code, lib::is_oracle_mode()), error_code);
  } else if (OB_SUCCESS == error_code) {
    warnings_buf_.reset_err();
  }
  show_warnings_buf_ = warnings_buf_;
}

void ObSQLSessionInfo::get_session_priv_info(share::schema::ObSessionPrivInfo& session_priv) const
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

ObPlanCache* ObSQLSessionInfo::get_plan_cache()
{
  if (OB_LIKELY(NULL != plan_cache_manager_)) {
    if (OB_NOT_NULL(plan_cache_)) {
      // do nothing
    } else {
      // release old plancache and get new
      if (NULL != plan_cache_) {
        plan_cache_->dec_ref_count();
      }
      ObPCMemPctConf pc_mem_conf;
      if (OB_SUCCESS != get_pc_mem_conf(pc_mem_conf)) {
        LOG_ERROR("fail to get pc mem conf");
        plan_cache_ = NULL;
      } else {
        uint64_t tenant_id = lib::current_resource_owner_id();
        if (tenant_id > OB_SYS_TENANT_ID && tenant_id <= OB_MAX_RESERVED_TENANT_ID) {
          // all virtual tenants use sys tenant's plan cache
          tenant_id = OB_SYS_TENANT_ID;
        }
        plan_cache_ = plan_cache_manager_->get_or_create_plan_cache(tenant_id, pc_mem_conf);
        if (NULL == plan_cache_) {
          LOG_WARN("failed to get plan cache");
        }
      }
    }
  } else {
    LOG_WARN("Invalid status", K(plan_cache_), K(plan_cache_manager_));
  }
  return plan_cache_;
}

ObPsCache* ObSQLSessionInfo::get_ps_cache()
{
  if (OB_ISNULL(plan_cache_manager_)) {
    LOG_WARN("invalid status", K_(ps_cache), K_(plan_cache_manager));
  } else if (OB_NOT_NULL(ps_cache_) && ps_cache_->is_valid()) {
    // do nothing
  } else {
    int ret = OB_SUCCESS;
    if (NULL != ps_cache_) {
      ps_cache_->dec_ref_count();
    }
    const uint64_t tenant_id = lib::current_resource_owner_id();
    ObPCMemPctConf pc_mem_conf;
    ObMemAttr mem_attr;
    mem_attr.label_ = "PsSessionInfo";
    mem_attr.tenant_id_ = tenant_id;
    mem_attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    if (OB_FAIL(get_pc_mem_conf(pc_mem_conf))) {
      LOG_ERROR("failed to get pc mem conf");
      ps_cache_ = NULL;
    } else {
      ps_cache_ = plan_cache_manager_->get_or_create_ps_cache(tenant_id, pc_mem_conf);
      if (OB_ISNULL(ps_cache_)) {
        LOG_WARN("failed to get ps pl an cache");
      } else {
        ps_session_info_allocator_.set_attr(mem_attr);
      }
    }
  }
  return ps_cache_;
}

// whether the user has the super privilege
bool ObSQLSessionInfo::has_user_super_privilege() const
{
  int ret = false;
  if (OB_PRIV_HAS_ANY(user_priv_set_, OB_PRIV_SUPER)) {
    ret = true;
  }
  return ret;
}

// whether the user has the process privilege
bool ObSQLSessionInfo::has_user_process_privilege() const
{
  int ret = false;
  if (OB_PRIV_HAS_ANY(user_priv_set_, OB_PRIV_PROCESS)) {
    ret = true;
  }
  return ret;
}

// check tenant read_only
int ObSQLSessionInfo::check_global_read_only_privilege(const bool read_only, const ObSqlTraits& sql_traits)
{
  int ret = OB_SUCCESS;
  if (!has_user_super_privilege() && !is_tenant_changed() && read_only) {
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
      if (sql_traits.is_commit_stmt_ && trans_flags_.has_exec_write_stmt()) {
        ret = OB_ERR_OPTION_PREVENTS_STATEMENT;

        LOG_WARN("the server is running with read_only, cannot execute stmt");
      }
    }
  }
  return ret;
}

int ObSQLSessionInfo::remove_prepare(const ObString& ps_name)
{
  int ret = OB_SUCCESS;
  ObPsStmtId ps_id = OB_INVALID_ID;
  if (OB_FAIL(ps_name_id_map_.erase_refactored(ps_name, &ps_id))) {
    LOG_WARN("ps session info not exist", K(ps_name));
  } else if (OB_INVALID_ID == ps_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSQLSessionInfo::get_prepare_id(const ObString& ps_name, ObPsStmtId& ps_id) const
{
  int ret = OB_SUCCESS;
  ps_id = OB_INVALID_ID;
  if (OB_FAIL(ps_name_id_map_.get_refactored(ps_name, ps_id))) {
    LOG_WARN("get ps session info failed", K(ps_name));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_EER_UNKNOWN_STMT_HANDLER;
    }
  } else if (OB_INVALID_ID == ps_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps info is null", K(ret), K(ps_name));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSQLSessionInfo::add_prepare(const ObString& ps_name, ObPsStmtId ps_id)
{
  int ret = OB_SUCCESS;
  ObString stored_name;
  ObPsStmtId exist_ps_id = OB_INVALID_ID;
  if (OB_FAIL(name_pool_.write_string(ps_name, &stored_name))) {
    LOG_WARN("failed to copy name", K(ps_name), K(ps_id), K(ret));
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

int ObSQLSessionInfo::get_ps_session_info(const ObPsStmtId stmt_id, ObPsSessionInfo*& ps_session_info) const
{
  int ret = OB_SUCCESS;
  ps_session_info = NULL;
  if (OB_FAIL(ps_session_info_map_.get_refactored(stmt_id, ps_session_info))) {
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
  LOG_TRACE("remove ps session info", K(ret), K(stmt_id), K(get_sessid()),
            K(lbt()));
  if (OB_FAIL(ps_session_info_map_.erase_refactored(stmt_id, &session_info))) {
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

int ObSQLSessionInfo::prepare_ps_stmt(const ObPsStmtId inner_stmt_id, const ObPsStmtInfo* stmt_info,
    ObPsStmtId& client_stmt_id, bool& already_exists, bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo* session_info = NULL;
  const bool is_new_proxy = ((is_obproxy_mode() && proxy_version_ >= min_proxy_version_ps_) || !is_obproxy_mode());
  if (is_new_proxy && !is_inner_sql) {
    client_stmt_id = ++next_client_ps_stmt_id_;
  } else {
    client_stmt_id = inner_stmt_id;
  }
  already_exists = false;
  if (is_inner_sql) {
    LOG_TRACE("is inner sql no need to add session info",
        K(proxy_version_),
        K(min_proxy_version_ps_),
        K(inner_stmt_id),
        K(client_stmt_id),
        K(next_client_ps_stmt_id_),
        K(is_new_proxy),
        K(is_inner_sql));
  } else {
    ret = ps_session_info_map_.get_refactored(client_stmt_id, session_info);
    LOG_TRACE("will add session info",
        K(proxy_version_),
        K(min_proxy_version_ps_),
        K(inner_stmt_id),
        K(client_stmt_id),
        K(next_client_ps_stmt_id_),
        K(is_new_proxy),
        K(ret),
        K(is_inner_sql));
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
      char* buf = static_cast<char*>(ps_session_info_allocator_.alloc(sizeof(ObPsSessionInfo)));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_ISNULL(stmt_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt info is null", K(ret), K(stmt_info));
      } else {
        session_info = new (buf) ObPsSessionInfo(stmt_info->get_num_of_param());
        session_info->set_stmt_id(client_stmt_id);
        session_info->set_stmt_type(stmt_info->get_stmt_type());
        session_info->set_ps_stmt_checksum(stmt_info->get_ps_stmt_checksum());
        session_info->set_inner_stmt_id(inner_stmt_id);
        LOG_TRACE("add ps session info",
            K(stmt_info->get_ps_sql()),
            K(stmt_info->get_ps_stmt_checksum()),
            K(client_stmt_id),
            K(inner_stmt_id),
            K(get_sessid()));
      }
      if (OB_SUCC(ret) && stmt::T_CALL_PROCEDURE == stmt_info->get_stmt_type() && stmt_info->has_complex_argument()) {
        ret = OB_NOT_SUPPORTED;
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
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get ps session failed", K(ret), K(client_stmt_id), K(inner_stmt_id));
    }
  }
  return ret;
}

int ObSQLSessionInfo::get_inner_ps_stmt_id(ObPsStmtId cli_stmt_id, ObPsStmtId& inner_stmt_id)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo* ps_session_info = NULL;
  if (OB_FAIL(ps_session_info_map_.get_refactored(cli_stmt_id, ps_session_info))) {
    LOG_WARN("get inner ps stmt id failed", K(ret), K(cli_stmt_id), K(lbt()));
  } else if (OB_ISNULL(ps_session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps session info is null", K(cli_stmt_id), "session_id", get_sessid(), K(ret));
  } else {
    inner_stmt_id = ps_session_info->get_inner_stmt_id();
  }
  return ret;
}

int ObSQLSessionInfo::check_read_only_privilege(const bool read_only, const ObSqlTraits& sql_traits)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_global_read_only_privilege(read_only, sql_traits))) {
    LOG_WARN("failed to check global read_only privilege!", K(ret));
  } else if (OB_FAIL(check_tx_read_only_privilege(sql_traits))) {
    LOG_WARN("failed to check tx_read_only privilege!", K(ret));
  }
  return ret;
}

ObTraceEventRecorder* ObSQLSessionInfo::get_trace_buf()
{
  if (NULL == trace_recorder_) {
    void* ptr = name_pool_.alloc(sizeof(ObTraceEventRecorder));
    if (NULL == ptr) {
      LOG_WARN("fail to alloc trace recorder");
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

OB_SERIALIZE_MEMBER((ObSQLSessionInfo, ObBasicSessionInfo), thread_data_.cur_query_start_time_, user_priv_set_,
    db_priv_set_, trans_type_, global_sessid_, inner_flag_, is_max_availability_mode_, session_type_,
    has_temp_table_flag_, enable_early_lock_release_, use_static_typing_engine_, enable_role_array_,
    in_definer_named_proc_, priv_user_id_, xa_end_timeout_seconds_, prelock_, proxy_version_, min_proxy_version_ps_);

int ObSQLSessionInfo::get_collation_type_of_names(const ObNameTypeClass type_class, ObCollationType& cs_type) const
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
    const ObString& name, const ObString& name_other, const ObNameTypeClass type_class, bool& is_equal) const
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
  ObSQLSessionInfo::LockGuard lock_guard(get_thread_data_lock());
  update_last_active_time();
  LOG_INFO("kill query", K(get_sessid()), K(get_proxy_sessid()), K(get_current_query_string()));
  set_session_state(QUERY_KILLED);
  return OB_SUCCESS;
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
        LOG_WARN("init piece cache fail");
      }
    }
  }
  return piece_cache_;
}

ObAuditRecordData& ObSQLSessionInfo::get_audit_record()
{
  audit_record_.try_cnt_++;
  return audit_record_;
}

ObAuditRecordData &ObSQLSessionInfo::get_raw_audit_record()
{
  return audit_record_;
}

const ObAuditRecordData &ObSQLSessionInfo::get_raw_audit_record() const
{
  return audit_record_;
}

const ObAuditRecordData& ObSQLSessionInfo::get_final_audit_record(ObExecuteMode mode)
{
  int ret = OB_SUCCESS;
  const uint64_t* trace_id = ObCurTraceId::get();
  if (NULL != trace_id) {
    MEMCPY(audit_record_.trace_id_, trace_id, sizeof(uint64_t) * 2);
  }
  audit_record_.request_type_ = mode;
  audit_record_.session_id_ = get_sessid();
  audit_record_.tenant_id_ = get_priv_tenant_id();
  audit_record_.user_id_ = get_user_id();
  audit_record_.effective_tenant_id_ = get_effective_tenant_id();
  audit_record_.ob_trace_info_ = get_ob_trace_info();
  if (EXECUTE_INNER == mode || EXECUTE_LOCAL == mode || EXECUTE_PS_PREPARE == mode || EXECUTE_PS_EXECUTE == mode) {
    audit_record_.tenant_name_ = const_cast<char*>(get_tenant_name().ptr());
    audit_record_.tenant_name_len_ = min(get_tenant_name().length(), OB_MAX_TENANT_NAME_LENGTH);
    audit_record_.user_name_ = const_cast<char*>(get_user_name().ptr());
    audit_record_.user_name_len_ = min(get_user_name().length(), OB_MAX_USER_NAME_LENGTH);
    audit_record_.db_name_ = const_cast<char*>(get_database_name().ptr());
    audit_record_.db_name_len_ = min(get_database_name().length(), OB_MAX_DATABASE_NAME_LENGTH);

    if (EXECUTE_PS_EXECUTE == mode) {
      // do nothing
      // ObMPStmtExecute will save query to audit.
    } else {
      ObString sql = get_current_query_string();
      audit_record_.sql_ = const_cast<char*>(sql.ptr());
      audit_record_.sql_len_ = min(sql.length(), OB_MAX_SQL_LENGTH);
    }
    audit_record_.request_memory_used_ = THIS_WORKER.get_sql_arena_allocator().total();

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
    audit_record_.db_id_ = 0;
    audit_record_.sql_ = NULL;
    audit_record_.sql_len_ = 0;
    audit_record_.request_memory_used_ = THIS_WORKER.get_sql_arena_allocator().total();
  }

  return audit_record_;
}

void ObSQLSessionInfo::update_stat_from_audit_record()
{
  session_stat_.total_logical_read_ +=
      (audit_record_.exec_record_.memstore_read_row_count_ + audit_record_.exec_record_.ssstore_read_row_count_);
  //  session_stat_.total_logical_write_ += 0;
  //  session_stat_.total_physical_read_ += 0;
  session_stat_.total_lock_count_ += (audit_record_.exec_record_.memstore_read_lock_succ_count_ +
                                      audit_record_.exec_record_.memstore_write_lock_succ_count_);
  session_stat_.total_cpu_time_us_ += audit_record_.exec_timestamp_.executor_t_;
  session_stat_.total_exec_time_us_ += audit_record_.exec_timestamp_.elapsed_t_;
}

void ObSQLSessionInfo::update_alive_time_stat()
{
  session_stat_.total_alive_time_us_ = ObTimeUtility::current_time() - sess_create_time_;
  ;
}

void ObSQLSessionInfo::reset_audit_record()
{
  MEMSET(&audit_record_, 0, sizeof(audit_record_));
}

void ObSQLSessionInfo::set_session_type_with_flag()
{
  if (OB_UNLIKELY(INVALID_TYPE == session_type_)) {
    LOG_WARN("session type is not init, only happen when old server send rpc to new server");
    session_type_ = inner_flag_ ? INNER_SESSION : USER_SESSION;
  }
}

void ObSQLSessionInfo::set_early_lock_release(bool enable)
{
  enable_early_lock_release_ = enable;
  if (enable) {
    SQL_SESSION_LOG(
        DEBUG, "set early lock release success", "sessid", get_sessid(), "proxy_sessid", get_proxy_sessid());
  }
}

int ObSQLSessionInfo::replace_user_variables(const ObSessionValMap& user_var_map)
{
  return ObBasicSessionInfo::replace_user_variables(user_var_map);
}

int ObSQLSessionInfo::replace_user_variables(ObExecContext& ctx, const ObSessionValMap& user_var_map)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  OZ(ObBasicSessionInfo::replace_user_variables(user_var_map));
  return ret;
}

int ObSQLSessionInfo::get_sequence_value(uint64_t tenant_id, uint64_t seq_id, share::ObSequenceValue& value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == seq_id)) {
    LOG_WARN("invalid args", K(tenant_id), K(seq_id), K(ret));
  } else if (OB_FAIL(sequence_currval_map_.get_refactored(seq_id, value))) {
    LOG_WARN("fail get seq", K(tenant_id), K(seq_id), K(ret));
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_HASH_NOT_EXIST, "sequence is not yet defined in this session");
    }
  } else {
    // ok
  }
  return ret;
}

int ObSQLSessionInfo::set_sequence_value(uint64_t tenant_id, uint64_t seq_id, const ObSequenceValue& value)
{
  int ret = OB_SUCCESS;
  const bool overwrite_exits = true;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == seq_id)) {
    LOG_WARN("invalid args", K(tenant_id), K(seq_id), K(ret));
  } else if (OB_FAIL(sequence_currval_map_.set_refactored(seq_id, value, overwrite_exits))) {
    LOG_WARN("fail get seq", K(tenant_id), K(seq_id), K(ret));
  } else {
    // ok
  }
  return ret;
}

int ObSQLSessionInfo::save_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OZ(save_basic_session(saved_value));
  OZ(save_sql_session(saved_value));
  return ret;
}

int ObSQLSessionInfo::save_sql_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OX(MEMCPY(&saved_value.audit_record_, &audit_record_, sizeof(audit_record_)));
  OX(audit_record_.reset());
  OX(saved_value.inner_flag_ = inner_flag_);
  OX(saved_value.session_type_ = session_type_);
  OX(saved_value.read_uncommited_ = read_uncommited_);
  OX(saved_value.use_static_typing_engine_ = use_static_typing_engine_);
  OX(saved_value.is_ignore_stmt_ = is_ignore_stmt_);
  OX(set_inner_session());
  return ret;
}

int ObSQLSessionInfo::restore_sql_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OX(session_type_ = saved_value.session_type_);
  OX(inner_flag_ = saved_value.inner_flag_);
  OX(read_uncommited_ = saved_value.read_uncommited_);
  OX(use_static_typing_engine_ = saved_value.use_static_typing_engine_);
  OX(is_ignore_stmt_ = saved_value.is_ignore_stmt_);
  OX(MEMCPY(&audit_record_, &saved_value.audit_record_, sizeof(audit_record_)));
  return ret;
}

int ObSQLSessionInfo::restore_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OZ(restore_sql_session(saved_value));
  OZ(restore_basic_session(saved_value));
  return ret;
}

int ObSQLSessionInfo::begin_nested_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables)
{
  int ret = OB_SUCCESS;
  OV(nested_count_ >= 0, OB_ERR_UNEXPECTED, nested_count_);
  OZ(ObBasicSessionInfo::begin_nested_session(saved_value, skip_cur_stmt_tables));
  OZ(save_sql_session(saved_value));
  OX(nested_count_++);
  LOG_DEBUG("begin_nested_session", K(ret), K_(nested_count));
  return ret;
}

int ObSQLSessionInfo::end_nested_session(StmtSavedValue& saved_value)
{
  int ret = OB_SUCCESS;
  OV(nested_count_ > 0, OB_ERR_UNEXPECTED, nested_count_);
  OX(nested_count_--);
  OZ(restore_sql_session(saved_value));
  OZ(ObBasicSessionInfo::end_nested_session(saved_value));
  OX(saved_value.reset());
  return ret;
}

int ObSQLSessionInfo::set_enable_role_array(const ObIArray<uint64_t>& role_id_array)
{
  int ret = OB_SUCCESS;
  ret = enable_role_array_.assign(role_id_array);
  return ret;
}

void ObSQLSessionInfo::refresh_tenant_config()
{
  int ret = OB_SUCCESS;
  const uint64_t effective_tenant_id = get_effective_tenant_id();
  int64_t cur_ts = ObClockGenerator::getClock();
  const bool change_tenant = (saved_tenant_info_ != effective_tenant_id);
  if (change_tenant || cur_ts - last_check_ec_ts_ > 5000000) {
    if (change_tenant) {
      LOG_DEBUG("refresh tenant config where tenant changed", K_(saved_tenant_info), K(effective_tenant_id));
      ATOMIC_STORE(&saved_tenant_info_, effective_tenant_id);
    }
    is_external_consistent_ = transaction::ObTsMgr::get_instance().is_external_consistent(effective_tenant_id);
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(effective_tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      enable_batched_multi_statement_ = tenant_config->ob_enable_batched_multi_statement;
      ATOMIC_STORE(&sort_area_size_, tenant_config->_sort_area_size);
    }
    ATOMIC_STORE(&last_check_ec_ts_, cur_ts);
  }
  UNUSED(ret);
}

int ObSQLSessionInfo::get_tmp_table_size(uint64_t& size)
{
  int ret = OB_SUCCESS;
  const ObBasicSysVar* tmp_table_size = get_sys_var(SYS_VAR_TMP_TABLE_SIZE);
  CK(OB_NOT_NULL(tmp_table_size));
  if (OB_SUCC(ret) && tmp_table_size->get_value().get_uint64() != tmp_table_size->get_max_val().get_uint64()) {
    size = tmp_table_size->get_value().get_uint64();
  } else {
    size = OB_INVALID_SIZE;
  }
  return ret;
}
int ObSQLSessionInfo::ps_use_stream_result_set(bool& use_stream)
{
  int ret = OB_SUCCESS;
  uint64_t size = 0;
  use_stream = false;
  OZ(get_tmp_table_size(size));
  if (OB_SUCC(ret) && OB_INVALID_SIZE == size) {
    use_stream = true;
#if !defined(NDEBUG)
    LOG_INFO("cursor use stream result.");
#endif
  }
  return ret;
}

int ObSQLSessionInfo::on_user_connect(schema::ObSessionPrivInfo& priv_info, const ObUserInfo* user_info)
{
  int ret = OB_SUCCESS;
  ObConnectResourceMgr* conn_res_mgr = GCTX.conn_res_mgr_;
  if (get_is_deserialized()) {
    // do nothing
  } else if (OB_ISNULL(conn_res_mgr) || OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connect resource mgr or user info is null", K(ret), K(conn_res_mgr));
  } else {
    const ObPrivSet& priv = priv_info.user_priv_set_;
    const ObString& user_name = priv_info.user_name_;
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
    if (OB_SUCC(ret) && OB_FAIL(conn_res_mgr->on_user_connect(tenant_id,
                            user_id,
                            priv,
                            user_name,
                            max_connections_per_hour,
                            max_user_connections,
                            max_tenant_connections,
                            *this))) {
      LOG_WARN("create user connection failed", K(ret));
    }
  }
  return ret;
}

int ObSQLSessionInfo::on_user_disconnect()
{
  int ret = OB_SUCCESS;
  ObConnectResourceMgr* conn_res_mgr = GCTX.conn_res_mgr_;
  if (OB_ISNULL(conn_res_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connect resource mgr is null", K(ret));
  } else if (OB_FAIL(conn_res_mgr->on_user_disconnect(*this))) {
    LOG_WARN("user disconnect failed", K(ret));
  }
  return ret;
}
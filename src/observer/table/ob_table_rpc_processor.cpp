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
#include "ob_table_rpc_processor.h"
#include "observer/ob_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/ob_end_trans_callback.h"
#include "ob_table_end_trans_cb.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
#include "sql/ob_sql.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "share/schema/ob_schema_mgr.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/ob_define.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

int ObTableLoginP::process()
{
  int ret = OB_SUCCESS;
  const ObTableLoginRequest &login = arg_;
  if (1 != login.auth_method_
      || 1 != login.client_version_
      || 0 != login.reserved1_
      || 0 != login.reserved2_
      || 0 != login.reserved3_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid login request", K(ret), K(login));
  } else if (1 != login.client_type_ && 2 != login.client_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid client type", K(ret), K(login));
  } else if (login.tenant_name_.empty()
             || login.user_name_.empty()
             || login.database_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or user or database", K(ret), K(login));
  } else if (login.pass_scramble_.length() != 20) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scramble", K(ret), K(login));
  } else if (0 > login.ttl_us_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl_us", K(ret), K(login));
  } else if (SS_STOPPING == GCTX.status_
             || SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("server is stopping", K(GCTX.status_), K(ret), K(login));
  } else if (SS_SERVING != GCTX.status_) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server is not serving", K(GCTX.status_), K(ret), K(login));
  } else {
    // @todo check client_capabilities and max_packet_size
    uint64_t user_token = 0;
    if (OB_FAIL(get_ids())) {
      LOG_WARN("failed to get ids", K(ret), K(login));
    } else if (OB_FAIL(verify_password(login.tenant_name_, login.user_name_,
                                       login.pass_secret_, login.pass_scramble_,
                                       login.database_name_, user_token))) {
      LOG_DEBUG("failed to verify password", K(ret), K(login));
    } else if (OB_FAIL(generate_credential(result_.tenant_id_, result_.user_id_, result_.database_id_,
                                           login.ttl_us_, user_token, result_.credential_))) {
      LOG_WARN("failed to generate credential", K(ret), K(login));
    } else {
      result_.reserved1_ = 0;
      result_.reserved2_ = 0;
      result_.server_version_ = ObString::make_string(PACKAGE_STRING);
    }
  }
  // whether the client should refresh location cache
  if (OB_SUCCESS != ret && is_bad_routing_err(ret)) {
    ObRpcProcessor::bad_routing_ = true;
    LOG_WARN("[TABLE] login bad routing", K(ret), "bad_routing", ObRpcProcessor::bad_routing_);
  }
  ObTenantStatEstGuard stat_guard(result_.tenant_id_);
#ifndef NDEBUG
    LOG_INFO("[TABLE] login", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout());
#else
    // @todo LOG_DEBUG
    LOG_INFO("[TABLE] login", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout(),
              "receive_ts", get_receive_timestamp());
#endif

    if (common::OB_INVALID_ARGUMENT == ret) {
      RPC_OBRPC_LOG(ERROR, "yyy retcode is 4002", "pkt", req_->get_packet());
    }
  EVENT_INC(TABLEAPI_LOGIN_COUNT);
  return ret;
}

int ObTableLoginP::get_ids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid schema service", K(ret), K(gctx_.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard guard;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_id(arg_.tenant_name_, result_.tenant_id_))) {
      LOG_WARN("get_tenant_id failed", K(ret), "tenant", arg_.tenant_name_);
    } else if (OB_INVALID_ID == result_.tenant_id_) {
      ret = OB_ERR_INVALID_TENANT_NAME;
      LOG_WARN("get_tenant_id failed", K(ret), "tenant", arg_.tenant_name_);
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(result_.tenant_id_, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret), "tenant_id", result_.tenant_id_);
    } else if (OB_FAIL(guard.get_database_id(result_.tenant_id_, arg_.database_name_,
                                             result_.database_id_))) {
      LOG_WARN("failed to get database id", K(ret), "database", arg_.database_name_);
    } else if (OB_INVALID_ID == result_.database_id_) {
      ret = OB_WRONG_DB_NAME;
      LOG_WARN("failed to get database id", K(ret), "database", arg_.database_name_);
    } else if (OB_FAIL(guard.get_user_id(result_.tenant_id_, arg_.user_name_,
                                         ObString::make_string("%")/*assume there is no specific host*/,
                                         result_.user_id_))) {
      LOG_WARN("failed to get user id", K(ret), "user", arg_.user_name_);
    } else if (OB_INVALID_ID == result_.user_id_) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("failed to get user id", K(ret), "user", arg_.user_name_);
    }
  }
  return ret;
}

int ObTableLoginP::verify_password(const ObString &tenant, const ObString &user,
                                   const ObString &pass_secret, const ObString &pass_scramble,
                                   const ObString &db, uint64_t &user_token)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid schema service", K(ret), K(gctx_.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard guard;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    share::schema::ObUserLoginInfo login_info;
    login_info.tenant_name_ = tenant;
    login_info.user_name_ = user;
    login_info.scramble_str_ = pass_scramble;
    login_info.db_ = db;
    login_info.passwd_ = pass_secret;
    SSL *ssl_st = NULL;//TODO::@yanhua not support ssl now for table-api
    share::schema::ObSessionPrivInfo session_priv;
    const share::schema::ObUserInfo *user_info = nullptr;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_id(tenant, tenant_id))) {
      LOG_WARN("fail to get tenant_id", KR(ret), K(tenant));
    } else if (gctx_.schema_service_->get_tenant_schema_guard(tenant_id, guard)) {
      LOG_WARN("fail to get tenant guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.check_user_access(login_info, session_priv, ssl_st, user_info))) {
      LOG_WARN("User access denied", K(login_info), K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", K(ret));
    } else {
      user_token = user_info->get_passwd_str().hash();
    }
  }
  return ret;
}


OB_SERIALIZE_MEMBER(ObTableApiCredential,
                    cluster_id_,
                    tenant_id_,
                    user_id_,
                    database_id_,
                    expire_ts_,
                    hash_val_);

ObTableApiCredential::ObTableApiCredential()
  :cluster_id_(0),
   tenant_id_(0),
   user_id_(0),
   database_id_(0),
   expire_ts_(0),
   hash_val_(0)
{

}

ObTableApiCredential::~ObTableApiCredential()
{

}

uint64_t ObTableApiCredential::hash(uint64_t seed /*= 0*/) const
{
  uint64_t hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), seed);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&user_id_, sizeof(user_id_), hash_val);
  hash_val = murmurhash(&database_id_, sizeof(database_id_), hash_val);
  hash_val = murmurhash(&expire_ts_, sizeof(expire_ts_), hash_val);
  return hash_val;
}

int ObTableLoginP::generate_credential(uint64_t tenant_id, uint64_t user_id, uint64_t database,
                                          int64_t ttl_us, uint64_t user_token, ObString &credential_str)
{
  int ret = OB_SUCCESS;
  ObTableApiCredential credential;
  credential.cluster_id_ = GCONF.cluster_id;
  credential.tenant_id_ = tenant_id;
  credential.user_id_ = user_id;
  credential.database_id_ = database;
  if (ttl_us > 0) {
    credential.expire_ts_ = ObTimeUtility::current_time() + ttl_us;
  } else {
    credential.expire_ts_ = 0;
  }
  credential.hash_val_ = credential.hash(user_token);
  int64_t pos = 0;
  if (OB_FAIL(serialization::encode(credential_buf_, CREDENTIAL_BUF_SIZE, pos, credential))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos));
  } else {
    credential_str.assign_ptr(credential_buf_, static_cast<int32_t>(pos));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableApiProcessorBase::ObTableApiProcessorBase(const ObGlobalContext &gctx)
    :gctx_(gctx),
     table_service_(gctx_.table_service_),
     access_service_(MTL(ObAccessService *)),
     location_service_(gctx.location_service_),
     stat_event_type_(-1),
     audit_row_count_(0),
     need_audit_(false),
     request_string_(NULL),
     request_string_len_(0),
     need_retry_in_queue_(false),
     retry_count_(0),
     trans_desc_(NULL),
     did_async_end_trans_(false)
{
  need_audit_ = GCONF.enable_sql_audit;
  trans_state_ptr_ = &trans_state_;
}

void ObTableApiProcessorBase::reset_ctx()
{
  trans_state_ptr_->reset();
  trans_desc_ = NULL;
  did_async_end_trans_ = false;
}

int ObTableApiProcessorBase::get_ls_id(const ObTabletID &tablet_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  int64_t expire_renew_time = INT64_MAX;
  if (OB_FAIL(location_service_->get(MTL_ID(), tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
    LOG_WARN("failed to get ls id", K(ret), K(is_cache_hit));
  }
  return ret;
}

int ObTableApiProcessorBase::check_user_access(const ObString &credential_str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObUserInfo *user_info = NULL;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(serialization::decode(credential_str.ptr(), credential_str.length(), pos, credential_))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), "tenant_id", credential_.tenant_id_);
  } else if (OB_FAIL(schema_guard.get_user_info(credential_.tenant_id_, credential_.user_id_, user_info))) {
    LOG_WARN("fail to get user info", K(ret), K(credential_));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user info is null", K(ret), K(credential_));
  } else {
    const uint64_t user_token = user_info->get_passwd_str().hash();
    uint64_t hash_val = credential_.hash(user_token);
    uint64_t my_cluster_id = GCONF.cluster_id;
    if (hash_val != credential_.hash_val_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("invalid credential", K(ret), K_(credential), K(hash_val));
    } else if (my_cluster_id != credential_.cluster_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("invalid credential cluster id", K(ret), K_(credential), K(my_cluster_id));
    } else if (user_info->get_is_locked()) { // check whether user is locked.
      ret = OB_ERR_USER_IS_LOCKED;
      LOG_WARN("user is locked", K(ret), K(credential_));
    } else {
      LOG_DEBUG("user can access", K(credential_));
    }
  }
  return ret;
}

oceanbase::sql::ObSQLSessionInfo &session()
{
  static oceanbase::sql::ObSQLSessionInfo SESSION;
  return SESSION;
}

ObArenaAllocator &session_alloc()
{
  static ObArenaAllocator SESSION_ALLOC;
  return SESSION_ALLOC;
}

int ObTableApiProcessorBase::init_session()
{
  int ret = OB_SUCCESS;
  static const uint32_t sess_version = 0;
  static const uint32_t sess_id = 1;
  static const uint64_t proxy_sess_id = 1;

  // ensure allocator is constructed before session to
  // avoid coredump at observer exit
  // https://work.aone.alibaba-inc.com/issue/38530967
  ObArenaAllocator *allocator = &session_alloc();
  oceanbase::sql::ObSQLSessionInfo &sess = session();

  if (OB_FAIL(sess.test_init(sess_version, sess_id, proxy_sess_id, allocator))) {
    LOG_WARN("init session failed", K(ret));
  } else if (OB_FAIL(sess.load_default_sys_variable(false, true))) {
    LOG_WARN("failed to load default sys var", K(ret));
  }
  return ret;
}

int ObTableApiProcessorBase::get_table_id(
    const ObString &table_name,
    const uint64_t arg_table_id,
    uint64_t &real_table_id) const
{
  int ret = OB_SUCCESS;
  real_table_id = arg_table_id;
  if (OB_INVALID_ID == real_table_id || 0 == real_table_id) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const bool is_index = false;
    const int64_t tenant_id = credential_.tenant_id_;
    const int64_t database_id = credential_.database_id_;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_id(tenant_id, database_id, table_name,
        is_index, share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, real_table_id))) {
      LOG_WARN("failed to get table id", K(ret), K(tenant_id), K(database_id), K(table_name));
    } else if (OB_INVALID_ID == real_table_id) {
      ret = OB_ERR_UNKNOWN_TABLE;
      LOG_WARN("get invalid id", K(ret), K(tenant_id), K(database_id), K(table_name));
    }
  }
  return ret;
}

// transaction control
int ObTableApiProcessorBase::get_tablet_by_rowkey(uint64_t table_id, const ObIArray<ObRowkey> &rowkeys,
                                                  ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 1> part_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  SMART_VAR(sql::ObTableLocation, location_calc) {
    const uint64_t tenant_id = MTL_ID();
    const ObTableSchema *table_schema;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (!table_schema->is_partitioned_table()) {
      tablet_ids.push_back(table_schema->get_tablet_id());
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(
                           session(), schema_guard, table_id, rowkeys, tablet_ids, part_ids))) {
      LOG_WARN("failed to calc partition id", K(ret));
    } else {}
  }
  return ret;
}

int ObTableApiProcessorBase::setup_tx_snapshot_(transaction::ObTxDesc &trans_desc,
                                                const bool strong_read,
                                                const share::ObLSID &ls_id,
                                                const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  if (strong_read) {
    if (ls_id.is_valid()) {
      if (OB_FAIL(txs->get_ls_read_snapshot(trans_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, tx_snapshot_))) {
        LOG_WARN("fail to get LS read snapshot", K(ret));
      }
    } else if (OB_FAIL(txs->get_read_snapshot(trans_desc, transaction::ObTxIsolationLevel::RC, timeout_ts, tx_snapshot_))) {
      LOG_WARN("fail to get global read snapshot", K(ret));
    }
  } else {
    int64_t weak_read_snapshot = 0;
    if (OB_FAIL(txs->get_weak_read_snapshot_version(weak_read_snapshot))) {
      LOG_WARN("fail to get weak read snapshot", K(ret));
    } else {
      tx_snapshot_.init_weak_read(weak_read_snapshot);
    }
  }
  return ret;
}

int ObTableApiProcessorBase::start_trans(bool is_readonly, const sql::stmt::StmtType stmt_type,
                                         const ObTableConsistencyLevel consistency_level, uint64_t table_id,
                                         const ObLSID &ls_id, int64_t timeout_ts)
{
  UNUSEDx(stmt_type);
  int ret = OB_SUCCESS;
  NG_TRACE(T_start_trans_begin);

  const uint64_t tenant_id = credential_.tenant_id_;
  bool strong_read = ObTableConsistencyLevel::STRONG == consistency_level;
  if ((!is_readonly) && (ObTableConsistencyLevel::EVENTUAL == consistency_level)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret), K(is_readonly), K(consistency_level));
  }
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);

  // 1. start transaction
  if (OB_SUCC(ret)) {
    transaction::ObTxParam tx_param;
    transaction::ObTxAccessMode access_mode = (is_readonly) ?
      transaction::ObTxAccessMode::RD_ONLY
      : transaction::ObTxAccessMode::RW;
    tx_param.access_mode_ = access_mode;
    tx_param.isolation_ = transaction::ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = ObServerConfig::get_instance().cluster_id;
    tx_param.timeout_us_ = std::max(0l, timeout_ts - ObClockGenerator::getClock());
    if (true == trans_state_ptr_->is_start_trans_executed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start_trans is executed", K(ret));
    } else {
      if (OB_FAIL(txs->acquire_tx(trans_desc_, session().get_sessid()))) {
        LOG_WARN("failed to acquire tx desc", K(ret));
      } else if (OB_FAIL(txs->start_tx(*trans_desc_, tx_param))) {
        LOG_WARN("failed to start trans", K(ret), KPC_(trans_desc));
        txs->release_tx(*trans_desc_);
        trans_desc_ = NULL;
      }
      trans_state_ptr_->set_start_trans_executed(OB_SUCC(ret));
    }
  }
  NG_TRACE(T_start_trans_end);
  // 2. acquire snapshot
  if (OB_SUCC(ret)) {
    if (OB_FAIL(setup_tx_snapshot_(*trans_desc_, strong_read, ls_id, timeout_ts))) {
      LOG_WARN("setup txn snapshot fail", K(ret), KPC_(trans_desc), K(strong_read), K(ls_id), K(timeout_ts));
    }
  }
  return ret;
}

int ObTableApiProcessorBase::end_trans(bool is_rollback, rpc::ObRequest *req, int64_t timeout_ts,
                                       bool use_sync /*=false*/)
{
  int ret = OB_SUCCESS;
  int end_ret = OB_SUCCESS;
  transaction::ObTxExecResult trans_result;
  if (OB_NOT_NULL(req)) {
    req->set_trace_point(rpc::ObRequest::OB_EASY_REQUEST_TABLE_API_END_TRANS);
  }
  if (trans_state_ptr_->is_start_trans_executed() && trans_state_ptr_->is_start_trans_success()) {
    if (OB_FAIL(MTL(transaction::ObTransService*)
                ->collect_tx_exec_result(*trans_desc_, trans_result))) {
      LOG_WARN("fail to get trans_result", KR(ret), KPC_(trans_desc));
    }
  }
  NG_TRACE(T_end_trans_begin);
  if (trans_state_ptr_->is_start_trans_executed() && trans_state_ptr_->is_start_trans_success()) {
    if (trans_desc_->is_rdonly() || use_sync) {
      ret = sync_end_trans(is_rollback, timeout_ts);
    } else {
      if (is_rollback) {
        ret = sync_end_trans(true, timeout_ts);
      } else {
        ret = async_commit_trans(req, timeout_ts);
      }
    }
    trans_state_ptr_->clear_start_trans_executed();
  }
  trans_state_ptr_->reset();
  NG_TRACE(T_end_trans_end);
  return ret;
}

int ObTableApiProcessorBase::sync_end_trans(bool is_rollback, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;

  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  const int64_t stmt_timeout_ts = timeout_ts;
  if (is_rollback) {
    if (OB_FAIL(txs->rollback_tx(*trans_desc_))) {
      LOG_WARN("fail rollback trans when session terminate", K(ret), KPC_(trans_desc));
    }
  } else if (OB_FAIL(txs->commit_tx(*trans_desc_, stmt_timeout_ts))) {
    LOG_WARN("fail commit trans when session terminate",
              K(ret), KPC_(trans_desc), K(stmt_timeout_ts));
  }

  int tmp_ret = ret;
  if (OB_FAIL(txs->release_tx(*trans_desc_))) {
    LOG_ERROR("release tx failed", K(ret), KPC_(trans_desc));
  }
  ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  trans_desc_ = NULL;
  LOG_DEBUG("ObTableApiProcessorBase::sync_end_trans", K(ret), K(is_rollback), K(stmt_timeout_ts));

  return ret;
}

int ObTableApiProcessorBase::async_commit_trans(rpc::ObRequest *req, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  const bool is_rollback = false;
  ObTableAPITransCb* cb = new_callback(req);
  if (NULL == cb) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc callback", K(ret));
  } else {
    ObTableAPITransCb& callback = *cb;
    if (OB_NOT_NULL(req)) {
      req->set_trace_point(rpc::ObRequest::OB_EASY_REQUEST_TABLE_API_ACOM_TRANS);
    }
    callback.set_is_need_rollback(is_rollback);
    callback.set_end_trans_type(sql::ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
    callback.handout();
    callback.set_tx_desc(trans_desc_);
    const int64_t stmt_timeout_ts = timeout_ts;
    // callback won't been called if any error occurred
    if (OB_FAIL(txs->submit_commit_tx(*trans_desc_, stmt_timeout_ts, callback))) {
      LOG_WARN("fail end trans when session terminate", K(ret), KPC_(trans_desc), K(stmt_timeout_ts), KP(&callback));
      callback.callback(ret);
    }
    // ignore the return code of end_trans
    did_async_end_trans_ = true; // don't send response in this worker thread
    // @note the req_ may be freed, req_processor can not be read any more.
    // The req_has_wokenup_ MUST set to be true, otherwise req_processor will invoke req_->set_process_start_end_diff, cause memory core
    // @see ObReqProcessor::run() req_->set_process_start_end_diff(ObTimeUtility::current_time());
    this->set_req_has_wokenup();
    // @note after this code, the callback object can NOT be read any more!!!
    callback.destroy_cb_if_no_ref();
    trans_desc_ = NULL;
    LOG_DEBUG("ObTableApiProcessorBase::async_commit_trans", K(ret), K(stmt_timeout_ts));
  }
  return ret;
}

bool ObTableApiProcessorBase::need_audit() const
{
  return need_audit_;
}

void ObTableApiProcessorBase::start_audit(const rpc::ObRequest *req)
{
  audit_record_.exec_record_.record_start();
  audit_record_.exec_timestamp_.before_process_ts_ = ObTimeUtility::current_time();
  if (OB_LIKELY(NULL != req)) {
    audit_record_.user_client_addr_ = RPC_REQ_OP.get_peer(req);
    audit_record_.trace_id_ = req->get_trace_id();
    audit_record_.exec_timestamp_.rpc_send_ts_ = req->get_send_timestamp();
    audit_record_.exec_timestamp_.receive_ts_ = req->get_receive_timestamp();
    audit_record_.exec_timestamp_.enter_queue_ts_ = req->get_enqueue_timestamp();
    save_request_string();
    generate_sql_id();
  }
}

static int set_audit_name(const char *info_name, char *&audit_name, int64_t &audit_name_length, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info_name)) {
    audit_name = NULL;
    audit_name_length = 0;
  } else {
    const int64_t name_length = strlen(info_name);
    const int64_t buf_size = name_length + 1;
    char *buf = reinterpret_cast<char *>(allocator.alloc(buf_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory", K(ret), K(buf_size));
    } else {
      strcpy(buf, info_name);
      audit_name = buf;
      audit_name_length = name_length;
    }
  }
  return ret;
}

void ObTableApiProcessorBase::end_audit()
{
  audit_record_.exec_record_.record_end();
  // credential info
//  audit_record_.server_addr_; // not necessary, because gv_sql_audit_iterator use local addr automatically
//  audit_record_.client_addr_; // not used for now
  audit_record_.tenant_id_ = credential_.tenant_id_;
  audit_record_.effective_tenant_id_ = credential_.tenant_id_;
  audit_record_.user_id_ = credential_.user_id_;
  audit_record_.db_id_ = credential_.database_id_;

  // update tenant_name, user_name, database_name
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret), "tenant_id", credential_.tenant_id_);
  } else {
    { // set tenant name, ignore ret
      const share::schema::ObSimpleTenantSchema *tenant_info = NULL;
      if(OB_FAIL(schema_guard.get_tenant_info(credential_.tenant_id_, tenant_info))) {
        SERVER_LOG(WARN, "fail to get tenant info", K(ret), K(credential_.tenant_id_));
      } else if (OB_ISNULL(tenant_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "tenant info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(tenant_info->get_tenant_name(),
          audit_record_.tenant_name_, audit_record_.tenant_name_len_, audit_allocator_))){
        SERVER_LOG(WARN, "fail to set tenant name", K(ret), "tenant_name", tenant_info->get_tenant_name());
      }
    }

    { // set user name, ignore ret
      const share::schema::ObUserInfo *user_info = NULL;
      if(OB_FAIL(schema_guard.get_user_info(credential_.tenant_id_, credential_.user_id_, user_info))) {
        SERVER_LOG(WARN, "fail to get user info", K(ret), K(credential_));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "user info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(user_info->get_user_name(),
          audit_record_.user_name_, audit_record_.user_name_len_, audit_allocator_))) {
        SERVER_LOG(WARN, "fail to set user name", K(ret), "user_name", user_info->get_user_name());
      }
    }

    { // set database name, ignore ret
      const share::schema::ObSimpleDatabaseSchema *database_info = NULL;
      if(OB_FAIL(schema_guard.get_database_schema(credential_.tenant_id_, credential_.database_id_, database_info))) {
        SERVER_LOG(WARN, "fail to get database info", K(ret), K(credential_));
      } else if (OB_ISNULL(database_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(database_info->get_database_name(),
          audit_record_.db_name_, audit_record_.db_name_len_, audit_allocator_))) {
        SERVER_LOG(WARN, "fail to set database name", K(ret), "database_name", database_info->get_database_name());
      }
    }
  }

  // append request string to query_sql
  if (NULL != request_string_ && request_string_len_ > 0) {
    static const char request_print_prefix[] = ", \nrequest: ";
    const int64_t buf_size = audit_record_.sql_len_ + sizeof(request_print_prefix) + request_string_len_;
    char *buf = reinterpret_cast<char *>(audit_allocator_.alloc(buf_size));
    if (NULL == buf) {
      SERVER_LOG(WARN, "fail to alloc audit memory", K(buf_size), K(audit_record_.sql_), K(request_string_));
    } else {
      memset(buf, 0, buf_size);
      if (OB_NOT_NULL(audit_record_.sql_)) {
        strcat(buf, audit_record_.sql_);
      }
      strcat(buf, request_print_prefix);
      strcat(buf, request_string_);
      audit_record_.sql_ = buf;
      audit_record_.sql_len_ = buf_size;
      audit_record_.sql_cs_type_ = ObCharset::get_system_collation();
    }
  }

  // request info
  audit_record_.is_executor_rpc_ = false; // FIXME(wenqu): set false for print sql_id
  audit_record_.is_hit_plan_cache_ = false;
  audit_record_.is_inner_sql_ = false;
  audit_record_.is_multi_stmt_ = false;
  audit_record_.partition_cnt_ = 1; // always 1 for now;
  audit_record_.plan_id_ = 0;
  audit_record_.plan_type_ = OB_PHY_PLAN_UNINITIALIZED;

  // in-process info
  audit_record_.execution_id_ = 0; // not used for table api
  audit_record_.request_id_ = 0; // not used for table api
  audit_record_.seq_ = 0; // not used
  audit_record_.session_id_ = 0; // not used  for table api
  audit_record_.exec_timestamp_.exec_type_ = RpcProcessor;

  const int64_t elapsed_time = common::ObTimeUtility::current_time() - audit_record_.exec_timestamp_.receive_ts_;
  if (elapsed_time > GCONF.trace_log_slow_query_watermark) {
    FORCE_PRINT_TRACE(THE_TRACE, "[table api][slow query]");
  }
  // update audit info and push
  audit_record_.exec_timestamp_.net_t_ = audit_record_.exec_timestamp_.receive_ts_ - audit_record_.exec_timestamp_.rpc_send_ts_;
  audit_record_.exec_timestamp_.net_wait_t_ = audit_record_.exec_timestamp_.enter_queue_ts_ - audit_record_.exec_timestamp_.receive_ts_;
  audit_record_.update_stage_stat();

  MTL_SWITCH(credential_.tenant_id_) {
    obmysql::ObMySQLRequestManager *req_manager = MTL(obmysql::ObMySQLRequestManager*);
    if (nullptr == req_manager) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get request manager for current tenant", K(ret));
    } else if (OB_FAIL(req_manager->record_request(audit_record_))) {
      if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
        LOG_DEBUG("cannot allocate mem for record", K(ret));
        ret = OB_SUCCESS;
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) { // in case logging is too frequent
          LOG_WARN("failed to record request info in request manager", K(ret));
        }
      }
    }
  }

}

int ObTableApiProcessorBase::process_with_retry(const ObString &credential, const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  audit_record_.exec_timestamp_.process_executor_ts_ = ObTimeUtility::current_time();
  ObWaitEventStat total_wait_desc;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(check_arg())) {
    LOG_WARN("check arg failed", K(ret));
  } else if (OB_FAIL(check_user_access(credential))) {
    LOG_WARN("check user access failed", K(ret));
  } else {
    ObMaxWaitGuard max_wait_guard(&audit_record_.exec_record_.max_wait_event_);
    ObTotalWaitGuard total_wait_guard(&total_wait_desc);
    ObTenantStatEstGuard stat_guard(credential_.tenant_id_);
    need_retry_in_queue_ = false;
    bool did_local_retry = false;
    do {
      ret = try_process();
      did_local_retry = false;
      // is_partition_change_error(ret) || is_master_changed_error(ret) retry in client
      // OB_SCHEMA_EAGAIN: https://aone.alibaba-inc.com/task/35541305
      if ((OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret || OB_SCHEMA_EAGAIN == ret)
          && retry_policy_.allow_retry()) {
        int64_t now = ObTimeUtility::current_time();
        if (now > timeout_ts) {
          LOG_WARN("process timeout", K(ret), K(now), K(timeout_ts));
          did_local_retry = false;
        } else {
          if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
            // throw to queue and retry
            if (retry_policy_.allow_rpc_retry() && THIS_WORKER.can_retry()) {
              THIS_WORKER.set_need_retry();
              LOG_DEBUG("set retry flag and retry later when lock available");
              need_retry_in_queue_ = true;
            } else {
              // retry in current thread
              did_local_retry = true;
            }
          } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
            EVENT_INC(TABLEAPI_TSC_VIOLATE_COUNT);
            did_local_retry = true;
            // @todo sleep for is_master_changed_error(ret) etc. ?
          } else if (OB_SCHEMA_EAGAIN == ret) {
            // retry in current thread
            did_local_retry = true;
          }
        }
      }
      if (did_local_retry) {
        if (retry_count_ < retry_policy_.max_local_retry_count_) {
          ++retry_count_;
          reset_ctx();
        } else {
          did_local_retry = false;
        }
      }
    } while (did_local_retry);
    // record events
    if (need_audit()) {
      audit_on_finish();
    }
  }
  audit_record_.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
  audit_record_.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
  audit_record_.status_ = ret;
  return ret;
}

// check whether the index type of given table is supported by table api or not.
// global index is not supported by table api. specially, global index in non-partitioned
// table was optimized to local index, which we can support.
int ObTableApiProcessorBase::check_table_index_supported(uint64_t table_id, bool &is_supported)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  is_supported = true;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObSimpleTableSchemaV2 *table_schema = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(ret));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(credential_.tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("get null table schema", K(ret), K(table_id));
  } else if (table_schema->is_partitioned_table()) {
    if (OB_FAIL(schema_guard.check_global_index_exist(credential_.tenant_id_, table_id, exists))) {
      LOG_WARN("fail to check global index", K(ret), K(table_id));
    } else {
      is_supported = !exists;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_BATCH_EXECUTE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE_QUERY> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_QUERY_AND_MUTATE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE_QUERY_SYNC> >;

template<class T>
int ObTableRpcProcessor<T>::deserialize()
{
  if (need_audit()) {
    audit_record_.exec_timestamp_.run_ts_ = ObTimeUtility::current_time();
  }
  return RpcProcessor::deserialize();
}

template<class T>
int ObTableRpcProcessor<T>::before_process()
{
  if (need_audit()) {
    start_audit(RpcProcessor::req_);
  }
  return RpcProcessor::before_process();
}

template<class T>
int ObTableRpcProcessor<T>::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(process_with_retry(RpcProcessor::arg_.credential_, get_timeout_ts()))) {
    if (OB_NOT_NULL(request_string_)) { // request_string_ has been generated if enable sql_audit
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_), K(request_string_));
    } else if (did_async_end_trans()) { // req_ may be freed
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_));
    } else {
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_), "request", RpcProcessor::arg_);
    }
    // whether the client should refresh location cache
    if (is_bad_routing_err(ret)) {
      ObRpcProcessor<T>::bad_routing_ = true;
      LOG_WARN("table_api request bad routing", K(ret), "bad_routing", ObRpcProcessor<T>::bad_routing_);
    }
  }
  return ret;
}

template<class T>
int ObTableRpcProcessor<T>::before_response(int error_code)
{
  if (need_audit()) {
    const int64_t curr_time = ObTimeUtility::current_time();
    audit_record_.exec_timestamp_.executor_end_ts_ = curr_time;
    // timestamp of start get plan, no need for table_api, set euqal to process_executor_ts_
    audit_record_.exec_timestamp_.single_process_ts_ = audit_record_.exec_timestamp_.process_executor_ts_;
    const int64_t elapsed_us = curr_time - RpcProcessor::get_receive_timestamp();
    ObTableRpcProcessorUtil::record_stat(audit_record_, stat_event_type_, elapsed_us, audit_row_count_);
    // todo: distinguish hbase rows and ob rows.
  }
  return RpcProcessor::before_response(error_code);
}

template<class T>
int ObTableRpcProcessor<T>::response(const int retcode)
{
  int ret = OB_SUCCESS;
  // if it is waiting for retry in queue, the response can NOT be sent.
  if (!need_retry_in_queue_) {
    ret = RpcProcessor::response(retcode);
  }
  return ret;
}

template<class T>
int ObTableRpcProcessor<T>::after_process(int error_code)
{
  NG_TRACE(process_end); // print trace log if necessary
  if (need_audit()) {
    end_audit();
  }
  return RpcProcessor::after_process(error_code);
}

template<class T>
void ObTableRpcProcessor<T>::set_req_has_wokenup()
{
  RpcProcessor::req_ = NULL;
}


template<class T>
void ObTableRpcProcessor<T>::save_request_string()
{
  int ret = OB_SUCCESS;
  const char *arg_str = to_cstring(RpcProcessor::arg_);
  if (OB_NOT_NULL(arg_str)) {
    const int64_t buf_size = strlen(arg_str) + 1;
    char *buf = reinterpret_cast<char *>(audit_allocator_.alloc(buf_size));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc audit memory", K(ret), K(buf_size), K(arg_str));
    } else {
      memset(buf, 0, buf_size);
      strcpy(buf, arg_str);
      request_string_ = buf;
      request_string_len_ = buf_size;
    }
  }
}

template<class T>
void ObTableRpcProcessor<T>::generate_sql_id()
{
  uint64_t checksum = get_request_checksum();
  checksum = ob_crc64(checksum, &credential_.tenant_id_, sizeof(credential_.tenant_id_));
  checksum = ob_crc64(checksum, &credential_.user_id_, sizeof(credential_.user_id_));
  checksum = ob_crc64(checksum, &credential_.database_id_, sizeof(credential_.database_id_));
  snprintf(audit_record_.sql_id_, (int32_t)sizeof(audit_record_.sql_id_),
     "TABLEAPI0x%04Xvv%016lX", RpcProcessor::PCODE, checksum);
}

////////////////////////////////////////////////////////////////
ObHTableDeleteExecutor::ObHTableDeleteExecutor(common::ObArenaAllocator &alloc,
                                               uint64_t table_id,
                                               ObTabletID tablet_id,
                                               ObLSID ls_id,
                                               int64_t timeout_ts,
                                               ObTableApiProcessorBase *processor,
                                               ObTableService *table_service,
                                               ObAccessService *access_service)
    :table_service_(table_service),
     access_service_(access_service),
     query_ctx_(alloc),
     mutate_ctx_(alloc)
{
  query_ctx_.param_table_id() = table_id;
  query_ctx_.param_tablet_id() = tablet_id;
  query_ctx_.param_ls_id() = ls_id;
  query_ctx_.init_param(timeout_ts, processor, &alloc,
                        false/*ignored*/, table::ObTableEntityType::ET_HKV,
                        table::ObBinlogRowImageType::MINIMAL/*ignored*/);
  mutate_ctx_.param_table_id() = table_id;
  mutate_ctx_.param_tablet_id() = tablet_id;
  mutate_ctx_.param_ls_id() = ls_id;
  mutate_ctx_.init_param(timeout_ts, processor, &alloc,
                         false/*no affected rows*/, table::ObTableEntityType::ET_HKV,
                         table::ObBinlogRowImageType::MINIMAL/*hbase cell can use put*/);
  mutations_result_.set_entity_factory(&entity_factory_);
}

// @see https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html
int ObHTableDeleteExecutor::htable_delete(const ObTableBatchOperation &batch_operation, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObHTableFilter &htable_filter = query_.htable_filter();
  htable_filter.set_valid(true);
  if (OB_FAIL(query_.add_select_column(ObHTableConstants::ROWKEY_CNAME_STR))) {
    LOG_WARN("failed to add K", K(ret));
  } else if (OB_FAIL(query_.add_select_column(ObHTableConstants::CQ_CNAME_STR))) {
    LOG_WARN("failed to add Q", K(ret));
  } else if (OB_FAIL(query_.add_select_column(ObHTableConstants::VERSION_CNAME_STR))) {
    LOG_WARN("failed to add T", K(ret));
  } else if (OB_FAIL(query_.add_select_column(ObHTableConstants::VALUE_CNAME_STR))) {
    LOG_WARN("failed to add V", K(ret));
  } else {
    query_.set_batch(1);  // mutate for each row
    query_.set_max_result_size(-1);
  }
  ObObj pk_objs_start[3];
  ObObj pk_objs_end[3];
  ObNewRange range;
  range.start_key_.assign(pk_objs_start, 3);
  range.end_key_.assign(pk_objs_end, 3);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  const int64_t N = batch_operation.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)  // for each delete
  {
    const ObTableOperation &del_op = batch_operation.at(i);
    const ObITableEntity &entity = del_op.entity();
    ObHTableCellEntity3 htable_cell(&entity);
    ObString row = htable_cell.get_rowkey();
    if (htable_cell.last_get_is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("K is null", K(ret), K(entity));
      break;
    }
    if (0 == i) {
      // generate scan range by K
      pk_objs_start[0].set_varbinary(row);
      pk_objs_start[1].set_min_value();
      pk_objs_start[2].set_min_value();
      pk_objs_end[0].set_varbinary(row);
      pk_objs_end[1].set_max_value();
      pk_objs_end[2].set_max_value();
      if (OB_FAIL(query_.add_scan_range(range))) {
        LOG_WARN("failed to add range", K(ret));
        break;
      }
    }
    htable_filter.clear_columns();
    ObString qualifier = htable_cell.get_qualifier();
    if (htable_cell.last_get_is_null()) {
      // delete column family, so we need to scan all qualifier
      // wildcard scan
    } else if (OB_FAIL(htable_filter.add_column(qualifier))) {
      LOG_WARN("failed to add column", K(ret));
      break;
    }
    int64_t timestamp = -htable_cell.get_timestamp();        // negative to get the original value
    if (-ObHTableConstants::LATEST_TIMESTAMP == timestamp) {  // INT64_MAX
      // delete the most recently added cell
      htable_filter.set_max_versions(1);
      htable_filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
    } else if (timestamp > 0) {
      // delete the specific version
      htable_filter.set_max_versions(1);
      htable_filter.set_timestamp(timestamp);
    } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) { // -INT64_MAX
      // delete all version
      htable_filter.set_max_versions(INT32_MAX);
      htable_filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
    } else {
      // delete all versions less than or equal to the timestamp
      htable_filter.set_max_versions(INT32_MAX);
      htable_filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, (-timestamp)+1);
    }
    // execute the query
    ObTableQueryResultIterator *result_iterator = nullptr;
    ObTableQueryResult *one_result = nullptr;
    if (OB_FAIL(execute_query(query_, result_iterator))) {
    } else {
      ret = result_iterator->get_next_result(one_result);
      if (OB_ITER_END == ret) {
        // empty
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        LOG_WARN("failed to query", K(ret));
      } else if (OB_ISNULL(one_result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("one_result is NULL", K(ret));
      } else {
        if (OB_FAIL(generate_delete_cells(*one_result, entity_factory_, mutations_))) {
          LOG_WARN("failed to delete cells", K(ret));
        } else if (OB_FAIL(execute_mutation(mutations_, mutations_result_))) {
          LOG_WARN("failed to execute mutations", K(ret));
        } else {
          const int64_t result_num = mutations_result_.count();
          affected_rows += result_num;
        }
      }  // end else
    }
    query_ctx_.reset_query_ctx(access_service_);
    mutate_ctx_.reset_get_ctx();
  }  // end for each delete op
  return ret;
}

int ObHTableDeleteExecutor::execute_query(const table::ObTableQuery &query,
                                          ObTableQueryResultIterator *&result_iterator)
{
  int ret = OB_SUCCESS;
  one_result_.reset();
  if (OB_FAIL(table_service_->execute_query(query_ctx_, query,
                                            one_result_, result_iterator))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute query", K(ret));
    }
  }
  return ret;
}

int ObHTableDeleteExecutor::generate_delete_cells(
    ObTableQueryResult &one_row,
    table::ObTableEntityFactory<table::ObTableEntity> &entity_factory,
    ObTableBatchOperation &mutations_out)
{
  int ret = OB_SUCCESS;
  mutations_out.reset();
  entity_factory.free_and_reuse();
  one_row.rewind();
  ObObj rk, cq, ts;
  ObObj key1, key2, key3;
  // delete all the selected key-values
  const ObITableEntity *key_value = nullptr;
  while (OB_SUCC(ret) && OB_SUCC(one_row.get_next_entity(key_value))) {
    // for each cell of the row
    ObHTableCellEntity2 cell(key_value);
    key1.set_varbinary(cell.get_rowkey());  // K
    key2.set_varbinary(cell.get_qualifier());  // Q
    key3.set_int(-cell.get_timestamp());        // T
    ObITableEntity* new_entity = entity_factory.alloc();
    if (NULL == new_entity) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret));
    } else if (OB_FAIL(new_entity->add_rowkey_value(key1))) {
    } else if (OB_FAIL(new_entity->add_rowkey_value(key2))) {
    } else if (OB_FAIL(new_entity->add_rowkey_value(key3))) {
    } else if (OB_FAIL(mutations_out.del(*new_entity))) {
      LOG_WARN("failed to add delete operation", K(ret));
    } else {
      LOG_DEBUG("[yzfdebug] delete cell", K(ret), "htable_cell", *new_entity, "kv", *key_value);
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHTableDeleteExecutor::execute_mutation(const ObTableBatchOperation &mutations,
                                             ObTableBatchOperationResult &mutations_result)
{
  int ret = OB_SUCCESS;
  mutations_result.reset();
  if (OB_FAIL(table_service_->multi_delete(mutate_ctx_, mutations, mutations_result))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to multi_delete", K(ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObHTablePutExecutor::ObHTablePutExecutor(common::ObArenaAllocator &alloc,
                                         uint64_t table_id,
                                         common::ObTabletID tablet_id,
                                         share::ObLSID ls_id,
                                         int64_t timeout_ts,
                                         ObTableApiProcessorBase *processor,
                                         ObTableService *table_service)
    :table_service_(table_service),
     mutate_ctx_(alloc)
{
  mutate_ctx_.param_table_id() = table_id;
  mutate_ctx_.param_tablet_id() = tablet_id;
  mutate_ctx_.param_ls_id() = ls_id;
  mutate_ctx_.init_param(timeout_ts, processor, &alloc,
                         false/*no affected rows*/, table::ObTableEntityType::ET_HKV,
                         table::ObBinlogRowImageType::MINIMAL/*hbase cell can use put*/);

  mutations_result_.set_entity_factory(&entity_factory_);
}

int ObHTablePutExecutor::htable_put(const ObTableBatchOperation &mutations, int64_t &affected_rows, int64_t now_ms/*=0*/)
{
  int ret = OB_SUCCESS;
  if (0 == now_ms) {
    now_ms = -ObHTableUtils::current_time_millis();
  }
  // store not set timestamp cell for tmp
  ObArray<ObObj*> reset_timestamp_obj;
  //ObString htable_row;
  const int64_t N = mutations.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &mutation = mutations.at(i);
    const ObITableEntity &entity = mutation.entity();
    if (ObTableOperationType::INSERT_OR_UPDATE != mutation.type()) { // for insert_or_update only
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable put should use INSERT_OR_UPDATE", K(ret), K(mutation));
    } else if (entity.get_rowkey_size() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
    } else {
      ObRowkey mutate_rowkey = const_cast<ObITableEntity&>(entity).get_rowkey();
      ObObj &hbase_timestamp = const_cast<ObObj&>(mutate_rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_T]);  // column T
      ObHTableCellEntity3 htable_cell(&entity);
      bool row_is_null = htable_cell.last_get_is_null();
      int64_t timestamp = htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
      } else {
        // update timestamp iff LATEST_TIMESTAMP
        if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) {
          hbase_timestamp.set_int(now_ms);
          reset_timestamp_obj.push_back(&hbase_timestamp);
        }
      }
    }
  }  // end for
  if (OB_SUCC(ret)) {
    // do the multi_put
    mutations_result_.reset();
    affected_rows = 0;
    if (OB_FAIL(table_service_->multi_insert_or_update(mutate_ctx_, mutations, mutations_result_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to multi_delete", K(ret));
      }
      if ( OB_TRANSACTION_SET_VIOLATION == ret ) {
        // When OB_TRANSACTION_SET_VIOLATION happen, there will not refresh timestamp
        // and will cover old row data in processor local retry. so here reset timestamp
        // to origin LATEST_TIMESTAMP in order to retry in local
        for (int64_t i = 0; i < reset_timestamp_obj.count(); i++) {
          reset_timestamp_obj.at(i)->set_int(ObHTableConstants::LATEST_TIMESTAMP);
        }
      }
    } else {
      affected_rows = 1;
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
ObHTableIncrementExecutor::ObHTableIncrementExecutor(table::ObTableOperationType::Type type,
                                                     common::ObArenaAllocator &alloc,
                                                     uint64_t table_id,
                                                     common::ObTabletID tablet_id,
                                                     share::ObLSID ls_id,
                                                     int64_t timeout_ts,
                                                     ObTableApiProcessorBase *processor,
                                                     ObTableService *table_service)
    :type_(type),
     table_service_(table_service),
     mutate_ctx_(alloc)
{
  mutate_ctx_.param_table_id() = table_id;
  mutate_ctx_.param_tablet_id() = tablet_id;
  mutate_ctx_.param_ls_id() = ls_id;
  mutate_ctx_.init_param(timeout_ts, processor, &alloc,
                         false/*no affected rows*/, table::ObTableEntityType::ET_HKV,
                         table::ObBinlogRowImageType::MINIMAL/*hbase cell can use put*/);
  mutations_result_.set_entity_factory(&entity_factory_);
}

class ObHTableIncrementExecutor::ColumnIdxComparator
{
public:
  bool operator()(const ColumnIdx &a, const ColumnIdx &b) const
  {
    return a.first.compare(b.first) < 0;
  }
};


int ObHTableIncrementExecutor::sort_qualifier(const table::ObTableBatchOperation &increment)
{
  int ret = OB_SUCCESS;
  const int64_t N = increment.count();
  if (N <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty increment", K(ret));
  }
  ObString htable_row;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &mutation = increment.at(i);
    const ObITableEntity &entity = mutation.entity();
    if (type_ != mutation.type()) { // increment or append
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should use INCREMENT/APPEND", K(ret), K_(type), K(mutation));
    } else if (entity.get_rowkey_size() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
    } else {
      ObHTableCellEntity3 htable_cell(&entity);
      ObString row = htable_cell.get_rowkey();
      bool row_is_null = htable_cell.last_get_is_null();
      ObString qualifier = htable_cell.get_qualifier();
      bool qualifier_is_null = htable_cell.last_get_is_null();
      (void)htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null || qualifier_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret),
                 K(row_is_null), K(timestamp_is_null), K(qualifier_is_null));
      } else {
        if (0 == i) {
          htable_row = row;  // shallow copy
        } else if (htable_row != row) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rowkey not the same", K(ret), K(row), K(htable_row));
          break;
        }
        if (OB_FAIL(columns_.push_back(std::make_pair(qualifier, i)))) {
          LOG_WARN("failed to push back", K(ret));
          break;
        }
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    // sort qualifiers
    ColumnIdx *end = &columns_.at(columns_.count()-1);
    ++end;
    std::sort(&columns_.at(0), end, ColumnIdxComparator());
  }
  if (OB_SUCC(ret)) {
    // check duplicated qualifiers
    for (int64_t i = 0; OB_SUCCESS == ret && i < N-1; ++i)
    {
      if (columns_.at(i).first == columns_.at(i+1).first) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), "cq", columns_.at(i).first, K(i));
      }
    } // end for
  }
  return ret;
}

int ObHTableIncrementExecutor::htable_increment(ObTableQueryResult &row_cells,
                                                const table::ObTableBatchOperation &increment,
                                                int64_t &affected_rows,
                                                table::ObTableQueryResult *results)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_qualifier(increment))) {
    LOG_WARN("failed to sort qualifier", K(ret));
  }
  row_cells.rewind();
  int64_t now_ms = -ObHTableUtils::current_time_millis();
  ObObj rk, cq, ts;
  const ObITableEntity *get_value = nullptr;
  const int64_t N = increment.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &mutation = increment.at(columns_.at(i).second);
    const ObITableEntity &kv_entity = mutation.entity();
    ObHTableCellEntity3 kv(&kv_entity);
    ObString qualifier = kv.get_qualifier();
    bool need_write = false;
    int64_t delta_int = 0;
    ObString delta_str = kv.get_value();
    bool value_is_null = kv.last_get_is_null();
    if (value_is_null) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("increment value is invalid", K(ret), K(kv_entity));
      break;
    }

    if (type_ == ObTableOperationType::INCREMENT) {
      if (OB_FAIL(ObHTableUtils::java_bytes_to_int64(delta_str, delta_int))) {
        LOG_WARN("failed to convert bytes to integer", K(ret), K(delta_str));
        break;
      } else {
        need_write = (0 != delta_int);
      }
    } else {  // ObTableOperationType::APPEND
      need_write = true;  // always apply for APPEND
    }

    if (nullptr == get_value) {
      if (OB_FAIL(row_cells.get_next_entity(get_value))) {
        if (OB_ITER_END == ret) {
          get_value = nullptr;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next", K(ret));
          break;
        }
      }
    }
    bool first_write = false;
    int64_t orig_ts = -1;
    ObString orig_str;
    if (nullptr != get_value) {
      ObHTableCellEntity2 cell(get_value);
      ObString qualifier2 = cell.get_qualifier();
      int cmp_ret = ObHTableUtils::compare_qualifier(qualifier, qualifier2);
      if (0 == cmp_ret) {
        // qualifier exists
        orig_str = cell.get_value();
        orig_ts = cell.get_timestamp();
        if (type_ == ObTableOperationType::INCREMENT) {
          int64_t orig_int = 0;
          if (OB_FAIL(ObHTableUtils::java_bytes_to_int64(orig_str, orig_int))) {
            LOG_WARN("failed to convert bytes to integer", K(ret), K(orig_str));
            break;
          } else {
            delta_int += orig_int;
          }
        } else {  // APPEND
          // nothing
        }
        get_value = nullptr;  // next cell
      } else {
        // qualifier not exist, first write
        first_write = true;
      }
    } else {
      // no more cells from get
      first_write = true;
    }

    rk.set_varbinary(kv.get_rowkey());  // K
    cq.set_varbinary(qualifier);  // Q
    // generate timestamp
    if (orig_ts >= 0) {
      // already exists
      ts.set_int(std::min(-orig_ts, now_ms));        // T
    } else {
      int64_t new_ts = kv.get_timestamp();
      if (ObHTableConstants::LATEST_TIMESTAMP == new_ts) {
        ts.set_int(now_ms);
      } else {
        ts.set_int(new_ts);
      }
    }

    // generate V
    ObObj value_obj;  // V
    if (type_ == ObTableOperationType::INCREMENT) {
      char* bytes = static_cast<char*>(allocator_.alloc(sizeof(int64_t)));
      if (NULL == bytes) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory", K(ret), KP(bytes));
      } else if (OB_FAIL(ObHTableUtils::int64_to_java_bytes(delta_int, bytes))) {
        LOG_WARN("failed to convert bytes", K(ret), K(delta_int));
      } else {
        ObString v(sizeof(int64_t), bytes);
        value_obj.set_varbinary(v);
      }
    } else {  // APPEND
      if (orig_str.empty()) {
        value_obj.set_varbinary(delta_str);
      } else {
        int32_t total_len = orig_str.length() + delta_str.length();
        char* bytes = static_cast<char*>(allocator_.alloc(total_len));
        if (NULL == bytes) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("no memory", K(ret), KP(bytes));
        } else {
          MEMCPY(bytes, orig_str.ptr(), orig_str.length());
          MEMCPY(bytes+orig_str.length(), delta_str.ptr(), delta_str.length());
          ObString new_str(total_len, bytes);
          value_obj.set_varbinary(new_str);
        }
      }
    }

    // generate entity
    ObITableEntity* new_entity = nullptr;
    if (OB_FAIL(ret)) {
    } else if (nullptr == (new_entity = entity_factory_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret), KP(new_entity));
    } else {
      if (OB_FAIL(new_entity->add_rowkey_value(rk))) {
      } else if (OB_FAIL(new_entity->add_rowkey_value(cq))) {
      } else if (OB_FAIL(new_entity->add_rowkey_value(ts))) {
      } else if (OB_FAIL(new_entity->set_property(ObHTableConstants::VALUE_CNAME_STR, value_obj))) {
      }
    }

    if (OB_SUCC(ret) && (need_write || first_write)) {
      if (OB_FAIL(mutations_.insert_or_update(*new_entity))) {
        LOG_WARN("failed to add put operation", K(ret));
      } else {
        LOG_DEBUG("[yzfdebug] put cell", K(ret), "new_cell", *new_entity, "kv", kv);
      }
    }  // end if need_write
    if (OB_SUCC(ret) && NULL != results) {
      // Add to results to get returned to the Client. If null, cilent does not want results.
      ret = add_to_results(*results, rk, cq, ts, value_obj);
    }
  }  // end for
  if (OB_SUCC(ret) && mutations_.count() > 0) {
    // do the multi_put
    mutations_result_.reset();
    affected_rows = 0;
    if (OB_FAIL(table_service_->multi_insert_or_update(mutate_ctx_, mutations_, mutations_result_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to multi_delete", K(ret));
      }
    } else {
      affected_rows = 1;
    }
  }
  return ret;
}

int ObHTableIncrementExecutor::add_to_results(table::ObTableQueryResult &results,
                                              const ObObj &rk, const ObObj &cq,
                                              const ObObj &ts, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (results.get_property_count() <= 0) {
    if (OB_FAIL(results.add_property_name(ObHTableConstants::ROWKEY_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::CQ_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::VERSION_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::VALUE_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObObj objs[4];
    objs[0] = rk;
    objs[1] = cq;
    objs[2] = ts;
    int64_t timestamp = 0;
    objs[2].get_int(timestamp);
    objs[2].set_int(-timestamp);  // negate_htable_timestamp
    objs[3] = value;
    common::ObNewRow row(objs, 4);
    if (OB_FAIL(results.add_row(row))) {  // deep copy
      LOG_WARN("failed to add row to results", K(ret), K(row));
    }
  }
  return ret;
}

bool oceanbase::observer::is_bad_routing_err(const int err)
{
  // bad routing check : whether client should refresh location cache
  // Now, following the same logic as in ../mysql/ob_query_retry_ctrl.cpp
  return (is_master_changed_error(err)
          || is_server_down_error(err)
          || is_partition_change_error(err)
          || is_server_status_error(err)
          || is_unit_migrate(err)
          || is_transaction_rpc_timeout_err(err)
          || is_has_no_readable_replica_err(err)
          || is_select_dup_follow_replic_err(err)
          || is_trans_stmt_need_retry_error(err));
}

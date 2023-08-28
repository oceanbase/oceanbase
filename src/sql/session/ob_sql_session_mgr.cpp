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

#define USING_LOG_PREFIX SQL
#include "sql/session/ob_sql_session_mgr.h"

#include "lib/string/ob_sql_string.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_resource_limit.h"
#include "io/easy_io.h"
#include "rpc/ob_rpc_define.h"
#include "sql/ob_sql_trans_control.h"
#include "observer/mysql/obsm_handler.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/ob_server_struct.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "sql/session/ob_user_resource_mgr.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::observer;

ObTenantSQLSessionMgr::SessionPool::SessionPool()
  : session_pool_()
{
  MEMSET(session_array_, 0, POOL_CAPACIPY * sizeof(ObSQLSessionInfo *));
}

int ObTenantSQLSessionMgr::SessionPool::init(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t real_cap = capacity;
  if (real_cap > POOL_CAPACIPY) {
    real_cap = POOL_CAPACIPY;
  }
  char *session_buf = reinterpret_cast<char *>(session_array_);
  OZ (session_pool_.init(real_cap, session_buf));
  return ret;
}

int ObTenantSQLSessionMgr::SessionPool::pop_session(ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = NULL;
  if (OB_FAIL(session_pool_.pop(session))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to pop session", K(ret),
               K(session_pool_.get_total()), K(session_pool_.get_free()));
    } else {
      ret = OB_SUCCESS;
      LOG_DEBUG("session pool is empty",
                K(session_pool_.get_total()), K(session_pool_.get_free()));
    }
  }
  return ret;
}

int ObTenantSQLSessionMgr::SessionPool::push_session(ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session)) {
    if (OB_FAIL(session_pool_.push(session))) {
      if (ret != OB_SIZE_OVERFLOW) {
        LOG_WARN("failed to push session", K(ret),
                 K(session_pool_.get_total()), K(session_pool_.get_free()));
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("session pool is full",
                  K(session_pool_.get_total()), K(session_pool_.get_free()));
      }
    } else {
      session = NULL;
    }
  }
  return ret;
}

int64_t ObTenantSQLSessionMgr::SessionPool::count() const
{
  return session_pool_.get_total();
}

ObTenantSQLSessionMgr::ObTenantSQLSessionMgr(const int64_t tenant_id)
  : tenant_id_(tenant_id),
    session_allocator_(lib::ObMemAttr(tenant_id, "SQLSessionInfo"), MTL_CPU_COUNT(), 4)
{}

ObTenantSQLSessionMgr::~ObTenantSQLSessionMgr()
{}

int ObTenantSQLSessionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session_pool_.init(SessionPool::POOL_CAPACIPY))) {
    LOG_WARN("fail to init session pool", K(tenant_id_), K(ret));
  }
  return ret;
}

void ObTenantSQLSessionMgr::destroy()
{
}

int ObTenantSQLSessionMgr::mtl_init(ObTenantSQLSessionMgr *&t_session_mgr)
{
  int ret = OB_SUCCESS;
  t_session_mgr = OB_NEW(ObTenantSQLSessionMgr, ObMemAttr(MTL_ID(), "TSQLSessionMgr"),
                         MTL_ID());
  if (OB_ISNULL(t_session_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tenant session manager", K(ret));
  } else if (OB_FAIL(t_session_mgr->init())) {
    LOG_WARN("failed to init tenant session manager", K(ret));
  }
  return ret;
}

void ObTenantSQLSessionMgr::mtl_destroy(ObTenantSQLSessionMgr *&t_session_mgr)
{
  if (nullptr != t_session_mgr) {
    t_session_mgr->destroy();
    OB_DELETE(ObTenantSQLSessionMgr, "unused", t_session_mgr);
    t_session_mgr = nullptr;
  }
}

ObSQLSessionInfo *ObTenantSQLSessionMgr::alloc_session()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  OX (session_pool_.pop_session(session));
  if (OB_ISNULL(session)) {
    OX (session = op_instance_alloc_args(&session_allocator_,
                                         ObSQLSessionInfo,
                                         tenant_id_));
  }
  OV (OB_NOT_NULL(session));
  OX (session->set_tenant_session_mgr(this));
  OX (session->set_valid(true));
  OX (session->set_shadow(true));
  return session;
}

void ObTenantSQLSessionMgr::free_session(ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  SessionPool *session_pool = NULL;
  // add tracepoint for control session pool.
  int64_t code = 0;
  code = OB_E(EventTable::EN_SESS_POOL_MGR_CTRL) OB_SUCCESS;
  if (ObTenantSQLSessionMgr::is_valid_tenant_id(session->get_login_tenant_id()) &&
      session->can_release_to_pool() && code == OB_SUCCESS) {
    if (session->is_use_inner_allocator() && !session->is_tenant_killed()) {
      session_pool = &session_pool_;
    }
  }
  if (OB_NOT_NULL(session_pool)) {
    OX (session->destroy(true));
    OX (session->set_acquire_from_pool(true));
    OX (session_pool->push_session(session));
  }
  if (OB_NOT_NULL(session)) {
    OX (op_free(session));
    OX (session = NULL);
  }
}

void ObTenantSQLSessionMgr::clean_session_pool()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  // 注意，这里并没有从设计上保证session池一定被彻底清空，有极低概率会有少量遗留session：
  // 1. 在生产系统中，删除租户是非常少见的操作。
  // 2. 在生产系统中，删除租户前一定会先保证业务层不再访问该租户。
  // 以上前提下，理论上该session池已经没有任何操作。
  // 3. 正常停掉某个observer，但即使有少数session遗漏，也会随着进程结束而释放。
  // 4. unit迁移等情况，可能会有少数session未被释放，但将来unit再迁回后可复用。
  while (session_pool_.count() > 0) {
    OX (session_pool_.pop_session(session));
    if (OB_NOT_NULL(session)) {
      OX (op_free(session));
      OX (session = NULL);
    }
  }
}

bool ObTenantSQLSessionMgr::is_valid_tenant_id(uint64_t tenant_id) const
{
  return ::is_valid_tenant_id(tenant_id) &&
    tenant_id != OB_INVALID_ID &&
    tenant_id != OB_SYS_TENANT_ID;
}

int ObSQLSessionMgr::ValueAlloc::clean_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    auto *t_session_mgr = MTL(ObTenantSQLSessionMgr*);
    t_session_mgr->clean_session_pool();
  } else {
    LOG_ERROR("switch tenant failed", K(ret), K(tenant_id));
  }
  return ret;
}

ObSQLSessionInfo *ObSQLSessionMgr::ValueAlloc::alloc_value(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  int64_t alloc_total_count = 0;
  // we use OX instead of OZ in operation of upper session pool, because we need acquire
  // from lower session pool when not success, no matter which errno we get here.
  MTL_SWITCH(tenant_id) {
    auto *t_session_mgr = MTL(ObTenantSQLSessionMgr*);
    if (OB_ISNULL(session = t_session_mgr->alloc_session())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc session", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(GCTX.session_mgr_->get_sess_hold_map()
                  .set_refactored(reinterpret_cast<uint64_t>(session), session))) {
        LOG_WARN("fail to set session", K(ret), KP(session));
      }
    }
    OX (alloc_total_count = ATOMIC_FAA(&alloc_total_count_, 1));
    if (alloc_total_count > 0 && alloc_total_count % 10000 == 0) {
      LOG_INFO("alloc_session_count", K(alloc_total_count));
    }
  } else {
    LOG_ERROR("switch tenant failed", K(ret), K(tenant_id));
  }
  return session;
}

void ObSQLSessionMgr::ValueAlloc::free_value(ObSQLSessionInfo *session)
{
  if (OB_NOT_NULL(session)) {
    int ret = OB_SUCCESS;
    uint64_t tenant_id = session->get_login_tenant_id();
    int64_t free_total_count = 0;
    // delete from hold map, ingore error
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = GCTX.session_mgr_->get_sess_hold_map().erase_refactored(
                                                    reinterpret_cast<uint64_t>(session)))) {
      LOG_WARN("fail to erase session", K(session->get_sessid()), K(tmp_ret), KP(session));
    }
    auto *t_session_mgr = session->get_tenant_session_mgr();
    if (t_session_mgr != NULL) {
      t_session_mgr->free_session(session);
    }
    OX (free_total_count = ATOMIC_FAA(&free_total_count_, 1));
    if (free_total_count > 0 && free_total_count % 10000 == 0) {
      LOG_INFO("free_session_count", K(free_total_count));
    }
    ObActiveSessionGuard::setup_default_ash();
  }
}

int ObSQLSessionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sessinfo_map_.init())) {
    LOG_WARN("fail to init session map", K(ret));
  } else if (OB_FAIL(sessid_sequence_.init(MAX_LOCAL_SEQ))) {
    LOG_WARN("init sessid sequence failed", K(ret));
  } else if (OB_FAIL(sess_hold_map_.create(BUCKET_COUNT,
                                           SET_USE_500("SessHoldMapBuck"),
                                           SET_USE_500("SessHoldMapNode")))) {
    LOG_WARN("failed to init sess_hold_map", K(ret));
  }
  for (uint32_t i = 1; OB_SUCC(ret) && i <= MAX_LOCAL_SEQ; ++i) {
    if (OB_FAIL(sessid_sequence_.push(reinterpret_cast<void*>(i)))) {
      LOG_WARN("store sessid sequence failed", K(ret), K(i));
    }
  }
  return ret;
}

void ObSQLSessionMgr::destroy()
{
  sessinfo_map_.destroy();
  sessid_sequence_.destroy();
  sess_hold_map_.destroy();
}

uint64_t ObSQLSessionMgr::extract_server_id(uint32_t sessid)
{
  uint64_t server_id = sessid >> LOCAL_SEQ_LEN;
  return server_id & MAX_SERVER_ID;
}

int ObSQLSessionMgr::inc_session_ref(const ObSQLSessionInfo *my_session)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(my_session)) {
    ObSQLSessionInfo *tmp_session = NULL;
    uint32_t sessid = my_session->get_sessid();
    if (OB_FAIL(get_session(sessid, tmp_session))) {
      LOG_WARN("fail to get session", K(sessid), K(ret));
    }
    UNUSED(tmp_session);
  }

  return ret;
}

// |<---------------------------------32bit---------------------------->|
// 0b   1b                             15b                              16b
// +----+------------------------------+--------------------------------+
// |Mask|          Server Id           |           Local Seq            |
// +----+------------------------------+--------------------------------+
//
//MASK: 1 表示是server自己生成connection id,
//      0 表示是proxy生成的connection id(已废弃，目前仅用于 in_mgr = false 的场景)；
//Server Id: 集群中server的id由RS分配，集群内唯一；
//Local Seq: 一个server可用连接数，目前单台server最多有INT16_MAX个连接;
//
int ObSQLSessionMgr::create_sessid(uint32_t &sessid, bool in_mgr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sessid = 0;
  const uint64_t server_id = GCTX.server_id_;
  uint32_t local_seq = 0;
  static uint32_t abnormal_seq = 0;//用于server_id == 0是的sessid分配
  if (server_id > MAX_SERVER_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("server id maybe invalid", K(ret), K(server_id));
  } else if (!in_mgr) {
    sessid = (GETTID() | LOCAL_SESSID_TAG);
    sessid |= static_cast<uint32_t>(server_id << LOCAL_SEQ_LEN);  // set observer
  } else if (0 == server_id) {
    local_seq = (ATOMIC_FAA(&abnormal_seq, 1) & MAX_LOCAL_SEQ);
    uint32_t max_local_seq = MAX_LOCAL_SEQ;
    uint32_t max_server_id = MAX_SERVER_ID;
    LOG_WARN("server is initiating", K(server_id), K(local_seq), K(max_local_seq), K(max_server_id));
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = tmp_ret = get_avaiable_local_seq(local_seq)))) {
    LOG_WARN("fail to get avaiable local_seq", K(local_seq));
  } else {/*do nothing*/}

  if (OB_SUCC(ret) && in_mgr) {
    sessid = local_seq | SERVER_SESSID_TAG;// set observer sessid mark
    sessid |= static_cast<uint32_t>(server_id << LOCAL_SEQ_LEN);  // set observer
    // high bit is reserved for server id
    sessid |= static_cast<uint32_t>(1ULL << (LOCAL_SEQ_LEN + SERVER_ID_LEN));
  }
  return ret;
}

//ret == OB_SUCCESS时,保证sess_info != NULL, 需要在外面进行revert_session
//ret != OB_SUCCESS时，保证sess_info == NULL, 不需要在外面进行revert_session
int ObSQLSessionMgr::create_session(ObSMConnection *conn, ObSQLSessionInfo *&sess_info)
{
  int ret = OB_SUCCESS;
  sess_info = NULL;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", K(ret));
  } else if (OB_FAIL(create_session(conn->tenant_id_,
                                    conn->sessid_,
                                    conn->proxy_sessid_,
                                    conn->sess_create_time_,
                                    sess_info))) {
    LOG_WARN("create session failed", K(ret));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_info is null", K(ret));
  } else {
    sess_info->set_vid(conn->vid_);
    sess_info->set_vip(conn->vip_buf_);
    sess_info->set_vport(conn->vport_);
    sess_info->inc_in_bytes(conn->connect_in_bytes_);
  }
  return ret;
}

int ObSQLSessionMgr::create_session(const uint64_t tenant_id,
                                    const uint32_t sessid,
                                    const uint64_t proxy_sessid,
                                    const int64_t create_time,
                                    ObSQLSessionInfo *&session_info)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  session_info = NULL;
  ObSQLSessionInfo *tmp_sess = NULL;
  if (OB_FAIL(sessinfo_map_.create(tenant_id, Key(sessid, proxy_sessid), tmp_sess))) {
    LOG_WARN("fail to create session", K(ret), K(sessid));
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SESSION_ENTRY_EXIST;
    }
  } else if (OB_ISNULL(tmp_sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc session info", K(ret), K(sessid), K(proxy_sessid));
  } else {
    // create session contains a 'get_session' action implicitly
    const bool v = GCONF._enable_trace_session_leak;
    if (OB_UNLIKELY(v)) {
      tmp_sess->on_get_session();
    }
  }

  if (OB_FAIL(ret)) {
    // pass
  } else if (OB_FAIL(tmp_sess->init(sessid, proxy_sessid, NULL, NULL, create_time,
                                    tenant_id))) {
    LOG_WARN("fail to init session", K(ret), K(tmp_sess),
        K(sessid), K(proxy_sessid), K(create_time));
    if (FALSE_IT(revert_session(tmp_sess))) {
      LOG_ERROR("fail to free session", K(err), K(sessid), K(proxy_sessid));
    } else if (OB_SUCCESS != (err = sessinfo_map_.del(Key(sessid)))) {
      LOG_ERROR("fail to free session", K(err), K(sessid), K(proxy_sessid));
    } else {
      LOG_DEBUG("free session successfully in create session", K(err),
          K(sessid), K(proxy_sessid));
    }
  } else {
    // set tenant info to session, if has.
    ObFLTControlInfoManager mgr(tenant_id);
    if (OB_FAIL(mgr.init())) {
      LOG_WARN("failed to init full link control info", K(ret));
      if (FALSE_IT(revert_session(tmp_sess))) {
        LOG_ERROR("fail to free session", K(err), K(sessid), K(proxy_sessid));
      } else if (OB_SUCCESS != (err = sessinfo_map_.del(Key(sessid)))) {
        LOG_ERROR("fail to free session", K(err), K(sessid), K(proxy_sessid));
      } else {
        LOG_DEBUG("free session successfully in create session", K(err),
            K(sessid), K(proxy_sessid));
      }
    } else if (mgr.is_valid_tenant_config()) {
      tmp_sess->set_flt_control_info(mgr.get_control_info());
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      tmp_sess->update_last_active_time();
      session_info = tmp_sess;
    }
  }
  return ret;
}

int ObSQLSessionMgr::free_session(const ObFreeSessionCtx &ctx)
{
  int ret = OB_SUCCESS;
  uint32_t sessid = ctx.sessid_;
  uint64_t proxy_sessid = ctx.proxy_sessid_;
  uint64_t tenant_id = ctx.tenant_id_;
  bool has_inc = ctx.has_inc_active_num_;
  ObSQLSessionInfo *sess_info = NULL;
  sessinfo_map_.get(Key(sessid), sess_info);
  if (NULL != sess_info) {
#ifdef OB_BUILD_AUDIT_SECURITY
    if (!sess_info->get_is_deserialized()) {
      ObString empty_comment_text;
      sess_info->update_alive_time_stat();
      int64_t cur_timeout_backup = THIS_WORKER.get_timeout_ts();
      THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
      ObSecurityAuditUtils::handle_security_audit(*sess_info,
                                                  stmt::T_LOGOFF,
                                                  ObString::make_string("DISCONNECT"),
                                                  empty_comment_text,
                                                  ret);
      THIS_WORKER.set_timeout_ts(cur_timeout_backup);
    }
#endif
    if (OB_UNLIKELY(OB_SUCCESS != sess_info->on_user_disconnect())) {
      LOG_WARN("user disconnect failed", K(ret), K(sess_info->get_user_id()));
    }
    sessinfo_map_.revert(sess_info);
  }
  if (OB_FAIL(sessinfo_map_.del(Key(sessid)))) {
    LOG_WARN("fail to remove session from session map", K(ret), K(sessid), K(proxy_sessid));
  } else if (tenant_id != 0 && sessid != 0 && has_inc) {
    ObTenantStatEstGuard guard(tenant_id);
    EVENT_DEC(ACTIVE_SESSIONS);
  }
  return ret;
}

void ObSQLSessionMgr::try_check_session()
{
  int ret = OB_SUCCESS;
  CheckSessionFunctor check_timeout(this);
  if (OB_FAIL(for_each_session(check_timeout))) {
    LOG_WARN("fail to check time out", K(ret));
  } else {
    if (REACH_TIME_INTERVAL(60000000)) { // 60s
      OZ (check_session_leak());
    }
  }
}

int ObSQLSessionMgr::get_min_active_snapshot_version(share::SCN &snapshot_version)
{
  int ret = OB_SUCCESS;

  concurrency_control::GetMinActiveSnapshotVersionFunctor min_active_txn_version_getter;

  if (OB_FAIL(for_each_session(min_active_txn_version_getter))) {
    LOG_WARN("fail to get min active snapshot version", K(ret));
  } else {
    snapshot_version = min_active_txn_version_getter.get_min_active_snapshot_version();
  }

  return ret;
}

int ObSQLSessionMgr::check_session_leak()
{
  int ret = OB_SUCCESS;
  int64_t hold_session_count = sess_hold_map_.size();
  int64_t used_session_count = 0;
  if (OB_FAIL(get_session_count(used_session_count))) {
    LOG_WARN("fail to get session count", K(ret));
  } else {
    static const int32_t DEFAULT_SESSION_LEAK_COUNT_THRESHOLD = 100;
    int64_t session_leak_count_threshold = - EVENT_CALL(EventTable::EN_SESSION_LEAK_COUNT_THRESHOLD);
    session_leak_count_threshold = session_leak_count_threshold > 0
                                  ? session_leak_count_threshold
                                  : DEFAULT_SESSION_LEAK_COUNT_THRESHOLD;
    LOG_INFO("get current session count", K(used_session_count), K(hold_session_count),
                                          K(session_leak_count_threshold));
    if (hold_session_count - used_session_count >= session_leak_count_threshold) {
      LOG_ERROR("session leak!!!", "leak_count", hold_session_count - used_session_count,
                 K(used_session_count), K(hold_session_count));
      DumpHoldSession dump_session;
      OZ(for_each_hold_session(dump_session));
    }
  }
  return ret;
}

void ObSQLSessionMgr::runTimerTask()
{
  try_check_session();
}

// just a wrapper
int ObSQLSessionMgr::kill_query(ObSQLSessionInfo &session)
{
  return kill_query(session, ObSQLSessionState::QUERY_KILLED);
}

int ObSQLSessionMgr::set_query_deadlocked(ObSQLSessionInfo &session)
{
  return kill_query(session, ObSQLSessionState::QUERY_DEADLOCKED);
}

int ObSQLSessionMgr::kill_query(ObSQLSessionInfo &session,
    const ObSQLSessionState status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  //如果start_stmt/end_stmt卡住，此时需要先唤醒sql线程，然后再设置标记，否则kill session不起作用
  if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::kill_query_session(session, status))) {
    LOG_WARN("fail to kill query or session", "ret", tmp_ret, K(session));
  }

  if (ObSQLSessionState::QUERY_KILLED == status) {
    ret = session.kill_query();
  } else if (ObSQLSessionState::QUERY_DEADLOCKED == status) {
    ret = session.set_query_deadlocked();
  } else {
    LOG_WARN("unexpected status", K(status));
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

// kill idle timeout transaction on this session
int ObSQLSessionMgr::kill_idle_timeout_tx(ObSQLSessionInfo *session)
{
  return ObSqlTransControl::kill_idle_timeout_tx(session);
}

// kill deadlock transaction on this session
int ObSQLSessionMgr::kill_deadlock_tx(ObSQLSessionInfo *session)
{
  return ObSqlTransControl::kill_deadlock_tx(session);
}

int ObSQLSessionMgr::kill_session(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  //如果start_stmt/end_stmt卡住，此时需要先唤醒sql线程，然后再设置标记，否则kill session不起作用
  if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::kill_query_session(
        session, ObSQLSessionState::SESSION_KILLED))) {
    LOG_WARN("fail to kill query or session", "ret", tmp_ret, K(session));
  }
  session.set_session_state(SESSION_KILLED);
  // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
  ObSQLSessionInfo::LockGuard query_lock_guard(session.get_query_lock());
  ObSQLSessionInfo::LockGuard data_lock_guard(session.get_thread_data_lock());
  bool need_disconnect = false;
  session.set_query_start_time(ObTimeUtility::current_time());
  if (session.is_in_transaction()) {
    auto tx_desc = session.get_tx_desc();
    if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::kill_tx_on_session_killed(&session))) {
      LOG_WARN("fail to rollback transaction", K(session.get_sessid()),
               "proxy_sessid", session.get_proxy_sessid(),
               K(tmp_ret), KPC(tx_desc),
               "query_str", session.get_current_query_string(),
               K(need_disconnect));
    }
  }

  session.update_last_active_time();
  session.set_disconnect_state(NORMAL_KILL_SESSION);
  rpc::ObSqlSockDesc &sock_desc = session.get_sock_desc();
  if (OB_LIKELY(NULL != sock_desc.sock_desc_)) {
    SQL_REQ_OP.disconnect_by_sql_sock_desc(sock_desc);
    // this function will trigger on_close(), and then free the session
    LOG_INFO("kill session successfully",
             "proxy", session.get_proxy_addr(),
             "peer", session.get_peer_addr(),
             "real_client_ip", session.get_client_ip(),
             "sessid", session.get_sessid(),
             "proxy_sessid", session.get_proxy_sessid(),
             "query_str", session.get_current_query_string());
  } else {
    LOG_WARN("get conn from session info is null", K(session.get_sessid()),
        "proxy_sessid", session.get_proxy_sessid(), K(session.get_magic_num()));
  }

  return ret;
}

int ObSQLSessionMgr::disconnect_session(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
  ObSQLSessionInfo::LockGuard query_lock_guard(session.get_query_lock());
  ObSQLSessionInfo::LockGuard data_lock_guard(session.get_thread_data_lock());
  bool need_disconnect = false;
  session.set_query_start_time(ObTimeUtility::current_time());
  // 调用这个函数之前会在ObSMHandler::on_disconnect中调session.set_session_state(SESSION_KILLED)，
  if (session.is_in_transaction()) {
    auto tx_desc = session.get_tx_desc();
    if (OB_FAIL(ObSqlTransControl::kill_tx_on_session_disconnect(&session))) {
      LOG_WARN("fail to rollback transaction", K(session.get_sessid()),
               "proxy_sessid", session.get_proxy_sessid(), K(ret),
               KPC(tx_desc),
               "query_str", session.get_current_query_string(),
               K(need_disconnect));
    }
  }
  session.update_last_active_time();
  return ret;
}

int ObSQLSessionMgr::kill_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  KillTenant kt_func(this, tenant_id);
  OZ (for_each_session(kt_func));
  OZ (sessinfo_map_.clean_tenant(tenant_id));
  return ret;
}

int ObSQLSessionMgr::mark_sessid_used(uint32_t sess_id)
{
  //这个接口是为了以前老版本解决proxy sessid复用添加的
  //现在的版本(从2.0开始)里不会再有switch_sessid的需求，因此这里不用再提供
  UNUSED(sess_id);
  return OB_NOT_SUPPORTED;
}

int ObSQLSessionMgr::mark_sessid_unused(uint32_t sess_id)
{
  int ret = OB_SUCCESS;
  uint64_t server_id = extract_server_id(sess_id);
  if (server_id == 0) {
    // 参考：create_sessid方法
    // 由于server_id == 0时, 此时的local_seq，是由ATOMIC_FAA(&abnormal_seq, 1)产生，
    // 使用ATOMIC_FAA的原因无从考证（原作者的信息描述无任何具体信息），采取保守修改策略
    // local_seq未曾从sessid_sequence_队列中获取,所以不需要归还到队列，不然会导致队列溢出的bug
    // bug详情:
  } else if (OB_FAIL(sessid_sequence_.push(reinterpret_cast<void*>(sess_id & MAX_LOCAL_SEQ)))) {
    LOG_WARN("fail to push sessid to sessid_sequence_", K(sess_id), K(ret));
  }
  return ret;
}

bool ObSQLSessionMgr::CheckSessionFunctor::operator()(sql::ObSQLSessionMgr::Key key,
                                                      ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  bool is_timeout = false;
  if (OB_ISNULL(sess_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is NULL");
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is not valid", K(ret));
  } else {
    int callback_retcode = OB_SUCCESS;
    transaction::ObITxCallback *commit_cb = NULL;
    // NOTE: 下面两个guard的顺序不可更换，否则有机会形成死锁
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (OB_FAIL(sess_info->try_lock_thread_data())) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to try lock thread data", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        if (OB_ISNULL(sess_mgr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session manager point is NULL");
        } else if (OB_FAIL(sess_info->is_timeout(is_timeout))) {
          LOG_WARN("fail to check is timeout", K(ret));
        } else if (true == is_timeout) {
          LOG_INFO("session is timeout, kill this session", K(key.sessid_));
          ret = sess_mgr_->kill_session(*sess_info);
        } else if (sess_info->is_txn_free_route_temp()) {
          sess_info->check_txn_free_route_alive();
        } else {
          //借助于session遍历的功能，尝试revert session上缓存的schema guard，
          //避免长时间持有guard，导致schema mgr的槽位无法释放
          sess_info->get_cached_schema_guard_info().try_revert_schema_guard();
          //定期更新租户级别的配置项，避免频繁获取租户级配置项给性能带来的开销
          sess_info->refresh_tenant_config();
          // send client commit result if txn commit timeout
          if (OB_FAIL(sess_info->is_trx_commit_timeout(commit_cb, callback_retcode))) {
            LOG_WARN("fail to check transaction commit timeout", K(ret));
          } else if (commit_cb) {
            LOG_INFO("transaction commit reach timeout", K(callback_retcode), K(key.sessid_));
          } else if (OB_FAIL(sess_info->is_trx_idle_timeout(is_timeout))) {
            // kill transaction which is idle more than configuration 'ob_trx_idle_timeout'
            LOG_WARN("fail to check transaction idle timeout", K(ret));
          } else if (true == is_timeout && !sess_info->associated_xa()) {
            LOG_INFO("transaction is idle timeout, start to rollback", K(key.sessid_));
            int tmp_ret;
            if (OB_SUCCESS != (tmp_ret = sess_mgr_->kill_idle_timeout_tx(sess_info))) {
              LOG_WARN("fail to kill transaction", K(ret), K(key.sessid_));
            }
          }
        }
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
      // NOTE: must execute callback after release query_lock
      if (commit_cb) {
        commit_cb->callback(callback_retcode);
      }
    }
  }
  return OB_SUCCESS == ret;
}

bool ObSQLSessionMgr::KillTenant::operator() (
    sql::ObSQLSessionMgr::Key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session mgr_ is NULL", K(mgr_));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess info is NULL", K(sess_info));
  } else {
    if (sess_info->get_priv_tenant_id() == tenant_id_ ||
      sess_info->get_effective_tenant_id() == tenant_id_) {
      ret = mgr_->kill_session(*sess_info);
    }
  }
  return OB_SUCCESS == ret;
}

int ObSQLSessionMgr::get_avaiable_local_seq(uint32_t &local_seq)
{
  int ret = OB_SUCCESS;
  local_seq = 0;
  void *ptr = nullptr;
  if (OB_FAIL(sessid_sequence_.pop(ptr))) {
    LOG_WARN("fail to find and set first zero bit", K(ret));
  } else {
    int64_t value = reinterpret_cast<int64_t>(ptr);
    if (value > MAX_LOCAL_SEQ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value from bitset", K(ret), K(value), K(ptr));
    } else {
      local_seq = static_cast<uint32_t>(value);
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ERR_CON_COUNT_ERROR;
    int64_t sess_count = 0;
    (void)get_session_count(sess_count);
    LOG_WARN("too many connection", "connection_count", sess_count, K(ret));
  }
  return ret;
}

int ObSQLSessionMgr::is_need_clear_sessid(const ObSMConnection *conn, bool &is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parameter", K(conn));
  } else if (is_server_sessid(conn->sessid_)
             && ObSMConnection::INITIAL_SESSID != conn->sessid_
             && 0 != extract_server_id(conn->sessid_)
             && GCTX.server_id_ == extract_server_id(conn->sessid_)
             && conn->is_need_clear_sessid_) {
    is_need = true;
  } else {/*do nothing*/  }
  return ret;
}

int ObSQLSessionMgr::DumpHoldSession::operator()(
               common::hash::HashMapPair<uint64_t, ObSQLSessionInfo *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    LOG_INFO("session is null", "sess_ptr", entry.first);
  } else {
    LOG_INFO("dump session", "sid", entry.second->get_sessid(),
                             "ref_count", entry.second->get_sess_ref_cnt(),
                             "state",ObString::make_string(entry.second->get_session_state_str()),
                             KP(entry.second),
                             K(entry.first),
                             "lbt", entry.second->get_sess_bt());
  }
  return ret;
}

ObSessionGetterGuard::ObSessionGetterGuard(ObSQLSessionMgr &sess_mgr, uint32_t sessid)
  : mgr_(sess_mgr), session_(NULL)
{
  ret_ = mgr_.get_session(sessid, session_);
  if (OB_SUCCESS != ret_) {
    LOG_WARN_RET(ret_, "get session fail", K(ret_), K(sessid));
  } else {
    NG_TRACE_EXT(session, OB_ID(sid), session_->get_sessid(),
                 OB_ID(tenant_id), session_->get_priv_tenant_id());
  }
}

ObSessionGetterGuard::~ObSessionGetterGuard()
{
  if (session_) {
    mgr_.revert_session(session_);
  }
}

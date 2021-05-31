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
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "io/easy_io.h"
#include "rpc/ob_rpc_define.h"
#include "sql/ob_sql_trans_control.h"
#include "observer/mysql/obsm_handler.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::observer;

ObSQLSessionMgr::SessionPool::SessionPool() : session_pool_()
{
  MEMSET(session_array, 0, POOL_CAPACIPY * sizeof(ObSQLSessionInfo*));
}

int ObSQLSessionMgr::SessionPool::init()
{
  int ret = OB_SUCCESS;
  char* session_buf = reinterpret_cast<char*>(session_array);
  OZ(session_pool_.init(POOL_CAPACIPY, session_buf));
  return ret;
}

int ObSQLSessionMgr::SessionPool::pop_session(uint64_t tenant_id, ObSQLSessionInfo*& session)
{
  int ret = OB_SUCCESS;
  session = NULL;
  if (OB_FAIL(session_pool_.pop(session))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN(
          "failed to pop session", K(ret), K(tenant_id), K(session_pool_.get_total()), K(session_pool_.get_free()));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO(
          "session pool is empty", K(tenant_id), K(session_pool_.get_total()), K(session_pool_.get_free()), K(lbt()));
    }
  }
  return ret;
}

int ObSQLSessionMgr::SessionPool::push_session(uint64_t tenant_id, ObSQLSessionInfo*& session)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session)) {
    if (OB_FAIL(session_pool_.push(session))) {
      if (ret != OB_SIZE_OVERFLOW) {
        LOG_WARN(
            "failed to push session", K(ret), K(tenant_id), K(session_pool_.get_total()), K(session_pool_.get_free()));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO(
            "session pool is full", K(tenant_id), K(session_pool_.get_total()), K(session_pool_.get_free()), K(lbt()));
      }
    } else {
      session = NULL;
    }
  }
  return ret;
}

int64_t ObSQLSessionMgr::SessionPool::count() const
{
  return session_pool_.get_total();
}

int ObSQLSessionMgr::SessionPoolMap::get_session_pool(uint64_t tenant_id, SessionPool*& session_pool)
{
  int ret = OB_SUCCESS;
  uint64_t block_id = get_block_id(tenant_id);
  uint64_t slot_id = get_slot_id(tenant_id);
  SessionPoolBlock* pool_block = NULL;
  if (block_id < pool_blocks_.count()) {
    OX(pool_block = pool_blocks_.at(block_id));
  } else {
    OZ(create_pool_block(block_id, pool_block));
  }
  OV(OB_NOT_NULL(pool_block), OB_ERR_UNEXPECTED, tenant_id, block_id, slot_id);
  OX(session_pool = ATOMIC_LOAD(&((*pool_block)[slot_id])));
  if (OB_ISNULL(session_pool)) {
    OZ(create_session_pool(*pool_block, slot_id, session_pool));
  }
  return ret;
}

int ObSQLSessionMgr::SessionPoolMap::create_pool_block(uint64_t block_id, SessionPoolBlock*& pool_block)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  while (OB_SUCC(ret) && block_id >= pool_blocks_.count()) {
    OV(OB_NOT_NULL(buf = alloc_.alloc(sizeof(SessionPoolBlock))), OB_ALLOCATE_MEMORY_FAILED, block_id);
    OX(MEMSET(buf, 0, sizeof(SessionPoolBlock)));
    OZ(pool_blocks_.push_back(static_cast<SessionPoolBlock*>(buf)), block_id);
  }
  OX(pool_block = pool_blocks_.at(block_id));
  return ret;
}

int ObSQLSessionMgr::SessionPoolMap::create_session_pool(
    SessionPoolBlock& pool_block, uint64_t slot_id, SessionPool*& session_pool)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  OV(slot_id < SLOT_PER_BLOCK);
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (OB_ISNULL(session_pool = ATOMIC_LOAD(&(pool_block[slot_id])))) {
    OV(OB_NOT_NULL(buf = alloc_.alloc(sizeof(SessionPool))), OB_ALLOCATE_MEMORY_FAILED);
    OV(OB_NOT_NULL(session_pool = new (buf) SessionPool()));
    OZ(session_pool->init());
    OX(ATOMIC_STORE(&(pool_block[slot_id]), session_pool));
  }
  return ret;
}

uint64_t ObSQLSessionMgr::SessionPoolMap::get_block_id(uint64_t tenant_id) const
{
  return tenant_id >> BLOCK_ID_SHIFT;
}

uint64_t ObSQLSessionMgr::SessionPoolMap::get_slot_id(uint64_t tenant_id) const
{
  return tenant_id & SLOT_ID_MASK;
}

int ObSQLSessionMgr::ValueAlloc::clean_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SessionPool* session_pool = NULL;
  ObSQLSessionInfo* session = NULL;
  if (is_valid_tenant_id(tenant_id)) {
    OX(session_pool_map_.get_session_pool(tenant_id, session_pool));
  }
  if (OB_NOT_NULL(session_pool)) {
    // Note that there is no design guarantee that the session pool must be completely cleaned.
    // There is a very low probability of session leak.
    // 1. In the production system, deleting tenants is a very rare operation.
    // 2. In the production system, before deleting a tenant, we must ensure that the business
    //    layer will no longer access the tenant.
    // Under the above premise, theoretically, the tenant will have no active  session.
    // 3. When stopping an observer, even if a few sessions are missing, they will be releasedi
    //    with the end of the process.
    // 4. In the case of unit migration, a few sessions may not be released, but in the future,
    //    the unit can be reused after migrating back.
    while (session_pool->count() > 0) {
      OX(session_pool->pop_session(tenant_id, session));
      if (OB_NOT_NULL(session)) {
        OX(op_reclaim_free(session));
        OX(session = NULL);
      }
    }
  }
  return ret;
}

ObSQLSessionInfo* ObSQLSessionMgr::ValueAlloc::alloc_value(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SessionPool* session_pool = NULL;
  ObSQLSessionInfo* session = NULL;
  int64_t alloc_total_count = 0;
  // we use OX instead of OZ in operation of upper session pool, because we need acquire
  // from lower session pool when not success, no matter which errno we get here.
  if (is_valid_tenant_id(tenant_id)) {
    OX(session_pool_map_.get_session_pool(tenant_id, session_pool));
    OX(alloc_total_count = ATOMIC_FAA(&alloc_total_count_, 1));
  }
  if (OB_NOT_NULL(session_pool)) {
    OX(session_pool->pop_session(tenant_id, session));
  }
  if (OB_ISNULL(session)) {
    OX(session = op_reclaim_alloc(ObSQLSessionInfo));
  } else {
    OX(ATOMIC_FAA(&alloc_from_pool_count_, 1));
  }
  OV(OB_NOT_NULL(session));
  OX(session->set_valid(true));
  OX(session->set_shadow(true));
  if (alloc_total_count > 0 && alloc_total_count % 10000 == 0) {
    LOG_INFO("alloc_session_count", K(alloc_total_count), K_(alloc_from_pool_count));
  }
  return session;
}

void ObSQLSessionMgr::ValueAlloc::free_value(ObSQLSessionInfo* session)
{
  if (OB_NOT_NULL(session)) {
    int ret = OB_SUCCESS;
    uint64_t tenant_id = session->get_login_tenant_id();
    SessionPool* session_pool = NULL;
    int64_t free_total_count = 0;
    if (is_valid_tenant_id(tenant_id) && session->can_release_to_pool()) {
      if (session->is_use_inner_allocator() && !session->is_tenant_killed()) {
        OX(session_pool_map_.get_session_pool(tenant_id, session_pool));
      }
      OX(free_total_count = ATOMIC_FAA(&free_total_count_, 1));
    }
    if (OB_NOT_NULL(session_pool)) {
      OX(session->destroy(true));
      OX(session->set_acquire_from_pool(true));
      OX(session_pool->push_session(tenant_id, session));
    }
    if (OB_NOT_NULL(session)) {
      OX(op_reclaim_free(session));
      OX(session = NULL);
    } else {
      OX(ATOMIC_FAA(&free_to_pool_count_, 1));
    }
    if (free_total_count > 0 && free_total_count % 10000 == 0) {
      LOG_INFO("free_session_count", K(free_total_count), K_(free_to_pool_count));
    }
  }
}

bool ObSQLSessionMgr::ValueAlloc::is_valid_tenant_id(uint64_t tenant_id) const
{
  return ::is_valid_tenant_id(tenant_id) && tenant_id != OB_INVALID_ID && tenant_id != OB_SYS_TENANT_ID;
}

int ObSQLSessionMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sessinfo_map_.init())) {
    LOG_WARN("fail to init session map", K(ret));
  } else if (OB_FAIL(sessid_sequence_.init(MAX_LOCAL_SEQ))) {
    LOG_WARN("init sessid sequence failed", K(ret));
  }
  for (uint32_t i = 1; OB_SUCC(ret) && i <= MAX_LOCAL_SEQ; ++i) {
    if (OB_FAIL(sessid_sequence_.push(reinterpret_cast<void*>(i)))) {
      LOG_WARN("store sessid sequence failed", K(ret), K(i));
    }
  }
  return ret;
}

uint64_t ObSQLSessionMgr::extract_server_id(uint32_t sessid)
{
  uint64_t server_id = sessid >> LOCAL_SEQ_LEN;
  return server_id & MAX_SERVER_ID;
}

int ObSQLSessionMgr::inc_session_ref(const ObSQLSessionInfo* my_session)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(my_session)) {
    ObSQLSessionInfo* tmp_session = NULL;
    uint32_t version = my_session->get_version();
    uint32_t sessid = my_session->get_sessid();
    if (OB_FAIL(get_session(version, sessid, tmp_session))) {
      LOG_WARN("fail to get session", K(version), K(sessid), K(ret));
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
// MASK: 1 means session_id is generated by observer, 0 means by proxy.
// Server Id: generated by root server, and unique in cluster.
// Local Seq: max session number in an observer.
//
int ObSQLSessionMgr::create_sessid(uint32_t& sessid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sessid = 0;
  const uint64_t server_id = GCTX.server_id_;
  uint32_t local_seq = 0;
  static uint32_t abnormal_seq = 0;
  if (server_id > MAX_SERVER_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("server id maybe invalid", K(ret), K(server_id));
  } else if (0 == server_id) {
    local_seq = (ATOMIC_FAA(&abnormal_seq, 1) & MAX_LOCAL_SEQ);
    uint32_t max_local_seq = MAX_LOCAL_SEQ;
    uint32_t max_server_id = MAX_SERVER_ID;
    LOG_WARN("server is initiating", K(server_id), K(local_seq), K(max_local_seq), K(max_server_id));
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = tmp_ret = get_avaiable_local_seq(local_seq)))) {
    LOG_WARN("fail to get avaiable local_seq", K(local_seq));
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    sessid = local_seq | SERVER_SESSID_TAG;                       // set observer sessid mark
    sessid |= static_cast<uint32_t>(server_id << LOCAL_SEQ_LEN);  // set observer
    // high bit is reserved for server id
    sessid |= static_cast<uint32_t>(1ULL << (LOCAL_SEQ_LEN + SERVER_ID_LEN));
  }
  return ret;
}

// if ret == OB_SUCCESS, sess_info must be not NULL, caller must call revert_session() later.
// if ret != OB_SUCCESS, sess_info must be NULL.
int ObSQLSessionMgr::create_session_by_version(
    uint64_t tenant_id, uint32_t sessid, uint64_t proxy_sessid, ObSQLSessionInfo*& sess_info, uint32_t& out_version)
{
  int ret = OB_SUCCESS;
  int gt_ret = OB_SUCCESS;
  int rt_ret = OB_SUCCESS;
  sess_info = NULL;
  out_version = 0;
  uint32_t version = 0;
  bool is_shadow = false;
  ObSQLSessionInfo* tmp_sess = NULL;
  do {
    is_shadow = false;
    if (OB_FAIL(sessinfo_map_.create(tenant_id, Key(version, sessid, proxy_sessid), tmp_sess))) {
      if (OB_LIKELY(OB_ENTRY_EXIST == ret)) {
        if (OB_UNLIKELY(OB_SUCCESS != (gt_ret = get_session(version, sessid, tmp_sess)))) {
          if (OB_ENTRY_NOT_EXIST == gt_ret) {
            LOG_DEBUG("session has been freed", K(gt_ret), K(version), K(sessid));
          } else {
            LOG_WARN("fail to get session", K(gt_ret), K(version), K(sessid));
          }
        } else {
          is_shadow = tmp_sess->is_shadow();
          if (OB_UNLIKELY(OB_SUCCESS != (rt_ret = revert_session(tmp_sess)))) {
            LOG_ERROR("fail to revert session", K(rt_ret));
          }
          ++version;
        }
      }
    }
  } while (OB_UNLIKELY(OB_ENTRY_EXIST == ret && ((OB_SUCCESS == gt_ret && is_shadow) || OB_ENTRY_NOT_EXIST == gt_ret) &&
                       version <= MAX_VERSION));

  if (OB_SUCC(ret)) {
    sess_info = tmp_sess;
    out_version = version;
  }
  return ret;
}

// if ret == OB_SUCCESS, sess_info must be not NULL, caller must call revert_session() later.
// if ret != OB_SUCCESS, sess_info must be NULL.
int ObSQLSessionMgr::create_session(ObSMConnection* conn, ObSQLSessionInfo*& sess_info)
{
  int ret = OB_SUCCESS;
  sess_info = NULL;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", K(ret));
  } else if (OB_FAIL(create_session(
                 conn->tenant_id_, conn->sessid_, conn->proxy_sessid_, conn->sess_create_time_, sess_info))) {
    LOG_WARN("create session failed", K(ret));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL session returned", K(ret));
  } else {
    conn->version_ = sess_info->get_version();
  }
  return ret;
}

int ObSQLSessionMgr::create_session(const uint64_t tenant_id, const uint32_t sessid, const uint64_t proxy_sessid,
    const int64_t create_time, ObSQLSessionInfo*& session_info)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  session_info = NULL;
  uint32_t version = 0;
  ObSQLSessionInfo* tmp_sess = NULL;
  if (OB_FAIL(create_session_by_version(tenant_id, sessid, proxy_sessid, tmp_sess, version))) {
    LOG_WARN("fail to create session by version", K(ret), K(sessid), K(version));
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SESSION_ENTRY_EXIST;
    }
  } else if (OB_ISNULL(tmp_sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc session info", K(ret), K(version), K(sessid), K(proxy_sessid));
  } else if (OB_FAIL(tmp_sess->init(version, sessid, proxy_sessid, NULL, NULL, create_time, tenant_id))) {
    LOG_WARN("fail to init session", K(ret), K(tmp_sess), K(version), K(sessid), K(proxy_sessid), K(create_time));
    if (OB_SUCCESS != (err = revert_session(tmp_sess))) {
      LOG_ERROR("fail to free session", K(err), K(version), K(sessid), K(proxy_sessid));
    } else if (OB_SUCCESS != (err = sessinfo_map_.del(Key(version, sessid)))) {
      LOG_ERROR("fail to free session", K(err), K(version), K(sessid), K(proxy_sessid));
    } else {
      LOG_DEBUG("free session successfully in create session", K(err), K(version), K(sessid), K(proxy_sessid));
    }
  } else {
    tmp_sess->update_last_active_time();
    session_info = tmp_sess;
  }
  return ret;
}

int ObSQLSessionMgr::free_session(const ObFreeSessionCtx& ctx)
{
  int ret = OB_SUCCESS;
  uint32_t version = ctx.version_;
  uint32_t sessid = ctx.sessid_;
  uint64_t proxy_sessid = ctx.proxy_sessid_;
  uint64_t tenant_id = ctx.tenant_id_;
  bool has_inc = ctx.has_inc_active_num_;
  ObSQLSessionInfo* sess_info = NULL;
  sessinfo_map_.get(Key(version, sessid), sess_info);
  if (NULL != sess_info) {
    sessinfo_map_.revert(sess_info);
  }
  if (OB_FAIL(sessinfo_map_.del(Key(version, sessid)))) {
    LOG_WARN("fail to remove session from session map", K(ret), K(version), K(sessid), K(proxy_sessid));
  } else if (tenant_id != 0 && sessid != 0 && has_inc) {
    ObTenantStatEstGuard guard(tenant_id);
    EVENT_DEC(ACTIVE_SESSIONS);
  }
  return ret;
}

void ObSQLSessionMgr::try_check_session()
{
  int ret = OB_SUCCESS;
  int64_t sess_count = 0;
  CheckSessionFunctor check_timeout(this);
  if (OB_FAIL(for_each_session(check_timeout))) {
    LOG_WARN("fail to check time out", K(ret));
  } else if (OB_FAIL(get_session_count(sess_count))) {
    LOG_WARN("fail to get session count", K(ret));
  } else {
    LOG_DEBUG("get current session count", K(sess_count));
  }
}

void ObSQLSessionMgr::runTimerTask()
{
  try_check_session();
}

// just a wrapper
int ObSQLSessionMgr::kill_query(ObSQLSessionInfo& session)
{
  return kill_query(session, partition_service_);
}

int ObSQLSessionMgr::kill_query(ObSQLSessionInfo& session, storage::ObPartitionService* partition_service)
{
  int tmp_ret = OB_SUCCESS;
  // must call kill_query_session() to wakeup worker thread first,
  // otherwise kill_query() may not work if worker is sleep.
  if (OB_SUCCESS !=
      (tmp_ret = ObSqlTransControl::kill_query_session(partition_service, session, ObSQLSessionState::QUERY_KILLED))) {
    LOG_WARN("fail to kill query or session", "ret", tmp_ret, K(session));
  }
  return session.kill_query();
}

// kill active transaction on this session
int ObSQLSessionMgr::kill_active_trx(ObSQLSessionInfo* session)
{
  return ObSqlTransControl::kill_active_trx(partition_service_, session);
}

int ObSQLSessionMgr::kill_session(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // must call kill_query_session() to wakeup worker thread first,
  // otherwise kill_query() may not work if worker is sleep.
  if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::kill_query_session(
                         partition_service_, session, ObSQLSessionState::SESSION_KILLED))) {
    LOG_WARN("fail to kill query or session", "ret", tmp_ret, K(session));
  }
  session.set_session_state(SESSION_KILLED);
  // DO NOT change the order of the two guards below, otherwise it may cause deadlock.
  ObSQLSessionInfo::LockGuard query_lock_guard(session.get_query_lock());
  ObSQLSessionInfo::LockGuard data_lock_guard(session.get_thread_data_lock());
  bool has_called_txs_end_trans = false;
  session.set_query_start_time(ObTimeUtility::current_time());
  if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::end_trans(partition_service_, &session, has_called_txs_end_trans))) {
    LOG_WARN("fail to rollback transaction",
        K(session.get_sessid()),
        "proxy_sessid",
        session.get_proxy_sessid(),
        K(tmp_ret),
        "trans_id",
        session.get_trans_desc().get_trans_id(),
        "query_str",
        session.get_current_query_string(),
        K(has_called_txs_end_trans));
  }

  session.update_last_active_time();
  easy_connection_t* c = session.get_conn();
  if (OB_LIKELY(NULL != c)) {
    // this function will trigger on_close(), and then free the session
    easy_connection_destroy_dispatch(c);
    session.set_conn(NULL);  // the connection is invalid now
    LOG_INFO("kill session successfully",
        "proxy",
        session.get_proxy_addr(),
        "peer",
        session.get_peer_addr(),
        "real_client_ip",
        session.get_client_ip(),
        "sessid",
        session.get_sessid(),
        "proxy_sessid",
        session.get_proxy_sessid(),
        "trans_id",
        session.get_trans_desc().get_trans_id(),
        "is_trans_end",
        session.get_trans_desc().is_trans_end(),
        "query_str",
        session.get_current_query_string());
  } else {
    LOG_WARN("get conn from session info is null",
        K(session.get_sessid()),
        K(session.get_version()),
        "proxy_sessid",
        session.get_proxy_sessid(),
        K(session.get_magic_num()));
  }
  return ret;
}

int ObSQLSessionMgr::disconnect_session(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  // DO NOT change the order of the two guards below, otherwise it may cause deadlock.
  ObSQLSessionInfo::LockGuard query_lock_guard(session.get_query_lock());
  ObSQLSessionInfo::LockGuard data_lock_guard(session.get_thread_data_lock());
  bool has_called_txs_end_trans = false;
  session.set_query_start_time(ObTimeUtility::current_time());
  if (OB_FAIL(ObSqlTransControl::end_trans(partition_service_, &session, has_called_txs_end_trans))) {
    LOG_WARN("fail to rollback transaction",
        K(session.get_sessid()),
        "proxy_sessid",
        session.get_proxy_sessid(),
        K(ret),
        "trans_id",
        session.get_trans_desc().get_trans_id(),
        "query_str",
        session.get_current_query_string(),
        K(has_called_txs_end_trans));
  }
  session.update_last_active_time();
  return ret;
}

int ObSQLSessionMgr::kill_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  KillTenant kt_func(this, tenant_id);
  OZ(for_each_session(kt_func));
  OZ(sessinfo_map_.clean_tenant(tenant_id));
  return ret;
}

int ObSQLSessionMgr::mark_sessid_used(uint32_t sess_id)
{
  UNUSED(sess_id);
  return OB_NOT_SUPPORTED;
}

int ObSQLSessionMgr::mark_sessid_unused(uint32_t sess_id)
{
  int ret = OB_SUCCESS;
  uint64_t server_id = extract_server_id(sess_id);
  if (server_id == 0) {
    // see create_sessid(), sess_id with server_id 0 is not allocated from sessid_sequence_.
  } else if (OB_FAIL(sessid_sequence_.push(reinterpret_cast<void*>(sess_id & MAX_LOCAL_SEQ)))) {
    LOG_WARN("fail to push sessid to sessid_sequence_", K(sess_id), K(ret));
  }
  return ret;
}

bool ObSQLSessionMgr::CheckSessionFunctor::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo* sess_info)
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
    // DO NOT change the order of the two locks below, otherwise it may cause deadlock.
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
          LOG_INFO("session is timeout, kill this session", K(key.version_), K(key.sessid_));
          ret = sess_mgr_->kill_session(*sess_info);
        } else {
          sess_info->get_cached_schema_guard_info().try_revert_schema_guard();
          // kill transaction which is idle more than configuration 'ob_trx_idle_timeout'
          if (OB_FAIL(sess_info->is_trx_idle_timeout(is_timeout))) {
            LOG_WARN("fail to check transaction idle timeout", K(ret));
          } else if (true == is_timeout) {
            LOG_INFO("transaction is idle timeout, start to rollback", K(key.version_), K(key.sessid_));
            int tmp_ret;
            if (OB_SUCCESS != (tmp_ret = sess_mgr_->kill_active_trx(sess_info))) {
              LOG_WARN("fail to kill transaction", K(tmp_ret), K(key.version_), K(key.sessid_));
            }
          }
        }
        (void)sess_info->unlock_thread_data();
      }
      (void)sess_info->unlock_query();
    }
  }
  return OB_SUCCESS == ret;
}

bool ObSQLSessionMgr::KillTenant::operator()(sql::ObSQLSessionMgr::Key, ObSQLSessionInfo* sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session mgr_ is NULL", K(mgr_));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess info is NULL", K(sess_info));
  } else {
    if (sess_info->get_priv_tenant_id() == tenant_id_) {
      ret = mgr_->kill_session(*sess_info);
    }
  }
  return OB_SUCCESS == ret;
}

int ObSQLSessionMgr::get_avaiable_local_seq(uint32_t& local_seq)
{
  int ret = OB_SUCCESS;
  local_seq = 0;
  void* ptr = nullptr;
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

int ObSQLSessionMgr::is_need_clear_sessid(const ObSMConnection* conn, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parameter", K(conn));
  } else if (is_server_sessid(conn->sessid_) && ObSMConnection::INITIAL_SESSID != conn->sessid_ &&
             0 != extract_server_id(conn->sessid_) && GCTX.server_id_ == extract_server_id(conn->sessid_) &&
             conn->is_need_clear_sessid_) {
    is_need = true;
  } else { /*do nothing*/
  }
  return ret;
}

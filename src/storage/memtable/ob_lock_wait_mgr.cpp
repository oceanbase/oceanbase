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

#include "ob_lock_wait_mgr.h"

#include "common/ob_clock_generator.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_errno.h"
#include "lib/rowid/ob_urowid.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "lib/function/ob_function.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/utility/utility.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include <cstdint>

namespace oceanbase
{
namespace obmysql
{

// replace function in library
void request_finish_callback()
{
  bool unused = false;
  memtable::ObLockWaitMgr *lock_wait_mgr = MTL(memtable::ObLockWaitMgr*);
  if (OB_ISNULL(lock_wait_mgr)) {
    TRANS_LOG(TRACE, "MTL(lock wait mgr) is null", K(MTL_ID()));
  } else {
    lock_wait_mgr->post_process(false, unused);
  }
  memtable::ObLockWaitMgr::clear_thread_node();
}

}
}
namespace oceanbase
{

using namespace storage;
using namespace common::hash;
using namespace common;
using namespace share;
using namespace memtable::tablelock;

namespace memtable
{

static const uint64_t TRANS_FLAG = 1L << 63L;       // 10
static const uint64_t TABLE_LOCK_FLAG = 1L << 62L;  // 01
static const uint64_t ROW_FLAG = 0L;                // 00
static const uint64_t HASH_MASK = ~(TRANS_FLAG | TABLE_LOCK_FLAG);

static inline
uint64_t hash_rowkey(const ObTabletID &tablet_id, const ObLockWaitMgr::Key &key)
{
  uint64_t hash_id = tablet_id.hash();
  uint64_t hash_key = key.hash();
  uint64_t hash = murmurhash(&hash_key, sizeof(hash_key), hash_id);
  return ((hash & HASH_MASK) | ROW_FLAG) | 1;
}

static inline
uint64_t hash_trans(const ObTransID &tx_id)
{
  return ((murmurhash(&tx_id, sizeof(tx_id), 0) & HASH_MASK) | TRANS_FLAG) | 1;
}

static inline
uint64_t hash_lock_id(const ObLockID &lock_id)
{
  return ((lock_id.hash() & HASH_MASK) | TABLE_LOCK_FLAG) | 1;
}

static inline
bool is_rowkey_hash(const uint64_t hash)
{
  return (hash & ~HASH_MASK) == ROW_FLAG;
}

ObLockWaitMgr::ObLockWaitMgr()
    : is_inited_(false),
      hash_(hash_buf_, sizeof(hash_buf_)),
      deadlocked_sessions_lock_(common::ObLatchIds::DEADLOCK_DETECT_LOCK),
      deadlocked_sessions_index_(0),
      total_wait_node_(0)
{
  memset(sequence_, 0, sizeof(sequence_));
}

ObLockWaitMgr::~ObLockWaitMgr() {}

int ObLockWaitMgr::mtl_init(ObLockWaitMgr *&lock_wait_mgr)
{
  return lock_wait_mgr->init();
}

int ObLockWaitMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ob lock wait mgr already init", K(ret), K(is_inited_));
  } else if (OB_FAIL(row_holder_mapper_.init())) {
    TRANS_LOG(WARN, "can't init row_holder", KR(ret));
  } else {
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    last_check_session_idle_ts_ = ObClockGenerator::getClock();
    total_wait_node_ = 0;
    is_inited_ = true;
  }
  TRANS_LOG(INFO, "LockWaitMgr.init", K(ret));
  return ret;
}

int ObLockWaitMgr::start() {
  int ret = share::ObThreadPool::start();
  TRANS_LOG(INFO, "LockWaitMgr.start", K(ret));
  return ret;
}

void ObLockWaitMgr::stop() {
  share::ObThreadPool::stop();
  TRANS_LOG(INFO, "LockWaitMgr.stop");
}
void ObLockWaitMgr::destroy() {
  is_inited_ = false;
  total_wait_node_ = 0;
  deadlocked_sessions_index_ = 0;
}

void RowHolderMapper::set_hash_holder(const ObTabletID &tablet_id,
                                      const ObMemtableKey &key,
                                      const transaction::ObTransID &tx_id) {
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    int ret = OB_SUCCESS;
    uint64_t hash = hash_rowkey(tablet_id, key);
    if (!tx_id.is_valid()) {
      TRANS_LOG(WARN, "trans_id is invalid", KR(ret), K(hash), K(tx_id));
    } else if (OB_FAIL(map_.insert(ObIntWarp(hash), tx_id)) && OB_ENTRY_EXIST != ret) {
      TRANS_LOG(WARN, "set hash holder error", KR(ret), K(hash), K(tx_id));
    }
  }
}

void RowHolderMapper::reset_hash_holder(const ObTabletID &tablet_id,
                                        const ObMemtableKey &key,
                                        const ObTransID &tx_id) {
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    int ret = OB_SUCCESS;
    uint64_t hash = LockHashHelper::hash_rowkey(tablet_id, key);
    auto remove_if_op = [tx_id](const ObIntWarp &k, const ObTransID &v) {
      return tx_id == v;
    };
    if (OB_FAIL(map_.erase_if(ObIntWarp(hash), remove_if_op))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        TRANS_LOG(TRACE, "clear hash holder error", KR(ret), K(hash), K(tx_id));
      }
    }
  }
}

int RowHolderMapper::get_rowkey_holder(const ObTabletID &tablet_id,
                                       const memtable::ObMemtableKey &key,
                                       transaction::ObTransID &holder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    ret = OB_NOT_RUNNING;
  } else {
    uint64_t hash = LockHashHelper::hash_rowkey(tablet_id, key);
    ret = map_.get(ObIntWarp(hash), holder);
  }
  return ret;
}

void ObLockWaitMgr::run1()
{
  int64_t last_dump_ts = 0;
  int64_t now = 0;
  lib::set_thread_name("LockWaitMgr");
  while(!has_set_stop() || !is_hash_empty()) {
    ObLink* iter = check_timeout();
    while (NULL != iter) {
      Node* cur = CONTAINER_OF(iter, Node, retire_link_);
      iter = iter->next_;
      (void)repost(cur);
    }
    // dump debug info, and check deadlock enabdle, clear mapper if deadlock is disabled
    now = ObClockGenerator::getClock();
    if (now - last_dump_ts > 5_s) {
      last_dump_ts = now;
      row_holder_mapper_.dump_mapper_info();
      if (!ObDeadLockDetectorMgr::is_deadlock_enabled()) {
        row_holder_mapper_.clear();
      }
    }
    ob_usleep(10000);
  }
}

int64_t ObLockWaitMgr::get_wait_lock_timeout(int64_t timeout)
{
  int64_t new_timeout = timeout;
  if (timeout <= 0 && NULL == get_thread_node()) {
    new_timeout = 2000;
  }
  return new_timeout;
}

bool ObLockWaitMgr::post_process(bool need_retry, bool& need_wait)
{
  bool wait_succ = false;
  Node* node = get_thread_node();
  if (node != nullptr) {
    uint64_t &hold_key = get_thread_hold_key();
    need_wait = false;
    if (0 != hold_key) {
      wakeup(hold_key);
    }
    if (need_retry) {
      if ((need_wait = node->need_wait())) {
        // FIXME(xuwang.txw):create detector in check_timeout process
        // below code must keep current order to fix concurrency bug
        // more info see
        int tmp_ret = OB_SUCCESS;
        if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
          ObTransID self_tx_id(node->tx_id_);
          ObTransID blocked_tx_id(node->holder_tx_id_);
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = register_to_deadlock_detector_(self_tx_id,
                                                                                  blocked_tx_id,
                                                                                  node)))) {
            DETECT_LOG_RET(WARN, tmp_ret, "register to deadlock detector failed", K(tmp_ret), K(*node));
          } else {
            DETECT_LOG(TRACE, "register to deadlock detector success", K(tmp_ret), K(*node));
          }
          advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::INQUEUE);
          wait_succ = wait(node);
          if (!wait_succ) {
            advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::OUTQUEUE);
          }
          if (OB_UNLIKELY(!wait_succ && (OB_SUCCESS == tmp_ret))) {
            (void) ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                                     ObTransDeadlockDetectorAdapter::
                                                                                     UnregisterPath::
                                                                                     LOCK_WAIT_MGR_WAIT_FAILED);
          }
        } else {
          advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::INQUEUE);
          wait_succ = wait(node);
          if (!wait_succ) {
            advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::OUTQUEUE);
          }
        }
        if (OB_UNLIKELY(!wait_succ)) {
          TRANS_LOG_RET(WARN, tmp_ret, "fail to wait node", KR(tmp_ret), KPC(node));
        }
      }
    }
    // CAUTION: if need_retry is false, node may be not NULL, but can't be accessed.
    // case IO thread may already free it.
  }
  return wait_succ;
}

int ObLockWaitMgr::register_to_deadlock_detector_(const ObTransID &self_tx_id,
                                                  const ObTransID &blocked_tx_id,
                                                  const Node * const node)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session_info = NULL;
  const uint32_t self_sess_id = node->sessid_;
  ObSessionGetterGuard guard(*GCTX.session_mgr_, self_sess_id);
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid session_mgr is NULL");
  } else if (OB_FAIL(guard.get_session(session_info))) {
    TRANS_LOG(WARN, "failed to get session_info", K(ret), K(self_sess_id));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "got session_info is NULL", K(ret), K(self_sess_id));
  } else {
    CollectCallBack on_collect_callback((LocalDeadLockCollectCallBack(self_tx_id,
                                                                      node->key_,
                                                                      self_sess_id)));
    if (LockHashHelper::is_rowkey_hash(node->hash())) {// waiting for row
      DeadLockBlockCallBack deadlock_block_call_back(row_holder_mapper_, node->hash());
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_row(on_collect_callback,
                                                                                                     deadlock_block_call_back,
                                                                                                     self_tx_id,
                                                                                                     self_sess_id))) {
        TRANS_LOG(WARN, "fail to regester to deadlock detector", K(ret), K(self_sess_id));
      } else {
        TRANS_LOG(TRACE, "wait for row", K(node->hash()), K(self_tx_id), K(blocked_tx_id), K(self_sess_id));
      }
    } else {// waiting for other trans
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_trans(on_collect_callback,
                                                                                                       blocked_tx_id,
                                                                                                       self_tx_id,
                                                                                                       self_sess_id))) {
        TRANS_LOG(WARN, "fail to regester to deadlock detector", K(ret), K(self_sess_id));
      } else {
        TRANS_LOG(TRACE, "wait for trans", K(node->hash()), K(self_tx_id), K(blocked_tx_id), K(self_sess_id));
      }
    }
  }
  return ret;
}

bool ObLockWaitMgr::wait(Node* node)
{
  int err = 0;
  bool wait_succ = false;
  uint64_t hash = node->hash();
  uint64_t last_lock_seq = node->lock_seq_;
  bool is_standalone_task = false;
  ATOMIC_INC(&total_wait_node_);
  if (has_set_stop()) {
    // wait fail if _stop
  } else if (check_wakeup_seq(hash, last_lock_seq, is_standalone_task)) {
    Node* tmp_node = NULL;
    {
      CriticalGuard(get_qs());
      // 1. set the task as standalone task which need to be forced to wake up
      node->try_lock_times_++;
      node->set_standalone_task(is_standalone_task);
      while(-EAGAIN == (err = hash_.insert(node)))
        ;
      assert(0 == err);

      // 2. double checkcheck_wakeup_seq
      if (!is_standalone_task && check_wakeup_seq(hash, last_lock_seq, is_standalone_task)) {
        if (is_standalone_task) {
          node->set_standalone_task(true);
        }
      }
      // 3. Exception operation
      if (has_set_stop()) {
        while(-EAGAIN == (err = hash_.del(node, tmp_node)))
          ;
        if (0 != err) {
          wait_succ = true; // maybe repost by checktimeout
          node = NULL;
        } else {
          node->try_lock_times_--;
        }
      } else {
        wait_succ = true;
      }
    }
    // 4. it should be promised that no other threads is visting the request if
    // the wait sync is needed
    if (NULL != tmp_node) {
      WaitQuiescent(get_qs());
    }
  }
  if (!wait_succ) {
    ATOMIC_DEC(&total_wait_node_);
  }
  TRANS_LOG(TRACE, "LockWaitMgr.wait", K(is_standalone_task),
            K(wait_succ), K(has_set_stop()), KPC(node));
  return wait_succ;
}

void ObLockWaitMgr::wakeup(uint64_t hash)
{
  TRANS_LOG(DEBUG, "LockWaitMgr.wakeup.start", K(hash));
  Node *node = NULL;
  do {
    node = fetch_waiter(hash);

    if (NULL != node) {
      EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
      EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, ObTimeUtility::current_time() - node->lock_ts_);
      node->on_retry_lock(hash);
      (void)repost(node);
    }
    // continue loop to wake up all requests waitting on the transaction.
    // or continue loop to wake up all requests waitting on the tablelock.
  } while (!LockHashHelper::is_rowkey_hash(hash) && node != NULL);
  TRANS_LOG(DEBUG, "LockWaitMgr.wakeup.done", K(hash));
}

ObLockWaitMgr::Node* ObLockWaitMgr::next(Node*& iter, Node* target)
{
  CriticalGuard(get_qs());
  if (NULL != (iter = hash_.next(iter))) {
    *target = *iter;
    Node *node = hash_.get_next_internal(target->hash());
    while(NULL != node && node->hash() < target->hash()) {
      node = (Node*)link_next(node);
    }
    if (NULL != node && node->hash() == target->hash()) {
      target->set_block_sessid(node->sessid_);
    }
  } else {
    target = NULL;
  }
  return target;
}

ObLockWaitMgr::Node* ObLockWaitMgr::fetch_waiter(uint64_t hash)
{
  Node* ret = NULL;
  Node* node = NULL;
  ATOMIC_INC(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
  if (ATOMIC_LOAD(&total_wait_node_) > 0) {
    CriticalGuard(get_qs());
    node = hash_.get_next_internal(hash);
    // we do not need to wake up if the request is not running
    while(NULL != node && node->hash() <= hash) {
      if (node->hash() == hash) {
        if (node->get_run_ts() > ObTimeUtility::current_time()) {
          // wake up the first task whose execution time is not yet
          break;
        } else {
          int err = 0;
          while(-EAGAIN == (err = hash_.del(node, ret)))
            ;
          if (0 != err) {
            ret = NULL;
          } else {
            ATOMIC_DEC(&total_wait_node_);
            break;
          }
        }
      }
      node = (Node*)link_next(node);
    }
  }
  if (NULL != ret) {
    WaitQuiescent(get_qs());
  }
  return ret;
}

ObLink* ObLockWaitMgr::check_timeout()
{
  ObLink* tail = NULL;
  Node* iter = NULL;
  Node* node2del = NULL;
  bool need_check_session = false;
  const int64_t MAX_WAIT_TIME_US = 10 * 1000 * 1000;
  DeadlockedSessionArray *deadlocked_session = NULL;
  fetch_deadlocked_sessions_(deadlocked_session);
  // FIX:
  // lower down session idle check frequency to 10s
  int64_t curr_ts = ObClockGenerator::getClock();
  if (curr_ts - last_check_session_idle_ts_ > MAX_WAIT_TIME_US) {
    need_check_session = true;
    last_check_session_idle_ts_ = curr_ts;
  }
  {
    CriticalGuard(get_qs());
    while(NULL != (iter = hash_.quick_next(iter))) {
      if (NULL != node2del) {
        retire_node(tail, node2del);
        node2del = NULL;
      }
      TRANS_LOG(TRACE, "LOCK_MGR: check", K(*iter));
      uint64_t hash = iter->hash();
      uint64_t last_lock_seq = iter->lock_seq_;
      uint64_t curr_lock_seq = ATOMIC_LOAD(&sequence_[(hash >> 1)% LOCK_BUCKET_COUNT]);
      if (iter->is_timeout() || has_set_stop()) {
        TRANS_LOG_RET(WARN, OB_TIMEOUT, "LOCK_MGR: req wait lock timeout", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        need_check_session = true;
        node2del = iter;
        EVENT_INC(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT);
        // it needs to be placed before the judgment of session_id to prevent the
        // abnormal case which session_id equals 0 from causing the problem of missing wakeup
      } else if (iter->is_standalone_task() && iter->get_run_ts() == 0) {
        node2del = iter;
        need_check_session = true;
        iter->on_retry_lock(hash);
        TRANS_LOG(INFO, "standalone task should be waken up", K(*iter), K(curr_lock_seq));
      } else if (iter->get_run_ts() > 0 && ObTimeUtility::current_time() > iter->get_run_ts()) {
        node2del = iter;
        need_check_session = true;
        // it is desgined to fix the case once the mvcc_write does not try
        // again, the reuqests waiting on the same row can also be wakup after
        // the request ends
        iter->on_retry_lock(hash);
        TRANS_LOG(INFO, "current task should be waken up cause reaching run ts", K(*iter));
      } else if (0 == iter->sessid_) {
        // do nothing, may be rpc plan, sessionid is not setted
      } else if (NULL != deadlocked_session
                 && is_deadlocked_session_(deadlocked_session,
                                           iter->sessid_)) {
        node2del = iter;
        TRANS_LOG(INFO, "session is deadlocked, pop the request",
                  "sessid", iter->sessid_, K(*iter));
      } else {
        // do nothing
      }
      if (need_check_session && iter->sessid_ != 0) { // when lock wait in dag worker, session is not exist
        sql::ObSQLSessionInfo *session_info = NULL;
        int ret = OB_SUCCESS;
        int tmp_ret = OB_SUCCESS;
        ObSessionGetterGuard guard(*GCTX.session_mgr_, iter->sessid_);
        if (OB_FAIL(guard.get_session(session_info))) {
          TRANS_LOG(ERROR, "failed to get session_info ", K(ret),  "sessid", iter->sessid_, K(*iter));
        } else if (OB_ISNULL(session_info)) {
          // when the request exist, the session should exist as well
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "got session_info is NULL", K(ret),  "sessid", iter->sessid_, K(*iter));
        } else if (OB_UNLIKELY(session_info->is_terminate(tmp_ret))) {
          // session is killed, just pop the request
          node2del = iter;
          TRANS_LOG(INFO, "session is killed, pop the request",  "sessid", iter->sessid_, K(*iter), K(tmp_ret));
        } else if (NULL == node2del && curr_ts - iter->lock_ts_ > MAX_WAIT_TIME_US/2) {
          // in order to prevent missing to wakeup request, so we force to wakeup every 5s
          node2del = iter;
          iter->on_retry_lock(hash);
          TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "LOCK_MGR: req wait lock cost too much time", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        } else {
          transaction::ObTxDesc *&tx_desc = session_info->get_tx_desc();
          bool ac = false, has_explicit_start_tx = session_info->has_explicit_start_trans();
          session_info->get_autocommit(ac);
          if (OB_ISNULL(tx_desc) && (!ac || has_explicit_start_tx)) {
            uint32_t session_id = session_info->get_sessid();
            const common::ObCurTraceId::TraceId &trace_id = session_info->get_current_trace_id();
            TRANS_LOG(WARN, "LOG_MGR: found session ac = 0 or has_explicit_start_trans but txDesc was released!",
                      K(session_id), K(trace_id), K(ac), K(has_explicit_start_tx));
          }
        }
        if (OB_NOT_NULL(session_info)) {
          transaction::ObTxDesc *&tx_desc = session_info->get_tx_desc();
          TRANS_LOG(INFO, "check transaction state", KP(tx_desc));
        }
      }
    }
    if (NULL != node2del) {
      retire_node(tail, node2del);
    }
  }
  if (NULL != tail) {
    WaitQuiescent(get_qs());
  }
  return tail;
}

void ObLockWaitMgr::retire_node(ObLink*& tail, Node* node)
{
  int err = 0;
  Node* tmp_node = NULL;
  EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
  EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, ObTimeUtility::current_time() - node->lock_ts_);
  while (-EAGAIN == (err = hash_.del(node, tmp_node)))
    ;
  if (0 == err) {
    node->retire_link_.next_ = tail;
    tail = &node->retire_link_;
    ATOMIC_DEC(&total_wait_node_);
  }
}

void ObLockWaitMgr::delay_header_node_run_ts(const uint64_t hash)
{
  Node* node = NULL;
  CriticalGuard(get_qs());
  node = hash_.get_next_internal(hash);
  if (NULL != node && !node->is_dummy()) {
    // delay the execution of the header node by 10ms to ensure that the remote
    // request can be executed successfully
    node->update_run_ts(ObTimeUtility::current_time() + 50 * 1000);
    TRANS_LOG(INFO, "LOCK_MGR: delay header node");
  }
}

int ObLockWaitMgr::post_lock(const int tmp_ret,
                             const ObTabletID &tablet_id,
                             const ObStoreRowkey &row_key,
                             const int64_t timeout,
                             const bool is_remote_sql,
                             const int64_t last_compact_cnt,
                             const int64_t total_trans_node_cnt,
                             const uint32_t sess_id,
                             const ObTransID &tx_id,
                             const ObTransID &holder_tx_id,
                             const ObLSID &ls_id,
                             ObFunction<int(bool &, bool &)> &rechecker)
{
  int ret = OB_SUCCESS;
  Node *node = NULL;
  if (OB_NOT_NULL(node = get_thread_node())) {
    Key key(&row_key);
    uint64_t &hold_key = get_thread_hold_key();
    if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
      uint64_t row_hash = hash_rowkey(tablet_id, key);
      uint64_t tx_hash = hash_trans(holder_tx_id);
      int64_t row_lock_seq = get_seq(row_hash);
      int64_t tx_lock_seq = get_seq(tx_hash);
      bool locked = false, wait_on_row = true;
      if (OB_FAIL(rechecker(locked, wait_on_row))) {
        TRANS_LOG(WARN, "recheck lock fail", K(key), K(holder_tx_id));
      } else if (locked) {
        uint64_t hash = wait_on_row ? row_hash : tx_hash;
        if (hold_key == hash) {
          hold_key = 0;
        }
        if (is_remote_sql) {
          delay_header_node_run_ts(hash);
        }
        transaction::ObTransService *tx_service = nullptr;
        uint32_t holder_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
        if (OB_ISNULL(tx_service = MTL(transaction::ObTransService *))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "ObTransService is null", K(sess_id), K(tx_id), K(holder_tx_id), K(ls_id));
        } else if (OB_FAIL(tx_service->get_trans_start_session_id(ls_id, holder_tx_id, holder_session_id))) {
          TRANS_LOG(WARN, "get transaction start session_id failed", K(sess_id), K(tx_id), K(holder_tx_id), K(ls_id));
        } else {
          node->set((void *)node,
                    hash,
                    wait_on_row ? row_lock_seq : tx_lock_seq,
                    timeout,
                    tablet_id.id(),
                    last_compact_cnt,
                    total_trans_node_cnt,
                    to_cstring(row_key),  // just for virtual table display
                    sess_id,
                    holder_session_id,
                    tx_id,
                    holder_tx_id,
                    ls_id);
          node->set_need_wait();
          advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::CONFLICTED);
        }
      }
    }
  }
  TRANS_LOG(TRACE, "post_lock", K(ret), K(row_key), K(tx_id), K(holder_tx_id));
  return ret;
}

int ObLockWaitMgr::post_lock(const int tmp_ret,
                             const ObTabletID &tablet_id,
                             const ObLockID &lock_id,
                             const int64_t timeout,
                             const bool is_remote_sql,
                             const int64_t last_compact_cnt,
                             const int64_t total_trans_node_cnt,
                             const uint32_t sess_id,
                             const transaction::ObTransID &tx_id,
                             const transaction::ObTransID &holder_tx_id,
                             const ObTableLockMode &lock_mode,
                             const ObLSID &ls_id,
                             ObFunction<int(bool&)> &check_need_wait)
{
  int ret = OB_SUCCESS;
  Node *node = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
  } else if (NULL == (node = get_thread_node())) {
  } else if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
    uint64_t hash = LockHashHelper::hash_lock_id(lock_id);
    const bool need_delay = is_remote_sql;
    char lock_id_buf[common::MAX_LOCK_ID_BUF_LENGTH];
    lock_id.to_string(lock_id_buf, sizeof(lock_id_buf));
    lock_id_buf[common::MAX_LOCK_ID_BUF_LENGTH - 1] = '\0';
    uint64_t &hold_key = get_thread_hold_key();
    if (hold_key == hash) {
      hold_key = 0;
    }
    if (need_delay) {
      delay_header_node_run_ts(hash);
    }
    int64_t lock_seq = get_seq(hash);
    bool need_wait = false;
    if (OB_FAIL(check_need_wait(need_wait))) {
      TRANS_LOG(WARN, "check need wait failed", K(ret));
    } else if (need_wait) {
      transaction::ObTransService *tx_service = nullptr;
      uint32_t holder_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
      if (OB_ISNULL(tx_service = MTL(transaction::ObTransService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "ObTransService is null", K(sess_id), K(tx_id), K(holder_tx_id), K(ls_id));
      } else if (OB_FAIL(tx_service->get_trans_start_session_id(ls_id, holder_tx_id, holder_session_id))) {
        TRANS_LOG(WARN, "get transaction start session_id failed", K(sess_id), K(tx_id), K(holder_tx_id), K(ls_id));
      } else {
        node->set((void*)node,
                  hash,
                  lock_seq,
                  timeout,
                  tablet_id.id(),
                  last_compact_cnt,
                  total_trans_node_cnt,
                  lock_id_buf, // just for virtual table display
                  sess_id,
                  holder_session_id,
                  tx_id,
                  holder_tx_id,
                  ls_id);
        node->set_need_wait();
        node->set_lock_mode(lock_mode);
        advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::CONFLICTED);
      }
    }
  }
  return ret;
}

int ObLockWaitMgr::repost(Node* node)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TRANS_LOG(TRACE, "LockWaitMgr.repost", KPC(node));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "lock wait mgr not inited", K(ret));
  } else {
    ObTransID self_tx_id(node->tx_id_);
    ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                      ObTransDeadlockDetectorAdapter::
                                                                      UnregisterPath::
                                                                      LOCK_WAIT_MGR_REPOST);
    node->advance_stat(rpc::RequestLockWaitStat::RequestStat::OUTQUEUE);
    if (OB_FAIL(OBSERVER.get_net_frame().get_deliver().repost((void*)node))) {
      TRANS_LOG(WARN, "report error", K(ret));
    }
  }

  return ret;
}

// wait lock on trans_id
int ObLockWaitMgr::transform_row_lock_to_tx_lock(const ObTabletID &tablet_id,
                                                 const Key &row_key,
                                                 const ObTransID &tx_id,
                                                 const ObAddr &tx_scheduler)
{
  int ret = OB_SUCCESS;

  uint64_t hash_tx_id = LockHashHelper::hash_trans(tx_id);
  uint64_t hash_row_key = LockHashHelper::hash_rowkey(tablet_id, row_key);

  int64_t lock_seq = get_seq(hash_tx_id);
  Node *node = NULL;
  while (NULL != (node = fetch_waiter(hash_row_key))) {
    int tmp_ret = OB_SUCCESS;
    ObTransID self_tx_id(node->tx_id_);
    if (tx_scheduler.is_valid()) {
      ObTransDeadlockDetectorAdapter::change_detector_waiting_obj_from_row_to_trans(self_tx_id, tx_id, tx_scheduler);
    } else {
      TRANS_LOG(WARN, "tx scheduler is invalid", K(tx_scheduler), K(tx_id), K(row_key));
    }
    node->change_hash(hash_tx_id, lock_seq);

    if (!wait(node)) {
      repost(node);
    } else {
      // remove the repeated calculations
      node->try_lock_times_--;
    }
  }

  return ret;
}

void ObLockWaitMgr::wakeup(const ObTabletID &tablet_id, const Key& key)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byRowKey", K(tablet_id), K(key), K(lbt()));
  wakeup(LockHashHelper::hash_rowkey(tablet_id, key));
}

void ObLockWaitMgr::wakeup(const ObTransID &tx_id)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byTransID", K(tx_id), K(lbt()));
  wakeup(LockHashHelper::hash_trans(tx_id));
}

void ObLockWaitMgr::wakeup(const ObLockID &lock_id)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byLockID", K(lock_id), K(lbt()));
  wakeup(LockHashHelper::hash_lock_id(lock_id));
}

int ObLockWaitMgr::fullfill_row_key(uint64_t hash, char *row_key, int64_t length)
{
  int ret = OB_SUCCESS;
  CriticalGuard(get_qs());
  Node *node = NULL;

  if (hash_.get(hash, node)) {
    snprintf(row_key, std::min(length, static_cast<int64_t>(sizeof(node->key_)) + 1), "%s", node->key_);
  } else {
    const char *err = "can't found";
    snprintf(row_key, std::min(length, static_cast<int64_t>(strlen(err)) + 1), "%s", err);
  }

  return ret;
}

int ObLockWaitMgr::notify_deadlocked_session(const uint32_t sess_id)
{
  ObSpinLockGuard guard(deadlocked_sessions_lock_);
  return deadlocked_sessions_[deadlocked_sessions_index_].push_back(SessPair{sess_id});
}

void ObLockWaitMgr::fetch_deadlocked_sessions_(DeadlockedSessionArray* &sessions)
{
  ObSpinLockGuard guard(deadlocked_sessions_lock_);

  deadlocked_sessions_index_ = 1 - deadlocked_sessions_index_;
  if (0 != deadlocked_sessions_index_
      && 1 != deadlocked_sessions_index_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected deadlocked session index", K(deadlocked_sessions_index_));
  }
  deadlocked_sessions_[deadlocked_sessions_index_].reset();
  sessions = deadlocked_sessions_ + (1 - deadlocked_sessions_index_);
}

bool ObLockWaitMgr::is_deadlocked_session_(DeadlockedSessionArray *sessions,
                                           const uint32_t sess_id)
{
  bool is_deadlocked_session = false;

  for (int64_t i = 0; i < sessions->count(); i++) {
    if (sess_id == (*sessions)[i].sess_id_) {
      is_deadlocked_session = true;
    }
  }

  return is_deadlocked_session;
}

}; // end namespace memtable
}; // end namespace oceanbase

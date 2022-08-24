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

#include "lib/hash_func/murmur_hash.h"
#include "observer/ob_server.h"

namespace oceanbase {
namespace obmysql {

// replace function in library
void request_finish_callback()
{
  bool unused = false;
  memtable::ObLockWaitMgr& lock_wait_mgr = memtable::get_global_lock_wait_mgr();
  lock_wait_mgr.post_process(false, unused);
  lock_wait_mgr.clear_thread_node();
}

}  // namespace obmysql
}  // namespace oceanbase
namespace oceanbase {

using namespace transaction;
using namespace storage;
using namespace common::hash;
using namespace common;

namespace memtable {

static const uint64_t TRANS_MARK = 1L << 63L;

static inline uint64_t hash_rowkey(const ObLockWaitMgr::Key& key)
{
  return (key.hash() & ~TRANS_MARK) | 1;
}

static inline uint64_t hash_trans(const uint32_t ctx_desc)
{
  return (murmurhash(&ctx_desc, sizeof(ctx_desc), 0) | TRANS_MARK) | 1;
}

ObLockWaitMgr::ObLockWaitMgr() : is_inited_(false), hash_(hash_buf_, sizeof(hash_buf_)), mt_id_map_(NULL)
{
  memset(sequence_, 0, sizeof(sequence_));
}

ObLockWaitMgr::~ObLockWaitMgr()
{}

int ObLockWaitMgr::init()
{
  int ret = OB_SUCCESS;
  ObIMemtableCtxFactory* imt_ctx_factory = NULL;
  ObMemtableCtxFactory* mt_ctx_factory = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ob lock wait mgr already init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(imt_ctx_factory = ObPartitionService::get_instance().get_mem_ctx_factory())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "can't get imem_ctx_factory from ObPartitionService", K(ret));
  } else if (OB_ISNULL(mt_ctx_factory = static_cast<ObMemtableCtxFactory*>(imt_ctx_factory))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "can't get mem_ctx_factory from imem_ctx_factory", K(ret), K(imt_ctx_factory));
  } else {
    mt_id_map_ = &(mt_ctx_factory->get_id_map());
    is_inited_ = true;
  }

  return ret;
}

void ObLockWaitMgr::destroy()
{
  is_inited_ = false;
}

void ObLockWaitMgr::run1()
{
  const int interval_us = (lib::is_mini_mode() ? 10 : 1000) * 1000;

  (void)prctl(PR_SET_NAME, "ObLockWaitMgr", 0, 0, 0);
  while (!has_set_stop() || !is_hash_empty()) {
    ObLink* iter = check_timeout();
    while (NULL != iter) {
      Node* cur = CONTAINER_OF(iter, Node, retire_link_);
      iter = iter->next_;
      (void)repost(cur);
    }

    usleep(interval_us);
  }
}

void ObLockWaitMgr::setup(Node& node, int64_t recv_ts)
{
  node.reset_need_wait();
  node.recv_ts_ = recv_ts;
  get_thread_node() = &node;
  get_thread_hold_key() = node.hold_key_;
  node.hold_key_ = 0;
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
    uint64_t& hold_key = get_thread_hold_key();
    need_wait = false;
    if (0 != hold_key) {
      wakeup(hold_key);
    }
    if (need_retry) {
      if ((need_wait = node->need_wait())) {
        wait_succ = wait(node);
      }
    }
  }
  return wait_succ;
}

bool ObLockWaitMgr::wait(Node* node)
{
  int err = 0;
  bool wait_succ = false;
  uint64_t hash = node->hash();
  uint64_t last_lock_seq = node->lock_seq_;
  bool is_standalone_task = false;
  if (has_set_stop()) {
    // wait fail if _stop
  } else if (check_wakeup_seq(hash, last_lock_seq, is_standalone_task)) {
    Node* tmp_node = NULL;
    {
      CriticalGuard(get_qs());
      // 1. set the task as standalone task which need to be forced to wake up
      node->try_lock_times_++;
      node->set_standalone_task(is_standalone_task);
      while (-EAGAIN == (err = hash_.insert(node)))
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
        while (-EAGAIN == (err = hash_.del(node, tmp_node)))
          ;
        if (0 != err) {
          wait_succ = true;  // maybe repost by checktimeout
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
  return wait_succ;
}

void ObLockWaitMgr::wakeup(uint64_t hash)
{
  Node* node = fetch_waiter(hash);

  if (NULL != node) {
    EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
    EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, obsys::ObSysTimeUtil::getTime() - node->lock_ts_);
    node->on_retry_lock(hash);

    (void)repost(node);
  }
}

ObLockWaitMgr::Node* ObLockWaitMgr::next(Node*& iter, Node* target)
{
  CriticalGuard(get_qs());
  if (NULL != (iter = hash_.next(iter))) {
    *target = *iter;
    Node* node = hash_.get_next_internal(target->hash());
    while (NULL != node && node->hash() < target->hash()) {
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
  {
    CriticalGuard(get_qs());
    ATOMIC_INC(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
    node = hash_.get_next_internal(hash);
    // we do not need to wake up if the request is not running
    while (NULL != node && node->hash() <= hash) {
      if (node->hash() == hash) {
        if (node->get_run_ts() > obsys::ObSysTimeUtil::getTime()) {
          // wake up the first task whose execution time is not yet
          break;
        } else {
          int err = 0;
          while (-EAGAIN == (err = hash_.del(node, ret)))
            ;
          if (0 != err) {
            ret = NULL;
          } else {
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
  static int64_t last_check_session_idle_ts = 0;
  bool need_check_session = false;
  const int64_t MAX_WAIT_TIME_US = 10 * 1000 * 1000;
  // lower down session idle check frequency to 10s
  int64_t curr_ts = ObClockGenerator::getClock();
  if (curr_ts - last_check_session_idle_ts > MAX_WAIT_TIME_US) {
    need_check_session = true;
    last_check_session_idle_ts = curr_ts;
  }
  {
    CriticalGuard(get_qs());
    while (NULL != (iter = hash_.quick_next(iter))) {
      if (NULL != node2del) {
        retire_node(tail, node2del);
        node2del = NULL;
      }
      TRANS_LOG(TRACE, "LOCK_MGR: check", K(*iter));
      uint64_t hash = iter->hash();
      uint64_t last_lock_seq = iter->lock_seq_;
      uint64_t curr_lock_seq = ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
      if (iter->is_timeout() || has_set_stop()) {
        TRANS_LOG(WARN, "LOCK_MGR: req wait lock timeout", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        need_check_session = true;
        node2del = iter;
        EVENT_INC(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT);
        // it needs to be placed before the judgment of session_id to prevent the
        // abnormal case which session_id equals 0 from causing the problem of missing wakeup
      } else if (iter->is_standalone_task() ||
                 (iter->get_run_ts() > 0 && obsys::ObSysTimeUtil::getTime() > iter->get_run_ts())) {
        node2del = iter;
        need_check_session = true;
        // it is desgined to fix the case once the lock_for_write does not try
        // again, the reuqests waiting on the same row can also be wakup after
        // the request ends
        iter->on_retry_lock(hash);
        TRANS_LOG(INFO, "current task should be waken up", K(*iter));
      } else if (0 == iter->sessid_) {
        // do nothing, may be rpc plan, sessionid is not setted
      } else {
        // do nothing
      }
      if (need_check_session && iter->sessid_ != 0) { // when lock wait in dag worker, session is not exist
        sql::ObSQLSessionInfo* session_info = NULL;
        int ret = OB_SUCCESS;
        int tmp_ret = OB_SUCCESS;
        if (OB_ISNULL(GCTX.session_mgr_)) {
          TRANS_LOG(ERROR, "invalid session_mgr is NULL");
        } else if (OB_FAIL(GCTX.session_mgr_->get_session(iter->sessid_version_, iter->sessid_, session_info))) {
          TRANS_LOG(ERROR,
              "failed to get session_info ",
              K(ret),
              "version",
              iter->sessid_version_,
              "sessid",
              iter->sessid_,
              K(*iter));
        } else if (OB_ISNULL(session_info)) {
          // when the request exist, the session should exist as well
          TRANS_LOG(ERROR,
              "got session_info is NULL",
              K(ret),
              "version",
              iter->sessid_version_,
              "sessid",
              iter->sessid_,
              K(*iter));
        } else if (OB_UNLIKELY(session_info->is_terminate(tmp_ret))) {
          // session is killed, just pop the request
          node2del = iter;
          TRANS_LOG(INFO,
              "session is killed, pop the request",
              "version",
              iter->sessid_version_,
              "sessid",
              iter->sessid_,
              K(*iter),
              K(tmp_ret));
        } else if (NULL == node2del && curr_ts - iter->lock_ts_ > MAX_WAIT_TIME_US / 2) {
          // in order to prevent missing to wakeup request, so we force to wakeup every 5s
          node2del = iter;
          iter->on_retry_lock(hash);
          TRANS_LOG(WARN, "LOCK_MGR: req wait lock cost too much time", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        } else { /*do nothing*/
        }

        if (NULL != session_info) {
          if (need_check_session) {
            TRANS_LOG(INFO, "check transaction state", "trans_desc", session_info->get_trans_desc());
          }
          if (OB_FAIL(GCTX.session_mgr_->revert_session(session_info))) {
            TRANS_LOG(ERROR,
                "failed to revert session",
                "version",
                iter->sessid_version_,
                "sessid",
                iter->sessid_,
                K(*iter),
                K(ret));
          }
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
  EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, obsys::ObSysTimeUtil::getTime() - node->lock_ts_);
  while (-EAGAIN == (err = hash_.del(node, tmp_node)))
    ;
  if (0 == err) {
    node->retire_link_.next_ = tail;
    tail = &node->retire_link_;
  }
}

void ObLockWaitMgr::delay_header_node_run_ts(const Key& key)
{
  uint64_t hash = (key.hash() | 1);
  Node* node = NULL;
  CriticalGuard(get_qs());
  node = hash_.get_next_internal(hash);
  if (NULL != node && !node->is_dummy()) {
    // delay the execution of the header node by 10ms to ensure that the remote
    // request can be executed successfully
    node->update_run_ts(obsys::ObSysTimeUtil::getTime() + 50 * 1000);
    TRANS_LOG(INFO, "LOCK_MGR: delay header node");
  }
}

int ObLockWaitMgr::post_lock(int tmp_ret, ObRowLock& lock, const Key& key, int64_t timeout, const bool is_remote_sql,
    const bool can_elr, const int64_t total_trans_node_cnt, uint32_t ctx_desc)
{
  int ret = OB_SUCCESS;
  Node* node = get_thread_node();

  if (node != nullptr) {
    uint64_t& hold_key = get_thread_hold_key();
    if (hold_key == hash_rowkey(key)) {
      hold_key = 0;
    }

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
    } else if (NULL != node) {
      if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
        if (is_remote_sql && can_elr) {
          delay_header_node_run_ts(key);
        }

        node->set((void*)node,
            hash_rowkey(key),
            get_seq(hash_rowkey(key)),
            timeout,
            key.get_table_id(),
            total_trans_node_cnt,
            GCONF.enable_record_trace_log ? to_cstring(key) : NULL,
            ctx_desc);

        if (lock.is_locked()) {
          node->set_need_wait();
        }
      }
    }
  }

  return ret;
}

int ObLockWaitMgr::repost(Node* node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "lock wait mgr not inited", K(ret));
  } else if (OB_FAIL(OBSERVER.get_net_frame().get_deliver().repost((void*)node))) {
    TRANS_LOG(WARN, "report error", K(ret), K(*node));
  }
  return ret;
}

int ObLockWaitMgr::post_lock(int tmp_ret, const ObIMvccCtx& ctx, const uint64_t table_id, const ObStoreRowkey& key,
    int64_t timeout, const int64_t total_trans_node_cnt, uint32_t ctx_desc)
{
  int ret = OB_SUCCESS;

  Node* node = NULL;

  if (NULL == (node = get_thread_node())) {
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
  } else {
    if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
      const uint32_t owner_desc = ctx.get_ctx_descriptor();
      uint64_t hash_trx = hash_trans(owner_desc);

      node->set(
          (void*)node, hash_trx, get_seq(hash_trx), timeout, table_id, total_trans_node_cnt, to_cstring(key), ctx_desc);

      if (!ctx.is_rowlocks_released()) {
        node->set_need_wait();
        TRANS_LOG(DEBUG, "try lock wait on trx", K(key), K(ctx_desc), K(owner_desc), K(hash_trx));
      }
    }
  }

  return ret;
}

int ObLockWaitMgr::delegate_waiting_querys_to_trx(const Key& key, const ObIMvccCtx& ctx)
{
  int ret = OB_SUCCESS;

  const uint32_t ctx_desc = ctx.get_ctx_descriptor();
  uint64_t hash_trx = hash_trans(ctx_desc);
  uint64_t hash_row = hash_rowkey(key);

  int64_t lock_seq = get_seq(hash_trx);
  Node* node = NULL;
  while (NULL != (node = fetch_waiter(hash_row))) {
    node->change_hash(hash_trx, lock_seq);

    if (!wait(node)) {
      repost(node);
    } else {
      // remove the repeated calculations
      node->try_lock_times_--;
    }
  }

  return ret;
}

void ObLockWaitMgr::wakeup(const Key& key)
{
  wakeup(hash_rowkey(key));
}

void ObLockWaitMgr::wakeup(const uint32_t ctx_desc)
{
  wakeup(hash_trans(ctx_desc));
}

int ObLockWaitMgr::fullfill_row_key(uint64_t hash, char* row_key, int64_t length)
{
  int ret = OB_SUCCESS;
  CriticalGuard(get_qs());
  Node* node = NULL;

  if (hash_.get(hash, node)) {
    snprintf(row_key, std::min(length, static_cast<int64_t>(sizeof(node->key_)) + 1), "%s", node->key_);
  } else {
    const char* err = "can't found";
    snprintf(row_key, std::min(length, static_cast<int64_t>(strlen(err)) + 1), "%s", err);
  }

  return ret;
}
};  // end namespace memtable
};  // end namespace oceanbase

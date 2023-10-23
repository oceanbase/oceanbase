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

#include "ob_trans_service.h"
#include "common/storage/ob_sequence.h"
#include "ob_trans_part_ctx.h"
#include "wrs/ob_i_weak_read_service.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/tx/wrs/ob_weak_read_service.h"
#include "storage/tx/wrs/ob_weak_read_util.h"
#include "ob_xa_service.h"
// ------------------------------------------------------------------------------------------
// Implimentation notes:
// there are two relation we need care:
// a) the relation between data: WAR, we need read data after writen
// b) the relation between operations:
//    i. write happened before rollback, to barrier write after rollback
//    ii. savepoint happened before following writes, to barrier write after savepoint
//
// thus, the interface use two logical clock to track these relations:
// 1. the Logical Clock for data relation, and
// 2. the In-Transaction-Clock for operation relation
//
// ::get_snapshots::
//   1. advance Logical Clock to establish data relation of Write-After-Read
// ::create savepoint::
//   1. advance Logical Clock to establish data relation of Write-After-SavePoint
//   2. advance In-Transaction-Clock to establish operation relation of Write-After-SavePoint
// ::rollback to savepoint::
//   1. advance In-Transaction-Clock to establish operation relation of Rollback-After-Write
// -------------------------------------------------------------------------------------------
#define TXN_API_SANITY_CHECK_FOR_TXN_FREE_ROUTE(end_txn)                \
  do {                                                                  \
    bool inv = false;                                                   \
    if (tx.is_xa_trans()) {                                             \
      inv = end_txn ? tx.addr_ != self_ : tx.xa_start_addr_ != self_;   \
    } else {                                                            \
      inv = tx.addr_ != self_;                                          \
    }                                                                   \
    if (inv) {                                                          \
      int ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;                      \
      TRANS_LOG(ERROR, "incorrect route of txn free route", K(ret), K(tx)); \
      return ret;                                                       \
    }                                                                   \
  } while (0);

namespace oceanbase {

using namespace share;

namespace transaction {

inline void ObTransService::init_tx_(ObTxDesc &tx, const uint32_t session_id)
{
  tx.tenant_id_ = tenant_id_;
  tx.addr_      = self_;
  tx.sess_id_   = session_id;
  tx.assoc_sess_id_ = session_id;
  tx.alloc_ts_  = ObClockGenerator::getClock();
  tx.expire_ts_ = INT64_MAX;
  tx.op_sn_     = 1;
  tx.state_     = ObTxDesc::State::IDLE;
}

int ObTransService::acquire_tx(ObTxDesc *&tx, const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_desc_mgr_.alloc(tx))) {
    TRANS_LOG(WARN, "alloc tx fail", K(ret));
  } else {
    init_tx_(*tx, session_id);
  }
  TRANS_LOG(TRACE, "acquire tx", KPC(tx), K(session_id));
  if (OB_SUCC(ret)) {
    ObTransTraceLog &tlog = tx->get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, acquire, OB_Y(ret),
                        OB_ID(addr), (void*)tx,
                        OB_ID(session), session_id,
                        OB_ID(ref), tx->get_ref(),
                        OB_ID(thread_id), GETTID());
  }
  return ret;
}

int ObTransService::finalize_tx_(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  if (!tx.flags_.RELEASED_) {
    tx.flags_.RELEASED_ = true;
    if (tx.is_tx_active() && !tx.flags_.REPLICA_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "release tx when tx is active", K(ret), KPC(this), K(tx));
      tx.print_trace_();
    } else if (tx.is_committing()) {
      TRANS_LOG(WARN, "release tx when tx is committing", KPC(this), K(tx));
    }
    tx.cancel_commit_cb();
    if (tx.tx_id_.is_valid()) {
      tx_desc_mgr_.remove(tx);
    }
  }
  return ret;
}

/*
 * release_tx - release the Tx object
 *
 * release tx is the final step for user operate on TxDesc.
 * generally, user should commit / rollback the tx before they release it.
 * the txDesc object should not been access anymore after release.
 *
 * - for tx in async committing
 *   the commit callback will not be called if not already called, and
 *   don't forget to call release_tx before release callback's memory
 * - for tx which is a shadow copy of original tx (started on another server)
 *   release just free its memory used
 */
int ObTransService::release_tx(ObTxDesc &tx)
{
  /*
   * for compatible with cross tenant session usage
   * we should switch tenant to prevent missmatch
   * eg.
   *    SYS tenant swith to Normal tenant execute SQL
   *    and then destory its session after switch back
   */
  int ret = OB_SUCCESS;
  TRANS_LOG(TRACE, "release tx", KPC(this), K(tx));
  if (tx.tenant_id_ != MTL_ID()) {
    MTL_SWITCH(tx.tenant_id_) {
      return MTL(ObTransService*)->release_tx(tx);
    }
  } else {
    ObTransTraceLog &tlog = tx.get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, release, OB_Y(ret),
                        OB_ID(ref), tx.get_ref(),
                        OB_ID(thread_id), GETTID());
    if (tx.flags_.SHADOW_) {
#ifndef NDEBUG
      if (tx.tx_id_.is_valid()) {
        tx.print_trace();
      }
#endif
      tx_desc_mgr_.revert(tx);
    } else {
      finalize_tx_(tx);
      tx_desc_mgr_.revert(tx);
    }
  }
  TRANS_LOG(TRACE, "release tx done", KP(&tx), KPC(this), K(lbt()));
  return ret;
}

int ObTransService::reuse_tx(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  int spin_cnt = 0;
  int final_ref_cnt = 0;
  ObTransID orig_tx_id = tx.tx_id_;
  if (tx.is_in_tx() && !tx.is_tx_end()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "can not reuse tx which has active and not end yet", K(ret), K(tx.tx_id_));
  } else if (OB_FAIL(finalize_tx_(tx))) {
    TRANS_LOG(WARN, "finalize tx fail", K(ret), K(tx.tx_id_));
  } else {
    // after finalize tx, the txDesc can not be fetch from TxDescMgr
    // but the reference maybe hold by user, wait to be queisenct
    // before we reuse it

    // if reuse come from commit_cb, assume current thread hold one reference
    final_ref_cnt = tx.commit_cb_lock_.self_locked() ? 2 : 1;
    while (tx.get_ref() > final_ref_cnt) {
      PAUSE();
      if (++spin_cnt > 2000) {
        TRANS_LOG(WARN, "blocking to wait tx referent quiescent cost too much time",
                  "tx_id", orig_tx_id, KP(&tx), K(final_ref_cnt), K(spin_cnt), K(tx.get_ref()));
        tx.print_trace();
        usleep(2000000); // 2s
      } else if (spin_cnt > 200) {
        usleep(2000);    // 2ms
      } else if (spin_cnt > 100) {
        usleep(200);     // 200us
      }
    }
    // it is safe to operate tx without lock when not shared
    uint32_t session_id = tx.sess_id_;
    tx.reset();
    init_tx_(tx, session_id);
  }
  TRANS_LOG(TRACE, "reuse tx", K(ret), K(orig_tx_id), K(tx));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, reuse, OB_Y(ret),
                      OB_ID(addr), (void*)&tx,
                      OB_ID(txid), orig_tx_id,
                      OB_ID(tag1), spin_cnt,
                      OB_ID(tag2), final_ref_cnt,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::stop_tx(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  tx.lock_.lock();
  TRANS_LOG(INFO, "stop_tx, print its trace as following", K(tx));
  tx.print_trace_();
  if (tx.addr_ != self_) {
    // either on txn temp node or xa temp node
    // depends on session cleanup to quit
    TRANS_LOG(INFO, "this is not txn start node.");
  } else {
    if (tx.state_ < ObTxDesc::State::IN_TERMINATE) {
      abort_tx_(tx, ObTxAbortCause::STOP, true);
    } else if (!tx.is_terminated()) {
      unregister_commit_retry_task_(tx);
      // arm callback arguments
      tx.commit_out_ = OB_TRANS_UNKNOWN;
      tx.state_ = ObTxDesc::State::COMMIT_UNKNOWN;
    }
    tx.lock_.unlock();
    // run callback after unlock
    tx.execute_commit_cb();
  }
  return ret;
}

int ObTransService::start_tx(ObTxDesc &tx, const ObTxParam &tx_param, const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  if (!tx_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid tx param", K(ret), KR(ret), K(tx_param));
  } else {
    TX_STAT_START_INC
      ObSpinLockGuard guard(tx.lock_);
    tx.inc_op_sn();
    if (!tx_id.is_valid()) {
      ret = tx_desc_mgr_.add(tx);
    } else {
      ret = tx_desc_mgr_.add_with_txid(tx_id, tx);
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "add tx to txMgr fail", K(ret), K(tx));
    } else {
      tx.cluster_version_ = GET_MIN_CLUSTER_VERSION();
      tx.cluster_id_      = tx_param.cluster_id_;
      tx.access_mode_     = tx_param.access_mode_;
      tx.isolation_       = tx_param.isolation_;
      tx.active_ts_       = ObClockGenerator::getClock();
      tx.timeout_us_      = tx_param.timeout_us_;
      tx.lock_timeout_us_ = tx_param.lock_timeout_us_;
      int64_t a = tx.timeout_us_ + tx.active_ts_;
      tx.expire_ts_       = a < 0 ? INT64_MAX : a;
      // start tx need reacquire snapshot
      tx.snapshot_version_.reset();
      // setup correct active_scn, whatever its used or not
      tx.active_scn_      = tx.get_tx_seq();
      tx.state_           = ObTxDesc::State::ACTIVE;
      tx.flags_.EXPLICIT_ = true;
    }
    ObTransTraceLog &tlog = tx.get_tlog();
    REC_TRANS_TRACE_EXT(&tlog, start_tx, OB_Y(ret),
                        OB_ID(txid), tx.tx_id_,
                        OB_ID(isolation_level), (int)tx.isolation_,
                        OB_ID(ref), tx.get_ref(),
                        OB_ID(thread_id), GETTID());
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "start tx failed", K(ret), K(tx));
  } else {
    tx.state_change_flags_.mark_all();
#ifndef NDEBUG
    TRANS_LOG(INFO, "start tx succeed", K(tx));
#endif
  }
  return ret;
}

int ObTransService::rollback_tx(ObTxDesc &tx)
{
  TXN_API_SANITY_CHECK_FOR_TXN_FREE_ROUTE(true)
  int ret = OB_SUCCESS;
  TX_STAT_ROLLBACK_INC
  ObSpinLockGuard guard(tx.lock_);
  tx.inc_op_sn();
  switch(tx.state_) {
  case ObTxDesc::State::ABORTED:
    TX_STAT_ROLLBACK_INC
    tx.state_ = ObTxDesc::State::ROLLED_BACK;
    break;
  case ObTxDesc::State::ROLLED_BACK:
    ret = OB_TRANS_ROLLBACKED;
    TRANS_LOG(WARN, "tx rollbacked", K(ret), K(tx));
    break;
  case ObTxDesc::State::COMMITTED:
    ret = OB_TRANS_COMMITED;
    TRANS_LOG(WARN, "tx committed", K(ret), K(tx));
    break;
  case ObTxDesc::State::IN_TERMINATE:
  case ObTxDesc::State::COMMIT_TIMEOUT:
  case ObTxDesc::State::COMMIT_UNKNOWN:
    ret = OB_TRANS_HAS_DECIDED;
    TRANS_LOG(WARN, "tx in terminating", K(ret), K(tx));
    break;
  case ObTxDesc::State::ACTIVE:
  case ObTxDesc::State::IMPLICIT_ACTIVE:
    TX_STAT_ROLLBACK_INC
    tx.state_ = ObTxDesc::State::IN_TERMINATE;
    tx.abort_cause_ = OB_TRANS_ROLLBACKED;
    abort_participants_(tx);
  case ObTxDesc::State::IDLE:
    TX_STAT_ROLLBACK_INC
    tx.state_ = ObTxDesc::State::ROLLED_BACK;
    tx.finish_ts_ = ObClockGenerator::getClock();
    tx_post_terminate_(tx);
    break;
  default:
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "invalid state", K(ret), K_(tx.state), K(tx));
  }
  TRANS_LOG(INFO, "rollback tx", K(ret), K(*this), K(tx));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, rollback_tx, OB_Y(ret),
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

// impl note
// abort tx should invalidate registered snapshot
// savepoint not invalidate, they were invalidate
// when do explicit rollback
int ObTransService::abort_tx(ObTxDesc &tx, int cause)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  tx.inc_op_sn();
  if (tx.state_ != ObTxDesc::State::ABORTED) {
    ret = abort_tx_(tx, cause);
  }
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, abort_tx, OB_Y(ret),
                      OB_ID(arg), cause,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  tx.print_trace_();
  return ret;
}

namespace {
  struct SyncTxCommitCb : public ObITxCallback
  {
    public:
    void callback(int ret) { cond_.notify(ret); }
    int wait(const int64_t time_us, int &ret) {
      return cond_.wait(time_us, ret);
    }
    ObTransCond cond_;
  };
}

int ObTransService::commit_tx(ObTxDesc &tx, const int64_t expire_ts, const ObString *trace_info)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  SyncTxCommitCb cb;
  if (OB_SUCC(submit_commit_tx(tx, expire_ts, cb, trace_info))) {
    int result = 0;
    // plus 10s to wait callback, if callback leaky, wakeup self
    int64_t wait_us = MAX(expire_ts - ObTimeUtility::current_time(), 0) + 10 * 1000 * 1000L;
    if (OB_FAIL(cb.wait(wait_us, result))) {
      TRANS_LOG(WARN, "wait commit fail", K(ret), K(expire_ts), K(wait_us), K(tx));
      /* NOTE: must cancel callback before release it */
      tx.cancel_commit_cb();
      ret = ret == OB_TIMEOUT ? OB_TRANS_STMT_TIMEOUT : ret;
    } else {
      ret = result;
    }
  }
  int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
#ifndef NDEBUG
  TRANS_LOG(INFO, "sync commit tx", K(ret), K(tx), K(expire_ts));
#else
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "sync commit tx fail", K(ret), K(tx), K(expire_ts));
  }
#endif
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, commit_tx, OB_Y(ret), OB_Y(expire_ts),
                      OB_ID(time_used), elapsed_us,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  if (OB_FAIL(ret)) {
    tx.print_trace();
  }
  return ret;
}

// impl note
// imediately succeed cases:
//   1. idle state
//   2. empty valid part
// imediately fail cases:
//   1. aborted state
//   2. incorrect state
//   3. tx-timeout state
// on commit finish:
//   1. release savepoints
// on commit fail:
//   1. invalid registered snapshot
int ObTransService::submit_commit_tx(ObTxDesc &tx,
                                     const int64_t expire_ts,
                                     ObITxCallback &cb,
                                     const ObString *trace_info)
{
  TXN_API_SANITY_CHECK_FOR_TXN_FREE_ROUTE(true)
  int ret = OB_SUCCESS;
  tx.lock_.lock();
  if (tx.commit_ts_ <= 0) {
    tx.commit_ts_ = ObClockGenerator::getClock();
  }
  tx.inc_op_sn();
  switch(tx.state_) {
  case ObTxDesc::State::IDLE:
    TRANS_LOG(TRACE, "commit a dummy tx", K(tx), KP(&cb));
    tx.set_commit_cb(&cb);
    handle_tx_commit_result_(tx, OB_SUCCESS);
    ret = OB_SUCCESS;
    break;
  case ObTxDesc::State::ABORTED:
    handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
    ret = OB_TRANS_ROLLBACKED;
    break;
  case ObTxDesc::State::ROLLED_BACK:
    ret = OB_TRANS_ROLLBACKED;
    TRANS_LOG(WARN, "insane tx action", K(ret), K(tx));
    break;
  case ObTxDesc::State::COMMITTED:
    ret = OB_TRANS_COMMITED;
    TRANS_LOG(WARN, "insane tx action", K(ret), K(tx));
    break;
  case ObTxDesc::State::IN_TERMINATE:
  case ObTxDesc::State::COMMIT_TIMEOUT:
  case ObTxDesc::State::COMMIT_UNKNOWN:
    ret = OB_TRANS_HAS_DECIDED;
    TRANS_LOG(WARN, "insane tx action", K(ret), K(tx));
    break;
  case ObTxDesc::State::ACTIVE:
  case ObTxDesc::State::IMPLICIT_ACTIVE:
    if (tx.expire_ts_ <= ObClockGenerator::getClock()) {
      TX_STAT_TIMEOUT_INC
      TRANS_LOG(WARN, "tx has timeout, it has rollbacked internally", K_(tx.expire_ts), K(tx));
      ret = OB_TRANS_ROLLBACKED;
      handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
    } else if (tx.flags_.PARTS_INCOMPLETE_) {
      TRANS_LOG(WARN, "txn participants set incomplete, can not commit", K(ret), K(tx));
      abort_tx_(tx, ObTxAbortCause::PARTICIPANTS_SET_INCOMPLETE);
      handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
      ret = OB_TRANS_ROLLBACKED;
    } else if (tx.flags_.PART_EPOCH_MISMATCH_) {
      TRANS_LOG(WARN, "txn participant state incomplete, can not commit", K(ret), K(tx));
      abort_tx_(tx, ObTxAbortCause::PARTICIPANT_STATE_INCOMPLETE);
      handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
      ret = OB_TRANS_ROLLBACKED;
    } else {
      int clean = true;
      ARRAY_FOREACH_X(tx.parts_, i, cnt, clean) {
        clean = tx.parts_[i].is_without_ctx() || tx.parts_[i].is_clean();
      }
      if (clean) {
        // explicit savepoint rollback cause empty valid-part-set
        tx.set_commit_cb(&cb);
        abort_participants_(tx);                  // let part ctx quit
        handle_tx_commit_result_(tx, OB_SUCCESS); // commit success
        ret = OB_SUCCESS;
      }
    }
    break;
  default:
    TRANS_LOG(WARN, "anormaly tx state", K(tx));
    abort_tx_(tx, ObTxAbortCause::IN_CONSIST_STATE);
    handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
    ret = OB_TRANS_ROLLBACKED;
  }
  // normal path, commit cont.
  if (OB_SUCC(ret) && (
      tx.state_ == ObTxDesc::State::ACTIVE ||
      tx.state_ == ObTxDesc::State::IMPLICIT_ACTIVE)) {
    ObTxDesc::State state0 = tx.state_;
    tx.state_ = ObTxDesc::State::IN_TERMINATE;
    // record trace_info
    if (OB_NOT_NULL(trace_info) &&
        OB_FAIL(tx.trace_info_.set_app_trace_info(*trace_info))) {
      TRANS_LOG(WARN, "set trace_info failed", K(ret), KPC(trace_info));
    }
    SCN commit_version;
    if (OB_SUCC(ret) &&
        OB_FAIL(do_commit_tx_(tx, expire_ts, cb, commit_version))) {
      TRANS_LOG(WARN, "try to commit tx fail, tx will be aborted",
                K(ret), K(expire_ts), K(tx), KP(&cb));
      // the error may caused by txn has terminated
      handle_tx_commit_result_(tx, ret, commit_version);
    }
    // if txn not terminated, it can be choice to abort
    if (OB_FAIL(ret) && tx.state_ == ObTxDesc::State::IN_TERMINATE) {
      tx.state_ = state0;
      abort_tx_(tx, ret);
      handle_tx_commit_result_(tx, OB_TRANS_ROLLBACKED);
      ret = OB_TRANS_ROLLBACKED;
    }
  }

  /* NOTE:
   * to prevent potential deadlock, distinguish the commit
   * completed by current thread from other cases
   */
  bool committed = tx.state_ == ObTxDesc::State::COMMITTED;
  // if tx committed, we should callback immediately
  //
  // NOTE: this must defer to final current function
  // in order to assure there is no access to tx, because
  // after calling the commit_cb, the tx object may be
  // released or reused
  DEFER({
      tx.lock_.unlock();
      if (OB_SUCC(ret) && committed) {
        direct_execute_commit_cb_(tx);
      }
    });
#ifndef NDEBUG
  TRANS_LOG(INFO, "submit commit tx", K(ret),
            K(committed), KPC(this), K(tx), K(expire_ts), KP(&cb));
#else
  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "submit commit tx fail", K(ret),
              K(committed), KPC(this), K(tx), K(expire_ts), KP(&cb));
  }
#endif
  ObTransTraceLog &tlog = tx.get_tlog();
  const char *trace_info_str = (trace_info == NULL ? NULL : trace_info->ptr());
  REC_TRANS_TRACE_EXT(&tlog, submit_commit_tx, OB_Y(ret), OB_Y(expire_ts),
                      OB_ID(tag1), committed,
                      OB_ID(tag2), trace_info_str,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

// when callback exec directly, mock the general pattern
// acquire ref -> exec callback -> release ref
void ObTransService::direct_execute_commit_cb_(ObTxDesc &tx)
{
  tx.inc_ref(1);
  tx.execute_commit_cb();
  tx_desc_mgr_.revert(tx);
}

int ObTransService::get_read_snapshot(ObTxDesc &tx,
                                      const ObTxIsolationLevel iso_level,
                                      const int64_t expire_ts,
                                      ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  ObTxIsolationLevel isolation = iso_level;
  if (OB_SUCC(tx_sanity_check_(tx))) {
    if (tx.is_in_tx() && isolation != tx.isolation_) {
      //use txn's isolation if txn is active
      isolation = tx.isolation_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (isolation == ObTxIsolationLevel::SERIAL ||
             isolation == ObTxIsolationLevel::RR) {
      // only acquire snapshot once in these isolation level
    if (tx.isolation_ != isolation /*change isolation*/ ||
        !tx.snapshot_version_.is_valid()/*version invalid*/) {
      SCN version;
      int64_t uncertain_bound = 0;
      if (OB_FAIL(sync_acquire_global_snapshot_(tx, expire_ts, version, uncertain_bound))) {
        TRANS_LOG(WARN, "acquire global snapshot fail", K(ret), K(tx));
      } else if (!tx.tx_id_.is_valid() && OB_FAIL(tx_desc_mgr_.add(tx))) {
        TRANS_LOG(WARN, "add tx to mgr fail", K(ret), K(tx));
      } else {
        tx.snapshot_version_ = version;
        tx.snapshot_uncertain_bound_ = uncertain_bound;
        tx.snapshot_scn_ = tx.get_tx_seq(ObSequence::get_max_seq_no() + 1);
        tx.state_change_flags_.EXTRA_CHANGED_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      tx.isolation_ = isolation;
      snapshot.core_.version_ = tx.snapshot_version_;
      snapshot.uncertain_bound_ = tx.snapshot_uncertain_bound_;
    }
  } else if (OB_FAIL(sync_acquire_global_snapshot_(tx,
                                                   expire_ts,
                                                   snapshot.core_.version_,
                                                   snapshot.uncertain_bound_))) {
    TRANS_LOG(WARN, "acquire global snapshot fail", K(ret), K(tx));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    snapshot.source_ = ObTxReadSnapshot::SRC::GLOBAL;
    snapshot.parts_.reset();
    // If tx id is valid , record tx_id and scn
    if (tx.tx_id_.is_valid()) {
      snapshot.core_.tx_id_ = tx.tx_id_;
      snapshot.core_.scn_ = tx.get_tx_seq();
    }
    if (tx.state_ != ObTxDesc::State::IDLE) {
      ARRAY_FOREACH(tx.parts_, i) {
        if (!tx.parts_[i].is_clean() &&
            OB_FAIL(snapshot.parts_.push_back(ObTxLSEpochPair(tx.parts_[i].id_, tx.parts_[i].epoch_)))) {
          TRANS_LOG(WARN, "push snapshot parts fail", K(ret), K(tx), K(snapshot));
        }
      }
    }
    snapshot.valid_ = true;
  }
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, get_read_snapshot, OB_Y(ret), OB_Y(expire_ts),
                      OB_ID(txid), tx.tx_id_,
                      OB_ID(isolation_level), (int)isolation,
                      OB_ID(snapshot_source), (int)snapshot.source_,
                      OB_ID(snapshot_version), snapshot.core_.version_,
                      OB_ID(snapshot_txid), snapshot.core_.tx_id_.get_id(),
                      OB_ID(snapshot_scn), snapshot.core_.scn_.cast_to_int(),
                      OB_ID(trace_id), ObCurTraceId::get_trace_id_str(),
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::get_ls_read_snapshot(ObTxDesc &tx,
                                         const ObTxIsolationLevel iso_level,
                                         const share::ObLSID &lsid,
                                         const int64_t expire_ts,
                                         ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  bool acquire_from_follower  = false;
  bool fallback_get_global_snapshot = false;
  // if txn is active use txn's isolation instead
  ObTxIsolationLevel isolation = tx.is_in_tx() ? tx.isolation_ : iso_level;
  if (isolation == ObTxIsolationLevel::SERIAL ||
      isolation == ObTxIsolationLevel::RR) {
    ret = get_read_snapshot(tx, isolation, expire_ts, snapshot);
  } else if (!lsid.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid lsid", K(ret), K(lsid));
  } else {
    ObSpinLockGuard guard(tx.lock_);
    if (OB_FAIL(tx_sanity_check_(tx))) {
  } else if (OB_SUCC(acquire_local_snapshot_(lsid,
                                             snapshot.core_.version_,
                                             true /*is_read_only*/,
                                             acquire_from_follower))) {
      snapshot.source_ = ObTxReadSnapshot::SRC::LS;
      snapshot.snapshot_lsid_ = lsid;
      snapshot.uncertain_bound_ = 0;
      snapshot.parts_.reset();
      if(acquire_from_follower) {
        snapshot.snapshot_ls_role_ = common::ObRole::FOLLOWER;
      }
      // If tx id is valid , record tx_id and scn
      if (tx.tx_id_.is_valid()) {
        snapshot.core_.tx_id_ = tx.tx_id_;
        snapshot.core_.scn_ = tx.get_tx_seq();
      }
      if (tx.state_ != ObTxDesc::State::IDLE) {
        ARRAY_FOREACH(tx.parts_, i) {
          if (tx.parts_[i].id_ == lsid && !tx.parts_[i].is_clean()) {
            if (OB_FAIL(snapshot.parts_.push_back(ObTxLSEpochPair(lsid, tx.parts_[i].epoch_)))) {
              TRANS_LOG(WARN, "push lsid to snapshot.parts fail", K(ret), K(lsid), K(tx));
            }
          }
        }
      }
      snapshot.valid_ = true;
    } else {
      // XXX In standby cluster mode, the failure to call acquire_local_snapshot_ is an
      // normal situation, no error log needs to be printed
      // TRANS_LOG(WARN, "acquire local snapshot fail, fallback gts", K(ret), K(lsid));
      fallback_get_global_snapshot = true;
    }
  }
  if (fallback_get_global_snapshot) {
    ret = get_read_snapshot(tx, isolation, expire_ts, snapshot);
  }
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, get_ls_read_snapshot, OB_Y(ret), OB_Y(expire_ts),
                      OB_ID(ls_id), lsid.id(),
                      OB_ID(isolation_level), (int)isolation,
                      OB_ID(snapshot_source), (int)snapshot.source_,
                      OB_ID(snapshot_version), snapshot.core_.version_,
                      OB_ID(snapshot_txid), snapshot.core_.tx_id_.get_id(),
                      OB_ID(snapshot_scn), snapshot.core_.scn_.cast_to_int(),
                      OB_ID(trace_id), ObCurTraceId::get_trace_id_str(),
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::get_read_snapshot_version(const int64_t expire_ts,
                                              SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t uncertain_bound = 0;

  ret = acquire_global_snapshot__(expire_ts,
                                  0,
                                  snapshot_version,
                                  uncertain_bound,
                                  []() -> bool { return false; });
  return ret;
}

int ObTransService::get_ls_read_snapshot_version(const share::ObLSID &local_ls_id,
                                                 SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  bool acquire_from_follower = false;
  ret = acquire_local_snapshot_(local_ls_id,
                                snapshot_version,
                                true /*is_read_only*/,
                                acquire_from_follower);
  UNUSED(acquire_from_follower);
  return ret;
}

int ObTransService::get_weak_read_snapshot_version(const int64_t max_read_stale_us_for_user,
                                                   const bool local_single_ls,
                                                   SCN &snapshot)
{
  int ret = OB_SUCCESS;
  bool monotinic_read = true;;
  SCN wrs_scn;

    // server weak read version
  if (!ObWeakReadUtil::enable_monotonic_weak_read(tenant_id_)) {
    if (local_single_ls) {
      if (OB_FAIL(GCTX.weak_read_service_->get_server_version(tenant_id_, wrs_scn))) {
        TRANS_LOG(WARN, "get server read snapshot fail", K(ret), KPC(this));
      }
      monotinic_read = false;
    } else {
      if (OB_FAIL(GCTX.weak_read_service_->get_cluster_version(tenant_id_, wrs_scn))) {
        TRANS_LOG(WARN, "get weak read snapshot fail", K(ret), KPC(this));
      }
    }
    // wrs cluster version
  } else if (OB_FAIL(GCTX.weak_read_service_->get_cluster_version(tenant_id_, wrs_scn))) {
    TRANS_LOG(WARN, "get weak read snapshot fail", K(ret), KPC(this));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (monotinic_read
        || max_read_stale_us_for_user < 0
        || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      // no need to check barrier version
      snapshot = wrs_scn;
    } else {
      // check snapshot version barrier which is setted by user system variable
      SCN gts_cache;
      SCN current_scn;
      if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id_, NULL, gts_cache))) {
        TRANS_LOG(WARN, "get ts sync error", K(ret), K(max_read_stale_us_for_user));
      } else {
        const int64_t current_time_us = MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()
                ? std::max(ObTimeUtility::current_time(), gts_cache.convert_to_ts())
                : gts_cache.convert_to_ts();
        current_scn.convert_from_ts(current_time_us - max_read_stale_us_for_user);
        snapshot = SCN::max(wrs_scn, current_scn);
      }
    }
  }
  TRANS_LOG(TRACE, "get weak-read snapshot", K(ret), K(snapshot), K(monotinic_read));
  return ret;
}

int ObTransService::release_snapshot(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  SCN snapshot;
  ObSpinLockGuard guard(tx.lock_);
  tx.inc_op_sn();
  if (tx.state_ != ObTxDesc::State::IDLE) {
    ret = OB_NOT_SUPPORTED;
  } else if (ObTxIsolationLevel::SERIAL == tx.isolation_ ||
             ObTxIsolationLevel::RR == tx.isolation_) {
    snapshot = tx.snapshot_version_;
    if (snapshot.is_valid()) {
      tx.snapshot_version_.reset();
      tx.snapshot_uncertain_bound_ = 0;
    }
  }
  TRANS_LOG(TRACE, "release snapshot", K(tx), K(snapshot));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, release_snapshot, OB_Y(ret), OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::register_tx_snapshot_verify(ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = snapshot.core_.tx_id_;
  if (tx_id.is_valid()) {
    ObTxDesc *tx = NULL;
    if (OB_SUCC(tx_desc_mgr_.get(tx_id, tx))) {
      ObTxSavePoint sp;
      sp.init(&snapshot);
      ObSpinLockGuard guard(tx->lock_);
      if (OB_FAIL(tx_sanity_check_(*tx))) {
      } else if (!tx->is_in_tx()) {
        // skip register if txn not active
      } else if (OB_FAIL(tx->savepoints_.push_back(sp))) {
        TRANS_LOG(WARN, "push back snapshot fail", K(ret),
                  K(snapshot), KPC(tx));
      }
      ObTransTraceLog &tlog = tx->get_tlog();
      REC_TRANS_TRACE_EXT(&tlog, register_snapshot, OB_Y(ret),
                          OB_ID(arg), (void*)&snapshot,
                          OB_ID(snapshot_version), snapshot.core_.version_,
                          OB_ID(snapshot_scn), snapshot.core_.scn_.cast_to_int(),
                          OB_ID(ref), tx->get_ref(),
                          OB_ID(thread_id), GETTID());
    } else if (ret != OB_ENTRY_NOT_EXIST) {
      TRANS_LOG(WARN, "get tx fail", K(tx_id), K(snapshot));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_NOT_NULL(tx)) {
      tx_desc_mgr_.revert(*tx);
    }
  }
  TRANS_LOG(TRACE, "register snapshot", K(ret), K(snapshot));
  return ret;
}

void ObTransService::unregister_tx_snapshot_verify(ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = snapshot.core_.tx_id_;
  if (tx_id.is_valid()) {
    ObTxDesc *tx = NULL;
    if (OB_SUCC(tx_desc_mgr_.get(tx_id, tx))) {
      ObSpinLockGuard guard(tx->lock_);
      ARRAY_FOREACH_N(tx->savepoints_, i, cnt) {
        ObTxSavePoint &it = tx->savepoints_[cnt - 1 - i];
        if (it.is_snapshot() && it.snapshot_ == &snapshot) {
          it.release();
          break;
        }
      }
      ObTransTraceLog &tlog = tx->get_tlog();
      REC_TRANS_TRACE_EXT(&tlog, unregister_snapshot, OB_Y(ret),
                          OB_ID(arg), (void*)&snapshot,
                          OB_ID(snapshot_version), snapshot.core_.version_,
                          OB_ID(snapshot_scn), snapshot.core_.scn_.cast_to_int(),
                          OB_ID(ref), tx->get_ref(),
                          OB_ID(thread_id), GETTID());
    }
    if (OB_NOT_NULL(tx)) {
      tx_desc_mgr_.revert(*tx);
    }
  }
  TRANS_LOG(TRACE, "unreigster snapshot", K(ret), K(snapshot));
}

int ObTransService::create_implicit_savepoint(ObTxDesc &tx,
                                              const ObTxParam &tx_param,
                                              ObTxSEQ &savepoint,
                                              const bool release)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  if (!tx_param.is_valid()) {
    // NOTE: tx_param only required for create global implicit_savepoint when txn in IDLE state
    // TODO: rework this interface, allow skip pass tx_param if not required
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "tx param invalid", K(ret), K(tx_param), K(tx));
  } else if (OB_FAIL(tx_sanity_check_(tx))) {
  } else if (tx.state_ >= ObTxDesc::State::IN_TERMINATE) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "create implicit savepoint but tx terminated", K(ret), K(tx));
  } else if (tx.flags_.SHADOW_ && tx.get_tx_id().is_valid()) {
    ret = create_local_implicit_savepoint_(tx, savepoint);
  } else {
    ret = create_global_implicit_savepoint_(tx, tx_param, savepoint, release);
  }
  return ret;
}

int ObTransService::create_local_implicit_savepoint_(ObTxDesc &tx,
                                                     ObTxSEQ &savepoint)
{
  int ret = OB_SUCCESS;
  savepoint = tx.inc_and_get_tx_seq(0);
  TRANS_LOG(TRACE, "create local implicit savepoint", K(ret), K(savepoint), K(tx));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, create_local_implicit_savepoint,
                      OB_Y(ret), OB_ID(savepoint), savepoint.cast_to_int(), OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::create_global_implicit_savepoint_(ObTxDesc &tx,
                                                      const ObTxParam &tx_param,
                                                      ObTxSEQ &savepoint,
                                                      const bool release)
{
  int ret = OB_SUCCESS;
  if (tx.state_ == ObTxDesc::State::IDLE) {
    tx.cluster_version_ = GET_MIN_CLUSTER_VERSION();
    tx.cluster_id_      = tx_param.cluster_id_;
    tx.access_mode_     = tx_param.access_mode_;
    tx.timeout_us_      = tx_param.timeout_us_;
    if (tx.isolation_ != tx_param.isolation_) {
      tx.isolation_ = tx_param.isolation_;
      tx.snapshot_version_.reset(); // invalidate previouse snapshot
    }
  }
  // NOTE: the lock_timeout_us_ can be changed even tx active
  tx.lock_timeout_us_ = tx_param.lock_timeout_us_;

  tx.inc_op_sn();
  savepoint = tx.inc_and_get_tx_seq(0);
  if (tx.state_ == ObTxDesc::State::IDLE && !tx.tx_id_.is_valid()) {
    if (tx.has_implicit_savepoint()) {
      ret = OB_TRANS_INVALID_STATE;
      TRANS_LOG(WARN, "has implicit savepoint, but tx_id is invalid", K(ret), K(tx));
    } else if (OB_FAIL(tx_desc_mgr_.add(tx))) {
      TRANS_LOG(WARN, "failed to register with txMgr", K(ret), K(tx));
    }
  }
  if (OB_SUCC(ret)) {
    if (release) {
      tx.release_all_implicit_savepoint();
    }
    tx.add_implicit_savepoint(savepoint);
  }
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, create_global_implicit_savepoint, OB_Y(ret),
                      OB_ID(txid), tx.tx_id_,
                      OB_ID(savepoint), savepoint.cast_to_int(), OB_Y(release),
                      OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  TRANS_LOG(TRACE, "create global implicit savepoint", K(ret), K(tx), K(tx_param), K(savepoint));
  return ret;
}

// impl note
// if tx aborted reject with need_rollback
// if tx terminated reject with COMMITTED / ABORTED
// if tx in-commtting reject with has_decided
// if tx in idle:
//    abort tx and reset tx [1]
// if tx in active:
//    normal rollback [2]
// if tx in implicit_active:
//    if tx.active_scn > savepoint:
//       abort tx and reset tx [1]
//    else
//       normal rollback [2]
// [1] abort tx and reset tx
//     re-register with new tx-id
//     state = IDLE
// [2] normal rollback:
//     if rollback failed: abort tx
int ObTransService::rollback_to_implicit_savepoint(ObTxDesc &tx,
                                                   const ObTxSEQ savepoint,
                                                   const int64_t expire_ts,
                                                   const share::ObLSArray *extra_touched_ls)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  if (OB_FAIL(tx_sanity_check_(tx))) {
  } else if (tx.flags_.SHADOW_) {
    if (OB_NOT_NULL(extra_touched_ls)) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "rollback on remote only suport collected tx parts",
                K(ret), K(savepoint), K(tx));
    } else {
      ret = rollback_to_local_implicit_savepoint_(tx, savepoint, expire_ts);
    }
  } else {
    ret = rollback_to_global_implicit_savepoint_(tx,
                                                 savepoint,
                                                 expire_ts,
                                                 extra_touched_ls);
  }
  return ret;
}

int ObTransService::rollback_to_local_implicit_savepoint_(ObTxDesc &tx,
                                                          const ObTxSEQ savepoint,
                                                          const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  ObTxPartRefList parts;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(find_parts_after_sp_(tx, parts, savepoint))) {
    TRANS_LOG(WARN, "find rollback parts fail", K(ret), K(savepoint), K(tx));
  } else {
    ARRAY_FOREACH(parts, i) {
      ObPartTransCtx *ctx = NULL;
      ObTxPart &p = parts[i];
      if (OB_FAIL(get_tx_ctx_(p.id_, tx.tx_id_, ctx))) {
        TRANS_LOG(WARN, "get tx ctx fail", K(ret), K_(p.id), K(tx));
      } else if (p.epoch_ != ctx->epoch_) {
        ret = OB_TRANS_CTX_NOT_EXIST; // FIXME more decent errno
      } else if (OB_FAIL(ls_sync_rollback_savepoint__(ctx, savepoint, tx.op_sn_, expire_ts))) {
        TRANS_LOG(WARN, "LS rollback savepoint fail", K(ret), K(savepoint), K(tx));
      } else {
        p.last_scn_ = savepoint;
      }
      if (OB_NOT_NULL(ctx)) {
        revert_tx_ctx_(ctx);
      }
    }
  }
  int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
#ifndef NDEBUG
  TRANS_LOG(INFO, "rollback local implicit savepoint", K(ret), K(savepoint));
#else
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "rollback local implicit savepoint fail", K(ret), K(savepoint));
  }
#endif
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, rollback_local_implicit_savepoint,
                      OB_Y(ret), OB_ID(savepoint), savepoint.cast_to_int(), OB_Y(expire_ts),
                      OB_ID(time_used) , elapsed_us,
                      OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::rollback_to_global_implicit_savepoint_(ObTxDesc &tx,
                                                           const ObTxSEQ savepoint,
                                                           const int64_t expire_ts,
                                                           const share::ObLSArray *extra_touched_ls)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  tx.inc_op_sn();
  bool reset_tx = false, normal_rollback = false;
  // merge extra touched ls
  if (OB_NOT_NULL(extra_touched_ls) && !extra_touched_ls->empty()) {
    if (OB_FAIL(tx.update_parts(*extra_touched_ls))) {
      TRANS_LOG(WARN, "add tx part with extra_touched_ls fail", K(ret), K(tx), KPC(extra_touched_ls));
      abort_tx_(tx, ret);
    } else {
      TRANS_LOG(INFO, "add tx part with extra_touched_ls", KPC(extra_touched_ls), K_(tx.tx_id));
    }
  }

  if (OB_SUCC(ret)) {
    switch(tx.state_) {
    case ObTxDesc::State::IDLE:
      tx.release_implicit_savepoint(savepoint);
      ret = OB_SUCCESS;
      break;
    case ObTxDesc::State::ACTIVE:
      tx.release_implicit_savepoint(savepoint);
      normal_rollback = true;
      break;
    case ObTxDesc::State::IMPLICIT_ACTIVE:
      tx.release_implicit_savepoint(savepoint);
      if (!tx.flags_.REPLICA_             // on tx start node
          && !tx.has_implicit_savepoint() // to first savepoint
          && tx.active_scn_ >= savepoint  // rollback all dirty state
          && !tx.has_extra_state_()) {    // hasn't explicit savepoint or serializable snapshot
        reset_tx = true;
        /*
         * Avoid lock conflicts between first stmt retry and tx async abort(end first stmt caused)
         * Add a sync rollback process before async abort tx.
         */
        normal_rollback = true;
      } else {
        normal_rollback = true;
      }
      break;
    default:
      ret = OB_TRANS_INVALID_STATE; // FIXME, better error code
    }
  }

  if (normal_rollback) {
    ObTxPartRefList parts;
    if (OB_FAIL(ret)) {
    } else if (tx.flags_.PART_EPOCH_MISMATCH_) {
      ret = OB_TRANS_NEED_ROLLBACK;
      TRANS_LOG(WARN, "some participant born epoch mismatch, txn will rollback internally", K(ret));
    } else if (tx.flags_.PARTS_INCOMPLETE_) {
      ret = OB_TRANS_NEED_ROLLBACK;
      TRANS_LOG(WARN, "txn participants set incomplete, txn will rollback internally", K(ret));
    } else if (OB_FAIL(find_parts_after_sp_(tx, parts, savepoint))) {
      TRANS_LOG(WARN, "find rollback parts fail", K(ret), K(tx), K(savepoint));
    } else if (OB_FAIL(rollback_savepoint_(tx,
                                           parts,
                                           savepoint,
                                           expire_ts))) {
      TRANS_LOG(WARN, "do savepoint rollback fail", K(ret));
    }
    // reset tx ignore rollback ret
    if (reset_tx) {
    } else if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "rollback savepoint fail, abort tx",
                K(ret), K(savepoint), KP(extra_touched_ls), K(parts), K(tx));
      // advance op_sequence to reject further rollback resp messsages
      tx.inc_op_sn();
      abort_tx_(tx, ObTxAbortCause::SAVEPOINT_ROLLBACK_FAIL);
    } else {
      /*
       * advance txn op_seqence to barrier duplicate rollback msg
       * otherwise, rollback may erase following write
       */
      tx.inc_op_sn();
    }
  }
  /*
   * reset tx state from IMPLICIT_ACTIVE to IDLE
   * in progress tx was cleaned up via abort
   * but resources hold before beginning of tx
   * were reserved
   */
  if (reset_tx) {
    if (OB_FAIL(abort_tx_(tx, ObTxAbortCause::IMPLICIT_ROLLBACK,
              false /*don't cleanup resource*/))) {
      TRANS_LOG(WARN, "internal abort tx fail", K(ret), K(tx));
    } else if (OB_FAIL(start_epoch_(tx))) {
      TRANS_LOG(WARN, "switch tx to idle fail", K(ret), K(tx));
    }
  }
  int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
#ifndef NDEBUG
  TRANS_LOG(INFO, "rollback to implicit savepoint", K(ret), K(savepoint), K(elapsed_us), K(tx));
#else
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "rollback to implicit savepoint fail", K(ret), K(savepoint), K(elapsed_us), K(tx));
  }
#endif
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, rollback_global_implicit_savepoint,
                      OB_Y(ret), OB_ID(savepoint), savepoint.cast_to_int(), OB_Y(expire_ts),
                      OB_ID(time_used), elapsed_us,
                      OB_ID(arg), (void*)extra_touched_ls,
                      OB_ID(tag1), reset_tx,
                      OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

int ObTransService::ls_sync_rollback_savepoint__(ObPartTransCtx *part_ctx,
                                                 const ObTxSEQ savepoint,
                                                 const int64_t op_sn,
                                                 const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  bool blockable = expire_ts > 0;
  const ObTxSEQ from_scn = savepoint.clone_with_seq(ObSequence::inc_and_get_max_seq_no());
  do {
    ret = part_ctx->rollback_to_savepoint(op_sn, from_scn, savepoint);
    if (OB_NEED_RETRY == ret && blockable) {
      if (ObTimeUtility::current_time() >= expire_ts) {
        ret = OB_TIMEOUT;
        TRANS_LOG(WARN, "can not retry rollback_to because of timeout", K(ret), K(retry_cnt));
      } else {
        if (retry_cnt % 5 == 0) {
          TRANS_LOG(WARN, "retry rollback_to savepoint in ctx", K(ret), K(retry_cnt));
        }
        retry_cnt += 1;
        ob_usleep(50 * 1000);
      }
    }
  } while (OB_NEED_RETRY == ret && blockable);
#ifndef NDEBUG
  TRANS_LOG(INFO, "rollback to savepoint sync", K(ret),
            K(part_ctx->get_trans_id()), K(part_ctx->get_ls_id()), K(retry_cnt),
            K(op_sn), K(savepoint), K(expire_ts));
#else
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "rollback to savepoint sync fail", K(ret),
              K(part_ctx->get_trans_id()), K(part_ctx->get_ls_id()), K(retry_cnt),
              K(op_sn), K(savepoint), K(expire_ts));
  }
#endif
  return ret;
}

int ObTransService::create_explicit_savepoint(ObTxDesc &tx,
                                              const ObString &savepoint,
                                              const uint32_t session_id,
                                              const bool user_create)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  tx.inc_op_sn();
  const ObTxSEQ scn = tx.inc_and_get_tx_seq(0);
  ObTxSavePoint sp;
  if (OB_SUCC(sp.init(scn, savepoint, session_id, user_create))) {
    if (OB_FAIL(tx.savepoints_.push_back(sp))) {
      TRANS_LOG(WARN, "push savepoint failed", K(ret));
    } else if (!tx.tx_id_.is_valid() && OB_FAIL(tx_desc_mgr_.add(tx))) {
      TRANS_LOG(WARN, "add tx to mgr failed", K(ret), K(tx));
      tx.savepoints_.pop_back();
    } else {
      // impl move semantic of savepoint
      ARRAY_FOREACH_X(tx.savepoints_, i, cnt, i != cnt - 1) {
        ObTxSavePoint &it = tx.savepoints_.at(cnt - 2 - i);
        if (it.is_stash()) { break; }
        if (it.is_savepoint() && it.name_ == savepoint && it.session_id_ == session_id) {
          TRANS_LOG(TRACE, "move savepoint", K(savepoint), "from", it.scn_, "to", scn, K(tx));
          it.release();
          break; // assume only one if exist
        }
      }
    }
  }
  tx.state_change_flags_.EXTRA_CHANGED_ = true;
  TRANS_LOG(TRACE, "normal savepoint", K(ret), K(savepoint), K(scn), K(tx));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, create_explicit_savepoint, OB_Y(ret),
                      OB_ID(savepoint), savepoint,
                      OB_ID(seq_no), scn.cast_to_int(),
                      OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

// impl note
// 1. find savepoint Node
// 3. do rollback savepoint by savepoint No.
// 2. invalidate savepoint and snapshot after the savepoint Node.
int ObTransService::rollback_to_explicit_savepoint(ObTxDesc &tx,
                                                   const ObString &savepoint,
                                                   const int64_t expire_ts,
                                                   const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObTxSEQ sp_scn;
  ObSpinLockGuard guard(tx.lock_);
  if (OB_SUCC(tx_sanity_check_(tx))) {
    tx.inc_op_sn();
    ARRAY_FOREACH_N(tx.savepoints_, i, cnt) {
      const ObTxSavePoint &it = tx.savepoints_.at(cnt - 1 - i);
      TRANS_LOG(TRACE, "sp iterate:", K(it));
      if (it.is_stash()) { break; }
      if (it.is_savepoint() && it.name_ == savepoint && it.session_id_ == session_id) {
        sp_scn = it.scn_;
        break;
      }
    }
    if (!sp_scn.is_valid()) {
      ret = OB_SAVEPOINT_NOT_EXIST;
      TRANS_LOG(WARN, "savepoint not exist", K(ret), K(savepoint), K_(tx.savepoints));
    }
  }
  if (OB_SUCC(ret)) {
    ObTxPartRefList parts;
    if (OB_FAIL(find_parts_after_sp_(tx, parts, sp_scn))) {
      TRANS_LOG(WARN, "find rollback parts fail", K(ret), K(tx), K(savepoint));
    } else if (OB_FAIL(rollback_savepoint_(tx,
                                           parts,
                                           sp_scn,
                                           expire_ts))) {
      TRANS_LOG(WARN, "do savepoint rollback fail", K(ret));
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "rollback savepoint fail, abort tx",
                K(ret), K(savepoint), K(sp_scn), K(parts), K(tx));
      abort_tx_(tx, ObTxAbortCause::SAVEPOINT_ROLLBACK_FAIL);
    }
  }
  if (OB_SUCC(ret)) {
    // rollback savepoints > sp (note, current savepoint with sp won't be released)
    ARRAY_FOREACH_N(tx.savepoints_, i, cnt) {
      ObTxSavePoint &it = tx.savepoints_.at(cnt - 1 - i);
      if (it.scn_ > sp_scn) {
        it.rollback();
      }
    }
  }
  tx.state_change_flags_.EXTRA_CHANGED_ = true;
  int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, rollback_explicit_savepoint, OB_Y(ret),
                      OB_ID(id), savepoint,
                      OB_ID(savepoint), sp_scn.cast_to_int(),
                      OB_ID(time_used), elapsed_us,
                      OB_ID(opid), tx.op_sn_,
                      OB_ID(ref), tx.get_ref(),
                      OB_ID(thread_id), GETTID());
  return ret;
}

// impl note
// registered snapshot keep valid
int ObTransService::release_explicit_savepoint(ObTxDesc &tx, const ObString &savepoint, const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  bool hit = false;
  ObTxSEQ sp_id;
  ObSpinLockGuard guard(tx.lock_);
  if (OB_SUCC(tx_sanity_check_(tx))) {
    tx.inc_op_sn();
    ARRAY_FOREACH_N(tx.savepoints_, i, cnt) {
      ObTxSavePoint &it = tx.savepoints_.at(cnt - 1 - i);
      if (it.is_savepoint() && it.name_ == savepoint && it.session_id_ == session_id) {
        hit = true;
        sp_id = it.scn_;
        break;
      }
      if (it.is_stash()) { break; }
    }
    if (!hit) {
      ret = OB_SAVEPOINT_NOT_EXIST;
      TRANS_LOG(WARN, "release savepoint fail", K(ret), K(savepoint), K(tx));
    } else {
      ARRAY_FOREACH_N(tx.savepoints_, i, cnt) {
        ObTxSavePoint &it = tx.savepoints_.at(cnt - 1 - i);
        if (it.is_savepoint() && it.scn_ >= sp_id) {
          it.release();
        }
      }
      TRANS_LOG(TRACE, "release savepoint", K(savepoint), K(sp_id), K(session_id), K(tx));
    }
  }
  tx.state_change_flags_.EXTRA_CHANGED_ = true;
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, release_explicit_savepoint, OB_Y(ret),
                      OB_ID(savepoint), savepoint,
                      OB_ID(seq_no), sp_id.cast_to_int(),
                      OB_ID(opid), tx.op_sn_);
  return ret;
}

int ObTransService::create_stash_savepoint(ObTxDesc &tx, const ObString &name)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(tx.lock_);
  tx.inc_op_sn();
  const ObTxSEQ seq_no = tx.inc_and_get_tx_seq(0);
  ObTxSavePoint sp;
  if (OB_SUCC(sp.init(seq_no, name, 0, false, true))) {
    if (OB_FAIL(tx.savepoints_.push_back(sp))) {
      TRANS_LOG(WARN, "push savepoint failed", K(ret));
    }
  }
  TRANS_LOG(TRACE, "create stash savepoint", K(ret), K(seq_no), K(name), K(tx));
  REC_TRANS_TRACE_EXT(&tx.tlog_, create_stash_savepoint, OB_Y(ret),
                      OB_ID(savepoint), name,
                      OB_ID(seq_no), seq_no.cast_to_int(),
                      OB_ID(opid), tx.op_sn_);
  return ret;
}

// impl note
// fastpath:
//  local call, if NOT_MASTER, fallback slowpath
// slowpath:
//  post msg,
//  abort tx if rollback failed
int ObTransService::rollback_savepoint_(ObTxDesc &tx,
                                        ObTxPartRefList &parts,
                                        const ObTxSEQ savepoint,
                                        int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  bool slowpath = true;
  expire_ts = std::min(expire_ts, tx.get_expire_ts());
  if (parts.count() == 0) {
    slowpath = false;
    TRANS_LOG(INFO, "empty rollback participant set", K(tx), K(savepoint));
  } else if (parts.count() == 1 && parts[0].addr_ == self_) {
    slowpath = false;
    ObTxPart &p = parts[0];
    int64_t born_epoch = 0;
    if (OB_FAIL(ls_rollback_to_savepoint_(tx.tx_id_,
                                          p.id_,
                                          p.epoch_,
                                          tx.op_sn_,
                                          savepoint,
                                          born_epoch,
                                          &tx,
                                          -1/*non-blocking*/))) {
      if (common_retryable_error_(ret)) {
        slowpath = true;
        TRANS_LOG(INFO, "fallback to msg driven rollback", K(ret), K(savepoint), K(p), K(tx));
      } else {
        TRANS_LOG(WARN, "rollback savepoint fail", K(ret), K(savepoint), K(p), K(tx));
      }
    } else {
      if (p.epoch_ <= 0) { tx.update_clean_part(p.id_, born_epoch, self_); }
      TRANS_LOG(TRACE, "succ to rollback on participant", K(p), K(tx), K(savepoint));
    }
  }
  if (slowpath &&
      OB_FAIL(rollback_savepoint_slowpath_(tx,
                                           parts,
                                           savepoint,
                                           expire_ts))) {
    TRANS_LOG(WARN, "rollback slowpath fail", K(ret),
              K(parts), K(savepoint), K(expire_ts), K(tx));
  }
  if (OB_TIMEOUT == ret && ObTimeUtility::current_time() >= tx.get_expire_ts()) {
     ret = OB_TRANS_TIMEOUT;
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH(parts, i) {
      parts[i].last_scn_ = savepoint;
    }
  }
  return ret;
}

/**
 * ls_rollback_to_savepoint - rollback savepoint on LogStream
 * @verify_epoch:             used verify tx_ctx's epoch, prevent
 *                            create-crash-recreate
 * @op_sn:                    the operator sequence inner transaction
 *                            used to keep operation order correctly
 * @ctx_born_epoch:           return the ctx's born epoch if created(born)
 * @tx:                       tnx descriptor used to create participant txn ctx
 * @expire_ts:                an expire_ts used if retry was required
 *                            -1 if non-blocking desired
 *
 * Impl Note:
 * if verify_epoch = -1, which means first time to touch the LS
 *    if the tx_ctx not exist, one will be created, and its epoch returned
 * otherwise, tx_ctx's epoch was verified with @verify_epoch
 */
int ObTransService::ls_rollback_to_savepoint_(const ObTransID &tx_id,
                                              const share::ObLSID &ls,
                                              const int64_t verify_epoch,
                                              const int64_t op_sn,
                                              const ObTxSEQ savepoint,
                                              int64_t &ctx_born_epoch,
                                              const ObTxDesc *tx,
                                              int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  ObPartTransCtx *ctx = NULL;
  if (OB_FAIL(get_tx_ctx_(ls, tx_id, ctx))) {
    if (OB_NOT_MASTER == ret) {
    } else if (OB_TRANS_CTX_NOT_EXIST == ret && verify_epoch <= 0) {
      int tx_state = ObTxData::RUNNING;
      share::SCN commit_version;
      if (OB_FAIL(get_tx_state_from_tx_table_(ls, tx_id, tx_state, commit_version))) {
        if (OB_TRANS_CTX_NOT_EXIST == ret) {
          if (OB_FAIL(create_tx_ctx_(ls, *tx, ctx))) {
            if ((OB_PARTITION_IS_BLOCKED == ret || OB_PARTITION_IS_STOPPED == ret) && is_ls_dropped_(ls)) {
              ctx_born_epoch = ObTxPart::EPOCH_DEAD;
              ret = OB_SUCCESS;
            } else {
              TRANS_LOG(WARN, "create tx ctx fail", K(ret), K(ls), KPC(tx));
            }
          }
        } else if (OB_REPLICA_NOT_READABLE == ret && is_ls_dropped_(ls)) {
          ctx_born_epoch = ObTxPart::EPOCH_DEAD;
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(WARN, "get tx state from tx table fail", K(ret), K(ls), K(tx_id));
        }
      } else {
        switch (tx_state) {
        case ObTxData::COMMIT:
          ret = OB_TRANS_COMMITED;
          break;
        case ObTxData::ABORT:
          ret = OB_TRANS_KILLED;
          break;
        case ObTxData::RUNNING:
        default:
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "tx in-progress but ctx miss", K(ret), K(tx_state), K(tx_id), K(ls));
        }
      }
    } else {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(tx_id), K(ls));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ctx)) {
    ctx_born_epoch = ctx->epoch_;
    if (verify_epoch > 0 && ctx->epoch_ != verify_epoch) {
      ret = OB_TRANS_CTX_NOT_EXIST;
      TRANS_LOG(WARN, "current ctx illegal, born epoch not match", K(ret), K(ls), K(tx_id),
                K(verify_epoch), KPC(ctx));
    } else if(OB_FAIL(ls_sync_rollback_savepoint__(ctx, savepoint, op_sn, expire_ts))){
      TRANS_LOG(WARN, "LS rollback to savepoint fail", K(ret), K(tx_id), K(ls), K(op_sn), K(savepoint), KPC(ctx));
    }
  }
  if (OB_NOT_NULL(ctx)) {
    revert_tx_ctx_(ctx);
  }
  return ret;
}

inline int ObTransService::rollback_savepoint_slowpath_(ObTxDesc &tx,
                                                        const ObTxPartRefList &parts,
                                                        const ObTxSEQ savepoint,
                                                        const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t max_retry_intval = GCONF._ob_trans_rpc_timeout;
  ObSEArray<ObTxLSEpochPair, 4> targets;
  if (OB_FAIL(targets.reserve(parts.count()))) {
    TRANS_LOG(WARN, "reserve space fail", K(ret), K(parts), K(tx));
  } else {
    ARRAY_FOREACH(parts, i) {
      targets.push_back(ObTxLSEpochPair(parts[i].id_, parts[i].epoch_));
    }
    tx.brpc_mask_set_.reset();
    if (OB_FAIL(tx.brpc_mask_set_.init(&targets))) {
      TRANS_LOG(WARN, "init rpc mask set fail", K(ret), K(tx));
    }
  }
  ObTxRollbackSPMsg msg;
  msg.cluster_version_ = tx.cluster_version_;
  msg.tenant_id_ = tx.tenant_id_;
  msg.sender_addr_ = self_;
  msg.sender_ = SCHEDULER_LS;
  msg.cluster_id_ = tx.cluster_id_;
  msg.tx_id_ = tx.tx_id_;
  msg.savepoint_ = savepoint;
  msg.op_sn_ = tx.op_sn_;
  msg.epoch_ = -1;
  msg.request_id_ = tx.op_sn_;
  // prepare msg.tx_ptr_ if required
  // TODO(yunxing.cyx) : in 4.1 rework here, won't serialize txDesc
  ObTxDesc *tmp_tx_desc = NULL;
  ARRAY_FOREACH_NORET(parts, i) {
    if (parts[i].epoch_ <= 0) {
      int64_t len = tx.get_serialize_size() + sizeof(ObTxDesc);
      char *buf = (char*)ob_malloc(len, "TxDesc");
      int64_t pos = sizeof(ObTxDesc);
      if (OB_FAIL(tx.serialize(buf, len, pos))) {
        TRANS_LOG(WARN, "serialize tx fail", KR(ret), K(tx));
        ob_free(buf);
      } else {
        tmp_tx_desc = new(buf)ObTxDesc();
        pos = sizeof(ObTxDesc);
        if (OB_FAIL(tmp_tx_desc->deserialize(buf, len, pos))) {
          TRANS_LOG(WARN, "deserialize tx fail", KR(ret));
        } else {
          tmp_tx_desc->parts_.reset();
          msg.tx_ptr_ = tmp_tx_desc;
        }
      }
      break;
    }
  }
  int64_t start_ts = ObTimeUtility::current_time();
  int retries = 0;
  if (OB_UNLIKELY(tx.flags_.INTERRUPTED_)) {
    ret = OB_ERR_INTERRUPTED;
    tx.clear_interrupt();
    TRANS_LOG(WARN, "interrupted", K(ret), K(tx));
  }
  if (OB_SUCC(ret)) {
    // setup state before release lock
    ObTxDesc::State save_state = tx.state_;
    tx.state_ = ObTxDesc::State::ROLLBACK_SAVEPOINT;
    tx.flags_.BLOCK_ = true;
    // release lock before blocking
    tx.lock_.unlock();
    ret = sync_rollback_savepoint__(tx, msg, tx.brpc_mask_set_,
                                    expire_ts, max_retry_intval, retries);
    tx.lock_.lock();
    // restore state
    if (OB_SUCC(ret) && tx.is_tx_active()) {
      tx.state_ = save_state;
    }
    // mask_set need clear
    tx.brpc_mask_set_.reset();
    // check interrupt
    if (OB_SUCC(ret) && tx.flags_.INTERRUPTED_) {
      ret = OB_ERR_INTERRUPTED;
      TRANS_LOG(WARN, "rollback savepoint was interrupted", K(ret));
    }
    // clear interrupt flag
    tx.clear_interrupt();
    tx.flags_.BLOCK_ = false;
  }
  if (OB_NOT_NULL(tmp_tx_desc)) {
    msg.tx_ptr_ = NULL;
    tmp_tx_desc->~ObTxDesc();
    ob_free(tmp_tx_desc);
  }
  int64_t elapsed_us = ObTimeUtility::current_time() - start_ts;
  TRANS_LOG(INFO, "rollback savepoint slowpath", K(ret),
            K_(tx.tx_id), K(start_ts), K(retries),
            K(savepoint), K(expire_ts), K(tx), K(parts.count()));
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, rollback_savepoint_slowpath, OB_Y(ret),
                      OB_ID(savepoint), savepoint.cast_to_int(), OB_Y(expire_ts),
                      OB_ID(retry_cnt), retries,
                      OB_ID(time_used), elapsed_us);
  return ret;
}

inline int ObTransService::sync_rollback_savepoint__(ObTxDesc &tx,
                                                     ObTxRollbackSPMsg &msg,
                                                     const ObTxDesc::MaskSet &mask_set,
                                                     int64_t expire_ts,
                                                     const int64_t max_retry_intval,
                                                     int &retries)
{
  const int64_t MIN_WAIT_TIME = 200 * 1000; // 200 ms
  int ret = OB_SUCCESS;
  int64_t start_ts = ObClockGenerator::getClock();
  retries = 0;
  int64_t min_retry_intval = 10 * 1000; // 10 ms
  expire_ts = std::max(ObTimeUtility::current_time() + MIN_WAIT_TIME, expire_ts);
  while (OB_SUCC(ret)) {
    int64_t retry_intval = std::min(min_retry_intval * (1 + retries), max_retry_intval);
    int64_t waittime = std::min(expire_ts - ObTimeUtility::current_time(), retry_intval);
    if (waittime <=0) {
      ret = OB_TIMEOUT;
      TRANS_LOG(WARN, "tx rpc wait result timeout", K(ret), K(expire_ts), K(retries));
    } else {
      ObSEArray<ObTxLSEpochPair, 4> remain;
      mask_set.get_not_mask(remain);
      int64_t remain_cnt = remain.count();
      TRANS_LOG(DEBUG, "unmasked parts", K(remain), K(tx), K(retries));
      // post msg to participants
      if (remain_cnt > 0) {
        tx.rpc_cond_.reset(); /* reset rpc_cond */
        int post_succ_num = 0;
        if (OB_FAIL(batch_post_rollback_savepoint_msg_(tx, msg, remain, post_succ_num))) {
          TRANS_LOG(WARN, "batch post tx msg fail", K(msg), K(remain), K(retries));
          if (is_location_service_renew_error(ret)) {
            // ignore ret
            ret = OB_SUCCESS;
          }
        } else { remain_cnt = post_succ_num; }
      }
      // if post failed, wait awhile and retry
      // if post succ, wait responses
      if (OB_SUCC(ret) && remain_cnt > 0) {
          // wait result
          int rpc_ret = OB_SUCCESS;
          if (OB_FAIL(tx.rpc_cond_.wait(waittime, rpc_ret))) {
            TRANS_LOG(WARN, "tx rpc condition wakeup", K(ret),
                      K(waittime), K(rpc_ret), K(expire_ts), K(remain), K(remain_cnt), K(retries),
                      K_(tx.state));
            // if trans is terminated, rollback savepoint should be terminated
            // NOTE that this case is only for xa trans
            // EXAMPLE, tx desc is shared by branch 1 and branch 2
            // 1. branch 1 starts to rollback savepoint
            // 2. branch 2 is terminated
            // 3. branch 1 receives callback of rollback savepoint
            if (tx.is_terminated()) {
              ret = OB_TRANS_HAS_DECIDED;
            } else {
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCCESS != rpc_ret) {
            TRANS_LOG(WARN, "tx rpc fail", K(rpc_ret), K_(tx.tx_id), K(waittime), K(remain), K(remain_cnt), K(retries));
            if (rpc_ret == OB_TRANS_CTX_NOT_EXIST) {
              // participant has quit, may be txn is timeout or other failure occured
              // txn need abort
              ret = tx.is_tx_timeout() ? OB_TRANS_TIMEOUT : OB_TRANS_KILLED;
            } else {
              ret = rpc_ret;
            }
          }
      }
      // check request complete
      if (OB_SUCC(ret) && mask_set.is_all_mask()) {
        TRANS_LOG(INFO, "all savepoint rollback succeed", K_(tx.tx_id),
                  K(remain_cnt), K(waittime), K(retries));
        break;
      }
      // interrupted, fail fastly
      if (tx.flags_.INTERRUPTED_) {
        ret = OB_ERR_INTERRUPTED;
        TRANS_LOG(WARN, "rollback was interrupted", K_(tx.tx_id),
                  K(remain_cnt), K(waittime), K(retries));
      }
    }
    ++retries;
  }
  return ret;
}

int ObTransService::merge_tx_state(ObTxDesc &to, const ObTxDesc &from)
{
  TRANS_LOG(TRACE, "merge_tx_state", K(to), K(from));
  int ret = to.merge_exec_info_with(from);
  ObTransTraceLog &tlog = to.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, merge_tx_state, OB_Y(ret),
                      OB_ID(to), (void*)&to,
                      OB_ID(from), (void*)&from,
                      OB_ID(opid), to.op_sn_,
                      OB_ID(thread_id), GETTID());
  return ret;
}
int ObTransService::get_tx_exec_result(ObTxDesc &tx, ObTxExecResult &exec_info)
{
  TRANS_LOG(TRACE, "get_tx_exec_result", K(tx), K(exec_info));
  int ret = tx.get_inc_exec_info(exec_info);
  return ret;
}
int ObTransService::add_tx_exec_result(ObTxDesc &tx, const ObTxExecResult &exec_info)
{
  TRANS_LOG(TRACE, "add_tx_exec_result", K(tx), K(exec_info));
  int ret = tx.add_exec_info(exec_info);
  ObTransTraceLog &tlog = tx.get_tlog();
  REC_TRANS_TRACE_EXT(&tlog, add_tx_exec_result, OB_ID(opid), tx.op_sn_,
                      OB_ID(num), exec_info.parts_.count(),
                      OB_ID(flag), exec_info.incomplete_,
                      OB_ID(thread_id), GETTID());
  return ret;
}

/*
 * tx_post_terminate - cleanup resource after tx terminated
 *
 * after tx committed/aborted/rollbacked
 * tx resource need to be released, we do it here
 */
void ObTransService::tx_post_terminate_(ObTxDesc &tx)
{
  // invalid registered snapshot
  if (tx.state_ == ObTxDesc::State::ABORTED ||
      tx.is_commit_unsucc()
      // FIXME: (yunxing.cyx)
      // bellow line is temproary, when storage layer support
      // record txn-id in SSTable's MvccRow, we can remove this
      // (the support was planed in v4.1)
      || tx.state_ == ObTxDesc::State::COMMITTED
      ) {
    invalid_registered_snapshot_(tx);
  } else if (tx.state_ == ObTxDesc::State::COMMITTED) {
    // cleanup snapshot's participant info, so that they will skip
    // verify participant txn ctx, which cause false negative,
    // because txn ctx has quit when txn committed.
    registered_snapshot_clear_part_(tx);
  }
  // statistic
  if (tx.is_tx_end()) {
    int64_t trans_used_time_us = 0;
    if (tx.active_ts_ > 0) { // skip txn has not active
      if (tx.finish_ts_ <= 0) {
        TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tx finish ts is unset", K(tx));
      } else if (tx.finish_ts_ > tx.active_ts_) {
        trans_used_time_us = tx.finish_ts_ - tx.active_ts_;
        TX_STAT_TIME_USED(trans_used_time_us);
      }
    }
    if (tx.is_committed()) {
      TX_STAT_COMMIT_INC;
      TX_STAT_COMMIT_TIME_USED(tx.finish_ts_ - tx.commit_ts_);
    }
    else if (tx.is_rollbacked()) {
      TX_STAT_ROLLBACK_INC;
      if (tx.commit_ts_ > 0) {
        TX_STAT_ROLLBACK_TIME_USED(MAX(0, tx.finish_ts_ - tx.commit_ts_));
      }
    }
    else {
      // TODO: COMMIT_UNKNOWN, COMMIT_TIMEOUT
    }
    switch(tx.parts_.count()) {
    case 0:  TX_STAT_READONLY_INC break;
    case 1:
      TX_STAT_LOCAL_INC;
      TX_STAT_LOCAL_TOTAL_TIME_USED(trans_used_time_us);
      break;
    default:
      TX_STAT_DIST_INC;
      TX_STAT_DIST_TOTAL_TIME_USED(trans_used_time_us);
    }
  }
  // release all savepoints
  tx.min_implicit_savepoint_.reset();
  tx.savepoints_.reset();
  // reset snapshot
  tx.snapshot_version_.reset();
  tx.snapshot_scn_.reset();
}

int ObTransService::start_epoch_(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  if (!tx.is_terminated()) {
    ret = OB_TRANS_INVALID_STATE;
    TRANS_LOG(WARN, "unexpected tx state to start new epoch", K(ret), K(tx));
  } else {
    tx.inc_op_sn();
    if (tx.flags_.RELEASED_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx released, cannot start new epoch", K(ret), K(tx));
    } else if (OB_FAIL(tx_desc_mgr_.remove(tx))) {
      TRANS_LOG(WARN, "unregister tx fail", K(ret), K(tx));
    } else if (OB_FAIL(tx.switch_to_idle())) {
      TRANS_LOG(WARN, "switch to idlefail", K(ret), K(tx));
    }
#ifndef NDEBUG
    TRANS_LOG(INFO, "tx start new epoch", K(ret), K(tx));
#endif
  }
  ObTransTraceLog &tlog = tx.get_tlog();
  int tlog_truncate_cnt = 0;
  if (OB_SUCC(ret) && tlog.count() > 50) {
    tlog_truncate_cnt = tlog.count() - 10;
    tlog.set_count(10);
  }
  REC_TRANS_TRACE_EXT(&tlog, start_epoch, OB_Y(ret), OB_ID(opid), tx.op_sn_, OB_ID(tag1), tlog_truncate_cnt);
  return ret;
}

int ObTransService::release_tx_ref(ObTxDesc &tx)
{
  return tx_desc_mgr_.release_tx_ref(&tx);
}

OB_INLINE int ObTransService::tx_sanity_check_(ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  if (tx.expire_ts_ <= ObClockGenerator::getClock()) {
    TX_STAT_TIMEOUT_INC
    ret = OB_TRANS_TIMEOUT;
  } else if (tx.flags_.BLOCK_) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "tx is blocked in other busy work", K(ret), K(tx));
  } else {
    switch(tx.state_) {
    case ObTxDesc::State::IDLE:
    case ObTxDesc::State::ACTIVE:
    case ObTxDesc::State::IMPLICIT_ACTIVE:
      if (tx.flags_.PART_ABORTED_) {
        TRANS_LOG(WARN, "some participant was aborted, abort tx now");
        abort_tx_(tx, tx.abort_cause_);
        // go through
      } else {
        break;
      }
    case ObTxDesc::State::ABORTED:
      ret = tx.abort_cause_ < 0 ? tx.abort_cause_ : OB_TRANS_NEED_ROLLBACK;
      break;
    case ObTxDesc::State::COMMITTED:
      ret = OB_TRANS_COMMITED;
      break;
    case ObTxDesc::State::ROLLED_BACK:
      ret = OB_TRANS_ROLLBACKED;
      break;
    case ObTxDesc::State::COMMIT_TIMEOUT:
    case ObTxDesc::State::COMMIT_UNKNOWN:
      ret = OB_TRANS_HAS_DECIDED;
      break;
    default:
      ret = OB_NOT_SUPPORTED; // FIXME: refine errno
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "tx state insanity", K(ret), K(tx));
    tx.print_trace_();
  }
  return ret;
}

int ObTransService::is_tx_active(const ObTransID &tx_id, bool &active)
{
  int ret = OB_SUCCESS;
  ObTxDesc *tx = NULL;
  if (OB_SUCC(tx_desc_mgr_.get(tx_id, tx))) {
    if (tx->state_ == ObTxDesc::State::IDLE ||
        tx->state_ == ObTxDesc::State::IMPLICIT_ACTIVE ||
        tx->state_ == ObTxDesc::State::ACTIVE) {
      active = true;
    } else {
      active = false;
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    active = false;
  }
  if (OB_NOT_NULL(tx)) {
    tx_desc_mgr_.revert(*tx);
  }
  return ret;
}
int ObTransService::sql_stmt_start_hook(const ObXATransID &xid,
                                        ObTxDesc &tx,
                                        const uint32_t session_id,
                                        const uint32_t real_session_id)
{
  int ret = OB_SUCCESS;
  if (tx.is_xa_trans()) {
    // xa txn execute stmt on non xa-start node
    // register tx before execute stmt
    bool registed = false;
    if (tx.xa_start_addr_ != self_ && tx.addr_ != self_) {
      if (OB_FAIL(tx_desc_mgr_.add_with_txid(tx.tx_id_, tx))) {
        TRANS_LOG(WARN, "register tx fail", K(ret), K_(tx.tx_id), K(xid), KP(&tx));
      } else { registed = true; }
    }
    if (OB_SUCC(ret) && OB_FAIL(MTL(ObXAService*)->start_stmt(xid, real_session_id, tx))) {
      TRANS_LOG(WARN, "xa trans start stmt failed", K(ret), K_(tx.xid), K(xid));
      ObGlobalTxType global_tx_type = tx.get_global_tx_type(xid);
      if (ObGlobalTxType::DBLINK_TRANS == global_tx_type && OB_TRANS_XA_BRANCH_FAIL == ret) {
        // if dblink trans, change errno (branch fail) to the errno of plain trans
        if (OB_FAIL(tx_sanity_check_(tx))) {
          TRANS_LOG(WARN, "tx state insanity", K(ret), K(global_tx_type), K(xid));
        } else {
          // if success, set ret to rollback
          ret = OB_TRANS_NEED_ROLLBACK;
          TRANS_LOG(WARN, "need rollback", K(ret), K(global_tx_type), K(xid));
        }
      }
    } else if (tx.is_xa_tightly_couple()) {
      // loosely couple mode txn-route use session_id to detect xa-start node's alive
      // so, can not overwrite session_id
      tx.set_sessid(session_id);
    }
    if (OB_FAIL(ret) && registed) {
      tx_desc_mgr_.remove(tx);
    }
    TRANS_LOG(INFO, "xa trans start stmt", K_(tx.xid), K(xid), K_(tx.sess_id), K(session_id));
  }
  return ret;
}

int ObTransService::sql_stmt_end_hook(const ObXATransID &xid, ObTxDesc &tx)
{
  int ret = OB_SUCCESS;
  if (tx.is_xa_trans()) {
    // need xid from session
    if (OB_FAIL(MTL(ObXAService*)->end_stmt(xid, tx))) {
      TRANS_LOG(WARN, "xa trans end stmt failed", K(ret), K_(tx.xid), K(xid));
      ObGlobalTxType global_tx_type = tx.get_global_tx_type(xid);
      if (ObGlobalTxType::DBLINK_TRANS == global_tx_type && OB_TRANS_XA_BRANCH_FAIL == ret) {
        // if dblink trans, change errno (branch fail) to the errno of plain trans
        if (OB_FAIL(tx_sanity_check_(tx))) {
          TRANS_LOG(WARN, "tx state insanity", K(ret), K(global_tx_type), K(xid));
        } else {
          // if success, set ret to rollback
          ret = OB_TRANS_NEED_ROLLBACK;
          TRANS_LOG(WARN, "need rollback", K(ret), K(global_tx_type), K(xid));
        }
      } else if (ObGlobalTxType::XA_TRANS == global_tx_type && OB_TRANS_XA_BRANCH_FAIL != ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = abort_tx(tx, ObTxAbortCause::END_STMT_FAIL))) {
          TRANS_LOG(WARN, "abort tx fail", K(ret), K(tmp_ret), K(global_tx_type), K(xid));
        }
        ret = OB_TRANS_NEED_ROLLBACK;
      }
    }
    // deregister from tx_desc_mgr to prevent
    // conflict with resumed xa branch execute
    if (tx.xa_start_addr_ != self_ && tx.addr_ != self_ ) {
      tx_desc_mgr_.remove(tx);
    }
    TRANS_LOG(INFO, "xa trans end stmt", K(ret), K_(tx.xid), K(xid));
  }
  return ret;
}
} // transaction
} // namespace
#undef TXN_API_SANITY_CHECK_FOR_TXN_FREE_ROUTE

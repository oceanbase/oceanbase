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
#include "storage/memtable/mvcc/ob_tx_callback_list.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "share/config/ob_server_config.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_stat.h"

namespace oceanbase
{
using namespace share;
namespace memtable
{

ObTxCallbackList::ObTxCallbackList(ObTransCallbackMgr &callback_mgr, const int16_t id)
  : id_(id),
    head_(),
    log_cursor_(&head_),
    parallel_start_pos_(NULL),
    length_(0),
    appended_(0),
    logged_(0),
    synced_(0),
    removed_(0),
    unlog_removed_(0),
    branch_removed_(0),
    data_size_(0),
    logged_data_size_(0),
    sync_scn_(SCN::min_scn()),
    batch_checksum_(),
    checksum_scn_(SCN::min_scn()),
    checksum_(0),
    tmp_checksum_(0),
    callback_mgr_(callback_mgr),
    append_latch_(),
    log_latch_(),
    iter_synced_latch_()
{
  reset();
}

ObTxCallbackList::~ObTxCallbackList() {}

void ObTxCallbackList::reset()
{
  if (length_ + removed_ != appended_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "BUG:list state insanity", KPC(this));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  if (length_ + removed_ != logged_ + unlog_removed_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "BUG:list state insanity", KPC(this));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  head_.set_prev(&head_);
  head_.set_next(&head_);
  log_cursor_ = &head_;
  checksum_ = 0;
  tmp_checksum_ = 0;
  checksum_scn_ = SCN::min_scn();
  batch_checksum_.reset();
  length_ = 0;
  appended_ = 0;
  logged_ = 0;
  synced_ = 0;
  removed_ = 0;
  unlog_removed_ = 0;
  branch_removed_ = 0;
  data_size_ = 0;
  logged_data_size_ = 0;
  sync_scn_ = SCN::min_scn();
}

// the semantic of the append_callback is atomic which means the cb is removed
// and no side effect is taken effects if some unexpected failure has happened.
int ObTxCallbackList::append_callback(ObITransCallback *callback,
                                      const bool for_replay,
                                      const bool parallel_replay,
                                      const bool serial_final)
{
  int ret = OB_SUCCESS;
  // It is important that we should put the before_append_cb and after_append_cb
  // into the latch guard otherwise the callback may already paxosed and released
  // before callback it.
  LockGuard gaurd(*this, LOCK_MODE::LOCK_APPEND);
  if (OB_ISNULL(callback)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "before_append_cb failed", K(ret), KPC(callback));
  } else if (OB_FAIL(callback->before_append_cb(for_replay))) {
    TRANS_LOG(WARN, "before_append_cb failed", K(ret), KPC(callback));
  } else {
    const bool repos_lc = !for_replay && (log_cursor_ == &head_);
    ObITransCallback *append_pos = NULL;
    if (!for_replay || parallel_replay || serial_final || !parallel_start_pos_) {
      append_pos = get_tail();
    } else {
      append_pos = parallel_start_pos_->get_prev();
    }
    // for replay, do sanity check: scn is incremental
    if (for_replay
        && append_pos != &head_  // the head with scn max
        && append_pos->get_scn() > callback->get_scn()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "replay callback scn out of order", K(ret), KPC(callback), KPC(this));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    } else {
      append_pos->append(callback);
      // start parallel replay, remember the position
      if (for_replay && parallel_replay && !serial_final && !parallel_start_pos_) {
        ATOMIC_STORE(&parallel_start_pos_, get_tail());
      }
      ++appended_;
      ATOMIC_INC(&length_);
      int64_t data_size = callback->get_data_size();
      data_size_ += data_size;
      if (repos_lc) {
        log_cursor_ = get_tail();
      }
      if (for_replay) {
        ++logged_;
        logged_data_size_ += data_size;
        ++synced_;
      }
      // Once callback is appended into callback lists, we can not handle the
      // error after it. So it should never report the error later. What's more,
      // after_append also should never return the error.
      (void)callback->after_append_cb(for_replay);
    }
  }
  return ret;
}

int64_t ObTxCallbackList::concat_callbacks(ObTxCallbackList &that)
{
  int64_t cnt = 0;

  if (that.empty()) {
    // do nothing
  } else {
    LockGuard this_guard(*this, LOCK_MODE::LOCK_ALL);
    LockGuard that_guard(that, LOCK_MODE::LOCK_ALL);
    ObITransCallback *that_head = that.head_.get_next();
    ObITransCallback *that_tail = that.get_tail();
    that_head->set_prev(get_tail());
    that_tail->set_next(&head_);
    get_tail()->set_next(that_head);
    head_.set_prev(that_tail);
    cnt = that.get_length();
    length_ += cnt;
    appended_ += cnt;
    if (log_cursor_ == &head_) {
      log_cursor_ = that_head;
    }
    { // fake callback removement to pass sanity check when reset
      that.length_ = 0;
      that.removed_ = cnt;
      that.unlog_removed_ = cnt;
      that.reset();
    }
  }

  return cnt;
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor, const LockState lock_state)
{
  return callback_(functor, get_guard(), get_guard(), lock_state);
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor,
                                const ObCallbackScope &callbacks,
                                const LockState lock_state)
{
  ObITransCallback *start = (ObITransCallback *)*(callbacks.start_);
  ObITransCallback *end = (ObITransCallback *)*(callbacks.end_);
  if (functor.is_reverse()) {
    return callback_(functor, start->get_next(), end->get_prev(), lock_state);
  } else {
    return callback_(functor, start->get_prev(), end->get_next(), lock_state);
  }
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor,
                                ObITransCallback *start /*exclusive*/,
                                ObITransCallback *end /*exclusive*/,
                                const LockState lock_state)
{
  int ret = OB_SUCCESS;
  int64_t traverse_count = 0;
  int64_t remove_count = 0;
  ObITransCallback *next = nullptr;
  bool iter_end = false;
  const bool is_reverse = functor.is_reverse();
  for (ObITransCallback *iter = (is_reverse ? start->get_prev() : start->get_next());
       OB_SUCC(ret) && !iter_end && iter != NULL && iter != end;
       iter = next) {
    functor.refresh();
    if (OB_UNLIKELY(iter->get_scn().is_min())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "callback with min_scn", K(ret), KPC(iter), KPC(this));
#ifdef ENABLE_DEBUG_LOG
      usleep(5000);
      ob_abort();
#endif
    } else if (functor.is_iter_end(iter)) {
      iter_end = true;
    } else {
      next = (is_reverse ? iter->get_prev() : iter->get_next());
      if (OB_FAIL(functor(iter))) {
        // don't print log, print it in functor
      } else if (functor.need_remove_callback()) {
        const share::SCN iter_scn = iter->get_scn();
        // sanity check before remove:
        // should not remove parallel replayed callback before serial replay finished
        if (parallel_start_pos_
            && !is_skip_checksum_()
            && !callback_mgr_.is_serial_final()
            && iter_scn >= parallel_start_pos_->get_scn()
            && iter_scn <= sync_scn_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "remove parallel callback while serial part not all replayed", K(ret), KPC(iter), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          usleep(5000);
          ob_abort();
#endif
        } else {
          if (removed_ && (removed_ % 10000 == 0)) {
            uint64_t checksum_now = batch_checksum_.calc();
            TRANS_LOG(INFO, "[CallbackList] remove-callback", K(checksum_now), KPC(this));
          }
          // the del operation must serialize with append operation
          // if it is operating on the list tail
          bool deleted = false;
          if (next == end) {
            if (!lock_state.APPEND_LOCKED_) {
              LockGuard guard(*this, LOCK_MODE::LOCK_APPEND);
              ret = iter->del();
              deleted = true;
            } else {
              ret = iter->del();
              deleted = true;
            }
          }
          if ((deleted && OB_FAIL(ret)) || (!deleted && OB_FAIL(iter->del()))) {
            TRANS_LOG(ERROR, "remove callback failed", K(ret), KPC(iter), K(deleted));
          } else {
            if (log_cursor_ == iter) {
              log_cursor_ = next;
            }
            if (parallel_start_pos_ == iter) {
              parallel_start_pos_ = (is_reverse || next == &head_) ? NULL : next;
            }
            ++removed_;
            if (iter->need_submit_log()) {
              ++unlog_removed_;
            }
            ++remove_count;
            if (iter->is_need_free()) {
              if (iter->is_table_lock_callback()) {
                callback_mgr_.get_ctx().free_table_lock_callback(iter);
              } else if (MutatorType::MUTATOR_ROW_EXT_INFO == iter->get_mutator_type()) {
                callback_mgr_.get_ctx().free_ext_info_callback(iter);
              } else {
                callback_mgr_.get_ctx().free_mvcc_row_callback(iter);
              }
            }
          }
        }
      }
    }
    if ((++traverse_count & 0xFFFFF) == 0) {
      TRANS_LOG(WARN, "memtable fifo callback too long",
                K(traverse_count), K(remove_count), K(functor));
    }
  }
  functor.set_statistics(traverse_count, remove_count);
  ATOMIC_AAF(&length_, -remove_count);
  return ret;
}

int64_t ObTxCallbackList::calc_need_remove_count_for_fast_commit_()
{
  // TODO: when support multiple callback-list, the remove count for single
  // list should be re-developed
  const int64_t fast_commit_callback_count = GCONF._fast_commit_callback_count;
  const int64_t recommand_reserve_count = (fast_commit_callback_count + 1) / 2;
  const int64_t need_remove_count = length_ - recommand_reserve_count;

  return need_remove_count;
}

int ObTxCallbackList::remove_callbacks_for_fast_commit(const share::SCN stop_scn)
{
  int ret = OB_SUCCESS;
  // if one thread doing the fast-commit, others give up
  LockGuard guard(*this, LOCK_MODE::TRY_LOCK_ITERATE);
  if (guard.is_locked()) {
    int64_t remove_cnt = calc_need_remove_count_for_fast_commit_();
    share::SCN right_bound;
    if (!stop_scn.is_valid()) {
      // unspecified stop_scn, used callback_list's sync_scn_
      right_bound = sync_scn_;
    } else if (!sync_scn_.is_min()) {
      // specified stop_scn, and callback_list's sync_scn_ is valid
      // use mininum
      right_bound = SCN::min(stop_scn, sync_scn_);
    } else {
      // callback_list's sync_scn is invalid, use stop_scn
      right_bound = stop_scn;
    }
    ObRemoveCallbacksForFastCommitFunctor functor(remove_cnt, right_bound);
    if (!is_skip_checksum_()) {
      functor.set_checksumer(checksum_scn_, &batch_checksum_);
    }
    if (OB_FAIL(callback_(functor, get_guard(), log_cursor_, guard.state_))) {
      TRANS_LOG(ERROR, "remove callbacks for fast commit wont report error", K(ret), K(functor));
    } else {
      callback_mgr_.add_fast_commit_callback_remove_cnt(functor.get_remove_cnt());
      ensure_checksum_(functor.get_checksum_last_scn());
    }
  }
  return ret;
}

int ObTxCallbackList::remove_callbacks_for_remove_memtable(
  const memtable::ObMemtableSet *memtable_set,
  const share::SCN stop_scn)
{
  // remove callbacks for remove_memtable imply the
  // callbacks must have been synced (because of the memtable must has been checkpointed)
  // so, it is safe to stop at sync_scn_
  // this operation also comes through TxCtx, and has hold TxCtx's whole lock
  // hence, acquire iter_latch is not required actually.
  int ret = OB_SUCCESS;
  LockGuard guard(*this, LOCK_MODE::LOCK_ITERATE);
  const bool skip_checksum = is_skip_checksum_();
  const share::SCN right_bound = skip_checksum ? share::SCN::max_scn()
    : (stop_scn.is_max() ? sync_scn_ : stop_scn);
  struct Functor final : public ObRemoveSyncCallbacksWCondFunctor {
    Functor(const bool need_remove_data = true, const bool is_reverse = false)
      : ObRemoveSyncCallbacksWCondFunctor(need_remove_data, is_reverse) {}
    bool cond_for_remove(ObITransCallback *callback) {
      bool ok = false;
      int ret = OB_SUCCESS;
      int bool_ret = true;
      while (!ok) {
        if (OB_HASH_EXIST == (ret = memtable_set_->exist_refactored((uint64_t)callback->get_memtable()))) {
          bool_ret = true;
          ok = true;
        } else if (OB_HASH_NOT_EXIST == ret) {
          bool_ret = false;
          ok = true;
        } else {
          // We have no idea to handle the error
          TRANS_LOG(ERROR, "hashset fetch encounter unexpected error", K(ret));
          ok = false;
        }
      }
      return bool_ret;
    }
    bool cond_for_stop(ObITransCallback *callback) const {
      return callback->get_scn() > right_bound_;
    }
    share::SCN right_bound_;
    const memtable::ObMemtableSet *memtable_set_;
  } functor(false);

  functor.right_bound_ = right_bound;
  functor.memtable_set_ = memtable_set;

  if (!skip_checksum) {
    functor.set_checksumer(checksum_scn_, &batch_checksum_);
  }

  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(ERROR, "remove callbacks for remove memtable wont report error", K(ret), K(functor));
  } else {
    callback_mgr_.add_release_memtable_callback_remove_cnt(functor.get_remove_cnt());
    ensure_checksum_(functor.get_checksum_last_scn());
    if (functor.get_remove_cnt() > 0) {
      TRANS_LOG(INFO, "remove callbacks for remove memtable", KP(memtable_set),
                K(functor.get_remove_cnt()), K(stop_scn), K(right_bound), K(functor), K(*this));
    }
  }

  return ret;
}

// when remove callback for rollback to in replay
// the caller has promise the replay of callback list is prefix completed
// especially for branch level savepoint rollback in parallel replay situation
int ObTxCallbackList::remove_callbacks_for_rollback_to(const transaction::ObTxSEQ to_seq,
                                                       const transaction::ObTxSEQ from_seq,
                                                       const share::SCN replay_scn)
{
  int ret = OB_SUCCESS;
  // because of rollback_to has acquire TxCtx's whole lock, it can prevent all operations
  // through TxCtx
  // acquire append_latch_ to prevent append, this is because of
  // table-lock will append callback directly to callback-list passthrough TxCtx's
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  const share::SCN right_bound = replay_scn.is_valid() ? replay_scn : share::SCN::max_scn();
  struct Functor final : public ObRemoveCallbacksWCondFunctor {
    Functor(const share::SCN right_bound, const bool need_remove_data = true)
      : ObRemoveCallbacksWCondFunctor(right_bound, need_remove_data) {}
    bool cond_for_remove(ObITransCallback *callback, int &ret) {
      transaction::ObTxSEQ dseq = callback->get_seq_no();
      bool match = false;
      if (to_seq_.get_branch() == 0                          // match all branches
          || to_seq_.get_branch() == dseq.get_branch()) {    // match target branch
        if (dseq.get_seq() >= from_seq_.get_seq()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "found callback with seq_no larger than rollback from point",
                    K(ret), K(dseq), K_(from_seq), K_(to_seq), KPC(callback));
        } else {
          match = dseq.get_seq() > to_seq_.get_seq();          // exclusive
        }
      }
      return match;
    }
    transaction::ObTxSEQ to_seq_;
    transaction::ObTxSEQ from_seq_;
  } functor(right_bound, true);

  functor.to_seq_ = to_seq;
  functor.from_seq_ = from_seq;

  if(!is_skip_checksum_()) {
    functor.set_checksumer(checksum_scn_, &batch_checksum_);
  }
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(ERROR, "remove callback by rollback wont report error", K(ret), K(functor));
  } else {
    int64_t removed = functor.get_remove_cnt();
    if (to_seq.support_branch() && to_seq.get_branch()) {
      branch_removed_ += removed;
    }
    callback_mgr_.add_rollback_to_callback_remove_cnt(removed);
    ensure_checksum_(functor.get_checksum_last_scn());
    TRANS_LOG(TRACE, "remove callbacks for rollback to", K(to_seq), K(from_seq), K(removed), K(functor), K(*this));
  }
  return ret;
}

int ObTxCallbackList::reverse_search_callback_by_seq_no(const transaction::ObTxSEQ seq_no,
                                                        ObITransCallback *search_res)
{
  int ret = OB_SUCCESS;

  ObSearchCallbackWCondFunctor functor(
    [seq_no](ObITransCallback *callback) -> bool {
      if (callback->get_seq_no() <= seq_no) {
        return true;
      } else {
        return false;
      }
    }, true/*is_reverse*/);

  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(ERROR, "search callbacks wont report error", K(ret), K(functor));
  } else {
    search_res = functor.get_search_result();
  }

  return ret;
}

// caller must has hold log_latch_
// fill log from log_cursor -> end
__attribute__((noinline))
int ObTxCallbackList::fill_log(ObITransCallback* log_cursor, ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &functor)
{
  // the remove callback operations is either remove logged callback or unlogged cabback
  // if remove logged callback, it is not conjunct with fill_log, because fill_log only
  // access callbacks which must has not bee submitted and synced
  // if remove unlogged callback, like rollback_to_savepoint, clean_unlogged_callback, sync_log_fail etc
  // these operation is exclusive with fill_log in TxCtx's level by using exclusive lock on TxCtx
  int ret = OB_SUCCESS;
  if (log_cursor == &head_) {
    // NOTE: log_cursor is un-stable, can not go through next `callack_`
    // which may caused some callback skipped
  } else {
    functor.reset();
    LockState lock_state;
    ret = callback_(functor, log_cursor->get_prev(), &head_, lock_state);
    ctx.helper_->max_seq_no_ = MAX(functor.get_max_seq_no(), ctx.helper_->max_seq_no_);
    ctx.helper_->data_size_ += functor.get_data_size();
    ctx.callback_scope_->data_size_ += functor.get_data_size();
  }
  TRANS_LOG(TRACE, "[FILL LOG] list_fill_log done", K(ret), K(log_cursor));
  return ret;
}

int ObTxCallbackList::submit_log_succ(const ObCallbackScope &callbacks)
{
  int ret = OB_SUCCESS;
  // when log submitted, update log_cursor_ point to next to-log callback
  // the thread has hold Tx-ctx's FLUSH_REDO lock and callback_list's log_lock_
  // this promise update log_cursor is serialized, but the next position
  // may be unstable due to append callback is allowed race with this by writer
  // thread, this can only happen if this scope's end is tail
  ObITransCallback *next = (*callbacks.end_)->get_next();
  if (next == &head_) {
    // next is un-stable, need serialize with append
    LockGuard guard(*this, LOCK_MODE::LOCK_APPEND);
    ATOMIC_STORE(&log_cursor_, (*callbacks.end_)->get_next());
  } else {
    ATOMIC_STORE(&log_cursor_, next);
  }
  ATOMIC_AAF(&logged_, (int64_t)callbacks.cnt_);
  ATOMIC_AAF(&logged_data_size_, callbacks.data_size_);
  return ret;
}

int ObTxCallbackList::sync_log_succ(const share::SCN scn, int64_t sync_cnt)
{
  // because log-succ callback is concurrent with submit log
  // so the sync_scn_ may be update before log_cursor updated
  int ret = OB_SUCCESS;
  sync_scn_.atomic_store(scn);
  synced_ += sync_cnt;
  return ret;
}

int ObTxCallbackList::sync_log_fail(const ObCallbackScope &callbacks,
                                    const share::SCN scn,
                                    int64_t &removed_cnt)
{
  int ret = OB_SUCCESS;
  ObSyncLogFailFunctor functor;
  functor.max_committed_scn_ = scn;
  // prevent append, because table-lock will not acquire tx-ctx'lock to append callback
  // the sync_log_fail will acquire tx-ctx' whole lock, which prevent other operations
  // on tx-ctx like : fast-commit, fill_redo, backfill-log-scn, remove-callbacks etc.
  // hence hold append_latch is enough
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, callbacks, guard.state_))) {
    TRANS_LOG(WARN, "clean unlog callbacks failed", K(ret), K(functor));
  } else {
    TRANS_LOG(INFO, "handle sync log fail success", K(functor), K(*this));
  }
  removed_cnt = functor.get_remove_cnt();
  return ret;
}

int ObTxCallbackList::clean_unlog_callbacks(int64_t &removed_cnt, common::ObFunction<void()> &before_remove)
{
  int ret = OB_SUCCESS;
  ObCleanUnlogCallbackFunctor functor(before_remove);

  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (log_cursor_ == &head_) {
    // empty set, all were logged
  } else if (OB_FAIL(callback_(functor, log_cursor_->get_prev(), &head_, guard.state_))) {
    TRANS_LOG(WARN, "clean unlog callbacks failed", K(ret), K(functor));
  } else {
    TRANS_LOG(INFO, "clean unlog callbacks", K(functor), K(*this));
  }
  removed_cnt = functor.get_remove_cnt();
  return ret;
}

int ObTxCallbackList::get_memtable_key_arr_w_timeout(transaction::ObMemtableKeyArray &memtable_key_arr)
{
  int ret = OB_SUCCESS;
  ObGetMemtableKeyWTimeoutFunctor functor(memtable_key_arr);

  ObTimeGuard tg("get memtable key arr", 500 * 1000L);
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL, &tg);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(WARN, "get memtable key arr failed", K(ret), K(functor));
  }

  return ret;
}

int ObTxCallbackList::tx_calc_checksum_before_scn(const SCN scn)
{
  int ret = OB_SUCCESS;
  LockGuard guard(*this, LOCK_MODE::LOCK_ITERATE);
  const share::SCN stop_scn = scn.is_max() ? sync_scn_ : scn;
  ObCalcChecksumFunctor functor(stop_scn);
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(ERROR, "calc checksum  failed", K(ret));
  } else {
    ensure_checksum_(functor.get_checksum_last_scn());
    TRANS_LOG(INFO, "calc checksum before log ts", K(scn), K(stop_scn), K(functor), KPC(this));
  }

  return ret;
}


int ObTxCallbackList::tx_calc_checksum_all()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(checksum_scn_.is_max() &&
                  0 != checksum_)) {
    // There could be a scenario where, under the condition that checksum_scn is
    // persisted at its maximum value, while checksum_ might be 0. In such a
    // scenario, we still rely on calculating the checksum to prevent mistakenly
    // using 0 for checksum verification and avoid potential errors.
    //
    // skip the unnecessary repeate calc checksum
  } else {
    LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
    ObCalcChecksumFunctor functor;
    functor.set_checksumer(checksum_scn_, &batch_checksum_);

    if (OB_FAIL(callback_(functor, guard.state_))) {
      TRANS_LOG(ERROR, "calc checksum wont report error", K(ret), K(functor));
    } else {
      ensure_checksum_(SCN::max_scn());
    }
  }
  return ret;
}

int ObTxCallbackList::tx_commit()
{
  int ret = OB_SUCCESS;
  ObTxEndFunctor functor(true/*is_commit*/);
  // exclusive with fast_commit, because fast-commit maybe in-progress
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(WARN, "trans commit failed", K(ret), K(functor));
  } else if (length_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "callback list has not been cleaned after commit callback", K(ret), K(functor));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  callback_mgr_.add_tx_end_callback_remove_cnt(functor.get_remove_cnt());
  return ret;
}

int ObTxCallbackList::tx_abort()
{
  int ret = OB_SUCCESS;
  ObTxEndFunctor functor(false/*is_commit*/);
  // exclusive with fast_commit, because fast-commit maybe in-progress
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(WARN, "trans abort failed", K(ret), K(functor));
  } else if (length_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "callback list has not been cleaned after abort", K(ret), K(functor));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  callback_mgr_.add_tx_end_callback_remove_cnt(functor.get_remove_cnt());

  return ret;
}

int ObTxCallbackList::tx_elr_preparing()
{
  int ret = OB_SUCCESS;
  ObTxForAllFunctor functor(
    [](ObITransCallback *callback) -> int {
      return callback->elr_trans_preparing();
    });
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(WARN, "trans elr preparing failed", K(ret), K(functor));
  }

  return ret;
}

int ObTxCallbackList::tx_print_callback()
{
  int ret = OB_SUCCESS;
  ObTxForAllFunctor functor(
    [](ObITransCallback *callback) -> int {
      return callback->print_callback();
    });
  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  if (OB_FAIL(callback_(functor, guard.state_))) {
    TRANS_LOG(WARN, "trans commit failed", K(ret), K(functor));
  }

  return ret;
}

int ObTxCallbackList::replay_succ(const SCN scn)
{
  sync_scn_.inc_update(scn);
  return OB_SUCCESS;
}

int ObTxCallbackList::replay_fail(const SCN scn, const bool serial_replay)
{
  int ret = OB_SUCCESS;
  struct Functor final : public ObRemoveSyncCallbacksWCondFunctor {
    Functor(const bool need_remove_data = true, const bool is_reverse = false)
      : ObRemoveSyncCallbacksWCondFunctor(need_remove_data, is_reverse) {}
    bool cond_for_remove(ObITransCallback *callback) {
      return scn_ == callback->get_scn();
    }
    bool cond_for_stop(ObITransCallback *callback) const {
      return scn_ != callback->get_scn();
    }
    share::SCN scn_;
  } functor(true, true);

  functor.scn_ = scn;

  LockGuard guard(*this, LOCK_MODE::LOCK_ALL);
  //
  // for replay fail of serial log, if parallel replay has happened,
  // must reverse traversal from parallel_start_pos_.prev_
  //
  // head_ --> ... -> parallel_start_pos_ -> ... -> head_
  //
  ObITransCallback *start_pos = NULL;
  ObITransCallback *end_pos = NULL;
  if (serial_replay) {
    start_pos = parallel_start_pos_ ?: get_guard();
    end_pos = get_guard();
  } else {
    start_pos = get_guard();
    end_pos = parallel_start_pos_ ? parallel_start_pos_->get_prev() : get_guard();
  }
  if (OB_FAIL(callback_(functor, start_pos, end_pos, guard.state_))) {
    TRANS_LOG(ERROR, "replay fail failed", K(ret), K(functor));
  } else {
    TRANS_LOG(INFO, "replay log failed, revert its callbacks done",
              KP(start_pos), KP(end_pos),
              K(functor), K(*this), K(scn));
  }
  // revert counters when replay fail
  ATOMIC_SAF(&appended_, functor.get_remove_cnt());
  ATOMIC_SAF(&removed_, functor.get_remove_cnt());
  ATOMIC_SAF(&logged_, functor.get_remove_cnt());
  ATOMIC_SAF(&synced_, functor.get_remove_cnt());
  return OB_SUCCESS;
}

void ObTxCallbackList::get_checksum_and_scn(uint64_t &checksum, SCN &checksum_scn)
{
  LockGuard guard(*this, LOCK_MODE::LOCK_ITERATE);

  if (checksum_scn_.is_max()) {
    checksum = checksum_;
    checksum_scn = checksum_scn_;
  } else {
    checksum = batch_checksum_.calc();
    checksum_scn = checksum_scn_;
  }
  if (checksum_scn.is_max() && checksum == 0) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "checksum should not be 0 if checksum_scn is max", KPC(this));
  }
  TRANS_LOG(INFO, "get checksum and checksum_scn", KPC(this), K(checksum), K(checksum_scn));
}

void ObTxCallbackList::update_checksum(const uint64_t checksum, const SCN checksum_scn)
{
  LockGuard guard(*this, LOCK_MODE::LOCK_ITERATE);
  if (checksum_scn.is_max()) {
    if (checksum == 0 && id_ > 0) {
      // only check extends list, because version before 4.2.4 with 0 may happen
      // and they will be replayed into first list (id_ equals to 0)
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "checksum should not be 0 if checksum_scn is max", KPC(this));
    }
    checksum_ = checksum ?: 1;
  }
  batch_checksum_.set_base(checksum);
  checksum_scn_.atomic_set(checksum_scn);
  TRANS_LOG(INFO, "update checksum and checksum_scn", KPC(this), K(checksum), K(checksum_scn));
}

void ObTxCallbackList::ensure_checksum_(const SCN scn)
{
  if (is_skip_checksum_()) {
  } else if (!scn.is_valid()) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "checksum scn is invalid", KPC(this));
  } else if (SCN::min_scn() == scn) {
    // Case1: no callback is invovled
  } else if (scn < checksum_scn_) {
    // Case2: no checksum is calculated
  } else if (SCN::max_scn() == scn) {
    checksum_scn_ = SCN::max_scn();
    ATOMIC_STORE(&checksum_, batch_checksum_.calc() ? : 1);
  } else {
    checksum_scn_ = SCN::plus(scn, 1);
    ATOMIC_STORE(&tmp_checksum_, batch_checksum_.calc() ? : 1);
  }
}

transaction::ObPartTransCtx *ObTxCallbackList::get_trans_ctx() const
{
  return callback_mgr_.get_trans_ctx();
}

DEF_TO_STRING(ObTxCallbackList)
{
  int64_t pos = 0;
  transaction::ObPartTransCtx *tx_ctx = get_trans_ctx();
  const transaction::ObTransID tx_id = tx_ctx ? tx_ctx->get_trans_id() : transaction::ObTransID();
  const share::ObLSID ls_id = tx_ctx ? tx_ctx->get_ls_id() : ObLSID();
  J_OBJ_START();
  J_KV(K_(id),
       KP(tx_ctx),
       K(tx_id),
       K(ls_id),
       K_(appended),
       K_(length),
       K_(logged),
       K_(removed),
       K_(branch_removed),
       K_(unlog_removed),
       K_(sync_scn),
       KP_(parallel_start_pos),
       "parallel_start_scn", (parallel_start_pos_ ? parallel_start_pos_->get_scn() : share::SCN::invalid_scn()),
       "skip_checksum", is_skip_checksum_(),
       K_(checksum_scn),
       K_(checksum),
       K_(tmp_checksum),
       K_(batch_checksum));
  J_OBJ_END();
  return pos;
}

int ObTxCallbackList::get_stat_for_display(ObTxCallbackListStat &stat) const
{
  int ret = OB_SUCCESS;
#define _ASSIGN_STAT_(x) stat.x = x;
  LST_DO(_ASSIGN_STAT_, (), id_, sync_scn_, length_, logged_, removed_, branch_removed_);
#undef __ASSIGN_STAT_
  return ret;
}

bool ObTxCallbackList::find(ObITxCallbackFinder &func)
{
  int ret = OB_SUCCESS;
  LockGuard guard(*this, LOCK_MODE::LOCK_ITERATE);
  if (OB_FAIL(callback_(func, guard.state_))) {
    TRANS_LOG(WARN, "", K(ret));
  }
  return func.is_found();
}

bool ObTxCallbackList::check_all_redo_flushed(const bool quite) const
{
  bool ok = (log_cursor_ == &head_) &&
    (appended_ == (logged_ + unlog_removed_));
  if (!ok && !quite) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback list is not all flushed", KPC(this));
  }
  return ok;
}

__attribute__((noinline))
int64_t ObTxCallbackList::get_log_epoch() const
{
  return log_cursor_ == &head_ ? INT64_MAX : log_cursor_->get_epoch();
}

void ObTxCallbackList::inc_update_sync_scn(const share::SCN scn)
{
  sync_scn_.inc_update(scn);
}

inline bool ObTxCallbackList::is_skip_checksum_() const
{
  return callback_mgr_.skip_checksum_calc();
}

inline bool ObTxCallbackList::is_append_only_() const
{
  return callback_mgr_.is_callback_list_append_only(id_);
}

bool ObTxCallbackList::is_logging_blocked() const
{
  bool blocked = false;
  if (log_latch_.try_lock()) {
    // acquire APPEND lock to prevent append callback and
    // reset log_cursor_ when log_cursor_ is point to head_
    // due to no callback need to logging
    // _NOTE_: the caller thread has hold TxCtx's FLUSH_REDO
    // lock which prevent operations of remove callbacks
    LockGuard guard(*this, LOCK_MODE::TRY_LOCK_APPEND);
    if (guard.is_locked()) {
      if (log_cursor_ != &head_ && log_cursor_->is_logging_blocked()) {
        blocked = true;
      }
    }
    log_latch_.unlock();
  }
  return blocked;
}

ObTxCallbackList::LockGuard::LockGuard(const ObTxCallbackList &list,
                                       const ObTxCallbackList::LOCK_MODE mode,
                                       ObTimeGuard *tg)
  : host_(list)
{
  using LOCK_MODE = ObTxCallbackList::LOCK_MODE;
  switch(mode) {
  case LOCK_MODE::LOCK_ALL:
    lock_iterate_(false);
    if (tg) {
      tg->click();
    }
    lock_append_(false);
    break;
  case LOCK_MODE::LOCK_APPEND:
    lock_append_(false);
    break;
  case LOCK_MODE::TRY_LOCK_APPEND:
    lock_append_(true);
    break;
  case LOCK_MODE::LOCK_ITERATE:
    lock_iterate_(false);
    break;
  case LOCK_MODE::TRY_LOCK_ITERATE:
    lock_iterate_(true);
    break;
  default:
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid lock mode", K(mode));
  }
}

void ObTxCallbackList::LockGuard::lock_iterate_(const bool try_lock)
{
  if (!state_.ITERATE_LOCKED_) {
    if (try_lock) {
      if (host_.iter_synced_latch_.try_lock()) {
        state_.ITERATE_LOCKED_ = true;
      }
    } else {
      host_.iter_synced_latch_.lock();
      state_.ITERATE_LOCKED_ = true;
    }
    if (state_.ITERATE_LOCKED_) {
      if (!host_.is_append_only_()) {
        lock_append_(false);
      }
    }
  }
}

void ObTxCallbackList::LockGuard::lock_append_(const bool try_lock)
{
  if (!state_.APPEND_LOCKED_) {
    if (try_lock) {
      if (host_.append_latch_.try_lock()) {
        state_.APPEND_LOCKED_ = true;
      }
    } else {
      host_.append_latch_.lock();
      state_.APPEND_LOCKED_ = true;
    }
  }
}

ObTxCallbackList::LockGuard::~LockGuard()
{
  if (state_.APPEND_LOCKED_) {
    host_.append_latch_.unlock();
  }
  if (state_.ITERATE_LOCKED_) {
    host_.iter_synced_latch_.unlock();
  }
}

} // memtable
} // oceanbase

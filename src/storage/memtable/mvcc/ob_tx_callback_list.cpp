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

namespace oceanbase
{
using namespace share;
namespace memtable
{

ObTxCallbackList::ObTxCallbackList(ObTransCallbackMgr &callback_mgr)
  : head_(),
    length_(0),
    batch_checksum_(),
    checksum_scn_(SCN::min_scn()),
    checksum_(0),
    tmp_checksum_(0),
    callback_mgr_(callback_mgr),
    latch_()
{ reset(); }

ObTxCallbackList::~ObTxCallbackList() {}

void ObTxCallbackList::reset()
{
  head_.set_prev(&head_);
  head_.set_next(&head_);
  checksum_ = 0;
  tmp_checksum_ = 0;
  checksum_scn_ = SCN::min_scn();
  batch_checksum_.reset();
  length_ = 0;
}

// the semantic of the append_callback is atomic which means the cb is removed
// and no side effect is taken effects if some unexpected failure has happened.
int ObTxCallbackList::append_callback(ObITransCallback *callback,
                                      const bool for_replay)
{
  int ret = OB_SUCCESS;
  // It is important that we should put the before_append_cb and after_append_cb
  // into the latch guard otherwise the callback may already paxosed and released
  // before callback it.
  ObByteLockGuard guard(latch_);

  if (OB_ISNULL(callback)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "before_append_cb failed", K(ret), KPC(callback));
  } else if (OB_FAIL(callback->before_append_cb(for_replay))) {
    TRANS_LOG(WARN, "before_append_cb failed", K(ret), KPC(callback));
  } else {
    (void)get_tail()->append(callback);
    length_++;

    // Once callback is appended into callback lists, we can not handle the
    // error after it. So it should never report the error later. What's more,
    // after_append also should never return the error.
    (void)callback->after_append_cb(for_replay);
  }

  return ret;
}

int64_t ObTxCallbackList::concat_callbacks(ObTxCallbackList &that)
{
  int64_t cnt = 0;

  if (that.empty()) {
    // do nothing
  } else {
    ObByteLockGuard this_guard(latch_);
    ObByteLockGuard that_guard(that.latch_);
    ObITransCallback *that_head = that.head_.get_next();
    ObITransCallback *that_tail = that.get_tail();
    that_head->set_prev(get_tail());
    that_tail->set_next(&head_);
    get_tail()->set_next(that_head);
    head_.set_prev(that_tail);
    cnt = that.get_length();
    length_ += cnt;
    that.reset();
  }

  return cnt;
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor)
{
  return callback_(functor, get_guard(), get_guard());
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor,
                                const ObCallbackScope &callbacks)
{
  ObITransCallback *start = (ObITransCallback *)*(callbacks.start_);
  ObITransCallback *end = (ObITransCallback *)*(callbacks.end_);
  if (functor.is_reverse()) {
    return callback_(functor, start->get_next(), end->get_prev());
  } else {
    return callback_(functor, start->get_prev(), end->get_next());
  }
}

int ObTxCallbackList::callback_(ObITxCallbackFunctor &functor,
                                ObITransCallback *start,
                                ObITransCallback *end)
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

    if (functor.is_iter_end(iter)) {
      iter_end = true;
    } else {
      next = (is_reverse ? iter->get_prev() : iter->get_next());

      if (OB_FAIL(functor(iter))) {
        TRANS_LOG(WARN, "functor call failed", KPC(iter));
      } else if (functor.need_remove_callback()) {
        if (OB_FAIL(iter->del())) {
          TRANS_LOG(ERROR, "remove callback failed", KPC(iter));
        } else {
          if (iter->is_need_free()) {
            callback_mgr_.get_ctx().callback_free(iter);
          }
          remove_count++;
        }
      }

      if ((++traverse_count & 0xFFFFF) == 0) {
        TRANS_LOG(WARN, "memtable fifo callback too long",
                  K(traverse_count), K(functor));
      }
    }
  }

  functor.set_statistics(traverse_count, remove_count);
  length_ -= remove_count;

  return ret;
}

int64_t ObTxCallbackList::calc_need_remove_count_for_fast_commit_()
{
  const int64_t fast_commit_callback_count = GCONF._fast_commit_callback_count;
  const int64_t recommand_reserve_count = (fast_commit_callback_count + 1) / 2;
  const int64_t need_remove_count = length_ - recommand_reserve_count;

  return need_remove_count;
}

int ObTxCallbackList::remove_callbacks_for_fast_commit(const ObITransCallback *generate_cursor,
                                                       bool &meet_generate_cursor)
{
  int ret = OB_SUCCESS;
  meet_generate_cursor = false;
  ObByteLockGuard guard(latch_);

  ObRemoveCallbacksForFastCommitFunctor functor(generate_cursor,
                                                calc_need_remove_count_for_fast_commit_());
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "remove callbacks for fast commit wont report error", K(ret), K(functor));
  } else {
    callback_mgr_.add_fast_commit_callback_remove_cnt(functor.get_remove_cnt());
    ensure_checksum_(functor.get_checksum_last_scn());
    meet_generate_cursor = functor.meet_generate_cursor();
  }

  return ret;
}

int ObTxCallbackList::remove_callbacks_for_remove_memtable(
  const memtable::ObMemtableSet *memtable_set,
  const share::SCN max_applied_scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(latch_);

  ObRemoveSyncCallbacksWCondFunctor functor(
    // condition for remove
    [memtable_set](ObITransCallback *callback) -> bool {
      bool ok = false;
      int ret = OB_SUCCESS;
      int bool_ret = true;
      while (!ok) {
        if (OB_HASH_EXIST == (ret = memtable_set->exist_refactored((uint64_t)callback->get_memtable()))) {
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
    }, // condition for stop
    [max_applied_scn](ObITransCallback *callback) -> bool {
      if (callback->get_scn() > max_applied_scn) {
        return true;
      } else {
        return false;
      }
    },
    false /*need_remove_data*/);
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "remove callbacks for remove memtable wont report error", K(ret), K(functor));
  } else {
    callback_mgr_.add_release_memtable_callback_remove_cnt(functor.get_remove_cnt());
    ensure_checksum_(functor.get_checksum_last_scn());
    if (functor.get_remove_cnt() > 0) {
      TRANS_LOG(INFO, "remove callbacks for remove memtable", KP(memtable_set),
                K(functor), K(*this));
    }
  }

  return ret;
}

int ObTxCallbackList::remove_callbacks_for_rollback_to(const transaction::ObTxSEQ to_seq_no)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(latch_);

  ObRemoveCallbacksWCondFunctor functor(
    [to_seq_no](ObITransCallback *callback) -> bool {
      if (callback->get_seq_no() > to_seq_no) {
        return true;
      } else {
        return false;
      }
    }, true/*need_remove_data*/);
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "remove callback by rollback wont report error", K(ret), K(functor));
  } else {
    callback_mgr_.add_rollback_to_callback_remove_cnt(functor.get_remove_cnt());
    ensure_checksum_(functor.get_checksum_last_scn());
    TRANS_LOG(DEBUG, "remove callbacks for rollback to", K(to_seq_no), K(functor), K(*this));
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

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "search callbacks wont report error", K(ret), K(functor));
  } else {
    search_res = functor.get_search_result();
  }

  return ret;
}

int ObTxCallbackList::sync_log_fail(const ObCallbackScope &callbacks,
                                    int64_t &removed_cnt)
{
  int ret = OB_SUCCESS;
  ObSyncLogFailFunctor functor;

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor, callbacks))) {
    TRANS_LOG(WARN, "clean unlog callbacks failed", K(ret), K(functor));
  } else {
    TRANS_LOG(INFO, "sync failed log successfully", K(functor), K(*this));
  }
  removed_cnt = functor.get_remove_cnt();
  return ret;
}

int ObTxCallbackList::clean_unlog_callbacks(int64_t &removed_cnt)
{
  int ret = OB_SUCCESS;
  ObCleanUnlogCallbackFunctor functor;

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
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

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(WARN, "get memtable key arr failed", K(ret), K(functor));
  }

  return ret;
}

int ObTxCallbackList::tx_calc_checksum_before_scn(const SCN scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(latch_);

  ObCalcChecksumFunctor functor(scn);
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "calc checksum  failed", K(ret));
  } else {
    ensure_checksum_(functor.get_checksum_last_scn());
    TRANS_LOG(INFO, "calc checksum before log ts", K(functor), K(*this));
  }

  return ret;
}


int ObTxCallbackList::tx_calc_checksum_all()
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(latch_);

  ObCalcChecksumFunctor functor;
  functor.set_checksumer(checksum_scn_, &batch_checksum_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "calc checksum wont report error", K(ret), K(functor));
  } else {
    ensure_checksum_(SCN::max_scn());
  }

  return ret;
}

int ObTxCallbackList::tx_commit()
{
  int ret = OB_SUCCESS;
  ObTxEndFunctor functor(true/*is_commit*/);

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(WARN, "trans commit failed", K(ret), K(functor));
  } else {
    callback_mgr_.add_tx_end_callback_remove_cnt(functor.get_remove_cnt());
  }

  return ret;
}

int ObTxCallbackList::tx_abort()
{
  int ret = OB_SUCCESS;
  ObTxEndFunctor functor(false/*is_commit*/);

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(WARN, "trans abort failed", K(ret), K(functor));
  } else {
    callback_mgr_.add_tx_end_callback_remove_cnt(functor.get_remove_cnt());
  }

  return ret;
}

int ObTxCallbackList::tx_elr_preparing()
{
  int ret = OB_SUCCESS;
  ObTxForAllFunctor functor(
    [](ObITransCallback *callback) -> int {
      return callback->elr_trans_preparing();
    });

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
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

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(WARN, "trans commit failed", K(ret), K(functor));
  }

  return ret;
}

int ObTxCallbackList::replay_fail(const SCN scn)
{
  int ret = OB_SUCCESS;
  ObRemoveSyncCallbacksWCondFunctor functor(
    // condition for remove
    [scn](ObITransCallback *callback) -> bool {
      if (scn == callback->get_scn()) {
        return true;
      } else {
        return false;
      }
    }, // condition for stop
    [scn](ObITransCallback *callback) -> bool {
      if (scn != callback->get_scn()) {
        return true;
      } else {
        return false;
      }
    },
    true, /*need_remove_data*/
    true /*is_reverse*/);

  ObByteLockGuard guard(latch_);

  if (OB_FAIL(callback_(functor))) {
    TRANS_LOG(ERROR, "replay fail failed", K(ret), K(functor));
  } else {
    TRANS_LOG(INFO, "replay callbacks failed and remove callbacks succeed",
              K(functor), K(*this), K(scn));
  }

  return OB_SUCCESS;
}

void ObTxCallbackList::get_checksum_and_scn(uint64_t &checksum, SCN &checksum_scn)
{
  ObByteLockGuard guard(latch_);
  checksum = batch_checksum_.calc();
  checksum_scn = checksum_scn_;
  TRANS_LOG(INFO, "get checksum and checksum_scn", KPC(this), K(checksum), K(checksum_scn));
}

void ObTxCallbackList::update_checksum(const uint64_t checksum, const SCN checksum_scn)
{
  ObByteLockGuard guard(latch_);
  batch_checksum_.set_base(checksum);
  checksum_scn_.atomic_set(checksum_scn);
  TRANS_LOG(INFO, "update checksum and checksum_scn", KPC(this), K(checksum), K(checksum_scn));
}

void ObTxCallbackList::ensure_checksum_(const SCN scn)
{
  if (SCN::min_scn() == scn) {
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
  J_OBJ_START();
  J_KV(KPC(get_trans_ctx()),
       K_(length),
       K_(checksum_scn),
       K_(checksum),
       K_(tmp_checksum));
  J_OBJ_END();
  return pos;
}

} // memtable
} // oceanbase

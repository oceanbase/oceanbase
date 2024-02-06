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

#ifndef OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_FUNCTOR
#define OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_FUNCTOR

#include "lib/function/ob_function.h"
#include "storage/memtable/mvcc/ob_mvcc.h"

namespace oceanbase
{
namespace memtable
{

class ObITxCallbackFunctor
{
public:
  ObITxCallbackFunctor()
    : need_remove_callback_(false),
    is_reverse_(false),
    traverse_cnt_(0),
    remove_cnt_(0) {}
  ~ObITxCallbackFunctor() {}
  virtual int operator()(ObITransCallback *callback) { return OB_SUCCESS; };
  virtual bool is_reverse() const { return is_reverse_; }
  virtual bool is_iter_end(ObITransCallback *callback) const { return false; }
  bool need_remove_callback() const { return need_remove_callback_; }
  void refresh() { need_remove_callback_ = false; }
  void set_statistics(int64_t traverse_cnt, int64_t remove_cnt)
  {
    traverse_cnt_ = traverse_cnt;
    remove_cnt_ = remove_cnt;
  }
  int64_t get_traverse_cnt() const { return traverse_cnt_; }
  int64_t get_remove_cnt() const { return remove_cnt_; }
  VIRTUAL_TO_STRING_KV(K_(need_remove_callback),
                       K_(is_reverse),
                       K_(traverse_cnt),
                       K_(remove_cnt));
protected:
  bool need_remove_callback_;
  bool is_reverse_;
  int64_t traverse_cnt_;
  int64_t remove_cnt_;
};

// We should remove callbacks for fast commit. Fast commit is designed to divide
// time-consuming tasks like traversing callbacks to regular tasks like read or
// write. The advantage of fast commit allows the cpu costs to be dispersed and
// improves the overall throughput.
//
// To implement fast commit, we maintain last FAST_FREEZE_MAX_ALLOWED_CB_COUNT
// callbacks and remove others during applying and replaying logs. When removing
// callbacks, we need pay attention to the following things:
// 1. we need calculate the checksum for removed callbacks otherwise we will
//    omit counting them and lead to mismatch with other replicas.
// 2. In order to correctly calculate the checksum, we should calculate them
//    based on the granlurity of redo logs. So even if need_remove_count has
//    reach 0, we should continue the calculation for the callbacks with same
//    log timestamp as last_scn_for_remove
// 3. we need free the callbacks because its allocator requires immdeiately
//    reclamination
class ObRemoveCallbacksForFastCommitFunctor : public ObITxCallbackFunctor
{
public:
  ObRemoveCallbacksForFastCommitFunctor(const ObITransCallback *generate_cursor,
                                        const int64_t need_remove_count)
    : generate_cursor_(generate_cursor),
    need_remove_count_(need_remove_count),
    last_scn_for_remove_(share::SCN::min_scn()),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    meet_generate_cursor_(false) {}

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    bool is_iter_end = false;

    if (NULL == callback) {
      // case1: the callback is nullptr
      is_iter_end = true;
    } else if (callback->need_fill_redo() || callback->need_submit_log()) {
      // case2: the callback has not been sync successfully
      is_iter_end = true;
    } else if (share::SCN::max_scn() == callback->get_scn()) {
      // case3: the callback has not been sync successfully
      is_iter_end = true;
    } else if (share::SCN::min_scn() != last_scn_for_remove_
               && callback->get_scn() != last_scn_for_remove_) {
      // case4: the callback has exceeded the last log whose log ts need to be
      //         removed
      is_iter_end = true;
    } else if (share::SCN::min_scn() == last_scn_for_remove_
               && 0 >= need_remove_count_) {
      // case6: the callback has not reached the last log whose log ts need to
      //         be removed while having no logs need to be removed
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }

    return is_iter_end;
  }

  void set_checksumer(const share::SCN checksum_scn,
                      ObBatchChecksum *checksumer)
  {
    checksum_scn_ = checksum_scn;
    checksumer_ = checksumer;
  }

  share::SCN get_checksum_last_scn() const
  {
    return last_scn_for_remove_;
  }

  int operator()(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;

    if (NULL == checksumer_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "checksumer is lost", K(ret), K(*callback));
    } else if (callback->get_scn() >= checksum_scn_
               && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
      TRANS_LOG(WARN, "calc checksum callback failed", K(ret), K(*callback));
    } else if (OB_FAIL(callback->checkpoint_callback())) {
      TRANS_LOG(ERROR, "row remove callback failed", K(ret), K(*callback));
    } else {
      need_remove_callback_ = true;
      need_remove_count_--;

      // If we are removing callback pointed by generate_cursor, we need reset
      // the generate_cursor. Otherwise the dangling pointer may coredump.
      if (generate_cursor_ == callback) {
        meet_generate_cursor_ = true;
      }

      // if we satisfy the removing count of fast commit, we still need remember
      // the last log ts we have already removed and then continue to remove the
      // callbacks until all callbacks with the same log ts has been removed in
      // order to satisfy the checksum calculation
      ObITransCallback *next = callback->get_next();
      if (share::SCN::min_scn() == last_scn_for_remove_) {
        if (0 == need_remove_count_) {
          last_scn_for_remove_ = callback->get_scn();
        } else if (NULL == next
                   || next->need_submit_log()
                   || next->need_fill_redo()
                   || share::SCN::max_scn() == next->get_scn()) {
          last_scn_for_remove_ = callback->get_scn();
        }
      }
    }

    return ret;
  }

  // return whether we are removing callbacks that pointed by generate cursor
  bool meet_generate_cursor() const
  {
    return meet_generate_cursor_;
  }

  VIRTUAL_TO_STRING_KV(KP_(generate_cursor),
                       K_(need_remove_count),
                       K_(checksum_scn),
                       K_(last_scn_for_remove),
                       K_(meet_generate_cursor));

private:
  const ObITransCallback *generate_cursor_;
  int64_t need_remove_count_;
  share::SCN last_scn_for_remove_;
  share::SCN checksum_scn_;
  ObBatchChecksum *checksumer_;

  bool meet_generate_cursor_;
};

class ObNeverStopForCallbackTraverseFunctor
{
public:
  bool operator()(ObITransCallback *callback)
  {
    UNUSED(callback);
    return false;
  }
};

// TODO(handora.qc): Adapt to ObRemoveCallbacksWCondFunctor
class ObRemoveSyncCallbacksWCondFunctor : public ObITxCallbackFunctor
{
public:
  ObRemoveSyncCallbacksWCondFunctor(
    const ObFunction<bool(ObITransCallback*)> &remove_func,
    const ObFunction<bool(ObITransCallback*)> &stop_func = ObNeverStopForCallbackTraverseFunctor(),
    const bool need_remove_data = true,
    const bool is_reverse = false)
    : cond_for_remove_(remove_func),
    cond_for_stop_(stop_func),
    need_checksum_(false),
    need_remove_data_(need_remove_data),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    checksum_last_scn_(share::SCN::min_scn())
    {
      is_reverse_ = is_reverse;
    }

  bool check_valid_()
  {
    bool is_valid = true;

    if (need_checksum_ && true == is_reverse_) {
      is_valid = false;
      TRANS_LOG_RET(ERROR, common::OB_INVALID_ERROR, "we cannot calc checksum when reverse remove", KPC(this));
    }

    return is_valid;
  }

  void set_checksumer(const share::SCN checksum_scn,
                      ObBatchChecksum *checksumer)
  {
    need_checksum_ = true;
    checksum_scn_ = checksum_scn;
    checksumer_ = checksumer;
  }

  share::SCN get_checksum_last_scn() const
  {
    if (need_checksum_) {
      return checksum_last_scn_;
    } else {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "we donot go here if we donot checksum", KPC(this));
      return share::SCN::min_scn();
    }
  }

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    bool is_iter_end = false;

    if (NULL == callback) {
      // case1: the callback is nullptr
      is_iter_end = true;
    } else if (callback->need_fill_redo() || callback->need_submit_log()) {
      // case2: the callback has not been sync successfully
      is_iter_end = true;
    } else if (share::SCN::max_scn() == callback->get_scn()) {
      // case3: the callback has not been sync successfully
      is_iter_end = true;
    } else if (cond_for_stop_(callback)) {
      // case4: user want to stop here
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }

    return is_iter_end;
  }

  int operator()(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;

    if (!check_valid_()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "we cannot calc checksum when reverse remove", K(ret), KPC(this));
    } else if (callback->need_fill_redo() || callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "remove synced will never go here", K(ret), KPC(callback));
    } else if (need_checksum_
               && callback->get_scn() >= checksum_scn_
               && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
      TRANS_LOG(WARN, "row remove callback failed", K(ret), K(*callback));
    } else if (FALSE_IT(checksum_last_scn_ = callback->get_scn())) {
    } else if (cond_for_remove_(callback)) {
      if (need_remove_data_ && OB_FAIL(callback->rollback_callback())) {
        TRANS_LOG(WARN, "rollback callback failed", K(ret), K(*callback));
      } else if (!need_remove_data_ && OB_FAIL(callback->checkpoint_callback())) {
        TRANS_LOG(WARN, "checkpoint callback failed", K(ret), K(*callback));
      } else {
        need_remove_callback_ = true;
      }
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(need_checksum),
                       K_(need_remove_data),
                       K_(checksum_scn),
                       K_(checksum_last_scn));

private:
  ObFunction<bool(ObITransCallback*)> cond_for_remove_;
  ObFunction<bool(ObITransCallback*)> cond_for_stop_;
  bool need_checksum_;
  bool need_remove_data_;
  share::SCN checksum_scn_;
  ObBatchChecksum *checksumer_;
  share::SCN checksum_last_scn_;
};

class ObRemoveCallbacksWCondFunctor : public ObITxCallbackFunctor
{
public:
  ObRemoveCallbacksWCondFunctor(ObFunction<bool(ObITransCallback*)> func,
                                const bool need_remove_data = true)
    : cond_for_remove_(func),
    need_remove_data_(need_remove_data),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    checksum_last_scn_(share::SCN::min_scn()) {}

  void set_checksumer(const share::SCN checksum_scn,
                      ObBatchChecksum *checksumer)
  {
    checksum_scn_ = checksum_scn;
    checksumer_ = checksumer;
  }

  share::SCN get_checksum_last_scn() const
  {
    if (NULL != checksumer_) {
      return checksum_last_scn_;
    } else {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "we donot go here if we donot checksum", KPC(this));
      return share::SCN::min_scn();
    }
  }

  int operator()(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    } else if (!callback->need_fill_redo() && callback->need_submit_log()) {
      // Case 1: callback synced before proposed to paxos
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "It will never on success before submit log", KPC(callback));
    } else if (callback->need_fill_redo() && !callback->need_submit_log()) {
      // Case 2: callback has been submitted but not be synced
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "callbacks cannot be removed before synced", KPC(callback));
    } else if (callback->need_fill_redo() && callback->need_submit_log()) {
      // Case 3: callback has not been proposed to paxos
      if (cond_for_remove_(callback)) {
        if (need_remove_data_ && OB_FAIL(callback->rollback_callback())) {
          TRANS_LOG(WARN, "rollback callback failed", K(ret), K(*callback));
        } else if (!need_remove_data_ && OB_FAIL(callback->checkpoint_callback())) {
          TRANS_LOG(WARN, "checkpoint callback failed", K(ret), K(*callback));
        } else {
          need_remove_callback_ = true;
        }
      }
    } else if (!callback->need_fill_redo() && !callback->need_submit_log()) {
      // Case 4: callback has synced successfully
      if (cond_for_remove_(callback)) {
        if (NULL == checksumer_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "checksumer is lost", K(ret), K(*callback));
        } else if (callback->get_scn() >= checksum_scn_
                   && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
          TRANS_LOG(WARN, "calc checksum callback failed", K(ret), K(*callback));
        } else if (need_remove_data_ && OB_FAIL(callback->rollback_callback())) {
          TRANS_LOG(WARN, "rollback callback failed", K(ret), K(*callback));
        } else if (!need_remove_data_ && OB_FAIL(callback->checkpoint_callback())) {
          TRANS_LOG(WARN, "checkpoint callback failed", K(ret), K(*callback));
        } else {
          need_remove_callback_ = true;
          checksum_last_scn_ = callback->get_scn();
        }
      } else {
        if (NULL == checksumer_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "checksumer is lost", K(ret), K(*callback));
        } else if (callback->get_scn() >= checksum_scn_
                   && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
          TRANS_LOG(WARN, "row remove callback failed", K(ret), K(*callback));
        } else {
          checksum_last_scn_ = callback->get_scn();
        }
      }
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(need_remove_data),
                       K_(checksum_scn),
                       K_(checksum_last_scn));

private:
  ObFunction<bool(ObITransCallback*)> cond_for_remove_;
  bool need_remove_data_;
  share::SCN checksum_scn_;
  ObBatchChecksum *checksumer_;
  share::SCN checksum_last_scn_;
};

class ObTxForAllFunctor : public ObITxCallbackFunctor
{
public:
  ObTxForAllFunctor(ObFunction<int(ObITransCallback*)> functor)
    : f_(functor)  {}

  virtual int operator()(ObITransCallback *callback) override
  {
    return f_(callback);
  }

  VIRTUAL_TO_STRING_KV("ObTxForAllFunctor", "ObTxForAllFunctor");
private:
  ObFunction<int(ObITransCallback*)> f_;
};

class ObTxEndFunctor : public ObITxCallbackFunctor
{
public:
  ObTxEndFunctor(bool is_commit)
    : is_commit_(is_commit),
      need_print_(false) {}

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    /* } else if (is_commit_ && */
    /*            (callback->need_submit_log() */
    /*             || callback->need_fill_redo())) { */
    /*   ret = OB_ERR_UNEXPECTED; */
    /*   TRANS_LOG(ERROR, "unexpected callback", KP(callback)); */
    } else if (is_commit_
               && OB_FAIL(callback->trans_commit())) {
      TRANS_LOG(ERROR, "trans commit failed", KPC(callback));
    } else if (!is_commit_
               && OB_FAIL(callback->trans_abort())) {
      TRANS_LOG(ERROR, "trans abort failed", KPC(callback));
    } else {
      need_remove_callback_ = true;
      print_callback_if_logging_block_(callback);
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(is_commit), K_(need_print));

private:
  int print_callback_if_logging_block_(ObITransCallback *callback)
  {
    // print callback list
    // if a callback has not been submitted log and
    // the memtable linked to it is logging_blocked
    int ret = OB_SUCCESS;
    if (!is_commit_ &&
        !need_print_ &&
        callback->need_submit_log() &&
        callback->need_fill_redo() &&
        callback->is_logging_blocked()) {
      need_print_ = true;
    }
    if (need_print_) {
      callback->print_callback();
    }
    return ret;
  }

private:
  const bool is_commit_;
  bool need_print_;
};

class ObCleanUnlogCallbackFunctor : public ObITxCallbackFunctor
{
public:
  ObCleanUnlogCallbackFunctor() {}

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    } else if (callback->need_fill_redo() && !callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "all callbacks must be invoked before leader switch", KPC(callback));
    } else if (!callback->need_fill_redo() && callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "It will never on success before submit log", KPC(callback));
    } else if (callback->need_fill_redo() && callback->need_submit_log()) {
      callback->rollback_callback();
      need_remove_callback_ = true;
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV("CleanUnlogCallback", "CleanUnlogCallback");
};

class ObSyncLogFailFunctor : public ObITxCallbackFunctor
{
public:
  ObSyncLogFailFunctor() {}

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    } else if (callback->need_fill_redo() && callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "sync log fail will only touch submitted log", KPC(callback));
    } else if (!callback->need_fill_redo() && !callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "sync log fail will only touch unsynced log", KPC(callback));
    } else if (!callback->need_fill_redo() && callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "It will never on success before submit log", KPC(callback));
    } else if (callback->need_fill_redo() && !callback->need_submit_log()) {
      if (OB_FAIL(callback->log_sync_fail_cb())) {
        // log_sync_fail_cb will never report error
        TRANS_LOG(ERROR, "log sync fail cb report error", K(ret));
      } else {
        need_remove_callback_ = true;
      }
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV("ObSyncLogFailFunctor", "ObSyncLogFailFunctor");
};

class ObSearchCallbackWCondFunctor : public ObITxCallbackFunctor
{
public:
  ObSearchCallbackWCondFunctor(ObFunction<bool(ObITransCallback *)> cond,
                               const bool is_reverse)
    : search_cond_(cond),
    search_res_(NULL)
    {
      is_reverse_ = is_reverse;
    }

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    UNUSED(callback);
    return NULL != search_res_;
  }

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (search_cond_(callback)) {
      search_res_ = callback;
    }

    return ret;
  }

  ObITransCallback *get_search_result() { return search_res_; }

  VIRTUAL_TO_STRING_KV(K_(search_res));

private:
  ObFunction<bool(ObITransCallback *)> search_cond_;
  ObITransCallback *search_res_;
};

class ObGetMemtableKeyWTimeoutFunctor : public ObITxCallbackFunctor
{
public:
  ObGetMemtableKeyWTimeoutFunctor(transaction::ObMemtableKeyArray &memtable_key_arr)
    : start_ts_(ObTimeUtility::current_time()),
    memtable_key_arr_(memtable_key_arr) {}

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    // It can only take up to 300ms
    return ObTimeUtility::current_time() - start_ts_ >= 300 * 1000L;
  }

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(callback->merge_memtable_key(memtable_key_arr_))) {
      TRANS_LOG(WARN, "fail to merge memtable key", K(ret));
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(memtable_key_arr));

private:
  int64_t start_ts_;
  transaction::ObMemtableKeyArray &memtable_key_arr_;
};

class ObCalcChecksumFunctor : public ObITxCallbackFunctor
{
public:
  ObCalcChecksumFunctor(const share::SCN target_scn = share::SCN::max_scn())
    : target_scn_(target_scn),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    checksum_last_scn_(share::SCN::min_scn()) {}

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    bool is_iter_end = false;

    if (NULL == callback) {
      // case1: the callback is nullptr
      is_iter_end = true;
    } else if (callback->get_scn() > target_scn_) {
      // case2: the callback is behind than the target
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }

    return is_iter_end;
  }


  void set_checksumer(const share::SCN checksum_scn,
                      ObBatchChecksum *checksumer)
  {
    checksum_scn_ = checksum_scn;
    checksumer_ = checksumer;
  }

  share::SCN get_checksum_last_scn() const
  {
    return checksum_last_scn_;
  }

  int operator()(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;

    if (NULL == checksumer_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "checksumer is lost", K(ret), K(*callback));
    } else if (callback->get_scn() > target_scn_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "callback is begind the target, should iter end", K(ret), K(*callback));
    } else if (callback->get_scn() >= checksum_scn_
               && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
      TRANS_LOG(WARN, "calc checksum callback failed", K(ret), K(*callback));
    } else {
      checksum_last_scn_ = callback->get_scn();
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(target_scn),
                       K_(checksum_scn),
                       K_(checksum_last_scn));
private:
  share::SCN target_scn_;
  share::SCN checksum_scn_;
  ObBatchChecksum *checksumer_;
  share::SCN checksum_last_scn_;
};

} // memtable
} // oceanbase

#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_FUNCTOR

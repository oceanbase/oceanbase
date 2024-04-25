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
namespace transaction
{
class ObCLogEncryptInfo;
}
namespace memtable
{
class ObMutatorWriter;
class ObTxFillRedoCtx;
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
  ObRemoveCallbacksForFastCommitFunctor(const int64_t need_remove_count, const share::SCN &sync_scn)
    : need_remove_count_(need_remove_count),
    sync_scn_(sync_scn),
    last_scn_for_remove_(share::SCN::min_scn()),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL) {}

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    bool is_iter_end = false;

    if (NULL == callback) {
      // case1: the callback is nullptr
      is_iter_end = true;
    } else if (callback->need_submit_log()) {
      // case2: the callback has not been sync successfully
      is_iter_end = true;
    } else if (sync_scn_ < callback->get_scn()) {
      // case3: the callback has not been sync successfully
      is_iter_end = true;
    } else if (callback->get_scn().is_min()) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback scn is min_scn", KPC(callback));
#ifdef ENABLE_DEBUG_LOG
      usleep(5000);
      ob_abort();
#endif
      is_iter_end = true;
    } else if (0 >= need_remove_count_ && callback->get_scn() != last_scn_for_remove_) {
      // case4: the callback has exceeded the last log whose log ts need to be
      //         removed
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }

    return is_iter_end;
  }

  void set_checksumer(const share::SCN checksum_scn,
                      TxChecksum *checksumer)
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
    if (checksumer_ && callback->get_scn() >= checksum_scn_
        && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
      TRANS_LOG(WARN, "calc checksum callback failed", K(ret), K(*callback));
    } else if (OB_FAIL(callback->checkpoint_callback())) {
      TRANS_LOG(ERROR, "row remove callback failed", K(ret), K(*callback));
    } else {
      need_remove_callback_ = true;
      --need_remove_count_;
      last_scn_for_remove_ = callback->get_scn();
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(need_remove_count),
                       KP_(checksumer),
                       K_(sync_scn),
                       K_(checksum_scn),
                       K_(last_scn_for_remove));

private:
  int64_t need_remove_count_;
  share::SCN sync_scn_;
  share::SCN last_scn_for_remove_;
  share::SCN checksum_scn_;
  TxChecksum *checksumer_;
};

// TODO(handora.qc): Adapt to ObRemoveCallbacksWCondFunctor
class ObRemoveSyncCallbacksWCondFunctor : public ObITxCallbackFunctor
{
public:
  ObRemoveSyncCallbacksWCondFunctor(const bool need_remove_data = true,
                                    const bool is_reverse = false)
    : need_checksum_(false),
    need_remove_data_(need_remove_data),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    checksum_last_scn_(share::SCN::min_scn())
    {
      is_reverse_ = is_reverse;
    }
  virtual bool cond_for_remove(ObITransCallback *callback) = 0;
  virtual bool cond_for_stop(ObITransCallback *callback) const
  {
    UNUSED(callback);
    return false;
  };
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
                      TxChecksum *checksumer
                      )
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
      return share::SCN::invalid_scn();
    }
  }

  virtual bool is_iter_end(ObITransCallback *callback) const override
  {
    bool is_iter_end = false;

    if (NULL == callback) {
      // case1: the callback is nullptr
      is_iter_end = true;
    } else if (callback->need_submit_log()) {
      // case2: the callback has not been sync successfully
      is_iter_end = true;
    } else if (share::SCN::max_scn() == callback->get_scn()) {
      // case3: the callback has not been sync successfully
      is_iter_end = true;
    } else if (cond_for_stop(callback)) {
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
    } else if (callback->need_submit_log()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "remove synced will never go here", K(ret), KPC(callback));
    } else if (need_checksum_ && callback->get_scn() >= checksum_scn_ ) {
      if (OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
      TRANS_LOG(WARN, "row remove callback failed", K(ret), K(*callback));
      } else if (FALSE_IT(checksum_last_scn_ = callback->get_scn())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cond_for_remove(callback)) {
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
                       K_(remove_cnt),
                       K_(checksum_scn),
                       K_(checksum_last_scn));

private:
  bool need_checksum_;
  bool need_remove_data_;
  share::SCN checksum_scn_;
  TxChecksum *checksumer_;
  share::SCN checksum_last_scn_;
};

class ObRemoveCallbacksWCondFunctor : public ObITxCallbackFunctor
{
public:
  ObRemoveCallbacksWCondFunctor(const share::SCN right_bound, const bool need_remove_data = true)
    : need_remove_data_(need_remove_data),
    right_bound_(right_bound),
    checksum_scn_(share::SCN::min_scn()),
    checksumer_(NULL),
    checksum_last_scn_(share::SCN::min_scn()) {}
  virtual bool cond_for_remove(ObITransCallback* callback, int &ret) = 0;
  void set_checksumer(const share::SCN checksum_scn,
                      TxChecksum *checksumer)
  {
    checksum_scn_ = checksum_scn;
    checksumer_ = checksumer;
  }

  share::SCN get_checksum_last_scn() const
  {
    if (NULL != checksumer_) {
      return checksum_last_scn_;
    } else {
      return share::SCN::invalid_scn();
    }
  }
  bool is_iter_end(ObITransCallback *callback) const {
    return callback->get_scn() > right_bound_;
  }
  int operator()(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    } else if (callback->need_submit_log()) {
      // Case 1: callback has not been proposed to paxos
      if (cond_for_remove(callback, ret)) {
        if (need_remove_data_ && OB_FAIL(callback->rollback_callback())) {
          TRANS_LOG(WARN, "rollback callback failed", K(ret), K(*callback));
        } else if (!need_remove_data_ && OB_FAIL(callback->checkpoint_callback())) {
          TRANS_LOG(WARN, "checkpoint callback failed", K(ret), K(*callback));
        } else {
          need_remove_callback_ = true;
        }
      } else if (OB_FAIL(ret)) {
        // check ret
      }
    } else if (!callback->need_submit_log()) {
      // Case 2: callback has submitted to log-service may not persistented
      // we check removable in cond_for_remove_ ensure it is synced
      if (cond_for_remove(callback, ret)) {
        if (checksumer_ && callback->get_scn() >= checksum_scn_
            && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
          TRANS_LOG(WARN, "calc checksum callback failed", K(ret), K(*callback));
        } else if (need_remove_data_ && OB_FAIL(callback->rollback_callback())) {
          TRANS_LOG(WARN, "rollback callback failed", K(ret), K(*callback));
        } else if (!need_remove_data_ && OB_FAIL(callback->checkpoint_callback())) {
          TRANS_LOG(WARN, "checkpoint callback failed", K(ret), K(*callback));
        } else {
          need_remove_callback_ = true;
          if (checksumer_) {
            checksum_last_scn_ = callback->get_scn();
          }
        }
      } else if (OB_FAIL(ret)) {
        // check ret
      } else {
        if (checksumer_) {
          if (callback->get_scn() >= checksum_scn_
              && OB_FAIL(callback->calc_checksum(checksum_scn_, checksumer_))) {
            TRANS_LOG(WARN, "row remove callback failed", K(ret), K(*callback));
          } else {
            checksum_last_scn_ = callback->get_scn();
          }
        }
      }
    }

    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(need_remove_data),
                       K_(right_bound),
                       KP_(checksumer),
                       K_(checksum_scn),
                       K_(checksum_last_scn));

private:
  bool need_remove_data_;
  share::SCN right_bound_;
  share::SCN checksum_scn_;
  TxChecksum *checksumer_;
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
    /*   ret = OB_ERR_UNEXPECTED; */
    /*   TRANS_LOG(ERROR, "unexpected callback", KP(callback)); */
    } else if (is_commit_ && callback->get_scn().is_max()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "callback has not submitted log yet when commit callback", K(ret), KP(callback));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
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
  ObCleanUnlogCallbackFunctor(common::ObFunction<void()> &before_remove)
  : before_remove_(&before_remove) {}

  virtual int operator()(ObITransCallback *callback) override
  {
    int ret = OB_SUCCESS;

    if (NULL == callback) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected callback", KP(callback));
    } else if (callback->need_submit_log()) {
      if (before_remove_) {
        before_remove_->operator()();
        before_remove_ = NULL;
      }
      callback->rollback_callback();
      need_remove_callback_ = true;
    }

    return ret;
  }
  common::ObFunction<void()> *before_remove_;
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
    } else if (!callback->need_submit_log()) { // log has been submitted out
      if (OB_FAIL(callback->log_sync_fail_cb(max_committed_scn_))) {
        // log_sync_fail_cb will never report error
        TRANS_LOG(ERROR, "log sync fail cb report error", K(ret));
      } else {
        need_remove_callback_ = true;
      }
    }

    return ret;
  }
  share::SCN max_committed_scn_;
  TO_STRING_KV(K_(max_committed_scn));
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
                      TxChecksum *checksumer)
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
  TxChecksum *checksumer_;
  share::SCN checksum_last_scn_;
};

class ObITxFillRedoFunctor : public ObITxCallbackFunctor
{
public:
  int operator()(ObITransCallback *iter) = 0;
  int64_t get_data_size() const { return data_size_; }
  transaction::ObTxSEQ get_max_seq_no() const { return max_seq_no_; }
  void reset() {
    data_size_ = 0;
    max_seq_no_.reset();
  }
protected:
  int64_t data_size_;
  transaction::ObTxSEQ max_seq_no_;
};

class ObITxCallbackFinder : public ObITxCallbackFunctor {
public:
  ObITxCallbackFinder() : found_(false) {}
  virtual bool match(ObITransCallback *callback) = 0;
  int operator()(ObITransCallback *callback) {
    found_ = match(callback);
    return OB_SUCCESS;
  }
  bool is_iter_end(ObITransCallback *callback) const { return found_; }
  bool is_found() const { return found_; }
private:
  bool found_;
};
} // memtable
} // oceanbase

#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_FUNCTOR

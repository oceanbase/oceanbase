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

#ifndef OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#define OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#include "mvcc/ob_mvcc_trans_ctx.h"
#include "ob_memtable_mutator.h"
#include "ob_memtable_interface.h"

namespace oceanbase
{
namespace memtable
{

// Represents the callbacks in [start_, end_]
struct ObCallbackScope
{
  ObCallbackScope() : start_(nullptr), end_(nullptr), host_(nullptr), cnt_(0), data_size_(0) {}
  ~ObCallbackScope() {}
  void reset()
  {
    start_.reset();
    end_.reset();
    host_ = nullptr;
    cnt_ = 0;
    data_size_ = 0;
  }
  bool is_empty() const { return (nullptr == *start_) || (nullptr == *end_); }
  ObITransCallbackIterator start_;
  ObITransCallbackIterator end_;
  ObTxCallbackList *host_;
  int32_t cnt_;
  int64_t data_size_;
  TO_STRING_KV("start", OB_P(*start_), "end", OB_P(*end_), K_(cnt), K_(data_size), KP_(host));
};
typedef ObIArray<ObCallbackScope> ObCallbackScopeArray;
struct ObRedoLogSubmitHelper
{
  ObRedoLogSubmitHelper() : callbacks_(), max_seq_no_(), data_size_(0), callback_redo_submitted_(true) {}
  ~ObRedoLogSubmitHelper() {}
  void reset()
  {
    callbacks_.reset();
    max_seq_no_.reset();
    data_size_ = 0;
    callback_redo_submitted_ = true;
  }
  ObSEArray<ObCallbackScope, 1> callbacks_; // callbacks in the redo log
  transaction::ObTxSEQ max_seq_no_;
  int64_t data_size_;  // records the data amount of all serialized trans node of this fill process
  share::SCN log_scn_;
  bool callback_redo_submitted_;
};
struct RedoLogEpoch {
  RedoLogEpoch() : v_(0) {}
  RedoLogEpoch(int64_t v): v_(v) {}
  operator int64_t&() { return v_; }
  operator int64_t() const { return v_; }
  DECLARE_TO_STRING
  {
    int64_t pos = 0;
    if (v_ == INT64_MAX) { BUF_PRINTF("MAX"); }
    else { BUF_PRINTF("%ld", v_); }
    return pos;
  }
  int64_t v_;
};

struct ObTxFillRedoCtx
{
  ObTxFillRedoCtx() :
    tx_id_(),
    write_seq_no_(),
    skip_lock_node_(false),
    all_list_(false),
    freeze_clock_(UINT32_MAX),
    list_log_epoch_arr_(),
    cur_epoch_(0),
    next_epoch_(0),
    epoch_from_(0),
    epoch_to_(0),
    list_(NULL),
    list_idx_(-1),
    callback_scope_(NULL),
    buf_(NULL),
    buf_len_(-1),
    buf_pos_(-1),
    helper_(NULL),
    last_log_blocked_memtable_(NULL),
    fill_count_(0),
    is_all_filled_(false),
    fill_time_(0)
  {
    list_log_epoch_arr_.set_max_print_count(256);
  }
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ write_seq_no_; // to select callback list in parallel logging
  bool skip_lock_node_;    // whether skip fill lock node
  bool all_list_;          // whether to fill all callback-list
  uint32_t freeze_clock_;  // memtables before and equals it will be flushed
  ObSEArray<RedoLogEpoch, 1> list_log_epoch_arr_; // record each list's next log epoch
  int64_t cur_epoch_;      // current filling epoch
  int64_t next_epoch_;     // next epoch of list, used to update list_log_epoch_arr_
  int64_t epoch_from_;     // the epoch range to fill
  int64_t epoch_to_;
  ObTxCallbackList *list_; // current filling callback-list
  int list_idx_;           // fill from which list idx
  ObCallbackScope *callback_scope_; // current filling callback scope
  char* buf_;              // the target buffer to fill
  int64_t buf_len_;
  int64_t buf_pos_;
  ObRedoLogSubmitHelper *helper_;
  ObMemtable *last_log_blocked_memtable_;
  int fill_count_;         // number of callbacks was filled
  int fill_round_;         // iter of `choice-list -> fill -> fill others` loop count
  bool is_all_filled_;     // no remains, all callbacks was filled
  int64_t fill_time_;      // time used
public:
  bool is_empty() const { return fill_count_ == 0; }
  bool not_empty() const { return fill_count_ > 0; }
  TO_STRING_KV(K_(tx_id),
               K_(write_seq_no),
               K_(all_list),
               K_(freeze_clock),
               K_(cur_epoch),
               K_(next_epoch),
               K_(epoch_from),
               K_(epoch_to),
               K_(fill_count),
               K_(fill_time),
               KPC_(callback_scope),
               K_(skip_lock_node),
               K_(is_all_filled),
               K_(list_idx),
               K_(list_log_epoch_arr),
               KP_(last_log_blocked_memtable),
               K_(buf_len),
               K_(buf_pos));
};

class ObCallbackListLogGuard
{
public:
  ObCallbackListLogGuard() : lock_ptr_(NULL) {}
  ~ObCallbackListLogGuard() { reset(); }
  void reset() {
    if (lock_ptr_) {
      lock_ptr_->unlock();
      lock_ptr_ = NULL;
    }
  }
  void set(common::ObByteLock *lock) { lock_ptr_ = lock; }
private:
  common::ObByteLock *lock_ptr_;
};

class ObRedoLogGenerator
{
public:
  ObRedoLogGenerator()
      : is_inited_(false),
        redo_filled_cnt_(0),
        redo_sync_succ_cnt_(0),
        redo_sync_fail_cnt_(0),
        callback_mgr_(nullptr),
        mem_ctx_(NULL),
        clog_encrypt_meta_(NULL)
  {}
  ~ObRedoLogGenerator()
  {
    if (clog_encrypt_meta_ != NULL) {
      op_free(clog_encrypt_meta_);
      clog_encrypt_meta_ = NULL;
    }
  }
  void reset();
  void reuse();
  int set(ObTransCallbackMgr *mgr, ObMemtableCtx *mem_ctx);
  int fill_redo_log(ObTxFillRedoCtx &ctx);
  int search_unsubmitted_dup_tablet_redo();
  int log_submitted(const ObCallbackScopeArray &callbacks, const share::SCN &scn);
  int sync_log_succ(const ObCallbackScopeArray &callbacks, const share::SCN &scn);
  void sync_log_fail(const ObCallbackScopeArray &callbacks, const share::SCN &scn);
  void inc_sync_log_fail_cnt(const int cnt)
  {
    redo_sync_fail_cnt_ += cnt;
  }
  int64_t get_redo_filled_count() const { return redo_filled_cnt_; }
  int64_t get_redo_sync_succ_count() const { return redo_sync_succ_cnt_; }
  int64_t get_redo_sync_fail_count() const { return redo_sync_fail_cnt_; }
  void print_first_mvcc_callback();
  bool check_dup_tablet(const ObITransCallback *callback_ptr) const;
private:
  void bug_detect_for_logging_blocked_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObRedoLogGenerator);
  bool is_inited_;
  int64_t redo_filled_cnt_;
  int64_t redo_sync_succ_cnt_;
  int64_t redo_sync_fail_cnt_;
  ObTransCallbackMgr *callback_mgr_;
  ObMemtableCtx *mem_ctx_;
  transaction::ObTxEncryptMeta *clog_encrypt_meta_;

  // logging block bug detector
  int64_t last_logging_blocked_time_;
};

}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_ */


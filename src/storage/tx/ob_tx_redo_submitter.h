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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_REDO_SUBMITTER
#define OCEANBASE_TRANSACTION_OB_TRANS_REDO_SUBMITTER

#include "ob_trans_part_ctx.h"

namespace oceanbase
{
namespace transaction
{
class ObPartTransCtx;

class ObTxRedoSubmitter
{
public:
  ObTxRedoSubmitter(ObPartTransCtx &tx_ctx,
                    memtable::ObMemtableCtx &mt_ctx) :
    tx_ctx_(tx_ctx),
    mt_ctx_(mt_ctx),
    tx_id_(tx_ctx.get_trans_id()),
    ls_id_(tx_ctx.get_ls_id()),
    log_block_(NULL),
    log_cb_(NULL),
    helper_(NULL),
    from_all_list_(false),
    flush_all_(false),
    serial_final_(false),
    submit_if_not_full_(true),
    flush_freeze_clock_(UINT32_MAX),
    write_seq_no_(),
    submit_cb_list_idx_(-1),
    submit_out_cnt_(0),
    submitted_scn_()
  {}
  ~ObTxRedoSubmitter();
  int submit_for_freeze(const uint32_t freeze_clock = UINT32_MAX, const bool display_blocked_info = true) {
    return submit_(true, freeze_clock, false, display_blocked_info);
  }
  int serial_submit(const bool is_final) {
    return submit_(false, UINT32_MAX, is_final, false);
  }
  int submit_all(const bool display_blocked_info) {
    return submit_(true, UINT32_MAX, false, display_blocked_info);
  }
  // fill log_block, and if full submit out and continue to fill
  int fill(ObTxLogBlock &log_block, memtable::ObRedoLogSubmitHelper &helper, const bool display_blocked_info = true);
  // parallel submit, only traversal writter's callback-list
  int parallel_submit(const ObTxSEQ &write_seq);
  int get_submitted_cnt() const { return submit_out_cnt_; }
  share::SCN get_submitted_scn() const { return submitted_scn_; }
private:
  // general submit entry, will traversal all callback-list
  int submit_(const bool flush_all, const uint32_t freeze_clock, const bool is_final, const bool display_blocked_info);
private:
  // common submit redo pipeline
  // prepare -> fill -> submit_out -> after_submit
  //   ^                                  |
  //   |                                  |
  //   \----------<--next-round-----------/
  // the loop will breaks in cases of:
  // 1. no data need to be logged, all TxNode is logged
  // 2. reach a blocking TxNode whose memtable is blocked to log
  // 3. big row, the log_block can not hold the row
  // 4. exception: memory allocated failure
  int _submit_redo_pipeline_(const bool display_blocked_info);
  int prepare_();
  int fill_log_block_(memtable::ObTxFillRedoCtx &ctx);
  int submit_log_block_out_(const int64_t replay_hint, bool &submitted);
  int after_submit_redo_out_();
public:
  TO_STRING_KV(K_(tx_id),
               K_(ls_id),
               K_(from_all_list),
               K_(flush_all),
               K_(flush_freeze_clock),
               K_(write_seq_no),
               K_(serial_final),
               K_(submit_if_not_full),
               K_(submit_out_cnt),
               K_(submit_cb_list_idx));
private:
  ObPartTransCtx &tx_ctx_;
  memtable::ObMemtableCtx &mt_ctx_;
  const ObTransID tx_id_;
  const share::ObLSID ls_id_;
  ObTxLogBlock *log_block_;
  ObTxLogCb *log_cb_;
  memtable::ObRedoLogSubmitHelper *helper_;
  // for writer thread submit, only submit single list
  // for freeze or switch leader or commit, submit from all list
  bool from_all_list_ : 1;
  // whether flush all logs before can return
  bool flush_all_ : 1;
  // whether is submitting the final serial log
  bool serial_final_ : 1;
  // wheter submit out if log_block is not full filled
  bool submit_if_not_full_ : 1;
  // flush memtables before and equals specified freeze_clock
  int64_t flush_freeze_clock_;
  // writer seq_no, used to select logging callback-list
  ObTxSEQ write_seq_no_;
  // submit from which list, use by wirte thread logging
  int submit_cb_list_idx_;
  // the count of logs this submitter submitted out
  int submit_out_cnt_;
  // last submitted log scn
  share::SCN submitted_scn_;
};

#define FLUSH_REDO_TRACE_LEVEL DEBUG
#define FLUSH_REDO_TRACE(fmt, ...) TRANS_LOG(FLUSH_REDO_TRACE_LEVEL, "[REDO FLUSH]" fmt, K(ret), KPC(this), ## __VA_ARGS__);

ObTxRedoSubmitter::~ObTxRedoSubmitter()
{
  if (log_cb_) {
    tx_ctx_.return_redo_log_cb(log_cb_);
    log_cb_ = NULL;
  }
}

// use by:
// flush redo after mvcc_write when txn has switched to parallel_logging
//
// the caller hold TxCtx's FlushRedo read Lock
int ObTxRedoSubmitter::parallel_submit(const ObTxSEQ &write_seq_no)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  from_all_list_ = false;
  flush_all_ = false;
  write_seq_no_ = write_seq_no;
  ObTxLogBlock log_block;
  log_block_ = &log_block;
  memtable::ObRedoLogSubmitHelper helper;
  helper_ = &helper;
  submit_if_not_full_ = true;
  bool do_submit = false;
  memtable::ObCallbackListLogGuard log_lock_guard;
  submit_cb_list_idx_ = -1;
  if (OB_FAIL(mt_ctx_.get_log_guard(write_seq_no, log_lock_guard, submit_cb_list_idx_))) {
    if (OB_NEED_RETRY == ret) {
      // lock conflict
    } else if (OB_BLOCK_FROZEN == ret) {
      // memtable is logging blocked
    } else if (OB_EAGAIN == ret) {
      // others need flush firstly
      if (TC_REACH_TIME_INTERVAL(5_s)) {
        TRANS_LOG(WARN, "blocked by other list has smaller wirte epoch unlogged",
                  K(write_seq_no), K(tx_ctx_.get_trans_id()));
      }
      if (submit_cb_list_idx_ >= 0) {
        // try to submit blocking list
        save_ret = OB_EAGAIN;
        do_submit = true;
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // no callback to log
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get log lock guard fail", K(ret));
    }
  } else {
    do_submit = true;
  }
  if (do_submit) {
    ret = _submit_redo_pipeline_(false);
    ret = save_ret != OB_SUCCESS ? save_ret : ret;
  }
  return ret;
}

// used by:
// - flush redo after mvcc_write when Txn has not switched to parallel_logging
// - freeze submit redo
// - when parallel logging, at switch to follower or commit time flush all redos
//
// the caller has hold TransCtx's FlushRedo write Lock
// which ensure no writer thread is logging
// and also hold TransCtx's CtxLock, which is safe to operate in the flush pipline
int ObTxRedoSubmitter::submit_(const bool flush_all,
                               const uint32_t freeze_clock,
                               const bool is_final,
                               const bool display_blocked_info)
{
  int ret = OB_SUCCESS;
  from_all_list_ = true;
  flush_all_ = flush_all;
  flush_freeze_clock_ = freeze_clock;
  serial_final_ = is_final;
  ObTxLogBlock log_block;
  log_block_ = &log_block;
  memtable::ObRedoLogSubmitHelper helper;
  helper_ = &helper;
  submit_if_not_full_ = true;
  ret = _submit_redo_pipeline_(display_blocked_info);
  FLUSH_REDO_TRACE("serial submit done");
  return ret;
}

// used by:
// - switch to follower when parallel logging is not enabled for this txn
// - commit time flush redos when parallel logging is not enabled for this txn
//
// the caller has hold TxCtx's CtxLock
// the caller has hold TxCtx's FlushRedo write Lock
int ObTxRedoSubmitter::fill(ObTxLogBlock &block,
                            memtable::ObRedoLogSubmitHelper &helper,
                            const bool display_blocked_info)
{
  int ret = OB_SUCCESS;
  from_all_list_ = true;
  flush_all_ = true;
  log_block_ = &block;
  helper_ = &helper;
  submit_if_not_full_ = false;
  ret = _submit_redo_pipeline_(display_blocked_info);
  FLUSH_REDO_TRACE("serial submit and fill done");
  return ret;
}

int ObTxRedoSubmitter::_submit_redo_pipeline_(const bool display_blocked_info)
{
  int ret = OB_SUCCESS;
  memtable::ObTxFillRedoCtx ctx;
  ctx.tx_id_ = tx_id_;
  ctx.write_seq_no_ = write_seq_no_;
  const bool is_parallel_logging = tx_ctx_.is_parallel_logging();
  bool stop = false;
  int fill_ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !stop) {
    if (submit_if_not_full_ && OB_FAIL(prepare_())) {
      if (OB_TX_NOLOGCB != ret) {
        TRANS_LOG(WARN, "prepare for submit log fail", K(ret));
      }
    } else {
      bool skip_submit = false;
      fill_ret = fill_log_block_(ctx);
      if (ctx.fill_count_ <= 0) {
        // no data to flush, or no data can be flushed
        ret = fill_ret;
        stop = true;
        skip_submit = true;
      } else if (OB_SUCCESS == fill_ret) {
        // this means all redo filled, no remians
        ret = fill_ret;
        stop = true;
        // this is just fill log block, if all is filled
        // won't submit out
        skip_submit = !submit_if_not_full_;
      } else if (OB_BUF_NOT_ENOUGH == fill_ret) {
        if (ctx.fill_count_ == 0) {
          // BIG_ROW has been handled in `fill_log_block_`
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "should not reach here", K(ret), K(ctx), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
          break;
        } else {
          fill_ret = OB_EAGAIN;
        }
      } else if (OB_BLOCK_FROZEN == fill_ret) {
        if (is_parallel_logging) {
          // should flush and continue retry other list
        } else {
          // serial logging, retry will failed with BLOCK_FROZEN also
          stop = true;
          ret = fill_ret;
        }
      } else if (OB_ITER_END == fill_ret) {
        // blocked by other list, current list remains
        if (is_parallel_logging) {
          // should flush and retry others
        } else {
          // serial logging, shouldn't reach here
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "oops! fatal panic", K(ret), K(ctx), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
          break;
        }
      } else if (OB_EAGAIN == fill_ret) {
        // this list is all filled, but others remains
        if (is_parallel_logging) {
          // should flush and retry others
        } else {
          // serial logging, shouldn't reach here
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "oops! fatal panic", K(ret), K(ctx), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          usleep(1000);
          ob_abort();
#endif
          break;
        }
      } else {
        stop = true;
        ret = fill_ret;
      }

      // start submit out filled log block
      bool submitted = false;
      int submit_ret = OB_SUCCESS;
      if (ctx.fill_count_ > 0 && !skip_submit) {
        const int64_t replay_hint = ctx.tx_id_.get_id() + (is_parallel_logging ? ctx.list_idx_ : 0);
        submit_ret = submit_log_block_out_(replay_hint, submitted);
        if (OB_SUCCESS == submit_ret) {
          submit_ret = after_submit_redo_out_();
        }
      }
      if (stop) {
        ret = submit_ret == OB_SUCCESS ? ret : submit_ret;
      } else {
        if (serial_final_ && submitted) {
          stop = true;
        } else if (!flush_all_) {
          stop = true;
        }
        if (stop) {
          // return fill_ret if submit success
          ret = (submit_ret == OB_SUCCESS) ? fill_ret : submit_ret;
        } else {
          // if submit failed, should stop
          ret = submit_ret;
        }
      }
    }
  }
  if (OB_UNLIKELY(display_blocked_info) && fill_ret == OB_BLOCK_FROZEN) {
    if (TC_REACH_TIME_INTERVAL(5_s)) {
      TRANS_LOG(INFO, "[display-blocked-info]", "submit_redo_ctx", ctx);
    }
  }
  FLUSH_REDO_TRACE("submit pipeline done", K(ctx), K_(tx_ctx));
  return ret;
}

int ObTxRedoSubmitter::submit_log_block_out_(const int64_t replay_hint, bool &submitted)
{
  int ret = OB_SUCCESS;
  submitted = false;
  if (OB_NOT_NULL(log_cb_) && OB_FAIL(log_cb_->reserve_callbacks(helper_->callbacks_.count()))) {
    TRANS_LOG(WARN, "log cb reserve callbacks space fail", K(ret));
  } else {
    share::SCN submitted_scn;
    helper_->callback_redo_submitted_ = false;
    bool has_hold_ctx_lock = from_all_list_;
    ret = tx_ctx_.submit_redo_log_out(*log_block_, log_cb_, *helper_, replay_hint, has_hold_ctx_lock, submitted_scn);
    OB_ASSERT(log_cb_ == NULL);
    if (submitted_scn.is_valid()) {
      submitted = true;
      ++submit_out_cnt_;
      submitted_scn_ = submitted_scn;
    }
  }
  FLUSH_REDO_TRACE("after submit redo log out", K(replay_hint), K(submitted));
  return ret;
}

int ObTxRedoSubmitter::after_submit_redo_out_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mt_ctx_.log_submitted(*helper_))) {
    TRANS_LOG(WARN, "callback memctx fail", K(ret));
#ifdef ENABLE_DEBUG_LOG
    usleep(1000);
    ob_abort();
#endif
  }
  helper_->reset();
  return ret;
}

// allocate/reserve resource for `after_submit_log_out_`
int ObTxRedoSubmitter::prepare_()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(log_cb_)) {
    ret = OB_ERR_UNEXPECTED;
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  } else {
    ret = tx_ctx_.prepare_for_submit_redo(log_cb_, *log_block_, serial_final_);
  }
  return ret;
}

int ObTxRedoSubmitter::fill_log_block_(memtable::ObTxFillRedoCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  do {
    need_retry = false;
    ObTxRedoLog log(tx_ctx_.get_cluster_version());
    ret = log_block_->prepare_mutator_buf(log);
    ctx.buf_ = log.get_mutator_buf();
    ctx.buf_len_ = log.get_mutator_size();
    ctx.buf_pos_ = 0;
    ctx.helper_ = helper_;
    ctx.skip_lock_node_ = false;
    ctx.all_list_ = from_all_list_;
    ctx.freeze_clock_ = flush_freeze_clock_;
    ctx.fill_count_ = 0;
    ctx.list_idx_ = submit_cb_list_idx_;
    int64_t start_ts = ObTimeUtility::fast_current_time();
    ret = mt_ctx_.fill_redo_log(ctx);
    ctx.fill_time_ = ObTimeUtility::fast_current_time() - start_ts;
    int save_ret = ret;
    int64_t real_buf_pos = ctx.fill_count_ > 0 ? ctx.buf_pos_ : 0;
    if (OB_FAIL(log_block_->finish_mutator_buf(log, real_buf_pos))) {
    } else if (OB_UNLIKELY(real_buf_pos == 0 && (OB_BUF_NOT_ENOUGH == save_ret))) {
      // extend log_block for big row
      if (OB_FAIL(log_block_->extend_log_buf())) {
        TRANS_LOG(WARN, "extend log buffer failed", K(ret), K(ctx), KPC_(log_block), KPC(this));
        if (OB_ALLOCATE_MEMORY_FAILED != ret) {
          ret = OB_ERR_TOO_BIG_ROWSIZE;
        }
      } else {
        need_retry = true;
        TRANS_LOG(INFO, "extend log buffer success", K(ctx.buf_pos_), KPC(this));
      }
    } else if (real_buf_pos > 0 && OB_FAIL(log_block_->add_new_log(log))) {
    } else {
      ret = save_ret;
    }
  } while(need_retry);
  FLUSH_REDO_TRACE("after fill redo", K(ctx));
  return ret;
}
} // transaction
} // oceanbase
#undef FLUSH_REDO_LOG_TRACE
#endif // OCEANBASE_TRANSACTION_OB_TRANS_REDO_SUBMITTER

/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TX_LOG_OPERATOR_HEADER
#define OCEANBASE_TX_LOG_OPERATOR_HEADER

#include "storage/ls/ob_ls_ddl_log_handler.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log.h"

namespace oceanbase
{
namespace transaction
{

/****************************************
 * Public TxLog Operator Template
 * Begin
 ****************************************/
enum class ObTxLogOpType
{
  SUBMIT,
  APPLY_SUCC,
  APPLY_FAIL,
  REPLAY
};

template <typename T>
class ObTxCtxLogOperator
{
private:
  int construct_log_object_();

  // submit log function
  int prepare_generic_resource_();
  int prepare_special_resource_();
  int submit_prev_log_();
  int insert_into_log_block_();
  int pre_check_for_log_submiting_();
  int submit_log_block_out_();
  int common_submit_log_succ_();
  void after_submit_log_succ_();
  void after_submit_log_fail_(const int submit_ret);
  int log_sync_succ_()
  {
    int ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "empty function of log_sync_succ", K(ret), KPC(this));
    return ret;
  }
  int log_sync_fail_()
  {
    int ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "empty function of log_sync_fail", K(ret), KPC(this));
    return ret;
  }

  // replay log function
  int deserialize_log_()
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(construct_log_object_())) {
      TRANS_LOG(WARN, "construct log object failed", K(ret), KPC(this));
    } else if (OB_FAIL(log_block_->deserialize_log_body(*log_object_ptr_))) {
      TRANS_LOG(WARN, "deserialize log body failed", K(ret), KPC(this));
    }
    return ret;
  }
  int replay_in_ctx_()
  {
    int ret = OB_SUCCESS;

    return ret;
  }
  int replay_out_ctx_()
  {
    int ret = OB_SUCCESS;

    return ret;
  }
  int replay_fail_out_ctx_()
  {
    int ret = OB_SUCCESS;

    return ret;
  }
  int replay_log_()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(deserialize_log_())) {
      TRANS_LOG(WARN, "deserialize the log failed", K(ret), K(tx_ctx_->trans_id_),
                K(tx_ctx_->ls_id_), KPC(this));
    } else if (OB_FAIL(replay_out_ctx_())) {
      TRANS_LOG(WARN, "replay log out ctx failed", K(ret), K(tx_ctx_->trans_id_),
                K(tx_ctx_->ls_id_), KPC(this));
    } else if (OB_FAIL(replay_in_ctx_())) {
      TRANS_LOG(WARN, "replay log in ctx failed", K(ret), K(tx_ctx_->trans_id_), K(tx_ctx_->ls_id_),
                KPC(this));
    }

    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(replay_fail_out_ctx_())) {
        TRANS_LOG(ERROR, "an error occurred while handling replay failure outside of the tx_ctx ",
                  K(tmp_ret), K(ret), KPC(this));
      }
    }
    return ret;
  }

public:
  // submit
  ObTxCtxLogOperator(ObPartTransCtx *tx_ctx_ptr,
                     ObTxLogBlock *log_block_ptr,
                     typename T::ConstructArg *construct_arg,
                     const typename T::SubmitArg &submit_arg)
      : tx_ctx_(tx_ctx_ptr), log_object_ptr_(nullptr), log_block_(log_block_ptr),
        construct_arg_(construct_arg), scn_(), lsn_()
  {
    log_op_arg_.submit_arg_.reset();
    log_op_arg_.submit_arg_ = submit_arg;
  };

  // apply
  ObTxCtxLogOperator(ObPartTransCtx *tx_ctx_ptr, ObTxLogCb *log_cb_ptr)
      : tx_ctx_(tx_ctx_ptr), log_object_ptr_(nullptr), log_block_(nullptr), construct_arg_(nullptr),
        scn_(log_cb_ptr->get_log_ts()), lsn_(log_cb_ptr->get_lsn())
  {
    log_op_arg_.submit_arg_.reset();
    log_op_arg_.submit_arg_.log_cb_ = log_cb_ptr;
  };

  // replay
  ObTxCtxLogOperator(ObPartTransCtx *tx_ctx_ptr,
                     ObTxLogBlock *log_block_ptr,
                     typename T::ConstructArg *construct_arg,
                     const typename T::ReplayArg &replay_arg,
                     const share::SCN scn,
                     const palf::LSN lsn)
      : tx_ctx_(tx_ctx_ptr), log_object_ptr_(nullptr), log_block_(log_block_ptr),
        construct_arg_(construct_arg), scn_(scn), lsn_(lsn)
  {
    log_op_arg_.replay_arg_.reset();
    log_op_arg_.replay_arg_ = replay_arg;
  };

  ~ObTxCtxLogOperator()
  {
    if (OB_NOT_NULL(log_object_ptr_)) {
      log_object_ptr_->~T();
    }
  };

  int operator()(const ObTxLogOpType op_type);

  TO_STRING_KV(KPC(construct_arg_), KPC(tx_ctx_), K(scn_), K(lsn_), KPC(log_block_));

public:
  const share::SCN &get_scn() { return scn_; }
  const palf::LSN &get_lsn() { return lsn_; }

private:
  ObPartTransCtx *tx_ctx_;
  char log_object_memory_[sizeof(T)];
  T *log_object_ptr_;
  ObTxLogBlock *log_block_;
  typename T::ConstructArg *construct_arg_;

  share::SCN scn_;
  palf::LSN lsn_;

  union LogOpArg
  {
    typename T::SubmitArg submit_arg_;
    typename T::ReplayArg replay_arg_;

    LogOpArg(){};
    ~LogOpArg(){};

  } log_op_arg_;
  // bool retain_in_memory_; TODO: retain prev log in log_block
};

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::prepare_generic_resource_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_ctx_) || OB_ISNULL(log_block_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), KPC(this));
  } else if (!log_block_->is_inited()) {
    {
      CtxLockGuard guard;
      if (!tx_ctx_->lock_.is_locked_by_self()) {
        tx_ctx_->get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
      }

      // From 4.3, we must init the cluster_version_ of the log block header before init the log
      // block.
      // the log_entry_no will be backfill before log-block to be submitted
      log_block_->get_header().init(tx_ctx_->cluster_id_, tx_ctx_->cluster_version_,
                                    INT64_MAX /*log_entry_no*/, tx_ctx_->trans_id_,
                                    tx_ctx_->exec_info_.scheduler_);
    }
    log_op_arg_.submit_arg_.suggested_buf_size_ = log_op_arg_.submit_arg_.suggested_buf_size_ <= 0
                                                      ? ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE
                                                      : log_op_arg_.submit_arg_.suggested_buf_size_;
    if (OB_FAIL(log_block_->init_for_fill(log_op_arg_.submit_arg_.suggested_buf_size_))) {
      TRANS_LOG(WARN, "init log block for fill failed", K(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tx_ctx_->prepare_log_cb_(false, log_op_arg_.submit_arg_.log_cb_))) {
    if (OB_UNLIKELY(OB_TX_NOLOGCB != ret)) {
      TRANS_LOG(WARN, "get log cb failed", K(ret), KPC(this));
    }
  } else if (OB_FAIL(tx_ctx_->acquire_ctx_ref_())) {
    TRANS_LOG(ERROR, "acquire ctx ref failed", KR(ret), K(tx_ctx_->trans_id_), K(tx_ctx_->ls_id_),
              KPC(this));
  } else if (OB_FALSE_IT(log_op_arg_.submit_arg_.hold_tx_ctx_ref_ = true)) {
    // do nothing
  }

  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::prepare_special_resource_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::submit_prev_log_()
{
  int ret = OB_SUCCESS;
  // do nothing
  // TODO submit memtable redo, mds redo, dlc redo, commit info before the prepare
  // T::LOG_TYPE
  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::construct_log_object_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(construct_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid construct arg pointer", K(ret), KPC(construct_arg_));
  } else {
    memset(log_object_memory_, 0, sizeof(T));
    new (log_object_memory_) T(*construct_arg_);
    log_object_ptr_ = (T *)(log_object_memory_);
  }
  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::insert_into_log_block_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(log_block_->add_new_log(*log_object_ptr_))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      TRANS_LOG(WARN, "add new log failed", KR(ret), KPC(this));
    } else {
      TRANS_LOG(DEBUG, "the buffer is not enough in log_block", K(ret), K(tx_ctx_->trans_id_),
                K(tx_ctx_->ls_id_), KPC(this));
    }
  }

  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::pre_check_for_log_submiting_()
{
  int ret = OB_SUCCESS;

  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::submit_log_block_out_()
{
  int ret = OB_SUCCESS;
  bool is_2pc_state_log = false;

  if (tx_ctx_->is_exiting()) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "the tx ctx is exiting", K(ret), K(T::LOG_TYPE), KPC(tx_ctx_));
  } else if (tx_ctx_->is_force_abort_logging_()
             || tx_ctx_->get_downstream_state() == ObTxState::ABORT) {
    ret = OB_TRANS_KILLED;
    TRANS_LOG(WARN, "tx has been aborting, can not submit log", K(ret), K(T::LOG_TYPE),
              KPC(tx_ctx_));
  } else if (tx_ctx_->is_follower_()) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "we can not submit a tx log on the follower", K(ret), K(T::LOG_TYPE),
              KPC(tx_ctx_));
  } else if (tx_ctx_->exec_info_.data_complete_
             && tx_ctx_->start_working_log_ts_ > tx_ctx_->exec_info_.max_applying_log_ts_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN,
              "There exists a data completed transaction whose start_working_log_ts_ is greater "
              "than any of its log_ts",
              K(ret), K(T::LOG_TYPE), KPC(tx_ctx_));
    tx_ctx_->print_trace_log_();
  } else if (ObTxLogTypeChecker::is_data_log(T::LOG_TYPE)
             && tx_ctx_->get_downstream_state() >= ObTxState::REDO_COMPLETE) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "the data log can not be submitted after the commit info log", K(ret),
              K(T::LOG_TYPE), KPC(tx_ctx_));
  } else if (OB_UNLIKELY(tx_ctx_->is_2pc_blocking())) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "tx submit log failed because of 2pc blocking", K(ret), KPC(this));
    // It is safe to merge the intermediate_participants because we will block
    // the persistent state machine with is_2pc_blocking. The detailed design
    // can be found in the implementation of the merge_intermediate_participants.
  } else if (is_contain_stat_log(log_block_->get_cb_arg_array())
             && FALSE_IT(is_2pc_state_log = true)) {
  } else if (is_2pc_state_log && OB_FAIL(tx_ctx_->merge_intermediate_participants())) {
    TRANS_LOG(WARN, "fail to merge intermediate participants", K(ret), KPC(this));
  } else {
    const int64_t real_replay_hint = log_op_arg_.submit_arg_.replay_hint_ > 0
                                         ? log_op_arg_.submit_arg_.replay_hint_
                                         : tx_ctx_->trans_id_.get_id();
    log_block_->get_header().set_log_entry_no(tx_ctx_->exec_info_.next_log_entry_no_);
    if (OB_FAIL(log_block_->seal(real_replay_hint, log_op_arg_.submit_arg_.replay_barrier_type_))) {
      TRANS_LOG(WARN, "seal log block fail", K(ret));
    } else if (OB_SUCC(tx_ctx_->ls_tx_ctx_mgr_->get_ls_log_adapter()->submit_log(
                   log_block_->get_buf(), log_block_->get_size(), log_op_arg_.submit_arg_.base_scn_,
                   log_op_arg_.submit_arg_.log_cb_, false))) {
      tx_ctx_->busy_cbs_.add_last(log_op_arg_.submit_arg_.log_cb_);
      scn_ = log_op_arg_.submit_arg_.log_cb_->get_log_ts();
      lsn_ = log_op_arg_.submit_arg_.log_cb_->get_lsn();
    }
  }

  return ret;
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::common_submit_log_succ_()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && OB_FAIL(tx_ctx_->update_rec_log_ts_(false /*for_replay*/, share::SCN::invalid_scn()))) {
    TRANS_LOG(WARN, "update rec log ts failed", KR(ret), KPC(log_op_arg_.submit_arg_.log_cb_),
              K(*this));
  }
  if (OB_SUCC(ret)) {
    const ObTxCbArgArray &cb_arg_array = log_block_->get_cb_arg_array();
    if (cb_arg_array.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cb arg array is empty", K(ret), K(*this));
    } else if (OB_FAIL(log_op_arg_.submit_arg_.log_cb_->get_cb_arg_array().assign(cb_arg_array))) {
      TRANS_LOG(WARN, "assign cb arg array failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tx_ctx_->ctx_tx_data_.get_start_log_ts().is_valid()) {
      if (OB_FAIL(tx_ctx_->ctx_tx_data_.set_start_log_ts(
              log_op_arg_.submit_arg_.log_cb_->get_log_ts()))) {
        TRANS_LOG(WARN, "set tx data start log ts failed", K(ret), K(tx_ctx_->ctx_tx_data_));
      }
    }
  }
  return ret;
}

template <typename T>
OB_INLINE void ObTxCtxLogOperator<T>::after_submit_log_succ_()
{}

template <typename T>
OB_INLINE void ObTxCtxLogOperator<T>::after_submit_log_fail_(const int submit_ret)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_BUF_NOT_ENOUGH == submit_ret) {
    // do nothing
    // TODO submit prev log block with BUF_NOT_ENOUGH. It will reuse the generic_resource and
    // rewrite err_ret.
    tmp_ret = OB_NOT_SUPPORTED;
  }

  if (submit_ret != OB_BUF_NOT_ENOUGH || tmp_ret != OB_SUCCESS) {
    if (OB_NOT_NULL(log_op_arg_.submit_arg_.log_cb_)) {
      if (log_op_arg_.submit_arg_.log_cb_->get_prev() != nullptr
          || log_op_arg_.submit_arg_.log_cb_->get_next() != nullptr) {
        TRANS_LOG(ERROR, "the log cb is not alone", K(submit_ret), K(tmp_ret),
                  K(tx_ctx_->get_trans_id()), K(tx_ctx_->get_ls_id()),
                  KPC(log_op_arg_.submit_arg_.log_cb_), K(T::LOG_TYPE), KPC(this));
      }
      if (OB_TMP_FAIL(tx_ctx_->return_log_cb_(log_op_arg_.submit_arg_.log_cb_))) {
        TRANS_LOG(ERROR, "free the log cb failed", K(submit_ret), K(tmp_ret),
                  K(tx_ctx_->get_trans_id()), K(tx_ctx_->get_ls_id()),
                  KPC(log_op_arg_.submit_arg_.log_cb_), K(T::LOG_TYPE), KPC(this));
      }
    }
    if (log_op_arg_.submit_arg_.hold_tx_ctx_ref_) {
      tx_ctx_->release_ctx_ref_();
    }
    log_block_->reset();
  }
}

template <typename T>
OB_INLINE int ObTxCtxLogOperator<T>::operator()(const ObTxLogOpType op_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (op_type == ObTxLogOpType::REPLAY) {
    if (OB_FAIL(replay_log_())) {
      TRANS_LOG(WARN, "replay log failed", K(ret), K(T::LOG_TYPE), KPC(this));
    }
  } else if (op_type == ObTxLogOpType::SUBMIT) {
    if (OB_FAIL(prepare_special_resource_())) {
      TRANS_LOG(WARN, "prepare special resource failed", K(ret), K(T::LOG_TYPE), KPC(this));
    } else if (OB_FAIL(prepare_generic_resource_())) {
      if (OB_TX_NOLOGCB != ret) {
        TRANS_LOG(WARN, "prepare generic resource failed", K(ret), K(T::LOG_TYPE), KPC(this));
      }
    } else if (OB_FAIL(construct_log_object_())) {
      TRANS_LOG(WARN, "construct log object failed", K(ret), K(T::LOG_TYPE), KPC(this));
    } else if (OB_FAIL(insert_into_log_block_())) {
      TRANS_LOG(WARN, "insert tx log into log block failed", K(ret), K(T::LOG_TYPE), KPC(this));
    } else {
      CtxLockGuard guard;
      if (!tx_ctx_->lock_.is_locked_by_self()) {
        tx_ctx_->get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
      }

      // TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> prepare to submit log",K(ret),KPC(this));
      if (OB_FAIL(pre_check_for_log_submiting_())) {
        TRANS_LOG(WARN, "pre check for log submitting",K(ret), K(T::LOG_TYPE), KPC(this));
      } else if (OB_FAIL(submit_log_block_out_())) {
        TRANS_LOG(WARN, "submit tx log block into palf failed", K(ret), K(T::LOG_TYPE), KPC(this));
      } else if (OB_TMP_FAIL(common_submit_log_succ_())) {
        TRANS_LOG(WARN, "common after_submit_log_succ failed", K(ret), K(T::LOG_TYPE), KPC(this));
      } else {
        (void)after_submit_log_succ_();
        tx_ctx_->exec_info_.next_log_entry_no_++;
        tx_ctx_->reuse_log_block_(*log_block_);
      }
    }

    if (OB_FAIL(ret)) {
      (void)after_submit_log_fail_(ret);
    }

  } else if (op_type == ObTxLogOpType::APPLY_SUCC) {

    if (OB_FAIL(log_sync_succ_())) {
      TRANS_LOG(ERROR, "invoke on_success for tx_log failed", K(ret), K(T::LOG_TYPE), KPC(this));
    }

  } else if (op_type == ObTxLogOpType::APPLY_FAIL) {

    if (OB_FAIL(log_sync_fail_())) {
      TRANS_LOG(WARN, "invoke on_failure for tx_log failed", K(ret), K(T::LOG_TYPE), KPC(this));
    }

  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid tx log op type", K(ret), K(op_type), K(T::LOG_TYPE), KPC(this));
  }
  return ret;
}

/****************************************
 * Public TxLog Operator Template
 * End
 ****************************************/

/****************************************
 * Direct Load Inc TxLog Operator
 * Begin
 ****************************************/

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::prepare_special_resource_()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard;
  if (!tx_ctx_->lock_.is_locked_by_self()) {
    tx_ctx_->get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  if (OB_ISNULL(log_op_arg_.submit_arg_.extra_cb_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid extra_cb", K(ret), KPC(construct_arg_), K(log_op_arg_.submit_arg_));
  } else if (OB_FAIL(tx_ctx_->exec_info_.redo_lsns_.reserve(tx_ctx_->exec_info_.redo_lsns_.count()
                                                            + 1))) {
    TRANS_LOG(WARN, "reserve memory for redo lsn failed", K(ret));
  }

  return ret;
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::pre_check_for_log_submiting_()
{
  int ret = OB_SUCCESS;

  if (construct_arg_->ddl_log_type_ == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START
      && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.before_submit_ddl_start(
             construct_arg_->batch_key_))) {
    TRANS_LOG(WARN, "register ddl_start key failed", K(ret), KPC(construct_arg_), KPC(this));
  } else if (construct_arg_->ddl_log_type_ == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END
             && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.before_submit_ddl_end(
                    construct_arg_->batch_key_))) {
    TRANS_LOG(WARN, "register ddl_end key failed", K(ret), KPC(construct_arg_), KPC(this));
  }
  return ret;
}

template <>
OB_INLINE void ObTxCtxLogOperator<ObTxDirectLoadIncLog>::after_submit_log_succ_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(
          tx_ctx_->exec_info_.redo_lsns_.push_back(log_op_arg_.submit_arg_.log_cb_->get_lsn()))) {
    TRANS_LOG(WARN, "push back redo lsns failed", K(ret), KPC(this),
              KPC(log_op_arg_.submit_arg_.log_cb_));
  } else if (construct_arg_->ddl_log_type_ == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START
             && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.submit_ddl_start_succ(
                    construct_arg_->batch_key_, scn_))) {
    TRANS_LOG(WARN, "update ddl_start scn after submit_log failed", K(ret), KPC(construct_arg_),
              KPC(this));
  } else if (construct_arg_->ddl_log_type_ == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END
             && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.submit_ddl_end_succ(
                    construct_arg_->batch_key_, scn_))) {
    TRANS_LOG(WARN, "update ddl_end key after submit_log failed", K(ret), KPC(construct_arg_),
              KPC(this));

  } else {
    log_op_arg_.submit_arg_.log_cb_->set_extra_cb(log_op_arg_.submit_arg_.extra_cb_);
    if (log_op_arg_.submit_arg_.need_free_extra_cb_) {
      log_op_arg_.submit_arg_.log_cb_->set_need_free_extra_cb();
    }
    log_op_arg_.submit_arg_.log_cb_->set_extra_cb(log_op_arg_.submit_arg_.extra_cb_);
    log_op_arg_.submit_arg_.log_cb_->set_ddl_log_type(construct_arg_->ddl_log_type_);
    log_op_arg_.submit_arg_.log_cb_->set_ddl_batch_key(construct_arg_->batch_key_);
    log_op_arg_.submit_arg_.log_cb_->get_extra_cb()->__set_scn(scn_);
  }
  TRANS_LOG(DEBUG, "<ObTxDirectLoadIncLog> after submit log succ", K(ret), KPC(this));
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::log_sync_succ_()
{
  int ret = OB_SUCCESS;

  const ObTxDirectLoadIncLog::DirectLoadIncLogType ddl_log_type =
      log_op_arg_.submit_arg_.log_cb_->get_ddl_log_type();
  const storage::ObDDLIncLogBasic batch_key = log_op_arg_.submit_arg_.log_cb_->get_batch_key();
  const share::SCN scn = log_op_arg_.submit_arg_.log_cb_->get_log_ts();

  if (OB_ISNULL(log_op_arg_.submit_arg_.log_cb_)
      || OB_ISNULL(log_op_arg_.submit_arg_.log_cb_->get_extra_cb())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid log_cb or extra_cb", K(ret), K(log_op_arg_.submit_arg_));
  } else if (ddl_log_type == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START
             && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_start_succ(batch_key, scn))) {
    TRANS_LOG(WARN, "update ddl_start key after log_sync_succ failed", K(ret),
              KPC(log_op_arg_.submit_arg_.log_cb_));
  } else if (ddl_log_type == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END
             && OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_end_succ(batch_key, scn))) {
    TRANS_LOG(WARN, "update ddl_end key after log_sync_succ failed", K(ret),
              KPC(log_op_arg_.submit_arg_.log_cb_));

  } else if (OB_FAIL(log_op_arg_.submit_arg_.log_cb_->get_extra_cb()->on_success())) {
    TRANS_LOG(WARN, "invoke the on_success of a extra_cb_ failed", K(ret),
              KPC(log_op_arg_.submit_arg_.log_cb_));
  } else {
    TRANS_LOG(DEBUG, "<ObTxDirectLoadIncLog> sync log succ", K(ret), KPC(this));
  }

  return ret;
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::log_sync_fail_()
{
  int ret = OB_SUCCESS;
  const ObTxDirectLoadIncLog::DirectLoadIncLogType ddl_log_type =
      log_op_arg_.submit_arg_.log_cb_->get_ddl_log_type();
  if (OB_ISNULL(log_op_arg_.submit_arg_.log_cb_)
      || OB_ISNULL(log_op_arg_.submit_arg_.log_cb_->get_extra_cb())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid log_cb or extra_cb", K(ret), K(log_op_arg_.submit_arg_));
  } else if (OB_FAIL(log_op_arg_.submit_arg_.log_cb_->get_extra_cb()->on_failure())) {
    TRANS_LOG(WARN, "invoke the on_failure of a extra_cb_ failed", K(ret),
              KPC(log_op_arg_.submit_arg_.log_cb_));
  } else {
    CtxLockGuard guard;
    if (!tx_ctx_->lock_.is_locked_by_self()) {
      tx_ctx_->get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (ddl_log_type == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START) {
      if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_start_fail(
              log_op_arg_.submit_arg_.log_cb_->get_batch_key()))) {
        TRANS_LOG(WARN, "update ddl_start key after log_sync_fail failed", K(ret),
                  KPC(log_op_arg_.submit_arg_.log_cb_), K(scn_), KPC(tx_ctx_));
      }
    } else if (ddl_log_type == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END) {
      if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_end_fail(
              log_op_arg_.submit_arg_.log_cb_->get_batch_key()))) {
        TRANS_LOG(WARN, "update ddl_end key after log_sync_fail failed", K(ret),
                  KPC(log_op_arg_.submit_arg_.log_cb_), K(scn_), KPC(tx_ctx_));
      }
    }
    TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> sync log fail", K(ret), KPC(this));
  }

  return ret;
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::replay_out_ctx_()
{
  int ret = OB_SUCCESS;

  construct_arg_->ddl_log_type_ = log_object_ptr_->get_ddl_log_type();
  if (OB_ISNULL(log_op_arg_.replay_arg_.ddl_log_handler_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(log_op_arg_.replay_arg_));
  } else {
    if (log_object_ptr_->get_ddl_log_type()
        == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_REDO) {

      storage::ObDDLRedoLog ddl_redo;
      if (OB_FAIL(log_object_ptr_->get_dli_buf().deserialize_log_object(&ddl_redo))) {
        TRANS_LOG(WARN, "deserialize ddl redo log failed", K(ret), K(ddl_redo),
                  K(log_op_arg_.replay_arg_));
      } else if (OB_FAIL(log_op_arg_.replay_arg_.ddl_log_handler_ptr_->get_ddl_log_replayer()
                             .replay_redo(ddl_redo, scn_))) {
        TRANS_LOG(WARN, "replay direct_load_inc redo for ddl_log_handler failed", K(ret),
                  KPC(log_object_ptr_), K(log_op_arg_.replay_arg_));
      }

    } else if (log_object_ptr_->get_ddl_log_type()
               == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START) {

      storage::ObDDLIncStartLog ddl_start;
      if (OB_FAIL(log_object_ptr_->get_dli_buf().deserialize_log_object(&ddl_start))) {
        TRANS_LOG(WARN, "deserialize ddl redo log failed", K(ret), K(ddl_start),
                  K(log_op_arg_.replay_arg_));
      } else if (OB_FAIL(log_op_arg_.replay_arg_.ddl_log_handler_ptr_->get_ddl_log_replayer()
                             .replay_inc_start(ddl_start, scn_))) {
        TRANS_LOG(WARN, "replay direct_load_inc redo for ddl_log_handler failed", K(ret),
                  KPC(log_object_ptr_), K(log_op_arg_.replay_arg_));
      }

      construct_arg_->batch_key_ = ddl_start.get_log_basic();

    } else if (log_object_ptr_->get_ddl_log_type()
               == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END) {

      storage::ObDDLIncCommitLog ddl_commit;
      if (OB_FAIL(log_object_ptr_->get_dli_buf().deserialize_log_object(&ddl_commit))) {
        TRANS_LOG(WARN, "deserialize ddl redo log failed", K(ret), K(ddl_commit),
                  K(log_op_arg_.replay_arg_));
      } else if (OB_FAIL(log_op_arg_.replay_arg_.ddl_log_handler_ptr_->get_ddl_log_replayer()
                             .replay_inc_commit(ddl_commit, scn_))) {
        TRANS_LOG(WARN, "replay direct_load_inc redo for ddl_log_handler failed", K(ret),
                  KPC(log_object_ptr_), K(log_op_arg_.replay_arg_));
      }

      construct_arg_->batch_key_ = ddl_commit.get_log_basic();
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> replay out ctx", K(ret), KPC(this), K(log_object_ptr_),
              K(log_op_arg_.replay_arg_));
  }
  return ret;
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::replay_fail_out_ctx_()
{
  int ret = OB_SUCCESS;
  // TODO direct_load_inc
  // replay data into ddl kv?
  TRANS_LOG(DEBUG, "<ObTxDirectLoadIncLog> replay fail out ctx", K(ret));

  return ret;
}

template <>
OB_INLINE int ObTxCtxLogOperator<ObTxDirectLoadIncLog>::replay_in_ctx_()
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard;
  if (!tx_ctx_->lock_.is_locked_by_self()) {
    tx_ctx_->get_ctx_guard(guard, CtxLockGuard::MODE::CTX);
  }

  bool need_replay = true;
  if (OB_FAIL(tx_ctx_->check_replay_avaliable_(lsn_, scn_, log_op_arg_.replay_arg_.part_log_no_,
                                               need_replay))) {
    TRANS_LOG(INFO, "check replay avaliable failed", K(ret), K(ObTxDirectLoadIncLog::LOG_TYPE),
              KPC(this));
  } else if (!need_replay) {
    TRANS_LOG(INFO, "need not replay log", KPC(log_object_ptr_), K(log_op_arg_.replay_arg_),
              KPC(tx_ctx_));
  } else if (construct_arg_->ddl_log_type_
             == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_START) {
    if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.before_submit_ddl_start(
            construct_arg_->batch_key_, scn_))) {
      TRANS_LOG(WARN, "register ddl_start key failed", K(ret), KPC(construct_arg_), K(scn_),
                KPC(tx_ctx_));
    } else if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.submit_ddl_start_succ(
                   construct_arg_->batch_key_, scn_))) {
      TRANS_LOG(WARN, "update ddl_start key after submit_log failed", K(ret), KPC(construct_arg_),
                K(scn_), KPC(tx_ctx_));
    } else if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_start_succ(
                   construct_arg_->batch_key_, scn_))) {
      TRANS_LOG(WARN, "update ddl_start key after log_sync_succ failed", K(ret),
                KPC(construct_arg_), K(scn_), KPC(tx_ctx_));
    }
  } else if (construct_arg_->ddl_log_type_ == ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END) {
    if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.before_submit_ddl_end(construct_arg_->batch_key_,
                                                                         scn_))) {
      TRANS_LOG(WARN, "register ddl_end key failed", K(ret), KPC(construct_arg_), K(scn_),
                KPC(tx_ctx_));
      if (OB_ENTRY_NOT_EXIST == ret || OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.submit_ddl_end_succ(
                   construct_arg_->batch_key_, scn_))) {
      TRANS_LOG(WARN, "update ddl_end key after submit_log failed", K(ret), KPC(construct_arg_),
                K(scn_), KPC(tx_ctx_));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }

    } else if (OB_FAIL(tx_ctx_->exec_info_.dli_batch_set_.sync_ddl_end_succ(
                   construct_arg_->batch_key_, scn_))) {
      TRANS_LOG(WARN, "update ddl_end key after log_sync_succ failed", K(ret), KPC(construct_arg_),
                K(scn_), KPC(tx_ctx_));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "<ObTxDirectLoadIncLog> replay in ctx", K(ret), KPC(this), K(log_object_ptr_),
              K(log_op_arg_.replay_arg_));
  }
  return ret;
}

/****************************************
 * Direct Load Inc TxLog Operator
 * End
 ****************************************/

} // namespace transaction

} // namespace oceanbase

#endif

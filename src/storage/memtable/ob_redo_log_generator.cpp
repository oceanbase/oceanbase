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

#include "ob_redo_log_generator.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_memtable_key.h"
#include "ob_memtable.h"
#include "ob_memtable_data.h"
#include "ob_memtable_context.h"
#include "mvcc/ob_tx_callback_functor.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tablelock/ob_table_lock_callback.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace memtable
{

void ObRedoLogGenerator::reset()
{
  is_inited_ = false;
  redo_filled_cnt_ = 0;
  redo_sync_succ_cnt_ = 0;
  redo_sync_fail_cnt_ = 0;
  callback_mgr_ = nullptr;
  mem_ctx_ = NULL;
  last_logging_blocked_time_ = 0;
  if (clog_encrypt_meta_ != NULL) {
    op_free(clog_encrypt_meta_);
    clog_encrypt_meta_ = NULL;
  }
}

void ObRedoLogGenerator::reuse()
{
  //TODO: remove this
}

int ObRedoLogGenerator::set(ObTransCallbackMgr *mgr, ObMemtableCtx *mem_ctx)
{
  if (IS_INIT) {
    // already set, reset first
    reset();
  }

  int ret = OB_SUCCESS;
  callback_mgr_ = mgr;
  mem_ctx_ = mem_ctx;
  last_logging_blocked_time_ = 0;
  is_inited_ = true;

  return ret;
}

//
// this functor handle _one_ callback
//
// return value:
// - OB_SUCCESS: success, all callbacks were filled
// - OB_BUF_NOT_ENOUGH: buffer can not hold this callback
// - OB_BLOCK_FROZEN: the callback's memtable logging is blocked
//                    on waiting the previous frozen siblings logged
// - OB_ITER_END: reach end of *ctx.epoch_to_*
// - OB_XXX: other error
class ObFillRedoLogFunctor final : public ObITxFillRedoFunctor
{
public:
  ObFillRedoLogFunctor(ObMemtableCtx *mem_ctx,
                       transaction::ObTxEncryptMeta *clog_encrypt_meta,
                       ObTxFillRedoCtx &ctx,
                       ObMutatorWriter &mmw,
                       transaction::ObCLogEncryptInfo &encrypt_info) :
    mem_ctx_(mem_ctx),
    clog_encrypt_meta_(clog_encrypt_meta),
    ctx_(ctx),
    mmw_(mmw),
    encrypt_info_(encrypt_info)
  {}
  int operator()(ObITransCallback *iter)
  {
    TRANS_LOG(DEBUG, "fill_redo_for_callback", KPC(iter));
    int ret = OB_SUCCESS;
    if (iter->get_epoch() > ctx_.epoch_to_) {
      ret = OB_ITER_END;
      ctx_.next_epoch_ = iter->get_epoch();
    } else if (iter->get_epoch() < ctx_.epoch_from_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "found callback with epoch less than `from`", K(ret), KPC(iter), K(ctx_));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    } else if (FALSE_IT(ctx_.cur_epoch_ = iter->get_epoch())) {
    } else if (!iter->need_submit_log()) {
      // this should not happend
      // because log_cursor is _strictly_ point to the right next to logging position
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "found callback has been logged, maybe log_cursor value is insane", K(ret), KPC(iter), K(ctx_));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    } else if (iter->is_logging_blocked()) {
      ret = OB_BLOCK_FROZEN;
      ctx_.last_log_blocked_memtable_ = static_cast<memtable::ObMemtable *>(iter->get_memtable());
    } else if (OB_UNLIKELY(iter->get_freeze_clock() >= ctx_.freeze_clock_)) {
      // when flush redo for frozen memtable, if memtable is active, should stop
      ret = OB_BLOCK_FROZEN;
    } else {
      bool fake_fill = false;
      if (MutatorType::MUTATOR_ROW == iter->get_mutator_type()) {
        ret = fill_row_redo_(iter, fake_fill);
      } else if (MutatorType::MUTATOR_TABLE_LOCK == iter->get_mutator_type()) {
        ret = fill_table_lock_redo_(iter, fake_fill);
      } else if (MutatorType::MUTATOR_ROW_EXT_INFO == iter->get_mutator_type()) {
        ret = fill_ext_info_redo_(iter, fake_fill);
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "mutator row type not expected.", K(ret));
      }

      if (OB_SUCC(ret)) {
        ObCallbackScope &callback_scope = *ctx_.callback_scope_;
        if (nullptr == *callback_scope.start_) {
          callback_scope.start_ = iter;
        }
        callback_scope.end_ = iter;
        ++callback_scope.cnt_;
        if (!fake_fill) {
          ctx_.fill_count_++;
        }
        data_size_ += iter->get_data_size();
        max_seq_no_ = MAX(max_seq_no_, iter->get_seq_no());
      }
    }
    return ret;
  }
private:
  int fill_row_redo_(ObITransCallback *callback, bool &fake_fill)
  {
    int ret = OB_SUCCESS;
    RedoDataNode redo;
    ObMvccRowCallback *riter = (ObMvccRowCallback *)callback;

    if (blocksstable::ObDmlFlag::DF_LOCK == riter->get_dml_flag()) {
      if (ctx_.skip_lock_node_) {
        riter->set_not_calc_checksum(true);
        fake_fill = true;
      } else {
        // need to calc checksum
        riter->set_not_calc_checksum(false);
      }
    }

    if (fake_fill) {
    } else if (OB_FAIL(riter->get_redo(redo)) && OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(ERROR, "get_redo", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      ObMemtable *memtable = static_cast<memtable::ObMemtable *>(riter->get_memtable());
      if (OB_ISNULL(memtable)) {
        TRANS_LOG(ERROR, "memtable is null", K(riter));
        ret = OB_ERR_UNEXPECTED;
#ifdef OB_BUILD_TDE_SECURITY
      } else if (OB_FAIL(memtable->get_encrypt_meta(clog_encrypt_meta_))) {
        TRANS_LOG(ERROR, "get encrypt meta failed", K(memtable), K(ret));
#endif
      } else if (OB_FAIL(mmw_.append_row_kv(mem_ctx_->get_max_table_version(),
                                            redo,
                                            clog_encrypt_meta_,
                                            encrypt_info_,
                                            false))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          TRANS_LOG(WARN, "mutator writer append_kv fail", "ret", ret);
        }
      }
    }
    return ret;
  }
  int fill_table_lock_redo_(ObITransCallback *callback, bool &fake_fill)
  {
    int ret = OB_SUCCESS;
    TableLockRedoDataNode redo;
    ObOBJLockCallback *titer = (ObOBJLockCallback *)callback;
    if (ctx_.skip_lock_node_ && !titer->must_log()) {
      fake_fill = true;
    } else if (OB_FAIL(titer->get_redo(redo))) {
      TRANS_LOG(ERROR, "get_redo failed.", K(ret));
    } else if (OB_FAIL(mmw_.append_table_lock_kv(mem_ctx_->get_max_table_version(), redo))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        TRANS_LOG(WARN, "fill table lock redo fail", K(ret));
      }
    }
    TRANS_LOG(DEBUG, "fill table lock redo.", K(ret), K(*titer), K(redo.lock_id_), K(redo.lock_mode_));
    return ret;
  }

  int fill_ext_info_redo_(ObITransCallback *callback, bool &fake_fill)
  {
    int ret = OB_SUCCESS;
    RedoDataNode redo;
    ObExtInfoCallback *ext_iter = (ObExtInfoCallback *)callback;
    if (OB_FAIL(ext_iter->get_redo(redo))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "get_redo fail", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(mmw_.append_ext_info_log_kv(mem_ctx_->get_max_table_version(),
                                                  redo, false/*is_big_row*/))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        TRANS_LOG(WARN, "mutator writer append_kv fail", K(ret));
      }
    }
    return ret;
  }

private:
  ObMemtableCtx *mem_ctx_;
  transaction::ObTxEncryptMeta *clog_encrypt_meta_;
  ObTxFillRedoCtx &ctx_;
  ObMutatorWriter &mmw_;
  transaction::ObCLogEncryptInfo &encrypt_info_;
};

//
// fill redo log into log block
//
// This handle both serial logging and parallel logging
// for serial logging:
//   callbacks from multi callback-list filled together into one log block
// for parallel logging:
//   each callback-list's logs are filled into seperate log block
//
// In parallel logging mode, there are two type of fill_redo scheme:
// 1. fill from all callback-list:
//    for freeze, switch leader, commit
// 2. fill from single callback-list:
//    writer thread flush pending logs from its callback-list after write
//
// return value:
// - OB_SUCCESS: all callbacks are filled
// - OB_BUF_NOT_ENOUGH: buffer is full or can not hold mutator row
// - OB_BLOCK_FROZEN: the callback's memtable is blocked, can not be fill
// - OB_ITER_END: has small write_epoch whose log is not flushed
int ObRedoLogGenerator::fill_redo_log(ObTxFillRedoCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx.buf_) || ctx.buf_len_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", KP(ctx.buf_), K(ctx.buf_len_));
  } else {
    // prepare the global varibles
    ObMutatorWriter mmw;
    mmw.set_buffer(ctx.buf_, ctx.buf_len_ - ctx.buf_pos_);
    // used to encrypt each mutator row
    transaction::ObCLogEncryptInfo encrypt_info;
    encrypt_info.init();
    ctx.helper_->reset();
    ObFillRedoLogFunctor functor(mem_ctx_, clog_encrypt_meta_, ctx, mmw, encrypt_info);
    ret = callback_mgr_->fill_log(ctx, functor);

    // finally, serialize meta and finish the RedoLog
    int save_ret = ret;
    if (ctx.fill_count_ > 0) {
      int64_t res_len = 0;
      uint8_t row_flag = ObTransRowFlag::NORMAL_ROW;
#ifdef OB_BUILD_TDE_SECURITY
      if (encrypt_info.has_encrypt_meta()) {
        row_flag |= ObTransRowFlag::ENCRYPT;
      }
#endif
      if (OB_FAIL(mmw.serialize(row_flag, res_len, encrypt_info))) {
        TRANS_LOG(WARN, "mmw.serialize fail, can not submit this redo out", K(ret));
        // if serialize meta failed, this round of fill redo failed
        // mark the fill_count_ to indicate this
        ctx.fill_count_ = 0;
      } else {
        ctx.buf_pos_ += res_len;
        ret = save_ret;
      }
    }
  }
  return ret;
}

// log_submitted - callback after log submitted
// - mark callback's state to log submitted
// - mark callback's log scn
int ObRedoLogGenerator::log_submitted(const ObCallbackScopeArray &callbacks_arr, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  int submitted_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (OB_FAIL(callback_mgr_->log_submitted(callbacks_arr, scn, submitted_cnt))) {
    TRANS_LOG(ERROR, "log submitted callback fail", K(ret));
  }
  ATOMIC_AAF(&redo_filled_cnt_, submitted_cnt);
  return ret;
}

int ObRedoLogGenerator::sync_log_succ(const ObCallbackScopeArray &callbacks_arr,
                                      const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t sync_cnt = 0;
  if (OB_FAIL(callback_mgr_->log_sync_succ(callbacks_arr, scn, sync_cnt))) {
    TRANS_LOG(ERROR, "sync succ fail", K(ret));
  } else {
    redo_sync_succ_cnt_ += sync_cnt;
  }
  return ret;
}

void ObRedoLogGenerator::sync_log_fail(const ObCallbackScopeArray &callbacks,
                                       const share::SCN &max_applied_scn)
{
  int ret = OB_SUCCESS;
  int64_t removed_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (OB_FAIL(callback_mgr_->log_sync_fail(callbacks, max_applied_scn, removed_cnt))) {
    TRANS_LOG(ERROR, "sync log failed", K(ret));
  } else {
    redo_sync_fail_cnt_ += removed_cnt;
  }
}

void ObRedoLogGenerator::print_first_mvcc_callback()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    // TODO:(yunxing.cyx) PRINT ...
  }
}

int ObRedoLogGenerator::search_unsubmitted_dup_tablet_redo()
{
  // OB_ENTRY_NOT_EXIST => no dup table tablet
  // OB_SUCCESS => find a dup table tablet
  int ret = OB_ENTRY_NOT_EXIST;
  if (!is_inited_) {
    TRANS_LOG(WARN, "redo log generate is not inited", K(ret));
  } else {
    struct CheckDupTabletFunc final : public ObITxCallbackFinder {
      bool match(ObITransCallback *callback) {
        bool ok =false;
        if (!callback->need_submit_log()) {
          //do nothing
        } else if (generator_->check_dup_tablet(callback)) {
          ok = true;
        }
        return ok;
      };
      ObRedoLogGenerator *generator_;
    };
    CheckDupTabletFunc check_func;
    check_func.generator_ = this;
    if (callback_mgr_->find(check_func)) {
      ret = OB_SUCCESS;
      mem_ctx_->get_trans_ctx()->set_dup_table_tx_();
    }
  }
  return ret;
}

bool ObRedoLogGenerator::check_dup_tablet(const ObITransCallback *callback_ptr) const
{
  bool is_dup_tablet = false;
  int64_t tmp_ret = OB_SUCCESS;

  // If id is a dup table tablet => true
  // If id is not a dup table tablet => false
  if (MutatorType::MUTATOR_ROW == callback_ptr->get_mutator_type()) {
    const ObMvccRowCallback *row_iter = static_cast<const ObMvccRowCallback *>(callback_ptr);
    const ObTabletID &target_tablet = row_iter->get_tablet_id();
    // if (OB_TMP_FAIL(mem_ctx_->get_trans_ctx()->merge_tablet_modify_record_(target_tablet))) {
    //   TRANS_LOG_RET(WARN, tmp_ret, "merge tablet modify record failed", K(tmp_ret),
    //                 K(target_tablet), KPC(row_iter));
    // }
    // check dup table
  }

  return is_dup_tablet;
}

void ObRedoLogGenerator::bug_detect_for_logging_blocked_()
{
  // TODO: (yunxing.cyx) print first 5 callback ?
}

}; // end namespace memtable
}; // end namespace oceanbase

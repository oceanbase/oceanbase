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
  generate_cursor_.reset();
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
  generate_cursor_.reset();
  generate_cursor_ = callback_mgr_->begin();
}

int ObRedoLogGenerator::set(ObTransCallbackMgr *mgr, ObIMemtableCtx *mem_ctx)
{
  if (IS_INIT) {
    // already set, reset first
    reset();
  }

  int ret = OB_SUCCESS;
  generate_cursor_ = mgr->begin();
  callback_mgr_ = mgr;
  mem_ctx_ = mem_ctx;
  last_logging_blocked_time_ = 0;
  is_inited_ = true;

  return ret;
}

int ObRedoLogGenerator::fill_redo_log(char *buf,
                                      const int64_t buf_len,
                                      int64_t &buf_pos,
                                      ObRedoLogSubmitHelper &helper,
                                      const bool log_for_lock_node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len < 0 || buf_pos < 0 || buf_pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", KP(buf), K(buf_len), K(buf_pos));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    helper.reset();
    ObMutatorWriter mmw;
    mmw.set_buffer(buf, buf_len - buf_pos);
    RedoDataNode redo;
    TableLockRedoDataNode table_lock_redo;
    // record the number of serialized trans node in the filling process
    int64_t data_node_count = 0;
    transaction::ObTxSEQ max_seq_no;
    // TODO by fengshuo.fs : fix this usage
    ObTransCallbackMgr::RDLockGuard guard(callback_mgr_->get_rwlock());
    ObCallbackScope callbacks;
    int64_t data_size = 0;
    ObITransCallbackIterator cursor;
    // for encrypt
    transaction::ObCLogEncryptInfo encrypt_info;
    encrypt_info.init();

    for (cursor = generate_cursor_ + 1; OB_SUCC(ret) && callback_mgr_->end() != cursor; ++cursor) {
      ObITransCallback *iter = (ObITransCallback *)*cursor;

      if (!iter->need_fill_redo() || !iter->need_submit_log()) {
      } else if (iter->is_logging_blocked()) {
        ret = (data_node_count == 0) ? OB_BLOCK_FROZEN : OB_EAGAIN;
        if (OB_BLOCK_FROZEN == ret) {
          // To prevent unnecessary submit_log actions for freeze
          // Becasue the first callback is linked to a logging_blocked memtable
          transaction::ObPartTransCtx *part_ctx = static_cast<transaction::ObPartTransCtx *>(mem_ctx_->get_trans_ctx());
          part_ctx->set_block_frozen_memtable(static_cast<memtable::ObMemtable *>(iter->get_memtable()));

          int64_t current_time = ObTimeUtility::current_time();
          if (last_logging_blocked_time_ == 0) {
            last_logging_blocked_time_ = current_time;
          } else if (current_time - last_logging_blocked_time_ > 5 * 1_min) {
            TRANS_LOG(WARN, "logging block cost too much time", KPC(part_ctx), KPC(iter));
            if (REACH_TENANT_TIME_INTERVAL(1_min)) {
              bug_detect_for_logging_blocked_();
            }
          }
        }
      } else {
        last_logging_blocked_time_ = 0;

        bool fake_fill = false;
        if (MutatorType::MUTATOR_ROW == iter->get_mutator_type()) {
          ret = fill_row_redo(cursor, mmw, redo, log_for_lock_node, fake_fill, encrypt_info);
        } else if (MutatorType::MUTATOR_TABLE_LOCK == iter->get_mutator_type()) {
          ret = fill_table_lock_redo(cursor, mmw, table_lock_redo, log_for_lock_node, fake_fill);
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "mutator row type not expected.", K(ret));
        }

        if (OB_BUF_NOT_ENOUGH == ret) {
          // buf is not enough: if some rows have been serialized before, that means
          // more redo data is demanding more buf, returns OB_EAGAIN;
          // if the buf is not enough for the first trans node, that means a big row
          // is comming, handle it according to the big row logic
          if (0 != data_node_count) {
            ret = OB_EAGAIN;
            // deal with big row logic
          } else {
            ret = OB_ERR_TOO_BIG_ROWSIZE;
          }
        }

        if (OB_UNLIKELY(OB_ERR_TOO_BIG_ROWSIZE == ret)) {
          callbacks.start_ = callbacks.end_ = cursor;
          data_size += iter->get_data_size();
          max_seq_no = MAX(max_seq_no, iter->get_seq_no());
        } else if (OB_SUCC(ret)) {
          if (nullptr == *callbacks.start_) {
            callbacks.start_ = cursor;
          }
          callbacks.end_ = cursor;

          if (!fake_fill) {
            data_node_count++;
          }
          data_size += iter->get_data_size();
          max_seq_no = MAX(max_seq_no, iter->get_seq_no());
        }
      }
    }

    if (OB_EAGAIN == ret || OB_SUCCESS == ret || OB_ERR_TOO_BIG_ROWSIZE == ret) {
      int tmp_ret = OB_SUCCESS;

      helper.callbacks_ = callbacks;
      helper.max_seq_no_ = max_seq_no;
      helper.data_size_ = data_size;

      if (OB_LIKELY(OB_ERR_TOO_BIG_ROWSIZE != ret)) {
        int64_t res_len = 0;
        uint8_t row_flag = ObTransRowFlag::NORMAL_ROW;
#ifdef OB_BUILD_TDE_SECURITY
        if (encrypt_info.has_encrypt_meta()) {
          row_flag |= ObTransRowFlag::ENCRYPT;
        }
#endif
        if (OB_SUCCESS != (tmp_ret = mmw.serialize(row_flag, res_len, encrypt_info))) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            TRANS_LOG(ERROR, "mmw.serialize fail", K(ret), K(tmp_ret));
            ret = tmp_ret;
          } else {
            #ifndef NDEBUG
            TRANS_LOG(INFO, "not row exist, ignore serialize", K(ret), K(tmp_ret), K(mmw.get_meta()),
                      K(data_node_count));
            #endif
            if (OB_SUCCESS == ret) {
              ret = tmp_ret;
            }
          }
        } else {
          buf_pos += res_len;
        }
      }
    }
  }
  return ret;
}

// sub unsubmitted cnt for the callback that has submitted log
int ObRedoLogGenerator::log_submitted(const ObCallbackScope &callbacks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (!callbacks.is_empty()) {
    ObITransCallbackIterator cursor = callbacks.start_;
    do {
      ObITransCallback *iter = (ObITransCallback *)*cursor;
      if (iter->need_submit_log()) {
        if (OB_TMP_FAIL(iter->log_submitted_cb())) {
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
          TRANS_LOG(ERROR, "fail to log_submitted cb", K(tmp_ret));
        } else {
          redo_filled_cnt_ += 1;
        }
        // check dup table tx 
        if(check_dup_tablet_(iter))
        {
          // mem_ctx_->get_trans_ctx()->set_dup_table_tx_();
        }
      } else {
        TRANS_LOG(ERROR, "log_submitted error", K(ret), K(iter), K(iter->need_submit_log()));
      }
    } while (cursor != callbacks.end_ && !FALSE_IT(cursor++));

    generate_cursor_ = callbacks.end_;
  }

  return ret;
}

int ObRedoLogGenerator::sync_log_succ(const SCN scn, const ObCallbackScope &callbacks)
{
  // no need to submit log
  // since the number of log callback is enough now
  // and will be allocated dynamically in the future
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (!callbacks.is_empty()) {
    ObTransCallbackMgr::RDLockGuard guard(callback_mgr_->get_rwlock());
    ObITransCallbackIterator cursor = callbacks.start_;
    do {
      ObITransCallback *iter = (ObITransCallback *)*cursor;
      if (iter->need_fill_redo()) {
        iter->set_scn(scn);
        if (OB_TMP_FAIL(iter->log_sync_cb(scn))) {
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
          TRANS_LOG(WARN, "failed to set sync log info for callback ", K(tmp_ret), K(*iter));
        } else {
          redo_sync_succ_cnt_ += 1;
        }
      } else {
        TRANS_LOG(ERROR, "sync_log_succ error", K(ret), K(iter), K(iter->need_fill_redo()), K(scn));
      }
    } while (cursor != callbacks.end_ && !FALSE_IT(cursor++));
  }
  // TRANS_LOG(INFO, "sync log succ for memtable callbacks", K(ret), K(redo_sync_succ_cnt_), "tx_id",
  //           this->mem_ctx_->get_trans_ctx()->get_trans_id());

  return ret;
}

void ObRedoLogGenerator::sync_log_fail(const ObCallbackScope &callbacks)
{
  int ret = OB_SUCCESS;
  int64_t removed_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "not init", K(ret));
  } else if (!callbacks.is_empty()) {
    if (OB_FAIL(callback_mgr_->sync_log_fail(callbacks, removed_cnt))) {
      TRANS_LOG(ERROR, "sync log failed", K(ret));
    }
    redo_sync_fail_cnt_ += removed_cnt;
  }
}

void ObRedoLogGenerator::print_first_mvcc_callback()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObTransCallbackMgr::RDLockGuard guard(callback_mgr_->get_rwlock());
    ObITransCallbackIterator cursor = generate_cursor_ + 1;
    if (callback_mgr_->end() != cursor) {
      ObITransCallback *iter = (ObITransCallback *)*cursor;
      LOG_DBA_WARN(OB_TRANS_LIVE_TOO_MUCH_TIME,
                   "msg",
                   "transaction live cost too much time without commit or abort",
                   KPC(iter));
    }
  }
}

int ObRedoLogGenerator::fill_row_redo(ObITransCallbackIterator &cursor,
                                      ObMutatorWriter &mmw,
                                      RedoDataNode &redo,
                                      const bool log_for_lock_node,
                                      bool &fake_fill,
                                      transaction::ObCLogEncryptInfo &encrypt_info)
{
  int ret = OB_SUCCESS;

  ObMvccRowCallback *riter = (ObMvccRowCallback *)*cursor;

  if (blocksstable::ObDmlFlag::DF_LOCK == riter->get_dml_flag()) {
    if (!log_for_lock_node) {
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
    } else if (OB_FAIL(mmw.append_row_kv(mem_ctx_->get_max_table_version(),
                                         redo,
                                         clog_encrypt_meta_,
                                         encrypt_info,
                                         false))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        TRANS_LOG(WARN, "mutator writer append_kv fail", "ret", ret);
      }
    }
  }

  return ret;
}

int ObRedoLogGenerator::fill_table_lock_redo(ObITransCallbackIterator &cursor,
                                             ObMutatorWriter &mmw,
                                             TableLockRedoDataNode &redo,
                                             const bool log_for_lock_node,
                                             bool &fake_fill)
{
  int ret = OB_SUCCESS;

  ObOBJLockCallback *titer = (ObOBJLockCallback *)*cursor;
  if (!log_for_lock_node && !titer->must_log()) {
    fake_fill = true;
  } else if (OB_FAIL(titer->get_redo(redo))) {
    TRANS_LOG(ERROR, "get_redo failed.", K(ret));
  } else if (OB_FAIL(mmw.append_table_lock_kv(mem_ctx_->get_max_table_version(),
                                              redo))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      TRANS_LOG(WARN, "fill table lock redo fail", K(ret));
    }
  }

  TRANS_LOG(DEBUG, "fill table lock redo.",
            K(ret), K(*titer), K(redo.lock_id_), K(redo.lock_mode_));
  return ret;
}

int ObRedoLogGenerator::search_unsubmitted_dup_tablet_redo()
{
  // OB_ENTRY_NOT_EXIST => no dup table tablet
  // OB_SUCCESS => find a dup table tablet
  int ret = OB_ENTRY_NOT_EXIST;

  ObITransCallbackIterator cursor;
  if (!is_inited_) {
    TRANS_LOG(WARN, "redo log generate is not inited", K(ret));
  } else {
    ObTransCallbackMgr::RDLockGuard guard(callback_mgr_->get_rwlock());
    for (cursor = generate_cursor_ + 1; OB_SUCC(ret) && callback_mgr_->end() != cursor; ++cursor) {
      ObITransCallback *iter = (ObITransCallback *)*cursor;

      if (!iter->need_fill_redo() || !iter->need_submit_log()) {
        //do nothing
      } else if (check_dup_tablet_(iter)) {
        // ret = OB_SUCCESS;
        // mem_ctx_->get_trans_ctx()->set_dup_table_tx_();
        // break;
      }
    }
  }
  return ret;
}

bool ObRedoLogGenerator::check_dup_tablet_(const ObITransCallback *callback_ptr) const
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
  int ret = OB_SUCCESS;

  ObITransCallbackIterator bug_detect_cursor;
  int64_t count = 0;
  for (bug_detect_cursor = generate_cursor_ + 1;
       callback_mgr_->end() !=bug_detect_cursor
         && count <= 5;
       ++bug_detect_cursor) {
    ObITransCallback *bug_detect_iter = (ObITransCallback *)*bug_detect_cursor;
    count++;
    TRANS_LOG(WARN, "logging block print callback", KPC(bug_detect_iter), K(count));
  }
}

}; // end namespace memtable
}; // end namespace oceanbase
